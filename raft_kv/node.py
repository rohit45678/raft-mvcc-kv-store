"""Raft node: state machine for a single server.

Deterministic, in-process: no sockets, no threads, no timers firing
on their own. Elections and replication are driven by explicit method
calls via the Cluster harness (or directly in tests).
"""
from __future__ import annotations
from enum import Enum
from typing import Optional

from .log import RaftLog, LogEntry
from .mvcc import MVCCStore


class NodeState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class RaftNode:
    def __init__(self, node_id: str, peer_ids: list[str]):
        self.id = node_id
        self.peer_ids = list(peer_ids)
        self.state = NodeState.FOLLOWER
        self.current_term = 0
        self.voted_for: Optional[str] = None
        self.leader_id: Optional[str] = None

        self.log = RaftLog()
        self.commit_index = 0
        self.last_applied = 0

        # Leader volatile state
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # State machine
        self.store = MVCCStore()

        # Election bookkeeping
        self.votes_received: set[str] = set()
        self.election_elapsed = 0  # ticks since last heartbeat/election reset

    # ---------- helpers ----------
    def is_leader(self) -> bool:
        return self.state == NodeState.LEADER

    def _become_follower(self, term: int):
        self.state = NodeState.FOLLOWER
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
        self.leader_id = None

    def step_down(self, new_term: Optional[int] = None):
        """Leader (or candidate) steps down to follower."""
        if new_term is not None and new_term >= self.current_term:
            self.current_term = new_term
            self.voted_for = None
        self.state = NodeState.FOLLOWER
        self.leader_id = None
        self.votes_received.clear()

    # ---------- election ----------
    def start_election(self) -> dict:
        """Begin an election: increment term, vote for self, return vote request info."""
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.leader_id = None
        self.election_elapsed = 0
        return {
            "term": self.current_term,
            "candidate_id": self.id,
            "last_log_index": self.log.last_index(),
            "last_log_term": self.log.last_term(),
        }

    def handle_request_vote(self, term: int, candidate_id: str,
                            last_log_index: int, last_log_term: int) -> tuple[bool, int]:
        """Return (vote_granted, current_term)."""
        if term < self.current_term:
            return False, self.current_term
        if term > self.current_term:
            self._become_follower(term)
        # term == current_term now
        if self.voted_for is not None and self.voted_for != candidate_id:
            return False, self.current_term
        if not self.log.is_up_to_date(last_log_index, last_log_term):
            return False, self.current_term
        self.voted_for = candidate_id
        self.state = NodeState.FOLLOWER
        self.election_elapsed = 0
        return True, self.current_term

    def handle_vote_response(self, voter_id: str, term: int, granted: bool, cluster_size: int) -> bool:
        """Process a vote; return True if this node just became leader."""
        if term > self.current_term:
            self._become_follower(term)
            return False
        if self.state != NodeState.CANDIDATE or term != self.current_term:
            return False
        if granted:
            self.votes_received.add(voter_id)
        majority = cluster_size // 2 + 1
        if len(self.votes_received) >= majority:
            self.become_leader()
            return True
        return False

    def become_leader(self):
        self.state = NodeState.LEADER
        self.leader_id = self.id
        last = self.log.last_index()
        self.next_index = {p: last + 1 for p in self.peer_ids}
        self.match_index = {p: 0 for p in self.peer_ids}
        self.match_index[self.id] = last

    # ---------- replication (leader side) ----------
    def leader_append(self, command: dict) -> LogEntry:
        assert self.is_leader(), f"{self.id} is not leader"
        entry = self.log.append(self.current_term, command)
        self.match_index[self.id] = entry.index
        return entry

    def prepare_append_entries(self, follower_id: str) -> dict:
        ni = self.next_index.get(follower_id, self.log.last_index() + 1)
        prev_index = ni - 1
        prev_term = self.log.term_at(prev_index) if prev_index > 0 else 0
        entries = self.log.entries_from(ni)
        return {
            "term": self.current_term,
            "leader_id": self.id,
            "prev_log_index": prev_index,
            "prev_log_term": prev_term,
            "entries": entries,
            "leader_commit": self.commit_index,
        }

    def handle_append_response(self, follower_id: str, term: int, success: bool,
                               follower_last_index: int, cluster_size: int) -> bool:
        """Update next/match index; advance commit if majority replicated.
        Returns True if commit_index advanced."""
        if term > self.current_term:
            self._become_follower(term)
            return False
        if not self.is_leader():
            return False
        if success:
            self.match_index[follower_id] = follower_last_index
            self.next_index[follower_id] = follower_last_index + 1
        else:
            # back off next_index by one and let caller retry
            cur = self.next_index.get(follower_id, 1)
            self.next_index[follower_id] = max(1, cur - 1)
            return False
        return self._try_advance_commit(cluster_size)

    def _try_advance_commit(self, cluster_size: int) -> bool:
        majority = cluster_size // 2 + 1
        advanced = False
        # Find highest N > commit_index replicated on majority and from current term
        for n in range(self.log.last_index(), self.commit_index, -1):
            if self.log.term_at(n) != self.current_term:
                continue
            count = 1  # leader itself
            for p in self.peer_ids:
                if self.match_index.get(p, 0) >= n:
                    count += 1
            if count >= majority:
                self.commit_index = n
                self._apply_committed()
                advanced = True
                break
        return advanced

    # ---------- replication (follower side) ----------
    def handle_append_entries(self, term: int, leader_id: str, prev_log_index: int,
                              prev_log_term: int, entries: list[LogEntry],
                              leader_commit: int) -> tuple[bool, int, int]:
        """Return (success, current_term, last_log_index)."""
        if term < self.current_term:
            return False, self.current_term, self.log.last_index()
        # Recognize leader for this term
        self._become_follower(term)
        self.state = NodeState.FOLLOWER
        self.leader_id = leader_id
        self.current_term = term
        self.election_elapsed = 0

        # Log consistency check
        if prev_log_index > 0:
            if prev_log_index > self.log.last_index():
                return False, self.current_term, self.log.last_index()
            if self.log.term_at(prev_log_index) != prev_log_term:
                return False, self.current_term, self.log.last_index()

        if entries:
            self.log.append_entries(prev_log_index, entries)

        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_index())
            self._apply_committed()

        return True, self.current_term, self.log.last_index()

    # ---------- applying ----------
    def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get(self.last_applied)
            if entry is not None:
                self.store.apply_command(entry.command, commit_index=entry.index)

    def tick(self):
        """Deterministic tick: only advances election elapsed counter.
        No autonomous election — tests/cluster drive elections explicitly
        so behaviour is reproducible. Provided for API completeness."""
        self.election_elapsed += 1
        return self.election_elapsed
