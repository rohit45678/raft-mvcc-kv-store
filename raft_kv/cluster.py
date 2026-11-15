"""Cluster harness: wires nodes together with synchronous, deterministic RPCs.

No sockets, no threads. Every operation is an explicit method call,
which makes elections and replication fully reproducible in tests.
Supports partitions (a set of node ids cut off from the rest) to
exercise leader step-down / re-election scenarios.
"""
from __future__ import annotations
from typing import Optional

from .node import RaftNode, NodeState


class Cluster:
    def __init__(self, size: int = 3):
        if size < 1:
            raise ValueError("cluster size must be >= 1")
        ids = [f"n{i+1}" for i in range(size)]
        self.nodes: dict[str, RaftNode] = {}
        for nid in ids:
            peers = [p for p in ids if p != nid]
            self.nodes[nid] = RaftNode(nid, peers)
        self.size = size
        self._partition: Optional[set[str]] = None  # if set, these nodes are isolated from the rest

    # ---------- topology / partition ----------
    def partition(self, isolated_ids: set[str]):
        """Isolate the given nodes from nodes outside the set (and vice versa)."""
        self._partition = set(isolated_ids)

    def heal(self):
        self._partition = None

    def _can_communicate(self, a: str, b: str) -> bool:
        if self._partition is None:
            return True
        return (a in self._partition) == (b in self._partition)

    # ---------- election ----------
    def elect(self, candidate_id: str) -> bool:
        """Run a full election round for candidate_id. Returns True if it became leader."""
        candidate = self.nodes[candidate_id]
        req = candidate.start_election()
        # Single-node cluster: self-vote is majority
        if self.size == 1:
            candidate.handle_vote_response(candidate_id, req["term"], True, self.size)
            return candidate.is_leader()
        # Contact every reachable peer even after reaching majority, so all
        # nodes learn the new term (as they would from the leader's heartbeats).
        for peer_id, peer in self.nodes.items():
            if peer_id == candidate_id:
                continue
            if not self._can_communicate(candidate_id, peer_id):
                continue
            granted, _ = peer.handle_request_vote(
                req["term"], req["candidate_id"], req["last_log_index"], req["last_log_term"]
            )
            candidate.handle_vote_response(peer_id, req["term"], granted, self.size)
        return candidate.is_leader()

    def leader(self) -> Optional[RaftNode]:
        leaders = [n for n in self.nodes.values() if n.is_leader()]
        if not leaders:
            return None
        # If somehow multiple (partition), return highest term
        return max(leaders, key=lambda n: n.current_term)

    def leader_id(self) -> Optional[str]:
        l = self.leader()
        return l.id if l else None

    # ---------- replication ----------
    def replicate_to(self, leader_id: str, follower_id: str) -> bool:
        """One AppendEntries round leader->follower, with retry on mismatch.
        Returns final success."""
        leader = self.nodes[leader_id]
        follower = self.nodes[follower_id]
        if not self._can_communicate(leader_id, follower_id):
            return False
        # Retry loop bounded by log length + 1 (next_index backoff)
        for _ in range(leader.log.last_index() + 2):
            msg = leader.prepare_append_entries(follower_id)
            success, term, last_idx = follower.handle_append_entries(
                msg["term"], msg["leader_id"], msg["prev_log_index"],
                msg["prev_log_term"], msg["entries"], msg["leader_commit"],
            )
            advanced = leader.handle_append_response(follower_id, term, success, last_idx, self.size)
            if success:
                return True
        return False

    def replicate_all(self, leader_id: Optional[str] = None) -> None:
        lid = leader_id or self.leader_id()
        if lid is None:
            raise RuntimeError("no leader to replicate from")
        leader = self.nodes[lid]
        for fid in self.nodes:
            if fid != lid:
                self.replicate_to(lid, fid)
        # Second pass: propagate new commit_index to followers via empty AppendEntries
        for fid in self.nodes:
            if fid != lid:
                self.replicate_to(lid, fid)

    # ---------- client API ----------
    def write(self, key: str, value: str, leader_id: Optional[str] = None) -> int:
        """Write via current leader, replicate, commit. Returns commit index."""
        lid = leader_id or self.leader_id()
        if lid is None:
            raise RuntimeError("no leader: elect one first")
        leader = self.nodes[lid]
        if not leader.is_leader():
            raise RuntimeError(f"{lid} is not leader")
        entry = leader.leader_append({"op": "put", "key": key, "value": value})
        self.replicate_all(lid)
        if leader.commit_index < entry.index:
            # Majority may not have been reached (e.g. partition) — surface it
            raise RuntimeError(
                f"write at index {entry.index} not committed (commit_index={leader.commit_index})"
            )
        return entry.index

    def delete(self, key: str, leader_id: Optional[str] = None) -> int:
        lid = leader_id or self.leader_id()
        if lid is None:
            raise RuntimeError("no leader")
        leader = self.nodes[lid]
        entry = leader.leader_append({"op": "delete", "key": key})
        self.replicate_all(lid)
        return entry.index

    def read(self, key: str, node_id: Optional[str] = None, snapshot: Optional[int] = None) -> Optional[str]:
        """Committed read: defaults to leader's state machine, which only
        applies committed entries, so uncommitted writes are never visible."""
        nid = node_id or self.leader_id()
        if nid is None:
            raise RuntimeError("no node to read from")
        return self.nodes[nid].store.get(key, snapshot=snapshot)

    def snapshot(self, node_id: Optional[str] = None) -> int:
        nid = node_id or self.leader_id()
        if nid is None:
            raise RuntimeError("no leader")
        return self.nodes[nid].store.snapshot()

    def tick(self):
        for n in self.nodes.values():
            n.tick()
