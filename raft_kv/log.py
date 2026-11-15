"""Raft log: entries and log container with consistency helpers."""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class LogEntry:
    index: int          # 1-based
    term: int
    command: dict       # e.g. {"op": "put", "key": "x", "value": "1"} or {"op": "delete", ...}
    def __repr__(self):
        return f"LogEntry(idx={self.index}, term={self.term}, cmd={self.command})"


class RaftLog:
    """Append-only log with Raft truncation semantics."""

    def __init__(self):
        self._entries: list[LogEntry] = []

    def __len__(self):
        return len(self._entries)

    def last_index(self) -> int:
        return len(self._entries)

    def last_term(self) -> int:
        return self._entries[-1].term if self._entries else 0

    def term_at(self, index: int) -> int:
        if index == 0:
            return 0
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1].term
        raise IndexError(f"log index {index} out of range (len={len(self._entries)})")

    def get(self, index: int) -> Optional[LogEntry]:
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1]
        return None

    def entries_from(self, index: int) -> list[LogEntry]:
        if index <= 1:
            return list(self._entries)
        return list(self._entries[index - 1:])

    def all_entries(self) -> list[LogEntry]:
        return list(self._entries)

    def append(self, term: int, command: dict) -> LogEntry:
        entry = LogEntry(index=len(self._entries) + 1, term=term, command=dict(command))
        self._entries.append(entry)
        return entry

    def append_entries(self, prev_index: int, entries: list[LogEntry]) -> bool:
        """Append/overwrite entries after prev_index, Raft style.
        Returns False if there's a conflicting existing entry that doesn't match
        and we cannot resolve (caller should have checked prev term).
        Truncates conflicting suffix then appends new entries."""
        for i, e in enumerate(entries):
            idx = prev_index + 1 + i
            if idx <= len(self._entries):
                existing = self._entries[idx - 1]
                if existing.term != e.term:
                    # conflict: truncate from idx onwards
                    self._entries = self._entries[: idx - 1]
                    self._entries.append(LogEntry(index=idx, term=e.term, command=dict(e.command)))
                # else identical, keep
            else:
                self._entries.append(LogEntry(index=idx, term=e.term, command=dict(e.command)))
        return True

    def truncate_from(self, index: int):
        if 1 <= index <= len(self._entries):
            self._entries = self._entries[: index - 1]

    def is_up_to_date(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        """Raft election restriction: candidate log is at least as up-to-date."""
        my_term = self.last_term()
        if candidate_last_term != my_term:
            return candidate_last_term > my_term
        return candidate_last_index >= self.last_index()
