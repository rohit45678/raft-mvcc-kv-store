"""MVCC key-value store with snapshot reads and version history.

Each committed write creates a new version. A global monotonically
increasing version counter (starting at 0) stamps every write.
Snapshots capture the current version; reads at a snapshot see only
versions <= snapshot, giving snapshot isolation from later writes.
Deletes are tombstones.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


@dataclass
class VersionedValue:
    version: int
    value: Optional[str]  # None = tombstone / deleted
    commit_index: int = 0


class MVCCStore:
    def __init__(self):
        self._data: dict[str, list[VersionedValue]] = {}
        self._version: int = 0

    @property
    def current_version(self) -> int:
        return self._version

    def snapshot(self) -> int:
        """Return a snapshot handle (just the current version number)."""
        return self._version

    def put(self, key: str, value: str, commit_index: int = 0) -> int:
        self._version += 1
        vv = VersionedValue(version=self._version, value=value, commit_index=commit_index)
        self._data.setdefault(key, []).append(vv)
        return self._version

    def delete(self, key: str, commit_index: int = 0) -> int:
        self._version += 1
        vv = VersionedValue(version=self._version, value=None, commit_index=commit_index)
        self._data.setdefault(key, []).append(vv)
        return self._version

    def apply_command(self, command: dict, commit_index: int = 0) -> int:
        op = command.get("op")
        if op == "put":
            return self.put(command["key"], command["value"], commit_index=commit_index)
        elif op == "delete":
            return self.delete(command["key"], commit_index=commit_index)
        elif op == "noop":
            # noop advances version? No — noop should not create a data version.
            # It still counts as applied but leaves store unchanged.
            return self._version
        else:
            raise ValueError(f"unknown command op: {op!r}")

    def get(self, key: str, snapshot: Optional[int] = None) -> Optional[str]:
        """Read latest version <= snapshot (or latest overall if snapshot is None)."""
        versions = self._data.get(key)
        if not versions:
            return None
        limit = snapshot if snapshot is not None else self._version
        # versions are in increasing order
        result: Optional[str] = None
        found = False
        for vv in versions:
            if vv.version <= limit:
                result = vv.value
                found = True
            else:
                break
        return result if found else None

    def get_at_version(self, key: str, version: int) -> Optional[str]:
        return self.get(key, snapshot=version)

    def history(self, key: str) -> list[VersionedValue]:
        return list(self._data.get(key, []))

    def keys_at_snapshot(self, snapshot: Optional[int] = None) -> list[str]:
        limit = snapshot if snapshot is not None else self._version
        out = []
        for k in self._data:
            if self.get(k, snapshot=limit) is not None:
                out.append(k)
        return sorted(out)

    def __repr__(self):
        return f"MVCCStore(version={self._version}, keys={list(self._data.keys())})"
