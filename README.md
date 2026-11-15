# Distributed Fault-Tolerant Key-Value Storage Engine Using Raft Consensus & MVCC

An in-process, deterministic simulation of a distributed key-value store that stays
consistent across node failures using the **Raft consensus protocol**, with a
**Multi-Version Concurrency Control (MVCC)** storage engine underneath for snapshot
reads and full version history.

No sockets, no threads, no external services — the whole cluster lives in one Python
process and is driven by explicit method calls (elections, replication rounds, and a
`tick()` hook), which makes every behaviour reproducible in tests.

## What it does

- **Leader election** — a candidate increments its term, votes for itself, and needs a
  majority of votes (with Raft's log up-to-date restriction) to become leader.
- **Log replication** — the leader appends commands to its log and replicates them to
  followers with AppendEntries-style consistency checks (`prev_log_index` /
  `prev_log_term`, conflicting-suffix truncation, and `next_index` backoff on mismatch).
- **Commit & apply** — an entry is committed once a majority holds it and it belongs to
  the leader's current term; committed entries are applied in order to each node's
  state machine. Uncommitted writes are never visible to reads.
- **MVCC store** — every committed write creates a new version instead of overwriting.
  A snapshot captures the current version, so a snapshot read sees a stable view that
  later writes cannot disturb (snapshot isolation). Old versions stay readable, deletes
  are tombstones, and per-key version history is preserved.
- **Failover** — when a leader steps down (or is partitioned away), a new leader can be
  elected at a higher term; previously committed data survives and replication resumes,
  including backfilling a follower that missed writes while partitioned.
- **Partitions** — the cluster harness can isolate a set of nodes to exercise minority /
  majority behaviour (a lone partitioned node cannot win an election or commit writes).

## Stack

- **Python** (pure standard library — no runtime dependencies)
- **pytest** as the test runner (dev/test only)

## Layout

```
raft_kv/
  log.py       LogEntry + RaftLog (append, truncation, up-to-date check)
  mvcc.py      MVCCStore (versions, snapshots, tombstone deletes, history)
  node.py      RaftNode state machine (follower / candidate / leader)
  cluster.py   Cluster harness (election, replication, partitions, client API)
main.py        Runnable demo
tests/         pytest suite
```

## How to run

```bash
cd raft-mvcc-kv-store
python main.py
```

The demo starts a 3-node cluster, elects `n1`, replicates a few writes, takes an MVCC
snapshot and shows it is unaffected by a later write, then steps the leader down,
elects `n2`, and writes through the new leader.

Quick interactive use:

```python
from raft_kv import Cluster

cluster = Cluster(size=3)
cluster.elect("n1")
cluster.write("user:1", "alice")

snap = cluster.snapshot()          # MVCC snapshot
cluster.write("user:1", "alice-2")

cluster.read("user:1")                  # 'alice-2'
cluster.read("user:1", snapshot=snap)   # 'alice'  (snapshot isolation)
```

## How to test

```bash
cd raft-mvcc-kv-store
python -m pytest
```

The suite covers leader election (including majority-under-partition and term
progression), log replication to followers (3- and 5-node clusters, plus catch-up after
a partition heals), committed reads (including that uncommitted writes are invisible),
MVCC snapshot isolation, old-version reads and version history, and leader step-down
with re-election and continued writes.
