#!/usr/bin/env python3
"""Demo: a 3-node in-process Raft cluster elects a leader and replicates writes,
with MVCC snapshot reads showing isolation from later writes.

Run:  python main.py
"""
from raft_kv import Cluster


def main():
    print("=== Raft + MVCC Key-Value Store Demo ===\n")
    cluster = Cluster(size=3)
    print(f"Cluster nodes: {list(cluster.nodes.keys())}")

    print("\n-- Election --")
    ok = cluster.elect("n1")
    print(f"n1 election succeeded: {ok}, leader = {cluster.leader_id()}, term = {cluster.nodes['n1'].current_term}")

    print("\n-- Writes (replicated + committed) --")
    cluster.write("user:1", "alice")
    cluster.write("user:2", "bob")
    cluster.write("config:mode", "production")
    for nid, node in cluster.nodes.items():
        print(f"  {nid}: log_len={len(node.log)} commit={node.commit_index} applied={node.last_applied}")

    print("\n-- Committed reads --")
    print(f"  user:1       = {cluster.read('user:1')!r}")
    print(f"  user:2       = {cluster.read('user:2')!r}")
    print(f"  config:mode  = {cluster.read('config:mode')!r}")

    print("\n-- MVCC snapshot isolation --")
    snap = cluster.snapshot()
    print(f"  snapshot taken at version {snap}")
    cluster.write("user:1", "alice-updated")
    print(f"  current read of user:1        = {cluster.read('user:1')!r}")
    print(f"  snapshot read of user:1       = {cluster.read('user:1', snapshot=snap)!r}  (unchanged)")
    print(f"  history of user:1: {[(v.version, v.value) for v in cluster.nodes[cluster.leader_id()].store.history('user:1')]}")

    print("\n-- Leader step-down & re-election --")
    old = cluster.leader_id()
    cluster.nodes[old].step_down()
    print(f"  {old} stepped down; leader now = {cluster.leader_id()}")
    cluster.elect("n2")
    print(f"  new leader = {cluster.leader_id()}, term = {cluster.nodes['n2'].current_term}")
    cluster.write("user:3", "carol")
    print(f"  after re-election write, user:3 = {cluster.read('user:3')!r} (read from new leader)")
    print(f"  user:1 on new leader = {cluster.read('user:1', node_id='n2')!r}")

    print("\nDemo complete.")


if __name__ == "__main__":
    main()
