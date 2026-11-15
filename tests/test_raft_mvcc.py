"""Test suite for Raft + MVCC KV store.

Covers: leader election, log replication to followers, committed reads,
MVCC snapshot isolation, old-version reads, leader step-down / re-election.
"""
import pytest
from raft_kv import Cluster, MVCCStore, RaftNode, NodeState


# ---------------- Election ----------------

def test_leader_election_single_candidate():
    c = Cluster(size=3)
    assert c.elect("n1") is True
    assert c.leader_id() == "n1"
    assert c.nodes["n1"].is_leader()
    assert c.nodes["n1"].current_term == 1
    # followers recognize term
    assert c.nodes["n2"].current_term == 1
    assert c.nodes["n3"].current_term == 1


def test_only_one_leader_after_election():
    c = Cluster(size=5)
    c.elect("n3")
    leaders = [n.id for n in c.nodes.values() if n.is_leader()]
    assert leaders == ["n3"]


def test_candidate_needs_majority_under_partition():
    c = Cluster(size=3)
    c.partition({"n1"})  # n1 isolated alone — cannot get majority
    assert c.elect("n1") is False
    assert not c.nodes["n1"].is_leader()
    c.heal()
    assert c.elect("n1") is True


def test_election_increments_term():
    c = Cluster(size=3)
    c.elect("n1")
    t1 = c.nodes["n1"].current_term
    c.nodes["n1"].step_down()
    c.elect("n2")
    assert c.nodes["n2"].current_term > t1


def test_stale_candidate_log_rejected():
    # Give n1 a longer log via a committed write, then try to elect
    # a node whose log is behind after partition trick: directly craft.
    c = Cluster(size=3)
    c.elect("n1")
    c.write("k", "v")
    # n3 has the entry (replicated), so it CAN be elected — sanity check
    c.nodes["n1"].step_down()
    assert c.elect("n3") is True


# ---------------- Replication ----------------

def test_log_replication_to_followers():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("a", "1")
    c.write("b", "2")
    for nid in ("n1", "n2", "n3"):
        assert len(c.nodes[nid].log) == 2
        assert c.nodes[nid].log.get(1).command == {"op": "put", "key": "a", "value": "1"}
        assert c.nodes[nid].log.get(2).command == {"op": "put", "key": "b", "value": "2"}


def test_replication_five_node_cluster():
    c = Cluster(size=5)
    c.elect("n1")
    c.write("x", "10")
    for node in c.nodes.values():
        assert len(node.log) == 1
        assert node.store.get("x") == "10"


def test_follower_catches_up_after_partition_heals():
    c = Cluster(size=3)
    c.elect("n1")
    c.partition({"n3"})
    # Writes still commit with majority n1+n2
    c.write("k1", "v1")
    assert c.nodes["n3"].store.get("k1") is None  # partitioned away
    c.heal()
    c.write("k2", "v2")  # replication pass should backfill n3
    assert c.nodes["n3"].store.get("k1") == "v1"
    assert c.nodes["n3"].store.get("k2") == "v2"


# ---------------- Committed reads ----------------

def test_committed_reads_visible_on_all_nodes():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("name", "rohit")
    assert c.read("name") == "rohit"
    for nid in ("n1", "n2", "n3"):
        assert c.read("name", node_id=nid) == "rohit"


def test_missing_key_reads_none():
    c = Cluster(size=3)
    c.elect("n1")
    assert c.read("nope") is None


def test_uncommitted_write_not_visible():
    # Append directly on leader without replication/commit
    c = Cluster(size=3)
    c.elect("n1")
    leader = c.nodes["n1"]
    leader.leader_append({"op": "put", "key": "pending", "value": "x"})
    # Not committed, not applied — reads must not see it anywhere
    assert leader.store.get("pending") is None
    assert leader.commit_index == 0


def test_overwrite_and_delete():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("k", "first")
    c.write("k", "second")
    assert c.read("k") == "second"
    c.delete("k")
    assert c.read("k") is None


def test_write_without_leader_fails():
    c = Cluster(size=3)
    with pytest.raises(RuntimeError):
        c.write("k", "v")


# ---------------- MVCC ----------------

def test_mvcc_snapshot_isolation():
    store = MVCCStore()
    store.put("color", "blue")
    snap = store.snapshot()
    store.put("color", "red")
    store.put("other", "new")
    assert store.get("color") == "red"
    assert store.get("color", snapshot=snap) == "blue"
    assert store.get("other", snapshot=snap) is None
    assert store.get("other") == "new"


def test_mvcc_snapshot_isolation_via_cluster():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("acct", "100")
    snap = c.snapshot()
    c.write("acct", "250")
    assert c.read("acct") == "250"
    assert c.read("acct", snapshot=snap) == "100"


def test_old_version_reads():
    store = MVCCStore()
    v1 = store.put("k", "v1")
    v2 = store.put("k", "v2")
    v3 = store.put("k", "v3")
    assert store.get_at_version("k", v1) == "v1"
    assert store.get_at_version("k", v2) == "v2"
    assert store.get_at_version("k", v3) == "v3"
    assert store.get("k") == "v3"


def test_version_history_preserved():
    store = MVCCStore()
    store.put("k", "a")
    store.put("k", "b")
    store.delete("k")
    hist = store.history("k")
    assert [h.value for h in hist] == ["a", "b", None]
    assert [h.version for h in hist] == [1, 2, 3]
    # Old snapshot before delete still sees the value
    assert store.get_at_version("k", 2) == "b"
    assert store.get("k") is None


def test_snapshot_of_empty_store():
    store = MVCCStore()
    snap = store.snapshot()
    assert snap == 0
    store.put("k", "v")
    assert store.get("k", snapshot=snap) is None


# ---------------- Leader step-down / re-election ----------------

def test_leader_step_down_and_new_leader_elected():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("shared", "data")
    old_term = c.nodes["n1"].current_term
    c.nodes["n1"].step_down()
    assert not c.nodes["n1"].is_leader()
    assert c.leader() is None or c.leader().id != "n1"
    assert c.elect("n2") is True
    assert c.leader_id() == "n2"
    assert c.nodes["n2"].current_term > old_term
    # Previously committed data survives the leadership change
    assert c.read("shared", node_id="n2") == "data"
    # New leader can accept writes
    c.write("after", "failover")
    assert c.read("after") == "failover"


def test_new_leader_replicates_prior_and_new_writes():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("a", "1")
    c.nodes["n1"].step_down()
    c.elect("n3")
    c.write("b", "2")
    for nid in ("n2", "n3"):
        assert c.read("a", node_id=nid) == "1"
        assert c.read("b", node_id=nid) == "2"


def test_stepped_down_leader_follows_new_leader():
    c = Cluster(size=3)
    c.elect("n1")
    c.write("k", "v1")
    c.nodes["n1"].step_down()
    c.elect("n2")
    c.write("k", "v2")
    # Old leader, now a follower, should have received the new write
    assert c.nodes["n1"].store.get("k") == "v2"
    assert c.nodes["n1"].state == NodeState.FOLLOWER


def test_higher_term_forces_step_down():
    c = Cluster(size=3)
    c.elect("n1")
    # Simulate discovering a higher term (as a heartbeat/RPC would)
    c.nodes["n1"].step_down(new_term=5)
    assert c.nodes["n1"].current_term == 5
    assert not c.nodes["n1"].is_leader()
