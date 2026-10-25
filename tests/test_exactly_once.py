"""Exactly-once commit protocol, exercised against a real (SQLite) Iceberg catalog."""

import pytest

from floe import lake
from floe.process import Batcher
from floe.produce import CORRUPTIONS, replay_events


def feed(b: Batcher, sample, run_id: str, n: int, partitions: int = 3, start: dict | None = None, **kw):
    """Simulate Kafka: round-robin events over partitions with contiguous offsets."""
    offs = dict(start or {p: 0 for p in range(partitions)})
    kinds = []
    for i, (_k, v, kind) in enumerate(replay_events(sample, run_id, n, **kw)):
        p = i % partitions
        b.add(p, offs[p], v)
        offs[p] += 1
        kinds.append(kind)
    return offs, kinds


def count_run(cat, run_id):
    from pyiceberg.expressions import StartsWith

    arr = cat.load_table(lake.BRONZE).scan(row_filter=StartsWith("event_id", run_id + "-")).to_arrow()
    ids = arr.column("event_id").to_pylist()
    return len(ids), len(set(ids))


def test_commit_advances_offsets_atomically(cat, contract, sample):
    b = Batcher(contract, cat)
    b.assign([0, 1, 2])
    offs, kinds = feed(b, sample, "a", 3000, invalid_ratio=0.03)
    res = b.flush()
    assert res is not None and res.committed
    valid = sum(k not in CORRUPTIONS for k in kinds)
    assert count_run(cat, "a") == (valid, valid)
    assert lake.read_offsets(cat.load_table(lake.BRONZE)) == offs
    rej = cat.load_table(lake.REJECTED).scan().to_arrow()
    assert rej.num_rows == len(kinds) - valid


def test_restart_resumes_without_loss_or_duplicates(cat, contract, sample):
    b1 = Batcher(contract, cat)
    b1.assign([0, 1])
    feed(b1, sample, "b", 1000, partitions=2)
    assert b1.flush().committed
    # crash: b1 buffered more rows but never committed them
    feed(b1, sample, "c", 500, partitions=2, start={0: 500, 1: 500})
    del b1
    b2 = Batcher(contract, cat)
    stored = b2.assign([0, 1])
    assert stored == {0: 500, 1: 500}  # resume exactly after the last committed batch
    feed(b2, sample, "c", 500, partitions=2, start=stored)
    assert b2.flush().committed
    assert count_run(cat, "b") == (1000, 1000)
    assert count_run(cat, "c") == (500, 500)


def test_zombie_writer_is_fenced(cat, contract, sample):
    owner = Batcher(contract, cat)
    zombie = Batcher(contract, cat)
    owner.assign([0])
    zombie.assign([0])  # both believe they own partition 0 (rebalance race)
    feed(owner, sample, "z", 200, partitions=1)
    feed(zombie, sample, "z", 200, partitions=1)
    assert owner.flush().committed
    res = zombie.flush()
    assert res is not None and not res.committed and res.fenced is not None
    assert res.fenced.found == 200
    assert count_run(cat, "z") == (200, 200)
    # after the fence the zombie re-reads the committed offset and continues correctly
    assert zombie.committed[0] == 200


def test_rejects_only_batch_does_not_advance_offsets(cat, contract, sample):
    b = Batcher(contract, cat)
    b.assign([0])
    b.add(0, 0, b"{broken")
    res = b.flush()
    assert res is not None and not res.committed and res.rejected == 1
    assert lake.read_offsets(cat.load_table(lake.BRONZE)) == {}
    feed(b, sample, "r", 10, partitions=1, start={0: 1})
    assert b.flush().committed
    assert lake.read_offsets(cat.load_table(lake.BRONZE)) == {0: 11}


def test_stale_table_commit_is_rejected_by_catalog(cat, contract, sample):
    """A commit based on an old snapshot must fail rather than be silently rebased."""
    import pyarrow as pa
    from pyiceberg.exceptions import CommitFailedException

    b = Batcher(contract, cat)
    b.assign([0])
    feed(b, sample, "s", 10, partitions=1)
    b.flush()
    stale = cat.load_table(lake.BRONZE)
    b2 = Batcher(contract, cat)
    b2.assign([0])
    feed(b2, sample, "s2", 10, partitions=1, start={0: 10})
    b2.flush()
    empty = pa.Table.from_pylist([], schema=contract.arrow_schema)
    rows = cat.load_table(lake.BRONZE).scan(limit=1).to_arrow()
    with pytest.raises(CommitFailedException):
        with stale.transaction() as tx:
            tx.append(rows if rows.num_rows else empty)
            tx.set_properties({lake.OFFSETS_PROP: '{"0": 1}'})
