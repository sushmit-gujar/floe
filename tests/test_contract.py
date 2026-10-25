import json

import pytest

from floe.contract import Contract, ContractViolation
from floe.produce import CORRUPTIONS, corrupt, replay_events


def test_real_sample_conforms(contract, sample):
    ok = 0
    for e in sample:
        e = dict(e, floe_produced_at=1_700_000_000_000)
        contract.validate(e)
        ok += 1
    assert ok == len(sample) > 10_000


def test_schema_has_lineage(contract):
    names = contract.arrow_schema.names
    assert names[0] == "event_id"
    assert {"kafka_partition", "kafka_offset", "batch_id", "ingested_at"} <= set(names)


@pytest.mark.parametrize("kind", CORRUPTIONS)
def test_corruptions_are_rejected(contract, sample, kind):
    e = dict(sample[0], floe_produced_at=1_700_000_000_000)
    raw = corrupt(e, kind)
    with pytest.raises((ContractViolation, ValueError)):
        contract.validate(json.loads(raw))


def test_type_rules(contract, sample):
    base = dict(sample[0], floe_produced_at=1_700_000_000_000)
    with pytest.raises(ContractViolation, match="type:is_bot"):
        contract.validate(dict(base, bot="yes"))
    with pytest.raises(ContractViolation, match="range:namespace"):
        contract.validate(dict(base, namespace=-5))
    with pytest.raises(ContractViolation, match="type:event_time"):
        contract.validate(dict(base, meta=dict(base["meta"], dt="yesterday")))
    row = contract.validate(dict(base, user=None))
    assert row["user_name"] is None


def test_replay_is_deterministic_and_unique(sample):
    a = [(k, kind) for k, _v, kind in replay_events(sample, "t1", 2000, invalid_ratio=0.05, late_ratio=0.02)]
    b = [(k, kind) for k, _v, kind in replay_events(sample, "t1", 2000, invalid_ratio=0.05, late_ratio=0.02)]
    assert a == b
    invalid = sum(1 for _k, kind in a if kind in CORRUPTIONS)
    assert 60 <= invalid <= 140
    ids = {json.loads(v)["meta"]["id"] for _k, v, kind in replay_events(sample, "t1", 2000) if kind == "valid"}
    assert len(ids) == 2000


def test_contract_file_loads_from_path(tmp_path):
    c = Contract.load()
    assert c.topic == "wiki.recentchange.v1"
    assert c.quality["max_reject_rate"] == 0.05
