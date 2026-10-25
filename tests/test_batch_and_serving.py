"""dbt batch layer, publish gating, verify and the query API against a SQLite Iceberg catalog."""

from fastapi.testclient import TestClient

from floe import lake, transform, verify
from floe.api import create_app
from floe.process import Batcher
from tests.test_exactly_once import feed


def ingest(cat, contract, sample, run_id="t", n=4000, **kw):
    b = Batcher(contract, cat)
    b.assign([0, 1, 2, 3])
    _offs, kinds = feed(b, sample, run_id, n, partitions=4, **kw)
    assert b.flush().committed
    return kinds


def test_transform_builds_tests_and_publishes(cat, contract, sample, settings, tmp_path):
    ingest(cat, contract, sample, invalid_ratio=0.02, late_ratio=0.01)
    out = transform.run(settings, workdir=str(tmp_path / "dbt"))
    assert out["status"] == "pass", out
    assert out["published"] is True
    assert out["tests_failed"] == 0 and out["tests_passed"] >= 15
    assert out["models_ok"] == 7
    gold = cat.load_table("gold.edits_per_minute").scan().to_arrow()
    assert sum(gold.column("edits").to_pylist()) == out["silver_rows"]
    q = cat.load_table("ops.ingest_quality").scan().to_arrow().to_pylist()[0]
    assert q["bronze_duplicates"] == 0
    assert 0 < q["reject_rate"] < 0.05
    assert q["late"] > 0
    runs = cat.load_table("ops.dq_runs").scan().to_arrow()
    assert runs.num_rows == 1


def test_duplicates_block_publishing(cat, contract, sample, settings, tmp_path):
    ingest(cat, contract, sample, run_id="d", n=500)
    # Simulate an at-least-once writer re-appending already committed rows.
    tbl = cat.load_table(lake.BRONZE)
    tbl.append(tbl.scan(limit=50).to_arrow())
    out = transform.run(settings, workdir=str(tmp_path / "dbt"))
    assert out["status"] == "fail"
    assert out["published"] is False
    assert "assert_bronze_exactly_once" in out["failed_tests"]
    assert lake.load_or_none(cat, "gold.edits_per_minute") is None  # nothing reached serving
    assert cat.load_table("ops.dq_runs").scan().to_arrow().num_rows == 1


def test_source_duplicates_are_deduped_not_blocking(cat, contract, sample, settings, tmp_path):
    import pyarrow as pa
    import pyarrow.compute as pc

    ingest(cat, contract, sample, run_id="s", n=500)
    tbl = cat.load_table(lake.BRONZE)
    again = tbl.scan(limit=40).to_arrow()
    # Same events re-delivered by the source => new Kafka records (fresh offsets).
    idx = again.schema.get_field_index("kafka_offset")
    again = again.set_column(idx, again.schema.field(idx), pc.add(again["kafka_offset"], pa.scalar(10_000_000, pa.int64())))
    tbl.append(again)
    out = transform.run(settings, workdir=str(tmp_path / "dbt"))
    assert out["status"] == "pass", out
    assert out["published"] is True
    q = lake.load_or_none(cat, "ops.ingest_quality").scan().to_arrow().to_pylist()[0]
    assert q["bronze_duplicates"] == 0 and q["source_duplicates"] == 40
    assert out["silver_rows"] == tbl.scan().to_arrow().num_rows - 40


def test_reject_rate_breach_fails_contract_test(cat, contract, sample, settings, tmp_path):
    ingest(cat, contract, sample, run_id="x", n=1000, invalid_ratio=0.2)
    out = transform.run(settings, workdir=str(tmp_path / "dbt"))
    assert out["status"] == "fail"
    assert "assert_reject_rate_within_contract" in out["failed_tests"]


def test_verify_reports_exactly_once_and_latency(cat, contract, sample):
    kinds = ingest(cat, contract, sample, run_id="v", n=1200, invalid_ratio=0.05)
    valid = sum(k in ("valid", "late") for k in kinds)
    out = verify.wait_and_verify(cat, "v", valid, len(kinds) - valid, timeout=5, poll=0.1)
    assert out["exactly_once"] and out["rejects_ok"]
    assert out["duplicates"] == 0 and out["missing"] == 0
    assert out["rows_with_commit_ts"] == valid
    assert out["p50_ms"] is not None and out["p50_ms"] >= 0


def test_api_serves_gold(cat, contract, sample, settings, tmp_path):
    ingest(cat, contract, sample, run_id="api", n=2000)
    assert transform.run(settings, workdir=str(tmp_path / "dbt"))["status"] == "pass"
    client = TestClient(create_app(settings))
    assert client.get("/healthz").status_code == 200
    assert client.get("/readyz").json()["tables"] == 5
    s = client.get("/v1/summary").json()
    assert s["last_batch_run"]["status"] == "pass"
    top = s["top_wikis"][0]["wiki"]
    assert client.get(f"/v1/wikis/{top}/edits-per-minute").status_code == 200
    assert client.get("/v1/wikis/nope/edits-per-minute").status_code == 404
    assert len(client.get("/v1/top-pages", params={"limit": 5}).json()) == 5
    m = client.get("/metrics").text
    assert "floe_dq_pass_rate 1.0" in m
