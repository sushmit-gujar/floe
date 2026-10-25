"""Verify a produced run landed exactly once, and measure end-to-end latency/throughput.

Latency for a row = commit time of the Iceberg snapshot that made it visible (catalog-
assigned ``timestamp-ms``; the processor tags each snapshot with its ``batch_id``) minus the
producer timestamp carried in the event. Producer and catalog share a clock on one node.
"""

from __future__ import annotations

import time
from typing import Any

import duckdb
import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import StartsWith

from floe import lake


def _scan_run(cat: Catalog, run_id: str) -> pa.Table:
    tbl = cat.load_table(lake.BRONZE)
    return tbl.scan(
        row_filter=StartsWith("event_id", run_id + "-"),
        selected_fields=("event_id", "produced_at", "batch_id", "kafka_partition", "kafka_offset"),
    ).to_arrow()


def _rejected_for_run(cat: Catalog, run_id: str) -> tuple[int, int]:
    tbl = cat.load_table(lake.REJECTED)
    arr = tbl.scan(selected_fields=("kafka_partition", "kafka_offset", "payload")).to_arrow()
    con = duckdb.connect()
    con.register("rej", arr)
    row = con.execute(
        "select count(*), count(distinct (kafka_partition, kafka_offset)) from rej where payload like ?",
        [f"%{run_id}-%"],
    ).fetchall()[0]
    assert row is not None
    return int(row[0]), int(row[1])


def wait_and_verify(
    cat: Catalog,
    run_id: str,
    expect_valid: int,
    expect_rejected: int | None = None,
    timeout: float = 600,
    poll: float = 2.0,
    produced_from: float | None = None,
) -> dict[str, Any]:
    t0 = time.time()
    arr = _scan_run(cat, run_id)
    while arr.num_rows < expect_valid and time.time() - t0 < timeout:
        time.sleep(poll)
        arr = _scan_run(cat, run_id)
    waited = time.time() - t0
    # Give at-least-once duplicates (if any) a chance to show up before judging.
    time.sleep(min(poll * 2, 5))
    arr = _scan_run(cat, run_id)
    tbl = cat.load_table(lake.BRONZE)
    snap_ts = {
        s.summary.additional_properties.get(lake.BATCH_PROP): s.timestamp_ms
        for s in tbl.metadata.snapshots
        if s.summary is not None and s.summary.additional_properties.get(lake.BATCH_PROP)
    }
    snaps = pa.table({"batch_id": list(snap_ts.keys()), "commit_ms": list(snap_ts.values())})
    con = duckdb.connect()
    con.register("rows", arr)
    con.register("snaps", snaps)
    q = con.execute(
        """
        with r as (
          select r.event_id, epoch_ms(r.produced_at) as produced_ms, s.commit_ms, r.batch_id
          from rows r left join snaps s using (batch_id)
        )
        select count(*) as rows,
               count(distinct event_id) as distinct_ids,
               count(distinct batch_id) as batches,
               count(commit_ms) as rows_with_commit_ts,
               quantile_cont(commit_ms - produced_ms, 0.50) as p50_ms,
               quantile_cont(commit_ms - produced_ms, 0.95) as p95_ms,
               quantile_cont(commit_ms - produced_ms, 0.99) as p99_ms,
               max(commit_ms - produced_ms) as max_ms,
               min(produced_ms) as first_produced_ms,
               max(commit_ms) as last_commit_ms
        from r
        """
    ).fetchall()[0]
    assert q is not None
    cols = [
        "rows",
        "distinct_ids",
        "batches",
        "rows_with_commit_ts",
        "p50_ms",
        "p95_ms",
        "p99_ms",
        "max_ms",
        "first_produced_ms",
        "last_commit_ms",
    ]
    out: dict[str, Any] = dict(zip(cols, q))
    for k in ("p50_ms", "p95_ms", "p99_ms"):
        if out[k] is not None:
            out[k] = round(float(out[k]), 1)
    out["run_id"] = run_id
    out["expected_valid"] = expect_valid
    out["duplicates"] = out["rows"] - out["distinct_ids"]
    out["missing"] = max(expect_valid - out["distinct_ids"], 0)
    out["exactly_once"] = out["rows"] == out["distinct_ids"] == expect_valid
    out["waited_s"] = round(waited, 1)
    if out["first_produced_ms"] and out["last_commit_ms"]:
        start = produced_from * 1000 if produced_from else out["first_produced_ms"]
        span = (out["last_commit_ms"] - start) / 1000
        out["e2e_span_s"] = round(span, 3)
        out["e2e_events_per_s"] = round(out["rows"] / span, 1) if span > 0 else None
    if expect_rejected is not None:
        raw, distinct = _rejected_for_run(cat, run_id)
        out["rejected_rows"] = raw
        out["rejected_distinct"] = distinct
        out["expected_rejected"] = expect_rejected
        out["rejects_ok"] = distinct == expect_rejected
    return out
