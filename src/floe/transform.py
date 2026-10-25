"""Batch layer: dbt (DuckDB engine) over Iceberg bronze -> silver/gold, gated on data tests.

1. Copy the packaged dbt project to a writable temp dir and render profiles.yml.
   Stage the bronze lookback window from Iceberg into the DuckDB working file by streaming
   Arrow record batches (the row filter prunes Iceberg files). DuckDB runs under a memory
   limit and spills to local disk, so memory stays bounded however much data is in the window.
2. ``dbt build`` = models + data tests (schema tests and contract tests in ``tests/``).
3. Only if every error-severity test passes, publish gold/ops tables back to Iceberg
   (``gold.*``) for the serving layer. Broken data never reaches serving.
4. Always append the outcome to ``ops.dq_runs`` (the DQ pass-rate panel reads it).
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import yaml
from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import NoSuchTableError

from floe import lake
from floe.config import Settings
from floe.contract import Contract

log = logging.getLogger("floe.transform")

PUBLISH = {
    "gold.edits_per_minute": "gold.edits_per_minute",
    "gold.wiki_leaderboard": "gold.wiki_leaderboard",
    "gold.top_pages": "gold.top_pages",
    "ops.ingest_quality": "ops.ingest_quality",
}

DQ_SCHEMA = pa.schema(
    [
        pa.field("run_id", pa.string(), nullable=False),
        pa.field("started_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("finished_at", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("status", pa.string(), nullable=False),
        pa.field("models_ok", pa.int32(), nullable=False),
        pa.field("models_failed", pa.int32(), nullable=False),
        pa.field("tests_passed", pa.int32(), nullable=False),
        pa.field("tests_failed", pa.int32(), nullable=False),
        pa.field("tests_warned", pa.int32(), nullable=False),
        pa.field("failed_tests", pa.string(), nullable=True),
        pa.field("silver_rows", pa.int64(), nullable=True),
        pa.field("published", pa.bool_(), nullable=False),
        pa.field("seconds", pa.float64(), nullable=False),
    ]
)


def prepare_project(s: Settings, workdir: Path) -> Path:
    src = resources.files("floe").joinpath("dbt_project")
    dst = workdir / "dbt_project"
    shutil.copytree(str(src), dst, dirs_exist_ok=True)
    profile = {
        "floe": {
            "target": "lake",
            "outputs": {
                "lake": {
                    "type": "duckdb",
                    "path": str(workdir / "floe.duckdb"),
                    "threads": 1,
                    "settings": {
                        "memory_limit": s.duckdb_memory_limit,
                        "preserve_insertion_order": False,
                        "threads": s.duckdb_threads,
                    },
                }
            },
        }
    }
    (dst / "profiles.yml").write_text(yaml.safe_dump(profile), encoding="utf-8")
    return dst


def stage_bronze(cat: Catalog, s: Settings, db_path: Path, since: str) -> dict[str, int]:
    """Stream bronze tables (lookback window only) from Iceberg into the DuckDB file."""
    from pyiceberg.expressions import GreaterThanOrEqual

    con = duckdb.connect(str(db_path))
    counts: dict[str, int] = {}
    try:
        con.execute(f"set memory_limit='{s.duckdb_memory_limit}'")
        con.execute("set preserve_insertion_order=false")
        con.execute(f"set threads={s.duckdb_threads}")
        con.execute("create schema if not exists bronze")
        for ident, col in (("bronze.wiki_edits", "event_time"), ("bronze.wiki_edits_rejected", "ingested_at")):
            tbl = cat.load_table(ident)
            reader = tbl.scan(row_filter=GreaterThanOrEqual(col, since)).to_arrow_batch_reader()  # noqa: F841 (used by DuckDB)
            con.execute(f"create or replace table {ident} as select * from reader")
            counts[ident] = int(con.execute(f"select count(*) from {ident}").fetchall()[0][0])
    finally:
        con.close()
    return counts


def run(s: Settings, workdir: str | None = None, publish: bool = True) -> dict[str, Any]:
    import os

    from dbt.cli.main import dbtRunner

    started = datetime.now(UTC)
    t0 = time.monotonic()
    contract = Contract.load()
    q = contract.quality
    since = (started - timedelta(hours=s.transform_lookback_hours)).isoformat()
    os.environ.setdefault("DBT_SEND_ANONYMOUS_USAGE_STATS", "false")
    work = Path(workdir or tempfile.mkdtemp(prefix="floe-dbt-"))
    os.environ.setdefault("DBT_LOG_PATH", str(work / "logs"))
    os.environ.setdefault("DBT_TARGET_PATH", str(work / "target"))
    project = prepare_project(s, work)
    cat = lake.catalog(s)
    staged = stage_bronze(cat, s, work / "floe.duckdb", since)
    log.info("staged %s", staged)
    dbt_vars = {
        "max_reject_rate": q.get("max_reject_rate", 0.05),
        "freshness_sla_seconds": q.get("freshness_sla_seconds", 900),
        "late_after_seconds": q.get("late_after_seconds", 600),
    }
    res = dbtRunner().invoke(
        [
            "build",
            "--project-dir",
            str(project),
            "--profiles-dir",
            str(project),
            "--vars",
            json.dumps(dbt_vars),
            "--no-use-colors",
            "--log-format",
            "text",
        ]
    )
    summary = _summarise(res)
    silver_rows = None
    db = work / "floe.duckdb"
    published = False
    if db.exists():
        con = duckdb.connect(str(db))
        try:
            silver_rows = int(con.execute("select count(*) from silver.wiki_edits").fetchall()[0][0])  # type: ignore[index]
        except duckdb.Error:
            silver_rows = None
        if publish and summary["status"] == "pass":
            for src_rel, ident in PUBLISH.items():
                _publish(cat, ident, con.sql(f"select * from {src_rel}").to_arrow_table())
            published = True
        con.close()
    finished = datetime.now(UTC)
    record = {
        "run_id": uuid.uuid4().hex,
        "started_at": started,
        "finished_at": finished,
        "status": summary["status"],
        "models_ok": summary["models_ok"],
        "models_failed": summary["models_failed"],
        "tests_passed": summary["tests_passed"],
        "tests_failed": summary["tests_failed"],
        "tests_warned": summary["tests_warned"],
        "failed_tests": ",".join(summary["failed_tests"]) or None,
        "silver_rows": silver_rows,
        "published": published,
        "seconds": round(time.monotonic() - t0, 3),
    }
    _append_dq(cat, record)
    out = dict(record)
    out["started_at"] = started.isoformat()
    out["finished_at"] = finished.isoformat()
    return out


def _summarise(res: Any) -> dict[str, Any]:
    models_ok = models_failed = passed = failed = warned = 0
    failed_tests: list[str] = []
    results = getattr(res.result, "results", None) or []
    for r in results:
        node = r.node
        status = str(r.status).lower().split(".")[-1]
        if node.resource_type == "test":
            if status == "pass":
                passed += 1
            elif status == "warn":
                warned += 1
            else:
                failed += 1
                failed_tests.append(node.name)
        elif node.resource_type == "model":
            if status == "success":
                models_ok += 1
            else:
                models_failed += 1
    ok = bool(res.success) and failed == 0 and models_failed == 0 and (models_ok + passed) > 0
    return {
        "status": "pass" if ok else "fail",
        "models_ok": models_ok,
        "models_failed": models_failed,
        "tests_passed": passed,
        "tests_failed": failed,
        "tests_warned": warned,
        "failed_tests": failed_tests,
        "exception": repr(res.exception) if res.exception else None,
    }


def _publish(cat: Catalog, ident: str, data: pa.Table) -> None:
    data = _normalise(data)
    try:
        tbl = cat.load_table(ident)
    except NoSuchTableError:
        tbl = cat.create_table(ident, schema=data.schema)
    else:
        if tbl.schema().as_arrow().names != data.schema.names:
            with tbl.update_schema() as upd:
                upd.union_by_name(data.schema)
            tbl = cat.load_table(ident)
    tbl.overwrite(data)


def _normalise(data: pa.Table) -> pa.Table:
    """Iceberg has no unsigned ints / ns timestamps / large strings: cast to portable types."""
    fields = []
    for f in data.schema:
        t = f.type
        if pa.types.is_unsigned_integer(t) or (pa.types.is_integer(t) and t.bit_width < 32):
            t = pa.int64() if t.bit_width >= 32 else pa.int32()
        elif pa.types.is_timestamp(t):
            t = pa.timestamp("us", tz="UTC")
        elif pa.types.is_large_string(t):
            t = pa.string()
        elif pa.types.is_decimal(t):
            t = pa.float64()
        fields.append(pa.field(f.name, t, nullable=True))
    return data.cast(pa.schema(fields))


def _append_dq(cat: Catalog, record: dict[str, Any]) -> None:
    try:
        tbl = cat.load_table("ops.dq_runs")
    except NoSuchTableError:
        tbl = cat.create_table("ops.dq_runs", schema=DQ_SCHEMA)
    tbl.append(pa.Table.from_pylist([record], schema=DQ_SCHEMA))
