"""Serving layer: a read-only query API over the gold Iceberg tables (DuckDB engine)."""

from __future__ import annotations

import threading
import time
from typing import Any

import duckdb
import pyarrow as pa
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import Response
from prometheus_client import CONTENT_TYPE_LATEST, Gauge, generate_latest
from pyiceberg.exceptions import NoSuchTableError

from floe import lake
from floe.config import Settings

TABLES = ("gold.edits_per_minute", "gold.wiki_leaderboard", "gold.top_pages", "ops.ingest_quality", "ops.dq_runs")

dq_pass_rate = Gauge("floe_dq_pass_rate", "Share of dbt data tests passing in the latest batch run")
dq_tests_failed = Gauge("floe_dq_tests_failed", "Failed dbt data tests in the latest batch run")
dq_tests_total = Gauge("floe_dq_tests_total", "dbt data tests in the latest batch run")
dq_last_run = Gauge("floe_dq_last_run_timestamp_seconds", "Finish time of the latest batch run")
dq_runs_passing = Gauge("floe_dq_runs_passing_ratio", "Share of the last 20 batch runs that passed all tests")
gold_freshness = Gauge("floe_gold_max_event_timestamp_seconds", "Newest event_time visible in gold")
reject_rate = Gauge("floe_ingest_reject_rate", "Contract reject rate over the batch lookback window")


class Lake:
    """Caches gold tables as Arrow for ``ttl`` seconds; DuckDB queries them in memory."""

    def __init__(self, s: Settings, ttl: float = 15.0):
        self.s = s
        self.ttl = ttl
        self._lock = threading.Lock()
        self._at = 0.0
        self._tables: dict[str, pa.Table] = {}
        self._cat = None

    def tables(self) -> dict[str, pa.Table]:
        with self._lock:
            if time.monotonic() - self._at > self.ttl:
                if self._cat is None:
                    self._cat = lake.catalog(self.s)
                fresh = {}
                for ident in TABLES:
                    try:
                        fresh[ident] = self._cat.load_table(ident).scan().to_arrow()
                    except NoSuchTableError:
                        continue
                self._tables = fresh
                self._at = time.monotonic()
            return self._tables

    def query(self, sql: str, params: list[Any] | None = None) -> list[dict[str, Any]]:
        tabs = self.tables()
        con = duckdb.connect()
        for ident, arr in tabs.items():
            con.register(ident.replace(".", "_"), arr)
        try:
            cur = con.execute(sql, params or [])
        except duckdb.CatalogException as exc:
            raise HTTPException(503, "gold tables not published yet") from exc
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]


def create_app(s: Settings | None = None) -> FastAPI:
    s = s or Settings()
    lk = Lake(s)
    app = FastAPI(title="floe query API", version="0.1.0")

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, Any]:
        try:
            n = len(lk.tables())
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(503, f"catalog unavailable: {type(exc).__name__}") from exc
        return {"status": "ready", "tables": n}

    @app.get("/v1/summary")
    def summary() -> dict[str, Any]:
        top = lk.query("select * from gold_wiki_leaderboard order by edits desc limit 10")
        quality = lk.query("select * from ops_ingest_quality")
        last = lk.query("select * from ops_dq_runs order by finished_at desc limit 1")
        return {"top_wikis": top, "ingest_quality": quality[0] if quality else None, "last_batch_run": last[0] if last else None}

    @app.get("/v1/wikis")
    def wikis(limit: int = Query(50, ge=1, le=1000)) -> list[dict[str, Any]]:
        return lk.query("select * from gold_wiki_leaderboard order by edits desc limit ?", [limit])

    @app.get("/v1/wikis/{wiki}/edits-per-minute")
    def per_minute(wiki: str, minutes: int = Query(60, ge=1, le=1440)) -> list[dict[str, Any]]:
        rows = lk.query("select * from gold_edits_per_minute where wiki = ? order by event_minute desc limit ?", [wiki, minutes])
        if not rows:
            raise HTTPException(404, f"no data for wiki {wiki!r}")
        return rows

    @app.get("/v1/top-pages")
    def top_pages(wiki: str | None = None, limit: int = Query(20, ge=1, le=100)) -> list[dict[str, Any]]:
        if wiki:
            return lk.query("select * from gold_top_pages where wiki = ? order by rank limit ?", [wiki, limit])
        return lk.query("select * from gold_top_pages order by rank limit ?", [limit])

    @app.get("/v1/quality")
    def quality(limit: int = Query(20, ge=1, le=500)) -> list[dict[str, Any]]:
        return lk.query("select * from ops_dq_runs order by finished_at desc limit ?", [limit])

    @app.get("/metrics")
    def prom() -> Response:
        try:
            _refresh_gauges(lk)
        except Exception:  # noqa: BLE001 - metrics must never 500
            pass
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def _refresh_gauges(lk: Lake) -> None:
    runs = lk.query("select * from ops_dq_runs order by finished_at desc limit 20")
    if runs:
        last = runs[0]
        total = last["tests_passed"] + last["tests_failed"] + last["tests_warned"]
        dq_tests_total.set(total)
        dq_tests_failed.set(last["tests_failed"])
        dq_pass_rate.set(last["tests_passed"] / total if total else 0)
        dq_last_run.set(last["finished_at"].timestamp())
        dq_runs_passing.set(sum(r["status"] == "pass" for r in runs) / len(runs))
    q = lk.query("select * from ops_ingest_quality")
    if q:
        if q[0]["max_event_time"] is not None:
            gold_freshness.set(q[0]["max_event_time"].timestamp())
        reject_rate.set(float(q[0]["reject_rate"] or 0))
