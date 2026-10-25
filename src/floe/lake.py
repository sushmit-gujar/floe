"""Iceberg catalog access, table bootstrap and the exactly-once commit protocol.

Exactly-once design
-------------------
Kafka offsets are stored *in the Iceberg table* (table property ``floe.kafka.offsets``)
and updated in the same atomic catalog commit as the data append. Bronze tables set
``commit.retry.num-retries=0`` so pyiceberg never silently rebases a stale append: a
commit based on an old snapshot is rejected by the catalog. The processor then reloads
the table and re-checks that the stored offset for every partition in the batch still
equals the offset the batch started from. If another consumer (for example a zombie
replica after a rebalance) already committed that range, the batch is dropped and the
consumer re-seeks. Kafka consumer-group offsets are committed afterwards only for lag
visibility (KEDA scales on them); they are never used for recovery.
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import (
    CommitFailedException,
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform

from floe.config import Settings
from floe.contract import REJECT_SCHEMA, Contract

log = logging.getLogger(__name__)

OFFSETS_PROP = "floe.kafka.offsets"
BATCH_PROP = "floe.batch_id"
BRONZE = "bronze.wiki_edits"
REJECTED = "bronze.wiki_edits_rejected"
NAMESPACES = ("bronze", "silver", "gold", "ops")
BRONZE_PROPS = {
    "commit.retry.num-retries": "0",  # we retry ourselves after re-validating offsets
    "write.parquet.compression-codec": "zstd",
    "floe.contract": "wiki_edits.v1",
}


class Fenced(Exception):
    """Another writer already committed (part of) this offset range."""

    def __init__(self, partition: int, expected: int | None, found: int | None):
        super().__init__(f"partition {partition}: expected start {expected}, table has {found}")
        self.partition = partition
        self.expected = expected
        self.found = found


def catalog(s: Settings) -> Catalog:
    if s.catalog_uri.startswith("sqlite"):
        # Local/test mode: SQL catalog + local filesystem warehouse.
        return load_catalog("floe", type="sql", uri=s.catalog_uri, warehouse=s.warehouse)
    return load_catalog("floe", type="rest", uri=s.catalog_uri, warehouse=s.warehouse)


def ensure_tables(cat: Catalog, contract: Contract) -> None:
    for ns in NAMESPACES:
        try:
            cat.create_namespace(ns)
        except NamespaceAlreadyExistsError:
            pass
    try:
        tbl = cat.create_table(BRONZE, schema=contract.arrow_schema, properties=BRONZE_PROPS)
        with tbl.update_spec() as spec:
            spec.add_field("event_time", DayTransform(), "event_day")
        log.info("created %s", BRONZE)
    except TableAlreadyExistsError:
        pass
    try:
        tbl = cat.create_table(REJECTED, schema=REJECT_SCHEMA, properties={"write.parquet.compression-codec": "zstd"})
        log.info("created %s", REJECTED)
    except TableAlreadyExistsError:
        pass


def read_offsets(tbl: Table) -> dict[int, int]:
    raw = tbl.properties.get(OFFSETS_PROP)
    if not raw:
        return {}
    return {int(k): int(v) for k, v in json.loads(raw).items()}


def commit_batch(
    cat: Catalog,
    data: pa.Table,
    ranges: dict[int, tuple[int | None, int]],
    batch_id: str,
    max_attempts: int = 8,
) -> tuple[Table, int]:
    """Append ``data`` and advance offsets atomically. Returns (table, attempts).

    ``ranges`` maps partition -> (start, next): ``start`` is the offset this writer believes
    is currently committed for the partition (None if it found no committed offset), ``next``
    the offset after the last message in the batch.
    """
    attempt = 0
    while True:
        attempt += 1
        tbl = cat.load_table(BRONZE)
        committed = read_offsets(tbl)
        for p, (start, _nxt) in ranges.items():
            found = committed.get(p)
            if found != start:
                raise Fenced(p, start, found)
        new = dict(committed)
        for p, (_start, nxt) in ranges.items():
            new[p] = nxt
        try:
            with tbl.transaction() as tx:
                tx.append(data, snapshot_properties={BATCH_PROP: batch_id})
                tx.set_properties({OFFSETS_PROP: json.dumps({str(k): v for k, v in sorted(new.items())})})
            return tbl, attempt
        except CommitFailedException:
            if attempt >= max_attempts:
                raise
            time.sleep(min(0.05 * 2**attempt, 1.0))


def append_rejects(cat: Catalog, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    tbl = cat.load_table(REJECTED)
    tbl.append(pa.Table.from_pylist(rows, schema=REJECT_SCHEMA))


def load_or_none(cat: Catalog, ident: str) -> Table | None:
    try:
        return cat.load_table(ident)
    except NoSuchTableError:
        return None
