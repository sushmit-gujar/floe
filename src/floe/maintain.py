"""Table maintenance: compact small files in closed day partitions, expire old snapshots."""

from __future__ import annotations

import logging
from datetime import UTC, date, datetime, timedelta
from typing import Any

from pyiceberg.catalog import Catalog
from pyiceberg.exceptions import CommitFailedException
from pyiceberg.expressions import And, GreaterThanOrEqual, LessThan

from floe import lake

log = logging.getLogger("floe.maintain")


def compact_closed_days(cat: Catalog, min_files: int = 8, max_days: int = 3) -> list[dict[str, Any]]:
    """Rewrite each closed (before today, UTC) day partition that has >= min_files data files.

    Uses an optimistic overwrite; bronze disables pyiceberg auto-retry, so a concurrent
    processor commit makes this attempt fail cleanly instead of losing late-arriving rows.
    """
    tbl = cat.load_table(lake.BRONZE)
    files = tbl.inspect.files().to_pylist()
    by_day: dict[int, int] = {}
    for f in files:
        day = f["partition"].get("event_day") if isinstance(f.get("partition"), dict) else None
        if isinstance(day, date):  # pyiceberg returns day-transform partitions as datetime.date
            day = (day - date(1970, 1, 1)).days
        if day is not None:
            by_day[day] = by_day.get(day, 0) + 1
    today = (datetime.now(UTC).date() - datetime(1970, 1, 1).date()).days
    out = []
    for day in sorted(d for d, n in by_day.items() if d < today and n >= min_files)[-max_days:]:
        start = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(days=day)
        flt = And(GreaterThanOrEqual("event_time", start.isoformat()), LessThan("event_time", (start + timedelta(days=1)).isoformat()))
        tbl = cat.load_table(lake.BRONZE)
        data = tbl.scan(row_filter=flt).to_arrow()
        try:
            tbl.overwrite(data, overwrite_filter=flt, snapshot_properties={"floe.maintenance": "compact"})
            out.append({"day": start.date().isoformat(), "files_before": by_day[day], "rows": data.num_rows})
        except CommitFailedException:
            log.info("compaction of %s lost a race with the processor; will retry next run", start.date())
    return out


def expire(cat: Catalog, older_than_hours: int = 24) -> dict[str, int]:
    out = {}
    cutoff = datetime.now(UTC) - timedelta(hours=older_than_hours)
    for ident in (lake.BRONZE, lake.REJECTED):
        tbl = cat.load_table(ident)
        before = len(tbl.metadata.snapshots)
        try:
            tbl.maintenance.expire_snapshots().older_than(cutoff).commit()
        except CommitFailedException:
            continue
        out[ident] = before - len(cat.load_table(ident).metadata.snapshots)
    return out
