"""Table maintenance against a real (SQLite) Iceberg catalog."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pyarrow.compute as pc

from floe import lake
from floe.maintain import compact_closed_days
from floe.process import Batcher
from tests.test_exactly_once import feed


def test_compacts_closed_day_partition(cat, contract, sample):
    b = Batcher(contract, cat)
    b.assign([0])
    feed(b, sample, "m", 200, partitions=1)
    assert b.flush().committed
    tbl = cat.load_table(lake.BRONZE)
    rows = tbl.scan().to_arrow()
    # Move a copy of the rows into a closed day, written as several small files.
    old = (datetime.now(UTC) - timedelta(days=2)).replace(hour=12, minute=0, second=0, microsecond=0)
    i = rows.schema.get_field_index("event_time")
    col = pa.array([old] * rows.num_rows, type=rows.schema.field("event_time").type)
    shifted = rows.set_column(i, rows.schema.field("event_time"), col)
    shifted = shifted.set_column(
        rows.schema.get_field_index("event_id"),
        rows.schema.field("event_id"),
        pc.binary_join_element_wise(shifted.column("event_id"), "-old", ""),
    )
    for chunk in range(3):
        cat.load_table(lake.BRONZE).append(shifted.slice(chunk * 50, 50))

    out = compact_closed_days(cat, min_files=2)
    assert len(out) == 1
    assert out[0]["day"] == old.date().isoformat()
    assert out[0]["files_before"] >= 3
    assert out[0]["rows"] == 150
    assert cat.load_table(lake.BRONZE).scan().to_arrow().num_rows == rows.num_rows + 150
