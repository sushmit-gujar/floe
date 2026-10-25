"""Prometheus metrics shared by floe services."""

from prometheus_client import Counter, Gauge, Histogram

LATENCY_BUCKETS = (0.1, 0.25, 0.5, 0.75, 1, 1.5, 2, 2.5, 3, 4, 5, 7.5, 10, 15, 20, 30, 60, 120, 300)

consumed = Counter("floe_events_consumed_total", "Kafka messages consumed")
rows_committed = Counter("floe_rows_committed_total", "Rows committed to Iceberg", ["table"])
rejected = Counter("floe_events_rejected_total", "Events rejected by the data contract", ["reason"])
commits = Counter("floe_commits_total", "Iceberg commits", ["outcome"])
commit_seconds = Histogram(
    "floe_commit_seconds",
    "Iceberg commit duration (append + offsets)",
    buckets=(0.02, 0.05, 0.1, 0.2, 0.3, 0.5, 0.75, 1, 2, 5, 10),
)
batch_rows = Histogram("floe_batch_rows", "Rows per committed batch", buckets=(10, 100, 500, 1000, 2500, 5000, 10000, 20000, 50000))
e2e_latency = Histogram("floe_e2e_latency_seconds", "Produce-to-queryable latency (row visible in Iceberg)", buckets=LATENCY_BUCKETS)
consumer_lag = Gauge("floe_consumer_lag", "High watermark minus consumer position", ["partition"])
assigned_partitions = Gauge("floe_assigned_partitions", "Partitions assigned to this replica")
last_commit_ts = Gauge("floe_last_commit_timestamp_seconds", "Unix time of the last successful commit")
last_event_ts = Gauge("floe_last_event_timestamp_seconds", "Max event_time committed by this replica")

REJECT_REASONS = ("invalid_json", "not_an_object", "missing", "type", "too_long", "enum", "empty", "range")


def init_series(tables: tuple[str, ...]) -> None:
    """Export labelled counters at 0 from startup, so the first increment shows up in rate()/increase()."""
    for r in REJECT_REASONS:
        rejected.labels(r)
    for t in tables:
        rows_committed.labels(t)
    for o in ("ok", "retried", "fenced", "error"):
        commits.labels(o)
