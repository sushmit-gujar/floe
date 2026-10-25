"""Generate the floe Grafana dashboard JSON (deploy/helm/floe/files/floe-dashboard.json)."""

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
panels = []
_id = 0


def panel(title, kind, exprs, x, y, w, h, unit=None, legend=None, thresholds=None, decimals=None, desc=None, mx=None):
    global _id
    _id += 1
    targets = [
        {"datasource": DS, "expr": e, "legendFormat": (legend[i] if legend else ""), "refId": chr(65 + i)} for i, e in enumerate(exprs)
    ]
    defaults = {"color": {"mode": "palette-classic"}, "custom": {}}
    if kind == "timeseries":
        defaults["custom"] = {"lineWidth": 2, "fillOpacity": 12, "showPoints": "never", "spanNulls": True}
    if unit:
        defaults["unit"] = unit
    if decimals is not None:
        defaults["decimals"] = decimals
    if mx is not None:
        defaults["max"] = mx
        defaults["min"] = 0
    if thresholds:
        defaults["thresholds"] = {"mode": "absolute", "steps": thresholds}
        defaults["color"] = {"mode": "thresholds"}
    p = {
        "id": _id,
        "type": kind,
        "title": title,
        "datasource": DS,
        "targets": targets,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {},
    }
    if desc:
        p["description"] = desc
    if kind == "stat":
        p["options"] = {
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "colorMode": "background",
            "graphMode": "area",
            "textMode": "value",
        }
    if kind == "timeseries":
        p["options"] = {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}}
    panels.append(p)


GREEN, AMBER, RED = "#3ddc84", "#ffb020", "#ff4d6d"
# Row 1: headline stats
panel(
    "Events committed / s",
    "stat",
    ['sum(rate(floe_rows_committed_total{table="bronze.wiki_edits"}[1m]))'],
    0,
    0,
    4,
    4,
    unit="ops",
    decimals=0,
    thresholds=[{"color": "#00e5ff", "value": None}],
)
panel(
    "Consumer lag (msgs)",
    "stat",
    ["sum(floe_consumer_lag) or vector(0)"],
    4,
    0,
    4,
    4,
    decimals=0,
    thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 10000}, {"color": RED, "value": 100000}],
)
panel(
    "Processor replicas",
    "stat",
    ['count(up{component="processor"} == 1)'],
    8,
    0,
    4,
    4,
    decimals=0,
    thresholds=[{"color": "#b388ff", "value": None}],
)
panel(
    "Freshness (s since newest event)",
    "stat",
    ["time() - max(floe_last_event_timestamp_seconds > 0)"],
    12,
    0,
    4,
    4,
    unit="s",
    decimals=1,
    thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 60}, {"color": RED, "value": 900}],
    desc="Contract SLA: 900 s",
)
panel(
    "e2e latency p95",
    "stat",
    ["histogram_quantile(0.95, sum by (le) (rate(floe_e2e_latency_seconds_bucket[2m])))"],
    16,
    0,
    4,
    4,
    unit="s",
    decimals=2,
    thresholds=[{"color": GREEN, "value": None}, {"color": AMBER, "value": 5}, {"color": RED, "value": 30}],
)
panel(
    "Data quality pass rate",
    "stat",
    ["max(floe_dq_pass_rate)"],
    20,
    0,
    4,
    4,
    unit="percentunit",
    decimals=1,
    thresholds=[{"color": RED, "value": None}, {"color": AMBER, "value": 0.9}, {"color": GREEN, "value": 1}],
    desc="Share of dbt data tests passing in the latest batch run",
)
# Row 2: throughput + lag
panel(
    "Throughput",
    "timeseries",
    [
        'sum(rate(floe_rows_committed_total{table="bronze.wiki_edits"}[30s]))',
        "sum(rate(floe_events_consumed_total[30s]))",
        'sum(rate(floe_rows_committed_total{table="bronze.wiki_edits_rejected"}[30s]))',
    ],
    0,
    4,
    12,
    8,
    unit="ops",
    legend=["committed rows/s", "consumed msgs/s", "rejected (DLQ)/s"],
)
panel(
    "Consumer lag and replicas",
    "timeseries",
    ["sum(floe_consumer_lag) or vector(0)", 'count(up{component="processor"} == 1)'],
    12,
    4,
    12,
    8,
    legend=["lag (msgs)", "processor replicas"],
)
# Row 3: latency + commits
panel(
    "Produce → queryable latency",
    "timeseries",
    [f"histogram_quantile({q}, sum by (le) (rate(floe_e2e_latency_seconds_bucket[1m])))" for q in (0.5, 0.95, 0.99)],
    0,
    12,
    12,
    8,
    unit="s",
    legend=["p50", "p95", "p99"],
)
panel(
    "Iceberg commits",
    "timeseries",
    [
        "sum by (outcome) (rate(floe_commits_total[1m])) * 60",
        "histogram_quantile(0.95, sum by (le) (rate(floe_commit_seconds_bucket[1m])))",
    ],
    12,
    12,
    12,
    8,
    legend=["commits/min ({{outcome}})", "commit p95 (s)"],
)
# Row 4: quality + freshness
panel(
    "Data quality (batch runs)",
    "timeseries",
    ["max(floe_dq_pass_rate)", "max(floe_dq_runs_passing_ratio)", "max(floe_ingest_reject_rate)"],
    0,
    20,
    12,
    8,
    unit="percentunit",
    legend=["tests passing (latest run)", "runs passing (last 20)", "ingest reject rate"],
    mx=1,
)
panel(
    "Freshness",
    "timeseries",
    [
        "time() - max(floe_last_event_timestamp_seconds > 0)",
        "time() - max(floe_last_commit_timestamp_seconds > 0)",
        "time() - max(floe_gold_max_event_timestamp_seconds > 0)",
    ],
    12,
    20,
    12,
    8,
    unit="s",
    legend=["bronze: newest event age", "bronze: last commit age", "gold: newest event age"],
)
panel(
    "Contract rejects by reason (per minute)",
    "timeseries",
    ["sum by (reason) (increase(floe_events_rejected_total[1m]))"],
    0,
    28,
    24,
    6,
    unit="short",
    legend=["{{reason}}"],
)

dash = {
    "uid": "floe",
    "title": "floe: streaming lakehouse",
    "tags": ["floe", "iceberg", "kafka"],
    "timezone": "browser",
    "refresh": "5s",
    "schemaVersion": 39,
    "version": 1,
    "editable": False,
    "time": {"from": "now-30m", "to": "now"},
    "panels": panels,
    "description": "Throughput, consumer lag, autoscaling, produce-to-queryable latency, freshness and data quality.",
}
out = Path(__file__).resolve().parents[1] / "helm" / "floe" / "files" / "floe-dashboard.json"
out.write_text(json.dumps(dash, indent=1) + "\n")
print(out, len(panels), "panels")
