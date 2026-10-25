"""End-to-end test against a deployed floe stack (make up && make e2e).

Pushes a replay of real Wikimedia events (with injected contract violations and late
events) through Redpanda -> processor -> Iceberg, then asserts:
  * every valid event landed exactly once (no loss, no duplicates),
  * every invalid event landed in the dead-letter table,
  * the dbt batch run passes all data tests and publishes gold,
  * the query API serves gold and exports DQ metrics,
  * Prometheus scrapes the pipeline, KEDA's ScaledObject is ready,
  * NetworkPolicies block an unlabelled pod from Postgres (when the CNI enforces them).
"""

from __future__ import annotations

import argparse
import json
import platform
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import k8s  # noqa: E402


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default="floe")
    ap.add_argument("--image", default="floe:dev")
    ap.add_argument("--count", type=int, default=20000)
    ap.add_argument("--invalid-ratio", type=float, default=0.02)
    ap.add_argument("--late-ratio", type=float, default=0.01)
    ap.add_argument("--require-netpol", action="store_true", help="fail if NetworkPolicies are not enforced")
    ap.add_argument("--out", default="results/e2e.json")
    a = ap.parse_args()
    k8s.NS = a.namespace
    ts = int(time.time())
    run_id = f"e2e{ts}"
    checks: dict[str, bool] = {}
    report: dict = {"run_id": run_id, "started_at": ts, "host": platform.node()}

    k8s.kubectl("rollout", "status", "deploy/floe-processor", "--timeout=300s")

    # 1. produce
    t0 = time.time()
    ok, prod, out = k8s.run_job(
        k8s.job_manifest(
            f"e2e-produce-{ts}",
            a.image,
            [
                "produce",
                "replay",
                "--run-id",
                run_id,
                "--count",
                str(a.count),
                "--invalid-ratio",
                str(a.invalid_ratio),
                "--late-ratio",
                str(a.late_ratio),
            ],
            labels=k8s.KAFKA,
        ),
        timeout=600,
    )
    assert ok and prod, out
    report["produce"] = prod
    print("produced:", json.dumps(prod))

    # 2. verify exactly-once + DLQ
    ok, ver, out = k8s.run_job(
        k8s.job_manifest(
            f"e2e-verify-{ts}",
            a.image,
            [
                "verify",
                "--run-id",
                run_id,
                "--expect-valid",
                str(prod["valid"]),
                "--expect-rejected",
                str(prod["invalid"]),
                "--timeout",
                "300",
                "--produced-from",
                str(prod["started_at"]),
            ],
            labels=k8s.LAKE,
            memory="1Gi",
        ),
        timeout=600,
    )
    assert ver, out
    report["verify"] = ver
    report["seconds_produce_to_verified"] = round(time.time() - t0, 1)
    checks["exactly_once"] = bool(ver["exactly_once"])
    checks["dead_letter_complete"] = bool(ver.get("rejects_ok"))
    print("verified:", json.dumps(ver))

    # 3. batch layer: dbt build + tests + publish
    job = f"e2e-transform-{ts}"
    k8s.kubectl("create", "job", "--from=cronjob/floe-transform", job)
    ok, out = k8s.wait_job(job, timeout=900)
    tr = k8s.last_json(out)
    report["transform"] = tr
    checks["dbt_tests_pass"] = ok and tr["status"] == "pass" and tr["tests_failed"] == 0
    checks["gold_published"] = bool(tr["published"])
    print("transform:", json.dumps(tr))

    # 4. serving layer
    with k8s.port_forward("floe-api", 8000) as api:
        summ = k8s.http_json(f"{api}/v1/summary")
        top = summ["top_wikis"][0]["wiki"]
        epm = k8s.http_json(f"{api}/v1/wikis/{top}/edits-per-minute?minutes=5")
        metrics = k8s.http_text(f"{api}/metrics")
    checks["api_serves_gold"] = bool(summ["top_wikis"]) and bool(epm)
    checks["api_reports_quality"] = summ["last_batch_run"]["status"] == "pass" and "floe_dq_pass_rate 1.0" in metrics
    report["api"] = {"top_wiki": top, "top_wiki_edits": summ["top_wikis"][0]["edits"], "ingest_quality": summ["ingest_quality"]}

    # 5. observability + autoscaling
    with k8s.port_forward("floe-prometheus", 9090) as prom:
        committed = None
        for _ in range(30):
            committed = k8s.prom_value(prom, 'sum(floe_rows_committed_total{table="bronze.wiki_edits"})')
            if committed:
                break
            time.sleep(2)
        replicas = k8s.prom_value(prom, 'count(up{component="processor"} == 1)')
    checks["prometheus_scrapes_pipeline"] = bool(committed and committed > 0 and replicas)
    keda_ok = False
    lag_metric = None
    for _ in range(60):  # KEDA needs a polling cycle or two after install
        so = json.loads(k8s.kubectl("get", "scaledobject", "floe-processor", "-o", "json"))
        ready = [c for c in so.get("status", {}).get("conditions", []) if c["type"] == "Ready"]
        hpa = json.loads(k8s.kubectl("get", "hpa", "keda-hpa-floe-processor", "-o", "json", check=False) or "{}")
        cur = (hpa.get("status") or {}).get("currentMetrics") or []
        if cur:
            lag_metric = cur[0].get("external", {}).get("current", {})
        if ready and ready[0]["status"] == "True" and lag_metric:
            keda_ok = True
            break
        time.sleep(3)
    checks["keda_lag_metric_flowing"] = keda_ok
    report["keda_hpa_current_metric"] = lag_metric

    # 6. network policy: an unlabelled pod must not reach Postgres
    probe = (
        "import socket\n"
        "s=socket.socket(); s.settimeout(4)\n"
        "try:\n s.connect(('floe-postgres',5432)); print('{\"reachable\": true}')\n"
        "except OSError:\n print('{\"reachable\": false}')\n"
    )
    m = k8s.job_manifest(f"e2e-netpol-{ts}", a.image, ["-c", probe])
    m["spec"]["template"]["spec"]["containers"][0]["command"] = ["python"]
    _ok, np_res, _out = k8s.run_job(m, timeout=120)
    enforced = bool(np_res) and np_res["reachable"] is False
    report["networkpolicy_enforced"] = enforced
    if a.require_netpol:
        checks["networkpolicy_enforced"] = enforced

    report["checks"] = checks
    report["passed"] = all(checks.values())
    Path(a.out).parent.mkdir(parents=True, exist_ok=True)
    Path(a.out).write_text(json.dumps(report, indent=2, default=str) + "\n")
    for k, v in checks.items():
        print(f"{'PASS' if v else 'FAIL'}  {k}")
    print(f"networkpolicy enforced by CNI: {enforced}")
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
