"""floe benchmarks on a live cluster (make up && make bench).

Scenarios (all events are replays of real Wikimedia recentchange events with unique ids):
  1. steady   - fixed producer rate, processor pinned at 1 replica: produce->queryable latency.
  2. burst-1  - large burst, processor pinned at 1 replica: max drain throughput.
  3. burst-keda - same burst with KEDA scaling on consumer lag: reaction + drain time.
  4. kill     - steady load with 2 replicas, SIGKILL one processor mid-stream: recovery + exactly-once.
Each scenario is verified for exactly-once delivery from the Iceberg table itself.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import subprocess
import sys
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests" / "e2e"))
import k8s  # noqa: E402

SO = "floe-processor"
DEPLOY = "deploy/floe-processor"


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, check=False).stdout.strip()


def hardware() -> dict:
    cpu = ""
    try:
        for line in open("/proc/cpuinfo"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    mem = ""
    try:
        mem = f"{int(open('/proc/meminfo').readline().split()[1]) / 1024 / 1024:.1f} GiB"
    except (OSError, ValueError):
        pass
    return {
        "host": platform.node(),
        "cpu": cpu,
        "vcpus": os.cpu_count(),
        "memory": mem,
        "kernel": platform.release(),
        "kind": sh("kind", "version"),
        "kubernetes": json.loads(k8s.kubectl("version", "-o", "json", ns="", check=False) or "{}")
        .get("serverVersion", {})
        .get("gitVersion"),
        "github_actions": bool(os.environ.get("GITHUB_ACTIONS")),
    }


def pin_replicas(n: int | None) -> None:
    if n is None:
        k8s.kubectl("annotate", "scaledobject", SO, "autoscaling.keda.sh/paused-replicas-", "--overwrite", check=False)
        k8s.kubectl("annotate", "scaledobject", SO, "autoscaling.keda.sh/paused-", "--overwrite", check=False)
    else:
        k8s.kubectl("annotate", "scaledobject", SO, f"autoscaling.keda.sh/paused-replicas={n}", "--overwrite")
        k8s.kubectl("scale", DEPLOY, f"--replicas={n}")
    wait_replicas(n or 1)


def wait_replicas(n: int, timeout: float = 300) -> None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = json.loads(k8s.kubectl("get", DEPLOY, "-o", "json"))
        spec, ready = st["spec"]["replicas"], st["status"].get("readyReplicas", 0)
        pods = [
            p
            for p in json.loads(k8s.kubectl("get", "pods", "-l", "app.kubernetes.io/component=processor", "-o", "json"))["items"]
            if not p["metadata"].get("deletionTimestamp")
        ]
        if spec == n and ready == n and len(pods) == n:
            time.sleep(8)  # let the consumer group settle
            return
        time.sleep(2)
    raise TimeoutError(f"processor did not settle at {n} replicas")


def produce(image: str, name: str, run_id: str, count: int, rate: float = 0) -> dict:
    ok, res, out = k8s.run_job(
        k8s.job_manifest(
            name,
            image,
            ["produce", "replay", "--run-id", run_id, "--count", str(count), "--rate", str(rate)],
            labels=k8s.KAFKA,
            cpu="2",
        ),
        timeout=1800,
    )
    assert ok and res, out
    return res


def verify(image: str, name: str, run_id: str, n: int, produced_from: float) -> dict:
    ok, res, out = k8s.run_job(
        k8s.job_manifest(
            name,
            image,
            ["verify", "--run-id", run_id, "--expect-valid", str(n), "--timeout", "1200", "--produced-from", str(produced_from)],
            labels=k8s.LAKE,
            memory="2Gi",
            cpu="2",
        ),
        timeout=1800,
    )
    assert res, out
    return res


class Sampler(threading.Thread):
    """1 Hz timeline of replicas (desired/ready) and consumer lag as seen by KEDA's HPA metric."""

    def __init__(self, prom: str):
        super().__init__(daemon=True)
        self.prom = prom
        self.rows: list[dict] = []
        self.stop = threading.Event()
        self.t0 = time.time()

    def run(self) -> None:
        while not self.stop.is_set():
            t = time.time()
            try:
                st = json.loads(k8s.kubectl("get", DEPLOY, "-o", "json"))
                lag = k8s.prom_value(self.prom, "sum(floe_consumer_lag)")
                rate = k8s.prom_value(self.prom, 'sum(rate(floe_rows_committed_total{table="bronze.wiki_edits"}[15s]))')
                self.rows.append(
                    {
                        "t": round(t - self.t0, 1),
                        "desired": st["spec"]["replicas"],
                        "ready": st["status"].get("readyReplicas", 0),
                        "lag": lag,
                        "rows_per_s": rate,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                print("sampler:", exc, file=sys.stderr)
            self.stop.wait(max(0.0, 1.0 - (time.time() - t)))


def scenario_steady(a, ts) -> dict:
    pin_replicas(1)
    run = f"bs{ts}"
    n = int(a.steady_rate * a.steady_seconds)
    p = produce(a.image, f"bench-steady-p-{ts}", run, n, a.steady_rate)
    v = verify(a.image, f"bench-steady-v-{ts}", run, n, p["started_at"])
    return {"rate": a.steady_rate, "events": n, "produce": p, "verify": v}


def scenario_burst(a, ts, keda: bool, prom: str) -> dict:
    pin_replicas(None if keda else 1)
    run = f"b{'k' if keda else '1'}{ts}"
    sampler = Sampler(prom)
    sampler.start()
    t_start = time.time()
    sampler.t0 = t_start
    p = produce(a.image, f"bench-burst-{'k' if keda else '1'}-p-{ts}", run, a.burst)
    v = verify(a.image, f"bench-burst-{'k' if keda else '1'}-v-{ts}", run, a.burst, p["started_at"])
    out = {
        "events": a.burst,
        "keda": keda,
        "produce": p,
        "verify": v,
        "drain_seconds": round(v["last_commit_ms"] / 1000 - p["started_at"], 1),
    }
    if keda:
        # watch the scale-down too
        t_end = time.time() + a.scale_down_watch
        while time.time() < t_end:
            st = json.loads(k8s.kubectl("get", DEPLOY, "-o", "json"))
            if st["spec"]["replicas"] == 1 and sampler.rows and max(r["desired"] for r in sampler.rows) > 1:
                break
            time.sleep(2)
        time.sleep(2)
    sampler.stop.set()
    sampler.join()
    tl = sampler.rows
    out["timeline"] = tl
    if keda and tl:
        first_up = next((r["t"] for r in tl if r["desired"] > 1), None)
        peak = max(r["desired"] for r in tl)
        peak_ready = next((r["t"] for r in tl if r["ready"] >= peak), None)
        back = next((r["t"] for r in tl if first_up and r["t"] > first_up and r["desired"] == 1), None)
        out.update(
            {"first_scale_out_s": first_up, "peak_replicas": peak, "peak_replicas_ready_s": peak_ready, "scaled_back_to_1_s": back}
        )
    return out


def scenario_kill(a, ts) -> dict:
    pin_replicas(2)
    run = f"bx{ts}"
    n = int(a.kill_rate * a.kill_seconds)
    name = f"bench-kill-p-{ts}"
    k8s.start_job(
        k8s.job_manifest(
            name,
            a.image,
            ["produce", "replay", "--run-id", run, "--count", str(n), "--rate", str(a.kill_rate)],
            labels=k8s.KAFKA,
            cpu="2",
        )
    )
    time.sleep(a.kill_after)
    pods = json.loads(k8s.kubectl("get", "pods", "-l", "app.kubernetes.io/component=processor", "-o", "json"))["items"]
    victim = sorted(p["metadata"]["name"] for p in pods)[0]
    t_kill = time.time()
    k8s.kubectl("delete", "pod", victim, "--grace-period=0", "--force", "--wait=false", check=False)
    # time until the replacement pod is Ready
    t_ready = None
    while time.time() - t_kill < 300:
        st = json.loads(k8s.kubectl("get", DEPLOY, "-o", "json"))["status"]
        names = [
            p["metadata"]["name"]
            for p in json.loads(k8s.kubectl("get", "pods", "-l", "app.kubernetes.io/component=processor", "-o", "json"))["items"]
        ]
        if st.get("readyReplicas", 0) == 2 and victim not in names:
            t_ready = time.time()
            break
        time.sleep(0.5)
    ok, out = k8s.wait_job(name, timeout=1800)
    p = k8s.last_json(out)
    v = verify(a.image, f"bench-kill-v-{ts}", run, p["valid"], p["started_at"])
    # largest gap between consecutive commits of this run around the kill, from Iceberg snapshot times
    gm = k8s.job_manifest(f"bench-kill-g-{ts}", a.image, ["-c", GAP_SCRIPT, run, str(t_kill)], labels=k8s.LAKE, memory="1Gi")
    gm["spec"]["template"]["spec"]["containers"][0]["command"] = ["python"]
    ok, gaps, out = k8s.run_job(gm, timeout=600)
    return {
        "rate": a.kill_rate,
        "events": n,
        "killed_pod": victim,
        "kill_at_s": a.kill_after,
        "replacement_ready_s": round(t_ready - t_kill, 1) if t_ready else None,
        "produce": p,
        "verify": v,
        "commit_gaps": gaps,
    }


GAP_SCRIPT = r"""
import json, sys
from floe import lake
from floe.config import Settings
run, t_kill = sys.argv[1], float(sys.argv[2])
t = lake.catalog(Settings()).load_table(lake.BRONZE)
ts = sorted(s.timestamp_ms / 1000 for s in t.metadata.snapshots)
after = [x for x in ts if x >= t_kill - 30]
gaps = [b - a for a, b in zip(after, after[1:])]
first_after = next((x for x in ts if x > t_kill), None)
print(json.dumps({"max_commit_gap_s": round(max(gaps), 2) if gaps else None,
                  "first_commit_after_kill_s": round(first_after - t_kill, 2) if first_after else None,
                  "commits_in_window": len(after)}))
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--namespace", default="floe")
    ap.add_argument("--image", default="floe:dev")
    ap.add_argument("--steady-rate", type=float, default=2000)
    ap.add_argument("--steady-seconds", type=float, default=60)
    ap.add_argument("--burst", type=int, default=1200000)
    ap.add_argument("--kill-rate", type=float, default=2000)
    ap.add_argument("--kill-seconds", type=float, default=90)
    ap.add_argument("--kill-after", type=float, default=30)
    ap.add_argument("--scale-down-watch", type=float, default=300)
    ap.add_argument("--scenarios", default="steady,burst1,burstkeda,kill")
    ap.add_argument("--out", default="results/bench.json")
    a = ap.parse_args()
    k8s.NS = a.namespace
    ts = int(time.time())
    report: dict = {"started_at": ts, "hardware": hardware(), "params": vars(a), "scenarios": {}}
    live = json.loads(k8s.kubectl("get", "deploy", "floe-producer-live", "-o", "json", check=False) or "{}")
    if live:
        k8s.kubectl("scale", "deploy/floe-producer-live", "--replicas=0")  # quiet cluster during benchmarks
    try:
        with k8s.port_forward("floe-prometheus", 9090) as prom:
            for sc in a.scenarios.split(","):
                print(f"== {sc}", flush=True)
                t0 = time.time()
                if sc == "steady":
                    r = scenario_steady(a, ts)
                elif sc == "burst1":
                    r = scenario_burst(a, ts, keda=False, prom=prom)
                elif sc == "burstkeda":
                    r = scenario_burst(a, ts, keda=True, prom=prom)
                elif sc == "kill":
                    r = scenario_kill(a, ts)
                else:
                    raise SystemExit(f"unknown scenario {sc}")
                r["wall_seconds"] = round(time.time() - t0, 1)
                report["scenarios"][sc] = r
                summary = {k: v for k, v in r.items() if k not in ("timeline",)}
                print(json.dumps(summary, default=str)[:2000], flush=True)
                Path(a.out).parent.mkdir(parents=True, exist_ok=True)
                Path(a.out).write_text(json.dumps(report, indent=2, default=str) + "\n")
                if "timeline" in r:
                    with open(Path(a.out).with_name(f"timeline-{sc}.csv"), "w", newline="") as fh:
                        w = csv.DictWriter(fh, fieldnames=["t", "desired", "ready", "lag", "rows_per_s"])
                        w.writeheader()
                        w.writerows(r["timeline"])
    finally:
        pin_replicas(None)
        if live:
            k8s.kubectl("scale", "deploy/floe-producer-live", "--replicas=1", check=False)
    return 0


if __name__ == "__main__":
    sys.exit(main())
