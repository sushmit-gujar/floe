"""Minimal kubectl helpers (stdlib only) shared by the e2e test and the benchmarks."""

from __future__ import annotations

import contextlib
import json
import socket
import subprocess
import time
import urllib.request
from collections.abc import Iterator
from typing import Any

NS = "floe"
RELEASE = "floe"


def kubectl(*args: str, ns: str | None = None, check: bool = True, input: str | None = None, timeout: float = 600) -> str:
    cmd = ["kubectl"] + (["-n", ns or NS] if ns != "" else []) + list(args)
    res = subprocess.run(cmd, capture_output=True, text=True, input=input, timeout=timeout)
    if check and res.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {res.stderr.strip()}")
    return res.stdout


def job_manifest(
    name: str,
    image: str,
    args: list[str],
    labels: dict[str, str] | None = None,
    memory: str = "512Mi",
    cpu: str = "1",
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    lbl = {"app.kubernetes.io/part-of": "floe", "app.kubernetes.io/component": "harness", "floe.io/run": name}
    lbl.update(labels or {})
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": name, "labels": lbl},
        "spec": {
            "backoffLimit": 0,
            "ttlSecondsAfterFinished": 1800,
            "template": {
                "metadata": {"labels": lbl},
                "spec": {
                    "restartPolicy": "Never",
                    "automountServiceAccountToken": False,
                    "securityContext": {
                        "runAsNonRoot": True,
                        "runAsUser": 10001,
                        "runAsGroup": 10001,
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "main",
                            "image": image,
                            "imagePullPolicy": "IfNotPresent",
                            "args": args,
                            "envFrom": [{"configMapRef": {"name": f"{RELEASE}-config"}}],
                            "env": [{"name": k, "value": v} for k, v in (env or {}).items()],
                            "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": cpu, "memory": memory}},
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "volumeMounts": [{"name": "tmp", "mountPath": "/tmp"}],
                        }
                    ],
                    "volumes": [{"name": "tmp", "emptyDir": {}}],
                },
            },
        },
    }


KAFKA = {"floe.io/kafka-client": "true"}
LAKE = {"floe.io/catalog-client": "true", "floe.io/s3-client": "true"}


def start_job(manifest: dict[str, Any]) -> str:
    kubectl("apply", "-f", "-", input=json.dumps(manifest))
    return manifest["metadata"]["name"]


def wait_job(name: str, timeout: float = 900) -> tuple[bool, str]:
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = json.loads(kubectl("get", "job", name, "-o", "json"))["status"]
        if st.get("succeeded"):
            return True, logs(name)
        if st.get("failed"):
            return False, logs(name)
        time.sleep(1)
    raise TimeoutError(f"job {name} did not finish in {timeout}s")


def logs(job: str) -> str:
    return kubectl("logs", f"job/{job}", "--tail=-1", check=False)


def last_json(text: str) -> dict[str, Any]:
    for line in reversed(text.strip().splitlines()):
        line = line.strip()
        if line.startswith("{") and not line.startswith('{"ts"'):
            try:
                return json.loads(line)
            except ValueError:
                continue
    raise ValueError(f"no JSON result in output:\n{text[-2000:]}")


def run_job(manifest: dict[str, Any], timeout: float = 900) -> tuple[bool, dict[str, Any] | None, str]:
    name = start_job(manifest)
    ok, out = wait_job(name, timeout)
    try:
        return ok, last_json(out), out
    except ValueError:
        return ok, None, out


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@contextlib.contextmanager
def port_forward(svc: str, port: int) -> Iterator[str]:
    local = _free_port()
    proc = subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", f"svc/{svc}", f"{local}:{port}"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    base = f"http://127.0.0.1:{local}"
    try:
        for _ in range(100):
            try:
                with socket.create_connection(("127.0.0.1", local), timeout=0.5):
                    break
            except OSError:
                time.sleep(0.2)
        yield base
    finally:
        proc.terminate()
        proc.wait(5)


def http_json(url: str, timeout: float = 30) -> Any:
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310 - local port-forward
        return json.loads(r.read())


def http_text(url: str, timeout: float = 30) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as r:  # noqa: S310
        return r.read().decode()


def prom_query(base: str, q: str) -> list[dict[str, Any]]:
    from urllib.parse import quote

    return http_json(f"{base}/api/v1/query?query={quote(q)}")["data"]["result"]


def prom_value(base: str, q: str) -> float | None:
    r = prom_query(base, q)
    return float(r[0]["value"][1]) if r else None
