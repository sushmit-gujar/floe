"""Idempotent platform bootstrap: bucket, catalog warehouse, namespaces/tables, Kafka topic."""

from __future__ import annotations

import logging
import time
from typing import Any

import httpx

from floe import lake
from floe.config import Settings
from floe.contract import Contract

log = logging.getLogger("floe.bootstrap")
PROJECT = "00000000-0000-0000-0000-000000000000"


def wait(fn: Any, what: str, timeout: float = 300) -> Any:
    t0 = time.time()
    while True:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001
            if time.time() - t0 > timeout:
                raise TimeoutError(f"{what} not ready after {timeout}s: {exc}") from exc
            log.info("waiting for %s (%s)", what, type(exc).__name__)
            time.sleep(2)


def ensure_bucket(s: Settings) -> None:
    from pyarrow import fs

    scheme, _, host = s.s3_endpoint.partition("://")
    s3 = fs.S3FileSystem(
        access_key=s.s3_access_key,
        secret_key=s.s3_secret_key,
        endpoint_override=host,
        scheme=scheme,
        region=s.s3_region,
        allow_bucket_creation=True,
    )
    info = s3.get_file_info(s.s3_bucket)
    if info.type == fs.FileType.NotFound:
        s3.create_dir(s.s3_bucket)
        log.info("created bucket %s", s.s3_bucket)


def ensure_warehouse(s: Settings) -> None:
    with httpx.Client(base_url=s.management_uri, timeout=30) as c:
        r = c.post("/v1/bootstrap", json={"accept-terms-of-use": True})
        if r.status_code not in (200, 204) and "already" not in r.text.lower():
            # 4xx when the server is already bootstrapped
            log.info("bootstrap response %s: %s", r.status_code, r.text[:200])
        ws = c.get("/v1/warehouse", params={"project-id": PROJECT}).json().get("warehouses", [])
        if any(w.get("name") == s.warehouse for w in ws):
            return
        body = {
            "warehouse-name": s.warehouse,
            "project-id": PROJECT,
            "storage-profile": {
                "type": "s3",
                "bucket": s.s3_bucket,
                "key-prefix": "warehouse",
                "endpoint": s.s3_endpoint,
                "sts-endpoint": s.s3_endpoint,
                "region": s.s3_region,
                "path-style-access": True,
                "flavor": "minio",
                "sts-enabled": True,
            },
            "storage-credential": {
                "type": "s3",
                "credential-type": "access-key",
                "access-key-id": s.s3_access_key,
                "secret-access-key": s.s3_secret_key,
            },
        }
        r = c.post("/v1/warehouse", json=body)
        r.raise_for_status()
        log.info("created warehouse %s", s.warehouse)


def ensure_topic(s: Settings) -> None:
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": s.kafka_bootstrap})
    md = admin.list_topics(timeout=10)
    if s.topic in md.topics:
        return
    fut = admin.create_topics(
        [NewTopic(s.topic, num_partitions=s.partitions, replication_factor=1, config={"retention.ms": str(6 * 3600 * 1000)})]
    )
    for _t, f in fut.items():
        f.result(30)
    log.info("created topic %s (%d partitions)", s.topic, s.partitions)


def run(s: Settings, kafka: bool = True) -> None:
    contract = Contract.load()
    wait(lambda: ensure_bucket(s), "object storage")
    wait(lambda: httpx.get(s.management_uri.rsplit("/management", 1)[0] + "/health", timeout=5).raise_for_status(), "catalog")
    ensure_warehouse(s)
    cat = lake.catalog(s)
    wait(lambda: lake.ensure_tables(cat, contract), "tables")
    if kafka:
        wait(lambda: ensure_topic(s), "kafka")
    log.info("bootstrap complete")


def wait_ready(s: Settings, timeout: float = 600, kafka: bool = True) -> None:
    """Used as an initContainer: succeed once bronze exists and (if reachable) the topic exists."""

    def check() -> None:
        cat = lake.catalog(s)
        cat.load_table(lake.BRONZE)
        if not kafka:
            return
        from confluent_kafka.admin import AdminClient

        md = AdminClient({"bootstrap.servers": s.kafka_bootstrap}).list_topics(timeout=5)
        if s.topic not in md.topics:
            raise RuntimeError("topic missing")

    wait(check, "bootstrap", timeout)
    log.info("platform ready")
