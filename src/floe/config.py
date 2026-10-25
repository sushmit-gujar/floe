"""Runtime settings, all from environment variables (12-factor; injected by the Helm chart)."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = field(default_factory=lambda: _env("FLOE_KAFKA_BOOTSTRAP", "localhost:9092"))
    topic: str = field(default_factory=lambda: _env("FLOE_TOPIC", "wiki.recentchange.v1"))
    partitions: int = field(default_factory=lambda: int(_env("FLOE_PARTITIONS", "6")))
    group_id: str = field(default_factory=lambda: _env("FLOE_GROUP_ID", "floe-processor"))
    catalog_uri: str = field(default_factory=lambda: _env("FLOE_CATALOG_URI", "http://localhost:8181/catalog"))
    warehouse: str = field(default_factory=lambda: _env("FLOE_WAREHOUSE", "floe"))
    s3_endpoint: str = field(default_factory=lambda: _env("FLOE_S3_ENDPOINT", "http://localhost:9000"))
    s3_bucket: str = field(default_factory=lambda: _env("FLOE_S3_BUCKET", "lake"))
    s3_access_key: str = field(default_factory=lambda: _env("FLOE_S3_ACCESS_KEY", ""))
    s3_secret_key: str = field(default_factory=lambda: _env("FLOE_S3_SECRET_KEY", ""))
    s3_region: str = field(default_factory=lambda: _env("FLOE_S3_REGION", "us-east-1"))
    management_uri: str = field(default_factory=lambda: _env("FLOE_MANAGEMENT_URI", "http://localhost:8181/management"))
    batch_max_rows: int = field(default_factory=lambda: int(_env("FLOE_BATCH_MAX_ROWS", "20000")))
    flush_interval_s: float = field(default_factory=lambda: float(_env("FLOE_FLUSH_INTERVAL_S", "2.0")))
    http_port: int = field(default_factory=lambda: int(_env("FLOE_HTTP_PORT", "8080")))
    transform_lookback_hours: int = field(default_factory=lambda: int(_env("FLOE_TRANSFORM_LOOKBACK_HOURS", "24")))
    # DuckDB spills to disk past this, so the batch layer is bounded by the pod limit, not by data volume.
    duckdb_memory_limit: str = field(default_factory=lambda: _env("FLOE_DUCKDB_MEMORY_LIMIT", "768MB"))
    duckdb_threads: int = field(default_factory=lambda: int(_env("FLOE_DUCKDB_THREADS", "2")))


def settings() -> Settings:
    return Settings()
