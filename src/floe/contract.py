"""Data contract: load the YAML, validate raw events, derive Arrow/Iceberg schemas."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from importlib import resources
from typing import Any

import pyarrow as pa
import yaml

ARROW_TYPES = {
    "string": pa.string(),
    "int": pa.int32(),
    "long": pa.int64(),
    "bool": pa.bool_(),
    "timestamp": pa.timestamp("us", tz="UTC"),
}

# Columns the processor adds to every accepted row (lineage + exactly-once bookkeeping).
LINEAGE_FIELDS = [
    pa.field("kafka_partition", pa.int32(), nullable=False),
    pa.field("kafka_offset", pa.int64(), nullable=False),
    pa.field("batch_id", pa.string(), nullable=False),
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
]

REJECT_SCHEMA = pa.schema(
    [
        pa.field("kafka_partition", pa.int32(), nullable=False),
        pa.field("kafka_offset", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("payload", pa.string(), nullable=True),
        pa.field("batch_id", pa.string(), nullable=False),
        pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


class ContractViolation(ValueError):
    pass


@dataclass(frozen=True)
class Field:
    name: str
    source: str
    type: str
    required: bool = False
    enum: tuple[str, ...] | None = None
    min: float | None = None
    max_length: int | None = None

    def extract(self, event: dict[str, Any]) -> Any:
        cur: Any = event
        for part in self.source.split("."):
            if not isinstance(cur, dict) or part not in cur:
                return None
            cur = cur[part]
        return cur


@dataclass
class Contract:
    name: str
    version: int
    topic: str
    fields: list[Field]
    quality: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def load(cls, path: str | None = None) -> Contract:
        if path:
            with open(path, encoding="utf-8") as fh:
                raw = yaml.safe_load(fh)
        else:
            text = resources.files("floe.contracts").joinpath("wiki_edit.v1.yaml").read_text("utf-8")
            raw = yaml.safe_load(text)
        fields = [
            Field(
                name=f["name"],
                source=f["source"],
                type=f["type"],
                required=bool(f.get("required", False)),
                enum=tuple(f["enum"]) if f.get("enum") else None,
                min=f.get("min"),
                max_length=f.get("max_length"),
            )
            for f in raw["fields"]
        ]
        for f in fields:
            if f.type not in ARROW_TYPES:
                raise ValueError(f"unsupported contract type {f.type!r} for {f.name}")
        return cls(raw["name"], int(raw["version"]), raw["topic"], fields, raw.get("quality", {}))

    # ---- schemas -------------------------------------------------------------------
    @property
    def arrow_schema(self) -> pa.Schema:
        cols = [pa.field(f.name, ARROW_TYPES[f.type], nullable=not f.required) for f in self.fields]
        return pa.schema(cols + LINEAGE_FIELDS)

    # ---- validation ----------------------------------------------------------------
    def validate(self, event: Any) -> dict[str, Any]:
        """Return the contract-shaped row or raise ContractViolation with a short reason."""
        if not isinstance(event, dict):
            raise ContractViolation("not_an_object")
        row: dict[str, Any] = {}
        for f in self.fields:
            val = f.extract(event)
            if val is None:
                if f.required:
                    raise ContractViolation(f"missing:{f.name}")
                row[f.name] = None
                continue
            row[f.name] = _coerce(f, val)
        return row


def _coerce(f: Field, val: Any) -> Any:
    t = f.type
    if t == "string":
        if not isinstance(val, str):
            raise ContractViolation(f"type:{f.name}")
        if f.max_length is not None and len(val) > f.max_length:
            raise ContractViolation(f"too_long:{f.name}")
        if f.enum is not None and val not in f.enum:
            raise ContractViolation(f"enum:{f.name}")
        if f.required and not val:
            raise ContractViolation(f"empty:{f.name}")
        return val
    if t in ("int", "long"):
        if isinstance(val, bool) or not isinstance(val, int):
            raise ContractViolation(f"type:{f.name}")
        if f.min is not None and val < f.min:
            raise ContractViolation(f"range:{f.name}")
        if t == "int" and not (-(2**31) <= val < 2**31):
            raise ContractViolation(f"range:{f.name}")
        return val
    if t == "bool":
        if not isinstance(val, bool):
            raise ContractViolation(f"type:{f.name}")
        return val
    if t == "timestamp":
        return _parse_ts(f, val)
    raise ContractViolation(f"type:{f.name}")  # pragma: no cover


def _parse_ts(f: Field, val: Any) -> datetime:
    if isinstance(val, bool):
        raise ContractViolation(f"type:{f.name}")
    if isinstance(val, (int, float)):
        # epoch milliseconds
        if val <= 0:
            raise ContractViolation(f"range:{f.name}")
        return datetime.fromtimestamp(val / 1000.0, tz=UTC)
    if isinstance(val, str):
        s = val.strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(s)
        except ValueError as exc:
            raise ContractViolation(f"type:{f.name}") from exc
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC)
    raise ContractViolation(f"type:{f.name}")
