"""Event producers.

* ``replay``: replays the bundled sample of real Wikimedia ``recentchange`` events
  (captured from EventStreams, user names hashed) at a target rate, with unique
  event ids per run and optional, deterministic injection of contract-violating and
  late events. Used by the e2e test and the benchmarks.
* ``wikimedia``: forwards the live public Wikimedia EventStreams feed (SSE).
"""

from __future__ import annotations

import copy
import gzip
import json
import logging
import random
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from importlib import resources
from pathlib import Path
from typing import Any

log = logging.getLogger("floe.produce")

SAMPLE_NAME = "wikimedia-recentchange-sample.jsonl.gz"
WIKIMEDIA_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "floe/0.1 (https://github.com/sushmit-gujar/floe)"


def load_sample(path: str | None = None) -> list[dict[str, Any]]:
    if path:
        fh = gzip.open(path, "rt", encoding="utf-8")
    else:
        fh = gzip.open(resources.files("floe.data").joinpath(SAMPLE_NAME).open("rb"), "rt", encoding="utf-8")  # type: ignore[arg-type]
    with fh:
        return [json.loads(line) for line in fh if line.strip()]


CORRUPTIONS = ("missing_title", "bad_enum", "bad_type", "not_json")


def corrupt(event: dict[str, Any], kind: str) -> bytes:
    e = copy.deepcopy(event)
    if kind == "missing_title":
        e.pop("title", None)
    elif kind == "bad_enum":
        e["type"] = "teleport"
    elif kind == "bad_type":
        e["namespace"] = "zero"
    elif kind == "not_json":
        return b"{not json" + e["meta"]["id"].encode()
    return json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode()


def replay_events(
    sample: list[dict[str, Any]],
    run_id: str,
    count: int,
    invalid_ratio: float = 0.0,
    late_ratio: float = 0.0,
    seed: int = 7,
) -> Iterator[tuple[bytes, bytes, str]]:
    """Yield (key, value, kind) with kind in {valid, late, <corruption>}. Timestamps set at yield time."""
    rng = random.Random(seed)
    n = len(sample)
    for i in range(count):
        e = dict(sample[i % n])
        meta = dict(e["meta"])
        now = datetime.now(UTC)
        meta["id"] = f"{run_id}-{i:09d}"
        kind = "valid"
        r = rng.random()
        if r < late_ratio:
            meta["dt"] = (now - timedelta(minutes=30)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            kind = "late"
        else:
            meta["dt"] = now.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        e["meta"] = meta
        e["floe_produced_at"] = int(now.timestamp() * 1000)
        key = str(e.get("wiki") or "").encode()
        if rng.random() < invalid_ratio:
            kind = CORRUPTIONS[rng.randrange(len(CORRUPTIONS))]
            yield key, corrupt(e, kind), kind
        else:
            yield key, json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode(), kind


def _producer(bootstrap: str) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 10,
            "batch.size": 262144,
            "compression.type": "lz4",
            "queue.buffering.max.messages": 500000,
        }
    )


def replay(
    bootstrap: str,
    topic: str,
    run_id: str,
    count: int,
    rate: float,
    invalid_ratio: float,
    late_ratio: float,
    seed: int,
    sample_path: str | None = None,
) -> dict[str, Any]:  # pragma: no cover
    sample = load_sample(sample_path)
    prod = _producer(bootstrap)
    kinds: dict[str, int] = {}
    t0 = time.time()
    interval = 1.0 / rate if rate > 0 else 0.0
    for i, (key, value, kind) in enumerate(replay_events(sample, run_id, count, invalid_ratio, late_ratio, seed)):
        kinds[kind] = kinds.get(kind, 0) + 1
        while True:
            try:
                prod.produce(topic, value=value, key=key)
                break
            except BufferError:
                prod.poll(0.05)
        if i % 1000 == 0:
            prod.poll(0)
        if interval:
            target = t0 + (i + 1) * interval
            delay = target - time.time()
            if delay > 0:
                time.sleep(delay)
    left = prod.flush(60)
    t1 = time.time()
    invalid = sum(v for k, v in kinds.items() if k in CORRUPTIONS)
    return {
        "run_id": run_id,
        "sent": count,
        "valid": count - invalid,
        "invalid": invalid,
        "late": kinds.get("late", 0),
        "kinds": kinds,
        "unflushed": left,
        "started_at": t0,
        "finished_at": t1,
        "seconds": round(t1 - t0, 3),
        "rate": round(count / (t1 - t0), 1) if t1 > t0 else None,
    }


def wikimedia(bootstrap: str, topic: str, max_events: int = 0) -> None:  # pragma: no cover
    import httpx

    prod = _producer(bootstrap)
    last_id: str | None = None
    sent = 0
    backoff = 1.0
    while True:
        headers = {"User-Agent": USER_AGENT, "Accept": "text/event-stream"}
        if last_id:
            headers["Last-Event-ID"] = last_id
        try:
            with httpx.stream("GET", WIKIMEDIA_URL, headers=headers, timeout=httpx.Timeout(30, read=60)) as resp:
                resp.raise_for_status()
                backoff = 1.0
                for line in resp.iter_lines():
                    if line.startswith("id:"):
                        last_id = line[3:].strip()
                    if not line.startswith("data:"):
                        continue
                    try:
                        e = json.loads(line[5:])
                    except ValueError:
                        continue
                    if e.get("meta", {}).get("domain") == "canary":
                        continue
                    e["floe_produced_at"] = int(time.time() * 1000)
                    prod.produce(
                        topic,
                        value=json.dumps(e, separators=(",", ":"), ensure_ascii=False).encode(),
                        key=str(e.get("wiki") or "").encode(),
                    )
                    prod.poll(0)
                    sent += 1
                    if max_events and sent >= max_events:
                        prod.flush(30)
                        return
        except (httpx.HTTPError, OSError) as exc:
            log.warning("wikimedia stream error: %s; reconnecting in %.0fs", exc, backoff)
            prod.flush(10)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


def sample_file() -> Path:
    return Path(str(resources.files("floe.data").joinpath(SAMPLE_NAME)))
