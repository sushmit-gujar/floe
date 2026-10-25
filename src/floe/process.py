"""Stream processor: Kafka -> data contract -> Iceberg bronze, exactly-once.

The pure batching logic lives in :class:`Batcher` (unit-tested without a broker);
:func:`run` wires it to a confluent-kafka consumer.
"""

from __future__ import annotations

import json
import logging
import signal
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog

from floe import lake, metrics
from floe.config import Settings
from floe.contract import Contract, ContractViolation

log = logging.getLogger("floe.process")


@dataclass
class FlushResult:
    batch_id: str
    rows: int
    rejected: int
    committed: bool
    attempts: int = 0
    fenced: lake.Fenced | None = None
    offsets: dict[int, int] = field(default_factory=dict)
    latencies: list[float] = field(default_factory=list)


class Batcher:
    """Buffers validated rows per batch and tracks per-partition offset ranges."""

    def __init__(self, contract: Contract, cat: Catalog):
        self.contract = contract
        self.cat = cat
        self.schema = contract.arrow_schema
        self.committed: dict[int, int | None] = {}  # offset committed in Iceberg (batch start)
        self.next: dict[int, int] = {}  # offset after the last buffered message
        self.dirty: set[int] = set()
        self.rows: list[dict[str, Any]] = []
        self.rejects: list[dict[str, Any]] = []
        self.started = time.monotonic()

    # -- partition lifecycle ---------------------------------------------------------
    def assign(self, partitions: list[int]) -> dict[int, int | None]:
        stored = lake.read_offsets(self.cat.load_table(lake.BRONZE))
        out = {}
        for p in partitions:
            self.committed[p] = stored.get(p)
            self.next.pop(p, None)
            out[p] = stored.get(p)
        return out

    def revoke(self, partitions: list[int]) -> None:
        for p in partitions:
            self.committed.pop(p, None)
            self.next.pop(p, None)
            self.dirty.discard(p)

    # -- ingest ------------------------------------------------------------------------
    def add(self, partition: int, offset: int, value: bytes | None) -> None:
        metrics.consumed.inc()
        self.next[partition] = offset + 1
        self.dirty.add(partition)
        try:
            event = json.loads(value) if value is not None else None
            row = self.contract.validate(event)
        except (ValueError, ContractViolation) as exc:
            reason = str(exc) if isinstance(exc, ContractViolation) else "invalid_json"
            metrics.rejected.labels(reason.split(":")[0]).inc()
            payload = value.decode("utf-8", "replace")[:4096] if value is not None else None
            self.rejects.append({"kafka_partition": partition, "kafka_offset": offset, "reason": reason, "payload": payload})
            return
        row["kafka_partition"] = partition
        row["kafka_offset"] = offset
        self.rows.append(row)

    def pending(self) -> int:
        return len(self.rows) + len(self.rejects)

    def age(self) -> float:
        return time.monotonic() - self.started

    # -- commit ------------------------------------------------------------------------
    def flush(self) -> FlushResult | None:
        if not self.dirty:
            self.started = time.monotonic()
            return None
        batch_id = uuid.uuid4().hex
        now = datetime.now(UTC)
        n_rej = len(self.rejects)
        if self.rejects:
            for r in self.rejects:
                r["batch_id"] = batch_id
                r["ingested_at"] = now
            lake.append_rejects(self.cat, self.rejects)  # at-least-once; deduped downstream
            metrics.rows_committed.labels(lake.REJECTED).inc(n_rej)
            self.rejects = []
        if not self.rows:
            # Rejects-only batch: offsets stay uncommitted and are covered by the next data commit.
            self.started = time.monotonic()
            return FlushResult(batch_id, 0, n_rej, committed=False)
        for r in self.rows:
            r["batch_id"] = batch_id
            r["ingested_at"] = now
        data = pa.Table.from_pylist(self.rows, schema=self.schema)
        ranges = {p: (self.committed.get(p), self.next[p]) for p in sorted(self.dirty)}
        t0 = time.monotonic()
        try:
            _, attempts = lake.commit_batch(self.cat, data, ranges, batch_id)
        except lake.Fenced as fenced:
            metrics.commits.labels("fenced").inc()
            log.warning("batch %s fenced: %s; dropping %d rows and re-seeking", batch_id, fenced, len(self.rows))
            self._reset_after_fence()
            return FlushResult(batch_id, 0, n_rej, committed=False, fenced=fenced)
        done = time.time()
        metrics.commit_seconds.observe(time.monotonic() - t0)
        metrics.commits.labels("ok" if attempts == 1 else "retried").inc()
        latencies = [done - r["produced_at"].timestamp() for r in self.rows]
        for lat in latencies:
            metrics.e2e_latency.observe(max(lat, 0.0))
        n = len(self.rows)
        metrics.rows_committed.labels(lake.BRONZE).inc(n)
        metrics.batch_rows.observe(n)
        metrics.last_commit_ts.set(done)
        metrics.last_event_ts.set(max(r["event_time"].timestamp() for r in self.rows))
        offsets = {p: nxt for p, (_s, nxt) in ranges.items()}
        for p, nxt in offsets.items():
            self.committed[p] = nxt
        self.rows = []
        self.dirty.clear()
        self.started = time.monotonic()
        return FlushResult(batch_id, n, n_rej, committed=True, attempts=attempts, offsets=offsets, latencies=latencies)

    def _reset_after_fence(self) -> None:
        self.rows = []
        self.rejects = []
        self.dirty.clear()
        stored = lake.read_offsets(self.cat.load_table(lake.BRONZE))
        for p in list(self.committed):
            self.committed[p] = stored.get(p)
            self.next.pop(p, None)
        self.started = time.monotonic()


def run(s: Settings) -> None:  # pragma: no cover - exercised by the kind e2e test
    from confluent_kafka import OFFSET_BEGINNING, Consumer, KafkaException, TopicPartition

    from floe import httpserver

    contract = Contract.load()
    cat = lake.catalog(s)
    batcher = Batcher(contract, cat)
    metrics.init_series((lake.BRONZE, lake.REJECTED))
    state = {"beat": time.monotonic(), "ready": False, "stop": False}

    httpserver.serve(
        s.http_port,
        live=lambda: time.monotonic() - state["beat"] < 60,
        ready=lambda: bool(state["ready"]),
    )

    consumer = Consumer(
        {
            "bootstrap.servers": s.kafka_bootstrap,
            "group.id": s.group_id,
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "partition.assignment.strategy": "range",
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
            "max.poll.interval.ms": 300000,
            "fetch.min.bytes": 1,
            "queued.max.messages.kbytes": 32768,
        }
    )

    def on_assign(c: Any, parts: list[Any]) -> None:
        stored = batcher.assign([tp.partition for tp in parts])
        for tp in parts:
            off = stored.get(tp.partition)
            tp.offset = off if off is not None else OFFSET_BEGINNING
        c.assign(parts)
        metrics.assigned_partitions.set(len(batcher.committed))
        log.info("assigned %s", {tp.partition: tp.offset for tp in parts})

    def on_revoke(c: Any, parts: list[Any]) -> None:
        try:
            _commit(c, batcher.flush())
        except Exception:  # noqa: BLE001
            log.exception("flush on revoke failed; uncommitted rows will be re-read by the new owner")
        batcher.revoke([tp.partition for tp in parts])
        for tp in parts:
            try:
                metrics.consumer_lag.remove(str(tp.partition))
            except KeyError:
                pass
        metrics.assigned_partitions.set(len(batcher.committed))
        log.info("revoked %s", [tp.partition for tp in parts])

    def _commit(c: Any, res: FlushResult | None) -> None:
        if res is None:
            return
        if res.committed:
            tps = [TopicPartition(s.topic, p, off) for p, off in res.offsets.items()]
            try:
                c.commit(offsets=tps, asynchronous=True)  # lag visibility only (KEDA)
            except KafkaException:
                pass
        elif res.fenced is not None:
            for p, off in batcher.committed.items():
                c.seek(TopicPartition(s.topic, p, off if off is not None else OFFSET_BEGINNING))

    def stop(*_: Any) -> None:
        state["stop"] = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    consumer.subscribe([s.topic], on_assign=on_assign, on_revoke=on_revoke)
    state["ready"] = True
    last_lag = 0.0
    log.info("processor started: topic=%s group=%s", s.topic, s.group_id)
    backoff = 0.0
    retry_at = 0.0
    paused = False
    while not state["stop"]:
        state["beat"] = time.monotonic()
        msgs = consumer.consume(num_messages=5000, timeout=0.5)  # returns nothing while paused, keeps the session alive
        for m in msgs:
            if m.error():
                log.warning("kafka error: %s", m.error())
                continue
            batcher.add(m.partition(), m.offset(), m.value())
        due = batcher.pending() >= s.batch_max_rows or (batcher.dirty and batcher.age() >= s.flush_interval_s)
        if due and time.monotonic() >= retry_at:
            try:
                res = batcher.flush()
            except Exception as exc:  # noqa: BLE001 - catalog/storage outage: keep the buffer, stop consuming, retry
                backoff = min(max(backoff * 2, 0.5), 10.0)
                retry_at = time.monotonic() + backoff
                metrics.commits.labels("error").inc()
                log.warning("commit failed (%s: %s); pausing consumption, retry in %.1fs", type(exc).__name__, exc, backoff)
                if not paused:
                    consumer.pause(consumer.assignment())
                    paused = True
                continue
            backoff = 0.0
            if paused:
                consumer.resume(consumer.assignment())
                paused = False
            _commit(consumer, res)
            if res is not None and res.committed:
                log.info("committed batch %s rows=%d rejected=%d attempts=%d", res.batch_id, res.rows, res.rejected, res.attempts)
        if time.monotonic() - last_lag > 5:
            last_lag = time.monotonic()
            _update_lag(consumer, s.topic, batcher)
    log.info("shutting down: final flush")
    try:
        _commit(consumer, batcher.flush())
    finally:
        consumer.close()


def _update_lag(consumer: Any, topic: str, batcher: Batcher) -> None:  # pragma: no cover
    from confluent_kafka import TopicPartition

    for p in list(batcher.committed):
        try:
            _low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=1.0)
        except Exception:  # noqa: BLE001
            continue
        pos = batcher.next.get(p, batcher.committed.get(p) or 0)
        metrics.consumer_lag.labels(str(p)).set(max(high - pos, 0))
