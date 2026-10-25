"""floe command line: one image, one entrypoint, many roles."""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time

from floe import __version__
from floe.config import Settings


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="floe", description="Streaming Iceberg lakehouse on Kubernetes")
    ap.add_argument("--version", action="version", version=__version__)
    ap.add_argument("-v", "--verbose", action="store_true")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("bootstrap", help="create bucket, warehouse, tables and topic (idempotent)")
    sub.add_parser("process", help="run the stream processor")
    w = sub.add_parser("wait-ready", help="block until bootstrap created the tables and topic")
    w.add_argument("--timeout", type=float, default=600)
    w.add_argument("--no-kafka", action="store_true", help="only wait for the catalog tables")
    w.add_argument("--no-catalog", action="store_true", help="only wait for the topic (Kafka-only clients)")

    p = sub.add_parser("produce", help="produce events")
    p.add_argument("mode", choices=["replay", "wikimedia"])
    p.add_argument("--run-id", default=None)
    p.add_argument("--count", type=int, default=10000)
    p.add_argument("--rate", type=float, default=0, help="events/s, 0 = as fast as possible")
    p.add_argument("--invalid-ratio", type=float, default=0.0)
    p.add_argument("--late-ratio", type=float, default=0.0)
    p.add_argument("--seed", type=int, default=7)
    p.add_argument("--max-events", type=int, default=0)

    t = sub.add_parser("transform", help="dbt build over bronze, publish gold if tests pass")
    t.add_argument("--no-publish", action="store_true")
    t.add_argument("--workdir", default=None)

    a = sub.add_parser("api", help="serve the query API")
    a.add_argument("--port", type=int, default=8000)

    v = sub.add_parser("verify", help="check a replay run landed exactly once; print latency/throughput")
    v.add_argument("--run-id", required=True)
    v.add_argument("--expect-valid", type=int, required=True)
    v.add_argument("--expect-rejected", type=int, default=None)
    v.add_argument("--timeout", type=float, default=600)
    v.add_argument("--produced-from", type=float, default=None)

    m = sub.add_parser("maintain", help="compact closed partitions and expire snapshots")
    m.add_argument("--expire-hours", type=int, default=24)

    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":"%(message)s"}',
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)
    s = Settings()

    if args.cmd == "bootstrap":
        from floe import bootstrap

        bootstrap.run(s)
    elif args.cmd == "wait-ready":
        from floe import bootstrap

        bootstrap.wait_ready(s, args.timeout, kafka=not args.no_kafka, catalog=not args.no_catalog)
    elif args.cmd == "process":
        from floe import process

        process.run(s)
    elif args.cmd == "produce":
        from floe import produce

        if args.mode == "replay":
            run_id = args.run_id or f"r{int(time.time())}"
            print(
                json.dumps(
                    produce.replay(
                        s.kafka_bootstrap, s.topic, run_id, args.count, args.rate, args.invalid_ratio, args.late_ratio, args.seed
                    )
                )
            )
        else:
            produce.wikimedia(s.kafka_bootstrap, s.topic, args.max_events)
    elif args.cmd == "transform":
        from floe import transform

        out = transform.run(s, workdir=args.workdir, publish=not args.no_publish)
        print(json.dumps(out, default=str))
        return 0 if out["status"] == "pass" else 1
    elif args.cmd == "api":
        import uvicorn

        from floe.api import create_app

        uvicorn.run(create_app(s), host="0.0.0.0", port=args.port, log_level="warning")
    elif args.cmd == "verify":
        from floe import lake, verify

        out = verify.wait_and_verify(
            lake.catalog(s),
            args.run_id,
            args.expect_valid,
            args.expect_rejected,
            timeout=args.timeout,
            produced_from=args.produced_from,
        )
        print(json.dumps(out, default=str))
        ok = out["exactly_once"] and out.get("rejects_ok", True)
        return 0 if ok else 1
    elif args.cmd == "maintain":
        from floe import lake, maintain

        cat = lake.catalog(s)
        print(json.dumps({"compacted": maintain.compact_closed_days(cat), "expired": maintain.expire(cat, args.expire_hours)}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
