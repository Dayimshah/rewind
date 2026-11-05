"""Live event-stream producer (Phase 2).

Emits synthetic revenue events to a Kafka topic, imitating a real ingestion
stream. Events carry a date within the hot window — the consumer lands them as
raw partitions, which is precisely the data a running backfill must NOT touch.

This replaces the Phase-1 "append to Parquet" simulator with a genuine stream,
making the hot-window / stream-collision problem real rather than simulated.

Usage:
    python -m rewind.stream.producer --table fct_revenue --count 50
    python -m rewind.stream.producer --table fct_revenue --rate 10   # ~10/s forever
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import time
import uuid

from confluent_kafka import Producer

from rewind.stream.config import bootstrap_servers, stream_topic


def _make_event(table: str, when: dt.date) -> dict:
    return {
        "event_id": f"stream-{uuid.uuid4().hex[:12]}",
        "dt": when.isoformat(),
        "region": "us",
        "product": "pro",
        "amount_cents": 500,
        "emitted_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }


def _producer() -> Producer:
    return Producer({"bootstrap.servers": bootstrap_servers()})


def emit(table: str, count: int, *, day: dt.date | None = None) -> int:
    """Emit `count` events for a table (defaults to today's partition)."""
    when = day or dt.date.today()
    p = _producer()
    topic = stream_topic(table)
    sent = 0
    for _ in range(count):
        ev = _make_event(table, when)
        p.produce(topic, key=ev["event_id"], value=json.dumps(ev).encode())
        sent += 1
        p.poll(0)
    p.flush(10)
    return sent


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Emit live stream events to Kafka.")
    ap.add_argument("--table", required=True)
    ap.add_argument("--count", type=int, default=50, help="number of events then exit")
    ap.add_argument("--rate", type=float, default=0.0, help="events/sec; >0 runs forever")
    ap.add_argument("--day", type=lambda s: dt.date.fromisoformat(s), default=None)
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    topic = stream_topic(args.table)
    if args.rate and args.rate > 0:
        print(f"producing ~{args.rate}/s to {topic} (Ctrl-C to stop)")
        interval = 1.0 / args.rate
        try:
            while True:
                emit(args.table, 1, day=args.day)
                time.sleep(interval)
        except KeyboardInterrupt:
            print("producer stopped")
    else:
        n = emit(args.table, args.count, day=args.day)
        print(f"emitted {n} events to {topic}")


if __name__ == "__main__":
    main()
