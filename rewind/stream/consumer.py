"""Live event-stream consumer (Phase 2).

Reads events off the Kafka stream topic and lands them as raw partitions in the
data plane — this consumer is what "owns" the hot window. It batches events by
their ``dt`` and appends each batch to the corresponding raw partition.

Because this writes real partitions from a real stream, a concurrently running
backfill that respected the hot window provably never collides with it.

Usage:
    # drain whatever is currently on the topic, then exit
    python -m rewind.stream.consumer --table fct_revenue --drain

    # run continuously, flushing every `interval` seconds
    python -m rewind.stream.consumer --table fct_revenue --interval 2
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
from collections import defaultdict

import pyarrow as pa
from confluent_kafka import Consumer

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.stream.config import bootstrap_servers, stream_topic


def _consumer(group: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def _land_batch(table: str, by_day: dict[dt.date, list[dict]]) -> dict[dt.date, int]:
    """Append buffered events to their raw partitions. Returns new row counts."""
    settings = get_settings()
    settings.ensure_dirs()
    totals: dict[dt.date, int] = {}
    for day, events in by_day.items():
        new_rows = pa.table(
            {
                "event_id": pa.array([e["event_id"] for e in events], pa.string()),
                "dt": pa.array([day] * len(events), pa.date32()),
                "region": pa.array([e.get("region", "us") for e in events], pa.string()),
                "product": pa.array([e.get("product", "pro") for e in events], pa.string()),
                "amount_cents": pa.array([int(e["amount_cents"]) for e in events], pa.int64()),
            }
        )
        if dp.partition_exists(settings.raw_root, table, day):
            existing = dp.read_partition(settings.raw_root, table, day)
            combined = pa.concat_tables([existing, new_rows])
        else:
            combined = new_rows
        dp.write_partition(settings.raw_root, table, day, combined)
        totals[day] = combined.num_rows
    return totals


def consume(table: str, *, drain: bool, interval: float, idle_timeout: float = 3.0) -> int:
    """Consume stream events into raw partitions.

    drain=True: read until no new messages for `idle_timeout`s, then stop.
    drain=False: loop forever, flushing a batch every `interval`s.
    Returns the total number of events landed.
    """
    topic = stream_topic(table)
    c = _consumer(group=f"rewind-stream-{table}")
    c.subscribe([topic])

    landed = 0
    buf: dict[dt.date, list[dict]] = defaultdict(list)
    import time

    last_flush = time.monotonic()
    last_msg = time.monotonic()
    try:
        while True:
            msg = c.poll(0.5)
            now = time.monotonic()
            if msg is not None and not msg.error():
                ev = json.loads(msg.value())
                day = dt.date.fromisoformat(ev["dt"])
                buf[day].append(ev)
                last_msg = now

            flush_due = (now - last_flush) >= interval
            if buf and (flush_due or drain):
                totals = _land_batch(table, dict(buf))
                for day, total in totals.items():
                    landed += len(buf[day])
                    print(f"landed {len(buf[day])} events -> {table} dt={day} (now {total} rows)")
                buf.clear()
                last_flush = now

            if drain and (now - last_msg) >= idle_timeout and not buf:
                break
    except KeyboardInterrupt:
        if buf:
            _land_batch(table, dict(buf))
        print("consumer stopped")
    finally:
        c.close()
    return landed


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Land stream events into raw partitions.")
    ap.add_argument("--table", required=True)
    ap.add_argument("--drain", action="store_true", help="drain current backlog then exit")
    ap.add_argument("--interval", type=float, default=2.0, help="flush interval (continuous mode)")
    return ap.parse_args()


def main() -> None:
    args = _parse_args()
    n = consume(args.table, drain=args.drain, interval=args.interval)
    print(f"total events landed: {n}")


if __name__ == "__main__":
    main()
