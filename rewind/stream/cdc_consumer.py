"""Backfill lifecycle CDC consumer (Phase 2).

Tails the Debezium change topics for the control-plane tables and prints a
human-readable stream of backfill lifecycle events. This is the "activity feed"
that a Run Monitor would subscribe to — proof that control-plane state changes
flow out as real-time events rather than requiring polling.

Usage (connector must be registered):
    python -m rewind.stream.cdc_consumer --drain     # print current backlog, then exit
    python -m rewind.stream.cdc_consumer             # follow live (Ctrl-C to stop)
"""
from __future__ import annotations

import argparse
import json
import time

from confluent_kafka import Consumer

from rewind.stream.config import bootstrap_servers, cdc_topic

_TOPICS = [cdc_topic("backfills"), cdc_topic("partition_tasks")]


def _consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers(),
            "group.id": "rewind-cdc-monitor",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )


def _format(topic: str, payload: dict) -> str | None:
    """Render a Debezium change event as a one-line lifecycle message."""
    op = payload.get("op")  # c=create, u=update, d=delete, r=snapshot
    after = payload.get("after") or {}
    before = payload.get("before") or {}
    op_label = {"c": "created", "u": "updated", "d": "deleted", "r": "snapshot"}.get(op, op)

    if topic.endswith("backfills"):
        bid = after.get("id", before.get("id"))
        status = after.get("status")
        prev = before.get("status")
        if op == "u" and prev != status:
            return f"backfill #{bid}: {prev} -> {status}"
        return f"backfill #{bid} {op_label} (status={status})"

    if topic.endswith("partition_tasks"):
        bid = after.get("backfill_id", before.get("backfill_id"))
        part = after.get("partition", before.get("partition"))
        status = after.get("status")
        return f"  task backfill#{bid} partition={part} {op_label} status={status}"
    return None


def follow(drain: bool, idle_timeout: float = 3.0) -> int:
    c = _consumer()
    c.subscribe(_TOPICS)
    seen = 0
    last_msg = time.monotonic()
    try:
        while True:
            msg = c.poll(0.5)
            now = time.monotonic()
            if msg is not None and not msg.error() and msg.value():
                try:
                    payload = json.loads(msg.value())
                except json.JSONDecodeError:
                    continue
                line = _format(msg.topic(), payload)
                if line:
                    print(line)
                    seen += 1
                last_msg = now
            if drain and (now - last_msg) >= idle_timeout:
                break
    except KeyboardInterrupt:
        print("cdc consumer stopped")
    finally:
        c.close()
    return seen


def main() -> None:
    ap = argparse.ArgumentParser(description="Tail backfill lifecycle CDC events.")
    ap.add_argument("--drain", action="store_true", help="print backlog then exit")
    args = ap.parse_args()
    n = follow(drain=args.drain)
    print(f"({n} lifecycle events)")


if __name__ == "__main__":
    main()
