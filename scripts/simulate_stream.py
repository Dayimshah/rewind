"""Live-stream simulator (Week 1).

Continuously appends fresh rows to the most recent (hot-window) raw partitions
for a table, imitating a live ingestion stream that "owns" recent partitions.
Backfills must never touch these partitions while the stream is live.

Each tick appends a small batch of new, uniquely-keyed rows to each of the last
``HOT_WINDOW_DAYS`` partitions (today and back).

Usage:
    # one tick then exit (good for tests / manual verification)
    python -m scripts.simulate_stream --table fct_revenue --once

    # continuous: a tick every 5 seconds (Ctrl-C to stop)
    python -m scripts.simulate_stream --table fct_revenue --interval 5
"""
from __future__ import annotations

import argparse
import datetime as dt
import time

import pyarrow as pa

from rewind.config import get_settings
from rewind.dataplane import partitions as dp


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Append hot-window stream partitions.")
    p.add_argument("--table", required=True)
    p.add_argument(
        "--hot-window-days",
        type=int,
        default=None,
        help="override HOT_WINDOW_DAYS from settings",
    )
    p.add_argument("--batch", type=int, default=5, help="rows appended per partition per tick")
    p.add_argument("--interval", type=float, default=5.0, help="seconds between ticks")
    p.add_argument("--once", action="store_true", help="run a single tick then exit")
    return p.parse_args()


def _hot_partitions(end: dt.date, hot_window_days: int) -> list[dt.date]:
    return [end - dt.timedelta(days=i) for i in range(hot_window_days)]


def _append_batch(zone_root, table: str, partition: dt.date, batch: int, tick: int) -> int:
    """Append `batch` new rows to a partition. Returns new total row count."""
    now = dt.datetime.now(dt.timezone.utc)
    event_ids, dts, regions, products, amounts = [], [], [], [], []
    for i in range(batch):
        # Keys include a monotonic-ish component so appends never collide.
        event_ids.append(f"stream-{partition.isoformat()}-{tick}-{i}-{now.timestamp():.0f}")
        dts.append(partition)
        regions.append("us")
        products.append("pro")
        amounts.append(500)

    new_rows = pa.table(
        {
            "event_id": pa.array(event_ids, pa.string()),
            "dt": pa.array(dts, pa.date32()),
            "region": pa.array(regions, pa.string()),
            "product": pa.array(products, pa.string()),
            "amount_cents": pa.array(amounts, pa.int64()),
        }
    )

    if dp.partition_exists(zone_root, table, partition):
        existing = dp.read_partition(zone_root, table, partition)
        combined = pa.concat_tables([existing, new_rows])
    else:
        combined = new_rows

    dp.write_partition(zone_root, table, partition, combined)
    return combined.num_rows


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    settings.ensure_dirs()
    hot_days = args.hot_window_days or settings.hot_window_days

    tick = 0
    try:
        while True:
            end = dt.date.today()
            parts = _hot_partitions(end, hot_days)
            for partition in parts:
                total = _append_batch(
                    settings.raw_root, args.table, partition, args.batch, tick
                )
                print(
                    f"[tick {tick}] appended {args.batch} rows to "
                    f"{args.table} dt={partition.isoformat()} (now {total} rows)"
                )
            tick += 1
            if args.once:
                break
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("stream simulator stopped")


if __name__ == "__main__":
    main()
