"""Event-source simulator (Week 1).

Generates N days of historical raw partitions for a table, ending at a given
date (default: today). Deterministic — re-running produces identical data.

Usage:
    python -m scripts.simulate_source --table fct_revenue --days 60
    python -m scripts.simulate_source --table fct_revenue --days 60 --end 2026-11-03
"""
from __future__ import annotations

import argparse
import datetime as dt

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from scripts._gen import generate_rows


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate historical raw partitions.")
    p.add_argument("--table", required=True, help="table name, e.g. fct_revenue")
    p.add_argument("--days", type=int, default=60, help="number of days to generate")
    p.add_argument(
        "--end",
        type=lambda s: dt.date.fromisoformat(s),
        default=dt.date.today(),
        help="last (most recent) partition date, inclusive (default: today)",
    )
    p.add_argument("--seed", default="v0", help="generation seed (transform id)")
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings()
    settings.ensure_dirs()

    start = args.end - dt.timedelta(days=args.days - 1)
    written = 0
    total_rows = 0
    for offset in range(args.days):
        partition = start + dt.timedelta(days=offset)
        table_data = generate_rows(args.table, partition, seed=args.seed)
        dp.write_partition(settings.raw_root, args.table, partition, table_data)
        written += 1
        total_rows += table_data.num_rows

    print(
        f"wrote {written} raw partitions for {args.table} "
        f"[{start.isoformat()}..{args.end.isoformat()}], {total_rows} rows total, "
        f"under {settings.raw_root}"
    )


if __name__ == "__main__":
    main()
