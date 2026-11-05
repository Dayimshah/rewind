"""Reproducible throughput benchmark (Week 7).

Measures real wall-clock throughput of the backfill pipeline on seeded data.
All numbers printed are measured here — nothing is hard-coded or invented
(per the README's "no invented metrics" rule). Re-running with the same flags
reprocesses the same deterministic data, so results are comparable.

Usage (requires the control DB up: docker compose up -d postgres):
    python -m scripts.benchmark --days 120 --rows 5000 --concurrency 8

It registers a throwaway table, generates raw partitions, then times:
  plan -> stage(execute) -> reconcile -> publish
and reports partitions/sec and rows/sec for the staging step.
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import time
import uuid

import pyarrow as pa

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database
from rewind.registry.registry import register_table


def _gen_partition(table: str, partition: dt.date, rows: int) -> pa.Table:
    day = partition.isoformat()
    event_ids = [f"{day}-{i}" for i in range(rows)]
    return pa.table(
        {
            "event_id": pa.array(event_ids, pa.string()),
            "dt": pa.array([partition] * rows, pa.date32()),
            "region": pa.array(["us"] * rows, pa.string()),
            "product": pa.array(["pro"] * rows, pa.string()),
            "amount_cents": pa.array([100 + (i % 900) for i in range(rows)], pa.int64()),
        }
    )


async def _run(args) -> None:
    settings = get_settings()
    settings.ensure_dirs()
    db = Database(settings.database_url)
    await db.connect()

    table = "bench_" + uuid.uuid4().hex[:8]
    await register_table(db, table, hot_window_days=0)  # no hot window for the bench

    end = dt.date(2026, 1, 1) + dt.timedelta(days=args.days - 1)
    start = dt.date(2026, 1, 1)

    t0 = time.perf_counter()
    for i in range(args.days):
        part = start + dt.timedelta(days=i)
        dp.write_partition(settings.raw_root, table, part, _gen_partition(table, part, args.rows))
    gen_s = time.perf_counter() - t0

    total_rows = args.days * args.rows

    try:
        t0 = time.perf_counter()
        bid, plan = await orch.create_backfill(db, table, start, end, "v7")
        plan_s = time.perf_counter() - t0
        assert len(plan.included) == args.days

        # Time the stage step on its own by calling the executor directly,
        # then let the orchestrator finish reconcile+publish.
        from rewind.executor.executor import execute_backfill
        from rewind.reconciliation.engine import reconcile_backfill
        from rewind.publisher.publisher import publish_backfill

        t0 = time.perf_counter()
        report = await execute_backfill(
            db, bid, table, plan.included, "v7", max_concurrency=args.concurrency
        )
        stage_s = time.perf_counter() - t0
        assert not report.failed

        t0 = time.perf_counter()
        await reconcile_backfill(db, bid, table, [r.partition for r in report.staged])
        recon_s = time.perf_counter() - t0

        t0 = time.perf_counter()
        await publish_backfill(db, bid, table, plan.included, "v7")
        publish_s = time.perf_counter() - t0
    finally:
        # Clean up the benchmark's data + control rows.
        import shutil
        for zone in (settings.raw_root, settings.staging_root, settings.published_root):
            shutil.rmtree(zone / table, ignore_errors=True)
        await db.execute("DELETE FROM publish_ledger WHERE table_name=$1", table)
        await db.execute("DELETE FROM backfills WHERE table_name=$1", table)
        await db.execute("DELETE FROM tables_registry WHERE name=$1", table)
        await db.close()

    def rate(n, s):
        return f"{n / s:,.1f}/s" if s > 0 else "n/a"

    print("=" * 60)
    print(f"Rewind throughput benchmark  (measured, seeded)")
    print(f"  partitions : {args.days}")
    print(f"  rows/part  : {args.rows:,}   total rows: {total_rows:,}")
    print(f"  concurrency: {args.concurrency}")
    print("-" * 60)
    print(f"  generate raw : {gen_s:7.3f}s")
    print(f"  plan         : {plan_s:7.3f}s")
    print(f"  stage        : {stage_s:7.3f}s   "
          f"{rate(args.days, stage_s)} partitions   {rate(total_rows, stage_s)} rows")
    print(f"  reconcile    : {recon_s:7.3f}s   {rate(args.days, recon_s)} partitions")
    print(f"  publish      : {publish_s:7.3f}s   {rate(args.days, publish_s)} partitions")
    print("=" * 60)


def main() -> None:
    p = argparse.ArgumentParser(description="Rewind throughput benchmark")
    p.add_argument("--days", type=int, default=90, help="number of partitions")
    p.add_argument("--rows", type=int, default=2000, help="rows per partition")
    p.add_argument("--concurrency", type=int, default=8)
    asyncio.run(_run(p.parse_args()))


if __name__ == "__main__":
    main()
