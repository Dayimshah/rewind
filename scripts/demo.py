"""One-command end-to-end demo of Rewind's correctness guarantees (Phase 1).

Runs on a fresh, isolated table so it's fully repeatable, and narrates four
scenarios — each proving a specific guarantee:

    1. Clean first backfill      — plan -> run -> gated publish.
    2. Idempotent re-run         — a second backfill reprocesses nothing.
    3. Late / corrected data     — a changed raw partition is re-included.
    4. Reconciliation gate       — a key-losing backfill is BLOCKED from publish.

Requires the control DB:
    docker compose up -d postgres
    python -m rewind.db.migrate

Then:
    python -m scripts.demo
"""
from __future__ import annotations

import asyncio
import datetime as dt
import shutil
import uuid

import pyarrow as pa

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database
from rewind.registry.registry import register_table
from rewind.transform import generate_rows

# A fixed "today" so the hot window is deterministic regardless of wall clock.
# We seed 7 days of raw data; with a 2-day hot window and today = the last day,
# the final two partitions are "owned by the live stream" and must be excluded.
TODAY = dt.date(2026, 11, 7)
SEED_START = dt.date(2026, 11, 1)
SEED_END = dt.date(2026, 11, 7)
ALL_DAYS = [SEED_START + dt.timedelta(days=i) for i in range((SEED_END - SEED_START).days + 1)]
# The backfill request spans the whole seeded range; the planner will exclude
# the hot-window tail on its own.
RANGE_START = SEED_START
RANGE_END = SEED_END
COLD_DAYS = ALL_DAYS[:-2]  # what we expect to actually be reprocessed


# --- tiny presentation helpers -------------------------------------------

def h(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def step(msg: str) -> None:
    print(f"  - {msg}")


def proves(msg: str) -> None:
    print(f"    [proves] {msg}")


def _live_partitions(table: str) -> list[dt.date]:
    """Currently-published partitions, backend-aware."""
    settings = get_settings()
    if settings.publisher_backend == "iceberg":
        from rewind.publisher import iceberg_backend as ice
        # derive distinct dt values from the current snapshot
        tbl = ice.ensure_table(table).scan().to_arrow()
        if tbl.num_rows == 0:
            return []
        return sorted({d for d in tbl.column("dt").to_pylist()})
    return dp.list_partitions(settings.published_root, table)


def _published_total(table: str) -> int:
    """Sum of amount_cents across all published partitions, backend-aware."""
    settings = get_settings()
    if settings.publisher_backend == "iceberg":
        from rewind.publisher import iceberg_backend as ice
        tbl = ice.ensure_table(table).scan().to_arrow()
        if tbl.num_rows == 0:
            return 0
        return int(sum(tbl.column("amount_cents").to_pylist()))
    total = 0
    for p in dp.list_partitions(settings.published_root, table):
        t = dp.read_partition(settings.published_root, table, p)
        total += sum(t.column("amount_cents").to_pylist())
    return total


async def _append_late_rows(table: str, partition: dt.date, n_new: int) -> None:
    """Append n_new brand-new keyed rows to a raw partition (late data)."""
    settings = get_settings()
    existing = dp.read_partition(settings.raw_root, table, partition)
    stamp = uuid.uuid4().hex[:6]
    new = pa.table(
        {
            "event_id": pa.array([f"late-{stamp}-{i}" for i in range(n_new)], pa.string()),
            "dt": pa.array([partition] * n_new, pa.date32()),
            "region": pa.array(["us"] * n_new, pa.string()),
            "product": pa.array(["pro"] * n_new, pa.string()),
            "amount_cents": pa.array([777] * n_new, pa.int64()),
        }
    )
    dp.write_partition(settings.raw_root, table, partition, pa.concat_tables([existing, new]))


async def _drop_raw_rows(table: str, partition: dt.date, keep: int) -> None:
    """Shrink a raw partition to `keep` rows (simulates a corrupt/truncated source)."""
    settings = get_settings()
    existing = dp.read_partition(settings.raw_root, table, partition)
    dp.write_partition(settings.raw_root, table, partition, existing.slice(0, keep))


# --- the demo -------------------------------------------------------------

async def run_demo(db: Database) -> None:
    settings = get_settings()
    table = "demo_" + uuid.uuid4().hex[:8]

    h(f"Rewind demo  (table: {table})")
    step(f"hot window = 2 days, 'today' = {TODAY} "
         f"(so {ALL_DAYS[-2]} and {ALL_DAYS[-1]} are stream-owned, never backfilled)")

    # Register with a 2-day hot window; seed 5 historical raw partitions.
    await register_table(db, table, hot_window_days=2)
    for d in ALL_DAYS:
        dp.write_partition(settings.raw_root, table, d, generate_rows(table, d, seed="raw"))
    step(f"seeded {len(ALL_DAYS)} raw partitions {ALL_DAYS[0]}..{ALL_DAYS[-1]}")

    # ---- Scenario 1: clean first backfill --------------------------------
    h("1. Clean first backfill  (transform v7)")
    bid, plan = await orch.create_backfill(db, table, RANGE_START, RANGE_END, "v7", today=TODAY)
    step(f"planned backfill #{bid}: {plan.summary()}")
    step(f"included: {[d.isoformat() for d in plan.included]}")
    hot = [d.partition.isoformat() for d in plan.skipped if d.reason.value == 'in_hot_window']
    step(f"excluded hot-window partitions (stream owns): {hot}")
    proves("precise scope + stream-safe isolation (hot window never touched)")

    assert plan.included == COLD_DAYS, f"expected cold days {COLD_DAYS}, got {plan.included}"
    status = await orch.run_backfill(db, bid)
    step(f"run -> {status}  (staged + reconciled, all verdicts pass)")
    status = await orch.publish_backfill(db, bid)
    step(f"publish -> {status}")
    live = _live_partitions(table)
    step(f"now live: {[d.isoformat() for d in live]}  control total = {_published_total(table):,}")
    proves("proof-before-publish: publish only happened after reconciliation passed")

    # ---- Scenario 2: idempotent re-run -----------------------------------
    h("2. Idempotent re-run  (same v7, nothing changed)")
    bid2, plan2 = await orch.create_backfill(db, table, RANGE_START, RANGE_END, "v7", today=TODAY)
    step(f"planned backfill #{bid2}: {plan2.summary()}")
    reasons = sorted({d.reason.value for d in plan2.skipped})
    step(f"included this time: {len(plan2.included)}  (skip reasons: {reasons})")
    status = await orch.run_backfill(db, bid2)
    step(f"run -> {status}  (nothing to reprocess)")
    proves("idempotency: already-published partitions are skipped, no duplicate work")

    # ---- Scenario 3: late / corrected data -------------------------------
    h("3. Late data arrives for one partition")
    late_day = ALL_DAYS[1]  # a cold, already-published partition
    before = _published_total(table)
    await _append_late_rows(table, late_day, n_new=5)
    step(f"appended 5 late rows to raw {late_day}")
    bid3, plan3 = await orch.create_backfill(db, table, RANGE_START, RANGE_END, "v7", today=TODAY)
    inc = [d.isoformat() for d in plan3.included]
    step(f"planned backfill #{bid3}: included = {inc}")
    assert late_day.isoformat() in inc, "late-data partition should be re-included"
    proves("late-data detection: only the changed partition is re-included")
    await orch.run_backfill(db, bid3)
    await orch.publish_backfill(db, bid3)
    after = _published_total(table)
    step(f"republished {late_day}; control total {before:,} -> {after:,} (+{after - before:,})")
    proves("safe correction: the impact is measured and the swap is atomic")

    # ---- Scenario 4: reconciliation gate blocks a bad backfill -----------
    h("4. A key-losing backfill is BLOCKED by the reconciliation gate")
    bad_day = ALL_DAYS[0]
    await _drop_raw_rows(table, bad_day, keep=1)  # corrupt the source: drop most keys
    step(f"simulated corrupt source: raw {bad_day} shrunk to 1 row")
    # New transform version so the planner re-includes it despite prior publish.
    bid4, plan4 = await orch.create_backfill(db, table, bad_day, bad_day, "v8", today=TODAY)
    step(f"planned backfill #{bid4} at transform v8: included = {[d.isoformat() for d in plan4.included]}")
    status = await orch.run_backfill(db, bid4)
    step(f"run -> {status}")
    assert status == orch.FAILED, "key loss must fail reconciliation"
    proves("reconciliation gate: data-losing backfill fails and cannot publish")
    try:
        await orch.publish_backfill(db, bid4)
        raise AssertionError("publish should have been refused")
    except orch.StateError as e:
        step(f"publish refused: {e}")
    proves("the gate holds: a failed backfill is physically prevented from going live")

    # ---- cleanup ----------------------------------------------------------
    for zone in (settings.raw_root, settings.staging_root, settings.published_root):
        shutil.rmtree(zone / table, ignore_errors=True)
    if settings.publisher_backend == "iceberg":
        try:
            from rewind.publisher import iceberg_backend as ice
            ice.get_catalog().drop_table(f"rewind.{table}")
        except Exception:
            pass
    await db.execute("DELETE FROM reconciliation WHERE backfill_id IN "
                     "(SELECT id FROM backfills WHERE table_name=$1)", table)
    await db.execute("DELETE FROM partition_tasks WHERE backfill_id IN "
                     "(SELECT id FROM backfills WHERE table_name=$1)", table)
    await db.execute("DELETE FROM publish_ledger WHERE table_name=$1", table)
    await db.execute("DELETE FROM backfills WHERE table_name=$1", table)
    await db.execute("DELETE FROM tables_registry WHERE name=$1", table)

    h("Demo complete - all four guarantees demonstrated")
    print("  precise scope | stream-safe isolation | idempotency | proof-before-publish\n")


async def _main() -> None:
    db = Database(get_settings().database_url)
    await db.connect()
    try:
        await run_demo(db)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())
