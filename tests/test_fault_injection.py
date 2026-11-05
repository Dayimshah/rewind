"""Fault-injection + whole-pipeline idempotency/concurrency tests (Week 7).

These exercise failure and retry paths across the full engine rather than a
single component, proving the correctness guarantees hold end-to-end.
"""
from __future__ import annotations

import asyncio
import datetime as dt

import rewind.executor.executor as ex
from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.registry.registry import register_table
from tests.helpers import unique_table, write_raw

TODAY = dt.date(2026, 11, 20)
DAYS = [dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 3)]


async def _ready(db, n_days=3, rows=3):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    for d in DAYS[:n_days]:
        write_raw(get_settings().raw_root, table, d, n=rows)
    return table


async def test_executor_failure_marks_failed_then_retry_succeeds(db, monkeypatch):
    """Inject a transform failure on one partition; backfill -> FAILED; fix -> retry succeeds."""
    table = await _ready(db)
    bid, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[-1], "v7", today=TODAY)

    # Fault: make the reprocess of the 2nd day raise once.
    real = ex._reprocess_one_sync
    boom_day = DAYS[1]

    def flaky(tbl, partition, transform_version):
        if partition == boom_day:
            raise RuntimeError("injected disk error")
        return real(tbl, partition, transform_version)

    monkeypatch.setattr(ex, "_reprocess_one_sync", flaky)
    status = await orch.run_backfill(db, bid)
    assert status == orch.FAILED

    # The failed partition is recorded as failed with the error text.
    row = await db.fetchrow(
        "SELECT status, error, attempt FROM partition_tasks WHERE backfill_id=$1 AND partition=$2",
        bid, boom_day,
    )
    assert row["status"] == "failed"
    assert "injected" in row["error"]

    # Remove the fault and retry from FAILED -> should reach PUBLISHABLE.
    monkeypatch.setattr(ex, "_reprocess_one_sync", real)
    status = await orch.run_backfill(db, bid)
    assert status == orch.PUBLISHABLE

    # attempt count incremented (ran at least twice for the boom day).
    row = await db.fetchrow(
        "SELECT status, attempt FROM partition_tasks WHERE backfill_id=$1 AND partition=$2",
        bid, boom_day,
    )
    assert row["status"] == "staged"
    assert row["attempt"] >= 2


async def test_whole_pipeline_rerun_is_idempotent(db):
    """Running the full lifecycle twice yields identical published bytes + no duplicate ledger."""
    table = await _ready(db)
    settings = get_settings()

    bid, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[-1], "v7", today=TODAY)
    await orch.run_backfill(db, bid)
    await orch.publish_backfill(db, bid)
    first = {d: dp.partition_path(settings.published_root, table, d).read_bytes() for d in DAYS}
    ledger1 = await db.fetchval("SELECT count(*) FROM publish_ledger WHERE table_name=$1", table)

    # A second, independent backfill for the same (table, range, transform).
    bid2, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[-1], "v7", today=TODAY)
    # Planner should include nothing (already published, raw unchanged).
    status = await orch.run_backfill(db, bid2)
    assert status == orch.PUBLISHABLE
    await orch.publish_backfill(db, bid2)

    second = {d: dp.partition_path(settings.published_root, table, d).read_bytes() for d in DAYS}
    ledger2 = await db.fetchval("SELECT count(*) FROM publish_ledger WHERE table_name=$1", table)

    assert first == second          # byte-identical published data
    assert ledger1 == ledger2 == 3  # no duplicate ledger rows


async def test_concurrent_backfills_on_disjoint_tables(db):
    """Two backfills on different tables run concurrently without interfering."""
    t1 = await _ready(db)
    t2 = await _ready(db)
    b1, _ = await orch.create_backfill(db, t1, DAYS[0], DAYS[-1], "v7", today=TODAY)
    b2, _ = await orch.create_backfill(db, t2, DAYS[0], DAYS[-1], "v7", today=TODAY)

    s1, s2 = await asyncio.gather(
        orch.run_backfill(db, b1), orch.run_backfill(db, b2)
    )
    assert s1 == s2 == orch.PUBLISHABLE
    await asyncio.gather(orch.publish_backfill(db, b1), orch.publish_backfill(db, b2))

    settings = get_settings()
    assert set(dp.list_partitions(settings.published_root, t1)) == set(DAYS)
    assert set(dp.list_partitions(settings.published_root, t2)) == set(DAYS)


async def test_staging_isolation_from_live_stream(db):
    """A backfill never writes into the hot-window partitions the stream owns."""
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    settings = get_settings()
    # Cold days + two hot days relative to a 'today' near the data.
    today = dt.date(2026, 10, 5)
    cold = [dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 3)]
    hot = [dt.date(2026, 10, 4), dt.date(2026, 10, 5)]
    for d in cold + hot:
        write_raw(settings.raw_root, table, d)

    bid, plan = await orch.create_backfill(
        db, table, cold[0], hot[-1], "v7", today=today
    )
    assert set(plan.included) == set(cold)  # hot excluded
    await orch.run_backfill(db, bid)

    # Nothing staged for the hot-window days.
    for d in hot:
        assert not dp.partition_exists(settings.staging_root, table, d)
    for d in cold:
        assert dp.partition_exists(settings.staging_root, table, d)
