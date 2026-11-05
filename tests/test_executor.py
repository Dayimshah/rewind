"""Executor tests (Week 3): idempotency, isolation, concurrency, status."""
from __future__ import annotations

import asyncio
import datetime as dt

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.executor.executor import execute_backfill
from rewind.registry.registry import register_table
from tests.helpers import create_backfill, unique_table, write_raw

DAYS = [dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 3)]


async def _setup(db, data_root, n=3):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    for d in DAYS:
        write_raw(data_root, table, d, n=n)
    bf = await create_backfill(db, table, DAYS[0], DAYS[-1], "v7")
    return table, bf


async def test_executor_stages_all_partitions(db, data_root):
    table, bf = await _setup(db, data_root)
    report = await execute_backfill(db, bf, table, DAYS, "v7")

    assert {r.partition for r in report.staged} == set(DAYS)
    assert report.failed == []
    settings = get_settings()
    for d in DAYS:
        assert dp.partition_exists(settings.staging_root, table, d)

    rows = await db.fetch(
        "SELECT partition, status FROM partition_tasks WHERE backfill_id=$1 ORDER BY partition",
        bf,
    )
    assert [r["status"] for r in rows] == ["staged", "staged", "staged"]


async def test_executor_is_idempotent(db, data_root):
    """Re-running produces byte-identical staged files."""
    table, bf = await _setup(db, data_root)
    settings = get_settings()

    await execute_backfill(db, bf, table, DAYS, "v7")
    first = {d: dp.partition_path(settings.staging_root, table, d).read_bytes() for d in DAYS}

    await execute_backfill(db, bf, table, DAYS, "v7")
    second = {d: dp.partition_path(settings.staging_root, table, d).read_bytes() for d in DAYS}

    assert first == second


async def test_executor_does_not_touch_raw_or_published(db, data_root):
    table, bf = await _setup(db, data_root)
    settings = get_settings()
    raw_before = {d: dp.partition_path(settings.raw_root, table, d).read_bytes() for d in DAYS}

    await execute_backfill(db, bf, table, DAYS, "v7")

    raw_after = {d: dp.partition_path(settings.raw_root, table, d).read_bytes() for d in DAYS}
    assert raw_before == raw_after
    # published zone has nothing for this table
    assert dp.list_partitions(settings.published_root, table) == []


async def test_output_changes_with_late_data(db, data_root):
    """Adding raw rows (late data) changes the staged output row count."""
    table, bf = await _setup(db, data_root, n=3)
    settings = get_settings()
    d = DAYS[0]

    await execute_backfill(db, bf, table, [d], "v7")
    rows_before = dp.row_count(settings.staging_root, table, d)

    # simulate late data: raw now has more rows
    write_raw(data_root, table, d, n=8)
    await execute_backfill(db, bf, table, [d], "v7")
    rows_after = dp.row_count(settings.staging_root, table, d)

    assert rows_before == 3
    assert rows_after == 8


async def test_bounded_concurrency_respected(db, data_root):
    """At most max_concurrency partitions run simultaneously."""
    import rewind.executor.executor as ex

    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    many = [dt.date(2026, 10, 1) + dt.timedelta(days=i) for i in range(10)]
    for d in many:
        write_raw(data_root, table, d)
    bf = await create_backfill(db, table, many[0], many[-1], "v7")

    live = 0
    peak = 0
    lock = asyncio.Lock()
    orig = ex._reprocess_one_sync

    # Wrap the sync reprocess with an async probe via monkeypatching the thread call.
    original_to_thread = asyncio.to_thread

    async def probe_to_thread(func, *a, **k):
        nonlocal live, peak
        async with lock:
            live += 1
            peak = max(peak, live)
        try:
            await asyncio.sleep(0.02)  # hold the "slot" briefly
            return await original_to_thread(func, *a, **k)
        finally:
            async with lock:
                live -= 1

    asyncio.to_thread = probe_to_thread  # type: ignore
    try:
        await execute_backfill(db, bf, table, many, "v7", max_concurrency=3)
    finally:
        asyncio.to_thread = original_to_thread  # type: ignore

    assert peak <= 3
    assert peak >= 2  # actually ran in parallel
