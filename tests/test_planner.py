"""Accuracy tests for the Scope Planner (Week 2).

Each test registers a uniquely-named table (so the shared control DB tables
don't collide) and writes raw partitions into the session temp DATA_ROOT.
Late-data vs already-published is controlled deterministically by setting the
raw file mtime and the ledger published_at explicitly — no wall-clock races.
"""
from __future__ import annotations

import datetime as dt
import os
import uuid

import pyarrow as pa
import pytest

from rewind.dataplane import partitions as dp
from rewind.planner.models import Action, Reason
from rewind.planner.planner import hot_window_cutoff, plan_scope
from rewind.registry.registry import register_table

TODAY = dt.date(2026, 11, 4)


def _unique_table() -> str:
    return "t_" + uuid.uuid4().hex[:12]


def _write_raw(data_root, table: str, partition: dt.date, mtime: dt.datetime | None = None):
    tbl = pa.table({"event_id": pa.array(["a", "b"]), "amount_cents": pa.array([10, 20])})
    path = dp.write_partition(data_root, table, partition, tbl)
    if mtime is not None:
        ts = mtime.timestamp()
        os.utime(path, (ts, ts))
    return path


async def _publish(db, table, partition, transform, published_at):
    await db.execute(
        """
        INSERT INTO publish_ledger (table_name, partition, transform_version, published_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (table_name, partition, transform_version)
        DO UPDATE SET published_at = EXCLUDED.published_at
        """,
        table,
        partition,
        transform,
        published_at,
    )


def test_hot_window_cutoff_math():
    # 2 hot days including today => cutoff is yesterday.
    assert hot_window_cutoff(TODAY, 2) == dt.date(2026, 11, 3)
    # 1 hot day => cutoff is today.
    assert hot_window_cutoff(TODAY, 1) == dt.date(2026, 11, 4)
    # 0 hot days => cutoff in the future (nothing hot).
    assert hot_window_cutoff(TODAY, 0) == dt.date(2026, 11, 5)


async def test_never_published_partitions_are_included(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    for day in (dt.date(2026, 11, 1), dt.date(2026, 11, 2)):
        _write_raw(data_root, table, day)

    plan = await plan_scope(
        db, table, dt.date(2026, 11, 1), dt.date(2026, 11, 2), "v1", today=TODAY
    )
    assert plan.included == [dt.date(2026, 11, 1), dt.date(2026, 11, 2)]
    assert all(d.reason is Reason.NEVER_PUBLISHED for d in plan.decisions)


async def test_hot_window_partitions_are_skipped(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    # 11-03 and 11-04 are hot (cutoff 11-03); 11-02 is cold.
    for day in (dt.date(2026, 11, 2), dt.date(2026, 11, 3), dt.date(2026, 11, 4)):
        _write_raw(data_root, table, day)

    plan = await plan_scope(
        db, table, dt.date(2026, 11, 2), dt.date(2026, 11, 4), "v1", today=TODAY
    )
    assert plan.included == [dt.date(2026, 11, 2)]
    hot = {d.partition: d for d in plan.skipped}
    assert hot[dt.date(2026, 11, 3)].reason is Reason.IN_HOT_WINDOW
    assert hot[dt.date(2026, 11, 4)].reason is Reason.IN_HOT_WINDOW


async def test_missing_raw_partition_is_skipped(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    _write_raw(data_root, table, dt.date(2026, 11, 1))
    # 11-02 intentionally has no raw data on disk.

    plan = await plan_scope(
        db, table, dt.date(2026, 11, 1), dt.date(2026, 11, 2), "v1", today=TODAY
    )
    assert plan.included == [dt.date(2026, 11, 1)]
    skip = {d.partition: d for d in plan.skipped}
    assert skip[dt.date(2026, 11, 2)].reason is Reason.NO_RAW_DATA


async def test_already_published_unchanged_is_skipped(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    day = dt.date(2026, 11, 1)
    # Raw written well before it was published => unchanged since publish.
    _write_raw(data_root, table, day, mtime=dt.datetime(2026, 11, 1, 6, tzinfo=dt.timezone.utc))
    await _publish(db, table, day, "v1", dt.datetime(2026, 11, 1, 12, tzinfo=dt.timezone.utc))

    plan = await plan_scope(db, table, day, day, "v1", today=TODAY)
    assert plan.included == []
    assert plan.skipped[0].reason is Reason.ALREADY_PUBLISHED


async def test_late_data_after_publish_is_reincluded(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    day = dt.date(2026, 11, 1)
    # Published at noon, then raw corrected at 6pm => late data.
    await _publish(db, table, day, "v1", dt.datetime(2026, 11, 1, 12, tzinfo=dt.timezone.utc))
    _write_raw(data_root, table, day, mtime=dt.datetime(2026, 11, 1, 18, tzinfo=dt.timezone.utc))

    plan = await plan_scope(db, table, day, day, "v1", today=TODAY)
    assert plan.included == [day]
    assert plan.decisions[0].reason is Reason.LATE_DATA


async def test_different_transform_version_is_reincluded(db, data_root):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    day = dt.date(2026, 11, 1)
    _write_raw(data_root, table, day, mtime=dt.datetime(2026, 11, 1, 6, tzinfo=dt.timezone.utc))
    # Published at v1; a backfill for v2 has no ledger entry => must reprocess.
    await _publish(db, table, day, "v1", dt.datetime(2026, 11, 1, 12, tzinfo=dt.timezone.utc))

    plan = await plan_scope(db, table, day, day, "v2", today=TODAY)
    assert plan.included == [day]
    assert plan.decisions[0].reason is Reason.NEVER_PUBLISHED


async def test_inverted_range_raises(db):
    table = _unique_table()
    await register_table(db, table, hot_window_days=2)
    with pytest.raises(ValueError):
        await plan_scope(db, table, dt.date(2026, 11, 3), dt.date(2026, 11, 1), "v1", today=TODAY)


async def test_unregistered_table_raises(db):
    with pytest.raises(ValueError):
        await plan_scope(db, "nope_" + uuid.uuid4().hex[:6], TODAY, TODAY, "v1", today=TODAY)
