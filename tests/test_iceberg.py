"""Iceberg publisher backend tests (Phase 2).

Exercise the atomic-snapshot publish + time-travel read that upgrade the
Phase-1 pointer swap. These run only with the Iceberg backend selected; they
leave the default (pointer) behaviour untouched.
"""
from __future__ import annotations

import datetime as dt

import pytest

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.publisher import iceberg_backend as ice
from rewind.registry.registry import register_table
from tests.helpers import unique_table, write_raw

TODAY = dt.date(2026, 11, 20)
DAY = dt.date(2026, 10, 1)


@pytest.fixture
def iceberg_backend():
    """Switch the publisher backend to iceberg for the duration of a test."""
    settings = get_settings()
    # pyiceberg caches the catalog; clear it so each test gets a clean one
    # against the (session) temp warehouse.
    ice.get_catalog.cache_clear()
    prev = settings.publisher_backend
    settings.publisher_backend = "iceberg"
    try:
        yield
    finally:
        settings.publisher_backend = prev


async def test_iceberg_publish_makes_partition_live(db, iceberg_backend):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    write_raw(get_settings().raw_root, table, DAY, n=3)

    bid, plan = await orch.create_backfill(db, table, DAY, DAY, "v7", today=TODAY)
    assert await orch.run_backfill(db, bid) == orch.PUBLISHABLE
    assert await orch.publish_backfill(db, bid) == orch.PUBLISHED

    live = ice.read_published(table, DAY)
    assert live.num_rows == 3
    # exactly one snapshot after the first publish
    assert len(ice.snapshots(table)) == 1


async def test_iceberg_time_travel_across_republish(db, iceberg_backend):
    """Late data creates a new snapshot; the old snapshot is still readable."""
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    write_raw(get_settings().raw_root, table, DAY, n=3)

    bid, _ = await orch.create_backfill(db, table, DAY, DAY, "v7", today=TODAY)
    await orch.run_backfill(db, bid)
    await orch.publish_backfill(db, bid)
    total_v1 = ice.control_total(table, DAY)
    snaps_v1 = ice.snapshots(table)
    assert len(snaps_v1) == 1
    first_snap = snaps_v1[0]["snapshot_id"]

    # Late data: add 2 rows, re-backfill the same partition at same transform.
    write_raw(get_settings().raw_root, table, DAY, n=5)  # 3 -> 5 rows
    bid2, plan2 = await orch.create_backfill(db, table, DAY, DAY, "v7", today=TODAY)
    assert plan2.included == [DAY]  # late data re-included
    await orch.run_backfill(db, bid2)
    await orch.publish_backfill(db, bid2)

    # Current = 5 rows; the republish added snapshot(s). (An overwrite that
    # replaces a partition commits as delete+append, so Iceberg may record more
    # than one new snapshot — we assert history grew, not an exact count.)
    assert ice.read_published(table, DAY).num_rows == 5
    assert len(ice.snapshots(table)) > len(snaps_v1)

    # Time travel: the first snapshot still shows the pre-late-data state.
    old = ice.read_published(table, DAY, snapshot_id=first_snap)
    assert old.num_rows == 3
    assert ice.control_total(table, DAY, snapshot_id=first_snap) == total_v1


async def test_iceberg_republish_identical_is_noop(db, iceberg_backend):
    """Publishing identical staged content does not create a new snapshot."""
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    write_raw(get_settings().raw_root, table, DAY, n=3)

    bid, _ = await orch.create_backfill(db, table, DAY, DAY, "v7", today=TODAY)
    await orch.run_backfill(db, bid)
    await orch.publish_backfill(db, bid)
    snaps = len(ice.snapshots(table))

    # Re-publish the exact same staged partition directly (no new backfill).
    from rewind.publisher.publisher import publish_partition
    r = await publish_partition(db, bid, table, DAY, "v7")
    assert r.published is False
    assert len(ice.snapshots(table)) == snaps  # no extra snapshot
