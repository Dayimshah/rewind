"""Orchestrator / state-machine tests (Week 6)."""
from __future__ import annotations

import datetime as dt

import pytest

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.registry.registry import register_table
from tests.helpers import unique_table, write_raw

# All partitions cold relative to this "today".
TODAY = dt.date(2026, 11, 20)
DAYS = [dt.date(2026, 10, 1), dt.date(2026, 10, 2), dt.date(2026, 10, 3)]


async def _setup(db):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    data_root = get_settings().raw_root
    for d in DAYS:
        write_raw(data_root, table, d)
    return table


async def test_full_happy_path(db):
    table = await _setup(db)
    settings = get_settings()

    bid, plan = await orch.create_backfill(
        db, table, DAYS[0], DAYS[-1], "v7", today=TODAY
    )
    assert plan.included == DAYS
    bf = await orch.get_backfill(db, bid)
    assert bf.status == orch.PLANNED

    status = await orch.run_backfill(db, bid)
    assert status == orch.PUBLISHABLE

    # publish gate opens
    status = await orch.publish_backfill(db, bid)
    assert status == orch.PUBLISHED

    # all partitions live + ledgered
    assert set(dp.list_partitions(settings.published_root, table)) == set(DAYS)
    n = await db.fetchval(
        "SELECT count(*) FROM publish_ledger WHERE table_name=$1", table
    )
    assert n == 3


async def test_publish_rejected_before_run(db):
    table = await _setup(db)
    bid, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[-1], "v7", today=TODAY)
    # status is 'planned' -> publishing must be refused
    with pytest.raises(orch.StateError):
        await orch.publish_backfill(db, bid)


async def test_failed_recon_blocks_publish(db):
    """A key-loss between published and re-staged data fails recon and blocks publish."""
    table = await _setup(db)
    settings = get_settings()

    # First backfill at v7, publish it.
    bid1, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[0], "v7", today=TODAY)
    await orch.run_backfill(db, bid1)
    await orch.publish_backfill(db, bid1)

    # Now corrupt the raw so the next transform drops keys: shrink raw to fewer rows.
    write_raw(settings.raw_root, table, DAYS[0], n=1)  # was 3 keys, now 1

    # New backfill at a new transform version so it's re-included.
    bid2, _ = await orch.create_backfill(db, table, DAYS[0], DAYS[0], "v8", today=TODAY)
    status = await orch.run_backfill(db, bid2)
    assert status == orch.FAILED

    with pytest.raises(orch.StateError):
        await orch.publish_backfill(db, bid2)


async def test_empty_scope_is_publishable_noop(db):
    """A backfill whose scope is empty is trivially publishable."""
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    # no raw data written -> planner includes nothing
    bid, plan = await orch.create_backfill(db, table, DAYS[0], DAYS[-1], "v7", today=TODAY)
    assert plan.included == []
    status = await orch.run_backfill(db, bid)
    assert status == orch.PUBLISHABLE
