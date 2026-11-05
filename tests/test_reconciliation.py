"""Reconciliation tests (Week 5): metrics, verdicts, persistence."""
from __future__ import annotations

import datetime as dt

import pyarrow as pa

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.reconciliation.engine import (
    VERDICT_FAIL,
    VERDICT_PASS,
    reconcile_partition,
)
from rewind.registry.registry import register_table
from tests.helpers import create_backfill, unique_table

DAY = dt.date(2026, 10, 1)


def _write(zone_root, table, keys_amounts: dict[str, int]):
    keys = list(keys_amounts)
    tbl = pa.table(
        {
            "event_id": pa.array(keys, pa.string()),
            "dt": pa.array([DAY] * len(keys), pa.date32()),
            "region": pa.array(["us"] * len(keys), pa.string()),
            "product": pa.array(["pro"] * len(keys), pa.string()),
            "amount_cents": pa.array([keys_amounts[k] for k in keys], pa.int64()),
        }
    )
    dp.write_partition(zone_root, table, DAY, tbl)


async def _bf(db, table):
    await register_table(db, table, hot_window_days=2)
    return await create_backfill(db, table, DAY, DAY, "v7")


async def test_first_publish_passes_all_added(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    # staged only, nothing published yet
    _write(settings.staging_root, table, {"a": 100, "b": 200, "c": 300})

    m = await reconcile_partition(db, bf, table, DAY)
    assert m.verdict == VERDICT_PASS
    assert m.published_row_count == 0
    assert m.staged_row_count == 3
    assert m.keyset_added == 3
    assert m.keyset_removed == 0
    assert m.control_total_before == 0
    assert m.control_total_after == 600


async def test_changed_totals_same_keys_passes(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    # same keys, corrected amounts — the whole point of a backfill
    _write(settings.published_root, table, {"a": 100, "b": 200})
    _write(settings.staging_root, table, {"a": 150, "b": 250})

    m = await reconcile_partition(db, bf, table, DAY)
    assert m.verdict == VERDICT_PASS
    assert m.keyset_added == 0
    assert m.keyset_removed == 0
    assert m.control_total_before == 300
    assert m.control_total_after == 400


async def test_key_loss_fails(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    # published has 3 keys; staged dropped one -> data loss -> FAIL
    _write(settings.published_root, table, {"a": 100, "b": 200, "c": 300})
    _write(settings.staging_root, table, {"a": 100, "b": 200})

    m = await reconcile_partition(db, bf, table, DAY)
    assert m.verdict == VERDICT_FAIL
    assert m.keyset_removed == 1
    assert any("removed" in r for r in m.reasons)


async def test_key_loss_allowed_with_threshold(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    _write(settings.published_root, table, {"a": 100, "b": 200, "c": 300})
    _write(settings.staging_root, table, {"a": 100, "b": 200})

    m = await reconcile_partition(db, bf, table, DAY, max_removed_keys=1)
    assert m.verdict == VERDICT_PASS


async def test_empty_staged_fails(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    _write(settings.published_root, table, {"a": 100})
    _write(settings.staging_root, table, {})  # empty staged

    m = await reconcile_partition(db, bf, table, DAY)
    assert m.verdict == VERDICT_FAIL
    assert any("empty" in r for r in m.reasons)


async def test_metrics_persisted(db):
    settings = get_settings()
    table = unique_table()
    bf = await _bf(db, table)
    _write(settings.staging_root, table, {"a": 100, "b": 200})

    await reconcile_partition(db, bf, table, DAY)
    row = await db.fetchrow(
        "SELECT staged_row_count, verdict, details FROM reconciliation "
        "WHERE backfill_id=$1 AND partition=$2",
        bf, DAY,
    )
    assert row["staged_row_count"] == 2
    assert row["verdict"] == VERDICT_PASS
    assert row["details"] is not None
