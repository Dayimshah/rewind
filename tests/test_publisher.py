"""Publisher tests (Week 4): idempotency, atomic pointer swap, crash recovery."""
from __future__ import annotations

import datetime as dt

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.dataplane import pointer as ptr
from rewind.executor.executor import execute_backfill
from rewind.publisher.publisher import publish_backfill, publish_partition
from rewind.registry.registry import register_table
from tests.helpers import create_backfill, unique_table, write_raw

DAYS = [dt.date(2026, 10, 1), dt.date(2026, 10, 2)]


async def _staged(db, data_root, transform="v7"):
    """Register, write raw, create backfill, and stage both partitions."""
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    for d in DAYS:
        write_raw(data_root, table, d)
    bf = await create_backfill(db, table, DAYS[0], DAYS[-1], transform)
    await execute_backfill(db, bf, table, DAYS, transform)
    return table, bf


async def test_publish_makes_partition_live(db, data_root):
    table, bf = await _staged(db, data_root)
    settings = get_settings()

    results = await publish_backfill(db, bf, table, DAYS, "v7")
    assert all(r.published for r in results)

    # published zone now has both partitions, and the pointer references them.
    assert set(dp.list_partitions(settings.published_root, table)) == set(DAYS)
    pointer = ptr.load_pointer(settings.published_root, table)
    assert set(pointer["partitions"].keys()) == {d.isoformat() for d in DAYS}
    assert pointer["version"] == 2  # one bump per partition

    # ledger recorded both.
    n = await db.fetchval(
        "SELECT count(*) FROM publish_ledger WHERE table_name=$1 AND transform_version=$2",
        table, "v7",
    )
    assert n == 2


async def test_publish_is_idempotent(db, data_root):
    table, bf = await _staged(db, data_root)
    settings = get_settings()

    first = await publish_backfill(db, bf, table, DAYS, "v7")
    bytes_after_first = {
        d: dp.partition_path(settings.published_root, table, d).read_bytes() for d in DAYS
    }
    pointer_v1 = ptr.load_pointer(settings.published_root, table)["version"]

    # Re-publish: must be a no-op (no new ledger rows, no pointer bump, same bytes).
    second = await publish_backfill(db, bf, table, DAYS, "v7")
    assert all(r.published for r in first)
    assert all(not r.published for r in second)

    bytes_after_second = {
        d: dp.partition_path(settings.published_root, table, d).read_bytes() for d in DAYS
    }
    pointer_v2 = ptr.load_pointer(settings.published_root, table)["version"]

    assert bytes_after_first == bytes_after_second
    assert pointer_v1 == pointer_v2
    n = await db.fetchval(
        "SELECT count(*) FROM publish_ledger WHERE table_name=$1", table
    )
    assert n == 2


async def test_crash_before_ledger_insert_recovers(db, data_root):
    """Simulate a crash after data+pointer but before the ledger row, then re-run."""
    table, bf = await _staged(db, data_root)
    settings = get_settings()
    day = DAYS[0]

    # First publish fully.
    await publish_partition(db, bf, table, day, "v7")
    published_bytes = dp.partition_path(settings.published_root, table, day).read_bytes()

    # Simulate the "crash": delete the ledger row, leaving data+pointer live but
    # unrecorded. A recovery re-run must converge.
    await db.execute(
        "DELETE FROM publish_ledger WHERE table_name=$1 AND partition=$2 AND transform_version=$3",
        table, day, "v7",
    )

    result = await publish_partition(db, bf, table, day, "v7")
    # Content-based idempotency: the data was already live and identical, so the
    # recovery is a data no-op (published=False) that simply restores the ledger
    # row. Convergence is proven by identical bytes + the ledger back to one row.
    assert result.published is False
    assert dp.partition_path(settings.published_root, table, day).read_bytes() == published_bytes
    n = await db.fetchval(
        "SELECT count(*) FROM publish_ledger WHERE table_name=$1 AND partition=$2",
        table, day,
    )
    assert n == 1


async def test_late_data_republishes_under_same_transform(db, data_root):
    """Regression: changed staged content must republish even at the same transform.

    The ledger key (table, partition, transform_version) alone is too coarse —
    a late-data correction keeps the same transform but changes the output, and
    it must still reach the published zone. Idempotency is content-based.
    """
    import pyarrow as pa

    table, bf = await _staged(db, data_root)
    settings = get_settings()
    day = DAYS[0]

    r1 = await publish_partition(db, bf, table, day, "v7")
    assert r1.published
    rows_before = dp.row_count(settings.published_root, table, day)

    # Late data: append new raw rows, then re-stage the same partition at v7.
    existing = dp.read_partition(settings.raw_root, table, day)
    extra = pa.table({
        "event_id": pa.array(["late1", "late2"], pa.string()),
        "dt": pa.array([day, day], pa.date32()),
        "region": pa.array(["us", "us"], pa.string()),
        "product": pa.array(["pro", "pro"], pa.string()),
        "amount_cents": pa.array([500, 500], pa.int64()),
    })
    dp.write_partition(settings.raw_root, table, day, pa.concat_tables([existing, extra]))
    await execute_backfill(db, bf, table, [day], "v7")

    # Re-publishing must now update the published partition (not a no-op).
    r2 = await publish_partition(db, bf, table, day, "v7")
    assert r2.published
    rows_after = dp.row_count(settings.published_root, table, day)
    assert rows_after == rows_before + 2


async def test_partition_tasks_marked_published(db, data_root):
    table, bf = await _staged(db, data_root)
    await publish_backfill(db, bf, table, DAYS, "v7")
    rows = await db.fetch(
        "SELECT status FROM partition_tasks WHERE backfill_id=$1", bf
    )
    assert all(r["status"] == "published" for r in rows)
