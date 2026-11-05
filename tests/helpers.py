"""Shared test helpers for Rewind."""
from __future__ import annotations

import datetime as dt
import uuid

import pyarrow as pa

from rewind.dataplane import partitions as dp
from rewind.db.client import Database


def unique_table() -> str:
    return "t_" + uuid.uuid4().hex[:12]


def write_raw(data_root, table: str, partition: dt.date, n: int = 3):
    """Write a small deterministic raw partition with n keyed rows."""
    event_ids = [f"ev{i}" for i in range(n)]
    tbl = pa.table(
        {
            "event_id": pa.array(event_ids, pa.string()),
            "dt": pa.array([partition] * n, pa.date32()),
            "region": pa.array(["us"] * n, pa.string()),
            "product": pa.array(["pro"] * n, pa.string()),
            "amount_cents": pa.array([100 * (i + 1) for i in range(n)], pa.int64()),
        }
    )
    return dp.write_partition(data_root, table, partition, tbl)


async def create_backfill(
    db: Database,
    table: str,
    start: dt.date,
    end: dt.date,
    transform_version: str,
) -> int:
    """Insert a backfills row and return its id."""
    return await db.fetchval(
        """
        INSERT INTO backfills (table_name, range_start, range_end, transform_version)
        VALUES ($1, $2, $3, $4) RETURNING id
        """,
        table,
        start,
        end,
        transform_version,
    )
