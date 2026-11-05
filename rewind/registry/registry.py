"""Table registry (Week 1).

Registering a table tells Rewind it exists, its partition grain, where its
published partitions live (relative to DATA_ROOT), and how many recent days
the live stream owns (the hot window) and backfills must not touch.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass

from rewind.config import get_settings
from rewind.db.client import Database


@dataclass(frozen=True)
class RegisteredTable:
    name: str
    partition_grain: str
    published_path: str
    hot_window_days: int


async def register_table(
    db: Database,
    name: str,
    *,
    partition_grain: str = "day",
    published_path: str | None = None,
    hot_window_days: int | None = None,
) -> RegisteredTable:
    """Upsert a table into the registry.

    Defaults: published_path = "<name>" under the published zone, and
    hot_window_days from settings (HOT_WINDOW_DAYS).
    """
    settings = get_settings()
    if published_path is None:
        published_path = name
    if hot_window_days is None:
        hot_window_days = settings.hot_window_days

    if partition_grain != "day":
        raise ValueError("Phase 1 supports only 'day' partition grain")

    await db.execute(
        """
        INSERT INTO tables_registry
            (name, partition_grain, published_path, hot_window_days)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (name) DO UPDATE SET
            partition_grain = EXCLUDED.partition_grain,
            published_path  = EXCLUDED.published_path,
            hot_window_days = EXCLUDED.hot_window_days
        """,
        name,
        partition_grain,
        published_path,
        hot_window_days,
    )
    return RegisteredTable(name, partition_grain, published_path, hot_window_days)


async def get_table(db: Database, name: str) -> RegisteredTable | None:
    row = await db.fetchrow(
        "SELECT name, partition_grain, published_path, hot_window_days "
        "FROM tables_registry WHERE name = $1",
        name,
    )
    if row is None:
        return None
    return RegisteredTable(
        row["name"], row["partition_grain"], row["published_path"], row["hot_window_days"]
    )


async def list_tables(db: Database) -> list[RegisteredTable]:
    rows = await db.fetch(
        "SELECT name, partition_grain, published_path, hot_window_days "
        "FROM tables_registry ORDER BY name"
    )
    return [
        RegisteredTable(
            r["name"], r["partition_grain"], r["published_path"], r["hot_window_days"]
        )
        for r in rows
    ]


async def _register_demo() -> None:
    """Register the demo table fct_revenue (run migrations first)."""
    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()
    try:
        t = await register_table(db, "fct_revenue")
        print(f"registered: {t}")
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_register_demo())
