"""Minimal forward-only migration runner.

Applies every ``*.sql`` file in ``rewind/db/migrations`` in lexical order,
recording applied versions in ``schema_migrations`` so re-running is a no-op.

Usage:
    python -m rewind.db.migrate
"""
from __future__ import annotations

import asyncio
from pathlib import Path

from rewind.config import get_settings
from rewind.db.client import Database

MIGRATIONS_DIR = Path(__file__).parent / "migrations"

_TRACKING_TABLE = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version    TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def _migration_files() -> list[Path]:
    return sorted(MIGRATIONS_DIR.glob("*.sql"))


async def run_migrations(db: Database) -> list[str]:
    """Apply pending migrations. Returns the versions newly applied."""
    await db.execute(_TRACKING_TABLE)
    applied = {
        r["version"] for r in await db.fetch("SELECT version FROM schema_migrations")
    }

    newly: list[str] = []
    for path in _migration_files():
        version = path.stem
        if version in applied:
            continue
        sql = path.read_text(encoding="utf-8")
        # Each migration file + its tracking insert run in one transaction.
        async with db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(sql)
                await conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES ($1)", version
                )
        newly.append(version)
        print(f"applied migration: {version}")

    if not newly:
        print("no pending migrations")
    return newly


async def _main() -> None:
    settings = get_settings()
    db = Database(settings.database_url)
    await db.connect()
    try:
        await run_migrations(db)
    finally:
        await db.close()


if __name__ == "__main__":
    asyncio.run(_main())
