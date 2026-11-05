"""Startup bootstrap for deployed environments (Phase 2 / deploy).

On a fresh free-tier deploy there's no schema and no data, so the control-room
UI would show an empty backend. This module optionally:

  * runs migrations on boot        (RUN_MIGRATIONS_ON_START=1, default on)
  * seeds a demo backfill on boot  (SEED_ON_START=1, default on)

Both are idempotent and safe to run every cold start. Seeding registers
``fct_revenue``, generates a small set of historical raw partitions, and runs
one backfill through to ``published`` so the UI (GET /backfills/1) has real,
reconciled data to display. On ephemeral-disk hosts the Parquet is regenerated
each boot; that's intentional and cheap.

Guarded so local/dev and the test suite are unaffected (both default off unless
the env vars are set — see lifespan wiring).
"""
from __future__ import annotations

import datetime as dt
import os

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database
from rewind.db.migrate import run_migrations
from rewind.registry.registry import get_table, register_table
from rewind.transform import generate_rows

_DEMO_TABLE = "fct_revenue"
_DEMO_DAYS = 45
_DEMO_END = dt.date(2026, 11, 2)  # all cold relative to the seed's "today"
_SEED_TODAY = dt.date(2026, 11, 20)


def _truthy(name: str, default: str = "1") -> bool:
    return os.environ.get(name, default).lower() in ("1", "true", "yes", "on")


async def _seed_demo(db: Database) -> None:
    """Register fct_revenue + run one backfill to 'published' (idempotent)."""
    settings = get_settings()
    settings.ensure_dirs()

    # Already seeded this boot? (registered + has at least one backfill)
    existing = await get_table(db, _DEMO_TABLE)
    has_backfill = await db.fetchval(
        "SELECT 1 FROM backfills WHERE table_name=$1 LIMIT 1", _DEMO_TABLE
    )
    if existing is not None and has_backfill:
        return

    await register_table(db, _DEMO_TABLE, hot_window_days=2)

    start = _DEMO_END - dt.timedelta(days=_DEMO_DAYS - 1)
    for i in range(_DEMO_DAYS):
        day = start + dt.timedelta(days=i)
        if not dp.partition_exists(settings.raw_root, _DEMO_TABLE, day):
            dp.write_partition(
                settings.raw_root, _DEMO_TABLE, day,
                generate_rows(_DEMO_TABLE, day, seed="raw"),
            )

    bid, _ = await orch.create_backfill(
        db, _DEMO_TABLE, start, _DEMO_END, "v7", today=_SEED_TODAY
    )
    status = await orch.run_backfill(db, bid)
    if status == orch.PUBLISHABLE:
        await orch.publish_backfill(db, bid)


async def bootstrap(db: Database) -> None:
    """Run migrations and seed demo data per env flags. Never fatal."""
    if _truthy("RUN_MIGRATIONS_ON_START", "0"):
        try:
            await run_migrations(db)
        except Exception as e:  # noqa: BLE001 — log-and-continue; health stays up
            print(f"[bootstrap] migration error (continuing): {e}")

    if _truthy("SEED_ON_START", "0"):
        try:
            await _seed_demo(db)
        except Exception as e:  # noqa: BLE001
            print(f"[bootstrap] seed error (continuing): {e}")
