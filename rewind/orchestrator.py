"""Backfill orchestrator + state machine (Week 6).

Ties the four engine stages into one safe operation and tracks the backfill's
lifecycle in the control DB. Shared by both the API and the CLI.

State machine (backfills.status):

    planned ──run──▶ running ──▶ reconciling ──┬─(all pass)─▶ publishable ──publish──▶ published
                                               └─(any fail)─▶ failed

  * planned      — scope computed, partition_tasks seeded, nothing touched.
  * running      — executor reprocessing into staging.
  * reconciling  — comparing staged vs published.
  * publishable  — every partition's verdict is PASS; safe to publish.
  * failed       — at least one partition failed to stage or failed recon.
  * published    — atomic swap + ledger done for all partitions.

Publishing is gated: ``publish_backfill`` refuses unless status is
``publishable`` (guarantee #4 — proof before publish).
"""
from __future__ import annotations

import datetime as dt
from dataclasses import dataclass

from rewind.db.client import Database
from rewind.executor.executor import execute_backfill
from rewind.planner.planner import plan_scope
from rewind.planner.models import ScopePlan
from rewind.publisher.publisher import publish_backfill as _publish_backfill
from rewind.reconciliation.engine import VERDICT_PASS, reconcile_backfill

# Lifecycle states
PLANNED = "planned"
RUNNING = "running"
RECONCILING = "reconciling"
PUBLISHABLE = "publishable"
PUBLISHED = "published"
FAILED = "failed"


class StateError(RuntimeError):
    """Raised when an operation is attempted from an invalid state."""


@dataclass
class BackfillRow:
    id: int
    table_name: str
    range_start: dt.date
    range_end: dt.date
    transform_version: str
    status: str


async def _set_status(db: Database, backfill_id: int, status: str) -> None:
    await db.execute(
        "UPDATE backfills SET status=$2, updated_at=now() WHERE id=$1",
        backfill_id,
        status,
    )


async def get_backfill(db: Database, backfill_id: int) -> BackfillRow | None:
    row = await db.fetchrow(
        "SELECT id, table_name, range_start, range_end, transform_version, status "
        "FROM backfills WHERE id=$1",
        backfill_id,
    )
    if row is None:
        return None
    return BackfillRow(**dict(row))


async def create_backfill(
    db: Database,
    table: str,
    range_start: dt.date,
    range_end: dt.date,
    transform_version: str,
    *,
    today: dt.date | None = None,
) -> tuple[int, ScopePlan]:
    """Plan scope, persist the backfill (status=planned) + its partition_tasks.

    Returns (backfill_id, plan). Nothing in the data plane is touched here.
    """
    plan = await plan_scope(
        db, table, range_start, range_end, transform_version, today=today
    )

    backfill_id = await db.fetchval(
        """
        INSERT INTO backfills
            (table_name, range_start, range_end, transform_version, status)
        VALUES ($1, $2, $3, $4, $5) RETURNING id
        """,
        table,
        range_start,
        range_end,
        transform_version,
        PLANNED,
    )

    # Seed one partition_tasks row per INCLUDED partition.
    for partition in plan.included:
        await db.execute(
            """
            INSERT INTO partition_tasks (backfill_id, partition, status)
            VALUES ($1, $2, 'pending')
            ON CONFLICT (backfill_id, partition) DO NOTHING
            """,
            backfill_id,
            partition,
        )

    return backfill_id, plan


async def run_backfill(
    db: Database,
    backfill_id: int,
    *,
    max_concurrency: int = 4,
    max_removed_keys: int = 0,
) -> str:
    """Execute (stage) then reconcile. Returns the resulting status.

    Allowed from ``planned`` or ``failed`` (retry). Sets ``running`` then
    ``reconciling`` then ``publishable`` or ``failed``.
    """
    bf = await get_backfill(db, backfill_id)
    if bf is None:
        raise StateError(f"no such backfill: {backfill_id}")
    if bf.status not in (PLANNED, FAILED):
        raise StateError(f"cannot run from status '{bf.status}'")

    partitions = [
        r["partition"]
        for r in await db.fetch(
            "SELECT partition FROM partition_tasks WHERE backfill_id=$1 ORDER BY partition",
            backfill_id,
        )
    ]
    if not partitions:
        # Nothing to do — an empty scope is a valid, already-correct result.
        await _set_status(db, backfill_id, PUBLISHABLE)
        return PUBLISHABLE

    await _set_status(db, backfill_id, RUNNING)
    report = await execute_backfill(
        db, backfill_id, bf.table_name, partitions, bf.transform_version,
        max_concurrency=max_concurrency,
    )
    if report.failed:
        await _set_status(db, backfill_id, FAILED)
        return FAILED

    await _set_status(db, backfill_id, RECONCILING)
    metrics = await reconcile_backfill(
        db, backfill_id, bf.table_name,
        [r.partition for r in report.staged],
        max_removed_keys=max_removed_keys,
    )
    if any(m.verdict != VERDICT_PASS for m in metrics):
        await _set_status(db, backfill_id, FAILED)
        return FAILED

    await _set_status(db, backfill_id, PUBLISHABLE)
    return PUBLISHABLE


async def publish_backfill(db: Database, backfill_id: int) -> str:
    """Gated publish: only from ``publishable``. Sets ``published``."""
    bf = await get_backfill(db, backfill_id)
    if bf is None:
        raise StateError(f"no such backfill: {backfill_id}")
    if bf.status != PUBLISHABLE:
        raise StateError(
            f"refusing to publish from status '{bf.status}' (must be '{PUBLISHABLE}')"
        )

    partitions = [
        r["partition"]
        for r in await db.fetch(
            "SELECT partition FROM partition_tasks WHERE backfill_id=$1 ORDER BY partition",
            backfill_id,
        )
    ]
    await _publish_backfill(db, backfill_id, bf.table_name, partitions, bf.transform_version)
    await _set_status(db, backfill_id, PUBLISHED)
    return PUBLISHED


async def backfill_detail(db: Database, backfill_id: int) -> dict:
    """Full status view: backfill row + partition_tasks + reconciliation."""
    bf = await get_backfill(db, backfill_id)
    if bf is None:
        raise StateError(f"no such backfill: {backfill_id}")

    tasks = await db.fetch(
        "SELECT partition, status, staging_path, attempt, error "
        "FROM partition_tasks WHERE backfill_id=$1 ORDER BY partition",
        backfill_id,
    )
    recon = await db.fetch(
        "SELECT partition, published_row_count, staged_row_count, keyset_added, "
        "keyset_removed, control_total_before, control_total_after, verdict "
        "FROM reconciliation WHERE backfill_id=$1 ORDER BY partition",
        backfill_id,
    )
    return {
        "id": bf.id,
        "table": bf.table_name,
        "range_start": bf.range_start.isoformat(),
        "range_end": bf.range_end.isoformat(),
        "transform_version": bf.transform_version,
        "status": bf.status,
        "partition_tasks": [dict(r) for r in tasks],
        "reconciliation": [dict(r) for r in recon],
    }
