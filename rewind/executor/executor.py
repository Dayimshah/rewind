"""Reprocessing Executor (Week 3).

Reprocesses a set of partitions into the staging zone, never touching raw/ or
published/. Properties this guarantees:

  * Stream-safe isolation: output goes only to ``staging/`` (guarantee #2).
  * Idempotency: the transform is a pure seeded function of
    (raw, table, partition, transform_version); re-running produces a
    byte-identical staged file (guarantee #3).
  * Bounded-concurrency parallelism: at most ``max_concurrency`` partitions are
    reprocessed at once, mirroring a real distributed executor's fan-out while
    staying predictable on a laptop.

Each partition's lifecycle is tracked in ``partition_tasks``
(pending -> running -> staged | failed), including the staging path and the
attempt count so retries are visible.
"""
from __future__ import annotations

import asyncio
import datetime as dt
from dataclasses import dataclass

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database
from rewind.transform import apply_transform


@dataclass
class PartitionResult:
    partition: dt.date
    status: str          # "staged" | "failed"
    staging_path: str | None
    rows: int
    error: str | None = None


@dataclass
class ExecutionReport:
    backfill_id: int
    results: list[PartitionResult]

    @property
    def staged(self) -> list[PartitionResult]:
        return [r for r in self.results if r.status == "staged"]

    @property
    def failed(self) -> list[PartitionResult]:
        return [r for r in self.results if r.status == "failed"]


def _reprocess_one_sync(table: str, partition: dt.date, transform_version: str) -> tuple[str, int]:
    """Blocking reprocess of a single partition. Runs in a worker thread.

    Reads raw, applies the transform, writes to staging. Returns
    (staging_path, row_count). Pure filesystem + CPU work — safe off the loop.
    """
    settings = get_settings()
    raw = dp.read_partition(settings.raw_root, table, partition)
    corrected = apply_transform(raw, table, partition, transform_version)
    path = dp.write_partition(settings.staging_root, table, partition, corrected)
    return str(path), corrected.num_rows


async def _mark(db: Database, backfill_id: int, partition: dt.date, **fields) -> None:
    """Upsert a partition_tasks row with the given fields."""
    # Ensure the row exists, then update. Keeps the executor usable even when
    # the caller hasn't pre-seeded partition_tasks.
    await db.execute(
        """
        INSERT INTO partition_tasks (backfill_id, partition, status)
        VALUES ($1, $2, 'pending')
        ON CONFLICT (backfill_id, partition) DO NOTHING
        """,
        backfill_id,
        partition,
    )
    sets = ", ".join(f"{k} = ${i + 3}" for i, k in enumerate(fields))
    sets = f"{sets}, updated_at = now()" if sets else "updated_at = now()"
    await db.execute(
        f"UPDATE partition_tasks SET {sets} WHERE backfill_id = $1 AND partition = $2",
        backfill_id,
        partition,
        *fields.values(),
    )


async def execute_backfill(
    db: Database,
    backfill_id: int,
    table: str,
    partitions: list[dt.date],
    transform_version: str,
    *,
    max_concurrency: int = 4,
) -> ExecutionReport:
    """Reprocess ``partitions`` into staging with bounded concurrency.

    Records per-partition status in ``partition_tasks``. A failure in one
    partition does not abort the others; it is captured on that task row.
    """
    sem = asyncio.Semaphore(max_concurrency)
    results: list[PartitionResult] = []

    async def run_one(partition: dt.date) -> PartitionResult:
        async with sem:
            # bump attempt + mark running
            await db.execute(
                """
                INSERT INTO partition_tasks (backfill_id, partition, status, attempt)
                VALUES ($1, $2, 'running', 1)
                ON CONFLICT (backfill_id, partition)
                DO UPDATE SET status = 'running', attempt = partition_tasks.attempt + 1,
                              updated_at = now()
                """,
                backfill_id,
                partition,
            )
            try:
                path, rows = await asyncio.to_thread(
                    _reprocess_one_sync, table, partition, transform_version
                )
            except Exception as exc:  # noqa: BLE001 — record, don't abort the batch
                await _mark(db, backfill_id, partition, status="failed", error=str(exc))
                return PartitionResult(partition, "failed", None, 0, str(exc))

            await _mark(
                db, backfill_id, partition,
                status="staged", staging_path=path, error=None,
            )
            return PartitionResult(partition, "staged", path, rows)

    # Launch all; the semaphore bounds how many actually run at once.
    results = await asyncio.gather(*(run_one(p) for p in partitions))
    return ExecutionReport(backfill_id=backfill_id, results=list(results))
