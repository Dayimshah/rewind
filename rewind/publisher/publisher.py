"""Publisher (Week 4): idempotent publication + atomic pointer swap.

Publishing a staged partition makes it live-readable. The order is chosen so a
crash at any point converges on a re-run:

    1. Copy staged parquet -> published zone (atomic temp+replace).
    2. Atomically swap ``_pointer.json`` to reference the new file.
    3. Record the publish in ``publish_ledger`` keyed
       (table, partition, transform_version), ON CONFLICT DO NOTHING.

Idempotency (guarantee #3): if the ledger already has the row, publish is a
no-op. Because the staged transform output is byte-identical on re-run, steps
1-2 are safe to repeat.

Crash recovery:
    * Crash after (1)/(2) but before (3): data is live but unrecorded. Re-run
      re-copies identical bytes, re-swaps the (identical) pointer, and records
      the ledger row -> converged.
    * Crash before (1): nothing published; re-run performs the full publish.

"Atomic swap" is a ``_pointer.json`` temp-write + os.replace — it mirrors
Iceberg's manifest swap, not POSIX object-store atomicity (see README limits).
"""
from __future__ import annotations

import datetime as dt
import shutil
from dataclasses import dataclass

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.dataplane import pointer as ptr
from rewind.db.client import Database


@dataclass
class PublishResult:
    partition: dt.date
    published: bool      # True if newly published, False if idempotent no-op
    rows: int
    pointer_version: int


async def _ledger_has(
    db: Database, table: str, partition: dt.date, transform_version: str
) -> bool:
    return bool(
        await db.fetchval(
            """
            SELECT 1 FROM publish_ledger
            WHERE table_name=$1 AND partition=$2 AND transform_version=$3
            """,
            table,
            partition,
            transform_version,
        )
    )


def _staged_equals_published(table: str, partition: dt.date) -> bool:
    """True if the published file already has byte-identical content to staged.

    This is the real idempotency check: re-publishing the *same* staged result
    is a no-op, but a changed staged result (e.g. late data corrected under the
    same transform version) is NOT equal and must be republished. Relying on
    the ledger's (table, partition, transform_version) key alone is too coarse —
    it cannot tell "already published this exact output" from "input changed".
    """
    settings = get_settings()
    pub_path = dp.partition_path(settings.published_root, table, partition)
    staged_path = dp.partition_path(settings.staging_root, table, partition)
    if not pub_path.exists() or not staged_path.exists():
        return False
    return pub_path.read_bytes() == staged_path.read_bytes()


def _copy_staged_to_published(table: str, partition: dt.date) -> tuple[str, int]:
    """Copy the staged partition file into the published zone (atomic).

    Returns (published_relative_path, row_count). Raises FileNotFoundError if
    the partition was never staged.
    """
    settings = get_settings()
    staged_path = dp.partition_path(settings.staging_root, table, partition)
    if not staged_path.exists():
        raise FileNotFoundError(f"nothing staged for {table} {partition}")

    pub_dir = dp.partition_dir(settings.published_root, table, partition)
    pub_dir.mkdir(parents=True, exist_ok=True)
    final = dp.partition_path(settings.published_root, table, partition)
    tmp = final.with_suffix(".parquet.tmp")
    shutil.copyfile(staged_path, tmp)
    tmp.replace(final)  # atomic on same filesystem

    rel = str(final.relative_to(settings.published_root)).replace("\\", "/")
    rows = dp.row_count(settings.published_root, table, partition)
    return rel, rows


async def publish_partition(
    db: Database,
    backfill_id: int | None,
    table: str,
    partition: dt.date,
    transform_version: str,
) -> PublishResult:
    """Publish a single staged partition idempotently.

    Idempotency is content-based: if the published partition already equals the
    staged output, this is a true no-op. Otherwise the staged result is swapped
    in atomically — this correctly republishes a partition whose input changed
    (late/corrected data) even under the same transform version.

    Dispatches to the Iceberg backend when ``PUBLISHER_BACKEND=iceberg``.
    """
    settings = get_settings()

    if settings.publisher_backend == "iceberg":
        return await _publish_partition_iceberg(
            db, backfill_id, table, partition, transform_version
        )

    if _staged_equals_published(table, partition):
        # Already live with exactly this result. No-op, but make sure the ledger
        # records it (covers crash-before-ledger recovery).
        if not await _ledger_has(db, table, partition, transform_version):
            await db.execute(
                """
                INSERT INTO publish_ledger
                    (table_name, partition, transform_version, backfill_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (table_name, partition, transform_version) DO NOTHING
                """,
                table, partition, transform_version, backfill_id,
            )
        pointer = ptr.load_pointer(settings.published_root, table)
        entry = pointer.get("partitions", {}).get(partition.isoformat(), {})
        return PublishResult(
            partition=partition,
            published=False,
            rows=int(entry.get("row_count", 0)),
            pointer_version=int(pointer.get("version", 0)),
        )

    # 1 + 2: copy data, then swap the pointer.
    rel, rows = _copy_staged_to_published(table, partition)
    pointer = ptr.set_partition(
        settings.published_root, table, partition, rel, transform_version, rows
    )

    # 3: record the publish. Upsert so a republish (changed content, same
    # transform) refreshes the owning backfill + timestamp.
    await db.execute(
        """
        INSERT INTO publish_ledger
            (table_name, partition, transform_version, backfill_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (table_name, partition, transform_version)
        DO UPDATE SET backfill_id = EXCLUDED.backfill_id, published_at = now()
        """,
        table,
        partition,
        transform_version,
        backfill_id,
    )

    if backfill_id is not None:
        await db.execute(
            """
            UPDATE partition_tasks SET status='published', updated_at=now()
            WHERE backfill_id=$1 AND partition=$2
            """,
            backfill_id,
            partition,
        )

    return PublishResult(
        partition=partition,
        published=True,
        rows=rows,
        pointer_version=int(pointer.get("version", 0)),
    )


async def _publish_partition_iceberg(
    db: Database,
    backfill_id: int | None,
    table: str,
    partition: dt.date,
    transform_version: str,
) -> PublishResult:
    """Iceberg-backed publish: atomic snapshot commit + ledger record.

    Idempotency is content-based: if the staged rows already equal the current
    Iceberg-published rows for this partition, skip the commit (no new snapshot)
    but ensure the ledger row exists.
    """
    import asyncio

    from rewind.publisher import iceberg_backend as ice

    settings = get_settings()
    staged = dp.read_partition(settings.staging_root, table, partition)
    staged_norm = staged.select([n for n in ice._SCHEMA.names]).cast(ice._SCHEMA)

    def _current_equals_staged() -> bool:
        current = ice.read_published(table, partition)
        if current.num_rows != staged_norm.num_rows:
            return False
        # Compare as sorted row sets (snapshot read order is not guaranteed).
        a = current.sort_by("event_id").to_pydict()
        b = staged_norm.sort_by("event_id").to_pydict()
        return a == b

    already = await asyncio.to_thread(_current_equals_staged)
    if already:
        if not await _ledger_has(db, table, partition, transform_version):
            await db.execute(
                """
                INSERT INTO publish_ledger
                    (table_name, partition, transform_version, backfill_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (table_name, partition, transform_version) DO NOTHING
                """,
                table, partition, transform_version, backfill_id,
            )
        return PublishResult(partition=partition, published=False, rows=staged_norm.num_rows,
                             pointer_version=0)

    result = await asyncio.to_thread(ice.publish_partition_iceberg, table, partition)

    await db.execute(
        """
        INSERT INTO publish_ledger
            (table_name, partition, transform_version, backfill_id)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (table_name, partition, transform_version)
        DO UPDATE SET backfill_id = EXCLUDED.backfill_id, published_at = now()
        """,
        table, partition, transform_version, backfill_id,
    )
    if backfill_id is not None:
        await db.execute(
            "UPDATE partition_tasks SET status='published', updated_at=now() "
            "WHERE backfill_id=$1 AND partition=$2",
            backfill_id, partition,
        )
    return PublishResult(partition=partition, published=True, rows=result["rows"],
                         pointer_version=0)


async def publish_backfill(
    db: Database,
    backfill_id: int,
    table: str,
    partitions: list[dt.date],
    transform_version: str,
) -> list[PublishResult]:
    """Publish all staged partitions for a backfill, idempotently and in order."""
    results: list[PublishResult] = []
    for partition in sorted(partitions):
        results.append(
            await publish_partition(db, backfill_id, table, partition, transform_version)
        )
    return results
