"""Reconciliation Engine (Week 5): prove the backfill before publishing.

For each partition, compares the *staged* (reprocessed, not yet published)
output against the *currently published* data using DuckDB over the two Parquet
files, and produces metrics + a verdict. The verdict is the gate: the Publisher
only commits a partition whose reconciliation PASSED (guarantee #4).

Metrics (per partition):
  * published_row_count / staged_row_count
  * keyset_added    — keys in staged but not published (new/recovered rows)
  * keyset_removed  — keys in published but not in staged (DROPPED rows)
  * control_total_before / control_total_after — sum(amount_cents)

Verdict policy (default, intentionally conservative):
  A backfill is *supposed* to change data, so a diff is not a failure. What is
  dangerous is silent data loss or an empty result. Default FAIL if:
    * staged partition is empty (staged_row_count == 0), or
    * keys were removed (keyset_removed > max_removed_keys, default 0).
  First publish (nothing currently published) always PASSES — everything is
  "added" by definition.
"""
from __future__ import annotations

import datetime as dt
import json
from dataclasses import dataclass, asdict

import duckdb

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database

VERDICT_PASS = "pass"
VERDICT_FAIL = "fail"


@dataclass
class ReconMetrics:
    partition: dt.date
    published_row_count: int
    staged_row_count: int
    keyset_added: int
    keyset_removed: int
    control_total_before: int
    control_total_after: int
    verdict: str
    reasons: list[str]

    def as_details(self) -> dict:
        d = asdict(self)
        d["partition"] = self.partition.isoformat()
        return d


def _reconcile_files(
    published_file: str | None, staged_file: str, key: str = "event_id", measure: str = "amount_cents"
) -> dict:
    """Run the DuckDB comparison. published_file=None means first publish."""
    con = duckdb.connect()
    try:
        staged_glob = staged_file.replace("\\", "/")
        staged_rows, staged_total = con.execute(
            f"SELECT count(*), coalesce(sum({measure}),0) FROM read_parquet('{staged_glob}')"
        ).fetchone()

        if published_file is None:
            return {
                "published_row_count": 0,
                "staged_row_count": int(staged_rows),
                "keyset_added": int(staged_rows and con.execute(
                    f"SELECT count(DISTINCT {key}) FROM read_parquet('{staged_glob}')"
                ).fetchone()[0]),
                "keyset_removed": 0,
                "control_total_before": 0,
                "control_total_after": int(staged_total),
            }

        pub_glob = published_file.replace("\\", "/")
        pub_rows, pub_total = con.execute(
            f"SELECT count(*), coalesce(sum({measure}),0) FROM read_parquet('{pub_glob}')"
        ).fetchone()

        added = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT DISTINCT {key} FROM read_parquet('{staged_glob}')
                EXCEPT
                SELECT DISTINCT {key} FROM read_parquet('{pub_glob}')
            )
            """
        ).fetchone()[0]
        removed = con.execute(
            f"""
            SELECT count(*) FROM (
                SELECT DISTINCT {key} FROM read_parquet('{pub_glob}')
                EXCEPT
                SELECT DISTINCT {key} FROM read_parquet('{staged_glob}')
            )
            """
        ).fetchone()[0]

        return {
            "published_row_count": int(pub_rows),
            "staged_row_count": int(staged_rows),
            "keyset_added": int(added),
            "keyset_removed": int(removed),
            "control_total_before": int(pub_total),
            "control_total_after": int(staged_total),
        }
    finally:
        con.close()


def _verdict(metrics: dict, max_removed_keys: int = 0) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if metrics["staged_row_count"] == 0:
        reasons.append("staged partition is empty")
    if metrics["keyset_removed"] > max_removed_keys:
        reasons.append(
            f"{metrics['keyset_removed']} keys removed (>{max_removed_keys} allowed)"
        )
    return (VERDICT_FAIL if reasons else VERDICT_PASS), reasons


def _materialize_published(table: str, partition: dt.date, tmp_dir: str) -> str | None:
    """Return a path to the currently-published partition parquet, or None.

    Backend-aware: with the pointer backend this is the file in the published
    zone; with the Iceberg backend we snapshot the partition's current rows out
    to a temp parquet so the same DuckDB comparison works unchanged. This is why
    reconciliation stays correct when the publish backend changes — it always
    compares against the *actual* current published state, not a fixed path.
    """
    import os

    import pyarrow.parquet as pq

    settings = get_settings()
    if settings.publisher_backend == "iceberg":
        from rewind.publisher import iceberg_backend as ice

        current = ice.read_published(table, partition)
        if current.num_rows == 0:
            return None
        out = os.path.join(tmp_dir, "published.parquet")
        pq.write_table(current, out)
        return out

    pub_path = dp.partition_path(settings.published_root, table, partition)
    return str(pub_path) if pub_path.exists() else None


def reconcile_partition_sync(
    table: str, partition: dt.date, *, max_removed_keys: int = 0
) -> ReconMetrics:
    """Compute reconciliation metrics + verdict for one partition (blocking)."""
    import tempfile

    settings = get_settings()
    staged_path = dp.partition_path(settings.staging_root, table, partition)
    if not staged_path.exists():
        raise FileNotFoundError(f"nothing staged for {table} {partition}")

    with tempfile.TemporaryDirectory(prefix="rewind-recon-") as tmp:
        published_file = _materialize_published(table, partition, tmp)
        metrics = _reconcile_files(published_file, str(staged_path))

    verdict, reasons = _verdict(metrics, max_removed_keys=max_removed_keys)
    return ReconMetrics(partition=partition, verdict=verdict, reasons=reasons, **metrics)


async def reconcile_partition(
    db: Database,
    backfill_id: int,
    table: str,
    partition: dt.date,
    *,
    max_removed_keys: int = 0,
) -> ReconMetrics:
    """Reconcile one partition and persist the result to the control DB."""
    import asyncio

    m = await asyncio.to_thread(
        reconcile_partition_sync, table, partition, max_removed_keys=max_removed_keys
    )

    await db.execute(
        """
        INSERT INTO reconciliation
            (backfill_id, partition, published_row_count, staged_row_count,
             keyset_added, keyset_removed, control_total_before,
             control_total_after, verdict, details)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        ON CONFLICT (backfill_id, partition) DO UPDATE SET
            published_row_count = EXCLUDED.published_row_count,
            staged_row_count    = EXCLUDED.staged_row_count,
            keyset_added        = EXCLUDED.keyset_added,
            keyset_removed      = EXCLUDED.keyset_removed,
            control_total_before= EXCLUDED.control_total_before,
            control_total_after = EXCLUDED.control_total_after,
            verdict             = EXCLUDED.verdict,
            details             = EXCLUDED.details
        """,
        backfill_id,
        partition,
        m.published_row_count,
        m.staged_row_count,
        m.keyset_added,
        m.keyset_removed,
        m.control_total_before,
        m.control_total_after,
        m.verdict,
        json.dumps(m.as_details()),
    )
    return m


async def reconcile_backfill(
    db: Database,
    backfill_id: int,
    table: str,
    partitions: list[dt.date],
    *,
    max_removed_keys: int = 0,
) -> list[ReconMetrics]:
    """Reconcile all staged partitions for a backfill. Order is deterministic."""
    out: list[ReconMetrics] = []
    for p in sorted(partitions):
        out.append(
            await reconcile_partition(db, backfill_id, table, p, max_removed_keys=max_removed_keys)
        )
    return out
