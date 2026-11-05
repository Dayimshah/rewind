"""Apache Iceberg publisher backend (Phase 2).

Replaces the Phase-1 ``_pointer.json`` swap with a real Iceberg table: each
publish is an atomic snapshot commit, and prior snapshots remain queryable
(time travel). This upgrades the "atomic swap" story from a filesystem pointer
to genuine snapshot isolation — the single concept the README flagged as the
Phase-2 win.

Design:
  * One Iceberg table per registered Rewind table, in namespace ``rewind``.
  * Catalog: a local SQLite SQL catalog; warehouse: a local directory.
    (On Windows the warehouse must be a plain path, not a file:// URI — the
    URI form mangles the drive letter inside pyiceberg's pyarrow IO.)
  * Publishing a partition = overwrite exactly that partition's rows
    (``dt == <partition>``) with the staged rows, in one commit → new snapshot.
  * Time travel = scan an older snapshot_id.

This backend is opt-in via ``PUBLISHER_BACKEND=iceberg``; the pointer backend
remains the default so Phase-1 behaviour is untouched.
"""
from __future__ import annotations

import datetime as dt
from functools import lru_cache

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.expressions import EqualTo

from rewind.config import get_settings
from rewind.dataplane import partitions as dp

_NAMESPACE = "rewind"

# The published schema (mirrors the staged parquet columns).
_SCHEMA = pa.schema(
    [
        ("event_id", pa.string()),
        ("dt", pa.date32()),
        ("region", pa.string()),
        ("product", pa.string()),
        ("amount_cents", pa.int64()),
    ]
)


@lru_cache(maxsize=1)
def get_catalog() -> SqlCatalog:
    """Open (bootstrapping if needed) the local SQLite Iceberg catalog."""
    settings = get_settings()
    settings.iceberg_warehouse.mkdir(parents=True, exist_ok=True)
    catalog = SqlCatalog(
        "rewind",
        uri=settings.iceberg_catalog_uri,
        # Plain filesystem path (NOT a file:// URI) — avoids the Windows
        # /C:/ path-mangling bug in pyiceberg's pyarrow IO.
        warehouse=str(settings.iceberg_warehouse),
    )
    try:
        catalog.create_namespace(_NAMESPACE)
    except NamespaceAlreadyExistsError:
        pass
    return catalog


def _table_id(table: str) -> str:
    return f"{_NAMESPACE}.{table}"


def ensure_table(table: str):
    """Return the Iceberg table for ``table``, creating it if absent."""
    catalog = get_catalog()
    try:
        return catalog.load_table(_table_id(table))
    except NoSuchTableError:
        return catalog.create_table(_table_id(table), schema=_SCHEMA)


def publish_partition_iceberg(table: str, partition: dt.date) -> dict:
    """Atomically publish one staged partition into the Iceberg table.

    Overwrites exactly the rows for this ``dt`` with the staged rows, producing
    a new snapshot. Returns {snapshot_id, rows}. Idempotent at the data level:
    overwriting with identical rows still commits a snapshot, but the scanned
    result is unchanged.
    """
    settings = get_settings()
    staged_path = dp.partition_path(settings.staging_root, table, partition)
    if not staged_path.exists():
        raise FileNotFoundError(f"nothing staged for {table} {partition}")

    staged = dp.read_partition(settings.staging_root, table, partition)
    # Normalise to the published schema/column order.
    staged = staged.select([f.name for f in _SCHEMA]).cast(_SCHEMA)

    tbl = ensure_table(table)
    # If the partition already has rows, atomically replace them (delete+append
    # in one overwrite commit). If it's empty, a plain append avoids a no-op
    # delete (and the "did not match any records" warning).
    existing = tbl.scan(row_filter=EqualTo("dt", partition)).to_arrow().num_rows
    if existing:
        tbl.overwrite(staged, overwrite_filter=EqualTo("dt", partition))
    else:
        tbl.append(staged)

    tbl = get_catalog().load_table(_table_id(table))  # refresh metadata
    snap = tbl.current_snapshot()
    rows = tbl.scan(row_filter=EqualTo("dt", partition)).to_arrow().num_rows
    return {"snapshot_id": snap.snapshot_id if snap else None, "rows": rows}


def read_published(table: str, partition: dt.date, *, snapshot_id: int | None = None) -> pa.Table:
    """Read a partition's currently-published rows, or as of an older snapshot."""
    tbl = ensure_table(table)
    scan = tbl.scan(row_filter=EqualTo("dt", partition), snapshot_id=snapshot_id)
    return scan.to_arrow()


def snapshots(table: str) -> list[dict]:
    """Return the table's snapshot history (oldest first) for time travel."""
    try:
        tbl = get_catalog().load_table(_table_id(table))
    except NoSuchTableError:
        return []
    out = []
    for s in tbl.metadata.snapshots:
        out.append(
            {
                "snapshot_id": s.snapshot_id,
                "timestamp_ms": s.timestamp_ms,
                "operation": getattr(s.summary, "operation", None) if s.summary else None,
            }
        )
    return out


def control_total(table: str, partition: dt.date, *, snapshot_id: int | None = None) -> int:
    """Sum of amount_cents for a partition (optionally at an older snapshot)."""
    t = read_published(table, partition, snapshot_id=snapshot_id)
    if t.num_rows == 0:
        return 0
    return int(sum(t.column("amount_cents").to_pylist()))
