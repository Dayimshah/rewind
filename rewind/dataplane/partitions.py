"""Partitioned Parquet data plane helpers (Week 1).

Layout on disk (relative to a zone root like ``data/raw``):

    <zone>/<table>/dt=YYYY-MM-DD/part.parquet

These helpers deliberately stay filesystem-only: no DB, no DuckDB. DuckDB is
used separately for reconciliation queries.
"""
from __future__ import annotations

import datetime as dt
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

_PART_FILE = "part.parquet"
_DT_PREFIX = "dt="


def partition_dir(zone_root: Path, table: str, partition: dt.date) -> Path:
    """Directory holding a single partition's parquet file."""
    return zone_root / table / f"{_DT_PREFIX}{partition.isoformat()}"


def partition_path(zone_root: Path, table: str, partition: dt.date) -> Path:
    """Full path to a partition's parquet file."""
    return partition_dir(zone_root, table, partition) / _PART_FILE


def write_partition(
    zone_root: Path,
    table: str,
    partition: dt.date,
    table_data: pa.Table,
) -> Path:
    """Write one partition atomically (write temp, then replace).

    Returns the path of the written parquet file. Overwrites any existing
    partition file — callers decide whether that's appropriate (raw appends,
    staging reprocesses).
    """
    pdir = partition_dir(zone_root, table, partition)
    pdir.mkdir(parents=True, exist_ok=True)
    final = pdir / _PART_FILE
    tmp = pdir / f".{_PART_FILE}.tmp"
    pq.write_table(table_data, tmp)
    tmp.replace(final)  # atomic on same filesystem
    return final


def read_partition(zone_root: Path, table: str, partition: dt.date) -> pa.Table:
    """Read a single partition into an Arrow table.

    Reads the parquet file directly (not via ``pq.read_table`` dataset
    discovery) so pyarrow does NOT infer the ``dt=YYYY-MM-DD`` directory as a
    Hive partition column — that would collide with our real ``dt`` data column.
    """
    path = partition_path(zone_root, table, partition)
    if not path.exists():
        raise FileNotFoundError(path)
    return pq.ParquetFile(path).read()


def partition_exists(zone_root: Path, table: str, partition: dt.date) -> bool:
    return partition_path(zone_root, table, partition).exists()


def list_partitions(zone_root: Path, table: str) -> list[dt.date]:
    """Return sorted partition dates present for a table in a zone."""
    base = zone_root / table
    if not base.exists():
        return []
    out: list[dt.date] = []
    for child in base.iterdir():
        if child.is_dir() and child.name.startswith(_DT_PREFIX):
            if (child / _PART_FILE).exists():
                try:
                    out.append(dt.date.fromisoformat(child.name[len(_DT_PREFIX):]))
                except ValueError:
                    continue
    return sorted(out)


def row_count(zone_root: Path, table: str, partition: dt.date) -> int:
    """Row count of a partition via Parquet metadata (no full read)."""
    path = partition_path(zone_root, table, partition)
    if not path.exists():
        return 0
    return pq.ParquetFile(path).metadata.num_rows


def partition_mtime(
    zone_root: Path, table: str, partition: dt.date
) -> dt.datetime | None:
    """Last-modified time of a partition's parquet file, as tz-aware UTC.

    Used by the Scope Planner as a cheap "source watermark": if a raw partition
    was modified after it was last published, late/corrected data arrived and
    the partition needs reprocessing. Returns None if the partition is absent.
    """
    path = partition_path(zone_root, table, partition)
    if not path.exists():
        return None
    ts = path.stat().st_mtime
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc)
