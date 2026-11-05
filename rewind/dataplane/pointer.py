"""Published-snapshot pointer file (Week 1).

The published zone tracks, per table, which concrete partition file is the
current live-readable one via ``_pointer.json``. Publishing swaps the pointer
atomically (write temp, then os.replace), mirroring Iceberg's manifest-swap
model. This is NOT a claim of object-store POSIX-rename atomicity — see the
README's "honest limitations" section. Iceberg is the Phase-2 upgrade.

Pointer schema::

    {
      "table": "fct_revenue",
      "version": 3,
      "partitions": {
        "2026-09-20": {
          "path": "fct_revenue/dt=2026-09-20/part.parquet",
          "transform_version": "v7",
          "row_count": 1234
        },
        ...
      },
      "updated_at": "2026-11-04T23:30:00+00:00"
    }
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

_POINTER_FILE = "_pointer.json"


def pointer_path(published_root: Path, table: str) -> Path:
    return published_root / table / _POINTER_FILE


def load_pointer(published_root: Path, table: str) -> dict[str, Any]:
    """Load the current pointer, or an empty v0 skeleton if none exists."""
    path = pointer_path(published_root, table)
    if not path.exists():
        return {"table": table, "version": 0, "partitions": {}, "updated_at": None}
    return json.loads(path.read_text(encoding="utf-8"))


def write_pointer(published_root: Path, table: str, pointer: dict[str, Any]) -> Path:
    """Atomically write the pointer (temp file + os.replace)."""
    tdir = published_root / table
    tdir.mkdir(parents=True, exist_ok=True)
    path = tdir / _POINTER_FILE
    tmp = tdir / f".{_POINTER_FILE}.tmp"
    tmp.write_text(json.dumps(pointer, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)  # atomic swap on same filesystem
    return path


def set_partition(
    published_root: Path,
    table: str,
    partition: dt.date,
    rel_path: str,
    transform_version: str,
    row_count: int,
) -> dict[str, Any]:
    """Point one partition at a new file and bump the pointer version.

    Returns the new pointer. Caller is responsible for having written the
    referenced parquet file first (write-new-then-swap-pointer).
    """
    pointer = load_pointer(published_root, table)
    pointer["partitions"][partition.isoformat()] = {
        "path": rel_path,
        "transform_version": transform_version,
        "row_count": row_count,
    }
    pointer["version"] = int(pointer.get("version", 0)) + 1
    pointer["updated_at"] = dt.datetime.now(dt.timezone.utc).isoformat()
    write_pointer(published_root, table, pointer)
    return pointer
