"""Week 1 verification: confirm partitions on disk are queryable via DuckDB.

Not part of the shipped engine — a standalone check for the Week 1 acceptance
criterion "partitions on disk, queryable via DuckDB".

Usage:
    python -m scripts.verify_dataplane --table fct_revenue
"""
from __future__ import annotations

import argparse

import duckdb

from rewind.config import get_settings
from rewind.dataplane import partitions as dp


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--table", required=True)
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    settings = get_settings()

    parts = dp.list_partitions(settings.raw_root, args.table)
    fs_rows = sum(dp.row_count(settings.raw_root, args.table, p) for p in parts)
    print(f"[filesystem] partitions={len(parts)} rows={fs_rows}")
    if parts:
        print(f"[filesystem] range={parts[0].isoformat()}..{parts[-1].isoformat()}")

    # Glob all partition files for this table and query them as one dataset.
    glob = str(settings.raw_root / args.table / "dt=*" / "part.parquet").replace("\\", "/")
    con = duckdb.connect()
    # Pull the partition date out of the file path so DuckDB-side grouping works
    # without relying on Hive partition inference.
    q = f"""
        SELECT
            regexp_extract(filename, 'dt=([0-9-]+)', 1) AS dt,
            count(*)        AS rows,
            sum(amount_cents) AS control_total
        FROM read_parquet('{glob}', filename = true)
        GROUP BY 1
        ORDER BY 1
    """
    rows = con.execute(q).fetchall()
    duck_total = con.execute(
        f"SELECT count(*), sum(amount_cents) FROM read_parquet('{glob}')"
    ).fetchone()
    con.close()

    print(f"[duckdb] partitions={len(rows)} rows={duck_total[0]} "
          f"control_total={duck_total[1]}")
    print("[duckdb] first 3 partitions:")
    for dt_str, n, total in rows[:3]:
        print(f"    dt={dt_str} rows={n} control_total={total}")

    # Cross-check filesystem vs DuckDB agree on partition count and row totals.
    assert len(parts) == len(rows), "partition count mismatch fs vs duckdb"
    assert fs_rows == duck_total[0], "row count mismatch fs vs duckdb"
    print("OK: filesystem and DuckDB agree on partitions and row counts")


if __name__ == "__main__":
    main()
