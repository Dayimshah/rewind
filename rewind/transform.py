"""The reprocessing transform (engine seam).

A transform turns a raw partition into a corrected/published partition. In
Phase 1 it is a deterministic, seeded function of (table, partition,
transform_version) AND the raw input, so that:

  * reprocessing the same inputs twice yields byte-identical output
    (idempotency — guarantee #3), and
  * if the raw input changes (late/corrected data), the output changes, so
    reconciliation can detect and prove the impact.

This is the single place a Phase-2 PySpark executor would swap in a distributed
implementation; the contract (raw Arrow table -> corrected Arrow table) stays.
"""
from __future__ import annotations

import datetime as dt
import hashlib

import pyarrow as pa

_REGIONS = ["us", "eu", "apac"]
_PRODUCTS = ["basic", "pro", "enterprise"]


def _seeded_int(*parts: str, mod: int) -> int:
    """Deterministic non-negative int derived from the parts."""
    h = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return int(h, 16) % mod


def generate_rows(table: str, partition: dt.date, seed: str = "v0") -> pa.Table:
    """Generate a deterministic fct_revenue-style partition.

    Schema: event_id (str, key), dt (date), region, product, amount_cents (int).
    The number of rows and their amounts depend only on (table, partition, seed).
    Used by the source simulator to create raw data.
    """
    day = partition.isoformat()
    n_rows = 20 + _seeded_int(table, day, seed, "n", mod=30)  # 20..49 rows

    event_ids: list[str] = []
    dts: list[dt.date] = []
    regions: list[str] = []
    products: list[str] = []
    amounts: list[int] = []

    for i in range(n_rows):
        key = f"{table}:{day}:{i}"
        event_ids.append(hashlib.sha1(key.encode()).hexdigest()[:16])
        dts.append(partition)
        regions.append(_REGIONS[_seeded_int(key, seed, "r", mod=len(_REGIONS))])
        products.append(_PRODUCTS[_seeded_int(key, seed, "p", mod=len(_PRODUCTS))])
        amounts.append(100 + _seeded_int(key, seed, "amt", mod=9900))

    return pa.table(
        {
            "event_id": pa.array(event_ids, pa.string()),
            "dt": pa.array(dts, pa.date32()),
            "region": pa.array(regions, pa.string()),
            "product": pa.array(products, pa.string()),
            "amount_cents": pa.array(amounts, pa.int64()),
        }
    )


def apply_transform(
    raw: pa.Table, table: str, partition: dt.date, transform_version: str
) -> pa.Table:
    """Produce the corrected partition for a given transform version.

    The transform is a pure function of (raw rows, table, partition,
    transform_version). It recomputes each row's ``amount_cents`` under the new
    transform version while preserving the input keys (``event_id``) and the
    row set — so the key-set is stable across transforms but control totals
    change, and adding raw rows (late data) adds output rows. This makes both
    idempotency and reconciliation observable.
    """
    event_ids = raw.column("event_id").to_pylist()
    day = partition.isoformat()

    regions: list[str] = []
    products: list[str] = []
    amounts: list[int] = []
    for ev in event_ids:
        key = f"{table}:{day}:{ev}"
        regions.append(_REGIONS[_seeded_int(key, transform_version, "r", mod=len(_REGIONS))])
        products.append(_PRODUCTS[_seeded_int(key, transform_version, "p", mod=len(_PRODUCTS))])
        amounts.append(100 + _seeded_int(key, transform_version, "amt", mod=9900))

    return pa.table(
        {
            "event_id": pa.array(event_ids, pa.string()),
            "dt": pa.array([partition] * len(event_ids), pa.date32()),
            "region": pa.array(regions, pa.string()),
            "product": pa.array(products, pa.string()),
            "amount_cents": pa.array(amounts, pa.int64()),
        }
    )
