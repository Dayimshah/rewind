"""Scope Planner (Week 2).

Given a backfill request, compute the minimal, explainable set of partitions to
reprocess. This is Rewind's "precise scope" guarantee: the planner never touches
partitions the live stream owns (the hot window), never reprocesses partitions
that are already correct (idempotent skip via the publish ledger), and only
includes partitions that actually have raw data to rework.

Decision rules, per candidate partition in [range_start, range_end]:

  1. In hot window (>= hot_window_cutoff)   -> SKIP  (in_hot_window)
  2. No raw partition on disk               -> SKIP  (no_raw_data)
  3. No publish-ledger entry for this
     (table, partition, transform_version)  -> INCLUDE (never_published)
  4. Ledger entry exists, but the raw file
     was modified after published_at        -> INCLUDE (late_data)
  5. Ledger entry exists, raw unchanged      -> SKIP  (already_published)

The hot window is defined relative to ``today``: the cutoff is
``today - (hot_window_days - 1)``, so the most recent ``hot_window_days`` days
(including today) are owned by the stream. ``today`` is injectable for testing.
"""
from __future__ import annotations

import datetime as dt

from rewind.config import get_settings
from rewind.dataplane import partitions as dp
from rewind.db.client import Database
from rewind.planner.models import Action, PartitionDecision, Reason, ScopePlan
from rewind.registry.registry import get_table


def _date_range(start: dt.date, end: dt.date) -> list[dt.date]:
    return [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]


def hot_window_cutoff(today: dt.date, hot_window_days: int) -> dt.date:
    """First date (inclusive) owned by the live stream.

    hot_window_days=2, today=2026-11-04 -> cutoff 2026-11-03, so 11-03 and
    11-04 are hot. hot_window_days=0 means no hot window (cutoff in the future).
    """
    if hot_window_days <= 0:
        return today + dt.timedelta(days=1)
    return today - dt.timedelta(days=hot_window_days - 1)


async def _ledger_published_at(
    db: Database, table: str, partition: dt.date, transform_version: str
) -> dt.datetime | None:
    """Return published_at for a ledger entry, or None if not published."""
    return await db.fetchval(
        """
        SELECT published_at FROM publish_ledger
        WHERE table_name = $1 AND partition = $2 AND transform_version = $3
        """,
        table,
        partition,
        transform_version,
    )


async def plan_scope(
    db: Database,
    table: str,
    range_start: dt.date,
    range_end: dt.date,
    transform_version: str,
    *,
    today: dt.date | None = None,
) -> ScopePlan:
    """Compute the scope plan for a backfill request.

    Raises ValueError for an unknown table or an inverted range.
    """
    if range_end < range_start:
        raise ValueError("range_end must be >= range_start")

    registered = await get_table(db, table)
    if registered is None:
        raise ValueError(f"table not registered: {table}")

    settings = get_settings()
    if today is None:
        today = dt.date.today()
    cutoff = hot_window_cutoff(today, registered.hot_window_days)

    decisions: list[PartitionDecision] = []
    for partition in _date_range(range_start, range_end):
        # 1. Hot window: the live stream owns these; never touch.
        if partition >= cutoff:
            decisions.append(
                PartitionDecision(
                    partition=partition, action=Action.SKIP, reason=Reason.IN_HOT_WINDOW
                )
            )
            continue

        # 2. No raw data to reprocess.
        raw_mtime = dp.partition_mtime(settings.raw_root, table, partition)
        if raw_mtime is None:
            decisions.append(
                PartitionDecision(
                    partition=partition, action=Action.SKIP, reason=Reason.NO_RAW_DATA
                )
            )
            continue

        published_at = await _ledger_published_at(
            db, table, partition, transform_version
        )

        # 3. Never published at this transform version -> must reprocess.
        if published_at is None:
            decisions.append(
                PartitionDecision(
                    partition=partition,
                    action=Action.INCLUDE,
                    reason=Reason.NEVER_PUBLISHED,
                )
            )
            continue

        # 4. Late/corrected data: raw changed after the last publish.
        if raw_mtime > published_at:
            decisions.append(
                PartitionDecision(
                    partition=partition, action=Action.INCLUDE, reason=Reason.LATE_DATA
                )
            )
            continue

        # 5. Already correct at this transform version: idempotent skip.
        decisions.append(
            PartitionDecision(
                partition=partition,
                action=Action.SKIP,
                reason=Reason.ALREADY_PUBLISHED,
            )
        )

    return ScopePlan(
        table=table,
        range_start=range_start,
        range_end=range_end,
        transform_version=transform_version,
        hot_window_cutoff=cutoff,
        decisions=decisions,
    )
