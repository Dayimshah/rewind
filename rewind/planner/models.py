"""Scope plan data models (Week 2).

The Scope Planner produces an explainable plan: every candidate partition in
the request range gets a decision (include or skip) with a machine-readable
reason. Explainability is a first-class goal — a backfill operator must be able
to see *why* each partition was or wasn't chosen before any data is touched.
"""
from __future__ import annotations

import datetime as dt
from enum import Enum

from pydantic import BaseModel


class Action(str, Enum):
    INCLUDE = "include"
    SKIP = "skip"


class Reason(str, Enum):
    # include reasons
    NEVER_PUBLISHED = "never_published"            # no ledger entry for this transform
    LATE_DATA = "late_data"                        # raw changed after last publish
    # skip reasons
    OUT_OF_RANGE = "out_of_range"                  # not in [start, end] (never emitted)
    IN_HOT_WINDOW = "in_hot_window"                # live stream owns it; do not touch
    NO_RAW_DATA = "no_raw_data"                    # nothing to reprocess
    ALREADY_PUBLISHED = "already_published"        # idempotent: same transform, raw unchanged


class PartitionDecision(BaseModel):
    partition: dt.date
    action: Action
    reason: Reason


class ScopePlan(BaseModel):
    """The full, ordered plan for a backfill request."""

    table: str
    range_start: dt.date
    range_end: dt.date
    transform_version: str
    # First date (inclusive) owned by the live stream; partitions >= this are
    # in the hot window and are never included.
    hot_window_cutoff: dt.date
    decisions: list[PartitionDecision]

    @property
    def included(self) -> list[dt.date]:
        return [d.partition for d in self.decisions if d.action is Action.INCLUDE]

    @property
    def skipped(self) -> list[PartitionDecision]:
        return [d for d in self.decisions if d.action is Action.SKIP]

    def summary(self) -> str:
        inc = len(self.included)
        skip = len(self.skipped)
        return (
            f"{self.table} [{self.range_start}..{self.range_end}] "
            f"transform={self.transform_version}: "
            f"{inc} to reprocess, {skip} skipped "
            f"(hot_window_cutoff={self.hot_window_cutoff})"
        )
