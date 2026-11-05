"""API request/response models (Week 6)."""
from __future__ import annotations

import datetime as dt

from pydantic import BaseModel


class CreateBackfillRequest(BaseModel):
    table: str
    range_start: dt.date
    range_end: dt.date
    transform_version: str


class PlanPreviewRequest(CreateBackfillRequest):
    pass


class PartitionDecisionOut(BaseModel):
    partition: dt.date
    action: str
    reason: str


class PlanOut(BaseModel):
    table: str
    range_start: dt.date
    range_end: dt.date
    transform_version: str
    hot_window_cutoff: dt.date
    included: list[dt.date]
    decisions: list[PartitionDecisionOut]


class CreateBackfillResponse(BaseModel):
    backfill_id: int
    status: str
    plan: PlanOut


class RunRequest(BaseModel):
    max_concurrency: int = 4
    max_removed_keys: int = 0


class StatusResponse(BaseModel):
    status: str
