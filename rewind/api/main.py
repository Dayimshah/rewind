"""FastAPI control plane for Rewind (Week 6).

Exposes the backfill lifecycle over HTTP, all delegating to
``rewind.orchestrator`` so the API and CLI share one implementation:

    POST /plan                      preview scope (no side effects)
    POST /backfills                 create (plan + persist, status=planned)
    GET  /backfills/{id}            full status (tasks + reconciliation)
    POST /backfills/{id}/run        stage + reconcile -> publishable | failed
    POST /backfills/{id}/publish    gated atomic publish -> published
"""
from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from rewind import orchestrator as orch
from rewind.api.schemas import (
    CreateBackfillRequest,
    CreateBackfillResponse,
    PlanOut,
    PlanPreviewRequest,
    RunRequest,
    StatusResponse,
)
from rewind.config import get_settings
from rewind.db.client import Database
from rewind.planner.models import ScopePlan
from rewind.planner.planner import plan_scope


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Open a shared asyncpg pool for the app's lifetime.

    In deployed environments, optionally run migrations + seed demo data on
    boot (controlled by RUN_MIGRATIONS_ON_START / SEED_ON_START; both default
    OFF, so local dev and tests are unaffected).
    """
    settings = get_settings()
    settings.ensure_dirs()
    db = Database(settings.database_url)
    await db.connect()
    app.state.db = db
    from rewind.api.bootstrap import bootstrap
    await bootstrap(db)
    try:
        yield
    finally:
        await db.close()


app = FastAPI(title="Rewind", version="0.1.0", lifespan=lifespan)

# The control-room UI runs on a separate origin. Dev defaults to Next.js on
# :3000; in production set CORS_ORIGINS to a comma-separated list of allowed
# origins (e.g. your Vercel URL). "*" allows any origin (handy for a public demo).
import os as _os  # noqa: E402

_origins_env = _os.environ.get("CORS_ORIGINS", "")
if _origins_env.strip() == "*":
    _allow_origins = ["*"]
elif _origins_env.strip():
    _allow_origins = [o.strip() for o in _origins_env.split(",") if o.strip()]
else:
    _allow_origins = ["http://localhost:3000", "http://127.0.0.1:3000"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_allow_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _plan_out(plan: ScopePlan) -> PlanOut:
    return PlanOut(
        table=plan.table,
        range_start=plan.range_start,
        range_end=plan.range_end,
        transform_version=plan.transform_version,
        hot_window_cutoff=plan.hot_window_cutoff,
        included=plan.included,
        decisions=[
            {"partition": d.partition, "action": d.action.value, "reason": d.reason.value}
            for d in plan.decisions
        ],
    )


@app.get("/health")
async def health() -> dict:
    return {"status": "ok", "service": "rewind"}


@app.get("/health/db")
async def health_db() -> dict:
    db: Database = app.state.db
    ok = await db.ping()
    return {"status": "ok" if ok else "error", "database": ok}


@app.post("/plan", response_model=PlanOut)
async def preview_plan(req: PlanPreviewRequest) -> PlanOut:
    """Compute and return the scope plan without creating a backfill."""
    db: Database = app.state.db
    try:
        plan = await plan_scope(
            db, req.table, req.range_start, req.range_end, req.transform_version
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return _plan_out(plan)


@app.post("/backfills", response_model=CreateBackfillResponse)
async def create_backfill(req: CreateBackfillRequest) -> CreateBackfillResponse:
    db: Database = app.state.db
    try:
        backfill_id, plan = await orch.create_backfill(
            db, req.table, req.range_start, req.range_end, req.transform_version
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return CreateBackfillResponse(
        backfill_id=backfill_id, status=orch.PLANNED, plan=_plan_out(plan)
    )


@app.get("/backfills/{backfill_id}")
async def get_backfill(backfill_id: int) -> dict:
    db: Database = app.state.db
    try:
        return await orch.backfill_detail(db, backfill_id)
    except orch.StateError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/backfills/{backfill_id}/run", response_model=StatusResponse)
async def run_backfill(backfill_id: int, req: RunRequest | None = None) -> StatusResponse:
    db: Database = app.state.db
    req = req or RunRequest()
    try:
        status = await orch.run_backfill(
            db, backfill_id,
            max_concurrency=req.max_concurrency,
            max_removed_keys=req.max_removed_keys,
        )
    except orch.StateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return StatusResponse(status=status)


@app.post("/backfills/{backfill_id}/publish", response_model=StatusResponse)
async def publish_backfill(backfill_id: int) -> StatusResponse:
    db: Database = app.state.db
    try:
        status = await orch.publish_backfill(db, backfill_id)
    except orch.StateError as e:
        raise HTTPException(status_code=409, detail=str(e))
    return StatusResponse(status=status)
