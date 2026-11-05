"""Rewind CLI (Week 6).

Thin wrapper over the orchestrator, sharing the exact logic the API uses.

Examples:
    python -m rewind.cli plan    --table fct_revenue --start 2026-09-20 --end 2026-11-02 --transform v7
    python -m rewind.cli create  --table fct_revenue --start 2026-09-20 --end 2026-11-02 --transform v7
    python -m rewind.cli run     --id 1
    python -m rewind.cli publish --id 1
    python -m rewind.cli status  --id 1
"""
from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json

from rewind import orchestrator as orch
from rewind.config import get_settings
from rewind.db.client import Database
from rewind.planner.planner import plan_scope


def _date(s: str) -> dt.date:
    return dt.date.fromisoformat(s)


async def _with_db(coro):
    db = Database(get_settings().database_url)
    await db.connect()
    try:
        return await coro(db)
    finally:
        await db.close()


async def _cmd_plan(args) -> None:
    async def run(db):
        plan = await plan_scope(db, args.table, args.start, args.end, args.transform)
        print(plan.summary())
        for d in plan.decisions:
            print(f"  {d.partition}  {d.action.value:8} {d.reason.value}")
    await _with_db(run)


async def _cmd_create(args) -> None:
    async def run(db):
        bid, plan = await orch.create_backfill(
            db, args.table, args.start, args.end, args.transform
        )
        print(f"created backfill id={bid} status={orch.PLANNED}")
        print(plan.summary())
    await _with_db(run)


async def _cmd_run(args) -> None:
    async def run(db):
        status = await orch.run_backfill(
            db, args.id, max_concurrency=args.concurrency, max_removed_keys=args.max_removed_keys
        )
        print(f"backfill {args.id} -> {status}")
    await _with_db(run)


async def _cmd_publish(args) -> None:
    async def run(db):
        status = await orch.publish_backfill(db, args.id)
        print(f"backfill {args.id} -> {status}")
    await _with_db(run)


async def _cmd_status(args) -> None:
    async def run(db):
        detail = await orch.backfill_detail(db, args.id)
        print(json.dumps(detail, indent=2, default=str))
    await _with_db(run)


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="rewind", description="Rewind backfill CLI")
    sub = p.add_subparsers(dest="command", required=True)

    def add_range(sp):
        sp.add_argument("--table", required=True)
        sp.add_argument("--start", type=_date, required=True)
        sp.add_argument("--end", type=_date, required=True)
        sp.add_argument("--transform", required=True)

    add_range(sub.add_parser("plan", help="preview scope"))
    add_range(sub.add_parser("create", help="create a backfill (planned)"))

    run_p = sub.add_parser("run", help="stage + reconcile")
    run_p.add_argument("--id", type=int, required=True)
    run_p.add_argument("--concurrency", type=int, default=4)
    run_p.add_argument("--max-removed-keys", dest="max_removed_keys", type=int, default=0)

    pub_p = sub.add_parser("publish", help="gated atomic publish")
    pub_p.add_argument("--id", type=int, required=True)

    st_p = sub.add_parser("status", help="show backfill detail")
    st_p.add_argument("--id", type=int, required=True)
    return p


_DISPATCH = {
    "plan": _cmd_plan,
    "create": _cmd_create,
    "run": _cmd_run,
    "publish": _cmd_publish,
    "status": _cmd_status,
}


def main() -> None:
    args = _build_parser().parse_args()
    asyncio.run(_DISPATCH[args.command](args))


if __name__ == "__main__":
    main()
