"""API happy-path test (Week 6).

Drives the full lifecycle over HTTP with httpx's ASGITransport. Rather than
run the app's lifespan (which would open a second DB pool on a different loop),
we inject the test's own ``db`` pool into ``app.state`` — the endpoints only
depend on that attribute.
"""
from __future__ import annotations

import datetime as dt

import httpx
import pytest

from rewind.api.main import app
from rewind.config import get_settings
from rewind.registry.registry import register_table
from tests.helpers import unique_table, write_raw

TODAY = dt.date(2026, 11, 20)
DAYS = [dt.date(2026, 10, 1), dt.date(2026, 10, 2)]


@pytest.fixture
async def client(db):
    app.state.db = db
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def test_health(client):
    r = await client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


async def test_full_lifecycle_over_http(client, db):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    for d in DAYS:
        write_raw(get_settings().raw_root, table, d)

    body = {
        "table": table,
        "range_start": DAYS[0].isoformat(),
        "range_end": DAYS[-1].isoformat(),
        "transform_version": "v7",
    }

    # preview
    r = await client.post("/plan", json=body)
    assert r.status_code == 200
    assert r.json()["included"] == [d.isoformat() for d in DAYS]

    # create
    r = await client.post("/backfills", json=body)
    assert r.status_code == 200
    bid = r.json()["backfill_id"]
    assert r.json()["status"] == "planned"

    # run
    r = await client.post(f"/backfills/{bid}/run", json={})
    assert r.status_code == 200
    assert r.json()["status"] == "publishable"

    # publish
    r = await client.post(f"/backfills/{bid}/publish")
    assert r.status_code == 200
    assert r.json()["status"] == "published"

    # status reflects published + reconciliation rows
    r = await client.get(f"/backfills/{bid}")
    assert r.status_code == 200
    detail = r.json()
    assert detail["status"] == "published"
    assert len(detail["reconciliation"]) == 2
    assert all(rr["verdict"] == "pass" for rr in detail["reconciliation"])


async def test_publish_before_run_conflicts(client, db):
    table = unique_table()
    await register_table(db, table, hot_window_days=2)
    write_raw(get_settings().raw_root, table, DAYS[0])
    body = {
        "table": table,
        "range_start": DAYS[0].isoformat(),
        "range_end": DAYS[0].isoformat(),
        "transform_version": "v7",
    }
    bid = (await client.post("/backfills", json=body)).json()["backfill_id"]
    r = await client.post(f"/backfills/{bid}/publish")
    assert r.status_code == 409


async def test_unknown_table_is_400(client):
    body = {
        "table": "does_not_exist_" + unique_table(),
        "range_start": DAYS[0].isoformat(),
        "range_end": DAYS[-1].isoformat(),
        "transform_version": "v7",
    }
    r = await client.post("/plan", json=body)
    assert r.status_code == 400
