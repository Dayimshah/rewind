"""Shared pytest fixtures for Rewind tests.

Tests need the Postgres control DB (via docker compose) and an isolated
DATA_ROOT. We point DATA_ROOT at a temp dir for the whole test session *before*
importing anything that caches settings.

pytest-asyncio runs each async test on its own function-scoped event loop, and
asyncpg pools are bound to the loop that created them. So the ``db`` fixture is
function-scoped: each test gets a fresh pool on its own loop. Migrations run
once per session via an autouse setup that uses a throwaway pool.

Run the control DB first:
    docker compose up -d postgres
"""
from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pytest
import pytest_asyncio

# Redirect the data plane to a throwaway dir before settings is first read.
_TMP_DATA = tempfile.mkdtemp(prefix="rewind-test-data-")
os.environ["DATA_ROOT"] = _TMP_DATA

from rewind.config import get_settings  # noqa: E402
from rewind.db.client import Database  # noqa: E402
from rewind.db.migrate import run_migrations  # noqa: E402

_migrated = False


@pytest.fixture(scope="session")
def settings():
    s = get_settings()
    s.ensure_dirs()
    return s


@pytest_asyncio.fixture
async def db(settings):
    """Function-scoped DB pool (bound to this test's event loop).

    Applies migrations once per session (idempotent runner).
    """
    global _migrated
    database = Database(settings.database_url)
    await database.connect()
    if not _migrated:
        await run_migrations(database)
        _migrated = True
    try:
        yield database
    finally:
        await database.close()


@pytest.fixture
def data_root(settings) -> Path:
    return settings.raw_root
