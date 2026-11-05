"""Thin asyncpg wrapper for the control plane.

asyncpg does not understand the ``postgresql+asyncpg://`` SQLAlchemy-style
scheme, nor the ``postgresql://`` scheme's every option. We normalise the URL
and expose a small pool-backed helper used by the API, migrations, registry,
and tests.
"""
from __future__ import annotations

import asyncpg


def _normalise_dsn(url: str) -> str:
    """Make a DSN asyncpg accepts.

    Accepts both ``postgresql://`` and ``postgres://`` (asyncpg is fine with
    both) and strips an optional ``+asyncpg`` driver suffix if present.
    """
    return url.replace("postgresql+asyncpg://", "postgresql://")


class Database:
    """Owns an asyncpg connection pool."""

    def __init__(self, url: str) -> None:
        self._dsn = _normalise_dsn(url)
        self._pool: asyncpg.Pool | None = None

    async def connect(self) -> None:
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn, min_size=1, max_size=10
            )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database.connect() was not called")
        return self._pool

    async def ping(self) -> bool:
        """Return True if a trivial query succeeds."""
        try:
            async with self.pool.acquire() as conn:
                return (await conn.fetchval("SELECT 1")) == 1
        except Exception:
            return False

    # Convenience passthroughs -------------------------------------------
    async def execute(self, query: str, *args) -> str:
        async with self.pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def fetch(self, query: str, *args) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchrow(self, query: str, *args) -> asyncpg.Record | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def fetchval(self, query: str, *args):
        async with self.pool.acquire() as conn:
            return await conn.fetchval(query, *args)
