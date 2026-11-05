"""Central configuration, loaded from environment variables."""
from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


class Settings:
    """Application settings sourced from the environment.

    Kept as a plain class (not pydantic BaseSettings) so it works identically
    in scripts, tests, and the API without import-time surprises.
    """

    def __init__(self) -> None:
        self.database_url: str = os.environ.get(
            "DATABASE_URL",
            "postgresql://rewind:rewind_dev@localhost:5440/rewind",
        )
        self.api_host: str = os.environ.get("API_HOST", "0.0.0.0")
        self.api_port: int = int(os.environ.get("API_PORT", "8010"))
        self.log_level: str = os.environ.get("LOG_LEVEL", "INFO")

        # Data plane root. Resolved to an absolute path so scripts run from
        # any working directory still agree on where partitions live.
        data_root = os.environ.get("DATA_ROOT", "./data")
        self.data_root: Path = Path(data_root).resolve()

        self.hot_window_days: int = int(os.environ.get("HOT_WINDOW_DAYS", "2"))

        # Publisher backend (Phase 2 opt-in). "pointer" is the Phase-1
        # filesystem pointer-swap; "iceberg" uses a real Apache Iceberg table
        # with atomic snapshot commits + time travel.
        self.publisher_backend: str = os.environ.get("PUBLISHER_BACKEND", "pointer")

    # Convenience paths within the data plane -----------------------------
    @property
    def raw_root(self) -> Path:
        """Where the event source / live stream write raw partitions."""
        return self.data_root / "raw"

    @property
    def published_root(self) -> Path:
        """Where published (live-readable) table partitions live."""
        return self.data_root / "published"

    @property
    def staging_root(self) -> Path:
        """Where backfills write reprocessed partitions before publish."""
        return self.data_root / "staging"

    @property
    def iceberg_root(self) -> Path:
        """Root for the Iceberg catalog DB + warehouse (Phase 2 backend)."""
        return self.data_root / "iceberg"

    @property
    def iceberg_warehouse(self) -> Path:
        return self.iceberg_root / "warehouse"

    @property
    def iceberg_catalog_uri(self) -> str:
        return f"sqlite:///{(self.iceberg_root / 'catalog.db').as_posix()}"

    def ensure_dirs(self) -> None:
        for p in (self.raw_root, self.published_root, self.staging_root):
            p.mkdir(parents=True, exist_ok=True)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
