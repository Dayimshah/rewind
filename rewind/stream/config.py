"""Kafka connection + topic naming for the Rewind streaming layer (Phase 2)."""
from __future__ import annotations

import os


def bootstrap_servers() -> str:
    """Kafka bootstrap servers; defaults to the compose EXTERNAL listener."""
    return os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")


def stream_topic(table: str) -> str:
    """Topic carrying the live event stream for a table."""
    return f"rewind.stream.{table}"


# Debezium publishes control-plane change events under this prefix (server name
# 'rewind' + schema 'public' + table), e.g. rewind.public.backfills.
CDC_PREFIX = "rewind.public"


def cdc_topic(table_name: str) -> str:
    return f"{CDC_PREFIX}.{table_name}"
