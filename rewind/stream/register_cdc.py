"""Register the Debezium Postgres connector for the Rewind control plane (Phase 2).

Posts ``debezium_connector.json`` to the Kafka Connect REST API. Debezium then
performs logical decoding of the ``backfills`` and ``partition_tasks`` tables,
emitting every row change to Kafka topics:

    rewind.public.backfills
    rewind.public.partition_tasks

This turns the backfill lifecycle (status transitions, task state) into a
real-time event stream — operational CDC off the control database.

Usage (stack must be up):
    python -m rewind.stream.register_cdc
    python -m rewind.stream.register_cdc --status     # show connector status
    python -m rewind.stream.register_cdc --delete     # remove the connector
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import httpx

CONNECT_URL = os.environ.get("CONNECT_URL", "http://localhost:8083")
_CONFIG = Path(__file__).with_name("debezium_connector.json")
_NAME = "rewind-control-plane"


def register() -> None:
    cfg = json.loads(_CONFIG.read_text(encoding="utf-8"))
    # PUT /connectors/{name}/config is idempotent (create-or-update).
    r = httpx.put(
        f"{CONNECT_URL}/connectors/{_NAME}/config",
        json=cfg["config"],
        timeout=30,
    )
    if r.status_code not in (200, 201):
        print(f"register failed: {r.status_code} {r.text}", file=sys.stderr)
        sys.exit(1)
    print(f"connector '{_NAME}' registered (HTTP {r.status_code})")


def status() -> None:
    r = httpx.get(f"{CONNECT_URL}/connectors/{_NAME}/status", timeout=30)
    if r.status_code == 404:
        print("connector not registered")
        return
    print(json.dumps(r.json(), indent=2))


def delete() -> None:
    r = httpx.delete(f"{CONNECT_URL}/connectors/{_NAME}", timeout=30)
    print(f"delete -> HTTP {r.status_code}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Manage the Rewind Debezium connector.")
    ap.add_argument("--status", action="store_true", help="show connector status")
    ap.add_argument("--delete", action="store_true", help="remove the connector")
    args = ap.parse_args()
    if args.status:
        status()
    elif args.delete:
        delete()
    else:
        register()


if __name__ == "__main__":
    main()
