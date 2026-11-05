"""Airflow DAG wrapping the Rewind backfill lifecycle (Phase 2).

Orchestrates a safe backfill as a DAG, calling the Rewind control-plane API
over HTTP so Airflow stays decoupled from the engine internals (it triggers the
service; it does not import it):

    plan  ->  create  ->  run (stage + reconcile)  ->  branch
                                                       |- publishable -> publish
                                                       |- failed      -> halt

The reconciliation gate is honoured: ``publish`` only runs when ``run`` reports
``publishable``. A failed reconciliation routes to a terminal ``halt`` task, so
a data-losing backfill never publishes.

Trigger with a config (Airflow UI "Trigger DAG w/ config" or CLI):
    {"table": "fct_revenue", "range_start": "2026-09-20",
     "range_end": "2026-11-02", "transform_version": "v7"}
"""
from __future__ import annotations

import os
import urllib.request
import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

REWIND_API = os.environ.get("REWIND_API", "http://api:8010")


def _post(path: str, body: dict | None) -> dict:
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        f"{REWIND_API}{path}", data=data,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.loads(resp.read().decode())


def _conf(context, key, default=None):
    return (context["dag_run"].conf or {}).get(key, default)


def plan(**context):
    body = {
        "table": _conf(context, "table", "fct_revenue"),
        "range_start": _conf(context, "range_start"),
        "range_end": _conf(context, "range_end"),
        "transform_version": _conf(context, "transform_version", "v7"),
    }
    context["ti"].xcom_push(key="request", value=body)
    plan = _post("/plan", body)
    print(f"planned scope: {len(plan['included'])} partitions to reprocess")
    return len(plan["included"])


def create(**context):
    body = context["ti"].xcom_pull(key="request")
    res = _post("/backfills", body)
    bid = res["backfill_id"]
    context["ti"].xcom_push(key="backfill_id", value=bid)
    print(f"created backfill #{bid} (status={res['status']})")
    return bid


def run(**context):
    bid = context["ti"].xcom_pull(key="backfill_id")
    res = _post(f"/backfills/{bid}/run", {})
    status = res["status"]
    context["ti"].xcom_push(key="run_status", value=status)
    print(f"backfill #{bid} run -> {status}")
    return status


def gate(**context):
    status = context["ti"].xcom_pull(key="run_status")
    # Branch: only proceed to publish when reconciliation passed.
    return "publish" if status == "publishable" else "halt"


def publish(**context):
    bid = context["ti"].xcom_pull(key="backfill_id")
    res = _post(f"/backfills/{bid}/publish", {})
    print(f"backfill #{bid} publish -> {res['status']}")


def halt(**context):
    bid = context["ti"].xcom_pull(key="backfill_id")
    raise RuntimeError(
        f"backfill #{bid} did not reach 'publishable' (reconciliation gate held); "
        f"refusing to publish"
    )


with DAG(
    dag_id="rewind_backfill",
    description="Safe backfill lifecycle (plan -> run -> reconcile gate -> publish)",
    schedule=None,            # triggered manually with a config
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["rewind", "backfill"],
) as dag:
    t_plan = PythonOperator(task_id="plan", python_callable=plan)
    t_create = PythonOperator(task_id="create", python_callable=create)
    t_run = PythonOperator(task_id="run", python_callable=run)
    t_gate = BranchPythonOperator(task_id="gate", python_callable=gate)
    t_publish = PythonOperator(task_id="publish", python_callable=publish)
    t_halt = PythonOperator(task_id="halt", python_callable=halt)

    t_plan >> t_create >> t_run >> t_gate >> [t_publish, t_halt]
