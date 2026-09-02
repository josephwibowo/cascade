#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from cascade.airflow_client import AirflowClient
from cascade.mock_client import MockSystemsClient
from cascade.store import clear_all, create_session_factory, ensure_schema


def main() -> None:
    MockSystemsClient().advance("day0")
    # The verification DAG keeps only this small watermark in Airflow state.
    # Reset it alongside Postgres so a subsequent day-7 rehearsal discovers
    # the full, deterministic delta again.  The import is optional so the
    # script remains useful from a host shell as well as an Airflow container.
    try:
        from airflow.models import Variable

        Variable.set("cascade_last_snapshot", "day0")
    except ImportError:
        pass
    ensure_schema()
    session = create_session_factory()()
    try:
        clear_all(session)
    finally:
        session.close()
    client = AirflowClient()
    for dag_id in ("product_change_assessment", "exception_resolution", "migration_verification"):
        try:
            payload = client._request("GET", f"/dags/{dag_id}/dagRuns", params={"limit": 100})
            runs = payload if isinstance(payload, list) else payload.get("dag_runs", payload.get("items", []))
            for run in runs:
                run_id = run.get("dag_run_id") or run.get("run_id")
                if run_id:
                    client._request("DELETE", f"/dags/{dag_id}/dagRuns/{run_id}")
        except Exception as exc:
            print(f"Warning: could not clear {dag_id} runs: {exc}")
    print("Cascade demo reset to day0")


if __name__ == "__main__":
    main()
