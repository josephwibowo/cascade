#!/usr/bin/env python3
from __future__ import annotations

import os
import sys
import time
from collections import Counter
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from cascade.airflow_client import AirflowClient
from cascade.mock_client import MockSystemsClient
from cascade.store import create_session_factory, ensure_schema, get_campaign_rollup, upsert_campaign


def main() -> None:
    MockSystemsClient().advance("day0")
    ensure_schema()
    session = create_session_factory()()
    try:
        from cascade.store import clear_all
        clear_all(session)
    finally:
        session.close()
    run = AirflowClient().trigger_dag("product_change_assessment", {"scenario": "day0"})
    run_id = run.get("dag_run_id") or run.get("run_id")
    if not run_id:
        raise RuntimeError(f"Airflow did not return a dag_run_id: {run}")
    client = AirflowClient()
    # LocalExecutor task startup/parsing is deliberately visible in this
    # demo; give the full mapped population enough time on a laptop-sized
    # Astro stack while still failing rather than polling forever.
    deadline = time.monotonic() + 3_600
    while time.monotonic() < deadline:
        state = client.dag_run("product_change_assessment", run_id).get("state")
        if state in {"success", "failed"}:
            break
        time.sleep(3)
    else:
        raise TimeoutError("Hero run did not reach a terminal state")
    if state != "success":
        raise RuntimeError(f"Hero run ended in {state}")
    mapped = client.list_mapped("product_change_assessment", run_id, "assess_account")
    successful = sum(item.get("state") == "success" for item in mapped)
    if successful <= 1000:
        raise AssertionError(f"Expected >1,000 successful mapped tasks, got {successful}")
    session = create_session_factory()()
    try:
        campaign = session.get(__import__("cascade.models", fromlist=["Campaign"]).Campaign, "api_v1_sunset")
        if campaign:
            upsert_campaign(session, {"id": campaign.id, "name": campaign.name, "change_type": campaign.change_type, "deadline": campaign.deadline, "airflow_dag_run_id": run_id, "verification_run_id": None, "status": campaign.status})
            session.commit()
        rollup = get_campaign_rollup(session, "api_v1_sunset")
    finally:
        session.close()
    expected = {"NOT_STARTED": 1871, "IN_PROGRESS": 495, "BLOCKED": 51, "READY_TO_VERIFY": 0, "MIGRATED": 0}
    actual = {key: (rollup or {}).get("status_distribution", {}).get(key, 0) for key in expected}
    if actual != expected:
        raise AssertionError(f"Unexpected day0 distribution: {(rollup or {}).get('status_distribution')}")
    print(f"Hero run {run_id}: {successful} successful mapped assessments")


if __name__ == "__main__":
    main()
