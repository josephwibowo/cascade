#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from cascade.airflow_client import AirflowClient
from cascade.mock_client import MockSystemsClient
from cascade.store import clear_all, create_session_factory, database_counts, ensure_schema


def _dag_runs(client: AirflowClient, dag_id: str) -> list[dict[str, Any]]:
    """Read all runs, following Airflow's offset pagination semantics."""

    runs: list[dict[str, Any]] = []
    offset = 0
    seen_ids: set[str] = set()
    while True:
        payload = client._request("GET", f"/dags/{dag_id}/dagRuns", params={"limit": 100, "offset": offset})
        page = payload if isinstance(payload, list) else payload.get("dag_runs", payload.get("items", []))
        if not page:
            break
        for run in page:
            run_id = run.get("dag_run_id") or run.get("run_id")
            if run_id and run_id not in seen_ids:
                seen_ids.add(run_id)
                runs.append(run)
        total = int(payload.get("total_entries") or 0) if isinstance(payload, dict) else 0
        offset += len(page)
        if (total and offset >= total) or len(page) < 100:
            break
        # Protect reset from a non-conforming API that ignores offset.
        if len(seen_ids) < offset:
            break
    return runs


def _reset_watermark(client: AirflowClient) -> None:
    """Reset and read back the Airflow-only telemetry watermark."""

    try:
        from airflow.models import Variable
    except ImportError:
        # Host-shell runs do not have Airflow's Python package. Use the public
        # API instead so a reset is still verified against the real state store.
        client._request("PATCH", "/variables/cascade_last_snapshot", json={"value": "day0"})
        observed = client._request("GET", "/variables/cascade_last_snapshot")
        value = observed.get("value") if isinstance(observed, dict) else None
    else:
        Variable.set("cascade_last_snapshot", "day0")
        value = Variable.get("cascade_last_snapshot", default_var=None)
    if value != "day0":
        raise RuntimeError(f"Airflow watermark was not reset (observed {value!r})")


def main() -> None:
    failures: list[str] = []
    mock = MockSystemsClient()
    try:
        result = mock.advance("day0")
        if result.get("snapshot") != "day0" or mock.scenario().get("snapshot") != "day0":
            raise RuntimeError("mock systems did not reset to day0")
    except Exception as exc:
        failures.append(f"mock scenario reset failed: {exc}")

    client = AirflowClient()
    try:
        _reset_watermark(client)
    except Exception as exc:
        failures.append(f"Airflow watermark reset failed: {exc}")

    try:
        ensure_schema()
        session = create_session_factory()()
        try:
            clear_all(session)
            counts = database_counts(session)
        finally:
            session.close()
        if any(counts.values()):
            raise RuntimeError(f"Cascade tables still contain rows: {counts}")
    except Exception as exc:
        failures.append(f"Cascade database reset failed: {exc}")

    for dag_id in ("product_change_assessment", "exception_resolution", "migration_verification"):
        try:
            runs = _dag_runs(client, dag_id)
            for run in runs:
                run_id = run.get("dag_run_id") or run.get("run_id")
                if run_id:
                    client._request("DELETE", f"/dags/{dag_id}/dagRuns/{run_id}")
            remaining = _dag_runs(client, dag_id)
            if remaining:
                lingering = [run.get("dag_run_id") or run.get("run_id") for run in remaining]
                raise RuntimeError(f"runs remain after deletion: {lingering}")
        except Exception as exc:
            failures.append(f"{dag_id} run cleanup failed: {exc}")

    if failures:
        for failure in failures:
            print(f"Error: {failure}", file=sys.stderr)
        raise SystemExit(1)
    print("Cascade demo reset to day0")


if __name__ == "__main__":
    main()
