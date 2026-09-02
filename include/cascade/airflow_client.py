from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from typing import Any

import httpx


class AirflowAPIError(RuntimeError):
    pass


class AirflowClient:
    def __init__(self, base_url: str | None = None, timeout: float = 30.0):
        self.base_url = (base_url or os.getenv("AIRFLOW_API_URL", "http://localhost:8080/api/v2")).rstrip("/")
        self.timeout = timeout

    def _request(self, method: str, path: str, **kwargs: Any) -> dict[str, Any] | list[Any]:
        try:
            response = httpx.request(method, f"{self.base_url}{path}", timeout=self.timeout, **kwargs)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise AirflowAPIError(str(exc)) from exc
        if response.status_code == 204:
            return {}
        return response.json()

    def trigger_dag(self, dag_id: str, conf: dict[str, Any] | None = None, logical_date: str | None = None) -> dict[str, Any]:
        payload: dict[str, Any] = {"conf": conf or {}, "logical_date": logical_date or datetime.now(timezone.utc).isoformat()}
        return self._request("POST", f"/dags/{dag_id}/dagRuns", json=payload)  # type: ignore[return-value]

    def dag_run(self, dag_id: str, run_id: str) -> dict[str, Any]:
        return self._request("GET", f"/dags/{dag_id}/dagRuns/{run_id}")  # type: ignore[return-value]

    def task_instance(self, dag_id: str, run_id: str, task_id: str, map_index: int = -1) -> dict[str, Any]:
        return self._request("GET", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/{map_index}")  # type: ignore[return-value]

    def list_mapped(self, dag_id: str, run_id: str, task_id: str, **params: Any) -> list[dict[str, Any]]:
        path = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/listMapped"
        query = dict(params)
        page = self._request("GET", path, params=query)
        if isinstance(page, list):
            return page
        items = list(page.get("task_instances", page.get("items", [])))
        total = int(page.get("total_entries") or len(items))
        # Airflow caps this endpoint at 100 rows even when a larger limit is
        # requested.  Follow its offset pagination so the live rail and the
        # hero verification count every mapped instance.
        offset = int(query.get("offset", 0))
        while len(items) < total and page.get("task_instances", page.get("items", [])):
            offset += len(page.get("task_instances", page.get("items", [])))
            query["offset"] = offset
            page = self._request("GET", path, params=query)
            if isinstance(page, list):
                items.extend(page)
                break
            chunk = page.get("task_instances", page.get("items", []))
            if not chunk:
                break
            items.extend(chunk)
        return items

    def hitl_details(self, dag_id: str, run_id: str) -> list[dict[str, Any]]:
        payload = self._request("GET", f"/dags/{dag_id}/dagRuns/{run_id}/hitlDetails")
        if isinstance(payload, list):
            return payload
        details = payload.get("hitl_details", payload.get("items"))
        if details is not None:
            return details if isinstance(details, list) else [details]
        # Some Airflow versions return one detail object directly for an
        # unmapped HITL task rather than wrapping it in a list.
        return [payload] if any(key in payload for key in ("subject", "body", "options", "params", "parameters")) else []

    def submit_hitl(
        self,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        chosen_options: list[str],
        reason: str,
        params_input: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        values = dict(params_input or {})
        values.setdefault("reason", reason)
        return self._request("PATCH", f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/{map_index}/hitlDetails", json={"chosen_options": chosen_options, "params_input": values})  # type: ignore[return-value]

    def wait_for_task_terminal(self, dag_id: str, run_id: str, task_id: str, map_index: int = -1, timeout: float = 120.0) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        terminal = {"success", "failed", "upstream_failed", "skipped", "removed"}
        while time.monotonic() < deadline:
            instance = self.task_instance(dag_id, run_id, task_id, map_index)
            if instance.get("state") in terminal:
                return instance
            time.sleep(1)
        raise AirflowAPIError(f"Timed out waiting for {dag_id}/{run_id}/{task_id}")

    def orchestration_counts(self, dag_id: str, run_id: str, task_id: str) -> dict[str, Any]:
        run = self.dag_run(dag_id, run_id)
        instances = self.list_mapped(dag_id, run_id, task_id)
        counts: dict[str, int] = {}
        for instance in instances:
            state = str(instance.get("state") or "none")
            counts[state] = counts.get(state, 0) + 1
        run_state = run.get("state") or run.get("dag_run_state")
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "dag_run_state": run_state,
            "run_state": run_state,
            "mapped": len(instances),
            "states": counts,
            **counts,
        }
