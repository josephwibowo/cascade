from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import httpx

# ``airflow.utils.state.TaskInstanceState``, spelled out rather than imported:
# this client is also used from host shells with no Airflow install, and
# importing Airflow here re-enters plugin loading and deadlocks on the plugin
# that imports this module.  A state missing from this list is still counted,
# it just lands in the "none" bucket, so drift degrades rather than loses work.
TASK_INSTANCE_STATES: tuple[str, ...] = (
    "removed", "scheduled", "queued", "running", "success", "restarting",
    "failed", "up_for_retry", "up_for_reschedule", "upstream_failed",
    "skipped", "deferred", "awaiting_input",
)

# Each state costs one request, and a loaded Airflow serialises them, so the
# live rail asks only for what it renders.  Everything else is reported as
# "none"; "failed" is here so a failing wave cannot masquerade as one that has
# not started.
ORCHESTRATION_STATES: tuple[str, ...] = ("running", "success", "failed", "awaiting_input")


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

    def mapped_state_counts(
        self, dag_id: str, run_id: str, task_id: str, states: tuple[str, ...] = TASK_INSTANCE_STATES
    ) -> tuple[int, dict[str, int]]:
        """Count mapped instances per state without downloading any of them.

        ``listMapped`` caps a page at 100 rows, so tallying the 2,417 mapped
        assessments by walking pages costs tens of round trips on every poll.
        Airflow reports ``total_entries`` for a filtered query, so one
        ``limit=1`` request per state is enough, and they are independent
        enough to issue together.
        """

        path = f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/listMapped"

        def total(state: str | None) -> int:
            params: dict[str, Any] = {"limit": 1}
            if state is not None:
                params["state"] = state
            page = self._request("GET", path, params=params)
            if isinstance(page, list):
                return len(page)
            return int(page.get("total_entries") or 0)

        queries: tuple[str | None, ...] = (None, *states)
        with ThreadPoolExecutor(max_workers=len(queries)) as pool:
            mapped, *totals = list(pool.map(total, queries))
        counts = {state: value for state, value in zip(states, totals) if value}
        unaccounted = mapped - sum(counts.values())
        if unaccounted > 0:
            # Instances in a state this call did not ask about, plus expanded
            # instances the scheduler has not reached, which carry no state at
            # all and match no filter.
            counts["none"] = unaccounted
        return mapped, counts

    def orchestration_counts(self, dag_id: str, run_id: str, task_id: str) -> dict[str, Any]:
        run = self.dag_run(dag_id, run_id)
        mapped, counts = self.mapped_state_counts(dag_id, run_id, task_id, ORCHESTRATION_STATES)
        run_state = run.get("state") or run.get("dag_run_state")
        return {
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "dag_run_state": run_state,
            "run_state": run_state,
            "mapped": mapped,
            "states": counts,
            **counts,
        }
