"""Builders for links from Cascade records back to exact Airflow work."""

from __future__ import annotations

from urllib.parse import quote


def _quote(value: str) -> str:
    return quote(str(value), safe="")


def dag_run_grid_url(
    dag_id: str,
    run_id: str,
    *,
    base_url: str = "",
) -> str:
    """Return the native Airflow 3 grid URL for one concrete DAG run."""

    return f"{base_url.rstrip('/')}/dags/{_quote(dag_id)}/runs/{_quote(run_id)}/grid"


def mapped_task_url(
    dag_id: str,
    run_id: str,
    task_id: str,
    map_index: int = -1,
    *,
    base_url: str = "",
) -> str:
    """Return a run grid URL focused on one mapped task instance."""

    url = dag_run_grid_url(dag_id, run_id, base_url=base_url)
    return f"{url}?task_id={_quote(task_id)}&map_index={int(map_index)}"


def account_task_url(account: dict, *, base_url: str = "") -> str | None:
    """Build an account link from its task-instance provenance, if present."""

    task = account.get("latest_airflow_task_instance") or {}
    if not task.get("dag_id") or not task.get("run_id") or not task.get("task_id"):
        return None
    return mapped_task_url(
        task["dag_id"], task["run_id"], task["task_id"], task.get("map_index", -1), base_url=base_url
    )


def exception_task_url(exception: dict, *, base_url: str = "") -> str | None:
    """Build the exact HITL task link for an exception record, if available."""

    dag_id = exception.get("airflow_dag_id") or "exception_resolution"
    run_id = exception.get("airflow_dag_run_id")
    task_id = exception.get("hitl_task_id")
    if not run_id or not task_id:
        return None
    return mapped_task_url(dag_id, run_id, task_id, -1, base_url=base_url)
