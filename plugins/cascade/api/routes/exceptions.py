from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session
from typing import Any

from cascade.airflow_client import AirflowAPIError, AirflowClient
from cascade.store import get_exception, get_pending_exceptions, update_exception
from cascade.api.routes.common import session_scope

router = APIRouter(prefix="/exceptions", tags=["exceptions"])


class RespondRequest(BaseModel):
    chosen_options: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1)
    params_input: dict[str, Any] = Field(default_factory=dict)


@router.get("/pending")
def pending_exceptions(session: Session = Depends(session_scope)):
    rows = get_pending_exceptions(session)
    hitl_count = None
    error = None
    enriched = []
    for row in rows:
        details = None
        if row["hitl_task_id"] and row["airflow_dag_run_id"]:
            try:
                details = _details_for(row)
            except AirflowAPIError as exc:
                # Keep product exception data visible while surfacing the
                # Airflow failure; the UI must not invent a decision form.
                error = str(exc)
        enriched.append({**row, "hitl_details": details})
    rows = enriched
    if error is None:
        hitl_count = sum(1 for row in rows if row["hitl_details"] and _is_awaiting_details(row["hitl_details"]))
    return {"items": rows, "product_exception_count": len(rows), "airflow_awaiting_input": hitl_count, "airflow_error": error}


def _is_awaiting(row: dict) -> bool:
    details = AirflowClient().hitl_details("exception_resolution", row["airflow_dag_run_id"])
    return _is_awaiting_details(details)


def _is_awaiting_details(details: dict | list[dict]) -> bool:
    entries = details if isinstance(details, list) else [details]
    entries = [item for item in entries if isinstance(item, dict)]
    return any(
        item.get("state") == "awaiting_input"
        or item.get("task_state") == "awaiting_input"
        or (item.get("task_instance") or {}).get("state") == "awaiting_input"
        for item in entries
    )


def _details_for(row: dict) -> dict[str, Any] | None:
    """Return the Airflow-provided HITL form for this exception's task."""

    details = AirflowClient().hitl_details("exception_resolution", row["airflow_dag_run_id"])
    candidates = [item for item in details if isinstance(item, dict)]
    selected = next(
        (
            item
            for item in candidates
            if item.get("task_id") == row["hitl_task_id"]
            or (item.get("task_instance") or {}).get("task_id") == row["hitl_task_id"]
        ),
        candidates[0] if len(candidates) == 1 else None,
    )
    if not selected:
        return None
    nested = selected.get("hitl") or selected.get("hitl_details") or selected.get("task") or {}
    if not isinstance(nested, dict):
        nested = {}
    # Airflow has returned these fields both at the top level and nested under
    # task details across supported API versions. Preserve the source values,
    # normalizing only the envelope consumed by Cascade's UI.
    subject = selected.get("subject", nested.get("subject"))
    body = selected.get("body", nested.get("body"))
    options = selected.get("options", nested.get("options", []))
    parameters = selected.get("params", selected.get("parameters", nested.get("params", nested.get("parameters", {}))))
    return {
        "subject": subject,
        "body": body,
        "options": options if isinstance(options, list) else [],
        "parameters": parameters if isinstance(parameters, dict) else {},
        "state": selected.get("state") or selected.get("task_state") or (selected.get("task_instance") or {}).get("state"),
    }


@router.post("/{exception_id}/respond")
def respond(exception_id: str, request: RespondRequest, session: Session = Depends(session_scope)):
    row = get_exception(session, exception_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Exception not found")
    if not row["hitl_task_id"] or not row["airflow_dag_run_id"]:
        raise HTTPException(status_code=409, detail="This product exception has no Airflow decision task")
    client = AirflowClient()
    try:
        client.submit_hitl("exception_resolution", row["airflow_dag_run_id"], row["hitl_task_id"], -1, request.chosen_options, request.reason, request.params_input)
        observed = client.wait_for_task_terminal("exception_resolution", row["airflow_dag_run_id"], row["hitl_task_id"], -1)
    except AirflowAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    state = observed.get("state")
    if state == "success":
        update_exception(session, exception_id, {"status": "RESOLVED", "decision": request.chosen_options[0], "decision_reason": request.reason, "resolved_at": datetime.now(timezone.utc)})
        session.commit()
    return {"exception_id": exception_id, "state": state, "airflow": observed}
