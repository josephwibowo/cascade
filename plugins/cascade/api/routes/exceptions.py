from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import update
from sqlalchemy.orm import Session

from cascade.airflow_client import AirflowAPIError, AirflowClient
from cascade.models import ExceptionRecord
from cascade.store import get_pending_exceptions
from cascade.api.routes.common import session_scope

router = APIRouter(prefix="/exceptions", tags=["exceptions"])


class RespondRequest(BaseModel):
    chosen_options: list[str] = Field(min_length=1)
    reason: str = Field(min_length=1)


@router.get("/pending")
def pending_exceptions(session: Session = Depends(session_scope)):
    rows = get_pending_exceptions(session)
    hitl_count = None
    error = None
    try:
        hitl_count = sum(1 for row in rows if row["hitl_task_id"] and row["airflow_dag_run_id"] and _is_awaiting(row))
    except AirflowAPIError as exc:
        error = str(exc)
    return {"items": rows, "product_exception_count": len(rows), "airflow_awaiting_input": hitl_count, "airflow_error": error}


def _is_awaiting(row: dict) -> bool:
    details = AirflowClient().hitl_details("exception_resolution", row["airflow_dag_run_id"])
    return any(
        item.get("state") == "awaiting_input"
        or item.get("task_state") == "awaiting_input"
        or (item.get("task_instance") or {}).get("state") == "awaiting_input"
        for item in details
    )


@router.post("/{exception_id}/respond")
def respond(exception_id: str, request: RespondRequest, session: Session = Depends(session_scope)):
    row = session.get(ExceptionRecord, exception_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Exception not found")
    if not row.hitl_task_id or not row.airflow_dag_run_id:
        raise HTTPException(status_code=409, detail="This product exception has no Airflow decision task")
    client = AirflowClient()
    try:
        client.submit_hitl("exception_resolution", row.airflow_dag_run_id, row.hitl_task_id, -1, request.chosen_options, request.reason)
        observed = client.wait_for_task_terminal("exception_resolution", row.airflow_dag_run_id, row.hitl_task_id, -1)
    except AirflowAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    state = observed.get("state")
    if state == "success":
        session.execute(update(ExceptionRecord).where(ExceptionRecord.id == exception_id).values(status="RESOLVED", decision=request.chosen_options[0], decision_reason=request.reason, resolved_at=datetime.now(timezone.utc)))
        session.commit()
    return {"exception_id": exception_id, "state": state, "airflow": observed}
