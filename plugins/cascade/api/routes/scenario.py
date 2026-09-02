from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from cascade.airflow_client import AirflowAPIError, AirflowClient
from cascade.mock_client import MockSystemsClient
from cascade.store import create_session_factory, get_campaign, upsert_campaign

router = APIRouter(prefix="/scenario", tags=["scenario"])
orchestration_router = APIRouter(prefix="/orchestration", tags=["orchestration"])


class AdvanceRequest(BaseModel):
    snapshot: str


@router.get("")
def scenario():
    try:
        return MockSystemsClient().scenario()
    except AirflowAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post("/advance")
def advance(request: AdvanceRequest):
    try:
        result = MockSystemsClient().advance(request.snapshot)
        run = AirflowClient().trigger_dag("migration_verification", {"snapshot": request.snapshot})
    except AirflowAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    run_id = run.get("dag_run_id") or run.get("run_id")
    session = create_session_factory()()
    try:
        campaign = get_campaign(session, "api_v1_sunset")
        if campaign and run_id:
            upsert_campaign(session, {"id": campaign["id"], "name": campaign["name"], "change_type": campaign["change_type"], "deadline": campaign["deadline"], "airflow_dag_run_id": campaign["airflow_dag_run_id"], "verification_run_id": run_id, "status": campaign["status"]})
            session.commit()
    finally:
        session.close()
    return {"snapshot": result["snapshot"], "dag_run_id": run_id}


@orchestration_router.get("/{campaign_id}")
def orchestration(campaign_id: str):
    session = create_session_factory()()
    try:
        campaign = get_campaign(session, campaign_id)
        if not campaign:
            raise HTTPException(status_code=404, detail="Campaign not found")
        verification_run_id = campaign.get("verification_run_id")
        run_id = verification_run_id or campaign.get("airflow_dag_run_id")
    finally:
        session.close()
    if not run_id:
        return {"status": "not_started", "mapped": 0, "states": {}}
    dag_id, task_id = ("migration_verification", "verify_account") if verification_run_id else ("product_change_assessment", "assess_account")
    try:
        return AirflowClient().orchestration_counts(dag_id, run_id, task_id)
    except AirflowAPIError as exc:
        raise HTTPException(status_code=503, detail=f"Airflow orchestration state unavailable: {exc}") from exc
