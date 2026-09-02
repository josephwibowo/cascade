from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cascade.store import get_campaign_rollup, get_timeline, query_accounts
from cascade.api.routes.common import session_scope

router = APIRouter(prefix="/campaigns", tags=["campaigns"])


@router.get("/{campaign_id}")
def campaign(campaign_id: str, session: Session = Depends(session_scope)):
    value = get_campaign_rollup(session, campaign_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return value


@router.get("/{campaign_id}/accounts")
def campaign_accounts(campaign_id: str, status: str | None = None, segment: str | None = None, risk: str | None = None, q: str | None = None, chip: str | None = None, session: Session = Depends(session_scope)):
    if get_campaign_rollup(session, campaign_id) is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return {"items": query_accounts(session, campaign_id, {"status": status, "segment": segment, "risk": risk, "q": q, "chip": chip})}


@router.get("/{campaign_id}/timeline")
def campaign_timeline(campaign_id: str, account_id: str | None = None, session: Session = Depends(session_scope)):
    if get_campaign_rollup(session, campaign_id) is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return get_timeline(session, campaign_id, account_id)


@router.post("/{campaign_id}/verify")
def verify_campaign(campaign_id: str, session: Session = Depends(session_scope)):
    from cascade.airflow_client import AirflowClient
    if get_campaign_rollup(session, campaign_id) is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return AirflowClient().trigger_dag("migration_verification", {"campaign_id": campaign_id})
