from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cascade.airflow_client import AirflowAPIError, AirflowClient
from cascade.store import chip_facets, get_campaign, get_campaign_rollup, get_timeline, query_accounts, upsert_campaign
from cascade.api.routes.common import session_scope

router = APIRouter(prefix="/campaigns", tags=["campaigns"])


@router.get("/{campaign_id}")
def campaign(campaign_id: str, session: Session = Depends(session_scope)):
    value = get_campaign_rollup(session, campaign_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return value


@router.get("/{campaign_id}/accounts")
def campaign_accounts(campaign_id: str, status: str | None = None, segment: str | None = None, risk: str | None = None, q: str | None = None, chip: str | None = None, limit: int = 200, offset: int = 0, session: Session = Depends(session_scope)):
    if get_campaign_rollup(session, campaign_id) is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    limit = max(1, min(500, limit))
    offset = max(0, offset)
    filters = {"status": status, "segment": segment, "risk": risk, "q": q, "chip": chip}
    result = query_accounts(session, campaign_id, filters, limit=limit, offset=offset)
    result["facets"] = {"chips": chip_facets(session, campaign_id, filters)}
    return result


@router.get("/{campaign_id}/timeline")
def campaign_timeline(campaign_id: str, account_id: str | None = None, session: Session = Depends(session_scope)):
    if get_campaign_rollup(session, campaign_id) is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return get_timeline(session, campaign_id, account_id)


@router.post("/{campaign_id}/verify")
def verify_campaign(campaign_id: str, session: Session = Depends(session_scope)):
    campaign = get_campaign(session, campaign_id)
    if campaign is None:
        raise HTTPException(status_code=404, detail="Campaign not found")
    try:
        result = AirflowClient().trigger_dag("migration_verification", {"campaign_id": campaign_id})
    except AirflowAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    run_id = result.get("dag_run_id") or result.get("run_id")
    if run_id:
        # Keep the campaign projection intact while making this newly-triggered
        # verification run the one used by the orchestration endpoint.
        upsert_campaign(session, {**campaign, "verification_run_id": run_id})
        session.commit()
    return result
