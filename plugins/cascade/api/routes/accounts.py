from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from cascade.store import get_account
from cascade.api.routes.common import session_scope

router = APIRouter(prefix="/accounts", tags=["accounts"])


@router.get("/{account_id}")
def account(account_id: str, campaign_id: str | None = None, session: Session = Depends(session_scope)):
    value = get_account(session, account_id, campaign_id)
    if value is None:
        raise HTTPException(status_code=404, detail="Account not found")
    return value
