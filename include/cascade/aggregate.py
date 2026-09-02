from __future__ import annotations

from sqlalchemy.orm import Session

from cascade.store import aggregate_campaign_projection


def aggregate_campaign(session: Session, campaign_id: str) -> dict:
    """Recompute and persist the campaign projection; safe to run repeatedly."""
    return aggregate_campaign_projection(session, campaign_id)
