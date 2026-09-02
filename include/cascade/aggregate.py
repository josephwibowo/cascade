from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from cascade.models import AccountMigration, Campaign
from cascade.store import get_campaign_rollup, upsert_campaign


def aggregate_campaign(session: Session, campaign_id: str) -> dict:
    """Recompute and persist the campaign projection; safe to run repeatedly."""
    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        raise ValueError(f"Campaign {campaign_id!r} does not exist")
    counts = session.execute(
        select(func.count(AccountMigration.account_id), func.coalesce(func.sum(AccountMigration.arr), 0))
        .where(AccountMigration.campaign_id == campaign_id)
    ).one()
    status = session.execute(
        select(AccountMigration.status, func.count()).where(AccountMigration.campaign_id == campaign_id).group_by(AccountMigration.status)
    ).all()
    migrated = next((count for value, count in status if value == "MIGRATED"), 0)
    total = counts[0] or 0
    next_status = "MIGRATED" if total and migrated == total else "IN_PROGRESS"
    upsert_campaign(session, {
        "id": campaign_id,
        "name": campaign.name,
        "change_type": campaign.change_type,
        "deadline": campaign.deadline,
        "airflow_dag_run_id": campaign.airflow_dag_run_id,
        "status": next_status,
        "affected_accounts": total,
        "affected_arr": float(counts[1] or 0),
        "updated_at": datetime.now(timezone.utc),
    })
    session.commit()
    return get_campaign_rollup(session, campaign_id) or {}
