"""The sole database access layer for Cascade product state."""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import create_engine, delete, func, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError

from cascade.models import AccountMigration, Base, Campaign, ExceptionRecord, TimelineEvent
from cascade.rules import zero_v1_streak


def database_url() -> str:
    return os.getenv("CASCADE_DB_URL", "sqlite:///cascade.db")


_schema_lock = threading.Lock()
_schema_ready: set[str] = set()


def ensure_schema(url: str | None = None) -> None:
    """Create Cascade's database and tables on first use.

    Astro starts the Airflow metadata database before plugin code runs, but
    Cascade intentionally uses a separate Postgres database on that same
    server.  Initializing lazily keeps ``astro dev start`` self-contained while
    remaining safe when several mapped workers race on their first connection.
    """

    target = url or database_url()
    with _schema_lock:
        if target in _schema_ready:
            return
        parsed = make_url(target)
        if parsed.get_backend_name() == "postgresql" and parsed.database:
            admin_engine = create_engine(parsed.set(database="postgres"), isolation_level="AUTOCOMMIT")
            try:
                with admin_engine.connect() as connection:
                    exists = connection.execute(
                        text("SELECT 1 FROM pg_database WHERE datname = :name"),
                        {"name": parsed.database},
                    ).scalar()
                    if not exists:
                        try:
                            connection.execute(text(f'CREATE DATABASE "{parsed.database}"'))
                        except DBAPIError:
                            # Another process may have created it between the
                            # existence check and CREATE DATABASE.
                            pass
            finally:
                admin_engine.dispose()
        engine = create_engine(target, pool_pre_ping=True)
        try:
            Base.metadata.create_all(engine)
        finally:
            engine.dispose()
        _schema_ready.add(target)


def make_engine(url: str | None = None):
    target = url or database_url()
    return create_engine(target, pool_pre_ping=True)


def create_session_factory(url: str | None = None):
    return sessionmaker(bind=make_engine(url), expire_on_commit=False)


def _upsert_statement(session: Session, model: type, values: Mapping[str, Any], keys: list[str]):
    dialect = session.get_bind().dialect.name
    if dialect == "postgresql":
        statement = pg_insert(model).values(**values)
    elif dialect == "sqlite":
        statement = sqlite_insert(model).values(**values)
    else:
        # MySQL and other development dialects are not part of the supported stack.
        return None
    update_values = {key: value for key, value in values.items() if key not in keys}
    return statement.on_conflict_do_update(index_elements=keys, set_=update_values)


def upsert_account_migration(session: Session, values: Mapping[str, Any]) -> AccountMigration:
    payload = dict(values)
    payload.setdefault("zero_v1_streak_days", zero_v1_streak(payload.get("daily_v1", [])))
    statement = _upsert_statement(session, AccountMigration, payload, ["campaign_id", "account_id"])
    if statement is not None:
        session.execute(statement)
    else:
        existing = session.get(AccountMigration, (payload["campaign_id"], payload["account_id"]))
        if existing is None:
            session.add(AccountMigration(**payload))
        else:
            for key, value in payload.items():
                setattr(existing, key, value)
    session.flush()
    return session.get(AccountMigration, (payload["campaign_id"], payload["account_id"]))


def upsert_campaign(session: Session, values: Mapping[str, Any]) -> Campaign:
    payload = dict(values)
    payload.setdefault("updated_at", datetime.now(timezone.utc))
    statement = _upsert_statement(session, Campaign, payload, ["id"])
    if statement is not None:
        session.execute(statement)
    else:
        existing = session.get(Campaign, payload["id"])
        if existing is None:
            session.add(Campaign(**payload))
        else:
            for key, value in payload.items():
                setattr(existing, key, value)
    session.flush()
    return session.get(Campaign, payload["id"])


def append_timeline_event(session: Session, values: Mapping[str, Any]) -> TimelineEvent:
    event = TimelineEvent(**dict(values))
    session.add(event)
    session.flush()
    return event


def upsert_exception(session: Session, values: Mapping[str, Any]) -> ExceptionRecord:
    payload = dict(values)
    statement = _upsert_statement(session, ExceptionRecord, payload, ["id"])
    if statement is not None:
        session.execute(statement)
    else:
        existing = session.get(ExceptionRecord, payload["id"])
        if existing is None:
            session.add(ExceptionRecord(**payload))
        else:
            for key, value in payload.items():
                setattr(existing, key, value)
    session.flush()
    return session.get(ExceptionRecord, payload["id"])


def update_account_migration(
    session: Session,
    campaign_id: str,
    account_id: str,
    values: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Update an assessed account while keeping model access in this module."""

    account = session.get(AccountMigration, (campaign_id, account_id))
    if account is None:
        return None
    for key, value in values.items():
        if key in {"campaign_id", "account_id"}:
            continue
        setattr(account, key, value)
    session.flush()
    return _account_dict(account)


def get_exception(session: Session, exception_id: str) -> dict[str, Any] | None:
    row = session.get(ExceptionRecord, exception_id)
    if row is None:
        return None
    return {
        "id": row.id,
        "campaign_id": row.campaign_id,
        "account_id": row.account_id,
        "exception_type": row.exception_type,
        "airflow_dag_run_id": row.airflow_dag_run_id,
        "hitl_task_id": row.hitl_task_id,
        "status": row.status,
        "decision": row.decision,
        "decision_reason": row.decision_reason,
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
    }


def update_exception(session: Session, exception_id: str, values: Mapping[str, Any]) -> dict[str, Any] | None:
    """Update an exception record and return its API-safe representation."""

    row = session.get(ExceptionRecord, exception_id)
    if row is None:
        return None
    for key, value in values.items():
        if key == "id":
            continue
        setattr(row, key, value)
    session.flush()
    return get_exception(session, exception_id)


def get_campaign(session: Session, campaign_id: str) -> dict[str, Any] | None:
    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        return None
    return {
        "id": campaign.id,
        "name": campaign.name,
        "change_type": campaign.change_type,
        "deadline": campaign.deadline,
        "airflow_dag_run_id": campaign.airflow_dag_run_id,
        "verification_run_id": campaign.verification_run_id,
        "status": campaign.status,
        "affected_accounts": campaign.affected_accounts,
        "affected_arr": campaign.affected_arr,
        "created_at": campaign.created_at,
        "updated_at": campaign.updated_at,
    }


def _account_dict(account: AccountMigration) -> dict[str, Any]:
    return {
        "campaign_id": account.campaign_id,
        "account_id": account.account_id,
        "account_name": account.account_name,
        "arr": account.arr,
        "tier": account.tier,
        "owner": account.owner,
        "region": account.region,
        "status": account.status,
        "segment": account.segment,
        "risk": account.risk,
        "blocker_type": account.blocker_type,
        "legacy_usage": account.legacy_usage,
        "replacement_usage": account.replacement_usage,
        "zero_v1_streak_days": account.zero_v1_streak_days,
        "daily_v1": account.daily_v1 or [],
        "daily_v2": account.daily_v2 or [],
        "latest_airflow_task_instance": account.latest_airflow_task_instance,
        "brief": account.brief,
        "brief_source": account.brief_source,
        "evidence": account.evidence,
        "updated_at": account.updated_at.isoformat() if account.updated_at else None,
    }


def get_campaign_rollup(session: Session, campaign_id: str) -> dict[str, Any] | None:
    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        return None
    rows = list(session.scalars(select(AccountMigration).where(AccountMigration.campaign_id == campaign_id)))
    status_distribution: dict[str, int] = {}
    risk_distribution: dict[str, int] = {}
    segment_distribution: dict[str, int] = {}
    for row in rows:
        status_distribution[row.status] = status_distribution.get(row.status, 0) + 1
        risk_distribution[row.risk] = risk_distribution.get(row.risk, 0) + 1
        segment_distribution[row.segment] = segment_distribution.get(row.segment, 0) + 1
    pending_exceptions = session.scalar(select(func.count()).select_from(ExceptionRecord).where(ExceptionRecord.campaign_id == campaign_id, ExceptionRecord.status == "PENDING")) or 0
    chip_counts = {
        "straightforward": sum(
            row.segment == "STANDARD" and row.status == "NOT_STARTED" and sum(row.daily_v2 or []) > 0
            for row in rows
        ),
        "actively_migrating": sum(row.status == "IN_PROGRESS" for row in rows),
        "no_progress": sum(
            row.status == "NOT_STARTED" and sum(row.daily_v1 or []) > 0 and sum(row.daily_v2 or []) == 0
            for row in rows
        ),
        "strategic": sum(row.segment == "STRATEGIC" for row in rows),
        "contractual": sum(row.segment == "CONTRACTUAL" for row in rows),
        "technical_blocker": sum(row.segment == "TECHNICAL_BLOCKER" for row in rows),
    }
    return {
        "id": campaign.id,
        "name": campaign.name,
        "change_type": campaign.change_type,
        "deadline": campaign.deadline.isoformat() if campaign.deadline else None,
        "airflow_dag_run_id": campaign.airflow_dag_run_id,
        "status": campaign.status,
        "affected_accounts": len(rows) if rows else campaign.affected_accounts,
        "affected_arr": sum(row.arr for row in rows) if rows else campaign.affected_arr,
        "migration_completion": (sum(row.status == "MIGRATED" for row in rows) / len(rows) if rows else 0),
        "status_distribution": status_distribution,
        "risk_distribution": risk_distribution,
        "segment_distribution": segment_distribution,
        "blocked_accounts": status_distribution.get("BLOCKED", 0),
        "pending_exceptions": pending_exceptions,
        "chip_counts": chip_counts,
        "updated_at": campaign.updated_at.isoformat() if campaign.updated_at else None,
    }


def query_accounts(session: Session, campaign_id: str, filters: Mapping[str, Any] | None = None) -> list[dict[str, Any]]:
    filters = filters or {}
    query = select(AccountMigration).where(AccountMigration.campaign_id == campaign_id)
    if filters.get("status"):
        query = query.where(AccountMigration.status == str(filters["status"]).upper())
    if filters.get("segment"):
        query = query.where(AccountMigration.segment == str(filters["segment"]).upper())
    if filters.get("risk"):
        query = query.where(AccountMigration.risk == str(filters["risk"]).upper())
    if filters.get("q"):
        term = f"%{filters['q']}%"
        query = query.where(AccountMigration.account_name.ilike(term) | AccountMigration.account_id.ilike(term))
    chip = filters.get("chip")
    rows = list(session.scalars(query.order_by(AccountMigration.arr.desc(), AccountMigration.account_name)))
    if chip:
        if chip == "straightforward":
            # The contract's numeric chip is the standard/not-started population
            # after excluding the separate no-progress slice (v2 is still zero).
            rows = [row for row in rows if row.segment == "STANDARD" and row.status == "NOT_STARTED" and sum(row.daily_v2 or []) > 0]
        elif chip == "actively_migrating":
            rows = [row for row in rows if row.status == "IN_PROGRESS"]
        elif chip == "no_progress":
            rows = [row for row in rows if row.status == "NOT_STARTED" and sum(row.daily_v1 or []) > 0 and sum(row.daily_v2 or []) == 0]
        elif chip in {"strategic", "contractual", "technical_blocker"}:
            segment = {"strategic": "STRATEGIC", "contractual": "CONTRACTUAL", "technical_blocker": "TECHNICAL_BLOCKER"}[chip]
            rows = [row for row in rows if row.segment == segment]
    return [_account_dict(row) for row in rows]


def get_account(session: Session, account_id: str, campaign_id: str | None = None) -> dict[str, Any] | None:
    query = select(AccountMigration).where(AccountMigration.account_id == account_id)
    if campaign_id:
        query = query.where(AccountMigration.campaign_id == campaign_id)
    account = session.scalars(query.order_by(AccountMigration.updated_at.desc())).first()
    return _account_dict(account) if account else None


def get_timeline(session: Session, campaign_id: str, account_id: str | None = None) -> list[dict[str, Any]]:
    query = select(TimelineEvent).where(TimelineEvent.campaign_id == campaign_id)
    if account_id:
        query = query.where(TimelineEvent.account_id == account_id)
    query = query.order_by(TimelineEvent.timestamp.asc(), TimelineEvent.id.asc())
    return [{
        "id": event.id,
        "campaign_id": event.campaign_id,
        "account_id": event.account_id,
        "event_type": event.event_type,
        "timestamp": event.timestamp.isoformat() if event.timestamp else None,
        "summary": event.summary,
        "source": event.source,
        "airflow_run_id": event.airflow_run_id,
        "airflow_task_id": event.airflow_task_id,
    } for event in session.scalars(query)]


def get_pending_exceptions(session: Session, campaign_id: str | None = None) -> list[dict[str, Any]]:
    query = select(ExceptionRecord).where(ExceptionRecord.status == "PENDING")
    if campaign_id:
        query = query.where(ExceptionRecord.campaign_id == campaign_id)
    return [{
        "id": row.id, "campaign_id": row.campaign_id, "account_id": row.account_id,
        "exception_type": row.exception_type, "airflow_dag_run_id": row.airflow_dag_run_id,
        "hitl_task_id": row.hitl_task_id, "status": row.status,
        "decision": row.decision, "decision_reason": row.decision_reason,
        "resolved_at": row.resolved_at.isoformat() if row.resolved_at else None,
    } for row in session.scalars(query.order_by(ExceptionRecord.id))]


def clear_all(session: Session) -> None:
    session.execute(delete(TimelineEvent))
    session.execute(delete(ExceptionRecord))
    session.execute(delete(AccountMigration))
    session.execute(delete(Campaign))
    session.commit()


def database_counts(session: Session) -> dict[str, int]:
    """Return row counts used by reset verification."""

    return {
        "campaign": int(session.scalar(select(func.count()).select_from(Campaign)) or 0),
        "account_migration": int(session.scalar(select(func.count()).select_from(AccountMigration)) or 0),
        "exception": int(session.scalar(select(func.count()).select_from(ExceptionRecord)) or 0),
        "timeline_event": int(session.scalar(select(func.count()).select_from(TimelineEvent)) or 0),
    }


def aggregate_campaign_projection(session: Session, campaign_id: str) -> dict[str, Any]:
    """Recompute and persist a campaign projection using only store internals."""

    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        raise ValueError(f"Campaign {campaign_id!r} does not exist")
    counts = session.execute(
        select(func.count(AccountMigration.account_id), func.coalesce(func.sum(AccountMigration.arr), 0))
        .where(AccountMigration.campaign_id == campaign_id)
    ).one()
    status = session.execute(
        select(AccountMigration.status, func.count())
        .where(AccountMigration.campaign_id == campaign_id)
        .group_by(AccountMigration.status)
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
        "verification_run_id": campaign.verification_run_id,
        "status": next_status,
        "affected_accounts": total,
        "affected_arr": float(counts[1] or 0),
        "updated_at": datetime.now(timezone.utc),
    })
    session.commit()
    return get_campaign_rollup(session, campaign_id) or {}
