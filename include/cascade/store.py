"""The sole database access layer for Cascade product state."""

from __future__ import annotations

import os
import threading
from collections.abc import Mapping
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import and_, case, create_engine, delete, func, or_, select, text, update
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
    if "daily_v1" in payload and "legacy_usage" not in payload:
        payload["legacy_usage"] = sum(payload.get("daily_v1") or [])
    if "daily_v2" in payload and "replacement_usage" not in payload:
        payload["replacement_usage"] = sum(payload.get("daily_v2") or [])
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


def _account_row_dict(account: AccountMigration) -> dict[str, Any]:
    """Return the fields needed to render one row in the account table."""

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
    }


CHIP_IDS: tuple[str, ...] = (
    "straightforward",
    "actively_migrating",
    "no_progress",
    "strategic",
    "contractual",
    "technical_blocker",
)


def _chip_predicates() -> dict[str, Any]:
    """SQL equivalents of the blast-radius chip predicates."""

    return {
        "straightforward": and_(
            AccountMigration.segment == "STANDARD",
            AccountMigration.status == "NOT_STARTED",
            AccountMigration.replacement_usage > 0,
        ),
        "actively_migrating": AccountMigration.status == "IN_PROGRESS",
        "no_progress": and_(
            AccountMigration.status == "NOT_STARTED",
            AccountMigration.legacy_usage > 0,
            AccountMigration.replacement_usage == 0,
        ),
        "strategic": AccountMigration.segment == "STRATEGIC",
        "contractual": AccountMigration.segment == "CONTRACTUAL",
        "technical_blocker": AccountMigration.segment == "TECHNICAL_BLOCKER",
    }


def _filter_clauses(campaign_id: str, filters: Mapping[str, Any] | None = None) -> list[Any]:
    filters = filters or {}
    clauses: list[Any] = [AccountMigration.campaign_id == campaign_id]
    if filters.get("status"):
        clauses.append(AccountMigration.status == str(filters["status"]).upper())
    if filters.get("segment"):
        clauses.append(AccountMigration.segment == str(filters["segment"]).upper())
    if filters.get("risk"):
        clauses.append(AccountMigration.risk == str(filters["risk"]).upper())
    if filters.get("q"):
        term = f"%{filters['q']}%"
        clauses.append(or_(AccountMigration.account_name.ilike(term), AccountMigration.account_id.ilike(term)))
    chip = filters.get("chip")
    predicate = _chip_predicates().get(chip) if chip else None
    if predicate is not None:
        clauses.append(predicate)
    return clauses


def _chip_counts(session: Session, clauses: list[Any]) -> dict[str, int]:
    predicates = _chip_predicates()
    values = [
        func.coalesce(func.sum(case((predicates[chip], 1), else_=0)), 0).label(chip)
        for chip in CHIP_IDS
    ]
    row = session.execute(select(*values).where(*clauses)).one()
    return {chip: int(getattr(row, chip) or 0) for chip in CHIP_IDS}


def get_campaign_rollup(session: Session, campaign_id: str) -> dict[str, Any] | None:
    campaign = session.get(Campaign, campaign_id)
    if campaign is None:
        return None
    campaign_clause = AccountMigration.campaign_id == campaign_id
    counts = session.execute(
        select(
            func.count(AccountMigration.account_id),
            func.coalesce(func.sum(AccountMigration.arr), 0.0),
        ).where(campaign_clause)
    ).one()
    affected_accounts = int(counts[0] or 0)
    affected_arr = counts[1] or 0.0

    def distribution(column: Any) -> dict[str, int]:
        return {
            value: int(count)
            for value, count in session.execute(
                select(column, func.count()).where(campaign_clause).group_by(column)
            ).all()
        }

    status_distribution = distribution(AccountMigration.status)
    risk_distribution = distribution(AccountMigration.risk)
    segment_distribution = distribution(AccountMigration.segment)
    pending_exceptions = session.scalar(select(func.count()).select_from(ExceptionRecord).where(ExceptionRecord.campaign_id == campaign_id, ExceptionRecord.status == "PENDING")) or 0
    chip_counts = _chip_counts(session, [campaign_clause])
    total = affected_accounts
    migrated = status_distribution.get("MIGRATED", 0)
    return {
        "id": campaign.id,
        "name": campaign.name,
        "change_type": campaign.change_type,
        "deadline": campaign.deadline.isoformat() if campaign.deadline else None,
        "airflow_dag_run_id": campaign.airflow_dag_run_id,
        "verification_run_id": campaign.verification_run_id,
        "status": campaign.status,
        "affected_accounts": affected_accounts if affected_accounts else campaign.affected_accounts,
        "affected_arr": affected_arr if affected_accounts else campaign.affected_arr,
        "migration_completion": migrated / total if total else 0,
        "status_distribution": status_distribution,
        "risk_distribution": risk_distribution,
        "segment_distribution": segment_distribution,
        "blocked_accounts": status_distribution.get("BLOCKED", 0),
        "pending_exceptions": pending_exceptions,
        "chip_counts": chip_counts,
        "updated_at": campaign.updated_at.isoformat() if campaign.updated_at else None,
    }


def query_accounts(
    session: Session,
    campaign_id: str,
    filters: Mapping[str, Any] | None = None,
    *,
    limit: int = 200,
    offset: int = 0,
) -> dict[str, Any]:
    limit = max(1, min(500, int(limit)))
    offset = max(0, int(offset))
    clauses = _filter_clauses(campaign_id, filters)
    base = select(AccountMigration).where(*clauses)
    total = int(session.scalar(select(func.count()).select_from(base.subquery())) or 0)
    rows = session.scalars(
        base.order_by(AccountMigration.arr.desc(), AccountMigration.account_name)
        .limit(limit)
        .offset(offset)
    )
    return {
        "items": [_account_row_dict(row) for row in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


def chip_facets(session: Session, campaign_id: str, filters: Mapping[str, Any] | None = None) -> dict[str, int]:
    """Count each chip while applying all non-chip filters."""

    filters = dict(filters or {})
    filters.pop("chip", None)
    return _chip_counts(session, _filter_clauses(campaign_id, filters))


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
    query = (
        select(
            ExceptionRecord,
            AccountMigration.account_name,
            AccountMigration.arr,
            AccountMigration.status,
            AccountMigration.risk,
            AccountMigration.blocker_type,
        )
        .outerjoin(
            AccountMigration,
            and_(
                AccountMigration.campaign_id == ExceptionRecord.campaign_id,
                AccountMigration.account_id == ExceptionRecord.account_id,
            ),
        )
        .where(ExceptionRecord.status == "PENDING")
    )
    if campaign_id:
        query = query.where(ExceptionRecord.campaign_id == campaign_id)
    return [
        {
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
            "account_name": account_name,
            "arr": arr,
            "account_status": account_status,
            "risk": risk,
            "blocker_type": blocker_type,
        }
        for row, account_name, arr, account_status, risk, blocker_type in session.execute(query.order_by(ExceptionRecord.id))
    ]


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
