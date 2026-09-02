from __future__ import annotations

from datetime import date, datetime
from typing import Any

from sqlalchemy import Date, DateTime, Float, Index, Integer, JSON, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Campaign(Base):
    __tablename__ = "campaign"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    change_type: Mapped[str] = mapped_column(String(128))
    deadline: Mapped[date] = mapped_column(Date)
    airflow_dag_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    verification_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="IN_PROGRESS")
    affected_accounts: Mapped[int] = mapped_column(Integer, default=0)
    affected_arr: Mapped[float] = mapped_column(Float, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)


class AccountMigration(Base):
    __tablename__ = "account_migration"

    campaign_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    account_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    account_name: Mapped[str] = mapped_column(String(255))
    arr: Mapped[float] = mapped_column(Float)
    tier: Mapped[str] = mapped_column(String(64))
    owner: Mapped[str] = mapped_column(String(255))
    region: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(64))
    segment: Mapped[str] = mapped_column(String(64))
    risk: Mapped[str] = mapped_column(String(64))
    blocker_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    legacy_usage: Mapped[int] = mapped_column(Integer, default=0)
    replacement_usage: Mapped[int] = mapped_column(Integer, default=0)
    zero_v1_streak_days: Mapped[int] = mapped_column(Integer, default=0)
    daily_v1: Mapped[list[int]] = mapped_column(JSON, default=list)
    daily_v2: Mapped[list[int]] = mapped_column(JSON, default=list)
    latest_airflow_task_instance: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    brief: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    brief_source: Mapped[str | None] = mapped_column(String(32), nullable=True)
    evidence: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index("ix_account_migration_campaign_status", "campaign_id", "status"),
        Index("ix_account_migration_campaign_segment", "campaign_id", "segment"),
        Index("ix_account_migration_campaign_risk", "campaign_id", "risk"),
        Index("ix_account_migration_campaign_arr", "campaign_id", "arr"),
    )


class ExceptionRecord(Base):
    __tablename__ = "exception"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    campaign_id: Mapped[str] = mapped_column(String(128), index=True)
    account_id: Mapped[str] = mapped_column(String(128), index=True)
    exception_type: Mapped[str] = mapped_column(String(128))
    airflow_dag_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    hitl_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(64), default="PENDING")
    decision: Mapped[str | None] = mapped_column(Text, nullable=True)
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class TimelineEvent(Base):
    __tablename__ = "timeline_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    campaign_id: Mapped[str] = mapped_column(String(128), index=True)
    account_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    event_type: Mapped[str] = mapped_column(String(128))
    timestamp: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    summary: Mapped[str] = mapped_column(Text)
    source: Mapped[str] = mapped_column(String(128))
    airflow_run_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    airflow_task_id: Mapped[str | None] = mapped_column(String(255), nullable=True)

    __table_args__ = (Index("ix_timeline_campaign_account_timestamp", "campaign_id", "account_id", "timestamp"),)
