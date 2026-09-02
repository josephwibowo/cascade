from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable

from cascade.aggregate import aggregate_campaign
from cascade.mock_client import MockSystemsClient
from cascade.rules import compute_risk, compute_status, zero_v1_streak
from cascade.store import append_timeline_event, create_session_factory, get_account, upsert_account_migration


@dag(
    dag_id="migration_verification",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cascade", "verification"],
)
def migration_verification():
    @task
    def load_telemetry_watermark() -> str:
        return Variable.get("cascade_last_snapshot", default_var="day0")

    @task
    def find_accounts_with_changed_usage(watermark: str) -> list[str]:
        client = MockSystemsClient()
        current = client.scenario()["snapshot"]
        before = {row["account_id"]: row for row in client.usage_accounts(watermark)}
        after = {row["account_id"]: row for row in client.usage_accounts(current)}
        changed = [account_id for account_id in after if account_id not in before or (after[account_id].get("daily_v1"), after[account_id].get("daily_v2")) != (before[account_id].get("daily_v1"), before[account_id].get("daily_v2"))]
        return changed

    @task
    def verify_account(account_id: str, watermark: str) -> dict:
        from airflow.operators.python import get_current_context
        context = get_current_context()
        client = MockSystemsClient()
        current_snapshot = client.scenario()["snapshot"]
        usage = client.usage_account(account_id, current_snapshot)
        session = create_session_factory()()
        try:
            existing = get_account(session, account_id)
            if existing is None:
                raise ValueError(f"No assessed account row for {account_id}")
            blocker = existing.get("blocker_type")
            status = compute_status(usage["daily_v1"], usage["daily_v2"], blocker, usage.get("prior_v1_7d"), usage.get("never_needed_replacement", False))
            risk = compute_risk(existing["arr"], status)
            ti = context["ti"]
            upsert_account_migration(session, {
                **{key: existing[key] for key in ("campaign_id", "account_id", "account_name", "arr", "tier", "owner", "region", "segment")},
                "status": status.value, "risk": risk.value, "blocker_type": blocker,
                "legacy_usage": sum(usage["daily_v1"]), "replacement_usage": sum(usage["daily_v2"]),
                "zero_v1_streak_days": zero_v1_streak(usage["daily_v1"]), "daily_v1": usage["daily_v1"], "daily_v2": usage["daily_v2"],
                "latest_airflow_task_instance": {"dag_id": ti.dag_id, "run_id": ti.run_id, "task_id": ti.task_id, "map_index": ti.map_index},
                "brief": existing.get("brief"), "brief_source": existing.get("brief_source"), "evidence": existing.get("evidence"),
            })
            append_timeline_event(session, {"campaign_id": existing["campaign_id"], "account_id": account_id, "event_type": "TELEMETRY_VERIFIED", "summary": f"Verification telemetry classified the account as {status.value}.", "source": "migration_verification", "airflow_run_id": ti.run_id, "airflow_task_id": ti.task_id})
            session.commit()
            return {"account_id": account_id, "status": status.value}
        finally:
            session.close()

    @task
    def save_telemetry_watermark() -> None:
        Variable.set("cascade_last_snapshot", MockSystemsClient().scenario()["snapshot"])

    @task
    def rollup() -> dict:
        session = create_session_factory()()
        try:
            return aggregate_campaign(session, "api_v1_sunset")
        finally:
            session.close()

    watermark = load_telemetry_watermark()
    changed = find_accounts_with_changed_usage(watermark)
    verified = verify_account.partial(watermark=watermark).expand(account_id=changed)
    saved = save_telemetry_watermark()
    summary = rollup()
    changed >> verified >> saved >> summary


migration_verification()
