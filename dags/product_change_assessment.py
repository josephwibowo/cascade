from __future__ import annotations

from datetime import datetime

try:
    # The provider-compatible TaskFlow collection adds ``task.llm`` while
    # retaining the normal ``@task`` API used by the rest of this DAG.
    from airflow.providers.common.compat.sdk import dag, task
except ImportError:  # Airflow 2 parser tooling fallback.
    from airflow.decorators import dag, task
try:
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2 compatibility for local parser tooling.
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule

from cascade.aggregate import aggregate_campaign
from cascade.briefs import MigrationBrief, deterministic_brief
from cascade.mock_client import MockSystemsClient
from cascade.rules import compute_risk, compute_status, zero_v1_streak
from cascade.states import Segment
from cascade.store import (
    append_timeline_event,
    create_session_factory,
    ensure_schema,
    get_account,
    update_account_migration,
    update_exception,
    upsert_account_migration,
    upsert_campaign,
    upsert_exception,
)


@dag(
    dag_id="product_change_assessment",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    # Keep the local demo API responsive while the 2,417-account mapping is
    # running.  Mapping remains real (one task instance per account), but the
    # bounded fan-out avoids overwhelming a laptop-sized Airflow stack.
    max_active_tasks=16,
    tags=["cascade", "assessment"],
    default_args={"retries": 1},
)
def product_change_assessment():
    @task
    def load_change() -> dict:
        from airflow.operators.python import get_current_context

        run_id = get_current_context()["dag_run"].run_id
        change = MockSystemsClient().migration_change()
        ensure_schema()
        session = create_session_factory()()
        try:
            from datetime import date
            upsert_campaign(session, {
                "id": change["campaign_id"],
                "name": change["name"],
                "change_type": change["change_type"],
                "deadline": date.fromisoformat(change["deadline"]),
                "status": "IN_PROGRESS",
                "affected_accounts": 0,
                "affected_arr": 0,
                "airflow_dag_run_id": run_id,
            })
            session.commit()
        finally:
            session.close()
        change["snapshot"] = MockSystemsClient().scenario()["snapshot"]
        return change

    @task
    def discover_affected_accounts(change: dict) -> list[str]:
        # This list is deliberately produced by a runtime HTTP response; the
        # mapped task count is never inferred from a config constant.
        usage = MockSystemsClient().usage_accounts(change["snapshot"])
        return [row["account_id"] for row in usage if row.get("daily_v1") or row.get("daily_v2")]

    @task(retries=1, multiple_outputs=False)
    def assess_account(account_id: str, change: dict) -> dict:
        from airflow.operators.python import get_current_context

        context = get_current_context()
        client = MockSystemsClient()
        usage = client.usage_account(account_id, change["snapshot"])
        crm = client.crm_account(account_id)
        contract = client.contract_account(account_id)
        segment = _segment_for(contract, account_id)
        blocker = _blocker_for(segment, usage, contract)
        status = compute_status(usage["daily_v1"], usage["daily_v2"], blocker, usage.get("prior_v1_7d"), usage.get("never_needed_replacement", False))
        risk = compute_risk(crm["arr"], status)
        task_instance = context["ti"]
        latest = {"dag_id": task_instance.dag_id, "run_id": task_instance.run_id, "task_id": task_instance.task_id, "map_index": task_instance.map_index}
        session = create_session_factory()()
        try:
            upsert_account_migration(session, {
                "campaign_id": change["campaign_id"], "account_id": account_id,
                "account_name": crm["account_name"], "arr": crm["arr"], "tier": crm["tier"], "owner": crm["csm"], "region": crm["region"],
                "status": status.value, "segment": segment.value, "risk": risk.value,
                "blocker_type": blocker.value if blocker else None,
                "legacy_usage": sum(usage["daily_v1"]), "replacement_usage": sum(usage["daily_v2"]),
                "zero_v1_streak_days": zero_v1_streak(usage["daily_v1"]), "daily_v1": usage["daily_v1"], "daily_v2": usage["daily_v2"],
                "latest_airflow_task_instance": latest,
                "evidence": {"endpoints": usage.get("endpoints", []), "sdk_name": usage.get("sdk_name"), "sdk_version": usage.get("sdk_version"), "compatibility_commitment": contract.get("compatibility_commitment", False), "commitment_expiry": contract.get("commitment_expiry")},
            })
            if blocker:
                exception_id = f"exception-{account_id}"
                upsert_exception(session, {
                    "id": exception_id, "campaign_id": change["campaign_id"], "account_id": account_id,
                    "exception_type": "TECHNICAL_BLOCKER" if segment.value == "TECHNICAL_BLOCKER" else "CONTRACTUAL_EXCEPTION",
                    "status": "PENDING", "hitl_task_id": None,
                })
            append_timeline_event(session, {"campaign_id": change["campaign_id"], "account_id": account_id, "event_type": "ASSESSMENT", "summary": f"Assessment classified {crm['account_name']} as {status.value}.", "source": "product_change_assessment", "airflow_run_id": task_instance.run_id, "airflow_task_id": task_instance.task_id})
            session.commit()
        finally:
            session.close()
        return {"account_id": account_id, "account_name": crm["account_name"], "arr": crm["arr"], "segment": segment.value, "status": status.value, "risk": risk.value, "blocker_type": blocker.value if blocker else None, "legacy_usage": sum(usage["daily_v1"]), "replacement_usage": sum(usage["daily_v2"]), "signals": usage}

    @task
    def select_high_risk(assessments: list[dict]) -> list[dict]:
        return sorted((item for item in assessments if item["segment"] != Segment.STANDARD.value), key=lambda item: item["arr"], reverse=True)[:8]

    @task.llm(
        task_id="generate_migration_brief",
        llm_conn_id="cascade_llm",
        output_type=MigrationBrief,
        system_prompt="You explain migration evidence. You never decide migration status.",
        retries=0,
        multiple_outputs=False,
        serialize_output=True,
    )
    def generate_migration_brief(evidence: dict) -> str:
        """Return a prompt; Common AI performs the structured model call."""

        return (
            f"Account {evidence['account_name']} has {evidence['legacy_usage']} legacy calls "
            f"and {evidence['replacement_usage']} replacement calls. "
            f"Signals: {evidence.get('signals', {})}. Explain the evidence and propose "
            "the next migration step without deciding status."
        )

    @task(trigger_rule=TriggerRule.ALL_DONE, multiple_outputs=False)
    def persist_migration_brief(evidence: dict) -> dict:
        """Persist the model output, degrading per account when it fails."""

        from airflow.operators.python import get_current_context

        context = get_current_context()
        ti = context["ti"]
        model_output = ti.xcom_pull(
            task_ids="generate_migration_brief", map_indexes=ti.map_index
        )
        try:
            brief = MigrationBrief.model_validate(model_output)
            brief_source = "llm"
        except Exception:
            # Missing connection, provider errors, and malformed model output
            # must never affect status or rollup correctness.
            brief = deterministic_brief(evidence)
            brief_source = "deterministic"
        session = create_session_factory()()
        try:
            campaign_id = evidence.get("campaign_id", "api_v1_sunset")
            account = get_account(session, evidence["account_id"], campaign_id)
            if account:
                update_account_migration(
                    session,
                    campaign_id,
                    evidence["account_id"],
                    {"brief": brief.model_dump(), "brief_source": brief_source},
                )
                session.commit()
        finally:
            session.close()
        return {"account_id": evidence["account_id"], "brief": brief.model_dump(), "brief_source": brief_source}

    @task(task_id="aggregate_campaign", trigger_rule=TriggerRule.ALL_DONE)
    def aggregate(campaign_id: str) -> dict:
        session = create_session_factory()()
        try:
            return aggregate_campaign(session, campaign_id)
        finally:
            session.close()

    @task
    def register_exception_run() -> None:
        from airflow.operators.python import get_current_context
        context = get_current_context()
        ti = context["ti"]
        run_id = ti.xcom_pull(task_ids="trigger_acme_exception", key="trigger_run_id")
        if not run_id:
            return
        session = create_session_factory()()
        try:
            update_exception(
                session,
                "exception-acme_logistics",
                {"airflow_dag_run_id": run_id, "hitl_task_id": "await_decision"},
            )
            session.commit()
        finally:
            session.close()

    change = load_change()
    accounts = discover_affected_accounts(change)
    assessments = assess_account.partial(change=change).expand(account_id=accounts)
    high_risk = select_high_risk(assessments)
    llm_briefs = generate_migration_brief.partial().expand(evidence=high_risk)
    briefs = persist_migration_brief.partial().expand(evidence=high_risk)
    rollup = aggregate(change["campaign_id"])
    trigger = TriggerDagRunOperator(task_id="trigger_acme_exception", trigger_dag_id="exception_resolution", conf={"account_id": "acme_logistics"}, wait_for_completion=False, do_xcom_push=True)
    register = register_exception_run()
    change >> accounts >> assessments >> high_risk >> llm_briefs >> briefs >> rollup
    rollup >> trigger >> register


def _segment_for(contract: dict, account_id: str) -> Segment:
    # Segment is seeded in the contract fixture. The stable fixture metadata is
    # intentionally represented by compatibility and account id at the API seam.
    return Segment(contract.get("segment", Segment.STANDARD.value))


def _blocker_for(segment: Segment, usage: dict, contract: dict):
    from cascade.states import BlockerType
    value = contract.get("blocker_type")
    return BlockerType(value) if value else None


product_change_assessment()
