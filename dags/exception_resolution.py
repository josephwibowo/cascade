from __future__ import annotations

from datetime import datetime, timezone

from airflow.decorators import dag, task
from airflow.models import Param
from airflow.providers.standard.operators.hitl import HITLOperator

from cascade.store import append_timeline_event, create_session_factory, get_account, get_pending_exceptions, upsert_exception


@dag(
    dag_id="exception_resolution",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"retries": 1},
    tags=["cascade", "human-review"],
)
def exception_resolution():
    @task
    def load_account(**context) -> dict:
        account_id = (context.get("dag_run").conf or {}).get("account_id")
        if not account_id:
            raise ValueError("exception_resolution requires conf.account_id")
        session = create_session_factory()()
        try:
            account = get_account(session, account_id)
            if not account:
                raise ValueError(f"Unknown account {account_id}")
            return account
        finally:
            session.close()

    @task
    def build_review_packet(account: dict) -> dict:
        return {
            **account,
            "summary": f"{account['account_name']} has ${account['arr']:,.0f} ARR, {account['legacy_usage']} v1 calls, {account['replacement_usage']} v2 calls, and blocker {account.get('blocker_type') or 'none'}.",
            "options": ["Grant extension to November 15", "Keep October 31 deadline", "Escalate for legal review"],
        }

    decide = HITLOperator(
        task_id="await_decision",
        subject="Migration exception: {{ ti.xcom_pull(task_ids='build_review_packet')['account_name'] }}",
        body="{{ ti.xcom_pull(task_ids='build_review_packet')['summary'] }}",
        options=["Grant extension to November 15", "Keep October 31 deadline", "Escalate for legal review"],
        # Airflow validates task params while a task starts, before the HITL form is
        # displayed. Keep a non-empty default so the operator can enter a rationale
        # in the response form without making the DAG run itself invalid.
        params={
            "reason": Param(
                "Operator review requested",
                type="string",
                title="Reason for decision",
                minLength=1,
            )
        },
    )

    @task
    def apply_decision(account: dict, **context) -> dict:
        result = context["ti"].xcom_pull(task_ids="await_decision") or {}
        chosen = result.get("chosen_options", [])
        params = result.get("params_input", {})
        if not chosen:
            raise ValueError("HITL completed without a chosen option")
        session = create_session_factory()()
        try:
            exception_id = f"exception-{account['account_id']}"
            upsert_exception(session, {
                "id": exception_id, "campaign_id": account["campaign_id"], "account_id": account["account_id"],
                "exception_type": "TECHNICAL_BLOCKER", "status": "RESOLVED", "decision": chosen[0],
                "decision_reason": params.get("reason", ""), "resolved_at": datetime.now(timezone.utc),
                "airflow_dag_run_id": context["dag_run"].run_id, "hitl_task_id": "await_decision",
            })
            append_timeline_event(session, {
                "campaign_id": account["campaign_id"],
                "account_id": account["account_id"],
                "event_type": "EXTENSION_GRANTED" if chosen[0].startswith("Grant") else "EXCEPTION_DECISION",
                "summary": f"{chosen[0]}: {params.get('reason', '')}",
                "source": "exception_resolution",
                "airflow_run_id": context["dag_run"].run_id,
                "airflow_task_id": context["ti"].task_id,
            })
            session.commit()
            return {"account": account, "decision": chosen[0], "reason": params.get("reason", "")}
        finally:
            session.close()

    @task
    def write_timeline_event(decision: dict, **context) -> None:
        account = decision["account"]
        session = create_session_factory()()
        try:
            append_timeline_event(session, {
                "campaign_id": account["campaign_id"], "account_id": account["account_id"], "event_type": "EXCEPTION_RESOLVED",
                "summary": f"{decision['decision']}: {decision['reason']}", "source": "exception_resolution",
                "airflow_run_id": context["dag_run"].run_id, "airflow_task_id": context["ti"].task_id,
            })
            session.commit()
        finally:
            session.close()

    account = load_account()
    packet = build_review_packet(account)
    packet >> decide
    decision = apply_decision(account)
    decide >> decision
    decision >> write_timeline_event(decision)


exception_resolution()
