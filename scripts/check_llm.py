#!/usr/bin/env python3
"""Make one real model call through the connection the assessment DAG uses.

Airflow's connection test for a ``pydanticai`` connection only resolves the
model string. It never calls the provider, so a rejected key, an unknown model
slug, or a model that cannot return a ``MigrationBrief`` all pass it. The DAG
then hides the failure: ``persist_migration_brief`` catches it and stores a
deterministic brief instead. This script fails loudly, before a 36-minute hero
run is spent finding out.

It follows the path ``@task.llm`` takes: ``PydanticAIHook.get_hook`` on the
``cascade_llm`` connection, ``create_agent`` with the DAG's output type and
system prompt, then ``run_sync``. The prompt is built
from Acme Logistics' real evidence in the mock systems, in the same shape
``generate_migration_brief`` sends. One call costs a fraction of a cent on the
demo model.

Run it inside the Airflow container after ``astro dev start``::

    printf 'python scripts/check_llm.py\\n' | astro dev bash -s
"""

from __future__ import annotations

import sys
from pathlib import Path

from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook
from pydantic_ai import NativeOutput

# Airflow puts include/ on the path itself inside the container. Append rather
# than insert, so this fallback can never shadow the plugin's `cascade` package.
sys.path.append(str(Path(__file__).resolve().parents[1] / "include"))

from cascade.briefs import MigrationBrief  # noqa: E402
from cascade.mock_client import MockSystemsClient  # noqa: E402

CONN_ID = "cascade_llm"
ACCOUNT_ID = "acme_logistics"
# Kept identical to the @task.llm declaration in dags/product_change_assessment.py.
SYSTEM_PROMPT = "You explain migration evidence. You never decide migration status."


def build_prompt(evidence: dict) -> str:
    # Kept identical to generate_migration_brief in dags/product_change_assessment.py.
    return (
        f"Account {evidence['account_name']} has {evidence['legacy_usage']} legacy calls "
        f"and {evidence['replacement_usage']} replacement calls. "
        f"Signals: {evidence.get('signals', {})}. Explain the evidence and propose "
        "the next migration step without deciding status."
    )


def main() -> int:
    step = f"reading {ACCOUNT_ID}'s evidence from the mock systems"
    try:
        client = MockSystemsClient()
        usage = client.usage_account(ACCOUNT_ID, client.scenario()["snapshot"])
        crm = client.crm_account(ACCOUNT_ID)
        evidence = {
            "account_name": crm["account_name"],
            "legacy_usage": sum(usage["daily_v1"]),
            "replacement_usage": sum(usage["daily_v2"]),
            "signals": usage,
        }

        step = f"resolving the model on connection {CONN_ID!r}"
        hook = PydanticAIHook.get_hook(CONN_ID, hook_params={"model_id": None})
        model = hook.get_connection(CONN_ID).extra_dejson.get("model")
        # Kept identical to the DAG's output_type; its comment explains why it is native.
        agent = hook.create_agent(output_type=NativeOutput(MigrationBrief), instructions=SYSTEM_PROMPT)

        step = f"calling the provider for {model!r}"
        result = agent.run_sync(build_prompt(evidence))

        step = "validating the reply as a MigrationBrief"
        brief = MigrationBrief.model_validate(result.output)
    except Exception:
        # Name the stage, then re-raise so the provider's own error survives intact.
        print(f"\nFAILED while {step}. Briefs would degrade to deterministic.", file=sys.stderr)
        raise

    # pydantic-ai 2.37 exposes usage as a property; the callable check keeps
    # method-style releases working too.
    usage = getattr(result, "usage", None)
    usage_report = usage() if callable(usage) else usage
    print(f"\nOK: {model} returned a valid MigrationBrief for {evidence['account_name']}.")
    print(f"usage: {usage_report}")
    print(brief.model_dump_json(indent=2))
    print("\nBriefs from the hero run will persist as brief_source=llm.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
