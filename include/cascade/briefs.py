from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class MigrationBrief(BaseModel):
    account_id: str
    summary: str
    blockers: list[str] = Field(default_factory=list)
    proposed_next_step: str
    evidence_refs: list[str] = Field(default_factory=list)


def deterministic_brief(evidence: dict[str, Any]) -> MigrationBrief:
    blocker = evidence.get("blocker_type") or "none identified"
    return MigrationBrief(
        account_id=evidence["account_id"],
        summary=f"{evidence['account_name']} has {evidence['legacy_usage']} legacy calls in the trailing window and {evidence['replacement_usage']} replacement calls.",
        blockers=[str(blocker)],
        proposed_next_step="Coordinate the next migration checkpoint with the account owner.",
        evidence_refs=["daily_v1", "daily_v2", "account_contract"],
    )
