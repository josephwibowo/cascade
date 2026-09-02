"""Reproducible Cascade world used by the mock systems service."""

from __future__ import annotations

import json
import random
from pathlib import Path
from typing import Any

from cascade.rules import compute_risk, compute_status
from cascade.states import BlockerType, Risk, Segment, Status

SEED = 20261031
ACCOUNT_COUNT = 2417
CAMPAIGN_ID = "api_v1_sunset"


def _series(kind: str, rng: random.Random) -> tuple[list[int], list[int], int]:
    """Create day-zero telemetry and its preceding comparison window."""
    if kind == "no_progress":
        v1 = [rng.randint(650, 1_450) for _ in range(7)]
        return v1, [0] * 7, sum(v1) + rng.randint(100, 500)
    if kind == "in_progress":
        v1 = [rng.randint(500, 1_100) for _ in range(7)]
        v2 = [rng.randint(100, 600) for _ in range(7)]
        return v1, v2, sum(v1) + rng.randint(500, 2_000)
    if kind == "straightforward":
        v1 = [rng.randint(800, 2_100) for _ in range(7)]
        v2 = [rng.randint(0, 250) for _ in range(7)]
        return v1, v2, max(0, sum(v1) - rng.randint(0, 500))
    # Blocked accounts are still using v1; a few have experimental v2 calls.
    v1 = [rng.randint(700, 2_000) for _ in range(7)]
    v2 = [rng.randint(0, 90) for _ in range(7)]
    return v1, v2, sum(v1) + rng.randint(100, 1_000)


def _account_ids() -> list[str]:
    generated = [f"acct_{index:05d}" for index in range(1, ACCOUNT_COUNT - 4 + 1)]
    return ["acme_logistics", "clean_strategic", "no_progress", "already_mostly_migrated", *generated]


def build_fixtures() -> dict[str, Any]:
    """Build all mock-system payloads without reading the clock or environment."""
    rng = random.Random(SEED)
    ids = _account_ids()
    assert len(ids) == ACCOUNT_COUNT

    segments: dict[str, Segment] = {"acme_logistics": Segment.TECHNICAL_BLOCKER}
    remaining = [account_id for account_id in ids if account_id != "acme_logistics"]
    generated = [account_id for account_id in remaining if account_id.startswith("acct_")]
    for account_id in generated[:12]:
        segments[account_id] = Segment.TECHNICAL_BLOCKER
    for account_id in generated[12:50]:
        segments[account_id] = Segment.CONTRACTUAL
    segments["clean_strategic"] = Segment.STRATEGIC
    for account_id in generated[50:123]:
        segments[account_id] = Segment.STRATEGIC
    for account_id in ids:
        segments.setdefault(account_id, Segment.STANDARD)

    blocked = {account_id for account_id, segment in segments.items() if segment in (Segment.CONTRACTUAL, Segment.TECHNICAL_BLOCKER)}
    strategic = {account_id for account_id, segment in segments.items() if segment is Segment.STRATEGIC}
    standard = set(ids) - blocked - strategic
    no_progress = {"no_progress"}
    no_progress.update(sorted(standard - no_progress)[:186])
    standard_remaining = sorted(standard - no_progress)
    in_progress = set(standard_remaining[:421]) | strategic
    straightforward = standard - no_progress - set(standard_remaining[:421])
    assert len(straightforward) == 1_684

    usage_by_snapshot: dict[str, list[dict[str, Any]]] = {"day0": [], "day7": []}
    crm: list[dict[str, Any]] = []
    contracts: list[dict[str, Any]] = []
    day7_migrated = {"acme_logistics", *sorted(straightforward)[:30]}
    day7_ready = set(sorted(straightforward)[30:70])
    day7_in_progress = set(sorted(no_progress)[0:13])

    for position, account_id in enumerate(ids, start=1):
        segment = segments[account_id]
        if account_id == "acme_logistics":
            arr, name, tier, csm, region = 2_400_000, "Acme Logistics", "Enterprise", "Priya Shah", "North America"
            v1 = [14_100, 14_240, 13_980, 14_180, 14_060, 14_220, 14_000]
            v2 = [0] * 7
            prior = sum(v1) + 5_000
            endpoints = ["/v1/events", "/v1/events/batch"]
            sdk_name, sdk_version = "beacon-python", "2.4.1"
            blocker = BlockerType.CUSTOM_PARSER
        else:
            arr = rng.randint(30_000, 1_750_000)
            name = {
                "clean_strategic": "Northstar Retail",
                "no_progress": "Harbor Health",
                "already_mostly_migrated": "Juniper Labs",
            }.get(account_id, f"Account {position:05d}")
            tier = "Enterprise" if arr >= 1_000_000 else "Growth" if arr >= 250_000 else "Scale"
            csm = f"CSM {((position - 1) % 17) + 1:02d}"
            region = ("North America", "EMEA", "APAC", "LATAM")[(position - 1) % 4]
            kind = "no_progress" if account_id in no_progress else "in_progress" if account_id in in_progress else "straightforward"
            v1, v2, prior = _series(kind, rng)
            if account_id in blocked:
                blocker_values = [BlockerType.CUSTOM_PARSER, BlockerType.SDK_PINNED, BlockerType.CONTRACT_COMMITMENT, BlockerType.NO_OWNER]
                blocker = blocker_values[(position - 1) % len(blocker_values)]
            else:
                blocker = None
            endpoints = ["/v1/events"] if account_id in no_progress else ["/v1/events", "/v1/events/batch"]
            sdk_name, sdk_version = "beacon-python", "2.4.1"

        day0 = {
            "account_id": account_id,
            "snapshot_id": "day0",
            "daily_v1": v1,
            "daily_v2": v2,
            "prior_v1_7d": prior,
            "endpoints": endpoints,
            "sdk_name": sdk_name,
            "sdk_version": sdk_version,
            "never_needed_replacement": False,
        }
        day7 = dict(day0)
        day7["snapshot_id"] = "day7"
        if account_id in day7_migrated:
            day7["daily_v1"] = [0] * 7
            day7["daily_v2"] = [rng.randint(850, 1_700) for _ in range(7)]
        elif account_id in day7_ready:
            day7["daily_v1"] = [max(1, value // 2) for value in v1[:6]] + [0]
            day7["daily_v2"] = list(v2)
        elif account_id in day7_in_progress:
            day7["daily_v1"] = [max(1, value // 2) for value in v1]
            day7["daily_v2"] = [value + 80 for value in v2]
        usage_by_snapshot["day0"].append(day0)
        usage_by_snapshot["day7"].append(day7)

        crm.append({"account_id": account_id, "account_name": name, "arr": arr, "tier": tier, "csm": csm, "region": region})
        contracts.append({
            "account_id": account_id,
            "segment": segment.value,
            "blocker_type": blocker.value if blocker else None,
            "compatibility_commitment": account_id == "acme_logistics" or segment is Segment.CONTRACTUAL,
            "commitment_expiry": "2026-10-31" if account_id == "acme_logistics" or segment is Segment.CONTRACTUAL else None,
        })

    usage_by_id = {snapshot: {row["account_id"]: row for row in rows} for snapshot, rows in usage_by_snapshot.items()}
    computed: dict[str, dict[str, Status]] = {snapshot: {} for snapshot in usage_by_snapshot}
    for snapshot, rows in usage_by_snapshot.items():
        for row in rows:
            computed[snapshot][row["account_id"]] = compute_status(row["daily_v1"], row["daily_v2"], _blocker_for(row["account_id"], segments), row["prior_v1_7d"], row["never_needed_replacement"])
    return {
        "day0_usage": usage_by_snapshot["day0"],
        "day7_usage": usage_by_snapshot["day7"],
        "crm": crm,
        "contracts": contracts,
        "api_v1_sunset": {
            "campaign_id": CAMPAIGN_ID,
            "name": "API v1 Sunset",
            "change_type": "API_DEPRECATION",
            "deadline": "2026-10-31",
            "legacy_endpoints": ["/v1/events", "/v1/events/batch"],
            "replacement_endpoints": ["/v2/events", "/v2/events/batch"],
        },
        "_computed": computed,
        "_segments": segments,
        "_blockers": {account_id: _blocker_for(account_id, segments) for account_id in ids},
        "_usage_by_id": usage_by_id,
    }


def _blocker_for(account_id: str, segments: dict[str, Segment]) -> BlockerType | None:
    if segments[account_id] not in (Segment.CONTRACTUAL, Segment.TECHNICAL_BLOCKER):
        return None
    if account_id == "acme_logistics":
        return BlockerType.CUSTOM_PARSER
    return list(BlockerType)[sum(ord(char) for char in account_id) % len(BlockerType)]


def public_fixtures() -> dict[str, Any]:
    """Return JSON-safe fixture payloads (without generator-only metadata)."""
    fixtures = build_fixtures()
    return {key: value for key, value in fixtures.items() if not key.startswith("_")}


def assert_fixture_integrity(fixtures: dict[str, Any] | None = None) -> None:
    fixtures = fixtures or build_fixtures()
    segments = fixtures["_segments"]
    blockers = fixtures["_blockers"]
    expected_status = {Status.NOT_STARTED: 1_871, Status.IN_PROGRESS: 495, Status.BLOCKED: 51, Status.READY_TO_VERIFY: 0, Status.MIGRATED: 0}
    expected_segments = {Segment.STANDARD: 2_292, Segment.STRATEGIC: 74, Segment.CONTRACTUAL: 38, Segment.TECHNICAL_BLOCKER: 13}
    for snapshot in ("day0", "day7"):
        statuses = fixtures["_computed"][snapshot]
        if snapshot == "day0":
            assert {status: list(statuses.values()).count(status) for status in Status} == expected_status
            blocked_status = {account_id for account_id, status in statuses.items() if status is Status.BLOCKED}
            blocked_segments = {account_id for account_id, segment in segments.items() if segment in (Segment.CONTRACTUAL, Segment.TECHNICAL_BLOCKER)}
            assert blocked_status == blocked_segments
    assert {segment: list(segments.values()).count(segment) for segment in Segment} == expected_segments
    assert sum(1 for value in blockers.values() if value is not None) == 51
    day0 = fixtures["_usage_by_id"]["day0"]
    day7 = fixtures["_usage_by_id"]["day7"]
    changed = [account_id for account_id in day0 if (day0[account_id]["daily_v1"], day0[account_id]["daily_v2"]) != (day7[account_id]["daily_v1"], day7[account_id]["daily_v2"])]
    assert len(changed) == 84
    assert fixtures["_computed"]["day7"]["acme_logistics"] is Status.MIGRATED
    straightforward = sum(
        segment is Segment.STANDARD
        and fixtures["_computed"]["day0"][account_id] is Status.NOT_STARTED
        and sum(day0[account_id]["daily_v2"]) > 0
        for account_id, segment in segments.items()
    )
    no_progress = sum(
        fixtures["_computed"]["day0"][account_id] is Status.NOT_STARTED
        and sum(day0[account_id]["daily_v1"]) > 0
        and sum(day0[account_id]["daily_v2"]) == 0
        for account_id in segments
    )
    assert straightforward == 1_684
    assert no_progress == 187


def write_fixtures(directory: Path) -> None:
    fixtures = public_fixtures()
    directory.mkdir(parents=True, exist_ok=True)
    for filename, key in (("day0_usage.json", "day0_usage"), ("day7_usage.json", "day7_usage"), ("crm.json", "crm"), ("contracts.json", "contracts"), ("api_v1_sunset.json", "api_v1_sunset")):
        (directory / filename).write_text(json.dumps(fixtures[key], indent=2, sort_keys=True) + "\n", encoding="utf-8")
