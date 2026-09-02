"""Pure, deterministic lifecycle and risk rules for Cascade."""

from __future__ import annotations

from collections.abc import Sequence

from cascade.states import BlockerType, Risk, Status


def zero_v1_streak(daily_v1: Sequence[int]) -> int:
    """Return consecutive zero-traffic days ending on the snapshot day."""
    streak = 0
    for calls in reversed(daily_v1):
        if calls != 0:
            break
        streak += 1
    return streak


def compute_status(
    daily_v1: Sequence[int],
    daily_v2: Sequence[int],
    blocker_type: BlockerType | str | None,
    prior_v1_7d: int | None = None,
    never_needed_replacement: bool = False,
) -> Status:
    """Apply the contract's first-match-wins lifecycle table.

    ``prior_v1_7d`` is the preceding trailing-window total.  It is optional so
    callers that only have a single snapshot can still classify accounts, but
    an account is only considered actively migrating when the preceding window
    is explicitly known and larger than the current one.
    """
    v1_today = daily_v1[-1] if daily_v1 else 0
    v1_7d = sum(daily_v1)
    v2_7d = sum(daily_v2)
    streak = zero_v1_streak(daily_v1)
    blocker = blocker_type is not None

    if (
        v1_today == 0
        and streak >= 7
        and (v2_7d > 0 or never_needed_replacement)
    ):
        return Status.MIGRATED
    if v1_today == 0 and 1 <= streak < 7:
        return Status.READY_TO_VERIFY
    if blocker:
        return Status.BLOCKED
    if v2_7d > 0 and prior_v1_7d is not None and v1_7d < prior_v1_7d:
        return Status.IN_PROGRESS
    return Status.NOT_STARTED


def compute_risk(arr: float, status: Status | str) -> Risk:
    """Derive risk from ARR and lifecycle status, in contract order."""
    status = Status(status)
    if arr >= 1_000_000 and status in (Status.BLOCKED, Status.NOT_STARTED):
        return Risk.CRITICAL
    if arr >= 1_000_000 or (arr >= 250_000 and status is Status.BLOCKED):
        return Risk.HIGH
    if arr >= 250_000 or status is Status.BLOCKED:
        return Risk.MEDIUM
    return Risk.LOW
