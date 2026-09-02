from cascade.rules import compute_risk, compute_status, zero_v1_streak
from cascade.states import BlockerType, Risk, Status


def test_streak_boundary_seven_zeros_migrates():
    assert compute_status([0] * 7, [1000] * 7, None) is Status.MIGRATED


def test_streak_boundary_six_zeros_does_not_migrate():
    assert compute_status([14000] + [0] * 6, [1000] * 7, None) is Status.READY_TO_VERIFY


def test_telemetry_outranks_blocker():
    assert compute_status([0] * 7, [1000] * 7, BlockerType.CUSTOM_PARSER) is Status.MIGRATED


def test_status_table_rows():
    assert compute_status([1, 0], [0, 0], None) is Status.READY_TO_VERIFY
    assert compute_status([1, 1], [1, 1], BlockerType.SDK_PINNED) is Status.BLOCKED
    assert compute_status([100, 100], [20, 20], None, prior_v1_7d=1_000) is Status.IN_PROGRESS
    assert compute_status([100, 100], [0, 0], None) is Status.NOT_STARTED


def test_empty_and_short_series():
    assert zero_v1_streak([]) == 0
    assert zero_v1_streak([0, 0]) == 2
    assert compute_status([], [], None) is Status.NOT_STARTED


def test_compute_risk_boundaries():
    assert compute_risk(1_000_000, Status.NOT_STARTED) is Risk.CRITICAL
    assert compute_risk(1_000_000, Status.IN_PROGRESS) is Risk.HIGH
    assert compute_risk(250_000, Status.IN_PROGRESS) is Risk.MEDIUM
    assert compute_risk(10_000, Status.BLOCKED) is Risk.MEDIUM
    assert compute_risk(10_000, Status.NOT_STARTED) is Risk.LOW
