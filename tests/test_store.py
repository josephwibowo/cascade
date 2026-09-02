import pytest
from datetime import date

pytest.importorskip("sqlalchemy")

from sqlalchemy import text
from cascade.models import AccountMigration, Base
from cascade.store import (
    chip_facets,
    create_session_factory,
    get_campaign_rollup,
    get_pending_exceptions,
    query_accounts,
    upsert_account_migration,
    upsert_campaign,
    upsert_exception,
)


def _session(tmp_path):
    session_factory = create_session_factory(f"sqlite:///{tmp_path / 'store.db'}")
    Base.metadata.create_all(session_factory.kw["bind"])
    return session_factory()


def _seed_campaign(session, campaign_id="demo"):
    upsert_campaign(session, {"id": campaign_id, "name": "Demo", "change_type": "sunset", "deadline": date(2026, 12, 31)})


def _seed_account(session, account_id, *, status="NOT_STARTED", segment="STANDARD", risk="LOW", arr=10, daily_v1=None, daily_v2=None):
    daily_v1 = [0] if daily_v1 is None else daily_v1
    daily_v2 = [0] if daily_v2 is None else daily_v2
    upsert_account_migration(session, {
        "campaign_id": "demo", "account_id": account_id, "account_name": account_id,
        "arr": arr, "tier": "GROWTH", "owner": "owner", "region": "US",
        "status": status, "segment": segment, "risk": risk,
        "daily_v1": daily_v1, "daily_v2": daily_v2,
    })


def test_upsert_derives_usage_from_daily_arrays(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "derived", daily_v1=[4, 3], daily_v2=[1, 2])
        _seed_account(session, "explicit", daily_v1=[4], daily_v2=[2])
        upsert_account_migration(session, {
            "campaign_id": "demo", "account_id": "explicit", "account_name": "explicit",
            "arr": 10, "tier": "GROWTH", "owner": "owner", "region": "US",
            "status": "NOT_STARTED", "segment": "STANDARD", "risk": "LOW",
            "daily_v1": [10], "daily_v2": [10], "legacy_usage": 99, "replacement_usage": 98,
        })
        session.commit()
        derived = query_accounts(session, "demo", limit=10)
        by_id = {row["account_id"]: row for row in derived["items"]}
        assert by_id["derived"]["legacy_usage"] == 7
        assert by_id["derived"]["replacement_usage"] == 3
        account = session.get(AccountMigration, ("demo", "explicit"))
        assert account.legacy_usage == 99
        assert account.replacement_usage == 98
    finally:
        session.close()


def test_rollup_matches_row_scan(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        rows = [
            ("migrated", "MIGRATED", "STANDARD", "LOW", [1], [1]),
            ("progress", "IN_PROGRESS", "STANDARD", "MEDIUM", [1], [2]),
            ("blocked", "BLOCKED", "TECHNICAL_BLOCKER", "CRITICAL", [3], [0]),
            ("strategic", "NOT_STARTED", "STRATEGIC", "HIGH", [0], [0]),
            ("contractual", "READY_TO_VERIFY", "CONTRACTUAL", "LOW", [0], [1]),
            ("simple", "NOT_STARTED", "STANDARD", "LOW", [0], [2]),
            ("no-progress", "NOT_STARTED", "STANDARD", "HIGH", [5], [0]),
        ]
        for index, (account_id, status, segment, risk, v1, v2) in enumerate(rows):
            _seed_account(session, account_id, status=status, segment=segment, risk=risk, arr=index + 1, daily_v1=v1, daily_v2=v2)
        session.commit()
        rollup = get_campaign_rollup(session, "demo")
        accounts = list(session.query(AccountMigration).filter_by(campaign_id="demo"))
        reference = {
            "affected_accounts": len(accounts), "affected_arr": sum(row.arr for row in accounts),
            "status_distribution": {}, "risk_distribution": {}, "segment_distribution": {},
            "chip_counts": {"straightforward": 0, "actively_migrating": 0, "no_progress": 0, "strategic": 0, "contractual": 0, "technical_blocker": 0},
        }
        for row in accounts:
            for key, value in (("status_distribution", row.status), ("risk_distribution", row.risk), ("segment_distribution", row.segment)):
                reference[key][value] = reference[key].get(value, 0) + 1
            if row.segment == "STANDARD" and row.status == "NOT_STARTED" and sum(row.daily_v2 or []) > 0: reference["chip_counts"]["straightforward"] += 1
            if row.status == "IN_PROGRESS": reference["chip_counts"]["actively_migrating"] += 1
            if row.status == "NOT_STARTED" and sum(row.daily_v1 or []) > 0 and sum(row.daily_v2 or []) == 0: reference["chip_counts"]["no_progress"] += 1
            if row.segment in {"STRATEGIC", "CONTRACTUAL", "TECHNICAL_BLOCKER"}: reference["chip_counts"][row.segment.lower()] = reference["chip_counts"].get(row.segment.lower(), 0) + 1
        assert rollup["affected_accounts"] == reference["affected_accounts"]
        assert rollup["affected_arr"] == reference["affected_arr"]
        assert rollup["status_distribution"] == reference["status_distribution"]
        assert rollup["risk_distribution"] == reference["risk_distribution"]
        assert rollup["segment_distribution"] == reference["segment_distribution"]
        assert rollup["chip_counts"]["straightforward"] == reference["chip_counts"]["straightforward"]
        assert rollup["chip_counts"]["actively_migrating"] == reference["chip_counts"]["actively_migrating"]
        assert rollup["chip_counts"]["no_progress"] == reference["chip_counts"]["no_progress"]
        assert rollup["chip_counts"]["strategic"] == 1
        assert rollup["chip_counts"]["contractual"] == 1
        assert rollup["chip_counts"]["technical_blocker"] == 1
    finally:
        session.close()


def test_rollup_response_keys_are_the_dashboard_contract(tmp_path):
    """The campaign payload is consumed field-by-field by the UI's Campaign type.
    Adding a key here is a contract change, so pin the exact shape and order."""
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "one")
        session.commit()
        assert list(get_campaign_rollup(session, "demo")) == [
            "id", "name", "change_type", "deadline", "airflow_dag_run_id", "status",
            "affected_accounts", "affected_arr", "migration_completion",
            "status_distribution", "risk_distribution", "segment_distribution",
            "blocked_accounts", "pending_exceptions", "chip_counts", "updated_at",
        ]
    finally:
        session.close()


def test_rollup_distributions_are_ordered_by_value(tmp_path):
    """Dashboard renders these dicts as bar segments in key order and re-reads
    them on every poll, so the order must not depend on row storage order."""
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        seeded = [
            ("d", "READY_TO_VERIFY", "MEDIUM", "TECHNICAL_BLOCKER"),
            ("c", "NOT_STARTED", "LOW", "STRATEGIC"),
            ("b", "MIGRATED", "CRITICAL", "STANDARD"),
            ("a", "BLOCKED", "HIGH", "CONTRACTUAL"),
        ]
        for index, (account_id, status, risk, segment) in enumerate(seeded):
            _seed_account(session, account_id, status=status, risk=risk, segment=segment, arr=index + 1)
        session.commit()
        distributions = ("status_distribution", "risk_distribution", "segment_distribution")
        rollup = get_campaign_rollup(session, "demo")
        for key in distributions:
            assert list(rollup[key]) == sorted(rollup[key]), key
        # Rewriting a status is what assess_account does; it must not reorder the bars.
        session.execute(text("UPDATE account_migration SET status = 'MIGRATED' WHERE account_id = 'c'"))
        session.commit()
        after = get_campaign_rollup(session, "demo")
        assert after["status_distribution"]["MIGRATED"] == 2
        for key in distributions:
            assert list(after[key]) == sorted(after[key]), key
    finally:
        session.close()


def test_query_accounts_chip_pushdown_matches_python(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "straight", daily_v2=[2])
        _seed_account(session, "active", status="IN_PROGRESS")
        _seed_account(session, "none", daily_v1=[2])
        _seed_account(session, "strategic", segment="STRATEGIC")
        _seed_account(session, "contractual", segment="CONTRACTUAL")
        _seed_account(session, "technical", segment="TECHNICAL_BLOCKER")
        session.commit()
        expected = {
            "straightforward": {"straight"}, "actively_migrating": {"active"}, "no_progress": {"none"},
            "strategic": {"strategic"}, "contractual": {"contractual"}, "technical_blocker": {"technical"},
        }
        for chip, account_ids in expected.items():
            result = query_accounts(session, "demo", {"chip": chip}, limit=500)
            assert {row["account_id"] for row in result["items"]} == account_ids
            assert result["total"] == len(account_ids)
    finally:
        session.close()


def test_query_accounts_pagination(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        for index in range(7): _seed_account(session, f"a-{index}", arr=index)
        session.commit()
        all_rows = query_accounts(session, "demo", limit=500)
        page = query_accounts(session, "demo", limit=3, offset=2)
        assert page["total"] == 7
        assert [row["account_id"] for row in page["items"]] == [row["account_id"] for row in all_rows["items"][2:5]]
        assert query_accounts(session, "demo", limit=3, offset=99)["items"] == []
    finally:
        session.close()


def test_chip_facets_match_rollup_when_unfiltered(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "simple", daily_v2=[1])
        _seed_account(session, "strategic", segment="STRATEGIC")
        session.commit()
        assert chip_facets(session, "demo", {}) == get_campaign_rollup(session, "demo")["chip_counts"]
    finally:
        session.close()


def test_chip_facets_respect_non_chip_filters(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "blocked", status="BLOCKED", segment="TECHNICAL_BLOCKER")
        _seed_account(session, "simple", daily_v2=[1])
        session.commit()
        assert chip_facets(session, "demo", {"status": "BLOCKED"}) == {
            "straightforward": 0, "actively_migrating": 0, "no_progress": 0,
            "strategic": 0, "contractual": 0, "technical_blocker": 1,
        }
    finally:
        session.close()


def test_pending_exceptions_include_account_fields(tmp_path):
    session = _session(tmp_path)
    try:
        _seed_campaign(session)
        _seed_account(session, "known", segment="TECHNICAL_BLOCKER", risk="CRITICAL", arr=42)
        upsert_exception(session, {"id": "known-ex", "campaign_id": "demo", "account_id": "known", "exception_type": "HITL", "status": "PENDING"})
        upsert_exception(session, {"id": "unknown-ex", "campaign_id": "demo", "account_id": "missing", "exception_type": "HITL", "status": "PENDING"})
        session.commit()
        rows = {row["id"]: row for row in get_pending_exceptions(session, "demo")}
        assert rows["known-ex"]["account_name"] == "known"
        assert rows["known-ex"]["arr"] == 42
        assert rows["known-ex"]["risk"] == "CRITICAL"
        assert rows["unknown-ex"]["account_name"] is None
    finally:
        session.close()


def test_account_upsert_is_idempotent(tmp_path):
    url = f"sqlite:///{tmp_path / 'store.db'}"
    session_factory = create_session_factory(url)
    Base.metadata.create_all(session_factory.kw["bind"])
    session = session_factory()
    payload = {
        "campaign_id": "demo",
        "account_id": "acct-1",
        "account_name": "Account 1",
        "arr": 125,
        "tier": "GROWTH",
        "owner": "owner",
        "region": "US",
        "status": "NOT_STARTED",
        "segment": "STANDARD",
        "risk": "LOW",
        "daily_v1": [5, 4],
        "daily_v2": [0, 0],
    }
    try:
        upsert_account_migration(session, payload)
        session.commit()
        payload["status"] = "IN_PROGRESS"
        payload["replacement_usage"] = 2
        upsert_account_migration(session, payload)
        session.commit()
        rows = list(session.execute(text("SELECT account_id, status, replacement_usage FROM account_migration")))
        assert rows == [("acct-1", "IN_PROGRESS", 2)]
    finally:
        session.close()
