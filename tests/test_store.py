import pytest

pytest.importorskip("sqlalchemy")

from sqlalchemy import text
from cascade.models import Base
from cascade.store import create_session_factory, upsert_account_migration


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
