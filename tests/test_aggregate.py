import pytest
from datetime import date

pytest.importorskip("sqlalchemy")

from cascade.aggregate import aggregate_campaign
from cascade.models import Base
from cascade.store import create_session_factory, upsert_account_migration, upsert_campaign


def test_aggregate_campaign_is_idempotent(tmp_path):
    url = f"sqlite:///{tmp_path / 'aggregate.db'}"
    engine_factory = create_session_factory(url)
    Base.metadata.create_all(engine_factory.kw["bind"])
    session = engine_factory()
    try:
        upsert_campaign(
            session,
            {
                "id": "demo",
                "name": "Demo",
                "change_type": "sunset",
                "deadline": date(2026, 12, 31),
            },
        )
        for account_id, status in (("a", "MIGRATED"), ("b", "IN_PROGRESS")):
            upsert_account_migration(
                session,
                {
                    "campaign_id": "demo",
                    "account_id": account_id,
                    "account_name": account_id,
                    "arr": 10,
                    "tier": "GROWTH",
                    "owner": "owner",
                    "region": "US",
                    "status": status,
                    "segment": "STANDARD",
                    "risk": "LOW",
                    "daily_v1": [1],
                    "daily_v2": [1],
                },
            )
        session.commit()
        first = aggregate_campaign(session, "demo")
        second = aggregate_campaign(session, "demo")
        assert first["affected_accounts"] == second["affected_accounts"] == 2
        assert first["affected_arr"] == second["affected_arr"] == 20
        assert first["status_distribution"] == second["status_distribution"]
    finally:
        session.close()
