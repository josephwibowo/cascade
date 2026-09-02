#!/usr/bin/env python3
"""Create Cascade's Postgres database and idempotently create its schema."""

from __future__ import annotations

import os
import sys

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import make_url

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(ROOT, "include"))

from cascade.models import Base
from cascade.store import database_url


def ensure_database(url: str) -> None:
    parsed = make_url(url)
    if parsed.get_backend_name() != "postgresql" or not parsed.database:
        return
    admin_url = parsed.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as connection:
            exists = connection.execute(text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": parsed.database}).scalar()
            if not exists:
                connection.execute(text(f'CREATE DATABASE "{parsed.database}"'))
    finally:
        admin_engine.dispose()


def main() -> None:
    url = database_url()
    ensure_database(url)
    engine = create_engine(url)
    Base.metadata.create_all(engine)
    print("Cascade database schema is ready")


if __name__ == "__main__":
    main()
