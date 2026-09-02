from __future__ import annotations

from collections.abc import Iterator

from cascade.store import create_session_factory


def session_scope() -> Iterator:
    session = create_session_factory()()
    try:
        yield session
    finally:
        session.close()
