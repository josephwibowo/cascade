#!/usr/bin/env python3
"""Create Cascade's Postgres database and idempotently create its schema."""

from __future__ import annotations

import sys

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from cascade.store import ensure_schema


def main() -> None:
    ensure_schema()
    print("Cascade database schema is ready")


if __name__ == "__main__":
    main()
