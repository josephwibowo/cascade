#!/usr/bin/env python3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "include"))

from cascade.fixtures import assert_fixture_integrity, build_fixtures, write_fixtures


if __name__ == "__main__":
    fixtures = build_fixtures()
    assert_fixture_integrity(fixtures)
    write_fixtures(ROOT / "mock_services" / "fixtures")
    print("Generated deterministic Cascade fixtures: 2,417 accounts, day0/day7 delta 84")
