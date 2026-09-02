"""Make the ``cascade`` package importable when running a single test file.

Running the full suite (``pytest tests -q``) happens to work today because
``tests/dags/*`` imports Airflow during collection, and Airflow appends
``include/`` and ``plugins/`` to ``sys.path`` as a side effect of loading DAGs
and plugins. Targeting a single test file (e.g. ``pytest tests/test_store.py``)
skips that collection path entirely, so ``cascade`` is never importable and
the run fails with ``ModuleNotFoundError``.

Only ``include/`` is added here, not ``plugins/``. ``plugins/cascade`` is a
namespace-extended package (see ``plugins/cascade/__init__.py``) that merges
itself with ``include/cascade`` at runtime inside Airflow; importing it here
outside of Airflow would pull in ``plugins/cascade/api`` and its FastAPI/
Airflow-only dependencies, which the tests do not need and do not want.
"""
from __future__ import annotations

import sys
from pathlib import Path

_include = Path(__file__).resolve().parents[1] / "include"
if str(_include) not in sys.path:
    sys.path.insert(0, str(_include))
