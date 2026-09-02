# Cascade

Cascade is an Airflow 3.3.1 control-plane demo for the API v1 sunset. It
combines a real dynamic-mapped assessment DAG, Postgres-backed account state,
Common AI migration briefs with deterministic degradation, a deferred Airflow
HITL review, and a live telemetry verification wave.

## Run locally

Requirements: Docker, the Astro CLI, Node.js, and pnpm.

```bash
cp .env.example .env
astro dev start
pnpm --dir ui install
pnpm --dir ui build
printf 'airflow dags unpause product_change_assessment exception_resolution migration_verification\n' | astro dev bash -s
```

The Airflow UI is available at `http://localhost:8080/`; the mock vendor
systems listen on `http://localhost:8001/`. The Cascade plugin is mounted at
`/cascade` and its React bundle is built into
`plugins/cascade/static/cascade.umd.cjs`.

To rehearse the deterministic hero run from a clean day-zero world:

```bash
printf 'python scripts/reset_demo.py\n' | astro dev bash -s
printf 'python scripts/prepare_hero_run.py\n' | astro dev bash -s
```

The demo UI can then advance the mock world to day 7 and run verification from
the scenario controls. The verification DAG
discovers and maps only the changed accounts. Fixture generation is seeded and
self-validating:

```bash
python scripts/generate_fixtures.py
```

Set `AIRFLOW_CONN_CASCADE_LLM` to a supported pydantic-ai connection for model
generated briefs. When running without a model, remove `AIRFLOW_CONN_CASCADE_LLM`
from `.env` rather than leaving a `REPLACE` placeholder; briefs are then persisted
with `brief_source=deterministic` and migration status remains rule-derived.

## Project layout

- `dags/` — assessment, exception-resolution, and migration-verification DAGs
- `include/cascade/` — rules, fixtures, models, store, API clients, and links
- `mock_services/` — deterministic vendor-system FastAPI service
- `plugins/cascade/` — Airflow FastAPI and React plugin entrypoints
- `ui/` — prefixed, preflight-free React/Tailwind frontend
- `tests/` — rules, aggregate, store, and DAG integrity tests
