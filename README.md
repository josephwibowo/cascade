# Cascade

Cascade is an Airflow 3.3.1 control-plane demo for the API v1 sunset. It
combines a real dynamic-mapped assessment DAG, Postgres-backed account state,
Common AI migration briefs with deterministic degradation, a deferred Airflow
human-in-the-loop (HITL) review, and a live telemetry verification wave.

It was built for Astronomer's Beyond the DAG hackathon (2026) around one
claim: an API deprecation is a coordination problem, so the program that runs
it belongs in the orchestrator. Each affected account becomes a mapped task
instance, the exception that needs a decision becomes a deferred DAG run
waiting on a person, and an account counts as migrated only when Airflow
reaches that verdict from telemetry.

## What it does

In the demo, API v1 shuts off on 2026-10-31. The day-0 assessment finds 2,417
accounts still calling it, carrying $2,135,902,569 of annual recurring revenue
(ARR), and classifies them as 1,871 not started, 495 in progress, and 51
blocked. The Cascade UI, served inside Airflow, shows that blast radius and
updates it while the run is still going. From there an operator can:

- narrow the accounts by status, risk, and preset segments, such as the 13
  accounts blocked on a technical dependency, with every count computed in SQL;
- open the exception queue, where Acme Logistics ($2,400,000 ARR, blocked on a
  custom parser) waits on an Airflow HITL decision, and answer it;
- advance the mock world to day 7 and watch a second DAG verify only the 84
  accounts whose telemetry changed, which moves Acme to migrated.

## How it works

One rule holds everywhere: migration status comes only from deterministic
rules over telemetry, in `include/cascade/rules.py`. A model writes
explanations and never writes status, so a model outage cannot move a number.

Cascade has four parts:

- Three DAGs: `product_change_assessment` classifies every affected account,
  `exception_resolution` pauses for a human decision, and
  `migration_verification` re-checks the accounts whose usage changed.
- An Airflow 3 plugin: a FastAPI app serves the Cascade API at `/cascade` from
  Airflow's API server, and a React app adds Cascade to Airflow's navigation at
  `/plugin/cascade`.
- The Cascade store: a `cascade` Postgres database on the Airflow Postgres
  server holds accounts, exceptions, and the timeline. Airflow tasks write every
  account row. The plugin writes only campaign bookkeeping, such as which
  verification run to follow, and the resolved state of an exception whose
  decision it relayed to Airflow.
- Mock vendor systems: `mock_services/` serves usage telemetry, CRM and contract
  records, the change definition, and a scenario clock with day-0 and day-7
  snapshots, all from deterministic fixtures.

The design keeps one boundary from `Cascade_Demo_Technical_Design.md`: fake the
world, never fake the orchestration. The vendor systems are mocks. The
scheduler, the DAG runs, the 2,417 mapped task instances, the HITL pause, the
plugin, and, when a model connection is configured, the model call are real.

### Assessment: day 0

1. `load_change` reads the change from the mock systems and records the
   campaign with this run's id.
2. `discover_affected_accounts` asks the usage system which accounts called v1
   or v2. The mapped count comes from that response, not from configuration:
   2,417 on day 0.
3. `assess_account` runs once per account, at most 16 at a time. Each instance
   reads usage, CRM, and contract data, applies a first-match-wins status table
   (migrated, ready to verify, blocked, in progress, not started), derives risk
   from ARR and status, and writes the account row and a timeline event. A
   blocked account also gets a pending exception.
4. `select_high_risk` takes the eight highest-ARR accounts outside the standard
   segment, and `generate_migration_brief`, a `@task.llm` task, asks the model
   for a typed `MigrationBrief` under the system prompt "You explain migration
   evidence. You never decide migration status."
5. If a model call fails, from a missing connection, a provider error, or
   output that does not validate, `persist_migration_brief` still runs
   (`trigger_rule=ALL_DONE`) and stores a deterministic brief with
   `brief_source=deterministic`. Status and the rollup never read the brief,
   so nothing else changes.
6. `aggregate_campaign` rolls the campaign up, and a `TriggerDagRunOperator`
   starts `exception_resolution` for Acme Logistics.

### Exception: the human decision

1. `exception_resolution` loads Acme's account and builds a review packet with
   its ARR, v1 and v2 call counts, and blocker.
2. `HITLOperator` defers the run on `await_decision`, with three options
   defined in the DAG and a required reason. Cascade's exception queue shows
   the row as awaiting input and renders the options it reads from Airflow,
   rather than defining its own.
3. The operator's answer goes back to Airflow through the plugin.
   `apply_decision` records it with an `EXTENSION_GRANTED` event, and
   `write_timeline_event` closes the exception with `EXCEPTION_RESOLVED`.
4. If the HITL task completes without a chosen option, `apply_decision` raises
   instead of recording a decision nobody made.

### Verification: day 7

1. The scenario controls advance the mock world to day 7 and trigger
   `migration_verification`. The plugin records the run id, so the UI's
   orchestration rail follows the new run.
2. `find_accounts_with_changed_usage` compares each account's daily v1 and v2
   calls against the snapshot named in the `cascade_last_snapshot` Airflow
   Variable. On day 7, 84 accounts changed.
3. `verify_account` runs once per changed account and re-applies the same
   status rules. Acme moves to migrated because its v1 calls have been zero
   for seven days while its v2 calls continue.
4. If a changed account has no assessed row, `verify_account` fails that
   instance rather than inventing one.

## Airflow features used

| Feature | Where | What it does here |
| --- | --- | --- |
| Plugin FastAPI app (`fastapi_apps`) | `plugins/cascade/__init__.py` | Serves the Cascade API at `/cascade` from Airflow's API server |
| Plugin React app (`react_apps`) | `plugins/cascade/__init__.py`, `ui/` | Adds Cascade to Airflow's navigation at `/plugin/cascade` |
| Dynamic task mapping | `assess_account`, `verify_account` | One task instance per account: 2,417 on day 0, 84 on day 7 |
| Common AI `@task.llm` | `generate_migration_brief` | Typed `MigrationBrief` output through a Pydantic AI connection |
| `HITLOperator` | `exception_resolution.await_decision` | Defers the run until a person picks a DAG-defined option |
| `TriggerDagRunOperator` | `trigger_acme_exception` | Hands the blocked account from assessment to the exception DAG |
| `trigger_rule=ALL_DONE` | `persist_migration_brief`, `aggregate_campaign` | Keeps briefs and the rollup running when a mapped upstream task fails |
| Airflow Variable | `cascade_last_snapshot` | The telemetry watermark the verification DAG diffs against |
| Airflow REST API v2 | `include/cascade/airflow_client.py` | Reads run state, mapped task counts, and HITL forms for the UI, and submits HITL responses |

## Challenges

### Counting 2,417 mapped task states

The orchestration rail read "Airflow state unavailable" for an entire
assessment run. It tallied states by walking `listMapped`, which returns at
most 100 rows a page, so each poll cost 49 sequential Airflow calls and took
90.4 seconds during a run. Airflow reports `total_entries` for a
state-filtered query, so the client now asks for one row per state, in
parallel, for only the four states the rail shows: 0.74 seconds median against
the same run.

### Keeping the dashboard live

The dashboard polls every four seconds. The old rollup loaded every account
row to count it in Python and took 1.45 seconds, so a poll would have spent
over a third of every interval scanning, and the account list shipped 1.34 MB
per request. Both became SQL aggregates behind one shared set of segment
predicates, so a count and the rows it filters to cannot disagree: about 13 ms
warm, and a 56 KB first page of 200 rows. Ordering the distributions by value
also mattered, because the old row-scan order moved a bar segment whenever a
task rewrote a status.

### Loading React inside Airflow's React tree

The plugin first failed to mount at all. Vite 8 no longer inlines
`process.env.NODE_ENV` for library builds, so React's development check threw
`process is not defined` in the browser. Fixing that exposed the second copy of
React in the bundle: Airflow renders the plugin inside its own React tree, so
every hook resolved against a null dispatcher. Airflow publishes React,
ReactDOM, and the JSX runtime on the window, so the bundle now takes them from
there.

## Run locally

Requirements: Docker, the Astro CLI, Node.js, and pnpm.

```bash
cp .env.example .env
astro dev start
pnpm --dir ui install
pnpm --dir ui build
printf 'for d in product_change_assessment exception_resolution migration_verification; do airflow dags unpause "$d"; done\n' | astro dev bash -s
```

The Airflow UI is available at `http://localhost:8080/`; the mock vendor
systems listen on `http://localhost:8001/`. The Cascade plugin is mounted at
`/cascade` and its React bundle is built into
`plugins/cascade/static/cascade.umd.cjs`. The Cascade UI opens from Airflow's
navigation, at `http://localhost:8080/plugin/cascade`.

If `http://localhost:8001/health` does not answer, the `mock-services`
container from `docker-compose.override.yml` came up attached to no network,
which also leaves Airflow unable to resolve `mock-services` and silently
breaks the scenario controls and telemetry. Compose reuses the container, so
`astro dev restart` does not repair it. Recreate it:

```bash
astro dev kill && astro dev start
```

To rehearse the deterministic hero run from a clean day-zero world:

```bash
printf 'python scripts/reset_demo.py\n' | astro dev bash -s
printf 'python scripts/prepare_hero_run.py\n' | astro dev bash -s
```

`prepare_hero_run.py` waits for the assessment and asserts the day-0
distribution. In the clean-room run on a laptop-sized stack, the assessment
took 36 minutes and the day-7 verification took under a minute.

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

Airflow's connection test for this connection resolves the model but never
calls it, and a failed call degrades silently to a deterministic brief. Check
the connection with one real call through the DAG's own code path before a
long run. It prints the provider's own error on failure, such as an account
setting the provider requires before it will serve the model:

```bash
printf 'python scripts/check_llm.py\n' | astro dev bash -s
```

## Project layout

- `dags/`: assessment, exception-resolution, and migration-verification DAGs
- `docs/plans/`: the implementation plan behind the demo-readiness work
- `include/cascade/`: rules, fixtures, models, store, API clients, and links
- `mock_services/`: deterministic vendor-system FastAPI service
- `plugins/cascade/`: Airflow FastAPI and React plugin entrypoints
- `scripts/`: schema setup, demo reset, hero-run preparation, fixture
  generation, and the model connection check
- `ui/`: prefixed, preflight-free React/Tailwind frontend
- `tests/`: rules, aggregate, store, and DAG integrity tests

## License

Apache License 2.0; see [`LICENSE`](LICENSE).
