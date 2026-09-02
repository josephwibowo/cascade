# Cascade — Implementation Plan

**Target:** `/Users/joseph/programming/github/astronomer-demo`
**Source of requirements:** `Cascade_Product_Hypothesis_and_Demo.md`, `Cascade_Demo_Technical_Design.md`, `Cascade_Build_Contract.md` (binding on conflicts)
**Sequencing:** by dependency only. No schedule, effort, or duration guidance appears in this plan by design.

---

## 1. Goal

A working Airflow 3.3.1 deployment in which a domain-specific business application — Cascade, a product-change control plane — is served as an Airflow plugin, and every business state it displays is derived from real Airflow execution.

When this is done:

- `astro dev start` brings up Airflow, a Cascade Postgres database, and a mock-systems service in one command.
- A real `product_change_assessment` DAG run has dynamically mapped `assess_account` across 2,417 accounts, each producing an independently observable task instance and a row in the Cascade database.
- Cascade appears in the Airflow navigation as a React application, reading exclusively from the Cascade database and the Airflow REST API.
- Acme Logistics' `exception_resolution` task genuinely enters `awaiting_input`; a decision submitted from the Cascade UI resumes the real task, and the resulting timeline event is written by the resumed DAG, not the frontend.
- Advancing the mock world to day 7 triggers a real `migration_verification` run that maps only changed accounts, and Acme becomes `MIGRATED` because a deterministic rule evaluated computed telemetry — not because anything set it.
- **View Orchestration** deep-links to the native Airflow run behind whatever the user is looking at.

---

## 2. Current state

### 2.1 Repository

Greenfield. The repository contains three markdown documents and nothing else — no code, no `package.json`, no `Dockerfile`, and it is not yet a git repository. There is no existing convention, abstraction, or test suite to reuse or extend; every file in this plan is new.

`git init` is a prerequisite to any of the work below.

### 2.2 Host toolchain (verified present)

Docker 29.1.3 with a running daemon, Docker Compose v5.0.1, Python 3.14.0, uv 0.10.8, Node v26.7.0, pnpm 10.0.0, npm 11.19.0. **Astro CLI is not installed** and must be added (`brew install astro`). `psql` is absent; use `docker compose exec` for database access rather than a host client.

### 2.3 External interfaces — verified against source, not documentation

The following were confirmed by reading the `apache-airflow` 3.3.1 package and its compiled UI bundle, downloaded via `uv pip install --target ./pkgs apache-airflow-providers-standard apache-airflow-providers-common-ai`. These supersede the "unverified" markers in build contract §10.

#### 2.3.1 The `react_apps` bundle contract — RESOLVED

Airflow's UI loads a plugin bundle with this logic (deminified from `airflow/ui/dist/assets/index-*.js`):

```js
const load = (reactApp) => lazy(() =>
  import(new URL(reactApp.bundle_url, document.baseURI).href).then(() => {
    let C = globalThis[reactApp.name];
    if (C === undefined) {
      C = globalThis.AirflowPlugin;
      globalThis[reactApp.name] = C;
    }
    if (typeof C !== 'function')
      throw TypeError(`Expected function, got ${typeof C} for plugin ${reactApp.name}`);
    return { default: C };
  })
).catch(e => (console.error('Component failed to load:', e), { default: ErrorFallback }));
```

and renders the result as:

```js
<Suspense fallback={<Spinner/>}>
  <Component dagId={dagId} mapIndex={mapIndex} runId={runId} taskId={taskId} />
</Suspense>
```

Four binding consequences:

1. The bundle is fetched by **dynamic `import()`**, so it must be served with a JavaScript MIME type. A `.cjs` file served by FastAPI `StaticFiles` defaults to `text/plain` and the browser will refuse to execute it. `mimetypes.add_type("application/javascript", ".cjs")` is mandatory.
2. The bundle's side effect must be assigning **`globalThis[<the react_apps "name" value>]`** — or `globalThis.AirflowPlugin` as fallback — to a React component. A UMD build satisfies this: under ESM import there is no `module` or `define`, so UMD falls through to global assignment.
3. The assigned value must be a **function**. A `React.memo(...)` or `forwardRef(...)` result is an object and will throw `TypeError`. Export a plain function component.
4. The component receives `{dagId, runId, taskId, mapIndex}`. For `destination: "nav"` all four are `undefined`; the component must not require them.

Airflow does **not** provide React to the bundle, so React must be bundled in.

#### 2.3.2 HITL operators — `airflow.providers.standard.operators.hitl`

```python
HITLOperator(
    task_id: str, subject: str, options: list[str], body: str | None = None,
    defaults: str | list[str] | None = None, multiple: bool = False,
    params: ParamsDict | dict | None = None,
    notifiers: Sequence[BaseNotifier] | BaseNotifier | None = None,
    assigned_users: HITLUser | list[HITLUser] | None = None,
    response_timeout: timedelta | None = None,
)
```

Subclasses: `ApprovalOperator` (adds `ignore_downstream_trigger_rules`, `fail_on_reject`), `HITLBranchOperator` (adds `options_mapping`), `HITLEntryOperator` (params-only).

The task pushes `{"chosen_options": [...], "params_input": {...}}` to XCom — confirmed by the provider's own example DAG, which reads `ti.xcom_pull(task_ids=...)["chosen_options"]` and `["params_input"]["information"]`.

`assigned_users` takes dicts shaped `{"id": "...", "name": "..."}`. There is also a useful helper: `HITLOperator.generate_link_to_ui_from_context(context, base_url)`.

**`HITLOperator` with both `options` and `params` in one task gives Cascade its decision-plus-reason in a single HITL interaction.** No branch operator is needed.

#### 2.3.3 Airflow REST API paths (prefix `/api/v2`)

| Purpose | Method + path |
|---|---|
| Trigger a DAG run | `POST /dags/{dag_id}/dagRuns` |
| List mapped task instances | `GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/listMapped` |
| One task instance | `GET /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}` |
| Submit a HITL response | `PATCH /dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails` |
| Read HITL details for a run | `GET /dags/{dag_id}/dagRuns/{dag_run_id}/hitlDetails` |

`listMapped` supports range and state filtering and is the correct source for the live rail's counters. Confirm exact filter parameter names against `/api/v2/openapi.json` in the running instance.

#### 2.3.4 Common AI — `@task.llm`

Built on pydantic-ai. **The decorated function returns the prompt string**; the decorator performs the call. `LLMOperator` accepts `prompt`, `llm_conn_id`, `model_id`, `system_prompt`, `output_type`, `agent_params`, `usage_limits`, `serialize_output`.

When `output_type` is a Pydantic model, the instance is returned to XCom — but the source computes `_serialize_model_output = serialize_output or not _CORE_WALKER`, so the XCom value may arrive as a **dict or a model instance** depending on the core's deserialization registration. Consuming tasks must normalize with `MigrationBrief.model_validate(...)`.

The model comes from the connection (`extra.model`), overridable per-task with `model_id`.

#### 2.3.5 Config defaults that matter

`core.max_map_length` 1024 · `core.parallelism` 32 · `core.max_active_tasks_per_dag` **16**. The last is the binding constraint on fan-out width and must be raised.

---

## 3. Scope

### In scope

- Astro project scaffold, Dockerfile, requirements, Airflow configuration.
- Cascade Postgres schema and data-access layer.
- The state model, rules engine, and its tests.
- Deterministic fixture generator with distribution assertions.
- Mock systems FastAPI service and its HTTP client.
- All three DAGs, including real dynamic task mapping and a real HITL pause/resume.
- Cascade FastAPI plugin app and its endpoints.
- Airflow REST client used by the plugin for orchestration state, HITL proxying, and DAG triggering.
- React application: dashboard, blast-radius table, account drawer, HITL modal, live rail, orchestration deep links.
- `@task.llm` migration brief with deterministic degradation.
- Seed, hero-run, and reset scripts.

### Supporting changes

- `git init` and a `.gitignore` covering `ui/node_modules`, `ui/dist`, `plugins/cascade/static/`, `.env`, `pkgs/`.
- `docker-compose.override.yml` to run the mock service inside the Astro stack.
- MIME-type registration for `.cjs` (a one-line fix, but the plugin does not load without it).

### Out of scope

Named so it is clear these were considered and rejected, not overlooked:

- **Create-campaign UI flow.** Cut per build contract decision 6. The hero campaign is produced by script. Contract §12 notes this is reversible.
- **Multiple concurrent HITL exceptions.** Exactly one real HITL exists, per contract §7.4.
- **Authentication, roles, multi-tenancy.** The demo runs as the Astro default user.
- **Alembic or any migration framework.** One idempotent `init_db.py`; the schema is created, never migrated.
- **AssetWatcher / event-driven scheduling.** First item in the technical design's cut list.
- **Real external integrations, email, Slack.** All mocked or absent.
- **A second product-change scenario.** One narrative: Beacon Events API v1 sunset.
- **Generalized contract parsing.** Seeded contract flags only.

---

## 4. Assumptions

| # | Assumption | What invalidates it | Response if invalidated |
|---|---|---|---|
| 1 | `react_apps` behaves as §2.3.1 describes in the running instance. | Bundle throws, or the component never mounts. | Contract §6.4 iframe fallback: the mounted component renders a full-bleed `<iframe src="/cascade/app">`. |
| 2 | Airflow's backend accepts same-origin API calls from the plugin using the session cookie, with no `Authorization` header. | A `401` from `fetch('/api/v2/version')` inside the plugin. | The React app calls only `/cascade/*`; the FastAPI app proxies every Airflow API call server-side. This is already how HITL submission works, so the change is additive. |
| 3 | Astro Runtime 3.3-7 does not already bundle `common-ai`. | `pip show` inside the container finds it. | Drop the explicit requirement, keep the extras. |
| 4 | 2,417 mapped task instances complete without exhausting the local Docker resources. | Scheduler thrashing, OOM, or task failures at high map index. | Lower `parallelism` (throughput, not correctness) — never shrink the population, per contract §5.1. |
| 5 | An OpenRouter free model is adequate for developing the brief prompt. | Model refuses structured output or produces unusable briefs. | Develop against the paid connection; the code path is identical since only the connection differs. |

---

## 5. Approach

### 5.1 Rules as pure functions, computed at the edge

`include/cascade/rules.py` contains only pure functions over plain data — no database, no Airflow imports, no I/O. Both DAG A and DAG C call the same functions; so do the tests. This is what makes the demo's central claim testable in isolation: the seven-day rule is a function of a telemetry series, and a unit test can prove the boundary case that the whole final beat depends on.

**Rejected alternative:** computing status in SQL. It would be faster at 2,417 rows, but it puts the demo's most load-bearing logic somewhere no test can reach cheaply and no reviewer can read.

### 5.2 The mock service is a real HTTP boundary

Per contract decision 9, the DAGs reach mocked systems over HTTP through `mock_client.py`, not by importing fixture files. This costs a container and gains an inspectable seam: "fake the world, never fake the orchestration" becomes a property someone can verify by looking at what the DAG talks to.

**Rejected alternative:** in-process fixture loading. Simpler and one fewer moving part, but it collapses the real/mocked boundary into an import statement and makes the DAGs structurally unlike anything real.

### 5.3 Bulk fetch, narrow map

`discover_affected_accounts` fetches the entire population from the mock service in one call and returns a list of account IDs. Each mapped `assess_account` receives one ID and performs one narrow lookup. This keeps 2,417 mapped instances cheap without pretending the service isn't there — and it is the shape a real implementation would take anyway, since no one makes 2,417 sequential API calls.

### 5.4 The frontend owns no state logic

The React app renders what `/cascade/*` returns and what the Airflow API reports. It computes no status, derives no count, and holds no fixture. Contract §3.7's no-literals rule is enforced structurally: if a number appears in the UI, there is an endpoint that produced it.

### 5.5 Two sources, cleanly separated

Cascade endpoints serve **product state** from the Cascade database. Orchestration endpoints proxy **Airflow state** from the Airflow REST API. They are never merged server-side into a single object that blurs which is which — the exceptions tile (§7.4) is exactly the case where conflating them would produce a false claim.

---

## 6. Steps

### Step 1 — Repository and Astro scaffold

**Files:** `.gitignore`, `Dockerfile`, `requirements.txt`, `packages.txt`, `.env`, `.astro/config.yaml`, `airflow_settings.yaml`, `dags/.airflowignore`

**Change:**

```bash
git init
brew install astro
astro dev init
```

Then replace the generated `Dockerfile` with:

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-7
```

`requirements.txt`:

```
apache-airflow-providers-common-ai[anthropic,openai]
psycopg[binary]
httpx
```

`.env` — the concurrency values are not optional; `max_active_tasks_per_dag` defaults to 16 and would cap the hero fan-out regardless of every other setting:

```bash
AIRFLOW__CORE__MAX_MAP_LENGTH=4096
AIRFLOW__CORE__PARALLELISM=64
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=4
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=64
CASCADE_DB_URL=postgresql+psycopg://postgres:postgres@postgres:5432/cascade
CASCADE_MOCK_URL=http://mock-services:8000
AIRFLOW_CONN_CASCADE_LLM='{"conn_type":"pydanticai","host":"https://openrouter.ai/api/v1","password":"REPLACE","extra":{"model":"openai:REPLACE"}}'
```

**Integration:** none yet; this is the substrate.

**Invariants:** Astro's own generated directories (`dags/`, `include/`, `plugins/`, `tests/`) keep their conventional meaning — `plugins/` is auto-registered, `include/` is on the Python path.

**Edge cases:** Astro's generated `Dockerfile` may pin an older runtime; replace rather than edit. `.env` must be gitignored before any key is added.

**Verify:** `astro dev start` reaches a healthy UI at `localhost:8080`. Then confirm assumption 3:

```bash
astro dev bash -s -c "pip show apache-airflow-providers-common-ai | head -2"
```

---

### Step 2 — Cascade database schema

**Files:** `include/cascade/models.py`, `scripts/init_db.py`

**Change:** SQLAlchemy 2.0 declarative models for four tables, per contract §12 with the additions this plan requires:

- `campaign` — `id` (PK), `name`, `change_type`, `deadline`, `airflow_dag_run_id`, `status`, `affected_accounts`, `affected_arr`, `created_at`, `updated_at`
- `account_migration` — composite PK `(campaign_id, account_id)`; `account_name`, `arr`, `tier`, `owner`, `region`, `status`, `segment`, `risk`, `blocker_type`, `legacy_usage`, `replacement_usage`, `zero_v1_streak_days`, `daily_v1` (JSON), `daily_v2` (JSON), `latest_airflow_task_instance` (JSON: `{dag_id, run_id, task_id, map_index}`), `brief` (JSON, nullable), `brief_source` (nullable), `updated_at`
- `exception` — `id` (PK), `campaign_id`, `account_id`, `exception_type`, `airflow_dag_run_id` (**nullable**), `hitl_task_id` (**nullable**), `status`, `decision`, `decision_reason`, `resolved_at`
- `timeline_event` — `id` (PK), `campaign_id`, `account_id` (nullable for campaign-level events), `event_type`, `timestamp`, `summary`, `source`, `airflow_run_id`, `airflow_task_id`

Indexes on `account_migration(campaign_id, status)`, `account_migration(campaign_id, segment)`, `account_migration(campaign_id, arr)`, `timeline_event(campaign_id, account_id, timestamp)`.

`scripts/init_db.py` creates the `cascade` database if absent, then `Base.metadata.create_all()`. Idempotent — safe to run repeatedly.

**Integration:** `CASCADE_DB_URL` from Step 1. The Postgres container is the one Astro already runs; this adds a second database on it.

**Invariants:** the nullability of `exception.hitl_task_id` is load-bearing for §7.4 — it is the sole discriminator between a product exception and a real Airflow HITL. Do not make it non-null.

**Edge cases:** `CREATE DATABASE` cannot run inside a transaction — use an `AUTOCOMMIT` connection to the `postgres` database.

**Verify:** `python scripts/init_db.py` twice in a row; second run is a no-op and exits 0. `\dt` in `docker compose exec postgres psql -U postgres -d cascade` lists four tables.

---

### Step 3 — State model and rules engine

**Files:** `include/cascade/states.py`, `include/cascade/rules.py`, `tests/test_rules.py`

**Change:** `states.py` defines four `StrEnum`s exactly per contract §3: `Status` (`NOT_STARTED`, `IN_PROGRESS`, `BLOCKED`, `READY_TO_VERIFY`, `MIGRATED`), `Segment` (`STANDARD`, `STRATEGIC`, `CONTRACTUAL`, `TECHNICAL_BLOCKER`), `Risk` (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`), `BlockerType` (`CUSTOM_PARSER`, `SDK_PINNED`, `CONTRACT_COMMITMENT`, `NO_OWNER`).

`rules.py` — pure functions, no imports beyond stdlib and `states`:

```python
def zero_v1_streak(daily_v1: list[int]) -> int:
    """Consecutive zero-traffic days ending on and including the snapshot day."""
    streak = 0
    for calls in reversed(daily_v1):
        if calls != 0:
            break
        streak += 1
    return streak


def compute_status(
    daily_v1: list[int],
    daily_v2: list[int],
    blocker_type: BlockerType | None,
    prior_v1_7d: int | None = None,
    never_needed_replacement: bool = False,
) -> Status:
    """First match wins. Telemetry outranks blockers — see contract §3.1."""
```

Implement the §3.1 table in exactly that order: `MIGRATED` → `READY_TO_VERIFY` → `BLOCKED` → `IN_PROGRESS` → `NOT_STARTED`. Also `compute_risk(arr, status) -> Risk` per §3.3.

**Integration:** imported by DAG A's `assess_account`, DAG C's `verify_account`, and the fixture generator's self-check. Nothing else may compute status.

**Invariants:** the ordering. `BLOCKED` must not preempt `READY_TO_VERIFY` or `MIGRATED` — Acme's entire arc depends on telemetry overriding an uncleared blocker flag.

**Edge cases:**

- `daily_v1` all zeros with length 7 → streak 7 → `MIGRATED` (given v2 traffic).
- `daily_v1 = [14000, 0, 0, 0, 0, 0, 0]` → streak 6 → `READY_TO_VERIFY`, **not** `MIGRATED`. This is the contract §4.4 boundary.
- Empty or short series → streak equals length; must not raise.
- `never_needed_replacement=True` accounts reach `MIGRATED` on zero v1 alone.

**Verify:** `tests/test_rules.py` covers every row of the §3.1 table plus both sides of the streak boundary:

```python
def test_streak_boundary_seven_zeros_migrates():
    assert compute_status([0]*7, [1000]*7, None) is Status.MIGRATED

def test_streak_boundary_six_zeros_does_not_migrate():
    assert compute_status([14000] + [0]*6, [1000]*7, None) is Status.READY_TO_VERIFY

def test_telemetry_outranks_blocker():
    assert compute_status([0]*7, [1000]*7, BlockerType.CUSTOM_PARSER) is Status.MIGRATED
```

`pytest tests/test_rules.py` green. **Write this test before the fixture generator** — the generator's assertions depend on these rules being right.

---

### Step 4 — Deterministic fixture generator

**Files:** `include/cascade/fixtures.py`, `scripts/generate_fixtures.py`, `mock_services/fixtures/*.json`

**Change:** a seeded generator (single module-level `SEED` constant) producing 2,417 accounts across two snapshots.

Per account, per snapshot, per contract §4.2:

```json
{
  "account_id": "acct_00417",
  "snapshot_id": "day7",
  "daily_v1": [0, 0, 0, 0, 0, 0, 0],
  "daily_v2": [1180, 1240, 1310, 1290, 1355, 1402, 1388],
  "endpoints": ["/v1/events"],
  "sdk_name": "beacon-python",
  "sdk_version": "2.4.1"
}
```

`zero_v1_streak_days` is deliberately **not** a fixture field — it is computed by `rules.zero_v1_streak`.

Also emit `crm.json` (account_name, arr, tier, csm, region), `contracts.json` (compatibility_commitment, commitment_expiry), and `api_v1_sunset.json` (campaign metadata, legacy/replacement endpoints, deadline 2026-10-31).

Hand-author four accounts before generating the rest: **Acme Logistics** ($2.4M ARR, `TECHNICAL_BLOCKER`, `CUSTOM_PARSER`, commitment through Oct 31, named CSM, day-0 `daily_v1` ≈ 14,000/day and no v2, day-7 `daily_v1 = [0]*7` with rising v2), one clean strategic migration, one no-progress account, one already-mostly-migrated account.

`scripts/generate_fixtures.py` regenerates and then **asserts**:

- Day-0 status distribution: `NOT_STARTED` 1,871 · `IN_PROGRESS` 495 · `BLOCKED` 51 · `READY_TO_VERIFY` 0 · `MIGRATED` 0
- Day-0 segment distribution: `STANDARD` 2,292 · `STRATEGIC` 74 · `CONTRACTUAL` 38 · `TECHNICAL_BLOCKER` 13
- `BLOCKED` set is exactly the `CONTRACTUAL ∪ TECHNICAL_BLOCKER` set
- Chip counts: straightforward 1,684, no-progress 187
- Day-7 delta is exactly 84 accounts, with movement in each of `MIGRATED`, `READY_TO_VERIFY`, `IN_PROGRESS`
- Acme's day-7 status is `MIGRATED`

Assertions run the real `rules.py` over the generated fixtures — the generator must not assign statuses directly, it must shape telemetry until the rules produce the target distribution.

**Integration:** output consumed only by the mock service.

**Invariants:** byte-identical output across runs. No wall-clock, `uuid4`, or unseeded randomness anywhere in the generator.

**Edge cases:** the day-7 file must contain all 2,417 accounts, not just the 84 changed — the delta is discovered by comparing snapshots, and a partial file would make every absent account look changed.

**Verify:**

```bash
python scripts/generate_fixtures.py && sha256sum mock_services/fixtures/*.json > /tmp/a
python scripts/generate_fixtures.py && sha256sum mock_services/fixtures/*.json > /tmp/b
diff /tmp/a /tmp/b
```

Exits clean, and the script's own assertions pass.

---

### Step 5 — Mock systems service

**Files:** `mock_services/app.py`, `docker-compose.override.yml`

**Change:** a FastAPI app serving contract §4.8:

```
GET  /usage/accounts?snapshot=<id>   # full population, one call
GET  /crm/accounts
GET  /contracts/accounts
GET  /migration/change
GET  /scenario                       # current snapshot id
POST /scenario/advance               # {"snapshot": "day7"}
```

Fixtures load into memory at startup. `POST /scenario/advance` swaps the served snapshot under an `asyncio.Lock` so no request observes a half-advanced world.

`docker-compose.override.yml` adds the service to Astro's stack:

```yaml
services:
  mock-services:
    build: ./mock_services
    ports: ["8001:8000"]
    volumes: ["./mock_services:/app"]
```

**Integration:** Astro merges `docker-compose.override.yml` into its own compose project, so `astro dev start` brings it up. Scheduler and API-server containers reach it at `http://mock-services:8000`.

**Invariants:** bulk endpoints return the whole population in one response — Step 6's design depends on it.

**Edge cases:** advancing to an unknown snapshot returns 400 rather than silently no-op'ing. Advancing twice to `day7` is idempotent.

**Verify:** `curl localhost:8001/usage/accounts?snapshot=day0 | jq 'length'` → 2417. `curl -XPOST localhost:8001/scenario/advance -d '{"snapshot":"day7"}'` then `curl localhost:8001/scenario` reflects it.

---

### Step 6 — Mock client and store

**Files:** `include/cascade/mock_client.py`, `include/cascade/store.py`

**Change:** `mock_client.py` — a thin `httpx` client reading `CASCADE_MOCK_URL`, with one method per endpoint and a typed return. No retries beyond httpx defaults; this is a local service and a failure should be loud.

`store.py` — the only module that touches the Cascade database. Functions: `upsert_account_migration()`, `upsert_campaign()`, `append_timeline_event()`, `upsert_exception()`, `get_campaign_rollup()`, `query_accounts(filters)`, `get_account()`. Each takes an explicit session; no ambient globals.

**Integration:** DAG tasks call `store` for writes; the FastAPI app calls `store` for reads.

**Invariants:** `upsert_account_migration` is idempotent on `(campaign_id, account_id)` — DAG C re-runs over accounts DAG A already wrote.

**Edge cases:** 64 concurrent mapped tasks upserting simultaneously. Use `INSERT ... ON CONFLICT DO UPDATE`, not read-then-write, or concurrent tasks will clobber each other.

**Verify:** a scratch script upserts the same account twice concurrently and leaves one row with the later values.

---

### Step 7 — DAG A: `product_change_assessment`

**Files:** `dags/product_change_assessment.py`, `include/cascade/aggregate.py`

**Change:** the task graph from technical design §6.

```python
@task
def load_change() -> dict:
    """Fetch change metadata, upsert campaign, record context['dag_run'].run_id."""

@task
def discover_affected_accounts(change: dict) -> list[str]:
    """One bulk call to /usage/accounts. Apply the lookback rule.
    Return account IDs — this list is the real input to expand()."""

@task
def assess_account(account_id: str, change: dict) -> dict:
    """One account: join usage + CRM + contract, call rules.compute_status
    and rules.compute_risk, upsert account_migration, append timeline event.
    Records latest_airflow_task_instance from context."""

@task
def select_high_risk(assessments: list[dict]) -> list[dict]:
    """segment != STANDARD, ordered by ARR desc, capped at 8."""

@task
def aggregate_campaign(campaign_id: str) -> dict:
    """Recompute counts, ARR sums, distribution. Write campaign projection."""
```

Wiring:

```python
change = load_change()
accounts = discover_affected_accounts(change)
assessments = assess_account.partial(change=change).expand(account_id=accounts)
high_risk = select_high_risk(assessments)
briefs = generate_migration_brief.expand(evidence=high_risk)   # added in Step 14
aggregate_campaign(change["campaign_id"]) << [briefs]
```

`aggregate_campaign` lives in `include/cascade/aggregate.py` and is imported by both DAG A and DAG C — one implementation, idempotent, safe to call repeatedly.

**Integration:** first real consumer of `rules`, `store`, and `mock_client`.

**Invariants:** `discover_affected_accounts` must return a genuine runtime-computed list. Never a literal, never a `range()`, never a length read from config — the dynamic mapping claim rests entirely on this.

**Edge cases:** `expand()` over 2,417 items requires `max_map_length` ≥ 2417 (Step 1 sets 4096). A single failed mapped task must not fail the run — set `assess_account` retries to 1 and let `aggregate_campaign` use a trigger rule tolerant of partial failure, so one bad account doesn't cost the whole hero run.

**Verify:** trigger with a 200-account fixture first. Confirm in the Airflow UI that `assess_account` shows 200 mapped instances with independent states, and that `account_migration` has 200 rows whose statuses match what `rules.py` produces for the same inputs.

---

### Step 8 — Hero run

**Files:** `scripts/prepare_hero_run.py`, `scripts/reset_demo.py`

**Change:** `prepare_hero_run.py` resets the mock service to day 0, clears Cascade tables, triggers `product_change_assessment` via `POST /api/v2/dags/product_change_assessment/dagRuns`, polls until terminal, then asserts >1,000 mapped `assess_account` instances in `success` and that the resulting distribution matches contract §3.5.

`reset_demo.py` returns the world to day 0, clears Cascade tables, and clears prior DAG runs — so the demo path can be rehearsed repeatedly from a known state.

**Integration:** uses the Airflow REST API, same client as Step 10.

**Invariants:** the hero run is a real completed run. Its `dag_run_id` is stored on `campaign.airflow_dag_run_id` and is what **View Orchestration** later links to.

**Edge cases:** if mapped tasks fail at high map index, capture the failing task log before reducing `parallelism` — resource exhaustion and a data bug in a rarely-hit fixture branch look identical from the run summary.

**Verify:** the script's own assertions, plus the Airflow UI showing the mapped task grid for the run.

---

### Step 9 — Cascade FastAPI app

**Files:** `plugins/cascade/__init__.py`, `plugins/cascade/api/app.py`, `plugins/cascade/api/routes/*.py`, `include/cascade/airflow_client.py`

**Change:** the endpoint surface from technical design §15:

```
GET  /cascade/campaigns/{campaign_id}
GET  /cascade/campaigns/{campaign_id}/accounts      # filters: status, segment, risk, q, chip
GET  /cascade/accounts/{account_id}
GET  /cascade/campaigns/{campaign_id}/timeline
GET  /cascade/exceptions/pending
POST /cascade/exceptions/{exception_id}/respond
POST /cascade/scenario/advance
POST /cascade/campaigns/{campaign_id}/verify
GET  /cascade/orchestration/{campaign_id}           # live rail counters
```

`include/cascade/airflow_client.py` wraps the Airflow REST API for: triggering runs, `listMapped` counters, and the HITL `PATCH`.

The plugin class — note the MIME registration, without which the bundle silently fails to execute:

```python
import mimetypes
from pathlib import Path
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from airflow.plugins_manager import AirflowPlugin
from cascade.api.app import cascade_app

mimetypes.add_type("application/javascript", ".cjs")

cascade_app.mount(
    "/static",
    StaticFiles(directory=Path(__file__).parent / "static", html=True),
    name="cascade_static",
)

class CascadePlugin(AirflowPlugin):
    name = "cascade"
    fastapi_apps = [{"app": cascade_app, "url_prefix": "/cascade", "name": "Cascade API"}]
    react_apps = [{
        "name": "Cascade",
        "bundle_url": "/cascade/static/cascade.umd.cjs",
        "destination": "nav",
        "url_route": "cascade",
    }]
```

**Integration:** `plugins/` is auto-registered by Airflow. `include/` is importable.

**Invariants:** `react_apps[0]["name"]` is `"Cascade"` and the Vite UMD global name in Step 11 must match it **exactly** — that string is the `globalThis` key the loader reads.

**Edge cases:** `/cascade/orchestration/*` must degrade to a clear error rather than fabricating counters if the Airflow API is unreachable. A rail that invents numbers is precisely the failure mode the integrity rule forbids.

**Verify:** `curl localhost:8080/cascade/campaigns/<id>` returns real rollups. Confirm assumption 2 by opening the browser console on the Airflow UI and running `await fetch('/api/v2/version').then(r=>r.status)` → 200.

---

### Step 10 — React bundle: satisfy the loader contract first

**Files:** `ui/package.json`, `ui/vite.config.ts`, `ui/src/main.tsx`, `ui/tailwind.config.js`

**Change:** before building any screen, get an empty component mounting in Airflow's nav. This proves §2.3.1 end-to-end and de-risks everything after it.

`ui/src/main.tsx`:

```tsx
function Cascade({ dagId, runId, taskId, mapIndex }: PluginProps) {
  return <div id="cascade-root">Cascade</div>;
}
// The loader reads globalThis[react_apps.name]; it must be a plain function.
globalThis.Cascade = Cascade;
globalThis.AirflowPlugin = Cascade;
export default Cascade;
```

`ui/vite.config.ts`:

```ts
export default defineConfig({
  plugins: [react()],
  build: {
    lib: {
      entry: 'src/main.tsx',
      name: 'Cascade',                    // must equal react_apps[0].name
      formats: ['umd'],
      fileName: () => 'cascade.umd.cjs',
    },
    outDir: '../plugins/cascade/static',
    emptyOutDir: true,
    // React is NOT provided by Airflow — bundle it. Do not mark it external.
  },
});
```

`ui/tailwind.config.js` per contract §6.3 — preflight off, prefixed, scoped:

```js
export default {
  content: ['./src/**/*.{ts,tsx}'],
  corePlugins: { preflight: false },
  prefix: 'csc-',
  important: '#cascade-root',
};
```

All markup renders inside `#cascade-root`, which carries its own local reset (box-sizing, font stack, color tokens).

**Integration:** `pnpm build` writes directly into the directory Step 9 mounts.

**Invariants:** the exported value stays a plain function. Wrapping in `memo()` or `forwardRef()` yields an object and the loader throws `TypeError`.

**Edge cases:**

- Bundle served as `text/plain` → dynamic `import()` refused. Step 9's `mimetypes.add_type` prevents it; if the word "Cascade" never appears, check the network response's `Content-Type` first.
- All four props are `undefined` under `destination: "nav"`. The component must not destructure them into required values.
- Tailwind preflight leaking would restyle Airflow's own chrome — visible immediately as broken Airflow navigation.

**Verify:** `pnpm build`, `astro dev restart`, then Cascade appears in Airflow's nav and renders. Console clean. Airflow's own pages visually unchanged.

**If this step cannot be made to work,** switch to contract §6.4: the mounted component renders `<iframe src="/cascade/app" />` and FastAPI serves a standalone SPA there. Everything downstream is unaffected.

---

### Step 11 — Dashboard

**Files:** `ui/src/screens/Dashboard.tsx`, `ui/src/api/client.ts`, `ui/src/components/*`

**Change:** hero metrics (affected accounts, affected ARR, days to sunset, migration completion, blocked accounts, pending exceptions), a stacked status bar, risk distribution, and the campaign header — all fed by `GET /cascade/campaigns/{id}`.

The exceptions tile renders **two** figures per contract §7.4:

> **51 exceptions · 1 awaiting input in Airflow**

The first comes from Cascade's `exception` table; the second from the Airflow API. They are visually distinct and never summed.

**Integration:** `ui/src/api/client.ts` is the single fetch wrapper; same-origin, no auth header (assumption 2).

**Invariants:** contract §3.7 — no number is a literal. Count-up animations interpolate toward a fetched value; they never invent one.

**Edge cases:** before the hero run exists, the campaign endpoint 404s. Render an empty state, not zeros — zeros read as real data.

**Verify:** change the fixture population, re-run the generator and hero run, and confirm every displayed figure changes with no frontend edit. This is the direct test of §3.7.

---

### Step 12 — Blast-radius table and account drawer

**Files:** `ui/src/screens/AccountTable.tsx`, `ui/src/screens/AccountDrawer.tsx`

**Change:** virtualized table over `GET /cascade/campaigns/{id}/accounts` with server-side filtering. Columns: account, ARR, risk, v1 usage, v2 adoption, CSM, status, blocker. Filter chips implement the §3.6 derived queries — passed as a `chip` parameter and resolved server-side, so chip definitions live in one place.

Every dashboard aggregate is clickable into the population that produced it.

The drawer shows ARR/tier/owner, the v1/v2 trend sparkline drawn from `daily_v1`/`daily_v2`, blocker, contract flag, the generated brief (when `brief_source` is set), evidence, and the timeline from `GET /cascade/campaigns/{id}/timeline?account_id=`.

**Integration:** row click opens the drawer; drawer carries a **View Orchestration** action built from `latest_airflow_task_instance`.

**Invariants:** filtering happens server-side. Fetching 2,417 rows to filter in the browser would work and would also make every count a frontend computation, which §3.7 forbids.

**Edge cases:** virtualize the list — 2,417 unvirtualized rows will make the demo scroll badly. Sparklines for accounts with no v2 traffic must render a flat line, not collapse.

**Verify:** each chip's count matches the §3.5 figures. Clicking the dashboard's blocked-accounts metric lands on a filtered table showing 51.

---

### Step 13 — DAG B: `exception_resolution` and real HITL

**Files:** `dags/exception_resolution.py`, `plugins/cascade/api/routes/exceptions.py`, `ui/src/components/HITLModal.tsx`

**Change:** the DAG, triggered with `conf={"account_id": ...}`:

```python
@task
def load_account(**context) -> dict: ...

@task
def build_review_packet(account: dict) -> dict:
    """ARR, deadline, v1/v2 traffic, blocker, contract flag, brief, options."""

decide = HITLOperator(
    task_id="await_decision",
    subject="Migration exception: {{ params.account_name }}",
    body="{{ ti.xcom_pull(task_ids='build_review_packet')['summary'] }}",
    options=[
        "Grant extension to November 15",
        "Keep October 31 deadline",
        "Escalate for legal review",
    ],
    params={"reason": Param("", type="string", title="Reason for decision")},
    # assigned_users deliberately unset — restricting it 403s the demo submission
)

@task
def apply_decision(**context) -> dict:
    """Read {"chosen_options", "params_input"} from XCom, update the exception row."""

@task
def write_timeline_event(decision: dict) -> None: ...
```

DAG A gains a final `trigger_acme_exception` using `TriggerDagRunOperator` with `conf={"account_id": "acme_logistics"}`, so exactly one exception DAG run exists.

`POST /cascade/exceptions/{id}/respond` proxies server-side to:

```
PATCH /api/v2/dags/exception_resolution/dagRuns/{run_id}/taskInstances/await_decision/-1/hitlDetails
{"chosen_options": ["Grant extension to November 15"], "params_input": {"reason": "..."}}
```

then polls the task instance until it leaves `awaiting_input` and returns the resulting state. The endpoint updates the `exception` row only from the observed post-response state.

`HITLModal.tsx` renders the options in business language and submits to that endpoint.

**Integration:** the modal opens from the account drawer when `hitl_task_id` is non-null.

**Invariants:**

- The timeline event is written by `write_timeline_event` inside the resumed DAG — **never** by the endpoint or the frontend. This is the demo's proof that the workflow actually resumed.
- `map_index` is `-1` for the unmapped HITL task.
- Only exceptions with a non-null `hitl_task_id` may render a decision control (§7.4).

**Edge cases:** `assigned_users` set to anything not matching the logged-in user returns 403. Submitting twice must be handled — the second `PATCH` should surface an already-responded error rather than appearing to succeed.

**Verify:** trigger the DAG; confirm `await_decision` shows `awaiting_input` in the Airflow UI and holds no worker slot. Submit from the Cascade modal; the task moves to `success`, downstream tasks run, and a timeline event appears whose `airflow_task_id` is `write_timeline_event`. Contract §7.5's narrative note: `apply_decision` writes the extension-granted event, and day 7 later adds the telemetry-confirmed event.

---

### Step 14 — Common AI migration brief

**Files:** `dags/product_change_assessment.py` (extend), `include/cascade/briefs.py`

**Change:**

```python
class MigrationBrief(BaseModel):
    account_id: str
    summary: str
    blockers: list[str]
    proposed_next_step: str
    evidence_refs: list[str]

@task.llm(
    llm_conn_id="cascade_llm",
    output_type=MigrationBrief,
    system_prompt="You explain migration evidence. You never decide migration status.",
)
def generate_migration_brief(evidence: dict) -> str:
    """Returns the PROMPT — the decorator makes the call."""
    return f"Account {evidence['account_name']} ... {evidence['signals']}"
```

Mapped over `select_high_risk`'s output (≤8 accounts). On any exception, write a deterministic template brief and set `brief_source='deterministic'`; on success set `brief_source='llm'`.

**Integration:** writes `account_migration.brief` and `.brief_source`. The UI renders an AI provenance badge only when `brief_source == 'llm'`.

**Invariants:**

- Status computation never reads the brief. A model failure must not change a single account's status.
- The model explains and proposes; it never decides. `MIGRATED` comes from `rules.py` alone.
- Normalize the XCom value with `MigrationBrief.model_validate(...)` — per §2.3.4 it may arrive as a dict or a model instance.

**Edge cases:** missing `cascade_llm` connection → deterministic path, run still succeeds. Model returns malformed structured output → pydantic-ai raises → deterministic path.

**Verify:** run once with the connection removed and confirm the DAG succeeds with `brief_source='deterministic'` and no AI badge in the UI. Then restore it and confirm a real brief with the badge. Contract §8.2's dev→demo swap is a `.env` change only; verify by swapping and re-running with no code edit.

---

### Step 15 — DAG C: `migration_verification` and the live wave

**Files:** `dags/migration_verification.py`, `plugins/cascade/api/routes/scenario.py`

**Change:**

```python
@task
def load_telemetry_watermark() -> str:
    """Last processed snapshot id, from Airflow Task/Asset State Store."""

@task
def find_accounts_with_changed_usage(watermark: str) -> list[str]:
    """Compare current snapshot against watermark. Returns ~84 ids."""

@task
def verify_account(account_id: str) -> dict:
    """Recompute status via rules.compute_status; upsert; append timeline event."""

@task
def save_telemetry_watermark(snapshot: str) -> None: ...
```

`verify_account.expand(account_id=changed)` maps only the delta. `aggregate_campaign` is the shared implementation from Step 7.

`POST /cascade/scenario/advance` proxies to the mock service, then triggers this DAG and returns the new `dag_run_id`. The UI polls `GET /cascade/orchestration/{campaign_id}`, which reads `listMapped` for live counters.

**Integration:** the live rail in the UI displays mapped/running/success/awaiting-input counts sourced from the Airflow API.

**Invariants:**

- Only changed accounts are mapped. Mapping all 2,417 would still be correct but destroys the "Airflow discovers the delta" point.
- Only small control state goes in State Store — a snapshot id. Account state stays in Postgres.
- Rail animation happens only when backend numbers change. No synthetic progress.

**Edge cases:** advancing twice must produce an empty or tiny delta, not a re-run of 84. Rail polling must stop when the run reaches a terminal state.

**Verify:** advance to day 7; confirm exactly 84 mapped `verify_account` instances in the Airflow UI, Acme's status becomes `MIGRATED`, and its timeline gains an event written by `verify_account`. Confirm rail counters track the Airflow UI's own numbers.

---

### Step 16 — Orchestration reveal and demo path

**Files:** `include/cascade/airflow_links.py`, `ui/src/components/ViewOrchestration.tsx`

**Change:** deep-link builders producing native Airflow UI URLs from stored identifiers — campaign → hero DAG run grid; account → its `latest_airflow_task_instance` mapped view; exception → the HITL task instance.

`ViewOrchestration` is a persistent action in the Cascade header and is also present in the account drawer and HITL modal.

**Integration:** reads `campaign.airflow_dag_run_id` and `account_migration.latest_airflow_task_instance` — both written by real tasks in Steps 7 and 15.

**Invariants:** links resolve to the *exact* run and task backing the view the user is on, not a generic DAG page. This is the judge moment and a generic link undercuts it.

**Edge cases:** confirm the Airflow 3 UI route shape for a mapped task instance against the running instance before hardcoding a pattern; adjust if it differs.

**Verify:** from Acme's drawer, **View Orchestration** opens Acme's own mapped `assess_account` instance with its logs — not the DAG list.

---

## 7. Testing

### Unit — `pytest tests/`

- `test_rules.py` (Step 3): every row of the §3.1 status table; `compute_risk` boundaries; the streak boundary in both directions; empty and short series; telemetry-outranks-blocker.
- `test_aggregate.py`: `aggregate_campaign` is idempotent — running twice over the same data produces identical rollups.
- `test_store.py`: `upsert_account_migration` is idempotent and concurrency-safe.

### Fixture integrity — `python scripts/generate_fixtures.py`

Self-asserting per Step 4: distributions, chip counts, day-7 delta size, Acme's day-7 status, byte-identical regeneration.

### Integration — against the running stack

- `python scripts/prepare_hero_run.py` asserts >1,000 successful mapped instances and a distribution matching §3.5.
- HITL round trip: trigger DAG B, assert `awaiting_input`, submit via `/cascade/exceptions/{id}/respond`, assert the task reaches `success` and the timeline event's `airflow_task_id` is `write_timeline_event`.
- Verification wave: advance, assert exactly 84 mapped instances and Acme `MIGRATED`.
- Degradation: unset `AIRFLOW_CONN_CASCADE_LLM`, re-run DAG A, assert success with `brief_source='deterministic'`.

### Negative and regression

- Six zero-days must **not** yield `MIGRATED` (the §4.4 trap).
- A failed `assess_account` instance must not fail the hero run.
- Advancing to an unknown snapshot returns 400.
- Double HITL submission surfaces an error rather than a false success.
- No frontend source file contains a hardcoded population, ARR, or distribution figure:

```bash
grep -rnE "2,?417|82\.4|1,?684|2417" ui/src/ || echo "clean"
```

### Full-path rehearsal

`python scripts/reset_demo.py && python scripts/prepare_hero_run.py`, then walk the technical design §16 demo path start to finish. Repeat from reset to confirm repeatability.

---

## 8. Risks

| Risk | Detection | Mitigation |
|---|---|---|
| Bundle loads but never mounts | "Cascade" absent from nav; console `TypeError: Expected function, got object` | Export a plain function, not `memo`/`forwardRef`. Confirm the UMD `name` equals `react_apps[0].name` exactly. |
| Bundle served as `text/plain` | Network tab shows wrong `Content-Type`; import silently fails | `mimetypes.add_type("application/javascript", ".cjs")` before the `StaticFiles` mount (Step 9). |
| Tailwind preflight leaks into Airflow | Airflow's own nav visibly restyled | `corePlugins.preflight: false`, `prefix`, `important: '#cascade-root'`. Check Airflow's pages, not just Cascade's, after the first build. |
| Cookie auth doesn't hold | `401` from `/api/v2/*` inside the plugin | Assumption 2's fallback: proxy every Airflow call through `/cascade/*` server-side. |
| Fan-out throttled to 16 | Hero run crawls; ~16 concurrent instances in the grid | `AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64` — the default, not `max_map_length`, is the real cap. |
| Streak off-by-one | Acme lands `READY_TO_VERIFY` at day 7 | The Step 3 test asserts both sides of the boundary before any fixture exists. |
| Concurrent upsert clobbering | Account rows with mixed-generation values | `ON CONFLICT DO UPDATE`, never read-then-write. |
| Rail invents state | Counters move without Airflow moving | Rail reads `listMapped` only; error state on API failure rather than a fallback number. |
| Exception count reads as fake | 51 "awaiting input" against 1 real HITL | §7.4's split tile; decision controls gated on non-null `hitl_task_id`. |
| Model latency or failure during recording | Brief missing or slow | Briefs are precomputed by the hero run, not generated live in the demo path; deterministic degradation keeps the DAG green. |

---

## 9. Done when

- [ ] `astro dev start` brings up Airflow 3.3.1, Postgres with a `cascade` database, and the mock service.
- [ ] `pytest tests/` passes, including both sides of the §4.4 streak boundary.
- [ ] `python scripts/generate_fixtures.py` is byte-reproducible and its distribution assertions pass.
- [ ] Cascade appears in the Airflow nav as a React app, with Airflow's own UI visually unchanged.
- [ ] The hero `product_change_assessment` run has >1,000 mapped `assess_account` instances in `success`, and `discover_affected_accounts` computed that population at runtime.
- [ ] `grep -rnE "2,?417|82\.4|1,?684" ui/src/` returns nothing; changing the fixture population changes every displayed figure with no frontend edit.
- [ ] Every filter chip's count matches §3.5, and every dashboard aggregate is clickable into its population.
- [ ] `await_decision` reaches `awaiting_input` in the Airflow UI.
- [ ] Submitting from the Cascade modal resumes the real task, and the timeline event's `airflow_task_id` is `write_timeline_event`.
- [ ] The exceptions tile shows product exceptions and real Airflow HITL as separate figures.
- [ ] `POST /cascade/scenario/advance` advances the mock service and triggers a real verification run mapping exactly the changed accounts.
- [ ] Acme reaches `MIGRATED` through `rules.compute_status` over a computed streak.
- [ ] Live rail counters match the Airflow UI's own numbers and never animate without a backend change.
- [ ] **View Orchestration** from Acme's drawer opens Acme's own mapped task instance.
- [ ] Any AI badge shown corresponds to a row with `brief_source='llm'`; removing the connection still yields a green DAG run.
- [ ] `scripts/reset_demo.py` restores day 0, and the full demo path runs clean from a reset.
- [ ] No Airflow run or task state displayed anywhere in Cascade is synthesized.
