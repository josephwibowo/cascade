# Cascade — Build Contract

**Status:** Decisions locked, implementation-ready
**Purpose:** Resolve the open decisions in the product and technical docs so the build can be planned and executed in one pass.
**Scope basis:** Decisions here are made on technical and product merits. Schedule, effort, and resource constraints are deliberately out of scope and must not shape the design.

## 0. Precedence

Three documents govern this build:

1. `Cascade_Product_Hypothesis_and_Demo.md` — narrative, scenario, user, story beats.
2. `Cascade_Demo_Technical_Design.md` — architecture intent, real/mocked boundary, cut order.
3. **This document** — binding technical decisions.

**Where they conflict, this document wins.** The other two are the "why" and "what"; this is the "exactly how." Sections below marked *(supersedes)* explicitly overrule earlier text.

## 1. Locked decisions

| # | Decision | Resolution |
|---|---|---|
| 1 | Runtime | Astro CLI on Astro Runtime `3.3-7` (Airflow 3.3.1) |
| 2 | Metadata DB / executor | Postgres + LocalExecutor (Astro defaults) |
| 3 | Cascade product DB | Postgres, separate database on the same Astro Postgres container — **not SQLite** |
| 4 | State model | Orthogonal: `status` (lifecycle) + `segment` + `risk` + `blocker_type` |
| 5 | Plugin embedding | Native `react_apps` bundle mount, iframe as documented fallback |
| 6 | Create-campaign UI flow | **Cut.** Hero campaign seeded by script |
| 7 | Hero population | 2,417, held. All displayed figures computed, never literal |
| 8 | Common AI | pydantic-ai connection swap: free OpenRouter model in dev, Anthropic/OpenAI for the recording |
| 9 | Mock backend | Standalone mock FastAPI service, reached over HTTP by the DAGs |

## 2. Environment

### 2.1 Image and project shape

Astro projects are not `pyproject.toml` projects. *(Supersedes §17 of the technical design.)*

```dockerfile
FROM astrocrpublic.azurecr.io/runtime:3.3-7
```

`requirements.txt`:

```
apache-airflow-providers-common-ai[anthropic,openai]
psycopg[binary]
sqlalchemy
pydantic
```

Verify at first build whether Astro Runtime 3.3-7 already bundles `common-ai`; if so, drop the explicit pin and keep only the extras.

### 2.2 Airflow configuration

These go in `.env`. The technical design named only `max_map_length`, which is **not** the binding constraint — `max_active_tasks_per_dag` defaults to 16 and would throttle the hero fan-out to 16-wide regardless of everything else.

```bash
AIRFLOW__CORE__MAX_MAP_LENGTH=4096
AIRFLOW__CORE__PARALLELISM=64
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=64
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG=4
AIRFLOW__SCHEDULER__MAX_TIS_PER_QUERY=64
```

Verified defaults in Airflow 3.3.1: `max_map_length` 1024, `parallelism` 32, `max_active_tasks_per_dag` 16.

### 2.3 Cascade database

Create `cascade` as a second database on the Astro Postgres container. Connection via `CASCADE_DB_URL` in `.env`. Schema managed by a single idempotent `scripts/init_db.py` — no Alembic, no migrations framework.

Rationale for not using SQLite: 64 concurrent mapped tasks writing account rows will produce `database is locked` on SQLite. This is not a preference, it is a hard constraint given decision 2.2.

## 3. State model *(supersedes both docs)*

The product doc's six-bucket table and the technical doc's five-state lifecycle are two different vocabularies describing overlapping things. They are replaced by four independent columns.

### 3.1 `status` — lifecycle, telemetry-driven

Exactly one of:

```
NOT_STARTED | IN_PROGRESS | BLOCKED | READY_TO_VERIFY | MIGRATED
```

This is the only column verification changes. Evaluated first-match-wins, in this order:

| Order | Status | Rule |
|---|---|---|
| 1 | `MIGRATED` | `v1_calls_today == 0` **and** `zero_v1_streak_days >= 7` **and** (`v2_calls_7d > 0` or `never_needed_replacement`) |
| 2 | `READY_TO_VERIFY` | `v1_calls_today == 0` **and** `1 <= zero_v1_streak_days < 7` |
| 3 | `BLOCKED` | `blocker_type is not null` |
| 4 | `IN_PROGRESS` | `v2_calls_7d > 0` **and** `v1_calls_7d` is falling vs. the prior window |
| 5 | `NOT_STARTED` | otherwise |

Ordering matters: telemetry outranks blockers. An account with a technical blocker that has nonetheless stopped calling v1 has demonstrably progressed, and the demo depends on exactly this — Acme is `BLOCKED` at day 0 and `MIGRATED` at day 7 without anyone clearing the blocker flag.

### 3.2 `segment` — business classification, stable

Exactly one of:

```
STANDARD | STRATEGIC | CONTRACTUAL | TECHNICAL_BLOCKER
```

Set at assessment, not changed by verification. Drives brief selection and the exception queue.

### 3.3 `risk` — derived, deterministic

```
CRITICAL  arr >= 1_000_000 and status in (BLOCKED, NOT_STARTED)
HIGH      arr >= 1_000_000, or (arr >= 250_000 and status == BLOCKED)
MEDIUM    arr >= 250_000, or status == BLOCKED
LOW       otherwise
```

### 3.4 `blocker_type` — nullable

```
CUSTOM_PARSER | SDK_PINNED | CONTRACT_COMMITMENT | NO_OWNER | null
```

### 3.5 Day-0 target distribution

The fixture generator must produce exactly this, and `scripts/generate_fixtures.py` asserts it:

**By status:** `NOT_STARTED` 1,871 · `IN_PROGRESS` 495 · `BLOCKED` 51 · `READY_TO_VERIFY` 0 · `MIGRATED` 0 = **2,417**

**By segment:** `STANDARD` 2,292 · `STRATEGIC` 74 · `CONTRACTUAL` 38 · `TECHNICAL_BLOCKER` 13 = **2,417**

`BLOCKED` 51 is exactly the CONTRACTUAL 38 + TECHNICAL_BLOCKER 13 accounts. The 74 STRATEGIC accounts are spread across NOT_STARTED and IN_PROGRESS — strategic is a business flag, not an impediment.

### 3.6 UI filter chips are derived queries, not statuses

The product doc's buckets survive as filters:

| Chip | Query | Day-0 count |
|---|---|---|
| Straightforward | `segment = STANDARD and status = NOT_STARTED` | 1,684 |
| Actively migrating | `status = IN_PROGRESS` | 495 |
| No progress | `status = NOT_STARTED and v1_calls_7d > 0 and v2_calls_7d = 0` | 187 |
| Strategic | `segment = STRATEGIC` | 74 |
| Contractual | `segment = CONTRACTUAL` | 38 |
| Technical blocker | `segment = TECHNICAL_BLOCKER` | 13 |

### 3.7 No hardcoded figures

**Every number rendered in the UI is computed from the Cascade DB at read time.** No account count, ARR total, percentage, or distribution figure may be a literal in frontend code, fixture JSON, or narration cue cards. The demo script's spoken numbers get re-read off the finished dashboard, not the other way round.

This is an integrity rule, not a convenience: a literal in the frontend is indistinguishable from a fabricated one, and the whole demo rests on the claim that displayed state is derived state. It also means the fixture population can be regenerated at any size without a copy rewrite.

## 4. Fixtures and the seven-day rule

### 4.1 The gap this closes

The success rule requires "zero v1 traffic for seven consecutive days," but the world advances day-0 → day-7 in one snapshot flip, and the proposed usage schema (`calls_7d`, `calls_today`, `replacement_calls_today`) cannot express a streak. Without this section the closing beat of the demo has no mechanism.

### 4.2 Usage fixture schema

Per account, per snapshot:

```json
{
  "account_id": "acct_00417",
  "snapshot_id": "day7",
  "daily_v1": [0, 0, 0, 0, 0, 0, 0],
  "daily_v2": [1180, 1240, 1310, 1290, 1355, 1402, 1388],
  "endpoints": ["/v1/events", "/v1/events/batch"],
  "sdk_name": "beacon-python",
  "sdk_version": "2.4.1"
}
```

`daily_v1` and `daily_v2` are trailing 7-day series, oldest first, ending on the snapshot day.

### 4.3 The streak is computed, never asserted

`zero_v1_streak_days` is **not** a fixture field. `verify_account` computes it from `daily_v1` in real Python:

```python
def zero_v1_streak(daily_v1: list[int]) -> int:
    streak = 0
    for calls in reversed(daily_v1):
        if calls != 0:
            break
        streak += 1
    return streak
```

This keeps the success rule real code operating on real evidence rather than a fixture handing over the answer.

### 4.4 The off-by-one that would break the demo

Define the streak as *consecutive zero days ending on and including the snapshot day.*

Acme at day 0 has ~14,000 v1 calls/day. For Acme to reach `MIGRATED` at the day-7 snapshot, its `daily_v1` at day 7 must be `[0,0,0,0,0,0,0]` — meaning it went to zero on day 1 and stayed there through day 7. That is seven consecutive zero days. The day-0 snapshot still shows traffic.

Get this wrong by one and Acme lands on `READY_TO_VERIFY` instead of `MIGRATED`, and the demo's final beat silently fails. Assert it in a unit test before building anything else.

### 4.5 Day-7 delta

Exactly **84 accounts** have changed telemetry at day 7. This matches the live-rail example in the technical design and sizes the live fan-out correctly for the recording.

Composition: Acme → `MIGRATED`, ~30 others → `MIGRATED`, ~40 → `READY_TO_VERIFY`, ~13 → `IN_PROGRESS`. Tune during fixture generation so the dashboard shows visible movement in every band.

### 4.6 Determinism

Single seed constant in `include/cascade/fixtures.py`. `scripts/generate_fixtures.py` regenerates both snapshots and asserts the §3.5 distribution and the §4.5 delta. Regeneration must be byte-identical across runs.

### 4.7 Hand-authored accounts

Acme Logistics ($2.4M ARR, `TECHNICAL_BLOCKER`, `CUSTOM_PARSER`, contract commitment through Oct 31, named CSM), plus one clean strategic migration, one no-progress account, and one already-mostly-migrated account. Everything else generated.

### 4.8 Mock service contract *(restores §10 of the technical design)*

Fixtures are served by a standalone FastAPI service, reached by the DAGs over HTTP through a thin client in `include/cascade/mock_client.py`. The DAGs do not read fixture files directly.

This keeps the real/mocked boundary a visible seam rather than an import: the mocked systems are addressable services, the DAGs interact with them the way they would with real vendor APIs, and "fake the world, never fake the orchestration" becomes inspectable instead of asserted.

Endpoints per the technical design §10, with the usage schema replaced by §4.2:

```
GET  /usage/accounts        # daily_v1 / daily_v2 series, per §4.2
GET  /crm/accounts          # account_name, arr, tier, csm, region
GET  /contracts/accounts    # compatibility_commitment, commitment_expiry
GET  /migration/change      # campaign id, legacy/replacement endpoints, deadline
POST /scenario/advance      # {"snapshot": "day7"} — atomic world switch
```

`POST /scenario/advance` owns the day-0 → day-7 transition. The switch is atomic from the service's perspective: it swaps the served snapshot under a lock, so no DAG run can observe a half-advanced world. Cascade's `POST /cascade/scenario/advance` proxies to it and then triggers `migration_verification`.

The service runs as an additional container alongside Astro. Bulk endpoints must support fetching the full population in one call so §5.2 holds.

## 5. Scale plan

### 5.1 Hero population

2,417 accounts, held. The figure appears throughout the narrative and the distribution in §3.5 is built around it.

The hero run is executed and completed before recording, so its wall-clock duration is not a demo constraint. If the run is slower than comfortable for iteration, fix it by raising concurrency (§2.2) and lightening per-task work (§5.2) — not by shrinking the population.

### 5.2 Keep mapped work light

`assess_account` does no LLM call and one DB upsert. Account data is fetched from the mock service in bulk by `discover_affected_accounts` and passed through the mapped argument, so each mapped task performs at most one narrow lookup rather than a full API round trip per account.

Per-task cost should be milliseconds of real work plus Airflow's scheduling overhead. The latter will dominate at this population size, which is the honest shape of a large mapped run and nothing to engineer around.

### 5.3 Live wave

84 mapped tasks at 64-wide concurrency. This is the only fan-out that runs during the recording, and it is sized to complete within the demo beat rather than to prove scale — §5.1 already proved scale.

## 6. Plugin architecture

### 6.1 Extension points (verified against Airflow 3.3.1)

```python
from airflow.plugins_manager import AirflowPlugin
from cascade.api.app import cascade_app

class CascadePlugin(AirflowPlugin):
    name = "cascade"
    fastapi_apps = [{
        "app": cascade_app,
        "url_prefix": "/cascade",
        "name": "Cascade API",
    }]
    react_apps = [{
        "name": "Cascade",
        "bundle_url": "/cascade/static/cascade.js",
        "destination": "nav",
        "url_route": "cascade",
    }]
```

`react_apps` requires `name`, `bundle_url`, `destination`, `url_route`. Valid destinations include `nav`, `dag`, `dag_run`, `task`, `task_instance`, `base`, and existing pages. The bundle is served by our own FastAPI app via a `StaticFiles` mount at `/cascade/static`, written there by the Vite build.

**The `react_apps` interface is documented as experimental.** Treat the exact bundle entry contract as unverified — see §10.

### 6.2 Authentication — no token plumbing needed

Airflow 3's backend falls back to the JWT in the session cookie when a request carries no auth header. Same-origin `fetch('/cascade/...')` and `fetch('/api/v2/...')` from the mounted bundle authenticate as the logged-in UI user with no client-side token handling.

Confirm this with one live call before building against it. If it does not hold, the fallback is server-side proxying: the React app talks only to `/cascade/*`, and Cascade's FastAPI calls the Airflow API using its own credentials.

### 6.3 Styling under the embed constraint

The bundle mounts into Airflow's DOM and shares its stylesheet. Tailwind's preflight will restyle Airflow's own chrome if left on.

```js
// tailwind.config.js
export default {
  corePlugins: { preflight: false },
  prefix: 'csc-',
  important: '#cascade-root',
}
```

All Cascade markup renders inside `#cascade-root`, which carries an explicit local reset (box-sizing, font stack, color tokens). shadcn/ui components must be generated with the prefix applied.

### 6.4 Fallback path

If the native bundle mount proves unworkable against the experimental interface — not merely fiddly, but blocked — switch to the iframe approach: the mounted bundle renders a single full-bleed `<iframe src="/cascade/app">` and FastAPI serves a standalone SPA page there. Total style isolation and a normal Vite dev experience, at the cost of a slightly weaker "embedded in Airflow" claim. Deep links out to Airflow use `target="_top"`.

Prefer the native mount. CSS collisions are a solved problem (§6.3); an interface that cannot express the app is not.

## 7. HITL contract

### 7.1 Trigger

`product_change_assessment` ends with a `trigger_acme_exception` task using `TriggerDagRunOperator` to start `exception_resolution` with `conf={"account_id": "acme_logistics"}`. Exactly one exception DAG run exists. *(Resolves an unspecified trigger path; consistent with §20's cut of multiple simultaneous HITL exceptions.)*

### 7.2 Waiting state

As of Airflow 3.3, HITL tasks use a scheduler-managed `awaiting_input` state rather than deferring onto the triggerer. The task holds neither a worker slot nor the triggerer. **No triggerer configuration is required** — this removes a risk the technical design implicitly carried.

### 7.3 Response path

Cascade's FastAPI proxies the response server-side so the decision, the reason, and the `exception` row update happen in one handler:

```
PATCH /api/v2/dags/{dag_id}/dagRuns/{dag_run_id}/taskInstances/{task_id}/{map_index}/hitlDetails
{"chosen_options": ["Grant extension to November 15"], "params_input": {"reason": "..."}}
```

`map_index` is `-1` for the unmapped HITL task.

**Do not set `assigned_users`** on the HITL operator. If set, only listed users may respond and the demo submission returns 403.

### 7.4 Exception vs. HITL integrity rule

51 accounts carry `segment` in (CONTRACTUAL, TECHNICAL_BLOCKER). Exactly one has a real Airflow HITL task. Showing "51 awaiting input" would violate the no-fake-state rule.

- `exception` rows are Cascade product state, written by real tasks. Showing 51 of them is honest.
- Only a row with non-null `hitl_task_id` may render Airflow HITL status, an `awaiting_input` badge, or a decision control.
- The dashboard tile reads: **"51 exceptions · 1 awaiting input in Airflow"**, with the second figure sourced from the Airflow API.
- The other 50 render as "needs review," with no Airflow state claimed.

### 7.5 Narrative note

Granting Acme an extension to Nov 15 and then having it migrate within 7 days reads oddly unless the timeline says why. `apply_decision` writes two timeline events: the extension grant, then (at day 7) telemetry confirmation. Narration: the extension removed the deadline pressure, the customer shipped their parser fix, and telemetry proved it — nobody clicked "done."

## 8. Common AI contract

### 8.1 Mechanism

`common.ai` is built on pydantic-ai. **The model is chosen by the Airflow connection `llm_conn_id` points at, not by DAG code.** Switching from a free dev model to a strong demo model is a connection change with zero code impact — exactly the dev/demo split required.

### 8.2 Connections

Dev (free, OpenRouter via its OpenAI-compatible endpoint):

```bash
AIRFLOW_CONN_CASCADE_LLM='{"conn_type":"pydanticai","host":"https://openrouter.ai/api/v1","password":"<openrouter_key>","extra":{"model":"openai:<free-model-id>"}}'
```

Recording (strong):

```bash
AIRFLOW_CONN_CASCADE_LLM='{"conn_type":"pydanticai","password":"<anthropic_key>","extra":{"model":"anthropic:claude-sonnet-5"}}'
```

Connection fields: Host = `base_url`, Password = API key, `extra.model` = model identifier. A `model_id` argument on the operator overrides `extra.model`. Confirm the exact Anthropic model id string the installed provider version accepts before the recording.

DAG code references `llm_conn_id="cascade_llm"` and nothing else.

### 8.3 Degradation

If the connection is absent or the call fails, `generate_migration_brief` writes a deterministic template brief and sets `brief_source='deterministic'`. The UI renders the AI provenance badge **only** when `brief_source='llm'`. The migration-state calculation never depends on the brief.

This satisfies the acceptance criterion that any claimed Common AI behavior is backed by a real task: if the model did not run, the demo makes no AI claim.

### 8.4 Fan-out

5–10 accounts, selected by `select_high_risk` from `segment != STANDARD` ordered by ARR. Never the full population.

## 9. Repository layout *(supersedes §17)*

```text
cascade/
├── Dockerfile                    # FROM astrocrpublic.azurecr.io/runtime:3.3-7
├── requirements.txt
├── .env                          # AIRFLOW__*, AIRFLOW_CONN_CASCADE_LLM, CASCADE_DB_URL
├── dags/
│   ├── product_change_assessment.py
│   ├── exception_resolution.py
│   └── migration_verification.py
├── plugins/
│   └── cascade/
│       ├── __init__.py           # AirflowPlugin
│       ├── api/app.py, routes/
│       └── static/               # built bundle, gitignored
├── include/cascade/              # Astro convention for shared Python
│   ├── models.py                 # SQLAlchemy tables
│   ├── states.py                 # status/segment/risk/blocker enums
│   ├── rules.py                  # §3.1 rules, §4.3 streak — pure functions
│   ├── store.py                  # Cascade DB access
│   ├── fixtures.py               # seeded generator
│   ├── mock_client.py            # HTTP client for the mock service
│   └── airflow_links.py          # deep-link builders
├── ui/                           # Vite + React source
├── mock_services/
│   ├── app.py                    # FastAPI, endpoints per §4.8
│   └── fixtures/
│       ├── api_v1_sunset.json
│       ├── day0_usage.json
│       └── day7_usage.json
├── docker-compose.override.yml   # mock service alongside Astro
├── scripts/
│   ├── init_db.py
│   ├── generate_fixtures.py      # asserts §3.5 and §4.5
│   ├── prepare_hero_run.py
│   └── reset_demo.py             # returns the world to day 0, wipes runs
└── tests/
    └── test_rules.py             # §3.1 table + §4.4 off-by-one
```

`aggregate_campaign` is one function in `include/cascade/`, imported by both DAG A and DAG C, and must be idempotent.

The mock service is wired in via `docker-compose.override.yml`, which Astro merges into its own compose stack, so `astro dev start` brings up the whole world in one command.

## 10. Verify early, do not assume

Three things this document asserts from documentation rather than from a running system. Each must be confirmed against the running environment before code is written on top of it.

1. **`react_apps` bundle entry contract.** The interface is documented as experimental. Read the plugin example in the installed Airflow 3.3.1 source before writing the Vite config — specifically what the bundle must export and how Airflow invokes it. Everything in §6.1 and §6.3 depends on this being what the docs imply.
2. **Cookie auth fallback (§6.2).** One live `fetch('/api/v2/version')` from the mounted bundle settles it. If it fails, §6.2's server-side proxy fallback applies and the frontend never talks to the Airflow API directly.
3. **`common-ai` in Astro Runtime 3.3-7.** If already bundled, drop the explicit requirements pin and keep only the provider extras.

## 11. Acceptance criteria

Supersedes §19 of the technical design. Each is objectively checkable.

- [ ] `astro dev start` brings up Airflow 3.3.1 with Cascade in the nav.
- [ ] `pytest tests/test_rules.py` passes, including the §4.4 streak boundary.
- [ ] `scripts/generate_fixtures.py` reproduces byte-identical fixtures and asserts the §3.5 distribution.
- [ ] The hero `product_change_assessment` run has >1,000 mapped `assess_account` task instances in `success`.
- [ ] No figure in the UI is a literal — changing the fixture population changes every displayed number.
- [ ] Acme's `exception_resolution` task reaches `awaiting_input`.
- [ ] Submitting the decision in Cascade resumes the real task; the timeline event is written by `apply_decision`, not the frontend.
- [ ] The exceptions tile distinguishes 51 product exceptions from 1 real Airflow HITL.
- [ ] `POST /cascade/scenario/advance` advances the mock service and `migration_verification` maps exactly the changed accounts.
- [ ] Acme reaches `MIGRATED` via `rules.py` evaluating computed streak data.
- [ ] The live rail's counters come from the Airflow API, and never animate without a backend change.
- [ ] **View Orchestration** deep-links to the correct DAG run and mapped task view.
- [ ] Any AI badge shown corresponds to a row with `brief_source='llm'`.
- [ ] `scripts/reset_demo.py` returns the world to day 0 and clears prior runs, and the full demo path runs clean from a reset.
- [ ] The recorded path fits inside three minutes.

## 12. Deliberately not decided

- Exact Vite output format and bundle entry shape — depends on §10.1.
- Whether the day-7 delta stays at 84 — tune during fixture generation for visible movement across every band.
- Anthropic model id string — confirm against the installed provider version.
- Visual design specifics beyond §6.3's constraints. The technical design's palette guidance stands.
- Whether to restore the create-campaign flow (decision 6). It was cut because the demo script never shows it, not for effort reasons, so it remains a defensible cut — but building it would make the product model complete end to end and give an honest answer to "can the UI actually start a campaign?"

## 13. Sources

- [Airflow plugins](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html) — `react_apps`, `fastapi_apps`, experimental status
- [Airflow HITL tutorial](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html) — `awaiting_input`, `hitlDetails` endpoint, `assigned_users`
- [Configuration reference 3.3.1](https://airflow.apache.org/docs/apache-airflow/3.3.1/configurations-ref.html) — concurrency defaults
- [Common AI provider](https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/index.html) — pydantic-ai foundation
- [Pydantic AI connection](https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/connections/pydantic_ai.html) — connection fields
- [Astro Runtime release notes](https://www.astronomer.io/docs/runtime/runtime-release-notes) — 3.3-7 → Airflow 3.3.1
