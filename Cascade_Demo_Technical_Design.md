# Cascade — Demo Technical Design

**Status:** Implementation-oriented hackathon design  
**Working name:** Cascade  
**Target Airflow version:** 3.3.1  
**Primary track hypothesis:** Plugin Powerhouse  
**Hero Airflow primitive:** Dynamic Task Mapping

## 1. Technical thesis

Build a polished business application whose execution engine is a real Airflow service.

The demo environment should follow one rule:

> **Fake the world. Never fake the orchestration.**

All external enterprise systems can be deterministic mocks. The following must be real:

- Airflow scheduler/executor,
- DAG runs,
- mapped task instances created with `expand()`,
- task states and retries,
- at least one real HITL pause/resume,
- the Airflow plugin serving the Cascade application,
- the product-state writes created by actual Airflow tasks,
- any Common AI capability claimed in the demo.

The goal is not production completeness. The goal is a three-minute demo that looks like a real SaaS operations product while remaining technically honest.

## 2. Recommended competition strategy

Enter **Plugin Powerhouse**.

Reasoning:

- The differentiated artifact is the domain-specific application embedded in Airflow.
- Dynamic Task Mapping is the primary orchestration behavior.
- HITL is a meaningful supporting capability.
- Common AI can improve a few high-risk account briefs without becoming the product thesis.
- The UI gives the judges a direct "Airflow as a platform" story rather than another DAG-centric demo.

## 3. Real vs mocked boundary

### Real

- Airflow 3.3.1
- scheduler/executor
- metadata DB
- DAG run creation
- Dynamic Task Mapping
- task-instance states
- task logs
- HITL state and response
- React plugin
- FastAPI plugin endpoints
- Cascade demo DB
- one narrow Common AI path if shown
- orchestration links from Cascade back to native Airflow

### Mocked

- API usage telemetry
- CRM/account metadata
- ARR/account tiers
- SDK inventory
- contract flags
- product-change registry
- customer behavior over time
- external emails/Slack/CRM writes
- passage of seven days

### Integrity requirement

Every Airflow status shown in Cascade must come from either:

1. actual Airflow state, or
2. a Cascade product-state record written by an actual Airflow task.

Do not animate fake running tasks or fabricate task-instance state for visual effect.

## 4. High-level architecture

```text
Browser
  |
  v
Airflow UI
  |
  +-- Cascade React plugin
  |      |
  |      v
  |   Cascade FastAPI plugin
  |      |
  |      +--> Cascade product DB
  |      +--> Airflow public/API endpoints as needed
  |
  +-- Native Airflow views

Airflow scheduler/executor
  |
  +-- product_change_assessment DAG
  +-- exception_resolution DAG
  +-- migration_verification DAG
        |
        +--> mock SaaS services / deterministic fixtures
        +--> Cascade product DB
        +--> Common AI for selected high-risk cases
```

The React app is the primary demo surface. Native Airflow is intentionally revealed late in the demo as proof of the execution engine underneath.

## 5. Core Airflow features and why they are load-bearing

### Dynamic Task Mapping — hero feature

The affected account population is discovered at runtime.

```python
accounts = discover_affected_accounts(change)
assess_account.expand(account_id=accounts)
```

Why it matters:

- the number of accounts is not known until the campaign runs,
- each account gets independent execution state,
- failures/retries are scoped to the account work unit,
- the fan-out is visible in Airflow,
- the business application can project that execution into a customer migration control plane.

### HITL — real exception handling

At least one hero account should reach a real Airflow HITL operator.

The workflow should pause because the case is genuinely ambiguous, not because the demo needs an approval button.

Example:

```text
Acme has a compatibility commitment + technical blocker
    -> exception_resolution DAG
    -> HITLEntryOperator / HITLBranchOperator
    -> awaiting_input
    -> reviewer responds from Cascade UI
    -> DAG resumes
```

### State Store — small control state only

Use Airflow Task/Asset State Store only for small orchestration checkpoints such as:

- last processed telemetry snapshot/event ID,
- last verification watermark,
- campaign control metadata if useful.

Do **not** store the customer migration application model in State Store.

### Common AI — narrow use only

Use Common AI for semantic work where an LLM visibly adds value, for example:

- summarize why a strategic account is blocked,
- translate technical evidence into a concise migration brief,
- propose a migration next step from supplied evidence.

Do not run an LLM across all 2,417 accounts.

Do not let the model determine deterministic statuses such as `MIGRATED`.

### React/FastAPI plugin — product surface

Cascade should be an Airflow plugin that exposes:

- a React application,
- FastAPI endpoints for product reads/actions,
- links back to the exact Airflow run/task state powering the UI.

The plugin is part of the architecture, not just presentation polish.

## 6. DAG A — `product_change_assessment`

### Purpose

Turn one product change into an independently assessed customer migration population.

### Task graph

```text
load_change
    |
    v
discover_affected_accounts
    |
    v
assess_account.expand(account_id=affected_accounts)
    |
    v
select_high_risk
    |
    +--> generate_migration_brief.expand(high_risk_accounts)
    |
    v
aggregate_campaign
```

### Tasks

#### `load_change`

Type: deterministic `@task`

Responsibilities:

- load the Beacon API v1 sunset definition,
- create/update campaign metadata,
- record the Airflow run ID.

#### `discover_affected_accounts`

Type: deterministic `@task`

Responsibilities:

- query mock usage data,
- apply the lookback rule,
- return the list of affected account IDs.

The returned list is the real input to Dynamic Task Mapping.

#### `assess_account`

Type: mapped deterministic `@task`

Responsibilities for one account:

- load usage telemetry,
- load CRM/account metadata,
- load SDK/version clues,
- load contract flags,
- calculate deterministic migration state/risk flags,
- write/update `account_migration` in Cascade DB,
- append timeline events.

This should be fast and deterministic.

#### `select_high_risk`

Type: deterministic reduce task

Responsibilities:

- select a small number of strategic, contractual, or technically blocked accounts for richer semantic analysis.

#### `generate_migration_brief`

Type: mapped `@task.llm` or `@task.agent`

Population: approximately 5–10 accounts in the demo.

Input should be structured evidence gathered by deterministic tasks.

Output should be typed, for example:

```python
class MigrationBrief(BaseModel):
    account_id: str
    summary: str
    blockers: list[str]
    proposed_next_step: str
    evidence_refs: list[str]
```

The model may explain or propose. It does not decide the final status.

#### `aggregate_campaign`

Type: deterministic reduce task

Responsibilities:

- calculate state counts,
- calculate affected ARR summaries,
- write campaign projection rows for the frontend.

## 7. DAG B — `exception_resolution`

### Purpose

Demonstrate scheduler-managed human judgment for one real business exception.

### Hero account

Use Acme Logistics.

### Task graph

```text
load_account
    |
    v
build_review_packet
    |
    v
HITL operator
    |
    v
apply_decision
    |
    v
write_timeline_event
```

### Review packet

Include:

- account name and ARR,
- deadline,
- current v1 traffic,
- current v2 traffic,
- technical blocker,
- compatibility/contract flag,
- generated migration brief,
- recommended decision options.

### HITL options

Keep the demo narrow:

- Grant extension to November 15
- Keep October 31 deadline
- Escalate for legal review

The Cascade UI should render these in business language and submit the response to the real Airflow HITL action.

The underlying Airflow task should genuinely enter `awaiting_input` and resume after the UI action.

## 8. DAG C — `migration_verification`

### Purpose

Prove that migration state changes because new telemetry arrived, not because the UI changed it.

### Task graph

```text
load_telemetry_watermark
    |
    v
find_accounts_with_changed_usage
    |
    v
verify_account.expand(account_id=changed_accounts)
    |
    v
aggregate_campaign
    |
    v
save_telemetry_watermark
```

### `verify_account`

Deterministic rules should calculate states such as:

- `NOT_STARTED`
- `IN_PROGRESS`
- `BLOCKED`
- `READY_TO_VERIFY`
- `MIGRATED`

For the demo, `MIGRATED` should require the seeded seven-day success rule.

### Watermark

Store only a small value such as `last_processed_snapshot=day7` in Airflow State Store or equivalent Airflow control state.

Account-level migration state remains in Cascade DB.

## 9. Dynamic Task Mapping scale strategy

The demo must show scale without making the recording depend on thousands of tasks starting live.

Use two scales.

### Hero campaign

Population: **2,417 accounts**

Purpose:

- prove a genuinely large mapped run exists,
- populate the dashboard,
- provide the visual scale reveal.

Run this before recording using the real Airflow environment.

Airflow's default `max_map_length` is typically 1024, so configure the demo environment to allow at least 2,417 mapped inputs, for example 4096.

### Live verification wave

Population: approximately **50–120 changed accounts**

Purpose:

- create real-time task-state motion during the recording,
- show a second runtime fan-out,
- finish quickly enough for a three-minute demo.

### Concurrency

Use a moderate task concurrency such as 32–64 so the UI visibly progresses without overwhelming the local/demo environment.

### LLM fan-out

Never make 2,417 model calls.

The large population is processed with deterministic code. Only the small high-risk subset receives Common AI analysis.

## 10. Mock backend

Use deterministic local services or fixtures.

Possible implementation:

- FastAPI mock service, or
- local JSON/SQLite fixtures read through a small client abstraction.

A mock HTTP service is more visually convincing if the DAG uses hooks/API-style interactions, but reliability is more important than realism.

### Suggested endpoints

#### `GET /usage/accounts`

Returns fields such as:

- `account_id`
- `endpoint`
- `calls_7d`
- `calls_today`
- `replacement_calls_today`
- `sdk_name`
- `sdk_version`
- `snapshot_id`

#### `GET /crm/accounts`

Returns:

- `account_id`
- `account_name`
- `arr`
- `tier`
- `csm`
- `region`

#### `GET /contracts/accounts`

Returns seeded fields only:

- `account_id`
- `compatibility_commitment`
- `commitment_expiry`

Do not attempt generalized contract parsing.

#### `GET /migration/change`

Returns:

- campaign/change ID,
- legacy endpoints,
- replacement endpoints,
- deadline,
- migration guide metadata.

#### `POST /scenario/advance`

Body example:

```json
{"snapshot": "day7"}
```

Atomically switches the mock world from day-0 telemetry to day-7 telemetry.

## 11. Fixture strategy

Generate most accounts from a fixed random seed so the campaign looks large and varied but remains reproducible.

Hand-author a small set of named accounts, especially:

- Acme Logistics — contractual + technical exception,
- one clean strategic migration,
- one no-progress account,
- one account already mostly migrated.

The fixture generator should produce the same 2,417-account population every run.

Never claim synthetic counts or ARR values as real market statistics.

## 12. Cascade product-state model

Use a small dedicated DB, likely SQLite for hackathon simplicity or Postgres if already available in the stack.

### `campaign`

Fields:

```text
id
name
change_type
deadline
airflow_dag_run_id
status
affected_accounts
affected_arr
created_at
updated_at
```

### `account_migration`

Fields:

```text
campaign_id
account_id
account_name
arr
risk
status
legacy_usage
replacement_usage
owner
blocker_type
latest_airflow_task_instance
updated_at
```

### `exception`

Fields:

```text
campaign_id
account_id
exception_type
airflow_dag_run_id
hitl_task_id
status
decision
decision_reason
resolved_at
```

### `timeline_event`

Fields:

```text
campaign_id
account_id
event_type
timestamp
summary
source
airflow_run_id
airflow_task_id
```

### State ownership rule

- Airflow metadata DB owns orchestration state.
- Cascade DB owns business/product projections.
- XCom owns temporary task-to-task payloads.
- State Store owns tiny cross-run checkpoints.

Do not use XCom as the application database.

## 13. Frontend design

The UI should make the application feel like a SaaS operations control plane before revealing the DAG.

### Visual direction

Aim for a polished command-center interface, not an Airflow-themed table dump.

Suggested visual system:

- deep navy/charcoal background,
- electric purple/cyan accents,
- green for migrated,
- amber for pending,
- red for blockers,
- large numerical metrics,
- subtle motion tied only to real backend state changes.

Possible stack:

- Vite + React,
- Tailwind,
- shadcn/ui,
- Recharts,
- Framer Motion.

Do not over-invest in a custom design system if shadcn components can get the UI to polished quickly.

## 14. Frontend screens

### Campaign dashboard

Hero metrics:

- `2,417 affected accounts`
- `$82.4M affected ARR`
- `58 days to sunset`
- migration completion percentage
- blocked accounts
- pending human exceptions

Visuals:

- large count-up metric,
- stacked migration-state bar,
- risk distribution,
- migration progress over simulated time.

### Blast-radius account table

Show:

- account,
- ARR,
- risk,
- v1 usage,
- v2 adoption,
- CSM,
- status,
- blocker.

Requirements:

- fast filtering,
- clickable aggregate metrics,
- filter chips for blocked/strategic/no-progress/etc.,
- strong visual status indicators.

### Live Airflow rail

A compact side panel can show real orchestration state, for example:

```text
Verification run
84 mapped tasks
31 running
52 success
1 awaiting input
```

Poll or subscribe to actual Airflow/Cascade state.

Animation is allowed only when the backend state actually changes.

### Account drawer/detail

For Acme, show:

- ARR/tier/owner,
- v1/v2 usage trend,
- technical blocker,
- contract flag,
- generated brief,
- evidence,
- timeline,
- pending human action.

### HITL modal

Render the Airflow human decision in Cascade's business UI.

When submitted:

1. send the response to the real Airflow HITL endpoint/action,
2. wait for the task/run state to change,
3. update Cascade only from the resulting real workflow transition.

### Orchestration reveal

Persistent action:

**View Orchestration**

Open or deep-link to the native Airflow DAG run / mapped task view for the exact campaign.

This is a key judge moment.

## 15. API surface for the Cascade plugin

The FastAPI plugin only needs enough endpoints to support the demo.

Suggested surface:

```text
GET  /cascade/campaigns/{campaign_id}
GET  /cascade/campaigns/{campaign_id}/accounts
GET  /cascade/accounts/{account_id}
GET  /cascade/campaigns/{campaign_id}/timeline
GET  /cascade/exceptions/pending
POST /cascade/exceptions/{exception_id}/respond
POST /cascade/scenario/advance
POST /cascade/campaigns/{campaign_id}/verify
```

Avoid building a generalized backend framework.

## 16. Three-minute demo plan

### 0:00–0:20 — establish the business problem

Open the API v1 Sunset campaign.

Show:

- 2,417 affected accounts,
- $82.4M ARR,
- 58 days remaining.

Narrative:

> "We are retiring API v1. The hard part isn't publishing a migration guide; it is getting every affected customer safely off it."

### 0:20–0:50 — scale reveal

Show the blast-radius table and campaign state distribution.

Reveal that a real Airflow run dynamically mapped assessment across all 2,417 accounts.

Do not wait for the large run live; use a real pre-completed hero run.

### 0:50–1:25 — one meaningful exception

Open Acme Logistics.

Show:

- $2.4M ARR,
- 14k v1 calls/day,
- custom parser blocker,
- compatibility commitment,
- migration brief.

Show that the account is waiting for a person.

### 1:25–1:50 — resolve real HITL

Choose **Grant extension to November 15** from the Cascade UI and enter a reason.

Airflow resumes the actual parked workflow.

The account timeline updates from the real workflow result.

### 1:50–2:20 — advance reality and run live fan-out

Click **Advance 7 days**.

The deterministic mock world switches telemetry snapshots.

Trigger `migration_verification`.

Approximately 50–120 changed accounts become real mapped tasks.

Show the live task counters changing.

Acme reaches `MIGRATED` because its telemetry satisfies the deterministic rule.

### 2:20–2:45 — reveal native Airflow

Click **View Orchestration**.

Show:

- DAG run,
- mapped tasks,
- task states,
- logs,
- HITL run/state.

Narrative:

> "The business application is not simulating this. These are the actual Airflow task instances behind every account state you just saw."

### 2:45–3:00 — close

Return to Cascade.

Closing line:

> "One product change. Thousands of customer migrations. Airflow keeps them moving."

## 17. Suggested repository structure

```text
cascade/
├── dags/
│   ├── product_change_assessment.py
│   ├── exception_resolution.py
│   └── migration_verification.py
├── plugins/
│   └── cascade_plugin.py
├── cascade_api/
│   ├── app.py
│   └── routes/
├── cascade_ui/
│   ├── package.json
│   └── src/
├── src/cascade/
│   ├── models.py
│   ├── risk.py
│   ├── store.py
│   ├── airflow_links.py
│   └── mock_client.py
├── mock_services/
│   ├── app.py
│   └── fixtures/
│       ├── api_v1_sunset.json
│       ├── day0_usage.json
│       └── day7_usage.json
├── scripts/
│   ├── seed_demo.py
│   └── prepare_hero_run.py
├── tests/
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

## 18. Implementation order

### Phase 1 — prove orchestration

1. Build deterministic fixtures.
2. Build `product_change_assessment`.
3. Make `discover_affected_accounts` return runtime data.
4. Dynamically map `assess_account`.
5. Persist account projections to Cascade DB.
6. Confirm a large real mapped run works.

Do this before building a polished UI.

### Phase 2 — build the business application

1. Add React plugin shell.
2. Add campaign dashboard.
3. Add blast-radius table.
4. Wire metrics to Cascade DB and real Airflow run IDs.

### Phase 3 — hero exception

1. Add Acme fixture.
2. Add `exception_resolution` DAG.
3. Add real HITL.
4. Add HITL modal in Cascade.
5. Verify pause/resume end to end.

### Phase 4 — live day-7 wave

1. Add `scenario/advance`.
2. Add telemetry watermark.
3. Build `migration_verification`.
4. Map only changed accounts.
5. Add live task-state rail.

### Phase 5 — Common AI

1. Add typed migration brief schema.
2. Run only on selected high-risk accounts.
3. Pre-warm or pre-run hero brief if model latency threatens recording reliability.

### Phase 6 — demo polish

1. Add motion tied to real state changes.
2. Seed hero campaign.
3. Add native Airflow deep links.
4. Rehearse the exact three-minute path.
5. Finish README and architecture write-up.

## 19. Acceptance criteria

The demo is ready only when all of these are true:

- [ ] Cascade is loaded as an Airflow React/plugin surface.
- [ ] A real Airflow campaign run creates mapped account tasks with `expand()`.
- [ ] The large hero campaign has more than 1,000 real mapped task instances.
- [ ] The frontend derives its campaign/account state from real workflow outputs.
- [ ] Acme reaches a real Airflow HITL waiting state.
- [ ] A response submitted in Cascade resumes the real Airflow workflow.
- [ ] Advancing the mock world creates a real verification run.
- [ ] Verification dynamically maps only changed accounts.
- [ ] Acme becomes migrated because a deterministic telemetry rule passes.
- [ ] `View Orchestration` opens the corresponding native Airflow execution.
- [ ] Any claimed Common AI behavior is backed by a real Common AI task in the recorded environment.
- [ ] No fake Airflow task/run status is displayed.
- [ ] The complete recorded judge path fits comfortably inside three minutes.

## 20. Scope cuts if time becomes constrained

Keep these at all costs:

1. React/FastAPI Airflow plugin
2. real Dynamic Task Mapping
3. real mapped hero campaign
4. Acme business scenario
5. real HITL pause/resume
6. live smaller verification fan-out
7. native Airflow reveal

Cut in this order:

1. AssetWatcher / external event watcher
2. generalized contract behavior
3. multiple simultaneous HITL exceptions
4. rich LLM use across many accounts
5. real external integrations
6. email/Slack notifications
7. advanced auth/roles
8. more than one product-change scenario

If Common AI threatens the core demo, keep **one** narrow real task rather than building an AI-heavy architecture.

## 21. Technical risks and mitigations

### Large mapped run is slow locally

Mitigation:

- pre-run the 2,417-account campaign,
- configure `max_map_length` above the population size,
- keep mapped work CPU-light,
- use a smaller live run during recording.

### UI drifts from Airflow state

Mitigation:

- derive orchestration counters from Airflow/task state,
- only write business state from real tasks,
- never synthesize a running/success state in frontend code.

### LLM latency or model failure breaks the recording

Mitigation:

- run AI on only a handful of accounts,
- keep deterministic analysis independent of AI,
- make AI briefs non-blocking for the main migration-state calculation.

### HITL integration takes too much time

Mitigation:

- support exactly one hero decision flow,
- proxy the real Airflow HITL response rather than building a generalized approval system.

### Mock data feels fake

Mitigation:

- use a large seeded population,
- use realistic distributions,
- hand-author a few named accounts,
- clearly label business numbers as demo fixtures,
- make the orchestration behavior genuine.

### Feature bingo obscures the thesis

Mitigation:

Keep the hierarchy explicit:

1. Plugin application
2. Dynamic Task Mapping
3. HITL
4. State Store / Common AI only where they naturally support the scenario

## 22. Reference docs

1. Astronomer — Beyond the DAG 2026: https://www.astronomer.io/events/beyond-the-dag-data-engineering-hackathon-2026/
2. Apache Airflow — Plugins: https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/plugins.html
3. Apache Airflow — Dynamic Task Mapping: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
4. Apache Airflow — HITL: https://airflow.apache.org/docs/apache-airflow/stable/tutorial/hitl.html
5. Apache Airflow — Event-driven scheduling: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/event-scheduling.html
6. Apache Airflow Common AI provider: https://airflow.apache.org/docs/apache-airflow-providers-common-ai/stable/index.html
7. Apache Airflow — Task and Asset State Store: https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/task-and-asset-state-store.html
8. Apache Airflow — Configuration reference (`max_map_length`): https://airflow.apache.org/docs/apache-airflow/3.3.1/configurations-ref.html
