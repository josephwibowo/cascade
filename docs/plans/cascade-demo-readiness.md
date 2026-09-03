# Cascade — Demo Readiness Plan

**Target:** `/Users/joseph/programming/github/astronomer-demo`
**Branch:** `fix/cascade-plugin-runtime` (3 commits ahead of `main`)
**Scope source:** defects found during local end-to-end verification on Airflow 3.3.1 / Astro Runtime `3.3-7`.

Three defects found in the same pass are **already fixed and committed**. They are not
re-planned here and must not be reverted:

| Commit | Fix |
| --- | --- |
| `1dc8c6d` | Vite `process.env.NODE_ENV` inlining + React externalised onto Airflow's window globals |
| `88b3bc2` | `View Orchestration` deep links use Airflow 3's path routing, not `/grid?task_id=` |
| `63ad187` | `orchestration_counts` counts via `total_entries`; `LiveRail` chains polls instead of `setInterval` |

---

## 1. Goal

Cascade becomes a control plane you can hand to someone else and let them drive.

When this is done:

- The dashboard updates itself while the assessment DAG runs. Cards, both distribution
  bars, chip counts and the account table all move without a reload.
- Filtering feels immediate. The account list is paginated and slim, chip predicates run
  in SQL, search is debounced, in-flight requests show a loading state, and stale
  responses can never overwrite newer ones.
- What is filtered is legible at a glance: an explicit active-filter bar with removable
  tokens, chip counts scoped to the current filter, and a `showing N of M` count.
- The pending Airflow decision is reachable. The Exceptions card opens a real exception
  queue; the queue opens the HITL modal.
- The mock world can be advanced to day 7 from the UI, which is what
  `docs/plans/cascade-implementation.md` promised and what the README already claims.
- A clean `astro dev start` → hero run works from the documented steps alone.

---

## 2. Current State

### Execution path today

`plugins/cascade/__init__.py` registers `cascade_app` (FastAPI) at `/cascade` and a React
app at `/plugin/cascade`. The bundle's default export is rendered by Airflow inside its
own React tree; `ui/src/main.tsx` returns `<div id="cascade-root"><Dashboard
campaignId="api_v1_sunset" /></div>`.

`ui/src/screens/Dashboard.tsx` owns all page state. `load()` (line 23) fetches
`api.campaign()` and `api.exceptions()`, and is called from exactly one effect keyed on
`[campaignId]` (line 27). **Nothing else calls it.** Every headline card, both
distribution bars and all six chip counts are therefore frozen at mount.
`ui/src/components/LiveRail.tsx` is the only component that polls.

Filter state lives in four separate `useState` hooks on `Dashboard` — `statusFilter`,
`riskFilter`, `chipFilter`, plus `filterReset` as a reset token (lines 16–19).
`chooseStatus`, `chooseRisk` and `onChipChange` each **clear the other two** (lines
35–36, 59), so the three are mutually exclusive while rendering as three independent
control groups. `q` lives inside `AccountTable`.

`ui/src/screens/AccountTable.tsx:8` refetches on every change to
`[campaignId, statusFilter, riskFilter, chipFilter, q]`, with no debounce, no loading
state, and no protection against out-of-order responses. Line 13 hand-rolls
virtualization with spacer rows at a fixed `rowHeight = 70`; it slices from a
fully-materialised `items` array.

### Measured cost (mid-run, 1,671 of 2,417 accounts assessed)

| Request | Time | Payload | Items |
| --- | --- | --- | --- |
| `GET /campaigns/api_v1_sunset/accounts` | **5.8–8.0 s** | 1.34 MB | 1,671 |
| `…/accounts?chip=technical_blocker` | **8.9 s** | 10 KB | 13 |
| `…/accounts?risk=CRITICAL` | 0.76 s | 410 KB | 510 |
| `…/accounts?status=BLOCKED` | 0.91 s | 42 KB | 51 |
| `…/accounts?q=Acme` | 1.6 s | 851 B | 1 |
| `GET /campaigns/api_v1_sunset` (rollup) | **1.45 s** | 740 B | — |
| `GET /exceptions/pending` | 0.31 s | 13.8 KB | 51 |
| `GET /accounts/acme_logistics` | 0.31 s | 839 B | 1 |

At the full 2,417 population the unfiltered list reaches roughly 1.9 MB.

### Why those numbers look like that

`include/cascade/store.py:291` `query_accounts` has no `LIMIT`. It builds a `select` with
`status` / `segment` / `risk` / `q` pushed into SQL, then at line 304 materialises
**every matching row**, and only then applies the chip predicate in Python (lines
305–316). This is why the most selective filter is the slowest: `chip=technical_blocker`
loads all 1,671 rows to return 13.

`_account_dict` (line 218) serialises `daily_v1`, `daily_v2`, `brief` and `evidence` on
every row. The table renders none of them — only `account_name`, `account_id`, `arr`,
`risk`, `legacy_usage`, `replacement_usage`, `owner`, `status`, `blocker_type`
(`AccountTable.tsx:15`). They exist in the list payload solely because
`AccountDrawer` reuses the row object it was handed (`AccountDrawer.tsx:9–10` read
`daily_v1`, `daily_v2`, `brief`, `evidence`, `latest_airflow_task_instance`).

`get_campaign_rollup` (line 244) has the same shape: it loads every row for the campaign
to compute three distributions and six chip counts in Python. That is the 1.45 s for a
740-byte response, and it is why the dashboard cannot simply be polled as-is.

### Chip predicates and the columns that mirror them

The chip predicates read `sum(row.daily_v2)` / `sum(row.daily_v1)`. Both DAGs write the
scalar equivalents from the same source expression:

- `dags/product_change_assessment.py:103` and `:119` — `"legacy_usage": sum(usage["daily_v1"]), "replacement_usage": sum(usage["daily_v2"])`
- `dags/migration_verification.py:55` — identical

So `legacy_usage == sum(daily_v1)` and `replacement_usage == sum(daily_v2)` for every row
either DAG writes, and the chip predicates are expressible against indexed scalar
columns. This equivalence is the load-bearing assumption of Step 1 and is addressed in
Assumptions and Risks.

### Schema

`include/cascade/models.py:55` already indexes `(campaign_id, status)`,
`(campaign_id, segment)` and `(campaign_id, arr)`. There is **no** index on `risk`,
which the risk filter and the risk distribution both use.

### Dead ends the UI references but does not have

- `AccountDrawer.tsx:10` renders a button reading *"Decision control is available in the
  exception queue"*. No exception queue exists; the button opens the HITL modal in place.
- `Dashboard.tsx:47` renders `Exceptions — N · M awaiting input` as a non-interactive
  card. Nothing navigates to those exceptions.
- `ui/src/api/client.ts:41` `api.advance` has zero callers (`grep -rn advance ui/src`
  returns only its definition).
- `POST /cascade/campaigns/{id}/verify` (`plugins/cascade/api/routes/campaigns.py:34`)
  has zero callers.
- The mock exposes `GET /scenario` (`mock_services/app.py:89`) but the plugin never
  proxies it, so the UI cannot know which day the world is on. Only `day0` and `day7`
  exist (`mock_services/fixtures/`).

### Tests

`tests/test_store.py`, `tests/test_aggregate.py`, `tests/test_rules.py`,
`tests/dags/test_dag_example.py` — 15 tests. Convention: `pytest.importorskip("sqlalchemy")`,
a SQLite URL under `tmp_path`, `create_session_factory(url)`, then
`Base.metadata.create_all(session_factory.kw["bind"])`. No `conftest.py`. Production is
Postgres, tests are SQLite, so **all new SQL must run on both**.

Run with:

```bash
printf 'python -m pytest tests -q\n' | astro dev bash -s
```

---

## 3. Scope

### In scope

1. `get_campaign_rollup` computed with SQL aggregates instead of row materialisation.
2. `query_accounts` gains `limit`/`offset`, pushes chip predicates into SQL, and returns
   a slim row shape; a companion facet count keeps chip counts honest under a filter.
3. `get_pending_exceptions` enriched with account display fields.
4. Route changes for the above, plus `GET /cascade/scenario`.
5. `AccountDrawer` fetches full account detail rather than reusing the list row.
6. Dashboard self-refresh on a chained poll.
7. One unified filter model, composable filters, an active-filter bar, scoped chip
   counts, `showing N of M`, debounced search, loading state, stale-response guarding,
   and page-append scrolling.
8. An exception queue screen reachable from the Exceptions card and from the drawer.
9. Scenario controls: advance to day 7 / run verification.
10. README: unpause the three DAGs.
11. The three minor UI defects (F).

### Supporting changes

- New index `ix_account_migration_campaign_risk` on `(campaign_id, risk)`.
- `upsert_account_migration` derives `legacy_usage` / `replacement_usage` from the daily
  arrays when the caller omits them, closing the divergence class that Step 1 depends on.
- New types in `ui/src/api/client.ts` (`AccountRow` vs the existing full `Account`).

### Out of scope

- **Deleting `include/cascade/airflow_links.py`.** It has no callers and no tests. It was
  corrected in `88b3bc2` rather than removed; removal is a separate call.
- **Replacing the hand-rolled virtualization with a library.** It works, it is ~4 lines,
  and adding a dependency to a plugin bundle that must stay small is not justified here.
- **Any DAG change.** The pipeline is verified correct end to end; nothing in this plan
  touches `dags/`.
- **Server-side sort controls.** The list is ARR-descending by design (`store.py:304`).
- **Auth, multi-campaign routing, dark mode.** No demand from the demo script.
- **Making `q` search the brief text.** Current behaviour is name/id `ILIKE`; changing
  search semantics is a product decision, not a readiness fix.

---

## 4. Assumptions

1. **`legacy_usage` / `replacement_usage` equal the daily-array sums for every persisted
   row.** Verified against both DAG write paths (see Current State). *Invalidated if* a
   future writer sets the arrays without the scalars — closed defensively by the
   `upsert_account_migration` change in Step 1b, and detected by the equivalence test in
   Step 1c.
2. **Filters should compose (AND) rather than remain mutually exclusive.** The API
   already ANDs `status`, `risk`, `chip` and `q`; only the UI forces exclusivity. Making
   them compose is what removes the "three independent-looking control groups that
   silently cancel each other" confusion. *Invalidated if* the demo script depends on
   one-click filter switching, in which case keep exclusivity and rely on the
   active-filter bar alone for legibility.
3. **Page size 200 with append-on-scroll.** Large enough that the first screen and most
   filtered results need one request; small enough to keep the payload near 50 KB.
4. **A 4-second dashboard poll.** Slower than `LiveRail`'s 2.2 s because the rollup is
   coarser data; fast enough that the blast radius visibly fills during the run.
5. **Polling stops when the campaign is terminal.** Same terminal-state logic
   `LiveRail` already uses, so a finished demo does not poll forever.
6. **Only `day0` and `day7` exist.** The scenario control is a two-state toggle, not a
   date picker.

---

## 5. Approach

The server work comes first and is the larger win. Every UI symptom the user reported —
lag, lost clicks, contradictory counts — traces back to two endpoints that materialise
the whole campaign to answer a question that SQL can answer with aggregates and a
`LIMIT`. Polling the dashboard *before* fixing the rollup would make things worse: a
1.45 s full-table scan every 4 seconds.

**Rollup and facets become aggregate queries.** Chip predicates move into a single
shared helper used by three call sites — rollup counts, list filtering, and facet counts
— so the definition of "straightforward" cannot drift between the chip's number and the
chip's rows. That drift is a live risk today: the counts come from
`get_campaign_rollup`'s Python loop while the rows come from `query_accounts`' separate,
independently-written Python loop.

**The list gets slim rows and pagination**, which forces `AccountDrawer` to fetch its own
detail. That is the right shape anyway — `GET /cascade/accounts/{id}` already exists,
costs 0.31 s, and returns 839 bytes.

**The UI's filter state collapses to one object.** Four `useState` hooks plus a reset
token is what makes the current state illegible; one `Filters` object renders directly
into an active-filter bar, and the reset token disappears.

**Stale-response guarding is not optional once search is debounced.** A 250 ms debounce
plus a 6-second unfiltered request means an old response can land after a new one.
An incrementing request token compared on arrival is enough and avoids threading
`AbortController` through the client.

Rejected: adding a server-side full-text index for `q`. `ILIKE` over 2,417 rows is not
the bottleneck — the 1.34 MB payload is.

Rejected: computing chip counts client-side from the fetched page. The page is 200 rows;
the counts describe the whole filtered set.

---

## 6. Steps

### Step 1a — Shared chip predicates in SQL

**Files / symbols:** `include/cascade/store.py` — new module-level `CHIP_PREDICATES`.

**Change:** add above `query_accounts`:

```python
from sqlalchemy import and_

def _chip_predicates() -> dict[str, Any]:
    """SQL equivalents of the blast-radius chips.

    ``legacy_usage`` and ``replacement_usage`` are the trailing sums of
    ``daily_v1`` / ``daily_v2``; both DAG write paths set them from the same
    expression, so the chips are expressible against indexed scalar columns
    instead of loading every row to sum a JSON array.
    """
    return {
        "straightforward": and_(
            AccountMigration.segment == "STANDARD",
            AccountMigration.status == "NOT_STARTED",
            AccountMigration.replacement_usage > 0,
        ),
        "actively_migrating": AccountMigration.status == "IN_PROGRESS",
        "no_progress": and_(
            AccountMigration.status == "NOT_STARTED",
            AccountMigration.legacy_usage > 0,
            AccountMigration.replacement_usage == 0,
        ),
        "strategic": AccountMigration.segment == "STRATEGIC",
        "contractual": AccountMigration.segment == "CONTRACTUAL",
        "technical_blocker": AccountMigration.segment == "TECHNICAL_BLOCKER",
    }

CHIP_IDS: tuple[str, ...] = (
    "straightforward", "actively_migrating", "no_progress",
    "strategic", "contractual", "technical_blocker",
)
```

**Integration:** consumed by Steps 1c, 2a and 2b. Nothing else changes yet.

**Invariants:** predicate semantics must match the current Python exactly, including
`straightforward` excluding the `no_progress` slice via `replacement_usage > 0`.

**Edge cases:** `daily_v2` absent or `[]` → `replacement_usage` is `0` (column default),
matching `sum([]) > 0` being false.

**Verify:** Step 1c's equivalence test.

---

### Step 1b — Derive usage scalars on upsert

**Files / symbols:** `include/cascade/store.py` — `upsert_account_migration`.

**Change:** before writing, when the payload carries `daily_v1` / `daily_v2` but omits
`legacy_usage` / `replacement_usage`, set them to `sum(...)` of the respective array.
Do not override an explicitly supplied value.

**Integration:** both DAGs already pass both, so their behaviour is unchanged. This
closes the divergence class for tests and any future writer.

**Invariants:** upsert must stay idempotent — `tests/test_store.py::test_account_upsert_is_idempotent` covers this.

**Edge cases:** payload with neither arrays nor scalars → columns keep their `0` default.

**Verify:** `python -m pytest tests/test_store.py -q`.

---

### Step 1c — Rollup as aggregate queries

**Files / symbols:** `include/cascade/store.py` — `get_campaign_rollup` (line 244).

**Change:** replace the `rows = list(session.scalars(...))` materialisation with:

- one `select(func.count(), func.coalesce(func.sum(AccountMigration.arr), 0.0))` for
  `affected_accounts` / `affected_arr`;
- three `select(col, func.count()).group_by(col)` queries for `status_distribution`,
  `risk_distribution`, `segment_distribution`;
- one query for all six chip counts using
  `func.sum(case((pred, 1), else_=0))` per predicate from `_chip_predicates()`.

Use `case`, not `count().filter()` — `FILTER` support varies across the SQLite builds the
test suite runs on.

Derive `migration_completion` from `status_distribution.get("MIGRATED", 0) / total`.

**Integration:** the returned dict's keys, types and ordering semantics must not change —
`ui/src/api/client.ts` `Campaign`, `Dashboard.tsx`, `AccountTable` chip counts and
`scripts/prepare_hero_run.py`'s day-0 assertion all consume it.

**Invariants:**

- With zero rows, `affected_accounts` and `affected_arr` still fall back to
  `campaign.affected_accounts` / `campaign.affected_arr` (current lines 278–279).
- `migration_completion` is `0` when there are no rows, not a `ZeroDivisionError`.
- `pending_exceptions` query is unchanged.

**Edge cases:** a campaign row with no accounts; a status value never seen before (must
appear in the distribution rather than be dropped).

**Verify:**

1. New `tests/test_store.py::test_rollup_matches_row_scan` — seed ~40 accounts spanning
   every status, risk, segment and both usage patterns; assert the new rollup equals a
   reference computed by the old Python loop (inline the reference in the test).
2. Against the live stack, `GET /cascade/campaigns/api_v1_sunset` returns byte-identical
   JSON to the pre-change response for the same DB state, and drops from ~1.45 s to
   under 100 ms.

---

### Step 2a — Paginated, slim, SQL-filtered account list

**Files / symbols:** `include/cascade/store.py` — `query_accounts`, new `_account_row_dict`.

**Change:**

- Signature becomes
  `query_accounts(session, campaign_id, filters=None, *, limit=200, offset=0) -> dict`
  returning `{"items": [...], "total": int, "limit": int, "offset": int}`.
- Apply the chip predicate from `_chip_predicates()` **inside the `select`**, alongside
  the existing status/segment/risk/`q` clauses. Remove the post-materialisation Python
  chip loop (lines 305–316) entirely.
- Compute `total` with `select(func.count()).select_from(...)` over the same filtered
  criteria before applying `limit`/`offset`.
- Serialise rows with a new `_account_row_dict` that omits `daily_v1`, `daily_v2`,
  `brief`, `evidence`, `latest_airflow_task_instance` and `updated_at`. Keep
  `_account_dict` untouched for `get_account`.

**Integration:** `plugins/cascade/api/routes/campaigns.py:20` returns the dict directly.
`get_account` and `_account_dict` are unchanged, so the drawer's detail fetch keeps the
full shape.

**Invariants:** ordering stays `arr DESC, account_name`. Filter semantics for
`status` / `segment` / `risk` / `q` are unchanged, including the `.upper()` coercion.

**Edge cases:** `offset` beyond `total` → empty `items`, correct `total`; `limit` clamped
to `1..500`; unknown `chip` value → treat as no chip filter (current behaviour).

**Verify:** new `tests/test_store.py::test_query_accounts_chip_pushdown_matches_python`
asserting, for each of the six chips over a seeded population, that the SQL result set
equals the old Python filter's result set; plus a pagination test that
`limit`/`offset` slices agree with the unpaginated ordering.

---

### Step 2b — Facet counts under the active filter

**Files / symbols:** `include/cascade/store.py` — new `chip_facets`.

**Change:** `chip_facets(session, campaign_id, filters) -> dict[str, int]` applies the
**non-chip** parts of `filters` (`status`, `risk`, `segment`, `q`) and returns all six
chip counts via the same `case`-sum query shape as Step 1c.

**Integration:** `campaign_accounts` returns it as `facets: {"chips": {...}}`, so one
request feeds both the table and the chip labels.

**Invariants:** with no filters applied, `chip_facets` must equal the rollup's
`chip_counts` exactly.

**Edge cases:** a filter that matches nothing → all six counts `0`, not absent keys.

**Verify:** test asserting `chip_facets(session, cid, {})` equals
`get_campaign_rollup(...)["chip_counts"]` over the same seeded data.

---

### Step 2c — Risk index

**Files / symbols:** `include/cascade/models.py:55` `AccountMigration.__table_args__`.

**Change:** add `Index("ix_account_migration_campaign_risk", "campaign_id", "risk")`.

**Integration:** `ensure_schema()` runs `create_all`, which is additive, so a fresh
`astro dev start` picks it up. An existing database needs
`python scripts/reset_demo.py` or a manual `CREATE INDEX`; note this in the PR.

**Invariants:** no column changes, so no data migration.

**Verify:** `python scripts/init_db.py` succeeds; the risk filter's latency does not
regress.

---

### Step 3 — Enrich pending exceptions

**Files / symbols:** `include/cascade/store.py` — `get_pending_exceptions` (line 346).

**Change:** outer-join `AccountMigration` on `(campaign_id, account_id)` and add
`account_name`, `arr`, `status`, `risk`, `blocker_type` to each returned dict.

**Integration:** `plugins/cascade/api/routes/exceptions.py:25` passes rows through
unchanged, so the new keys reach the UI automatically. The exception queue (Step 8)
needs them; without this it would issue one `GET /accounts/{id}` per row — 51 requests.

**Invariants:** existing keys keep their names and types. `respond` is untouched.

**Edge cases:** an exception whose account row is missing → the new fields are `None`,
and the UI must render the `account_id` as a fallback.

**Verify:** new `tests/test_store.py` case seeding one exception with a matching account
and one without; `GET /cascade/exceptions/pending` shows `account_name` for Acme.

---

### Step 4 — Route surface

**Files / symbols:** `plugins/cascade/api/routes/campaigns.py`,
`plugins/cascade/api/routes/scenario.py`.

**Change:**

- `campaign_accounts` accepts `limit: int = 200` and `offset: int = 0`, clamps them, and
  returns `{items, total, limit, offset, facets}`.
- Add to `scenario.router`: `GET ""` returning `MockSystemsClient().scenario()` — the
  current snapshot — mapping `AirflowAPIError`/transport failure to a 502 the way
  `advance` already does.

**Integration:** `ui/src/api/client.ts` consumes both in Step 5.

**Invariants:** the existing `POST /scenario/advance` and
`POST /campaigns/{id}/verify` contracts are unchanged.

**Edge cases:** `limit=0` or negative → clamp to 1; `limit>500` → clamp to 500.

**Verify:** `curl "…/accounts?limit=5"` returns 5 items with the true `total`;
`curl …/cascade/scenario` returns `{"snapshot": "day0"}`.

---

### Step 5 — API client types

**Files / symbols:** `ui/src/api/client.ts`.

**Change:**

- Add `AccountRow` = the slim shape; keep `Account` as the full detail shape used by the
  drawer.
- `accounts()` returns `{items: AccountRow[]; total: number; limit: number; offset: number; facets: {chips: Record<string, number>}}` and takes `limit`/`offset`.
- Add `scenario: () => request<{snapshot: string}>('/cascade/scenario')`.
- Add `verify: (id: string) => request(..., {method:'POST'})` for
  `POST /cascade/campaigns/{id}/verify`.

**Integration:** `AccountTable` switches to `AccountRow`; `AccountDrawer` keeps `Account`.

**Invariants:** `request<T>` and its error behaviour are unchanged.

**Verify:** `pnpm --dir ui typecheck`.

---

### Step 6 — Drawer fetches its own detail

**Files / symbols:** `ui/src/screens/AccountDrawer.tsx`.

**Change:** accept `{campaignId, accountId}` instead of a full `account` object. Fetch
`api.account(accountId, campaignId)` in the existing effect, hold it in state, and render
a skeleton until it arrives. Guard `Math.max(...account.daily_v1, ...)` (line 9) against
the pre-load `null`.

**Integration:** `Dashboard.tsx:60` passes ids from the selected row instead of the row
object. `selected` state becomes `{account_id, campaign_id} | null`.

**Invariants:** everything the drawer renders today — telemetry trend, blocker note,
brief with the `AI generated` pill only when `brief_source === 'llm'`, timeline,
`ViewOrchestration` — must render identically once loaded.

**Edge cases:** account deleted between list and fetch → render "Account not available"
rather than crashing on `null`. The effect currently depends on `[account, refresh]` (an
object identity); with polling added in Step 7 this would refetch on every tick — depend
on `[accountId, campaignId, refresh]` instead.

**Verify:** open Acme's drawer; trend, deterministic brief and both timeline events
render, and no `AI generated` pill appears while `brief_source` is `deterministic`.

---

### Step 7 — Dashboard self-refresh and unified filters

**Files / symbols:** `ui/src/screens/Dashboard.tsx`.

**Change:**

1. Replace `statusFilter` / `riskFilter` / `chipFilter` / `filterReset` with a single
   `const [filters, setFilters] = useState<Filters>({})` where
   `type Filters = {status?: string; risk?: string; chip?: Chip; q?: string}`.
   Toggling a value sets or clears **only that key** — filters compose.
2. Chain `load()` the way `LiveRail` now polls: call it, and on settle schedule the next
   call 4 s later via `setTimeout`; stop when `campaign.status` is terminal; clear the
   timer on unmount. Do not use `setInterval`.
3. Render an active-filter bar directly under **Blast radius** when any key is set: one
   removable token per active filter (`Status: BLOCKED ×`), plus `Clear all`.
4. Pass `filters` and a `total` down to `AccountTable`; render
   `Showing {items.length} of {total}` beside the heading.
5. `Metric` renders a `<div>` when `onClick` is undefined so "Days to sunset" stops being
   an interactive control with no behaviour **(F)**.
6. Make the Exceptions card clickable, opening the exception queue from Step 8.

**Integration:** `AccountTable`'s props collapse from five filter-ish props to
`{campaignId, filters, onFiltersChange, ...}`. `LiveRail` is untouched.

**Invariants:** the campaign fetch must not clear `selected` — the drawer stays open
across a refresh. Chip toggling still round-trips to the server; counts still come from
the server, never from the fetched page.

**Edge cases:** a poll that errors must not blank the dashboard into
"The campaign is not ready yet." once data has loaded — keep the last good campaign and
schedule the next poll.

**Verify:** with the assessment DAG running, load the page and do not touch it; the
cards, both bars and the chip counts climb. This is the acceptance test for (A).

---

### Step 8 — Filter feel: debounce, loading, staleness, paging

**Files / symbols:** `ui/src/screens/AccountTable.tsx`.

**Change:**

1. Debounce `q` by 250 ms into the value that enters the fetch dependency; keep the input
   controlled and immediate.
2. Add `loading` state; while true, keep the previous rows rendered and show a progress
   bar over the table. This is the direct fix for "clicks appear to do nothing".
3. Guard staleness with a `useRef` request counter: increment on issue, compare on
   arrival, discard responses that are not the newest.
4. Fetch `limit=200, offset=0` on any filter change; when the virtualization's `last`
   reaches `items.length - 20` and `items.length < total`, fetch the next page and
   **append**. Reset to page 0 whenever `filters` changes.
5. Read chip counts from the response's `facets.chips` rather than `campaign.chip_counts`,
   so a chip cannot read `Straightforward 977` beside a table of 51 blocked accounts.
6. Distinguish failure from emptiness: on a rejected fetch render
   "Couldn't load accounts. Retry." with a retry button, not
   "No accounts match the current filters." **(F)**

**Integration:** consumes `filters` from Step 7 and the paginated envelope from Step 5.

**Invariants:** virtualization keeps its spacer-row technique and `rowHeight = 70`;
the DOM row count stays bounded (14 rows observed today).

**Edge cases:** appending must not duplicate rows if a page boundary shifts mid-run
(dedupe by `account_id` on append). Scroll position must survive an append. A filter
change during an in-flight append must discard that append.

**Verify:** type `Acme` — one request fires, not four. Click `Technical blocker` — the
response is under 500 ms and the chip label reads `13` while the table shows 13 rows.
Confirm in DevTools that the unfiltered payload is ~50 KB, not 1.34 MB.

---

### Step 9 — Exception queue

**Files / symbols:** new `ui/src/screens/ExceptionQueue.tsx`;
`ui/src/screens/Dashboard.tsx`; `ui/src/screens/AccountDrawer.tsx`.

**Change:**

- A panel or drawer listing `api.exceptions().items`, each row showing `account_name`
  (falling back to `account_id`), `arr`, `exception_type`, and a state badge —
  `Awaiting input` when `hitl_details.state === 'awaiting_input'`, otherwise `Pending`.
- Rows with `hitl_task_id` get a **Decide** button opening the existing `HITLModal` with
  that row's `hitl_details`; rows without one render as informational.
- Surface `airflow_error` as a banner when present — the endpoint already returns it and
  nothing displays it today.
- On successful submit, refresh the queue and the dashboard.
- Opened from the Exceptions card (Step 7.6) and from the drawer, whose button copy
  changes from *"Decision control is available in the exception queue"* to
  **"Review decision"**.

**Integration:** reuses `HITLModal` unchanged — it already renders whatever options and
parameters Airflow declares and gates submit on a non-empty reason.

**Invariants:** the UI must never synthesise decision options; they come from Airflow.
`POST /exceptions/{id}/respond` blocks until the task is terminal, so the submit button's
busy state must remain until it resolves.

**Edge cases:** 51 pending rows with only 1 awaiting input — sort awaiting-input rows
first. A 409 (`no Airflow decision task`) must surface as a message, not a silent close.

**Verify:** from a fresh hero run, reach the Acme decision **only** by clicking the
Exceptions card. Submit; `pending_exceptions` goes 51→50, `airflow_awaiting_input` 1→0,
and the account timeline gains `EXTENSION_GRANTED` and `EXCEPTION_RESOLVED`.

---

### Step 10 — Scenario controls

**Files / symbols:** new `ui/src/components/ScenarioControls.tsx`;
`ui/src/screens/Dashboard.tsx` header.

**Change:** show the current snapshot from `api.scenario()`. When it is `day0`, offer
**Advance to day 7**, calling `api.advance('day7')`; when it is `day7`, offer
**Run verification**, calling `api.verify(campaignId)`. Disable both while a request is
in flight or while `LiveRail` reports a non-terminal verification run. On success, force
an immediate dashboard `load()`.

**Integration:** `LiveRail` already switches to the verification run once
`verification_run_id` is set by `POST /scenario/advance`, so the rail follows without
change.

**Invariants:** advancing must remain a real `migration_verification` trigger — the UI
must not write status itself.

**Edge cases:** advancing twice must not double-trigger (disable while in flight).
A 502 from Airflow must surface the detail string.

**Verify:** from `day0`, click **Advance to day 7**. The rail switches to
`migration_verification`, maps exactly **84**, and Acme becomes `MIGRATED`.

---

### Step 11 — `installStyles` out of render

**Files / symbols:** `ui/src/main.tsx`, `ui/src/components/styles.ts`.

**Change:** call `installStyles()` at module scope in `main.tsx` instead of inside the
`Cascade` component body **(F)**. The `installed` guard already makes it idempotent.

**Invariants:** styles must still be present before first paint; module scope runs at
bundle evaluation, which precedes render.

**Verify:** the dashboard renders styled on a cold load with an empty cache.

---

### Step 12 — README

**Files / symbols:** `README.md`.

**Change:** add the unpause step to **Run locally**, before the hero-run commands:

```bash
printf 'airflow dags unpause product_change_assessment exception_resolution migration_verification\n' | astro dev bash -s
```

Correct the claim that the UI advances the world to describe the control added in Step 10,
and note that `AIRFLOW_CONN_CASCADE_LLM` should be **removed** from `.env` rather than
left with `REPLACE` placeholders when running without a model.

**Invariants:** the documented sequence must work start to finish on a clean machine.

**Verify:** Step 13's clean-room run.

---

### Step 13 — Clean-room verification

Run the full documented path on a reset environment:

```bash
printf 'python scripts/reset_demo.py\n'      | astro dev bash -s
printf 'python scripts/prepare_hero_run.py\n' | astro dev bash -s
```

`prepare_hero_run.py` asserts the day-0 distribution
(`NOT_STARTED 1871 / IN_PROGRESS 495 / BLOCKED 51`) and `>1000` successful mapped
assessments, so it is itself a regression test for Steps 1–2.

---

## 7. Testing

### Must keep passing

`printf 'python -m pytest tests -q\n' | astro dev bash -s` — 15 tests.
`test_aggregate.py::test_aggregate_campaign_is_idempotent` and
`test_store.py::test_account_upsert_is_idempotent` both exercise paths touched by
Steps 1b and 1c.

`pnpm --dir ui typecheck && pnpm --dir ui build` after every UI step.

### New tests (`tests/test_store.py`, SQLite via `tmp_path`)

| Test | Covers |
| --- | --- |
| `test_rollup_matches_row_scan` | Step 1c equals the old Python loop across every status/risk/segment |
| `test_query_accounts_chip_pushdown_matches_python` | all six chips, SQL vs Python result sets |
| `test_query_accounts_pagination` | `limit`/`offset` slices agree with unpaginated order; `total` is filter-scoped not page-scoped |
| `test_chip_facets_match_rollup_when_unfiltered` | Step 2b invariant |
| `test_chip_facets_respect_non_chip_filters` | facets narrow under `status=BLOCKED` |
| `test_upsert_derives_usage_from_daily_arrays` | Step 1b, including not overriding explicit values |
| `test_pending_exceptions_include_account_fields` | Step 3, with and without a matching account row |

Seed a population that covers the boundary rows the chips disagree on: `STANDARD` +
`NOT_STARTED` with `replacement_usage == 0` (must be `no_progress`, not
`straightforward`) and with `replacement_usage > 0` (the reverse).

### Manual / integration

1. **(A)** Load during a running assessment DAG, touch nothing for 60 s — cards, bars and
   chip counts climb.
2. **(B)** Unfiltered payload ~50 KB and under 1 s; `chip=technical_blocker` under 500 ms;
   typing `Acme` issues one request; a filter click during an in-flight fetch shows the
   loading bar and applies.
3. **(B)** Active-filter bar shows every active filter; chip counts match the table.
4. **(C)** Reach and resolve the Acme decision starting only from the Exceptions card.
5. **(D)** Advance to day 7 from the UI; verification maps exactly 84; Acme → `MIGRATED`.
6. **(E)** Clean-room run per Step 13.
7. **Regression on the committed fixes:** the plugin still mounts with no console errors,
   and both `View Orchestration` link shapes still resolve (run root for the campaign
   header, `/tasks/<task>/mapped/<index>` from the drawer).

---

## 8. Risks

**Chip-count drift between SQL and the old Python.** The two definitions currently live
in two places (`get_campaign_rollup` and `query_accounts`) and are already at risk of
disagreeing. *Mitigation:* one `_chip_predicates()` helper feeding all three call sites,
plus the equivalence tests. *Detection:* a chip label whose number differs from its row
count is the visible symptom — check this explicitly during manual pass 3.

**`replacement_usage` diverging from `sum(daily_v2)`.** Step 1a's correctness rests on
it. *Mitigation:* Step 1b derives them on upsert. *Detection:*
`test_upsert_derives_usage_from_daily_arrays`, plus a one-off assertion over the live DB
that `replacement_usage = sum(daily_v2)` for all 2,417 rows before merging.

**SQLite/Postgres SQL divergence.** Tests run on SQLite, production on Postgres.
*Mitigation:* use `case`-based conditional sums, not `FILTER`; no dialect-specific JSON
operators anywhere in the new queries.

**Polling amplifies any remaining N-row endpoint.** A 4 s dashboard poll against the
unfixed 1.45 s rollup would be worse than today. *Mitigation:* Steps 1c and 2a land
before Step 7; do not reorder.

**Stale responses overwriting fresh ones.** Introduced by debounce + variable latency.
*Mitigation:* the request-token guard in Step 8.3. *Detection:* type `Acme`, then clear
the box quickly, and confirm the table ends on the unfiltered result.

**Append-scroll duplication.** Rows shift as the DAG writes new accounts, so a fixed
`offset` can repeat or skip. *Mitigation:* dedupe by `account_id` on append; accept
minor skew during a live run, since ordering is `arr DESC` and ARR does not change.

**`create_all` will not add the new index to an existing database.** *Mitigation:* call
it out in the PR and in Step 2c; `reset_demo.py` is the supported path.

---

## 9. Done when

- [ ] `python -m pytest tests -q` passes with the seven new tests included.
- [ ] `pnpm --dir ui typecheck` and `pnpm --dir ui build` both clean.
- [ ] `GET /cascade/campaigns/api_v1_sunset` returns in under 100 ms and its JSON is
      unchanged for identical DB state.
- [ ] `GET /cascade/campaigns/api_v1_sunset/accounts` returns ≤ 200 items, a correct
      `total`, and a payload under 100 KB.
- [ ] `…/accounts?chip=technical_blocker` returns 13 items in under 500 ms.
- [ ] Typing a 4-character search term issues exactly one accounts request.
- [ ] A filter click during an in-flight fetch shows a loading state and applies.
- [ ] With the assessment DAG running, an untouched dashboard's cards, distribution bars
      and chip counts advance without a reload.
- [ ] Every active filter is named in the active-filter bar; chip counts equal the row
      counts they filter to; `Showing N of M` is present.
- [ ] The Acme HITL decision is reachable starting only from the Exceptions card, and
      resolving it moves `pending_exceptions` 51→50 and `airflow_awaiting_input` 1→0.
- [ ] The UI advances the world to day 7; `migration_verification` maps exactly 84;
      Acme becomes `MIGRATED`.
- [ ] `reset_demo.py` → `prepare_hero_run.py` succeeds from the README's steps alone,
      with no undocumented commands.
- [ ] No console errors on the Cascade page; both `View Orchestration` link shapes
      resolve to real Airflow pages.
- [ ] `git diff` touches no file under `dags/`.
