# Cascade — Product Hypothesis and Demo Scenario

**Status:** Working hypothesis for Astronomer Beyond the DAG 2026  
**Working name:** Cascade  
**Primary track hypothesis:** Plugin Powerhouse  
**Core idea:** One product change creates an unknown number of customer-specific workflows. Airflow discovers, fans out, tracks, and resolves them.

## 1. Product thesis

When a SaaS or API company makes a breaking product change, the hard problem is not publishing a migration guide. The hard problem is coordinating every affected customer to a proven migration state.

Cascade is a **Product Change Control Plane**. It treats a product change as an executable campaign:

1. determine which customers are actually affected,
2. create one independently tracked migration workflow per affected customer,
3. automate straightforward cases,
4. escalate only exceptional cases to humans,
5. prove migration completion from product telemetry rather than customer self-report.

The product thesis is intentionally broader than API deprecation. The same orchestration model could apply to:

- API version sunsets,
- SDK deprecations,
- authentication migrations,
- webhook schema changes,
- legacy feature retirement,
- product/package migrations,
- regional endpoint migrations,
- billing or pricing migrations.

For the hackathon, the demo should tell **one narrow story: an API v1 sunset**.

## 2. Why this is a credible business problem

The exact end-to-end product is still a hypothesis, but public products and vendor workflows validate the pieces.

### API deprecations are already operational programs

Real API vendors already expose pieces of this workflow:

- Pipedrive tells customers to use an API Usage Dashboard to determine whether they are affected, follow a migration guide, and contact Customer Success for support.
- Stripe documents explicit API version testing and a rollback window during upgrades.
- GitHub and other API vendors use versioning, deprecation notices, and compatibility windows to manage migrations.

This validates that deprecation is not just a documentation problem. It has discovery, migration, monitoring, and exception-handling phases.

### Product teams already care about customer and revenue blast radius

Bruin markets a "Feature Deprecation Impact" use case that connects product usage to affected account count and ARR.

That validates a basic operating question:

> If we change this product behavior, which customers are affected and how much revenue is exposed?

Cascade starts with that question but turns the result into execution.

### Post-sales platforms are moving toward scaled per-account automation

Gainsight and similar Customer Success platforms are moving toward agentic workflows operating across large account populations, with humans reserved for higher-judgment cases.

This validates the orchestration shape:

```text
large customer population
        -> automated per-account work
        -> confidence / risk routing
        -> sparse human intervention
```

### Market gap hypothesis

We did not identify an obvious mainstream product that owns the entire sequence:

```text
product change
    -> technical usage discovery
    -> affected customer population
    -> per-account migration workflow
    -> exception handling
    -> telemetry-based proof of completion
```

That does **not** prove the category is empty. It is the product hypothesis to demonstrate, not a market claim to state as fact.

## 3. Target users

### Primary user

A platform or product engineering owner responsible for safely sunsetting an API, SDK, feature, or integration.

### Secondary users

- Solutions Engineering
- Customer Success
- Support
- Technical Account Management
- Product Operations
- Legal, only for exceptional commitments
- Account owners, only when customer coordination is required

## 4. Job to be done

> Tell me exactly who is affected by this product change, move each account through the right migration path, surface only the blockers that need humans, and prove when the migration is complete.

## 5. Core product model

The main product object is a **migration campaign**.

A campaign has:

- one product change,
- one deadline,
- one discovered population of affected accounts,
- many independently stateful account migrations,
- a small number of exceptions,
- telemetry that determines whether each account has actually completed the migration.

The important modeling decision is:

> The customer migration, not the campaign, is the primary unit of execution.

This is what makes Airflow Dynamic Task Mapping central rather than decorative.

## 6. Demo scenario: Beacon Events API v1 sunset

Use one fictional company and one deterministic scenario throughout the demo.

### Company

**Beacon** is a fictional API-first SaaS company.

### Product change

On September 2, 2026, Beacon announces that **Events API v1 will be retired on October 31, 2026** and replaced by Events API v2.

### Initial campaign state

The mocked business systems produce:

- 2,417 affected customer accounts,
- $82.4M affected ARR,
- 58 days until sunset,
- 14.3M v1 API calls per day.

These numbers are synthetic demo fixtures, not market data.

### Migration success rule

An account is considered migrated only when:

1. it has zero v1 traffic,
2. it has stable v2 traffic where applicable,
3. the condition holds for seven consecutive simulated days.

A customer or CSM cannot simply click "done".

## 7. Demo scenario walkthrough

### Step 1 — Register the change

A platform engineer creates an **API v1 Sunset** campaign in Cascade.

The campaign contains:

- legacy endpoints,
- replacement endpoints,
- sunset date,
- migration guide metadata,
- success criteria.

This action triggers a real Airflow DAG run.

### Step 2 — Discover the blast radius

Cascade queries mocked API telemetry and account metadata.

It discovers 2,417 active accounts that still called v1 during the configured lookback window.

The key conceptual transition is:

> The query result does not become a dashboard row count. It becomes the runtime execution population.

### Step 3 — Fan out one workflow per customer

Airflow dynamically maps account assessment across all affected accounts.

Conceptually:

```text
API v1 sunset
      |
      v
2,417 affected accounts
      |
      v
assess_account.expand(account_id=...)
      |
      +-- Acme Logistics
      +-- Northstar Labs
      +-- Redwood Commerce
      +-- ... 2,414 more
```

Every account produces an independently observable result containing fields such as:

- v1 calls per day,
- v2 adoption,
- endpoints used,
- SDK/version clues,
- ARR,
- account tier,
- owner/CSM,
- migration risk,
- exception flags.

### Step 4 — The campaign resolves into meaningful business states

Seed the fixture so the first large campaign lands in a distribution like:

| State | Accounts | Meaning |
|---|---:|---|
| Straightforward | 1,684 | Standard migration path is known. |
| Actively migrating | 421 | v2 traffic is increasing while v1 traffic falls. |
| No migration activity | 187 | Account remains on v1 with no evidence of progress. |
| Strategic account | 74 | High-value account requires tailored coordination. |
| Contractual exception | 38 | Timing or a commitment requires human judgment. |
| Technical blocker | 13 | Usage cannot cleanly migrate through the default path. |

The exact counts are demo fixtures. Their purpose is to make scale and exception routing visible.

### Step 5 — Zoom into one hero exception

Use **Acme Logistics** as the named account throughout the demo.

Seed Acme with:

- $2.4M ARR,
- approximately 14,000 v1 calls/day,
- no meaningful v2 traffic at campaign start,
- a custom webhook parser that depends on the v1 payload shape,
- a seeded contractual compatibility commitment through October 31,
- a named CSM.

Cascade generates a migration brief from the gathered evidence.

The important part is that the system does not ask the LLM to decide whether Acme is migrated or whether Beacon should break its commitment. The LLM only explains evidence and proposes a next step.

The account becomes a real HITL exception.

The reviewer sees a business decision such as:

- Grant extension to November 15
- Keep October 31 deadline
- Escalate for legal review

For the demo, choose **Grant extension to November 15** and require a reason.

Airflow resumes the parked workflow and the account timeline updates.

### Step 6 — Advance the simulated world

The demo advances from "day 0" to "day 7" using a deterministic mock scenario control.

New telemetry now shows:

- Acme v1 traffic falling to zero,
- Acme v2 traffic becoming stable,
- a subset of other customers changing migration state.

This triggers or starts a real verification DAG.

Airflow discovers only the accounts whose telemetry changed and dynamically maps verification over that delta.

Acme becomes `MIGRATED` only after the deterministic success rule is satisfied.

### Step 7 — Reveal Airflow underneath the product

The main demo should spend most of its time in Cascade.

Near the end, click **View Orchestration** and reveal the native Airflow DAG run and mapped task instances powering the business UI.

The reveal should communicate:

> This is not a mock dashboard that happens to mention Airflow. Airflow is the real execution engine underneath the application.

## 8. Product surfaces

### Campaign dashboard

Purpose: establish scale and business stakes immediately.

Show:

- campaign name,
- deadline,
- affected accounts,
- affected ARR,
- current migration percentage,
- blocked accounts,
- accounts requiring human action,
- distribution of migration states.

### Account explorer

Purpose: make the mapped population tangible.

Columns can include:

- account,
- ARR,
- risk,
- v1 usage,
- v2 adoption,
- owner,
- status,
- blocker type.

Every aggregate metric on the dashboard should be clickable into the account population that produced it.

### Account detail

Purpose: explain why one customer is in its current state.

Show:

- usage trend,
- current SDK/version clues,
- evidence,
- migration plan,
- generated brief,
- exception reason,
- timeline of orchestration events.

### Exception queue

Purpose: demonstrate sparse HITL.

Show only accounts that genuinely need a person, sorted by urgency and revenue exposure.

### Orchestration reveal

Purpose: prove that real Airflow state powers the product.

Link the campaign/account UI back to:

- DAG run ID,
- mapped task instances,
- task states,
- HITL state,
- logs.

## 9. Adjacent product categories and differentiation hypothesis

| Adjacent category | What it usually answers | What Cascade adds |
|---|---|---|
| Product analytics | Who still uses the deprecated thing? | Turns the affected population into independently tracked execution. |
| Customer Success platform | Which accounts need attention? | Adds technical migration evidence and telemetry-based proof of completion. |
| API migration guide/workbench | How should one customer upgrade? | Coordinates the vendor-side portfolio of customer migrations. |
| Workflow automation | Can I run a predefined business workflow? | Airflow-style runtime fan-out, retries, observability, HITL, and reprocessing. |

## 10. Why Airflow is part of the product thesis

The strongest Airflow-specific statement is:

> One business event discovers an unknown population at runtime, and that population becomes thousands of independently stateful units of work.

The product depends on Airflow for:

- runtime fan-out through Dynamic Task Mapping,
- independent task state and retry behavior,
- visible execution history,
- scheduler-managed HITL,
- cross-run orchestration state,
- triggering new work when new evidence arrives,
- a plugin surface that turns orchestration into a domain-specific application.

If Cascade were just a dashboard that ran one Python loop over 2,417 customers, the central thesis would be weaker.

## 11. Judge-facing story

The three-minute story should be simple.

### Opening

> "We're retiring API v1 in 58 days. 2,417 customers representing $82.4M ARR still depend on it."

### Scale reveal

Show the control room and reveal that one product change became thousands of account-specific Airflow work units.

### Human exception

Open Acme. Show why it is different. Resolve one real HITL decision from the Cascade UI.

### New evidence

Advance the mock world seven days. A new verification wave runs across only changed accounts. Acme becomes migrated because telemetry proves it.

### Airflow reveal

Open the native Airflow run and show that the mapped work and HITL state were real.

### Closing line

> "Cascade turns product change into an executable customer migration program. The UI is a business application; Airflow is the engine that makes one change manageable across thousands of customers."

## 12. Competition rubric alignment

| Rubric | Why Cascade can score | Product implication |
|---|---|---|
| Creativity & originality — 25% | Uses Airflow as the engine of a business operations application rather than a traditional data pipeline. | Lead with the unexpected product use, not with LLMs. |
| Airflow feature usage — 25% | Dynamic Task Mapping, real HITL, React/FastAPI plugin, real task state; Common AI/State Store add depth. | Each feature shown should be load-bearing. |
| Impact & usefulness — 20% | Product/API deprecation and customer migration are demonstrably real operating problems. | Ground the demo in one realistic scenario. |
| Demo & story — 20% | Large mapped population, one high-stakes exception, and a telemetry-driven resolution create obvious visual state changes. | Optimize the build around the three-minute narrative. |
| Technical implementation — 10% | Real Airflow service and state with deterministic mock systems around it. | Prefer reliable fixtures over fragile integrations. |

## 13. Assumptions and open questions

### Assumptions

- Product/platform teams have enough account-level usage telemetry to identify affected customers.
- A customer migration can be modeled as an independently trackable unit of work.
- Customer ownership and business metadata can be joined to technical usage.
- The vendor has enough influence over the migration to benefit from centralized orchestration.

### Open questions to validate after the hackathon

- Which function actually owns large product deprecations: Product, Platform, CS, Solutions Engineering, or a temporary program team?
- How are strategic customers tracked today during migrations?
- How often do product changes require contractual or policy exceptions?
- What systems are normally the source of truth for migration status?
- Would teams trust telemetry-based completion rules, and what exceptions exist?

## 14. Explicit non-goals for the hackathon

Do not build:

- a CRM,
- a contract management platform,
- a full Customer Success suite,
- arbitrary contract parsing,
- real third-party enterprise integrations,
- a generalized migration engine for every change type,
- invented ROI claims.

The hackathon deliverable only needs to prove the **orchestration/product model** through the Beacon API sunset scenario.

## 15. Evidence and references

1. Astronomer — Beyond the DAG 2026: https://www.astronomer.io/events/beyond-the-dag-data-engineering-hackathon-2026/
2. Pipedrive — API v1 endpoint deprecation: https://developers.pipedrive.com/changelog/post/deprecation-of-selected-api-v1-endpoints
3. Stripe — API upgrades: https://docs.stripe.com/upgrades
4. Bruin — Feature Deprecation Impact: https://getbruin.com/use-cases/saas/feature-deprecation-impact/
5. Gainsight — Agentic Stack for Customer Retention: https://www.gainsight.com/press/gainsight-launches-the-agentic-stack-for-customer-retention/
