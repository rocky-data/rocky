---
title: Why Rocky?
description: The single thesis behind Rocky and the seven concrete reasons it exists
sidebar:
  order: 2
---

Rocky is a control plane for warehouse SQL pipelines built around one bet: **if the compiler knows your DAG end-to-end — types, dependencies, contracts, cost, drift — a lot of the operational work that other tools push onto humans becomes automatic.**

This page is the argument for picking Rocky. For *what* Rocky is, see the [Introduction](/getting-started/introduction/). For a side-by-side feature comparison, see [Features → Comparison](/features/comparison/).

Every claim below cites a runnable POC, a feature page, or a demo GIF — if you can't see it work, we don't claim it.

## What's actually different

### 1. Drift is recovery, not failure

dbt errors on schema change. Rocky compares source vs target every run and picks a recovery strategy per-column: safe type widening becomes an `ALTER`, unsafe change triggers a full refresh, an added column becomes an `ADD COLUMN`.

- **See it run:** [POC `02-performance/06-schema-drift-recover`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/02-performance/06-schema-drift-recover)
- **Feature:** [Schema drift detection](/features/schema-drift/)

### 2. Column-level lineage at PR time

`rocky lineage --column` traces a single column back through aggregations and joins; `rocky lineage-diff` compares two git refs and lists downstream consumers of every changed column. dbt's lineage stops at the table.

- **See it run:** [POC `06-developer-experience/01-lineage-column-level`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/01-lineage-column-level) and [`11-lineage-diff`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/11-lineage-diff)
- **Feature:** [Column-level lineage](/features/all-features/#column-level-lineage)

### 3. Cost is a first-class observable

Every `rocky run` attributes cost per model. `[budget] max_usd` in `rocky.toml` fires a `budget_breach` event on exceed. `rocky preview cost` projects breaches against a branch before merge so reviewers see "this PR would exceed the budget" in the PR comment.

- **See it run:** [POC `02-performance/10-cost-budgets`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/02-performance/10-cost-budgets) and [`02-performance/05-optimize-recommendations`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/02-performance/05-optimize-recommendations)

### 4. AI is grounded in the compiler

`rocky ai "<intent>"` generates a model, then re-runs the compiler on the generated output before returning it to you. Generated SQL that doesn't type-check never reaches your PR. The compile-validate loop is visible in the demo: `Attempts: 2` means the first generation failed and the engine retried.

- **See it run:** [POC `03-ai/01-model-generation`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/03-ai/01-model-generation) and the schema-grounded variant [`03-ai/05-schema-grounded-validation`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/03-ai/05-schema-grounded-validation)
- **Guide:** [AI features](/guides/ai-features/)

### 5. Contracts and governance are checked at compile time

Required columns, protected columns, allowed type changes, PII classification, mask strategies, and `GRANT`/`REVOKE` diffs — all declared in `rocky.toml` and enforced before any data moves. `rocky compile` returns diagnostic codes like `E010` / `E013` for contract violations; `rocky compliance --fail-on exception` exits 1 if a classified PII column has no resolved mask strategy.

- **See them run:** [POC `01-quality/01-data-contracts-strict`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/01-quality/01-data-contracts-strict) and [`04-governance/05-classification-masking-compliance`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/04-governance/05-classification-masking-compliance)
- **Guide:** [Governance](/guides/governance/)

### 6. One binary, no Python runtime

`curl … | bash` installs a single SHA256-verified binary. No `pip install`, no venv, no Python version drift between contributors and CI. The Dagster integration is a subprocess wrapper — the Python side does not depend on Rust source, only on the binary on `$PATH`.

- **Try it:** [Installation](/getting-started/installation/) → [Quickstart](/getting-started/quickstart/)

### 7. Generated SDKs cannot drift

Every `rocky` command emits JSON; the output structs derive `JsonSchema`; `just codegen` exports 55 JSON schemas and regenerates Pydantic v2 models for the Dagster integration and TypeScript interfaces for the VS Code extension. A CI workflow (`codegen-drift.yml`) fails any PR where the committed bindings disagree with the engine.

- **Reference:** [JSON contract](/advanced/json-contract/) · [JSON output](/reference/json-output/)

## When Rocky is the wrong choice

- **You're greenfield and need orchestration first.** Pick Dagster, Prefect, or Airflow; add Rocky later as the transformation engine they call. Rocky is intentionally not an orchestrator.
- **Your team's pain is the semantic layer or metrics.** Rocky has no semantic-layer story today — Cube, MetricFlow, or Looker LookML is a better starting point.
- **You have deep dbt-utils / dbt_artifacts / elementary investments.** Start with [hybrid mode](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/06-hybrid-dbt-packages), not a rewrite; reach for [`rocky import-dbt`](https://github.com/rocky-data/rocky/tree/main/examples/playground/pocs/06-developer-experience/03-import-dbt-validate) only when you're ready.

## What's still aspirational

These are directions Rocky is built for but not yet shipping end-to-end:

- **Cost as a constraint, not just an observable.** Budgets exist today and breach as events; the planner does not yet pick materialization strategy to fit a budget.
- **Autonomous AI agents on production pipelines.** The compile-verify loop is the primitive; an agent loop that detects, proposes, validates, and applies fixes is not yet packaged.
- **`rocky.toml` as the source-of-truth data spec.** Contracts and governance are declarative; emitting them as Protobuf for producers or JSON Schema for API consumers is on the roadmap, not in-tree.

## Try it

```bash
curl -fsSL https://raw.githubusercontent.com/rocky-data/rocky/main/engine/install.sh | bash
rocky playground my-first-project
cd my-first-project
rocky compile && rocky test && rocky run
```

No credentials, runs against local DuckDB, exits in seconds. Then read the [Quickstart](/getting-started/quickstart/) for the production path.
