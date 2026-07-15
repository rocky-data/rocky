# 07-freshness-sla — model-level freshness SLAs + the W005 coverage diagnostic

> **Category:** 01-quality
> **Credentials:** none (compile-time only, no warehouse)
> **Runtime:** < 5s
> **Rocky features:** `[freshness]` config block (per-model + project default), `expected_lag_seconds` alias, **W005** freshness-coverage diagnostic

## What it shows

A model can declare a freshness SLA (a TTL after which it's considered stale)
via a per-model `[freshness]` sidecar block (`expected_lag_seconds`, optional
`time_column`, optional `severity`), or project-wide via a top-level
`[freshness]` block in `rocky.toml`. The compiler raises the soft-warn **W005**
for any model that emits a temporal output column (DATE / TIMESTAMP) but has no
freshness declaration in scope, neither a per-model block nor a project-level
default. This POC has three models that exercise all three outcomes, then shows
the project-level default silencing the warning globally.

`rocky compile --with-seed` loads `data/seed.sql` so the leaf models get
concrete column types (W005 only fires on a column the compiler knows is
temporal; without source types it would degrade to `Unknown` and stay silent).

## Why it's distinctive

- Freshness is a **first-class, type-aware compile-time concern**, not a
  runtime-only check bolted on after the fact. W005 only fires on models that
  actually emit a temporal column, so the nudge is targeted: no noise on pure
  dimensions like `dim_customer`.
- `expected_lag_seconds` matches the dbt/SQLMesh public field name while
  aliasing Rocky's legacy `max_lag_seconds`, so the declaration is portable and
  surrounding fixtures keep working.
- This is the *coverage* diagnostic, distinct from the runtime freshness *check*
  under `[pipeline.poc.checks.freshness]` (see [`02-inline-checks`](../02-inline-checks/)).

## Layout

```
.
├── README.md                    this file
├── rocky.toml                   default config — NO project [freshness] default (so W005 fires in step 1)
├── rocky-project-freshness.toml  same project + a project-wide [freshness] default (step 2)
├── run.sh                       two-step compile demo
├── data/seed.sql                source tables (so --with-seed gives leaf models real types)
└── models/
    ├── _defaults.toml           shared target catalog/schema
    ├── stg_orders.sql           TIMESTAMP column, NO freshness block  -> W005
    ├── stg_orders.toml
    ├── stg_shipments.sql        TIMESTAMP column + [freshness] block   -> silent
    ├── stg_shipments.toml
    ├── dim_customer.sql         no temporal column                    -> silent
    └── dim_customer.toml
```

## Prerequisites

- `rocky` on PATH (≥ 1.43.0; `[freshness]` + W005 shipped in 1.43.0)

## Run

```bash
./run.sh
```

## Expected output

```text
=== Step 1: compile against rocky.toml (NO project freshness default) ===
    stg_orders emits a TIMESTAMP column with no [freshness] block -> expect W005
    diagnostic codes raised:
         1 "code": "W005"
    W005 count: 1

=== Step 2: compile with -c rocky-project-freshness.toml (project default set) ===
    project [freshness] expected_lag_seconds -> W005 suppressed for every model
    W005 count: 0
```

## What happened

1. **Step 1** compiles against `rocky.toml`, which declares no project freshness
   default. `stg_orders` emits an `order_ts TIMESTAMP` with no `[freshness]`
   block, so W005 fires once with a suggestion to add one. (`rocky compile`
   auto-loads `rocky.toml`; keeping the `[freshness]` block out of it is exactly
   what lets W005 fire here.)
2. `stg_shipments` emits `shipped_at TIMESTAMP` but its sidecar already carries a
   `[freshness]` block, so it's silent: per-model coverage.
3. `dim_customer` has no temporal output column, so W005 never applies; the
   diagnostic is targeted, not blanket.
4. **Step 2** points `rocky compile` at `rocky-project-freshness.toml`, whose
   top-level `[freshness]` block declares `expected_lag_seconds`. That marks the
   whole project freshness-covered and suppresses W005 for every model.

W005 also surfaces in the editor via `rocky lsp`, where it carries an AI
code-action (`rocky.ai-freshness-fix.v1`) that proposes a `[freshness]` block on
the model's sidecar.

## Related

- Source: `engine/crates/rocky-compiler/src/typecheck.rs` (`check_freshness_coverage`), `engine/crates/rocky-core/src/config.rs` (`ProjectFreshnessConfig`), `engine/crates/rocky-core/src/models.rs` (`ModelFreshnessConfig`), `engine/crates/rocky-server/src/lsp.rs` (W005 + AI fix)
- Companion: [`02-inline-checks`](../02-inline-checks/) for the runtime freshness *check* (vs. this compile-time *coverage* diagnostic)
