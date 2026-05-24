# 07-freshness-sla — model-level freshness SLAs (declarative metadata + W005)

> **Category:** 01-quality
> **Credentials:** none (compile-time only — no warehouse)
> **Runtime:** < 5s
> **Rocky features:** `[freshness]` config block (per-model + project default), `expected_lag_seconds` alias, W005 freshness-coverage diagnostic (editor/LSP)

## What it shows

A model can declare a freshness SLA — a TTL after which it's considered stale —
via a per-model `[freshness]` sidecar block (`expected_lag_seconds`, optional
`time_column`, optional `severity`), or project-wide via a top-level
`[freshness]` block in `rocky.toml`. This is **declarative metadata**: Rocky
parses it and carries it through `rocky compile`'s `models_detail[].freshness`,
where downstream tooling (the `dagster-rocky` `FreshnessPolicy`, `rocky doctor`,
a Dagster UI freshness badge) reads it without re-parsing `rocky.toml`.

The companion **W005** coverage diagnostic nudges you toward declaring an SLA on
any model that emits a temporal column but has none — see the W005 section below.

## Why it's distinctive

- **Freshness is structured, first-class config**, not a comment or an external
  monitoring rule. It rides in the same compile output as schemas and lineage.
- `expected_lag_seconds` is the public field name (matching dbt/SQLMesh), and it
  deserializes to Rocky's canonical `max_lag_seconds` — so the declaration is
  portable and existing sidecar fixtures keep working. The POC proves the alias:
  the sidecar writes `expected_lag_seconds`, the compile output reports
  `max_lag_seconds`.
- This is distinct from the runtime freshness *check* under
  `[pipeline.poc.checks.freshness]` (see [`02-inline-checks`](../02-inline-checks/)),
  which executes against the warehouse during a run.

## Layout

```
.
├── README.md             this file
├── rocky.toml            pipeline config with a project-wide [freshness] default
├── run.sh                validate + compile, surfacing each model's SLA
├── data/seed.sql         source tables (so --with-seed gives leaf models real types)
└── models/
    ├── _defaults.toml    shared target catalog/schema
    ├── stg_orders.sql    TIMESTAMP column, NO freshness block  -> W005 candidate
    ├── stg_orders.toml
    ├── stg_shipments.sql TIMESTAMP column + [freshness] SLA     -> covered
    ├── stg_shipments.toml
    ├── dim_customer.sql  no temporal column                     -> n/a
    └── dim_customer.toml
```

## Prerequisites

- `rocky` on PATH (≥ 1.43.0 — `[freshness]` + W005 shipped in 1.43.0)

## Run

```bash
./run.sh
```

## Expected output

```text
=== 2. Each model's declared freshness SLA surfaces in compile output ===
    (models_detail[].freshness — the structured metadata downstream tools read)
    dim_customer   (no freshness SLA declared)
    stg_orders     (no freshness SLA declared)
    stg_shipments  SLA: expected_lag_seconds=  3600  time_column='shipped_at'  severity='warning'
```

## The W005 coverage diagnostic

W005 fires for a model that has a temporal output column (DATE / TIMESTAMP) but
no `[freshness]` declaration in scope — neither a per-model block nor a
project-level default with an `expected_lag_seconds`. The message looks like:

```
W005  model 'stg_orders' has temporal column(s) (order_ts) but no `freshness` block declared
      help: add a `[freshness]` block to the model sidecar, e.g.
            `[freshness] expected_lag_seconds = 3600, time_column = "order_ts"`
```

**Where it surfaces:** W005 is realised in the editor (the `rocky lsp` language
server), where it also carries an AI code-action (`rocky.ai-freshness-fix.v1`)
that proposes a `[freshness]` block on the model's sidecar. It fires wherever the
compiler can resolve a model's output columns to concrete temporal types — i.e.
when source schemas are known (the editor, or a compile backed by a warehouse
schema cache). The credential-free standalone `rocky compile --models …` path in
this POC types leaf-model outputs lazily, so it surfaces the *declared* SLAs
(above) rather than the W005 nudge; the nudge is best seen in the VS Code
extension. Suppress W005 by adding a per-model `[freshness]` block (like
`stg_shipments`) or a project-level default (like this POC's `rocky.toml`).

## Related

- Source: `rocky/crates/rocky-compiler/src/typecheck.rs` (`check_freshness_coverage`), `rocky/crates/rocky-core/src/config.rs` (`ProjectFreshnessConfig`), `rocky/crates/rocky-core/src/models.rs` (`ModelFreshnessConfig`), `rocky/crates/rocky-server/src/lsp.rs` (W005 + AI fix)
- Companion: [`02-inline-checks`](../02-inline-checks/) for the runtime freshness *check* (vs. this declarative *SLA metadata* + coverage diagnostic)
