# 07-freshness-sla — model-level freshness SLAs + the W005 coverage diagnostic

> **Category:** 01-quality
> **Credentials:** none (compile-time only, no warehouse)
> **Runtime:** < 5s
> **Rocky features:** `[freshness]` config block (per-model + project default), `warn_after` / `error_after`, **W005** freshness-coverage diagnostic

## What it shows

A model declares a freshness SLA with a per-model `[freshness]` block (`warn_after`, `error_after`, optional `time_column`), or a project declares a default with a top-level `[freshness]` block in `rocky.toml`. The compiler raises the soft warning **W005** for a model that emits a DATE / TIMESTAMP column with neither.

`warn_after` and `error_after` take a positive integer plus `s`, `h` or `d` (e.g. `"1h"`, `"24h"`), the same grammar as the per-source `freshness` block. W005 is a compile-time *coverage* diagnostic, not the runtime freshness *check* under `[pipeline.poc.checks.freshness]`.

`rocky compile --with-seed` loads `data/seed.sql` so leaf models get concrete column types; without them a column is `Unknown` and W005 stays silent.

| Model | Setup | W005 |
|---|---|---|
| `stg_orders` | `order_ts TIMESTAMP`, no `[freshness]` | fires |
| `stg_shipments` | `shipped_at TIMESTAMP` + `[freshness]` | silent |
| `dim_customer` | no temporal column | silent |

Step 1 compiles with `rocky.toml` (no project default). Step 2 uses `rocky-project-freshness.toml`, whose top-level `warn_after` silences W005 everywhere.

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
    project [freshness] warn_after -> W005 suppressed for every model
    W005 count: 0
```

W005 also appears in the editor via `rocky lsp`, with an AI code action (`rocky.ai-freshness-fix.v1`) that proposes a `[freshness]` block for the sidecar.

## Related

- Source: `engine/crates/rocky-compiler/src/typecheck.rs` (`check_freshness_coverage`), `engine/crates/rocky-core/src/config.rs` (`ProjectFreshnessConfig`), `engine/crates/rocky-core/src/models.rs` (`ModelFreshnessConfig`), `engine/crates/rocky-server/src/lsp.rs`
