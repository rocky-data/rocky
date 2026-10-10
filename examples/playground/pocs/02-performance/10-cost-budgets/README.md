# 10-cost-budgets — Trust arc 2: per-run cost summary + budget breach

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[budget]`, `cost_summary`, `budget_breach` event

## What it shows

Every `rocky run` emits a `cost_summary` block in its JSON output:
per-model durations, aggregate wall-clock, and (on adapters that wire
`bytes_scanned`) a USD estimate.

A top-level `[budget]` block in `rocky.toml` turns those numbers into
enforcement: set `max_usd`, `max_duration_ms` or `max_bytes_scanned`, pick `on_breach =
"warn" | "error"`, and Rocky records the breach in the run output's
top-level `budget_breaches` array. `"error"` fails the run with a
non-zero exit code; the `on_budget_breach` hook fires either way.

## Why it's distinctive

- **Cost appears in the run output**, not a separate billing export. The
  same JSON that carries row counts and timings carries cost: one
  artefact, one run, one source of truth.
- **Cost before the run** comes from `rocky estimate`
  ([`13-estimate-explain-cost`](../13-estimate-explain-cost/)) and
  `rocky preview cost`
  ([`06-developer-experience/10-pr-preview-and-data-diff`](../../06-developer-experience/10-pr-preview-and-data-diff/)).

## Layout

```
.
├── README.md       this file
├── rocky.toml      DuckDB pipeline + intentionally tiny [budget] to force a breach
├── run.sh          end-to-end demo (validate → run → extract cost_summary)
└── data/seed.sql   500-row synthetic orders table
```

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `duckdb` CLI for seeding
- `python3` for the cost-summary extraction step

## Run

```bash
./run.sh
```

## What happened

1. `rocky validate` parses the `[budget]` block: `max_duration_ms =
   1`, `on_breach = "warn"`.
2. `rocky run` replicates the 500-row orders table; the run exceeds the
   1ms budget, so Rocky records a breach and fires the `on_budget_breach`
   hook.
3. The JSON run output contains both `cost_summary` (with
   `total_duration_ms` and `per_model` entries) and the top-level
   `budget_breaches` array. `run.sh` prints both.

## Related

- Engine source: `engine/crates/rocky-core/src/config.rs` (`BudgetConfig`),
  `engine/crates/rocky-cli/src/output.rs` (`cost_summary`, `budget_breach`)
- Companion command: `rocky estimate` (dry-run cost projection via `EXPLAIN`)
