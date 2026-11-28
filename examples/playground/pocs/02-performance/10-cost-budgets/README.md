# 10-cost-budgets — Trust arc 2: per-run cost summary + budget breach

> **Category:** 02-performance
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[budget]`, `cost_summary`, `budget_breach` event

## What it shows

Every `rocky run` emits a `cost_summary` block in its JSON output: per-model durations, total wall-clock, and (on adapters that wire `bytes_scanned`) a USD estimate.

A top-level `[budget]` block enforces limits: set `max_usd`, `max_duration_ms` or `max_bytes_scanned` and `on_breach = "warn" | "error"`. Breaches land in the top-level `budget_breaches` array and fire the `on_budget_breach` hook either way; `"error"` also exits non-zero.

For cost before a run, see [`13-estimate-explain-cost`](../13-estimate-explain-cost/) and `rocky preview cost` ([`06-developer-experience/10-pr-preview-and-data-diff`](../../06-developer-experience/10-pr-preview-and-data-diff/)).

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `duckdb` CLI for seeding
- `python3` for the cost-summary extraction step

## Run

```bash
./run.sh
```

## What happens

`rocky.toml` sets a deliberately tiny budget (`max_duration_ms = 1`, `on_breach = "warn"`), so replicating the 500-row orders table breaches it. `run.sh` prints both `cost_summary` and `budget_breaches`; the full output is in `expected/run.json`.
