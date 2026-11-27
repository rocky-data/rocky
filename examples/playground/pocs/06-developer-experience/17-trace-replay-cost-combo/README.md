# 17-trace-replay-cost-combo — One RunRecord, three views

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky run`, `rocky trace`, `rocky cost`, `rocky replay`

## What it shows

`rocky run` writes one `RunRecord` to the state store (`.rocky-state.redb`, with per-model SQL hashes, row counts and timings). Three read commands project the same `run_id`:

| Command | Question it answers |
|---|---|
| `rocky trace latest` | What ran when, with what concurrency? |
| `rocky cost latest` | What did each model cost in the warehouse? |
| `rocky replay latest` | What exactly executed, byte-for-byte? |

Cost is computed with the same formula `rocky run` uses for its live summary (`compute_observed_cost_usd`), not read from a billing dashboard. This POC uses only the inspection view of `rocky replay`, which also has a read-only audit (`--check`) and re-execution (`--execute [--verify]`).

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

The script seeds `raw__orders` (10k), `raw__events` (5k) and `raw__customers` (200). `rocky run` discovers them via the `schema_pattern` and runs them under adaptive concurrency (the default: it halves parallelism on a warehouse rate-limit signal). Then it runs the three commands.

## Expected output

```text
==> 4. 'rocky cost latest' — per-model warehouse spend over the SAME RunRecord
run: run-20260712-122851-046
status: success
trigger: manual
started_at: 2026-07-12T12:28:51.046082182+00:00
finished_at: 2026-07-12T12:28:51.117358845+00:00
duration_ms: 71
adapter_type: duckdb

models (3):
  model                               duration          rows      bytes_scan     bytes_write      cost_usd  status
  customers                                  7             -               -               -     $0.000000  success
  events                                    11             -               -               -     $0.000000  success
  orders                                    17             -               -               -     $0.000000  success

total_duration_ms: 35
total_cost_usd: $0.000000
```

Run id, timestamps and durations vary; the shape is stable. DuckDB doesn't publish per-statement byte/cost telemetry, so those columns are empty here. Databricks, Snowflake and BigQuery pipelines populate them.

## Related

- [`07-run-trace-gantt`](../07-run-trace-gantt/): `rocky trace` alone.
- [`00-foundations/06-branches-replay-lineage`](../../00-foundations/06-branches-replay-lineage/): branches and replay.
- Engine source: `engine/crates/rocky-cli/src/commands/{cost,trace,replay}.rs`
- OpenTelemetry export: `engine/crates/rocky-cli/src/otel_guard.rs` plus `engine/crates/rocky-observe/src/otel.rs` (gated behind the `otel` feature).
