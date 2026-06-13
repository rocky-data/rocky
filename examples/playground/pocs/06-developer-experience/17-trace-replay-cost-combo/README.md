# 17-trace-replay-cost-combo — One RunRecord, three views

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky run`, `rocky trace`, `rocky cost`, `rocky replay`

## What it shows

`rocky run` writes a single `RunRecord` to the state store. Three read
commands (`rocky trace`, `rocky cost`, `rocky replay`) each project
that record from a different angle:

| Command | Question it answers |
|---|---|
| `rocky trace latest` | What ran when, with what concurrency? |
| `rocky cost latest` | What did each model cost in the warehouse? |
| `rocky replay latest` | What exactly executed, byte-for-byte? |

All three target the same `run_id`. Causality, cost, and reproducibility
are projections of one tree, not three separate telemetry domains
stitched together in a UI.

## Why it's distinctive

- **One artefact, three projections.** A bundled stack (ingest + transform
  + quality, each with its own observability surface) reconstructs the
  cross-stage picture from N data sources: pipeline logs from one tool,
  artifact metadata from another, warehouse query history from a third.
  Rocky emits a single typed `RunRecord` and reads it three ways.
- **Cost is a direct projection.** Per-model warehouse spend is
  computed via the same formula `rocky run` uses for the live summary
  (`compute_observed_cost_usd`), not pulled from a billing dashboard
  after the fact.
- **Replay handle on the same run.** `rocky replay latest` reads the
  same `RunRecord` (same SQL hashes, same timings, same model graph).
  Re-execution with pinned inputs follows in the content-addressed
  write path (in flight); the inspection surface is live today.

## Layout

```
.
├── README.md         this file
├── rocky.toml        DuckDB pipeline, 3 sources of varied size
├── run.sh            seed → run → trace → cost → replay
└── data/seed.sql     raw__orders (10k), raw__events (5k), raw__customers (200)
```

## Prerequisites

- `rocky` on PATH (≥ 1.31.0 for `rocky cost`)
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## Expected output

The full pipeline finishes in well under a second, then the three views
project from the same `RunRecord`:

```text
==> 4. 'rocky cost latest' — per-model warehouse spend over the SAME RunRecord
run: run-20260514-220020-755
status: success
adapter_type: duckdb

models (3):
  model        duration   rows  bytes_scan  bytes_write   cost_usd  status
  orders             6      -           -            -   $0.000000  success
  events             7      -           -            -   $0.000000  success
  customers          8      -           -            -   $0.000000  success
```

DuckDB doesn't publish per-statement byte/cost telemetry, so those
columns are empty in this POC. The same command against a Databricks /
Snowflake / BigQuery pipeline populates the real numbers. The
*structure* (one RunRecord, one cost projection) is identical
regardless of adapter.

## What happened

1. `data/seed.sql` materializes three `raw__<source>.<table>` tables.
2. `rocky run` discovers all three via the `schema_pattern`, runs them
   concurrently under AIMD throttle, and writes one `RunRecord` to
   `.rocky-state.redb` with per-model SQL hashes, row counts, and
   timings.
3. `rocky trace latest` reads that `RunRecord` and emits the timeline
   shape: per-model offsets, durations, concurrency lanes.
4. `rocky cost latest` reads the same record and re-derives per-model
   cost via `compute_observed_cost_usd`, the projection a tracing UI
   would hang cost annotations off span attributes from.
5. `rocky replay latest` reads the same record as the reproducibility
   artefact: the SQL that ran, the rows produced, the timings observed.

## Related

- Sibling POC: [`07-run-trace-gantt`](../07-run-trace-gantt/), the
  Gantt-shape view of `rocky trace` in isolation.
- Sibling POC: [`00-foundations/06-branches-replay-lineage`](../../00-foundations/06-branches-replay-lineage/),
  the branches + replay primitives.
- Engine source: `engine/crates/rocky-cli/src/commands/{cost,trace,replay}.rs`
- OpenTelemetry export: `engine/crates/rocky-cli/src/otlp.rs`
  (feature-gated; the same `RunRecord` flows as OTel spans to any
  collector).
