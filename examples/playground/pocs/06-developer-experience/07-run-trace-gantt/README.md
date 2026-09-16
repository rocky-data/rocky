# 07-run-trace-gantt — Trust arc 4: render a run as a timeline

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky trace`, OTLP span export (`otel` feature, on by default)

## What it shows

`rocky trace <run_id>` reads a `RunRecord` from the state store and
renders it as a timeline: per-model offsets, duration bars,
concurrency lanes. Same backing store as `rocky replay`, different
view: replay is *"what exactly ran?"*, trace is *"show me the shape
of the run"*.

The default `rocky` build includes the `otel` Cargo feature. Set
`OTEL_EXPORTER_OTLP_ENDPOINT` and Rocky also exports its tracing spans
over OTLP: a `run` span for the run, with child spans for its steps.
Your OpenTelemetry collector receives them as ordinary spans.

## Why it's distinctive

- **The Gantt is trace data, not render metadata.** Other orchestrators
  reconstruct a timeline from logs post-hoc; Rocky produces it from the
  primary RunRecord, so it always matches reality.
- **OTLP is the on-ramp**: `rocky trace` is the local-inspection view.
  No special build is needed. Set `OTEL_EXPORTER_OTLP_ENDPOINT` and the
  run's spans go to Grafana Tempo / Honeycomb / Datadog as ordinary
  OTel spans.

## Layout

```
.
├── README.md      this file
├── rocky.toml     DuckDB pipeline with two sources to give the Gantt concurrency
├── run.sh         end-to-end demo (seed → run orders + customers → trace)
└── data/seed.sql  raw__orders.orders + raw__customers.customers
```

## Prerequisites

- `rocky` ≥ 1.11.0 on PATH
- `duckdb` CLI for seeding

## Run

```bash
./run.sh
```

## What happened

1. Seed two source tables.
2. Run the pipeline once — both sources copy concurrently, so the
   single run record holds two models on two concurrency lanes.
3. `rocky trace latest` — renders the recorded run as a timeline
   (per-model offsets, duration bars, concurrency lanes), read from the
   run record `rocky run` persists to the state store.
4. `rocky replay latest` — the sibling inspection command.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/trace.rs`
- Sibling POC: [`00-foundations/06-branches-replay-lineage`](../../00-foundations/06-branches-replay-lineage/)
- OpenTelemetry integration notes: `engine/crates/rocky-observe/src/tracing_setup.rs`
