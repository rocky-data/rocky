# 07-run-trace-gantt — Trust arc 4: render a run as a timeline

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky trace`, feature-gated OTLP exporter

## What it shows

`rocky trace <run_id>` reads a `RunRecord` from the state store and
renders it as a timeline — per-model offsets, duration bars,
concurrency lanes. Same backing store as `rocky replay`, different
view: replay is *"what exactly ran?"*, trace is *"show me the shape
of the run"*.

When built with the `otlp` feature flag, Rocky streams the same
timeline as OpenTelemetry spans — one `pipeline_run` span per run,
child spans per-model — so the Gantt chart is the same artefact your
platform's existing OpenTelemetry collector already consumes.

## Why it's distinctive

- **The Gantt is trace data, not render metadata.** Other orchestrators
  reconstruct a timeline from logs post-hoc; Rocky produces it from the
  primary RunRecord, so it always matches reality.
- **OTLP is the on-ramp**: `rocky trace` is the local-inspection view;
  bolt on `--features otlp` and the same data flows to Grafana Tempo /
  Honeycomb / Datadog as ordinary OTel spans.

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
2. Run the pipeline twice — once per source — so the state store has
   two runs to inspect.
3. `rocky trace latest` — the read path for the timeline view (today:
   inspection-only — run-record writes ship in Arc 1 wave 2).
4. `rocky replay latest` — the sibling inspection command.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/trace.rs`
- Sibling POC: [`00-foundations/06-branches-replay-lineage`](../../00-foundations/06-branches-replay-lineage/)
- OpenTelemetry integration notes: `engine/crates/rocky-cli/src/otlp.rs`
