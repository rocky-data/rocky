# 07-run-trace-gantt — render a run as a timeline

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `rocky trace`, OTLP span export (`otel` feature, on by default)

## What it shows

`rocky trace <run_id>` renders a `RunRecord` from the state store as a timeline: per-model offsets, duration bars, concurrency lanes. `rocky replay` reads the same store and answers what ran; trace shows the shape of the run.

The default build includes the `otel` Cargo feature. Set `OTEL_EXPORTER_OTLP_ENDPOINT` and Rocky also exports its tracing spans over OTLP (a `run` span with child spans for its steps) to your OpenTelemetry collector.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding

## Run

```bash
./run.sh
```

The script seeds two sources and runs the pipeline once. Both copy concurrently, so one run record holds two models on two lanes. It then runs `rocky trace latest` and `rocky replay latest`.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/trace.rs`
- Sibling POC: [`00-foundations/06-branches-replay-lineage`](../../00-foundations/06-branches-replay-lineage/)
- OpenTelemetry integration notes: `engine/crates/rocky-observe/src/tracing_setup.rs`
