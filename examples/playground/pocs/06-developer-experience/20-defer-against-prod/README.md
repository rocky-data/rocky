# 20-defer-against-prod — build one model locally, defer its upstreams to prod

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run --model <name> --defer --defer-to <schema>`

## What it shows

The inner-loop dev convenience: change one downstream model and rebuild **only
that model** locally, while its unbuilt upstream `ref()`s resolve against an
existing production schema instead of forcing a full local rebuild of the DAG.

The run script builds the whole pipeline once into `poc.main`, publishes the
upstream into a `poc.prod` schema (standing in for "prod already built this"),
then drops the local `main.stg_events` to simulate a fresh checkout. A
downstream-only build **without** `--defer` fails (the local upstream is gone);
the same build **with** `--defer --defer-to prod` succeeds: `user_counts`
materializes alone, reading its `stg_events` ref from `poc.prod`.

## Why it's distinctive

- `--defer` is **default-off** and changes nothing unless you both pass it and
  scope the run with `--model`. A plain `rocky run` builds every model and has
  nothing to defer, so the flag is inert; behavior is byte-identical when off.
- Only the selected model is materialized; the deferred upstream is **not**
  rebuilt (the POC asserts `main.stg_events` stays absent afterward).

## The honest contract

`--defer` rewrites each unbuilt upstream `ref()` to point at the deferred
schema. That rewrite **parses the model SQL with the Databricks dialect**, so a
handful of constructs cannot be rewritten and fail with a clear error:

- `SELECT * EXCEPT(col, ...)`
- trailing-comma select lists
- `STRUCT(...)` / struct literals

For models using those, run without `--defer` (build the upstream locally). The
models in this POC deliberately use plain bare-name `ref()`s so the rewrite
applies cleanly. `--defer` is a **dev convenience**: a CI / production run
should build the real DAG, not defer it.

## Layout

```
.
├── README.md                 this file
├── rocky.toml                transformation pipeline, both models in poc.main
├── run.sh                    end-to-end demo (build-aware; uses the release binary)
├── data/
│   └── seed.sql              byte-stable raw__events.events (100 rows)
└── models/
    ├── stg_events.sql/.toml  staging upstream (the deferred model)
    └── user_counts.sql/.toml downstream under development (refs stg_events)
```

## Run

```bash
./run.sh
```

The script defaults to the freshly-built engine binary at
`engine/target/release/rocky`. Override with `ROCKY_BIN=/path/to/rocky ./run.sh`.

## Expected output

```text
=== 4. control: build only user_counts WITHOUT --defer (must fail — no local upstream) ===
    failed as expected — the local upstream is missing

=== 5. build only user_counts WITH --defer --defer-to prod ===
    materialized: poc.main.user_counts
    user_counts = 5 groups, built from poc.prod.stg_events (100 rows)
    main.stg_events still absent — the upstream was deferred, not rebuilt

POC complete: only the selected model built locally; its ref resolved to prod.
```

## What happened

1. `rocky run` builds `stg_events` + `user_counts` into `poc.main`.
2. `stg_events` is snapshotted into `poc.prod` (its "production home").
3. The local `main.stg_events` is dropped (a fresh dev checkout).
4. `rocky run --model user_counts` (no defer) **fails**: the bare `stg_events`
   ref has no local table to resolve to.
5. `rocky run --model user_counts --defer --defer-to prod` **succeeds**: only
   `user_counts` materializes, its `stg_events` ref resolved to `poc.prod`.

## Related

- Source: `engine/crates/rocky-cli/src/commands/run.rs` (`--defer` / `--defer-to`)
- Companion POC: [`06-developer-experience/13-run-watch-inner-loop`](../13-run-watch-inner-loop) — the other half of the inner loop.
