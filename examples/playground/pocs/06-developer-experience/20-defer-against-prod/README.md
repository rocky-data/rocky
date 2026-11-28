# 20-defer-against-prod — build one model locally, defer its upstreams to prod

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run --model <name> --defer --defer-to <schema>`

## What it shows

Rebuild **only one downstream model** locally while its unbuilt upstream `ref()`s resolve against an existing production schema, with `--defer --defer-to <schema>`.

## Limits

- `--defer` is **default-off** and does nothing unless you pass it *and* scope the run with `--model`. Deferred upstreams are not rebuilt.
- `--defer` rewrites each unbuilt upstream `ref()` and **parses the model SQL with the Databricks dialect**, so these cannot be rewritten and fail with a clear error: `SELECT * EXCEPT(col, ...)`, trailing-comma select lists, and `STRUCT(...)` / struct literals. For such models, run without `--defer` and build the upstream locally.
- It is a **dev convenience**: CI / production runs should build the real DAG.

## Run

```bash
./run.sh
```

The script uses `engine/target/release/rocky` if present, else `rocky` on PATH. Override with `ROCKY_BIN=/path/to/rocky ./run.sh`.

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

## What to expect

`rocky run` builds `stg_events` and `user_counts` into `poc.main`. The script copies `stg_events` into `poc.prod` (standing in for production), then drops `main.stg_events` to simulate a fresh checkout. `rocky run --model user_counts` then **fails**; adding `--defer --defer-to prod` materializes only `user_counts`, reading `stg_events` from `poc.prod`.

## Related

- Source: `engine/crates/rocky-cli/src/commands/run.rs` (`--defer` / `--defer-to`)
- Companion POC: [`06-developer-experience/13-run-watch-inner-loop`](../13-run-watch-inner-loop)
