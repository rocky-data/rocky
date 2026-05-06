# 13-run-watch-inner-loop — `rocky run --watch` reacts to file edits

![rocky run --watch detects a touch, debounces 200 ms, and re-runs the pipeline; Ctrl-C exits cleanly](../../../../../docs/public/demo-run-watch.gif)

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (background watcher + one synthetic edit, then SIGINT)
> **Rocky features:** `rocky run --watch`, 200 ms debounce, FSEvents-safe directory watch, NDJSON-stream output

## What it shows

`rocky run --watch` wraps the existing `rocky run` path in a filesystem
watcher, so an interactive editor session re-materialises the pipeline
on every save. The POC runs non-interactively by:

1. Launching `rocky run --watch` in the background, redirected to a log.
2. Waiting for the first run to land (`"command":"run"` in the log).
3. Touching `rocky.toml` — same shape as a `:w` from vim or a save in
   VSCode.
4. Waiting for the second run to land.
5. Sending SIGINT; the watch loop completes the in-flight run and exits 0.

The log under `expected/watch.log` carries one full `RunOutput` JSON per
iteration (newline-delimited stream) plus stderr banner + `detected
change` notices.

## Why it's distinctive

- **The compile-verify loop becomes interactive** — pair this with
  `rocky lsp` for type-aware authoring + auto-materialise. dbt's
  `--no-version-check --select <model>` story doesn't survive contact
  with watching.
- **Debounce window coalesces editor save bursts** — a quick burst
  triggers exactly one re-run, not one per `notify` event.
- **FSEvents-safe on macOS** — the watcher monitors the parent directory
  with a filename filter, so atomic-rename saves (vim's
  `set backupcopy=auto`, VSCode's default save) are caught where a
  file-level watch would miss the new inode.

## Layout

```
.
├── README.md         this file
├── rocky.toml        DuckDB replication pipeline
├── run.sh            launch watch → touch → wait → SIGINT
└── data/seed.sql     50-row synthetic orders table
```

## Prerequisites

- `rocky` ≥ 1.27.0 (the next release after the merge of [#423](https://github.com/rocky-data/rocky/pull/423)) on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## What happened

1. **Seed** `raw__orders.orders` into DuckDB (50 rows).
2. **Launch watch** — `rocky run --watch -c rocky.toml --filter source=orders` runs in the background, prints a watching banner on stderr, runs the pipeline once on startup.
3. **Touch `rocky.toml`** — fires a filesystem event; the 200 ms debounce window collects any related events; one re-run is dispatched.
4. **Two `RunOutput` records** land in `expected/watch.log` — one per iteration, newline-delimited.
5. **SIGINT** completes the in-flight run, then the watch loop exits 0.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/run_watch.rs`
- CLI reference: [`rocky run --watch`](../../../../../docs/src/content/docs/reference/commands/core-pipeline.md#rocky-run)
- Sibling POC: [`02-rocky-serve-api`](../02-rocky-serve-api/) — `rocky serve --watch` for HTTP-API authoring (different surface; same `--watch` philosophy).
