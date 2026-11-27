# 13-run-watch-inner-loop — `rocky run --watch` reacts to file edits

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (background watcher + one synthetic edit, then SIGINT)
> **Rocky features:** `rocky run --watch`, 200 ms debounce, FSEvents-safe directory watch, NDJSON-stream output

## What it shows

`rocky run --watch` wraps `rocky run` in a filesystem watcher that re-materialises the pipeline on every save. A 200 ms debounce coalesces a burst of saves into one re-run. The watcher monitors the parent directory with a filename filter, so atomic-rename saves (vim's `set backupcopy=auto`, VSCode's default) are caught on macOS.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI for seeding (`brew install duckdb`)

## Run

```bash
./run.sh
```

## What to expect

The script is non-interactive. It seeds `raw__orders.orders` (50 rows), starts `rocky -c rocky.toml run --watch --filter source=orders` in the background, waits for `[watch] run completed`, touches `rocky.toml`, waits for the second run, then sends SIGINT. The watch loop completes the in-flight run and exits 0.

`expected/watch.log` holds one `RunOutput` JSON per iteration (newline-delimited) plus the stderr banner and `detected change` notices.

## Related

- Engine source: `engine/crates/rocky-cli/src/commands/run_watch.rs`
- CLI reference: [`rocky run --watch`](../../../../../docs/src/content/docs/reference/commands/core-pipeline.md#rocky-run)
- Sibling POC: [`02-rocky-serve-api`](../02-rocky-serve-api/) — `rocky serve --watch`.
