# 01-shell-hooks — Lifecycle hooks via shell commands

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[hook.<event>]]`, `rocky hooks list`, `rocky hooks test`

## What it shows

Hooks declared in `rocky.toml` fire shell commands on pipeline events. Each
receives a JSON event payload on stdin and can set `on_failure` to abort, warn,
or ignore. `rocky run` only logs a failing hook; only `rocky hooks test` acts on
abort.

## Run

```bash
./run.sh
```

## Expected output

- `rocky validate` reports the config valid.
- `rocky hooks list` shows `on_pipeline_start` and `on_pipeline_complete`, each
  with `on_failure: Warn`, and `"total": 2`.
- `rocky hooks test on_pipeline_start` fires `scripts/notify_start.sh` and
  reports `"status": "continue"`.
- Final line: `POC complete: hooks listed and tested.` (exit 0).
