# 01-shell-hooks — Lifecycle hooks via shell commands

> **Category:** 05-orchestration
> **Credentials:** none (DuckDB)
> **Runtime:** < 5s
> **Rocky features:** `[[hook.<event>]]`, `rocky hooks list`, `rocky hooks test`

## What it shows

Lifecycle hooks declared in `rocky.toml` that fire shell commands on
pipeline events. Each hook receives a JSON event payload on stdin and can
abort, warn, or ignore on failure.

## Why it's distinctive

- **Built into the binary**, no orchestrator dependency.
- Hooks can do approval gating, notifications, or custom side-effects.
- `rocky hooks test on_pipeline_start` lets you exercise hooks without running
  the full pipeline.

## Layout

```
.
├── README.md
├── rocky.toml          [[hook.on_pipeline_start]] / [[hook.on_pipeline_complete]]
├── run.sh
└── scripts/
    ├── notify_start.sh    Logs the event JSON to stdout
    └── notify_success.sh  Same, on pipeline completion
```

## Run

```bash
./run.sh
```

## Expected output

- `rocky validate` reports the config valid (one `duckdb` adapter, one
  `replication` / `full_refresh` pipeline).
- `rocky hooks list` prints both hooks (`on_pipeline_start` →
  `scripts/notify_start.sh`, `on_pipeline_complete` →
  `scripts/notify_success.sh`), each with `on_failure: Warn` and
  `"total": 2`.
- `rocky hooks test on_pipeline_start` fires `notify_start.sh` and reports
  `"status": "continue"`.
- Final line: `POC complete: hooks listed and tested.` (exit 0).
