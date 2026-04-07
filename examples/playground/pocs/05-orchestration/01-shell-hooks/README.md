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
- `rocky hooks test pipeline_start` lets you exercise hooks without running
  the full pipeline.

## Layout

```
.
├── README.md
├── rocky.toml          [[hook.pipeline_start]] / [[hook.pipeline_success]]
├── run.sh
└── scripts/
    ├── notify_start.sh    Logs the event JSON to stdout
    └── notify_success.sh  Same, for success
```

## Run

```bash
./run.sh
```
