# 02-rocky-serve-api — HTTP API for the compiler

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (server starts in background)
> **Rocky features:** `rocky serve --port 9876`, REST endpoints

## What it shows

`rocky serve` exposes the compiler's semantic graph and lineage over HTTP. It listens on port 8080 by default. It also takes `--watch` (recompile when a model file changes) and `--ui` (browser UI at `/ui/`); this POC passes neither.

## Endpoints

All routes are under the `/api/v1` prefix:

- `GET /api/v1/health` — health check
- `GET /api/v1/models` — list compiled models
- `GET /api/v1/models/:model/lineage` — lineage for one model
- `GET /api/v1/dag` — full DAG
- `GET /api/v1/meta` — every route this build serves (more than these: runs, jobs, review, products and others)

## Run

```bash
./run.sh
```

The script runs `rocky serve --models models --port 9876` in the background, reads `/health`, `/models`, `/models/customer_totals/lineage` and `/dag` with `curl`, writes each response under `expected/`, then kills the server.
