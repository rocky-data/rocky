# 02-rocky-serve-api — HTTP API for the compiler

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (server starts in background)
> **Rocky features:** `rocky serve --port 9876`, REST endpoints

## What it shows

`rocky serve` exposes the compiler's semantic graph + lineage over HTTP.
It listens on port 8080 by default. `run.sh` starts it with
`rocky serve --models models --port 9876`, so the `curl` calls below use
9876. Embed the API into custom dashboards or IDE plugins.

`rocky serve` also takes `--watch`, which recompiles when a model file
changes. This POC does not pass it: the script starts the server, reads
four endpoints, and stops.

## Endpoints

All routes are served under the `/api/v1` prefix:

- `GET /api/v1/health` — health check
- `GET /api/v1/models` — list compiled models
- `GET /api/v1/models/:model/lineage` — lineage for one model
- `GET /api/v1/dag` — full DAG
- `GET /api/v1/meta` — list every route this build serves

## Run

```bash
./run.sh
```

The script starts the server in the background on port 9876. It reads
`/health`, `/models`, `/models/customer_totals/lineage` and `/dag` with
`curl`, writes each response under `expected/`, then kills the server.
