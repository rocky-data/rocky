# 02-rocky-serve-api — HTTP API for the compiler

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (server starts in background)
> **Rocky features:** `rocky serve`, `--watch`, REST endpoints

## What it shows

`rocky serve` exposes the compiler's semantic graph + lineage over HTTP
on port 8080. With `--watch`, it auto-recompiles when model files change.
Embed it into custom dashboards or IDE plugins via the REST API.

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

The script starts the server in the background, hits each endpoint with
`curl`, then kills it.
