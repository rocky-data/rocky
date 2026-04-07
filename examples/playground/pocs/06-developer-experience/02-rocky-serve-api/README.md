# 02-rocky-serve-api — HTTP API for the compiler

> **Category:** 06-developer-experience
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s (server starts in background)
> **Rocky features:** `rocky serve`, `--watch`, REST endpoints

## What it shows

`rocky serve` exposes the compiler's semantic graph + lineage over HTTP
on port 8080. With `--watch`, it auto-recompiles when model files change,
making it easy to embed Rocky into custom dashboards or IDE plugins.

## Endpoints

- `GET /api/health` — health check
- `GET /api/models` — list compiled models
- `GET /api/lineage/:model` — lineage for one model
- `GET /api/dag` — full DAG

## Run

```bash
./run.sh
```

The script starts the server in the background, hits each endpoint with
`curl`, then kills it.
