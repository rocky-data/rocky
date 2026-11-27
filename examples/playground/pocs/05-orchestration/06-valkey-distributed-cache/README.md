# 06-valkey-distributed-cache — Distributed Schema Cache + Valkey State

> **Category:** 05-orchestration
> **Credentials:** none (Valkey via docker-compose; degrades gracefully if Docker is absent)
> **Runtime:** < 30s (first run pulls Valkey image)
> **Rocky features:** `[cache.schemas] replicate`, `[state] backend = "valkey"`, cross-instance schema/watermark sharing

## What it shows

Rocky instances share **watermarks** and the `DESCRIBE TABLE` **schema cache**
through Valkey. `[state] backend = "valkey"` works in local redb and replicates
to Valkey. `[cache.schemas] replicate = true` includes the schema cache, so a
fresh instance warms up without re-describing sources.

`backend = "tiered"` means Valkey + S3 and also requires `s3_bucket`; this POC
stays Valkey-only. The three-tier `rocky-cache` crate (memory, Valkey, source)
is internal and not driven by the `rocky.toml` `[cache]` surface.

## Prerequisites

- `rocky` on PATH
- `duckdb` CLI (`brew install duckdb`)
- `docker` + `docker compose` (for Valkey)

## Run

```bash
./run.sh
docker compose down   # cleanup
```

## Expected output

`run.sh` starts Valkey (port 6379), validates, copies `raw__events.events` to
`poc.staging__events.events`, and lists Valkey keys (abridged):

```text
=== Check Valkey state ===
  rocky:state:poc:v32:state.redb        # only when Valkey (Docker) is running

POC complete: replicated schema cache + Valkey state backend configured.
```

The pipeline `run` output goes to `expected/run.json` (gitignored).

> **Without Docker:** the run still validates config and executes the local
> DuckDB pipeline. The Valkey download/upload legs log a warning
> (`Connection refused`) and the run continues on local state — this is the
> intended graceful-degradation path, not a failure.

## Related

- S3 state backend: [`05-orchestration/03-remote-state-s3`](../03-remote-state-s3/)
