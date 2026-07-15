# 06-valkey-distributed-cache — Distributed Schema Cache + Valkey State

> **Category:** 05-orchestration
> **Credentials:** none (Valkey via docker-compose; degrades gracefully if Docker is absent)
> **Runtime:** < 30s (first run pulls Valkey image)
> **Rocky features:** `[cache.schemas] replicate`, `[state] backend = "valkey"`, cross-instance schema/watermark sharing

## What it shows

Rocky can share warehouse **schema metadata** and **watermarks** across multiple
Rocky instances through a Valkey-backed remote state store:

1. **Schema (DESCRIBE) cache** — Rocky caches `DESCRIBE TABLE` results in its local
   state store so leaf models typecheck without a live round-trip. With
   `[cache.schemas] replicate = true`, that cache is replicated through
   `state_sync` to the remote backend.
2. **Valkey state backend** — `[state] backend = "valkey"` keeps the working store
   in local redb and replicates watermarks + the schema cache to Valkey, so a
   fresh instance warms up from Valkey instead of re-DESCRIBE-ing every source.

## Why it's distinctive

- **Multi-instance coordination** — multiple Rocky processes share schema metadata and watermarks via one Valkey
- **Replicated schema cache** — `[cache.schemas] replicate = true` ships the DESCRIBE-result cache off-machine
- **Valkey state** — `backend = "valkey"` uses local redb as the working store and Valkey as the shared remote
- **Graceful degradation** — if Valkey is unreachable, state sync warns and the run continues on local state
- **Zero-config Valkey** — set `valkey_url` and Rocky handles serialization + TTL

> **Note on `tiered`:** a `backend = "tiered"` state store means **Valkey (fast) + S3
> (durable)** and additionally requires `s3_bucket`. This POC stays Valkey-only to
> keep it credential-free. The in-process three-tier cache crate
> (`rocky-cache`: memory → Valkey → source) is an engine-internal capability and
> is not driven by the `rocky.toml` `[cache]` surface — see **Related** below.

## Layout

```
.
├── README.md            this file
├── rocky.toml           pipeline + [cache.schemas] + Valkey state config
├── run.sh               end-to-end demo (starts Valkey, runs pipeline)
├── docker-compose.yml   Valkey 8 (Redis-compatible)
└── data/
    └── seed.sql         sample events (500 rows)
```

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

`run.sh` prints (abridged — `rocky validate` emits a full JSON report):

```text
=== Starting Valkey ===
...                                    # docker compose up (skipped if Docker is down)
+-------+
| Count |
+-------+
| 500   |                             # seed rows loaded into raw__events.events
+-------+
{ "command": "validate", "valid": true, ... }

=== Distributed schema/metadata cache ===
  [cache.schemas] replicate=true  → schema (DESCRIBE) cache replicated to remote state
  [state] backend=valkey          → local redb (working) + Valkey (shared remote)
  ...

=== Run pipeline (cache-enabled) ===

=== Check Valkey state ===
  rocky:state:poc:v19:state.redb        # only when Valkey (Docker) is running

POC complete: replicated schema cache + Valkey state backend configured.
```

The pipeline `run` output is captured to `expected/run.json` (gitignored).

## What happened

1. Started Valkey via docker-compose (port 6379) — skipped with a notice if Docker is unavailable
2. `rocky validate` checked the `[cache.schemas]` and `[state]` config blocks
3. Pipeline copied `raw__events.events` → `poc.staging__events.events` (auto-creating the catalog/schema)
4. Watermark recorded in local redb and pushed to Valkey under `rocky:state:poc:*`; the replicated schema cache rides the same sync
5. Subsequent runs by any Rocky instance download state from Valkey first, warming watermarks + schema types without re-DESCRIBE-ing

> **Without Docker:** the run still validates config and executes the local
> DuckDB pipeline. The Valkey download/upload legs log a warning
> (`Connection refused`) and the run continues on local state — this is the
> intended graceful-degradation path, not a failure.

## Related

- S3 state backend: [`05-orchestration/03-remote-state-s3`](../03-remote-state-s3/)
- Cache crate: `engine/crates/rocky-cache/`
  - `memory.rs` — in-process LRU with TTL
  - `valkey.rs` — Valkey client (feature-gated)
  - `tiered.rs` — fallback chain
