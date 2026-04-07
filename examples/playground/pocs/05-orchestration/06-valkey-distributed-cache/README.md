# 06-valkey-distributed-cache — Distributed Cache + Tiered State

> **Category:** 05-orchestration
> **Credentials:** none (Valkey via docker-compose)
> **Runtime:** < 30s (first run pulls Valkey image)
> **Rocky features:** `[cache]`, `[state] backend = "tiered"`, three-tier caching (memory → Valkey → source)

## What it shows

Rocky's distributed caching layer — a three-tier fallback chain that reduces warehouse API calls across multiple Rocky instances:

1. **Tier 1: Memory** — In-process LRU cache with TTL (sub-millisecond)
2. **Tier 2: Valkey** — Shared Redis-compatible cache (cross-instance)
3. **Tier 3: Source** — Actual warehouse/connector API call

Plus the tiered state backend, where watermarks and run history are stored in both local redb and Valkey for cross-instance coordination.

## Why it's distinctive

- **Multi-instance coordination** — multiple Rocky processes share schema metadata and discovery results
- **Three-tier fallback** — memory → Valkey → source, each tier caches misses from above
- **Tiered state** — `backend = "tiered"` persists watermarks in both local redb AND Valkey
- **Zero-config Valkey** — just set `valkey_url` and Rocky handles serialization + TTL

## Layout

```
.
├── README.md            this file
├── rocky.toml           pipeline + cache + tiered state config
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

```text
Starting Valkey
Cache configuration:
  [cache]  valkey_url  → schema/metadata caching in Valkey
  [state]  backend     → tiered (local redb + Valkey)
  Tier 1: In-process LRU (memory.rs) — sub-millisecond
  Tier 2: Valkey (valkey.rs) — shared across instances
  Tier 3: Source API — warehouse/connector queries

Run pipeline (cache-enabled)
Check Valkey state:
  rocky:state:poc:... → watermark data
POC complete.
```

## What happened

1. Started Valkey via docker-compose (port 6379)
2. `rocky validate` checked `[cache]` and `[state]` config blocks
3. Pipeline ran with cache enabled — schema discovery results cached in Valkey
4. Watermarks stored in both `.rocky-state.redb` (local) and Valkey (shared)
5. Subsequent runs by any Rocky instance hit Valkey first, skipping warehouse calls

## Related

- S3 state backend: [`05-orchestration/03-remote-state-s3`](../03-remote-state-s3/)
- Cache crate: `engine/crates/rocky-cache/`
  - `memory.rs` — in-process LRU with TTL
  - `valkey.rs` — Valkey client (feature-gated)
  - `tiered.rs` — fallback chain
