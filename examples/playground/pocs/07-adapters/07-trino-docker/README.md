# 07-trino-docker — Trino adapter via Docker

> **Credentials:** none (Docker daemon required for the live tour)
> **Runtime:** ~45-60s on a warm Docker pull (parse-only fallback <1s)

## What it shows

The `rocky-trino` v0 adapter against a real Trino coordinator started by docker-compose. A `full_refresh` model copies `tpch.tiny.nation` (25 rows, built into Trino's `tpch` connector, so no seed script) into the writable `memory` catalog.

## Status — what works, what doesn't

The Rocky docs list the Trino adapter as Beta. It is not gated behind `is_experimental`, so no startup warning fires.

- **Works:** `full_refresh` (CTAS), `incremental` (INSERT + watermark WHERE), `DESCRIBE`-based drift, double-quoted three-part identifiers (`"catalog"."schema"."table"`), `CREATE SCHEMA IF NOT EXISTS`, HTTP Basic + JWT bearer auth.
- **Not in v0:** `MERGE` (`strategy = "merge"` errors at validate time; Trino MERGE is connector-dependent), OAuth 2.0 / Kerberos / SPNEGO auth, governance / loader / batch checks, `row_hash_expr` for checksum-bisection diff, `CREATE CATALOG` (Trino has no SQL for it).

## Prerequisites

- `rocky` on PATH
- `docker` + `docker compose` on PATH (live tour)
- `python3` on PATH (used by `run.sh` to parse JSON)

Without a reachable Docker daemon, `run.sh` only validates and compiles, then exits 0.

## Run

```bash
./run.sh
```

`rocky.toml` reads the coordinator URL and auth from env (default URL `http://localhost:8080`):

```bash
TRINO_HOST=http://my-trino:8080 \
TRINO_USER=alice \
TRINO_PASSWORD=secret \
./run.sh
```

## Expected output

```text
=== rocky validate (parse-only — no network) ===
ok V001 Config syntax valid (v2 format)
...
=== rocky compile (parse + type-check models, dialect-agnostic) ===
    compile ok (1 model, 0 diagnostics)
=== docker compose up -d (booting trinodb/trino) ===
=== Waiting for Trino coordinator readiness ===
    Trino ready after 25s
=== rocky run (full_refresh — CTAS into memory.rocky_demo) ===
    run ok
=== verifying row count via Trino REST ===
    rows = 25 (matches tpch.tiny.nation cardinality)

POC complete: rocky-trino v0 exercised end-to-end against a real Trino coordinator.
```

## Related

- Adapter source: `engine/crates/rocky-trino/`
- [`06-rust-native-adapter-skeleton`](../06-rust-native-adapter-skeleton/): starter for warehouses Rocky doesn't ship
