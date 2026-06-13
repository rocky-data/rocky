# 07-trino-docker — Trino adapter via Docker

> **Category:** 07-adapters
> **Credentials:** none (Docker daemon required for the live tour)
> **Runtime:** ~45-60s on a warm Docker pull (parse-only fallback runs in <1s)
> **Rocky features:** `rocky-trino` v0 warehouse adapter, Trino dialect (double-quoted three-part refs), HTTP Basic auth, `/v1/statement` REST polling, full_refresh CTAS

## What it shows

End-to-end demo of the `rocky-trino` v0 adapter against a real Trino
coordinator booted via docker-compose. Rocky materializes a model that
copies `tpch.tiny.nation` (25 rows, baked into Trino's built-in `tpch`
connector) into the writable `memory` catalog with a `full_refresh`
strategy.

## Why it's distinctive

- **First Rocky POC against Trino.** v0 of the adapter (engine PR #427)
  ships the `WarehouseAdapter` trait surface (`dialect`,
  `execute_statement`, `execute_query`, `describe_table`), driven by the
  hand-rolled `/v1/statement` polling client in `rocky-trino/src/connector.rs`.
- **Zero seed-data wiring.** Trino's `tpch` connector is registered by
  default in `trinodb/trino` and exposes deterministic sample tables
  (`tpch.tiny.nation` = 25 rows). No `CREATE TABLE` / `INSERT` init
  script. The model just `SELECT`s from `tpch.tiny.nation`.
- **Skip-on-no-Docker fallback.** `run.sh` runs `rocky validate` +
  `rocky compile` unconditionally and only attempts the live tour when
  the Docker daemon is reachable. Same pattern as
  [`05-orchestration/03-remote-state-s3`](../../05-orchestration/03-remote-state-s3/)
  and [`06-valkey-distributed-cache`](../../05-orchestration/06-valkey-distributed-cache/).

## Status — what works, what doesn't

The adapter is marked `is_experimental: true`; the runtime logs a
warning when it's selected. v0 coverage:

- **Works:** `full_refresh` (CTAS), `incremental` (INSERT + watermark
  WHERE), `DESCRIBE`-based drift, double-quoted three-part identifiers
  (`"catalog"."schema"."table"`), `CREATE SCHEMA IF NOT EXISTS`, HTTP
  Basic + JWT bearer auth.
- **Not in v0:** `MERGE` (`strategy = "merge"` errors at validate time,
  Trino MERGE is connector-dependent), OAuth 2.0 / Kerberos / SPNEGO
  auth, governance / loader / batch checks, `row_hash_expr` for
  checksum-bisection diff, `CREATE CATALOG` (Trino has no SQL for it,
  catalogs are server-side connector instances).

## Layout

```
.
├── README.md                 this file
├── rocky.toml                Trino adapter + transformation pipeline targeting memory.rocky_demo
├── docker-compose.yml        Single-node trinodb/trino with built-in tpch + memory catalogs
├── run.sh                    Boot Trino → rocky run → assert row count → tear down
└── models/
    ├── nations_copy.sql      SELECT * FROM tpch.tiny.nation
    └── nations_copy.toml     strategy = "full_refresh"
```

## Prerequisites

- `rocky` on PATH
- `docker` + `docker compose` on PATH (live tour)
- `python3` on PATH (used by `run.sh` to parse Trino's `/v1/statement`
  JSON for the row-count assertion, using the same shape `rocky-trino`'s
  connector polls in Rust)

When Docker isn't installed or the daemon is down, `run.sh` falls back
to a parse-only smoke (validate + compile) and exits 0.

## Run

```bash
./run.sh
```

Or override the coordinator URL / auth (the `rocky.toml` reads them
from env at parse time via `${TRINO_HOST:-http://localhost:8080}` etc.):

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

## What happened

1. `rocky validate` parsed `rocky.toml` and confirmed the Trino adapter
   shape is well-formed (no network).
2. `rocky compile` type-checked the model against the Trino dialect.
3. `docker compose up` booted the `trinodb/trino` coordinator with the
   default `tpch` and `memory` catalogs registered.
4. `rocky run` issued, against `/v1/statement`:
   `CREATE SCHEMA IF NOT EXISTS "memory"."rocky_demo"` then
   `CREATE TABLE "memory"."rocky_demo"."nations_copy" AS SELECT ... FROM tpch.tiny.nation`.
5. The shell-side row-count assertion polled the same endpoint via curl
   and confirmed 25 rows landed in `memory.rocky_demo.nations_copy`.

## Related

- Rocky Trino adapter source: `engine/crates/rocky-trino/`
- Trino dialect tests: `engine/crates/rocky-trino/src/dialect.rs`
- Adapter SDK skeleton (Rust starter for warehouses Rocky doesn't ship
  in-tree): [`07-adapters/06-rust-native-adapter-skeleton`](../06-rust-native-adapter-skeleton/)
