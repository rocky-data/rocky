---
title: Fivetran state cache
description: Pluggable persistent cache for Rocky's Fivetran state envelope — share one fetcher per org across processes.
sidebar:
  order: 9
---

Rocky's Fivetran adapter can be configured with a persistent state cache so concurrent `rocky` processes against a single Fivetran org share their discover fetches. The first process pays the API cost; every subsequent process within the cache window reads the canonical envelope from the cache layer.

## Why

Without a shared cache, every `rocky` process boots cold:

1. `GET /v1/destinations/{id}`
2. `GET /v1/groups/{id}/connectors` (paginated)
3. `GET /v1/connectors/{conn_id}/schemas` for each connector

For a 50-connector tenant this is ~50+ calls per cold start. When several processes converge in a window (e.g. a sensor fan-out), the org's rate-limit budget gets exhausted and Fivetran responds with 429s.

The state cache dedupes (1)-(3) so a steady-state tenant pays one discover cycle per cache window, not N per cold start. Combined with the per-host rate-limit budget (shipped in engine-v1.37.0), this is the cold-start equivalent of an asset cache.

## Configuration

Add an `[adapter.<name>.cache]` block under the Fivetran adapter you want to cache:

```toml
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
destination_id = "popularity_cultivator"

[adapter.fivetran_main.cache]
backend = "tiered"
file_root = ".rocky/fivetran-state/"
object_store_url = "s3://my-bucket/rocky/fivetran-state/"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

When the `[adapter.<name>.cache]` block is absent the cache layer defaults to `backend = "none"` and the adapter behaves exactly as it did before the cache layer landed — every fetch goes straight to the Fivetran API.

## Backends

### `none` (default)

No persistent cache. Every fetch goes to the Fivetran API. Useful for local development where the API budget isn't a concern.

### `file`

Local-filesystem JSON files under `file_root`. Cheapest backend — no external dependency, no credentials. Suitable for single-process deployments and CI runs that want to dedupe a single discover across multiple Rocky invocations on the same host.

```toml
[adapter.fivetran_main.cache]
backend = "file"
file_root = ".rocky/fivetran-state/"
```

The directory layout under `file_root` is `<account_hash>/<destination_id>.json`. `account_hash` is a short SHA-256-derived token of the Fivetran API key — different orgs sharing one root never collide on a destination id.

### `object_store`

S3 / GCS / Azure / `file://`-backed cache via the [`object_store`](https://docs.rs/object_store/) crate. Cross-process, durable, no separate cache process to run.

```toml
[adapter.fivetran_main.cache]
backend = "object_store"
object_store_url = "s3://my-bucket/rocky/fivetran-state/"
```

Supported URL schemes:

| Scheme | Backend |
|---|---|
| `s3://bucket/prefix/` | AWS S3 |
| `gs://bucket/prefix/` | Google Cloud Storage |
| `az://container/prefix/` | Azure Blob Storage |
| `file:///absolute/path/` | Local filesystem (mainly for testing) |

**Credentials** are resolved through each SDK's default provider chain — `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / IAM role / `~/.aws/credentials` for S3, `GOOGLE_APPLICATION_CREDENTIALS` for GCS, and so on. Rocky doesn't introduce its own credential surface for cloud storage.

The backend writes single-part PUTs and uses the response ETag (MD5 for single-part) to dedupe re-writes of identical envelopes — no PUT goes over the wire when the bytes haven't changed. The serialized envelope is capped at 5 MB to keep dedupe safe; real envelopes for a 57-connector tenant land around 60-120 KB.

### `valkey`

Redis / Valkey backend. Sub-millisecond reads, the hot path for sensors and sync detection that need fresh-ish data fast. Gated by the `valkey` Cargo feature.

```toml
[adapter.fivetran_main.cache]
backend = "valkey"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

Keys are namespaced under `fivetran-state:` so operator-side inspection is trivial (`KEYS fivetran-state:*`). Every `SET` carries the configured TTL so the layer never accumulates stale envelopes.

### `tiered`

Composes Valkey (primary, fast) + object-store (secondary, durable). Reads hit Valkey first; on miss they fall through to the object store and write back to Valkey. Writes go to both layers.

```toml
[adapter.fivetran_main.cache]
backend = "tiered"
object_store_url = "s3://my-bucket/rocky/fivetran-state/"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

This is the recommended production configuration — Valkey gives operators fast hot-path reads while S3 keeps the canonical envelope durable across Valkey outages and pod restarts.

## `--no-cache` flag

`rocky discover --no-cache` skips the cache read on the current invocation and forces a fresh API fetch. The fresh envelope is still written back to the cache so a subsequent invocation sees the up-to-date data.

```bash
rocky discover --emit-fivetran-state-to /tmp/state.json --no-cache
```

Useful when an operator suspects the cache is stale (e.g. after rolling a Fivetran credential) and wants the next envelope to come straight from the wire.

## Hash-dedupe

Every backend's write path computes the envelope hash (excluding `fetched_at`) and compares it with the prior cached value. If the hashes match the write is a no-op:

- `FileCache` — skips the rename entirely; on-disk mtime stays stable.
- `ObjectStoreCache` — HEAD the existing object; if its ETag matches `MD5(new_bytes)`, skip the PUT.
- `ValkeyCache` — `GET` the prior value; if hashes match, skip the `SET`.
- `TieredCache` — both layers run their own dedupe.

This is the cold-start herd protection: N processes converging on identical envelopes write at most once per cache window.

## Observability

Every cache decision emits an OTLP span event on the active trace, also published on the in-process pipeline event bus:

| Event | Fields |
|---|---|
| `fivetran.cache_hit` | `backend`, `key`, `age_seconds` |
| `fivetran.cache_miss` | `backend`, `key`, `reason` (`"no-entry"` / `"refresh-forced"`) |
| `fivetran.cache_write` | `backend`, `key`, `bytes`, `outcome` (`"written"`) |
| `fivetran.cache_write_skipped` | `backend`, `key`, `reason` (`"hash-match"`) |
| `fivetran.cache_write_failed` | `backend`, `key`, `error` |

Cache failures are fail-open — the HTTP path runs regardless. Failures show up as `cache_write_failed` events plus a `warn!` log line so ops can alert on a pathologically failing backend.

## Volume reduction

The cache layer dedupes correctness — it prevents redundant writes when the bytes haven't changed. The cold-start herd reduction depends on the per-host rate-limit budget (shipped in engine-v1.37.0) serializing concurrent fetches so the first process pays the API cost and subsequent processes see the populated cache.

For a 57-connector tenant with 5 active processes and 2-8 sensor triggers per hour, the combined effect cuts steady-state Fivetran call volume from ~600-2400 calls/hour to ~80 calls/hour.
