---
title: Fivetran state cache
description: Pluggable persistent cache for Rocky's Fivetran state envelope, sharing one fetcher per org across processes.
sidebar:
  order: 11
---

Stop every `rocky` process from re-fetching the same Fivetran metadata. Give the adapter a persistent cache and concurrent processes against one Fivetran org share their discover fetches: the first process pays the API cost, and every process after it reads the same envelope from the cache until the window closes.

## Why

Without a shared cache, every `rocky` process starts from nothing:

```
   process 1        process 2        process 3
       │                │                │
       ▼                ▼                ▼
   GET /destinations/{id}                       ─┐
   GET /groups/{id}/connectors  (paginated)      │  ~50+ calls
   GET /connectors/{id}/schemas × N connectors  ─┘  per process

                    Fivetran API  ──►  429 Too Many Requests
```

For a 50-connector tenant that is more than 50 calls per cold start. When several processes converge in one window — a sensor fan-out, say — they exhaust the org's rate-limit budget and Fivetran answers with 429s.

The cache removes the repetition. A steady-state tenant pays one discover cycle per cache window instead of one per process:

```
   process 1 ──► cache MISS ──► Fivetran API ──► write envelope
   process 2 ──► cache HIT  ──► envelope
   process 3 ──► cache HIT  ──► envelope
```

Together with the per-host rate-limit budget (engine-v1.37.0), this is the cold-start equivalent of an asset cache.

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

Omit the `[adapter.<name>.cache]` block and the backend defaults to `"none"`. The adapter then behaves exactly as it did before the cache existed: every fetch goes straight to the Fivetran API.

## Backends

### `none` (default)

No cache at all. Every fetch hits the Fivetran API. Fine for local development, where the API budget does not matter.

### `file`

JSON files on the local filesystem under `file_root`. The cheapest backend: no external service, no credentials. Use it for a single-host deployment, or for a CI run that wants several Rocky invocations on one machine to share a single discover.

```toml
[adapter.fivetran_main.cache]
backend = "file"
file_root = ".rocky/fivetran-state/"
```

Files land at `<account_hash>/<destination_id>.json` under `file_root`. The `account_hash` is a short token derived from the Fivetran API key with SHA-256. Two orgs sharing one root therefore never collide on the same destination id.

### `object_store`

S3, GCS, Azure, or a `file://` path, through the [`object_store`](https://docs.rs/object_store/) crate. It works across processes and machines, it is durable, and it needs no cache service of its own.

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

**Credentials** come from each SDK's own default provider chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, an IAM role, or `~/.aws/credentials` for S3; `GOOGLE_APPLICATION_CREDENTIALS` for GCS; and so on. Rocky adds no credential surface of its own for cloud storage.

The backend writes single-part PUTs. It compares the response ETag, an MD5 for a single part, against the new bytes. No PUT crosses the wire when the envelope has not changed. It caps the serialized envelope at 5 MB to keep that comparison safe. Real envelopes for a 57-connector tenant land between 60 and 120 KB.

### `valkey`

Redis or Valkey. Sub-millisecond reads, so this is the hot path for sensors and sync detection that need fresh-ish data fast. The `valkey` Cargo feature gates it.

```toml
[adapter.fivetran_main.cache]
backend = "valkey"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

Every key sits under the `fivetran-state:` namespace, so you can list them with `KEYS fivetran-state:*`. Every `SET` carries the configured TTL, so the layer never accumulates stale envelopes.

### `tiered`

Valkey in front, an object store behind. A read tries Valkey first; on a miss it falls through to the object store and writes the result back into Valkey. A write goes to both.

```toml
[adapter.fivetran_main.cache]
backend = "tiered"
object_store_url = "s3://my-bucket/rocky/fivetran-state/"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

Use this in production. Valkey serves the hot path fast, and the object store keeps the envelope through a Valkey outage or a pod restart.

## `--no-cache` flag

`rocky discover --no-cache` skips the cache read for this one invocation and fetches from the API. Rocky still writes the fresh envelope back, so the next invocation sees the up-to-date data.

```bash
rocky discover --emit-fivetran-state-to /tmp/state.json --no-cache
```

Reach for it when you suspect the cache has gone stale, after rolling a Fivetran credential for instance. It gives you the next envelope straight from the wire.

## Hash-dedupe

Every backend hashes the envelope before writing it, excluding `fetched_at`, and compares that [digest](/reference/glossary/#digest) against what is already cached. Matching hashes mean the write does nothing:

- `FileCache` — skips the rename entirely; on-disk mtime stays stable.
- `ObjectStoreCache` — HEAD the existing object; if its ETag matches `MD5(new_bytes)`, skip the PUT.
- `ValkeyCache` — `GET` the prior value; if hashes match, skip the `SET`.
- `TieredCache` — both layers run their own dedupe.

This is the herd protection: however many processes converge on an identical envelope, the cache is written at most once per window.

## Observability

Every cache decision emits a span event over [OTLP](/reference/glossary/#otlp-opentelemetry-protocol), the wire format OpenTelemetry uses, and publishes the same event on the in-process pipeline event bus:

| Event | Fields |
|---|---|
| `fivetran.cache_hit` | `backend`, `key`, `age_seconds` |
| `fivetran.cache_miss` | `backend`, `key`, `reason` (`"no-entry"` / `"refresh-forced"`) |
| `fivetran.cache_write` | `backend`, `key`, `bytes`, `outcome` (`"written"`) |
| `fivetran.cache_write_skipped` | `backend`, `key`, `reason` (`"hash-match"`) |
| `fivetran.cache_write_failed` | `backend`, `key`, `error` |

The cache fails open. When a backend is unreachable Rocky still fetches over HTTP, so a broken cache slows a run down but never stops it. Each failure emits a `cache_write_failed` event and a warning log line, so you can alert on a backend that fails consistently.

## Volume reduction

Take a 57-connector tenant running 5 processes with 2 to 8 sensor triggers an hour. The cache plus the rate-limit budget cut steady-state Fivetran calls from roughly 600–2400 an hour to roughly 80.
