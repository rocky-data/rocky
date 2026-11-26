---
title: Fivetran state cache
description: Pluggable persistent cache for Rocky's Fivetran state envelope, sharing one fetcher per org across processes.
sidebar:
  order: 11
---

Give the Fivetran adapter a persistent cache so concurrent processes against one org share their discover fetches. The first process pays the API cost and every process after it reads the same envelope until the window closes.

Without a shared cache every `rocky` process starts cold: `GET /destinations/{id}`, a paginated `GET /groups/{id}/connectors`, and one `GET /connectors/{id}/schemas` per connector. That is more than 50 calls per process for a 50-connector tenant, and several processes in one window (a sensor fan-out) exhaust the rate-limit budget and draw `429 Too Many Requests`.

```
   process 1 ──► cache MISS ──► Fivetran API ──► write envelope
   process 2 ──► cache HIT  ──► envelope
   process 3 ──► cache HIT  ──► envelope
```

## Configuration

Add an `[adapter.<name>.cache]` block under the Fivetran adapter. Omit it and `backend` defaults to `"none"`: every fetch goes straight to the API.

```toml
[adapter.fivetran_main]
type = "fivetran"
kind = "discovery"
api_key = "${FIVETRAN_API_KEY}"
api_secret = "${FIVETRAN_API_SECRET}"
destination_id = "popularity_cultivator"

[adapter.fivetran_main.cache]
backend = "tiered"
object_store_url = "s3://my-bucket/rocky/fivetran-state/"
valkey_url = "rediss://valkey.internal:6379/"
valkey_ttl_seconds = 600
```

| `backend` | Keys | Use it for |
|---|---|---|
| `none` (default) | | Local development, where the API budget does not matter. |
| `file` | `file_root` | One host, or a CI run where several invocations share one discover. No service, no credentials. Files land at `<account_hash>/<destination_id>.json` under `file_root`. The `account_hash` is a short SHA-256 token of the API key, so two orgs sharing a root never collide. |
| `object_store` | `object_store_url` | Many processes and machines, durable, no cache service. S3, GCS, Azure or `file://` through the [`object_store`](https://docs.rs/object_store/) crate. |
| `valkey` | `valkey_url`, `valkey_ttl_seconds` | The hot path for sensors and sync detection that need recent data. Keys sit under `fivetran-state:` (list with `KEYS fivetran-state:*`). Every `SET` carries the TTL, so stale envelopes never accumulate. |
| `tiered` | `object_store_url`, `valkey_url`, `valkey_ttl_seconds` | Production. A read tries Valkey, then the object store (and writes the result back into Valkey). A write goes to both, so the envelope survives a Valkey outage or a pod restart. |

`object_store` URL schemes: `s3://bucket/prefix/`, `gs://bucket/prefix/`, `az://container/prefix/`, and `file:///absolute/path/` (mainly for testing). Credentials come from each SDK's default provider chain (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, an IAM role or `~/.aws/credentials` for S3; `GOOGLE_APPLICATION_CREDENTIALS` for GCS). Rocky adds no credential surface of its own. The backend writes single-part PUTs, compares the response ETag (an MD5 for a single part) against the new bytes, and caps the serialized envelope at 5 MB to keep that comparison safe.

## `--no-cache` flag

`rocky discover --no-cache` skips the cache read for one invocation and fetches from the API. Rocky still writes the fresh envelope back. Use it when you suspect the cache is stale, for instance after rolling a Fivetran credential:

```bash
rocky discover --emit-fivetran-state-to /tmp/state.json --no-cache
```

## Hash-dedupe

Every backend hashes the envelope before writing it, excluding `fetched_at`, and compares that [digest](/reference/glossary/#digest) against what is cached. A match means the write does nothing. However many processes converge on an identical envelope, the cache is written at most once per window.

| Backend | How it skips |
|---|---|
| `FileCache` | Skips the rename, so on-disk mtime stays stable. |
| `ObjectStoreCache` | HEADs the object. If its ETag matches `MD5(new_bytes)`, it skips the PUT. |
| `ValkeyCache` | `GET`s the prior value. If hashes match, it skips the `SET`. |
| `TieredCache` | Both layers run their own dedupe. |

## Observability

Every cache decision emits a span event over [OTLP](/reference/glossary/#otlp-opentelemetry-protocol) and publishes the same event on the in-process pipeline event bus:

| Event | Fields |
|---|---|
| `fivetran.cache_hit` | `backend`, `key`, `age_seconds` |
| `fivetran.cache_miss` | `backend`, `key`, `reason` (`"no-entry"` / `"refresh-forced"`) |
| `fivetran.cache_write` | `backend`, `key`, `bytes`, `outcome` (`"written"`) |
| `fivetran.cache_write_skipped` | `backend`, `key`, `reason` (`"hash-match"`) |
| `fivetran.cache_write_failed` | `backend`, `key`, `error` |

The cache fails open. When a backend is unreachable Rocky still fetches over HTTP, so a broken cache slows a run but never stops it. Each failure emits `cache_write_failed` and a warning, so you can alert on a backend that fails consistently.

## Cross-process coordination

Three more optional blocks on the same adapter coordinate processes. Each fails open: when its backend is unreachable, Rocky calls the API directly. A `valkey` backend without `valkey_url` fails the config load. Rocky ignores these blocks on every adapter type except `fivetran`.

| Block | Default | What it does |
|---|---|---|
| `[adapter.<name>.ratelimit]` | `backend = "file"`: a per-host budget under `${TMPDIR}/rocky-fivetran-ratelimit/` | After one process gets a 429, the others on the same budget wait for the same `wake_at`. `backend = "valkey"` shares the budget across hosts. Keys: `valkey_url`, `max_wake_seconds` (default `600`). |
| `[adapter.<name>.stampede]` | `backend = "none"`: every process is the leader | On a cold cache, one leader calls the API and the others poll the cache. Keys: `valkey_url`, `lock_ttl_seconds` (default `60`), `poll_timeout_seconds` (default `30`). |
| `[adapter.<name>.circuit_breaker]` | `backend = "none"`: no breaker | Stops API calls for the whole org after repeated failures, then probes again after a cooldown. Keys: `valkey_url`, `failure_threshold` (default `5`), `window_seconds` (default `60`), `cooldown_seconds` (default `300`), `cooldown_max_seconds` (default `3600`). |

```toml
[adapter.fivetran_main.stampede]
backend = "valkey"
valkey_url = "rediss://valkey.internal:6379/"
```
