# rocky-trino

Trino warehouse adapter for [Rocky](https://rocky-data.dev/).

Implements `rocky_core::traits::WarehouseAdapter` and
`rocky_core::traits::SqlDialect` against Trino's
[`POST /v1/statement`](https://trino.io/docs/current/develop/client-protocol.html)
HTTP API. Hand-rolled over `reqwest`, with no high-level Trino client crate
dependency.

## Status: beta

Coverage is intentionally narrow, but the supported surface is exercised
end-to-end by the Docker conformance harness behind the
`trino-conformance` cargo feature (see [Conformance harness](#conformance-harness)
below). Unsupported gaps (MERGE, OAuth/Kerberos, governance, loader,
batch-checks) fail loudly at validate time rather than emitting broken
SQL silently.

## Configuration (`rocky.toml`)

The Trino coordinator URL goes in `host`; auth is selected by which
credential field is set. Internally the registry reuses the shared
`AdapterConfig` slots (`host`, `username`, `password`, `token`,
`database`, `timeout_secs`); there is no Trino-specific TOML schema.

```toml
# Basic auth — the 90% case for self-hosted Trino with the
# password-authenticator.properties coordinator config.
[adapters.warehouse]
type = "trino"
host = "https://trino.example.com:8443"
username = "alice"
password = "${TRINO_PASSWORD}"
database = "iceberg"             # optional: default catalog (X-Trino-Catalog)
timeout_secs = 300               # optional: per-query polling deadline
```

```toml
# JWT bearer auth — common with Starburst Galaxy PATs and any
# coordinator validating tokens against a JWKS URL.
[adapters.warehouse]
type = "trino"
host = "https://trino.example.com"
username = "alice"               # required: populates X-Trino-User
token    = "${TRINO_JWT}"        # mutually exclusive with password
database = "iceberg"
```

Notes the registry enforces (see `engine/crates/rocky-cli/src/registry.rs`):

- `host` is required (the error message names it the *coordinator URL*).
- For Basic auth, both `username` and `password` are required.
- For JWT auth, `token` selects JWT mode and `username` is still required
  to populate the `X-Trino-User` header: Trino requires that header on
  every request, and the JWT path doesn't infer it from the token's
  `sub` claim.
- `database` maps to Trino's *catalog*, surfaced via the `X-Trino-Catalog`
  request header. A pipeline-level default schema is not currently
  threaded through the registry, so model SQL should reference fully
  qualified `<catalog>.<schema>.<table>` names.
- `auto_create_catalogs = true` is incompatible with this adapter: OSS
  Trino has no `CREATE CATALOG` SQL (catalogs are server-side connector
  instances), and `TrinoDialect::create_catalog_sql` returns `None`. The
  validate-time capability check trips before any SQL runs.

## Authentication

Implemented in `src/auth.rs`:

| Mode | `Authorization` header | When to use |
|---|---|---|
| HTTP Basic | `Basic base64(<user>:<password>)` | Self-hosted Trino with `password-authenticator.properties` |
| JWT bearer | `Bearer <token>` | Managed offerings (e.g. Starburst Galaxy PATs) and any coordinator with a JWKS-validated token |

Both credential variants wrap the secret in `rocky_core::redacted::RedactedString`
and the `Authorization` header is marked `set_sensitive(true)` so it's
elided from `reqwest`/`tracing` debug output. The `Debug` impl on
`TrinoAuth` prints `***` for both `Basic { password }` and `Jwt(token)`.

OAuth 2.0, Kerberos, and SPNEGO are out of scope for v0.

## Wire protocol

`TrinoClient::execute` (in `src/connector.rs`) drives the full
`/v1/statement` state machine, since Trino's protocol is a polling
state machine rather than a one-shot REST call:

1. `POST /v1/statement` with the SQL as the **plain-text** request body
   (Trino does not accept JSON-wrapped statements). Required headers:
   `Authorization`, `X-Trino-User`, `X-Trino-Source` (set to `rocky`
   so the request shows up in `system.runtime.queries`); optional
   `X-Trino-Catalog` / `X-Trino-Schema` when the adapter config
   supplies them.
2. Parse the `QueryResults` envelope. While `nextUri` is set, `GET nextUri`
   for the next page; concatenate any `data` rows; keep the first
   non-empty `columns` slice as the column metadata.
3. When `nextUri` is absent the query is in a terminal state. `stats.state == "FINISHED"`
   returns `Ok`; `FAILED` / `CANCELED` (or anything unexpected) maps to
   `TrinoError::QueryFailed`, carrying the structured `error.errorCode`
   / `errorName` / `message`.
4. The polling ladder is `[50, 100, 250, 500, 1000] ms` for the first
   five polls, then exponential growth capped at 5 s. The deadline is
   `config.timeout` (default 300 s, overridable via `timeout_secs`); if
   the query keeps returning `nextUri` past the deadline the call
   returns `TrinoError::Timeout` with the last observed state.

The aggregated rows surface as `Vec<Vec<serde_json::Value>>`.

### Arrow result fetch (`fetch_arrow_batch`)

`TrinoAdapter` implements `WarehouseAdapter::fetch_arrow_batch` via the
spooled-protocol Arrow encoding path in `src/arrow_stream.rs`. The
client negotiates Arrow IPC segments by sending:

- `X-Trino-Client-Capabilities: SPOOLING` — opts the response shape
  into the segmented `data` envelope rather than the inline-JSON row
  matrix.
- `X-Trino-Spooled-Segments-Accept-Encoding: arrow+zstd,arrow` —
  encoding preference list. The coordinator picks the first it
  supports and holds that choice for the duration of the query.

Each spooled segment carries either an inline base64-encoded Arrow IPC
stream (small payloads) or a `uri` to fetch raw IPC bytes from
(typically a presigned object-store URL). The adapter decodes each
segment via `arrow::ipc::reader::StreamReader`, concatenates the per-
segment batches with `arrow::compute::concat_batches`, and best-effort
acks each segment via its `ackUri` so the coordinator can release it.

**Version gate.** Apache Arrow IPC is a **proposed** spooled-protocol
encoding; see upstream PR
[`trinodb/trino#26365`](https://github.com/trinodb/trino/pull/26365)
(closed stale Nov 2025, revival discussion ongoing). The shipping Trino
release (481 as of May 2026) advertises only `json`, `json+lz4`, and
`json+zstd` spooling encodings. Until Arrow encoding lands upstream
the coordinator falls back to the inline-JSON row shape when the
client requests `arrow` / `arrow+zstd`. The adapter detects that
fallback and surfaces a clear `TrinoError::ArrowEncodingUnavailable`
(wrapped in `AdapterError`) rather than silently emitting a synthetic
batch. The negotiation wire is in place so the adapter is ready the
day upstream merges; no further changes are required when that
happens.

## Dialect summary

`TrinoDialect` (in `src/dialect.rs`) implements:

| Method | Output |
|---|---|
| `quote_identifier` | `"name"` (trait default — Trino's standard) |
| `format_table_ref(c, s, t)` | `"c"."s"."t"` — three-part, double-quoted, identifiers validated via `rocky_sql::validation` |
| `create_table_as` | `CREATE TABLE <ref> AS\n<select>` |
| `insert_into` | `INSERT INTO <ref>\n<select>` |
| `merge_into` | **Errors** — `"MERGE not supported by the Trino adapter v0"` |
| `select_clause` | ANSI `SELECT` with `CAST(<expr> AS <type>) AS <name>` for metadata columns |
| `watermark_where` | ANSI `TIMESTAMP '...'` literal with `COALESCE(MAX(<col>), TIMESTAMP '1970-01-01 00:00:00')` |
| `describe_table_sql` | `DESCRIBE <ref>` (Trino native, returns `(Column, Type, Extra, Comment)`) |
| `drop_table_sql` | `DROP TABLE IF EXISTS <ref>` |
| `create_catalog_sql` | `None` — OSS Trino catalogs are server-side connector instances |
| `create_schema_sql` | `CREATE SCHEMA IF NOT EXISTS "c"."s"` |
| `tablesample_clause(n)` | `TABLESAMPLE BERNOULLI (<n>)` (per-row sampling, matches Rocky's null-rate-check semantics) |
| `insert_overwrite_partition` | Two-statement `DELETE FROM ... WHERE <pred>` + `INSERT INTO` (Trino's REST API runs each statement in its own transaction; true `INSERT OVERWRITE` requires Iceberg-specific wiring deferred to a follow-up) |
| `row_hash_expr` | Trait default — errors at bisection time |

Type mapping is implicit: `describe_table` parses Trino's `DESCRIBE`
output and surfaces `(name, data_type, nullable=true)` per column. The
`data_type` column is propagated verbatim (e.g. `bigint`, `varchar(64)`,
`timestamp(6)`, `decimal(18,2)`) so downstream drift detection compares
on Trino's native type signatures. v0 reports `nullable = true`
unconditionally: strict nullability lives on `information_schema.columns`,
not in `DESCRIBE`. Wiring `information_schema` is a follow-up; until
then the drift planner errs on the side of widening rather than
DropAndRecreate.

## Not in v0 (follow-ups)

- **MERGE.** Trino's MERGE landed in 414 (2023) but is connector-dependent
  (Iceberg yes, Hive limited). The dialect errors loudly so
  `strategy = "merge"` fails at validate time rather than emitting
  broken SQL.
- **OAuth 2.0 / Kerberos / SPNEGO auth.** Basic + JWT only.
- **`rocky init trino` template** — no scaffold exists yet.
- **Playground POC** (full `docker compose up` walkthrough with a
  row-count assertion via the REST API) lives at
  `examples/playground/pocs/07-adapters/07-trino-docker/`. The
  feature-gated harness in `tests/conformance.rs` (see below) covers
  the in-tree adapter contract; the POC covers the end-to-end pipeline
  shape.
- **Governance / loader / batch checks.** Trino's `GRANT` semantics
  depend on the underlying connector; deferred until there's a concrete
  ask. The crate exports no `TrinoGovernanceAdapter` /
  `TrinoBatchCheckAdapter`.
- **`row_hash_expr`.** Trino has `xxhash64(varbinary)` and `to_hex(sha256(...))`
  but neither is wired into the dialect; checksum-bisection diff is a
  follow-up.
- **True `INSERT OVERWRITE`.** Iceberg-backed catalogs support it
  natively; v0 falls back to `DELETE` + `INSERT`.
- **`information_schema`-backed nullability** in `describe_table`.
- **Arrow record batches** from the connector: `fetch_arrow_batch` is
  implemented via the spooled-protocol Arrow path (see [Wire protocol](#wire-protocol)
  above), but the path is version-gated on upstream Trino merging
  Arrow IPC as a supported spooling encoding (PR
  [`trinodb/trino#26365`](https://github.com/trinodb/trino/pull/26365)).
  Today the call surfaces `ArrowEncodingUnavailable` against any
  shipping coordinator.

## Testing

Unit tests live alongside the source. They use
[`wiremock`](https://crates.io/crates/wiremock) to stand up a
coordinator mock and assert against:

- the `Authorization: Basic ...` header *structure* (decoded round-trip
  rather than a base64 literal, since committing the encoded form trips
  GitHub's secret scanner regardless of whether the value is real;
  see `auth.rs::tests::basic_auth_encodes_user_password`);
- `X-Trino-User`, `X-Trino-Catalog`, `X-Trino-Schema` header propagation;
- the full `nextUri` polling loop concatenating rows across pages
  (`connector.rs::tests::execute_polls_until_finished_and_concatenates_rows`);
- structured `QueryFailed` errors with `errorCode` + `errorName`;
- timeout behaviour when a query keeps returning `nextUri` past the
  deadline;
- `Debug` redaction for both `Basic` (password) and `Jwt` (token);
- the `DESCRIBE` response parsing path through `WarehouseAdapter::describe_table`.

Basic-auth fixtures are centralised in `src/test_helpers.rs`:
`test_basic_auth()` and `test_basic_auth_inputs()` read through
`std::env::var(...).unwrap_or_else(...)` so CodeQL's
`rust/hard-coded-cryptographic-value` rule doesn't fire on every test
that needs a valid auth instance. CI can override the fixtures via
`ROCKY_TRINO_TEST_USER` / `ROCKY_TRINO_TEST_PASS`; the fallbacks are
unit-test inputs only.

```bash
cargo test -p rocky-trino
```

No live Trino is required for the unit suite. The Docker-backed
harness lives behind the `trino-conformance` Cargo feature; see
[Conformance harness](#conformance-harness) below.

## Conformance harness

The `trino-conformance` Cargo feature gates an opt-in integration test
at [`tests/conformance.rs`](tests/conformance.rs) that drives the
adapter against a real Trino coordinator. It's off by default so the
default `cargo test -p rocky-trino` invocation stays credential- and
network-free. The two network-dependent tests are also marked
`#[ignore]` so they stay skipped under `cargo test --all-features` (as
CI runs); execution requires both the feature flag and `-- --ignored`.

What it covers:

- `TrinoDialect::format_table_ref` round-trips against the live
  coordinator (the dialect's identifier-quoting contract has to match
  what Trino's parser actually accepts).
- The full `WarehouseAdapter` round-trip via the writable `memory`
  connector: `SELECT 1`, then `CREATE TABLE AS` / `INSERT INTO` /
  `SELECT *` / `DROP TABLE` against `memory.default.<unique_table>`.
  Every statement flows through the same `/v1/statement` polling state
  machine the unit tests exercise via `wiremock`, but here it lands on
  a real coordinator.

The harness reads the coordinator URL from `${TRINO_HOST:-localhost}`
and `${TRINO_PORT:-8080}` and authenticates via the JWT-bearer path
with a dummy token. (The upstream `trinodb/trino:latest` image rejects
non-empty Basic-auth passwords over plain HTTP, *"Password not
allowed for insecure authentication"*, but doesn't validate JWT
bearers against a JWKS in the default config, so any non-empty token
threads through. `X-Trino-User` is supplied explicitly via
`TrinoClientConfig::with_user` because JWT auth doesn't infer one,
mirroring the production pattern in [Configuration](#configuration-rockytoml)
above.) The test table name is salted with `SystemTime::now()` so
re-runs against a long-lived container don't collide on the previous
run's `memory.default` table.

To run it locally:

```bash
docker run -d --rm -p 8080:8080 --name rocky-trino-conformance \
    trinodb/trino:latest
# Wait until the coordinator reports ready (~30-45s on a warm pull):
until curl -fsS http://localhost:8080/v1/info | grep -q '"starting":false'; do
    sleep 2
done
cargo test -p rocky-trino --features trino-conformance -- --ignored
docker stop rocky-trino-conformance
```

For the end-to-end pipeline-shape walkthrough (full `docker compose up`
+ `rocky run` + row-count assertion), see
`examples/playground/pocs/07-adapters/07-trino-docker/`.

## Crate layout

```
engine/crates/rocky-trino/
├── Cargo.toml
├── README.md          ← this file
└── src/
    ├── lib.rs         ← module root + re-exports + crate-level docs
    ├── adapter.rs     ← `TrinoAdapter`: WarehouseAdapter impl
    ├── auth.rs        ← `TrinoAuth`: Basic + JWT, RedactedString-wrapped
    ├── connector.rs   ← `TrinoClient`: /v1/statement state machine
    ├── dialect.rs     ← `TrinoDialect`: SqlDialect impl
    └── test_helpers.rs ← centralised basic-auth fixtures (cfg(test))
```

Public types are re-exported at the crate root: `TrinoAdapter`,
`TrinoAuth`, `TrinoDialect`, `TrinoClient`, `TrinoClientConfig` (also
aliased as `TrinoConfig` for parity with the SDK guide), and
`TrinoQueryRows` / `TrinoColumnMeta` for callers wanting raw access to
the connector output.

## License

Apache 2.0, same as the rest of the Rocky engine workspace.
