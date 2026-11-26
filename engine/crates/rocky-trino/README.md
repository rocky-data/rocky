# rocky-trino

Trino warehouse adapter for [Rocky](https://rocky-data.dev/).

Implements `rocky_core::traits::WarehouseAdapter` and `rocky_core::traits::SqlDialect` against Trino's [`POST /v1/statement`](https://trino.io/docs/current/develop/client-protocol.html) HTTP API. It is hand-rolled over `reqwest`, with no Trino client crate.

## Status: beta

Coverage is narrow. The Docker conformance harness behind the `trino-conformance` cargo feature exercises the supported surface (see [Conformance harness](#conformance-harness)). `rocky validate` does not check the gaps (MERGE, OAuth/Kerberos, governance, loader, batch checks). A model with `strategy = "merge"` and a pipeline with `auto_create_catalogs = true` both pass validation. MERGE fails later, when the dialect refuses to build the statement. Governance calls go to a no-op adapter: they succeed and change nothing.

## Configuration (`rocky.toml`)

Run `rocky init --template trino <dir>` to scaffold a Trino project. Put the coordinator URL in `host`. The credential field you set selects the auth mode. The registry reuses the shared `AdapterConfig` slots (`host`, `username`, `password`, `token`, `database`, `timeout_secs`). There is no Trino-specific TOML schema.

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
username = "alice"               # needed: populates X-Trino-User
token    = "${TRINO_JWT}"        # if password is also set, the token wins
database = "iceberg"
```

How the registry reads these fields (`engine/crates/rocky-cli/src/registry.rs`):

- `host` is required (the error names it the *coordinator URL*).
- When `token` is set, the registry selects JWT mode and ignores `password`.
- For Basic auth (no `token`), `username` and `password` are both required.
- For JWT auth, the registry does not require `username`, but you need it. Trino requires the `X-Trino-User` header on every request, and Rocky does not infer it from the token's `sub` claim. Without `username`, the config loads and the first query fails with `no X-Trino-User available`.
- `database` maps to Trino's *catalog* (the `X-Trino-Catalog` header). No pipeline-level default schema is threaded through, so model SQL should use fully qualified `<catalog>.<schema>.<table>` names.
- `auto_create_catalogs = true` is incompatible: OSS Trino has no `CREATE CATALOG` SQL, and `TrinoDialect::create_catalog_sql` returns `None`. `rocky validate` accepts the key, the run skips the step, and the catalog must already exist on the coordinator.

## Authentication

Implemented in `src/auth.rs`:

| Mode | `Authorization` header | When to use |
|---|---|---|
| HTTP Basic | `Basic base64(<user>:<password>)` | Self-hosted Trino with `password-authenticator.properties` |
| JWT bearer | `Bearer <token>` | Managed offerings (e.g. Starburst Galaxy PATs) and any coordinator with a JWKS-validated token |

Both credential variants wrap the secret in `rocky_core::redacted::RedactedString`. The `Authorization` header is marked `set_sensitive(true)`, so `reqwest` and `tracing` debug output elide it. `Debug` on `TrinoAuth` prints `***`. OAuth 2.0, Kerberos and SPNEGO are out of scope for v0.

## Wire protocol

`TrinoClient::execute` (`src/connector.rs`) drives the `/v1/statement` polling state machine:

1. `POST /v1/statement` with the SQL as the **plain-text** body (Trino rejects JSON-wrapped statements). Headers: `Authorization`, `X-Trino-User`, `X-Trino-Source` (`rocky`, visible in `system.runtime.queries`), and optional `X-Trino-Catalog` / `X-Trino-Schema`.
2. Parse the `QueryResults` envelope. While `nextUri` is set, `GET nextUri`, concatenate `data` rows, and keep the first non-empty `columns` slice. The client refuses a `nextUri` whose scheme, host or port differs from the coordinator URL. It returns `TrinoError::UntrustedNextUri` and does not send `Authorization` to that host.
3. When `nextUri` is absent, the query is terminal. `stats.state == "FINISHED"` returns `Ok`. `FAILED`, `CANCELED` or anything else maps to `TrinoError::QueryFailed` with the structured `error.errorCode` / `errorName` / `message`.
4. Polls wait `[50, 100, 250, 500, 1000] ms` for the first five, then grow exponentially, capped at 5 s. The deadline is `config.timeout` (default 300 s, `timeout_secs` overrides). Past it, the call returns `TrinoError::Timeout` with the last observed state.

Rows surface as `Vec<Vec<serde_json::Value>>`.

### Arrow result fetch (`fetch_arrow_batch`)

`TrinoAdapter` implements `WarehouseAdapter::fetch_arrow_batch` through the spooled-protocol Arrow path in `src/arrow_stream.rs`. The client sends `X-Trino-Client-Capabilities: SPOOLING` (segmented `data` envelope instead of inline JSON rows) and `X-Trino-Spooled-Segments-Accept-Encoding: arrow+zstd,arrow` (the coordinator picks the first it supports for the whole query).

Each spooled segment is an inline base64 Arrow IPC stream (small payloads) or a `uri` to fetch raw IPC bytes (typically a presigned object-store URL). The adapter decodes each with `arrow::ipc::reader::StreamReader`, joins them with `arrow::compute::concat_batches`, and best-effort acks each via its `ackUri`.

**Version gate.** Arrow IPC is a **proposed** spooling encoding ([`trinodb/trino#26365`](https://github.com/trinodb/trino/pull/26365), closed stale Nov 2025). Trino 481 (May 2026) advertises only `json`, `json+lz4` and `json+zstd`, so the coordinator falls back to inline JSON rows. The adapter detects the fallback and returns `TrinoError::ArrowEncodingUnavailable` (wrapped in `AdapterError`) instead of a synthetic batch. The negotiation is in place, so no change is needed if upstream merges.

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
| `watermark_where` | `WHERE <col> > TIMESTAMP '<last watermark>'`, with the literal `TIMESTAMP '1970-01-01 00:00:00'` when no watermark exists yet |
| `describe_table_sql` | `DESCRIBE <ref>` (Trino native, returns `(Column, Type, Extra, Comment)`) |
| `drop_table_sql` | `DROP TABLE IF EXISTS <ref>` |
| `create_catalog_sql` | `None` — OSS Trino catalogs are server-side connector instances |
| `create_schema_sql` | `CREATE SCHEMA IF NOT EXISTS "c"."s"` |
| `tablesample_clause(n)` | `TABLESAMPLE BERNOULLI (<n>)` (per-row sampling, matches Rocky's null-rate-check semantics) |
| `insert_overwrite_partition` | Two-statement `DELETE FROM ... WHERE <pred>` + `INSERT INTO`. **Not atomic**: Trino's REST API runs each statement in its own transaction, so if the `INSERT` fails the `DELETE` stays committed and the partition is left empty. True `INSERT OVERWRITE` requires Iceberg-specific wiring deferred to a follow-up |
| `row_hash_expr` | Trait default — errors at bisection time |

Type mapping is implicit. `describe_table` parses Trino's `DESCRIBE` output into `(name, data_type, nullable=true)` per column. `data_type` passes through verbatim (`bigint`, `varchar(64)`, `timestamp(6)`, `decimal(18,2)`), so drift detection compares native Trino types. v0 reports `nullable = true` always, because strict nullability lives in `information_schema.columns`, not `DESCRIBE`. Until that is wired, the drift planner errs toward widening rather than DropAndRecreate.

## Not in v0 (follow-ups)

- **MERGE.** Trino's MERGE landed in 414 (2023) but depends on the connector (Iceberg yes, Hive limited). The dialect returns an error. `rocky validate` does not catch `strategy = "merge"`.
- **OAuth 2.0 / Kerberos / SPNEGO auth.** Basic + JWT only.
- **Governance / loader / batch checks.** `GRANT` semantics depend on the connector. The crate exports no `TrinoGovernanceAdapter` or `TrinoBatchCheckAdapter`.
- **`row_hash_expr`.** Trino has `xxhash64(varbinary)` and `to_hex(sha256(...))`, but neither is wired, so checksum-bisection diff is missing.
- **True `INSERT OVERWRITE`.** Iceberg catalogs support it. v0 uses a non-atomic `DELETE` + `INSERT`.
- **`information_schema`-backed nullability** in `describe_table`.
- **Arrow record batches.** `fetch_arrow_batch` exists, but every shipping coordinator returns `ArrowEncodingUnavailable`. See [Arrow result fetch](#arrow-result-fetch-fetch_arrow_batch).

For an end-to-end walkthrough (`docker compose up`, `rocky run`, row-count assertion), see `examples/playground/pocs/07-adapters/07-trino-docker/`.

## Testing

Unit tests sit beside the source and use [`wiremock`](https://crates.io/crates/wiremock) as a coordinator mock. They assert:

- the `Authorization: Basic ...` header structure, as a decoded round trip, because committing the encoded form trips GitHub's secret scanner (`auth.rs::tests::basic_auth_encodes_user_password`);
- `X-Trino-User`, `X-Trino-Catalog` and `X-Trino-Schema` propagation;
- the `nextUri` polling loop joining rows across pages (`connector.rs::tests::execute_polls_until_finished_and_concatenates_rows`);
- structured `QueryFailed` errors with `errorCode` + `errorName`, and timeout behaviour;
- `Debug` redaction for `Basic` and `Jwt`;
- `DESCRIBE` parsing through `WarehouseAdapter::describe_table`.

Basic-auth fixtures live in `src/test_helpers.rs` (`test_basic_auth()`, `test_basic_auth_inputs()`). They read `std::env::var(...).unwrap_or_else(...)` so CodeQL's `rust/hard-coded-cryptographic-value` rule does not fire. CI can override them with `ROCKY_TRINO_TEST_USER` / `ROCKY_TRINO_TEST_PASS`.

```bash
cargo test -p rocky-trino
```

The unit suite needs no live Trino.

## Conformance harness

The `trino-conformance` feature gates [`tests/conformance.rs`](tests/conformance.rs), which drives the adapter against a real coordinator. It is off by default, so `cargo test -p rocky-trino` stays credential- and network-free. All five network tests are also `#[ignore]`, so `cargo test --all-features` (as CI runs it) skips them. Running them needs both the feature and `-- --ignored`.

- `round_trip_select_one`: `SELECT 1` against the live coordinator.
- `round_trip_create_insert_select_drop`: the `WarehouseAdapter` round trip on the writable `memory` connector (`CREATE TABLE AS`, `INSERT INTO`, `SELECT *`, `DROP TABLE` on `memory.default.<unique_table>`, refs from `TrinoDialect::format_table_ref`).
- `fetch_arrow_batch_against_live_trino_surfaces_version_gate`: asserts `ArrowEncodingUnavailable` until upstream ships Arrow spooling.
- `literal_escape_round_trips_live`: string literals with quotes and backslashes round-trip byte-identical.
- `drift_widening_alters_iceberg_in_place`: an Iceberg widening through `ALTER ... SET DATA TYPE`. It needs the stack in `tests/iceberg/docker-compose.yml` and prints a skip message when the coordinator has no `iceberg` catalog.

The harness reads the coordinator from `${TRINO_HOST:-localhost}` and `${TRINO_PORT:-8080}` and authenticates through the JWT path with a dummy token. (`trinodb/trino:latest` rejects non-empty Basic passwords over plain HTTP, "Password not allowed for insecure authentication", but accepts any non-empty JWT.) It sets `X-Trino-User` through `TrinoClientConfig::with_user`. Table names are salted with `SystemTime::now()` so reruns do not collide.

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
├── src/
│   ├── lib.rs         ← module root + re-exports + crate-level docs
│   ├── adapter.rs     ← `TrinoAdapter`: WarehouseAdapter impl
│   ├── arrow_stream.rs ← spooled-protocol Arrow fetch (`fetch_arrow_batch`)
│   ├── auth.rs        ← `TrinoAuth`: Basic + JWT, RedactedString-wrapped
│   ├── connector.rs   ← `TrinoClient`: /v1/statement state machine
│   ├── dialect.rs     ← `TrinoDialect`: SqlDialect impl
│   └── test_helpers.rs ← centralised basic-auth fixtures (cfg(test))
└── tests/
    ├── conformance.rs ← live-coordinator harness (`trino-conformance` feature)
    └── iceberg/       ← docker-compose stack for the Iceberg drift test
```

Public types are re-exported at the crate root: `TrinoAdapter`, `TrinoAuth`, `TrinoDialect`, `TrinoClient`, `TrinoClientConfig` (alias `TrinoConfig`), and `TrinoQueryRows` / `TrinoColumnMeta`.

## License

Apache 2.0, same as the rest of the Rocky engine workspace.
