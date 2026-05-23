//! Docker-backed conformance harness for the Trino adapter.
//!
//! Gated behind the `trino-conformance` Cargo feature so the default
//! `cargo test -p rocky-trino` run stays credential- and network-free.
//! When the feature is enabled this file expects a Trino coordinator
//! reachable at `http://${TRINO_HOST:-localhost}:${TRINO_PORT:-8080}`.
//! The upstream `trinodb/trino:latest` image works as-is, no auth
//! configuration required: the harness uses the JWT-bearer path with
//! a dummy token because Trino refuses non-empty Basic-auth passwords
//! over plain HTTP, and the default config doesn't validate JWT
//! bearers against a JWKS.
//!
//! Run with:
//!
//! ```bash
//! docker run -d --rm -p 8080:8080 --name rocky-trino-conformance \
//!     trinodb/trino:latest
//! # wait until /v1/info reports {"starting":false}
//! cargo test -p rocky-trino --features trino-conformance -- --ignored
//! ```
//!
//! What it covers:
//!
//! - `TrinoDialect::format_table_ref` round-trips against the live
//!   coordinator (the dialect's identifier-quoting contract has to
//!   match what Trino's parser actually accepts).
//! - The full `WarehouseAdapter` round-trip via the writable `memory`
//!   connector: `CREATE TABLE` (CTAS), `INSERT INTO`, `SELECT *`,
//!   `DROP TABLE`. Every statement flows through the same
//!   `/v1/statement` polling state machine the unit tests exercise via
//!   `wiremock`, but here it lands on a real Trino coordinator.
//!
//! The test uses a unique table name per run so re-running the suite
//! against the same long-lived container doesn't trip on leftover
//! state from a prior run.

#![cfg(feature = "trino-conformance")]

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rocky_core::traits::WarehouseAdapter;
use rocky_trino::{TrinoAdapter, TrinoAuth, TrinoClientConfig};

/// Coordinator URL assembled from `TRINO_HOST` (default `localhost`) +
/// `TRINO_PORT` (default `8080`). Matches the conventions in
/// `examples/playground/pocs/07-adapters/07-trino-docker/`.
fn coordinator_url() -> String {
    let host = std::env::var("TRINO_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("TRINO_PORT").unwrap_or_else(|_| "8080".into());
    format!("http://{host}:{port}")
}

/// Per-run table name in `memory.default` so concurrent / repeat runs
/// don't collide. Trino's `memory` connector keeps tables for the
/// container's lifetime, so re-running against the same container
/// without cleanup would otherwise fail at CTAS.
fn unique_table_name() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("rocky_conformance_{nanos}")
}

fn build_adapter() -> TrinoAdapter {
    // The stock `trinodb/trino` image runs without any
    // `password-authenticator.properties`, which means Trino refuses
    // a non-empty Basic-auth password over plain HTTP ("Password not
    // allowed for insecure authentication"). The JWT-bearer path is
    // not validated against a JWKS in the default config either, so
    // a dummy bearer token threads cleanly through the wire-protocol
    // state machine without needing TLS or a real authenticator.
    // The `X-Trino-User` header still has to be set explicitly because
    // JWT auth doesn't infer one — we route the value via
    // `TrinoClientConfig::with_user`, mirroring the production
    // recommendation in the README.
    let auth =
        TrinoAuth::jwt("rocky-conformance-dummy-token").expect("jwt auth fixture should construct");
    let cfg = TrinoClientConfig::new(coordinator_url())
        .with_user("rocky")
        .with_timeout(Duration::from_secs(30));
    TrinoAdapter::new(cfg, auth)
}

#[test]
fn dialect_format_table_ref_matches_trino_quoting() {
    let adapter = build_adapter();
    let formatted = adapter
        .dialect()
        .format_table_ref("memory", "default", "rocky_test")
        .expect("identifiers are valid");
    // Trino's standard identifier quoting is double-quotes; the dialect
    // must emit a three-part `"catalog"."schema"."table"` reference.
    assert_eq!(formatted, "\"memory\".\"default\".\"rocky_test\"");
}

#[tokio::test]
#[ignore = "requires a live Trino coordinator at TRINO_HOST:TRINO_PORT (default localhost:8080); run with `--ignored`"]
async fn round_trip_select_one() {
    // The simplest possible query — exercises POST /v1/statement +
    // nextUri polling without touching any catalog.
    let adapter = build_adapter();
    let result = adapter
        .execute_query("SELECT 1 AS n")
        .await
        .expect("SELECT 1 should succeed against a live coordinator");
    assert_eq!(result.columns, vec!["n".to_string()]);
    assert_eq!(result.rows.len(), 1);
    let n = result.rows[0]
        .first()
        .expect("row has one column")
        .as_i64()
        .expect("column is an integer");
    assert_eq!(n, 1);
}

#[tokio::test]
#[ignore = "requires a live Trino coordinator at TRINO_HOST:TRINO_PORT (default localhost:8080); run with `--ignored`. Apache Arrow IPC is not yet supported by shipping Trino (current: 481, May 2026) — see upstream trinodb/trino#26365. Until that lands the test asserts the version-gated `ArrowEncodingUnavailable` error rather than a successful round-trip."]
async fn fetch_arrow_batch_against_live_trino_surfaces_version_gate() {
    // Live conformance receipt for `WarehouseAdapter::fetch_arrow_batch`.
    //
    // Until Apache Arrow IPC ships as a spooled-protocol encoding the
    // upstream coordinator falls back to inline-JSON `data` whenever
    // the client requests `arrow` / `arrow+zstd`. The adapter MUST
    // surface that gracefully — `ArrowEncodingUnavailable`, wrapped in
    // `AdapterError` — rather than panic or quietly degrade. When
    // Arrow encoding eventually lands upstream this test flips: drop
    // the assertion and read the `RecordBatch` rows back.
    let adapter = build_adapter();
    let result = adapter.fetch_arrow_batch("SELECT 1 AS n").await;
    match result {
        Ok(batch) => {
            // Forward-compat: when upstream Trino enables Arrow this
            // branch becomes the receipt. Until then it shouldn't fire.
            assert_eq!(
                batch.num_rows(),
                1,
                "Trino coordinator unexpectedly accepted Arrow encoding — \
                 update this assertion to verify the actual decoded row."
            );
        }
        Err(err) => {
            let msg = err.to_string();
            assert!(
                msg.contains("Arrow"),
                "expected ArrowEncodingUnavailable-style error against \
                 a pre-Arrow Trino, got: {msg}"
            );
        }
    }
}

#[tokio::test]
#[ignore = "requires a live Trino coordinator at TRINO_HOST:TRINO_PORT (default localhost:8080); run with `--ignored`"]
async fn round_trip_create_insert_select_drop() {
    // Exercises the full `WarehouseAdapter` surface against the writable
    // `memory.default` schema:
    //   1. CREATE TABLE AS (a single-row VALUES seed)
    //   2. INSERT INTO (append two more rows)
    //   3. SELECT (read the three rows back)
    //   4. DROP TABLE IF EXISTS (cleanup)
    let adapter = build_adapter();
    let dialect = adapter.dialect();
    let table = unique_table_name();
    let table_ref = dialect
        .format_table_ref("memory", "default", &table)
        .expect("identifiers are valid");

    // Belt-and-braces: drop first in case a previous run leaked state.
    adapter
        .execute_statement(&dialect.drop_table_sql(&table_ref))
        .await
        .expect("pre-test DROP should succeed");

    // 1) CREATE TABLE AS — VALUES list seeds one row.
    let ctas = dialect.create_table_as(&table_ref, "SELECT 1 AS id, 'alice' AS name");
    adapter
        .execute_statement(&ctas)
        .await
        .expect("CTAS should succeed against memory.default");

    // 2) INSERT INTO — append two rows.
    let insert = dialect.insert_into(
        &table_ref,
        "SELECT * FROM (VALUES (2, 'bob'), (3, 'carol')) AS t(id, name)",
    );
    adapter
        .execute_statement(&insert)
        .await
        .expect("INSERT should succeed");

    // 3) SELECT — read all three rows back, ordered for stable assertion.
    let select_sql = format!("SELECT id, name FROM {table_ref} ORDER BY id");
    let result = adapter
        .execute_query(&select_sql)
        .await
        .expect("SELECT should succeed");
    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows.len(), 3, "expected 3 rows after CTAS + INSERT");
    let ids: Vec<i64> = result
        .rows
        .iter()
        .map(|r| r[0].as_i64().expect("id is an integer"))
        .collect();
    assert_eq!(ids, vec![1, 2, 3]);
    let names: Vec<&str> = result
        .rows
        .iter()
        .map(|r| r[1].as_str().expect("name is a string"))
        .collect();
    assert_eq!(names, vec!["alice", "bob", "carol"]);

    // 4) DROP — cleanup so the container stays usable for re-runs.
    adapter
        .execute_statement(&dialect.drop_table_sql(&table_ref))
        .await
        .expect("DROP should succeed");
}
