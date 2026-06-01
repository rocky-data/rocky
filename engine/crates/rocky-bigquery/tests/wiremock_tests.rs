//! Wire-level mock tests for the BigQuery REST API connector
//! (`jobs.query` + `jobs.getQueryResults`).
//!
//! Uses wiremock to simulate the BigQuery REST API at the HTTP level so
//! the connector's execute / poll-loop / error-mapping / parse paths run
//! in normal CI with **no live credentials**. Until now `rocky-bigquery`
//! was the only warehouse adapter without HTTP-mock coverage — its
//! `tests/` held only live (`#[ignore]`) suites. BigQuery also has the
//! worst structural-bug history of the adapters, every bug in the
//! HTTP/parse path, so this closes a high-risk gap.
//!
//! ## Characterization, not aspiration
//!
//! Snowflake and Databricks classify 429/503 as transient and **retry**,
//! and map a job-level error (`status.error` / `status.errorResult`) to a
//! typed "statement failed" / "query rejected" variant. **BigQuery's
//! connector does neither today**: `run_query` / `poll_query_results`
//! make a single request and return `BigQueryError::ApiError` on any
//! non-2xx, and `BigQueryResponse` doesn't parse `status.errorResult` at
//! all. These tests therefore *pin current behavior* (429/503 →
//! `ApiError` on the first hit, no retry; a 200-with-`errorResult` body
//! is silently ignored and yields empty rows) rather than asserting a
//! retry/error-mapping feature that doesn't exist. Closing those gaps is
//! a follow-up feature PR, deliberately kept out of this test-only change.
//!
//! ## Test seam
//!
//! Token injection is free: `BigQueryAuth::Bearer` returns a static
//! bearer token with no OAuth exchange. The one production seam is
//! `BigQueryAdapter::with_base_url` (gated on `cfg(test)` /
//! `feature = "test-support"`), which points the REST URLs at the mock
//! server and rebuilds the HTTP client without `http2_prior_knowledge`
//! (wiremock serves HTTP/1.1; the production h2-prior-knowledge client
//! cannot connect to it). Production behavior is unchanged.

use rocky_bigquery::BigQueryAdapter;
use rocky_bigquery::auth::BigQueryAuth;
use rocky_bigquery::connector::BigQueryError;
use rocky_core::traits::{AdapterError, WarehouseAdapter};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Path of the `jobs.query` endpoint for the test project.
const QUERIES_PATH: &str = "/bigquery/v2/projects/test-project/queries";

/// Build an adapter with a static bearer token pointed at the mock server.
/// `with_timeout` defaults to the production 300 s so the poll deadline
/// doesn't bite on the happy paths; individual tests shrink it where the
/// timeout itself is under test.
fn test_adapter(server: &MockServer) -> BigQueryAdapter {
    let auth = BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
        "test-bq-token".into(),
    ));
    BigQueryAdapter::new("test-project", "EU", auth).with_base_url(server.uri())
}

/// Downcast an [`AdapterError`] to the concrete [`BigQueryError`] so tests
/// assert on the *mapped* variant (status / message / timeout) rather than
/// a bare `is_err()`. The public surface is the `WarehouseAdapter` trait,
/// which boxes the connector error into `AdapterError`; `AdapterError::inner`
/// exposes it for `downcast_ref`.
fn as_bq_error(err: &AdapterError) -> &BigQueryError {
    err.inner()
        .downcast_ref::<BigQueryError>()
        .unwrap_or_else(|| panic!("expected a BigQueryError, got: {err}"))
}

// ---------------------------------------------------------------------------
// (a) Happy path: execute + result parse
// ---------------------------------------------------------------------------

/// Synchronous `jobs.query` returns `jobComplete=true` inline with rows.
/// Asserts the actual parsed VALUES, including the BigQuery wire quirks the
/// deserializers handle: every scalar (even int64) comes back as a JSON
/// **string** (`"1"`, not `1`), and a null cell deserializes to
/// `Value::Null`.
#[tokio::test]
async fn happy_path_inline_result_parses_values() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .and(header("authorization", "Bearer test-bq-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-happy"},
            "schema": {
                "fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "name", "type": "STRING"},
                    {"name": "note", "type": "STRING"}
                ]
            },
            // BigQuery encodes int64 as a decimal *string*; a SQL NULL is a
            // JSON null in the `v` slot.
            "rows": [
                {"f": [{"v": "1"}, {"v": "alice"}, {"v": null}]},
                {"f": [{"v": "2"}, {"v": "bob"}, {"v": "hi"}]}
            ],
            "totalRows": "2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let result = adapter.execute_query("SELECT * FROM t").await.unwrap();

    assert_eq!(result.columns, vec!["id", "name", "note"]);
    assert_eq!(result.rows.len(), 2);

    // Row 0: string-encoded int64 stays a JSON string; null cell → Value::Null.
    assert_eq!(result.rows[0][0], json!("1"));
    assert_eq!(result.rows[0][1], json!("alice"));
    assert_eq!(result.rows[0][2], Value::Null);

    // Row 1: all present.
    assert_eq!(result.rows[1][0], json!("2"));
    assert_eq!(result.rows[1][1], json!("bob"));
    assert_eq!(result.rows[1][2], json!("hi"));
}

/// `execute_statement` ignores the result body and just confirms success —
/// the bearer token rides in the Authorization header.
#[tokio::test]
async fn execute_statement_success_sends_bearer_token() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .and(header("authorization", "Bearer test-bq-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-ddl"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    adapter
        .execute_statement("CREATE TABLE t (id INT64)")
        .await
        .expect("DDL statement should succeed");
    // If the Authorization header hadn't matched, the mock would not have
    // responded 200 and the call would have surfaced an ApiError.
}

// ---------------------------------------------------------------------------
// (b) Poll-loop: jobComplete=false on first response, completes on a later poll
// ---------------------------------------------------------------------------

/// `jobs.query` defers (`jobComplete=false` + a `jobReference`), so the
/// connector polls `jobs.getQueryResults`. First poll is still incomplete;
/// the second completes with rows. Verifies the loop submits once, polls
/// the right `queries/{jobId}` path, and parses the eventual result.
#[tokio::test]
async fn poll_loop_completes_after_deferred_job() {
    let server = MockServer::start().await;

    // Initial submit: async deferral.
    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": "test-project", "jobId": "job-poll"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let poll_path = "/bigquery/v2/projects/test-project/queries/job-poll";

    // First poll: still running.
    Mock::given(method("GET"))
        .and(path(poll_path))
        .and(query_param("location", "EU"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": "test-project", "jobId": "job-poll"}
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    // Second poll: done with a single row.
    Mock::given(method("GET"))
        .and(path(poll_path))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-poll"},
            "schema": {"fields": [{"name": "value", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "42"}]}],
            "totalRows": "1"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let result = adapter
        .execute_query("SELECT 42 AS value")
        .await
        .expect("deferred job should complete after polling");

    assert_eq!(result.columns, vec!["value"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("42"));
}

/// Deferred job (`jobComplete=false`) with **no** `jobReference` is
/// unpollable — the connector returns an `ApiError` ("missing
/// jobReference") rather than hanging or panicking.
#[tokio::test]
async fn deferred_job_without_reference_errors_cleanly() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("no jobReference to poll → error");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert_eq!(status, "missing jobReference");
            assert!(
                message.contains("cannot poll"),
                "message should explain the unpollable response, got: {message}"
            );
        }
        other => panic!("expected ApiError(missing jobReference), got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// (c) 429 / 503 — CHARACTERIZATION: current connector does NOT retry.
// ---------------------------------------------------------------------------
//
// SF/DBX classify these as transient and retry. BigQuery's connector has
// no retry layer (`new` takes no RetryConfig; `run_query` makes one
// request), so a 429 or 503 maps straight to `ApiError` on the first hit.
// These tests pin that: the error mock is `.expect(1)` and a "would only
// be hit on retry" success mock is `.expect(0)`, with `server.verify()`
// proving no second attempt fired. The BQ analog of "gives up after the
// budget" is the poll-loop `Timeout` path, covered separately below.

/// 429 (rate limited) is surfaced as `ApiError` on the first request, with
/// no retry. The mock is `.expect(1)`: had the connector retried, the
/// call count would be ≥2 and `verify()` would fail — proving the absence
/// of a retry layer. (SF/DBX classify 429 as transient and retry to a
/// success; BigQuery does not yet.)
#[tokio::test]
async fn rate_limit_429_maps_to_api_error_without_retry() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("429 should surface as an error");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("429"),
                "status should carry 429, got: {status}"
            );
            assert!(message.contains("rate limited"));
        }
        other => panic!("expected ApiError(429), got: {other:?}"),
    }
    // Exactly one request — `.expect(1)` fires on drop if the connector
    // retried (would be 2+) or never called (would be 0).
    server.verify().await;
}

/// 503 (service unavailable) likewise maps to `ApiError` on the first hit
/// with no retry. Pins that BigQuery does not yet treat 503 as transient.
#[tokio::test]
async fn service_unavailable_503_maps_to_api_error_without_retry() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("503 should surface as an error");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("503"),
                "status should carry 503, got: {status}"
            );
            assert!(message.contains("service unavailable"));
        }
        other => panic!("expected ApiError(503), got: {other:?}"),
    }
    server.verify().await;
}

/// The BQ analog of "gives up after the retry budget": a job that never
/// completes drives the poll-loop to its deadline and returns
/// `BigQueryError::Timeout`. Uses a 1 s `with_timeout` so the test is fast
/// (first poll sleeps 100 ms, well inside the window; the deadline check
/// then fires).
#[tokio::test]
async fn never_completing_job_times_out() {
    let server = MockServer::start().await;

    // Submit defers.
    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": "test-project", "jobId": "job-stuck"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Every poll says "still running".
    Mock::given(method("GET"))
        .and(path("/bigquery/v2/projects/test-project/queries/job-stuck"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": "test-project", "jobId": "job-stuck"}
        })))
        .mount(&server)
        .await;

    let auth = BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
        "test-bq-token".into(),
    ));
    let adapter = BigQueryAdapter::new("test-project", "EU", auth)
        .with_base_url(server.uri())
        .with_timeout(1);

    let err = adapter
        .execute_query("SELECT slow()")
        .await
        .expect_err("a never-completing job must time out, not hang");
    match as_bq_error(&err) {
        BigQueryError::Timeout { timeout_secs } => assert_eq!(*timeout_secs, 1),
        other => panic!("expected Timeout, got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// (d) Auth: 401 / 403 → ApiError carrying the status + body
// ---------------------------------------------------------------------------

/// A 401 from BigQuery (expired/invalid token) maps to `ApiError` with the
/// status and the error body preserved.
#[tokio::test]
async fn auth_failure_401_maps_to_api_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(401).set_body_json(json!({
            "error": {
                "code": 401,
                "status": "UNAUTHENTICATED",
                "message": "Invalid Credentials"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("401 should surface as an auth-flavored ApiError");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("401"),
                "status should carry 401, got: {status}"
            );
            assert!(
                message.contains("UNAUTHENTICATED") || message.contains("Invalid Credentials"),
                "401 body should be preserved, got: {message}"
            );
        }
        other => panic!("expected ApiError(401), got: {other:?}"),
    }
}

/// A 403 (e.g. `accessDenied` / quota or IAM denial) likewise maps to
/// `ApiError` with the status and body preserved.
#[tokio::test]
async fn auth_failure_403_maps_to_api_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {
                "code": 403,
                "status": "PERMISSION_DENIED",
                "message": "Access Denied: Project test-project: User does not have bigquery.jobs.create permission"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("403 should surface as ApiError");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("403"),
                "status should carry 403, got: {status}"
            );
            assert!(
                message.contains("PERMISSION_DENIED") || message.contains("Access Denied"),
                "403 body should be preserved, got: {message}"
            );
        }
        other => panic!("expected ApiError(403), got: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// (e) Malformed / truncated JSON body → clean error, not a panic
// ---------------------------------------------------------------------------

/// A 200 with a non-JSON body produces a graceful `BigQueryError::Http`
/// (the reqwest `.json()` decode error) rather than panicking.
#[tokio::test]
async fn malformed_json_body_maps_to_http_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("non-JSON body should error, not panic");
    assert!(
        matches!(as_bq_error(&err), BigQueryError::Http(_)),
        "expected Http (JSON decode) error, got: {err}"
    );
}

/// A truncated JSON body (valid-prefix, cut off mid-object) also lands on
/// `BigQueryError::Http` cleanly.
#[tokio::test]
async fn truncated_json_body_maps_to_http_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string(r#"{"jobComplete": true, "rows": [{"f": [{"v": "#),
        )
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("truncated body should error, not panic");
    assert!(
        matches!(as_bq_error(&err), BigQueryError::Http(_)),
        "expected Http (JSON decode) error, got: {err}"
    );
}

// ---------------------------------------------------------------------------
// (f) Job error — reject shapes
// ---------------------------------------------------------------------------

/// A SQL/job error returned as HTTP **400** with a BigQuery error envelope
/// is the path the connector actually treats as a rejection today: any
/// non-2xx → `ApiError` carrying the message. Mirrors how `jobs.query`
/// returns syntax errors and invalid-query failures synchronously.
#[tokio::test]
async fn job_error_http_400_maps_to_api_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(400).set_body_json(json!({
            "error": {
                "code": 400,
                "status": "INVALID_ARGUMENT",
                "message": "Syntax error: Unexpected identifier \"SELCT\" at [1:1]"
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELCT 1")
        .await
        .expect_err("a 400 query error should surface as ApiError");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("400"),
                "status should carry 400, got: {status}"
            );
            assert!(
                message.contains("Syntax error"),
                "rejection message should be preserved, got: {message}"
            );
        }
        other => panic!("expected ApiError(400), got: {other:?}"),
    }
}

/// CHARACTERIZATION (documented gap): a 200 response carrying a job-level
/// `status.errorResult` — BigQuery's async failure shape — is currently
/// **ignored** by the connector. `BigQueryResponse` doesn't parse
/// `status`, and `run_query` returns `Ok` whenever `jobComplete=true`, so
/// the failure is swallowed and the caller sees an empty result set rather
/// than a query-rejected error. SF/DBX map their equivalent to a typed
/// "statement failed". This test pins the current (incorrect) behavior so
/// a future fix that maps `errorResult` to an error is an intentional,
/// visible change. See the module header.
#[tokio::test]
async fn job_error_result_in_200_body_is_currently_ignored() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-errresult"},
            "status": {
                "state": "DONE",
                "errorResult": {
                    "reason": "invalidQuery",
                    "message": "Table not found: missing_table"
                },
                "errors": [
                    {"reason": "invalidQuery", "message": "Table not found: missing_table"}
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    // GAP: this resolves to Ok with no rows instead of a query-rejected
    // error. Asserting Ok pins the current behavior; flip this assertion
    // when the connector learns to map `status.errorResult`.
    let result = adapter
        .execute_query("SELECT * FROM missing_table")
        .await
        .expect("connector currently ignores status.errorResult and returns Ok");
    assert!(
        result.rows.is_empty(),
        "errorResult body carries no rows; current behavior yields an empty result set"
    );
}

// ---------------------------------------------------------------------------
// (g) Empty result set
// ---------------------------------------------------------------------------

/// A completed job with a schema but zero rows yields the column names and
/// an empty row vec — not an error. (Routed through `execute_query`, not
/// `describe_table`, because the latter maps an empty result to a
/// "not found" error.)
#[tokio::test]
async fn empty_result_set_yields_columns_and_no_rows() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-empty"},
            "schema": {"fields": [{"name": "id", "type": "INTEGER"}]},
            // No `rows` key at all — BigQuery omits it for a zero-row result.
            "totalRows": "0"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let result = adapter
        .execute_query("SELECT id FROM t WHERE 1=0")
        .await
        .expect("empty result should be Ok, not an error");

    assert_eq!(result.columns, vec!["id"]);
    assert!(result.rows.is_empty(), "zero-row result has no rows");
}

/// A completed job that omits the schema entirely (some DDL / no-result
/// statements) yields empty columns and empty rows without panicking.
#[tokio::test]
async fn missing_schema_yields_empty_columns_and_rows() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-noschema"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let result = adapter
        .execute_query("CREATE TABLE t (id INT64)")
        .await
        .expect("schema-less response should parse to an empty QueryResult");

    assert!(result.columns.is_empty());
    assert!(result.rows.is_empty());
}
