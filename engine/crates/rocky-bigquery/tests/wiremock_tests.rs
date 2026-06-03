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
//! ## Behavior these tests pin
//!
//! Like the Snowflake and Databricks adapters, BigQuery now classifies
//! 429/502/503/504 as transient and **retries** with bounded
//! exponential backoff, and maps an accepted-but-failed job (a top-level
//! `errors[]` array on a `jobComplete=true` 200 response) to a typed
//! [`BigQueryError::JobError`] instead of swallowing it as an empty
//! result set. The three tests that previously *characterized the gap*
//! (429/503 → `ApiError` on the first hit; a 200-with-error body ignored)
//! now assert the fixed behavior.
//!
//! ### Job-failure shape (captured live, not guessed)
//!
//! The `jobs.query` `QueryResponse` and `jobs.getQueryResults`
//! `GetQueryResultsResponse` surface a *job-level* failure as a
//! **top-level `errors[]`** array of `ErrorProto`
//! (`{reason, message, location, ...}`) — there is **no** top-level
//! `status.errorResult` block on these endpoints (that nesting belongs to
//! the `jobs.get` `Job` resource). A live sandbox probe confirmed that a
//! runtime failure submitted through `jobs.query` (`SELECT ERROR(col)`,
//! div-by-zero on a real table) is in fact rejected *synchronously* with
//! an HTTP 400 Google API `error` envelope, which the non-2xx branch
//! already maps to `ApiError`. The 200-with-`errors[]` path is the
//! documented async-failure surface on `getQueryResults`; these tests pin
//! the `errors[]` parsing using that real `ErrorProto` shape so the
//! connector is correct regardless of which path a given job takes.
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
use rocky_bigquery::{BigQuerySourceFormat, LoadCreateDisposition, LoadJobSpec, WriteDisposition};
use rocky_core::config::RetryConfig;
use rocky_core::retry_budget::RetryBudget;
use rocky_core::traits::{AdapterError, WarehouseAdapter};
use serde_json::{Value, json};
use wiremock::matchers::{body_partial_json, header, method, path, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Path of the `jobs.query` endpoint for the test project.
const QUERIES_PATH: &str = "/bigquery/v2/projects/test-project/queries";
/// Path of the `jobs.insert` endpoint for the test project.
const JOBS_PATH: &str = "/bigquery/v2/projects/test-project/jobs";

/// Build an adapter with a static bearer token pointed at the mock server.
/// `with_timeout` defaults to the production 300 s so the poll deadline
/// doesn't bite on the happy paths; individual tests shrink it where the
/// timeout itself is under test.
///
/// **Retries disabled** (`max_retries = 0`) by default so the single-
/// request error-mapping tests (auth 401/403, the 400 rejection, the
/// job-error path) assert exactly one request without a retry layer
/// interfering. The retry tests opt in via [`retrying_adapter`].
fn test_adapter(server: &MockServer) -> BigQueryAdapter {
    let auth = BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
        "test-bq-token".into(),
    ));
    BigQueryAdapter::new("test-project", "EU", auth)
        .with_base_url(server.uri())
        .with_retry(no_retry())
}

/// `RetryConfig` with retries disabled — the error-mapping characterization
/// tests want exactly one request.
fn no_retry() -> RetryConfig {
    RetryConfig {
        max_retries: 0,
        ..Default::default()
    }
}

/// A fast `RetryConfig` for the retry tests: `max_retries` retries with a
/// 1 ms base backoff and no jitter so the test doesn't sleep on the
/// production 1000 ms ladder. Assertions on request count use the mock's
/// `.expect(N)` / `server.verify()`.
fn fast_retry(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        initial_backoff_ms: 1,
        max_backoff_ms: 5,
        backoff_multiplier: 2.0,
        jitter: false,
        ..Default::default()
    }
}

/// Adapter that retries transient errors with the fast backoff above.
fn retrying_adapter(server: &MockServer, max_retries: u32) -> BigQueryAdapter {
    let auth = BigQueryAuth::Bearer(rocky_core::redacted::RedactedString::new(
        "test-bq-token".into(),
    ));
    BigQueryAdapter::new("test-project", "EU", auth)
        .with_base_url(server.uri())
        .with_retry(fast_retry(max_retries))
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
// (c) 429 / 503 — transient: the connector classifies these as transient
// and retries with bounded backoff, mirroring the SF/DBX adapters.
// ---------------------------------------------------------------------------

/// 429 (rate limited) is transient: the connector retries. Here every
/// attempt returns 429, so after the budget is exhausted the call surfaces
/// the final `ApiError(429)`. `.expect(max_retries + 1)` pins that all
/// attempts fired — proving the retry layer engaged (the pre-fix
/// connector would have made exactly one request).
#[tokio::test]
async fn rate_limit_429_is_retried_then_surfaces_after_budget() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        // 2 retries → 3 total attempts.
        .expect(3)
        .mount(&server)
        .await;

    let adapter = retrying_adapter(&server, 2);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("429 on every attempt should surface as an error after retries");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("429"),
                "status should carry 429, got: {status}"
            );
            assert!(message.contains("rate limited"));
        }
        other => panic!("expected ApiError(429) after exhausting retries, got: {other:?}"),
    }
    // 3 requests total (1 + 2 retries) — `.expect(3)` fails on drop if the
    // connector made fewer (no retry) or more.
    server.verify().await;
}

/// 503 (service unavailable) is likewise transient and retried. Same
/// shape: every attempt 503s, the final error is `ApiError(503)`, and the
/// request count proves the retries fired.
#[tokio::test]
async fn service_unavailable_503_is_retried_then_surfaces_after_budget() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .expect(3)
        .mount(&server)
        .await;

    let adapter = retrying_adapter(&server, 2);
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("503 on every attempt should surface as an error after retries");
    match as_bq_error(&err) {
        BigQueryError::ApiError { status, message } => {
            assert!(
                status.contains("503"),
                "status should carry 503, got: {status}"
            );
            assert!(message.contains("service unavailable"));
        }
        other => panic!("expected ApiError(503) after exhausting retries, got: {other:?}"),
    }
    server.verify().await;
}

/// Retry **succeeds** after N transient 429s then a 200: the connector
/// retries through the rate-limit responses and returns the eventual
/// result. Two scoped mocks with priorities — the 429 mock answers the
/// first two requests (`up_to_n_times(2)`), the success mock answers the
/// third — let `server.verify()` assert the exact attempt counts.
#[tokio::test]
async fn retry_succeeds_after_two_429s_then_200() {
    let server = MockServer::start().await;

    // First two attempts: 429. Higher priority so it wins while it still
    // has responses left.
    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
        .up_to_n_times(2)
        .with_priority(1)
        .expect(2)
        .mount(&server)
        .await;

    // Third attempt: success with a row.
    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-retry-ok"},
            "schema": {"fields": [{"name": "n", "type": "INTEGER"}]},
            "rows": [{"f": [{"v": "7"}]}],
            "totalRows": "1"
        })))
        .with_priority(2)
        .expect(1)
        .mount(&server)
        .await;

    // 3 retries available; only 2 are needed.
    let adapter = retrying_adapter(&server, 3);
    let result = adapter
        .execute_query("SELECT 7 AS n")
        .await
        .expect("connector should retry through the 429s and return the eventual result");

    assert_eq!(result.columns, vec!["n"]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], json!("7"));

    // 2 × 429 then 1 × 200 = exactly 3 requests.
    server.verify().await;
}

/// Retry **exhausts the run-level budget**: with `max_retries` high enough
/// to keep retrying but a `RetryBudget` of 1, the second transient failure
/// can't consume a retry slot, so the call aborts with
/// `RetryBudgetExhausted` rather than continuing to burn the warehouse's
/// rate-limit quota. Exactly 2 requests fire (initial + the single budgeted
/// retry).
#[tokio::test]
async fn retry_exhausts_budget_after_repeated_503() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        // initial attempt + 1 budgeted retry.
        .expect(2)
        .mount(&server)
        .await;

    // max_retries = 5 would otherwise allow 5 retries, but the shared
    // budget caps total retries at 1.
    let adapter = retrying_adapter(&server, 5).with_retry_budget(RetryBudget::new(1));
    let err = adapter
        .execute_query("SELECT 1")
        .await
        .expect_err("budget exhaustion should abort remaining retries with an error");
    match as_bq_error(&err) {
        BigQueryError::RetryBudgetExhausted { limit } => {
            assert_eq!(*limit, 1, "budget limit should be reported");
        }
        other => panic!("expected RetryBudgetExhausted, got: {other:?}"),
    }
    // initial + 1 retry = 2 requests; the budget blocks any further retry.
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
/// status and the error body preserved, and is **not retried** — even with
/// a retrying adapter, `.expect(1)` proves the classifier treats 401 as
/// terminal (retrying a bad token only burns quota).
#[tokio::test]
async fn auth_failure_401_maps_to_api_error_without_retry() {
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

    // Retrying adapter — a 401 must still surface on the first attempt.
    let adapter = retrying_adapter(&server, 3);
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
    server.verify().await;
}

/// A 403 (e.g. `accessDenied` / quota or IAM denial) likewise maps to
/// `ApiError` with the status and body preserved, and is **not retried**.
#[tokio::test]
async fn auth_failure_403_maps_to_api_error_without_retry() {
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

    let adapter = retrying_adapter(&server, 3);
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
    server.verify().await;
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
/// maps to `ApiError` carrying the message, and is **not retried** — a
/// retrying adapter with `.expect(1)` proves the 400 rejection is terminal.
/// This is the path the connector takes for a synchronously-rejected query;
/// the live sandbox probe confirmed `jobs.query` returns a runtime failure
/// (`SELECT ERROR(col)`, div-by-zero) as exactly this 400 envelope.
#[tokio::test]
async fn job_error_http_400_maps_to_api_error_without_retry() {
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

    let adapter = retrying_adapter(&server, 3);
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
    server.verify().await;
}

/// FIXED (was `job_error_result_in_200_body_is_currently_ignored`): a 200
/// response on a *completed* job (`jobComplete=true`) that carries a
/// top-level `errors[]` array — BigQuery's async job-failure shape on the
/// `jobs.query` / `jobs.getQueryResults` endpoints — now maps to a typed
/// [`BigQueryError::JobError`] instead of being swallowed as an empty
/// result set.
///
/// The body uses the **real** `ErrorProto` shape captured live
/// (`{reason, message, location, locationType, domain}` at the response
/// top level — there is no `status.errorResult` nesting on these
/// endpoints; that belongs to the `jobs.get` `Job` resource). The first
/// `ErrorProto`'s `reason` / `message` flow into the typed error.
#[tokio::test]
async fn job_error_in_200_body_maps_to_job_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-errresult"},
            // Real top-level `errors[]` ErrorProto shape (captured live).
            "errors": [
                {
                    "reason": "invalidQuery",
                    "message": "Table not found: missing_table",
                    "domain": "global",
                    "location": "q",
                    "locationType": "parameter"
                }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT * FROM missing_table")
        .await
        .expect_err("a job-level errors[] must surface as an error, not empty rows");
    match as_bq_error(&err) {
        BigQueryError::JobError { reason, message } => {
            assert_eq!(reason, "invalidQuery");
            assert!(
                message.contains("Table not found"),
                "job error message should be preserved, got: {message}"
            );
        }
        other => panic!("expected JobError, got: {other:?}"),
    }
    server.verify().await;
}

/// The same async job-failure shape on the **poll path**: the initial
/// `jobs.query` defers (`jobComplete=false` + `jobReference`), and the
/// eventual `getQueryResults` poll returns `jobComplete=true` with a
/// top-level `errors[]`. The connector applies the same `check_job_error`
/// guard at the poll completion point, so a job that fails *after* being
/// deferred also surfaces as `JobError`.
#[tokio::test]
async fn job_error_in_poll_result_maps_to_job_error() {
    let server = MockServer::start().await;

    // Submit defers.
    Mock::given(method("POST"))
        .and(path(QUERIES_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": false,
            "jobReference": {"projectId": "test-project", "jobId": "job-poll-fail"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Poll completes — but with a job-level error and no rows.
    Mock::given(method("GET"))
        .and(path(
            "/bigquery/v2/projects/test-project/queries/job-poll-fail",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobComplete": true,
            "jobReference": {"projectId": "test-project", "jobId": "job-poll-fail"},
            "errors": [
                {"reason": "resourcesExceeded", "message": "Query exceeded resource limits"}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server);
    let err = adapter
        .execute_query("SELECT heavy()")
        .await
        .expect_err("a deferred job that fails on poll must surface as an error");
    match as_bq_error(&err) {
        BigQueryError::JobError { reason, message } => {
            assert_eq!(reason, "resourcesExceeded");
            assert!(message.contains("resource limits"));
        }
        other => panic!("expected JobError from poll path, got: {other:?}"),
    }
    server.verify().await;
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

// ---------------------------------------------------------------------------
// (f) Native LOAD JOBS (`jobs.insert` → poll `jobs.get`)
// ---------------------------------------------------------------------------
//
// The load path is `POST /…/jobs` (insert) followed by `GET /…/jobs/{id}`
// (get) polled until `status.state == "DONE"`. Unlike the query path, a
// load failure surfaces under `status.errorResult` on the `Job` resource,
// not a top-level `errors[]` — these tests pin that distinction plus the
// request-body shape per source format and per writeDisposition.

/// A CSV `LoadJobSpec` for `test-project`, WRITE_TRUNCATE + CREATE_IF_NEEDED.
fn csv_truncate_spec() -> LoadJobSpec {
    LoadJobSpec {
        destination_project: "test-project".into(),
        destination_dataset: "ds".into(),
        destination_table: "tbl".into(),
        source_uris: vec!["gs://bucket/data.csv".into()],
        source_format: BigQuerySourceFormat::Csv,
        write_disposition: WriteDisposition::Truncate,
        create_disposition: LoadCreateDisposition::IfNeeded,
        autodetect: true,
        skip_leading_rows: Some(1),
        field_delimiter: Some(",".into()),
    }
}

/// `jobs.get` (or inline `jobs.insert`) DONE response carrying load
/// statistics. `output_rows` and `input_file_bytes` are decimal strings
/// (BigQuery's int64-as-string).
fn load_done_body(job_id: &str, output_rows: &str, input_bytes: &str) -> Value {
    json!({
        "jobReference": {"projectId": "test-project", "jobId": job_id},
        "status": {"state": "DONE"},
        "statistics": {
            "load": {
                "outputRows": output_rows,
                "inputFileBytes": input_bytes
            }
        }
    })
}

/// Mount a `jobs.get` mock for `job_id` that returns a DONE body with the
/// given load stats. The request-shape tests assert the *insert* body; the
/// connector always resolves rows-loaded from `jobs.get`, so they need a
/// matching GET to complete.
async fn mount_get_done(server: &MockServer, job_id: &str, output_rows: &str, input_bytes: &str) {
    Mock::given(method("GET"))
        .and(path_regex(format!(
            r"^/bigquery/v2/projects/test-project/jobs/{job_id}$"
        )))
        .respond_with(ResponseTemplate::new(200).set_body_json(load_done_body(
            job_id,
            output_rows,
            input_bytes,
        )))
        .mount(server)
        .await;
}

/// Happy path: insert returns RUNNING, the poll loop hits `jobs.get` which
/// returns DONE with `statistics.load.outputRows`. Asserts rows-loaded and
/// bytes-read are parsed from the load statistics.
#[tokio::test]
async fn load_job_polls_to_done_and_parses_output_rows() {
    let server = MockServer::start().await;

    // jobs.insert → RUNNING, no statistics yet.
    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .and(header("authorization", "Bearer test-bq-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-1"},
            "status": {"state": "RUNNING"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    // jobs.get → DONE with load stats.
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/bigquery/v2/projects/test-project/jobs/load-1$",
        ))
        .and(header("authorization", "Bearer test-bq-token"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(load_done_body("load-1", "1000", "204800")),
        )
        .expect(1..)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(5);
    let outcome = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect("load job should complete");

    assert_eq!(outcome.rows_loaded, 1000);
    assert_eq!(outcome.input_file_bytes, Some(204_800));
}

/// Inline DONE: `jobs.insert` returns `state=DONE` but **without** a
/// `statistics.load` block (BigQuery only reliably populates `outputRows`
/// on the `Job` resource from `jobs.get`, not on the insert response). The
/// connector must therefore still call `jobs.get` for the authoritative
/// rows-loaded rather than reporting `0` from the stats-less insert body.
/// This pins the fix for that silent-zero-rows hazard.
#[tokio::test]
async fn load_job_inline_done_still_fetches_stats_via_get() {
    let server = MockServer::start().await;

    // Insert → DONE but no statistics block.
    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-inline"},
            "status": {"state": "DONE"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    // jobs.get → DONE with the real load stats.
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/bigquery/v2/projects/test-project/jobs/load-inline$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(load_done_body(
            "load-inline",
            "7",
            "512",
        )))
        .expect(1..)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(5);
    let outcome = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect("inline-DONE load job should still resolve stats via jobs.get");

    assert_eq!(outcome.rows_loaded, 7);
    assert_eq!(outcome.input_file_bytes, Some(512));
}

/// Request-shape: a CSV WRITE_TRUNCATE load sends the right
/// `configuration.load` block — sourceFormat=CSV, writeDisposition,
/// createDisposition, the structured destinationTable, and the CSV-only
/// knobs (skipLeadingRows / fieldDelimiter). `body_partial_json` asserts
/// the request the connector PUT on the wire.
#[tokio::test]
async fn load_job_csv_truncate_request_shape() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .and(body_partial_json(json!({
            "jobReference": {"projectId": "test-project", "location": "EU"},
            "configuration": {
                "load": {
                    "sourceUris": ["gs://bucket/data.csv"],
                    "sourceFormat": "CSV",
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                    "autodetect": true,
                    "destinationTable": {
                        "projectId": "test-project",
                        "datasetId": "ds",
                        "tableId": "tbl"
                    },
                    "skipLeadingRows": 1,
                    "fieldDelimiter": ","
                }
            }
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(load_done_body("load-csv", "1", "10")),
        )
        .expect(1)
        .mount(&server)
        .await;
    mount_get_done(&server, "load-csv", "1", "10").await;

    let adapter = test_adapter(&server).with_timeout(5);
    adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect("csv truncate load should submit");
    server.verify().await;
}

/// Request-shape: a PARQUET WRITE_APPEND / CREATE_NEVER load. Asserts the
/// sourceFormat + flipped dispositions on the wire. (The CSV-only knobs
/// `skipLeadingRows` / `fieldDelimiter` are omitted from the body via
/// `skip_serializing_if` for non-CSV formats — absence is covered by the
/// `build_load_spec_*` unit tests; `body_partial_json` here only asserts
/// the keys that *are* present.)
#[tokio::test]
async fn load_job_parquet_append_request_shape() {
    let server = MockServer::start().await;

    let spec = LoadJobSpec {
        destination_project: "test-project".into(),
        destination_dataset: "ds".into(),
        destination_table: "tbl".into(),
        source_uris: vec!["gs://bucket/data.parquet".into()],
        source_format: BigQuerySourceFormat::Parquet,
        write_disposition: WriteDisposition::Append,
        create_disposition: LoadCreateDisposition::Never,
        autodetect: true,
        skip_leading_rows: None,
        field_delimiter: None,
    };

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .and(body_partial_json(json!({
            "configuration": {
                "load": {
                    "sourceUris": ["gs://bucket/data.parquet"],
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND",
                    "createDisposition": "CREATE_NEVER"
                }
            }
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(load_done_body("load-pq", "42", "9999")),
        )
        .expect(1)
        .mount(&server)
        .await;
    mount_get_done(&server, "load-pq", "42", "9999").await;

    let adapter = test_adapter(&server).with_timeout(5);
    let outcome = adapter
        .load_via_job(&spec)
        .await
        .expect("parquet append load");
    assert_eq!(outcome.rows_loaded, 42);
    server.verify().await;
}

/// Request-shape: a NEWLINE_DELIMITED_JSON load sends `sourceFormat =
/// NEWLINE_DELIMITED_JSON`. Pins the JSONL → API-enum mapping.
#[tokio::test]
async fn load_job_jsonl_source_format_request_shape() {
    let server = MockServer::start().await;

    let spec = LoadJobSpec {
        destination_project: "test-project".into(),
        destination_dataset: "ds".into(),
        destination_table: "tbl".into(),
        source_uris: vec!["gs://bucket/data.jsonl".into()],
        source_format: BigQuerySourceFormat::NewlineDelimitedJson,
        write_disposition: WriteDisposition::Truncate,
        create_disposition: LoadCreateDisposition::IfNeeded,
        autodetect: true,
        skip_leading_rows: None,
        field_delimiter: None,
    };

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .and(body_partial_json(json!({
            "configuration": {
                "load": {
                    "sourceFormat": "NEWLINE_DELIMITED_JSON",
                    "writeDisposition": "WRITE_TRUNCATE"
                }
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(load_done_body(
            "load-jsonl",
            "3",
            "120",
        )))
        .expect(1)
        .mount(&server)
        .await;
    mount_get_done(&server, "load-jsonl", "3", "120").await;

    let adapter = test_adapter(&server).with_timeout(5);
    adapter.load_via_job(&spec).await.expect("jsonl load");
    server.verify().await;
}

/// Empty `destination_project` on the spec falls back to the adapter's
/// project so the wire `destinationTable.projectId` is never empty.
#[tokio::test]
async fn load_job_empty_destination_project_falls_back_to_adapter_project() {
    let server = MockServer::start().await;

    let spec = LoadJobSpec {
        destination_project: String::new(),
        destination_dataset: "ds".into(),
        destination_table: "tbl".into(),
        source_uris: vec!["gs://bucket/data.csv".into()],
        source_format: BigQuerySourceFormat::Csv,
        write_disposition: WriteDisposition::Append,
        create_disposition: LoadCreateDisposition::IfNeeded,
        autodetect: true,
        skip_leading_rows: Some(1),
        field_delimiter: Some(",".into()),
    };

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .and(body_partial_json(json!({
            "configuration": {
                "load": {
                    "destinationTable": {"projectId": "test-project"}
                }
            }
        })))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(load_done_body("load-fb", "1", "10")),
        )
        .expect(1)
        .mount(&server)
        .await;
    mount_get_done(&server, "load-fb", "1", "10").await;

    let adapter = test_adapter(&server).with_timeout(5);
    adapter
        .load_via_job(&spec)
        .await
        .expect("load with project fallback");
    server.verify().await;
}

/// A load job that reaches DONE with a `status.errorResult` maps to
/// [`BigQueryError::LoadJobError`], and the message aggregates the
/// per-file reasons from `status.errors[]` (not just the summary). This is
/// the load path's analogue of the query path's top-level-`errors[]`
/// handling — and proves the connector reads the *right* nesting.
#[tokio::test]
async fn load_job_error_result_maps_to_load_job_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-bad"},
            "status": {"state": "RUNNING"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path_regex(r"^/bigquery/v2/projects/test-project/jobs/load-bad$"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-bad"},
            "status": {
                "state": "DONE",
                "errorResult": {
                    "reason": "invalid",
                    "message": "Error while reading data, error message: CSV table references column position 3, but line contains only 2 columns."
                },
                "errors": [
                    {"reason": "invalid", "message": "Error while reading data: field count mismatch (line 1)"},
                    {"reason": "invalid", "message": "Error while reading data: field count mismatch (line 2)"}
                ]
            }
        })))
        .expect(1..)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(5);
    let err = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect_err("a DONE job with errorResult must be an error");

    match err {
        BigQueryError::LoadJobError { reason, message } => {
            assert_eq!(reason, "invalid");
            // Message aggregates the two per-line errors[] entries.
            assert!(
                message.contains("line 1") && message.contains("line 2"),
                "expected aggregated per-line reasons, got: {message}"
            );
        }
        other => panic!("expected LoadJobError, got: {other:?}"),
    }
}

/// A load job whose `errorResult` has no `errors[]` detail falls back to
/// the `errorResult.message` summary.
#[tokio::test]
async fn load_job_error_result_without_errors_uses_summary_message() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-bad2"},
            "status": {
                "state": "DONE",
                "errorResult": {"reason": "notFound", "message": "Not found: URI gs://bucket/missing.csv"}
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(5);
    let err = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect_err("missing-source load must error");

    match err {
        BigQueryError::LoadJobError { reason, message } => {
            assert_eq!(reason, "notFound");
            assert!(message.contains("missing.csv"), "got: {message}");
        }
        other => panic!("expected LoadJobError, got: {other:?}"),
    }
}

/// A non-2xx from `jobs.insert` (e.g. a 403 from a missing
/// `bigquery.jobUser` role) maps to [`BigQueryError::ApiError`] on the
/// first attempt — no poll, retries disabled by `test_adapter`.
#[tokio::test]
async fn load_job_insert_non_2xx_maps_to_api_error() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": {"code": 403, "message": "Access Denied: missing bigquery.jobs.create"}
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(5);
    let err = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect_err("403 insert must error");

    match err {
        BigQueryError::ApiError { status, message } => {
            assert!(status.starts_with("403"), "status: {status}");
            assert!(message.contains("Access Denied"), "message: {message}");
        }
        other => panic!("expected ApiError, got: {other:?}"),
    }
}

/// The poll loop respects the configured timeout: if `jobs.get` never
/// reports DONE, the call returns [`BigQueryError::Timeout`] rather than
/// looping forever. A 1 s timeout keeps the test fast.
#[tokio::test]
async fn load_job_poll_times_out_when_never_done() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path(JOBS_PATH))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-stuck"},
            "status": {"state": "RUNNING"}
        })))
        .mount(&server)
        .await;

    // Always RUNNING — never reaches DONE.
    Mock::given(method("GET"))
        .and(path_regex(
            r"^/bigquery/v2/projects/test-project/jobs/load-stuck$",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "jobReference": {"projectId": "test-project", "jobId": "load-stuck"},
            "status": {"state": "RUNNING"}
        })))
        .mount(&server)
        .await;

    let adapter = test_adapter(&server).with_timeout(1);
    let err = adapter
        .load_via_job(&csv_truncate_spec())
        .await
        .expect_err("a never-DONE load job must time out");

    assert!(
        matches!(err, BigQueryError::Timeout { timeout_secs: 1 }),
        "expected Timeout, got: {err:?}"
    );
}
