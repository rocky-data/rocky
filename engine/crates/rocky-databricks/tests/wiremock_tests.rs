//! Wire-level mock tests for the Databricks SQL Statement Execution API.
//!
//! Uses wiremock to simulate the Databricks REST API at the HTTP level,
//! verifying correct behavior for happy paths, auth failures, retries,
//! and malformed responses.

use std::sync::Arc;
use std::time::Duration;

use rocky_adapter_sdk::{LoadOptions, LoadSource, LoaderAdapter, TableRef};
use rocky_core::config::RetryConfig;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, ConnectorError, DatabricksConnector};
use rocky_databricks::loader::DatabricksLoaderAdapter;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Creates a PAT-authenticated connector pointing at the given wiremock server.
fn test_connector(server: &MockServer) -> DatabricksConnector {
    let auth = Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .unwrap();

    let config = ConnectorConfig {
        host: "test.databricks.com".into(),
        warehouse_id: "test-warehouse".into(),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
    };

    DatabricksConnector::new(config, auth).with_base_url(server.uri())
}

/// Creates a connector with retry enabled (for retry tests).
fn test_connector_with_retries(server: &MockServer, max_retries: u32) -> DatabricksConnector {
    let auth = Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .unwrap();

    let config = ConnectorConfig {
        host: "test.databricks.com".into(),
        warehouse_id: "test-warehouse".into(),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries,
            initial_backoff_ms: 10, // Fast backoff for tests
            max_backoff_ms: 50,
            backoff_multiplier: 1.0,
            jitter: false,
            circuit_breaker_threshold: 0, // Disable circuit breaker for retry tests
        },
    };

    DatabricksConnector::new(config, auth).with_base_url(server.uri())
}

/// Happy path: SQL statement executes inline and returns results.
#[tokio::test]
async fn test_happy_path_inline_result() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-001",
            "status": {
                "state": "SUCCEEDED"
            },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "id", "type_name": "INT", "position": 0},
                        {"name": "name", "type_name": "STRING", "position": 1}
                    ]
                },
                "total_row_count": 2
            },
            "result": {
                "data_array": [
                    [1, "alice"],
                    [2, "bob"]
                ]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT * FROM users").await.unwrap();

    assert_eq!(result.statement_id, "stmt-001");
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name, "id");
    assert_eq!(result.columns[1].name, "name");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.total_row_count, Some(2));
}

/// Happy path: statement needs polling before completing.
#[tokio::test]
async fn test_happy_path_with_polling() {
    let server = MockServer::start().await;

    // Initial submit returns PENDING
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-poll",
            "status": { "state": "PENDING" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Poll returns SUCCEEDED
    Mock::given(method("GET"))
        .and(path("/api/2.0/sql/statements/stmt-poll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-poll",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "value", "type_name": "INT", "position": 0}
                    ]
                },
                "total_row_count": 1
            },
            "result": {
                "data_array": [[42]]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 42 AS value").await.unwrap();

    assert_eq!(result.statement_id, "stmt-poll");
    assert_eq!(result.rows.len(), 1);
}

/// Auth failure: 401 response is propagated as an ApiError.
#[tokio::test]
async fn test_auth_failure_401() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(401).set_body_json(serde_json::json!({
            "error_code": "UNAUTHORIZED",
            "message": "Invalid access token"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 1").await;

    let err = result.unwrap_err();
    match &err {
        ConnectorError::ApiError { status, body } => {
            assert_eq!(*status, 401);
            assert!(body.contains("UNAUTHORIZED"));
        }
        other => panic!("expected ApiError, got: {other}"),
    }
}

/// Retry on 429: mock returns 429 once, then 200 on retry.
#[tokio::test]
async fn test_retry_on_429() {
    let server = MockServer::start().await;

    // First call: 429 rate limited
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    // Second call: success
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-retry",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "schema": { "columns": [] },
                "total_row_count": 0
            },
            "result": { "data_array": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 2);
    let result = connector.execute_sql("SELECT 1").await.unwrap();

    assert_eq!(result.statement_id, "stmt-retry");
}

/// Retry on 503: mock returns 503 once, then 200 on retry.
#[tokio::test]
async fn test_retry_on_503() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-503",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [] }, "total_row_count": 0 },
            "result": { "data_array": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 2);
    let result = connector.execute_sql("SELECT 1").await.unwrap();

    assert_eq!(result.statement_id, "stmt-503");
}

/// Malformed response: invalid JSON body produces a graceful error.
#[tokio::test]
async fn test_malformed_json_response() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_string("this is not json"))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 1").await;

    let err = result.unwrap_err();
    assert!(
        matches!(err, ConnectorError::Http(_)),
        "expected Http error from JSON parse failure, got: {err}"
    );
}

/// Statement failure: API returns FAILED state with an error message.
#[tokio::test]
async fn test_statement_failed() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-fail",
            "status": {
                "state": "FAILED",
                "error": {
                    "error_code": "PARSE_ERROR",
                    "message": "syntax error at position 5"
                }
            },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECTT INVALID").await;

    let err = result.unwrap_err();
    match &err {
        ConnectorError::StatementFailed { id, message } => {
            assert_eq!(id, "stmt-fail");
            assert!(message.contains("syntax error"));
        }
        other => panic!("expected StatementFailed, got: {other}"),
    }
}

/// Statement canceled: API returns CANCELED state.
#[tokio::test]
async fn test_statement_canceled() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-cancel",
            "status": { "state": "CANCELED" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 1").await;

    let err = result.unwrap_err();
    assert!(
        matches!(err, ConnectorError::Canceled { .. }),
        "expected Canceled, got: {err}"
    );
}

/// execute_statement returns just the statement ID.
#[tokio::test]
async fn test_execute_statement_returns_id() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-exec",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let id = connector
        .execute_statement("CREATE TABLE test (id INT)")
        .await
        .unwrap();

    assert_eq!(id, "stmt-exec");
}

/// Non-transient HTTP error (400) is not retried.
#[tokio::test]
async fn test_non_transient_error_not_retried() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({"error": "bad request"})),
        )
        .expect(1) // Should only be called once, no retry
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 3);
    let result = connector.execute_sql("SELECT 1").await;

    let err = result.unwrap_err();
    match &err {
        ConnectorError::ApiError { status, .. } => assert_eq!(*status, 400),
        other => panic!("expected ApiError, got: {other}"),
    }
}

/// Empty result set is handled correctly.
#[tokio::test]
async fn test_empty_result_set() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-empty",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "id", "type_name": "INT", "position": 0}
                    ]
                },
                "total_row_count": 0
            },
            "result": { "data_array": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector
        .execute_sql("SELECT * FROM empty_table")
        .await
        .unwrap();

    assert_eq!(result.columns.len(), 1);
    assert!(result.rows.is_empty());
    assert_eq!(result.total_row_count, Some(0));
}

/// End-to-end: `DatabricksLoaderAdapter::load` parses `num_affected_rows`
/// from the COPY INTO response and surfaces it as `LoadResult.rows_loaded`.
#[tokio::test]
async fn test_loader_surfaces_num_affected_rows() {
    let server = MockServer::start().await;

    // Mock COPY INTO response: a single-row result set whose first column is
    // `num_affected_rows`. This matches Databricks' observed REST shape for
    // DML statements (including COPY INTO).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("COPY INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-copy-001",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "num_affected_rows", "type_name": "LONG", "position": 0},
                        {"name": "num_inserted_rows", "type_name": "LONG", "position": 1}
                    ]
                },
                "total_row_count": 1
            },
            "result": {
                "data_array": [["1234", "1234"]]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let loader = DatabricksLoaderAdapter::new(connector);

    let source = LoadSource::CloudUri("s3://bucket/users.csv".into());
    let target = TableRef {
        catalog: "main".into(),
        schema: "raw".into(),
        table: "users".into(),
    };

    let result = loader
        .load(&source, &target, &LoadOptions::default())
        .await
        .unwrap();

    assert_eq!(
        result.rows_loaded, 1234,
        "loader should surface num_affected_rows from COPY INTO"
    );
}

/// Loader degrades gracefully when `num_affected_rows` isn't in the response:
/// the load still succeeds with `rows_loaded = 0` rather than erroring.
#[tokio::test]
async fn test_loader_missing_num_affected_rows() {
    let server = MockServer::start().await;

    // No manifest / no results — older API shapes or unexpected responses.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("COPY INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-copy-002",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let loader = DatabricksLoaderAdapter::new(connector);

    let source = LoadSource::CloudUri("s3://bucket/users.csv".into());
    let target = TableRef {
        catalog: "main".into(),
        schema: "raw".into(),
        table: "users".into(),
    };

    let result = loader
        .load(&source, &target, &LoadOptions::default())
        .await
        .unwrap();

    assert_eq!(result.rows_loaded, 0);
}

/// Bearer token is sent correctly in the Authorization header.
#[tokio::test]
async fn test_bearer_token_sent() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-auth",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    connector.execute_statement("SELECT 1").await.unwrap();
    // If the header didn't match, the mock would not have responded with 200
}
