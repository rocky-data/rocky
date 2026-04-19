//! Wire-level mock tests for the Snowflake SQL REST API v2 connector.
//!
//! Uses wiremock to simulate the Snowflake REST API at the HTTP level,
//! verifying correct behavior for happy paths, auth failures, retries,
//! and malformed responses.

use std::sync::Arc;
use std::time::Duration;

use rocky_adapter_sdk::{LoadOptions, LoadSource, LoaderAdapter, TableRef};
use rocky_core::config::RetryConfig;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, ConnectorError, SnowflakeConnector};
use rocky_snowflake::loader::SnowflakeLoaderAdapter;
use wiremock::matchers::{body_string_contains, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Creates an OAuth-authenticated connector pointing at the given wiremock server.
fn test_connector(server: &MockServer) -> SnowflakeConnector {
    let auth = Auth::from_config(AuthConfig {
        account: "test_account".into(),
        username: None,
        password: None,
        oauth_token: Some("test-sf-token".into()),
        private_key_path: None,
    })
    .unwrap();

    let config = ConnectorConfig {
        account: "test_account".into(),
        warehouse: "COMPUTE_WH".into(),
        database: Some("TEST_DB".into()),
        schema: Some("PUBLIC".into()),
        role: Some("SYSADMIN".into()),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
    };

    SnowflakeConnector::new(config, auth).with_base_url(server.uri())
}

/// Creates a connector with retry enabled (for retry tests).
fn test_connector_with_retries(server: &MockServer, max_retries: u32) -> SnowflakeConnector {
    let auth = Auth::from_config(AuthConfig {
        account: "test_account".into(),
        username: None,
        password: None,
        oauth_token: Some("test-sf-token".into()),
        private_key_path: None,
    })
    .unwrap();

    let config = ConnectorConfig {
        account: "test_account".into(),
        warehouse: "COMPUTE_WH".into(),
        database: Some("TEST_DB".into()),
        schema: Some("PUBLIC".into()),
        role: Some("SYSADMIN".into()),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries,
            initial_backoff_ms: 10,
            max_backoff_ms: 50,
            backoff_multiplier: 1.0,
            jitter: false,
            circuit_breaker_threshold: 0,
            ..Default::default()
        },
    };

    SnowflakeConnector::new(config, auth).with_base_url(server.uri())
}

/// Happy path: SQL statement executes inline and returns results.
#[tokio::test]
async fn test_happy_path_inline_result() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer test-sf-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-stmt-001",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": {
                "numRows": 2,
                "rowType": [
                    {"name": "ID", "type": "FIXED", "nullable": false},
                    {"name": "NAME", "type": "TEXT", "nullable": true}
                ]
            },
            "data": [
                [1, "alice"],
                [2, "bob"]
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT * FROM users").await.unwrap();

    assert_eq!(result.statement_handle, "sf-stmt-001");
    assert_eq!(result.columns.len(), 2);
    assert_eq!(result.columns[0].name, "ID");
    assert_eq!(result.columns[1].name, "NAME");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.total_row_count, Some(2));
}

/// Happy path: statement needs polling before completing.
#[tokio::test]
async fn test_happy_path_with_polling() {
    let server = MockServer::start().await;

    // Initial submit returns async-in-progress
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-poll",
            "code": "333334",
            "message": "Statement executing.",
            "statementStatusUrl": "/api/v2/statements/sf-poll",
            "resultSetMetaData": null,
            "data": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Poll returns success
    Mock::given(method("GET"))
        .and(path("/api/v2/statements/sf-poll"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-poll",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": {
                "numRows": 1,
                "rowType": [
                    {"name": "VALUE", "type": "FIXED", "nullable": false}
                ]
            },
            "data": [[42]]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 42 AS VALUE").await.unwrap();

    assert_eq!(result.statement_handle, "sf-poll");
    assert_eq!(result.rows.len(), 1);
}

/// Auth failure: 401 response is propagated as an ApiError.
#[tokio::test]
async fn test_auth_failure_401() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(
            ResponseTemplate::new(401)
                .set_body_json(serde_json::json!({"message": "JWT token is invalid"})),
        )
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECT 1").await;

    let err = result.unwrap_err();
    match &err {
        ConnectorError::ApiError { status, body } => {
            assert_eq!(*status, 401);
            assert!(body.contains("invalid"));
        }
        other => panic!("expected ApiError, got: {other}"),
    }
}

/// Retry on 429: mock returns 429 once, then 200 on retry.
#[tokio::test]
async fn test_retry_on_429() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-retry",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": {
                "numRows": 0,
                "rowType": []
            },
            "data": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 2);
    let result = connector.execute_sql("SELECT 1").await.unwrap();

    assert_eq!(result.statement_handle, "sf-retry");
}

/// Retry on 503: mock returns 503 once, then 200 on retry.
#[tokio::test]
async fn test_retry_on_503() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(503).set_body_string("service unavailable"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-503",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": { "numRows": 0, "rowType": [] },
            "data": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 2);
    let result = connector.execute_sql("SELECT 1").await.unwrap();

    assert_eq!(result.statement_handle, "sf-503");
}

/// Malformed response: invalid JSON body produces a graceful error.
#[tokio::test]
async fn test_malformed_json_response() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
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

/// Statement failure: API returns a non-success SQLSTATE code.
#[tokio::test]
async fn test_statement_failed() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-fail",
            "code": "002140",
            "message": "SQL compilation error: syntax error at position 5",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let result = connector.execute_sql("SELECTT INVALID").await;

    let err = result.unwrap_err();
    match &err {
        ConnectorError::StatementFailed { handle, message } => {
            assert_eq!(handle, "sf-fail");
            assert!(message.contains("syntax error"));
        }
        other => panic!("expected StatementFailed, got: {other}"),
    }
}

/// execute_statement returns just the statement handle.
#[tokio::test]
async fn test_execute_statement_returns_handle() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-exec",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let handle = connector
        .execute_statement("CREATE TABLE test (id INT)")
        .await
        .unwrap();

    assert_eq!(handle, "sf-exec");
}

/// Non-transient HTTP error (400) is not retried.
#[tokio::test]
async fn test_non_transient_error_not_retried() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .respond_with(
            ResponseTemplate::new(400).set_body_json(serde_json::json!({"message": "bad request"})),
        )
        .expect(1) // Should only be called once
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
        .and(path("/api/v2/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-empty",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": {
                "numRows": 0,
                "rowType": [
                    {"name": "ID", "type": "FIXED", "nullable": false}
                ]
            },
            "data": []
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

/// End-to-end: `SnowflakeLoaderAdapter::load` parses per-file `rows_loaded`
/// from the COPY INTO response and surfaces the sum as `LoadResult.rows_loaded`.
#[tokio::test]
async fn test_loader_surfaces_rows_loaded() {
    let server = MockServer::start().await;

    // CREATE TEMPORARY STAGE — result set not inspected.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("CREATE TEMPORARY STAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-create-stage",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .mount(&server)
        .await;

    // COPY INTO — Snowflake returns one row per source file, with
    // `rows_loaded` as one of the columns. Mocks two files summing to 300.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("COPY INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-copy-001",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": {
                "numRows": 2,
                "rowType": [
                    {"name": "file", "type": "TEXT", "nullable": false},
                    {"name": "status", "type": "TEXT", "nullable": false},
                    {"name": "rows_parsed", "type": "FIXED", "nullable": false},
                    {"name": "rows_loaded", "type": "FIXED", "nullable": false},
                    {"name": "errors_seen", "type": "FIXED", "nullable": false}
                ]
            },
            "data": [
                ["s3://bucket/users_1.csv", "LOADED", "100", "100", "0"],
                ["s3://bucket/users_2.csv", "LOADED", "200", "200", "0"]
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // DROP STAGE IF EXISTS — fires after COPY INTO, result ignored.
    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("DROP STAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-drop-stage",
            "code": "00000",
            "message": "Statement executed successfully.",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let loader = SnowflakeLoaderAdapter::new(connector);

    let source = LoadSource::CloudUri("s3://bucket/users/".into());
    let target = TableRef {
        catalog: "DB".into(),
        schema: "RAW".into(),
        table: "USERS".into(),
    };

    let options = LoadOptions {
        format: Some(rocky_adapter_sdk::FileFormat::Csv),
        ..Default::default()
    };
    let result = loader.load(&source, &target, &options).await.unwrap();

    assert_eq!(
        result.rows_loaded, 300,
        "loader should sum per-file rows_loaded from COPY INTO response"
    );
}

/// Loader degrades gracefully when the COPY INTO response doesn't carry a
/// `rows_loaded` column: the load still succeeds with `rows_loaded = 0`.
#[tokio::test]
async fn test_loader_missing_rows_loaded_column() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("CREATE TEMPORARY STAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-create",
            "code": "00000",
            "message": "ok",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("COPY INTO"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-copy",
            "code": "00000",
            "message": "ok",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/v2/statements"))
        .and(body_string_contains("DROP STAGE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-drop",
            "code": "00000",
            "message": "ok",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let loader = SnowflakeLoaderAdapter::new(connector);
    let source = LoadSource::CloudUri("s3://bucket/data.csv".into());
    let target = TableRef {
        catalog: "DB".into(),
        schema: "RAW".into(),
        table: "USERS".into(),
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
        .and(path("/api/v2/statements"))
        .and(header("Authorization", "Bearer test-sf-token"))
        .and(header(
            "X-Snowflake-Authorization-Token-Type",
            "KEYPAIR_JWT",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statementHandle": "sf-auth",
            "code": "00000",
            "message": "ok",
            "statementStatusUrl": "",
            "resultSetMetaData": null,
            "data": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    connector.execute_statement("SELECT 1").await.unwrap();
    // If the headers didn't match, the mock would not have responded with 200
}
