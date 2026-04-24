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
            ..Default::default()
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

/// §P2.8 emit site: retry transitions publish a `statement_retry`
/// PipelineEvent with structured attempt / error_class. Subscribes to the
/// global event bus BEFORE the retry fires to avoid racing the broadcast.
#[tokio::test]
async fn test_retry_emits_pipeline_event() {
    use rocky_observe::events::{ErrorClass, global_event_bus};

    let server = MockServer::start().await;

    // One 429 then a success — exactly one retry, one emitted event.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-event",
            "status": { "state": "SUCCEEDED" },
            "manifest": { "schema": { "columns": [] }, "total_row_count": 0 },
            "result": { "data_array": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut events = global_event_bus().subscribe();
    let connector = test_connector_with_retries(&server, 2);
    connector.execute_sql("SELECT 1").await.unwrap();

    // Drain pending events and filter for the retry we just triggered.
    // The bus is process-global — other wiremock tests running in parallel
    // may publish their own statement_retry events with different
    // max_attempts values. Match on the exact attempt/max_attempts pair
    // we configured (1/2) so we don't accept a neighbour's event.
    let mut retry_event = None;
    for _ in 0..100 {
        match tokio::time::timeout(Duration::from_millis(200), events.recv()).await {
            Ok(Ok(ev))
                if ev.event_type == "statement_retry"
                    && ev.attempt == Some(1)
                    && ev.max_attempts == Some(2) =>
            {
                retry_event = Some(ev);
                break;
            }
            Ok(Ok(_)) => continue,
            _ => break,
        }
    }
    let ev = retry_event.expect("retry should publish a 1/2 statement_retry event");
    assert_eq!(ev.error_class, Some(ErrorClass::RateLimit));
    assert!(ev.error.as_deref().unwrap_or("").contains("429"));
}

/// §P2.7: retry budget caps total retries across the connector's lifetime.
/// With max_retries=5 but budget=2, a 429-spewing server exhausts the budget
/// on the second retry and short-circuits with `RetryBudgetExhausted`.
#[tokio::test]
async fn test_retry_budget_exhausted_short_circuits() {
    let server = MockServer::start().await;

    // Server always returns 429 — budget, not per-statement retries, should
    // be what stops us.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    let connector = test_connector_with_retries(&server, 5)
        .with_retry_budget(rocky_core::retry_budget::RetryBudget::new(2));

    let err = connector
        .execute_sql("SELECT 1")
        .await
        .expect_err("429-spewing server should error");
    match err {
        ConnectorError::RetryBudgetExhausted { limit } => assert_eq!(limit, 2),
        other => panic!("expected RetryBudgetExhausted, got: {other:?}"),
    }
}

/// §P2.7: an unbounded budget behaves like legacy — per-statement
/// `max_retries` is the only cap. Exhausted per-statement retries surface as
/// the underlying transient error, not `RetryBudgetExhausted`.
#[tokio::test]
async fn test_unbounded_budget_preserves_legacy_exhaustion() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(429).set_body_string("rate limited"))
        .mount(&server)
        .await;

    // max_retries = 1 → one attempt + one retry before legacy exhaustion.
    let connector = test_connector_with_retries(&server, 1);
    let err = connector
        .execute_sql("SELECT 1")
        .await
        .expect_err("legacy retry limit should still fail");
    match err {
        ConnectorError::ApiError { status: 429, .. } => {} // expected
        ConnectorError::RetryBudgetExhausted { .. } => {
            panic!("unbounded budget should never produce RetryBudgetExhausted");
        }
        other => panic!("expected ApiError(429), got: {other:?}"),
    }
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

// ---------------------------------------------------------------------------
// Workspace-binding reconcile (Unity Catalog)
// ---------------------------------------------------------------------------
//
// Unity Catalog workspace bindings control *which Databricks workspaces a
// catalog is visible in*, distinct from *who within a workspace can read or
// write it*. Rocky treats the desired binding set as declarative state and
// reconciles current-vs-desired alongside grants — these tests cover the
// list / add / remove / access-level-change paths against a wiremock'd UC
// REST API.

fn test_workspace_mgr(server: &MockServer) -> rocky_databricks::workspace::WorkspaceManager {
    let auth = Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .unwrap();
    rocky_databricks::workspace::WorkspaceManager::new("test.databricks.com".into(), auth)
        .with_base_url(server.uri())
}

/// `list_workspace_bindings` returns the current bindings from the UC API.
#[tokio::test]
async fn test_list_workspace_bindings_parses_api_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/bindings/catalog/my_catalog"))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "bindings": [
                { "workspace_id": 100, "binding_type": "BINDING_TYPE_READ_WRITE" },
                { "workspace_id": 200, "binding_type": "BINDING_TYPE_READ_ONLY" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let ws_mgr = test_workspace_mgr(&server);
    let bindings = ws_mgr.get_bindings("my_catalog").await.unwrap();

    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].workspace_id, 100);
    assert_eq!(
        bindings[0].binding_type.as_deref(),
        Some("BINDING_TYPE_READ_WRITE")
    );
    assert_eq!(bindings[1].workspace_id, 200);
    assert_eq!(
        bindings[1].binding_type.as_deref(),
        Some("BINDING_TYPE_READ_ONLY")
    );
}

/// `update_bindings` posts a single PATCH with add + remove payloads, so the
/// reconcile apply is one API call even with many deltas.
#[tokio::test]
async fn test_update_bindings_single_patch_for_add_and_remove() {
    use rocky_databricks::workspace::WorkspaceBinding;
    use wiremock::matchers::body_partial_json;

    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path("/api/2.1/unity-catalog/bindings/catalog/my_catalog"))
        .and(header("Authorization", "Bearer test-token"))
        .and(body_partial_json(serde_json::json!({
            "add": [{"workspace_id": 300, "binding_type": "BINDING_TYPE_READ_ONLY"}],
            "remove": [{"workspace_id": 400}]
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let ws_mgr = test_workspace_mgr(&server);
    ws_mgr
        .update_bindings(
            "my_catalog",
            vec![WorkspaceBinding {
                workspace_id: 300,
                binding_type: Some("BINDING_TYPE_READ_ONLY".into()),
            }],
            vec![WorkspaceBinding {
                workspace_id: 400,
                binding_type: None,
            }],
        )
        .await
        .unwrap();
    // Mock `.expect(1)` would panic at drop if the PATCH wasn't called exactly once.
}

/// Combined reconcile pass: `PermissionManager::reconcile_access` diffs grants
/// and bindings together and applies both in one flow. Verifies the unified
/// plan delta with current grants + current bindings mocked separately from
/// the desired state.
#[tokio::test]
async fn test_reconcile_access_combined_grants_and_bindings() {
    use rocky_core::ir::{Grant, GrantTarget, Permission};
    use rocky_databricks::permissions::{PermissionManager, WorkspaceBindingDesired};

    let server = MockServer::start().await;

    // SHOW GRANTS returns no current grants → desired grant is a pure add.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("SHOW GRANTS"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-show",
            "status": { "state": "SUCCEEDED" },
            "manifest": {
                "schema": { "columns": [
                    {"name": "Principal", "type_name": "STRING", "position": 0},
                    {"name": "ActionType", "type_name": "STRING", "position": 1},
                    {"name": "ObjectType", "type_name": "STRING", "position": 2},
                    {"name": "ObjectKey", "type_name": "STRING", "position": 3}
                ]},
                "total_row_count": 0
            },
            "result": { "data_array": [] }
        })))
        .expect(1)
        .mount(&server)
        .await;

    // GRANT apply — succeeds with an empty statement body.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("GRANT BROWSE"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-grant",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    // GET bindings — current state has workspace 3 (READ_WRITE).
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/bindings/catalog/my_catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "bindings": [
                { "workspace_id": 3, "binding_type": "BINDING_TYPE_READ_WRITE" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    // PATCH bindings — desired adds workspace 2, removes workspace 3.
    Mock::given(method("PATCH"))
        .and(path("/api/2.1/unity-catalog/bindings/catalog/my_catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .expect(1)
        .mount(&server)
        .await;

    let connector = test_connector(&server);
    let ws_mgr = test_workspace_mgr(&server);
    let perm_mgr = PermissionManager::new(&connector);

    let desired_grants = vec![Grant {
        principal: "engineers".into(),
        permission: Permission::Browse,
        target: GrantTarget::Catalog("my_catalog".into()),
    }];
    let desired_bindings = vec![WorkspaceBindingDesired {
        workspace_id: 2,
        binding_type: "BINDING_TYPE_READ_ONLY".into(),
    }];

    let diff = perm_mgr
        .reconcile_access(
            &desired_grants,
            &GrantTarget::Catalog("my_catalog".into()),
            Some(&ws_mgr),
            &desired_bindings,
        )
        .await
        .expect("combined reconcile should succeed");

    // Grants: empty current → one add, nothing revoked.
    assert_eq!(diff.permissions.grants_to_add.len(), 1);
    assert_eq!(diff.permissions.grants_to_add[0].principal, "engineers");
    assert!(diff.permissions.grants_to_revoke.is_empty());

    // Bindings: desired {2}, current {3} → add 2, remove 3.
    assert_eq!(diff.bindings.bindings_to_add.len(), 1);
    assert_eq!(diff.bindings.bindings_to_add[0].workspace_id, 2);
    assert_eq!(
        diff.bindings.bindings_to_add[0].binding_type,
        "BINDING_TYPE_READ_ONLY"
    );
    assert_eq!(diff.bindings.bindings_to_remove.len(), 1);
    assert_eq!(diff.bindings.bindings_to_remove[0].workspace_id, 3);
}

/// Verifies `reconcile_role_graph` routes through the GovernanceAdapter
/// trait dispatch on `DatabricksGovernanceAdapter`.
///
/// v1 is log-only (no SCIM group creation, no per-catalog GRANT
/// application), so this test asserts the trait method returns `Ok(())`
/// and, critically, **no HTTP request is made** — the adapter's failure
/// mode if it accidentally started calling the warehouse would be a
/// wiremock "unmatched request" panic.
#[tokio::test]
async fn test_reconcile_role_graph_is_log_only_v1() {
    use rocky_core::ir::{Permission, ResolvedRole};
    use rocky_core::traits::GovernanceAdapter as _;
    use rocky_databricks::governance::{DatabricksGovernanceAdapter, role_group_name};
    use std::collections::BTreeMap;

    let server = MockServer::start().await;
    // Intentionally no mocks mounted: the test fails fast if v1
    // accidentally starts calling the warehouse.

    let connector = test_connector(&server);
    let adapter = DatabricksGovernanceAdapter::without_workspace(Arc::new(connector));

    let mut roles: BTreeMap<String, ResolvedRole> = BTreeMap::new();
    roles.insert(
        "reader".into(),
        ResolvedRole {
            name: "reader".into(),
            flattened_permissions: vec![Permission::UseCatalog, Permission::Select],
            inherits_from: vec![],
        },
    );
    roles.insert(
        "admin".into(),
        ResolvedRole {
            name: "admin".into(),
            flattened_permissions: vec![
                Permission::UseCatalog,
                Permission::Select,
                Permission::Manage,
            ],
            inherits_from: vec!["reader".into()],
        },
    );

    adapter
        .reconcile_role_graph(&roles)
        .await
        .expect("v1 reconcile_role_graph should succeed without network calls");

    // Naming-convention assertion lives alongside the dispatch test so
    // anyone refactoring the helper sees this test pin the public
    // contract.
    assert_eq!(role_group_name("reader"), "rocky_role_reader");
    assert_eq!(role_group_name("admin"), "rocky_role_admin");
}

/// Empty role graph is a no-op — exercise the early-return path through
/// the trait dispatch.
#[tokio::test]
async fn test_reconcile_role_graph_empty_is_ok() {
    use rocky_core::ir::ResolvedRole;
    use rocky_core::traits::GovernanceAdapter as _;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;
    use std::collections::BTreeMap;

    let server = MockServer::start().await;
    let connector = test_connector(&server);
    let adapter = DatabricksGovernanceAdapter::without_workspace(Arc::new(connector));

    let empty: BTreeMap<String, ResolvedRole> = BTreeMap::new();
    adapter
        .reconcile_role_graph(&empty)
        .await
        .expect("empty role graph should be a no-op");
}

// ---------------------------------------------------------------------------
// Retention policy (Wave C-2)
// ---------------------------------------------------------------------------

/// `apply_retention_policy` translates a RetentionPolicy into the paired
/// Delta TBLPROPERTIES and executes a single ALTER TABLE.
#[tokio::test]
async fn test_apply_retention_policy_emits_paired_tblproperties() {
    use rocky_core::ir::TableRef;
    use rocky_core::retention::RetentionPolicy;
    use rocky_core::traits::GovernanceAdapter;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER TABLE warehouse.silver.events"))
        .and(body_string_contains(
            "'delta.logRetentionDuration' = '90 days'",
        ))
        .and(body_string_contains(
            "'delta.deletedFileRetentionDuration' = '90 days'",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-retention-90",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "events".into(),
    };
    governance
        .apply_retention_policy(&table, &RetentionPolicy { duration_days: 90 })
        .await
        .expect("apply_retention_policy should succeed");
}

/// A `"1y"` (365 days) retention value flows through to the same DDL
/// shape — adapters never see the raw string.
#[tokio::test]
async fn test_apply_retention_policy_year_equivalent_emits_days() {
    use rocky_core::ir::TableRef;
    use rocky_core::retention::RetentionPolicy;
    use rocky_core::traits::GovernanceAdapter;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("'365 days'"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-retention-365",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "gold".into(),
        table: "archive".into(),
    };
    governance
        .apply_retention_policy(&table, &RetentionPolicy { duration_days: 365 })
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// Column tags + masking policy (Wave A — #241 follow-up)
// ---------------------------------------------------------------------------
//
// `apply_column_tags` and `apply_masking_policy` have SQL-generation unit
// coverage in `rocky_core::catalog` and `rocky_core::masking`, but the REST
// path through `DatabricksGovernanceAdapter` into the SQL Statement
// Execution API was wiremock-gapped. These tests pin the REST-level
// contract: request count, per-request body shape, and the iteration
// strategy (one statement per column / per distinct strategy).

/// Happy path: one `ALTER TABLE ... ALTER COLUMN ... SET TAGS (...)` per
/// column — `apply_column_tags` iterates because Unity Catalog rejects
/// multi-column tag DDL in a single statement.
#[tokio::test]
async fn test_apply_column_tags_fires_one_request_per_column() {
    use rocky_core::ir::TableRef;
    use rocky_core::traits::GovernanceAdapter;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;
    use std::collections::BTreeMap;

    let server = MockServer::start().await;

    // One mock per column — each matches the column name in the body and
    // carries its own `.expect(1)` so drift in either direction fails the
    // test (missed column → unsatisfied expect; extra column → unmatched
    // request panic).
    for column in ["email", "phone", "ssn"] {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains("ALTER COLUMN"))
            .and(body_string_contains(column))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": format!("stmt-tag-{column}"),
                "status": { "state": "SUCCEEDED" },
                "manifest": null,
                "result": null,
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "users".into(),
    };

    // Each column gets a distinct classification tag so we can verify the
    // per-column tags_clause makes it into the right request body.
    let mut column_tags: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    column_tags.insert(
        "email".into(),
        BTreeMap::from([("classification".into(), "pii".into())]),
    );
    column_tags.insert(
        "phone".into(),
        BTreeMap::from([("classification".into(), "contact".into())]),
    );
    column_tags.insert(
        "ssn".into(),
        BTreeMap::from([("classification".into(), "sensitive".into())]),
    );

    governance
        .apply_column_tags(&table, &column_tags)
        .await
        .expect("apply_column_tags happy path should succeed");
}

/// Partial failure: columns 1 + 2 succeed, column 3 returns FAILED. The
/// adapter short-circuits via `?` on the third column and surfaces the
/// error; since BTreeMap iterates in sorted order, `col_aaa` + `col_bbb`
/// succeed before `col_zzz` fails. No column fires beyond the failure
/// because the `?` on `set_column_tags` returns immediately.
#[tokio::test]
async fn test_apply_column_tags_short_circuits_on_column_failure() {
    use rocky_core::ir::TableRef;
    use rocky_core::traits::GovernanceAdapter;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;
    use std::collections::BTreeMap;

    let server = MockServer::start().await;

    // col_aaa + col_bbb succeed.
    for column in ["col_aaa", "col_bbb"] {
        Mock::given(method("POST"))
            .and(path("/api/2.0/sql/statements"))
            .and(body_string_contains(column))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "statement_id": format!("stmt-tag-{column}"),
                "status": { "state": "SUCCEEDED" },
                "manifest": null,
                "result": null,
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    // col_zzz returns FAILED — the connector maps SUCCEEDED/FAILED based
    // on the response `status.state`.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("col_zzz"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-tag-col_zzz",
            "status": {
                "state": "FAILED",
                "error": {
                    "error_code": "PERMISSION_DENIED",
                    "message": "cannot set tags on column col_zzz"
                }
            },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "users".into(),
    };

    let mut column_tags: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    column_tags.insert(
        "col_aaa".into(),
        BTreeMap::from([("classification".into(), "ok".into())]),
    );
    column_tags.insert(
        "col_bbb".into(),
        BTreeMap::from([("classification".into(), "ok".into())]),
    );
    column_tags.insert(
        "col_zzz".into(),
        BTreeMap::from([("classification".into(), "fails".into())]),
    );

    let err = governance
        .apply_column_tags(&table, &column_tags)
        .await
        .expect_err("column 3 failure should surface as Err");
    assert!(
        err.to_string().to_lowercase().contains("col_zzz")
            || err.to_string().to_lowercase().contains("permission_denied")
            || err.to_string().to_lowercase().contains("cannot set tags"),
        "error should carry context about the failing column, got: {err}"
    );
    // The `.expect(1)` on every mock guarantees the adapter reached col_zzz
    // (no short-circuit before column 3) and did not re-fire col_aaa / col_bbb.
}

/// Happy path for `apply_masking_policy`: two columns with distinct
/// strategies (Hash + Redact) produce four requests total — Pass 1 fires
/// one `CREATE OR REPLACE FUNCTION` per **distinct strategy** (not per
/// column), Pass 2 fires one `ALTER TABLE ... SET MASK` per column.
///
/// Note: the impl emits `CREATE OR REPLACE FUNCTION ... RETURN <expr>` and
/// `ALTER TABLE ... ALTER COLUMN ... SET MASK <fn>` — not `CREATE MASK` /
/// `SET MASKING POLICY`. See `rocky_core::masking` for the exact shapes.
#[tokio::test]
async fn test_apply_masking_policy_happy_path_hash_plus_redact() {
    use rocky_core::ir::TableRef;
    use rocky_core::traits::{GovernanceAdapter, MaskStrategy, MaskingPolicy};
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    // Pass 1: one CREATE FUNCTION per distinct strategy.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("CREATE OR REPLACE FUNCTION"))
        .and(body_string_contains("rocky_mask_hash_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-create-hash",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("CREATE OR REPLACE FUNCTION"))
        .and(body_string_contains("rocky_mask_redact_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-create-redact",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Pass 2: one SET MASK per column, binding the column to the correct
    // strategy function.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER COLUMN email"))
        .and(body_string_contains("SET MASK"))
        .and(body_string_contains("rocky_mask_hash_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-bind-email",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER COLUMN phone"))
        .and(body_string_contains("SET MASK"))
        .and(body_string_contains("rocky_mask_redact_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-bind-phone",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "customers".into(),
    };

    let mut policy = MaskingPolicy::default();
    policy
        .column_strategies
        .insert("email".into(), MaskStrategy::Hash);
    policy
        .column_strategies
        .insert("phone".into(), MaskStrategy::Redact);

    governance
        .apply_masking_policy(&table, &policy, "prod")
        .await
        .expect("apply_masking_policy happy path should succeed");
}

/// `MaskStrategy::None` mixed with a non-None strategy emits `DROP MASK`
/// for the None column — it is NOT a skip. The adapter's contract (see
/// `apply_masking_policy` Pass 2 in `governance.rs`) is: None overrides a
/// prior mask by explicitly clearing it. For a policy of {None, Hash},
/// the request count is:
///
/// - Pass 1: 1 `CREATE OR REPLACE FUNCTION` for Hash (None is filtered out
///   of `distinct_strategies`).
/// - Pass 2: 1 `DROP MASK` for the None column + 1 `SET MASK` for Hash.
///
/// Total = 3 requests. This is a deliberate departure from "skip None
/// entirely" — documenting that here so a future reader doesn't file a
/// bug against the drop.
#[tokio::test]
async fn test_apply_masking_policy_none_emits_drop_mask_to_clear_prior_state() {
    use rocky_core::ir::TableRef;
    use rocky_core::traits::{GovernanceAdapter, MaskStrategy, MaskingPolicy};
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    // Pass 1: CREATE FUNCTION fires only for Hash (distinct_strategies
    // filters MaskStrategy::None out).
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("CREATE OR REPLACE FUNCTION"))
        .and(body_string_contains("rocky_mask_hash_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-create-hash",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Pass 2a: DROP MASK for the None column.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER COLUMN cleared"))
        .and(body_string_contains("DROP MASK"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-drop-cleared",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    // Pass 2b: SET MASK for the Hash column.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER COLUMN secret"))
        .and(body_string_contains("SET MASK"))
        .and(body_string_contains("rocky_mask_hash_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-bind-secret",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "customers".into(),
    };

    let mut policy = MaskingPolicy::default();
    policy
        .column_strategies
        .insert("cleared".into(), MaskStrategy::None);
    policy
        .column_strategies
        .insert("secret".into(), MaskStrategy::Hash);

    governance
        .apply_masking_policy(&table, &policy, "prod")
        .await
        .expect("mixed None+Hash policy should succeed");
}

/// SQL-injection guard: a column name with a character outside the
/// identifier allowlist (`^[a-zA-Z0-9_]+$`) is rejected by
/// `validate_identifier` inside `generate_set_mask_sql` — the Pass 2
/// helper in `rocky_core::masking`. Before reaching Pass 2, the adapter
/// fires Pass 1 (`CREATE OR REPLACE FUNCTION ...`), which only validates
/// catalog/schema/env (the column name is not interpolated into that
/// DDL). So for a bad column, the sequence is:
///
/// 1. Pass 1 `CREATE FUNCTION` — request fires successfully.
/// 2. Pass 2 `generate_set_mask_sql` — `validate_identifier("bad;col")`
///    returns `Err(ValidationError::InvalidIdentifier)` BEFORE any second
///    HTTP call.
///
/// The pinned contract: the bad column never appears in an outbound SQL
/// statement, and the adapter surfaces a validation error naming the
/// offending value. Pass 2 is strict-mode for the column identifier; it
/// does not get quoted/escaped and slip through.
#[tokio::test]
async fn test_apply_masking_policy_rejects_injection_in_column_identifier() {
    use rocky_core::ir::TableRef;
    use rocky_core::traits::{GovernanceAdapter, MaskStrategy, MaskingPolicy};
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    // Pass 1 succeeds: CREATE FUNCTION doesn't use the column name.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("CREATE OR REPLACE FUNCTION"))
        .and(body_string_contains("rocky_mask_hash_prod"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "stmt-create-hash",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(1)
        .mount(&server)
        .await;

    // No mock for Pass 2 — validation must short-circuit before any
    // second request hits the mock server. Unmatched requests panic the
    // test, so reaching the REST layer with a bad identifier would fail
    // loudly.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("ALTER COLUMN"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "statement_id": "should-not-fire",
            "status": { "state": "SUCCEEDED" },
            "manifest": null,
            "result": null,
        })))
        .expect(0)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "warehouse".into(),
        schema: "silver".into(),
        table: "customers".into(),
    };

    // Column name contains a semicolon — classic SQL-injection probe and
    // outside the `^[a-zA-Z0-9_]+$` identifier allowlist.
    let mut policy = MaskingPolicy::default();
    policy
        .column_strategies
        .insert("bad;col".into(), MaskStrategy::Hash);

    let err = governance
        .apply_masking_policy(&table, &policy, "prod")
        .await
        .expect_err("bad column identifier must be rejected by the validator");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("invalid") || msg.contains("identifier") || msg.contains("bad;col"),
        "validation error should name the identifier rule or bad value, got: {err}"
    );
}

/// Warehouse 400 on the ALTER (unknown property, unsupported table format,
/// etc.) propagates as an AdapterError so the runtime can warn!.
#[tokio::test]
async fn test_apply_retention_policy_propagates_api_error() {
    use rocky_core::ir::TableRef;
    use rocky_core::retention::RetentionPolicy;
    use rocky_core::traits::GovernanceAdapter;
    use rocky_databricks::governance::DatabricksGovernanceAdapter;

    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(400).set_body_json(serde_json::json!({
            "error_code": "INVALID_PARAMETER",
            "message": "retention duration out of range"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let connector = Arc::new(test_connector(&server));
    let governance = DatabricksGovernanceAdapter::without_workspace(connector);
    let table = TableRef {
        catalog: "w".into(),
        schema: "s".into(),
        table: "t".into(),
    };
    let err = governance
        .apply_retention_policy(&table, &RetentionPolicy { duration_days: 999 })
        .await
        .unwrap_err();
    // The AdapterError wraps the connector error; surface any signal we can.
    let msg = err.to_string();
    assert!(!msg.is_empty(), "AdapterError should not be empty: {msg}");
}
