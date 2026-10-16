//! #2395: the Databricks adapter reads `current_timezone()` once and refuses
//! a session zone other than UTC before a watermark filter is built.

use std::time::Duration;

use rocky_core::config::RetryConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use serde_json::json;
use wiremock::matchers::{body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn adapter(server: &MockServer) -> DatabricksWarehouseAdapter {
    let auth = Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .expect("PAT auth is infallible");
    let config = ConnectorConfig {
        host: "test.databricks.com".into(),
        warehouse_id: "test-warehouse".into(),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
    };
    DatabricksWarehouseAdapter::new(
        DatabricksConnector::new(config, auth).with_base_url(server.uri()),
    )
}

async fn mount_zone(server: &MockServer, zone: &str, expected_calls: u64) {
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .and(body_string_contains("current_timezone()"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-tz",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "current_timezone()", "type_name": "STRING", "position": 0}
                    ]
                },
                "total_row_count": 1
            },
            "result": {"data_array": [[zone]]}
        })))
        .expect(expected_calls)
        .mount(server)
        .await;
}

#[tokio::test]
async fn a_utc_session_passes_and_is_read_once() {
    let server = MockServer::start().await;
    mount_zone(&server, "Etc/UTC", 1).await;
    let adapter = adapter(&server);
    adapter
        .check_watermark_time_zone()
        .await
        .expect("UTC passes");
    adapter
        .check_watermark_time_zone()
        .await
        .expect("cached pass");
}

#[tokio::test]
async fn a_session_west_of_utc_is_refused() {
    let server = MockServer::start().await;
    mount_zone(&server, "America/New_York", 2).await;
    let adapter = adapter(&server);
    let err = adapter
        .check_watermark_time_zone()
        .await
        .expect_err("a non-UTC zone is refused");
    let text = err.to_string();
    assert!(text.contains("America/New_York"), "{text}");
    assert!(text.contains("TIMEZONE"), "{text}");
    // A refusal is not cached: the next call reads the zone again.
    adapter
        .check_watermark_time_zone()
        .await
        .expect_err("still refused");
}
