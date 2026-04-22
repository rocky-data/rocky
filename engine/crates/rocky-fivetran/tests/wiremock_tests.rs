//! Wire-level mock tests for the Fivetran REST API client.
//!
//! Uses wiremock to simulate the Fivetran REST API at the HTTP level,
//! verifying correct behavior for connector listing, auth failures,
//! pagination, and malformed responses.

use rocky_core::traits::DiscoveryAdapter;
use rocky_fivetran::adapter::FivetranDiscoveryAdapter;
use rocky_fivetran::client::{FivetranClient, FivetranError};
use rocky_fivetran::connector;
use rocky_fivetran::schema;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Creates a Fivetran client pointing at the given wiremock server.
fn test_client(server: &MockServer) -> FivetranClient {
    FivetranClient::with_base_url(
        "test-api-key".into(),
        "test-api-secret".into(),
        server.uri(),
    )
}

/// Happy path: list connectors for a group.
#[tokio::test]
async fn test_list_connectors_happy_path() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_abc/connectors"))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_1",
                        "group_id": "dest_abc",
                        "service": "shopify",
                        "schema": "src__acme__na__shopify",
                        "status": {
                            "setup_state": "connected",
                            "sync_state": "scheduled"
                        },
                        "succeeded_at": "2026-04-10T10:00:00Z",
                        "failed_at": null
                    },
                    {
                        "id": "conn_2",
                        "group_id": "dest_abc",
                        "service": "stripe",
                        "schema": "src__acme__na__stripe",
                        "status": {
                            "setup_state": "connected",
                            "sync_state": "scheduled"
                        },
                        "succeeded_at": null,
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let connectors = connector::list_connectors(&client, "dest_abc")
        .await
        .unwrap();

    assert_eq!(connectors.len(), 2);
    assert_eq!(connectors[0].id, "conn_1");
    assert_eq!(connectors[0].service, "shopify");
    assert!(connectors[0].is_connected());
    assert_eq!(connectors[1].id, "conn_2");
    assert_eq!(connectors[1].service, "stripe");
}

/// Auth failure: 401 produces an Api error.
#[tokio::test]
async fn test_auth_failure_401() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_abc/connectors"))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized: invalid API key"))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::list_connectors(&client, "dest_abc").await;

    let err = result.unwrap_err();
    match &err {
        FivetranError::Api { code, message } => {
            assert!(code.contains("401"));
            assert!(message.contains("Unauthorized"));
        }
        other => panic!("expected Api error, got: {other}"),
    }
}

/// Malformed response: invalid JSON body.
#[tokio::test]
async fn test_malformed_json_response() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_123"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not valid json at all"))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::get_connector(&client, "conn_123").await;

    let err = result.unwrap_err();
    assert!(
        matches!(err, FivetranError::UnexpectedResponse(_)),
        "expected UnexpectedResponse, got: {err}"
    );
}

/// API returns Success code but no data field.
#[tokio::test]
async fn test_missing_data_field() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": null,
            "message": null
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::get_connector(&client, "conn_123").await;

    let err = result.unwrap_err();
    assert!(
        matches!(err, FivetranError::UnexpectedResponse(_)),
        "expected UnexpectedResponse for null data, got: {err}"
    );
}

/// API returns a non-Success code in the envelope.
#[tokio::test]
async fn test_api_envelope_error() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_bad"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "NotFound",
            "data": null,
            "message": "Connector not found"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::get_connector(&client, "conn_bad").await;

    let err = result.unwrap_err();
    match &err {
        FivetranError::Api { code, message } => {
            assert_eq!(code, "NotFound");
            assert!(message.contains("not found"));
        }
        other => panic!("expected Api error, got: {other}"),
    }
}

/// Get single connector happy path.
#[tokio::test]
async fn test_get_single_connector() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_abc"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": "conn_abc",
                "group_id": "group_xyz",
                "service": "google_ads",
                "schema": "src__globex__emea__google_ads",
                "status": {
                    "setup_state": "connected",
                    "sync_state": "syncing"
                },
                "succeeded_at": "2026-04-11T08:30:00Z",
                "failed_at": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let conn = connector::get_connector(&client, "conn_abc").await.unwrap();

    assert_eq!(conn.id, "conn_abc");
    assert_eq!(conn.service, "google_ads");
    assert!(conn.is_connected());
    assert!(conn.succeeded_at.is_some());
}

/// Server error (500) with no retries configured.
#[tokio::test]
async fn test_server_error_500() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_err"))
        .respond_with(ResponseTemplate::new(500).set_body_string("Internal Server Error"))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::get_connector(&client, "conn_err").await;

    let err = result.unwrap_err();
    match &err {
        FivetranError::Api { code, .. } => {
            assert!(code.contains("500"));
        }
        other => panic!("expected Api error, got: {other}"),
    }
}

/// Schema config: happy path with nested schema/table/column structure.
#[tokio::test]
async fn test_get_schema_config() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_schema/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "schemas": {
                    "src__acme__na__fb_ads": {
                        "enabled": true,
                        "tables": {
                            "campaigns": {
                                "enabled": true,
                                "sync_mode": "SOFT_DELETE",
                                "columns": {
                                    "id": {"enabled": true, "hashed": false},
                                    "name": {"enabled": true, "hashed": false}
                                }
                            },
                            "disabled_table": {
                                "enabled": false,
                                "columns": {}
                            }
                        }
                    }
                }
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let config = schema::get_schema_config(&client, "conn_schema")
        .await
        .unwrap();

    let tables = config.enabled_tables();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].table_name, "campaigns");
    assert_eq!(tables[0].column_count, 2);
    assert_eq!(config.enabled_table_count(), 1);
}

/// Discover connectors filters by prefix and connected status.
#[tokio::test]
async fn test_discover_connectors_filters() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_filter/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "c1",
                        "group_id": "dest_filter",
                        "service": "shopify",
                        "schema": "src__acme__shopify",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": null,
                        "failed_at": null
                    },
                    {
                        "id": "c2",
                        "group_id": "dest_filter",
                        "service": "stripe",
                        "schema": "other__prefix__stripe",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": null,
                        "failed_at": null
                    },
                    {
                        "id": "c3",
                        "group_id": "dest_filter",
                        "service": "hubspot",
                        "schema": "src__acme__hubspot",
                        "status": { "setup_state": "incomplete", "sync_state": "scheduled" },
                        "succeeded_at": null,
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let filtered = connector::discover_connectors(&client, "dest_filter", "src__")
        .await
        .unwrap();

    // c1 matches prefix and is connected
    // c2 wrong prefix -> excluded
    // c3 right prefix but incomplete -> excluded
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].id, "c1");
}

/// End-to-end discover path: the adapter should project the connector's
/// service-specific `config` into the adapter-neutral
/// [`DiscoveredConnector::metadata`] map, using the `fivetran.*` key
/// namespace. This is what lets downstream consumers branch on connector
/// type (stock vs custom) without re-calling the Fivetran API.
#[tokio::test]
async fn test_discover_projects_namespaced_metadata() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_meta/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_ads",
                        "group_id": "dest_meta",
                        "service": "facebook_ads",
                        "schema": "src__acme__na__fb_ads",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": null,
                        "failed_at": null,
                        "config": {
                            "schema_prefix": "fb_ads",
                            "custom_tables": [
                                {"table_name": "ads_insights", "breakdowns": ["age"]}
                            ],
                            "reports": [
                                {"name": "custom_report_revenue", "report_type": "ads_insights"}
                            ]
                        }
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_ads/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "schemas": {
                    "src__acme__na__fb_ads": {
                        "enabled": true,
                        "tables": {
                            "ads_insights": {
                                "enabled": true,
                                "columns": {"id": {"enabled": true}}
                            }
                        }
                    }
                }
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let adapter = FivetranDiscoveryAdapter::new(client, "dest_meta".into());
    let connectors = adapter.discover("src__").await.unwrap();

    assert_eq!(connectors.len(), 1);
    let conn = &connectors[0];
    assert_eq!(conn.id, "conn_ads");
    assert_eq!(conn.source_type, "facebook_ads");
    // Core identity is always stamped.
    assert_eq!(conn.metadata["fivetran.service"], "facebook_ads");
    assert_eq!(conn.metadata["fivetran.connector_id"], "conn_ads");
    // Service-specific projections relay the JSON verbatim.
    assert_eq!(conn.metadata["fivetran.schema_prefix"], "fb_ads");
    assert!(conn.metadata["fivetran.custom_tables"].is_array());
    // Fivetran's wire-level `config.reports` is surfaced under
    // `fivetran.custom_reports` — the semantic rename is load-bearing for
    // downstream stock-vs-custom branching.
    assert_eq!(
        conn.metadata["fivetran.custom_reports"][0]["name"],
        "custom_report_revenue"
    );
    assert!(!conn.metadata.contains_key("fivetran.reports"));
}

/// Connectors without a `config` blob (partial-setup, legacy) still get
/// core identity metadata so downstream consumers can always rely on
/// `fivetran.service` / `fivetran.connector_id` being present.
#[tokio::test]
async fn test_discover_without_config_still_stamps_identity() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_noconfig/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_bare",
                        "group_id": "dest_noconfig",
                        "service": "stripe",
                        "schema": "src__acme__na__stripe",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": null,
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_bare/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": { "schemas": {} }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let adapter = FivetranDiscoveryAdapter::new(client, "dest_noconfig".into());
    let connectors = adapter.discover("src__").await.unwrap();

    assert_eq!(connectors.len(), 1);
    let conn = &connectors[0];
    assert_eq!(conn.metadata["fivetran.service"], "stripe");
    assert_eq!(conn.metadata["fivetran.connector_id"], "conn_bare");
    // No service-specific keys since `config` was absent.
    assert!(!conn.metadata.contains_key("fivetran.custom_reports"));
    assert!(!conn.metadata.contains_key("fivetran.schema_prefix"));
}
