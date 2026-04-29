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
    let result = adapter.discover("src__").await.unwrap();

    assert_eq!(result.connectors.len(), 1);
    assert!(result.failed.is_empty());
    let conn = &result.connectors[0];
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
    let result = adapter.discover("src__").await.unwrap();

    assert_eq!(result.connectors.len(), 1);
    assert!(result.failed.is_empty());
    let conn = &result.connectors[0];
    assert_eq!(conn.metadata["fivetran.service"], "stripe");
    assert_eq!(conn.metadata["fivetran.connector_id"], "conn_bare");
    // No service-specific keys since `config` was absent.
    assert!(!conn.metadata.contains_key("fivetran.custom_reports"));
    assert!(!conn.metadata.contains_key("fivetran.schema_prefix"));
}

/// FR-014 — when one connector's schema fetch returns a sustained 503,
/// the connector must surface in `result.failed` (classified `Transient`)
/// rather than being silently dropped from `result.connectors`. This is
/// the exact failure mode that caused the asset-graph-shrinkage bug Gold
/// patched on the dbt path (Gold commit `c43394d`).
#[tokio::test]
async fn test_discover_surfaces_transient_503_in_failed_sources() {
    let server = MockServer::start().await;

    // Two connectors, both pass the connectors list step.
    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_partial/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_ok",
                        "group_id": "dest_partial",
                        "service": "shopify",
                        "schema": "src__acme__na__shopify",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": "2026-04-25T10:00:00Z",
                        "failed_at": null
                    },
                    {
                        "id": "conn_flaky",
                        "group_id": "dest_partial",
                        "service": "stripe",
                        "schema": "src__acme__na__stripe",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "succeeded_at": "2026-04-25T10:00:00Z",
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    // conn_ok's schema fetch succeeds.
    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_ok/schemas"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "schemas": {
                    "src__acme__na__shopify": {
                        "enabled": true,
                        "tables": {
                            "orders": {
                                "enabled": true,
                                "sync_mode": "SOFT_DELETE",
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

    // conn_flaky's schema fetch returns 503 (server-error). Test client
    // has `max_retries = 0`, so this surfaces immediately to the adapter.
    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_flaky/schemas"))
        .respond_with(ResponseTemplate::new(503).set_body_string("Service Unavailable"))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let adapter = FivetranDiscoveryAdapter::new(client, "dest_partial".into());
    let result = adapter.discover("src__").await.unwrap();

    // The healthy connector still came through.
    assert_eq!(result.connectors.len(), 1, "expected conn_ok in connectors");
    assert_eq!(result.connectors[0].id, "conn_ok");

    // The flaky connector is NOT silently dropped — it's reported as a
    // failed source so downstream consumers can distinguish "tried and
    // failed" from "removed upstream."
    assert_eq!(
        result.failed.len(),
        1,
        "expected conn_flaky in failed_sources, not silently dropped"
    );
    let failed = &result.failed[0];
    assert_eq!(failed.id, "conn_flaky");
    assert_eq!(failed.schema, "src__acme__na__stripe");
    assert_eq!(failed.source_type, "stripe");
    assert_eq!(
        failed.error_class,
        rocky_core::source::FailedSourceErrorClass::Transient,
        "503 should classify as Transient"
    );
    assert!(
        !failed.message.is_empty(),
        "failed source must carry a non-empty message for debugging"
    );
}

/// FR-014 — auth failures classify as `Auth` so consumers can route them
/// to alerting / credentials-rotation paths instead of silently retrying.
#[tokio::test]
async fn test_discover_classifies_401_as_auth() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_auth/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_no_auth",
                        "group_id": "dest_auth",
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
        .and(path("/v1/connectors/conn_no_auth/schemas"))
        .respond_with(ResponseTemplate::new(401).set_body_string("Unauthorized"))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let adapter = FivetranDiscoveryAdapter::new(client, "dest_auth".into());
    let result = adapter.discover("src__").await.unwrap();

    assert!(result.connectors.is_empty());
    assert_eq!(result.failed.len(), 1);
    assert_eq!(
        result.failed[0].error_class,
        rocky_core::source::FailedSourceErrorClass::Auth
    );
}

/// Path-injection guard: user-supplied IDs must be percent-encoded before
/// being interpolated into the URL path. The mock server is configured to
/// match only on the encoded path, so a regression that drops encoding
/// would surface as a 404-shaped error instead of the expected success.
#[tokio::test]
async fn test_list_connectors_percent_encodes_group_id() {
    let server = MockServer::start().await;

    // Caller passes a group_id that contains every metacharacter we care
    // about: `/`, `?`, `#`, `..`, and `%`. The wire format must encode
    // each of them so the server sees a single path segment.
    let raw_group_id = "../evil/group?x=1#frag%2F";
    let encoded = "%2E%2E%2Fevil%2Fgroup%3Fx%3D1%23frag%252F";

    Mock::given(method("GET"))
        .and(path(format!("/v1/groups/{encoded}/connectors")))
        .and(query_param("limit", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": { "items": [], "next_cursor": null }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let result = connector::list_connectors(&client, raw_group_id)
        .await
        .expect("encoded request should succeed");
    assert!(result.is_empty());
}

/// Same guard for `GET /v1/connectors/{id}` — single-connector lookups
/// are the most exposed path because `connector_id` is the most common
/// caller-controlled value in the discovery flow.
#[tokio::test]
async fn test_get_connector_percent_encodes_connector_id() {
    let server = MockServer::start().await;

    let raw_id = "conn/../admin?token=x";
    let encoded = "conn%2F%2E%2E%2Fadmin%3Ftoken%3Dx";

    Mock::given(method("GET"))
        .and(path(format!("/v1/connectors/{encoded}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": "conn_safe",
                "group_id": "g",
                "service": "stripe",
                "schema": "src__a__b__stripe",
                "status": { "setup_state": "connected", "sync_state": "scheduled" },
                "succeeded_at": null,
                "failed_at": null
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let conn = connector::get_connector(&client, raw_id)
        .await
        .expect("encoded request should succeed");
    assert_eq!(conn.id, "conn_safe");
}

/// Same guard for `GET /v1/connectors/{id}/schemas`.
#[tokio::test]
async fn test_get_schema_config_percent_encodes_connector_id() {
    let server = MockServer::start().await;

    let raw_id = "id with space/and?slash";
    let encoded = "id%20with%20space%2Fand%3Fslash";

    Mock::given(method("GET"))
        .and(path(format!("/v1/connectors/{encoded}/schemas")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": { "schemas": {} }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let cfg = schema::get_schema_config(&client, raw_id)
        .await
        .expect("encoded request should succeed");
    assert!(cfg.schemas.is_empty());
}
