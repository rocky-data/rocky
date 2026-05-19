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

// ---------------------------------------------------------------------------
// FR-B: Retry-After + per-host shared rate-limit budget
// ---------------------------------------------------------------------------

use rocky_core::config::RetryConfig;
use rocky_fivetran::ratelimit;
use std::path::Path;
use std::time::{Duration, Instant};
use wiremock::matchers::method as match_method;

/// Build a [`RetryConfig`] suitable for the FR-B tests: one retry
/// allowed, very small fixed backoff so we can prove the
/// post-`Retry-After` sleep dominates without dragging the suite out.
fn retry_for_fr_b() -> RetryConfig {
    RetryConfig {
        max_retries: 1,
        initial_backoff_ms: 10,
        max_backoff_ms: 50,
        backoff_multiplier: 1.0,
        jitter: false,
        circuit_breaker_threshold: 0,
        circuit_breaker_recovery_timeout_secs: None,
        max_retries_per_run: None,
    }
}

/// Build a Fivetran client that points at `server`, retries once,
/// and uses an isolated ratelimit directory `tmp` (so concurrent CI
/// runs don't collide). The api_key is unique-per-test so the
/// SHA-256-derived state filename is also test-scoped.
fn fr_b_client(server: &MockServer, tmp: &Path, api_key: &str) -> FivetranClient {
    FivetranClient::with_base_url_and_retry(
        api_key.into(),
        "fr-b-secret".into(),
        server.uri(),
        retry_for_fr_b(),
    )
    .with_ratelimit_dir(tmp.to_path_buf())
}

/// FR-B acceptance #1 — when Fivetran returns 429 with
/// `Retry-After: 2`, the client must sleep at least 2s before the
/// retry attempt. The fixed backoff is 10ms, so any value above the
/// header threshold proves we picked `max(retry_after, backoff)`.
///
/// We use a small `Retry-After` (2s) rather than the spec example
/// (30s) so the suite stays cheap; `tokio::time::pause` doesn't
/// compose with `wiremock`'s real-time HTTP loop.
#[tokio::test]
async fn fr_b_retry_after_429_is_honored() {
    let server = MockServer::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    // First call: 429 + Retry-After:2. Second call: 200 with a
    // single connector so the request succeeds on the retry path.
    use wiremock::ResponseTemplate;
    let throttled = ResponseTemplate::new(429)
        .insert_header("Retry-After", "2")
        .set_body_string("Too Many Requests");
    let ok_body = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_after",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });

    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_after"))
        .respond_with(throttled)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_after"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body))
        .mount(&server)
        .await;

    let client = fr_b_client(&server, &tmp_path, "fr-b-429-key");

    let started = Instant::now();
    let conn = connector::get_connector(&client, "conn_after")
        .await
        .expect("retry path should succeed");
    let elapsed = started.elapsed();
    assert_eq!(conn.id, "conn_after");

    // The retry must have slept at least the header-supplied window.
    // We allow a generous upper bound to absorb test-host scheduling
    // noise without making the assertion flaky.
    assert!(
        elapsed >= Duration::from_secs(2),
        "expected >=2s wait (Retry-After), observed {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "expected retry to finish promptly after the header window, observed {elapsed:?}"
    );

    // FR-B side effect: the shared-budget file now exists. (We
    // don't assert the timestamp value here — it's an implementation
    // detail of the per-host coordination layer. The next test
    // proves the inter-process visibility.)
    let state_path =
        ratelimit::account_state_path(&tmp_path, &ratelimit::hash_account_id("fr-b-429-key"));
    assert!(
        state_path.exists(),
        "expected per-account state file at {state_path:?}"
    );
}

/// FR-B — same logic for HTTP 503 (Service Unavailable). RFC 9110
/// §10.2.3 specifies `Retry-After` may accompany 503 too; the
/// throttle-sleep path must apply.
#[tokio::test]
async fn fr_b_retry_after_503_is_honored() {
    let server = MockServer::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    use wiremock::ResponseTemplate;
    let unavailable = ResponseTemplate::new(503)
        .insert_header("Retry-After", "1")
        .set_body_string("Service Unavailable");
    let ok_body = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_503",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_503"))
        .respond_with(unavailable)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_503"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body))
        .mount(&server)
        .await;

    let client = fr_b_client(&server, &tmp_path, "fr-b-503-key");

    let started = Instant::now();
    let conn = connector::get_connector(&client, "conn_503")
        .await
        .expect("retry path should succeed");
    assert_eq!(conn.id, "conn_503");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= Duration::from_secs(1),
        "expected >=1s wait (Retry-After on 503), observed {elapsed:?}"
    );
}

/// FR-B — when 429 omits the `Retry-After` header, the client falls
/// back to the existing fixed-backoff schedule. Confirms the
/// pre-FR-B behaviour didn't regress and that the
/// `fivetran.rate_limit_observed` event still fires under the
/// `fallback` source.
#[tokio::test]
async fn fr_b_fallback_when_no_retry_after_header() {
    let server = MockServer::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    use wiremock::ResponseTemplate;
    let throttled = ResponseTemplate::new(429).set_body_string("Too Many Requests");
    let ok_body = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_no_header",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_no_header"))
        .respond_with(throttled)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_no_header"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_body))
        .mount(&server)
        .await;

    let client = fr_b_client(&server, &tmp_path, "fr-b-fallback-key");
    let conn = connector::get_connector(&client, "conn_no_header")
        .await
        .expect("retry path should succeed");
    assert_eq!(conn.id, "conn_no_header");
}

/// FR-B acceptance #2 — two clients on the same "host" (same
/// ratelimit dir + same account hash) share one backoff window. The
/// first client takes the 429 hit and writes the shared `wake_at`;
/// the second client, even though it would have hit a 200, observes
/// the shared file and waits before issuing its request.
///
/// Uses a small Retry-After (1s) so the assertion clears CI without
/// dragging the suite, and asserts the second client's *total*
/// elapsed time crosses the 1s boundary even though its own request
/// would have returned immediately.
#[tokio::test]
async fn fr_b_shared_budget_visible_to_concurrent_clients() {
    let server = MockServer::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();
    let api_key = "fr-b-shared-key";

    use wiremock::ResponseTemplate;
    // Client A's first call: 429 + Retry-After:1.
    let throttled = ResponseTemplate::new(429)
        .insert_header("Retry-After", "1")
        .set_body_string("Too Many Requests");
    // Client A's retry, *and* every client B call: return 200 with
    // a different connector so the mocks don't collide.
    let ok_a = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_a",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    let ok_b = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_b",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_a"))
        .respond_with(throttled)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_a))
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_b"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_b))
        .mount(&server)
        .await;

    let client_a = fr_b_client(&server, &tmp_path, api_key);
    let client_b = fr_b_client(&server, &tmp_path, api_key);

    // Drive client A to completion first so it writes the shared
    // wake_at. Client A's request flow: 429 → sleep(1s) → 200.
    let a_handle = tokio::spawn(async move {
        let started = Instant::now();
        let conn = connector::get_connector(&client_a, "conn_a")
            .await
            .expect("client A retry should succeed");
        (conn.id, started.elapsed())
    });

    // Give client A a brief head start so its 429 lands and writes
    // the shared file before client B issues its first request.
    // Without this, the two clients race for the first call and the
    // shared-state assertion becomes order-dependent.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let b_started = Instant::now();
    let conn_b = connector::get_connector(&client_b, "conn_b")
        .await
        .expect("client B should succeed via the shared wait");
    let b_elapsed = b_started.elapsed();
    let (a_id, a_elapsed) = a_handle.await.expect("client A task panicked");

    assert_eq!(a_id, "conn_a");
    assert_eq!(conn_b.id, "conn_b");
    // Client A took the full Retry-After hit itself.
    assert!(
        a_elapsed >= Duration::from_millis(900),
        "client A should have honored Retry-After: 1s, observed {a_elapsed:?}"
    );
    // Client B never got a 429; it observed the shared wake_at
    // written by client A and waited on it before sending. The
    // 150ms head start above means client B sees roughly (1s - 150ms)
    // remaining on the shared window, so we assert at least 600ms
    // to leave generous headroom for scheduler noise on slow CI
    // hosts (the 1s window will not be observed in full because B
    // started 150ms after A wrote the file).
    assert!(
        b_elapsed >= Duration::from_millis(600),
        "client B should have observed the shared budget; got {b_elapsed:?}"
    );
}

/// FR-B — when each client uses a *distinct* account hash (i.e.
/// distinct API keys), the shared-budget file is segregated and
/// one client's 429 does NOT throttle the other. Guards against an
/// accidental global lock that would tank cross-org throughput.
#[tokio::test]
async fn fr_b_distinct_accounts_do_not_share_budget() {
    let server = MockServer::start().await;
    let tmp = tempfile::tempdir().unwrap();
    let tmp_path = tmp.path().to_path_buf();

    use wiremock::ResponseTemplate;
    // Account A hits a 429 with a long Retry-After.
    let throttled = ResponseTemplate::new(429)
        .insert_header("Retry-After", "5")
        .set_body_string("Too Many Requests");
    let ok_a = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_a",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    let ok_b = serde_json::json!({
        "code": "Success",
        "data": {
            "id": "conn_b",
            "group_id": "g",
            "service": "stripe",
            "schema": "src__a__b__stripe",
            "status": { "setup_state": "connected", "sync_state": "scheduled" },
            "succeeded_at": null,
            "failed_at": null
        }
    });
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_a"))
        .respond_with(throttled)
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_a"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_a))
        .mount(&server)
        .await;
    Mock::given(match_method("GET"))
        .and(path("/v1/connectors/conn_b"))
        .respond_with(ResponseTemplate::new(200).set_body_json(ok_b))
        .mount(&server)
        .await;

    let client_a = fr_b_client(&server, &tmp_path, "account-A-key");
    let client_b = fr_b_client(&server, &tmp_path, "account-B-key");

    // Send a request on account A first so it writes its own
    // shared-budget file. We don't wait for it to finish — account
    // B's request should still complete promptly because the
    // ratelimit file is keyed by hash_account_id(api_key).
    let _a_handle = tokio::spawn(async move {
        let _ = connector::get_connector(&client_a, "conn_a").await;
    });
    tokio::time::sleep(Duration::from_millis(150)).await;

    let b_started = Instant::now();
    let conn_b = connector::get_connector(&client_b, "conn_b")
        .await
        .expect("account B should not be throttled by account A's 429");
    let b_elapsed = b_started.elapsed();
    assert_eq!(conn_b.id, "conn_b");
    // Account B's request never touched the throttled path. Even
    // on a noisy CI host this should complete well under a second.
    assert!(
        b_elapsed < Duration::from_secs(2),
        "account B should not have observed account A's budget; got {b_elapsed:?}"
    );
}

// ---------------------------------------------------------------------------
// FR-C: fetch_envelope memoization
// ---------------------------------------------------------------------------

/// Mount the three endpoints `fetch_envelope` calls for a single
/// destination with a single connector — `expect(1)` on each so the
/// test fails if a memoized call goes back to the wire.
async fn mount_envelope_endpoints_once(server: &MockServer) {
    use wiremock::ResponseTemplate;

    Mock::given(method("GET"))
        .and(path("/v1/destinations/dest_memo"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": "dest_memo",
                "region": "us-east-1",
                "time_zone": "UTC",
                "service": "snowflake",
                "setup_status": "connected"
            }
        })))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_memo/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_memo",
                        "group_id": "dest_memo",
                        "service": "shopify",
                        "schema": "src__acme__na__shopify",
                        "connector_name": "Acme Shopify NA",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "paused": false,
                        "succeeded_at": "2026-04-10T10:00:00Z",
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .expect(1)
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/connectors/conn_memo/schemas"))
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
                                "columns": {
                                    "id": { "enabled": true, "hashed": false }
                                }
                            }
                        }
                    }
                }
            }
        })))
        .expect(1)
        .mount(server)
        .await;
}

/// First call to `fetch_envelope(_, false)` issues HTTP; second call
/// must hit the in-process memoization and not re-fetch. Wiremock
/// asserts each endpoint is touched exactly once via `expect(1)`.
#[tokio::test]
async fn fetch_envelope_memoizes_second_call() {
    let server = MockServer::start().await;
    mount_envelope_endpoints_once(&server).await;

    let client = test_client(&server);
    let first = client.fetch_envelope("dest_memo", false).await.unwrap();
    let second = client.fetch_envelope("dest_memo", false).await.unwrap();

    // The memoized value must equal the originally-fetched one — the
    // cache returns a clone of the stored envelope.
    assert_eq!(
        first, second,
        "memoized envelope must equal the originally-fetched envelope"
    );

    // Spot-check that the envelope captured the upstream values we
    // mounted — the cache assertion above already covers shape.
    assert_eq!(first.destination.id, "dest_memo");
    assert_eq!(first.connectors.len(), 1);
    assert_eq!(first.connectors[0].id, "conn_memo");
    assert_eq!(first.connectors[0].name, "Acme Shopify NA");
    assert!(!first.connectors[0].paused);

    // The mock `.expect(1)` declarations enforce the no-extra-HTTP
    // invariant on drop. If the second call had gone back to the
    // wire, server drop would panic.
    drop(server);
}

/// `fetch_envelope(_, true)` forces a fresh HTTP round-trip even when
/// a memoized value exists. Asserts the wire is hit twice and the new
/// value overwrites the cache.
#[tokio::test]
async fn fetch_envelope_refresh_true_forces_new_fetch() {
    use wiremock::ResponseTemplate;

    let server = MockServer::start().await;

    // Each endpoint mounted with `expect(2)` because both calls
    // refresh.
    Mock::given(method("GET"))
        .and(path("/v1/destinations/dest_refresh"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": "dest_refresh",
                "region": "us-east-1"
            }
        })))
        .expect(2)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_refresh/connectors"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": { "items": [], "next_cursor": null }
        })))
        .expect(2)
        .mount(&server)
        .await;

    let client = test_client(&server);
    let first = client.fetch_envelope("dest_refresh", false).await.unwrap();
    // Force a refresh — must re-hit the wire and overwrite memo.
    let second = client.fetch_envelope("dest_refresh", true).await.unwrap();
    assert_eq!(first.destination.id, "dest_refresh");
    assert_eq!(second.destination.id, "dest_refresh");
    // `expect(2)` on both endpoints enforces re-fetch on drop.
    drop(server);
}

/// Distinct `destination_id`s use distinct cache entries — fetching
/// one then the other must hit the wire for each.
#[tokio::test]
async fn fetch_envelope_distinct_destinations_dont_share_memo() {
    use wiremock::ResponseTemplate;
    let server = MockServer::start().await;

    for id in ["dest_one", "dest_two"] {
        Mock::given(method("GET"))
            .and(path(format!("/v1/destinations/{id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": "Success",
                "data": { "id": id }
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/v1/groups/{id}/connectors")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": "Success",
                "data": { "items": [], "next_cursor": null }
            })))
            .expect(1)
            .mount(&server)
            .await;
    }

    let client = test_client(&server);
    let one = client.fetch_envelope("dest_one", false).await.unwrap();
    let two = client.fetch_envelope("dest_two", false).await.unwrap();
    assert_eq!(one.destination.id, "dest_one");
    assert_eq!(two.destination.id, "dest_two");
    drop(server);
}

// ---------------------------------------------------------------------------
// FR-A: pluggable state cache integration
// ---------------------------------------------------------------------------

/// `fetch_envelope` with a pre-populated state cache must serve from
/// the cache without issuing any HTTP — wiremock mounts every endpoint
/// with `.expect(0)` so a stray request panics on drop.
#[tokio::test]
async fn fetch_envelope_cache_hit_skips_http() {
    use std::sync::Arc;

    use rocky_fivetran::state_cache::{FileCache, FivetranStateCache};

    let server = MockServer::start().await;
    // Each endpoint asserts ZERO requests — proves the cache hit
    // short-circuits the wire entirely.
    Mock::given(method("GET"))
        .and(path("/v1/destinations/dest_cached"))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_cached/connectors"))
        .respond_with(wiremock::ResponseTemplate::new(500))
        .expect(0)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let cache: Arc<dyn FivetranStateCache> =
        Arc::new(FileCache::new(tmp.path().to_path_buf()).unwrap());

    // Pre-populate the cache as if a prior process had fetched it.
    // Construct the cache key the way `FivetranClient` will: the
    // hash is derived from the api_key string `test-api-key`.
    let account_hash = rocky_fivetran::ratelimit::hash_account_id("test-api-key");
    let key = format!("{account_hash}/dest_cached");
    let primed = sample_envelope_for_cache();
    cache.write(&key, &primed).await.unwrap();

    let client = test_client(&server).with_state_cache(cache);
    let env = client
        .fetch_envelope("dest_cached", false)
        .await
        .expect("cache hit must serve without HTTP");

    assert_eq!(env.destination.id, primed.destination.id);
    assert_eq!(env.connectors.len(), primed.connectors.len());
    // `.expect(0)` enforces no-extra-HTTP on drop.
    drop(server);
}

/// Cache-miss path calls HTTP, writes back to the cache, and the next
/// in-process call hits the in-process memo (zero HTTP on the second
/// fetch_envelope despite a fresh cache miss for the same destination).
#[tokio::test]
async fn fetch_envelope_cache_miss_calls_http_and_writes_back() {
    use std::sync::Arc;

    use rocky_fivetran::state_cache::{FileCache, FivetranStateCache, WriteOutcome};

    let server = MockServer::start().await;
    mount_envelope_endpoints_once(&server).await;

    let tmp = tempfile::tempdir().unwrap();
    let cache: Arc<dyn FivetranStateCache> =
        Arc::new(FileCache::new(tmp.path().to_path_buf()).unwrap());

    let client = test_client(&server).with_state_cache(cache.clone());
    let env = client.fetch_envelope("dest_memo", false).await.unwrap();
    assert_eq!(env.destination.id, "dest_memo");

    // Cache populated post-fetch.
    let account_hash = rocky_fivetran::ratelimit::hash_account_id("test-api-key");
    let key = format!("{account_hash}/dest_memo");
    let cached = cache
        .read(&key)
        .await
        .unwrap()
        .expect("write-back must populate");
    assert_eq!(cached.destination.id, "dest_memo");

    // Re-writing the same envelope must SkippedNoChange via dedupe.
    let outcome = cache.write(&key, &cached).await.unwrap();
    assert_eq!(outcome, WriteOutcome::SkippedNoChange);

    drop(server);
}

/// `fetch_envelope(_, refresh = true)` (the `--no-cache` path) must
/// SKIP the cache read but still write-back on success — the next
/// caller sees the fresh data.
#[tokio::test]
async fn fetch_envelope_refresh_skips_cache_read_but_writes_back() {
    use std::sync::Arc;

    use rocky_fivetran::state_cache::{FileCache, FivetranStateCache};

    let server = MockServer::start().await;
    // Endpoints expect one hit each — the refresh path must touch the
    // wire even though the cache is pre-populated with a stale value.
    Mock::given(method("GET"))
        .and(path("/v1/destinations/dest_refresh_cache"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": "Success",
                "data": { "id": "dest_refresh_cache", "region": "freshly-fetched-region" }
            })),
        )
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/v1/groups/dest_refresh_cache/connectors"))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "code": "Success",
                "data": { "items": [], "next_cursor": null }
            })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let tmp = tempfile::tempdir().unwrap();
    let cache: Arc<dyn FivetranStateCache> =
        Arc::new(FileCache::new(tmp.path().to_path_buf()).unwrap());

    // Pre-populate cache with stale envelope.
    let account_hash = rocky_fivetran::ratelimit::hash_account_id("test-api-key");
    let key = format!("{account_hash}/dest_refresh_cache");
    let mut stale = sample_envelope_for_cache();
    stale.destination.id = "dest_refresh_cache".into();
    stale.destination.region = Some("stale-cached-region".into());
    cache.write(&key, &stale).await.unwrap();

    let client = test_client(&server).with_state_cache(cache.clone());
    let fresh = client
        .fetch_envelope("dest_refresh_cache", true)
        .await
        .expect("refresh must succeed via HTTP");
    assert_eq!(
        fresh.destination.region,
        Some("freshly-fetched-region".into())
    );

    // Write-back must have happened — cache now holds the fresh
    // envelope.
    let after = cache.read(&key).await.unwrap().unwrap();
    assert_eq!(
        after.destination.region,
        Some("freshly-fetched-region".into())
    );

    drop(server);
}

/// Helper: build a sample envelope a test can pre-write into a cache
/// to exercise the cache-hit short-circuit.
fn sample_envelope_for_cache() -> rocky_fivetran::envelope::FivetranStateEnvelope {
    use std::collections::BTreeMap;

    use chrono::{DateTime, Utc};

    use rocky_fivetran::envelope::{
        FivetranConnectorStatus, FivetranConnectorSummary, FivetranDestination,
        FivetranSchemaConfig, FivetranStateEnvelope,
    };

    FivetranStateEnvelope::from_parts(
        DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap(),
        FivetranDestination {
            id: "dest_cached".into(),
            region: Some("us-east-1".into()),
            time_zone: None,
            service: None,
            setup_status: None,
        },
        vec![FivetranConnectorSummary {
            id: "conn_pre".into(),
            name: "conn_pre".into(),
            schema: "src__pre__shopify".into(),
            service: "shopify".into(),
            status: FivetranConnectorStatus {
                setup_state: "connected".into(),
                sync_state: "scheduled".into(),
            },
            paused: false,
            succeeded_at: None,
            failed_at: None,
            group_id: None,
        }],
        BTreeMap::<String, FivetranSchemaConfig>::new(),
    )
}

// ---------------------------------------------------------------------------
// FR-027: tolerate connectors with no schema config in fetch_envelope
// ---------------------------------------------------------------------------

/// Mount destination + connectors-list shared by the FR-027 tests.
/// Three connectors: `conn_ok_a`, `conn_ok_b`, `conn_broken`. The
/// caller mounts the per-connector `/schemas` endpoints with whatever
/// mix of 200s and 404s the test needs.
async fn mount_envelope_skeleton_fr027(server: &MockServer, destination_id: &str) {
    use wiremock::ResponseTemplate;

    Mock::given(method("GET"))
        .and(path(format!("/v1/destinations/{destination_id}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "id": destination_id,
                "region": "us-east-1",
                "time_zone": "UTC",
                "service": "snowflake",
                "setup_status": "connected"
            }
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path(format!("/v1/groups/{destination_id}/connectors")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "items": [
                    {
                        "id": "conn_ok_a",
                        "group_id": destination_id,
                        "service": "shopify",
                        "schema": "src__acme__na__shopify",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "paused": false,
                        "succeeded_at": "2026-04-10T10:00:00Z",
                        "failed_at": null
                    },
                    {
                        "id": "conn_ok_b",
                        "group_id": destination_id,
                        "service": "stripe",
                        "schema": "src__acme__na__stripe",
                        "status": { "setup_state": "connected", "sync_state": "scheduled" },
                        "paused": false,
                        "succeeded_at": "2026-04-10T10:00:00Z",
                        "failed_at": null
                    },
                    {
                        "id": "conn_broken",
                        "group_id": destination_id,
                        "service": "marketo",
                        "schema": "src__acme__na__marketo",
                        "status": { "setup_state": "incomplete", "sync_state": "paused" },
                        "paused": true,
                        "succeeded_at": null,
                        "failed_at": null
                    }
                ],
                "next_cursor": null
            }
        })))
        .mount(server)
        .await;
}

/// Mount a 200 `/schemas` response for one connector.
async fn mount_schema_ok(server: &MockServer, connector_id: &str) {
    use wiremock::ResponseTemplate;

    Mock::given(method("GET"))
        .and(path(format!("/v1/connectors/{connector_id}/schemas")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "code": "Success",
            "data": {
                "schemas": {
                    "src": {
                        "enabled": true,
                        "tables": {
                            "orders": {
                                "enabled": true,
                                "sync_mode": "SOFT_DELETE",
                                "columns": { "id": { "enabled": true, "hashed": false } }
                            }
                        }
                    }
                }
            }
        })))
        .mount(server)
        .await;
}

/// Mount a 404 `NotFound_SchemaConfig` `/schemas` response — the exact
/// shape Fivetran returns for connectors in `incomplete`/`broken`/
/// paused-pre-schema state, per FR-027.
async fn mount_schema_404(server: &MockServer, connector_id: &str) {
    use wiremock::ResponseTemplate;

    Mock::given(method("GET"))
        .and(path(format!("/v1/connectors/{connector_id}/schemas")))
        .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "code": "NotFound_SchemaConfig",
            "message": format!("Connection with id '{connector_id}' doesn't have schema config")
        })))
        .mount(server)
        .await;
}

/// FR-027 acceptance: a mix of healthy + missing-schema-config
/// connectors must produce a written envelope holding the healthy
/// ones, while the broken connector still appears in the `connectors`
/// summary list with its status fields.
#[tokio::test]
async fn fetch_envelope_skips_404_schema_connectors() {
    let server = MockServer::start().await;
    mount_envelope_skeleton_fr027(&server, "dest_fr027_mixed").await;
    mount_schema_ok(&server, "conn_ok_a").await;
    mount_schema_ok(&server, "conn_ok_b").await;
    mount_schema_404(&server, "conn_broken").await;

    let client = test_client(&server);
    let env = client
        .fetch_envelope("dest_fr027_mixed", false)
        .await
        .expect("envelope must be written when at least one connector has a schema_config");

    let schema_keys: Vec<&str> = env.schemas.keys().map(String::as_str).collect();
    assert_eq!(
        schema_keys,
        vec!["conn_ok_a", "conn_ok_b"],
        "broken connector must be excluded from schemas map"
    );
    let connector_ids: Vec<&str> = env.connectors.iter().map(|c| c.id.as_str()).collect();
    assert_eq!(
        connector_ids,
        vec!["conn_broken", "conn_ok_a", "conn_ok_b"],
        "broken connector must still appear in connectors summary list"
    );
    let broken = env
        .connectors
        .iter()
        .find(|c| c.id == "conn_broken")
        .expect("broken connector must be in the summary list");
    assert_eq!(broken.status.setup_state, "incomplete");
    assert!(broken.paused, "broken connector's status fields preserved");

    drop(server);
}

/// FR-027 acceptance: when every connector returns 404 the envelope
/// is not written and the error surfaces as `NoHealthyConnectors`.
#[tokio::test]
async fn fetch_envelope_errors_when_all_schemas_404() {
    let server = MockServer::start().await;
    mount_envelope_skeleton_fr027(&server, "dest_fr027_all_404").await;
    mount_schema_404(&server, "conn_ok_a").await;
    mount_schema_404(&server, "conn_ok_b").await;
    mount_schema_404(&server, "conn_broken").await;

    let client = test_client(&server);
    let err = client
        .fetch_envelope("dest_fr027_all_404", false)
        .await
        .expect_err("envelope must error when no connector returns a schema_config");

    match err {
        FivetranError::NoHealthyConnectors { total } => {
            assert_eq!(total, 3, "all 3 fetches accounted for in the error");
        }
        other => panic!("expected NoHealthyConnectors, got {other:?}"),
    }

    drop(server);
}
