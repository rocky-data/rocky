//! Wire-level mock tests for the Iceberg discovery adapter and the
//! catalog-client adapter.
//!
//! Uses wiremock to simulate the Iceberg REST Catalog at the HTTP level
//! and verifies that `IcebergError` variants flow through to the
//! correct `FailedSourceErrorClass` on the discovered `failed_sources`
//! payload, and that the `CatalogClient` implementation maps REST
//! status codes onto the appropriate `CatalogError` variants.

use rocky_catalog_core::{CatalogClient, CatalogError, Grant, TableRef};
use rocky_core::source::FailedSourceErrorClass;
use rocky_core::traits::DiscoveryAdapter;
use rocky_iceberg::IcebergCatalogClientAdapter;
use rocky_iceberg::adapter::IcebergDiscoveryAdapter;
use rocky_iceberg::client::IcebergCatalogClient;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[tokio::test]
async fn list_tables_401_classifies_as_auth() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": [["good_ns"], ["bad_ns"]]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/good_ns/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": []
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/bad_ns/tables"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergDiscoveryAdapter::new(client);
    let result = adapter.discover("").await.expect("discover should succeed");

    assert_eq!(result.connectors.len(), 1, "good_ns should be discovered");
    assert_eq!(result.connectors[0].id, "good_ns");

    assert_eq!(result.failed.len(), 1, "bad_ns should be a failed source");
    let failed = &result.failed[0];
    assert_eq!(failed.id, "bad_ns");
    assert_eq!(failed.error_class, FailedSourceErrorClass::Auth);
    assert!(
        failed.message.contains("401"),
        "message should preserve the status code, got: {}",
        failed.message
    );
}

#[tokio::test]
async fn list_tables_403_classifies_as_auth() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": [["forbidden_ns"]]
        })))
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/forbidden_ns/tables"))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergDiscoveryAdapter::new(client);
    let result = adapter.discover("").await.expect("discover should succeed");

    assert_eq!(result.connectors.len(), 0);
    assert_eq!(result.failed.len(), 1);
    assert_eq!(result.failed[0].error_class, FailedSourceErrorClass::Auth);
}

#[tokio::test]
async fn list_tables_unknown_status_classifies_as_unknown() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "namespaces": [["weird_ns"]]
        })))
        .mount(&server)
        .await;

    // 404 isn't in the Auth/Timeout/RateLimit/Transient classes — falls
    // through to Unknown. Pinning this avoids accidental Transient
    // classification of misconfigured endpoints, which would have
    // consumers assume "retry will fix it" when it won't.
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/weird_ns/tables"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergDiscoveryAdapter::new(client);
    let result = adapter.discover("").await.expect("discover should succeed");

    assert_eq!(result.failed.len(), 1);
    assert_eq!(
        result.failed[0].error_class,
        FailedSourceErrorClass::Unknown
    );
}

// ---------------------------------------------------------------------------
// CatalogClient adapter tests
// ---------------------------------------------------------------------------

/// `load_table` response with two schemas; only the second is current.
/// Used by happy-path tests below.
fn load_table_body_two_schemas() -> serde_json::Value {
    json!({
        "metadata-location": "s3://bucket/path/metadata.json",
        "metadata": {
            "current-schema-id": 2,
            "schemas": [
                {
                    "schema-id": 1,
                    "fields": [
                        { "id": 1, "name": "old_col", "required": true, "type": "long" }
                    ]
                },
                {
                    "schema-id": 2,
                    "fields": [
                        { "id": 1, "name": "id", "required": true, "type": "long" },
                        { "id": 2, "name": "amount", "required": false, "type": "double" },
                        {
                            "id": 3,
                            "name": "tags",
                            "required": false,
                            "type": {
                                "type": "list",
                                "element-id": 4,
                                "element": "string",
                                "element-required": true
                            }
                        }
                    ]
                }
            ]
        }
    })
}

#[tokio::test]
async fn describe_table_happy_path_distills_current_schema() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(load_table_body_two_schemas()))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    };

    let schema = adapter
        .describe_table(&table)
        .await
        .expect("describe_table should succeed");

    // Picked schema-id == 2 (current); the schema-id == 1 entry is
    // ignored. Required → nullable inversion holds.
    assert_eq!(schema.columns.len(), 3);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].type_str, "long");
    assert!(!schema.columns[0].nullable);

    assert_eq!(schema.columns[1].name, "amount");
    assert_eq!(schema.columns[1].type_str, "double");
    assert!(schema.columns[1].nullable);

    // Nested type — serialised back to JSON rather than dropped.
    assert_eq!(schema.columns[2].name, "tags");
    assert!(
        schema.columns[2].type_str.contains("\"type\":\"list\""),
        "nested-type column should round-trip JSON, got {}",
        schema.columns[2].type_str
    );
    assert!(schema.columns[2].nullable);
}

#[tokio::test]
async fn describe_table_404_maps_to_table_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("table does not exist"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "missing".into(),
    };

    let err = adapter
        .describe_table(&table)
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::TableNotFound(_)),
        "expected TableNotFound, got {err:?}"
    );
}

#[tokio::test]
async fn describe_table_401_maps_to_auth_failed() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/private"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "private".into(),
    };

    let err = adapter
        .describe_table(&table)
        .await
        .expect_err("401 should not succeed");
    assert!(
        matches!(err, CatalogError::AuthFailed(_)),
        "expected AuthFailed, got {err:?}"
    );
}

#[tokio::test]
async fn list_tables_via_adapter_routes_to_inner_client() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "identifiers": [
                { "namespace": ["analytics"], "name": "orders" },
                { "namespace": ["analytics"], "name": "customers" }
            ]
        })))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let tables = adapter
        .list_tables(&["analytics".to_string()])
        .await
        .expect("list_tables should succeed");

    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].name, "orders");
    assert_eq!(tables[0].namespace, vec!["analytics".to_string()]);
    assert!(tables[0].catalog.is_none());
    assert_eq!(tables[1].name, "customers");
}

#[tokio::test]
async fn list_tables_404_maps_to_namespace_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/nope/tables"))
        .respond_with(ResponseTemplate::new(404).set_body_string("namespace not found"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let err = adapter
        .list_tables(&["nope".to_string()])
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::NamespaceNotFound(_)),
        "expected NamespaceNotFound (rewritten from default TableNotFound), got {err:?}"
    );
}

#[tokio::test]
async fn tag_table_returns_unsupported_operation() {
    // No HTTP mock needed — tag_table short-circuits before any wire
    // traffic. The constructor still requires a base URL, so we point
    // it at a never-contacted MockServer to be explicit that no
    // request must be issued.
    let server = MockServer::start().await;
    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    };

    let err = adapter
        .tag_table(&table, "owner", "analytics")
        .await
        .expect_err("tag_table must be UnsupportedOperation");
    assert!(
        matches!(err, CatalogError::UnsupportedOperation(_)),
        "expected UnsupportedOperation, got {err:?}"
    );
}

#[tokio::test]
async fn governance_methods_all_return_unsupported_operation() {
    let server = MockServer::start().await;
    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    };
    let grant = Grant {
        principal: "analytics@example.com".into(),
        privilege: "SELECT".into(),
    };

    assert!(matches!(
        adapter.get_grants(&table).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
    assert!(matches!(
        adapter.apply_grant(&table, &grant).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
    assert!(matches!(
        adapter.revoke_grant(&table, &grant).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
}

#[tokio::test]
async fn deferred_lifecycle_methods_return_unsupported_operation() {
    let server = MockServer::start().await;
    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    };
    let schema = rocky_catalog_core::TableSchema {
        columns: vec![rocky_catalog_core::ColumnSchema {
            name: "id".into(),
            type_str: "long".into(),
            nullable: false,
        }],
    };

    assert!(matches!(
        adapter.create_table(&table, &schema).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
    assert!(matches!(
        adapter.drop_table(&table).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
    assert!(matches!(
        adapter.commit_transaction(&[]).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
    assert!(matches!(
        adapter.list_branches(&table).await.unwrap_err(),
        CatalogError::UnsupportedOperation(_)
    ));
}
