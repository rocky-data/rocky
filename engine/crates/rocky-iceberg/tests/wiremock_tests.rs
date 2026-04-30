//! Wire-level mock tests for the Iceberg discovery adapter.
//!
//! Uses wiremock to simulate the Iceberg REST Catalog at the HTTP level
//! and verifies that `IcebergError` variants flow through to the
//! correct `FailedSourceErrorClass` on the discovered `failed_sources`
//! payload.

use rocky_core::source::FailedSourceErrorClass;
use rocky_core::traits::DiscoveryAdapter;
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
    assert_eq!(
        result.failed[0].error_class,
        FailedSourceErrorClass::Auth
    );
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
