//! Wire-level mock tests for the Iceberg discovery adapter and the
//! catalog-client adapter.
//!
//! Uses wiremock to simulate the Iceberg REST Catalog at the HTTP level
//! and verifies that `IcebergError` variants flow through to the
//! correct `FailedSourceErrorClass` on the discovered `failed_sources`
//! payload, and that the `CatalogClient` implementation maps REST
//! status codes onto the appropriate `CatalogError` variants.

use rocky_catalog_core::{
    BranchKind, CatalogClient, CatalogError, ColumnSchema, Grant, TableCommit, TableRef,
    TableSchema, TableStats,
};
use rocky_core::source::FailedSourceErrorClass;
use rocky_core::traits::DiscoveryAdapter;
use rocky_iceberg::IcebergCatalogClientAdapter;
use rocky_iceberg::adapter::IcebergDiscoveryAdapter;
use rocky_iceberg::client::IcebergCatalogClient;
use serde_json::json;
use wiremock::matchers::{body_json, method, path};
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

// ---------------------------------------------------------------------------
// Lifecycle method tests — PR-3
// ---------------------------------------------------------------------------

fn sample_table() -> TableRef {
    TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    }
}

fn sample_schema() -> TableSchema {
    TableSchema {
        columns: vec![
            ColumnSchema {
                name: "id".into(),
                type_str: "long".into(),
                nullable: false,
            },
            ColumnSchema {
                name: "amount".into(),
                type_str: "double".into(),
                nullable: true,
            },
        ],
    }
}

/// Minimal `LoadTableResponse` body — Iceberg REST returns the freshly
/// created table on `POST /v1/namespaces/{ns}/tables`, so the happy
/// path needs a body that satisfies our partial deser surface.
fn minimal_load_table_body() -> serde_json::Value {
    json!({
        "metadata-location": "s3://bucket/path/metadata.json",
        "metadata": {
            "current-schema-id": 0,
            "schemas": [{
                "schema-id": 0,
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" }
                ]
            }]
        }
    })
}

#[tokio::test]
async fn create_table_happy_path_posts_to_namespace_endpoint() {
    let server = MockServer::start().await;

    // Expected wire body: name + schema with fields + sequential ids.
    let expected_body = json!({
        "name": "orders",
        "schema": {
            "type": "struct",
            "schema-id": 0,
            "fields": [
                { "id": 1, "name": "id", "required": true, "type": "long" },
                { "id": 2, "name": "amount", "required": false, "type": "double" }
            ]
        }
    });

    Mock::given(method("POST"))
        .and(path("/v1/namespaces/analytics/tables"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(200).set_body_json(minimal_load_table_body()))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    adapter
        .create_table(&sample_table(), &sample_schema())
        .await
        .expect("create_table should succeed");
}

#[tokio::test]
async fn create_table_404_maps_to_namespace_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/namespaces/missing/tables"))
        .respond_with(ResponseTemplate::new(404).set_body_string("namespace not found"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let table = TableRef {
        catalog: None,
        namespace: vec!["missing".into()],
        name: "orders".into(),
    };
    let err = adapter
        .create_table(&table, &sample_schema())
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::NamespaceNotFound(_)),
        "expected NamespaceNotFound (rewritten from default TableNotFound), got {err:?}"
    );
}

#[tokio::test]
async fn create_table_409_maps_to_commit_conflict() {
    // A 409 on create_table indicates a name collision (the catalog's
    // serial table-name CAS rejected the new row). The trait surfaces
    // this as `CommitConflict` because the recovery shape is the same
    // as a transaction CAS failure: re-read state, decide whether to
    // retry under a different name or abort.
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/namespaces/analytics/tables"))
        .respond_with(ResponseTemplate::new(409).set_body_string("already exists"))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let err = adapter
        .create_table(&sample_table(), &sample_schema())
        .await
        .expect_err("409 should not succeed");
    assert!(
        matches!(err, CatalogError::CommitConflict(_)),
        "expected CommitConflict, got {err:?}"
    );
}

#[tokio::test]
async fn drop_table_happy_path_calls_delete() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path("/v1/namespaces/analytics/tables/orders"))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    adapter
        .drop_table(&sample_table())
        .await
        .expect("drop_table should succeed");
}

#[tokio::test]
async fn drop_table_404_maps_to_table_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
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
        .drop_table(&table)
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::TableNotFound(_)),
        "expected TableNotFound (default), got {err:?}"
    );
}

#[tokio::test]
async fn commit_transaction_happy_path_assert_ref_snapshot_id() {
    let server = MockServer::start().await;

    let expected_body = json!({
        "table-changes": [{
            "identifier": {
                "namespace": ["analytics"],
                "name": "orders"
            },
            "requirements": [{
                "type": "assert-ref-snapshot-id",
                "ref": "main",
                "snapshot-id": 42
            }],
            "updates": [{
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "type": "branch",
                "snapshot-id": 43
            }]
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let commit = TableCommit::new(sample_table(), Some(42), 43);
    adapter
        .commit_transaction(&[commit])
        .await
        .expect("commit_transaction should succeed");
}

#[tokio::test]
async fn commit_transaction_assert_create_when_no_base_snapshot() {
    let server = MockServer::start().await;

    let expected_body = json!({
        "table-changes": [{
            "identifier": {
                "namespace": ["analytics"],
                "name": "orders"
            },
            "requirements": [{ "type": "assert-create" }],
            "updates": [{
                "action": "set-snapshot-ref",
                "ref-name": "main",
                "type": "branch",
                "snapshot-id": 1
            }]
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let commit = TableCommit::new(sample_table(), None, 1);
    adapter
        .commit_transaction(&[commit])
        .await
        .expect("commit_transaction should succeed");
}

#[tokio::test]
async fn commit_transaction_409_maps_to_commit_conflict() {
    // The discriminating case: 409 on `/v1/transactions/commit` is the
    // CAS failure the trait contract surfaces as `CommitConflict`.
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .respond_with(
            ResponseTemplate::new(409)
                .set_body_string("expected ref main at snapshot 42, found 99 — concurrent writer"),
        )
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let commit = TableCommit::new(sample_table(), Some(42), 43);
    let err = adapter
        .commit_transaction(&[commit])
        .await
        .expect_err("409 should not succeed");
    assert!(
        matches!(err, CatalogError::CommitConflict(_)),
        "expected CommitConflict, got {err:?}"
    );
}

#[tokio::test]
async fn commit_transaction_empty_slice_is_a_noop() {
    // No mock — if the adapter fires a request for an empty slice the
    // test fails because the request is unmatched. Empty short-circuit
    // lets callers compose commits without special-casing empty
    // batches.
    let server = MockServer::start().await;
    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    adapter
        .commit_transaction(&[])
        .await
        .expect("empty commit slice should be a no-op");
}

#[tokio::test]
async fn commit_transaction_advances_named_branch() {
    // Pins the multi-ref headline: `TableCommit::with_branch(Some("dev_feature"))`
    // threads through to BOTH the assertion (`assert-ref-snapshot-id.ref`)
    // and the update (`set-snapshot-ref.ref-name`) on the wire body.
    // The `body_json` matcher fails the test if the adapter advanced
    // the wrong ref.
    let server = MockServer::start().await;

    let expected_body = json!({
        "table-changes": [{
            "identifier": {
                "namespace": ["analytics"],
                "name": "orders"
            },
            "requirements": [{
                "type": "assert-ref-snapshot-id",
                "ref": "dev_feature",
                "snapshot-id": 42
            }],
            "updates": [{
                "action": "set-snapshot-ref",
                "ref-name": "dev_feature",
                "type": "branch",
                "snapshot-id": 43
            }]
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let commit =
        TableCommit::new(sample_table(), Some(42), 43).with_branch(Some("dev_feature".into()));
    adapter
        .commit_transaction(&[commit])
        .await
        .expect("multi-ref commit should succeed");
}

#[tokio::test]
async fn commit_transaction_threads_schema_and_partition_updates() {
    // Pins the schema-update + partition-spec passthrough: opaque JSON
    // blobs supplied via `with_extra_updates` land in the wire body's
    // `updates` array *after* the synthesized `set-snapshot-ref` entry.
    // Likewise `with_extra_requirements` appends after the synthesized
    // assert. Order is load-bearing.
    let server = MockServer::start().await;

    let extra_requirement = json!({
        "type": "assert-last-assigned-field-id",
        "last-assigned-field-id": 7
    });
    let add_schema_update = json!({
        "action": "add-schema",
        "schema": {
            "type": "struct",
            "schema-id": 1,
            "fields": [
                { "id": 1, "name": "id", "required": true, "type": "long" },
                { "id": 2, "name": "name", "required": false, "type": "string" }
            ]
        }
    });
    let add_partition_spec_update = json!({
        "action": "add-partition-spec",
        "spec": {
            "spec-id": 1,
            "fields": [
                { "source-id": 1, "field-id": 1000, "name": "id_bucket",
                  "transform": "bucket[16]" }
            ]
        }
    });

    let expected_body = json!({
        "table-changes": [{
            "identifier": {
                "namespace": ["analytics"],
                "name": "orders"
            },
            "requirements": [
                {
                    "type": "assert-ref-snapshot-id",
                    "ref": "main",
                    "snapshot-id": 42
                },
                extra_requirement
            ],
            "updates": [
                {
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "type": "branch",
                    "snapshot-id": 43
                },
                add_schema_update,
                add_partition_spec_update
            ]
        }]
    });

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let commit = TableCommit::new(sample_table(), Some(42), 43)
        .with_extra_requirements(vec![json!({
            "type": "assert-last-assigned-field-id",
            "last-assigned-field-id": 7
        })])
        .with_extra_updates(vec![
            json!({
                "action": "add-schema",
                "schema": {
                    "type": "struct",
                    "schema-id": 1,
                    "fields": [
                        { "id": 1, "name": "id", "required": true, "type": "long" },
                        { "id": 2, "name": "name", "required": false, "type": "string" }
                    ]
                }
            }),
            json!({
                "action": "add-partition-spec",
                "spec": {
                    "spec-id": 1,
                    "fields": [
                        { "source-id": 1, "field-id": 1000, "name": "id_bucket",
                          "transform": "bucket[16]" }
                    ]
                }
            }),
        ]);

    adapter
        .commit_transaction(&[commit])
        .await
        .expect("commit with schema + partition updates should succeed");
}

#[tokio::test]
async fn commit_transaction_multi_table_with_per_table_refs() {
    // Multi-table commit where each table advances its own ref — pins
    // that the ref-selection is per-`TableCommit`, not a single
    // batch-wide default. A real consumer (the writer running a
    // multi-table replication slice) needs this: dev-branch and
    // main-branch tables in the same atomic batch.
    let server = MockServer::start().await;

    let expected_body = json!({
        "table-changes": [
            {
                "identifier": { "namespace": ["analytics"], "name": "orders" },
                "requirements": [{
                    "type": "assert-ref-snapshot-id",
                    "ref": "main",
                    "snapshot-id": 100
                }],
                "updates": [{
                    "action": "set-snapshot-ref",
                    "ref-name": "main",
                    "type": "branch",
                    "snapshot-id": 101
                }]
            },
            {
                "identifier": { "namespace": ["analytics"], "name": "customers" },
                "requirements": [{
                    "type": "assert-ref-snapshot-id",
                    "ref": "dev_feature",
                    "snapshot-id": 200
                }],
                "updates": [{
                    "action": "set-snapshot-ref",
                    "ref-name": "dev_feature",
                    "type": "branch",
                    "snapshot-id": 201
                }]
            }
        ]
    });

    Mock::given(method("POST"))
        .and(path("/v1/transactions/commit"))
        .and(body_json(&expected_body))
        .respond_with(ResponseTemplate::new(204))
        .expect(1)
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let main_commit = TableCommit::new(
        TableRef {
            catalog: None,
            namespace: vec!["analytics".into()],
            name: "orders".into(),
        },
        Some(100),
        101,
    );
    let dev_commit = TableCommit::new(
        TableRef {
            catalog: None,
            namespace: vec!["analytics".into()],
            name: "customers".into(),
        },
        Some(200),
        201,
    )
    .with_branch(Some("dev_feature".into()));

    adapter
        .commit_transaction(&[main_commit, dev_commit])
        .await
        .expect("multi-table multi-ref commit should succeed");
}

#[tokio::test]
async fn list_branches_projects_refs_to_branch_refs() {
    let server = MockServer::start().await;

    // `load_table` body with two refs: main (branch, snapshot 1) and
    // v1.0.0 (tag, snapshot 2).
    let body = json!({
        "metadata-location": "s3://bucket/path/metadata.json",
        "metadata": {
            "current-schema-id": 0,
            "schemas": [{
                "schema-id": 0,
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" }
                ]
            }],
            "refs": {
                "main": { "snapshot-id": 1, "type": "branch" },
                "v1.0.0": { "snapshot-id": 2, "type": "tag" }
            }
        }
    });

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let branches = adapter
        .list_branches(&sample_table())
        .await
        .expect("list_branches should succeed");

    // Sorted by name for determinism.
    assert_eq!(branches.len(), 2);
    assert_eq!(branches[0].name, "main");
    assert_eq!(branches[0].snapshot_id, Some(1));
    assert!(matches!(branches[0].kind, BranchKind::Branch));

    assert_eq!(branches[1].name, "v1.0.0");
    assert_eq!(branches[1].snapshot_id, Some(2));
    assert!(matches!(branches[1].kind, BranchKind::Tag));
}

#[tokio::test]
async fn list_branches_returns_empty_when_refs_missing() {
    // Older catalogs may omit `refs` from the metadata entirely.
    // serde's `#[serde(default)]` means we get an empty HashMap rather
    // than a deser failure.
    let server = MockServer::start().await;

    let body = json!({
        "metadata-location": "s3://bucket/path/metadata.json",
        "metadata": {
            "current-schema-id": 0,
            "schemas": [{
                "schema-id": 0,
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" }
                ]
            }]
        }
    });

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);

    let branches = adapter
        .list_branches(&sample_table())
        .await
        .expect("list_branches should succeed when refs absent");
    assert!(branches.is_empty());
}

/// `load_table` body with two snapshots; only snapshot 2 is current
/// and carries the spec-defined summary keys. Used to verify
/// `table_stats` picks the current snapshot's summary, not snapshot 1.
fn load_table_body_with_snapshots() -> serde_json::Value {
    json!({
        "metadata-location": "s3://bucket/path/metadata.json",
        "metadata": {
            "current-schema-id": 0,
            "schemas": [
                {
                    "schema-id": 0,
                    "fields": [
                        { "id": 1, "name": "id", "required": true, "type": "long" }
                    ]
                }
            ],
            "current-snapshot-id": 2,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "summary": {
                        "operation": "append",
                        "total-records": "100",
                        "total-files-size": "1024",
                        "total-data-files": "1"
                    }
                },
                {
                    "snapshot-id": 2,
                    "summary": {
                        "operation": "append",
                        "added-records": "150",
                        "total-records": "250",
                        "total-files-size": "4096",
                        "total-data-files": "4"
                    }
                }
            ]
        }
    })
}

#[tokio::test]
async fn table_stats_happy_path_parses_current_snapshot_summary() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/orders"))
        .respond_with(ResponseTemplate::new(200).set_body_json(load_table_body_with_snapshots()))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "orders".into(),
    };

    let stats = adapter
        .table_stats(&table)
        .await
        .expect("table_stats should succeed");

    // Picked snapshot-id == 2 (current); snapshot 1's summary is ignored.
    // `total-records` (cumulative) is the source-of-truth, NOT
    // `added-records` (delta).
    assert_eq!(stats.row_count, Some(250));
    assert_eq!(stats.total_bytes, Some(4096));
    assert_eq!(stats.file_count, Some(4));
}

#[tokio::test]
async fn table_stats_returns_empty_when_no_current_snapshot() {
    // Freshly-created Iceberg tables have no snapshots yet — the spec
    // permits `current-snapshot-id` to be absent (or `null`).
    let server = MockServer::start().await;

    let body = json!({
        "metadata": {
            "current-schema-id": 0,
            "schemas": [
                { "schema-id": 0, "fields": [] }
            ]
        }
    });
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/empty"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "empty".into(),
    };

    let stats = adapter
        .table_stats(&table)
        .await
        .expect("table_stats should succeed even with no snapshots");
    assert_eq!(stats, TableStats::empty());
}

#[tokio::test]
async fn table_stats_returns_partial_when_summary_keys_missing() {
    // Writers populate the summary inconsistently — the Unity
    // foreign-Iceberg path is known to lag spec compliance. We must
    // surface what's there without failing the whole call when a key
    // is absent.
    let server = MockServer::start().await;

    let body = json!({
        "metadata": {
            "current-schema-id": 0,
            "schemas": [
                { "schema-id": 0, "fields": [] }
            ],
            "current-snapshot-id": 7,
            "snapshots": [
                {
                    "snapshot-id": 7,
                    "summary": {
                        "operation": "append",
                        "total-files-size": "2048"
                        // total-records + total-data-files intentionally
                        // absent
                    }
                }
            ]
        }
    });
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/partial"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "partial".into(),
    };

    let stats = adapter
        .table_stats(&table)
        .await
        .expect("partial summary should not error");
    assert_eq!(stats.row_count, None);
    assert_eq!(stats.total_bytes, Some(2048));
    assert_eq!(stats.file_count, None);
}

#[tokio::test]
async fn table_stats_ignores_unparseable_summary_values() {
    // Catalogs occasionally emit non-numeric values (e.g. `"unknown"`,
    // or scientific notation). Trait contract: silent fall-back to
    // None rather than failing the whole `table_stats` call.
    let server = MockServer::start().await;

    let body = json!({
        "metadata": {
            "current-schema-id": 0,
            "schemas": [
                { "schema-id": 0, "fields": [] }
            ],
            "current-snapshot-id": 1,
            "snapshots": [
                {
                    "snapshot-id": 1,
                    "summary": {
                        "total-records": "not_a_number",
                        "total-files-size": "1024",
                        "total-data-files": "1.5e3"
                    }
                }
            ]
        }
    });
    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/weird"))
        .respond_with(ResponseTemplate::new(200).set_body_json(body))
        .mount(&server)
        .await;

    let client = IcebergCatalogClient::new(&server.uri(), None);
    let adapter = IcebergCatalogClientAdapter::new(client);
    let table = TableRef {
        catalog: None,
        namespace: vec!["analytics".into()],
        name: "weird".into(),
    };

    let stats = adapter
        .table_stats(&table)
        .await
        .expect("unparseable values must not error");
    assert_eq!(stats.row_count, None, "not_a_number → None");
    assert_eq!(stats.total_bytes, Some(1024));
    assert_eq!(
        stats.file_count, None,
        "scientific notation is not a u64 → None"
    );
}

#[tokio::test]
async fn table_stats_404_maps_to_table_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/v1/namespaces/analytics/tables/missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("not found"))
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
        .table_stats(&table)
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::TableNotFound(_)),
        "expected TableNotFound, got {err:?}"
    );
}

#[tokio::test]
async fn list_branches_404_maps_to_table_not_found() {
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
        .list_branches(&table)
        .await
        .expect_err("404 should not succeed");
    assert!(
        matches!(err, CatalogError::TableNotFound(_)),
        "expected TableNotFound, got {err:?}"
    );
}
