//! Wire-level mock tests for [`UnityCatalogClient`] against Unity REST
//! endpoints.
//!
//! Uses wiremock to simulate `/api/2.1/unity-catalog/*` paths at the
//! HTTP level, verifying URL paths, body shapes, and the
//! [`UnityRestError`] → [`CatalogError`] mapping for the production
//! call sites.
//!
//! Requires the `test-support` cargo feature so [`UnityCatalogClient::with_base_url`]
//! is callable. Run via:
//!
//! ```sh
//! cargo test -p rocky-databricks --features test-support --test unity_catalog_client_wiremock
//! ```
//!
//! [`UnityCatalogClient`]: rocky_databricks::UnityCatalogClient
//! [`UnityCatalogClient::with_base_url`]: rocky_databricks::UnityCatalogClient::with_base_url
//! [`UnityRestError`]: rocky_databricks::UnityRestError
//! [`CatalogError`]: rocky_catalog_core::CatalogError

use rocky_catalog_core::{
    CatalogClient, CatalogError, ColumnSchema, GovernanceCatalogClient, Grant, Securable, TableRef,
    TableSchema,
};
use rocky_databricks::UnityCatalogClient;
use rocky_databricks::auth::{Auth, AuthConfig};
use serde_json::json;
use wiremock::matchers::{body_partial_json, header, method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn test_client(server: &MockServer) -> UnityCatalogClient {
    let auth = Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .expect("PAT auth");
    UnityCatalogClient::new("test.databricks.com".into(), auth).with_base_url(server.uri())
}

fn sample_table_ref() -> TableRef {
    TableRef {
        catalog: Some("hcv2_cat".into()),
        namespace: vec!["hcv2_sch".into()],
        name: "hcv2_orders".into(),
    }
}

// ---------------------------------------------------------------------------
// describe_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn describe_table_happy_path() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .and(header("Authorization", "Bearer test-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "hcv2_orders",
            "columns": [
                {"name": "id", "type_text": "bigint", "type_name": "LONG", "nullable": false},
                {"name": "amount", "type_text": "decimal(10,2)", "type_name": "DECIMAL", "nullable": true}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let schema = test_client(&server)
        .describe_table(&sample_table_ref())
        .await
        .expect("describe_table happy path");

    assert_eq!(schema.columns.len(), 2);
    assert_eq!(schema.columns[0].name, "id");
    assert_eq!(schema.columns[0].type_str, "bigint");
    assert!(!schema.columns[0].nullable);
    assert_eq!(schema.columns[1].type_str, "decimal(10,2)");
    assert!(schema.columns[1].nullable);
}

#[tokio::test]
async fn describe_table_404_maps_to_table_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_string("table not found"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .describe_table(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::TableNotFound(_)),
        "expected TableNotFound, got {err:?}"
    );
}

#[tokio::test]
async fn describe_table_403_maps_to_permission_denied() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(403).set_body_string("forbidden"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .describe_table(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::PermissionDenied(_)));
}

// ---------------------------------------------------------------------------
// list_tables
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_tables_happy_path_single_page() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "hcv2_cat"))
        .and(query_param("schema_name", "hcv2_sch"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"name": "orders", "columns": []},
                {"name": "customers", "columns": []}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tables = test_client(&server)
        .list_tables(&["hcv2_cat".into(), "hcv2_sch".into()])
        .await
        .expect("list_tables happy path");
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[0].name, "orders");
    assert_eq!(tables[1].name, "customers");
    assert_eq!(tables[0].catalog.as_deref(), Some("hcv2_cat"));
    assert_eq!(tables[0].namespace, vec!["hcv2_sch".to_string()]);
}

#[tokio::test]
async fn list_tables_paginates_until_no_next_token() {
    let server = MockServer::start().await;

    // First page carries a token, second page is the terminator.
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "hcv2_cat"))
        .and(query_param("schema_name", "hcv2_sch"))
        .and(query_param("page_token", "tok-2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"name": "events", "columns": []}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "hcv2_cat"))
        .and(query_param("schema_name", "hcv2_sch"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"name": "orders", "columns": []}
            ],
            "next_page_token": "tok-2"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let tables = test_client(&server)
        .list_tables(&["hcv2_cat".into(), "hcv2_sch".into()])
        .await
        .expect("list_tables paginates");
    let names: Vec<String> = tables.into_iter().map(|t| t.name).collect();
    assert_eq!(names, vec!["orders", "events"]);
}

#[tokio::test]
async fn list_tables_404_maps_to_namespace_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "missing"))
        .and(query_param("schema_name", "missing"))
        .respond_with(ResponseTemplate::new(404).set_body_string("no such schema"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .list_tables(&["missing".into(), "missing".into()])
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::NamespaceNotFound(_)),
        "expected NamespaceNotFound (404 rewritten), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// create_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_table_posts_managed_delta_body() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(body_partial_json(json!({
            "name": "hcv2_orders",
            "catalog_name": "hcv2_cat",
            "schema_name": "hcv2_sch",
            "table_type": "MANAGED",
            "data_source_format": "DELTA"
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "hcv2_orders",
            "columns": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let schema = TableSchema {
        columns: vec![ColumnSchema {
            name: "id".into(),
            type_str: "bigint".into(),
            nullable: false,
        }],
    };
    test_client(&server)
        .create_table(&sample_table_ref(), &schema)
        .await
        .expect("create_table happy path");
}

#[tokio::test]
async fn create_table_404_maps_to_namespace_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("POST"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .respond_with(ResponseTemplate::new(404).set_body_string("schema does not exist"))
        .mount(&server)
        .await;

    let schema = TableSchema {
        columns: vec![ColumnSchema {
            name: "id".into(),
            type_str: "bigint".into(),
            nullable: false,
        }],
    };
    let err = test_client(&server)
        .create_table(&sample_table_ref(), &schema)
        .await
        .unwrap_err();
    assert!(
        matches!(err, CatalogError::NamespaceNotFound(_)),
        "expected NamespaceNotFound (404 rewritten on create), got {err:?}"
    );
}

// ---------------------------------------------------------------------------
// drop_table
// ---------------------------------------------------------------------------

#[tokio::test]
async fn drop_table_issues_delete() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    test_client(&server)
        .drop_table(&sample_table_ref())
        .await
        .expect("drop_table happy path");
}

#[tokio::test]
async fn drop_table_404_maps_to_table_not_found() {
    let server = MockServer::start().await;

    Mock::given(method("DELETE"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(404).set_body_string("no such table"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .drop_table(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::TableNotFound(_)));
}

// ---------------------------------------------------------------------------
// get_grants
// ---------------------------------------------------------------------------

#[tokio::test]
async fn get_grants_flattens_assignments_to_pairs() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/table/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "privilege_assignments": [
                {"principal": "engineers", "privileges": ["SELECT", "MODIFY"]},
                {"principal": "analysts", "privileges": ["SELECT"]}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let grants = test_client(&server)
        .get_grants(&sample_table_ref())
        .await
        .expect("get_grants happy path");

    // 2 + 1 privileges = 3 (principal, privilege) pairs.
    assert_eq!(grants.len(), 3);
    assert!(
        grants
            .iter()
            .any(|g| g.principal == "engineers" && g.privilege == "SELECT")
    );
    assert!(
        grants
            .iter()
            .any(|g| g.principal == "engineers" && g.privilege == "MODIFY")
    );
    assert!(
        grants
            .iter()
            .any(|g| g.principal == "analysts" && g.privilege == "SELECT")
    );
}

// ---------------------------------------------------------------------------
// apply_grant / revoke_grant
// ---------------------------------------------------------------------------

#[tokio::test]
async fn apply_grant_patches_with_add_list() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/table/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "engineers", "add": ["SELECT"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grant = Grant {
        principal: "engineers".into(),
        privilege: "SELECT".into(),
    };
    test_client(&server)
        .apply_grant(&sample_table_ref(), &grant)
        .await
        .expect("apply_grant happy path");
}

#[tokio::test]
async fn revoke_grant_patches_with_remove_list() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/table/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "old_user", "remove": ["MODIFY"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grant = Grant {
        principal: "old_user".into(),
        privilege: "MODIFY".into(),
    };
    test_client(&server)
        .revoke_grant(&sample_table_ref(), &grant)
        .await
        .expect("revoke_grant happy path");
}

// ---------------------------------------------------------------------------
// GovernanceCatalogClient — multi-change PATCH batching
// ---------------------------------------------------------------------------

#[tokio::test]
async fn governance_apply_grants_emits_one_patch_for_n_grants() {
    // The receipt: three grants across two principals on a schema
    // securable. The impl MUST collapse this into ONE PATCH carrying both
    // principals in `changes[]`. `.expect(1)` makes wiremock fail if the
    // impl loops one-grant-per-call (which would emit three PATCHes).
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/schema/hcv2_cat.hcv2_sch",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "alice", "add": ["SELECT", "MODIFY"]},
                {"principal": "bob", "add": ["SELECT"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grants = vec![
        Grant {
            principal: "alice".into(),
            privilege: "SELECT".into(),
        },
        Grant {
            principal: "alice".into(),
            privilege: "MODIFY".into(),
        },
        Grant {
            principal: "bob".into(),
            privilege: "SELECT".into(),
        },
    ];
    let securable = Securable::Schema {
        catalog: "hcv2_cat".into(),
        name: "hcv2_sch".into(),
    };
    test_client(&server)
        .apply_grants(&securable, &grants)
        .await
        .expect("multi-change apply_grants happy path");
}

#[tokio::test]
async fn governance_apply_grants_to_catalog_uses_catalog_endpoint() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path("/api/2.1/unity-catalog/permissions/catalog/hcv2_cat"))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "engineers", "add": ["USE_CATALOG"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grants = vec![Grant {
        principal: "engineers".into(),
        privilege: "USE_CATALOG".into(),
    }];
    let securable = Securable::Catalog {
        name: "hcv2_cat".into(),
    };
    test_client(&server)
        .apply_grants(&securable, &grants)
        .await
        .expect("catalog-scoped apply_grants happy path");
}

#[tokio::test]
async fn governance_apply_grants_to_table_uses_table_endpoint() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/table/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "alice", "add": ["SELECT"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grants = vec![Grant {
        principal: "alice".into(),
        privilege: "SELECT".into(),
    }];
    let securable = Securable::Table {
        catalog: "hcv2_cat".into(),
        schema: "hcv2_sch".into(),
        name: "hcv2_orders".into(),
    };
    test_client(&server)
        .apply_grants(&securable, &grants)
        .await
        .expect("table-scoped apply_grants happy path");
}

#[tokio::test]
async fn governance_revoke_grants_lands_in_remove_slot() {
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/schema/hcv2_cat.hcv2_sch",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "alice", "remove": ["SELECT", "MODIFY"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grants = vec![
        Grant {
            principal: "alice".into(),
            privilege: "SELECT".into(),
        },
        Grant {
            principal: "alice".into(),
            privilege: "MODIFY".into(),
        },
    ];
    let securable = Securable::Schema {
        catalog: "hcv2_cat".into(),
        name: "hcv2_sch".into(),
    };
    test_client(&server)
        .revoke_grants(&securable, &grants)
        .await
        .expect("multi-change revoke_grants happy path");
}

#[tokio::test]
async fn governance_apply_grants_empty_slice_makes_no_http_call() {
    // No mock mounted — empty input must short-circuit before any HTTP
    // request. wiremock rejects unexpected calls so this test fails
    // with a clear diagnostic if the impl ever loses its empty-slice
    // guard.
    let server = MockServer::start().await;
    let securable = Securable::Schema {
        catalog: "hcv2_cat".into(),
        name: "hcv2_sch".into(),
    };
    test_client(&server)
        .apply_grants(&securable, &[])
        .await
        .expect("empty apply_grants is a noop without any network call");
}

#[tokio::test]
async fn governance_list_grants_at_schema_flattens_assignments() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/schema/hcv2_cat.hcv2_sch",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "privilege_assignments": [
                {"principal": "engineers", "privileges": ["SELECT", "MODIFY"]},
                {"principal": "analysts", "privileges": ["SELECT"]}
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let securable = Securable::Schema {
        catalog: "hcv2_cat".into(),
        name: "hcv2_sch".into(),
    };
    let grants = test_client(&server)
        .list_grants(&securable)
        .await
        .expect("schema-scoped list_grants happy path");
    assert_eq!(grants.len(), 3);
    assert!(
        grants
            .iter()
            .any(|g| g.principal == "engineers" && g.privilege == "MODIFY")
    );
}

// ---------------------------------------------------------------------------
// Singular CatalogClient::apply_grant still works (both paths coexist)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn singular_apply_grant_on_catalog_client_still_works() {
    // The dual-path contract: introducing `GovernanceCatalogClient`
    // does NOT change `CatalogClient::apply_grant`. The table-scoped
    // singular path stays exactly as it was — same endpoint, same single-
    // entry `changes[]` body. This test pins that contract.
    let server = MockServer::start().await;

    Mock::given(method("PATCH"))
        .and(path(
            "/api/2.1/unity-catalog/permissions/table/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .and(body_partial_json(json!({
            "changes": [
                {"principal": "engineers", "add": ["SELECT"]}
            ]
        })))
        .respond_with(ResponseTemplate::new(200))
        .expect(1)
        .mount(&server)
        .await;

    let grant = Grant {
        principal: "engineers".into(),
        privilege: "SELECT".into(),
    };
    // Note: routing through the CatalogClient trait, not GovernanceCatalogClient.
    let client: &dyn CatalogClient = &test_client(&server);
    client
        .apply_grant(&sample_table_ref(), &grant)
        .await
        .expect("singular apply_grant unchanged by the new governance trait");
}

// ---------------------------------------------------------------------------
// Unsupported operations (no network)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tag_table_returns_unsupported_without_network() {
    let server = MockServer::start().await;
    // No mock is mounted — the call must short-circuit before any HTTP
    // request. If a future change accidentally introduces a network
    // call, wiremock will reject the request and the test will fail
    // with a clear diagnostic.
    let err = test_client(&server)
        .tag_table(&sample_table_ref(), "owner", "analytics")
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
}

#[tokio::test]
async fn list_branches_returns_unsupported_without_network() {
    let server = MockServer::start().await;
    let err = test_client(&server)
        .list_branches(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
}

#[tokio::test]
async fn commit_transaction_returns_unsupported_without_network() {
    let server = MockServer::start().await;
    let err = test_client(&server)
        .commit_transaction(&[])
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
}

// ---------------------------------------------------------------------------
// 5xx → Transport
// ---------------------------------------------------------------------------

#[tokio::test]
async fn describe_table_500_maps_to_transport() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .describe_table(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::Transport(_)));
}

// ---------------------------------------------------------------------------
// 401 → AuthFailed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn describe_table_401_maps_to_auth_failed() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .mount(&server)
        .await;

    let err = test_client(&server)
        .describe_table(&sample_table_ref())
        .await
        .unwrap_err();
    assert!(matches!(err, CatalogError::AuthFailed(_)));
}
