//! Wire-level mock tests for `DatabricksWarehouseAdapter`'s optional
//! Unity REST delegation of `describe_table` and `list_tables`.
//!
//! The adapter exposes two read-side catalog operations on the
//! `WarehouseAdapter` trait. Without a wired `UnityCatalogClient` they go
//! through `DESCRIBE TABLE` / `information_schema` SQL via
//! `DatabricksConnector`. With a wired client they try Unity's REST
//! surface first (`GET /api/2.1/unity-catalog/tables[/{full_name}]`) and
//! fall back to SQL on `UnsupportedOperation` / `Transport` errors only.
//!
//! These tests pin the projection shape (REST `TableSchema` →
//! `Vec<ColumnInfo>`, REST `Vec<TableRef>` → `Vec<String>` lowercased) and
//! the fallback policy. Real SQL-vs-REST content parity is exercised by
//! the live `#[ignore]` tests in `integration.rs`.
//!
//! Requires the `test-support` cargo feature so
//! `UnityCatalogClient::with_base_url` is callable.

use std::time::Duration;

use rocky_core::config::RetryConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_databricks::adapter::DatabricksWarehouseAdapter;
use rocky_databricks::auth::{Auth, AuthConfig};
use rocky_databricks::connector::{ConnectorConfig, DatabricksConnector};
use rocky_databricks::unity_catalog_client::UnityCatalogClient;
use rocky_ir::TableRef;
use serde_json::json;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

fn pat_auth() -> Auth {
    Auth::from_config(AuthConfig {
        host: "test.databricks.com".into(),
        token: Some("test-token".into()),
        client_id: None,
        client_secret: None,
    })
    .expect("PAT auth is infallible")
}

fn sql_connector(server: &MockServer) -> DatabricksConnector {
    let config = ConnectorConfig {
        host: "test.databricks.com".into(),
        warehouse_id: "test-warehouse".into(),
        timeout: Duration::from_secs(30),
        retry: RetryConfig {
            max_retries: 0,
            ..Default::default()
        },
    };
    DatabricksConnector::new(config, pat_auth()).with_base_url(server.uri())
}

fn unity_client(server: &MockServer) -> UnityCatalogClient {
    UnityCatalogClient::new("test.databricks.com".into(), pat_auth()).with_base_url(server.uri())
}

fn sample_ir_ref() -> TableRef {
    TableRef {
        catalog: "hcv2_cat".into(),
        schema: "hcv2_sch".into(),
        table: "hcv2_orders".into(),
    }
}

// ---------------------------------------------------------------------------
// describe_table — delegation parity
// ---------------------------------------------------------------------------

/// REST and SQL paths return the same `Vec<ColumnInfo>` shape for the
/// same logical table. Mocks both endpoints and asserts the adapter's
/// output is field-equal regardless of which path was taken.
#[tokio::test]
async fn describe_table_rest_and_sql_paths_project_identically() {
    let server = MockServer::start().await;

    // REST path: GET /api/2.1/unity-catalog/tables/{full_name}
    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "name": "hcv2_orders",
            "columns": [
                {"name": "id", "type_text": "bigint", "type_name": "LONG", "nullable": false},
                {"name": "amount", "type_text": "decimal(10,2)", "type_name": "DECIMAL", "nullable": true}
            ]
        })))
        .mount(&server)
        .await;

    // SQL path: POST /api/2.0/sql/statements with DESCRIBE TABLE.
    // DESCRIBE TABLE always reports `nullable = true` per the existing
    // CatalogManager impl, so the SQL projection is permissive on
    // nullability. That difference is documented in `catalog.rs`.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "col_name", "type_name": "STRING", "position": 0},
                        {"name": "data_type", "type_name": "STRING", "position": 1},
                        {"name": "comment", "type_name": "STRING", "position": 2}
                    ]
                },
                "total_row_count": 2
            },
            "result": {
                "data_array": [
                    ["id", "bigint", null],
                    ["amount", "decimal(10,2)", null]
                ]
            }
        })))
        .mount(&server)
        .await;

    let table = sample_ir_ref();

    // REST-routed adapter.
    let rest_adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let rest_cols = rest_adapter
        .describe_table(&table)
        .await
        .expect("REST describe_table");

    // SQL-only adapter (no catalog client wired).
    let sql_adapter = DatabricksWarehouseAdapter::new(sql_connector(&server));
    let sql_cols = sql_adapter
        .describe_table(&table)
        .await
        .expect("SQL describe_table");

    // Field-by-field parity: column count, names, data types.
    assert_eq!(rest_cols.len(), sql_cols.len());
    for (r, s) in rest_cols.iter().zip(sql_cols.iter()) {
        assert_eq!(r.name, s.name, "column names diverge between REST and SQL");
        assert_eq!(r.data_type, s.data_type, "data types diverge");
    }
    // Names and types pinned to the fixture for one final sanity check.
    assert_eq!(rest_cols[0].name, "id");
    assert_eq!(rest_cols[0].data_type, "bigint");
    assert_eq!(rest_cols[1].name, "amount");
    assert_eq!(rest_cols[1].data_type, "decimal(10,2)");
}

/// REST path returns 401 → `CatalogError::AuthFailed` → propagated as
/// `AdapterError`. Must NOT silently fall through to SQL — a bad token
/// on the catalog client is a real configuration problem.
#[tokio::test]
async fn describe_table_rest_401_surfaces_as_error_no_sql_fallback() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .expect(1)
        .mount(&server)
        .await;

    // Mock the SQL path so that if (incorrectly) reached, we'd see a 0-row
    // success — making it easy to detect a regression where the adapter
    // silently fell through to SQL on auth failure.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "should-not-be-hit",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": []}, "total_row_count": 0},
            "result": {"data_array": []}
        })))
        .expect(0)
        .mount(&server)
        .await;

    let adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let err = adapter
        .describe_table(&sample_ir_ref())
        .await
        .expect_err("401 must surface, not fall through");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("auth") || msg.contains("401") || msg.contains("unauthorized"),
        "expected auth-flavored error, got: {err}"
    );
}

/// REST returns a 5xx → `CatalogError::Transport` → adapter falls
/// through to the SQL path which succeeds. This is the "REST blip"
/// graceful-degradation contract.
#[tokio::test]
async fn describe_table_rest_5xx_falls_back_to_sql() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path(
            "/api/2.1/unity-catalog/tables/hcv2_cat.hcv2_sch.hcv2_orders",
        ))
        .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [
                        {"name": "col_name", "type_name": "STRING", "position": 0},
                        {"name": "data_type", "type_name": "STRING", "position": 1},
                        {"name": "comment", "type_name": "STRING", "position": 2}
                    ]
                },
                "total_row_count": 1
            },
            "result": {
                "data_array": [["id", "bigint", null]]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let cols = adapter
        .describe_table(&sample_ir_ref())
        .await
        .expect("SQL path picks up after REST 5xx");
    assert_eq!(cols.len(), 1);
    assert_eq!(cols[0].name, "id");
    assert_eq!(cols[0].data_type, "bigint");
}

// ---------------------------------------------------------------------------
// list_tables — delegation parity
// ---------------------------------------------------------------------------

/// REST and SQL paths return the same `Vec<String>` set (lowercased) for
/// the same catalog/schema. Order is not asserted — both surfaces return
/// rows in catalog-defined order and the trait contract is about the set,
/// not the order.
#[tokio::test]
async fn list_tables_rest_and_sql_paths_lowercase_consistently() {
    let server = MockServer::start().await;

    // REST: returns mixed-case identifiers. The adapter must lowercase
    // them on both paths to honour the trait's "lowercase for
    // case-insensitive matching" contract.
    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "hcv2_cat"))
        .and(query_param("schema_name", "hcv2_sch"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "tables": [
                {"name": "ORDERS", "columns": []},
                {"name": "Customers", "columns": []}
            ]
        })))
        .mount(&server)
        .await;

    // SQL: information_schema.tables returns the same names — emit them
    // mixed-case so we exercise the lowercasing step.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [{"name": "table_name", "type_name": "STRING", "position": 0}]
                },
                "total_row_count": 2
            },
            "result": {
                "data_array": [["ORDERS"], ["Customers"]]
            }
        })))
        .mount(&server)
        .await;

    let rest_adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let mut rest = rest_adapter
        .list_tables("hcv2_cat", "hcv2_sch")
        .await
        .expect("REST list_tables");

    let sql_adapter = DatabricksWarehouseAdapter::new(sql_connector(&server));
    let mut sql = sql_adapter
        .list_tables("hcv2_cat", "hcv2_sch")
        .await
        .expect("SQL list_tables");

    rest.sort();
    sql.sort();
    assert_eq!(rest, sql, "REST and SQL must surface the same set");
    assert_eq!(rest, vec!["customers".to_string(), "orders".to_string()]);
}

/// REST 404 on `list_tables` → `CatalogError::NamespaceNotFound` →
/// surfaces as `AdapterError`. Must NOT fall through to SQL; a missing
/// schema is a deterministic answer.
#[tokio::test]
async fn list_tables_rest_404_surfaces_as_error_no_sql_fallback() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "missing_cat"))
        .and(query_param("schema_name", "missing_sch"))
        .respond_with(ResponseTemplate::new(404).set_body_string("no such schema"))
        .expect(1)
        .mount(&server)
        .await;

    // SQL path should NOT be hit — namespace-not-found is non-recoverable.
    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "should-not-be-hit",
            "status": {"state": "SUCCEEDED"},
            "manifest": {"schema": {"columns": []}, "total_row_count": 0},
            "result": {"data_array": []}
        })))
        .expect(0)
        .mount(&server)
        .await;

    let adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let err = adapter
        .list_tables("missing_cat", "missing_sch")
        .await
        .expect_err("404 must surface");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("not found") || msg.contains("404") || msg.contains("namespace"),
        "expected namespace-not-found error, got: {err}"
    );
}

/// REST 5xx on `list_tables` → fallback to SQL succeeds.
#[tokio::test]
async fn list_tables_rest_5xx_falls_back_to_sql() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/2.1/unity-catalog/tables"))
        .and(query_param("catalog_name", "hcv2_cat"))
        .and(query_param("schema_name", "hcv2_sch"))
        .respond_with(ResponseTemplate::new(500).set_body_string("server error"))
        .expect(1)
        .mount(&server)
        .await;

    Mock::given(method("POST"))
        .and(path("/api/2.0/sql/statements"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "statement_id": "stmt-1",
            "status": {"state": "SUCCEEDED"},
            "manifest": {
                "schema": {
                    "columns": [{"name": "table_name", "type_name": "STRING", "position": 0}]
                },
                "total_row_count": 1
            },
            "result": {
                "data_array": [["orders"]]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let adapter = DatabricksWarehouseAdapter::new(sql_connector(&server))
        .with_catalog_client(unity_client(&server));
    let tables = adapter
        .list_tables("hcv2_cat", "hcv2_sch")
        .await
        .expect("SQL fallback succeeds after REST 5xx");
    assert_eq!(tables, vec!["orders".to_string()]);
}
