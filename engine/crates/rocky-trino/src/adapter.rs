//! Trino warehouse adapter — implements `rocky_core::traits::WarehouseAdapter`.
//!
//! v0 surface: `dialect`, `execute_statement`, `execute_query`,
//! `describe_table`, `is_experimental` (returns `true`). Everything else
//! falls back to the trait defaults: `merge_into` errors via the dialect
//! ("v0 doesn't support MERGE"), `checksum_chunks` errors via the
//! `row_hash_expr` default, governance / batch / loader are absent.

use async_trait::async_trait;
use rocky_core::ir::{ColumnInfo, TableRef};
use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter};

use crate::auth::TrinoAuth;
use crate::connector::{TrinoClient, TrinoClientConfig};
use crate::dialect::TrinoDialect;

/// Trino warehouse adapter.
///
/// Marks itself as experimental — the registry's startup loop logs a
/// warning when this adapter is selected.
pub struct TrinoAdapter {
    client: TrinoClient,
    dialect: TrinoDialect,
}

impl std::fmt::Debug for TrinoAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrinoAdapter")
            .field("client", &self.client)
            .finish()
    }
}

impl TrinoAdapter {
    /// Construct a new adapter from explicit client + auth config.
    pub fn new(config: TrinoClientConfig, auth: TrinoAuth) -> Self {
        Self {
            client: TrinoClient::new(config, auth),
            dialect: TrinoDialect::new(),
        }
    }

    /// Read-only access to the underlying Trino client (used by
    /// out-of-tree extensions for raw `/v1/statement` access).
    pub fn client(&self) -> &TrinoClient {
        &self.client
    }
}

#[async_trait]
impl WarehouseAdapter for TrinoAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    fn is_experimental(&self) -> bool {
        // The Trino adapter is v0 / experimental — coverage is intentionally
        // narrow (no MERGE, no governance, no loader, no Docker-conformance
        // harness). The registry's startup loop reads this to surface a
        // warning to the operator.
        true
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.client
            .execute(sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn execute_query(&self, sql: &str) -> AdapterResult<QueryResult> {
        let out = self.client.execute(sql).await.map_err(AdapterError::new)?;
        let columns = out.columns.iter().map(|c| c.name.clone()).collect();
        Ok(QueryResult {
            columns,
            rows: out.rows,
        })
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        // Trino's `DESCRIBE <three-part>` returns one row per column with
        // (Column, Type, Extra, Comment). The adapter validates the
        // identifiers via the dialect, formats the reference, and parses
        // the first two columns of the response.
        let table_ref =
            self.dialect
                .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        let sql = self.dialect.describe_table_sql(&table_ref);
        let result = self.execute_query(&sql).await?;
        if result.rows.is_empty() {
            return Err(AdapterError::msg(format!(
                "table {} not found (Trino DESCRIBE returned 0 rows)",
                table.full_name()
            )));
        }
        let columns = result
            .rows
            .iter()
            .filter_map(|row| {
                if row.len() < 2 {
                    return None;
                }
                let name = row[0].as_str()?.to_string();
                let data_type = row[1].as_str()?.to_string();
                Some(ColumnInfo {
                    name,
                    data_type,
                    // Trino's DESCRIBE doesn't surface nullability —
                    // information_schema.columns does. v0 reports
                    // `nullable = true` so the drift planner errs on
                    // the side of widening rather than DropAndRecreate.
                    // A follow-up can wire information_schema for the
                    // strict answer.
                    nullable: true,
                })
            })
            .collect();
        Ok(columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_experimental_is_true() {
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let auth = crate::test_helpers::test_basic_auth();
        let adapter = TrinoAdapter::new(cfg, auth);
        assert!(adapter.is_experimental());
    }

    #[test]
    fn dialect_round_trips() {
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let auth = crate::test_helpers::test_basic_auth();
        let adapter = TrinoAdapter::new(cfg, auth);
        let r = adapter
            .dialect()
            .format_table_ref("iceberg", "raw", "orders")
            .unwrap();
        assert_eq!(r, "\"iceberg\".\"raw\".\"orders\"");
    }

    #[tokio::test]
    async fn describe_table_parses_describe_response() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "describe",
                "columns": [
                    {"name": "Column", "type": "varchar"},
                    {"name": "Type", "type": "varchar"},
                    {"name": "Extra", "type": "varchar"},
                    {"name": "Comment", "type": "varchar"}
                ],
                "data": [
                    ["id", "bigint", "", ""],
                    ["name", "varchar", "", ""]
                ],
                "stats": {"state": "FINISHED"}
            })))
            .mount(&server)
            .await;

        let cfg =
            TrinoClientConfig::new(server.uri()).with_timeout(std::time::Duration::from_secs(5));
        let auth = crate::test_helpers::test_basic_auth();
        let adapter = TrinoAdapter::new(cfg, auth);
        let table = TableRef {
            catalog: "iceberg".into(),
            schema: "raw".into(),
            table: "users".into(),
        };
        let cols = adapter.describe_table(&table).await.unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "id");
        assert_eq!(cols[0].data_type, "bigint");
        assert_eq!(cols[1].name, "name");
        assert_eq!(cols[1].data_type, "varchar");
    }
}
