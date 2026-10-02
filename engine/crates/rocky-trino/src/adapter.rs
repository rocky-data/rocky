//! Trino warehouse adapter — implements `rocky_core::traits::WarehouseAdapter`.
//!
//! Surface: `dialect`, `execute_statement`, `execute_query`,
//! `describe_table`. Everything else falls back to the trait defaults:
//! `merge_into` errors via the dialect ("v0 doesn't support MERGE"),
//! `checksum_chunks` errors via the `row_hash_expr` default, governance /
//! batch / loader are absent.

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use rocky_core::traits::{
    AdapterError, AdapterResult, ObjectKind, QueryResult, SqlDialect, WarehouseAdapter,
};
use rocky_ir::{ColumnInfo, TableRef};

use crate::auth::TrinoAuth;
use crate::connector::{TrinoClient, TrinoClientConfig};
use crate::dialect::TrinoDialect;

/// Trino warehouse adapter.
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

fn promotion_queries(
    dialect: &dyn SqlDialect,
    table: &TableRef,
) -> AdapterResult<(String, String)> {
    let catalog = dialect.quote_identifier(&table.catalog.replace('"', "\"\""));
    let schema = rocky_core::sql_gen::string_literal(dialect, &table.schema);
    let name = rocky_core::sql_gen::string_literal(dialect, &table.table);
    Ok((
        format!(
            "SELECT schema_name FROM {catalog}.information_schema.schemata WHERE lower(schema_name) = lower({schema})"
        ),
        format!(
            "SELECT table_type FROM {catalog}.information_schema.tables WHERE lower(table_schema) = lower({schema}) AND lower(table_name) = lower({name})"
        ),
    ))
}

fn promotion_kind(result: &QueryResult) -> AdapterResult<Option<ObjectKind>> {
    match result.rows.as_slice() {
        [] => Ok(None),
        [row] => match row.first().and_then(|v| v.as_str()) {
            Some("BASE TABLE") => Ok(Some(ObjectKind::Table)),
            Some("VIEW") => Ok(Some(ObjectKind::View)),
            Some(other) => Err(AdapterError::msg(format!(
                "unknown promotion destination kind: {other}"
            ))),
            None => Err(AdapterError::msg(
                "promotion destination kind is missing or malformed",
            )),
        },
        _ => Err(AdapterError::msg(
            "promotion destination kind returned multiple rows",
        )),
    }
}

#[async_trait]
impl WarehouseAdapter for TrinoAdapter {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    /// Trino folds identifier case, so this is a dialect constant and needs
    /// no round trip (#1281).
    ///
    /// "Identifiers are not treated as case sensitive" —
    /// trino.io/docs/current/language/reserved.html
    async fn identifier_case_significance(
        &self,
    ) -> AdapterResult<rocky_core::traits::CaseSignificance> {
        Ok(rocky_core::traits::CaseSignificance::Insignificant)
    }

    fn classify_failure(&self, err: &AdapterError) -> rocky_core::failure_class::FailureClass {
        crate::connector::classify_trino_failure(err)
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

    async fn promotion_destination_kind(
        &self,
        table: &TableRef,
    ) -> AdapterResult<Option<ObjectKind>> {
        let (namespace_sql, kind_sql) = promotion_queries(&self.dialect, table)?;
        let namespace = self.execute_query(&namespace_sql).await?;
        if namespace.rows.is_empty() {
            return Err(AdapterError::msg(
                "promotion destination namespace does not exist",
            ));
        }
        if namespace.rows.len() != 1 || namespace.rows[0].first().and_then(|v| v.as_str()).is_none()
        {
            return Err(AdapterError::msg(
                "promotion destination namespace response is ambiguous",
            ));
        }
        promotion_kind(&self.execute_query(&kind_sql).await?)
    }

    /// Fetch `sql` results as a single Arrow `RecordBatch` via Trino's
    /// spooled-protocol Arrow encoding.
    ///
    /// Drives [`TrinoClient::execute_arrow`], which negotiates Arrow IPC
    /// segments through the `X-Trino-Client-Capabilities: SPOOLING` +
    /// `X-Trino-Spooled-Segments-Accept-Encoding: arrow+zstd,arrow`
    /// headers (the spec from upstream PR `trinodb/trino#26365`).
    ///
    /// # Version-gating
    ///
    /// Apache Arrow IPC is **not yet supported by any shipping Trino
    /// release** (current: 481, May 2026) — the upstream PR is closed
    /// stale and awaiting revival. When the coordinator falls back to
    /// inline JSON rows the call surfaces
    /// [`TrinoError::ArrowEncodingUnavailable`] (wrapped in
    /// [`AdapterError`]) rather than silently degrading. Once Arrow
    /// encoding lands upstream the negotiation wire is already in
    /// place — no further adapter changes needed.
    async fn fetch_arrow_batch(&self, sql: &str) -> AdapterResult<RecordBatch> {
        self.client
            .execute_arrow(sql)
            .await
            .map_err(AdapterError::new)
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
    fn promotion_probe_sql_and_kinds() {
        let table = TableRef {
            catalog: "iceberg".into(),
            schema: "Mixed'Case".into(),
            table: "Orders'24".into(),
        };
        let (namespace, kind) = promotion_queries(&TrinoDialect::new(), &table).unwrap();
        assert_eq!(
            namespace,
            "SELECT schema_name FROM \"iceberg\".information_schema.schemata WHERE lower(schema_name) = lower('Mixed''Case')"
        );
        assert_eq!(
            kind,
            "SELECT table_type FROM \"iceberg\".information_schema.tables WHERE lower(table_schema) = lower('Mixed''Case') AND lower(table_name) = lower('Orders''24')"
        );
        let quoted_catalog = TableRef {
            catalog: "iceberg\"archive".into(),
            ..table.clone()
        };
        assert!(
            promotion_queries(&TrinoDialect::new(), &quoted_catalog)
                .unwrap()
                .0
                .contains("\"iceberg\"\"archive\".information_schema.schemata")
        );
        let result = |value: &str| QueryResult {
            columns: vec!["table_type".into()],
            rows: vec![vec![serde_json::json!(value)]],
        };
        assert_eq!(
            promotion_kind(&result("BASE TABLE")).unwrap(),
            Some(ObjectKind::Table)
        );
        assert_eq!(
            promotion_kind(&result("VIEW")).unwrap(),
            Some(ObjectKind::View)
        );
        assert_eq!(
            promotion_kind(&QueryResult {
                columns: vec![],
                rows: vec![]
            })
            .unwrap(),
            None
        );
        assert!(
            promotion_kind(&result("OTHER"))
                .unwrap_err()
                .to_string()
                .contains("OTHER")
        );
    }

    #[test]
    fn is_not_experimental() {
        let cfg = TrinoClientConfig::new("http://localhost:8080");
        let auth = crate::test_helpers::test_basic_auth();
        let adapter = TrinoAdapter::new(cfg, auth);
        assert!(!adapter.is_experimental());
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
    async fn fetch_arrow_batch_surfaces_version_gate_when_server_falls_back_to_inline_rows() {
        // The shipping Trino release (481) doesn't advertise an Arrow
        // spooled encoding, so the coordinator ignores the negotiation
        // headers and ships inline-JSON rows. The adapter MUST surface
        // this clearly rather than silently route around the gap.
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/statement"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "20260523_version_gate",
                "data": [[1, "alice"]],
                "stats": {"state": "FINISHED"}
            })))
            .mount(&server)
            .await;

        let cfg =
            TrinoClientConfig::new(server.uri()).with_timeout(std::time::Duration::from_secs(5));
        let auth = crate::test_helpers::test_basic_auth();
        let adapter = TrinoAdapter::new(cfg, auth);
        let err = adapter
            .fetch_arrow_batch("SELECT id, name FROM users")
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Arrow"),
            "expected Arrow-encoding-related error, got: {err}"
        );
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
