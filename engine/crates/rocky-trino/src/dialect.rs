//! Trino SQL dialect implementation.
//!
//! Trino is ANSI SQL with extensions. The v0 implementation focuses on the
//! identifier-quoting, table-reference, and DDL/DML shapes Rocky's planner
//! emits today. MERGE, OAuth, and Kerberos auth land in follow-ups.

use std::sync::Arc;

use rocky_core::traits::{AdapterError, AdapterResult, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

/// Trino SQL dialect.
#[derive(Debug, Clone, Default)]
pub struct TrinoDialect;

impl TrinoDialect {
    /// Construct a fresh dialect instance.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl SqlDialect for TrinoDialect {
    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        // Trino is always three-part: <catalog>.<schema>.<table>. Each
        // component is double-quoted (Trino's standard identifier
        // quoting; backticks are not accepted).
        validation::validate_identifier(catalog).map_err(AdapterError::new)?;
        validation::validate_identifier(schema).map_err(AdapterError::new)?;
        validation::validate_identifier(table).map_err(AdapterError::new)?;
        Ok(format!("\"{catalog}\".\"{schema}\".\"{table}\""))
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        // Trino's `CREATE OR REPLACE TABLE ... AS` lands behind connector
        // capability flags (Iceberg supports it; Hive does not pre-414).
        // The portable form is `DROP TABLE IF EXISTS` + `CREATE TABLE AS`,
        // but the planner already issues a DROP via `drop_table_sql` for
        // full-refresh strategies, so a plain `CREATE TABLE AS` keeps the
        // dialect surface narrow. Connectors that don't support CTAS
        // surface a clear error from the coordinator.
        format!("CREATE TABLE {target} AS\n{select_sql}")
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        format!("INSERT INTO {target}\n{select_sql}")
    }

    fn merge_into(
        &self,
        _target: &str,
        _source_sql: &str,
        _keys: &[Arc<str>],
        _update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        // Trino MERGE landed in 414 (2023) but is connector-dependent
        // (Iceberg yes, Hive limited). v0 advertises `merge: false` via
        // the manifest so the planner rejects `strategy = "merge"` at
        // validate time — this method should never be reached for a
        // well-formed pipeline. Returning `not_supported` keeps the
        // error surface honest if the planner ever calls it.
        Err(AdapterError::msg(
            "MERGE not supported by the Trino adapter v0 — \
             configure `strategy = \"full_refresh\"` or \"incremental\" instead",
        ))
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        let base = match columns {
            ColumnSelection::All => "SELECT *".to_string(),
            ColumnSelection::Explicit(cols) => {
                for col in cols {
                    validation::validate_identifier(col).map_err(AdapterError::new)?;
                }
                format!("SELECT {}", cols.join(", "))
            }
        };
        if metadata.is_empty() {
            return Ok(base);
        }
        let meta_cols: Vec<String> = metadata
            .iter()
            .map(|m| format!("CAST({} AS {}) AS {}", m.value, m.data_type, m.name))
            .collect();
        Ok(format!("{base}, {}", meta_cols.join(", ")))
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        validation::validate_identifier(timestamp_col).map_err(AdapterError::new)?;
        // Trino accepts ANSI `TIMESTAMP '...'` literals with sub-second
        // precision. The 1970-01-01 sentinel keeps the WHERE permissive
        // on first runs and after `delete_watermark`.
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
        Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        // Trino implements `DESCRIBE <table>` natively, returning
        // (Column, Type, Extra, Comment). The adapter parses the first
        // two columns and infers `nullable = true` (Trino's catalogs
        // surface nullability via information_schema, not DESCRIBE).
        format!("DESCRIBE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // Trino catalogs are configured server-side as connector
        // instances; OSS Trino has no `CREATE CATALOG` SQL. Returning
        // None tells the planner to skip the create-catalog step;
        // `auto_create_catalogs = true` against this adapter trips the
        // capability check at validate time.
        None
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        let validate = || -> AdapterResult<String> {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!(
                "CREATE SCHEMA IF NOT EXISTS \"{catalog}\".\"{schema}\""
            ))
        };
        Some(validate())
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        // Trino: `TABLESAMPLE BERNOULLI(<percent>)`. SYSTEM is also
        // accepted but Bernoulli matches the per-row sampling semantics
        // Rocky's null-rate checks expect.
        Some(format!("TABLESAMPLE BERNOULLI ({percent})"))
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        // Trino's REST API runs each statement in its own transaction
        // (the `START TRANSACTION` / `COMMIT` flow requires sticky
        // sessions and connector support). The portable form is a
        // 2-statement DELETE + INSERT — the runtime executes them in
        // order and rolls back via `DROP TABLE` on failure for v0.
        // Iceberg-backed catalogs support true `INSERT OVERWRITE`;
        // wiring that up is a follow-up.
        Ok(vec![
            format!("DELETE FROM {target} WHERE {partition_filter}"),
            format!("INSERT INTO {target}\n{select_sql}"),
        ])
    }

    // `view_ddl` defaults to `CREATE OR REPLACE VIEW <target> AS …`,
    // which Trino accepts natively (connector-gated — Iceberg + Hive
    // both support it) so the default impl is correct here.
    //
    // `materialized_view_ddl` defaults to a fail-loud error. Trino has
    // materialized-view support through its `system` catalog for some
    // connectors, but the surface is heterogeneous and Rocky v0 hasn't
    // wired it up; falling back to the trait default keeps the error
    // surface honest.
    //
    // `dynamic_table_ddl` is Snowflake-specific; the default error is
    // the right behaviour for Trino.

    // `quote_identifier` defaults to `"name"` on the trait, which is
    // exactly Trino's quoting style — leaving the default intact.

    // `row_hash_expr` defaults to an error on the trait. Trino has
    // `xxhash64(varbinary)` and `to_hex(sha256(...))` but neither is
    // wired up in v0 — checksum-bisection diff against Trino is a
    // follow-up.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_table_ref_uses_double_quotes() {
        let d = TrinoDialect::new();
        let r = d.format_table_ref("iceberg", "raw", "orders").unwrap();
        assert_eq!(r, "\"iceberg\".\"raw\".\"orders\"");
    }

    #[test]
    fn format_table_ref_rejects_bad_identifier() {
        let d = TrinoDialect::new();
        assert!(d.format_table_ref("ice; DROP", "raw", "orders").is_err());
        assert!(d.format_table_ref("iceberg", "raw", "orders\"").is_err());
    }

    #[test]
    fn create_table_as_emits_ctas() {
        let d = TrinoDialect::new();
        let sql = d.create_table_as("\"c\".\"s\".\"t\"", "SELECT 1");
        assert!(sql.starts_with("CREATE TABLE"));
        assert!(sql.contains("AS\nSELECT 1"));
    }

    #[test]
    fn insert_into_keeps_select_intact() {
        let d = TrinoDialect::new();
        let sql = d.insert_into("\"c\".\"s\".\"t\"", "SELECT a FROM \"c\".\"s\".\"src\"");
        assert!(sql.starts_with("INSERT INTO"));
        assert!(sql.contains("SELECT a FROM \"c\".\"s\".\"src\""));
    }

    #[test]
    fn merge_into_returns_not_supported() {
        let d = TrinoDialect::new();
        let result = d.merge_into(
            "\"c\".\"s\".\"t\"",
            "SELECT 1",
            &[Arc::from("id")],
            &ColumnSelection::All,
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("MERGE"));
    }

    #[test]
    fn select_clause_explicit_columns() {
        let d = TrinoDialect::new();
        let sql = d
            .select_clause(
                &ColumnSelection::Explicit(vec!["id".into(), "name".into()]),
                &[],
            )
            .unwrap();
        assert_eq!(sql, "SELECT id, name");
    }

    #[test]
    fn select_clause_with_metadata_columns() {
        let d = TrinoDialect::new();
        let meta = vec![MetadataColumn {
            name: "_loaded_by".to_string(),
            data_type: "VARCHAR".to_string(),
            value: "NULL".to_string(),
        }];
        let sql = d.select_clause(&ColumnSelection::All, &meta).unwrap();
        assert!(sql.contains("CAST(NULL AS VARCHAR) AS _loaded_by"));
    }

    #[test]
    fn watermark_where_no_prior_uses_sentinel_literal() {
        let d = TrinoDialect::new();
        let sql = d.watermark_where("_synced_at", None).unwrap();
        assert_eq!(sql, "WHERE _synced_at > TIMESTAMP '1970-01-01 00:00:00'");
    }

    #[test]
    fn watermark_where_with_prior_substitutes_literal() {
        use chrono::TimeZone;
        let d = TrinoDialect::new();
        let prior = chrono::Utc.with_ymd_and_hms(2026, 4, 17, 9, 30, 0).unwrap();
        let sql = d.watermark_where("_synced_at", Some(&prior)).unwrap();
        // Source-side semantics: literal substitution, no correlated subquery.
        assert_eq!(sql, "WHERE _synced_at > TIMESTAMP '2026-04-17 09:30:00'");
    }

    #[test]
    fn watermark_where_rejects_bad_identifier() {
        let d = TrinoDialect::new();
        assert!(d.watermark_where("'; DROP", None).is_err());
    }

    #[test]
    fn describe_table_sql_uses_describe_keyword() {
        let d = TrinoDialect::new();
        assert_eq!(
            d.describe_table_sql("\"c\".\"s\".\"t\""),
            "DESCRIBE \"c\".\"s\".\"t\""
        );
    }

    #[test]
    fn drop_table_sql_is_idempotent() {
        let d = TrinoDialect::new();
        assert_eq!(
            d.drop_table_sql("\"c\".\"s\".\"t\""),
            "DROP TABLE IF EXISTS \"c\".\"s\".\"t\""
        );
    }

    #[test]
    fn create_catalog_returns_none() {
        let d = TrinoDialect::new();
        assert!(d.create_catalog_sql("iceberg").is_none());
    }

    #[test]
    fn create_schema_emits_three_part_form() {
        let d = TrinoDialect::new();
        let sql = d.create_schema_sql("iceberg", "raw").unwrap().unwrap();
        assert_eq!(sql, "CREATE SCHEMA IF NOT EXISTS \"iceberg\".\"raw\"");
    }

    #[test]
    fn tablesample_uses_bernoulli() {
        let d = TrinoDialect::new();
        assert_eq!(
            d.tablesample_clause(10).unwrap(),
            "TABLESAMPLE BERNOULLI (10)"
        );
    }

    #[test]
    fn view_ddl_emits_create_or_replace_view() {
        let d = TrinoDialect::new();
        let sql = d
            .view_ddl("\"c\".\"s\".\"v\"", "SELECT * FROM src")
            .unwrap();
        assert_eq!(
            sql,
            "CREATE OR REPLACE VIEW \"c\".\"s\".\"v\" AS\nSELECT * FROM src"
        );
    }

    #[test]
    fn materialized_view_ddl_unsupported_on_trino() {
        let d = TrinoDialect::new();
        let err = d
            .materialized_view_ddl("\"c\".\"s\".\"mv\"", "SELECT 1")
            .expect_err("Trino v0 doesn't wire MV");
        assert!(
            err.to_string().contains("MATERIALIZED VIEW"),
            "error message should mention MV unsupported: {err}"
        );
    }

    #[test]
    fn dynamic_table_ddl_unsupported_on_trino() {
        let d = TrinoDialect::new();
        let err = d
            .dynamic_table_ddl("\"c\".\"s\".\"dt\"", "SELECT 1", "1 minute", "wh")
            .expect_err("Trino has no dynamic-table concept");
        assert!(
            err.to_string().contains("DYNAMIC TABLE"),
            "error message should mention DT unsupported: {err}"
        );
    }

    #[test]
    fn insert_overwrite_partition_emits_delete_then_insert() {
        let d = TrinoDialect::new();
        let stmts = d
            .insert_overwrite_partition(
                "\"c\".\"s\".\"t\"",
                "ds >= DATE '2024-01-01'",
                "SELECT * FROM src",
            )
            .unwrap();
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("DELETE FROM"));
        assert!(stmts[1].starts_with("INSERT INTO"));
    }

    #[test]
    fn quote_identifier_uses_double_quotes_by_default() {
        // The trait's default `quote_identifier` is `"name"`, which
        // matches Trino. This test pins the default so a future
        // refactor that changes the trait default is caught here.
        let d = TrinoDialect::new();
        assert_eq!(d.quote_identifier("col"), "\"col\"");
    }
}
