//! Apache Spark SQL dialect.
//!
//! Spark SQL is the language Databricks SQL grew from, so most methods
//! delegate to [`DatabricksSqlDialect`]. Each method below is listed
//! explicitly — none falls through to a trait default by accident — and the
//! ones that differ say why:
//!
//! | Method | Spark |
//! |---|---|
//! | `create_catalog_sql` | `None`: a Spark catalog is server configuration (`spark.sql.catalog.<name>`), not DDL |
//! | `materialized_view_ddl` | refused: open-source Spark has no materialized views |
//! | `view_governance_probe_sql` | `None`: Unity Catalog tags and policies do not exist |
//! | `is_safe_type_widening` | always `false`: a column type change rebuilds the table (full refresh) |
//! | `pre_alter_column_type_sql` | `None`: follows from the line above |
//! | `insert_overwrite_partition` | Delta: one `INSERT INTO … REPLACE WHERE`; Iceberg: `DELETE` then `INSERT` |
//! | `snapshot_column_identifier` | backticks, as Databricks (the trait default picks `"x"` for any other dialect name) |
//! | `delete_partitions_sql` | `MERGE … WHEN MATCHED THEN DELETE`: open-source Delta refuses a subquery in `DELETE` |
//! | `supports_lakehouse_format_ddl`, `supports_delta_maintenance` | `false`: not verified on open-source Spark |
//!
//! Everything else (three-part names, backtick quoting, backslash string
//! escapes, `MERGE … UPDATE SET * / INSERT *`, `TABLESAMPLE`, `RLIKE`,
//! `xxhash64` row hashes, `STRING`, `* EXCEPT`) is the Databricks rendering,
//! which is plain Spark SQL.

use rocky_core::traits::{AdapterError, AdapterResult, LiteralEscape, SqlDialect};
use rocky_databricks::dialect::DatabricksSqlDialect;
use rocky_ir::{ColumnSelection, MetadataColumn, TableRef};

/// The table format Rocky's tables are created in.
///
/// The adapter sets it as the session's `spark.sql.sources.default`, so every
/// `CREATE TABLE` Rocky runs (models, snapshots, seeds, branches) makes a
/// table of this format. Both formats support `MERGE`, `DELETE` and
/// `CREATE OR REPLACE TABLE`, which Spark's built-in Parquet tables do not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TableFormat {
    /// Delta Lake (`USING delta`). The default.
    #[default]
    Delta,
    /// Apache Iceberg (`USING iceberg`).
    Iceberg,
}

impl TableFormat {
    /// Parse `extra.table_format`.
    ///
    /// # Errors
    ///
    /// Returns a message naming the accepted values.
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "delta" => Ok(Self::Delta),
            "iceberg" => Ok(Self::Iceberg),
            other => Err(format!(
                "extra.table_format '{other}' is not supported; use \"delta\" or \"iceberg\""
            )),
        }
    }

    /// The data source name Spark knows the format by.
    #[must_use]
    pub fn source_name(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
        }
    }
}

/// Apache Spark SQL dialect (Spark 4.0; see the module docs).
#[derive(Debug, Clone, Copy, Default)]
pub struct SparkDialect {
    format: TableFormat,
}

const INNER: DatabricksSqlDialect = DatabricksSqlDialect;

impl SparkDialect {
    /// The Delta Lake dialect.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// The Delta Lake dialect, usable in a `static`.
    #[must_use]
    pub const fn const_default() -> Self {
        Self {
            format: TableFormat::Delta,
        }
    }

    /// The dialect for a table format.
    #[must_use]
    pub const fn with_table_format(format: TableFormat) -> Self {
        Self { format }
    }

    /// The table format this dialect renders for.
    #[must_use]
    pub fn table_format(&self) -> TableFormat {
        self.format
    }
}

impl SqlDialect for SparkDialect {
    fn name(&self) -> &'static str {
        "spark"
    }

    /// Spark's default parser processes backslash escapes in `'…'`: a quote is
    /// `\'`, a backslash `\\`. Proven live by the conformance harness
    /// (`literal_escape_round_trips`).
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Backslash
    }

    fn supports_lakehouse_format_ddl(&self) -> bool {
        false
    }

    fn supports_delta_maintenance(&self) -> bool {
        false
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        INNER.format_table_ref(catalog, schema, table)
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        INNER.create_table_as(target, select_sql)
    }

    fn insert_into(&self, target: &str, select_sql: &str) -> String {
        INNER.insert_into(target, select_sql)
    }

    fn merge_into(
        &self,
        target: &str,
        source_sql: &str,
        keys: &[std::sync::Arc<str>],
        update_cols: &ColumnSelection,
    ) -> AdapterResult<String> {
        INNER.merge_into(target, source_sql, keys, update_cols)
    }

    fn select_clause(
        &self,
        columns: &ColumnSelection,
        metadata: &[MetadataColumn],
    ) -> AdapterResult<String> {
        INNER.select_clause(columns, metadata)
    }

    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        INNER.watermark_where(timestamp_col, last_watermark)
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        INNER.describe_table_sql(table_ref)
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        INNER.drop_table_sql(table_ref)
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        None
    }

    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        INNER.create_schema_sql(catalog, schema)
    }

    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        match self.format {
            // Delta Lake's `INSERT INTO … REPLACE WHERE` is one atomic commit.
            TableFormat::Delta => {
                INNER.insert_overwrite_partition(target, partition_filter, select_sql)
            }
            // Iceberg has no `REPLACE WHERE`, and Spark has no multi-statement
            // transactions: the DELETE commits before the INSERT runs. A
            // failed INSERT leaves the window empty until the next run
            // rewrites it.
            TableFormat::Iceberg => Ok(vec![
                format!("DELETE FROM {target} WHERE {partition_filter}"),
                format!("INSERT INTO {target}\n{select_sql}"),
            ]),
        }
    }

    /// `MERGE … WHEN MATCHED THEN DELETE` in place of the default
    /// `DELETE … WHERE (cols) IN (SELECT …)`: open-source Delta Lake refuses a
    /// subquery in a `DELETE` condition (`DELTA_UNSUPPORTED_SUBQUERY`). The
    /// rows matched are the same: equality on every partition column, and a
    /// NULL partition value matches nothing under either form. `DISTINCT`
    /// keeps one source row per partition, so no target row matches twice.
    fn delete_partitions_sql(
        &self,
        target: &str,
        partition_cols: &[std::sync::Arc<str>],
        source_sql: &str,
    ) -> String {
        let cols = partition_cols.join(", ");
        let on = partition_cols
            .iter()
            .map(|c| format!("t.{c} = s.{c}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        format!(
            "MERGE INTO {target} AS t\n\
             USING (SELECT DISTINCT {cols} FROM ({source_sql}) AS _rocky_incoming) AS s\n\
             ON {on}\n\
             WHEN MATCHED THEN DELETE"
        )
    }

    /// Backticks, as Databricks. The trait default picks its quote by
    /// dialect name and would give `"x"`, which Spark parses as a string
    /// literal.
    fn snapshot_column_identifier(&self, name: &str) -> String {
        INNER.snapshot_column_identifier(name)
    }

    fn tablesample_clause(&self, percent: u32) -> Option<String> {
        INNER.tablesample_clause(percent)
    }

    fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
        INNER.regex_match_predicate(column, pattern)
    }

    fn quote_identifier(&self, name: &str) -> String {
        INNER.quote_identifier(name)
    }

    /// `* EXCEPT (a, b)` — Spark 4.0 and later.
    fn star_excluding(&self, columns: &[&str]) -> Option<String> {
        INNER.star_excluding(columns)
    }

    fn string_type_name(&self) -> &'static str {
        INNER.string_type_name()
    }

    fn materialized_view_ddl(&self, _target: &str, _select_sql: &str) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "materialized views are not supported on spark: open-source Spark has no \
             materialized view; use strategy = \"full_refresh\" or \"view\"",
        ))
    }

    fn view_governance_probe_sql(&self, _view: &TableRef) -> Option<AdapterResult<String>> {
        None
    }

    fn row_hash_expr(&self, columns: &[String]) -> AdapterResult<String> {
        INNER.row_hash_expr(columns)
    }

    /// No in-place type change is classified safe. Delta's type widening is a
    /// table feature Rocky has not verified on open-source Delta, and an
    /// unsafe `ALTER COLUMN … TYPE` loses data, while a full refresh only
    /// costs time.
    fn is_safe_type_widening(&self, _source_type: &str, _target_type: &str) -> bool {
        false
    }

    fn pre_alter_column_type_sql(&self, _table_ref: &str) -> Option<AdapterResult<String>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d() -> SparkDialect {
        SparkDialect::new()
    }

    #[test]
    fn table_refs_are_three_part_and_validated() {
        assert_eq!(
            d().format_table_ref("spark_catalog", "silver", "orders")
                .unwrap(),
            "spark_catalog.silver.orders"
        );
        assert!(d().format_table_ref("c", "s", "t; DROP TABLE x").is_err());
        assert!(d().format_table_ref("c", "s", "").is_err());
    }

    #[test]
    fn full_refresh_view_and_append_render_like_databricks() {
        assert_eq!(
            d().create_table_as("c.s.t", "SELECT 1"),
            "CREATE OR REPLACE TABLE c.s.t AS\nSELECT 1"
        );
        assert_eq!(
            d().view_ddl("c.s.v", "SELECT 1").unwrap(),
            "CREATE OR REPLACE VIEW c.s.v AS\nSELECT 1"
        );
        assert_eq!(
            d().insert_into("c.s.t", "SELECT 1"),
            "INSERT INTO c.s.t\nSELECT 1"
        );
        assert_eq!(d().drop_table_sql("c.s.t"), "DROP TABLE IF EXISTS c.s.t");
        assert_eq!(d().describe_table_sql("c.s.t"), "DESCRIBE TABLE c.s.t");
    }

    #[test]
    fn merge_uses_delta_star_forms_and_refuses_bad_keys() {
        let sql = d()
            .merge_into(
                "c.s.t",
                "SELECT * FROM src",
                &["id".into()],
                &ColumnSelection::All,
            )
            .unwrap();
        assert_eq!(
            sql,
            "MERGE INTO c.s.t AS t\nUSING (\nSELECT * FROM src\n) AS s\nON t.id = s.id\n\
             WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
        );
        let explicit = d()
            .merge_into(
                "c.s.t",
                "SELECT 1",
                &["id".into(), "day".into()],
                &ColumnSelection::Explicit(vec!["amount".into()]),
            )
            .unwrap();
        assert!(
            explicit.contains("ON t.id = s.id AND t.day = s.day"),
            "{explicit}"
        );
        assert!(
            explicit.contains("UPDATE SET t.amount = s.amount"),
            "{explicit}"
        );
        assert!(
            d().merge_into("c.s.t", "SELECT 1", &[], &ColumnSelection::All)
                .is_err()
        );
        assert!(
            d().merge_into(
                "c.s.t",
                "SELECT 1",
                &["id; --".into()],
                &ColumnSelection::All
            )
            .is_err()
        );
        assert_eq!(d().merge_unsupported_reason(), None);
    }

    #[test]
    fn time_interval_is_one_replace_where_on_delta_and_delete_insert_on_iceberg() {
        assert_eq!(
            d().insert_overwrite_partition("c.s.t", "d >= '2024-01-01'", "SELECT 1")
                .unwrap(),
            vec!["INSERT INTO c.s.t REPLACE WHERE d >= '2024-01-01'\nSELECT 1".to_string()]
        );
        let ice = SparkDialect::with_table_format(TableFormat::Iceberg);
        assert_eq!(
            ice.insert_overwrite_partition("c.s.t", "d >= '2024-01-01'", "SELECT 1")
                .unwrap(),
            vec![
                "DELETE FROM c.s.t WHERE d >= '2024-01-01'".to_string(),
                "INSERT INTO c.s.t\nSELECT 1".to_string(),
            ]
        );
    }

    #[test]
    fn delete_insert_deletes_through_merge_not_a_subquery() {
        let sql = d().delete_partitions_sql(
            "c.s.t",
            &["day".into(), "region".into()],
            "SELECT * FROM src",
        );
        assert_eq!(
            sql,
            "MERGE INTO c.s.t AS t\n\
             USING (SELECT DISTINCT day, region FROM (SELECT * FROM src) AS _rocky_incoming) AS s\n\
             ON t.day = s.day AND t.region = s.region\n\
             WHEN MATCHED THEN DELETE"
        );
        assert!(!sql.contains(" IN ("), "{sql}");
        // Two statements: the delete commits, then the insert runs.
        assert_eq!(
            d().delete_insert_statements("D".into(), "I".into()),
            vec!["D".to_string(), "I".to_string()]
        );
    }

    #[test]
    fn unity_catalog_and_unverified_features_are_off() {
        assert!(d().create_catalog_sql("c").is_none());
        assert_eq!(
            d().create_schema_sql("c", "s").unwrap().unwrap(),
            "CREATE SCHEMA IF NOT EXISTS c.s"
        );
        let mv = d().materialized_view_ddl("c.s.t", "SELECT 1").unwrap_err();
        assert!(mv.to_string().contains("not supported on spark"), "{mv}");
        let view = TableRef {
            catalog: "c".into(),
            schema: "s".into(),
            table: "v".into(),
        };
        assert!(d().view_governance_probe_sql(&view).is_none());
        assert!(!d().supports_lakehouse_format_ddl());
        assert!(!d().supports_delta_maintenance());
        assert!(d().pre_alter_column_type_sql("c.s.t").is_none());
    }

    #[test]
    fn no_type_change_is_classified_safe() {
        // Databricks allows these in place; Spark always rebuilds.
        for (from, to) in [
            ("BIGINT", "INT"),
            ("DOUBLE", "FLOAT"),
            ("DECIMAL(12,2)", "DECIMAL(10,2)"),
        ] {
            assert!(DatabricksSqlDialect.is_safe_type_widening(from, to));
            assert!(!d().is_safe_type_widening(from, to), "{to} -> {from}");
        }
    }

    #[test]
    fn literals_identifiers_and_expressions() {
        assert_eq!(d().literal_escape(), LiteralEscape::Backslash);
        assert_eq!(
            rocky_core::sql_gen::string_literal(&d(), "it's a \\ path"),
            "'it\\'s a \\\\ path'"
        );
        assert_eq!(d().quote_identifier("order"), "`order`");
        assert_eq!(d().string_type_name(), "STRING");
        assert_eq!(
            d().star_excluding(&["a", "b"]).as_deref(),
            Some("* EXCEPT (a, b)")
        );
        assert_eq!(
            d().tablesample_clause(10).as_deref(),
            Some("TABLESAMPLE (10 PERCENT)")
        );
        assert_eq!(
            d().regex_match_predicate("c", "^a").unwrap(),
            "c RLIKE '^a'"
        );
        assert_eq!(
            d().row_hash_expr(&["a".into()]).unwrap(),
            "xxhash64(`a`, isnull(`a`))"
        );
        assert_eq!(
            d().watermark_where("ts", None).unwrap(),
            "WHERE ts > TIMESTAMP '1970-01-01 00:00:00'"
        );
    }

    #[test]
    fn select_clause_refuses_hostile_metadata() {
        let hostile = MetadataColumn::new_unchecked("x", "STRING", "1; DROP TABLE t");
        assert!(
            d().select_clause(&ColumnSelection::All, &[hostile])
                .is_err()
        );
        assert_eq!(
            d().select_clause(&ColumnSelection::Explicit(vec!["a".into()]), &[])
                .unwrap(),
            "SELECT a"
        );
    }

    #[test]
    fn snapshot_identifiers_use_backticks_not_double_quotes() {
        assert_eq!(d().snapshot_column_identifier("valid_from"), "`valid_from`");
        assert_eq!(d().snapshot_column_identifier("a`b"), "`a``b`");
        assert_eq!(
            d().snapshot_metadata_identifier("is_current"),
            "`is_current`"
        );
        let (names, values) = d()
            .snapshot_insert_columns(&["id".into()], &[("valid_to", "NULL")])
            .unwrap();
        assert_eq!(names, ["`id`", "`valid_to`"]);
        assert_eq!(values, ["source.`id`", "NULL"]);
    }

    #[test]
    fn table_format_parses_case_insensitively() {
        assert_eq!(TableFormat::parse("Delta").unwrap(), TableFormat::Delta);
        assert_eq!(
            TableFormat::parse(" iceberg ").unwrap(),
            TableFormat::Iceberg
        );
        assert!(
            TableFormat::parse("parquet")
                .unwrap_err()
                .contains("\"delta\"")
        );
        assert_eq!(TableFormat::default().source_name(), "delta");
    }
}
