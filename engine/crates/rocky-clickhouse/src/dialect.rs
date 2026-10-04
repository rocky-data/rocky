//! ClickHouse SQL dialect.
//!
//! | Concern | ClickHouse rendering |
//! |---|---|
//! | Names | `database.table`, bare. ClickHouse has no catalog level: a non-empty catalog is refused. Names are case-sensitive. |
//! | String literals | backslash escapes (`\'`, `\\`) |
//! | Full refresh | `CREATE OR REPLACE TABLE … ENGINE = MergeTree ORDER BY tuple() AS …`: one statement that builds the new table and swaps it in atomically (an `EXCHANGE TABLES` under the server's Atomic database engine); a failed `SELECT` leaves the old table in place |
//! | Table attributes | `[clickhouse]` `engine` / `partition_by` / `order_by` between the name and `AS` |
//! | `merge` | refused (E053): ClickHouse has no `MERGE` statement |
//! | Snapshots | refused (E049): they need `MERGE` |
//! | Materialized views | refused: a ClickHouse materialized view is an insert trigger, not a stored query result |
//! | `delete_insert` | lightweight `DELETE`, then `INSERT` (two statements, not atomic) |
//! | `time_interval` | stage the window, lightweight `DELETE`, copy the stage in (not atomic; see [`ClickHouseDialect::insert_overwrite_partition`]) |
//! | Schema | `CREATE DATABASE IF NOT EXISTS` (a Rocky schema is a ClickHouse database) |
//! | Type widening | none: every type change rebuilds the table (see [`ClickHouseDialect::is_safe_type_widening`]) |
//!
//! **No transactions.** ClickHouse commits every statement on its own, so a
//! write Rocky needs several statements for can stop half way. Each such
//! path is ordered so the model's own `SELECT` fails before anything is
//! deleted, and re-running the model (or the partition) repairs the rest.
//!
//! **Identifiers render bare.** Every name is validated against
//! `^[A-Za-z_][A-Za-z0-9_]*$` first, ClickHouse's own rule for an unquoted
//! identifier, so bare rendering is injection-safe and names exactly the
//! object `CREATE` made.

use std::sync::Arc;

use rocky_core::traits::{AdapterError, AdapterResult, LiteralEscape, SqlDialect};
use rocky_ir::{ColumnSelection, MetadataColumn};
use rocky_sql::validation;

const CH: &str = "clickhouse";

/// The default table attributes: a MergeTree with no sorting key.
const DEFAULT_TABLE_ATTRS: &str = "ENGINE = MergeTree ORDER BY tuple()";

/// Why `merge` cannot run on ClickHouse. Reported as E053 by `rocky compile`
/// and returned by [`ClickHouseDialect::merge_into`].
pub const MERGE_UNSUPPORTED: &str = "ClickHouse has no MERGE statement or transactional upsert, \
     so Rocky cannot update rows by key (strategy `merge`, or `incremental` with a `unique_key`). \
     A ReplacingMergeTree only removes duplicate keys during background merges, so reads would \
     see both versions until then";

/// ClickHouse dialect.
#[derive(Debug, Clone, Copy, Default)]
pub struct ClickHouseDialect;

impl ClickHouseDialect {
    /// The dialect.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

/// ClickHouse's unquoted-identifier rule. Stricter than
/// [`validation::validate_identifier`], which also admits a leading digit
/// (`1abc` would lex as a number here).
fn check_identifier(name: &str) -> AdapterResult<()> {
    validation::validate_identifier(name).map_err(AdapterError::new)?;
    if name.starts_with(|c: char| c.is_ascii_digit()) {
        return Err(AdapterError::msg(format!(
            "identifier '{name}' starts with a digit; ClickHouse needs a letter or `_` first \
             for an unquoted name"
        )));
    }
    Ok(())
}

fn check_no_catalog(catalog: &str) -> AdapterResult<()> {
    if catalog.is_empty() {
        return Ok(());
    }
    Err(AdapterError::msg(format!(
        "catalog '{catalog}' cannot be used on clickhouse: ClickHouse names a table \
         `database.table`, and Rocky's schema is the ClickHouse database. Set the target's \
         `catalog = \"\"`"
    )))
}

fn table_head(target: &str, replace: bool, attrs: &str) -> String {
    if replace {
        format!("CREATE OR REPLACE TABLE {target} {attrs} AS")
    } else {
        format!("CREATE TABLE {target} {attrs} AS")
    }
}

/// The staging table a `time_interval` overwrite fills first, one per
/// window: `rocky run` writes several partitions of a model concurrently, so
/// a shared name would collide. The suffix is an FNV-1a hash of the window's
/// filter — stable across runs, so a re-run of a failed window drops the
/// stage that run left behind.
fn stage_table(target: &str, partition_filter: &str) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in partition_filter.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{target}__rocky_stage_{hash:016x}")
}

/// Rewrite every `'YYYY-MM-DD HH:MM:SS'` literal in a Rocky-built partition
/// filter to `toDateTime('…')`. A bare string compared with a `Date` column
/// is parsed AS a date, and ClickHouse refuses a string with a time part
/// ("Cannot convert string … to type Date"); a `DateTime` compares with a
/// `Date`, `DateTime` and `DateTime64` column alike. The session time zone
/// is UTC (see the connector), so the window's instants are unchanged.
fn datetime_literals(filter: &str) -> String {
    let bytes = filter.as_bytes();
    let mut out = String::with_capacity(filter.len() + 32);
    let mut i = 0;
    while i < filter.len() {
        if bytes[i] == b'\''
            && let Some(lit) = filter.get(i + 1..i + 20)
            && filter.as_bytes().get(i + 20) == Some(&b'\'')
            && is_datetime_text(lit)
        {
            out.push_str(&format!("toDateTime('{lit}')"));
            i += 21;
            continue;
        }
        let ch = filter[i..].chars().next().expect("in bounds");
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn is_datetime_text(s: &str) -> bool {
    s.len() == 19
        && s.char_indices().all(|(i, c)| match i {
            4 | 7 => c == '-',
            10 => c == ' ',
            13 | 16 => c == ':',
            _ => c.is_ascii_digit(),
        })
}

impl SqlDialect for ClickHouseDialect {
    fn name(&self) -> &'static str {
        CH
    }

    /// Backslash: ClickHouse reads `\'`, `\\`, `\n` … inside a string
    /// literal ("Syntax → String": "the escape sequences … `\\`, `\'`, …").
    /// Proven by the live round trip in `tests/live_clickhouse.rs`
    /// (`literal_escape_round_trips`).
    fn literal_escape(&self) -> LiteralEscape {
        LiteralEscape::Backslash
    }

    /// ClickHouse quoted identifiers (`"…"` and `` `…` ``) take the same
    /// backslash escapes as string literals.
    fn identifier_takes_backslash_escapes(&self) -> bool {
        true
    }

    fn format_table_ref(&self, catalog: &str, schema: &str, table: &str) -> AdapterResult<String> {
        check_no_catalog(catalog)?;
        check_identifier(schema)?;
        check_identifier(table)?;
        Ok(format!("{schema}.{table}"))
    }

    fn create_table_as(&self, target: &str, select_sql: &str) -> String {
        format!(
            "{}\n{select_sql}",
            table_head(target, true, DEFAULT_TABLE_ATTRS)
        )
    }

    fn create_table_as_new(&self, target: &str, select_sql: &str) -> String {
        format!(
            "{}\n{select_sql}",
            table_head(target, false, DEFAULT_TABLE_ATTRS)
        )
    }

    /// `CREATE [OR REPLACE] TABLE t ENGINE = … PARTITION BY … ORDER BY …
    /// AS …`. Invalid options are refused with the same messages
    /// `rocky compile` reports as E053.
    fn create_table_as_with_clickhouse_options(
        &self,
        target: &str,
        select_sql: &str,
        options: &rocky_ir::ClickHouseTableOptions,
        replace: bool,
    ) -> AdapterResult<String> {
        let attrs = options.to_sql().map_err(AdapterError::msg)?;
        Ok(format!(
            "{}\n{select_sql}",
            table_head(target, replace, &attrs)
        ))
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
        Err(AdapterError::msg(format!("{MERGE_UNSUPPORTED} (E053)")))
    }

    fn merge_unsupported_reason(&self) -> Option<&'static str> {
        Some(MERGE_UNSUPPORTED)
    }

    fn snapshot_unsupported_reason(&self) -> Option<&'static str> {
        Some("snapshots use MERGE, which ClickHouse does not have")
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
                    check_identifier(col)?;
                }
                format!("SELECT {}", cols.join(", "))
            }
        };
        if metadata.is_empty() {
            return Ok(base);
        }
        // All three fields are interpolated raw into the CAST, so all three
        // are validated; `value` is an expression and NOT trusted (see the
        // PostgreSQL dialect for the full reasoning).
        let mut meta_cols = Vec::with_capacity(metadata.len());
        for m in metadata {
            check_identifier(m.name())?;
            rocky_core::sql_gen::validate_sql_type(m.data_type()).map_err(AdapterError::new)?;
            validation::reject_statement_terminator("metadata_columns[].value", m.value())
                .map_err(AdapterError::new)?;
            meta_cols.push(format!(
                "CAST({} AS {}) AS {}",
                m.value(),
                m.data_type(),
                m.name()
            ));
        }
        Ok(format!("{base}, {}", meta_cols.join(", ")))
    }

    /// `WHERE col > toDateTime64('…', 6, 'UTC')`: an explicit UTC instant
    /// with microseconds, so the comparison holds for a `DateTime`,
    /// `DateTime64` or `Date` column whatever its own time zone, and the row
    /// the watermark was read from does not re-pass (#2004).
    fn watermark_where(
        &self,
        timestamp_col: &str,
        last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
    ) -> AdapterResult<String> {
        check_identifier(timestamp_col)?;
        let literal = last_watermark
            .map(|t| t.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| "1970-01-01 00:00:00.000000".to_string());
        Ok(format!(
            "WHERE {timestamp_col} > toDateTime64('{literal}', 6, 'UTC')"
        ))
    }

    fn describe_table_sql(&self, table_ref: &str) -> String {
        format!("DESCRIBE TABLE {table_ref}")
    }

    fn drop_table_sql(&self, table_ref: &str) -> String {
        format!("DROP TABLE IF EXISTS {table_ref}")
    }

    /// Refused. A ClickHouse `MATERIALIZED VIEW` is an insert trigger: it
    /// transforms only the rows inserted into its source table after it was
    /// created, and never recomputes. That is not the stored, refreshed
    /// query result the `materialized_view` strategy promises elsewhere.
    fn materialized_view_ddl(&self, _target: &str, _select_sql: &str) -> AdapterResult<String> {
        Err(AdapterError::msg(
            "MATERIALIZED VIEW strategy is not supported on clickhouse: a ClickHouse materialized \
             view is an insert trigger over new rows, not a refreshed query result. Use \
             `full_refresh` (an atomic CREATE OR REPLACE TABLE) or `view`",
        ))
    }

    fn create_catalog_sql(&self, _name: &str) -> Option<AdapterResult<String>> {
        // No catalog level.
        None
    }

    /// A Rocky schema is a ClickHouse database.
    fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
        Some(
            check_no_catalog(catalog)
                .and_then(|()| check_identifier(schema))
                .map(|()| format!("CREATE DATABASE IF NOT EXISTS {schema}")),
        )
    }

    /// `SAMPLE` needs a `SAMPLE BY` key declared on the table, which Rocky's
    /// tables do not have; the null-rate check reads every row instead.
    fn tablesample_clause(&self, _percent: u32) -> Option<String> {
        None
    }

    /// Replace one time window. ClickHouse has no transaction to wrap a
    /// DELETE and an INSERT in, so the window is written in an order that
    /// keeps the target whole whenever the model's own `SELECT` fails:
    ///
    /// 1. drop a leftover staging table for this window, then create an
    ///    empty one with the target's structure and engine (`CREATE TABLE
    ///    stage AS target`);
    /// 2. run the model into the staging table — a failing `SELECT` stops
    ///    here, before the target is touched;
    /// 3. delete the window from the target (lightweight `DELETE`);
    /// 4. copy the staging table into the target, then drop it.
    ///
    /// A failure between 3 and 4 leaves the window empty until the
    /// partition is re-run, which rebuilds it from scratch. Readers can see
    /// the window empty between 3 and 4.
    fn insert_overwrite_partition(
        &self,
        target: &str,
        partition_filter: &str,
        select_sql: &str,
    ) -> AdapterResult<Vec<String>> {
        let stage = stage_table(target, partition_filter);
        Ok(vec![
            format!("DROP TABLE IF EXISTS {stage}"),
            format!("CREATE TABLE {stage} AS {target}"),
            format!("INSERT INTO {stage}\n{select_sql}"),
            format!(
                "DELETE FROM {target} WHERE {}",
                datetime_literals(partition_filter)
            ),
            format!("INSERT INTO {target} SELECT * FROM {stage}"),
            format!("DROP TABLE IF EXISTS {stage}"),
        ])
    }

    fn list_tables_sql(&self, catalog: &str, schema: &str) -> AdapterResult<String> {
        check_no_catalog(catalog)?;
        check_identifier(schema)?;
        Ok(format!(
            "SELECT name AS table_name FROM system.tables WHERE database = {} \
             AND NOT is_temporary",
            rocky_core::sql_gen::string_literal(self, schema)
        ))
    }

    fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
        Ok(format!(
            "match({column}, {})",
            rocky_core::sql_gen::string_literal(self, pattern)
        ))
    }

    fn date_minus_days_expr(&self, days: u32) -> AdapterResult<String> {
        Ok(format!("subtractDays(today(), {days})"))
    }

    /// The dbt-utils surrogate key, with ClickHouse's hashing spelled out:
    /// `MD5` returns 16 raw bytes, so `lower(hex(…))` turns it into the same
    /// 32-character hex digest every other warehouse's `md5` returns.
    fn surrogate_key_expr(&self, columns: &[&str]) -> String {
        let str_type = self.string_type_name();
        let fields: Vec<String> = columns
            .iter()
            .map(|c| format!("coalesce(cast({c} as {str_type}), '_dbt_utils_surrogate_key_null_')"))
            .collect();
        let concatenated = if fields.is_empty() {
            "''".to_string()
        } else {
            fields.join(" || '-' || ")
        };
        format!("lower(hex(MD5(cast({concatenated} as {str_type}))))")
    }

    /// `database.table` only: there is no catalog to put in front.
    fn ground_table_ref(&self, parts: &[&str]) -> AdapterResult<String> {
        let [schema, table] = parts else {
            return Err(AdapterError::msg(
                "table reference must be `database.table` on clickhouse",
            ));
        };
        self.format_table_ref("", schema, table)
    }

    /// `* EXCEPT (a, b)` — ClickHouse's `SELECT` modifier.
    fn star_excluding(&self, columns: &[&str]) -> Option<String> {
        Some(format!("* EXCEPT ({})", columns.join(", ")))
    }

    /// Always `false`: every type change rebuilds the table.
    ///
    /// ClickHouse's `MODIFY COLUMN` widens integers (`TINYINT` → … →
    /// `BIGINT`) and `REAL` → `DOUBLE` without changing a value, but
    /// `describe_table` reports a type with its `Nullable` wrapper peeled
    /// off, so the `MODIFY COLUMN` drift would replay could drop a column's
    /// nullability. A rebuild is always correct; an `ALTER` that loses NULLs
    /// is not.
    fn is_safe_type_widening(&self, _source_type: &str, _target_type: &str) -> bool {
        false
    }

    /// `ALTER TABLE t MODIFY COLUMN c T`. Not reached while
    /// [`Self::is_safe_type_widening`] refuses every change; ClickHouse does
    /// not accept the ANSI `ALTER COLUMN … TYPE` form.
    fn alter_column_type_sql(
        &self,
        table_ref: &str,
        column: &str,
        new_type: &str,
    ) -> AdapterResult<String> {
        check_identifier(column)?;
        rocky_core::sql_gen::validate_sql_type(new_type).map_err(AdapterError::new)?;
        Ok(format!(
            "ALTER TABLE {table_ref} MODIFY COLUMN {column} {new_type}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn d() -> ClickHouseDialect {
        ClickHouseDialect::new()
    }

    #[test]
    fn table_refs_render_bare_and_validate() {
        assert_eq!(
            d().format_table_ref("", "raw", "orders").unwrap(),
            "raw.orders"
        );
        assert!(d().format_table_ref("", "raw; DROP", "orders").is_err());
        assert!(d().format_table_ref("", "raw", "1orders").is_err());
        let err = d()
            .format_table_ref("analytics", "raw", "orders")
            .unwrap_err();
        assert!(err.to_string().contains("catalog = \"\""), "{err}");
        assert_eq!(
            d().ground_table_ref(&["raw", "orders"]).unwrap(),
            "raw.orders"
        );
        assert!(d().ground_table_ref(&["c", "raw", "orders"]).is_err());
    }

    #[test]
    fn full_refresh_is_one_atomic_replace() {
        assert_eq!(
            d().create_table_as("m.t", "SELECT 1 AS a"),
            "CREATE OR REPLACE TABLE m.t ENGINE = MergeTree ORDER BY tuple() AS\nSELECT 1 AS a"
        );
        assert!(!d().full_refresh_needs_predrop());
        // A bootstrap create must never replace a table.
        assert_eq!(
            d().create_table_as_new("m.t", "SELECT 1"),
            "CREATE TABLE m.t ENGINE = MergeTree ORDER BY tuple() AS\nSELECT 1"
        );
    }

    #[test]
    fn table_options_render_between_name_and_as() {
        let opts = rocky_ir::ClickHouseTableOptions {
            engine: None,
            order_by: vec!["customer_id".into()],
            partition_by: Some("toYYYYMM(order_date)".into()),
        };
        assert_eq!(
            d().create_table_as_with_clickhouse_options("m.t", "SELECT 1", &opts, true)
                .unwrap(),
            "CREATE OR REPLACE TABLE m.t ENGINE = MergeTree PARTITION BY toYYYYMM(order_date) \
             ORDER BY customer_id AS\nSELECT 1"
        );
        assert!(
            d().create_table_as_with_clickhouse_options("m.t", "SELECT 1", &opts, false)
                .unwrap()
                .starts_with("CREATE TABLE m.t ENGINE")
        );
        let bad = rocky_ir::ClickHouseTableOptions {
            engine: Some("Log".into()),
            ..Default::default()
        };
        assert!(
            d().create_table_as_with_clickhouse_options("m.t", "SELECT 1", &bad, true)
                .is_err()
        );
        // Redshift options are refused, not dropped.
        assert!(
            d().create_table_as_with_redshift_options(
                "m.t",
                "SELECT 1",
                &rocky_ir::RedshiftTableOptions::default(),
                true
            )
            .is_err()
        );
    }

    #[test]
    fn merge_snapshot_and_materialized_view_are_refused() {
        let err = d()
            .merge_into("m.t", "SELECT 1", &[Arc::from("id")], &ColumnSelection::All)
            .unwrap_err();
        assert!(err.to_string().contains("E053"), "{err}");
        assert!(d().merge_unsupported_reason().is_some());
        assert!(d().snapshot_unsupported_reason().is_some());
        assert!(d().materialized_view_ddl("m.t", "SELECT 1").is_err());
    }

    #[test]
    fn time_interval_stages_before_deleting() {
        let stmts = d()
            .insert_overwrite_partition(
                "m.t",
                "d >= '2026-01-01 00:00:00' AND d < '2026-01-02 00:00:00'",
                "SELECT * FROM s",
            )
            .unwrap();
        let stage = stage_table(
            "m.t",
            "d >= '2026-01-01 00:00:00' AND d < '2026-01-02 00:00:00'",
        );
        assert!(stage.starts_with("m.t__rocky_stage_"), "{stage}");
        assert_eq!(
            stmts,
            vec![
                format!("DROP TABLE IF EXISTS {stage}"),
                format!("CREATE TABLE {stage} AS m.t"),
                format!("INSERT INTO {stage}\nSELECT * FROM s"),
                "DELETE FROM m.t WHERE d >= toDateTime('2026-01-01 00:00:00') AND d < \
                 toDateTime('2026-01-02 00:00:00')"
                    .to_string(),
                format!("INSERT INTO m.t SELECT * FROM {stage}"),
                format!("DROP TABLE IF EXISTS {stage}"),
            ]
        );
        // Concurrent windows of one model never share a staging table.
        assert_ne!(
            stage,
            stage_table(
                "m.t",
                "d >= '2026-01-02 00:00:00' AND d < '2026-01-03 00:00:00'"
            )
        );
    }

    #[test]
    fn datetime_literals_only_rewrite_exact_timestamps() {
        assert_eq!(
            datetime_literals("a = '2026-01-01' AND b = 'x' AND c < '2026-01-01 00:00:00'"),
            "a = '2026-01-01' AND b = 'x' AND c < toDateTime('2026-01-01 00:00:00')"
        );
        assert_eq!(datetime_literals("é '2026'"), "é '2026'");
    }

    #[test]
    fn watermark_is_an_explicit_utc_instant() {
        let t = chrono::DateTime::parse_from_rfc3339("2026-09-15T10:00:00.25Z")
            .unwrap()
            .with_timezone(&chrono::Utc);
        assert_eq!(
            d().watermark_where("ts", Some(&t)).unwrap(),
            "WHERE ts > toDateTime64('2026-09-15 10:00:00.250000', 6, 'UTC')"
        );
        assert!(d().watermark_where("ts; DROP", None).is_err());
    }

    #[test]
    fn select_clause_validates_metadata() {
        let meta = [MetadataColumn::new_unchecked(
            "_loaded",
            "TEXT",
            "'x'; DROP",
        )];
        assert!(d().select_clause(&ColumnSelection::All, &meta).is_err());
        let ok = [MetadataColumn::new_unchecked("_src", "TEXT", "'a'")];
        assert_eq!(
            d().select_clause(&ColumnSelection::All, &ok).unwrap(),
            "SELECT *, CAST('a' AS TEXT) AS _src"
        );
    }

    #[test]
    fn small_renderings() {
        assert_eq!(
            d().create_schema_sql("", "marts").unwrap().unwrap(),
            "CREATE DATABASE IF NOT EXISTS marts"
        );
        assert!(d().create_schema_sql("c", "marts").unwrap().is_err());
        assert!(d().create_catalog_sql("c").is_none());
        assert!(d().tablesample_clause(10).is_none());
        assert_eq!(
            d().star_excluding(&["a", "b"]).as_deref(),
            Some("* EXCEPT (a, b)")
        );
        assert_eq!(
            d().regex_match_predicate("email", "^a\\.b$").unwrap(),
            "match(email, '^a\\\\.b$')"
        );
        assert_eq!(
            d().list_tables_sql("", "raw").unwrap(),
            "SELECT name AS table_name FROM system.tables WHERE database = 'raw' AND NOT is_temporary"
        );
        assert_eq!(
            d().surrogate_key_expr(&["a"]),
            "lower(hex(MD5(cast(coalesce(cast(a as VARCHAR), '_dbt_utils_surrogate_key_null_') \
             as VARCHAR))))"
        );
        assert_eq!(
            d().alter_column_type_sql("m.t", "c", "BIGINT").unwrap(),
            "ALTER TABLE m.t MODIFY COLUMN c BIGINT"
        );
    }

    #[test]
    fn every_type_change_rebuilds() {
        assert!(!d().is_safe_type_widening("BIGINT", "INTEGER"));
        assert!(!d().is_safe_type_widening("DOUBLE", "REAL"));
    }
}
