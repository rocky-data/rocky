//! SCD Type 2 snapshot SQL generation.
//!
//! This module provides the types and SQL generators for slowly-changing
//! dimension (SCD2) snapshots. A snapshot tracks historical changes to a
//! source table by maintaining `valid_from`, `valid_to`, `is_current`, and
//! `snapshot_id` columns in the target history table.
//!
//! Two change-detection strategies are supported:
//!
//! - **Timestamp** — detects changes by comparing a designated `updated_at`
//!   column between source and target. Efficient when the source reliably
//!   maintains a last-modified timestamp.
//!
//! - **Check** — detects changes by comparing all non-key columns between
//!   source and the current target row. Useful when there is no reliable
//!   `updated_at` column.
//!
//! # Foundation only
//!
//! This module provides types and SQL generators. The CLI command and run
//! integration are planned as a follow-up.

use chrono::Utc;
use rocky_sql::validation;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::config::SnapshotPipelineConfig;
use crate::ir::{GovernanceConfig, SnapshotPlan, SourceRef, TargetRef};
use crate::sql_gen::SqlGenError;
use crate::traits::SqlDialect;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// How the snapshot detects changed rows.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SnapshotStrategy {
    /// Compare a designated timestamp column between source and target.
    Timestamp {
        /// Column name that holds the last-modified timestamp (e.g., `updated_at`).
        updated_at: String,
    },
    /// Compare all non-key columns to detect changes. Used when there is
    /// no reliable `updated_at` column on the source.
    Check {
        /// Columns to compare. When empty the generator compares all
        /// non-key columns (requires the caller to resolve the column
        /// list from schema introspection before calling SQL gen).
        check_columns: Vec<String>,
    },
}

/// Parsed snapshot configuration ready for SQL generation.
///
/// Bridges [`SnapshotPipelineConfig`] (TOML-level) to [`SnapshotPlan`]
/// (IR-level) and carries the additional metadata needed by the SQL
/// generators in this module.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotConfig {
    /// Source table (three-part reference).
    pub source: SourceRef,
    /// Target history table (three-part reference).
    pub target: TargetRef,
    /// Column(s) that uniquely identify a row in the source table.
    pub unique_key: Vec<String>,
    /// Change-detection strategy.
    pub strategy: SnapshotStrategy,
    /// When true, rows deleted from the source get their `valid_to` set
    /// to `CURRENT_TIMESTAMP` and `is_current` set to `FALSE`.
    pub invalidate_hard_deletes: bool,
}

impl SnapshotConfig {
    /// Build a [`SnapshotConfig`] from the TOML-level pipeline config.
    ///
    /// The `updated_at` field on [`SnapshotPipelineConfig`] maps to the
    /// [`SnapshotStrategy::Timestamp`] strategy.
    pub fn from_pipeline_config(cfg: &SnapshotPipelineConfig) -> Self {
        Self {
            source: SourceRef {
                catalog: cfg.source.catalog.clone(),
                schema: cfg.source.schema.clone(),
                table: cfg.source.table.clone(),
            },
            target: TargetRef {
                catalog: cfg.target.catalog.clone(),
                schema: cfg.target.schema.clone(),
                table: cfg.target.table.clone(),
            },
            unique_key: cfg.unique_key.clone(),
            strategy: SnapshotStrategy::Timestamp {
                updated_at: cfg.updated_at.clone(),
            },
            invalidate_hard_deletes: cfg.invalidate_hard_deletes,
        }
    }

    /// Convert to the IR-level [`SnapshotPlan`] consumed by the existing
    /// `sql_gen::generate_snapshot_sql` path.
    pub fn to_plan(&self) -> SnapshotPlan {
        let updated_at = match &self.strategy {
            SnapshotStrategy::Timestamp { updated_at } => updated_at.clone(),
            // For check strategy, the updated_at field in SnapshotPlan is
            // unused — the check-strategy SQL generator bypasses it.
            SnapshotStrategy::Check { .. } => String::new(),
        };
        SnapshotPlan {
            source: self.source.clone(),
            target: self.target.clone(),
            unique_key: self.unique_key.clone(),
            updated_at,
            invalidate_hard_deletes: self.invalidate_hard_deletes,
            governance: GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// SCD2 column names (constants)
// ---------------------------------------------------------------------------

/// Column that records when this version became active.
pub const COL_VALID_FROM: &str = "valid_from";
/// Column that records when this version was superseded (`NULL` = current).
pub const COL_VALID_TO: &str = "valid_to";
/// Boolean flag: `TRUE` for the currently-active version of each key.
pub const COL_IS_CURRENT: &str = "is_current";
/// UUID that groups all rows captured in a single snapshot run.
pub const COL_SNAPSHOT_ID: &str = "snapshot_id";

// ---------------------------------------------------------------------------
// SQL generation — initial load
// ---------------------------------------------------------------------------

/// Generate the DDL for first-run bootstrap when the target table does not
/// yet exist.
///
/// Creates the target with all source columns plus the four SCD2 columns
/// (`valid_from`, `valid_to`, `is_current`, `snapshot_id`). Uses `WHERE 1=0`
/// so no data rows are copied — the subsequent `generate_snapshot_sql` call
/// handles the actual data load.
pub fn generate_initial_load_sql(
    config: &SnapshotConfig,
    dialect: &dyn SqlDialect,
) -> Result<String, SqlGenError> {
    let source = format_source(config, dialect)?;
    let target = format_target(config, dialect)?;

    // Validate SCD2 column names (they are constants, but defense in depth).
    for col in [
        COL_VALID_FROM,
        COL_VALID_TO,
        COL_IS_CURRENT,
        COL_SNAPSHOT_ID,
    ] {
        validation::validate_identifier(col)?;
    }

    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {target} AS \
         SELECT *, \
         CAST(NULL AS TIMESTAMP) AS {vf}, \
         CAST(NULL AS TIMESTAMP) AS {vt}, \
         CAST(NULL AS BOOLEAN) AS {ic}, \
         CAST(NULL AS STRING) AS {sid} \
         FROM {source} WHERE 1=0",
        vf = COL_VALID_FROM,
        vt = COL_VALID_TO,
        ic = COL_IS_CURRENT,
        sid = COL_SNAPSHOT_ID,
    ))
}

// ---------------------------------------------------------------------------
// SQL generation — snapshot MERGE
// ---------------------------------------------------------------------------

/// Generate the SCD2 MERGE statements for a snapshot run.
///
/// Returns a `Vec<String>` of SQL statements that must be executed in order:
///
/// 1. **Close changed rows** — sets `valid_to = CURRENT_TIMESTAMP` and
///    `is_current = FALSE` for rows whose tracked columns have changed.
/// 2. **Insert new versions** — inserts the updated rows from source with
///    `valid_from = CURRENT_TIMESTAMP`, `valid_to = NULL`, `is_current = TRUE`,
///    and a fresh `snapshot_id`.
/// 3. **Insert brand-new rows** — inserts rows that exist in source but
///    have no match in the target at all (same shape as step 2).
/// 4. *(optional)* **Invalidate hard deletes** — closes rows in the target
///    that no longer exist in the source.
///
/// The `snapshot_id` is a deterministic UUID generated once per call and
/// shared across all statements in the batch.
pub fn generate_snapshot_sql(
    config: &SnapshotConfig,
    dialect: &dyn SqlDialect,
) -> Result<Vec<String>, SqlGenError> {
    if config.unique_key.is_empty() {
        return Err(SqlGenError::MergeNoKey);
    }

    let source = format_source(config, dialect)?;
    let target = format_target(config, dialect)?;

    // Validate unique_key identifiers.
    for k in &config.unique_key {
        validation::validate_identifier(k)?;
    }

    // Validate strategy-specific identifiers.
    match &config.strategy {
        SnapshotStrategy::Timestamp { updated_at } => {
            validation::validate_identifier(updated_at)?;
        }
        SnapshotStrategy::Check { check_columns } => {
            if check_columns.is_empty() {
                return Err(SqlGenError::UnsafeFragment {
                    value: String::new(),
                    reason: "check strategy requires at least one check_column".to_string(),
                });
            }
            for col in check_columns {
                validation::validate_identifier(col)?;
            }
        }
    }

    let snapshot_id = generate_snapshot_id(&config.target);

    let join_cond = build_join_condition(&config.unique_key, "target", "source");

    // Build the change-detection predicate for WHEN MATCHED.
    let change_predicate = match &config.strategy {
        SnapshotStrategy::Timestamp { updated_at } => {
            format!("source.{updated_at} != target.{updated_at}")
        }
        SnapshotStrategy::Check { check_columns } => check_columns
            .iter()
            .map(|c| format!("source.{c} IS DISTINCT FROM target.{c}"))
            .collect::<Vec<_>>()
            .join(" OR "),
    };

    let mut stmts = Vec::new();

    // Statement 1: MERGE — close changed rows and insert new keys.
    let merge = format!(
        "MERGE INTO {target} AS target \
         USING {source} AS source \
         ON {join_cond} AND target.{ic} = TRUE \
         WHEN MATCHED AND ({change_predicate}) THEN \
           UPDATE SET {vt} = CURRENT_TIMESTAMP, {ic} = FALSE \
         WHEN NOT MATCHED THEN \
           INSERT (*) VALUES (\
             source.*, CURRENT_TIMESTAMP, CAST(NULL AS TIMESTAMP), TRUE, '{sid}'\
           )",
        ic = COL_IS_CURRENT,
        vt = COL_VALID_TO,
        sid = snapshot_id,
    );
    stmts.push(merge);

    // Statement 2: Insert fresh versions for rows that were just closed.
    // These are rows where the MERGE set valid_to (changed rows) but we
    // still need the new version with is_current = TRUE.
    let self_join_cond = build_join_condition(&config.unique_key, "t2", "source");
    let existing_join_cond = build_join_condition(&config.unique_key, "existing", "source");

    let insert_updated = format!(
        "INSERT INTO {target} \
         SELECT source.*, \
         CURRENT_TIMESTAMP AS {vf}, \
         CAST(NULL AS TIMESTAMP) AS {vt}, \
         TRUE AS {ic}, \
         '{sid}' AS {snapshot_id_col} \
         FROM {source} AS source \
         INNER JOIN {target} AS target \
         ON {join_cond} \
         WHERE target.{vt} IS NOT NULL \
         AND target.{vt} = (\
           SELECT MAX(t2.{vt}) FROM {target} AS t2 \
           WHERE {self_join_cond}\
         ) \
         AND NOT EXISTS (\
           SELECT 1 FROM {target} AS existing \
           WHERE {existing_join_cond} AND existing.{ic} = TRUE\
         )",
        vf = COL_VALID_FROM,
        vt = COL_VALID_TO,
        ic = COL_IS_CURRENT,
        sid = snapshot_id,
        snapshot_id_col = COL_SNAPSHOT_ID,
    );
    stmts.push(insert_updated);

    // Statement 3 (optional): Invalidate hard-deleted rows.
    if config.invalidate_hard_deletes {
        let invalidate = format!(
            "UPDATE {target} SET \
             {vt} = CURRENT_TIMESTAMP, \
             {ic} = FALSE \
             WHERE {ic} = TRUE \
             AND NOT EXISTS (\
               SELECT 1 FROM {source} AS source \
               WHERE {join_cond}\
             )",
            vt = COL_VALID_TO,
            ic = COL_IS_CURRENT,
        );
        stmts.push(invalidate);
    }

    Ok(stmts)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Generate a unique snapshot ID from the current timestamp and target table.
///
/// Produces a 16-hex-char identifier (first 8 bytes of SHA-256) that is
/// unique per run and deterministic within a single `generate_snapshot_sql`
/// call. All statements in the batch share the same ID.
fn generate_snapshot_id(target: &TargetRef) -> String {
    let now = Utc::now();
    let input = format!(
        "{}|{}",
        target.full_name(),
        now.timestamp_nanos_opt().unwrap_or(0)
    );
    let hash = Sha256::digest(input.as_bytes());
    // First 8 bytes → 16 hex chars. Short enough for readability, collision
    // probability negligible for a run-scoped identifier.
    hash[..8]
        .iter()
        .fold(String::with_capacity(16), |mut acc, b| {
            use std::fmt::Write;
            write!(acc, "{b:02x}").expect("hex formatting");
            acc
        })
}

/// Format and validate the source table reference.
fn format_source(config: &SnapshotConfig, dialect: &dyn SqlDialect) -> Result<String, SqlGenError> {
    dialect
        .format_table_ref(
            &config.source.catalog,
            &config.source.schema,
            &config.source.table,
        )
        .map_err(|e| SqlGenError::UnsafeFragment {
            value: String::new(),
            reason: e.to_string(),
        })
}

/// Format and validate the target table reference.
fn format_target(config: &SnapshotConfig, dialect: &dyn SqlDialect) -> Result<String, SqlGenError> {
    dialect
        .format_table_ref(
            &config.target.catalog,
            &config.target.schema,
            &config.target.table,
        )
        .map_err(|e| SqlGenError::UnsafeFragment {
            value: String::new(),
            reason: e.to_string(),
        })
}

/// Build a `left.k1 = right.k1 AND left.k2 = right.k2` join condition.
fn build_join_condition(keys: &[String], left: &str, right: &str) -> String {
    keys.iter()
        .map(|k| format!("{left}.{k} = {right}.{k}"))
        .collect::<Vec<_>>()
        .join(" AND ")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::fmt::Write;

    use super::*;
    use crate::ir::{ColumnSelection, MetadataColumn};
    use crate::traits::{AdapterError, AdapterResult};

    /// Test dialect mirroring Databricks behavior (three-part table refs).
    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> AdapterResult<String> {
            rocky_sql::validation::format_table_ref(catalog, schema, table)
                .map_err(AdapterError::new)
        }

        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }

        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target}\n{select_sql}")
        }

        fn merge_into(
            &self,
            target: &str,
            source_sql: &str,
            keys: &[String],
            update_cols: &ColumnSelection,
        ) -> AdapterResult<String> {
            if keys.is_empty() {
                return Err(AdapterError::msg("merge requires at least one key"));
            }
            let on_clause = keys
                .iter()
                .map(|k| format!("t.{k} = s.{k}"))
                .collect::<Vec<_>>()
                .join(" AND ");
            let update_clause = match update_cols {
                ColumnSelection::All => "UPDATE SET *".to_string(),
                ColumnSelection::Explicit(cols) => {
                    let sets = cols
                        .iter()
                        .map(|c| format!("t.{c} = s.{c}"))
                        .collect::<Vec<_>>();
                    format!("UPDATE SET {}", sets.join(", "))
                }
            };
            Ok(format!(
                "MERGE INTO {target} AS t\n\
                 USING (\n{source_sql}\n) AS s\n\
                 ON {on_clause}\n\
                 WHEN MATCHED THEN {update_clause}\n\
                 WHEN NOT MATCHED THEN INSERT *"
            ))
        }

        fn select_clause(
            &self,
            columns: &ColumnSelection,
            metadata: &[MetadataColumn],
        ) -> AdapterResult<String> {
            let mut sql = String::from("SELECT ");
            match columns {
                ColumnSelection::All => sql.push('*'),
                ColumnSelection::Explicit(cols) => {
                    sql.push_str(&cols.join(", "));
                }
            }
            for mc in metadata {
                write!(
                    sql,
                    ", CAST({} AS {}) AS {}",
                    mc.value, mc.data_type, mc.name
                )
                .unwrap();
            }
            Ok(sql)
        }

        fn watermark_where(&self, timestamp_col: &str, target_ref: &str) -> AdapterResult<String> {
            Ok(format!(
                "WHERE {timestamp_col} > (SELECT COALESCE(MAX({timestamp_col}), TIMESTAMP '1970-01-01') FROM {target_ref})"
            ))
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE TABLE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }

        fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
            Some(Ok(format!("CREATE CATALOG IF NOT EXISTS {name}")))
        }

        fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
            Some(Ok(format!(
                "CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
            )))
        }

        fn tablesample_clause(&self, percent: u32) -> Option<String> {
            Some(format!("TABLESAMPLE ({percent} PERCENT)"))
        }

        fn insert_overwrite_partition(
            &self,
            target: &str,
            partition_filter: &str,
            select_sql: &str,
        ) -> AdapterResult<Vec<String>> {
            Ok(vec![format!(
                "INSERT INTO {target} REPLACE WHERE {partition_filter}\n{select_sql}"
            )])
        }
    }

    fn dialect() -> TestDialect {
        TestDialect
    }

    fn timestamp_config() -> SnapshotConfig {
        SnapshotConfig {
            source: SourceRef {
                catalog: "raw_catalog".into(),
                schema: "raw__us_west__shopify".into(),
                table: "customers".into(),
            },
            target: TargetRef {
                catalog: "acme_warehouse".into(),
                schema: "silver__scd".into(),
                table: "customers_history".into(),
            },
            unique_key: vec!["customer_id".into()],
            strategy: SnapshotStrategy::Timestamp {
                updated_at: "updated_at".into(),
            },
            invalidate_hard_deletes: false,
        }
    }

    fn check_config() -> SnapshotConfig {
        SnapshotConfig {
            strategy: SnapshotStrategy::Check {
                check_columns: vec!["name".into(), "email".into(), "status".into()],
            },
            ..timestamp_config()
        }
    }

    // -----------------------------------------------------------------------
    // Initial load tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_initial_load_creates_table_with_scd2_columns() {
        let config = timestamp_config();
        let sql = generate_initial_load_sql(&config, &dialect()).unwrap();

        assert!(
            sql.starts_with(
                "CREATE TABLE IF NOT EXISTS acme_warehouse.silver__scd.customers_history"
            ),
            "expected CREATE TABLE IF NOT EXISTS, got: {sql}"
        );
        assert!(sql.contains("valid_from"), "missing valid_from: {sql}");
        assert!(sql.contains("valid_to"), "missing valid_to: {sql}");
        assert!(sql.contains("is_current"), "missing is_current: {sql}");
        assert!(sql.contains("snapshot_id"), "missing snapshot_id: {sql}");
        assert!(
            sql.contains("WHERE 1=0"),
            "initial load should create empty table: {sql}"
        );
        assert!(
            sql.contains("FROM raw_catalog.raw__us_west__shopify.customers"),
            "should reference source: {sql}"
        );
    }

    #[test]
    fn test_initial_load_scd2_column_types() {
        let config = timestamp_config();
        let sql = generate_initial_load_sql(&config, &dialect()).unwrap();

        assert!(
            sql.contains("CAST(NULL AS TIMESTAMP) AS valid_from"),
            "valid_from should be TIMESTAMP: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS TIMESTAMP) AS valid_to"),
            "valid_to should be TIMESTAMP: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS BOOLEAN) AS is_current"),
            "is_current should be BOOLEAN: {sql}"
        );
        assert!(
            sql.contains("CAST(NULL AS STRING) AS snapshot_id"),
            "snapshot_id should be STRING: {sql}"
        );
    }

    // -----------------------------------------------------------------------
    // Timestamp strategy tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_timestamp_strategy_merge_statement() {
        let config = timestamp_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        // Without hard deletes: MERGE + INSERT = 2 statements.
        assert_eq!(stmts.len(), 2, "expected 2 statements, got: {stmts:?}");

        let merge = &stmts[0];
        assert!(
            merge.contains("MERGE INTO acme_warehouse.silver__scd.customers_history AS target"),
            "MERGE should target history table: {merge}"
        );
        assert!(
            merge.contains("USING raw_catalog.raw__us_west__shopify.customers AS source"),
            "MERGE should use source table: {merge}"
        );
        assert!(
            merge.contains("target.customer_id = source.customer_id"),
            "join should use unique_key: {merge}"
        );
        assert!(
            merge.contains("target.is_current = TRUE"),
            "should only match current rows: {merge}"
        );
        assert!(
            merge.contains("source.updated_at != target.updated_at"),
            "timestamp strategy should compare updated_at: {merge}"
        );
        assert!(
            merge.contains("valid_to = CURRENT_TIMESTAMP"),
            "MATCHED should close the row: {merge}"
        );
        assert!(
            merge.contains("is_current = FALSE"),
            "MATCHED should set is_current = FALSE: {merge}"
        );
    }

    #[test]
    fn test_timestamp_strategy_insert_new_versions() {
        let config = timestamp_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();
        let insert = &stmts[1];

        assert!(
            insert.starts_with("INSERT INTO acme_warehouse.silver__scd.customers_history"),
            "second statement should INSERT: {insert}"
        );
        assert!(
            insert.contains("CURRENT_TIMESTAMP AS valid_from"),
            "new version should have valid_from: {insert}"
        );
        assert!(
            insert.contains("CAST(NULL AS TIMESTAMP) AS valid_to"),
            "new version should have NULL valid_to: {insert}"
        );
        assert!(
            insert.contains("TRUE AS is_current"),
            "new version should be current: {insert}"
        );
        assert!(
            insert.contains("AS snapshot_id"),
            "new version should carry snapshot_id: {insert}"
        );
    }

    #[test]
    fn test_snapshot_id_is_hex() {
        let config = timestamp_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        // The snapshot_id appears in the MERGE NOT MATCHED clause and in the
        // INSERT statement. Extract one instance and verify it's valid hex.
        let insert = &stmts[1];
        let marker = "' AS snapshot_id";
        let end_pos = insert
            .find(marker)
            .expect("should contain snapshot_id marker");
        let before = &insert[..end_pos];
        let start_pos = before.rfind('\'').expect("opening quote") + 1;
        let sid = &insert[start_pos..end_pos];
        assert_eq!(
            sid.len(),
            16,
            "snapshot_id should be 16 hex chars, got: '{sid}'"
        );
        assert!(
            sid.chars().all(|c| c.is_ascii_hexdigit()),
            "snapshot_id should be hex, got: '{sid}'"
        );
    }

    #[test]
    fn test_consistent_snapshot_id_across_statements() {
        let config = timestamp_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        // Extract snapshot_id from each statement that contains one.
        let marker = "' AS snapshot_id";
        let ids: Vec<&str> = stmts
            .iter()
            .filter_map(|s| {
                let end_pos = s.find(marker)?;
                let before = &s[..end_pos];
                let start_pos = before.rfind('\'')? + 1;
                Some(&s[start_pos..end_pos])
            })
            .collect();

        assert!(
            !ids.is_empty(),
            "at least the INSERT should carry a snapshot_id"
        );
        let first = ids[0];
        for id in &ids {
            assert_eq!(
                *id, first,
                "all statements should share the same snapshot_id"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Check strategy tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_check_strategy_change_detection() {
        let config = check_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        let merge = &stmts[0];
        assert!(
            merge.contains("source.name IS DISTINCT FROM target.name"),
            "check strategy should use IS DISTINCT FROM for name: {merge}"
        );
        assert!(
            merge.contains("source.email IS DISTINCT FROM target.email"),
            "check strategy should use IS DISTINCT FROM for email: {merge}"
        );
        assert!(
            merge.contains("source.status IS DISTINCT FROM target.status"),
            "check strategy should use IS DISTINCT FROM for status: {merge}"
        );
        // Columns joined by OR.
        assert!(
            merge.contains(" OR "),
            "check columns should be ORed: {merge}"
        );
    }

    #[test]
    fn test_check_strategy_no_updated_at_reference() {
        let config = check_config();
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        let merge = &stmts[0];
        assert!(
            !merge.contains("updated_at"),
            "check strategy should not reference updated_at: {merge}"
        );
    }

    #[test]
    fn test_check_strategy_empty_columns_errors() {
        let config = SnapshotConfig {
            strategy: SnapshotStrategy::Check {
                check_columns: vec![],
            },
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(result.is_err(), "empty check_columns should error");
    }

    // -----------------------------------------------------------------------
    // Multiple unique_key columns
    // -----------------------------------------------------------------------

    #[test]
    fn test_composite_unique_key() {
        let config = SnapshotConfig {
            unique_key: vec!["customer_id".into(), "region".into()],
            ..timestamp_config()
        };
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();
        let merge = &stmts[0];

        assert!(
            merge.contains(
                "target.customer_id = source.customer_id AND target.region = source.region"
            ),
            "composite key should produce AND-joined condition: {merge}"
        );
    }

    #[test]
    fn test_composite_unique_key_in_insert() {
        let config = SnapshotConfig {
            unique_key: vec!["customer_id".into(), "region".into()],
            ..timestamp_config()
        };
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();
        let insert = &stmts[1];

        assert!(
            insert.contains("target.customer_id = source.customer_id"),
            "INSERT join should use all key columns: {insert}"
        );
        assert!(
            insert.contains("target.region = source.region"),
            "INSERT join should use all key columns: {insert}"
        );
    }

    // -----------------------------------------------------------------------
    // Hard deletes
    // -----------------------------------------------------------------------

    #[test]
    fn test_hard_deletes_enabled() {
        let config = SnapshotConfig {
            invalidate_hard_deletes: true,
            ..timestamp_config()
        };
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        // With hard deletes: MERGE + INSERT + UPDATE = 3 statements.
        assert_eq!(stmts.len(), 3, "expected 3 statements with hard deletes");

        let invalidate = &stmts[2];
        assert!(
            invalidate.starts_with("UPDATE acme_warehouse.silver__scd.customers_history"),
            "third statement should UPDATE: {invalidate}"
        );
        assert!(
            invalidate.contains("valid_to = CURRENT_TIMESTAMP"),
            "should close deleted rows: {invalidate}"
        );
        assert!(
            invalidate.contains("is_current = FALSE"),
            "should mark as not current: {invalidate}"
        );
        assert!(
            invalidate.contains("NOT EXISTS"),
            "should check source existence: {invalidate}"
        );
    }

    #[test]
    fn test_hard_deletes_disabled() {
        let config = SnapshotConfig {
            invalidate_hard_deletes: false,
            ..timestamp_config()
        };
        let stmts = generate_snapshot_sql(&config, &dialect()).unwrap();

        // Without hard deletes: MERGE + INSERT = 2 statements.
        assert_eq!(stmts.len(), 2, "expected 2 statements without hard deletes");
    }

    // -----------------------------------------------------------------------
    // Error cases
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_unique_key_errors() {
        let config = SnapshotConfig {
            unique_key: vec![],
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(
            matches!(result, Err(SqlGenError::MergeNoKey)),
            "empty unique_key should error"
        );
    }

    #[test]
    fn test_unsafe_unique_key_rejected() {
        let config = SnapshotConfig {
            unique_key: vec!["id; DROP TABLE".into()],
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(result.is_err(), "unsafe identifier should be rejected");
    }

    #[test]
    fn test_unsafe_updated_at_rejected() {
        let config = SnapshotConfig {
            strategy: SnapshotStrategy::Timestamp {
                updated_at: "col; DROP TABLE".into(),
            },
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(result.is_err(), "unsafe updated_at should be rejected");
    }

    #[test]
    fn test_unsafe_check_column_rejected() {
        let config = SnapshotConfig {
            strategy: SnapshotStrategy::Check {
                check_columns: vec!["ok_col".into(), "bad col".into()],
            },
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(result.is_err(), "unsafe check column should be rejected");
    }

    #[test]
    fn test_unsafe_source_table_rejected() {
        let config = SnapshotConfig {
            source: SourceRef {
                catalog: "cat; DROP TABLE".into(),
                schema: "sch".into(),
                table: "tbl".into(),
            },
            ..timestamp_config()
        };
        let result = generate_snapshot_sql(&config, &dialect());
        assert!(result.is_err(), "unsafe source catalog should be rejected");
    }

    // -----------------------------------------------------------------------
    // from_pipeline_config
    // -----------------------------------------------------------------------

    #[test]
    fn test_from_pipeline_config() {
        use crate::config::{
            ChecksConfig, ExecutionConfig, SnapshotPipelineConfig, SnapshotSourceConfig,
            SnapshotTargetConfig,
        };

        let pipeline_cfg = SnapshotPipelineConfig {
            unique_key: vec!["order_id".into()],
            updated_at: "modified_at".into(),
            invalidate_hard_deletes: true,
            source: SnapshotSourceConfig {
                adapter: "default".into(),
                catalog: "src_cat".into(),
                schema: "src_sch".into(),
                table: "orders".into(),
            },
            target: SnapshotTargetConfig {
                adapter: "default".into(),
                catalog: "tgt_cat".into(),
                schema: "tgt_sch".into(),
                table: "orders_history".into(),
                governance: Default::default(),
            },
            checks: ChecksConfig::default(),
            execution: ExecutionConfig::default(),
            depends_on: vec![],
        };

        let config = SnapshotConfig::from_pipeline_config(&pipeline_cfg);

        assert_eq!(config.source.catalog, "src_cat");
        assert_eq!(config.source.schema, "src_sch");
        assert_eq!(config.source.table, "orders");
        assert_eq!(config.target.catalog, "tgt_cat");
        assert_eq!(config.target.schema, "tgt_sch");
        assert_eq!(config.target.table, "orders_history");
        assert_eq!(config.unique_key, vec!["order_id"]);
        assert!(config.invalidate_hard_deletes);
        assert_eq!(
            config.strategy,
            SnapshotStrategy::Timestamp {
                updated_at: "modified_at".into()
            }
        );
    }
}
