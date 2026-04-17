//! Row-quarantine SQL compilation for the quality pipeline.
//!
//! Phase 3 of the DQX parity plan. Lowers a subset of row-level
//! [`crate::tests::TestType`] assertions into a single boolean predicate
//! per target table, and emits CTAS statements that split the source into
//! `<table>__valid` (passing rows) and `<table>__quarantine` (failing
//! rows with per-assertion `_error_*` label columns), or one of the
//! variants described by [`crate::config::QuarantineMode`].
//!
//! Only **error-severity** assertions of kind `not_null`,
//! `accepted_values`, and `expression` are lowered. Aggregate / set-based
//! kinds (`unique`, `relationships`, `row_count_range`) are non-quarantining
//! by design — they still emit observational `CheckResult` entries from
//! [`crate::checks`].

use std::collections::HashSet;

use thiserror::Error;

use rocky_sql::validation::{self, ValidationError};

use crate::config::{QualityAssertion, QuarantineConfig, QuarantineMode};
use crate::ir::TableRef;
use crate::tests::{TestSeverity, TestType};
use crate::traits::{AdapterError, SqlDialect};

/// Errors from quarantine SQL compilation.
#[derive(Debug, Error)]
pub enum QuarantineError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("adapter error: {0}")]
    Adapter(String),

    #[error("assertion '{name}' requires a column but none was provided")]
    MissingColumn { name: String },

    #[error("accepted_values assertion '{name}' requires at least one value")]
    EmptyAcceptedValues { name: String },

    #[error("expression assertion has an empty expression")]
    EmptyExpression,

    #[error("in_range bound '{value}' must parse as a number")]
    InvalidInRangeBound { value: String },

    #[error("regex_match assertion '{name}' contains an unsafe pattern")]
    UnsafeRegexPattern { name: String },
}

impl From<AdapterError> for QuarantineError {
    fn from(e: AdapterError) -> Self {
        QuarantineError::Adapter(e.to_string())
    }
}

/// A compiled quarantine plan for a single target table.
#[derive(Debug, Clone)]
pub struct QuarantinePlan {
    /// Execution mode — mirrors the input [`QuarantineConfig::mode`].
    pub mode: QuarantineMode,
    /// Fully-qualified name of the source table (input to the split).
    pub source_table: String,
    /// Fully-qualified name of the `__valid` output table. Empty for
    /// [`QuarantineMode::Tag`] (that mode rewrites the source in place).
    pub valid_table: String,
    /// Fully-qualified name of the `__quarantine` output table. Empty for
    /// [`QuarantineMode::Drop`] (failing rows are discarded).
    pub quarantine_table: String,
    /// SQL statements to execute, in order. The runtime executes them
    /// sequentially and counts row effects per statement. Quarantine
    /// CTAS runs before the valid CTAS so a partial failure leaves a
    /// stray quarantine table (cheap to inspect) rather than a stale
    /// valid table downstream pipelines might read.
    pub statements: Vec<QuarantineStatement>,
}

/// A single SQL statement produced by [`compile_quarantine_sql`].
#[derive(Debug, Clone)]
pub struct QuarantineStatement {
    /// Human-readable role of this statement (`"quarantine"`, `"valid"`,
    /// `"tag"`). Used for logging and row-effect attribution.
    pub role: StatementRole,
    /// Fully-qualified table name this statement writes to.
    pub target: String,
    /// The SQL text to execute.
    pub sql: String,
}

/// Role of a statement inside a [`QuarantinePlan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementRole {
    /// CTAS that writes the `__quarantine` table.
    Quarantine,
    /// CTAS that writes the `__valid` table.
    Valid,
    /// CTAS that rewrites the source table in-place with `_error_*` tags.
    Tag,
}

/// Compile a quarantine plan for one table.
///
/// Returns `Ok(None)` when the table has no quarantinable assertions
/// (nothing to split). Returns an error if SQL generation fails — a
/// single bad assertion fails the whole plan so the caller doesn't
/// silently ship partial quarantine SQL.
///
/// `assertions` should be the full list of assertions attached to the
/// pipeline's [`crate::config::ChecksConfig`]. Filtering by target table
/// happens here; filtering by severity and kind also happens here.
pub fn compile_quarantine_sql(
    assertions: &[QualityAssertion],
    unqualified_table: &str,
    table_ref: &TableRef,
    dialect: &dyn SqlDialect,
    config: &QuarantineConfig,
) -> Result<Option<QuarantinePlan>, QuarantineError> {
    if !config.enabled {
        return Ok(None);
    }

    let quarantinable: Vec<&QualityAssertion> = assertions
        .iter()
        .filter(|a| a.table == unqualified_table)
        .filter(|a| a.test.severity == TestSeverity::Error)
        .filter(|a| is_quarantinable(&a.test.test_type))
        .collect();

    if quarantinable.is_empty() {
        return Ok(None);
    }

    let source_table =
        dialect.format_table_ref(&table_ref.catalog, &table_ref.schema, &table_ref.table)?;

    let valid_name = suffixed_table_name(&table_ref.table, &config.suffix_valid)?;
    let quarantine_name = suffixed_table_name(&table_ref.table, &config.suffix_quarantine)?;
    let valid_table =
        dialect.format_table_ref(&table_ref.catalog, &table_ref.schema, &valid_name)?;
    let quarantine_table =
        dialect.format_table_ref(&table_ref.catalog, &table_ref.schema, &quarantine_name)?;

    let mut names: HashSet<String> = HashSet::new();
    let mut labeled: Vec<LabeledPredicate> = Vec::with_capacity(quarantinable.len());
    for assertion in &quarantinable {
        let label = safe_error_label(assertion, &mut names)?;
        let base_pred = lower_valid_predicate(
            &assertion.test.test_type,
            &assertion.test.column,
            &label,
            dialect,
        )?;
        let valid_pred = wrap_filter(&assertion.test.filter, &base_pred);
        labeled.push(LabeledPredicate { label, valid_pred });
    }

    let valid_where = labeled
        .iter()
        .map(|p| p.valid_pred.clone())
        .collect::<Vec<_>>()
        .join(" AND ");

    let mut statements = Vec::with_capacity(2);
    match config.mode {
        QuarantineMode::Split => {
            statements.push(build_quarantine_ctas(
                &quarantine_table,
                &source_table,
                &labeled,
                &valid_where,
                dialect,
            ));
            statements.push(build_valid_ctas(
                &valid_table,
                &source_table,
                &valid_where,
                dialect,
            ));
        }
        QuarantineMode::Drop => {
            statements.push(build_valid_ctas(
                &valid_table,
                &source_table,
                &valid_where,
                dialect,
            ));
        }
        QuarantineMode::Tag => {
            statements.push(build_tag_ctas(&source_table, &labeled, dialect));
        }
    }

    Ok(Some(QuarantinePlan {
        mode: config.mode,
        source_table,
        valid_table: if matches!(config.mode, QuarantineMode::Tag) {
            String::new()
        } else {
            valid_table
        },
        quarantine_table: if matches!(config.mode, QuarantineMode::Split) {
            quarantine_table
        } else {
            String::new()
        },
        statements,
    }))
}

struct LabeledPredicate {
    label: String,
    valid_pred: String,
}

/// Whether a test kind lowers cleanly to a row-level boolean predicate.
///
/// `unique` / `relationships` / `row_count_range` are aggregate / set-based
/// — they stay observational. `expression` and `regex_match` are
/// trusted user SQL (same contract as [`crate::tests::generate_test_sql`]).
pub fn is_quarantinable(test_type: &TestType) -> bool {
    matches!(
        test_type,
        TestType::NotNull
            | TestType::AcceptedValues { .. }
            | TestType::Expression { .. }
            | TestType::InRange { .. }
            | TestType::RegexMatch { .. }
    )
}

/// Build a SQL-identifier-safe `_error_<name>` label for one assertion.
///
/// The user's optional `name` can contain characters (colons, spaces) that
/// are not valid identifiers. We synthesize a safe name from the kind +
/// column (and the explicit `name` when it parses as an identifier),
/// appending `_2`, `_3`, ... on collision so two assertions with the same
/// kind + column still produce distinct error columns.
fn safe_error_label(
    assertion: &QualityAssertion,
    taken: &mut HashSet<String>,
) -> Result<String, QuarantineError> {
    let base = if let Some(name) = assertion.name.as_deref() {
        if validation::validate_identifier(name).is_ok() {
            name.to_string()
        } else {
            synthesize_label(&assertion.test.test_type, assertion.test.column.as_deref())
        }
    } else {
        synthesize_label(&assertion.test.test_type, assertion.test.column.as_deref())
    };

    let mut candidate = format!("_error_{base}");
    validation::validate_identifier(&candidate)?;

    let mut n = 2u32;
    while taken.contains(&candidate) {
        candidate = format!("_error_{base}_{n}");
        validation::validate_identifier(&candidate)?;
        n += 1;
    }
    taken.insert(candidate.clone());
    Ok(candidate)
}

fn synthesize_label(test_type: &TestType, column: Option<&str>) -> String {
    let kind = match test_type {
        TestType::NotNull => "not_null",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Expression { .. } => "expression",
        TestType::Unique => "unique",
        TestType::Relationships { .. } => "relationships",
        TestType::RowCountRange { .. } => "row_count_range",
        TestType::InRange { .. } => "in_range",
        TestType::RegexMatch { .. } => "regex_match",
    };
    match column {
        Some(c) if validation::validate_identifier(c).is_ok() => format!("{kind}_{c}"),
        _ => kind.to_string(),
    }
}

/// Lower a quarantinable assertion into a boolean "row is valid" predicate.
///
/// Semantics match the existing [`crate::tests::generate_test_sql`] output:
/// - `NotNull`: NULL values are failures → `col IS NOT NULL`.
/// - `AcceptedValues`: NULL passes (existing `col NOT IN (...)` treats NULL
///   as excluded) → `(col IS NULL OR col IN (...))`.
/// - `Expression`: NULL passes (existing `WHERE NOT (expr)` treats NULL
///   as excluded) → `COALESCE((expr), TRUE)`.
/// - `InRange`: NULL passes → `(col IS NULL OR NOT (col < min OR col > max))`.
/// - `RegexMatch`: NULL passes → `(col IS NULL OR <dialect regex match>)`.
///
/// The returned predicate is total — it evaluates to `TRUE` or `FALSE`,
/// never NULL — so the top-level `AND` and `NOT` cannot propagate NULL.
fn lower_valid_predicate(
    test_type: &TestType,
    column: &Option<String>,
    label: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, QuarantineError> {
    match test_type {
        TestType::NotNull => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            Ok(format!("{col} IS NOT NULL"))
        }
        TestType::AcceptedValues { values } => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            if values.is_empty() {
                return Err(QuarantineError::EmptyAcceptedValues {
                    name: label.to_string(),
                });
            }
            let in_list = values
                .iter()
                .map(|v| format!("'{}'", v.replace('\'', "''")))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("({col} IS NULL OR {col} IN ({in_list}))"))
        }
        TestType::Expression { expression } => {
            if expression.trim().is_empty() {
                return Err(QuarantineError::EmptyExpression);
            }
            // Expression is user-supplied SQL (same contract as
            // `generate_test_sql`). Wrap in COALESCE so NULL expressions
            // count as passing — matches the existing
            // `WHERE NOT (expression)` semantic.
            Ok(format!("COALESCE(({expression}), TRUE)"))
        }
        TestType::InRange { min, max } => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            let fail_pred =
                crate::tests::in_range_fail_predicate_public(col, min.as_deref(), max.as_deref())
                    .map_err(|e| match e {
                    crate::tests::TestGenError::MissingInRangeBound => {
                        QuarantineError::EmptyExpression
                    }
                    crate::tests::TestGenError::InvalidInRangeBound { value } => {
                        QuarantineError::InvalidInRangeBound { value }
                    }
                    _ => QuarantineError::EmptyExpression,
                })?;
            // NULL-permissive: (col IS NULL OR NOT (col < min OR col > max))
            Ok(format!("({col} IS NULL OR NOT ({fail_pred}))"))
        }
        TestType::RegexMatch { pattern } => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            crate::tests::validate_regex_pattern(pattern).map_err(|_| {
                QuarantineError::UnsafeRegexPattern {
                    name: label.to_string(),
                }
            })?;
            let match_pred = dialect
                .regex_match_predicate(col, pattern)
                .map_err(QuarantineError::from)?;
            // NULL-permissive: (col IS NULL OR <match>)
            Ok(format!("({col} IS NULL OR {match_pred})"))
        }
        // Non-quarantinable kinds filtered upstream by `is_quarantinable`.
        TestType::Unique | TestType::Relationships { .. } | TestType::RowCountRange { .. } => {
            Err(QuarantineError::EmptyExpression)
        }
    }
}

fn required_column<'a>(
    column: &'a Option<String>,
    label: &str,
) -> Result<&'a str, QuarantineError> {
    column
        .as_deref()
        .ok_or_else(|| QuarantineError::MissingColumn {
            name: label.to_string(),
        })
}

/// Apply a per-check `filter` to a base valid predicate.
///
/// Model: if the filter is FALSE or NULL, the row is "out of scope" for
/// this assertion and passes unconditionally. If the filter is TRUE, the
/// base predicate decides.
///
/// Implemented as `CASE WHEN (filter) THEN base ELSE TRUE END` — a total
/// boolean across all rows, so the top-level `AND` / `NOT` can't
/// propagate NULL.
fn wrap_filter(filter: &Option<String>, base_pred: &str) -> String {
    match filter.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(f) => format!("(CASE WHEN ({f}) THEN ({base_pred}) ELSE TRUE END)"),
        None => base_pred.to_string(),
    }
}

fn build_quarantine_ctas(
    target: &str,
    source: &str,
    labeled: &[LabeledPredicate],
    valid_where: &str,
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let error_cols = labeled
        .iter()
        .map(|p| {
            format!(
                "CASE WHEN NOT ({}) THEN '{}' END AS {}",
                p.valid_pred, p.label, p.label
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let select = format!("SELECT *, {error_cols} FROM {source} WHERE NOT ({valid_where})");
    QuarantineStatement {
        role: StatementRole::Quarantine,
        target: target.to_string(),
        sql: dialect.create_table_as(target, &select),
    }
}

fn build_valid_ctas(
    target: &str,
    source: &str,
    valid_where: &str,
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let select = format!("SELECT * FROM {source} WHERE {valid_where}");
    QuarantineStatement {
        role: StatementRole::Valid,
        target: target.to_string(),
        sql: dialect.create_table_as(target, &select),
    }
}

fn build_tag_ctas(
    source: &str,
    labeled: &[LabeledPredicate],
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let error_cols = labeled
        .iter()
        .map(|p| {
            format!(
                "CASE WHEN NOT ({}) THEN '{}' END AS {}",
                p.valid_pred, p.label, p.label
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let select = format!("SELECT *, {error_cols} FROM {source}");
    QuarantineStatement {
        role: StatementRole::Tag,
        target: source.to_string(),
        sql: dialect.create_table_as(source, &select),
    }
}

fn suffixed_table_name(table: &str, suffix: &str) -> Result<String, QuarantineError> {
    validation::validate_identifier(table)?;
    let candidate = format!("{table}{suffix}");
    validation::validate_identifier(&candidate)?;
    Ok(candidate)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::ir::{ColumnSelection, MetadataColumn};
    use crate::tests::TestDecl;
    use crate::traits::{AdapterError, AdapterResult};

    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn format_table_ref(&self, c: &str, s: &str, t: &str) -> AdapterResult<String> {
            rocky_sql::validation::format_table_ref(c, s, t).map_err(AdapterError::new)
        }
        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }
        fn insert_into(&self, _: &str, _: &str) -> String {
            unimplemented!()
        }
        fn merge_into(
            &self,
            _: &str,
            _: &str,
            _: &[String],
            _: &ColumnSelection,
        ) -> AdapterResult<String> {
            unimplemented!()
        }
        fn select_clause(
            &self,
            _: &ColumnSelection,
            _: &[MetadataColumn],
        ) -> AdapterResult<String> {
            unimplemented!()
        }
        fn watermark_where(&self, _: &str, _: &str) -> AdapterResult<String> {
            unimplemented!()
        }
        fn describe_table_sql(&self, t: &str) -> String {
            format!("DESCRIBE TABLE {t}")
        }
        fn drop_table_sql(&self, t: &str) -> String {
            format!("DROP TABLE IF EXISTS {t}")
        }
        fn create_catalog_sql(&self, _: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn create_schema_sql(&self, _: &str, _: &str) -> Option<AdapterResult<String>> {
            None
        }
        fn tablesample_clause(&self, _: u32) -> Option<String> {
            None
        }
        fn insert_overwrite_partition(
            &self,
            _: &str,
            _: &str,
            _: &str,
        ) -> AdapterResult<Vec<String>> {
            unimplemented!()
        }
        fn regex_match_predicate(&self, column: &str, pattern: &str) -> AdapterResult<String> {
            Ok(format!("regexp_matches({column}, '{pattern}')"))
        }
    }

    fn table() -> TableRef {
        TableRef {
            catalog: "poc".into(),
            schema: "staging__orders".into(),
            table: "orders".into(),
        }
    }

    fn assertion(
        name: Option<&str>,
        kind: TestType,
        column: Option<&str>,
        severity: TestSeverity,
    ) -> QualityAssertion {
        QualityAssertion {
            table: "orders".into(),
            name: name.map(str::to_string),
            test: TestDecl {
                test_type: kind,
                column: column.map(str::to_string),
                severity,
                filter: None,
            },
        }
    }

    fn split_config() -> QuarantineConfig {
        QuarantineConfig {
            enabled: true,
            mode: QuarantineMode::Split,
            suffix_valid: "__valid".into(),
            suffix_quarantine: "__quarantine".into(),
        }
    }

    #[test]
    fn disabled_returns_none() {
        let cfg = QuarantineConfig {
            enabled: false,
            ..split_config()
        };
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan =
            compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn no_quarantinable_assertions_returns_none() {
        let cfg = split_config();
        let assertions = vec![
            // Unique is not quarantinable (set-based).
            assertion(None, TestType::Unique, Some("id"), TestSeverity::Error),
            // Warning-severity NotNull is filtered out.
            assertion(
                None,
                TestType::NotNull,
                Some("email"),
                TestSeverity::Warning,
            ),
        ];
        let plan =
            compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn split_mode_produces_two_statements() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        assert_eq!(plan.statements.len(), 2);
        assert_eq!(plan.statements[0].role, StatementRole::Quarantine);
        assert_eq!(plan.statements[1].role, StatementRole::Valid);
        assert_eq!(plan.valid_table, "poc.staging__orders.orders__valid");
        assert_eq!(
            plan.quarantine_table,
            "poc.staging__orders.orders__quarantine"
        );
    }

    #[test]
    fn not_null_predicate_shape() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Valid CTAS contains: WHERE customer_id IS NOT NULL
        assert!(
            plan.statements[1]
                .sql
                .contains("WHERE customer_id IS NOT NULL")
        );
        // Quarantine CTAS contains: WHERE NOT (customer_id IS NOT NULL)
        assert!(
            plan.statements[0]
                .sql
                .contains("WHERE NOT (customer_id IS NOT NULL)")
        );
        // Error label column is present.
        assert!(
            plan.statements[0]
                .sql
                .contains("_error_not_null_customer_id")
        );
    }

    #[test]
    fn accepted_values_predicate_is_null_permissive() {
        // This is the advisor's flagged case: NULL must pass the
        // AcceptedValues check to match existing Rocky `NOT IN` semantics.
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::AcceptedValues {
                values: vec!["pending".into(), "shipped".into()],
            },
            Some("status"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Valid predicate: NULL or in list.
        assert!(
            plan.statements[1]
                .sql
                .contains("(status IS NULL OR status IN ('pending', 'shipped'))")
        );
    }

    #[test]
    fn accepted_values_escapes_quotes() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::AcceptedValues {
                values: vec!["it's".into()],
            },
            Some("name"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        assert!(plan.statements[1].sql.contains("'it''s'"));
    }

    #[test]
    fn expression_uses_coalesce_true() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "amount >= 0".into(),
            },
            None,
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // COALESCE makes NULL `amount >= 0` resolve to TRUE (preserves
        // existing `WHERE NOT (expr)` semantic that excludes NULL).
        assert!(
            plan.statements[1]
                .sql
                .contains("COALESCE((amount >= 0), TRUE)")
        );
    }

    #[test]
    fn multiple_assertions_anded() {
        let cfg = split_config();
        let assertions = vec![
            assertion(
                None,
                TestType::NotNull,
                Some("customer_id"),
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::AcceptedValues {
                    values: vec!["pending".into()],
                },
                Some("status"),
                TestSeverity::Error,
            ),
        ];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        let valid_sql = &plan.statements[1].sql;
        assert!(valid_sql.contains("customer_id IS NOT NULL AND"));
        assert!(valid_sql.contains("status IS NULL OR status IN ('pending')"));
    }

    #[test]
    fn drop_mode_emits_only_valid_ctas() {
        let cfg = QuarantineConfig {
            mode: QuarantineMode::Drop,
            ..split_config()
        };
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        assert_eq!(plan.statements.len(), 1);
        assert_eq!(plan.statements[0].role, StatementRole::Valid);
        assert!(plan.quarantine_table.is_empty());
    }

    #[test]
    fn tag_mode_rewrites_source_in_place() {
        let cfg = QuarantineConfig {
            mode: QuarantineMode::Tag,
            ..split_config()
        };
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        assert_eq!(plan.statements.len(), 1);
        assert_eq!(plan.statements[0].role, StatementRole::Tag);
        // Target of the tag CTAS is the source table (no suffix).
        assert_eq!(plan.statements[0].target, "poc.staging__orders.orders");
        // Tag CTAS has no WHERE filter — every row is kept.
        assert!(!plan.statements[0].sql.contains("WHERE"));
        // But it does add the `_error_*` case column.
        assert!(
            plan.statements[0]
                .sql
                .contains("_error_not_null_customer_id")
        );
    }

    #[test]
    fn colon_in_user_name_is_synthesized_to_identifier() {
        // Matches the run_local.rs fallback `{kind}:{column}` — not a valid
        // identifier. The quarantine compiler must not trust user names
        // verbatim; it synthesizes a safe label instead.
        let cfg = split_config();
        let assertions = vec![assertion(
            Some("not_null:customer_id"),
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        assert!(
            plan.statements[0]
                .sql
                .contains("_error_not_null_customer_id")
        );
    }

    #[test]
    fn duplicate_assertion_labels_deduped_with_suffix() {
        let cfg = split_config();
        let assertions = vec![
            assertion(
                None,
                TestType::NotNull,
                Some("customer_id"),
                TestSeverity::Error,
            ),
            // Same kind + column — must not produce colliding _error columns.
            assertion(
                None,
                TestType::NotNull,
                Some("customer_id"),
                TestSeverity::Error,
            ),
        ];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        let sql = &plan.statements[0].sql;
        assert!(sql.contains("AS _error_not_null_customer_id,"));
        assert!(sql.contains("AS _error_not_null_customer_id_2"));
    }

    #[test]
    fn statement_order_is_quarantine_before_valid() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Quarantine runs first so a partial failure leaves a stray
        // quarantine table rather than a stale valid table.
        assert_eq!(plan.statements[0].role, StatementRole::Quarantine);
        assert_eq!(plan.statements[1].role, StatementRole::Valid);
    }

    #[test]
    fn rejects_bad_column_identifier() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("col; DROP TABLE"),
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg).err();
        assert!(err.is_some());
    }

    #[test]
    fn rejects_bad_suffix() {
        let cfg = QuarantineConfig {
            suffix_valid: "; DROP".into(),
            ..split_config()
        };
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg).err();
        assert!(err.is_some());
    }

    // ----- Phase 4a -----

    fn assertion_with_filter(
        kind: TestType,
        column: Option<&str>,
        filter: Option<&str>,
    ) -> QualityAssertion {
        QualityAssertion {
            table: "orders".into(),
            name: None,
            test: TestDecl {
                test_type: kind,
                column: column.map(str::to_string),
                severity: TestSeverity::Error,
                filter: filter.map(str::to_string),
            },
        }
    }

    #[test]
    fn in_range_lowers_to_null_permissive_valid_predicate() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::InRange {
                min: Some("0".into()),
                max: Some("1000".into()),
            },
            Some("amount"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Valid CTAS: (amount IS NULL OR NOT (amount < 0 OR amount > 1000))
        assert!(
            plan.statements[1]
                .sql
                .contains("(amount IS NULL OR NOT (amount < 0 OR amount > 1000))")
        );
    }

    #[test]
    fn regex_match_lowers_to_null_permissive_valid_predicate() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::RegexMatch {
                pattern: "^[a-z]+$".into(),
            },
            Some("email"),
            TestSeverity::Error,
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Valid CTAS: (email IS NULL OR regexp_matches(email, '^[a-z]+$'))
        assert!(
            plan.statements[1]
                .sql
                .contains("(email IS NULL OR regexp_matches(email, '^[a-z]+$'))")
        );
    }

    #[test]
    fn filter_wraps_valid_predicate_with_case() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("region = 'US'"),
        )];
        let plan = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .unwrap()
            .unwrap();
        // Out-of-scope rows (filter false/null) pass unconditionally:
        // (CASE WHEN (region = 'US') THEN (customer_id IS NOT NULL) ELSE TRUE END)
        assert!(
            plan.statements[1]
                .sql
                .contains("CASE WHEN (region = 'US') THEN (customer_id IS NOT NULL) ELSE TRUE END")
        );
    }

    #[test]
    fn in_range_rejects_non_numeric_bounds() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::InRange {
                min: Some("yesterday".into()),
                max: None,
            },
            Some("amount"),
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .err()
            .unwrap();
        assert!(matches!(err, QuarantineError::InvalidInRangeBound { .. }));
    }

    #[test]
    fn regex_match_rejects_unsafe_pattern() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::RegexMatch {
                pattern: "foo'; DROP TABLE".into(),
            },
            Some("email"),
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(&assertions, "orders", &table(), &TestDialect, &cfg)
            .err()
            .unwrap();
        assert!(matches!(err, QuarantineError::UnsafeRegexPattern { .. }));
    }

    #[test]
    fn all_phase4a_kinds_covered_by_is_quarantinable() {
        assert!(is_quarantinable(&TestType::NotNull));
        assert!(is_quarantinable(&TestType::AcceptedValues {
            values: vec!["a".into()]
        }));
        assert!(is_quarantinable(&TestType::Expression {
            expression: "x > 0".into()
        }));
        assert!(is_quarantinable(&TestType::InRange {
            min: Some("0".into()),
            max: Some("1".into()),
        }));
        assert!(is_quarantinable(&TestType::RegexMatch {
            pattern: "x".into()
        }));
        assert!(!is_quarantinable(&TestType::Unique));
        assert!(!is_quarantinable(&TestType::Relationships {
            to_table: "t".into(),
            to_column: "c".into(),
        }));
        assert!(!is_quarantinable(&TestType::RowCountRange {
            min: None,
            max: Some(10),
        }));
    }
}
