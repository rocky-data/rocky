//! Row-quarantine SQL compilation for the quality pipeline.
//!
//! Lowers a subset of row-level
//! [`crate::tests::TestType`] assertions into one boolean predicate each,
//! and emits CTAS statements that split the source into `<table>__valid`
//! (passing rows) and `<table>__quarantine` (failing rows with
//! per-assertion `_error_*` label columns), or one of the variants
//! described by [`crate::config::QuarantineMode`].
//!
//! Every mode evaluates each predicate once, in one statement. `split`
//! writes the labels to an intermediate table first, then derives both
//! outputs from those labels (#1937):
//!
//! ```text
//!   source ──CTAS, predicates evaluated here only──▶ _quarantine_labels_<token>
//!                                                     │  one label column per assertion
//!            ┌────────────────────────────────────────┤
//!            ▼ any label set                          ▼ no label set, labels left out
//!   <table>__quarantine                        <table>__valid
//! ```
//!
//! Only **error-severity** assertions of kind `not_null`,
//! `accepted_values`, and `expression` are lowered. Aggregate / set-based
//! kinds (`unique`, `relationships`, `row_count_range`) are non-quarantining
//! by design — they still emit observational `CheckResult` entries from
//! [`crate::checks`].

use std::collections::HashSet;

use thiserror::Error;

use rocky_sql::check_expression::ExpressionUse;
use rocky_sql::validation::{self, ValidationError};

use crate::config::{QualityAssertion, QuarantineConfig, QuarantineMode};
use crate::tests::{TestSeverity, TestType};
use crate::traits::{AdapterError, SqlDialect};
use rocky_ir::TableRef;

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

    /// `split` keeps its label columns out of the valid table with the
    /// warehouse's star-exclusion form, and this warehouse has none.
    #[error(
        "quarantine mode = \"split\" is not supported on {dialect}: the valid table is \
         written as `SELECT * EXCEPT (<labels>)`, and {dialect} has no such form"
    )]
    SplitNeedsStarExclusion { dialect: &'static str },

    #[error(
        "quarantine mode = \"tag\" is not supported on {dialect}: it rewrites its source table, and this dialect requires a pre-drop before CREATE TABLE AS; use mode = \"drop\""
    )]
    TagNeedsSourceReplacement { dialect: &'static str },

    /// `tag` replaces a label column it wrote on an earlier run with
    /// star exclusion, and this warehouse has none.
    #[error(
        "quarantine mode = \"tag\" cannot replace its earlier label columns on {dialect}: the \
         rewrite is `SELECT * EXCLUDE (<labels>)`, and {dialect} has no such form"
    )]
    TagNeedsStarExclusion { dialect: &'static str },

    /// The source already has a column under a label's name, and Rocky cannot
    /// prove it wrote that column (#2065). Never overwritten.
    #[error(
        "quarantine mode = \"{mode}\" would write the label column '{label}', but the source \
         table already has a column '{column}' {why}. Rocky does not overwrite a column it \
         cannot prove it wrote. Rename or remove that column (`ALTER TABLE <table> RENAME \
         COLUMN {column} TO <new_name>`, or rebuild the table without it where the warehouse \
         cannot rename), or give the assertion a `name` so its label is `_error_<name>`"
    )]
    LabelColumnCollision {
        mode: &'static str,
        label: String,
        column: String,
        /// Why ownership is not proven, for this mode.
        why: &'static str,
    },

    /// `split` and `tag` were compiled without the source's column list, so
    /// a label collision could not be checked (#2065).
    #[error(
        "quarantine mode = \"{mode}\" needs the source table's columns to check its label \
         names against, and they were not read"
    )]
    SourceColumnsUnread { mode: &'static str },

    /// A name quarantine would create exceeds the identifier length bound
    /// (#2065). Refused before any statement runs.
    #[error(
        "quarantine would create the {what} '{name}', which is {len} characters; identifiers are \
         limited to {max} characters (Snowflake and Databricks): {fix}"
    )]
    GeneratedNameTooLong {
        what: &'static str,
        name: String,
        len: usize,
        max: usize,
        fix: &'static str,
    },

    /// Two tables a mode writes, or one it writes and the source it reads,
    /// resolve to the same name.
    #[error(
        "quarantine would write the {first} table under the {second} table's name, '{name}': {fix}"
    )]
    TableNameCollision {
        name: String,
        first: &'static str,
        second: &'static str,
        /// What to change, for this pair.
        fix: &'static str,
    },
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
    /// sequentially and counts row effects per statement.
    ///
    /// For `split`: the label CTAS, then the quarantine CTAS, then the
    /// valid CTAS. Quarantine runs before valid so a partial failure leaves
    /// a stray quarantine table (cheap to inspect) rather than a stale
    /// valid table downstream pipelines might read.
    pub statements: Vec<QuarantineStatement>,
    /// `split` only: the statement that drops the intermediate label table.
    /// `None` for `drop` and `tag`, which write no intermediate table.
    ///
    /// The runtime runs it after [`Self::statements`] whatever they
    /// reported, the label statement included, because a failed statement
    /// must not leave the intermediate table behind (#1937, #2052). A CTAS
    /// can commit while the client sees a timeout, so a reported failure is
    /// not proof that nothing was created.
    ///
    /// The table is `_quarantine_labels_<token>` in the source's schema, with
    /// a random token per plan, and created with a plain `CREATE TABLE`. So
    /// it cannot replace a table someone else owns, two runs of the same
    /// pipeline do not share it, and whatever sits under that name is this
    /// plan's to drop. A process killed between the label statement and the
    /// drop leaves the table behind under that name: no later run can tell
    /// it from another run's table still in use.
    ///
    /// The name does not contain the source table's name, so its length is
    /// fixed (51 characters) however long the source's name is.
    pub drop_intermediate: Option<QuarantineStatement>,
    /// `tag` only: what Rocky owns on the source once [`Self::statements`]
    /// succeed, to record with `StateStore::set_quarantine_ownership` under
    /// [`Self::source_table`]. `None` for `split` and `drop`, which do not
    /// write their source.
    pub ownership_after: Option<QuarantineOwnership>,
}

/// The record that proves which `_error_*` columns of a source Rocky wrote
/// with `tag` (#2065).
///
/// It names the labels, and binds them to the exact column list the `tag`
/// statement left the table with. A label is owned only while the table
/// still has exactly those columns, in that order (ASCII case ignored):
///
/// ```text
///   rows appended or updated since          columns unchanged -> owned
///   table replaced or reshaped out of band  columns differ    -> not owned
/// ```
///
/// So a table swapped for one carrying a user's own `_error_<label>` column
/// is refused, not overwritten, unless the new table repeats Rocky's output
/// shape column for column.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct QuarantineOwnership {
    /// The label columns Rocky wrote: this run's, plus earlier proven labels
    /// the table still carries (an assertion removed from the config leaves
    /// its column behind, and it is still Rocky's).
    pub labels: Vec<String>,
    /// The table's columns, in order, right after the `tag` statement.
    pub columns: Vec<String>,
}

impl QuarantineOwnership {
    /// The labels this record proves on a table with `columns`: all of them
    /// when the table still has exactly the recorded columns, none otherwise.
    pub fn proven_labels(&self, columns: &[String]) -> &[String] {
        let unchanged = self.columns.len() == columns.len()
            && self
                .columns
                .iter()
                .zip(columns)
                .all(|(a, b)| a.eq_ignore_ascii_case(b));
        if unchanged { &self.labels } else { &[] }
    }
}

/// What the source table already holds, read before compiling `split` or
/// `tag` (#2065).
///
/// A label is written as `_error_<name>` after the source's columns. When
/// the source already has a column under that name (ASCII case ignored,
/// since labels are emitted unquoted):
///
/// ```text
///   split   refused: split never writes its source, so it owns none of
///           the source's columns
///   tag     label proven by `ownership` -> replaced (excluded from `*`)
///           otherwise                   -> refused, the column may be a user's
/// ```
///
/// Ownership is a state-store record written after a successful `tag`
/// (`StateStore::set_quarantine_ownership`); see [`QuarantineOwnership`]. It
/// travels with remote state like any other record. If the store is lost,
/// nothing proves ownership, so the next `tag` refuses and names the column.
/// Removing the column (or rebuilding the table without it) lets the
/// following run write it afresh and record it again. Failing closed there is
/// the point: a column Rocky cannot prove it wrote may be a user's.
#[derive(Debug, Clone, Default)]
pub struct SourceColumns {
    /// The source's column names as the warehouse reports them, or `None`
    /// when they were not read. `split` and `tag` refuse `None`.
    pub columns: Option<Vec<String>>,
    /// Rocky's record of the label columns it wrote here with `tag`.
    pub ownership: Option<QuarantineOwnership>,
}

impl SourceColumns {
    /// The source's columns as read, with no ownership record.
    pub fn read(columns: Vec<String>) -> Self {
        Self {
            columns: Some(columns),
            ownership: None,
        }
    }
}

/// Whether [`compile_quarantine_sql`] needs [`SourceColumns::columns`] for
/// this config: every mode that writes label columns.
pub fn needs_source_columns(config: &QuarantineConfig) -> bool {
    config.enabled && !matches!(config.mode, QuarantineMode::Drop)
}

/// The identifier length bound every generated quarantine name is checked
/// against (#2065). Snowflake and Databricks cap identifiers at 255
/// characters; BigQuery allows longer, so the bound is conservative there.
pub const MAX_GENERATED_IDENTIFIER_LEN: usize = 255;

/// A single SQL statement produced by [`compile_quarantine_sql`].
#[derive(Debug, Clone)]
pub struct QuarantineStatement {
    /// Human-readable role of this statement (`"label"`, `"quarantine"`,
    /// `"valid"`, `"tag"`, `"predrop_valid"`, `"drop_labels"`). Used for logging and row-effect
    /// attribution.
    pub role: StatementRole,
    /// Fully-qualified table name this statement writes to.
    pub target: String,
    /// The SQL text to execute.
    pub sql: String,
}

/// Role of a statement inside a [`QuarantinePlan`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementRole {
    /// `split` only: CTAS that writes the source rows plus one `_error_*`
    /// label column per assertion. The only statement that evaluates the
    /// predicates.
    Label,
    /// CTAS that writes the `__quarantine` table.
    Quarantine,
    /// CTAS that writes the `__valid` table.
    Valid,
    /// CTAS that rewrites the source table in-place with `_error_*` tags.
    Tag,
    /// `drop` only: removes the previous valid table before a dialect's
    /// non-replacing CTAS.
    PredropValid,
    /// `split` only: drops the intermediate table the [`Self::Label`]
    /// statement wrote.
    DropLabels,
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
///
/// Not deterministic for `split`: each call draws a new token for the
/// intermediate table and its label columns. See
/// [`QuarantinePlan::drop_intermediate`].
///
/// `source` is what the source table already holds; see [`SourceColumns`].
/// `split` and `tag` refuse a label that collides with a source column Rocky
/// cannot prove it wrote, before any statement runs. Every generated name is
/// checked against [`MAX_GENERATED_IDENTIFIER_LEN`] here too.
pub fn compile_quarantine_sql(
    assertions: &[QualityAssertion],
    unqualified_table: &str,
    table_ref: &TableRef,
    dialect: &dyn SqlDialect,
    config: &QuarantineConfig,
    source: &SourceColumns,
) -> Result<Option<QuarantinePlan>, QuarantineError> {
    let token = uuid::Uuid::new_v4().simple().to_string();
    compile_with_token(
        assertions,
        unqualified_table,
        table_ref,
        dialect,
        config,
        source,
        &token[..SPLIT_TOKEN_LEN],
    )
}

/// Hex digits of the per-plan token `split` names its working objects with:
/// a whole v4 UUID. The token is the ownership mark. The drop after a failed
/// label statement trusts that no other plan drew the same one, so it gets
/// every random bit the UUID has rather than a truncation of them.
const SPLIT_TOKEN_LEN: usize = 32;

/// `split`'s intermediate table is this prefix plus the token.
///
/// Neither working name is built from a user-controlled name. A 210-character
/// table name leaves `<table>__valid` inside the 255-character identifier
/// limit of Snowflake and Databricks; appending a suffix and a 32-digit token
/// to it would not.
const SPLIT_TABLE_PREFIX: &str = "_quarantine_labels_";

/// [`compile_quarantine_sql`] with the `split` token supplied, so tests can
/// pin the exact SQL.
fn compile_with_token(
    assertions: &[QualityAssertion],
    unqualified_table: &str,
    table_ref: &TableRef,
    dialect: &dyn SqlDialect,
    config: &QuarantineConfig,
    source: &SourceColumns,
    token: &str,
) -> Result<Option<QuarantinePlan>, QuarantineError> {
    if !config.enabled {
        return Ok(None);
    }

    let quarantinable: Vec<&QualityAssertion> = assertions
        .iter()
        .filter(|a| quarantines(a, unqualified_table))
        .collect();

    if quarantinable.is_empty() {
        return Ok(None);
    }

    if matches!(config.mode, QuarantineMode::Tag) && dialect.full_refresh_needs_predrop() {
        return Err(QuarantineError::TagNeedsSourceReplacement {
            dialect: dialect.name(),
        });
    }

    let source_table =
        dialect.format_table_ref(&table_ref.catalog, &table_ref.schema, &table_ref.table)?;

    // Only a table the mode writes is held to the length bound: `tag` writes
    // neither suffixed table and `drop` no quarantine table.
    let writes_valid = !matches!(config.mode, QuarantineMode::Tag);
    let writes_quarantine = matches!(config.mode, QuarantineMode::Split);
    let valid_name = suffixed_table_name(&table_ref.table, &config.suffix_valid, writes_valid)?;
    let quarantine_name = suffixed_table_name(
        &table_ref.table,
        &config.suffix_quarantine,
        writes_quarantine,
    )?;
    refuse_colliding_names(config.mode, &table_ref.table, &valid_name, &quarantine_name)?;
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
        let valid_pred = wrap_filter(&assertion.test.filter, &base_pred, &label, dialect)?;
        labeled.push(LabeledPredicate { label, valid_pred });
    }

    let labels: Vec<&str> = labeled.iter().map(|p| p.label.as_str()).collect();
    let replaced = resolve_label_collisions(config.mode, &labels, source)?;

    let mut statements = Vec::with_capacity(3);
    let mut drop_intermediate = None;
    let mut ownership_after = None;
    match config.mode {
        QuarantineMode::Split => {
            // In the intermediate table each label sits under a working name,
            // `_ql<ordinal>_<token>`, not under its own name. The source may
            // already have a column called `_error_…` (a quarantine table
            // checked again, say). DuckDB renames the second of two same-named
            // CTAS columns, so a label under a name the source also has would
            // be read as the source's column by every WHERE below. A per-plan
            // token is a name no existing column was given, and the ordinal
            // keeps the name's length fixed whatever the label's length. The
            // quarantine CTAS gives each label its real name back.
            let working: Vec<String> = (0..labeled.len())
                .map(|i| {
                    let name = format!("_ql{i}_{token}");
                    validate_generated_name("working label column", &name, FIX_INTERNAL)?;
                    Ok::<_, QuarantineError>(name)
                })
                .collect::<Result<_, _>>()?;
            let working: Vec<&str> = working.iter().map(String::as_str).collect();
            let without_labels = dialect.star_excluding(&working).ok_or(
                QuarantineError::SplitNeedsStarExclusion {
                    dialect: dialect.name(),
                },
            )?;

            let intermediate_name = format!("{SPLIT_TABLE_PREFIX}{token}");
            validate_generated_name("intermediate table", &intermediate_name, FIX_INTERNAL)?;
            let intermediate_table = dialect.format_table_ref(
                &table_ref.catalog,
                &table_ref.schema,
                &intermediate_name,
            )?;

            statements.push(build_label_ctas(
                StatementRole::Label,
                &intermediate_table,
                &source_table,
                "*",
                &labeled,
                &working,
                dialect,
            ));
            statements.push(build_split_quarantine_ctas(
                &quarantine_table,
                &intermediate_table,
                &labeled,
                &working,
                &without_labels,
                dialect,
            ));
            statements.push(build_split_valid_ctas(
                &valid_table,
                &intermediate_table,
                &working,
                &without_labels,
                dialect,
            ));
            drop_intermediate = Some(QuarantineStatement {
                role: StatementRole::DropLabels,
                sql: dialect.drop_table_sql(&intermediate_table),
                target: intermediate_table,
            });
        }
        QuarantineMode::Drop => {
            let valid_where = labeled
                .iter()
                .map(|p| p.valid_pred.as_str())
                .collect::<Vec<_>>()
                .join(" AND ");
            if dialect.full_refresh_needs_predrop() {
                statements.push(QuarantineStatement {
                    role: StatementRole::PredropValid,
                    target: valid_table.clone(),
                    sql: dialect.drop_table_sql(&valid_table),
                });
            }
            statements.push(build_valid_ctas(
                &valid_table,
                &source_table,
                &valid_where,
                dialect,
            ));
        }
        QuarantineMode::Tag => {
            // The label columns an earlier `tag` wrote are left out of `*`
            // and written afresh, so the rewrite does not carry a stale label
            // beside the new one (#2065).
            let star = if replaced.is_empty() {
                "*".to_string()
            } else {
                let replaced: Vec<&str> = replaced.iter().map(String::as_str).collect();
                dialect
                    .star_excluding(&replaced)
                    .ok_or(QuarantineError::TagNeedsStarExclusion {
                        dialect: dialect.name(),
                    })?
            };
            statements.push(build_label_ctas(
                StatementRole::Tag,
                &source_table,
                &source_table,
                &star,
                &labeled,
                &labels,
                dialect,
            ));
            ownership_after = Some(ownership_after_tag(&labels, &replaced, source));
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
        drop_intermediate,
        ownership_after,
    }))
}

/// Shown when a name Rocky builds from fixed parts alone exceeds the bound,
/// which a change to those parts would have to cause.
const FIX_INTERNAL: &str = "this name is built by Rocky alone; report it as a bug";

/// Validate a name quarantine generates: identifier characters, and at most
/// [`MAX_GENERATED_IDENTIFIER_LEN`] characters (#2065).
fn validate_generated_name(
    what: &'static str,
    name: &str,
    fix: &'static str,
) -> Result<(), QuarantineError> {
    validation::validate_identifier(name)?;
    // Identifier characters are ASCII, so bytes are characters.
    if name.len() > MAX_GENERATED_IDENTIFIER_LEN {
        return Err(QuarantineError::GeneratedNameTooLong {
            what,
            name: name.to_string(),
            len: name.len(),
            max: MAX_GENERATED_IDENTIFIER_LEN,
            fix,
        });
    }
    Ok(())
}

fn mode_name(mode: QuarantineMode) -> &'static str {
    match mode {
        QuarantineMode::Split => "split",
        QuarantineMode::Tag => "tag",
        QuarantineMode::Drop => "drop",
    }
}

/// Check each label against the source's columns (#2065) and return the
/// source columns `tag` replaces. See [`SourceColumns`] for the rule.
///
/// Compared with ASCII case ignored. Labels are emitted unquoted, so DuckDB,
/// Databricks and BigQuery resolve them without case, and Snowflake folds
/// them to upper case. On Snowflake a quoted lower-case user column
/// `"_error_x"` is distinct from the label `_ERROR_X`, and this comparison
/// still refuses it: a refusal in doubt, never an overwrite.
fn resolve_label_collisions(
    mode: QuarantineMode,
    labels: &[&str],
    source: &SourceColumns,
) -> Result<Vec<String>, QuarantineError> {
    if matches!(mode, QuarantineMode::Drop) {
        // `drop` writes no label column.
        return Ok(Vec::new());
    }
    let columns = source
        .columns
        .as_ref()
        .ok_or(QuarantineError::SourceColumnsUnread {
            mode: mode_name(mode),
        })?;
    let proven = source
        .ownership
        .as_ref()
        .map(|o| o.proven_labels(columns))
        .unwrap_or_default();
    let mut replaced = Vec::new();
    for label in labels {
        let matching: Vec<&String> = columns
            .iter()
            .filter(|c| c.eq_ignore_ascii_case(label))
            .collect();
        let Some(first) = matching.first() else {
            continue;
        };
        let refuse = |why| QuarantineError::LabelColumnCollision {
            mode: mode_name(mode),
            label: (*label).to_string(),
            column: (*first).clone(),
            why,
        };
        match mode {
            QuarantineMode::Split => {
                return Err(refuse(
                    "(split writes new tables and never its source, so the column is not \
                     Rocky's to replace)",
                ));
            }
            QuarantineMode::Tag => {
                let owned = proven.iter().any(|o| o.eq_ignore_ascii_case(label));
                if !owned {
                    return Err(refuse(
                        "and Rocky cannot prove an earlier `tag` run wrote it: the state store \
                         has no record of it, or the table's columns have changed since that \
                         run",
                    ));
                }
                if matching.len() > 1 {
                    return Err(refuse(
                        "more than once under different case, so Rocky cannot tell which \
                         one it wrote",
                    ));
                }
                // Matched a label ignoring ASCII case, so it is identifier
                // characters only; checked anyway before it is spliced.
                validation::validate_identifier(first)?;
                replaced.push((*first).clone());
            }
            QuarantineMode::Drop => {}
        }
    }
    Ok(replaced)
}

/// What Rocky owns on the source after a successful `tag`: this run's
/// labels plus earlier proven labels the source still carries through `*`,
/// bound to the column list the statement leaves (the source's columns minus
/// the replaced ones, then the labels).
fn ownership_after_tag(
    labels: &[&str],
    replaced: &[String],
    source: &SourceColumns,
) -> QuarantineOwnership {
    let columns = source.columns.as_deref().unwrap_or_default();
    let proven = source
        .ownership
        .as_ref()
        .map(|o| o.proven_labels(columns))
        .unwrap_or_default();
    let mut owned: Vec<String> = labels.iter().map(|l| (*l).to_string()).collect();
    for prior in proven {
        let still_there = columns.iter().any(|c| c.eq_ignore_ascii_case(prior));
        let already = owned.iter().any(|o| o.eq_ignore_ascii_case(prior));
        if still_there && !already {
            owned.push(prior.clone());
        }
    }
    owned.sort();
    let after: Vec<String> = columns
        .iter()
        .filter(|c| !replaced.contains(c))
        .cloned()
        .chain(labels.iter().map(|l| (*l).to_string()))
        .collect();
    QuarantineOwnership {
        labels: owned,
        columns: after,
    }
}

/// Whether `assertion` is lowered into `table`'s quarantine.
fn quarantines(assertion: &QualityAssertion, table: &str) -> bool {
    assertion.table == table
        && assertion.test.severity == TestSeverity::Error
        && is_quarantinable(&assertion.test.test_type)
}

/// Whether any of `assertions` is lowered into `table`'s quarantine, so a
/// caller reads [`SourceColumns`] only for a table that will be compiled.
pub fn has_quarantinable_assertions(assertions: &[QualityAssertion], table: &str) -> bool {
    assertions.iter().any(|a| quarantines(a, table))
}

struct LabeledPredicate {
    label: String,
    valid_pred: String,
}

/// Whether a test kind lowers cleanly to a row-level boolean predicate.
///
/// `unique` / `relationships` / `row_count_range` / `aggregate` /
/// `composite` are aggregate / set-based — they stay observational.
/// `expression` and `regex_match` are trusted user SQL (same contract as
/// [`crate::tests::generate_test_sql`]).
pub fn is_quarantinable(test_type: &TestType) -> bool {
    matches!(
        test_type,
        TestType::NotNull
            | TestType::AcceptedValues { .. }
            | TestType::Expression { .. }
            | TestType::InRange { .. }
            | TestType::RegexMatch { .. }
            | TestType::NotInFuture
            | TestType::OlderThanNDays { .. }
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

    const FIX_LABEL: &str =
        "give the assertion a shorter `name`, or check a column with a shorter name";
    let mut candidate = format!("_error_{base}");
    validate_generated_name("label column", &candidate, FIX_LABEL)?;

    let mut n = 2u32;
    // Compared without ASCII case: labels are emitted unquoted, so `_error_A`
    // and `_error_a` name one column on a case-insensitive warehouse.
    while taken.contains(&candidate.to_ascii_lowercase()) {
        candidate = format!("_error_{base}_{n}");
        validate_generated_name("label column", &candidate, FIX_LABEL)?;
        n += 1;
    }
    taken.insert(candidate.to_ascii_lowercase());
    Ok(candidate)
}

fn synthesize_label(test_type: &TestType, column: Option<&str>) -> String {
    let kind = match test_type {
        TestType::NotNull => "not_null",
        TestType::AcceptedValues { .. } => "accepted_values",
        TestType::Expression { .. } => "expression",
        TestType::Unique => "unique",
        TestType::UniqueExpr { .. } => "unique_expr",
        TestType::Relationships { .. } => "relationships",
        TestType::RowCountRange { .. } => "row_count_range",
        TestType::InRange { .. } => "in_range",
        TestType::RegexMatch { .. } => "regex_match",
        TestType::Aggregate { .. } => "aggregate",
        TestType::Composite { .. } => "composite",
        TestType::NotInFuture => "not_in_future",
        TestType::OlderThanNDays { .. } => "older_than_n_days",
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
                .map(|v| crate::sql_gen::string_literal(dialect, v))
                .collect::<Vec<_>>()
                .join(", ");
            Ok(format!("({col} IS NULL OR {col} IN ({in_list}))"))
        }
        TestType::Expression { expression } => {
            if expression.trim().is_empty() {
                return Err(QuarantineError::EmptyExpression);
            }
            // Expression is user-supplied SQL (same contract as
            // `generate_test_sql`) — refuse anything that could end the CTAS
            // and start another statement.
            let context = format!("quarantine assertion '{label}' `expression`");
            validation::reject_statement_terminator(&context, expression)?;
            // The terminator check stops a predicate ENDING the statement. It
            // does not stop one that stays inside the expression and still
            // reaches further than the model under test: a subquery reads any
            // table the run's credentials can see, and a content-reading
            // function (DuckDB `read_text`, Snowflake `GETVARIABLE`, a
            // BigQuery remote function) sits in scalar position and reads
            // whatever it is pointed at.
            //
            // #1820 closed that on the CHECKS path (`tests.rs`, before the
            // `NOT (...)` splice). This predicate is spliced into a CTAS that
            // runs with warehouse credentials, so it needs the same gate —
            // parse under the target dialect, then allowlist the functions.
            // Same validator, same dialect mapping, so the two paths cannot
            // drift into disagreeing about what is allowed.
            let sql_dialect = rocky_sql::check_expression::dialect_for(dialect.name());
            rocky_sql::check_expression::validate_check_expression(
                &context,
                expression,
                sql_dialect.as_ref(),
                // Every mode evaluates the predicate once, in one statement.
                // `split` used to evaluate it in two, and refused a clock
                // function for that reason (#1922); it now reads labels
                // written once (#1937).
                ExpressionUse::SinglePredicate,
            )?;
            // Wrap in COALESCE so NULL expressions count as passing — matches
            // the existing `WHERE NOT (expression)` semantic.
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
        TestType::NotInFuture => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            let now = dialect.current_timestamp_expr();
            // NULL-permissive: (col IS NULL OR col <= <now>)
            Ok(format!("({col} IS NULL OR {col} <= {now})"))
        }
        TestType::OlderThanNDays { days } => {
            let col = required_column(column, label)?;
            validation::validate_identifier(col)?;
            if *days == 0 {
                return Err(QuarantineError::InvalidInRangeBound { value: "0".into() });
            }
            let bound = dialect
                .date_minus_days_expr(*days)
                .map_err(QuarantineError::from)?;
            // NULL-permissive: (col IS NULL OR col <= <N days ago>)
            Ok(format!("({col} IS NULL OR {col} <= ({bound}))"))
        }
        // Non-quarantinable kinds filtered upstream by `is_quarantinable`.
        TestType::Unique
        | TestType::UniqueExpr { .. }
        | TestType::Relationships { .. }
        | TestType::RowCountRange { .. }
        | TestType::Aggregate { .. }
        | TestType::Composite { .. } => Err(QuarantineError::EmptyExpression),
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
///
/// The filter is user-supplied SQL, so it goes through
/// [`validation::reject_statement_terminator`] first — the same gate the
/// declarative-check builder applies.
fn wrap_filter(
    filter: &Option<String>,
    base_pred: &str,
    label: &str,
    dialect: &dyn SqlDialect,
) -> Result<String, QuarantineError> {
    match filter.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(f) => {
            let context = format!("quarantine assertion '{label}' `filter`");
            validation::reject_statement_terminator(&context, f)?;
            // The filter is spliced into the same CTAS as the predicate, so it
            // reaches exactly as far. Guarding only `expression` would close
            // one door and leave an identical one beside it. `Filter` shares
            // `SinglePredicate`'s rules exactly; it is a separate mode only
            // so a refusal names the right field, not "an expression check"
            // (#1971).
            let sql_dialect = rocky_sql::check_expression::dialect_for(dialect.name());
            rocky_sql::check_expression::validate_check_expression(
                &context,
                f,
                sql_dialect.as_ref(),
                ExpressionUse::Filter,
            )?;
            Ok(format!(
                "(CASE WHEN ({f}) THEN ({base_pred}) ELSE TRUE END)"
            ))
        }
        None => Ok(base_pred.to_string()),
    }
}

/// CTAS that writes every source row plus one label column per assertion:
/// the label's name when the row fails it, NULL when it passes.
///
/// `split` writes it to the intermediate table and `tag` over the source.
/// Either way it is the only place a predicate is evaluated. Each predicate
/// is total (TRUE or FALSE, never NULL), so a NULL label means the row
/// passed that assertion.
///
/// `columns[i]` names the column for `labeled[i]`: the label itself for
/// `tag`, a working name for `split`. `star` is `*`, or for `tag` a `*` that
/// leaves out the label columns an earlier run wrote.
fn build_label_ctas(
    role: StatementRole,
    target: &str,
    source: &str,
    star: &str,
    labeled: &[LabeledPredicate],
    columns: &[&str],
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let error_cols = labeled
        .iter()
        .zip(columns)
        .map(|(p, column)| {
            format!(
                "CASE WHEN NOT ({}) THEN '{}' END AS {column}",
                p.valid_pred, p.label
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let select = format!("SELECT {star}, {error_cols} FROM {source}");
    // `split`'s intermediate table must be new: replacing an existing table
    // would destroy something this run does not own, and the drop after it
    // would finish the job. `tag` replaces its source by design.
    let sql = match role {
        StatementRole::Label => dialect.create_table_as_new(target, &select),
        _ => dialect.create_table_as(target, &select),
    };
    QuarantineStatement {
        role,
        target: target.to_string(),
        sql,
    }
}

/// `split`: the rows with at least one label set, with each label under its
/// own name after the source's columns.
fn build_split_quarantine_ctas(
    target: &str,
    intermediate: &str,
    labeled: &[LabeledPredicate],
    working: &[&str],
    without_labels: &str,
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let renamed = labeled
        .iter()
        .zip(working)
        .map(|(p, w)| format!("{w} AS {}", p.label))
        .collect::<Vec<_>>()
        .join(", ");
    let any_failed = working
        .iter()
        .map(|w| format!("{w} IS NOT NULL"))
        .collect::<Vec<_>>()
        .join(" OR ");
    let select =
        format!("SELECT {without_labels}, {renamed} FROM {intermediate} WHERE {any_failed}");
    QuarantineStatement {
        role: StatementRole::Quarantine,
        target: target.to_string(),
        sql: dialect.create_table_as(target, &select),
    }
}

/// `split`: the rows with no label set, without the label columns, so the
/// valid table has exactly the source's columns.
fn build_split_valid_ctas(
    target: &str,
    intermediate: &str,
    working: &[&str],
    without_labels: &str,
    dialect: &dyn SqlDialect,
) -> QuarantineStatement {
    let none_failed = working
        .iter()
        .map(|w| format!("{w} IS NULL"))
        .collect::<Vec<_>>()
        .join(" AND ");
    let select = format!("SELECT {without_labels} FROM {intermediate} WHERE {none_failed}");
    QuarantineStatement {
        role: StatementRole::Valid,
        target: target.to_string(),
        sql: dialect.create_table_as(target, &select),
    }
}

/// `drop`: the rows that pass every assertion. One statement, so the
/// predicates are evaluated once without an intermediate table.
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

/// Refuse suffixes that give two tables one name.
///
/// An empty `suffix_valid` names the valid table after the source, so the
/// valid CTAS replaces the source with its passing rows. Equal suffixes name
/// the valid and quarantine tables alike, so the valid CTAS replaces the
/// quarantined rows and they land in neither output.
///
/// Compared exactly, not without case. Snowflake quotes the names Rocky
/// formats, so `"orders__OUT"` and `"orders__out"` are two tables there, and a
/// case-insensitive comparison would refuse a working config. On DuckDB and
/// Databricks those two names are one table, and this comparison does not
/// catch it: suffixes that differ only in case are not refused.
///
/// Only the tables a mode writes are compared. `drop` writes no quarantine
/// table, so its suffix is free; `tag` rewrites the source by design.
fn refuse_colliding_names(
    mode: QuarantineMode,
    source: &str,
    valid: &str,
    quarantine: &str,
) -> Result<(), QuarantineError> {
    const VALID_EMPTY: &str = "set suffix_valid to a non-empty value";
    const QUARANTINE_EMPTY: &str = "set suffix_quarantine to a non-empty value";
    const EQUAL: &str = "give suffix_valid and suffix_quarantine different values";
    let pairs: Vec<(&str, &'static str, &str, &'static str, &'static str)> = match mode {
        QuarantineMode::Split => vec![
            (valid, "valid", source, "source", VALID_EMPTY),
            (quarantine, "quarantine", source, "source", QUARANTINE_EMPTY),
            (valid, "valid", quarantine, "quarantine", EQUAL),
        ],
        QuarantineMode::Drop => vec![(valid, "valid", source, "source", VALID_EMPTY)],
        QuarantineMode::Tag => vec![],
    };
    for (a, first, b, second, fix) in pairs {
        if a == b {
            return Err(QuarantineError::TableNameCollision {
                name: a.to_string(),
                first,
                second,
                fix,
            });
        }
    }
    Ok(())
}

fn suffixed_table_name(
    table: &str,
    suffix: &str,
    written: bool,
) -> Result<String, QuarantineError> {
    validation::validate_identifier(table)?;
    let candidate = format!("{table}{suffix}");
    if !written {
        validation::validate_identifier(&candidate)?;
        return Ok(candidate);
    }
    validate_generated_name(
        "output table",
        &candidate,
        "shorten the table name or the quarantine suffix",
    )?;
    Ok(candidate)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::tests::TestDecl;
    use crate::traits::{AdapterError, AdapterResult};
    use rocky_ir::{ColumnSelection, MetadataColumn};

    struct TestDialect;

    impl SqlDialect for TestDialect {
        fn literal_escape(&self) -> crate::traits::LiteralEscape {
            crate::traits::LiteralEscape::Standard
        }

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
            _: &[std::sync::Arc<str>],
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
        fn watermark_where(
            &self,
            _: &str,
            _: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> AdapterResult<String> {
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
        fn star_excluding(&self, columns: &[&str]) -> Option<String> {
            Some(format!("* EXCLUDE ({})", columns.join(", ")))
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap();
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn split_mode_produces_three_statements_and_a_drop() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_with_token(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
            "t0k3n",
        )
        .unwrap()
        .unwrap();
        let roles: Vec<StatementRole> = plan.statements.iter().map(|s| s.role).collect();
        assert_eq!(
            roles,
            [
                StatementRole::Label,
                StatementRole::Quarantine,
                StatementRole::Valid
            ]
        );
        assert_eq!(plan.valid_table, "poc.staging__orders.orders__valid");
        assert_eq!(
            plan.quarantine_table,
            "poc.staging__orders.orders__quarantine"
        );
        let intermediate = "poc.staging__orders._quarantine_labels_t0k3n";
        assert_eq!(plan.statements[0].target, intermediate);
        let drop = plan
            .drop_intermediate
            .expect("split drops its intermediate");
        assert_eq!(drop.role, StatementRole::DropLabels);
        assert_eq!(drop.target, intermediate);
        assert_eq!(drop.sql, format!("DROP TABLE IF EXISTS {intermediate}"));
    }

    /// The whole statement text for one assertion, so the three statements
    /// can be read side by side.
    #[test]
    fn not_null_split_shape() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = compile_with_token(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
            "t0k3n",
        )
        .unwrap()
        .unwrap();
        let sql: Vec<&str> = plan.statements.iter().map(|s| s.sql.as_str()).collect();
        assert_eq!(
            sql,
            [
                // A plain CREATE TABLE: it fails rather than replace a table
                // that already has this name.
                "CREATE TABLE poc.staging__orders._quarantine_labels_t0k3n AS\n\
                 SELECT *, CASE WHEN NOT (customer_id IS NOT NULL) \
                 THEN '_error_not_null_customer_id' END AS _ql0_t0k3n \
                 FROM poc.staging__orders.orders",
                "CREATE OR REPLACE TABLE poc.staging__orders.orders__quarantine AS\n\
                 SELECT * EXCLUDE (_ql0_t0k3n), \
                 _ql0_t0k3n AS _error_not_null_customer_id \
                 FROM poc.staging__orders._quarantine_labels_t0k3n \
                 WHERE _ql0_t0k3n IS NOT NULL",
                "CREATE OR REPLACE TABLE poc.staging__orders.orders__valid AS\n\
                 SELECT * EXCLUDE (_ql0_t0k3n) \
                 FROM poc.staging__orders._quarantine_labels_t0k3n \
                 WHERE _ql0_t0k3n IS NULL",
            ]
        );
    }

    /// #1937: each predicate text appears in exactly one statement of the
    /// plan, whatever the mode and whatever the assertion kind.
    ///
    /// `split` used to splice the predicate into the quarantine CTAS and
    /// again into the valid CTAS. The two statements ran separately, so a
    /// source row written between them, or a clock read, could put one row
    /// in both outputs or in neither.
    #[test]
    fn every_predicate_is_evaluated_in_exactly_one_statement() {
        let mut filtered = assertion(None, TestType::NotNull, Some("email"), TestSeverity::Error);
        filtered.test.filter = Some("region = 'US'".into());
        let assertions = vec![
            assertion(
                Some("nn"),
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
            assertion(
                None,
                TestType::Expression {
                    expression: "amount >= 0".into(),
                },
                None,
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::InRange {
                    min: Some("0".into()),
                    max: Some("10".into()),
                },
                Some("qty"),
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::RegexMatch {
                    pattern: "^[a-z]+$".into(),
                },
                Some("code"),
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::NotInFuture,
                Some("created_at"),
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::OlderThanNDays { days: 7 },
                Some("updated_at"),
                TestSeverity::Error,
            ),
            filtered,
        ];
        // One fragment per assertion that only its predicate contains. The
        // first assertion is named so its label, which the split's WHERE
        // clauses repeat, does not contain `customer_id IS NOT NULL`.
        let fragments = [
            "customer_id IS NOT NULL",
            "status IN ('pending')",
            "amount >= 0",
            "qty < 0",
            "regexp_matches(code",
            "created_at <=",
            "updated_at <=",
            "region = 'US'",
        ];
        for mode in [
            QuarantineMode::Split,
            QuarantineMode::Drop,
            QuarantineMode::Tag,
        ] {
            let cfg = QuarantineConfig {
                mode,
                ..split_config()
            };
            let plan = compile_quarantine_sql(
                &assertions,
                "orders",
                &table(),
                &TestDialect,
                &cfg,
                &SourceColumns::read(Vec::new()),
            )
            .unwrap_or_else(|e| panic!("{mode:?}: {e:?}"))
            .expect("a plan");
            for fragment in fragments {
                let total: usize = plan
                    .statements
                    .iter()
                    .map(|s| s.sql.matches(fragment).count())
                    .sum();
                assert_eq!(
                    total, 1,
                    "{mode:?}: `{fragment}` must be evaluated exactly once, in one statement; \
                     plan: {:#?}",
                    plan.statements
                );
            }
        }
    }

    /// A `split` on a warehouse with no star exclusion is refused before any
    /// SQL is produced, and the refusal names the warehouse.
    ///
    /// The valid table is written as the intermediate table minus its label
    /// columns. Without star exclusion the only other way is a column list,
    /// and that needs a round trip to read the columns first.
    #[test]
    fn split_is_refused_on_a_dialect_without_star_exclusion() {
        use crate::traits::LiteralEscape;
        use crate::traits::test_dialects::StubDialect;

        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let dialect = StubDialect(LiteralEscape::Standard);
        assert!(dialect.star_excluding(&["x"]).is_none(), "precondition");

        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &dialect,
            &split_config(),
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        assert!(
            matches!(err, QuarantineError::SplitNeedsStarExclusion { .. }),
            "{err:?}"
        );

        // `drop` and `tag` never needed it.
        for mode in [QuarantineMode::Drop, QuarantineMode::Tag] {
            let cfg = QuarantineConfig {
                mode,
                ..split_config()
            };
            compile_quarantine_sql(
                &assertions,
                "orders",
                &table(),
                &dialect,
                &cfg,
                &SourceColumns::read(Vec::new()),
            )
            .unwrap_or_else(|e| panic!("{mode:?} does not exclude columns: {e:?}"))
            .expect("a plan");
        }
    }

    /// Two plans for the same table name different working objects, and the
    /// public entry point uses a token of the documented shape.
    ///
    /// A fixed name let a run replace and then drop a table that happened to
    /// carry it, let two runs of one pipeline share (and drop) one
    /// intermediate table, and let a source column with the fixed working
    /// name steer the split.
    #[test]
    fn each_split_plan_names_its_working_objects_with_a_fresh_token() {
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let plan = || {
            compile_quarantine_sql(
                &assertions,
                "orders",
                &table(),
                &TestDialect,
                &split_config(),
                &SourceColumns::read(Vec::new()),
            )
            .unwrap()
            .unwrap()
        };
        let (a, b) = (plan(), plan());
        let target = |p: &QuarantinePlan| p.statements[0].target.clone();
        assert_ne!(target(&a), target(&b), "two plans must not share a table");

        let prefix = "poc.staging__orders._quarantine_labels_";
        let token = target(&a)
            .strip_prefix(prefix)
            .unwrap_or_else(|| panic!("{}", target(&a)))
            .to_string();
        assert_eq!(token.len(), SPLIT_TOKEN_LEN, "{token}");
        assert!(token.chars().all(|c| c.is_ascii_hexdigit()), "{token}");
        assert!(
            a.statements[0].sql.contains(&format!("AS _ql0_{token} ")),
            "the label columns carry the same token: {}",
            a.statements[0].sql
        );
        assert_eq!(
            a.drop_intermediate.as_ref().map(|d| d.target.clone()),
            Some(target(&a)),
            "the drop names the table this plan creates"
        );
    }

    /// The working names do not grow with the source's names.
    ///
    /// A 210-character table and a 200-character column are legal on
    /// Snowflake and Databricks (255-character limit). Names built by
    /// appending a suffix and a 32-digit token to them would not be.
    #[test]
    fn split_working_names_have_a_fixed_length() {
        let long_table = "t".repeat(210);
        let long_column = "c".repeat(200);
        let table_ref = TableRef {
            catalog: "poc".into(),
            schema: "staging__orders".into(),
            table: long_table.clone(),
        };
        let mut a = assertion(
            None,
            TestType::NotNull,
            Some(&long_column),
            TestSeverity::Error,
        );
        a.table = long_table.clone();
        let plan = compile_quarantine_sql(
            &[a],
            &long_table,
            &table_ref,
            &TestDialect,
            &split_config(),
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        let intermediate = plan.statements[0]
            .target
            .strip_prefix("poc.staging__orders.")
            .expect("schema-qualified");
        assert_eq!(
            intermediate.len(),
            SPLIT_TABLE_PREFIX.len() + SPLIT_TOKEN_LEN,
            "{intermediate}"
        );
        assert!(intermediate.len() <= 255, "{intermediate}");
        let token = &intermediate[SPLIT_TABLE_PREFIX.len()..];
        assert!(
            plan.statements[0]
                .sql
                .contains(&format!("END AS _ql0_{token} ")),
            "{}",
            plan.statements[0].sql
        );
    }

    /// Suffixes that give two written tables one name are refused, in the
    /// modes that write both, and only there.
    ///
    /// An empty `suffix_valid` made the valid CTAS replace the source; equal
    /// suffixes made the valid CTAS replace the quarantined rows. Both ran
    /// without complaint, and the second put failing rows in neither output.
    #[test]
    fn suffixes_that_name_two_tables_alike_are_refused() {
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        )];
        let compile = |mode, valid: &str, quarantine: &str| {
            let cfg = QuarantineConfig {
                mode,
                suffix_valid: valid.into(),
                suffix_quarantine: quarantine.into(),
                ..split_config()
            };
            compile_quarantine_sql(
                &assertions,
                "orders",
                &table(),
                &TestDialect,
                &cfg,
                &SourceColumns::read(Vec::new()),
            )
        };
        let collision = |r: Result<Option<QuarantinePlan>, QuarantineError>| {
            matches!(r, Err(QuarantineError::TableNameCollision { .. }))
        };

        use QuarantineMode::{Drop, Split, Tag};
        for (mode, valid, quarantine, why) in [
            (Split, "", "__quarantine", "valid replaces the source"),
            (Split, "__valid", "", "quarantine replaces the source"),
            (Split, "__out", "__out", "valid replaces quarantine"),
            (Drop, "", "__quarantine", "valid replaces the source"),
        ] {
            assert!(
                collision(compile(mode, valid, quarantine)),
                "{mode:?} {valid:?}/{quarantine:?}: {why}"
            );
        }

        // The fix names the suffix to change for that pair, so `drop` is
        // never told to change the quarantine suffix it does not use.
        let message = |mode, valid: &str, quarantine: &str| {
            compile(mode, valid, quarantine).unwrap_err().to_string()
        };
        assert!(message(Drop, "", "").ends_with("set suffix_valid to a non-empty value"));
        assert!(message(Split, "__v", "").ends_with("set suffix_quarantine to a non-empty value"));
        assert!(message(Split, "__x", "__x").ends_with("different values"));

        // Controls: tables a mode does not write do not collide, and names
        // that differ only in case are two tables on Snowflake, which quotes
        // them.
        for (mode, valid, quarantine) in [
            (Split, "__valid", "__quarantine"),
            (Split, "__OUT", "__out"),
            (Drop, "__out", "__out"),
            (Drop, "__valid", ""),
            (Tag, "", ""),
        ] {
            compile(mode, valid, quarantine)
                .unwrap_or_else(|e| panic!("{mode:?} {valid:?}/{quarantine:?}: {e}"))
                .expect("a plan");
        }
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // Valid predicate: NULL or in list.
        assert!(
            plan.statements[0]
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        assert!(plan.statements[0].sql.contains("'it''s'"));
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // COALESCE makes NULL `amount >= 0` resolve to TRUE (preserves
        // existing `WHERE NOT (expr)` semantic that excludes NULL).
        assert!(
            plan.statements[0]
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
        let plan = compile_with_token(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
            "t0k3n",
        )
        .unwrap()
        .unwrap();
        // Split combines the labels: any set goes to quarantine, none set to valid.
        let working = "_ql0_t0k3n, _ql1_t0k3n";
        let quarantine_sql = &plan.statements[1].sql;
        assert!(
            quarantine_sql.contains(&format!(
                "SELECT * EXCLUDE ({working}), \
                 _ql0_t0k3n AS _error_not_null_customer_id, \
                 _ql1_t0k3n AS _error_accepted_values_status "
            )),
            "{quarantine_sql}"
        );
        assert!(quarantine_sql.ends_with(
            "WHERE _ql0_t0k3n IS NOT NULL \
             OR _ql1_t0k3n IS NOT NULL"
        ));
        let valid_sql = &plan.statements[2].sql;
        assert!(
            valid_sql.contains(&format!("SELECT * EXCLUDE ({working}) FROM")),
            "{valid_sql}"
        );
        assert!(valid_sql.ends_with(
            "WHERE _ql0_t0k3n IS NULL \
             AND _ql1_t0k3n IS NULL"
        ));

        // Drop has no labels; it ANDs the predicates in its one statement.
        let drop_cfg = QuarantineConfig {
            mode: QuarantineMode::Drop,
            ..split_config()
        };
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &drop_cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        let valid_sql = &plan.statements[0].sql;
        assert!(valid_sql.contains("customer_id IS NOT NULL AND"));
        assert!(valid_sql.contains("status IS NULL OR status IN ('pending')"));
    }

    /// A clock is accepted in every mode, `split` included: in the
    /// expression, in the filter, and in the predicates Rocky generates for
    /// `not_in_future` and `older_than_n_days`.
    ///
    /// `split` refused all four while it evaluated each predicate in two
    /// statements, where a boundary row could be called invalid by the first
    /// and valid by the second (#1922). It now evaluates each predicate once,
    /// in the label statement (#1937), so the refusal would be a false one.
    /// `every_predicate_is_evaluated_in_exactly_one_statement` pins the
    /// reason; this pins the consequence.
    #[test]
    fn a_clock_is_accepted_in_every_mode() {
        let mut clock_filter = assertion(
            None,
            TestType::NotNull,
            Some("customer_id"),
            TestSeverity::Error,
        );
        clock_filter.test.filter = Some("created_at <= now()".to_string());
        let cases = [
            assertion(
                None,
                TestType::Expression {
                    expression: "created_at <= now()".to_string(),
                },
                None,
                TestSeverity::Error,
            ),
            clock_filter,
            assertion(
                None,
                TestType::NotInFuture,
                Some("created_at"),
                TestSeverity::Error,
            ),
            assertion(
                None,
                TestType::OlderThanNDays { days: 7 },
                Some("created_at"),
                TestSeverity::Error,
            ),
        ];
        for case in cases {
            for mode in [
                QuarantineMode::Split,
                QuarantineMode::Drop,
                QuarantineMode::Tag,
            ] {
                let cfg = QuarantineConfig {
                    mode,
                    ..split_config()
                };
                compile_quarantine_sql(
                    std::slice::from_ref(&case),
                    "orders",
                    &table(),
                    &TestDialect,
                    &cfg,
                    &SourceColumns::read(Vec::new()),
                )
                .unwrap_or_else(|e| panic!("{mode:?} must accept {:?}: {e:?}", case.test))
                .expect("a quarantinable assertion produces a plan");
            }
        }
    }

    /// Control for the test above: `split` still refuses what every mode
    /// refuses. Without it, a change that stopped validating split
    /// predicates altogether would pass the test above.
    #[test]
    fn split_still_refuses_a_disallowed_function() {
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "my_udf(created_at) IS NOT NULL".to_string(),
            },
            None,
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &split_config(),
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("my_udf"), "{err}");
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        assert_eq!(plan.statements.len(), 1);
        assert_eq!(plan.statements[0].role, StatementRole::Valid);
        assert_eq!(
            plan.statements[0].sql,
            "CREATE OR REPLACE TABLE poc.staging__orders.orders__valid AS\n\
             SELECT * FROM poc.staging__orders.orders WHERE customer_id IS NOT NULL"
        );
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // The quarantine table is where the labels carry their own names.
        let sql = &plan.statements[1].sql;
        assert!(sql.contains("AS _error_not_null_customer_id,"), "{sql}");
        assert!(sql.contains("AS _error_not_null_customer_id_2 "), "{sql}");
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // Quarantine runs before valid so a partial failure leaves a stray
        // quarantine table rather than a stale valid table.
        assert_eq!(plan.statements[1].role, StatementRole::Quarantine);
        assert_eq!(plan.statements[2].role, StatementRole::Valid);
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
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .err();
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
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .err();
        assert!(err.is_some());
    }

    // ----- where-clause filter handling -----

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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // Label CTAS: (amount IS NULL OR NOT (amount < 0 OR amount > 1000))
        assert!(
            plan.statements[0]
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // Label CTAS: (email IS NULL OR regexp_matches(email, '^[a-z]+$'))
        assert!(
            plan.statements[0]
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
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        // Out-of-scope rows (filter false/null) pass unconditionally:
        // (CASE WHEN (region = 'US') THEN (customer_id IS NOT NULL) ELSE TRUE END)
        assert!(
            plan.statements[0]
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
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
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
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
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

    // ----- Dialect-owned literal encoding (issue #1596) -----

    /// Each `accepted_values` entry is encoded by the dialect's own lexer
    /// rule: a backslash stands for itself under `Standard` (DuckDB, Trino)
    /// and is doubled under `Backslash` (Snowflake, Databricks, BigQuery),
    /// where the quote is `\'` rather than `''`.
    #[test]
    fn quarantine_accepted_values_encodes_a_backslash_and_a_quote_per_dialect() {
        use crate::traits::LiteralEscape;
        use crate::traits::test_dialects::StubDialect;

        // `drop`, because `StubDialect` has no star exclusion and so cannot
        // `split`. The encoding under test is in the predicate either way.
        let cfg = QuarantineConfig {
            mode: QuarantineMode::Drop,
            ..split_config()
        };
        let assertions = vec![assertion(
            None,
            TestType::AcceptedValues {
                values: vec!["ok".into(), r"trailing\".into(), "it's".into()],
            },
            Some("status"),
            TestSeverity::Error,
        )];

        let standard = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &StubDialect(LiteralEscape::Standard),
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        let sql = &standard.statements[0].sql;
        assert!(
            sql.contains(r"(status IS NULL OR status IN ('ok', 'trailing\', 'it''s'))"),
            "{sql}"
        );

        let backslash = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &StubDialect(LiteralEscape::Backslash),
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        let sql = &backslash.statements[0].sql;
        assert!(
            sql.contains(r"(status IS NULL OR status IN ('ok', 'trailing\\', 'it\'s'))"),
            "{sql}"
        );
    }

    // ----- Statement-terminator refusal (issue #1524) -----

    #[test]
    fn quarantine_expression_refuses_a_statement_terminator() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "1=1), TRUE); SELECT 1; --".into(),
            },
            None,
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("statement terminator"), "{msg}");
        assert!(msg.contains("`expression`"), "{msg}");
    }

    /// A quarantine `expression` runs inside the quarantine CTAS, with
    /// warehouse credentials. #1820 gave the CHECKS path a parse-and-allowlist
    /// gate (`validate_check_expression`, called at `tests.rs` before the
    /// `NOT (...)` splice) so a predicate cannot call a content-reading
    /// function. Quarantine spliced the same user SQL with only the
    /// terminator check, so the same predicate was still reachable here.
    ///
    /// A function off the allowlist sits in scalar position and is refused by
    /// name — the content boundary #1524 asked for.
    #[test]
    fn quarantine_expression_refuses_a_disallowed_function() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "my_udf(id) IS NOT NULL".into(),
            },
            None,
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("my_udf"),
            "the refusal must NAME the function an operator has to remove: {msg}"
        );
    }

    /// A subquery reaches any table the run's credentials can see, which is a
    /// wider surface than the model under test. Refused for the same reason
    /// the checks path refuses it.
    #[test]
    fn quarantine_expression_refuses_a_subquery() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "customer_id IN (SELECT 1)".into(),
            },
            None,
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_lowercase().contains("subquery"),
            "the refusal must say a subquery is the problem: {msg}"
        );
    }

    /// The gate must not refuse ordinary predicates. Without this, the two
    /// refusals above would also pass on a change that refused everything.
    #[test]
    fn quarantine_expression_still_accepts_an_ordinary_predicate() {
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "total >= 0 AND status <> 'void'".into(),
            },
            None,
            TestSeverity::Error,
        )];
        compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect("an ordinary predicate must still compile");
    }

    /// A dialect identical to `TestDialect` in every respect EXCEPT its
    /// name, so a test using both isolates the name as the only variable.
    struct SnowflakeNamed;

    impl SqlDialect for SnowflakeNamed {
        fn name(&self) -> &'static str {
            "snowflake"
        }
        fn literal_escape(&self) -> crate::traits::LiteralEscape {
            TestDialect.literal_escape()
        }
        fn format_table_ref(&self, c: &str, s: &str, t: &str) -> AdapterResult<String> {
            TestDialect.format_table_ref(c, s, t)
        }
        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            TestDialect.create_table_as(target, select_sql)
        }
        fn insert_into(&self, a: &str, b: &str) -> String {
            TestDialect.insert_into(a, b)
        }
        fn merge_into(
            &self,
            a: &str,
            b: &str,
            c: &[std::sync::Arc<str>],
            d: &ColumnSelection,
        ) -> AdapterResult<String> {
            TestDialect.merge_into(a, b, c, d)
        }
        fn select_clause(
            &self,
            a: &ColumnSelection,
            b: &[MetadataColumn],
        ) -> AdapterResult<String> {
            TestDialect.select_clause(a, b)
        }
        fn watermark_where(
            &self,
            a: &str,
            b: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> AdapterResult<String> {
            TestDialect.watermark_where(a, b)
        }
        fn describe_table_sql(&self, t: &str) -> String {
            TestDialect.describe_table_sql(t)
        }
        fn drop_table_sql(&self, t: &str) -> String {
            TestDialect.drop_table_sql(t)
        }
        fn create_catalog_sql(&self, a: &str) -> Option<AdapterResult<String>> {
            TestDialect.create_catalog_sql(a)
        }
        fn create_schema_sql(&self, a: &str, b: &str) -> Option<AdapterResult<String>> {
            TestDialect.create_schema_sql(a, b)
        }
        fn tablesample_clause(&self, a: u32) -> Option<String> {
            TestDialect.tablesample_clause(a)
        }
        // Delegated too. Without it `split` would refuse this dialect for
        // having no star exclusion, and the refusals below would pass
        // whatever the parser did.
        fn star_excluding(&self, columns: &[&str]) -> Option<String> {
            TestDialect.star_excluding(columns)
        }
        fn insert_overwrite_partition(
            &self,
            a: &str,
            b: &str,
            c: &str,
        ) -> AdapterResult<Vec<String>> {
            TestDialect.insert_overwrite_partition(a, b, c)
        }
    }

    /// The call site really threads ITS dialect through to the parser.
    ///
    /// Every other test here uses `TestDialect`, which does not override
    /// `name()`, so it takes the trait default `"unknown"` and
    /// `dialect_for` hands back `GenericDialect`. That means none of them
    /// could catch a call site that passed the WRONG dialect — they would
    /// all parse generically and agree.
    ///
    /// `a->'k'` is the discriminator: `GenericDialect` parses it,
    /// `SnowflakeDialect` does not. So the same expression at the same call
    /// site must be accepted under one and refused under the other, and that
    /// difference can only come from the name being threaded.
    #[test]
    fn the_call_site_threads_its_own_dialect_to_the_parser() {
        // Everything but `name()` delegates to TestDialect, so the ONLY
        // difference between the two runs below is the dialect name.

        let cfg = split_config();
        let assertions = || {
            vec![assertion(
                None,
                TestType::Expression {
                    expression: "(a->'k') IS NOT NULL".into(),
                },
                None,
                TestSeverity::Error,
            )]
        };

        // TestDialect -> name() defaults to "unknown" -> GenericDialect, which
        // parses the operator.
        compile_quarantine_sql(
            &assertions(),
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect("the generic parser accepts this operator");

        // The same expression, same call site, a dialect NAMED snowflake.
        // Refused under snowflake, where `->` is the LAMBDA arrow rather than
        // a JSON operator — so the same text parses to a different AST and
        // the walker refuses it. The reason does not matter here; the
        // DIFFERENCE does, and it can only come from the name being threaded.
        compile_quarantine_sql(
            &assertions(),
            "orders",
            &table(),
            &SnowflakeNamed,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect_err("the snowflake parser must not accept this operator");
    }

    /// The same proof for the quarantine FILTER route.
    ///
    /// The expression route has its own version above. Four of the five new
    /// call sites were covered only by tests using the default-named test
    /// dialect, so replacing their dialect argument with generic would have
    /// left everything green — the review's finding 4.
    #[test]
    fn the_filter_route_threads_its_own_dialect_to_the_parser() {
        let cfg = split_config();
        let assertions = || {
            vec![assertion_with_filter(
                TestType::NotNull,
                Some("customer_id"),
                Some("(a->'k') IS NOT NULL"),
            )]
        };
        compile_quarantine_sql(
            &assertions(),
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect("the generic parser accepts this operator");
        compile_quarantine_sql(
            &assertions(),
            "orders",
            &table(),
            &SnowflakeNamed,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect_err("the snowflake parser must not accept it");
    }

    /// The `filter` reaches the same CTAS as the predicate, so it gets the
    /// same boundary. Guarding only `expression` would close one door and
    /// leave an identical one beside it — which is what the independent review
    /// of #1922 found.
    #[test]
    fn quarantine_filter_refuses_a_disallowed_function() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("my_udf(id) IS NOT NULL"),
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("my_udf"), "must name the function: {msg}");
        assert!(msg.contains("`filter`"), "must name the field: {msg}");
    }

    /// A refused quarantine `filter` is described as a filter, not "an
    /// expression check" — the noun used to come from the evaluation mode
    /// (`SinglePredicate`), which the quarantine `expression` shares, rather
    /// than the field kind (#1971).
    #[test]
    fn quarantine_filter_is_described_as_a_filter() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("amount >"),
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("A filter is one boolean expression over the row's columns"),
            "{msg}"
        );
        assert!(!msg.contains("An expression check"), "{msg}");
    }

    /// The control: an ordinary filter still compiles.
    #[test]
    fn quarantine_filter_still_accepts_an_ordinary_predicate() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("status <> 'void'"),
        )];
        compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .expect("an ordinary filter must still compile");
    }

    #[test]
    fn quarantine_filter_refuses_a_statement_terminator() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("1=1); SELECT 1; --"),
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("statement terminator"), "{msg}");
        assert!(msg.contains("`filter`"), "{msg}");
    }

    #[test]
    fn quarantine_expression_refuses_an_unbalanced_quote() {
        // An odd quote count in the predicate could pair with a quote later
        // in the same statement, such as the label literal after it.
        let cfg = split_config();
        let assertions = vec![assertion(
            None,
            TestType::Expression {
                expression: "note <> 'x".into(),
            },
            None,
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap_err();
        assert!(err.to_string().contains("unterminated"), "{err}");
    }

    #[test]
    fn quarantine_still_accepts_quoted_semicolons() {
        let cfg = split_config();
        let assertions = vec![assertion_with_filter(
            TestType::NotNull,
            Some("customer_id"),
            Some("region = 'US;CA'"),
        )];
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &cfg,
            &SourceColumns::read(Vec::new()),
        )
        .unwrap()
        .unwrap();
        assert!(
            plan.statements[0].sql.contains("region = 'US;CA'"),
            "{}",
            plan.statements[0].sql
        );
    }

    fn mode_config(mode: QuarantineMode) -> QuarantineConfig {
        QuarantineConfig {
            mode,
            ..split_config()
        }
    }

    fn not_null_name() -> Vec<QualityAssertion> {
        vec![assertion(
            None,
            TestType::NotNull,
            Some("name"),
            TestSeverity::Error,
        )]
    }

    fn compile_against(
        mode: QuarantineMode,
        source: &SourceColumns,
    ) -> Result<Option<QuarantinePlan>, QuarantineError> {
        compile_quarantine_sql(
            &not_null_name(),
            "orders",
            &table(),
            &TestDialect,
            &mode_config(mode),
            source,
        )
    }

    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|c| (*c).to_string()).collect()
    }

    /// A source with `columns`, and a record owning `owned` that was written
    /// when the table had exactly those columns.
    fn source(columns: &[&str], owned: &[&str]) -> SourceColumns {
        SourceColumns {
            columns: Some(strings(columns)),
            ownership: (!owned.is_empty()).then(|| QuarantineOwnership {
                labels: strings(owned),
                columns: strings(columns),
            }),
        }
    }

    /// #2065 part 1: a user's own `_error_<label>` column is never
    /// overwritten. With no ownership record, `tag` and `split` refuse before
    /// any statement exists, and the error names the column and the label.
    #[test]
    fn a_source_column_named_like_a_label_is_refused_without_ownership() {
        for mode in [QuarantineMode::Tag, QuarantineMode::Split] {
            let err = compile_against(mode, &source(&["id", "name", "_ERROR_not_null_name"], &[]))
                .unwrap_err();
            match &err {
                QuarantineError::LabelColumnCollision { label, column, .. } => {
                    assert_eq!(label, "_error_not_null_name");
                    assert_eq!(column, "_ERROR_not_null_name");
                }
                other => panic!("{mode:?}: expected LabelColumnCollision, got {other:?}"),
            }
            assert!(err.to_string().contains("RENAME COLUMN"), "{err}");
        }
    }

    /// `split` never writes its source, so even a recorded label is not its
    /// to replace in the quarantine table.
    #[test]
    fn split_refuses_a_label_collision_even_with_an_ownership_record() {
        let err = compile_against(
            QuarantineMode::Split,
            &source(&["id", "_error_not_null_name"], &["_error_not_null_name"]),
        )
        .unwrap_err();
        assert!(
            matches!(err, QuarantineError::LabelColumnCollision { .. }),
            "{err:?}"
        );
    }

    /// `tag` run two: the column run one wrote is recorded as Rocky's, so it
    /// is left out of `*` and written afresh, not carried beside the new one.
    #[test]
    fn tag_replaces_a_label_column_it_recorded_writing() {
        let plan = compile_against(
            QuarantineMode::Tag,
            &source(
                &["id", "name", "_error_not_null_name", "_error_old"],
                &["_error_not_null_name", "_error_old", "_error_gone"],
            ),
        )
        .unwrap()
        .unwrap();
        let sql = &plan.statements[0].sql;
        assert!(
            sql.contains("SELECT * EXCLUDE (_error_not_null_name), CASE WHEN NOT"),
            "{sql}"
        );
        // This run's label, plus the earlier label the table still carries;
        // a recorded label the table no longer has is dropped from the record.
        let after = plan.ownership_after.expect("tag records ownership");
        assert_eq!(
            after.labels,
            strings(&["_error_not_null_name", "_error_old"])
        );
        // Bound to the columns the rewrite leaves: replaced label last.
        assert_eq!(
            after.columns,
            strings(&["id", "name", "_error_old", "_error_not_null_name"])
        );
    }

    /// The reviewer's case for #2065: run one recorded the label, then the
    /// table was replaced out of band by one carrying a user's own column of
    /// that name. Its columns no longer match the record, so nothing is
    /// proven and the column is refused, not overwritten.
    #[test]
    fn tag_refuses_a_recorded_label_once_the_table_was_reshaped() {
        let reshaped = SourceColumns {
            columns: Some(strings(&["id", "name", "note", "_error_not_null_name"])),
            ownership: Some(QuarantineOwnership {
                labels: strings(&["_error_not_null_name"]),
                columns: strings(&["id", "name", "_error_not_null_name"]),
            }),
        };
        let err = compile_against(QuarantineMode::Tag, &reshaped).unwrap_err();
        assert!(
            matches!(err, QuarantineError::LabelColumnCollision { .. }),
            "{err:?}"
        );
    }

    /// Labels differing only by case name one column on an unquoted,
    /// case-insensitive warehouse, so the second is suffixed.
    #[test]
    fn labels_differing_only_by_case_are_kept_distinct() {
        let assertions = vec![
            assertion(
                Some("Amount_ok"),
                TestType::NotNull,
                Some("a"),
                TestSeverity::Error,
            ),
            assertion(
                Some("amount_OK"),
                TestType::NotNull,
                Some("b"),
                TestSeverity::Error,
            ),
        ];
        let plan = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &mode_config(QuarantineMode::Tag),
            &source(&["id", "a", "b"], &[]),
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            plan.ownership_after.unwrap().labels,
            strings(&["_error_Amount_ok", "_error_amount_OK_2"])
        );
    }

    /// Two source columns that both match a label ignoring case: Rocky cannot
    /// tell which one it wrote, so it replaces neither.
    #[test]
    fn tag_refuses_a_label_matched_by_two_columns() {
        let err = compile_against(
            QuarantineMode::Tag,
            &source(
                &["_error_not_null_name", "_ERROR_NOT_NULL_NAME"],
                &["_error_not_null_name"],
            ),
        )
        .unwrap_err();
        assert!(
            matches!(err, QuarantineError::LabelColumnCollision { .. }),
            "{err:?}"
        );
    }

    #[test]
    fn tag_and_split_refuse_unread_source_columns_and_drop_needs_none() {
        for mode in [QuarantineMode::Tag, QuarantineMode::Split] {
            let err = compile_against(mode, &SourceColumns::default()).unwrap_err();
            assert!(
                matches!(err, QuarantineError::SourceColumnsUnread { .. }),
                "{mode:?}: {err:?}"
            );
        }
        let plan = compile_against(QuarantineMode::Drop, &SourceColumns::default())
            .unwrap()
            .unwrap();
        assert!(plan.ownership_after.is_none());
    }

    #[test]
    fn tag_without_a_collision_writes_plain_star_and_records_its_labels() {
        let plan = compile_against(QuarantineMode::Tag, &source(&["id", "name"], &[]))
            .unwrap()
            .unwrap();
        assert!(
            plan.statements[0].sql.contains("SELECT *, CASE WHEN NOT"),
            "{}",
            plan.statements[0].sql
        );
        assert_eq!(
            plan.ownership_after,
            Some(QuarantineOwnership {
                labels: strings(&["_error_not_null_name"]),
                columns: strings(&["id", "name", "_error_not_null_name"]),
            })
        );
    }

    /// #2065 part 2: a 250-character table plus `__valid` is 257 characters,
    /// over the 255 bound. Refused at compile time, before any statement.
    #[test]
    fn an_output_table_name_over_the_identifier_limit_is_refused() {
        let long = "t".repeat(250);
        let assertions = vec![QualityAssertion {
            table: long.clone(),
            ..not_null_name().remove(0)
        }];
        let err = compile_quarantine_sql(
            &assertions,
            &long,
            &TableRef {
                table: long.clone(),
                ..table()
            },
            &TestDialect,
            &mode_config(QuarantineMode::Drop),
            &SourceColumns::default(),
        )
        .unwrap_err();
        match err {
            QuarantineError::GeneratedNameTooLong { what, len, max, .. } => {
                assert_eq!(what, "output table");
                assert_eq!(len, 257);
                assert_eq!(max, 255);
            }
            other => panic!("expected GeneratedNameTooLong, got {other:?}"),
        }
    }

    /// A 240-character column gives `_error_not_null_<column>` of 256
    /// characters. Refused at compile time.
    #[test]
    fn a_label_name_over_the_identifier_limit_is_refused() {
        let column = "c".repeat(240);
        let assertions = vec![assertion(
            None,
            TestType::NotNull,
            Some(&column),
            TestSeverity::Error,
        )];
        let err = compile_quarantine_sql(
            &assertions,
            "orders",
            &table(),
            &TestDialect,
            &mode_config(QuarantineMode::Tag),
            &source(&["id"], &[]),
        )
        .unwrap_err();
        match err {
            QuarantineError::GeneratedNameTooLong { what, len, .. } => {
                assert_eq!(what, "label column");
                assert_eq!(len, 256);
            }
            other => panic!("expected GeneratedNameTooLong, got {other:?}"),
        }
    }

    /// The bound is inclusive: a 248-character table plus `__valid` is
    /// exactly 255 characters and compiles.
    #[test]
    fn an_output_table_name_at_the_identifier_limit_compiles() {
        let long = "t".repeat(248);
        let assertions = vec![QualityAssertion {
            table: long.clone(),
            ..not_null_name().remove(0)
        }];
        let plan = compile_quarantine_sql(
            &assertions,
            &long,
            &TableRef {
                table: long.clone(),
                ..table()
            },
            &TestDialect,
            &mode_config(QuarantineMode::Drop),
            &SourceColumns::default(),
        )
        .unwrap()
        .unwrap();
        assert!(plan.valid_table.ends_with(&format!("{long}__valid")));
        assert!(
            plan.quarantine_table.is_empty(),
            "drop writes no quarantine table"
        );
    }

    /// `tag` writes neither suffixed table, so a table name that would make
    /// them too long is not refused for them.
    #[test]
    fn tag_is_not_refused_over_suffixed_tables_it_does_not_write() {
        let long = "t".repeat(250);
        let assertions = vec![QualityAssertion {
            table: long.clone(),
            ..not_null_name().remove(0)
        }];
        compile_quarantine_sql(
            &assertions,
            &long,
            &TableRef {
                table: long.clone(),
                ..table()
            },
            &TestDialect,
            &mode_config(QuarantineMode::Tag),
            &source(&["id", "name"], &[]),
        )
        .unwrap()
        .unwrap();
    }
}
