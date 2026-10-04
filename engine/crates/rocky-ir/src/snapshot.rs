//! Snapshot (SCD Type 2) materialization vocabulary for transformation models.
//!
//! A `type = "snapshot"` model keeps the history of its SELECT's rows: every
//! change to a key opens a new version row and closes the previous one. These
//! types are the IR shape of that config ([`SnapshotSpec`], carried by
//! [`crate::MaterializationStrategy::Snapshot`]) plus the pieces the TOML
//! sidecar shares with it ([`SnapshotHardDeletes`], [`SnapshotMetaColumns`]).
//!
//! The semantics follow dbt snapshots
//! (<https://docs.getdbt.com/docs/build/snapshots>): a `timestamp` strategy
//! keyed on an `updated_at` column or a `check` strategy over a column list,
//! three `hard_deletes` modes, and renameable metadata columns. The defaults
//! are Rocky's own column names, the same ones the `snapshot` pipeline writes
//! (`valid_from`, `valid_to`, `is_current`, `snapshot_id`).

use std::sync::Arc;

use rocky_sql::validation;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// What a snapshot does with a key that disappears from the model's result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotHardDeletes {
    /// Keep the last version current. The default, as in dbt.
    #[default]
    Ignore,
    /// Close the current version: set `valid_to` and clear `is_current`.
    Invalidate,
    /// Close the current version and insert a deletion-marker version with
    /// `is_deleted = TRUE`.
    NewRecord,
}

impl SnapshotHardDeletes {
    /// The TOML spelling.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Ignore => "ignore",
            Self::Invalidate => "invalidate",
            Self::NewRecord => "new_record",
        }
    }
}

fn default_valid_from() -> String {
    "valid_from".to_string()
}
fn default_valid_to() -> String {
    "valid_to".to_string()
}
fn default_is_current() -> SnapshotFlagColumn {
    SnapshotFlagColumn::Name("is_current".to_string())
}
fn default_scd_id() -> String {
    "snapshot_id".to_string()
}
fn default_is_deleted() -> String {
    "is_deleted".to_string()
}

/// The `is_current` metadata column: a name, or `false` for none.
///
/// A snapshot table built by dbt has no `is_current` column. Setting
/// `is_current = false` lets Rocky continue such a table: a version is then
/// current when its `valid_to` is NULL (or equals `valid_to_current`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
pub enum SnapshotFlagColumn {
    /// Write the flag under this column name.
    Name(String),
    /// `false`: write no flag column. `true` means the default name.
    Enabled(bool),
}

impl SnapshotFlagColumn {
    /// The column name, or `None` when disabled.
    pub fn name(&self) -> Option<&str> {
        match self {
            Self::Name(n) => Some(n.as_str()),
            Self::Enabled(true) => Some("is_current"),
            Self::Enabled(false) => None,
        }
    }
}

/// Names of the metadata columns a snapshot model adds to its rows.
///
/// The defaults match the `snapshot` pipeline. Each key also accepts the dbt
/// spelling (`dbt_valid_from`, `dbt_valid_to`, `dbt_scd_id`, `dbt_updated_at`,
/// `dbt_is_deleted`) so an imported `snapshot_meta_column_names` block reads
/// unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SnapshotMetaColumns {
    /// When this version became current. Default `valid_from`.
    #[serde(default = "default_valid_from", alias = "dbt_valid_from")]
    pub valid_from: String,
    /// When this version stopped being current. Default `valid_to`.
    #[serde(default = "default_valid_to", alias = "dbt_valid_to")]
    pub valid_to: String,
    /// `TRUE` on the current version of each key. Default `is_current`;
    /// `false` writes no flag (dbt has no such column).
    #[serde(default = "default_is_current")]
    pub is_current: SnapshotFlagColumn,
    /// Deterministic per-version id: a hash of the key and `valid_from`.
    /// Default `snapshot_id`.
    #[serde(default = "default_scd_id", alias = "dbt_scd_id")]
    pub scd_id: String,
    /// Optional copy of the version's change timestamp (dbt's
    /// `dbt_updated_at`). Not written unless named.
    #[serde(
        default,
        alias = "dbt_updated_at",
        skip_serializing_if = "Option::is_none"
    )]
    pub updated_at: Option<String>,
    /// Deletion marker, written only under `hard_deletes = "new_record"`.
    /// Default `is_deleted`.
    #[serde(default = "default_is_deleted", alias = "dbt_is_deleted")]
    pub is_deleted: String,
}

impl Default for SnapshotMetaColumns {
    fn default() -> Self {
        Self {
            valid_from: default_valid_from(),
            valid_to: default_valid_to(),
            is_current: default_is_current(),
            scd_id: default_scd_id(),
            updated_at: None,
            is_deleted: default_is_deleted(),
        }
    }
}

impl SnapshotMetaColumns {
    /// Every metadata column this spec writes, in table order.
    pub fn written(&self, hard_deletes: SnapshotHardDeletes) -> Vec<&str> {
        let mut cols = vec![self.valid_from.as_str(), self.valid_to.as_str()];
        if let Some(flag) = self.is_current.name() {
            cols.push(flag);
        }
        cols.push(self.scd_id.as_str());
        if let Some(updated_at) = &self.updated_at {
            cols.push(updated_at.as_str());
        }
        if hard_deletes == SnapshotHardDeletes::NewRecord {
            cols.push(self.is_deleted.as_str());
        }
        cols
    }

    /// Every name a source column may not take, whether or not the current
    /// `hard_deletes` mode writes it.
    pub fn reserved(&self) -> Vec<&str> {
        let mut cols = self.written(SnapshotHardDeletes::NewRecord);
        cols.dedup();
        cols
    }
}

/// The columns a `check` strategy compares.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotCheckColumns {
    /// Every non-key column of the model's output (`check_cols = "all"`).
    All,
    /// The named columns.
    Explicit(Vec<Arc<str>>),
}

/// How a snapshot decides that a key's row changed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum SnapshotChangeStrategy {
    /// A row changed when its `updated_at` is later than the current
    /// version's. The new version's `valid_from` is that `updated_at`.
    Timestamp { updated_at: Arc<str> },
    /// A row changed when any checked column differs (NULL-safe) from the
    /// current version. The new version's `valid_from` is `updated_at` when
    /// one is named (dbt allows it on a check snapshot), else the run time.
    Check {
        check_cols: SnapshotCheckColumns,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        updated_at: Option<Arc<str>>,
    },
}

impl SnapshotChangeStrategy {
    /// The TOML spelling.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Timestamp { .. } => "timestamp",
            Self::Check { .. } => "check",
        }
    }

    /// The column whose value becomes a new version's `valid_from`, if any.
    pub fn version_column(&self) -> Option<&str> {
        match self {
            Self::Timestamp { updated_at } => Some(updated_at),
            Self::Check { updated_at, .. } => updated_at.as_deref(),
        }
    }
}

/// The resolved IR form of a `type = "snapshot"` model sidecar.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotSpec {
    /// Column(s) that identify a row of the model's output.
    pub unique_key: Vec<Arc<str>>,
    /// Change detection.
    pub change: SnapshotChangeStrategy,
    /// Handling of keys that leave the model's output.
    #[serde(default)]
    pub hard_deletes: SnapshotHardDeletes,
    /// Metadata column names.
    #[serde(default)]
    pub meta_columns: SnapshotMetaColumns,
    /// SQL expression written to `valid_to` on current rows instead of NULL
    /// (dbt's `dbt_valid_to_current`), e.g. `'9999-12-31'`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub valid_to_current: Option<String>,
}

impl SnapshotSpec {
    /// Structural validation shared by the compiler (E049) and SQL
    /// generation (defense in depth). Returns one message per problem.
    ///
    /// Column *existence* is not checked here: that needs the model's output
    /// schema, which only the compiler (typed columns) and the runtime
    /// (`describe_table`) have.
    pub fn problems(&self) -> Vec<String> {
        let mut problems = Vec::new();
        if self.unique_key.is_empty() {
            problems.push("`unique_key` is required".to_string());
        }
        for key in &self.unique_key {
            if key.trim().is_empty() {
                problems.push("`unique_key` contains an empty column name".to_string());
            }
        }
        match &self.change {
            SnapshotChangeStrategy::Timestamp { updated_at } => {
                if updated_at.trim().is_empty() {
                    problems.push("`strategy = \"timestamp\"` requires `updated_at`".to_string());
                }
            }
            SnapshotChangeStrategy::Check { check_cols, .. } => match check_cols {
                SnapshotCheckColumns::All => {}
                SnapshotCheckColumns::Explicit(cols) => {
                    if cols.is_empty() {
                        problems.push(
                            "`strategy = \"check\"` requires `check_cols` (a list or \"all\")"
                                .to_string(),
                        );
                    }
                    if cols.iter().any(|c| c.trim().is_empty()) {
                        problems.push("`check_cols` contains an empty column name".to_string());
                    }
                }
            },
        }
        // Key and change columns are spliced into SQL as identifiers. An
        // expression (dbt's `"id || '-' || region"` key idiom) cannot run.
        let mut named: Vec<&str> = self.unique_key.iter().map(AsRef::as_ref).collect();
        if let Some(col) = self.change.version_column() {
            named.push(col);
        }
        if let SnapshotChangeStrategy::Check {
            check_cols: SnapshotCheckColumns::Explicit(cols),
            ..
        } = &self.change
        {
            named.extend(cols.iter().map(AsRef::as_ref));
        }
        for col in named {
            let col = col.trim();
            if !col.is_empty() && validation::validate_identifier(col).is_err() {
                problems.push(format!(
                    "'{col}' is not a column name; snapshot keys and change columns must name \
                     output columns (compute an expression in the model SQL and name it)"
                ));
            }
        }
        let meta = &self.meta_columns;
        let mut seen: Vec<String> = Vec::new();
        for name in meta.reserved() {
            if let Err(e) = validation::validate_identifier(name) {
                problems.push(format!(
                    "snapshot metadata column name '{name}' is not a valid identifier: {e}"
                ));
            }
            let folded = name.to_ascii_lowercase();
            if seen.contains(&folded) {
                problems.push(format!(
                    "snapshot metadata column name '{name}' is used twice in \
                     `snapshot_meta_column_names`"
                ));
            }
            seen.push(folded);
        }
        for key in &self.unique_key {
            if meta
                .reserved()
                .iter()
                .any(|m| m.eq_ignore_ascii_case(key.trim()))
            {
                problems.push(format!(
                    "`unique_key` column '{key}' collides with a snapshot metadata column"
                ));
            }
        }
        if let Some(expr) = &self.valid_to_current
            && let Some(reason) = valid_to_current_problem(expr)
        {
            problems.push(format!("`valid_to_current` {reason}"));
        }
        problems
    }
}

/// `valid_to_current` is a SQL expression the author writes, like the model
/// SQL itself. It is spliced into `CAST(<expr> AS TIMESTAMP)`, so refuse the
/// shapes that could end that expression early or comment out the rest of
/// the statement.
fn valid_to_current_problem(expr: &str) -> Option<&'static str> {
    let trimmed = expr.trim();
    if trimmed.is_empty() {
        return Some("is empty");
    }
    if trimmed.contains(';') || trimmed.contains("--") || trimmed.contains("/*") {
        return Some("must be a single SQL expression (no `;` or comments)");
    }
    let mut depth: i64 = 0;
    let mut in_quote = false;
    for ch in trimmed.chars() {
        match ch {
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => depth += 1,
            ')' if !in_quote => {
                depth -= 1;
                if depth < 0 {
                    return Some("has unbalanced parentheses");
                }
            }
            _ => {}
        }
    }
    if in_quote {
        return Some("has an unterminated string literal");
    }
    if depth != 0 {
        return Some("has unbalanced parentheses");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> SnapshotSpec {
        SnapshotSpec {
            unique_key: vec!["id".into()],
            change: SnapshotChangeStrategy::Timestamp {
                updated_at: "updated_at".into(),
            },
            hard_deletes: SnapshotHardDeletes::Ignore,
            meta_columns: SnapshotMetaColumns::default(),
            valid_to_current: None,
        }
    }

    #[test]
    fn a_complete_spec_has_no_problems() {
        assert!(spec().problems().is_empty());
    }

    #[test]
    fn missing_key_and_updated_at_are_problems() {
        let mut s = spec();
        s.unique_key.clear();
        s.change = SnapshotChangeStrategy::Timestamp {
            updated_at: "".into(),
        };
        let p = s.problems();
        assert!(p.iter().any(|m| m.contains("unique_key")), "{p:?}");
        assert!(p.iter().any(|m| m.contains("updated_at")), "{p:?}");
    }

    #[test]
    fn empty_check_list_is_a_problem_but_all_is_not() {
        let mut s = spec();
        s.change = SnapshotChangeStrategy::Check {
            check_cols: SnapshotCheckColumns::Explicit(vec![]),
            updated_at: None,
        };
        assert!(!s.problems().is_empty());
        s.change = SnapshotChangeStrategy::Check {
            check_cols: SnapshotCheckColumns::All,
            updated_at: None,
        };
        assert!(s.problems().is_empty());
    }

    #[test]
    fn key_expressions_are_problems() {
        let mut s = spec();
        s.unique_key = vec!["id || '-' || region".into()];
        assert!(s.problems().iter().any(|m| m.contains("not a column name")));
    }

    #[test]
    fn meta_names_must_be_identifiers_and_distinct() {
        let mut s = spec();
        s.meta_columns.valid_from = "valid to".into();
        assert!(!s.problems().is_empty());
        let mut s = spec();
        s.meta_columns.valid_to = "VALID_FROM".into();
        assert!(s.problems().iter().any(|m| m.contains("used twice")));
    }

    #[test]
    fn valid_to_current_refuses_statement_breakers() {
        for bad in ["", "'x'; DROP TABLE t", "1 -- c", "(1", "'abc"] {
            let mut s = spec();
            s.valid_to_current = Some(bad.into());
            assert!(!s.problems().is_empty(), "{bad} should be refused");
        }
        let mut s = spec();
        s.valid_to_current = Some("CAST('9999-12-31' AS DATE)".into());
        assert!(s.problems().is_empty());
    }

    #[test]
    fn dbt_meta_column_spellings_deserialize() {
        let meta: SnapshotMetaColumns = serde_json::from_str(
            r#"{"dbt_valid_from":"dbt_valid_from","dbt_valid_to":"dbt_valid_to",
                "dbt_scd_id":"dbt_scd_id","dbt_updated_at":"dbt_updated_at"}"#,
        )
        .unwrap();
        assert_eq!(meta.valid_from, "dbt_valid_from");
        assert_eq!(meta.scd_id, "dbt_scd_id");
        assert_eq!(meta.updated_at.as_deref(), Some("dbt_updated_at"));
        assert_eq!(meta.is_current.name(), Some("is_current"));
        let meta: SnapshotMetaColumns = serde_json::from_str(r#"{"is_current":false}"#).unwrap();
        assert_eq!(meta.is_current.name(), None);
        assert_eq!(
            meta.written(SnapshotHardDeletes::Ignore),
            vec!["valid_from", "valid_to", "snapshot_id"]
        );
    }
}
