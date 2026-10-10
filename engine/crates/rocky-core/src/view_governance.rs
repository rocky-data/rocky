//! Guard for `CREATE OR REPLACE VIEW` over a governed view (#2234).
//!
//! On Databricks Unity Catalog, `CREATE OR REPLACE VIEW` drops the view's
//! governed tags and the policies attached to it. A reader with inherited
//! schema-level `SELECT` can then read the unfiltered replacement.
//!
//! Before Rocky replaces a view, it lists the governance attached to it:
//!
//! ```text
//!   probe (SqlDialect::view_governance_probe_sql)
//!     │ query fails ─────────────────────────▶ refuse (ProbeFailed)
//!     ▼
//!   rows ─▶ foreign_governance(rows, declared)
//!     │ any foreign item ────────────────────▶ refuse (Foreign)
//!     ▼
//!   CREATE OR REPLACE VIEW, then Rocky re-applies its declared tags
//! ```
//!
//! "Foreign" is anything Rocky does not declare and so cannot restore:
//! a tag whose `(key, value)` is not in the model's `[governance.tags]`
//! (or, for a column tag, its `classification`), and any row filter or
//! column mask at all.
//!
//! A dialect without a probe (every dialect except Databricks today) skips
//! the guard.

use std::collections::BTreeMap;
use std::fmt;

use rocky_ir::TableRef;

use crate::traits::{AdapterError, QueryResult, WarehouseAdapter};

/// The kind of one governance item attached to a view.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AttachedGovernanceKind {
    /// A tag on the view itself.
    TableTag,
    /// A tag on one column of the view.
    ColumnTag,
    /// A row filter on the view.
    RowFilter,
    /// A column mask on one column of the view.
    ColumnMask,
}

impl AttachedGovernanceKind {
    /// The `kind` value the probe query emits for this item.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TableTag => "table_tag",
            Self::ColumnTag => "column_tag",
            Self::RowFilter => "row_filter",
            Self::ColumnMask => "column_mask",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        match value {
            "table_tag" => Some(Self::TableTag),
            "column_tag" => Some(Self::ColumnTag),
            "row_filter" => Some(Self::RowFilter),
            "column_mask" => Some(Self::ColumnMask),
            _ => None,
        }
    }
}

/// One governance item the probe found on a view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttachedGovernance {
    pub kind: AttachedGovernanceKind,
    /// The column, for a column tag or a column mask.
    pub column: Option<String>,
    /// The tag key, the row-filter function or the column-mask function.
    pub name: String,
    /// The tag value. `None` for a filter or a mask.
    pub value: Option<String>,
}

impl fmt::Display for AttachedGovernance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let column = self.column.as_deref().unwrap_or("?");
        let value = self.value.as_deref().unwrap_or("");
        match self.kind {
            AttachedGovernanceKind::TableTag => write!(f, "tag {}={value}", self.name),
            AttachedGovernanceKind::ColumnTag => {
                write!(f, "tag {}={value} on column {column}", self.name)
            }
            AttachedGovernanceKind::RowFilter => write!(f, "row filter {}", self.name),
            AttachedGovernanceKind::ColumnMask => {
                write!(f, "column mask {} on column {column}", self.name)
            }
        }
    }
}

/// The governance Rocky itself declares for a view, and so re-applies after
/// it replaces the view.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredViewGovernance {
    /// The model's `[governance.tags]`, applied to the view.
    pub tags: BTreeMap<String, String>,
    /// Column tags Rocky applies, keyed by column then tag key.
    pub column_tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl DeclaredViewGovernance {
    /// Declared governance for a transformation model: its
    /// `[governance.tags]` and one `classification` tag per classified
    /// column (the tag `reconcile_model_governance` applies).
    pub fn for_model(
        governance_tags: &BTreeMap<String, String>,
        classification: &BTreeMap<String, String>,
    ) -> Self {
        let column_tags = classification
            .iter()
            .map(|(column, class)| {
                (
                    column.clone(),
                    BTreeMap::from([("classification".to_string(), class.clone())]),
                )
            })
            .collect();
        Self {
            tags: governance_tags.clone(),
            column_tags,
        }
    }

    fn declares_tag(&self, key: &str, value: &str) -> bool {
        self.tags.get(key).is_some_and(|v| v == value)
    }

    fn declares_column_tag(&self, column: &str, key: &str, value: &str) -> bool {
        self.column_tags
            .iter()
            .filter(|(declared, _)| declared.eq_ignore_ascii_case(column))
            .any(|(_, tags)| tags.get(key).is_some_and(|v| v == value))
    }
}

/// The items in `attached` that Rocky does not declare, in input order.
///
/// A tag counts as declared only when its key AND its value match. A row
/// filter or a column mask is never declared: Rocky does not re-apply one
/// to a view, so the replace would always drop it.
pub fn foreign_governance(
    attached: &[AttachedGovernance],
    declared: &DeclaredViewGovernance,
) -> Vec<AttachedGovernance> {
    attached
        .iter()
        .filter(|item| {
            let value = item.value.as_deref().unwrap_or("");
            match item.kind {
                AttachedGovernanceKind::TableTag => !declared.declares_tag(&item.name, value),
                AttachedGovernanceKind::ColumnTag => match item.column.as_deref() {
                    Some(column) => !declared.declares_column_tag(column, &item.name, value),
                    None => true,
                },
                AttachedGovernanceKind::RowFilter | AttachedGovernanceKind::ColumnMask => true,
            }
        })
        .cloned()
        .collect()
}

/// Parse the probe result: four columns `kind, column_name, name, value`.
///
/// # Errors
///
/// Returns an error for a row with the wrong width, an unknown `kind`, or a
/// missing `name`. The caller refuses the replace on any error.
pub fn parse_probe_rows(result: &QueryResult) -> Result<Vec<AttachedGovernance>, AdapterError> {
    fn opt_str(value: &serde_json::Value) -> Result<Option<String>, AdapterError> {
        match value {
            serde_json::Value::Null => Ok(None),
            serde_json::Value::String(s) => Ok(Some(s.clone())),
            other => Err(AdapterError::msg(format!(
                "view governance probe returned a non-string value: {other}"
            ))),
        }
    }

    result
        .rows
        .iter()
        .map(|row| {
            let [kind, column, name, value] = row.as_slice() else {
                return Err(AdapterError::msg(format!(
                    "view governance probe returned a row with {} columns, expected 4",
                    row.len()
                )));
            };
            let kind_str = opt_str(kind)?.unwrap_or_default();
            let kind = AttachedGovernanceKind::parse(&kind_str).ok_or_else(|| {
                AdapterError::msg(format!(
                    "view governance probe returned an unknown kind '{kind_str}'"
                ))
            })?;
            let name = opt_str(name)?.ok_or_else(|| {
                AdapterError::msg("view governance probe returned a row with no name")
            })?;
            Ok(AttachedGovernance {
                kind,
                column: opt_str(column)?,
                name,
                value: opt_str(value)?,
            })
        })
        .collect()
}

/// Rocky refuses to replace a view (#2234).
#[derive(thiserror::Error)]
pub enum ViewReplaceRefused {
    /// The view carries governance Rocky does not declare.
    #[error(
        "refusing to replace view {view}: it carries governance that Rocky does not declare: \
         {items}. CREATE OR REPLACE VIEW drops a view's governed tags and the policies \
         attached to it, so readers could see the unfiltered replacement (#2234). \
         Remove these from the view, then re-run. `rocky run` also accepts a tag declared \
         in the model's [governance.tags], because it re-applies that tag after the replace"
    )]
    Foreign {
        view: String,
        items: String,
        attached: Vec<AttachedGovernance>,
    },
    /// The probe failed, so Rocky cannot tell what the replace would drop.
    #[error(
        "refusing to replace view {view}: cannot read its tags, row filters and column \
         masks ({source}). Rocky does not replace a view whose governance it cannot \
         check (#2234)"
    )]
    ProbeFailed {
        view: String,
        #[source]
        source: AdapterError,
    },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for ViewReplaceRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::secret_registry::fmt_rendered_debug(f, "ViewReplaceRefused", self)
    }
}

/// Check that replacing `view` drops no governance Rocky cannot restore.
///
/// Runs the dialect's probe (see
/// [`SqlDialect::view_governance_probe_sql`](crate::traits::SqlDialect::view_governance_probe_sql)).
/// A dialect without a probe passes. A view that does not exist yet has no
/// rows, so it passes.
///
/// # Errors
///
/// [`ViewReplaceRefused::Foreign`] when the view carries governance outside
/// `declared`. [`ViewReplaceRefused::ProbeFailed`] when the probe cannot be
/// built, run or parsed: the check fails closed.
pub async fn check_view_replace(
    warehouse: &dyn WarehouseAdapter,
    view: &TableRef,
    declared: &DeclaredViewGovernance,
) -> Result<(), ViewReplaceRefused> {
    let Some(sql) = warehouse.dialect().view_governance_probe_sql(view) else {
        return Ok(());
    };
    let probe_failed = |source| ViewReplaceRefused::ProbeFailed {
        view: view.full_name(),
        source,
    };
    let sql = sql.map_err(probe_failed)?;
    let result = warehouse.execute_query(&sql).await.map_err(probe_failed)?;
    let attached = parse_probe_rows(&result).map_err(probe_failed)?;
    let foreign = foreign_governance(&attached, declared);
    if foreign.is_empty() {
        return Ok(());
    }
    Err(ViewReplaceRefused::Foreign {
        view: view.full_name(),
        items: foreign
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", "),
        attached: foreign,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `Debug` output prints a resolved `${VAR}` value as `${NAME}` (#1919).
    #[test]
    fn view_replace_refused_debug_prints_a_resolved_value_as_its_name() {
        const SECRET: &str = "view-catalog-1919-a3b4";
        crate::secret_registry::register_substitution("RV_VIEWGOV_DBG", SECRET);
        let err = ViewReplaceRefused::Foreign {
            view: format!("{SECRET}.sales.v_orders"),
            items: "tag pii".into(),
            attached: Vec::new(),
        };
        let debug = format!("{err:?}");
        assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_VIEWGOV_DBG}"), "{debug}");
    }

    fn item(
        kind: AttachedGovernanceKind,
        column: Option<&str>,
        name: &str,
        value: Option<&str>,
    ) -> AttachedGovernance {
        AttachedGovernance {
            kind,
            column: column.map(str::to_string),
            name: name.to_string(),
            value: value.map(str::to_string),
        }
    }

    fn declared() -> DeclaredViewGovernance {
        DeclaredViewGovernance::for_model(
            &BTreeMap::from([("domain".to_string(), "finance".to_string())]),
            &BTreeMap::from([("Email".to_string(), "pii".to_string())]),
        )
    }

    #[test]
    fn declared_tags_are_not_foreign() {
        let attached = vec![
            item(
                AttachedGovernanceKind::TableTag,
                None,
                "domain",
                Some("finance"),
            ),
            item(
                AttachedGovernanceKind::ColumnTag,
                Some("email"),
                "classification",
                Some("pii"),
            ),
        ];
        assert!(foreign_governance(&attached, &declared()).is_empty());
    }

    #[test]
    fn undeclared_key_or_changed_value_is_foreign() {
        let attached = vec![
            item(AttachedGovernanceKind::TableTag, None, "pii", Some("true")),
            item(AttachedGovernanceKind::TableTag, None, "domain", Some("hr")),
            item(
                AttachedGovernanceKind::ColumnTag,
                Some("ssn"),
                "classification",
                Some("pii"),
            ),
            item(
                AttachedGovernanceKind::ColumnTag,
                None,
                "classification",
                Some("pii"),
            ),
        ];
        assert_eq!(foreign_governance(&attached, &declared()), attached);
    }

    #[test]
    fn row_filters_and_column_masks_are_always_foreign() {
        let attached = vec![
            item(AttachedGovernanceKind::RowFilter, None, "cat.sch.f", None),
            item(
                AttachedGovernanceKind::ColumnMask,
                Some("email"),
                "cat.sch.m",
                None,
            ),
        ];
        assert_eq!(foreign_governance(&attached, &declared()), attached);
    }

    #[test]
    fn parse_probe_rows_reads_all_kinds_and_rejects_bad_rows() {
        use serde_json::json;
        let ok = QueryResult {
            columns: vec![],
            rows: vec![
                vec![json!("table_tag"), json!(null), json!("k"), json!("v")],
                vec![json!("column_tag"), json!("c"), json!("k"), json!(null)],
                vec![json!("row_filter"), json!(null), json!("f"), json!(null)],
                vec![json!("column_mask"), json!("c"), json!("m"), json!(null)],
            ],
        };
        let parsed = parse_probe_rows(&ok).unwrap();
        assert_eq!(parsed.len(), 4);
        assert_eq!(parsed[1].column.as_deref(), Some("c"));
        assert_eq!(parsed[1].value, None);

        for bad in [
            vec![json!("table_tag"), json!(null), json!("k")],
            vec![json!("grant"), json!(null), json!("k"), json!("v")],
            vec![json!("table_tag"), json!(null), json!(null), json!("v")],
            vec![json!("table_tag"), json!(null), json!(1), json!("v")],
        ] {
            let result = QueryResult {
                columns: vec![],
                rows: vec![bad],
            };
            assert!(parse_probe_rows(&result).is_err());
        }
    }
}
