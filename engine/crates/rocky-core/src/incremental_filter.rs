//! The watermark filter of a transformation `incremental` model.
//!
//! A transformation model with `type = "incremental"` loads only the rows
//! newer than what its target already holds. The model SQL marks where that
//! filter belongs with a placeholder:
//!
//! ```sql
//! SELECT order_id, amount, updated_at
//! FROM raw.orders
//! WHERE @incremental_filter
//! ```
//!
//! The placeholder compares the declared watermark column (`timestamp_column`),
//! or the sidecar's `filter_column` when the input column is qualified
//! (`o.updated_at`) or renamed (`_synced_at`). The right-hand side is always
//! `MAX(<watermark>)` over the **target**, so the watermark is read from the
//! warehouse each run and stays correct after a manual edit of the target. No
//! state-store row is involved.
//!
//! The placeholder is a bare token on purpose: Rocky's SQL parser reads
//! `@name` as a parameter, so the model still parses for lineage and type
//! checking, while `@name(...)` would not.
//!
//! | Run | Placeholder resolves to |
//! |---|---|
//! | first run (target absent) | `TRUE` |
//! | `rocky run --full-refresh` | `TRUE` |
//! | incremental run | `(<col> > (SELECT MAX(<wm>) [- lookback] FROM <target>) OR NOT EXISTS (SELECT 1 FROM <target>))` |
//!
//! The `NOT EXISTS` arm loads everything when the target exists but is empty.
//! A target whose rows all hold a `NULL` watermark loads nothing more:
//! `NULL` never compares greater, so rows without a watermark load only on
//! the first run and on a full refresh.
//!
//! A model without the placeholder is wrapped — `SELECT * FROM (<model>) AS
//! _rocky_incremental WHERE <predicate on the output column>` — which the
//! compiler allows only when lineage proves the watermark is a direct
//! passthrough (E046 otherwise).
//!
//! Placeholders inside SQL comments and quoted literals are ignored: a
//! commented-out `@incremental_filter` must not count as the filter.

use rocky_ir::{IncrementalLookback, MaterializationStrategy, ModelIr};
use rocky_sql::validation;

use crate::sql_gen::SqlGenError;
use crate::traits::SqlDialect;

/// The placeholder token.
pub const PLACEHOLDER: &str = "@incremental_filter";

/// Alias of the subquery a placeholder-less model is wrapped in.
pub const WRAP_ALIAS: &str = "_rocky_incremental";

/// Byte ranges of every `@incremental_filter` token outside comments and
/// quoted text, in source order.
///
/// A token glued to identifier characters (`@incremental_filters`,
/// `x@incremental_filter`) is a different word and is skipped.
#[must_use]
pub fn find_placeholders(sql: &str) -> Vec<std::ops::Range<usize>> {
    let bytes = sql.as_bytes();
    let is_ident = |c: &u8| c.is_ascii_alphanumeric() || *c == b'_';
    let mut found = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'-' if bytes.get(i + 1) == Some(&b'-') => {
                i = sql[i..].find('\n').map_or(bytes.len(), |n| i + n + 1);
            }
            b'/' if bytes.get(i + 1) == Some(&b'*') => {
                i = sql[i + 2..]
                    .find("*/")
                    .map_or(bytes.len(), |n| i + 2 + n + 2);
            }
            quote @ (b'\'' | b'"' | b'`') => {
                i = skip_quoted(bytes, i, quote);
            }
            // `$$ ... $$` / `$tag$ ... $tag$` dollar-quoted text.
            b'$' if dollar_tag(&sql[i..]).is_some() => {
                let tag = dollar_tag(&sql[i..]).unwrap_or("$$");
                let body = i + tag.len();
                i = sql[body..]
                    .find(tag)
                    .map_or(bytes.len(), |n| body + n + tag.len());
            }
            b'@' if sql[i..].starts_with(PLACEHOLDER) => {
                let end = i + PLACEHOLDER.len();
                let glued =
                    bytes.get(end).is_some_and(is_ident) || (i > 0 && is_ident(&bytes[i - 1]));
                if !glued {
                    found.push(i..end);
                }
                i = end;
            }
            _ => i += 1,
        }
    }
    found
}

/// `true` when `sql` holds at least one live placeholder.
#[must_use]
pub fn has_placeholder(sql: &str) -> bool {
    !find_placeholders(sql).is_empty()
}

/// The opening tag of a dollar-quoted string at the start of `s` (`$$` or
/// `$name$`), or `None` for a positional parameter such as `$1`.
fn dollar_tag(s: &str) -> Option<&str> {
    let rest = s.strip_prefix('$')?;
    let end = rest.find('$')?;
    let name = &rest[..end];
    let valid = name.is_empty()
        || (name.starts_with(|c: char| c.is_ascii_alphabetic() || c == '_')
            && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    valid.then(|| &s[..end + 2])
}

/// Skip a quoted run starting at `start` (the opening quote). A doubled quote
/// is an escaped quote; a backslash escapes the next byte inside `'…'`, which
/// is how most of Rocky's dialects read it. Returns the index after the
/// closing quote, or the end of input.
fn skip_quoted(bytes: &[u8], start: usize, quote: u8) -> usize {
    let mut i = start + 1;
    while i < bytes.len() {
        if bytes[i] == b'\\' && quote == b'\'' {
            i += 2;
            continue;
        }
        if bytes[i] == quote {
            if bytes.get(i + 1) == Some(&quote) {
                i += 2;
                continue;
            }
            return i + 1;
        }
        i += 1;
    }
    bytes.len()
}

/// Replace every placeholder with `replacement`.
fn replace_placeholders(sql: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(sql.len());
    let mut last = 0;
    for span in find_placeholders(sql) {
        out.push_str(&sql[last..span.start]);
        out.push_str(replacement);
        last = span.end;
    }
    out.push_str(&sql[last..]);
    out
}

/// Which run is being generated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterMode<'a> {
    /// First run or `--full-refresh`: load every row.
    Unfiltered,
    /// An incremental run against the existing, dialect-formatted `target`.
    SinceTarget { target: &'a str },
}

/// The watermark settings of an `Incremental` IR, validated.
struct Watermark<'a> {
    column: &'a str,
    /// What the placeholder compares: `filter_column`, else `column`.
    filter: &'a str,
    lookback: Option<IncrementalLookback>,
}

fn watermark_of(model_ir: &ModelIr) -> Result<Watermark<'_>, SqlGenError> {
    let MaterializationStrategy::Incremental {
        timestamp_column,
        lookback,
        filter_column,
        ..
    } = &model_ir.materialization
    else {
        return Err(SqlGenError::InvalidRequest(format!(
            "model '{}': incremental filter requested for a non-incremental strategy",
            model_ir.name
        )));
    };
    if timestamp_column.is_empty() {
        return Err(no_watermark_refused(model_ir));
    }
    // SECURITY: the column comes from the sidecar TOML. The compiler checks it
    // too (E046), but re-validate where it is spliced into SQL.
    validation::validate_identifier(timestamp_column).map_err(|e| SqlGenError::UnsafeFragment {
        value: timestamp_column.clone(),
        reason: e.to_string(),
    })?;
    let filter = match filter_column.as_deref() {
        Some(filter) => {
            validate_filter_column(filter).map_err(|reason| SqlGenError::UnsafeFragment {
                value: filter.to_string(),
                reason,
            })?;
            filter
        }
        None => timestamp_column,
    };
    Ok(Watermark {
        column: timestamp_column,
        filter,
        lookback: *lookback,
    })
}

/// A `filter_column` is a column name, optionally qualified by one table
/// alias: `updated_at` or `o.updated_at`. Each part is a plain identifier.
///
/// # Errors
///
/// A reason string naming the offending part.
pub fn validate_filter_column(filter: &str) -> Result<(), String> {
    let parts: Vec<&str> = filter.split('.').collect();
    if parts.len() > 2 {
        return Err(format!(
            "filter_column '{filter}' has more than one qualifier; use `<alias>.<column>`"
        ));
    }
    for part in parts {
        validation::validate_identifier(part)
            .map_err(|e| format!("filter_column '{filter}': {e}"))?;
    }
    Ok(())
}

/// The refusal for an `incremental` transformation model with no watermark
/// column (E037).
pub(crate) fn no_watermark_refused(model_ir: &ModelIr) -> SqlGenError {
    SqlGenError::InvalidRequest(format!(
        "model '{}': `type = \"incremental\"` declares no watermark (E037); set \
         `timestamp_column = \"<column>\"` in [strategy] and mark the filter with \
         `@incremental_filter`, or use merge, delete_insert, time_interval or full_refresh",
        model_ir.name
    ))
}

/// `<lhs> > (SELECT MAX(<wm>) [- <lookback>] FROM <target>) OR NOT EXISTS
/// (SELECT 1 FROM <target>)`, parenthesised so it composes with any
/// surrounding `AND` / `OR`.
fn predicate(lhs: &str, wm: &Watermark<'_>, target: &str, dialect: &dyn SqlDialect) -> String {
    let column = wm.column;
    let bound = match wm.lookback {
        Some(lb) if lb.amount > 0 => dialect.subtract_interval_expr(
            &format!("MAX({column})"),
            lb.amount,
            lb.unit.sql_keyword(),
        ),
        _ => format!("MAX({column})"),
    };
    format!("({lhs} > (SELECT {bound} FROM {target}) OR NOT EXISTS (SELECT 1 FROM {target}))")
}

/// The `SELECT` an `incremental` transformation model loads in `mode`.
///
/// With a placeholder, each occurrence is resolved in place. Without one, an
/// incremental run wraps the model and filters its output column; an
/// unfiltered run returns the SQL unchanged.
///
/// # Errors
///
/// [`SqlGenError::InvalidRequest`] when `model_ir` is not `Incremental` or
/// declares no watermark (E037); [`SqlGenError::UnsafeFragment`] when the
/// watermark is not a plain identifier.
pub fn incremental_select(
    model_ir: &ModelIr,
    dialect: &dyn SqlDialect,
    mode: FilterMode<'_>,
) -> Result<String, SqlGenError> {
    let wm = watermark_of(model_ir)?;
    let sql = model_ir.sql.as_str();
    if has_placeholder(sql) {
        return Ok(match mode {
            FilterMode::Unfiltered => replace_placeholders(sql, dialect.true_predicate()),
            FilterMode::SinceTarget { target } => {
                replace_placeholders(sql, &predicate(wm.filter, &wm, target, dialect))
            }
        });
    }
    Ok(match mode {
        FilterMode::Unfiltered => sql.to_string(),
        FilterMode::SinceTarget { target } => {
            let lhs = format!("{WRAP_ALIAS}.{}", wm.column);
            format!(
                "SELECT * FROM (\n{sql}\n) AS {WRAP_ALIAS}\nWHERE {}",
                predicate(&lhs, &wm, target, dialect)
            )
        }
    })
}

/// Resolve placeholders to `TRUE` in any model's SQL.
///
/// For the full-refresh rebuild of an `incremental` model, which runs the
/// model SQL under another strategy.
#[must_use]
pub fn unfiltered_sql(sql: &str) -> String {
    replace_placeholders(sql, "TRUE")
}

/// [`unfiltered_sql`] with the dialect's always-true predicate
/// ([`SqlDialect::true_predicate`]) — `(1 = 1)` on SQL Server, which has no
/// `TRUE` literal.
#[must_use]
pub fn unfiltered_sql_for(sql: &str, dialect: &dyn SqlDialect) -> String {
    replace_placeholders(sql, dialect.true_predicate())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_ir::{GovernanceConfig, LookbackUnit, TargetRef};
    use std::sync::Arc;

    struct Ansi;
    impl SqlDialect for Ansi {
        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> crate::traits::AdapterResult<String> {
            Ok(if catalog.is_empty() {
                format!("{schema}.{table}")
            } else {
                format!("{catalog}.{schema}.{table}")
            })
        }
        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE OR REPLACE TABLE {target} AS\n{select_sql}")
        }
        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target}\n{select_sql}")
        }
        fn merge_into(
            &self,
            _target: &str,
            _source_sql: &str,
            _keys: &[Arc<str>],
            _update_cols: &rocky_ir::ColumnSelection,
        ) -> crate::traits::AdapterResult<String> {
            unimplemented!()
        }
        fn select_clause(
            &self,
            _columns: &rocky_ir::ColumnSelection,
            _metadata: &[rocky_ir::MetadataColumn],
        ) -> crate::traits::AdapterResult<String> {
            unimplemented!()
        }
        fn watermark_where(
            &self,
            _timestamp_col: &str,
            _last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> crate::traits::AdapterResult<String> {
            unimplemented!()
        }
        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE {table_ref}")
        }
        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE IF EXISTS {table_ref}")
        }
        fn create_catalog_sql(&self, _name: &str) -> Option<crate::traits::AdapterResult<String>> {
            None
        }
        fn create_schema_sql(
            &self,
            _catalog: &str,
            _schema: &str,
        ) -> Option<crate::traits::AdapterResult<String>> {
            None
        }
        fn tablesample_clause(&self, _percent: u32) -> Option<String> {
            None
        }
        fn insert_overwrite_partition(
            &self,
            _target: &str,
            _partition_filter: &str,
            _select_sql: &str,
        ) -> crate::traits::AdapterResult<Vec<String>> {
            unimplemented!()
        }
        fn literal_escape(&self) -> crate::traits::LiteralEscape {
            crate::traits::LiteralEscape::Standard
        }
    }

    fn ir(sql: &str, ts: &str, lookback: Option<IncrementalLookback>) -> ModelIr {
        ir_filtering(sql, ts, lookback, None)
    }

    fn ir_filtering(
        sql: &str,
        ts: &str,
        lookback: Option<IncrementalLookback>,
        filter_column: Option<&str>,
    ) -> ModelIr {
        ModelIr::transformation(
            TargetRef {
                catalog: String::new(),
                schema: "main".into(),
                table: "fct".into(),
            },
            MaterializationStrategy::Incremental {
                timestamp_column: ts.into(),
                unique_key: Vec::new(),
                lookback,
                filter_column: filter_column.map(str::to_string),
            },
            vec![],
            sql.into(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        )
    }

    #[test]
    fn finds_every_live_token() {
        let sql = "SELECT * FROM t WHERE @incremental_filter AND x = 1 OR @incremental_filter";
        let spans = find_placeholders(sql);
        assert_eq!(spans.len(), 2);
        assert_eq!(&sql[spans[1].clone()], "@incremental_filter");
    }

    #[test]
    fn ignores_comments_literals_and_longer_tokens() {
        for sql in [
            "SELECT 1 -- WHERE @incremental_filter\nFROM t",
            "SELECT 1 /* @incremental_filter */ FROM t",
            "SELECT '@incremental_filter' AS s FROM t",
            "SELECT 'it''s @incremental_filter' AS s FROM t",
            "SELECT \"@incremental_filter\" FROM t",
            "SELECT @incremental_filters FROM t",
            "SELECT a@incremental_filter FROM t",
            "SELECT $$ @incremental_filter $$ FROM t",
            "SELECT $q$ @incremental_filter $q$ FROM t",
        ] {
            assert!(!has_placeholder(sql), "{sql}");
        }
        assert!(has_placeholder(
            "SELECT 1 -- note\nFROM t WHERE @incremental_filter"
        ));
    }

    #[test]
    fn unfiltered_run_resolves_to_true() {
        let m = ir(
            "SELECT * FROM raw.orders WHERE status = 'x' AND @incremental_filter",
            "updated_at",
            None,
        );
        let sql = incremental_select(&m, &Ansi, FilterMode::Unfiltered).unwrap();
        assert_eq!(sql, "SELECT * FROM raw.orders WHERE status = 'x' AND TRUE");
    }

    #[test]
    fn incremental_run_compares_against_target_max() {
        let m = ir(
            "SELECT * FROM raw.orders WHERE @incremental_filter",
            "updated_at",
            None,
        );
        let sql =
            incremental_select(&m, &Ansi, FilterMode::SinceTarget { target: "main.fct" }).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM raw.orders WHERE (updated_at > (SELECT MAX(updated_at) FROM \
             main.fct) OR NOT EXISTS (SELECT 1 FROM main.fct))"
        );
    }

    #[test]
    fn filter_column_replaces_the_compared_column_not_the_bound() {
        let m = ir_filtering(
            "SELECT o.id, o._synced_at AS updated_at FROM raw.orders o WHERE @incremental_filter",
            "updated_at",
            None,
            Some("o._synced_at"),
        );
        let sql =
            incremental_select(&m, &Ansi, FilterMode::SinceTarget { target: "main.fct" }).unwrap();
        assert!(
            sql.ends_with(
                "WHERE (o._synced_at > (SELECT MAX(updated_at) FROM main.fct) \
                 OR NOT EXISTS (SELECT 1 FROM main.fct))"
            ),
            "{sql}"
        );
        for bad in ["a.b.c", "o.x; DROP TABLE t", "o.", ""] {
            let m = ir_filtering("SELECT 1 WHERE @incremental_filter", "ts", None, Some(bad));
            assert!(
                matches!(
                    incremental_select(&m, &Ansi, FilterMode::Unfiltered),
                    Err(SqlGenError::UnsafeFragment { .. })
                ),
                "{bad:?} must be refused"
            );
        }
    }

    #[test]
    fn lookback_subtracts_a_dialect_interval() {
        let m = ir(
            "SELECT * FROM raw.orders WHERE @incremental_filter",
            "updated_at",
            Some(IncrementalLookback {
                amount: 3,
                unit: LookbackUnit::Day,
            }),
        );
        let sql =
            incremental_select(&m, &Ansi, FilterMode::SinceTarget { target: "main.fct" }).unwrap();
        assert!(
            sql.contains("(SELECT MAX(updated_at) - INTERVAL '3' DAY FROM main.fct)"),
            "{sql}"
        );
        // The empty-target arm does not depend on the watermark at all.
        assert!(
            sql.contains("OR NOT EXISTS (SELECT 1 FROM main.fct)"),
            "{sql}"
        );
    }

    #[test]
    fn model_without_placeholder_is_wrapped_on_its_output_column() {
        let m = ir("SELECT id, updated_at FROM raw.orders", "updated_at", None);
        let sql =
            incremental_select(&m, &Ansi, FilterMode::SinceTarget { target: "main.fct" }).unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM (\nSELECT id, updated_at FROM raw.orders\n) AS _rocky_incremental\n\
             WHERE (_rocky_incremental.updated_at > (SELECT MAX(updated_at) FROM main.fct) \
             OR NOT EXISTS (SELECT 1 FROM main.fct))"
        );
        let first = incremental_select(&m, &Ansi, FilterMode::Unfiltered).unwrap();
        assert_eq!(first, "SELECT id, updated_at FROM raw.orders");
    }

    #[test]
    fn missing_or_unsafe_watermark_is_refused() {
        let none = ir("SELECT 1", "", None);
        let err = incremental_select(&none, &Ansi, FilterMode::Unfiltered).unwrap_err();
        assert!(err.to_string().contains("E037"), "{err}");
        let unsafe_col = ir("SELECT 1", "ts; DROP TABLE x", None);
        assert!(matches!(
            incremental_select(&unsafe_col, &Ansi, FilterMode::Unfiltered),
            Err(SqlGenError::UnsafeFragment { .. })
        ));
    }
}
