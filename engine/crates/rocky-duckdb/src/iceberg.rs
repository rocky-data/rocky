//! Iceberg REST catalogs attached to a DuckDB connection
//! (`docs/adr/ADR-ICEBERG-DUCKDB.md`): the in-place overwrite. The attach
//! step lives in [`crate::attach`].
//!
//! DuckDB-Iceberg refuses `CREATE OR REPLACE` and a rename inside a write
//! transaction. A full refresh of an existing table is therefore one
//! transaction ([`overwrite_in_place`]):
//!
//! ```text
//! BEGIN TRANSACTION
//!   DESCRIBE <target>; DESCRIBE <select>       (read both column lists)
//!   ALTER TABLE … ADD COLUMN …                 (new columns first)
//!   ALTER TABLE … DROP COLUMN …                (removed columns)
//!   ALTER TABLE … ALTER COLUMN … TYPE …        (widenings)
//!   DELETE FROM <target>
//!   INSERT INTO <target> BY NAME <select>
//! COMMIT                 any error → ROLLBACK; the old table and rows stay
//! ```

use crate::{DuckDbConnector, DuckDbError};

/// How many times a full refresh runs its transaction when the catalog
/// reports a commit conflict with another writer. A full refresh reads
/// nothing it wrote, so running it again is safe.
pub const COMMIT_CONFLICT_ATTEMPTS: u32 = 3;

// ---------------------------------------------------------------------------
// Column reconcile
// ---------------------------------------------------------------------------

/// One column change an in-place overwrite makes before it rewrites the rows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnChange {
    /// The query has a column the table does not.
    Add { name: String, data_type: String },
    /// The table has a column the query does not.
    Drop { name: String },
    /// The column's type widens (`INTEGER` → `BIGINT`, `FLOAT` → `DOUBLE`,
    /// `DECIMAL(p,s)` → `DECIMAL(p',s)` with `p' > p`).
    Widen {
        name: String,
        from: String,
        to: String,
    },
}

/// A column type as DuckDB-Iceberg stores it, so a query's type and the
/// table's type read back compare equal when Iceberg would store them the
/// same. Iceberg has no 8-, 16-bit or unsigned integers and no 128-bit
/// integer: `TINYINT`, `SMALLINT`, `UTINYINT` and `USMALLINT` come back
/// `INTEGER`, `UINTEGER` comes back `BIGINT`, `HUGEINT` comes back
/// `DECIMAL(38,0)` (measured on DuckDB 2.0). Nested types are normalised
/// inside, and the space after a comma is dropped (`MAP(VARCHAR, INTEGER)`).
/// Quoted names inside a `STRUCT` are kept as written.
#[must_use]
pub fn normalize_type(data_type: &str) -> String {
    let mut out = String::with_capacity(data_type.len());
    let mut chars = data_type.trim().chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            out.push(c);
            while let Some(q) = chars.next() {
                out.push(q);
                if q == '"' {
                    if chars.peek() == Some(&'"') {
                        out.push(chars.next().unwrap_or('"'));
                    } else {
                        break;
                    }
                }
            }
        } else if c.is_ascii_alphanumeric() || c == '_' {
            let mut token = String::from(c);
            while let Some(&n) = chars.peek() {
                if n.is_ascii_alphanumeric() || n == '_' {
                    token.push(n);
                    chars.next();
                } else {
                    break;
                }
            }
            out.push_str(match token.as_str() {
                "TINYINT" | "SMALLINT" | "UTINYINT" | "USMALLINT" => "INTEGER",
                "UINTEGER" => "BIGINT",
                "HUGEINT" => "DECIMAL(38,0)",
                other => other,
            });
        } else if c == ',' {
            out.push(',');
            while chars.peek().is_some_and(|n| n.is_whitespace()) {
                chars.next();
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Whether a normalised type is nested (a `LIST`, `STRUCT` or `MAP`).
fn is_nested(normalized: &str) -> bool {
    normalized.ends_with(']')
        || normalized.starts_with("STRUCT(")
        || normalized.starts_with("MAP(")
        || normalized.starts_with("UNION(")
}

/// `DECIMAL(p,s)` as `(p, s)`.
fn decimal_parts(normalized: &str) -> Option<(u32, u32)> {
    let inner = normalized.strip_prefix("DECIMAL(")?.strip_suffix(')')?;
    let (p, s) = inner.split_once(',')?;
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

/// Whether Iceberg can change a column from `from` to `to` in place (both
/// normalised). These are the type promotions Iceberg v2 allows.
#[must_use]
pub fn is_iceberg_widening(from: &str, to: &str) -> bool {
    match (from, to) {
        ("INTEGER", "BIGINT") | ("FLOAT", "DOUBLE") => true,
        _ => match (decimal_parts(from), decimal_parts(to)) {
            (Some((p1, s1)), Some((p2, s2))) => s1 == s2 && p2 > p1,
            _ => false,
        },
    }
}

/// The column changes that make `table`'s columns match `query`'s, as
/// `(name, type)` pairs from `DESCRIBE`. Names compare case-insensitively,
/// as DuckDB resolves them. Adds come first in the query's order, then
/// drops in the table's order, then widenings. A run with no change
/// returns no change.
///
/// # Errors
///
/// A message naming the column when a type change is not an Iceberg
/// widening, or a nested type changes.
pub fn reconcile_columns(
    table: &[(String, String)],
    query: &[(String, String)],
) -> Result<Vec<ColumnChange>, String> {
    let find = |cols: &[(String, String)], name: &str| {
        cols.iter()
            .find(|(n, _)| n.eq_ignore_ascii_case(name))
            .map(|(_, t)| t.clone())
    };
    let mut adds = Vec::new();
    let mut widens = Vec::new();
    for (name, query_type) in query {
        let Some(table_type) = find(table, name) else {
            let data_type = normalize_type(query_type);
            // Measured on DuckDB 2.0: "ADD COLUMN for Nested Types not
            // supported for Iceberg tables". Refused here, before any write.
            if is_nested(&data_type) {
                return Err(format!(
                    "new column '{name}' is {query_type}; DuckDB cannot add a nested column to \
                     an Iceberg table, so drop the table by hand to rebuild it"
                ));
            }
            adds.push(ColumnChange::Add {
                name: name.clone(),
                data_type,
            });
            continue;
        };
        let (from, to) = (normalize_type(&table_type), normalize_type(query_type));
        if from == to {
            continue;
        }
        if is_nested(&from) || is_nested(&to) {
            return Err(format!(
                "column '{name}' changes from {table_type} to {query_type}; a nested type \
                 cannot change in place on Iceberg"
            ));
        }
        if !is_iceberg_widening(&from, &to) {
            return Err(format!(
                "column '{name}' changes from {from} to {to}, which Iceberg cannot do in place \
                 (only INTEGER to BIGINT, FLOAT to DOUBLE and a wider DECIMAL precision); \
                 drop the table by hand to rebuild it"
            ));
        }
        widens.push(ColumnChange::Widen {
            name: name.clone(),
            from,
            to,
        });
    }
    let drops = table
        .iter()
        .filter(|(name, _)| find(query, name).is_none())
        .map(|(name, _)| ColumnChange::Drop { name: name.clone() });
    let mut changes = adds;
    changes.extend(drops);
    changes.extend(widens);
    Ok(changes)
}

/// A column name as a quoted DuckDB identifier.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// The `ALTER TABLE` for one change on `target` (a formatted table ref).
#[must_use]
pub fn alter_sql(target: &str, change: &ColumnChange) -> String {
    match change {
        ColumnChange::Add { name, data_type } => {
            format!(
                "ALTER TABLE {target} ADD COLUMN {} {data_type}",
                quote_ident(name)
            )
        }
        ColumnChange::Drop { name } => {
            format!("ALTER TABLE {target} DROP COLUMN {}", quote_ident(name))
        }
        ColumnChange::Widen { name, to, .. } => format!(
            "ALTER TABLE {target} ALTER COLUMN {} TYPE {to}",
            quote_ident(name)
        ),
    }
}

// ---------------------------------------------------------------------------
// The overwrite transaction
// ---------------------------------------------------------------------------

/// The two things an in-place overwrite asks of a connection. The real one
/// is [`DuckDbConnector`]; tests script it.
pub trait OverwriteSession {
    /// Run one statement.
    ///
    /// # Errors
    ///
    /// The driver's error.
    fn execute(&mut self, sql: &str) -> Result<(), DuckDbError>;

    /// `DESCRIBE <relation>` as `(name, type)` pairs.
    ///
    /// # Errors
    ///
    /// The driver's error.
    fn describe(&mut self, relation: &str) -> Result<Vec<(String, String)>, DuckDbError>;
}

impl OverwriteSession for &DuckDbConnector {
    fn execute(&mut self, sql: &str) -> Result<(), DuckDbError> {
        self.execute_statement(sql)
    }

    fn describe(&mut self, relation: &str) -> Result<Vec<(String, String)>, DuckDbError> {
        let result = self.execute_sql(&format!("DESCRIBE {relation}"))?;
        Ok(result
            .rows
            .iter()
            .filter_map(|row| {
                Some((
                    row.first()?.as_str()?.to_string(),
                    row.get(1)?.as_str()?.to_string(),
                ))
            })
            .collect())
    }
}

/// What a finished in-place overwrite did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverwriteReport {
    /// The statements of the transaction that committed, `BEGIN` to `COMMIT`.
    pub statements: Vec<String>,
    /// How many times the transaction ran (more than one after a commit
    /// conflict).
    pub attempts: u32,
}

/// Whether a driver error is the catalog refusing a commit because another
/// writer committed first. DuckDB-Iceberg 2.0 reports two shapes, both
/// measured against Lakekeeper:
///
/// - its own check: `Failed to commit Iceberg transaction: DELETE on "t"
///   conflicts with a concurrent commit (scanned snapshot …)`;
/// - the catalog's: `Failed to commit Iceberg transaction: Request to '…'
///   returned a non-200 status code (Conflict_409). … CatalogCommitConflicts`.
///
/// Both start with the commit-failure prefix, so a failed statement inside
/// the transaction (which never says that) is never retried.
#[must_use]
pub fn is_commit_conflict(err: &DuckDbError) -> bool {
    let DuckDbError::Database(e) = err else {
        return false;
    };
    let message = e.to_string();
    message.contains("Failed to commit Iceberg transaction")
        && (message.contains("conflicts with a concurrent commit")
            || message.contains("(Conflict_409)")
            || message.contains("CatalogCommitConflicts"))
}

/// Replace every row of `target` with `select_sql`'s rows, reconciling the
/// columns first, in one transaction (module docs). Runs the whole
/// transaction again, up to [`COMMIT_CONFLICT_ATTEMPTS`] times, when the
/// catalog reports a commit conflict.
///
/// # Errors
///
/// [`DuckDbError::InPlaceOverwrite`] for a column change Iceberg cannot
/// make, the driver's error for a failed statement, and
/// [`DuckDbError::InPlaceOverwriteRollbackFailed`] when the rollback after a
/// failure fails too. In every error case but the last, the old table and
/// its rows are unchanged.
pub fn overwrite_in_place<S: OverwriteSession>(
    session: &mut S,
    target: &str,
    select_sql: &str,
) -> Result<OverwriteReport, DuckDbError> {
    let mut attempt = 1;
    loop {
        match overwrite_once(session, target, select_sql) {
            Ok(statements) => {
                return Ok(OverwriteReport {
                    statements,
                    attempts: attempt,
                });
            }
            Err(err) if attempt < COMMIT_CONFLICT_ATTEMPTS && is_commit_conflict(&err) => {
                tracing::warn!(
                    target = target,
                    attempt,
                    "Iceberg commit conflict; running the full refresh transaction again"
                );
                attempt += 1;
            }
            Err(err) => return Err(err),
        }
    }
}

/// The temporary table that holds the model's rows during the overwrite.
/// One connection runs one overwrite at a time (the adapter holds its
/// mutex), so one name is enough.
pub const OVERWRITE_STAGE: &str = rocky_core::sql_gen::IN_PLACE_OVERWRITE_STAGE;

/// One run of the transaction. Any error after `BEGIN` rolls it back.
///
/// The model's rows are materialised into [`OVERWRITE_STAGE`] before the
/// `DELETE`, so a model that reads its own target sees the old rows, as it
/// does with `CREATE OR REPLACE` on every other catalog. Measured on DuckDB
/// 2.0: without the stage, `DELETE` then `INSERT … SELECT … FROM <target>`
/// in one transaction reads the emptied table.
fn overwrite_once<S: OverwriteSession>(
    session: &mut S,
    target: &str,
    select_sql: &str,
) -> Result<Vec<String>, DuckDbError> {
    session.execute("BEGIN TRANSACTION")?;
    let mut statements = vec!["BEGIN TRANSACTION".to_string()];
    let mut committing = false;
    let result = (|| {
        // The newlines keep a trailing `-- comment` in the model SQL from
        // swallowing what follows.
        let stage = format!("CREATE OR REPLACE TEMP TABLE {OVERWRITE_STAGE} AS\n{select_sql}\n");
        session.execute(&stage)?;
        let mut body = vec![stage];
        let table = session.describe(target)?;
        let query = session.describe(OVERWRITE_STAGE)?;
        let changes =
            reconcile_columns(&table, &query).map_err(|detail| DuckDbError::InPlaceOverwrite {
                target: target.to_string(),
                detail,
            })?;
        body.extend(changes.iter().map(|c| alter_sql(target, c)));
        body.push(format!("DELETE FROM {target}"));
        body.push(format!(
            "INSERT INTO {target} BY NAME\nSELECT * FROM {OVERWRITE_STAGE}"
        ));
        for sql in &body[1..] {
            session.execute(sql)?;
        }
        committing = true;
        session.execute("COMMIT")?;
        body.push("COMMIT".to_string());
        Ok(body)
    })();
    match result {
        Ok(body) => {
            statements.extend(body);
            // The stage is temporary; a failed drop only costs memory until
            // the connection closes.
            let _ = session.execute(&format!("DROP TABLE IF EXISTS {OVERWRITE_STAGE}"));
            Ok(statements)
        }
        Err(err) => {
            let err = if committing && !is_commit_conflict(&err) {
                // The catalog may have accepted a commit whose answer was
                // lost; Rocky cannot tell. A conflict is a certain refusal.
                DuckDbError::InPlaceOverwriteCommitUncertain {
                    target: target.to_string(),
                    cause: err.to_string(),
                }
            } else {
                err
            };
            let outcome = match session.execute("ROLLBACK") {
                Ok(()) => Err(err),
                // A failed statement or COMMIT has already ended the transaction.
                Err(rollback) if rollback.to_string().contains("no transaction is active") => {
                    Err(err)
                }
                Err(rollback) => Err(DuckDbError::InPlaceOverwriteRollbackFailed {
                    target: target.to_string(),
                    cause: err.to_string(),
                    rollback: rollback.to_string(),
                }),
            };
            let _ = session.execute(&format!("DROP TABLE IF EXISTS {OVERWRITE_STAGE}"));
            outcome
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, t)| ((*n).to_string(), (*t).to_string()))
            .collect()
    }

    #[test]
    fn normalize_maps_the_measured_round_trip() {
        for (written, read_back) in [
            ("TINYINT", "INTEGER"),
            ("SMALLINT", "INTEGER"),
            ("UTINYINT", "INTEGER"),
            ("USMALLINT", "INTEGER"),
            ("UINTEGER", "BIGINT"),
            ("HUGEINT", "DECIMAL(38,0)"),
            ("STRUCT(x TINYINT)", "STRUCT(x INTEGER)"),
            ("MAP(VARCHAR,INTEGER)", "MAP(VARCHAR, INTEGER)"),
            ("SMALLINT[]", "INTEGER[]"),
        ] {
            assert_eq!(
                normalize_type(written),
                normalize_type(read_back),
                "{written}"
            );
        }
        assert_eq!(
            normalize_type("STRUCT(\"SMALLINT\" INTEGER)"),
            "STRUCT(\"SMALLINT\" INTEGER)"
        );
        assert_ne!(normalize_type("INTEGER"), normalize_type("BIGINT"));
    }

    #[test]
    fn a_rerun_with_no_change_reconciles_to_nothing() {
        let table = cols(&[
            ("id", "INTEGER"),
            ("n", "DECIMAL(38,0)"),
            ("l", "INTEGER[]"),
        ]);
        let query = cols(&[("ID", "UTINYINT"), ("n", "HUGEINT"), ("l", "SMALLINT[]")]);
        assert_eq!(reconcile_columns(&table, &query), Ok(vec![]));
    }

    #[test]
    fn adds_come_before_drops_before_widenings() {
        let table = cols(&[("a", "INTEGER"), ("gone", "VARCHAR"), ("f", "FLOAT")]);
        let query = cols(&[("a", "BIGINT"), ("f", "DOUBLE"), ("new", "TINYINT")]);
        let changes = reconcile_columns(&table, &query).unwrap();
        assert_eq!(
            changes,
            vec![
                ColumnChange::Add {
                    name: "new".into(),
                    data_type: "INTEGER".into()
                },
                ColumnChange::Drop {
                    name: "gone".into()
                },
                ColumnChange::Widen {
                    name: "a".into(),
                    from: "INTEGER".into(),
                    to: "BIGINT".into()
                },
                ColumnChange::Widen {
                    name: "f".into(),
                    from: "FLOAT".into(),
                    to: "DOUBLE".into()
                },
            ]
        );
    }

    #[test]
    fn decimal_widens_only_in_precision() {
        assert!(is_iceberg_widening("DECIMAL(10,2)", "DECIMAL(12,2)"));
        assert!(!is_iceberg_widening("DECIMAL(10,2)", "DECIMAL(12,3)"));
        assert!(!is_iceberg_widening("DECIMAL(12,2)", "DECIMAL(10,2)"));
        assert!(!is_iceberg_widening("INTEGER", "DOUBLE"));
        assert!(!is_iceberg_widening("DATE", "TIMESTAMP"));
    }

    #[test]
    fn a_narrowing_or_unrelated_change_is_refused() {
        for (from, to) in [
            ("BIGINT", "INTEGER"),
            ("VARCHAR", "INTEGER"),
            ("INTEGER", "DOUBLE"),
        ] {
            let err = reconcile_columns(&cols(&[("c", from)]), &cols(&[("c", to)])).unwrap_err();
            assert!(err.contains("column 'c'"), "{err}");
        }
    }

    #[test]
    fn a_nested_change_is_refused() {
        let err = reconcile_columns(
            &cols(&[("s", "STRUCT(x INTEGER)")]),
            &cols(&[("s", "STRUCT(x BIGINT)")]),
        )
        .unwrap_err();
        assert!(err.contains("nested"), "{err}");
    }

    #[test]
    fn a_new_nested_column_is_refused() {
        for nested in ["STRUCT(x INTEGER)", "INTEGER[]", "MAP(VARCHAR, INTEGER)"] {
            let err = reconcile_columns(&cols(&[]), &cols(&[("s", nested)])).unwrap_err();
            assert!(err.contains("cannot add a nested column"), "{err}");
        }
    }

    #[test]
    fn alter_quotes_the_column() {
        let sql = alter_sql(
            "lk.s.t",
            &ColumnChange::Drop {
                name: "we\"ird".into(),
            },
        );
        assert_eq!(sql, "ALTER TABLE lk.s.t DROP COLUMN \"we\"\"ird\"");
    }

    /// A scripted connection: `describe` answers from fixed lists, and
    /// `execute` fails at the statement whose prefix is in `fail_on`.
    struct Scripted {
        table: Vec<(String, String)>,
        query: Vec<(String, String)>,
        fail_on: Vec<(String, DuckDbError)>,
        log: Vec<String>,
        rollback_error: Option<String>,
    }

    impl Scripted {
        fn new(table: &[(&str, &str)], query: &[(&str, &str)]) -> Self {
            Self {
                table: cols(table),
                query: cols(query),
                fail_on: Vec::new(),
                log: Vec::new(),
                rollback_error: None,
            }
        }
        fn failing(mut self, prefix: &str, message: &str) -> Self {
            self.fail_on.push((prefix.to_string(), db_error(message)));
            self
        }
    }

    fn db_error(message: &str) -> DuckDbError {
        DuckDbError::Database(duckdb::Error::InvalidParameterName(message.to_string()))
    }

    impl OverwriteSession for Scripted {
        fn execute(&mut self, sql: &str) -> Result<(), DuckDbError> {
            self.log.push(sql.to_string());
            if sql == "ROLLBACK"
                && let Some(msg) = self.rollback_error.clone()
            {
                return Err(db_error(&msg));
            }
            if let Some(i) = self
                .fail_on
                .iter()
                .position(|(p, _)| sql.starts_with(p.as_str()))
            {
                return Err(self.fail_on.remove(i).1);
            }
            Ok(())
        }
        fn describe(&mut self, relation: &str) -> Result<Vec<(String, String)>, DuckDbError> {
            self.log.push(format!("DESCRIBE {relation}"));
            Ok(if relation == OVERWRITE_STAGE {
                self.query.clone()
            } else {
                self.table.clone()
            })
        }
    }

    #[test]
    fn a_clean_rerun_runs_no_alter() {
        let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]);
        let report = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap();
        assert_eq!(
            report.statements,
            vec![
                "BEGIN TRANSACTION",
                "CREATE OR REPLACE TEMP TABLE temp.main.__rocky_overwrite AS\nSELECT 1 AS a\n",
                "DELETE FROM lk.s.t",
                "INSERT INTO lk.s.t BY NAME\nSELECT * FROM temp.main.__rocky_overwrite",
                "COMMIT"
            ]
        );
        assert_eq!(report.attempts, 1);
        assert!(!s.log.iter().any(|l| l.starts_with("ALTER")));
    }

    #[test]
    fn every_failure_point_rolls_back() {
        for prefix in [
            "CREATE OR REPLACE TEMP",
            "ALTER TABLE",
            "DELETE",
            "INSERT",
            "COMMIT",
        ] {
            let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER"), ("b", "VARCHAR")])
                .failing(prefix, "boom");
            let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a, 'x' AS b").unwrap_err();
            assert!(err.to_string().contains("boom"), "{prefix}: {err}");
            let after = s.log.iter().position(|l| l.starts_with(prefix)).unwrap();
            assert_eq!(
                s.log[after + 1..],
                [
                    "ROLLBACK".to_string(),
                    format!("DROP TABLE IF EXISTS {OVERWRITE_STAGE}")
                ],
                "{prefix}: only the rollback and the stage drop ran after the failure"
            );
        }
    }

    #[test]
    fn a_refused_change_rolls_back_before_any_write() {
        let mut s = Scripted::new(&[("a", "BIGINT")], &[("a", "INTEGER")]);
        let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap_err();
        assert!(matches!(err, DuckDbError::InPlaceOverwrite { .. }), "{err}");
        assert!(
            !s.log
                .iter()
                .any(|l| l.starts_with("DELETE") || l.starts_with("ALTER"))
        );
        assert!(s.log.iter().any(|l| l == "ROLLBACK"));
    }

    #[test]
    fn no_active_transaction_on_rollback_keeps_the_first_error() {
        let mut s =
            Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]).failing("INSERT", "boom");
        s.rollback_error =
            Some("TransactionContext Error: cannot rollback - no transaction is active".into());
        let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap_err();
        assert!(err.to_string().contains("boom"), "{err}");
        assert!(matches!(err, DuckDbError::Database(_)));
    }

    #[test]
    fn a_failed_commit_that_is_not_a_conflict_is_uncertain() {
        let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")])
            .failing("COMMIT", "IO Error: connection reset");
        let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap_err();
        assert!(
            matches!(err, DuckDbError::InPlaceOverwriteCommitUncertain { .. }),
            "{err}"
        );
        assert!(err.to_string().contains("may have committed"), "{err}");
    }

    #[test]
    fn the_stage_reads_the_model_before_the_delete() {
        let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]);
        overwrite_in_place(&mut s, "lk.s.t", "SELECT a FROM lk.s.t -- trailing note").unwrap();
        let stage = s
            .log
            .iter()
            .position(|l| l.starts_with("CREATE OR REPLACE TEMP"))
            .unwrap();
        let delete = s.log.iter().position(|l| l.starts_with("DELETE")).unwrap();
        assert!(stage < delete);
        assert!(
            s.log[stage].ends_with("-- trailing note\n"),
            "{}",
            s.log[stage]
        );
    }

    #[test]
    fn a_failed_rollback_is_reported() {
        let mut s =
            Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]).failing("DELETE", "boom");
        s.rollback_error = Some("connection lost".into());
        let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap_err();
        assert!(
            matches!(err, DuckDbError::InPlaceOverwriteRollbackFailed { .. }),
            "{err}"
        );
    }

    #[test]
    fn a_commit_conflict_runs_the_transaction_again() {
        let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]).failing(
            "COMMIT",
            "TransactionContext Error: Failed to commit: Failed to commit Iceberg \
                 transaction: DELETE on \"t\" conflicts with a concurrent commit (scanned \
                 snapshot 1, now at 2); re-run the DELETE.",
        );
        let report = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap();
        assert_eq!(report.attempts, 2);
        assert_eq!(
            s.log.iter().filter(|l| *l == "BEGIN TRANSACTION").count(),
            2
        );
    }

    #[test]
    fn commit_conflicts_stop_after_the_attempt_limit() {
        let mut s = Scripted::new(&[("a", "INTEGER")], &[("a", "INTEGER")]);
        for _ in 0..COMMIT_CONFLICT_ATTEMPTS {
            s = s.failing(
                "COMMIT",
                "Failed to commit: Failed to commit Iceberg transaction: Request to \
                 'http://lk/catalog/v1' returned a non-200 status code (Conflict_409). \
                 type: CatalogCommitConflicts",
            );
        }
        let err = overwrite_in_place(&mut s, "lk.s.t", "SELECT 1 AS a").unwrap_err();
        assert!(is_commit_conflict(&err));
        assert_eq!(
            s.log.iter().filter(|l| *l == "BEGIN TRANSACTION").count(),
            COMMIT_CONFLICT_ATTEMPTS as usize
        );
    }

    #[test]
    fn only_a_commit_conflict_is_retried() {
        for message in [
            "Conversion Error: value 409 is out of range",
            "Catalog Error: Column 'v' can't be altered (Conflict_409)",
            "Failed to commit Iceberg transaction: Request returned a non-200 status code (Bad_Request_400)",
        ] {
            assert!(!is_commit_conflict(&db_error(message)), "{message}");
        }
        assert!(!is_commit_conflict(&DuckDbError::InPlaceOverwrite {
            target: "t".into(),
            detail: "CatalogCommitConflicts".into(),
        }));
    }
}
