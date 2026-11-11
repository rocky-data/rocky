//! The staged replace (ADR-REPLACE, RD-009) driven through Trino's real
//! dialect against a scripted catalog: every failure point of
//! `StagedSwap::execute` leaves the old data under the target name, or under
//! a name the next run restores from.
//!
//! The scripted catalog follows what the spike measured on Trino 484: CTAS
//! refuses an existing name, rename refuses an existing destination, and
//! `ALTER TABLE IF EXISTS` on a missing table is a no-op.

use std::collections::BTreeMap;
use std::sync::Mutex;

use async_trait::async_trait;
use rocky_core::failure_class::{FailureClass, TransientKind};
use rocky_core::replace::{ReplaceError, StagedSwap, SwapNames, recover_interrupted_swap};
use rocky_core::traits::{AdapterError, AdapterResult, QueryResult, SqlDialect, WarehouseAdapter};
use rocky_ir::{ColumnInfo, TableRef};
use rocky_trino::dialect::TrinoDialect;

/// One catalog: formatted table ref → the rows' label.
struct Catalog {
    dialect: TrinoDialect,
    tables: Mutex<BTreeMap<String, String>>,
    log: Mutex<Vec<String>>,
    /// A statement starting with one of these fails without effect.
    fail: Mutex<Vec<String>>,
    /// Probing one of these tables fails with a permanent error, which
    /// reads as absent (a metastore that cannot see the table).
    blind: Mutex<Vec<String>>,
    /// Probing one of these tables fails with a retryable error.
    flaky: Mutex<Vec<String>>,
}

impl Catalog {
    fn new(tables: &[(&str, &str)]) -> Self {
        Self {
            dialect: TrinoDialect::new(),
            tables: Mutex::new(
                tables
                    .iter()
                    .map(|(t, d)| (fq(t), (*d).to_string()))
                    .collect(),
            ),
            log: Mutex::new(Vec::new()),
            fail: Mutex::new(Vec::new()),
            blind: Mutex::new(Vec::new()),
            flaky: Mutex::new(Vec::new()),
        }
    }
    fn fail_on(&self, prefix: String) {
        self.fail.lock().unwrap().push(prefix);
    }
    fn table(&self, name: &str) -> Option<String> {
        self.tables.lock().unwrap().get(&fq(name)).cloned()
    }
    fn names(&self) -> Vec<String> {
        self.tables.lock().unwrap().keys().cloned().collect()
    }
    fn log(&self) -> Vec<String> {
        self.log.lock().unwrap().clone()
    }
}

fn fq(table: &str) -> String {
    format!("\"lake\".\"marts\".\"{table}\"")
}

fn tref(table: &str) -> TableRef {
    TableRef {
        catalog: "lake".into(),
        schema: "marts".into(),
        table: table.into(),
    }
}

fn err(msg: impl Into<String>) -> AdapterError {
    AdapterError::msg(msg.into())
}

#[async_trait]
impl WarehouseAdapter for Catalog {
    fn dialect(&self) -> &dyn SqlDialect {
        &self.dialect
    }

    async fn execute_statement(&self, sql: &str) -> AdapterResult<()> {
        self.log.lock().unwrap().push(sql.to_string());
        if self.fail.lock().unwrap().iter().any(|p| sql.starts_with(p)) {
            return Err(err(format!("scripted failure: {sql}")));
        }
        let mut tables = self.tables.lock().unwrap();
        if let Some(rest) = sql.strip_prefix("CREATE TABLE ") {
            let (name, _) = rest.split_once(" AS").ok_or_else(|| err("bad CTAS"))?;
            if tables.contains_key(name) {
                return Err(err(format!("Table '{name}' already exists")));
            }
            tables.insert(name.to_string(), "new".into());
            return Ok(());
        }
        if let Some(rest) = sql.strip_prefix("ALTER TABLE ") {
            let (if_exists, rest) = match rest.strip_prefix("IF EXISTS ") {
                Some(r) => (true, r),
                None => (false, rest),
            };
            let (from, to) = rest
                .split_once(" RENAME TO ")
                .ok_or_else(|| err("bad ALTER"))?;
            let Some(data) = tables.get(from).cloned() else {
                return if if_exists {
                    Ok(())
                } else {
                    Err(err(format!("Table '{from}' does not exist")))
                };
            };
            if tables.contains_key(to) {
                return Err(err(format!("Target table '{to}' already exists")));
            }
            tables.remove(from);
            tables.insert(to.to_string(), data);
            return Ok(());
        }
        if let Some(name) = sql.strip_prefix("DROP TABLE IF EXISTS ") {
            tables.remove(name);
            return Ok(());
        }
        Err(err(format!("unscripted statement: {sql}")))
    }

    async fn execute_query(&self, _sql: &str) -> AdapterResult<QueryResult> {
        Err(err("no queries in this script"))
    }

    fn classify_failure(&self, e: &AdapterError) -> FailureClass {
        if e.to_string().contains("timed out") {
            FailureClass::Transient(TransientKind::Timeout)
        } else {
            FailureClass::Permanent
        }
    }

    async fn describe_table(&self, table: &TableRef) -> AdapterResult<Vec<ColumnInfo>> {
        let name = self
            .dialect
            .format_table_ref(&table.catalog, &table.schema, &table.table)?;
        if self.flaky.lock().unwrap().contains(&name) {
            return Err(err(format!("describe {name} timed out")));
        }
        if self.blind.lock().unwrap().contains(&name) {
            return Err(err(format!("metastore error describing {name}")));
        }
        if self.tables.lock().unwrap().contains_key(&name) {
            Ok(vec![ColumnInfo {
                name: "id".into(),
                data_type: "integer".into(),
                nullable: true,
            }])
        } else {
            Err(err(format!("table {name} not found")))
        }
    }
}

fn swap() -> StagedSwap {
    let dialect = TrinoDialect::new();
    let names = SwapNames::for_target(&tref("orders")).unwrap();
    let build = dialect.create_table_as_new(&fq("orders__rocky_stage"), "SELECT 1 AS id");
    let create = dialect.create_table_as_new(&fq("orders"), "SELECT 1 AS id");
    StagedSwap::new(&dialect, names, build, create).unwrap()
}

#[tokio::test]
async fn a_full_refresh_replaces_the_table_and_leaves_nothing_behind() {
    let wh = Catalog::new(&[("orders", "old")]);
    let outcome = swap().execute(&wh).await.unwrap();
    assert_eq!(wh.table("orders").as_deref(), Some("new"));
    assert_eq!(wh.names(), [fq("orders")]);
    assert!(outcome.recovered.is_none() && outcome.warning.is_none());
    assert!(
        !wh.log()
            .iter()
            .any(|s| s == &format!("DROP TABLE IF EXISTS {}", fq("orders"))),
        "the target is never dropped: {:?}",
        wh.log()
    );
}

/// A first build creates the target directly, so it needs no rename and
/// works on a catalog that cannot rename tables (Hive on Glue).
#[tokio::test]
async fn a_first_run_creates_the_target_without_a_rename() {
    let wh = Catalog::new(&[]);
    wh.fail_on("ALTER TABLE".into());
    swap().execute(&wh).await.unwrap();
    assert_eq!(wh.table("orders").as_deref(), Some("new"));
    assert_eq!(wh.names(), [fq("orders")]);
    assert_eq!(
        wh.log(),
        [format!("CREATE TABLE {} AS\nSELECT 1 AS id", fq("orders"))]
    );
}

/// A stage the probe could not see (or that a concurrent run made after the
/// probe) makes the build fail with "already exists". That table is not this
/// run's, so it is kept.
#[tokio::test]
async fn a_stage_this_run_did_not_make_is_never_dropped() {
    let wh = Catalog::new(&[("orders", "old"), ("orders__rocky_stage", "theirs")]);
    wh.blind.lock().unwrap().push(fq("orders__rocky_stage"));
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::StageTaken { .. }), "{e}");
    assert_eq!(wh.table("orders__rocky_stage").as_deref(), Some("theirs"));
    assert_eq!(wh.table("orders").as_deref(), Some("old"));
    assert!(
        !wh.log().iter().any(|s| s.starts_with("DROP")),
        "{:?}",
        wh.log()
    );
}

/// A retryable probe failure means absence cannot be assumed: the swap stops
/// before it runs any statement.
#[tokio::test]
async fn a_retryable_probe_failure_runs_nothing() {
    for table in ["orders", "orders__rocky_old", "orders__rocky_stage"] {
        let wh = Catalog::new(&[("orders", "old")]);
        wh.flaky.lock().unwrap().push(fq(table));
        let e = swap().execute(&wh).await.unwrap_err();
        assert!(matches!(e, ReplaceError::Probe { .. }), "{table}: {e}");
        assert!(wh.log().is_empty(), "{table}: {:?}", wh.log());
        assert_eq!(wh.table("orders").as_deref(), Some("old"));
    }
}

/// RD-009 itself: the build fails, and the old table must survive.
#[tokio::test]
async fn a_failed_build_leaves_the_old_table_untouched() {
    let wh = Catalog::new(&[("orders", "old")]);
    wh.fail_on(format!("CREATE TABLE {}", fq("orders__rocky_stage")));
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::Build { .. }), "{e}");
    assert_eq!(wh.table("orders").as_deref(), Some("old"));
    assert_eq!(wh.names(), [fq("orders")]);
}

#[tokio::test]
async fn a_failed_move_aside_leaves_the_old_table_and_drops_the_stage() {
    let wh = Catalog::new(&[("orders", "old")]);
    wh.fail_on(format!("ALTER TABLE IF EXISTS {}", fq("orders")));
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::MoveAside { .. }), "{e}");
    assert_eq!(wh.table("orders").as_deref(), Some("old"));
    assert_eq!(wh.names(), [fq("orders")]);
}

#[tokio::test]
async fn a_failed_swap_in_rolls_the_old_table_back() {
    let wh = Catalog::new(&[("orders", "old")]);
    wh.fail_on(format!("ALTER TABLE {} RENAME", fq("orders__rocky_stage")));
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::SwapIn { .. }), "{e}");
    assert_eq!(wh.table("orders").as_deref(), Some("old"));
    assert_eq!(wh.names(), [fq("orders")]);
}

#[tokio::test]
async fn a_failed_rollback_names_the_rename_that_restores_the_table() {
    let wh = Catalog::new(&[("orders", "old")]);
    wh.fail_on(format!("ALTER TABLE {} RENAME", fq("orders__rocky_stage")));
    wh.fail_on(format!("ALTER TABLE {} RENAME", fq("orders__rocky_old")));
    let e = swap().execute(&wh).await.unwrap_err();
    let ReplaceError::RestoreFailed { restore_sql, .. } = &e else {
        panic!("expected RestoreFailed, got {e}");
    };
    assert_eq!(
        restore_sql,
        &format!(
            "ALTER TABLE {} RENAME TO {}",
            fq("orders__rocky_old"),
            fq("orders")
        )
    );
    assert!(e.to_string().contains(restore_sql.as_str()));
    // The old data is safe under the backup name, and the next run's
    // recovery puts it back.
    assert_eq!(wh.table("orders__rocky_old").as_deref(), Some("old"));
    wh.fail.lock().unwrap().clear();
    let recovered = recover_interrupted_swap(&wh, &tref("orders"))
        .await
        .unwrap();
    assert!(recovered.is_some());
    assert_eq!(wh.table("orders").as_deref(), Some("old"));
}

#[tokio::test]
async fn a_failed_backup_drop_succeeds_with_a_warning() {
    let wh = Catalog::new(&[("orders", "old")]);
    wh.fail_on(format!("DROP TABLE IF EXISTS {}", fq("orders__rocky_old")));
    let outcome = swap().execute(&wh).await.unwrap();
    assert_eq!(wh.table("orders").as_deref(), Some("new"));
    let warning = outcome.warning.expect("a warning names the leftover");
    assert!(warning.contains(&fq("orders__rocky_old")), "{warning}");
    // The next replace refuses rather than guessing the backup is its own.
    wh.fail.lock().unwrap().clear();
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::LeftoverBackup { .. }), "{e}");
    assert_eq!(wh.table("orders__rocky_old").as_deref(), Some("old"));
}

/// A run died between moving the old table aside and renaming the new one
/// in: the target is missing and the backup holds the old data. The next
/// swap restores it first, then replaces it.
#[tokio::test]
async fn a_swap_restores_a_backup_an_interrupted_run_left() {
    let wh = Catalog::new(&[
        ("orders__rocky_old", "old"),
        ("orders__rocky_stage", "half"),
    ]);
    // A leftover stage too: refused, never dropped.
    let e = swap().execute(&wh).await.unwrap_err();
    assert!(matches!(e, ReplaceError::LeftoverStage { .. }), "{e}");
    assert_eq!(wh.table("orders").as_deref(), Some("old"), "restored first");
    assert_eq!(wh.table("orders__rocky_stage").as_deref(), Some("half"));

    let wh = Catalog::new(&[("orders__rocky_old", "old")]);
    let outcome = swap().execute(&wh).await.unwrap();
    assert!(outcome.recovered.is_some());
    assert_eq!(wh.table("orders").as_deref(), Some("new"));
    assert_eq!(wh.names(), [fq("orders")]);
}

#[tokio::test]
async fn recovery_never_touches_a_live_target() {
    let wh = Catalog::new(&[("orders", "live"), ("orders__rocky_old", "old")]);
    assert!(
        recover_interrupted_swap(&wh, &tref("orders"))
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(wh.table("orders").as_deref(), Some("live"));
    assert_eq!(wh.table("orders__rocky_old").as_deref(), Some("old"));
    assert!(wh.log().is_empty(), "probes only: {:?}", wh.log());
}

/// Run as a plain list (the way quarantine runs it), every prefix of the
/// statements leaves the old data at the target or at the backup.
#[tokio::test]
async fn every_prefix_of_the_plain_statement_list_keeps_the_old_data() {
    let statements = swap().statements();
    for stop in 0..=statements.len() {
        let wh = Catalog::new(&[("orders", "old")]);
        for sql in &statements[..stop] {
            wh.execute_statement(sql).await.unwrap();
        }
        let old_is_reachable = wh.table("orders").as_deref() == Some("old")
            || wh.table("orders__rocky_old").as_deref() == Some("old");
        let done = wh.table("orders").as_deref() == Some("new");
        assert!(
            old_is_reachable || done,
            "after {stop} statements: {:?}",
            wh.names()
        );
    }
}
