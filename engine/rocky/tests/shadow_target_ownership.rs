//! PROBE for #1273 symptom 1: does an unnamed `--shadow` run write over a
//! pre-existing warehouse object Rocky did not create?
//!
//! This file documents a defect; it does not fix one. See the `#[ignore]` on
//! the test below and the reason in its doc comment.
//!
//! ```text
//!   warehouse before:  main.orders_rocky_shadow  (unrelated, one column
//!                                                 `unrelated_sentinel`)
//!            │
//!   rocky run --shadow  ──►  apply_shadow_rewrite rewrites the model target
//!            │                to `orders_rocky_shadow`
//!            ▼
//!   full_refresh CTAS  ──►  CREATE OR REPLACE TABLE …
//!            │
//!   warehouse after:   main.orders_rocky_shadow  (the MODEL's columns)
//! ```
//!
//! Nothing between the rewrite and the write asks whether the object already
//! exists or who owns it. `apply_shadow_rewrite` (`rocky-cli`,
//! `commands/run.rs`) refuses a derived shadow target that collides with a
//! configured production target or with another selected model's shadow
//! target — both are checks against the PROJECT, not against the warehouse.
//! The transformation executor's only existence probe (`describe_table` in
//! `execute_one_plain_model`) is gated on the strategies that mutate an
//! existing target (Merge / Incremental / DeleteInsert / Microbatch), so a
//! `full_refresh` model reaches its `CREATE OR REPLACE TABLE` having asked
//! the warehouse nothing at all.

use std::fs;
use std::process::Command;

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
type = "transformation"
models = "models"

[pipeline.probe.target.governance]
auto_create_schemas = true
"#;

const SIDECAR: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "probe"
schema = "main"
"#;

/// An unnamed `--shadow` run replaces a pre-existing table at the derived
/// shadow name, with no ownership check.
///
/// **`#[ignore]` on purpose.** It documents the open defect in #1273 rather
/// than guarding a fix: with no ownership or lifecycle contract for shadow
/// objects there is no correct behaviour to assert, so a green CI lane must
/// not depend on the current one. Delete the `#[ignore]` and invert the
/// assertions when #1273 lands a refusal; until then run it with
/// `cargo test -p rocky --test shadow_target_ownership -- --ignored`.
///
/// The pre-existing table carries a column no model produces, so the
/// observation is a schema identity — not a row count, which a same-shape
/// coincidence could satisfy.
#[test]
#[ignore = "documents open defect #1273: shadow objects have no ownership contract"]
fn an_unnamed_shadow_run_replaces_a_pre_existing_table_it_does_not_own() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(&models).expect("create models dir");
    fs::write(root.join("rocky.toml"), CONFIG).expect("write rocky.toml");
    fs::write(
        models.join("orders.sql"),
        "SELECT 1 AS id, 'from-the-model' AS origin\n",
    )
    .expect("write model sql");
    fs::write(models.join("orders.toml"), SIDECAR).expect("write sidecar");

    // Somebody else's table, sitting at the name `--shadow` derives.
    {
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE TABLE main.orders_rocky_shadow AS \
             SELECT 'do-not-touch' AS unrelated_sentinel;",
        )
        .expect("seed the pre-existing shadow-name table");
    }
    assert_eq!(
        columns_of(root, "orders_rocky_shadow"),
        vec!["unrelated_sentinel".to_string()],
        "precondition: the pre-existing table is the one we seeded"
    );

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "run", "--shadow", "--output", "json"])
        .current_dir(root)
        .env("RUST_LOG", "error")
        .output()
        .expect("rocky must launch");

    let after = columns_of(root, "orders_rocky_shadow");
    assert!(
        !after.contains(&"unrelated_sentinel".to_string()),
        "the pre-existing table survived — a guard has landed, so #1273 \
         symptom 1 is fixed and this probe should be inverted. columns: \
         {after:?}\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
    assert_eq!(
        after,
        vec!["id".to_string(), "origin".to_string()],
        "the shadow run replaced the unrelated table with the model's own \
         columns\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Column names of `main.<table>`, in ordinal order. Empty when absent.
fn columns_of(root: &std::path::Path, table: &str) -> Vec<String> {
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).expect("reopen duckdb");
    let mut stmt = conn
        .prepare(
            "SELECT column_name FROM information_schema.columns \
             WHERE table_schema = 'main' AND table_name = ? \
             ORDER BY ordinal_position",
        )
        .expect("prepare");
    let rows = stmt
        .query_map([table], |r| r.get::<_, String>(0))
        .expect("query");
    rows.map(|r| r.expect("row")).collect()
}
