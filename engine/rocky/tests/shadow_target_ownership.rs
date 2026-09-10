//! #1273: a shadow object is disposable and belongs to the run that made it.
//!
//! This file began as a PROBE that documented the open defect — an unnamed
//! `--shadow` run replaced a pre-existing warehouse object at the derived
//! shadow name, having asked the warehouse nothing. Its own doc comment said
//! to invert it when a refusal landed. That is what these tests are now.
//!
//! ```text
//!   warehouse before   main.orders_rocky_shadow   (somebody else's, one
//!                                                  column `unrelated_sentinel`)
//!            │
//!   rocky run --shadow ──▶ ownership preflight ──▶ REFUSE, naming the object
//!            │                                     and printing the DROP
//!            ▼
//!   warehouse after    main.orders_rocky_shadow   (untouched)
//! ```
//!
//! The two tests are the two halves of the contract, and neither means much
//! alone: a run refuses to write a name it does not own, and a run that DID
//! own its name leaves nothing behind — which is what makes "already there"
//! a sound signal in the first place.

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

/// Lay out a project with one `full_refresh` model targeting
/// `probe.main.orders`, whose derived shadow name is
/// `orders_rocky_shadow`.
fn project(root: &std::path::Path) {
    let models = root.join("models");
    fs::create_dir_all(&models).expect("create models dir");
    fs::write(root.join("rocky.toml"), CONFIG).expect("write rocky.toml");
    fs::write(
        models.join("orders.sql"),
        "SELECT 1 AS id, 'from-the-model' AS origin\n",
    )
    .expect("write model sql");
    fs::write(models.join("orders.toml"), SIDECAR).expect("write sidecar");
}

fn run_shadow(root: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "run", "--shadow", "--output", "json"])
        .current_dir(root)
        .env("RUST_LOG", "error")
        .output()
        .expect("rocky must launch")
}

/// An unnamed `--shadow` run REFUSES a pre-existing object at the derived
/// shadow name, and leaves it exactly as it found it.
///
/// The pre-existing table carries a column no model produces, so the
/// observation is a schema identity — not a row count, which a same-shape
/// coincidence could satisfy.
#[test]
fn an_unnamed_shadow_run_refuses_a_pre_existing_table_it_does_not_own() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    project(root);

    // Somebody else's table, sitting at the name `--shadow` derives.
    {
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE TABLE main.orders_rocky_shadow AS \
             SELECT 'do-not-touch' AS unrelated_sentinel;",
        )
        .expect("seed the pre-existing shadow-name table");
    }

    let out = run_shadow(root);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);

    // The CONSEQUENCE first, so a revert fails on the thing that matters —
    // the object surviving — rather than on the wording of a message. This
    // is the assertion the original probe watched fail.
    assert_eq!(
        columns_of(root, "orders_rocky_shadow"),
        vec!["unrelated_sentinel".to_string()],
        "the pre-existing table must be untouched\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        !out.status.success(),
        "the run must refuse, not proceed\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );

    // The refusal reaches the operator through the JSON `errors` array,
    // which is where `--output json` puts a run failure; stderr carries the
    // one-line summary. Assert over both so the test does not pin which.
    let reported = format!("{stdout}{stderr}");
    assert!(
        reported.contains("orders_rocky_shadow"),
        "the refusal names the occupied object\n{reported}"
    );
    assert!(
        reported.contains("does not own"),
        "the refusal says why\n{reported}"
    );
    assert!(
        reported.contains("DROP TABLE"),
        "the refusal prints the remedy\n{reported}"
    );
}

/// A shadow run that DOES own its name succeeds and leaves nothing behind:
/// `cleanup_after` defaults on for a one-off `--shadow`, so the object it
/// created is dropped.
///
/// This is what makes the refusal above sound rather than merely strict —
/// without it, every second shadow run would trip over its own leftover.
/// Running twice proves it: the second run cannot succeed unless the first
/// one cleaned up after itself.
#[test]
fn a_shadow_run_cleans_up_after_itself_so_the_next_one_can_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();
    project(root);

    // Production exists, so the shadow run has something to be a shadow OF
    // — and gives the run a real schema to write into.
    {
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).expect("open duckdb");
        conn.execute_batch("CREATE TABLE main.orders AS SELECT 0 AS id, 'prod' AS origin;")
            .expect("seed production");
    }

    for attempt in 1..=2 {
        let out = run_shadow(root);
        assert!(
            out.status.success(),
            "shadow run {attempt} must succeed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr),
        );
        assert!(
            columns_of(root, "orders_rocky_shadow").is_empty(),
            "run {attempt} must leave no shadow object behind — cleanup_after \
             defaults on for a one-off --shadow"
        );
    }

    // Production is untouched by either run: the shadow never wrote there.
    assert_eq!(
        columns_of(root, "orders"),
        vec!["id".to_string(), "origin".to_string()],
        "production must be untouched"
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
