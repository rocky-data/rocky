//! #1594: a refused `metadata_columns` value must land before any GOVERNANCE
//! operation and before any table copy.
//!
//! `value` carries `{placeholder}`s that `rocky` fills from source schema
//! names read back from the warehouse. Refusing one is correct; refusing it
//! *late* is not. The setup loop creates catalogs and schemas, sets tags,
//! binds workspaces and applies grants, so a refusal raised while collecting
//! tables would abort only after access control had already changed.
//!
//! These tests run the real binary against a DuckDB fixture and read the
//! warehouse back to answer one question: on refusal, was the target schema
//! created?
//!
//! # What this claim does NOT cover — read before widening it
//!
//! Two things happen before the guard on every `rocky run`, whatever the
//! outcome, and neither is this guard's doing:
//!
//! - **The destination database file.** Adapter construction opens it, which
//!   creates it when absent. `rocky discover`, a read-only command, creates it
//!   too.
//! - **The state store, and the end-of-run retention sweep.** The store opens
//!   before any command logic, and the sweep runs on the `Err` path by design
//!   (see the comment at the sweep site in `commands/run.rs`). The
//!   target-collision refusal that shares this preflight block behaves
//!   identically.
//!
//! # What this fixture can and cannot observe
//!
//! DuckDB's `create_catalog_sql` returns `None`, so `auto_create_schemas` is
//! the FIRST governance mutation observable on this adapter — there is no
//! catalog step to sit between the guard and the assertion. A guard placed
//! between catalog and schema creation on a catalog-bearing warehouse would
//! still pass here; proving that needs a recording adapter, which the tree
//! does not have yet.

use std::fs;
use std::process::Command;

/// The source schema `raw__ship-it` parses fine — `SchemaPattern::parse`
/// splits on the separator and checks nothing — so `{source}` resolves to
/// `ship-it`, which is not a plain SQL identifier.
///
/// The target schema template deliberately does NOT use `{source}`, so the
/// only thing `TARGET_ONLY` below changes is the metadata column — which makes
/// it the control for *when* the run stops, not whether it stops.
const WITH_METADATA: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[[pipeline.ingest.metadata_columns]]
name = "_src"
type = "VARCHAR"
value = "'{source}'"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

/// Same pipeline with no `metadata_columns` block at all.
const TARGET_ONLY: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

fn seed(dir: &std::path::Path, schema: &str) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(&format!(
        "CREATE SCHEMA \"{schema}\";
         CREATE TABLE \"{schema}\".orders AS SELECT * FROM (VALUES (1),(2)) t(id);"
    ))
    .expect("seed source");
}

fn table_exists(dir: &std::path::Path, schema: &str, table: &str) -> bool {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let mut stmt = conn
        .prepare(
            "SELECT count(*) FROM information_schema.tables \
             WHERE table_schema = ? AND table_name = ?",
        )
        .expect("prepare");
    let n: i64 = stmt
        .query_row([schema, table], |r| r.get(0))
        .expect("query");
    n > 0
}

fn schema_exists(dir: &std::path::Path, schema: &str) -> bool {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let mut stmt = conn
        .prepare("SELECT count(*) FROM information_schema.schemata WHERE schema_name = ?")
        .expect("prepare");
    let n: i64 = stmt.query_row([schema], |r| r.get(0)).expect("query");
    n > 0
}

fn run(dir: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        // Pin the state store inside the temp dir so the reset between the
        // control run and the guarded run is complete — otherwise a leftover
        // store could route the second run down a resume or idempotency path
        // and the observation would not be about the guard.
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .arg("run")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run rocky")
}

/// The property under test, stated as a delta against the same fixture
/// without the metadata column:
///
/// ```text
///   no metadata column   ──►  setup creates `staging`  ──►  copy FAILS
///                                    (mutation left behind)
///
///   with metadata column ──►  preflight REFUSES        ──►  setup never runs
///                                    (no mutation)
/// ```
///
/// The copy fails either way: `format_table_ref` refuses the same
/// non-identifier schema one frame later when it builds `FROM
/// <catalog>.<schema>.<table>`. That is exactly why the control is the right
/// control — the only thing that differs between the two runs is *when* the
/// run stops, and therefore whether the target schema exists afterwards.
#[test]
fn a_refused_metadata_value_lands_before_governance_setup() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "raw__ship-it");

    // Control: no metadata column. The run still fails, but only after
    // governance setup has created the target schema.
    fs::write(dir.join("rocky.toml"), TARGET_ONLY).expect("write config");
    let late = run(dir);
    assert!(
        !late.status.success(),
        "control: the copy must fail on the non-identifier source schema"
    );
    assert!(
        schema_exists(dir, "staging"),
        "control: setup must have created the target schema before failing — \
         without this the assertion below proves nothing"
    );

    // Reset the warehouse AND the state store so the observation is about
    // THIS run.
    fs::remove_file(dir.join("fixture.duckdb")).expect("drop fixture");
    let _ = fs::remove_file(dir.join("state.redb"));
    let _ = fs::remove_dir_all(dir.join(".rocky"));
    seed(dir, "raw__ship-it");
    assert!(!schema_exists(dir, "staging"));

    fs::write(dir.join("rocky.toml"), WITH_METADATA).expect("write config");
    let refused = run(dir);
    assert!(
        !refused.status.success(),
        "a non-identifier schema component in a metadata value must be refused; stdout: {}",
        String::from_utf8_lossy(&refused.stdout)
    );
    let stderr = String::from_utf8_lossy(&refused.stderr);
    let stdout = String::from_utf8_lossy(&refused.stdout);
    let message = format!("{stderr}{stdout}");
    assert!(
        message.contains("metadata_columns") && message.contains("ship-it"),
        "the refusal must name the field and the offending component; got: {message}"
    );

    assert!(
        !schema_exists(dir, "staging"),
        "the refusal must land BEFORE governance setup — the target schema was created"
    );
    // Weaker than the assertion above and deliberately kept: the control run
    // also stops before writing this table, so it does not discriminate
    // between the two runs. It guards the other direction — a future change
    // that moved the guard past the copy.
    assert!(
        !table_exists(dir, "staging", "orders"),
        "the refusal must land before the copy — the target table was written"
    );
}

/// A clean source schema still runs end to end with the metadata column, so
/// the guard refuses the hostile shape and nothing else.
#[test]
fn a_plain_identifier_component_still_resolves_and_runs() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    seed(dir, "raw__shopify");
    fs::write(dir.join("rocky.toml"), WITH_METADATA).expect("write config");

    let out = run(dir);
    assert!(
        out.status.success(),
        "a benign component must resolve; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(schema_exists(dir, "staging"));

    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let mut stmt = conn
        .prepare("SELECT DISTINCT _src FROM staging.orders")
        .expect("prepare");
    let value: String = stmt.query_row([], |r| r.get(0)).expect("read _src");
    assert_eq!(value, "shopify", "the resolved value must reach the target");
}
