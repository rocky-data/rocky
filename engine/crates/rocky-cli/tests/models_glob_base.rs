//! #1268: a `models` glob's base directory is derived once, by
//! `models_loader::locate_models_dir`, for every caller that decides whether to
//! build.
//!
//! What is pinned here, end to end through `run()` against a real DuckDB file:
//! - **`models = "models/*.sql"` materializes its models.** RED before this
//!   change: `run` split the glob on `**` alone, kept `models/*.sql` intact,
//!   probed it as a literal directory, never found it, took the `Absent` branch
//!   and exited 0 having built nothing. The assertion is on the **table in the
//!   warehouse**, not on a planned materialization — a run that plans work and
//!   builds none is exactly the failure being closed.
//! - **A glob resolving outside the project root is refused.** This is a new
//!   refusal surface, so it is probed rather than assumed: the escaping
//!   directory really contains a loadable model, so a run that does not refuse
//!   would visibly build it.
//!
//! Scope: these exercise plain `run()` only, which is where the decision is
//! built inline and therefore unreachable from a unit test. They do **not**
//! pin the other consumers of the shared derivation (`tick`, `run --dag`,
//! `scope`, `validate`) — `validate`'s share is covered by its own unit tests,
//! and the unit-level derivation table lives in `models_loader`'s tests.

#![cfg(feature = "duckdb")]

use std::path::Path;
use std::sync::Arc;

use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

/// Count tables named `orders` in `main`, the target every fixture here writes.
async fn orders_tables(db: &Path) -> u64 {
    let adapter = DuckDbWarehouseAdapter::open(db).expect("verify open");
    let conn = adapter.shared_connector();
    let guard = conn.lock().unwrap();
    let out = guard
        .execute_sql(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_name = 'orders'",
        )
        .expect("information_schema query");
    rocky_core::checks::cell_as_u64(out.rows[0].first())
        .unwrap_or_else(|| panic!("expected integer cell, got {:?}", out.rows[0][0]))
}

/// A transformation project whose single model selects a literal, so nothing
/// needs seeding. `models_glob` is the field under test.
fn write_project(root: &Path, db: &Path, models_glob: &str, models_dir: &Path) {
    std::fs::create_dir_all(models_dir).expect("mkdir models");
    std::fs::write(
        models_dir.join("orders.sql"),
        "SELECT 1 AS id, 'widget' AS name\n",
    )
    .expect("write sql");
    // The DuckDB catalog is the database file's stem.
    let catalog = db
        .file_stem()
        .and_then(|s| s.to_str())
        .expect("db file stem");
    std::fs::write(
        models_dir.join("orders.toml"),
        format!(
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"{catalog}\"\nschema = \"main\"\n"
        ),
    )
    .expect("write sidecar");
    std::fs::write(
        root.join("rocky.toml"),
        format!(
            r#"
[adapter]
type = "duckdb"
path = "{}"

[pipeline.silver]
type = "transformation"
models = "{models_glob}"

[pipeline.silver.target]

[pipeline.silver.target.governance]
auto_create_schemas = true
"#,
            db.display()
        ),
    )
    .expect("write config");
}

/// Invoke `run()` exactly as the CLI does for a plain `rocky run`, with no
/// `--models` override so the config's glob is what gets derived.
async fn run_pipeline(config_path: &Path, state_path: &Path) -> anyhow::Result<()> {
    let loaded = Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path).expect("load config"),
    );
    rocky_cli::commands::run(
        config_path,
        loaded,
        None, // filter
        None, // pipeline_name_arg — single pipeline resolves
        state_path,
        None,  // governance_override
        false, // output_json
        None,  // models_dir — the point: the glob must be derived from config
        false, // run_all
        None,  // resume_run_id
        false, // resume_latest
        None,  // shadow_config
        &rocky_cli::commands::PartitionRunOptions::default(),
        None, // model_name_filter
        None, // cache_ttl_override
        None, // idempotency_key
        None, // env
        &rocky_cli::commands::DeferOptions::default(),
        &rocky_cli::commands::SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None, // run_id_override
        None, // governed_ctx
        false,
    )
    .await
}

/// `models = "models/*.sql"` is a supported glob shape. Before #1268 the base
/// derivation split on `**` alone, so this project built nothing and reported
/// success.
///
/// Mutation that must turn this red: restore
/// `t.models.split("**").next()...` in `run`'s session gate.
#[tokio::test]
async fn a_wildcard_file_glob_materializes_its_models() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/*.sql", &root.join("models"));

    run_pipeline(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("the run must succeed");

    assert_eq!(
        orders_tables(&db).await,
        1,
        "a `models/*.sql` glob must materialize its model; 0 means the base \
         derivation took the Absent branch and the run silently built nothing"
    );
}

/// The `**` form must keep working — the fix is a widening of which shapes
/// resolve, not a replacement.
#[tokio::test]
async fn the_recursive_glob_form_still_materializes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/**", &root.join("models"));

    run_pipeline(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("the run must succeed");

    assert_eq!(orders_tables(&db).await, 1, "`models/**` must still build");
}

/// A `models` glob pointing outside the project root is refused, not executed.
///
/// Probed rather than asserted: `outside/` holds a genuinely loadable model
/// writing to the same target as the passing cases above, so a run that failed
/// to refuse would build a table here. Both halves are checked — the error names
/// the containment breach, AND nothing was materialized.
///
/// Mutation that must turn this red: drop the containment check from
/// `locate_models_dir`, or have `run` join the base without calling it.
#[tokio::test]
async fn an_escaping_models_glob_is_refused_and_builds_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).expect("mkdir project");
    let db = root.join("wh.duckdb");
    // The models live OUTSIDE the project root, and really do load.
    write_project(&root, &db, "../outside/**", &dir.path().join("outside"));

    let state_path = root.join(".rocky-state.redb");
    assert!(!db.exists(), "sanity: no database before the run");

    let err = run_pipeline(&root.join("rocky.toml"), &state_path)
        .await
        .expect_err("a glob escaping the project root must be refused");
    let rendered = format!("{err:#}");
    assert!(
        rendered.contains("outside the project root"),
        "the refusal must name the containment breach, got: {rendered}"
    );

    // Unconditional, and stronger than "the escaped model's target is absent":
    // the refusal must land before ANY warehouse work, so the database file the
    // adapter would open must never come into existence at all. Asserting only
    // that `orders` is missing would pass for a regression that created the
    // database, or mutated some other object, before failing.
    assert!(
        !db.exists(),
        "the refusal must precede any warehouse mutation, but the database was created"
    );
    assert!(
        !state_path.exists(),
        "a refused run must not open a state session"
    );
}
