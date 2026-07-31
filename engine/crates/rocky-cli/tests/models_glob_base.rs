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
//! - **`models = "models/ord*.sql"` materializes its models.** RED before
//!   #1293: the shared derivation retained the literal filename prefix and
//!   probed `models/ord` as a directory.
//! - **`models = "models/orders.sql"` materializes exactly that model.** A
//!   wildcard-free model path is still a valid file glob; it must not be
//!   treated as a directory and expanded to `models/orders.sql/**`.
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

/// Count a named table in `main`.
async fn named_tables(db: &Path, table: &str) -> u64 {
    let adapter = DuckDbWarehouseAdapter::open(db).expect("verify open");
    let conn = adapter.shared_connector();
    let guard = conn.lock().unwrap();
    let out = guard
        .execute_sql(&format!(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'main' AND table_name = '{table}'"
        ))
        .expect("information_schema query");
    rocky_core::checks::cell_as_u64(out.rows[0].first())
        .unwrap_or_else(|| panic!("expected integer cell, got {:?}", out.rows[0][0]))
}

/// A transformation project whose models select literals, so nothing needs
/// seeding. `models_glob` is the field under test.
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
        models_dir.join("customers.sql"),
        "SELECT 2 AS id, 'customer' AS name\n",
    )
    .expect("write non-matching sql");
    std::fs::write(
        models_dir.join("customers.toml"),
        format!(
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"{catalog}\"\nschema = \"main\"\n"
        ),
    )
    .expect("write non-matching sidecar");
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

/// Invoke the model-scoped path used by `rocky run --pipeline silver --model
/// orders`, still without a `--models` override.
async fn run_pipeline_model(config_path: &Path, state_path: &Path) -> anyhow::Result<()> {
    let loaded = Arc::new(
        rocky_core::config::load_rocky_config_fingerprinted(config_path).expect("load config"),
    );
    rocky_cli::commands::run(
        config_path,
        loaded,
        None,
        Some("silver"),
        state_path,
        None,
        false,
        None,
        false,
        None,
        false,
        None,
        &rocky_cli::commands::PartitionRunOptions::default(),
        Some("orders"),
        None,
        None,
        None,
        &rocky_cli::commands::DeferOptions::default(),
        &rocky_cli::commands::SkipRunOptions::default(),
        &rocky_core::run_vars::RunVars::new(),
        None,
        None,
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
        named_tables(&db, "orders").await,
        1,
        "a `models/*.sql` glob must materialize its model; 0 means the base \
         derivation took the Absent branch and the run silently built nothing"
    );
}

/// A literal prefix in the wildcard-bearing filename component is not a
/// directory component. Before #1293 this resolved to the nonexistent
/// `models/ord` directory and the run reported success without building.
///
/// Mutation that must turn this red: restore the old split-at-first-wildcard
/// implementation in `models_base`.
#[tokio::test]
async fn a_literal_prefix_file_glob_materializes_its_models() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/ord*.sql", &root.join("models"));
    std::fs::write(root.join("models/customers.toml"), "name = [\n")
        .expect("break the non-matching sidecar");
    run_pipeline(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("the run must succeed");

    assert_eq!(
        named_tables(&db, "orders").await,
        1,
        "a `models/ord*.sql` glob must materialize its model; 0 means the \
         literal filename prefix was mistaken for a directory"
    );
    assert_eq!(
        named_tables(&db, "customers").await,
        0,
        "a non-matching model must not be materialized"
    );
}

/// A literal path to one model source is a valid glob pattern. The selector
/// must use its parent as the load base and preserve the path as an exact
/// match, rather than probing the `.sql` file as a directory.
#[tokio::test]
async fn a_literal_model_file_glob_materializes_only_that_model() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/orders.sql", &root.join("models"));

    run_pipeline(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("the run must succeed");

    assert_eq!(named_tables(&db, "orders").await, 1);
    assert_eq!(
        named_tables(&db, "customers").await,
        0,
        "an exact model-file glob must not broaden to sibling models"
    );
}

#[tokio::test]
async fn a_literal_model_file_glob_works_with_a_relative_config_path() {
    let cwd = std::env::current_dir().expect("cwd");
    let dir = tempfile::tempdir_in(&cwd).expect("tempdir in cwd");
    let root = dir.path();
    let relative_root = root.strip_prefix(&cwd).expect("tempdir below cwd");
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/orders.sql", &root.join("models"));

    run_pipeline(
        &relative_root.join("rocky.toml"),
        &root.join(".rocky-state.redb"),
    )
    .await
    .expect("a config-relative exact glob must match config-relative model paths");

    assert_eq!(named_tables(&db, "orders").await, 1);
    assert_eq!(named_tables(&db, "customers").await, 0);
}

#[tokio::test]
async fn a_literal_directory_named_with_a_model_extension_remains_a_directory() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models.sql", &root.join("models.sql"));

    run_pipeline(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("an existing models.sql directory must retain directory semantics");

    assert_eq!(named_tables(&db, "orders").await, 1);
    assert_eq!(named_tables(&db, "customers").await, 1);
}

#[tokio::test]
async fn a_model_scoped_run_honors_the_pipelines_literal_prefix_glob() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let db = root.join("wh.duckdb");
    write_project(root, &db, "models/ord*.sql", &root.join("models"));
    std::fs::write(root.join("models/customers.toml"), "name = [\n")
        .expect("break the non-matching sidecar");

    run_pipeline_model(&root.join("rocky.toml"), &root.join(".rocky-state.redb"))
        .await
        .expect("the model-scoped run must ignore the non-matching sidecar");

    assert_eq!(named_tables(&db, "orders").await, 1);
    assert_eq!(named_tables(&db, "customers").await, 0);
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

    assert_eq!(
        named_tables(&db, "orders").await,
        1,
        "`models/**` must still build"
    );
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
