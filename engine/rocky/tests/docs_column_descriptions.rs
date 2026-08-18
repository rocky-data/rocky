//! #1444 regression: `rocky docs` must render sidecar `[columns]`
//! descriptions instead of silently discarding them.
//!
//! The production call site passed `column_map: None` to `build_doc_index`,
//! and descriptions only attach to columns from that map — so every
//! description parsed and then rendered nowhere, with exit 0 and no warning.
//! The map now comes from the offline compile step's type inference.

use std::fs;
use std::process::{Command, Output};

const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "warehouse.duckdb"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"
"#;

const MARKER: &str = "ROCKY_DOCS_MARKER_1444_primary_order_key";
const GHOST_MARKER: &str = "ROCKY_DOCS_MARKER_1444_ghost_column";

fn write_sidecar(dir: &std::path::Path, name: &str, columns_toml: &str) {
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n\n{columns_toml}"
        ),
    )
    .expect("write model sidecar");
}

fn run_docs(dir: &std::path::Path) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("docs")
        .arg("--models")
        .arg(dir.join("models"))
        .arg("--output-path")
        .arg(dir.join("catalog.html"))
        .current_dir(dir)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn rocky docs")
}

fn read_html(dir: &std::path::Path) -> String {
    fs::read_to_string(dir.join("catalog.html")).expect("read generated catalog")
}

#[test]
fn sidecar_column_descriptions_render_in_the_html() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    fs::write(models.join("orders.sql"), "SELECT 1 AS id, 'open' AS status\n")
        .expect("write model sql");
    write_sidecar(
        &models,
        "orders",
        &format!("[columns.id]\ndescription = \"{MARKER}\"\n"),
    );

    let out = run_docs(dir);
    assert!(
        out.status.success(),
        "rocky docs must succeed; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    let html = read_html(dir);
    assert!(
        html.contains(MARKER),
        "the [columns] description must appear in the generated HTML"
    );
    // With inferred columns present, the empty-state message must be gone.
    assert!(
        !html.contains("No column metadata available"),
        "a model with inferred columns must render a column table"
    );
}

/// A description whose column the compile step cannot see still cannot
/// render — but it must warn on stderr instead of vanishing, and it must
/// not take the matching descriptions down with it.
#[test]
fn an_unknown_column_description_warns_and_the_rest_still_render() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").expect("write model sql");
    write_sidecar(
        &models,
        "orders",
        &format!(
            "[columns.id]\ndescription = \"{MARKER}\"\n\n\
             [columns.ghost]\ndescription = \"{GHOST_MARKER}\"\n"
        ),
    );

    let out = run_docs(dir);
    assert!(out.status.success(), "an orphaned description is a warning, not an error");

    let html = read_html(dir);
    assert!(html.contains(MARKER), "the matching description must still render");
    assert!(
        !html.contains(GHOST_MARKER),
        "a description without a column must not be invented into the table"
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("ghost"),
        "the skipped column must be named on stderr, got: {stderr}"
    );
}

/// `rocky docs` is a reporting command: a model that does not compile must
/// not fail the docs build.
///
/// Project load is all-or-nothing (matching `rocky compile`), so a broken
/// sibling costs the whole project its column tables — the docs page falls
/// back to exactly the pre-#1444 output. That degradation must be NAMED on
/// stderr, not silent: silence is the failure mode this command had.
#[test]
fn a_broken_sibling_model_degrades_loudly_and_docs_still_build() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
    fs::write(models.join("orders.sql"), "SELECT 1 AS id\n").expect("write model sql");
    write_sidecar(
        &models,
        "orders",
        &format!("[columns.id]\ndescription = \"{MARKER}\"\n"),
    );
    fs::write(models.join("broken.sql"), "SELEC 1 AS oops\n").expect("write broken sql");
    write_sidecar(&models, "broken", "");

    let out = run_docs(dir);
    assert!(
        out.status.success(),
        "docs must degrade, not fail; stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The docs page still builds, minus column metadata for everyone —
    // and the degradation is named on stderr rather than silent.
    let html = read_html(dir);
    assert!(html.contains("orders"), "the healthy model must still be listed");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("docs render without column metadata"),
        "the compile failure must be named on stderr, got: {stderr}"
    );
}
