//! `rocky run --dag --var` reaches every model (#2379). It replaces the guard
//! test that pinned the old refusal. The binary parses `--var` and must hand
//! the one map to the graph builder and to every sub-run.

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

#[test]
fn run_dag_var_reaches_every_model() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    for name in ["alpha", "beta"] {
        fs::write(
            models.join(format!("{name}.sql")),
            format!("SELECT '@var(region)' AS region, '{name}' AS tag\n"),
        )
        .unwrap();
        fs::write(models.join(format!("{name}.toml")), SIDECAR).unwrap();
    }

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args([
            "-c",
            "rocky.toml",
            "run",
            "--dag",
            "--var",
            "region=emea",
            "--output",
            "json",
        ])
        .current_dir(root)
        .output()
        .expect("rocky must launch");
    assert!(
        out.status.success(),
        "run --dag --var must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let conn = duckdb::Connection::open(root.join("probe.duckdb")).expect("open duckdb");
    for name in ["alpha", "beta"] {
        let region: String = conn
            .query_row(&format!("SELECT region FROM probe.main.{name}"), [], |r| {
                r.get(0)
            })
            .unwrap_or_else(|e| panic!("{name} must exist: {e}"));
        assert_eq!(region, "emea", "{name}: --var region must reach the model");
    }
}

#[test]
fn run_dag_without_a_required_var_still_fails() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    fs::write(
        models.join("alpha.sql"),
        "SELECT '@var(region)' AS region\n",
    )
    .unwrap();
    fs::write(models.join("alpha.toml"), SIDECAR).unwrap();

    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml", "run", "--dag", "--output", "json"])
        .current_dir(root)
        .output()
        .expect("rocky must launch");
    let output = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !out.status.success(),
        "a required @var with no --var must not run\n{output}"
    );
    // The failure must be the missing variable, not some other error.
    assert!(
        output.contains("E028") || output.contains("region"),
        "the failure must name the missing variable\n{output}"
    );
}
