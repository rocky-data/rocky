//! A model below the first subdirectory level materializes under
//! `rocky run --dag` (#1262).
//!
//! The end-to-end shape of the whole family: discovery walks the tree, the
//! compile inside each sub-run walks the same tree (so the deep node's
//! sub-run finds its model instead of failing "model not found"), and the
//! same-layer nodes serialize on the state-file turnstile (#1312) instead of
//! racing the writer flock. On the pre-fix binaries this fixture failed two
//! different ways depending on which defect fired first — measured 6/6 as a
//! lock-race failure that also took the previously-green top-level model
//! down with it.

use std::fs;
use std::process::Command;

const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
type = "transformation"
models = "models/**"

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
fn run_dag_materializes_a_model_two_levels_down() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    let deep = root.join("models").join("a").join("b");
    fs::create_dir_all(&deep).unwrap();
    fs::write(
        root.join("models").join("top.sql"),
        "SELECT 1 AS id, 'top' AS tag\n",
    )
    .unwrap();
    fs::write(root.join("models").join("top.toml"), SIDECAR).unwrap();
    fs::write(deep.join("lvl2.sql"), "SELECT 2 AS id, 'lvl2' AS tag\n").unwrap();
    fs::write(deep.join("lvl2.toml"), SIDECAR).unwrap();

    // Twice: create-state and reopen-state rounds both covered.
    for round in 1..=2 {
        let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
            .args(["-c", "rocky.toml", "run", "--dag", "--output", "json"])
            .current_dir(root)
            .output()
            .expect("rocky must launch");
        let stdout = String::from_utf8_lossy(&out.stdout);
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            out.status.success(),
            "round {round}: nested run --dag must exit 0\
             \nstdout:\n{stdout}\nstderr:\n{stderr}"
        );
        let v: serde_json::Value = serde_json::from_str(
            stdout
                .lines()
                .skip_while(|l| !l.trim_start().starts_with('{'))
                .collect::<Vec<_>>()
                .join("\n")
                .as_str(),
        )
        .unwrap_or_else(|e| panic!("round {round}: DAG output must be JSON ({e})\n{stdout}"));
        assert_eq!(v["failed"], 0, "round {round}: no node may fail\n{stdout}");
        assert_eq!(
            v["completed"], 2,
            "round {round}: top AND the deep model must both materialize\n{stdout}"
        );
    }
}
