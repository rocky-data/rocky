use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn project(root: &Path) {
    fs::write(
        root.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
         [pipeline.ingest]\nstrategy = \"full_refresh\"\n\
         [pipeline.ingest.source.discovery]\nadapter = \"default\"\n\
         [pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
         [pipeline.ingest.target]\ncatalog_template = \"warehouse\"\nschema_template = \"staging__{source}\"\n",
    )
    .unwrap();
    let db = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    db.execute_batch("CREATE SCHEMA raw__shop; CREATE TABLE raw__shop.orders AS SELECT 1 AS id;")
        .unwrap();
}

fn model(root: &Path, name: &str, sql: &str, strategy: &str) {
    let dir = root.join("models");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n[strategy]\ntype = \"{strategy}\"\n{}[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n",
            if strategy == "incremental" { "timestamp_column = \"updated_at\"\n" } else { "" }
        ),
    )
    .unwrap();
}

fn rocky(root: &Path, args: &[&str], json: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command.current_dir(root).args(["--config", "rocky.toml"]);
    if json {
        command.args(["--output", "json"]);
    } else {
        command.args(["--output", "table"]);
    }
    command
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

fn assert_no_plan(root: &Path) {
    assert!(
        !root.join(".rocky/plans").exists()
            || fs::read_dir(root.join(".rocky/plans"))
                .unwrap()
                .next()
                .is_none(),
        "refused plan wrote a file"
    );
}

#[test]
fn plan_refuses_all_error_diagnostics_without_persisting_json_or_text() {
    for json in [true, false] {
        let tmp = tempfile::tempdir().unwrap();
        project(tmp.path());
        model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
        model(tmp.path(), "inc", "SELECT 2 AS id", "incremental");
        model(tmp.path(), "fine", "SELECT 3 AS id", "full_refresh");
        let out = rocky(tmp.path(), &["plan"], json);
        assert!(
            !out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stdout)
        );
        if json {
            let value: serde_json::Value =
                serde_json::from_slice(&out.stdout).unwrap_or_else(|error| {
                    panic!(
                        "{error}: stdout={} stderr={}",
                        String::from_utf8_lossy(&out.stdout),
                        String::from_utf8_lossy(&out.stderr)
                    )
                });
            let skipped = value["skipped"].as_array().unwrap();
            for code in ["E037", "E038"] {
                assert!(
                    skipped
                        .iter()
                        .any(|s| s["reason"].as_str().unwrap().contains(code)),
                    "{value}"
                );
            }
            assert!(value["plan_id"].is_null(), "{value}");
        } else {
            let stderr = String::from_utf8_lossy(&out.stderr);
            assert!(
                stderr.contains("E037") && stderr.contains("E038"),
                "{stderr}"
            );
            assert!(!String::from_utf8_lossy(&out.stdout).contains("Apply with:"));
        }
        assert_no_plan(tmp.path());
    }
}

#[test]
fn selected_model_refuses_its_error_or_required_model_but_not_unrelated_error() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
    model(tmp.path(), "reader", "SELECT id FROM eph", "full_refresh");
    model(tmp.path(), "fine", "SELECT 2 AS id", "full_refresh");

    for selected in ["eph", "reader"] {
        for json in [true, false] {
            let out = rocky(tmp.path(), &["plan", "--model", selected], json);
            assert!(!out.status.success(), "{selected} falsely planned");
            if json {
                let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
                assert!(
                    value["skipped"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .any(|s| s["model"] == "eph"
                            && s["reason"].as_str().unwrap().contains("E038")),
                    "{value}"
                );
                assert!(value["plan_id"].is_null(), "{value}");
                assert!(
                    value["statements"].as_array().unwrap().is_empty(),
                    "{value}"
                );
            } else {
                assert!(String::from_utf8_lossy(&out.stderr).contains("E038"));
                assert!(!String::from_utf8_lossy(&out.stdout).contains("Apply with:"));
            }
            assert_no_plan(tmp.path());
        }
    }

    let out = rocky(tmp.path(), &["plan", "--model", "fine"], true);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["models"], serde_json::json!(["fine"]));
    assert!(value["plan_id"].as_str().is_some(), "{value}");
}

#[test]
fn warning_only_project_still_persists_plan() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    model(tmp.path(), "dated", "SELECT 1 AS id", "full_refresh");
    let sidecar = tmp.path().join("models/dated.toml");
    let mut contents = fs::read_to_string(&sidecar).unwrap();
    contents.push_str("[classification]\nid = \"pii\"\n");
    fs::write(sidecar, contents).unwrap();
    let compile = rocky(tmp.path(), &["compile"], true);
    assert!(
        compile.status.success(),
        "{}",
        String::from_utf8_lossy(&compile.stderr)
    );
    assert!(
        String::from_utf8_lossy(&compile.stdout).contains("W004"),
        "{}",
        String::from_utf8_lossy(&compile.stdout)
    );
    let out = rocky(tmp.path(), &["plan"], true);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(value["models"], serde_json::json!(["dated"]));
    assert!(value["plan_id"].as_str().is_some(), "{value}");
}

#[test]
fn plan_refuses_duplicate_targets_with_model_and_code_in_error() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    model(tmp.path(), "first", "SELECT 1 AS id", "full_refresh");
    model(tmp.path(), "second", "SELECT 2 AS id", "full_refresh");
    let second = tmp.path().join("models/second.toml");
    let contents = fs::read_to_string(&second)
        .unwrap()
        .replace("table = \"second\"", "table = \"first\"");
    fs::write(second, contents).unwrap();

    for json in [true, false] {
        let out = rocky(tmp.path(), &["plan"], json);
        assert_eq!(out.status.code(), Some(1), "{out:?}");
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.lines().any(|line| line == "first: [E036]"),
            "{stderr}"
        );
        assert!(
            stderr.lines().any(|line| line == "second: [E036]"),
            "{stderr}"
        );
        if json {
            let value: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
            assert!(value["plan_id"].is_null(), "{value}");
            assert_eq!(value["skipped"].as_array().unwrap().len(), 2, "{value}");
        }
        assert_no_plan(tmp.path());
    }
}
