//! A function or macro the project's `data/seed.sql` creates is the
//! project's own: `rocky compile`, `rocky ci` and `rocky test` do not judge a
//! call to it against DuckDB's built-ins (E057, E068, E069). The names are
//! read from the DuckDB that ran the seed, the same seed the source schemas
//! come from, so every command finds them however the project is laid out
//! (#2659 red team).

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// A seed whose string literals hold comment markers: a text scan for
/// `CREATE MACRO` that strips comments first misses the macro between them.
const SEED: &str = "\
CREATE SCHEMA raw;
CREATE TABLE raw.orders AS SELECT 1::BIGINT AS order_id, 'done' AS status;
CREATE TABLE raw.paths AS SELECT 'raw/orders/*.csv' AS p, 'a--b' AS q;
CREATE MACRO lower(a, b) AS a || b;
CREATE TABLE raw.more_paths AS SELECT 'raw/events/*/part.parquet' AS p;
";

const CONFIG: &str = "\
[adapter]
type = \"duckdb\"
path = \"warehouse.duckdb\"

[pipeline.main]
type = \"transformation\"
models = \"models/**\"

[pipeline.main.target.governance]
auto_create_schemas = true
";

/// `uses_macro` calls the seed's two-argument `lower`; `control` calls
/// `upper` with two arguments, which DuckDB refuses, so the check runs.
fn write_project(root: &Path) {
    fs::create_dir_all(root.join("data")).expect("create data");
    fs::write(root.join("data/seed.sql"), SEED).expect("write seed");
    let dir = root.join("models");
    fs::create_dir_all(&dir).expect("create models");
    for (name, sql) in [
        (
            "uses_macro",
            "SELECT lower(status, status) AS x FROM raw.orders",
        ),
        (
            "control",
            "SELECT upper(status, status) AS x FROM raw.orders",
        ),
    ] {
        fs::write(dir.join(format!("{name}.sql")), sql).expect("write model sql");
        fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n"
            ),
        )
        .expect("write model sidecar");
    }
}

fn rocky(cwd: &Path, config: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(cwd)
        .arg("--config")
        .arg(config)
        .arg("--state-path")
        .arg(cwd.join("state.redb"))
        .arg("--output")
        .arg("json")
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

/// `(code, model)` of every diagnostic anywhere in the command's JSON.
fn diagnostics(output: &Output) -> Vec<(String, String)> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let json: serde_json::Value = serde_json::from_str(stdout.trim()).unwrap_or_else(|error| {
        panic!(
            "stdout is not JSON: {error}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    let mut out = Vec::new();
    collect(&json, &mut out);
    out.sort();
    out.dedup();
    out
}

fn collect(value: &serde_json::Value, out: &mut Vec<(String, String)>) {
    match value {
        serde_json::Value::Object(map) => {
            if let (Some(code), Some(model)) = (
                map.get("code").and_then(|c| c.as_str()),
                map.get("model").and_then(|m| m.as_str()),
            ) {
                out.push((code.to_string(), model.to_string()));
            }
            map.values().for_each(|v| collect(v, out));
        }
        serde_json::Value::Array(items) => items.iter().for_each(|v| collect(v, out)),
        _ => {}
    }
}

/// The function-call codes (E057, E068, E069 and their warnings) per model.
fn call_codes(output: &Output) -> Vec<(String, String)> {
    diagnostics(output)
        .into_iter()
        .filter(|(code, _)| ["E057", "W057", "E068", "E069", "W069"].contains(&code.as_str()))
        .collect()
}

fn refused_control() -> Vec<(String, String)> {
    vec![("E068".to_string(), "control".to_string())]
}

#[test]
fn a_seed_macro_is_not_judged_by_compile_ci_or_test() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    write_project(tmp.path());
    let config = tmp.path().join("rocky.toml");
    for args in [
        &["compile"][..],
        &["compile", "--with-seed"],
        &["ci"],
        &["test"],
    ] {
        let output = rocky(tmp.path(), &config, args);
        assert_eq!(call_codes(&output), refused_control(), "{args:?}");
    }
}

/// `--models` naming a directory beside another project's `rocky.toml`: the
/// seed beside the models is the one every command reads.
#[test]
fn a_seed_macro_is_not_judged_with_models_elsewhere() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let a = tmp.path().join("a");
    let b = tmp.path().join("b");
    fs::create_dir_all(&a).expect("create a");
    fs::write(a.join("rocky.toml"), CONFIG).expect("write config");
    write_project(&b);
    let config = a.join("rocky.toml");
    let models = b.join("models");
    let models = models.to_str().expect("utf-8 path");
    for command in ["compile", "ci", "test"] {
        let output = rocky(&a, &config, &[command, "--models", models]);
        assert_eq!(call_codes(&output), refused_control(), "{command}");
    }
}
