//! A function or macro the project's `data/seed.sql` creates is the
//! project's own: no command judges a call to it against DuckDB's built-ins
//! (E057, E068), whether or not the seed ran (#2659 red team).
//!
//! The names are the union of what the DuckDB that ran the seed reports and
//! a text scan of the seed file, so `compile`, `ci`, `test` and `emit-sql`
//! agree, with the models beside the config or elsewhere, and when the seed
//! fails to run.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

/// The seed's string literals hold comment markers: a text scan that strips
/// comments first misses the macro between them. `strip_accents` is a
/// built-in that takes one argument; the seed adds a two-argument macro.
/// `my_fn` is a name DuckDB does not have.
const SEED: &str = "\
CREATE SCHEMA raw;
CREATE TABLE raw.orders AS SELECT 1::BIGINT AS order_id, 'done' AS status;
CREATE TABLE raw.paths AS SELECT 'raw/orders/*.csv' AS p, 'a--b' AS q;
CREATE MACRO strip_accents(a, b) AS a || b;
CREATE TABLE raw.more_paths AS SELECT 'raw/events/*/part.parquet' AS p;
CREATE MACRO my_fn(x) AS x;
";

/// A seed that fails when Rocky runs it from another directory: the CSV path
/// is relative to the current directory.
const SEED_THAT_FAILS_ELSEWHERE: &str = "\
CREATE MACRO strip_accents(a, b) AS a || b;
CREATE MACRO my_fn(x) AS x;
CREATE SCHEMA raw;
CREATE TABLE raw.orders AS SELECT * FROM read_csv('data/orders.csv');
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

const USES_SEED: &[(&str, &str)] = &[
    (
        "uses_macro",
        "SELECT strip_accents(status, status) AS x FROM raw.orders",
    ),
    ("uses_fn", "SELECT my_fn(status) AS x FROM raw.orders"),
];

/// DuckDB refuses `upper` with two arguments, so the check is running.
const CONTROL: (&str, &str) = (
    "control",
    "SELECT upper(status, status) AS x FROM raw.orders",
);

fn write_project(root: &Path, seed: &str, models: &[(&str, &str)]) {
    fs::create_dir_all(root.join("data")).expect("create data");
    fs::write(root.join("data/seed.sql"), seed).expect("write seed");
    fs::write(root.join("data/orders.csv"), "order_id,status\n1,done\n").expect("write csv");
    let dir = root.join("models");
    fs::create_dir_all(&dir).expect("create models");
    for (name, sql) in models {
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

fn describe(output: &Output) -> String {
    format!(
        "exit {:?}\nstdout: {}\nstderr: {}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// `(code, model)` of every E057 / W057 / E068: anywhere in the JSON, or,
/// for `emit-sql`, which prints SQL, in its `CODE: message (model)` lines.
fn call_codes(output: &Output) -> Vec<(String, String)> {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut out = Vec::new();
    match serde_json::from_str::<serde_json::Value>(stdout.trim()) {
        Ok(json) => collect(&json, &mut out),
        Err(_) => {
            let text = format!("{stdout}\n{}", String::from_utf8_lossy(&output.stderr));
            for line in text.lines() {
                let line = line.trim();
                if let Some((code, rest)) = line.split_once(": ")
                    && let Some(model) = rest
                        .strip_suffix(')')
                        .and_then(|r| r.rsplit_once('('))
                        .map(|(_, m)| m)
                {
                    out.push((code.to_string(), model.to_string()));
                }
            }
        }
    }
    out.retain(|(code, _)| ["E057", "W057", "E068"].contains(&code.as_str()));
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

fn control_refused() -> Vec<(String, String)> {
    vec![("E068".to_string(), "control".to_string())]
}

const COMMANDS: &[&[&str]] = &[
    &["compile"],
    &["compile", "--with-seed"],
    &["ci"],
    &["test"],
    &["emit-sql"],
];

#[test]
fn a_seed_macro_is_not_judged_by_any_command() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    write_project(tmp.path(), SEED, USES_SEED);
    let config = tmp.path().join("rocky.toml");
    for args in COMMANDS {
        let output = rocky(tmp.path(), &config, args);
        assert!(
            call_codes(&output).is_empty(),
            "{args:?}: {}",
            describe(&output)
        );
        assert_eq!(
            output.status.code(),
            Some(0),
            "{args:?}: {}",
            describe(&output)
        );
    }
    // The check runs: a built-in the seed does not shadow is still refused.
    write_project(tmp.path(), SEED, &[CONTROL]);
    for args in COMMANDS {
        let output = rocky(tmp.path(), &config, args);
        assert_eq!(call_codes(&output), control_refused(), "{args:?}");
        assert_ne!(output.status.code(), Some(0), "{args:?}");
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
    let mut models = USES_SEED.to_vec();
    models.push(CONTROL);
    write_project(&b, SEED, &models);
    let config = a.join("rocky.toml");
    let dir = b.join("models");
    let dir = dir.to_str().expect("utf-8 path");
    for command in ["compile", "ci", "test", "emit-sql"] {
        let output = rocky(&a, &config, &[command, "--models", dir]);
        assert_eq!(call_codes(&output), control_refused(), "{command}");
    }
}

/// A seed that does not run still names its macros: the text scan finds them.
#[test]
fn a_seed_macro_is_not_judged_when_the_seed_does_not_run() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let project = tmp.path().join("project");
    fs::create_dir_all(&project).expect("create project");
    fs::write(project.join("rocky.toml"), CONFIG).expect("write config");
    let mut models = USES_SEED.to_vec();
    models.push(CONTROL);
    write_project(&project, SEED_THAT_FAILS_ELSEWHERE, &models);
    let config = project.join("rocky.toml");
    let dir = project.join("models");
    let dir = dir.to_str().expect("utf-8 path");
    // Run from the parent directory, where `data/orders.csv` does not exist.
    for command in ["compile", "emit-sql"] {
        let args = [command, "--models", dir];
        let output = rocky(tmp.path(), &config, &args);
        assert_eq!(call_codes(&output), control_refused(), "{args:?}");
    }
}
