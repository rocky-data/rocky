//! A function or macro the project's `data/seed.sql` creates is the
//! project's own: no command judges a call to it against DuckDB's built-ins
//! (E057, E068), whether or not the seed ran (#2691).
//!
//! The names are the union of what the DuckDB that ran the seed reports and
//! a text scan of the seed file, so `compile`, `ci`, `test` and `emit-sql`
//! agree, with the models beside the config or elsewhere, and when the seed
//! fails to run. `rocky serve` and `rocky lsp` install the gate function
//! `rocky-cli`'s `the_serve_and_lsp_gates_skip_a_seed_macro_without_running_the_seed`
//! tests.

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
/// is relative to the current directory. Its macros follow a `$$` and an
/// `E'...'` string that hold a quote, with comments between the words, the
/// shapes the first text scan missed (#2691).
const SEED_THAT_FAILS_ELSEWHERE: &str = "\
CREATE TABLE notes AS SELECT $$it's$$ AS a, E'it\\'s' AS b;
CREATE /* shadows a built-in */ MACRO strip_accents(a, b) AS a || b;
CREATE -- a project macro
  MACRO my_fn(x) AS x;
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

/// `(code, model)` of every E057 / W057 / E068 / E073: anywhere in the JSON, or,
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
    out.retain(|(code, _)| ["E057", "W057", "E068", "E073"].contains(&code.as_str()));
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

/// The #2691 repro: a bare seed table and a seed macro DuckDB does not have.
/// `rocky compile` and `rocky compile --with-seed` both refused the call with
/// `E057` before.
#[test]
fn the_issue_repro_compiles() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    write_project(
        tmp.path(),
        "CREATE TABLE orders AS SELECT 'a' AS status;\nCREATE MACRO my_fn(x) AS upper(x);\n",
        &[("m", "SELECT my_fn(status) AS s FROM orders")],
    );
    let config = tmp.path().join("rocky.toml");
    for args in [&["compile"][..], &["compile", "--with-seed"]] {
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
}

/// A macro the seed brings in with `IMPORT DATABASE` is not in the seed's
/// text: only the names the DuckDB that ran the seed reports cover it. Every
/// command that runs the seed skips it. (`emit-sql`, `serve` and `lsp` never
/// run the seed and still refuse it with `E057`.)
#[test]
fn an_imported_seed_macro_is_not_judged_by_the_commands_that_run_the_seed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    write_project(
        tmp.path(),
        "CREATE SCHEMA raw;\n\
         CREATE TABLE raw.orders AS SELECT 1::BIGINT AS order_id, 'done' AS status;\n\
         IMPORT DATABASE 'data/export';\n",
        &[(
            "uses_import",
            "SELECT imported_fn(status) AS x FROM raw.orders",
        )],
    );
    let export = tmp.path().join("data/export");
    fs::create_dir_all(&export).expect("create export");
    fs::write(
        export.join("schema.sql"),
        "CREATE MACRO imported_fn(x) AS x;\n",
    )
    .expect("write schema.sql");
    fs::write(export.join("load.sql"), "").expect("write load.sql");
    let config = tmp.path().join("rocky.toml");
    for args in [
        &["compile"][..],
        &["compile", "--with-seed"],
        &["ci"],
        &["test"],
    ] {
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
    // The control: without the seed run, the name is unknown.
    let output = rocky(tmp.path(), &config, &["emit-sql"]);
    assert_eq!(
        call_codes(&output),
        [("E057".to_string(), "uses_import".to_string())],
        "{}",
        describe(&output)
    );
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

/// A seed file that is there but cannot be read fails closed (#2753): a call
/// the check would refuse may be one of its macros, so `compile` and
/// `emit-sql` refuse it with `E073`, naming the seed, and never skip the
/// check. A call to a built-in DuckDB has is not affected. The commands that
/// run the seed (`compile --with-seed`, `ci`, `test`) refuse the whole run,
/// naming the seed.
#[test]
fn an_unreadable_seed_refuses_the_call_and_names_the_seed() {
    let tmp = tempfile::tempdir().expect("tempdir");
    fs::write(tmp.path().join("rocky.toml"), CONFIG).expect("write config");
    write_project(
        tmp.path(),
        "",
        &[
            ("uses_fn", "SELECT my_fn(status) AS x FROM raw.orders"),
            ("plain", "SELECT upper(status) AS x FROM raw.orders"),
        ],
    );
    // Not valid UTF-8: the file is there, but does not read as text.
    fs::write(
        tmp.path().join("data/seed.sql"),
        b"CREATE MACRO my_fn(x) AS x; -- \xff\xfe\n",
    )
    .expect("write seed");
    let config = tmp.path().join("rocky.toml");
    for args in [&["compile"][..], &["emit-sql"]] {
        let output = rocky(tmp.path(), &config, args);
        let text = describe(&output);
        assert_eq!(
            call_codes(&output),
            [("E073".to_string(), "uses_fn".to_string())],
            "{args:?}: {text}"
        );
        assert!(text.contains("seed.sql"), "{args:?}: {text}");
        assert!(text.contains("could not be read"), "{args:?}: {text}");
        assert_ne!(output.status.code(), Some(0), "{args:?}: {text}");
    }
    for args in [&["compile", "--with-seed"][..], &["ci"], &["test"]] {
        let output = rocky(tmp.path(), &config, args);
        let text = describe(&output);
        assert_ne!(output.status.code(), Some(0), "{args:?}: {text}");
        assert!(text.contains("seed.sql"), "{args:?}: {text}");
        assert!(text.contains("failed to read"), "{args:?}: {text}");
    }
}
