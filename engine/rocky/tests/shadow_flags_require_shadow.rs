//! Run and plan shadow overrides require shadow mode; compare reads shadow targets directly.

use std::{
    fs,
    path::Path,
    process::{Command, Output},
};

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

const PLAN_CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
strategy = "full_refresh"

[pipeline.probe.source.discovery]
adapter = "default"

[pipeline.probe.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.probe.target]
catalog_template = "probe"
schema_template = "staging__{source}"

[pipeline.probe.target.governance]
auto_create_schemas = true
"#;

const MODEL: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "probe"
schema = "main"
"#;

fn project(root: &Path, verb: &str) {
    if verb == "plan" {
        fs::write(root.join("rocky.toml"), PLAN_CONFIG).unwrap();
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
        conn.execute_batch(
            "CREATE SCHEMA raw__orders; CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .unwrap();
        return;
    }
    fs::create_dir(root.join("models")).unwrap();
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    fs::write(root.join("models/orders.sql"), "SELECT 1 AS id").unwrap();
    fs::write(root.join("models/orders.toml"), MODEL).unwrap();
}

fn command(root: &Path, verb: &str, flags: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .env("RUST_LOG", "error")
        .args(["--config", "rocky.toml", "--output", "json", verb])
        .args(flags)
        .output()
        .expect("launch rocky")
}

fn assert_success(output: &Output, context: &str) {
    assert!(
        output.status.success(),
        "{context}: stdout={} stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn refused(verb: &str, flags: &[&str]) {
    let tmp = tempfile::tempdir().unwrap();
    // No config exists. A clap rejection must happen before any project I/O.
    let out = command(tmp.path(), verb, flags);
    assert_eq!(
        out.status.code(),
        Some(2),
        "{verb} {flags:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("the following required arguments were not provided:\n  --shadow"),
        "{stderr}"
    );
    assert!(
        !stderr.contains("rocky.toml"),
        "config I/O preceded clap: {stderr}"
    );
}

fn allowed(verb: &str, flags: &[&str], shadow_table: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    project(root, verb);

    if verb == "compare" {
        assert_success(&command(root, "run", &[]), "prepare production");
        let (schema, table) = shadow_table.split_once('.').unwrap();
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
        if schema != "main" {
            conn.execute_batch(&format!("CREATE SCHEMA {schema}"))
                .unwrap();
        }
        conn.execute_batch(&format!(
            "CREATE TABLE {schema}.{table} AS SELECT * FROM main.orders"
        ))
        .unwrap();
    }

    let out = command(root, verb, flags);
    assert_success(&out, &format!("{verb} {flags:?}"));
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).expect("one JSON output");
    match verb {
        "run" => {
            let (schema, table) = shadow_table.split_once('.').unwrap();
            assert_eq!(
                json["materializations"][0]["asset_key"],
                serde_json::json!(["probe", schema, table]),
                "{json}"
            );
            let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
            if flags.is_empty() {
                let count: i64 = conn
                    .query_row("SELECT count(*) FROM main.orders", [], |row| row.get(0))
                    .unwrap();
                assert_eq!(count, 1);
            } else {
                let production_count: i64 = conn.query_row(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main' AND table_name = 'orders'",
                    [], |row| row.get(0)).unwrap();
                assert_eq!(production_count, 0, "shadow run wrote production: {json}");
            }
        }
        "plan" => {
            let plan_id = json["plan_id"].as_str().unwrap();
            let stored: serde_json::Value = serde_json::from_slice(
                &fs::read(root.join(".rocky/plans").join(format!("{plan_id}.json"))).unwrap(),
            )
            .unwrap();
            assert_eq!(
                stored["payload"]["shadow"].as_bool().unwrap_or(false),
                !flags.is_empty(),
                "{stored}"
            );
            let expected_suffix = if flags.is_empty() {
                None
            } else if flags.contains(&"--shadow-suffix") {
                Some("_custom")
            } else {
                Some("_rocky_shadow")
            };
            assert_eq!(
                stored["payload"]["shadow_suffix"].as_str(),
                expected_suffix,
                "{stored}"
            );
            let expected_schema = if shadow_table.starts_with("shadow_x.") {
                Some("shadow_x")
            } else {
                None
            };
            assert_eq!(
                stored["payload"]["shadow_schema"].as_str(),
                expected_schema,
                "{stored}"
            );
        }
        "compare" => {
            assert_eq!(json["tables_compared"], 1, "{json}");
            assert_eq!(json["tables_passed"], 1, "{json}");
            assert!(
                json["results"][0]["shadow_table"]
                    .as_str()
                    .unwrap()
                    .ends_with(shadow_table),
                "{json}"
            );
        }
        _ => unreachable!(),
    }
}

macro_rules! refuse_case {
    ($name:ident, $verb:literal, [$($flag:literal),+]) => {
        #[test]
        fn $name() { refused($verb, &[$($flag),+]); }
    };
}

macro_rules! allow_case {
    ($name:ident, $verb:literal, [$($flag:literal),*], $target:literal) => {
        #[test]
        fn $name() { allowed($verb, &[$($flag),*], $target); }
    };
}

refuse_case!(
    plan_refuses_suffix_without_shadow,
    "plan",
    ["--shadow-suffix", "_custom"]
);
refuse_case!(
    plan_refuses_default_suffix_when_explicit,
    "plan",
    ["--shadow-suffix", "_rocky_shadow"]
);
refuse_case!(
    plan_refuses_schema_without_shadow,
    "plan",
    ["--shadow-schema", "shadow_x"]
);
refuse_case!(
    run_refuses_suffix_without_shadow,
    "run",
    ["--shadow-suffix", "_custom"]
);
refuse_case!(
    run_refuses_default_suffix_when_explicit,
    "run",
    ["--shadow-suffix", "_rocky_shadow"]
);
refuse_case!(
    run_refuses_schema_without_shadow,
    "run",
    ["--shadow-schema", "shadow_x"]
);
allow_case!(plan_plain, "plan", [], "staging__orders.orders");
allow_case!(
    plan_shadow_default,
    "plan",
    ["--shadow"],
    "staging__orders.orders_rocky_shadow"
);
allow_case!(
    plan_shadow_custom_suffix,
    "plan",
    ["--shadow", "--shadow-suffix", "_custom"],
    "staging__orders.orders_custom"
);
allow_case!(
    plan_shadow_custom_schema,
    "plan",
    ["--shadow", "--shadow-schema", "shadow_x"],
    "shadow_x.orders"
);
allow_case!(run_plain, "run", [], "main.orders");
allow_case!(
    run_shadow_default,
    "run",
    ["--shadow"],
    "main.orders_rocky_shadow"
);
allow_case!(
    run_shadow_custom_suffix,
    "run",
    ["--shadow", "--shadow-suffix", "_custom"],
    "main.orders_custom"
);
allow_case!(
    run_shadow_custom_schema,
    "run",
    ["--shadow", "--shadow-schema", "shadow_x"],
    "shadow_x.orders"
);
#[test]
fn compare_accepts_shadow_overrides_without_shadow() {
    allowed(
        "compare",
        &["--shadow-suffix", "_custom"],
        "main.orders_custom",
    );
    allowed(
        "compare",
        &["--shadow-schema", "shadow_x"],
        "shadow_x.orders",
    );
}

#[test]
fn branch_conflicts_remain_on_plan_and_run() {
    let tmp = tempfile::tempdir().unwrap();
    for verb in ["plan", "run"] {
        for flags in [
            &["--branch", "feature", "--shadow"][..],
            &["--branch", "feature", "--shadow-schema", "shadow_x"][..],
        ] {
            let out = command(tmp.path(), verb, flags);
            assert_eq!(
                out.status.code(),
                Some(2),
                "{verb} {flags:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let out = command(tmp.path(), verb, &["--branch", "feature"]);
        assert_ne!(
            out.status.code(),
            Some(2),
            "bare branch failed clap: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

#[test]
fn plan_refuses_branch_with_shadow_suffix() {
    refused_branch_suffix("plan");
}

#[test]
fn run_refuses_branch_with_shadow_suffix() {
    refused_branch_suffix("run");
}

fn refused_branch_suffix(verb: &str) {
    let tmp = tempfile::tempdir().unwrap();
    let out = command(
        tmp.path(),
        verb,
        &["--branch", "feature", "--shadow-suffix", "_x"],
    );
    assert_eq!(
        out.status.code(),
        Some(2),
        "{verb}: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("cannot be used with"),
        "{verb}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}
