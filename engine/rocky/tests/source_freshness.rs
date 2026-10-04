//! `rocky freshness` end to end, through the real binary, on DuckDB.
//!
//! ```text
//!   raw.fresh   MAX(loaded_at) = now - 1h    -> pass
//!   raw.stale   MAX(loaded_at) = now - 13h   -> warn   (warn_after 12h)
//!   raw.old     MAX(loaded_at) = now - 30h   -> error  (error_after 24h)
//!   raw.empty   no rows                      -> error  (never loaded)
//! ```
//!
//! Any `error` or `runtime_error` exits 1; `warn` alone exits 0. The compile
//! half pins E050 (malformed declaration, refuses) and W050 (non-temporal
//! column, warns only), and the G8 half pins that `discover --with-schemas`
//! on a transformation pipeline names the route that works.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

use chrono::{Duration, Utc};

const ADAPTER: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.silver]
type = "transformation"
models = "models/**"

[pipeline.silver.target]
adapter = "default"
"#;

fn source(table: &str, freshness: &str) -> String {
    format!(
        "\n[[pipeline.silver.sources]]\nschema = \"raw\"\ntable = \"{table}\"\n\n\
         [pipeline.silver.sources.freshness]\n{freshness}\n"
    )
}

fn ts(hours_ago: i64) -> String {
    (Utc::now() - Duration::hours(hours_ago))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

/// Seed `raw.{fresh,stale,old,empty,mixed}` and `main.daily` with load times
/// computed in UTC here, so the DuckDB session time zone cannot shift them.
fn seed_db(dir: &Path) {
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
    conn.execute_batch(&format!(
        "CREATE SCHEMA raw;
         CREATE TABLE raw.fresh AS SELECT 1 AS id, TIMESTAMP '{fresh}' AS loaded_at;
         CREATE TABLE raw.stale AS SELECT 1 AS id, TIMESTAMP '{stale}' AS loaded_at;
         CREATE TABLE raw.old AS SELECT 1 AS id, TIMESTAMP '{old}' AS loaded_at;
         CREATE TABLE raw.empty (id BIGINT, loaded_at TIMESTAMP);
         CREATE TABLE raw.mixed (id BIGINT, status VARCHAR, loaded_at TIMESTAMP);
         INSERT INTO raw.mixed VALUES (1, 'test', TIMESTAMP '{fresh}'), (2, 'real', TIMESTAMP '{old}');
         CREATE TABLE main.daily AS SELECT DATE '{day}' AS event_date, 1 AS n;",
        fresh = ts(1),
        stale = ts(13),
        old = ts(30),
        day = (Utc::now() - Duration::days(3)).format("%Y-%m-%d"),
    ))
    .expect("seed tables");
}

fn project(config_tail: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    seed_db(dir.path());
    fs::write(
        dir.path().join("rocky.toml"),
        format!("{ADAPTER}{config_tail}"),
    )
    .expect("write config");
    dir
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn json(out: &Output) -> serde_json::Value {
    serde_json::from_slice(&out.stdout).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON ({e}).\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn by_name<'a>(rows: &'a serde_json::Value, name: &str) -> &'a serde_json::Value {
    rows.as_array()
        .unwrap()
        .iter()
        .find(|r| r["name"] == name)
        .unwrap_or_else(|| panic!("no entry {name} in {rows}"))
}

const THRESHOLDS: &str =
    "loaded_at_field = \"loaded_at\"\nwarn_after = \"12h\"\nerror_after = \"24h\"";

#[test]
fn fresh_warn_error_and_empty_sources_are_graded_and_error_exits_1() {
    let tail = ["fresh", "stale", "old", "empty"]
        .iter()
        .map(|t| source(t, THRESHOLDS))
        .collect::<String>();
    let dir = project(&tail);
    let out = rocky(dir.path(), &["freshness"]);
    assert_eq!(out.status.code(), Some(1), "an error source must exit 1");
    let v = json(&out);
    assert_eq!(v["command"], "freshness");
    let sources = &v["sources"];
    assert_eq!(by_name(sources, "raw.fresh")["status"], "pass");
    assert_eq!(by_name(sources, "raw.stale")["status"], "warn");
    assert_eq!(by_name(sources, "raw.old")["status"], "error");
    let empty = by_name(sources, "raw.empty");
    assert_eq!(empty["status"], "error");
    assert!(empty["max_loaded_at"].is_null());
    assert!(empty["age_seconds"].is_null());

    let fresh_age = by_name(sources, "raw.fresh")["age_seconds"]
        .as_i64()
        .unwrap();
    assert!(
        (3_500..3_700).contains(&fresh_age),
        "age_seconds is now - max_loaded_at: {fresh_age}"
    );
    assert_eq!(by_name(sources, "raw.old")["error_after_seconds"], 86_400);
    assert_eq!(
        v["summary"],
        serde_json::json!({"pass": 1, "warn": 1, "error": 2, "runtime_error": 0})
    );
}

#[test]
fn warn_alone_exits_0() {
    let tail = source("fresh", THRESHOLDS) + &source("stale", THRESHOLDS);
    let dir = project(&tail);
    let out = rocky(dir.path(), &["freshness"]);
    assert!(
        out.status.success(),
        "warn must not fail the command: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v = json(&out);
    assert_eq!(v["summary"]["warn"], 1);
    assert_eq!(v["summary"]["pass"], 1);
}

#[test]
fn filter_narrows_the_rows_the_maximum_is_taken_over() {
    // Unfiltered, the fresh test row makes raw.mixed pass.
    let dir = project(&source("mixed", THRESHOLDS));
    let v = json(&rocky(dir.path(), &["freshness"]));
    assert_eq!(by_name(&v["sources"], "raw.mixed")["status"], "pass");

    // Filtered to real rows, only the 30h-old row counts.
    let filtered = format!("{THRESHOLDS}\nfilter = \"status <> 'test'\"");
    let dir = project(&source("mixed", &filtered));
    let out = rocky(dir.path(), &["freshness"]);
    assert_eq!(
        by_name(&json(&out)["sources"], "raw.mixed")["status"],
        "error"
    );
    assert_eq!(out.status.code(), Some(1));
}

#[test]
fn a_missing_column_is_a_runtime_error_and_exits_1() {
    let dir = project(&source(
        "fresh",
        "loaded_at_field = \"no_such_column\"\nwarn_after = \"12h\"",
    ));
    let out = rocky(dir.path(), &["freshness"]);
    assert_eq!(out.status.code(), Some(1));
    let v = json(&out);
    let row = by_name(&v["sources"], "raw.fresh");
    assert_eq!(row["status"], "runtime_error");
    assert!(
        row["message"].as_str().unwrap().contains("query failed"),
        "{row}"
    );
}

#[test]
fn no_declarations_report_empty_and_exit_0() {
    let dir = project("");
    let out = rocky(dir.path(), &["freshness"]);
    assert!(out.status.success());
    let v = json(&out);
    assert_eq!(v["sources"], serde_json::json!([]));
    assert_eq!(v["models"], serde_json::json!([]));
}

/// A model `[freshness]` block is enforced the same way: `MAX(time_column)`
/// from the model's target, its one TTL graded by `severity`. A DATE column
/// reads as midnight UTC.
#[test]
fn model_freshness_is_enforced_by_severity() {
    let dir = project("");
    let models = dir.path().join("models");
    fs::create_dir_all(&models).unwrap();
    for (name, severity) in [("daily_warn", "warning"), ("daily_error", "error")] {
        fs::write(
            models.join(format!("{name}.sql")),
            "SELECT event_date, n FROM main.daily",
        )
        .unwrap();
        fs::write(
            models.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\n\
                 table = \"daily\"\n\n[freshness]\nmax_lag_seconds = 86400\n\
                 time_column = \"event_date\"\nseverity = \"{severity}\"\n"
            ),
        )
        .unwrap();
    }
    let out = rocky(dir.path(), &["freshness"]);
    let v = json(&out);
    assert_eq!(by_name(&v["models"], "daily_warn")["status"], "warn");
    assert_eq!(by_name(&v["models"], "daily_error")["status"], "error");
    assert_eq!(
        by_name(&v["models"], "daily_error")["measured_from"],
        "warehouse"
    );
    assert_eq!(out.status.code(), Some(1));
}

/// Without a `time_column`, a model is as fresh as its last successful build;
/// a model never built is stale.
#[test]
fn model_without_time_column_uses_state_store_and_never_built_is_stale() {
    let dir = project("");
    let models = dir.path().join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("m.sql"), "SELECT 1 AS n").unwrap();
    fs::write(
        models.join("m.toml"),
        "name = \"m\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\ntable = \"m\"\n\n\
         [freshness]\nmax_lag_seconds = 3600\n",
    )
    .unwrap();
    let out = rocky(dir.path(), &["freshness"]);
    assert!(out.status.success(), "warning severity must not fail");
    let v = json(&out);
    let m = by_name(&v["models"], "m");
    assert_eq!(m["measured_from"], "state_store");
    assert_eq!(m["status"], "warn");
    assert!(
        m["message"]
            .as_str()
            .unwrap()
            .contains("no successful build")
    );
}

// ----- compile-time: E050 / W050 -----

fn compile_project(config_tail: &str, seed_sql: &str) -> tempfile::TempDir {
    let dir = project(config_tail);
    let models = dir.path().join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join("stg.sql"), "SELECT order_id FROM raw.orders").unwrap();
    fs::write(
        models.join("stg.toml"),
        "name = \"stg\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\ntable = \"stg\"\n",
    )
    .unwrap();
    fs::create_dir_all(dir.path().join("data")).unwrap();
    fs::write(dir.path().join("data/seed.sql"), seed_sql).unwrap();
    dir
}

fn diagnostic_codes(v: &serde_json::Value) -> Vec<String> {
    v["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| d["code"].as_str().unwrap().to_string())
        .collect()
}

const ORDERS_SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, status VARCHAR, loaded_at TIMESTAMP);\n";

fn orders_source(freshness: &str) -> String {
    format!(
        "\n[[pipeline.silver.sources]]\nschema = \"raw\"\ntable = \"orders\"\n\n\
         [pipeline.silver.sources.freshness]\n{freshness}\n"
    )
}

#[test]
fn compile_valid_source_freshness_is_clean() {
    let dir = compile_project(&orders_source(THRESHOLDS), ORDERS_SEED);
    let out = rocky(
        dir.path(),
        &["compile", "--models", "models", "--with-seed"],
    );
    let v = json(&out);
    assert!(out.status.success(), "{v}");
    let codes = diagnostic_codes(&v);
    assert!(
        !codes.iter().any(|c| c == "E050" || c == "W050"),
        "{codes:?}"
    );
}

#[test]
fn compile_error_after_shorter_than_warn_after_is_e050_and_fails() {
    let dir = compile_project(
        &orders_source(
            "loaded_at_field = \"loaded_at\"\nwarn_after = \"24h\"\nerror_after = \"12h\"",
        ),
        ORDERS_SEED,
    );
    let out = rocky(
        dir.path(),
        &["compile", "--models", "models", "--with-seed"],
    );
    assert!(!out.status.success());
    assert!(diagnostic_codes(&json(&out)).contains(&"E050".to_string()));
}

#[test]
fn compile_non_temporal_or_absent_loaded_at_field_warns_only() {
    for field in ["status", "not_in_seed"] {
        let dir = compile_project(
            &orders_source(&format!(
                "loaded_at_field = \"{field}\"\nwarn_after = \"12h\""
            )),
            ORDERS_SEED,
        );
        let out = rocky(
            dir.path(),
            &["compile", "--models", "models", "--with-seed"],
        );
        let v = json(&out);
        assert!(
            out.status.success(),
            "a stale-able schema must never refuse: {v}"
        );
        assert!(
            diagnostic_codes(&v).contains(&"W050".to_string()),
            "{field}: {v}"
        );
    }
}

// ----- G8: discover --with-schemas on a transformation pipeline -----

#[test]
fn discover_with_schemas_on_transformation_names_the_working_route() {
    let dir = project("");
    let out = rocky(dir.path(), &["discover", "--with-schemas"]);
    assert!(!out.status.success());
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("rocky compile --with-seed"),
        "the refusal must name the supported route: {stderr}"
    );
}

/// Model `[freshness] time_column`: absent from an explicit projection is
/// E050 (the model's own SQL decides its output). Absent behind `SELECT *`
/// stays silent (the star may expand to it), and a non-temporal column warns.
#[test]
fn compile_model_time_column_checks() {
    let case = |sql: &str, column: &str| {
        let dir = compile_project("", ORDERS_SEED);
        let models = dir.path().join("models");
        fs::write(models.join("m.sql"), sql).unwrap();
        fs::write(
            models.join("m.toml"),
            format!(
                "name = \"m\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\ntable = \"m\"\n\n\
                 [freshness]\nmax_lag_seconds = 3600\ntime_column = \"{column}\"\n"
            ),
        )
        .unwrap();
        let out = rocky(
            dir.path(),
            &["compile", "--models", "models", "--with-seed"],
        );
        let v = json(&out);
        (out.status.success(), diagnostic_codes(&v))
    };

    let (ok, codes) = case("SELECT order_id, loaded_at FROM raw.orders", "updated_at");
    assert!(!ok && codes.contains(&"E050".to_string()), "{codes:?}");

    let (ok, codes) = case("SELECT * FROM raw.orders", "not_there");
    assert!(ok, "SELECT * must not refuse: {codes:?}");
    assert!(!codes.iter().any(|c| c == "E050"), "{codes:?}");

    let (ok, codes) = case("SELECT order_id, status FROM raw.orders", "status");
    assert!(ok && codes.contains(&"W050".to_string()), "{codes:?}");

    let (ok, codes) = case("SELECT order_id, loaded_at FROM raw.orders", "loaded_at");
    assert!(ok, "{codes:?}");
    assert!(
        !codes.iter().any(|c| c == "E050" || c == "W050"),
        "{codes:?}"
    );
}

/// A project `[freshness] time_column` is inherited by every model with no
/// block of its own. A model that does not output that column never asked for
/// it, so compile only warns (W050), and `rocky freshness` measures the last
/// successful build instead of reporting a runtime error. (The
/// `07-freshness-sla` POC ships exactly this shape.)
#[test]
fn inherited_time_column_absent_from_a_model_never_refuses() {
    let dir = compile_project(
        "\n[freshness]\nexpected_lag_seconds = 86400\ntime_column = \"loaded_at\"\nseverity = \"warning\"\n",
        ORDERS_SEED,
    );
    // `stg` outputs only `order_id`: the inherited `loaded_at` is absent.
    let out = rocky(
        dir.path(),
        &["compile", "--models", "models", "--with-seed"],
    );
    let v = json(&out);
    assert!(
        out.status.success(),
        "an inherited column must not refuse: {v}"
    );
    let codes = diagnostic_codes(&v);
    assert!(codes.contains(&"W050".to_string()), "{codes:?}");
    assert!(!codes.contains(&"E050".to_string()), "{codes:?}");

    let out = rocky(dir.path(), &["freshness"]);
    let v = json(&out);
    let stg = by_name(&v["models"], "stg");
    assert_eq!(stg["measured_from"], "state_store", "{stg}");
    assert_eq!(
        stg["status"], "warn",
        "never built, warning severity: {stg}"
    );
    assert!(out.status.success(), "warning severity must not fail: {v}");
}
