//! `rocky docs` site and Parquet export, end to end.
//!
//! The site test checks the directory layout, the content of a model page,
//! the search/lineage data file and back-compat of a `.html` output path.
//! The Parquet test reads every table back through DuckDB, so a schema or
//! encoding mistake that only a real reader notices fails here.

use std::fs;
use std::path::Path;
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

fn sidecar(dir: &Path, name: &str, extra: &str) {
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\nintent = \"The {name} model\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n\n{extra}"
        ),
    )
    .expect("write sidecar");
}

fn project(dir: &Path) {
    let models = dir.join("models");
    fs::create_dir(&models).expect("models dir");
    fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("config");
    fs::write(
        models.join("stg_orders.sql"),
        "SELECT id, email, amount FROM raw.orders\n",
    )
    .expect("stg sql");
    sidecar(
        &models,
        "stg_orders",
        "[classification]\nemail = \"pii\"\n\n[columns.id]\ndescription = \"Order key\"\n\n\
         [[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n",
    );
    fs::write(
        models.join("stg_orders.contract.toml"),
        "[[columns]]\nname = \"id\"\ndescription = \"Order key\"\n\n[rules]\nrequired = [\"id\"]\n",
    )
    .expect("contract");
    fs::write(
        models.join("order_totals.sql"),
        "SELECT id, SUM(amount) AS total FROM stg_orders GROUP BY id\n",
    )
    .expect("mart sql");
    sidecar(&models, "order_totals", "");
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("docs")
        .arg("--models")
        .arg(dir.join("models"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "warn")
        .output()
        .expect("spawn rocky docs")
}

fn read(path: impl AsRef<Path>) -> String {
    fs::read_to_string(path.as_ref()).unwrap_or_else(|e| panic!("{}: {e}", path.as_ref().display()))
}

#[test]
fn site_directory_has_pages_assets_and_lineage_data() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    project(dir);
    let out = dir.join("site");
    let result = rocky(dir, &["--output-path", out.to_str().expect("utf8")]);
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );

    for file in [
        "index.html",
        "lineage.html",
        "assets/site.css",
        "assets/site.js",
        "assets/data.js",
        "models/stg_orders.html",
        "models/order_totals.html",
        "sources/raw.orders.html",
    ] {
        assert!(out.join(file).is_file(), "missing {file}");
    }

    let page = read(out.join("models/stg_orders.html"));
    assert!(page.contains("Order key"), "column description");
    assert!(page.contains("pii"), "classification");
    assert!(page.contains("not_null"), "test");
    assert!(page.contains("Required columns"), "contract rules");
    assert!(
        page.contains("../sources/raw.orders.html#col-id"),
        "upstream column link"
    );
    assert!(
        page.contains("../models/order_totals.html"),
        "downstream link"
    );

    let totals = read(out.join("models/order_totals.html"));
    assert!(totals.contains("../models/stg_orders.html#col-amount"));
    assert!(totals.contains("aggregation"), "transform shown");

    let data = read(out.join("assets/data.js"));
    let json: serde_json::Value = serde_json::from_str(
        data.trim_start_matches("window.ROCKY_DOCS = ")
            .trim_end()
            .trim_end_matches(';'),
    )
    .expect("data.js is JSON");
    assert_eq!(json["models"].as_array().expect("models").len(), 2);
    assert_eq!(json["sources"][0]["name"], "raw.orders");
    assert!(
        json["column_lineage"]
            .as_array()
            .expect("column_lineage")
            .iter()
            .any(|e| e[0] == "stg_orders"
                && e[1] == "amount"
                && e[2] == "order_totals"
                && e[3] == "total")
    );

    for file in ["index.html", "models/stg_orders.html", "assets/site.js"] {
        let text = read(out.join(file)).replace("http://www.w3.org/2000/svg", "");
        assert!(
            !text.contains("http://") && !text.contains("https://"),
            "{file} reaches out"
        );
    }
}

#[test]
fn a_rerun_removes_pages_of_deleted_models() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    project(dir);
    let out = dir.join("site");
    let out_arg = out.to_str().expect("utf8");
    assert!(rocky(dir, &["--output-path", out_arg]).status.success());
    assert!(out.join("models/order_totals.html").is_file());

    fs::remove_file(dir.join("models/order_totals.sql")).expect("rm sql");
    fs::remove_file(dir.join("models/order_totals.toml")).expect("rm toml");
    fs::write(out.join("notes.txt"), "mine").expect("user file");
    assert!(rocky(dir, &["--output-path", out_arg]).status.success());
    assert!(
        !out.join("models/order_totals.html").exists(),
        "stale page kept"
    );
    assert!(out.join("notes.txt").exists(), "unrelated file removed");
}

#[test]
fn an_html_output_path_still_writes_one_file() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    project(dir);
    let file = dir.join("catalog.html");
    let result = rocky(dir, &["--output-path", file.to_str().expect("utf8")]);
    assert!(result.status.success());
    assert!(file.is_file());
    assert!(read(&file).contains("stg_orders"));
    assert!(!dir.join("catalog.html").is_dir());
}

#[test]
fn parquet_tables_round_trip_through_duckdb() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    project(dir);
    let out = dir.join("meta");
    let result = rocky(
        dir,
        &[
            "--format",
            "parquet",
            "--output-path",
            out.to_str().expect("utf8"),
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    for table in [
        "models",
        "columns",
        "edges",
        "column_lineage",
        "tests",
        "contracts",
        "sources",
    ] {
        assert!(out.join(format!("{table}.parquet")).is_file(), "{table}");
    }

    let conn = duckdb::Connection::open_in_memory().expect("duckdb");
    let q = |sql: &str| -> Vec<String> {
        let mut stmt = conn.prepare(sql).expect("prepare");
        let rows = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .expect("query");
        rows.map(|r| r.expect("row")).collect()
    };
    let t = |name: &str| {
        format!(
            "read_parquet('{}')",
            out.join(format!("{name}.parquet")).display()
        )
    };

    assert_eq!(
        q(&format!("SELECT name FROM {} ORDER BY name", t("models"))),
        vec!["order_totals", "stg_orders"]
    );
    assert_eq!(
        q(&format!(
            "SELECT column_name FROM (SELECT name AS column_name FROM {} WHERE model = 'stg_orders' AND classification = 'pii')",
            t("columns")
        )),
        vec!["email"]
    );
    assert_eq!(
        q(&format!(
            "SELECT description FROM {} WHERE model = 'stg_orders' AND name = 'id'",
            t("columns")
        )),
        vec!["Order key"]
    );
    // Join the tables: which models read a given source column?
    assert_eq!(
        q(&format!(
            "SELECT DISTINCT target_model FROM {} WHERE source_model = 'raw.orders' AND source_column = 'amount'",
            t("column_lineage")
        )),
        vec!["stg_orders"]
    );
    assert_eq!(
        q(&format!(
            "SELECT upstream || '>' || downstream FROM {} ORDER BY 1",
            t("edges")
        )),
        vec!["raw.orders>stg_orders", "stg_orders>order_totals"]
    );
    assert_eq!(
        q(&format!(
            "SELECT kind FROM {} WHERE model = 'stg_orders'",
            t("tests")
        )),
        vec!["not_null"]
    );
    assert_eq!(
        q(&format!(
            "SELECT kind || ':' || coalesce(column_name, '') FROM {} ORDER BY 1",
            t("contracts")
        )),
        vec!["column:id", "required:id"]
    );
    assert_eq!(
        q(&format!("SELECT name FROM {}", t("sources"))),
        vec!["raw.orders"]
    );
    // Typed columns survive: `nullable` is a BOOLEAN, `ordinal` a BIGINT.
    assert_eq!(
        q(&format!(
            "SELECT typeof(nullable) || '/' || typeof(ordinal) FROM {} LIMIT 1",
            t("columns")
        )),
        vec!["BOOLEAN/BIGINT"]
    );
}

#[test]
fn json_output_reports_format_and_files() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    project(dir);
    let out = dir.join("meta");
    let result = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .args([
            "--output", "json", "docs", "--format", "parquet", "--models",
        ])
        .arg(dir.join("models"))
        .arg("--output-path")
        .arg(&out)
        .current_dir(dir)
        .output()
        .expect("spawn");
    assert!(result.status.success());
    let json: serde_json::Value = serde_json::from_slice(&result.stdout).expect("stdout is JSON");
    assert_eq!(json["format"], "parquet");
    assert_eq!(json["sources_count"], 1);
    assert_eq!(json["files"].as_array().expect("files").len(), 7);
}
