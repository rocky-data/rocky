//! End-to-end checks for model governance: access levels (E047) and model
//! versions (E048, W048), through the real `rocky` binary on DuckDB.
//!
//! Covers the user-visible contract:
//! - a `private` model read from another group fails `rocky compile` (E047),
//!   while same-group reads and the `protected` default stay clean;
//! - versioned models materialize as `<name>_v<N>`, `<name>` is a view over
//!   the latest version, and `<name>_v<N>` pins a version;
//! - an undeclared version or a missing latest fails with E048;
//! - a deprecated version warns (W048) on a pinned clock, and not before the
//!   warning window;
//! - `rocky publish-ir` exports only public models, and a consumer reading a
//!   withheld producer model gets E047;
//! - a project that declares no governance compiles exactly as before.

#![cfg(feature = "duckdb")]

use std::path::Path;
use std::process::{Command, Output};

const SEED: &str = "CREATE SCHEMA raw;\n\
CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, order_date DATE);\n\
INSERT INTO raw.orders VALUES (1, 10, 5.0, 'ok', DATE '2026-01-01');\n\
CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n\
INSERT INTO raw.customers VALUES (10, 'Ada', 'ada@example.com');\n";

fn write(root: &Path, rel: &str, body: &str) {
    let path = root.join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, body).unwrap();
}

/// A DuckDB project with `raw.orders` / `raw.customers` seeded both into the
/// warehouse file (for `rocky run`) and into `data/seed.sql` (for
/// `--with-seed`).
fn project(root: &Path) {
    let db = root.join("wh.duckdb");
    write(
        root,
        "rocky.toml",
        &format!(
            "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n[pipeline.p.target.governance]\nauto_create_schemas = true\n",
            db.display()
        ),
    );
    write(root, "data/seed.sql", SEED);
    write(
        root,
        "models/_defaults.toml",
        "[target]\ncatalog = \"wh\"\nschema = \"main\"\n",
    );
    let conn = duckdb::Connection::open(&db).unwrap();
    conn.execute_batch(SEED).unwrap();
}

fn rocky(root: &Path, args: &[&str], today: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    cmd.current_dir(root)
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("--state-path")
        .arg(root.join("state.redb"))
        .args(args)
        .env_remove("ROCKY_GOVERNANCE_TODAY");
    if let Some(today) = today {
        cmd.env("ROCKY_GOVERNANCE_TODAY", today);
    }
    cmd.output().unwrap()
}

/// `(severity, code, model, message)` for every diagnostic of a JSON compile.
fn compile(root: &Path, today: Option<&str>) -> (bool, Vec<(String, String, String, String)>) {
    let out = rocky(root, &["compile", "--with-seed", "--output", "json"], today);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json: serde_json::Value = serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "compile JSON did not parse ({e}): {stdout}\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    });
    let diags = json["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .map(|d| {
            (
                d["severity"].as_str().unwrap_or_default().to_string(),
                d["code"].as_str().unwrap_or_default().to_string(),
                d["model"].as_str().unwrap_or_default().to_string(),
                d["message"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    (out.status.success(), diags)
}

fn codes<'a>(diags: &'a [(String, String, String, String)], code: &str) -> Vec<&'a str> {
    diags
        .iter()
        .filter(|d| d.1 == code)
        .map(|d| d.2.as_str())
        .collect()
}

#[test]
fn private_model_is_refused_across_groups_and_allowed_within() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    project(root);
    write(
        root,
        "models/groups/finance.toml",
        "[owner]\nname = \"Finance Data\"\nemail = \"finance@example.com\"\n",
    );
    write(
        root,
        "models/fin_base.sql",
        "SELECT order_id, customer_id, amount FROM raw.orders\n",
    );
    write(
        root,
        "models/fin_base.toml",
        "access = \"private\"\naccess_group = \"finance\"\n",
    );
    // Same group: allowed.
    write(
        root,
        "models/fin_mart.sql",
        "SELECT order_id, amount FROM fin_base\n",
    );
    write(root, "models/fin_mart.toml", "access_group = \"finance\"\n");
    // Protected (default) upstream read from anywhere: allowed.
    write(
        root,
        "models/stg_customers.sql",
        "SELECT customer_id, customer_name FROM raw.customers\n",
    );
    write(root, "models/stg_customers.toml", "");
    write(
        root,
        "models/mkt_customers.sql",
        "SELECT customer_id FROM stg_customers\n",
    );
    write(
        root,
        "models/mkt_customers.toml",
        "access_group = \"marketing\"\n",
    );

    let (ok, diags) = compile(root, None);
    assert!(ok, "valid governance must compile clean: {diags:?}");
    assert!(codes(&diags, "E047").is_empty(), "{diags:?}");

    // Another group reads the private model: E047 on the consumer.
    write(
        root,
        "models/mkt_report.sql",
        "SELECT order_id FROM fin_base\n",
    );
    write(
        root,
        "models/mkt_report.toml",
        "access_group = \"marketing\"\n",
    );
    let (ok, diags) = compile(root, None);
    assert!(!ok, "cross-group private read must fail compile");
    assert_eq!(codes(&diags, "E047"), vec!["mkt_report"], "{diags:?}");

    // The group owner surfaces in the catalog.
    std::fs::remove_file(root.join("models/mkt_report.sql")).unwrap();
    std::fs::remove_file(root.join("models/mkt_report.toml")).unwrap();
    let out = rocky(root, &["catalog", "--output", "json"], None);
    assert!(
        out.status.success(),
        "catalog: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let catalog = std::fs::read_to_string(root.join(".rocky/catalog/catalog.json")).unwrap();
    let catalog: serde_json::Value = serde_json::from_str(&catalog).unwrap();
    let fin_base = catalog["assets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["model_name"] == "fin_base")
        .unwrap();
    assert_eq!(fin_base["governance"]["access"], "private");
    assert_eq!(fin_base["governance"]["group"], "finance");
    assert_eq!(fin_base["governance"]["owner_email"], "finance@example.com");
}

fn versioned_project(root: &Path) {
    project(root);
    write(
        root,
        "models/orders.toml",
        "latest_version = 2\naccess = \"public\"\n\n[[versions]]\nv = 1\ndeprecation_date = \"2026-10-20\"\n\n[[versions]]\nv = 2\n",
    );
    write(
        root,
        "models/orders_v1.sql",
        "SELECT order_id, amount FROM raw.orders\n",
    );
    write(root, "models/orders_v1.toml", "");
    write(
        root,
        "models/orders_v2.sql",
        "SELECT order_id, amount AS order_amount FROM raw.orders\n",
    );
    write(root, "models/orders_v2.toml", "");
    // Bare name: the latest version.
    write(
        root,
        "models/use_latest.sql",
        "SELECT order_id, order_amount FROM orders\n",
    );
    write(root, "models/use_latest.toml", "");
    // Pinned version.
    write(
        root,
        "models/use_v1.sql",
        "SELECT order_id, amount FROM orders_v1\n",
    );
    write(root, "models/use_v1.toml", "");
}

#[test]
fn versions_materialize_alias_latest_and_pin() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    versioned_project(root);

    // Far before the deprecation window: no W048, no error.
    let (ok, diags) = compile(root, Some("2026-01-01"));
    assert!(ok, "{diags:?}");
    assert!(codes(&diags, "W048").is_empty(), "{diags:?}");
    assert!(codes(&diags, "E048").is_empty(), "{diags:?}");
    // The generated alias raises no SELECT * noise.
    assert!(
        !diags
            .iter()
            .any(|d| d.2 == "orders" && (d.1 == "I001" || d.1 == "P002")),
        "{diags:?}"
    );

    let out = rocky(
        root,
        &["run", "--pipeline", "p", "--output", "json"],
        Some("2026-01-01"),
    );
    assert!(
        out.status.success(),
        "run: {}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let conn = duckdb::Connection::open(root.join("wh.duckdb")).unwrap();
    let kind = |table: &str| -> String {
        conn.query_row(
            "SELECT table_type FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?",
            [table],
            |row| row.get(0),
        )
        .unwrap_or_else(|e| panic!("{table}: {e}"))
    };
    assert_eq!(kind("orders_v1"), "BASE TABLE");
    assert_eq!(kind("orders_v2"), "BASE TABLE");
    assert_eq!(kind("orders"), "VIEW", "latest alias is a view");
    // The alias reads the latest version's columns.
    let latest_amount: f64 = conn
        .query_row("SELECT order_amount FROM main.orders", [], |r| r.get(0))
        .unwrap();
    assert_eq!(latest_amount, 5.0);
    let pinned: f64 = conn
        .query_row("SELECT amount FROM main.use_v1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(pinned, 5.0);
}

#[test]
fn deprecated_version_warns_on_a_pinned_clock() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    versioned_project(root);

    // Within 30 days of the date: a warning, not an error.
    let (ok, diags) = compile(root, Some("2026-10-04"));
    assert!(ok, "W048 must not fail the compile: {diags:?}");
    assert_eq!(codes(&diags, "W048"), vec!["use_v1"], "{diags:?}");
    // After the date: still a warning, worded as past.
    let (ok, diags) = compile(root, Some("2026-11-01"));
    assert!(ok);
    let w = diags.iter().find(|d| d.1 == "W048").unwrap();
    assert!(w.3.contains("was deprecated on 2026-10-20"), "{}", w.3);
    // The latest-version reader is never warned.
    assert!(!codes(&diags, "W048").contains(&"use_latest"));
}

#[test]
fn unknown_version_and_missing_latest_are_e048() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    versioned_project(root);
    write(
        root,
        "models/use_v9.sql",
        "SELECT order_id FROM orders_v9\n",
    );
    write(root, "models/use_v9.toml", "");
    let (ok, diags) = compile(root, Some("2026-01-01"));
    assert!(!ok);
    assert_eq!(codes(&diags, "E048"), vec!["use_v9"], "{diags:?}");

    std::fs::remove_file(root.join("models/use_v9.sql")).unwrap();
    std::fs::remove_file(root.join("models/use_v9.toml")).unwrap();
    write(
        root,
        "models/orders.toml",
        "latest_version = 3\n\n[[versions]]\nv = 1\n\n[[versions]]\nv = 2\n",
    );
    let (ok, diags) = compile(root, Some("2026-01-01"));
    assert!(!ok);
    let e048: Vec<_> = diags.iter().filter(|d| d.1 == "E048").collect();
    assert!(
        e048.iter().any(|d| d.3.contains("latest_version = 3")),
        "{diags:?}"
    );
}

#[test]
fn publish_ir_exports_only_public_and_consumers_get_e047() {
    let temp = tempfile::tempdir().unwrap();
    let producer = temp.path().join("producer");
    let consumer = temp.path().join("consumer");
    project(&producer);
    write(
        &producer,
        "models/orders.sql",
        "SELECT order_id, amount FROM raw.orders\n",
    );
    write(&producer, "models/orders.toml", "access = \"public\"\n");
    write(
        &producer,
        "models/margins.sql",
        "SELECT order_id, amount * 0.1 AS margin FROM raw.orders\n",
    );
    write(&producer, "models/margins.toml", "");

    let out = rocky(
        &producer,
        &["publish-ir", "--with-seed", "--out", "snap.json"],
        None,
    );
    assert!(
        out.status.success(),
        "publish-ir: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let snap: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(producer.join("snap.json")).unwrap())
            .unwrap();
    let names: Vec<&str> = snap["ir"]["models"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["orders"], "only the public model is exported");
    assert_eq!(
        snap["governance"]["withheld"]["wh.main.margins"]["access"],
        "protected"
    );

    project(&consumer);
    std::fs::create_dir_all(consumer.join("vendor")).unwrap();
    std::fs::copy(
        producer.join("snap.json"),
        consumer.join("vendor/shop.json"),
    )
    .unwrap();
    let toml = std::fs::read_to_string(consumer.join("rocky.toml")).unwrap();
    write(
        &consumer,
        "rocky.toml",
        &format!("{toml}\n[imports.shop]\npath = \"vendor\"\nsnapshot = \"shop.json\"\n"),
    );
    // Reads the public model: clean.
    write(
        &consumer,
        "models/c_orders.sql",
        "SELECT order_id FROM wh.main.orders\n",
    );
    write(
        &consumer,
        "models/c_orders.toml",
        "[[sources]]\ncatalog = \"wh\"\nschema = \"main\"\ntable = \"orders\"\n",
    );
    let (ok, diags) = compile(&consumer, None);
    assert!(ok, "{diags:?}");
    assert!(codes(&diags, "E047").is_empty());

    // Reads the withheld model: E047.
    write(
        &consumer,
        "models/c_margins.sql",
        "SELECT order_id FROM wh.main.margins\n",
    );
    write(
        &consumer,
        "models/c_margins.toml",
        "[[sources]]\ncatalog = \"wh\"\nschema = \"main\"\ntable = \"margins\"\n",
    );
    let (ok, diags) = compile(&consumer, None);
    assert!(!ok);
    assert_eq!(codes(&diags, "E047"), vec!["c_margins"], "{diags:?}");
}

/// The reference controls from the dbt-gap corpus: a project with no
/// governance keys must compile exactly as before (exit 0, no E047/E048/W048).
#[test]
fn ungoverned_project_is_unchanged() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path();
    project(root);
    write(
        root,
        "models/stg_orders.sql",
        "SELECT order_id, customer_id, amount FROM raw.orders\n",
    );
    write(root, "models/stg_orders.toml", "");
    write(
        root,
        "models/fct_revenue.sql",
        "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name\n",
    );
    write(root, "models/fct_revenue.toml", "");
    // Model names that merely look versioned are not versions.
    write(
        root,
        "models/report_v2.sql",
        "SELECT order_id FROM stg_orders\n",
    );
    write(root, "models/report_v2.toml", "");
    let (ok, diags) = compile(root, Some("2026-10-04"));
    assert!(ok, "{diags:?}");
    for code in ["E047", "E048", "W048"] {
        assert!(codes(&diags, code).is_empty(), "{code}: {diags:?}");
    }
}
