//! End-to-end tests for dbt-style node selection (`--select` / `--exclude`)
//! against a DuckDB project: seven models across two directories, with tags,
//! a diamond join, a view, and raw sources.
//!
//! ```text
//!   raw.orders ──► stg_orders ──┐
//!                               ├──► int_orders_enriched ──► fct_revenue
//!   raw.customers ► stg_customers ┤
//!                               └──► dim_customers
//!   raw.orders ──► stg_payments ──► rpt_payments
//! ```

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::process::{Command, Output};

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

const SEED: &str = "
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, order_date DATE);
CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);
INSERT INTO raw.orders VALUES (1, 10, 5.0, 'paid', DATE '2026-01-01'), (2, 20, 7.5, 'paid', DATE '2026-01-02');
INSERT INTO raw.customers VALUES (10, 'ada', 'a@x'), (20, 'bob', 'b@x');
";

/// (directory, name, sql, strategy, tags)
const MODELS: &[(&str, &str, &str, &str, &str)] = &[
    (
        "staging",
        "stg_orders",
        "SELECT order_id, customer_id, amount, status FROM raw.orders",
        "full_refresh",
        "domain = \"finance\"",
    ),
    (
        "staging",
        "stg_customers",
        "SELECT customer_id, customer_name FROM raw.customers",
        "view",
        "domain = \"crm\"",
    ),
    (
        "staging",
        "stg_payments",
        "SELECT order_id, amount AS paid FROM raw.orders",
        "full_refresh",
        "domain = \"finance\"\nlifecycle = \"deprecated\"",
    ),
    (
        "marts",
        "int_orders_enriched",
        "SELECT o.order_id, o.amount, c.customer_name FROM stg_orders o JOIN stg_customers c ON o.customer_id = c.customer_id",
        "full_refresh",
        "tier = \"silver\"",
    ),
    (
        "marts",
        "fct_revenue",
        "SELECT customer_name, SUM(amount) AS total FROM int_orders_enriched GROUP BY customer_name",
        "full_refresh",
        "tier = \"gold\"\ndomain = \"finance\"",
    ),
    (
        "marts",
        "dim_customers",
        "SELECT customer_id, customer_name FROM stg_customers",
        "full_refresh",
        "tier = \"gold\"",
    ),
    (
        "marts",
        "rpt_payments",
        "SELECT order_id, paid FROM stg_payments",
        "full_refresh",
        "tier = \"bronze\"",
    ),
];

fn write_project(root: &Path) {
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("data/seed.sql"), SEED).unwrap();
    for (dir, name, sql, strategy, tags) in MODELS {
        let d = root.join("models").join(dir);
        fs::create_dir_all(&d).unwrap();
        fs::write(d.join(format!("{name}.sql")), format!("{sql}\n")).unwrap();
        fs::write(
            d.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"{strategy}\"\n\n[target]\ncatalog = \"probe\"\nschema = \"main\"\ntable = \"{name}\"\n\n[tags]\n{tags}\n"
            ),
        )
        .unwrap();
    }
}

fn project() -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    write_project(tmp.path());
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("rocky must launch")
}

fn json(out: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(
        stdout
            .lines()
            .skip_while(|l| !l.trim_start().starts_with('{'))
            .collect::<Vec<_>>()
            .join("\n")
            .as_str(),
    )
    .unwrap_or_else(|e| {
        panic!(
            "stdout must be JSON ({e})\nstdout:\n{stdout}\nstderr:\n{}",
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

fn ok(out: &Output, what: &str) {
    assert!(
        out.status.success(),
        "{what} must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(ToString::to_string).collect()
}

/// `rocky list [models] --select ... -o json` → the resolved model names.
fn list(root: &Path, args: &[&str]) -> BTreeSet<String> {
    let mut full = vec!["list"];
    full.extend_from_slice(args);
    full.extend_from_slice(&["--output", "json"]);
    let out = rocky(root, &full);
    ok(&out, &format!("rocky {}", full.join(" ")));
    json(&out)["models"]
        .as_array()
        .expect("models array")
        .iter()
        .map(|m| m["name"].as_str().unwrap().to_string())
        .collect()
}

#[test]
fn list_resolves_names_globs_graph_and_methods() {
    let tmp = project();
    let root = tmp.path();

    // No selector: every model, unchanged from before.
    assert_eq!(list(root, &["models"]).len(), 7);
    assert_eq!(list(root, &[]).len(), 7, "bare `rocky list` lists models");

    // Graph operators over the diamond.
    assert_eq!(
        list(root, &["--select", "+fct_revenue"]),
        set(&[
            "stg_orders",
            "stg_customers",
            "int_orders_enriched",
            "fct_revenue"
        ])
    );
    assert_eq!(
        list(root, &["models", "-s", "stg_customers+1"]),
        set(&["stg_customers", "int_orders_enriched", "dim_customers"])
    );
    assert_eq!(
        list(root, &["--select", "1+fct_revenue"]),
        set(&["int_orders_enriched", "fct_revenue"])
    );
    assert_eq!(
        list(root, &["--select", "@stg_orders"]),
        set(&[
            "stg_orders",
            "stg_customers",
            "int_orders_enriched",
            "fct_revenue"
        ])
    );

    // Directories, tags, config, sources.
    assert_eq!(
        list(root, &["--select", "path:models/staging"]),
        set(&["stg_orders", "stg_customers", "stg_payments"])
    );
    assert_eq!(
        list(root, &["--select", "tag:finance"]),
        set(&["stg_orders", "stg_payments", "fct_revenue"])
    );
    assert_eq!(
        list(root, &["--select", "tag:finance,path:marts"]),
        set(&["fct_revenue"])
    );
    assert_eq!(
        list(root, &["--select", "config.materialized:view"]),
        set(&["stg_customers"])
    );
    assert_eq!(
        list(root, &["--select", "source:raw.customers"]),
        set(&["stg_customers"])
    );

    // Union (two values and a space), globs, and exclude.
    assert_eq!(
        list(root, &["--select", "dim_customers", "rpt_*"]),
        set(&["dim_customers", "rpt_payments"])
    );
    assert_eq!(
        list(
            root,
            &["--select", "stg_*", "--exclude", "tag:lifecycle=deprecated"]
        ),
        set(&["stg_orders", "stg_customers"])
    );
    assert_eq!(
        list(root, &["--exclude", "path:models/marts"]),
        set(&["stg_orders", "stg_customers", "stg_payments"]),
        "exclude alone subtracts from every model"
    );
}

#[test]
fn list_selector_errors_and_empty_selection() {
    let tmp = project();
    let root = tmp.path();

    let out = rocky(root, &["list", "--select", "owner:me", "--output", "json"]);
    assert!(!out.status.success(), "an unknown method must fail");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("unknown selector method 'owner'"),
        "stderr:\n{stderr}"
    );

    // dbt semantics: no match is a warning plus "nothing to do", exit 0.
    let out = rocky(root, &["list", "--select", "nope_*", "--output", "json"]);
    ok(&out, "an empty selection");
    assert_eq!(json(&out)["models"].as_array().unwrap().len(), 0);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not match any enabled nodes"),
        "stderr:\n{stderr}"
    );
    assert!(stderr.contains("Nothing to do"), "stderr:\n{stderr}");

    // `--model` folded into a selection must still name a real model.
    let out = rocky(root, &["list", "models", "--exclude", "x"]);
    ok(&out, "exclude alone");
    let out = rocky(
        root,
        &[
            "compile",
            "--models",
            "models",
            "--model",
            "typo",
            "--exclude",
            "dim_customers",
        ],
    );
    assert!(!out.status.success(), "an unknown --model stays an error");
    assert!(String::from_utf8_lossy(&out.stderr).contains("model 'typo' not found"));

    // `--model` with `--select` is refused; `--model` alone is unchanged.
    let out = rocky(
        root,
        &[
            "compile",
            "--models",
            "models",
            "--model",
            "fct_revenue",
            "--select",
            "dim_customers",
        ],
    );
    assert!(!out.status.success());
}

#[test]
fn compile_and_emit_sql_scope_to_the_selection() {
    let tmp = project();
    let root = tmp.path();

    let out = rocky(
        root,
        &[
            "compile",
            "--models",
            "models",
            "--with-seed",
            "--select",
            "+fct_revenue",
            "--output",
            "json",
        ],
    );
    ok(&out, "compile --select");
    let v = json(&out);
    let names: BTreeSet<String> = v["models_detail"]
        .as_array()
        .expect("models_detail")
        .iter()
        .map(|m| m["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        names,
        set(&[
            "stg_orders",
            "stg_customers",
            "int_orders_enriched",
            "fct_revenue"
        ])
    );
    assert_eq!(v["has_errors"], false);

    let out = rocky(
        root,
        &[
            "emit-sql",
            "--models",
            "models",
            "--select",
            "tag:tier=gold",
        ],
    );
    ok(&out, "emit-sql --select");
    let sql = String::from_utf8_lossy(&out.stdout);
    assert!(
        sql.contains("fct_revenue") && sql.contains("dim_customers"),
        "{sql}"
    );
    assert!(!sql.contains("rpt_payments"), "{sql}");
}

#[test]
fn test_command_scopes_to_the_selection() {
    let tmp = project();
    let root = tmp.path();
    let out = rocky(
        root,
        &[
            "test",
            "--models",
            "models",
            "--select",
            "path:marts",
            "--exclude",
            "rpt_payments",
            "--output",
            "json",
        ],
    );
    ok(&out, "test --select");
    let v = json(&out);
    assert_eq!(v["total"], 3, "{v}");
    assert_eq!(v["passed"], 3, "{v}");
}

fn tables(root: &Path) -> BTreeSet<String> {
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let mut stmt = conn
        .prepare("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'")
        .unwrap();
    stmt.query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn run_builds_only_the_selection_and_reads_unselected_upstreams_as_is() {
    let tmp = project();
    let root = tmp.path();
    {
        let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
        conn.execute_batch(SEED).unwrap();
    }

    // Multi-model selection: the upstream half of the diamond.
    let out = rocky(
        root,
        &[
            "run",
            "--select",
            "+int_orders_enriched",
            "--output",
            "json",
        ],
    );
    ok(&out, "run --select +int_orders_enriched");
    let built = tables(root);
    assert_eq!(
        built,
        set(&["stg_orders", "stg_customers", "int_orders_enriched"]),
        "exactly the selection is built"
    );

    // An unselected upstream is read as it exists (dbt semantics): building
    // two marts reads the already-built `int_orders_enriched` / `stg_customers`.
    let out = rocky(
        root,
        &[
            "run",
            "--select",
            "fct_revenue dim_customers",
            "--output",
            "json",
        ],
    );
    ok(&out, "run --select two marts");
    let built = tables(root);
    assert!(built.contains("fct_revenue") && built.contains("dim_customers"));
    assert!(!built.contains("rpt_payments") && !built.contains("stg_payments"));

    // `--defer` still works with a multi-model selection.
    let out = rocky(
        root,
        &[
            "run",
            "--select",
            "fct_revenue,tag:tier=gold dim_customers",
            "--defer",
            "--output",
            "json",
        ],
    );
    ok(&out, "run --select ... --defer");

    // A one-model selection takes the `--model` path.
    let out = rocky(
        root,
        &["run", "--select", "stg_payments", "--output", "json"],
    );
    ok(&out, "run --select one model");
    assert!(tables(root).contains("stg_payments"));

    // Nothing selected: nothing to do, exit 0, no table written.
    let out = rocky(root, &["run", "--select", "tag:nope", "--output", "json"]);
    ok(&out, "run with an empty selection");
    let v = json(&out);
    assert_eq!(v["tables_failed"], 0, "{v}");
    assert_eq!(v["materializations"].as_array().unwrap().len(), 0, "{v}");
    assert!(!tables(root).contains("rpt_payments"));

    // Incompatible flags are refused before any work.
    let out = rocky(root, &["run", "--select", "fct_revenue", "--dag"]);
    assert!(!out.status.success());
}

#[test]
fn plan_accepts_a_single_model_selection_only() {
    let tmp = project();
    let root = tmp.path();
    let out = rocky(
        root,
        &["plan", "--select", "+fct_revenue", "--output", "json"],
    );
    assert!(!out.status.success(), "a multi-model plan is refused");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("resolved to 4 models"), "stderr:\n{stderr}");
}

fn git(root: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(["-c", "user.email=t@example.com", "-c", "user.name=t"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("git must launch");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn state_selectors_diff_against_a_git_ref() {
    let tmp = project();
    let root = tmp.path();
    git(root, &["init", "-q", "-b", "trunk"]);
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "base"]);
    git(root, &["checkout", "-qb", "feature"]);

    // Modify a nested model and add a new one, then commit (state compares
    // committed changes, like `rocky ci-diff`).
    fs::write(
        root.join("models/marts/dim_customers.sql"),
        "SELECT customer_id, upper(customer_name) AS customer_name FROM stg_customers\n",
    )
    .unwrap();
    fs::write(
        root.join("models/marts/dim_new.sql"),
        "SELECT customer_id FROM dim_customers\n",
    )
    .unwrap();
    fs::write(
        root.join("models/marts/dim_new.toml"),
        "name = \"dim_new\"\n\n[target]\ncatalog = \"probe\"\nschema = \"main\"\ntable = \"dim_new\"\n",
    )
    .unwrap();
    git(root, &["add", "-A"]);
    git(root, &["commit", "-qm", "change"]);

    assert_eq!(
        list(
            root,
            &["--select", "state:modified", "--state-ref", "trunk"]
        ),
        set(&["dim_customers", "dim_new"]),
        "state:modified includes new models (dbt semantics)"
    );
    assert_eq!(
        list(root, &["--select", "state:new", "--state-ref", "trunk"]),
        set(&["dim_new"])
    );
    assert_eq!(
        list(
            root,
            &[
                "--select",
                "state:modified+",
                "--exclude",
                "state:new",
                "--state-ref",
                "trunk"
            ]
        ),
        set(&["dim_customers"])
    );

    // A bad ref is a clear error, not an empty selection.
    let out = rocky(
        root,
        &[
            "list",
            "--select",
            "state:modified",
            "--state-ref",
            "no_such_ref",
        ],
    );
    assert!(!out.status.success());
}
