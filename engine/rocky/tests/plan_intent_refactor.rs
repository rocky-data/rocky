//! `rocky plan --intent refactor` (RV2-P1): end-to-end through the binary.
//!
//! Each test builds a git repository with a DuckDB project, commits the base
//! models on `main`, edits the working tree, and runs `rocky plan --intent
//! refactor`. The check compares the base SQL with the working-tree SQL on
//! the data in `fixture.duckdb`.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

const ROCKY_TOML: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "default"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
catalog_template = "fixture"
schema_template = "staging__{source}"

[pipeline.ingest.target.governance]
auto_create_schemas = true
"#;

/// Five orders. Order 1 has the only amount under 10. Order 4 has a NULL
/// customer. Order 5 has a NULL amount.
const SEED: &str = "
    CREATE SCHEMA raw__orders;
    CREATE TABLE raw__orders.orders AS SELECT * FROM (VALUES
        (1, 10, 9.99, 'complete'),
        (2, 10, 20.0, 'pending'),
        (3, 11, 35.5, 'complete'),
        (4, NULL, 12.0, 'complete'),
        (5, 12, NULL, 'cancelled')
    ) AS v(order_id, customer_id, amount, status);
";

const STG_ORDERS: &str = "SELECT order_id, customer_id, amount, status FROM raw__orders.orders";
const FCT_REVENUE: &str =
    "SELECT customer_id, SUM(amount) AS total FROM stg_orders GROUP BY customer_id";

fn sidecar(depends_on: &[&str]) -> String {
    let deps = if depends_on.is_empty() {
        String::new()
    } else {
        let quoted: Vec<String> = depends_on.iter().map(|d| format!("\"{d}\"")).collect();
        format!("depends_on = [{}]\n\n", quoted.join(", "))
    };
    format!(
        "{deps}[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"main\"\n"
    )
}

fn run_git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git must run");
    assert!(
        out.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

struct Project {
    tmp: tempfile::TempDir,
}

impl Project {
    /// A committed `main` with `stg_orders` and `fct_revenue`, and the
    /// materialized `stg_orders` table that `fct_revenue` reads.
    fn new() -> Self {
        let project = Project {
            tmp: tempfile::tempdir().expect("tempdir"),
        };
        let dir = project.dir();
        {
            let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
            conn.execute_batch(SEED).expect("seed source");
            conn.execute_batch(&format!("CREATE TABLE main.stg_orders AS {STG_ORDERS}"))
                .expect("materialize stg_orders");
        }
        fs::write(dir.join("rocky.toml"), ROCKY_TOML).expect("write config");
        fs::write(
            dir.join(".gitignore"),
            "fixture.duckdb*\n.rocky/\n*.redb\n*.redb.*\n",
        )
        .expect("write .gitignore");
        fs::create_dir(dir.join("models")).expect("create models");
        project.write_model("stg_orders", STG_ORDERS, &[]);
        project.write_model("fct_revenue", FCT_REVENUE, &["stg_orders"]);

        run_git(dir, &["init", "-q", "-b", "main"]);
        run_git(dir, &["config", "user.email", "tester@example.com"]);
        run_git(dir, &["config", "user.name", "Tester"]);
        run_git(dir, &["config", "commit.gpgsign", "false"]);
        run_git(dir, &["add", "."]);
        run_git(dir, &["commit", "-q", "-m", "base"]);
        project
    }

    fn dir(&self) -> &Path {
        self.tmp.path()
    }

    fn write_model(&self, name: &str, sql: &str, depends_on: &[&str]) {
        let models = self.dir().join("models");
        fs::write(models.join(format!("{name}.sql")), sql).expect("write model sql");
        fs::write(models.join(format!("{name}.toml")), sidecar(depends_on))
            .expect("write model sidecar");
    }

    fn plan(&self, output: &str, extra: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_rocky"))
            .args(["--output", output])
            .arg("--config")
            .arg(self.dir().join("rocky.toml"))
            .arg("plan")
            .args(extra)
            .current_dir(self.dir())
            .env("RUST_LOG", "error")
            .output()
            .expect("run rocky plan")
    }

    fn plan_json(&self, extra: &[&str]) -> Value {
        let out = self.plan("json", extra);
        assert!(
            out.status.success(),
            "plan failed ({:?}): {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        serde_json::from_slice(&out.stdout).expect("plan output is JSON")
    }

    fn check(&self, extra: &[&str]) -> Value {
        let mut args = vec!["--intent", "refactor"];
        args.extend_from_slice(extra);
        let plan = self.plan_json(&args);
        plan["intent_check"].clone()
    }

    fn plan_file(&self, plan_id: &str) -> PathBuf {
        self.dir()
            .join(".rocky")
            .join("plans")
            .join(format!("{plan_id}.json"))
    }
}

fn model<'a>(check: &'a Value, name: &str) -> &'a Value {
    check["models"]
        .as_array()
        .expect("models array")
        .iter()
        .find(|m| m["model"] == name)
        .unwrap_or_else(|| panic!("no verdict for {name} in {check:#}"))
}

/// (1) A CTE plus alias rewrite keeps the output: `match`, with row counts.
#[test]
fn cte_and_alias_refactor_matches() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        "WITH src AS (SELECT * FROM raw__orders.orders)\n\
         SELECT o.order_id, o.customer_id, o.amount, o.status FROM src AS o",
        &[],
    );
    let check = p.check(&[]);
    assert_eq!(check["intent"], "refactor");
    assert_eq!(check["base_ref"], "main");
    assert_eq!(check["adapter"], "duckdb");
    assert_eq!(check["probes"], 5);
    assert!(
        check["caveat"]
            .as_str()
            .unwrap_or("")
            .contains("not a proof for other inputs")
    );
    assert_eq!(
        check["models"].as_array().map(Vec::len),
        Some(1),
        "{check:#}"
    );
    let m = model(&check, "stg_orders");
    assert_eq!(m["verdict"], "match", "{m:#}");
    assert!(m.get("reason").is_none());
    assert_eq!(m["rows_base"], 5);
    assert_eq!(m["rows_head"], 5);
    assert_eq!(m["rows_only_in_base"], 0);
    assert_eq!(m["rows_only_in_head"], 0);
    assert_eq!(check["summary"]["match"], 1);

    // (12) No temporary build table leaks into the project database.
    let conn = duckdb::Connection::open(p.dir().join("fixture.duckdb")).expect("reopen");
    let leaked: i64 = conn
        .query_row(
            "SELECT count(*) FROM duckdb_tables() WHERE table_name LIKE 'rocky_ic_%'",
            [],
            |row| row.get(0),
        )
        .expect("count");
    assert_eq!(leaked, 0);
    // Release the file lock before the next `rocky plan` opens the database.
    drop(conn);

    // The table renderer carries the verdict and the caveat.
    let text = p.plan("table", &["--intent", "refactor"]);
    assert!(
        text.status.success(),
        "{}",
        String::from_utf8_lossy(&text.stderr)
    );
    let stdout = String::from_utf8_lossy(&text.stdout);
    assert!(stdout.contains("[MATCH] stg_orders"), "{stdout}");
    assert!(stdout.contains("not a proof for other inputs"), "{stdout}");
}

/// (2) A WHERE the data exercises: `mismatch`, `rows_differ`. Order 1
/// (9.99) and order 5 (NULL amount) drop out.
#[test]
fn exercised_where_is_rows_differ() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        &format!("{STG_ORDERS} WHERE amount > 10"),
        &[],
    );
    let check = p.check(&["--model", "stg_orders"]);
    let m = model(&check, "stg_orders");
    assert_eq!(m["verdict"], "mismatch");
    assert_eq!(m["reason"], "rows_differ");
    assert_eq!(m["rows_base"], 5);
    assert_eq!(m["rows_head"], 3);
    assert_eq!(m["rows_only_in_base"], 2);
    assert_eq!(m["rows_only_in_head"], 0);
}

/// (3) A CAST that changes a column type: `schema_differs`, both schemas.
#[test]
fn cast_type_change_is_schema_differs() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        "SELECT order_id, customer_id, CAST(amount AS BIGINT) AS amount, status \
         FROM raw__orders.orders",
        &[],
    );
    let check = p.check(&[]);
    let m = model(&check, "stg_orders");
    assert_eq!(m["verdict"], "mismatch");
    assert_eq!(m["reason"], "schema_differs");
    assert_eq!(m["schema_base"][2]["name"], "amount");
    assert_eq!(m["schema_head"][2]["type"], "BIGINT");
    assert_ne!(m["schema_base"][2]["type"], "BIGINT");
}

/// (4) `random()` is volatile: `unverified`, `nondeterministic`.
#[test]
fn random_is_nondeterministic() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        &format!("{STG_ORDERS} WHERE random() >= 0"),
        &[],
    );
    let m = model(&p.check(&[]), "stg_orders").clone();
    assert_eq!(m["verdict"], "unverified");
    assert_eq!(m["reason"], "nondeterministic");
}

/// (5) `current_timestamp` is volatile even though DuckDB pins it inside
/// one transaction, so both builds would agree.
#[test]
fn current_timestamp_is_nondeterministic() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        "SELECT order_id, customer_id, amount, status, current_timestamp AS loaded_at \
         FROM raw__orders.orders",
        &[],
    );
    let m = model(&p.check(&[]), "stg_orders").clone();
    assert_eq!(m["verdict"], "unverified");
    assert_eq!(m["reason"], "nondeterministic");
}

/// (6) A model that does not exist at the base ref: `no_base`.
#[test]
fn new_model_has_no_base() {
    let p = Project::new();
    p.write_model("new_model", "SELECT order_id FROM raw__orders.orders", &[]);
    let check = p.check(&[]);
    assert_eq!(
        check["models"].as_array().map(Vec::len),
        Some(1),
        "{check:#}"
    );
    let m = model(&check, "new_model");
    assert_eq!(m["verdict"], "unverified");
    assert_eq!(m["reason"], "no_base");
}

/// (7) Upstream and downstream both changed. Each is checked against the
/// materialized upstream table; the downstream names the changed upstream.
#[test]
fn changed_upstream_is_listed() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        "SELECT o.order_id, o.customer_id, o.amount, o.status FROM raw__orders.orders AS o",
        &[],
    );
    p.write_model(
        "fct_revenue",
        "SELECT s.customer_id, SUM(s.amount) AS total FROM stg_orders AS s GROUP BY s.customer_id",
        &["stg_orders"],
    );
    let check = p.check(&[]);
    let fct = model(&check, "fct_revenue");
    assert_eq!(fct["verdict"], "match", "{fct:#}");
    assert_eq!(fct["upstream_changed"], serde_json::json!(["stg_orders"]));
    let stg = model(&check, "stg_orders");
    assert!(stg.get("upstream_changed").is_none());
    assert_eq!(check["summary"]["match"], 2);
}

/// (8) Two rows with equal values at two keys change together. Row counts
/// match; a value-only XOR checksum cancels out. The multiset diff must not.
#[test]
fn equal_value_rows_changing_together_mismatch() {
    let p = Project::new();
    let base = "SELECT order_id, 'a' AS label FROM raw__orders.orders WHERE order_id IN (1, 2)";
    p.write_model("labels", base, &[]);
    run_git(p.dir(), &["add", "."]);
    run_git(p.dir(), &["commit", "-q", "-m", "labels"]);
    {
        let conn = duckdb::Connection::open(p.dir().join("fixture.duckdb")).expect("open");
        conn.execute_batch(&format!("CREATE TABLE main.labels AS {base}"))
            .expect("materialize labels");
    }
    p.write_model(
        "labels",
        "SELECT order_id, 'b' AS label FROM raw__orders.orders WHERE order_id IN (1, 2)",
        &[],
    );
    let m = model(&p.check(&[]), "labels").clone();
    assert_eq!(m["verdict"], "mismatch");
    assert_eq!(m["reason"], "rows_differ");
    assert_eq!(m["rows_base"], m["rows_head"]);
    assert_eq!(m["rows_only_in_base"], 2);
    assert_eq!(m["rows_only_in_head"], 2);
}

/// (9) Dropping the row with a NULL customer is a difference.
#[test]
fn dropped_null_row_mismatch() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        &format!("{STG_ORDERS} WHERE customer_id IS NOT NULL"),
        &[],
    );
    let m = model(&p.check(&[]), "stg_orders").clone();
    assert_eq!(m["verdict"], "mismatch");
    assert_eq!(m["rows_only_in_base"], 1);
    assert_eq!(m["rows_only_in_head"], 0);
}

/// (10) A mismatch keeps exit 0. The persisted plan records the intent and
/// carries no verdict.
#[test]
fn mismatch_is_report_only_and_the_verdict_is_not_persisted() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        &format!("{STG_ORDERS} WHERE amount > 10"),
        &[],
    );
    let out = p.plan("json", &["--intent", "refactor"]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "a mismatch must not change the exit code"
    );
    let plan: Value = serde_json::from_slice(&out.stdout).expect("JSON");
    assert_eq!(plan["intent_check"]["summary"]["mismatch"], 1);
    let plan_id = plan["plan_id"].as_str().expect("persisted plan id");

    let raw = fs::read_to_string(p.plan_file(plan_id)).expect("read plan file");
    let persisted: Value = serde_json::from_str(&raw).expect("plan file is JSON");
    assert_eq!(persisted["payload"]["intent"], "refactor");
    for absent in ["intent_check", "verdict", "rows_differ", "mismatch"] {
        assert!(
            !raw.contains(absent),
            "the persisted plan must not carry `{absent}`: {raw}"
        );
    }
}

/// (11) Without `--intent`: no `intent_check` in the output, no `intent`
/// key in the payload, and the plan_id is the one a plain plan gets. With
/// `--intent` the id moves, because the intent is part of the hashed payload.
#[test]
fn without_intent_the_plan_is_unchanged() {
    let p = Project::new();
    p.write_model(
        "stg_orders",
        &format!("{STG_ORDERS} WHERE amount > 10"),
        &[],
    );
    let plain = p.plan_json(&[]);
    assert!(plain.get("intent_check").is_none());
    let plain_id = plain["plan_id"].as_str().expect("plan id").to_string();
    let raw = fs::read_to_string(p.plan_file(&plain_id)).expect("read plan file");
    let persisted: Value = serde_json::from_str(&raw).expect("JSON");
    assert!(persisted["payload"].get("intent").is_none(), "{raw}");

    let again = p.plan_json(&[]);
    assert_eq!(
        again["plan_id"], plain["plan_id"],
        "the plain plan_id is stable"
    );

    let with_intent = p.plan_json(&["--intent", "refactor"]);
    assert_ne!(
        with_intent["plan_id"], plain["plan_id"],
        "the intent is part of the hashed payload"
    );
}

/// (13) The vocabulary is closed: clap refuses any other value.
#[test]
fn unknown_intent_is_refused_by_clap() {
    let p = Project::new();
    let out = p.plan("json", &["--intent", "bogus"]);
    assert_eq!(out.status.code(), Some(2), "clap usage errors exit 2");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("invalid value 'bogus'"), "{stderr}");
    assert!(stderr.contains("refactor"), "{stderr}");
}
