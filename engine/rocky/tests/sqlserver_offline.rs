//! `rocky compile`, `rocky emit-sql` and `rocky validate` against a project
//! whose target adapter is `type = "sqlserver"` — no warehouse, no
//! credentials (the compile reads source schemas from `data/seed.sql`
//! through DuckDB).
//!
//! Proves the user-visible SQL Server behaviour that needs no server:
//!
//! - operand checks follow SQL Server's rules: `SUM(VARCHAR)` is `E042`, a
//!   `BIGINT` = `VARCHAR` column join is `W043` (exit 0);
//! - valid controls stay clean (`10::BIGINT = '10'::VARCHAR`, a CTE model,
//!   a lateral alias);
//! - a function is refused with `E051`, a snapshot model with `E049`;
//! - `emit-sql` renders T-SQL: bracketed names, the staging + `sp_rename`
//!   full refresh, `MERGE … ;` with the model's CTE lifted to the head;
//! - `validate` reports a bad adapter block (two auth methods) as `V011`.

use std::fs;
use std::path::Path;
use std::process::Command;

const SEED: &str = "CREATE SCHEMA raw;\n\
    CREATE TABLE raw.orders (order_id BIGINT, customer_id BIGINT, amount DOUBLE, status VARCHAR, order_date DATE);\n\
    CREATE TABLE raw.customers (customer_id BIGINT, customer_name VARCHAR, email VARCHAR);\n";

const CONFIG: &str = "[adapter.wh]\ntype = \"sqlserver\"\nhost = \"localhost\"\n\
    database = \"analytics\"\nusername = \"rocky\"\npassword = \"x\"\n\n\
    [pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
    [pipeline.t.target]\nadapter = \"wh\"\n";

fn project(root: &Path, models: &[(&str, &str, &str)]) {
    fs::write(root.join("rocky.toml"), CONFIG).unwrap();
    fs::create_dir_all(root.join("data")).unwrap();
    fs::write(root.join("data/seed.sql"), SEED).unwrap();
    let dir = root.join("models");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("_defaults.toml"),
        "[target]\ncatalog = \"analytics\"\nschema = \"marts\"\n",
    )
    .unwrap();
    for (name, sql, toml) in models {
        fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        fs::write(dir.join(format!("{name}.toml")), toml).unwrap();
    }
}

fn rocky(root: &Path, args: &[&str]) -> (i32, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(args)
        .env("RUST_LOG", "error")
        .env("RUST_BACKTRACE", "0")
        .output()
        .expect("spawn rocky");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8(out.stdout).expect("utf8"),
    )
}

/// The JSON document on stdout (the last line that parses; `compile` may
/// print a summary line before it).
fn json(stdout: &str) -> serde_json::Value {
    serde_json::from_str(stdout.trim())
        .or_else(|_| {
            stdout
                .lines()
                .rev()
                .find_map(|l| serde_json::from_str(l).ok())
                .ok_or(())
        })
        .unwrap_or_else(|()| panic!("no JSON on stdout:\n{stdout}"))
}

fn diagnostics(parsed: &serde_json::Value) -> Vec<(String, String, String)> {
    parsed["diagnostics"]
        .as_array()
        .expect("diagnostics")
        .iter()
        .map(|d| {
            (
                d["code"].as_str().unwrap_or_default().to_string(),
                d["model"].as_str().unwrap_or_default().to_string(),
                d["severity"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

#[test]
fn operand_checks_follow_sql_server_rules() {
    let tmp = tempfile::tempdir().unwrap();
    project(
        tmp.path(),
        &[(
            "bad_agg",
            "SELECT customer_id, SUM(customer_name) AS s FROM raw.customers GROUP BY customer_id",
            "",
        )],
    );
    let (code, out) = rocky(tmp.path(), &["compile", "--with-seed", "--output", "json"]);
    let parsed = json(&out);
    let diags = diagnostics(&parsed);
    assert!(
        diags.contains(&("E042".into(), "bad_agg".into(), "Error".into())),
        "{diags:?}"
    );
    assert_ne!(code, 0, "an E042 fails compile");

    let tmp = tempfile::tempdir().unwrap();
    project(
        tmp.path(),
        &[(
            "bad_join",
            "SELECT o.order_id, c.customer_name FROM raw.orders o JOIN raw.customers c \
             ON o.customer_id = c.customer_name",
            "",
        )],
    );
    let (code, out) = rocky(tmp.path(), &["compile", "--with-seed", "--output", "json"]);
    let diags = diagnostics(&json(&out));
    assert!(
        diags.contains(&("W043".into(), "bad_join".into(), "Warning".into())),
        "{diags:?}"
    );
    assert_eq!(code, 0, "W043 alone keeps compile green: {diags:?}");
}

#[test]
fn valid_controls_compile_clean() {
    let tmp = tempfile::tempdir().unwrap();
    project(
        tmp.path(),
        &[
            (
                "stg_orders",
                "SELECT order_id, customer_id, amount FROM raw.orders",
                "",
            ),
            (
                "fct_revenue",
                "WITH o AS (SELECT customer_id, amount FROM stg_orders)\n\
                 SELECT c.customer_name, SUM(o.amount) AS total FROM o \
                 JOIN raw.customers c ON o.customer_id = c.customer_id GROUP BY c.customer_name",
                "depends_on = [\"stg_orders\"]\n",
            ),
            ("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value", ""),
            (
                "v3",
                "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
                "",
            ),
        ],
    );
    let (code, out) = rocky(tmp.path(), &["compile", "--with-seed", "--output", "json"]);
    let parsed = json(&out);
    let bad: Vec<_> = diagnostics(&parsed)
        .into_iter()
        .filter(|(_, _, sev)| sev == "Error" || sev == "Warning")
        .collect();
    assert!(bad.is_empty(), "{bad:?}");
    assert_eq!(code, 0);
}

#[test]
fn functions_and_snapshots_are_refused_at_compile() {
    let tmp = tempfile::tempdir().unwrap();
    project(
        tmp.path(),
        &[
            ("uf", "SELECT dbl(1.0) AS a2", ""),
            (
                "snap",
                "SELECT order_id, CAST(order_date AS TIMESTAMP) AS updated_at FROM raw.orders",
                "[strategy]\ntype = \"snapshot\"\nunique_key = \"order_id\"\n\
                 strategy = \"timestamp\"\nupdated_at = \"updated_at\"\n",
            ),
        ],
    );
    fs::create_dir_all(tmp.path().join("functions")).unwrap();
    fs::write(
        tmp.path().join("functions/dbl.toml"),
        "returns = \"FLOAT\"\n\n[[arguments]]\nname = \"x\"\ntype = \"FLOAT\"\n",
    )
    .unwrap();
    fs::write(tmp.path().join("functions/dbl.sql"), "x * 2\n").unwrap();
    let (code, out) = rocky(tmp.path(), &["compile", "--with-seed", "--output", "json"]);
    let parsed = json(&out);
    let diags = diagnostics(&parsed);
    assert!(
        diags.iter().any(|(c, m, _)| c == "E051" && m == "dbl"),
        "{diags:?}"
    );
    assert!(
        diags.iter().any(|(c, m, _)| c == "E049" && m == "snap"),
        "{diags:?}"
    );
    let e051 = parsed["diagnostics"]
        .as_array()
        .unwrap()
        .iter()
        .find(|d| d["code"] == "E051")
        .unwrap();
    assert!(
        e051["message"].as_str().unwrap().contains("sqlserver"),
        "{e051}"
    );
    assert_ne!(code, 0);
}

#[test]
fn emit_sql_renders_tsql() {
    let tmp = tempfile::tempdir().unwrap();
    project(
        tmp.path(),
        &[
            (
                "stg_orders",
                "SELECT order_id, customer_id, amount FROM raw.orders",
                "",
            ),
            (
                "dim_customers",
                "WITH c AS (SELECT customer_id, customer_name FROM raw.customers)\n\
                 SELECT customer_id, customer_name FROM c",
                "[strategy]\ntype = \"merge\"\nunique_key = [\"customer_id\"]\n\
                 update_columns = [\"customer_name\"]\n",
            ),
            (
                "v_orders",
                "SELECT order_id FROM raw.orders",
                "[strategy]\ntype = \"view\"\n",
            ),
        ],
    );
    let (code, out) = rocky(tmp.path(), &["emit-sql", "--output", "table"]);
    assert_eq!(code, 0, "{out}");
    assert!(
        out.contains(
            "SELECT * INTO [analytics].[marts].[stg_orders__rocky_new] FROM (\n\
             SELECT order_id, customer_id, amount FROM raw.orders\n) AS rocky_src\n\
             UNION ALL\n"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "EXEC [analytics].sys.sp_rename N'[marts].[stg_orders__rocky_new]', N'stg_orders';"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "WITH c AS (SELECT customer_id, customer_name FROM raw.customers\n)\n\
             MERGE INTO [analytics].[marts].[dim_customers] WITH (HOLDLOCK) AS rocky_t"
        ),
        "{out}"
    );
    assert!(
        out.contains(
            "WHEN NOT MATCHED BY TARGET THEN INSERT ([customer_name], [customer_id]) VALUES \
             (rocky_s.[customer_name], rocky_s.[customer_id]);"
        ),
        "{out}"
    );
    assert!(
        out.contains("CREATE OR ALTER VIEW [marts].[v_orders] AS"),
        "{out}"
    );
    assert!(!out.contains("LIMIT"), "{out}");
}

#[test]
fn validate_reports_a_bad_sqlserver_block() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path(), &[]);
    let (code, out) = rocky(tmp.path(), &["validate", "--output", "json"]);
    let parsed = json(&out);
    assert_eq!(code, 0, "{parsed}");
    assert!(
        parsed["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["code"] == "V010" && m["message"] == "adapter.wh: sqlserver"),
        "{parsed}"
    );

    fs::write(
        tmp.path().join("rocky.toml"),
        CONFIG.replace(
            "password = \"x\"",
            "password = \"x\"\noauth_token = \"tok\"",
        ),
    )
    .unwrap();
    let (_, out) = rocky(tmp.path(), &["validate", "--output", "json"]);
    let parsed = json(&out);
    let v011 = parsed["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["code"] == "V011")
        .unwrap_or_else(|| panic!("{parsed}"));
    assert!(
        v011["message"]
            .as_str()
            .unwrap()
            .contains("several auth methods"),
        "{v011}"
    );
    // A warning, like the other adapters' V011: `rocky run` fails with the
    // same message.
    assert_eq!(v011["severity"], serde_json::json!("warn"));
}
