//! `type = "manual"` discovery through the real binary (#1994).
//!
//! Before the fix, `rocky validate` accepted a manual adapter while
//! `rocky plan` failed with `no discovery adapter named '<name>'`: the
//! registry arm registered nothing. Now the two agree in both directions:
//! a manual adapter that lists schemas validates AND plans the listed
//! tables; one that lists none is refused by both.

use std::path::Path;
use std::process::{Command, Output};

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(dir)
        .args(["-c", "rocky.toml", "--state-path", "state.redb"])
        .args(args)
        .output()
        .expect("spawn rocky")
}

/// A replication pipeline whose data moves through DuckDB and whose source
/// tables come from a manual discovery adapter. `{schemas}` is spliced in
/// after the manual adapter block.
fn project(schemas: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let toml = format!(
        r#"
[adapter.warehouse]
type = "duckdb"
kind = "data"
path = "warehouse.duckdb"

[adapter.local_discovery]
type = "manual"
kind = "discovery"
{schemas}

[pipeline.poc]
type = "replication"
strategy = "full_refresh"

[pipeline.poc.source]
adapter = "warehouse"

[pipeline.poc.source.discovery]
adapter = "local_discovery"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
adapter = "warehouse"
catalog_template = "warehouse"
schema_template = "staging__{{source}}"
"#
    );
    std::fs::write(dir.path().join("rocky.toml"), toml).expect("write rocky.toml");
    dir
}

fn json(out: &Output, what: &str) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&out.stdout);
    serde_json::from_str(&stdout).unwrap_or_else(|e| {
        panic!(
            "`{what}` did not print JSON ({e}); exit {:?}\nstdout: {stdout}\nstderr: {}",
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        )
    })
}

#[test]
fn a_manual_adapter_that_validates_also_plans_its_listed_tables() {
    let dir = project(
        r#"
[[adapter.local_discovery.schemas]]
name = "raw__orders"
tables = ["orders", "order_items"]

[[adapter.local_discovery.schemas]]
name = "raw__customers"
tables = ["customers"]
"#,
    );

    let validate = rocky(dir.path(), &["-o", "json", "validate"]);
    assert!(
        validate.status.success(),
        "validate: {}",
        String::from_utf8_lossy(&validate.stdout)
    );
    assert_eq!(json(&validate, "validate")["valid"], true);

    let plan = rocky(
        dir.path(),
        &["-o", "json", "plan", "--filter", "source=orders"],
    );
    assert!(
        plan.status.success(),
        "plan must succeed where validate did\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&plan.stdout),
        String::from_utf8_lossy(&plan.stderr)
    );
    let body = json(&plan, "plan");
    let planned: Vec<(&str, &str)> = body["statements"]
        .as_array()
        .expect("statements")
        .iter()
        .map(|st| (st["target"].as_str().unwrap(), st["sql"].as_str().unwrap()))
        .collect();
    assert_eq!(
        planned.iter().map(|(t, _)| *t).collect::<Vec<_>>(),
        ["staging__orders.orders", "staging__orders.order_items"],
        "the listed raw__orders tables, in config order; raw__customers filtered out"
    );
    for ((_, sql), table) in planned.iter().zip(["orders", "order_items"]) {
        assert!(
            sql.ends_with(&format!("FROM raw__orders.{table}")),
            "{table}: {sql}"
        );
    }
}

#[test]
fn a_manual_adapter_with_no_schemas_is_refused_by_validate_and_plan() {
    let dir = project("");

    let validate = rocky(dir.path(), &["-o", "json", "validate"]);
    assert!(!validate.status.success(), "validate must refuse");
    let body = json(&validate, "validate");
    assert_eq!(body["valid"], false);
    let v057: Vec<_> = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .filter(|m| m["code"] == "V057" && m["severity"] == "error")
        .collect();
    assert_eq!(v057.len(), 1, "one V057 error: {body:#}");
    assert_eq!(v057[0]["field"], "adapter.local_discovery.schemas");

    let plan = rocky(dir.path(), &["plan"]);
    assert!(!plan.status.success(), "plan must refuse too");
    let stderr = String::from_utf8_lossy(&plan.stderr);
    assert!(
        stderr.contains("lists no schemas"),
        "plan must name the same cause: {stderr}"
    );
    assert!(
        !stderr.contains("no discovery adapter named"),
        "the pre-#1994 failure must be gone: {stderr}"
    );
}

/// `rocky discover` returns the listed schemas that match the prefix, with
/// their tables, and `source_type = "manual"`.
#[test]
fn discover_returns_the_listed_schemas() {
    let dir = project(
        r#"
[[adapter.local_discovery.schemas]]
name = "raw__orders"
tables = ["orders", "order_items"]

[[adapter.local_discovery.schemas]]
name = "other__x"
tables = ["x"]
"#,
    );
    let out = rocky(dir.path(), &["-o", "json", "discover"]);
    assert!(
        out.status.success(),
        "discover: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let body = json(&out, "discover");
    let sources = body["sources"].as_array().expect("sources");
    assert_eq!(sources.len(), 1, "{body:#}");
    assert_eq!(sources[0]["source_type"], "manual");
    let tables: Vec<&str> = sources[0]["tables"]
        .as_array()
        .expect("tables")
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(tables, ["orders", "order_items"], "{body:#}");
}

/// A listed schema the pattern cannot parse: validate warns V058 and plan
/// plans nothing. The two agree that nothing would be built.
#[test]
fn a_schema_the_pattern_cannot_parse_warns_and_plans_nothing() {
    let dir = project(
        r#"
[[adapter.local_discovery.schemas]]
name = "orders"
tables = ["orders"]
"#,
    );
    let validate = rocky(dir.path(), &["-o", "json", "validate"]);
    let body = json(&validate, "validate");
    let v058 = body["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .filter(|m| m["code"] == "V058" && m["severity"] == "warn")
        .count();
    assert_eq!(v058, 1, "{body:#}");

    let plan = rocky(dir.path(), &["-o", "json", "plan"]);
    assert!(plan.status.success());
    let body = json(&plan, "plan");
    assert_eq!(
        body["statements"].as_array().map(Vec::len),
        Some(0),
        "{body:#}"
    );
}
