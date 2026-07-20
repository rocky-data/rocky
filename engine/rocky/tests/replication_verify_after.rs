//! Governed replication plans must enforce policy `verify_after` requirements.

use std::fs;
use std::process::Command;

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

[pipeline.ingest.checks]
row_count = true

[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "allow"
verify_after = ["freshness"]
"#;

/// #1120 regression: `PlanKind::Replication` has its own apply arm. Once the
/// in-run target gate accepted `verify_after`, that arm could return success
/// without consuming the captured requirement. Require a check the configured
/// replication run does not emit: execution may land, but apply must halt and
/// report the absent check rather than falsely exit zero.
#[test]
fn replication_plan_fails_closed_when_verify_after_check_is_absent() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let config = dir.join("rocky.toml");

    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("open duckdb");
        conn.execute_batch(
            "CREATE SCHEMA raw__orders;
             CREATE TABLE raw__orders.orders AS SELECT 1 AS id;",
        )
        .expect("seed source");
    }
    fs::write(&config, ROCKY_TOML).expect("write config");

    let plan = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "--principal", "agent"])
        .arg("--config")
        .arg(&config)
        .arg("plan")
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run plan");
    assert!(
        plan.status.success(),
        "plan failed: {}",
        String::from_utf8_lossy(&plan.stderr)
    );
    let plan: serde_json::Value =
        serde_json::from_slice(&plan.stdout).expect("plan output should be JSON");
    assert_eq!(plan["plan_kind"], "replication");
    let plan_id = plan["plan_id"].as_str().expect("persisted plan ID");

    let apply = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "--principal", "agent"])
        .arg("--config")
        .arg(&config)
        .args(["apply", plan_id])
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("apply plan");

    assert!(
        !apply.status.success(),
        "an absent replication verify_after check must fail closed; stdout: {}",
        String::from_utf8_lossy(&apply.stdout)
    );
    let stderr = String::from_utf8_lossy(&apply.stderr);
    assert!(
        stderr.contains("verify_after gate FAILED")
            && stderr.contains("freshness")
            && stderr.contains("absent"),
        "expected the absent verify_after check, got: {stderr}"
    );

    // Post-verification is halt-only today: prove the mutation landed before
    // the policy failure so the test exercises the intended post-run seam.
    let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
    let materialized: i64 = conn
        .query_row(
            "SELECT count(*) FROM information_schema.tables
             WHERE table_schema = 'staging__orders' AND table_name = 'orders'",
            [],
            |row| row.get(0),
        )
        .expect("query target table");
    assert_eq!(
        materialized, 1,
        "replication should reach the post-run gate"
    );
}
