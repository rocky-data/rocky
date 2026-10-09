//! `rocky plan` cost preview on DuckDB: rebuild scope and estimates before a
//! run, marked as estimates, offline by default, and never part of `plan_id`.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn project(root: &Path) {
    let db = root.join("p.duckdb");
    fs::write(
        root.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
             [pipeline.ingest]\nstrategy = \"full_refresh\"\n\n\
             [pipeline.ingest.source.discovery]\nadapter = \"default\"\n\n\
             [pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\n\
             components = [\"source\"]\n\n\
             [pipeline.ingest.target]\ncatalog_template = \"p\"\n\
             schema_template = \"staging__{{source}}\"\n",
            db.display()
        ),
    )
    .unwrap();
    let conn = duckdb::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE SCHEMA raw__shop; CREATE TABLE raw__shop.orders AS SELECT 1 AS id, 10 AS amount;",
    )
    .unwrap();
    drop(conn);
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    let sidecar =
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"p\"\nschema = \"main\"\n";
    fs::write(
        models.join("stg_orders.sql"),
        "SELECT id, amount FROM raw__shop.orders\n",
    )
    .unwrap();
    fs::write(models.join("stg_orders.toml"), sidecar).unwrap();
    fs::write(
        models.join("report.sql"),
        "SELECT SUM(amount) AS total FROM stg_orders\n",
    )
    .unwrap();
    fs::write(models.join("report.toml"), sidecar).unwrap();
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
    assert!(
        out.status.success(),
        "plan must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("plan prints JSON")
}

#[test]
fn plan_reports_rebuild_scope_and_heuristic_estimates_offline() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());

    let plan = json(&rocky(tmp.path(), &["plan", "-o", "json"]));
    let preview = &plan["cost_preview"];
    assert_eq!(preview["is_estimate"], true);
    assert_eq!(preview["source"], "heuristic");
    assert_eq!(
        preview["models_to_rebuild"],
        plan["models"].as_array().unwrap().len()
    );
    assert_eq!(preview["models_to_rebuild"], 2);
    assert!(preview["estimated_bytes_scanned"].as_u64().unwrap() > 0);
    assert_eq!(preview["estimated_cost_usd"], 0.0, "DuckDB bills nothing");
    assert!(
        preview.get("cost_delta_usd").is_none(),
        "a heuristic is never compared with an observed cost"
    );
    for model in preview["models"].as_array().unwrap() {
        assert_eq!(model["source"], "heuristic");
        assert_eq!(model["confidence"], "low");
    }

    // The adapter mode falls back to the heuristic where DuckDB's EXPLAIN has
    // no figures, says so, and leaves the plan itself unchanged.
    let adapter = json(&rocky(
        tmp.path(),
        &["plan", "--cost-estimate", "adapter", "-o", "json"],
    ));
    assert_eq!(
        adapter["plan_id"], plan["plan_id"],
        "the preview never enters plan_id"
    );
    assert_eq!(adapter["models"], plan["models"]);
    let notes = adapter["cost_preview"]["notes"].as_array().unwrap();
    assert!(!notes.is_empty(), "{adapter:#}");
}

#[test]
fn cost_estimate_flag_is_refused_with_a_plan_subcommand() {
    let tmp = tempfile::tempdir().unwrap();
    project(tmp.path());
    let out = rocky(
        tmp.path(),
        &["plan", "--cost-estimate", "adapter", "promote", "b"],
    );
    assert!(!out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--cost-estimate"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
