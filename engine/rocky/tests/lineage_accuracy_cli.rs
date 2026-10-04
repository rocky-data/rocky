//! End-to-end coverage, through the `rocky` binary, for column-lineage
//! accuracy and coherent CI snapshots:
//!
//! - `rocky lineage-diff` classifies every consumer of a removed column;
//! - `rocky lineage <m> --column` labels row-selection edges;
//! - `rocky ci-diff` compiles the HEAD commit by default and the working tree
//!   only under `--working-tree`, and reports which mode ran.
//!
//! DuckDB-only config, no credentials. Each test drives a scratch git repo
//! and runs the binary with that repo as its working directory.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn git(dir: &Path, args: &[&str]) {
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

fn write_model(models: &Path, name: &str, sql: &str) {
    fs::write(models.join(format!("{name}.sql")), sql).expect("write sql");
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .expect("write sidecar");
}

fn remove_model(models: &Path, name: &str) {
    fs::remove_file(models.join(format!("{name}.sql"))).expect("remove sql");
    fs::remove_file(models.join(format!("{name}.toml"))).expect("remove sidecar");
}

/// A git repo with `rocky.toml` and an empty `models/` dir.
fn init_repo(dir: &Path) -> std::path::PathBuf {
    let models = dir.join("models");
    fs::create_dir_all(&models).expect("models dir");
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \":memory:\"\n",
    )
    .expect("rocky.toml");
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["config", "user.email", "tester@example.com"]);
    git(dir, &["config", "user.name", "Tester"]);
    git(dir, &["config", "commit.gpgsign", "false"]);
    models
}

fn commit_all(dir: &Path, message: &str) {
    git(dir, &["add", "-A"]);
    git(dir, &["commit", "-q", "-m", message]);
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn json(output: &Output) -> serde_json::Value {
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "rocky failed\nstdout: {stdout}\nstderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_str(stdout.trim()).unwrap_or_else(|e| {
        panic!(
            "stdout is not JSON: {e}\nstdout: {stdout}\nstderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    })
}

#[test]
fn lineage_diff_classifies_consumers_of_a_renamed_column() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = init_repo(dir);
    write_model(
        &models,
        "stg_orders",
        "SELECT order_id, customer_id, amount FROM raw.orders",
    );
    write_model(
        &models,
        "fct_repaired",
        "SELECT order_id, amount FROM stg_orders",
    );
    write_model(
        &models,
        "fct_deleted",
        "SELECT order_id, amount AS a FROM stg_orders",
    );
    write_model(
        &models,
        "fct_broken",
        "SELECT order_id, amount FROM stg_orders",
    );
    // Valid control: reads `amount` from a different relation.
    write_model(
        &models,
        "fct_control",
        "SELECT s.order_id, r.amount AS raw_amount FROM stg_orders s JOIN raw.orders r ON s.order_id = r.order_id",
    );
    commit_all(dir, "base");

    write_model(
        &models,
        "stg_orders",
        "SELECT order_id, customer_id, amount AS order_amount FROM raw.orders",
    );
    write_model(
        &models,
        "fct_repaired",
        "SELECT order_id, order_amount AS amount FROM stg_orders",
    );
    remove_model(&models, "fct_deleted");
    commit_all(dir, "rename amount -> order_amount");

    let out = json(&rocky(dir, &["lineage-diff", "HEAD~1", "-o", "json"]));
    assert_eq!(out["mode"], "head");
    let stg = out["results"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["model_name"] == "stg_orders")
        .expect("stg_orders row");
    let amount = stg["column_changes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["column_name"] == "amount")
        .expect("amount change");
    assert_eq!(amount["change_type"], "removed");
    let impacts = amount["consumer_impact"]
        .as_array()
        .expect("consumer_impact");
    let status_of = |model: &str| {
        impacts
            .iter()
            .find(|i| i["model"] == model)
            .map(|i| i["status"].as_str().unwrap().to_string())
    };
    assert_eq!(
        status_of("fct_repaired").as_deref(),
        Some("repaired"),
        "{impacts:#?}"
    );
    assert_eq!(
        status_of("fct_deleted").as_deref(),
        Some("deleted"),
        "{impacts:#?}"
    );
    assert_eq!(
        status_of("fct_broken").as_deref(),
        Some("newly_broken"),
        "{impacts:#?}"
    );
    assert_eq!(status_of("fct_control"), None, "{impacts:#?}");

    let markdown = out["markdown"].as_str().unwrap();
    assert!(
        markdown.contains("Consumers of removed columns"),
        "{markdown}"
    );
    assert!(markdown.contains("**newly broken**"), "{markdown}");
}

#[test]
fn lineage_column_labels_value_and_row_selection_edges() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = init_repo(dir);
    write_model(
        &models,
        "stg",
        "SELECT order_id, customer_id, amount, status FROM raw.orders",
    );
    write_model(
        &models,
        "fct",
        "SELECT customer_id, SUM(amount) AS total FROM stg WHERE status = 'paid' GROUP BY customer_id",
    );

    let out = json(&rocky(dir, &["lineage", "fct.total", "-o", "json"]));
    let trace = out["trace"].as_array().unwrap();
    assert!(
        trace.iter().all(|e| e["source"]["column"] != "status"),
        "a filter column is not value lineage: {trace:#?}"
    );
    let rs = out["row_selection"].as_array().expect("row_selection");
    let has = |kind: &str, column: &str| {
        rs.iter().any(|e| {
            e["kind"] == kind && e["source"]["model"] == "stg" && e["source"]["column"] == column
        })
    };
    assert!(has("filter", "status"), "{rs:#?}");
    assert!(has("group_by", "customer_id"), "{rs:#?}");

    let text = rocky(dir, &["lineage", "fct.total", "-o", "table"]);
    assert!(text.status.success());
    let text = String::from_utf8_lossy(&text.stdout);
    assert!(text.contains("Value derivation:"), "{text}");
    assert!(text.contains("Row selection"), "{text}");
    assert!(text.contains("[filter] stg.status -> fct"), "{text}");
}

#[test]
fn ci_diff_default_ignores_dirty_tree_and_working_tree_includes_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    let models = init_repo(dir);
    write_model(&models, "orders", "SELECT 1 AS id");
    commit_all(dir, "base");
    write_model(&models, "orders", "SELECT 1 AS id, 2 AS committed");
    commit_all(dir, "committed edit");

    // Uncommitted edit plus an untracked model.
    write_model(
        &models,
        "orders",
        "SELECT 1 AS id, 2 AS committed, 3 AS dirty",
    );
    write_model(&models, "fresh", "SELECT 1 AS id");

    let columns_of = |out: &serde_json::Value, model: &str| -> Vec<String> {
        out["models"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| m["model_name"] == model)
            .flat_map(|m| m["column_changes"].as_array().unwrap().clone())
            .map(|c| c["column_name"].as_str().unwrap().to_string())
            .collect()
    };
    let models_of = |out: &serde_json::Value| -> Vec<String> {
        out["models"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["model_name"].as_str().unwrap().to_string())
            .collect()
    };

    let head = json(&rocky(dir, &["ci-diff", "HEAD~1", "-o", "json"]));
    assert_eq!(head["mode"], "head");
    assert_eq!(head["head_ref"], "HEAD");
    assert!(head["base_commit"].is_string());
    assert_eq!(columns_of(&head, "orders"), vec!["committed".to_string()]);
    assert_eq!(models_of(&head), vec!["orders".to_string()]);

    let wt = json(&rocky(
        dir,
        &["ci-diff", "HEAD~1", "--working-tree", "-o", "json"],
    ));
    assert_eq!(wt["mode"], "working_tree");
    assert_eq!(wt["head_ref"], "WORKTREE");
    let mut cols = columns_of(&wt, "orders");
    cols.sort();
    assert_eq!(cols, vec!["committed".to_string(), "dirty".to_string()]);
    assert!(models_of(&wt).contains(&"fresh".to_string()), "{wt:#?}");
}
