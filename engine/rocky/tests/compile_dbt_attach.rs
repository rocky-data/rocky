//! `rocky compile --dbt-project <DIR>` — experimental dbt attach mode.
//!
//! Each test builds a small dbt project (a `target/manifest.json`, optionally
//! a `run_results.json`) in a temp directory and drives the real binary with
//! the dbt project as its working directory, so any stray write would land
//! inside the tree the tests hash.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const V12: &str = "https://schemas.getdbt.com/dbt/manifest/v12.json";
const INVOCATION: &str = "11111111-2222-3333-4444-555555555555";

fn write_project_files(dbt_dir: &Path) {
    fs::create_dir_all(dbt_dir.join("target")).expect("create target");
    fs::create_dir_all(dbt_dir.join("models")).expect("create models");
    fs::write(
        dbt_dir.join("dbt_project.yml"),
        "name: attach_probe\nversion: '1.0'\nprofile: attach_probe\n",
    )
    .expect("write dbt_project.yml");
    fs::write(
        dbt_dir.join("profiles.yml"),
        "attach_probe:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: probe.duckdb\n      database: probe\n      schema: main\n",
    )
    .expect("write profiles.yml");
}

fn model_node(
    name: &str,
    materialized: &str,
    compiled: Option<&str>,
    raw: &str,
) -> serde_json::Value {
    let mut node = serde_json::json!({
        "unique_id": format!("model.attach_probe.{name}"),
        "name": name,
        "resource_type": "model",
        "database": "probe",
        "schema": "main",
        "relation_name": format!("\"probe\".\"main\".\"{name}\""),
        "raw_code": raw,
        "depends_on": { "nodes": [], "macros": [] },
        "config": { "materialized": materialized, "full_refresh": null },
        "columns": {},
        "tags": []
    });
    if let Some(sql) = compiled {
        node["compiled_code"] = serde_json::Value::String(sql.to_string());
    }
    node
}

fn write_manifest(dbt_dir: &Path, schema_version: &str, nodes: Vec<serde_json::Value>) {
    let nodes: serde_json::Map<String, serde_json::Value> = nodes
        .into_iter()
        .map(|n| (n["unique_id"].as_str().unwrap().to_string(), n))
        .collect();
    let manifest = serde_json::json!({
        "metadata": {
            "dbt_schema_version": schema_version,
            "dbt_version": "1.10.0",
            "project_name": "attach_probe",
            "invocation_id": INVOCATION
        },
        "nodes": nodes,
        "sources": {}
    });
    fs::write(
        dbt_dir.join("target/manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .expect("write manifest");
}

fn write_full_refresh_run_results(dbt_dir: &Path, unique_ids: &[&str]) {
    let results: Vec<_> = unique_ids
        .iter()
        .map(|id| serde_json::json!({ "unique_id": id, "status": "success" }))
        .collect();
    let rr = serde_json::json!({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "invocation_id": INVOCATION
        },
        "args": { "which": "compile", "full_refresh": true },
        "results": results
    });
    fs::write(
        dbt_dir.join("target/run_results.json"),
        serde_json::to_vec_pretty(&rr).unwrap(),
    )
    .expect("write run_results");
}

/// A dbt project with one table and one view. `fct_orders` reads
/// `stg_orders` through dbt's compiled relation string, which the importer
/// rewrites back to a bare Rocky model name.
fn tables_and_views_project(dbt_dir: &Path) {
    write_project_files(dbt_dir);
    let mut fct = model_node(
        "fct_orders",
        "table",
        Some("select id, amount * 2 as doubled\nfrom \"probe\".\"main\".\"stg_orders\""),
        "select id, amount * 2 as doubled from {{ ref('stg_orders') }}",
    );
    fct["depends_on"]["nodes"] = serde_json::json!(["model.attach_probe.stg_orders"]);
    write_manifest(
        dbt_dir,
        V12,
        vec![
            model_node(
                "stg_orders",
                "view",
                Some("select 1 as id, 10 as amount"),
                "select 1 as id, 10 as amount",
            ),
            fct,
        ],
    );
}

fn rocky_attach(dbt_dir: &Path, json: bool) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    if json {
        cmd.args(["--output", "json"]);
    }
    cmd.args(["compile", "--dbt-project"])
        .arg(dbt_dir)
        .current_dir(dbt_dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run rocky compile --dbt-project")
}

/// Every file and directory under `root`, with file bytes, in stable order.
fn snapshot(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
    fn walk(root: &Path, dir: &Path, out: &mut BTreeMap<PathBuf, Option<Vec<u8>>>) {
        for entry in fs::read_dir(dir).expect("read_dir") {
            let path = entry.expect("dir entry").path();
            let rel = path.strip_prefix(root).unwrap().to_path_buf();
            if path.is_dir() {
                out.insert(rel, None);
                walk(root, &path, out);
            } else {
                out.insert(rel, Some(fs::read(&path).expect("read file")));
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(root, root, &mut out);
    out
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn attach_compiles_v12_tables_and_views() {
    let tmp = tempfile::tempdir().unwrap();
    let dbt_dir = tmp.path().join("dbt");
    tables_and_views_project(&dbt_dir);

    let output = rocky_attach(&dbt_dir, true);
    assert!(
        output.status.success(),
        "attach compile failed: {}",
        stderr(&output)
    );

    let result: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("stdout is the CompileOutput JSON");
    assert_eq!(result["command"], "compile");
    assert_eq!(result["models"], 2);
    assert_eq!(result["has_errors"], false);
    assert_eq!(
        result["execution_layers"], 2,
        "fct_orders depends on stg_orders"
    );
    let names: Vec<&str> = result["models_detail"]
        .as_array()
        .expect("models_detail")
        .iter()
        .filter_map(|m| m["name"].as_str())
        .collect();
    assert!(names.contains(&"stg_orders"), "{names:?}");
    assert!(names.contains(&"fct_orders"), "{names:?}");

    let err = stderr(&output);
    assert!(err.contains("experimental dbt attach"), "{err}");
    assert!(err.contains("manifest schema v12"), "{err}");
}

#[test]
fn attach_writes_nothing_into_the_dbt_project() {
    let tmp = tempfile::tempdir().unwrap();
    let dbt_dir = tmp.path().join("dbt");
    tables_and_views_project(&dbt_dir);
    // A seed exercises the emit path that copies from the dbt tree.
    fs::create_dir_all(dbt_dir.join("seeds")).unwrap();
    fs::write(dbt_dir.join("seeds/regions.csv"), "id,name\n1,north\n").unwrap();

    let before = snapshot(&dbt_dir);
    for json in [true, false] {
        let output = rocky_attach(&dbt_dir, json);
        assert!(
            output.status.success(),
            "attach compile failed: {}",
            stderr(&output)
        );
    }
    let after = snapshot(&dbt_dir);
    assert_eq!(
        before.keys().collect::<Vec<_>>(),
        after.keys().collect::<Vec<_>>(),
        "attach mode created or removed paths under the dbt project"
    );
    assert!(
        before == after,
        "attach mode changed file bytes under the dbt project"
    );
}

#[test]
fn attach_refuses_incremental_without_full_refresh_run_results() {
    let tmp = tempfile::tempdir().unwrap();
    let dbt_dir = tmp.path().join("dbt");
    write_project_files(&dbt_dir);
    let mut inc = model_node(
        "orders_inc",
        "incremental",
        Some("select 1 as id"),
        "{{ config(materialized='incremental', unique_key='id') }}\nselect 1 as id",
    );
    inc["config"]["unique_key"] = serde_json::json!("id");
    write_manifest(&dbt_dir, V12, vec![inc]);
    let before = snapshot(&dbt_dir);

    let output = rocky_attach(&dbt_dir, true);
    assert!(
        !output.status.success(),
        "incremental model must be refused"
    );
    assert!(output.stdout.is_empty(), "no CompileOutput on refusal");
    let err = stderr(&output);
    assert!(err.contains("model `orders_inc`"), "{err}");
    assert!(
        err.contains("incremental model without full-refresh compile evidence"),
        "{err}"
    );
    assert!(err.contains("dbt compile --full-refresh"), "{err}");
    assert!(
        before == snapshot(&dbt_dir),
        "refusal path wrote into the dbt project"
    );

    // Positive control: the matching full-refresh run_results.json is the fix.
    write_full_refresh_run_results(&dbt_dir, &["model.attach_probe.orders_inc"]);
    let output = rocky_attach(&dbt_dir, true);
    assert!(
        output.status.success(),
        "with full-refresh evidence the model compiles: {}",
        stderr(&output)
    );
}

#[test]
fn attach_refuses_unknown_manifest_schema_version_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dbt_dir = tmp.path().join("dbt");
    tables_and_views_project(&dbt_dir);
    // Same nodes, future schema version.
    let manifest_path = dbt_dir.join("target/manifest.json");
    let text = fs::read_to_string(&manifest_path).unwrap();
    fs::write(
        &manifest_path,
        text.replace(V12, "https://schemas.getdbt.com/dbt/manifest/v99.json"),
    )
    .unwrap();

    let output = rocky_attach(&dbt_dir, true);
    assert!(
        !output.status.success(),
        "unknown schema version must be refused"
    );
    assert!(output.stdout.is_empty(), "no CompileOutput on refusal");
    let err = stderr(&output);
    assert!(
        err.contains("unsupported dbt manifest schema version"),
        "{err}"
    );
    assert!(err.contains("manifest/v99.json"), "{err}");
    assert!(err.contains("v12"), "names the supported version: {err}");
}

#[test]
fn attach_refuses_jinja_control_flow_with_the_import_dbt_reason() {
    let tmp = tempfile::tempdir().unwrap();
    let dbt_dir = tmp.path().join("dbt");
    write_project_files(&dbt_dir);
    // No compiled_code: the importer would fall back to raw_code, which holds
    // Jinja control flow it cannot evaluate.
    write_manifest(
        &dbt_dir,
        V12,
        vec![model_node(
            "env_branched",
            "table",
            None,
            "select * from raw.orders\n{% if target.name == 'prod' %}\nwhere id > 100\n{% endif %}",
        )],
    );

    let attach = rocky_attach(&dbt_dir, true);
    assert!(
        !attach.status.success(),
        "Jinja control flow must be refused"
    );
    let err = stderr(&attach);
    assert!(err.contains("model `env_branched`"), "{err}");
    assert!(err.contains("Jinja control flow"), "{err}");

    // Same manifest through `rocky import-dbt`: the reason must be identical.
    let out_dir = tmp.path().join("imported");
    let import = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json", "import-dbt", "--dbt-project"])
        .arg(&dbt_dir)
        .arg("--output-dir")
        .arg(&out_dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("run import-dbt");
    assert!(import.status.success(), "import-dbt: {}", stderr(&import));
    let result: serde_json::Value = serde_json::from_slice(&import.stdout).unwrap();
    assert_eq!(result["failed_details"][0]["name"], "env_branched");
    let import_reason = result["failed_details"][0]["reason"].as_str().unwrap();
    assert!(
        err.contains(&format!("): {import_reason}")),
        "attach reason must equal import-dbt's.\nimport: {import_reason}\nattach: {err}"
    );
}
