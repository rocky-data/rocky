use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn model(root: &Path, name: &str, sql: &str, strategy: &str) {
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(models.join(format!("{name}.sql")), sql).unwrap();
    fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n[strategy]\ntype = \"{strategy}\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args(["--config", "rocky.toml", "--output", "json"])
        .args(args)
        .env("RUST_LOG", "error")
        .output()
        .unwrap()
}

fn replication_project(root: &Path) {
    fs::write(
        root.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n\
         [pipeline.ingest]\nstrategy = \"full_refresh\"\n\
         [pipeline.ingest.source.discovery]\nadapter = \"default\"\n\
         [pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\ncomponents = [\"source\"]\n\
         [pipeline.ingest.target]\ncatalog_template = \"warehouse\"\nschema_template = \"staging__{source}\"\n",
    )
    .unwrap();
    let db = duckdb::Connection::open(root.join("warehouse.duckdb")).unwrap();
    db.execute_batch("CREATE SCHEMA raw__shop; CREATE TABLE raw__shop.orders AS SELECT 1 AS id;")
        .unwrap();
}

fn assert_refused_plan(root: &Path, expect_fine: bool) {
    let out = rocky(root, &["plan"]);
    assert!(
        !out.status.success(),
        "plan exited 0: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let skipped = json["skipped"].as_array().unwrap();
    assert!(
        skipped
            .iter()
            .any(|s| s["model"] == "eph" && s["reason"].as_str().unwrap().contains("E038")),
        "{skipped:?}"
    );
    assert!(
        skipped
            .iter()
            .any(|s| s["model"] == "reader" && s["reason"].as_str().unwrap().contains("eph")),
        "{skipped:?}"
    );
    let expected = if expect_fine {
        vec!["fine"]
    } else {
        Vec::new()
    };
    let output_models: Vec<&str> = json["models"]
        .as_array()
        .map(|models| models.iter().map(|v| v.as_str().unwrap()).collect())
        .unwrap_or_default();
    assert_eq!(output_models, expected);
    let plan_id = json["plan_id"].as_str().expect("persisted run plan");
    let persisted: serde_json::Value = serde_json::from_slice(
        &fs::read(root.join(".rocky/plans").join(format!("{plan_id}.json"))).unwrap(),
    )
    .unwrap();
    let body = &persisted["payload"];
    assert_eq!(body["models"], json["models"]);
    assert_eq!(body["execution_layers"], json["execution_layers"]);
    for key in ["models", "execution_layers"] {
        let members = body[key].as_array().cloned().unwrap_or_default();
        assert!(!format!("{members:?}").contains("eph"));
        assert!(!format!("{members:?}").contains("reader"));
    }
}

#[test]
fn plan_refuses_ephemeral_and_dependent_without_model_selector() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path());
    model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
    model(tmp.path(), "reader", "SELECT id FROM eph", "full_refresh");
    assert_refused_plan(tmp.path(), false);
}

#[test]
fn plan_keeps_unrelated_healthy_model() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path());
    model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
    model(tmp.path(), "reader", "SELECT id FROM eph", "full_refresh");
    model(tmp.path(), "fine", "SELECT 2 AS id", "full_refresh");
    assert_refused_plan(tmp.path(), true);
}

#[test]
fn dag_excludes_refused_model_before_deriving_edges() {
    let tmp = tempfile::tempdir().unwrap();
    fs::write(tmp.path().join("rocky.toml"), "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n[pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n[pipeline.t.target]\nadapter = \"default\"\n[run]\nstrict_scheduling = true\n").unwrap();
    model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
    model(tmp.path(), "reader", "SELECT id FROM eph", "full_refresh");
    model(tmp.path(), "fine", "SELECT 2 AS id", "full_refresh");
    let out = rocky(tmp.path(), &["run", "--dag"]);
    assert!(!out.status.success(), "DAG falsely succeeded");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let nodes = json["nodes"].as_array().unwrap();
    assert!(
        !nodes
            .iter()
            .any(|node| node["label"] == "eph" || node["label"] == "reader"),
        "{nodes:?}"
    );
    assert!(
        nodes
            .iter()
            .any(|node| node["label"] == "fine" && node["status"] == "completed"),
        "{nodes:?}"
    );
    assert!(
        json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("E038"))
    );
}

#[test]
fn plan_uses_cached_source_types_when_refusing_a_model() {
    use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use rocky_core::state::StateStore;

    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path());
    let models = tmp.path().join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join("bad_time.sql"),
        "SELECT id AS event_time FROM raw__shop.orders WHERE id >= @start_date AND id < @end_date",
    )
    .unwrap();
    fs::write(
        models.join("bad_time.toml"),
        "name = \"bad_time\"\n[[sources]]\ncatalog = \"warehouse\"\nschema = \"raw__shop\"\ntable = \"orders\"\n[strategy]\ntype = \"time_interval\"\ntime_column = \"event_time\"\ngranularity = \"day\"\nfirst_partition = \"2026-01-01\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"bad_time\"\n",
    )
    .unwrap();
    let state_path = tmp.path().join("state.redb");
    let state = StateStore::open(&state_path).unwrap();
    state
        .write_schema_cache_entry(
            &schema_cache_key("warehouse", "raw__shop", "orders"),
            &SchemaCacheEntry {
                columns: vec![StoredColumn {
                    name: "id".to_string(),
                    data_type: "BIGINT".to_string(),
                    nullable: true,
                }],
                cached_at: chrono::Utc::now(),
            },
        )
        .unwrap();
    drop(state);

    let out = rocky(tmp.path(), &["--state-path", "state.redb", "plan"]);
    assert!(
        !out.status.success(),
        "plan falsely succeeded: {}",
        String::from_utf8_lossy(&out.stdout)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    for code in ["E021", "E022"] {
        assert!(
            json["skipped"].as_array().unwrap().iter().any(|s| {
                s["model"] == "bad_time" && s["reason"].as_str().unwrap().contains(code)
            }),
            "{}",
            json
        );
    }
    assert!(
        json["models"].as_array().is_none_or(Vec::is_empty),
        "{}",
        json
    );

    let selected = rocky(
        tmp.path(),
        &["--state-path", "state.redb", "plan", "--model", "bad_time"],
    );
    assert!(!selected.status.success());
    let selected: serde_json::Value = serde_json::from_slice(&selected.stdout).unwrap();
    for code in ["E021", "E022"] {
        assert!(
            selected["skipped"].as_array().unwrap().iter().any(|s| {
                s["model"] == "bad_time" && s["reason"].as_str().unwrap().contains(code)
            }),
            "{selected}"
        );
    }
    assert!(
        selected["statements"].as_array().unwrap().is_empty(),
        "a refused selected model must not preview executable SQL: {selected}"
    );
}

#[test]
fn selected_compile_error_does_not_preview_its_sql() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path());
    model(tmp.path(), "a", "SELECT 1 AS id", "full_refresh");
    model(tmp.path(), "b", "SELECT 2 AS id", "full_refresh");
    let sidecar = tmp.path().join("models/b.toml");
    fs::write(
        &sidecar,
        fs::read_to_string(&sidecar)
            .unwrap()
            .replace("table = \"b\"", "table = \"a\""),
    )
    .unwrap();
    let out = rocky(tmp.path(), &["plan", "--model", "a"]);
    assert!(!out.status.success(), "selected E036 model falsely planned");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert!(
        json["skipped"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["model"] == "a" && s["reason"].as_str().unwrap().contains("E036")),
        "{json}"
    );
    assert!(
        json["statements"].as_array().unwrap().is_empty(),
        "refused model SQL was previewed: {json}"
    );
}

#[test]
fn plan_reports_refusals_even_when_plan_store_cannot_write() {
    let tmp = tempfile::tempdir().unwrap();
    replication_project(tmp.path());
    model(tmp.path(), "eph", "SELECT 1 AS id", "ephemeral");
    model(tmp.path(), "reader", "SELECT id FROM eph", "full_refresh");
    fs::create_dir(tmp.path().join(".rocky")).unwrap();
    fs::write(tmp.path().join(".rocky/plans"), "occupied by a file").unwrap();
    let out = rocky(tmp.path(), &["plan"]);
    assert!(!out.status.success(), "plan falsely succeeded");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let skipped = json["skipped"].as_array().unwrap();
    assert!(
        skipped
            .iter()
            .any(|s| s["model"] == "eph" && s["reason"].as_str().unwrap().contains("E038")),
        "{json}"
    );
    assert!(skipped.iter().any(|s| s["model"] == "reader"), "{json}");
    assert!(json["plan_id"].is_null(), "no plan was persisted: {json}");
}

#[test]
fn dag_compile_gate_keeps_separate_adapter_targets_separate() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::create_dir_all(root.join("left/models")).unwrap();
    fs::create_dir_all(root.join("right/models")).unwrap();
    fs::write(root.join("rocky.toml"), "[adapter.left]\ntype = \"duckdb\"\npath = \"left/warehouse.duckdb\"\n[adapter.right]\ntype = \"duckdb\"\npath = \"right/warehouse.duckdb\"\n[pipeline.left]\ntype = \"transformation\"\nmodels = \"left/models/**\"\n[pipeline.left.target]\nadapter = \"left\"\n[pipeline.right]\ntype = \"transformation\"\nmodels = \"right/models/**\"\n[pipeline.right.target]\nadapter = \"right\"\n").unwrap();
    for (side, name) in [("left", "a"), ("right", "b")] {
        let dir = root.join(side).join("models");
        fs::write(dir.join(format!("{name}.sql")), "SELECT 1 AS id").unwrap();
        fs::write(dir.join(format!("{name}.toml")), format!("name = \"{name}\"\n[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"shared\"\n")).unwrap();
    }
    let out = rocky(root, &["run", "--dag"]);
    assert!(
        out.status.success(),
        "separate adapters were conflated: stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let nodes = json["nodes"].as_array().unwrap();
    assert!(
        nodes
            .iter()
            .any(|n| n["label"] == "a" && n["status"] == "completed"),
        "{json}"
    );
    assert!(
        nodes
            .iter()
            .any(|n| n["label"] == "b" && n["status"] == "completed"),
        "{json}"
    );
}

#[test]
fn dag_withholds_cross_pipeline_reader_of_refused_model_case_insensitively() {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::create_dir_all(root.join("one")).unwrap();
    fs::create_dir_all(root.join("two")).unwrap();
    fs::write(root.join("rocky.toml"), "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n[pipeline.one]\ntype = \"transformation\"\nmodels = \"one/**\"\n[pipeline.one.target]\nadapter = \"default\"\n[pipeline.two]\ntype = \"transformation\"\nmodels = \"two/**\"\n[pipeline.two.target]\nadapter = \"default\"\n").unwrap();
    for (dir, name, sql, strategy) in [
        ("one", "EPH", "SELECT 1 AS id", "ephemeral"),
        ("two", "reader", "SELECT id FROM EPH", "full_refresh"),
        ("two", "fine", "SELECT 2 AS id", "full_refresh"),
    ] {
        let base = root.join(dir);
        fs::write(base.join(format!("{name}.sql")), sql).unwrap();
        fs::write(base.join(format!("{name}.toml")), format!("name = \"{name}\"\n[strategy]\ntype = \"{strategy}\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\ntable = \"{name}\"\n")).unwrap();
    }
    let out = rocky(root, &["run", "--dag"]);
    assert!(!out.status.success(), "refusal vanished across pipelines");
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let nodes = json["nodes"].as_array().unwrap();
    assert!(
        !nodes
            .iter()
            .any(|n| n["label"] == "EPH" || n["label"] == "reader"),
        "{json}"
    );
    assert!(
        nodes
            .iter()
            .any(|n| n["label"] == "fine" && n["status"] == "completed"),
        "{json}"
    );
    assert!(
        json["warnings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|w| w.as_str().unwrap().contains("reader")),
        "{json}"
    );
}
