//! Scripted MCP round-trip tests — no LLM, no network.
//!
//! Each test serves a [`RockyMcpServer`] over an in-process duplex pipe and
//! drives it with an rmcp client, exercising `tools/list` + `tools/call`
//! exactly as a real harness would over stdio.

use std::path::Path;

use rmcp::ServiceExt;
use rmcp::model::{CallToolRequestParams, GetPromptRequestParams};
use rocky_mcp::RockyMcpServer;
use tempfile::TempDir;

/// Write a minimal DuckDB project: `rocky.toml` + one model + sidecar.
/// `db_path` is the DuckDB file the adapter connects to.
fn write_project(dir: &Path, db_path: &Path) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"[adapter]
type = "duckdb"
path = "{}"

[pipeline.p]
strategy = "full_refresh"

[pipeline.p.source.discovery]
adapter = "default"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "warehouse"
schema_template = "out"
"#,
            db_path.display()
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("models").join("orders.sql"),
        "SELECT 1 AS id, 'COMPLETE' AS status\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("models").join("orders.toml"),
        "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"out\"\ntable = \"orders\"\n",
    )
    .unwrap();
}

/// Spawn `server` on one end of a duplex pipe and return a connected client.
async fn connect(server: RockyMcpServer) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Ok(svc) = server.serve(server_io).await {
            let _ = svc.waiting().await;
        }
    });
    ().serve(client_io).await.expect("client connects")
}

#[tokio::test]
async fn tools_list_returns_expected_set() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let tools = client.list_all_tools().await.expect("list tools");
    let mut names: Vec<String> = tools.into_iter().map(|t| t.name.to_string()).collect();
    names.sort();

    assert_eq!(
        names,
        vec![
            "breaking_change",
            "catalog",
            "compile",
            "dependents",
            "draft_contract",
            "drift_preview",
            "explain_model",
            "generate_tests",
            "governance_preview",
            "history",
            "inspect_schema",
            "lineage",
            "list",
            "metrics",
            "optimize",
            "plan_preview",
            "profile_column",
            "propose",
            "sample_rows",
            "suggest_freshness_block",
            "test",
        ]
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn compile_returns_trimmed_shape() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let result = client
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call");

    assert_ne!(result.is_error, Some(true), "compile should not error");
    let sc = result
        .structured_content
        .expect("compile returns structured content");
    let obj = sc.as_object().unwrap();
    // Trimmed shape: counts + diagnostics, no expanded_sql / models_detail.
    assert_eq!(obj["has_errors"], serde_json::json!(false));
    assert_eq!(obj["model_count"], serde_json::json!(1));
    assert!(obj.contains_key("error_count"));
    assert!(obj.contains_key("warning_count"));
    assert!(obj.contains_key("diagnostics"));
    assert!(
        !obj.contains_key("expanded_sql"),
        "expanded_sql must be dropped"
    );
    assert!(
        !obj.contains_key("models_detail"),
        "models_detail must be dropped"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn list_models_round_trips() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let args = serde_json::json!({ "kind": "models" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("list").with_arguments(args))
        .await
        .expect("list call");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["kind"], serde_json::json!("models"));
    let entries = sc["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["name"], serde_json::json!("orders"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn propose_writes_ai_authored_plan() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let result = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose call");
    let sc = result.structured_content.expect("structured content");
    let plan_id = sc["plan_id"].as_str().expect("plan_id");
    assert_eq!(plan_id.len(), 64, "blake3 hex is 64 chars");

    // The plan was persisted as an AI-authored plan under .rocky/plans.
    let plan_path = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.json"));
    let plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&plan_path).unwrap()).unwrap();
    assert_eq!(plan["kind"], serde_json::json!("ai_authored"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn sample_rows_returns_capped_rows_on_duckdb() {
    let dir = TempDir::new().unwrap();
    // The model declares `catalog = "warehouse"`; the bare-model ref the tool
    // builds is the runner.s three-part `warehouse.out.orders`. On DuckDB the
    // catalog name is the file stem, so the file must be `warehouse.duckdb`.
    // (DuckDB reserves `main`, renaming a `main.duckdb` catalog to `main_db`.)
    let db_path = dir.path().join("warehouse.duckdb");
    write_project(dir.path(), &db_path);

    // Pre-materialize the model's target table so sample_rows has data to read.
    // The DuckDB adapter the server builds connects to the same file.
    {
        use rocky_core::traits::WarehouseAdapter;
        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db_path).unwrap();
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS out")
            .await
            .unwrap();
        adapter
            .execute_statement(
                "CREATE OR REPLACE TABLE out.orders AS \
                 SELECT * FROM (VALUES (1,'COMPLETE'),(2,'COMPLETE'),(3,'COMPLETE')) AS t(id,status)",
            )
            .await
            .unwrap();
    }

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders", "percent": 100 })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("sample_rows").with_arguments(args))
        .await
        .expect("sample_rows call");
    let sc = result.structured_content.expect("structured content");
    assert_ne!(sc["unavailable"], serde_json::json!(true));
    let cols = sc["columns"].as_array().unwrap();
    assert_eq!(cols.len(), 2, "id + status");
    let rows = sc["rows"].as_array().unwrap();
    assert!(!rows.is_empty(), "sampled at least one row");
    assert!(rows.len() <= 50, "capped at 50 rows");

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn breaking_change_skips_gate_outside_git_repo() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // No `git init` — `extract_base_compile` cannot resolve the base ref, so
    // the gate is skipped and the wire contract reports why.
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let result = client
        .call_tool(CallToolRequestParams::new("breaking_change"))
        .await
        .expect("breaking_change call");

    let sc = result.structured_content.expect("structured content");
    let obj = sc.as_object().unwrap();
    assert_eq!(obj["has_breaking"], serde_json::json!(false));
    assert_eq!(obj["breaking_count"], serde_json::json!(0));
    assert!(
        obj.get("skipped_reason").and_then(|v| v.as_str()).is_some(),
        "non-git project must surface a skipped_reason, got: {obj:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn dependents_returns_downstream_consumers() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // Add a second model that selects from `orders`, making it a dependent.
    std::fs::write(
        dir.path().join("models").join("order_ids.sql"),
        "SELECT id FROM orders\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models").join("order_ids.toml"),
        "name = \"order_ids\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"out\"\ntable = \"order_ids\"\n",
    )
    .unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("dependents").with_arguments(args))
        .await
        .expect("dependents call");

    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["model"], serde_json::json!("orders"));
    let deps = sc["dependents"].as_array().unwrap();
    assert!(
        deps.iter()
            .any(|d| d["model"] == serde_json::json!("order_ids")),
        "order_ids depends on orders; got {deps:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn prompts_list_returns_expected_set() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let prompts = client.list_all_prompts().await.expect("list prompts");
    let mut names: Vec<String> = prompts.iter().map(|p| p.name.clone()).collect();
    names.sort();
    assert_eq!(
        names,
        vec![
            "add_tests_to_pks",
            "build_model",
            "find_untested_models",
            "fix_failing_test",
            "summarize_project",
        ],
        "prompts/list must enumerate the full trajectory set"
    );

    // The build_model prompt declares its single `intent` argument.
    let build_model = prompts
        .iter()
        .find(|p| p.name == "build_model")
        .expect("build_model prompt present");
    let args = build_model
        .arguments
        .as_ref()
        .expect("build_model declares arguments");
    assert!(
        args.iter().any(|a| a.name == "intent"),
        "build_model must declare an `intent` argument"
    );

    // The scoped trajectories declare an optional `model` argument.
    let add_tests = prompts
        .iter()
        .find(|p| p.name == "add_tests_to_pks")
        .expect("add_tests_to_pks prompt present");
    assert!(
        add_tests
            .arguments
            .as_ref()
            .is_some_and(|a| a.iter().any(|arg| arg.name == "model")),
        "add_tests_to_pks must declare a `model` argument"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn prompt_get_build_model_returns_authoring_loop() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let intent = "daily completed-orders revenue by region";
    let args = serde_json::json!({ "intent": intent })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .get_prompt(GetPromptRequestParams::new("build_model").with_arguments(args))
        .await
        .expect("get_prompt build_model");

    assert!(
        !result.messages.is_empty(),
        "build_model must return prompt messages"
    );

    // Flatten every text message into one haystack and assert on the key
    // workflow steps + the reconcile discipline + the user's intent — wording
    // is free to drift, but these anchors must survive copy edits.
    use rmcp::model::PromptMessageContent;
    let haystack: String = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            PromptMessageContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n");

    for anchor in [
        intent,
        "inspect_schema",
        "sample_rows",
        "profile_column",
        "compile",
        "plan_preview",
        "propose",
        "review",
        "apply",
    ] {
        assert!(
            haystack.contains(anchor),
            "build_model prompt should mention `{anchor}`; full text:\n{haystack}"
        );
    }
    // The reconcile discipline is the load-bearing instruction.
    assert!(
        haystack.to_lowercase().contains("reconcile"),
        "build_model must emphasize the reconcile discipline"
    );

    client.cancel().await.unwrap();
}

/// Flatten a prompt result's text messages into one searchable haystack.
fn prompt_text(result: &rmcp::model::GetPromptResult) -> String {
    use rmcp::model::PromptMessageContent;
    result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            PromptMessageContent::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Each authoring trajectory orchestrates the existing read-only / grounding
/// MCP tools and stops at the human gate — the propose-stop discipline must
/// survive copy edits, so assert on the load-bearing anchors. `summarize_project`
/// is the read-only exception: it must NOT propose.
#[tokio::test]
async fn authoring_trajectories_orchestrate_tools_and_stop_at_the_gate() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // find_untested_models — catalog -> generate_tests/draft_contract -> propose,
    // stopping at the review/apply gate.
    let untested = client
        .get_prompt(GetPromptRequestParams::new("find_untested_models"))
        .await
        .expect("get_prompt find_untested_models");
    let haystack = prompt_text(&untested);
    for anchor in [
        "catalog",
        "generate_tests",
        "draft_contract",
        "propose",
        "review",
        "apply",
    ] {
        assert!(
            haystack.contains(anchor),
            "find_untested_models should mention `{anchor}`; full text:\n{haystack}"
        );
    }
    assert!(
        haystack.to_lowercase().contains("reconcile"),
        "find_untested_models must carry the reconcile discipline"
    );

    // add_tests_to_pks — inspect_schema -> profile_column -> generate_tests ->
    // propose. The optional `model` arg scopes the trajectory text.
    let pk_args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let pks = client
        .get_prompt(GetPromptRequestParams::new("add_tests_to_pks").with_arguments(pk_args))
        .await
        .expect("get_prompt add_tests_to_pks");
    let haystack = prompt_text(&pks);
    for anchor in [
        "orders", // the scoped model name threads into the text
        "inspect_schema",
        "profile_column",
        "generate_tests",
        "propose",
        "review",
        "apply",
    ] {
        assert!(
            haystack.contains(anchor),
            "add_tests_to_pks should mention `{anchor}`; full text:\n{haystack}"
        );
    }

    // fix_failing_test — test -> profile_column -> propose, stopping at the gate.
    let fix = client
        .get_prompt(GetPromptRequestParams::new("fix_failing_test"))
        .await
        .expect("get_prompt fix_failing_test");
    let haystack = prompt_text(&fix);
    for anchor in ["test", "profile_column", "propose", "review", "apply"] {
        assert!(
            haystack.contains(anchor),
            "fix_failing_test should mention `{anchor}`; full text:\n{haystack}"
        );
    }

    // summarize_project — read-only: catalog + lineage, and explicitly NOT a
    // propose/apply trajectory.
    let summary = client
        .get_prompt(GetPromptRequestParams::new("summarize_project"))
        .await
        .expect("get_prompt summarize_project");
    let haystack = prompt_text(&summary);
    for anchor in ["catalog", "lineage"] {
        assert!(
            haystack.contains(anchor),
            "summarize_project should mention `{anchor}`; full text:\n{haystack}"
        );
    }
    assert!(
        haystack.to_lowercase().contains("read-only")
            || haystack.to_lowercase().contains("read only"),
        "summarize_project must declare itself read-only"
    );
    assert!(
        !haystack.contains("plan_id"),
        "summarize_project is read-only and must not drive a propose/plan flow:\n{haystack}"
    );

    client.cancel().await.unwrap();
}

/// Seed one successful run (recording an `orders` execution) plus one quality
/// snapshot into the state store the server will resolve for `models_dir`. The
/// `StateStore` handle is dropped before the caller starts the server so the
/// read-only opens inside the tools don't contend on the redb lock.
fn seed_run_history(models_dir: &Path) {
    use rocky_core::state::{
        ModelExecution, QualityMetrics, QualitySnapshot, RunRecord, RunStatus, RunTrigger,
        SessionSource, StateStore,
    };

    let state_path = rocky_core::state::resolve_state_path(None, models_dir).path;
    let store = StateStore::open(&state_path).expect("open state store");
    let now = chrono::Utc::now();

    let run = RunRecord {
        run_id: "run-seed-001".to_string(),
        started_at: now,
        finished_at: now + chrono::Duration::seconds(2),
        status: RunStatus::Success,
        models_executed: vec![ModelExecution {
            model_name: "orders".to_string(),
            started_at: now,
            finished_at: now + chrono::Duration::seconds(2),
            duration_ms: 2000,
            rows_affected: Some(42),
            status: "success".to_string(),
            sql_hash: "abc123def456".to_string(),
            bytes_scanned: None,
            bytes_written: Some(1024),
        }],
        trigger: RunTrigger::Manual,
        config_hash: "cfg".to_string(),
        triggering_identity: None,
        session_source: SessionSource::Cli,
        git_commit: None,
        git_branch: None,
        idempotency_key: None,
        target_catalog: None,
        hostname: "test-host".to_string(),
        rocky_version: "0.0.0-test".to_string(),
    };
    store.record_run(&run).expect("record run");

    let mut null_rates = std::collections::HashMap::new();
    // 0.6 > the 0.5 critical threshold → exercises the alert projection.
    null_rates.insert("status".to_string(), 0.6);
    store
        .record_quality(&QualitySnapshot {
            timestamp: now,
            run_id: "run-seed-001".to_string(),
            model_name: "orders".to_string(),
            metrics: QualityMetrics {
                row_count: 42,
                null_rates,
                freshness_lag_seconds: Some(120),
            },
        })
        .expect("record quality");
}

#[tokio::test]
async fn catalog_returns_project_assets() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // Add a model that selects `id FROM orders` so it has real column lineage
    // (a literal-only leaf has no produced edges, hence no tracked columns).
    // This exercises the column projection, not just the asset inventory.
    std::fs::write(
        dir.path().join("models").join("order_ids.sql"),
        "SELECT id FROM orders\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("models").join("order_ids.toml"),
        "name = \"order_ids\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"out\"\ntable = \"order_ids\"\n",
    )
    .unwrap();
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let result = client
        .call_tool(CallToolRequestParams::new("catalog"))
        .await
        .expect("catalog call");

    let sc = result.structured_content.expect("structured content");
    assert!(sc["asset_count"].as_u64().unwrap() >= 2);
    assert!(sc.as_object().unwrap().contains_key("column_count"));
    let assets = sc["assets"].as_array().unwrap();

    let orders = assets
        .iter()
        .find(|a| a["model_name"] == serde_json::json!("orders"))
        .expect("orders asset present");
    // `kind` is snake_cased at the projection boundary (the underlying
    // `AssetKind` serializes PascalCase).
    assert_eq!(orders["kind"], serde_json::json!("model"));

    // `order_ids` selects from `orders`, so its `id` column has tracked
    // lineage and must surface through the column projection.
    let order_ids = assets
        .iter()
        .find(|a| a["model_name"] == serde_json::json!("order_ids"))
        .expect("order_ids asset present");
    let cols = order_ids["columns"].as_array().unwrap();
    assert!(
        cols.iter().any(|c| c["name"] == serde_json::json!("id")),
        "order_ids should carry its `id` column; got {cols:?}"
    );

    // The token-heavy column edge set is intentionally not part of the lite
    // catalog — agents reach for `lineage` instead.
    assert!(!sc.as_object().unwrap().contains_key("edges"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn history_reports_runs_and_model_executions() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    seed_run_history(&dir.path().join("models"));

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Project-level: recent runs, no `model` arg.
    let runs = client
        .call_tool(CallToolRequestParams::new("history"))
        .await
        .expect("history call")
        .structured_content
        .expect("structured content");
    let run_list = runs["runs"].as_array().unwrap();
    assert_eq!(run_list.len(), 1);
    assert_eq!(run_list[0]["run_id"], serde_json::json!("run-seed-001"));
    assert_eq!(run_list[0]["status"], serde_json::json!("Success"));
    assert_eq!(run_list[0]["models_executed"], serde_json::json!(1));

    // Model-scoped: executions for `orders`.
    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let model_hist = client
        .call_tool(CallToolRequestParams::new("history").with_arguments(args))
        .await
        .expect("history --model call")
        .structured_content
        .expect("structured content");
    assert_eq!(model_hist["model"], serde_json::json!("orders"));
    let execs = model_hist["executions"].as_array().unwrap();
    assert_eq!(execs.len(), 1);
    assert_eq!(execs[0]["rows_affected"], serde_json::json!(42));
    assert_eq!(execs[0]["sql_hash"], serde_json::json!("abc123def456"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn history_is_empty_without_runs() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let sc = client
        .call_tool(CallToolRequestParams::new("history"))
        .await
        .expect("history call")
        .structured_content
        .expect("structured content");
    // No runs recorded → `runs` omitted (skip_serializing_if empty), no panic.
    assert!(sc.get("runs").is_none() || sc["runs"].as_array().unwrap().is_empty());

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn metrics_returns_seeded_snapshot_and_alert() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    seed_run_history(&dir.path().join("models"));

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let sc = client
        .call_tool(CallToolRequestParams::new("metrics").with_arguments(args))
        .await
        .expect("metrics call")
        .structured_content
        .expect("structured content");

    assert_eq!(sc["model"], serde_json::json!("orders"));
    let snapshots = sc["snapshots"].as_array().unwrap();
    assert_eq!(snapshots.len(), 1);
    assert_eq!(snapshots[0]["row_count"], serde_json::json!(42));
    let null_rates = snapshots[0]["null_rates"].as_array().unwrap();
    assert_eq!(null_rates[0]["column"], serde_json::json!("status"));
    // The 0.6 null rate trips the critical null_rate alert.
    let alerts = sc["alerts"].as_array().unwrap();
    assert!(
        alerts
            .iter()
            .any(|a| a["kind"] == serde_json::json!("null_rate")),
        "0.6 null rate should raise a null_rate alert; got {alerts:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn optimize_recommends_from_seeded_history() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    seed_run_history(&dir.path().join("models"));

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let sc = client
        .call_tool(CallToolRequestParams::new("optimize"))
        .await
        .expect("optimize call")
        .structured_content
        .expect("structured content");

    let recs = sc["recommendations"].as_array().unwrap();
    assert!(
        recs.iter()
            .any(|r| r["model_name"] == serde_json::json!("orders")),
        "optimize should analyse the seeded orders model; got {recs:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn optimize_reports_message_without_history() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let sc = client
        .call_tool(CallToolRequestParams::new("optimize"))
        .await
        .expect("optimize call")
        .structured_content
        .expect("structured content");

    assert!(sc["recommendations"].as_array().unwrap().is_empty());
    assert!(
        sc["message"].as_str().unwrap().contains("no run history"),
        "empty optimize must explain why"
    );

    client.cancel().await.unwrap();
}

/// Like `write_project` but with zero model files — exercises the cold-start
/// path (a project an agent has not authored any model into yet).
fn write_empty_project(dir: &Path, db_path: &Path) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"[adapter]
type = "duckdb"
path = "{}"

[pipeline.p]
strategy = "full_refresh"

[pipeline.p.source.discovery]
adapter = "default"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "warehouse"
schema_template = "out"
"#,
            db_path.display()
        ),
    )
    .unwrap();
}

/// Pre-materialize `out.orders` with the given `VALUES` body on `db_path`.
async fn materialize_orders(db_path: &Path, schema: &str, values: &str) {
    use rocky_core::traits::WarehouseAdapter;
    let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(db_path).unwrap();
    adapter
        .execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .await
        .unwrap();
    adapter
        .execute_statement(&format!(
            "CREATE OR REPLACE TABLE {schema}.orders AS \
             SELECT * FROM (VALUES {values}) AS t(id, status)"
        ))
        .await
        .unwrap();
}

#[tokio::test]
async fn sample_rows_default_returns_rows_on_small_table() {
    // Regression: the old default (10% bernoulli) returned ~0 rows on a tiny
    // table. With no `percent`, sample_rows now returns the first rows.
    let dir = TempDir::new().unwrap();
    // File stem must equal the model.s declared catalog (`warehouse`) so the
    // runner-shaped three-part ref `warehouse.out.orders` resolves on DuckDB.
    let db_path = dir.path().join("warehouse.duckdb");
    write_project(dir.path(), &db_path);
    materialize_orders(
        &db_path,
        "out",
        "(1,'COMPLETE'),(2,'COMPLETE'),(3,'PENDING')",
    )
    .await;

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    // No `percent` → deterministic first-rows, not a percentage sample.
    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let sc = client
        .call_tool(CallToolRequestParams::new("sample_rows").with_arguments(args))
        .await
        .expect("sample_rows call")
        .structured_content
        .expect("structured content");
    let rows = sc["rows"].as_array().unwrap();
    assert_eq!(
        rows.len(),
        3,
        "all 3 rows returned without sampling; got {rows:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn profile_column_lists_top_values_for_low_cardinality() {
    let dir = TempDir::new().unwrap();
    // File stem must equal the model.s declared catalog (`warehouse`) so the
    // runner-shaped three-part ref `warehouse.out.orders` resolves on DuckDB.
    let db_path = dir.path().join("warehouse.duckdb");
    write_project(dir.path(), &db_path);
    materialize_orders(
        &db_path,
        "out",
        "(1,'COMPLETE'),(2,'COMPLETE'),(3,'PENDING')",
    )
    .await;

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders", "column": "status" })
        .as_object()
        .unwrap()
        .clone();
    let sc = client
        .call_tool(CallToolRequestParams::new("profile_column").with_arguments(args))
        .await
        .expect("profile_column call")
        .structured_content
        .expect("structured content");

    assert_eq!(sc["distinct"], serde_json::json!(2));
    let top_values = sc["top_values"]
        .as_array()
        .expect("top_values present for a low-cardinality column");
    // The exact literal 'COMPLETE' is surfaced — what min/max alone cannot show.
    let complete = top_values
        .iter()
        .find(|v| v["value"] == serde_json::json!("COMPLETE"))
        .expect("COMPLETE listed in top_values");
    assert_eq!(complete["count"], serde_json::json!(2));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn sample_rows_reaches_raw_source_by_qualified_ref() {
    // Source-reach: a qualified `schema.table` that is NOT a model is sampled
    // directly, with no compile required.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("src.duckdb");
    write_project(dir.path(), &db_path);
    materialize_orders(&db_path, "seeds", "(1,'COMPLETE'),(2,'PENDING')").await;

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    // `seeds.orders` is a raw table, not a model — reached by its qualified name.
    let args = serde_json::json!({ "model": "seeds.orders" })
        .as_object()
        .unwrap()
        .clone();
    let sc = client
        .call_tool(CallToolRequestParams::new("sample_rows").with_arguments(args))
        .await
        .expect("sample_rows call")
        .structured_content
        .expect("structured content");

    assert_ne!(sc["unavailable"], serde_json::json!(true));
    assert_eq!(
        sc["columns"].as_array().unwrap().len(),
        2,
        "id + status from the raw source"
    );
    assert_eq!(sc["rows"].as_array().unwrap().len(), 2);

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn inspect_schema_discovers_raw_sources_and_tolerates_cold_start() {
    // Cold start: a project with ZERO models must not error, and the physical
    // raw source tables must still be discovered with their columns.
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("cold.duckdb");
    write_empty_project(dir.path(), &db_path);
    materialize_orders(&db_path, "seeds", "(1,'COMPLETE')").await;

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let sc = client
        .call_tool(CallToolRequestParams::new("inspect_schema"))
        .await
        .expect("inspect_schema must not error at cold start")
        .structured_content
        .expect("structured content");

    assert!(
        sc["models"].as_array().unwrap().is_empty(),
        "no models authored yet"
    );
    let sources = sc["sources"].as_array().unwrap();
    let orders = sources
        .iter()
        .find(|s| s["name"] == serde_json::json!("seeds.orders"))
        .expect("seeds.orders discovered as a source");
    let cols = orders["columns"].as_array().unwrap();
    assert!(
        cols.iter()
            .any(|c| c["name"] == serde_json::json!("status")),
        "discovered source carries its columns; got {cols:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn compile_runs_p001_dialect_lint_on_demand() {
    // Passing `target_dialect` to the compile tool runs the P001 portability
    // lint on demand, even with no `[portability]` in rocky.toml.
    // `NVL(...)` does not port to BigQuery, so it must surface as P001.
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    std::fs::write(
        dir.path().join("models").join("orders.sql"),
        "SELECT 1 AS id, NVL('COMPLETE', 'PENDING') AS status\n",
    )
    .unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Without target_dialect → no P001 (behaviour unchanged).
    let baseline = client
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call")
        .structured_content
        .expect("structured content");
    let baseline_diags = baseline["diagnostics"].as_array().unwrap();
    assert!(
        !baseline_diags
            .iter()
            .any(|d| d["code"] == serde_json::json!("P001")),
        "no P001 without target_dialect; got {baseline_diags:?}"
    );

    // With target_dialect = bigquery → P001 fires for NVL.
    let args = serde_json::json!({ "target_dialect": "bigquery" })
        .as_object()
        .unwrap()
        .clone();
    let linted = client
        .call_tool(CallToolRequestParams::new("compile").with_arguments(args))
        .await
        .expect("compile call with target_dialect")
        .structured_content
        .expect("structured content");
    let diags = linted["diagnostics"].as_array().unwrap();
    assert!(
        diags.iter().any(|d| d["code"] == serde_json::json!("P001")),
        "target_dialect=bigquery must surface P001 for NVL; got {diags:?}"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn compile_rejects_unknown_target_dialect() {
    // An unrecognised target_dialect is a caller error, not a silent no-op.
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({ "target_dialect": "redshift" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("compile").with_arguments(args))
        .await
        .expect("compile call returns a result");
    assert_eq!(
        result.is_error,
        Some(true),
        "unknown target_dialect must be an error"
    );

    client.cancel().await.unwrap();
}

/// The three generator tools degrade gracefully without an API key: a null
/// draft + a message naming the missing env var, never an error and never a
/// network call. Driven from one test so the `remove_var` happens once.
#[tokio::test]
async fn generator_tools_degrade_without_api_key() {
    // SAFETY: `#[tokio::test]` runs on a current-thread runtime, so the spawned
    // server task shares this single thread; nothing else reads the env
    // concurrently.
    unsafe {
        std::env::remove_var(rocky_ai::client::AI_API_KEY_ENV);
    }

    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();

    // draft_contract: contract_toml null, message names the env var.
    let dc = client
        .call_tool(CallToolRequestParams::new("draft_contract").with_arguments(args.clone()))
        .await
        .expect("draft_contract call");
    assert_ne!(dc.is_error, Some(true), "missing key is a no-op, not error");
    let sc = dc.structured_content.expect("structured content");
    assert!(
        sc.get("contract_toml").is_none() || sc["contract_toml"].is_null(),
        "no contract without a key; got {sc:?}"
    );
    assert!(
        sc["message"]
            .as_str()
            .unwrap()
            .contains(rocky_ai::client::AI_API_KEY_ENV),
        "draft_contract message should name the env var; got {sc:?}"
    );

    // generate_tests: assertions empty, message names the env var.
    let gt = client
        .call_tool(CallToolRequestParams::new("generate_tests").with_arguments(args.clone()))
        .await
        .expect("generate_tests call");
    assert_ne!(gt.is_error, Some(true));
    let sc = gt.structured_content.expect("structured content");
    assert!(
        sc["assertions"].as_array().unwrap().is_empty(),
        "no assertions without a key; got {sc:?}"
    );
    assert!(
        sc["message"]
            .as_str()
            .unwrap()
            .contains(rocky_ai::client::AI_API_KEY_ENV)
    );

    // explain_model: intent null, message names the env var.
    let em = client
        .call_tool(CallToolRequestParams::new("explain_model").with_arguments(args))
        .await
        .expect("explain_model call");
    assert_ne!(em.is_error, Some(true));
    let sc = em.structured_content.expect("structured content");
    assert!(
        sc.get("intent").is_none() || sc["intent"].is_null(),
        "no intent without a key; got {sc:?}"
    );
    assert!(
        sc["message"]
            .as_str()
            .unwrap()
            .contains(rocky_ai::client::AI_API_KEY_ENV)
    );

    client.cancel().await.unwrap();
}

/// Write a DuckDB project whose single model declares governance — a
/// `[classification]` tag + a `retention` policy, with a workspace `[mask]`
/// resolving the tag. `governance_preview` reads this offline (no warehouse).
fn write_governed_project(dir: &Path, db_path: &Path) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"[adapter]
type = "duckdb"
path = "{}"

[mask]
pii = "hash"

[mask.prod]
pii = "redact"

[pipeline.p]
strategy = "full_refresh"

[pipeline.p.source.discovery]
adapter = "default"

[pipeline.p.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.p.target]
catalog_template = "warehouse"
schema_template = "out"
"#,
            db_path.display()
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("models").join("orders.sql"),
        "SELECT 1 AS id, 'a@b.com' AS email\n",
    )
    .unwrap();
    std::fs::write(
        dir.join("models").join("orders.toml"),
        r#"name = "orders"
retention = "90d"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "out"
table = "orders"

[classification]
email = "pii"
"#,
    )
    .unwrap();
}

/// `governance_preview` is offline (compile + sidecar read, no warehouse): it
/// surfaces the classification / mask / retention the model declares, resolving
/// the mask against the active env. Requires no API key and no live creds.
#[tokio::test]
async fn governance_preview_surfaces_declared_actions_offline() {
    let dir = TempDir::new().unwrap();
    // No DuckDB file is created — the tool must not touch the warehouse.
    write_governed_project(dir.path(), &dir.path().join("warehouse.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Default env → `pii` resolves to `hash`.
    let result = client
        .call_tool(CallToolRequestParams::new("governance_preview"))
        .await
        .expect("governance_preview call");
    assert_ne!(
        result.is_error,
        Some(true),
        "governance_preview is offline and must not error"
    );
    let sc = result.structured_content.expect("structured content");

    let classifications = sc["classification_actions"].as_array().unwrap();
    assert_eq!(classifications.len(), 1, "one (model,column,tag): {sc:?}");
    assert_eq!(classifications[0]["model"], serde_json::json!("orders"));
    assert_eq!(classifications[0]["column"], serde_json::json!("email"));
    assert_eq!(classifications[0]["tag"], serde_json::json!("pii"));

    let masks = sc["mask_actions"].as_array().unwrap();
    assert_eq!(masks.len(), 1, "pii resolves to a strategy: {sc:?}");
    assert_eq!(masks[0]["resolved_strategy"], serde_json::json!("hash"));

    let retentions = sc["retention_actions"].as_array().unwrap();
    assert_eq!(retentions.len(), 1, "one retention policy: {sc:?}");
    assert_eq!(retentions[0]["duration_days"], serde_json::json!(90));

    // The `prod` env override resolves `pii` to `redact` instead.
    let prod_args = serde_json::json!({ "env": "prod" })
        .as_object()
        .unwrap()
        .clone();
    let prod = client
        .call_tool(CallToolRequestParams::new("governance_preview").with_arguments(prod_args))
        .await
        .expect("governance_preview --env prod call")
        .structured_content
        .expect("structured content");
    assert_eq!(prod["env"], serde_json::json!("prod"));
    assert_eq!(
        prod["mask_actions"][0]["resolved_strategy"],
        serde_json::json!("redact"),
        "prod env must resolve pii to redact: {prod:?}"
    );

    client.cancel().await.unwrap();
}

/// `drift_preview` DESCRIBEs two warehouse tables and compares their reported
/// column types — the same apples-to-apples comparison `rocky run` performs.
/// Source has a widened `id` (BIGINT vs INTEGER) plus an extra column the
/// target lacks; the tool must surface both, and report a missing target as
/// `target_exists: false`.
#[tokio::test]
async fn drift_preview_compares_source_and_target_on_duckdb() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("warehouse.duckdb");
    write_project(dir.path(), &db_path);

    {
        use rocky_core::traits::WarehouseAdapter;
        let adapter = rocky_duckdb::adapter::DuckDbWarehouseAdapter::open(&db_path).unwrap();
        adapter
            .execute_statement("CREATE SCHEMA IF NOT EXISTS out")
            .await
            .unwrap();
        // Target: id INTEGER, status VARCHAR.
        adapter
            .execute_statement(
                "CREATE OR REPLACE TABLE out.orders AS \
                 SELECT CAST(1 AS INTEGER) AS id, 'COMPLETE' AS status",
            )
            .await
            .unwrap();
        // Source: id BIGINT (widened), status VARCHAR, plus a new `region` column.
        adapter
            .execute_statement(
                "CREATE OR REPLACE TABLE out.orders_next AS \
                 SELECT CAST(1 AS BIGINT) AS id, 'COMPLETE' AS status, 'EU' AS region",
            )
            .await
            .unwrap();
    }

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({
        "source_table": "out.orders_next",
        "target_table": "out.orders",
    })
    .as_object()
    .unwrap()
    .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("drift_preview").with_arguments(args))
        .await
        .expect("drift_preview call");
    assert_ne!(result.is_error, Some(true), "drift_preview must not error");
    let sc = result.structured_content.expect("structured content");

    assert_eq!(sc["target_exists"], serde_json::json!(true));
    // `id` drifted (BIGINT vs INTEGER) — a type change between the two tables.
    let drifted = sc["drifted_columns"].as_array().unwrap();
    assert!(
        drifted.iter().any(|d| d["name"] == serde_json::json!("id")),
        "id must drift (BIGINT vs INTEGER): {sc:?}"
    );
    // `region` is present in the source but absent from the target.
    let added = sc["added_columns"].as_array().unwrap();
    assert!(
        added.iter().any(|c| *c == serde_json::json!("region")),
        "region must be an added column: {sc:?}"
    );

    // A non-existent target → target_exists false, empty drift lists.
    let absent_args = serde_json::json!({
        "source_table": "out.orders_next",
        "target_table": "out.does_not_exist",
    })
    .as_object()
    .unwrap()
    .clone();
    let absent = client
        .call_tool(CallToolRequestParams::new("drift_preview").with_arguments(absent_args))
        .await
        .expect("drift_preview call (absent target)")
        .structured_content
        .expect("structured content");
    assert_eq!(absent["target_exists"], serde_json::json!(false));
    assert!(
        absent["drifted_columns"].as_array().unwrap().is_empty(),
        "absent target → no drift rows: {absent:?}"
    );

    // A missing SOURCE is an error, never a vacuously-clean "no drift" answer —
    // the source side is the thing being compared against, so its absence must
    // surface, not silently report zero drift.
    let bad_source_args = serde_json::json!({
        "source_table": "out.does_not_exist",
        "target_table": "out.orders",
    })
    .as_object()
    .unwrap()
    .clone();
    let bad_source = client
        .call_tool(CallToolRequestParams::new("drift_preview").with_arguments(bad_source_args))
        .await
        .expect("drift_preview call (absent source)");
    assert_eq!(
        bad_source.is_error,
        Some(true),
        "a missing source_table must be an error, not a clean no-drift result"
    );

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn suggest_freshness_block_returns_null_without_api_key() {
    // Without ANTHROPIC_API_KEY the tool degrades gracefully: a null block
    // plus an explanatory message, never an error and never a network call.
    // SAFETY: `#[tokio::test]` runs on a current-thread runtime, so the
    // spawned server task shares this single thread; nothing else reads the
    // env concurrently.
    unsafe {
        std::env::remove_var(rocky_ai::client::AI_API_KEY_ENV);
    }

    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({
        "model": "orders",
        "temporal_columns": ["created_at", "updated_at"],
    })
    .as_object()
    .unwrap()
    .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("suggest_freshness_block").with_arguments(args))
        .await
        .expect("suggest_freshness_block call");
    assert_ne!(
        result.is_error,
        Some(true),
        "missing key is a graceful no-op, not an error"
    );
    let sc = result.structured_content.expect("structured content");
    assert!(
        sc.get("freshness_block").is_none() || sc["freshness_block"].is_null(),
        "no block without a key; got {sc:?}"
    );
    let message = sc["message"].as_str().expect("explanatory message");
    assert!(
        message.contains(rocky_ai::client::AI_API_KEY_ENV),
        "message should name the missing env var; got `{message}`"
    );

    client.cancel().await.unwrap();
}
