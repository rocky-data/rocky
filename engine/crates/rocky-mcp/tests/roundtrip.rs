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
catalog_template = "main"
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
        "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"main\"\nschema = \"out\"\ntable = \"orders\"\n",
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
            "compile",
            "inspect_schema",
            "lineage",
            "list",
            "plan_preview",
            "profile_column",
            "propose",
            "sample_rows",
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

/// Write a project whose target adapter is Snowflake (not DuckDB) so the
/// grounding tools must report `unavailable` without touching a warehouse.
fn write_snowflake_project(dir: &Path) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        r#"[adapter]
type = "snowflake"
account = "acct"
username = "u"
password = "p"
warehouse = "wh"
database = "db"

[pipeline.p]
type = "transformation"
target = { adapter = "default" }
"#,
    )
    .unwrap();
    std::fs::write(dir.join("models").join("orders.sql"), "SELECT 1 AS id\n").unwrap();
    std::fs::write(
        dir.join("models").join("orders.toml"),
        "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"db\"\nschema = \"out\"\ntable = \"orders\"\n",
    )
    .unwrap();
}

#[tokio::test]
async fn sample_rows_unavailable_on_non_duckdb_adapter() {
    let dir = TempDir::new().unwrap();
    write_snowflake_project(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("sample_rows").with_arguments(args))
        .await
        .expect("sample_rows call");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["unavailable"], serde_json::json!(true));
    assert!(
        sc["reason"].as_str().unwrap().contains("DuckDB-only"),
        "reason should explain the DuckDB-only limitation"
    );
    // Data fields must be empty on the unavailable path.
    assert!(sc["columns"].as_array().unwrap().is_empty());
    assert!(sc["rows"].as_array().unwrap().is_empty());

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn profile_column_unavailable_on_non_duckdb_adapter() {
    let dir = TempDir::new().unwrap();
    write_snowflake_project(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let args = serde_json::json!({ "model": "orders", "column": "id" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("profile_column").with_arguments(args))
        .await
        .expect("profile_column call");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["unavailable"], serde_json::json!(true));
    assert!(sc["reason"].as_str().unwrap().contains("DuckDB-only"));

    client.cancel().await.unwrap();
}

#[tokio::test]
async fn sample_rows_returns_capped_rows_on_duckdb() {
    let dir = TempDir::new().unwrap();
    let db_path = dir.path().join("sample.duckdb");
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
async fn prompts_list_includes_build_model() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));

    let client = connect(server).await;
    let prompts = client.list_all_prompts().await.expect("list prompts");
    let names: Vec<String> = prompts.iter().map(|p| p.name.clone()).collect();
    assert!(
        names.iter().any(|n| n == "build_model"),
        "prompts/list must include build_model, got {names:?}"
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
