//! Live Snowflake data-grounding verification for the #775 MCP change.
//!
//! Snowflake has no `fetch_arrow_batch` implementation, so the grounding helper
//! `query_grounding` must fall back to `execute_query` (JSON). This test drives
//! the real MCP grounding tools (`sample_rows`, `profile_column`,
//! `inspect_schema`) over an in-process rmcp client against a live Snowflake
//! table, proving that fallback path end-to-end.
//!
//! `#[ignore]`-gated — needs a real Snowflake account. Reads the standard
//! `SNOWFLAKE_*` connection env vars (`SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`,
//! `SNOWFLAKE_TEST_DATABASE`, `SNOWFLAKE_USERNAME`, `SNOWFLAKE_ROLE`); the PAT
//! must be supplied via `SNOWFLAKE_PAT`. Run:
//!
//! ```bash
//! cargo test -p rocky-mcp --test grounding_snowflake_live -- --ignored --nocapture
//! ```

use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rocky_core::config::RetryConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_mcp::RockyMcpServer;
use rocky_snowflake::adapter::SnowflakeWarehouseAdapter;
use rocky_snowflake::auth::{Auth, AuthConfig};
use rocky_snowflake::connector::{ConnectorConfig, SnowflakeConnector};
use tempfile::TempDir;

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok()
}

/// Build the Snowflake adapter from the environment, mirroring the
/// rocky-snowflake live tests. Used only to set up / tear down the fixture
/// table — the grounding under test goes through the MCP server's own adapter.
fn adapter_from_env() -> Option<(SnowflakeWarehouseAdapter, String)> {
    let account = env("SNOWFLAKE_ACCOUNT")?;
    let warehouse = env("SNOWFLAKE_WAREHOUSE")?;
    let database = env("SNOWFLAKE_TEST_DATABASE").or_else(|| env("SNOWFLAKE_DATABASE"))?;
    let auth = Auth::from_config(AuthConfig {
        account: account.clone(),
        username: env("SNOWFLAKE_USERNAME"),
        password: None,
        oauth_token: env("SNOWFLAKE_OAUTH_TOKEN"),
        private_key_path: env("SNOWFLAKE_PRIVATE_KEY_PATH"),
        pat: env("SNOWFLAKE_PAT"),
    })
    .ok()?;
    let config = ConnectorConfig {
        account,
        warehouse,
        database: Some(database.clone()),
        schema: None,
        role: env("SNOWFLAKE_ROLE"),
        timeout: Duration::from_secs(180),
        retry: RetryConfig::default(),
    };
    Some((
        SnowflakeWarehouseAdapter::new(SnowflakeConnector::new(config, auth)),
        database,
    ))
}

/// A Snowflake-targeted project. The unnamed `[adapter]` becomes `default`,
/// which the pipeline targets — exactly the adapter `warehouse_adapter()`
/// resolves for the grounding tools. Creds are env-substituted at parse time.
fn write_snowflake_project(dir: &Path) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        r#"[adapter]
type = "snowflake"
account = "${SNOWFLAKE_ACCOUNT}"
warehouse = "${SNOWFLAKE_WAREHOUSE}"
database = "${SNOWFLAKE_TEST_DATABASE}"
username = "${SNOWFLAKE_USERNAME}"
pat = "${SNOWFLAKE_PAT}"
role = "${SNOWFLAKE_ROLE}"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target]
adapter = "default"
"#,
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
#[ignore = "needs a live Snowflake account (SNOWFLAKE_* env)"]
async fn snowflake_grounding_falls_back_to_json_and_returns_real_rows() {
    let Some((adapter, db)) = adapter_from_env() else {
        eprintln!("SKIP: SNOWFLAKE_* env not set");
        return;
    };

    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    // All-uppercase, unquoted identifiers so the fixture matches the grounding
    // ref regardless of whether the dialect quotes it (Snowflake folds unquoted
    // names to uppercase; quoted-uppercase resolves to the same object).
    let schema = format!("ROCKY_MCP_GROUND_{suffix}");
    let table = "ORDERS";

    // --- setup: a real schema + table + rows (incl. a genuine NULL) ---
    adapter
        .execute_statement(&format!("CREATE SCHEMA {db}.{schema}"))
        .await
        .expect("create schema");
    adapter
        .execute_statement(&format!(
            "CREATE TABLE {db}.{schema}.{table} (ID NUMBER(38,0), STATUS VARCHAR)"
        ))
        .await
        .expect("create table");
    adapter
        .execute_statement(&format!(
            "INSERT INTO {db}.{schema}.{table} (ID, STATUS) VALUES \
             (1, 'COMPLETE'), (2, 'COMPLETE'), (3, 'PENDING'), (4, NULL)"
        ))
        .await
        .expect("insert rows");

    // --- drive the MCP grounding tools over an in-process client ---
    let dir = TempDir::new().unwrap();
    write_snowflake_project(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Two-part `schema.table` ref resolves in the session database (set on the
    // connector). The tool validates + quotes each segment dialect-correctly.
    let model_ref = format!("{schema}.{table}");

    // sample_rows — first rows, no percent. This is the execute_query JSON path
    // on Snowflake (no Arrow impl → fetch_arrow_batch falls back).
    let sample = client
        .call_tool(
            CallToolRequestParams::new("sample_rows").with_arguments(
                serde_json::json!({ "model": model_ref })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("sample_rows call");
    let sc = sample
        .structured_content
        .expect("sample structured content");
    eprintln!("sample_rows -> {sc}");
    assert_ne!(
        sc["unavailable"],
        serde_json::json!(true),
        "sample available"
    );
    let cols = sc["columns"].as_array().expect("columns array");
    assert_eq!(cols.len(), 2, "id + status columns");
    let rows = sc["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 4, "all four rows sampled");

    // profile_column on `status` — counts + low-cardinality top_values literals.
    let profile = client
        .call_tool(
            CallToolRequestParams::new("profile_column").with_arguments(
                serde_json::json!({ "model": model_ref, "column": "STATUS" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("profile_column call");
    let pc = profile
        .structured_content
        .expect("profile structured content");
    eprintln!("profile_column -> {pc}");
    assert_eq!(pc["rows"], serde_json::json!(4), "4 total rows");
    assert_eq!(pc["nulls"], serde_json::json!(1), "1 null");
    assert_eq!(pc["distinct"], serde_json::json!(2), "COMPLETE + PENDING");
    assert_eq!(
        pc["min"],
        serde_json::json!("COMPLETE"),
        "min cast to string"
    );
    assert_eq!(
        pc["max"],
        serde_json::json!("PENDING"),
        "max cast to string"
    );
    let top: Vec<&str> = pc["top_values"]
        .as_array()
        .map(|a| a.iter().filter_map(|e| e["value"].as_str()).collect())
        .unwrap_or_default();
    assert!(
        top.contains(&"COMPLETE") && top.contains(&"PENDING"),
        "top_values surfaces the literals: {top:?}"
    );

    // inspect_schema — best-effort warehouse source discovery; should surface
    // the physical table. Soft check (information_schema breadth varies).
    let inspect = client
        .call_tool(
            CallToolRequestParams::new("inspect_schema").with_arguments(serde_json::Map::new()),
        )
        .await
        .expect("inspect_schema call");
    let is = inspect
        .structured_content
        .expect("inspect structured content");
    let found = is["sources"]
        .as_array()
        .map(|s| {
            s.iter().any(|e| {
                e["name"]
                    .as_str()
                    .map(|n| n.contains(table))
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false);
    eprintln!("inspect_schema source-discovery found our table: {found}");

    client.cancel().await.unwrap();

    // --- cleanup ---
    let _ = adapter
        .execute_statement(&format!("DROP SCHEMA IF EXISTS {db}.{schema} CASCADE"))
        .await;
}
