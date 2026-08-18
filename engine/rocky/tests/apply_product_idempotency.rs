//! FF-WP1 (finding 8) — the full product-bound fulfillment chain, end to end,
//! on a DuckDB scratch project with no credentials:
//!
//! 1. MCP `propose` with product fields → an AI-authored plan whose payload
//!    carries the pair AND the derived idempotency key.
//! 2. A bare apply and a pre-review apply both REFUSE (digest gate / marker
//!    floor) — the chain's fail-closed edges.
//! 3. The real `rocky review <plan-id> --approve` writes the sign-off marker.
//! 4. First `rocky apply --expect-spec-digest` EXECUTES (the model
//!    materializes in DuckDB).
//! 5. A second identical apply reports `skipped_idempotent` — the derived
//!    key's receipt deflects the re-execution.
//!
//! The propose step runs the in-process `RockyMcpServer` over a duplex pipe
//! (the same harness as rocky-mcp's round-trip tests) because `propose` is
//! deliberately MCP-only; everything after the plan exists is driven through
//! the REAL binary.

use std::fs;
use std::path::Path;
use std::process::Command;

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rocky_mcp::RockyMcpServer;

const PRODUCT_ID: &str = "product:revenue_daily";
const SPEC_DIGEST: &str = "sha256:e2e0aa";

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
"#;

const MODEL_TOML: &str = r#"
[strategy]
type = "full_refresh"

[target]
catalog = "fixture"
schema = "main"
"#;

/// Run the real binary with `--output json` against `config`, from `dir`.
fn rocky(dir: &Path, config: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(config)
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

#[tokio::test]
async fn product_bound_plan_applies_once_then_skips_idempotently() {
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
    let models = dir.join("models");
    fs::create_dir(&models).expect("create models");
    fs::write(
        models.join("orders_mart.sql"),
        "SELECT 1 AS id, 100 AS revenue",
    )
    .expect("write model sql");
    fs::write(models.join("orders_mart.toml"), MODEL_TOML).expect("write model config");

    // --- 1. propose over the in-process MCP server (the sole plan writer) ---
    let server = RockyMcpServer::new(config.clone());
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Ok(svc) = server.serve(server_io).await {
            let _ = svc.waiting().await;
        }
    });
    let client = ().serve(client_io).await.expect("client connects");

    let args = serde_json::json!({
        "model": "orders_mart",
        "product_id": PRODUCT_ID,
        "spec_digest": SPEC_DIGEST,
    })
    .as_object()
    .unwrap()
    .clone();
    let proposed = client
        .call_tool(CallToolRequestParams::new("propose").with_arguments(args))
        .await
        .expect("propose call");
    assert_ne!(
        proposed.is_error,
        Some(true),
        "propose succeeds under NotConfigured policy: {:?}",
        proposed.structured_content
    );
    let sc = proposed.structured_content.expect("propose result");
    let plan_id = sc["plan_id"].as_str().expect("plan_id").to_string();
    client.cancel().await.unwrap();

    // The persisted payload carries the pair + the DERIVED idempotency key.
    let plan_path = dir
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.json"));
    let plan: serde_json::Value =
        serde_json::from_slice(&fs::read(&plan_path).expect("read plan")).expect("plan json");
    assert_eq!(plan["kind"], serde_json::json!("ai_authored"));
    assert_eq!(plan["payload"]["product_id"], serde_json::json!(PRODUCT_ID));
    assert_eq!(
        plan["payload"]["spec_digest"],
        serde_json::json!(SPEC_DIGEST)
    );
    let derived_key = format!("{PRODUCT_ID}@{SPEC_DIGEST}");
    assert_eq!(
        plan["payload"]["idempotency_key"],
        serde_json::json!(derived_key),
        "propose derives the attempt-aliasing fallback key"
    );

    // --- 2. the chain's fail-closed edges, via the real binary -------------
    // Bare apply → refused by the digest gate (product-bound, no flag).
    let bare = rocky(dir, &config, &["apply", &plan_id]);
    assert!(
        !bare.status.success(),
        "a bare apply of a product-bound plan must refuse"
    );
    let stderr = String::from_utf8_lossy(&bare.stderr);
    assert!(
        stderr.contains("product-bound") && stderr.contains("--expect-spec-digest"),
        "the refusal names the binding: {stderr}"
    );

    // Correct flag but NO review marker yet → refused by the review floor.
    let unreviewed = rocky(
        dir,
        &config,
        &["apply", &plan_id, "--expect-spec-digest", SPEC_DIGEST],
    );
    assert!(
        !unreviewed.status.success(),
        "an unreviewed AI-authored plan must refuse even with the right digest"
    );
    let stderr = String::from_utf8_lossy(&unreviewed.stderr);
    assert!(
        stderr.contains("has not been reviewed"),
        "the refusal is the review floor: {stderr}"
    );

    // --- 3. the real review-approve path writes the marker -----------------
    let review = rocky(dir, &config, &["review", &plan_id, "--approve"]);
    assert!(
        review.status.success(),
        "review --approve failed: {}",
        String::from_utf8_lossy(&review.stderr)
    );

    // --- 4. first apply with the expectation EXECUTES ----------------------
    let first = rocky(
        dir,
        &config,
        &["apply", &plan_id, "--expect-spec-digest", SPEC_DIGEST],
    );
    assert!(
        first.status.success(),
        "first apply failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let first_json: serde_json::Value =
        serde_json::from_slice(&first.stdout).expect("first apply emits RunOutput JSON");
    // `RunStatus` serializes in its derived PascalCase form on the wire.
    assert_ne!(
        first_json["status"],
        serde_json::json!("SkippedIdempotent"),
        "the FIRST apply must actually execute, not skip: {first_json}"
    );
    assert_eq!(
        first_json["idempotency_key"],
        serde_json::json!(derived_key),
        "the run echoes the derived key it executed under"
    );

    // The model materialized in the warehouse.
    {
        let conn = duckdb::Connection::open(dir.join("fixture.duckdb")).expect("reopen duckdb");
        let count: i64 = conn
            .query_row(
                "SELECT count(*) FROM information_schema.tables
                 WHERE table_schema = 'main' AND table_name = 'orders_mart'",
                [],
                |row| row.get(0),
            )
            .expect("query table");
        assert_eq!(
            count, 1,
            "the first apply materialized fixture.main.orders_mart"
        );
    }

    // --- 5. the second identical apply reports the idempotent skip ---------
    let second = rocky(
        dir,
        &config,
        &["apply", &plan_id, "--expect-spec-digest", SPEC_DIGEST],
    );
    assert!(
        second.status.success(),
        "second apply failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let second_json: serde_json::Value =
        serde_json::from_slice(&second.stdout).expect("second apply emits RunOutput JSON");
    assert_eq!(
        second_json["status"],
        serde_json::json!("SkippedIdempotent"),
        "the second identical apply is deflected by the derived key: {second_json}"
    );
    assert_eq!(
        second_json["idempotency_key"],
        serde_json::json!(derived_key),
        "the skip names the deflecting key"
    );
    assert!(
        second_json["skipped_by_run_id"].is_string(),
        "the skip names the prior run that holds the receipt: {second_json}"
    );
}
