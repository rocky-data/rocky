//! Propose wire-parity goldens — one per outcome arm.
//!
//! The `propose` tool's wire envelopes are a consumed contract (harnesses
//! parse `plan_id`, `code`, `policy_rule`, the handoff fields, and the
//! prose). These three tests pin the COMPLETE `tools/call` result for
//! each arm — success (plan written), `policy_review_required` (plan
//! written, human review pending), and `policy_denied` (nothing written)
//! — against fixtures captured BEFORE the propose sequence was extracted
//! into `rocky_cli::commands::propose_governed_run_plan`, so the
//! refactor is provably wire-neutral byte for byte.
//!
//! The captured project is fully deterministic: the plan id is
//! blake3 over the plan payload, the payload is derived from one fixed
//! model, no envelope field carries a timestamp, and the adapter path is
//! RELATIVE — the embedded config identity hashes the config as written,
//! so a tempdir-absolute path would change the plan id on every run.
//! Propose never connects to the adapter (it compiles only), so the
//! relative path is never resolved. Regenerate
//! deliberately with `ROCKY_BLESS_WIRE=1 cargo test -p rocky-mcp --test
//! wire_parity` and review the diff like any golden change.

use std::path::Path;

use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rocky_mcp::RockyMcpServer;
use tempfile::TempDir;

/// The same minimal DuckDB project the roundtrip suite drives, with a
/// deterministic (relative) adapter path.
fn write_project(dir: &Path, policy: &str) {
    std::fs::create_dir_all(dir.join("models")).unwrap();
    std::fs::write(
        dir.join("rocky.toml"),
        format!(
            r#"[adapter]
type = "duckdb"
path = "test.duckdb"

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
{policy}"#
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

async fn propose_wire(policy: &str) -> serde_json::Value {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), policy);
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Ok(svc) = server.serve(server_io).await {
            let _ = svc.waiting().await;
        }
    });
    let client = ().serve(client_io).await.expect("client connects");
    let result = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose returns a result envelope");
    client.cancel().await.unwrap();
    serde_json::to_value(&result).expect("the wire result serializes")
}

/// Compare a captured envelope against its committed fixture, or bless a
/// new fixture under `ROCKY_BLESS_WIRE=1`.
fn assert_matches_fixture(name: &str, captured: &serde_json::Value) {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name);
    let rendered = serde_json::to_string_pretty(captured).expect("render") + "\n";
    if std::env::var_os("ROCKY_BLESS_WIRE").is_some() {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &rendered).unwrap();
        eprintln!("blessed {}", path.display());
        return;
    }
    let expected = std::fs::read_to_string(&path).unwrap_or_else(|_| {
        panic!(
            "missing wire fixture {} — capture it deliberately with \
             ROCKY_BLESS_WIRE=1 and review the diff",
            path.display()
        )
    });
    assert_eq!(
        rendered, expected,
        "the propose wire envelope drifted from the pre-refactor golden {name}"
    );
}

#[tokio::test]
async fn propose_written_arm_wire_envelope_is_pinned() {
    let wire = propose_wire("").await;
    assert_matches_fixture("propose_wire_written.json", &wire);
}

#[tokio::test]
async fn propose_review_required_arm_wire_envelope_is_pinned() {
    let wire = propose_wire(
        r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "require_review"
"#,
    )
    .await;
    assert_matches_fixture("propose_wire_review_required.json", &wire);
}

#[tokio::test]
async fn propose_denied_arm_wire_envelope_is_pinned() {
    let wire = propose_wire(
        r#"
[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "deny"
"#,
    )
    .await;
    assert_matches_fixture("propose_wire_denied.json", &wire);
}
