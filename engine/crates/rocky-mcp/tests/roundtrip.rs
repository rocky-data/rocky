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
            "ai_contract",
            "ai_test",
            "audit_query",
            "breaking_change",
            "catalog",
            "compile",
            "dependents",
            "draft_check",
            "draft_contract",
            "draft_metadata",
            "draft_model",
            "drift_preview",
            "estate_brief",
            "explain_model",
            "governance_preview",
            "history",
            "inspect_schema",
            "lineage",
            "list",
            "metrics",
            "optimize",
            "pause_schedule",
            "plan_preview",
            "profile_column",
            "propose",
            "review_queue",
            "sample_rows",
            "schedule_status",
            "scorecard",
            "suggest_freshness_block",
            "test",
        ]
    );

    client.cancel().await.unwrap();
}

/// FF-WP1 golden worker-profile surface ⟦RTL-1,3⟧: `--profile worker` serves
/// EXACTLY the drafting allowlist. This vec is the profile's contract — a
/// future tool addition must consciously decide its profiles or it is
/// excluded by default (the sibling default-profile golden pins the other
/// surface).
#[tokio::test]
async fn worker_profile_tools_list_is_the_minimal_allowlist() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Worker,
    );

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
            "draft_check",
            "draft_model",
            "inspect_schema",
            "lineage",
            "list",
            "plan_preview",
            "profile_column",
            "sample_rows",
            "test",
        ],
        "the worker profile is an exhaustive allowlist — nothing else may appear"
    );

    // The prompt NAMES are served in both profiles; the workflow prompts'
    // CONTENT branches on the profile (worker variants end at the runner
    // handoff — pinned by `worker_profile_prompts_end_at_the_runner_handoff`).
    let prompts = client.list_all_prompts().await.expect("list prompts");
    let mut prompt_names: Vec<String> = prompts.iter().map(|p| p.name.clone()).collect();
    prompt_names.sort();
    assert_eq!(
        prompt_names,
        vec![
            "add_tests_to_pks",
            "build_model",
            "find_untested_models",
            "fix_failing_test",
            "summarize_project",
        ],
        "the worker profile keeps the full prompt-name set"
    );

    client.cancel().await.unwrap();
}

/// The adversarial half: a worker-profile session CALLING an excluded tool
/// gets rmcp's tool-not-found error — the route is absent, not merely
/// unlisted — while an allowlisted tool still runs.
#[tokio::test]
async fn worker_profile_calling_an_excluded_tool_is_tool_not_found() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Worker,
    );
    let client = connect(server).await;

    for excluded in [
        "propose",
        "draft_contract",
        "draft_metadata",
        "review_queue",
        "pause_schedule",
        "estate_brief",
        "audit_query",
    ] {
        let err = client
            .call_tool(CallToolRequestParams::new(excluded))
            .await
            .expect_err(&format!(
                "calling excluded tool '{excluded}' must be a protocol error"
            ));
        assert!(
            err.to_string().contains("tool not found"),
            "'{excluded}' must be tool-not-found, got: {err}"
        );
    }

    // Control: an allowlisted tool still routes and runs.
    let compile = client
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile is allowlisted and must run under the worker profile");
    assert_ne!(compile.is_error, Some(true), "compile runs");

    client.cancel().await.unwrap();
}

/// Malformed tool arguments (wrong-typed or missing required fields) fail
/// *inside rmcp's parameter extraction*, before the tool body runs. rmcp 2.x
/// surfaces that as an `isError` tool result carrying a plain-text message —
/// deliberately NOT Rocky's structured `{code, message, remediation_hint}`
/// envelope, which is the tool layer's own validation of *well-typed* arguments
/// (see the error-contract eval). This pins that boundary so a future rmcp bump
/// can't silently turn a bad-argument call into a transport error or reshape it.
#[tokio::test]
async fn malformed_arguments_are_a_plain_is_error_not_the_structured_envelope() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // `lineage` requires `model: String`; pass a number so deserialization into
    // the parameter struct fails before the tool ever runs.
    let bad_type = serde_json::json!({ "model": 12345 })
        .as_object()
        .unwrap()
        .clone();
    let res = client
        .call_tool(CallToolRequestParams::new("lineage").with_arguments(bad_type))
        .await
        .expect("a malformed call still returns a tool result, not a transport error");
    assert_eq!(
        res.is_error,
        Some(true),
        "malformed arguments surface as an isError tool result"
    );
    assert!(
        res.structured_content.is_none(),
        "a deserialization failure is a plain rmcp error, not Rocky's structured envelope"
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

/// FF-WP1: a propose carrying the product pair binds the identity into the
/// hashed plan payload, derives the documented idempotency-key fallback, gets
/// a DIFFERENT plan_id than the identical propose without the pair, and echoes
/// the pair in the result.
#[tokio::test]
async fn propose_with_product_fields_binds_identity_and_derives_key() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Baseline: a bare propose (no product identity).
    let bare = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("bare propose");
    let bare_plan_id = bare.structured_content.expect("result")["plan_id"]
        .as_str()
        .unwrap()
        .to_string();

    // Product-bound propose.
    let args = serde_json::json!({
        "product_id": "product:revenue_daily",
        "spec_digest": "sha256:abc123",
    })
    .as_object()
    .unwrap()
    .clone();
    let bound = client
        .call_tool(CallToolRequestParams::new("propose").with_arguments(args))
        .await
        .expect("product propose");
    assert_ne!(bound.is_error, Some(true), "product propose succeeds");
    let sc = bound.structured_content.expect("structured content");
    let plan_id = sc["plan_id"].as_str().unwrap();
    assert_ne!(
        plan_id, bare_plan_id,
        "the product identity is part of the hashed payload, so the id moves"
    );
    assert_eq!(sc["product_id"], serde_json::json!("product:revenue_daily"));
    assert_eq!(sc["spec_digest"], serde_json::json!("sha256:abc123"));

    // The persisted payload carries the pair + the derived idempotency key.
    let plan_path = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.json"));
    let plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&plan_path).unwrap()).unwrap();
    assert_eq!(
        plan["payload"]["product_id"],
        serde_json::json!("product:revenue_daily")
    );
    assert_eq!(
        plan["payload"]["spec_digest"],
        serde_json::json!("sha256:abc123")
    );
    assert_eq!(
        plan["payload"]["idempotency_key"],
        serde_json::json!("product:revenue_daily@sha256:abc123"),
        "absent a runner key, the engine derives the attempt-aliasing fallback"
    );
    // The bare plan carries none of them.
    let bare_path = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{bare_plan_id}.json"));
    let bare_plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&bare_path).unwrap()).unwrap();
    assert!(bare_plan["payload"].get("product_id").is_none());
    assert!(bare_plan["payload"].get("idempotency_key").is_none());

    client.cancel().await.unwrap();
}

/// FF-WP1: the `test` tool accepts an optional `model` scope; an unknown
/// model is the stable `model_not_found` taxonomy, and the unscoped call is
/// unchanged.
#[tokio::test]
async fn test_tool_scopes_to_a_model_and_refuses_unknown() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Unscoped: unchanged behavior.
    let all = client
        .call_tool(CallToolRequestParams::new("test"))
        .await
        .expect("unscoped test call");
    assert_ne!(all.is_error, Some(true));

    // Scoped to the real model: runs (it declares no tests — 0/0 is fine).
    let scoped = client
        .call_tool(
            CallToolRequestParams::new("test").with_arguments(
                serde_json::json!({ "model": "orders" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("scoped test call");
    assert_ne!(scoped.is_error, Some(true), "a known model scope runs");

    // Unknown model: the stable model_not_found envelope.
    let missing = client
        .call_tool(
            CallToolRequestParams::new("test").with_arguments(
                serde_json::json!({ "model": "nope" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("unknown-model test call returns a tool result");
    assert_eq!(missing.is_error, Some(true));
    let err = missing.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("model_not_found"), "{err:?}");

    client.cancel().await.unwrap();
}

/// FF-WP1: `review_queue` filters the listing by `product_id` via
/// integrity-checked plan reads — and a pending plan whose file no longer
/// passes its integrity check surfaces as a WARNING entry, never a silent
/// drop.
#[tokio::test]
async fn review_queue_product_filter_reads_plans_and_surfaces_corruption() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "require_review"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Two pending escalations: one product-bound, one bare.
    let bound = client
        .call_tool(
            CallToolRequestParams::new("propose").with_arguments(
                serde_json::json!({
                    "product_id": "product:revenue_daily",
                    "spec_digest": "sha256:abc",
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("bound propose");
    let bound_err = bound.structured_content.expect("review-required envelope");
    // FF-WP1 fix round (finding 4): the recorded plan reference is TYPED —
    // `plan_id` + the product binding ride as envelope fields, so the runner
    // reads them structurally instead of scraping prose.
    assert_eq!(
        bound_err["code"],
        serde_json::json!("policy_review_required")
    );
    let bound_plan_id = bound_err["plan_id"]
        .as_str()
        .expect("the recorded plan_id is a typed envelope field")
        .to_string();
    assert_eq!(
        bound_plan_id.len(),
        64,
        "the typed plan_id is the 64-char blake3 id"
    );
    assert_eq!(
        bound_err["product_id"],
        serde_json::json!("product:revenue_daily"),
        "the product binding rides typed on the handoff: {bound_err:?}"
    );
    assert_eq!(bound_err["spec_digest"], serde_json::json!("sha256:abc"));
    let bare = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("bare propose");
    let bare_err = bare.structured_content.expect("review-required envelope");
    let bare_plan_id = bare_err["plan_id"]
        .as_str()
        .expect("the recorded plan_id is a typed envelope field")
        .to_string();
    assert!(
        bare_err.get("product_id").is_none() || bare_err["product_id"].is_null(),
        "an unbound propose carries no product fields on the handoff: {bare_err:?}"
    );
    assert_ne!(bound_plan_id, bare_plan_id);

    // Unfiltered: both pending.
    let unfiltered = client
        .call_tool(CallToolRequestParams::new("review_queue"))
        .await
        .expect("queue list");
    let sc = unfiltered.structured_content.expect("result");
    assert_eq!(sc["total"], serde_json::json!(2), "{sc:?}");

    // Filtered: exactly the product-bound plan.
    let filtered = client
        .call_tool(
            CallToolRequestParams::new("review_queue").with_arguments(
                serde_json::json!({ "product_id": "product:revenue_daily" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("filtered queue list");
    let sc = filtered.structured_content.expect("result");
    assert_eq!(sc["total"], serde_json::json!(1), "{sc:?}");
    assert_eq!(
        sc["pending"][0]["plan_id"],
        serde_json::json!(bound_plan_id)
    );

    // Corrupt the BARE plan's payload on disk (its integrity re-hash now
    // fails). The filter cannot classify it, so it must surface as a
    // warning entry — not vanish.
    let bare_path = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{bare_plan_id}.json"));
    let mut plan_json: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&bare_path).unwrap()).unwrap();
    plan_json["payload"]["model"] = serde_json::json!("tampered");
    std::fs::write(&bare_path, serde_json::to_vec_pretty(&plan_json).unwrap()).unwrap();

    let filtered = client
        .call_tool(
            CallToolRequestParams::new("review_queue").with_arguments(
                serde_json::json!({ "product_id": "product:revenue_daily" })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .expect("filtered queue list over a corrupt plan");
    let sc = filtered.structured_content.expect("result");
    assert_eq!(
        sc["total"],
        serde_json::json!(2),
        "match + warning — the corrupt plan is counted, not dropped: {sc:?}"
    );
    let pending = sc["pending"].as_array().unwrap();
    let warning = pending
        .iter()
        .find(|e| e["plan_id"] == serde_json::json!(bare_plan_id))
        .expect("the corrupt plan surfaces");
    assert!(
        warning["warning"]
            .as_str()
            .unwrap()
            .contains("could not be read"),
        "the entry says WHY it is unclassifiable: {warning:?}"
    );

    // The filter is list-only: combining it with an approve is refused.
    let combined = client
        .call_tool(
            CallToolRequestParams::new("review_queue").with_arguments(
                serde_json::json!({
                    "product_id": "product:revenue_daily",
                    "approve_plan_id": bound_plan_id,
                    "confirm": true,
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .expect("combined call returns a result");
    assert_eq!(combined.is_error, Some(true));
    let err = combined.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));

    client.cancel().await.unwrap();
}

/// FF-WP1: a runner-supplied idempotency key WINS over the derived fallback.
#[tokio::test]
async fn propose_runner_supplied_idempotency_key_wins() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({
        "product_id": "product:revenue_daily",
        "spec_digest": "sha256:abc123",
        "idempotency_key": "product:revenue_daily@sha256:abc123@seq-7",
    })
    .as_object()
    .unwrap()
    .clone();
    let res = client
        .call_tool(CallToolRequestParams::new("propose").with_arguments(args))
        .await
        .expect("propose");
    let plan_id = res.structured_content.expect("result")["plan_id"]
        .as_str()
        .unwrap()
        .to_string();
    let plan: serde_json::Value = serde_json::from_slice(
        &std::fs::read(
            dir.path()
                .join(".rocky")
                .join("plans")
                .join(format!("{plan_id}.json")),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(
        plan["payload"]["idempotency_key"],
        serde_json::json!("product:revenue_daily@sha256:abc123@seq-7"),
        "the runner's per-attempt key must not be replaced by the derived fallback"
    );

    client.cancel().await.unwrap();
}

/// FF-WP1 ⟦RTL-4⟧: exactly one product field (or an empty one) is an
/// `invalid_argument` refusal, and no plan is written.
#[tokio::test]
async fn propose_with_partial_product_identity_is_refused() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    for args in [
        serde_json::json!({ "product_id": "product:revenue_daily" }),
        serde_json::json!({ "spec_digest": "sha256:abc123" }),
        serde_json::json!({ "product_id": "  ", "spec_digest": "sha256:abc123" }),
    ] {
        let res = client
            .call_tool(
                CallToolRequestParams::new("propose")
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
            .expect("refusal is a tool result, not a transport error");
        assert_eq!(res.is_error, Some(true), "partial identity must refuse");
        let err = res.structured_content.expect("structured envelope");
        assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    }
    assert!(
        plan_files(dir.path()).is_empty(),
        "a refused propose must not write a plan file"
    );

    client.cancel().await.unwrap();
}

/// Like [`write_project`] but appends a `[policy]` block so the `propose`
/// agent-policy gate is exercised. The single `orders` model is unclassified
/// and uncontracted, so an `{ any = true }` rule is the deterministic lever.
fn write_project_with_policy(dir: &Path, db_path: &Path, policy: &str) {
    write_project(dir, db_path);
    let cfg_path = dir.join("rocky.toml");
    let mut cfg = std::fs::read_to_string(&cfg_path).unwrap();
    cfg.push('\n');
    cfg.push_str(policy);
    std::fs::write(&cfg_path, cfg).unwrap();
}

/// Whether `.rocky/plans` holds any persisted plan `*.json`.
fn plan_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let plans_dir = dir.join(".rocky").join("plans");
    let Ok(entries) = std::fs::read_dir(&plans_dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect()
}

/// The load-bearing policy-gate proof, creds-free and deterministic: a
/// `[policy]` block that denies an agent apply makes `propose` return a
/// parseable structured deny — `{code = "policy_denied", policy_rule, message,
/// remediation_hint}` — naming the deciding rule. A deny does **not** persist
/// the plan, and the decision is recorded in the audit ledger.
#[tokio::test]
async fn propose_denied_by_policy_returns_structured_error() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "deny"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose returns a result");

    // Structured deny, over the wire: is_error + a parseable envelope.
    assert_eq!(result.is_error, Some(true), "a denied propose is an error");
    let err = result
        .structured_content
        .expect("a denied propose carries the structured error envelope");
    assert_eq!(err["code"], serde_json::json!("policy_denied"));
    assert_eq!(
        err["policy_rule"],
        serde_json::json!("0"),
        "the envelope names the deciding rule: {err:?}"
    );
    let message = err["message"].as_str().unwrap();
    assert!(
        message.contains("orders"),
        "message names the denied model: {message}"
    );
    let hint = err["remediation_hint"].as_str().unwrap();
    assert!(
        hint.contains("branch") || hint.contains("Re-scope"),
        "remediation_hint points at a reroute: {hint}"
    );

    // A deny must NOT persist the plan — the decision is reserved for a human.
    assert!(
        plan_files(dir.path()).is_empty(),
        "a denied propose must not write a plan file"
    );

    // The decision IS recorded in the audit ledger (queryable via `rocky audit`).
    client.cancel().await.unwrap();
    let state_path = rocky_core::state::resolve_state_path(None, &dir.path().join("models")).path;
    let store = rocky_core::state::StateStore::open(&state_path).expect("open ledger");
    let decisions = store.list_policy_decisions().expect("list decisions");
    assert!(
        decisions
            .iter()
            .any(|d| d.model == "orders" && d.effect == rocky_core::config::PolicyEffect::Deny),
        "the propose-time deny is recorded in the ledger: {decisions:?}"
    );
}

/// A `require_review` verdict at propose time still **persists** the plan (it is
/// headed to human review) and returns a structured `policy_review_required`
/// signal naming the rule and the recorded plan_id, so the agent surfaces the
/// review/apply path to the user instead of applying autonomously.
#[tokio::test]
async fn propose_require_review_persists_plan_and_signals() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "require_review"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose returns a result");

    assert_eq!(
        result.is_error,
        Some(true),
        "require_review returns a structured signal the agent parses"
    );
    let err = result.structured_content.expect("structured envelope");
    assert_eq!(err["code"], serde_json::json!("policy_review_required"));
    assert_eq!(err["policy_rule"], serde_json::json!("0"));
    let message = err["message"].as_str().unwrap();
    let hint = err["remediation_hint"].as_str().unwrap();
    assert!(
        hint.contains("rocky review") && hint.contains("--approve"),
        "remediation_hint points at the human review path: {hint}"
    );

    // require_review DOES persist the plan — it is on its way to a reviewer.
    let plans = plan_files(dir.path());
    assert_eq!(plans.len(), 1, "the plan was recorded for review");
    let plan: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&plans[0]).unwrap()).unwrap();
    assert_eq!(plan["kind"], serde_json::json!("ai_authored"));

    // The recorded plan_id is surfaced to the agent so it can name it to the user.
    let plan_id = plans[0].file_stem().unwrap().to_str().unwrap();
    assert!(
        message.contains(plan_id) || hint.contains(plan_id),
        "the recorded plan_id is surfaced: message={message} hint={hint}"
    );

    client.cancel().await.unwrap();
}

/// End-to-end reachability for the four governor tools over the real stdio
/// server: an agent `propose` under a `require_review` policy plants one
/// escalation in the ledger, then every governor projection surfaces it with
/// citations, the scorecard matches hand-computed truth, and the `review_queue`
/// approve action is gated on an explicit confirmation before it writes the
/// sign-off marker that unblocks `rocky apply`.
#[tokio::test]
async fn governor_tools_surface_escalation_and_gate_the_approve() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { any = true }
effect = "require_review"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // An agent proposes → recorded as a require_review escalation in the ledger,
    // and the AI-authored plan is persisted for a reviewer.
    let _ = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose returns a result");
    let plans = plan_files(dir.path());
    assert_eq!(plans.len(), 1, "the propose persisted one plan for review");
    let plan_id = plans[0].file_stem().unwrap().to_str().unwrap().to_string();

    // review_queue (read) lists the escalation, cited.
    let queue = client
        .call_tool(CallToolRequestParams::new("review_queue"))
        .await
        .expect("review_queue call");
    assert_ne!(
        queue.is_error,
        Some(true),
        "listing the queue is not an error"
    );
    let sc = queue.structured_content.expect("queue structured content");
    assert_eq!(sc["total"], serde_json::json!(1));
    let pending = sc["pending"].as_array().unwrap();
    assert_eq!(pending.len(), 1, "one escalation awaits review");
    assert_eq!(pending[0]["plan_id"], serde_json::json!(plan_id));
    assert_eq!(pending[0]["principal"], serde_json::json!("agent"));
    assert!(
        pending[0]["decision_ref"]
            .as_str()
            .unwrap()
            .contains(&plan_id),
        "the queue entry carries a ledger citation naming the plan"
    );
    assert!(
        pending[0]["approve_command"]
            .as_str()
            .unwrap()
            .contains(&plan_id)
    );

    // Approving a plan that is NOT in the pending queue is refused up front —
    // the approve action only clears genuinely-pending escalations.
    let bogus = serde_json::json!({ "approve_plan_id": "0".repeat(64), "confirm": true })
        .as_object()
        .unwrap()
        .clone();
    let bogus_res = client
        .call_tool(CallToolRequestParams::new("review_queue").with_arguments(bogus))
        .await
        .expect("review_queue bogus approve returns a result");
    assert_eq!(bogus_res.is_error, Some(true));
    assert_eq!(
        bogus_res.structured_content.expect("error envelope")["code"],
        serde_json::json!("invalid_argument"),
        "approving a non-pending plan is rejected even with confirm=true"
    );

    // estate_brief surfaces the escalation + agent activity, cited.
    let brief = client
        .call_tool(CallToolRequestParams::new("estate_brief"))
        .await
        .expect("estate_brief call");
    assert_ne!(brief.is_error, Some(true));
    let bc = brief.structured_content.expect("brief structured content");
    assert_eq!(
        bc["agent_activity"]["require_review"],
        serde_json::json!(1),
        "the brief counts the agent escalation"
    );
    let escalations = bc["escalations"]["pending"].as_array().unwrap();
    assert!(
        escalations
            .iter()
            .any(|e| e["plan_id"] == serde_json::json!(plan_id)),
        "the brief's escalations name the pending plan: {bc:?}"
    );

    // audit_query --for <plan_id> returns the resolved custody chain.
    let audit_args = serde_json::json!({ "subject": plan_id })
        .as_object()
        .unwrap()
        .clone();
    let audit = client
        .call_tool(CallToolRequestParams::new("audit_query").with_arguments(audit_args))
        .await
        .expect("audit_query call");
    let ac = audit.structured_content.expect("audit structured content");
    assert_eq!(ac["subject_kind"], serde_json::json!("plan"));
    assert_eq!(ac["resolved"], serde_json::json!(true));
    assert!(
        ac["decisions"]["total"].as_u64().unwrap() >= 1,
        "the custody chain carries the governing decision: {ac:?}"
    );

    // scorecard --by principal matches hand truth: 1 agent decision, all review.
    let scorecard_args = serde_json::json!({ "by": "principal" })
        .as_object()
        .unwrap()
        .clone();
    let scorecard = client
        .call_tool(CallToolRequestParams::new("scorecard").with_arguments(scorecard_args))
        .await
        .expect("scorecard call");
    let sco = scorecard.structured_content.expect("scorecard content");
    assert_eq!(sco["by"], serde_json::json!("principal"));
    assert_eq!(sco["total_decisions"], serde_json::json!(1));
    let groups = sco["groups"].as_array().unwrap();
    let agent = groups
        .iter()
        .find(|g| g["key"] == serde_json::json!("agent"))
        .expect("an agent group in the scorecard");
    assert_eq!(agent["total"], serde_json::json!(1));
    assert_eq!(agent["require_review"], serde_json::json!(1));
    assert_eq!(agent["allow"], serde_json::json!(0));
    assert_eq!(agent["deny"], serde_json::json!(0));
    assert!(
        (agent["review_rate"].as_f64().unwrap() - 1.0).abs() < 1e-9,
        "escalation rate is 1.0: {agent:?}"
    );

    // review_queue approve WITHOUT confirm → the gate refuses.
    let approve_no_confirm = serde_json::json!({ "approve_plan_id": plan_id })
        .as_object()
        .unwrap()
        .clone();
    let refused = client
        .call_tool(CallToolRequestParams::new("review_queue").with_arguments(approve_no_confirm))
        .await
        .expect("review_queue approve (no confirm) returns a result");
    assert_eq!(
        refused.is_error,
        Some(true),
        "an unconfirmed approve is refused"
    );
    let err = refused
        .structured_content
        .expect("structured error envelope");
    assert_eq!(err["code"], serde_json::json!("policy_review_required"));

    // No sign-off marker before confirmation — the gate held.
    let marker = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.reviewed.json"));
    assert!(!marker.exists(), "no sign-off marker before confirmation");

    // review_queue approve WITH confirm=true → writes the marker, attributed.
    let approve = serde_json::json!({ "approve_plan_id": plan_id, "confirm": true })
        .as_object()
        .unwrap()
        .clone();
    let approved = client
        .call_tool(CallToolRequestParams::new("review_queue").with_arguments(approve))
        .await
        .expect("review_queue approve call");
    assert_ne!(
        approved.is_error,
        Some(true),
        "a confirmed approve succeeds: {approved:?}"
    );
    let ap = approved.structured_content.expect("approval content");
    assert_eq!(ap["approval"]["marker_written"], serde_json::json!(true));
    assert_eq!(ap["approval"]["plan_id"], serde_json::json!(plan_id));
    assert!(
        ap["approval"]["attribution"]
            .as_str()
            .unwrap()
            .contains("git identity"),
        "the approval is honest that attribution is the operator's git identity"
    );
    assert_eq!(
        ap["total"],
        serde_json::json!(0),
        "the approved escalation is cleared from the re-listed queue"
    );

    // The sign-off marker that unblocks `rocky apply` now exists on disk.
    assert!(
        marker.exists(),
        "the confirmed approve wrote the sign-off marker"
    );

    client.cancel().await.unwrap();
}

/// Write a `models/_defaults.toml` supplying the target catalog/schema, so a
/// drafted `<name>.sql` + intent-only sidecar resolves its target from project
/// conventions (matching the real fixture convention) and compiles.
fn write_target_defaults(dir: &Path) {
    std::fs::write(
        dir.join("models").join("_defaults.toml"),
        "[target]\ncatalog = \"warehouse\"\nschema = \"out\"\n",
    )
    .unwrap();
}

fn draft_args(name: &str, sql: &str, intent: &str) -> serde_json::Map<String, serde_json::Value> {
    serde_json::json!({ "name": name, "sql": sql, "intent": intent })
        .as_object()
        .unwrap()
        .clone()
}

/// The happy path: `draft_model` writes the SQL + a sidecar carrying the intent,
/// compiles in the same call, returns the diagnostics, and reminds the flow.
#[tokio::test]
async fn draft_model_writes_compiles_and_reminds_flow() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    write_target_defaults(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "daily_revenue",
                "SELECT 1 AS id, 100 AS revenue",
                "Daily revenue rollup for the demo",
            )),
        )
        .await
        .expect("draft_model call");

    assert_ne!(result.is_error, Some(true), "a valid draft is not an error");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["model"], serde_json::json!("daily_revenue"));
    assert_eq!(sc["has_errors"], serde_json::json!(false));
    assert_eq!(
        sc["sql_path"],
        serde_json::json!("models/daily_revenue.sql")
    );
    assert!(
        sc["diagnostics"].is_array(),
        "diagnostics returned with the write"
    );
    let next = sc["next_steps"].as_str().expect("next_steps");
    assert!(
        next.contains("propose") && next.contains("review") && next.contains("Never apply"),
        "the response reminds the compile -> plan -> propose -> review flow: {next}"
    );

    // The draft landed on disk: SQL body + a sidecar carrying the intent.
    let sql = dir.path().join("models").join("daily_revenue.sql");
    let sidecar = dir.path().join("models").join("daily_revenue.toml");
    assert!(sql.is_file(), "draft SQL written");
    assert!(sidecar.is_file(), "sidecar written");
    let sidecar_text = std::fs::read_to_string(&sidecar).unwrap();
    assert!(
        sidecar_text.contains("Daily revenue rollup for the demo"),
        "sidecar carries the intent: {sidecar_text}"
    );

    client.cancel().await.unwrap();
}

/// The path guard: a name that would escape the models directory is refused with
/// a structured `invalid_argument` envelope and writes nothing.
#[tokio::test]
async fn draft_model_refuses_path_escaping_name() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    for bad in [
        "../evil",
        "/etc/passwd",
        "sub/model",
        "..\\win",
        "revenue.sql",
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                    bad,
                    "SELECT 1 AS id",
                    "x",
                )),
            )
            .await
            .expect("draft_model call");
        assert_eq!(
            result.is_error,
            Some(true),
            "path-escaping name '{bad}' must be refused"
        );
        let err = result.structured_content.expect("envelope");
        assert_eq!(
            err["code"],
            serde_json::json!("invalid_argument"),
            "name '{bad}'"
        );
    }

    // Nothing was written for any refused name — the models dir still holds only
    // the fixture's `orders` model, and no `evil` file escaped anywhere.
    let mut sql_files: Vec<String> = std::fs::read_dir(dir.path().join("models"))
        .unwrap()
        .filter_map(Result::ok)
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.ends_with(".sql"))
        .collect();
    sql_files.sort();
    assert_eq!(
        sql_files,
        vec!["orders.sql".to_string()],
        "no draft written for a refused name"
    );
    assert!(
        !dir.path().parent().unwrap().join("evil.sql").exists(),
        "no file escaped the models directory"
    );

    client.cancel().await.unwrap();
}

/// THE PIN: a policy-DENIED draft returns the structured `policy_denied`
/// envelope AND leaves no file on disk — the deny rolls the write back, exactly
/// as the propose gate's deny writes no plan. The decision is still recorded in
/// the audit ledger.
#[tokio::test]
async fn draft_model_denied_by_policy_leaves_no_file() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "deny"
"#,
    );
    write_target_defaults(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "shadow",
                "SELECT 1 AS id",
                "a draft the policy denies",
            )),
        )
        .await
        .expect("draft_model returns a result");

    // Structured deny over the wire.
    assert_eq!(result.is_error, Some(true), "a denied draft is an error");
    let err = result
        .structured_content
        .expect("a denied draft carries the structured error envelope");
    assert_eq!(err["code"], serde_json::json!("policy_denied"));
    assert_eq!(
        err["policy_rule"],
        serde_json::json!("0"),
        "the envelope names the deciding rule: {err:?}"
    );
    let hint = err["remediation_hint"].as_str().unwrap();
    assert!(
        hint.contains("Re-scope") || hint.contains("different"),
        "remediation_hint points at a reroute: {hint}"
    );

    // THE PIN: a denied draft leaves NO file on disk.
    assert!(
        !dir.path().join("models").join("shadow.sql").exists(),
        "a denied draft must not leave the .sql on disk"
    );
    assert!(
        !dir.path().join("models").join("shadow.toml").exists(),
        "a denied draft must not leave the sidecar on disk"
    );

    // The decision IS recorded in the audit ledger (the trail survives the
    // rollback), mirroring the propose gate's deny.
    client.cancel().await.unwrap();
    let state_path = rocky_core::state::resolve_state_path(None, &dir.path().join("models")).path;
    let store = rocky_core::state::StateStore::open(&state_path).expect("open ledger");
    let decisions = store.list_policy_decisions().expect("list decisions");
    assert!(
        decisions
            .iter()
            .any(|d| d.model == "shadow" && d.effect == rocky_core::config::PolicyEffect::Deny),
        "the denied draft is recorded in the ledger: {decisions:?}"
    );
}

/// A `require_review` verdict PERSISTS the draft (it is the reviewable artifact,
/// mirroring the propose gate) and returns a structured `policy_review_required`
/// signal that routes the agent to human review.
#[tokio::test]
async fn draft_model_require_review_keeps_file_and_signals() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "require_review"
"#,
    );
    write_target_defaults(dir.path());
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "reviewed",
                "SELECT 1 AS id",
                "a draft that needs review",
            )),
        )
        .await
        .expect("draft_model returns a result");

    assert_eq!(
        result.is_error,
        Some(true),
        "require_review returns a structured signal the agent parses"
    );
    let err = result.structured_content.expect("structured envelope");
    assert_eq!(err["code"], serde_json::json!("policy_review_required"));
    assert_eq!(err["policy_rule"], serde_json::json!("0"));

    // require_review KEEPS the draft — it is on its way to a human reviewer.
    assert!(
        dir.path().join("models").join("reviewed.sql").is_file(),
        "a require_review draft persists as the reviewable artifact"
    );
    assert!(
        dir.path().join("models").join("reviewed.toml").is_file(),
        "the sidecar persists too"
    );

    client.cancel().await.unwrap();
}

/// A sidecar for the `orders` fixture carrying the spec-owned metadata a
/// worker must never be able to erase: a PII classification, a freshness
/// block, a test, tags, and explicit strategy/target.
const SPEC_OWNED_ORDERS_SIDECAR: &str = r#"name = "orders"
intent = "original spec-owned intent"

[tags]
layer = "gold"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "out"
table = "orders"

[classification]
status = "pii"

[freshness]
expected_lag_seconds = 3600
time_column = "id"

[[tests]]
type = "not_null"
column = "id"
"#;

/// FF-WP1 fix round (finding 2) — `draft_model` on an EXISTING model is a
/// preserve-merge: the SQL body is replaced and ONLY `name` / `intent` change
/// in the sidecar; classification, freshness, tests, tags, strategy, and
/// target all survive the redraft. Before the fix, the sidecar was replaced
/// wholesale with the minimal name+intent document.
#[tokio::test]
async fn draft_model_on_existing_model_preserves_spec_owned_metadata() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let sidecar_path = dir.path().join("models").join("orders.toml");
    std::fs::write(&sidecar_path, SPEC_OWNED_ORDERS_SIDECAR).unwrap();
    let before: toml::Table = toml::from_str(SPEC_OWNED_ORDERS_SIDECAR).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 2 AS id, 'REDRAFTED' AS status",
                "redrafted intent",
            )),
        )
        .await
        .expect("draft_model call");
    assert_ne!(
        result.is_error,
        Some(true),
        "redrafting an existing model succeeds: {:?}",
        result.structured_content
    );

    // The SQL body was replaced.
    let sql = std::fs::read_to_string(dir.path().join("models").join("orders.sql")).unwrap();
    assert!(sql.contains("REDRAFTED"), "the SQL body is the new draft");

    // The sidecar changed in EXACTLY `name` + `intent`; everything else is
    // preserved value-for-value (the merge re-serializes, so the comparison
    // is over parsed tables, not raw bytes).
    let after_text = std::fs::read_to_string(&sidecar_path).unwrap();
    let mut after: toml::Table = toml::from_str(&after_text).unwrap();
    assert_eq!(
        after.remove("name"),
        Some(toml::Value::String("orders".to_string()))
    );
    assert_eq!(
        after.remove("intent"),
        Some(toml::Value::String("redrafted intent".to_string())),
        "the intent is the redraft's"
    );
    let mut expected = before.clone();
    expected.remove("name");
    expected.remove("intent");
    assert_eq!(
        after, expected,
        "every key except name/intent is preserved exactly"
    );

    client.cancel().await.unwrap();
}

/// FF-WP1 fix round (finding 2) — an existing sidecar that does not parse as
/// TOML REFUSES the redraft (mirroring draft_metadata): spec-owned metadata
/// is never clobbered just because it is malformed. Both files stay
/// byte-identical.
#[tokio::test]
async fn draft_model_refuses_to_clobber_an_unparseable_sidecar() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let sidecar_path = dir.path().join("models").join("orders.toml");
    std::fs::write(&sidecar_path, "name = \"orders\"\n[strategy\nbroken !!").unwrap();
    let sidecar_before = std::fs::read(&sidecar_path).unwrap();
    let sql_path = dir.path().join("models").join("orders.sql");
    let sql_before = std::fs::read(&sql_path).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 2 AS id",
                "should not land",
            )),
        )
        .await
        .expect("draft_model returns a result");
    assert_eq!(
        result.is_error,
        Some(true),
        "an unparseable sidecar refuses"
    );
    let err = result.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["message"]
            .as_str()
            .unwrap()
            .contains("models/orders.toml"),
        "the error names the sidecar: {err:?}"
    );

    assert_eq!(
        std::fs::read(&sidecar_path).unwrap(),
        sidecar_before,
        "the unparseable sidecar is byte-identical"
    );
    assert_eq!(
        std::fs::read(&sql_path).unwrap(),
        sql_before,
        "the SQL body is untouched too"
    );

    client.cancel().await.unwrap();
}

/// FF-WP1 fix round 2 (item 2) — an EXISTS-but-unreadable sidecar refuses the
/// redraft instead of being treated as a NEW model. The rollback snapshot
/// converts read errors to "absent"; before the guard, the draft would
/// overwrite the sidecar's spec-owned metadata and a policy-denied rollback
/// would DELETE the file. The refusal names the path and the file survives
/// untouched.
#[cfg(unix)]
#[tokio::test]
async fn draft_model_refuses_an_existing_but_unreadable_sidecar() {
    use std::os::unix::fs::PermissionsExt;

    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let sidecar_path = dir.path().join("models").join("orders.toml");
    let sidecar_before = std::fs::read(&sidecar_path).unwrap();
    let sql_path = dir.path().join("models").join("orders.sql");
    let sql_before = std::fs::read(&sql_path).unwrap();

    // Make the sidecar exist-but-unreadable, restoring permissions on every
    // exit path so cleanup (and the byte comparison) can read it again.
    let readable = std::fs::Permissions::from_mode(0o644);
    std::fs::set_permissions(&sidecar_path, std::fs::Permissions::from_mode(0o000)).unwrap();
    struct RestorePerms(std::path::PathBuf, std::fs::Permissions);
    impl Drop for RestorePerms {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let _restore = RestorePerms(sidecar_path.clone(), readable);

    // Probe the condition the test needs: running as root (e.g. a container
    // CI), mode 0o000 does not make the file unreadable, so the guard under
    // test cannot fire — skip rather than assert a condition this
    // environment cannot exhibit.
    if std::fs::read(&sidecar_path).is_ok() {
        eprintln!("skipping: chmod 0o000 does not make files unreadable here (running as root?)");
        return;
    }

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 2 AS id",
                "should not land",
            )),
        )
        .await
        .expect("draft_model returns a result");
    assert_eq!(
        result.is_error,
        Some(true),
        "an existing-but-unreadable sidecar refuses"
    );
    let err = result.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    let message = err["message"].as_str().unwrap();
    assert!(
        message.contains("models/orders.toml") && message.contains("cannot be read"),
        "the error names the unreadable sidecar: {err:?}"
    );

    client.cancel().await.unwrap();

    // The file SURVIVES — neither overwritten nor deleted by a rollback —
    // and the SQL body is untouched.
    drop(_restore);
    assert_eq!(
        std::fs::read(&sidecar_path).unwrap(),
        sidecar_before,
        "the unreadable sidecar is byte-identical"
    );
    assert_eq!(
        std::fs::read(&sql_path).unwrap(),
        sql_before,
        "the SQL body is untouched too"
    );
}

/// FF-WP1 fix round (finding 2) — THE de-scope pin: with a rule denying agent
/// authorship on `classifications = ["pii"]`, redrafting a PII-classified
/// model via `draft_model` is DENIED, and the rollback restores BOTH files
/// byte-for-byte. Before the fix, the redraft replaced the sidecar with the
/// minimal name+intent document — erasing the `pii` classification the deny
/// rule matches on — and the policy evaluation (which runs post-write, on the
/// on-disk image) resolved to the default posture instead of the deny.
#[tokio::test]
async fn draft_model_redraft_of_pii_model_is_denied_and_byte_restored() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { classifications = ["pii"] }
effect = "deny"
"#,
    );
    let sidecar_path = dir.path().join("models").join("orders.toml");
    std::fs::write(&sidecar_path, SPEC_OWNED_ORDERS_SIDECAR).unwrap();
    let sidecar_before = std::fs::read(&sidecar_path).unwrap();
    let sql_path = dir.path().join("models").join("orders.sql");
    let sql_before = std::fs::read(&sql_path).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 2 AS id, 'X' AS status",
                "attempt to redraft a pii model",
            )),
        )
        .await
        .expect("draft_model returns a result");

    assert_eq!(result.is_error, Some(true), "the redraft is refused");
    let err = result.structured_content.expect("envelope");
    assert_eq!(
        err["code"],
        serde_json::json!("policy_denied"),
        "the pii-scoped DENY decides — not the default require_review: {err:?}"
    );
    assert_eq!(
        err["policy_rule"],
        serde_json::json!("0"),
        "the deciding rule is the classification-scoped deny: {err:?}"
    );

    // Byte-restore: the deny rolled back BOTH files exactly.
    assert_eq!(
        std::fs::read(&sidecar_path).unwrap(),
        sidecar_before,
        "the deny restores the prior sidecar bytes"
    );
    assert_eq!(
        std::fs::read(&sql_path).unwrap(),
        sql_before,
        "the deny restores the prior SQL bytes"
    );

    client.cancel().await.unwrap();
}

// --- draft_contract / draft_check (agent-authored write path) ---------------

/// The happy path: `draft_contract` writes the agent's `.contract.toml` next to
/// the model, compiles it against the model's inferred schema in the same call,
/// and reminds the flow. The `orders` fixture is `SELECT 1 AS id, 'COMPLETE' AS
/// status`, so a contract over `id`/`status` compiles clean.
#[tokio::test]
async fn draft_contract_writes_and_compiles() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let spec = "[[columns]]\nname = \"id\"\ntype = \"Int64\"\nnullable = true\n\n\
                [[columns]]\nname = \"status\"\ntype = \"String\"\nnullable = true\n";
    let args = serde_json::json!({ "model": "orders", "spec": spec })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_contract").with_arguments(args))
        .await
        .expect("draft_contract call");

    assert_ne!(
        result.is_error,
        Some(true),
        "a valid contract is not an error"
    );
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["model"], serde_json::json!("orders"));
    assert_eq!(sc["has_errors"], serde_json::json!(false));
    assert_eq!(
        sc["contract_path"],
        serde_json::json!("models/orders.contract.toml")
    );
    assert!(sc["diagnostics"].is_array());

    // The contract landed on disk where compile auto-discovers it.
    let contract = dir.path().join("models").join("orders.contract.toml");
    assert!(contract.is_file(), "contract written to the sibling path");
    assert!(
        std::fs::read_to_string(&contract)
            .unwrap()
            .contains("status"),
        "the agent's contract body was written verbatim"
    );

    client.cancel().await.unwrap();
}

/// A `draft_contract` call with no `spec` is a mis-dispatch to the generator: it
/// returns a structured `invalid_argument` error whose hint names `ai_contract`,
/// and writes nothing.
#[tokio::test]
async fn draft_contract_without_spec_redirects_to_ai_contract() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_contract").with_arguments(args))
        .await
        .expect("draft_contract call");

    assert_eq!(result.is_error, Some(true), "a no-spec call is an error");
    let err = result.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["remediation_hint"]
            .as_str()
            .unwrap()
            .contains("ai_contract"),
        "the redirect points at ai_contract: {err:?}"
    );
    assert!(
        !dir.path()
            .join("models")
            .join("orders.contract.toml")
            .exists(),
        "a redirected call writes nothing"
    );

    client.cancel().await.unwrap();
}

/// THE PIN: a policy-DENIED `draft_contract` returns the structured
/// `policy_denied` envelope AND leaves no contract on disk — the deny rolls the
/// write back, exactly as `draft_model`'s deny.
#[tokio::test]
async fn draft_contract_denied_by_policy_leaves_no_file() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "deny"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let spec = "[[columns]]\nname = \"id\"\ntype = \"Int64\"\nnullable = false\n";
    let args = serde_json::json!({ "model": "orders", "spec": spec })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_contract").with_arguments(args))
        .await
        .expect("draft_contract returns a result");

    assert_eq!(result.is_error, Some(true), "a denied contract is an error");
    let err = result.structured_content.expect("structured envelope");
    assert_eq!(err["code"], serde_json::json!("policy_denied"));
    assert_eq!(err["policy_rule"], serde_json::json!("0"));
    assert!(
        !dir.path()
            .join("models")
            .join("orders.contract.toml")
            .exists(),
        "a denied contract must not leave a file on disk"
    );

    client.cancel().await.unwrap();
}

/// The happy path: `draft_check` merges the agent's `[[tests]]` block into the
/// model's sidecar, compiles, and reminds the flow. The prior sidecar's
/// `name = "orders"` survives the merge.
#[tokio::test]
async fn draft_check_writes_and_compiles() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let spec = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
    let args = serde_json::json!({ "model": "orders", "spec": spec })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(args))
        .await
        .expect("draft_check call");

    assert_ne!(result.is_error, Some(true), "a valid check is not an error");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["model"], serde_json::json!("orders"));
    assert_eq!(sc["has_errors"], serde_json::json!(false));
    assert_eq!(sc["sidecar_path"], serde_json::json!("models/orders.toml"));

    let sidecar = std::fs::read_to_string(dir.path().join("models").join("orders.toml")).unwrap();
    assert!(
        sidecar.contains("[[tests]]") && sidecar.contains("not_null"),
        "the check was merged into the sidecar: {sidecar}"
    );
    assert!(
        sidecar.contains("name = \"orders\""),
        "the prior sidecar content survives the merge: {sidecar}"
    );

    client.cancel().await.unwrap();
}

/// A `draft_check` call with no `spec` is a mis-dispatch to the generator: it
/// returns a structured `invalid_argument` error whose hint names `ai_test`.
#[tokio::test]
async fn draft_check_without_spec_redirects_to_ai_test() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(args))
        .await
        .expect("draft_check call");

    assert_eq!(result.is_error, Some(true), "a no-spec call is an error");
    let err = result.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["remediation_hint"]
            .as_str()
            .unwrap()
            .contains("ai_test"),
        "the redirect points at ai_test: {err:?}"
    );

    client.cancel().await.unwrap();
}

/// THE PIN for the merge case: a policy-DENIED `draft_check` returns the
/// structured `policy_denied` envelope AND restores the model's PRIOR sidecar —
/// the check rolls back without deleting the model's `name`/intent.
#[tokio::test]
async fn draft_check_denied_by_policy_restores_prior_sidecar() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { any = true }
effect = "deny"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let before = std::fs::read_to_string(dir.path().join("models").join("orders.toml")).unwrap();
    let spec = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
    let args = serde_json::json!({ "model": "orders", "spec": spec })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(args))
        .await
        .expect("draft_check returns a result");

    assert_eq!(result.is_error, Some(true), "a denied check is an error");
    let err = result.structured_content.expect("structured envelope");
    assert_eq!(err["code"], serde_json::json!("policy_denied"));

    // The PRIOR sidecar is restored byte-for-byte — the check left nothing, and
    // the model's own name/target were not corrupted by the rolled-back merge.
    let after = std::fs::read_to_string(dir.path().join("models").join("orders.toml")).unwrap();
    assert_eq!(
        after, before,
        "a denied check restores the prior sidecar exactly"
    );
    assert!(
        !after.contains("[[tests]]"),
        "no check lingered after the deny: {after}"
    );

    client.cancel().await.unwrap();
}

/// THE PIN for the structural gate: a spec that hides a `[target]` override
/// behind a valid `[[tests]]` block is rejected as a structured
/// `invalid_argument` naming the smuggled key, and the model's sidecar is
/// untouched — the check write path cannot be used to rewrite model config.
#[tokio::test]
async fn draft_check_rejects_smuggled_sidecar_config() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let before = std::fs::read_to_string(dir.path().join("models").join("orders.toml")).unwrap();
    let spec = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n\n\
                [target]\nschema = \"prod_finance\"\n";
    let args = serde_json::json!({ "model": "orders", "spec": spec })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(args))
        .await
        .expect("draft_check call");

    assert_eq!(
        result.is_error,
        Some(true),
        "a smuggled [target] override is an error"
    );
    let err = result.structured_content.expect("structured envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["message"].as_str().unwrap().contains("target"),
        "the offending key is named: {err:?}"
    );

    // The sidecar is byte-for-byte untouched: the gate fires BEFORE the write.
    let after = std::fs::read_to_string(dir.path().join("models").join("orders.toml")).unwrap();
    assert_eq!(after, before, "a rejected spec writes nothing");

    client.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// draft_metadata (FF-WP1)
// ---------------------------------------------------------------------------

fn metadata_args(json: serde_json::Value) -> serde_json::Map<String, serde_json::Value> {
    match json {
        serde_json::Value::Object(map) => map,
        other => panic!("metadata_args needs a JSON object, got {other}"),
    }
}

/// Happy path: a structured patch merges `[freshness]` + `[classification]`
/// into the sidecar via parse-merge, preserving the existing strategy/target,
/// and the result carries the compile with the write.
#[tokio::test]
async fn draft_metadata_writes_freshness_and_classifications() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "freshness": {
                        "expected_lag_seconds": 86400,
                        "time_column": "status",
                        "severity": "error",
                    },
                    "classifications": { "status": "internal" },
                }),
            )),
        )
        .await
        .expect("draft_metadata call");

    assert_ne!(result.is_error, Some(true), "a valid patch is not an error");
    let sc = result.structured_content.expect("structured content");
    assert_eq!(sc["model"], serde_json::json!("orders"));
    assert_eq!(sc["sidecar_path"], serde_json::json!("models/orders.toml"));
    assert_eq!(sc["has_errors"], serde_json::json!(false));
    assert!(
        sc["next_steps"]
            .as_str()
            .unwrap()
            .contains("Never apply a draft directly"),
        "the reminder rides along"
    );

    // The sidecar re-parses and carries the patch AND the prior config.
    let sidecar = dir.path().join("models").join("orders.toml");
    let parsed: toml::Table = toml::from_str(&std::fs::read_to_string(&sidecar).unwrap()).unwrap();
    assert_eq!(
        parsed["freshness"]["expected_lag_seconds"],
        toml::Value::Integer(86400)
    );
    assert_eq!(
        parsed["freshness"]["time_column"],
        toml::Value::String("status".to_string())
    );
    assert_eq!(
        parsed["freshness"]["severity"],
        toml::Value::String("error".to_string())
    );
    assert_eq!(
        parsed["classification"]["status"],
        toml::Value::String("internal".to_string())
    );
    assert_eq!(
        parsed["strategy"]["type"],
        toml::Value::String("full_refresh".to_string()),
        "the pre-existing strategy survives the parse-merge"
    );
    assert_eq!(
        parsed["target"]["table"],
        toml::Value::String("orders".to_string()),
        "the pre-existing target survives the parse-merge"
    );

    client.cancel().await.unwrap();
}

/// The merge case that motivated parse-merge: a sidecar `draft_check`
/// previously string-appended `[[tests]]` blocks into still round-trips —
/// the tests survive, the patch lands, and a second freshness patch REPLACES
/// the first rather than duplicating the table. Classification merges keep
/// other columns' tags.
#[tokio::test]
async fn draft_metadata_merges_over_appended_checks_and_replaces_freshness() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Step 1: draft_check string-appends a [[tests]] block (the legacy merge).
    let check = client
        .call_tool(
            CallToolRequestParams::new("draft_check").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "spec": "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n",
                }),
            )),
        )
        .await
        .expect("draft_check call");
    assert_ne!(check.is_error, Some(true), "the check draft succeeds");

    // Step 2: a first metadata patch.
    let first = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "freshness": { "expected_lag_seconds": 3600 },
                    "classifications": { "id": "internal" },
                }),
            )),
        )
        .await
        .expect("draft_metadata call");
    assert_ne!(first.is_error, Some(true), "the first patch succeeds");

    // Step 3: a second patch replaces [freshness] and merges a NEW column tag.
    let second = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "freshness": { "expected_lag_seconds": 7200, "severity": "warning" },
                    "classifications": { "status": "pii" },
                }),
            )),
        )
        .await
        .expect("draft_metadata call");
    assert_ne!(second.is_error, Some(true), "the second patch succeeds");

    let sidecar = dir.path().join("models").join("orders.toml");
    let text = std::fs::read_to_string(&sidecar).unwrap();
    let parsed: toml::Table = toml::from_str(&text).unwrap();
    // The appended check survived both parse-merges.
    let tests = parsed["tests"].as_array().expect("[[tests]] survives");
    assert_eq!(tests.len(), 1, "exactly the one appended check: {text}");
    // [freshness] was REPLACED, not duplicated or unioned.
    assert_eq!(
        parsed["freshness"]["expected_lag_seconds"],
        toml::Value::Integer(7200)
    );
    assert!(
        parsed["freshness"].get("time_column").is_none(),
        "replace semantics: the absent field does not linger from patch 1"
    );
    // Classification MERGED: both columns tagged.
    assert_eq!(
        parsed["classification"]["id"],
        toml::Value::String("internal".to_string())
    );
    assert_eq!(
        parsed["classification"]["status"],
        toml::Value::String("pii".to_string())
    );

    client.cancel().await.unwrap();
}

/// Refusals: an unknown model, an empty patch, and a malformed-value patch
/// each refuse with a structured envelope and write nothing.
#[tokio::test]
async fn draft_metadata_refuses_bad_arguments_without_writing() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let before = std::fs::read(dir.path().join("models").join("orders.toml")).unwrap();

    for (args, expect_code) in [
        (
            serde_json::json!({ "model": "nope", "classifications": { "id": "pii" } }),
            "model_not_found",
        ),
        (serde_json::json!({ "model": "orders" }), "invalid_argument"),
        (
            serde_json::json!({ "model": "orders", "classifications": {} }),
            "invalid_argument",
        ),
        (
            serde_json::json!({
                "model": "orders",
                "freshness": { "expected_lag_seconds": 0 },
            }),
            "invalid_argument",
        ),
        (
            serde_json::json!({
                "model": "orders",
                "freshness": { "expected_lag_seconds": 60, "severity": "fatal" },
            }),
            "invalid_argument",
        ),
        (
            serde_json::json!({
                "model": "orders",
                "classifications": { "  ": "pii" },
            }),
            "invalid_argument",
        ),
    ] {
        let result = client
            .call_tool(
                CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(args)),
            )
            .await
            .expect("draft_metadata returns a result");
        assert_eq!(result.is_error, Some(true));
        let err = result.structured_content.expect("envelope");
        assert_eq!(err["code"], serde_json::json!(expect_code), "{err:?}");
    }

    let after = std::fs::read(dir.path().join("models").join("orders.toml")).unwrap();
    assert_eq!(after, before, "every refusal leaves the sidecar untouched");

    client.cancel().await.unwrap();
}

/// An unparseable sidecar is NEVER clobbered: the call fails naming the file
/// and the bytes on disk stay identical.
#[tokio::test]
async fn draft_metadata_never_clobbers_an_unparseable_sidecar() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // Corrupt the sidecar AFTER project creation: model source still exists.
    let sidecar = dir.path().join("models").join("orders.toml");
    std::fs::write(&sidecar, "name = \"orders\"\n[strategy\nbroken !!").unwrap();
    let before = std::fs::read(&sidecar).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "classifications": { "id": "pii" },
                }),
            )),
        )
        .await
        .expect("draft_metadata returns a result");

    assert_eq!(
        result.is_error,
        Some(true),
        "an unparseable sidecar refuses"
    );
    let err = result.structured_content.expect("envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["message"]
            .as_str()
            .unwrap()
            .contains("models/orders.toml"),
        "the error names the sidecar: {err:?}"
    );
    let after = std::fs::read(&sidecar).unwrap();
    assert_eq!(after, before, "the unparseable sidecar is byte-identical");

    client.cancel().await.unwrap();
}

/// A patch whose merged sidecar fails to compile-load rolls back: the sidecar
/// parses as TOML (so the parse gate passes) but is not a valid model config,
/// the compile step hard-fails, and the guard restores the prior bytes.
#[tokio::test]
async fn draft_metadata_compile_failure_rolls_back() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // TOML-valid but ModelConfig-invalid: `strategy` must be a table.
    let sidecar = dir.path().join("models").join("orders.toml");
    std::fs::write(&sidecar, "name = \"orders\"\nstrategy = \"full_refresh\"\n").unwrap();
    let before = std::fs::read(&sidecar).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;
    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "classifications": { "id": "internal" },
                }),
            )),
        )
        .await
        .expect("draft_metadata returns a result");

    assert_eq!(
        result.is_error,
        Some(true),
        "the merged sidecar cannot load"
    );
    let err = result.structured_content.expect("envelope");
    assert_eq!(
        err["code"],
        serde_json::json!("compile_failed"),
        "a config-invalid sidecar is a hard compile failure: {err:?}"
    );
    let after = std::fs::read(&sidecar).unwrap();
    assert_eq!(
        after, before,
        "the compile failure restores the pre-patch sidecar bytes"
    );

    client.cancel().await.unwrap();
}

/// ⟦RTL-2⟧ THE post-image gate proof: a rule denying the agent on
/// `classifications = ["pii"]` must deny the very patch that ADDS the first
/// `pii` tag — the gate reads the attributes AS PATCHED, not the pre-write
/// ones (which carry no pii and would evade the rule). The deny restores the
/// prior sidecar byte-for-byte. The control half: the same patch with a
/// non-pii tag does NOT match the deny rule (it falls to the default
/// require_review and persists), proving the deny came from the
/// classification scope, not a blanket rule.
#[tokio::test]
async fn draft_metadata_newly_added_pii_is_denied_on_the_post_image() {
    let dir = TempDir::new().unwrap();
    write_project_with_policy(
        dir.path(),
        &dir.path().join("test.duckdb"),
        r#"[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { classifications = ["pii"] }
effect = "deny"
"#,
    );
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let sidecar = dir.path().join("models").join("orders.toml");
    let before = std::fs::read(&sidecar).unwrap();

    // The attack shape: the model carries NO pii tag yet; the patch adds the
    // first one. A pre-image gate would see zero classifications and let it
    // through as require_review.
    let result = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "classifications": { "status": "pii" },
                }),
            )),
        )
        .await
        .expect("draft_metadata returns a result");
    assert_eq!(result.is_error, Some(true));
    let err = result.structured_content.expect("envelope");
    assert_eq!(
        err["code"],
        serde_json::json!("policy_denied"),
        "the pii-scoped rule must catch the patch that ADDS pii: {err:?}"
    );
    assert_eq!(err["policy_rule"], serde_json::json!("0"));
    let after = std::fs::read(&sidecar).unwrap();
    assert_eq!(
        after, before,
        "a denied patch restores the prior sidecar bytes exactly"
    );

    // Control: a non-pii tag does not match the deny scope; it resolves to
    // the default require_review and the patched sidecar PERSISTS.
    let control = client
        .call_tool(
            CallToolRequestParams::new("draft_metadata").with_arguments(metadata_args(
                serde_json::json!({
                    "model": "orders",
                    "classifications": { "status": "internal" },
                }),
            )),
        )
        .await
        .expect("draft_metadata returns a result");
    assert_eq!(control.is_error, Some(true));
    let control_err = control.structured_content.expect("envelope");
    assert_eq!(
        control_err["code"],
        serde_json::json!("policy_review_required"),
        "a non-pii patch falls to the default, not the pii deny: {control_err:?}"
    );
    let parsed: toml::Table = toml::from_str(&std::fs::read_to_string(&sidecar).unwrap()).unwrap();
    assert_eq!(
        parsed["classification"]["status"],
        toml::Value::String("internal".to_string()),
        "the require_review patch persists as the reviewable artifact"
    );

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
    use rmcp::model::ContentBlock;
    let haystack: String = result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text(t) => Some(t.text.as_str()),
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
    use rmcp::model::ContentBlock;
    result
        .messages
        .iter()
        .filter_map(|m| match &m.content {
            ContentBlock::Text(t) => Some(t.text.as_str()),
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

    // find_untested_models — catalog -> ai_test/ai_contract -> draft_check/
    // draft_contract -> propose, stopping at the review/apply gate.
    let untested = client
        .get_prompt(GetPromptRequestParams::new("find_untested_models"))
        .await
        .expect("get_prompt find_untested_models");
    let haystack = prompt_text(&untested);
    for anchor in [
        "catalog",
        "ai_test",
        "draft_check",
        "ai_contract",
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

    // add_tests_to_pks — inspect_schema -> profile_column -> draft_check ->
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
        "draft_check",
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

/// FF-WP1 fix round (finding 7) — the worker-profile GOLDEN prompt surface:
/// every workflow prompt served under `--profile worker` (1) never mentions a
/// tool the profile excludes (`propose`, `draft_contract`, `draft_metadata`,
/// `ai_test`, `ai_contract`, `review_queue`, `pause_schedule`) and (2) ends
/// at an explicit hand-off to the trusted runner. The default-profile golden
/// tests above pin the other surface, so a prompt edit must consciously pick
/// its profiles.
#[tokio::test]
async fn worker_profile_prompts_end_at_the_runner_handoff() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Worker,
    );
    let client = connect(server).await;

    // Tools the worker profile excludes — no worker prompt may instruct them.
    // (`ai_test`/`ai_contract` also cover the excluded-generator class.)
    const EXCLUDED_TOOL_MENTIONS: &[&str] = &[
        "propose",
        "draft_contract",
        "draft_metadata",
        "ai_test",
        "ai_contract",
        "review_queue",
        "pause_schedule",
    ];

    let build_args = serde_json::json!({ "intent": "daily revenue" })
        .as_object()
        .unwrap()
        .clone();
    let model_args = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let prompts: Vec<(&str, Option<serde_json::Map<String, serde_json::Value>>)> = vec![
        ("build_model", Some(build_args)),
        ("find_untested_models", None),
        ("add_tests_to_pks", Some(model_args)),
        ("fix_failing_test", None),
    ];

    for (name, args) in prompts {
        let mut params = GetPromptRequestParams::new(name);
        if let Some(args) = args {
            params = params.with_arguments(args);
        }
        let result = client
            .get_prompt(params)
            .await
            .unwrap_or_else(|e| panic!("get_prompt {name}: {e}"));
        let haystack = prompt_text(&result);

        for excluded in EXCLUDED_TOOL_MENTIONS {
            assert!(
                !haystack.contains(excluded),
                "worker-profile `{name}` must not instruct excluded tool `{excluded}`; \
                 full text:\n{haystack}"
            );
        }
        assert!(
            haystack.contains("HAND OFF") && haystack.contains("trusted runner"),
            "worker-profile `{name}` must end at the trusted-runner handoff; \
             full text:\n{haystack}"
        );
        // The drafting loop itself survives: the worker still grounds and
        // verifies with in-profile tools.
        for allowed in ["profile_column", "test"] {
            assert!(
                haystack.contains(allowed),
                "worker-profile `{name}` still orchestrates in-profile tool `{allowed}`; \
                 full text:\n{haystack}"
            );
        }
    }

    // The read-only summary prompt is profile-invariant.
    let summary = client
        .get_prompt(GetPromptRequestParams::new("summarize_project"))
        .await
        .expect("get_prompt summarize_project");
    let haystack = prompt_text(&summary);
    assert!(
        haystack.to_lowercase().contains("read-only"),
        "summarize_project stays the read-only orientation under the worker profile"
    );

    client.cancel().await.unwrap();
}

/// FF-WP1 fix round 2 (item 5) — the worker-profile guidance surfaces ON THE
/// WIRE: the `prompts/list` descriptions and the draft tools' `next_steps`
/// name no tool the profile excludes and end at the trusted-runner hand-off.
/// (The default surfaces are byte-pinned by the rocky-mcp unit goldens; the
/// existing default-profile draft tests here pin the `propose` ending.)
#[tokio::test]
async fn worker_profile_descriptions_and_next_steps_end_at_the_handoff() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Worker,
    );
    let client = connect(server).await;

    const EXCLUDED_TOOL_MENTIONS: &[&str] = &[
        "propose",
        "review_queue",
        "draft_contract",
        "draft_metadata",
        "pause_schedule",
        "ai_test",
        "ai_contract",
    ];

    // Surface (b): every listed prompt description, as served over the wire.
    let prompts = client.list_all_prompts().await.expect("list prompts");
    assert_eq!(prompts.len(), 5, "the worker profile keeps all 5 prompts");
    for prompt in &prompts {
        let description = prompt.description.as_deref().unwrap_or_default();
        for excluded in EXCLUDED_TOOL_MENTIONS {
            assert!(
                !description.contains(excluded),
                "worker `prompts/list` description of '{}' must not name `{excluded}`: \
                 {description}",
                prompt.name
            );
        }
    }

    // Surface (c): the draft tools' next_steps.
    let draft = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 2 AS id, 'WORKER' AS status",
                "worker redraft",
            )),
        )
        .await
        .expect("draft_model call");
    assert_ne!(draft.is_error, Some(true), "draft_model succeeds");
    let check_args = serde_json::json!({
        "model": "orders",
        "spec": "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n",
    })
    .as_object()
    .unwrap()
    .clone();
    let check = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(check_args))
        .await
        .expect("draft_check call");
    assert_ne!(check.is_error, Some(true), "draft_check succeeds");

    for (tool, result) in [("draft_model", &draft), ("draft_check", &check)] {
        let next_steps = result.structured_content.as_ref().expect("structured")["next_steps"]
            .as_str()
            .expect("next_steps is a string")
            .to_string();
        for excluded in EXCLUDED_TOOL_MENTIONS {
            assert!(
                !next_steps.contains(excluded),
                "worker `{tool}` next_steps must not name `{excluded}`: {next_steps}"
            );
        }
        assert!(
            next_steps.contains("hand-off to the trusted runner"),
            "worker `{tool}` next_steps end at the runner hand-off: {next_steps}"
        );
    }

    client.cancel().await.unwrap();
}

/// The default profile's `build_model` still ends at `propose` — the worker
/// variant did not leak into the default surface (both golden pins together
/// force a conscious per-profile choice on any prompt edit).
#[tokio::test]
async fn default_profile_build_model_still_ends_at_propose() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let args = serde_json::json!({ "intent": "daily revenue" })
        .as_object()
        .unwrap()
        .clone();
    let result = client
        .get_prompt(GetPromptRequestParams::new("build_model").with_arguments(args))
        .await
        .expect("get_prompt build_model");
    let haystack = prompt_text(&result);
    assert!(
        haystack.contains("propose") && haystack.contains("STOP at propose"),
        "the DEFAULT build_model still stops at propose:\n{haystack}"
    );
    assert!(
        !haystack.contains("HAND OFF to the trusted runner"),
        "the worker handoff text must not leak into the default profile:\n{haystack}"
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
            skip_hash: None,
            upstream_freshness: None,
            bytes_scanned: None,
            bytes_written: Some(1024),
            tenant: None,
            recipe_hash: None,
            input_hash: None,
            input_proof_class: None,
            env_hash: None,
            hash_scheme: None,
            output_column_hashes: None,
            attempts: Vec::new(),
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
        check_outcomes: Vec::new(),
        pipeline: None,
        submission_id: None,
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
    // The failure carries the structured envelope: a machine-matchable code, a
    // message, and an actionable remediation_hint that names the accepted set.
    let err = result
        .structured_content
        .expect("a failing tool call carries the structured error envelope");
    assert_eq!(err["code"], serde_json::json!("invalid_argument"));
    assert!(
        err["message"].as_str().unwrap().contains("redshift"),
        "message names the offending value: {err:?}"
    );
    let hint = err["remediation_hint"].as_str().unwrap();
    assert!(
        hint.contains("duckdb") && hint.contains("snowflake"),
        "remediation_hint names the accepted dialects: {hint:?}"
    );

    client.cancel().await.unwrap();
}

/// The structured-error contract, end-to-end over the wire: every failing tool
/// call comes back as `is_error: true` with a `{code, message,
/// remediation_hint}` envelope in `structured_content`. Drives one
/// representative error class per code an offline call can reach, so the
/// envelope shape is proven reachable through `rocky mcp` (not just unit-typed).
#[tokio::test]
async fn tool_failures_carry_structured_error_envelope() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    // Assert a failing tool call carries the full envelope and return the
    // parsed error object for further per-case assertions.
    async fn expect_error(
        client: &rmcp::service::RunningService<rmcp::RoleClient, ()>,
        tool: &'static str,
        args: serde_json::Value,
    ) -> serde_json::Value {
        let params = match args {
            serde_json::Value::Null => CallToolRequestParams::new(tool),
            other => {
                CallToolRequestParams::new(tool).with_arguments(other.as_object().unwrap().clone())
            }
        };
        let result = client
            .call_tool(params)
            .await
            .unwrap_or_else(|e| panic!("{tool} call returns a result: {e}"));
        assert_eq!(result.is_error, Some(true), "{tool} must be an error");
        let err = result
            .structured_content
            .unwrap_or_else(|| panic!("{tool} error carries structured_content"));
        for key in ["code", "message", "remediation_hint"] {
            let v = err.get(key).and_then(|v| v.as_str());
            assert!(
                v.is_some_and(|s| !s.trim().is_empty()),
                "{tool} envelope has a non-empty {key}: {err:?}"
            );
        }
        // policy_rule is reserved for a future policy plane and absent today.
        assert!(
            err.get("policy_rule").is_none(),
            "{tool} envelope omits policy_rule until the policy plane sets it: {err:?}"
        );
        err
    }

    // invalid_argument — unknown `list` kind (no compile, no warehouse).
    let bad_kind = expect_error(&client, "list", serde_json::json!({ "kind": "frobnicate" })).await;
    assert_eq!(bad_kind["code"], serde_json::json!("invalid_argument"));
    assert!(
        bad_kind["remediation_hint"]
            .as_str()
            .unwrap()
            .contains("models"),
        "list hint names the accepted kinds: {bad_kind:?}"
    );

    // model_not_found — the project compiles (one model) but the name is absent.
    let ghost = expect_error(
        &client,
        "dependents",
        serde_json::json!({ "model": "ghost" }),
    )
    .await;
    assert_eq!(ghost["code"], serde_json::json!("model_not_found"));
    let ghost_hint = ghost["remediation_hint"].as_str().unwrap();
    assert!(
        ghost_hint.contains("list") || ghost_hint.contains("inspect_schema"),
        "model_not_found hint points at a discovery tool: {ghost_hint:?}"
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

    // ai_contract: contract_toml null, message names the env var.
    let dc = client
        .call_tool(CallToolRequestParams::new("ai_contract").with_arguments(args.clone()))
        .await
        .expect("ai_contract call");
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
        "ai_contract message should name the env var; got {sc:?}"
    );

    // ai_test: assertions empty, message names the env var.
    let gt = client
        .call_tool(CallToolRequestParams::new("ai_test").with_arguments(args.clone()))
        .await
        .expect("ai_test call");
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

/// The added-columns-only case: the source has a column the target lacks, but
/// NO existing column drifted in type. `detect_drift` returns
/// `DriftAction::Ignore` for this (it tracks only type changes), yet a
/// `rocky run` would issue `ALTER TABLE ADD COLUMN` and report the action as
/// `add_columns`. `drift_preview` must mirror the runtime, not the raw enum —
/// reporting `ignore` here would tell an agent "no action" for a run that
/// actually alters the target. The existing drift test combines a type-drift
/// with an added column (which resolves to `alter_column_types`), so it does
/// not exercise this path.
#[tokio::test]
async fn drift_preview_reports_add_columns_when_only_columns_added() {
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
        // Target and source share IDENTICAL types for `id`/`status` (no type
        // drift); the source carries one extra column the target lacks.
        adapter
            .execute_statement(
                "CREATE OR REPLACE TABLE out.orders AS \
                 SELECT CAST(1 AS INTEGER) AS id, 'COMPLETE' AS status",
            )
            .await
            .unwrap();
        adapter
            .execute_statement(
                "CREATE OR REPLACE TABLE out.orders_next AS \
                 SELECT CAST(1 AS INTEGER) AS id, 'COMPLETE' AS status, 'EU' AS region",
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
    // No existing column changed type — the empty drift list proves we are on
    // the added-columns-only path, not `alter_column_types`.
    assert!(
        sc["drifted_columns"].as_array().unwrap().is_empty(),
        "no column drifted in type: {sc:?}"
    );
    // The new column is surfaced.
    let added = sc["added_columns"].as_array().unwrap();
    assert!(
        added.iter().any(|c| *c == serde_json::json!("region")),
        "region must be an added column: {sc:?}"
    );
    // And the action must mirror what `rocky run` emits, not the raw
    // `DriftAction::Ignore` the detector returns.
    assert_eq!(
        sc["action"],
        serde_json::json!("add_columns"),
        "added-columns-only must report add_columns (run would ALTER TABLE ADD COLUMN): {sc:?}"
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
