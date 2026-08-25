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
///
/// The `()` handler requests `ClientInfo::default()`, whose `protocol_version`
/// is rmcp 3.1.2's `ProtocolVersion::LATEST` — `2025-11-25` today. Every test
/// in this file that uses `connect` is therefore describing THAT negotiated
/// version, which matters for `resultType`: see
/// [`result_type_reaches_a_2026_07_28_client_and_no_other`].
async fn connect(server: RockyMcpServer) -> rmcp::service::RunningService<rmcp::RoleClient, ()> {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Ok(svc) = server.serve(server_io).await {
            let _ = svc.waiting().await;
        }
    });
    ().serve(client_io).await.expect("client connects")
}

/// [`connect`], but the client asks for a SPECIFIC protocol version instead of
/// taking rmcp's default.
///
/// `impl ClientHandler for ClientInfo` returns the value itself from
/// `get_info`, so handing `serve` a `ClientInfo` is the whole mechanism — no
/// custom handler type is needed.
async fn connect_at_version(
    server: RockyMcpServer,
    protocol_version: rmcp::model::ProtocolVersion,
) -> rmcp::service::RunningService<rmcp::RoleClient, rmcp::model::ClientInfo> {
    let (server_io, client_io) = tokio::io::duplex(64 * 1024);
    tokio::spawn(async move {
        if let Ok(svc) = server.serve(server_io).await {
            let _ = svc.waiting().await;
        }
    });
    // `ClientInfo::default()` is exactly what the `()` handler in [`connect`]
    // sends, so the ONLY difference between the two clients is the version.
    let info = rmcp::model::ClientInfo::default().with_protocol_version(protocol_version);
    info.serve(client_io).await.expect("client connects")
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
            // `draft_check` is deliberately absent: a check's expression is
            // raw-interpolated into SQL the loop executes unattended after
            // an apply, so the untrusted profile must not serve one. This
            // closes the MCP route only — a worker with a file writer can
            // still write the sidecar (#1491, #1515).
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

/// F3 round 11, finding 1 — A FAILING FIXTURE `[[test]]` MUST NOT COME BACK
/// GREEN.
///
/// `commands::test_output` runs two suites and records them separately: each
/// model executed against DuckDB, and the fixture-driven `[[test]]` blocks in
/// the sidecars. The MCP result read only the first. A project whose models
/// all execute but whose fixture test FAILS came back as `failures: []` —
/// byte-identical to a clean run, and `is_error` unset either way.
///
/// That is the vacuous-pass class the work package exists to remove, and the
/// worker prompt turned it from latent into live: `fix_failing_test` promises
/// unit-test results and tells the worker to stop when the tests pass. A
/// worker read the empty list, saw green, and stopped with a failing test on
/// disk.
///
/// THE PROJECT IS BUILT SO THE TWO SUITES DISAGREE, which is the only shape
/// that can catch this. `orders` is self-contained (`SELECT 1 AS id, …`), so
/// executing it succeeds — the model suite is genuinely green. Its `[[test]]`
/// expects `id = 2`, so the fixture run genuinely fails. Before the fix the
/// result reported the green half and dropped the red one.
///
/// `failures` is asserted FIRST and on its own, because that is the field a
/// worker reads and the field that was empty. An assertion that only checked
/// the newly-added `all_passed` / `unit_tests` fields would fail before the
/// fix for the wrong reason — those keys were simply absent — and would not
/// demonstrate the false green at all.
#[tokio::test]
async fn a_failing_fixture_test_is_reported_not_swallowed() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    // Append a fixture test the model cannot satisfy: `orders` emits
    // `id = 1`, the expectation demands `id = 2`.
    let sidecar = dir.path().join("models").join("orders.toml");
    let mut toml = std::fs::read_to_string(&sidecar).unwrap();
    toml.push_str(
        "\n[[test]]\nname = \"orders_start_at_two\"\n\n\
         [test.expect]\nrows = [ { id = 2, status = \"COMPLETE\" } ]\n",
    );
    std::fs::write(&sidecar, toml).unwrap();

    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(CallToolRequestParams::new("test"))
        .await
        .expect("test call");
    let sc = result.structured_content.expect("test result");

    // THE REGRESSION. Empty here is the false green.
    let failures = sc["failures"].as_array().expect("failures is a list");
    assert!(
        !failures.is_empty(),
        "a failing fixture `[[test]]` must appear in `failures` — an empty list is the \
         vacuous pass a worker stops on: {sc:?}"
    );

    // The model suite really is clean, so the failure could only have come
    // from the fixture run. Without this the test would also pass on a
    // project that was simply broken.
    assert_eq!(sc["models"]["failed"], serde_json::json!(0), "{sc:?}");
    assert_eq!(sc["models"]["passed"], serde_json::json!(1), "{sc:?}");

    assert_eq!(sc["unit_tests"]["total"], serde_json::json!(1), "{sc:?}");
    assert_eq!(sc["unit_tests"]["failed"], serde_json::json!(1), "{sc:?}");
    assert_eq!(
        sc["all_passed"],
        serde_json::json!(false),
        "`all_passed` is the field the prompt tells a worker to branch on: {sc:?}"
    );

    // The entry names its suite and the test, so a worker can act on it
    // rather than guessing which half of the run broke.
    let unit_failure = failures
        .iter()
        .find(|f| f["suite"] == serde_json::json!("unit"))
        .unwrap_or_else(|| panic!("a unit-suite failure entry: {sc:?}"));
    assert_eq!(
        unit_failure["name"],
        serde_json::json!("orders::orders_start_at_two"),
        "{sc:?}"
    );
    assert!(
        !unit_failure["error"]
            .as_str()
            .expect("error text")
            .is_empty(),
        "the failure carries a reason: {sc:?}"
    );

    client.cancel().await.unwrap();
}

/// The other half of the same contract: a project with NO `[[test]]` block
/// reports the fixture suite as zero, not as absent.
///
/// "None declared" and "all passed" are different facts. An omitted field
/// states neither, and the omission is what let the failing case above go
/// unnoticed for as long as it did.
#[tokio::test]
async fn a_project_with_no_fixture_tests_reports_zero_not_absent() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let result = client
        .call_tool(CallToolRequestParams::new("test"))
        .await
        .expect("test call");
    let sc = result.structured_content.expect("test result");

    assert_eq!(sc["unit_tests"]["total"], serde_json::json!(0), "{sc:?}");
    assert_eq!(sc["unit_tests"]["failed"], serde_json::json!(0), "{sc:?}");
    assert_eq!(sc["all_passed"], serde_json::json!(true), "{sc:?}");
    assert_eq!(
        sc["total"], sc["models"]["total"],
        "with no fixture tests the aggregate is the model suite: {sc:?}"
    );

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
    // The APPROVER profile (#1517): the last assertion here is that combining
    // `product_id` with an approve is an ARGUMENT error. On the default
    // profile the availability gate fires first and that call never reaches
    // the argument check, so testing it needs a server that serves approving.
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Approver,
    );
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
///
/// Runs on the APPROVER profile (#1517) — the only profile that serves the
/// approve action at all. The confirm gate tested here is the SECOND gate: it
/// still holds even once the operator has opted in. The default profile's
/// refusal is
/// [`default_profile_lists_the_queue_but_refuses_to_approve`].
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
    let server = RockyMcpServer::new_with_profile(
        dir.path().join("rocky.toml"),
        rocky_mcp::McpProfile::Approver,
    );
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

/// #1517 — the split, on the profile `rocky mcp` serves with NO flag: reading
/// the queue works, approving does not.
///
/// This is the whole point of the issue. The stock command used to hand one
/// agent both halves — propose a plan, then sign it off — with `confirm` as
/// the only thing standing in the way, and `confirm` is set by the caller.
/// Here the same call, with `confirm: true` and a genuinely pending plan_id,
/// leaves no marker on disk and comes back naming the opt-in.
#[tokio::test]
async fn default_profile_lists_the_queue_but_refuses_to_approve() {
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
    // The DEFAULT profile — exactly what `rocky mcp` serves with no flag.
    let server = RockyMcpServer::new(dir.path().join("rocky.toml"));
    let client = connect(server).await;

    let _ = client
        .call_tool(CallToolRequestParams::new("propose"))
        .await
        .expect("propose returns a result");
    let plans = plan_files(dir.path());
    assert_eq!(plans.len(), 1, "the propose persisted one plan for review");
    let plan_id = plans[0].file_stem().unwrap().to_str().unwrap().to_string();

    // Half one: LISTING is untouched. The queue is useful and harmless to
    // read, so the gate must not have removed it along with approving.
    let queue = client
        .call_tool(CallToolRequestParams::new("review_queue"))
        .await
        .expect("review_queue list call");
    assert_ne!(
        queue.is_error,
        Some(true),
        "listing the queue still works on the default profile: {queue:?}"
    );
    let sc = queue.structured_content.expect("queue structured content");
    assert_eq!(sc["total"], serde_json::json!(1));
    let pending = sc["pending"].as_array().unwrap();
    assert_eq!(pending.len(), 1, "the escalation is listed");
    assert_eq!(pending[0]["plan_id"], serde_json::json!(plan_id));

    // Half two: APPROVING is refused — with confirm=true, on a real pending
    // plan. Nothing about this call is malformed; the profile is the refusal.
    let approve = serde_json::json!({ "approve_plan_id": plan_id, "confirm": true })
        .as_object()
        .unwrap()
        .clone();
    let refused = client
        .call_tool(CallToolRequestParams::new("review_queue").with_arguments(approve))
        .await
        .expect("review_queue approve returns a result");
    assert_eq!(
        refused.is_error,
        Some(true),
        "the default profile refuses to approve: {refused:?}"
    );
    let err = refused
        .structured_content
        .expect("structured error envelope");

    // A DISTINCT code. Not `policy_review_required`: no rule decided this and
    // no plan was recorded by it, and no retry with confirm can satisfy it.
    assert_eq!(
        err["code"],
        serde_json::json!("approve_not_enabled"),
        "the refusal has its own machine-matchable code: {err:?}"
    );

    // The refusal NAMES the opt-in. An operator hitting this at 3am must not
    // have to read source to find out what to do. The flag spelling is pinned
    // as a literal here on purpose — `mcp_profile_arg_accepts_approver` proves
    // the same literal is what clap actually parses.
    let message = err["message"].as_str().expect("a message");
    let hint = err["remediation_hint"].as_str().expect("a hint");
    let both = format!("{message}\n{hint}");
    assert!(
        both.contains("--profile approver"),
        "the refusal names the opt-in flag verbatim: {both}"
    );
    assert!(
        both.contains(&plan_id),
        "the refusal names the plan it refused: {both}"
    );
    assert!(
        hint.contains("rocky review") && hint.contains("--approve"),
        "the hint offers the human's own terminal as the normal path: {hint}"
    );
    assert!(
        hint.contains("OPERATOR") || hint.contains("operator"),
        "the hint says WHO can turn it on: {hint}"
    );

    // The only assertion that really matters: nothing was written.
    let marker = dir
        .path()
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.reviewed.json"));
    assert!(
        !marker.exists(),
        "no sign-off marker exists after a refused approve"
    );

    // And the plan is still pending — the refusal changed no state at all.
    let after = client
        .call_tool(CallToolRequestParams::new("review_queue"))
        .await
        .expect("review_queue re-list");
    let after_sc = after.structured_content.expect("queue content");
    assert_eq!(
        after_sc["total"],
        serde_json::json!(1),
        "the escalation is still awaiting review"
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

/// FOURTEENTH ROUND, finding 1 — `build_model` step 6 told the agent
/// `plan_preview` returns "the exact SQL Rocky would execute", on BOTH
/// profiles.
///
/// It does not. `commands::plan_preview_output` renders offline: it passes
/// no warehouse to `sql_gen::generate_transformation_sql_with_warehouse`,
/// and any model that call cannot render is logged at debug and DROPPED
/// from the result. `PlanPreviewResult` has a `statements` field and
/// nothing else, so there is no `skipped_models` for a caller to read. An
/// agent that believes the exactness claim reads a short preview as a
/// short plan.
///
/// Pinned in both directions, on both profiles, because a one-sided edit to
/// these two nearly-identical bodies is exactly how the round-thirteen
/// `build_model` variants slipped.
#[tokio::test]
async fn build_model_does_not_promise_plan_preview_is_exact() {
    for profile in [
        rocky_mcp::McpProfile::Default,
        rocky_mcp::McpProfile::Worker,
    ] {
        let dir = TempDir::new().unwrap();
        write_project(dir.path(), &dir.path().join("test.duckdb"));
        let server = RockyMcpServer::new_with_profile(dir.path().join("rocky.toml"), profile);
        let client = connect(server).await;
        let args = serde_json::json!({ "intent": "daily revenue" })
            .as_object()
            .unwrap()
            .clone();
        let result = client
            .get_prompt(GetPromptRequestParams::new("build_model").with_arguments(args))
            .await
            .expect("get_prompt build_model");
        let body = prompt_text(&result);
        assert!(
            !body.contains("exact SQL Rocky would execute"),
            "{profile:?}: `build_model` promises `plan_preview` returns the EXACT execution \
             SQL; the preview is offline and drops what it cannot render: {body}"
        );
        assert!(
            body.contains("not renderable offline"),
            "{profile:?}: `build_model` must tell the agent that a model missing from the \
             preview is unrenderable rather than absent, or an empty preview reads as a \
             finished plan: {body}"
        );
        client.cancel().await.unwrap();
    }
}

/// FOURTEENTH ROUND, finding 2 — `fix_failing_test` promised failure-local
/// evidence that its own tools do not supply.
///
/// Two claims, and they are ASYMMETRIC across the profiles, so this test is
/// asymmetric too rather than tidy:
///
/// - "sample_rows to look at offending rows" was in BOTH bodies.
///   `sample_rows` takes `model` and `percent` and nothing else — it issues
///   `SELECT * FROM <ref> [tablesample] LIMIT n`, with no predicate. A
///   sparse bad row can simply be absent from the sample, and an agent told
///   the rows are "offending" reads that absence as evidence.
/// - "and the failing-row count" was WORKER-ONLY. `TestFailureLite` carries
///   `name`, `error` and `suite`; there is no count field, and the compile,
///   seed and model-execution failure paths in `rocky-engine`'s
///   `test_runner` put no row numbers in the error text either. The default
///   body never made that claim, so only the worker body gained the
///   disclosure — pinning the default for it would pin a sentence that has
///   no defect behind it.
///
/// No predicate sampling was added. The fix is the wording.
#[tokio::test]
async fn fix_failing_test_does_not_promise_failure_local_evidence() {
    for profile in [
        rocky_mcp::McpProfile::Default,
        rocky_mcp::McpProfile::Worker,
    ] {
        let dir = TempDir::new().unwrap();
        write_project(dir.path(), &dir.path().join("test.duckdb"));
        let server = RockyMcpServer::new_with_profile(dir.path().join("rocky.toml"), profile);
        let client = connect(server).await;
        let args = serde_json::json!({ "model": "orders" })
            .as_object()
            .unwrap()
            .clone();
        let result = client
            .get_prompt(GetPromptRequestParams::new("fix_failing_test").with_arguments(args))
            .await
            .expect("get_prompt fix_failing_test");
        let body = prompt_text(&result);
        assert!(
            !body.contains("offending rows"),
            "{profile:?}: `fix_failing_test` calls the `sample_rows` output the OFFENDING \
             rows; the tool takes no predicate and samples the whole table: {body}"
        );
        assert!(
            body.contains("takes no predicate"),
            "{profile:?}: `fix_failing_test` must say `sample_rows` is unfiltered, or a \
             sample with no bad row in it reads as proof there is none: {body}"
        );
        assert!(
            !body.contains("and the failing-row count"),
            "{profile:?}: `fix_failing_test` promises a failing-row count; `TestFailureLite` \
             carries only `name`, `error` and `suite`: {body}"
        );
        if profile == rocky_mcp::McpProfile::Worker {
            assert!(
                body.contains("no failing-row count field"),
                "the worker body is the one that promised the count, so it is the one that \
                 must say the field does not exist: {body}"
            );
        }
        client.cancel().await.unwrap();
    }
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

/// FF-WP1 fix round (finding 7), extended by the F3 red team — the
/// worker-profile GOLDEN prompt surface: every prompt BODY served under
/// `--profile worker` (1) never names a tool the profile excludes and (2)
/// ends at an explicit hand-off to the trusted runner. The default-profile
/// golden tests above pin the other surface, so a prompt edit must
/// consciously pick its profiles.
///
/// Two things here are DERIVED rather than written down, and both are the
/// finding this test was rewritten for.
///
/// - **The excluded list** comes from the two real routers (default minus
///   worker), exactly as `tools.rs`'s unit sweep and `briefs.rs` build
///   theirs. A hand-picked literal is how `draft_check` slipped: the F3
///   work package removed it from the worker allowlist while three prompt
///   bodies still instructed it, and the literal here did not name it, so
///   this test went green over three messages steering the worker at a
///   tool that answers tool-not-found.
/// - **The prompt set** comes from `prompts/list`, not a written-out
///   vector. The same defect had a second half: `prompts/list`
///   DESCRIPTIONS were swept and the `prompts/get` BODIES were not, and a
///   sweep over a hand-written four could never have covered a fifth
///   prompt added later. Arguments are synthesised from each prompt's own
///   declared `arguments`, so a new prompt with new arguments is swept
///   with nothing to remember.
#[tokio::test]
async fn worker_profile_prompts_end_at_the_runner_handoff() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let config_path = dir.path().join("rocky.toml");

    // DERIVED, not hand-picked: every tool the default profile serves and
    // the worker profile does not. `tool_names()` reads the same router
    // the constructor filtered, so this can never disagree with what
    // `tools/list` serves.
    let worker_tools =
        RockyMcpServer::new_with_profile(config_path.clone(), rocky_mcp::McpProfile::Worker)
            .tool_names();
    let excluded_tool_mentions: Vec<String> = RockyMcpServer::new(config_path.clone())
        .tool_names()
        .into_iter()
        .filter(|name| !worker_tools.contains(name))
        .collect();
    assert!(
        excluded_tool_mentions.iter().any(|n| n == "draft_check"),
        "the worker profile still excludes `draft_check`, so the sweep must cover it: \
         {excluded_tool_mentions:?}"
    );

    let server = RockyMcpServer::new_with_profile(config_path, rocky_mcp::McpProfile::Worker);
    let client = connect(server).await;

    // ENUMERATED from the served router, not written down here.
    let prompts = client.list_all_prompts().await.expect("list prompts");
    assert_eq!(
        prompts.len(),
        5,
        "the worker profile serves all 5 prompts; update the expectations below, not this \
         sweep, when one is added"
    );

    for prompt in &prompts {
        let name = prompt.name.clone();
        // Synthesised from the prompt's OWN declared arguments, so a new
        // prompt needs no edit here. `add_tests_to_pks` / `fix_failing_test`
        // take a model name and `build_model` takes free text, and a
        // placeholder is fine for both: this sweeps guidance text, not
        // behaviour that depends on the value.
        let mut args = serde_json::Map::new();
        for declared in prompt.arguments.iter().flatten() {
            let value = if declared.name == "model" {
                "orders"
            } else {
                "daily revenue"
            };
            args.insert(declared.name.clone(), serde_json::json!(value));
        }
        let mut params = GetPromptRequestParams::new(name.clone());
        if !args.is_empty() {
            params = params.with_arguments(args);
        }
        let result = client
            .get_prompt(params)
            .await
            .unwrap_or_else(|e| panic!("get_prompt {name}: {e}"));
        let haystack = prompt_text(&result);

        // TWO HAYSTACKS, TWO JOBS (ninth round, finding 2).
        //
        // The NAME rule runs over the WHOLE serialized result, not over a
        // chosen field. `prompt_text` reads `messages`, and
        // `GetPromptResult` also carries a `description` that no sweep
        // read — which is how two worker descriptions went on promising a
        // write ("draft tests", "Add key tests") while their bodies were
        // report-only.
        //
        // Row 3 is therefore defined by CHANNEL, not by field: everything
        // `prompts/get` returns. Adding a "surface 10" for `description`
        // would have bought a surface 11 for the next field — enumerating
        // fields is exactly what lost here.
        let whole = serde_json::to_string(&result).expect("prompt result serializes");
        // ELEVENTH ROUND, finding 4 — the row's field list in
        // `WORKER_GUIDANCE_SURFACES` named only `messages` and
        // `description`, while rmcp 3.1.2's `GetPromptResult` also carries
        // `resultType` and `_meta`. The list was stale, not the coverage:
        // the sweep below reads the whole value.
        //
        // AND `resultType` IS NOT ON THE WIRE, which the first attempt at
        // this correction asserted the opposite of. `GetPromptResult::new`
        // does set `Some(ResultType::COMPLETE)`, but `get_info` pins
        // `ProtocolVersion::V_2024_11_05`, and rmcp's server handler calls
        // `strip_result_type_for_legacy_peer()` for any peer older than
        // `2026-07-28`. So the field is defined, set, and then cleared
        // before it is serialized.
        //
        // Pinned in the direction that is TRUE, so a protocol-version bump
        // fails here and the row gets re-read rather than quietly gaining a
        // field nobody swept on purpose.
        let shape: serde_json::Value =
            serde_json::from_str(&whole).expect("prompt result is an object");
        assert!(
            shape.get("messages").is_some(),
            "`prompts/get` must serialize `messages`, or the field list on row 3 of \
             WORKER_GUIDANCE_SURFACES is describing a shape this crate no longer \
             serves: {whole}"
        );
        assert!(
            shape.get("resultType").is_none(),
            "`resultType` is stripped for peers older than 2026-07-28, and this server \
             pins 2024-11-05 — if it is on the wire the negotiated version moved, and \
             row 3's field list needs re-reading: {whole}"
        );
        assert_eq!(
            rocky_mcp::names_excluded_tool(&whole, &excluded_tool_mentions),
            None,
            "worker-profile `{name}` must not name an excluded tool anywhere in its \
             `prompts/get` result; whole result:\n{whole}"
        );

        // The ANCHOR assertions keep reading `messages`, because they are
        // about what the body instructs. Deliberately not merged with the
        // sweep above: a `description` that happens to contain the word
        // would otherwise satisfy a body assertion.
        //
        // The rule is `names_excluded_tool`, not `contains` — derived
        // rather than written down, and it caught a violation on THIS
        // surface after two rounds of believing it clean: the
        // `add_tests_to_pks` worker body said "Proposing a wrong key
        // invariant", which an exact-name compare reads as prose. See the
        // rule's own doc for why the remedy is to reword rather than to
        // relax it.
        assert_eq!(
            rocky_mcp::names_excluded_tool(&haystack, &excluded_tool_mentions),
            None,
            "worker-profile `{name}` must not instruct an excluded tool in any form; \
             full text:\n{haystack}"
        );

        // The read-only summary prompt is profile-invariant: it has no
        // worker branch and no hand-off, because it never drafts anything.
        // Every other prompt ends at the runner.
        if name == "summarize_project" {
            assert!(
                haystack.to_lowercase().contains("read-only"),
                "summarize_project stays the read-only orientation under the worker profile"
            );
            continue;
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

        // TENTH ROUND, finding 1 — two steers that name no excluded tool,
        // so `names_excluded_tool` above reads both as clean. Absence is
        // the only thing that pins them, and the paired presence check
        // below is what stops "delete the sentence" from passing.
        let haystack_lower = haystack.to_lowercase();
        for steer in [
            // The CARVE-OUT. `fix_failing_test` reserved "test edits beyond
            // append-only checks" for the runner, which leaves append-only
            // checks AVAILABLE to the worker — the exact capability
            // removing `draft_check` from the allowlist exists to stop. It
            // is reachable: the worker runs in the project root, and Phase
            // B PRESERVES a worker-added `[[tests]]` block
            // (`rocky_core::product::lowering`).
            "append-only",
            // The FALSE PROMISE. The `test` tool calls
            // `commands::test_output` — the compiled model tests plus the
            // unit tests. The declarative check set runs through `rocky
            // test --declarative`, a different path this profile does not
            // serve, and its checks need an applied table besides. Same
            // defect `WORKER_DRAFT_NEXT_STEPS` already corrects one surface
            // over.
            "run the declarative tests",
            "the declarative tests and read",
        ] {
            assert!(
                !haystack_lower.contains(steer),
                "worker-profile `{name}` still instructs `{steer}` — a withheld action or a \
                 suite this profile cannot run, and one the name-based sweep cannot see; \
                 full text:\n{haystack}"
            );
        }
        assert!(
            haystack_lower.contains("local tests")
                || haystack_lower.contains("local model and unit tests"),
            "worker-profile `{name}` must say WHICH suite the `test` tool runs, not merely \
             drop the wrong claim; full text:\n{haystack}"
        );

        // The DESCRIPTION must agree with the body about where the work
        // ends (ninth round, finding 2). Both fields of this result are
        // guidance; a description that promises an ending the body
        // withholds is a contradiction served in one payload, and it is
        // the field half that went unchecked.
        let description = result
            .description
            .as_deref()
            .unwrap_or_else(|| panic!("worker-profile `{name}` returns a description"));
        let description_lower = description.to_lowercase();
        assert!(
            description_lower.contains("runner handoff"),
            "worker-profile `{name}`'s `prompts/get` description must end where its body \
             ends — at the runner handoff: {description}"
        );

        // And it must not PROMISE a spec-owned write. `find_untested_models`
        // said "draft tests" and `add_tests_to_pks` said "Add key tests"
        // while both bodies were report-only.
        //
        // HAND-WRITTEN, which is this file's own anti-pattern, so the
        // alternative was TESTED rather than dismissed.
        //
        // The obvious derivation is from the WITHHELD CAPABILITIES, read
        // off the excluded tool names the routers already give us:
        // `draft_check` is the capability "draft a check", so generate
        // "<verb> <noun>" from every `<verb>_<noun>` excluded name. Run
        // against the two strings that actually shipped, that derivation
        // produces `draft check` / `draft checks` / `draft a check` and
        // MATCHES NEITHER.
        //
        // Two gaps, and both are instructive:
        //
        //  - A SYNONYM gap. `draft_check` writes a `[[tests]]` block. The
        //    tool noun is "check" and the artifact noun is "tests", so a
        //    rule derived from the NAME cannot reach the word the text
        //    actually used.
        //  - A VERB gap. "Add key tests" uses "add", which appears in no
        //    tool name at all.
        //
        // So the derivation would have looked like coverage while catching
        // nothing that happened. A named boundary beats that. The list
        // stays hand-written, and the honest statement is that it is a
        // backstop, not a rule: the general defence is finding 1's lesson
        // — read what the served text tells the worker to DO.
        //
        // The wider point, and the reason this is written out. Every
        // derived rule in this crate is LEXICAL. Lexical derivation closes
        // the NAME axis completely and the MEANING axis not at all. Both
        // of this round's findings were meaning-axis: an instruction that
        // named no tool, and a promise that named no tool. Neither could
        // have been found by any rule here, and both were found by reading.
        for promise in [
            "draft tests",
            "draft the tests",
            "add tests",
            "add key tests",
            "write tests",
            "draft checks",
            "add checks",
            "write checks",
            "draft a contract",
            "add a contract",
            "write metadata",
        ] {
            assert!(
                !description_lower.contains(promise),
                "worker-profile `{name}`'s description promises `{promise}`, which is \
                 spec-owned here and which its body does not do: {description}"
            );
        }
    }

    client.cancel().await.unwrap();
}

/// THIRTEENTH ROUND — the surface this branch predicted round fourteen
/// would find, swept before it did.
///
/// Every earlier sweep over `prompts/get` bodies asks the same question:
/// does this text name a tool the profile does not serve? None of them ever
/// asked whether it makes a claim the served tool does not keep. That is a
/// different defect class, and it was here: `build_model` step 1 said
/// "inspect_schema — read EVERY existing model and source table", then
/// "select only what's actually there" — an assertion of completeness over
/// the tool with TWO silent failure paths, telling the caller to treat
/// absence as absence. It is the same over-claim removed from that tool's
/// own description this round, one surface over.
///
/// BOTH PROFILES, because the two variants are separate string literals
/// branched on `self.profile` with no construction-time link between them
/// (unlike `WORKER_TOOL_DESCRIPTIONS`, whose rewrite refuses when its
/// needle stops matching). A one-sided fix is otherwise silent, which is
/// what this sweep is for — see the mutation that proves it.
///
/// WHAT THIS DOES NOT CATCH, stated rather than left to be found: a
/// substring pin catches THIS phrasing. "all source tables", "the complete
/// set of", or a fresh universal in a future prompt walk straight past it.
/// It is a regression pin on a defect that has now recurred four times on
/// four surfaces, not a semantic rule — deriving one would need to read the
/// claim, not the words.
#[tokio::test]
async fn no_prompt_body_tells_the_caller_inspect_schema_is_complete() {
    for profile in [
        rocky_mcp::McpProfile::Default,
        rocky_mcp::McpProfile::Worker,
    ] {
        let dir = TempDir::new().unwrap();
        write_project(dir.path(), &dir.path().join("test.duckdb"));
        let server = RockyMcpServer::new_with_profile(dir.path().join("rocky.toml"), profile);
        let client = connect(server).await;

        // ENUMERATED from the served router, like the sweep above, so a
        // sixth prompt is covered without an edit here.
        let prompts = client.list_all_prompts().await.expect("list prompts");
        assert_eq!(
            prompts.len(),
            5,
            "{profile:?} serves all 5 prompts; a new one must be swept, not excused"
        );

        let mut saw_build_model = false;
        for prompt in &prompts {
            let name = prompt.name.clone();
            // Synthesised from the prompt's OWN declared arguments — the
            // same rule as the worker sweep, for the same reason.
            let mut args = serde_json::Map::new();
            for declared in prompt.arguments.iter().flatten() {
                let value = if declared.name == "model" {
                    "orders"
                } else {
                    "daily revenue"
                };
                args.insert(declared.name.clone(), serde_json::json!(value));
            }
            let mut params = GetPromptRequestParams::new(name.clone());
            if !args.is_empty() {
                params = params.with_arguments(args);
            }
            let result = client
                .get_prompt(params)
                .await
                .unwrap_or_else(|e| panic!("get_prompt {name}: {e}"));
            let haystack = prompt_text(&result).to_lowercase();

            assert!(
                !haystack.contains("every existing model and source table"),
                "{profile:?} `{name}` tells the caller inspect_schema returns every model \
                 and source table. Its physical-table discovery reports none of them and \
                 still succeeds, so that instructs the caller to read a silent failure as \
                 proof of absence"
            );

            // AND THE REPLACEMENT MUST CARRY THE CAVEAT, not merely drop
            // the universal. Dropping it is the cheap edit and it leaves
            // the caller with no reason to distrust an empty `sources` —
            // the same both-directions rule the served instructions use.
            //
            // Pinned on `build_model` only: it is the prompt that opens by
            // instructing inspect_schema over the WHOLE project.
            // `add_tests_to_pks` also calls it, for the typed columns of
            // one named model, and claims no completeness.
            if name == "build_model" {
                saw_build_model = true;
                assert!(
                    haystack.contains("inconclusive, not absent"),
                    "{profile:?} `build_model` instructs inspect_schema first and must say \
                     a table missing from `sources` is inconclusive: {haystack}"
                );
                assert!(
                    haystack.contains("ask sample_rows for that table"),
                    "{profile:?} `build_model` must name the reader that fails loudly for \
                     the same table, or the caveat leaves the caller stuck: {haystack}"
                );
            }
        }
        assert!(
            saw_build_model,
            "{profile:?} serves `build_model` — if it was renamed, the pins above moved \
             with it or silently stopped running"
        );

        client.cancel().await.unwrap();
    }
}

/// FF-WP1 fix round 2 (item 5), extended by the F3 red team (finding 3) —
/// the worker-profile guidance surfaces ON THE WIRE.
///
/// Five of the NINE surfaces `WORKER_GUIDANCE_SURFACES` enumerates are
/// checked here as the worker actually receives them: the whole
/// `initialize` result (1), the whole listed `Prompt` (2), the whole listed
/// `Tool` — its description (4) and its input schema (5) together — and
/// the draft result's `next_steps` (7).
///
/// The other four live elsewhere in this file or in the crate's unit
/// tests: the whole `prompts/get` result (3) in
/// `worker_profile_prompts_end_at_the_runner_handoff`, and the successful
/// result text (8), the pinned-absent `output_schema` (6) and the error
/// envelope (9) in `worker_result_text_names_no_excluded_tool`. Six swept,
/// two PARTIAL, one NOT SERVED — the enumeration in `tools.rs` carries the
/// reasons, and it is the one place to update.
///
/// THIS HEADER WAS ITSELF STALE, and saying so is the point. It described
/// a SEVEN-row enumeration, called surface 1 EXEMPT after it had been
/// projected and swept, numbered `next_steps` as 6 while the sibling test
/// in this same file correctly numbered `output_schema` 6, and called row
/// 9 `remediation_hint` alone and OPEN. Prose about a gate rots faster
/// than the gate, and a test file that disagrees with itself about which
/// surface is which is the cheapest possible finding to hand a reviewer.
///
/// This test does not claim to be the whole sweep, because "this is the
/// whole sweep" is the sentence that has been wrong five times.
///
/// TENTH ROUND, finding 2 — surfaces 2, 4 and 5 here were FIELD sweeps
/// while the enumeration in `tools.rs` claimed every row matched its
/// channel's whole serialized payload. They now serialize the whole
/// `Prompt` and the whole `Tool`. Nothing leaked through the omitted
/// fields; the false GUARANTEE was the finding.
///
/// TWO THINGS ARE DERIVED, and both are the finding this was rewritten
/// for. The excluded-tool set comes from the two real routers, replacing a
/// hand-picked literal of seven names that was already the anti-pattern
/// its sibling test above documents — the real set is nineteen. And the
/// match is `names_excluded_tool`, which reads inflections: two of the
/// three tool-description violations here were `propose`, but the third
/// was "proposing", and an exact compare called it clean.
///
/// (The default surfaces are pinned by the rocky-mcp unit goldens; the
/// existing default-profile draft tests here pin the `propose` ending.)
#[tokio::test]
async fn worker_profile_guidance_surfaces_name_no_excluded_tool() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let config_path = dir.path().join("rocky.toml");

    // DERIVED from the two real routers, exactly as the prompt-body sweep
    // above and `briefs.rs` derive theirs. The literal this replaces named
    // seven tools; the profile actually excludes nineteen, so twelve
    // excluded verbs could have appeared in any of these surfaces and the
    // test would have stayed green.
    let worker_tools =
        RockyMcpServer::new_with_profile(config_path.clone(), rocky_mcp::McpProfile::Worker)
            .tool_names();
    let excluded: Vec<String> = RockyMcpServer::new(config_path.clone())
        .tool_names()
        .into_iter()
        .filter(|name| !worker_tools.contains(name))
        .collect();
    assert!(
        excluded.len() > 7,
        "the derived set must be wider than the literal it replaced, or this change bought \
         nothing: {excluded:?}"
    );

    let server = RockyMcpServer::new_with_profile(config_path, rocky_mcp::McpProfile::Worker);
    let client = connect(server).await;

    // Surface 1: the whole `initialize` result, ON THE WIRE. Checked here
    // as well as at unit level because this is the surface the whole round
    // is about, and because a reviewer running only the integration tests
    // must not read their silence as coverage.
    //
    // The `instructions` field is taken first and split in two, because the
    // two halves have opposite properties. The BANNER names excluded tools
    // deliberately — saying `propose` is not available is the opposite of
    // steering at it — and must name EVERY member of the derived set. The
    // BODY below it must name none of them. The rest of the handshake is
    // swept after them.
    let instructions = client
        .peer_info()
        .and_then(|info| info.instructions.clone())
        .expect("the worker profile serves instructions over the wire");
    let (banner, body) = instructions
        .split_once("\n\n---\n")
        .map(|(banner, body)| (banner.to_string(), format!("---\n{body}")))
        .expect("the banner precedes the skill frontmatter");
    assert_eq!(
        rocky_mcp::names_excluded_tool(&body, &excluded),
        None,
        "the worker `instructions` BODY must not name an excluded tool: {body}"
    );
    for tool in &excluded {
        assert!(
            banner.to_lowercase().contains(&tool.to_lowercase()),
            "the worker `instructions` BANNER must name `{tool}` as unavailable — derived \
             from the routers, so shrinking the list to a literal fails here: {banner}"
        );
    }
    assert!(
        body.to_lowercase().contains("checks are spec-owned"),
        "the body must stop the worker at CHECK authorship, which is the hole this \
         projection closed: {body}"
    );
    // TENTH ROUND — and the WHOLE handshake, not the `instructions` field.
    // The header above says a reviewer running only the integration tests
    // must not read their silence as coverage; that argument applies to the
    // rest of the initialize result too. `InitializeResult` also carries
    // `protocolVersion`, `capabilities` and `serverInfo`, whose
    // `Implementation` has `title` / `description` / `icons` / `websiteUrl`
    // — free text a worker reads before anything else. All four are `None`
    // under `from_build_env()`, so nothing leaks; the unbacked guarantee was
    // the defect. The banner is spliced out and nothing else is, because it
    // is the one surface that names excluded tools on purpose.
    let mut handshake = serde_json::to_value(
        client
            .peer_info()
            .expect("the worker profile completes the handshake"),
    )
    .expect("initialize result serializes");
    handshake["instructions"] = serde_json::Value::String(body.clone());
    assert!(
        handshake.get("serverInfo").is_some() && handshake.get("capabilities").is_some(),
        "the handshake must carry serverInfo and capabilities, or this sweeps an envelope \
         with the newly-covered fields missing: {handshake}"
    );
    let handshake = handshake.to_string();
    assert_eq!(
        rocky_mcp::names_excluded_tool(&handshake, &excluded),
        None,
        "the worker `initialize` result must not name an excluded tool anywhere outside \
         the banner: {handshake}"
    );

    // Surface 2: every listed prompt, as served over the wire.
    //
    // TENTH ROUND, finding 2 — the WHOLE `Prompt`, not its `description`.
    // The enumeration in `tools.rs` claims every row matches the whole
    // serialized payload of its channel, "so a field added later is covered
    // without any test knowing the shape". That was true of row 3 and of no
    // other row: this one read `description` alone, while rmcp 3.1.2's
    // `Prompt` also carries `name`, `title`, `arguments` (each with its own
    // `description`), `icons` and `_meta`. No leak was demonstrated in the
    // omitted fields — the false claim is the finding, not a leak.
    let prompts = client.list_all_prompts().await.expect("list prompts");
    assert_eq!(prompts.len(), 5, "the worker profile keeps all 5 prompts");
    // ELEVENTH ROUND, finding 4 — the row's field list omitted `name`,
    // which is the one field of the six that is ALWAYS on the wire. The
    // sweep below already covered it, so this pins the shape the list
    // describes rather than adding coverage: a prompt named after an
    // excluded tool would fire on `name` alone.
    for prompt in &prompts {
        let whole = serde_json::to_string(prompt).expect("prompt serializes");
        let shape: serde_json::Value = serde_json::from_str(&whole).expect("prompt is an object");
        assert!(
            shape.get("name").is_some(),
            "`prompts/list` must serialize `name`, or the field list on row 2 of \
             WORKER_GUIDANCE_SURFACES is describing a shape this crate no longer \
             serves: {whole}"
        );
        assert_eq!(
            rocky_mcp::names_excluded_tool(&whole, &excluded),
            None,
            "worker `prompts/list` entry for '{}' must not name an excluded tool anywhere \
             in what it serves: {whole}",
            prompt.name
        );
    }

    // Surfaces 4 and 5: the `tools/list` entry itself — the DESCRIPTION the
    // worker reads to choose a tool, and the INPUT SCHEMA it reads to fill
    // one in. Both are served text and both were outside every previous
    // sweep; the description half is finding 3, and the schema half is
    // swept here because it is the same kind of text one field away.
    //
    // TENTH ROUND, finding 2 — those two fields are now swept as the WHOLE
    // `Tool`, for the same reason as surface 2 above. rmcp 3.1.2's `Tool`
    // also carries `title`, `output_schema`, `annotations` (which has its
    // own `title`), `icons` and `_meta`; selecting two of seven fields is
    // not the future-field guarantee the enumeration claimed. Serializing
    // the whole value keeps the nested-schema coverage the field-selecting
    // version bought — a doc comment on any parameter field is still
    // covered wherever schemars put it, at any depth.
    let tools = client.list_all_tools().await.expect("list tools");
    assert_eq!(
        tools.len(),
        worker_tools.len(),
        "the wire surface is the router surface, or this sweeps the wrong set"
    );
    for tool in &tools {
        let whole = serde_json::to_string(tool).expect("tool serializes");
        assert_eq!(
            rocky_mcp::names_excluded_tool(&whole, &excluded),
            None,
            "worker `tools/list` entry for '{}' must not name an excluded tool anywhere in \
             what it serves: {whole}",
            tool.name
        );
        // The description and the input schema are the two fields that ever
        // carried a violation, so their PRESENCE is pinned: a `Tool` that
        // served neither would satisfy the sweep above while sweeping
        // nothing.
        assert!(
            tool.description.is_some(),
            "`{}` serves no description — the sweep above would then be reading an \
             envelope with no guidance in it",
            tool.name
        );
        assert!(
            whole.contains("input_schema") || whole.contains("inputSchema"),
            "`{}`'s serialized entry must carry its input schema, or the nested-parameter \
             coverage is gone: {whole}",
            tool.name
        );
    }

    // Surface 7: the draft tools' next_steps. (Numbered 6 here until the
    // ninth round, while the sibling test in this file correctly numbered
    // `output_schema` 6 — the file disagreed with itself.)
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
    // `draft_check` must NOT be served here. A check's `expression` is
    // raw-interpolated into the SQL the loop executes unattended after every
    // apply, so a check served to an untrusted worker is SQL the warehouse
    // later runs with credentials. This asserts it at the PROTOCOL level —
    // absent from the router the worker actually talks to, not merely
    // filtered out of a constant a test could read back to itself.
    //
    // WHAT THIS PROVES, AND WHAT IT DOES NOT. It proves the MCP route is
    // closed. It does NOT prove the worker cannot author a check. The
    // subprocess driver runs an arbitrary command with the project root as
    // its working directory and no filesystem confinement, and Phase B
    // PRESERVES a worker-added `[[tests]]` block rather than discarding it.
    // A worker holding a file writer can still write the sidecar. That is
    // the conceded local-process boundary, tracked by #1491 (an OS sandbox
    // for the worker) and #1515 (trusted custody); the post-apply custody
    // digest is what catches a sidecar changed after verify.
    let refused = client
        .call_tool(CallToolRequestParams::new("draft_check").with_arguments(check_args))
        .await;
    let err = refused.expect_err("draft_check must not be served to the worker profile");
    assert!(
        err.to_string().contains("tool not found"),
        "draft_check must be refused as absent, not fail some other way: {err}"
    );

    // ONE producer, deliberately — `draft_model` is the only worker-served
    // tool that returns a `next_steps`. `draft_check` carries one too and is
    // swept at unit level instead, because the assertion just above proves
    // it is unreachable over this wire. Written straight through rather than
    // as a one-element loop, so the count is visible instead of implied.
    let next_steps = draft.structured_content.as_ref().expect("structured")["next_steps"]
        .as_str()
        .expect("next_steps is a string")
        .to_string();
    assert_eq!(
        rocky_mcp::names_excluded_tool(&next_steps, &excluded),
        None,
        "worker `draft_model` next_steps must not name an excluded tool: {next_steps}"
    );
    assert!(
        next_steps.contains("hand-off to the trusted runner"),
        "worker `draft_model` next_steps end at the runner hand-off: {next_steps}"
    );

    client.cancel().await.unwrap();
}

/// The excluded-tool set, DERIVED from the two real routers: every tool the
/// default profile serves that the worker profile does not.
fn worker_excluded_tools(config_path: &Path) -> Vec<String> {
    let worker =
        RockyMcpServer::new_with_profile(config_path.to_path_buf(), rocky_mcp::McpProfile::Worker)
            .tool_names();
    RockyMcpServer::new(config_path.to_path_buf())
        .tool_names()
        .into_iter()
        .filter(|name| !worker.contains(name))
        .collect()
}

/// F3 red team round 2, finding 2 — the SUCCESSFUL-RESULT guidance surface,
/// which every previous count omitted.
///
/// The enumeration counted `next_steps` and the error `remediation_hint`. It
/// did not count the free text a tool carries when it SUCCEEDS: diagnostic
/// messages and suggestions, breaking-change finding messages, the
/// skipped-gate reason, test-failure text, unavailability reasons. That text
/// is guidance by any reading — a worker acts on a `suggestion` exactly as
/// it acts on a `next_steps`.
///
/// And it already misfired. Four E027 budget constructors suggested "or
/// optimize the query to reduce scan volume"; `optimize` is a tool this
/// profile does not serve. The previous sweep drove one GREEN `draft_model`
/// and never a red compile, so it could not see it.
///
/// TWO PRODUCERS, not one. `compile` and `draft_model` both return
/// `diagnostics: Vec<DiagnosticLite>`, so the same diagnostic text reaches a
/// worker by two routes. Both are driven red here.
///
/// WHAT THIS PROVES AND WHAT IT DOES NOT — the honest half, because
/// "everything is swept" is the sentence this enumeration exists to stop.
///
///  - It proves the results of the paths driven below carry no excluded
///    name. Every result is swept as its WHOLE serialized JSON, so a field
///    added later is covered without this test knowing the shape.
///  - UNFINISHED AUDIT COVERAGE, not a boundary. It does NOT prove every
///    Rocky-authored diagnostic is clean. There is no table of diagnostic
///    text to audit: constructors are written per call site across
///    rocky-compiler, rocky-core and rocky-cli, for consumers that are
///    mostly not this worker. Reaching all of them means driving every
///    constructor, which this harness does not — the same reason surface 9
///    is OPEN. That is work not yet done, and it closes by doing it.
///  - A REAL LEXICAL BOUNDARY, which never closes. It CANNOT prove the
///    interpolated spans are clean, because a diagnostic quotes the user's
///    own model and column names. A project containing an identifier that
///    IS an excluded tool name — a model literally called `propose` —
///    produces text no rule Rocky ships can fix.
///
///    The example this comment used to give was WRONG: `propose_v2` does
///    NOT collide, because `_` is an identifier byte, so the boundary rule
///    rejects it exactly as it rejects `proposal_id` and `propose_only`.
///    Only an EXACT identifier collides. Corrected rather than dropped — a
///    wrong example makes a true boundary look invented.
///
/// The two are stated separately because reporting them as one PARTIAL let
/// the unfinished audit borrow the boundary's excuse.
#[tokio::test]
async fn worker_result_text_names_no_excluded_tool() {
    let dir = TempDir::new().unwrap();
    // The DuckDB file stem must equal the declared catalog (`warehouse`) so
    // the three-part ref `warehouse.out.orders` resolves — same convention
    // the sample_rows tests use.
    let db_path = dir.path().join("warehouse.duckdb");
    write_project(dir.path(), &db_path);
    write_target_defaults(dir.path());
    // Make `compile` RED, and red with a SUGGESTION — a breached `[budget]`
    // ceiling is E027, whose suggestion is prose Rocky writes rather than a
    // name it echoes back. That is the text that was wrong.
    std::fs::write(
        dir.path().join("models").join("orders.toml"),
        "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \
         \"warehouse\"\nschema = \"out\"\ntable = \"orders\"\n\n[budget]\nmax_usd = \
         0.0000001\nmax_bytes_scanned = 1\n",
    )
    .unwrap();
    materialize_orders(&db_path, "out", "(1,'COMPLETE')").await;

    let config_path = dir.path().join("rocky.toml");
    let excluded = worker_excluded_tools(&config_path);
    let server = RockyMcpServer::new_with_profile(config_path, rocky_mcp::McpProfile::Worker);
    let client = connect(server).await;

    // THE PROBE MUST EXHIBIT THE CONDITION. A green compile would sweep text
    // that carries no diagnostic at all and report success either way, which
    // is precisely how the old sweep stayed green over a real violation.
    let compile = client
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call");
    let compiled = compile.structured_content.as_ref().expect("structured");
    assert_eq!(
        compiled["has_errors"],
        serde_json::json!(true),
        "the fixture must compile RED or this sweeps text that was never emitted: {compiled}"
    );
    let diagnostics = compiled["diagnostics"]
        .as_array()
        .expect("diagnostics array");
    assert!(
        diagnostics
            .iter()
            .any(|d| d["code"] == serde_json::json!("E027")),
        "the budget breach must surface as E027: {compiled}"
    );
    assert!(
        diagnostics.iter().any(|d| d.get("suggestion").is_some()),
        "at least one diagnostic must carry a SUGGESTION — the field the \
         violation lived in: {compiled}"
    );

    // The second producer: `draft_model` carries its own `diagnostics`, so a
    // diagnostic reaches the worker by two routes. Drive it red too.
    //
    // Re-drafting `orders` is what makes it red, and deliberately so:
    // `draft_model` preserve-merges an existing sidecar, so the `[budget]`
    // block survives the write and the same E027 lands in this result. Bad
    // SQL would NOT work — Rocky's inference is best-effort and an unknown
    // column against a known upstream is `Unknown`, not an error (verified:
    // `SELECT missing_col FROM orders` compiles clean).
    let draft = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 1 AS id, 'COMPLETE' AS status",
                "re-draft, to sweep draft_model's own diagnostics",
            )),
        )
        .await
        .expect("draft_model call");
    let drafted = draft.structured_content.as_ref().expect("structured");
    assert_eq!(
        drafted["has_errors"],
        serde_json::json!(true),
        "the re-draft must compile RED or draft_model's diagnostics are empty: {drafted}"
    );
    assert!(
        drafted["diagnostics"]
            .as_array()
            .expect("diagnostics array")
            .iter()
            .any(|d| d["code"] == serde_json::json!("E027") && d.get("suggestion").is_some()),
        "draft_model must carry the SAME diagnostic text `compile` does, suggestion \
         included — that is what makes it a second route: {drafted}"
    );

    // `breaking_change` in a non-git tree sets `skipped_reason` — a distinct
    // free-text field on a SUCCESSFUL result, and one no previous sweep read.
    let breaking = client
        .call_tool(CallToolRequestParams::new("breaking_change"))
        .await
        .expect("breaking_change call");
    let skipped = breaking.structured_content.as_ref().expect("structured");
    assert!(
        skipped.get("skipped_reason").is_some(),
        "the non-git fixture must set skipped_reason or that field goes unswept: {skipped}"
    );

    // Now sweep those three, plus every other worker tool that runs offline
    // here, as WHOLE serialized results.
    //
    // TENTH ROUND, finding 2 — "whole" now means the whole `CallToolResult`,
    // not its `structured_content`. The three assertions above still read
    // the structured half, because they are about a specific field's value;
    // the SWEEP takes the envelope rmcp actually serialises, which also
    // carries `content` (the text block a client renders when it ignores
    // structured output), `is_error`, `result_type` and `_meta`. Selecting
    // one field of five is not the future-field guarantee the enumeration
    // claimed for every row.
    let mut results = vec![
        (
            "compile",
            serde_json::to_value(&compile).expect("serializes"),
        ),
        (
            "draft_model",
            serde_json::to_value(&draft).expect("serializes"),
        ),
        (
            "breaking_change",
            serde_json::to_value(&breaking).expect("serializes"),
        ),
    ];
    let model_arg = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let obj = |v: serde_json::Value| v.as_object().unwrap().clone();
    for (tool, args) in [
        ("plan_preview", None),
        ("catalog", None),
        ("inspect_schema", None),
        ("test", None),
        ("list", Some(obj(serde_json::json!({ "kind": "models" })))),
        ("lineage", Some(model_arg.clone())),
        ("dependents", Some(model_arg.clone())),
        ("sample_rows", Some(model_arg.clone())),
        (
            "profile_column",
            Some(obj(
                serde_json::json!({ "model": "orders", "column": "status" }),
            )),
        ),
    ] {
        let mut request = CallToolRequestParams::new(tool);
        if let Some(args) = args {
            request = request.with_arguments(args);
        }
        let called = client
            .call_tool(request)
            .await
            .unwrap_or_else(|e| panic!("`{tool}` call: {e}"));
        // Both halves must be THERE — the sweep below reads the whole
        // envelope, and an envelope with neither would pass it while
        // carrying none of the text this row exists to sweep.
        //
        // `content` is not decorative: `CallToolResult::structured` fills it
        // with `value.to_string()`, so it is a SECOND rendering of the same
        // guidance, and it is what a client that ignores structured output
        // shows the worker. That is the channel reading `structured_content`
        // alone discarded.
        assert!(
            called.structured_content.is_some(),
            "`{tool}` returns structured content, or the whole-envelope sweep below reads \
             an empty result"
        );
        assert!(
            !called.content.is_empty(),
            "`{tool}` returns a text content block too — the second rendering the whole \
             envelope exists to cover"
        );
        results.push((
            tool,
            serde_json::to_value(&called).expect("result serializes"),
        ));
    }
    assert_eq!(
        results.len(),
        12,
        "every one of the 12 worker-served tools is driven, or the sweep has a hole"
    );

    for (tool, result) in &results {
        let whole = serde_json::to_string(result).expect("result serializes");
        assert_eq!(
            rocky_mcp::names_excluded_tool(&whole, &excluded),
            None,
            "worker `{tool}` result text must not name an excluded tool: {whole}"
        );
    }

    // Surface 9 — the ERROR envelope, which the enumeration inventoried as
    // `remediation_hint` alone. A `ToolError` carries `message` too, plus
    // `policy_rule` and the flattened plan-handoff fields, and every one of
    // them is served text. Same fix as row 3: sweep the WHOLE envelope, so
    // a field added to `ToolError` is covered without this test knowing the
    // shape.
    //
    // TENTH ROUND, finding 2 — "whole envelope" now means the whole
    // `CallToolResult`, as with the success sweep above. Reading
    // `structured_content` alone discarded `content`, which is the text
    // block a client renders when it does not read structured output, and
    // that is exactly a channel a worker reads.
    //
    // STILL PARTIAL, and the label is not softened. What is driven here is
    // the ARGUMENT-VALIDATION arm of the tools that have one — the errors a
    // harness can reach offline. Policy denials, warehouse failures and
    // internal errors are not driven, so the row stays OPEN in the
    // enumeration for the reason it always was: the hints are written per
    // call site and there is no table to audit.
    let mut errors = Vec::new();
    for (tool, args) in [
        ("compile", serde_json::json!({ "model": "no_such_model" })),
        (
            "plan_preview",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        ("lineage", serde_json::json!({ "model": "no_such_model" })),
        (
            "dependents",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        // TENTH ROUND, finding 2 — `test` WAS in the "no argument
        // validation" list below, and it does not belong there. It takes an
        // optional `model` and rejects an unknown one as `model_not_found`
        // (`tools.rs`'s `test` arm over `commands::ModelNotFound`, which
        // `commands::test_output` raises through `reject_unknown_model`).
        // So the row drives NINE reachable argument failures, not eight,
        // and the comment that excused the ninth was wrong about the code.
        ("test", serde_json::json!({ "model": "no_such_model" })),
        // `inspect_schema`, `catalog` and `breaking_change` are absent on
        // purpose, and this was RE-VERIFIED rather than inherited: the
        // first two bind their `Parameters` to `_params` and never read
        // them, and `breaking_change` takes a git `base` whose failure mode
        // is a SUCCESSFUL result carrying `skipped_reason` (driven above),
        // not an error. Named here rather than silently omitted — an
        // unexplained gap in a list like this is how the last five rounds
        // started, and an unchecked one is how this entry was wrong.
        (
            "sample_rows",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        (
            "profile_column",
            serde_json::json!({ "model": "orders", "column": "no_such_column" }),
        ),
        ("list", serde_json::json!({ "kind": "not_a_kind" })),
        (
            "draft_model",
            serde_json::json!({ "name": "../escape", "sql": "SELECT 1", "intent": "x" }),
        ),
    ] {
        let called = client
            .call_tool(CallToolRequestParams::new(tool).with_arguments(obj(args)))
            .await
            .unwrap_or_else(|e| panic!("`{tool}` error-path call: {e}"));
        // THE PROBE MUST EXHIBIT THE CONDITION: a call that SUCCEEDED
        // sweeps a success envelope and reports green either way.
        assert_eq!(
            called.is_error,
            Some(true),
            "`{tool}` must FAIL here or this sweeps no error envelope at all: {called:?}"
        );
        let envelope = called
            .structured_content
            .clone()
            .unwrap_or_else(|| panic!("`{tool}` error returns a structured envelope"));
        assert!(
            envelope.get("message").is_some() && envelope.get("remediation_hint").is_some(),
            "`{tool}`'s envelope carries BOTH text fields, or the sweep below is \
             inventorying the wrong thing: {envelope}"
        );
        // The error envelope carries the same second rendering as the
        // success one, and it is the half a non-structured client shows.
        assert!(
            !called.content.is_empty(),
            "`{tool}`'s error result carries a text content block too — the channel \
             reading `structured_content` alone discarded"
        );
        errors.push((tool, serde_json::to_value(&called).expect("serializes")));
    }
    assert_eq!(errors.len(), 9, "nine reachable error paths are driven");
    for (tool, envelope) in &errors {
        let whole = serde_json::to_string(envelope).expect("envelope serializes");
        assert_eq!(
            rocky_mcp::names_excluded_tool(&whole, &excluded),
            None,
            "worker `{tool}` ERROR envelope must not name an excluded tool: {whole}"
        );
    }

    // Surface 6: `output_schema` is NOT served, so the result-type doc
    // comments schemars would put there never reach the worker. That matters
    // because those doc comments DO name excluded tools (`DraftModelResult`'s
    // `next_steps` doc spells out the `propose` chain). Pinned, so opting in
    // fails here rather than silently opening a surface.
    for tool in client.list_all_tools().await.expect("list tools") {
        assert!(
            tool.output_schema.is_none(),
            "`{}` now serves an output_schema — the result-type doc comments are \
             suddenly worker-served text and must be swept before this is enabled",
            tool.name
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
    //
    // A SERVED INSTRUCTION DEPENDS ON THIS, so do not delete it without
    // reading that instruction. The worker-profile guidance
    // (`WORKER_INSTRUCTIONS_REWRITES` in src/tools.rs) tells a worker that a
    // table missing from `inspect_schema`'s `sources` is inconclusive rather
    // than absent — `inspect_schema` degrades to a silent empty list — and
    // sends it here: "Ask `sample_rows` for that table before you conclude it
    // is absent." That only holds while a dotted target reaches the raw
    // source without a compile, which is exactly what this test proves.
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

/// SIXTEENTH ROUND, finding 3 — `resultType` coverage was OBSERVED, never
/// GUARDED.
///
/// The fifteenth round corrected a false justification in `tools.rs`: the
/// server does NOT pin `2024-11-05`, it advertises rmcp's whole
/// `KNOWN_VERSIONS` list, and `negotiate_protocol_version` hands a client back
/// whatever it asked for when the server supports it. So a client that
/// requests `2026-07-28` gets it, `sep_2322_supported` is true,
/// `strip_result_type_for_legacy_peer()` is skipped, and `resultType` reaches
/// that client.
///
/// That correction was right and completely unexercised: every roundtrip in
/// this file connects with rmcp's default `()` handler, which asks for
/// `ProtocolVersion::LATEST` (`2025-11-25`). The stripping was being credited
/// to a mechanism no test drove.
///
/// BOTH DIRECTIONS, in one test, deliberately. A present-only assertion would
/// still pass if serde stopped emitting the field for everyone, and an
/// absent-only assertion would still pass if the server narrowed
/// `supported_protocol_versions` and stopped speaking `2026-07-28` at all.
/// Neither alone distinguishes "negotiated per peer" from "off everywhere".
///
/// The negotiated version is asserted first on each connection, because
/// `result_type` says nothing if the handshake did not land where the test
/// thinks it did.
///
/// This does not change what the server SPEAKS. Closing the gap by
/// construction would mean narrowing `supported_protocol_versions`, which is a
/// behaviour change and is still not made here.
#[tokio::test]
async fn result_type_reaches_a_2026_07_28_client_and_no_other() {
    use rmcp::model::{ProtocolVersion, ResultType};

    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let config_path = dir.path().join("rocky.toml");

    // 1. A client that ASKS for 2026-07-28 is given it, and keeps `resultType`.
    let modern = connect_at_version(
        RockyMcpServer::new(config_path.clone()),
        ProtocolVersion::V_2026_07_28,
    )
    .await;
    let negotiated = modern
        .peer_info()
        .expect("the server returned an initialize result")
        .protocol_version
        .clone();
    assert_eq!(
        negotiated,
        ProtocolVersion::V_2026_07_28,
        "the server advertises rmcp's whole KNOWN_VERSIONS list and does not \
         override supported_protocol_versions, so a client asking for \
         2026-07-28 must be given it; if this fails the rest of the test is \
         measuring the wrong session"
    );
    let modern_result = modern
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call returns a result");
    assert_eq!(
        modern_result.result_type,
        Some(ResultType::COMPLETE),
        "a peer that negotiated 2026-07-28 skips \
         strip_result_type_for_legacy_peer, so resultType is on the wire for \
         it: {modern_result:?}"
    );
    modern.cancel().await.unwrap();

    // 2. The default `()` client every other test in this file uses asks for
    //    `LATEST` (2025-11-25), which is older, so the field is stripped.
    let legacy = connect(RockyMcpServer::new(config_path)).await;
    let legacy_negotiated = legacy
        .peer_info()
        .expect("the server returned an initialize result")
        .protocol_version
        .clone();
    assert_eq!(
        legacy_negotiated,
        ProtocolVersion::LATEST,
        "the default handler asks for rmcp's LATEST; if that constant moves \
         past 2026-07-28 this whole test inverts and the tools.rs note about \
         'no client asks for it yet' has to be re-read"
    );
    assert!(
        legacy_negotiated.as_str() < ProtocolVersion::V_2026_07_28.as_str(),
        "the default client's version must be OLDER than 2026-07-28 for the \
         strip to apply at all: {legacy_negotiated:?}"
    );
    let legacy_result = legacy
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call returns a result");
    assert_eq!(
        legacy_result.result_type, None,
        "a peer on an older negotiated version has resultType stripped: \
         {legacy_result:?}"
    );

    // Non-vacuity: the two calls differ ONLY in `resultType`. Without this a
    // failure to produce any result at all would satisfy both assertions.
    assert_eq!(
        legacy_result.structured_content, modern_result.structured_content,
        "the two peers must receive the SAME tool result; only the protocol \
         discriminator may differ"
    );
    assert!(
        legacy_result.structured_content.is_some(),
        "the compile call must actually return content, or both assertions \
         above are about an empty result"
    );

    legacy.cancel().await.unwrap();
}

/// SIXTEENTH ROUND, finding 1 — the two tools that read `self.config_path`
/// disagreed about a `rocky.toml` that exists but does not load.
///
/// `main`'s #1521/#1522 made `compile_inner` refuse it (`FileNotFound` is the
/// only tolerated `ConfigError`), and `compile` uses that path. Its sibling
/// `plan_preview` discarded EVERY config error with `.ok()` and fell through to
/// the DuckDB default. So one malformed Snowflake config failed `compile` while
/// `plan_preview` returned confident DuckDB-rendered SQL for a project whose
/// config cannot be read.
///
/// No test failure could ever have caught that: both paths passed. It is a
/// semantic gap between two siblings, which is why the assertion here is that
/// they AGREE — not merely that each one does something.
///
/// The config is Snowflake and the models are valid, so the dialect is the
/// only thing the swallowed error changed. A `plan_preview` that still
/// swallowed it would return `is_error: None` and a `CREATE OR REPLACE TABLE`
/// in the wrong dialect, and every other assertion in this file would stay
/// green.
#[tokio::test]
async fn a_malformed_config_refuses_on_plan_preview_the_way_it_does_on_compile() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let config_path = dir.path().join("rocky.toml");

    // Baseline FIRST, on the well-formed project this harness writes: both
    // tools succeed. Without it, "both error" could be true of a fixture that
    // never worked, and the guard would prove nothing about the config.
    let server = RockyMcpServer::new(config_path.clone());
    let client = connect(server).await;
    for tool in ["compile", "plan_preview"] {
        let ok = client
            .call_tool(CallToolRequestParams::new(tool))
            .await
            .unwrap_or_else(|e| panic!("{tool} baseline call returns a result: {e}"));
        assert_ne!(
            ok.is_error,
            Some(true),
            "{tool} must succeed on the well-formed fixture, or this test's \
             failure assertions prove nothing: {ok:?}"
        );
    }
    client.cancel().await.unwrap();

    // A Snowflake target, then broken: an unterminated table header, so TOML
    // parsing fails rather than the file going missing.
    std::fs::write(
        &config_path,
        "[adapter.default\ntype = \"snowflake\"\naccount = \"acct\"\n",
    )
    .unwrap();

    let server = RockyMcpServer::new(config_path);
    let client = connect(server).await;
    for tool in ["compile", "plan_preview"] {
        let result = client
            .call_tool(CallToolRequestParams::new(tool))
            .await
            .unwrap_or_else(|e| panic!("{tool} call returns a result: {e}"));
        assert_eq!(
            result.is_error,
            Some(true),
            "{tool} must refuse a rocky.toml that does not load; returning SQL \
             in the DEFAULT dialect for a project whose config cannot be read \
             answers a different question than the caller asked: {result:?}"
        );
        let err = result
            .structured_content
            .unwrap_or_else(|| panic!("{tool} error carries structured_content"));
        // The message has to NAME the config, not merely be non-empty — an
        // agent that cannot tell "your rocky.toml is broken" from "your SQL is
        // broken" will go and edit the model.
        let message = err["message"].as_str().unwrap_or_default();
        assert!(
            message.contains("config") || message.contains("rocky.toml"),
            "{tool}'s refusal must name the config as the cause so the caller \
             fixes the right file, got: {message:?}"
        );
        assert!(
            !message.trim().is_empty() && err["code"].as_str().is_some_and(|c| !c.is_empty()),
            "{tool} keeps the structured envelope on a config failure: {err:?}"
        );
    }

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

// ---------------------------------------------------------------------------
// The served-text golden.
// ---------------------------------------------------------------------------

/// Where the golden lives. Read and re-blessed through the SAME constant, so
/// a bless cannot land in a different file from the one the test compares
/// against.
const SERVED_TEXT_GOLDEN: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/served_text.golden"
);

/// Set this to re-bless the golden. It is read, never written, so the test
/// needs no `unsafe` env mutation and CI (where it is unset) can only compare.
const BLESS_VAR: &str = "ROCKY_BLESS_MCP_SERVED_TEXT";

/// Does this value of [`BLESS_VAR`] ask for a re-bless?
///
/// FIFTEENTH ROUND, finding 3 — **only `1`**, after trimming. The previous
/// rule was "nonempty and not `0`", which blesses on `false`, on `no`, and
/// on every other value a person could write meaning *don't*. A stray
/// `ROCKY_BLESS_MCP_SERVED_TEXT=false` in a shell profile or a CI `env:`
/// block would then rewrite the golden on every run and this guard would
/// pass forever without anyone reading a word of the text it protects.
///
/// The earlier fix stopped an EMPTY value from blessing, for the same
/// reason one step short: an unset-looking value must not mean yes. An
/// UNRECOGNISED value must not mean yes either — the two are the same
/// mistake, and only the allowlist form is closed against the next value
/// nobody thought of.
///
/// Fails CLOSED: anything this does not recognise compares instead of
/// writing, so the worst case of a typo is a test failure telling you how
/// to bless, never a silent rubber stamp.
///
/// A free function over `Option<&str>` rather than a predicate inline in
/// the test body, so the rule is testable without mutating the process
/// environment — see `bless_requires_an_explicit_one`.
fn should_bless(value: Option<&str>) -> bool {
    value.is_some_and(|v| v.trim() == "1")
}

/// FIFTEENTH ROUND — the path normalizer, driven directly.
///
/// It has NO live call site that changes anything: no worker call payload
/// carries a path today (see [`normalize_run_paths`]). A helper whose only
/// caller never exercises it is a helper nobody has checked, so it is
/// checked here rather than trusted.
///
/// The longest-first ordering is the case worth pinning. On macOS the raw
/// root is a PREFIX of nothing, but the canonical root
/// (`/private/var/…`) CONTAINS the raw one (`/var/…`) as a substring — so
/// replacing the shorter first would leave `/private<TMP>` behind, which
/// still embeds nothing per-run but is not the sentinel either.
#[test]
fn normalize_run_paths_replaces_every_root_longest_first() {
    let roots = vec![
        "/private/var/folders/ab/T/.tmpXYZ".to_string(),
        "/var/folders/ab/T/.tmpXYZ".to_string(),
    ];
    assert_eq!(
        normalize_run_paths("wrote /var/folders/ab/T/.tmpXYZ/models/a.sql", &roots),
        "wrote <TMP>/models/a.sql",
        "the raw root is replaced"
    );
    assert_eq!(
        normalize_run_paths(
            "wrote /private/var/folders/ab/T/.tmpXYZ/models/a.sql",
            &roots
        ),
        "wrote <TMP>/models/a.sql",
        "the canonical root is replaced WHOLE — a shorter root must not eat its own prefix"
    );
    assert_eq!(
        normalize_run_paths("no path here", &roots),
        "no path here",
        "text without a root is passed through untouched"
    );

    // And the ordering the helper itself produces, not just the one this
    // test hand-wrote: `temp_roots` must sort longest-first or the case
    // above regresses without this test noticing.
    let dir = TempDir::new().unwrap();
    let ordered = temp_roots(dir.path());
    assert!(
        ordered.windows(2).all(|w| w[0].len() >= w[1].len()),
        "temp_roots must return longest-first: {ordered:?}"
    );
}

/// FIFTEENTH ROUND, finding 3 — the bless switch is an ALLOWLIST.
///
/// Driven over the values a person or a CI file actually writes. `false`
/// and `no` are the ones that matter: under the previous "nonempty and not
/// `0`" rule both blessed, which is the inverse of what either word means.
#[test]
fn bless_requires_an_explicit_one() {
    for asks in ["1", " 1 ", "\t1\n"] {
        assert!(
            should_bless(Some(asks)),
            "`{asks}` asks for a re-bless and must be honoured, or the documented \
             instruction in the failure message does not work"
        );
    }
    for refuses in [
        "", " ", "0", "false", "no", "off", "FALSE", "true", "yes", "y", "2",
    ] {
        assert!(
            !should_bless(Some(refuses)),
            "`{refuses}` is not an explicit `1`; blessing on it turns the golden into a \
             rubber stamp that nobody asked for"
        );
    }
    assert!(
        !should_bless(None),
        "an unset variable is how CI runs; it must compare, never write"
    );
}

/// Digest one payload under `key`, refusing anything that would make the
/// golden per-run garbage rather than a pin.
///
/// `nonce` is the temp directory's unique final component. It is checked
/// AFTER any normalization the caller applies, so a path the caller thought
/// it had replaced fails here instead of being blessed as a fresh hash every
/// run. That ordering is the whole value of the check: it turns "I believe I
/// normalized every run-dependent value" into something the test decides.
fn record(
    out: &mut std::collections::BTreeMap<String, String>,
    key: &str,
    payload: &str,
    nonce: &str,
) {
    assert!(
        !payload.is_empty(),
        "'{key}' serialized to nothing; a golden over an empty payload pins nothing"
    );
    assert!(
        !payload.contains(nonce),
        "'{key}' embeds the per-run temp path; the golden would drift every run: {payload}"
    );
    let previous = out.insert(
        key.to_string(),
        blake3::hash(payload.as_bytes()).to_hex().to_string(),
    );
    assert!(previous.is_none(), "duplicate golden key '{key}'");
}

/// Replace this run's temporary roots with a fixed sentinel.
///
/// EXACT REPLACEMENT, not a pattern. A regex over "things that look like a
/// path" would quietly absorb a genuine change to served text; replacing the
/// literal root this run was given cannot, because any other value survives
/// and is caught either by [`record`]'s nonce check or by the golden itself.
///
/// Both the raw and the canonicalized root are replaced. On macOS `TempDir`
/// hands out `/var/folders/…` while a canonicalized path comes back as
/// `/private/var/folders/…`, and a payload can carry either.
///
/// IT DOES NOT FIRE TODAY, and saying so is the point rather than an
/// apology. Every one of the 21 worker call payloads was dumped and read
/// while this was written: not one contains an absolute path, a timestamp,
/// a duration or an id. `draft_model` reports a bare model NAME, `test`
/// reports counts with no timings, and `breaking_change`'s `skipped_reason`
/// names no path. That is the grounding for pinning these rows at all — the
/// exclusion assumed run-dependent payloads that this profile does not
/// produce.
///
/// So this is here for the field that has not been added yet, and the
/// trade is deliberate: it ABSORBS a temp root silently rather than failing
/// on it. [`record`]'s nonce check is the half that cannot be absorbed, and
/// a value this misses fails there instead of blessing.
fn normalize_run_paths(payload: &str, roots: &[String]) -> String {
    let mut out = payload.to_string();
    for root in roots {
        out = out.replace(root, "<TMP>");
    }
    out
}

/// The temp roots [`normalize_run_paths`] should replace, longest first so a
/// prefix cannot shadow the longer form it is a prefix of.
fn temp_roots(dir: &Path) -> Vec<String> {
    let mut roots = vec![dir.to_string_lossy().to_string()];
    if let Ok(canonical) = dir.canonicalize() {
        roots.push(canonical.to_string_lossy().to_string());
    }
    roots.sort_by_key(|r| std::cmp::Reverse(r.len()));
    roots.dedup();
    roots
}

/// Digest every worded surface one profile serves, keyed by surface.
///
/// Keys carry no profile prefix — the caller adds one — so the approver
/// surface can be compared against the default surface key-for-key.
async fn served_text_digests(
    config_path: &Path,
    profile: rocky_mcp::McpProfile,
    nonce: &str,
) -> std::collections::BTreeMap<String, String> {
    let server = RockyMcpServer::new_with_profile(config_path.to_path_buf(), profile);
    let client = connect(server).await;
    let mut out = std::collections::BTreeMap::new();

    // Surface 1 — the WHOLE `InitializeResult`, not its `instructions`.
    //
    // FIFTEENTH ROUND, finding 2 — this hashed `instructions` alone, which
    // is field selection under a heading that claims a channel. That is
    // the exact defect the eleventh round found in the sweeps and this
    // golden was built to catch: the guard against the class had
    // instantiated the class.
    //
    // What the omission left unpinned is real. `initialize` also carries
    // `protocolVersion`, `capabilities`, `serverInfo` and `_meta`, and the
    // `serverInfo` `Implementation` carries `title`, `description`, `icons`
    // and `websiteUrl` besides its name and version. A server title or
    // description added later is text a client shows the operator, and
    // under the old key it moved without moving the golden.
    //
    // Pinning the whole value is CHEAP here, which is why there is no
    // carve-out. `Implementation::from_build_env()` expands `env!` inside
    // rmcp, so `serverInfo` is rmcp's own name and version — NOT
    // rocky-mcp's — and the row therefore does not churn on a Rocky release
    // bump. It moves on an rmcp upgrade, which is a change that should
    // force someone to re-read what this server announces.
    //
    // The banner is NOT spliced out: this golden is about drift, and a
    // banner that changes because the allowlist changed is exactly the kind
    // of change that should need a re-bless.
    let initialize = client.peer_info().expect("every profile serves peer info");
    assert!(
        initialize.instructions.is_some(),
        "every profile serves instructions; a golden over an initialize result without \
         them pins the wrong thing"
    );
    record(
        &mut out,
        "initialize",
        &serde_json::to_string(&*initialize).expect("initialize result serializes"),
        nonce,
    );

    // Surfaces 2 and 3 — the whole listed `Prompt`, and the whole
    // `prompts/get` result for it. Arguments are synthesised from each
    // prompt's own declared list, the same way the sweeps above do it, so a
    // new prompt is covered with nothing to remember.
    let prompts = client.list_all_prompts().await.expect("list prompts");
    assert!(
        !prompts.is_empty(),
        "a profile that serves no prompt pins nothing"
    );
    for prompt in &prompts {
        record(
            &mut out,
            &format!("prompts/list/{}", prompt.name),
            &serde_json::to_string(prompt).expect("prompt serializes"),
            nonce,
        );
        let mut args = serde_json::Map::new();
        for declared in prompt.arguments.iter().flatten() {
            let value = if declared.name == "model" {
                "orders"
            } else {
                "daily revenue"
            };
            args.insert(declared.name.clone(), serde_json::json!(value));
        }
        let mut params = GetPromptRequestParams::new(prompt.name.clone());
        if !args.is_empty() {
            params = params.with_arguments(args);
        }
        let result = client
            .get_prompt(params)
            .await
            .unwrap_or_else(|e| panic!("get_prompt {}: {e}", prompt.name));
        record(
            &mut out,
            &format!("prompts/get/{}", prompt.name),
            &serde_json::to_string(&result).expect("prompt result serializes"),
            nonce,
        );
    }

    // Surfaces 4 and 5 — the whole listed `Tool`: description AND input
    // schema, plus every other field serde emits.
    let tools = client.list_all_tools().await.expect("list tools");
    assert!(
        !tools.is_empty(),
        "a profile that serves no tool pins nothing"
    );
    for tool in &tools {
        record(
            &mut out,
            &format!("tools/list/{}", tool.name),
            &serde_json::to_string(tool).expect("tool serializes"),
            nonce,
        );
    }

    client.cancel().await.unwrap();
    out
}

/// Digest the whole serialized `CallToolResult` of every `tools/call` a
/// worker can reach — surfaces 7, 8 and 9.
///
/// FIFTEENTH ROUND — these were excluded from the golden on the argument
/// that their payloads embed run-dependent values (paths, plan ids,
/// timestamps), so a digest over them would drift every run and get blessed
/// reflexively. The argument is sound in general and does not hold on THIS
/// set, which is the check the exclusion skipped: the plan- and
/// timestamp-producing tools are `propose`, `optimize` and the rest of the
/// withheld set, and the worker profile does not serve any of them. What is
/// left run-dependent is the temp root, and replacing it exactly is cheap.
///
/// WORKER ONLY, deliberately. The default profile serves the tools the
/// exclusion was really about, so driving it here would import exactly the
/// drift the reviewer established is absent from the worker surface. Rows
/// 1–5 stay on both profiles; these three are worker-scoped, and the golden
/// keys say so.
///
/// The fixture MIRRORS `worker_result_text_names_no_excluded_tool`: the same
/// budget-breached sidecar so `compile` and `draft_model` are RED with an
/// E027 suggestion, and the same non-git tree so `breaking_change` sets
/// `skipped_reason`. A golden over a green fixture would pin text that was
/// never emitted, which is the failure that sweep already had to correct.
///
/// ROW 7 GETS ITS OWN KEY even though its text sits inside row 8's envelope.
/// The row exists because `next_steps` is profile-selected and has its own
/// failure mode; a separate key makes a change to it legible in the drift
/// report instead of hiding inside a whole-envelope hash.
async fn worker_call_digests(
    dir: &Path,
    nonce: &str,
) -> std::collections::BTreeMap<String, String> {
    let roots = temp_roots(dir);
    let db_path = dir.join("warehouse.duckdb");
    write_project(dir, &db_path);
    write_target_defaults(dir);
    std::fs::write(
        dir.join("models").join("orders.toml"),
        "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \
         \"warehouse\"\nschema = \"out\"\ntable = \"orders\"\n\n[budget]\nmax_usd = \
         0.0000001\nmax_bytes_scanned = 1\n",
    )
    .unwrap();
    materialize_orders(&db_path, "out", "(1,'COMPLETE')").await;

    let config_path = dir.join("rocky.toml");
    let server = RockyMcpServer::new_with_profile(config_path, rocky_mcp::McpProfile::Worker);
    let client = connect(server).await;
    let mut out = std::collections::BTreeMap::new();

    let digest = |out: &mut std::collections::BTreeMap<String, String>,
                  key: String,
                  value: &serde_json::Value| {
        let whole = serde_json::to_string(value).expect("result serializes");
        record(out, &key, &normalize_run_paths(&whole, &roots), nonce);
    };

    // Surface 8, the diagnostic route. RED on purpose — see the fixture note
    // above.
    let compile = client
        .call_tool(CallToolRequestParams::new("compile"))
        .await
        .expect("compile call");
    assert_eq!(
        compile.structured_content.as_ref().expect("structured")["has_errors"],
        serde_json::json!(true),
        "the fixture must compile RED or this pins text that was never emitted"
    );

    // Surfaces 7 and 8 together — `draft_model` is the one worker-served
    // producer of `next_steps`.
    let draft = client
        .call_tool(
            CallToolRequestParams::new("draft_model").with_arguments(draft_args(
                "orders",
                "SELECT 1 AS id, 'COMPLETE' AS status",
                "re-draft, to pin draft_model's own served text",
            )),
        )
        .await
        .expect("draft_model call");
    let drafted = draft.structured_content.as_ref().expect("structured");
    assert_eq!(
        drafted["has_errors"],
        serde_json::json!(true),
        "the re-draft must compile RED or draft_model's diagnostics are empty"
    );
    let next_steps = drafted["next_steps"]
        .as_str()
        .expect("draft_model carries next_steps — surface 7 has no other producer");
    record(
        &mut out,
        "tools/call/ok/draft_model.next_steps",
        &normalize_run_paths(next_steps, &roots),
        nonce,
    );

    let breaking = client
        .call_tool(CallToolRequestParams::new("breaking_change"))
        .await
        .expect("breaking_change call");
    assert!(
        breaking
            .structured_content
            .as_ref()
            .expect("structured")
            .get("skipped_reason")
            .is_some(),
        "the non-git fixture must set skipped_reason or that free text goes unpinned"
    );

    digest(
        &mut out,
        "tools/call/ok/compile".to_string(),
        &serde_json::to_value(&compile).expect("serializes"),
    );
    digest(
        &mut out,
        "tools/call/ok/draft_model".to_string(),
        &serde_json::to_value(&draft).expect("serializes"),
    );
    digest(
        &mut out,
        "tools/call/ok/breaking_change".to_string(),
        &serde_json::to_value(&breaking).expect("serializes"),
    );

    let model_arg = serde_json::json!({ "model": "orders" })
        .as_object()
        .unwrap()
        .clone();
    let obj = |v: serde_json::Value| v.as_object().unwrap().clone();
    let mut ok_count = 3;
    for (tool, args) in [
        ("plan_preview", None),
        ("catalog", None),
        ("inspect_schema", None),
        ("test", None),
        ("list", Some(obj(serde_json::json!({ "kind": "models" })))),
        ("lineage", Some(model_arg.clone())),
        ("dependents", Some(model_arg.clone())),
        ("sample_rows", Some(model_arg.clone())),
        (
            "profile_column",
            Some(obj(
                serde_json::json!({ "model": "orders", "column": "status" }),
            )),
        ),
    ] {
        let mut request = CallToolRequestParams::new(tool);
        if let Some(args) = args {
            request = request.with_arguments(args);
        }
        let called = client
            .call_tool(request)
            .await
            .unwrap_or_else(|e| panic!("`{tool}` call: {e}"));
        assert!(
            called.structured_content.is_some() && !called.content.is_empty(),
            "`{tool}` returns both renderings, or this pins half an envelope"
        );
        digest(
            &mut out,
            format!("tools/call/ok/{tool}"),
            &serde_json::to_value(&called).expect("serializes"),
        );
        ok_count += 1;
    }
    assert_eq!(
        ok_count, 12,
        "all 12 worker-served tools are driven, or the golden has the same hole the \
         name-based sweep closed"
    );

    // Surface 9 — the argument-validation arm of the nine worker-served
    // tools that have one. Still PARTIAL for the reason the enumeration
    // states: policy denials, warehouse failures and internal errors are not
    // reachable from this harness.
    let mut err_count = 0;
    for (tool, args) in [
        ("compile", serde_json::json!({ "model": "no_such_model" })),
        (
            "plan_preview",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        ("lineage", serde_json::json!({ "model": "no_such_model" })),
        (
            "dependents",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        ("test", serde_json::json!({ "model": "no_such_model" })),
        (
            "sample_rows",
            serde_json::json!({ "model": "no_such_model" }),
        ),
        (
            "profile_column",
            serde_json::json!({ "model": "orders", "column": "no_such_column" }),
        ),
        ("list", serde_json::json!({ "kind": "not_a_kind" })),
        (
            "draft_model",
            serde_json::json!({ "name": "../escape", "sql": "SELECT 1", "intent": "x" }),
        ),
    ] {
        let called = client
            .call_tool(CallToolRequestParams::new(tool).with_arguments(obj(args)))
            .await
            .unwrap_or_else(|e| panic!("`{tool}` error-path call: {e}"));
        assert_eq!(
            called.is_error,
            Some(true),
            "`{tool}` must FAIL here or this pins a success envelope: {called:?}"
        );
        digest(
            &mut out,
            format!("tools/call/err/{tool}"),
            &serde_json::to_value(&called).expect("serializes"),
        );
        err_count += 1;
    }
    assert_eq!(err_count, 9, "nine reachable error paths are pinned");

    client.cancel().await.unwrap();
    out
}

/// Render a digest table as the golden's on-disk form: `key<TAB>hash`, one
/// per line, sorted (a `BTreeMap` iterates in key order).
fn render_golden(table: &std::collections::BTreeMap<String, String>) -> String {
    let mut out = String::new();
    for (key, hash) in table {
        out.push_str(key);
        out.push('\t');
        out.push_str(hash);
        out.push('\n');
    }
    out
}

/// FOURTEENTH ROUND, finding 3 — the GOLDEN over every worded MCP surface,
/// both profiles.
///
/// WHAT THIS BUYS, precisely: no change to text this server serves can land
/// without someone re-blessing the golden in the same diff. The previous
/// thirteen rounds all found the same defect class — a served sentence that
/// claimed more than the code delivers — and every guard built against it
/// was a negative substring pin, which an arbitrary paraphrase walks past.
/// This does not read the text, so a paraphrase cannot dodge it.
///
/// WHAT IT DOES NOT BUY, and this must not be read otherwise: it is NOT
/// semantic enforcement. It cannot tell a true sentence from a false one.
/// A wrong claim that is blessed once stays blessed forever, and a re-bless
/// that nobody reads converts this guard into a rubber stamp. It turns an
/// unbounded problem (is every served sentence true?) into a bounded one
/// (is this specific changed sentence true?). Answering the bounded
/// question is still a person's job, which is why the failure message asks
/// for it by name.
///
/// WHAT IT COVERS: surfaces 1–5 of `WORKER_GUIDANCE_SURFACES` — the whole
/// `initialize` result, the whole listed `Prompt`, the whole `prompts/get`
/// result, and the whole listed `Tool` — for the DEFAULT and WORKER
/// profiles. Both, because a one-sided edit to the two near-identical
/// `build_model` bodies is how round thirteen's defect nearly shipped.
///
/// EVERY ROW HASHES THE WHOLE SERIALIZED VALUE OF ITS CHANNEL, and the
/// fifteenth round is why that sentence is here rather than assumed. Row 1
/// hashed `instructions` alone — one field of an `InitializeResult` that
/// also carries `protocolVersion`, `capabilities`, `serverInfo` and
/// `_meta`. Field selection is the defect the eleventh round found in the
/// sweeps, and this golden reproduced it while being the guard against it.
/// A new row must hash a serialized value, never a field read off one.
///
/// IT ALSO COVERS surfaces 7, 8 and 9 on the WORKER profile — the text a
/// `tools/call` carries when it succeeds or fails: all 12 worker-served
/// tools, and the nine reachable argument-validation failures, each as a
/// whole serialized `CallToolResult`. See [`worker_call_digests`].
///
/// THOSE THREE WERE EXCLUDED, and the fifteenth round is why they are not.
/// The exclusion said their payloads embed run-dependent values — paths,
/// plan ids, timestamps — so a digest over them would drift every run and
/// get blessed reflexively. That is true of the surface in general and NOT
/// of this set, which is the step the exclusion skipped: the plan- and
/// timestamp-producing tools are `propose`, `optimize` and the rest of the
/// withheld set, and the worker profile serves none of them. What remains
/// run-dependent is the temp root, replaced exactly by
/// `normalize_run_paths` and then re-checked by [`record`]'s nonce
/// assertion, so a path that was missed fails rather than blessing.
///
/// The general principle was right; whether it applied to the specific set
/// was never checked. That gap is the finding, not the principle.
///
/// WORKER ONLY for those three, and the asymmetry with rows 1–5 is
/// deliberate rather than an oversight: the DEFAULT profile serves the
/// plan-producing tools the exclusion was really about, so driving it here
/// would import the drift that is genuinely absent from the worker surface.
///
/// WHAT IT STILL DOES NOT COVER, listed rather than left to be discovered:
///
///  - surface 6 is not served (pinned absent by
///    `worker_result_text_names_no_excluded_tool`);
///  - row 9 stays PARTIAL for the reason the enumeration gives — policy
///    denials, warehouse failures and internal errors are not reachable
///    from an offline harness;
///  - the DEFAULT profile's call results are unpinned, deliberately (it
///    serves the plan-producing tools);
///  - the APPROVER profile's call results are unpinned too, and this one is
///    a genuine hole rather than a choice. The approver serves an action
///    neither other profile does — `review_queue` approve, #1517 — and its
///    result envelope is read by no sweep and pinned by no golden. Rows 1–5
///    cover the approver only because they are compared for EQUALITY
///    against the default surface, and that equality says nothing about
///    what a call returns.
///
/// This is not "every MCP surface".
///
/// The `Approver` profile is compared against `Default` rather than
/// blessed: `try_new_with_profile` branches on `Worker` alone, so the two
/// serve identical text today, and pinning the EQUALITY catches a future
/// divergence that a third blessed column would silently absorb.
#[tokio::test]
async fn served_text_golden_pins_every_worded_surface() {
    let dir = TempDir::new().unwrap();
    write_project(dir.path(), &dir.path().join("test.duckdb"));
    let config_path = dir.path().join("rocky.toml");
    let nonce = dir
        .path()
        .file_name()
        .expect("temp dir has a final component")
        .to_string_lossy()
        .to_string();

    let default = served_text_digests(&config_path, rocky_mcp::McpProfile::Default, &nonce).await;
    let worker = served_text_digests(&config_path, rocky_mcp::McpProfile::Worker, &nonce).await;

    // The approver profile serves the same TEXT as the default profile — it
    // enables one `review_queue` action, it does not reword anything. Pinned
    // as an equality so a future divergence has to be noticed here.
    let approver = served_text_digests(&config_path, rocky_mcp::McpProfile::Approver, &nonce).await;
    assert_eq!(
        approver, default,
        "the approver profile now serves different text from the default profile; it is \
         blessed as the default surface, so either revert the divergence or give it its own \
         golden rows"
    );

    // Non-vacuity, before any comparison. A digest table that went empty, or
    // a worker surface that stopped being a subset of the default one, would
    // otherwise compare clean against a golden blessed from the same bug.
    let tool_keys = |table: &std::collections::BTreeMap<String, String>| {
        table
            .keys()
            .filter(|k| k.starts_with("tools/list/"))
            .cloned()
            .collect::<std::collections::BTreeSet<String>>()
    };
    let default_tools = tool_keys(&default);
    let worker_tools = tool_keys(&worker);
    assert!(
        worker_tools.len() < default_tools.len(),
        "the worker profile must serve strictly fewer tools than the default profile, or \
         this golden is pinning one surface twice"
    );
    assert!(
        worker_tools.is_subset(&default_tools),
        "the worker profile serves a tool the default profile does not: {:?}",
        worker_tools.difference(&default_tools).collect::<Vec<_>>()
    );
    for table in [&default, &worker] {
        for key in table.keys() {
            if let Some(name) = key.strip_prefix("prompts/list/") {
                assert!(
                    table.contains_key(&format!("prompts/get/{name}")),
                    "prompt '{name}' is listed but its body is not digested"
                );
            }
        }
    }

    // Surfaces 7, 8 and 9, worker only. Its OWN temp directory, because this
    // pass calls `draft_model` and therefore WRITES into the project — and
    // the three passes above share one fixture that must stay untouched.
    let call_dir = TempDir::new().unwrap();
    let call_nonce = call_dir
        .path()
        .file_name()
        .expect("temp dir has a final component")
        .to_string_lossy()
        .to_string();
    let worker_calls = worker_call_digests(call_dir.path(), &call_nonce).await;
    assert!(
        !worker_calls.is_empty(),
        "the call sweep produced no rows; it would pin nothing"
    );

    // `record` refuses a duplicate key WITHIN one table. Two tables now merge
    // under the same `worker` label, and a plain `insert` would drop the
    // older row silently — the guard losing its own precondition one layer
    // up. The `tools/call/*` keys cannot collide with `instructions` /
    // `prompts/*` / `tools/list/*` today; "cannot" is exactly the kind of
    // unenforced claim this file exists to stop taking on trust.
    let mut live = std::collections::BTreeMap::new();
    for (label, table) in [
        ("default", default),
        ("worker", worker),
        ("worker", worker_calls),
    ] {
        for (key, hash) in table {
            let full = format!("{label}/{key}");
            assert!(
                live.insert(full.clone(), hash).is_none(),
                "two digest tables both produced '{full}'; one row would silently replace \
                 the other and the golden would pin only the survivor"
            );
        }
    }
    let rendered = render_golden(&live);

    // Blessing has to be ASKED FOR, with an explicit `1` — see
    // [`should_bless`], which is where the rule and its evidence live. Every
    // other value, recognised or not, compares.
    let value = std::env::var(BLESS_VAR).ok();
    if should_bless(value.as_deref()) {
        std::fs::write(SERVED_TEXT_GOLDEN, &rendered)
            .unwrap_or_else(|e| panic!("write {SERVED_TEXT_GOLDEN}: {e}"));
        return;
    }

    let on_disk = std::fs::read_to_string(SERVED_TEXT_GOLDEN).unwrap_or_default();
    if on_disk == rendered {
        return;
    }

    // BIDIRECTIONAL, not "every golden row still matches". A tool or prompt
    // added later is ABSENT from the golden, and a one-directional check
    // reads that as clean — which is the same shape as every hand-picked
    // list this crate has already been bitten by.
    let expected: std::collections::BTreeMap<&str, &str> = on_disk
        .lines()
        .filter_map(|line| line.split_once('\t'))
        .collect();
    let mut changed = Vec::new();
    let mut added = Vec::new();
    for (key, hash) in &live {
        match expected.get(key.as_str()) {
            Some(blessed) if *blessed == hash.as_str() => {}
            Some(blessed) => changed.push(format!(
                "  CHANGED  {key}\n    was {blessed}\n    now {hash}"
            )),
            None => added.push(format!("  NEW      {key}")),
        }
    }
    let removed: Vec<String> = expected
        .keys()
        .filter(|key| !live.contains_key(**key))
        .map(|key| format!("  GONE     {key}"))
        .collect();

    let mut report = String::new();
    for line in changed.iter().chain(added.iter()).chain(removed.iter()) {
        report.push_str(line);
        report.push('\n');
    }
    if report.is_empty() {
        report.push_str("  (no keyed drift — the file's formatting differs from the renderer)\n");
    }

    panic!(
        "The text this MCP server serves has changed:\n\n{report}\n\
         This guard does NOT check that the new wording is TRUE — it only stops a wording \
         change from landing unreviewed. So, in this order:\n\n\
         1. Read each changed surface above and check the new sentence against the code it \
            describes. Every round of this branch has found a served claim that out-ran its \
            implementation; that is the failure this exists to make visible.\n\
         2. Only then re-bless:\n\
            \x20  {BLESS_VAR}=1 cargo test -p rocky-mcp --test roundtrip \
         served_text_golden_pins_every_worded_surface\n\
         3. Commit the golden with the change that caused it, so a reviewer sees both.\n\n\
         Re-blessing without step 1 makes this file a rubber stamp and buys nothing.\n"
    );
}
