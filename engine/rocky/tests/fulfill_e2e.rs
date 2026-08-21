//! FF-WP-E2 — the fulfillment reconciler, end to end: ReplayDriver +
//! the REAL binary + DuckDB, no credentials.
//!
//! The acceptance battery from the brief:
//! - full happy path init → applied → observing from a truly COLD
//!   directory (the replay elicitation writes the candidate; the POC
//!   rule: nothing pre-populates it);
//! - candidate-edit surfacing while waiting, then supersession-by-
//!   re-approval mid-flight (the CAS fence: the old plan is orphaned,
//!   never applied);
//! - the engine backstop: `--expect-spec-digest` refuses independently
//!   of the loop's own pre-apply check, and a bare apply refuses a
//!   product-bound plan;
//! - Phase-A tamper drill → `blocked` (tampered) at the byte-verify;
//! - snapshot tamper → `blocked` (tampered) at the pre-apply recompute;
//! - fault injection at digest-recompute→apply (abort = the no-cleanup
//!   exit a SIGKILL produces), then resume through `applying_unknown`:
//!   the authoritative-receipt arms (NoRecord → dedup-safe retry;
//!   seeded InFlight → parked; seeded Succeeded → resolved WITHOUT
//!   re-running);
//! - the takeover-race: a live foreign owner stands the loop down; a
//!   dead one is taken over immediately;
//! - the privilege gate: a worker-profile session calling excluded
//!   tools gets tool-not-found (protocol-level), asserted by the replay
//!   expectations against the real `rocky mcp --profile worker`.

use std::path::{Path, PathBuf};
use std::process::Command;

const PRODUCT: &str = "revenue_daily";

/// The candidate spec the recorded elicitation session hands off.
const CANDIDATE_SPEC: &str = r#"[product]
name = "revenue_daily"
intent = "Daily gross revenue per client in EUR, refunds excluded"

[product.source]
tables = ["wh.raw.stripe_charges"]

[product.output]
model = "revenue_daily"
grain = ["client_id"]
columns = [
  { name = "client_id", type = "Int64", nullable = true },
  { name = "loaded_at", type = "Timestamp", nullable = true },
  { name = "revenue_eur", type = "Float64", nullable = true },
]
checks = ["revenue_eur >= 0"]
freshness = { max_lag = "24h", time_column = "loaded_at" }

[product.trust]
agent = "propose_only"
"#;

/// The SQL the recorded drafting session authors. Self-contained (reads
/// no source) so the DuckDB-backed local tests and the apply both run
/// without seeded warehouse data.
const DRAFT_SQL: &str =
    "SELECT 1::BIGINT AS client_id, now() AS loaded_at, 1.5::DOUBLE AS revenue_eur";

fn rocky_toml() -> String {
    r#"[adapter]
type = "duckdb"
path = "wh.duckdb"

[pipeline.p]
type = "transformation"
models = "models/**"

[pipeline.p.target]
adapter = "default"

[pipeline.p.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "propose"
scope = { models = ["revenue_daily"] }
effect = "allow"

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = { models = ["revenue_daily"] }
effect = "require_review"

[fulfill]

[fulfill.driver]
type = "replay"
session = "replay/session.json"
"#
    .to_string()
}

/// Build the recorded session: the elicitation hand-off (with the REAL
/// digest over the candidate bytes) and the drafting `draft_model`
/// call. `extra_drafting_calls` prepends privilege-gate probes.
fn session_json(extra_drafting_calls: &[serde_json::Value]) -> String {
    let digest = rocky_core::product::spec::spec_digest(CANDIDATE_SPEC.as_bytes());
    let mut drafting_calls = extra_drafting_calls.to_vec();
    drafting_calls.push(serde_json::json!({
        "tool": "draft_model",
        "arguments": {
            "name": PRODUCT,
            "sql": DRAFT_SQL,
            "intent": "Daily gross revenue per client in EUR"
        },
        "expect": "ok"
    }));
    serde_json::json!({
        "mcp_command": [env!("CARGO_BIN_EXE_rocky"), "mcp", "--profile", "worker"],
        "tasks": {
            "elicitation": {
                "calls": [],
                "outcome": {
                    "candidate_spec": CANDIDATE_SPEC,
                    "questions": ["Should refunds subtract from revenue_eur?"],
                    "expected_digest": digest
                }
            },
            "drafting": { "calls": drafting_calls },
            "repair": { "calls": drafting_calls_clone(&drafting_calls) }
        }
    })
    .to_string()
}

fn drafting_calls_clone(calls: &[serde_json::Value]) -> Vec<serde_json::Value> {
    calls.to_vec()
}

/// A COLD scratch project: config + models/_defaults.toml + the session.
/// No products/ candidate — the replay elicitation writes it.
fn write_project(dir: &Path, session: &str) -> PathBuf {
    std::fs::create_dir_all(dir.join("models")).expect("models dir");
    std::fs::create_dir_all(dir.join("replay")).expect("replay dir");
    std::fs::write(
        dir.join("models/_defaults.toml"),
        "[target]\ncatalog = \"wh\"\nschema = \"out\"\n",
    )
    .expect("defaults");
    std::fs::write(dir.join("rocky.toml"), rocky_toml()).expect("config");
    std::fs::write(dir.join("replay/session.json"), session).expect("session");
    dir.join("rocky.toml")
}

/// Run the real binary from `dir`, JSON output, returning (exit_code,
/// parsed stdout JSON when it parses, raw stdout, raw stderr).
fn rocky(dir: &Path, args: &[&str]) -> (i32, Option<serde_json::Value>, String, String) {
    rocky_env(dir, args, &[])
}

fn rocky_env(
    dir: &Path,
    args: &[&str],
    env: &[(&str, &str)],
) -> (i32, Option<serde_json::Value>, String, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
    cmd.args(["--output", "json"])
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error");
    for (k, v) in env {
        cmd.env(k, v);
    }
    let out = cmd.output().expect("spawn rocky");
    let stdout = String::from_utf8_lossy(&out.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let json = serde_json::from_str::<serde_json::Value>(stdout.trim()).ok();
    (out.status.code().unwrap_or(-1), json, stdout, stderr)
}

fn state_store(dir: &Path) -> rocky_core::state::StateStore {
    rocky_core::state::StateStore::open(&dir.join("models/.rocky-state.redb")).expect("store")
}

/// Count the declared data checks in the MERGED sidecar, independently
/// of the engine's own counter.
///
/// Deliberately a different parser from the one under test — the `toml`
/// crate, not the engine's `product::toml_compat` — so this cannot
/// agree with a buggy counter by construction.
///
/// Counting `[[tests]]` headers in the TEXT would NOT work: the
/// sidecar renderer inlines an array of tables whenever every entry
/// fits the line budget, so short checks appear as `tests = [ { … } ]`
/// with no header at all. Parse, never scan.
fn declared_check_count(dir: &Path) -> usize {
    let sidecar = dir.join(format!("models/{PRODUCT}.toml"));
    let text = std::fs::read_to_string(&sidecar)
        .unwrap_or_else(|err| panic!("merged sidecar {}: {err}", sidecar.display()));
    let document: toml::Value = toml::from_str(&text)
        .unwrap_or_else(|err| panic!("merged sidecar {}: {err}", sidecar.display()));
    // Both arrays: the model loader appends every resolved `[[use_test]]`
    // to `ModelConfig.tests`, so the executed set is inline + referenced.
    // Counting only `tests` here would rebuild the very blind spot the
    // production counter was fixed to avoid.
    let count = |key: &str| {
        document
            .get(key)
            .and_then(toml::Value::as_array)
            .map_or(0, Vec::len)
    };
    count("tests") + count("use_test")
}

/// Drive the loop up to the plan-review ask, returning the plan id.
fn drive_to_plan_review(dir: &Path) -> String {
    // 1. Cold init: elicitation writes the candidate, stop at approval.
    let (code, json, _out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "elicit stop: {err}");
    let json = json.expect("fulfill json");
    assert_eq!(json["state"], "needs_input");
    assert!(
        dir.join(format!("products/{PRODUCT}.toml")).exists(),
        "the RUNNER wrote the candidate"
    );
    assert!(
        json["message"]
            .as_str()
            .unwrap()
            .contains("Should refunds subtract"),
        "the worker's questions surface in the stop: {json}"
    );

    // 2. Human approves the spec.
    let (code, json, _out, err) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "approve-spec: {err}");
    assert_eq!(json.expect("approve json")["state"], "spec_approved");

    // 3. The loop lowers, drafts, merges, verifies, proposes, then asks
    //    for plan review.
    let (code, json, _out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "drive to proposed: {err}");
    let json = json.expect("fulfill json");
    assert_eq!(json["state"], "needs_input", "{json}");
    let plan_id = json["plan_id"].as_str().expect("plan pinned").to_string();
    assert!(
        json["next_command"]
            .as_str()
            .unwrap()
            .contains(&format!("rocky review {plan_id} --approve")),
        "{json}"
    );
    // The spec-owned Phase-A artifact exists.
    assert!(
        dir.join(format!("models/{PRODUCT}.contract.toml")).exists(),
        "phase A contract committed"
    );
    plan_id
}

/// Approve the plan and drive to `observing`, asserting the applied
/// journey.
fn approve_and_apply(dir: &Path, plan_id: &str) {
    let (code, _json, _out, err) = rocky(dir, &["review", plan_id, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");

    let (code, json, _out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "apply+observe: {err}");
    let json = json.expect("fulfill json");
    assert_eq!(json["state"], "observing", "{json}");
    assert!(
        json["message"].as_str().unwrap().contains("applied"),
        "{json}"
    );

    // The warehouse table exists with the drafted row.
    let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM out.revenue_daily", [], |r| r.get(0))
        .expect("target table");
    assert_eq!(count, 1);
}

#[test]
fn happy_path_cold_init_to_observing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));

    let plan_id = drive_to_plan_review(dir);
    approve_and_apply(dir, &plan_id);

    // The journal tells the whole story, IN ORDER: dedup consecutive
    // repeats of the to_state projection (ownership stamps and releases
    // keep the state they annotate) and demand the exact sequence — an
    // out-of-order or skipped gate cannot pass. (Scoped: an open state
    // store would lock out the next binary invocation.)
    {
        let store = state_store(dir);
        let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
        let mut sequence: Vec<&str> = Vec::new();
        for row in &rows {
            if sequence.last() != Some(&row.to_state.as_str()) {
                sequence.push(row.to_state.as_str());
            }
        }
        assert_eq!(
            sequence,
            vec![
                "init",
                "needs_input",
                "spec_approved",
                "lowered_contract",
                "drafting",
                "merged",
                "verifying",
                "proposed",
                "needs_input",
                "plan_approved",
                "applying",
                "applied",
                "observing",
            ],
            "the D6 order, exactly"
        );
        let applied_rows = rows.iter().filter(|r| r.to_state == "applied").count();
        assert_eq!(applied_rows, 1, "exactly one applied journal row");

        // #1495: the verify bundle's green verdict must say what green
        // did NOT cover. The product's declared data checks lower into
        // the model sidecar and run only against a materialised table,
        // so at verify time (before apply) none of them can run. The
        // journal names them deferred, with the real count read from
        // the merged sidecar on disk.
        //
        // This is the WIRING pin: the count is read through
        // `sidecar_rel` against the runner's project root, so a wrong
        // root would silently degrade every run to "count unavailable"
        // — which no unit test can catch.
        let verdict = rows
            .iter()
            .find(|r| r.event.starts_with("verify green"))
            .expect("the green verify verdict is journaled");
        let declared = declared_check_count(dir);
        assert!(
            declared > 0,
            "fixture must declare at least one data check to make this pin meaningful"
        );
        assert_eq!(
            verdict.event,
            format!(
                "verify green: {declared} declared data checks deferred \
                 (not evaluable before the model is materialized)"
            ),
            "the green verdict must state the real deferred count"
        );
        assert!(
            !verdict.event.contains("count unavailable"),
            "the sidecar must actually be found: {}",
            verdict.event
        );
    }

    // A rerun re-observes and stays observing — idempotent resume.
    let (code, json, _out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "re-observe: {err}");
    assert_eq!(json.expect("json")["state"], "observing");
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert_eq!(
        rows.iter().filter(|r| r.to_state == "applied").count(),
        1,
        "the rerun applied nothing new"
    );
    drop(store);

    // A transcript was captured for the drafting task.
    let transcripts = dir
        .join(".rocky/fulfillment")
        .join(PRODUCT)
        .join("transcripts");
    assert!(
        std::fs::read_dir(&transcripts)
            .map(|d| d.count() > 0)
            .unwrap_or(false),
        "transcripts exist at {}",
        transcripts.display()
    );
}

#[test]
fn candidate_edit_surfaces_then_reapproval_supersedes_the_plan() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));

    // Reach needs_input(spec_approval), then EDIT the candidate: the
    // loop stays waiting and shows the NEW digest.
    let (code, _json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    let candidate_path = dir.join(format!("products/{PRODUCT}.toml"));
    let edited = format!("# reviewer note\n{CANDIDATE_SPEC}");
    std::fs::write(&candidate_path, &edited).expect("edit candidate");
    let edited_digest = rocky_core::product::spec::spec_digest(edited.as_bytes());
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    let json = json.expect("json");
    assert_eq!(json["state"], "needs_input");
    assert!(
        json["message"].as_str().unwrap().contains(&edited_digest),
        "the revised digest is surfaced: {json}"
    );

    // Approve, drive to the plan-review ask.
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let json = json.expect("json");
    let old_plan = json["plan_id"].as_str().expect("plan").to_string();

    // Edit the candidate AGAIN while the plan waits: surfaced next run,
    // but NO supersession yet — the plan stays reviewable.
    let re_edited = format!("# second thought\n{CANDIDATE_SPEC}");
    std::fs::write(&candidate_path, &re_edited).expect("edit candidate");
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    let json = json.expect("json");
    assert_eq!(
        json["state"], "needs_input",
        "still waiting on the SAME plan"
    );
    assert_eq!(json["plan_id"].as_str(), Some(old_plan.as_str()));

    // Re-approve mid-flight: the authority transition fences the wait —
    // the loop re-enters at spec_approved with the NEW snapshot and the
    // old plan is orphaned (a NEW plan id reaches review; the old one is
    // never applied).
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "re-approve: {e}");
    {
        let store = state_store(dir);
        let record = store
            .fulfill_state_get(PRODUCT)
            .expect("read")
            .expect("record");
        assert_eq!(record.state.tag(), "spec_approved", "re-entry point");
    }
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let json = json.expect("json");
    let new_plan = json["plan_id"].as_str().expect("new plan").to_string();
    assert_ne!(new_plan, old_plan, "a fresh plan for the fresh snapshot");

    // The journal recorded the supersession fence: the re-approval row
    // moved needs_input → spec_approved with the new digest.
    let rows = {
        let store = state_store(dir);
        store.fulfill_journal_rows(PRODUCT).expect("journal")
    };
    let fence = rows
        .iter()
        .filter(|r| r.event == "spec approved" && r.from_state.as_deref() == Some("needs_input"))
        .count();
    assert!(fence >= 1, "the re-approval fenced the waiting loop");

    // The ORPHANED plan still refuses to apply bare (engine backstop),
    // and was never applied.
    let (code, _j, _o, err) = rocky(dir, &["apply", &old_plan]);
    assert_ne!(code, 0, "bare apply of a product-bound plan must refuse");
    assert!(err.contains("expect-spec-digest"), "{err}");
    let applied = rows.iter().filter(|r| r.to_state == "applied").count();
    assert_eq!(applied, 0, "nothing applied during supersession");
}

#[test]
fn engine_backstop_refuses_independently_of_the_loop() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);

    // Approve the review so the ONLY gate left is the digest equality —
    // the loop's own pre-apply check is bypassed by calling the engine
    // directly.
    let (code, _j, _o, e) = rocky(dir, &["review", &plan_id, "--approve"]);
    assert_eq!(code, 0, "{e}");

    // 1. Bare apply refuses: the plan carries a product identity.
    let (code, _j, _o, err) = rocky(dir, &["apply", &plan_id]);
    assert_ne!(code, 0);
    assert!(err.contains("expect-spec-digest"), "{err}");

    // 2. A WRONG expectation refuses — the engine compares, not the loop.
    let (code, _j, _o, err) = rocky(
        dir,
        &["apply", &plan_id, "--expect-spec-digest", "sha256:wrong"],
    );
    assert_ne!(code, 0);
    assert!(
        err.contains("spec_digest") || err.contains("digest"),
        "{err}"
    );

    // 3. The RIGHT expectation executes.
    let digest = rocky_core::product::spec::spec_digest(CANDIDATE_SPEC.as_bytes());
    let (code, _j, _o, err) = rocky(dir, &["apply", &plan_id, "--expect-spec-digest", &digest]);
    assert_eq!(code, 0, "{err}");
}

#[test]
fn phase_a_tamper_blocks_at_the_byte_verify() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));

    let (code, _j, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // Crash right after drafting, BEFORE the byte-verify (the abort is
    // the no-cleanup exit a SIGKILL produces).
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "post-drafting")],
    );
    assert_ne!(code, 0, "the fault aborts the process");

    // Tamper the spec-owned Phase-A contract while the loop is down.
    let contract = dir.join(format!("models/{PRODUCT}.contract.toml"));
    assert!(contract.exists(), "phase A committed before the fault");
    let mut bytes = std::fs::read_to_string(&contract).expect("read contract");
    bytes.push_str("\n# tampered\n");
    std::fs::write(&contract, bytes).expect("tamper");

    // Resume: the dead owner is taken over immediately, drafting re-runs
    // (the interrupted attempt consumed budget), and the byte-verify
    // catches the tamper → blocked, exit 2.
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 2, "blocked exits 2");
    let json = json.expect("json");
    assert_eq!(json["state"], "blocked");
    let message = json["message"].as_str().unwrap();
    assert!(message.contains("tampered"), "{json}");
    // The LOOP's own byte-verify fired — not the substrate's Phase-B
    // backstop (`[phase-a-tampered]`, prefixed "phase B rejected"). Both
    // refuse a tampered generation (defense in depth, proven by the
    // mutation pass), but the merged-precondition check must be the one
    // that answers first.
    assert!(
        message.contains("content drift") && !message.contains("phase B rejected"),
        "the pre-Phase-B byte-verify must fire first: {json}"
    );

    // F4: the block fired at the PRE-PROPOSE byte-verify — while the
    // record still sat in `drafting`, before Phase B could commit and
    // before anything could reach the review queue. The journal has no
    // merged/verifying/proposed row at all, and NO plan was ever
    // written (the plan store directory does not exist).
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let blocked_row = rows
        .iter()
        .find(|r| r.to_state == "blocked")
        .expect("the blocked transition is journaled");
    assert_eq!(
        blocked_row.from_state.as_deref(),
        Some("drafting"),
        "tamper was caught in the drafting window (the merged precondition)"
    );
    for never in [
        "merged",
        "verifying",
        "proposed",
        "plan_approved",
        "applying",
    ] {
        assert!(
            !rows.iter().any(|r| r.to_state == never),
            "'{never}' must never be reached after a Phase-A tamper: {rows:?}"
        );
    }
    assert!(
        !dir.join(".rocky").join("plans").exists(),
        "no plan may reach the store before the byte-verify clears"
    );
}

#[test]
fn snapshot_tamper_blocks_at_the_pre_apply_recompute() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);

    // Approve the plan, then rewrite the approved snapshot out-of-band.
    let (code, _j, _o, e) = rocky(dir, &["review", &plan_id, "--approve"]);
    assert_eq!(code, 0, "{e}");
    let store = state_store(dir);
    let approval = store
        .product_approval_get(PRODUCT)
        .expect("read")
        .expect("approval");
    drop(store);
    let snapshot = dir.join(&approval.snapshot_path);
    std::fs::write(&snapshot, format!("# rewritten\n{CANDIDATE_SPEC}")).expect("tamper");

    // The marker is valid, so the loop reaches the pre-apply recompute —
    // which refuses: the snapshot no longer digests to the record.
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 2, "blocked exits 2");
    let json = json.expect("json");
    assert_eq!(json["state"], "blocked");
    assert!(
        json["message"].as_str().unwrap().contains("tampered"),
        "{json}"
    );
}

#[test]
fn fault_at_digest_recompute_to_apply_resumes_through_applying_unknown() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);
    let (code, _j, _o, e) = rocky(dir, &["review", &plan_id, "--approve"]);
    assert_eq!(code, 0, "{e}");

    // Crash between the digest recompute and the apply: `applying` is
    // journaled (digest + key pinned), no receipt exists.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "digest-recompute-to-apply")],
    );
    assert_ne!(code, 0, "the fault aborts the process");
    let store = state_store(dir);
    let record = store
        .fulfill_state_get(PRODUCT)
        .expect("read")
        .expect("record");
    assert_eq!(record.state.tag(), "applying", "crashed mid-applying");
    let key = record.idempotency_key.clone().expect("key pinned");
    assert!(
        record.owner_pid.is_some(),
        "the dead owner's stamp survives the crash"
    );
    drop(store);

    // Resume: dead-owner takeover is IMMEDIATE (pid liveness is
    // definitive), applying → applying_unknown → authoritative lookup →
    // NoRecord → dedup-safe retry → applied → observing.
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "resume: {e}");
    let json = json.expect("json");
    assert_eq!(json["state"], "observing", "{json}");
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let tags: Vec<&str> = rows.iter().map(|r| r.to_state.as_str()).collect();
    assert!(
        tags.contains(&"applying_unknown"),
        "the resume went through applying_unknown: {tags:?}"
    );
    assert_eq!(
        rows.iter().filter(|r| r.to_state == "applied").count(),
        1,
        "exactly one applied row"
    );
    // The receipt under the pinned key is the authoritative Succeeded.
    let entry = store.idempotency_get(&key).expect("read").expect("receipt");
    assert_eq!(
        entry.state,
        rocky_core::idempotency::IdempotencyState::Succeeded
    );
}

#[test]
fn applying_unknown_receipt_arms_park_on_in_flight_and_resolve_on_success() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);
    let (code, _j, _o, e) = rocky(dir, &["review", &plan_id, "--approve"]);
    assert_eq!(code, 0, "{e}");

    // Crash at the seam, then seed an IN-FLIGHT receipt under the pinned
    // key: the resume must PARK (exit 1), never blind-retry and never
    // adopt the in-flight run as success.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "digest-recompute-to-apply")],
    );
    assert_ne!(code, 0);
    let store = state_store(dir);
    let key = store
        .fulfill_state_get(PRODUCT)
        .expect("read")
        .expect("record")
        .idempotency_key
        .expect("key pinned");
    let now = chrono::Utc::now();
    store
        .idempotency_put(&rocky_core::idempotency::IdempotencyEntry {
            key: key.clone(),
            run_id: "run-inflight".to_string(),
            state: rocky_core::idempotency::IdempotencyState::InFlight,
            stamped_at: now,
            expires_at: now + chrono::Duration::days(7),
            dedup_on: rocky_core::config::DedupPolicy::Success,
        })
        .expect("seed in-flight");
    drop(store);

    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 3, "parked at applying_unknown exits 3");
    let json = json.expect("json");
    assert_eq!(json["state"], "applying_unknown", "{json}");
    assert!(
        json["message"].as_str().unwrap().contains("run-inflight"),
        "{json}"
    );

    // Replace it with a terminal Succeeded receipt: the resume resolves
    // to applied WITHOUT re-running (the drafted table never appears —
    // the resolution came from the receipt, not an execution).
    let store = state_store(dir);
    store
        .idempotency_put(&rocky_core::idempotency::IdempotencyEntry {
            key: key.clone(),
            run_id: "run-done-elsewhere".to_string(),
            state: rocky_core::idempotency::IdempotencyState::Succeeded,
            stamped_at: now,
            expires_at: now + chrono::Duration::days(7),
            dedup_on: rocky_core::config::DedupPolicy::Success,
        })
        .expect("seed succeeded");
    drop(store);

    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    let json = json.expect("json");
    // Applied via the receipt; observation runs (and may find the target
    // missing — that is a finding to journal, not a failure).
    assert_eq!(json["state"], "observing", "{json}");
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let receipt_row = rows
        .iter()
        .find(|r| r.event.contains("run-done-elsewhere"))
        .expect("the receipt resolution is journaled");
    assert_eq!(receipt_row.to_state, "applied");
    let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
    let table_exists: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'out' AND table_name = 'revenue_daily'",
            [],
            |r| r.get(0),
        )
        .expect("query");
    assert_eq!(table_exists, 0, "nothing executed: the receipt resolved it");
}

/// F3: the REAL `skipped_in_flight` arm — the loop reaches an ACTUAL
/// apply whose idempotency key is held by a live (unexpired) claim, so
/// the engine's typed outcome is `SkippedInFlight`, and the loop keeps
/// the state (never journals applied). The prior drills only ever hit
/// the receipt-lookup path BEFORE an apply; this one executes the
/// deflection arm itself.
#[test]
fn a_live_in_flight_claim_deflects_the_real_apply_and_keeps_the_state() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);
    let (code, _j, _o, e) = rocky(dir, &["review", &plan_id, "--approve"]);
    assert_eq!(code, 0, "{e}");

    // Hold the pinned key with a LIVE claim (future expiry): the engine
    // classifies an unexpired InFlight as SkipInFlight, never adopts it.
    let key = {
        let store = state_store(dir);
        let key = store
            .fulfill_state_get(PRODUCT)
            .expect("read")
            .expect("record")
            .idempotency_key
            .expect("key pinned at propose time");
        let now = chrono::Utc::now();
        store
            .idempotency_put(&rocky_core::idempotency::IdempotencyEntry {
                key: key.clone(),
                run_id: "run-held".to_string(),
                state: rocky_core::idempotency::IdempotencyState::InFlight,
                stamped_at: now,
                expires_at: now + chrono::Duration::hours(1),
                dedup_on: rocky_core::config::DedupPolicy::Success,
            })
            .expect("seed live claim");
        key
    };

    // The loop reaches the REAL apply; the engine deflects it as
    // skipped_in_flight; the machine KEEPS the state (a clean stop).
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "a deflected apply is a clean stop");
    let json = json.expect("json");
    assert_eq!(json["state"], "applying", "the state is KEPT: {json}");
    assert!(
        json["message"].as_str().unwrap().contains("run-held"),
        "the holding run is named: {json}"
    );
    {
        let store = state_store(dir);
        let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
        assert_eq!(
            rows.iter().filter(|r| r.to_state == "applied").count(),
            0,
            "a deflected apply must never journal applied"
        );
        // The claim is untouched: still InFlight under the same run.
        let entry = store.idempotency_get(&key).expect("read").expect("entry");
        assert_eq!(
            entry.state,
            rocky_core::idempotency::IdempotencyState::InFlight
        );
        assert_eq!(entry.run_id, "run-held");
        // The target was never materialized.
        drop(store);
        let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
        let table_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM information_schema.tables \
                 WHERE table_schema = 'out' AND table_name = 'revenue_daily'",
                [],
                |r| r.get(0),
            )
            .expect("query");
        assert_eq!(table_exists, 0, "nothing executed under a held claim");
    }

    // A rerun (cold resume at applying) goes through applying_unknown
    // and PARKS on the live claim — still no applied row.
    let (_code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("json");
    assert_eq!(json["state"], "applying_unknown", "{json}");
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert_eq!(
        rows.iter().filter(|r| r.to_state == "applied").count(),
        0,
        "parked resume adds no applied row either"
    );
}

#[test]
fn a_live_foreign_owner_stands_the_loop_down_and_a_dead_one_is_taken_over() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));

    // Seed a record owned by a LIVE process we control.
    let mut sleeper = Command::new("/bin/sleep")
        .arg("300")
        .spawn()
        .expect("sleeper");
    let sleeper_pid = sleeper.id();
    let start_time = process_start_time_for_test(sleeper_pid).expect("sleeper start time");
    {
        let store = state_store(dir);
        let mut record = rocky_core::fulfill::FulfillStateRecord::new(
            rocky_core::fulfill::FulfillState::Init,
            format!("product:{PRODUCT}"),
            None,
            None,
        );
        record.owner_pid = Some(sleeper_pid);
        record.owner_start_time = Some(start_time);
        let row = rocky_core::fulfill::FulfillJournalRow {
            seq: 0,
            at: None,
            event: "seeded foreign owner".to_string(),
            from_state: None,
            to_state: "init".to_string(),
            spec_digest: None,
            plan_id: None,
            idempotency_key: None,
        };
        store
            .fulfill_state_cas(PRODUCT, None, &record, &row)
            .expect("seed");
    }

    // A live owner: stand down, exit clean, write nothing.
    let (code, _json, out, _err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "stand-down is clean");
    assert!(
        out.contains(&sleeper_pid.to_string()),
        "the live owner is named: {out}"
    );
    {
        let store = state_store(dir);
        let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
        assert_eq!(rows.len(), 1, "the stand-down wrote nothing");
    }

    // Kill the owner: the next invocation takes over IMMEDIATELY (the
    // pid probe is definitive) and drives to the elicitation stop.
    sleeper.kill().expect("kill");
    sleeper.wait().expect("reap");
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "takeover: {e}");
    assert_eq!(json.expect("json")["state"], "needs_input");
}

/// The test-side start-time probe (mirrors the store's platform probe).
fn process_start_time_for_test(pid: u32) -> Option<u64> {
    #[cfg(target_os = "macos")]
    {
        use std::mem::MaybeUninit;
        let mut info = MaybeUninit::<libc::proc_bsdinfo>::zeroed();
        let size = std::mem::size_of::<libc::proc_bsdinfo>() as libc::c_int;
        // SAFETY: PROC_PIDTBSDINFO writes at most `size` bytes into the
        // zeroed, exactly-sized buffer; no pointer outlives the call.
        let written = unsafe {
            libc::proc_pidinfo(
                pid as libc::c_int,
                libc::PROC_PIDTBSDINFO,
                0,
                info.as_mut_ptr().cast(),
                size,
            )
        };
        if written < size {
            return None;
        }
        // SAFETY: the kernel reported a full write.
        let info = unsafe { info.assume_init() };
        Some(info.pbi_start_tvsec.saturating_mul(1_000_000) + info.pbi_start_tvusec)
    }
    #[cfg(target_os = "linux")]
    {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        let after = stat.rsplit_once(')')?.1;
        after.split_ascii_whitespace().nth(19)?.parse::<u64>().ok()
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = pid;
        None
    }
}

#[test]
fn worker_profile_excluded_tools_are_not_found() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    // The drafting session FIRST probes every excluded tool, expecting
    // the protocol-level tool-not-found from the REAL worker-profile
    // server, then drafts normally. If the server SERVED any of them,
    // the expectation fails and the drafting task errors.
    let probes: Vec<serde_json::Value> = [
        "propose",
        "review_queue",
        "draft_contract",
        "draft_metadata",
        "pause_schedule",
    ]
    .iter()
    .map(|tool| {
        serde_json::json!({
            "tool": tool,
            "arguments": {},
            "expect": "tool_not_found"
        })
    })
    .collect();
    write_project(dir, &session_json(&probes));

    let plan_id = drive_to_plan_review(dir);
    assert!(!plan_id.is_empty(), "the gate held and the flow completed");
}
