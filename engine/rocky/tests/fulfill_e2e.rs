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

/// A draft that violates the Phase-A contract (required column
/// `revenue_eur` missing → E010), so the runner's own verify bundle
/// comes back red on the compile leg and the machine enters a repair
/// round (#1493).
const BAD_DRAFT_SQL: &str = "SELECT 1::BIGINT AS client_id, now() AS loaded_at";

/// A draft that COMPILES, satisfies the contract, and applies cleanly —
/// and is wrong. The spec declares `checks = ["revenue_eur >= 0"]`,
/// which lowers into the model sidecar as an expression test at
/// `severity = "error"`, and a sidecar test can only run against a
/// materialised table. So nothing before the apply can catch this: the
/// compiler sees a well-typed Float64, the runner's own model test
/// executes fine, and the number is negative.
///
/// It is also the mistake the fixture's own elicitation asks about
/// ("Should refunds subtract from revenue_eur?"), which is the point:
/// this is the failure class a data product exists to prevent, and the
/// one the loop could not see before F3.
const NEGATIVE_DRAFT_SQL: &str =
    "SELECT 1::BIGINT AS client_id, now() AS loaded_at, -3.5::DOUBLE AS revenue_eur";

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

/// One recorded `draft_model` call authoring `sql`.
fn draft_model_call(sql: &str) -> serde_json::Value {
    serde_json::json!({
        "tool": "draft_model",
        "arguments": {
            "name": PRODUCT,
            "sql": sql,
            "intent": "Daily gross revenue per client in EUR"
        },
        "expect": "ok"
    })
}

/// Build the recorded session: the elicitation hand-off (with the REAL
/// digest over the candidate bytes) and the drafting `draft_model`
/// call. `extra_drafting_calls` prepends privilege-gate probes.
fn session_json(extra_drafting_calls: &[serde_json::Value]) -> String {
    let mut drafting_calls = extra_drafting_calls.to_vec();
    drafting_calls.push(draft_model_call(DRAFT_SQL));
    session_json_with_tasks(&drafting_calls, &drafting_calls)
}

/// A session whose drafting round authors `drafting_sql` and whose
/// repair round authors `repair_sql` — the repair drills record a red
/// first draft and a fixing (or still-red) repair.
fn session_json_with_repair(drafting_sql: &str, repair_sql: &str) -> String {
    session_json_with_tasks(
        &[draft_model_call(drafting_sql)],
        &[draft_model_call(repair_sql)],
    )
}

fn session_json_with_tasks(
    drafting_calls: &[serde_json::Value],
    repair_calls: &[serde_json::Value],
) -> String {
    // Drills that never reach a data-red reuse the repair round's calls
    // for the data-repair task: the task must EXIST (a missing one is a
    // replay error), but nothing dispatches it.
    session_json_with_rounds(drafting_calls, repair_calls, repair_calls)
}

fn session_json_with_rounds(
    drafting_calls: &[serde_json::Value],
    repair_calls: &[serde_json::Value],
    data_repair_calls: &[serde_json::Value],
) -> String {
    let digest = rocky_core::product::spec::spec_digest(CANDIDATE_SPEC.as_bytes());
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
            "repair": { "calls": repair_calls },
            "data-repair": { "calls": data_repair_calls }
        }
    })
    .to_string()
}

/// A session whose first draft applies CLEANLY but violates a check the
/// product declared about its own output, and whose data-repair round
/// authors `data_repair_sql`.
fn session_json_with_data_repair(data_repair_sql: &str) -> String {
    session_json_with_rounds(
        &[draft_model_call(NEGATIVE_DRAFT_SQL)],
        &[draft_model_call(NEGATIVE_DRAFT_SQL)],
        &[draft_model_call(data_repair_sql)],
    )
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

/// Create the target the recorded `DRAFT_SQL` would have produced.
///
/// For drills that seed a receipt instead of running an apply. The
/// declared checks (grain uniqueness on `client_id`, `revenue_eur >= 0`)
/// pass against this row, so observation reaches a real verdict rather
/// than erroring on a missing table.
fn materialize_target(dir: &Path) {
    let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
    conn.execute_batch(&format!(
        "CREATE SCHEMA IF NOT EXISTS out; \
         CREATE OR REPLACE TABLE out.{PRODUCT} AS {DRAFT_SQL};"
    ))
    .expect("materialize the target");
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
    let (code, json, out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "drive to proposed: {err}{out}");
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
        // Counted by EVENT, not by `to_state`: the observation journals
        // its staleness/test reading before claiming any state, so more
        // than one row legitimately carries `to_state == "applied"`.
        // What must be exactly one is the APPLY.
        let applied_rows = rows
            .iter()
            .filter(|r| r.event.starts_with("applied ("))
            .count();
        assert_eq!(applied_rows, 1, "exactly one apply journal row");

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
        rows.iter()
            .filter(|r| r.event.starts_with("applied ("))
            .count(),
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

/// The journal's state walk, consecutive repeats collapsed (ownership
/// stamps and releases keep the state they annotate).
fn deduped_states(rows: &[rocky_core::fulfill::FulfillJournalRow]) -> Vec<&str> {
    let mut sequence: Vec<&str> = Vec::new();
    for row in rows {
        if sequence.last() != Some(&row.to_state.as_str()) {
            sequence.push(row.to_state.as_str());
        }
    }
    sequence
}

/// #1493: a red verify enters a repair round, and the repair driver's
/// `draft_model` legitimately rewrites the merged sidecar (parse,
/// re-serialize, overwrite). The loop dispatched that write itself, so
/// it must treat it as authorized — reopen the drafting window through
/// the staged commit — and converge, never mis-classify its own repair
/// as tamper.
#[test]
fn a_red_verify_repairs_and_converges() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, DRAFT_SQL));

    // Elicit, then approve the spec.
    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // ONE invocation: draft (red) → merge → verify red → repair round →
    // re-merge → verify green → propose → the plan-review ask.
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "the repair round must recover, not block: {e}");
    let json = json.expect("json");
    assert_eq!(json["state"], "needs_input", "{json}");
    let plan_id = json["plan_id"].as_str().expect("plan pinned").to_string();

    // The journal shows BOTH drafting windows and exactly one repair.
    {
        let store = state_store(dir);
        let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
        assert_eq!(
            deduped_states(&rows),
            vec![
                "init",
                "needs_input",
                "spec_approved",
                "lowered_contract",
                "drafting",
                "merged",
                "verifying",
                "drafting", // the repair round reopens the window
                "merged",
                "verifying",
                "proposed",
                "needs_input",
            ],
            "one repair round, then convergence"
        );
        assert_eq!(
            rows.iter()
                .filter(|r| r.event.starts_with("repair round"))
                .count(),
            1,
            "exactly one repair dispatch: {rows:?}"
        );
        assert!(
            !rows.iter().any(|r| r.to_state == "blocked"),
            "the loop's own repair write must never be classified as tamper: {rows:?}"
        );
    }

    // The REPAIRED draft is what reaches the warehouse — not the red one.
    approve_and_apply(dir, &plan_id);
    let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
    let revenue: f64 = conn
        .query_row("SELECT revenue_eur FROM out.revenue_daily", [], |r| {
            r.get(0)
        })
        .expect("applied row");
    assert!(
        revenue > 0.0,
        "the repaired SQL applied, not the red draft (revenue_eur = {revenue})"
    );
}

/// The #1493 invariant: authorizing the loop's OWN repair write must not
/// bless anyone else's. An out-of-band edit to a file the repair round
/// does NOT write (the spec-owned contract), made while the repair
/// window is open, is still caught and blocked as tamper.
#[test]
fn out_of_band_tamper_during_the_repair_window_still_blocks() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, DRAFT_SQL));

    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // Crash right after the REPAIR driver finished — the window is
    // reopened and the sidecar legitimately rewritten, but the
    // byte-verify has not run yet.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "post-repair-drafting")],
    );
    assert_ne!(code, 0, "the fault aborts the process");

    // Tamper the spec-owned contract while the loop is down: the repair
    // window authorizes the WORKER's sidecar/SQL writes, never this.
    let contract = dir.join(format!("models/{PRODUCT}.contract.toml"));
    let mut bytes = std::fs::read_to_string(&contract).expect("read contract");
    bytes.push_str("\n# tampered during the repair window\n");
    std::fs::write(&contract, bytes).expect("tamper");

    // Resume: the byte-verify catches the contract drift → blocked.
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 2, "blocked exits 2");
    let json = json.expect("json");
    assert_eq!(json["state"], "blocked", "{json}");
    let message = json["message"].as_str().unwrap();
    assert!(
        message.contains("tampered")
            && message.contains(&format!("models/{PRODUCT}.contract.toml"))
            && message.contains("content drift"),
        "the CONTRACT drift is what blocks: {json}"
    );
    // The LOOP's byte-verify fired, not the substrate's Phase-B
    // backstop — same layer pin as the round-1 tamper drill.
    assert!(
        !message.contains("phase B rejected"),
        "the pre-Phase-B byte-verify must answer first: {json}"
    );

    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert_eq!(
        rows.iter()
            .filter(|r| r.event.starts_with("repair round"))
            .count(),
        1,
        "the tamper happened inside a live repair round: {rows:?}"
    );
    let blocked_row = rows
        .iter()
        .find(|r| r.to_state == "blocked")
        .expect("the blocked transition is journaled");
    assert_eq!(
        blocked_row.from_state.as_deref(),
        Some("drafting"),
        "caught in the drafting window, before Phase B"
    );
    for never in ["proposed", "plan_approved", "applying", "applied"] {
        assert!(
            !rows.iter().any(|r| r.to_state == never),
            "'{never}' must never be reached after a repair-window tamper: {rows:?}"
        );
    }
}

/// The reopen itself fails closed: a crash lands between the repair CAS
/// and the reopen (state = drafting, manifest still MERGED, no write
/// authorized yet), the sidecar is edited while the loop is down, and
/// the resume's reopen refuses to demote drifted bytes — blocked as
/// tamper BEFORE any driver dispatch.
#[test]
fn tamper_before_the_reopen_blocks_at_the_reopen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, DRAFT_SQL));

    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // Crash after the repair transition is journaled, BEFORE the reopen.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "pre-repair-reopen")],
    );
    assert_ne!(code, 0, "the fault aborts the process");

    // The merged sidecar is still hash-pinned (no window is open).
    // Edit it out-of-band.
    let sidecar = dir.join(format!("models/{PRODUCT}.toml"));
    let mut bytes = std::fs::read_to_string(&sidecar).expect("read sidecar");
    bytes.push_str("\n# tampered before the reopen\n");
    std::fs::write(&sidecar, bytes).expect("tamper");

    // Count the transcripts before the resume: elicitation + drafting.
    let transcripts = dir
        .join(".rocky/fulfillment")
        .join(PRODUCT)
        .join("transcripts");
    let transcripts_before = std::fs::read_dir(&transcripts)
        .expect("transcripts")
        .count();

    // Resume: the reopen verifies the merged manifest in full and
    // refuses — blocked as tamper, with NO further driver dispatch.
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 2, "blocked exits 2");
    let json = json.expect("json");
    assert_eq!(json["state"], "blocked", "{json}");
    let message = json["message"].as_str().unwrap();
    assert!(
        message.contains("tampered")
            && message.contains(&format!("models/{PRODUCT}.toml"))
            && message.contains("content drift"),
        "the sidecar drift is what blocks: {json}"
    );
    assert_eq!(
        std::fs::read_dir(&transcripts)
            .expect("transcripts")
            .count(),
        transcripts_before,
        "the reopen blocked BEFORE dispatching another worker"
    );
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let blocked_row = rows
        .iter()
        .find(|r| r.to_state == "blocked")
        .expect("the blocked transition is journaled");
    assert_eq!(blocked_row.from_state.as_deref(), Some("drafting"));
}

/// Transcript file names by task kind — the driver writes
/// `<stamp>-<kind>.log`, so this reports which ROUND actually reached a
/// worker, independent of anything the loop says about itself.
fn transcript_kinds(dir: &Path) -> Vec<String> {
    let transcripts = dir
        .join(".rocky/fulfillment")
        .join(PRODUCT)
        .join("transcripts");
    let mut kinds: Vec<String> = std::fs::read_dir(&transcripts)
        .expect("transcripts dir")
        .flatten()
        .filter_map(|entry| {
            let name = entry.file_name().to_string_lossy().into_owned();
            name.strip_suffix(".log")
                .and_then(|stem| stem.rsplit_once('-'))
                .map(|(_, kind)| kind.to_string())
        })
        .collect();
    kinds.sort();
    kinds
}

/// #1493 (F2): the repair transition compare-and-swaps `drafting` and
/// THEN dispatches its worker. A crash in between must resume as the
/// same round — the repair brief and the repair budget — not silently
/// downgrade to a plain draft.
///
/// The observable is which transcript the resumed round writes, because
/// the driver names it after the task kind it was handed. Before the
/// fix the resume produced a SECOND `drafting` transcript and no
/// `repair` one.
#[test]
fn a_crash_between_the_repair_cas_and_its_worker_resumes_as_a_repair() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, DRAFT_SQL));

    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // Crash after the repair transition is journaled, BEFORE the reopen
    // and before any repair worker is dispatched.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "pre-repair-reopen")],
    );
    assert_ne!(code, 0, "the fault aborts the process");
    assert_eq!(
        transcript_kinds(dir),
        vec!["drafting", "elicitation"],
        "the crash lands before the repair worker runs"
    );

    // The record on disk carries the DECIDED round, which is the whole
    // point: the resume reads this, not the decision that produced it.
    {
        let store = state_store(dir);
        let record = store
            .fulfill_state_get(PRODUCT)
            .expect("reads")
            .expect("recorded");
        assert_eq!(record.state.tag(), "drafting");
        assert_eq!(
            record.drafting_round,
            rocky_core::fulfill::DraftingRound::Repair,
            "the repair transition must persist its round"
        );
    }

    // Resume: the reopen runs, and the round dispatched is the REPAIR
    // one — so the recorded repair SQL lands and the loop converges.
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "the resumed repair must converge: {e}");
    let json = json.expect("json");
    assert_eq!(json["state"], "needs_input", "{json}");

    assert_eq!(
        transcript_kinds(dir),
        vec!["drafting", "elicitation", "repair"],
        "the resumed round wrote a REPAIR transcript — a second 'drafting' \
         one would mean the round was downgraded"
    );

    // And the budget it consumed is the repair one: still exactly one
    // repair round, never re-counted by the resume.
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert_eq!(
        rows.iter()
            .filter(|r| r.event.starts_with("repair round"))
            .count(),
        1,
        "the resume continues the decided round, it does not open a new one: {rows:?}"
    );
    assert!(
        rows.iter().any(|r| r.event.starts_with("repair attempt")),
        "the resumed dispatch is journaled as a repair: {rows:?}"
    );
    assert!(
        !rows.iter().any(|r| r.to_state == "blocked"),
        "nothing blocks: {rows:?}"
    );
}

/// #1493: a crash between `acquire` and the first state transition must
/// not lock the product out of its own drafting window.
///
/// The reopen gate requires the record's owner stamp to name THIS
/// process — pid paired with process start time. A hard crash leaves a
/// stamp behind that no live process matches. If the next invocation
/// could not take that stamp over, or took it over without restamping,
/// the loop would refuse to open a window it is entitled to and the
/// product would be permanently stuck.
///
/// This is the seam the reasoning covered but no test did: the fault
/// fires with ownership on disk and no transition yet.
#[test]
fn a_crash_right_after_acquire_does_not_lock_the_product_out() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, DRAFT_SQL));

    // Crash with ownership stamped and nothing else done.
    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "post-acquire")],
    );
    assert_ne!(code, 0, "the fault aborts the process");

    // A dead process's stamp is on the record.
    {
        let store = state_store(dir);
        let record = store
            .fulfill_state_get(PRODUCT)
            .expect("reads")
            .expect("the crash left a record");
        let stamped = record.owner_pid.expect("ownership was stamped");
        assert!(
            !rocky_core::process::stamp_is_this_process(record.owner_pid, record.owner_start_time),
            "the stale stamp (pid {stamped}) must not read as this process"
        );
    }

    // Drive to completion: the takeover restamps ownership, and every
    // later gate that asks "is this record mine?" answers yes.
    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "the stale owner stamp must be taken over: {e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    // The full path including a REPAIR round, which is the one that
    // reopens the drafting window through the owner-stamp gate.
    let (code, json, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "the reopen must not refuse its own loop: {e}");
    let json = json.expect("json");
    assert_eq!(json["state"], "needs_input", "{json}");

    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert!(
        rows.iter().any(|r| r.event.starts_with("repair round")),
        "the repair round ran, so the reopen gate was exercised: {rows:?}"
    );
    assert!(
        !rows.iter().any(|r| r.to_state == "blocked"),
        "nothing blocks — a self-lockout would show up here: {rows:?}"
    );
}

/// A repair that never turns the verify green exhausts
/// MAX_REPAIR_ROUNDS and blocks on the BUDGET — a plain red, never a
/// tamper claim, with each round's reopen + re-merge journaled.
#[test]
fn a_repair_that_stays_red_exhausts_the_budget_and_blocks() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    // The repair rounds re-author the SAME red draft.
    write_project(dir, &session_json_with_repair(BAD_DRAFT_SQL, BAD_DRAFT_SQL));

    let (code, _j, _o, e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "{e}");
    let (code, _j, _o, e) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "{e}");

    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 2, "blocked exits 2");
    let json = json.expect("json");
    assert_eq!(json["state"], "blocked", "{json}");
    let message = json["message"].as_str().unwrap();
    assert!(
        message.contains("verification red after 3 repair rounds"),
        "the budget is what blocks: {json}"
    );
    assert!(
        !message.contains("tampered"),
        "a red repair is never classified as tamper: {json}"
    );

    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    for round in 1..=3 {
        assert_eq!(
            rows.iter()
                .filter(|r| r.event.starts_with(&format!("repair round {round} ")))
                .count(),
            1,
            "repair round {round} dispatched exactly once: {rows:?}"
        );
    }
    assert_eq!(
        rows.iter()
            .filter(|r| r.event.starts_with("repair round"))
            .count(),
        3,
        "the budget is EXACTLY 3 rounds — never a fourth dispatch: {rows:?}"
    );
    assert!(
        !rows.iter().any(|r| r.to_state == "proposed"),
        "a red bundle never proposes: {rows:?}"
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
        rows.iter()
            .filter(|r| r.event.starts_with("applied ("))
            .count(),
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
    // Applied via the receipt — and observation HOLDS, because this
    // drill seeds a receipt instead of running an apply, so the target
    // was never written and not one declared check can be evaluated.
    //
    // It used to assert `observing` here, over a read of nothing. That
    // is the silent-zero this work package exists to remove, so the
    // drill now asserts the honest terminal instead. The table-absence
    // proof below is why it cannot simply materialize first: that proof
    // IS the drill.
    assert_eq!(json["state"], "applied", "{json}");
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

    // With the target in place — what a real apply would have left — the
    // same resume reaches `observing` on a genuine verdict. Both halves
    // hold: nothing re-executed, AND health is claimed only once the
    // declared checks could actually run.
    //
    // The store is dropped first: an open handle locks out the next
    // binary invocation (the convention this file follows throughout).
    drop(store);
    materialize_target(dir);
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0);
    assert_eq!(
        json.expect("json")["state"],
        "observing",
        "the checks pass against the materialized target"
    );
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
        // FF-WP-F3: a check's `expression` is raw-interpolated into SQL the
        // loop now EXECUTES unattended after every apply, so the worker
        // profile must not SERVE one. The probe proves the route is gone;
        // it does not prove the worker cannot author a check — a file
        // writer still can, and that boundary is #1491 / #1515.
        "draft_check",
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

// =========================================================================
// FF-WP-F3 — the declared data checks, read against the APPLIED output
// =========================================================================

/// Drive one lap: confirm the red, spend a data-repair round, and return
/// the NEW plan id parked at the human gate.
fn confirm_red_and_repair(dir: &Path) -> String {
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "data-repair round: {err}");
    let json = json.expect("fulfill json");
    assert_eq!(
        json["state"], "needs_input",
        "a repair must park at the human gate, never apply itself: {json}"
    );
    json["plan_id"]
        .as_str()
        .expect("the repair produced a plan")
        .to_string()
}

/// THE F3 acceptance path. A model that applies cleanly but violates a
/// check the product declared about its own output is observed red,
/// routes into a repair round, converges, and lands a NEW proposal that
/// a FRESH human approval must accept before it applies.
///
/// The load-bearing negative is the plan id. A review marker is a file
/// named for the plan, and the plan id is a hash of the plan payload —
/// so a repair that recomputed the id of the plan the human already
/// approved would find that approval still on disk and re-apply with no
/// review at all. The loop's idempotency key rides inside the hashed
/// payload and advances with the journal, which is what makes that
/// impossible; this asserts the property, not the mechanism.
#[test]
fn a_data_red_after_apply_routes_to_repair_behind_a_new_human_gate() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_data_repair(DRAFT_SQL));

    let first_plan = drive_to_plan_review(dir);
    let (code, _j, _o, err) = rocky(dir, &["review", &first_plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");

    // The apply succeeds — and the output is wrong.
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(
        code, 4,
        "a data-red is its own exit code, never a clean stop: {err}"
    );
    assert_eq!(
        json["state"], "observed_failing",
        "applied, and failing its own declared checks: {json}"
    );
    assert_ne!(
        json["state"], "observing",
        "a failing product must never be recorded as a healthy one"
    );
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("failing its own declared data checks"),
        "{message}"
    );
    assert!(
        message.contains("violating row"),
        "the stop reports what the check MEASURED, not just that one failed: {message}"
    );
    assert!(
        message.contains("expression"),
        "and which check it was: {message}"
    );

    // The apply really did happen: the wrong number is live.
    {
        let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
        let revenue: f64 = conn
            .query_row("SELECT revenue_eur FROM out.revenue_daily", [], |r| {
                r.get(0)
            })
            .expect("target table");
        assert!(revenue < 0.0, "the bad data is applied, not prevented");
    }

    // A second, independent reading confirms it and spends one round.
    let repaired_plan = confirm_red_and_repair(dir);
    assert_ne!(
        repaired_plan, first_plan,
        "a repair that reused the applied plan's id would inherit its approval marker"
    );

    // The earlier approval buys the new plan nothing.
    let (code, _j, _o, _e) = rocky(dir, &["apply", &repaired_plan]);
    assert_ne!(
        code, 0,
        "a bare apply of the repaired plan must refuse — a data-red grants the loop \
         no authority it lacked"
    );

    // A FRESH approval, and only then does it apply.
    let (code, _j, _o, err) = rocky(dir, &["review", &repaired_plan, "--approve"]);
    assert_eq!(code, 0, "fresh review: {err}");
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "repaired apply: {err}");
    assert_eq!(
        json.expect("json")["state"],
        "observing",
        "the repaired output passes its own checks"
    );
    {
        let conn = duckdb::Connection::open(dir.join("wh.duckdb")).expect("duckdb");
        let revenue: f64 = conn
            .query_row("SELECT revenue_eur FROM out.revenue_daily", [], |r| {
                r.get(0)
            })
            .expect("target table");
        assert!(revenue >= 0.0, "the repaired output is correct: {revenue}");
    }

    // The journal shows the whole path, in order.
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let walk = deduped_states(&rows);
    let tail: Vec<&str> = walk
        .iter()
        .skip_while(|s| **s != "observed_failing")
        .copied()
        .collect();
    assert_eq!(
        tail,
        vec![
            "observed_failing",
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
        "the red re-enters at drafting and leaves through the SAME gates as any \
         other change; walk was {walk:?}"
    );
    assert!(
        rows.iter().any(|r| r.event.contains("data repair round 1")),
        "the round is journaled as a data repair"
    );
    assert!(
        rows.iter()
            .any(|r| r.event.contains("declared data checks FAILING")
                && r.event.contains("violating row")),
        "and the red verdict carries its evidence into the journal"
    );
    assert_eq!(
        rows.iter()
            .filter(|r| r.event.starts_with("applied ("))
            .count(),
        2,
        "exactly two applies: the original and the repaired one"
    );
    drop(store);

    // The worker was dispatched on the DATA-repair brief, not the
    // verify-repair one — pinned by transcript kind, because a
    // data-repair dispatched as a plain repair would hand the worker a
    // brief about a compiler error it cannot act on.
    // Transcripts are named `<stamp>-<kind>.log`. Matched by SUFFIX with
    // `data-repair` tried first: splitting on the last dash cannot tell a
    // data-repair from a plain repair, and telling them apart is the
    // whole point — a data-red dispatched as a verify-repair would hand
    // the worker a brief about a compiler error it cannot act on.
    let kind_of = |name: &str| -> &'static str {
        let stem = name.trim_end_matches(".log");
        for kind in ["data-repair", "elicitation", "drafting", "repair"] {
            if stem.ends_with(&format!("-{kind}")) {
                return kind;
            }
        }
        "unrecognised"
    };
    let mut kinds: Vec<&'static str> = std::fs::read_dir(
        dir.join(".rocky/fulfillment")
            .join(PRODUCT)
            .join("transcripts"),
    )
    .expect("transcripts")
    .map(|e| kind_of(&e.expect("entry").file_name().to_string_lossy()))
    .collect();
    kinds.sort_unstable();
    assert_eq!(
        kinds,
        vec!["data-repair", "drafting", "elicitation"],
        "the round ran on the DATA-repair brief; no verify-repair round was dispatched"
    );
}

/// The budget binds. A data-repair that keeps producing a failing output
/// exhausts the ceiling and lands `blocked`, naming the check — there is
/// no unbounded repair cycle against a live table.
///
/// This is also the test that would fail if the data budget were folded
/// into `repair_rounds`: that counter is reset on every successful
/// propose, and this loop proposes on every lap, so a shared counter
/// would be zeroed each time round and never reach any ceiling.
///
/// Note what bounds this in the meantime: every lap still crosses a
/// human review, so the loop was never re-applying unattended. The
/// ceiling is what makes the bound TESTABLE, and what stops a product
/// from cycling a reviewer forever on a defect the worker cannot fix.
#[test]
fn repeated_data_reds_exhaust_the_ceiling_and_block_naming_the_check() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    // The data-repair round never fixes anything.
    write_project(dir, &session_json_with_data_repair(NEGATIVE_DRAFT_SQL));
    let ceiling = rocky_fulfill::machine::MAX_REPAIR_ROUNDS;

    let mut plan = drive_to_plan_review(dir);
    for round in 0..ceiling {
        let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
        assert_eq!(code, 0, "review approve (round {round}): {err}");
        let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
        let json = json.expect("fulfill json");
        assert_eq!(code, 4, "round {round} must observe red: {err} {json}");
        assert_eq!(json["state"], "observed_failing", "round {round}: {json}");
        plan = confirm_red_and_repair(dir);
    }

    // The ceiling is spent. The next red does not even record a
    // repairable state: it blocks, and a human is the escalation.
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "final review approve: {err}");
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(code, 2, "an exhausted budget is `blocked`: {json}");
    assert_eq!(json["state"], "blocked", "{json}");
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("still fails its declared data checks"),
        "{message}"
    );
    assert!(
        message.contains("violating row"),
        "the block NAMES the check that would not go green: {message}"
    );
    assert!(
        message.contains(&ceiling.to_string()),
        "and how many rounds it spent: {message}"
    );
    assert!(
        json["next_command"]
            .as_str()
            .expect("next_command")
            .contains("--retry"),
        "a human is the escalation, not another round: {json}"
    );

    // And the ceiling really was reached by DATA repairs, not by the
    // verify budget standing in for them.
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    let repairs = rows
        .iter()
        .filter(|r| r.event.starts_with("data repair round "))
        .count();
    assert_eq!(
        repairs, ceiling as usize,
        "exactly {ceiling} data-repair rounds ran before the block"
    );
    let applies = rows
        .iter()
        .filter(|r| r.event.starts_with("applied ("))
        .count();
    assert_eq!(
        applies,
        ceiling as usize + 1,
        "and the live table was written a BOUNDED number of times"
    );
}

/// Resume honesty at all three F3 seams. A crash before, during, or
/// after the observation must resume into a fresh READING — never into
/// the verdict the record last carried.
#[test]
fn a_crash_at_every_observation_seam_resumes_into_a_re_read() {
    for seam in ["pre-observation", "mid-observation"] {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path();
        write_project(dir, &session_json_with_data_repair(DRAFT_SQL));
        let plan = drive_to_plan_review(dir);
        let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
        assert_eq!(code, 0, "review approve: {err}");

        // Abort mid-flight: the no-cleanup exit a SIGKILL produces.
        let (code, _j, _o, _e) =
            rocky_env(dir, &["fulfill", PRODUCT], &[("ROCKY_FULFILL_FAULT", seam)]);
        assert_ne!(code, 0, "the fault at '{seam}' must abort the process");
        {
            let store = state_store(dir);
            let record = store
                .fulfill_state_get(PRODUCT)
                .expect("state")
                .expect("record");
            assert_eq!(
                record.state.tag(),
                "applied",
                "a crash around the observation leaves `applied`, never a \
                 verdict nobody finished reading (seam {seam})"
            );
            assert_eq!(
                record.observation_detail, None,
                "and no evidence is recorded for a reading that did not complete"
            );
        }

        // The resume READS, and finds the red.
        let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
        let json = json.expect("fulfill json");
        assert_eq!(code, 4, "resume at '{seam}': {err} {json}");
        assert_eq!(json["state"], "observed_failing", "seam {seam}: {json}");
    }

    // The third seam: the data-repair transition is committed, the
    // worker not yet dispatched.
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_data_repair(DRAFT_SQL));
    let plan = drive_to_plan_review(dir);
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");
    let (code, _j, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 4, "the first reading records the red");

    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "pre-repair-reopen")],
    );
    assert_ne!(code, 0, "the fault must abort the process");
    {
        let store = state_store(dir);
        let record = store
            .fulfill_state_get(PRODUCT)
            .expect("state")
            .expect("record");
        assert_eq!(record.state.tag(), "drafting");
        assert_eq!(
            record.drafting_round,
            rocky_core::fulfill::DraftingRound::DataRepair,
            "the round is persisted WITH the transition that decided it, so the \
             resume dispatches a data repair rather than a plain draft"
        );
        assert_eq!(
            record.data_repair_rounds, 1,
            "the round was counted before the dispatch, so a crash cannot buy a free one"
        );
        assert!(
            record
                .observation_detail
                .as_deref()
                .is_some_and(|d| d.contains("violating row")),
            "and the evidence survives, so the resumed worker is not handed a blank: {:?}",
            record.observation_detail
        );
    }

    // The resume converges through the same human gate.
    let repaired = confirm_red_and_repair(dir);
    assert_ne!(repaired, plan);
    let (code, _j, _o, err) = rocky(dir, &["review", &repaired, "--approve"]);
    assert_eq!(code, 0, "fresh review: {err}");
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "repaired apply: {err}");
    assert_eq!(json.expect("json")["state"], "observing");
}

/// Deleting a check must not beat fixing it.
///
/// The sidecar holding the declared `[[tests]]` is byte-verified against
/// the committed lowering manifest at Phase B and then not looked at
/// again until observation — which is after an apply, and arbitrarily
/// later. Without a custody gate at that point, emptying the sidecar
/// yields `declared = 0`, which tallies identically to "every declared
/// check passed": the loop would transition a known-red product to a
/// healthy `observing`, clear the evidence, and refund the repair budget,
/// all while the bad table sits untouched.
///
/// That also makes the repair ceiling meaningless — alternate removing
/// and restoring the checks and the budget refills every lap.
///
/// So a sidecar that no longer matches what was approved is UNEVALUABLE:
/// the checks are not run, the state does not move, and the loop says
/// why. Deliberately not `blocked` — a human editing their own models
/// directory is ordinary and must not strand the product.
#[test]
fn emptying_the_sidecar_cannot_turn_a_known_red_into_observing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_data_repair(DRAFT_SQL));

    let plan = drive_to_plan_review(dir);
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 4, "the product is observed red first");
    assert_eq!(json.expect("json")["state"], "observed_failing");
    assert!(
        declared_check_count(dir) > 0,
        "the fixture must declare checks for this to mean anything"
    );

    // The attack: delete every declared check from the sidecar.
    let sidecar = dir.join(format!("models/{PRODUCT}.toml"));
    let text = std::fs::read_to_string(&sidecar).expect("sidecar");
    let mut document: toml::Table = toml::from_str(&text).expect("sidecar parses");
    document.remove("tests");
    document.remove("use_test");
    std::fs::write(&sidecar, toml::to_string(&document).expect("re-serialize"))
        .expect("write sidecar");
    assert_eq!(
        declared_check_count(dir),
        0,
        "the sidecar now declares nothing — the tally a clean run would produce"
    );

    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_ne!(
        json["state"], "observing",
        "a product whose checks were DELETED must never read as healthy: {json}"
    );
    assert_eq!(
        json["state"], "observed_failing",
        "and the known red is not cleared by a reading that could not be trusted: {json} {err}"
    );
    assert_eq!(code, 4, "still the data-red exit code");
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("not the ones this generation verified"),
        "the stop says the checks on disk are not the verified ones: {message}"
    );
    // And it names a way OUT — which is a RESTORE, stated before the
    // command. No engine verb adopts a post-apply check change: nothing
    // after an apply can re-enter `verifying` to pin a new digest.
    assert!(
        message.contains("restore the file you changed"),
        "the hold states the manual step first: {message}"
    );
    assert!(
        message.contains("approve the spec again"),
        "and the route for keeping the change: {message}"
    );

    // The evidence and the budget both survive — otherwise the ceiling
    // could be refilled by editing a file.
    let store = state_store(dir);
    let record = store
        .fulfill_state_get(PRODUCT)
        .expect("state")
        .expect("record");
    assert!(
        record
            .observation_detail
            .as_deref()
            .is_some_and(|d| d.contains("violating row")),
        "the recorded evidence survives: {:?}",
        record.observation_detail
    );
    assert_eq!(
        record.data_repair_rounds, 0,
        "no round was spent on a reading that never ran"
    );
    drop(store);

    // THE REMEDY IS REAL — and it is the restore, not a verb.
    //
    // `rocky product compile` was offered here once and does NOT work:
    // it byte-verifies before Phase B and refuses a drifted sidecar
    // outright. Asserting that refusal keeps the message honest, because
    // the moment someone re-offers the verb this test fails.
    let (code, _j, _o, err) = rocky(dir, &["product", "compile", PRODUCT]);
    assert_ne!(
        code, 0,
        "re-lowering must NOT be presented as the remedy — it refuses drift"
    );
    assert!(
        err.contains("tampered") || err.contains("drift"),
        "and it refuses for the reason the message relies on: {err}"
    );

    // Restoring is what works.
    std::fs::write(&sidecar, &text).expect("restore the sidecar");
    assert!(
        declared_check_count(dir) > 0,
        "the restored sidecar declares its checks again"
    );
    let (code, json, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(code, 0, "the loop resumes its normal course: {json}");
    // The record was already at `observed_failing`, so this reading is
    // the CONFIRMING one: the gate held while the checks were unreadable,
    // and the moment they are readable again the red is confirmed and a
    // repair round runs — parking at the human gate, as any repair does.
    assert_eq!(json["state"], "needs_input", "{json}");
    assert!(
        json["plan_id"].as_str().is_some_and(|p| !p.is_empty()),
        "with a new plan for a human to review: {json}"
    );

    // And the round was spent on the real finding, not on the custody
    // hold — the gate never burned budget while it was holding.
    let store = state_store(dir);
    let rows = store.fulfill_journal_rows(PRODUCT).expect("journal");
    assert!(
        rows.iter()
            .any(|r| r.event.starts_with("data repair round 1")),
        "exactly one repair round, and only after the checks were readable"
    );
    drop(store);
}

/// A BROKEN CONFIG IS NOT A CUSTODY DIVERGENCE.
///
/// The check set and the warehouse are now bound together — one call
/// reads the models directory and `rocky.toml` and hands back a handle
/// that owns both — so a config failure and a check-set failure arrive
/// at the same place. They must not leave through the same exit.
///
/// The custody hold's remedy is "restore the file you changed … then
/// put the change in the product spec". That is the right instruction
/// for an edited sidecar and a useless one for a mistyped adapter name:
/// there is nothing to restore into the verified set, and no spec field
/// carries a warehouse. Re-running after fixing the config genuinely
/// resolves it, which is the `Unreadable` remedy.
///
/// So the assertion that earns this test is the NEGATIVE one — the stop
/// must not say "restore the file you changed". Collapsing the two
/// failures into one error passes an assertion that only checks the
/// product held.
#[test]
fn an_unresolvable_warehouse_holds_without_the_custody_remedy() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan_id = drive_to_plan_review(dir);
    approve_and_apply(dir, &plan_id);

    // THE BREAKAGE: the config still parses and the pipeline still
    // resolves — only the adapter it names is gone. Nothing under
    // `models/` is touched, so the check digest is unchanged and the
    // custody comparison has no complaint to make.
    let broken = rocky_toml().replace(
        "[pipeline.p.target]\nadapter = \"default\"",
        "[pipeline.p.target]\nadapter = \"no_such_warehouse\"",
    );
    assert!(
        broken.contains("no_such_warehouse"),
        "the fixture must actually rewrite the target adapter"
    );
    std::fs::write(dir.join("rocky.toml"), &broken).expect("broken config");

    let (code, json, out, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    // Exit 0 and `applied`: a clean stop carrying an ask. Exit 4 is
    // reserved for `observed_failing`, and nothing here says the output is
    // wrong — only that it could not be read.
    assert_eq!(
        code, 0,
        "an unevaluable reading is a clean stop: {err}{out}"
    );
    assert_eq!(
        json["state"], "applied",
        "it holds where it is rather than claiming a healthy `observing`: {json}"
    );
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("the warehouse the declared checks run against could not be resolved"),
        "the stop names what actually failed: {message}"
    );
    assert!(
        !message.contains("restore the file you changed"),
        "a config problem must NOT print the custody remedy — there is no file to \
         restore into the verified set: {message}"
    );
    assert!(
        !message.contains("put the change in the product spec"),
        "and it must not send the operator to a spec field that cannot hold a \
         warehouse: {message}"
    );
    assert_eq!(
        json["next_command"].as_str(),
        Some(format!("rocky fulfill {PRODUCT}").as_str()),
        "re-running after fixing the config IS the remedy here: {json}"
    );

    // And it is genuinely recoverable: restore the config and the same
    // command clears the hold.
    std::fs::write(dir.join("rocky.toml"), rocky_toml()).expect("restore config");
    let (code, json, out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "the printed remedy has to work: {err}{out}");
    assert_eq!(
        json.expect("fulfill json")["state"],
        "observing",
        "the product returns to a healthy reading once the warehouse resolves"
    );
}

/// The `[[use_test]]` bypass, end to end.
///
/// A sidecar's `[[use_test]]` entry carries a NAME and a binding. The
/// check's type and its SQL live in `models/test_definitions.toml`,
/// which is not a lowering artifact, appears in no manifest, and is
/// hashed nowhere. So editing that one file changes the SQL the loop is
/// about to run while the sidecar stays byte-identical and every
/// recorded artifact hash still matches.
///
/// The assertion that makes this test worth having is the SECOND one:
/// that `artifact_problems` is empty at the moment the gate fires. That
/// is what proves the sidecar-hash check walked straight past this and
/// the digest over the expanded set is what caught it. Without it, the
/// test would pass on a gate that merely noticed the sidecar changed.
#[test]
fn editing_a_shared_test_definition_cannot_change_what_the_loop_executes() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json_with_data_repair(DRAFT_SQL));

    // The reference cannot be pre-placed: Phase A refuses a cold start
    // when the sidecar already exists (`model-collision`). It goes in at
    // the seam where the drafting worker has just written the sidecar and
    // Phase B has not merged yet — the same window a worker's own
    // `[[use_test]]` would arrive through. Phase B preserves it (not a
    // spec-owned key), so the digest the verify bundle pins covers the
    // expansion.
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "elicit: {err}");
    assert_eq!(json.expect("json")["state"], "needs_input");
    let (code, _j, _o, err) = rocky(dir, &["fulfill", "approve-spec", PRODUCT]);
    assert_eq!(code, 0, "approve-spec: {err}");

    let (code, _j, _o, _e) = rocky_env(
        dir,
        &["fulfill", PRODUCT],
        &[("ROCKY_FULFILL_FAULT", "post-drafting")],
    );
    assert_ne!(code, 0, "the fault aborts after drafting, before Phase B");

    std::fs::write(
        dir.join("models/test_definitions.toml"),
        "[revenue_is_positive]\ntype = \"expression\"\nexpression = \"revenue_eur >= 0\"\n",
    )
    .expect("definitions");
    let sidecar_path = dir.join(format!("models/{PRODUCT}.toml"));
    let drafted = std::fs::read_to_string(&sidecar_path).expect("the worker wrote a sidecar");
    std::fs::write(
        &sidecar_path,
        format!("{drafted}\n[[use_test]]\nname = \"revenue_is_positive\"\n"),
    )
    .expect("sidecar with a use_test reference");

    // Resume: Phase B merges, the bundle verifies, and a plan is pinned.
    let (code, json, out, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "resume to proposed: {err}{out}");
    let json = json.expect("fulfill json");
    assert_eq!(json["state"], "needs_input", "{json}");
    let plan = json["plan_id"].as_str().expect("plan pinned").to_string();
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");
    let (code, _j, _o, _e) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 4, "the product applies and is observed red");

    // The reference must have survived the merge, or this proves nothing.
    let sidecar_before = std::fs::read(&sidecar_path).expect("sidecar");
    assert!(
        String::from_utf8_lossy(&sidecar_before).contains("use_test"),
        "the fixture needs the reference to survive Phase B: {}",
        String::from_utf8_lossy(&sidecar_before)
    );

    // THE EDIT: only the shared definition, and it now asserts the
    // opposite of what was verified.
    std::fs::write(
        dir.join("models/test_definitions.toml"),
        "[revenue_is_positive]\ntype = \"expression\"\nexpression = \"revenue_eur < 999999\"\n",
    )
    .expect("edited definitions");
    assert_eq!(
        std::fs::read(&sidecar_path).expect("sidecar"),
        sidecar_before,
        "the sidecar must be byte-identical — that is the whole bypass"
    );

    // The sidecar-hash check sees nothing wrong. This is the control.
    let (code, status, _o, err) = rocky(dir, &["product", "status", PRODUCT]);
    assert_eq!(code, 0, "product status: {err}");
    let status = status.expect("status json");
    assert_eq!(
        status["artifact_problems"]
            .as_array()
            .map(Vec::len)
            .unwrap_or(0),
        0,
        "every recorded artifact hash still matches — the sidecar check is blind here: {status}"
    );

    // And the loop still refuses to run them.
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(code, 4, "still held, not clean: {err} {json}");
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("something changed what would run"),
        "the digest over the EXPANDED set is what catches this: {message}"
    );
    assert!(
        message.contains("restore the file you changed"),
        "and it still names the remedy — a restore, not a verb: {message}"
    );
}

/// A record carrying NO check digest must hold, not pass.
///
/// This is the upgrade path, and it is the only way the `None` arm is
/// reachable now that an unpinnable generation fails its verify bundle:
/// a product mid-flight when the binary was upgraded has a record
/// written before `checks_digest` existed, so it deserializes to `None`.
///
/// The rule that arm encodes is that absence of evidence is not evidence
/// of absence — "every claim matched" is trivially true when no claim
/// was made. Without this test the rule is unexercised: a mutation
/// making `None` pass survives the entire suite, which is how it was
/// found.
#[test]
fn a_record_with_no_recorded_digest_holds_rather_than_passing() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan = drive_to_plan_review(dir);
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "apply+observe: {err}");
    assert_eq!(
        json.expect("json")["state"],
        "observing",
        "the baseline is a clean, pinned generation"
    );

    // Rewrite the record the way an older binary left it: everything
    // else intact, no digest.
    {
        let store = state_store(dir);
        let current = store
            .fulfill_state_get(PRODUCT)
            .expect("state")
            .expect("record");
        assert!(
            current.checks_digest.is_some(),
            "the fixture must start pinned, or this proves nothing"
        );
        let mut older = current.clone();
        older.checks_digest = None;
        let row = rocky_core::fulfill::FulfillJournalRow {
            seq: 0,
            at: None,
            event: "test: simulate a record written before checks_digest existed".to_string(),
            from_state: Some(current.state.tag().to_string()),
            to_state: older.state.tag().to_string(),
            spec_digest: older.spec_digest.clone(),
            plan_id: older.plan_id.clone(),
            idempotency_key: older.idempotency_key.clone(),
        };
        let outcome = store
            .fulfill_state_cas(PRODUCT, Some(&current), &older, &row)
            .expect("seed the unpinned record");
        assert!(
            matches!(outcome, rocky_core::fulfill::FulfillCas::Won),
            "the seed must land"
        );
    }

    // The checks on disk are unchanged and would pass. The loop must
    // still refuse to call that health, because it cannot show they are
    // the checks this generation verified.
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(code, 0, "a hold is a clean stop: {err}");
    assert_ne!(
        json["state"], "observing",
        "an unpinned generation must not be reported healthy: {json}"
    );
    assert_eq!(json["state"], "applied", "{json}");
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("recorded no digest"),
        "and it says exactly what is missing: {message}"
    );
}

/// A DIGEST FROM AN OLDER PREIMAGE SCHEME IS NOT A CUSTODY DIVERGENCE.
///
/// The persisted `checks_digest` is an opaque string, and the
/// comparison at observation is exact. So the day the preimage changes
/// — which `CheckSetPreimage`'s own rule invites, and which this work
/// package already did once when it folded `[target]` in — every
/// generation the previous build pinned mismatches on a directory
/// nobody touched.
///
/// Reported as custody, that is unrecoverable. The remedy printed is
/// "restore the file you changed", and no restore changes a hash
/// algorithm. The landing is `applied`, where `rocky product approve`
/// is refused. Both exits are closed, permanently, by an upgrade.
///
/// The scheme tag makes the two facts separable, and this drives the
/// separation end to end: seed the record with the untagged digest an
/// intermediate build wrote, and the loop must say the two were never
/// comparable, must NOT print the restore, and must land somewhere a
/// human can act — `blocked`, whose `--retry` starts a generation that
/// re-pins under the current scheme.
///
/// The recovery half is the point. A test that only asserted the new
/// message would pass over a hold just as permanent as the one it
/// replaced.
#[test]
fn a_digest_from_an_older_scheme_blocks_with_a_remedy_that_works() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let dir = tmp.path();
    write_project(dir, &session_json(&[]));
    let plan = drive_to_plan_review(dir);
    let (code, _j, _o, err) = rocky(dir, &["review", &plan, "--approve"]);
    assert_eq!(code, 0, "review approve: {err}");
    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    assert_eq!(code, 0, "apply+observe: {err}");
    assert_eq!(
        json.expect("json")["state"],
        "observing",
        "the baseline is a clean, pinned generation"
    );

    // Seed the record the way a build BEFORE the scheme tag left it:
    // the same hash, without the `checks/N:` prefix. Nothing on disk
    // moves, so the check set itself is beyond suspicion — which is
    // exactly the case a strict compare gets wrong.
    {
        let store = state_store(dir);
        let current = store
            .fulfill_state_get(PRODUCT)
            .expect("state")
            .expect("record");
        let pinned = current
            .checks_digest
            .clone()
            .expect("the fixture must start pinned, or this proves nothing");
        let (scheme, hash) = pinned
            .split_once(':')
            .expect("a pinned digest carries its scheme");
        assert_eq!(
            scheme, "checks/1",
            "this test strips the CURRENT scheme tag; update it when the scheme is bumped"
        );
        let mut older = current.clone();
        older.checks_digest = Some(hash.to_string());
        let row = rocky_core::fulfill::FulfillJournalRow {
            seq: 0,
            at: None,
            event: "test: simulate a digest pinned under an older preimage scheme".to_string(),
            from_state: Some(current.state.tag().to_string()),
            to_state: older.state.tag().to_string(),
            spec_digest: older.spec_digest.clone(),
            plan_id: older.plan_id.clone(),
            idempotency_key: older.idempotency_key.clone(),
        };
        let outcome = store
            .fulfill_state_cas(PRODUCT, Some(&current), &older, &row)
            .expect("seed the old-scheme record");
        assert!(
            matches!(outcome, rocky_core::fulfill::FulfillCas::Won),
            "the seed must land"
        );
    }

    let (code, json, _o, err) = rocky(dir, &["fulfill", PRODUCT]);
    let json = json.expect("fulfill json");
    assert_eq!(code, 2, "blocked exits 2: {err} {json}");
    assert_eq!(
        json["state"], "blocked",
        "it must land where a human can act, not in the `applied` holding pattern that \
         refuses `rocky product approve`: {json}"
    );
    let message = json["message"].as_str().expect("message");
    assert!(
        message.contains("was taken under an older check-set scheme"),
        "the stop says the two were never comparable, not that something changed: {message}"
    );
    // THE ASSERTIONS THAT EARN THIS TEST are the negative ones. A
    // change that routed the scheme mismatch back through the custody
    // arm would still pass an assertion that only checked the product
    // held.
    assert!(
        !message.contains("restore the file you changed"),
        "no restore changes a hash algorithm, so the custody remedy must not be printed \
         here: {message}"
    );
    assert!(
        !message.contains("something changed what would run"),
        "and no comparison here says anything did — saying so would accuse an operator of \
         an edit that was never checked for: {message}"
    );
    // THE OTHER DIRECTION, which this test used to get wrong in its own
    // prose: the message must not claim disk is CLEAN either. The scheme
    // branch returns before the check set is loaded, so an edit to
    // unmanifested `models/test_definitions.toml` can sit underneath this
    // stop undetected. Custody here is unknown, and the stop has to say
    // so — asserted positively, because "does not say clean" is satisfied
    // by a message that says nothing at all.
    assert!(
        !message.contains("nothing on disk changed"),
        "the expanded check set was never loaded here, so the stop cannot claim disk is \
         unchanged: {message}"
    );
    assert!(
        message.contains("UNKNOWN"),
        "it must name the custody it did NOT establish, not quietly omit it: {message}"
    );
    assert_eq!(
        json["next_command"].as_str(),
        Some(format!("rocky fulfill {PRODUCT} --retry").as_str()),
        "and the printed command is the one that starts a generation this build can pin: \
         {json}"
    );

    // THE REMEDY IS EXECUTED. `--retry` re-enters at `spec_approved`,
    // and the fresh generation pins its own digest at its own verify —
    // so the product comes back, rather than trading one permanent hold
    // for another.
    let (code, json, out, err) = rocky(dir, &["fulfill", PRODUCT, "--retry"]);
    assert_eq!(code, 0, "the printed remedy has to work: {err}{out}");
    let json = json.expect("fulfill json");
    assert_eq!(
        json["state"], "needs_input",
        "the retry re-enters the loop at the next gate rather than staying blocked: {json}"
    );

    // AND THE OLD-SCHEME VALUE IS GONE, replaced rather than carried.
    // `--retry` clears the pin, and the run above went on through
    // `verifying` — which re-pins — before stopping at the plan gate.
    // So the record here is pinned again, under the CURRENT scheme, and
    // the next observation compares like for like.
    //
    // Asserted positively (`Some` that starts with the tag), not as
    // "None or tagged". The state here is `needs_input`, and an
    // `is_none_or` would silently pass through its `None` branch the
    // day the retry stopped one transition earlier — proving nothing
    // about the tagging it appears to check.
    let store = state_store(dir);
    let record = store
        .fulfill_state_get(PRODUCT)
        .expect("state")
        .expect("record");
    let repinned = record
        .checks_digest
        .as_deref()
        .expect("the retry ran through `verifying`, which pins a digest");
    assert!(
        repinned.starts_with("checks/1:"),
        "the new generation pins under the CURRENT scheme, so the old value is replaced \
         rather than carried: {repinned}"
    );
    drop(store);
}
