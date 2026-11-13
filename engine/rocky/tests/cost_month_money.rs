//! `rocky cost --month YYYY-MM --by product --output json` prices the month
//! from the prices set in `[cost]`, through the real binary.
//!
//! A project with no `[cost]` prices gets `no_price`, never a figure built
//! from Rocky's shipped default prices.

use std::path::Path;
use std::process::Command;

use chrono::{Datelike, TimeZone, Utc};
use rocky_core::state::{
    ModelExecution, RecordedTarget, RunRecord, RunStatus, RunTrigger, SessionSource, StateStore,
    UpstreamRead, UpstreamReads,
};

const SPEC: &str =
    include_str!("../../crates/rocky-core/src/product/testdata/revenue_daily.spec.toml");

const BASE_CONFIG: &str = "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
     [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
     [pipeline.main.target.governance]\nauto_create_schemas = true\n";

fn exec(name: &str, start: chrono::DateTime<Utc>, dur_ms: i64) -> ModelExecution {
    ModelExecution {
        row_count: None,
        model_name: name.to_string(),
        started_at: start,
        finished_at: start + chrono::Duration::milliseconds(dur_ms),
        duration_ms: dur_ms as u64,
        rows_affected: Some(1),
        status: "success".to_string(),
        sql_hash: format!("hash_{name}"),
        skip_hash: None,
        upstream_freshness: None,
        bytes_scanned: None,
        bytes_written: None,
        egress_bytes: None,
        tenant: None,
        recipe_hash: None,
        input_hash: None,
        input_proof_class: None,
        env_hash: None,
        hash_scheme: None,
        output_column_hashes: None,
        attempts: Vec::new(),
        output_version: None,
        stored_bytes: None,
        upstream_reads: None,
        output_target: Some(RecordedTarget {
            model: name.to_string(),
            catalog: String::new(),
            schema: "main".to_string(),
            table: name.to_string(),
        }),
        due_at: None,
        wait_for_upstream_ms: None,
    }
}

/// Last month, so the month is over and run history covers it. Returns the
/// month as `YYYY-MM`.
fn seed_last_month(state_path: &Path) -> String {
    let today = Utc::now().date_naive();
    let (year, month) = if today.month() == 1 {
        (today.year() - 1, 12)
    } else {
        (today.year(), today.month() - 1)
    };
    let start = Utc.with_ymd_and_hms(year, month, 10, 2, 0, 0).unwrap();
    let mut product = exec("revenue_daily", start + chrono::Duration::minutes(1), 400);
    product.upstream_reads = Some(UpstreamReads {
        reads: vec![UpstreamRead {
            table: "memory.main.raw".to_string(),
            rows_scanned: 10,
            projected_columns: Vec::new(),
            est_bytes: Some(10),
        }],
        unattributed_scans: 0,
    });
    let models = vec![exec("raw", start, 1_000), product];
    let store = StateStore::open(state_path).unwrap();
    store
        .record_run(&RunRecord {
            run_id: "run-1".to_string(),
            started_at: start,
            finished_at: start + chrono::Duration::minutes(2),
            status: RunStatus::Success,
            models_executed: models,
            trigger: RunTrigger::Manual,
            config_hash: "cfghash".to_string(),
            triggering_identity: None,
            session_source: SessionSource::Cli,
            git_commit: None,
            git_branch: None,
            idempotency_key: None,
            target_catalog: None,
            hostname: "money-test-host".to_string(),
            rocky_version: "0.0.0-test".to_string(),
            check_outcomes: Vec::new(),
            pipeline: None,
            submission_id: None,
            check_gate_failed: false,
            verify_after_failed: false,
            rocky_branch: None,
            run_scope: Some(rocky_core::state::RunScope::Production),
            queued_ms: None,
            source_volumes: Vec::new(),
            output_volumes: Vec::new(),
            hold: None,
        })
        .unwrap();
    format!("{year:04}-{month:02}")
}

fn month_json(cost: &str) -> serde_json::Value {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("products")).unwrap();
    std::fs::create_dir_all(root.join("models")).unwrap();
    std::fs::write(root.join("products/revenue_daily.toml"), SPEC).unwrap();
    std::fs::write(root.join("rocky.toml"), format!("{BASE_CONFIG}{cost}")).unwrap();
    let state_path = root.join("state.redb");
    let month = seed_last_month(&state_path);
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args([
            "--state-path",
            state_path.to_str().unwrap(),
            "cost",
            "--month",
            &month,
            "--by",
            "product",
            "--output",
            "json",
        ])
        .output()
        .expect("spawn rocky");
    assert!(
        out.status.success(),
        "rocky cost exited {:?}\nstderr:\n{}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("json stdout")
}

#[test]
fn month_cost_json_prices_the_product_from_cost_prices() {
    // $3.60 per machine-hour: 1 ms costs $0.000001.
    let json = month_json("\n[cost]\ncompute_cost_per_machine_hour = 3.6\n");
    assert_eq!(json["history_complete"], true, "{json:#}");
    let product = &json["products"][0];
    assert_eq!(product["product"], "revenue_daily");
    assert_eq!(product["money_status"], "priced", "{product:#}");
    // revenue_daily ran 400 ms and pays for the 1,000 ms raw it read.
    let money = product["money"].as_f64().unwrap();
    assert!((money - 0.0014).abs() < 1e-12, "{money}");
    assert_eq!(json["measured_total_money_status"], "priced");
    let parts = product["money_parts"].as_array().unwrap();
    // No adapter reports egress. The store was created after the month, so
    // its AI part is partial: it never saw the month's calls.
    for (figure, status) in [("egress", "not_measured"), ("ai_tokens", "partial")] {
        let part = parts.iter().find(|p| p["figure"] == figure).unwrap();
        assert_eq!(part["status"], status, "{figure}");
        assert!(part["usd"].is_null());
    }
}

/// `rocky cost` reads the AI spool that the LSP and MCP servers write, with
/// no writer to drain it: the calls show as tokens, charged to the product
/// that owns the named model, and the other call to `unallocated`.
#[test]
fn month_cost_json_reads_the_ai_spool() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("products")).unwrap();
    std::fs::create_dir_all(root.join("models")).unwrap();
    std::fs::write(root.join("products/revenue_daily.toml"), SPEC).unwrap();
    std::fs::write(
        root.join("rocky.toml"),
        format!("{BASE_CONFIG}\n[cost]\nai_cost_per_million_tokens = 2.0\n"),
    )
    .unwrap();
    let state_path = root.join("state.redb");
    let month = seed_last_month(&state_path);
    let at = {
        let (year, mo) = month.split_once('-').unwrap();
        Utc.with_ymd_and_hms(year.parse().unwrap(), mo.parse().unwrap(), 12, 9, 0, 0)
            .unwrap()
    };
    let call =
        |id: &str, model: Option<&str>, tokens: (u64, u64)| rocky_core::ai_usage::AiUsageRecord {
            id: id.to_string(),
            at,
            command: "lsp_ai_contract_fix".to_string(),
            model: model.map(str::to_string),
            llm_model: "claude-sonnet-4-6".to_string(),
            input_tokens: Some(tokens.0),
            output_tokens: Some(tokens.1),
        };
    rocky_core::ai_usage::append_to_spool(
        &rocky_core::ai_usage::spool_path(&state_path),
        &[
            call("a", Some("revenue_daily"), (100, 50)),
            call("a", Some("revenue_daily"), (100, 50)),
            call("b", None, (7, 3)),
        ],
    )
    .unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .current_dir(root)
        .args([
            "--state-path",
            state_path.to_str().unwrap(),
            "cost",
            "--month",
            &month,
            "--by",
            "product",
            "--output",
            "json",
        ])
        .output()
        .expect("spawn rocky");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let json: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    let amount = |amounts: &serde_json::Value, unit: &str| {
        amounts
            .as_array()
            .unwrap()
            .iter()
            .find(|a| a["unit"] == unit)
            .map_or(0, |a| a["amount"].as_u64().unwrap())
    };
    let product = &json["products"][0];
    assert_eq!(
        amount(&product["charged"], "ai_input_tokens"),
        100,
        "{json:#}"
    );
    assert_eq!(amount(&product["charged"], "ai_output_tokens"), 50);
    assert_eq!(amount(&json["unallocated"], "ai_input_tokens"), 7);
    assert_eq!(amount(&json["measured_total"], "ai_input_tokens"), 107);
    // The call that sits in the spool twice counts once. The spool is left
    // as it was: `rocky cost` only reads.
    let left =
        rocky_core::ai_usage::read_spool(&rocky_core::ai_usage::spool_path(&state_path)).unwrap();
    assert_eq!(left.records.len(), 3);
}

#[test]
fn month_cost_json_without_prices_is_no_price() {
    let json = month_json("");
    let product = &json["products"][0];
    assert_eq!(product["money_status"], "no_price", "{product:#}");
    assert!(product["money"].is_null());
    assert_eq!(json["measured_total_money_status"], "no_price");
    assert!(json["measured_total_money"].is_null());
    // Nothing is unallocated: a zero amount is $0 with or without a price.
    assert_eq!(json["unallocated_money_status"], "priced");
    assert_eq!(json["unallocated_money"], 0.0);
}
