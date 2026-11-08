//! A monthly product budget is checked at the end of a real `rocky run`,
//! through the binary against DuckDB.
//!
//! ```text
//!   run 1  revenue_daily builds; charged > 0 hours  -> breached, event, hook fires
//!   run 2  still over                               -> breached, no event, no hook
//!   rocky cost --month <this month> --by product    -> one breach event, as of run 1
//! ```
//!
//! Both runs exit 0: a product budget only warns.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const SPEC: &str =
    include_str!("../../crates/rocky-core/src/product/testdata/revenue_daily.spec.toml");

/// `max_compute_hours = 0`: any recorded model time is over.
const CONFIG: &str = r#"
[adapter]
type = "duckdb"
path = "fixture.duckdb"

[pipeline.transform]
type = "transformation"

[pipeline.transform.target.governance]
auto_create_schemas = true

[product_budget.revenue_daily]
max_compute_hours = 0
on_breach = "warn"

[hook.on_budget_breach]
command = "cat >> hook.log"
timeout_ms = 10000
"#;

fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join("rocky.toml"), CONFIG).expect("write config");
    fs::create_dir_all(dir.path().join("products")).expect("products dir");
    fs::write(dir.path().join("products/revenue_daily.toml"), SPEC).expect("write spec");
    let models = dir.path().join("models");
    fs::create_dir_all(&models).expect("models dir");
    // Enough rows that building the table records some model time.
    fs::write(
        models.join("revenue_daily.sql"),
        "SELECT range AS client_id, range % 7 AS bucket FROM range(300000)\n",
    )
    .expect("write sql");
    fs::write(
        models.join("revenue_daily.toml"),
        "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"fixture\"\nschema = \"marts\"\n",
    )
    .expect("write sidecar");
    dir
}

fn rocky(dir: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["--output", "json"])
        .arg("--config")
        .arg(dir.join("rocky.toml"))
        .arg("--state-path")
        .arg(dir.join("state.redb"))
        .args(args)
        .current_dir(dir)
        .env("RUST_LOG", "error")
        .output()
        .expect("spawn rocky")
}

fn json_ok(dir: &Path, args: &[&str]) -> serde_json::Value {
    let out = rocky(dir, args);
    assert!(
        out.status.success(),
        "rocky {args:?} exited {:?}: {}\nstdout: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr),
        String::from_utf8_lossy(&out.stdout)
    );
    serde_json::from_slice(&out.stdout).expect("JSON stdout")
}

/// The hook log: one JSON context per fire, one after another.
fn hook_fires(dir: &Path) -> Vec<serde_json::Value> {
    let Ok(text) = fs::read_to_string(dir.join("hook.log")) else {
        return Vec::new();
    };
    serde_json::Deserializer::from_str(&text)
        .into_iter::<serde_json::Value>()
        .map(|value| value.expect("hook context JSON"))
        .collect()
}

#[test]
fn a_production_run_checks_the_product_budget_and_fires_the_hook_once() {
    let dir = project();

    let first = json_ok(dir.path(), &["run", "--pipeline", "transform"]);
    let budgets = first["product_budgets"]
        .as_array()
        .unwrap_or_else(|| panic!("no product_budgets in {first:#}"));
    assert_eq!(budgets.len(), 1, "{first:#}");
    let budget = &budgets[0];
    assert_eq!(budget["product"], "revenue_daily");
    assert_eq!(budget["budget"]["verdict"], "breached", "{budget:#}");
    assert_eq!(budget["budget"]["on_breach"], "warn");
    let event = &budget["breach_event"];
    let run_one = event["run_id"].as_str().expect("event run_id").to_string();
    assert_eq!(event["breached"][0]["limit_type"], "max_compute_hours");

    let fires = hook_fires(dir.path());
    assert_eq!(fires.len(), 1, "{fires:#?}");
    let meta = &fires[0]["metadata"];
    assert_eq!(meta["scope"], "product_month");
    assert_eq!(meta["product"], "revenue_daily");
    assert_eq!(meta["month"], budget["month"]);
    assert_eq!(meta["limit_type"], "max_compute_hours");
    assert_eq!(fires[0]["run_id"], run_one.as_str());

    // Still over: the verdict stays, but there is no new event and no hook.
    let second = json_ok(dir.path(), &["run", "--pipeline", "transform"]);
    let budget = &second["product_budgets"][0];
    assert_eq!(budget["budget"]["verdict"], "breached", "{second:#}");
    assert!(budget.get("breach_event").is_none(), "{budget:#}");
    assert_eq!(hook_fires(dir.path()).len(), 1, "the hook fired again");

    // The monthly report shows the one event, as of the first run.
    let month = budget["month"].as_str().unwrap().to_string();
    let cost = json_ok(
        dir.path(),
        &["cost", "--month", &month, "--by", "product"],
    );
    let product = &cost["products"][0];
    assert_eq!(product["product"], "revenue_daily");
    assert_eq!(product["budget"]["verdict"], "breached", "{product:#}");
    let events = product["breach_events"].as_array().expect("breach_events");
    assert_eq!(events.len(), 1, "{product:#}");
    assert_eq!(events[0]["run_id"], run_one.as_str());
}

/// Without `[product_budget]` the run output carries no `product_budgets`
/// key at all, so its JSON does not change.
#[test]
fn a_project_without_product_budgets_has_no_product_budgets_key() {
    let dir = project();
    let text = CONFIG.split("[product_budget.revenue_daily]").next().unwrap();
    fs::write(dir.path().join("rocky.toml"), text).expect("write config");
    let run = json_ok(dir.path(), &["run", "--pipeline", "transform"]);
    assert!(run.get("product_budgets").is_none(), "{run:#}");
    let cost_month = chrono_month_now();
    let cost = json_ok(
        dir.path(),
        &["cost", "--month", &cost_month, "--by", "product"],
    );
    assert!(cost["products"][0].get("budget").is_none(), "{cost:#}");
    assert!(cost["products"][0].get("breach_events").is_none(), "{cost:#}");
}

fn chrono_month_now() -> String {
    chrono::Utc::now().format("%Y-%m").to_string()
}
