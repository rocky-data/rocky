//! `rocky plan` reports the first-run fill of a `time_interval` model.
//!
//! A model with a `first_partition` and no recorded partition fills from
//! there on its first run. The plan names the model, the partition count
//! and the range, notes it in the cost preview, and records the model in
//! the persisted plan. Once the model has run, the plan reports no fill.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn project(root: &Path, first_partition: &str) {
    let db = root.join("p.duckdb");
    fs::write(
        root.join("rocky.toml"),
        format!(
            "[adapter]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
             [pipeline.ingest]\nstrategy = \"full_refresh\"\n\n\
             [pipeline.ingest.source.discovery]\nadapter = \"default\"\n\n\
             [pipeline.ingest.source.schema_pattern]\nprefix = \"raw__\"\nseparator = \"__\"\n\
             components = [\"source\"]\n\n\
             [pipeline.ingest.target]\ncatalog_template = \"p\"\n\
             schema_template = \"staging__{{source}}\"\n",
            db.display()
        ),
    )
    .unwrap();
    let conn = duckdb::Connection::open(&db).unwrap();
    conn.execute_batch(&format!(
        "CREATE SCHEMA raw__shop; CREATE TABLE raw__shop.events AS \
         SELECT 1 AS id, TIMESTAMP '{first_partition} 12:00:00' AS ts;"
    ))
    .unwrap();
    drop(conn);
    let models = root.join("models");
    fs::create_dir_all(&models).unwrap();
    fs::write(
        models.join("daily_events.sql"),
        "SELECT CAST(ts AS DATE) AS event_date, id FROM raw__shop.events \
         WHERE ts >= @start_date AND ts < @end_date\n",
    )
    .unwrap();
    fs::write(
        models.join("daily_events.toml"),
        format!(
            "[strategy]\ntype = \"time_interval\"\ntime_column = \"event_date\"\n\
             granularity = \"day\"\nlookback = 0\nfirst_partition = \"{first_partition}\"\n\n\
             [target]\ncatalog = \"p\"\nschema = \"main\"\n"
        ),
    )
    .unwrap();
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml"])
        .args(args)
        .current_dir(root)
        .env_remove("ROCKY_PRINCIPAL")
        .output()
        .expect("rocky must launch")
}

fn json(out: &Output) -> serde_json::Value {
    assert!(
        out.status.success(),
        "rocky must exit 0\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    serde_json::from_slice(&out.stdout).expect("rocky prints JSON")
}

#[test]
fn plan_reports_a_first_run_fill_and_records_it() {
    let tmp = tempfile::tempdir().unwrap();
    let today = chrono::Utc::now().date_naive();
    let first = (today - chrono::Duration::days(2)).to_string();
    project(tmp.path(), &first);

    let plan = json(&rocky(tmp.path(), &["plan", "-o", "json"]));
    let fills = plan["first_run_fills"]
        .as_array()
        .expect("a fill is reported");
    assert_eq!(fills.len(), 1, "{plan:#}");
    assert_eq!(fills[0]["model"], "daily_events");
    assert_eq!(fills[0]["partitions"], 3);
    assert_eq!(fills[0]["from"], first.as_str());
    assert_eq!(fills[0]["to"], today.to_string());
    assert_eq!(fills[0]["fills"], true);
    let notes = plan["cost_preview"]["notes"].as_array().unwrap();
    assert!(
        notes
            .iter()
            .any(|n| n.as_str().unwrap().contains("first run fills 3 partitions")),
        "{plan:#}"
    );

    // The persisted plan records which models fill.
    let plan_id = plan["plan_id"].as_str().unwrap();
    let persisted = fs::read_to_string(
        tmp.path()
            .join(".rocky")
            .join("plans")
            .join(format!("{plan_id}.json")),
    )
    .unwrap();
    let persisted: serde_json::Value = serde_json::from_str(&persisted).unwrap();
    let recorded = persisted
        .pointer("/payload/policy_capabilities/first_run_fills")
        .unwrap_or_else(|| panic!("the plan records its fills: {persisted:#}"));
    assert_eq!(recorded, &serde_json::json!(["daily_events"]));

    // A plan with a partition flag starts no fill, so it reports none.
    let latest = json(&rocky(tmp.path(), &["plan", "--latest", "-o", "json"]));
    assert!(latest.get("first_run_fills").is_none(), "{latest:#}");

    // Once the model has run, it has recorded partitions: no fill.
    let run = rocky(
        tmp.path(),
        &["run", "--model", "daily_events", "-o", "json"],
    );
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );
    let replanned = json(&rocky(tmp.path(), &["plan", "-o", "json"]));
    assert!(replanned.get("first_run_fills").is_none(), "{replanned:#}");
}
