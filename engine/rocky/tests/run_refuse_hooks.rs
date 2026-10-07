//! `rocky run --refuse-hooks` (#2162): a config that would fire a `[hook]`
//! command or a webhook is refused before anything fires, and a config with
//! none runs as usual. The control run without the flag proves the hook does
//! fire, so the refusal test is not passing on a hook that never runs.

use std::fs;
use std::path::Path;
use std::process::{Command, Output};

const BASE: &str = r#"
[adapter]
type = "duckdb"
path = "probe.duckdb"

[pipeline.probe]
strategy = "full_refresh"
timestamp_column = "_updated_at"

[pipeline.probe.source.discovery]
adapter = "default"

[pipeline.probe.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.probe.target]
catalog_template = "probe"
schema_template = "staging__{source}"

[pipeline.probe.target.governance]
auto_create_schemas = true

[pipeline.probe.checks]
row_count = false
column_match = false
"#;

// A replication run fires `on_pipeline_start`; a transformation-only run
// fires no hooks today, so it could not show the hook firing.
const HOOK: &str = r#"
[[hook.on_pipeline_start]]
command = "touch hook_fired"
timeout_ms = 5000
on_failure = "warn"
"#;

const WEBHOOK: &str = r#"
[hook.webhooks.on_pipeline_complete]
url = "http://127.0.0.1:9/never"
"#;

fn project(extra: &str) -> tempfile::TempDir {
    let tmp = tempfile::tempdir().unwrap();
    let root = tmp.path();
    fs::write(root.join("rocky.toml"), format!("{BASE}{extra}")).unwrap();
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    conn.execute_batch(
        "CREATE SCHEMA raw__orders;
         CREATE TABLE raw__orders.orders (id BIGINT, _updated_at TIMESTAMP);
         INSERT INTO raw__orders.orders VALUES (1, TIMESTAMP '2024-01-01 00:00:00');",
    )
    .unwrap();
    drop(conn);
    tmp
}

fn rocky(root: &Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["-c", "rocky.toml"])
        .args(args)
        .current_dir(root)
        .output()
        .expect("rocky must launch")
}

/// Whether the replication wrote its target, `staging__orders.orders`.
fn target_exists(root: &Path) -> bool {
    let conn = duckdb::Connection::open(root.join("probe.duckdb")).unwrap();
    let n: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM information_schema.tables \
             WHERE table_schema = 'staging__orders' AND table_name = 'orders'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    n > 0
}

fn show(out: &Output) -> String {
    format!(
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

#[test]
fn without_the_flag_the_hook_fires() {
    let tmp = project(HOOK);
    let root = tmp.path();
    let out = rocky(root, &["run"]);
    assert!(out.status.success(), "run must exit 0\n{}", show(&out));
    assert!(
        root.join("hook_fired").exists(),
        "control: the hook must fire\n{}",
        show(&out)
    );
}

#[test]
fn the_flag_refuses_a_shell_hook_before_it_fires() {
    let tmp = project(HOOK);
    let root = tmp.path();
    let out = rocky(root, &["run", "--refuse-hooks"]);
    assert!(!out.status.success(), "run must be refused\n{}", show(&out));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("--refuse-hooks"),
        "the refusal must name the flag\n{}",
        show(&out)
    );
    assert!(
        !root.join("hook_fired").exists(),
        "the hook must not fire\n{}",
        show(&out)
    );
    assert!(
        !target_exists(root),
        "nothing may be written\n{}",
        show(&out)
    );
}

#[test]
fn the_flag_refuses_a_webhook() {
    let tmp = project(WEBHOOK);
    let out = rocky(tmp.path(), &["run", "--refuse-hooks"]);
    assert!(!out.status.success(), "run must be refused\n{}", show(&out));
    assert!(
        !target_exists(tmp.path()),
        "nothing may be written\n{}",
        show(&out)
    );
}

#[test]
fn the_flag_refuses_hooks_under_dag() {
    let tmp = project(HOOK);
    let root = tmp.path();
    let out = rocky(root, &["run", "--dag", "--refuse-hooks"]);
    assert!(!out.status.success(), "run must be refused\n{}", show(&out));
    assert!(
        !root.join("hook_fired").exists(),
        "the hook must not fire\n{}",
        show(&out)
    );
}

#[test]
fn the_flag_lets_a_config_without_hooks_run() {
    let tmp = project("");
    let root = tmp.path();
    let out = rocky(root, &["run", "--refuse-hooks"]);
    assert!(out.status.success(), "run must exit 0\n{}", show(&out));
    assert!(
        target_exists(root),
        "the replication must write its target\n{}",
        show(&out)
    );
}
