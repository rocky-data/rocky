//! The `rocky connector init` scaffold and its helper.
//!
//! The first tests scaffold a connector, run its `main.py` through
//! `run_with_retries` (which verifies the batch), and check the result. They
//! choose inputs that write no rows, so they need `python3` but not the
//! `duckdb` CLI.
//!
//! The `write()` tests run the helper straight from `python3`, with a fake
//! `duckdb` first on PATH. The fake records the SQL it gets, keeps a copy of
//! the staged rows, and writes an empty target file. These tests check what
//! the helper sends and refuses; they do not check DuckDB's Parquet.
//!
//! Unix only: `main.py` runs through its `#!` line, and the fake is a shell
//! script.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use rocky_cli::commands::{ConnectorLang, scaffold_connector};
use rocky_cli::connector::runner::{
    AttemptError, ChildSpec, RunFailed, RunSucceeded, run_with_retries,
};
use rocky_core::connector::{ConnectorManifest, EffectiveLimits, StateEnvelope};
use serde_json::json;

struct Scaffold {
    _root: tempfile::TempDir,
    dir: PathBuf,
    landing: PathBuf,
}

impl Scaffold {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let dir = scaffold_connector(
            "demo",
            Some(&root.path().join("demo")),
            ConnectorLang::Python,
        )
        .unwrap();
        let landing = root.path().join("landing");
        Self {
            _root: root,
            dir,
            landing,
        }
    }

    async fn run(
        &self,
        state: Option<StateEnvelope>,
        options: serde_json::Value,
        state_schema_version: u32,
    ) -> Result<RunSucceeded, RunFailed> {
        let manifest = ConnectorManifest::load(&self.dir.join("connector.toml")).unwrap();
        let argv = manifest.resolved_command(&self.dir);
        let options = serde_json::to_vec(&options).unwrap();
        run_with_retries(&ChildSpec {
            name: "demo",
            argv: &argv,
            secrets: &[],
            run_id: "runid",
            limits: EffectiveLimits {
                wall_time_secs: 60,
                cpu_time_secs: None,
                memory_bytes: None,
                output_bytes: 1 << 30,
            },
            state: state.as_ref(),
            state_schema_version,
            landing_dir: &self.landing,
            retries: 0,
            output_poll: Duration::from_millis(100),
            options_json: &options,
        })
        .await
    }
}

fn envelope(value: serde_json::Value) -> StateEnvelope {
    StateEnvelope {
        schema_version: 1,
        value,
    }
}

#[tokio::test]
async fn a_first_run_with_no_rows_commits_a_null_cursor() {
    let s = Scaffold::new();
    let done = s.run(None, json!({"example_rows": 0}), 1).await.unwrap();
    assert!(done.batch.files.is_empty());
    assert_eq!(done.batch.state.schema_version, 1);
    assert_eq!(done.batch.state.value, json!({"cursor": null}));
}

#[tokio::test]
async fn a_saved_cursor_reaches_the_program_and_comes_back_unchanged() {
    let s = Scaffold::new();
    // The example source has rows 1 to 5; a cursor of 5 leaves none to read.
    let done = s
        .run(Some(envelope(json!({"cursor": 5}))), json!({}), 1)
        .await
        .unwrap();
    assert!(done.batch.files.is_empty());
    assert_eq!(done.batch.state.value, json!({"cursor": 5}));
}

#[tokio::test]
async fn a_state_schema_version_the_program_does_not_read_fails_the_run() {
    let s = Scaffold::new();
    // Rocky passes a state of version 2; the scaffold reads version 1. A
    // cursor of 5 reads no rows, so the run never needs `duckdb`: without the
    // helper's check it would finish, and the verifier would refuse the
    // result's version 1 with a `Verify` error, not an `Exit`.
    let saved = StateEnvelope {
        schema_version: 2,
        value: json!({"cursor": 5}),
    };
    let failed = s.run(Some(saved), json!({}), 2).await.unwrap_err();
    assert!(
        matches!(failed.error, AttemptError::Exit(_)),
        "{:?}",
        failed.error
    );
}

// --- write() with a fake duckdb ----------------------------------------------

const FAKE_DUCKDB: &str = r#"#!/bin/sh
sql=$(cat)
printf '%s\n' "$sql" >> "$FAKE_DUCKDB_LOG"
target=$(printf '%s\n' "$sql" | sed -n "s/.*) TO '\(.*\)' (FORMAT parquet);/\1/p")
staging=$(printf '%s\n' "$sql" | sed -n "s/.*read_json('\([^']*\)'.*/\1/p")
cp "$staging" "$FAKE_DUCKDB_ROWS"
: > "$target"
printf '[{"n":"%s"}]\n' "$(wc -l < "$staging" | tr -d ' ')"
"#;

/// The helper's `write()` and `finish()`, driven by a short Python program.
struct Helper {
    root: tempfile::TempDir,
}

struct Outcome {
    ok: bool,
    stderr: String,
    /// The SQL the fake got, empty when it was never called.
    sql: String,
    /// The staged rows the fake saw, one JSON object per line.
    rows: Vec<serde_json::Value>,
    /// Every file left in the output directory, sorted.
    left: Vec<String>,
    result: Option<serde_json::Value>,
}

impl Helper {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        scaffold_connector(
            "demo",
            Some(&root.path().join("conn")),
            ConnectorLang::Python,
        )
        .unwrap();
        let bin = root.path().join("bin");
        std::fs::create_dir(&bin).unwrap();
        std::fs::write(bin.join("duckdb"), FAKE_DUCKDB).unwrap();
        std::fs::set_permissions(bin.join("duckdb"), std::fs::Permissions::from_mode(0o755))
            .unwrap();
        std::fs::create_dir(root.path().join("out")).unwrap();
        std::fs::write(root.path().join("options.json"), "{}").unwrap();
        Self { root }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.root.path().join(name)
    }

    /// Run `body` after `run = rocky_connector.start(1)`, then `run.finish`.
    fn run(&self, body: &str) -> Outcome {
        let program = format!(
            "import datetime, decimal, sys\n\
             sys.path.insert(0, {conn:?})\n\
             import rocky_connector\n\
             run = rocky_connector.start(1)\n\
             {body}\n\
             run.finish({{\"cursor\": 1}})\n",
            conn = self.path("conn").display().to_string(),
        );
        let request = json!({
            "protocol_version": 0,
            "run_id": "r1",
            "output_dir": self.path("out"),
            "options_path": self.path("options.json"),
            "state": null,
            "limits": {"wall_time_secs": 60, "output_bytes": 1000},
            "a_field_from_a_newer_rocky": true,
        });
        let path = format!(
            "{}:{}",
            self.path("bin").display(),
            std::env::var("PATH").unwrap_or_default()
        );
        let mut child = Command::new("python3")
            .args(["-c", &program])
            .env("PATH", path)
            .env("FAKE_DUCKDB_LOG", self.path("sql.log"))
            .env("FAKE_DUCKDB_ROWS", self.path("rows.jsonl"))
            .stdin(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("python3 must be on PATH");
        std::io::Write::write_all(
            child.stdin.as_mut().unwrap(),
            request.to_string().as_bytes(),
        )
        .unwrap();
        let out = child.wait_with_output().unwrap();
        let read = |p: &Path| std::fs::read_to_string(p).unwrap_or_default();
        let mut left: Vec<String> = std::fs::read_dir(self.path("out"))
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        left.sort();
        let result = std::fs::read_to_string(self.path("out/_rocky_result.json"))
            .ok()
            .map(|t| serde_json::from_str(&t).unwrap());
        Outcome {
            ok: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
            sql: read(&self.path("sql.log")),
            rows: read(&self.path("rows.jsonl"))
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect(),
            left,
            result,
        }
    }
}

#[test]
fn write_stages_rows_in_one_column_order_and_reports_the_file() {
    let h = Helper::new();
    let out = h.run(
        "run.write([{\"name\": \"a\", \"id\": 1}, {\"id\": 2, \"name\": \"b\"}], \
         columns={\"id\": \"BIGINT\", \"name\": \"VARCHAR\"})",
    );
    assert!(out.ok, "{}", out.stderr);
    assert!(
        out.sql
            .contains(r#"COPY (SELECT "id", "name" FROM read_json("#),
        "{}",
        out.sql
    );
    assert!(
        out.sql
            .contains("columns = {'id': 'BIGINT', 'name': 'VARCHAR'}"),
        "{}",
        out.sql
    );
    let keys: Vec<Vec<&str>> = out
        .rows
        .iter()
        .map(|r| r.as_object().unwrap().keys().map(String::as_str).collect())
        .collect();
    assert_eq!(keys, vec![vec!["id", "name"], vec!["id", "name"]]);
    // The staging file is gone; only the batch and the result remain.
    assert_eq!(out.left, vec!["_rocky_result.json", "batch-0.parquet"]);
    assert_eq!(
        out.result.unwrap(),
        json!({
            "protocol_version": 0,
            "files": [{"path": "batch-0.parquet", "bytes": 0, "rows": 2}],
            "state": {"schema_version": 1, "value": {"cursor": 1}},
        })
    );
}

#[test]
fn a_row_key_outside_columns_stops_the_run_before_duckdb() {
    let h = Helper::new();
    let out = h.run("run.write([{\"id\": 1, \"extra\": \"x\"}], columns={\"id\": \"BIGINT\"})");
    assert!(!out.ok);
    assert!(out.stderr.contains("extra ['extra']"), "{}", out.stderr);
    assert!(out.sql.is_empty(), "duckdb ran: {}", out.sql);
    assert!(out.left.is_empty(), "{:?}", out.left);
}

#[test]
fn a_column_in_columns_that_no_row_has_stops_the_run() {
    let h = Helper::new();
    let out =
        h.run("run.write([{\"id\": 1}], columns={\"id\": \"BIGINT\", \"typo\": \"VARCHAR\"})");
    assert!(!out.ok);
    assert!(out.stderr.contains("missing ['typo']"), "{}", out.stderr);
    assert!(out.left.is_empty(), "{:?}", out.left);
}

#[test]
fn rows_with_different_keys_stop_the_run() {
    let h = Helper::new();
    let out = h.run("run.write([{\"id\": 1}, {\"id\": 2, \"late\": 3}])");
    assert!(!out.ok);
    assert!(out.stderr.contains("row 1"), "{}", out.stderr);
    assert!(out.left.is_empty(), "{:?}", out.left);
}

#[test]
fn a_second_write_with_other_columns_stops_the_run() {
    let h = Helper::new();
    let out = h.run("run.write([{\"id\": 1}])\nrun.write([{\"other\": 2}])");
    assert!(!out.ok);
    assert!(out.stderr.contains("an earlier write()"), "{}", out.stderr);
    assert!(out.result.is_none());
}

#[test]
fn a_second_write_keeps_the_first_column_order() {
    let h = Helper::new();
    let out = h.run("run.write([{\"a\": 1, \"b\": 2}])\nrun.write([{\"b\": 3, \"a\": 4}])");
    assert!(out.ok, "{}", out.stderr);
    let selects: Vec<&str> = out.sql.lines().filter(|l| l.starts_with("COPY")).collect();
    assert_eq!(selects.len(), 2, "{}", out.sql);
    assert!(
        selects
            .iter()
            .all(|l| l.contains(r#"SELECT "a", "b" FROM"#)),
        "{selects:?}"
    );
    let files = &out.result.unwrap()["files"];
    assert_eq!(files.as_array().unwrap().len(), 2);
}

#[test]
fn bytes_are_refused_and_dates_are_written_as_iso_text() {
    let h = Helper::new();
    let out = h.run("run.write([{\"id\": 1, \"blob\": b\"x\"}])");
    assert!(!out.ok);
    assert!(out.stderr.contains("type bytes"), "{}", out.stderr);
    assert!(out.left.is_empty(), "{:?}", out.left);

    let h = Helper::new();
    let out = h.run(
        "run.write([{\"at\": datetime.datetime(2026, 1, 2, 3, 4, 5), \
         \"amount\": decimal.Decimal(\"1.50\")}])",
    );
    assert!(out.ok, "{}", out.stderr);
    assert_eq!(
        out.rows,
        vec![json!({"at": "2026-01-02T03:04:05", "amount": "1.50"})]
    );
}

#[test]
fn a_type_that_is_not_a_plain_duckdb_type_is_refused() {
    let h = Helper::new();
    let out = h.run("run.write([{\"id\": 1}], columns={\"id\": \"BIGINT'; --\"})");
    assert!(!out.ok);
    assert!(
        out.stderr.contains("not a plain DuckDB type"),
        "{}",
        out.stderr
    );
    assert!(out.sql.is_empty(), "duckdb ran: {}", out.sql);
    assert!(out.left.is_empty(), "{:?}", out.left);
}
