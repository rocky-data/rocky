//! The `rocky connector init` scaffold runs through the real child runner.
//!
//! Each test scaffolds a connector, runs its `main.py` through
//! `run_with_retries` (which verifies the batch), and checks the result.
//! These tests choose inputs that write no rows, so they need `python3` but
//! not the `duckdb` CLI. A run that writes Parquet needs the CLI; the
//! `rocky connector test` checks cover that. Unix only: `main.py` runs
//! through its `#!` line.
#![cfg(unix)]

use std::ffi::OsString;
use std::path::PathBuf;
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
        let argv: Vec<OsString> = manifest
            .command
            .iter()
            .enumerate()
            .map(|(i, part)| {
                if i == 0 {
                    self.dir.join(part).into_os_string()
                } else {
                    OsString::from(part)
                }
            })
            .collect();
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
    // Rocky passes a state of version 2; the scaffold reads version 1.
    let saved = StateEnvelope {
        schema_version: 2,
        value: json!({"cursor": 1}),
    };
    let failed = s.run(Some(saved), json!({}), 2).await.unwrap_err();
    assert!(
        matches!(failed.error, AttemptError::Exit(_)),
        "{:?}",
        failed.error
    );
}
