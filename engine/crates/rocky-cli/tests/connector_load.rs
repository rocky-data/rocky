//! Child connector runner: spawn, limits, stop, verify and retry.
//!
//! Each test writes a small shell script as the "connector", runs it through
//! `run_with_retries`, and checks the verified batch or the failure. The
//! scripts copy ready-made Parquet files, so no Parquet writer is needed in
//! the child. Unix only: the scripts need `sh`.
#![cfg(unix)]

use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use rocky_cli::connector::runner::{AttemptError, ChildSpec, RunFailed, run_with_retries};
use rocky_core::connector::{EffectiveLimits, StateEnvelope};

struct Harness {
    root: tempfile::TempDir,
}

impl Harness {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("fixtures")).unwrap();
        Self { root }
    }

    fn landing(&self) -> PathBuf {
        self.root.path().join("landing")
    }

    /// A Parquet file of `rows` rows under `fixtures/`; returns its size.
    fn parquet(&self, name: &str, rows: i64) -> u64 {
        let path = self.root.path().join("fixtures").join(name);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from_iter_values(0..rows))],
        )
        .unwrap();
        let mut writer =
            ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        std::fs::metadata(&path).unwrap().len()
    }

    /// An executable shell script next to the fixtures.
    fn script(&self, name: &str, body: &str) -> PathBuf {
        let path = self.root.path().join(name);
        std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        path
    }

    fn fixtures(&self) -> PathBuf {
        self.root.path().join("fixtures")
    }

    /// Shell lines that copy a fixture into the scratch directory and write
    /// a matching result file.
    fn publish_script(&self, rows: i64, state_version: u32, extra: &str) -> String {
        let bytes = self.parquet("batch.parquet", rows);
        format!(
            "cat > request.json\n{extra}\ncp '{}' batch.parquet\n\
             cat > _rocky_result.json <<'EOF'\n\
             {{\"protocol_version\":0,\"files\":[{{\"path\":\"batch.parquet\",\"bytes\":{bytes},\"rows\":{rows}}}],\
             \"state\":{{\"schema_version\":{state_version},\"value\":{{\"cursor\":{rows}}}}}}}\n\
             EOF",
            self.fixtures().join("batch.parquet").display()
        )
    }
}

struct Case {
    argv: Vec<OsString>,
    secrets: Vec<(String, String)>,
    limits: EffectiveLimits,
    state: Option<StateEnvelope>,
    landing: PathBuf,
    retries: u32,
}

impl Case {
    fn new(harness: &Harness, script: &Path) -> Self {
        Self {
            argv: vec![script.as_os_str().to_owned()],
            secrets: Vec::new(),
            limits: EffectiveLimits {
                wall_time_secs: 30,
                cpu_time_secs: None,
                memory_bytes: None,
                output_bytes: 1 << 30,
            },
            state: None,
            landing: harness.landing(),
            retries: 0,
        }
    }

    async fn run(&self) -> Result<rocky_cli::connector::runner::RunSucceeded, RunFailed> {
        run_with_retries(&ChildSpec {
            name: "demo",
            argv: &self.argv,
            secrets: &self.secrets,
            run_id: "runid",
            limits: self.limits,
            state: self.state.as_ref(),
            state_schema_version: 1,
            landing_dir: &self.landing,
            retries: self.retries,
            output_poll: Duration::from_millis(100),
        })
        .await
    }
}

fn scratch_dirs(landing: &Path) -> Vec<String> {
    let Ok(entries) = std::fs::read_dir(landing) else {
        return Vec::new();
    };
    let mut names: Vec<String> = entries
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    names.sort();
    names
}

#[tokio::test]
async fn a_good_run_yields_a_verified_batch_and_keeps_the_scratch_dir() {
    let h = Harness::new();
    let script = h.script("ok.sh", &h.publish_script(4, 1, ""));
    let case = Case::new(&h, &script);
    let ran = case.run().await.expect("run succeeds");
    assert_eq!(ran.attempts, 1);
    assert_eq!(ran.batch.total_rows(), 4);
    assert_eq!(ran.batch.state.value["cursor"], 4);
    assert_eq!(scratch_dirs(&h.landing()), vec!["runid-1".to_string()]);
}

#[tokio::test]
async fn the_child_reads_the_request_from_stdin() {
    let h = Harness::new();
    let script = h.script("ok.sh", &h.publish_script(1, 1, ""));
    let mut case = Case::new(&h, &script);
    case.state = Some(StateEnvelope {
        schema_version: 1,
        value: serde_json::json!({"cursor": 7}),
    });
    case.limits.cpu_time_secs = Some(600);
    let ran = case.run().await.expect("run succeeds");
    let request: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(ran.batch.scratch.join("request.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(request["protocol_version"], 0);
    assert_eq!(request["run_id"], "runid");
    assert_eq!(
        request["output_dir"],
        ran.batch.scratch.to_str().unwrap(),
        "output_dir is the absolute scratch directory"
    );
    assert_eq!(request["state"]["schema_version"], 1);
    assert_eq!(request["state"]["value"]["cursor"], 7);
    assert_eq!(request["limits"]["wall_time_secs"], 30);
    assert_eq!(request["limits"]["cpu_time_secs"], 600);
    assert!(request["limits"].get("memory_bytes").is_none());
}

#[tokio::test]
async fn no_saved_state_is_sent_as_null() {
    let h = Harness::new();
    let script = h.script("ok.sh", &h.publish_script(1, 1, ""));
    let ran = Case::new(&h, &script).run().await.unwrap();
    let request: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(ran.batch.scratch.join("request.json")).unwrap(),
    )
    .unwrap();
    assert!(request["state"].is_null());
}

/// A failed attempt leaves no scratch directory and reports why.
async fn failure(h: &Harness, case: &Case) -> RunFailed {
    let failed = case.run().await.expect_err("the run must fail");
    assert!(
        scratch_dirs(&h.landing()).is_empty(),
        "a failed attempt's scratch directory is removed"
    );
    failed
}

#[tokio::test]
async fn a_crash_mid_write_fails_the_attempt() {
    let h = Harness::new();
    let script = h.script(
        "crash.sh",
        "cat > /dev/null\nprintf partial > half.parquet\nexit 1",
    );
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert_eq!(failed.attempts, 1);
    assert!(matches!(failed.error, AttemptError::Exit(ref m) if m.contains("code 1")));
}

#[tokio::test]
async fn a_hang_is_stopped_at_the_wall_time_with_its_whole_group() {
    let h = Harness::new();
    let pid_file = h.root.path().join("grandchild.pid");
    let script = h.script(
        "hang.sh",
        &format!("sleep 300 &\necho $! > '{}'\nwait", pid_file.display()),
    );
    let mut case = Case::new(&h, &script);
    case.limits.wall_time_secs = 1;
    let started = std::time::Instant::now();
    let failed = failure(&h, &case).await;
    assert!(
        matches!(failed.error, AttemptError::WallTime(1)),
        "{failed}"
    );
    assert!(started.elapsed() < Duration::from_secs(20));

    let pid = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .to_string();
    let mut gone = false;
    for _ in 0..50 {
        let alive = std::process::Command::new("kill")
            .args(["-0", &pid])
            .stderr(std::process::Stdio::null())
            .status()
            .unwrap()
            .success();
        if !alive {
            gone = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(gone, "the grandchild must die with the group");
}

#[tokio::test]
async fn output_past_the_cap_stops_the_child() {
    let h = Harness::new();
    // Three 1 MiB files stay under the single-file limit; together they pass
    // a 2 MiB cap. The child then sleeps, so only the byte poll can stop it.
    let script = h.script(
        "big.sh",
        "cat > /dev/null\nmkdir data\nfor n in 1 2 3; do head -c 1048576 /dev/zero > data/part-$n.bin; done\nsleep 120",
    );
    let mut case = Case::new(&h, &script);
    case.limits.output_bytes = 2 * 1024 * 1024;
    let failed = failure(&h, &case).await;
    assert!(
        matches!(failed.error, AttemptError::OutputCap { limit, .. } if limit == 2 * 1024 * 1024),
        "{failed}"
    );
}

#[tokio::test]
async fn a_result_with_the_wrong_state_version_fails_the_attempt() {
    let h = Harness::new();
    let script = h.script("v2.sh", &h.publish_script(2, 2, ""));
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert!(
        failed.to_string().contains("state.schema_version 2"),
        "{failed}"
    );
}

#[tokio::test]
async fn a_listed_file_that_is_missing_fails_the_attempt() {
    let h = Harness::new();
    let script = h.script(
        "missing.sh",
        "cat > /dev/null\ncat > _rocky_result.json <<'EOF'\n\
         {\"protocol_version\":0,\"files\":[{\"path\":\"nope.parquet\",\"bytes\":10,\"rows\":1}],\
         \"state\":{\"schema_version\":1,\"value\":null}}\nEOF",
    );
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert!(failed.to_string().contains("nope.parquet"), "{failed}");
}

#[tokio::test]
async fn a_size_mismatch_fails_the_attempt() {
    let h = Harness::new();
    let body = h
        .publish_script(2, 1, "")
        .replace("\"bytes\":", "\"bytes\":1");
    let script = h.script("size.sh", &body);
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert!(
        failed
            .to_string()
            .contains("bytes, but the result file says"),
        "{failed}"
    );
}

#[tokio::test]
async fn a_path_that_escapes_the_scratch_dir_fails_the_attempt() {
    let h = Harness::new();
    let bytes = h.parquet("outside.parquet", 1);
    let script = h.script(
        "escape.sh",
        &format!(
            "cat > /dev/null\ncat > _rocky_result.json <<'EOF'\n\
             {{\"protocol_version\":0,\"files\":[{{\"path\":\"../../fixtures/outside.parquet\",\"bytes\":{bytes},\"rows\":1}}],\
             \"state\":{{\"schema_version\":1,\"value\":null}}}}\nEOF"
        ),
    );
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert!(failed.to_string().contains("`..`"), "{failed}");
}

#[tokio::test]
async fn a_missing_result_file_fails_the_attempt() {
    let h = Harness::new();
    let script = h.script("quiet.sh", "cat > /dev/null");
    let failed = failure(&h, &Case::new(&h, &script)).await;
    assert!(failed.to_string().contains("wrote no"), "{failed}");
}

#[tokio::test]
async fn a_program_that_cannot_start_fails_the_attempt() {
    let h = Harness::new();
    let mut case = Case::new(&h, Path::new("/nonexistent/connector"));
    case.argv = vec![OsString::from("/nonexistent/connector")];
    let failed = failure(&h, &case).await;
    assert!(matches!(failed.error, AttemptError::Spawn(_)), "{failed}");
}

#[tokio::test]
async fn one_failed_attempt_then_success_loads_on_the_second() {
    let h = Harness::new();
    let marker = h.root.path().join("failed-once");
    let publish = h.publish_script(3, 1, "");
    let body = format!(
        "if [ ! -e '{m}' ]; then cat > /dev/null; touch '{m}'; exit 3; fi\n{publish}",
        m = marker.display()
    );
    let script = h.script("flaky.sh", &body);
    let mut case = Case::new(&h, &script);
    case.retries = 1;
    let ran = case.run().await.expect("the second attempt succeeds");
    assert_eq!(ran.attempts, 2);
    assert_eq!(ran.batch.total_rows(), 3);
    assert_eq!(
        scratch_dirs(&h.landing()),
        vec!["runid-2".to_string()],
        "each attempt has its own directory and only the failed one is removed"
    );
}

#[tokio::test]
async fn with_no_retries_the_first_failure_is_final() {
    let h = Harness::new();
    let marker = h.root.path().join("failed-once");
    let publish = h.publish_script(3, 1, "");
    let body = format!(
        "if [ ! -e '{m}' ]; then cat > /dev/null; touch '{m}'; exit 3; fi\n{publish}",
        m = marker.display()
    );
    let script = h.script("flaky.sh", &body);
    let mut case = Case::new(&h, &script);
    case.retries = 0;
    let failed = failure(&h, &case).await;
    assert_eq!(failed.attempts, 1);
}

#[tokio::test]
async fn every_attempt_failing_reports_the_attempt_count() {
    let h = Harness::new();
    let script = h.script("always.sh", "cat > /dev/null\nexit 2");
    let mut case = Case::new(&h, &script);
    case.retries = 2;
    let failed = failure(&h, &case).await;
    assert_eq!(failed.attempts, 3);
}

#[tokio::test]
async fn the_child_sees_the_resource_limits_applied_to_it() {
    let h = Harness::new();
    let script = h.script(
        "limits.sh",
        &h.publish_script(1, 1, "{ ulimit -St; ulimit -Ht; ulimit -Sf; } > limits.txt"),
    );
    let mut case = Case::new(&h, &script);
    case.limits.cpu_time_secs = Some(600);
    case.limits.output_bytes = 1_048_576;
    let ran = case.run().await.expect("run succeeds");
    let text = std::fs::read_to_string(ran.batch.scratch.join("limits.txt")).unwrap();
    let lines: Vec<u64> = text.lines().map(|l| l.trim().parse().unwrap()).collect();
    assert_eq!(lines[0], 600, "soft CPU limit");
    assert_eq!(
        lines[1], 605,
        "hard CPU limit sits 5 seconds above the soft one"
    );
    // `ulimit -f` counts 512- or 1024-byte blocks, by shell.
    assert!(
        lines[2] == 1024 || lines[2] == 2048,
        "file size limit is 1 MiB, got {} blocks",
        lines[2]
    );
}

#[tokio::test]
async fn stdout_and_stderr_of_the_child_do_not_stall_it() {
    let h = Harness::new();
    // Far more than a pipe buffer on both streams: if Rocky did not drain
    // them, the child would block and hit the wall time.
    let extra =
        "yes 'noise on stdout' | head -n 200000\nyes 'noise on stderr' | head -n 200000 >&2";
    let script = h.script("noisy.sh", &h.publish_script(1, 1, extra));
    let ran = Case::new(&h, &script).run().await.expect("run succeeds");
    assert_eq!(ran.batch.total_rows(), 1);
}
