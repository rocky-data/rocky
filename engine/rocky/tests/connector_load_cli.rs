//! `rocky load --connector`: the real binary, end to end through the CLI.
//!
//! The fake connectors are shell or Python scripts written into a temp
//! project. Unix only: they need `sh`.
#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const SECRET: &str = "sk_live_super_secret_value_1234";

/// A temp project: `rocky.toml`, a connector directory and a landing area.
struct Project {
    dir: tempfile::TempDir,
}

impl Project {
    /// `connector_extra` is appended to the `[connector.demo]` block;
    /// `manifest` replaces the manifest text; `script` is the whole
    /// executable file, shebang line included.
    fn new(connector_extra: &str, manifest: &str, script: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::create_dir_all(root.join("connectors/demo")).unwrap();
        std::fs::write(
            root.join("rocky.toml"),
            format!(
                r#"[adapter.default]
type = "duckdb"
path = "warehouse.duckdb"

[adapter.bq]
type = "bigquery"
project_id = "p"

[connector.demo]
manifest = "connectors/demo/connector.toml"
output_uri = "file://./.rocky/landing/demo"
target = {{ catalog = "main", schema = "raw", table = "demo" }}
{connector_extra}
"#
            ),
        )
        .unwrap();
        std::fs::write(root.join("connectors/demo/connector.toml"), manifest).unwrap();
        let script_path = root.join("connectors/demo/run.sh");
        std::fs::write(&script_path, script).unwrap();
        std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755)).unwrap();
        Self { dir }
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn rocky(&self) -> Command {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_rocky"));
        cmd.current_dir(self.path())
            .args(["--config", "rocky.toml", "--output", "json"])
            .env_remove("RUST_LOG")
            .env("ROCKY_TRACE_DISABLE", "1");
        cmd
    }

    fn load(&self) -> Output {
        self.rocky()
            .args(["load", "--connector", "demo"])
            .output()
            .unwrap()
    }

    /// The scratch directories a successful run left behind.
    fn scratch_dirs(&self) -> Vec<PathBuf> {
        let landing = self.path().join(".rocky/landing/demo");
        let mut dirs: Vec<PathBuf> = std::fs::read_dir(landing)
            .map(|rd| rd.flatten().map(|e| e.path()).collect())
            .unwrap_or_default();
        dirs.sort();
        dirs
    }
}

fn manifest(egress: &str) -> String {
    format!(
        "protocol_version = 0\nname = \"demo\"\ncommand = [\"./run.sh\"]\n\
         state_schema_version = 1\n{egress}\n"
    )
}

const GOOD_MANIFEST_EGRESS: &str = "egress_hosts = [\"api.example.com\"]";

/// A script that reads the request and reports an empty batch.
const EMPTY_BATCH: &str = "cat > /dev/null\n\
    cat > _rocky_result.json <<'EOF'\n\
    {\"protocol_version\":0,\"files\":[],\"state\":{\"schema_version\":1,\"value\":{\"cursor\":1}}}\n\
    EOF";

/// A `/bin/sh` script with the given body.
fn sh(body: &str) -> String {
    format!("#!/bin/sh\n{body}\n")
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn the_childs_output_never_reaches_rockys_stdout() {
    let project = Project::new(
        "",
        &manifest(GOOD_MANIFEST_EGRESS),
        &sh(&format!(
            "echo 'JUNK-ON-STDOUT {{not json'\necho 'junk on stderr' >&2\n{EMPTY_BATCH}"
        )),
    );
    let output = project.load();
    assert!(output.status.success(), "{}", stderr_of(&output));
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!stdout.contains("JUNK-ON-STDOUT"), "{stdout}");
    let value: serde_json::Value =
        serde_json::from_str(&stdout).expect("stdout is exactly one JSON document");
    assert_eq!(value["command"], "load");
}

#[test]
fn the_childs_stderr_is_logged_with_secret_values_replaced() {
    let project = Project::new(
        "secrets = [\"STRIPE_API_KEY\"]",
        &manifest(GOOD_MANIFEST_EGRESS),
        &sh(&format!(
            "echo \"token is $STRIPE_API_KEY\" >&2\n{EMPTY_BATCH}"
        )),
    );
    let output = project
        .rocky()
        .args(["load", "--connector", "demo"])
        .env("STRIPE_API_KEY", SECRET)
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr_of(&output));
    let stderr = stderr_of(&output);
    assert!(!stderr.contains(SECRET), "{stderr}");
    assert!(stderr.contains("token is ${STRIPE_API_KEY}"), "{stderr}");
}

#[test]
fn the_child_sees_only_the_variables_rocky_hands_it() {
    // The probe needs python3. A missing interpreter is a failure, not a
    // silent pass: this test is the only proof of the environment boundary.
    let probe = Command::new("python3").arg("--version").output();
    assert!(
        probe.is_ok_and(|o| o.status.success()),
        "python3 is required on PATH to run the environment probe"
    );
    let project = Project::new(
        "secrets = [\"STRIPE_API_KEY\"]",
        &manifest(GOOD_MANIFEST_EGRESS),
        // The connector is a Python script itself, so no shell sits between
        // Rocky and the probe (a shell adds PWD, SHLVL and `_` on its own).
        "#!/usr/bin/env python3\n\
         import json, os, sys\n\
         sys.stdin.read()\n\
         json.dump({'env': dict(os.environ), 'cwd': os.getcwd()}, open('env.json', 'w'))\n\
         open('_rocky_result.json', 'w').write(json.dumps(\n\
             {'protocol_version': 0, 'files': [], 'state': {'schema_version': 1, 'value': None}}))\n",
    );
    let output = project
        .rocky()
        .args(["load", "--connector", "demo"])
        .env("STRIPE_API_KEY", SECRET)
        .env("LANG", "en_US.UTF-8")
        .env("ROCKY_TEST_WAREHOUSE_TOKEN", "must-not-leak")
        .env("AWS_ACCESS_KEY_ID", "must-not-leak")
        .env("AWS_SECRET_ACCESS_KEY", "must-not-leak")
        .env("DATABRICKS_TOKEN", "must-not-leak")
        .output()
        .unwrap();
    assert!(output.status.success(), "{}", stderr_of(&output));

    let scratch = project.scratch_dirs();
    assert_eq!(scratch.len(), 1, "{scratch:?}");
    let probe: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(scratch[0].join("env.json")).unwrap())
            .unwrap();
    let env = probe["env"].as_object().unwrap();

    // Variables the interpreter or the OS adds on its own, after Rocky has
    // started the program: Python coerces the C locale into `LC_CTYPE`, and
    // macOS libraries set `__CF_USER_TEXT_ENCODING`.
    let injected = ["LC_CTYPE", "__CF_USER_TEXT_ENCODING"];
    let mut keys: Vec<&str> = env
        .keys()
        .map(String::as_str)
        .filter(|k| !injected.contains(k))
        .collect();
    keys.sort_unstable();
    let mut expected = vec![
        "HOME",
        "LANG",
        "PATH",
        "ROCKY_RUN_ID",
        "STRIPE_API_KEY",
        "TMPDIR",
    ];
    expected.sort_unstable();
    assert_eq!(keys, expected, "the child's environment: {env:?}");

    assert_eq!(env["STRIPE_API_KEY"], SECRET);
    assert_eq!(env["LANG"], "en_US.UTF-8");
    let scratch_real = scratch[0].canonicalize().unwrap();
    for key in ["HOME", "TMPDIR"] {
        assert_eq!(
            Path::new(env[key].as_str().unwrap())
                .canonicalize()
                .unwrap(),
            scratch_real,
            "{key} points at the scratch directory"
        );
    }
    assert_eq!(
        Path::new(probe["cwd"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        scratch_real,
        "the working directory is the scratch directory"
    );
    let run_id = env["ROCKY_RUN_ID"].as_str().unwrap();
    assert_eq!(run_id.len(), 32);
    assert!(run_id.chars().all(|c| c.is_ascii_hexdigit()), "{run_id}");
    assert!(
        scratch[0]
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with(run_id),
        "the scratch directory is named with the run id"
    );
}

#[test]
fn a_connector_flag_cannot_be_mixed_with_the_per_file_options() {
    let project = Project::new("", &manifest(GOOD_MANIFEST_EGRESS), &sh(EMPTY_BATCH));
    for extra in [
        vec!["--source-dir", "somewhere"],
        vec!["--format", "parquet"],
        vec!["--target", "t"],
        vec!["--truncate"],
        vec!["--pipeline", "p"],
    ] {
        let output = project
            .rocky()
            .args(["load", "--connector", "demo"])
            .args(&extra)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{extra:?}");
        assert!(
            stderr_of(&output).contains("cannot be used with"),
            "{extra:?}: {}",
            stderr_of(&output)
        );
    }
}

#[test]
fn a_manifest_without_egress_hosts_is_refused_before_anything_runs() {
    let project = Project::new("", &manifest(""), &sh(EMPTY_BATCH));
    let output = project.load();
    assert!(!output.status.success());
    assert!(
        stderr_of(&output).contains("egress_hosts"),
        "{}",
        stderr_of(&output)
    );
    assert!(
        project.scratch_dirs().is_empty(),
        "no scratch directory, so no spawn"
    );
}

#[test]
fn config_errors_name_the_problem() {
    let cases = [
        ("retries = 11", "retries"),
        ("output_uri = \"s3://bucket/x\"", "not supported yet"),
        (
            "target = { adapter = \"bq\", catalog = \"c\", schema = \"s\", table = \"t\" }",
            "BigQuery",
        ),
    ];
    for (extra, expected) in cases {
        // Duplicate keys are a TOML error, so rewrite the block instead.
        let project = Project::new("", &manifest(GOOD_MANIFEST_EGRESS), &sh(EMPTY_BATCH));
        let config_path = project.path().join("rocky.toml");
        let mut config = std::fs::read_to_string(&config_path).unwrap();
        if let Some((key, _)) = extra.split_once(" =") {
            let start = config
                .lines()
                .position(|l| l.starts_with(&format!("{key} =")));
            if let Some(index) = start {
                let mut lines: Vec<&str> = config.lines().collect();
                lines.remove(index);
                config = lines.join("\n");
            }
        }
        config.push_str(&format!("\n{extra}\n"));
        std::fs::write(&config_path, config).unwrap();
        let output = project.load();
        assert!(!output.status.success(), "{extra}");
        assert!(
            stderr_of(&output).contains(expected),
            "{extra}: {}",
            stderr_of(&output)
        );
        assert!(project.scratch_dirs().is_empty(), "{extra}");
    }
}

#[test]
fn a_secret_whose_variable_is_unset_is_an_error_at_run_time() {
    let project = Project::new(
        "secrets = [\"STRIPE_API_KEY\"]",
        &manifest(GOOD_MANIFEST_EGRESS),
        &sh(EMPTY_BATCH),
    );
    let output = project
        .rocky()
        .args(["load", "--connector", "demo"])
        .env_remove("STRIPE_API_KEY")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr_of(&output).contains("STRIPE_API_KEY"),
        "{}",
        stderr_of(&output)
    );
    assert!(
        project.scratch_dirs().is_empty(),
        "no spawn without the secret"
    );
}

#[test]
fn an_unknown_connector_name_lists_the_declared_ones() {
    let project = Project::new("", &manifest(GOOD_MANIFEST_EGRESS), &sh(EMPTY_BATCH));
    let output = project
        .rocky()
        .args(["load", "--connector", "other"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        stderr_of(&output).contains("demo"),
        "{}",
        stderr_of(&output)
    );
}

fn alive(pid: &str) -> bool {
    Command::new("kill")
        .args(["-0", pid])
        .stderr(std::process::Stdio::null())
        .status()
        .unwrap()
        .success()
}

#[test]
fn a_sigterm_to_rocky_stops_the_connectors_whole_group() {
    let pid_dir = tempfile::tempdir().unwrap();
    let pid_file = pid_dir.path().join("grandchild.pid");
    let project = Project::new(
        "",
        &manifest(GOOD_MANIFEST_EGRESS),
        &sh(&format!(
            "cat > /dev/null\nsleep 300 &\necho $! > '{}'\nwait",
            pid_file.display()
        )),
    );
    let mut rocky = project
        .rocky()
        .args(["load", "--connector", "demo"])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    while std::fs::read_to_string(&pid_file)
        .unwrap_or_default()
        .trim()
        .is_empty()
    {
        assert!(
            started.elapsed().as_secs() < 30,
            "the connector never started"
        );
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    let grandchild = std::fs::read_to_string(&pid_file)
        .unwrap()
        .trim()
        .to_string();
    assert!(alive(&grandchild));

    Command::new("kill")
        .args(["-TERM", &rocky.id().to_string()])
        .status()
        .unwrap();
    let status = rocky.wait().unwrap();
    assert!(!status.success(), "an interrupted load is a failure");
    for _ in 0..50 {
        if !alive(&grandchild) {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
    }
    panic!("the connector's grandchild outlived the interrupted run");
}
