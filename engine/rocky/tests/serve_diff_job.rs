//! `POST /api/v1/jobs/diff` on the real binary, end to end.
//!
//! The in-process tests in `rocky-cli` cannot run a job: there the job's
//! subprocess is `current_exe`, which is the test harness. Here the server is
//! the real `rocky`, so the diff job runs the real `rocky ci-diff` as its
//! child. Three claims:
//!
//! - the job's `result` is the CLI's own `rocky ci-diff` output, byte for byte
//!   once parsed, on a git fixture with a dropped column;
//! - every `git` the job starts sees `GIT_OPTIONAL_LOCKS=0`, even though the
//!   server itself runs without it (a `git` shim on `PATH` records what each
//!   call saw);
//! - the job leaves no `index.lock` behind.

#![cfg(unix)]

mod common;

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

fn rocky() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
}

/// A child process that is killed when the test ends, pass or fail.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One HTTP/1.0 request: the status line and the body.
fn http(port: u16, method: &str, path: &str, body: &str) -> (String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("read timeout");
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .expect("request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("response");
    let text = String::from_utf8(raw).expect("utf-8 response");
    let (head, body) = text.split_once("\r\n\r\n").expect("a header/body split");
    (
        head.lines().next().unwrap_or("").to_string(),
        body.to_string(),
    )
}

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A playground project in a git repository with two commits: the second
/// drops `first_order` from `revenue_summary`.
fn project_with_a_dropped_column(dir: &Path) -> PathBuf {
    let root = dir.join("project");
    let out = rocky()
        .args(["playground", root.to_str().unwrap()])
        .output()
        .expect("spawn rocky playground");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    git(&root, &["init", "-q", "-b", "main"]);
    git(&root, &["config", "user.email", "tester@example.com"]);
    git(&root, &["config", "user.name", "Tester"]);
    git(&root, &["config", "commit.gpgsign", "false"]);
    git(&root, &["add", "."]);
    git(&root, &["commit", "-q", "-m", "base"]);
    let model = root.join("models").join("revenue_summary.sql");
    let sql = std::fs::read_to_string(&model).expect("the model");
    let edited = sql.replace(",\n    first_order\n", "\n");
    assert_ne!(sql, edited, "the fixture edit must change the model");
    std::fs::write(&model, edited).expect("edit the model");
    git(&root, &["commit", "-q", "-am", "drop first_order"]);
    root
}

/// A `git` on `PATH` that records the `GIT_OPTIONAL_LOCKS` each call sees,
/// then runs the real git. Returns the directory to put on `PATH` and the log.
fn git_shim(dir: &Path) -> (PathBuf, PathBuf) {
    let real = Command::new("sh")
        .args(["-c", "command -v git"])
        .output()
        .expect("find git");
    let real = String::from_utf8(real.stdout).unwrap().trim().to_string();
    assert!(!real.is_empty(), "git must be on PATH");
    let bin = dir.join("shim");
    std::fs::create_dir_all(&bin).unwrap();
    let log = dir.join("git-env.log");
    let script = format!(
        "#!/bin/sh\necho \"${{GIT_OPTIONAL_LOCKS-unset}}\" >> '{}'\nexec '{real}' \"$@\"\n",
        log.display()
    );
    let shim = bin.join("git");
    std::fs::write(&shim, script).unwrap();
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(&shim, std::fs::Permissions::from_mode(0o755)).unwrap();
    (bin, log)
}

fn serve(dir: &Path, root: &Path, shim: &Path) -> (Server, u16) {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let path = format!(
        "{}:{}",
        shim.display(),
        std::env::var("PATH").unwrap_or_default()
    );
    let server = Server(
        rocky()
            .current_dir(dir)
            .env("PATH", path)
            .env_remove("GIT_OPTIONAL_LOCKS")
            .env_remove("ROCKY_SERVE_TOKEN")
            .env_remove("ROCKY_SERVE_TOKEN_SCOPE")
            .args([
                "--config",
                root.join("rocky.toml").to_str().unwrap(),
                "--state-path",
                dir.join("state.redb").to_str().unwrap(),
                "serve",
                "--models",
                root.join("models").to_str().unwrap(),
                "--port",
                &port.to_string(),
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn rocky serve"),
    );
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if common::serve_health_ok(port) {
            break;
        }
        assert!(Instant::now() < deadline, "rocky serve did not come up");
        std::thread::sleep(Duration::from_millis(200));
    }
    (server, port)
}

/// Submit a diff job and wait for its terminal record.
fn diff_job(port: u16, body: &str) -> serde_json::Value {
    let (status, response) = http(port, "POST", "/api/v1/jobs/diff", body);
    assert!(status.contains("202"), "{status}: {response}");
    let job_id = serde_json::from_str::<serde_json::Value>(&response).unwrap()["job_id"]
        .as_str()
        .expect("a job id")
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let (status, body) = http(port, "GET", &format!("/api/v1/jobs/{job_id}"), "");
        assert!(status.contains("200"), "{status}: {body}");
        let job: serde_json::Value = serde_json::from_str(&body).unwrap();
        if job["state"] != "running" && job["state"] != "queued" {
            assert_eq!(job["kind"], "diff", "{body}");
            return job;
        }
        assert!(Instant::now() < deadline, "the diff job never finished");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn the_diff_job_returns_the_cli_output_and_runs_every_git_without_optional_locks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = project_with_a_dropped_column(dir.path());
    let (shim, log) = git_shim(dir.path());
    let (server, port) = serve(dir.path(), &root, &shim);

    // Start from an empty log: only the job's own git calls count.
    std::fs::write(&log, "").unwrap();
    let job = diff_job(port, r#"{"base":"HEAD~1","semantic":true}"#);
    assert_eq!(job["state"], "succeeded", "{job}");

    // Read before the CLI runs below: only the job's own git calls are in the
    // log now. A job that lost the env would log `unset` here.
    let seen = std::fs::read_to_string(&log).expect("the git log");
    let lines: Vec<&str> = seen.lines().collect();
    assert!(!lines.is_empty(), "the job ran no git command");
    assert!(
        lines.iter().all(|line| *line == "0"),
        "a git command ran without GIT_OPTIONAL_LOCKS=0: {lines:?}"
    );

    let cli = rocky()
        .current_dir(&root)
        .args([
            "--output",
            "json",
            "--config",
            root.join("rocky.toml").to_str().unwrap(),
            "--state-path",
            dir.path().join("state.redb").to_str().unwrap(),
            "ci-diff",
            "HEAD~1",
            "--models",
            root.join("models").to_str().unwrap(),
            "--semantic",
        ])
        .output()
        .expect("spawn rocky ci-diff");
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let cli: serde_json::Value = serde_json::from_slice(&cli.stdout).expect("ci-diff JSON");
    assert_eq!(job["result"], cli, "the job result is the CLI's output");
    let models = cli["models"].as_array().expect("models");
    assert!(
        models.iter().any(|m| m["model_name"] == "revenue_summary"),
        "the fixture must show the dropped column: {cli}"
    );

    assert!(
        !root.join(".git").join("index.lock").exists(),
        "the diff left a git index lock"
    );
    drop(server);
}
