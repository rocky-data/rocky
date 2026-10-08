//! `POST /api/v1/jobs/approve` on the real binary, end to end.
//!
//! The in-process tests in `rocky-cli` cannot run a job: there the job's
//! subprocess is `current_exe`, which is the test harness. Here the server is
//! the real `rocky`, so the approve job runs the real `rocky review <plan_id>
//! --approve` as its child, with the `ROCKY_SESSION_SOURCE=http_api` the
//! server sets.
//!
//! The git identity the marker records is pinned per server with
//! `GIT_CONFIG_GLOBAL` (and no system config), so the test does not depend on
//! the machine's own git setup:
//!
//! - an identity resolves: the marker says `source: http_api` and names it,
//!   and `GET /api/v1/review/{plan_id}/status` shows the same;
//! - none resolves: the job fails with `approver_identity_unresolved` and
//!   writes no marker, where the CLI would have recorded `unknown`.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
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
        .set_read_timeout(Some(Duration::from_secs(30)))
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

/// A playground project with one backfill plan (always review-gated).
fn project_with_a_backfill_plan(dir: &Path) -> (std::path::PathBuf, String) {
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
    let out = rocky()
        .current_dir(&root)
        .args([
            "-o",
            "json",
            "--state-path",
            dir.join("state.redb").to_str().unwrap(),
            "backfill",
            "--model",
            "revenue_summary",
        ])
        .output()
        .expect("spawn rocky backfill");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json_start = stdout.find("\n{").map_or(0, |i| i + 1);
    let plan: serde_json::Value =
        serde_json::from_str(&stdout[json_start..]).expect("backfill prints its plan");
    let plan_id = plan["plan_id"].as_str().expect("a plan id").to_string();
    (root, plan_id)
}

/// `rocky serve` on a free loopback port, no token, with `gitconfig` as the
/// only git configuration the server and its children can see.
fn serve(dir: &Path, root: &Path, gitconfig: &Path) -> (Server, u16) {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let server = Server(
        rocky()
            .current_dir(root)
            .env("GIT_CONFIG_GLOBAL", gitconfig)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .env("HOME", dir)
            .env_remove("ROCKY_SERVE_TOKEN")
            .env_remove("ROCKY_SERVE_TOKEN_SCOPE")
            .env_remove("ROCKY_SESSION_SOURCE")
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
        if TcpStream::connect(("127.0.0.1", port)).is_ok()
            && http(port, "GET", "/api/v1/health", "").0.contains("200")
        {
            break;
        }
        assert!(Instant::now() < deadline, "rocky serve did not come up");
        std::thread::sleep(Duration::from_millis(200));
    }
    (server, port)
}

/// Submit an approve job and wait for its terminal record.
fn approve(port: u16, plan_id: &str) -> serde_json::Value {
    let (status, body) = http(
        port,
        "POST",
        "/api/v1/jobs/approve",
        &format!(r#"{{"plan_id":"{plan_id}"}}"#),
    );
    assert!(status.contains("202"), "{status}: {body}");
    let job_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .expect("a job id")
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let (status, body) = http(port, "GET", &format!("/api/v1/jobs/{job_id}"), "");
        assert!(status.contains("200"), "{status}: {body}");
        let job: serde_json::Value = serde_json::from_str(&body).unwrap();
        if job["state"] != "running" && job["state"] != "queued" {
            assert_eq!(job["kind"], "approve", "{body}");
            return job;
        }
        assert!(Instant::now() < deadline, "the approve job never finished");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn the_approve_job_stamps_http_api_and_the_identity_or_refuses_without_one() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (root, plan_id) = project_with_a_backfill_plan(dir.path());
    let marker = root
        .join(".rocky")
        .join("plans")
        .join(format!("{plan_id}.reviewed.json"));

    // No git identity: the job fails with the stable code, and no marker.
    let empty = dir.path().join("empty.gitconfig");
    std::fs::write(&empty, "").unwrap();
    let (server, port) = serve(dir.path(), &root, &empty);
    let job = approve(port, &plan_id);
    assert_eq!(job["state"], "failed", "{job}");
    assert!(
        job["error"]
            .as_str()
            .unwrap_or_default()
            .contains("approver_identity_unresolved"),
        "{job}"
    );
    assert!(!marker.exists(), "a refused approval must write no marker");
    drop(server);

    // An identity: the marker records it, over the HTTP API.
    let named = dir.path().join("named.gitconfig");
    std::fs::write(
        &named,
        "[user]\n\temail = operator@example.com\n\tname = Operator\n",
    )
    .unwrap();
    let (_server, port) = serve(dir.path(), &root, &named);
    let job = approve(port, &plan_id);
    assert_eq!(job["state"], "succeeded", "{job}");
    let written: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&marker).expect("the marker")).unwrap();
    assert_eq!(written["plan_id"], plan_id.as_str());
    assert_eq!(written["approver"]["source"], "http_api");
    assert_eq!(written["approver"]["email"], "operator@example.com");

    let (status, body) = http(port, "GET", &format!("/api/v1/review/{plan_id}/status"), "");
    assert!(status.contains("200"), "{status}: {body}");
    let review: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(review["reviewed"], true, "{body}");
    assert_eq!(review["approver"]["source"], "http_api", "{body}");
}
