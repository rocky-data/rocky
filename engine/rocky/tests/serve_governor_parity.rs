//! The real `rocky serve` answers the governor routes with the bytes the real
//! CLI prints for the same project and store.
//!
//! The playground is run once so the store holds a run; then `/brief` is
//! compared modulo its two clock fields, `/audit/scorecard` byte for byte,
//! and `/custody/{subject}` byte for byte for a playground model. This test
//! closes the CLI-dispatch and server-input links; the per-window and
//! per-subject coverage lives in the in-process tests of `rocky-cli`.

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

/// One HTTP/1.0 GET against the local server: the status line and the body.
fn http_get(port: u16, path: &str) -> (String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .expect("read timeout");
    write!(stream, "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n\r\n").expect("request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("response");
    let text = String::from_utf8(raw).expect("utf-8 response");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("a header/body split in the response");
    (
        head.lines().next().unwrap_or("").to_string(),
        body.to_string(),
    )
}

/// The CLI's stdout for `rocky -o json --state-path <state> <args…>` in `root`.
fn cli_json(root: &Path, state: &Path, args: &[&str]) -> String {
    let out = rocky()
        .current_dir(root)
        .args(["-o", "json", "--state-path", state.to_str().unwrap()])
        .args(args)
        .output()
        .expect("spawn rocky");
    assert!(
        out.status.success(),
        "rocky {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).expect("utf-8 stdout")
}

/// Drop the clock-derived fields of a digest.
fn brief_without_clock(text: &str) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(text).expect("json body");
    let obj = value.as_object_mut().expect("brief object");
    obj.remove("generated_at").expect("generated_at");
    obj.remove("since_timestamp").expect("since_timestamp");
    value
}

#[test]
fn real_server_answers_the_real_cli_governor_bytes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    let out = rocky()
        .args(["playground", root.to_str().unwrap()])
        .output()
        .expect("spawn rocky playground");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let state = dir.path().join("state.redb");
    let config = root.join("rocky.toml");

    // One run, so the digest and the custody chain have a run to cite.
    let run = rocky()
        .current_dir(&root)
        .args([
            "--config",
            config.to_str().unwrap(),
            "--state-path",
            state.to_str().unwrap(),
            "run",
        ])
        .output()
        .expect("spawn rocky run");
    assert!(
        run.status.success(),
        "{}",
        String::from_utf8_lossy(&run.stderr)
    );

    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let server = Server(
        rocky()
            .current_dir(&root)
            .args([
                "--config",
                config.to_str().unwrap(),
                "--state-path",
                state.to_str().unwrap(),
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
    let _keep_alive = &server;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let (status, _) = http_get(port, "/api/v1/health");
            if status.contains("200") {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "rocky serve did not come up on {port}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // The digest: identical but for the clock, and it cites the run.
    let cli = cli_json(&root, &state, &["brief", "--since", "7d"]);
    let (status, body) = http_get(port, "/api/v1/brief?since=7d");
    assert!(status.contains("200"), "{status}: {body}");
    let served = brief_without_clock(&body);
    assert_eq!(served, brief_without_clock(&cli));
    assert_eq!(served["runs"]["total"], 1, "{body}");

    // The scorecard over all time: byte for byte.
    let cli = cli_json(&root, &state, &["audit", "--scorecard"]);
    let (status, body) = http_get(port, "/api/v1/audit/scorecard");
    assert!(status.contains("200"), "{status}: {body}");
    assert_eq!(body, cli);

    // The custody chain of a playground model: byte for byte, and resolved.
    let models = root.join("models");
    let cli = cli_json(
        &root,
        &state,
        &[
            "audit",
            "--for",
            "revenue_summary",
            "--models",
            models.to_str().unwrap(),
        ],
    );
    let (status, body) = http_get(port, "/api/v1/custody/revenue_summary");
    assert!(status.contains("200"), "{status}: {body}");
    assert_eq!(body, cli);
    let chain: serde_json::Value = serde_json::from_str(&body).expect("json body");
    assert_eq!(chain["resolved"], true, "{body}");
    assert_eq!(chain["subject_kind"], "model");
}
