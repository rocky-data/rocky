//! The real `rocky serve` answers `GET /api/v1/review/queue` with the exact
//! bytes the real `rocky review --queue --output json` prints for the same
//! project and store.
//!
//! On a fresh playground the queue is empty, so the two clock-derived fields
//! (`staleness_seconds`, `score`) never appear and the comparison is byte for
//! byte. A populated queue is compared modulo the clock in the in-process
//! tests of `rocky-cli`; this test closes the CLI-dispatch and server-input
//! links only.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
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
        .set_read_timeout(Some(Duration::from_secs(10)))
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

#[test]
fn real_server_answers_the_real_cli_review_queue_bytes() {
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

    let cli = rocky()
        .current_dir(&root)
        .args([
            "-o",
            "json",
            "--state-path",
            state.to_str().unwrap(),
            "review",
            "--queue",
            "--models",
            root.join("models").to_str().unwrap(),
        ])
        .output()
        .expect("spawn rocky review --queue");
    assert!(
        cli.status.success(),
        "{}",
        String::from_utf8_lossy(&cli.stderr)
    );
    let (status, body) = http_get(port, "/api/v1/review/queue");
    assert!(status.contains("200"), "{status}");
    assert_eq!(body, String::from_utf8_lossy(&cli.stdout));
    let queue: serde_json::Value = serde_json::from_str(&body).expect("json body");
    assert_eq!(
        queue["total"], 0,
        "a fresh playground has nothing pending: {body}"
    );

    // A plan id no file backs is the documented 404 on the status route.
    let (status, body) = http_get(port, &format!("/api/v1/review/{}/status", "a".repeat(64)));
    assert!(status.contains("404"), "{status}: {body}");
    assert!(body.contains("plan_not_found"), "{body}");
}
