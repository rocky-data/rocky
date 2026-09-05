//! The real `rocky serve --ui` on the playground: the printed address carries
//! the token, the page is served with its headers and without a token, and
//! the API behind it still wants one.
//!
//! This test exists only in a build with the `ui` feature (the CI `ui` job
//! builds `engine/ui/dist` first and runs `cargo test --features ui`); a
//! plain `cargo test` compiles an empty file here.

#![cfg(feature = "ui")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

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

/// One HTTP/1.0 GET: the status line, the headers and the body.
fn http_get(port: u16, path: &str, extra: &str) -> (String, String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("read timeout");
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: 127.0.0.1:{port}\r\n{extra}\r\n"
    )
    .expect("request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("response");
    let text = String::from_utf8(raw).expect("utf-8 response");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("a header/body split in the response");
    let mut lines = head.lines();
    let status = lines.next().unwrap_or("").to_string();
    let headers = lines.collect::<Vec<_>>().join("\n").to_ascii_lowercase();
    (status, headers, body.to_string())
}

#[test]
fn real_server_prints_the_token_address_and_serves_the_public_page() {
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
    let config = root.join("rocky.toml");

    // Refusals first: a full-scope token is not a UI token.
    let refused = rocky()
        .current_dir(&root)
        .args([
            "--config",
            config.to_str().unwrap(),
            "serve",
            "--ui",
            "--token",
            "t",
            "--port",
            "0",
        ])
        .output()
        .expect("spawn rocky serve");
    assert!(!refused.status.success());
    assert!(
        String::from_utf8_lossy(&refused.stderr).contains("read-only"),
        "{}",
        String::from_utf8_lossy(&refused.stderr)
    );

    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let mut child = rocky()
        .current_dir(&root)
        .args([
            "--config",
            config.to_str().unwrap(),
            "serve",
            "--ui",
            "--token",
            "s3cret",
            "--token-scope",
            "read-only",
            "--port",
            &port.to_string(),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rocky serve --ui");
    let stdout = child.stdout.take().expect("piped stdout");
    let server = Server(child);
    let _keep_alive = &server;

    // The printed address: the page, with the token in the fragment.
    let mut first_line = String::new();
    BufReader::new(stdout)
        .read_line(&mut first_line)
        .expect("read the banner");
    assert_eq!(
        first_line.trim(),
        format!("Rocky UI: http://127.0.0.1:{port}/ui/#token=s3cret")
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let (status, _, _) = http_get(port, "/api/v1/health", "");
            if status.contains("200") {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rocky serve did not come up on {port}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }

    // The page: public, typed, with the headers, and it references the
    // hashed bundle under /ui/assets/.
    let (status, headers, body) = http_get(port, "/ui/", "");
    assert!(status.contains("200"), "{status}");
    assert!(headers.contains("content-type: text/html"), "{headers}");
    assert!(headers.contains("content-security-policy:"), "{headers}");
    assert!(headers.contains("x-frame-options: deny"), "{headers}");
    assert!(body.contains("/ui/assets/"), "{body}");

    // The API behind it still wants the token.
    let (status, _, body) = http_get(port, "/api/v1/meta", "");
    assert!(status.contains("401"), "{status}: {body}");
    let (status, _, body) = http_get(port, "/api/v1/meta", "Authorization: Bearer s3cret\r\n");
    assert!(status.contains("200"), "{status}: {body}");
    assert!(body.contains("\"capabilities\""), "{body}");

    // And a foreign Host is refused before routing.
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    write!(stream, "GET /ui/ HTTP/1.0\r\nHost: evil.example\r\n\r\n").expect("request");
    let mut raw = String::new();
    stream.read_to_string(&mut raw).expect("response");
    assert!(
        raw.starts_with("HTTP/1.0 421") || raw.starts_with("HTTP/1.1 421"),
        "{raw}"
    );
}
