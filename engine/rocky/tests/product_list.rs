//! `rocky product list` through the real binary: the working directory is
//! the project root, `--state-path` is the store, and the rows are the
//! projection `rocky product status <name>` makes for the same product.
//!
//! The in-process tests in `rocky-cli` prove list == status projection and
//! HTTP == helper. These close the last links: the CLI dispatch, the root
//! derivation from the working directory, the exit codes, and a real
//! `rocky serve` answering the same bytes the real CLI prints.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

/// The answer key's spec fixture, shared with the rocky-core lowering tests.
const SPEC_FIXTURE: &[u8] =
    include_bytes!("../../crates/rocky-core/src/product/testdata/revenue_daily.spec.toml");

fn rocky() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
}

fn run_json(root: &Path, state: &Path, args: &[&str]) -> (i32, serde_json::Value) {
    let out = rocky()
        .current_dir(root)
        .args(["-o", "json", "--state-path", state.to_str().unwrap()])
        .args(args)
        .output()
        .expect("spawn rocky");
    let code = out.status.code().expect("exit code, not a signal");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let body = serde_json::from_str(&stdout).unwrap_or_else(|err| {
        panic!(
            "`rocky {}` did not print JSON ({err}); exit {code}; stdout: {stdout}; stderr: {}",
            args.join(" "),
            String::from_utf8_lossy(&out.stderr)
        )
    });
    (code, body)
}

/// A project with no `products/` directory and no store lists nothing and
/// exits 0 — an empty project is not an error.
#[test]
fn empty_project_lists_nothing_and_exits_zero() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    std::fs::create_dir_all(&root).expect("mkdir");
    let state = dir.path().join("state.redb");

    let (code, body) = run_json(&root, &state, &["product", "list"]);
    assert_eq!(code, 0);
    assert_eq!(body["command"], "product_list");
    assert_eq!(body["count"], 0);
    assert_eq!(body["products"], serde_json::json!([]));
}

/// With one spec on disk, the list carries one row, and that row is the
/// projection of `product status` for the same name: every key the row
/// has, status has too, with the same value.
#[test]
fn list_row_is_the_status_projection() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("project");
    std::fs::create_dir_all(root.join("products")).expect("mkdir");
    std::fs::write(root.join("products/revenue_daily.toml"), SPEC_FIXTURE).expect("write spec");
    let state = dir.path().join("state.redb");

    let (code, list) = run_json(&root, &state, &["product", "list"]);
    assert_eq!(code, 0, "{list}");
    assert_eq!(list["count"], 1);
    let row = &list["products"][0];
    assert_eq!(row["name"], "revenue_daily");
    assert_eq!(row["spec_present"], true);
    assert_eq!(row["product_id"], "product:revenue_daily");

    let (code, status) = run_json(&root, &state, &["product", "status", "revenue_daily"]);
    assert_eq!(code, 0, "{status}");
    assert_eq!(status["command"], "product_status");
    for (key, value) in row.as_object().expect("row object") {
        let expected = match key.as_str() {
            "name" => &status["product"],
            // The list carries a count where status carries the list.
            "artifact_problems" => {
                assert_eq!(
                    value.as_u64().expect("count"),
                    status["artifact_problems"].as_array().expect("list").len() as u64
                );
                continue;
            }
            other => &status[other],
        };
        assert_eq!(value, expected, "row key `{key}` differs from status");
    }
}

/// A child process that is killed when the test ends, pass or fail.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// One HTTP/1.0 GET against the local server: the response body, and the
/// status line.
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
    let status_line = head.lines().next().unwrap_or("").to_string();
    (status_line, body.to_string())
}

/// The real server answers `GET /api/v1/products` and
/// `GET /api/v1/products/{name}` with the exact bytes the real CLI prints
/// for `product list` and `product status` on the same project and store.
///
/// The project is the playground scaffold with one spec added; `serve`
/// binds the same `--state-path` the CLI is given. The routes do not need
/// the compile the server starts in the background.
#[test]
fn real_server_answers_the_real_cli_bytes() {
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
    std::fs::create_dir_all(root.join("products")).expect("mkdir");
    std::fs::write(root.join("products/revenue_daily.toml"), SPEC_FIXTURE).expect("write spec");
    let state = dir.path().join("state.redb");
    let config = root.join("rocky.toml");
    assert!(
        config.is_file(),
        "the playground scaffold writes rocky.toml"
    );

    // A free loopback port, released before the server takes it.
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

    // Wait for the listener, not for the compile: health is auth-exempt and
    // answers as soon as the socket is bound.
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

    let cli_list = rocky()
        .current_dir(&root)
        .args([
            "-o",
            "json",
            "--state-path",
            state.to_str().unwrap(),
            "product",
            "list",
        ])
        .output()
        .expect("spawn rocky product list");
    assert!(
        cli_list.status.success(),
        "{}",
        String::from_utf8_lossy(&cli_list.stderr)
    );
    let (status, body) = http_get(port, "/api/v1/products");
    assert!(status.contains("200"), "{status}");
    assert_eq!(body, String::from_utf8_lossy(&cli_list.stdout));

    let cli_status = rocky()
        .current_dir(&root)
        .args([
            "-o",
            "json",
            "--state-path",
            state.to_str().unwrap(),
            "product",
            "status",
            "revenue_daily",
        ])
        .output()
        .expect("spawn rocky product status");
    assert!(
        cli_status.status.success(),
        "{}",
        String::from_utf8_lossy(&cli_status.stderr)
    );
    let (status, body) = http_get(port, "/api/v1/products/revenue_daily");
    assert!(status.contains("200"), "{status}");
    assert_eq!(body, String::from_utf8_lossy(&cli_status.stdout));
}

/// `rocky serve` refuses `--state-namespace` instead of answering from a
/// different store than the commands that set it.
#[test]
fn serve_refuses_a_state_namespace() {
    let dir = tempfile::tempdir().expect("tempdir");
    let out = rocky()
        .current_dir(dir.path())
        .args(["--state-namespace", "acme", "serve", "--port", "0"])
        .output()
        .expect("spawn rocky serve");
    assert_eq!(out.status.code(), Some(1), "{:?}", out.status);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not support --state-namespace"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("--state-path"),
        "names the flag that works: {stderr}"
    );
}
