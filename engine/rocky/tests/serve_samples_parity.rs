//! The real `rocky serve` answers `GET /api/v1/models/{name}/rows` with the
//! bytes the real CLI prints for the same model, and carries the two headers a
//! browser-reachable sample needs.
//!
//! The playground is a DuckDB project, so the sample really executes: this is
//! the one place the route's whole path is exercised end to end, warehouse call
//! included. The in-process tests of `rocky-cli` cover the refusals that need no
//! warehouse (the row cap, the consent gate, the permit).

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

/// One HTTP/1.0 GET: the status line, the headers, and the body.
fn http_get(port: u16, path: &str) -> (String, String, String) {
    http_get_with(port, path, &[])
}

/// [`http_get`] with extra request headers, each `(name, value)`.
fn http_get_with(port: u16, path: &str, headers: &[(&str, &str)]) -> (String, String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(60)))
        .expect("read timeout");
    let extra: String = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect();
    write!(
        stream,
        "GET {path} HTTP/1.0\r\nHost: 127.0.0.1\r\n{extra}\r\n"
    )
    .expect("request");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("response");
    let text = String::from_utf8(raw).expect("utf-8 response");
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("a header/body split in the response");
    (
        head.lines().next().unwrap_or("").to_string(),
        head.to_ascii_lowercase(),
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

/// Drop the field that measures how long the sample took.
fn without_duration(text: &str) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(text).expect("json body");
    value
        .as_object_mut()
        .expect("sample object")
        .remove("duration_ms")
        .expect("duration_ms");
    value
}

#[test]
fn real_server_answers_the_real_cli_sample_bytes() {
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

    // The models must be materialized before anything can be sampled from them.
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
            let (status, _, _) = http_get(port, "/api/v1/health");
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

    // The sample itself: the same bytes as the CLI, but for how long it took.
    let cli = cli_json(
        &root,
        &state,
        &[
            "preview",
            "rows",
            "--model",
            "customer_orders",
            "--limit",
            "3",
        ],
    );
    let (status, headers, body) = http_get(port, "/api/v1/models/customer_orders/rows?limit=3");
    assert!(status.contains("200"), "{status}: {body}");
    assert_eq!(without_duration(&body), without_duration(&cli));

    // The rows really came from the warehouse, so the parity above is about a
    // real query rather than two identical empty answers.
    let served: serde_json::Value = serde_json::from_str(&body).expect("json body");
    assert!(
        !served["columns"].as_array().expect("columns").is_empty(),
        "the sample returned no columns: {body}"
    );
    assert_eq!(served["limit_applied"], 3);
    assert!(
        served["executed_sql"]
            .as_str()
            .expect("executed_sql")
            .to_ascii_uppercase()
            .contains("LIMIT 3"),
        "the executed SQL does not carry the cap: {body}"
    );

    // Warehouse rows must not be cached by a browser, a proxy or a worker.
    assert!(
        headers.contains("cache-control: no-store"),
        "the sample response is cacheable: {headers}"
    );

    // A DuckDB project is local, so no consent header is needed — and passing
    // one changes nothing. (The header is really sent: an earlier version of
    // this check repeated the headerless call, #1816.)
    let (with_consent, _, consent_body) = http_get_with(
        port,
        "/api/v1/models/customer_orders/rows?limit=3",
        &[("X-Rocky-Allow-Warehouse", "true")],
    );
    assert!(with_consent.contains("200"), "{with_consent}");
    assert_eq!(
        without_duration(&consent_body),
        without_duration(&body),
        "consent on a local project changed the answer"
    );

    // The row cap is the route's, not the CLI's: the CLI takes any u32.
    let (capped, _, capped_body) = http_get(port, "/api/v1/models/customer_orders/rows?limit=501");
    assert!(capped.contains("400"), "{capped}: {capped_body}");

    // A model the project does not have.
    let (missing, _, missing_body) = http_get(port, "/api/v1/models/not_a_model/rows");
    assert!(missing.contains("404"), "{missing}: {missing_body}");
}

/// The consent header really travels through the real server (#1816): on a
/// remote adapter the headerless request is refused `403 warehouse_gated`
/// before anything is compiled, and the same request with
/// `X-Rocky-Allow-Warehouse: true` gets past the gate — to `422
/// compile_error`, since the project has no models to compile, which is the
/// proof the header was read. The DuckDB check above cannot show this: a
/// local project needs no consent, so the header changes nothing there by
/// design.
#[test]
fn consent_header_reaches_the_gate_on_a_remote_adapter() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().join("remote");
    std::fs::create_dir_all(root.join("models")).expect("models dir");
    let config = root.join("rocky.toml");
    std::fs::write(
        &config,
        "[adapter]\ntype = \"databricks\"\nhost = \"example.invalid\"\n\
         http_path = \"/sql/1.0/warehouses/x\"\ntoken = \"unused\"\n\n\
         [pipeline.main]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
         [pipeline.main.target.governance]\nauto_create_schemas = true\n",
    )
    .expect("write config");
    let state = dir.path().join("state.redb");

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
            let (status, _, _) = http_get(port, "/api/v1/health");
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

    let (refused, _, refused_body) = http_get(port, "/api/v1/models/orders/rows");
    assert!(refused.contains("403"), "{refused}: {refused_body}");
    assert!(
        refused_body.contains("warehouse_gated"),
        "the headerless request is refused at the gate: {refused_body}"
    );

    let (consented, _, consented_body) = http_get_with(
        port,
        "/api/v1/models/orders/rows",
        &[("X-Rocky-Allow-Warehouse", "true")],
    );
    assert!(
        !consented.contains("403"),
        "the header was not read: {consented}: {consented_body}"
    );
    assert!(
        consented.contains("422") && consented_body.contains("compile_error"),
        "past the gate the project itself answers: {consented}: {consented_body}"
    );
}
