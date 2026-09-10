//! What `rocky serve` gives the browser UI.
//!
//! Two tests, and they need different things from the build:
//!
//! - The page itself: the printed address carries the token, the page is
//!   served with its headers and without a token, and the API behind it
//!   still wants one. This one needs `engine/ui/dist` in the binary, so it
//!   returns early when the embed is empty.
//! - The click path: which DAG node the model route can serve, and under
//!   which of its two names. This one asks only the API, so it never skips.
//!
//! Both live in a build with the `ui` feature; a plain `cargo test` compiles
//! an empty file here. Locally:
//!
//! ```text
//! cargo test --features ui --test serve_ui
//! ```
//!
//! In CI the job that runs them is **`Test`**, through `cargo nextest run
//! --all-features` (`engine-ci.yml`), which turns the feature on. That job
//! never builds `engine/ui/dist`, so the page test above takes its early
//! return there and only the click test does real work. The `Browser UI` job
//! is node only and has no Rust toolchain; the release-build smoke does embed
//! a real page, but it runs curl rather than this binary.

#![cfg(feature = "ui")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use rocky_core::unified_dag::NodeKind;

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
    // CI's `--all-features` test job builds this binary with the feature but
    // without `engine/ui/dist`, so the embed is empty and `--ui` refuses to
    // start (the flag tests cover that refusal). This test wants the real
    // page, which only a build made after `npm run build` carries.
    let dist_index = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../ui/dist/index.html");
    if !dist_index.is_file() {
        eprintln!(
            "skipping: {} is absent, so the binary embeds no page",
            dist_index.display()
        );
        return;
    }

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

/// The click path, against the live API rather than a fixture.
///
/// The SPA's DAG panel opens a model's pane by asking
/// `GET /api/v1/models/{name}`. Which node can answer that, and under which
/// of its two names, is the whole of D1: the route searches the compiled
/// model set, so only a transformation node is there, and it is there under
/// its `label`, never under its `kind:`-prefixed `id`.
///
/// This test needs no page, only the API, so it does **not** pass `--ui` and
/// does **not** skip when `engine/ui/dist` is absent. That is what lets CI's
/// `Test` job run it, since that job never builds the page.
///
/// The match in [`servable`] is exhaustive on purpose. A new variant in
/// `unified_dag.rs` fails this file to compile, which is the one mechanical
/// link this package has between the engine's kinds and the click path.
#[test]
fn only_a_transformation_node_is_servable_and_only_under_its_label() {
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
    widen_to_seven_kinds(&root, &config);

    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    // No `--ui` and no `--token`: a loopback server with no token needs no
    // auth, and the page this test never asks for is what needs the embed.
    let child = rocky()
        .current_dir(&root)
        .args([
            "--config",
            config.to_str().unwrap(),
            "serve",
            "--port",
            &port.to_string(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn rocky serve");
    let server = Server(child);
    let _keep_alive = &server;
    wait_for_health(port);
    // `/health` and `/dag` both answer before the project has compiled:
    // `health` is an unconditional handler and `full_dag` gates only on
    // `config_path`, which is set at construction. The model routes gate on
    // `compile_result`, and `serve` merely sleeps 100ms for the compile it
    // spawned. So without this latch a slow compile answers the first model
    // request `503 engine_not_ready`, which is neither the 200 nor the 404
    // this test asserts, and every assertion below fails on timing alone.
    wait_for_compiled_models(port);

    let (status, _, body) = http_get(port, "/api/v1/dag", "");
    assert!(status.contains("200"), "{status}: {body}");
    let dag: serde_json::Value = serde_json::from_str(&body).expect("a JSON DAG");
    let nodes = dag["nodes"].as_array().expect("nodes").clone();
    assert!(!nodes.is_empty(), "the DAG has no nodes: {body}");

    let mut kinds_seen = std::collections::BTreeSet::new();
    let mut servable_labels = Vec::new();
    for node in &nodes {
        let id = node["id"].as_str().expect("a node id");
        let label = node["label"].as_str().expect("a node label");
        // An unclassified kind fails here: `NodeKind` is the engine's own
        // enum, so a string it does not name will not deserialize.
        let kind: NodeKind = serde_json::from_value(node["kind"].clone())
            .unwrap_or_else(|e| panic!("unclassified node kind {}: {e}", node["kind"]));
        kinds_seen.insert(node["kind"].as_str().expect("a kind string").to_string());

        let (label_status, _, label_body) = http_get(
            port,
            &format!("/api/v1/models/{}", percent_encode(label)),
            "",
        );
        let (id_status, _, _) =
            http_get(port, &format!("/api/v1/models/{}", percent_encode(id)), "");

        if servable(kind) {
            assert!(
                label_status.contains("200"),
                "{kind:?} {label:?} should be servable: {label_status}: {label_body}"
            );
            servable_labels.push(label.to_string());
        } else {
            assert!(
                label_status.contains("404"),
                "{kind:?} {label:?} should not be servable: {label_status}: {label_body}"
            );
        }
        // The id is never the name the route wants — not even for the one
        // kind that is servable. This is the defect the panel used to have.
        assert!(
            id_status.contains("404"),
            "{kind:?} id {id:?} should always 404: {id_status}"
        );
    }

    servable_labels.sort();
    assert_eq!(
        servable_labels,
        vec!["customer_orders", "raw_orders", "revenue_summary"],
        "the playground's three models, each under its bare label"
    );
    // Without several kinds the loop above proves only one branch. The set is
    // read from the fixture the SPA's own tests run against, not written out
    // here, so the two cannot drift apart: a live server that stops emitting a
    // kind the fixture carries, or starts emitting one it does not, fails here
    // rather than leaving the SPA asserting against a stale capture.
    assert_eq!(
        kinds_seen,
        kinds_in_ui_fixture(),
        "the live DAG and engine/ui/src/test/fixtures/dag-mixed-kinds.json \
         disagree about which node kinds exist; recapture the fixture (its \
         README says how) or fix the project this test builds"
    );
}

/// The node kinds in the capture the SPA's tests read.
///
/// The fixture is recorded by hand from a real `rocky serve` — no script
/// regenerates it — so nothing but this comparison keeps it honest.
fn kinds_in_ui_fixture() -> std::collections::BTreeSet<String> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ui/src/test/fixtures/dag-mixed-kinds.json");
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let dag: serde_json::Value = serde_json::from_str(&raw).expect("the fixture is JSON");
    dag["nodes"]
        .as_array()
        .expect("the fixture has nodes")
        .iter()
        .map(|n| {
            n["kind"]
                .as_str()
                .expect("every fixture node has a kind")
                .to_string()
        })
        .collect()
}

/// Which node kinds `GET /api/v1/models/{label}` can answer.
///
/// Exhaustive with no wildcard: a new [`NodeKind`] variant must be decided
/// here, and the same table lives in `engine/ui/src/estate/nodeRoute.ts`.
fn servable(kind: NodeKind) -> bool {
    match kind {
        // The only kind whose label names a compiled model.
        NodeKind::Transformation => true,
        // A pipeline name, a seed name, a test label, or a phrase with a
        // space in it — none of them are in `Project.models`.
        NodeKind::Source
        | NodeKind::Replication
        | NodeKind::Quality
        | NodeKind::Snapshot
        | NodeKind::Load
        | NodeKind::Seed
        | NodeKind::Test => false,
    }
}

/// Grow `rocky playground` (three models) into a project that emits seven of
/// the engine's eight node kinds. The eighth, `Replication`, cannot be
/// configured: the parser expands a replication pipeline into `Source` +
/// `Load` and keeps the variant only to read stored DAGs.
fn widen_to_seven_kinds(root: &std::path::Path, config: &std::path::Path) {
    let mut toml = std::fs::OpenOptions::new()
        .append(true)
        .open(config)
        .expect("open rocky.toml");
    toml.write_all(
        br#"
[pipeline.ecommerce]
type = "replication"
strategy = "full_refresh"
timestamp_column = "_updated_at"

[pipeline.ecommerce.source.discovery]
adapter = "default"

[pipeline.ecommerce.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ecommerce.target]
catalog_template = "playground"
schema_template = "staging__{source}"

[pipeline.nightly_dq]
type = "quality"

[pipeline.nightly_dq.target]
adapter = "default"

[[pipeline.nightly_dq.tables]]
catalog = "playground"
schema = "main"
table = "raw_orders"

[pipeline.nightly_dq.checks]
enabled = true
row_count = true

[pipeline.customer_history]
type = "snapshot"
unique_key = ["customer_id"]
updated_at = "updated_at"

[pipeline.customer_history.source]
catalog = "playground"
schema = "main"
table = "customer_orders"

[pipeline.customer_history.target]
catalog = "playground"
schema = "snapshots"
table = "customer_orders_history"
"#,
    )
    .expect("append pipelines");

    let mut sidecar = std::fs::OpenOptions::new()
        .append(true)
        .open(root.join("models/revenue_summary.toml"))
        .expect("open the model sidecar");
    sidecar
        .write_all(
            br#"
[[tests]]
type = "not_null"
column = "customer_id"
"#,
        )
        .expect("append a declarative test");

    std::fs::create_dir_all(root.join("seeds")).expect("seeds dir");
    std::fs::write(
        root.join("seeds/country_codes.csv"),
        "code,name\nUS,United States\nPT,Portugal\n",
    )
    .expect("write a seed");
}

/// Percent-encode one path segment, as `encodeURIComponent` does for the
/// SPA. Two node labels carry a space (`ecommerce (source)`), which would
/// otherwise end the request line early.
fn percent_encode(segment: &str) -> String {
    let mut out = String::with_capacity(segment.len());
    for byte in segment.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// Block until the compile the server spawned at start-up has landed.
///
/// `GET /api/v1/models` reads the same `compile_result` the per-model route
/// reads, so a `200` here is the readiness the model assertions need. Before
/// it lands the route answers `503 engine_not_ready`, which would fail an
/// assertion that expects `200` or `404`.
fn wait_for_compiled_models(port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let (status, _, _) = http_get(port, "/api/v1/models", "");
        if status.contains("200") {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the project never compiled; /api/v1/models last said {status}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Block until the server answers `/api/v1/health`, or fail the test.
fn wait_for_health(port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            let (status, _, _) = http_get(port, "/api/v1/health", "");
            if status.contains("200") {
                return;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "rocky serve did not come up on {port}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}
