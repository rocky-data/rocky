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

    let (server, port) = serve_api(&root, &config);
    let _keep_alive = &server;

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
        kinds_seen.insert((
            node["kind"].as_str().expect("a kind string").to_string(),
            id.to_string(),
            label.to_string(),
        ));

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
    // Without several kinds the loop above proves only one branch, and the set
    // is read from the fixture the SPA's own tests run against rather than
    // written out here, so the two cannot drift apart unnoticed.
    //
    // Every node is compared by kind, id AND label, not by kind alone. A kind
    // set would still match after a node was added, removed or renamed, which
    // is most of what a stale capture looks like. What is deliberately NOT
    // compared is the rest of the payload — edges, targets, the engine version
    // string — because that is release churn, not drift the SPA can see. What
    // the SPA reads from this fixture is exactly these three fields.
    assert_eq!(
        kinds_seen,
        nodes_in_ui_fixture("dag-mixed-kinds.json"),
        "the live DAG and engine/ui/src/test/fixtures/dag-mixed-kinds.json \
         disagree about their nodes (kind, id, label); recapture the fixture \
         (its README says how) or fix the project this test builds"
    );
}

/// The model list names exactly the DAG models the detail route can serve.
///
/// This is what the estate screen's gate stands on (`nodeRoute.ts`). The DAG
/// reads every transformation pipeline's own models directory, and the
/// server compiles one. So a second pipeline with its models in `reporting/`
/// puts `weekly_revenue` in the graph and not in the compile (#2011). The
/// SPA marks a transformation node "not compiled" when its label is absent
/// from `GET /api/v1/models`. That is right only if, for every such node,
/// being listed is exactly a `200` from `GET /api/v1/models/{label}` and
/// being absent is exactly a `404`. Both directions are asserted here,
/// against the real server.
///
/// If the server starts compiling every pipeline's directory, the split
/// goes away and the `weekly_revenue` assertion fails. Keep the equivalence,
/// drop the split, and recapture the fixtures.
#[test]
fn the_model_list_names_exactly_the_dag_models_the_detail_route_serves() {
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
    add_a_second_models_directory(&root, &config);

    let (server, port) = serve_api(&root, &config);
    let _keep_alive = &server;

    let (status, _, body) = http_get(port, "/api/v1/models", "");
    assert!(status.contains("200"), "{status}: {body}");
    let list: serde_json::Value = serde_json::from_str(&body).expect("a JSON model list");
    let listed: std::collections::BTreeSet<String> = list["models"]
        .as_array()
        .expect("models")
        .iter()
        .map(|m| m["name"].as_str().expect("a model name").to_string())
        .collect();
    // The SPA reads a list whose count disagrees with its entries as unknown.
    // A real list must never be read that way.
    assert_eq!(list["count"].as_u64(), Some(listed.len() as u64), "{body}");

    let (status, _, body) = http_get(port, "/api/v1/dag", "");
    assert!(status.contains("200"), "{status}: {body}");
    let dag: serde_json::Value = serde_json::from_str(&body).expect("a JSON DAG");
    let nodes = dag["nodes"].as_array().expect("nodes");
    let mut drawn = std::collections::BTreeSet::new();
    let mut live_nodes = std::collections::BTreeSet::new();
    for node in nodes {
        let field = |name: &str| node[name].as_str().expect("a node field").to_string();
        live_nodes.insert((field("kind"), field("id"), field("label")));
        let kind: NodeKind = serde_json::from_value(node["kind"].clone()).expect("a node kind");
        if kind != NodeKind::Transformation {
            continue;
        }
        let label = field("label");
        let (status, _, body) = http_get(
            port,
            &format!("/api/v1/models/{}", percent_encode(&label)),
            "",
        );
        if listed.contains(&label) {
            assert!(
                status.contains("200"),
                "{label:?} is in the model list, so its detail must serve: {status}: {body}"
            );
        } else {
            assert!(
                status.contains("404"),
                "{label:?} is not in the model list, so its detail must 404: {status}: {body}"
            );
        }
        drawn.insert(label);
    }

    // Both branches above ran, and the split is the one #2011 describes.
    assert!(
        drawn.contains("weekly_revenue") && !listed.contains("weekly_revenue"),
        "the DAG no longer draws a model the compile lacks; drawn {drawn:?}, listed {listed:?}"
    );
    assert!(
        listed.is_subset(&drawn),
        "every compiled model is a DAG node; drawn {drawn:?}, listed {listed:?}"
    );

    // The pair the SPA's tests read must still describe this server.
    assert_eq!(
        live_nodes,
        nodes_in_ui_fixture("dag-two-pipelines.json"),
        "the live DAG and engine/ui/src/test/fixtures/dag-two-pipelines.json disagree \
         about their nodes (kind, id, label); recapture it (its README says how)"
    );
    assert_eq!(
        listed,
        models_in_ui_fixture("model-list-two-pipelines.json"),
        "the live model list and engine/ui/src/test/fixtures/model-list-two-pipelines.json \
         disagree; recapture it (its README says how)"
    );
    // The SPA reads a list whose `count` disagrees with its entries as unknown,
    // and then offers every model. So the capture's `count` is compared too.
    let fixture = ui_fixture("model-list-two-pipelines.json");
    assert_eq!(
        fixture["count"], list["count"],
        "the captured model list's count disagrees with the live one; recapture it"
    );
}

/// Start `rocky serve` on a free loopback port, and return once the project
/// has compiled.
///
/// No `--ui` and no `--token`: a loopback server with no token needs no auth,
/// and the page these tests never ask for is what needs the embed.
fn serve_api(root: &std::path::Path, config: &std::path::Path) -> (Server, u16) {
    let port = TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port();
    let child = rocky()
        .current_dir(root)
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
    wait_for_health(port);
    // `/health` and `/dag` both answer before the project has compiled:
    // `health` is an unconditional handler and `full_dag` gates only on
    // `config_path`, which is set at construction. The model routes gate on
    // `compile_result`, and `serve` merely sleeps 100ms for the compile it
    // spawned. So without this latch a slow compile answers the first model
    // request `503 engine_not_ready`, which is neither the 200 nor the 404
    // the tests assert, and every assertion fails on timing alone.
    wait_for_compiled_models(port);
    (server, port)
}

/// Every node in a capture the SPA's tests read, as (kind, id, label).
///
/// The fixtures are recorded by hand from a real `rocky serve` — no script
/// regenerates them — so nothing but this comparison keeps them honest.
fn nodes_in_ui_fixture(file: &str) -> std::collections::BTreeSet<(String, String, String)> {
    let dag = ui_fixture(file);
    dag["nodes"]
        .as_array()
        .expect("the fixture has nodes")
        .iter()
        .map(|n| {
            let field = |name: &str| {
                n[name]
                    .as_str()
                    .unwrap_or_else(|| panic!("every fixture node has a {name}"))
                    .to_string()
            };
            (field("kind"), field("id"), field("label"))
        })
        .collect()
}

/// Every model name in a captured `GET /api/v1/models` the SPA's tests read.
fn models_in_ui_fixture(file: &str) -> std::collections::BTreeSet<String> {
    ui_fixture(file)["models"]
        .as_array()
        .expect("the fixture has models")
        .iter()
        .map(|m| {
            m["name"]
                .as_str()
                .expect("every fixture model has a name")
                .to_string()
        })
        .collect()
}

/// One JSON capture from `engine/ui/src/test/fixtures/`.
fn ui_fixture(file: &str) -> serde_json::Value {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../ui/src/test/fixtures")
        .join(file);
    let raw =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_str(&raw).expect("the fixture is JSON")
}

/// Grow `rocky playground` into a project whose DAG draws a model the server
/// does not compile: a second transformation pipeline, its one model kept in
/// `reporting/` rather than `models/`.
fn add_a_second_models_directory(root: &std::path::Path, config: &std::path::Path) {
    let mut toml = std::fs::OpenOptions::new()
        .append(true)
        .open(config)
        .expect("open rocky.toml");
    toml.write_all(
        br#"
[pipeline.reporting]
type = "transformation"
models = "reporting/**"

[pipeline.reporting.target.governance]
auto_create_schemas = true
"#,
    )
    .expect("append a pipeline");

    let reporting = root.join("reporting");
    std::fs::create_dir_all(&reporting).expect("reporting dir");
    std::fs::write(
        reporting.join("weekly_revenue.sql"),
        "SELECT 1 AS week, 2 AS revenue\n",
    )
    .expect("write the model");
    std::fs::write(
        reporting.join("weekly_revenue.toml"),
        r#"name = "weekly_revenue"

[strategy]
type = "full_refresh"

[target]
catalog = "playground"
schema = "main"
table = "weekly_revenue"
"#,
    )
    .expect("write the sidecar");
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

/// `--open` hands the system opener EXACTLY the address the server printed,
/// and an opener that exits non-zero leaves the server serving. The real
/// binary, a real page, and a fake `open` / `xdg-open` first on `PATH` that
/// records the one argument it is given. Same skip rule as the page test: it
/// needs `engine/ui/dist` in the binary.
///
/// What it does not pin: that the opener runs only AFTER the bind. That
/// ordering is a race a test cannot observe from outside without flaking,
/// and it is pinned in-process by `the_opener_receives_the_printed_address_only_after_ready`
/// on the readiness latch.
#[cfg(unix)]
#[test]
fn open_hands_the_opener_the_printed_address_and_survives_an_opener_that_fails() {
    use std::os::unix::fs::PermissionsExt;

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

    // A fake opener under both names the binary may call on unix. It appends
    // its first argument to `record` and exits with `code`.
    let bin = dir.path().join("bin");
    std::fs::create_dir_all(&bin).expect("bin dir");
    let install_fake = |record: &std::path::Path, code: i32| {
        for name in ["open", "xdg-open"] {
            let path = bin.join(name);
            std::fs::write(
                &path,
                format!(
                    "#!/bin/sh\nprintf '%s\\n' \"$1\" >> '{}'\nexit {code}\n",
                    record.display()
                ),
            )
            .expect("write the fake opener");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("make the fake opener executable");
        }
    };
    let path_env = format!(
        "{}:{}",
        bin.display(),
        std::env::var("PATH").unwrap_or_default()
    );

    let serve_with_open = |port: u16| -> (Server, String) {
        let mut child = rocky()
            .current_dir(&root)
            .env("PATH", &path_env)
            .args([
                "--config",
                config.to_str().unwrap(),
                "serve",
                "--ui",
                "--open",
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
            .expect("spawn rocky serve --ui --open");
        let stdout = child.stdout.take().expect("piped stdout");
        let mut first_line = String::new();
        BufReader::new(stdout)
            .read_line(&mut first_line)
            .expect("read the banner");
        (Server(child), first_line.trim().to_string())
    };
    let free_port = || {
        TcpListener::bind("127.0.0.1:0")
            .expect("bind")
            .local_addr()
            .expect("addr")
            .port()
    };
    let recorded = |record: &std::path::Path| -> Vec<String> {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            if let Ok(text) = std::fs::read_to_string(record)
                && !text.trim().is_empty()
            {
                return text.lines().map(str::to_string).collect();
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the opener was never invoked"
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    // An opener that succeeds: it receives the printed address, exactly.
    let record_ok = dir.path().join("opened-ok.txt");
    install_fake(&record_ok, 0);
    let port = free_port();
    let (server, printed) = serve_with_open(port);
    let address = printed
        .strip_prefix("Rocky UI: ")
        .unwrap_or_else(|| panic!("the banner is the address line: {printed}"))
        .to_string();
    assert_eq!(address, format!("http://127.0.0.1:{port}/ui/#token=s3cret"));
    wait_for_health(port);
    assert_eq!(
        recorded(&record_ok),
        vec![address.clone()],
        "the opener must receive the printed address, once"
    );
    drop(server);

    // An opener that exits 3 ("no application"): invoked with the same
    // address, and the server still serves and still printed it.
    let record_fail = dir.path().join("opened-fail.txt");
    install_fake(&record_fail, 3);
    let port = free_port();
    let (server, printed) = serve_with_open(port);
    assert_eq!(
        printed,
        format!("Rocky UI: http://127.0.0.1:{port}/ui/#token=s3cret")
    );
    wait_for_health(port);
    assert_eq!(recorded(&record_fail).len(), 1);
    let (status, _, _) = http_get(port, "/api/v1/health", "");
    assert!(
        status.contains("200"),
        "a failing opener must not take the server down: {status}"
    );
    drop(server);
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
