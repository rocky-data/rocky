//! `rocky apply` re-checks the models a plan fingerprinted, for every
//! principal, on the real binary.
//!
//! Apply does not replay stored SQL: `run` recompiles the models on disk. So
//! a plan applied after its models were edited would run the edit. A plan
//! that carries a models fingerprint now refuses that apply with
//! `plan_models_changed`, both from the CLI (a person: no
//! `ROCKY_PRINCIPAL`) and through `POST /api/v1/jobs/apply`. An unchanged
//! plan applies.
//!
//! The plan is a backfill: the one model-carrying plan `rocky` writes for a
//! transformation project from the CLI. It is review-gated, so the test
//! approves it first, with a pinned git identity.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

const CODE: &str = "plan_models_changed";

fn rocky(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .env("GIT_CONFIG_GLOBAL", dir.join("gitconfig"))
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .env("HOME", dir)
        .env_remove("ROCKY_PRINCIPAL")
        .env_remove("ROCKY_SESSION_SOURCE")
        .env_remove("ROCKY_SERVE_TOKEN")
        .env_remove("ROCKY_SERVE_TOKEN_SCOPE");
    command
}

fn ok(out: &Output) {
    assert!(
        out.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A playground project with one approved backfill plan.
fn project_with_an_approved_backfill(dir: &Path) -> (PathBuf, String) {
    std::fs::write(
        dir.join("gitconfig"),
        "[user]\n\temail = operator@example.com\n\tname = Operator\n",
    )
    .unwrap();
    let root = dir.join("project");
    ok(&rocky(dir)
        .args(["playground", root.to_str().unwrap()])
        .output()
        .unwrap());
    let state = dir.join("state.redb");
    // Build every model once, so the backfill's upstreams exist.
    ok(&rocky(dir)
        .current_dir(&root)
        .args(["--state-path", state.to_str().unwrap(), "run"])
        .output()
        .unwrap());
    let out = rocky(dir)
        .current_dir(&root)
        .args(["-o", "json", "--state-path", state.to_str().unwrap()])
        .args(["backfill", "--model", "revenue_summary"])
        .output()
        .unwrap();
    ok(&out);
    let stdout = String::from_utf8_lossy(&out.stdout);
    let json_start = stdout.find("\n{").map_or(0, |i| i + 1);
    let plan: serde_json::Value = serde_json::from_str(&stdout[json_start..]).unwrap();
    let plan_id = plan["plan_id"].as_str().expect("a plan id").to_string();
    ok(&rocky(dir)
        .current_dir(&root)
        .args(["--state-path", state.to_str().unwrap()])
        .args(["review", &plan_id, "--approve"])
        .output()
        .unwrap());
    (root, plan_id)
}

fn cli_apply(dir: &Path, root: &Path, plan_id: &str) -> Output {
    rocky(dir)
        .current_dir(root)
        .args([
            "-o",
            "json",
            "--state-path",
            dir.join("state.redb").to_str().unwrap(),
        ])
        .args(["apply", plan_id])
        .output()
        .unwrap()
}

/// Append a comment to a model the backfill rebuilds; return the original.
fn edit_model(root: &Path) -> (PathBuf, String) {
    let path = root.join("models").join("revenue_summary.sql");
    let original = std::fs::read_to_string(&path).unwrap();
    std::fs::write(&path, format!("{original}\n-- edited after the plan\n")).unwrap();
    (path, original)
}

#[test]
fn cli_apply_refuses_a_plan_whose_models_changed_and_applies_an_unchanged_one() {
    let dir = tempfile::tempdir().unwrap();
    let (root, plan_id) = project_with_an_approved_backfill(dir.path());

    let (path, original) = edit_model(&root);
    let out = cli_apply(dir.path(), &root, &plan_id);
    assert!(!out.status.success(), "an edited plan must not apply");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains(CODE), "{stderr}");
    assert!(
        stderr.contains("models changed since this plan was made; plan again"),
        "{stderr}"
    );

    std::fs::write(&path, original).unwrap();
    ok(&cli_apply(dir.path(), &root, &plan_id));
}

/// A child killed when the test ends, pass or fail.
struct Server(Child);

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn http(port: u16, method: &str, path: &str, body: &str) -> (String, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    write!(
        stream,
        "{method} {path} HTTP/1.0\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\n\
         Content-Length: {}\r\n\r\n{body}",
        body.len()
    )
    .unwrap();
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).unwrap();
    let text = String::from_utf8(raw).unwrap();
    let (head, body) = text.split_once("\r\n\r\n").unwrap();
    (
        head.lines().next().unwrap_or("").to_string(),
        body.to_string(),
    )
}

/// `rocky serve` on loopback, no token, started from above the project.
fn serve(dir: &Path, root: &Path) -> (Server, u16) {
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let server = Server(
        rocky(dir)
            .current_dir(dir)
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
            .unwrap(),
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

fn apply_job(port: u16, plan_id: &str) -> serde_json::Value {
    // No retry on `409 mutation_in_progress`: the second apply is submitted
    // the moment the first one reads terminal, which is what a person
    // clicking Apply again does. The permit must be free by then.
    let (status, body) = http(
        port,
        "POST",
        "/api/v1/jobs/apply",
        &format!(r#"{{"plan_id":"{plan_id}"}}"#),
    );
    assert!(status.contains("202"), "{status}: {body}");
    let job_id = serde_json::from_str::<serde_json::Value>(&body).unwrap()["job_id"]
        .as_str()
        .unwrap()
        .to_string();
    let deadline = Instant::now() + Duration::from_secs(180);
    loop {
        let (status, body) = http(port, "GET", &format!("/api/v1/jobs/{job_id}"), "");
        assert!(status.contains("200"), "{status}: {body}");
        let job: serde_json::Value = serde_json::from_str(&body).unwrap();
        if job["state"] != "running" && job["state"] != "queued" {
            return job;
        }
        assert!(Instant::now() < deadline, "the apply job never finished");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn http_apply_job_refuses_a_plan_whose_models_changed_and_applies_an_unchanged_one() {
    let dir = tempfile::tempdir().unwrap();
    let (root, plan_id) = project_with_an_approved_backfill(dir.path());
    let (_server, port) = serve(dir.path(), &root);

    let (path, original) = edit_model(&root);
    let job = apply_job(port, &plan_id);
    assert_eq!(job["state"], "failed", "{job}");
    let error = job["error"].as_str().unwrap_or_default();
    assert!(error.contains(CODE), "{job}");

    std::fs::write(&path, original).unwrap();
    let job = apply_job(port, &plan_id);
    assert_eq!(job["state"], "succeeded", "{job}");
}
