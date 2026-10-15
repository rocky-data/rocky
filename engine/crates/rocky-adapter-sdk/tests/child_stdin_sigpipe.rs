//! A write to a dead child must return `BrokenPipe`, not kill Rocky (#2370).
//!
//! This file is its own test binary because it sets SIGPIPE to the default
//! action for the whole process, as `rocky`'s `main` does. Under that
//! action, a write from an ordinary thread kills the process with signal 13.

#![cfg(unix)]

use std::io;

use rocky_adapter_sdk::child_stdin::ChildStdinWriter;
use rocky_adapter_sdk::process::ProcessAdapter;

fn sigpipe_default() {
    // SAFETY: sets the action the `rocky` binary uses. Every test in this
    // binary wants it.
    unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
}

/// The child exits at once without reading. A 1 MiB write is larger than
/// any pipe buffer, so it must reach the closed pipe.
#[tokio::test]
async fn a_write_to_a_dead_child_returns_broken_pipe() {
    sigpipe_default();
    let mut child = tokio::process::Command::new("/bin/sh")
        .args(["-c", "exit 0"])
        .stdin(std::process::Stdio::piped())
        .spawn()
        .expect("spawn sh");
    let writer = ChildStdinWriter::new(child.stdin.take().unwrap()).unwrap();
    child.wait().await.unwrap();

    // Twice: the second write runs with a SIGPIPE already pending.
    for _ in 0..2 {
        let err = writer
            .write_all(vec![b'x'; 1 << 20])
            .await
            .expect_err("a write to a dead child must fail");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe, "{err}");
    }
}

/// The adapter answers `initialize`, then exits. The next call must return
/// an error, not kill the process.
#[tokio::test]
async fn a_call_to_an_adapter_that_exited_returns_an_error() {
    sigpipe_default();
    let script = r#"
import json, sys
req = json.loads(sys.stdin.readline())
manifest = {
    "name": "dies", "version": "0.0.0", "sdk_version": "0.0.0",
    "dialect": "dies",
    "capabilities": {
        "warehouse": True, "discovery": False, "governance": False,
        "batch_checks": False, "create_catalog": False, "create_schema": False,
        "merge": False, "tablesample": False, "file_load": False,
    },
    "auth_methods": [], "config_schema": {},
}
print(json.dumps({"jsonrpc": "2.0", "id": req["id"],
                  "result": {"manifest": manifest}}), flush=True)
"#;
    let adapter =
        match ProcessAdapter::spawn("python3", &["-c", script], &serde_json::json!({})).await {
            Ok(adapter) => adapter,
            Err(e) => {
                eprintln!("skipping: spawn failed (python3 unavailable?): {e}");
                return;
            }
        };
    // Give the adapter time to exit after its one reply.
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;

    let sql = "x".repeat(1 << 20);
    let err = adapter
        .call("execute_statement", serde_json::json!({ "sql": sql }))
        .await
        .expect_err("a call to an exited adapter must fail");
    assert!(err.to_string().contains("adapter"), "{err}");
}
