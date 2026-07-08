//! Regression: `rocky audit --scorecard --window <span>` must treat an
//! i64-parseable but out-of-range magnitude (a fat-fingered `100000000d` ≈
//! 273k years) as a clean usage error, NOT a panic. An earlier implementation
//! fed the magnitude straight into `chrono::Duration::days` / a `DateTime`
//! subtraction, both of which panic on overflow — crashing the process
//! (exit 101 / SIGABRT) instead of surfacing the documented usage error.
//!
//! Asserts at the process level so a future regression that reintroduces the
//! panic is caught by the exit code, not just the in-process unit test.

use std::process::Command;

fn rocky() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
}

/// An out-of-range window exits with the normal usage-error code (1), carries
/// the usage message, and never panics.
#[test]
fn window_out_of_range_is_clean_usage_error_not_panic() {
    for span in ["100000000d", "200000000000d", "100000000000h"] {
        let out = rocky()
            .args(["-o", "json", "audit", "--scorecard", "--window", span])
            .output()
            .expect("spawn rocky");

        // Exit code 1 (anyhow usage error) — explicitly NOT 101 (Rust panic)
        // and NOT a signal termination (code() == None, e.g. SIGABRT/134).
        assert_eq!(
            out.status.code(),
            Some(1),
            "`--window {span}` must exit 1 (usage error), got {:?}",
            out.status
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(
            stderr.contains("invalid --window"),
            "`--window {span}` must print the usage error, got stderr: {stderr}"
        );
        assert!(
            !stderr.contains("panicked"),
            "`--window {span}` must not panic, got stderr: {stderr}"
        );
    }
}

/// A well-formed window is accepted and the command exits 0 (an absent state
/// store renders an empty scorecard, it does not error).
#[test]
fn window_valid_is_accepted() {
    let dir = tempfile::tempdir().expect("tempdir");
    let state = dir.path().join("nonexistent-state.redb");
    let out = rocky()
        .args([
            "-o",
            "json",
            "--state-path",
            state.to_str().unwrap(),
            "audit",
            "--scorecard",
            "--window",
            "30d",
        ])
        .output()
        .expect("spawn rocky");
    assert!(
        out.status.success(),
        "a valid `--window 30d` must exit 0, got {:?}; stderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(
        stdout.contains("\"window\"") && stdout.contains("30d"),
        "expected the window echoed in the JSON, got: {stdout}"
    );
}
