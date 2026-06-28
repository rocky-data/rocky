//! Regression: `rocky run --var` combined with `--dag` or `--watch` must error
//! loudly rather than silently drop the variable. Those two dispatch paths
//! compile their sub-runs with an empty `RunVars`, so a supplied `--var` would
//! otherwise be ignored — resolving an inline `@var(name, default)` to the
//! default and still reporting success (wrong data), or failing a required var
//! that was in fact supplied. The guard fires right after `--var` is parsed,
//! before any config/state load, so these need no project on disk.

use std::process::Command;

fn rocky() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rocky"))
}

#[test]
fn var_with_dag_is_rejected() {
    let out = rocky()
        .args(["run", "--dag", "--var", "x=y"])
        .output()
        .expect("spawn rocky");
    assert!(
        !out.status.success(),
        "`run --dag --var` must exit non-zero, got {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--var is not yet supported with --dag"),
        "expected the --var/--dag guard message, got stderr: {stderr}"
    );
}

#[test]
fn var_with_watch_is_rejected() {
    let out = rocky()
        .args(["run", "--watch", "--var", "x=y"])
        .output()
        .expect("spawn rocky");
    assert!(
        !out.status.success(),
        "`run --watch --var` must exit non-zero, got {:?}",
        out.status
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("--var is not yet supported"),
        "expected the --var/--watch guard message, got stderr: {stderr}"
    );
}
