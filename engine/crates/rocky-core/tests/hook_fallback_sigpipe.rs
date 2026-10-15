//! With no writable temp directory, a hook gets its context through a pipe
//! (#2374). These tests send a context larger than the pipe buffer.
//!
//! This file is its own test binary: it sets SIGPIPE to the default action,
//! as `rocky`'s `main` does, and changes `TMPDIR` for the whole process.
//! The setup runs once, before either test spawns a hook.

#![cfg(unix)]

use std::collections::HashMap;

use rocky_core::hooks::{
    FailureAction, HookConfig, HookConfigOrList, HookContext, HookRegistry, HookResult, HooksConfig,
};

/// Fire `on_pipeline_start` for `command` with a 1 MiB context. 1 MiB is
/// larger than any pipe buffer, so the write cannot finish unless the hook
/// reads it.
async fn fire_with_large_context(command: &str, timeout_ms: u64) -> HookResult {
    static SETUP: std::sync::Once = std::sync::Once::new();
    SETUP.call_once(|| {
        let out_dir =
            std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("hook_fallback_sigpipe");
        std::fs::create_dir_all(&out_dir).unwrap();
        // SAFETY: `Once` runs this before any test in this binary reads the
        // environment or spawns a hook; a second test blocks until it ends.
        unsafe {
            std::env::set_var("TMPDIR", out_dir.join("does-not-exist"));
            libc::signal(libc::SIGPIPE, libc::SIG_DFL);
        }
    });

    let mut hooks = HashMap::new();
    hooks.insert(
        "on_pipeline_start".to_string(),
        HookConfigOrList::Single(HookConfig {
            command: command.to_string(),
            timeout_ms,
            on_failure: FailureAction::Abort,
            env: HashMap::new(),
        }),
    );
    let registry = HookRegistry::from_config(&HooksConfig {
        hooks,
        webhooks: HashMap::new(),
    });
    let pipeline = "p".repeat(1 << 20);
    let ctx = HookContext::pipeline_start("run-1", &pipeline);
    registry.fire(&ctx).await.expect("fire must return")
}

/// A hook that exits without reading must not kill Rocky with SIGPIPE.
#[tokio::test]
async fn a_hook_that_ignores_a_large_piped_context_does_not_kill_rocky() {
    assert_eq!(
        fire_with_large_context("exit 0", 10_000).await,
        HookResult::Continue
    );
}

/// A hook that neither reads nor exits must not hang the write: it times
/// out within the hook's `timeout_ms`.
#[tokio::test]
async fn a_hook_that_never_reads_times_out_instead_of_hanging() {
    let started = std::time::Instant::now();
    let result = fire_with_large_context("sleep 30", 500).await;
    assert!(
        started.elapsed() < std::time::Duration::from_secs(10),
        "the write hung for {:?}",
        started.elapsed()
    );
    match result {
        HookResult::Abort { reason } => assert!(reason.contains("timed out"), "{reason}"),
        other => panic!("expected a timeout abort, got {other:?}"),
    }
}
