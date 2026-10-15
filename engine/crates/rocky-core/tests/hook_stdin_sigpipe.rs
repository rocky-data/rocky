//! A hook that exits without reading its stdin must not kill the process
//! that fires it (#2360).
//!
//! The `rocky` CLI keeps SIGPIPE at its default disposition, which kills the
//! process on a write to a closed pipe. This test binary does the same, so it
//! dies if the hook context still goes through a pipe. The context is larger
//! than any pipe buffer, so a pipe write blocks until the hook exits and then
//! fails every time, not only when the hook wins a race. This file holds one
//! test so the SIGPIPE change cannot reach another test.

#![cfg(unix)]

use std::collections::HashMap;

use rocky_core::hooks::{
    FailureAction, HookConfig, HookConfigOrList, HookContext, HookRegistry, HookResult, HooksConfig,
};

#[tokio::test]
async fn a_hook_that_ignores_its_stdin_does_not_kill_the_caller() {
    // SAFETY: `signal(2)` is async-signal-safe. This binary holds this one
    // test, and nothing else here touches the SIGPIPE disposition.
    unsafe {
        libc::signal(libc::SIGPIPE, libc::SIG_DFL);
    }

    let mut hooks = HashMap::new();
    hooks.insert(
        "on_pipeline_error".to_string(),
        HookConfigOrList::Single(HookConfig {
            command: "true".to_string(),
            timeout_ms: 10_000,
            on_failure: FailureAction::Abort,
            env: HashMap::new(),
        }),
    );
    let registry = HookRegistry::from_config(&HooksConfig {
        hooks,
        webhooks: HashMap::new(),
    });

    let big_error = "x".repeat(1 << 20);
    let ctx = HookContext::pipeline_error("run-1", "probe", &big_error);
    let result = registry.fire(&ctx).await.expect("fire must return");
    assert_eq!(result, HookResult::Continue);
}
