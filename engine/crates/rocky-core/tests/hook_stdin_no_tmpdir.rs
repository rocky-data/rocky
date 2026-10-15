//! A hook still runs, and still gets its context, when the temp directory
//! cannot be written (a read-only container root). The context then goes
//! through a pipe. This file holds one test because it changes `TMPDIR` for
//! the whole process.

#![cfg(unix)]

use std::collections::HashMap;

use rocky_core::hooks::{
    FailureAction, HookConfig, HookConfigOrList, HookContext, HookRegistry, HookResult, HooksConfig,
};

#[tokio::test]
async fn a_hook_gets_its_context_without_a_temp_directory() {
    let out_dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("hook_stdin_no_tmpdir");
    std::fs::create_dir_all(&out_dir).unwrap();
    let seen = out_dir.join("seen.json");
    let _ = std::fs::remove_file(&seen);

    // SAFETY: this binary holds this one test, and the change happens before
    // the hook runs. Nothing else reads the environment at the same time.
    unsafe {
        std::env::set_var("TMPDIR", out_dir.join("does-not-exist"));
    }

    let mut hooks = HashMap::new();
    hooks.insert(
        "on_pipeline_start".to_string(),
        HookConfigOrList::Single(HookConfig {
            command: format!("cat > '{}'", seen.display()),
            timeout_ms: 10_000,
            on_failure: FailureAction::Abort,
            env: HashMap::new(),
        }),
    );
    let registry = HookRegistry::from_config(&HooksConfig {
        hooks,
        webhooks: HashMap::new(),
    });

    let ctx = HookContext::pipeline_start("run-1", "probe");
    let result = registry.fire(&ctx).await.expect("fire must return");
    assert_eq!(result, HookResult::Continue);
    assert_eq!(
        std::fs::read_to_string(&seen).unwrap(),
        serde_json::to_string_pretty(&ctx).unwrap()
    );
}
