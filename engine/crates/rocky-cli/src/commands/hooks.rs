use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::config::load_rocky_config;
use rocky_core::hooks::{HookContext, HookEvent, HookRegistry, HookResult};

use crate::output::{HookEntry, HooksListOutput, HooksTestOutput};

/// List all configured hooks from the pipeline config.
pub fn run_hooks_list(config_path: &Path, json: bool) -> Result<()> {
    let config = load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    let registry = HookRegistry::from_config(&config.hooks);

    if json {
        let mut entries: Vec<HookEntry> = Vec::new();
        for event in HookEvent::all() {
            let hooks = registry.hooks_for(event);
            for hook in hooks {
                entries.push(HookEntry {
                    event: event.config_key().to_string(),
                    command: hook.command.clone(),
                    timeout_ms: hook.timeout_ms,
                    on_failure: format!("{:?}", hook.on_failure),
                    env_keys: hook.env.keys().cloned().collect(),
                });
            }
        }
        let output = HooksListOutput {
            total: entries.len(),
            hooks: entries,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        let total = registry.total_hook_count();
        if total == 0 {
            println!("No hooks configured.");
            println!("\nAdd hooks to your rocky.toml:");
            println!("  [hook.on_pipeline_start]");
            println!("  command = \"scripts/notify.sh\"");
            return Ok(());
        }

        println!("Configured hooks ({total} total):\n");
        for event in HookEvent::all() {
            let hooks = registry.hooks_for(event);
            if !hooks.is_empty() {
                println!("  {}:", event.config_key());
                for hook in hooks {
                    println!(
                        "    - {} (timeout: {}ms, on_failure: {:?})",
                        hook.command, hook.timeout_ms, hook.on_failure
                    );
                }
            }
        }
    }

    Ok(())
}

/// Fire a test event to validate hook configuration.
pub async fn run_hooks_test(config_path: &Path, event_name: &str, json: bool) -> Result<()> {
    let config = load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    let event = HookEvent::from_config_key(event_name)
        .with_context(|| format!("unknown hook event: {event_name}"))?;

    let registry = HookRegistry::from_config(&config.hooks);

    if !registry.has_hooks(&event) {
        if json {
            let output = HooksTestOutput {
                event: event_name.to_string(),
                status: "no_hooks".to_string(),
                message: Some(format!("No hooks configured for {event_name}")),
                result: None,
            };
            println!("{}", serde_json::to_string_pretty(&output)?);
        } else {
            println!("No hooks configured for {event_name}.");
        }
        return Ok(());
    }

    // Determine pipeline name from config (use first pipeline or "test")
    let pipeline_name = config
        .pipelines
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "test".to_string());

    let ctx = HookContext::synthetic(event, &pipeline_name);

    if !json {
        println!("Firing test event: {event_name}");
        println!("Context JSON:\n{}\n", serde_json::to_string_pretty(&ctx)?);
    }

    info!(event = %event_name, "firing test hook");
    let result = registry.fire(&ctx).await.map_err(|e| anyhow::anyhow!(e))?;

    // Drain any async webhooks so fire-and-forget deliveries complete before
    // the command exits — otherwise the test would report success while the
    // spawned task is silently dropped at runtime shutdown.
    let async_summary = registry.wait_async_webhooks().await;

    if json {
        let status = match &result {
            HookResult::Continue => "continue",
            HookResult::Abort { .. } => "abort",
        };
        let output = HooksTestOutput {
            event: event_name.to_string(),
            status: status.to_string(),
            message: None,
            result: Some(format!("{result:?}")),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        match &result {
            HookResult::Continue => println!("All hooks passed."),
            HookResult::Abort { reason } => println!("Hook aborted: {reason}"),
        }
        if async_summary.total > 0 {
            println!(
                "Async webhooks: {}/{} succeeded ({} failed)",
                async_summary.succeeded, async_summary.total, async_summary.failed,
            );
            for (url, err) in &async_summary.failures {
                println!("  ! {url} — {err}");
            }
        }
    }

    Ok(())
}
