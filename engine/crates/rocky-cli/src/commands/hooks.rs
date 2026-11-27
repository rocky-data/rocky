use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::config::load_rocky_config;
use rocky_core::hooks::{HookContext, HookEvent, HookRegistry, HookResult};

use crate::output::{HookEntry, HooksListOutput, HooksTestOutput, WebhookEntry};

/// List all configured hooks from the pipeline config.
pub fn run_hooks_list(config_path: &Path, json: bool) -> Result<()> {
    let config = load_rocky_config(config_path)
        .with_context(|| format!("failed to load config from {}", config_path.display()))?;

    let registry = HookRegistry::from_config(&config.hooks);

    if json {
        let output = hooks_list_output(&registry);
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
                        rocky_core::secret_registry::render_placeholders(&hook.command),
                        hook.timeout_ms,
                        hook.on_failure
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
            // The abort reason quotes the hook command or webhook URL (#1919).
            result: Some(rocky_core::secret_registry::render_placeholders(&format!(
                "{result:?}"
            ))),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        match &result {
            HookResult::Continue => println!("All hooks passed."),
            HookResult::Abort { reason } => {
                println!(
                    "Hook aborted: {}",
                    rocky_core::secret_registry::render_placeholders(reason)
                );
            }
        }
        if async_summary.total > 0 {
            println!(
                "Async webhooks: {}/{} succeeded ({} failed)",
                async_summary.succeeded, async_summary.total, async_summary.failed,
            );
            for (url, err) in &async_summary.failures {
                // `url` is an `EnvString`: `Display` prints `${NAME}` (#2383).
                println!(
                    "  ! {url} — {}",
                    rocky_core::secret_registry::render_placeholders(err)
                );
            }
        }
    }

    Ok(())
}

/// The `rocky hooks list --output json` payload. Pure: reads the registry
/// only, runs no hook and calls no endpoint.
pub(crate) fn hooks_list_output(registry: &HookRegistry) -> HooksListOutput {
    let hooks = hook_entries(registry);
    HooksListOutput {
        total: hooks.len(),
        hooks,
        webhooks: webhook_entries(registry),
    }
}

/// The `rocky hooks list --output json` rows.
fn hook_entries(registry: &HookRegistry) -> Vec<HookEntry> {
    let mut entries: Vec<HookEntry> = Vec::new();
    for event in HookEvent::all() {
        for hook in registry.hooks_for(event) {
            entries.push(HookEntry {
                event: event.config_key().to_string(),
                // A resolved `${VAR}` value prints as `${NAME}` (#1919).
                command: rocky_core::secret_registry::render_placeholders(&hook.command),
                timeout_ms: hook.timeout_ms,
                on_failure: format!("{:?}", hook.on_failure),
                env_keys: hook.env.keys().cloned().collect(),
            });
        }
    }
    entries
}

/// The configured webhooks, in event order, each with a redacted URL.
fn webhook_entries(registry: &HookRegistry) -> Vec<WebhookEntry> {
    let mut entries: Vec<WebhookEntry> = Vec::new();
    for event in HookEvent::all() {
        for webhook in registry.webhooks_for(event) {
            let mut header_keys: Vec<String> = webhook.headers.keys().cloned().collect();
            header_keys.sort();
            entries.push(WebhookEntry {
                event: event.config_key().to_string(),
                // `Display` prints a resolved `${VAR}` value as `${NAME}`.
                url: redact_webhook_url(&webhook.url.to_string()),
                method: webhook.method.clone(),
                timeout_ms: webhook.timeout_ms,
                on_failure: format!("{:?}", webhook.on_failure),
                async_mode: webhook.async_mode,
                retry_count: webhook.retry_count,
                header_keys,
                preset: webhook.preset.clone(),
            });
        }
    }
    entries
}

/// Cut a webhook URL to `scheme://host`.
///
/// Drops credentials (`user:pass@`), port, path, query and fragment: a
/// webhook URL often carries its token in one of them. Anything without a
/// `scheme://` prefix prints as `<redacted>`, because it cannot be told
/// apart from a bare token.
fn redact_webhook_url(url: &str) -> String {
    const REDACTED: &str = "<redacted>";
    let Some((scheme, rest)) = url.split_once("://") else {
        return REDACTED.to_string();
    };
    let scheme_ok = !scheme.is_empty()
        && scheme
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
    if !scheme_ok {
        return REDACTED.to_string();
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = if host_port.starts_with('[') {
        let inner = host_port.split_once(']').map_or(host_port, |(h, _)| h);
        format!("{inner}]")
    } else {
        host_port.split(':').next().unwrap_or_default().to_string()
    };
    if host.is_empty() {
        return REDACTED.to_string();
    }
    format!("{scheme}://{host}")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// #1919: a hook command expanded from `${VAR}` prints as `${NAME}` in
    /// `rocky hooks list --output json`.
    #[test]
    fn hooks_list_prints_a_resolved_command_as_its_placeholder() {
        const SECRET: &str = "rocky-1919-hook-token-8f8f";
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(
            &path,
            "[adapter]\ntype = \"duckdb\"\npath = \":memory:\"\n\n\
             [[hook.on_pipeline_start]]\ncommand = \"notify.sh --token ${ROCKY_T1919_HOOK}\"\n",
        )
        .unwrap();
        // SAFETY: test-only; the variable name is unique to this test.
        unsafe { std::env::set_var("ROCKY_T1919_HOOK", SECRET) };
        let config = load_rocky_config(&path);
        // SAFETY: as above.
        unsafe { std::env::remove_var("ROCKY_T1919_HOOK") };
        let config = config.expect("config loads");
        let entries = hook_entries(&HookRegistry::from_config(&config.hooks));
        assert_eq!(entries.len(), 1, "PRECONDITION: the hook loaded");
        let json = serde_json::to_string(&entries).unwrap();
        assert!(!json.contains(SECRET), "leaked: {json}");
        assert!(json.contains("--token ${ROCKY_T1919_HOOK}"), "{json}");
    }

    #[test]
    fn webhook_urls_are_cut_to_scheme_and_host() {
        for (url, want) in [
            (
                "https://hooks.slack.com/services/T0/B0/TOKEN",
                "https://hooks.slack.com",
            ),
            (
                "http://user:pw@host.example:8080/p?q=1#f",
                "http://host.example",
            ),
            ("https://host.example?key=secret", "https://host.example"),
            ("https://[::1]:9000/x", "https://[::1]"),
            (
                "https://${WEBHOOK_HOST}/hook/${TOKEN}",
                "https://${WEBHOOK_HOST}",
            ),
            ("${WEBHOOK_URL}", "<redacted>"),
            ("not a url with a token", "<redacted>"),
            ("://host/x", "<redacted>"),
            ("https:///path-only", "<redacted>"),
        ] {
            assert_eq!(redact_webhook_url(url), want, "{url}");
        }
    }
}
