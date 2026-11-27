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
    let webhooks = webhook_entries(registry);
    HooksListOutput {
        // Shell hooks and webhooks together, as `rocky hooks list` counts.
        total: hooks.len() + webhooks.len(),
        hooks,
        webhooks,
    }
}

/// The `rocky hooks list --output json` rows.
fn hook_entries(registry: &HookRegistry) -> Vec<HookEntry> {
    let mut entries: Vec<HookEntry> = Vec::new();
    for event in HookEvent::all() {
        for hook in registry.hooks_for(event) {
            entries.push(HookEntry {
                event: event.config_key().to_string(),
                // A resolved `${VAR}` value prints as `${NAME}` (#1919). A
                // literal URL is cut to scheme and host, and a bearer or
                // token value is withheld: a command often carries one.
                command: redact_command(&rocky_core::secret_registry::render_placeholders(
                    &hook.command,
                )),
                timeout_ms: hook.timeout_ms,
                on_failure: format!("{:?}", hook.on_failure),
                env_keys: {
                    let mut keys: Vec<String> = hook.env.keys().cloned().collect();
                    keys.sort();
                    keys
                },
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
/// webhook URL often carries its token in one of them. The host must be an
/// allow-list of host characters (or a `${NAME}` placeholder), so a URL with
/// a stray separator cannot carry text through as the host. Anything that
/// fails prints as `<redacted>`, because it cannot be told apart from a bare
/// token.
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
    let authority = rest.split(['/', '?', '#', '\\']).next().unwrap_or_default();
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = if let Some(inner) = host_port.strip_prefix('[') {
        let Some((inner, _)) = inner.split_once(']') else {
            return REDACTED.to_string();
        };
        if inner.is_empty()
            || !inner
                .chars()
                .all(|c| c.is_ascii_hexdigit() || c == ':' || c == '.')
        {
            return REDACTED.to_string();
        }
        format!("[{inner}]")
    } else {
        let host = host_port.split(':').next().unwrap_or_default();
        let host_ok = !host.is_empty()
            && host.chars().all(|c| {
                c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_' | '$' | '{' | '}')
            });
        if !host_ok {
            return REDACTED.to_string();
        }
        host.to_string()
    };
    format!("{scheme}://{host}")
}

/// Whether `value` is only a variable reference (`${NAME}` or `$NAME`): the
/// name is safe to show, the value never appears.
fn is_variable_reference(value: &str) -> bool {
    let name = value
        .strip_prefix("${")
        .and_then(|v| v.strip_suffix('}'))
        .or_else(|| value.strip_prefix('$'));
    name.is_some_and(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'))
}

/// Replace a secret-looking value with `<redacted>`, keeping any quotes
/// around it and a bare variable reference as it is.
fn redact_value(word: &str) -> String {
    let quotes = |c: char| matches!(c, '"' | '\'');
    let core = word.trim_matches(quotes);
    if core.is_empty() || is_variable_reference(core) {
        return word.to_string();
    }
    let lead = &word[..word.len() - word.trim_start_matches(quotes).len()];
    let trail = &word[word.trim_end_matches(quotes).len()..];
    format!("{lead}<redacted>{trail}")
}

/// Cut every URL inside `word` to `scheme://host`.
fn redact_urls_in(word: &str) -> String {
    let mut out = String::with_capacity(word.len());
    let mut rest = word;
    while let Some(at) = rest.find("://") {
        let before = &rest[..at];
        let scheme_start = before
            .rfind(|c: char| !(c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')))
            .map_or(0, |i| i + 1);
        let after = &rest[at + 3..];
        let end = after
            .find(|c: char| matches!(c, '"' | '\'' | '<' | '>' | ')' | '`') || c.is_whitespace())
            .unwrap_or(after.len());
        out.push_str(&before[..scheme_start]);
        out.push_str(&redact_webhook_url(&rest[scheme_start..at + 3 + end]));
        rest = &after[end..];
    }
    out.push_str(rest);
    out
}

/// A shell hook's command with the secrets it may carry withheld.
///
/// A literal URL is cut to scheme and host. The value after `Bearer`,
/// `--token` (or any `--*token` flag) or inside `token=` is replaced by
/// `<redacted>`. A bare `${NAME}` or `$NAME` stays: it shows the variable's
/// name, never its value.
fn redact_command(command: &str) -> String {
    let mut out = String::with_capacity(command.len());
    let mut redact_next = false;
    let mut rest = command;
    while !rest.is_empty() {
        let space_len = rest.len() - rest.trim_start().len();
        out.push_str(&rest[..space_len]);
        rest = &rest[space_len..];
        let word_len = rest.find(char::is_whitespace).unwrap_or(rest.len());
        let word = &rest[..word_len];
        rest = &rest[word_len..];
        if word.is_empty() {
            continue;
        }
        if redact_next {
            redact_next = false;
            out.push_str(&redact_value(word));
            continue;
        }
        let bare = word
            .trim_matches(|c| matches!(c, '"' | '\''))
            .to_ascii_lowercase();
        if bare == "bearer"
            || (bare.starts_with('-') && !bare.contains('=') && bare.ends_with("token"))
        {
            redact_next = true;
            out.push_str(word);
            continue;
        }
        if let Some(at) = word.to_ascii_lowercase().find("token=") {
            let (key, value) = word.split_at(at + "token=".len());
            out.push_str(key);
            out.push_str(&redact_value(value));
            continue;
        }
        out.push_str(&redact_urls_in(word));
    }
    out
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

    #[test]
    fn a_host_must_be_host_characters() {
        for url in [
            r"https://host\services\TOKEN",
            "https://host name/x",
            "https://ho st.example/x",
            "https://host;rm -rf/x",
            "https://[not-ipv6]/x",
            "https://[::1/x",
        ] {
            let got = redact_webhook_url(url);
            assert!(
                !got.contains("TOKEN") && !got.contains("rm -rf") && !got.contains("not-ipv6"),
                "{url} -> {got}"
            );
        }
        assert_eq!(
            redact_webhook_url(r"https://host\services\TOKEN"),
            "https://host"
        );
        assert_eq!(redact_webhook_url("https://host name/x"), "<redacted>");
    }

    #[test]
    fn a_command_loses_its_urls_and_tokens_but_keeps_variable_names() {
        for (command, leaked) in [
            (
                "curl -H \"Authorization: Bearer abc123SECRET\" https://api.example.com/v1/x?key=URLSECRET",
                ["abc123SECRET", "URLSECRET"],
            ),
            (
                "notify.sh --token tok-SECRET1 --url https://h.example/p/PATHSECRET",
                ["tok-SECRET1", "PATHSECRET"],
            ),
            (
                "notify.sh --api-token=EQSECRET2 --x https://u:PW@h.example/p",
                ["EQSECRET2", "PW@"],
            ),
            (
                "run.sh TOKEN=ENVSECRET3 'https://h.example/a/QUOTEDSECRET'",
                ["ENVSECRET3", "QUOTEDSECRET"],
            ),
        ] {
            let got = redact_command(command);
            for secret in leaked {
                assert!(!got.contains(secret), "{secret} leaked in {got}");
            }
            assert!(got.contains("<redacted>") || got.contains("://"), "{got}");
        }
        assert_eq!(
            redact_command("curl -sS https://api.example.com/v1/x?key=1 -d x"),
            "curl -sS https://api.example.com -d x"
        );
        // A variable reference shows its name, never a value.
        assert_eq!(
            redact_command("notify.sh --token ${ROCKY_T} --x=$HOME Bearer ${B} token=${T2}"),
            "notify.sh --token ${ROCKY_T} --x=$HOME Bearer ${B} token=${T2}"
        );
        assert_eq!(
            redact_command("echo  hi\tthere"),
            "echo  hi\tthere",
            "whitespace is kept"
        );
    }

    #[test]
    fn the_listed_hooks_are_stable_and_counted_with_their_webhooks() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(
            &path,
            "[adapter]\ntype = \"duckdb\"\npath = \":memory:\"\n\n\
             [[hook.on_pipeline_start]]\ncommand = \"a.sh\"\n\
             [hook.on_pipeline_start.env]\nZED = \"1\"\nALPHA = \"2\"\nMID = \"3\"\n\n\
             [hook.webhooks.on_pipeline_complete]\nurl = \"https://h.example/x\"\n",
        )
        .unwrap();
        let config = load_rocky_config(&path).unwrap();
        let output = hooks_list_output(&HookRegistry::from_config(&config.hooks));
        assert_eq!(output.hooks[0].env_keys, ["ALPHA", "MID", "ZED"]);
        assert_eq!(output.hooks.len(), 1);
        assert_eq!(output.webhooks.len(), 1);
        assert_eq!(output.total, 2, "total counts shell hooks and webhooks");
    }
}
