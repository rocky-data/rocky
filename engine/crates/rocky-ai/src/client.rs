//! LLM API client for model generation.
//!
//! Supports Claude (Anthropic Messages API) as the primary provider.
//! Uses reqwest for HTTP, serde for JSON.

use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::residency::AiResidency;

/// Default `max_tokens` for the Anthropic Messages API request body. Also
/// serves as the default cumulative-output token budget enforced across the
/// compile-verify retry loop in [`crate::generate::generate_model`]. Stays at
/// 4096 to preserve pre-token-budget behaviour for users who don't set
/// `[ai] max_tokens` in `rocky.toml`.
pub const DEFAULT_MAX_TOKENS: u32 = 4096;

/// Environment variable that holds the Anthropic API key. Both the LSP's AI
/// code-action arms and the MCP `suggest_freshness_block` tool gate on it;
/// keeping the name in one place keeps their behaviour aligned with
/// `rocky ai`.
pub const AI_API_KEY_ENV: &str = "ANTHROPIC_API_KEY";

/// Default API base URL. The Messages endpoint is `{base_url}/v1/messages`.
pub const DEFAULT_BASE_URL: &str = "https://api.anthropic.com";

/// Environment variable that overrides the API base URL when `[ai] base_url`
/// is not set in `rocky.toml`. Read by the `rocky ai*` commands.
pub const AI_BASE_URL_ENV: &str = "ROCKY_AI_BASE_URL";

/// Providers that [`LlmClient`] can talk to.
const SUPPORTED_PROVIDERS: &[&str] = &["anthropic"];

/// AI provider configuration.
///
/// `api_key` is wrapped in [`RedactedString`] so accidental `?config` /
/// `{config:?}` formatting in logs prints `***` instead of the raw secret.
/// Use [`RedactedString::expose`] only at the outbound HTTP boundary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    /// Provider: "anthropic" or "openai".
    pub provider: String,
    /// Model name (e.g., "claude-sonnet-4-6").
    pub model: String,
    /// API key. Wrapped in [`RedactedString`] to block accidental log leaks.
    pub api_key: RedactedString,
    /// Default output format: "rocky" or "sql".
    #[serde(default = "default_format")]
    pub default_format: String,
    /// Max compile-verify retries.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: usize,
    /// Per-request `max_tokens` for the Anthropic Messages API and the
    /// cumulative output-token budget enforced across retries by
    /// [`crate::generate::generate_model`]. Default
    /// [`DEFAULT_MAX_TOKENS`] preserves pre-knob behaviour.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Optional API base URL (default [`DEFAULT_BASE_URL`]). The request
    /// goes to `{base_url}/v1/messages`; a trailing slash is ignored. Must be
    /// `https`, except for loopback hosts (`localhost`, `127.0.0.1`, `::1`).
    #[serde(default)]
    pub base_url: Option<String>,
}

fn default_format() -> String {
    "rocky".to_string()
}

fn default_max_attempts() -> usize {
    3
}

fn default_max_tokens() -> u32 {
    DEFAULT_MAX_TOKENS
}

/// Errors from AI operations.
#[derive(Error)]
pub enum AiError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error: {message}")]
    Api { message: String },

    #[error("compilation failed after {attempts} attempts")]
    CompileFailed { attempts: usize },

    #[error("no API key configured")]
    NoApiKey,

    /// The request would carry data to an AI endpoint in a region that
    /// `[residency]` does not allow (V062), or of unknown region under
    /// `enforce`. Raised by [`LlmClient::new`], so no request is sent.
    #[error("{0}")]
    Residency(rocky_core::config::ConfigError),

    /// The configured provider has no client in this build.
    #[error("unsupported AI provider `{provider}`: supported providers are {supported}")]
    UnsupportedProvider { provider: String, supported: String },

    /// The configured base URL is malformed or uses plain `http` for a
    /// non-loopback host.
    #[error("invalid AI base URL `{url}`: {reason}")]
    InvalidBaseUrl { url: String, reason: String },

    /// Cumulative output tokens consumed by the compile-verify retry loop
    /// exceeded the configured `[ai] max_tokens` budget. Bounds the
    /// worst-case cost when the LLM produces runaway responses across
    /// retries.
    #[error(
        "AI token budget exceeded after {attempts} attempts: \
         consumed {consumed_output_tokens} output tokens, budget is {budget}. \
         Increase `[ai] max_tokens` in rocky.toml to allow longer generations."
    )]
    TokenBudgetExceeded {
        attempts: usize,
        consumed_output_tokens: u64,
        budget: u32,
    },
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for AiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        rocky_core::secret_registry::fmt_rendered_debug(f, "AiError", self)
    }
}

/// Pick the base URL: the `[ai] base_url` value if set, else the
/// `ROCKY_AI_BASE_URL` environment variable, else `None` (the default).
pub fn resolve_base_url(configured: Option<String>) -> Option<String> {
    configured.or_else(|| {
        std::env::var(AI_BASE_URL_ENV)
            .ok()
            .filter(|v| !v.is_empty())
    })
}

/// Resolve the base URL for a project the way `rocky ai` does: the project's
/// `[ai] base_url`, else `ROCKY_AI_BASE_URL`, else `None` (the default).
///
/// A missing `rocky.toml` falls back to the environment variable. A
/// `rocky.toml` that exists but does not load is an error, so a project that
/// pins a private endpoint never sends its key to the default host because of
/// a config fault.
pub fn resolve_project_base_url(
    config_path: Option<&std::path::Path>,
) -> Result<Option<String>, rocky_core::config::ConfigError> {
    let configured = rocky_core::config::load_optional_project_config(config_path)?
        .and_then(|cfg| cfg.ai.base_url);
    Ok(resolve_base_url(configured))
}

/// The host of `base_url` when it differs from the default endpoint, else
/// `None`. Names where the API key will be sent.
fn custom_base_url_host(base_url: Option<&str>) -> Option<String> {
    let raw = base_url?;
    if raw.trim_end_matches('/') == DEFAULT_BASE_URL {
        return None;
    }
    let url = reqwest::Url::parse(raw).ok()?;
    let host = url.host_str()?;
    if host.eq_ignore_ascii_case("api.anthropic.com") {
        return None;
    }
    Some(host.to_string())
}

/// Validate `base_url` (or the default) and build the Messages endpoint URL.
///
/// Requires `https`; plain `http` is allowed only for loopback hosts so a
/// local model server works without sending the API key over the network.
pub fn messages_url(base_url: Option<&str>) -> Result<String, AiError> {
    let raw = base_url.unwrap_or(DEFAULT_BASE_URL);
    let invalid = |reason: &str| AiError::InvalidBaseUrl {
        url: raw.to_string(),
        reason: reason.to_string(),
    };
    let url = reqwest::Url::parse(raw).map_err(|e| invalid(&e.to_string()))?;
    let host = url
        .host_str()
        .ok_or_else(|| invalid("missing host"))?
        .trim_start_matches('[')
        .trim_end_matches(']');
    match url.scheme() {
        "https" => {}
        "http" => {
            let loopback = host.eq_ignore_ascii_case("localhost")
                || host
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback());
            if !loopback {
                return Err(invalid(
                    "http is only allowed for loopback hosts (localhost, 127.0.0.1, ::1); use https",
                ));
            }
        }
        _ => {
            return Err(invalid(
                "scheme must be https (or http for a loopback host)",
            ));
        }
    }
    Ok(format!("{}/v1/messages", raw.trim_end_matches('/')))
}

/// Response from the LLM.
#[derive(Debug, Clone)]
pub struct LlmResponse {
    /// Generated content (model source code).
    pub content: String,
    /// Model name used.
    pub model: String,
    /// Token usage.
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
}

/// LLM client for generating model code.
pub struct LlmClient {
    config: AiConfig,
    messages_url: String,
    http: reqwest::Client,
}

impl LlmClient {
    /// Create a new LLM client for requests that carry the data `residency`
    /// names.
    ///
    /// Refuses with [`AiError::Residency`] when that data may not go to the
    /// `[ai]` endpoint's region. Every request goes through a client, so the
    /// refusal comes before any request is sent.
    pub fn new(config: AiConfig, residency: AiResidency) -> Result<Self, AiError> {
        residency.into_verdict().map_err(AiError::Residency)?;
        if config.api_key.expose().is_empty() {
            return Err(AiError::NoApiKey);
        }
        if !SUPPORTED_PROVIDERS.contains(&config.provider.as_str()) {
            return Err(AiError::UnsupportedProvider {
                provider: config.provider.clone(),
                supported: SUPPORTED_PROVIDERS.join(", "),
            });
        }
        let messages_url = messages_url(config.base_url.as_deref())?;
        if let Some(host) = custom_base_url_host(config.base_url.as_deref()) {
            tracing::warn!(
                host = %host,
                "AI requests, including the API key, go to a custom base URL instead of {DEFAULT_BASE_URL}"
            );
        }
        // Match the per-adapter timeout pair used elsewhere in the
        // engine (Databricks/Snowflake/BigQuery/Trino/Fivetran/Airbyte).
        // Without these, a stalled Anthropic connection would pin
        // `rocky ai` indefinitely.
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(120))
            .build()
            .expect("reqwest client builder is infallible with these options");
        Ok(Self {
            config,
            messages_url,
            http,
        })
    }

    /// `max_tokens` configured for this client. Used as the per-request cap
    /// on the Anthropic API call and as the cumulative output-token budget
    /// across retries in the compile-verify loop.
    pub fn max_tokens(&self) -> u32 {
        self.config.max_tokens
    }

    /// Generate content from a system prompt and user message.
    pub async fn generate(
        &self,
        system: &str,
        user: &str,
        error_context: Option<&str>,
    ) -> Result<LlmResponse, AiError> {
        match self.config.provider.as_str() {
            "anthropic" => self.call_anthropic(system, user, error_context).await,
            other => Err(AiError::Api {
                message: format!("unsupported provider: {other}"),
            }),
        }
    }

    async fn call_anthropic(
        &self,
        system: &str,
        user: &str,
        error_context: Option<&str>,
    ) -> Result<LlmResponse, AiError> {
        let mut messages = vec![serde_json::json!({
            "role": "user",
            "content": user,
        })];

        if let Some(errors) = error_context {
            messages.push(serde_json::json!({
                "role": "user",
                "content": format!("The previous attempt had compilation errors:\n{errors}\n\nPlease fix and regenerate."),
            }));
        }

        let body = serde_json::json!({
            "model": self.config.model,
            "max_tokens": self.config.max_tokens,
            "system": system,
            "messages": messages,
        });

        let response = self
            .http
            .post(&self.messages_url)
            .header("x-api-key", self.config.api_key.expose())
            .header("anthropic-version", "2023-06-01")
            .header("content-type", "application/json")
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            return Err(AiError::Api {
                message: format!("HTTP {status}: {text}"),
            });
        }

        let json: serde_json::Value = response.json().await?;

        let content = json["content"]
            .as_array()
            .and_then(|arr| arr.first())
            .and_then(|block| block["text"].as_str())
            .unwrap_or("")
            .to_string();

        Ok(LlmResponse {
            content,
            model: json["model"].as_str().unwrap_or("").to_string(),
            input_tokens: json["usage"]["input_tokens"].as_u64(),
            output_tokens: json["usage"]["output_tokens"].as_u64(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config: AiConfig = serde_json::from_str(
            r#"{"provider": "anthropic", "model": "claude-sonnet-4-6", "api_key": "test"}"#,
        )
        .unwrap();
        assert_eq!(config.default_format, "rocky");
        assert_eq!(config.max_attempts, 3);
        assert_eq!(config.max_tokens, DEFAULT_MAX_TOKENS);
    }

    #[test]
    fn test_config_max_tokens_override() {
        let config: AiConfig = serde_json::from_str(
            r#"{"provider": "anthropic", "model": "m", "api_key": "k", "max_tokens": 8192}"#,
        )
        .unwrap();
        assert_eq!(config.max_tokens, 8192);
    }

    #[test]
    fn custom_base_url_host_names_non_default_hosts_only() {
        assert_eq!(custom_base_url_host(None), None);
        assert_eq!(
            custom_base_url_host(Some("https://api.anthropic.com/")),
            None
        );
        assert_eq!(
            custom_base_url_host(Some("https://llm.internal.example:8443/x")).as_deref(),
            Some("llm.internal.example")
        );
        assert_eq!(
            custom_base_url_host(Some("http://localhost:8080")).as_deref(),
            Some("localhost")
        );
    }

    #[test]
    fn resolve_project_base_url_reads_the_project_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(&path, "[ai]\nbase_url = \"https://llm.example.com\"\n").unwrap();
        assert_eq!(
            resolve_project_base_url(Some(&path)).unwrap().as_deref(),
            Some("https://llm.example.com")
        );
    }

    #[test]
    fn resolve_project_base_url_refuses_a_broken_config() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rocky.toml");
        std::fs::write(&path, "[ai\nbase_url = ").unwrap();
        assert!(resolve_project_base_url(Some(&path)).is_err());
    }

    #[test]
    fn test_no_api_key() {
        let config = AiConfig {
            provider: "anthropic".to_string(),
            model: "test".to_string(),
            api_key: RedactedString::new(String::new()),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens: DEFAULT_MAX_TOKENS,
            base_url: None,
        };
        assert!(LlmClient::new(config, AiResidency::no_warehouse_data()).is_err());
    }

    /// `Debug` on [`AiConfig`] must NOT leak the api_key. Defends against a
    /// future `tracing::error!(?config)` accidentally exfiltrating the
    /// credential into log files. Issue 6a.
    #[test]
    fn debug_does_not_leak_api_key() {
        let config = AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: RedactedString::new("sk-ant-supersecret".to_string()),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens: DEFAULT_MAX_TOKENS,
            base_url: None,
        };
        let dbg = format!("{config:?}");
        assert!(
            !dbg.contains("sk-ant-supersecret"),
            "AiConfig Debug leaked the api_key: {dbg}"
        );
        assert!(
            dbg.contains("***"),
            "AiConfig Debug should mask api_key with ***: {dbg}"
        );
    }
}

/// `Debug` prints a resolved `${VAR}` value as `${NAME}` (#1919).
#[cfg(test)]
mod rendered_debug_1919 {
    use super::*;

    #[test]
    fn ai_error_debug_renders_a_resolved_value() {
        const SECRET: &str = "ai-base-url-1919-c013";
        rocky_core::secret_registry::register_substitution("RV_AI_ERR_DBG", SECRET);
        let err = AiError::Api {
            message: format!("POST https://{SECRET}/v1/messages: 401"),
        };
        let debug = format!("{err:?}");
        assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_AI_ERR_DBG}"), "{debug}");
    }
}
