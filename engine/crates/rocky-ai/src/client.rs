//! LLM API client for model generation.
//!
//! Supports Claude (Anthropic Messages API) as the primary provider.
//! Uses reqwest for HTTP, serde for JSON.

use rocky_core::redacted::RedactedString;
use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Default `max_tokens` for the Anthropic Messages API request body. Also
/// serves as the default cumulative-output token budget enforced across the
/// compile-verify retry loop in [`crate::generate::generate_model`]. Stays at
/// 4096 to preserve pre-token-budget behaviour for users who don't set
/// `[ai] max_tokens` in `rocky.toml`.
pub const DEFAULT_MAX_TOKENS: u32 = 4096;

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
#[derive(Debug, Error)]
pub enum AiError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error: {message}")]
    Api { message: String },

    #[error("compilation failed after {attempts} attempts")]
    CompileFailed { attempts: usize },

    #[error("no API key configured")]
    NoApiKey,

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
    http: reqwest::Client,
}

impl LlmClient {
    /// Create a new LLM client.
    pub fn new(config: AiConfig) -> Result<Self, AiError> {
        if config.api_key.expose().is_empty() {
            return Err(AiError::NoApiKey);
        }
        Ok(Self {
            config,
            http: reqwest::Client::new(),
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
            .post("https://api.anthropic.com/v1/messages")
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
    fn test_no_api_key() {
        let config = AiConfig {
            provider: "anthropic".to_string(),
            model: "test".to_string(),
            api_key: RedactedString::new(String::new()),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens: DEFAULT_MAX_TOKENS,
        };
        assert!(LlmClient::new(config).is_err());
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
