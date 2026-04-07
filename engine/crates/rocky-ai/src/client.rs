//! LLM API client for model generation.
//!
//! Supports Claude (Anthropic Messages API) as the primary provider.
//! Uses reqwest for HTTP, serde for JSON.

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// AI provider configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AiConfig {
    /// Provider: "anthropic" or "openai".
    pub provider: String,
    /// Model name (e.g., "claude-sonnet-4-6").
    pub model: String,
    /// API key.
    pub api_key: String,
    /// Default output format: "rocky" or "sql".
    #[serde(default = "default_format")]
    pub default_format: String,
    /// Max compile-verify retries.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: usize,
}

fn default_format() -> String {
    "rocky".to_string()
}

fn default_max_attempts() -> usize {
    3
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
        if config.api_key.is_empty() {
            return Err(AiError::NoApiKey);
        }
        Ok(Self {
            config,
            http: reqwest::Client::new(),
        })
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
            "max_tokens": 4096,
            "system": system,
            "messages": messages,
        });

        let response = self
            .http
            .post("https://api.anthropic.com/v1/messages")
            .header("x-api-key", &self.config.api_key)
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
    }

    #[test]
    fn test_no_api_key() {
        let config = AiConfig {
            provider: "anthropic".to_string(),
            model: "test".to_string(),
            api_key: String::new(),
            default_format: "rocky".to_string(),
            max_attempts: 3,
        };
        assert!(LlmClient::new(config).is_err());
    }
}
