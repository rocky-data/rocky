//! `AiConfig::base_url` routing and validation.

use rocky_ai::client::{AiConfig, AiError, DEFAULT_MAX_TOKENS, LlmClient, messages_url};
use rocky_ai::residency::AiResidency;
use rocky_core::redacted::RedactedString;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn config(provider: &str, base_url: Option<String>) -> AiConfig {
    AiConfig {
        provider: provider.to_string(),
        model: "test-model".to_string(),
        api_key: RedactedString::new("secret-key".to_string()),
        default_format: "rocky".to_string(),
        max_attempts: 1,
        max_tokens: DEFAULT_MAX_TOKENS,
        base_url,
    }
}

#[tokio::test]
async fn generate_posts_to_custom_base_url() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .and(header("x-api-key", "secret-key"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "content": [{"type": "text", "text": "hello"}],
            "model": "test-model",
            "usage": {"input_tokens": 3, "output_tokens": 5},
        })))
        .expect(1)
        .mount(&server)
        .await;

    // wiremock listens on http://127.0.0.1:<port>; a trailing slash is allowed.
    let client = LlmClient::new(
        config("anthropic", Some(format!("{}/", server.uri()))),
        AiResidency::no_warehouse_data(),
    )
    .unwrap();
    let resp = client.generate("sys", "user", None).await.unwrap();
    assert_eq!(resp.content, "hello");
    assert_eq!(resp.output_tokens, Some(5));
}

#[test]
fn default_base_url_is_anthropic() {
    assert_eq!(
        messages_url(None).unwrap(),
        "https://api.anthropic.com/v1/messages"
    );
}

#[test]
fn trailing_slash_is_trimmed() {
    assert_eq!(
        messages_url(Some("https://proxy.example.com/")).unwrap(),
        "https://proxy.example.com/v1/messages"
    );
}

#[test]
fn http_allowed_only_for_loopback() {
    for ok in [
        "http://localhost:11434",
        "http://127.0.0.1:8080",
        "http://[::1]:8080",
    ] {
        assert!(messages_url(Some(ok)).is_ok(), "{ok}");
    }
    for bad in [
        "http://example.com",
        "http://10.0.0.5:8080",
        "http://localhost.evil.com",
    ] {
        let err = messages_url(Some(bad)).unwrap_err();
        assert!(matches!(err, AiError::InvalidBaseUrl { .. }), "{bad}");
        assert!(err.to_string().contains("loopback"), "{err}");
    }
}

#[test]
fn malformed_or_odd_scheme_is_refused() {
    for bad in ["", "not a url", "ftp://example.com"] {
        assert!(
            matches!(messages_url(Some(bad)), Err(AiError::InvalidBaseUrl { .. })),
            "{bad}"
        );
    }
}

#[test]
fn client_new_rejects_bad_base_url() {
    let err = LlmClient::new(
        config("anthropic", Some("http://example.com".into())),
        AiResidency::no_warehouse_data(),
    )
    .err()
    .unwrap();
    assert!(matches!(err, AiError::InvalidBaseUrl { .. }));
}

#[test]
fn unknown_provider_is_an_error_at_construction() {
    let err = LlmClient::new(config("openai", None), AiResidency::no_warehouse_data())
        .err()
        .unwrap();
    assert!(matches!(err, AiError::UnsupportedProvider { .. }));
    assert!(err.to_string().contains("openai"));
    assert!(LlmClient::new(config("anthropic", None), AiResidency::no_warehouse_data()).is_ok());
}
