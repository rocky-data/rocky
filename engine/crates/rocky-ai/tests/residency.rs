//! V062: an AI request over data in one region is refused before any request
//! reaches an AI endpoint in another region.

use rocky_ai::client::{AiConfig, AiError, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::residency::{AiResidency, data_regions};
use rocky_core::config::{ConfigError, RockyConfig, load_optional_project_config};
use rocky_core::redacted::RedactedString;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// A transformation pipeline whose target is in `target_region`, an `[ai]`
/// endpoint in `ai_region`, and `extra` appended verbatim. Loaded the way
/// every command loads it, so the V060/V061 validators run too.
fn project(target_region: Option<&str>, ai_region: Option<&str>, extra: &str) -> RockyConfig {
    let target = target_region.map_or(String::new(), |r| format!("region = \"{r}\"\n"));
    let ai = ai_region.map_or(String::new(), |r| format!("\n[ai]\nregion = \"{r}\"\n"));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rocky.toml");
    std::fs::write(
        &path,
        format!(
            r#"
[adapter]
type = "duckdb"
path = "x.duckdb"

[pipeline.xf]
type = "transformation"

[pipeline.xf.target]
adapter = "default"
{target}{ai}{extra}"#
        ),
    )
    .unwrap();
    load_optional_project_config(Some(&path))
        .expect("test config loads")
        .expect("test config is present")
}

/// Mount a Messages endpoint that expects `expected` requests.
async fn endpoint(expected: u64) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "content": [{"type": "text", "text": "ok"}],
            "model": "test-model",
            "usage": {"input_tokens": 1, "output_tokens": 1},
        })))
        .expect(expected)
        .mount(&server)
        .await;
    server
}

/// Build a client for a request about model `orders` and send one request,
/// the way every AI caller does.
async fn ask_about_orders(server: &MockServer, project: &RockyConfig) -> Result<(), AiError> {
    let dir = tempfile::tempdir().unwrap();
    let data = data_regions(
        Some(project),
        &dir.path().join("rocky.toml"),
        &["orders"],
        &[],
    );
    let client = LlmClient::new(
        AiConfig {
            provider: "anthropic".to_string(),
            model: "test-model".to_string(),
            api_key: RedactedString::new("secret-key".to_string()),
            default_format: "rocky".to_string(),
            max_attempts: 1,
            max_tokens: DEFAULT_MAX_TOKENS,
            base_url: Some(server.uri()),
        },
        AiResidency::for_data(Some(project), &data),
    )?;
    client.generate("sys", "user", None).await.map(|_| ())
}

#[tokio::test]
async fn eu_data_to_a_us_endpoint_is_refused_before_any_request() {
    let server = endpoint(0).await;
    let err = ask_about_orders(&server, &project(Some("eu"), Some("us"), ""))
        .await
        .expect_err("EU data to a US endpoint must be refused");
    let AiError::Residency(ConfigError::ResidencyAiCrossRegion { data, from, to }) = &err else {
        panic!("expected a V062 cross-region refusal, got {err}");
    };
    assert_eq!((from.as_str(), to.as_str()), ("eu", "us"));
    assert!(data.contains("`orders`"), "{data}");
    assert!(err.to_string().starts_with("V062"), "{err}");
    server.verify().await;
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn an_allow_entry_lets_the_request_through() {
    let server = endpoint(1).await;
    let allow = "\n[residency]\nallow = [{ from = \"eu\", to = \"us\" }]\n";
    ask_about_orders(&server, &project(Some("eu"), Some("us"), allow))
        .await
        .expect("allow lists eu -> us");
    server.verify().await;
}

#[tokio::test]
async fn the_reverse_allow_entry_does_not() {
    let server = endpoint(0).await;
    let allow = "\n[residency]\nallow = [{ from = \"us\", to = \"eu\" }]\n";
    let err = ask_about_orders(&server, &project(Some("eu"), Some("us"), allow))
        .await
        .expect_err("allow lists only us -> eu");
    assert!(
        matches!(
            err,
            AiError::Residency(ConfigError::ResidencyAiCrossRegion { .. })
        ),
        "{err}"
    );
    server.verify().await;
}

#[tokio::test]
async fn the_same_region_passes() {
    let server = endpoint(1).await;
    ask_about_orders(&server, &project(Some("eu"), Some("eu"), ""))
        .await
        .expect("same region");
    server.verify().await;
}

#[tokio::test]
async fn unlabelled_data_passes_without_enforce() {
    let server = endpoint(1).await;
    ask_about_orders(&server, &project(None, Some("us"), ""))
        .await
        .expect("no label, no enforce");
    server.verify().await;

    let server = endpoint(1).await;
    ask_about_orders(&server, &project(Some("eu"), None, ""))
        .await
        .expect("no [ai] region, no enforce");
    server.verify().await;
}

/// Under `enforce`, a table that is not a project model has no region, so a
/// request carrying its schema is refused before any request is sent.
#[tokio::test]
async fn data_of_unknown_region_is_refused_under_enforce() {
    let server = endpoint(0).await;
    let cfg = project(Some("eu"), Some("eu"), "\n[residency]\nenforce = true\n");
    let dir = tempfile::tempdir().unwrap();
    let data = data_regions(
        Some(&cfg),
        &dir.path().join("rocky.toml"),
        &["orders"],
        &["raw.orders"],
    );
    let err = LlmClient::new(
        AiConfig {
            provider: "anthropic".to_string(),
            model: "test-model".to_string(),
            api_key: RedactedString::new("secret-key".to_string()),
            default_format: "rocky".to_string(),
            max_attempts: 1,
            max_tokens: DEFAULT_MAX_TOKENS,
            base_url: Some(server.uri()),
        },
        AiResidency::for_data(Some(&cfg), &data),
    )
    .err()
    .expect("unknown region under enforce");
    let AiError::Residency(ConfigError::ResidencyAiRegionUnknown { data }) = &err else {
        panic!("expected a V062 unknown-region refusal, got {err}");
    };
    assert!(data.contains("`raw.orders`"), "{data}");
    server.verify().await;
}
