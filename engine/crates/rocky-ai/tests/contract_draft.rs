//! The `draft_contract` compile-verify loop against a stub LLM endpoint.
//!
//! No real API call: wiremock serves fixed drafts, so the test pins how many
//! attempts the loop makes and what the accepted contract says.

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient};
use rocky_ai::contract::{ColumnProfile, TableProfile, draft_contract};
use rocky_ai::residency::AiResidency;
use rocky_compiler::types::{RockyType, TypedColumn};
use rocky_core::redacted::RedactedString;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

const COLUMNS: [&str; 3] = ["order_id", "customer_id", "status"];

fn client(server: &MockServer) -> LlmClient {
    LlmClient::new(
        AiConfig {
            provider: "anthropic".to_string(),
            model: "test-model".to_string(),
            api_key: RedactedString::new("secret-key".to_string()),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens: DEFAULT_MAX_TOKENS,
            base_url: Some(server.uri()),
        },
        AiResidency::no_warehouse_data(),
    )
    .unwrap()
}

fn reply(text: &str) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(serde_json::json!({
        "content": [{"type": "text", "text": text}],
        "model": "test-model",
        "usage": {"input_tokens": 3, "output_tokens": 5},
    }))
}

fn draft(nullable: bool) -> String {
    COLUMNS
        .iter()
        .map(|c| format!("[[columns]]\nname = \"{c}\"\nnullable = {nullable}\n\n"))
        .collect::<String>()
        + "[rules]\nrequired = [\"order_id\"]\nprotected = [\"order_id\"]\n"
}

/// A passthrough over a source Rocky has no schema for: every column is
/// `(Unknown, nullable)`, which is "not proven non-null".
fn unknown_schema() -> Vec<TypedColumn> {
    COLUMNS
        .iter()
        .map(|c| TypedColumn {
            name: (*c).to_string(),
            data_type: RockyType::Unknown,
            nullable: true,
        })
        .collect()
}

fn profile(nulls: u64) -> TableProfile {
    TableProfile {
        model: "raw_orders".to_string(),
        columns: COLUMNS
            .iter()
            .map(|c| ColumnProfile {
                name: (*c).to_string(),
                type_name: "Unknown".to_string(),
                rows: 500,
                nulls,
                null_rate: nulls as f64 / 500.0,
                distinct: 50,
                observed_values: vec![],
                min: None,
                max: None,
            })
            .collect(),
    }
}

/// The LLM proposes `nullable = false` for columns the profile shows hold no
/// NULL, and Rocky cannot prove them non-null. The loop must accept the
/// draft on the first attempt with `nullable` unset, not spend a retry that
/// rewrites every column to `nullable = true`.
#[tokio::test]
async fn unproven_non_null_is_accepted_first_time_without_a_flip_to_true() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(&draft(false)))
        .expect(1)
        .mount(&server)
        .await;

    let drafted = draft_contract(&profile(0), &unknown_schema(), &client(&server), 3)
        .await
        .expect("the draft verifies");

    assert_eq!(drafted.attempts, 1, "no E012 retry");
    assert!(
        !drafted.toml.contains("\nnullable ="),
        "no unproven `false` and no flipped `true`:\n{}",
        drafted.toml
    );
    assert_eq!(drafted.unproven_non_null.len(), COLUMNS.len());
    assert!(
        drafted
            .contract
            .columns
            .iter()
            .all(|c| c.nullable.is_none()),
        "{:?}",
        drafted.contract.columns
    );
}

/// When the profile DID count NULLs, `nullable = false` contradicts the
/// evidence: `E012` still goes back to the LLM and the loop retries.
#[tokio::test]
async fn observed_nulls_still_send_e012_to_the_retry() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(&draft(false)))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(&draft(true)))
        .expect(1)
        .mount(&server)
        .await;

    let drafted = draft_contract(&profile(7), &unknown_schema(), &client(&server), 3)
        .await
        .expect("the retry verifies");

    assert_eq!(drafted.attempts, 2, "E012 on observed NULLs is retried");
    assert!(drafted.unproven_non_null.is_empty());
}

/// The retry carries the rejected draft beside the diagnostics, so the LLM
/// fixes what they name instead of rewriting the contract from the profile.
#[tokio::test]
async fn the_retry_shows_the_rejected_draft() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(&draft(false)))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(&draft(true)))
        .mount(&server)
        .await;

    draft_contract(&profile(7), &unknown_schema(), &client(&server), 3)
        .await
        .expect("the retry verifies");

    let requests = server.received_requests().await.expect("recorded");
    assert_eq!(requests.len(), 2);
    let body: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
    let retry = body["messages"][1]["content"].as_str().unwrap();
    assert!(retry.contains("E012"), "{retry}");
    assert!(
        retry.contains("The rejected contract was:") && retry.contains(&draft(false)),
        "the retry names the rejected draft: {retry}"
    );
    // The first request carries Rocky's proof beside the observed counts.
    let first: serde_json::Value = serde_json::from_slice(&requests[0].body).unwrap();
    let evidence = first["messages"][0]["content"].as_str().unwrap();
    assert!(
        evidence.contains("rocky_nullability=not proven non-null"),
        "{evidence}"
    );
}
