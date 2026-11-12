//! The usage sink sees every AI call that returned a response, and only
//! those.

use std::sync::{Arc, Mutex};

use rocky_ai::client::{AiConfig, DEFAULT_MAX_TOKENS, LlmClient, LlmUsage};
use rocky_ai::residency::AiResidency;
use rocky_ai::usage::UsageRecorder;
use rocky_core::redacted::RedactedString;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

fn client(server: &MockServer) -> LlmClient {
    LlmClient::new(
        AiConfig {
            provider: "anthropic".to_string(),
            model: "configured-model".to_string(),
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

fn reply(body: serde_json::Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(body)
}

#[tokio::test]
async fn the_sink_sees_each_call_with_its_own_usage() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(serde_json::json!({
            "content": [{"type": "text", "text": "a"}],
            "model": "answering-model",
            "usage": {"input_tokens": 3, "output_tokens": 5},
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    // A response without usage, and without a model id.
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(serde_json::json!({
            "content": [{"type": "text", "text": "b"}],
        })))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .mount(&server)
        .await;

    let seen: Arc<Mutex<Vec<LlmUsage>>> = Arc::default();
    let sink_seen = Arc::clone(&seen);
    let client = client(&server).with_usage_sink(Arc::new(move |usage: &LlmUsage| {
        sink_seen.lock().unwrap().push(usage.clone());
    }));
    client.generate("s", "u", None).await.unwrap();
    client.generate("s", "u", Some("retry")).await.unwrap();
    assert!(client.generate("s", "u", None).await.is_err());

    let seen = seen.lock().unwrap();
    assert_eq!(
        *seen,
        vec![
            LlmUsage {
                llm_model: "answering-model".to_string(),
                input_tokens: Some(3),
                output_tokens: Some(5),
            },
            LlmUsage {
                llm_model: "configured-model".to_string(),
                input_tokens: None,
                output_tokens: None,
            },
        ],
        "one record per response that parsed; the failed call records nothing"
    );
}

#[tokio::test]
async fn a_recorder_labels_calls_with_the_command_and_the_model() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(serde_json::json!({
            "content": [{"type": "text", "text": "a"}],
            "model": "m",
            "usage": {"input_tokens": 1, "output_tokens": 2},
        })))
        .mount(&server)
        .await;

    let recorder = UsageRecorder::collect("ai_explain");
    let client = client(&server).with_usage_sink(recorder.sink());
    recorder.set_model(Some("orders"));
    client.generate("s", "u", None).await.unwrap();
    recorder.set_model(None);
    client.generate("s", "u", None).await.unwrap();
    recorder.label_unlabelled("generated");

    let records = recorder.records();
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.command == "ai_explain"));
    assert_eq!(records[0].model.as_deref(), Some("orders"));
    assert_eq!(records[1].model.as_deref(), Some("generated"));
    assert_ne!(records[0].id, records[1].id, "each call has its own id");

    // Persisting with no state file spools; the store is never created.
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.redb");
    assert_eq!(
        recorder.persist(&state),
        Some(rocky_core::ai_usage::RecordOutcome::Spooled)
    );
    assert!(!state.exists());
    let spooled = rocky_core::ai_usage::read_spool(&rocky_core::ai_usage::spool_path(&state))
        .unwrap()
        .records;
    assert_eq!(spooled, records);
    assert!(recorder.records().is_empty(), "persist clears the records");
}

#[tokio::test]
async fn a_spooling_recorder_appends_each_call_at_once() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/messages"))
        .respond_with(reply(serde_json::json!({
            "content": [{"type": "text", "text": "a"}],
            "model": "m",
            "usage": {"input_tokens": 1, "output_tokens": 2},
        })))
        .mount(&server)
        .await;
    let dir = tempfile::tempdir().unwrap();
    let state = dir.path().join("state.redb");
    let recorder = UsageRecorder::spool("lsp_ai_dsl_fix", &state);
    let client = client(&server).with_usage_sink(recorder.sink());
    client.generate("s", "u", None).await.unwrap();
    let spooled = rocky_core::ai_usage::read_spool(&rocky_core::ai_usage::spool_path(&state))
        .unwrap()
        .records;
    assert_eq!(spooled.len(), 1);
    assert_eq!(spooled[0].command, "lsp_ai_dsl_fix");
    assert!(recorder.records().is_empty());
}
