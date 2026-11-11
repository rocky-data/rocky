//! Wiremock tests for the Airbyte sync state read and sync trigger
//! (`[...discovery.sync] wait = true` / `trigger = true`).

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use rocky_core::config::RetryConfig;
use rocky_core::source_sync::{
    SourceSyncOutcome, SourceSyncReport, SourceSyncState, SourceSyncTriggerError,
    SourceSyncTriggered, SourceSyncWaitOptions, trigger_and_wait_for_source_syncs,
    wait_for_source_syncs,
};
use rocky_core::traits::DiscoveryAdapter;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::adapter::AirbyteDiscoveryAdapter;
use crate::client::AirbyteClient;

/// The server clock every answer carries, in HTTP-date form.
const SERVER_DATE: &str = "Wed, 21 Oct 2026 10:00:00 GMT";
const OLD: &str = "2026-10-21T09:00:00Z";

fn server_time() -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 10, 21, 10, 0, 0).unwrap()
}

fn fast_retry() -> RetryConfig {
    RetryConfig {
        max_retries: 2,
        initial_backoff_ms: 1,
        max_backoff_ms: 2,
        backoff_multiplier: 1.0,
        ..RetryConfig::default()
    }
}

fn client(server: &MockServer) -> AirbyteClient {
    AirbyteClient::with_retry(&server.uri(), Some("tok_secret".into()), fast_retry())
}

fn adapter(server: &MockServer, allowed: bool) -> AirbyteDiscoveryAdapter {
    AirbyteDiscoveryAdapter::new(client(server)).with_sync_trigger_allowed(allowed)
}

fn connection(id: &str, namespace: &str, status: &str) -> Value {
    json!({
        "connectionId": id,
        "name": format!("conn {id}"),
        "sourceId": "src",
        "destinationId": "dst",
        "status": status,
        "namespaceFormat": namespace,
    })
}

async fn serve_connections(server: &MockServer, connections: Vec<Value>) {
    Mock::given(method("GET"))
        .and(path("/v1/connections"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("Date", SERVER_DATE)
                .set_body_json(json!({ "data": Value::Array(connections) })),
        )
        .mount(server)
        .await;
}

fn job(id: i64, job_type: &str, status: &str, last_updated_at: Option<&str>) -> Value {
    let mut job = json!({
        "jobId": id,
        "status": status,
        "jobType": job_type,
        "startTime": OLD,
        "connectionId": "conn_1",
    });
    if let Some(t) = last_updated_at {
        job["lastUpdatedAt"] = json!(t);
    }
    job
}

fn jobs_page(jobs: Vec<Value>) -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("Date", SERVER_DATE)
        .set_body_json(json!({ "data": Value::Array(jobs) }))
}

/// Serve the jobs answers of one connection in order; the last repeats.
async fn serve_jobs(server: &MockServer, connection_id: &str, answers: Vec<ResponseTemplate>) {
    let last = answers.len() - 1;
    for (i, answer) in answers.into_iter().enumerate() {
        let mock = Mock::given(method("GET"))
            .and(path("/v1/jobs"))
            .and(query_param("connectionId", connection_id))
            .respond_with(answer);
        if i < last {
            mock.up_to_n_times(1).mount(server).await;
        } else {
            mock.mount(server).await;
        }
    }
}

async fn serve_post(server: &MockServer, answer: ResponseTemplate) {
    Mock::given(method("POST"))
        .and(path("/v1/jobs"))
        .respond_with(answer)
        .mount(server)
        .await;
}

fn accepted() -> ResponseTemplate {
    ResponseTemplate::new(200)
        .insert_header("Date", SERVER_DATE)
        .set_body_json(job(99, "sync", "pending", None))
}

async fn posts(server: &MockServer) -> Vec<Request> {
    server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.method == wiremock::http::Method::POST)
        .collect()
}

fn ts(raw: &str) -> Option<DateTime<Utc>> {
    Some(
        DateTime::parse_from_rfc3339(raw)
            .unwrap()
            .with_timezone(&Utc),
    )
}

fn by_id<'a>(states: &'a [SourceSyncState], id: &str) -> &'a SourceSyncState {
    states
        .iter()
        .find(|s| s.id == id)
        .unwrap_or_else(|| panic!("{id} missing from {states:?}"))
}

// ---------------------------------------------------------------------------
// Sync state read
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_states_map_the_newest_jobs_of_each_connection() {
    let server = MockServer::start().await;
    serve_connections(
        &server,
        vec![
            connection("c_run", "src__run", "active"),
            connection("c_ok", "src__ok", "active"),
            connection("c_fail", "src__fail", "active"),
            connection("c_cancel", "src__cancel", "active"),
            connection("c_paused", "src__paused", "inactive"),
            connection("c_new", "src__new", "active"),
            connection("c_gone", "src__gone", "deprecated"),
            connection("c_other", "other__x", "active"),
        ],
    )
    .await;
    serve_jobs(
        &server,
        "c_run",
        vec![jobs_page(vec![
            job(3, "sync", "running", Some("2026-10-21T09:59:00Z")),
            job(2, "sync", "succeeded", Some(OLD)),
        ])],
    )
    .await;
    serve_jobs(
        &server,
        "c_ok",
        vec![jobs_page(vec![
            job(5, "sync", "succeeded", Some("2026-10-21T09:30:00Z")),
            job(4, "sync", "failed", Some(OLD)),
        ])],
    )
    .await;
    serve_jobs(
        &server,
        "c_fail",
        vec![jobs_page(vec![
            job(7, "sync", "failed", Some("2026-10-21T09:40:00Z")),
            job(6, "sync", "succeeded", Some(OLD)),
        ])],
    )
    .await;
    serve_jobs(
        &server,
        "c_cancel",
        vec![jobs_page(vec![job(
            8,
            "sync",
            "cancelled",
            Some("2026-10-21T09:45:00Z"),
        )])],
    )
    .await;
    serve_jobs(
        &server,
        "c_paused",
        vec![jobs_page(vec![job(9, "sync", "succeeded", Some(OLD))])],
    )
    .await;
    serve_jobs(&server, "c_new", vec![jobs_page(vec![])]).await;

    let states = adapter(&server, false)
        .source_sync_states("src__")
        .await
        .unwrap()
        .unwrap();

    let ids: Vec<&str> = states.iter().map(|s| s.id.as_str()).collect();
    assert_eq!(
        ids,
        ["c_run", "c_ok", "c_fail", "c_cancel", "c_paused", "c_new"],
        "deprecated and off-prefix connections are left out"
    );

    let run = by_id(&states, "c_run");
    assert!(run.syncing && !run.paused);
    assert_eq!(run.succeeded_at, ts(OLD));
    assert_eq!(run.failed_at, None);
    assert_eq!(run.schema, "src__run");
    assert_eq!(run.source_type, "airbyte");
    assert_eq!(run.observed_at, Some(server_time()));

    let ok = by_id(&states, "c_ok");
    assert!(!ok.syncing && !ok.paused);
    assert_eq!(ok.succeeded_at, ts("2026-10-21T09:30:00Z"));
    assert_eq!(ok.failed_at, ts(OLD));

    let fail = by_id(&states, "c_fail");
    assert!(!fail.syncing);
    assert_eq!(fail.failed_at, ts("2026-10-21T09:40:00Z"));
    assert_eq!(fail.succeeded_at, ts(OLD));

    let cancel = by_id(&states, "c_cancel");
    assert!(!cancel.syncing);
    assert_eq!(cancel.succeeded_at, None, "a cancelled sync is no success");
    assert_eq!(cancel.failed_at, ts("2026-10-21T09:45:00Z"));

    let paused = by_id(&states, "c_paused");
    assert!(paused.paused, "inactive is paused");
    assert_eq!(paused.succeeded_at, ts(OLD));

    let new = by_id(&states, "c_new");
    assert!(!new.syncing && !new.paused);
    assert_eq!((new.succeeded_at, new.failed_at), (None, None));

    // The jobs read asks for the newest jobs first, of every type.
    let job_reads: Vec<Request> = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.url.path() == "/v1/jobs")
        .collect();
    assert_eq!(job_reads.len(), 6);
    let query: Vec<(String, String)> = job_reads[0]
        .url
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    assert!(query.contains(&("orderBy".into(), "createdAt|DESC".into())));
    assert!(query.contains(&("limit".into(), "25".into())));
    assert!(!query.iter().any(|(k, _)| k == "jobType"));
    assert!(posts(&server).await.is_empty(), "a state read never POSTs");
}

#[tokio::test]
async fn pending_incomplete_and_unknown_statuses_are_syncing() {
    for status in ["pending", "running", "incomplete", "queued_somewhere_new"] {
        let server = MockServer::start().await;
        serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
        serve_jobs(
            &server,
            "conn_1",
            vec![jobs_page(vec![job(2, "sync", status, None)])],
        )
        .await;
        let states = adapter(&server, false)
            .source_sync_states("src__")
            .await
            .unwrap()
            .unwrap();
        assert!(states[0].syncing, "{status} must count as syncing");
    }
}

/// A reset writes to the destination too, so a running reset is waited for.
/// But a reset that ended is never the end of a sync.
#[tokio::test]
async fn a_reset_job_counts_as_syncing_but_never_as_a_sync_end() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![jobs_page(vec![
            job(3, "reset", "running", None),
            job(2, "reset", "succeeded", Some("2026-10-21T09:50:00Z")),
            job(1, "reset", "failed", Some("2026-10-21T09:40:00Z")),
        ])],
    )
    .await;
    let states = adapter(&server, false)
        .source_sync_states("src__")
        .await
        .unwrap()
        .unwrap();
    assert!(states[0].syncing);
    assert_eq!((states[0].succeeded_at, states[0].failed_at), (None, None));
}

#[tokio::test]
async fn the_latest_end_wins_and_a_job_without_an_end_time_gives_none() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![jobs_page(vec![
            job(3, "sync", "succeeded", None),
            job(2, "sync", "succeeded", Some("2026-10-21T09:10:00Z")),
            job(1, "sync", "succeeded", Some("2026-10-21T09:20:00Z")),
        ])],
    )
    .await;
    let states = adapter(&server, false)
        .source_sync_states("src__")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(states[0].succeeded_at, ts("2026-10-21T09:20:00Z"));
}

#[tokio::test]
async fn a_job_time_that_is_not_rfc3339_fails_the_read() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![jobs_page(vec![job(
            1,
            "sync",
            "succeeded",
            Some("21/10/2026 10:00"),
        )])],
    )
    .await;
    let err = adapter(&server, false)
        .source_sync_states("src__")
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not RFC 3339"), "{err}");
}

#[tokio::test]
async fn the_connection_id_is_encoded_into_the_jobs_query() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("a&limit=1#x", "src__a", "active")]).await;
    Mock::given(method("GET"))
        .and(path("/v1/jobs"))
        .respond_with(jobs_page(vec![]))
        .mount(&server)
        .await;
    adapter(&server, false)
        .source_sync_states("src__")
        .await
        .unwrap()
        .unwrap();
    let read = server
        .received_requests()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.url.path() == "/v1/jobs")
        .unwrap();
    let ids: Vec<String> = read
        .url
        .query_pairs()
        .filter(|(k, _)| k == "connectionId")
        .map(|(_, v)| v.into_owned())
        .collect();
    assert_eq!(ids, ["a&limit=1#x"], "one connectionId, the whole raw id");
    let limits = read.url.query_pairs().filter(|(k, _)| k == "limit").count();
    assert_eq!(limits, 1, "the id cannot add a query parameter");
}

// ---------------------------------------------------------------------------
// Sync trigger
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_trigger_posts_exactly_one_sync_job_and_returns_the_date() {
    let server = MockServer::start().await;
    serve_post(&server, accepted()).await;
    let date = adapter(&server, true)
        .trigger_source_sync("conn_1")
        .await
        .unwrap();
    assert_eq!(date, Some(server_time()));
    let sent = posts(&server).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].url.path(), "/v1/jobs");
    let body: Value = serde_json::from_slice(&sent[0].body).unwrap();
    assert_eq!(
        body,
        json!({ "connectionId": "conn_1", "jobType": "sync" }),
        "a sync job, never a reset"
    );
    let auth = sent[0].headers.get("authorization").unwrap();
    assert_eq!(auth.to_str().unwrap(), "Bearer tok_secret");
}

/// Opt-in check, adapter leg: without `allow_sync_trigger` not one request
/// leaves, even when the pipeline asks for a trigger.
#[tokio::test]
async fn without_the_owner_opt_in_no_request_is_sent() {
    let server = MockServer::start().await;
    serve_post(&server, accepted()).await;
    let err = adapter(&server, false)
        .trigger_source_sync("conn_1")
        .await
        .unwrap_err();
    assert!(matches!(err, SourceSyncTriggerError::NotAllowed), "{err}");
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn a_403_is_forbidden_and_names_the_fix() {
    let server = MockServer::start().await;
    serve_post(
        &server,
        ResponseTemplate::new(403).set_body_string("Not allowed"),
    )
    .await;
    let err = adapter(&server, true)
        .trigger_source_sync("conn_1")
        .await
        .unwrap_err();
    let SourceSyncTriggerError::Forbidden(text) = err else {
        panic!("expected Forbidden, got {err:?}");
    };
    assert!(text.contains("HTTP 403"), "{text}");
    assert!(text.contains("set trigger = false"), "{text}");
    assert!(text.contains("Not allowed"), "{text}");
    assert!(!text.contains("tok_secret"), "{text}");
    assert_eq!(posts(&server).await.len(), 1);
}

#[tokio::test]
async fn a_400_or_404_is_failed() {
    for status in [400, 404] {
        let server = MockServer::start().await;
        serve_post(
            &server,
            ResponseTemplate::new(status).set_body_string("nope"),
        )
        .await;
        let err = adapter(&server, true)
            .trigger_source_sync("conn_1")
            .await
            .unwrap_err();
        assert!(
            matches!(&err, SourceSyncTriggerError::Failed(m) if m.contains(&status.to_string())),
            "{status}: {err:?}"
        );
        assert_eq!(posts(&server).await.len(), 1, "{status} is not retried");
    }
}

#[tokio::test]
async fn a_409_is_a_conflict() {
    let server = MockServer::start().await;
    serve_post(
        &server,
        ResponseTemplate::new(409).set_body_string("A sync is already running"),
    )
    .await;
    let err = adapter(&server, true)
        .trigger_source_sync("conn_1")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, SourceSyncTriggerError::Conflict(m) if m.contains("already running")),
        "{err:?}"
    );
}

#[tokio::test]
async fn a_5xx_is_unknown_and_never_retried() {
    let server = MockServer::start().await;
    serve_post(
        &server,
        ResponseTemplate::new(502).set_body_string("bad gateway"),
    )
    .await;
    let err = adapter(&server, true)
        .trigger_source_sync("conn_1")
        .await
        .unwrap_err();
    assert!(
        matches!(&err, SourceSyncTriggerError::Unknown(m) if m.contains("502")),
        "{err:?}"
    );
    assert_eq!(
        posts(&server).await.len(),
        1,
        "a 5xx may have started a job"
    );
}

#[tokio::test]
async fn a_timeout_is_unknown_and_never_retried() {
    let server = MockServer::start().await;
    serve_post(&server, accepted().set_delay(Duration::from_millis(500))).await;
    let http = reqwest::Client::builder()
        .timeout(Duration::from_millis(100))
        .build()
        .unwrap();
    let adapter = AirbyteDiscoveryAdapter::new(client(&server).with_http_client(http))
        .with_sync_trigger_allowed(true);
    let err = adapter.trigger_source_sync("conn_1").await.unwrap_err();
    assert!(matches!(err, SourceSyncTriggerError::Unknown(_)), "{err:?}");
    assert_eq!(posts(&server).await.len(), 1);
}

#[tokio::test]
async fn an_exhausted_429_is_failed_after_the_retries() {
    let server = MockServer::start().await;
    serve_post(&server, ResponseTemplate::new(429)).await;
    let err = adapter(&server, true)
        .trigger_source_sync("conn_1")
        .await
        .unwrap_err();
    assert!(matches!(err, SourceSyncTriggerError::Failed(_)), "{err:?}");
    assert_eq!(posts(&server).await.len(), 3, "max_retries = 2, so 3 tries");
}

#[tokio::test]
async fn a_connect_error_is_failed() {
    // Nothing listens on port 9 of localhost in the test container.
    let adapter = AirbyteDiscoveryAdapter::new(AirbyteClient::with_retry(
        "http://127.0.0.1:9",
        None,
        fast_retry(),
    ))
    .with_sync_trigger_allowed(true);
    let err = adapter.trigger_source_sync("conn_1").await.unwrap_err();
    assert!(matches!(err, SourceSyncTriggerError::Failed(_)), "{err:?}");
}

// ---------------------------------------------------------------------------
// The shared wait and trigger loops, end to end over HTTP
// ---------------------------------------------------------------------------

fn opts(timeout_ms: u64) -> SourceSyncWaitOptions {
    SourceSyncWaitOptions {
        timeout: Duration::from_millis(timeout_ms),
        poll_interval: Duration::from_millis(20),
    }
}

fn all(_: &SourceSyncState) -> bool {
    true
}

fn no_event(_: &SourceSyncTriggered) {}

async fn trigger_run(adapter: &AirbyteDiscoveryAdapter, timeout_ms: u64) -> SourceSyncReport {
    trigger_and_wait_for_source_syncs(
        adapter,
        "src__",
        &all,
        opts(timeout_ms),
        &no_event,
        std::future::pending(),
    )
    .await
    .unwrap()
    .unwrap()
}

#[tokio::test]
async fn trigger_and_wait_goes_idle_running_succeeded() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![
            jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))]),
            jobs_page(vec![
                job(2, "sync", "running", Some("2026-10-21T10:00:10Z")),
                job(1, "sync", "succeeded", Some(OLD)),
            ]),
            jobs_page(vec![
                job(2, "sync", "succeeded", Some("2026-10-21T10:30:00Z")),
                job(1, "sync", "succeeded", Some(OLD)),
            ]),
        ],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, true), 5000).await;
    assert_eq!(report.syncs.len(), 1);
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    assert!(report.syncs[0].triggered);
    assert_eq!(report.syncs[0].last_sync_at, ts("2026-10-21T10:30:00Z"));
    let sent = posts(&server).await;
    assert_eq!(sent.len(), 1, "exactly one POST for one connection");
    let body: Value = serde_json::from_slice(&sent[0].body).unwrap();
    assert_eq!(body, json!({ "connectionId": "conn_1", "jobType": "sync" }));
}

/// A failed sync after the trigger ends the connection `failed`.
#[tokio::test]
async fn trigger_and_wait_reports_a_failed_sync() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![
            jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))]),
            jobs_page(vec![job(2, "sync", "running", None)]),
            jobs_page(vec![
                job(2, "sync", "cancelled", Some("2026-10-21T10:20:00Z")),
                job(1, "sync", "succeeded", Some(OLD)),
            ]),
        ],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, true), 5000).await;
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Failed);
}

/// An end inside the 5 second margin past the server time cannot be told
/// from an earlier sync: it ends `unknown`, never `succeeded`.
#[tokio::test]
async fn an_end_inside_the_identity_margin_is_not_counted() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![
            jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))]),
            jobs_page(vec![job(2, "sync", "running", None)]),
            jobs_page(vec![job(
                2,
                "sync",
                "succeeded",
                Some("2026-10-21T10:00:05Z"),
            )]),
        ],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, true), 5000).await;
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Unknown);
}

#[tokio::test]
async fn an_inactive_connection_is_paused_and_gets_no_request() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "inactive")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))])],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, true), 1000).await;
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Paused);
    assert!(posts(&server).await.is_empty());
}

#[tokio::test]
async fn the_loop_without_the_owner_opt_in_sends_no_post() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))])],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, false), 1000).await;
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::TriggerFailed);
    assert!(!report.syncs[0].triggered);
    assert!(posts(&server).await.is_empty());
}

/// A 409 sends the connection back to waiting; once it is idle, one more
/// request starts the sync Rocky counts.
#[tokio::test]
async fn a_409_waits_for_the_running_sync_then_asks_again() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![
            jobs_page(vec![job(1, "sync", "succeeded", Some(OLD))]),
            // After the 409: the other sync is seen running, then it ends.
            jobs_page(vec![job(2, "sync", "running", None)]),
            jobs_page(vec![job(
                2,
                "sync",
                "succeeded",
                Some("2026-10-21T10:10:00Z"),
            )]),
            // After the second POST: Rocky's sync runs, then ends.
            jobs_page(vec![job(3, "sync", "running", None)]),
            jobs_page(vec![
                job(3, "sync", "succeeded", Some("2026-10-21T10:40:00Z")),
                job(2, "sync", "succeeded", Some("2026-10-21T10:10:00Z")),
            ]),
        ],
    )
    .await;
    Mock::given(method("POST"))
        .and(path("/v1/jobs"))
        .respond_with(ResponseTemplate::new(409).set_body_string("already running"))
        .up_to_n_times(1)
        .mount(&server)
        .await;
    serve_post(&server, accepted()).await;
    let report = trigger_run(&adapter(&server, true), 5000).await;
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    assert_eq!(report.syncs[0].last_sync_at, ts("2026-10-21T10:40:00Z"));
    assert_eq!(posts(&server).await.len(), 2);
}

/// `wait = true` alone: reads only, never a POST.
#[tokio::test]
async fn wait_only_follows_a_running_sync_to_its_end_without_posting() {
    let server = MockServer::start().await;
    serve_connections(&server, vec![connection("conn_1", "src__a", "active")]).await;
    serve_jobs(
        &server,
        "conn_1",
        vec![
            jobs_page(vec![
                job(2, "sync", "running", None),
                job(1, "sync", "succeeded", Some(OLD)),
            ]),
            jobs_page(vec![
                job(2, "sync", "succeeded", Some("2026-10-21T10:05:00Z")),
                job(1, "sync", "succeeded", Some(OLD)),
            ]),
        ],
    )
    .await;
    serve_post(&server, accepted()).await;
    let report = wait_for_source_syncs(
        &adapter(&server, true),
        "src__",
        &all,
        opts(5000),
        std::future::pending(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.syncs[0].outcome, SourceSyncOutcome::Succeeded);
    assert!(posts(&server).await.is_empty());
}
