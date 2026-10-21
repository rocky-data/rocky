//! The source-sync wait phase of `rocky run` (`[...discovery.sync] wait = true`).
//!
//! Runs on a replication pipeline before the run takes its state session. It
//! holds the run's cancellation token, the adapter handles the run opened
//! (the registry and the hook registry) and, when the run has an idempotency
//! key, the in-flight claim. No state download, remote lock, state store or
//! ledger is open yet, so a long wait blocks no other run's state.
//!
//! ```text
//!   discover state (GET group list)
//!        |
//!        +-- nothing syncing ---------------> carry on (no rows)
//!        +-- sync running: poll every poll_interval_secs
//!                |-- all succeeded ----------> carry on (rows in run JSON)
//!                |-- failed / timeout / error -> on_sync_failure
//!                |        fail: stop, no warehouse write, no RunRecord
//!                |        warn: log, carry on, EXCEPT a sync Rocky started
//!                |              that is not known to be finished (timeout,
//!                |              error, unknown): it may still be writing, so
//!                |              the run stops as with fail
//!                '-- signal -----------------> Interrupted (exit 130), no RunRecord
//! ```
//!
//! A phase that stops the run writes no `RunRecord`: there is no state store
//! yet to hold it. `rocky history` does not show it. The run JSON and the
//! `pipeline_start` / `pipeline_error` hooks carry the outcome.

use std::time::{Duration, Instant};

use anyhow::Result;
use rocky_core::config::{DiscoverySyncConfig, OnSyncFailure};
use rocky_core::hooks::{HookContext, HookRegistry};
use rocky_core::schema::SchemaPattern;
use rocky_core::source::DiscoveredConnector;
use rocky_core::source_sync::{
    SourceSyncOutcome, SourceSyncReport, SourceSyncState, SourceSyncTriggered,
    SourceSyncWaitOptions, trigger_and_wait_for_source_syncs, wait_for_source_syncs,
};
use rocky_core::traits::DiscoveryAdapter;
use rocky_observe::events::{PipelineEvent, global_event_bus, record_span_event};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use super::matches_filter;
use super::run::{Interrupted, RecordCustody};
use crate::output::{RunOutput, SourceSyncOutput, print_json};

/// Everything the phase reads from the run.
pub(super) struct SyncPhase<'a> {
    pub sync: &'a DiscoverySyncConfig,
    pub pipeline_name: &'a str,
    pub run_id: &'a str,
    pub schema_prefix: &'a str,
    pub pattern: &'a SchemaPattern,
    /// The parsed `--filter key=value`, if any.
    pub filter: Option<&'a (String, String)>,
    pub filter_text: &'a str,
    pub concurrency: usize,
    pub started: Instant,
    pub output_json: bool,
    pub cancel: &'a CancellationToken,
    pub hooks: &'a HookRegistry,
    /// `--no-sync-trigger`, or a resume (`--resume`, `--resume-latest`): wait
    /// only, even when the config sets `trigger`.
    pub no_sync_trigger: bool,
}

impl SyncPhase<'_> {
    /// Wait (and, with both opt-ins and no `--no-sync-trigger`, start syncs
    /// first), then decide. `Ok` carries the rows for
    /// `RunOutput::source_syncs` (empty when nothing was running or the
    /// adapter reports no sync state). `Err` stops the run before any
    /// warehouse write.
    pub(super) async fn run(self, adapter: &dyn DiscoveryAdapter) -> Result<Vec<SourceSyncOutput>> {
        let include = |s: &SourceSyncState| self.includes(s);
        let opts = SourceSyncWaitOptions {
            timeout: Duration::from_secs(self.sync.wait_timeout_secs),
            poll_interval: Duration::from_secs(self.sync.poll_interval_secs),
        };
        let waited = if self.sync.trigger && !self.no_sync_trigger {
            let announce = |t: &SourceSyncTriggered| self.announce_trigger(t);
            trigger_and_wait_for_source_syncs(
                adapter,
                self.schema_prefix,
                &include,
                opts,
                &announce,
                self.cancel.cancelled(),
            )
            .await
        } else {
            wait_for_source_syncs(
                adapter,
                self.schema_prefix,
                &include,
                opts,
                self.cancel.cancelled(),
            )
            .await
        };

        let report = match waited {
            Ok(Some(report)) => report,
            Ok(None) => {
                warn!(
                    pipeline = self.pipeline_name,
                    "[discovery.sync] wait = true, but this discovery adapter does not report \
                     sync state; running without a wait"
                );
                return Ok(Vec::new());
            }
            Err(error) => {
                let message = format!(
                    "could not read the source sync state for pipeline '{}': {error}",
                    self.pipeline_name
                );
                // No connector is known yet, so the row stands for the whole
                // status read. `on_sync_failure = "warn"` still reports it.
                let row = SourceSyncOutput {
                    id: "*".to_string(),
                    schema: String::new(),
                    outcome: SourceSyncOutcome::Error,
                    waited_secs: self.started.elapsed().as_secs(),
                    last_sync_at: None,
                    detail: Some(error.to_string()),
                    triggered: false,
                };
                return self.phase_failed(&message, vec![row]).await;
            }
        };
        let rows = rows_of(&report);
        if report.interrupted {
            self.fire_failure_hooks("interrupted by signal").await;
            self.emit_json(&rows, true);
            return Err(anyhow::Error::new(Interrupted {
                custody: RecordCustody::Lost,
            }));
        }
        if report.bad().next().is_none() {
            return Ok(rows);
        }
        let detail = report
            .bad()
            .map(|r| match &r.detail {
                Some(text) => format!("{} ({}: {text})", r.id, r.outcome.as_str()),
                None => format!("{} ({})", r.id, r.outcome.as_str()),
            })
            .collect::<Vec<_>>()
            .join(", ");
        let message = format!(
            "source sync did not finish cleanly for pipeline '{}': {detail}",
            self.pipeline_name
        );
        // A sync Rocky started and cannot call finished may still be writing
        // the tables the run would read. `warn` never covers that.
        let may_still_write = report.bad().any(|r| {
            r.triggered
                && matches!(
                    r.outcome,
                    SourceSyncOutcome::Timeout
                        | SourceSyncOutcome::Error
                        | SourceSyncOutcome::Unknown
                )
        });
        self.phase_failed_with(&message, rows, may_still_write)
            .await
    }

    /// Apply `on_sync_failure` to a failed phase. `rows` are the connector
    /// rows to report either way.
    async fn phase_failed(
        &self,
        message: &str,
        rows: Vec<SourceSyncOutput>,
    ) -> Result<Vec<SourceSyncOutput>> {
        self.phase_failed_with(message, rows, false).await
    }

    /// [`Self::phase_failed`], with `may_still_write` to stop the run whatever
    /// `on_sync_failure` says.
    async fn phase_failed_with(
        &self,
        message: &str,
        rows: Vec<SourceSyncOutput>,
        may_still_write: bool,
    ) -> Result<Vec<SourceSyncOutput>> {
        let mode = if may_still_write {
            OnSyncFailure::Fail
        } else {
            self.sync.on_sync_failure
        };
        match mode {
            OnSyncFailure::Warn => {
                warn!(
                    pipeline = self.pipeline_name,
                    "{message}; on_sync_failure = \"warn\", running on the last landed data"
                );
                Ok(rows)
            }
            OnSyncFailure::Fail => {
                self.fire_failure_hooks(message).await;
                self.emit_json(&rows, false);
                if may_still_write {
                    anyhow::bail!(
                        "{message}. Nothing was written. A sync Rocky started may still be \
                         writing, so `on_sync_failure = \"warn\"` does not apply. Run again \
                         once the sync has finished"
                    )
                }
                anyhow::bail!(
                    "{message}. Nothing was written. Set `on_sync_failure = \"warn\"` to run \
                     on the last landed data instead"
                )
            }
        }
    }

    /// One request went out: log it and emit the `source_sync_triggered`
    /// event, so a subscriber sees every vendor sync Rocky starts.
    fn announce_trigger(&self, triggered: &SourceSyncTriggered) {
        tracing::info!(
            pipeline = self.pipeline_name,
            connector = %triggered.id,
            schema = %triggered.schema,
            "started a source sync"
        );
        let event = PipelineEvent::new("source_sync_triggered")
            .with_run_id(self.run_id)
            .with_target(triggered.id.clone())
            .with_metadata("pipeline", self.pipeline_name)
            .with_metadata("schema", triggered.schema.clone())
            .with_metadata("idle_seen_at", triggered.idle_seen_at.to_rfc3339());
        record_span_event(&event);
        global_event_bus().emit(event);
    }

    fn includes(&self, state: &SourceSyncState) -> bool {
        let Some((key, value)) = self.filter else {
            return true;
        };
        let Ok(parsed) = self.pattern.parse(&state.schema) else {
            return false;
        };
        let conn = DiscoveredConnector {
            id: state.id.clone(),
            schema: state.schema.clone(),
            source_type: state.source_type.clone(),
            last_sync_at: None,
            tables: Vec::new(),
            metadata: Default::default(),
            external_object_ids: Vec::new(),
        };
        matches_filter(&conn, &parsed, key, value)
    }

    /// `pipeline_error` does not fire on a `?` exit, so the phase fires it. For
    /// a replication pipeline `pipeline_start` has not fired yet, so it goes
    /// first: a hook subscriber never sees an error without a start.
    async fn fire_failure_hooks(&self, message: &str) {
        let _ = self
            .hooks
            .fire(&HookContext::pipeline_start(
                self.run_id,
                self.pipeline_name,
            ))
            .await;
        let _ = self
            .hooks
            .fire(&HookContext::pipeline_error(
                self.run_id,
                self.pipeline_name,
                message,
            ))
            .await;
        let _ = self.hooks.wait_async_webhooks().await;
    }

    /// The run JSON for a run that stopped here. Nothing else ran, so the
    /// payload is the empty run plus `source_syncs`.
    fn emit_json(&self, rows: &[SourceSyncOutput], interrupted: bool) {
        if !self.output_json {
            return;
        }
        let mut output = RunOutput::new(
            self.filter_text.to_string(),
            u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX),
            self.concurrency,
        );
        output.interrupted = interrupted;
        output.status = rocky_core::state::RunStatus::Failure;
        output.source_syncs = rows.to_vec();
        if let Err(error) = print_json(&output) {
            warn!(%error, "could not print the run JSON for the source sync phase");
        }
        super::run::capture_run_output_for_test(self.run_id, &output);
    }
}

fn rows_of(report: &SourceSyncReport) -> Vec<SourceSyncOutput> {
    report
        .syncs
        .iter()
        .map(|r| SourceSyncOutput {
            id: r.id.clone(),
            schema: r.schema.clone(),
            outcome: r.outcome,
            waited_secs: r.waited_secs,
            last_sync_at: r.last_sync_at,
            detail: r.detail.clone(),
            triggered: r.triggered,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use chrono::{DateTime, TimeZone, Utc};
    use rocky_core::config::OnSyncFailure;
    use rocky_core::hooks::HooksConfig;
    use rocky_core::schema::PatternComponent;
    use rocky_core::source_sync::SourceSyncOutcome;

    use super::*;
    use crate::commands::run::RunTermination;
    use crate::commands::{DeferOptions, PartitionRunOptions, SkipRunOptions};
    use crate::testing::{recorder, sync_source};

    fn at(secs: i64) -> Option<DateTime<Utc>> {
        Some(Utc.timestamp_opt(1_800_000_000 + secs, 0).unwrap())
    }

    fn state(
        syncing: bool,
        ok: Option<DateTime<Utc>>,
        bad: Option<DateTime<Utc>>,
    ) -> SourceSyncState {
        SourceSyncState {
            id: "c1".into(),
            schema: "raw__acme".into(),
            source_type: "shopify".into(),
            syncing,
            paused: false,
            succeeded_at: ok,
            failed_at: bad,
            observed_at: None,
        }
    }

    /// A state read at server time `at(0)`.
    fn seen(
        syncing: bool,
        ok: Option<DateTime<Utc>>,
        bad: Option<DateTime<Utc>>,
    ) -> SourceSyncState {
        SourceSyncState {
            observed_at: at(0),
            ..state(syncing, ok, bad)
        }
    }

    fn pattern() -> SchemaPattern {
        SchemaPattern {
            prefix: "raw__".into(),
            separator: "__".into(),
            components: vec![PatternComponent::Terminal {
                name: "source".into(),
            }],
        }
    }

    /// Drive the phase alone. `poll_interval_secs = 0` is below the
    /// validated floor on purpose: the floor is a config rule, not a phase one.
    async fn phase(
        key: &str,
        on_sync_failure: OnSyncFailure,
        timeout_secs: u64,
        cancel: &CancellationToken,
        hook_log: Option<&Path>,
    ) -> Result<Vec<SourceSyncOutput>> {
        phase_with(
            key,
            on_sync_failure,
            timeout_secs,
            cancel,
            hook_log,
            false,
            false,
        )
        .await
    }

    /// [`phase`], with `trigger` from the config and `--no-sync-trigger`.
    async fn phase_with(
        key: &str,
        on_sync_failure: OnSyncFailure,
        timeout_secs: u64,
        cancel: &CancellationToken,
        hook_log: Option<&Path>,
        trigger: bool,
        no_sync_trigger: bool,
    ) -> Result<Vec<SourceSyncOutput>> {
        let sync = DiscoverySyncConfig {
            wait: true,
            trigger,
            wait_timeout_secs: timeout_secs,
            poll_interval_secs: 0,
            on_sync_failure,
        };
        let hooks = match hook_log {
            None => HookRegistry::from_config(&HooksConfig::default()),
            Some(log) => {
                let toml = format!(
                    "[[on_pipeline_start]]\ncommand = \"cat >> {log}\"\n\n\
                     [[on_pipeline_error]]\ncommand = \"cat >> {log}\"\n",
                    log = log.display()
                );
                HookRegistry::from_config(&toml::from_str(&toml).unwrap())
            }
        };
        let pattern = pattern();
        SyncPhase {
            sync: &sync,
            pipeline_name: "ingest",
            run_id: "run-test",
            schema_prefix: "raw__",
            pattern: &pattern,
            filter: None,
            filter_text: "",
            concurrency: 1,
            started: Instant::now(),
            output_json: false,
            cancel,
            hooks: &hooks,
            no_sync_trigger,
        }
        .run(sync_source(key).as_ref())
        .await
    }

    #[tokio::test]
    async fn a_waited_sync_that_succeeds_reports_one_row_and_lets_the_run_go() {
        let key = "phase-success";
        sync_source(key).script(vec![
            vec![state(true, at(0), None)],
            vec![state(false, at(60), None)],
        ]);
        let rows = phase(key, OnSyncFailure::Fail, 5, &CancellationToken::new(), None)
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].id, "c1");
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(rows[0].last_sync_at, at(60));
    }

    #[tokio::test]
    async fn nothing_syncing_means_one_poll_and_no_rows() {
        let key = "phase-idle";
        let source = sync_source(key);
        source.script(vec![vec![state(false, at(0), None)]]);
        let rows = phase(key, OnSyncFailure::Fail, 5, &CancellationToken::new(), None)
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert_eq!(source.polls(), 1);
    }

    /// `fail` stops the run, and the hooks see `pipeline_start` then
    /// `pipeline_error`, never an error alone.
    #[tokio::test]
    async fn a_failed_sync_stops_the_run_and_fires_start_then_error() {
        let key = "phase-failed-fail";
        sync_source(key).script(vec![
            vec![state(true, at(0), None)],
            vec![state(false, at(0), at(60))],
        ]);
        let dir = tempfile::tempdir().unwrap();
        let log = dir.path().join("hooks.log");
        let error = phase(
            key,
            OnSyncFailure::Fail,
            5,
            &CancellationToken::new(),
            Some(&log),
        )
        .await
        .expect_err("a failed sync fails the run");
        let text = format!("{error:#}");
        assert!(
            text.contains("c1 (failed)") && text.contains("Nothing was written"),
            "{text}"
        );
        assert!(!error.is::<Interrupted>());
        let seen = std::fs::read_to_string(&log).unwrap();
        let events: Vec<String> = serde_json::Deserializer::from_str(&seen)
            .into_iter::<serde_json::Value>()
            .map(|v| v.unwrap()["event"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(events, ["pipeline_start", "pipeline_error"], "{seen}");
    }

    /// `warn` runs on the last landed data and reports the bad row.
    #[tokio::test]
    async fn a_failed_sync_in_warn_mode_lets_the_run_go_and_keeps_the_row() {
        let key = "phase-failed-warn";
        sync_source(key).script(vec![
            vec![state(true, at(0), None)],
            vec![state(false, at(0), at(60))],
        ]);
        let rows = phase(key, OnSyncFailure::Warn, 5, &CancellationToken::new(), None)
            .await
            .expect("warn mode does not stop the run");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Failed);
    }

    /// A started sync that ends past the margin: one row, `triggered`, one
    /// request, and the `source_sync_triggered` event on the bus.
    #[tokio::test]
    async fn a_triggered_sync_reports_a_triggered_row_and_emits_an_event() {
        let key = "phase-trigger-success";
        let source = sync_source(key);
        source.script(vec![
            vec![seen(false, at(-500), None)],
            vec![seen(true, at(-500), None)],
            vec![seen(false, at(60), None)],
        ]);
        let mut events = rocky_observe::events::global_event_bus().subscribe();
        let rows = phase_with(
            key,
            OnSyncFailure::Fail,
            5,
            &CancellationToken::new(),
            None,
            true,
            false,
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].triggered);
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Succeeded);
        assert_eq!(source.triggered(), ["c1"]);
        let mut found = None;
        while let Ok(event) = events.try_recv() {
            if event.event_type == "source_sync_triggered"
                && event.run_id.as_deref() == Some("run-test")
                && event.target.as_deref() == Some("c1")
            {
                found = Some(event);
            }
        }
        let event = found.expect("a source_sync_triggered event for the request");
        assert_eq!(event.metadata["pipeline"], "ingest");
    }

    /// `--no-sync-trigger` leaves the config alone but sends no request: the
    /// phase only waits, so an idle connector costs one read and no row.
    #[tokio::test]
    async fn no_sync_trigger_waits_only() {
        let key = "phase-no-sync-trigger";
        let source = sync_source(key);
        source.script(vec![vec![seen(false, at(-500), None)]]);
        let rows = phase_with(
            key,
            OnSyncFailure::Fail,
            5,
            &CancellationToken::new(),
            None,
            true,
            true,
        )
        .await
        .unwrap();
        assert!(rows.is_empty());
        assert!(source.triggered().is_empty());
        assert_eq!(source.polls(), 1);
    }

    /// A refused key (403) fails the run with the fix in the message.
    #[tokio::test]
    async fn a_refused_key_fails_the_run_with_the_fix_named() {
        let key = "phase-trigger-forbidden";
        let source = sync_source(key);
        source.script(vec![vec![seen(false, at(-500), None)]]);
        source.forbid_triggers();
        let error = phase_with(
            key,
            OnSyncFailure::Fail,
            5,
            &CancellationToken::new(),
            None,
            true,
            false,
        )
        .await
        .expect_err("a refused key stops the run");
        let text = format!("{error:#}");
        assert!(
            text.contains("c1 (trigger_failed: this key cannot start syncs")
                && text.contains("trigger = false"),
            "{text}"
        );
        assert_eq!(source.triggered(), ["c1"]);
    }

    /// A paused connector gets no request and does not stop the run.
    #[tokio::test]
    async fn a_paused_connector_is_reported_and_does_not_stop_the_run() {
        let key = "phase-trigger-paused";
        let source = sync_source(key);
        let mut paused = seen(false, at(-500), None);
        paused.paused = true;
        source.script(vec![vec![paused]]);
        let rows = phase_with(
            key,
            OnSyncFailure::Fail,
            5,
            &CancellationToken::new(),
            None,
            true,
            false,
        )
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Paused);
        assert!(!rows[0].triggered);
        assert!(source.triggered().is_empty());
    }

    /// The first status read fails: no connector is known. `warn` still
    /// reports one row naming the error, not an empty list.
    #[tokio::test]
    async fn an_abandoned_first_poll_in_warn_mode_still_reports_a_row() {
        let key = "phase-first-poll-error-warn";
        let source = sync_source(key);
        source.script(vec![vec![state(false, at(0), None)]]);
        source.fail_after(0);
        let rows = phase(key, OnSyncFailure::Warn, 5, &CancellationToken::new(), None)
            .await
            .expect("warn mode does not stop the run");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Error);
        assert!(
            rows[0]
                .detail
                .as_deref()
                .is_some_and(|d| d.contains("boom")),
            "{:?}",
            rows[0].detail
        );
    }

    #[tokio::test]
    async fn an_abandoned_first_poll_in_fail_mode_stops_the_run() {
        let key = "phase-first-poll-error-fail";
        let source = sync_source(key);
        source.script(vec![vec![state(false, at(0), None)]]);
        source.fail_after(0);
        let error = phase(key, OnSyncFailure::Fail, 5, &CancellationToken::new(), None)
            .await
            .expect_err("a status outage at the first poll stops a wait = true run");
        let text = format!("{error:#}");
        assert!(
            text.contains("could not read the source sync state") && text.contains("boom"),
            "{text}"
        );
    }

    /// Three failed polls in a row abandon the wait. The connector still gets
    /// a row, with the error.
    #[tokio::test]
    async fn an_abandoned_wait_in_warn_mode_names_the_connector_and_the_error() {
        let key = "phase-abandoned-warn";
        let source = sync_source(key);
        source.script(vec![vec![state(true, at(0), None)]]);
        source.fail_after(1);
        let rows = phase(key, OnSyncFailure::Warn, 5, &CancellationToken::new(), None)
            .await
            .expect("warn mode does not stop the run");
        assert_eq!(rows.len(), 1, "{rows:?}");
        assert_eq!(rows[0].id, "c1");
        assert_eq!(rows[0].outcome, SourceSyncOutcome::Error);
        assert!(
            rows[0]
                .detail
                .as_deref()
                .is_some_and(|d| d.contains("boom"))
        );
    }

    #[tokio::test]
    async fn a_signal_during_the_wait_is_interrupted_with_no_record() {
        let key = "phase-cancel";
        sync_source(key).script(vec![vec![state(true, at(0), None)]]);
        let cancel = CancellationToken::new();
        let trigger = cancel.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            trigger.cancel();
        });
        let error = phase(key, OnSyncFailure::Warn, 600, &cancel, None)
            .await
            .expect_err("a signal ends the wait whatever on_sync_failure says");
        let interrupted = error.downcast_ref::<Interrupted>().expect("Interrupted");
        assert_eq!(interrupted.custody, RecordCustody::Lost);
    }

    const CONFIG: &str = r#"
[adapter.default]
type = "recording"
path = "__KEY__"

[adapter.src]
type = "test-sync-source"
path = "__KEY__"

[pipeline.ingest]
strategy = "full_refresh"

[pipeline.ingest.source.discovery]
adapter = "src"

[pipeline.ingest.source.discovery.sync]
wait = true
trigger = __TRIGGER__
wait_timeout_secs = __TIMEOUT__
poll_interval_secs = 10
on_sync_failure = "__MODE__"

[pipeline.ingest.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.ingest.target]
adapter = "default"
catalog_template = "fixture"
schema_template = "staging"

[pipeline.ingest.target.governance]
auto_create_catalogs = true
auto_create_schemas = true
"#;

    async fn drive(
        dir: &Path,
        key: &str,
        mode: &str,
        timeout_secs: u64,
        idempotency_key: Option<&str>,
    ) -> Result<RunTermination> {
        drive_with(
            dir,
            key,
            mode,
            timeout_secs,
            idempotency_key,
            false,
            &SkipRunOptions::default(),
            (None, false),
        )
        .await
    }

    /// [`drive`], with the config's `trigger` and the run's skip overlay.
    #[allow(clippy::too_many_arguments)]
    async fn drive_with(
        dir: &Path,
        key: &str,
        mode: &str,
        timeout_secs: u64,
        idempotency_key: Option<&str>,
        trigger: bool,
        skip_opts: &SkipRunOptions,
        resume: (Option<&str>, bool),
    ) -> Result<RunTermination> {
        let config_path = dir.join("rocky.toml");
        std::fs::write(
            &config_path,
            CONFIG
                .replace("__KEY__", key)
                .replace("__MODE__", mode)
                .replace("__TRIGGER__", &trigger.to_string())
                .replace("__TIMEOUT__", &timeout_secs.to_string()),
        )
        .unwrap();
        crate::commands::run::run(
            &config_path,
            std::sync::Arc::new(
                rocky_core::config::load_rocky_config_fingerprinted(&config_path).unwrap(),
            ),
            None,
            None,
            &dir.join("state.redb"),
            None,
            false,
            None,
            false,
            resume.0,
            resume.1,
            None,
            &PartitionRunOptions::default(),
            None,
            None,
            idempotency_key,
            None,
            &DeferOptions::default(),
            skip_opts,
            &rocky_core::run_vars::RunVars::new(),
            None,
            None,
            false,
            None,
            &rocky_core::config::PrincipalRef::unnamed(),
        )
        .await
    }

    fn still_syncing() -> Vec<Vec<SourceSyncState>> {
        vec![vec![state(true, at(0), None)]]
    }

    /// The recording warehouse refuses reads, so a run that reaches it ends in
    /// a table failure. The sync phase is judged by what came before: whether
    /// the warehouse was touched at all, and whether the error is the phase's.
    fn is_sync_phase_error(result: &Result<RunTermination>) -> bool {
        result
            .as_ref()
            .err()
            .is_some_and(|e| format!("{e:#}").contains("source sync did not finish"))
    }

    /// The run-path proof. A sync still running at `wait_timeout_secs` fails
    /// the run before the state session and before any warehouse call. The
    /// control (the same fixture with an idle source) reaches the warehouse,
    /// so the empty log is not the adapter never being reached.
    #[tokio::test]
    async fn a_failed_sync_phase_writes_nothing_to_the_warehouse() {
        let tmp = tempfile::tempdir().unwrap();

        let control = tmp.path().join("control");
        std::fs::create_dir_all(&control).unwrap();
        let control_key = control.join("calls").display().to_string();
        sync_source(&control_key).script(vec![vec![state(false, at(0), None)]]);
        let result = drive(&control, &control_key, "fail", 1, None).await;
        let control_log = recorder(&control_key);
        assert!(
            !control_log.statements().is_empty(),
            "control: the run must reach the warehouse. result: {result:?}, calls: {:?}",
            control_log.calls()
        );
        assert!(!is_sync_phase_error(&result), "control: {result:?}");
        assert!(control.join("state.redb").exists(), "control: state opened");

        let treatment = tmp.path().join("treatment");
        std::fs::create_dir_all(&treatment).unwrap();
        let key = treatment.join("calls").display().to_string();
        let source = sync_source(&key);
        source.script(still_syncing());
        let error = drive(&treatment, &key, "fail", 1, None)
            .await
            .expect_err("a sync still running at the timeout fails the run");
        assert!(format!("{error:#}").contains("c1 (timeout)"), "{error:#}");
        let log = recorder(&key);
        assert!(
            log.calls().is_empty(),
            "no warehouse call may precede the sync phase: {:?}",
            log.calls()
        );
        assert!(
            !treatment.join("state.redb").exists(),
            "the wait runs before the state store opens"
        );
    }

    /// A failed phase releases the idempotency claim, so a retry with the same
    /// key runs instead of being deflected as in flight.
    #[tokio::test]
    async fn a_failed_sync_phase_releases_the_idempotency_claim() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        let source = sync_source(&key);
        source.script(still_syncing());
        let error = drive(dir.path(), &key, "fail", 1, Some("k1"))
            .await
            .expect_err("a sync still running at the timeout fails the run");
        assert!(format!("{error:#}").contains("c1 (timeout)"), "{error:#}");
        assert!(recorder(&key).calls().is_empty());

        source.script(vec![vec![state(false, at(0), None)]]);
        let retry = drive(dir.path(), &key, "fail", 1, Some("k1")).await;
        assert!(
            !matches!(retry, Ok(RunTermination::SkippedInFlight { .. })),
            "a failed sync phase must not leave the key in flight: {retry:?}"
        );
        assert!(!is_sync_phase_error(&retry), "{retry:?}");
        assert!(!recorder(&key).statements().is_empty(), "the retry ran");
    }

    /// The run path carries the config's `trigger` to the phase: a refused
    /// key (403) stops the run before the state store opens and before any
    /// warehouse call, with the request logged once.
    #[tokio::test]
    async fn run_starts_a_sync_when_the_config_says_trigger_and_stops_on_a_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        let source = sync_source(&key);
        source.script(vec![vec![seen(false, at(-500), None)]]);
        source.forbid_triggers();
        let error = drive_with(
            dir.path(),
            &key,
            "fail",
            5,
            None,
            true,
            &SkipRunOptions::default(),
            (None, false),
        )
        .await
        .expect_err("a refused key fails the run");
        assert!(
            format!("{error:#}").contains("c1 (trigger_failed: this key cannot start syncs"),
            "{error:#}"
        );
        assert_eq!(source.triggered(), ["c1"]);
        assert!(recorder(&key).calls().is_empty());
        assert!(!dir.path().join("state.redb").exists());
    }

    /// `--no-sync-trigger` reaches the phase through `SkipRunOptions`: the
    /// same config sends no request and the run goes on to the warehouse.
    #[tokio::test]
    async fn no_sync_trigger_on_the_run_path_sends_no_request() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        let source = sync_source(&key);
        source.script(vec![vec![seen(false, at(-500), None)]]);
        source.forbid_triggers();
        let skip = SkipRunOptions {
            no_sync_trigger: true,
            ..SkipRunOptions::default()
        };
        let result = drive_with(
            dir.path(),
            &key,
            "fail",
            5,
            None,
            true,
            &skip,
            (None, false),
        )
        .await;
        assert!(
            !is_sync_phase_error(&result),
            "no request, no refusal: {result:?}"
        );
        assert!(source.triggered().is_empty());
        assert!(
            !recorder(&key).statements().is_empty(),
            "the run reached the warehouse"
        );
    }

    /// A resume never starts a vendor sync, whatever the config says: the same
    /// idle source and the same `trigger = true` that start a sync on a fresh
    /// run send no request under `--resume-latest` or `--resume <id>`. (The
    /// resume itself fails later, with no earlier run to resume; the phase
    /// comes first.)
    #[tokio::test]
    async fn a_resume_sends_no_sync_request() {
        for resume in [(None, true), (Some("run-earlier"), false)] {
            let dir = tempfile::tempdir().unwrap();
            let key = dir.path().join("calls").display().to_string();
            let source = sync_source(&key);
            source.script(vec![vec![seen(false, at(-500), None)]]);
            source.forbid_triggers();
            let result = drive_with(
                dir.path(),
                &key,
                "fail",
                5,
                None,
                true,
                &SkipRunOptions::default(),
                resume,
            )
            .await;
            assert!(
                !is_sync_phase_error(&result),
                "a resume must not reach the trigger: {result:?}"
            );
            assert!(
                source.triggered().is_empty(),
                "a resume sent a sync request: {:?}",
                source.triggered()
            );
        }
    }

    /// `on_sync_failure = "warn"` does not cover a sync Rocky started and
    /// cannot call finished: the request went out, no read ever shows the sync
    /// end (`timeout`), and the run stops before the warehouse. The control is
    /// a wait-only run with the same script, which `warn` lets through.
    #[tokio::test]
    async fn warn_does_not_cover_a_triggered_sync_that_may_still_be_writing() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        let source = sync_source(&key);
        source.script(vec![vec![seen(false, at(-500), None)]]);
        let error = drive_with(
            dir.path(),
            &key,
            "warn",
            1,
            None,
            true,
            &SkipRunOptions::default(),
            (None, false),
        )
        .await
        .expect_err("a triggered sync that timed out stops the run despite warn");
        let text = format!("{error:#}");
        assert!(text.contains("c1 (timeout)"), "{text}");
        assert!(text.contains("may still be writing"), "{text}");
        assert_eq!(source.triggered(), ["c1"]);
        assert!(recorder(&key).calls().is_empty(), "nothing was written");

        // Control: the same timeout on a wait-only row is what `warn` is for.
        let control = tempfile::tempdir().unwrap();
        let control_key = control.path().join("calls").display().to_string();
        sync_source(&control_key).script(still_syncing());
        let result = drive(control.path(), &control_key, "warn", 1, None).await;
        assert!(!is_sync_phase_error(&result), "{result:?}");
    }

    #[tokio::test]
    async fn warn_mode_runs_on_the_last_landed_data_after_a_timeout() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        sync_source(&key).script(still_syncing());
        let result = drive(dir.path(), &key, "warn", 1, None).await;
        assert!(
            !is_sync_phase_error(&result),
            "warn mode carries on: {result:?}"
        );
        assert!(
            !recorder(&key).statements().is_empty(),
            "the run reached the warehouse"
        );
    }

    #[tokio::test]
    async fn a_signal_in_the_wait_exits_interrupted_before_any_state_or_write() {
        let dir = tempfile::tempdir().unwrap();
        let key = dir.path().join("calls").display().to_string();
        sync_source(&key).script(still_syncing());
        let token = CancellationToken::new();
        let trigger = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            trigger.cancel();
        });
        let started = Instant::now();
        // A long wait: only the signal can end it inside the test's time.
        let error =
            crate::commands::run_cancel::scope(token, drive(dir.path(), &key, "fail", 600, None))
                .await
                .expect_err("a signal ends the wait");
        assert!(error.is::<Interrupted>(), "{error:#}");
        assert!(started.elapsed() < Duration::from_secs(30));
        assert!(recorder(&key).calls().is_empty());
        assert!(!dir.path().join("state.redb").exists());
    }
}
