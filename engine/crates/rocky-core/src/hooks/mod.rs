pub mod presets;
pub mod template;
pub mod webhook;

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

use webhook::{AsyncWebhookHandle, WebhookConfig};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug, Error)]
pub enum HookError {
    #[error("hook command failed: {command} (exit code {exit_code})")]
    CommandFailed {
        command: String,
        exit_code: i32,
        stderr: String,
    },

    #[error("hook command timed out after {timeout_ms}ms: {command}")]
    Timeout { command: String, timeout_ms: u64 },

    #[error("hook aborted pipeline: {reason}")]
    Aborted { reason: String },

    #[error("hook I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("hook serialization error: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("webhook error: {0}")]
    Webhook(#[from] webhook::WebhookError),
}

// ---------------------------------------------------------------------------
// HookEvent — the 18 lifecycle points
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum HookEvent {
    // Pipeline lifecycle
    PipelineStart,
    DiscoverComplete,
    CompileComplete,
    PipelineComplete,
    PipelineError,

    // Table lifecycle (per table, parallel)
    BeforeMaterialize,
    AfterMaterialize,
    MaterializeError,

    // Model lifecycle (per model, DAG order)
    BeforeModelRun,
    AfterModelRun,
    ModelError,

    // Check lifecycle
    BeforeChecks,
    CheckResult,
    AfterChecks,

    // State lifecycle
    DriftDetected,
    AnomalyDetected,
    StateSynced,

    // Budget lifecycle (Arc 2)
    BudgetBreach,
}

impl HookEvent {
    /// Returns the config key for this event (e.g., `on_pipeline_start`).
    pub fn config_key(&self) -> &'static str {
        match self {
            Self::PipelineStart => "on_pipeline_start",
            Self::DiscoverComplete => "on_discover_complete",
            Self::CompileComplete => "on_compile_complete",
            Self::PipelineComplete => "on_pipeline_complete",
            Self::PipelineError => "on_pipeline_error",
            Self::BeforeMaterialize => "on_before_materialize",
            Self::AfterMaterialize => "on_after_materialize",
            Self::MaterializeError => "on_materialize_error",
            Self::BeforeModelRun => "on_before_model_run",
            Self::AfterModelRun => "on_after_model_run",
            Self::ModelError => "on_model_error",
            Self::BeforeChecks => "on_before_checks",
            Self::CheckResult => "on_check_result",
            Self::AfterChecks => "on_after_checks",
            Self::DriftDetected => "on_drift_detected",
            Self::AnomalyDetected => "on_anomaly_detected",
            Self::StateSynced => "on_state_synced",
            Self::BudgetBreach => "on_budget_breach",
        }
    }

    /// Parses a config key like `on_pipeline_start` into a HookEvent.
    pub fn from_config_key(key: &str) -> Option<Self> {
        match key {
            "on_pipeline_start" => Some(Self::PipelineStart),
            "on_discover_complete" => Some(Self::DiscoverComplete),
            "on_compile_complete" => Some(Self::CompileComplete),
            "on_pipeline_complete" => Some(Self::PipelineComplete),
            "on_pipeline_error" => Some(Self::PipelineError),
            "on_before_materialize" => Some(Self::BeforeMaterialize),
            "on_after_materialize" => Some(Self::AfterMaterialize),
            "on_materialize_error" => Some(Self::MaterializeError),
            "on_before_model_run" => Some(Self::BeforeModelRun),
            "on_after_model_run" => Some(Self::AfterModelRun),
            "on_model_error" => Some(Self::ModelError),
            "on_before_checks" => Some(Self::BeforeChecks),
            "on_check_result" => Some(Self::CheckResult),
            "on_after_checks" => Some(Self::AfterChecks),
            "on_drift_detected" => Some(Self::DriftDetected),
            "on_anomaly_detected" => Some(Self::AnomalyDetected),
            "on_state_synced" => Some(Self::StateSynced),
            "on_budget_breach" => Some(Self::BudgetBreach),
            _ => None,
        }
    }

    /// Returns all known hook events.
    pub fn all() -> &'static [HookEvent] {
        &[
            Self::PipelineStart,
            Self::DiscoverComplete,
            Self::CompileComplete,
            Self::PipelineComplete,
            Self::PipelineError,
            Self::BeforeMaterialize,
            Self::AfterMaterialize,
            Self::MaterializeError,
            Self::BeforeModelRun,
            Self::AfterModelRun,
            Self::ModelError,
            Self::BeforeChecks,
            Self::CheckResult,
            Self::AfterChecks,
            Self::DriftDetected,
            Self::AnomalyDetected,
            Self::StateSynced,
            Self::BudgetBreach,
        ]
    }
}

impl fmt::Display for HookEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.config_key())
    }
}

// ---------------------------------------------------------------------------
// FailureAction — what to do when a hook command fails
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FailureAction {
    /// Stop pipeline immediately.
    Abort,
    /// Log warning, continue execution.
    #[default]
    Warn,
    /// Silent continue.
    Ignore,
}

// ---------------------------------------------------------------------------
// HookConfig — per-hook configuration
// ---------------------------------------------------------------------------

fn default_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct HookConfig {
    /// Shell command to execute (passed to `sh -c`).
    pub command: String,

    /// Maximum time to wait for the hook to complete.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// What to do if the hook command exits non-zero or times out.
    #[serde(default)]
    pub on_failure: FailureAction,

    /// Extra environment variables passed to the hook process.
    #[serde(default)]
    pub env: HashMap<String, String>,
}

// ---------------------------------------------------------------------------
// HookContext — the JSON payload piped to hooks on stdin
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookContext {
    pub event: HookEvent,
    pub run_id: String,
    pub pipeline: String,
    pub timestamp: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl HookContext {
    fn new(event: HookEvent, run_id: &str, pipeline: &str) -> Self {
        Self {
            event,
            run_id: run_id.to_string(),
            pipeline: pipeline.to_string(),
            timestamp: Utc::now(),
            model: None,
            table: None,
            error: None,
            duration_ms: None,
            metadata: HashMap::new(),
        }
    }

    // -- Pipeline lifecycle builders --

    pub fn pipeline_start(run_id: &str, pipeline: &str) -> Self {
        Self::new(HookEvent::PipelineStart, run_id, pipeline)
    }

    pub fn discover_complete(run_id: &str, pipeline: &str, connector_count: usize) -> Self {
        let mut ctx = Self::new(HookEvent::DiscoverComplete, run_id, pipeline);
        ctx.metadata.insert(
            "connector_count".to_string(),
            serde_json::Value::Number(connector_count.into()),
        );
        ctx
    }

    pub fn compile_complete(run_id: &str, pipeline: &str, model_count: usize) -> Self {
        let mut ctx = Self::new(HookEvent::CompileComplete, run_id, pipeline);
        ctx.metadata.insert(
            "model_count".to_string(),
            serde_json::Value::Number(model_count.into()),
        );
        ctx
    }

    pub fn pipeline_complete(
        run_id: &str,
        pipeline: &str,
        duration_ms: u64,
        table_count: usize,
    ) -> Self {
        let mut ctx = Self::new(HookEvent::PipelineComplete, run_id, pipeline);
        ctx.duration_ms = Some(duration_ms);
        ctx.metadata.insert(
            "table_count".to_string(),
            serde_json::Value::Number(table_count.into()),
        );
        ctx
    }

    pub fn pipeline_error(run_id: &str, pipeline: &str, error: &str) -> Self {
        let mut ctx = Self::new(HookEvent::PipelineError, run_id, pipeline);
        ctx.error = Some(error.to_string());
        ctx
    }

    // -- Table lifecycle builders --

    pub fn before_materialize(run_id: &str, pipeline: &str, table: &str) -> Self {
        let mut ctx = Self::new(HookEvent::BeforeMaterialize, run_id, pipeline);
        ctx.table = Some(table.to_string());
        ctx
    }

    pub fn after_materialize(
        run_id: &str,
        pipeline: &str,
        table: &str,
        duration_ms: u64,
        row_count: Option<u64>,
    ) -> Self {
        let mut ctx = Self::new(HookEvent::AfterMaterialize, run_id, pipeline);
        ctx.table = Some(table.to_string());
        ctx.duration_ms = Some(duration_ms);
        if let Some(rows) = row_count {
            ctx.metadata.insert(
                "row_count".to_string(),
                serde_json::Value::Number(rows.into()),
            );
        }
        ctx
    }

    pub fn materialize_error(run_id: &str, pipeline: &str, table: &str, error: &str) -> Self {
        let mut ctx = Self::new(HookEvent::MaterializeError, run_id, pipeline);
        ctx.table = Some(table.to_string());
        ctx.error = Some(error.to_string());
        ctx
    }

    // -- Model lifecycle builders --

    pub fn before_model_run(run_id: &str, pipeline: &str, model: &str) -> Self {
        let mut ctx = Self::new(HookEvent::BeforeModelRun, run_id, pipeline);
        ctx.model = Some(model.to_string());
        ctx
    }

    pub fn after_model_run(run_id: &str, pipeline: &str, model: &str, duration_ms: u64) -> Self {
        let mut ctx = Self::new(HookEvent::AfterModelRun, run_id, pipeline);
        ctx.model = Some(model.to_string());
        ctx.duration_ms = Some(duration_ms);
        ctx
    }

    pub fn model_error(run_id: &str, pipeline: &str, model: &str, error: &str) -> Self {
        let mut ctx = Self::new(HookEvent::ModelError, run_id, pipeline);
        ctx.model = Some(model.to_string());
        ctx.error = Some(error.to_string());
        ctx
    }

    // -- Check lifecycle builders --

    pub fn before_checks(run_id: &str, pipeline: &str, table_count: usize) -> Self {
        let mut ctx = Self::new(HookEvent::BeforeChecks, run_id, pipeline);
        ctx.metadata.insert(
            "table_count".to_string(),
            serde_json::Value::Number(table_count.into()),
        );
        ctx
    }

    pub fn check_result(run_id: &str, pipeline: &str, check_name: &str, passed: bool) -> Self {
        let mut ctx = Self::new(HookEvent::CheckResult, run_id, pipeline);
        ctx.metadata.insert(
            "check_name".to_string(),
            serde_json::Value::String(check_name.to_string()),
        );
        ctx.metadata
            .insert("passed".to_string(), serde_json::Value::Bool(passed));
        ctx
    }

    pub fn after_checks(
        run_id: &str,
        pipeline: &str,
        total: usize,
        passed: usize,
        failed: usize,
    ) -> Self {
        let mut ctx = Self::new(HookEvent::AfterChecks, run_id, pipeline);
        ctx.metadata
            .insert("total".to_string(), serde_json::Value::Number(total.into()));
        ctx.metadata.insert(
            "passed".to_string(),
            serde_json::Value::Number(passed.into()),
        );
        ctx.metadata.insert(
            "failed".to_string(),
            serde_json::Value::Number(failed.into()),
        );
        ctx
    }

    // -- State lifecycle builders --

    pub fn drift_detected(run_id: &str, pipeline: &str, table: &str, columns: &[String]) -> Self {
        let mut ctx = Self::new(HookEvent::DriftDetected, run_id, pipeline);
        ctx.table = Some(table.to_string());
        ctx.metadata.insert(
            "drifted_columns".to_string(),
            serde_json::Value::Array(
                columns
                    .iter()
                    .map(|c| serde_json::Value::String(c.clone()))
                    .collect(),
            ),
        );
        ctx
    }

    pub fn anomaly_detected(run_id: &str, pipeline: &str, table: &str, description: &str) -> Self {
        let mut ctx = Self::new(HookEvent::AnomalyDetected, run_id, pipeline);
        ctx.table = Some(table.to_string());
        ctx.metadata.insert(
            "description".to_string(),
            serde_json::Value::String(description.to_string()),
        );
        ctx
    }

    pub fn state_synced(run_id: &str, pipeline: &str) -> Self {
        Self::new(HookEvent::StateSynced, run_id, pipeline)
    }

    // -- Budget lifecycle builders (Arc 2) --

    /// Build a [`HookContext`] for a run-level budget breach.
    ///
    /// `limit_type` is the stable tag string (e.g. `"max_usd"`,
    /// `"max_duration_ms"`) that
    /// [`crate::config::BudgetLimitType::as_str`] emits. `limit` is the
    /// configured cap; `actual` is what the run observed. Both ride on
    /// `metadata` so the existing shell-hook template engine can
    /// interpolate them (`{{metadata.limit}}`, `{{metadata.actual}}`).
    pub fn budget_breach(
        run_id: &str,
        pipeline: &str,
        limit_type: &str,
        limit: f64,
        actual: f64,
    ) -> Self {
        let mut ctx = Self::new(HookEvent::BudgetBreach, run_id, pipeline);
        ctx.metadata.insert(
            "limit_type".to_string(),
            serde_json::Value::String(limit_type.to_string()),
        );
        if let Some(limit_val) = serde_json::Number::from_f64(limit) {
            ctx.metadata
                .insert("limit".to_string(), serde_json::Value::Number(limit_val));
        }
        if let Some(actual_val) = serde_json::Number::from_f64(actual) {
            ctx.metadata
                .insert("actual".to_string(), serde_json::Value::Number(actual_val));
        }
        ctx
    }

    /// Creates a synthetic test context for `rocky hooks test`.
    pub fn synthetic(event: HookEvent, pipeline: &str) -> Self {
        let mut ctx = Self::new(event, "test-run-id", pipeline);
        ctx.table = Some("catalog.schema.table".to_string());
        ctx.model = Some("example_model".to_string());
        ctx.duration_ms = Some(1234);
        ctx.metadata
            .insert("test".to_string(), serde_json::Value::Bool(true));
        ctx
    }
}

// ---------------------------------------------------------------------------
// HookResult
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HookResult {
    Continue,
    Abort { reason: String },
}

// ---------------------------------------------------------------------------
// HooksConfig — TOML deserialization
// ---------------------------------------------------------------------------

/// The `[hook]` section from rocky.toml.
///
/// Each key is an event name like `on_pipeline_start`. The value can be either
/// a single hook config (TOML table) or an array of hook configs (TOML array of tables).
///
/// Webhook entries live under `[hook.webhooks.on_<event>]` or `[[hook.webhooks.on_<event>]]`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, schemars::JsonSchema)]
pub struct HooksConfig {
    /// Webhook configurations keyed by event name.
    #[serde(default)]
    pub webhooks: HashMap<String, WebhookConfigOrList>,

    /// Shell hook configurations keyed by event name (flattened for backward compat).
    #[serde(flatten)]
    pub hooks: HashMap<String, HookConfigOrList>,
}

/// Supports both single-webhook and multi-webhook syntax per event.
///
/// Single: `[hook.webhooks.on_pipeline_start]`
/// Multiple: `[[hook.webhooks.on_pipeline_start]]`
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum WebhookConfigOrList {
    Single(WebhookConfig),
    Multiple(Vec<WebhookConfig>),
}

/// Supports both single-hook and multi-hook syntax per event.
///
/// Single: `[hook.on_pipeline_start]`
/// Multiple: `[[hook.on_after_checks]]`
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(untagged)]
pub enum HookConfigOrList {
    Single(HookConfig),
    Multiple(Vec<HookConfig>),
}

// ---------------------------------------------------------------------------
// HookRegistry — runtime hook lookup + executor
// ---------------------------------------------------------------------------

/// Summary of async webhook outcomes, produced by
/// [`HookRegistry::wait_async_webhooks`] at pipeline shutdown.
#[derive(Debug, Default, Clone)]
pub struct AsyncWebhookSummary {
    pub total: usize,
    pub succeeded: usize,
    pub failed: usize,
    /// `(url, error message)` pairs for failed deliveries, in the order they
    /// were spawned.
    pub failures: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub struct HookRegistry {
    hooks: HashMap<HookEvent, Vec<HookConfig>>,
    webhooks: HashMap<HookEvent, Vec<WebhookConfig>>,
    http: reqwest::Client,
    /// Handles of async webhooks spawned via `fire` — drained by
    /// `wait_async_webhooks` at pipeline end.
    async_webhook_handles: Arc<Mutex<Vec<AsyncWebhookHandle>>>,
}

impl HookRegistry {
    /// Creates a registry with no hooks configured.
    pub fn empty() -> Self {
        Self {
            hooks: HashMap::new(),
            webhooks: HashMap::new(),
            http: build_http_client(),
            async_webhook_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Builds a HookRegistry from the TOML config's `[hooks]` section.
    ///
    /// Unknown event keys are logged as warnings and ignored.
    /// Webhook presets are resolved during construction.
    pub fn from_config(config: &HooksConfig) -> Self {
        let mut hooks: HashMap<HookEvent, Vec<HookConfig>> = HashMap::new();
        let mut webhooks: HashMap<HookEvent, Vec<WebhookConfig>> = HashMap::new();

        // Parse shell hooks
        for (key, value) in &config.hooks {
            let Some(event) = HookEvent::from_config_key(key) else {
                warn!(key = %key, "unknown hook event in config, ignoring");
                continue;
            };

            let configs = match value {
                HookConfigOrList::Single(c) => vec![c.clone()],
                HookConfigOrList::Multiple(cs) => cs.clone(),
            };

            hooks.entry(event).or_default().extend(configs);
        }

        // Parse webhooks
        for (key, value) in &config.webhooks {
            let Some(event) = HookEvent::from_config_key(key) else {
                warn!(key = %key, "unknown webhook event in config, ignoring");
                continue;
            };

            let configs = match value {
                WebhookConfigOrList::Single(c) => vec![c.clone()],
                WebhookConfigOrList::Multiple(cs) => cs.clone(),
            };

            // Resolve presets
            let resolved: Vec<WebhookConfig> = configs
                .into_iter()
                .map(|wh| {
                    if let Some(ref preset_name) = wh.preset {
                        match presets::resolve_preset(preset_name, &wh) {
                            Ok(resolved) => resolved,
                            Err(err) => {
                                warn!(preset = %preset_name, error = %err, "failed to resolve webhook preset, using config as-is");
                                wh
                            }
                        }
                    } else {
                        wh
                    }
                })
                .collect();

            // Validate HTTP methods at load time. An unparseable method used
            // to silently fall through to POST at request time
            // (`unwrap_or(Method::POST)`); surface it now with the bogus value
            // + URL so the operator sees the misconfiguration instead of
            // wondering why their PUT hook is firing as a POST.
            for wh in &resolved {
                if wh.method.parse::<reqwest::Method>().is_err() {
                    warn!(
                        webhook = %wh.url,
                        method = %wh.method,
                        event = %key,
                        "invalid HTTP method in webhook config; will default to POST at runtime",
                    );
                }
            }

            webhooks.entry(event).or_default().extend(resolved);
        }

        Self {
            hooks,
            webhooks,
            http: build_http_client(),
            async_webhook_handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Returns true if any hooks (shell or webhook) are registered for this event.
    pub fn has_hooks(&self, event: &HookEvent) -> bool {
        self.hooks.get(event).is_some_and(|h| !h.is_empty())
            || self.webhooks.get(event).is_some_and(|w| !w.is_empty())
    }

    /// Returns the shell hooks registered for this event (empty slice if none).
    pub fn hooks_for(&self, event: &HookEvent) -> &[HookConfig] {
        self.hooks.get(event).map_or(&[], |v| v.as_slice())
    }

    /// Returns the webhooks registered for this event (empty slice if none).
    pub fn webhooks_for(&self, event: &HookEvent) -> &[WebhookConfig] {
        self.webhooks.get(event).map_or(&[], |v| v.as_slice())
    }

    /// Returns an iterator over all registered shell (event, hooks) pairs.
    pub fn all_hooks(&self) -> impl Iterator<Item = (&HookEvent, &Vec<HookConfig>)> {
        self.hooks.iter()
    }

    /// Returns an iterator over all registered webhook (event, webhooks) pairs.
    pub fn all_webhooks(&self) -> impl Iterator<Item = (&HookEvent, &Vec<WebhookConfig>)> {
        self.webhooks.iter()
    }

    /// Returns the total number of hooks (shell + webhooks) registered across all events.
    pub fn total_hook_count(&self) -> usize {
        let shell_count: usize = self.hooks.values().map(std::vec::Vec::len).sum();
        let webhook_count: usize = self.webhooks.values().map(std::vec::Vec::len).sum();
        shell_count + webhook_count
    }

    /// Returns the total number of shell hooks registered.
    pub fn shell_hook_count(&self) -> usize {
        self.hooks.values().map(std::vec::Vec::len).sum()
    }

    /// Returns the total number of webhooks registered.
    pub fn webhook_count(&self) -> usize {
        self.webhooks.values().map(std::vec::Vec::len).sum()
    }

    /// Fires all hooks registered for the given context's event.
    ///
    /// Execution order:
    /// 1. Shell hooks fire first, sequentially. If any shell hook with `on_failure = Abort`
    ///    fails, remaining hooks (shell and webhook) are skipped.
    /// 2. Webhooks fire after shell hooks. Async webhooks are spawned and not awaited.
    ///    Sync webhooks are awaited with their configured timeout.
    pub async fn fire(&self, ctx: &HookContext) -> Result<HookResult, HookError> {
        let shell_hooks = self.hooks_for(&ctx.event);
        let wh = self.webhooks_for(&ctx.event);

        if shell_hooks.is_empty() && wh.is_empty() {
            return Ok(HookResult::Continue);
        }

        // --- Phase 1: Shell hooks ---
        if !shell_hooks.is_empty() {
            let json = serde_json::to_string_pretty(ctx)?;
            debug!(event = %ctx.event, hook_count = shell_hooks.len(), "firing shell hooks");

            for hook in shell_hooks {
                let result = execute_hook(hook, &json).await;
                match result {
                    Ok(()) => {
                        info!(event = %ctx.event, command = %hook.command, "hook completed successfully");
                    }
                    Err(e) => match hook.on_failure {
                        FailureAction::Abort => {
                            let reason = format!(
                                "hook '{}' for event {} failed: {}",
                                hook.command, ctx.event, e
                            );
                            warn!(event = %ctx.event, command = %hook.command, error = %e, "hook failed, aborting");
                            return Ok(HookResult::Abort { reason });
                        }
                        FailureAction::Warn => {
                            warn!(event = %ctx.event, command = %hook.command, error = %e, "hook failed, continuing");
                        }
                        FailureAction::Ignore => {
                            debug!(event = %ctx.event, command = %hook.command, error = %e, "hook failed, ignoring");
                        }
                    },
                }
            }
        }

        // --- Phase 2: Webhooks ---
        if !wh.is_empty() {
            debug!(event = %ctx.event, webhook_count = wh.len(), "firing webhooks");

            for webhook_config in wh {
                let result = webhook::fire_webhook(webhook_config, ctx, &self.http).await;
                match result {
                    Ok((HookResult::Continue, async_handle)) => {
                        if let Some(handle) = async_handle {
                            self.track_async_handle(handle);
                        }
                    }
                    Ok((HookResult::Abort { reason }, _)) => {
                        return Ok(HookResult::Abort { reason });
                    }
                    Err(e) => match webhook_config.on_failure {
                        FailureAction::Abort => {
                            let reason = format!(
                                "webhook '{}' for event {} failed: {}",
                                webhook_config.url, ctx.event, e
                            );
                            warn!(event = %ctx.event, webhook = %webhook_config.url, error = %e, "webhook failed, aborting");
                            return Ok(HookResult::Abort { reason });
                        }
                        FailureAction::Warn => {
                            warn!(event = %ctx.event, webhook = %webhook_config.url, error = %e, "webhook failed, continuing");
                        }
                        FailureAction::Ignore => {
                            debug!(event = %ctx.event, webhook = %webhook_config.url, error = %e, "webhook failed, ignoring");
                        }
                    },
                }
            }
        }

        Ok(HookResult::Continue)
    }

    /// Records an async webhook handle for later joining. Silent no-op if the
    /// mutex is poisoned — an earlier panic already broke the pipeline, and
    /// losing summary fidelity on the way down is acceptable.
    fn track_async_handle(&self, handle: AsyncWebhookHandle) {
        match self.async_webhook_handles.lock() {
            Ok(mut guard) => guard.push(handle),
            Err(poisoned) => {
                warn!("async webhook tracker mutex was poisoned; recovering");
                poisoned.into_inner().push(handle);
            }
        }
    }

    /// Awaits every async webhook spawned via `fire` and returns a summary.
    /// Callers (e.g. `rocky hooks test`, future `rocky run` integration)
    /// should invoke this before returning so fire-and-forget deliveries
    /// don't vanish without a trace.
    ///
    /// Subsequent calls return an empty summary — handles are drained.
    pub async fn wait_async_webhooks(&self) -> AsyncWebhookSummary {
        let handles: Vec<AsyncWebhookHandle> = match self.async_webhook_handles.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(poisoned) => std::mem::take(&mut *poisoned.into_inner()),
        };
        let total = handles.len();
        let mut summary = AsyncWebhookSummary {
            total,
            ..Default::default()
        };
        if total == 0 {
            return summary;
        }

        for handle in handles {
            let (url, outcome) = handle.join().await;
            match outcome {
                Ok(Ok(())) => summary.succeeded += 1,
                Ok(Err(err)) => {
                    summary.failed += 1;
                    summary.failures.push((url, err.to_string()));
                }
                Err(join_err) => {
                    summary.failed += 1;
                    summary
                        .failures
                        .push((url, format!("task join error: {join_err}")));
                }
            }
        }
        info!(
            total = summary.total,
            succeeded = summary.succeeded,
            failed = summary.failed,
            "async webhook summary"
        );
        summary
    }
}

/// Builds a shared HTTP client for webhook delivery.
fn build_http_client() -> reqwest::Client {
    reqwest::Client::builder()
        .pool_max_idle_per_host(5)
        .connect_timeout(Duration::from_secs(5))
        .build()
        .expect("failed to create HTTP client for webhooks")
}

// ---------------------------------------------------------------------------
// Hook executor — runs a single shell command
// ---------------------------------------------------------------------------

async fn execute_hook(hook: &HookConfig, json: &str) -> Result<(), HookError> {
    use tokio::io::AsyncWriteExt;
    use tokio::process::Command;

    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(&hook.command);
    cmd.stdin(std::process::Stdio::piped());
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());

    // Set extra environment variables
    for (k, v) in &hook.env {
        cmd.env(k, v);
    }

    let mut child = cmd.spawn()?;

    // Write JSON context to stdin, then close it.
    //
    // Hooks that don't read stdin (e.g. `test "$VAR" = "x"`) may exit before
    // we finish writing, closing the pipe and producing `EPIPE`. Treat that
    // as success — the hook simply chose to ignore its stdin, which is fine.
    if let Some(mut stdin) = child.stdin.take() {
        match stdin.write_all(json.as_bytes()).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Err(e) => return Err(HookError::Io(e)),
        }
        drop(stdin); // close stdin so the child sees EOF
    }

    // Wait with timeout
    let timeout = Duration::from_millis(hook.timeout_ms);
    let wait_result = tokio::time::timeout(timeout, child.wait_with_output()).await;

    match wait_result {
        Ok(Ok(output)) => {
            if output.status.success() {
                Ok(())
            } else {
                let stderr = String::from_utf8_lossy(&output.stderr).to_string();
                Err(HookError::CommandFailed {
                    command: hook.command.clone(),
                    exit_code: output.status.code().unwrap_or(-1),
                    stderr,
                })
            }
        }
        Ok(Err(e)) => Err(HookError::Io(e)),
        Err(_) => {
            // Timeout — the child process is already dropped (and killed) when
            // `wait_with_output` future is cancelled by the timeout.
            Err(HookError::Timeout {
                command: hook.command.clone(),
                timeout_ms: hook.timeout_ms,
            })
        }
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -- Registry tests --

    #[test]
    fn test_empty_config_produces_empty_registry() {
        let config = HooksConfig::default();
        let registry = HookRegistry::from_config(&config);
        assert_eq!(registry.total_hook_count(), 0);
        assert!(!registry.has_hooks(&HookEvent::PipelineStart));
    }

    #[test]
    fn test_registry_from_config_single_hook() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "echo start".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Warn,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        assert!(registry.has_hooks(&HookEvent::PipelineStart));
        assert_eq!(registry.hooks_for(&HookEvent::PipelineStart).len(), 1);
        assert_eq!(
            registry.hooks_for(&HookEvent::PipelineStart)[0].command,
            "echo start"
        );
    }

    #[test]
    fn test_registry_from_config_multiple_hooks() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_after_checks".to_string(),
            HookConfigOrList::Multiple(vec![
                HookConfig {
                    command: "echo first".to_string(),
                    timeout_ms: 10000,
                    on_failure: FailureAction::Warn,
                    env: HashMap::new(),
                },
                HookConfig {
                    command: "echo second".to_string(),
                    timeout_ms: 10000,
                    on_failure: FailureAction::Ignore,
                    env: HashMap::new(),
                },
            ]),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        assert!(registry.has_hooks(&HookEvent::AfterChecks));
        assert_eq!(registry.hooks_for(&HookEvent::AfterChecks).len(), 2);
    }

    #[test]
    fn test_registry_unknown_event_ignored() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_nonexistent_event".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "echo nope".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Warn,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);
        assert_eq!(registry.total_hook_count(), 0);
    }

    #[test]
    fn test_no_hooks_for_unregistered_event() {
        let registry = HookRegistry::empty();
        assert!(!registry.has_hooks(&HookEvent::DriftDetected));
        assert!(registry.hooks_for(&HookEvent::DriftDetected).is_empty());
    }

    // -- Fire tests (async) --

    #[tokio::test]
    async fn test_fire_successful_command() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "cat > /dev/null".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Abort,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn test_fire_failing_command_abort() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_error".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "exit 1".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Abort,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_error("run-1", "test_pipeline", "something broke");
        let result = registry.fire(&ctx).await.unwrap();
        assert!(matches!(result, HookResult::Abort { .. }));
    }

    #[tokio::test]
    async fn test_fire_failing_command_warn() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_error".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "exit 1".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Warn,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_error("run-1", "test_pipeline", "something broke");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn test_fire_failing_command_ignore() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_error".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "exit 1".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Ignore,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_error("run-1", "test_pipeline", "something broke");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn test_fire_no_hooks_returns_continue() {
        let registry = HookRegistry::empty();
        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn test_fire_multiple_hooks_abort_stops_chain() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Multiple(vec![
                HookConfig {
                    command: "exit 1".to_string(),
                    timeout_ms: 5000,
                    on_failure: FailureAction::Abort,
                    env: HashMap::new(),
                },
                HookConfig {
                    // This should never run because the first hook aborts
                    command: "echo should_not_run".to_string(),
                    timeout_ms: 5000,
                    on_failure: FailureAction::Abort,
                    env: HashMap::new(),
                },
            ]),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert!(matches!(result, HookResult::Abort { .. }));
    }

    #[tokio::test]
    async fn test_fire_hook_receives_json_on_stdin() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Single(HookConfig {
                // Read stdin, parse JSON, check that the event field exists
                command: r#"python3 -c "import sys, json; d = json.load(sys.stdin); assert d['event'] == 'pipeline_start'""#.to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Abort,
                env: HashMap::new(),
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    #[tokio::test]
    async fn test_fire_hook_with_env_vars() {
        let mut env = HashMap::new();
        env.insert("ROCKY_TEST_HOOK_VAR".to_string(), "hello_hooks".to_string());

        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: r#"test "$ROCKY_TEST_HOOK_VAR" = "hello_hooks""#.to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Abort,
                env,
            }),
        );
        let config = HooksConfig {
            hooks,
            webhooks: HashMap::new(),
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);
    }

    // -- Context builder tests --

    #[test]
    fn test_context_pipeline_start_json() {
        let ctx = HookContext::pipeline_start("run-42", "raw_replication");
        let json: serde_json::Value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["event"], "pipeline_start");
        assert_eq!(json["run_id"], "run-42");
        assert_eq!(json["pipeline"], "raw_replication");
        assert!(json.get("model").is_none());
        assert!(json.get("table").is_none());
    }

    #[test]
    fn test_context_materialize_error_json() {
        let ctx = HookContext::materialize_error("run-1", "p1", "cat.sch.tbl", "timeout");
        let json: serde_json::Value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["event"], "materialize_error");
        assert_eq!(json["table"], "cat.sch.tbl");
        assert_eq!(json["error"], "timeout");
    }

    #[test]
    fn test_context_drift_detected_json() {
        let ctx = HookContext::drift_detected(
            "run-1",
            "p1",
            "cat.sch.tbl",
            &["col_a".to_string(), "col_b".to_string()],
        );
        let json: serde_json::Value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["event"], "drift_detected");
        let cols = json["metadata"]["drifted_columns"].as_array().unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0], "col_a");
    }

    #[test]
    fn test_context_after_checks_json() {
        let ctx = HookContext::after_checks("run-1", "p1", 10, 8, 2);
        let json: serde_json::Value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["event"], "after_checks");
        assert_eq!(json["metadata"]["total"], 10);
        assert_eq!(json["metadata"]["passed"], 8);
        assert_eq!(json["metadata"]["failed"], 2);
    }

    #[test]
    fn test_context_synthetic() {
        let ctx = HookContext::synthetic(HookEvent::PipelineStart, "test_pipe");
        let json: serde_json::Value = serde_json::to_value(&ctx).unwrap();
        assert_eq!(json["event"], "pipeline_start");
        assert_eq!(json["run_id"], "test-run-id");
        assert_eq!(json["metadata"]["test"], true);
    }

    // -- HookEvent tests --

    #[test]
    fn test_hook_event_roundtrip() {
        for event in HookEvent::all() {
            let key = event.config_key();
            let parsed = HookEvent::from_config_key(key).unwrap();
            assert_eq!(&parsed, event);
        }
    }

    #[test]
    fn test_hook_event_display() {
        assert_eq!(HookEvent::PipelineStart.to_string(), "on_pipeline_start");
        assert_eq!(HookEvent::DriftDetected.to_string(), "on_drift_detected");
    }

    // -- FailureAction default --

    #[test]
    fn test_failure_action_default_is_warn() {
        let action: FailureAction = Default::default();
        assert_eq!(action, FailureAction::Warn);
    }

    // -- TOML deserialization --

    #[test]
    fn test_hooks_config_toml_single() {
        let toml_str = r#"
[on_pipeline_start]
command = "echo start"
timeout_ms = 5000
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.hooks.contains_key("on_pipeline_start"));
        match &config.hooks["on_pipeline_start"] {
            HookConfigOrList::Single(c) => {
                assert_eq!(c.command, "echo start");
                assert_eq!(c.timeout_ms, 5000);
                assert_eq!(c.on_failure, FailureAction::Warn); // default
            }
            HookConfigOrList::Multiple(_) => panic!("expected single"),
        }
    }

    #[test]
    fn test_hooks_config_toml_multiple() {
        let toml_str = r#"
[[on_after_checks]]
command = "echo first"
timeout_ms = 10000

[[on_after_checks]]
command = "echo second"
on_failure = "abort"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.hooks["on_after_checks"] {
            HookConfigOrList::Multiple(list) => {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0].command, "echo first");
                assert_eq!(list[1].on_failure, FailureAction::Abort);
            }
            HookConfigOrList::Single(_) => panic!("expected multiple"),
        }
    }

    #[test]
    fn test_hooks_config_toml_with_env() {
        let toml_str = r#"
[on_anomaly_detected]
command = "scripts/alert.sh"
on_failure = "warn"

[on_anomaly_detected.env]
PAGERDUTY_KEY = "test_key"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.hooks["on_anomaly_detected"] {
            HookConfigOrList::Single(c) => {
                assert_eq!(c.env["PAGERDUTY_KEY"], "test_key");
            }
            HookConfigOrList::Multiple(_) => panic!("expected single"),
        }
    }

    #[test]
    fn test_hook_config_defaults() {
        let toml_str = r#"
[on_state_synced]
command = "echo done"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.hooks["on_state_synced"] {
            HookConfigOrList::Single(c) => {
                assert_eq!(c.timeout_ms, 30_000); // default
                assert_eq!(c.on_failure, FailureAction::Warn); // default
                assert!(c.env.is_empty()); // default
            }
            HookConfigOrList::Multiple(_) => panic!("expected single"),
        }
    }

    // -- Webhook registry tests --

    #[test]
    fn test_registry_with_webhooks() {
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_pipeline_error".to_string(),
            WebhookConfigOrList::Single(WebhookConfig {
                url: "https://hooks.slack.com/test".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                body_template: Some(r#"{"text": "{{error}}"}"#.to_string()),
                secret: None,
                timeout_ms: 5000,
                async_mode: true,
                on_failure: FailureAction::Warn,
                retry_count: 0,
                retry_delay_ms: 1000,
                preset: None,
            }),
        );
        let config = HooksConfig {
            hooks: HashMap::new(),
            webhooks,
        };
        let registry = HookRegistry::from_config(&config);

        assert!(registry.has_hooks(&HookEvent::PipelineError));
        assert_eq!(registry.webhooks_for(&HookEvent::PipelineError).len(), 1);
        assert_eq!(registry.total_hook_count(), 1);
        assert_eq!(registry.shell_hook_count(), 0);
        assert_eq!(registry.webhook_count(), 1);
    }

    #[test]
    fn test_registry_mixed_shell_and_webhooks() {
        let mut hooks = HashMap::new();
        hooks.insert(
            "on_pipeline_start".to_string(),
            HookConfigOrList::Single(HookConfig {
                command: "echo start".to_string(),
                timeout_ms: 5000,
                on_failure: FailureAction::Warn,
                env: HashMap::new(),
            }),
        );

        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_pipeline_start".to_string(),
            WebhookConfigOrList::Single(WebhookConfig {
                url: "https://example.com/hook".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                body_template: None,
                secret: None,
                timeout_ms: 5000,
                async_mode: false,
                on_failure: FailureAction::Warn,
                retry_count: 0,
                retry_delay_ms: 1000,
                preset: None,
            }),
        );

        let config = HooksConfig { hooks, webhooks };
        let registry = HookRegistry::from_config(&config);

        assert!(registry.has_hooks(&HookEvent::PipelineStart));
        assert_eq!(registry.hooks_for(&HookEvent::PipelineStart).len(), 1);
        assert_eq!(registry.webhooks_for(&HookEvent::PipelineStart).len(), 1);
        assert_eq!(registry.total_hook_count(), 2);
        assert_eq!(registry.shell_hook_count(), 1);
        assert_eq!(registry.webhook_count(), 1);
    }

    #[test]
    fn test_registry_multiple_webhooks_per_event() {
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_pipeline_complete".to_string(),
            WebhookConfigOrList::Multiple(vec![
                WebhookConfig {
                    url: "https://slack.example.com".to_string(),
                    method: "POST".to_string(),
                    headers: HashMap::new(),
                    body_template: None,
                    secret: None,
                    timeout_ms: 5000,
                    async_mode: true,
                    on_failure: FailureAction::Warn,
                    retry_count: 0,
                    retry_delay_ms: 1000,
                    preset: None,
                },
                WebhookConfig {
                    url: "https://api.company.com/complete".to_string(),
                    method: "POST".to_string(),
                    headers: HashMap::new(),
                    body_template: None,
                    secret: Some("secret123".to_string()),
                    timeout_ms: 10000,
                    async_mode: false,
                    on_failure: FailureAction::Abort,
                    retry_count: 2,
                    retry_delay_ms: 1000,
                    preset: None,
                },
            ]),
        );

        let config = HooksConfig {
            hooks: HashMap::new(),
            webhooks,
        };
        let registry = HookRegistry::from_config(&config);

        assert_eq!(registry.webhooks_for(&HookEvent::PipelineComplete).len(), 2);
        assert_eq!(registry.total_hook_count(), 2);
    }

    #[test]
    fn test_registry_unknown_webhook_event_ignored() {
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_nonexistent".to_string(),
            WebhookConfigOrList::Single(WebhookConfig {
                url: "https://example.com".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                body_template: None,
                secret: None,
                timeout_ms: 5000,
                async_mode: false,
                on_failure: FailureAction::Warn,
                retry_count: 0,
                retry_delay_ms: 1000,
                preset: None,
            }),
        );

        let config = HooksConfig {
            hooks: HashMap::new(),
            webhooks,
        };
        let registry = HookRegistry::from_config(&config);
        assert_eq!(registry.webhook_count(), 0);
    }

    #[test]
    fn test_webhooks_for_empty() {
        let registry = HookRegistry::empty();
        assert!(registry.webhooks_for(&HookEvent::PipelineStart).is_empty());
    }

    // -- Webhook TOML parsing tests --

    #[test]
    fn test_webhooks_toml_single() {
        let toml_str = r#"
[webhooks.on_materialize_error]
url = "https://hooks.slack.com/services/T/B/x"
method = "POST"
body_template = '{"text": "error: {{error}}"}'
secret = "my_secret"
timeout_ms = 5000
async = true
on_failure = "abort"
retry_count = 2
retry_delay_ms = 2000

[webhooks.on_materialize_error.headers]
"X-Custom" = "value"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.webhooks.contains_key("on_materialize_error"));
        match &config.webhooks["on_materialize_error"] {
            WebhookConfigOrList::Single(wh) => {
                assert_eq!(wh.url, "https://hooks.slack.com/services/T/B/x");
                assert_eq!(wh.method, "POST");
                assert_eq!(
                    wh.body_template.as_deref(),
                    Some(r#"{"text": "error: {{error}}"}"#)
                );
                assert_eq!(wh.secret.as_deref(), Some("my_secret"));
                assert_eq!(wh.timeout_ms, 5000);
                assert!(wh.async_mode);
                assert_eq!(wh.on_failure, FailureAction::Abort);
                assert_eq!(wh.retry_count, 2);
                assert_eq!(wh.retry_delay_ms, 2000);
                assert_eq!(wh.headers.get("X-Custom").unwrap(), "value");
            }
            WebhookConfigOrList::Multiple(_) => panic!("expected single"),
        }
    }

    #[test]
    fn test_webhooks_toml_multiple() {
        let toml_str = r#"
[[webhooks.on_pipeline_complete]]
url = "https://api.company.com/rocky/complete"
secret = "secret1"

[[webhooks.on_pipeline_complete]]
url = "https://hooks.slack.com/services/T/B/x"
async = true
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.webhooks["on_pipeline_complete"] {
            WebhookConfigOrList::Multiple(list) => {
                assert_eq!(list.len(), 2);
                assert_eq!(list[0].url, "https://api.company.com/rocky/complete");
                assert_eq!(list[0].secret.as_deref(), Some("secret1"));
                assert_eq!(list[1].url, "https://hooks.slack.com/services/T/B/x");
                assert!(list[1].async_mode);
            }
            WebhookConfigOrList::Single(_) => panic!("expected multiple"),
        }
    }

    #[test]
    fn test_webhooks_toml_with_preset() {
        let toml_str = r#"
[webhooks.on_materialize_error]
preset = "slack"
url = "https://hooks.slack.com/services/T/B/x"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.webhooks["on_materialize_error"] {
            WebhookConfigOrList::Single(wh) => {
                assert_eq!(wh.preset.as_deref(), Some("slack"));
                assert_eq!(wh.url, "https://hooks.slack.com/services/T/B/x");
            }
            WebhookConfigOrList::Multiple(_) => panic!("expected single"),
        }

        // Verify preset resolution during registry construction
        let registry = HookRegistry::from_config(&config);
        let webhooks = registry.webhooks_for(&HookEvent::MaterializeError);
        assert_eq!(webhooks.len(), 1);
        // Preset should have populated the body_template
        assert!(webhooks[0].body_template.is_some());
        let template = webhooks[0].body_template.as_ref().unwrap();
        assert!(template.contains("{{event}}"));
    }

    #[test]
    fn test_webhooks_toml_defaults() {
        let toml_str = r#"
[webhooks.on_pipeline_start]
url = "https://example.com/hook"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        match &config.webhooks["on_pipeline_start"] {
            WebhookConfigOrList::Single(wh) => {
                assert_eq!(wh.method, "POST");
                assert_eq!(wh.timeout_ms, 10_000);
                assert!(!wh.async_mode);
                assert_eq!(wh.on_failure, FailureAction::Warn);
                assert_eq!(wh.retry_count, 0);
                assert_eq!(wh.retry_delay_ms, 1000);
                assert!(wh.secret.is_none());
                assert!(wh.body_template.is_none());
                assert!(wh.headers.is_empty());
                assert!(wh.preset.is_none());
            }
            WebhookConfigOrList::Multiple(_) => panic!("expected single"),
        }
    }

    #[test]
    fn test_mixed_shell_and_webhooks_toml() {
        let toml_str = r#"
[on_pipeline_start]
command = "echo start"

[webhooks.on_pipeline_start]
url = "https://example.com/hook"
"#;
        let config: HooksConfig = toml::from_str(toml_str).unwrap();
        assert!(config.hooks.contains_key("on_pipeline_start"));
        assert!(config.webhooks.contains_key("on_pipeline_start"));

        let registry = HookRegistry::from_config(&config);
        assert_eq!(registry.hooks_for(&HookEvent::PipelineStart).len(), 1);
        assert_eq!(registry.webhooks_for(&HookEvent::PipelineStart).len(), 1);
        assert_eq!(registry.total_hook_count(), 2);
    }

    // -- Method validation at config load --

    #[test]
    fn test_invalid_method_does_not_prevent_load() {
        // A webhook with an unparseable HTTP method used to silently default
        // to POST at request time. After P2.10 the loader emits a warn! log
        // but still registers the webhook (backward-compatible — invalid
        // methods don't break existing configs).
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_pipeline_start".to_string(),
            WebhookConfigOrList::Single(WebhookConfig {
                url: "https://example.com/hook".to_string(),
                method: "PØST".to_string(), // invalid token — reqwest rejects non-ASCII
                headers: HashMap::new(),
                body_template: None,
                secret: None,
                timeout_ms: 1000,
                async_mode: false,
                on_failure: FailureAction::Warn,
                retry_count: 0,
                retry_delay_ms: 100,
                preset: None,
            }),
        );
        let config = HooksConfig {
            hooks: HashMap::new(),
            webhooks,
        };
        let registry = HookRegistry::from_config(&config);
        // Webhook is still registered — the load-time warning is visibility,
        // not a hard failure (changing that would break hot-reload configs).
        assert_eq!(registry.webhooks_for(&HookEvent::PipelineStart).len(), 1);
    }

    // -- Async webhook tracking --

    #[tokio::test]
    async fn test_wait_async_webhooks_empty_registry() {
        let registry = HookRegistry::empty();
        let summary = registry.wait_async_webhooks().await;
        assert_eq!(summary.total, 0);
        assert_eq!(summary.succeeded, 0);
        assert_eq!(summary.failed, 0);
        assert!(summary.failures.is_empty());
    }

    #[tokio::test]
    async fn test_wait_async_webhooks_tracks_spawned_delivery() {
        // Fire an async webhook against an unroutable address so the spawned
        // task completes quickly with a delivery error. We want to verify that
        // (a) fire() returns without awaiting the HTTP roundtrip and (b)
        // wait_async_webhooks surfaces the failure in the summary.
        let mut webhooks = HashMap::new();
        webhooks.insert(
            "on_pipeline_start".to_string(),
            WebhookConfigOrList::Single(WebhookConfig {
                url: "https://127.0.0.1:1/nope".to_string(),
                method: "POST".to_string(),
                headers: HashMap::new(),
                body_template: None,
                secret: None,
                // Short per-request timeout so the task ends well inside the
                // test deadline; retries are off so we get exactly one attempt.
                timeout_ms: 200,
                async_mode: true,
                on_failure: FailureAction::Warn,
                retry_count: 0,
                retry_delay_ms: 100,
                preset: None,
            }),
        );
        let config = HooksConfig {
            hooks: HashMap::new(),
            webhooks,
        };
        let registry = HookRegistry::from_config(&config);

        let ctx = HookContext::pipeline_start("run-1", "test_pipeline");
        let result = registry.fire(&ctx).await.unwrap();
        assert_eq!(result, HookResult::Continue);

        let summary = registry.wait_async_webhooks().await;
        assert_eq!(summary.total, 1);
        // Delivery to 127.0.0.1:1 fails; the summary should record that.
        assert_eq!(summary.succeeded, 0);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.failures.len(), 1);
        assert!(summary.failures[0].0.contains("127.0.0.1:1"));
    }

    #[tokio::test]
    async fn test_wait_async_webhooks_drains_handles() {
        // After calling wait_async_webhooks once, a second call should report
        // an empty summary (the tracker was drained).
        let registry = HookRegistry::empty();
        let first = registry.wait_async_webhooks().await;
        assert_eq!(first.total, 0);
        let second = registry.wait_async_webhooks().await;
        assert_eq!(second.total, 0);
    }
}
