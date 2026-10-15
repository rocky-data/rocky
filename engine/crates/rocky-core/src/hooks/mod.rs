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

#[derive(Error)]
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

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919). `Display` leaves
/// out a failed command's stderr, so it is added here, rendered too.
impl std::fmt::Debug for HookError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HookError::CommandFailed { stderr, .. } => {
                let text = format!("{self}; stderr: {stderr}");
                crate::secret_registry::fmt_rendered_debug(f, "HookError", &text)
            }
            HookError::Timeout { .. }
            | HookError::Aborted { .. }
            | HookError::Io(_)
            | HookError::Serialize(_)
            | HookError::Webhook(_) => {
                crate::secret_registry::fmt_rendered_debug(f, "HookError", self)
            }
        }
    }
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

    /// `pipeline_complete` for a pipeline type with no table loop
    /// (transformation, quality, snapshot, load): `duration_ms` is set and
    /// `metadata.table_count` is absent.
    pub fn pipeline_complete_untabled(run_id: &str, pipeline: &str, duration_ms: u64) -> Self {
        let mut ctx = Self::new(HookEvent::PipelineComplete, run_id, pipeline);
        ctx.duration_ms = Some(duration_ms);
        ctx
    }

    pub fn pipeline_complete(
        run_id: &str,
        pipeline: &str,
        duration_ms: u64,
        table_count: usize,
    ) -> Self {
        let mut ctx = Self::pipeline_complete_untabled(run_id, pipeline, duration_ms);
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
    /// were spawned. The URL is an [`crate::env_string::EnvString`], so it
    /// prints a resolved `${VAR}` value as `${NAME}` (#2383).
    pub failures: Vec<(crate::env_string::EnvString, String)>,
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
        // A webhook is delivered to the URL it names. Following a redirect
        // would also make reqwest log both URLs at debug level, outside the
        // `${NAME}` masking (#2383).
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("failed to create HTTP client for webhooks")
}

// ---------------------------------------------------------------------------
// Hook executor — runs a single shell command
// ---------------------------------------------------------------------------

/// Build the platform shell command that runs a hook's command string.
///
/// On Unix this is `sh -c <command>`; on Windows there is no `sh`, so it
/// is `cmd /C <command>`. Returning a `tokio::process::Command` lets the
/// caller wire up stdio / env without duplicating the cfg branch.
fn shell_command(command: &str) -> tokio::process::Command {
    #[cfg(windows)]
    {
        let mut cmd = tokio::process::Command::new("cmd");
        cmd.args(["/C", command]);
        cmd
    }
    #[cfg(unix)]
    {
        let mut cmd = tokio::process::Command::new("sh");
        cmd.arg("-c").arg(command);
        cmd
    }
}

/// Write the hook's JSON context to an anonymous temp file, rewound to the
/// start, for use as the child's stdin.
///
/// A pipe would need Rocky to write after the spawn. The CLI keeps SIGPIPE
/// at its default disposition, so a write to a hook that already exited
/// without reading (`touch done`) kills Rocky silently (#2360). A file needs
/// no write after the spawn. `tempfile()` unlinks the file at once, so the
/// context never stays on disk under a name.
fn context_stdin(json: &str) -> std::io::Result<std::fs::File> {
    use std::io::{Seek, SeekFrom, Write};

    let mut file = tempfile::tempfile()?;
    file.write_all(json.as_bytes())?;
    file.seek(SeekFrom::Start(0))?;
    Ok(file)
}

async fn execute_hook(hook: &HookConfig, json: &str) -> Result<(), HookError> {
    let mut cmd = shell_command(&hook.command);
    // A host with no writable temp directory (a read-only container root)
    // still runs the hook: the context goes through a pipe, as before #2360.
    // The pipe is written through `ChildStdinWriter`, so a hook that exits
    // before the write cannot kill Rocky with SIGPIPE (#2374).
    let piped_context = match context_stdin(json) {
        Ok(file) => {
            cmd.stdin(file);
            false
        }
        Err(e) => {
            warn!(command = %hook.command, error = %e, "cannot create the hook's stdin file; using a pipe");
            cmd.stdin(std::process::Stdio::piped());
            true
        }
    };
    cmd.stdout(std::process::Stdio::piped());
    cmd.stderr(std::process::Stdio::piped());
    // SIGKILL the child if our `Child` handle is dropped — including when
    // `tokio::time::timeout` cancels the `wait_with_output` future on
    // expiry. Without this, a hook that runs past `timeout_ms` keeps
    // executing after Rocky returns `HookError::Timeout`.
    cmd.kill_on_drop(true);

    // Set extra environment variables
    for (k, v) in &hook.env {
        cmd.env(k, v);
    }
    crate::process::strip_dagster_pipes_env(cmd.as_std_mut());

    let mut child = cmd.spawn()?;
    // One deadline for the stdin write and the wait together, so
    // `timeout_ms` bounds the whole hook.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(hook.timeout_ms);

    if piped_context && let Some(stdin) = child.stdin.take() {
        let stdin = rocky_adapter_sdk::child_stdin::ChildStdinWriter::new(stdin)?;
        // A hook that exits without reading closes the pipe: that is
        // `BrokenPipe`, and the hook chose to ignore its stdin, which is
        // fine. A hook that neither reads nor exits would block the write
        // on a full pipe, so the write shares the hook's deadline; dropping
        // `child` then kills the hook, which ends the write.
        match tokio::time::timeout_at(deadline, stdin.write_all(json.as_bytes().to_vec())).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Ok(Err(e)) => return Err(HookError::Io(e)),
            Err(_) => {
                return Err(HookError::Timeout {
                    command: hook.command.clone(),
                    timeout_ms: hook.timeout_ms,
                });
            }
        }
        // Close the pipe so the hook sees end-of-file.
        drop(stdin);
    }

    // Wait until the same deadline.
    let wait_result = tokio::time::timeout_at(deadline, child.wait_with_output()).await;

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
            // Timeout — `kill_on_drop(true)` on the `Command` ensures the
            // OS process is SIGKILL'd when `wait_with_output`'s `Child`
            // handle is dropped by the timeout cancellation.
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

    // -- Shell-command platform builder --

    #[cfg(unix)]
    #[test]
    fn shell_command_uses_sh_on_unix() {
        let cmd = shell_command("echo hi");
        assert_eq!(cmd.as_std().get_program(), "sh");
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, ["-c", "echo hi"]);
    }

    #[cfg(windows)]
    #[test]
    fn shell_command_uses_cmd_on_windows() {
        let cmd = shell_command("echo hi");
        assert_eq!(cmd.as_std().get_program(), "cmd");
        let args: Vec<_> = cmd.as_std().get_args().collect();
        assert_eq!(args, ["/C", "echo hi"]);
    }

    /// The `Debug` output prints a resolved `${VAR}` value as `${NAME}` (#1919),
    /// in the command and in the stderr that `Display` leaves out.
    #[test]
    fn hook_error_debug_prints_a_resolved_value_as_its_name() {
        const COMMAND_SECRET: &str = "hook-cmd-1919-a1c3";
        const STDERR_SECRET: &str = "hook-err-1919-b2d4";
        crate::secret_registry::register_substitution("RV_HOOK_CMD_DBG", COMMAND_SECRET);
        crate::secret_registry::register_substitution("RV_HOOK_ERR_DBG", STDERR_SECRET);
        let err = HookError::CommandFailed {
            command: format!("notify --token {COMMAND_SECRET}"),
            exit_code: 1,
            stderr: format!("401 for {STDERR_SECRET}"),
        };
        let debug = format!("{err:?}");
        assert!(!debug.contains(COMMAND_SECRET), "Debug leaks: {debug}");
        assert!(!debug.contains(STDERR_SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_HOOK_CMD_DBG}"), "{debug}");
        assert!(
            debug.contains("stderr: 401 for ${RV_HOOK_ERR_DBG}"),
            "{debug}"
        );
    }

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

    #[cfg(unix)]
    #[tokio::test]
    async fn hook_child_does_not_receive_pipes_environment() {
        let hook = HookConfig {
            command: "test -z \"$DAGSTER_PIPES_CONTEXT\" && test -z \"$DAGSTER_PIPES_MESSAGES\" && test -z \"$DAGSTER_PIPES_EXTRA\"".into(),
            timeout_ms: 5000,
            on_failure: FailureAction::Abort,
            env: HashMap::from([
                ("DAGSTER_PIPES_CONTEXT".into(), "outer".into()),
                ("DAGSTER_PIPES_MESSAGES".into(), "outer".into()),
                ("DAGSTER_PIPES_EXTRA".into(), "outer".into()),
            ]),
        };
        execute_hook(&hook, "{}")
            .await
            .expect("hook must see no Pipes environment");
    }

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

    /// The stdin file is rewound before the spawn, so the hook reads the
    /// whole context, byte for byte, and then EOF.
    #[cfg(unix)]
    #[tokio::test]
    async fn hook_stdin_is_the_exact_context() {
        let dir = tempfile::tempdir().unwrap();
        let seen = dir.path().join("seen.json");
        let hook = HookConfig {
            command: format!("cat > '{}'", seen.display()),
            timeout_ms: 5000,
            on_failure: FailureAction::Abort,
            env: HashMap::new(),
        };
        let json = r#"{"event":"pipeline_start","run_id":"run-1"}"#;
        execute_hook(&hook, json).await.expect("hook must succeed");
        assert_eq!(std::fs::read_to_string(&seen).unwrap(), json);
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
                url: "https://hooks.slack.com/test".into(),
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
                url: "https://example.com/hook".into(),
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
                    url: "https://slack.example.com".into(),
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
                    url: "https://api.company.com/complete".into(),
                    method: "POST".to_string(),
                    headers: HashMap::new(),
                    body_template: None,
                    secret: Some(crate::redacted::RedactedString::new("secret123".into())),
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
                url: "https://example.com".into(),
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
                assert_eq!(
                    wh.secret
                        .as_ref()
                        .map(crate::redacted::RedactedString::expose),
                    Some("my_secret")
                );
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
                assert_eq!(
                    list[0]
                        .secret
                        .as_ref()
                        .map(crate::redacted::RedactedString::expose),
                    Some("secret1")
                );
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
                url: "https://example.com/hook".into(),
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
                url: "https://127.0.0.1:1/nope".into(),
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
        assert!(summary.failures[0].0.expose().contains("127.0.0.1:1"));
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

    // -- #2383: a webhook URL set with `${VAR}` never prints --

    /// Every `tracing` event emitted on this thread, as text.
    #[derive(Clone, Default)]
    struct CapturedLogs(Arc<Mutex<Vec<u8>>>);

    impl std::io::Write for CapturedLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl CapturedLogs {
        /// A TRACE-level subscriber that writes here. `set_default` is
        /// thread-local, so the caller runs on a current-thread runtime and
        /// every spawned task stays on this thread.
        fn install(&self) -> tracing::subscriber::DefaultGuard {
            let writer = self.clone();
            let subscriber = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_writer(move || writer.clone())
                .with_ansi(false)
                .finish();
            tracing::subscriber::set_default(subscriber)
        }

        fn text(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    /// A loopback URL with nothing listening on its port.
    fn unreachable_base() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        format!("http://127.0.0.1:{port}")
    }

    /// The `[webhooks.on_pipeline_start]` table, parsed the way `rocky.toml`
    /// is: from text that already holds the expanded `${VAR}` value.
    fn webhook_hooks_config(url: &str, extra: &str) -> HooksConfig {
        toml::from_str(&format!(
            "[webhooks.on_pipeline_start]\nurl = \"{url}\"\n{extra}\n"
        ))
        .unwrap()
    }

    #[tokio::test]
    async fn a_failing_sync_webhook_prints_its_resolved_url_only_as_its_name() {
        let url = format!("{}/services/T2383-sync-a91f/B0/xyzzy", unreachable_base());
        crate::secret_registry::register_substitution("ROCKY_T2383_SYNC_URL", &url);
        let placeholder = "${ROCKY_T2383_SYNC_URL}";

        let logs = CapturedLogs::default();
        let _guard = logs.install();
        tracing::info!("capture is live");

        let config = webhook_hooks_config(
            &url,
            "on_failure = \"abort\"\nretry_count = 1\nretry_delay_ms = 1\ntimeout_ms = 2000",
        );
        let registry = HookRegistry::from_config(&config);
        let ctx = HookContext::pipeline_start("run-2383", "p");

        let HookResult::Abort { reason } = registry.fire(&ctx).await.unwrap() else {
            panic!("an unreachable abort webhook must abort");
        };
        assert!(!reason.contains(&url), "abort reason leaks: {reason}");
        assert!(reason.contains(placeholder), "{reason}");

        let wh = &registry.webhooks_for(&HookEvent::PipelineStart)[0];
        assert_eq!(
            wh.url.expose(),
            url,
            "PRECONDITION: the request uses the value"
        );
        assert!(!wh.to_string().contains(&url), "Display leaks: {wh}");
        let err = webhook::fire_webhook(wh, &ctx, &reqwest::Client::new())
            .await
            .expect_err("nothing listens on the port");
        for printed in [err.to_string(), format!("{err:?}")] {
            assert!(!printed.contains(&url), "error leaks: {printed}");
            assert!(printed.contains(placeholder), "{printed}");
        }

        let text = logs.text();
        assert!(
            text.contains("webhook failed, retrying"),
            "PRECONDITION: the retry line was captured: {text}"
        );
        assert!(text.contains("webhook failed, aborting"), "{text}");
        assert!(
            !text.contains("T2383-sync-a91f"),
            "a log line leaks: {text}"
        );
        assert!(text.contains(placeholder), "{text}");
    }

    #[tokio::test]
    async fn a_failing_async_webhook_prints_its_resolved_url_only_as_its_name() {
        let url = format!("{}/services/T2383-async-6d0e/B1/plugh", unreachable_base());
        crate::secret_registry::register_substitution("ROCKY_T2383_ASYNC_URL", &url);
        let placeholder = "${ROCKY_T2383_ASYNC_URL}";

        let logs = CapturedLogs::default();
        let _guard = logs.install();

        let config = webhook_hooks_config(&url, "async = true\ntimeout_ms = 2000");
        let registry = HookRegistry::from_config(&config);
        let ctx = HookContext::pipeline_start("run-2383", "p");
        assert_eq!(registry.fire(&ctx).await.unwrap(), HookResult::Continue);

        let summary = registry.wait_async_webhooks().await;
        assert_eq!(summary.failed, 1, "PRECONDITION: the delivery failed");
        let (failed_url, err) = &summary.failures[0];
        assert_eq!(failed_url.expose(), url);
        assert_eq!(failed_url.to_string(), placeholder);
        assert!(!err.contains(&url), "error leaks: {err}");
        assert!(err.contains(placeholder), "{err}");

        let text = logs.text();
        assert!(
            text.contains("async webhook failed"),
            "PRECONDITION: {text}"
        );
        assert!(
            !text.contains("T2383-async-6d0e"),
            "a log line leaks: {text}"
        );
        assert!(text.contains(placeholder), "{text}");
    }

    #[tokio::test]
    async fn a_webhook_set_with_env_vars_is_sent_to_the_resolved_url_and_headers() {
        use wiremock::matchers::{header, method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let secret_path = "/services/T2383-deliver-5c71/B2/quux";
        let url = format!("{}{secret_path}", server.uri());
        let auth = "Bearer t2383-header-secret-9e4d";
        crate::secret_registry::register_substitution("ROCKY_T2383_DELIVER_URL", &url);
        crate::secret_registry::register_substitution("ROCKY_T2383_AUTH", auth);
        Mock::given(method("POST"))
            .and(path(secret_path))
            .and(header("Authorization", auth))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;

        let config = webhook_hooks_config(
            &url,
            &format!("[webhooks.on_pipeline_start.headers]\nAuthorization = \"{auth}\""),
        );
        let registry = HookRegistry::from_config(&config);
        let wh = &registry.webhooks_for(&HookEvent::PipelineStart)[0];
        assert_eq!(
            wh.headers["Authorization"].to_string(),
            "${ROCKY_T2383_AUTH}"
        );
        assert!(!format!("{wh:?}").contains(auth), "Debug leaks a header");

        let ctx = HookContext::pipeline_start("run-2383", "p");
        assert_eq!(registry.fire(&ctx).await.unwrap(), HookResult::Continue);
        server.verify().await;
    }

    #[tokio::test]
    async fn a_webhook_that_returns_an_http_error_prints_its_url_only_as_its_name() {
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let url = format!("{}/services/T2383-status-3b8c/B3/frob", server.uri());
        crate::secret_registry::register_substitution("ROCKY_T2383_STATUS_URL", &url);
        let placeholder = "${ROCKY_T2383_STATUS_URL}";
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;

        let logs = CapturedLogs::default();
        let _guard = logs.install();

        let config = webhook_hooks_config(&url, "on_failure = \"abort\"");
        let registry = HookRegistry::from_config(&config);
        let ctx = HookContext::pipeline_start("run-2383", "p");
        let HookResult::Abort { reason } = registry.fire(&ctx).await.unwrap() else {
            panic!("a 500 from an abort webhook must abort");
        };
        assert!(reason.contains("status 500"), "PRECONDITION: {reason}");
        assert!(
            !reason.contains("T2383-status-3b8c"),
            "abort reason leaks: {reason}"
        );
        assert!(reason.contains(placeholder), "{reason}");

        let text = logs.text();
        assert!(
            text.contains("webhook failed, aborting"),
            "PRECONDITION: {text}"
        );
        assert!(
            !text.contains("T2383-status-3b8c"),
            "a log line leaks: {text}"
        );
        // The log names the webhook, not just omits it.
        assert!(text.contains(placeholder), "{text}");
    }

    /// A webhook is delivered only to the URL it names: a `3xx` reply is a
    /// failed delivery, and the redirect target is never requested.
    #[tokio::test]
    async fn a_webhook_does_not_follow_a_redirect() {
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let server = MockServer::start().await;
        let url = format!("{}/services/T2383-redirect", server.uri());
        Mock::given(wiremock::matchers::path("/services/T2383-redirect"))
            .respond_with(
                ResponseTemplate::new(302)
                    .insert_header("Location", format!("{}/elsewhere", server.uri())),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(wiremock::matchers::path("/elsewhere"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;

        let config = webhook_hooks_config(&url, "on_failure = \"abort\"");
        let registry = HookRegistry::from_config(&config);
        let ctx = HookContext::pipeline_start("run-2383-redirect", "p");
        let result = registry.fire(&ctx).await.unwrap();
        assert!(
            matches!(result, HookResult::Abort { .. }),
            "a 302 must count as a failed delivery: {result:?}"
        );
        server.verify().await;
    }
}
