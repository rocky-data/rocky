//! Recording the token usage of AI calls for `rocky cost`.
//!
//! A [`UsageRecorder`] hands out a [`UsageSink`] for an
//! [`LlmClient`](crate::client::LlmClient) and turns each call it sees into a
//! [`rocky_core::ai_usage::AiUsageRecord`], stamped with the command and the
//! Rocky model the call is about.
//!
//! - [`UsageRecorder::collect`] keeps the records in memory. A CLI command
//!   calls [`UsageRecorder::persist`] when it is done: into the state store
//!   when it can open the writer, else into the spool.
//! - [`UsageRecorder::spool`] appends each call to the spool at once. The LSP
//!   and MCP servers use it: they never write the state store.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use rocky_core::ai_usage::{AiUsageRecord, RecordOutcome};

use crate::client::{LlmUsage, UsageSink};

struct Inner {
    command: String,
    model: Mutex<Option<String>>,
    records: Mutex<Vec<AiUsageRecord>>,
    spool: Option<PathBuf>,
}

/// Turns the calls of one command into usage records. Cheap to clone.
#[derive(Clone)]
pub struct UsageRecorder {
    inner: Arc<Inner>,
}

impl UsageRecorder {
    /// Keep the records in memory until [`Self::persist`].
    #[must_use]
    pub fn collect(command: &str) -> Self {
        Self::new(command, None)
    }

    /// Append each call's record to the spool next to `state_path` as soon
    /// as the call returns. Never opens the state store.
    #[must_use]
    pub fn spool(command: &str, state_path: &Path) -> Self {
        Self::new(
            command,
            Some(rocky_core::ai_usage::spool_path(state_path)),
        )
    }

    fn new(command: &str, spool: Option<PathBuf>) -> Self {
        Self {
            inner: Arc::new(Inner {
                command: command.to_string(),
                model: Mutex::new(None),
                records: Mutex::new(Vec::new()),
                spool,
            }),
        }
    }

    /// The Rocky model the next calls are about, or `None` for none.
    pub fn set_model(&self, model: Option<&str>) {
        *lock(&self.inner.model) = model.map(str::to_string);
    }

    /// Name `model` on every kept record that names no model yet. For a
    /// command that learns the model only after its calls, such as
    /// `rocky ai "<intent>"`.
    pub fn label_unlabelled(&self, model: &str) {
        for record in lock(&self.inner.records).iter_mut() {
            if record.model.is_none() {
                record.model = Some(model.to_string());
            }
        }
    }

    /// The sink to give an [`LlmClient`](crate::client::LlmClient).
    #[must_use]
    pub fn sink(&self) -> UsageSink {
        let recorder = self.clone();
        Arc::new(move |usage: &LlmUsage| recorder.observe(usage))
    }

    fn observe(&self, usage: &LlmUsage) {
        let record = AiUsageRecord::new(
            chrono::Utc::now(),
            &self.inner.command,
            lock(&self.inner.model).clone(),
            usage.llm_model.clone(),
            usage.input_tokens,
            usage.output_tokens,
        );
        match &self.inner.spool {
            Some(path) => {
                if let Err(error) = rocky_core::ai_usage::append_to_spool(path, &[record]) {
                    tracing::warn!(
                        path = %path.display(),
                        %error,
                        "AI usage of this call was not recorded"
                    );
                }
            }
            None => lock(&self.inner.records).push(record),
        }
    }

    /// The records kept so far (none for a spooling recorder).
    #[must_use]
    pub fn records(&self) -> Vec<AiUsageRecord> {
        lock(&self.inner.records).clone()
    }

    /// Write the kept records for the state file at `state_path` and clear
    /// them. Never fails the command: a failure is a warning.
    pub fn persist(&self, state_path: &Path) -> Option<RecordOutcome> {
        let records = std::mem::take(&mut *lock(&self.inner.records));
        match rocky_core::ai_usage::record_or_spool(state_path, &records) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                tracing::warn!(
                    calls = records.len(),
                    %error,
                    "AI usage of this command was not recorded; `rocky cost` will not count it"
                );
                None
            }
        }
    }
}

impl UsageRecorder {
    /// A guard that calls [`Self::persist`] when it drops, so a command that
    /// returns early with an error still records the calls it already paid
    /// for.
    #[must_use]
    pub fn persist_on_drop(&self, state_path: &Path) -> PersistGuard {
        PersistGuard {
            recorder: self.clone(),
            state_path: state_path.to_path_buf(),
        }
    }
}

/// Persists a [`UsageRecorder`]'s records when dropped. See
/// [`UsageRecorder::persist_on_drop`].
pub struct PersistGuard {
    recorder: UsageRecorder,
    state_path: PathBuf,
}

impl Drop for PersistGuard {
    fn drop(&mut self) {
        let _ = self.recorder.persist(&self.state_path);
    }
}

/// A poisoned lock still holds usable records: a panic elsewhere must not
/// lose them.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
