//! Run-level idempotency-key dedup for `rocky run --idempotency-key`.
//!
//! Before a run starts, the caller asks Rocky: "has this key been seen?"
//! Three outcomes:
//!
//! * **Seen, succeeded** (or any terminal status under
//!   [`DedupPolicy::Any`]) → [`IdempotencyCheck::SkipCompleted`] — exit 0
//!   with `status = skipped_idempotent` and the prior `run_id`.
//! * **Seen, in-flight** (another pod is running the same key) →
//!   [`IdempotencyCheck::SkipInFlight`] — exit 0 with
//!   `status = skipped_in_flight`.
//! * **Unseen** (or existing `InFlight` past its TTL — a crashed-pod
//!   corpse) → [`IdempotencyCheck::Proceed`] / [`IdempotencyCheck::AdoptStale`]
//!   — mint a fresh `run_id`, stamp an `InFlight` entry, run normally, and
//!   update to `Succeeded` / `Failed` at every terminal exit.
//!
//! # Backend matrix
//!
//! The atomic "claim this key if absent" primitive varies per state backend:
//!
//! | Backend | Primitive |
//! |---|---|
//! | [`StateBackend::Local`] | redb write transaction inside the existing `state.redb.lock` file lock; claim, take-over and finalize are each one transaction |
//! | [`StateBackend::Valkey`] / [`StateBackend::Tiered`] | Valkey `SET NX EX` on `{prefix}idempotency:<key>`. Taking over a stale claim, and finalize, are plain `SET` / `DEL` and are not exclusive (tracked separately) |
//! | [`StateBackend::S3`] | S3 `PutObject` with `If-None-Match: "*"`; take-over and finalize are conditional on the ETag, so exactly one adopter wins |
//! | [`StateBackend::Gcs`] | GCS `insertObject` with `x-goog-if-generation-match: 0`; take-over and finalize are conditional on the generation, so exactly one adopter wins |
//!
//! The `s3` / `gcs` backends route through [`IdempotencyBackend::ObjectStore`]
//! using the conditional-PUT primitives in the table above — every
//! [`StateBackend`] variant maps to a working claim path.
//! [`IdempotencyError::UnsupportedBackend`] exists as a defensive error arm
//! consumed by `rocky run`, but nothing in this module constructs it.
//!
//! # Key storage
//!
//! Keys are stored **verbatim** (not hashed) — debuggability over privacy
//! on the premise that "don't put secrets in idempotency keys" is a
//! reasonable documentation constraint. The public CLI help and the
//! Dagster kwarg docstring both carry this warning.
//!
//! # GC
//!
//! `rocky run` sweeps the local `state.redb` table after each run (see
//! `sweep_idempotency_best_effort` in `rocky-cli`). For Valkey backends
//! server-side `EX` handles TTL autonomously and `sweep_expired` is a
//! no-op; a tiered backend sweeps its local mirror. The local sweep deletes
//! entries whose own `expires_at` has passed. An `InFlight` claim carries the
//! `expires_at` set when it was claimed (`claim time + in_flight_ttl_hours` of
//! that run), so lowering `in_flight_ttl_hours` later never makes a sweep
//! remove a live claim.
//!
//! Expired object-store entries are **not swept today**: `rocky run` never
//! calls the object-store sweep, so nothing removes them except a take-over by
//! a later run or a bucket lifecycle rule. [`IdempotencyBackend::sweep_expired`]
//! can sweep them, and removes only `Succeeded` entries past retention.
//!
//! # Object-store writes are conditional
//!
//! On `s3` / `gcs` a take-over of a stale claim is `put_if_match` on the ETag
//! (generation) read together with the claim, so exactly one of two concurrent
//! adopters wins and the loser re-reads and re-classifies. Finalize is
//! `put_if_match` on the claim this run owns, and refuses a claim another run
//! holds. The sweep re-reads a claim right before deleting it and skips it if
//! it changed. The object-store API has no conditional delete, so the sweep
//! deletes only `Succeeded` entries, which no run rewrites; `InFlight`,
//! `Failed` and unreadable entries are left for a take-over. A `Failed`
//! finalize under `dedup_on = "success"` writes an already-expired `Failed`
//! entry instead of deleting, so a retry adopts it with a conditional write.

use std::path::PathBuf;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::{DedupPolicy, IdempotencyConfig, StateBackend, StateConfig};
use crate::object_store::{
    Generation, ObjectStoreError, ObjectStoreProvider, PutIfMatchOutcome, PutIfNotExistsOutcome,
};
use crate::state::{StateError, StateStore};

/// Default Valkey key prefix for idempotency entries (shares the top-level
/// `valkey_prefix` from `StateConfig` when set; otherwise falls back to
/// `"rocky:state:"` to keep the existing Valkey namespace conventions).
const DEFAULT_VALKEY_STATE_PREFIX: &str = "rocky:state:";
/// Default S3 key prefix (mirrors `state_sync::DEFAULT_S3_PREFIX`).
const DEFAULT_S3_PREFIX: &str = "rocky/state/";
/// Default GCS key prefix (mirrors `state_sync::DEFAULT_GCS_PREFIX`).
const DEFAULT_GCS_PREFIX: &str = "rocky/state/";

/// Persisted idempotency-key entry.
///
/// Serialized as JSON into the redb `IDEMPOTENCY_KEYS` table (local / tiered
/// mirror) or as a JSON blob under a Valkey key (valkey / tiered primary).
/// Shape is version-1 — if a field changes semantically bump the top-level
/// state-store schema version alongside it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdempotencyEntry {
    /// The caller-supplied key. Stored verbatim (not hashed).
    pub key: String,
    /// `run_id` of the run that most recently claimed the key.
    pub run_id: String,
    /// Lifecycle state.
    pub state: IdempotencyState,
    /// When the current owner claimed the key (or when the key was last
    /// finalized, for `Succeeded` / `Failed`).
    pub stamped_at: DateTime<Utc>,
    /// When this stamp expires and GC is allowed to reap it. For
    /// `Succeeded` entries this is `stamped_at + retention_days`. For
    /// `InFlight` entries this is `stamped_at + in_flight_ttl_hours` —
    /// readers past this time adopt the stale entry.
    pub expires_at: DateTime<Utc>,
    /// [`DedupPolicy`] in effect at stamp time, captured so a retention
    /// policy change mid-flight doesn't retroactively reinterpret prior
    /// entries.
    pub dedup_on: DedupPolicy,
}

/// Lifecycle state of an idempotency entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IdempotencyState {
    /// A run has claimed the key and is currently executing. Readers hitting
    /// this state before `expires_at` skip with
    /// [`IdempotencyCheck::SkipInFlight`]; readers hitting it past
    /// `expires_at` treat it as crashed-pod corpse and adopt.
    InFlight,
    /// The run that claimed the key completed successfully. Always
    /// dedups under both [`DedupPolicy`] values.
    Succeeded,
    /// The run that claimed the key failed. Dedups only under
    /// [`DedupPolicy::Any`]; under [`DedupPolicy::Success`] the entry is
    /// deleted at finalize time so retries proceed.
    Failed,
}

/// Terminal outcome reported to [`IdempotencyBackend::finalize`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalOutcome {
    Succeeded,
    Failed,
}

/// Result of [`IdempotencyBackend::check_and_claim`].
#[derive(Debug, Clone)]
pub enum IdempotencyCheck {
    /// No prior entry; the caller claimed an `InFlight` stamp and should
    /// proceed to run normally.
    Proceed,
    /// Prior entry exists and dedups — short-circuit.
    SkipCompleted { run_id: String },
    /// Another run is in flight and its TTL has not expired — short-circuit.
    SkipInFlight { run_id: String },
    /// Prior entry was `InFlight` but past its TTL (crashed-pod corpse).
    /// The caller replaced it with a fresh `InFlight` stamp and should
    /// proceed. The prior `run_id` is surfaced so logs / `rocky history`
    /// can cross-reference.
    AdoptStale { prior_run_id: String },
}

impl IdempotencyCheck {
    /// Returns `true` when the caller should short-circuit `rocky run`.
    pub fn is_skip(&self) -> bool {
        matches!(
            self,
            IdempotencyCheck::SkipCompleted { .. } | IdempotencyCheck::SkipInFlight { .. }
        )
    }
}

/// Errors surfaced by the idempotency subsystem.
#[derive(Error)]
pub enum IdempotencyError {
    #[error("state store error: {0}")]
    State(#[from] StateError),

    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("valkey error: {0}")]
    Valkey(String),

    #[error("object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),

    #[error(
        "--idempotency-key requires a state backend that supports atomic \
         set-if-absent. Current backend = \"{backend}\" does not. Switch to \
         \"tiered\" (Valkey + S3 — recommended for multi-pod) or \"valkey\" \
         to enable idempotency, or run without `--idempotency-key`."
    )]
    UnsupportedBackend { backend: String },

    #[error("idempotency backend misconfigured: {0}")]
    Config(String),

    #[error(
        "idempotency claim `{path}` kept changing under concurrent writers; \
         gave up after {attempts} attempts"
    )]
    Contended { path: String, attempts: usize },

    #[error("task join error: {0}")]
    TaskJoin(String),
}

/// `Debug` prints the rendered `Display` text. A derived `Debug` would print
/// the plaintext of every field and wrapped error (#1919).
impl std::fmt::Debug for IdempotencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        crate::secret_registry::fmt_rendered_debug(f, "IdempotencyError", self)
    }
}

/// Idempotency dispatch handle.
///
/// Constructed via [`IdempotencyBackend::from_state_config`]; drives
/// per-backend implementations of the claim / finalize / sweep operations.
#[derive(Debug, Clone)]
pub enum IdempotencyBackend {
    /// Local backend: atomicity via the existing `state.redb.lock` file
    /// lock + a redb write transaction.
    Local,
    /// Valkey / Tiered backend: atomicity via `SET NX EX` on
    /// `{prefix}idempotency:<key>`. `mirror_to_redb` controls whether the
    /// finalized entry is also mirrored into the local state.redb (true
    /// for Tiered so `rocky history` has a local record; false for pure
    /// Valkey).
    Valkey {
        url: String,
        prefix: String,
        mirror_to_redb: bool,
    },
    /// Object-store backend (S3 / GCS) using conditional PUT primitives
    /// (`If-None-Match: "*"` for S3, `x-goog-if-generation-match: 0` for
    /// GCS) to implement atomic claim-if-absent. Objects live at
    /// `{prefix}/idempotency/<sha256(key)>`.
    ObjectStore {
        scheme: ObjectStoreScheme,
        uri: String,
    },
}

/// Which object-store provider backs a given [`IdempotencyBackend::ObjectStore`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectStoreScheme {
    S3,
    Gcs,
}

impl ObjectStoreScheme {
    pub fn label(self) -> &'static str {
        match self {
            ObjectStoreScheme::S3 => "s3",
            ObjectStoreScheme::Gcs => "gcs",
        }
    }
}

impl IdempotencyBackend {
    /// Resolve the backend from the user's `[state]` config.
    pub fn from_state_config(state_config: &StateConfig) -> Self {
        match state_config.backend {
            StateBackend::Local => IdempotencyBackend::Local,
            StateBackend::Valkey => IdempotencyBackend::Valkey {
                url: state_config
                    .valkey_url
                    .as_ref()
                    .map(|s| s.expose().to_string())
                    .unwrap_or_default(),
                prefix: state_config
                    .valkey_prefix
                    .clone()
                    .unwrap_or_else(|| DEFAULT_VALKEY_STATE_PREFIX.to_string()),
                mirror_to_redb: false,
            },
            StateBackend::Tiered => IdempotencyBackend::Valkey {
                url: state_config
                    .valkey_url
                    .as_ref()
                    .map(|s| s.expose().to_string())
                    .unwrap_or_default(),
                prefix: state_config
                    .valkey_prefix
                    .clone()
                    .unwrap_or_else(|| DEFAULT_VALKEY_STATE_PREFIX.to_string()),
                mirror_to_redb: true,
            },
            StateBackend::S3 => {
                let bucket = state_config
                    .s3_bucket
                    .clone()
                    .unwrap_or_else(|| "__missing_bucket__".to_string());
                let prefix = state_config
                    .s3_prefix
                    .as_deref()
                    .unwrap_or(DEFAULT_S3_PREFIX)
                    .trim_end_matches('/');
                let uri = if prefix.is_empty() {
                    format!("s3://{bucket}")
                } else {
                    format!("s3://{bucket}/{prefix}")
                };
                IdempotencyBackend::ObjectStore {
                    scheme: ObjectStoreScheme::S3,
                    uri,
                }
            }
            StateBackend::Gcs => {
                let bucket = state_config
                    .gcs_bucket
                    .clone()
                    .unwrap_or_else(|| "__missing_bucket__".to_string());
                let prefix = state_config
                    .gcs_prefix
                    .as_deref()
                    .unwrap_or(DEFAULT_GCS_PREFIX)
                    .trim_end_matches('/');
                let uri = if prefix.is_empty() {
                    format!("gs://{bucket}")
                } else {
                    format!("gs://{bucket}/{prefix}")
                };
                IdempotencyBackend::ObjectStore {
                    scheme: ObjectStoreScheme::Gcs,
                    uri,
                }
            }
        }
    }

    /// Human-readable label for error messages.
    pub fn backend_label(&self) -> &'static str {
        match self {
            IdempotencyBackend::Local => "local",
            IdempotencyBackend::Valkey {
                mirror_to_redb: true,
                ..
            } => "tiered",
            IdempotencyBackend::Valkey {
                mirror_to_redb: false,
                ..
            } => "valkey",
            IdempotencyBackend::ObjectStore { scheme, .. } => scheme.label(),
        }
    }

    /// Claim an idempotency key atomically. See module-level docs for
    /// outcome semantics.
    ///
    /// `state_store` is always threaded in — Local uses it as primary; Tiered
    /// uses it for the redb mirror; pure Valkey ignores it.
    pub async fn check_and_claim(
        &self,
        state_store: Option<&StateStore>,
        key: &str,
        run_id: &str,
        config: &IdempotencyConfig,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        self.check_and_claim_at(state_store, key, run_id, config, Utc::now())
            .await
    }

    /// [`check_and_claim`](Self::check_and_claim) with an explicit `now`, so a
    /// test can inject the clock. The claim's `stamped_at` is `now`.
    pub async fn check_and_claim_at(
        &self,
        state_store: Option<&StateStore>,
        key: &str,
        run_id: &str,
        config: &IdempotencyConfig,
        now: DateTime<Utc>,
    ) -> Result<IdempotencyCheck, IdempotencyError> {
        let entry = IdempotencyEntry {
            key: key.to_string(),
            run_id: run_id.to_string(),
            state: IdempotencyState::InFlight,
            stamped_at: now,
            expires_at: now + chrono::Duration::hours(config.in_flight_ttl_hours as i64),
            dedup_on: config.dedup_on,
        };
        match self {
            IdempotencyBackend::Local => {
                let store = state_store.ok_or_else(|| {
                    IdempotencyError::Config(
                        "local idempotency backend requires an open StateStore".into(),
                    )
                })?;
                store
                    .idempotency_check_and_claim(&entry, now)
                    .map_err(IdempotencyError::from)
            }
            IdempotencyBackend::Valkey {
                url,
                prefix,
                mirror_to_redb,
            } => {
                let result = valkey_check_and_claim(url, prefix, &entry).await?;
                if let (true, IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. }) =
                    (*mirror_to_redb, &result)
                    && let Some(store) = state_store
                {
                    // Best-effort mirror into local state.redb so the tiered
                    // backend's post-upload snapshot has the same stamp.
                    // Race with Valkey NX is benign — if two pods both reach
                    // this line one will lose the Valkey NX already.
                    if let Err(e) = store.idempotency_put(&entry) {
                        warn!(
                            error = %e,
                            key = %entry.key,
                            "failed to mirror idempotency claim into local state.redb; Valkey remains authoritative"
                        );
                    }
                }
                Ok(result)
            }
            IdempotencyBackend::ObjectStore { uri, .. } => {
                let provider = provider_for_uri(uri)?;
                object_store_check_and_claim(&provider, &entry, now).await
            }
        }
    }

    /// Update an existing `InFlight` stamp to its terminal state, or delete
    /// it entirely when [`DedupPolicy::Success`] + failure semantics apply.
    pub async fn finalize(
        &self,
        state_store: Option<&StateStore>,
        key: &str,
        run_id: &str,
        outcome: FinalOutcome,
        config: &IdempotencyConfig,
    ) -> Result<(), IdempotencyError> {
        self.finalize_at(state_store, key, run_id, outcome, config, Utc::now())
            .await
    }

    /// [`finalize`](Self::finalize) with an explicit `now`.
    pub async fn finalize_at(
        &self,
        state_store: Option<&StateStore>,
        key: &str,
        run_id: &str,
        outcome: FinalOutcome,
        config: &IdempotencyConfig,
        now: DateTime<Utc>,
    ) -> Result<(), IdempotencyError> {
        let retention_until = now + chrono::Duration::days(config.retention_days as i64);
        match self {
            IdempotencyBackend::Local => {
                let store = state_store.ok_or_else(|| {
                    IdempotencyError::Config(
                        "local idempotency backend requires an open StateStore".into(),
                    )
                })?;
                store
                    .idempotency_finalize(key, run_id, outcome, retention_until, config.dedup_on)
                    .map_err(IdempotencyError::from)
            }
            IdempotencyBackend::Valkey {
                url,
                prefix,
                mirror_to_redb,
            } => {
                valkey_finalize(url, prefix, key, run_id, outcome, retention_until, config).await?;
                if *mirror_to_redb && let Some(store) = state_store {
                    // Mirror finalized state into local redb best-effort; see
                    // claim path for the race reasoning.
                    let should_keep = match (outcome, config.dedup_on) {
                        (FinalOutcome::Succeeded, _) => true,
                        (FinalOutcome::Failed, DedupPolicy::Any) => true,
                        (FinalOutcome::Failed, DedupPolicy::Success) => false,
                    };
                    let result = if should_keep {
                        let entry = IdempotencyEntry {
                            key: key.to_string(),
                            run_id: run_id.to_string(),
                            state: match outcome {
                                FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                                FinalOutcome::Failed => IdempotencyState::Failed,
                            },
                            stamped_at: now,
                            expires_at: retention_until,
                            dedup_on: config.dedup_on,
                        };
                        store.idempotency_put(&entry)
                    } else {
                        store.idempotency_delete(key).map(|_| ())
                    };
                    if let Err(e) = result {
                        warn!(
                            error = %e,
                            key = %key,
                            "failed to mirror idempotency finalize into local state.redb; Valkey remains authoritative"
                        );
                    }
                }
                Ok(())
            }
            IdempotencyBackend::ObjectStore { uri, .. } => {
                let provider = provider_for_uri(uri)?;
                object_store_finalize(
                    &provider,
                    key,
                    run_id,
                    outcome,
                    retention_until,
                    config,
                    now,
                )
                .await
            }
        }
    }

    /// Sweep expired entries.
    ///
    /// - Local: iterates the redb table and removes entries whose own
    ///   `expires_at < now`. An `InFlight` claim's `expires_at` was set when it
    ///   was claimed, so the current `in_flight_ttl_hours` plays no part.
    /// - Valkey: no-op (server-side `EX` handles TTL); Tiered sweeps the
    ///   local mirror.
    /// - Object store: lists, re-reads, and deletes `Succeeded` entries past
    ///   retention; see the module docs. `rocky run` does not call this today.
    ///
    /// Returns the number of entries swept.
    pub async fn sweep_expired(
        &self,
        state_store: Option<&StateStore>,
    ) -> Result<usize, IdempotencyError> {
        self.sweep_expired_at(state_store, Utc::now()).await
    }

    /// [`sweep_expired`](Self::sweep_expired) with an explicit `now`.
    pub async fn sweep_expired_at(
        &self,
        state_store: Option<&StateStore>,
        now: DateTime<Utc>,
    ) -> Result<usize, IdempotencyError> {
        match self {
            IdempotencyBackend::Local => match state_store {
                Some(store) => Ok(store.idempotency_sweep_expired(now)?),
                None => Ok(0),
            },
            IdempotencyBackend::Valkey { mirror_to_redb, .. } => {
                if *mirror_to_redb && let Some(store) = state_store {
                    // Tiered: sweep the local mirror too (Valkey handles its own).
                    Ok(store.idempotency_sweep_expired(now)?)
                } else {
                    Ok(0)
                }
            }
            IdempotencyBackend::ObjectStore { uri, .. } => {
                // The recommended GC path for S3/GCS is a bucket lifecycle
                // rule. For operators who can't configure that, this is an
                // active sweep: list, parse, re-check, delete-if-expired.
                // Best-effort and non-authoritative.
                let provider = provider_for_uri(uri)?;
                object_store_sweep_expired(&provider, now).await
            }
        }
    }
}

// -----------------------------------------------------------------------
// Valkey implementation (SET NX EX)
// -----------------------------------------------------------------------

fn valkey_key(prefix: &str, user_key: &str) -> String {
    let sep = if prefix.ends_with(':') || prefix.is_empty() {
        ""
    } else {
        ":"
    };
    format!("{prefix}{sep}idempotency:{user_key}")
}

async fn valkey_check_and_claim(
    url: &str,
    prefix: &str,
    entry: &IdempotencyEntry,
) -> Result<IdempotencyCheck, IdempotencyError> {
    if url.is_empty() {
        return Err(IdempotencyError::Config(
            "valkey backend selected but `state.valkey_url` is unset".into(),
        ));
    }
    let full_key = valkey_key(prefix, &entry.key);
    let value = serde_json::to_vec(entry)?;
    let ttl_secs = entry
        .expires_at
        .signed_duration_since(entry.stamped_at)
        .num_seconds()
        .max(1) as u64;

    let url = url.to_string();
    let key_for_task = full_key.clone();

    let outcome: ValkeyClaimOutcome =
        tokio::task::spawn_blocking(move || -> Result<ValkeyClaimOutcome, IdempotencyError> {
            let client =
                redis::Client::open(url).map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
            let mut conn = client
                .get_connection()
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

            // Try to claim atomically: SET key value NX EX ttl
            let reply: Option<String> = redis::cmd("SET")
                .arg(&key_for_task)
                .arg(&value)
                .arg("NX")
                .arg("EX")
                .arg(ttl_secs)
                .query(&mut conn)
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

            if reply.as_deref() == Some("OK") {
                return Ok(ValkeyClaimOutcome::Claimed);
            }

            // Key exists — read existing entry to classify.
            let existing: Option<Vec<u8>> = redis::cmd("GET")
                .arg(&key_for_task)
                .query(&mut conn)
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

            match existing {
                Some(bytes) => {
                    let prior: IdempotencyEntry = serde_json::from_slice(&bytes)?;
                    Ok(ValkeyClaimOutcome::Existing(prior))
                }
                None => {
                    // Race: key was TTL'd between SET NX failure and GET. Re-try once.
                    let retry: Option<String> = redis::cmd("SET")
                        .arg(&key_for_task)
                        .arg(&value)
                        .arg("NX")
                        .arg("EX")
                        .arg(ttl_secs)
                        .query(&mut conn)
                        .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
                    if retry.as_deref() == Some("OK") {
                        Ok(ValkeyClaimOutcome::Claimed)
                    } else {
                        // Another claimer won the retry race; re-read.
                        let bytes: Option<Vec<u8>> = redis::cmd("GET")
                            .arg(&key_for_task)
                            .query(&mut conn)
                            .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
                        match bytes {
                            Some(b) => {
                                let prior: IdempotencyEntry = serde_json::from_slice(&b)?;
                                Ok(ValkeyClaimOutcome::Existing(prior))
                            }
                            None => Ok(ValkeyClaimOutcome::Claimed),
                        }
                    }
                }
            }
        })
        .await
        .map_err(|e| IdempotencyError::TaskJoin(e.to_string()))??;

    match outcome {
        ValkeyClaimOutcome::Claimed => {
            debug!(key = %full_key, "claimed idempotency key via SET NX EX");
            Ok(IdempotencyCheck::Proceed)
        }
        ValkeyClaimOutcome::Existing(prior) => Ok(classify_existing(prior, Utc::now(), entry)),
    }
}

async fn valkey_finalize(
    url: &str,
    prefix: &str,
    key: &str,
    run_id: &str,
    outcome: FinalOutcome,
    retention_until: DateTime<Utc>,
    config: &IdempotencyConfig,
) -> Result<(), IdempotencyError> {
    if url.is_empty() {
        return Ok(());
    }
    let full_key = valkey_key(prefix, key);
    let should_keep = match (outcome, config.dedup_on) {
        (FinalOutcome::Succeeded, _) => true,
        (FinalOutcome::Failed, DedupPolicy::Any) => true,
        (FinalOutcome::Failed, DedupPolicy::Success) => false,
    };

    let url = url.to_string();
    let key_for_task = full_key.clone();
    let run_id = run_id.to_string();
    let user_key = key.to_string();
    let dedup_on = config.dedup_on;
    let ttl_secs = retention_until
        .signed_duration_since(Utc::now())
        .num_seconds()
        .max(1) as u64;

    tokio::task::spawn_blocking(move || -> Result<(), IdempotencyError> {
        let client =
            redis::Client::open(url).map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

        if should_keep {
            let entry = IdempotencyEntry {
                key: user_key,
                run_id,
                state: match outcome {
                    FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                    FinalOutcome::Failed => IdempotencyState::Failed,
                },
                stamped_at: Utc::now(),
                expires_at: retention_until,
                dedup_on,
            };
            let value = serde_json::to_vec(&entry)?;
            // SET key value EX retention — overwrites the InFlight entry
            // with the terminal state + retention TTL.
            redis::cmd("SET")
                .arg(&key_for_task)
                .arg(value)
                .arg("EX")
                .arg(ttl_secs)
                .query::<()>(&mut conn)
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
        } else {
            // Dedup-on-success + failure: delete the InFlight so a retry
            // can claim the key freely.
            redis::cmd("DEL")
                .arg(&key_for_task)
                .query::<()>(&mut conn)
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
        }
        Ok(())
    })
    .await
    .map_err(|e| IdempotencyError::TaskJoin(e.to_string()))?
}

#[derive(Debug)]
enum ValkeyClaimOutcome {
    Claimed,
    Existing(IdempotencyEntry),
}

// -----------------------------------------------------------------------
// Object-store implementation (PutMode::Create = If-None-Match/generation)
// -----------------------------------------------------------------------

/// Relative path inside the object store prefix where an idempotency entry
/// is persisted. Digest keeps the path URL-safe + bounded regardless of
/// caller-supplied key shape.
fn object_store_relative_path(key: &str) -> String {
    object_store_idempotency_path(key)
        .to_string_lossy()
        .to_string()
}

/// How many times a conditional write is retried after losing a race, before
/// the call gives up with an error. Each retry re-reads and re-classifies.
const MAX_CONDITIONAL_ATTEMPTS: usize = 5;

/// Build the provider for an object-store idempotency URI.
///
/// Test seam: under the `test-support` feature (or in this crate's own tests)
/// an installed provider override wins, so tests run the real claim, finalize
/// and sweep code over an in-memory store. The `rocky` binary never enables
/// the feature.
fn provider_for_uri(uri: &str) -> Result<ObjectStoreProvider, IdempotencyError> {
    #[cfg(any(test, feature = "test-support"))]
    if let Some(provider) = crate::state_sync::test_provider_override() {
        return Ok(provider);
    }
    Ok(ObjectStoreProvider::from_uri(uri)?)
}

/// A claim object as read from the store: its body, version and parse.
///
/// `entry` is `None` when the body is not a valid [`IdempotencyEntry`].
#[derive(Debug, Clone)]
pub struct ObservedClaim {
    path: String,
    body: Bytes,
    generation: Option<Generation>,
    entry: Option<IdempotencyEntry>,
}

/// The object was deleted between a listing and a read.
fn is_gone(e: &ObjectStoreError) -> bool {
    matches!(
        e,
        ObjectStoreError::Backend(object_store::Error::NotFound { .. })
    )
}

/// Read the claim at `rel_path` with its version, from one read. `None` when
/// the object does not exist.
async fn observe_claim_at_path(
    provider: &ObjectStoreProvider,
    rel_path: &str,
) -> Result<Option<ObservedClaim>, IdempotencyError> {
    let (body, generation) = match provider.get_with_generation(rel_path).await {
        Ok(read) => read,
        Err(e) if is_gone(&e) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let entry = serde_json::from_slice(&body).ok();
    Ok(Some(ObservedClaim {
        path: rel_path.to_string(),
        body,
        generation,
        entry,
    }))
}

/// A backend that reports no ETag or generation cannot compare-and-swap. Fail
/// rather than fall back to a blind write, which is the race this code closes.
fn require_generation(observed: &ObservedClaim) -> Result<&Generation, IdempotencyError> {
    observed.generation.as_ref().ok_or_else(|| {
        IdempotencyError::Config(format!(
            "the object store returned no ETag or generation for `{}`, so the idempotency \
             claim cannot be updated with a conditional write",
            observed.path
        ))
    })
}

fn contended(path: &str) -> IdempotencyError {
    IdempotencyError::Contended {
        path: path.to_string(),
        attempts: MAX_CONDITIONAL_ATTEMPTS,
    }
}

async fn object_store_check_and_claim(
    provider: &ObjectStoreProvider,
    entry: &IdempotencyEntry,
    now: DateTime<Utc>,
) -> Result<IdempotencyCheck, IdempotencyError> {
    let rel_path = object_store_relative_path(&entry.key);
    let encoded = Bytes::from(serde_json::to_vec(entry)?);

    for _ in 0..MAX_CONDITIONAL_ATTEMPTS {
        match provider
            .put_if_not_exists(&rel_path, encoded.clone())
            .await?
        {
            PutIfNotExistsOutcome::Created => {
                debug!(path = %rel_path, "claimed idempotency key via conditional PUT");
                return Ok(IdempotencyCheck::Proceed);
            }
            PutIfNotExistsOutcome::AlreadyExists => {}
        }

        // Read the existing claim and its version in one call.
        let Some(observed) = observe_claim_at_path(provider, &rel_path).await? else {
            // Deleted between the failed create and the read: create again.
            continue;
        };
        let verdict = match observed.entry.clone() {
            Some(prior) => classify_existing(prior, now, entry),
            None => {
                warn!(
                    path = %rel_path,
                    "idempotency entry is not deserializable; treating it as stale"
                );
                IdempotencyCheck::AdoptStale {
                    prior_run_id: "<corrupt>".to_string(),
                }
            }
        };
        match &verdict {
            IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. } => {
                // Take over only if the object is still the one we read. When
                // two runs adopt at once, exactly one write commits; the
                // other sees a conflict, re-reads the winner's fresh claim and
                // skips as in-flight.
                let generation = require_generation(&observed)?;
                match provider
                    .put_if_match(&rel_path, encoded.clone(), Some(generation))
                    .await?
                {
                    PutIfMatchOutcome::Committed(_) => return Ok(verdict),
                    PutIfMatchOutcome::Conflict => continue,
                }
            }
            IdempotencyCheck::SkipCompleted { .. } | IdempotencyCheck::SkipInFlight { .. } => {
                return Ok(verdict);
            }
        }
    }
    Err(contended(&rel_path))
}

async fn object_store_finalize(
    provider: &ObjectStoreProvider,
    key: &str,
    run_id: &str,
    outcome: FinalOutcome,
    retention_until: DateTime<Utc>,
    config: &IdempotencyConfig,
    now: DateTime<Utc>,
) -> Result<(), IdempotencyError> {
    let rel_path = object_store_relative_path(key);

    let should_keep = match (outcome, config.dedup_on) {
        (FinalOutcome::Succeeded, _) => true,
        (FinalOutcome::Failed, DedupPolicy::Any) => true,
        (FinalOutcome::Failed, DedupPolicy::Success) => false,
    };
    // Dedup-on-success + failure frees the key for a retry. The object-store
    // API has no conditional delete, so this writes a `Failed` entry that is
    // already expired instead: a retry classifies it as stale and takes it
    // over with a conditional write, and it cannot erase a newer claim. Its
    // `expires_at` is the claim's own `stamped_at`, a time clearly in the past,
    // not `now`: a pod whose clock runs slower than ours must not read it as
    // live.
    let encode = |expires_at: DateTime<Utc>| -> Result<Bytes, IdempotencyError> {
        let updated = IdempotencyEntry {
            key: key.to_string(),
            run_id: run_id.to_string(),
            state: match outcome {
                FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                FinalOutcome::Failed => IdempotencyState::Failed,
            },
            stamped_at: now,
            expires_at,
            dedup_on: config.dedup_on,
        };
        Ok(Bytes::from(serde_json::to_vec(&updated)?))
    };

    for _ in 0..MAX_CONDITIONAL_ATTEMPTS {
        let Some(observed) = observe_claim_at_path(provider, &rel_path).await? else {
            // The claim is gone (an operator or a bucket lifecycle rule
            // removed it). Record the outcome only if nobody has claimed the
            // key since. A freed key needs no record.
            if !should_keep {
                return Ok(());
            }
            match provider
                .put_if_match(&rel_path, encode(retention_until)?, None)
                .await?
            {
                PutIfMatchOutcome::Committed(_) => return Ok(()),
                PutIfMatchOutcome::Conflict => continue,
            }
        };
        let owned = observed
            .entry
            .as_ref()
            .is_some_and(|prior| prior.run_id == run_id);
        if !owned {
            // Another run took over (or the object is unreadable). Leave it:
            // overwriting would erase the new owner's claim.
            warn!(
                path = %rel_path,
                run_id = %run_id,
                "idempotency claim is held by another run; not finalizing it"
            );
            return Ok(());
        }
        let generation = require_generation(&observed)?;
        let expires_at = if should_keep {
            retention_until
        } else {
            // Owned, so the claim parsed: back-date to its own stamp.
            observed
                .entry
                .as_ref()
                .map_or(now, |prior| prior.stamped_at)
                .min(now)
        };
        match provider
            .put_if_match(&rel_path, encode(expires_at)?, Some(generation))
            .await?
        {
            PutIfMatchOutcome::Committed(_) => return Ok(()),
            // Changed since the read: re-read, and the not-owned path above
            // handles an adopter that won.
            PutIfMatchOutcome::Conflict => continue,
        }
    }
    Err(contended(&rel_path))
}

async fn object_store_sweep_expired(
    provider: &ObjectStoreProvider,
    now: DateTime<Utc>,
) -> Result<usize, IdempotencyError> {
    // Active sweep for object-store backends: list `idempotency/*`, read each
    // claim, delete a `Succeeded` entry past its retention. Errors are
    // warn-swallowed so a flaky bucket doesn't fail the run.
    let paths = match provider.list("idempotency").await {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "idempotency sweep list failed; bucket-lifecycle rules are the durable GC path");
            return Ok(0);
        }
    };
    let mut removed = 0;
    for path in paths {
        let rel_path = format!("idempotency/{}", path.trim_start_matches("idempotency/"));
        let observed = match observe_claim_at_path(provider, &rel_path).await {
            Ok(Some(observed)) => observed,
            Ok(None) => continue,
            Err(e) => {
                warn!(error = %e, path = %rel_path, "idempotency sweep could not read an entry; skipping it");
                continue;
            }
        };
        if observed.entry.is_none() {
            warn!(path = %rel_path, "idempotency sweep skipped an entry that is not deserializable");
            continue;
        }
        if let Ok(true) = sweep_observed_claim(provider, &observed, now).await {
            removed += 1;
        }
    }
    Ok(removed)
}

/// Delete `observed` if it is a `Succeeded` entry past its retention and is
/// still the object in the store.
///
/// Only `Succeeded` entries are swept. The object-store API has no
/// conditional delete, so a read-then-delete can remove an object a run wrote
/// in between. A `Succeeded` entry is never rewritten by a claim, finalize or
/// take-over (a later run skips it as completed), so nothing can replace it
/// in that window. `InFlight`, `Failed` and unreadable entries are all
/// taken over with a conditional write, so the sweep leaves them: they stay
/// until a run adopts them or a bucket lifecycle rule removes them.
async fn sweep_observed_claim(
    provider: &ObjectStoreProvider,
    observed: &ObservedClaim,
    now: DateTime<Utc>,
) -> Result<bool, IdempotencyError> {
    let expired_success = |claim: &ObservedClaim| {
        claim.entry.as_ref().is_some_and(|entry| {
            entry.state == IdempotencyState::Succeeded && entry.expires_at < now
        })
    };
    if !expired_success(observed) {
        return Ok(false);
    }
    let Some(current) = observe_claim_at_path(provider, &observed.path).await? else {
        return Ok(false);
    };
    let unchanged = match (&observed.generation, &current.generation) {
        (Some(before), Some(after)) => before == after,
        _ => observed.body == current.body,
    };
    if !unchanged || !expired_success(&current) {
        return Ok(false);
    }
    provider.delete(&observed.path).await?;
    Ok(true)
}

/// Seams for tests that need to interleave a sweep with another writer.
/// Compiled only for this crate's tests or under `test-support`.
#[cfg(any(test, feature = "test-support"))]
pub mod testing {
    use super::*;

    /// Read the claim for `key` the way the sweep does.
    pub async fn observe_claim(
        provider: &ObjectStoreProvider,
        key: &str,
    ) -> Result<Option<ObservedClaim>, IdempotencyError> {
        observe_claim_at_path(provider, &object_store_relative_path(key)).await
    }

    /// Run the sweep's delete step on a claim read earlier. `Ok(true)` when
    /// it deleted the object.
    pub async fn sweep_observed(
        provider: &ObjectStoreProvider,
        observed: &ObservedClaim,
        now: DateTime<Utc>,
    ) -> Result<bool, IdempotencyError> {
        sweep_observed_claim(provider, observed, now).await
    }
}

// -----------------------------------------------------------------------
// Shared classification logic
// -----------------------------------------------------------------------

/// Given a prior entry, decide the [`IdempotencyCheck`] outcome.
///
/// Used by both Local (inside the redb write txn — if Proceed / AdoptStale
/// the store then inserts the new entry) and Valkey (after a failed
/// SET NX). Centralising the branching keeps the two backends' semantics
/// bit-for-bit identical.
pub(crate) fn classify_existing(
    prior: IdempotencyEntry,
    now: DateTime<Utc>,
    new_entry: &IdempotencyEntry,
) -> IdempotencyCheck {
    // One rule for every backend: an entry past its own `expires_at` is dead,
    // whatever its state. This must come before the state match. Otherwise a
    // `Failed` entry written already expired (the object-store finalize under
    // `dedup_on = "success"`) would dedup a later `dedup_on = "any"` run as
    // completed work that never succeeded.
    if prior.expires_at <= now {
        if prior.state == IdempotencyState::InFlight {
            info!(
                key = %prior.key,
                prior_run_id = %prior.run_id,
                "adopting stale InFlight idempotency entry past TTL"
            );
        }
        return IdempotencyCheck::AdoptStale {
            prior_run_id: prior.run_id,
        };
    }
    match prior.state {
        IdempotencyState::Succeeded => IdempotencyCheck::SkipCompleted {
            run_id: prior.run_id,
        },
        IdempotencyState::Failed => match new_entry.dedup_on {
            DedupPolicy::Any => IdempotencyCheck::SkipCompleted {
                run_id: prior.run_id,
            },
            // A live failure under `dedup_on = "success"` does not block a
            // retry; AdoptStale surfaces the prior run_id for log
            // cross-reference.
            DedupPolicy::Success => IdempotencyCheck::AdoptStale {
                prior_run_id: prior.run_id,
            },
        },
        IdempotencyState::InFlight => IdempotencyCheck::SkipInFlight {
            run_id: prior.run_id,
        },
    }
}

// -----------------------------------------------------------------------
// Phase 2/3 placeholders — kept here so the compile surface already knows
// the types, even though the concrete providers land in later patches.
// -----------------------------------------------------------------------

/// Object-store–backed idempotency key path (S3 / GCS).
///
/// Layout: `idempotency/<sha256-hex>` — hashed to keep the object name
/// URL-safe + bounded regardless of caller-supplied key shape. The key
/// itself is stored verbatim inside the object payload so `rocky history`
/// and operator introspection can recover it.
pub fn object_store_idempotency_path(key: &str) -> PathBuf {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let hex = hasher
        .finalize()
        .iter()
        .fold(String::with_capacity(64), |mut acc, byte| {
            use std::fmt::Write;
            let _ = write!(acc, "{byte:02x}");
            acc
        });
    PathBuf::from(format!("idempotency/{hex}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The `Debug` output prints a resolved `${VAR}` value as `${NAME}` (#1919).
    #[test]
    fn idempotency_error_debug_prints_a_resolved_value_as_its_name() {
        const SECRET: &str = "idem-valkey-1919-c9d0";
        crate::secret_registry::register_substitution("RV_IDEM_DBG", SECRET);
        let err = IdempotencyError::Valkey(format!("{SECRET}:6379 refused"));
        let debug = format!("{err:?}");
        assert!(!debug.contains(SECRET), "Debug leaks: {debug}");
        assert!(debug.contains("${RV_IDEM_DBG}"), "{debug}");
    }
    use crate::config::DedupPolicy;

    fn mk_entry(
        run_id: &str,
        state: IdempotencyState,
        stamped: DateTime<Utc>,
        ttl_hours: i64,
    ) -> IdempotencyEntry {
        IdempotencyEntry {
            key: "k".into(),
            run_id: run_id.into(),
            state,
            stamped_at: stamped,
            expires_at: stamped + chrono::Duration::hours(ttl_hours),
            dedup_on: DedupPolicy::Success,
        }
    }

    #[test]
    fn classify_succeeded_always_skips() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::Succeeded,
            now - chrono::Duration::hours(1),
            24,
        );
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::SkipCompleted { run_id } => assert_eq!(run_id, "run-prior"),
            other => panic!("expected SkipCompleted, got {other:?}"),
        }
    }

    #[test]
    fn classify_failed_with_dedup_success_adopts() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::Failed,
            now - chrono::Duration::hours(1),
            24,
        );
        let mut new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        new.dedup_on = DedupPolicy::Success;
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::AdoptStale { prior_run_id } => assert_eq!(prior_run_id, "run-prior"),
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }

    #[test]
    fn classify_failed_with_dedup_any_skips() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::Failed,
            now - chrono::Duration::hours(1),
            24,
        );
        let mut new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        new.dedup_on = DedupPolicy::Any;
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::SkipCompleted { run_id } => assert_eq!(run_id, "run-prior"),
            other => panic!("expected SkipCompleted, got {other:?}"),
        }
    }

    #[test]
    fn classify_inflight_within_ttl_skips() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::InFlight,
            now - chrono::Duration::minutes(30),
            24,
        );
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::SkipInFlight { run_id } => assert_eq!(run_id, "run-prior"),
            other => panic!("expected SkipInFlight, got {other:?}"),
        }
    }

    #[test]
    fn classify_inflight_past_ttl_adopts() {
        let now = Utc::now();
        // stamped 25 hours ago with a 24h TTL — past stale.
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::InFlight,
            now - chrono::Duration::hours(25),
            24,
        );
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::AdoptStale { prior_run_id } => assert_eq!(prior_run_id, "run-prior"),
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }

    #[test]
    fn is_skip_distinguishes_proceed() {
        assert!(!IdempotencyCheck::Proceed.is_skip());
        assert!(
            !IdempotencyCheck::AdoptStale {
                prior_run_id: "x".into()
            }
            .is_skip()
        );
        assert!(IdempotencyCheck::SkipCompleted { run_id: "x".into() }.is_skip());
        assert!(IdempotencyCheck::SkipInFlight { run_id: "x".into() }.is_skip());
    }

    #[test]
    fn object_store_path_encodes_unsafe_chars() {
        let p = object_store_idempotency_path("fivetran:conn_abc/2026-04-22T14:00:00Z");
        let path = p.to_str().unwrap();
        assert!(path.starts_with("idempotency/"));
        // Deterministic hex digest, no path-unsafe characters.
        let suffix = &path["idempotency/".len()..];
        assert_eq!(suffix.len(), 64);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn object_store_path_is_deterministic() {
        let a = object_store_idempotency_path("same-key");
        let b = object_store_idempotency_path("same-key");
        assert_eq!(a, b);
        let c = object_store_idempotency_path("other-key");
        assert_ne!(a, c);
    }

    #[test]
    fn s3_backend_resolves_to_object_store() {
        let sc = StateConfig {
            backend: StateBackend::S3,
            s3_bucket: Some("my-bucket".into()),
            s3_prefix: Some("rocky/state/".into()),
            ..StateConfig::default()
        };
        let backend = IdempotencyBackend::from_state_config(&sc);
        match &backend {
            IdempotencyBackend::ObjectStore { scheme, uri } => {
                assert_eq!(*scheme, ObjectStoreScheme::S3);
                assert_eq!(uri, "s3://my-bucket/rocky/state");
            }
            other => panic!("expected ObjectStore, got {other:?}"),
        }
        assert_eq!(backend.backend_label(), "s3");
    }

    #[test]
    fn gcs_backend_resolves_to_object_store() {
        let sc = StateConfig {
            backend: StateBackend::Gcs,
            gcs_bucket: Some("my-gcs-bucket".into()),
            gcs_prefix: Some("rocky/state/".into()),
            ..StateConfig::default()
        };
        let backend = IdempotencyBackend::from_state_config(&sc);
        match &backend {
            IdempotencyBackend::ObjectStore { scheme, uri } => {
                assert_eq!(*scheme, ObjectStoreScheme::Gcs);
                assert_eq!(uri, "gs://my-gcs-bucket/rocky/state");
            }
            other => panic!("expected ObjectStore, got {other:?}"),
        }
        assert_eq!(backend.backend_label(), "gcs");
    }

    #[test]
    fn local_backend_label() {
        let sc = StateConfig::default();
        let backend = IdempotencyBackend::from_state_config(&sc);
        assert!(matches!(backend, IdempotencyBackend::Local));
        assert_eq!(backend.backend_label(), "local");
    }

    #[test]
    fn tiered_backend_mirrors_to_redb() {
        let sc = StateConfig {
            backend: StateBackend::Tiered,
            valkey_url: Some(crate::redacted::RedactedString::new(
                "redis://localhost:6379".into(),
            )),
            ..StateConfig::default()
        };
        let backend = IdempotencyBackend::from_state_config(&sc);
        match backend {
            IdempotencyBackend::Valkey { mirror_to_redb, .. } => assert!(mirror_to_redb),
            other => panic!("expected Valkey, got {other:?}"),
        }
    }

    #[test]
    fn valkey_key_format() {
        assert_eq!(
            valkey_key("rocky:state:", "abc"),
            "rocky:state:idempotency:abc"
        );
        assert_eq!(
            valkey_key("rocky:state", "abc"),
            "rocky:state:idempotency:abc"
        );
        assert_eq!(valkey_key("", "abc"), "idempotency:abc");
    }

    // -------------------------------------------------------------------
    // Expiry rule (no server needed)
    // -------------------------------------------------------------------

    #[test]
    fn classify_succeeded_past_its_expiry_adopts() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::Succeeded,
            now - chrono::Duration::hours(25),
            24,
        );
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::AdoptStale { prior_run_id } => assert_eq!(prior_run_id, "run-prior"),
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }

    #[test]
    fn classify_failed_past_its_expiry_adopts_under_dedup_on_any() {
        let now = Utc::now();
        let prior = mk_entry(
            "run-prior",
            IdempotencyState::Failed,
            now - chrono::Duration::hours(25),
            24,
        );
        let mut new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        new.dedup_on = DedupPolicy::Any;
        match classify_existing(prior, now, &new) {
            IdempotencyCheck::AdoptStale { prior_run_id } => assert_eq!(prior_run_id, "run-prior"),
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }
}
