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
//! | [`StateBackend::Local`] | redb write transaction inside the existing `state.redb.lock` file lock |
//! | [`StateBackend::Valkey`] / [`StateBackend::Tiered`] | Valkey `SET NX EX` on `{prefix}idempotency:<key>` |
//! | [`StateBackend::S3`] | S3 `PutObject` with `If-None-Match: "*"` |
//! | [`StateBackend::Gcs`] | GCS `insertObject` with `x-goog-if-generation-match: 0` |
//!
//! The `s3` / `gcs` backends are not yet wired through this module — calls
//! return [`IdempotencyError::UnsupportedBackend`], consumed by `rocky run`
//! at flag-parse time with a clear "switch to tiered" error message.
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
//! Expired stamps are swept during [`crate::state_sync::upload_state`] —
//! reuses the existing post-run cadence, no new cron. For Valkey backends
//! server-side `EX` handles TTL autonomously and `sweep_expired` is a
//! no-op. For local, the sweep deletes entries whose `expires_at` has
//! passed OR whose `state = InFlight` has aged past
//! `in_flight_ttl_hours` (crashed-pod reaping).

use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::config::{DedupPolicy, IdempotencyConfig, StateBackend, StateConfig};
use crate::object_store::{ObjectStoreError, ObjectStoreProvider, PutIfNotExistsOutcome};
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
#[derive(Debug, Error)]
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

    #[error("task join error: {0}")]
    TaskJoin(String),
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
                url: state_config.valkey_url.clone().unwrap_or_default(),
                prefix: state_config
                    .valkey_prefix
                    .clone()
                    .unwrap_or_else(|| DEFAULT_VALKEY_STATE_PREFIX.to_string()),
                mirror_to_redb: false,
            },
            StateBackend::Tiered => IdempotencyBackend::Valkey {
                url: state_config.valkey_url.clone().unwrap_or_default(),
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
        let now = Utc::now();
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
                object_store_check_and_claim(uri, &entry).await
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
        let now = Utc::now();
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
                object_store_finalize(uri, key, run_id, outcome, retention_until, config).await
            }
        }
    }

    /// Sweep expired entries.
    ///
    /// - Local: iterates the redb table and removes entries whose
    ///   `expires_at < now`, plus any `InFlight` entries whose
    ///   `stamped_at + in_flight_ttl_hours < now`.
    /// - Valkey: no-op (server-side `EX` handles TTL).
    /// - Unsupported: no-op (nothing stored yet).
    ///
    /// Returns the number of entries swept.
    pub async fn sweep_expired(
        &self,
        state_store: Option<&StateStore>,
        config: &IdempotencyConfig,
    ) -> Result<usize, IdempotencyError> {
        match self {
            IdempotencyBackend::Local => match state_store {
                Some(store) => {
                    Ok(store.idempotency_sweep_expired(Utc::now(), config.in_flight_ttl_hours)?)
                }
                None => Ok(0),
            },
            IdempotencyBackend::Valkey { mirror_to_redb, .. } => {
                if *mirror_to_redb && let Some(store) = state_store {
                    // Tiered: sweep the local mirror too (Valkey handles its own).
                    Ok(store.idempotency_sweep_expired(Utc::now(), config.in_flight_ttl_hours)?)
                } else {
                    Ok(0)
                }
            }
            IdempotencyBackend::ObjectStore { uri, .. } => {
                // The recommended GC path for S3/GCS is a bucket lifecycle
                // rule (Rocky writes objects with a path prefix operators
                // target via `Lifecycle` / `bucket.lifecycle` policies). For
                // operators who can't configure that, we also offer an
                // active sweep: list → parse → delete-if-expired. Best-
                // effort and non-authoritative — the bucket-lifecycle path
                // is the durable one.
                object_store_sweep_expired(uri, config).await
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

async fn object_store_check_and_claim(
    uri: &str,
    entry: &IdempotencyEntry,
) -> Result<IdempotencyCheck, IdempotencyError> {
    let provider = ObjectStoreProvider::from_uri(uri)?;
    let rel_path = object_store_relative_path(&entry.key);
    let encoded = serde_json::to_vec(entry)?;

    let outcome = provider
        .put_if_not_exists(&rel_path, Bytes::from(encoded))
        .await?;

    match outcome {
        PutIfNotExistsOutcome::Created => {
            debug!(path = %rel_path, "claimed idempotency key via conditional PUT");
            Ok(IdempotencyCheck::Proceed)
        }
        PutIfNotExistsOutcome::AlreadyExists => {
            // Read the existing object to classify.
            let body = provider.get(&rel_path).await?;
            let prior: IdempotencyEntry = match serde_json::from_slice(&body) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        error = %e,
                        path = %rel_path,
                        "idempotency entry is not deserializable — treating as stale and overwriting"
                    );
                    // Overwrite the corrupt object with a fresh InFlight claim.
                    // Use plain put (not conditional) since we've already lost
                    // the race atomicity once — best-effort recovery.
                    let bytes = serde_json::to_vec(entry)?;
                    provider.put(&rel_path, Bytes::from(bytes)).await?;
                    return Ok(IdempotencyCheck::AdoptStale {
                        prior_run_id: "<corrupt>".to_string(),
                    });
                }
            };
            let verdict = classify_existing(prior, Utc::now(), entry);
            match &verdict {
                IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. } => {
                    // Stale / adoptable prior — overwrite with our InFlight.
                    // Small race: another claimer might also have adopted. The
                    // last-writer-wins overwrite is acceptable because either
                    // claimant ultimately finalizes and the entry becomes
                    // Succeeded regardless. For dedup-on-success+Failed
                    // entries the sweep removes stale entries entirely.
                    let bytes = serde_json::to_vec(entry)?;
                    provider.put(&rel_path, Bytes::from(bytes)).await?;
                }
                IdempotencyCheck::SkipCompleted { .. } | IdempotencyCheck::SkipInFlight { .. } => {}
            }
            Ok(verdict)
        }
    }
}

async fn object_store_finalize(
    uri: &str,
    key: &str,
    run_id: &str,
    outcome: FinalOutcome,
    retention_until: DateTime<Utc>,
    config: &IdempotencyConfig,
) -> Result<(), IdempotencyError> {
    let provider = ObjectStoreProvider::from_uri(uri)?;
    let rel_path = object_store_relative_path(key);

    let should_keep = match (outcome, config.dedup_on) {
        (FinalOutcome::Succeeded, _) => true,
        (FinalOutcome::Failed, DedupPolicy::Any) => true,
        (FinalOutcome::Failed, DedupPolicy::Success) => false,
    };

    if should_keep {
        let updated = IdempotencyEntry {
            key: key.to_string(),
            run_id: run_id.to_string(),
            state: match outcome {
                FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                FinalOutcome::Failed => IdempotencyState::Failed,
            },
            stamped_at: Utc::now(),
            expires_at: retention_until,
            dedup_on: config.dedup_on,
        };
        let bytes = serde_json::to_vec(&updated)?;
        provider.put(&rel_path, Bytes::from(bytes)).await?;
    } else {
        // Dedup-on-success + failure: delete the InFlight so retries can
        // claim the key freshly. Tolerate already-absent (e.g. the sweep
        // cleaned up while we were running).
        if let Err(e) = provider.delete(&rel_path).await {
            debug!(
                error = %e,
                path = %rel_path,
                "idempotency finalize delete failed — entry may have been swept already"
            );
        }
    }
    Ok(())
}

async fn object_store_sweep_expired(
    uri: &str,
    _config: &IdempotencyConfig,
) -> Result<usize, IdempotencyError> {
    // Active sweep for object-store backends: list `idempotency/*`, parse
    // each object, delete any past `expires_at`. Intentionally conservative
    // — errors are warn-swallowed so a flaky bucket doesn't fail the run.
    let provider = ObjectStoreProvider::from_uri(uri)?;
    let paths = match provider.list("idempotency").await {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "idempotency sweep list failed; bucket-lifecycle rules are the durable GC path");
            return Ok(0);
        }
    };
    let now = Utc::now();
    let mut removed = 0;
    for path in paths {
        let Some(rel) = path.strip_prefix("idempotency/").or(Some(&path)) else {
            continue;
        };
        // Read the object body so we can inspect `expires_at`. Skipping a
        // HEAD-based shortcut because the object-store crate's metadata API
        // doesn't surface custom timestamps.
        let rel_path = format!("idempotency/{}", rel.trim_start_matches("idempotency/"));
        let body = match provider.get(&rel_path).await {
            Ok(b) => b,
            Err(_) => continue,
        };
        let parsed: IdempotencyEntry = match serde_json::from_slice(&body) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if parsed.expires_at < now {
            if let Ok(()) = provider.delete(&rel_path).await {
                removed += 1;
            }
        }
    }
    Ok(removed)
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
    match prior.state {
        IdempotencyState::Succeeded => IdempotencyCheck::SkipCompleted {
            run_id: prior.run_id,
        },
        IdempotencyState::Failed => match new_entry.dedup_on {
            DedupPolicy::Any => IdempotencyCheck::SkipCompleted {
                run_id: prior.run_id,
            },
            DedupPolicy::Success => {
                // Stale failure — sweep semantics will remove it; caller
                // proceeds (AdoptStale surfaces the prior run_id for log
                // cross-reference).
                IdempotencyCheck::AdoptStale {
                    prior_run_id: prior.run_id,
                }
            }
        },
        IdempotencyState::InFlight => {
            if prior.expires_at > now {
                IdempotencyCheck::SkipInFlight {
                    run_id: prior.run_id,
                }
            } else {
                info!(
                    key = %prior.key,
                    prior_run_id = %prior.run_id,
                    "adopting stale InFlight idempotency entry past TTL"
                );
                IdempotencyCheck::AdoptStale {
                    prior_run_id: prior.run_id,
                }
            }
        }
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

/// Duration before an in-flight claim is treated as stale.
pub fn in_flight_stale_after(config: &IdempotencyConfig) -> Duration {
    Duration::from_secs(config.in_flight_ttl_hours as u64 * 3600)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{DedupPolicy, IdempotencyConfig};

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
    fn in_flight_stale_duration_matches_config() {
        let cfg = IdempotencyConfig {
            in_flight_ttl_hours: 6,
            ..Default::default()
        };
        assert_eq!(in_flight_stale_after(&cfg), Duration::from_secs(6 * 3600));
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
            valkey_url: Some("redis://localhost:6379".into()),
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
}
