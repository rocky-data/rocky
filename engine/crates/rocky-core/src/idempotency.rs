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
//! | [`StateBackend::Valkey`] / [`StateBackend::Tiered`] | Valkey `SET NX EX` on `{prefix}idempotency:<key>`; a stale claim is taken over, and finalize written, by Lua `EVAL` scripts that compare the stored value first. The server must allow `EVAL` ([`IdempotencyError::ScriptingUnavailable`]) |
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
//!
//! A conditional write can commit and still be reported as a conflict: the
//! store client retries a request whose reply was lost, and the retry finds
//! the object already changed (#2479). So when a re-read finds exactly the
//! bytes this call wrote, the write landed and this call won. The check
//! compares the whole entry, not only the `run_id`: a `run_id` has
//! millisecond resolution and can be set by the caller, so two runs can share
//! one, but the entry also carries this call's `stamped_at`.

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
    /// can cross-reference. `prior_state` is the state the replaced entry
    /// was in: only an `InFlight` one means a run may have died holding the
    /// claim. A `Failed` entry that a failed run left (the object-store
    /// finalize writes it already expired) is adopted too, and needs no
    /// warning.
    AdoptStale {
        prior_run_id: String,
        prior_state: IdempotencyState,
    },
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

    #[error(
        "the Valkey backend for idempotency needs server-side Lua scripting (`EVAL`), \
         and this server refused it: {detail}. Use a Valkey or Redis server that allows \
         `EVAL`, or switch the state backend to \"local\", \"s3\" or \"gcs\"."
    )]
    ScriptingUnavailable { detail: String },

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
                let wrote = valkey_finalize(url, prefix, key, run_id, outcome, config, now).await?;
                // Mirror only what the script wrote. When another run owns the
                // key, the local row belongs to that run.
                if wrote
                    && *mirror_to_redb
                    && let Some(store) = state_store
                {
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

/// Compare-and-set for the take-over of a stale claim. `KEYS[1]` is the claim
/// key; `ARGV[1]` is the exact raw value this run read, `ARGV[2]` the new
/// `InFlight` value and `ARGV[3]` its TTL in seconds. Returns 1 when the key
/// still holds exactly `ARGV[1]` and was replaced, and 0 otherwise.
const VALKEY_ADOPT_SCRIPT: &str = r#"
if redis.call('GET', KEYS[1]) == ARGV[1] then
  redis.call('SET', KEYS[1], ARGV[2], 'EX', ARGV[3])
  return 1
end
return 0
"#;

/// The idempotency claim and finalize run server-side Lua. Some managed
/// Valkey / Redis services restrict scripting, and a claim made there could
/// never be finalized, so it would stay `InFlight` for `in_flight_ttl_hours`.
const VALKEY_PROBE_SCRIPT: &str = "return 1";

/// Say whether a server reply means the server refuses scripting: no
/// permission (`NOPERM`), an unknown or renamed command, or scripting turned
/// off. Any other reply is not a refusal.
fn is_scripting_refusal(e: &redis::RedisError) -> bool {
    if e.is_io_error() || e.is_connection_dropped() || e.is_connection_refusal() {
        return false;
    }
    if e.code() == Some("NOPERM") {
        return true;
    }
    let text = e.to_string().to_ascii_lowercase();
    if text.contains("unknown command") || text.contains("noperm") {
        return true;
    }
    let names_scripting = text.contains("eval") || text.contains("script");
    names_scripting
        && ["disabled", "not allowed", "not supported", "unsupported"]
            .iter()
            .any(|w| text.contains(w))
}

/// Turn a failed script call into an error. Only a scripting refusal becomes
/// [`IdempotencyError::ScriptingUnavailable`]; every other failure stays an
/// ordinary Valkey error.
fn map_script_error(e: &redis::RedisError) -> IdempotencyError {
    if is_scripting_refusal(e) {
        IdempotencyError::ScriptingUnavailable {
            detail: e.to_string(),
        }
    } else {
        IdempotencyError::Valkey(e.to_string())
    }
}

/// Run `script` with plain `EVAL` on one key. Plain `EVAL` is the only form
/// used: the probe and the real scripts then need the same permission, which
/// `EVALSHA` plus `SCRIPT LOAD` would not guarantee.
fn valkey_eval(
    conn: &mut redis::Connection,
    script: &str,
    key: &str,
    args: &[&[u8]],
) -> Result<i64, redis::RedisError> {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(script).arg(1).arg(key);
    for a in args {
        cmd.arg(*a);
    }
    cmd.query::<i64>(conn)
}

/// Check, before any work, that the server runs `EVAL` on a key.
fn valkey_probe_eval(conn: &mut redis::Connection, key: &str) -> Result<(), IdempotencyError> {
    valkey_eval(conn, VALKEY_PROBE_SCRIPT, key, &[])
        .map(|_| ())
        .map_err(|e| map_script_error(&e))
}

/// Replace the value at `key` with `new_value` (TTL `ttl_secs`) only if the
/// key still holds exactly `expected`. `true` when this call took the key.
fn valkey_adopt_cas(
    conn: &mut redis::Connection,
    key: &str,
    expected: &[u8],
    new_value: &[u8],
    ttl_secs: u64,
) -> Result<bool, IdempotencyError> {
    let ttl = ttl_secs.to_string();
    let swapped = valkey_eval(
        conn,
        VALKEY_ADOPT_SCRIPT,
        key,
        &[expected, new_value, ttl.as_bytes()],
    )
    .map_err(|e| map_script_error(&e))?;
    Ok(swapped == 1)
}

/// Classify the raw value found at a Valkey claim key. A value that does not
/// parse as an entry, and names no owner either, is treated as stale, as on `s3` and `gcs`; the caller
/// takes it with the raw-bytes compare-and-set, so a bad value cannot block
/// every run until its TTL.
fn classify_valkey_raw(raw: &[u8], key: &str, new_entry: &IdempotencyEntry) -> IdempotencyCheck {
    match serde_json::from_slice::<IdempotencyEntry>(raw) {
        Ok(prior) => classify_live(prior, new_entry),
        Err(_) => {
            // An entry from a newer Rocky (an unknown state or policy) fails
            // the strict parse but still names its owner. Treat it as live:
            // the server TTL frees it. Taking it would steal a live claim.
            #[derive(Deserialize)]
            struct OwnerOnly {
                run_id: String,
            }
            if let Ok(owner) = serde_json::from_slice::<OwnerOnly>(raw) {
                return IdempotencyCheck::SkipInFlight {
                    run_id: owner.run_id,
                };
            }
            warn!(
                key = %key,
                "idempotency entry is not deserializable; treating it as stale"
            );
            IdempotencyCheck::AdoptStale {
                prior_run_id: "<corrupt>".to_string(),
                prior_state: IdempotencyState::InFlight,
            }
        }
    }
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
    let entry_for_task = entry.clone();

    let verdict: IdempotencyCheck =
        tokio::task::spawn_blocking(move || -> Result<IdempotencyCheck, IdempotencyError> {
            let client =
                redis::Client::open(url).map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
            let mut conn = client
                .get_connection()
                .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

            // Scripting is needed by finalize and by a take-over. Fail now,
            // before the run does any work.
            valkey_probe_eval(&mut conn, &key_for_task)?;

            // The verdict of the last take-over this call attempted.
            let mut attempted = IdempotencyCheck::Proceed;
            for _ in 0..MAX_CONDITIONAL_ATTEMPTS {
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
                    return Ok(IdempotencyCheck::Proceed);
                }

                // Key exists: keep the raw bytes, so the take-over below
                // compares against exactly what this run classified.
                let existing: Option<Vec<u8>> = redis::cmd("GET")
                    .arg(&key_for_task)
                    .query(&mut conn)
                    .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
                let Some(raw) = existing else {
                    // Expired between the failed SET NX and the GET.
                    continue;
                };
                if raw == value {
                    // The key holds exactly this call's claim, so an earlier
                    // write landed (#2479). This client does not resend a
                    // command today, so a lost reply is an error instead;
                    // this keeps a resend from skipping the run's own claim.
                    return Ok(attempted);
                }
                // The server TTL decides expiry: a key that still exists is
                // live, whatever this pod's clock says. A value that is not an
                // entry is stale, and the same compare-and-set takes it.
                let verdict = classify_valkey_raw(&raw, &key_for_task, &entry_for_task);
                match &verdict {
                    IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. } => {
                        // Take the key only if it still holds what we read.
                        // When two runs adopt at once, exactly one script
                        // swaps; the other re-reads and re-classifies.
                        attempted = verdict.clone();
                        if valkey_adopt_cas(&mut conn, &key_for_task, &raw, &value, ttl_secs)? {
                            return Ok(verdict);
                        }
                    }
                    IdempotencyCheck::SkipCompleted { .. }
                    | IdempotencyCheck::SkipInFlight { .. } => return Ok(verdict),
                }
            }
            Err(IdempotencyError::Contended {
                path: key_for_task,
                attempts: MAX_CONDITIONAL_ATTEMPTS,
            })
        })
        .await
        .map_err(|e| IdempotencyError::TaskJoin(e.to_string()))??;

    debug!(key = %full_key, verdict = ?verdict, "idempotency claim on valkey");
    Ok(verdict)
}

/// Compare-and-set for [`valkey_finalize`]. `KEYS[1]` is the claim key;
/// `ARGV` is our run id, "1" to keep (write `ARGV[3]` with `EX ARGV[4]`) or "0"
/// to delete, then the value and the TTL. Returns 1 when the key is absent or
/// holds our run id, and 0 when another run (or an unreadable value) holds it.
const VALKEY_FINALIZE_SCRIPT: &str = r#"
local cur = redis.call('GET', KEYS[1])
if cur then
  local ok, entry = pcall(cjson.decode, cur)
  if not ok or type(entry) ~= 'table' or entry.run_id ~= ARGV[1] then
    return 0
  end
end
if ARGV[2] == '1' then
  redis.call('SET', KEYS[1], ARGV[3], 'EX', ARGV[4])
else
  redis.call('DEL', KEYS[1])
end
return 1
"#;

/// Returns `true` when the script wrote (or deleted) the claim, and `false`
/// when another run owns the key and nothing was changed.
async fn valkey_finalize(
    url: &str,
    prefix: &str,
    key: &str,
    run_id: &str,
    outcome: FinalOutcome,
    config: &IdempotencyConfig,
    now: DateTime<Utc>,
) -> Result<bool, IdempotencyError> {
    if url.is_empty() {
        return Ok(false);
    }
    let retention_until = now + chrono::Duration::days(config.retention_days as i64);
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
        .signed_duration_since(now)
        .num_seconds()
        .max(1) as u64;

    tokio::task::spawn_blocking(move || -> Result<bool, IdempotencyError> {
        let client =
            redis::Client::open(url).map_err(|e| IdempotencyError::Valkey(e.to_string()))?;
        let mut conn = client
            .get_connection()
            .map_err(|e| IdempotencyError::Valkey(e.to_string()))?;

        let value = if should_keep {
            let entry = IdempotencyEntry {
                key: user_key,
                run_id: run_id.clone(),
                state: match outcome {
                    FinalOutcome::Succeeded => IdempotencyState::Succeeded,
                    FinalOutcome::Failed => IdempotencyState::Failed,
                },
                stamped_at: now,
                expires_at: retention_until,
                dedup_on,
            };
            serde_json::to_vec(&entry)?
        } else {
            Vec::new()
        };
        // One atomic script: write the terminal state (or, for dedup-on-success
        // + failure, delete the claim) only if the stored claim is ours. A
        // blind SET / DEL would erase the claim of a run that took the key
        // after ours expired.
        let ttl = ttl_secs.to_string();
        let owned = valkey_eval(
            &mut conn,
            VALKEY_FINALIZE_SCRIPT,
            &key_for_task,
            &[
                run_id.as_bytes(),
                if should_keep { b"1" } else { b"0" },
                &value,
                ttl.as_bytes(),
            ],
        )
        .map_err(|e| map_script_error(&e))?;
        if owned == 0 {
            warn!(
                key = %key_for_task,
                run_id = %run_id,
                "idempotency claim is held by another run; not finalizing it"
            );
        }
        Ok(owned != 0)
    })
    .await
    .map_err(|e| IdempotencyError::TaskJoin(e.to_string()))?
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
    // The verdict of the last write this call attempted. A take-over that
    // reported a conflict may still have landed; if it did, this is what the
    // call won.
    let mut attempted = IdempotencyCheck::Proceed;

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
        if observed.body == encoded {
            // The object holds exactly the claim this call wrote: an earlier
            // write landed but its reply was lost, and the store client's
            // retry saw a conflict (#2479). This call owns the key.
            debug!(path = %rel_path, "idempotency claim already holds this run's write");
            return Ok(attempted);
        }
        let verdict = match observed.entry.clone() {
            Some(prior) => classify_existing(prior, now, entry),
            None => {
                warn!(
                    path = %rel_path,
                    "idempotency entry is not deserializable; treating it as stale"
                );
                IdempotencyCheck::AdoptStale {
                    prior_run_id: "<corrupt>".to_string(),
                    prior_state: IdempotencyState::InFlight,
                }
            }
        };
        match &verdict {
            IdempotencyCheck::Proceed | IdempotencyCheck::AdoptStale { .. } => {
                // Take over only if the object is still the one we read. When
                // two runs adopt at once, exactly one write commits; the
                // other sees a conflict, re-reads the winner's fresh claim and
                // skips as in-flight. A conflict can also hide this call's
                // own landed write; the re-read above catches that.
                let generation = require_generation(&observed)?;
                attempted = verdict.clone();
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

    // The last entry this call tried to write. A write reported as a conflict
    // may still have landed (#2479).
    let mut attempted: Option<Bytes> = None;
    for _ in 0..MAX_CONDITIONAL_ATTEMPTS {
        let Some(observed) = observe_claim_at_path(provider, &rel_path).await? else {
            // The claim is gone (an operator or a bucket lifecycle rule
            // removed it). Record the outcome only if nobody has claimed the
            // key since. A freed key needs no record.
            if !should_keep {
                return Ok(());
            }
            let body = encode(retention_until)?;
            attempted = Some(body.clone());
            match provider.put_if_match(&rel_path, body, None).await? {
                PutIfMatchOutcome::Committed(_) => return Ok(()),
                PutIfMatchOutcome::Conflict => continue,
            }
        };
        if attempted.as_ref() == Some(&observed.body) {
            // This call's write landed; its reply was lost. Writing again
            // would back-date a `Failed` entry to the time of that write
            // instead of to the claim's own stamp.
            return Ok(());
        }
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
        let body = encode(expires_at)?;
        attempted = Some(body.clone());
        match provider
            .put_if_match(&rel_path, body, Some(generation))
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
/// Used by Local (inside the redb write txn — if Proceed / AdoptStale the
/// store then inserts the new entry) and the object-store backends. These
/// judge expiry by the clock. Valkey does not: the server TTL decides, so it
/// calls [`classify_live`] (through `classify_valkey_raw`) instead.
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
        match prior.state {
            IdempotencyState::InFlight => info!(
                key = %prior.key,
                prior_run_id = %prior.run_id,
                "adopting stale InFlight idempotency entry past TTL"
            ),
            IdempotencyState::Succeeded => warn!(
                key = %prior.key,
                prior_run_id = %prior.run_id,
                "adopting an expired Succeeded idempotency entry; the work it recorded will run again"
            ),
            IdempotencyState::Failed => {}
        }
        return IdempotencyCheck::AdoptStale {
            prior_run_id: prior.run_id,
            prior_state: prior.state,
        };
    }
    classify_live(prior, new_entry)
}

/// Decide the outcome for an entry that is live, with no clock check. The
/// Valkey backend uses this directly: the server drops a key at its TTL, so a
/// key that still exists is live and this pod's clock must not overrule it.
pub(crate) fn classify_live(
    prior: IdempotencyEntry,
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
            // A live failure under `dedup_on = "success"` does not block a
            // retry; AdoptStale surfaces the prior run_id for log
            // cross-reference.
            DedupPolicy::Success => IdempotencyCheck::AdoptStale {
                prior_run_id: prior.run_id,
                prior_state: IdempotencyState::Failed,
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

    /// An adopted entry reports the state it was in, so a caller can tell a
    /// run that died holding the claim (`InFlight`) from a run that ended
    /// cleanly (`Failed`, which the object-store finalize writes already
    /// expired). Mutation: report `InFlight` for every adoption.
    #[test]
    fn adopting_reports_the_state_of_the_prior_entry() {
        let now = Utc::now();
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        for (state, ttl, age) in [
            (IdempotencyState::InFlight, 1, 5),
            (IdempotencyState::Failed, 1, 5),
            (IdempotencyState::Failed, 24, 1),
        ] {
            let prior = mk_entry("run-prior", state, now - chrono::Duration::hours(age), ttl);
            match classify_existing(prior, now, &new) {
                IdempotencyCheck::AdoptStale { prior_state, .. } => {
                    assert_eq!(prior_state, state);
                }
                other => panic!("expected AdoptStale, got {other:?}"),
            }
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
            IdempotencyCheck::AdoptStale { prior_run_id, .. } => {
                assert_eq!(prior_run_id, "run-prior")
            }
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
            IdempotencyCheck::AdoptStale { prior_run_id, .. } => {
                assert_eq!(prior_run_id, "run-prior")
            }
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }

    #[test]
    fn is_skip_distinguishes_proceed() {
        assert!(!IdempotencyCheck::Proceed.is_skip());
        assert!(
            !IdempotencyCheck::AdoptStale {
                prior_run_id: "x".into(),
                prior_state: IdempotencyState::InFlight,
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
    // Live Valkey tests for the finalize script. Gated on
    // `ROCKY_TEST_VALKEY_URL` (same convention as rocky-fivetran); the
    // default CI run skips them.
    // -------------------------------------------------------------------

    fn live_valkey_url() -> Option<String> {
        match std::env::var("ROCKY_TEST_VALKEY_URL") {
            Ok(u) => Some(u),
            Err(_) => {
                eprintln!("skipping live Valkey test; set ROCKY_TEST_VALKEY_URL to enable");
                None
            }
        }
    }

    fn live_conn(url: &str) -> redis::Connection {
        redis::Client::open(url).unwrap().get_connection().unwrap()
    }

    fn live_backend(url: &str, prefix: &str) -> IdempotencyBackend {
        IdempotencyBackend::Valkey {
            url: url.to_string(),
            prefix: prefix.to_string(),
            mirror_to_redb: false,
        }
    }

    fn live_prefix(tag: &str) -> String {
        format!("rocky-test-{tag}-{}:", uuid::Uuid::new_v4())
    }

    fn live_cfg(dedup_on: DedupPolicy) -> IdempotencyConfig {
        IdempotencyConfig {
            retention_days: 2,
            dedup_on,
            ..IdempotencyConfig::default()
        }
    }

    fn put_raw(conn: &mut redis::Connection, key: &str, value: &[u8]) {
        let _: () = redis::cmd("SET")
            .arg(key)
            .arg(value)
            .arg("EX")
            .arg(1000)
            .query(conn)
            .unwrap();
    }

    fn get_raw(conn: &mut redis::Connection, key: &str) -> Option<Vec<u8>> {
        redis::cmd("GET").arg(key).query(conn).unwrap()
    }

    fn del_raw(conn: &mut redis::Connection, key: &str) {
        let _: i64 = redis::cmd("DEL").arg(key).query(conn).unwrap();
    }

    fn entry_json(run_id: &str, state: IdempotencyState) -> Vec<u8> {
        let now = Utc::now();
        serde_json::to_vec(&IdempotencyEntry {
            key: "k".into(),
            run_id: run_id.into(),
            state,
            stamped_at: now,
            expires_at: now + chrono::Duration::hours(1),
            dedup_on: DedupPolicy::Success,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn live_valkey_finalize_owner_succeeded_writes_entry_with_ttl() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("own-ok");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(
            &mut conn,
            &full,
            &entry_json("run-a", IdempotencyState::InFlight),
        );

        let cfg = live_cfg(DedupPolicy::Success);
        live_backend(&url, &prefix)
            .finalize(None, "k", "run-a", FinalOutcome::Succeeded, &cfg)
            .await
            .unwrap();

        let raw = get_raw(&mut conn, &full).expect("entry kept");
        let entry: IdempotencyEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(entry.state, IdempotencyState::Succeeded);
        assert_eq!(entry.run_id, "run-a");
        let ttl: i64 = redis::cmd("TTL").arg(&full).query(&mut conn).unwrap();
        let two_days = 2 * 86_400;
        assert!((two_days - 60..=two_days).contains(&ttl), "ttl {ttl}");
        del_raw(&mut conn, &full);
    }

    #[tokio::test]
    async fn live_valkey_finalize_owner_failed_dedup_success_deletes_key() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("own-fail");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(
            &mut conn,
            &full,
            &entry_json("run-a", IdempotencyState::InFlight),
        );

        let cfg = live_cfg(DedupPolicy::Success);
        live_backend(&url, &prefix)
            .finalize(None, "k", "run-a", FinalOutcome::Failed, &cfg)
            .await
            .unwrap();

        assert!(get_raw(&mut conn, &full).is_none(), "claim must be deleted");
    }

    #[tokio::test]
    async fn live_valkey_finalize_owner_failed_dedup_any_keeps_failed_entry() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("own-fail-any");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(
            &mut conn,
            &full,
            &entry_json("run-a", IdempotencyState::InFlight),
        );

        let cfg = live_cfg(DedupPolicy::Any);
        live_backend(&url, &prefix)
            .finalize(None, "k", "run-a", FinalOutcome::Failed, &cfg)
            .await
            .unwrap();

        let raw = get_raw(&mut conn, &full).expect("entry kept");
        let entry: IdempotencyEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(entry.state, IdempotencyState::Failed);
        del_raw(&mut conn, &full);
    }

    #[tokio::test]
    async fn live_valkey_finalize_non_owner_leaves_key_untouched() {
        let Some(url) = live_valkey_url() else { return };
        let cfg = live_cfg(DedupPolicy::Success);
        for outcome in [FinalOutcome::Succeeded, FinalOutcome::Failed] {
            let prefix = live_prefix("non-owner");
            let full = valkey_key(&prefix, "k");
            let mut conn = live_conn(&url);
            let theirs = entry_json("run-b", IdempotencyState::InFlight);
            put_raw(&mut conn, &full, &theirs);

            live_backend(&url, &prefix)
                .finalize(None, "k", "run-a", outcome, &cfg)
                .await
                .unwrap();

            assert_eq!(get_raw(&mut conn, &full), Some(theirs), "{outcome:?}");
            del_raw(&mut conn, &full);
        }
    }

    #[tokio::test]
    async fn live_valkey_finalize_absent_key_is_written() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("absent");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);

        let cfg = live_cfg(DedupPolicy::Success);
        live_backend(&url, &prefix)
            .finalize(None, "k", "run-a", FinalOutcome::Succeeded, &cfg)
            .await
            .unwrap();

        let raw = get_raw(&mut conn, &full).expect("entry written");
        let entry: IdempotencyEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(entry.state, IdempotencyState::Succeeded);
        assert_eq!(entry.run_id, "run-a");
        del_raw(&mut conn, &full);
    }

    #[tokio::test]
    async fn live_valkey_finalize_unreadable_value_is_untouched() {
        let Some(url) = live_valkey_url() else { return };
        let cfg = live_cfg(DedupPolicy::Success);
        let junk_values: [&[u8]; 4] = [b"not json {", b"[1,2]", b"\"str\"", b"{\"no_run_id\":1}"];
        for junk in junk_values {
            let prefix = live_prefix("junk");
            let full = valkey_key(&prefix, "k");
            let mut conn = live_conn(&url);
            put_raw(&mut conn, &full, junk);

            live_backend(&url, &prefix)
                .finalize(None, "k", "run-a", FinalOutcome::Succeeded, &cfg)
                .await
                .unwrap();

            assert_eq!(get_raw(&mut conn, &full), Some(junk.to_vec()));
            del_raw(&mut conn, &full);
        }
    }

    // -------------------------------------------------------------------
    // Expiry rule and probe mapping (no server needed)
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
            IdempotencyCheck::AdoptStale { prior_run_id, .. } => {
                assert_eq!(prior_run_id, "run-prior")
            }
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
            IdempotencyCheck::AdoptStale { prior_run_id, .. } => {
                assert_eq!(prior_run_id, "run-prior")
            }
            other => panic!("expected AdoptStale, got {other:?}"),
        }
    }

    fn server_error(text: &'static str) -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            text,
        ))
    }

    #[test]
    fn only_a_scripting_refusal_maps_to_scripting_unavailable() {
        for refused in [
            "unknown command 'EVAL'",
            "NOPERM User app has no permissions to run the 'eval' command",
            "ERR EVAL is disabled",
            "ERR scripting is not allowed on this instance",
        ] {
            match map_script_error(&server_error(refused)) {
                IdempotencyError::ScriptingUnavailable { detail } => {
                    assert!(!detail.is_empty());
                }
                other => panic!("`{refused}`: expected ScriptingUnavailable, got {other:?}"),
            }
        }
        let text = IdempotencyError::ScriptingUnavailable { detail: "x".into() }.to_string();
        assert!(text.contains("server-side Lua scripting"), "{text}");
    }

    #[test]
    fn other_script_errors_stay_valkey_errors() {
        for other in [
            "WRONGTYPE Operation against a key holding the wrong kind of value",
            "OOM command not allowed when used memory > 'maxmemory'.",
            "BUSY Redis is busy running a script. You can only call SCRIPT KILL",
            "READONLY You can't write against a read only replica.",
            "ERR Error running script: user_script:1: Script attempted to access a non existent key",
        ] {
            assert!(
                matches!(
                    map_script_error(&server_error(other)),
                    IdempotencyError::Valkey(_)
                ),
                "`{other}` must stay a Valkey error"
            );
        }
    }

    #[test]
    fn a_transport_failure_is_a_valkey_error() {
        let io = redis::RedisError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        ));
        assert!(matches!(map_script_error(&io), IdempotencyError::Valkey(_)));
    }

    #[test]
    fn the_scripts_compare_before_they_write() {
        // The take-over writes only when the key still holds the value read.
        assert!(VALKEY_ADOPT_SCRIPT.contains("GET', KEYS[1]) == ARGV[1]"));
        assert!(VALKEY_ADOPT_SCRIPT.contains("'SET'"));
        // Finalize writes or deletes only for the owner (or an absent key).
        assert!(VALKEY_FINALIZE_SCRIPT.contains("entry.run_id ~= ARGV[1]"));
        assert!(VALKEY_FINALIZE_SCRIPT.contains("return 0"));
        // Every script is a one-key script, so it can go as plain EVAL.
        for script in [
            VALKEY_PROBE_SCRIPT,
            VALKEY_ADOPT_SCRIPT,
            VALKEY_FINALIZE_SCRIPT,
        ] {
            assert!(!script.contains("redis.call('EVAL"));
        }
    }

    #[test]
    fn an_unreadable_valkey_value_is_stale_not_an_error() {
        let now = Utc::now();
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        for junk in [
            &b"not json {"[..],
            b"[1,2]",
            b"\"str\"",
            b"{\"no_run_id\":1}",
            b"",
        ] {
            assert!(
                matches!(
                    classify_valkey_raw(junk, "k", &new),
                    IdempotencyCheck::AdoptStale { .. }
                ),
                "{junk:?}"
            );
        }
        let ok =
            serde_json::to_vec(&mk_entry("run-a", IdempotencyState::InFlight, now, 24)).unwrap();
        assert!(matches!(
            classify_valkey_raw(&ok, "k", &new),
            IdempotencyCheck::SkipInFlight { .. }
        ));
    }

    #[test]
    fn a_newer_rockys_entry_is_live_not_junk() {
        let now = Utc::now();
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        let future = br#"{"key":"k","run_id":"run-x","state":"Paused","dedup_on":"Never"}"#;
        match classify_valkey_raw(future, "k", &new) {
            IdempotencyCheck::SkipInFlight { run_id } => assert_eq!(run_id, "run-x"),
            other => panic!("expected SkipInFlight, got {other:?}"),
        }
        // A run_id that is not a string is junk.
        assert!(matches!(
            classify_valkey_raw(br#"{"run_id":5}"#, "k", &new),
            IdempotencyCheck::AdoptStale { .. }
        ));
    }

    #[test]
    fn a_key_that_still_exists_is_live_whatever_the_clock_says() {
        let now = Utc::now();
        let new = mk_entry("run-new", IdempotencyState::InFlight, now, 24);
        // Stamped 30 hours ago, "expired" 6 hours ago by this clock.
        let in_flight = mk_entry(
            "run-prior",
            IdempotencyState::InFlight,
            now - chrono::Duration::hours(30),
            24,
        );
        assert!(matches!(
            classify_live(in_flight, &new),
            IdempotencyCheck::SkipInFlight { .. }
        ));
        let done = mk_entry(
            "run-prior",
            IdempotencyState::Succeeded,
            now - chrono::Duration::hours(30),
            24,
        );
        assert!(matches!(
            classify_live(done, &new),
            IdempotencyCheck::SkipCompleted { .. }
        ));
    }

    // -------------------------------------------------------------------
    // Live Valkey tests for take-over, the EVAL probe and the tiered mirror
    // -------------------------------------------------------------------

    /// An entry stamped 30 hours ago whose own expiry is 6 hours past. Valkey
    /// drops a key at its server TTL, so a key like this one only exists when
    /// the test puts it there. The server, not the entry, decides it is live.
    fn expired_entry_json(run_id: &str, state: IdempotencyState) -> Vec<u8> {
        let now = Utc::now();
        serde_json::to_vec(&IdempotencyEntry {
            key: "k".into(),
            run_id: run_id.into(),
            state,
            stamped_at: now - chrono::Duration::hours(30),
            expires_at: now - chrono::Duration::hours(6),
            dedup_on: DedupPolicy::Success,
        })
        .unwrap()
    }

    /// The compare-and-set takes the key once. A second take-over that read the
    /// same old value must lose, because the key no longer holds it. This is
    /// the deterministic form of the two-adopters race.
    #[tokio::test]
    async fn live_valkey_adopt_cas_lets_only_one_taker_of_the_same_read_win() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("cas");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        let old = expired_entry_json("run-old", IdempotencyState::InFlight);
        put_raw(&mut conn, &full, &old);

        let first = valkey_adopt_cas(&mut conn, &full, &old, b"first", 100).unwrap();
        let second = valkey_adopt_cas(&mut conn, &full, &old, b"second", 100).unwrap();
        assert!(first, "the first taker must win");
        assert!(
            !second,
            "the second taker read the same value and must lose"
        );
        assert_eq!(get_raw(&mut conn, &full), Some(b"first".to_vec()));
        del_raw(&mut conn, &full);
    }

    /// Two runs take over one stale claim at once: exactly one proceeds, the
    /// other skips as in-flight, and the winner's finalize records `Succeeded`.
    /// The stale claim is a `Failed` entry under `dedup_on = "success"`, the
    /// one case where Valkey hands out a take-over. If the take-over were a
    /// plain `SET`, both runs would be told `AdoptStale` and this test fails.
    /// Repeated, because one lucky interleaving would hide a plain `SET`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn live_valkey_two_takers_of_one_stale_claim_have_one_winner() {
        let Some(url) = live_valkey_url() else { return };
        for round in 0..20 {
            live_two_takers_round(&url, round).await;
        }
    }

    async fn live_two_takers_round(url: &str, round: usize) {
        let prefix = live_prefix(&format!("two-takers-{round}"));
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(url);
        put_raw(
            &mut conn,
            &full,
            &expired_entry_json("run-old", IdempotencyState::Failed),
        );
        let cfg = live_cfg(DedupPolicy::Success);
        let backend = live_backend(url, &prefix);

        let (a, b) = tokio::join!(
            backend.check_and_claim(None, "k", "run-a", &cfg),
            backend.check_and_claim(None, "k", "run-b", &cfg),
        );
        let (a, b) = (a.unwrap(), b.unwrap());
        let (winner, loser_verdict) = match (&a, &b) {
            (IdempotencyCheck::AdoptStale { .. }, IdempotencyCheck::SkipInFlight { .. }) => {
                ("run-a", &b)
            }
            (IdempotencyCheck::SkipInFlight { .. }, IdempotencyCheck::AdoptStale { .. }) => {
                ("run-b", &a)
            }
            other => panic!("expected one adopter and one skip, got {other:?}"),
        };
        assert!(
            matches!(loser_verdict, IdempotencyCheck::SkipInFlight { run_id } if run_id == winner),
            "{loser_verdict:?}"
        );

        backend
            .finalize(None, "k", winner, FinalOutcome::Succeeded, &cfg)
            .await
            .unwrap();
        let raw = get_raw(&mut conn, &full).expect("entry kept");
        let entry: IdempotencyEntry = serde_json::from_slice(&raw).unwrap();
        assert_eq!(entry.state, IdempotencyState::Succeeded);
        assert_eq!(entry.run_id, winner);
        del_raw(&mut conn, &full);
    }

    /// A key that still exists on the server is live, even when the entry's own
    /// expiry is in the past by this pod's clock (clock skew).
    #[tokio::test]
    async fn live_valkey_a_key_past_its_stamp_but_still_on_the_server_is_live() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("skew");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        let old = expired_entry_json("run-old", IdempotencyState::InFlight);
        put_raw(&mut conn, &full, &old);

        let verdict = live_backend(&url, &prefix)
            .check_and_claim(None, "k", "run-new", &live_cfg(DedupPolicy::Success))
            .await
            .unwrap();
        assert!(
            matches!(verdict, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-old"),
            "{verdict:?}"
        );
        assert_eq!(get_raw(&mut conn, &full), Some(old));
        del_raw(&mut conn, &full);
    }

    /// A stored value that is not an entry does not block runs: the next run
    /// takes it with the compare-and-set and its own finalize then lands.
    #[tokio::test]
    async fn live_valkey_an_unreadable_claim_is_taken_over() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("junk-claim");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(&mut conn, &full, b"not json {");
        let cfg = live_cfg(DedupPolicy::Success);
        let backend = live_backend(&url, &prefix);

        let verdict = backend
            .check_and_claim(None, "k", "run-new", &cfg)
            .await
            .unwrap();
        assert!(
            matches!(verdict, IdempotencyCheck::AdoptStale { .. }),
            "{verdict:?}"
        );
        backend
            .finalize(None, "k", "run-new", FinalOutcome::Succeeded, &cfg)
            .await
            .unwrap();
        let held: IdempotencyEntry =
            serde_json::from_slice(&get_raw(&mut conn, &full).unwrap()).unwrap();
        assert_eq!(held.run_id, "run-new");
        assert_eq!(held.state, IdempotencyState::Succeeded);
        del_raw(&mut conn, &full);
    }

    /// An entry with an unknown state names its owner, so it is left alone.
    #[tokio::test]
    async fn live_valkey_an_entry_from_a_newer_rocky_is_left_alone() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("newer");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        let theirs = br#"{"key":"k","run_id":"run-x","state":"Paused"}"#;
        put_raw(&mut conn, &full, theirs);

        let verdict = live_backend(&url, &prefix)
            .check_and_claim(None, "k", "run-new", &live_cfg(DedupPolicy::Success))
            .await
            .unwrap();
        assert!(
            matches!(verdict, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-x"),
            "{verdict:?}"
        );
        assert_eq!(get_raw(&mut conn, &full), Some(theirs.to_vec()));
        del_raw(&mut conn, &full);
    }

    /// An adopter takes the key, so its own finalize is not a no-op.
    #[tokio::test]
    async fn live_valkey_adopt_writes_the_claim_and_the_adopters_finalize_lands() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("adopt-writes");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(
            &mut conn,
            &full,
            &expired_entry_json("run-old", IdempotencyState::Failed),
        );
        let cfg = live_cfg(DedupPolicy::Success);
        let backend = live_backend(&url, &prefix);

        let verdict = backend
            .check_and_claim(None, "k", "run-new", &cfg)
            .await
            .unwrap();
        assert!(
            matches!(verdict, IdempotencyCheck::AdoptStale { ref prior_run_id, .. } if prior_run_id == "run-old"),
            "{verdict:?}"
        );
        let held: IdempotencyEntry =
            serde_json::from_slice(&get_raw(&mut conn, &full).unwrap()).unwrap();
        assert_eq!(held.run_id, "run-new");
        assert_eq!(held.state, IdempotencyState::InFlight);

        backend
            .finalize(None, "k", "run-new", FinalOutcome::Succeeded, &cfg)
            .await
            .unwrap();
        let held: IdempotencyEntry =
            serde_json::from_slice(&get_raw(&mut conn, &full).unwrap()).unwrap();
        assert_eq!(held.run_id, "run-new");
        assert_eq!(held.state, IdempotencyState::Succeeded);
        del_raw(&mut conn, &full);
    }

    /// A server that refuses `EVAL` fails the claim before any work. The
    /// refusal is real: an ACL user without `eval` / `evalsha`.
    #[tokio::test]
    async fn live_valkey_claim_fails_when_the_server_refuses_eval() {
        let Some(url) = live_valkey_url() else { return };
        let user = format!("noscript{}", uuid::Uuid::new_v4().simple());
        let mut admin = live_conn(&url);
        let _: String = redis::cmd("ACL")
            .arg("SETUSER")
            .arg(&user)
            .arg("on")
            .arg(">pw")
            .arg("~*")
            .arg("+@all")
            .arg("-eval")
            .arg("-evalsha")
            .query(&mut admin)
            .unwrap();
        let restricted = url.replacen("redis://", &format!("redis://{user}:pw@"), 1);
        let prefix = live_prefix("no-eval");

        let result = live_backend(&restricted, &prefix)
            .check_and_claim(None, "k", "run-a", &live_cfg(DedupPolicy::Success))
            .await;
        let _: i64 = redis::cmd("ACL")
            .arg("DELUSER")
            .arg(&user)
            .query(&mut admin)
            .unwrap();
        match result {
            Err(IdempotencyError::ScriptingUnavailable { .. }) => {}
            other => panic!("expected ScriptingUnavailable, got {other:?}"),
        }
        assert!(
            get_raw(&mut admin, &valkey_key(&prefix, "k")).is_none(),
            "no claim may be written when scripting is refused"
        );
    }

    /// Tiered: when another run owns the key, the finalize script writes
    /// nothing, so the local mirror row of the new owner stays as it is.
    #[tokio::test]
    async fn live_tiered_finalize_does_not_overwrite_the_new_owners_mirror() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("tiered-mirror");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        put_raw(
            &mut conn,
            &full,
            &entry_json("run-b", IdempotencyState::InFlight),
        );
        let dir = tempfile::tempdir().unwrap();
        let store = StateStore::open(&dir.path().join("state.redb")).unwrap();
        let theirs = IdempotencyEntry {
            key: "k".into(),
            run_id: "run-b".into(),
            state: IdempotencyState::InFlight,
            stamped_at: Utc::now(),
            expires_at: Utc::now() + chrono::Duration::hours(1),
            dedup_on: DedupPolicy::Success,
        };
        store.idempotency_put(&theirs).unwrap();

        let backend = IdempotencyBackend::Valkey {
            url: url.clone(),
            prefix,
            mirror_to_redb: true,
        };
        backend
            .finalize(
                Some(&store),
                "k",
                "run-a",
                FinalOutcome::Succeeded,
                &live_cfg(DedupPolicy::Success),
            )
            .await
            .unwrap();

        assert_eq!(store.idempotency_get("k").unwrap(), Some(theirs));
        del_raw(&mut conn, &full);
    }

    // -------------------------------------------------------------------
    // #2479: a conditional write that lands but is reported as a conflict.
    // The store client re-sends a request whose reply was lost, and the
    // re-send finds the object already written. `land_then_conflict_nth_put`
    // compresses that into what this code sees.
    // -------------------------------------------------------------------

    fn faulting_provider() -> (ObjectStoreProvider, crate::fault_store::FaultHandle) {
        let (store, faults) = crate::fault_store::FaultingStore::wrap(std::sync::Arc::new(
            object_store::memory::InMemory::new(),
        ));
        (
            ObjectStoreProvider::from_store(store, "s3", "bucket", ""),
            faults,
        )
    }

    fn lost_reply_cfg() -> IdempotencyConfig {
        IdempotencyConfig {
            in_flight_ttl_hours: 24,
            dedup_on: DedupPolicy::Success,
            ..IdempotencyConfig::default()
        }
    }

    fn t0() -> DateTime<Utc> {
        use chrono::TimeZone;
        Utc.with_ymd_and_hms(2026, 10, 21, 8, 0, 0).unwrap()
    }

    async fn claim_on(
        provider: &ObjectStoreProvider,
        run_id: &str,
        now: DateTime<Utc>,
    ) -> IdempotencyCheck {
        let entry = mk_entry(run_id, IdempotencyState::InFlight, now, 24);
        object_store_check_and_claim(provider, &entry, now)
            .await
            .unwrap()
    }

    async fn seed(provider: &ObjectStoreProvider, entry: &IdempotencyEntry) {
        provider
            .put(
                &object_store_relative_path(&entry.key),
                Bytes::from(serde_json::to_vec(entry).unwrap()),
            )
            .await
            .unwrap();
    }

    async fn stored(provider: &ObjectStoreProvider) -> IdempotencyEntry {
        let body = provider
            .get(&object_store_relative_path("k"))
            .await
            .unwrap();
        serde_json::from_slice(&body).unwrap()
    }

    /// A first claim that lands but is reported as `AlreadyExists` is this
    /// run's claim: the run proceeds, it does not skip itself.
    #[tokio::test]
    async fn a_first_claim_whose_reply_is_lost_still_proceeds() {
        let (provider, faults) = faulting_provider();
        faults.land_then_conflict_nth_put(1);

        let verdict = claim_on(&provider, "run-a", t0()).await;
        assert!(
            matches!(verdict, IdempotencyCheck::Proceed),
            "the run skipped its own claim: {verdict:?}"
        );
        assert_eq!(stored(&provider).await.run_id, "run-a");
    }

    /// A take-over of a stale claim that lands but is reported as a conflict
    /// is a win, and still names the claim it replaced.
    #[tokio::test]
    async fn a_take_over_whose_reply_is_lost_still_adopts() {
        let (provider, faults) = faulting_provider();
        // Put #1 seeds an expired claim; #2 is the failed create; #3 is the
        // take-over, which lands and reports a conflict.
        seed(
            &provider,
            &mk_entry("run-old", IdempotencyState::InFlight, t0(), 1),
        )
        .await;
        faults.land_then_conflict_nth_put(3);

        let now = t0() + chrono::Duration::hours(2);
        let verdict = claim_on(&provider, "run-new", now).await;
        assert!(
            matches!(
                verdict,
                IdempotencyCheck::AdoptStale { ref prior_run_id, .. } if prior_run_id == "run-old"
            ),
            "the run skipped its own take-over: {verdict:?}"
        );
        assert_eq!(stored(&provider).await.run_id, "run-new");
    }

    /// Bound: another run's live claim still makes this run skip.
    #[tokio::test]
    async fn another_runs_live_claim_still_skips() {
        let (provider, _faults) = faulting_provider();
        seed(
            &provider,
            &mk_entry("run-b", IdempotencyState::InFlight, t0(), 24),
        )
        .await;

        let verdict = claim_on(&provider, "run-a", t0()).await;
        assert!(
            matches!(verdict, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-b"),
            "{verdict:?}"
        );
    }

    /// Bound: a live claim with this run's `run_id` but another stamp is not
    /// this call's write. A `run_id` can repeat (millisecond resolution, or a
    /// caller-chosen id), so the run skips rather than share the key.
    #[tokio::test]
    async fn a_live_claim_with_the_same_run_id_but_another_stamp_still_skips() {
        let (provider, _faults) = faulting_provider();
        seed(
            &provider,
            &mk_entry("run-a", IdempotencyState::InFlight, t0(), 24),
        )
        .await;

        let later = t0() + chrono::Duration::seconds(1);
        let verdict = claim_on(&provider, "run-a", later).await;
        assert!(
            matches!(verdict, IdempotencyCheck::SkipInFlight { ref run_id } if run_id == "run-a"),
            "{verdict:?}"
        );
    }

    /// A `Failed` finalize under `dedup_on = "success"` whose reply is lost
    /// keeps the back-date to the claim's own stamp. Writing it a second
    /// time would back-date it only to the first finalize's time.
    #[tokio::test]
    async fn a_finalize_whose_reply_is_lost_is_not_written_twice() {
        let (provider, faults) = faulting_provider();
        let cfg = lost_reply_cfg();
        assert!(matches!(
            claim_on(&provider, "run-a", t0()).await,
            IdempotencyCheck::Proceed
        ));
        // Put #1 was the claim; #2 is the finalize, which lands and reports a
        // conflict.
        faults.land_then_conflict_nth_put(2);

        let now = t0() + chrono::Duration::hours(1);
        object_store_finalize(
            &provider,
            "k",
            "run-a",
            FinalOutcome::Failed,
            now + chrono::Duration::days(2),
            &cfg,
            now,
        )
        .await
        .unwrap();
        let entry = stored(&provider).await;
        assert_eq!(entry.state, IdempotencyState::Failed);
        assert_eq!(entry.expires_at, t0(), "back-dated to the wrong time");
    }

    /// Valkey: a key that already holds exactly this call's claim is this
    /// run's. The client does not resend today, so this seeds the state a
    /// resend would find.
    #[tokio::test]
    async fn live_valkey_a_key_holding_this_calls_claim_proceeds() {
        let Some(url) = live_valkey_url() else { return };
        let prefix = live_prefix("own-claim");
        let full = valkey_key(&prefix, "k");
        let mut conn = live_conn(&url);
        let cfg = lost_reply_cfg();
        let now = Utc::now();
        let mine = mk_entry("run-a", IdempotencyState::InFlight, now, 24);
        put_raw(&mut conn, &full, &serde_json::to_vec(&mine).unwrap());
        let backend = live_backend(&url, &prefix);

        let verdict = backend
            .check_and_claim_at(None, "k", "run-a", &cfg, now)
            .await
            .unwrap();
        assert!(matches!(verdict, IdempotencyCheck::Proceed), "{verdict:?}");

        // Bound: the same run_id with another stamp is not this call's claim.
        let later = now + chrono::Duration::seconds(1);
        let verdict = backend
            .check_and_claim_at(None, "k", "run-a", &cfg, later)
            .await
            .unwrap();
        assert!(
            matches!(verdict, IdempotencyCheck::SkipInFlight { .. }),
            "{verdict:?}"
        );
        del_raw(&mut conn, &full);
    }
}
