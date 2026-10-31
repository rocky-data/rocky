//! `rocky load --connector <name>`: run a child connector and load what it
//! writes.
//!
//! One run, in order:
//!
//! ```text
//!  read saved state ──► claim the key ──► run the child ──► check its files
//!   (commit_seq N)      connector:x:N      (retries)         (verify)
//!                                                              │
//!   finalize the claim ◄── commit state ◄── promote ◄── drift ◄── stage
//!                          (seq N+1)        (INSERT)
//! ```
//!
//! Delivery is at least once. The promote and the state commit are two
//! writes. A crash between them loads the batch twice on the next run.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Utc};
use tracing::{info, warn};

use rocky_adapter_sdk::{FileFormat, LoadOptions, LoadSource, LoaderAdapter, TableRef};
use rocky_core::config::{DedupPolicy, IdempotencyConfig, LoadedConfig};
use rocky_core::connector::{
    ConnectorConfig, ConnectorLimitsConfig, ConnectorManifest, ConnectorStateRecord,
    EffectiveLimits, StateEnvelope, local_output_dir,
};
use rocky_core::idempotency::{FinalOutcome, IdempotencyBackend, IdempotencyCheck};
use rocky_core::state::{LoadedFileRecord, StateStore};
use rocky_core::state_sync::LedgerSeamSession;
use rocky_core::traits::{SqlDialect, WarehouseAdapter};

use crate::connector::runner::{ChildSpec, DEFAULT_OUTPUT_POLL, run_with_retries, was_interrupted};
use crate::connector::verify::VerifiedBatch;
use crate::output::{
    ConnectorLoadInfo, ConnectorLoadStatus, LoadFileOutput, LoadOutput, print_json,
};
use crate::registry::AdapterRegistry;

use super::load::{ContractGateError, build_loader, to_ir_table_ref, validate_landed_contract};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Hour headroom on top of the child's own time limits for the claim: the
/// staging load, the drift step, the promote and the state commit.
const CLAIM_HEADROOM_SECS: u64 = 3600;

/// Suffix of a connector's staging table; the run id follows.
const STAGING_INFIX: &str = "__rocky_stg_";

/// What a run needs, and the seams a test can move.
#[derive(Debug, Clone)]
pub struct ConnectorLoadParams<'a> {
    pub config_path: &'a Path,
    pub loaded: &'a LoadedConfig,
    pub state_path: &'a Path,
    pub name: &'a str,
    /// The clock for the idempotency claim. `None` is the real time.
    pub now: Option<DateTime<Utc>>,
    /// Gap between two checks of the bytes the child has written.
    pub output_poll: Duration,
    /// The time the promote deadline is checked against. `None` is the real
    /// time. A test moves it past the claim's expiry.
    pub promote_now: Option<DateTime<Utc>>,
    /// Test seam: act as if a signal arrived just before the promote.
    #[doc(hidden)]
    pub interrupt_before_promote: bool,
}

impl<'a> ConnectorLoadParams<'a> {
    /// Parameters for a normal run.
    pub fn new(
        config_path: &'a Path,
        loaded: &'a LoadedConfig,
        state_path: &'a Path,
        name: &'a str,
    ) -> Self {
        Self {
            config_path,
            loaded,
            state_path,
            name,
            now: None,
            output_poll: DEFAULT_OUTPUT_POLL,
            promote_now: None,
            interrupt_before_promote: false,
        }
    }
}

/// What a run produced: the document to print, and whether the run failed.
#[derive(Debug)]
pub struct ConnectorLoadOutcome {
    /// `None` when the run stopped before it claimed its key (a bad config,
    /// a missing secret, a downgrade). Nothing ran, so there is nothing to
    /// report.
    pub output: Option<LoadOutput>,
    /// The failure, if the run failed.
    pub result: Result<()>,
}

/// A connector with its config and manifest read and checked, before any
/// child runs.
struct Prepared<'a> {
    config: &'a ConnectorConfig,
    manifest: ConnectorManifest,
    argv: Vec<std::ffi::OsString>,
    limits: EffectiveLimits,
    landing_dir: PathBuf,
}

fn prepare<'a>(config_path: &Path, loaded: &'a LoadedConfig, name: &str) -> Result<Prepared<'a>> {
    let Some(config) = loaded.config.connectors.get(name) else {
        let known: Vec<&str> = loaded
            .config
            .connectors
            .keys()
            .map(String::as_str)
            .collect();
        if known.is_empty() {
            bail!("no [connector.{name}] in the config; the config declares no connectors");
        }
        bail!(
            "no [connector.{name}] in the config; declared connectors: {}",
            known.join(", ")
        );
    };
    let config_dir = match config_path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let manifest_path = config_dir.join(&config.manifest);
    let manifest_path = manifest_path.canonicalize().with_context(|| {
        format!(
            "cannot find the manifest for connector `{name}` at {}",
            manifest_path.display()
        )
    })?;
    let manifest = ConnectorManifest::load(&manifest_path)?;
    let manifest_dir = manifest_path.parent().unwrap_or(Path::new("."));
    let argv = manifest.resolved_command(manifest_dir);
    let limits = ConnectorLimitsConfig::effective(&manifest.limits, &config.limits);
    let landing_dir = local_output_dir(&config.output_uri, config_dir)
        .map_err(|e| anyhow!("connector `{name}`: {e}"))?;
    Ok(Prepared {
        config,
        manifest,
        argv,
        limits,
        landing_dir,
    })
}

/// Read each declared secret from Rocky's own environment and register its
/// value for redaction. An unset variable is an error: the connector would
/// otherwise run without a credential it was promised.
fn resolve_secrets(names: &[String]) -> Result<Vec<(String, String)>> {
    let mut secrets = Vec::with_capacity(names.len());
    for name in names {
        let value = match std::env::var(name) {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => bail!(
                "the connector declares the secret `{name}`, but that environment variable is not set"
            ),
            Err(std::env::VarError::NotUnicode(_)) => bail!(
                "the connector declares the secret `{name}`, but its value is not valid UTF-8"
            ),
        };
        rocky_core::secret_registry::register_substitution(name, &value);
        secrets.push((name.clone(), value));
    }
    Ok(secrets)
}

/// The claim's lifetime in whole hours: the child's wall time for every
/// attempt, plus an hour for the load, the drift step and the commit,
/// rounded up. At least one hour.
pub fn claim_ttl_hours(wall_time_secs: u64, retries: u32) -> u32 {
    let attempts = u64::from(retries) + 1;
    let secs = wall_time_secs
        .saturating_mul(attempts)
        .saturating_add(CLAIM_HEADROOM_SECS);
    // At most a year, so a clock addition cannot overflow.
    let hours = secs.div_ceil(3600).clamp(1, 24 * 366);
    u32::try_from(hours).unwrap_or(u32::MAX)
}

/// The idempotency key of the batch that follows `commit_seq`.
pub fn claim_key(name: &str, commit_seq: u64) -> String {
    format!("connector:{name}:{commit_seq}")
}

/// The saved state of one connector, from the shared state when a remote
/// backend is configured, else from the local file. Absent state is `None`.
async fn read_saved_state(
    loaded: &LoadedConfig,
    state_path: &Path,
    name: &str,
) -> Result<Option<ConnectorStateRecord>> {
    let session = LedgerSeamSession::new(
        &loaded.config.state,
        state_path,
        loaded.config.cache.schemas.replicate,
    );
    let name = name.to_string();
    session
        .read_state(move |store| store.connector_state_get(&name))
        .await
        .context("failed to read the connector's saved state")
}

/// Execute `rocky load --connector <name>` and print its result.
pub async fn run_connector_load(
    config_path: &Path,
    loaded: &LoadedConfig,
    state_path: &Path,
    name: &str,
    json: bool,
) -> Result<()> {
    let outcome = execute_connector_load(&ConnectorLoadParams::new(
        config_path,
        loaded,
        state_path,
        name,
    ))
    .await;
    if let Some(output) = &outcome.output {
        if json {
            print_json(output)?;
        } else {
            print_text(output);
        }
    }
    outcome.result
}

fn print_text(output: &LoadOutput) {
    let Some(info) = &output.connector else {
        return;
    };
    match info.status {
        ConnectorLoadStatus::Skipped => println!(
            "Connector `{}` skipped: another run holds the claim for this batch.",
            info.name
        ),
        ConnectorLoadStatus::Loaded => println!(
            "Connector `{}` loaded {} file(s), {} rows, {} bytes ({} attempt(s), commit {} -> {}, {} ms)",
            info.name,
            output.files_loaded,
            output.total_rows,
            output.total_bytes,
            info.attempts,
            info.commit_seq_before,
            info.commit_seq_after,
            output.duration_ms,
        ),
        ConnectorLoadStatus::Failed => println!(
            "Connector `{}` failed after {} attempt(s){}",
            info.name,
            info.attempts,
            info.error
                .as_deref()
                .map(|e| format!(": {e}"))
                .unwrap_or_default()
        ),
    }
}

/// Run one connector load and report what happened. Never prints.
pub async fn execute_connector_load(params: &ConnectorLoadParams<'_>) -> ConnectorLoadOutcome {
    let start = Instant::now();
    let name = params.name;

    // Everything here runs before the claim. A failure leaves no trace.
    let (prepared, secrets, saved) = match preflight(params).await {
        Ok(ready) => ready,
        Err(e) => {
            return ConnectorLoadOutcome {
                output: None,
                result: Err(e),
            };
        }
    };
    let commit_seq_before = saved.as_ref().map_or(0, |s| s.commit_seq);

    let mut progress = Progress {
        info: ConnectorLoadInfo {
            name: name.to_string(),
            status: ConnectorLoadStatus::Failed,
            attempts: 0,
            commit_seq_before,
            commit_seq_after: commit_seq_before,
            adopted_expired_claim: false,
            egress_hosts: prepared.manifest.egress_hosts.clone(),
            error: None,
        },
        files: Vec::new(),
        source_dir: prepared.landing_dir.display().to_string(),
    };

    let result = drive(params, &prepared, &secrets, saved, &mut progress).await;
    if let Err(e) = &result {
        progress.info.error = Some(format!("{e:#}"));
    }
    // Count like the directory loader: a file with an error is failed and adds
    // no rows or bytes. A staging or promote failure marks every file, so the batch
    // reports `files_loaded = 0` and `files_failed = n`.
    let files_failed = progress.files.iter().filter(|f| f.error.is_some()).count();
    let files_loaded = progress.files.len() - files_failed;
    let (total_rows, total_bytes) = progress
        .files
        .iter()
        .filter(|f| f.error.is_none())
        .fold((0u64, 0u64), |(r, b), f| {
            (r + f.rows_loaded, b + f.bytes_read)
        });
    let output = LoadOutput {
        version: VERSION.into(),
        command: "load".into(),
        source_dir: progress.source_dir,
        format: "parquet".into(),
        files_loaded,
        files_failed,
        total_rows,
        total_bytes,
        files: progress.files,
        duration_ms: start.elapsed().as_millis() as u64,
        connector: Some(progress.info),
    };
    ConnectorLoadOutcome {
        output: Some(output),
        result,
    }
}

struct Progress {
    info: ConnectorLoadInfo,
    files: Vec<LoadFileOutput>,
    source_dir: String,
}

type Preflight<'a> = (
    Prepared<'a>,
    Vec<(String, String)>,
    Option<ConnectorStateRecord>,
);

/// Read the config, the manifest, the secrets and the saved state, and
/// refuse a downgrade, before anything is claimed or run.
async fn preflight<'a>(params: &ConnectorLoadParams<'a>) -> Result<Preflight<'a>> {
    let prepared = prepare(params.config_path, params.loaded, params.name)?;
    let secrets = resolve_secrets(&prepared.config.secrets)?;
    // Refuse a writer mode the commit would refuse, now. The commit comes
    // after the promote; a refusal there would leave the batch loaded and the
    // claim in flight, and every later run would load it again.
    LedgerSeamSession::new(
        &params.loaded.config.state,
        params.state_path,
        params.loaded.config.cache.schemas.replicate,
    )
    .preflight_writer()
    .await
    .map_err(|e| {
        use rocky_core::state_sync::StateSyncError;
        match e {
            e @ (StateSyncError::CasRequired { .. } | StateSyncError::CasUnsupported { .. }) => {
                anyhow::Error::new(e).context(
                    "the state backend refuses this writer before anything runs; fix [state] \
                     concurrency_control, or use the setting the other writers use",
                )
            }
            e => anyhow::Error::new(e).context(
                "the state backend refuses this writer because the marker check could not \
                 complete. Rocky stops rather than risk loading a batch twice. Retry later",
            ),
        }
    })?;
    // The saved state is read BEFORE the claim: the claim key names the
    // commit sequence this run will extend.
    let saved = read_saved_state(params.loaded, params.state_path, params.name).await?;
    if let Some(saved) = &saved
        && saved.envelope.schema_version > prepared.manifest.state_schema_version
    {
        bail!(
            "connector `{}`: the saved state has schema version {}, but the manifest declares \
             state_schema_version {}. Rocky will not hand a newer state to an older connector; \
             install the newer connector, or remove the saved state on purpose",
            params.name,
            saved.envelope.schema_version,
            prepared.manifest.state_schema_version
        );
    }
    Ok((prepared, secrets, saved))
}

/// The claim a run holds.
struct Claim {
    backend: IdempotencyBackend,
    key: String,
    config: IdempotencyConfig,
}

async fn drive(
    params: &ConnectorLoadParams<'_>,
    prepared: &Prepared<'_>,
    secrets: &[(String, String)],
    saved: Option<ConnectorStateRecord>,
    progress: &mut Progress,
) -> Result<()> {
    let name = params.name;
    let loaded = params.loaded;
    let state_cfg = &loaded.config.state;
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let seq = progress.info.commit_seq_before;

    // --- claim ----------------------------------------------------------
    let backend = IdempotencyBackend::from_state_config(state_cfg);
    let claim_config = IdempotencyConfig {
        in_flight_ttl_hours: claim_ttl_hours(
            prepared.limits.wall_time_secs,
            prepared.config.retries,
        ),
        // A connector batch is done once it succeeds; a failed run frees the
        // key whatever the project's own `dedup_on` says.
        dedup_on: DedupPolicy::Success,
        ..state_cfg.idempotency.clone()
    };
    let key = claim_key(name, seq);
    let now = params.now.unwrap_or_else(Utc::now);
    let verdict = {
        // The local state file takes the claim on the local backend, and
        // mirrors it on the tiered one. Hold it only for the claim: the child
        // runs for a long time.
        let store = match backend {
            IdempotencyBackend::Local
            | IdempotencyBackend::Valkey {
                mirror_to_redb: true,
                ..
            } => Some(
                StateStore::open_with_policy(params.state_path, state_cfg.on_schema_mismatch)
                    .with_context(|| {
                        format!(
                            "failed to open the state store at {} for the idempotency claim",
                            params.state_path.display()
                        )
                    })?,
            ),
            _ => None,
        };
        backend
            .check_and_claim_at(store.as_ref(), &key, &run_id, &claim_config, now)
            .await
            .context("failed to claim the idempotency key for this batch")?
    };
    // After this time another run may adopt the claim, so this run must not
    // send the promote.
    let deadline = now
        .checked_add_signed(chrono::Duration::hours(i64::from(
            claim_config.in_flight_ttl_hours,
        )))
        .unwrap_or(DateTime::<Utc>::MAX_UTC);
    let claim = Claim {
        backend,
        key,
        config: claim_config,
    };
    match &verdict {
        IdempotencyCheck::Proceed => {}
        IdempotencyCheck::AdoptStale {
            prior_run_id,
            prior_state,
        } => {
            // Only a prior entry that was still in flight can mean a run died
            // holding the claim. A failed run on an object store leaves an
            // already-expired `Failed` entry, which is adopted without
            // doubt.
            if *prior_state == rocky_core::idempotency::IdempotencyState::InFlight {
                progress.info.adopted_expired_claim = true;
                warn!(
                    connector = name,
                    prior_run_id = %prior_run_id,
                    "a prior run may have promoted its batch before crashing; \
                     this run may load a duplicate batch"
                );
            } else {
                info!(
                    connector = name,
                    prior_run_id = %prior_run_id,
                    "taking over the claim of a prior run that ended"
                );
            }
        }
        IdempotencyCheck::SkipInFlight { run_id: prior } => {
            info!(
                connector = name,
                held_by = %prior,
                key = %claim.key,
                "another run holds the claim for this batch; skipping"
            );
            progress.info.status = ConnectorLoadStatus::Skipped;
            return Ok(());
        }
        IdempotencyCheck::SkipCompleted { run_id: prior } => {
            // A finished claim for the batch after `seq` means the state
            // should have moved past `seq`. If it did (another run committed
            // between our read and our claim), this is a skip. If it did not,
            // the saved state is behind the claim: it was reset or restored.
            // Loading again would duplicate the batch, and a skip would hide
            // that the connector is stuck.
            let now_saved = read_saved_state(loaded, params.state_path, name).await?;
            let now_seq = now_saved.as_ref().map_or(0, |s| s.commit_seq);
            if now_seq > seq {
                info!(
                    connector = name,
                    held_by = %prior,
                    key = %claim.key,
                    "another run already loaded this batch; skipping"
                );
                progress.info.status = ConnectorLoadStatus::Skipped;
                return Ok(());
            }
            bail!(
                "the saved state is behind claim `{}`: the claim says run {prior} already \
                 loaded this batch, but the connector's commit sequence is still {now_seq}. \
                 The state may have been reset or restored. Restore the state that holds the \
                 later sequence, or wait for the claim to expire (`[state.idempotency] \
                 retention_days`)",
                claim.key
            );
        }
    }

    // --- run the child and check its files ------------------------------
    info!(
        connector = name,
        run_id = %run_id,
        commit_seq = seq,
        egress_hosts = ?prepared.manifest.egress_hosts,
        "running the child connector (egress hosts are declared, not enforced)"
    );
    let state = saved.as_ref().map(|s| &s.envelope);
    let spec = ChildSpec {
        name,
        argv: &prepared.argv,
        secrets,
        run_id: &run_id,
        limits: prepared.limits,
        state,
        state_schema_version: prepared.manifest.state_schema_version,
        landing_dir: &prepared.landing_dir,
        retries: prepared.config.retries,
        output_poll: params.output_poll,
    };
    let ran = match run_with_retries(&spec).await {
        Ok(ran) => ran,
        Err(failed) => {
            progress.info.attempts = failed.attempts;
            finalize_claim(&claim, params, &run_id, FinalOutcome::Failed).await;
            return Err(failed.into());
        }
    };
    // A signal that came after the child exited was swallowed by the signal
    // listener. Stop here, before the batch reaches the warehouse.
    if was_interrupted() {
        progress.info.attempts = ran.attempts;
        finalize_claim(&claim, params, &run_id, FinalOutcome::Failed).await;
        bail!("interrupted by a signal before the batch was loaded; nothing was written");
    }
    let batch = ran.batch;
    progress.info.attempts = ran.attempts;
    progress.source_dir = batch.scratch.display().to_string();
    let target = &prepared.config.target;
    let target_display = format!("{}.{}.{}", target.catalog, target.schema, target.table);
    progress.files = batch
        .files
        .iter()
        .map(|f| LoadFileOutput {
            file: f.path.display().to_string(),
            target: target_display.clone(),
            rows_loaded: f.rows,
            bytes_read: f.bytes,
            duration_ms: 0,
            error: None,
            contract: None,
        })
        .collect();

    // --- stage, drift, promote ------------------------------------------
    if !batch.files.is_empty() {
        let staged = stage_and_promote(
            loaded,
            prepared.config,
            &batch,
            &run_id,
            &PromoteGate {
                deadline,
                now: params.promote_now,
                interrupted: params.interrupt_before_promote,
            },
        )
        .await;
        if let Err(failure) = staged {
            progress.files.iter_mut().for_each(|f| {
                f.error = Some(format!("{:#}", failure.error));
            });
            match failure.stage {
                FailureStage::BeforePromote => {
                    progress.files.iter_mut().for_each(|f| {
                        f.rows_loaded = 0;
                        f.bytes_read = 0;
                    });
                    finalize_claim(&claim, params, &run_id, FinalOutcome::Failed).await;
                    return Err(failure.error);
                }
                FailureStage::PromoteIssued => {
                    progress.files.iter_mut().for_each(|f| {
                        f.rows_loaded = 0;
                        f.bytes_read = 0;
                    });
                    // The promote statement was sent, so the rows may be in
                    // the table. Like a failed commit, this leaves the claim
                    // in flight: a retry would load the batch again.
                    return Err(failure.error.context(format!(
                        "connector `{name}` sent the promote into {target_display}, and it \
                         failed, so the batch may or may not be in the table. The claim `{}` \
                         is left in flight on purpose, so the batch is not loaded again until \
                         the claim expires. Check the table before the claim expires",
                        claim.key
                    )));
                }
            }
        }
    }

    // --- commit ----------------------------------------------------------
    // The batch is in the target now. A failure from here on does NOT free
    // the claim: a retry would load the same batch again. The claim stays
    // InFlight until it expires, and the next run after that may load a
    // duplicate. This is the at-least-once window.
    let committed = commit_state(params, &batch, seq).await;
    match committed {
        Ok(new_seq) => {
            progress.info.commit_seq_after = new_seq;
            progress.info.status = ConnectorLoadStatus::Loaded;
        }
        Err(e) => {
            return Err(e.context(format!(
                "connector `{name}` promoted its batch into {target_display}, but the \
                 connector state was not committed. The claim `{}` is left in flight on \
                 purpose, so the batch is not loaded again until the claim expires. After \
                 that the next run loads it again",
                claim.key
            )));
        }
    }

    // --- finalize ---------------------------------------------------------
    finalize_claim(&claim, params, &run_id, FinalOutcome::Succeeded).await;
    if !prepared.config.keep_scratch {
        remove_scratch(&prepared.landing_dir, &batch.scratch);
    }
    Ok(())
}

/// Remove a committed run's own scratch directory. Only that directory: the
/// landing directory may be shared, and another run may be using its own
/// scratch directory beside this one. A failure is a warning, because the
/// batch is already committed.
fn remove_scratch(landing_dir: &Path, scratch: &Path) {
    // Directly inside the landing directory, as `new_scratch_dir` makes it.
    let inside_landing = match (landing_dir.canonicalize(), scratch.canonicalize()) {
        (Ok(landing), Ok(dir)) => dir.parent() == Some(landing.as_path()),
        (Err(_), _) | (_, Err(_)) => false,
    };
    if !inside_landing {
        warn!(
            scratch = %scratch.display(),
            "kept the scratch directory: it is not inside the landing directory"
        );
        return;
    }
    if let Err(e) = std::fs::remove_dir_all(scratch) {
        warn!(
            scratch = %scratch.display(),
            error = %e,
            "could not remove the scratch directory of a committed run"
        );
    }
}

/// Stamp the claim with its end state. A failure here is a warning: the
/// batch is already decided, and an unstamped claim expires.
async fn finalize_claim(
    claim: &Claim,
    params: &ConnectorLoadParams<'_>,
    run_id: &str,
    outcome: FinalOutcome,
) {
    let state_cfg = &params.loaded.config.state;
    let store = match claim.backend {
        IdempotencyBackend::Local
        | IdempotencyBackend::Valkey {
            mirror_to_redb: true,
            ..
        } => match StateStore::open_with_policy(params.state_path, state_cfg.on_schema_mismatch) {
            Ok(store) => Some(store),
            Err(e) => {
                warn!(error = %e, key = %claim.key, "could not open the state store to finalize the claim; it expires on its own");
                return;
            }
        },
        _ => None,
    };
    let now = params.now.unwrap_or_else(Utc::now);
    if let Err(e) = claim
        .backend
        .finalize_at(
            store.as_ref(),
            &claim.key,
            run_id,
            outcome,
            &claim.config,
            now,
        )
        .await
    {
        warn!(error = %e, key = %claim.key, "failed to finalize the idempotency claim; it expires on its own");
    }
}

/// Write the connector's new state and one ledger row per file, in one
/// ledger transition, refusing when another run moved `commit_seq` since the
/// claim was taken.
async fn commit_state(
    params: &ConnectorLoadParams<'_>,
    batch: &VerifiedBatch,
    expected_seq: u64,
) -> Result<u64> {
    let session = LedgerSeamSession::new(
        &params.loaded.config.state,
        params.state_path,
        params.loaded.config.cache.schemas.replicate,
    );
    let name = params.name.to_string();
    let pipeline_key = format!("connector:{name}");
    let loaded_at = params.now.unwrap_or_else(Utc::now);
    let files: Vec<(String, LoadedFileRecord)> = batch
        .files
        .iter()
        .map(|f| {
            (
                f.path.display().to_string(),
                LoadedFileRecord {
                    loaded_at,
                    rows_loaded: f.rows,
                    bytes_read: f.bytes,
                    duration_ms: 0,
                    file_hash: None,
                },
            )
        })
        .collect();
    let envelope: StateEnvelope = batch.state.clone();
    let new_seq = session
        .execute(|store, _generation| {
            // The store call is synchronous; the future only carries its
            // result, so it borrows nothing from this closure.
            let result = store
                .connector_commit_with_files(&name, expected_seq, &envelope, &pipeline_key, &files)
                .map_err(rocky_core::state_sync::StateSyncError::from);
            Box::pin(async move { result })
        })
        .await
        .context("failed to commit the connector state")?;
    Ok(new_seq)
}

/// When the promote may still be sent.
struct PromoteGate {
    /// The claim's expiry. Past it, another run may adopt the claim.
    deadline: DateTime<Utc>,
    /// The time to compare with `deadline`. `None` is the real time.
    now: Option<DateTime<Utc>>,
    /// Test seam: act as if a signal arrived before the promote.
    interrupted: bool,
}

/// Where a staging, drift or promote failure happened.
#[derive(Debug, Clone, Copy)]
enum FailureStage {
    /// The promote was not sent. The target rows are unchanged, and the claim
    /// is freed. Drift `ALTER`s may have run, and are not rolled back.
    BeforePromote,
    /// The promote statement was sent. The rows may be in the target.
    PromoteIssued,
}

struct StageFailure {
    stage: FailureStage,
    error: anyhow::Error,
}

/// Every `?` on an `anyhow` error becomes `BeforePromote`. So no `?` may
/// follow the point where the promote is sent: an error after that point must
/// be built as `PromoteIssued` by hand.
impl From<anyhow::Error> for StageFailure {
    fn from(error: anyhow::Error) -> Self {
        Self {
            stage: FailureStage::BeforePromote,
            error,
        }
    }
}

/// Load the batch into a staging table, deal with drift, and promote it to
/// the target with one append. Drops the staging table when anything fails.
async fn stage_and_promote(
    loaded: &LoadedConfig,
    config: &ConnectorConfig,
    batch: &VerifiedBatch,
    run_id: &str,
    gate: &PromoteGate,
) -> std::result::Result<(), StageFailure> {
    let registry = AdapterRegistry::from_config(&loaded.config)?;
    let adapter_name = config.target.adapter.as_str();
    let adapter_cfg = registry
        .adapter_config(adapter_name)
        .context(format!("no adapter config for '{adapter_name}'"))?;
    let loader = build_loader(adapter_name, adapter_cfg, &registry)?;
    let warehouse: Arc<dyn WarehouseAdapter> = registry
        .warehouse_adapter(adapter_name)
        .context("failed to resolve the target warehouse adapter")?;

    let target = TableRef {
        catalog: config.target.catalog.clone(),
        schema: config.target.schema.clone(),
        table: config.target.table.clone(),
    };
    let staging_name = format!("{}{STAGING_INFIX}{run_id}", target.table);
    rocky_sql::validation::validate_identifier(&target.table)
        .with_context(|| format!("invalid target table name '{}'", target.table))?;
    rocky_sql::validation::validate_identifier(&target.schema)
        .with_context(|| format!("invalid target schema '{}'", target.schema))?;
    rocky_sql::validation::validate_identifier(&staging_name)
        .with_context(|| format!("invalid staging table name '{staging_name}'"))?;
    let staging = TableRef {
        catalog: target.catalog.clone(),
        schema: target.schema.clone(),
        table: staging_name,
    };

    let dialect = warehouse.dialect();
    let staging_ref = dialect
        .format_table_ref(&staging.catalog, &staging.schema, &staging.table)
        .map_err(|e| anyhow!("failed to format the staging table ref: {e}"))?;
    let drop_staging = dialect.drop_table_sql(&staging_ref);

    let outcome = stage_and_promote_inner(
        warehouse.as_ref(),
        loader.as_ref(),
        config,
        batch,
        &target,
        &staging,
        &staging_ref,
        gate,
    )
    .await;
    match outcome {
        Ok(()) => {
            // The staging table has done its job. A failed drop leaves a
            // table behind; there is no cleanup of old staging tables yet.
            if let Err(e) = warehouse.execute_statement(&drop_staging).await {
                warn!(error = %e, staging = %staging_ref, "failed to drop the staging table after the promote");
            }
            Ok(())
        }
        Err(e) => {
            if let Err(drop_error) = warehouse.execute_statement(&drop_staging).await {
                warn!(error = %drop_error, staging = %staging_ref, "failed to drop the staging table after a failure");
            }
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn stage_and_promote_inner(
    warehouse: &dyn WarehouseAdapter,
    loader: &dyn LoaderAdapter,
    config: &ConnectorConfig,
    batch: &VerifiedBatch,
    target: &TableRef,
    staging: &TableRef,
    staging_ref: &str,
    gate: &PromoteGate,
) -> std::result::Result<(), StageFailure> {
    let promote = stage_and_prepare(
        warehouse,
        loader,
        config,
        batch,
        target,
        staging,
        staging_ref,
    )
    .await?;

    // Last checks before the one statement that changes the target.
    if was_interrupted() || gate.interrupted {
        return Err(
            anyhow!("interrupted by a signal before the promote; the target is unchanged").into(),
        );
    }
    let now = gate.now.unwrap_or_else(Utc::now);
    if now >= gate.deadline {
        return Err(anyhow!(
            "the claim for this batch expired at {} and another run may have adopted it; \
             refusing to promote",
            gate.deadline
        )
        .into());
    }
    // From here on the promote is on its way. Any error means the rows may be
    // in the target.
    warehouse
        .execute_statement(&promote)
        .await
        .map_err(|e| StageFailure {
            stage: FailureStage::PromoteIssued,
            error: anyhow!("the promote failed: {e}"),
        })
}

/// Stage the files, check the contract, settle drift, and return the promote
/// statement. Sends nothing that changes the target's rows.
async fn stage_and_prepare(
    warehouse: &dyn WarehouseAdapter,
    loader: &dyn LoaderAdapter,
    config: &ConnectorConfig,
    batch: &VerifiedBatch,
    target: &TableRef,
    staging: &TableRef,
    staging_ref: &str,
) -> Result<String> {
    let dialect = warehouse.dialect();
    let target_ref = dialect
        .format_table_ref(&target.catalog, &target.schema, &target.table)
        .map_err(|e| anyhow!("failed to format the target table ref: {e}"))?;
    let target_ir = to_ir_table_ref(target);
    let staging_ir = to_ir_table_ref(staging);

    // --- stage: the first file creates the table, the rest append --------
    for (index, file) in batch.files.iter().enumerate() {
        if index > 0 {
            same_parquet_schema(&batch.files[0].path, &file.path).with_context(|| {
                format!(
                    "file `{}` does not have the same columns as `{}`",
                    file.relative, batch.files[0].relative
                )
            })?;
        }
        let options = LoadOptions {
            format: Some(FileFormat::Parquet),
            create_table: index == 0,
            truncate_first: false,
            ..LoadOptions::default()
        };
        loader
            .load(&LoadSource::LocalFile(file.path.clone()), staging, &options)
            .await
            .map_err(|e| anyhow!("staging load of `{}` failed: {e}", file.relative))?;
    }
    let staged_rows = count_rows(warehouse, staging_ref).await?;
    if staged_rows != batch.total_rows() {
        bail!(
            "the staging table holds {staged_rows} rows, but the files hold {}",
            batch.total_rows()
        );
    }
    let staging_cols = warehouse
        .describe_table(&staging_ir)
        .await
        .map_err(|e| anyhow!("failed to describe the staging table: {e}"))?;

    // --- contract on the staging table -----------------------------------
    if let Some(contract) = &config.contract {
        let result = validate_landed_contract(contract, &staging_cols, dialect);
        for warning in &result.warnings {
            warn!(table = %target_ref, "{warning}");
        }
        if !result.passed {
            return Err(ContractGateError(result).into());
        }
    }

    // --- does the target exist? ------------------------------------------
    // A transient failure stops the run rather than guess the target is
    // absent and promote over a table that may be live.
    let target_cols = match warehouse.describe_table(&target_ir).await {
        Ok(cols) if !cols.is_empty() => Some(cols),
        Ok(_) => None,
        Err(e) if warehouse.classify_failure(&e).is_retryable() => {
            return Err(anyhow::Error::from(e).context(format!(
                "the existence check for {target_ref} failed with a retryable error; \
                 refusing to assume the target is absent"
            )));
        }
        Err(_) => None,
    };

    // --- drift ------------------------------------------------------------
    if let Some(target_cols) = &target_cols {
        let drift =
            rocky_core::drift::detect_drift(&target_ir, &staging_cols, target_cols, dialect);
        match drift.action {
            rocky_ir::DriftAction::Ignore => {}
            rocky_ir::DriftAction::AlterColumnTypes => {
                let statements = rocky_core::drift::generate_alter_column_sql(
                    &target_ir,
                    &drift.drifted_columns,
                    dialect,
                )
                .map_err(|e| anyhow!("cannot widen the changed columns: {e}"))?;
                for sql in statements {
                    warehouse
                        .execute_statement(&sql)
                        .await
                        .map_err(|e| anyhow!("failed to widen a column of {target_ref}: {e}"))?;
                }
            }
            rocky_ir::DriftAction::DropAndRecreate => {
                let changed: Vec<String> = drift
                    .drifted_columns
                    .iter()
                    .map(|c| format!("{} ({} -> {})", c.name, c.target_type, c.source_type))
                    .collect();
                bail!(
                    "the batch changes column types in a way that is not a safe widening: {}. \
                     A connector load only appends, so Rocky will not rebuild {target_ref}",
                    changed.join(", ")
                );
            }
        }
        let statements =
            rocky_core::drift::generate_add_column_sql(&target_ir, &drift.added_columns, dialect)
                .map_err(|e| anyhow!("cannot add the new columns: {e}"))?;
        for sql in statements {
            warehouse
                .execute_statement(&sql)
                .await
                .map_err(|e| anyhow!("failed to add a column to {target_ref}: {e}"))?;
        }
    }

    // --- promote: one append, columns named, never by position -----------
    Ok(match &target_cols {
        None => dialect.create_table_as_new(&target_ref, &format!("SELECT * FROM {staging_ref}")),
        Some(_) => named_insert_sql(dialect, &target_ref, staging_ref, &staging_cols)?,
    })
}

/// `INSERT INTO target (c1, c2) SELECT c1, c2 FROM staging`, with every
/// column named. A column name is quoted by the dialect; a name that could
/// break out of the quotes is refused.
fn named_insert_sql(
    dialect: &dyn SqlDialect,
    target_ref: &str,
    staging_ref: &str,
    columns: &[rocky_ir::ColumnInfo],
) -> Result<String> {
    if columns.is_empty() {
        bail!("the staging table has no columns");
    }
    let mut quoted = Vec::with_capacity(columns.len());
    for column in columns {
        if column.name.is_empty() || column.name.contains(['"', '`', '\n', '\r', '\0', '\\']) {
            bail!(
                "column name {:?} cannot be quoted safely; rename it in the connector",
                column.name
            );
        }
        quoted.push(dialect.quote_identifier(&column.name));
    }
    let list = quoted.join(", ");
    // The dialect owns the shape of a named append (SQL Server lifts CTEs
    // out of the derived table). The inner SELECT names every column too.
    Ok(dialect.insert_into_columns(
        target_ref,
        &quoted,
        &format!("SELECT {list} FROM {staging_ref}"),
    ))
}

async fn count_rows(warehouse: &dyn WarehouseAdapter, table_ref: &str) -> Result<u64> {
    let result = warehouse
        .execute_query(&format!("SELECT COUNT(*) FROM {table_ref}"))
        .await
        .map_err(|e| anyhow!("failed to count the staged rows: {e}"))?;
    let cell = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| anyhow!("the row count query returned nothing"))?;
    match cell {
        serde_json::Value::Number(n) => n.as_u64(),
        serde_json::Value::String(s) => s.parse().ok(),
        _ => None,
    }
    .ok_or_else(|| anyhow!("the row count query returned {cell}"))
}

/// Refuse an append whose Parquet columns differ (by name or type) from the
/// first file's. A load by position would otherwise put values in the wrong
/// columns.
fn same_parquet_schema(first: &Path, other: &Path) -> Result<()> {
    use parquet::file::reader::{FileReader, SerializedFileReader};
    let columns = |path: &Path| -> Result<Vec<(String, String)>> {
        let reader = SerializedFileReader::new(std::fs::File::open(path)?)?;
        let schema = reader.metadata().file_metadata().schema_descr();
        Ok(schema
            .columns()
            .iter()
            .map(|c| (c.path().string(), format!("{:?}", c.physical_type())))
            .collect())
    };
    let (a, b) = (columns(first)?, columns(other)?);
    if a != b {
        bail!("expected {a:?}, found {b:?}");
    }
    Ok(())
}
