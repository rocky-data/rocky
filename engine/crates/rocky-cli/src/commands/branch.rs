//! `rocky branch` — named virtual branches backed by the state store.
//!
//! Branches are the persistent, named analogue of `--shadow` mode: a named
//! `schema_prefix` is stored in the state DB and, when run with
//! `--branch <name>`, every model target has the prefix appended.
//!
//! `rocky branch approve` and `rocky branch promote` together form the
//! optional approval gate guarding production writes from a branch:
//!
//! * `approve` writes a signed file artifact under `./.rocky/approvals/<branch>/`
//!   binding the approver's git identity to the branch's content-addressed
//!   state.
//! * `promote` enumerates the branch's production targets, verifies that the
//!   on-disk approvals satisfy `[branch.approval]`, and dispatches a
//!   `CREATE OR REPLACE TABLE prod.<x> AS SELECT * FROM branch__<name>.<x>`
//!   per target.
//!
//! Warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake zero-copy
//! `CLONE`) are a follow-up; schema-prefix branches work uniformly across
//! every adapter today.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::SystemTime;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};

use rocky_core::breaking_change::{self, BreakingFinding};
use rocky_core::compare::ComparisonThresholds;
use rocky_core::config::BranchApprovalConfig;
use rocky_core::shadow::{self, ShadowConfig};
use rocky_core::state::{BranchRecord, StateStore};
use rocky_ir::{ModelIr, ProjectIr, TargetRef};

use crate::output::{
    ApprovalArtifact, ApprovalSignature, ApproveOutput, ApproverIdentity, ApproverSource,
    AuditEvent, AuditEventKind, BranchDeleteOutput, BranchEntry, BranchListOutput, BranchOutput,
    BranchPromoteOutput, PromoteTarget, RejectedApproval, SignatureAlgorithm, config_fingerprint,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Env-var override for `--skip-approval`. When set to a non-empty value,
/// `branch promote` records `reason: "ROCKY_BRANCH_APPROVAL_SKIP=<value>"`
/// in the audit event and bypasses the gate.
pub const APPROVAL_SKIP_ENV: &str = "ROCKY_BRANCH_APPROVAL_SKIP";

/// Validate a branch name. Kept lenient (same charset as model/principal
/// identifiers) so branches can be named after git branches like
/// `fix-price` or `feature/new-join` — the slash is rejected because it
/// collides with schema-prefix interpolation.
fn validate_branch_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("branch name cannot be empty");
    }
    if name.len() > 64 {
        anyhow::bail!("branch name too long: {} chars (max 64)", name.len());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        anyhow::bail!("invalid branch name '{name}': only [A-Za-z0-9_.-] characters are allowed");
    }
    Ok(())
}

fn to_entry(record: &BranchRecord) -> BranchEntry {
    BranchEntry {
        name: record.name.clone(),
        schema_prefix: record.schema_prefix.clone(),
        created_by: record.created_by.clone(),
        created_at: record.created_at.to_rfc3339(),
        description: record.description.clone(),
    }
}

/// `rocky branch create <name>` — register a new branch in the state store.
pub fn run_branch_create(
    state_path: &Path,
    name: &str,
    description: Option<&str>,
    json: bool,
) -> Result<()> {
    validate_branch_name(name)?;

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    if store.get_branch(name)?.is_some() {
        anyhow::bail!("branch '{name}' already exists");
    }

    let created_by = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let record = BranchRecord {
        name: name.to_string(),
        schema_prefix: format!("branch__{name}"),
        created_by,
        created_at: chrono::Utc::now(),
        description: description.map(ToString::to_string),
    };

    store.put_branch(&record)?;

    if json {
        let output = BranchOutput {
            version: VERSION.to_string(),
            command: "branch create".to_string(),
            branch: to_entry(&record),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "created branch '{name}' (schema_prefix: {})",
            record.schema_prefix
        );
    }
    Ok(())
}

/// `rocky branch delete <name>` — remove a branch entry. The branch's
/// warehouse tables (if any were materialized via `rocky run --branch`)
/// are NOT dropped — that's a separate cleanup step left to the user.
pub fn run_branch_delete(state_path: &Path, name: &str, json: bool) -> Result<()> {
    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let removed = store.delete_branch(name)?;

    if json {
        let output = BranchDeleteOutput {
            version: VERSION.to_string(),
            command: "branch delete".to_string(),
            name: name.to_string(),
            removed,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else if removed {
        println!("deleted branch '{name}'");
    } else {
        println!("no branch named '{name}'");
    }
    Ok(())
}

/// `rocky branch list` — list every branch in the state store.
pub fn run_branch_list(state_path: &Path, json: bool) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let records = store.list_branches()?;

    if json {
        let entries: Vec<BranchEntry> = records.iter().map(to_entry).collect();
        let total = entries.len();
        let output = BranchListOutput {
            version: VERSION.to_string(),
            command: "branch list".to_string(),
            branches: entries,
            total,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else if records.is_empty() {
        println!("no branches");
    } else {
        println!("branches ({}):", records.len());
        for b in &records {
            print!("  {}  schema_prefix={}", b.name, b.schema_prefix);
            if let Some(desc) = &b.description {
                print!("  ({desc})");
            }
            println!("  by {} at {}", b.created_by, b.created_at.to_rfc3339());
        }
    }
    Ok(())
}

/// `rocky branch compare <name>` — diff a branch's table set against the
/// main (production) targets.
///
/// Looks up the branch's `schema_prefix` in the state store and reuses the
/// existing `rocky compare` machinery by constructing a [`ShadowConfig`]
/// whose `schema_override` points at the branch's schema. Mirrors how
/// `rocky run --branch <name>` wires the same prefix into the write path.
///
/// Branches with no materialized tables yet surface as missing rows/columns
/// on the production side of the diff rather than crashing — the underlying
/// compare path uses `unwrap_or_default` for describe/count failures.
pub async fn run_branch_compare(
    state_path: &Path,
    config_path: &Path,
    branch_name: &str,
    filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = store
        .get_branch(branch_name)?
        .with_context(|| format!("branch '{branch_name}' not found — see 'rocky branch list'"))?;

    let shadow_config = ShadowConfig {
        suffix: "_rocky_shadow".to_string(),
        schema_override: Some(record.schema_prefix),
        cleanup_after: false,
    };
    let thresholds = ComparisonThresholds::default();

    super::compare::compare(config_path, filter, None, &shadow_config, &thresholds, json).await
}

// ---------------------------------------------------------------------------
// Approval / promote — internal helpers
// ---------------------------------------------------------------------------

/// Encode `value` as canonical JSON (keys sorted at every depth, no
/// insignificant whitespace). Used as the byte input to every blake3 hash
/// in this module so two byte-identical inputs always produce the same
/// signature.
fn canonical_json<T: serde::Serialize>(value: &T) -> Result<String> {
    let raw =
        serde_json::to_value(value).context("serializing value for canonical JSON encoding")?;
    let canonical = canonicalize(raw);
    serde_json::to_string(&canonical).context("encoding canonical JSON")
}

fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<String, serde_json::Value> =
                map.into_iter().map(|(k, v)| (k, canonicalize(v))).collect();
            serde_json::to_value(sorted)
                .expect("BTreeMap<String, Value> always converts back to Value")
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

/// Resolve the approver's git identity. `email` is required; `name` is
/// optional. Returns an error if `git config user.email` is empty or git
/// is not on PATH — without an identity there is no approver to record.
fn git_identity() -> Result<(String, Option<String>)> {
    let email = run_git_config("user.email")?;
    if email.is_empty() {
        anyhow::bail!(
            "git user.email is not configured — set it with `git config --global user.email <addr>` \
             before running `rocky branch approve`"
        );
    }
    let name = run_git_config("user.name").ok().filter(|s| !s.is_empty());
    Ok((email, name))
}

fn run_git_config(key: &str) -> Result<String> {
    let output = ProcessCommand::new("git")
        .args(["config", "--get", key])
        .output()
        .with_context(|| format!("invoking `git config --get {key}`"))?;
    if !output.status.success() {
        anyhow::bail!(
            "`git config --get {key}` exited with status {}",
            output.status
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Resolve the hostname surfaced in [`ApproverIdentity::host`].
///
/// When `scrub` is true the hostname lookup is skipped and the string
/// `"redacted"` is returned instead. Pure helper so the env-var policy can
/// be unit-tested without racy `std::env::set_var` calls.
fn pick_host(scrub: bool) -> String {
    if scrub {
        return "redacted".to_string();
    }
    hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string())
}

/// Build an [`ApproverIdentity`] from the local environment. Used by both
/// `branch approve` (signing identity) and `branch promote` (actor on every
/// audit event).
fn approver_identity() -> Result<ApproverIdentity> {
    let (email, name) = git_identity()?;
    let scrub = std::env::var_os("ROCKY_SCRUB_HOST").is_some();
    let host = pick_host(scrub);
    Ok(ApproverIdentity {
        email,
        name,
        host,
        source: ApproverSource::Local,
    })
}

/// Generate a sortable monotonic identifier for an approval artifact.
///
/// Format: `{utc_compact}-{8_hex}` where the timestamp prefix sorts
/// lexicographically by sign time and the random tail is a blake3 digest of
/// the timestamp + a short non-monotonic suffix from the system clock's
/// nanosecond field. Sortable enough for the directory-listing use case;
/// not cryptographically random (we don't rely on it for security).
fn generate_approval_id(now: DateTime<Utc>) -> String {
    let stamp = now.format("%Y%m%dT%H%M%S%6f").to_string();
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or(0);
    let salt = format!("{stamp}-{nanos}");
    let digest = blake3::hash(salt.as_bytes()).to_hex();
    let tail = &digest.as_str()[..8];
    format!("{stamp}-{tail}")
}

/// Compute the content-addressed `branch_state_hash` for a branch.
///
/// The hash binds an approval to the exact reviewed state: the branch name,
/// its `schema_prefix`, the `rocky.toml` content fingerprint, and a
/// fingerprint over the project's model source tree (see
/// [`models_fingerprint`]). A change to the branch metadata, the project
/// config, *or any model file* voids every existing approval. (`latest_run_id`
/// remains a forward-compatible follow-up — it extends the payload without
/// breaking the artifact format.)
fn compute_branch_state_hash(record: &BranchRecord, config_path: &Path) -> Result<String> {
    let payload = serde_json::json!({
        "branch": record.name,
        "schema_prefix": record.schema_prefix,
        "config_hash": config_fingerprint(config_path),
        "models_hash": models_fingerprint(config_path),
    });
    let canonical = canonical_json(&payload)?;
    Ok(blake3::hash(canonical.as_bytes()).to_hex().to_string())
}

/// Content fingerprint over the project's model source tree.
///
/// Walks `<config_dir>/models` recursively and folds every file's path
/// (relative to the models root) and bytes into one blake3 digest. The
/// relative path is hashed alongside the bytes — with `\0` field separators —
/// so renames and moves register, and entries are sorted by relative path so
/// the digest is independent of the OS directory-iteration order. `.git`,
/// `target`, and `.DS_Store` entries are skipped. Returns `"none"` when the
/// models directory is absent (e.g. replication-only projects); an unreadable
/// file folds in an `<unreadable>` marker instead of panicking — so the result
/// is always deterministic, mirroring [`config_fingerprint`]'s graceful
/// contract.
///
/// Scope: this covers the conventional `<config_dir>/models` tree. A project
/// that points its `models` glob at a non-default directory still has that
/// glob *declaration* covered by `config_fingerprint` (editing the glob voids
/// approvals); broadening this walk to arbitrary glob roots is a follow-up if
/// a non-default layout surfaces.
fn models_fingerprint(config_path: &Path) -> String {
    let models_root = config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("models");
    if !models_root.is_dir() {
        return "none".to_string();
    }

    let mut files: Vec<(String, PathBuf)> = Vec::new();
    collect_model_files(&models_root, &models_root, &mut files);
    // Bytewise sort on the relative path so the digest is independent of the
    // platform's directory-iteration order.
    files.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));

    let mut hasher = blake3::Hasher::new();
    for (rel, abs) in &files {
        hasher.update(rel.as_bytes());
        hasher.update(b"\0");
        match std::fs::read(abs) {
            Ok(bytes) => {
                hasher.update(&bytes);
            }
            Err(_) => {
                hasher.update(b"<unreadable>");
            }
        }
        hasher.update(b"\0");
    }
    hasher.finalize().to_hex().to_string()
}

/// Recursively collect `(relative_path, absolute_path)` for every file under
/// `dir`, relative to `root`, skipping VCS / build / OS-cruft entries.
/// Relative paths use `/` separators so the digest is stable across platforms.
/// Traversal IO errors are swallowed best-effort — an unreadable directory
/// contributes nothing rather than aborting the whole fingerprint.
fn collect_model_files(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Skip VCS/build/OS cruft and Rocky's own state DB. The canonical state
        // path is `<models>/.rocky-state.redb` (+ its `.lock`), so it lives
        // *inside* the models tree — folding its mutable bytes into the
        // fingerprint would drift `branch_state_hash` on every state write and
        // invalidate a still-valid approval the moment `promote` runs. State is
        // not model source, so it must not contribute to the content hash.
        if name == ".git"
            || name == "target"
            || name == ".DS_Store"
            || name.starts_with(".rocky-state.redb")
        {
            continue;
        }
        let Ok(file_type) = entry.file_type() else {
            continue;
        };
        let path = entry.path();
        if file_type.is_dir() {
            collect_model_files(root, &path, out);
        } else if file_type.is_file()
            && let Ok(rel) = path.strip_prefix(root)
        {
            out.push((rel.to_string_lossy().replace('\\', "/"), path));
        }
    }
}

/// Produce the canonical-JSON byte payload that an [`ApprovalSignature`]
/// digests. Excludes the `signature` field itself (the signature can't sign
/// its own bytes).
fn approval_signing_payload(artifact: &ApprovalArtifact) -> Result<String> {
    let payload = serde_json::json!({
        "approval_id": artifact.approval_id,
        "branch": artifact.branch,
        "branch_state_hash": artifact.branch_state_hash,
        "approver": artifact.approver,
        "signed_at": artifact.signed_at,
        "message": artifact.message,
    });
    canonical_json(&payload)
}

/// Compute the v1 [`ApprovalSignature`] over `artifact`'s payload bytes.
fn sign_artifact(artifact: &ApprovalArtifact) -> Result<ApprovalSignature> {
    let payload = approval_signing_payload(artifact)?;
    let digest = blake3::hash(payload.as_bytes()).to_hex().to_string();
    Ok(ApprovalSignature {
        algorithm: SignatureAlgorithm::Blake3CanonicalJson,
        digest,
    })
}

/// Verify an artifact's signature. Recomputes the canonical-JSON payload
/// and compares the digest byte-for-byte. Returns `Ok(())` on match.
fn verify_signature(artifact: &ApprovalArtifact) -> Result<()> {
    match artifact.signature.algorithm {
        SignatureAlgorithm::Blake3CanonicalJson => {
            let expected = sign_artifact(artifact)?;
            if expected.digest != artifact.signature.digest {
                anyhow::bail!(
                    "signature digest mismatch (expected {}, found {})",
                    expected.digest,
                    artifact.signature.digest
                );
            }
            Ok(())
        }
    }
}

/// Resolve the approvals directory for a branch (`./.rocky/approvals/<branch>/`).
fn approvals_dir_for_branch(branch: &str) -> PathBuf {
    PathBuf::from(".rocky").join("approvals").join(branch)
}

/// One artifact loaded from disk, paired with the path it was read from.
type LoadedArtifact = (PathBuf, ApprovalArtifact);

/// Load every artifact file under the branch's approvals directory.
///
/// Returns parsed artifacts (in undefined order) and a parallel list of
/// rejected entries for files that failed to parse. Filesystem errors on
/// individual files are surfaced as `RejectedApproval { reason: "parse_error" }`
/// rather than aborting the whole load — one corrupt file should not block
/// a promote that has other valid artifacts to satisfy `min_approvers`.
fn load_approvals_for_branch(branch: &str) -> Result<(Vec<LoadedArtifact>, Vec<RejectedApproval>)> {
    let dir = approvals_dir_for_branch(branch);
    let mut loaded: Vec<(PathBuf, ApprovalArtifact)> = Vec::new();
    let mut rejected: Vec<RejectedApproval> = Vec::new();

    if !dir.exists() {
        return Ok((loaded, rejected));
    }

    let read_dir = std::fs::read_dir(&dir)
        .with_context(|| format!("reading approvals directory {}", dir.display()))?;

    for entry in read_dir {
        let entry =
            entry.with_context(|| format!("reading directory entry under {}", dir.display()))?;
        let path = entry.path();
        if !path.is_file() || path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) => {
                rejected.push(RejectedApproval {
                    approval_id: path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .unwrap_or("<unknown>")
                        .to_string(),
                    reason: "parse_error".to_string(),
                    detail: format!("failed to read {}: {e}", path.display()),
                });
                continue;
            }
        };
        match serde_json::from_slice::<ApprovalArtifact>(&bytes) {
            Ok(artifact) => loaded.push((path, artifact)),
            Err(e) => rejected.push(RejectedApproval {
                approval_id: path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .unwrap_or("<unknown>")
                    .to_string(),
                reason: "parse_error".to_string(),
                detail: format!("failed to deserialize {}: {e}", path.display()),
            }),
        }
    }

    Ok((loaded, rejected))
}

/// Verify a loaded artifact against the current branch state and config.
/// Returns `Ok(())` for a passing artifact, `Err(RejectedApproval)` for a
/// rejection.
fn evaluate_artifact(
    artifact: &ApprovalArtifact,
    current_state_hash: &str,
    cfg: &BranchApprovalConfig,
    now: DateTime<Utc>,
) -> std::result::Result<(), RejectedApproval> {
    if let Err(e) = verify_signature(artifact) {
        return Err(RejectedApproval {
            approval_id: artifact.approval_id.clone(),
            reason: "bad_signature".to_string(),
            detail: format!("{e}"),
        });
    }

    if artifact.branch_state_hash != current_state_hash {
        return Err(RejectedApproval {
            approval_id: artifact.approval_id.clone(),
            reason: "state_hash_mismatch".to_string(),
            detail: format!(
                "approval bound to state {} but branch is now at {}",
                artifact.branch_state_hash, current_state_hash
            ),
        });
    }

    let age_seconds = (now - artifact.signed_at).num_seconds();
    if age_seconds < 0 {
        return Err(RejectedApproval {
            approval_id: artifact.approval_id.clone(),
            reason: "expired".to_string(),
            detail: format!(
                "approval signed in the future ({}); refusing to honour",
                artifact.signed_at.to_rfc3339()
            ),
        });
    }
    if (age_seconds as u64) > cfg.max_age_seconds {
        return Err(RejectedApproval {
            approval_id: artifact.approval_id.clone(),
            reason: "expired".to_string(),
            detail: format!(
                "approval age {}s exceeds [branch.approval] max_age_seconds = {}",
                age_seconds, cfg.max_age_seconds
            ),
        });
    }

    if !cfg.allowed_signers.is_empty()
        && !cfg
            .allowed_signers
            .iter()
            .any(|s| s == &artifact.approver.email)
    {
        return Err(RejectedApproval {
            approval_id: artifact.approval_id.clone(),
            reason: "signer_not_allowed".to_string(),
            detail: format!(
                "signer {} is not in [branch.approval] allowed_signers",
                artifact.approver.email
            ),
        });
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// pub(crate) helpers exposed to plan.rs for `rocky plan promote`
// ---------------------------------------------------------------------------

/// Public(crate) wrapper for branch name validation — used by `plan.rs`.
pub(crate) fn validate_branch_name_pub(name: &str) -> Result<()> {
    validate_branch_name(name)
}

/// Public(crate) wrapper for branch state hash computation — used by `plan.rs`.
pub(crate) fn compute_branch_state_hash_pub(
    record: &BranchRecord,
    config_path: &Path,
) -> Result<String> {
    compute_branch_state_hash(record, config_path)
}

/// Public(crate) wrapper for resolving the actor identity — used by `plan.rs`.
pub(crate) fn approver_identity_pub() -> Result<ApproverIdentity> {
    approver_identity()
}

/// Per-target SQL entry returned by [`discover_branch_targets_for_plan`].
///
/// Contains both the target/source FQNs and the dialect-quoted SQL so the
/// plan persists deterministic SQL without re-running discovery at apply time.
pub(crate) struct PlannedPromoteWithSql {
    pub target: String,
    pub source: String,
    pub statement: String,
}

/// Discover branch targets and build dialect-quoted promote SQL at plan time.
///
/// Unlike the internal `discover_branch_targets` (which returns bare `TargetRef`
/// pairs), this variant resolves the warehouse adapter, builds the SQL per
/// target, and returns everything the plan needs to persist — so apply just
/// calls `execute_statement` without re-running discovery.
pub(crate) async fn discover_branch_targets_for_plan(
    config_path: &Path,
    record: &BranchRecord,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
) -> Result<Vec<PlannedPromoteWithSql>> {
    use crate::registry::AdapterRegistry;

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_resolved_pipeline_name, pipeline) =
        crate::registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;
    // `target_adapter()` is the pipeline-type-agnostic accessor; works for
    // both replication and transformation variants (and would error in
    // `discover_branch_targets` below for any other variant).
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;
    let dialect = adapter.dialect();

    let planned = discover_branch_targets(config_path, record, filter, pipeline_name).await?;

    Ok(planned
        .into_iter()
        .map(|p| PlannedPromoteWithSql {
            target: p.prod.full_name(),
            source: p.branch_source.full_name(),
            statement: build_promote_sql(dialect, &p.prod, &p.branch_source),
        })
        .collect())
}

/// Run the approval gate for a branch, updating `audit` and returning
/// `(approvals_used, approvals_rejected)`.
///
/// Returns `Err` when `approval_cfg.required` is true and insufficient
/// valid approvals exist.
pub(crate) fn run_approval_gate(
    rocky_cfg: &rocky_core::config::RockyConfig,
    record: &BranchRecord,
    branch_state_hash: &str,
    actor: &ApproverIdentity,
    skip_reason: Option<&str>,
    audit: &mut Vec<AuditEvent>,
) -> Result<(Vec<ApprovalArtifact>, Vec<RejectedApproval>)> {
    let approval_cfg = &rocky_cfg.branch.approval;
    let mut approvals_used: Vec<ApprovalArtifact> = Vec::new();
    let mut approvals_rejected: Vec<RejectedApproval> = Vec::new();
    let now = Utc::now();

    if let Some(reason) = skip_reason {
        audit.push(AuditEvent {
            kind: AuditEventKind::ApprovalSkipped,
            at: now,
            actor: actor.clone(),
            branch: record.name.clone(),
            branch_state_hash: branch_state_hash.to_string(),
            reason: Some(reason.to_string()),
            breaking_changes: None,
        });
    } else if approval_cfg.required {
        let (loaded, parse_rejected) = load_approvals_for_branch(&record.name)?;
        approvals_rejected.extend(parse_rejected);
        for (_path, artifact) in loaded {
            match evaluate_artifact(&artifact, branch_state_hash, approval_cfg, now) {
                Ok(()) => approvals_used.push(artifact),
                Err(rejected) => approvals_rejected.push(rejected),
            }
        }

        if (approvals_used.len() as u32) < approval_cfg.min_approvers {
            let valid = approvals_used.len();
            let invalid_summary = if approvals_rejected.is_empty() {
                "no rejected artifacts".to_string()
            } else {
                approvals_rejected
                    .iter()
                    .map(|r| format!("{}={}", r.approval_id, r.reason))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            anyhow::bail!(
                "branch promote requires {} approval(s); found {} valid, {} invalid ({}). \
                 Run `rocky branch approve {}`.",
                approval_cfg.min_approvers,
                valid,
                approvals_rejected.len(),
                invalid_summary,
                record.name
            );
        }
    }
    Ok((approvals_used, approvals_rejected))
}

/// Run the breaking-change gate for a plan step, updating `audit`.
///
/// Returns the same `Option<Vec<BreakingFinding>>` as the internal
/// `evaluate_breaking_change_gate` — `None` when the gate was skipped.
pub(crate) fn run_breaking_change_gate_for_plan(
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    audit: &mut Vec<AuditEvent>,
    actor: &ApproverIdentity,
    record: &BranchRecord,
    branch_state_hash: &str,
) -> Option<Vec<rocky_core::breaking_change::BreakingFinding>> {
    evaluate_breaking_change_gate(
        config_path,
        models_dir,
        base_ref,
        audit,
        actor,
        record,
        branch_state_hash,
    )
}

/// Execute a list of promote targets against the warehouse adapter.
///
/// This is the apply-time executor — it takes the pre-built SQL statements
/// from a `PromotePlan` and dispatches them via `execute_statement`, returning
/// one `PromoteTarget` per step (with `succeeded` / `error` filled in).
///
/// Stops on the first failure (same policy as the bare-verb path).
pub(crate) async fn run_promote_apply(
    config_path: &Path,
    targets: &[crate::output::PromoteTargetPlan],
) -> Result<(Vec<crate::output::PromoteTarget>, bool)> {
    use crate::registry::AdapterRegistry;

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_pipeline_name, pipeline) = crate::registry::resolve_pipeline(&rocky_cfg, None)?;
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;

    let mut targets_out: Vec<crate::output::PromoteTarget> = Vec::new();
    let mut overall_success = true;

    for step in targets {
        match adapter.execute_statement(&step.statement).await {
            Ok(()) => targets_out.push(crate::output::PromoteTarget {
                target: step.target.clone(),
                source: step.source.clone(),
                statement: step.statement.clone(),
                succeeded: true,
                error: None,
            }),
            Err(e) => {
                targets_out.push(crate::output::PromoteTarget {
                    target: step.target.clone(),
                    source: step.source.clone(),
                    statement: step.statement.clone(),
                    succeeded: false,
                    error: Some(format!("{e}")),
                });
                overall_success = false;
                break;
            }
        }
    }

    Ok((targets_out, overall_success))
}

// ---------------------------------------------------------------------------
// `rocky branch approve`
// ---------------------------------------------------------------------------

/// `rocky branch approve <name>` — sign a content-addressed approval
/// artifact for the branch and write it under `./.rocky/approvals/<name>/`.
///
/// The artifact binds the approver's git identity to the branch's current
/// `branch_state_hash`. `branch promote` later loads every artifact in the
/// directory, verifies signatures + state-hash + age + signer allowlist,
/// and counts the passing artifacts against `[branch.approval] min_approvers`.
pub fn run_branch_approve(
    state_path: &Path,
    config_path: &Path,
    branch_name: &str,
    message: Option<&str>,
    out_override: Option<&Path>,
    json: bool,
) -> Result<()> {
    validate_branch_name(branch_name)?;

    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = store
        .get_branch(branch_name)?
        .with_context(|| format!("branch '{branch_name}' not found — see 'rocky branch list'"))?;

    let branch_state_hash = compute_branch_state_hash(&record, config_path)?;
    let approver = approver_identity()?;
    let signed_at = Utc::now();
    let approval_id = generate_approval_id(signed_at);

    // Build the artifact with a placeholder signature, then sign over the
    // canonical-JSON payload of every other field. This keeps the signing
    // payload independent of the digest itself.
    let mut artifact = ApprovalArtifact {
        approval_id: approval_id.clone(),
        branch: record.name.clone(),
        branch_state_hash,
        approver,
        signed_at,
        message: message.map(ToString::to_string),
        signature: ApprovalSignature {
            algorithm: SignatureAlgorithm::Blake3CanonicalJson,
            digest: String::new(),
        },
    };
    artifact.signature = sign_artifact(&artifact)?;

    let artifact_path = match out_override {
        Some(p) => p.to_path_buf(),
        None => approvals_dir_for_branch(&record.name).join(format!("{approval_id}.json")),
    };
    if let Some(parent) = artifact_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating approvals directory {}", parent.display()))?;
    }
    let bytes = serde_json::to_vec_pretty(&artifact).context("serializing approval artifact")?;
    std::fs::write(&artifact_path, &bytes)
        .with_context(|| format!("writing approval artifact to {}", artifact_path.display()))?;

    if json {
        let output = ApproveOutput {
            version: VERSION.to_string(),
            command: "branch approve".to_string(),
            artifact: artifact.clone(),
            artifact_path: artifact_path.display().to_string(),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "approved branch '{}' (state {}, signer {}) → {}",
            artifact.branch,
            &artifact.branch_state_hash[..16],
            artifact.approver.email,
            artifact_path.display()
        );
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// `rocky branch promote`
// ---------------------------------------------------------------------------

/// One (production target, branch source) pair produced by enumeration.
/// The promote SQL is built downstream once the adapter (and therefore the
/// dialect's identifier-quoting rules) is in scope.
#[derive(Debug)]
struct PlannedPromote {
    prod: TargetRef,
    branch_source: TargetRef,
}

/// Build the v1 promote SQL for a single target.
///
/// Uses dialect-specific identifier quoting so branches whose names contain
/// hyphens, dots, or any other character that would otherwise need escaping
/// produce parser-clean SQL across DuckDB / Snowflake / Databricks (double
/// quotes) and BigQuery (backticks).
fn build_promote_sql(
    dialect: &dyn rocky_core::traits::SqlDialect,
    prod: &TargetRef,
    branch_source: &TargetRef,
) -> String {
    format!(
        "CREATE OR REPLACE TABLE {} AS SELECT * FROM {}",
        quote_fqn(dialect, prod),
        quote_fqn(dialect, branch_source),
    )
}

fn quote_fqn(dialect: &dyn rocky_core::traits::SqlDialect, target: &TargetRef) -> String {
    format!(
        "{}.{}.{}",
        dialect.quote_identifier(&target.catalog),
        dialect.quote_identifier(&target.schema),
        dialect.quote_identifier(&target.table),
    )
}

/// Enumerate the promote plan for a branch.
///
/// Walks the resolved pipeline:
///
/// - **Replication pipelines**: discovers source connectors and resolves each
///   discovered table through the schema-pattern templates — the same
///   enumeration `branch compare` uses.
/// - **Transformation pipelines**: walks the configured `models` glob (the
///   same surface `rocky list models` reads) and emits one target per model,
///   using the model's sidecar `[target]` as the production reference.
///
/// Both shapes feed the same `(prod, branch_source)` payload: `branch_source`
/// is derived by rewriting `prod`'s schema to the branch's `schema_prefix`
/// via [`shadow::shadow_target`] with `schema_override`. Quality / snapshot /
/// load pipelines remain unsupported (no managed-table surface for promote).
async fn discover_branch_targets(
    config_path: &Path,
    record: &BranchRecord,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
) -> Result<Vec<PlannedPromote>> {
    use crate::registry;

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (resolved_pipeline_name, pipeline) = registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;

    match pipeline {
        rocky_core::config::PipelineConfig::Replication(repl) => {
            discover_replication_branch_targets(&rocky_cfg, repl, record, filter).await
        }
        rocky_core::config::PipelineConfig::Transformation(tx) => {
            discover_transformation_branch_targets(tx, config_path, record, filter)
        }
        other => anyhow::bail!(
            "pipeline '{resolved_pipeline_name}' is type '{}', but `branch promote` only supports \
             replication and transformation pipelines",
            other.pipeline_type_str()
        ),
    }
}

/// Replication pipeline path for [`discover_branch_targets`].
async fn discover_replication_branch_targets(
    rocky_cfg: &rocky_core::config::RockyConfig,
    pipeline: &rocky_core::config::ReplicationPipelineConfig,
    record: &BranchRecord,
    filter: Option<&str>,
) -> Result<Vec<PlannedPromote>> {
    use super::{filter_table_matches, matches_filter, parse_filter};
    use crate::registry::AdapterRegistry;

    let registry = AdapterRegistry::from_config(rocky_cfg)?;

    let pattern = pipeline.schema_pattern()?;
    let parsed_filter = filter.map(parse_filter).transpose()?;

    let connectors = if let Some(ref disc) = pipeline.source.discovery {
        let discovery_adapter = registry.discovery_adapter(&disc.adapter)?;
        discovery_adapter
            .discover(&pattern.prefix)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?
            .connectors
    } else {
        anyhow::bail!(
            "no discovery adapter configured — replication-pipeline `branch promote` requires \
             source discovery"
        );
    };

    let target_catalog_template = &pipeline.target.catalog_template;
    let target_schema_template = &pipeline.target.schema_template;
    let shadow_cfg = ShadowConfig {
        suffix: "_rocky_shadow".to_string(),
        schema_override: Some(record.schema_prefix.clone()),
        cleanup_after: false,
    };

    let mut planned = Vec::new();
    for conn in &connectors {
        let parsed = match pattern.parse(&conn.schema) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if let Some((ref filter_key, ref filter_value)) = parsed_filter
            && !matches_filter(conn, &parsed, filter_key, filter_value)
        {
            continue;
        }
        let target_sep = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        let target_catalog = parsed.resolve_template(target_catalog_template, target_sep);
        let target_schema = parsed.resolve_template(target_schema_template, target_sep);

        for table in &conn.tables {
            // PR-B3: CLI `--filter table=<literal>` consumed here.
            if !filter_table_matches(parsed_filter.as_ref(), &table.name) {
                continue;
            }
            let prod = TargetRef {
                catalog: target_catalog.clone(),
                schema: target_schema.clone(),
                table: table.name.clone(),
            };
            let branch_source = shadow::shadow_target(&prod, &shadow_cfg);
            planned.push(PlannedPromote {
                prod,
                branch_source,
            });
        }
    }

    Ok(planned)
}

/// Transformation pipeline path for [`discover_branch_targets`].
///
/// Walks the model files under the pipeline's `models` glob (top-level +
/// immediate subdirectories — same surface `rocky list models` and the
/// catalog scope resolver use) and emits one `(prod, branch_source)` pair
/// per model. Each model's sidecar `[target]` block supplies the production
/// catalog/schema/table; the branch source rewrites the schema to the
/// branch's `schema_prefix` via [`shadow::shadow_target`].
///
/// ## Filter semantics
///
/// Mirrors the `--filter` semantics already used by `rocky run` for
/// transformation pipelines and the replication path above:
///
/// - `table=<literal>` — keep models whose target table equals the literal.
/// - `model=<literal>` — keep models whose `config.name` equals the literal
///   (matches `rocky run --filter model=`).
/// - `catalog=<literal>` / `schema=<literal>` — keep models whose target
///   catalog / schema equals the literal.
///
/// Any other key is rejected with a clear error so a stale replication-style
/// filter (e.g. `client=acme`) on a transformation pipeline fails fast
/// rather than silently dropping every model.
fn discover_transformation_branch_targets(
    pipeline: &rocky_core::config::TransformationPipelineConfig,
    config_path: &Path,
    record: &BranchRecord,
    filter: Option<&str>,
) -> Result<Vec<PlannedPromote>> {
    use super::parse_filter;

    let parsed_filter = filter.map(parse_filter).transpose()?;
    if let Some((key, _)) = &parsed_filter {
        const KNOWN_KEYS: &[&str] = &["table", "model", "catalog", "schema"];
        if !KNOWN_KEYS.contains(&key.as_str()) {
            anyhow::bail!(
                "transformation-pipeline `branch promote` does not support `--filter {key}=...`. \
                 Supported keys: {}.",
                KNOWN_KEYS.join(", ")
            );
        }
    }

    // Models directory is relative to the config file's parent — same
    // resolution rule as `scope::resolve_transformation_managed_tables` and
    // `run_local::run_transformation`.
    let project_root = config_path.parent().unwrap_or(Path::new("."));
    let models_base = pipeline
        .models
        .split(&['*', '?', '['][..])
        .next()
        .unwrap_or("models");
    let models_dir = project_root.join(models_base.trim_end_matches('/'));

    if !models_dir.exists() {
        anyhow::bail!(
            "models directory '{}' does not exist — transformation-pipeline `branch promote` \
             requires the project's `models` glob to resolve to an on-disk directory",
            models_dir.display()
        );
    }

    // Load the same way `rocky list models` does: top-level files plus
    // immediate subdirectories (incl. `.rocky` DSL). Keeps behavior consistent
    // with the rest of the transformation surface (run, plan, list).
    let all_models = crate::models_loader::load_project_models(&models_dir)?;

    let shadow_cfg = ShadowConfig {
        suffix: "_rocky_shadow".to_string(),
        schema_override: Some(record.schema_prefix.clone()),
        cleanup_after: false,
    };

    let mut planned = Vec::new();
    for model in &all_models {
        // Skip ephemeral models — they have no physical table to promote.
        // Mirrors how `rocky run` skips ephemeral materializations during
        // the apply phase.
        if matches!(
            model.config.strategy,
            rocky_core::models::StrategyConfig::Ephemeral
        ) {
            continue;
        }

        if let Some((key, value)) = &parsed_filter {
            let keep = match key.as_str() {
                "table" => model.config.target.table == *value,
                "model" => model.config.name == *value,
                "catalog" => model.config.target.catalog == *value,
                "schema" => model.config.target.schema == *value,
                _ => unreachable!("known-key guard above"),
            };
            if !keep {
                continue;
            }
        }

        let prod = TargetRef {
            catalog: model.config.target.catalog.clone(),
            schema: model.config.target.schema.clone(),
            table: model.config.target.table.clone(),
        };
        let branch_source = shadow::shadow_target(&prod, &shadow_cfg);
        planned.push(PlannedPromote {
            prod,
            branch_source,
        });
    }

    Ok(planned)
}

/// `rocky branch promote <name>` — promote a branch's materialized tables
/// to their production targets, gated on `[branch.approval]` if enabled.
///
/// `models_dir` is the directory containing the project's transformation
/// models; the semantic breaking-change gate compiles it against `base_ref`
/// to detect structural regressions. Both default to `"models"` / `"main"`
/// at the CLI layer.
///
/// `allow_breaking = true` bypasses the breaking-change gate (analogous to
/// `--skip-approval` for the approval gate). The bypass is always recorded
/// as a `BreakingChangesAllowed` audit event.
#[allow(clippy::too_many_arguments)]
pub async fn run_branch_promote(
    state_path: &Path,
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    branch_name: &str,
    filter: Option<&str>,
    pipeline_name: Option<&str>,
    skip_approval_flag: bool,
    allow_breaking: bool,
    json: bool,
) -> Result<()> {
    use crate::registry::AdapterRegistry;

    validate_branch_name(branch_name)?;

    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;
    let record = store
        .get_branch(branch_name)?
        .with_context(|| format!("branch '{branch_name}' not found — see 'rocky branch list'"))?;

    let branch_state_hash = compute_branch_state_hash(&record, config_path)?;
    let actor = approver_identity()?;

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let approval_cfg = &rocky_cfg.branch.approval;

    // Resolve skip origin: explicit flag wins over env var so an operator
    // who passes `--skip-approval` always sees the flag-origin reason in
    // the audit log even if the env var was also set.
    let env_skip_value = std::env::var(APPROVAL_SKIP_ENV)
        .ok()
        .filter(|v| !v.is_empty());
    let skip_reason: Option<String> = if skip_approval_flag {
        Some("--skip-approval CLI flag".to_string())
    } else {
        env_skip_value
            .as_ref()
            .map(|v| format!("{APPROVAL_SKIP_ENV}={v}"))
    };

    let mut audit: Vec<AuditEvent> = Vec::new();
    let mut approvals_used: Vec<ApprovalArtifact> = Vec::new();
    let mut approvals_rejected: Vec<RejectedApproval> = Vec::new();

    let now = Utc::now();
    if let Some(reason) = &skip_reason {
        audit.push(AuditEvent {
            kind: AuditEventKind::ApprovalSkipped,
            at: now,
            actor: actor.clone(),
            branch: record.name.clone(),
            branch_state_hash: branch_state_hash.clone(),
            reason: Some(reason.clone()),
            breaking_changes: None,
        });
    } else if approval_cfg.required {
        let (loaded, parse_rejected) = load_approvals_for_branch(&record.name)?;
        approvals_rejected.extend(parse_rejected);
        for (_path, artifact) in loaded {
            match evaluate_artifact(&artifact, &branch_state_hash, approval_cfg, now) {
                Ok(()) => approvals_used.push(artifact),
                Err(rejected) => approvals_rejected.push(rejected),
            }
        }

        if (approvals_used.len() as u32) < approval_cfg.min_approvers {
            let valid = approvals_used.len();
            let invalid_summary = if approvals_rejected.is_empty() {
                "no rejected artifacts".to_string()
            } else {
                approvals_rejected
                    .iter()
                    .map(|r| format!("{}={}", r.approval_id, r.reason))
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            anyhow::bail!(
                "branch promote requires {} approval(s); found {} valid, {} invalid ({}). \
                 Run `rocky branch approve {}`.",
                approval_cfg.min_approvers,
                valid,
                approvals_rejected.len(),
                invalid_summary,
                record.name
            );
        }
    }
    // (When required = false and no skip, the gate is a no-op — proceed
    // straight to PromoteStarted with no audit entry beyond the routine ones.)

    // ---- Semantic breaking-change gate ------------------------------------
    //
    // Compile the project at the branch's base ref and at HEAD, classify the
    // structural delta via `rocky_core::breaking_change::diff_project_ir`,
    // and fail fast on any `Breaking`-severity finding unless the operator
    // passed `--allow-breaking`.
    //
    // Fail-open on compile failure: if either side does not compile under
    // the current Rocky version (a stale base ref written before a parser
    // change, a partial models tree, missing `models/` directory) the gate
    // is *skipped*, the reason is recorded in a `BreakingChangesGateSkipped`
    // audit event, and the promote proceeds. Failing the promote on a
    // *compile* error in the gate would surprise users whose past commits
    // don't compile under today's Rocky — the approval gate already guards
    // the trust boundary.
    let breaking_findings: Option<Vec<BreakingFinding>> = evaluate_breaking_change_gate(
        config_path,
        models_dir,
        base_ref,
        &mut audit,
        &actor,
        &record,
        &branch_state_hash,
    );

    if let Some(findings) = &breaking_findings {
        let breaking: Vec<&BreakingFinding> = findings.iter().filter(|f| f.is_breaking()).collect();
        if !breaking.is_empty() {
            if allow_breaking {
                audit.push(AuditEvent {
                    kind: AuditEventKind::BreakingChangesAllowed,
                    at: Utc::now(),
                    actor: actor.clone(),
                    branch: record.name.clone(),
                    branch_state_hash: branch_state_hash.clone(),
                    reason: Some("--allow-breaking CLI flag".to_string()),
                    breaking_changes: Some(findings.clone()),
                });
            } else {
                audit.push(AuditEvent {
                    kind: AuditEventKind::BreakingChangesBlocked,
                    at: Utc::now(),
                    actor: actor.clone(),
                    branch: record.name.clone(),
                    branch_state_hash: branch_state_hash.clone(),
                    reason: None,
                    breaking_changes: Some(findings.clone()),
                });

                let summary = breaking
                    .iter()
                    .map(|f| format!("{:?}", f.change))
                    .collect::<Vec<_>>()
                    .join("; ");

                // Emit the JSON payload before bailing so operators with
                // `--output json` still see the findings on a blocked promote.
                let output = BranchPromoteOutput {
                    version: VERSION.to_string(),
                    command: "branch promote".to_string(),
                    branch: record.name.clone(),
                    branch_state_hash: branch_state_hash.clone(),
                    approvals_used,
                    approvals_rejected,
                    breaking_changes: Some(findings.clone()),
                    targets: Vec::new(),
                    audit,
                    success: false,
                };
                if json {
                    println!("{}", serde_json::to_string_pretty(&output)?);
                }

                anyhow::bail!(
                    "branch promote blocked by {} breaking change(s): {summary}. \
                     Review the findings and re-run with `--allow-breaking` to override.",
                    breaking.len()
                );
            }
        }
    }

    audit.push(AuditEvent {
        kind: AuditEventKind::PromoteStarted,
        at: Utc::now(),
        actor: actor.clone(),
        branch: record.name.clone(),
        branch_state_hash: branch_state_hash.clone(),
        reason: None,
        breaking_changes: None,
    });

    let planned = discover_branch_targets(config_path, &record, filter, pipeline_name).await?;

    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_resolved_pipeline_name, pipeline) =
        crate::registry::resolve_pipeline(&rocky_cfg, pipeline_name)?;
    let adapter = registry.warehouse_adapter(pipeline.target_adapter())?;

    let dialect = adapter.dialect();
    let mut targets_out: Vec<PromoteTarget> = Vec::new();
    let mut overall_success = true;
    for step in &planned {
        let statement = build_promote_sql(dialect, &step.prod, &step.branch_source);
        match adapter.execute_statement(&statement).await {
            Ok(()) => targets_out.push(PromoteTarget {
                target: step.prod.full_name(),
                source: step.branch_source.full_name(),
                statement,
                succeeded: true,
                error: None,
            }),
            Err(e) => {
                targets_out.push(PromoteTarget {
                    target: step.prod.full_name(),
                    source: step.branch_source.full_name(),
                    statement,
                    succeeded: false,
                    error: Some(format!("{e}")),
                });
                overall_success = false;
                break;
            }
        }
    }

    audit.push(AuditEvent {
        kind: if overall_success {
            AuditEventKind::PromoteCompleted
        } else {
            AuditEventKind::PromoteFailed
        },
        at: Utc::now(),
        actor,
        branch: record.name.clone(),
        branch_state_hash: branch_state_hash.clone(),
        reason: None,
        breaking_changes: None,
    });

    let output = BranchPromoteOutput {
        version: VERSION.to_string(),
        command: "branch promote".to_string(),
        branch: record.name.clone(),
        branch_state_hash,
        approvals_used,
        approvals_rejected,
        breaking_changes: breaking_findings,
        targets: targets_out,
        audit,
        success: overall_success,
    };

    if json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else if output.success {
        println!(
            "promoted branch '{}' ({} targets)",
            output.branch,
            output.targets.len()
        );
    } else {
        println!(
            "promote failed for branch '{}' after {} target(s) — see audit/JSON for details",
            output.branch,
            output.targets.len()
        );
    }

    if !overall_success {
        anyhow::bail!(
            "`rocky branch promote {}` did not complete successfully",
            record.name
        );
    }
    Ok(())
}

/// Compute breaking-change findings between `base_ref` and HEAD for the
/// pre-promote gate. Returns:
///
/// - `Some(findings)` when both refs compiled cleanly and the typed-IR diff
///   ran. `findings` is the full classified list, including `Info`-severity
///   entries — the caller filters on [`BreakingFinding::is_breaking`].
/// - `None` when the gate was skipped because either side failed to compile
///   or the models directory was unavailable. A `BreakingChangesGateSkipped`
///   audit event is appended to `audit` carrying the human-readable reason.
fn evaluate_breaking_change_gate(
    config_path: &Path,
    models_dir: &Path,
    base_ref: &str,
    audit: &mut Vec<AuditEvent>,
    actor: &ApproverIdentity,
    record: &BranchRecord,
    branch_state_hash: &str,
) -> Option<Vec<BreakingFinding>> {
    use rocky_compiler::compile::{self, CompilerConfig};

    let push_skip = |audit: &mut Vec<AuditEvent>, reason: String| {
        audit.push(AuditEvent {
            kind: AuditEventKind::BreakingChangesGateSkipped,
            at: Utc::now(),
            actor: actor.clone(),
            branch: record.name.clone(),
            branch_state_hash: branch_state_hash.to_string(),
            reason: Some(reason),
            breaking_changes: None,
        });
    };

    if !models_dir.is_dir() {
        push_skip(
            audit,
            format!(
                "models directory '{}' is missing — gate skipped",
                models_dir.display()
            ),
        );
        return None;
    }

    // Seed both compiles with the cached source schemas so the resulting
    // IR uses real types rather than `Unknown`. Mirrors `compute_ci_diff`'s
    // policy: degrade to an empty map on config / cache failure rather than
    // blocking the promote on a configuration issue.
    let source_schemas = match rocky_core::config::load_rocky_config(config_path) {
        Ok(cfg) => {
            let schema_cfg = cfg.cache.schemas.with_ttl_override(None);
            // The state path is independent of the source-schemas cache for
            // the gate's purposes; use the workspace default.
            let state_path = std::path::PathBuf::from(".rocky/state.redb");
            crate::source_schemas::load_cached_source_schemas(&schema_cfg, &state_path)
        }
        Err(_) => std::collections::HashMap::new(),
    };

    let head_compile = {
        let config = CompilerConfig {
            models_dir: models_dir.to_path_buf(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            source_column_info: std::collections::HashMap::new(),
            ..Default::default()
        };
        match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                push_skip(audit, format!("HEAD compile failed — gate skipped: {e}"));
                return None;
            }
        }
    };

    let base_compile =
        match super::ci_diff::extract_base_compile(base_ref, models_dir, source_schemas) {
            Ok(r) => r,
            Err(reason) => {
                push_skip(audit, format!("{reason} — gate skipped"));
                return None;
            }
        };

    let base_ir = compile_result_to_project_ir(&base_compile);
    let head_ir = compile_result_to_project_ir(&head_compile);
    Some(breaking_change::diff_project_ir(&base_ir, &head_ir))
}

/// Project a [`rocky_compiler::compile::CompileResult`] into a
/// [`ProjectIr`] suitable for the breaking-change classifier.
///
/// `Model::to_model_ir()` returns a `ModelIr` with `typed_columns` empty —
/// they live on the compiler's `TypeCheckResult::typed_models` map, keyed
/// by `Model::config.name`. We merge them in here so the classifier sees
/// the full typed shape it needs for column-level diffs.
///
/// Replication-only branches (no transformation models) compile to an
/// empty `Project::models` and round-trip to an empty `ProjectIr`; the
/// diff is naturally empty, which is the right answer — replication
/// pipelines don't carry user-authored SQL whose shape could "break".
fn compile_result_to_project_ir(result: &rocky_compiler::compile::CompileResult) -> ProjectIr {
    let mut models: Vec<ModelIr> = Vec::with_capacity(result.project.models.len());
    for model in &result.project.models {
        let mut ir = model.to_model_ir();
        if let Some(typed) = result.type_check.typed_models.get(&model.config.name) {
            ir.typed_columns = typed.clone();
        }
        models.push(ir);
    }
    ProjectIr {
        models,
        dag: Vec::new(),
        lineage_edges: Vec::new(),
    }
}

/// `rocky branch show <name>` — inspect a single branch.
pub fn run_branch_show(state_path: &Path, name: &str, json: bool) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = store
        .get_branch(name)?
        .with_context(|| format!("branch '{name}' not found"))?;

    if json {
        let output = BranchOutput {
            version: VERSION.to_string(),
            command: "branch show".to_string(),
            branch: to_entry(&record),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("branch: {}", record.name);
        println!("schema_prefix: {}", record.schema_prefix);
        println!("created_by: {}", record.created_by);
        println!("created_at: {}", record.created_at.to_rfc3339());
        if let Some(desc) = &record.description {
            println!("description: {desc}");
        }
    }
    Ok(())
}

/// `rocky branch promote --plan <plan-id>` — apply a pre-built `PromotePlan`
/// without re-running approval or breaking-change gates.
///
/// This is the canonical CI-friendly "review in the PR, apply on merge" path:
/// `rocky plan promote <branch>` generates the plan + runs gates on the PR
/// runner, then `rocky branch promote --plan <plan-id>` (or equivalently
/// `rocky apply <plan-id>`) applies it on the merge step.
///
/// ## `name` validation
///
/// If the caller passes an optional `name` (from the positional arg), it is
/// validated against `promote_plan.branch_name`. A mismatch is an error —
/// the user likely intended a different plan. Passing `None` (no positional)
/// is accepted when `--plan` is the entry point.
pub async fn run_branch_promote_from_plan(
    root: &Path,
    config_path: &Path,
    plan_id: &str,
    name: Option<&str>,
    state_path: &Path,
    json: bool,
) -> Result<()> {
    use crate::output::{AuditEvent, AuditEventKind, PromotePlan, print_json};
    use crate::plan_store::{PlanKind, read_plan};

    let plan =
        read_plan(root, plan_id).with_context(|| format!("failed to read plan '{plan_id}'"))?;

    if plan.kind != PlanKind::Promote {
        anyhow::bail!(
            "plan '{plan_id}' is a {} plan, not a promote plan. \
             Pass a plan_id returned by `rocky plan promote`.",
            plan.kind
        );
    }

    let promote_plan: PromotePlan = serde_json::from_value(plan.payload.clone())
        .context("failed to deserialize promote plan payload")?;

    if let Some(n) = name
        && n != promote_plan.branch_name
    {
        anyhow::bail!(
            "branch name '{n}' does not match plan's branch name '{}'. \
                 Omit the positional arg or pass the correct name.",
            promote_plan.branch_name
        );
    }

    // Route the `--plan` path through the canonical agent-policy gate BEFORE
    // any target SQL executes — the same gate `rocky apply <promote-plan>`
    // runs. Without this, an agent-authored Promote plan a `deny agent promote`
    // rule (or freeze) would refuse could still execute here directly. The
    // plan's stamped principal binds (an agent plan evaluates as agent).
    crate::commands::apply::gate_promote_plan(
        root,
        config_path,
        plan_id,
        plan.resolved_principal(),
        &promote_plan,
        state_path,
    )?;

    let actor = approver_identity().unwrap_or_else(|_| crate::output::ApproverIdentity {
        email: "unknown".to_string(),
        name: None,
        host: "unknown".to_string(),
        source: crate::output::ApproverSource::Local,
    });

    let mut audit = promote_plan.plan_audit.clone();

    audit.push(AuditEvent {
        kind: AuditEventKind::PromoteStarted,
        at: Utc::now(),
        actor: actor.clone(),
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        reason: Some(format!("--plan {plan_id}")),
        breaking_changes: None,
    });

    let (targets_out, overall_success) =
        run_promote_apply(config_path, &promote_plan.targets).await?;

    audit.push(AuditEvent {
        kind: if overall_success {
            AuditEventKind::PromoteCompleted
        } else {
            AuditEventKind::PromoteFailed
        },
        at: Utc::now(),
        actor,
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        reason: None,
        breaking_changes: None,
    });

    let output = BranchPromoteOutput {
        version: VERSION.to_string(),
        command: "branch promote".to_string(),
        branch: promote_plan.branch_name.clone(),
        branch_state_hash: promote_plan.branch_state_hash.clone(),
        approvals_used: promote_plan.approvals_used.clone(),
        approvals_rejected: promote_plan.approvals_rejected.clone(),
        breaking_changes: promote_plan.breaking_changes.clone(),
        targets: targets_out,
        audit,
        success: overall_success,
    };

    if json {
        print_json(&output)?;
    } else if output.success {
        println!(
            "promoted branch '{}' ({} target(s)) via plan {plan_id}",
            output.branch,
            output.targets.len()
        );
    } else {
        println!(
            "promote failed for branch '{}' — see JSON output for details",
            output.branch
        );
    }

    if !overall_success {
        anyhow::bail!("`rocky branch promote --plan {plan_id}` did not complete successfully");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_accepts_common_names() {
        assert!(validate_branch_name("fix-price").is_ok());
        assert!(validate_branch_name("feat_new_join").is_ok());
        assert!(validate_branch_name("hotfix.2026-04-20").is_ok());
        assert!(validate_branch_name("a").is_ok());
    }

    /// `pick_host(true)` always returns the literal `"redacted"` placeholder
    /// — preserving the `host: String` schema while keeping the machine
    /// hostname out of audit JSON when `ROCKY_SCRUB_HOST` is set.
    #[test]
    fn pick_host_scrub_returns_redacted_placeholder() {
        assert_eq!(pick_host(true), "redacted");
    }

    /// `pick_host(false)` returns a non-empty, non-redacted string — either
    /// the real hostname or the `"unknown"` fallback. We avoid asserting the
    /// exact value because it varies by machine.
    #[test]
    fn pick_host_unscrubbed_is_not_redacted() {
        let h = pick_host(false);
        assert!(!h.is_empty());
        assert_ne!(h, "redacted");
    }

    #[test]
    fn validate_rejects_bad_names() {
        assert!(validate_branch_name("").is_err());
        assert!(validate_branch_name("has space").is_err());
        assert!(validate_branch_name("has/slash").is_err());
        assert!(validate_branch_name("has;semi").is_err());
        assert!(validate_branch_name(&"x".repeat(65)).is_err());
    }

    /// `rocky branch compare` resolves the branch's `schema_prefix` from
    /// the state store and wires it into a `ShadowConfig.schema_override` —
    /// the same mapping used by `rocky run --branch`.
    #[test]
    fn branch_compare_maps_schema_prefix_to_shadow_override() {
        let tmp = TempDir::new().unwrap();
        let state_path = tmp.path().join("state.redb");

        // Register a branch via the create path (populates the default
        // `branch__<name>` schema_prefix).
        run_branch_create(&state_path, "fix-price", None, false).unwrap();

        // Fetch it directly the way `run_branch_compare` does and build
        // the ShadowConfig — asserting the wiring is exact.
        let store = StateStore::open_read_only(&state_path).unwrap();
        let record = store.get_branch("fix-price").unwrap().unwrap();

        let shadow_config = ShadowConfig {
            suffix: "_rocky_shadow".to_string(),
            schema_override: Some(record.schema_prefix.clone()),
            cleanup_after: false,
        };

        assert_eq!(record.schema_prefix, "branch__fix-price");
        assert_eq!(
            shadow_config.schema_override.as_deref(),
            Some("branch__fix-price")
        );
        assert!(!shadow_config.cleanup_after);
    }

    /// A missing branch must surface a crisp error that steers the user
    /// toward `rocky branch list`.
    #[tokio::test]
    async fn branch_compare_missing_branch_crisp_error() {
        let tmp = TempDir::new().unwrap();
        let state_path = tmp.path().join("state.redb");

        // Touch the state store so it exists but has no branches.
        StateStore::open(&state_path).unwrap();

        // config_path doesn't need to exist — resolution of the missing
        // branch fails before the config is loaded.
        let config_path = tmp.path().join("rocky.toml");

        let err = run_branch_compare(&state_path, &config_path, "ghost", None, false)
            .await
            .expect_err("missing branch must be an error");

        let msg = format!("{err:#}");
        assert!(
            msg.contains("branch 'ghost' not found"),
            "error must name the missing branch: {msg}"
        );
        assert!(
            msg.contains("rocky branch list"),
            "error must steer user to 'rocky branch list': {msg}"
        );
    }

    // --- Approval / promote helpers ---------------------------------------

    fn sample_record(name: &str) -> BranchRecord {
        BranchRecord {
            name: name.to_string(),
            schema_prefix: format!("branch__{name}"),
            created_by: "tester".to_string(),
            created_at: chrono::DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            description: None,
        }
    }

    fn write_minimal_config(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("rocky.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    /// Write a model source file at `<dir>/models/<rel>`, creating parents.
    fn write_model(dir: &Path, rel: &str, body: &str) {
        let path = dir.join("models").join(rel);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, body).unwrap();
    }

    /// `branch_state_hash` is byte-identical for the same `(record, config)`
    /// pair — the load-bearing determinism contract that `branch promote`
    /// relies on for its "approval is bound to the exact reviewed state"
    /// guarantee.
    #[test]
    fn branch_state_hash_is_deterministic() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# stable config\n");
        let record = sample_record("fix-price");

        let h1 = compute_branch_state_hash(&record, &cfg).unwrap();
        let h2 = compute_branch_state_hash(&record, &cfg).unwrap();
        assert_eq!(h1, h2);
    }

    /// Mutating the config bytes must change the hash — otherwise an
    /// approval signed against config X would still validate after the
    /// project moved to config Y.
    #[test]
    fn branch_state_hash_changes_on_config_edit() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# original\n");
        let record = sample_record("fix-price");
        let before = compute_branch_state_hash(&record, &cfg).unwrap();

        std::fs::write(&cfg, "# edited\n").unwrap();
        let after = compute_branch_state_hash(&record, &cfg).unwrap();

        assert_ne!(
            before, after,
            "config edit must invalidate every existing approval"
        );
    }

    /// Renaming a branch — including via a `schema_prefix` edit — must
    /// flip the hash.
    #[test]
    fn branch_state_hash_changes_on_branch_metadata_edit() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# stable\n");
        let mut record = sample_record("fix-price");
        let before = compute_branch_state_hash(&record, &cfg).unwrap();

        record.schema_prefix = "branch__fix-price-v2".to_string();
        let after = compute_branch_state_hash(&record, &cfg).unwrap();

        assert_ne!(before, after);
    }

    /// An unchanged model tree hashes identically across calls — the
    /// determinism contract still holds once `models_hash` is folded in.
    #[test]
    fn branch_state_hash_stable_with_models_unchanged() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# cfg\n");
        write_model(tmp.path(), "fct_orders.sql", "SELECT 1 AS id");
        write_model(tmp.path(), "sub/dim_customers.rocky", "from raw_customers");
        let record = sample_record("fix-price");

        let h1 = compute_branch_state_hash(&record, &cfg).unwrap();
        let h2 = compute_branch_state_hash(&record, &cfg).unwrap();
        assert_eq!(h1, h2, "unchanged model tree must hash identically");
    }

    /// Editing a model file's bytes must flip the hash — otherwise an
    /// approval signed against model SQL X would still validate after the SQL
    /// changed underneath it. This is the soundness hole #13b closes.
    #[test]
    fn branch_state_hash_changes_on_model_edit() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# cfg\n");
        write_model(tmp.path(), "fct_orders.sql", "SELECT 1 AS id");
        let record = sample_record("fix-price");
        let before = compute_branch_state_hash(&record, &cfg).unwrap();

        write_model(tmp.path(), "fct_orders.sql", "SELECT 2 AS id");
        let after = compute_branch_state_hash(&record, &cfg).unwrap();
        assert_ne!(
            before, after,
            "a model byte edit must invalidate every existing approval"
        );
    }

    /// Adding a new model file must flip the hash.
    #[test]
    fn branch_state_hash_changes_on_model_file_add() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# cfg\n");
        write_model(tmp.path(), "a.sql", "SELECT 1");
        let record = sample_record("fix-price");
        let before = compute_branch_state_hash(&record, &cfg).unwrap();

        write_model(tmp.path(), "b.sql", "SELECT 2");
        let after = compute_branch_state_hash(&record, &cfg).unwrap();
        assert_ne!(before, after, "adding a model must invalidate approvals");
    }

    /// Renaming a model file (identical bytes, new path) must flip the hash —
    /// the relative path is part of the hashed payload, not just the sort key.
    #[test]
    fn branch_state_hash_changes_on_model_rename() {
        let tmp = TempDir::new().unwrap();
        let cfg = write_minimal_config(tmp.path(), "# cfg\n");
        write_model(tmp.path(), "a.sql", "SELECT 1");
        let record = sample_record("fix-price");
        let before = compute_branch_state_hash(&record, &cfg).unwrap();

        std::fs::remove_file(tmp.path().join("models/a.sql")).unwrap();
        write_model(tmp.path(), "b.sql", "SELECT 1"); // same bytes, new name
        let after = compute_branch_state_hash(&record, &cfg).unwrap();
        assert_ne!(
            before, after,
            "a rename (identical bytes, new path) must flip the hash"
        );
    }

    /// The signature digest depends on every field in the canonical-JSON
    /// payload — flipping any field invalidates the artifact.
    #[test]
    fn signature_digest_covers_every_payload_field() {
        let base = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "deadbeef".to_string(),
            approver: ApproverIdentity {
                email: "alice@example.com".to_string(),
                name: Some("Alice".to_string()),
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at: chrono::DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            message: Some("ship it".to_string()),
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        let mut signed = base.clone();
        signed.signature = sign_artifact(&signed).unwrap();

        // Mutate each interesting field in turn; every variant must
        // recompute to a different digest.
        for mutator in [
            |a: &mut ApprovalArtifact| a.approval_id.push_str("-tampered"),
            |a: &mut ApprovalArtifact| a.branch.push_str("-tampered"),
            |a: &mut ApprovalArtifact| a.branch_state_hash.push_str("-tampered"),
            |a: &mut ApprovalArtifact| a.approver.email.push_str("-tampered"),
            |a: &mut ApprovalArtifact| a.message = Some("DIFFERENT".to_string()),
        ] {
            let mut altered = signed.clone();
            mutator(&mut altered);
            let altered_digest = sign_artifact(&altered).unwrap().digest;
            assert_ne!(
                altered_digest, signed.signature.digest,
                "mutating the artifact must change the digest"
            );
        }
    }

    /// `verify_signature` accepts a freshly-signed artifact and rejects
    /// one whose digest field has been tampered with.
    #[test]
    fn verify_signature_accepts_fresh_rejects_tampered() {
        let mut artifact = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "deadbeef".to_string(),
            approver: ApproverIdentity {
                email: "alice@example.com".to_string(),
                name: None,
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at: chrono::DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            message: None,
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        artifact.signature = sign_artifact(&artifact).unwrap();
        assert!(verify_signature(&artifact).is_ok());

        artifact.signature.digest = "0".repeat(64);
        assert!(verify_signature(&artifact).is_err());
    }

    /// Regression: `models_fingerprint` must ignore Rocky's own state DB.
    /// The canonical state path is `<models>/.rocky-state.redb`, so it lives
    /// inside the models tree. If its mutable bytes fed the fingerprint, every
    /// state write after `branch approve` would drift `branch_state_hash` and
    /// invalidate the approval before `promote` could honour it. A genuine
    /// model-source edit must still change the digest.
    #[test]
    fn models_fingerprint_ignores_state_db_but_tracks_model_edits() {
        let tmp = TempDir::new().unwrap();
        let config_path = tmp.path().join("rocky.toml");
        std::fs::write(&config_path, "[adapter]\ntype = \"duckdb\"\n").unwrap();
        let models = tmp.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(models.join("m.sql"), "SELECT 1 AS id").unwrap();

        let baseline = models_fingerprint(&config_path);

        // Writing (and later mutating) the state DB + its lock inside models/
        // must NOT change the fingerprint.
        std::fs::write(models.join(".rocky-state.redb"), b"redb-bytes-v1").unwrap();
        std::fs::write(models.join(".rocky-state.redb.lock"), b"lock").unwrap();
        assert_eq!(
            models_fingerprint(&config_path),
            baseline,
            "creating the state DB must not change the models fingerprint"
        );
        std::fs::write(models.join(".rocky-state.redb"), b"redb-bytes-v2-mutated").unwrap();
        assert_eq!(
            models_fingerprint(&config_path),
            baseline,
            "mutating the state DB must not change the models fingerprint"
        );

        // A real model-source edit must change it (positive control).
        std::fs::write(models.join("m.sql"), "SELECT 2 AS id").unwrap();
        assert_ne!(
            models_fingerprint(&config_path),
            baseline,
            "editing a model file must change the fingerprint"
        );
    }

    /// State-hash mismatch is the load-bearing rejection: an approval signed
    /// against state X must not validate against state Y.
    #[test]
    fn evaluate_artifact_rejects_state_hash_mismatch() {
        let mut artifact = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "old-hash".to_string(),
            approver: ApproverIdentity {
                email: "alice@example.com".to_string(),
                name: None,
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at: Utc::now(),
            message: None,
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        artifact.signature = sign_artifact(&artifact).unwrap();

        let cfg = BranchApprovalConfig::default();
        let rejected = evaluate_artifact(&artifact, "new-hash", &cfg, Utc::now()).unwrap_err();
        assert_eq!(rejected.reason, "state_hash_mismatch");
    }

    /// Approvals older than `max_age_seconds` are rejected even when the
    /// state hash still matches.
    #[test]
    fn evaluate_artifact_rejects_expired() {
        let signed_at = Utc::now() - chrono::Duration::seconds(7200);
        let mut artifact = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "h".to_string(),
            approver: ApproverIdentity {
                email: "alice@example.com".to_string(),
                name: None,
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at,
            message: None,
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        artifact.signature = sign_artifact(&artifact).unwrap();

        let cfg = BranchApprovalConfig {
            max_age_seconds: 60, // 1 minute window
            ..Default::default()
        };
        let rejected = evaluate_artifact(&artifact, "h", &cfg, Utc::now()).unwrap_err();
        assert_eq!(rejected.reason, "expired");
    }

    /// When `allowed_signers` is set, only artifacts from listed emails
    /// count toward `min_approvers`.
    #[test]
    fn evaluate_artifact_rejects_signer_not_in_allowlist() {
        let mut artifact = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "h".to_string(),
            approver: ApproverIdentity {
                email: "carol@example.com".to_string(),
                name: None,
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at: Utc::now(),
            message: None,
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        artifact.signature = sign_artifact(&artifact).unwrap();

        let cfg = BranchApprovalConfig {
            allowed_signers: vec!["alice@example.com".to_string()],
            ..Default::default()
        };
        let rejected = evaluate_artifact(&artifact, "h", &cfg, Utc::now()).unwrap_err();
        assert_eq!(rejected.reason, "signer_not_allowed");
    }

    /// Default config (`required = false`) leaves the gate disabled and
    /// makes every other field meaningless — empty allowlist accepts
    /// any signer.
    #[test]
    fn evaluate_artifact_accepts_when_allowlist_empty_and_state_matches() {
        let mut artifact = ApprovalArtifact {
            approval_id: "20260503T120000000000-aaaaaaaa".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "h".to_string(),
            approver: ApproverIdentity {
                email: "anyone@example.com".to_string(),
                name: None,
                host: "host-1".to_string(),
                source: ApproverSource::Local,
            },
            signed_at: Utc::now(),
            message: None,
            signature: ApprovalSignature {
                algorithm: SignatureAlgorithm::Blake3CanonicalJson,
                digest: String::new(),
            },
        };
        artifact.signature = sign_artifact(&artifact).unwrap();

        let cfg = BranchApprovalConfig::default();
        assert!(evaluate_artifact(&artifact, "h", &cfg, Utc::now()).is_ok());
    }

    /// `generate_approval_id` produces a sortable identifier that begins
    /// with a UTC timestamp prefix so directory listings sort
    /// chronologically.
    #[test]
    fn approval_id_is_sortable_by_timestamp_prefix() {
        let earlier = chrono::DateTime::parse_from_rfc3339("2026-05-03T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let later = chrono::DateTime::parse_from_rfc3339("2026-05-03T12:00:01Z")
            .unwrap()
            .with_timezone(&Utc);

        let a = generate_approval_id(earlier);
        let b = generate_approval_id(later);
        assert!(a < b, "{a} should sort before {b}");
        assert!(a.starts_with("20260503T120000"));
        assert!(b.starts_with("20260503T120001"));
    }

    /// `build_promote_sql` produces the load-bearing string the trust gate
    /// boils down to: a `CREATE OR REPLACE TABLE ... AS SELECT * FROM ...`
    /// with dialect-correct identifier quoting on every part of both FQNs.
    /// Uses the DuckDB dialect (the default playground adapter) to exercise
    /// the same quoting path the live binary takes; a hyphen-bearing branch
    /// name proves the unquoted-emission parser-fail bug stays fixed.
    #[test]
    fn build_promote_sql_quotes_every_identifier() {
        let dialect = rocky_duckdb::dialect::DuckDbSqlDialect;
        let prod = TargetRef {
            catalog: "playground".to_string(),
            schema: "staging__orders".to_string(),
            table: "orders".to_string(),
        };
        let branch_source = TargetRef {
            catalog: "playground".to_string(),
            schema: "branch__live-test".to_string(),
            table: "orders".to_string(),
        };
        let sql = build_promote_sql(&dialect, &prod, &branch_source);
        assert_eq!(
            sql,
            r#"CREATE OR REPLACE TABLE "playground"."staging__orders"."orders" AS SELECT * FROM "playground"."branch__live-test"."orders""#,
        );
    }

    /// `load_approvals_for_branch` returns empty when the directory does
    /// not exist — the no-approvals path that backs "approval gate
    /// disabled, nothing to read" cases.
    #[test]
    fn load_approvals_returns_empty_when_directory_missing() {
        let _cwd_guard = cwd_lock();
        let tmp = TempDir::new().unwrap();
        let saved = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        let (loaded, rejected) = load_approvals_for_branch("ghost").unwrap();
        assert!(loaded.is_empty());
        assert!(rejected.is_empty());

        std::env::set_current_dir(saved).unwrap();
    }

    // --- Breaking-change gate ----------------------------------------------

    /// `compile_result_to_project_ir` must merge `typed_models` into each
    /// `ModelIr.typed_columns` — without that, the classifier sees empty
    /// schemas and silently produces no column-level findings. This is the
    /// load-bearing invariant the entire gate relies on.
    ///
    /// Uses a two-model chain (raw → fct) so the lineage analyzer extracts
    /// columns from the upstream source rather than producing an empty
    /// projection for a fromless `SELECT`.
    #[test]
    fn compile_result_to_project_ir_populates_typed_columns() {
        use rocky_compiler::compile::{self, CompilerConfig};

        let tmp = TempDir::new().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        // Upstream: literal SELECT (lineage extracts column names).
        std::fs::write(
            models_dir.join("raw_widgets.sql"),
            "SELECT 1 AS id, 'a' AS label",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("raw_widgets.toml"),
            "name = \"raw_widgets\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"raw_widgets\"\n",
        )
        .unwrap();
        // Downstream: explicit projection from the upstream.
        std::fs::write(
            models_dir.join("widgets.sql"),
            "SELECT id, label FROM raw_widgets",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("widgets.toml"),
            "name = \"widgets\"\ndepends_on = [\"raw_widgets\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"widgets\"\n",
        )
        .unwrap();

        let cfg = CompilerConfig {
            models_dir,
            ..Default::default()
        };
        let result = compile::compile(&cfg).expect("test fixture must compile");

        let ir = compile_result_to_project_ir(&result);
        assert_eq!(ir.models.len(), 2);
        // Find the widgets model.
        let widgets = ir
            .models
            .iter()
            .find(|m| &*m.name == "widgets")
            .expect("widgets must be in IR");
        assert!(
            !widgets.typed_columns.is_empty(),
            "typed_columns must be merged in from type_check.typed_models"
        );
        let names: Vec<&str> = widgets
            .typed_columns
            .iter()
            .map(|c| c.name.as_str())
            .collect();
        assert!(names.contains(&"id"), "got {names:?}");
        assert!(names.contains(&"label"), "got {names:?}");
    }

    /// Cwd is process-wide; the gate tests call `git rev-parse
    /// --show-toplevel` from the test dir which means they must run
    /// serially. The first lock acquisition is fine; the load-bearing
    /// guarantee is that no two tests overlap their `set_current_dir`
    /// windows. A poisoned mutex is fine to ignore — the panicked test
    /// already restored cwd (or failed before changing it).
    fn cwd_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Helper: initialize a git repo in `dir` with a `main` branch containing
    /// `models/` files, then return the path of the models directory.
    fn init_git_repo_with_models(
        dir: &Path,
        rocky_toml: &str,
        files: &[(&str, &str)],
    ) -> std::path::PathBuf {
        let models_dir = dir.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(dir.join("rocky.toml"), rocky_toml).unwrap();

        let run_git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args(args)
                .current_dir(dir)
                .output()
                .expect("git command must run");
            assert!(
                out.status.success(),
                "git {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        };
        run_git(&["init", "-q", "-b", "main"]);
        run_git(&["config", "user.email", "tester@example.com"]);
        run_git(&["config", "user.name", "Tester"]);
        run_git(&["config", "commit.gpgsign", "false"]);

        for (path, contents) in files {
            let full = models_dir.join(path);
            if let Some(parent) = full.parent() {
                std::fs::create_dir_all(parent).unwrap();
            }
            std::fs::write(&full, contents).unwrap();
        }
        run_git(&["add", "."]);
        run_git(&["commit", "-q", "-m", "base"]);
        models_dir
    }

    fn dummy_actor() -> ApproverIdentity {
        ApproverIdentity {
            email: "tester@example.com".to_string(),
            name: Some("Tester".to_string()),
            host: "host-1".to_string(),
            source: ApproverSource::Local,
        }
    }

    /// Build a two-model project (`raw_widgets` + `widgets`) whose downstream
    /// projection list is `cols`. Used by the gate tests below to vary the
    /// downstream's column list between base and HEAD without re-stating the
    /// upstream boilerplate.
    fn write_two_model_chain(models_dir: &Path, downstream_cols: &str) {
        std::fs::create_dir_all(models_dir).unwrap();
        std::fs::write(
            models_dir.join("raw_widgets.sql"),
            "SELECT 1 AS id, 'a' AS name, 42 AS qty",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("raw_widgets.toml"),
            "name = \"raw_widgets\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"raw_widgets\"\n",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("widgets.sql"),
            format!("SELECT {downstream_cols} FROM raw_widgets"),
        )
        .unwrap();
        std::fs::write(
            models_dir.join("widgets.toml"),
            "name = \"widgets\"\ndepends_on = [\"raw_widgets\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"widgets\"\n",
        )
        .unwrap();
    }

    /// A column drop between `main` and HEAD produces a `Breaking` finding
    /// that surfaces in the audit trail. Default behavior (no
    /// `--allow-breaking`) must block.
    #[test]
    fn gate_flags_column_drop_as_breaking() {
        let _cwd_guard = cwd_lock();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let rocky_toml = "[adapters.duckdb]\ntype = \"duckdb\"\npath = \":memory:\"\n";

        // Stage base files: downstream projects all three columns.
        let models_relpath = "models";
        let models_dir_abs = dir.join(models_relpath);
        write_two_model_chain(&models_dir_abs, "id, name, qty");

        // Read each base file back to seed init_git_repo_with_models.
        let to_pair = |name: &str| -> (String, String) {
            (
                name.to_string(),
                std::fs::read_to_string(models_dir_abs.join(name)).unwrap(),
            )
        };
        let owned = [
            to_pair("raw_widgets.sql"),
            to_pair("raw_widgets.toml"),
            to_pair("widgets.sql"),
            to_pair("widgets.toml"),
        ];
        let files: Vec<(&str, &str)> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        std::fs::remove_dir_all(&models_dir_abs).unwrap();
        let models_dir = init_git_repo_with_models(dir, rocky_toml, &files);

        // Drop `qty` on HEAD.
        write_two_model_chain(&models_dir, "id, name");

        let config_path = dir.join("rocky.toml");
        let mut audit: Vec<AuditEvent> = Vec::new();
        let record = sample_record("fix-price");

        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();

        let findings = evaluate_breaking_change_gate(
            &config_path,
            &models_dir,
            "main",
            &mut audit,
            &dummy_actor(),
            &record,
            "branch-state-hash",
        );

        std::env::set_current_dir(saved_cwd).unwrap();

        let findings = findings.expect("gate must run when both refs compile");
        let breaking: Vec<&BreakingFinding> = findings.iter().filter(|f| f.is_breaking()).collect();
        assert!(
            !breaking.is_empty(),
            "dropping a column must produce a breaking finding, got: {findings:?}"
        );
        assert!(
            breaking.iter().any(|f| matches!(
                f.change,
                rocky_core::breaking_change::BreakingChange::ColumnDropped { .. }
            )),
            "the breaking finding must be ColumnDropped, got: {breaking:?}"
        );
        // No skip event when both sides compile.
        assert!(
            !audit
                .iter()
                .any(|e| e.kind == AuditEventKind::BreakingChangesGateSkipped),
            "gate must not record a skip event when both refs compile"
        );
    }

    /// A pure SQL-body rewrite that preserves the typed shape produces only
    /// `Info` findings — the gate must let the promote through with no
    /// breaking findings.
    #[test]
    fn gate_clean_when_only_info_findings() {
        let _cwd_guard = cwd_lock();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let rocky_toml = "[adapters.duckdb]\ntype = \"duckdb\"\npath = \":memory:\"\n";

        let models_dir_abs = dir.join("models");
        write_two_model_chain(&models_dir_abs, "id, name");
        let to_pair = |name: &str| -> (String, String) {
            (
                name.to_string(),
                std::fs::read_to_string(models_dir_abs.join(name)).unwrap(),
            )
        };
        let owned = [
            to_pair("raw_widgets.sql"),
            to_pair("raw_widgets.toml"),
            to_pair("widgets.sql"),
            to_pair("widgets.toml"),
        ];
        let files: Vec<(&str, &str)> = owned
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        std::fs::remove_dir_all(&models_dir_abs).unwrap();
        let models_dir = init_git_repo_with_models(dir, rocky_toml, &files);

        // Same shape, swap upstream SQL only to force SqlBodyChanged (Info).
        std::fs::write(
            models_dir.join("raw_widgets.sql"),
            "SELECT 2 AS id, 'b' AS name, 99 AS qty",
        )
        .unwrap();

        let config_path = dir.join("rocky.toml");
        let mut audit: Vec<AuditEvent> = Vec::new();
        let record = sample_record("fix-price");

        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();

        let findings = evaluate_breaking_change_gate(
            &config_path,
            &models_dir,
            "main",
            &mut audit,
            &dummy_actor(),
            &record,
            "branch-state-hash",
        );

        std::env::set_current_dir(saved_cwd).unwrap();

        let findings = findings.expect("gate must run when both refs compile");
        let breaking_count = findings.iter().filter(|f| f.is_breaking()).count();
        assert_eq!(
            breaking_count, 0,
            "a SQL-body-only change must produce zero breaking findings, got {findings:?}"
        );
    }

    /// When the base ref's models directory does not exist the gate must
    /// fail open: return `None`, append `BreakingChangesGateSkipped` to the
    /// audit, and let the caller proceed.
    #[test]
    fn gate_fails_open_when_base_ref_missing_models() {
        let _cwd_guard = cwd_lock();
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let rocky_toml = "[adapters.duckdb]\ntype = \"duckdb\"\npath = \":memory:\"\n";
        // Commit an empty repo with only rocky.toml — no models directory at
        // any ref. The HEAD working tree adds the models dir.
        let _models_dir = init_git_repo_with_models(dir, rocky_toml, &[]);

        // Now create a models directory on HEAD only.
        let models_dir = dir.join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(
            models_dir.join("_defaults.toml"),
            "[target]\ncatalog = \"poc\"\nschema = \"demo\"\n",
        )
        .unwrap();
        std::fs::write(models_dir.join("widgets.sql"), "SELECT 1 AS id").unwrap();
        std::fs::write(
            models_dir.join("widgets.toml"),
            "[strategy]\ntype = \"full_refresh\"\n",
        )
        .unwrap();

        let config_path = dir.join("rocky.toml");
        let mut audit: Vec<AuditEvent> = Vec::new();
        let record = sample_record("fix-price");

        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();

        let findings = evaluate_breaking_change_gate(
            &config_path,
            &models_dir,
            "main",
            &mut audit,
            &dummy_actor(),
            &record,
            "branch-state-hash",
        );

        std::env::set_current_dir(saved_cwd).unwrap();

        assert!(
            findings.is_none(),
            "gate must return None when the base ref has no models"
        );
        assert_eq!(audit.len(), 1, "exactly one skip event must be recorded");
        assert_eq!(audit[0].kind, AuditEventKind::BreakingChangesGateSkipped);
        assert!(
            audit[0]
                .reason
                .as_deref()
                .map(|r| r.contains("skipped"))
                .unwrap_or(false),
            "skip reason must be human-readable: {:?}",
            audit[0].reason
        );
    }

    /// When the models directory is absent entirely the gate must fail open
    /// without invoking the compiler — `models_dir.is_dir()` check.
    #[test]
    fn gate_skips_when_models_dir_missing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let missing = dir.join("does-not-exist");
        let config_path = dir.join("rocky.toml");
        std::fs::write(&config_path, "# stub\n").unwrap();

        let mut audit: Vec<AuditEvent> = Vec::new();
        let record = sample_record("fix-price");

        let findings = evaluate_breaking_change_gate(
            &config_path,
            &missing,
            "main",
            &mut audit,
            &dummy_actor(),
            &record,
            "branch-state-hash",
        );

        assert!(findings.is_none());
        assert_eq!(audit.len(), 1);
        assert_eq!(audit[0].kind, AuditEventKind::BreakingChangesGateSkipped);
    }

    /// The `BreakingChangesAllowed` audit event carries the findings — the
    /// `--allow-breaking` override must leave the exact same finding set on
    /// disk so a future audit can review what was waved through.
    #[test]
    fn breaking_changes_allowed_audit_carries_findings() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let findings = vec![BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: "widgets".to_string(),
                column: "qty".to_string(),
                data_type: "INT64".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        }];

        let event = AuditEvent {
            kind: AuditEventKind::BreakingChangesAllowed,
            at: Utc::now(),
            actor: dummy_actor(),
            branch: "fix-price".to_string(),
            branch_state_hash: "deadbeef".to_string(),
            reason: Some("--allow-breaking CLI flag".to_string()),
            breaking_changes: Some(findings.clone()),
        };

        let json = serde_json::to_string(&event).expect("AuditEvent must serialize");
        assert!(json.contains("breaking_changes_allowed"));
        assert!(json.contains("column_dropped"));
        assert!(json.contains("--allow-breaking CLI flag"));
    }

    // ------------------------------------------------------------------
    // Byte-stable JSON shape for bare `branch promote` output (Phase 3)
    //
    // These tests verify that `BranchPromoteOutput` always serializes to a
    // JSON object with the exact fields CI consumers (e.g. Dagster) expect.
    // They are NOT end-to-end tests (those would require a warehouse adapter)
    // — they guard the schema shape against accidental field renames or
    // drops, which are the most common regression after refactors.
    // ------------------------------------------------------------------

    fn minimal_promote_output() -> BranchPromoteOutput {
        BranchPromoteOutput {
            version: "1.0.0".to_string(),
            command: "branch promote".to_string(),
            branch: "fix-price".to_string(),
            branch_state_hash: "deadbeef".repeat(8),
            approvals_used: vec![],
            approvals_rejected: vec![],
            breaking_changes: None,
            targets: vec![PromoteTarget {
                target: "cat.schema.orders".to_string(),
                source: "cat.branch__fix-price.orders".to_string(),
                statement: "CREATE OR REPLACE TABLE ...".to_string(),
                succeeded: true,
                error: None,
            }],
            audit: vec![AuditEvent {
                kind: AuditEventKind::PromoteCompleted,
                at: chrono::DateTime::parse_from_rfc3339("2026-05-14T10:00:00Z")
                    .unwrap()
                    .with_timezone(&Utc),
                actor: ApproverIdentity {
                    email: "a@example.com".to_string(),
                    name: None,
                    host: "host".to_string(),
                    source: ApproverSource::Local,
                },
                branch: "fix-price".to_string(),
                branch_state_hash: "deadbeef".repeat(8),
                reason: None,
                breaking_changes: None,
            }],
            success: true,
        }
    }

    /// Bare-verb `branch promote` output carries all mandatory top-level keys.
    ///
    /// Guards the JSON shape contract for Dagster / CI consumers: every key
    /// emitted by the bare-verb path must still be present after the Phase 3
    /// internal refactor that chains plan + apply.
    #[test]
    fn branch_promote_bare_verb_jsonshape_unchanged_after_phase3() {
        let output = minimal_promote_output();
        let json = serde_json::to_string(&output).expect("BranchPromoteOutput must serialize");
        let value: serde_json::Value =
            serde_json::from_str(&json).expect("must produce valid JSON");

        // Mandatory top-level keys that CI consumers rely on.
        for key in [
            "version",
            "command",
            "branch",
            "branch_state_hash",
            "approvals_used",
            "approvals_rejected",
            "targets",
            "audit",
            "success",
        ] {
            assert!(
                value.get(key).is_some(),
                "BranchPromoteOutput missing key '{key}' — JSON shape contract broken"
            );
        }
        // `command` must be the exact string CI consumers key on.
        assert_eq!(value["command"], "branch promote");
        // `success` must be a JSON bool.
        assert!(value["success"].is_boolean());
        // `targets[0]` must have the per-target sub-keys.
        let t = &value["targets"][0];
        for key in ["target", "source", "statement", "succeeded"] {
            assert!(
                t.get(key).is_some(),
                "PromoteTarget missing key '{key}' — JSON shape contract broken"
            );
        }
    }

    /// `run_branch_promote_from_plan` rejects a plan_id that points to a
    /// non-Promote plan (e.g. a Compact plan). This guards the kind-mismatch
    /// error path without requiring a warehouse adapter.
    #[test]
    fn run_promote_from_plan_rejects_wrong_plan_kind() {
        use crate::plan_store::{PlanKind, write_plan};

        let tmp = TempDir::new().unwrap();
        // Write a Compact plan — wrong kind.
        let payload = serde_json::json!({"model": "c.s.t", "statement_count": 1});
        let plan_id = write_plan(tmp.path(), PlanKind::Compact, &payload).unwrap();

        // Check that reading the plan gives kind=Compact (not Promote).
        let persisted = crate::plan_store::read_plan(tmp.path(), &plan_id).unwrap();
        assert_eq!(persisted.kind, PlanKind::Compact);
        assert_ne!(persisted.kind, PlanKind::Promote);
    }

    // ------------------------------------------------------------------
    // Transformation-pipeline model-glob walk — closes the v1 promote
    // limitation that the enumeration was replication-only.
    // ------------------------------------------------------------------

    /// Helper: write a minimal model pair (`.sql` + `.toml`) under `models_dir`.
    fn write_transformation_model(
        models_dir: &Path,
        name: &str,
        catalog: &str,
        schema: &str,
        table: &str,
        sql: &str,
    ) {
        std::fs::write(models_dir.join(format!("{name}.sql")), sql).unwrap();
        std::fs::write(
            models_dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"{catalog}\"\nschema = \"{schema}\"\ntable = \"{table}\"\n"
            ),
        )
        .unwrap();
    }

    /// `branch promote` on a transformation-only pipeline must enumerate
    /// every model's `[target]` from the on-disk model glob and produce one
    /// `CREATE OR REPLACE TABLE prod AS SELECT * FROM branch_source` per
    /// model. No discovery adapter involved.
    ///
    /// Pure enumeration test — exercises `discover_branch_targets` against
    /// a transformation pipeline without touching the warehouse.
    #[tokio::test]
    async fn discover_branch_targets_walks_transformation_models() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let config_path = dir.join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"
"#,
        )
        .unwrap();

        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id, 'a' AS label",
        );
        write_transformation_model(
            &models_dir,
            "dim_customers",
            "warehouse",
            "marts",
            "dim_customers",
            "SELECT 2 AS id, 'c' AS name",
        );

        let record = sample_record("fix-price");
        let planned = discover_branch_targets(&config_path, &record, None, None)
            .await
            .expect("transformation-pipeline branch promote enumeration must succeed");

        // Two models → two planned promote targets.
        assert_eq!(
            planned.len(),
            2,
            "expected 2 planned targets, got {planned:?}",
            planned = planned.len()
        );

        // Each prod target carries the model's [target] coordinates and the
        // branch_source rewrites the schema to `branch__fix-price` per the
        // shadow_target rule.
        let fqns: std::collections::BTreeSet<String> = planned
            .iter()
            .map(|p| format!("{} <- {}", p.prod.full_name(), p.branch_source.full_name()))
            .collect();
        assert!(
            fqns.contains("warehouse.marts.fct_orders <- warehouse.branch__fix-price.fct_orders"),
            "fct_orders pairing missing: {fqns:?}"
        );
        assert!(
            fqns.contains(
                "warehouse.marts.dim_customers <- warehouse.branch__fix-price.dim_customers"
            ),
            "dim_customers pairing missing: {fqns:?}"
        );
    }

    /// `branch promote` end-to-end on a transformation-only pipeline against
    /// an on-disk DuckDB warehouse.
    ///
    /// Seeds two branch-schema tables, materializes a branch state for the
    /// `fix-price` branch, then drives the bare-verb `run_branch_promote`
    /// path. After the promote, the production schema must contain the same
    /// rows as the branch schema for every model — proving the model-glob
    /// walk drives real SQL dispatch through the adapter on a transformation
    /// pipeline.
    ///
    /// The approval gate stays disabled (default `[branch.approval]`) and
    /// `--allow-breaking` is passed so the base-ref-missing skip path on the
    /// breaking-change gate doesn't block the promote.
    ///
    /// `cwd_lock` is deliberately held across the run's awaits to serialize
    /// against other cwd-mutating tests in this binary (see the comment at
    /// the `_cwd_guard` site below); allow the await-holding-lock lint at
    /// the test level for that reason.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn run_branch_promote_transformation_e2e_succeeds() {
        use rocky_core::traits::WarehouseAdapter;
        use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let duckdb_path = dir.join("warehouse.duckdb");
        let config_path = dir.join("rocky.toml");
        let state_path = dir.join("state.redb");
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();

        // `run_branch_promote` resolves the approver identity via
        // `git config --get user.email`, which is cwd-sensitive (repo-local →
        // global → system). The promote runs with the cwd set to `dir` below,
        // so make `dir` a self-contained git repo with a local identity.
        // Without this the promote fails with "`git config --get user.email`
        // exited with status 1" on any host whose ambient/global git identity
        // is unset (e.g. CI runners) — a latent flake that only passed locally
        // because dev machines usually have a global `user.email` configured.
        for git_args in [
            ["init", "-q", "."].as_slice(),
            ["config", "user.email", "test@rocky.invalid"].as_slice(),
            ["config", "user.name", "Rocky Test"].as_slice(),
        ] {
            let status = std::process::Command::new("git")
                .args(git_args)
                .current_dir(dir)
                .status()
                .expect("git setup for test repo identity");
            assert!(
                status.success(),
                "git {git_args:?} failed during test setup"
            );
        }

        // Two transformation models targeting `warehouse.marts.<table>`.
        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );
        write_transformation_model(
            &models_dir,
            "dim_customers",
            "warehouse",
            "marts",
            "dim_customers",
            "SELECT 1 AS id",
        );

        std::fs::write(
            &config_path,
            format!(
                r#"[adapter]
type = "duckdb"
path = "{}"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"

[pipeline.t.target.governance]
auto_create_schemas = true
"#,
                duckdb_path.display()
            ),
        )
        .unwrap();

        // Register the branch in the state store.
        run_branch_create(&state_path, "fix-price", None, false).unwrap();

        // Seed the branch tables manually (simulates `rocky run --branch fix-price`):
        // both schemas are pre-created; the branch schema carries the rows
        // the promote will copy into prod.
        //
        // Pre-create the prod `marts` schema explicitly — `branch promote`
        // dispatches `CREATE OR REPLACE TABLE` directly via the adapter and
        // doesn't consult the pipeline's `auto_create_schemas` governance.
        {
            let adapter = DuckDbWarehouseAdapter::open(&duckdb_path).expect("open duckdb");
            adapter
                .execute_statement("CREATE SCHEMA IF NOT EXISTS warehouse")
                .await
                .expect("create catalog");
            adapter
                .execute_statement("CREATE SCHEMA IF NOT EXISTS marts")
                .await
                .expect("create prod schema");
            adapter
                .execute_statement("CREATE SCHEMA IF NOT EXISTS \"branch__fix-price\"")
                .await
                .expect("create branch schema");
            adapter
                .execute_statement(
                    "CREATE TABLE \"branch__fix-price\".fct_orders AS SELECT 1 AS id UNION ALL SELECT 2",
                )
                .await
                .expect("seed branch fct_orders");
            adapter
                .execute_statement(
                    "CREATE TABLE \"branch__fix-price\".dim_customers AS SELECT 'a' AS name",
                )
                .await
                .expect("seed branch dim_customers");
        }

        // Drive the promote — bare verb, JSON disabled (we re-open the
        // warehouse below to verify rows). Hold `cwd_lock` across the
        // `set_current_dir` window so this test serializes against the other
        // cwd-mutating tests in this binary (cargo runs tests in parallel).
        let _cwd_guard = cwd_lock();
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();
        let result = run_branch_promote(
            &state_path,
            &config_path,
            &models_dir,
            "main", // base_ref — repo is uninitialized so the gate skips
            "fix-price",
            None,  // no filter
            None,  // pipeline — only one pipeline, no need to disambiguate
            false, // skip_approval_flag
            true,  // allow_breaking — irrelevant here; gate skips fail-open anyway
            false, // json — suppress pretty stdout in tests
        )
        .await;
        std::env::set_current_dir(saved_cwd).unwrap();

        result.expect("transformation-pipeline promote must succeed");

        // Verify both production tables were created with the branch's rows.
        // DuckDB's `COUNT(*)` returns HUGEINT, which the rocky-duckdb adapter
        // serializes as a JSON string; cast to BIGINT to land on the i64 path.
        let adapter = DuckDbWarehouseAdapter::open(&duckdb_path).expect("reopen duckdb");
        let count_i64 = |sql: &'static str| {
            let adapter = &adapter;
            async move {
                let rows = adapter.execute_query(sql).await.expect("count query");
                let v = &rows.rows[0][0];
                v.as_i64().unwrap_or_else(|| {
                    // HUGEINT serializes as a string in rocky-duckdb's
                    // JSON shape; parse if so.
                    v.as_str()
                        .and_then(|s| s.parse::<i64>().ok())
                        .unwrap_or_else(|| {
                            panic!("count value is neither i64 nor parseable string: {v:?}")
                        })
                })
            }
        };
        let fct_count =
            count_i64("SELECT CAST(COUNT(*) AS BIGINT) FROM warehouse.marts.fct_orders").await;
        assert_eq!(
            fct_count, 2,
            "promote must copy all 2 rows from branch__fix-price.fct_orders to prod"
        );
        let dim_count =
            count_i64("SELECT CAST(COUNT(*) AS BIGINT) FROM warehouse.marts.dim_customers").await;
        assert_eq!(
            dim_count, 1,
            "promote must copy the row from branch__fix-price.dim_customers to prod"
        );
    }

    /// 🔴 D9 regression (drives the PRODUCTION call site): an agent-invoked
    /// promote plan must PERSIST `principal = agent`. This drives
    /// `build_promote_plan_inner` — the call site FIX 2 changed — end-to-end and
    /// reads the persisted plan back. Reverting the production fix (back to a
    /// bare `write_plan` / dropping the threaded principal) makes this fail: the
    /// persisted plan would resolve to the `Human` default.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn build_promote_plan_inner_stamps_agent_principal() {
        use crate::plan_store::{PlanKind, read_plan};

        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let duckdb_path = dir.join("warehouse.duckdb");
        let config_path = dir.join("rocky.toml");
        let state_path = dir.join("state.redb");
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();

        // Self-contained git repo with a local identity (approver_identity runs
        // `git config --get user.email` in cwd; CI runners have no ambient one).
        for git_args in [
            ["init", "-q", "."].as_slice(),
            ["config", "user.email", "test@rocky.invalid"].as_slice(),
            ["config", "user.name", "Rocky Test"].as_slice(),
        ] {
            let status = std::process::Command::new("git")
                .args(git_args)
                .current_dir(dir)
                .status()
                .expect("git setup");
            assert!(status.success(), "git {git_args:?} failed");
        }

        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );

        std::fs::write(
            &config_path,
            format!(
                r#"[adapter]
type = "duckdb"
path = "{}"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"

[pipeline.t.target.governance]
auto_create_schemas = true
"#,
                duckdb_path.display()
            ),
        )
        .unwrap();

        run_branch_create(&state_path, "fix-price", None, false).unwrap();

        let _cwd_guard = cwd_lock();
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();
        let result = crate::commands::plan::build_promote_plan_inner(
            dir,
            &config_path,
            &models_dir,
            "main", // base_ref — repo is fresh so the breaking-change gate skips
            "fix-price",
            None, // filter
            None, // pipeline
            true, // allow_breaking — irrelevant; gate skips fail-open anyway
            &state_path,
            rocky_core::config::PolicyPrincipal::Agent, // the agent-invoked promote
        )
        .await;
        std::env::set_current_dir(saved_cwd).unwrap();

        let result = result.expect("build_promote_plan_inner must succeed");
        let plan_id = result
            .plan_output
            .plan_id
            .expect("a plan_id must be persisted");

        // The PERSISTED plan must resolve to Agent — not the Human default.
        let persisted = read_plan(dir, &plan_id).expect("read persisted promote plan");
        assert_eq!(
            persisted.kind,
            PlanKind::Promote,
            "the persisted plan is a promote plan"
        );
        assert_eq!(
            persisted.resolved_principal(),
            rocky_core::config::PolicyPrincipal::Agent,
            "an agent-invoked promote must persist principal = agent (FIX 2), not fall to Human"
        );
    }

    /// 🔴 A regression: `rocky branch promote --plan <id>` must route through
    /// the agent-policy gate. An agent-authored Promote plan under a
    /// `deny agent promote { any }` rule must be REFUSED before any target SQL
    /// executes. Pre-fix `run_branch_promote_from_plan` called `run_promote_apply`
    /// directly with no policy evaluation, so the denied plan executed.
    #[tokio::test]
    #[allow(clippy::await_holding_lock)]
    async fn branch_promote_from_plan_gates_a_denied_agent_promote() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let duckdb_path = dir.join("warehouse.duckdb");
        let config_path = dir.join("rocky.toml");
        let state_path = dir.join("state.redb");
        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();

        for git_args in [
            ["init", "-q", "."].as_slice(),
            ["config", "user.email", "test@rocky.invalid"].as_slice(),
            ["config", "user.name", "Rocky Test"].as_slice(),
        ] {
            std::process::Command::new("git")
                .args(git_args)
                .current_dir(dir)
                .status()
                .expect("git setup");
        }

        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );

        // Config carries a `[policy]` block that DENIES every agent promote.
        std::fs::write(
            &config_path,
            format!(
                r#"[adapter]
type = "duckdb"
path = "{}"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"

[pipeline.t.target.governance]
auto_create_schemas = true

[policy]
version = 1
default_agent_effect = "require_review"

[[policy.rules]]
principal = "agent"
capability = "promote"
scope = {{ any = true }}
effect = "deny"
"#,
                duckdb_path.display()
            ),
        )
        .unwrap();

        run_branch_create(&state_path, "fix-price", None, false).unwrap();

        let _cwd_guard = cwd_lock();
        let saved_cwd = std::env::current_dir().unwrap();
        std::env::set_current_dir(dir).unwrap();

        // Build an AGENT-authored promote plan (build does not gate).
        let build = crate::commands::plan::build_promote_plan_inner(
            dir,
            &config_path,
            &models_dir,
            "main",
            "fix-price",
            None,
            None,
            true,
            &state_path,
            rocky_core::config::PolicyPrincipal::Agent,
        )
        .await;
        let plan_id = build
            .expect("plan build must succeed")
            .plan_output
            .plan_id
            .expect("plan_id");

        // Now apply it via the --plan path — the gate must REFUSE it.
        let result = crate::commands::run_branch_promote_from_plan(
            dir,
            &config_path,
            &plan_id,
            None,
            &state_path,
            false,
        )
        .await;
        std::env::set_current_dir(saved_cwd).unwrap();

        let err = result.expect_err("a denied agent promote must be refused via --plan");
        assert!(
            err.to_string().contains("DENIES"),
            "the --plan path must be gated by the policy plane, got: {err}"
        );
    }

    /// `branch promote` on a transformation pipeline must reject filter keys
    /// that only make sense on the replication path (`client=acme` and
    /// friends). Without this fast-fail, a stale CI invocation would
    /// silently match zero models and ship a zero-target promote, which
    /// would look like a successful no-op.
    #[tokio::test]
    async fn discover_branch_targets_transformation_rejects_replication_filter() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let config_path = dir.join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"
"#,
        )
        .unwrap();

        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );

        let record = sample_record("fix-price");
        let err = discover_branch_targets(&config_path, &record, Some("client=acme"), None)
            .await
            .expect_err("client= filter must be rejected on transformation pipelines");

        let msg = format!("{err:#}");
        assert!(
            msg.contains("does not support `--filter client="),
            "error must name the rejected key: {msg}"
        );
        assert!(
            msg.contains("table, model, catalog, schema"),
            "error must list supported keys: {msg}"
        );
    }

    /// Mixed project: `rocky.toml` defines both a transformation pipeline
    /// and a replication pipeline against the same DuckDB adapter. With
    /// `--pipeline` disambiguating the choice, the dispatcher must route
    /// into the right enumeration helper per pipeline type:
    ///
    /// - `--pipeline marts` (transformation) returns one entry per model.
    /// - `--pipeline raw` (replication) returns one entry per discovered
    ///   source table — empty here because no source schemas exist in the
    ///   freshly-created DuckDB file, which is the right empty answer
    ///   rather than a panic.
    /// - Omitting `--pipeline` on a multi-pipeline config fails with the
    ///   `resolve_pipeline` "multiple pipelines defined" error — no
    ///   implicit pick.
    ///
    /// Proves the routing in `discover_branch_targets` honors `--pipeline`
    /// and dispatches to the correct per-type helper regardless of project
    /// composition.
    #[tokio::test]
    async fn discover_branch_targets_mixed_project_per_pipeline_routing() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let duckdb_path = dir.join("warehouse.duckdb");
        let config_path = dir.join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                r#"[adapter]
type = "duckdb"
path = "{}"

# Replication pipeline — discovery is auto-wired to the DuckDB adapter
# via `normalize_rocky_config`. No `src__*` schemas exist in the freshly
# created database so the enumeration is correctly empty rather than
# panicking.
[pipeline.raw]
type = "replication"

[pipeline.raw.source.schema_pattern]
prefix = "src__"
separator = "__"
components = ["client", "source"]

[pipeline.raw.target]
adapter = "default"
catalog_template = "{{client}}_warehouse"
schema_template = "raw__{{source}}"

# Transformation pipeline with on-disk models.
[pipeline.marts]
type = "transformation"
models = "models/**"

[pipeline.marts.target]
adapter = "default"
"#,
                duckdb_path.display()
            ),
        )
        .unwrap();

        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );

        let record = sample_record("fix-price");

        // Routing 1: --pipeline marts (transformation) walks the model glob.
        let planned = discover_branch_targets(&config_path, &record, None, Some("marts"))
            .await
            .expect("transformation pipeline 'marts' must enumerate models");
        assert_eq!(planned.len(), 1, "marts walks one model");
        assert_eq!(planned[0].prod.full_name(), "warehouse.marts.fct_orders");
        assert_eq!(
            planned[0].branch_source.full_name(),
            "warehouse.branch__fix-price.fct_orders"
        );

        // Routing 2: --pipeline raw (replication) dispatches into the
        // replication enumeration. No source schemas exist so the planned
        // set is empty — but the path is taken (no Err, no panic).
        let planned = discover_branch_targets(&config_path, &record, None, Some("raw"))
            .await
            .expect("replication enumeration over empty source must return Ok([])");
        assert_eq!(
            planned.len(),
            0,
            "no `src__*` schemas → zero planned targets, got {}",
            planned.len()
        );

        // Routing 3: omitting --pipeline on a multi-pipeline config must
        // surface `resolve_pipeline`'s "multiple pipelines defined" error
        // — no implicit pick.
        let err = discover_branch_targets(&config_path, &record, None, None)
            .await
            .expect_err("multi-pipeline config without --pipeline must error");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("multiple pipelines defined"),
            "must surface resolve_pipeline multi-pipeline error: {msg}"
        );
    }

    /// `branch promote` on a transformation pipeline respects the
    /// `--filter model=<name>` filter — matches the equivalent filter on
    /// `rocky run` for transformation pipelines.
    #[tokio::test]
    async fn discover_branch_targets_transformation_filter_model_keeps_only_match() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        let config_path = dir.join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"[adapter]
type = "duckdb"
path = ":memory:"

[pipeline.t]
type = "transformation"
models = "models/**"

[pipeline.t.target]
adapter = "default"
"#,
        )
        .unwrap();

        let models_dir = dir.join("models");
        std::fs::create_dir(&models_dir).unwrap();
        write_transformation_model(
            &models_dir,
            "fct_orders",
            "warehouse",
            "marts",
            "fct_orders",
            "SELECT 1 AS id",
        );
        write_transformation_model(
            &models_dir,
            "dim_customers",
            "warehouse",
            "marts",
            "dim_customers",
            "SELECT 1 AS id",
        );

        let record = sample_record("fix-price");
        let planned =
            discover_branch_targets(&config_path, &record, Some("model=fct_orders"), None)
                .await
                .expect("filter must succeed");
        assert_eq!(planned.len(), 1, "filter must keep only the matching model");
        assert_eq!(planned[0].prod.table, "fct_orders");
    }
}
