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

use rocky_core::compare::ComparisonThresholds;
use rocky_core::config::BranchApprovalConfig;
use rocky_core::ir::TargetRef;
use rocky_core::shadow::{self, ShadowConfig};
use rocky_core::state::{BranchRecord, StateStore};

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

/// Build an [`ApproverIdentity`] from the local environment. Used by both
/// `branch approve` (signing identity) and `branch promote` (actor on every
/// audit event).
fn approver_identity() -> Result<ApproverIdentity> {
    let (email, name) = git_identity()?;
    let host = hostname::get()
        .ok()
        .and_then(|h| h.into_string().ok())
        .unwrap_or_else(|| "unknown".to_string());
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
/// The hash inputs are intentionally minimal in v1 (Option B in the design
/// memo): the branch name, its `schema_prefix`, and the rocky.toml content
/// fingerprint. A change to either the branch metadata or the project
/// config voids every existing approval. Adding `latest_run_id` and a
/// `models_hash` are forward-compatible follow-ups — they extend the hashed
/// payload without breaking the artifact format.
fn compute_branch_state_hash(record: &BranchRecord, config_path: &Path) -> Result<String> {
    let payload = serde_json::json!({
        "branch": record.name,
        "schema_prefix": record.schema_prefix,
        "config_hash": config_fingerprint(config_path),
    });
    let canonical = canonical_json(&payload)?;
    Ok(blake3::hash(canonical.as_bytes()).to_hex().to_string())
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

/// Enumerate the promote plan for a branch by walking the configured
/// replication pipeline's discovery surface — the same enumeration
/// `branch compare` uses. Transformation-pipeline promote (model glob walk)
/// is a v2 follow-up; the v1 implementation surfaces a clear error if the
/// resolved pipeline is not a replication pipeline.
async fn discover_branch_targets(
    config_path: &Path,
    record: &BranchRecord,
    filter: Option<&str>,
) -> Result<Vec<PlannedPromote>> {
    use super::{matches_filter, parse_filter};
    use crate::registry::{self, AdapterRegistry};

    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (_pipeline_name, pipeline) = registry::resolve_replication_pipeline(&rocky_cfg, None)?;
    let registry = AdapterRegistry::from_config(&rocky_cfg)?;

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
            "no discovery adapter configured — `branch promote` requires source discovery in v1 \
             (transformation-pipeline promote is a follow-up)"
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
        if let Some((ref filter_key, ref filter_value)) = parsed_filter {
            if !matches_filter(conn, &parsed, filter_key, filter_value) {
                continue;
            }
        }
        let target_sep = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        let target_catalog = parsed.resolve_template(target_catalog_template, target_sep);
        let target_schema = parsed.resolve_template(target_schema_template, target_sep);

        for table in &conn.tables {
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

/// `rocky branch promote <name>` — promote a branch's materialized tables
/// to their production targets, gated on `[branch.approval]` if enabled.
pub async fn run_branch_promote(
    state_path: &Path,
    config_path: &Path,
    branch_name: &str,
    filter: Option<&str>,
    skip_approval_flag: bool,
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

    audit.push(AuditEvent {
        kind: AuditEventKind::PromoteStarted,
        at: Utc::now(),
        actor: actor.clone(),
        branch: record.name.clone(),
        branch_state_hash: branch_state_hash.clone(),
        reason: None,
    });

    let planned = discover_branch_targets(config_path, &record, filter).await?;

    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let (_pipeline_name, pipeline) =
        crate::registry::resolve_replication_pipeline(&rocky_cfg, None)?;
    let adapter = registry.warehouse_adapter(&pipeline.target.adapter)?;

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
    });

    let output = BranchPromoteOutput {
        version: VERSION.to_string(),
        command: "branch promote".to_string(),
        branch: record.name.clone(),
        branch_state_hash,
        approvals_used,
        approvals_rejected,
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
        let tmp = TempDir::new().unwrap();
        let saved = std::env::current_dir().unwrap();
        std::env::set_current_dir(tmp.path()).unwrap();

        let (loaded, rejected) = load_approvals_for_branch("ghost").unwrap();
        assert!(loaded.is_empty());
        assert!(rejected.is_empty());

        std::env::set_current_dir(saved).unwrap();
    }
}
