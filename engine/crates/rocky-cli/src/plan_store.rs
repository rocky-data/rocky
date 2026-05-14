//! Persistent plan store for `rocky compact apply` / `rocky archive apply` /
//! `rocky apply`.
//!
//! Plans are written to `<root>/.rocky/plans/<plan_id>.json` where `plan_id`
//! is the full 64-character blake3 hex digest of the canonical JSON
//! `{"kind": "<kind>", "payload": <payload>}`.
//!
//! ## Determinism guarantee
//!
//! The blake3 digest is computed over `serde_json::to_vec` applied to the
//! canonical envelope `{"kind": ..., "payload": ...}`. Because all payloads
//! (`CompactOutput`, `ArchiveOutput`, `RunPlan`) originate from serde-derived
//! structs with a fixed field order, `to_vec` produces a byte-stable JSON
//! encoding for the same plan content. The `plan_id` field in `CompactOutput`
//! and `ArchiveOutput` is excluded from the hash (serialized as `None`) so that
//! re-hashing the persisted `payload` reproduces the filename.

use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// The kind of plan — used to guard cross-apply mismatches.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanKind {
    Compact,
    Archive,
    /// A `rocky plan` / `rocky apply` run plan. The payload is a `RunPlan`
    /// struct (operational metadata: filter, pipeline, partition flags, model
    /// list, execution layers). Full `ProjectIr` is not persisted — `apply`
    /// re-derives it by re-compiling with the same flags.
    Run,
    /// A `rocky plan promote` / `rocky apply` promote plan. The payload is a
    /// `PromotePlan` struct (branch name, base ref, per-target SQL statements,
    /// plan-time audit events). At apply time the branch-state hash is
    /// recomputed and checked against the persisted value so stale plans are
    /// rejected before any SQL is executed.
    Promote,
}

impl std::fmt::Display for PlanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanKind::Compact => write!(f, "compact"),
            PlanKind::Archive => write!(f, "archive"),
            PlanKind::Run => write!(f, "run"),
            PlanKind::Promote => write!(f, "promote"),
        }
    }
}

/// A plan record as persisted to disk.
#[derive(Debug, Serialize, Deserialize)]
pub struct PersistedPlan {
    /// Full 64-char blake3 hex plan identifier.
    pub plan_id: String,
    pub kind: PlanKind,
    pub created_at: DateTime<Utc>,
    /// Raw plan body, opaque to plan_store. Contains the full `*Output`
    /// struct serialized to JSON (with `plan_id = None` so the digest
    /// remains reproducible).
    pub payload: serde_json::Value,
}

/// Compute the blake3 plan_id for the given `(kind, payload)` pair.
///
/// The digest is over the JSON bytes of `{"kind": <kind>, "payload": <payload>}`.
/// `payload` must be the pre-`plan_id` version (i.e. `plan_id` field is `None`
/// or absent) so the id is stable.
fn compute_plan_id(kind: &PlanKind, payload: &serde_json::Value) -> String {
    #[derive(Serialize)]
    struct Envelope<'a> {
        kind: &'a PlanKind,
        payload: &'a serde_json::Value,
    }
    let envelope = Envelope { kind, payload };
    let bytes = serde_json::to_vec(&envelope).expect("envelope serialization is infallible");
    let hash = blake3::hash(&bytes);
    hash.to_hex().to_string()
}

/// Return the directory where plans are stored, creating it if needed.
fn plans_dir(root: &Path) -> Result<std::path::PathBuf> {
    let dir = root.join(".rocky").join("plans");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create plans directory at {}", dir.display()))?;
    Ok(dir)
}

/// Serialize `payload` via `impl Serialize`, compute the plan_id, persist the
/// plan to `<root>/.rocky/plans/<plan_id>.json`, and return the plan_id.
///
/// `payload` should be the output struct with its `plan_id` field set to
/// `None` (so the digest is stable). Callers populate `plan_id` in the
/// printed output *after* this call returns.
pub fn write_plan<T: Serialize>(root: &Path, kind: PlanKind, payload: &T) -> Result<String> {
    let payload_value =
        serde_json::to_value(payload).context("failed to serialize plan payload to JSON value")?;

    let plan_id = compute_plan_id(&kind, &payload_value);

    let record = PersistedPlan {
        plan_id: plan_id.clone(),
        kind,
        created_at: Utc::now(),
        payload: payload_value,
    };

    let dir = plans_dir(root)?;
    let path = dir.join(format!("{plan_id}.json"));

    let bytes =
        serde_json::to_vec_pretty(&record).context("failed to serialize persisted plan to JSON")?;
    std::fs::write(&path, bytes)
        .with_context(|| format!("failed to write plan to {}", path.display()))?;

    tracing::debug!(plan_id = %plan_id, path = %path.display(), "plan persisted");
    Ok(plan_id)
}

/// Read a plan from `<root>/.rocky/plans/<plan_id>.json`.
///
/// Returns a clear error if the file is missing or the JSON is malformed.
pub fn read_plan(root: &Path, plan_id: &str) -> Result<PersistedPlan> {
    let dir = plans_dir(root)?;
    let path = dir.join(format!("{plan_id}.json"));

    if !path.exists() {
        bail!(
            "plan '{}' not found — no file at {}. \
             Generate a plan first with `rocky compact <model>`, `rocky archive <model>`, \
             or `rocky plan`.",
            plan_id,
            path.display()
        );
    }

    let bytes = std::fs::read(&path)
        .with_context(|| format!("failed to read plan file at {}", path.display()))?;

    let plan: PersistedPlan = serde_json::from_slice(&bytes)
        .with_context(|| format!("plan file at {} is not valid JSON", path.display()))?;

    Ok(plan)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[derive(Serialize)]
    struct DummyPayload {
        model: &'static str,
        statement_count: usize,
        // Mirrors the pattern in CompactOutput / ArchiveOutput: plan_id is
        // absent from what we hash (serialized as None / missing).
    }

    #[test]
    fn round_trip_write_and_read() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "mydb.myschema.orders",
            statement_count: 2,
        };

        let plan_id = write_plan(dir.path(), PlanKind::Compact, &payload)?;
        assert_eq!(plan_id.len(), 64, "blake3 hex is always 64 chars");

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.plan_id, plan_id);
        assert_eq!(plan.kind, PlanKind::Compact);
        assert_eq!(plan.payload["model"], json!("mydb.myschema.orders"));
        assert_eq!(plan.payload["statement_count"], json!(2));
        Ok(())
    }

    #[test]
    fn same_payload_same_plan_id() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 3,
        };

        let id1 = write_plan(dir.path(), PlanKind::Compact, &payload)?;
        let id2 = write_plan(dir.path(), PlanKind::Compact, &payload)?;
        assert_eq!(id1, id2, "identical payload → identical plan_id");
        Ok(())
    }

    #[test]
    fn different_kinds_different_plan_ids() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 2,
        };

        let id_compact = write_plan(dir.path(), PlanKind::Compact, &payload)?;
        let id_archive = write_plan(dir.path(), PlanKind::Archive, &payload)?;
        let id_run = write_plan(dir.path(), PlanKind::Run, &payload)?;
        let id_promote = write_plan(dir.path(), PlanKind::Promote, &payload)?;
        assert_ne!(
            id_compact, id_archive,
            "different kinds must produce different plan_ids"
        );
        assert_ne!(
            id_compact, id_run,
            "compact and run must produce different plan_ids"
        );
        assert_ne!(
            id_archive, id_run,
            "archive and run must produce different plan_ids"
        );
        assert_ne!(
            id_compact, id_promote,
            "compact and promote must produce different plan_ids"
        );
        assert_ne!(
            id_run, id_promote,
            "run and promote must produce different plan_ids"
        );
        Ok(())
    }

    #[test]
    fn promote_kind_round_trip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.branch_tbl",
            statement_count: 7,
        };

        let plan_id = write_plan(dir.path(), PlanKind::Promote, &payload)?;
        assert_eq!(plan_id.len(), 64);

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::Promote);
        assert_eq!(
            plan.payload["model"],
            serde_json::json!("cat.sc.branch_tbl")
        );
        assert_eq!(plan.payload["statement_count"], serde_json::json!(7));
        Ok(())
    }

    #[test]
    fn run_kind_round_trip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 5,
        };

        let plan_id = write_plan(dir.path(), PlanKind::Run, &payload)?;
        assert_eq!(plan_id.len(), 64);

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::Run);
        assert_eq!(plan.payload["model"], serde_json::json!("cat.sc.tbl"));
        assert_eq!(plan.payload["statement_count"], serde_json::json!(5));
        Ok(())
    }

    #[test]
    fn plan_kind_display() {
        assert_eq!(PlanKind::Compact.to_string(), "compact");
        assert_eq!(PlanKind::Archive.to_string(), "archive");
        assert_eq!(PlanKind::Run.to_string(), "run");
        assert_eq!(PlanKind::Promote.to_string(), "promote");
    }

    #[test]
    fn missing_plan_returns_clear_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = read_plan(dir.path(), "deadbeef".repeat(8).as_str()).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("not found"),
            "error should mention 'not found', got: {msg}"
        );
    }

    #[test]
    fn compute_plan_id_is_64_hex_chars() {
        let id = compute_plan_id(&PlanKind::Compact, &json!({"foo": "bar"}));
        assert_eq!(id.len(), 64);
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn created_at_is_populated() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "x.y.z",
            statement_count: 1,
        };
        let id = write_plan(dir.path(), PlanKind::Archive, &payload)?;
        let plan = read_plan(dir.path(), &id)?;
        // created_at must be a valid, recent timestamp
        let elapsed = Utc::now()
            .signed_duration_since(plan.created_at)
            .num_seconds();
        assert!(
            (0..5).contains(&elapsed),
            "created_at should be within 5s of now, got {elapsed}s ago"
        );
        Ok(())
    }
}
