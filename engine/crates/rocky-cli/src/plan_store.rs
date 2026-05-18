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
//!
//! ## Persisted plan format versioning (Phase C — "SQL as `.o` files")
//!
//! [`PersistedPlan::format_version`] tags the on-disk plan shape:
//!
//! - **`1` (legacy; the only format prior to C-5, opt-in as of v1.35):**
//!   the `payload` is a full `CompactOutput` / `ArchiveOutput` / `RunPlan`
//!   / `PromotePlan` envelope including inline SQL strings. Apply reads
//!   SQL directly from the payload.
//! - **`2` (default as of v1.35; selected when `[plan_store] format = "v2"`
//!   or omitted):** for compact and archive plans, the `payload` is a
//!   [`rocky_ir::CompactPlanIr`] / [`rocky_ir::ArchivePlanIr`]; apply
//!   regenerates SQL via `rocky_core::sql_gen::{compact_from_ir,
//!   archive_from_ir}`. Run plans keep `format_version = 1` (they are
//!   already IR-only by construction); promote plans always keep
//!   `format_version = 1` (governance audit exception per the Phase C
//!   audit memo §Q2).
//!
//! The reader accepts both versions unconditionally — a v1 plan on disk
//! continues to apply against a binary configured for v2 writes, and vice
//! versa, for the duration of the migration window.
//!
//! The `format_version` field is **not** included in the `plan_id`
//! digest. The digest is computed over `{kind, payload}` only; equivalent
//! payloads written under either format use different payload bytes
//! anyway, so the digest is unique to the persisted shape without
//! mixing the version tag in.

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
    /// A `rocky plan` / `rocky apply` plan for a replication-only project
    /// (no `models/` directory, or `models/` exists but contains zero
    /// compiled models). The payload is a `ReplicationPlan` struct
    /// capturing the canonical `RockyConfig` snapshot and the discovered
    /// source state (sorted connectors + tables) at plan time. At apply
    /// time discovery is re-run and the snapshot is asserted byte-equal
    /// against the persisted one — stale plans are rejected with a clear
    /// "re-plan and re-apply" error before any SQL is executed.
    Replication,
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
            PlanKind::Replication => write!(f, "replication"),
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
    /// Persisted plan-format version (Phase C — "SQL as `.o` files").
    ///
    /// `1` is the legacy shape: `payload` is the full `*Output` envelope
    /// with inline SQL. `2` (compact/archive only) is the typed-IR shape:
    /// `payload` is `CompactPlanIr` / `ArchivePlanIr` and the apply path
    /// regenerates SQL at execution time.
    ///
    /// `#[serde(default = "default_format_version")]` so plans on disk
    /// written before C-5 (which had no `format_version` field) parse as
    /// `1` without any migration step.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
    /// Raw plan body, opaque to plan_store. For `format_version = 1`
    /// this is the full `*Output` struct serialized to JSON (with
    /// `plan_id = None` so the digest remains reproducible). For
    /// `format_version = 2` this is the typed-IR shape
    /// ([`rocky_ir::CompactPlanIr`] / [`rocky_ir::ArchivePlanIr`]).
    pub payload: serde_json::Value,
}

/// Default `format_version` for `PersistedPlan` when the field is absent
/// on disk — kept at `1` so legacy plans (which predate C-5) read cleanly
/// without any migration step.
fn default_format_version() -> u32 {
    1
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
///
/// This is the **v1** writer entry point. Tags the persisted record with
/// `format_version = 1`. For the v2 typed-IR writer used by `rocky compact`
/// / `rocky archive` under the default `[plan_store] format = "v2"`, see
/// [`write_plan_v2`].
pub fn write_plan<T: Serialize>(root: &Path, kind: PlanKind, payload: &T) -> Result<String> {
    write_plan_inner(root, kind, payload, 1)
}

/// Persist a **v2** typed-IR plan payload (`format_version = 2`).
///
/// Used by `rocky compact` / `rocky archive` under the default
/// `[plan_store] format = "v2"` (v1.35.0 onward). `payload` is expected
/// to be a [`rocky_ir::CompactPlanIr`] /
/// [`rocky_ir::ArchivePlanIr`] (or any serde value the apply path knows
/// how to reconstruct into one).
///
/// The `kind` discriminator stays the same as v1 (`PlanKind::Compact` /
/// `PlanKind::Archive`); the payload shape — not the kind — is what
/// changes between formats. Apply-side dispatch reads
/// [`PersistedPlan::format_version`] to pick the right deserialization
/// arm.
///
/// `PromotePlan` and `RunPlan` are intentionally **not** supported by
/// this writer: promote is held as a documented exception (Phase C
/// audit memo §Q2 — governance audit-grep surface) and run plans were
/// already IR-only by construction.
pub fn write_plan_v2<T: Serialize>(root: &Path, kind: PlanKind, payload: &T) -> Result<String> {
    debug_assert!(
        matches!(kind, PlanKind::Compact | PlanKind::Archive),
        "v2 plan-store format is defined only for compact and archive; got {kind}"
    );
    write_plan_inner(root, kind, payload, 2)
}

/// Internal writer shared by [`write_plan`] (v1) and [`write_plan_v2`]
/// (v2). Stamps `format_version` on the persisted record; the
/// `plan_id` digest is computed over `{kind, payload}` only, so a v1
/// `CompactOutput` payload and a v2 `CompactPlanIr` payload for the
/// same logical compact plan yield different plan_ids (different
/// payload bytes → different hashes), which is the desired property:
/// the on-disk filename also disambiguates the format.
fn write_plan_inner<T: Serialize>(
    root: &Path,
    kind: PlanKind,
    payload: &T,
    format_version: u32,
) -> Result<String> {
    let payload_value =
        serde_json::to_value(payload).context("failed to serialize plan payload to JSON value")?;

    let plan_id = compute_plan_id(&kind, &payload_value);

    let record = PersistedPlan {
        plan_id: plan_id.clone(),
        kind,
        created_at: Utc::now(),
        format_version,
        payload: payload_value,
    };

    let dir = plans_dir(root)?;
    let path = dir.join(format!("{plan_id}.json"));

    let bytes =
        serde_json::to_vec_pretty(&record).context("failed to serialize persisted plan to JSON")?;
    std::fs::write(&path, bytes)
        .with_context(|| format!("failed to write plan to {}", path.display()))?;

    tracing::debug!(
        plan_id = %plan_id,
        path = %path.display(),
        format_version,
        "plan persisted"
    );
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
        let id_replication = write_plan(dir.path(), PlanKind::Replication, &payload)?;
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
            id_run, id_replication,
            "run and replication must produce different plan_ids"
        );
        assert_ne!(
            id_compact, id_replication,
            "compact and replication must produce different plan_ids"
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
    fn replication_kind_round_trip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.replication_table",
            statement_count: 4,
        };

        let plan_id = write_plan(dir.path(), PlanKind::Replication, &payload)?;
        assert_eq!(plan_id.len(), 64);

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::Replication);
        assert_eq!(
            plan.payload["model"],
            serde_json::json!("cat.sc.replication_table")
        );
        assert_eq!(plan.payload["statement_count"], serde_json::json!(4));
        Ok(())
    }

    /// `PlanKind::Replication` serializes to the snake_case wire name
    /// `"replication"`, mirroring the other variants. The dispatcher in
    /// `commands::apply` relies on this for round-trip dispatch.
    #[test]
    fn replication_kind_wire_name() {
        let kind = PlanKind::Replication;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""replication""#);
        let parsed: PlanKind = serde_json::from_str(r#""replication""#).unwrap();
        assert_eq!(parsed, PlanKind::Replication);
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
        assert_eq!(PlanKind::Replication.to_string(), "replication");
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

    // ---------- Phase C — format_version tagging (C-5) ----------

    #[test]
    fn write_plan_tags_format_version_one() -> anyhow::Result<()> {
        // C-5 invariant: the legacy `write_plan` entry point always tags
        // the persisted record with `format_version = 1`. Callers that
        // opt into the v1 on-disk shape via `[plan_store] format = "v1"`
        // (single-model and catalog-scope compact/archive paths) keep
        // producing v1 plans byte-for-byte.
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 2,
        };
        let id = write_plan(dir.path(), PlanKind::Compact, &payload)?;
        let plan = read_plan(dir.path(), &id)?;
        assert_eq!(plan.format_version, 1);
        Ok(())
    }

    #[test]
    fn write_plan_v2_tags_format_version_two() -> anyhow::Result<()> {
        // C-5 invariant: the new `write_plan_v2` entry point tags the
        // persisted record with `format_version = 2`. The apply path
        // uses that tag to dispatch into the IR-regeneration arm.
        let dir = tempfile::tempdir()?;
        let payload = serde_json::json!({
            "target_table": "cat.sc.tbl",
            "target_size_mb": 256,
            "vacuum_retention_hours": 168
        });
        let id = write_plan_v2(dir.path(), PlanKind::Compact, &payload)?;
        let plan = read_plan(dir.path(), &id)?;
        assert_eq!(plan.format_version, 2);
        // Payload bytes are the v2-shape JSON (no `purpose` / `sql`
        // fields anywhere) — sanity check that the v2 writer round-trips
        // the IR payload verbatim.
        assert_eq!(
            plan.payload["target_table"],
            serde_json::json!("cat.sc.tbl")
        );
        assert!(plan.payload.get("statements").is_none());
        Ok(())
    }

    #[test]
    fn read_plan_defaults_format_version_to_one_for_legacy_files() -> anyhow::Result<()> {
        // Backward-compat: a plan file written by a pre-C-5 binary has no
        // `format_version` field. `#[serde(default)]` must surface that
        // file as `format_version = 1` so it parses cleanly and the apply
        // path picks the v1 dispatch arm without any migration step.
        let dir = tempfile::tempdir()?;
        let plans_dir = dir.path().join(".rocky").join("plans");
        std::fs::create_dir_all(&plans_dir)?;
        let plan_id = "a".repeat(64);
        let legacy_json = serde_json::json!({
            "plan_id": plan_id,
            "kind": "compact",
            "created_at": "2026-05-15T12:34:56Z",
            "payload": {"dummy": true}
        });
        std::fs::write(
            plans_dir.join(format!("{plan_id}.json")),
            serde_json::to_vec_pretty(&legacy_json)?,
        )?;

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.format_version, 1);
        assert_eq!(plan.kind, PlanKind::Compact);
        Ok(())
    }

    #[test]
    fn write_plan_and_write_plan_v2_yield_distinct_plan_ids() -> anyhow::Result<()> {
        // v1 and v2 writers serialize different payload bytes for the
        // same logical plan (full output envelope vs typed IR), so the
        // blake3 digest — and the on-disk filename — disambiguate the
        // two formats. This is the invariant that lets us write a v1
        // plan and a v2 plan for the same compact target without one
        // overwriting the other.
        let dir = tempfile::tempdir()?;
        let v1_payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 2,
        };
        let v2_payload = serde_json::json!({
            "target_table": "cat.sc.tbl",
            "target_size_mb": 256,
            "vacuum_retention_hours": 168
        });
        let v1_id = write_plan(dir.path(), PlanKind::Compact, &v1_payload)?;
        let v2_id = write_plan_v2(dir.path(), PlanKind::Compact, &v2_payload)?;
        assert_ne!(
            v1_id, v2_id,
            "different payload shapes must hash to different plan_ids"
        );
        Ok(())
    }
}
