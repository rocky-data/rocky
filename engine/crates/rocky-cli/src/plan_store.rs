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
//! - **`1` (used for `Run` / `Replication` / `Promote` plans):** the
//!   `payload` is a `RunPlan` / `ReplicationPlan` / `PromotePlan` struct.
//!   These payloads were never inline-SQL envelopes — run/replication
//!   plans are IR-only by construction, and promote plans persist
//!   per-target SQL strings as a documented governance-audit exception
//!   (Phase C audit memo §Q2). They have always parsed as
//!   `format_version = 1` and continue to do so.
//! - **`2` (used for `Compact` / `Archive` plans, the only loadable
//!   shape for those kinds as of C-7):** the `payload` is a
//!   [`rocky_ir::CompactPlanIr`] / [`rocky_ir::ArchivePlanIr`]; apply
//!   regenerates SQL via `rocky_core::sql_gen::{compact_from_ir,
//!   archive_from_ir}`.
//!
//! The reader is format-agnostic — it parses `PersistedPlan` from disk
//! and hands the payload + kind + `format_version` to the per-kind
//! apply dispatch. The compact-apply and archive-apply paths now
//! require `format_version = 2`; v1-shaped compact/archive payloads
//! (legacy `CompactOutput` / `ArchiveOutput` envelopes with inline SQL,
//! written by Rocky < engine-v1.35.0) are rejected with a clear
//! migration error — see the corresponding `format_version` dispatch
//! arms in [`crate::commands::compact`] and [`crate::commands::archive`].
//!
//! The `format_version` field is **not** included in the `plan_id`
//! digest. The digest is computed over `{kind, payload}` only.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use rocky_core::config::{PolicyCapability, PolicyPrincipal};
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
    /// An AI-authored run plan. The payload is a `RunPlan` struct — the same
    /// shape as [`PlanKind::Run`] — but the kind discriminator marks it as
    /// machine-authored, so a bare `rocky apply` refuses to execute it until a
    /// human signs off via `rocky review <plan-id> --approve`. The review step
    /// writes a marker file alongside the plan; `apply` requires that marker
    /// before dispatching the same execution path as a `Run` plan.
    #[serde(rename = "ai_authored")]
    AiAuthored,
}

impl std::fmt::Display for PlanKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanKind::Compact => write!(f, "compact"),
            PlanKind::Archive => write!(f, "archive"),
            PlanKind::Run => write!(f, "run"),
            PlanKind::Replication => write!(f, "replication"),
            PlanKind::Promote => write!(f, "promote"),
            PlanKind::AiAuthored => write!(f, "ai_authored"),
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
    /// - `1` — used for `Run` / `Replication` / `Promote` plans (the
    ///   payload was never an inline-SQL envelope; these kinds have
    ///   always been parsed under this tag).
    /// - `2` — used for `Compact` / `Archive` plans (typed-IR payload —
    ///   `CompactPlanIr` / `ArchivePlanIr`; SQL is regenerated at apply
    ///   time).
    ///
    /// Compact/archive payloads written by Rocky < engine-v1.35.0 carry
    /// `format_version = 1` (the legacy inline-SQL envelope) and are no
    /// longer loadable — see the dispatch arms in
    /// [`crate::commands::compact`] / [`crate::commands::archive`].
    ///
    /// `#[serde(default = "default_format_version")]` so plans on disk
    /// written before C-5 (which had no `format_version` field) parse
    /// as `1` without any migration step — relevant for `Run` /
    /// `Replication` / `Promote` plans that are still legitimately
    /// `format_version = 1`.
    #[serde(default = "default_format_version")]
    pub format_version: u32,
    /// The principal that authored this plan (F3 agent-policy plane).
    ///
    /// Rides **outside** the `plan_id` digest (like `created_at` /
    /// `format_version`) — the authoring identity must not perturb the hash,
    /// while the reviewed capability classification (in `payload`) must bind
    /// to it. Stamped at plan-creation.
    ///
    /// **Absent on legacy plans** (written before the field existed). An
    /// absent value does NOT default to `human`: it resolves *by kind* via
    /// [`PersistedPlan::resolved_principal`] — an `ai_authored` plan with no
    /// stamp resolves to `agent` (never `human`, which would let a legacy
    /// AI plan apply unreviewed once the hardcoded gate becomes rule-driven);
    /// any other kind resolves to `human`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub principal: Option<PolicyPrincipal>,
    /// Raw plan body, opaque to plan_store. For `format_version = 2`
    /// (compact/archive) this is the typed-IR shape
    /// ([`rocky_ir::CompactPlanIr`] / [`rocky_ir::ArchivePlanIr`]). For
    /// `format_version = 1` (run/replication/promote) this is the
    /// per-kind plan struct serialized to JSON.
    ///
    /// For governed run/ai_authored plans the payload additionally carries a
    /// `policy_capabilities` object (the propose-time change-classification —
    /// see [`EmbeddedCapabilities`]) so the reviewed capability binds to the
    /// `plan_id`.
    pub payload: serde_json::Value,
}

/// The reserved payload key under which the propose-time change-classification
/// is embedded (D3). Kept out of every typed plan struct — it is injected into
/// the serialized payload `Value` at write time and read back as an
/// [`EmbeddedCapabilities`] at apply time.
const POLICY_CAPABILITIES_KEY: &str = "policy_capabilities";

/// The propose-time change-classification embedded in a governed plan's
/// payload (D3). Part of `blake3({kind, payload})`, so the reviewed capability
/// decision binds to the `plan_id`.
///
/// Apply reads this back and never recomputes from a live diff. **Fail-closed
/// default:** an absent embed (a legacy plan, or a plan whose base↔head diff
/// was skipped) deserializes to `diff_available = false` with an empty
/// `changed` map, which the enforcement path treats as "classify every planned
/// model as breaking".
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct EmbeddedCapabilities {
    /// Whether the base↔head classification diff was available at
    /// plan-creation. `false` ⇒ apply fails closed (every planned model is
    /// treated as `schema_change.breaking`).
    #[serde(default)]
    pub diff_available: bool,
    /// Per-changed-model capability, keyed by the logical model name (matching
    /// `RunPlan.models` and the apply-time `ModelAttributes.name`). Only models
    /// the diff found a change on appear here; unchanged models are absent and
    /// are not gated.
    #[serde(default)]
    pub changed: BTreeMap<String, PolicyCapability>,
}

/// The principal an absent stamp resolves to, **by kind** (never a blanket
/// `human`): an `ai_authored` plan is agent-authored by construction
/// (`AiAuthored ⊂ agent`), so a legacy AI plan with no stamp is `agent`; any
/// other kind is `human`.
fn default_principal_for_kind(kind: &PlanKind) -> PolicyPrincipal {
    match kind {
        PlanKind::AiAuthored => PolicyPrincipal::Agent,
        _ => PolicyPrincipal::Human,
    }
}

impl PersistedPlan {
    /// The authoring principal, resolving an absent stamp by kind (see
    /// [`default_principal_for_kind`]). This is the value the F3 policy plane
    /// evaluates — the *authoring* identity binds, so a plan authored by an
    /// agent still evaluates as `agent` regardless of who runs `apply`.
    pub fn resolved_principal(&self) -> PolicyPrincipal {
        self.principal
            .unwrap_or_else(|| default_principal_for_kind(&self.kind))
    }

    /// The propose-time change-classification embedded in the payload (D3).
    /// Fail-closed: a plan with no embed yields `diff_available = false`.
    pub fn embedded_capabilities(&self) -> EmbeddedCapabilities {
        self.payload
            .get(POLICY_CAPABILITIES_KEY)
            .and_then(|v| serde_json::from_value(v.clone()).ok())
            .unwrap_or_default()
    }
}

/// Default `format_version` for `PersistedPlan` when the field is absent
/// on disk — kept at `1` so plans written before C-5 (which had no
/// `format_version` field) read cleanly. `Run` / `Replication` /
/// `Promote` plans still parse as `format_version = 1` legitimately;
/// `Compact` / `Archive` plans with `format_version = 1` are rejected
/// by the per-kind apply dispatch (see the C-7 drop note in the module
/// docs).
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
    let rocky_dir = root.join(".rocky");
    // Keep `.rocky/` (plans + traces) out of the user's git repo.
    rocky_observe::traces::ensure_rocky_gitignore(&rocky_dir);
    let dir = rocky_dir.join("plans");
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("failed to create plans directory at {}", dir.display()))?;
    Ok(dir)
}

/// Persist a `Run` / `Replication` / `Promote` plan with
/// `format_version = 1`.
///
/// These payloads were never inline-SQL envelopes — run/replication
/// plans are IR-only by construction, and promote plans persist
/// per-target SQL strings as a documented governance-audit exception
/// (Phase C audit memo §Q2). The `format_version = 1` tag has always
/// applied to these kinds and continues to do so after C-7.
///
/// Production `Compact` / `Archive` callers must go through
/// [`write_plan_v2`]; v1-shaped compact/archive payloads are no
/// longer a loadable shape (the apply dispatch rejects them with a
/// migration error). `PlanKind::Compact` / `PlanKind::Archive` are
/// still accepted here so that round-trip / kind-mismatch tests can
/// fabricate a v1 plan on disk and assert the apply path errors
/// cleanly.
pub fn write_plan<T: Serialize>(root: &Path, kind: PlanKind, payload: &T) -> Result<String> {
    let principal = default_principal_for_kind(&kind);
    write_plan_inner(root, kind, payload, 1, principal, None)
}

/// Persist a governed `Run` / `AiAuthored` plan (`format_version = 1`) with an
/// explicit authoring `principal` and an embedded propose-time
/// change-classification (D3).
///
/// The `capabilities` value is injected into the payload under
/// `policy_capabilities` **before** the `plan_id` is computed, so the reviewed
/// capability decision is part of `blake3({kind, payload})`. The `principal`
/// rides outside the hash. Used by `rocky plan` and the MCP `propose` tool.
pub fn write_plan_governed<T: Serialize>(
    root: &Path,
    kind: PlanKind,
    payload: &T,
    principal: PolicyPrincipal,
    capabilities: EmbeddedCapabilities,
) -> Result<String> {
    let caps_value =
        serde_json::to_value(capabilities).context("failed to serialize embedded capabilities")?;
    write_plan_inner(root, kind, payload, 1, principal, Some(caps_value))
}

/// Persist a typed-IR `Compact` / `Archive` plan
/// (`format_version = 2`).
///
/// `payload` is expected to be a [`rocky_ir::CompactPlanIr`] /
/// [`rocky_ir::ArchivePlanIr`] envelope (or any serde value the apply
/// path knows how to reconstruct into one). Apply-side dispatch in
/// [`crate::commands::compact`] / [`crate::commands::archive`] reads
/// [`PersistedPlan::format_version`] to validate the loaded plan is
/// indeed v2 — v1-shaped compact/archive payloads on disk (written by
/// Rocky < engine-v1.35.0) are rejected with a migration error.
pub fn write_plan_v2<T: Serialize>(root: &Path, kind: PlanKind, payload: &T) -> Result<String> {
    debug_assert!(
        matches!(kind, PlanKind::Compact | PlanKind::Archive),
        "v2 plan-store format is defined only for compact and archive; got {kind}"
    );
    let principal = default_principal_for_kind(&kind);
    write_plan_inner(root, kind, payload, 2, principal, None)
}

/// Internal writer shared by [`write_plan`] and [`write_plan_v2`].
/// Stamps `format_version` on the persisted record; the `plan_id`
/// digest is computed over `{kind, payload}` only, so different
/// payload shapes for the same logical plan kind yield different
/// `plan_id`s (different payload bytes → different hashes).
fn write_plan_inner<T: Serialize>(
    root: &Path,
    kind: PlanKind,
    payload: &T,
    format_version: u32,
    principal: PolicyPrincipal,
    capabilities: Option<serde_json::Value>,
) -> Result<String> {
    let mut payload_value =
        serde_json::to_value(payload).context("failed to serialize plan payload to JSON value")?;

    // Embed the propose-time change-classification (D3) INTO the payload so it
    // is part of `blake3({kind, payload})` — the reviewed capability binds to
    // the `plan_id`. Only meaningful for object payloads (run/ai_authored);
    // typed-IR compact/archive payloads never carry it.
    if let Some(caps) = capabilities
        && let serde_json::Value::Object(ref mut map) = payload_value
    {
        map.insert(POLICY_CAPABILITIES_KEY.to_string(), caps);
    }

    let plan_id = compute_plan_id(&kind, &payload_value);

    let record = PersistedPlan {
        plan_id: plan_id.clone(),
        kind,
        created_at: Utc::now(),
        format_version,
        principal: Some(principal),
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

    // Integrity check: the plan_id is the blake3 digest of `{kind, payload}`.
    // Recompute it from the bytes we just parsed and reject the plan if it does
    // not match the requested id (and the stored id). This binds the applied
    // bytes to the reviewed plan id — a plan whose payload was tampered with or
    // truncated-but-still-parseable after it was written (and reviewed) no
    // longer matches its filename / stored id, so apply refuses it.
    let recomputed = compute_plan_id(&plan.kind, &plan.payload);
    if recomputed != plan_id || recomputed != plan.plan_id {
        bail!(
            "plan '{}' failed its integrity check: the payload hashes to '{}', \
             which does not match the requested id '{}' (stored id '{}'). \
             The plan file at {} may have been modified after it was written — \
             re-generate the plan and (if AI-authored) re-review it before applying.",
            plan_id,
            recomputed,
            plan_id,
            plan.plan_id,
            path.display()
        );
    }

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

    /// L3: a persisted plan whose payload is mutated after write (so it no
    /// longer hashes to its filename / stored id) must be rejected by
    /// `read_plan`, not silently applied.
    #[test]
    fn tampered_payload_is_rejected() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.tbl",
            statement_count: 2,
        };
        let plan_id = write_plan(dir.path(), PlanKind::Compact, &payload)?;

        // It reads cleanly before tampering.
        assert!(read_plan(dir.path(), &plan_id).is_ok());

        // Mutate the payload on disk while leaving the JSON valid and the
        // filename + stored plan_id unchanged — exactly the post-review
        // tamper the integrity check must catch.
        let path = dir
            .path()
            .join(".rocky")
            .join("plans")
            .join(format!("{plan_id}.json"));
        let raw = std::fs::read_to_string(&path)?;
        let mut record: serde_json::Value = serde_json::from_str(&raw)?;
        record["payload"]["statement_count"] = json!(999);
        std::fs::write(&path, serde_json::to_vec_pretty(&record)?)?;

        let err = read_plan(dir.path(), &plan_id).expect_err("a tampered plan must be rejected");
        let msg = format!("{err}");
        assert!(
            msg.contains("integrity check"),
            "expected an integrity-check error, got: {msg}"
        );
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
        let id_ai = write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;
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
        // An AI-authored plan and a plain run plan share the same RunPlan
        // payload shape, so the *kind* discriminator is the only thing that
        // makes their digests differ — that's the whole point of the gate.
        assert_ne!(
            id_run, id_ai,
            "run and ai_authored must produce different plan_ids"
        );
        assert_ne!(
            id_compact, id_ai,
            "compact and ai_authored must produce different plan_ids"
        );
        Ok(())
    }

    #[test]
    fn ai_authored_kind_round_trip() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "cat.sc.ai_tbl",
            statement_count: 9,
        };

        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;
        assert_eq!(plan_id.len(), 64);

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.kind, PlanKind::AiAuthored);
        // AI-authored plans persist via the v1 writer (RunPlan payload), so
        // the on-disk format_version stays 1 like the other run-shaped kinds.
        assert_eq!(plan.format_version, 1);
        assert_eq!(plan.payload["model"], serde_json::json!("cat.sc.ai_tbl"));
        assert_eq!(plan.payload["statement_count"], serde_json::json!(9));
        Ok(())
    }

    /// `PlanKind::AiAuthored` serializes to the multi-word wire name
    /// `"ai_authored"` (the variant overrides the enum's `lowercase`
    /// rename with an explicit `serde(rename)`). The apply dispatcher
    /// relies on this for round-trip dispatch.
    #[test]
    fn ai_authored_kind_wire_name() {
        let kind = PlanKind::AiAuthored;
        let json = serde_json::to_string(&kind).unwrap();
        assert_eq!(json, r#""ai_authored""#);
        let parsed: PlanKind = serde_json::from_str(r#""ai_authored""#).unwrap();
        assert_eq!(parsed, PlanKind::AiAuthored);
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
        assert_eq!(PlanKind::AiAuthored.to_string(), "ai_authored");
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

    // ---------- F3 principal + embedded-capability (D3 / seam 1) ----------

    /// 🔴 The load-bearing safety invariant: a legacy `ai_authored` plan file
    /// with NO `principal` field must resolve to `agent`, never `human` —
    /// otherwise a legacy AI plan would evaluate as a human and apply
    /// unreviewed once the hardcoded gate becomes rule-driven.
    #[test]
    fn legacy_ai_authored_plan_with_no_principal_resolves_to_agent() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plans_dir = dir.path().join(".rocky").join("plans");
        std::fs::create_dir_all(&plans_dir)?;
        let payload = serde_json::json!({"models": ["db.s.t"]});
        let plan_id = compute_plan_id(&PlanKind::AiAuthored, &payload);
        // A pre-F3 plan file: kind = ai_authored, NO principal key at all.
        let legacy = serde_json::json!({
            "plan_id": plan_id,
            "kind": "ai_authored",
            "created_at": "2026-05-15T12:34:56Z",
            "payload": payload,
        });
        std::fs::write(
            plans_dir.join(format!("{plan_id}.json")),
            serde_json::to_vec_pretty(&legacy)?,
        )?;

        let plan = read_plan(dir.path(), &plan_id)?;
        assert!(
            plan.principal.is_none(),
            "legacy file has no stamped principal"
        );
        assert_eq!(
            plan.resolved_principal(),
            PolicyPrincipal::Agent,
            "an unstamped ai_authored plan MUST resolve to agent, never human"
        );
        Ok(())
    }

    /// A legacy non-AI plan (e.g. a `run` plan) with no principal resolves to
    /// `human` — humans are never gated in v0, so this is the safe default for
    /// the plan kinds that predate agent authorship.
    #[test]
    fn legacy_run_plan_with_no_principal_resolves_to_human() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let plans_dir = dir.path().join(".rocky").join("plans");
        std::fs::create_dir_all(&plans_dir)?;
        let payload = serde_json::json!({"models": ["db.s.t"]});
        let plan_id = compute_plan_id(&PlanKind::Run, &payload);
        let legacy = serde_json::json!({
            "plan_id": plan_id,
            "kind": "run",
            "created_at": "2026-05-15T12:34:56Z",
            "payload": payload,
        });
        std::fs::write(
            plans_dir.join(format!("{plan_id}.json")),
            serde_json::to_vec_pretty(&legacy)?,
        )?;

        let plan = read_plan(dir.path(), &plan_id)?;
        assert_eq!(plan.resolved_principal(), PolicyPrincipal::Human);
        Ok(())
    }

    /// `write_plan` stamps the kind-derived principal explicitly: an
    /// `ai_authored` plan is stamped `agent`, a `run` plan `human`.
    #[test]
    fn write_plan_stamps_kind_derived_principal() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "db.s.t",
            statement_count: 1,
        };
        let ai_id = write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;
        let run_id = write_plan(dir.path(), PlanKind::Run, &payload)?;
        assert_eq!(
            read_plan(dir.path(), &ai_id)?.principal,
            Some(PolicyPrincipal::Agent)
        );
        assert_eq!(
            read_plan(dir.path(), &run_id)?.principal,
            Some(PolicyPrincipal::Human)
        );
        Ok(())
    }

    /// A governed write embeds the capability classification in the payload
    /// (so it rides inside `plan_id`), stamps the given principal outside the
    /// hash, and round-trips back through `embedded_capabilities`.
    #[test]
    fn write_plan_governed_embeds_capabilities_in_plan_id() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = serde_json::json!({"models": ["bronze_events"]});

        let mut caps = EmbeddedCapabilities {
            diff_available: true,
            ..Default::default()
        };
        caps.changed.insert(
            "bronze_events".to_string(),
            PolicyCapability::SchemaChangeAdditive,
        );

        let governed_id = write_plan_governed(
            dir.path(),
            PlanKind::AiAuthored,
            &payload,
            PolicyPrincipal::Agent,
            caps.clone(),
        )?;
        // The same payload written UNgoverned hashes differently — the embedded
        // capability is part of the reviewed artifact's id.
        let plain_id = write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;
        assert_ne!(
            governed_id, plain_id,
            "embedding capabilities must change the plan_id"
        );

        let plan = read_plan(dir.path(), &governed_id)?;
        assert_eq!(plan.principal, Some(PolicyPrincipal::Agent));
        assert_eq!(plan.embedded_capabilities(), caps);
        Ok(())
    }

    /// A plan with no embed yields the fail-closed default:
    /// `diff_available = false`, empty `changed`.
    #[test]
    fn embedded_capabilities_absent_fails_closed() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let payload = DummyPayload {
            model: "db.s.t",
            statement_count: 1,
        };
        let plan_id = write_plan(dir.path(), PlanKind::AiAuthored, &payload)?;
        let plan = read_plan(dir.path(), &plan_id)?;
        let caps = plan.embedded_capabilities();
        assert!(!caps.diff_available, "no embed ⇒ fail closed");
        assert!(caps.changed.is_empty());
        Ok(())
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

    // ---------- Phase C — format_version tagging (C-5 / C-7) ----------

    #[test]
    fn write_plan_tags_format_version_one() -> anyhow::Result<()> {
        // `write_plan` always tags the persisted record with
        // `format_version = 1`. Production callers after C-7 are
        // `Run` / `Replication` / `Promote` (whose payloads were
        // never inline-SQL envelopes). The `PlanKind::Compact` here
        // is a fabricated-on-disk plan used by the apply-side test
        // that the compact-apply dispatch rejects v1 compact plans
        // with the migration error.
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
        // `write_plan_v2` tags the persisted record with
        // `format_version = 2`. This is the only loadable shape for
        // `Compact` / `Archive` plans after C-7; the apply path uses
        // the tag to dispatch into the IR-regeneration arm.
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
        // file as `format_version = 1` so it parses cleanly. After C-7
        // the per-kind apply dispatch (compact/archive) rejects these
        // files with a migration error — but the reader itself is
        // format-agnostic, so this round-trip stays load-clean for
        // tests + diagnostics + future migration tooling.
        let dir = tempfile::tempdir()?;
        let plans_dir = dir.path().join(".rocky").join("plans");
        std::fs::create_dir_all(&plans_dir)?;
        // Use a hash-consistent plan_id for the payload so the integrity check
        // passes — this test exercises the `format_version` default, not a
        // tamper case. (Pre-integrity-check this used a fabricated all-`a` id.)
        let payload = serde_json::json!({"dummy": true});
        let plan_id = compute_plan_id(&PlanKind::Compact, &payload);
        let legacy_json = serde_json::json!({
            "plan_id": plan_id,
            "kind": "compact",
            "created_at": "2026-05-15T12:34:56Z",
            "payload": payload
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
        // The two writers serialize different payload bytes for the
        // same logical plan, so the blake3 digest — and the on-disk
        // filename — disambiguate the formats. The `write_plan`
        // (v1-tagged) compact plan in this test is a fabricated-on-disk
        // shape used to drive the apply-side rejection test; production
        // compact callers go through `write_plan_v2` only.
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
