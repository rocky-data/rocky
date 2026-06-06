//! Fail-closed reuse decision (B5 capstone) — the gate that decides whether a
//! content-addressed model may **point-to** a prior run `R`'s already-written
//! parquet instead of executing its SQL.
//!
//! # The trust rule
//!
//! A wrong reuse is a *silent wrong production output* — the same blast radius
//! as a wrong skip. So the gate is **fail-closed**: a model reuses `R`'s output
//! **iff every** clause below holds, and **any** false answer or error means
//! **BUILD** (execute the SQL). There is no "probably fine" branch.
//!
//! # The six clauses (all must hold to reuse)
//!
//! 1. **Enabled.** `[reuse]` is on in config AND the run was not invoked with
//!    `--no-reuse`. (Evaluated by the caller; passed in as
//!    [`ReuseInputs::enabled`].)
//! 2. **Eligible shape.** The model is content-addressed, **unpartitioned**,
//!    and **non-rowTracking** — the only shape the point-to writer supports
//!    today (a partitioned/rowTracking point-to needs per-group commits /
//!    re-allocated `baseRowId` ranges; deferred). ([`ReuseInputs::eligible_shape`].)
//! 3. **Input match.** This run's recomputed `input_hash` (its resolved
//!    upstream content identities ‖ `skip_hash` ‖ target) hits the
//!    `INPUT_INDEX`, yielding a candidate prior run `R`
//!    ([`ReuseInputs::candidate`]). No hit ⇒ BUILD.
//! 4. **Strong proof.** `R`'s `proof_class` is exactly `"strong"`. A
//!    `"heuristic"` (or any other) class is a watermark/freshness attestation,
//!    **not** a byte-identity claim — never point-to on it.
//! 5. **Artifact resolves.** `R`'s recorded output blake3 resolves to an
//!    [`ArtifactRecord`] (a `(blake3, path, commit_version)` we can locate) and
//!    its refcount is **sane** (`>= 1` — `R`'s own row exists; the reusing run
//!    adds the second). ([`ReuseInputs::artifact`] + [`ReuseInputs::refcount`].)
//! 6. **Liveness.** `R`'s `add` action is **still live** in the *target's
//!    current `_delta_log`* — not VACUUM'd, compacted, or superseded by a later
//!    `remove`. If the file is gone or the log can't be read, BUILD.
//!    ([`ReuseInputs::add_is_live`].)
//!
//! # Posture — wired to the live point-to
//!
//! On a positive [`ReuseVerdict::WouldReuse`] the runner performs a real
//! **zero-copy point-to**: it commits a pointer to `R`'s existing parquet via
//! `commit_pointer_with_state` and **skips the model's SQL entirely**. Any
//! doubt while recovering or re-confirming `R`'s bytes (the commit-time
//! liveness re-check, a basename or blake3 mismatch) falls closed to BUILD, so
//! a wrong point-to can never reach production. Gated default-OFF behind
//! `[reuse]` (clause 1); see `run_content_addressed::attempt_point_to_reuse`
//! for the point-to itself.

use rocky_core::state::{ArtifactRecord, InputIndexEntry};

/// Why a model must BUILD rather than reuse — one variant per fail-closed
/// clause (and the error/doubt cases that also force a build).
///
/// Every variant is a *safe* outcome: BUILD always produces correct output.
/// The variants exist for observability (logging *why* a reuse was declined),
/// not for control flow beyond "do not reuse".
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BuildReason {
    /// Clause 1 — `[reuse]` disabled or `--no-reuse` passed. The default.
    Disabled,
    /// Clause 2 — the model is partitioned, rowTracking, or otherwise not the
    /// supported unpartitioned content-addressed shape.
    IneligibleShape,
    /// Clause 3 — this run's `input_hash` could not be computed (the read set
    /// did not resolve to an all-strong content-hash chain: a raw source, a
    /// mixed/heuristic input, an ambiguous read, or a non-canonicalisable
    /// model). No candidate was even looked up.
    NoInputHash,
    /// Clause 3 — `input_hash` was computed but missed the `INPUT_INDEX`: no
    /// prior run produced these exact declared inputs for this target.
    NoIndexMatch,
    /// Clause 4 — the candidate `R`'s `proof_class` is not `"strong"`.
    NotStrong,
    /// Clause 5 — `R` recorded no output blake3 (nothing to point at).
    NoRecordedOutput,
    /// Clause 5 — `R`'s output blake3 resolved to no [`ArtifactRecord`] (the
    /// ledger row is gone) — cannot locate the bytes to reference.
    ArtifactUnresolved,
    /// Clause 5 — refcount sanity failed (`< 1`): `R`'s own ledger row is
    /// missing, so a point-to would not be refcount-protected against VACUUM.
    RefcountInsufficient,
    /// Clause 6 — `R`'s `add` is no longer live in the target's `_delta_log`
    /// (VACUUM'd / superseded by a `remove`). Pointing at it would reference
    /// removed bytes.
    NotLive,
}

impl BuildReason {
    /// Lowercase, stable string for structured logs.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            BuildReason::Disabled => "disabled",
            BuildReason::IneligibleShape => "ineligible_shape",
            BuildReason::NoInputHash => "no_input_hash",
            BuildReason::NoIndexMatch => "no_index_match",
            BuildReason::NotStrong => "not_strong",
            BuildReason::NoRecordedOutput => "no_recorded_output",
            BuildReason::ArtifactUnresolved => "artifact_unresolved",
            BuildReason::RefcountInsufficient => "refcount_insufficient",
            BuildReason::NotLive => "not_live",
        }
    }
}

/// A fully-validated reuse candidate — every clause passed. Carries exactly
/// what the live point-to invocation needs (`R`'s commit, blake3, path, and
/// the resolved artifact) plus the proof class for the reuse-provenance entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReuseCandidate {
    /// Prior run `R` whose bytes would be reused.
    pub run_id: String,
    /// `R`'s output blake3 (hex) — the shared content hash the reusing run's
    /// fresh `ArtifactRecord` is recorded at, driving refcount `>= 2`.
    pub blake3_hash: String,
    /// `R`'s parquet object-store path (relative form lives in the `add`).
    pub file_path: String,
    /// Delta commit version of `R`'s `add` — the direct pointer the
    /// point-to writer GETs to lift the `add` verbatim.
    pub commit_version: u64,
    /// Parquet size in bytes (from `R`'s `ArtifactRecord`).
    pub size_bytes: u64,
    /// `R`'s proof class — always `"strong"` here (clause 4), carried through
    /// to the reuse-provenance record so the label is never lost.
    pub proof_class: String,
}

/// The fail-closed verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReuseVerdict {
    /// Execute the SQL. Carries the reason for observability.
    Build(BuildReason),
    /// Every clause passed — `R`'s bytes are reusable. The runner acts on this
    /// by committing a zero-copy point-to to `R`'s parquet and skipping the
    /// SQL, falling closed to BUILD on any recovery/re-confirmation doubt.
    WouldReuse(ReuseCandidate),
}

/// Already-resolved inputs to the **pure** decision. Every field is gathered
/// by the caller (config read, shape inspection, state-store lookups, the
/// writer liveness check) so the decision logic itself does no I/O and is
/// fully unit-testable with stubs.
///
/// The ordering of the fields mirrors clause order. A `None`/empty in an
/// earlier-clause field short-circuits to BUILD before a later clause is even
/// consulted — so the caller may pass `None` for fields it never had to fetch
/// (e.g. it need not run the liveness GET when there was no index match).
pub struct ReuseInputs<'a> {
    /// Clause 1 — `[reuse]` enabled AND not `--no-reuse`.
    pub enabled: bool,
    /// Clause 2 — content-addressed, unpartitioned, non-rowTracking.
    pub eligible_shape: bool,
    /// Clause 3 — this run's recomputed `input_hash`, or `None` when the read
    /// set did not resolve to an all-strong chain (so no lookup was possible).
    pub input_hash: Option<&'a str>,
    /// Clause 3 — the `INPUT_INDEX` hit for [`Self::input_hash`], or `None` on
    /// a miss.
    pub candidate: Option<&'a InputIndexEntry>,
    /// Clause 5 — the [`ArtifactRecord`] for `R`'s output blake3, or `None`
    /// when the ledger row could not be resolved.
    pub artifact: Option<&'a ArtifactRecord>,
    /// Clause 5 — refcount of `R`'s output blake3 in the ledger. `None` when
    /// the caller short-circuited before counting.
    pub refcount: Option<u64>,
    /// Clause 6 — whether `R`'s `add` is still live in the target's
    /// `_delta_log`. `None` when the caller short-circuited before checking.
    pub add_is_live: Option<bool>,
}

/// Decide — purely — whether a content-addressed model may reuse a prior run.
///
/// Evaluates clauses 1–6 in order; the **first** failing clause yields its
/// [`BuildReason`]. Only an all-pass produces [`ReuseVerdict::WouldReuse`].
///
/// This function performs **no I/O**: the caller resolves every input first.
/// That keeps the trust-critical logic small and exhaustively testable — each
/// clause's false branch is a unit test, and the all-true branch is a unit
/// test, with the index + writer stubbed.
#[must_use]
pub fn decide_reuse(inputs: &ReuseInputs<'_>) -> ReuseVerdict {
    // Clause 1 — enabled.
    if !inputs.enabled {
        return ReuseVerdict::Build(BuildReason::Disabled);
    }
    // Clause 2 — eligible shape.
    if !inputs.eligible_shape {
        return ReuseVerdict::Build(BuildReason::IneligibleShape);
    }
    // Clause 3 — input hash + index match.
    if inputs.input_hash.is_none() {
        return ReuseVerdict::Build(BuildReason::NoInputHash);
    }
    let Some(candidate) = inputs.candidate else {
        return ReuseVerdict::Build(BuildReason::NoIndexMatch);
    };
    // Clause 4 — strong proof only. Never point-to on a heuristic/watermark
    // match: it attests freshness, not byte-identity.
    if candidate.proof_class != "strong" {
        return ReuseVerdict::Build(BuildReason::NotStrong);
    }
    // Clause 5 — artifact resolves + refcount sane. `R` must have recorded an
    // output blake3 to point at.
    let Some(blake3_hash) = candidate.output_blake3.first() else {
        return ReuseVerdict::Build(BuildReason::NoRecordedOutput);
    };
    let Some(artifact) = inputs.artifact else {
        return ReuseVerdict::Build(BuildReason::ArtifactUnresolved);
    };
    // The resolved artifact must be the very blake3 the index entry names —
    // a defence-in-depth guard so a caller bug can never point at the wrong
    // bytes.
    if &artifact.blake3_hash != blake3_hash {
        return ReuseVerdict::Build(BuildReason::ArtifactUnresolved);
    }
    // Refcount sanity: `R`'s own ledger row must exist (>= 1). The reusing run
    // records the second reference, taking it to >= 2, which is what keeps the
    // shared bytes VACUUM-safe by construction.
    match inputs.refcount {
        Some(n) if n >= 1 => {}
        _ => return ReuseVerdict::Build(BuildReason::RefcountInsufficient),
    }
    // Clause 6 — liveness. Any doubt (None) is fail-closed to BUILD.
    if inputs.add_is_live != Some(true) {
        return ReuseVerdict::Build(BuildReason::NotLive);
    }

    ReuseVerdict::WouldReuse(ReuseCandidate {
        run_id: candidate.run_id.clone(),
        blake3_hash: blake3_hash.clone(),
        file_path: artifact.file_path.clone(),
        commit_version: artifact.commit_version,
        size_bytes: artifact.size_bytes,
        proof_class: candidate.proof_class.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn index_entry(proof_class: &str, blake3: &str) -> InputIndexEntry {
        InputIndexEntry {
            input_hash: "ih-1".to_string(),
            run_id: "run-R".to_string(),
            model_name: "fct_orders".to_string(),
            output_blake3: if blake3.is_empty() {
                vec![]
            } else {
                vec![blake3.to_string()]
            },
            output_path: vec!["s3://b/p/out.parquet".to_string()],
            proof_class: proof_class.to_string(),
            recorded_at: Utc::now(),
        }
    }

    fn artifact(blake3: &str) -> ArtifactRecord {
        ArtifactRecord {
            blake3_hash: blake3.to_string(),
            run_id: "run-R".to_string(),
            model_name: "fct_orders".to_string(),
            file_path: "s3://b/p/out.parquet".to_string(),
            commit_version: 3,
            size_bytes: 4096,
            written_at: Utc::now(),
        }
    }

    /// The all-true fixture: every clause passes. Individual tests below mutate
    /// exactly one field to false and assert the matching BUILD reason.
    fn all_pass<'a>(entry: &'a InputIndexEntry, art: &'a ArtifactRecord) -> ReuseInputs<'a> {
        ReuseInputs {
            enabled: true,
            eligible_shape: true,
            input_hash: Some("ih-1"),
            candidate: Some(entry),
            artifact: Some(art),
            refcount: Some(1),
            add_is_live: Some(true),
        }
    }

    #[test]
    fn all_clauses_true_yields_would_reuse() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let verdict = decide_reuse(&all_pass(&entry, &art));
        match verdict {
            ReuseVerdict::WouldReuse(c) => {
                assert_eq!(c.run_id, "run-R");
                assert_eq!(c.blake3_hash, "out-hash");
                assert_eq!(c.commit_version, 3);
                assert_eq!(c.size_bytes, 4096);
                assert_eq!(c.proof_class, "strong");
            }
            other => panic!("expected WouldReuse, got {other:?}"),
        }
    }

    #[test]
    fn clause1_disabled_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.enabled = false;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::Disabled)
        );
    }

    #[test]
    fn clause2_ineligible_shape_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.eligible_shape = false;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::IneligibleShape)
        );
    }

    #[test]
    fn clause3_no_input_hash_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.input_hash = None;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NoInputHash)
        );
    }

    #[test]
    fn clause3_no_index_match_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.candidate = None;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NoIndexMatch)
        );
    }

    #[test]
    fn clause4_heuristic_builds() {
        let entry = index_entry("heuristic", "out-hash");
        let art = artifact("out-hash");
        let inp = all_pass(&entry, &art);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NotStrong),
            "a heuristic match must never point-to — it attests freshness, not bytes"
        );
    }

    #[test]
    fn clause4_unknown_proof_class_builds() {
        let entry = index_entry("none", "out-hash");
        let art = artifact("out-hash");
        let inp = all_pass(&entry, &art);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NotStrong)
        );
    }

    #[test]
    fn clause5_no_recorded_output_builds() {
        let entry = index_entry("strong", ""); // empty output_blake3
        let art = artifact("out-hash");
        let inp = all_pass(&entry, &art);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NoRecordedOutput)
        );
    }

    #[test]
    fn clause5_artifact_unresolved_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.artifact = None;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::ArtifactUnresolved)
        );
    }

    #[test]
    fn clause5_artifact_hash_mismatch_builds() {
        // The resolved artifact's blake3 disagrees with the index entry's —
        // defence-in-depth: never point at the wrong bytes.
        let entry = index_entry("strong", "out-hash");
        let art = artifact("DIFFERENT-hash");
        let inp = all_pass(&entry, &art);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::ArtifactUnresolved)
        );
    }

    #[test]
    fn clause5_refcount_zero_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.refcount = Some(0);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::RefcountInsufficient)
        );
    }

    #[test]
    fn clause5_refcount_none_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.refcount = None;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::RefcountInsufficient)
        );
    }

    #[test]
    fn clause6_not_live_builds() {
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.add_is_live = Some(false);
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NotLive),
            "a removed/VACUUM'd file must fall back to BUILD"
        );
    }

    #[test]
    fn clause6_liveness_unknown_builds() {
        // A None liveness (e.g. the caller could not read the log) is a doubt
        // ⇒ fail-closed to BUILD.
        let entry = index_entry("strong", "out-hash");
        let art = artifact("out-hash");
        let mut inp = all_pass(&entry, &art);
        inp.add_is_live = None;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::NotLive)
        );
    }

    #[test]
    fn first_failing_clause_wins() {
        // Disabled AND ineligible AND no match: clause 1 (Disabled) reports.
        let mut inp = ReuseInputs {
            enabled: false,
            eligible_shape: false,
            input_hash: None,
            candidate: None,
            artifact: None,
            refcount: None,
            add_is_live: None,
        };
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::Disabled)
        );
        // With clause 1 satisfied, clause 2 (IneligibleShape) reports next.
        inp.enabled = true;
        assert_eq!(
            decide_reuse(&inp),
            ReuseVerdict::Build(BuildReason::IneligibleShape)
        );
    }
}
