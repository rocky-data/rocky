//! The opt-in content-addressed **column-level skip** decision.
//!
//! This is the *sound* half of the per-column skip substrate: "an upstream was
//! rebuilt, but the specific columns this downstream actually consumes did NOT
//! change → skip the downstream." It is the one decision nothing else in the
//! engine can make soundly — the whole-output blake3 is table-granular, the
//! breaking-change classifier is schema-only, and the freshness gate is
//! watermark-only. Per-column content hashes over column *values* are what
//! defeat the documented trap: a schema-stable `WHERE`/`JOIN`/`CASE` rewrite
//! upstream that keeps the type byte-identical while moving the data still
//! flips the consumed column's content hash ⇒ build.
//!
//! # Why this lives on the content-addressed surface, not the plain skip gate
//!
//! The two skip surfaces must not double-decide. The plain
//! [`super::skip_gate`] adjudicates *plain*-strategy models (its `is_eligible`
//! deliberately excludes `content_addressed`); the content-addressed runner
//! ([`super::run_content_addressed`]) adjudicates *content-addressed* models
//! (byte-level point-to reuse vs build). The column-level skip is a **third
//! outcome of the content-addressed surface**, evaluated by the caller for a
//! content-addressed model *before* the point-to reuse / build path: either it
//! column-skips, or it falls through to that path. The two never both act on
//! one model. This placement is forced, not chosen: the consumer-side baseline
//! ([`rocky_core::state::UpstreamSig::consumed_column_hashes`]) and the
//! producer output hashes ([`rocky_core::state::ModelExecution::output_column_hashes`])
//! are only ever recorded for content-addressed models, so a column-level skip
//! can only ever be decided for a content-addressed downstream.
//!
//! # The load-bearing safety rule
//!
//! A wrong skip is **silent production staleness** — the worst failure a
//! transformation engine can have. So, like the plain gate, this decision is:
//!
//! - **default-OFF** (behind `[reuse] column_level = false`);
//! - **fail-safe** — *every* missing, partial, or mismatched input resolves to
//!   **build**. There is exactly one code path that yields [`ColumnSkipVerdict::Skip`],
//!   and it requires every clause below to hold;
//! - **over-sensitive by construction** (design Fork 4) — the per-column hash
//!   is over the column's written values *as stored*, with no canonicalisation.
//!   A row-order, encoding, or NULL-render difference makes the hash differ
//!   even when the logical content is identical, so the decision errs toward a
//!   redundant **build**, never a wrong skip. A concrete, intended consequence:
//!   an **order-unstable producer never skips** — its column hash changes every
//!   run, so a consumer's comparison never matches. That inertness is accepted,
//!   not "fixed" (normalising columns would add cost and a new correctness
//!   surface); the win lands for order-stable content-addressed producers,
//!   which is the scoped target.
//!
//! # Clauses (skip IFF all hold)
//!
//! The caller pre-gates the shape (feature enabled, content-addressed,
//! unpartitioned, a prior *successful* build exists) and resolves the inputs;
//! [`decide_column_skip`] then requires, in order:
//!
//! - (1) the model's SQL is provably **deterministic** — else recomputing it
//!   need not reproduce the prior output even with identical inputs;
//! - (2) the model's **logic is unchanged** — its recipe hash (canonical
//!   `ModelIr`) matches the prior build's (the content-addressed analog of the
//!   plain gate's B2 logic-key clause; content-addressed models record a
//!   `recipe_hash`, not a `skip_hash`);
//! - (3) the **environment is unchanged** — its env hash (engine + adapter
//!   identity) matches the prior build's, so "recompute equals prior" is not
//!   silently invalidated by an engine/dialect change (a conservative extra
//!   clause — its only failure direction is a safe rebuild);
//! - (4) the **consumed-column set is provably complete** (design Fork 1 — the
//!   [`rocky_sql::consumed_columns`] guard, surfaced here as a present
//!   `current_baseline`); a `None` means it could not be enumerated ⇒ build;
//! - (5) a **prior consumed-column baseline exists** to compare against;
//! - (6) **every consumed column of every consumed upstream matches** the
//!   prior baseline exactly (design Fork 2/3 — consumer-side, local, from two
//!   stored snapshots plus this run; a built upstream is evaluated per this
//!   downstream, not as one blanket verdict). Any absent, partial, or moved
//!   hash ⇒ build.

use std::collections::BTreeMap;

use rocky_core::state::{ColumnHash, UpstreamSig};

/// Why a content-addressed model must BUILD rather than column-skip — one
/// variant per fail-closed clause.
///
/// Every variant is a *safe* outcome: BUILD always produces correct output.
/// The variants exist for observability (logging *why* a column-skip was
/// declined), never for control flow beyond "do not skip". Mirrors the
/// fail-closed [`super::reuse_decision::BuildReason`] discipline: the reason is
/// recorded at the decision point, so the surfaced justification cannot drift
/// from what the decision did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSkipReason {
    /// (1) the model's SQL is not provably deterministic, so recomputing it
    /// need not reproduce the prior output even with identical inputs.
    NotDeterministic,
    /// (2) the model's logic (canonical `ModelIr` recipe hash) changed since
    /// the last build — the content-addressed analog of the plain gate's B2.
    LogicChanged,
    /// (3) the engine/adapter environment (env hash) changed since the last
    /// build, so "recompute equals prior" can no longer be assumed.
    EnvChanged,
    /// (4) the consumed-column set could not be proven exhaustive (a `SELECT *`,
    /// CTE, sub-query, ambiguous reference, or any shape the completeness guard
    /// rejects) — the column-level analog of "upstreams not enumerable".
    ConsumedColumnsNotEnumerable,
    /// (5) no prior consumed-column baseline exists to compare against (a build
    /// predating per-column hashing, a build where the guard failed, or an
    /// upstream whose producer hashes were unavailable).
    NoPriorColumnBaseline,
    /// (6) the model provably consumes no upstream *column* (a source-less
    /// query, or every read resolved to a raw source with no column hash) —
    /// there is nothing whose stability could be proven, so fail closed.
    NoConsumedColumns,
    /// (6) a consumed upstream has no column hash *this run* — it was not built
    /// content-addressed, was partitioned, or resolved to a raw source — so its
    /// stability can't be proven.
    MissingCurrentColumnHash,
    /// (6) a consumed column's content hash changed since the last build — the
    /// headline case the value hash exists to catch (incl. the over-sensitive
    /// row-order/encoding differences that also, safely, force a build).
    ConsumedColumnChanged,
}

impl ColumnSkipReason {
    /// Stable lowercase token for structured logs / programmatic branching.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ColumnSkipReason::NotDeterministic => "not_deterministic",
            ColumnSkipReason::LogicChanged => "logic_changed",
            ColumnSkipReason::EnvChanged => "env_changed",
            ColumnSkipReason::ConsumedColumnsNotEnumerable => "consumed_columns_not_enumerable",
            ColumnSkipReason::NoPriorColumnBaseline => "no_prior_column_baseline",
            ColumnSkipReason::NoConsumedColumns => "no_consumed_columns",
            ColumnSkipReason::MissingCurrentColumnHash => "missing_current_column_hash",
            ColumnSkipReason::ConsumedColumnChanged => "consumed_column_changed",
        }
    }

    /// Short human-readable justification for the run output's per-model
    /// `reason` field on a declined column-skip.
    #[must_use]
    pub fn message(self) -> &'static str {
        match self {
            ColumnSkipReason::NotDeterministic => {
                "model SQL is not provably deterministic — cannot prove a recompute is identical"
            }
            ColumnSkipReason::LogicChanged => "model logic changed since last build",
            ColumnSkipReason::EnvChanged => "engine/adapter environment changed since last build",
            ColumnSkipReason::ConsumedColumnsNotEnumerable => {
                "consumed columns not provably enumerable (e.g. SELECT *, CTE, or subquery)"
            }
            ColumnSkipReason::NoPriorColumnBaseline => {
                "no prior consumed-column baseline to compare against"
            }
            ColumnSkipReason::NoConsumedColumns => "no provable consumed upstream columns",
            ColumnSkipReason::MissingCurrentColumnHash => {
                "a consumed upstream has no column hash this run"
            }
            ColumnSkipReason::ConsumedColumnChanged => {
                "a consumed column's content changed since last build"
            }
        }
    }
}

/// The single honest justification for a column-skip. There is exactly one
/// skip path (all clauses held), so this is a constant rather than an enum
/// variant — the caller stamps it on the per-model decision.
pub const SKIP_MESSAGE: &str =
    "upstream changed but the consumed columns are unchanged since last build";

/// The fail-closed verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnSkipVerdict {
    /// Do not column-skip — fall through to the content-addressed reuse/build
    /// path. Carries the first-failing clause for observability.
    Build(ColumnSkipReason),
    /// Every clause held — the consumed columns are provably unchanged, so the
    /// downstream's output would be identical to its last successful build. The
    /// caller skips re-materialization and keeps the prior state authoritative.
    Skip,
}

/// Already-resolved inputs to the **pure** decision. Every field is gathered by
/// the caller (config read, SQL determinism scan, recipe/env recompute, a
/// state-store read of the prior build, and the current consumed-column
/// baseline computed the same way it is *recorded*), so the decision logic
/// itself does no I/O and is fully unit-testable.
///
/// Field order mirrors clause order; an earlier-clause failure short-circuits
/// before a later clause is consulted.
pub struct ColumnSkipInputs<'a> {
    /// (1) the model's SQL is provably deterministic.
    pub deterministic: bool,
    /// (2) the model's recipe hash equals the prior build's.
    pub recipe_unchanged: bool,
    /// (3) the model's env hash equals the prior build's.
    pub env_unchanged: bool,
    /// (4) the consumed-column baseline for *this* run — the columns this model
    /// provably consumes from each upstream, paired with the upstream's current
    /// output-column hashes (built this run, or the prior build's hashes for an
    /// upstream that was itself skipped). `None` when the consumed set could not
    /// be proven complete (design Fork 1 ⇒ build).
    pub current_baseline: Option<&'a [UpstreamSig]>,
    /// (5) the consumed-column baseline this model recorded at its last
    /// successful build (the consumer-side snapshot it is compared against —
    /// design Fork 2). `None` when no such baseline exists ⇒ build.
    pub prior_baseline: Option<&'a [UpstreamSig]>,
}

/// Decide — purely — whether a content-addressed model may column-skip.
///
/// Evaluates the clauses in [module-doc](self) order; the **first** failing
/// clause yields its [`ColumnSkipReason`]. Only an all-pass produces
/// [`ColumnSkipVerdict::Skip`]. Performs **no I/O**: the caller resolves every
/// input first, keeping this trust-critical logic small and exhaustively
/// testable — each clause's false branch is a unit test, and the all-true
/// branch is a unit test.
#[must_use]
pub fn decide_column_skip(inputs: &ColumnSkipInputs<'_>) -> ColumnSkipVerdict {
    use ColumnSkipReason as R;
    use ColumnSkipVerdict::{Build, Skip};

    // (1) determinism.
    if !inputs.deterministic {
        return Build(R::NotDeterministic);
    }
    // (2) logic unchanged (B2 analog).
    if !inputs.recipe_unchanged {
        return Build(R::LogicChanged);
    }
    // (3) environment unchanged.
    if !inputs.env_unchanged {
        return Build(R::EnvChanged);
    }
    // (4) consumed set provably complete.
    let Some(current) = inputs.current_baseline else {
        return Build(R::ConsumedColumnsNotEnumerable);
    };
    // (5) a prior baseline to compare against.
    let Some(prior) = inputs.prior_baseline else {
        return Build(R::NoPriorColumnBaseline);
    };
    // A model that provably consumes no upstream column has nothing whose
    // stability we could prove — fail closed rather than skip vacuously.
    if current.is_empty() {
        return Build(R::NoConsumedColumns);
    }

    // (6) every consumed upstream's every consumed column matches the prior
    // baseline exactly. Any absent current hash, any upstream missing from the
    // prior baseline, any moved hash ⇒ build. This is where a built upstream is
    // adjudicated *for this downstream* (design Fork 3) rather than as one
    // blanket verdict.
    for cur in current {
        let Some(cur_hashes) = cur.consumed_column_hashes.as_ref() else {
            return Build(R::MissingCurrentColumnHash);
        };
        let Some(prior_sig) = prior.iter().find(|p| p.upstream_key == cur.upstream_key) else {
            // A consumed upstream with no prior baseline entry — treat as a
            // changed input (the set of upstreams this model reads shifted).
            return Build(R::ConsumedColumnChanged);
        };
        let Some(prior_hashes) = prior_sig.consumed_column_hashes.as_ref() else {
            return Build(R::NoPriorColumnBaseline);
        };
        if !column_hashes_equal(cur_hashes, prior_hashes) {
            return Build(R::ConsumedColumnChanged);
        }
    }

    // Symmetric coverage: a prior consumed upstream missing from the current
    // baseline means the consumed set shrank — a logic change the recipe hash
    // *should* already have caught, but a red surface never trusts a single
    // signal. Fail closed.
    for prior_sig in prior {
        if prior_sig.consumed_column_hashes.is_some()
            && !current
                .iter()
                .any(|c| c.upstream_key == prior_sig.upstream_key)
        {
            return Build(R::ConsumedColumnChanged);
        }
    }

    Skip
}

/// Two consumed-column hash sets are equal iff they cover the **same columns**
/// with the **same hashes**. Keyed case-insensitively on the column name (the
/// consumed set is lowercased while a producer keeps the original Arrow field
/// name), so a case-only rename is not mistaken for a change. A differing set
/// of columns, or any single moved hash, is inequality ⇒ the caller builds.
fn column_hashes_equal(a: &[ColumnHash], b: &[ColumnHash]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let index = |hs: &[ColumnHash]| -> BTreeMap<String, String> {
        hs.iter()
            .map(|c| (c.column.to_lowercase(), c.hash.clone()))
            .collect()
    };
    let am = index(a);
    // Distinct-column guarantee: if two entries collapsed on a case-insensitive
    // key, the maps would be shorter than the slices and could compare equal
    // spuriously. Require the map to preserve every entry on both sides.
    if am.len() != a.len() {
        return false;
    }
    let bm = index(b);
    if bm.len() != b.len() {
        return false;
    }
    am == bm
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ch(column: &str, hash: &str) -> ColumnHash {
        ColumnHash {
            column: column.to_string(),
            hash: hash.to_string(),
        }
    }

    /// A consumer baseline entry for `upstream` carrying the given consumed
    /// column hashes.
    fn sig(upstream: &str, cols: &[(&str, &str)]) -> UpstreamSig {
        UpstreamSig {
            upstream_key: upstream.to_string(),
            max_ts: None,
            row_count: None,
            consumed_column_hashes: Some(cols.iter().map(|(c, h)| ch(c, h)).collect()),
        }
    }

    /// The all-clauses-hold baseline the individual tests perturb.
    fn base<'a>(current: &'a [UpstreamSig], prior: &'a [UpstreamSig]) -> ColumnSkipInputs<'a> {
        ColumnSkipInputs {
            deterministic: true,
            recipe_unchanged: true,
            env_unchanged: true,
            current_baseline: Some(current),
            prior_baseline: Some(prior),
        }
    }

    #[test]
    fn all_unchanged_skips() {
        let current = vec![sig("cat.sch.u", &[("a", "h1"), ("b", "h2")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1"), ("b", "h2")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Skip
        );
    }

    /// THE TRAP: a schema-stable value change to a *consumed* column moves its
    /// content hash ⇒ BUILD. The case the schema-diff classifier is blind to.
    #[test]
    fn consumed_column_value_change_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "MOVED"), ("b", "h2")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1"), ("b", "h2")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    /// The win: a change confined to a column this model does NOT consume never
    /// enters the consumed baseline (the recorder filters to `consumed(D,U)`),
    /// so the consumed columns still match ⇒ SKIP. Modelled here by identical
    /// consumed baselines — the non-consumed column is simply absent from both.
    #[test]
    fn non_consumed_column_change_skips() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Skip
        );
    }

    /// Over-sensitivity is the safe direction (design Fork 4): an order-unstable
    /// producer's column hash changes every run, so the comparison never matches
    /// ⇒ BUILD. Documents the accepted inertness — the feature never skips such a
    /// producer, it does not silently skip it.
    #[test]
    fn order_unstable_producer_never_skips() {
        // Same logical content, different stored order ⇒ different hash.
        let current = vec![sig("cat.sch.u", &[("a", "hash_order_v2")])];
        let prior = vec![sig("cat.sch.u", &[("a", "hash_order_v1")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    #[test]
    fn not_deterministic_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1")])];
        let mut inputs = base(&current, &prior);
        inputs.deterministic = false;
        assert_eq!(
            decide_column_skip(&inputs),
            ColumnSkipVerdict::Build(ColumnSkipReason::NotDeterministic)
        );
    }

    #[test]
    fn logic_changed_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1")])];
        let mut inputs = base(&current, &prior);
        inputs.recipe_unchanged = false;
        assert_eq!(
            decide_column_skip(&inputs),
            ColumnSkipVerdict::Build(ColumnSkipReason::LogicChanged)
        );
    }

    #[test]
    fn env_changed_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1")])];
        let mut inputs = base(&current, &prior);
        inputs.env_unchanged = false;
        assert_eq!(
            decide_column_skip(&inputs),
            ColumnSkipVerdict::Build(ColumnSkipReason::EnvChanged)
        );
    }

    #[test]
    fn consumed_columns_not_enumerable_builds() {
        let prior = vec![sig("cat.sch.u", &[("a", "h1")])];
        let mut inputs = base(&prior, &prior);
        inputs.current_baseline = None;
        assert_eq!(
            decide_column_skip(&inputs),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnsNotEnumerable)
        );
    }

    #[test]
    fn no_prior_baseline_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let mut inputs = base(&current, &current);
        inputs.prior_baseline = None;
        assert_eq!(
            decide_column_skip(&inputs),
            ColumnSkipVerdict::Build(ColumnSkipReason::NoPriorColumnBaseline)
        );
    }

    #[test]
    fn empty_consumed_set_builds() {
        let current: Vec<UpstreamSig> = vec![];
        let prior: Vec<UpstreamSig> = vec![];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::NoConsumedColumns)
        );
    }

    /// A consumed upstream that resolved to a raw source (no column hash this
    /// run) can't be proven stable ⇒ BUILD.
    #[test]
    fn missing_current_column_hash_builds() {
        let current = vec![UpstreamSig {
            upstream_key: "raw_source".to_string(),
            max_ts: None,
            row_count: None,
            consumed_column_hashes: None,
        }];
        let prior = vec![sig("raw_source", &[("a", "h1")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::MissingCurrentColumnHash)
        );
    }

    /// A consumed upstream absent from the prior baseline is a changed input.
    #[test]
    fn upstream_absent_from_prior_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.other", &[("a", "h1")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    /// A prior consumed upstream missing from the current baseline (the
    /// consumed set shrank) fails the symmetric-coverage check ⇒ BUILD.
    #[test]
    fn prior_has_extra_consumed_upstream_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![
            sig("cat.sch.u", &[("a", "h1")]),
            sig("cat.sch.v", &[("x", "h9")]),
        ];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    /// Multi-upstream: one consumed upstream unchanged, the other's consumed
    /// column moved ⇒ BUILD (per-upstream evaluation; any changed consumed
    /// upstream forces a build).
    #[test]
    fn multi_upstream_one_changed_builds() {
        let current = vec![
            sig("cat.sch.u1", &[("a", "h1")]),
            sig("cat.sch.u2", &[("b", "MOVED")]),
        ];
        let prior = vec![
            sig("cat.sch.u1", &[("a", "h1")]),
            sig("cat.sch.u2", &[("b", "h2")]),
        ];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    /// A consumed column present in the prior baseline but absent this run
    /// (differing column set) is inequality ⇒ BUILD.
    #[test]
    fn differing_consumed_column_set_builds() {
        let current = vec![sig("cat.sch.u", &[("a", "h1")])];
        let prior = vec![sig("cat.sch.u", &[("a", "h1"), ("b", "h2")])];
        assert_eq!(
            decide_column_skip(&base(&current, &prior)),
            ColumnSkipVerdict::Build(ColumnSkipReason::ConsumedColumnChanged)
        );
    }

    /// Every `ColumnSkipReason` carries a stable, distinct log token and a
    /// non-empty human message. Guards against a silent reason-table drift (a
    /// trust-sensitive surface: a misleading reason is worse than none).
    #[test]
    fn column_skip_reason_tables_are_stable_and_distinct() {
        let all = [
            ColumnSkipReason::NotDeterministic,
            ColumnSkipReason::LogicChanged,
            ColumnSkipReason::EnvChanged,
            ColumnSkipReason::ConsumedColumnsNotEnumerable,
            ColumnSkipReason::NoPriorColumnBaseline,
            ColumnSkipReason::NoConsumedColumns,
            ColumnSkipReason::MissingCurrentColumnHash,
            ColumnSkipReason::ConsumedColumnChanged,
        ];
        let mut tokens = std::collections::HashSet::new();
        for r in all {
            assert!(!r.as_str().is_empty(), "as_str must be non-empty");
            assert!(!r.message().is_empty(), "message must be non-empty");
            assert!(
                tokens.insert(r.as_str()),
                "as_str token must be unique: {}",
                r.as_str()
            );
        }
        // Pin the load-bearing reason wording so a careless edit trips here.
        assert_eq!(
            ColumnSkipReason::ConsumedColumnChanged.message(),
            "a consumed column's content changed since last build"
        );
        assert!(!SKIP_MESSAGE.is_empty());
    }
}
