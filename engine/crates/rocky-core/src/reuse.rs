//! Input-match spine for auditable reuse — warehouse-agnostic, additive,
//! and **dormant**.
//!
//! This module builds the *input* side of reuse: a pre-execution-style key
//! that identifies a model build by its **declared inputs** rather than by
//! the bytes it produced. It is the missing half of the shipped
//! content-addressed ledger ([`crate::state::ArtifactRecord`]), which records
//! the blake3 of the *output* parquet and so can only be read *after* a build
//! has already run.
//!
//! ## What "input identity" means here
//!
//! For a model `M`, the input-match key folds three things:
//!
//! 1. `M`'s own cosmetic-invariant logic key — [`rocky_ir::ModelIr::skip_hash`]
//!    (normalised SQL + typed structural facts, already including `M`'s
//!    target identity).
//! 2. The target `catalog.schema.table` identity, folded in explicitly so a
//!    match can never point at bytes for a *different* target (it is also
//!    inside `skip_hash`, but folding it explicitly keeps the key robust to
//!    any future change in `skip_hash`'s projection).
//! 3. Each immediate upstream's **input identity**, of two strengths:
//!    - [`UpstreamIdentity::Content`] — the upstream's recorded output blake3
//!      (available only where the upstream itself was written
//!      content-addressed). This is the **strong** signal.
//!    - [`UpstreamIdentity::Watermark`] — the upstream's `MAX(ts)` / rowcount
//!      freshness signal (available everywhere). This is the **heuristic**
//!      signal.
//!
//! ## Proof strength
//!
//! [`ProofClass`] for a model is the **minimum** over its upstreams: a model
//! is [`ProofClass::Strong`] only when *every* upstream contributes a content
//! hash. A single watermark-only upstream degrades the whole model to
//! [`ProofClass::Heuristic`]. A model with no upstreams at all is
//! vacuously [`ProofClass::Strong`] (there is no weaker input to drag it
//! down) — its identity rests entirely on its own logic key.
//!
//! ## What this is NOT (the precise claim)
//!
//! Matching input identity attests that two builds **declared the same
//! inputs and the same logic** — an *input-logic match*. It does **not**
//! assert that re-executing the model would reproduce a given output;
//! determinism is not assumed (see the caveat on
//! [`rocky_ir::ModelIr::skip_hash`]). The byte-identity half of the
//! auditable-reuse claim is carried separately by the output blake3 recorded
//! in the ledger, not by this key.
//!
//! ## Dormant in Stage 1
//!
//! Nothing in this module changes run behavior. The index it backs (see
//! [`crate::state::StateStore`]) is *populated* on a successful run and
//! *read back* only by a future reuse decision — no skip, point-to, or clone
//! happens in Stage 1. Population is gated behind the opt-in `[reuse]` config
//! ([`crate::config::ReuseConfig`]) so the default run path is byte- and
//! cost-identical to before this module existed.

use serde::{Deserialize, Serialize};

/// Strength of a reuse claim, derived as the minimum over a model's
/// upstream input identities.
///
/// Ordering is meaningful: [`Heuristic`](Self::Heuristic) is *weaker* than
/// [`Strong`](Self::Strong). [`ProofClass::min`] uses the derived `Ord`
/// (declaration order: `Heuristic < Strong`) to fold a set of per-upstream
/// strengths into the model's class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProofClass {
    /// At least one upstream input identity is a freshness heuristic
    /// (`MAX(ts)` / rowcount) rather than a content hash. Attests freshness,
    /// not byte-identity.
    Heuristic,
    /// Every upstream input identity is a content hash (the model is on the
    /// content-addressed path end-to-end). The strong claim — offline
    /// byte-verifiable once a reuse backend lands (Stage 2).
    Strong,
}

impl ProofClass {
    /// Fold an iterator of per-upstream strengths into a model's class by
    /// taking the **minimum** (`Heuristic` wins over `Strong`).
    ///
    /// An empty iterator yields [`ProofClass::Strong`]: a model with no
    /// upstreams has no weaker input to degrade it, so its class rests
    /// entirely on its own logic key.
    pub fn min<I>(strengths: I) -> ProofClass
    where
        I: IntoIterator<Item = ProofClass>,
    {
        strengths.into_iter().min().unwrap_or(ProofClass::Strong)
    }

    /// Lowercase string form for logs and persisted records.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            ProofClass::Heuristic => "heuristic",
            ProofClass::Strong => "strong",
        }
    }
}

/// One immediate upstream's contribution to a model's input-match key.
///
/// The two arms map 1:1 to the two proof strengths. The enum is hashed (via
/// its canonical-JSON encoding) into the model's `input_hash`, so a change of
/// *kind* (a content hash replacing a watermark, or vice-versa) — not just a
/// change of value — alters the key. That is intentional: an upstream that
/// gains a content hash is genuinely a different (stronger) input identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UpstreamIdentity {
    /// **Strong.** The upstream's recorded output blake3 from the
    /// content-addressed ledger. Available only where the upstream was
    /// itself written content-addressed.
    Content {
        /// Fully-qualified `catalog.schema.table` identity of the upstream.
        upstream_key: String,
        /// Hex-encoded blake3 of the upstream's output parquet.
        blake3_hash: String,
    },
    /// **Heuristic.** The upstream's freshness signal — latest tracked
    /// timestamp and/or observed row count. Attests freshness, not
    /// byte-identity.
    Watermark {
        /// Fully-qualified `catalog.schema.table` identity of the upstream.
        upstream_key: String,
        /// Latest observed `MAX(ts)` (RFC-3339), when a tracked timestamp
        /// column is available. `None` when only a rowcount was observed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_ts: Option<String>,
        /// Observed `COUNT(*)`, when the rowcount fallback is enabled.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        row_count: Option<u64>,
    },
}

impl UpstreamIdentity {
    /// The proof strength this upstream contributes.
    #[must_use]
    pub fn proof_class(&self) -> ProofClass {
        match self {
            UpstreamIdentity::Content { .. } => ProofClass::Strong,
            UpstreamIdentity::Watermark { .. } => ProofClass::Heuristic,
        }
    }

    /// The upstream's fully-qualified identity key, regardless of arm.
    #[must_use]
    pub fn upstream_key(&self) -> &str {
        match self {
            UpstreamIdentity::Content { upstream_key, .. }
            | UpstreamIdentity::Watermark { upstream_key, .. } => upstream_key,
        }
    }
}

/// Borrowed projection hashed into a model's `input_hash`.
///
/// Serialised through canonical JSON (key-sorted, whitespace-free) so the
/// resulting key is deterministic across runs and machines for the same
/// logical inputs. `upstreams` is sorted by `upstream_key` before hashing so
/// the key is independent of upstream enumeration order.
#[derive(Serialize)]
struct InputHashProjection<'a> {
    /// Version byte; bump to deterministically invalidate every previously
    /// recorded `input_hash` if this projection's shape changes.
    spine_version: u8,
    /// Hex of the model's own [`rocky_ir::ModelIr::skip_hash`].
    skip_hash: &'a str,
    /// The target `catalog.schema.table` identity, folded in explicitly.
    target_identity: &'a str,
    /// Immediate upstream identities, pre-sorted by key.
    upstreams: &'a [UpstreamIdentity],
}

/// Version byte folded into every [`compute_input_hash`] result.
///
/// Bump this whenever the input-hash projection changes in a way that could
/// alter the key for unchanged inputs. The bump deterministically
/// invalidates every previously-stored `input_hash`, so a stale index entry
/// can never be mistaken for a current one across spine versions.
const SPINE_VERSION: u8 = 1;

/// Compute a model's deterministic input-match key.
///
/// Folds the model's own logic key (`skip_hash`), its target identity, and
/// its immediate upstreams' input identities into a single blake3 hash. The
/// upstream set is sorted by key first, so the result is independent of the
/// order in which upstreams were enumerated.
///
/// # Determinism
///
/// Byte-identical inputs yield a byte-identical hash. Changing the model's
/// logic (a different `skip_hash`), any upstream's identity (a different
/// content hash, a moved watermark, or a content↔watermark kind change), or
/// the target identity changes the hash.
///
/// # What it attests
///
/// An *input-logic match* — same declared inputs and logic — only. Not
/// reproduction of any output (see this module's docs).
pub fn compute_input_hash(
    skip_hash: &str,
    target_identity: &str,
    upstreams: &[UpstreamIdentity],
) -> blake3::Hash {
    let mut sorted: Vec<UpstreamIdentity> = upstreams.to_vec();
    sorted.sort_by(|a, b| a.upstream_key().cmp(b.upstream_key()));
    let projection = InputHashProjection {
        spine_version: SPINE_VERSION,
        skip_hash,
        target_identity,
        upstreams: &sorted,
    };
    let canonical = canonical_json(&projection);
    blake3::hash(canonical.as_bytes())
}

/// The proof class for a model given its upstreams (minimum over them).
///
/// Thin wrapper over [`ProofClass::min`] for callers that hold a slice.
#[must_use]
pub fn proof_class_for(upstreams: &[UpstreamIdentity]) -> ProofClass {
    ProofClass::min(upstreams.iter().map(UpstreamIdentity::proof_class))
}

/// Canonical-JSON encoder mirroring [`rocky_ir::ModelIr::canonical_json`]'s
/// key-sorted, whitespace-free form so spine keys are stable across machines.
fn canonical_json<T: Serialize>(value: &T) -> String {
    let raw = serde_json::to_value(value)
        .expect("input-hash projection must be JSON-encodable; programming error");
    let canonical = canonicalize(raw);
    serde_json::to_string(&canonical)
        .expect("BTreeMap-based serde_json::Value is always serializable")
}

fn canonicalize(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: std::collections::BTreeMap<String, serde_json::Value> =
                map.into_iter().map(|(k, v)| (k, canonicalize(v))).collect();
            serde_json::to_value(sorted).expect("BTreeMap<String, Value> always converts to Value")
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonicalize).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn content(key: &str, hash: &str) -> UpstreamIdentity {
        UpstreamIdentity::Content {
            upstream_key: key.to_string(),
            blake3_hash: hash.to_string(),
        }
    }

    fn watermark(key: &str, ts: Option<&str>, rows: Option<u64>) -> UpstreamIdentity {
        UpstreamIdentity::Watermark {
            upstream_key: key.to_string(),
            max_ts: ts.map(str::to_string),
            row_count: rows,
        }
    }

    #[test]
    fn input_hash_is_deterministic() {
        let ups = vec![content("cat.sch.up", "abc123")];
        let a = compute_input_hash("logic-key-1", "cat.sch.tgt", &ups);
        let b = compute_input_hash("logic-key-1", "cat.sch.tgt", &ups);
        assert_eq!(a, b, "same inputs must yield the same hash");
    }

    #[test]
    fn input_hash_independent_of_upstream_order() {
        let ordered = vec![content("cat.sch.a", "h1"), content("cat.sch.b", "h2")];
        let reversed = vec![content("cat.sch.b", "h2"), content("cat.sch.a", "h1")];
        assert_eq!(
            compute_input_hash("logic", "cat.sch.tgt", &ordered),
            compute_input_hash("logic", "cat.sch.tgt", &reversed),
            "upstream enumeration order must not affect the key"
        );
    }

    #[test]
    fn input_hash_changes_on_logic_change() {
        let ups = vec![content("cat.sch.up", "abc123")];
        let a = compute_input_hash("logic-key-1", "cat.sch.tgt", &ups);
        let b = compute_input_hash("logic-key-2", "cat.sch.tgt", &ups);
        assert_ne!(a, b, "a different skip_hash must change the input_hash");
    }

    #[test]
    fn input_hash_changes_on_upstream_change() {
        let before = vec![content("cat.sch.up", "abc123")];
        let after = vec![content("cat.sch.up", "def456")];
        assert_ne!(
            compute_input_hash("logic", "cat.sch.tgt", &before),
            compute_input_hash("logic", "cat.sch.tgt", &after),
            "a changed upstream identity must change the input_hash"
        );
    }

    #[test]
    fn input_hash_changes_on_target_change() {
        let ups = vec![content("cat.sch.up", "abc123")];
        assert_ne!(
            compute_input_hash("logic", "cat.sch.tgt_a", &ups),
            compute_input_hash("logic", "cat.sch.tgt_b", &ups),
            "a different target identity must change the input_hash"
        );
    }

    #[test]
    fn input_hash_changes_on_identity_kind_switch() {
        let as_content = vec![content("cat.sch.up", "abc123")];
        let as_watermark = vec![watermark("cat.sch.up", Some("2026-06-04T00:00:00Z"), None)];
        assert_ne!(
            compute_input_hash("logic", "cat.sch.tgt", &as_content),
            compute_input_hash("logic", "cat.sch.tgt", &as_watermark),
            "a content↔watermark kind switch must change the input_hash"
        );
    }

    #[test]
    fn proof_class_all_content_is_strong() {
        let ups = vec![content("cat.sch.a", "h1"), content("cat.sch.b", "h2")];
        assert_eq!(proof_class_for(&ups), ProofClass::Strong);
    }

    #[test]
    fn proof_class_mixed_is_heuristic() {
        let ups = vec![
            content("cat.sch.a", "h1"),
            watermark("cat.sch.b", Some("2026-06-04T00:00:00Z"), None),
        ];
        assert_eq!(
            proof_class_for(&ups),
            ProofClass::Heuristic,
            "a single watermark upstream degrades the whole model to heuristic"
        );
    }

    #[test]
    fn proof_class_no_upstreams_is_strong() {
        assert_eq!(
            proof_class_for(&[]),
            ProofClass::Strong,
            "a model with no upstreams rests on its own logic key — vacuously strong"
        );
    }

    #[test]
    fn proof_class_all_watermark_is_heuristic() {
        let ups = vec![
            watermark("cat.sch.a", Some("2026-06-04T00:00:00Z"), None),
            watermark("cat.sch.b", None, Some(42)),
        ];
        assert_eq!(proof_class_for(&ups), ProofClass::Heuristic);
    }

    #[test]
    fn proof_class_min_ordering() {
        assert_eq!(
            ProofClass::min([ProofClass::Strong, ProofClass::Heuristic]),
            ProofClass::Heuristic
        );
        assert_eq!(
            ProofClass::min([ProofClass::Strong, ProofClass::Strong]),
            ProofClass::Strong
        );
    }
}
