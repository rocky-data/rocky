//! Recipe identity — the one place the `(recipe_hash, input_hash, env_hash)`
//! triple is defined.
//!
//! A table version's identity is three orthogonal questions:
//!
//! 1. **What program produced it?** — [`recipe_hash`]: the blake3 of the
//!    canonical [`rocky_ir::ModelIr`] JSON (the exact form
//!    [`crate::state::ProvenanceRecord`] embeds). This is the *identity* key.
//! 2. **What inputs did it read?** — [`compute_input_hash`]: a blake3 over the
//!    ordered set of *resolved* input identities. A content-addressed upstream
//!    contributes its artifact blake3 (a [`ProofClass::Strong`] claim); a
//!    mutable source contributes an adapter-observed freshness signature —
//!    a watermark / rowcount that is explicitly a [`ProofClass::Heuristic`]
//!    (i.e. *weak*) claim, never a byte-identity claim.
//! 3. **In what environment did it run?** — [`env_hash`]: a blake3 over the
//!    engine version, the adapter/dialect identity, and any relevant execution
//!    config. It deliberately **excludes hostname** — machine identity is not
//!    environment semantics.
//!
//! Every stored triple is tagged with [`HASH_SCHEME`] so a future
//! canonicalisation change can be introduced deliberately (a new scheme value)
//! rather than silently bifurcating recorded history.
//!
//! ## `recipe_hash` is not `skip_hash`
//!
//! Do not conflate the two. [`recipe_hash`] hashes the *raw* IR (whitespace and
//! all) and is the stable **identity** of a program — two whitespace-different
//! but logically-equal models have *different* recipe hashes.
//! [`rocky_ir::ModelIr::skip_hash`] hashes a *cosmetic-invariant* projection
//! and is the **skip** key — two cosmetically-different but logically-equal
//! models have the *same* skip hash. `skip_hash` answers "may I skip rebuilding
//! this?"; `recipe_hash` answers "exactly which program is this?". The skip
//! gate's fail-closed soundness story is unrelated to and untouched by this
//! module.
//!
//! ## 🔴 Ledger readability is never broken
//!
//! Everything here feeds the durable ledger ([`crate::state`]) and the
//! offline-verifiable [`crate::state::ProvenanceRecord`]. The canonical-JSON
//! encoding below is therefore a **review-critical surface**: any change to it
//! silently changes `recipe_hash` / `input_hash` for unchanged logic, which
//! bifurcates recorded history and can break an auditor's ability to re-read a
//! stored record. Treat every edit to [`canonical_json`], to the hashed
//! projections, or to a serde attribute on a hashed type as a 🔴 change: pair
//! it with a deliberate scheme/version bump and update the pinned goldens on
//! purpose, never to make a test go green. The serialized-record and
//! absolute-hex goldens (see this module's tests) are the tripwire.

use serde::{Deserialize, Serialize};

/// Versioned tag stamped alongside every recorded recipe-identity triple.
///
/// The scheme names the *algorithm family* that produced a stored triple, so a
/// consumer reading an old ledger row knows which canonicalisation rules were
/// in force. Bump it **deliberately** (to `"v2"`, …) when a change to
/// [`recipe_hash`], [`compute_input_hash`], or [`env_hash`] would otherwise
/// change the value for unchanged inputs — that turns a would-be silent history
/// fork into an explicit, queryable one.
pub const HASH_SCHEME: &str = "v1";

/// The **identity** key for a model: blake3 of the canonical
/// [`rocky_ir::ModelIr`] JSON.
///
/// This is a thin, single-source-of-truth wrapper over
/// [`rocky_ir::ModelIr::recipe_hash`] so every caller in the engine computes
/// the identity key the same way. The canonical form it hashes is byte-for-byte
/// the one [`crate::state::ProvenanceRecord::model_ir_canonical_json`] embeds,
/// so an offline auditor can recompute this hash from a stored record without
/// trusting the runtime.
///
/// Distinct from [`rocky_ir::ModelIr::skip_hash`] — see the module docs.
#[must_use]
pub fn recipe_hash(model_ir: &rocky_ir::ModelIr) -> blake3::Hash {
    model_ir.recipe_hash()
}

// ---------------------------------------------------------------------------
// Input identity (moved verbatim from `crate::reuse` — byte-preserving)
// ---------------------------------------------------------------------------

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
    /// not byte-identity. This is the *weak* label the recipe-identity design
    /// refers to for mutable sources.
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

/// Resolve a model's **entire read set** to STRONG content-hash upstream
/// identities, or refuse.
///
/// `read_tables` is every table the model reads, as enumerated from its SQL
/// lineage — *not* just its project-model dependencies. `resolve` maps a read
/// table to the content blake3 hash + canonical upstream key for the artifact
/// it produced this run, or `None` when that table has no content hash
/// available (a raw source, a model not written content-addressed, a model
/// not built this run, or a partitioned write whose ledger hash is
/// incomplete).
///
/// # Returns
///
/// `Some(_)` **only when every** read table resolves to a content hash — the
/// all-strong chain that is legitimately `b3sum`-verifiable end-to-end. A
/// single unresolved read (most importantly a raw source, which `skip_hash`
/// captures by name but cannot byte-verify) returns `None` so the caller does
/// **not** index the model. This is the fail-safe that keeps a `strong`
/// label honest: a mixed-input model is never mislabeled strong — it is
/// simply not indexed in this slice (heuristic/watermark population for such
/// models is deferred). A model that reads nothing resolves to an empty
/// (vacuously strong) set.
pub fn resolve_content_upstreams<F>(
    read_tables: &[String],
    mut resolve: F,
) -> Option<Vec<UpstreamIdentity>>
where
    F: FnMut(&str) -> Option<(String, String)>,
{
    let mut identities = Vec::with_capacity(read_tables.len());
    for table in read_tables {
        let (upstream_key, blake3_hash) = resolve(table)?;
        identities.push(UpstreamIdentity::Content {
            upstream_key,
            blake3_hash,
        });
    }
    Some(identities)
}

// ---------------------------------------------------------------------------
// Environment identity
// ---------------------------------------------------------------------------

/// The inputs to [`env_hash`] — the *semantics* of the environment a model ran
/// in, deliberately excluding machine identity.
///
/// Serialised through the same canonical JSON as the other hashes so the key is
/// stable across machines for the same logical environment. Two runs on
/// different hosts with the same engine, adapter, and execution config share an
/// `env_hash`; that is the point — the hostname lives on
/// [`crate::state::RunRecord`] for audit, not here.
///
/// [`Self::exec_config`] is an explicit, ordinarily-empty structured slot: the
/// v1 scheme folds nothing execution-config-specific in (a whole-config hash
/// would churn `env_hash` on environment-irrelevant edits like a renamed
/// pipeline), and the [`HASH_SCHEME`] tag is the extension point when a future
/// scheme needs to add a genuinely env-relevant setting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct EnvIdentity {
    /// The engine's semver (`CARGO_PKG_VERSION` of the `rocky` binary).
    pub engine_version: String,
    /// The adapter / dialect identity — `WarehouseAdapter::dialect().name()`
    /// (`"duckdb"`, `"databricks"`, …). For Rocky's in-tree adapters this
    /// single string is both the adapter name and the SQL dialect: they ship
    /// and version together with the engine, so a separate adapter-version is
    /// [`Self::engine_version`].
    pub adapter: String,
    /// Explicit, ordinarily-empty slot for genuinely environment-relevant
    /// execution settings. Empty under the v1 scheme; see the struct docs.
    pub exec_config: std::collections::BTreeMap<String, String>,
}

/// Compute the deterministic environment key for a run.
///
/// blake3 over the canonical JSON of [`EnvIdentity`]. Excludes hostname by
/// construction (it is not a field). The [`HASH_SCHEME`] tag is stored
/// *alongside* the resulting hash rather than folded in, keeping `env_hash` a
/// pure content hash of the environment inputs.
#[must_use]
pub fn env_hash(env: &EnvIdentity) -> blake3::Hash {
    let canonical = canonical_json(env);
    blake3::hash(canonical.as_bytes())
}

// ---------------------------------------------------------------------------
// Canonical JSON (moved verbatim from `crate::reuse`; mirrors
// `rocky_ir::ModelIr::canonical_json`)
// ---------------------------------------------------------------------------

/// Canonical-JSON encoder mirroring [`rocky_ir::ModelIr::canonical_json`]'s
/// key-sorted, whitespace-free form so identity keys are stable across
/// machines.
///
/// 🔴 Review-critical: changing this changes `input_hash` / `env_hash` for
/// unchanged inputs. See the module docs.
fn canonical_json<T: Serialize>(value: &T) -> String {
    let raw = serde_json::to_value(value)
        .expect("recipe-identity projection must be JSON-encodable; programming error");
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

    #[test]
    fn resolve_content_upstreams_all_resolved_is_some() {
        let reads = vec!["a".to_string(), "b".to_string()];
        let got = resolve_content_upstreams(&reads, |t| {
            Some((format!("cat.sch.{t}"), format!("hash-{t}")))
        })
        .expect("every read resolved");
        assert_eq!(got.len(), 2);
        assert!(
            got.iter()
                .all(|u| matches!(u, UpstreamIdentity::Content { .. }))
        );
        assert_eq!(proof_class_for(&got), ProofClass::Strong);
    }

    #[test]
    fn resolve_content_upstreams_any_unresolved_is_none() {
        let reads = vec!["a".to_string(), "raw_source".to_string()];
        let got = resolve_content_upstreams(&reads, |t| {
            // The raw source has no content hash.
            if t == "raw_source" {
                None
            } else {
                Some((format!("cat.sch.{t}"), format!("hash-{t}")))
            }
        });
        assert!(
            got.is_none(),
            "a single unresolved read (raw source) must refuse to index, not mislabel strong"
        );
    }

    #[test]
    fn resolve_content_upstreams_empty_read_set_is_strong() {
        let got = resolve_content_upstreams(&[], |_| unreachable!("no reads"))
            .expect("an empty read set resolves");
        assert!(got.is_empty());
        assert_eq!(proof_class_for(&got), ProofClass::Strong);
    }

    // -- absolute-hex goldens: soft-swap + canonicalisation tripwires --------

    /// The soft-swap tripwire. This absolute-hex pin was captured from the
    /// pre-move `crate::reuse::compute_input_hash` and MUST survive the move of
    /// the input-hash cluster into this module. A drift here is a real
    /// reuse-gate divergence (the gate is live-verified on the sandbox), not a
    /// test to re-pin — investigate before touching this value.
    #[test]
    fn input_hash_absolute_hex_pinned() {
        let ups = vec![
            content("analytics.marts.dim_x", "up-hash"),
            watermark("raw.src.events", Some("2026-06-04T00:00:00Z"), Some(42)),
        ];
        let got = compute_input_hash("skip-key-fixed", "analytics.marts.fct_orders", &ups)
            .to_hex()
            .to_string();
        assert_eq!(got, INPUT_HASH_GOLDEN, "input_hash canonicalisation drift");
    }

    /// Absolute-hex pin for [`env_hash`] under the v1 scheme. Guards the
    /// environment canonicalisation the same way.
    #[test]
    fn env_hash_absolute_hex_pinned() {
        let env = EnvIdentity {
            engine_version: "1.56.0".to_string(),
            adapter: "duckdb".to_string(),
            exec_config: std::collections::BTreeMap::new(),
        };
        let got = env_hash(&env).to_hex().to_string();
        assert_eq!(got, ENV_HASH_GOLDEN, "env_hash canonicalisation drift");
    }

    // Pins captured with `just`/`cargo test` under REGEN and hand-verified;
    // see `PIN_REGEN` below to reprint.
    const INPUT_HASH_GOLDEN: &str =
        "baa07cee48236730d8100fa6947ec103ccfac83005ed1b9b68527ae942b77452";
    const ENV_HASH_GOLDEN: &str =
        "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd";

    /// Prints the two absolute-hex pins so an intentional scheme bump can
    /// re-capture them deliberately. Ignored by default; run with
    /// `--ignored --nocapture`.
    #[test]
    #[ignore = "prints goldens for a deliberate re-pin"]
    fn pin_regen() {
        let ups = vec![
            content("analytics.marts.dim_x", "up-hash"),
            watermark("raw.src.events", Some("2026-06-04T00:00:00Z"), Some(42)),
        ];
        eprintln!(
            "INPUT_HASH_GOLDEN = {}",
            compute_input_hash("skip-key-fixed", "analytics.marts.fct_orders", &ups).to_hex()
        );
        let env = EnvIdentity {
            engine_version: "1.56.0".to_string(),
            adapter: "duckdb".to_string(),
            exec_config: std::collections::BTreeMap::new(),
        };
        eprintln!("ENV_HASH_GOLDEN  = {}", env_hash(&env).to_hex());
    }

    #[test]
    fn recipe_hash_delegates_to_model_ir() {
        // The module's identity key must be byte-identical to
        // `ModelIr::recipe_hash` — the form ProvenanceRecord embeds.
        let mut m = rocky_ir::ModelIr::transformation(
            rocky_ir::TargetRef {
                catalog: "analytics".to_string(),
                schema: "marts".to_string(),
                table: "fct_orders".to_string(),
            },
            rocky_ir::MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT id FROM raw.orders".to_string(),
            rocky_ir::GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        m.name = std::sync::Arc::from("fct_orders");
        assert_eq!(recipe_hash(&m), m.recipe_hash());
    }
}
