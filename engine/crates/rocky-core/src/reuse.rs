//! Input-match spine for auditable reuse — warehouse-agnostic, additive,
//! and opt-in.
//!
//! This module builds the *input* side of reuse: a pre-execution-style key
//! that identifies a model build by its **declared inputs** rather than by
//! the bytes it produced. It is the missing half of the shipped
//! content-addressed ledger ([`crate::state::ArtifactRecord`]), which records
//! the blake3 of the *output* parquet and so can only be read *after* a build
//! has already run.
//!
//! ## Relationship to [`crate::recipe_identity`]
//!
//! The three identity primitives — `recipe_hash`, `input_hash`, `env_hash` —
//! and the [`UpstreamIdentity`] / [`ProofClass`] vocabulary now live in
//! [`crate::recipe_identity`], the single definition site for recipe identity.
//! They are re-exported here so this module's historical call sites (and the
//! `crate::reuse::…` doc links across the state layer) keep resolving
//! unchanged. This module keeps only the reuse-specific record assembly
//! ([`build_records`]) that folds those primitives into the persisted index +
//! provenance rows.
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
//! ## Input side only
//!
//! This module just builds and folds the key; it makes no reuse decision. The
//! index it backs (see [`crate::state::StateStore`]) is *populated* on a
//! successful run and *read back* by the runner's fail-closed reuse decision,
//! which acts on a match by pointing-to a prior run's parquet. Population —
//! and therefore point-to reuse — is gated behind `[reuse] enabled`
//! (default-OFF, [`crate::config::ReuseConfig::enabled`]); with it off this
//! module's input-hash spine is never built, so the run path pays none of its
//! normalize+hash cost. (Column-level skip is a separate, orthogonal knob —
//! [`crate::config::ReuseConfig::column_level`], default-ON — that lives in the
//! `column_skip` module and engages only on the content-addressed path.)

pub use crate::recipe_identity::{
    ProofClass, UpstreamIdentity, compute_input_hash, proof_class_for, resolve_content_upstreams,
};

/// The output a model produced for one content-addressed file.
///
/// Index-aligned `(blake3, path)` pairs flow from the writer's `WriteResult`
/// straight into the index entry + provenance record. A model with no
/// content-addressed output passes an empty slice.
#[derive(Debug, Clone)]
pub struct OutputArtifact {
    /// Hex-encoded blake3 of the output parquet.
    pub blake3_hash: String,
    /// Object-store path of the output parquet.
    pub file_path: String,
}

/// Build the [`crate::state::InputIndexEntry`] +
/// [`crate::state::ProvenanceRecord`] pair for one successfully-built model.
///
/// This is the pure heart of spine population: given the model's typed IR,
/// the run it built in, its resolved immediate-upstream identities, and the
/// output artifact(s) it produced, it assembles both records — computing the
/// `input_hash`, the `proof_class` (MIN over upstreams), and embedding the
/// canonical `ModelIr` JSON for the offline verifier. It performs no I/O, so
/// the warehouse-agnostic population logic is unit-testable without a live
/// adapter.
///
/// # Returns
///
/// `None` when the model cannot be safely canonicalised — its
/// [`rocky_ir::ModelIr::skip_hash`] is `None` (empty typed columns or SQL
/// that does not re-normalise). A non-canonicalisable model is **not
/// indexed**: a never-equal logic key must never be presented as a reuse
/// candidate. Otherwise returns `Some((index_entry, provenance))`.
///
/// # What the records attest
///
/// An *input-logic match* plus the *byte-identity of the recorded output
/// bytes* — never that re-executing the model would reproduce them.
pub fn build_records(
    model_ir: &rocky_ir::ModelIr,
    run_id: &str,
    upstreams: &[UpstreamIdentity],
    outputs: &[OutputArtifact],
    recorded_at: chrono::DateTime<chrono::Utc>,
) -> Option<(
    crate::state::InputIndexEntry,
    crate::state::ProvenanceRecord,
)> {
    let skip_hash = model_ir.skip_hash()?.to_hex().to_string();
    let target_identity = format!(
        "{}.{}.{}",
        model_ir.target.catalog, model_ir.target.schema, model_ir.target.table
    );
    let input_hash = compute_input_hash(&skip_hash, &target_identity, upstreams)
        .to_hex()
        .to_string();
    let proof_class = proof_class_for(upstreams).as_str().to_string();
    let model_name = model_ir.name.to_string();

    let output_blake3: Vec<String> = outputs.iter().map(|o| o.blake3_hash.clone()).collect();
    let output_path: Vec<String> = outputs.iter().map(|o| o.file_path.clone()).collect();

    let index_entry = crate::state::InputIndexEntry {
        input_hash: input_hash.clone(),
        run_id: run_id.to_string(),
        model_name: model_name.clone(),
        output_blake3: output_blake3.clone(),
        output_path: output_path.clone(),
        proof_class: proof_class.clone(),
        recorded_at,
    };
    let provenance = crate::state::ProvenanceRecord {
        run_id: run_id.to_string(),
        model_name,
        input_hash,
        skip_hash,
        model_ir_canonical_json: model_ir.canonical_json(),
        // Persist the exact upstream identities folded into `input_hash` so an
        // offline auditor can recompute it (and re-verify each strong
        // upstream's blake3) without trusting the runtime.
        upstreams: upstreams.to_vec(),
        output_blake3,
        output_path,
        proof_class,
        recorded_at,
    };
    Some((index_entry, provenance))
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

    // -- pure record builder ------------------------------------------------

    fn model_ir(table: &str, sql: &str, canonicalisable: bool) -> rocky_ir::ModelIr {
        let mut m = rocky_ir::ModelIr::transformation(
            rocky_ir::TargetRef {
                catalog: "analytics".to_string(),
                schema: "marts".to_string(),
                table: table.to_string(),
            },
            rocky_ir::MaterializationStrategy::FullRefresh,
            Vec::new(),
            sql.to_string(),
            rocky_ir::GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        m.name = std::sync::Arc::from(table);
        if canonicalisable {
            // skip_hash returns None on empty typed_columns; populate one.
            m.typed_columns = vec![rocky_ir::TypedColumn {
                name: "id".into(),
                data_type: rocky_ir::RockyType::Int64,
                nullable: false,
            }];
        }
        m
    }

    fn output(hash: &str, path: &str) -> OutputArtifact {
        OutputArtifact {
            blake3_hash: hash.to_string(),
            file_path: path.to_string(),
        }
    }

    #[test]
    fn build_records_assembles_index_and_provenance() {
        let m = model_ir("fct_orders", "SELECT id FROM raw.orders", true);
        let ups = vec![content("analytics.marts.dim_x", "up-hash")];
        let outs = vec![output("out-hash", "s3://b/p/out.parquet")];
        let now = chrono::Utc::now();

        let (entry, prov) = build_records(&m, "run-1", &ups, &outs, now).expect("canonicalisable");

        assert_eq!(entry.run_id, "run-1");
        assert_eq!(entry.model_name, "fct_orders");
        assert_eq!(entry.proof_class, "strong");
        assert_eq!(entry.output_blake3, vec!["out-hash".to_string()]);
        assert_eq!(entry.output_path, vec!["s3://b/p/out.parquet".to_string()]);

        // Both records key off the same input_hash, derived from skip_hash.
        assert_eq!(entry.input_hash, prov.input_hash);
        let expected_skip = m.skip_hash().unwrap().to_hex().to_string();
        assert_eq!(prov.skip_hash, expected_skip);

        // The provenance record persists the exact upstreams folded into
        // input_hash, so an offline recompute is possible without the runtime.
        assert_eq!(prov.upstreams, ups);
        let recomputed = compute_input_hash(
            &prov.skip_hash,
            "analytics.marts.fct_orders",
            &prov.upstreams,
        )
        .to_hex()
        .to_string();
        assert_eq!(
            recomputed, prov.input_hash,
            "input_hash must recompute from the persisted skip_hash + target + upstreams"
        );

        // Provenance embeds the round-trippable canonical IR.
        let reparsed: rocky_ir::ModelIr =
            serde_json::from_str(&prov.model_ir_canonical_json).expect("round-trips");
        assert_eq!(
            reparsed.skip_hash().unwrap().to_hex().to_string(),
            prov.skip_hash,
            "offline skip_hash recompute matches the recorded value"
        );
    }

    #[test]
    fn build_records_input_hash_matches_compute_input_hash() {
        // The builder's input_hash must equal what an offline verifier would
        // recompute from the same skip_hash + target + upstreams.
        let m = model_ir("fct_orders", "SELECT id FROM raw.orders", true);
        let ups = vec![content("analytics.marts.dim_x", "up-hash")];
        let (entry, _) =
            build_records(&m, "run-1", &ups, &[], chrono::Utc::now()).expect("canonicalisable");

        let skip = m.skip_hash().unwrap().to_hex().to_string();
        let expected = compute_input_hash(&skip, "analytics.marts.fct_orders", &ups)
            .to_hex()
            .to_string();
        assert_eq!(entry.input_hash, expected);
    }

    #[test]
    fn build_records_proof_class_is_min_over_upstreams() {
        let m = model_ir("fct_orders", "SELECT id FROM raw.orders", true);
        let mixed = vec![
            content("analytics.marts.a", "h1"),
            watermark("raw.src.b", Some("2026-06-04T00:00:00Z"), None),
        ];
        let (entry, prov) =
            build_records(&m, "run-1", &mixed, &[], chrono::Utc::now()).expect("canonicalisable");
        assert_eq!(entry.proof_class, "heuristic");
        assert_eq!(prov.proof_class, "heuristic");
    }

    #[test]
    fn build_records_skips_non_canonicalisable_model() {
        // Empty typed_columns ⇒ skip_hash None ⇒ not indexed.
        let m = model_ir("fct_orders", "SELECT id FROM raw.orders", false);
        assert!(m.skip_hash().is_none());
        assert!(
            build_records(&m, "run-1", &[], &[], chrono::Utc::now()).is_none(),
            "a non-canonicalisable model must not be indexed"
        );
    }
}
