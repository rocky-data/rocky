//! Lower a parsed spec onto primitives the engine already enforces, in
//! two phases.
//!
//! Two phases because the worker's drafting tool wholesale-rewrites the
//! model sidecar (it writes `name` and `intent` and nothing else), so
//! metadata lowered before drafting would be destroyed. Therefore:
//!
//! - **Phase A**, before drafting: `models/<model>.contract.toml`, the
//!   worker's compile-loop goal. Spec-owned; the drafting brief forbids
//!   the worker writing a contract of its own.
//! - **Phase B**, after drafting: merge spec-owned metadata into
//!   `models/<model>.toml`. Own only the spec's keys; preserve `name`,
//!   `intent`, and every test the worker appended.
//!
//! Everything here is a pure function. Staging the writes, journalling
//! them, and recovering a crashed generation are a separate concern and
//! live elsewhere.
//!
//! # Why freshness is emitted without its severity
//!
//! The sidecar's freshness block is read by the engine in several
//! places — the tick reconciler's schedule demand, `rocky validate`'s
//! budget resolution, the standing freshness-demand computation, the
//! compile and dag JSON passthrough, and the typechecker's soft warning
//! about temporal columns. All of them read `max_lag_seconds`.
//!
//! `severity` has **no read site**. The only severity near freshness is
//! the pipeline-level check config, a different type, and even that is
//! surfaced advisorily. `rocky test` never evaluates freshness and
//! `rocky doctor` has no freshness consumer.
//!
//! So the lowering emits `[freshness]` as declared metadata plus a
//! runner observation — `expected_lag_seconds` and `time_column`, never
//! `severity`. Writing a severity the engine ignores would dress the
//! file up as enforcement that does not exist. The freshness guarantee
//! is the runner's staleness observation, not an engine gate.

use std::collections::BTreeMap;

use crate::product::manifest::{
    Disposition, FieldRow, Location, LocationPhase, MANIFEST_FILENAME, Manifest, ManifestPhase,
    assert_total, content_digest,
};
use crate::product::spec::{ParsedSpec, SpecRejected, SpecResult};
use crate::product::toml_compat::{
    TomlTable, TomlValue, basic_string, parse_ordered, render_document,
};

/// The top-level sidecar keys the lowering owns.
///
/// Owned means rewritten from the spec on every merge, so a hand edit to
/// one of them is silently replaced. Everything else in the file is
/// preserved verbatim.
const OWNED_TOP_LEVEL: &[&str] = &["sources", "tags", "classification", "freshness", "tests"];

/// One emitted file: a project-root-relative POSIX path plus its final
/// bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Artifact {
    /// Project-root-relative POSIX path.
    pub relpath: String,
    /// The exact bytes to write.
    pub content: Vec<u8>,
}

/// A lowering run's output: the artifacts plus the total manifest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lowering {
    /// The files this phase emits.
    pub artifacts: Vec<Artifact>,
    /// The manifest accounting for every spec field.
    pub manifest: Manifest,
}

/// The per-product state directory, project-root-relative.
pub fn state_dir_rel(product_name: &str) -> String {
    format!(".rocky/fulfillment/{product_name}")
}

/// Where the lowering manifest is committed.
pub fn manifest_rel(product_name: &str) -> String {
    format!("{}/{MANIFEST_FILENAME}", state_dir_rel(product_name))
}

/// The header every generated artifact opens with.
pub fn generated_header(product_name: &str, digest: &str) -> String {
    format!(
        "# GENERATED from products/{product_name}.toml (spec {digest}) \
         — edit the spec, not this file."
    )
}

/// The contract file this spec lowers to.
pub fn contract_rel(parsed: &ParsedSpec) -> String {
    format!("models/{}.contract.toml", parsed.output_model())
}

/// The model sidecar this spec merges into.
pub fn sidecar_rel(parsed: &ParsedSpec) -> String {
    format!("models/{}.toml", parsed.output_model())
}

/// The model SQL file. Agent-authored; the lowering never writes it.
pub fn sql_rel(parsed: &ParsedSpec) -> String {
    format!("models/{}.sql", parsed.output_model())
}

// ---------------------------------------------------------------------------
// Phase A: the contract
// ---------------------------------------------------------------------------

/// Render `models/<model>.contract.toml` — byte-stable, hand-ordered.
///
/// `[[columns]]` comes in spec order with the Rocky type names verbatim,
/// and `[rules] required` lists EVERY declared column (the grain is a
/// subset of it).
///
/// Never `no_new_nullable`: the engine parses that rule and enforces it
/// nowhere, so emitting it would promise a guard that does not run.
pub fn render_contract(parsed: &ParsedSpec) -> Vec<u8> {
    let product = parsed.product();
    let mut lines: Vec<String> = vec![
        generated_header(&product.name, &parsed.digest),
        String::new(),
    ];
    for column in &product.output.columns {
        lines.push("[[columns]]".to_string());
        lines.push(format!("name = {}", basic_string(&column.name)));
        lines.push(format!("type = {}", basic_string(&column.type_name)));
        lines.push(format!(
            "nullable = {}",
            if column.nullable { "true" } else { "false" }
        ));
        lines.push(String::new());
    }
    lines.push("[rules]".to_string());
    let required: Vec<String> = product
        .output
        .columns
        .iter()
        .map(|column| basic_string(&column.name))
        .collect();
    lines.push(format!("required = [{}]", required.join(", ")));
    lines.push(String::new());
    lines.join("\n").into_bytes()
}

// ---------------------------------------------------------------------------
// Phase B: the sidecar metadata merge
// ---------------------------------------------------------------------------

/// The spec-owned `[[tests]]` entries, in the engine's test-declaration
/// shape.
///
/// - A single-column grain becomes a `unique` test on that column; a
///   composite grain becomes `composite` + `kind = "unique"`. The engine
///   has no first-class grain field, so this fan-out IS the lowering.
/// - Every `nullable = false` column becomes a `not_null` test. The
///   compile-time contract guards the inference; the test guards the
///   data.
/// - Every declared check becomes an `expression` test at error
///   severity.
pub fn generated_tests(parsed: &ParsedSpec) -> Vec<TomlTable> {
    let product = parsed.product();
    let mut tests: Vec<TomlTable> = Vec::new();

    let grain = &product.output.grain;
    let mut grain_test = TomlTable::new();
    if grain.len() == 1 {
        grain_test.insert("type".to_string(), TomlValue::string("unique"));
        grain_test.insert("column".to_string(), TomlValue::string(grain[0].clone()));
    } else {
        grain_test.insert("type".to_string(), TomlValue::string("composite"));
        grain_test.insert("kind".to_string(), TomlValue::string("unique"));
        grain_test.insert(
            "columns".to_string(),
            TomlValue::Array(grain.iter().map(TomlValue::string).collect()),
        );
    }
    tests.push(grain_test);

    for column in &product.output.columns {
        if column.nullable {
            continue;
        }
        let mut test = TomlTable::new();
        test.insert("type".to_string(), TomlValue::string("not_null"));
        test.insert("column".to_string(), TomlValue::string(column.name.clone()));
        tests.push(test);
    }

    for check in &product.output.checks {
        let mut test = TomlTable::new();
        test.insert("type".to_string(), TomlValue::string("expression"));
        test.insert("expression".to_string(), TomlValue::string(check.clone()));
        test.insert("severity".to_string(), TomlValue::string("error"));
        tests.push(test);
    }
    tests
}

/// Merge spec-owned metadata into an existing sidecar document.
///
/// Owned wholesale: `[[sources]]`, `[classification]`, `[freshness]`,
/// and the generated subset of `[[tests]]`. Owned as a single key:
/// `[tags].product` — other tag keys survive. Preserved: `name` and
/// `intent` (`intent` is filled from the spec only when the file has
/// none), every worker-appended test the lowering did not generate, and
/// every other key verbatim.
///
/// The output order is fixed, so merging twice is byte-identical: the
/// header comment, `name`, `intent`, the remaining preserved scalars in
/// their original order, the preserved tables in their original order,
/// then the owned blocks in the order above.
///
/// # Errors
///
/// Returns `sidecar-unparseable` naming the path when the existing file
/// is not TOML — the lowering refuses rather than clobbering a file it
/// cannot read. Returns `max-lag-unrepresentable` when the freshness
/// budget in seconds exceeds what a TOML integer can hold.
pub fn merge_sidecar_text(
    parsed: &ParsedSpec,
    sidecar_text: &str,
    source_name: &str,
) -> SpecResult<String> {
    let document = parse_ordered(sidecar_text).map_err(|err| {
        SpecRejected::new(
            "sidecar-unparseable",
            format!(
                "refusing to merge into {source_name}: the existing sidecar does not parse \
                 as TOML ({err}); fix or remove it — the lowering never clobbers"
            ),
        )
    })?;

    let product = parsed.product();
    let preserved: TomlTable = document
        .iter()
        .filter(|(key, _)| !OWNED_TOP_LEVEL.contains(&key.as_str()))
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect();

    let mut merged = TomlTable::new();
    merged.insert(
        "name".to_string(),
        preserved
            .get("name")
            .cloned()
            .unwrap_or_else(|| TomlValue::string(parsed.output_model())),
    );
    merged.insert(
        "intent".to_string(),
        preserved
            .get("intent")
            .cloned()
            .unwrap_or_else(|| TomlValue::string(product.intent.clone())),
    );
    // Scalars before tables, each group in the file's own order: TOML
    // reads a bare key after a table header as a member of that table,
    // so a preserved scalar must never fall below one.
    for (key, value) in &preserved {
        if key != "name" && key != "intent" && !value.is_table_valued() {
            merged.insert(key.clone(), value.clone());
        }
    }
    for (key, value) in &preserved {
        if key != "name" && key != "intent" && value.is_table_valued() {
            merged.insert(key.clone(), value.clone());
        }
    }

    let sources: Vec<TomlValue> = product
        .source
        .tables
        .iter()
        .map(|reference| {
            let mut parts = reference.split('.');
            let mut source = TomlTable::new();
            // The parser guarantees exactly three non-empty parts, so a
            // missing one can only mean the parser stopped enforcing it.
            for key in ["catalog", "schema", "table"] {
                source.insert(
                    key.to_string(),
                    TomlValue::string(parts.next().unwrap_or_default()),
                );
            }
            TomlValue::Table(source)
        })
        .collect();
    merged.insert("sources".to_string(), TomlValue::Array(sources));

    let mut tags = TomlTable::new();
    tags.insert(
        "product".to_string(),
        TomlValue::string(product.name.clone()),
    );
    if let Some(existing) = document.get("tags").and_then(TomlValue::as_table) {
        for (key, value) in existing {
            if key != "product" {
                tags.insert(key.clone(), value.clone());
            }
        }
    }
    merged.insert("tags".to_string(), TomlValue::Table(tags));

    if !product.output.classifications.is_empty() {
        let classification: TomlTable = product
            .output
            .classifications
            .iter()
            .map(|(column, tag)| (column.clone(), TomlValue::string(tag.clone())))
            .collect();
        merged.insert(
            "classification".to_string(),
            TomlValue::Table(classification),
        );
    }

    if let Some(freshness) = &product.output.freshness {
        let seconds = freshness.max_lag_seconds()?;
        let seconds = i64::try_from(seconds).map_err(|_| {
            SpecRejected::new(
                "max-lag-unrepresentable",
                format!(
                    "freshness.max_lag '{}' is {seconds} seconds, which no TOML integer can \
                     hold — the sidecar would not parse back",
                    freshness.max_lag
                ),
            )
        })?;
        let mut block = TomlTable::new();
        block.insert(
            "expected_lag_seconds".to_string(),
            TomlValue::Integer(seconds),
        );
        block.insert(
            "time_column".to_string(),
            TomlValue::string(freshness.time_column.clone()),
        );
        // No `severity` — see the module docs.
        merged.insert("freshness".to_string(), TomlValue::Table(block));
    }

    let owned_tests = generated_tests(parsed);
    let mut tests: Vec<TomlValue> = owned_tests.iter().cloned().map(TomlValue::Table).collect();
    if let Some(existing) = document.get("tests").and_then(TomlValue::as_array) {
        for test in existing {
            // A worker test identical to a generated one is absorbed, not
            // duplicated. Table comparison ignores key order, so a
            // reordered spelling of the same test is still the same test.
            let Some(table) = test.as_table() else {
                continue;
            };
            if !owned_tests.contains(table) {
                tests.push(test.clone());
            }
        }
    }
    merged.insert("tests".to_string(), TomlValue::Array(tests));

    let header = generated_header(&product.name, &parsed.digest);
    Ok(format!("{header}\n{}", render_document(&merged)))
}

// ---------------------------------------------------------------------------
// Manifest rows
// ---------------------------------------------------------------------------

/// Which phase's manifest is being built.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    A,
    B,
}

/// Every spec field's disposition.
///
/// In the Phase-A manifest, a field whose artifacts land in Phase B is
/// `pending`, with whatever Phase-A locations it already has listed.
fn field_rows(parsed: &ParsedSpec, phase: Phase) -> BTreeMap<String, FieldRow> {
    let product = parsed.product();
    let contract = contract_rel(parsed);
    let sidecar = sidecar_rel(parsed);

    let row = |a_locations: Vec<Location>, b_locations: Vec<Location>, note: &str| -> FieldRow {
        if phase == Phase::A && !b_locations.is_empty() {
            return FieldRow::new(Disposition::Pending)
                .with_locations(a_locations)
                .with_note(note);
        }
        let mut locations = a_locations;
        locations.extend(b_locations);
        FieldRow::new(Disposition::Artifact)
            .with_locations(locations)
            .with_note(note)
    };

    let mut rows: BTreeMap<String, FieldRow> = BTreeMap::new();
    if parsed.spec.spec_version.is_some() {
        rows.insert(
            "spec_version".to_string(),
            FieldRow::new(Disposition::Identity)
                .with_note("schema version pin, consumed by the parser gate; no artifact"),
        );
    }
    rows.insert(
        "product.name".to_string(),
        row(
            Vec::new(),
            vec![Location::new(&sidecar, "[tags] product", LocationPhase::B)],
            &format!(
                "also the identity input: product_id = '{}'",
                parsed.product_id()
            ),
        ),
    );
    rows.insert(
        "product.intent".to_string(),
        row(
            Vec::new(),
            vec![Location::new(
                &sidecar,
                "intent (preserved when the draft carries one)",
                LocationPhase::B,
            )],
            "delivered verbatim to the worker via the drafting brief (WP-3)",
        ),
    );
    rows.insert(
        "product.source.tables".to_string(),
        row(
            Vec::new(),
            vec![Location::new(
                &sidecar,
                "[[sources]] {catalog, schema, table}",
                LocationPhase::B,
            )],
            "also the grounding list in the agent task brief (WP-3)",
        ),
    );
    // Only when the spec declares it: the resolved default is computed,
    // never written back, so an omitted `model` is not a field the
    // manifest can account for — and a row for it would fail totality.
    if product.output.model.is_some() {
        rows.insert(
            "product.output.model".to_string(),
            row(
                vec![Location::new(
                    &contract,
                    "file name (spec-owned, wholesale)",
                    LocationPhase::A,
                )],
                vec![Location::new(
                    &sidecar,
                    "file name (metadata merged, agent content kept)",
                    LocationPhase::B,
                )],
                &format!(
                    "names {} too — agent-authored, never overwritten by lowering",
                    sql_rel(parsed)
                ),
            ),
        );
    }
    rows.insert(
        "product.output.grain".to_string(),
        row(
            vec![Location::new(
                &contract,
                "[rules] required (grain ⊆ required)",
                LocationPhase::A,
            )],
            vec![Location::new(
                &sidecar,
                if product.output.grain.len() == 1 {
                    "[[tests]] type=unique"
                } else {
                    "[[tests]] type=composite kind=unique"
                },
                LocationPhase::B,
            )],
            "no first-class grain field exists in the engine — this fan-out is the lowering",
        ),
    );
    rows.insert(
        "product.output.columns".to_string(),
        row(
            vec![
                Location::new(
                    &contract,
                    "[[columns]] {name, type, nullable}",
                    LocationPhase::A,
                ),
                Location::new(
                    &contract,
                    "[rules] required = [all listed columns]",
                    LocationPhase::A,
                ),
            ],
            vec![Location::new(
                &sidecar,
                "[[tests]] not_null per nullable=false column",
                LocationPhase::B,
            )],
            "",
        ),
    );
    // Checks and classifications default to empty rather than absent, so
    // their rows exist even when empty: an empty declaration is
    // accounted for, not silently skipped.
    let has_checks = !product.output.checks.is_empty();
    rows.insert(
        "product.output.checks".to_string(),
        row(
            Vec::new(),
            if has_checks {
                vec![Location::new(
                    &sidecar,
                    "[[tests]] type=expression, severity=error",
                    LocationPhase::B,
                )]
            } else {
                Vec::new()
            },
            if has_checks {
                ""
            } else {
                "no checks declared — nothing to emit"
            },
        ),
    );
    if product.output.freshness.is_some() {
        rows.insert(
            "product.output.freshness".to_string(),
            row(
                Vec::new(),
                vec![Location::new(
                    &sidecar,
                    "[freshness] expected_lag_seconds + time_column",
                    LocationPhase::B,
                )],
                "declared metadata plus runner observation — not an engine gate; severity is \
                 accepted for the runner's observation phase and NOT written to the sidecar \
                 (no engine path reads it; see the module docstring trace)",
            ),
        );
    }
    let has_classifications = !product.output.classifications.is_empty();
    rows.insert(
        "product.output.classifications".to_string(),
        row(
            Vec::new(),
            if has_classifications {
                vec![Location::new(
                    &sidecar,
                    "[classification] <column> = <tag>",
                    LocationPhase::B,
                )]
            } else {
                Vec::new()
            },
            if has_classifications {
                "tag RESOLUTION is verified against [mask]/[classifications].allow_unmasked \
                 (spec-compile error, stricter than the engine's W004 warning); masking \
                 APPLICATION is warehouse-dependent — Databricks-only today"
            } else {
                "no classifications declared"
            },
        ),
    );
    rows.insert(
        "product.trust.agent".to_string(),
        FieldRow::new(Disposition::Verified).with_note(
            "verify, don't write (FF-DESIGN D5): the runtime never edits rocky.toml — the \
             three-check posture verification ran before lowering",
        ),
    );
    rows
}

fn build_manifest(
    parsed: &ParsedSpec,
    spec_path: &str,
    phase: Phase,
    artifact_hashes: BTreeMap<String, String>,
) -> SpecResult<Manifest> {
    let manifest = Manifest {
        product_id: parsed.product_id(),
        spec_digest: parsed.digest.clone(),
        spec_path: spec_path.to_string(),
        output_model: parsed.output_model().to_string(),
        phase: match phase {
            Phase::A => ManifestPhase::LoweredContract,
            Phase::B => ManifestPhase::Merged,
        },
        fields: field_rows(parsed, phase),
        artifacts: artifact_hashes,
    };
    assert_total(&parsed.spec, &manifest)?;
    Ok(manifest)
}

// ---------------------------------------------------------------------------
// The two lowering runs
// ---------------------------------------------------------------------------

/// Phase A: the contract, plus a total manifest with the Phase-B fields
/// marked pending.
///
/// # Errors
///
/// Returns `manifest-not-total` when a spec field has no disposition —
/// a bug in this module, surfaced as a refusal rather than a silent
/// half-accounted manifest.
pub fn lower_phase_a(parsed: &ParsedSpec, spec_path: &str) -> SpecResult<Lowering> {
    let contract = Artifact {
        relpath: contract_rel(parsed),
        content: render_contract(parsed),
    };
    let hashes = [(contract.relpath.clone(), content_digest(&contract.content))]
        .into_iter()
        .collect();
    let manifest = build_manifest(parsed, spec_path, Phase::A, hashes)?;
    Ok(Lowering {
        artifacts: vec![contract],
        manifest,
    })
}

/// Phase B: the merged sidecar, plus the total merged manifest.
///
/// `phase_a_manifest` is the VERIFIED Phase-A manifest — the typed
/// object whose bytes the caller checked against disk. This is a public
/// boundary, so the generation identity is enforced HERE rather than
/// delegated: the manifest's `product_id`, `output_model`, and
/// `spec_path` must match the current spec's, and its `spec_digest` must
/// match this spec's. A digest mismatch means the spec moved after Phase
/// A, and Phase B refuses rather than binding spec B's metadata to spec
/// A's contract hash. No path, public or internal, can mix generations.
///
/// The carry-forward hashes come from that identity-checked object and
/// are never re-rendered. Re-rendering would record a hash for bytes
/// this run never wrote and never verified were on disk, detaching the
/// verification target from reality the moment the two could differ.
///
/// # Errors
///
/// Returns `phase-a-identity-mismatch` for a foreign generation,
/// `spec-superseded` for a spec that moved after Phase A,
/// `phase-a-manifest-incomplete` for a manifest that does not cover the
/// contract, plus any refusal from [`merge_sidecar_text`].
pub fn lower_phase_b(
    parsed: &ParsedSpec,
    spec_path: &str,
    sidecar_text: &str,
    phase_a_manifest: &Manifest,
) -> SpecResult<Lowering> {
    let output_model = parsed.output_model().to_string();
    let product_id = parsed.product_id();
    let mismatches: Vec<String> = [
        ("product_id", &phase_a_manifest.product_id, &product_id),
        (
            "output_model",
            &phase_a_manifest.output_model,
            &output_model,
        ),
        (
            "spec_path",
            &phase_a_manifest.spec_path,
            &spec_path.to_string(),
        ),
    ]
    .into_iter()
    .filter(|(_, recorded, current)| recorded != current)
    .map(|(name, recorded, current)| {
        format!("{name}: manifest {recorded:?} vs current {current:?}")
    })
    .collect();
    if !mismatches.is_empty() {
        return Err(SpecRejected::new(
            "phase-a-identity-mismatch",
            format!(
                "the Phase-A manifest belongs to a different generation identity: {}",
                mismatches.join("; ")
            ),
        ));
    }
    if phase_a_manifest.spec_digest != parsed.digest {
        return Err(SpecRejected::new(
            "spec-superseded",
            format!(
                "the Phase-A lowering was generated from spec {}, but the current spec digests \
                 to {} — the Phase-A generation is superseded (FF-DESIGN D6). Re-run Phase A \
                 under the current spec before the metadata merge; refusing to mix generations",
                phase_a_manifest.spec_digest, parsed.digest
            ),
        ));
    }
    let contract = contract_rel(parsed);
    if !phase_a_manifest.artifacts.contains_key(&contract) {
        return Err(SpecRejected::new(
            "phase-a-manifest-incomplete",
            format!(
                "the Phase-A manifest does not cover {contract} — the merged manifest would \
                 lose its contract byte-verification target; re-run Phase A"
            ),
        ));
    }

    let sidecar_relpath = sidecar_rel(parsed);
    let merged = merge_sidecar_text(parsed, sidecar_text, &sidecar_relpath)?.into_bytes();
    let mut hashes = phase_a_manifest.artifacts.clone();
    hashes.insert(sidecar_relpath.clone(), content_digest(&merged));
    let manifest = build_manifest(parsed, spec_path, Phase::B, hashes)?;
    Ok(Lowering {
        artifacts: vec![Artifact {
            relpath: sidecar_relpath,
            content: merged,
        }],
        manifest,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::product::spec::parse_spec_bytes;

    const SPEC_PATH: &str = "products/revenue_daily.toml";

    /// The answer key's spec fixture, copied byte for byte. Its digest is
    /// embedded in every golden, so the bytes cannot be retyped.
    const SPEC_FIXTURE: &[u8] = include_bytes!("testdata/revenue_daily.spec.toml");
    const GOLDEN_CONTRACT: &[u8] = include_bytes!("testdata/revenue_daily.contract.toml");
    const GOLDEN_MERGED: &[u8] = include_bytes!("testdata/revenue_daily.merged.toml");
    const GOLDEN_MANIFEST_A: &[u8] = include_bytes!("testdata/lowering-manifest.phase-a.json");
    const GOLDEN_MANIFEST_B: &[u8] = include_bytes!("testdata/lowering-manifest.merged.json");

    /// A sidecar exactly as the worker's drafting tool leaves it: the
    /// two-line header comment, then `name` and `intent`. Target and
    /// strategy are absent on purpose — they resolve from project
    /// conventions.
    const DRAFT_MODEL_SIDECAR: &str = concat!(
        "# Draft authored via the Rocky MCP `draft_model` tool. Target and strategy resolve\n",
        "# from the project's conventions (rocky.toml pipeline + _defaults.toml).\n",
        "name = \"revenue_daily\"\n",
        "intent = \"Daily gross revenue per client in EUR, refunds excluded\"\n",
    );

    const WORKER_CHECK_SPEC: &str =
        "[[tests]]\ntype = \"expression\"\nexpression = \"client_id > 0\"\n";

    /// The engine's `draft_check` string append, mirrored exactly.
    fn draft_check_append(prior: &str, spec: &str) -> String {
        let merged = format!("{}\n\n{}", prior.trim_end(), spec.trim_start_matches('\n'));
        format!("{}\n", merged.trim_end_matches('\n'))
    }

    fn parsed_d3() -> ParsedSpec {
        parse_spec_bytes(SPEC_FIXTURE, SPEC_PATH).expect("the fixture parses")
    }

    fn drafted_sidecar_text() -> String {
        draft_check_append(DRAFT_MODEL_SIDECAR, WORKER_CHECK_SPEC)
    }

    fn verified_phase_a_manifest(parsed: &ParsedSpec) -> Manifest {
        lower_phase_a(parsed, SPEC_PATH).expect("phase A").manifest
    }

    fn merged_text(parsed: &ParsedSpec, sidecar_text: &str) -> String {
        merge_sidecar_text(parsed, sidecar_text, "models/revenue_daily.toml").expect("merges")
    }

    fn merged_document(parsed: &ParsedSpec, sidecar_text: &str) -> TomlTable {
        parse_ordered(&merged_text(parsed, sidecar_text)).expect("the merge output is TOML")
    }

    fn test_entry(pairs: &[(&str, &str)]) -> TomlTable {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_string(), TomlValue::string(*value)))
            .collect()
    }

    fn tests_of(document: &TomlTable) -> Vec<TomlTable> {
        document["tests"]
            .as_array()
            .expect("tests is an array")
            .iter()
            .map(|test| test.as_table().expect("a test is a table").clone())
            .collect()
    }

    fn utf8(bytes: &[u8]) -> &str {
        std::str::from_utf8(bytes).expect("utf-8")
    }

    // ----- the fixture itself -----

    #[test]
    fn the_imported_fixture_still_digests_to_the_goldens_value() {
        // Every golden embeds this digest. A fixture mangled in transit
        // fails here, naming the cause, instead of failing four byte
        // comparisons that look like renderer bugs.
        assert_eq!(
            parsed_d3().digest,
            "sha256:54df46b9278c1f265f64fd23203396ea69e68771197d5d0e5a7291cd6ffd82ff"
        );
    }

    // ----- goldens -----

    #[test]
    fn golden_phase_a_contract() {
        let lowering = lower_phase_a(&parsed_d3(), SPEC_PATH).expect("phase A");
        assert_eq!(
            lowering.artifacts[0].relpath,
            "models/revenue_daily.contract.toml"
        );
        assert_eq!(
            utf8(&lowering.artifacts[0].content),
            utf8(GOLDEN_CONTRACT),
            "the contract must byte-match the frozen golden"
        );
    }

    #[test]
    fn golden_phase_a_manifest() {
        let lowering = lower_phase_a(&parsed_d3(), SPEC_PATH).expect("phase A");
        assert_eq!(
            utf8(&lowering.manifest.to_json_bytes()),
            utf8(GOLDEN_MANIFEST_A)
        );
    }

    #[test]
    fn golden_phase_b_sidecar() {
        let parsed = parsed_d3();
        let lowering = lower_phase_b(
            &parsed,
            SPEC_PATH,
            &drafted_sidecar_text(),
            &verified_phase_a_manifest(&parsed),
        )
        .expect("phase B");
        assert_eq!(lowering.artifacts[0].relpath, "models/revenue_daily.toml");
        assert_eq!(
            utf8(&lowering.artifacts[0].content),
            utf8(GOLDEN_MERGED),
            "the merged sidecar must byte-match the frozen golden"
        );
    }

    #[test]
    fn golden_phase_b_manifest() {
        let parsed = parsed_d3();
        let lowering = lower_phase_b(
            &parsed,
            SPEC_PATH,
            &drafted_sidecar_text(),
            &verified_phase_a_manifest(&parsed),
        )
        .expect("phase B");
        assert_eq!(
            utf8(&lowering.manifest.to_json_bytes()),
            utf8(GOLDEN_MANIFEST_B)
        );
    }

    #[test]
    fn golden_manifest_field_set_equals_spec_field_set() {
        let parsed = parsed_d3();
        let phase_a = lower_phase_a(&parsed, SPEC_PATH).expect("phase A");
        assert_total(&parsed.spec, &phase_a.manifest).expect("phase A is total");
        let phase_b = lower_phase_b(
            &parsed,
            SPEC_PATH,
            &drafted_sidecar_text(),
            &verified_phase_a_manifest(&parsed),
        )
        .expect("phase B");
        assert_total(&parsed.spec, &phase_b.manifest).expect("phase B is total");
    }

    #[test]
    fn lowering_is_deterministic() {
        let parsed = parsed_d3();
        let manifest_a = verified_phase_a_manifest(&parsed);
        let sidecar = drafted_sidecar_text();
        let first = lower_phase_b(&parsed, SPEC_PATH, &sidecar, &manifest_a).expect("phase B");
        let second = lower_phase_b(&parsed, SPEC_PATH, &sidecar, &manifest_a).expect("phase B");
        assert_eq!(first.artifacts[0].content, second.artifacts[0].content);
        assert_eq!(
            first.manifest.to_json_bytes(),
            second.manifest.to_json_bytes()
        );
    }

    // ----- the generation-identity guards -----

    #[test]
    fn phase_b_refuses_a_mismatched_digest_manifest() {
        // The real invariant at the public boundary: spec B's lowering
        // can never carry spec A's contract hash forward.
        let parsed = parsed_d3();
        let manifest_a = verified_phase_a_manifest(&parsed);
        let edited = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"checks = ["revenue_eur >= 0"]"#,
                r#"checks = ["revenue_eur > 0"]"#,
            );
        let spec_b = parse_spec_bytes(edited.as_bytes(), SPEC_PATH).expect("valid");
        assert_ne!(spec_b.digest, parsed.digest);

        let error = lower_phase_b(&spec_b, SPEC_PATH, &drafted_sidecar_text(), &manifest_a)
            .expect_err("superseded");
        assert_eq!(error.code, "spec-superseded");
        // The manifest's generation…
        assert!(error.message.contains(&manifest_a.spec_digest), "{error}");
        // …and the current spec, both named.
        assert!(error.message.contains(&spec_b.digest), "{error}");
    }

    #[test]
    fn phase_b_refuses_a_foreign_identity_manifest() {
        let parsed = parsed_d3();
        let mut manifest_a = verified_phase_a_manifest(&parsed);
        manifest_a.spec_path = "products/elsewhere.toml".to_string();
        let error = lower_phase_b(&parsed, SPEC_PATH, &drafted_sidecar_text(), &manifest_a)
            .expect_err("foreign");
        assert_eq!(error.code, "phase-a-identity-mismatch");
        assert!(error.message.contains("spec_path"), "{error}");
    }

    #[test]
    fn phase_b_refuses_a_foreign_product_id_or_model() {
        // The other two identity fields, which no answer-key test pinned.
        let parsed = parsed_d3();
        for mutate in [
            |manifest: &mut Manifest| manifest.product_id = "product:other".to_string(),
            |manifest: &mut Manifest| manifest.output_model = "other".to_string(),
        ] {
            let mut manifest_a = verified_phase_a_manifest(&parsed);
            mutate(&mut manifest_a);
            let error = lower_phase_b(&parsed, SPEC_PATH, &drafted_sidecar_text(), &manifest_a)
                .expect_err("foreign");
            assert_eq!(error.code, "phase-a-identity-mismatch");
        }
    }

    #[test]
    fn phase_b_refuses_a_manifest_without_the_contract() {
        let parsed = parsed_d3();
        let mut incomplete = verified_phase_a_manifest(&parsed);
        incomplete.artifacts.clear();
        let error = lower_phase_b(&parsed, SPEC_PATH, &drafted_sidecar_text(), &incomplete)
            .expect_err("incomplete");
        assert_eq!(error.code, "phase-a-manifest-incomplete");
    }

    // ----- the contract -----

    #[test]
    fn the_contract_never_emits_the_inert_surfaces() {
        // The never-emit list: the contract must not dress inert engine
        // surfaces up as enforcement.
        let contract = String::from_utf8(render_contract(&parsed_d3())).expect("utf-8");
        for inert in [
            "no_new_nullable",
            "conditions",
            "models_dir",
            "contracts_dir",
        ] {
            assert!(!contract.contains(inert), "{inert} must never be emitted");
        }
    }

    #[test]
    fn the_generated_header_convention_holds_for_both_artifacts() {
        let parsed = parsed_d3();
        let header = format!(
            "# GENERATED from products/revenue_daily.toml (spec {}) — edit the spec, not this file.",
            parsed.digest
        );
        let contract = String::from_utf8(render_contract(&parsed)).expect("utf-8");
        assert!(contract.starts_with(&header), "{contract}");
        assert!(
            merged_text(&parsed, &drafted_sidecar_text()).starts_with(&header),
            "the merged sidecar carries the same header"
        );
    }

    // ----- the sidecar merge -----

    #[test]
    fn the_merge_preserves_agent_name_intent_and_appended_check() {
        let parsed = parsed_d3();
        let document = merged_document(&parsed, &drafted_sidecar_text());
        assert_eq!(document["name"], TomlValue::string("revenue_daily"));
        assert_eq!(
            document["intent"],
            TomlValue::string("Daily gross revenue per client in EUR, refunds excluded")
        );
        let worker_test = test_entry(&[("type", "expression"), ("expression", "client_id > 0")]);
        assert!(
            tests_of(&document).contains(&worker_test),
            "the worker's appended check must survive"
        );
    }

    #[test]
    fn the_merge_emits_the_spec_owned_blocks() {
        let parsed = parsed_d3();
        let document = merged_document(&parsed, &drafted_sidecar_text());
        assert_eq!(
            document["tags"].as_table().expect("tags")["product"],
            TomlValue::string("revenue_daily")
        );
        assert_eq!(
            *document["classification"]
                .as_table()
                .expect("classification"),
            test_entry(&[("client_id", "pii")])
        );
        let freshness = document["freshness"].as_table().expect("freshness");
        assert_eq!(
            freshness["expected_lag_seconds"],
            TomlValue::Integer(86_400)
        );
        assert_eq!(freshness["time_column"], TomlValue::string("date"));
        assert_eq!(
            document["sources"],
            TomlValue::Array(vec![TomlValue::Table(test_entry(&[
                ("catalog", "poc"),
                ("schema", "raw"),
                ("table", "stripe_charges"),
            ]))])
        );
    }

    #[test]
    fn the_merge_never_emits_freshness_severity() {
        // The spec accepts severity, but the sidecar never carries it: no
        // engine path reads it, so writing it would dress the file up as
        // enforcement that does not exist.
        let parsed = parsed_d3();
        let freshness = parsed
            .product()
            .output
            .freshness
            .as_ref()
            .expect("the fixture declares freshness");
        assert!(freshness.severity.is_some(), "accepted…");
        let document = merged_document(&parsed, &drafted_sidecar_text());
        assert!(
            !document["freshness"]
                .as_table()
                .expect("freshness")
                .contains_key("severity"),
            "…but never emitted"
        );
    }

    #[test]
    fn the_merge_generates_grain_not_null_and_expression_tests() {
        let parsed = parsed_d3();
        let tests = tests_of(&merged_document(&parsed, &drafted_sidecar_text()));
        let mut composite = TomlTable::new();
        composite.insert("type".to_string(), TomlValue::string("composite"));
        composite.insert("kind".to_string(), TomlValue::string("unique"));
        composite.insert(
            "columns".to_string(),
            TomlValue::Array(vec![
                TomlValue::string("client_id"),
                TomlValue::string("date"),
            ]),
        );
        assert!(tests.contains(&composite), "{tests:?}");
        for column in ["client_id", "date", "revenue_eur"] {
            assert!(
                tests.contains(&test_entry(&[("type", "not_null"), ("column", column)])),
                "missing not_null for {column}"
            );
        }
        assert!(tests.contains(&test_entry(&[
            ("type", "expression"),
            ("expression", "revenue_eur >= 0"),
            ("severity", "error"),
        ])));
    }

    #[test]
    fn a_single_column_grain_lowers_to_unique() {
        let single = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"grain = ["client_id", "date"]"#,
                r#"grain = ["client_id"]"#,
            );
        let parsed = parse_spec_bytes(single.as_bytes(), SPEC_PATH).expect("valid");
        let tests = generated_tests(&parsed);
        assert!(tests.contains(&test_entry(&[("type", "unique"), ("column", "client_id")])));
        assert!(
            !tests
                .iter()
                .any(|test| test.get("type") == Some(&TomlValue::string("composite"))),
            "a one-column grain never becomes a composite test"
        );
    }

    #[test]
    fn the_merge_is_idempotent() {
        let parsed = parsed_d3();
        let once = merged_text(&parsed, &drafted_sidecar_text());
        let twice = merged_text(&parsed, &once);
        assert_eq!(once, twice, "merging a merged sidecar must change nothing");
    }

    #[test]
    fn the_merge_preserves_unowned_tables_verbatim() {
        // A drafted project may carry target/strategy in the sidecar, or a
        // human may have added keys; everything outside the owned set
        // survives.
        let parsed = parsed_d3();
        let sidecar = format!(
            "{DRAFT_MODEL_SIDECAR}\n[target]\ncatalog = \"poc\"\nschema = \"gold\"\n\
             table = \"revenue_daily\"\n"
        );
        let document = merged_document(&parsed, &sidecar);
        assert_eq!(
            *document["target"].as_table().expect("target"),
            test_entry(&[
                ("catalog", "poc"),
                ("schema", "gold"),
                ("table", "revenue_daily"),
            ])
        );
    }

    #[test]
    fn the_merge_preserves_foreign_tag_keys() {
        let parsed = parsed_d3();
        let sidecar = format!("{DRAFT_MODEL_SIDECAR}\n[tags]\nlayer = \"gold\"\n");
        let document = merged_document(&parsed, &sidecar);
        assert_eq!(
            *document["tags"].as_table().expect("tags"),
            test_entry(&[("product", "revenue_daily"), ("layer", "gold")])
        );
    }

    #[test]
    fn the_merge_fills_intent_only_when_it_is_absent() {
        let parsed = parsed_d3();
        let intent_line = "intent = \"Daily gross revenue per client in EUR, refunds excluded\"\n";

        let without_intent = DRAFT_MODEL_SIDECAR.replace(intent_line, "");
        assert_eq!(
            merged_document(&parsed, &without_intent)["intent"],
            TomlValue::string(parsed.product().intent.clone()),
            "filled from the spec"
        );

        let reworded = DRAFT_MODEL_SIDECAR.replace(intent_line, "intent = \"worker wording\"\n");
        assert_eq!(
            merged_document(&parsed, &reworded)["intent"],
            TomlValue::string("worker wording"),
            "preserved, not clobbered"
        );
    }

    #[test]
    fn the_merge_rejects_an_unparseable_sidecar_naming_the_path() {
        let error = merge_sidecar_text(&parsed_d3(), "name = [broken", "models/revenue_daily.toml")
            .expect_err("unparseable");
        assert_eq!(error.code, "sidecar-unparseable");
        assert!(
            error.message.contains("models/revenue_daily.toml"),
            "{error}"
        );
    }

    #[test]
    fn a_worker_test_identical_to_a_generated_one_is_absorbed_once() {
        let parsed = parsed_d3();
        let duplicate = draft_check_append(
            DRAFT_MODEL_SIDECAR,
            "[[tests]]\ntype = \"not_null\"\ncolumn = \"client_id\"\n",
        );
        let wanted = test_entry(&[("type", "not_null"), ("column", "client_id")]);
        let count = tests_of(&merged_document(&parsed, &duplicate))
            .iter()
            .filter(|test| **test == wanted)
            .count();
        assert_eq!(count, 1);
    }

    // ----- rows the goldens do not reach -----

    #[test]
    fn an_empty_checks_or_classifications_list_still_gets_a_row() {
        let text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(r#"checks = ["revenue_eur >= 0"]"#, "")
            .replace(r#"classifications = { client_id = "pii" }"#, "");
        let parsed = parse_spec_bytes(text.as_bytes(), SPEC_PATH).expect("valid");
        let manifest = lower_phase_a(&parsed, SPEC_PATH).expect("phase A").manifest;
        let checks = &manifest.fields["product.output.checks"];
        // No Phase-B location, so the row is an accounted `artifact` with
        // nowhere to point — never `pending` forever.
        assert_eq!(checks.disposition, Disposition::Artifact);
        assert!(checks.locations.is_empty());
        assert_eq!(checks.note, "no checks declared — nothing to emit");
        let classifications = &manifest.fields["product.output.classifications"];
        assert_eq!(classifications.disposition, Disposition::Artifact);
        assert_eq!(classifications.note, "no classifications declared");
    }

    #[test]
    fn a_spec_without_an_output_model_still_lowers() {
        // The default model name is computed, never written back, so the
        // manifest has no `product.output.model` field to account for. A
        // row emitted regardless would fail the totality check on a
        // perfectly valid spec.
        let text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace("model = \"revenue_daily\"", "");
        let parsed = parse_spec_bytes(text.as_bytes(), SPEC_PATH).expect("valid");
        assert_eq!(parsed.output_model(), "revenue_daily");
        let manifest = lower_phase_a(&parsed, SPEC_PATH).expect("phase A").manifest;
        assert!(!manifest.fields.contains_key("product.output.model"));
    }

    #[test]
    fn a_spec_version_pin_is_recorded_as_an_identity_row() {
        let text = format!(
            "spec_version = 0\n{}",
            String::from_utf8(SPEC_FIXTURE.to_vec()).expect("utf-8")
        );
        let parsed = parse_spec_bytes(text.as_bytes(), SPEC_PATH).expect("valid");
        let manifest = lower_phase_a(&parsed, SPEC_PATH).expect("phase A").manifest;
        assert_eq!(
            manifest.fields["spec_version"].disposition,
            Disposition::Identity
        );
    }

    #[test]
    fn a_single_column_grain_changes_the_manifest_location_detail() {
        let single = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"grain = ["client_id", "date"]"#,
                r#"grain = ["client_id"]"#,
            );
        let parsed = parse_spec_bytes(single.as_bytes(), SPEC_PATH).expect("valid");
        let manifest = lower_phase_b(
            &parsed,
            SPEC_PATH,
            &drafted_sidecar_text(),
            &verified_phase_a_manifest(&parsed),
        )
        .expect("phase B")
        .manifest;
        let grain = &manifest.fields["product.output.grain"];
        assert!(
            grain
                .locations
                .iter()
                .any(|location| location.detail == "[[tests]] type=unique")
        );
    }

    #[test]
    fn a_non_table_tags_value_is_replaced_not_carried() {
        // `tags` is owned, so a scalar sitting at that key is not a table
        // to preserve keys from — it is replaced wholesale.
        let parsed = parsed_d3();
        let sidecar = format!("{DRAFT_MODEL_SIDECAR}tags = \"not-a-table\"\n");
        let document = merged_document(&parsed, &sidecar);
        assert_eq!(
            *document["tags"].as_table().expect("tags"),
            test_entry(&[("product", "revenue_daily")])
        );
    }

    #[test]
    fn a_preserved_scalar_survives_and_stays_above_the_tables() {
        // An inline table puts a table-valued key BEFORE a scalar in the
        // source, which is the one arrangement that can invert the two.
        // The scalar must survive the merge and still render above every
        // table header, or the file it produces is not valid TOML.
        let parsed = parsed_d3();
        let sidecar = format!(
            "{DRAFT_MODEL_SIDECAR}target = {{ catalog = \"poc\" }}\nstrategy = \"merge\"\n"
        );
        let merged = merged_text(&parsed, &sidecar);
        let strategy = merged.find("strategy = ").expect("the scalar must survive");
        let target = merged.find("[target]").expect("the table must survive");
        assert!(
            strategy < target,
            "a preserved scalar must stay above every table:\n{merged}"
        );
        let document = parse_ordered(&merged).expect("valid TOML");
        assert_eq!(document["strategy"], TomlValue::string("merge"));
        assert_eq!(
            *document["target"].as_table().expect("target"),
            test_entry(&[("catalog", "poc")])
        );
    }

    #[test]
    fn a_nullable_column_gets_no_not_null_test() {
        // Every column in the fixture is non-nullable, so the goldens
        // cannot tell the guard from its absence. This can.
        let text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(
                r#"{ name = "revenue_eur", type = "Decimal(18,2)", nullable = false },"#,
                r#"{ name = "revenue_eur", type = "Decimal(18,2)", nullable = true },"#,
            );
        let parsed = parse_spec_bytes(text.as_bytes(), SPEC_PATH).expect("valid");
        let tests = generated_tests(&parsed);
        assert!(
            !tests.contains(&test_entry(&[
                ("type", "not_null"),
                ("column", "revenue_eur")
            ])),
            "a nullable column must not be asserted non-null: {tests:?}"
        );
        // …and the non-nullable ones still are.
        assert!(tests.contains(&test_entry(&[
            ("type", "not_null"),
            ("column", "client_id")
        ])));
    }

    #[test]
    fn a_freshness_budget_too_wide_for_toml_is_refused() {
        // The budget grammar accepts more seconds than a TOML integer can
        // hold. Emitting one would write a sidecar that does not parse
        // back, so the merge refuses instead.
        let text = String::from_utf8(SPEC_FIXTURE.to_vec())
            .expect("utf-8")
            .replace(r#"max_lag = "24h""#, r#"max_lag = "200000000000000d""#);
        let parsed = parse_spec_bytes(text.as_bytes(), SPEC_PATH).expect("the grammar accepts it");
        let error = merge_sidecar_text(
            &parsed,
            &drafted_sidecar_text(),
            "models/revenue_daily.toml",
        )
        .expect_err("unrepresentable");
        assert_eq!(error.code, "max-lag-unrepresentable");
    }
}
