//! The lowering manifest: every spec field accounted for, or it is a bug.
//!
//! The lowering is published as a manifest per run — every spec field
//! maps to an artifact location, a verification, an identity input, or a
//! refusal with a reason. Nothing is silent.
//!
//! Granularity is one row per spec field as the design names them
//! (`product.name`, `product.output.freshness`, ...). Sub-keys of a
//! field that lowers as one unit (`freshness.max_lag` and friends) are
//! covered by their field's row, and that coverage is MECHANIZED down to
//! the leaves: [`spec_leaf_coverage`] derives every nested model's leaf
//! set from the schemars schema and compares it against the explicit
//! [`covered_leaf_fields`] claim, so a field newly added to `ColumnSpec`
//! or `FreshnessSpec` breaks totality until someone accounts for it.
//! `deny_unknown_fields` alone cannot do this: it rejects unknown INPUT
//! keys, but it never notices a new schema field the lowering drops.
//!
//! [`spec_row_paths`] extends the same trick one level up, to the rows
//! themselves: a field added to a *container* model (`OutputSpec`,
//! `ProductSpec`) appears in the schema-derived row set and breaks the
//! hand-written instance walk that has to stay equal to it.
//!
//! The manifest also carries a content hash per emitted artifact — the
//! byte-verification target that catches a worker who mutated a
//! spec-owned file through any route.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::product::spec::{SpecFile, SpecRejected, SpecResult, spec_digest};

/// The file name the manifest is committed under.
pub const MANIFEST_FILENAME: &str = "lowering-manifest.json";

/// What became of one spec field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Disposition {
    /// Lowered into the listed locations.
    Artifact,
    /// A Phase-B field recorded by the Phase-A manifest. The merged
    /// manifest carries none of these.
    Pending,
    /// Verified rather than written (the trust posture).
    Verified,
    /// Consumed as an identity or versioning input; no file.
    Identity,
    /// Not lowerable; `reject_reason` says why.
    Reject,
}

/// Which lowering run a manifest describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ManifestPhase {
    /// Phase A: the pre-draft contract.
    LoweredContract,
    /// Phase B: the post-draft sidecar merge.
    Merged,
}

/// Which lowering phase emits a location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LocationPhase {
    /// The pre-draft contract.
    A,
    /// The post-draft sidecar merge.
    B,
}

/// One place a spec field landed: a project-root-relative path plus what
/// of the file it accounts for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    /// Project-root-relative POSIX path.
    pub path: String,
    /// Which part of that file this field lands in.
    pub detail: String,
    /// The phase that emits it.
    pub phase: LocationPhase,
}

impl Location {
    /// A location in `path`, describing `detail`, emitted by `phase`.
    pub fn new(path: impl Into<String>, detail: impl Into<String>, phase: LocationPhase) -> Self {
        Self {
            path: path.into(),
            detail: detail.into(),
            phase,
        }
    }
}

/// The disposition of one spec field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FieldRow {
    /// What became of the field.
    pub disposition: Disposition,
    /// Where it landed, when it landed anywhere.
    #[serde(default)]
    pub locations: Vec<Location>,
    /// Why this disposition, in plain language.
    #[serde(default)]
    pub note: String,
    /// Set only on a [`Disposition::Reject`] row.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reject_reason: Option<String>,
}

impl FieldRow {
    /// A row with a disposition and nothing else.
    pub fn new(disposition: Disposition) -> Self {
        Self {
            disposition,
            locations: Vec::new(),
            note: String::new(),
            reject_reason: None,
        }
    }

    /// The same row with a note attached.
    #[must_use]
    pub fn with_note(mut self, note: impl Into<String>) -> Self {
        self.note = note.into();
        self
    }

    /// The same row with locations attached.
    #[must_use]
    pub fn with_locations(mut self, locations: Vec<Location>) -> Self {
        self.locations = locations;
        self
    }

    /// The same row with a refusal reason attached.
    #[must_use]
    pub fn with_reject_reason(mut self, reason: impl Into<String>) -> Self {
        self.reject_reason = Some(reason.into());
        self
    }
}

/// The total lowering manifest, regenerated on every lowering run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// `product:<name>`.
    pub product_id: String,
    /// `sha256:<hex>` over the spec's raw bytes.
    pub spec_digest: String,
    /// Project-root-relative path of the spec this run read.
    pub spec_path: String,
    /// The resolved output model name.
    pub output_model: String,
    /// Which run this manifest describes.
    pub phase: ManifestPhase,
    /// One row per spec field.
    pub fields: BTreeMap<String, FieldRow>,
    /// Emitted-artifact content hashes: relative path -> `sha256:<hex>`.
    #[serde(default)]
    pub artifacts: BTreeMap<String, String>,
}

/// An artifact's content hash, in the same spelling as the spec digest.
///
/// The same function as [`spec_digest`], named for its other job: the
/// spec digest is an identity, this is a tamper-detection target, and
/// the shared spelling is what lets a reader compare them by eye.
pub fn content_digest(data: &[u8]) -> String {
    spec_digest(data)
}

// ---------------------------------------------------------------------------
// Deterministic serialization
// ---------------------------------------------------------------------------

impl Manifest {
    /// Serialize deterministically: sorted keys, two-space indent, an
    /// escape for every non-ASCII character, and a trailing newline.
    ///
    /// Written by hand rather than through a serializer's pretty printer
    /// because these bytes are hashed, committed, and byte-compared
    /// across two implementations. A pretty printer's key order and
    /// escaping are its own business and can move under a dependency
    /// bump; this cannot. The manifest carries no timestamps, so equal
    /// inputs give equal bytes by construction.
    pub fn to_json_bytes(&self) -> Vec<u8> {
        let mut out = String::new();
        out.push_str("{\n");
        // Keys are emitted in sorted order, which for the fixed struct
        // fields means this hand-written sequence.
        json_key(&mut out, 1, "artifacts");
        json_string_map(&mut out, 1, &self.artifacts);
        out.push_str(",\n");
        json_key(&mut out, 1, "fields");
        json_field_map(&mut out, 1, &self.fields);
        out.push_str(",\n");
        json_key(&mut out, 1, "output_model");
        json_string(&mut out, &self.output_model);
        out.push_str(",\n");
        json_key(&mut out, 1, "phase");
        json_string(
            &mut out,
            match self.phase {
                ManifestPhase::LoweredContract => "lowered_contract",
                ManifestPhase::Merged => "merged",
            },
        );
        out.push_str(",\n");
        json_key(&mut out, 1, "product_id");
        json_string(&mut out, &self.product_id);
        out.push_str(",\n");
        json_key(&mut out, 1, "spec_digest");
        json_string(&mut out, &self.spec_digest);
        out.push_str(",\n");
        json_key(&mut out, 1, "spec_path");
        json_string(&mut out, &self.spec_path);
        out.push('\n');
        out.push_str("}\n");
        out.into_bytes()
    }

    /// Read a manifest back from its committed bytes.
    ///
    /// # Errors
    ///
    /// Returns `manifest-unreadable` when the bytes are not a manifest.
    pub fn from_json_bytes(raw: &[u8]) -> SpecResult<Self> {
        serde_json::from_slice(raw).map_err(|err| {
            SpecRejected::new(
                "manifest-unreadable",
                format!("the lowering manifest does not parse: {err}"),
            )
        })
    }
}

fn indent(out: &mut String, level: usize) {
    for _ in 0..level {
        out.push_str("  ");
    }
}

fn json_key(out: &mut String, level: usize, key: &str) {
    indent(out, level);
    json_string(out, key);
    out.push_str(": ");
}

fn json_string_map(out: &mut String, level: usize, map: &BTreeMap<String, String>) {
    if map.is_empty() {
        out.push_str("{}");
        return;
    }
    out.push_str("{\n");
    for (index, (key, value)) in map.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n");
        }
        json_key(out, level + 1, key);
        json_string(out, value);
    }
    out.push('\n');
    indent(out, level);
    out.push('}');
}

fn json_field_map(out: &mut String, level: usize, map: &BTreeMap<String, FieldRow>) {
    if map.is_empty() {
        out.push_str("{}");
        return;
    }
    out.push_str("{\n");
    for (index, (key, row)) in map.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n");
        }
        json_key(out, level + 1, key);
        json_field_row(out, level + 1, row);
    }
    out.push('\n');
    indent(out, level);
    out.push('}');
}

fn json_field_row(out: &mut String, level: usize, row: &FieldRow) {
    out.push_str("{\n");
    json_key(out, level + 1, "disposition");
    json_string(
        out,
        match row.disposition {
            Disposition::Artifact => "artifact",
            Disposition::Pending => "pending",
            Disposition::Verified => "verified",
            Disposition::Identity => "identity",
            Disposition::Reject => "reject",
        },
    );
    out.push_str(",\n");
    json_key(out, level + 1, "locations");
    json_locations(out, level + 1, &row.locations);
    out.push_str(",\n");
    json_key(out, level + 1, "note");
    json_string(out, &row.note);
    // Sorted key order puts an optional `reject_reason` last, and an
    // absent one is omitted rather than written as null.
    if let Some(reason) = &row.reject_reason {
        out.push_str(",\n");
        json_key(out, level + 1, "reject_reason");
        json_string(out, reason);
    }
    out.push('\n');
    indent(out, level);
    out.push('}');
}

fn json_locations(out: &mut String, level: usize, locations: &[Location]) {
    if locations.is_empty() {
        out.push_str("[]");
        return;
    }
    out.push_str("[\n");
    for (index, location) in locations.iter().enumerate() {
        if index > 0 {
            out.push_str(",\n");
        }
        indent(out, level + 1);
        out.push_str("{\n");
        json_key(out, level + 2, "detail");
        json_string(out, &location.detail);
        out.push_str(",\n");
        json_key(out, level + 2, "path");
        json_string(out, &location.path);
        out.push_str(",\n");
        json_key(out, level + 2, "phase");
        json_string(
            out,
            match location.phase {
                LocationPhase::A => "A",
                LocationPhase::B => "B",
            },
        );
        out.push('\n');
        indent(out, level + 1);
        out.push('}');
    }
    out.push('\n');
    indent(out, level);
    out.push(']');
}

/// A JSON string literal, ASCII-only.
///
/// Everything outside printable ASCII is escaped, so the committed bytes
/// are readable and diffable on any terminal and cannot depend on a
/// locale. Characters above the basic plane are written as the surrogate
/// pair JSON requires.
fn json_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ' '..='~' => out.push(ch),
            ch => {
                let code = ch as u32;
                if code > 0xFFFF {
                    let offset = code - 0x1_0000;
                    let _ = write!(
                        out,
                        "\\u{:04x}\\u{:04x}",
                        0xD800 + (offset >> 10),
                        0xDC00 + (offset & 0x3FF)
                    );
                } else {
                    let _ = write!(out, "\\u{code:04x}");
                }
            }
        }
    }
    out.push('"');
}

// ---------------------------------------------------------------------------
// Totality, mechanized from the schema
// ---------------------------------------------------------------------------

/// The container models: the fixed structure the row walker recurses
/// through, rather than stopping at.
///
/// A model outside this list lowers as ONE unit and earns exactly one
/// manifest row, whose leaf coverage [`covered_leaf_fields`] claims.
/// This is a claim about the lowering, not a mirror of the schema —
/// never regenerate it mechanically. Promoting a model into this list
/// says "its fields get their own rows"; that is a design decision.
const CONTAINER_MODELS: &[&str] = &[
    "SpecFile",
    "ProductSpec",
    "SourceSpec",
    "OutputSpec",
    "TrustSpec",
];

/// For each row whose field holds a nested unit-lowered model, the EXACT
/// leaf fields that row's disposition accounts for.
///
/// [`check_leaf_coverage`] derives the required set from the schema and
/// fails on any difference, so adding a field to `ColumnSpec` or
/// `FreshnessSpec` breaks totality until a human both lists the leaf
/// here and gives it a real disposition in the lowering. Like
/// [`CONTAINER_MODELS`], this is a claim, not a mirror.
const COVERED_LEAF_FIELDS: &[(&str, &[&str])] = &[
    ("product.output.columns", &["name", "nullable", "type"]),
    (
        "product.output.freshness",
        &["max_lag", "severity", "time_column"],
    ),
];

/// The declared leaf coverage, as a map.
pub fn covered_leaf_fields() -> BTreeMap<String, BTreeSet<String>> {
    COVERED_LEAF_FIELDS
        .iter()
        .map(|(path, leaves)| {
            (
                (*path).to_string(),
                leaves.iter().map(|leaf| (*leaf).to_string()).collect(),
            )
        })
        .collect()
}

/// The spec schema, as a JSON document.
///
/// Walked as JSON rather than through schemars' own types: the shapes
/// this module cares about (`properties`, `$ref`, `items`,
/// `additionalProperties`) are stable parts of JSON Schema, while the
/// Rust types that carry them are a dependency's internals.
fn spec_schema() -> serde_json::Value {
    let schema = schemars::schema_for!(SpecFile);
    serde_json::to_value(schema).expect("the spec schema serializes to JSON")
}

/// The definition name a schema fragment refers to, when it refers to a
/// model rather than describing a scalar.
///
/// Reaches through the wrappers schemars emits for a required nested
/// model (`allOf`), an optional one (`anyOf` with a null branch), and a
/// list of them (`items`). A `$ref` to a definition with no `properties`
/// — an enum, say — is NOT a model: it lowers as a scalar leaf, exactly
/// as the answer key treats a `Literal[...]`.
fn referenced_models(fragment: &serde_json::Value, definitions: &serde_json::Value) -> Vec<String> {
    let mut found = Vec::new();
    if let Some(reference) = fragment.get("$ref").and_then(serde_json::Value::as_str) {
        let name = reference.rsplit('/').next().unwrap_or_default();
        if definitions
            .get(name)
            .and_then(|definition| definition.get("properties"))
            .is_some()
        {
            found.push(name.to_string());
        }
        return found;
    }
    for key in ["allOf", "anyOf", "oneOf"] {
        if let Some(branches) = fragment.get(key).and_then(serde_json::Value::as_array) {
            for branch in branches {
                found.extend(referenced_models(branch, definitions));
            }
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(inner) = fragment.get(key) {
            found.extend(referenced_models(inner, definitions));
        }
    }
    found
}

fn properties_of<'s>(
    model: &str,
    definitions: &'s serde_json::Value,
    root: &'s serde_json::Value,
) -> Vec<(String, &'s serde_json::Value)> {
    // The root model is inlined by schemars rather than listed among the
    // definitions, so it is looked up separately.
    let source = if model == "SpecFile" {
        root
    } else {
        definitions.get(model).unwrap_or_else(|| {
            panic!(
                "the spec schema carries no definition for '{model}', yet the leaf walk \
                 reached it as a model. The walk only names models it has already resolved, \
                 so this means the schema shape and this walk disagree — and an empty field \
                 set here would hide every field of '{model}' from the totality check."
            )
        })
    };
    // Reached only for a model the walk resolved, which by construction has
    // properties. Loud rather than empty for the same reason: silence here is
    // indistinguishable from "this model has no fields to account for".
    let properties = source
        .get("properties")
        .and_then(serde_json::Value::as_object)
        .unwrap_or_else(|| {
            panic!(
                "the spec schema defines no properties for '{model}', yet the leaf walk \
                 reached it as a model — the totality check would silently cover nothing."
            )
        });
    properties
        .iter()
        .map(|(name, fragment)| (name.clone(), fragment))
        .collect()
}

/// A unit-lowered model's leaf field names, dotted where models nest.
fn leaf_fields_of(
    model: &str,
    prefix: &str,
    definitions: &serde_json::Value,
    root: &serde_json::Value,
) -> BTreeSet<String> {
    let mut leaves = BTreeSet::new();
    for (name, fragment) in properties_of(model, definitions, root) {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let nested = referenced_models(fragment, definitions);
        if nested.is_empty() {
            leaves.insert(path);
        } else {
            for model in nested {
                leaves.extend(leaf_fields_of(&model, &path, definitions, root));
            }
        }
    }
    leaves
}

/// Row path -> the leaf fields under it, derived from the schema.
///
/// Walks the model TYPES, recursing through the containers and stopping
/// at each unit-lowered model, whose whole leaf set becomes that row's
/// required coverage. A scalar row covers itself and produces no entry.
pub fn spec_leaf_coverage() -> BTreeMap<String, BTreeSet<String>> {
    let root = spec_schema();
    let definitions = root
        .get("definitions")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let mut derived = BTreeMap::new();
    walk_leaf_coverage("SpecFile", "", &definitions, &root, &mut derived);
    derived
}

fn walk_leaf_coverage(
    model: &str,
    prefix: &str,
    definitions: &serde_json::Value,
    root: &serde_json::Value,
    derived: &mut BTreeMap<String, BTreeSet<String>>,
) {
    for (name, fragment) in properties_of(model, definitions, root) {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let nested = referenced_models(fragment, definitions);
        let mut leaves = BTreeSet::new();
        for referenced in nested {
            if CONTAINER_MODELS.contains(&referenced.as_str()) {
                walk_leaf_coverage(&referenced, &path, definitions, root, derived);
            } else {
                leaves.extend(leaf_fields_of(&referenced, "", definitions, root));
            }
        }
        if !leaves.is_empty() {
            derived.insert(path, leaves);
        }
    }
}

/// Every row path the schema declares, present or absent in any one spec.
///
/// The type-level companion to [`spec_field_paths`]: one path per
/// non-container field, recursing through the containers. Added coverage
/// beyond the answer key, which mechanizes only the leaves of the unit
/// models — a field added to a CONTAINER model was invisible to it.
pub fn spec_row_paths() -> BTreeSet<String> {
    let root = spec_schema();
    let definitions = root
        .get("definitions")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let mut paths = BTreeSet::new();
    walk_row_paths("SpecFile", "", &definitions, &root, &mut paths);
    paths
}

fn walk_row_paths(
    model: &str,
    prefix: &str,
    definitions: &serde_json::Value,
    root: &serde_json::Value,
    paths: &mut BTreeSet<String>,
) {
    for (name, fragment) in properties_of(model, definitions, root) {
        let path = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}.{name}")
        };
        let containers: Vec<String> = referenced_models(fragment, definitions)
            .into_iter()
            .filter(|model| CONTAINER_MODELS.contains(&model.as_str()))
            .collect();
        if containers.is_empty() {
            paths.insert(path);
        } else {
            for container in containers {
                walk_row_paths(&container, &path, definitions, root, paths);
            }
        }
    }
}

/// The field set of one parsed spec, at row granularity.
///
/// An optional field that is absent is not part of the instance's field
/// set — there is nothing to account for. A field that defaults to empty
/// is ALWAYS accounted: an empty declaration is a disposition, not a
/// silence.
///
/// Hand-written, and held to [`spec_row_paths`] by a test: a field added
/// to a container model appears there and not here, and that difference
/// is the failure.
pub fn spec_field_paths(spec: &SpecFile) -> BTreeSet<String> {
    let mut paths = BTreeSet::new();
    if spec.spec_version.is_some() {
        paths.insert("spec_version".to_string());
    }
    paths.insert("product.name".to_string());
    paths.insert("product.intent".to_string());
    paths.insert("product.source.tables".to_string());
    if spec.product.output.model.is_some() {
        paths.insert("product.output.model".to_string());
    }
    paths.insert("product.output.grain".to_string());
    paths.insert("product.output.columns".to_string());
    paths.insert("product.output.checks".to_string());
    if spec.product.output.freshness.is_some() {
        paths.insert("product.output.freshness".to_string());
    }
    paths.insert("product.output.classifications".to_string());
    paths.insert("product.trust.agent".to_string());
    paths
}

/// Check the declared leaf coverage against the schema-derived one.
///
/// Takes the claim as an argument so a test can hand it a mutated map
/// and watch the check fail — the guard is exercised, not assumed.
///
/// # Errors
///
/// Returns `manifest-not-total` naming the drift in either direction: a
/// leaf the schema carries but the claim does not is an unaccounted spec
/// field, and a claimed leaf the schema lost is a stale claim.
pub fn check_leaf_coverage(declared: &BTreeMap<String, BTreeSet<String>>) -> SpecResult<()> {
    let derived = spec_leaf_coverage();
    let mut problems: Vec<String> = Vec::new();
    let paths: BTreeSet<&String> = derived.keys().chain(declared.keys()).collect();
    for path in paths {
        let empty = BTreeSet::new();
        let required = derived.get(path).unwrap_or(&empty);
        let claimed = declared.get(path).unwrap_or(&empty);
        let uncovered: Vec<&str> = required.difference(claimed).map(String::as_str).collect();
        let stale: Vec<&str> = claimed.difference(required).map(String::as_str).collect();
        if !uncovered.is_empty() {
            problems.push(format!(
                "{path}: leaf field(s) {} have no covered disposition",
                uncovered.join(", ")
            ));
        }
        if !stale.is_empty() {
            problems.push(format!(
                "{path}: covered leaf field(s) {} no longer exist in the schema",
                stale.join(", ")
            ));
        }
    }
    if problems.is_empty() {
        return Ok(());
    }
    Err(SpecRejected::new(
        "manifest-not-total",
        format!(
            "lowering leaf coverage is not total: {}",
            problems.join("; ")
        ),
    ))
}

/// [`check_leaf_coverage`] against the committed claim.
///
/// # Errors
///
/// Returns `manifest-not-total`; see [`check_leaf_coverage`].
pub fn assert_leaf_coverage() -> SpecResult<()> {
    check_leaf_coverage(&covered_leaf_fields())
}

/// Hard totality check, at both granularities.
///
/// 1. Leaf coverage, at the type level: every leaf field of every
///    unit-lowered model is explicitly accounted for.
/// 2. Row totality, at the instance level: the manifest's field set
///    EQUALS the spec's.
///
/// # Errors
///
/// Returns `manifest-not-total` naming the drift. A spec field with no
/// disposition is a lowering bug; a manifest row for a field the spec
/// does not carry is a stale manifest. Returned rather than panicked so
/// the caller refuses the run instead of aborting the process, and so a
/// test can pin the code.
pub fn assert_total(spec: &SpecFile, manifest: &Manifest) -> SpecResult<()> {
    check_total(spec, manifest, &covered_leaf_fields())
}

/// [`assert_total`] against a caller-supplied coverage claim.
///
/// # Errors
///
/// Returns `manifest-not-total`; see [`assert_total`].
pub fn check_total(
    spec: &SpecFile,
    manifest: &Manifest,
    declared: &BTreeMap<String, BTreeSet<String>>,
) -> SpecResult<()> {
    check_leaf_coverage(declared)?;
    let expected = spec_field_paths(spec);
    let actual: BTreeSet<String> = manifest.fields.keys().cloned().collect();
    let missing: Vec<&str> = expected.difference(&actual).map(String::as_str).collect();
    let stale: Vec<&str> = actual.difference(&expected).map(String::as_str).collect();
    if missing.is_empty() && stale.is_empty() {
        return Ok(());
    }
    Err(SpecRejected::new(
        "manifest-not-total",
        format!(
            "lowering manifest is not total: unaccounted spec fields [{}]; \
             stale manifest rows [{}]",
            missing.join(", "),
            stale.join(", ")
        ),
    ))
}

/// Byte-verify every manifest-listed artifact against its recorded hash.
///
/// Returns the mismatches — a missing file or a hash drift — and an
/// empty list when the tree is clean. This is the precondition that runs
/// before anything a worker touched is trusted.
/// Resolve a manifest artifact key against the project root, or refuse it.
///
/// A manifest is a file on disk, so its keys are input rather than fact. Two
/// shapes have to be refused before the path is ever used:
///
/// - an absolute key, because `Path::join` DISCARDS the root when the argument
///   is absolute, so `/etc/passwd` would be read as itself rather than as a
///   file inside the project;
/// - any `..` (or `.`) component, which walks out of the project.
///
/// One rule covers both: every component must be a plain name. An absolute
/// path leads with a root (or, on Windows, a prefix) component, and traversal
/// shows up as `..`, so neither is `Component::Normal`. An explicit
/// `is_absolute()` test was written here first and removed: mutation testing
/// showed nothing broke when it was deleted, and a guard that cannot fail is a
/// guard a later reader trusts for the wrong reason.
///
/// Verification only reads today, so the immediate blast radius is a wrong
/// answer about a file that is none of the manifest's business. The reason to
/// refuse here anyway is that this result is what a caller trusts when it
/// decides a generation is intact, and the same keys drive writes later.
pub(super) fn contained_artifact_path(project_root: &Path, rel_path: &str) -> Option<PathBuf> {
    let candidate = Path::new(rel_path);
    if rel_path.is_empty() {
        return None;
    }
    if candidate
        .components()
        .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return None;
    }
    Some(project_root.join(candidate))
}

pub fn verify_artifact_hashes(project_root: &Path, manifest: &Manifest) -> Vec<String> {
    let mut problems = Vec::new();
    for (rel_path, expected) in &manifest.artifacts {
        let Some(artifact_path) = contained_artifact_path(project_root, rel_path) else {
            problems.push(format!(
                "{rel_path}: refused (not a project-relative path; a manifest names only \
                 files inside its own project)"
            ));
            continue;
        };
        if !artifact_path.is_file() {
            problems.push(format!("{rel_path}: missing (expected {expected})"));
            continue;
        }
        let Ok(bytes) = std::fs::read(&artifact_path) else {
            problems.push(format!("{rel_path}: unreadable (expected {expected})"));
            continue;
        };
        let actual = content_digest(&bytes);
        if &actual != expected {
            problems.push(format!(
                "{rel_path}: content drift (expected {expected}, found {actual})"
            ));
        }
    }
    problems
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::product::spec::{ParsedSpec, parse_spec_bytes};

    const MINIMAL: &str = r#"
[product]
name = "tiny"
intent = "x"

[product.source]
tables = ["c.s.t"]

[product.output]
grain = ["id"]
columns = [{ name = "id", type = "Int64", nullable = false }]

[product.trust]
agent = "propose_only"
"#;

    /// A spec that sets every optional field, so the instance walk can be
    /// held against the schema-derived row set.
    const MAXIMAL: &str = r#"
spec_version = 0

[product]
name = "tiny"
intent = "x"

[product.source]
tables = ["c.s.t"]

[product.output]
model = "tiny"
grain = ["id"]
columns = [{ name = "id", type = "Int64", nullable = false }]
checks = ["id > 0"]
freshness = { max_lag = "24h", time_column = "id", severity = "error" }
classifications = { id = "pii" }

[product.trust]
agent = "propose_only"
"#;

    fn parse(text: &str) -> ParsedSpec {
        parse_spec_bytes(text.as_bytes(), "<test>").expect("valid")
    }

    fn manifest_with_fields(parsed: &ParsedSpec, fields: BTreeMap<String, FieldRow>) -> Manifest {
        Manifest {
            product_id: parsed.product_id(),
            spec_digest: parsed.digest.clone(),
            spec_path: "products/tiny.toml".to_string(),
            output_model: parsed.output_model().to_string(),
            phase: ManifestPhase::Merged,
            fields,
            artifacts: BTreeMap::new(),
        }
    }

    fn verified_rows(parsed: &ParsedSpec) -> BTreeMap<String, FieldRow> {
        spec_field_paths(&parsed.spec)
            .into_iter()
            .map(|path| (path, FieldRow::new(Disposition::Verified)))
            .collect()
    }

    // ----- the field walk -----

    #[test]
    fn field_paths_skip_absent_optionals() {
        let parsed = parse(MINIMAL);
        let paths = spec_field_paths(&parsed.spec);
        assert!(!paths.contains("product.output.freshness"));
        assert!(!paths.contains("spec_version"));
        assert!(!paths.contains("product.output.model"));
        // An empty declaration is a disposition, not a silence.
        assert!(paths.contains("product.output.checks"));
        assert!(paths.contains("product.output.classifications"));
    }

    #[test]
    fn field_paths_include_present_optionals() {
        let paths = spec_field_paths(&parse(MAXIMAL).spec);
        assert!(paths.contains("product.output.freshness"));
        assert!(paths.contains("spec_version"));
        assert!(paths.contains("product.output.model"));
    }

    #[test]
    fn the_instance_walk_covers_every_row_the_schema_declares() {
        // The mechanization the answer key lacks: a field added to a
        // CONTAINER model shows up in the schema-derived set, and the
        // hand-written walk above does not produce it. A maximal spec
        // sets every optional, so the two sets must be equal.
        assert_eq!(spec_field_paths(&parse(MAXIMAL).spec), spec_row_paths());
    }

    // ----- totality -----

    #[test]
    fn assert_total_flags_an_unaccounted_spec_field() {
        let parsed = parse(MINIMAL);
        let mut rows = verified_rows(&parsed);
        rows.remove("product.output.grain");
        let error = assert_total(&parsed.spec, &manifest_with_fields(&parsed, rows))
            .expect_err("not total");
        assert_eq!(error.code, "manifest-not-total");
        assert!(error.message.contains("product.output.grain"), "{error}");
    }

    #[test]
    fn assert_total_flags_a_stale_manifest_row() {
        let parsed = parse(MINIMAL);
        let mut rows = verified_rows(&parsed);
        rows.insert(
            "product.output.ghost".to_string(),
            FieldRow::new(Disposition::Artifact),
        );
        let error = assert_total(&parsed.spec, &manifest_with_fields(&parsed, rows))
            .expect_err("not total");
        assert_eq!(error.code, "manifest-not-total");
        assert!(error.message.contains("ghost"), "{error}");
    }

    #[test]
    fn assert_total_passes_when_the_sets_are_equal() {
        let parsed = parse(MINIMAL);
        let rows = verified_rows(&parsed);
        assert_total(&parsed.spec, &manifest_with_fields(&parsed, rows)).expect("total");
    }

    #[test]
    fn a_reject_row_round_trips() {
        let parsed = parse(MINIMAL);
        let mut rows = verified_rows(&parsed);
        rows.insert(
            "product.output.grain".to_string(),
            FieldRow::new(Disposition::Reject).with_reject_reason("example: not lowerable"),
        );
        let manifest = manifest_with_fields(&parsed, rows);
        // A reject row still accounts for the field.
        assert_total(&parsed.spec, &manifest).expect("total");
        let round_tripped = Manifest::from_json_bytes(&manifest.to_json_bytes()).expect("reads");
        let row = &round_tripped.fields["product.output.grain"];
        assert_eq!(row.disposition, Disposition::Reject);
        assert!(row.reject_reason.is_some());
    }

    #[test]
    fn serialization_is_deterministic() {
        let parsed = parse(MINIMAL);
        // A `BTreeMap` cannot be handed rows in a different order, so the
        // insertion order is varied instead and the sorted output has to
        // absorb it.
        let forward = verified_rows(&parsed);
        let mut reversed = BTreeMap::new();
        for (path, row) in forward.iter().rev() {
            reversed.insert(path.clone(), row.clone());
        }
        assert_eq!(
            manifest_with_fields(&parsed, forward).to_json_bytes(),
            manifest_with_fields(&parsed, reversed).to_json_bytes()
        );
    }

    // ----- leaf coverage -----

    #[test]
    fn leaf_coverage_is_derived_from_the_schema_and_matches_the_claim() {
        let derived = spec_leaf_coverage();
        assert_eq!(derived, covered_leaf_fields());
        let leaves = |names: &[&str]| -> BTreeSet<String> {
            names.iter().map(|name| (*name).to_string()).collect()
        };
        // The derivation reaches the LEAVES of the nested models — the
        // exact fields a silent schema addition would hide behind the row.
        assert_eq!(
            derived["product.output.columns"],
            leaves(&["name", "nullable", "type"])
        );
        assert_eq!(
            derived["product.output.freshness"],
            leaves(&["max_lag", "severity", "time_column"])
        );
        assert_leaf_coverage().expect("the claim passes");
    }

    #[test]
    fn a_new_nested_leaf_breaks_totality() {
        // The checker's view of a newly added `ColumnSpec` field: the
        // schema-derived set carries a leaf the claim does not.
        let mut without_nullable = covered_leaf_fields();
        without_nullable.insert(
            "product.output.columns".to_string(),
            ["name", "type"].iter().map(|s| (*s).to_string()).collect(),
        );
        let error = check_leaf_coverage(&without_nullable).expect_err("uncovered");
        assert_eq!(error.code, "manifest-not-total");
        assert!(error.message.contains("nullable"), "{error}");

        let parsed = parse(MINIMAL);
        let rows = verified_rows(&parsed);
        let error = check_total(
            &parsed.spec,
            &manifest_with_fields(&parsed, rows),
            &without_nullable,
        )
        .expect_err("uncovered");
        assert!(error.message.contains("nullable"), "{error}");
    }

    #[test]
    fn a_stale_covered_leaf_breaks_totality() {
        let mut with_ghost = covered_leaf_fields();
        with_ghost
            .get_mut("product.output.columns")
            .expect("row")
            .insert("ghost".to_string());
        let error = check_leaf_coverage(&with_ghost).expect_err("stale");
        assert!(error.message.contains("ghost"), "{error}");
    }

    #[test]
    fn an_uncovered_aggregate_row_breaks_totality() {
        // A whole unit-lowered model missing from the claim cannot slip in.
        let mut without_freshness = covered_leaf_fields();
        without_freshness.remove("product.output.freshness");
        let error = check_leaf_coverage(&without_freshness).expect_err("uncovered");
        assert!(
            error.message.contains("product.output.freshness"),
            "{error}"
        );
    }

    // ----- byte verification -----

    #[test]
    fn verify_artifact_hashes_refuses_a_path_outside_the_project() {
        // A manifest key is input. An absolute key makes `Path::join` throw the
        // project root away, so this would otherwise read a file the manifest
        // has no business naming — and report on it as if it belonged to the
        // generation.
        let parsed = parse(MINIMAL);
        let root = tempfile::tempdir().expect("tempdir");
        let outside = root.path().join("outside.txt");
        std::fs::write(&outside, b"not mine").expect("write");
        for key in [
            outside.to_string_lossy().to_string(),
            "../escape.toml".to_string(),
            "./models/tiny.contract.toml".to_string(),
            "models/../../escape.toml".to_string(),
            String::new(),
        ] {
            let manifest = Manifest {
                product_id: parsed.product_id(),
                spec_digest: parsed.digest.clone(),
                output_model: parsed.output_model().to_string(),
                spec_path: "products/tiny.toml".to_string(),
                phase: ManifestPhase::LoweredContract,
                fields: BTreeMap::new(),
                artifacts: [(key.clone(), content_digest(b"not mine"))]
                    .into_iter()
                    .collect(),
            };
            let problems = verify_artifact_hashes(root.path(), &manifest);
            assert_eq!(problems.len(), 1, "{key:?} -> {problems:?}");
            assert!(
                problems[0].contains("refused"),
                "{key:?} must be refused, got {problems:?}"
            );
        }
        assert_eq!(
            std::fs::read(&outside).expect("still there"),
            b"not mine",
            "a refused key must not have been acted on"
        );
    }

    #[test]
    fn verify_artifact_hashes_detects_drift_and_absence() {
        let parsed = parse(MINIMAL);
        let root = tempfile::tempdir().expect("tempdir");
        let artifact = root.path().join("models/tiny.contract.toml");
        std::fs::create_dir_all(artifact.parent().expect("parent")).expect("mkdir");
        std::fs::write(&artifact, b"content-v1").expect("write");
        let manifest = Manifest {
            product_id: parsed.product_id(),
            spec_digest: parsed.digest.clone(),
            spec_path: "products/tiny.toml".to_string(),
            output_model: "tiny".to_string(),
            phase: ManifestPhase::LoweredContract,
            fields: BTreeMap::new(),
            artifacts: [(
                "models/tiny.contract.toml".to_string(),
                content_digest(b"content-v1"),
            )]
            .into_iter()
            .collect(),
        };
        assert!(verify_artifact_hashes(root.path(), &manifest).is_empty());

        std::fs::write(&artifact, b"tampered").expect("write");
        let problems = verify_artifact_hashes(root.path(), &manifest);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("drift"), "{}", problems[0]);

        std::fs::remove_file(&artifact).expect("unlink");
        let problems = verify_artifact_hashes(root.path(), &manifest);
        assert_eq!(problems.len(), 1);
        assert!(problems[0].contains("missing"), "{}", problems[0]);
    }

    // ----- the JSON writer -----

    #[test]
    fn json_escapes_everything_outside_printable_ascii() {
        let mut out = String::new();
        json_string(&mut out, "a\u{2014}b\u{1}\"\\\n\u{1F600}");
        // Lowercase hex, a surrogate pair above the basic plane, and not
        // one non-ASCII byte left in the output.
        assert_eq!(out, r#""a\u2014b\u0001\"\\\n\ud83d\ude00""#);
        assert!(out.is_ascii());
    }

    // ----- the schema walk itself -----

    #[test]
    #[should_panic(expected = "carries no definition")]
    fn an_unresolvable_model_stops_the_walk_instead_of_covering_nothing() {
        // The walk names only models it has already resolved, so this state
        // means the schema shape and the walk disagree. The failure mode being
        // pinned is the quiet one: returning an empty field set would let the
        // whole model vanish from the totality check while it still reported
        // success.
        let root = serde_json::json!({ "properties": {} });
        let definitions = serde_json::json!({});
        let _ = leaf_fields_of("Missing", "", &definitions, &root);
    }

    #[test]
    #[should_panic(expected = "defines no properties")]
    fn a_model_reached_without_properties_stops_the_walk() {
        let root = serde_json::json!({ "properties": {} });
        let definitions = serde_json::json!({ "Hollow": { "type": "object" } });
        let _ = leaf_fields_of("Hollow", "", &definitions, &root);
    }

    #[test]
    fn leaf_derivation_recurses_through_a_nested_unit_model() {
        // Today no unit model nests another, so the real schema cannot
        // exercise this branch — and an unexercised branch in a totality
        // checker is a hole waiting for the first nested model. A
        // synthetic schema of the shape schemars emits pins it now.
        let root = serde_json::json!({
            "properties": {
                "outer": { "$ref": "#/definitions/Outer" }
            }
        });
        let definitions = serde_json::json!({
            "Outer": {
                "properties": {
                    "plain": { "type": "string" },
                    "inner": {
                        "anyOf": [{ "$ref": "#/definitions/Inner" }, { "type": "null" }]
                    },
                    "tag": { "$ref": "#/definitions/Tag" }
                }
            },
            "Inner": { "properties": { "deep": { "type": "string" } } },
            // No `properties`: an enum, which lowers as a scalar leaf.
            "Tag": { "oneOf": [{ "type": "string", "enum": ["a"] }] }
        });
        let leaves = leaf_fields_of("Outer", "", &definitions, &root);
        let expected: BTreeSet<String> = ["plain", "inner.deep", "tag"]
            .iter()
            .map(|leaf| (*leaf).to_string())
            .collect();
        assert_eq!(leaves, expected);
    }

    #[test]
    fn a_ref_to_a_property_less_definition_is_a_leaf_not_a_model() {
        // `FreshnessSpec.severity` is exactly this shape in the real
        // schema. Treating the enum as a model would silently drop the
        // `severity` leaf from the coverage claim, and the derived map
        // would stop matching the claim.
        assert!(spec_leaf_coverage()["product.output.freshness"].contains("severity"));
    }
}
