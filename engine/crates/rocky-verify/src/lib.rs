//! Standalone offline verifier for `rocky-manifest` (v0.1 and v0.2) attestations.
//!
//! A rocky-manifest attests *which program, over which inputs, in which
//! environment produced a table version*, in a vendor-neutral serialized form.
//! This crate answers two questions about a manifest without any producing
//! engine installed:
//!
//! 1. **Is it well-formed?** — validate the document against the embedded
//!    [`MANIFEST_SCHEMA_V0_1`] JSON Schema, or [`MANIFEST_SCHEMA_V0_2`] when the
//!    document says `"manifest_version": "0.2"` (required fields, hex-encoded hash
//!    shapes, the `inputs_hash` / `inputs_proof_class` coupling, enum values).
//! 2. **Do the output bytes match?** — when the manifest carries content-
//!    addressed `output_hashes`, hash the referenced artifact files with BLAKE3
//!    and confirm each equals the recorded hash. This is pure byte arithmetic:
//!    no producing tool, no network, no trust in the manifest's own claims.
//!
//! The crate is deliberately free of any `rocky-*` engine dependency — the
//! point of an open manifest format is that a consumer can verify it with a
//! tiny tool, not the vendor's engine.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// The canonical JSON Schema for `rocky-manifest v0.1`, embedded at build time.
///
/// This is the exact schema the verifier validates against, so the tool and the
/// published spec can never drift: there is one source of truth.
pub const MANIFEST_SCHEMA_V0_1: &str = include_str!("../spec/rocky-manifest-v0.1.schema.json");

/// The `$id` of the embedded schema, used as its resource URL when compiling.
pub const MANIFEST_SCHEMA_ID: &str = "https://rocky-data.dev/spec/rocky-manifest-v0.1.schema.json";

/// The canonical JSON Schema for `rocky-manifest v0.2`, embedded at build time.
///
/// v0.2 is v0.1 plus the optional, unsigned, producer-asserted `policy`,
/// `checks` and `tests` objects.
pub const MANIFEST_SCHEMA_V0_2: &str = include_str!("../spec/rocky-manifest-v0.2.schema.json");

/// The `$id` of the embedded v0.2 schema, used as its resource URL when compiling.
pub const MANIFEST_SCHEMA_V0_2_ID: &str =
    "https://rocky-data.dev/spec/rocky-manifest-v0.2.schema.json";

/// Which embedded manifest schema to validate against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaVersion {
    /// `rocky-manifest v0.1`.
    V0_1,
    /// `rocky-manifest v0.2`.
    V0_2,
}

impl SchemaVersion {
    /// Pick the schema for a document: v0.2 only when its `manifest_version`
    /// is the string `"0.2"`, otherwise v0.1 (so a missing or unknown version
    /// is reported by the v0.1 `const` error).
    #[must_use]
    pub fn of_document(instance: &serde_json::Value) -> Self {
        match instance.get("manifest_version").and_then(|v| v.as_str()) {
            Some("0.2") => Self::V0_2,
            _ => Self::V0_1,
        }
    }

    /// The embedded schema source for this version.
    #[must_use]
    pub fn schema_source(self) -> &'static str {
        match self {
            Self::V0_1 => MANIFEST_SCHEMA_V0_1,
            Self::V0_2 => MANIFEST_SCHEMA_V0_2,
        }
    }

    /// The `$id` of this version's schema.
    #[must_use]
    pub fn schema_id(self) -> &'static str {
        match self {
            Self::V0_1 => MANIFEST_SCHEMA_ID,
            Self::V0_2 => MANIFEST_SCHEMA_V0_2_ID,
        }
    }

    /// A short label such as `"v0.1"`.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::V0_1 => "v0.1",
            Self::V0_2 => "v0.2",
        }
    }
}

/// A typed view of a rocky-manifest (v0.1 or v0.2) document.
///
/// Deserializing into this type is lenient by construction (it mirrors the
/// schema's optionality); it exists for ergonomic access to `output_hashes`
/// during byte-verification. Structural validation is the schema's job — see
/// [`validate_schema`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Manifest {
    /// The spec version this document claims to conform to (`"0.1"`).
    pub manifest_version: String,
    /// The producer's hash-scheme tag in force when the hashes were computed.
    pub hash_scheme: String,
    /// Who produced this attestation.
    pub producer: Producer,
    /// What the manifest attests.
    pub subject: Subject,
    /// The program-identity key (lowercase-hex BLAKE3).
    pub program_hash: String,
    /// The input-match key, when inputs were observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_hash: Option<String>,
    /// The strength of [`Self::inputs_hash`] (`"strong"` or `"heuristic"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inputs_proof_class: Option<String>,
    /// The environment key (lowercase-hex BLAKE3).
    pub env_hash: String,
    /// Byte-hashes of the produced output artifact file(s), on a content-
    /// addressed write path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_hashes: Option<Vec<OutputHash>>,
    /// The policy decision the producer asserts let this run proceed (v0.2).
    /// Unsigned and producer-asserted: a claim, not verified evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyClaim>,
    /// Data-check counts the producer asserts (v0.2). Unsigned and
    /// producer-asserted: a claim, not verified evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<CountsClaim>,
    /// Model-test counts the producer asserts (v0.2). Unsigned and
    /// producer-asserted: a claim, not verified evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tests: Option<CountsClaim>,
}

/// An unsigned, producer-asserted policy decision (v0.2).
///
/// This is a claim the producer makes about itself. Nothing in it is
/// byte-verifiable, so a verifier checks its shape only and never treats it as
/// evidence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicyClaim {
    /// The decision: `"allow"` or `"require_review"`.
    pub effect: String,
    /// The kind of principal the decision was made for (`"human"` or `"agent"`).
    pub principal: String,
    /// Index of the matching policy rule; absent for the policy default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rule_id: Option<u64>,
    /// Where the producer recorded the full decision in its own ledger.
    pub ledger_ref: String,
}

/// Unsigned, producer-asserted check or test counts (v0.2).
///
/// A claim, not verified evidence. The only property a verifier can check is
/// that `passed + failed + not_evaluated == total`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountsClaim {
    /// Number configured.
    pub total: u64,
    /// Number evaluated and passed.
    pub passed: u64,
    /// Number evaluated and failed.
    pub failed: u64,
    /// Number configured but not evaluated.
    pub not_evaluated: u64,
}

/// The producer-asserted v0.2 claims of a schema-valid v0.2 document.
///
/// Built from the same parsed value that was schema-validated, never from a
/// second read of the file. Every field is unsigned and unverified: a claim,
/// not evidence.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssertedClaims {
    /// The asserted policy decision, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub policy: Option<PolicyClaim>,
    /// The asserted data-check counts, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checks: Option<CountsClaim>,
    /// The asserted model-test counts, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tests: Option<CountsClaim>,
}

/// Who produced a manifest — vendor-neutral name plus version.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Producer {
    /// The producing tool's name, e.g. `"rocky"`.
    pub name: String,
    /// The producing tool's version string.
    pub version: String,
}

/// What a manifest attests: the produced table version and its linkage.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subject {
    /// The model / target name whose production this manifest attests.
    pub model: String,
    /// The producer's run identifier, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// When the production ran (RFC 3339), when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub produced_at: Option<String>,
    /// The recorded execution status, when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
}

/// One content-addressed output artifact's recorded byte-hash and location.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputHash {
    /// Lowercase-hex BLAKE3 of the artifact file's bytes.
    pub hash: String,
    /// Where the artifact bytes live, when recorded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// A failure to even begin verification (not a *verification* failure — those
/// are reported in [`VerifyReport`]).
#[derive(Debug, thiserror::Error)]
pub enum VerifyError {
    /// The manifest file could not be read.
    #[error("could not read manifest {path}: {source}")]
    Read {
        /// The path that failed to read.
        path: String,
        /// The underlying I/O error.
        source: std::io::Error,
    },
    /// The manifest bytes are not valid JSON.
    #[error("manifest is not valid JSON: {0}")]
    Json(#[source] serde_json::Error),
    /// The embedded schema failed to compile — a bug in this crate, not the
    /// manifest.
    #[error("internal: the embedded rocky-manifest schema failed to compile: {0}")]
    Schema(String),
}

/// The outcome of one output-artifact byte-check.
#[derive(Debug, Clone)]
pub struct ArtifactCheck {
    /// The recorded (expected) lowercase-hex BLAKE3.
    pub expected: String,
    /// The file that was hashed, when one was found.
    pub file: Option<PathBuf>,
    /// The BLAKE3 actually computed over the file's bytes, when one was found.
    pub actual: Option<String>,
    /// Whether `actual == expected`.
    pub matched: bool,
    /// A human note (why a file was not found, etc.).
    pub note: Option<String>,
}

/// The full result of verifying a manifest.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    /// Whether the document validates against the embedded schema for its
    /// `manifest_version` (v0.2 for `"0.2"`, otherwise v0.1), including the
    /// v0.2 count-sum check.
    pub schema_valid: bool,
    /// Human-readable schema violations (empty when [`Self::schema_valid`]).
    pub schema_errors: Vec<String>,
    /// Per-artifact byte-check results. Empty when byte-verification was not
    /// requested (no artifact directory) or the manifest carries no
    /// `output_hashes`.
    pub artifact_checks: Vec<ArtifactCheck>,
    /// The schema version the document was validated against.
    pub schema_version: SchemaVersion,
    /// The producer-asserted claims. `Some` only for a schema-valid v0.2
    /// document; always `None` for v0.1 and for any failed document.
    pub claims: Option<AssertedClaims>,
}

impl VerifyReport {
    /// The overall verdict: the document is schema-valid and every requested
    /// artifact byte-check matched.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.schema_valid && self.artifact_checks.iter().all(|c| c.matched)
    }
}

/// Validate a parsed manifest value against the embedded v0.1 schema.
///
/// This is the v0.1 function only: it rejects every v0.2 document, because
/// the v0.1 schema requires `manifest_version` to be `"0.1"`. To validate a
/// document of either version, pick the schema with
/// [`SchemaVersion::of_document`] and call [`validate_schema_for`] (or use
/// [`verify_value`] for the full verdict).
///
/// Returns the list of human-readable violations — empty means valid. The
/// embedded schema is compiled on each call; for a hot loop, compile once with
/// [`compile_schema`] and reuse.
///
/// # Errors
///
/// Returns [`VerifyError::Schema`] only if the *embedded* schema itself fails to
/// compile, which is a bug in this crate rather than a property of the input.
pub fn validate_schema(instance: &serde_json::Value) -> Result<Vec<String>, VerifyError> {
    validate_schema_for(SchemaVersion::V0_1, instance)
}

/// Validate a parsed manifest value against the embedded schema of `version`.
///
/// For [`SchemaVersion::V0_2`] this also runs the checks JSON Schema cannot
/// express (counts written as plain integers, and
/// `passed + failed + not_evaluated == total`), so the verdict matches the one
/// [`verify_value`] reports.
///
/// # Errors
///
/// Returns [`VerifyError::Schema`] only if the embedded schema fails to compile.
pub fn validate_schema_for(
    version: SchemaVersion,
    instance: &serde_json::Value,
) -> Result<Vec<String>, VerifyError> {
    let (schemas, sch) = compile_schema_for(version)?;
    Ok(schema_and_semantic_errors(version, &schemas, sch, instance))
}

/// Schema errors, then (for a schema-valid v0.2 document) the semantic errors.
fn schema_and_semantic_errors(
    version: SchemaVersion,
    schemas: &boon::Schemas,
    sch: boon::SchemaIndex,
    instance: &serde_json::Value,
) -> Vec<String> {
    let mut errors = collect_schema_errors(schemas, sch, instance);
    if errors.is_empty() && version == SchemaVersion::V0_2 {
        errors = v0_2_semantic_errors(instance);
    }
    errors
}

/// Compile the embedded schema, returning the validator handle for reuse.
///
/// # Errors
///
/// Returns [`VerifyError::Schema`] if the embedded schema JSON is malformed or
/// rejected by the compiler — a bug in this crate, not the caller's input.
pub fn compile_schema() -> Result<(boon::Schemas, boon::SchemaIndex), VerifyError> {
    compile_schema_for(SchemaVersion::V0_1)
}

/// Compile the embedded schema of `version`, returning the validator handle.
///
/// # Errors
///
/// Returns [`VerifyError::Schema`] if the embedded schema JSON is malformed or
/// rejected by the compiler — a bug in this crate, not the caller's input.
pub fn compile_schema_for(
    version: SchemaVersion,
) -> Result<(boon::Schemas, boon::SchemaIndex), VerifyError> {
    let schema_json: serde_json::Value = serde_json::from_str(version.schema_source())
        .map_err(|e| VerifyError::Schema(e.to_string()))?;
    let mut schemas = boon::Schemas::new();
    let mut compiler = boon::Compiler::new();
    compiler
        .add_resource(version.schema_id(), schema_json)
        .map_err(|e| VerifyError::Schema(e.to_string()))?;
    let sch = compiler
        .compile(version.schema_id(), &mut schemas)
        .map_err(|e| VerifyError::Schema(e.to_string()))?;
    Ok((schemas, sch))
}

/// Flatten a boon validation result into concise, leaf-level messages.
fn collect_schema_errors(
    schemas: &boon::Schemas,
    sch: boon::SchemaIndex,
    instance: &serde_json::Value,
) -> Vec<String> {
    match schemas.validate(instance, sch) {
        Ok(()) => Vec::new(),
        Err(err) => {
            let mut out = Vec::new();
            flatten(&err, &mut out);
            if out.is_empty() {
                out.push(err.to_string());
            }
            out
        }
    }
}

/// Walk a boon [`boon::ValidationError`] tree, emitting one string per leaf.
fn flatten(err: &boon::ValidationError, out: &mut Vec<String>) {
    if err.causes.is_empty() {
        let loc = if err.instance_location.to_string().is_empty() {
            "<root>".to_string()
        } else {
            err.instance_location.to_string()
        };
        out.push(format!("{loc}: {}", err.kind));
    } else {
        for cause in &err.causes {
            flatten(cause, out);
        }
    }
}

/// The v0.2 checks JSON Schema cannot express. Assumes the document is already
/// schema-valid.
///
/// * `policy.rule_id` and every count must be a plain non-negative integer:
///   `serde_json::Value::as_u64` accepts `4` but not `4.0` or `1e30`, although
///   the schema's `integer` type accepts both.
/// * `passed + failed + not_evaluated == total` for `checks` and `tests`
///   (overflow counts as a violation).
fn v0_2_semantic_errors(instance: &serde_json::Value) -> Vec<String> {
    let mut errors = Vec::new();
    if let Some(rule_id) = instance.get("policy").and_then(|p| p.get("rule_id"))
        && rule_id.as_u64().is_none()
    {
        errors.push(
            "policy.rule_id must be a non-negative integer written without a fraction or exponent"
                .to_string(),
        );
    }
    for key in ["checks", "tests"] {
        let Some(obj) = instance.get(key) else {
            continue;
        };
        let field = |name: &str| obj.get(name).and_then(serde_json::Value::as_u64);
        let (Some(total), Some(passed), Some(failed), Some(not_evaluated)) = (
            field("total"),
            field("passed"),
            field("failed"),
            field("not_evaluated"),
        ) else {
            errors.push(format!(
                "{key}: counts must be non-negative integers written without a fraction or exponent"
            ));
            continue;
        };
        let sum = passed
            .checked_add(failed)
            .and_then(|s| s.checked_add(not_evaluated));
        match sum {
            Some(sum) if sum == total => {}
            Some(sum) => errors.push(format!(
                "{key}: passed + failed + not_evaluated ({sum}) does not equal total ({total})"
            )),
            None => errors.push(format!(
                "{key}: passed + failed + not_evaluated overflows and does not equal total ({total})"
            )),
        }
    }
    errors
}

/// Build the asserted claims from the validated value itself.
///
/// # Errors
///
/// Returns a message when a claim object cannot be converted; the caller turns
/// that into a schema error so a claim can never be dropped silently.
fn extract_claims(instance: &serde_json::Value) -> Result<AssertedClaims, String> {
    fn take<T: serde::de::DeserializeOwned>(
        instance: &serde_json::Value,
        key: &str,
    ) -> Result<Option<T>, String> {
        instance
            .get(key)
            .map(|v| serde_json::from_value(v.clone()).map_err(|e| format!("{key}: {e}")))
            .transpose()
    }
    Ok(AssertedClaims {
        policy: take(instance, "policy")?,
        checks: take(instance, "checks")?,
        tests: take(instance, "tests")?,
    })
}

/// Verify a manifest document (already parsed) end to end.
///
/// Runs schema validation (v0.2 when `manifest_version` is `"0.2"`, otherwise
/// v0.1), the v0.2 count-sum check, then — when `artifacts_dir` is `Some` and the
/// manifest carries `output_hashes` — byte-verifies each referenced artifact by
/// hashing it with BLAKE3 and comparing to the recorded hash.
///
/// # Errors
///
/// Returns [`VerifyError::Schema`] only on an internal schema-compile failure.
pub fn verify_value(
    instance: &serde_json::Value,
    artifacts_dir: Option<&Path>,
) -> Result<VerifyReport, VerifyError> {
    let schema_version = SchemaVersion::of_document(instance);
    let mut schema_errors = validate_schema_for(schema_version, instance)?;
    let mut claims = None;
    if schema_errors.is_empty() && schema_version == SchemaVersion::V0_2 {
        match extract_claims(instance) {
            Ok(c) => claims = Some(c),
            Err(e) => schema_errors.push(format!("could not read asserted claims: {e}")),
        }
    }
    let schema_valid = schema_errors.is_empty();

    let artifact_checks = match artifacts_dir {
        // Only byte-verify a structurally-valid manifest we can trust to have
        // well-formed hashes; a schema-invalid document's byte-checks would be
        // noise on top of the real (structural) failure.
        Some(dir) if schema_valid => match serde_json::from_value::<Manifest>(instance.clone()) {
            Ok(manifest) => verify_artifacts(&manifest, dir),
            // v0.1 keeps its original behaviour exactly: no artifact checks,
            // no new error. A v0.2 document must not pass with its byte
            // checks silently skipped.
            Err(_) if schema_version == SchemaVersion::V0_1 => Vec::new(),
            Err(e) => {
                schema_errors.push(format!("could not read manifest: {e}"));
                claims = None;
                Vec::new()
            }
        },
        _ => Vec::new(),
    };
    let schema_valid = schema_valid && schema_errors.is_empty();

    Ok(VerifyReport {
        schema_valid,
        schema_errors,
        artifact_checks,
        schema_version,
        claims,
    })
}

/// Read, parse, and verify a manifest file.
///
/// # Errors
///
/// Returns [`VerifyError::Read`] if the file cannot be read, [`VerifyError::Json`]
/// if it is not valid JSON, or [`VerifyError::Schema`] on an internal
/// schema-compile failure.
pub fn verify_file(
    manifest_path: &Path,
    artifacts_dir: Option<&Path>,
) -> Result<VerifyReport, VerifyError> {
    let bytes = std::fs::read(manifest_path).map_err(|source| VerifyError::Read {
        path: manifest_path.display().to_string(),
        source,
    })?;
    let instance: serde_json::Value = serde_json::from_slice(&bytes).map_err(VerifyError::Json)?;
    verify_value(&instance, artifacts_dir)
}

/// Byte-verify every `output_hashes` entry against files under `artifacts_dir`.
///
/// For each recorded hash, the first of these local candidates that exists is
/// hashed and compared: the recorded `path`'s basename under `artifacts_dir`,
/// `artifacts_dir/<hash>`, and `artifacts_dir/<hash>.parquet` (the content-
/// addressed naming convention). A missing file is a failed check, not a
/// skipped one: asking to verify artifacts that cannot be located is a failure.
#[must_use]
pub fn verify_artifacts(manifest: &Manifest, artifacts_dir: &Path) -> Vec<ArtifactCheck> {
    let Some(outputs) = manifest.output_hashes.as_ref() else {
        return Vec::new();
    };
    outputs
        .iter()
        .map(|o| check_one_artifact(o, artifacts_dir))
        .collect()
}

fn check_one_artifact(output: &OutputHash, artifacts_dir: &Path) -> ArtifactCheck {
    // Resolution is confined to `artifacts_dir`: the manifest's recorded
    // `path` contributes only its basename. Honoring the recorded path
    // verbatim (absolute, or relative to the verifier's cwd) would let a
    // check pass against a file OUTSIDE the directory being verified — a
    // manifest is untrusted input, and "the bytes under --artifacts-dir
    // match" is the question being asked.
    let mut candidates: Vec<PathBuf> = Vec::new();
    if let Some(path) = output.path.as_ref()
        && let Some(name) = Path::new(path).file_name()
    {
        candidates.push(artifacts_dir.join(name));
    }
    candidates.push(artifacts_dir.join(&output.hash));
    candidates.push(artifacts_dir.join(format!("{}.parquet", output.hash)));

    let Some(file) = candidates.into_iter().find(|p| p.is_file()) else {
        return ArtifactCheck {
            expected: output.hash.clone(),
            file: None,
            actual: None,
            matched: false,
            note: Some(format!(
                "no artifact file found for hash {} under {}",
                output.hash,
                artifacts_dir.display()
            )),
        };
    };

    match blake3_file(&file) {
        Ok(actual) => {
            let matched = actual == output.hash;
            ArtifactCheck {
                expected: output.hash.clone(),
                file: Some(file),
                actual: Some(actual),
                matched,
                note: (!matched)
                    .then(|| "recorded hash does not match the file's bytes".to_string()),
            }
        }
        Err(e) => ArtifactCheck {
            expected: output.hash.clone(),
            file: Some(file),
            actual: None,
            matched: false,
            note: Some(format!("could not hash artifact: {e}")),
        },
    }
}

/// Streaming BLAKE3 of a file's bytes, lowercase hex.
fn blake3_file(path: &Path) -> std::io::Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut file = std::fs::File::open(path)?;
    std::io::copy(&mut file, &mut hasher)?;
    Ok(hasher.finalize().to_hex().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_manifest() -> serde_json::Value {
        serde_json::json!({
            "manifest_version": "0.1",
            "hash_scheme": "v1",
            "producer": { "name": "rocky", "version": "1.56.0" },
            "subject": { "model": "orders_clean", "status": "success" },
            "program_hash": "2148e619b51421f507453036000000000000000000000000000000000000abcd",
            "inputs_hash": "75ee2a328c52b236c21b9c60000000000000000000000000000000000000abcd",
            "inputs_proof_class": "heuristic",
            "env_hash": "bbf2c2045328f738683a6336df5fa61b5f00076aeaed7c495c04f9d6991d92bd"
        })
    }

    fn valid_v0_2() -> serde_json::Value {
        let mut m = valid_manifest();
        m["manifest_version"] = serde_json::json!("0.2");
        m["policy"] = serde_json::json!({
            "effect": "allow", "principal": "agent", "rule_id": 0, "ledger_ref": "run:r1"
        });
        m["checks"] = serde_json::json!(
            { "total": 4, "passed": 3, "failed": 0, "not_evaluated": 1 });
        m["tests"] = serde_json::json!(
            { "total": 2, "passed": 2, "failed": 0, "not_evaluated": 0 });
        m
    }

    fn v0_2_errors(m: &serde_json::Value) -> Vec<String> {
        let report = verify_value(m, None).unwrap();
        assert_eq!(report.schema_valid, report.schema_errors.is_empty());
        report.schema_errors
    }

    #[test]
    fn valid_v0_2_with_all_claims_passes() {
        let errs = v0_2_errors(&valid_v0_2());
        assert!(errs.is_empty(), "{errs:?}");
        let typed: Manifest = serde_json::from_value(valid_v0_2()).unwrap();
        assert_eq!(typed.checks.unwrap().total, 4);
        assert_eq!(typed.policy.unwrap().rule_id, Some(0));
    }

    #[test]
    fn v0_2_without_claims_is_valid() {
        let mut m = valid_v0_2();
        let obj = m.as_object_mut().unwrap();
        obj.remove("policy");
        obj.remove("checks");
        obj.remove("tests");
        assert!(v0_2_errors(&m).is_empty());
    }

    #[test]
    fn v0_1_with_policy_fails() {
        let mut m = valid_manifest();
        m["policy"] = serde_json::json!(
            { "effect": "allow", "principal": "agent", "ledger_ref": "run:r1" });
        assert!(!v0_2_errors(&m).is_empty());
    }

    #[test]
    fn v0_2_deny_effect_fails() {
        let mut m = valid_v0_2();
        m["policy"]["effect"] = serde_json::json!("deny");
        assert!(!v0_2_errors(&m).is_empty());
    }

    #[test]
    fn v0_2_negative_or_fractional_count_fails() {
        let mut m = valid_v0_2();
        m["checks"]["failed"] = serde_json::json!(-1);
        assert!(!v0_2_errors(&m).is_empty());
        let mut m = valid_v0_2();
        m["checks"]["total"] = serde_json::json!(4.5);
        assert!(!v0_2_errors(&m).is_empty());
    }

    #[test]
    fn v0_2_counts_that_do_not_sum_fail() {
        let mut m = valid_v0_2();
        m["checks"]["total"] = serde_json::json!(6);
        m["checks"]["passed"] = serde_json::json!(3);
        m["checks"]["not_evaluated"] = serde_json::json!(2);
        m["checks"]["failed"] = serde_json::json!(0);
        let errs = v0_2_errors(&m);
        assert_eq!(
            errs,
            vec!["checks: passed + failed + not_evaluated (5) does not equal total (6)"]
        );
        let mut m = valid_v0_2();
        m["tests"]["total"] = serde_json::json!(u64::MAX);
        m["tests"]["passed"] = serde_json::json!(u64::MAX);
        m["tests"]["failed"] = serde_json::json!(1);
        m["tests"]["not_evaluated"] = serde_json::json!(0);
        assert!(v0_2_errors(&m).iter().any(|e| e.starts_with("tests:")));
    }

    #[test]
    fn v0_2_unknown_key_in_policy_fails() {
        let mut m = valid_v0_2();
        m["policy"]["surprise"] = serde_json::json!(1);
        assert!(!v0_2_errors(&m).is_empty());
    }

    #[test]
    fn unknown_version_is_reported_by_the_v0_1_schema() {
        let mut m = valid_v0_2();
        m["manifest_version"] = serde_json::json!("0.3");
        assert!(!v0_2_errors(&m).is_empty());
    }

    #[test]
    fn embedded_schema_compiles() {
        compile_schema().expect("the embedded schema must compile");
        compile_schema_for(SchemaVersion::V0_2).expect("the v0.2 schema must compile");
    }

    #[test]
    fn a_valid_manifest_passes() {
        let errs = validate_schema(&valid_manifest()).unwrap();
        assert!(errs.is_empty(), "expected no schema errors, got {errs:?}");
    }

    #[test]
    fn missing_required_field_fails() {
        let mut m = valid_manifest();
        m.as_object_mut().unwrap().remove("program_hash");
        let errs = validate_schema(&m).unwrap();
        assert!(!errs.is_empty(), "removing program_hash must fail");
        assert!(
            errs.iter().any(|e| e.contains("program_hash")),
            "error should name the missing field: {errs:?}"
        );
    }

    #[test]
    fn tampered_nonhex_hash_fails() {
        let mut m = valid_manifest();
        m["env_hash"] = serde_json::json!("TAMPERED");
        let errs = validate_schema(&m).unwrap();
        assert!(!errs.is_empty(), "a non-hex env_hash must fail the pattern");
    }

    #[test]
    fn inputs_hash_without_proof_class_fails() {
        let mut m = valid_manifest();
        m.as_object_mut().unwrap().remove("inputs_proof_class");
        let errs = validate_schema(&m).unwrap();
        assert!(
            !errs.is_empty(),
            "inputs_hash present without inputs_proof_class must fail"
        );
    }

    #[test]
    fn proof_class_without_inputs_hash_fails() {
        let mut m = valid_manifest();
        m.as_object_mut().unwrap().remove("inputs_hash");
        let errs = validate_schema(&m).unwrap();
        assert!(
            !errs.is_empty(),
            "inputs_proof_class present without inputs_hash must fail"
        );
    }

    #[test]
    fn bad_proof_class_enum_fails() {
        let mut m = valid_manifest();
        m["inputs_proof_class"] = serde_json::json!("forged");
        let errs = validate_schema(&m).unwrap();
        assert!(!errs.is_empty(), "an out-of-enum proof class must fail");
    }

    #[test]
    fn no_inputs_side_is_valid() {
        // The default run path observes no inputs: the triple is 2-of-3.
        let mut m = valid_manifest();
        let obj = m.as_object_mut().unwrap();
        obj.remove("inputs_hash");
        obj.remove("inputs_proof_class");
        let errs = validate_schema(&m).unwrap();
        assert!(errs.is_empty(), "a no-inputs manifest is valid: {errs:?}");
    }

    #[test]
    fn wrong_manifest_version_fails() {
        let mut m = valid_manifest();
        m["manifest_version"] = serde_json::json!("0.2");
        let errs = validate_schema(&m).unwrap();
        assert!(!errs.is_empty(), "an unknown manifest_version must fail");
    }

    #[test]
    fn artifact_check_never_resolves_outside_artifacts_dir() {
        // A manifest's recorded `path` is untrusted: an absolute path to an
        // intact copy elsewhere on the machine must NOT satisfy the check
        // when the copy under --artifacts-dir was tampered with.
        let outside = tempfile::TempDir::new().unwrap();
        let intact = outside.path().join("orders.parquet");
        std::fs::write(&intact, b"intact bytes").unwrap();
        let hash = blake3::hash(b"intact bytes").to_hex().to_string();

        let artifacts = tempfile::TempDir::new().unwrap();
        std::fs::write(artifacts.path().join("orders.parquet"), b"TAMPERED").unwrap();

        let output = OutputHash {
            hash,
            path: Some(intact.display().to_string()),
        };
        let check = check_one_artifact(&output, artifacts.path());
        assert!(
            !check.matched,
            "a tampered in-dir artifact must fail even when the recorded \
             absolute path points at an intact out-of-dir copy: {check:?}"
        );
        let resolved = check.file.expect("the in-dir candidate must resolve");
        assert!(
            resolved.starts_with(artifacts.path()),
            "resolution must stay inside --artifacts-dir, got {}",
            resolved.display()
        );
    }

    #[test]
    fn artifact_check_resolves_basename_and_hash_forms_in_dir() {
        let artifacts = tempfile::TempDir::new().unwrap();
        let bytes = b"the artifact bytes";
        let hash = blake3::hash(bytes).to_hex().to_string();

        // Basename of the recorded path, under artifacts_dir.
        std::fs::write(artifacts.path().join("orders.parquet"), bytes).unwrap();
        let by_name = check_one_artifact(
            &OutputHash {
                hash: hash.clone(),
                path: Some("/produced/on/another/host/orders.parquet".into()),
            },
            artifacts.path(),
        );
        assert!(by_name.matched, "basename-in-dir must verify: {by_name:?}");

        // Content-addressed <hash>.parquet naming, no recorded path at all.
        std::fs::remove_file(artifacts.path().join("orders.parquet")).unwrap();
        std::fs::write(artifacts.path().join(format!("{hash}.parquet")), bytes).unwrap();
        let by_hash = check_one_artifact(&OutputHash { hash, path: None }, artifacts.path());
        assert!(
            by_hash.matched,
            "hash-named artifact must verify: {by_hash:?}"
        );
    }

    #[test]
    fn unknown_top_level_field_fails() {
        let mut m = valid_manifest();
        m["surprise"] = serde_json::json!(true);
        let errs = validate_schema(&m).unwrap();
        assert!(
            !errs.is_empty(),
            "additionalProperties:false must reject unknown fields"
        );
    }

    fn report_for(m: &serde_json::Value) -> VerifyReport {
        verify_value(m, None).unwrap()
    }

    #[test]
    fn v0_2_float_rule_id_fails() {
        for bad in [serde_json::json!(1.0), serde_json::json!(1e30)] {
            let mut m = valid_v0_2();
            m["policy"]["rule_id"] = bad;
            let r = report_for(&m);
            assert!(!r.schema_valid && r.claims.is_none());
            assert!(
                r.schema_errors
                    .iter()
                    .any(|e| e.starts_with("policy.rule_id"))
            );
        }
    }

    #[test]
    fn v0_2_float_count_fails() {
        let mut m = valid_v0_2();
        m["checks"] = serde_json::json!(
            { "total": 4.0, "passed": 3, "failed": 0, "not_evaluated": 1 });
        let r = report_for(&m);
        assert!(!r.schema_valid);
        assert!(r.schema_errors.iter().any(|e| e.starts_with("checks:")));
    }

    #[test]
    fn validate_schema_for_v0_2_catches_a_bad_sum() {
        let mut m = valid_v0_2();
        m["checks"]["total"] = serde_json::json!(9);
        let errs = validate_schema_for(SchemaVersion::V0_2, &m).unwrap();
        assert_eq!(errs, report_for(&m).schema_errors);
        assert!(!errs.is_empty());
        let mut m = valid_v0_2();
        m["policy"]["rule_id"] = serde_json::json!(1.0);
        assert!(
            !validate_schema_for(SchemaVersion::V0_2, &m)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn valid_v0_2_report_carries_the_documents_claims() {
        let m = valid_v0_2();
        let r = report_for(&m);
        let claims = r.claims.expect("claims present");
        assert_eq!(serde_json::to_value(&claims.policy).unwrap(), m["policy"]);
        assert_eq!(serde_json::to_value(&claims.checks).unwrap(), m["checks"]);
        assert_eq!(serde_json::to_value(&claims.tests).unwrap(), m["tests"]);
    }

    #[test]
    fn failed_document_has_no_claims() {
        let mut m = valid_v0_2();
        m["checks"]["total"] = serde_json::json!(9);
        assert!(report_for(&m).claims.is_none());
        m = valid_v0_2();
        m["policy"]["ledger_ref"] = serde_json::json!("a\nb");
        let r = report_for(&m);
        assert!(!r.schema_valid && r.claims.is_none());
    }

    /// Hex BLAKE3 of the published v0.1 schema. A change to the published v0.1
    /// schema must be a deliberate new version, not an edit of this constant.
    const V0_1_SCHEMA_BLAKE3: &str =
        "2c3c96d14acbfda8eea2046a536d1e78d8778b7ad4824db00ed7be6a3c1b37ad";

    #[test]
    fn v0_1_verdicts_are_unchanged() {
        assert_eq!(
            blake3::hash(MANIFEST_SCHEMA_V0_1.as_bytes())
                .to_hex()
                .as_str(),
            V0_1_SCHEMA_BLAKE3,
            "the published v0.1 schema changed; that must be a new version"
        );
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures");
        let mut seen = 0;
        for dir in ["valid", "tampered"] {
            for entry in std::fs::read_dir(root.join(dir)).unwrap() {
                let path = entry.unwrap().path();
                if path.extension().is_none_or(|x| x != "json") {
                    continue;
                }
                let Ok(value) =
                    serde_json::from_slice::<serde_json::Value>(&std::fs::read(&path).unwrap())
                else {
                    continue;
                };
                if value["manifest_version"] == "0.2" {
                    continue;
                }
                let original = validate_schema(&value).unwrap();
                let r = verify_value(&value, None).unwrap();
                assert_eq!(r.schema_valid, original.is_empty(), "{}", path.display());
                assert_eq!(r.schema_errors, original, "{}", path.display());
                assert!(
                    r.claims.is_none(),
                    "v0.1 shows no claims: {}",
                    path.display()
                );
                seen += 1;
            }
        }
        assert!(seen >= 5, "expected the v0.1 fixtures, saw {seen}");
    }
}
