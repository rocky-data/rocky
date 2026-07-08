//! Conformance tests: golden manifests captured from real engine output must
//! validate against the embedded v0.1 schema, and known-bad manifests must
//! fail. This is the drift tripwire between the spec and the implementation —
//! if the engine's recorded recipe-identity shape moves, a golden regenerated
//! from live output stops validating here.

use std::path::{Path, PathBuf};

use rocky_verify::{verify_file, verify_value};

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures")
}

fn read_json(path: &Path) -> serde_json::Value {
    let bytes = std::fs::read(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("parse {}: {e}", path.display()))
}

fn json_files(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()))
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    files.sort();
    files
}

/// Every committed valid manifest must pass schema validation. These are
/// captured from real `rocky history --recipe --output json` output (the
/// general recipe-identity manifest) and from the shipped content-addressed
/// audit sample.
#[test]
fn valid_fixtures_pass_schema() {
    let dir = fixtures().join("valid");
    let files = json_files(&dir);
    assert!(!files.is_empty(), "no valid fixtures found in {dir:?}");
    for file in files {
        let value = read_json(&file);
        let report = verify_value(&value, None).expect("schema compiles");
        assert!(
            report.schema_valid,
            "valid fixture {} failed schema: {:?}",
            file.display(),
            report.schema_errors
        );
    }
}

/// The content-addressed golden byte-verifies against the self-contained
/// artifact copy: the recorded output hash equals the BLAKE3 of the real file.
#[test]
fn content_addressed_golden_byte_verifies() {
    let manifest = fixtures().join("valid/content-addressed.json");
    let artifacts = fixtures().join("artifacts");
    let report = verify_file(&manifest, Some(&artifacts)).expect("schema compiles");
    assert!(
        report.passed(),
        "content-addressed golden must verify: schema_errors={:?}, checks={:?}",
        report.schema_errors,
        report.artifact_checks
    );
    assert_eq!(report.artifact_checks.len(), 1, "one artifact expected");
    assert!(
        report.artifact_checks[0].matched,
        "the byte-check must match"
    );
}

/// Every committed tampered manifest must fail verification. Schema-level
/// tampers (missing field, malformed hash, broken honesty invariant, bad enum)
/// fail structurally; the byte-tamper (a schema-valid manifest whose recorded
/// output hash no longer matches the artifact's bytes) fails the byte-check.
#[test]
fn tampered_fixtures_fail() {
    let dir = fixtures().join("tampered");
    let artifacts = fixtures().join("artifacts");
    let files = json_files(&dir);
    assert!(!files.is_empty(), "no tampered fixtures found in {dir:?}");
    for file in files {
        // Pass the artifacts dir so the byte-tamper case can be caught by the
        // byte-check rather than slipping through schema validation.
        let report = verify_file(&file, Some(&artifacts)).expect("schema compiles");
        assert!(
            !report.passed(),
            "tampered fixture {} must NOT verify, but it passed",
            file.display()
        );
    }
}

/// The general golden is genuinely a recipe-identity manifest: it carries the
/// program and environment identity and the honest input proof class.
#[test]
fn general_golden_is_a_real_recipe_identity_manifest() {
    let value = read_json(&fixtures().join("valid/general-recipe-identity.json"));
    let obj = value.as_object().expect("object");
    assert_eq!(obj["manifest_version"], serde_json::json!("0.1"));
    assert_eq!(obj["hash_scheme"], serde_json::json!("v1"));
    assert!(obj.contains_key("program_hash"), "must carry program_hash");
    assert!(obj.contains_key("env_hash"), "must carry env_hash");
    // This POC observes its input via a freshness signal, so the honest label
    // is heuristic — never a strong (byte-identity) claim on a watermark.
    assert_eq!(obj["inputs_proof_class"], serde_json::json!("heuristic"));
}
