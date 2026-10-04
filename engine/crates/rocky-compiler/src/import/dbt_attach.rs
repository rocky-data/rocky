//! dbt "attach mode" — read a dbt project's compiled artifacts in place.
//!
//! `rocky compile --dbt-project <DIR>` (experimental) does not migrate a dbt
//! project. It reads `<DIR>/target/manifest.json` (and the sibling
//! `run_results.json` when present) on every invocation and hands the
//! translated models to the normal compiler.
//!
//! This module is the read side. It reuses the `rocky import-dbt` pipeline
//! unchanged — [`dbt_manifest::parse_manifest`] plus
//! [`dbt::import_from_manifest`] — so attach mode refuses exactly what import
//! refuses, with the same reasons. The only new gate is the manifest schema
//! version check, which runs *before* the full parse so an unknown manifest
//! shape is refused by name instead of misparsed.
//!
//! Nothing here writes to disk. The caller decides where (a private temp
//! directory) the translated project is materialized.

use std::io::BufReader;
use std::path::{Path, PathBuf};

use rocky_core::models::TargetConfig;
use serde::Deserialize;

use super::dbt::{self, ImportResult, MicrobatchMode};
use super::dbt_manifest;
use super::dbt_profiles::{self, ProfileResolution, StubReason};

/// dbt manifest schema versions (`metadata.dbt_schema_version`) attach mode
/// accepts.
///
/// v12 is what dbt-core 1.8 and later write, and it is the version every
/// manifest fixture in this repository carries. Older versions are refused
/// until a real fixture proves the parser reads them correctly.
pub const SUPPORTED_MANIFEST_SCHEMA_VERSIONS: &[u32] = &[12];

/// One model attach mode refuses to translate.
#[derive(Debug, Clone)]
pub struct AttachRefusal {
    /// The dbt model name.
    pub model: String,
    /// Short label for the refused dbt construct.
    pub construct: &'static str,
    /// The full refusal reason — identical to `rocky import-dbt`'s.
    pub reason: String,
}

/// Why attach mode cannot produce a project.
#[derive(Debug, thiserror::Error)]
pub enum AttachError {
    /// `<dbt_project>/target/manifest.json` does not exist.
    #[error(
        "no dbt manifest at {}; run `dbt compile --full-refresh` in the dbt project first",
        path.display()
    )]
    ManifestMissing { path: PathBuf },

    /// The manifest has no `metadata.dbt_schema_version`.
    #[error(
        "{} has no metadata.dbt_schema_version; attach mode supports manifest schema {}",
        path.display(),
        supported_versions_label()
    )]
    SchemaVersionMissing { path: PathBuf },

    /// The manifest declares a schema version attach mode does not support.
    #[error(
        "{} declares unsupported dbt manifest schema version `{found}`; attach mode supports \
         manifest schema {}. Regenerate the manifest with a dbt version that writes a supported \
         schema",
        path.display(),
        supported_versions_label()
    )]
    UnsupportedSchemaVersion { path: PathBuf, found: String },

    /// The manifest could not be read or parsed.
    #[error("{0}")]
    Parse(String),

    /// One or more models use a construct import refuses.
    #[error("{}", format_refusals(.0))]
    Refused(Vec<AttachRefusal>),
}

/// A dbt project translated in memory, ready to be materialized and compiled.
pub struct AttachedDbtProject {
    /// The manifest that was read.
    pub manifest_path: PathBuf,
    /// The accepted manifest schema version (e.g. `12`).
    pub schema_version: u32,
    /// Resolved adapter shape from `<dbt_project>/profiles.yml`.
    pub profile: ProfileResolution,
    /// Default model target derived from the profile.
    pub default_target: TargetConfig,
    /// The importer result. `failed` is always empty here: any failure
    /// becomes [`AttachError::Refused`].
    pub import: ImportResult,
}

/// Read and translate a dbt project's compiled artifacts without writing.
///
/// # Errors
///
/// Returns [`AttachError`] when the manifest is missing, declares an
/// unsupported (or no) schema version, cannot be parsed, or when any model is
/// refused by the shared importer.
pub fn attach_dbt_project(dbt_project: &Path) -> Result<AttachedDbtProject, AttachError> {
    let manifest_path = dbt_project.join("target").join("manifest.json");
    if !manifest_path.is_file() {
        return Err(AttachError::ManifestMissing {
            path: manifest_path,
        });
    }

    let schema_version = check_manifest_schema_version(&manifest_path)?;

    let manifest = dbt_manifest::parse_manifest(&manifest_path).map_err(AttachError::Parse)?;

    let profile = resolve_profile(dbt_project);
    let default_target = default_target_from_profile(&profile);

    // Same calls, same order, same defaults as `rocky import-dbt` with a
    // manifest and no flags: unit tests converted, microbatch → merge.
    let mut import =
        dbt::import_from_manifest(&manifest, &default_target, false, MicrobatchMode::default());
    dbt::apply_dbt_tests(dbt_project, &default_target, &mut import);

    if !import.failed.is_empty() {
        let mut refusals: Vec<AttachRefusal> = import
            .failed
            .iter()
            .map(|f| AttachRefusal {
                model: f.name.clone(),
                construct: refusal_construct(&f.reason),
                reason: f.reason.clone(),
            })
            .collect();
        // `manifest.nodes` is a HashMap; sort so the message is stable.
        refusals.sort_by(|a, b| a.model.cmp(&b.model));
        return Err(AttachError::Refused(refusals));
    }

    Ok(AttachedDbtProject {
        manifest_path,
        schema_version,
        profile,
        default_target,
        import,
    })
}

/// Read only `metadata.dbt_schema_version` from a manifest and check it
/// against [`SUPPORTED_MANIFEST_SCHEMA_VERSIONS`].
///
/// # Errors
///
/// Returns [`AttachError::SchemaVersionMissing`] when the field is absent or
/// empty, [`AttachError::UnsupportedSchemaVersion`] when it names a version
/// outside the supported set (or cannot be read as a version), and
/// [`AttachError::Parse`] when the file is not valid JSON.
pub fn check_manifest_schema_version(manifest_path: &Path) -> Result<u32, AttachError> {
    #[derive(Deserialize)]
    struct Peek {
        #[serde(default)]
        metadata: PeekMetadata,
    }
    #[derive(Deserialize, Default)]
    struct PeekMetadata {
        #[serde(default)]
        dbt_schema_version: Option<String>,
    }

    let file = std::fs::File::open(manifest_path).map_err(|e| {
        AttachError::Parse(format!("failed to open {}: {e}", manifest_path.display()))
    })?;
    let peek: Peek = serde_json::from_reader(BufReader::new(file)).map_err(|e| {
        AttachError::Parse(format!("failed to parse {}: {e}", manifest_path.display()))
    })?;

    let Some(raw) = peek
        .metadata
        .dbt_schema_version
        .filter(|v| !v.trim().is_empty())
    else {
        return Err(AttachError::SchemaVersionMissing {
            path: manifest_path.to_path_buf(),
        });
    };

    match parse_manifest_schema_version(&raw) {
        Some(v) if SUPPORTED_MANIFEST_SCHEMA_VERSIONS.contains(&v) => Ok(v),
        _ => Err(AttachError::UnsupportedSchemaVersion {
            path: manifest_path.to_path_buf(),
            found: raw,
        }),
    }
}

/// Extract the integer from a dbt manifest schema URL.
///
/// dbt writes both `https://schemas.getdbt.com/dbt/manifest/v12.json` and
/// `https://schemas.getdbt.com/dbt/manifest/v12/manifest.json`. Anything that
/// is not a `manifest/v<N>` URL returns `None`.
pub fn parse_manifest_schema_version(raw: &str) -> Option<u32> {
    let (_, rest) = raw.split_once("/manifest/v")?;
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    let tail = &rest[digits.len()..];
    if digits.is_empty() || !(tail == ".json" || tail == "/manifest.json") {
        return None;
    }
    digits.parse().ok()
}

/// Name the dbt construct behind an importer refusal reason.
///
/// The reasons are private constants in [`dbt`]; this module does not edit
/// that file, so it matches a distinctive phrase from each one. The full
/// reason is always printed next to the label, so a phrase that drifts only
/// degrades the label to the generic one — never the refusal itself. The
/// unit tests below pin the label for each refusal the fixtures produce.
pub fn refusal_construct(reason: &str) -> &'static str {
    const LABELS: &[(&str, &str)] = &[
        (
            "effective `full_refresh=false`",
            "incremental model with `full_refresh=false`",
        ),
        (
            "per-model evidence in a matching full-refresh",
            "incremental model without full-refresh compile evidence",
        ),
        (
            "no Rocky append equivalent",
            "incremental model with `is_incremental()` and no Rocky append equivalent",
        ),
        (
            "cannot evaluate Jinja control flow",
            "Jinja control flow (`{% ... %}`) without compiled SQL",
        ),
        (
            "unresolved reference to dbt's `is_incremental()`",
            "unresolved `is_incremental()` without compiled SQL",
        ),
        (
            "effectively incremental dbt model",
            "incremental model without compiled SQL",
        ),
        (
            "cannot resolve a dbt config expression",
            "unresolvable dbt `config()` expression",
        ),
        (
            "cannot resolve versioned model properties",
            "versioned model properties",
        ),
    ];
    LABELS
        .iter()
        .find(|(phrase, _)| reason.contains(phrase))
        .map_or("unsupported dbt construct", |(_, label)| label)
}

/// Profile resolution as `rocky import-dbt` does it with no
/// `--target-adapter` override: honor `dbt_project.yml`'s `profile:` key,
/// read only `<dbt_project>/profiles.yml`, stub DuckDB when absent.
fn resolve_profile(dbt_project: &Path) -> ProfileResolution {
    let profile_name = dbt::read_project_profile_name(dbt_project);
    dbt_profiles::resolve_from_project(dbt_project, profile_name.as_deref())
        .unwrap_or_else(|| dbt_profiles::stub_resolution(StubReason::ProfilesAbsent))
}

fn default_target_from_profile(profile: &ProfileResolution) -> TargetConfig {
    TargetConfig {
        catalog: profile
            .database
            .clone()
            .unwrap_or_else(|| "warehouse".to_string()),
        schema: profile.schema.clone().unwrap_or_else(|| "main".to_string()),
        table: String::new(),
    }
}

fn supported_versions_label() -> String {
    SUPPORTED_MANIFEST_SCHEMA_VERSIONS
        .iter()
        .map(|v| format!("v{v}"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_refusals(refusals: &[AttachRefusal]) -> String {
    let mut out = format!(
        "attach mode refused {} dbt model(s); nothing was compiled:",
        refusals.len()
    );
    for r in refusals {
        out.push_str(&format!(
            "\n  - model `{}` ({}): {}",
            r.model, r.construct, r.reason
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_both_dbt_schema_url_shapes() {
        assert_eq!(
            parse_manifest_schema_version("https://schemas.getdbt.com/dbt/manifest/v12.json"),
            Some(12)
        );
        assert_eq!(
            parse_manifest_schema_version(
                "https://schemas.getdbt.com/dbt/manifest/v12/manifest.json"
            ),
            Some(12)
        );
        assert_eq!(
            parse_manifest_schema_version("https://schemas.getdbt.com/dbt/manifest/v7.json"),
            Some(7)
        );
    }

    #[test]
    fn rejects_non_manifest_schema_urls() {
        for raw in [
            "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "https://schemas.getdbt.com/dbt/manifest/vX.json",
            "https://schemas.getdbt.com/dbt/manifest/v12.yaml",
            "v12",
            "",
        ] {
            assert_eq!(parse_manifest_schema_version(raw), None, "{raw}");
        }
    }

    fn write_manifest(dir: &Path, schema_version: Option<&str>) -> PathBuf {
        let mut metadata = serde_json::json!({ "project_name": "p" });
        if let Some(v) = schema_version {
            metadata["dbt_schema_version"] = serde_json::Value::String(v.to_string());
        }
        let path = dir.join("manifest.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&serde_json::json!({ "metadata": metadata, "nodes": {} })).unwrap(),
        )
        .unwrap();
        path
    }

    #[test]
    fn accepts_v12_and_refuses_unknown_versions_by_name() {
        let dir = tempfile::tempdir().unwrap();

        let path = write_manifest(
            dir.path(),
            Some("https://schemas.getdbt.com/dbt/manifest/v12.json"),
        );
        assert_eq!(check_manifest_schema_version(&path).unwrap(), 12);

        let path = write_manifest(
            dir.path(),
            Some("https://schemas.getdbt.com/dbt/manifest/v99.json"),
        );
        let err = check_manifest_schema_version(&path).unwrap_err();
        assert!(matches!(err, AttachError::UnsupportedSchemaVersion { .. }));
        let msg = err.to_string();
        assert!(msg.contains("manifest/v99.json"), "{msg}");
        assert!(msg.contains("v12"), "{msg}");

        let path = write_manifest(dir.path(), None);
        assert!(matches!(
            check_manifest_schema_version(&path).unwrap_err(),
            AttachError::SchemaVersionMissing { .. }
        ));
    }

    #[test]
    fn missing_manifest_names_the_fix() {
        let dir = tempfile::tempdir().unwrap();
        let err = attach_dbt_project(dir.path()).err().unwrap();
        assert!(matches!(err, AttachError::ManifestMissing { .. }));
        assert!(err.to_string().contains("dbt compile --full-refresh"));
    }

    #[test]
    fn refusal_construct_labels_every_manifest_refusal() {
        // A generic label is the fallback; the manifest-path refusals must
        // each get a specific one.
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/dbt_incremental_compile/plain/manifest.json");
        let manifest = dbt_manifest::parse_manifest(&fixtures).unwrap();
        let result = dbt::import_from_manifest(
            &manifest,
            &TargetConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: String::new(),
            },
            false,
            MicrobatchMode::default(),
        );
        assert!(!result.failed.is_empty());
        let mut jinja = manifest.clone();
        let node = jinja.nodes.values_mut().next().unwrap();
        node.config.materialized = "table".to_string();
        node.compiled_code = None;
        node.raw_code = "select 1 {% if target.name == 'prod' %} where 1 = 1 {% endif %}".into();
        let jinja_result = dbt::import_from_manifest(
            &jinja,
            &TargetConfig {
                catalog: "c".into(),
                schema: "s".into(),
                table: String::new(),
            },
            false,
            MicrobatchMode::default(),
        );
        assert!(
            jinja_result
                .failed
                .iter()
                .any(|f| refusal_construct(&f.reason)
                    == "Jinja control flow (`{% ... %}`) without compiled SQL")
        );
        for f in result.failed.iter().chain(&jinja_result.failed) {
            assert_ne!(
                refusal_construct(&f.reason),
                "unsupported dbt construct",
                "{}: {}",
                f.name,
                f.reason
            );
        }
    }
}
