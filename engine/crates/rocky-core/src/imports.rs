//! Loading and verifying imported producer-project snapshots.
//!
//! A producer project publishes a serialized [`ProjectIr`] snapshot (via
//! `rocky publish-ir`). A consumer project vendors that file and declares it
//! through an `[imports.<name>]` block in `rocky.toml` (see
//! [`crate::config::ImportEntry`]). This module loads such a snapshot back
//! into a [`ProjectIr`] and verifies it against an optional recipe-hash pin.
//!
//! The actual cross-team contract checking — diffing the import's baseline
//! against its current snapshot and matching dropped columns against the
//! consumer's references — lives in the CLI (`commands/imports_check.rs`),
//! which composes [`load_snapshot`] + [`verify_pin`] with the breaking-change
//! classifier in [`crate::breaking_change`].

use std::path::Path;

use rocky_ir::ProjectIr;
use thiserror::Error;

/// Errors that can occur loading or verifying an imported snapshot.
#[derive(Debug, Error)]
pub enum ImportsError {
    /// The snapshot file could not be read.
    #[error("failed to read import snapshot '{path}': {source}")]
    ReadFile {
        path: String,
        source: std::io::Error,
    },

    /// The snapshot file could not be written.
    #[error("failed to write import snapshot '{path}': {source}")]
    WriteFile {
        path: String,
        source: std::io::Error,
    },

    /// The snapshot file was not valid serialized [`ProjectIr`] JSON.
    #[error("failed to parse import snapshot '{path}': {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },

    /// The snapshot declares a `snapshot_version` newer than this build
    /// understands. Fail closed rather than silently mis-read a future format
    /// (which would recreate the "looks enforced, checks nothing" footgun).
    #[error(
        "import snapshot '{path}' is format version {found}, but this build of rocky \
         understands at most version {max_supported}; upgrade rocky to read it"
    )]
    UnsupportedVersion {
        path: String,
        found: u64,
        max_supported: u32,
    },
}

/// Current on-disk format version for published snapshots. A snapshot with no
/// `snapshot_version` key is a pre-versioning (`#774`-era) snapshot and is
/// read as version 1; bump this whenever the envelope or IR wire shape changes
/// in a way an older consumer cannot read.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 1;

/// Owned envelope used when reading a versioned snapshot.
#[derive(serde::Deserialize)]
struct SnapshotEnvelope {
    /// Honoured by [`load_snapshot`] before the `ir` is deserialized.
    #[allow(dead_code)]
    snapshot_version: u32,
    ir: ProjectIr,
    #[serde(default)]
    governance: Option<SnapshotGovernance>,
}

/// Borrowing envelope used when writing a snapshot (avoids cloning the IR).
#[derive(serde::Serialize)]
struct SnapshotEnvelopeRef<'a> {
    snapshot_version: u32,
    ir: &'a ProjectIr,
    #[serde(skip_serializing_if = "Option::is_none")]
    governance: Option<&'a SnapshotGovernance>,
}

/// Governance metadata a producer publishes beside its IR: the access level,
/// ownership and version of each exported model, plus the targets of the
/// models it withheld because they are not `public`.
///
/// Lives in the envelope, not in [`ProjectIr`], so it never enters the
/// recipe hash: adding it changes no existing pin. Older consumers ignore the
/// key; snapshots without it load with `None`. Additive, so the format version
/// stays [`SNAPSHOT_FORMAT_VERSION`].
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SnapshotGovernance {
    /// Exported models keyed by target full name (`catalog.schema.table`).
    #[serde(default)]
    pub models: std::collections::BTreeMap<String, ExportedModelGovernance>,
    /// Withheld models keyed by target full name. A consumer that reads one of
    /// these targets gets E047.
    #[serde(default)]
    pub withheld: std::collections::BTreeMap<String, WithheldModel>,
}

/// Governance of one exported model.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExportedModelGovernance {
    /// Producer model name.
    pub name: String,
    /// Declared access; `None` when the producer predates access levels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access: Option<crate::model_governance::ModelAccess>,
    /// Ownership group.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_group: Option<String>,
    /// Group owner.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub owner: Option<crate::model_governance::GroupOwner>,
    /// Version metadata for a versioned model or its latest alias.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<crate::model_governance::ModelVersionInfo>,
}

/// A producer model left out of the snapshot because it is not `public`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct WithheldModel {
    /// Producer model name.
    pub name: String,
    /// Its access level (`private` or `protected`).
    pub access: crate::model_governance::ModelAccess,
}

/// Serialize `ir` into a versioned snapshot envelope at `path`.
///
/// The envelope is `{"snapshot_version": N, "ir": <ProjectIr>}`. This is the
/// single place the on-disk format is written, so a future v2 only needs to
/// change here and in [`load_snapshot`].
///
/// # Errors
///
/// Returns [`ImportsError::WriteFile`] if the file cannot be written, or
/// [`ImportsError::Parse`] if serialization fails (should not happen for a
/// well-formed `ProjectIr`).
pub fn write_snapshot(ir: &ProjectIr, path: &Path) -> Result<(), ImportsError> {
    write_snapshot_with_governance(ir, None, path)
}

/// [`write_snapshot`] plus the producer's [`SnapshotGovernance`].
///
/// # Errors
///
/// Same as [`write_snapshot`].
pub fn write_snapshot_with_governance(
    ir: &ProjectIr,
    governance: Option<&SnapshotGovernance>,
    path: &Path,
) -> Result<(), ImportsError> {
    let envelope = SnapshotEnvelopeRef {
        snapshot_version: SNAPSHOT_FORMAT_VERSION,
        ir,
        governance,
    };
    let json = serde_json::to_string_pretty(&envelope).map_err(|source| ImportsError::Parse {
        path: path.display().to_string(),
        source,
    })?;
    std::fs::write(path, json).map_err(|source| ImportsError::WriteFile {
        path: path.display().to_string(),
        source,
    })
}

/// Outcome of comparing a snapshot's recipe hash against a configured pin.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PinStatus {
    /// No pin configured (or `"*"`): the consumer trusts whatever is vendored.
    Trusted,
    /// A concrete pin was configured and the snapshot matches it.
    Clean,
    /// A concrete pin was configured but the snapshot's hash differs.
    Mismatch { expected: String, actual: String },
}

/// Load a vendored producer snapshot into a [`ProjectIr`].
///
/// `dir` is the import's `path` (resolved relative to `rocky.toml` by the
/// caller); `file` is the snapshot filename within it.
///
/// Handles both the versioned envelope written by [`write_snapshot`]
/// (`{"snapshot_version": N, "ir": …}`) and a pre-versioning bare `ProjectIr`
/// (read as version 1). A snapshot declaring a version newer than this build
/// understands fails closed.
///
/// # Errors
///
/// Returns [`ImportsError::ReadFile`] if the file cannot be read,
/// [`ImportsError::Parse`] if it is not valid snapshot JSON, and
/// [`ImportsError::UnsupportedVersion`] if it declares a future format version.
pub fn load_snapshot(dir: &Path, file: &str) -> Result<ProjectIr, ImportsError> {
    load_snapshot_with_governance(dir, file).map(|(ir, _)| ir)
}

/// [`load_snapshot`] plus the envelope's [`SnapshotGovernance`], when the
/// producer wrote one. A pre-versioning bare snapshot has none.
///
/// # Errors
///
/// Same as [`load_snapshot`].
pub fn load_snapshot_with_governance(
    dir: &Path,
    file: &str,
) -> Result<(ProjectIr, Option<SnapshotGovernance>), ImportsError> {
    let path = dir.join(file);
    let display = path.display().to_string();
    let contents = std::fs::read_to_string(&path).map_err(|source| ImportsError::ReadFile {
        path: display.clone(),
        source,
    })?;

    // A `snapshot_version` key marks a versioned envelope; its absence marks a
    // pre-versioning (#774-era) bare `ProjectIr`, read as version 1. Detection
    // is unambiguous because `ProjectIr` has no `snapshot_version` field.
    let value: serde_json::Value =
        serde_json::from_str(&contents).map_err(|source| ImportsError::Parse {
            path: display.clone(),
            source,
        })?;
    match value
        .get("snapshot_version")
        .and_then(serde_json::Value::as_u64)
    {
        Some(version) if version > u64::from(SNAPSHOT_FORMAT_VERSION) => {
            Err(ImportsError::UnsupportedVersion {
                path: display,
                found: version,
                max_supported: SNAPSHOT_FORMAT_VERSION,
            })
        }
        Some(_) => {
            let envelope: SnapshotEnvelope =
                serde_json::from_value(value).map_err(|source| ImportsError::Parse {
                    path: display,
                    source,
                })?;
            Ok((envelope.ir, envelope.governance))
        }
        None => serde_json::from_value(value)
            .map(|ir| (ir, None))
            .map_err(|source| ImportsError::Parse {
                path: display,
                source,
            }),
    }
}

/// Compare a snapshot's recipe hash against an optional configured pin.
///
/// - `None` or `"*"` ⇒ [`PinStatus::Trusted`] (no verification requested).
/// - A concrete hex pin that matches the snapshot ⇒ [`PinStatus::Clean`].
/// - A concrete hex pin that differs ⇒ [`PinStatus::Mismatch`].
pub fn verify_pin(ir: &ProjectIr, pin: Option<&str>) -> PinStatus {
    let pin = match pin {
        None => return PinStatus::Trusted,
        Some(p) if p.trim() == "*" || p.trim().is_empty() => return PinStatus::Trusted,
        Some(p) => p.trim(),
    };
    let actual = ir.recipe_hash().to_hex().to_string();
    if actual.eq_ignore_ascii_case(pin) {
        PinStatus::Clean
    } else {
        PinStatus::Mismatch {
            expected: pin.to_string(),
            actual,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_ir::{
        GovernanceConfig, MaterializationStrategy, ModelIr, RockyType, TargetRef, TypedColumn,
    };

    fn sample_ir() -> ProjectIr {
        let mut model = ModelIr::transformation(
            TargetRef {
                catalog: "shop".to_string(),
                schema: "core".to_string(),
                table: "orders".to_string(),
            },
            MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT id FROM raw.orders".to_string(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model.typed_columns = vec![TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: false,
        }];
        ProjectIr {
            models: vec![model],
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    #[test]
    fn load_snapshot_round_trips() {
        let ir = sample_ir();
        let dir = tempfile::tempdir().unwrap();
        let json = serde_json::to_string_pretty(&ir).unwrap();
        std::fs::write(dir.path().join("snap.json"), json).unwrap();

        let loaded = load_snapshot(dir.path(), "snap.json").unwrap();
        assert_eq!(loaded.models.len(), 1);
        assert_eq!(loaded.models[0].typed_columns.len(), 1);
        // Recipe hash survives the round-trip.
        assert_eq!(
            loaded.recipe_hash().to_hex().to_string(),
            ir.recipe_hash().to_hex().to_string()
        );
    }

    #[test]
    fn load_snapshot_missing_file_errors() {
        let dir = tempfile::tempdir().unwrap();
        let err = load_snapshot(dir.path(), "nope.json").unwrap_err();
        assert!(matches!(err, ImportsError::ReadFile { .. }));
    }

    #[test]
    fn verify_pin_trusted_when_none_or_star() {
        let ir = sample_ir();
        assert_eq!(verify_pin(&ir, None), PinStatus::Trusted);
        assert_eq!(verify_pin(&ir, Some("*")), PinStatus::Trusted);
        assert_eq!(verify_pin(&ir, Some("  ")), PinStatus::Trusted);
    }

    #[test]
    fn verify_pin_clean_on_match() {
        let ir = sample_ir();
        let hash = ir.recipe_hash().to_hex().to_string();
        assert_eq!(verify_pin(&ir, Some(&hash)), PinStatus::Clean);
        // Case-insensitive.
        assert_eq!(
            verify_pin(&ir, Some(&hash.to_uppercase())),
            PinStatus::Clean
        );
    }

    #[test]
    fn verify_pin_mismatch_on_wrong_hash() {
        let ir = sample_ir();
        let status = verify_pin(&ir, Some("deadbeef"));
        match status {
            PinStatus::Mismatch { expected, actual } => {
                assert_eq!(expected, "deadbeef");
                assert_eq!(actual, ir.recipe_hash().to_hex().to_string());
            }
            other => panic!("expected mismatch, got {other:?}"),
        }
    }

    #[test]
    fn write_snapshot_round_trips_with_envelope() {
        let ir = sample_ir();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("snap.json");
        write_snapshot(&ir, &path).unwrap();
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            raw.contains("\"snapshot_version\""),
            "envelope carries the version header"
        );

        let loaded = load_snapshot(dir.path(), "snap.json").unwrap();
        assert_eq!(loaded.models.len(), 1);
        assert_eq!(
            loaded.recipe_hash().to_hex().to_string(),
            ir.recipe_hash().to_hex().to_string()
        );
    }

    #[test]
    fn headerless_snapshot_loads_as_v1_with_identical_recipe_hash() {
        // A pre-versioning (#774-era) snapshot is a bare ProjectIr with no
        // envelope. It must still load, and its recipe hash must equal the
        // enveloped form's — proving wrapping never breaks an existing pin.
        let ir = sample_ir();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("headerless.json"),
            serde_json::to_string_pretty(&ir).unwrap(),
        )
        .unwrap();
        write_snapshot(&ir, &dir.path().join("enveloped.json")).unwrap();

        let from_headerless = load_snapshot(dir.path(), "headerless.json").unwrap();
        let from_enveloped = load_snapshot(dir.path(), "enveloped.json").unwrap();
        assert_eq!(
            from_headerless.recipe_hash().to_hex().to_string(),
            from_enveloped.recipe_hash().to_hex().to_string(),
            "wrapping in an envelope must not change recipe_hash — #774 pins must still match"
        );
    }

    #[test]
    fn governance_round_trips_and_leaves_recipe_hash_alone() {
        use crate::model_governance::ModelAccess;
        let ir = sample_ir();
        let mut gov = SnapshotGovernance::default();
        gov.models.insert(
            "shop.core.orders".into(),
            ExportedModelGovernance {
                name: "orders".into(),
                access: Some(ModelAccess::Public),
                access_group: None,
                owner: None,
                version: None,
            },
        );
        gov.withheld.insert(
            "shop.core.secret".into(),
            WithheldModel {
                name: "secret".into(),
                access: ModelAccess::Private,
            },
        );
        let dir = tempfile::tempdir().unwrap();
        write_snapshot_with_governance(&ir, Some(&gov), &dir.path().join("g.json")).unwrap();
        write_snapshot(&ir, &dir.path().join("plain.json")).unwrap();
        let (with, loaded_gov) = load_snapshot_with_governance(dir.path(), "g.json").unwrap();
        let (plain, no_gov) = load_snapshot_with_governance(dir.path(), "plain.json").unwrap();
        assert_eq!(loaded_gov, Some(gov));
        assert_eq!(no_gov, None);
        assert_eq!(
            with.recipe_hash().to_hex().to_string(),
            plain.recipe_hash().to_hex().to_string(),
            "governance metadata must not change the recipe hash"
        );
    }

    #[test]
    fn too_new_snapshot_version_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let future = u64::from(SNAPSHOT_FORMAT_VERSION) + 1;
        std::fs::write(
            dir.path().join("future.json"),
            format!(
                "{{\"snapshot_version\": {future}, \"ir\": {}}}",
                serde_json::to_string(&sample_ir()).unwrap()
            ),
        )
        .unwrap();
        let err = load_snapshot(dir.path(), "future.json").unwrap_err();
        match err {
            ImportsError::UnsupportedVersion {
                found,
                max_supported,
                ..
            } => {
                assert_eq!(found, future);
                assert_eq!(max_supported, SNAPSHOT_FORMAT_VERSION);
            }
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }
}
