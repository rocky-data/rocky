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

    /// The snapshot file was not valid serialized [`ProjectIr`] JSON.
    #[error("failed to parse import snapshot '{path}': {source}")]
    Parse {
        path: String,
        source: serde_json::Error,
    },
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
/// # Errors
///
/// Returns [`ImportsError::ReadFile`] if the file cannot be read and
/// [`ImportsError::Parse`] if it is not valid serialized `ProjectIr` JSON.
pub fn load_snapshot(dir: &Path, file: &str) -> Result<ProjectIr, ImportsError> {
    let path = dir.join(file);
    let display = path.display().to_string();
    let contents = std::fs::read_to_string(&path).map_err(|source| ImportsError::ReadFile {
        path: display.clone(),
        source,
    })?;
    serde_json::from_str(&contents).map_err(|source| ImportsError::Parse {
        path: display,
        source,
    })
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
}
