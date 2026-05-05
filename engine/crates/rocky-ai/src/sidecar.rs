//! TOML sidecar emission for AI-generated models.
//!
//! `rocky ai` historically emitted only the body source (`.sql` or `.rocky`)
//! and printed it to stdout; the user then had to author a matching `.toml`
//! sidecar by hand for Rocky's model loader to pick the model up. This
//! module renders the sidecar from a small CLI-driven config so a single
//! `rocky ai` invocation produces a complete, loadable model package.
//!
//! The shape mirrors a subset of [`rocky_core::models::ModelConfig`] that's
//! authorable from the CLI today: name + strategy + target. Other fields
//! (intent, freshness, classification, retention, budget, …) are out of
//! scope for the first cut and inherit defaults from the Rocky loader.

use std::path::{Path, PathBuf};

use serde::Serialize;
use thiserror::Error;

/// Errors emitted while writing the body + sidecar files.
#[derive(Debug, Error)]
pub enum SidecarError {
    #[error("file already exists at {path} (pass --overwrite to replace)")]
    AlreadyExists { path: String },

    #[error("failed to write {path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },

    #[error("failed to serialize sidecar TOML: {0}")]
    Serialize(#[from] toml::ser::Error),

    #[error(
        "unknown materialization '{value}'; expected one of: full_refresh, incremental, merge, ephemeral"
    )]
    UnknownMaterialization { value: String },

    #[error("--watermark is required when --materialization=incremental")]
    MissingWatermark,

    #[error("invalid --target '{value}': expected catalog.schema.table")]
    InvalidTarget { value: String },
}

/// Materialization strategy authorable from the `rocky ai --materialization`
/// flag. A deliberately narrow subset of [`rocky_core::models::StrategyConfig`]:
/// the four shapes a generated model is most likely to want today.
///
/// `time_interval`, `delete_insert`, and `microbatch` exist in the engine's
/// strategy enum but require richer flag plumbing (granularity, partition
/// columns) than this first cut bothers with — adding them is a follow-up.
#[derive(Debug, Clone)]
pub enum SidecarMaterialization {
    FullRefresh,
    Incremental { watermark: String },
    Merge,
    Ephemeral,
}

impl SidecarMaterialization {
    /// Parse the `--materialization` CLI value. The optional `watermark` is
    /// only consumed when `value == "incremental"` — it's required there
    /// and ignored otherwise.
    pub fn parse(value: &str, watermark: Option<&str>) -> Result<Self, SidecarError> {
        match value {
            "full_refresh" => Ok(Self::FullRefresh),
            "incremental" => {
                let w = watermark.ok_or(SidecarError::MissingWatermark)?;
                Ok(Self::Incremental {
                    watermark: w.to_string(),
                })
            }
            "merge" => Ok(Self::Merge),
            "ephemeral" => Ok(Self::Ephemeral),
            other => Err(SidecarError::UnknownMaterialization {
                value: other.to_string(),
            }),
        }
    }
}

/// `catalog.schema.table` triple. Parsed from the `--target` CLI flag or
/// derived from the model name (default: `generated.ai.<name>`).
#[derive(Debug, Clone)]
pub struct SidecarTarget {
    pub catalog: String,
    pub schema: String,
    pub table: String,
}

impl SidecarTarget {
    /// Default target when `--target` is omitted. Mirrors the in-memory
    /// default in `rocky_ai::generate::build_generated_model`.
    pub fn default_for(name: &str) -> Self {
        Self {
            catalog: "generated".to_string(),
            schema: "ai".to_string(),
            table: name.to_string(),
        }
    }

    /// Parse the `--target` CLI value as `catalog.schema.table`. Each
    /// component must be non-empty; anything else is rejected.
    pub fn parse(value: &str) -> Result<Self, SidecarError> {
        let parts: Vec<&str> = value.split('.').collect();
        if parts.len() != 3 || parts.iter().any(|p| p.is_empty()) {
            return Err(SidecarError::InvalidTarget {
                value: value.to_string(),
            });
        }
        Ok(Self {
            catalog: parts[0].to_string(),
            schema: parts[1].to_string(),
            table: parts[2].to_string(),
        })
    }
}

// ---------------------------------------------------------------------------
// On-the-wire sidecar TOML
// ---------------------------------------------------------------------------

/// Lean serialization-only mirror of the on-disk sidecar shape. Keeping
/// this struct local rather than reusing `rocky_core::models::ModelConfig`
/// lets us emit a compact file (no empty `depends_on = []`, no empty
/// `sources = []`, no `classification = {}` block) that Rocky's loader
/// still accepts cleanly via field-default inference.
#[derive(Debug, Serialize)]
struct SidecarToml<'a> {
    name: &'a str,
    strategy: SidecarStrategyToml<'a>,
    target: SidecarTargetToml<'a>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "type")]
enum SidecarStrategyToml<'a> {
    #[serde(rename = "full_refresh")]
    FullRefresh,
    #[serde(rename = "incremental")]
    Incremental { timestamp_column: &'a str },
    #[serde(rename = "merge")]
    Merge,
    #[serde(rename = "ephemeral")]
    Ephemeral,
}

#[derive(Debug, Serialize)]
struct SidecarTargetToml<'a> {
    catalog: &'a str,
    schema: &'a str,
    table: &'a str,
}

/// Render the sidecar TOML body for the given (name, strategy, target).
///
/// The `Merge` variant intentionally serializes without a `unique_key`
/// list — Rocky's `StrategyConfig::Merge` requires `unique_key`, so the
/// emitted TOML for `--materialization merge` is **incomplete on its own**.
/// The user is expected to fill in the unique key before running
/// `rocky run`. This is consistent with the brief's "first cut" scope:
/// the AI doesn't know the unique key, and we don't have a flag to capture
/// it yet. A future iteration can add `--unique-key`.
pub fn render_sidecar_toml(
    name: &str,
    materialization: &SidecarMaterialization,
    target: &SidecarTarget,
) -> Result<String, SidecarError> {
    let strategy = match materialization {
        SidecarMaterialization::FullRefresh => SidecarStrategyToml::FullRefresh,
        SidecarMaterialization::Incremental { watermark } => SidecarStrategyToml::Incremental {
            timestamp_column: watermark,
        },
        SidecarMaterialization::Merge => SidecarStrategyToml::Merge,
        SidecarMaterialization::Ephemeral => SidecarStrategyToml::Ephemeral,
    };

    let toml = SidecarToml {
        name,
        strategy,
        target: SidecarTargetToml {
            catalog: &target.catalog,
            schema: &target.schema,
            table: &target.table,
        },
    };

    Ok(toml::to_string(&toml)?)
}

// ---------------------------------------------------------------------------
// Filesystem write
// ---------------------------------------------------------------------------

/// Result of [`write_model_files`] — the absolute paths written so the CLI
/// can surface them in `--output json` and the human-readable summary.
#[derive(Debug, Clone)]
pub struct WrittenFiles {
    pub body_path: PathBuf,
    pub sidecar_path: PathBuf,
}

/// Write the generated body + sidecar TOML to `<dir>/<name>.<ext>` and
/// `<dir>/<name>.toml`. `format` is `"rocky"` or `"sql"` and dictates the
/// body extension; the sidecar is always `.toml`.
///
/// Pre-existing files trigger [`SidecarError::AlreadyExists`] unless
/// `overwrite` is true. The check is performed before any write so a
/// failed precondition leaves the filesystem untouched.
///
/// The parent directory is created if it doesn't already exist.
pub fn write_model_files(
    dir: &Path,
    name: &str,
    format: &str,
    body: &str,
    materialization: &SidecarMaterialization,
    target: &SidecarTarget,
    overwrite: bool,
) -> Result<WrittenFiles, SidecarError> {
    let ext = match format {
        "rocky" => "rocky",
        // Anything that isn't the Rocky DSL is treated as raw SQL — the
        // generate pipeline only ever produces "rocky" or "sql" today.
        _ => "sql",
    };

    let body_path = dir.join(format!("{name}.{ext}"));
    let sidecar_path = dir.join(format!("{name}.toml"));

    if !overwrite {
        if body_path.exists() {
            return Err(SidecarError::AlreadyExists {
                path: body_path.display().to_string(),
            });
        }
        if sidecar_path.exists() {
            return Err(SidecarError::AlreadyExists {
                path: sidecar_path.display().to_string(),
            });
        }
    }

    if !dir.exists() {
        std::fs::create_dir_all(dir).map_err(|e| SidecarError::Io {
            path: dir.display().to_string(),
            source: e,
        })?;
    }

    let sidecar_toml = render_sidecar_toml(name, materialization, target)?;

    std::fs::write(&body_path, body).map_err(|e| SidecarError::Io {
        path: body_path.display().to_string(),
        source: e,
    })?;
    std::fs::write(&sidecar_path, &sidecar_toml).map_err(|e| SidecarError::Io {
        path: sidecar_path.display().to_string(),
        source: e,
    })?;

    Ok(WrittenFiles {
        body_path,
        sidecar_path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use rocky_core::models::{StrategyConfig, parse_model_inline};

    #[test]
    fn render_full_refresh_default_target() {
        let target = SidecarTarget::default_for("fct_orders");
        let toml = render_sidecar_toml("fct_orders", &SidecarMaterialization::FullRefresh, &target)
            .unwrap();

        assert!(toml.contains("name = \"fct_orders\""));
        assert!(toml.contains("[strategy]"));
        assert!(toml.contains("type = \"full_refresh\""));
        assert!(toml.contains("[target]"));
        assert!(toml.contains("catalog = \"generated\""));
        assert!(toml.contains("schema = \"ai\""));
        assert!(toml.contains("table = \"fct_orders\""));
    }

    #[test]
    fn render_incremental_carries_watermark_as_timestamp_column() {
        let target = SidecarTarget::default_for("events");
        let toml = render_sidecar_toml(
            "events",
            &SidecarMaterialization::Incremental {
                watermark: "_synced_at".to_string(),
            },
            &target,
        )
        .unwrap();
        assert!(toml.contains("type = \"incremental\""));
        assert!(toml.contains("timestamp_column = \"_synced_at\""));
    }

    #[test]
    fn parse_target_rejects_two_part() {
        let err = SidecarTarget::parse("schema.table").unwrap_err();
        assert!(matches!(err, SidecarError::InvalidTarget { .. }));
    }

    #[test]
    fn parse_target_rejects_empty_segment() {
        let err = SidecarTarget::parse("a..c").unwrap_err();
        assert!(matches!(err, SidecarError::InvalidTarget { .. }));
    }

    #[test]
    fn parse_materialization_unknown_rejected() {
        let err = SidecarMaterialization::parse("materialized_view", None).unwrap_err();
        assert!(matches!(err, SidecarError::UnknownMaterialization { .. }));
    }

    #[test]
    fn parse_incremental_requires_watermark() {
        let err = SidecarMaterialization::parse("incremental", None).unwrap_err();
        assert!(matches!(err, SidecarError::MissingWatermark));
    }

    #[test]
    fn write_creates_body_and_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let target = SidecarTarget::default_for("fct_orders");

        let written = write_model_files(
            dir.path(),
            "fct_orders",
            "rocky",
            "from orders\nselect { id }",
            &SidecarMaterialization::FullRefresh,
            &target,
            false,
        )
        .unwrap();

        assert!(written.body_path.exists());
        assert!(written.sidecar_path.exists());
        assert!(written.body_path.ends_with("fct_orders.rocky"));
        assert!(written.sidecar_path.ends_with("fct_orders.toml"));
    }

    #[test]
    fn write_refuses_existing_files_without_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let target = SidecarTarget::default_for("fct_orders");
        // Pre-create the sidecar file
        std::fs::write(dir.path().join("fct_orders.toml"), "name = \"old\"\n").unwrap();

        let err = write_model_files(
            dir.path(),
            "fct_orders",
            "sql",
            "SELECT 1",
            &SidecarMaterialization::FullRefresh,
            &target,
            false,
        )
        .unwrap_err();
        assert!(matches!(err, SidecarError::AlreadyExists { .. }));

        // Body should not have been written either — precondition fails before
        // any disk mutation.
        assert!(!dir.path().join("fct_orders.sql").exists());
    }

    #[test]
    fn write_overwrites_when_flag_set() {
        let dir = tempfile::tempdir().unwrap();
        let target = SidecarTarget::default_for("m");
        std::fs::write(dir.path().join("m.toml"), "stale\n").unwrap();
        std::fs::write(dir.path().join("m.sql"), "stale\n").unwrap();

        write_model_files(
            dir.path(),
            "m",
            "sql",
            "SELECT 42",
            &SidecarMaterialization::FullRefresh,
            &target,
            true,
        )
        .unwrap();

        let body = std::fs::read_to_string(dir.path().join("m.sql")).unwrap();
        assert_eq!(body, "SELECT 42");
        let sidecar = std::fs::read_to_string(dir.path().join("m.toml")).unwrap();
        assert!(sidecar.contains("type = \"full_refresh\""));
    }

    #[test]
    fn emitted_sidecar_loads_via_rocky_core_full_refresh() {
        // Smoke-check: the TOML we emit deserializes through rocky-core's
        // model loader. Use `parse_model_inline` with synthetic frontmatter
        // to exercise `RawModelConfig` → `ModelConfig` resolution against
        // our emitted TOML body.
        let target = SidecarTarget::default_for("fct_orders");
        let sidecar =
            render_sidecar_toml("fct_orders", &SidecarMaterialization::FullRefresh, &target)
                .unwrap();

        let inline = format!("---toml\n{sidecar}---\n\nSELECT 1\n");
        let model = parse_model_inline(&inline, "fct_orders.sql", None).unwrap();

        assert_eq!(model.config.name, "fct_orders");
        assert_eq!(model.config.target.catalog, "generated");
        assert_eq!(model.config.target.schema, "ai");
        assert_eq!(model.config.target.table, "fct_orders");
        assert!(matches!(model.config.strategy, StrategyConfig::FullRefresh));
    }

    #[test]
    fn emitted_sidecar_loads_via_rocky_core_incremental() {
        let target = SidecarTarget {
            catalog: "warehouse".to_string(),
            schema: "raw".to_string(),
            table: "events".to_string(),
        };
        let sidecar = render_sidecar_toml(
            "events",
            &SidecarMaterialization::Incremental {
                watermark: "_synced_at".to_string(),
            },
            &target,
        )
        .unwrap();

        let inline = format!("---toml\n{sidecar}---\n\nSELECT 1\n");
        let model = parse_model_inline(&inline, "events.sql", None).unwrap();

        match model.config.strategy {
            StrategyConfig::Incremental { timestamp_column } => {
                assert_eq!(timestamp_column, "_synced_at");
            }
            other => panic!("expected Incremental, got {other:?}"),
        }
        assert_eq!(model.config.target.catalog, "warehouse");
    }

    #[test]
    fn emitted_sidecar_loads_via_rocky_core_ephemeral() {
        let target = SidecarTarget::default_for("staging_users");
        let sidecar =
            render_sidecar_toml("staging_users", &SidecarMaterialization::Ephemeral, &target)
                .unwrap();

        let inline = format!("---toml\n{sidecar}---\n\nSELECT 1\n");
        let model = parse_model_inline(&inline, "staging_users.sql", None).unwrap();
        assert!(matches!(model.config.strategy, StrategyConfig::Ephemeral));
    }
}
