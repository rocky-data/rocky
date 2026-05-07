//! Minimal `profiles.yml` parser for `rocky import-dbt`.
//!
//! Reads `<dbt_project>/profiles.yml` (we deliberately do not consult
//! `~/.dbt/profiles.yml` in v0 — the importer is meant to operate on a
//! self-contained project directory) and extracts just enough information
//! to drive Rocky's `[adapter]` block: the profile's active target name and
//! the `type` field of that target's output (`duckdb` / `databricks` /
//! `snowflake` / `bigquery` / …).
//!
//! Connection secrets are deliberately **not** copied across — every
//! adapter field that would normally hold a host/token/path is written as
//! a `${VAR}` env-var placeholder in the emitted `rocky.toml`, with the
//! variable names captured in [`ProfileEnvVars`] so the importer can list
//! them under "Required env vars" in `MIGRATION-NOTES.md`.

use std::path::Path;

use serde::Deserialize;

/// The Rocky adapter type a dbt profile maps to.
///
/// `Unmapped` is returned for any dbt profile type Rocky does not yet
/// support natively (e.g. `redshift`, `postgres`, `spark`). Callers stub
/// the adapter as DuckDB and surface a TODO entry in `MIGRATION-NOTES.md`
/// — the project still compiles even when the original warehouse isn't
/// supported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdapterKind {
    DuckDb,
    Databricks,
    Snowflake,
    BigQuery,
    /// dbt profile type not natively supported (e.g. redshift, postgres).
    Unmapped(String),
}

impl AdapterKind {
    /// Map a dbt profile `type` string to a Rocky adapter kind.
    pub fn from_dbt_type(s: &str) -> Self {
        match s.to_ascii_lowercase().as_str() {
            "duckdb" => Self::DuckDb,
            "databricks" => Self::Databricks,
            "snowflake" => Self::Snowflake,
            "bigquery" => Self::BigQuery,
            other => Self::Unmapped(other.to_string()),
        }
    }

    /// The Rocky adapter `type` literal to emit in `rocky.toml`.
    ///
    /// `Unmapped(_)` falls back to `"duckdb"` so the generated repo still
    /// loads; the original dbt type is preserved on
    /// [`ProfileResolution::original_type`] for the MIGRATION-NOTES entry.
    pub fn rocky_type(&self) -> &'static str {
        match self {
            Self::DuckDb | Self::Unmapped(_) => "duckdb",
            Self::Databricks => "databricks",
            Self::Snowflake => "snowflake",
            Self::BigQuery => "bigquery",
        }
    }
}

/// The env-var refs an emitted `[adapter]` block depends on.
///
/// Used to populate the "Required env vars" list in `MIGRATION-NOTES.md`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProfileEnvVars {
    pub vars: Vec<String>,
}

/// Result of resolving a dbt profile to a Rocky adapter shape.
#[derive(Debug, Clone)]
pub struct ProfileResolution {
    /// Mapped Rocky adapter kind. `Unmapped(_)` indicates a fallback to DuckDB.
    pub kind: AdapterKind,
    /// Original dbt profile `type` value, preserved for diagnostics regardless
    /// of mapping outcome.
    pub original_type: String,
    /// `[adapter]` body to write into `rocky.toml`. Connection fields are
    /// `${VAR}` placeholders, never live secrets.
    pub adapter_toml: String,
    /// Env vars referenced by `adapter_toml` — surfaced in MIGRATION-NOTES.
    pub env_vars: ProfileEnvVars,
    /// Database / catalog name as declared in profiles.yml, when present —
    /// used as a default for `[target] catalog` in `models/_defaults.toml`.
    pub database: Option<String>,
    /// Schema / dataset name as declared in profiles.yml, when present —
    /// used as a default for `[target] schema`.
    pub schema: Option<String>,
}

/// Resolve the adapter shape from a dbt project directory.
///
/// Tries `<dbt_project>/profiles.yml`. If absent or unparseable, returns
/// `None` and the caller stubs a DuckDB adapter with a TODO note. We
/// deliberately do **not** read `~/.dbt/profiles.yml` in v0 — the importer
/// is meant to operate on a self-contained project tree.
pub fn resolve_from_project(dbt_project: &Path) -> Option<ProfileResolution> {
    let profiles_path = dbt_project.join("profiles.yml");
    if !profiles_path.exists() {
        return None;
    }
    parse_profiles_file(&profiles_path).ok()
}

/// Stub adapter used when profiles.yml is missing or unparseable, or when
/// the caller passed `--target-adapter` to override profile detection.
///
/// Always emits a DuckDB adapter pointing at a relative `warehouse.duckdb`
/// path — keeps the generated repo loadable without any external config.
pub fn stub_resolution(reason: StubReason) -> ProfileResolution {
    ProfileResolution {
        kind: AdapterKind::DuckDb,
        original_type: match reason {
            StubReason::ProfilesAbsent => "<no profiles.yml>".to_string(),
            StubReason::ProfilesUnparseable => "<unparseable profiles.yml>".to_string(),
            StubReason::ForcedDuckDb => "duckdb (forced via --target-adapter)".to_string(),
        },
        adapter_toml: "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n".to_string(),
        env_vars: ProfileEnvVars::default(),
        database: None,
        schema: None,
    }
}

/// Why the importer fell back to a stub adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StubReason {
    ProfilesAbsent,
    ProfilesUnparseable,
    ForcedDuckDb,
}

/// Build a [`ProfileResolution`] for a caller-provided adapter kind, used
/// when `--target-adapter` overrides profile detection. The emitted
/// `[adapter]` block is the canonical `${VAR}` skeleton for the requested
/// adapter — never inlines literal credentials.
pub fn resolution_for_kind(kind: AdapterKind, original_label: &str) -> ProfileResolution {
    let (adapter_toml, env_vars) = render_adapter_skeleton(&kind);
    ProfileResolution {
        kind,
        original_type: original_label.to_string(),
        adapter_toml,
        env_vars,
        database: None,
        schema: None,
    }
}

#[derive(Debug, Deserialize)]
struct ProfilesFile {
    #[serde(flatten)]
    profiles: std::collections::BTreeMap<String, RawProfile>,
}

#[derive(Debug, Deserialize)]
struct RawProfile {
    target: Option<String>,
    outputs: Option<std::collections::BTreeMap<String, RawOutput>>,
}

#[derive(Debug, Deserialize)]
struct RawOutput {
    #[serde(rename = "type")]
    profile_type: Option<String>,
    database: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
    dataset: Option<String>,
}

fn parse_profiles_file(path: &Path) -> Result<ProfileResolution, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let parsed: ProfilesFile = serde_yaml::from_str(&content)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    // Pick the first profile (dbt projects typically have exactly one).
    let (_profile_name, profile) = parsed
        .profiles
        .into_iter()
        .next()
        .ok_or_else(|| format!("{}: no profiles defined", path.display()))?;

    let target_name = profile.target.clone().unwrap_or_else(|| "dev".to_string());

    let outputs = profile
        .outputs
        .ok_or_else(|| format!("{}: profile has no outputs", path.display()))?;

    let output = outputs
        .get(&target_name)
        .or_else(|| outputs.values().next())
        .ok_or_else(|| format!("{}: target '{target_name}' not found", path.display()))?;

    let profile_type = output
        .profile_type
        .clone()
        .ok_or_else(|| format!("{}: missing 'type' on output", path.display()))?;

    let kind = AdapterKind::from_dbt_type(&profile_type);
    let (adapter_toml, env_vars) = render_adapter_skeleton(&kind);

    let database = output.database.clone().or_else(|| output.catalog.clone());
    let schema = output.schema.clone().or_else(|| output.dataset.clone());

    Ok(ProfileResolution {
        kind,
        original_type: profile_type,
        adapter_toml,
        env_vars,
        database,
        schema,
    })
}

fn render_adapter_skeleton(kind: &AdapterKind) -> (String, ProfileEnvVars) {
    match kind {
        AdapterKind::DuckDb => (
            "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n".to_string(),
            ProfileEnvVars::default(),
        ),
        AdapterKind::Databricks => {
            let body = "[adapter]\n\
                type      = \"databricks\"\n\
                host      = \"${DATABRICKS_HOST}\"\n\
                http_path = \"${DATABRICKS_HTTP_PATH}\"\n\n\
                [adapter.auth]\n\
                token = \"${DATABRICKS_TOKEN}\"\n"
                .to_string();
            (
                body,
                ProfileEnvVars {
                    vars: vec![
                        "DATABRICKS_HOST".to_string(),
                        "DATABRICKS_HTTP_PATH".to_string(),
                        "DATABRICKS_TOKEN".to_string(),
                    ],
                },
            )
        }
        AdapterKind::Snowflake => {
            let body = "[adapter]\n\
                type     = \"snowflake\"\n\
                account  = \"${SNOWFLAKE_ACCOUNT}\"\n\
                username = \"${SNOWFLAKE_USER}\"\n\n\
                [adapter.auth]\n\
                password = \"${SNOWFLAKE_PASSWORD}\"\n"
                .to_string();
            (
                body,
                ProfileEnvVars {
                    vars: vec![
                        "SNOWFLAKE_ACCOUNT".to_string(),
                        "SNOWFLAKE_USER".to_string(),
                        "SNOWFLAKE_PASSWORD".to_string(),
                    ],
                },
            )
        }
        AdapterKind::BigQuery => {
            let body = "[adapter]\n\
                type    = \"bigquery\"\n\
                project = \"${BIGQUERY_PROJECT}\"\n\
                dataset = \"${BIGQUERY_DATASET}\"\n\
                credentials_path = \"${GOOGLE_APPLICATION_CREDENTIALS}\"\n"
                .to_string();
            (
                body,
                ProfileEnvVars {
                    vars: vec![
                        "BIGQUERY_PROJECT".to_string(),
                        "BIGQUERY_DATASET".to_string(),
                        "GOOGLE_APPLICATION_CREDENTIALS".to_string(),
                    ],
                },
            )
        }
        AdapterKind::Unmapped(_) => (
            "[adapter]\ntype = \"duckdb\"\npath = \"warehouse.duckdb\"\n".to_string(),
            ProfileEnvVars::default(),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_adapter_kinds() {
        assert_eq!(AdapterKind::from_dbt_type("duckdb"), AdapterKind::DuckDb);
        assert_eq!(
            AdapterKind::from_dbt_type("databricks"),
            AdapterKind::Databricks
        );
        assert_eq!(
            AdapterKind::from_dbt_type("snowflake"),
            AdapterKind::Snowflake
        );
        assert_eq!(
            AdapterKind::from_dbt_type("bigquery"),
            AdapterKind::BigQuery
        );
    }

    #[test]
    fn unmapped_falls_back_to_duckdb_literal() {
        let kind = AdapterKind::from_dbt_type("redshift");
        assert!(matches!(kind, AdapterKind::Unmapped(ref s) if s == "redshift"));
        assert_eq!(kind.rocky_type(), "duckdb");
    }

    #[test]
    fn parses_minimal_duckdb_profile() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("profiles.yml"),
            "ecommerce:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: dev.db\n      schema: main\n",
        )
        .unwrap();
        let resolved = resolve_from_project(dir.path()).unwrap();
        assert_eq!(resolved.kind, AdapterKind::DuckDb);
        assert!(resolved.adapter_toml.contains("type = \"duckdb\""));
        assert!(resolved.env_vars.vars.is_empty());
        assert_eq!(resolved.schema.as_deref(), Some("main"));
    }

    #[test]
    fn parses_databricks_with_env_vars() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("profiles.yml"),
            "shop:\n  target: prod\n  outputs:\n    prod:\n      type: databricks\n      host: dbc-x.cloud.databricks.com\n      http_path: /sql/1.0/warehouses/abc\n      catalog: analytics\n      schema: marts\n      token: should-not-leak\n",
        )
        .unwrap();
        let resolved = resolve_from_project(dir.path()).unwrap();
        assert_eq!(resolved.kind, AdapterKind::Databricks);
        // No literal secrets propagated.
        assert!(!resolved.adapter_toml.contains("should-not-leak"));
        assert!(!resolved.adapter_toml.contains("dbc-x.cloud.databricks.com"));
        // Env-var refs surfaced.
        assert!(
            resolved
                .env_vars
                .vars
                .contains(&"DATABRICKS_TOKEN".to_string())
        );
        assert_eq!(resolved.database.as_deref(), Some("analytics"));
    }

    #[test]
    fn unmapped_profile_stubs_duckdb_with_original_type_preserved() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(
            dir.path().join("profiles.yml"),
            "p:\n  target: dev\n  outputs:\n    dev:\n      type: redshift\n      host: x\n",
        )
        .unwrap();
        let resolved = resolve_from_project(dir.path()).unwrap();
        assert!(matches!(
            resolved.kind,
            AdapterKind::Unmapped(ref s) if s == "redshift"
        ));
        assert!(resolved.adapter_toml.contains("type = \"duckdb\""));
        assert_eq!(resolved.original_type, "redshift");
    }

    #[test]
    fn missing_profiles_file_returns_none() {
        let dir = tempfile::TempDir::new().unwrap();
        assert!(resolve_from_project(dir.path()).is_none());
    }

    #[test]
    fn forced_kind_overrides_profile() {
        let resolved = resolution_for_kind(
            AdapterKind::Databricks,
            "databricks (forced via --target-adapter)",
        );
        assert_eq!(resolved.kind, AdapterKind::Databricks);
        assert!(resolved.adapter_toml.contains("databricks"));
        assert!(resolved.original_type.contains("forced"));
    }
}
