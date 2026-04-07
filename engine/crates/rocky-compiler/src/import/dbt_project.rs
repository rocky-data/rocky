//! dbt_project.yml parser and config inheritance resolution.
//!
//! Parses dbt's project-level configuration and resolves per-model
//! materialization strategy by walking the directory hierarchy.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use serde::Deserialize;

/// Parsed representation of a dbt_project.yml file.
#[derive(Debug, Clone)]
pub struct DbtProjectConfig {
    pub name: String,
    pub version: Option<String>,
    pub profile: Option<String>,
    pub model_paths: Vec<PathBuf>,
    pub seed_paths: Vec<PathBuf>,
    pub test_paths: Vec<PathBuf>,
    pub target_path: Option<PathBuf>,
    pub vars: HashMap<String, String>,
    pub models: DbtModelDefaults,
}

/// Project-level model defaults and per-directory overrides.
#[derive(Debug, Clone, Default)]
pub struct DbtModelDefaults {
    /// Project-level default materialization.
    pub materialized: Option<String>,
    /// Per-directory overrides: key is directory name (e.g. "staging").
    pub directories: HashMap<String, DbtDirectoryConfig>,
}

/// Configuration for models in a specific directory.
#[derive(Debug, Clone, Default)]
pub struct DbtDirectoryConfig {
    pub materialized: Option<String>,
    pub schema: Option<String>,
    pub tags: Vec<String>,
}

/// Resolved configuration for a specific model after walking the hierarchy.
#[derive(Debug, Clone)]
pub struct ResolvedModelConfig {
    pub materialized: String,
    pub schema: Option<String>,
    pub tags: Vec<String>,
}

// --- Raw YAML deserialization types ---

#[derive(Deserialize)]
struct RawDbtProject {
    name: Option<String>,
    version: Option<String>,
    profile: Option<String>,
    #[serde(rename = "model-paths", default)]
    model_paths: Option<Vec<String>>,
    #[serde(rename = "seed-paths", default)]
    seed_paths: Option<Vec<String>>,
    #[serde(rename = "test-paths", default)]
    test_paths: Option<Vec<String>>,
    #[serde(rename = "target-path", default)]
    target_path: Option<String>,
    #[serde(default)]
    vars: HashMap<String, serde_yaml::Value>,
    #[serde(default)]
    models: Option<serde_yaml::Value>,
}

/// Parse a dbt_project.yml file into a `DbtProjectConfig`.
pub fn from_yaml(path: &Path) -> Result<DbtProjectConfig, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

    parse_yaml_content(&content, path)
}

fn parse_yaml_content(content: &str, path: &Path) -> Result<DbtProjectConfig, String> {
    let raw: RawDbtProject = serde_yaml::from_str(content)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    let name = raw
        .name
        .ok_or_else(|| format!("{}: missing required field 'name'", path.display()))?;

    let model_paths = raw
        .model_paths
        .unwrap_or_else(|| vec!["models".to_string()])
        .into_iter()
        .map(PathBuf::from)
        .collect();

    let seed_paths = raw
        .seed_paths
        .unwrap_or_else(|| vec!["seeds".to_string()])
        .into_iter()
        .map(PathBuf::from)
        .collect();

    let test_paths = raw
        .test_paths
        .unwrap_or_else(|| vec!["tests".to_string()])
        .into_iter()
        .map(PathBuf::from)
        .collect();

    let target_path = raw.target_path.map(PathBuf::from);

    // Flatten vars to strings
    let vars = raw
        .vars
        .into_iter()
        .map(|(k, v)| {
            let s = match v {
                serde_yaml::Value::String(s) => s,
                other => format!("{other:?}"),
            };
            (k, s)
        })
        .collect();

    // Parse models section
    let models = parse_models_section(&raw.models, &name);

    Ok(DbtProjectConfig {
        name,
        version: raw.version,
        profile: raw.profile,
        model_paths,
        seed_paths,
        test_paths,
        target_path,
        vars,
        models,
    })
}

/// Parse the `models:` section of dbt_project.yml.
///
/// The structure is:
/// ```yaml
/// models:
///   project_name:
///     +materialized: view     # project-level default
///     staging:
///       +materialized: view
///       +schema: staging
///     marts:
///       +materialized: table
/// ```
fn parse_models_section(value: &Option<serde_yaml::Value>, project_name: &str) -> DbtModelDefaults {
    let Some(serde_yaml::Value::Mapping(top)) = value else {
        return DbtModelDefaults::default();
    };

    // Look for the project name key
    let project_key = serde_yaml::Value::String(project_name.to_string());
    let Some(serde_yaml::Value::Mapping(project_map)) = top.get(&project_key) else {
        return DbtModelDefaults::default();
    };

    let mut defaults = DbtModelDefaults::default();

    for (key, val) in project_map {
        let Some(key_str) = key.as_str() else {
            continue;
        };

        // Keys starting with + are config values applied at the project level
        let clean_key = key_str.strip_prefix('+').unwrap_or(key_str);

        if clean_key == "materialized" {
            if let Some(s) = val.as_str() {
                defaults.materialized = Some(s.to_string());
            }
        } else if let serde_yaml::Value::Mapping(_) = val {
            // This is a directory config
            let dir_config = parse_directory_config(val);
            defaults
                .directories
                .insert(clean_key.to_string(), dir_config);
        }
    }

    defaults
}

fn parse_directory_config(value: &serde_yaml::Value) -> DbtDirectoryConfig {
    let Some(map) = value.as_mapping() else {
        return DbtDirectoryConfig::default();
    };

    let mut config = DbtDirectoryConfig::default();

    for (key, val) in map {
        let Some(key_str) = key.as_str() else {
            continue;
        };
        let clean_key = key_str.strip_prefix('+').unwrap_or(key_str);

        match clean_key {
            "materialized" => {
                if let Some(s) = val.as_str() {
                    config.materialized = Some(s.to_string());
                }
            }
            "schema" => {
                if let Some(s) = val.as_str() {
                    config.schema = Some(s.to_string());
                }
            }
            "tags" => {
                if let Some(seq) = val.as_sequence() {
                    config.tags = seq
                        .iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect();
                }
            }
            _ => {}
        }
    }

    config
}

/// Resolve the effective configuration for a model at a given relative path.
///
/// Resolution order (later overrides earlier):
/// 1. dbt defaults (materialized: "view")
/// 2. Project-level `models:` config
/// 3. Directory-level config (walking from root to model's directory)
///
/// Note: model-level `config()` blocks override everything but are
/// handled separately in the import pipeline.
pub fn resolve_model_config(project: &DbtProjectConfig, model_path: &Path) -> ResolvedModelConfig {
    // Start with dbt's default: materialized = "view"
    let mut materialized = "view".to_string();
    let mut schema: Option<String> = None;
    let mut tags: Vec<String> = Vec::new();

    // Apply project-level default
    if let Some(ref mat) = project.models.materialized {
        materialized = mat.clone();
    }

    // Walk directory components and apply matching overrides
    let components: Vec<&str> = model_path
        .parent()
        .unwrap_or(Path::new(""))
        .components()
        .filter_map(|c| c.as_os_str().to_str())
        .collect();

    for component in &components {
        if let Some(dir_config) = project.models.directories.get(*component) {
            if let Some(ref mat) = dir_config.materialized {
                materialized = mat.clone();
            }
            if let Some(ref s) = dir_config.schema {
                schema = Some(s.clone());
            }
            if !dir_config.tags.is_empty() {
                tags.extend(dir_config.tags.clone());
            }
        }
    }

    ResolvedModelConfig {
        materialized,
        schema,
        tags,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_project() {
        let yaml = r#"
name: my_analytics
version: "1.0.0"
profile: my_profile
model-paths: ["models"]
target-path: target
vars:
  start_date: "2020-01-01"
models:
  my_analytics:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: analytics
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        assert_eq!(config.name, "my_analytics");
        assert_eq!(config.version.as_deref(), Some("1.0.0"));
        assert_eq!(config.profile.as_deref(), Some("my_profile"));
        assert_eq!(config.model_paths, vec![PathBuf::from("models")]);
        assert_eq!(config.target_path, Some(PathBuf::from("target")));
        assert_eq!(config.vars.get("start_date").unwrap(), "2020-01-01");

        let staging = config.models.directories.get("staging").unwrap();
        assert_eq!(staging.materialized.as_deref(), Some("view"));
        assert_eq!(staging.schema.as_deref(), Some("staging"));

        let marts = config.models.directories.get("marts").unwrap();
        assert_eq!(marts.materialized.as_deref(), Some("table"));
        assert_eq!(marts.schema.as_deref(), Some("analytics"));
    }

    #[test]
    fn test_parse_custom_model_paths() {
        let yaml = r#"
name: custom_paths
model-paths: ["sql/models", "transformations"]
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        assert_eq!(
            config.model_paths,
            vec![
                PathBuf::from("sql/models"),
                PathBuf::from("transformations")
            ]
        );
    }

    #[test]
    fn test_parse_missing_optional_fields() {
        let yaml = "name: minimal\n";
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        assert_eq!(config.name, "minimal");
        assert!(config.version.is_none());
        assert!(config.profile.is_none());
        assert_eq!(config.model_paths, vec![PathBuf::from("models")]);
        assert_eq!(config.seed_paths, vec![PathBuf::from("seeds")]);
        assert_eq!(config.test_paths, vec![PathBuf::from("tests")]);
        assert!(config.target_path.is_none());
        assert!(config.vars.is_empty());
    }

    #[test]
    fn test_parse_malformed_yaml() {
        let yaml = "name: [invalid yaml structure\n  bad: indentation";
        let result = parse_yaml_content(yaml, Path::new("bad.yml"));
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_missing_name() {
        let yaml = "version: '1.0'\n";
        let result = parse_yaml_content(yaml, Path::new("dbt_project.yml"));
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("missing required field 'name'")
        );
    }

    #[test]
    fn test_resolve_root_default() {
        let yaml = r#"
name: proj
models:
  proj:
    +materialized: table
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        let resolved = resolve_model_config(&config, Path::new("my_model.sql"));
        assert_eq!(resolved.materialized, "table");
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_directory_override() {
        let yaml = r#"
name: proj
models:
  proj:
    +materialized: table
    staging:
      +materialized: view
      +schema: staging
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();

        // Model in staging directory
        let resolved = resolve_model_config(&config, Path::new("staging/stg_orders.sql"));
        assert_eq!(resolved.materialized, "view");
        assert_eq!(resolved.schema.as_deref(), Some("staging"));

        // Model at root
        let resolved = resolve_model_config(&config, Path::new("other_model.sql"));
        assert_eq!(resolved.materialized, "table");
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_nested_directory() {
        let yaml = r#"
name: proj
models:
  proj:
    staging:
      +materialized: view
      +schema: staging
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();

        // Model nested under staging/stripe — staging config applies
        let resolved = resolve_model_config(&config, Path::new("staging/stripe/stg_payments.sql"));
        assert_eq!(resolved.materialized, "view");
        assert_eq!(resolved.schema.as_deref(), Some("staging"));
    }

    #[test]
    fn test_resolve_no_project_config() {
        let yaml = "name: proj\n";
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        let resolved = resolve_model_config(&config, Path::new("staging/model.sql"));
        // Falls back to dbt default
        assert_eq!(resolved.materialized, "view");
        assert!(resolved.schema.is_none());
    }

    #[test]
    fn test_resolve_tags_inherited() {
        let yaml = r#"
name: proj
models:
  proj:
    staging:
      +materialized: view
      +tags: ["pii", "daily"]
"#;
        let config = parse_yaml_content(yaml, Path::new("dbt_project.yml")).unwrap();
        let resolved = resolve_model_config(&config, Path::new("staging/stg_customers.sql"));
        assert_eq!(resolved.tags, vec!["pii", "daily"]);
    }
}
