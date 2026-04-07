//! dbt sources.yml parser and Rocky SourceConfig mapping.
//!
//! Parses dbt source definitions from YAML files and converts them into
//! Rocky's `SourceConfig` format for catalog.schema.table resolution.

use std::collections::HashMap;
use std::path::Path;

use rocky_core::models::SourceConfig;
use serde::Deserialize;

/// A dbt source definition.
#[derive(Debug, Clone)]
pub struct DbtSource {
    pub name: String,
    pub database: Option<String>,
    pub schema: Option<String>,
    pub tables: Vec<DbtSourceTable>,
}

/// A table within a dbt source.
#[derive(Debug, Clone)]
pub struct DbtSourceTable {
    pub name: String,
    pub columns: Vec<DbtSourceColumn>,
    pub freshness: Option<DbtFreshness>,
    pub loaded_at_field: Option<String>,
}

/// A column within a dbt source table.
#[derive(Debug, Clone)]
pub struct DbtSourceColumn {
    pub name: String,
    pub description: Option<String>,
    pub tests: Vec<DbtTest>,
}

/// Freshness configuration for a dbt source.
#[derive(Debug, Clone)]
pub struct DbtFreshness {
    pub warn_after: Option<DbtFreshnessThreshold>,
    pub error_after: Option<DbtFreshnessThreshold>,
}

/// A freshness threshold (count + period).
#[derive(Debug, Clone)]
pub struct DbtFreshnessThreshold {
    pub count: u64,
    pub period: String,
}

/// A dbt test reference.
#[derive(Debug, Clone)]
pub struct DbtTest {
    pub test_type: String,
    pub config: serde_yaml::Value,
}

/// Mapping result from dbt source to Rocky config.
#[derive(Debug, Clone)]
pub struct RockySourceMapping {
    pub source_config: SourceConfig,
    pub columns: Vec<(String, String)>,
    pub tests: Vec<DbtTest>,
}

// --- Raw YAML deserialization types ---

#[derive(Deserialize)]
struct RawSourceFile {
    #[serde(default)]
    sources: Option<Vec<RawSource>>,
}

#[derive(Deserialize)]
struct RawSource {
    name: String,
    #[serde(default)]
    database: Option<String>,
    #[serde(default)]
    schema: Option<String>,
    #[serde(default)]
    tables: Vec<RawSourceTable>,
    #[serde(default)]
    freshness: Option<RawFreshness>,
    #[serde(default)]
    loaded_at_field: Option<String>,
}

#[derive(Deserialize)]
struct RawSourceTable {
    name: String,
    #[serde(default)]
    columns: Vec<RawSourceColumn>,
    #[serde(default)]
    freshness: Option<RawFreshness>,
    #[serde(default)]
    loaded_at_field: Option<String>,
}

#[derive(Deserialize)]
struct RawSourceColumn {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    tests: Option<Vec<serde_yaml::Value>>,
}

#[derive(Deserialize)]
struct RawFreshness {
    #[serde(default)]
    warn_after: Option<RawFreshnessThreshold>,
    #[serde(default)]
    error_after: Option<RawFreshnessThreshold>,
}

#[derive(Deserialize)]
struct RawFreshnessThreshold {
    count: u64,
    period: String,
}

/// Parse all source definitions from a YAML file.
///
/// The file must have a top-level `sources:` key. If the key is absent,
/// an empty vec is returned (the file is simply not a sources file).
pub fn parse_sources(path: &Path) -> Result<Vec<DbtSource>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;

    parse_sources_yaml(&content, path)
}

fn parse_sources_yaml(content: &str, path: &Path) -> Result<Vec<DbtSource>, String> {
    let raw: RawSourceFile = serde_yaml::from_str(content)
        .map_err(|e| format!("failed to parse {}: {e}", path.display()))?;

    let Some(raw_sources) = raw.sources else {
        return Ok(Vec::new());
    };

    let mut sources = Vec::new();
    for raw_source in raw_sources {
        let source_freshness = raw_source.freshness.as_ref();
        let source_loaded_at = raw_source.loaded_at_field.as_deref();

        let tables = raw_source
            .tables
            .into_iter()
            .map(|t| {
                let columns = t
                    .columns
                    .into_iter()
                    .map(|c| {
                        let tests = parse_column_tests(&c.tests);
                        DbtSourceColumn {
                            name: c.name,
                            description: c.description,
                            tests,
                        }
                    })
                    .collect();

                // Table-level freshness overrides source-level
                let freshness = t
                    .freshness
                    .as_ref()
                    .or(source_freshness)
                    .map(convert_freshness);

                let loaded_at_field = t
                    .loaded_at_field
                    .or_else(|| source_loaded_at.map(String::from));

                DbtSourceTable {
                    name: t.name,
                    columns,
                    freshness,
                    loaded_at_field,
                }
            })
            .collect();

        sources.push(DbtSource {
            name: raw_source.name,
            database: raw_source.database,
            schema: raw_source.schema,
            tables,
        });
    }

    Ok(sources)
}

fn parse_column_tests(tests: &Option<Vec<serde_yaml::Value>>) -> Vec<DbtTest> {
    let Some(tests) = tests else {
        return Vec::new();
    };

    tests
        .iter()
        .map(|t| match t {
            serde_yaml::Value::String(s) => DbtTest {
                test_type: s.clone(),
                config: serde_yaml::Value::Null,
            },
            serde_yaml::Value::Mapping(m) => {
                let test_type = m
                    .keys()
                    .next()
                    .and_then(|k| k.as_str())
                    .unwrap_or("unknown")
                    .to_string();
                let config = m
                    .values()
                    .next()
                    .cloned()
                    .unwrap_or(serde_yaml::Value::Null);
                DbtTest { test_type, config }
            }
            _ => DbtTest {
                test_type: "unknown".to_string(),
                config: t.clone(),
            },
        })
        .collect()
}

fn convert_freshness(raw: &RawFreshness) -> DbtFreshness {
    DbtFreshness {
        warn_after: raw.warn_after.as_ref().map(|t| DbtFreshnessThreshold {
            count: t.count,
            period: t.period.clone(),
        }),
        error_after: raw.error_after.as_ref().map(|t| DbtFreshnessThreshold {
            count: t.count,
            period: t.period.clone(),
        }),
    }
}

/// Scan a directory for YAML files containing source definitions.
///
/// Any `.yml` or `.yaml` file with a top-level `sources:` key is parsed.
pub fn scan_sources_in_dir(dir: &Path) -> Result<Vec<DbtSource>, String> {
    let mut all_sources = Vec::new();

    if !dir.exists() {
        return Ok(all_sources);
    }

    scan_sources_recursive(dir, &mut all_sources)?;
    Ok(all_sources)
}

fn scan_sources_recursive(dir: &Path, sources: &mut Vec<DbtSource>) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.is_dir() {
            scan_sources_recursive(&path, sources)?;
        } else if is_yaml_file(&path) {
            match parse_sources(&path) {
                Ok(found) => sources.extend(found),
                Err(_) => {
                    // Not a valid sources file; skip silently
                }
            }
        }
    }

    Ok(())
}

fn is_yaml_file(path: &Path) -> bool {
    path.extension()
        .is_some_and(|ext| ext == "yml" || ext == "yaml")
}

/// Convert dbt sources to a lookup map keyed by `(source_name, table_name)`.
///
/// Each entry maps to a `RockySourceMapping` with the fully qualified
/// catalog.schema.table reference.
pub fn sources_to_rocky_config(
    sources: &[DbtSource],
    default_catalog: &str,
) -> HashMap<(String, String), RockySourceMapping> {
    let mut map = HashMap::new();

    for source in sources {
        let catalog = source
            .database
            .as_deref()
            .unwrap_or(default_catalog)
            .to_string();

        let schema = source.schema.as_deref().unwrap_or(&source.name).to_string();

        for table in &source.tables {
            let source_config = SourceConfig {
                catalog: catalog.clone(),
                schema: schema.clone(),
                table: table.name.clone(),
            };

            let columns: Vec<(String, String)> = table
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.description.clone().unwrap_or_default()))
                .collect();

            let tests: Vec<DbtTest> = table.columns.iter().flat_map(|c| c.tests.clone()).collect();

            map.insert(
                (source.name.clone(), table.name.clone()),
                RockySourceMapping {
                    source_config,
                    columns,
                    tests,
                },
            );
        }
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_sources() {
        let yaml = r#"
sources:
  - name: stripe
    database: raw_db
    schema: stripe_data
    tables:
      - name: payments
        columns:
          - name: id
            description: Primary key
            tests:
              - unique
              - not_null
          - name: amount
            description: Payment amount
      - name: customers
"#;
        let sources = parse_sources_yaml(yaml, Path::new("_sources.yml")).unwrap();
        assert_eq!(sources.len(), 1);

        let stripe = &sources[0];
        assert_eq!(stripe.name, "stripe");
        assert_eq!(stripe.database.as_deref(), Some("raw_db"));
        assert_eq!(stripe.schema.as_deref(), Some("stripe_data"));
        assert_eq!(stripe.tables.len(), 2);

        let payments = &stripe.tables[0];
        assert_eq!(payments.name, "payments");
        assert_eq!(payments.columns.len(), 2);
        assert_eq!(payments.columns[0].name, "id");
        assert_eq!(payments.columns[0].tests.len(), 2);
        assert_eq!(payments.columns[0].tests[0].test_type, "unique");
        assert_eq!(payments.columns[0].tests[1].test_type, "not_null");
    }

    #[test]
    fn test_parse_source_with_freshness() {
        let yaml = r#"
sources:
  - name: events
    loaded_at_field: _loaded_at
    freshness:
      warn_after:
        count: 12
        period: hour
      error_after:
        count: 24
        period: hour
    tables:
      - name: page_views
      - name: clicks
        freshness:
          warn_after:
            count: 1
            period: hour
"#;
        let sources = parse_sources_yaml(yaml, Path::new("sources.yml")).unwrap();
        let events = &sources[0];

        // page_views inherits source-level freshness
        let pv = &events.tables[0];
        assert!(pv.freshness.is_some());
        let f = pv.freshness.as_ref().unwrap();
        assert_eq!(f.warn_after.as_ref().unwrap().count, 12);

        // clicks overrides with table-level freshness
        let clicks = &events.tables[1];
        let f = clicks.freshness.as_ref().unwrap();
        assert_eq!(f.warn_after.as_ref().unwrap().count, 1);
    }

    #[test]
    fn test_parse_file_without_sources_key() {
        let yaml = r#"
version: 2
models:
  - name: stg_orders
"#;
        let sources = parse_sources_yaml(yaml, Path::new("schema.yml")).unwrap();
        assert!(sources.is_empty());
    }

    #[test]
    fn test_sources_to_rocky_config_explicit() {
        let sources = vec![DbtSource {
            name: "stripe".to_string(),
            database: Some("raw_db".to_string()),
            schema: Some("stripe_data".to_string()),
            tables: vec![DbtSourceTable {
                name: "payments".to_string(),
                columns: vec![],
                freshness: None,
                loaded_at_field: None,
            }],
        }];

        let map = sources_to_rocky_config(&sources, "default_catalog");
        let key = ("stripe".to_string(), "payments".to_string());
        let mapping = map.get(&key).unwrap();
        assert_eq!(mapping.source_config.catalog, "raw_db");
        assert_eq!(mapping.source_config.schema, "stripe_data");
        assert_eq!(mapping.source_config.table, "payments");
    }

    #[test]
    fn test_sources_to_rocky_config_defaults() {
        let sources = vec![DbtSource {
            name: "stripe".to_string(),
            database: None,
            schema: None,
            tables: vec![DbtSourceTable {
                name: "payments".to_string(),
                columns: vec![],
                freshness: None,
                loaded_at_field: None,
            }],
        }];

        let map = sources_to_rocky_config(&sources, "my_catalog");
        let key = ("stripe".to_string(), "payments".to_string());
        let mapping = map.get(&key).unwrap();
        // database defaults to default_catalog
        assert_eq!(mapping.source_config.catalog, "my_catalog");
        // schema defaults to source name
        assert_eq!(mapping.source_config.schema, "stripe");
        assert_eq!(mapping.source_config.table, "payments");
    }

    #[test]
    fn test_multiple_sources() {
        let yaml = r#"
sources:
  - name: stripe
    database: raw
    schema: stripe
    tables:
      - name: payments
  - name: shopify
    database: raw
    schema: shopify
    tables:
      - name: orders
      - name: products
"#;
        let sources = parse_sources_yaml(yaml, Path::new("sources.yml")).unwrap();
        assert_eq!(sources.len(), 2);
        assert_eq!(sources[0].tables.len(), 1);
        assert_eq!(sources[1].tables.len(), 2);

        let map = sources_to_rocky_config(&sources, "raw");
        assert_eq!(map.len(), 3);
        assert!(map.contains_key(&("stripe".to_string(), "payments".to_string())));
        assert!(map.contains_key(&("shopify".to_string(), "orders".to_string())));
        assert!(map.contains_key(&("shopify".to_string(), "products".to_string())));
    }

    #[test]
    fn test_column_tests_parsed() {
        let yaml = r#"
sources:
  - name: raw
    tables:
      - name: users
        columns:
          - name: id
            tests:
              - unique
              - not_null
          - name: status
            tests:
              - accepted_values:
                  values: ["active", "inactive"]
"#;
        let sources = parse_sources_yaml(yaml, Path::new("src.yml")).unwrap();
        let users = &sources[0].tables[0];

        assert_eq!(users.columns[0].tests.len(), 2);
        assert_eq!(users.columns[0].tests[0].test_type, "unique");

        assert_eq!(users.columns[1].tests.len(), 1);
        assert_eq!(users.columns[1].tests[0].test_type, "accepted_values");
    }
}
