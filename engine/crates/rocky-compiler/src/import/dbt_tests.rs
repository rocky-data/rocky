//! dbt test YAML parser and Rocky contract conversion.
//!
//! Parses dbt `schema.yml` model test definitions (not_null, unique, accepted_values,
//! relationships, etc.) and converts them to Rocky `.contract.toml` files.

use std::collections::HashMap;
use std::path::Path;

use serde::Deserialize;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A dbt model definition extracted from a schema YAML file.
#[derive(Debug, Clone)]
pub struct DbtModelYaml {
    pub name: String,
    pub description: Option<String>,
    pub columns: Vec<DbtColumnYaml>,
}

/// A column definition within a dbt model YAML.
#[derive(Debug, Clone)]
pub struct DbtColumnYaml {
    pub name: String,
    pub description: Option<String>,
    pub tests: Vec<DbtTestDef>,
}

/// A dbt test definition: either a simple string or a configured test with parameters.
#[derive(Debug, Clone)]
pub enum DbtTestDef {
    /// Simple test like "unique" or "not_null".
    Simple(String),
    /// Configured test like `accepted_values` with parameters.
    Configured {
        name: String,
        config: HashMap<String, serde_yaml::Value>,
    },
}

/// A Rocky contract check produced from dbt test conversion.
#[derive(Debug, Clone)]
pub struct ContractCheck {
    pub check_type: String,
    pub columns: Vec<String>,
    pub config: HashMap<String, serde_yaml::Value>,
    pub custom_sql: Option<String>,
    pub description: Option<String>,
}

// ---------------------------------------------------------------------------
// Raw YAML deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RawModelFile {
    #[serde(default)]
    models: Option<Vec<RawModel>>,
}

#[derive(Deserialize)]
struct RawModel {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    columns: Vec<RawColumn>,
}

#[derive(Deserialize)]
struct RawColumn {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    tests: Option<Vec<serde_yaml::Value>>,
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

/// Scan a directory for YAML files containing model test definitions.
///
/// Any `.yml` or `.yaml` file with a top-level `models:` key is parsed.
/// Returns a map from model name to its YAML definition.
pub fn parse_model_yamls(models_dir: &Path) -> Result<HashMap<String, DbtModelYaml>, String> {
    let mut models = HashMap::new();

    if !models_dir.exists() {
        return Ok(models);
    }

    scan_models_recursive(models_dir, &mut models)?;
    Ok(models)
}

fn scan_models_recursive(
    dir: &Path,
    models: &mut HashMap<String, DbtModelYaml>,
) -> Result<(), String> {
    let entries =
        std::fs::read_dir(dir).map_err(|e| format!("failed to read {}: {e}", dir.display()))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.is_dir() {
            scan_models_recursive(&path, models)?;
        } else if is_yaml_file(&path) {
            match parse_model_yaml_file(&path) {
                Ok(parsed) => {
                    for model in parsed {
                        models.insert(model.name.clone(), model);
                    }
                }
                Err(_) => {
                    // Not a valid models file; skip silently.
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

fn parse_model_yaml_file(path: &Path) -> Result<Vec<DbtModelYaml>, String> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    parse_model_yaml_content(&content)
}

/// Parse model definitions from YAML content.
pub fn parse_model_yaml_content(content: &str) -> Result<Vec<DbtModelYaml>, String> {
    let raw: RawModelFile =
        serde_yaml::from_str(content).map_err(|e| format!("failed to parse YAML: {e}"))?;

    let Some(raw_models) = raw.models else {
        return Ok(Vec::new());
    };

    let mut models = Vec::new();
    for raw_model in raw_models {
        let columns = raw_model
            .columns
            .into_iter()
            .map(|c| {
                let tests = parse_column_tests(&c.tests);
                DbtColumnYaml {
                    name: c.name,
                    description: c.description,
                    tests,
                }
            })
            .collect();

        models.push(DbtModelYaml {
            name: raw_model.name,
            description: raw_model.description,
            columns,
        });
    }

    Ok(models)
}

fn parse_column_tests(tests: &Option<Vec<serde_yaml::Value>>) -> Vec<DbtTestDef> {
    let Some(tests) = tests else {
        return Vec::new();
    };

    tests
        .iter()
        .map(|t| match t {
            serde_yaml::Value::String(s) => DbtTestDef::Simple(s.clone()),
            serde_yaml::Value::Mapping(m) => {
                let name = m
                    .keys()
                    .next()
                    .and_then(|k| k.as_str())
                    .unwrap_or("unknown")
                    .to_string();

                let config = m
                    .values()
                    .next()
                    .and_then(|v| v.as_mapping())
                    .map(|m| {
                        m.iter()
                            .filter_map(|(k, v)| k.as_str().map(|ks| (ks.to_string(), v.clone())))
                            .collect()
                    })
                    .unwrap_or_default();

                DbtTestDef::Configured { name, config }
            }
            _ => DbtTestDef::Simple("unknown".to_string()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Test -> contract conversion
// ---------------------------------------------------------------------------

/// Convert dbt model tests to Rocky contract checks.
///
/// Mapping:
/// - `not_null` -> required column (batched)
/// - `unique` -> uniqueness contract (batched)
/// - `accepted_values` -> custom SQL check
/// - `relationships` -> custom SQL check (referential integrity)
/// - `dbt_utils.accepted_range` -> custom SQL range check
/// - `dbt_utils.unique_combination_of_columns` -> unique contract with multiple columns
/// - Other -> skipped with None return for that test
pub fn tests_to_contracts(model: &DbtModelYaml) -> (Vec<ContractCheck>, usize) {
    let mut checks = Vec::new();
    let mut not_null_cols = Vec::new();
    let mut unique_cols = Vec::new();
    let mut skipped = 0;

    for col in &model.columns {
        for test in &col.tests {
            match test {
                DbtTestDef::Simple(name) => match name.as_str() {
                    "not_null" => {
                        not_null_cols.push(col.name.clone());
                    }
                    "unique" => {
                        unique_cols.push(col.name.clone());
                    }
                    _ => {
                        skipped += 1;
                    }
                },
                DbtTestDef::Configured { name, config } => match name.as_str() {
                    "accepted_values" => {
                        if let Some(check) = convert_accepted_values(&model.name, &col.name, config)
                        {
                            checks.push(check);
                        } else {
                            skipped += 1;
                        }
                    }
                    "relationships" => {
                        if let Some(check) = convert_relationships(&model.name, &col.name, config) {
                            checks.push(check);
                        } else {
                            skipped += 1;
                        }
                    }
                    "dbt_utils.accepted_range" => {
                        if let Some(check) = convert_accepted_range(&model.name, &col.name, config)
                        {
                            checks.push(check);
                        } else {
                            skipped += 1;
                        }
                    }
                    "dbt_utils.unique_combination_of_columns" => {
                        if let Some(check) = convert_unique_combination(config) {
                            checks.push(check);
                        } else {
                            skipped += 1;
                        }
                    }
                    "dbt_utils.not_null_where" => {
                        if let Some(check) = convert_not_null_where(&model.name, &col.name, config)
                        {
                            checks.push(check);
                        } else {
                            skipped += 1;
                        }
                    }
                    _ => {
                        skipped += 1;
                    }
                },
            }
        }
    }

    // Batch not_null columns into a single check
    if !not_null_cols.is_empty() {
        checks.insert(
            0,
            ContractCheck {
                check_type: "not_null".to_string(),
                columns: not_null_cols,
                config: HashMap::new(),
                custom_sql: None,
                description: None,
            },
        );
    }

    // Batch unique columns: one check per unique column (they are independent constraints)
    for col_name in unique_cols {
        checks.push(ContractCheck {
            check_type: "unique".to_string(),
            columns: vec![col_name],
            config: HashMap::new(),
            custom_sql: None,
            description: None,
        });
    }

    (checks, skipped)
}

fn convert_accepted_values(
    model_name: &str,
    col_name: &str,
    config: &HashMap<String, serde_yaml::Value>,
) -> Option<ContractCheck> {
    let values = config.get("values")?;
    let values_list: Vec<String> = match values {
        serde_yaml::Value::Sequence(seq) => seq
            .iter()
            .filter_map(|v| match v {
                serde_yaml::Value::String(s) => Some(format!("'{s}'")),
                serde_yaml::Value::Number(n) => Some(n.to_string()),
                _ => None,
            })
            .collect(),
        _ => return None,
    };

    if values_list.is_empty() {
        return None;
    }

    let values_str = values_list.join(", ");
    let sql = format!("SELECT * FROM {model_name} WHERE {col_name} NOT IN ({values_str})");

    Some(ContractCheck {
        check_type: "custom_sql".to_string(),
        columns: vec![col_name.to_string()],
        config: config.clone(),
        custom_sql: Some(sql),
        description: Some(format!("{col_name} must be one of [{values_str}]")),
    })
}

fn convert_relationships(
    model_name: &str,
    col_name: &str,
    config: &HashMap<String, serde_yaml::Value>,
) -> Option<ContractCheck> {
    let to_raw = config.get("to")?.as_str()?;
    let field = config.get("field")?.as_str()?;

    // Extract model name from ref('model_name') or use raw value
    let ref_model = extract_ref_model(to_raw).unwrap_or(to_raw);

    let sql = format!(
        "SELECT * FROM {model_name} WHERE {col_name} NOT IN (SELECT {field} FROM {ref_model})"
    );

    Some(ContractCheck {
        check_type: "custom_sql".to_string(),
        columns: vec![col_name.to_string()],
        config: config.clone(),
        custom_sql: Some(sql),
        description: Some(format!("{col_name} references {ref_model}.{field}")),
    })
}

fn convert_accepted_range(
    model_name: &str,
    col_name: &str,
    config: &HashMap<String, serde_yaml::Value>,
) -> Option<ContractCheck> {
    let min_val = config.get("min_value");
    let max_val = config.get("max_value");

    if min_val.is_none() && max_val.is_none() {
        return None;
    }

    let mut conditions = Vec::new();
    let mut desc_parts = Vec::new();

    if let Some(min) = min_val {
        let min_str = yaml_value_to_string(min);
        conditions.push(format!("{col_name} < {min_str}"));
        desc_parts.push(format!(">= {min_str}"));
    }
    if let Some(max) = max_val {
        let max_str = yaml_value_to_string(max);
        conditions.push(format!("{col_name} > {max_str}"));
        desc_parts.push(format!("<= {max_str}"));
    }

    let where_clause = conditions.join(" OR ");
    let sql = format!("SELECT * FROM {model_name} WHERE {where_clause}");
    let description = format!("{col_name} must be {}", desc_parts.join(" and "));

    Some(ContractCheck {
        check_type: "custom_sql".to_string(),
        columns: vec![col_name.to_string()],
        config: config.clone(),
        custom_sql: Some(sql),
        description: Some(description),
    })
}

fn convert_unique_combination(
    config: &HashMap<String, serde_yaml::Value>,
) -> Option<ContractCheck> {
    let combination = config.get("combination_of_columns")?;
    let cols: Vec<String> = match combination {
        serde_yaml::Value::Sequence(seq) => seq
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        _ => return None,
    };

    if cols.is_empty() {
        return None;
    }

    Some(ContractCheck {
        check_type: "unique".to_string(),
        columns: cols,
        config: config.clone(),
        custom_sql: None,
        description: None,
    })
}

fn convert_not_null_where(
    model_name: &str,
    col_name: &str,
    config: &HashMap<String, serde_yaml::Value>,
) -> Option<ContractCheck> {
    let where_clause = config.get("where")?.as_str()?;

    let sql = format!("SELECT * FROM {model_name} WHERE {where_clause} AND {col_name} IS NULL");

    Some(ContractCheck {
        check_type: "custom_sql".to_string(),
        columns: vec![col_name.to_string()],
        config: config.clone(),
        custom_sql: Some(sql),
        description: Some(format!("{col_name} must not be null where {where_clause}")),
    })
}

fn extract_ref_model(raw: &str) -> Option<&str> {
    // Match ref('model') or ref("model")
    let trimmed = raw.trim();
    if trimmed.starts_with("ref(") && trimmed.ends_with(')') {
        let inner = &trimmed[4..trimmed.len() - 1].trim();
        // Strip quotes
        if (inner.starts_with('\'') && inner.ends_with('\''))
            || (inner.starts_with('"') && inner.ends_with('"'))
        {
            return Some(&inner[1..inner.len() - 1]);
        }
    }
    None
}

fn yaml_value_to_string(v: &serde_yaml::Value) -> String {
    match v {
        serde_yaml::Value::String(s) => s.clone(),
        serde_yaml::Value::Number(n) => n.to_string(),
        serde_yaml::Value::Bool(b) => b.to_string(),
        other => format!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Contract file writer
// ---------------------------------------------------------------------------

/// Write contract checks as a `.contract.toml` file.
///
/// Groups not_null columns together and writes each check as a `[[checks]]` entry.
pub fn write_contracts(
    model_name: &str,
    checks: &[ContractCheck],
    output_dir: &Path,
) -> Result<(), String> {
    if checks.is_empty() {
        return Ok(());
    }

    std::fs::create_dir_all(output_dir)
        .map_err(|e| format!("failed to create {}: {e}", output_dir.display()))?;

    let path = output_dir.join(format!("{model_name}.contract.toml"));
    let mut content = String::new();

    for check in checks {
        content.push_str("[[checks]]\n");
        content.push_str(&format!("type = \"{}\"\n", check.check_type));

        if !check.columns.is_empty() {
            let cols: Vec<String> = check.columns.iter().map(|c| format!("\"{c}\"")).collect();
            content.push_str(&format!("columns = [{}]\n", cols.join(", ")));
        }

        if let Some(ref desc) = check.description {
            content.push_str(&format!("description = \"{desc}\"\n"));
        }

        if let Some(ref sql) = check.custom_sql {
            content.push_str(&format!("sql = \"{}\"\n", sql.replace('"', "\\\"")));
        }

        content.push('\n');
    }

    std::fs::write(&path, content)
        .map_err(|e| format!("failed to write {}: {e}", path.display()))?;

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_standard_model_yaml() {
        let yaml = r#"
models:
  - name: fct_orders
    description: Completed customer orders
    columns:
      - name: order_id
        description: Primary key
        tests:
          - unique
          - not_null
      - name: amount
        tests:
          - not_null
      - name: status
        tests:
          - accepted_values:
              values: ['completed', 'pending', 'cancelled']
"#;
        let models = parse_model_yaml_content(yaml).unwrap();
        assert_eq!(models.len(), 1);

        let fct = &models[0];
        assert_eq!(fct.name, "fct_orders");
        assert_eq!(
            fct.description.as_deref(),
            Some("Completed customer orders")
        );
        assert_eq!(fct.columns.len(), 3);

        // order_id has unique + not_null
        assert_eq!(fct.columns[0].name, "order_id");
        assert_eq!(fct.columns[0].tests.len(), 2);
        assert!(matches!(&fct.columns[0].tests[0], DbtTestDef::Simple(s) if s == "unique"));
        assert!(matches!(&fct.columns[0].tests[1], DbtTestDef::Simple(s) if s == "not_null"));

        // amount has not_null
        assert_eq!(fct.columns[1].tests.len(), 1);

        // status has accepted_values (configured)
        assert_eq!(fct.columns[2].tests.len(), 1);
        assert!(
            matches!(&fct.columns[2].tests[0], DbtTestDef::Configured { name, .. } if name == "accepted_values")
        );
    }

    #[test]
    fn test_parse_relationships_test() {
        let yaml = r#"
models:
  - name: fct_orders
    columns:
      - name: customer_id
        tests:
          - relationships:
              to: ref('dim_customers')
              field: customer_id
"#;
        let models = parse_model_yaml_content(yaml).unwrap();
        let col = &models[0].columns[0];
        assert_eq!(col.tests.len(), 1);
        match &col.tests[0] {
            DbtTestDef::Configured { name, config } => {
                assert_eq!(name, "relationships");
                assert!(config.contains_key("to"));
                assert!(config.contains_key("field"));
            }
            _ => panic!("expected Configured test"),
        }
    }

    #[test]
    fn test_parse_model_without_tests() {
        let yaml = r#"
models:
  - name: dim_date
    columns:
      - name: date_key
      - name: year
"#;
        let models = parse_model_yaml_content(yaml).unwrap();
        assert_eq!(models[0].columns[0].tests.len(), 0);
        assert_eq!(models[0].columns[1].tests.len(), 0);
    }

    #[test]
    fn test_parse_no_models_key() {
        let yaml = r#"
version: 2
sources:
  - name: raw
"#;
        let models = parse_model_yaml_content(yaml).unwrap();
        assert!(models.is_empty());
    }

    #[test]
    fn test_parse_multiple_models() {
        let yaml = r#"
models:
  - name: stg_orders
    columns:
      - name: id
        tests:
          - unique
  - name: stg_customers
    columns:
      - name: id
        tests:
          - unique
          - not_null
"#;
        let models = parse_model_yaml_content(yaml).unwrap();
        assert_eq!(models.len(), 2);
        assert_eq!(models[0].name, "stg_orders");
        assert_eq!(models[1].name, "stg_customers");
    }

    // --- Contract conversion tests ---

    #[test]
    fn test_not_null_to_contract() {
        let model = DbtModelYaml {
            name: "fct_orders".to_string(),
            description: None,
            columns: vec![
                DbtColumnYaml {
                    name: "order_id".to_string(),
                    description: None,
                    tests: vec![DbtTestDef::Simple("not_null".to_string())],
                },
                DbtColumnYaml {
                    name: "amount".to_string(),
                    description: None,
                    tests: vec![DbtTestDef::Simple("not_null".to_string())],
                },
            ],
        };

        let (checks, skipped) = tests_to_contracts(&model);
        assert_eq!(skipped, 0);
        // Should batch not_null columns together
        let not_null = checks.iter().find(|c| c.check_type == "not_null").unwrap();
        assert_eq!(not_null.columns, vec!["order_id", "amount"]);
    }

    #[test]
    fn test_unique_to_contract() {
        let model = DbtModelYaml {
            name: "fct_orders".to_string(),
            description: None,
            columns: vec![DbtColumnYaml {
                name: "order_id".to_string(),
                description: None,
                tests: vec![DbtTestDef::Simple("unique".to_string())],
            }],
        };

        let (checks, skipped) = tests_to_contracts(&model);
        assert_eq!(skipped, 0);
        let unique = checks.iter().find(|c| c.check_type == "unique").unwrap();
        assert_eq!(unique.columns, vec!["order_id"]);
    }

    #[test]
    fn test_accepted_values_to_contract() {
        let mut config = HashMap::new();
        config.insert(
            "values".to_string(),
            serde_yaml::Value::Sequence(vec![
                serde_yaml::Value::String("completed".to_string()),
                serde_yaml::Value::String("pending".to_string()),
            ]),
        );

        let model = DbtModelYaml {
            name: "fct_orders".to_string(),
            description: None,
            columns: vec![DbtColumnYaml {
                name: "status".to_string(),
                description: None,
                tests: vec![DbtTestDef::Configured {
                    name: "accepted_values".to_string(),
                    config,
                }],
            }],
        };

        let (checks, _) = tests_to_contracts(&model);
        let custom = checks
            .iter()
            .find(|c| c.check_type == "custom_sql")
            .unwrap();
        assert!(custom.custom_sql.as_ref().unwrap().contains("NOT IN"));
        assert!(
            custom
                .custom_sql
                .as_ref()
                .unwrap()
                .contains("'completed', 'pending'")
        );
    }

    #[test]
    fn test_relationships_to_contract() {
        let mut config = HashMap::new();
        config.insert(
            "to".to_string(),
            serde_yaml::Value::String("ref('dim_customers')".to_string()),
        );
        config.insert(
            "field".to_string(),
            serde_yaml::Value::String("customer_id".to_string()),
        );

        let model = DbtModelYaml {
            name: "fct_orders".to_string(),
            description: None,
            columns: vec![DbtColumnYaml {
                name: "customer_id".to_string(),
                description: None,
                tests: vec![DbtTestDef::Configured {
                    name: "relationships".to_string(),
                    config,
                }],
            }],
        };

        let (checks, _) = tests_to_contracts(&model);
        let custom = checks
            .iter()
            .find(|c| c.check_type == "custom_sql")
            .unwrap();
        let sql = custom.custom_sql.as_ref().unwrap();
        assert!(sql.contains("customer_id NOT IN (SELECT customer_id FROM dim_customers)"));
    }

    #[test]
    fn test_accepted_range_to_contract() {
        let mut config = HashMap::new();
        config.insert(
            "min_value".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(0)),
        );
        config.insert(
            "max_value".to_string(),
            serde_yaml::Value::Number(serde_yaml::Number::from(100000)),
        );

        let model = DbtModelYaml {
            name: "fct_orders".to_string(),
            description: None,
            columns: vec![DbtColumnYaml {
                name: "amount".to_string(),
                description: None,
                tests: vec![DbtTestDef::Configured {
                    name: "dbt_utils.accepted_range".to_string(),
                    config,
                }],
            }],
        };

        let (checks, _) = tests_to_contracts(&model);
        let custom = checks
            .iter()
            .find(|c| c.check_type == "custom_sql")
            .unwrap();
        let sql = custom.custom_sql.as_ref().unwrap();
        assert!(sql.contains("amount < 0"));
        assert!(sql.contains("amount > 100000"));
    }

    #[test]
    fn test_unknown_test_skipped() {
        let model = DbtModelYaml {
            name: "model".to_string(),
            description: None,
            columns: vec![DbtColumnYaml {
                name: "col".to_string(),
                description: None,
                tests: vec![DbtTestDef::Simple("some_custom_test".to_string())],
            }],
        };

        let (checks, skipped) = tests_to_contracts(&model);
        assert_eq!(skipped, 1);
        assert!(checks.is_empty());
    }

    #[test]
    fn test_mixed_tests_grouped() {
        let mut av_config = HashMap::new();
        av_config.insert(
            "values".to_string(),
            serde_yaml::Value::Sequence(vec![serde_yaml::Value::String("active".to_string())]),
        );

        let model = DbtModelYaml {
            name: "users".to_string(),
            description: None,
            columns: vec![
                DbtColumnYaml {
                    name: "id".to_string(),
                    description: None,
                    tests: vec![
                        DbtTestDef::Simple("unique".to_string()),
                        DbtTestDef::Simple("not_null".to_string()),
                    ],
                },
                DbtColumnYaml {
                    name: "email".to_string(),
                    description: None,
                    tests: vec![DbtTestDef::Simple("not_null".to_string())],
                },
                DbtColumnYaml {
                    name: "status".to_string(),
                    description: None,
                    tests: vec![DbtTestDef::Configured {
                        name: "accepted_values".to_string(),
                        config: av_config,
                    }],
                },
            ],
        };

        let (checks, _) = tests_to_contracts(&model);

        // not_null should be batched with both columns
        let not_null = checks.iter().find(|c| c.check_type == "not_null").unwrap();
        assert_eq!(not_null.columns, vec!["id", "email"]);

        // unique for id
        let unique = checks.iter().find(|c| c.check_type == "unique").unwrap();
        assert_eq!(unique.columns, vec!["id"]);

        // custom_sql for accepted_values
        assert!(checks.iter().any(|c| c.check_type == "custom_sql"));
    }

    // --- Contract file writer tests ---

    #[test]
    fn test_write_contracts_creates_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let checks = vec![
            ContractCheck {
                check_type: "not_null".to_string(),
                columns: vec!["order_id".to_string(), "amount".to_string()],
                config: HashMap::new(),
                custom_sql: None,
                description: None,
            },
            ContractCheck {
                check_type: "unique".to_string(),
                columns: vec!["order_id".to_string()],
                config: HashMap::new(),
                custom_sql: None,
                description: None,
            },
        ];

        write_contracts("fct_orders", &checks, dir.path()).unwrap();

        let path = dir.path().join("fct_orders.contract.toml");
        assert!(path.exists());

        let content = std::fs::read_to_string(&path).unwrap();
        assert!(content.contains("type = \"not_null\""));
        assert!(content.contains("type = \"unique\""));
        assert!(content.contains("\"order_id\", \"amount\""));
    }

    #[test]
    fn test_write_contracts_with_custom_sql() {
        let dir = tempfile::TempDir::new().unwrap();
        let checks = vec![ContractCheck {
            check_type: "custom_sql".to_string(),
            columns: vec!["status".to_string()],
            config: HashMap::new(),
            custom_sql: Some("SELECT * FROM orders WHERE status NOT IN ('completed')".to_string()),
            description: Some("status must be completed".to_string()),
        }];

        write_contracts("orders", &checks, dir.path()).unwrap();

        let content = std::fs::read_to_string(dir.path().join("orders.contract.toml")).unwrap();
        assert!(content.contains("type = \"custom_sql\""));
        assert!(content.contains("description = \"status must be completed\""));
        assert!(content.contains("sql = "));
    }

    #[test]
    fn test_write_contracts_empty_skipped() {
        let dir = tempfile::TempDir::new().unwrap();
        write_contracts("empty_model", &[], dir.path()).unwrap();
        // No file created for empty checks
        let path = dir.path().join("empty_model.contract.toml");
        assert!(!path.exists());
    }

    #[test]
    fn test_extract_ref_model_single_quotes() {
        assert_eq!(
            extract_ref_model("ref('dim_customers')"),
            Some("dim_customers")
        );
    }

    #[test]
    fn test_extract_ref_model_double_quotes() {
        assert_eq!(
            extract_ref_model("ref(\"dim_customers\")"),
            Some("dim_customers")
        );
    }

    #[test]
    fn test_extract_ref_model_not_ref() {
        assert_eq!(extract_ref_model("dim_customers"), None);
    }

    #[test]
    fn test_scan_model_yamls_from_dir() {
        let dir = tempfile::TempDir::new().unwrap();
        let yaml = r#"
models:
  - name: stg_orders
    columns:
      - name: id
        tests:
          - unique
"#;
        std::fs::write(dir.path().join("_models.yml"), yaml).unwrap();

        let models = parse_model_yamls(dir.path()).unwrap();
        assert_eq!(models.len(), 1);
        assert!(models.contains_key("stg_orders"));
    }
}
