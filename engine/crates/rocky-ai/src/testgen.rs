//! Generate test assertions from model intent.
//!
//! Uses the LLM to analyze a model's intent and schema to produce SQL
//! assertions that verify the model satisfies its business requirements.

use rocky_compiler::compile::CompileResult;
use rocky_core::models::Model;
use serde::{Deserialize, Serialize};

use crate::client::{AiError, LlmClient};

/// A generated test assertion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestAssertion {
    pub name: String,
    pub sql: String,
    pub description: String,
}

/// Generate test assertions for a model based on its intent and schema.
pub async fn generate_tests(
    model: &Model,
    compile_result: &CompileResult,
    client: &LlmClient,
) -> Result<Vec<TestAssertion>, AiError> {
    let model_name = &model.config.name;
    let intent = model.config.intent.as_deref().unwrap_or("");

    // Build column context
    let mut columns_desc = String::new();
    if let Some(cols) = compile_result.type_check.typed_models.get(model_name) {
        for col in cols {
            let nullable = if col.nullable {
                " (nullable)"
            } else {
                " (not null)"
            };
            columns_desc.push_str(&format!(
                "  - {}: {:?}{}\n",
                col.name, col.data_type, nullable
            ));
        }
    }

    // Build target table reference
    let target = &model.config.target;
    let full_table = format!("{}.{}.{}", target.catalog, target.schema, target.table);

    let system = "You are a data quality engineer generating test assertions for SQL models.\n\
        \n\
        Rules:\n\
        - Each test is a SQL query that returns 0 rows when the assertion holds\n\
        - Cover: not-null constraints, grain uniqueness, value range expectations, referential integrity\n\
        - Use the full table name provided\n\
        - Return tests as a JSON array of objects with fields: name, sql, description\n\
        - Return ONLY the JSON array, no markdown fences, no explanation";

    let user = if intent.is_empty() {
        format!(
            "Model: {model_name}\nTable: {full_table}\n\n\
             Source code:\n```\n{}\n```\n\n\
             Columns:\n{columns_desc}\n\
             Generate test assertions based on the code.",
            model.sql
        )
    } else {
        format!(
            "Model: {model_name}\nTable: {full_table}\n\
             Intent: {intent}\n\n\
             Source code:\n```\n{}\n```\n\n\
             Columns:\n{columns_desc}\n\
             Generate test assertions that verify this model satisfies its intent.",
            model.sql
        )
    };

    let response = client.generate(system, &user, None).await?;

    // Parse JSON response
    let content = response.content.trim();
    // Strip markdown fences if present
    let json_str = if content.starts_with("```") {
        content
            .lines()
            .skip(1)
            .take_while(|l| !l.starts_with("```"))
            .collect::<Vec<&str>>()
            .join("\n")
    } else {
        content.to_string()
    };

    let assertions: Vec<TestAssertion> =
        serde_json::from_str(&json_str).map_err(|e| AiError::Api {
            message: format!("Failed to parse test assertions: {e}"),
        })?;

    Ok(assertions)
}

/// Validate an assertion / model name component before it's interpolated
/// into a filesystem path.
///
/// The LLM controls `assertion.name` end-to-end — left unvalidated, model
/// output can write `tests/../../etc/passwd.sql` via path traversal.
/// `model_name` flows from project config rather than the LLM, but the
/// same allow-list applies for defence-in-depth.
///
/// Allow-list: ASCII alphanumeric, `_`, and `-`. Rejects `..`, `.`, `/`,
/// `\\`, leading dots, empty strings, and any non-ASCII / control chars.
fn is_safe_path_component(name: &str) -> bool {
    !name.is_empty()
        && !name.starts_with('.')
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// Save test assertions to SQL files in a tests directory.
///
/// Validates `assertion.name` against [`is_safe_path_component`] before
/// constructing the output path to prevent the LLM from steering writes
/// outside `tests_dir` via path traversal (`../../etc/passwd`,
/// absolute paths, etc.). Rejected names produce a
/// [`std::io::ErrorKind::InvalidInput`] error.
pub fn save_tests(
    model_name: &str,
    assertions: &[TestAssertion],
    tests_dir: &std::path::Path,
) -> Result<(), std::io::Error> {
    if !is_safe_path_component(model_name) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "refusing to save tests: model name {model_name:?} contains \
                 disallowed characters (allow-list: alphanumeric, _, -)"
            ),
        ));
    }

    std::fs::create_dir_all(tests_dir)?;

    for assertion in assertions {
        // The previous behaviour silently lower-cased + space-collapsed the
        // raw `assertion.name` into the filename. That is unsafe when the
        // string is LLM-controlled — `..` and `/` slip straight through.
        // Validate the raw name against a strict allow-list and reject
        // anything that doesn't match.
        if !is_safe_path_component(&assertion.name) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "refusing to save test {model_name:?}/{:?}: \
                     assertion name contains disallowed characters \
                     (allow-list: alphanumeric, _, -). \
                     This guards against LLM-driven path traversal.",
                    assertion.name
                ),
            ));
        }

        let file_name = format!("{}_{}.sql", model_name, assertion.name.to_lowercase());
        let path = tests_dir.join(&file_name);

        let content = format!(
            "-- Test: {}\n-- {}\n{}\n",
            assertion.name, assertion.description, assertion.sql
        );

        std::fs::write(path, content)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assertion(name: &str) -> TestAssertion {
        TestAssertion {
            name: name.to_string(),
            sql: "SELECT 1 WHERE 0".to_string(),
            description: "stub".to_string(),
        }
    }

    fn unique_dir(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let pid = std::process::id();
        let dir = std::env::temp_dir().join(format!("rocky-ai-testgen-{label}-{pid}-{nanos}"));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn safe_path_component_accepts_allow_list() {
        assert!(is_safe_path_component("grain_uniqueness"));
        assert!(is_safe_path_component("revenue-positive"));
        assert!(is_safe_path_component("test123"));
    }

    #[test]
    fn safe_path_component_rejects_traversal_and_separators() {
        assert!(!is_safe_path_component(""));
        assert!(!is_safe_path_component("."));
        assert!(!is_safe_path_component(".."));
        assert!(!is_safe_path_component("../etc/passwd"));
        assert!(!is_safe_path_component("..\\windows"));
        assert!(!is_safe_path_component("foo/bar"));
        assert!(!is_safe_path_component("foo\\bar"));
        assert!(!is_safe_path_component(".hidden"));
        assert!(!is_safe_path_component("name with space"));
        assert!(!is_safe_path_component("name\0null"));
        // Absolute Unix path
        assert!(!is_safe_path_component("/etc/passwd"));
    }

    /// Issue 6b — direct path traversal regression test. The LLM-controlled
    /// `assertion.name` must never reach `Path::join` unvalidated.
    #[test]
    fn save_tests_rejects_path_traversal_in_assertion_name() {
        let dir = unique_dir("traversal");
        let assertions = vec![assertion("../../../etc/passwd")];

        let result = save_tests("model", &assertions, &dir);

        let err = result.expect_err("path traversal must be rejected");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(
            err.to_string().contains("disallowed characters"),
            "unexpected error message: {err}"
        );

        // The escaped target must NOT exist on disk.
        assert!(
            !std::path::Path::new("/etc/passwd_../../../etc/passwd.sql").exists(),
            "writing to /etc/passwd via traversal would have succeeded"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_tests_rejects_absolute_path_in_assertion_name() {
        let dir = unique_dir("absolute");
        let assertions = vec![assertion("/tmp/evil")];

        let result = save_tests("model", &assertions, &dir);

        assert!(result.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_tests_rejects_dotted_name() {
        let dir = unique_dir("dotted");
        let assertions = vec![assertion(".hidden")];

        assert!(save_tests("model", &assertions, &dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_tests_rejects_unsafe_model_name() {
        let dir = unique_dir("unsafe-model");
        let assertions = vec![assertion("ok_name")];

        assert!(save_tests("../escaped", &assertions, &dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn save_tests_writes_safe_assertion() {
        let dir = unique_dir("happy");
        let assertions = vec![assertion("Grain_Uniqueness")];

        save_tests("orders", &assertions, &dir).expect("safe write should succeed");

        let expected = dir.join("orders_grain_uniqueness.sql");
        assert!(
            expected.exists(),
            "expected file to be written at {}",
            expected.display()
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
