use regex::Regex;
use std::sync::LazyLock;
use thiserror::Error;

/// Pattern for valid SQL identifiers (catalogs, schemas, tables, clients, etc.)
static SQL_IDENTIFIER_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_]+$").unwrap());

/// Pattern for valid principal names (for GRANT/REVOKE statements)
static PRINCIPAL_NAME_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-zA-Z0-9_ \-\.@]+$").unwrap());

/// Pattern for valid GCP project IDs.
///
/// GCP requires project IDs to be 6–30 chars, lowercase alphanumeric +
/// hyphens, starting with a letter and not ending in a hyphen. The
/// hyphen is what makes the stricter [`SQL_IDENTIFIER_RE`] reject them
/// (e.g. `rocky-sandbox-hc-test-63874`).
static GCP_PROJECT_ID_RE: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z][a-z0-9\-]{4,28}[a-z0-9]$").unwrap());

/// Errors from SQL identifier and principal name validation.
#[derive(Debug, Error)]
pub enum ValidationError {
    #[error("invalid SQL identifier '{value}': must match [a-zA-Z0-9_]+")]
    InvalidIdentifier { value: String },

    #[error("invalid principal name '{value}': must match [a-zA-Z0-9_ \\-\\.@]+")]
    InvalidPrincipal { value: String },

    #[error(
        "invalid GCP project ID '{value}': must be 6-30 chars, start with a letter, \
         contain only lowercase letters / digits / hyphens, and not end with a hyphen"
    )]
    InvalidGcpProjectId { value: String },

    #[error("SQL identifier cannot be empty")]
    EmptyIdentifier,

    #[error("principal name cannot be empty")]
    EmptyPrincipal,
}

/// Validates a SQL identifier (catalog, schema, table, column names).
///
/// Rejects anything that doesn't match `^[a-zA-Z0-9_]+$`.
/// Never use `format!()` to build SQL with untrusted input — validate first.
pub fn validate_identifier(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyIdentifier);
    }
    if !SQL_IDENTIFIER_RE.is_match(value) {
        return Err(ValidationError::InvalidIdentifier {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Validates a GCP project ID for safe interpolation into BigQuery SQL.
///
/// GCP project IDs allow hyphens (`my-project-id-123`), which the
/// stricter [`validate_identifier`] would reject. Use this for the
/// catalog/project component on the BigQuery adapter; keep
/// [`validate_identifier`] for dataset and table names, which still
/// must match `[a-zA-Z0-9_]+`.
pub fn validate_gcp_project_id(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyIdentifier);
    }
    if !GCP_PROJECT_ID_RE.is_match(value) {
        return Err(ValidationError::InvalidGcpProjectId {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Validates a principal name for use in GRANT/REVOKE statements.
///
/// Allows alphanumeric, underscores, spaces, hyphens, dots, and @.
/// Principal names should always be wrapped in backticks in SQL.
pub fn validate_principal(value: &str) -> Result<&str, ValidationError> {
    if value.is_empty() {
        return Err(ValidationError::EmptyPrincipal);
    }
    if !PRINCIPAL_NAME_RE.is_match(value) {
        return Err(ValidationError::InvalidPrincipal {
            value: value.to_string(),
        });
    }
    Ok(value)
}

/// Formats a validated identifier for use in SQL (no quoting needed for valid identifiers).
pub fn format_table_ref(
    catalog: &str,
    schema: &str,
    table: &str,
) -> Result<String, ValidationError> {
    validate_identifier(catalog)?;
    validate_identifier(schema)?;
    validate_identifier(table)?;
    Ok(format!("{catalog}.{schema}.{table}"))
}

/// Formats a principal name wrapped in backticks for SQL GRANT/REVOKE.
pub fn format_principal(name: &str) -> Result<String, ValidationError> {
    validate_principal(name)?;
    Ok(format!("`{name}`"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifiers() {
        assert!(validate_identifier("my_table").is_ok());
        assert!(validate_identifier("CamelCase").is_ok());
        assert!(validate_identifier("table123").is_ok());
        assert!(validate_identifier("_leading_underscore").is_ok());
        assert!(validate_identifier("ALL_CAPS_123").is_ok());
    }

    #[test]
    fn test_invalid_identifiers() {
        assert!(validate_identifier("").is_err());
        assert!(validate_identifier("has space").is_err());
        assert!(validate_identifier("has-dash").is_err());
        assert!(validate_identifier("has.dot").is_err());
        assert!(validate_identifier("has;semicolon").is_err());
        assert!(validate_identifier("DROP TABLE users--").is_err());
        assert!(validate_identifier("'; DROP TABLE users; --").is_err());
        assert!(validate_identifier("table\nname").is_err());
    }

    #[test]
    fn test_valid_principals() {
        assert!(validate_principal("user@domain.com").is_ok());
        assert!(validate_principal("my-service-principal").is_ok());
        assert!(validate_principal("Data Engineers").is_ok());
        assert!(validate_principal("user_name").is_ok());
        assert!(validate_principal("group.name").is_ok());
    }

    #[test]
    fn test_invalid_principals() {
        assert!(validate_principal("").is_err());
        assert!(validate_principal("user;DROP TABLE").is_err());
        assert!(validate_principal("user`backtick").is_err());
        assert!(validate_principal("user'quote").is_err());
        assert!(validate_principal("user\nnewline").is_err());
    }

    #[test]
    fn test_format_table_ref() {
        assert_eq!(
            format_table_ref("my_catalog", "my_schema", "my_table").unwrap(),
            "my_catalog.my_schema.my_table"
        );
    }

    #[test]
    fn test_format_table_ref_rejects_injection() {
        assert!(format_table_ref("catalog; DROP TABLE", "schema", "table").is_err());
    }

    #[test]
    fn test_valid_gcp_project_ids() {
        assert!(validate_gcp_project_id("rocky-sandbox").is_ok());
        assert!(validate_gcp_project_id("rocky-sandbox-hc-test-63874").is_ok());
        assert!(validate_gcp_project_id("my-project-1").is_ok());
        // Lower bound: 6 chars total (1 leading letter + 4 middle + 1 tail).
        assert!(validate_gcp_project_id("abc12d").is_ok());
    }

    #[test]
    fn test_invalid_gcp_project_ids() {
        // Empty / too short.
        assert!(validate_gcp_project_id("").is_err());
        assert!(validate_gcp_project_id("abc12").is_err());
        // Must start with a letter.
        assert!(validate_gcp_project_id("1-project").is_err());
        assert!(validate_gcp_project_id("-project").is_err());
        // Cannot end with a hyphen.
        assert!(validate_gcp_project_id("rocky-sandbox-").is_err());
        // No uppercase, dots, underscores, spaces.
        assert!(validate_gcp_project_id("Rocky-Sandbox").is_err());
        assert!(validate_gcp_project_id("rocky.sandbox").is_err());
        assert!(validate_gcp_project_id("rocky_sandbox").is_err());
        assert!(validate_gcp_project_id("rocky sandbox").is_err());
        // Injection attempts.
        assert!(validate_gcp_project_id("'; DROP TABLE users; --").is_err());
        assert!(validate_gcp_project_id("project`backtick").is_err());
    }

    #[test]
    fn test_format_principal() {
        assert_eq!(
            format_principal("Data Engineers").unwrap(),
            "`Data Engineers`"
        );
        assert_eq!(
            format_principal("user@domain.com").unwrap(),
            "`user@domain.com`"
        );
    }
}
