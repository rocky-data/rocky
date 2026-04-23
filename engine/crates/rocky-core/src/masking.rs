//! Column-masking SQL generation.
//!
//! Rocky's masking layer composes a workspace-default `[mask]` block with
//! an optional per-env `[mask.<env>]` override to resolve each
//! classification tag into a [`MaskStrategy`]. At apply time, the adapter
//! renders each strategy as a warehouse-native function: Databricks uses
//! `CREATE OR REPLACE MASK ... RETURN <expr>` + `ALTER TABLE ... ALTER
//! COLUMN ... SET MASK`, gated behind Unity Catalog.
//!
//! The SQL emitted here is Databricks-flavored but kept dialect-neutral
//! where possible — identifier quoting, function naming, and the actual
//! expression per strategy are hardcoded to Databricks/Spark SQL because
//! that's the only adapter that implements
//! [`GovernanceAdapter::apply_masking_policy`] in v1.
//!
//! [`GovernanceAdapter::apply_masking_policy`]: crate::traits::GovernanceAdapter::apply_masking_policy
//! [`MaskStrategy`]: crate::traits::MaskStrategy

use rocky_sql::validation::{self, ValidationError};
use thiserror::Error;

use crate::traits::MaskStrategy;

/// Errors from masking-policy SQL generation.
#[derive(Debug, Error)]
pub enum MaskingError {
    #[error("validation error: {0}")]
    Validation(#[from] ValidationError),

    #[error("unsupported masking strategy for SQL generation: {0}")]
    UnsupportedStrategy(&'static str),
}

/// Canonical name for the Unity Catalog masking function backing a given
/// strategy in a given environment.
///
/// Policy functions live at the schema level and are reused across every
/// table that masks a column with the same strategy. Rocky namespaces them
/// by environment so a `prod` override doesn't stomp a `dev` default:
///
/// ```text
/// rocky_mask_<strategy>_<env>
/// ```
///
/// e.g., `rocky_mask_hash_prod`, `rocky_mask_partial_dev`. Env names are
/// validated as identifiers upstream (by the config layer); this function
/// assumes both inputs are safe identifiers.
pub fn masking_function_name(strategy: MaskStrategy, env: &str) -> String {
    format!("rocky_mask_{}_{env}", strategy.as_str())
}

/// Generates `CREATE OR REPLACE FUNCTION <catalog>.<schema>.rocky_mask_<strategy>_<env>(v STRING) RETURN <expr>`.
///
/// The function is idempotent (CREATE OR REPLACE) so rerunning `rocky run`
/// against the same environment is a no-op. Returns `Ok(None)` for
/// [`MaskStrategy::None`] because "explicit identity" doesn't need a
/// warehouse-side function.
///
/// The function body is pinned to STRING input/output — row-level masking
/// in Databricks only applies to string-typed columns in v1. Non-string
/// columns are rejected at plan time by the compiler rather than silently
/// cast here.
pub fn generate_create_mask_sql(
    catalog: &str,
    schema: &str,
    strategy: MaskStrategy,
    env: &str,
) -> Result<Option<String>, MaskingError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(env)?;
    let name = masking_function_name(strategy, env);

    let body = match strategy {
        MaskStrategy::Hash => "sha2(v, 256)".to_string(),
        MaskStrategy::Redact => "'***'".to_string(),
        MaskStrategy::Partial => partial_mask_expr(),
        MaskStrategy::None => return Ok(None),
    };

    Ok(Some(format!(
        "CREATE OR REPLACE FUNCTION {catalog}.{schema}.{name}(v STRING) RETURNS STRING RETURN {body}"
    )))
}

/// Generates `ALTER TABLE <catalog>.<schema>.<table> ALTER COLUMN <column> SET MASK <catalog>.<schema>.<name>`.
///
/// Binds an existing masking function to a column. The column expression
/// is reset by Databricks on `SET MASK` — callers don't need to `DROP
/// MASK` first.
///
/// Returns `Ok(None)` when `strategy` is [`MaskStrategy::None`] — the
/// explicit-identity case. Rocky doesn't emit a no-op `SET MASK` for
/// `None`; if the column previously had a mask, Rocky drops it in a
/// separate pass (see [`generate_drop_mask_sql`]).
pub fn generate_set_mask_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    column: &str,
    strategy: MaskStrategy,
    env: &str,
) -> Result<Option<String>, MaskingError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(table)?;
    validation::validate_identifier(column)?;
    validation::validate_identifier(env)?;

    if strategy == MaskStrategy::None {
        return Ok(None);
    }

    let fn_name = masking_function_name(strategy, env);
    Ok(Some(format!(
        "ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} SET MASK {catalog}.{schema}.{fn_name}"
    )))
}

/// Generates `ALTER TABLE <catalog>.<schema>.<table> ALTER COLUMN <column> DROP MASK`.
///
/// Called when `[mask.<env>]` overrides a classification to
/// [`MaskStrategy::None`] after the column was previously masked.
pub fn generate_drop_mask_sql(
    catalog: &str,
    schema: &str,
    table: &str,
    column: &str,
) -> Result<String, MaskingError> {
    validation::validate_identifier(catalog)?;
    validation::validate_identifier(schema)?;
    validation::validate_identifier(table)?;
    validation::validate_identifier(column)?;
    Ok(format!(
        "ALTER TABLE {catalog}.{schema}.{table} ALTER COLUMN {column} DROP MASK"
    ))
}

/// SQL expression implementing the [`MaskStrategy::Partial`] strategy.
///
/// Keeps the first two and last two characters, replaces the middle with
/// `***`. Values shorter than 5 chars fall through to full redaction so
/// short strings aren't effectively unmasked.
fn partial_mask_expr() -> String {
    "CASE WHEN v IS NULL THEN NULL \
       WHEN length(v) < 5 THEN '***' \
       ELSE concat(substring(v, 1, 2), '***', substring(v, length(v) - 1, 2)) \
     END"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_masking_function_name() {
        assert_eq!(
            masking_function_name(MaskStrategy::Hash, "prod"),
            "rocky_mask_hash_prod"
        );
        assert_eq!(
            masking_function_name(MaskStrategy::Redact, "dev"),
            "rocky_mask_redact_dev"
        );
        assert_eq!(
            masking_function_name(MaskStrategy::Partial, "stage"),
            "rocky_mask_partial_stage"
        );
    }

    #[test]
    fn test_create_mask_hash() {
        let sql = generate_create_mask_sql("cat", "sch", MaskStrategy::Hash, "prod")
            .unwrap()
            .expect("Hash has SQL");
        assert_eq!(
            sql,
            "CREATE OR REPLACE FUNCTION cat.sch.rocky_mask_hash_prod(v STRING) RETURNS STRING RETURN sha2(v, 256)"
        );
    }

    #[test]
    fn test_create_mask_redact() {
        let sql = generate_create_mask_sql("cat", "sch", MaskStrategy::Redact, "dev")
            .unwrap()
            .expect("Redact has SQL");
        assert_eq!(
            sql,
            "CREATE OR REPLACE FUNCTION cat.sch.rocky_mask_redact_dev(v STRING) RETURNS STRING RETURN '***'"
        );
    }

    #[test]
    fn test_create_mask_partial() {
        let sql = generate_create_mask_sql("cat", "sch", MaskStrategy::Partial, "prod")
            .unwrap()
            .expect("Partial has SQL");
        assert!(sql.starts_with(
            "CREATE OR REPLACE FUNCTION cat.sch.rocky_mask_partial_prod(v STRING) RETURNS STRING RETURN "
        ));
        // Body should be a CASE covering NULL + short-string short-circuits.
        assert!(sql.contains("CASE WHEN v IS NULL THEN NULL"));
        assert!(sql.contains("length(v) < 5"));
        assert!(sql.contains("concat(substring(v, 1, 2)"));
    }

    #[test]
    fn test_create_mask_none_returns_nothing() {
        let result =
            generate_create_mask_sql("cat", "sch", MaskStrategy::None, "prod").unwrap();
        assert!(
            result.is_none(),
            "MaskStrategy::None is identity — no CREATE FUNCTION needed"
        );
    }

    #[test]
    fn test_set_mask_hash() {
        let sql = generate_set_mask_sql(
            "cat",
            "sch",
            "users",
            "email",
            MaskStrategy::Hash,
            "prod",
        )
        .unwrap()
        .expect("Hash has SQL");
        assert_eq!(
            sql,
            "ALTER TABLE cat.sch.users ALTER COLUMN email SET MASK cat.sch.rocky_mask_hash_prod"
        );
    }

    #[test]
    fn test_set_mask_none_returns_nothing() {
        let result = generate_set_mask_sql(
            "cat",
            "sch",
            "users",
            "email",
            MaskStrategy::None,
            "prod",
        )
        .unwrap();
        assert!(
            result.is_none(),
            "None is the explicit-identity case — no SET MASK"
        );
    }

    #[test]
    fn test_drop_mask() {
        let sql = generate_drop_mask_sql("cat", "sch", "users", "email").unwrap();
        assert_eq!(
            sql,
            "ALTER TABLE cat.sch.users ALTER COLUMN email DROP MASK"
        );
    }

    #[test]
    fn test_rejects_bad_identifiers() {
        assert!(
            generate_create_mask_sql("bad; DROP", "sch", MaskStrategy::Hash, "prod").is_err()
        );
        assert!(
            generate_create_mask_sql("cat", "sch", MaskStrategy::Hash, "bad env").is_err()
        );
        assert!(generate_set_mask_sql(
            "cat",
            "sch",
            "users",
            "bad col",
            MaskStrategy::Hash,
            "prod"
        )
        .is_err());
        assert!(generate_drop_mask_sql("cat", "sch", "users", "bad col").is_err());
    }
}
