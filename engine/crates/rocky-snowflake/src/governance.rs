//! Snowflake governance adapter implementing [`GovernanceAdapter`].
//!
//! Snowflake supports object tagging and role-based grants but does NOT
//! support workspace binding or catalog isolation (those are Databricks-specific
//! Unity Catalog concepts). The unsupported methods return `Ok(())`.
//!
//! ## Snowflake SQL mapping
//!
//! | Trait method       | Snowflake SQL                                                   |
//! |--------------------|-----------------------------------------------------------------|
//! | `set_tags`         | `ALTER DATABASE/SCHEMA/TABLE ... SET TAG key = 'value'`         |
//! | `get_grants`       | `SHOW GRANTS ON DATABASE/SCHEMA ...`                            |
//! | `apply_grants`     | `GRANT <priv> ON DATABASE/SCHEMA ... TO ROLE <role>`            |
//! | `revoke_grants`    | `REVOKE <priv> ON DATABASE/SCHEMA ... FROM ROLE <role>`         |
//! | `bind_workspace`   | No-op (not supported)                                           |
//! | `set_isolation`    | No-op (not supported)                                           |

use std::collections::BTreeMap;

use async_trait::async_trait;
use tracing::debug;

use rocky_core::ir::{Grant, GrantTarget, Permission};
use rocky_core::traits::{AdapterError, AdapterResult, GovernanceAdapter, TagTarget};
use rocky_sql::validation;

use crate::connector::SnowflakeConnector;

/// Snowflake governance adapter for tags and grants.
///
/// Workspace binding and isolation are not supported by Snowflake and
/// silently return `Ok(())`.
pub struct SnowflakeGovernanceAdapter<'a> {
    connector: &'a SnowflakeConnector,
}

impl<'a> SnowflakeGovernanceAdapter<'a> {
    pub fn new(connector: &'a SnowflakeConnector) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl GovernanceAdapter for SnowflakeGovernanceAdapter<'_> {
    async fn set_tags(
        &self,
        target: &TagTarget,
        tags: &BTreeMap<String, String>,
    ) -> AdapterResult<()> {
        if tags.is_empty() {
            return Ok(());
        }

        let sql = format_set_tags_sql(target, tags)?;
        debug!(sql = sql.as_str(), "setting Snowflake tags");
        self.connector
            .execute_statement(&sql)
            .await
            .map(|_| ())
            .map_err(AdapterError::new)
    }

    async fn get_grants(&self, target: &GrantTarget) -> AdapterResult<Vec<Grant>> {
        let sql = format_show_grants_sql(target)?;
        debug!(sql = sql.as_str(), "querying Snowflake grants");
        let result = self
            .connector
            .execute_sql(&sql)
            .await
            .map_err(AdapterError::new)?;

        let mut grants = Vec::new();
        for row in &result.rows {
            // Snowflake SHOW GRANTS ON DATABASE/SCHEMA returns rows with columns:
            //   created_on, privilege, granted_on, name, granted_to, grantee_name, ...
            // We need privilege (col 1) and grantee_name (col 5).
            let privilege = row.get(1).and_then(|v| v.as_str()).unwrap_or_default();

            let grantee = row.get(5).and_then(|v| v.as_str()).unwrap_or_default();

            if grantee.is_empty() {
                continue;
            }

            if let Some(permission) = parse_snowflake_privilege(privilege) {
                grants.push(Grant {
                    principal: grantee.to_string(),
                    permission,
                    target: target.clone(),
                });
            }
        }

        Ok(grants)
    }

    async fn apply_grants(&self, grants: &[Grant]) -> AdapterResult<()> {
        for grant in grants {
            let sql = format_grant_sql(grant)?;
            debug!(
                principal = grant.principal,
                permission = %grant.permission,
                "granting Snowflake privilege"
            );
            self.connector
                .execute_statement(&sql)
                .await
                .map(|_| ())
                .map_err(AdapterError::new)?;
        }
        Ok(())
    }

    async fn revoke_grants(&self, grants: &[Grant]) -> AdapterResult<()> {
        for grant in grants {
            let sql = format_revoke_sql(grant)?;
            debug!(
                principal = grant.principal,
                permission = %grant.permission,
                "revoking Snowflake privilege"
            );
            self.connector
                .execute_statement(&sql)
                .await
                .map(|_| ())
                .map_err(AdapterError::new)?;
        }
        Ok(())
    }

    async fn bind_workspace(
        &self,
        _catalog: &str,
        _workspace_id: u64,
        _binding_type: &str,
    ) -> AdapterResult<()> {
        // Snowflake does not support workspace binding.
        Ok(())
    }

    async fn set_isolation(&self, _catalog: &str, _enabled: bool) -> AdapterResult<()> {
        // Snowflake does not support catalog isolation mode.
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// SQL generation helpers
// ---------------------------------------------------------------------------

/// Generates `ALTER DATABASE/SCHEMA/TABLE ... SET TAG k1 = 'v1', k2 = 'v2'`.
fn format_set_tags_sql(
    target: &TagTarget,
    tags: &BTreeMap<String, String>,
) -> AdapterResult<String> {
    let tag_pairs: Vec<String> = tags
        .iter()
        .map(|(k, v)| {
            validation::validate_identifier(k).map_err(AdapterError::new)?;
            // Escape single quotes in tag values.
            let escaped = v.replace('\'', "''");
            Ok(format!("{k} = '{escaped}'"))
        })
        .collect::<AdapterResult<Vec<_>>>()?;

    let tag_clause = tag_pairs.join(", ");

    match target {
        TagTarget::Catalog(database) => {
            validation::validate_identifier(database).map_err(AdapterError::new)?;
            Ok(format!("ALTER DATABASE {database} SET TAG {tag_clause}"))
        }
        TagTarget::Schema { catalog, schema } => {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!(
                "ALTER SCHEMA {catalog}.{schema} SET TAG {tag_clause}"
            ))
        }
        TagTarget::Table {
            catalog,
            schema,
            table,
        } => {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            validation::validate_identifier(table).map_err(AdapterError::new)?;
            Ok(format!(
                "ALTER TABLE {catalog}.{schema}.{table} SET TAG {tag_clause}"
            ))
        }
    }
}

/// Generates `SHOW GRANTS ON DATABASE/SCHEMA ...`.
fn format_show_grants_sql(target: &GrantTarget) -> AdapterResult<String> {
    match target {
        GrantTarget::Catalog(database) => {
            validation::validate_identifier(database).map_err(AdapterError::new)?;
            Ok(format!("SHOW GRANTS ON DATABASE {database}"))
        }
        GrantTarget::Schema { catalog, schema } => {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!("SHOW GRANTS ON SCHEMA {catalog}.{schema}"))
        }
    }
}

/// Generates `GRANT <priv> ON DATABASE/SCHEMA ... TO ROLE <role>`.
fn format_grant_sql(grant: &Grant) -> AdapterResult<String> {
    let privilege = snowflake_privilege_name(&grant.permission);
    let target_sql = format_grant_target(&grant.target)?;
    let role = format_role(&grant.principal)?;
    Ok(format!("GRANT {privilege} ON {target_sql} TO ROLE {role}"))
}

/// Generates `REVOKE <priv> ON DATABASE/SCHEMA ... FROM ROLE <role>`.
fn format_revoke_sql(grant: &Grant) -> AdapterResult<String> {
    let privilege = snowflake_privilege_name(&grant.permission);
    let target_sql = format_grant_target(&grant.target)?;
    let role = format_role(&grant.principal)?;
    Ok(format!(
        "REVOKE {privilege} ON {target_sql} FROM ROLE {role}"
    ))
}

/// Formats a `GrantTarget` as `DATABASE <name>` or `SCHEMA <db>.<schema>`.
fn format_grant_target(target: &GrantTarget) -> AdapterResult<String> {
    match target {
        GrantTarget::Catalog(database) => {
            validation::validate_identifier(database).map_err(AdapterError::new)?;
            Ok(format!("DATABASE {database}"))
        }
        GrantTarget::Schema { catalog, schema } => {
            validation::validate_identifier(catalog).map_err(AdapterError::new)?;
            validation::validate_identifier(schema).map_err(AdapterError::new)?;
            Ok(format!("SCHEMA {catalog}.{schema}"))
        }
    }
}

/// Validates and double-quotes a Snowflake role name.
///
/// Snowflake uses `ROLE "name"` (double-quoted) rather than backtick-quoted.
fn format_role(name: &str) -> AdapterResult<String> {
    validation::validate_principal(name).map_err(AdapterError::new)?;
    Ok(format!("\"{name}\""))
}

/// Maps Rocky [`Permission`] variants to Snowflake privilege names.
///
/// Snowflake privileges differ from Databricks:
/// - `BROWSE` has no direct equivalent; mapped to `MONITOR` (closest read-only metadata priv).
/// - `USE CATALOG` maps to `USAGE` on DATABASE.
/// - `USE SCHEMA` maps to `USAGE` on SCHEMA.
/// - `SELECT` maps to `SELECT`.
/// - `MODIFY` maps to `MODIFY`.
/// - `MANAGE` maps to `MANAGE GRANTS`.
fn snowflake_privilege_name(perm: &Permission) -> &'static str {
    match perm {
        Permission::Browse => "MONITOR",
        Permission::UseCatalog => "USAGE",
        Permission::UseSchema => "USAGE",
        Permission::Select => "SELECT",
        Permission::Modify => "MODIFY",
        Permission::Manage => "MANAGE GRANTS",
    }
}

/// Parses a Snowflake privilege string into a Rocky [`Permission`].
///
/// Only parses privileges that Rocky manages; others are skipped.
fn parse_snowflake_privilege(privilege: &str) -> Option<Permission> {
    match privilege {
        "MONITOR" => Some(Permission::Browse),
        "USAGE" => Some(Permission::UseCatalog),
        "SELECT" => Some(Permission::Select),
        "MODIFY" => Some(Permission::Modify),
        "MANAGE GRANTS" => Some(Permission::Manage),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // ---- Tag SQL generation ----

    #[test]
    fn test_set_tags_catalog() {
        let mut tags = BTreeMap::new();
        tags.insert("managed_by".into(), "rocky".into());
        let sql = format_set_tags_sql(&TagTarget::Catalog("my_database".into()), &tags).unwrap();
        assert_eq!(
            sql,
            "ALTER DATABASE my_database SET TAG managed_by = 'rocky'"
        );
    }

    #[test]
    fn test_set_tags_schema() {
        let mut tags = BTreeMap::new();
        tags.insert("env".into(), "production".into());
        let sql = format_set_tags_sql(
            &TagTarget::Schema {
                catalog: "my_db".into(),
                schema: "staging".into(),
            },
            &tags,
        )
        .unwrap();
        assert_eq!(sql, "ALTER SCHEMA my_db.staging SET TAG env = 'production'");
    }

    #[test]
    fn test_set_tags_table() {
        let mut tags = BTreeMap::new();
        tags.insert("pii".into(), "true".into());
        let sql = format_set_tags_sql(
            &TagTarget::Table {
                catalog: "db".into(),
                schema: "raw".into(),
                table: "users".into(),
            },
            &tags,
        )
        .unwrap();
        assert_eq!(sql, "ALTER TABLE db.raw.users SET TAG pii = 'true'");
    }

    #[test]
    fn test_set_tags_multiple() {
        let mut tags = BTreeMap::new();
        tags.insert("env".into(), "dev".into());
        tags.insert("managed_by".into(), "rocky".into());
        let sql = format_set_tags_sql(&TagTarget::Catalog("warehouse".into()), &tags).unwrap();
        // BTreeMap is sorted by key, so env comes before managed_by
        assert_eq!(
            sql,
            "ALTER DATABASE warehouse SET TAG env = 'dev', managed_by = 'rocky'"
        );
    }

    #[test]
    fn test_set_tags_escapes_single_quotes() {
        let mut tags = BTreeMap::new();
        tags.insert("desc".into(), "Hugo's pipeline".into());
        let sql = format_set_tags_sql(&TagTarget::Catalog("db".into()), &tags).unwrap();
        assert_eq!(sql, "ALTER DATABASE db SET TAG desc = 'Hugo''s pipeline'");
    }

    // ---- Grant SQL generation ----

    #[test]
    fn test_grant_select_on_database() {
        let grant = Grant {
            principal: "DATA_READERS".into(),
            permission: Permission::Select,
            target: GrantTarget::Catalog("analytics".into()),
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "GRANT SELECT ON DATABASE analytics TO ROLE \"DATA_READERS\""
        );
    }

    #[test]
    fn test_grant_usage_on_schema() {
        let grant = Grant {
            principal: "ANALYSTS".into(),
            permission: Permission::UseSchema,
            target: GrantTarget::Schema {
                catalog: "analytics".into(),
                schema: "raw_na_fb_ads".into(),
            },
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "GRANT USAGE ON SCHEMA analytics.raw_na_fb_ads TO ROLE \"ANALYSTS\""
        );
    }

    #[test]
    fn test_grant_browse_maps_to_monitor() {
        let grant = Grant {
            principal: "OPS".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("warehouse".into()),
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert!(sql.starts_with("GRANT MONITOR ON DATABASE warehouse"));
    }

    #[test]
    fn test_grant_manage_maps_to_manage_grants() {
        let grant = Grant {
            principal: "ADMINS".into(),
            permission: Permission::Manage,
            target: GrantTarget::Catalog("warehouse".into()),
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert!(sql.starts_with("GRANT MANAGE GRANTS ON DATABASE warehouse"));
    }

    // ---- Revoke SQL generation ----

    #[test]
    fn test_revoke_select_on_database() {
        let grant = Grant {
            principal: "OLD_ROLE".into(),
            permission: Permission::Select,
            target: GrantTarget::Catalog("analytics".into()),
        };
        let sql = format_revoke_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "REVOKE SELECT ON DATABASE analytics FROM ROLE \"OLD_ROLE\""
        );
    }

    #[test]
    fn test_revoke_usage_on_schema() {
        let grant = Grant {
            principal: "STALE_ROLE".into(),
            permission: Permission::UseSchema,
            target: GrantTarget::Schema {
                catalog: "db".into(),
                schema: "staging".into(),
            },
        };
        let sql = format_revoke_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "REVOKE USAGE ON SCHEMA db.staging FROM ROLE \"STALE_ROLE\""
        );
    }

    // ---- SHOW GRANTS SQL generation ----

    #[test]
    fn test_show_grants_database() {
        let sql = format_show_grants_sql(&GrantTarget::Catalog("my_warehouse".into())).unwrap();
        assert_eq!(sql, "SHOW GRANTS ON DATABASE my_warehouse");
    }

    #[test]
    fn test_show_grants_schema() {
        let sql = format_show_grants_sql(&GrantTarget::Schema {
            catalog: "db".into(),
            schema: "raw".into(),
        })
        .unwrap();
        assert_eq!(sql, "SHOW GRANTS ON SCHEMA db.raw");
    }

    // ---- Privilege mapping ----

    #[test]
    fn test_snowflake_privilege_mapping() {
        assert_eq!(snowflake_privilege_name(&Permission::Browse), "MONITOR");
        assert_eq!(snowflake_privilege_name(&Permission::UseCatalog), "USAGE");
        assert_eq!(snowflake_privilege_name(&Permission::UseSchema), "USAGE");
        assert_eq!(snowflake_privilege_name(&Permission::Select), "SELECT");
        assert_eq!(snowflake_privilege_name(&Permission::Modify), "MODIFY");
        assert_eq!(
            snowflake_privilege_name(&Permission::Manage),
            "MANAGE GRANTS"
        );
    }

    #[test]
    fn test_parse_snowflake_privileges() {
        assert_eq!(
            parse_snowflake_privilege("MONITOR"),
            Some(Permission::Browse)
        );
        assert_eq!(
            parse_snowflake_privilege("USAGE"),
            Some(Permission::UseCatalog)
        );
        assert_eq!(
            parse_snowflake_privilege("SELECT"),
            Some(Permission::Select)
        );
        assert_eq!(
            parse_snowflake_privilege("MODIFY"),
            Some(Permission::Modify)
        );
        assert_eq!(
            parse_snowflake_privilege("MANAGE GRANTS"),
            Some(Permission::Manage)
        );
    }

    #[test]
    fn test_parse_snowflake_unmanaged_privileges() {
        assert_eq!(parse_snowflake_privilege("OWNERSHIP"), None);
        assert_eq!(parse_snowflake_privilege("ALL PRIVILEGES"), None);
        assert_eq!(parse_snowflake_privilege("CREATE TABLE"), None);
    }

    // ---- Validation ----

    #[test]
    fn test_format_role_double_quotes() {
        let role = format_role("DATA_READERS").unwrap();
        assert_eq!(role, "\"DATA_READERS\"");
    }

    #[test]
    fn test_set_tags_invalid_identifier_rejected() {
        let mut tags = BTreeMap::new();
        tags.insert("managed_by".into(), "rocky".into());
        let result = format_set_tags_sql(&TagTarget::Catalog("DROP TABLE; --".into()), &tags);
        assert!(result.is_err());
    }

    #[test]
    fn test_grant_invalid_identifier_rejected() {
        let grant = Grant {
            principal: "ROLE".into(),
            permission: Permission::Select,
            target: GrantTarget::Catalog("DROP TABLE; --".into()),
        };
        assert!(format_grant_sql(&grant).is_err());
    }

    #[test]
    fn test_show_grants_invalid_identifier_rejected() {
        let result = format_show_grants_sql(&GrantTarget::Catalog("bad;name".into()));
        assert!(result.is_err());
    }
}
