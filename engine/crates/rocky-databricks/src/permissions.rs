use rocky_core::ir::{Grant, GrantTarget, Permission, PermissionDiff};
use rocky_sql::validation;
use tracing::debug;

use crate::connector::{ConnectorError, DatabricksConnector};

/// Manages Databricks permission reconciliation.
///
/// Compares desired permissions (from YAML config) against current grants,
/// then applies GRANT/REVOKE statements to reconcile.
pub struct PermissionManager<'a> {
    connector: &'a DatabricksConnector,
}

#[derive(Debug, thiserror::Error)]
pub enum PermissionError {
    #[error("connector error: {0}")]
    Connector(#[from] ConnectorError),

    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("failed to parse grant row: {0}")]
    ParseError(String),
}

/// Permissions that Rocky manages. Others (OWNERSHIP, ALL PRIVILEGES, CREATE SCHEMA) are skipped.
const MANAGED_PERMISSIONS: &[&str] = &[
    "BROWSE",
    "USE CATALOG",
    "USE SCHEMA",
    "SELECT",
    "MODIFY",
    "MANAGE",
];

impl<'a> PermissionManager<'a> {
    pub fn new(connector: &'a DatabricksConnector) -> Self {
        PermissionManager { connector }
    }

    /// Gets current grants on a catalog.
    pub async fn get_catalog_grants(&self, catalog: &str) -> Result<Vec<Grant>, PermissionError> {
        validation::validate_identifier(catalog)?;
        let sql = format!("SHOW GRANTS ON CATALOG {catalog}");
        self.parse_grants_result(&sql, GrantTarget::Catalog(catalog.to_string()))
            .await
    }

    /// Gets current grants on a schema.
    pub async fn get_schema_grants(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<Grant>, PermissionError> {
        validation::validate_identifier(catalog)?;
        validation::validate_identifier(schema)?;
        let sql = format!("SHOW GRANTS ON SCHEMA {catalog}.{schema}");
        self.parse_grants_result(
            &sql,
            GrantTarget::Schema {
                catalog: catalog.to_string(),
                schema: schema.to_string(),
            },
        )
        .await
    }

    /// Computes the diff between desired and current grants.
    pub fn compute_diff(desired: &[Grant], current: &[Grant]) -> PermissionDiff {
        let mut grants_to_add = Vec::new();
        let mut grants_to_revoke = Vec::new();

        // Find grants to add (in desired but not in current)
        for d in desired {
            let exists = current.iter().any(|c| {
                c.principal.to_lowercase() == d.principal.to_lowercase()
                    && c.permission == d.permission
            });
            if !exists {
                grants_to_add.push(d.clone());
            }
        }

        // Find grants to revoke (in current but not in desired, only for managed permissions)
        for c in current {
            let desired_exists = desired.iter().any(|d| {
                d.principal.to_lowercase() == c.principal.to_lowercase()
                    && d.permission == c.permission
            });
            if !desired_exists {
                grants_to_revoke.push(c.clone());
            }
        }

        PermissionDiff {
            grants_to_add,
            grants_to_revoke,
        }
    }

    /// Applies a permission diff (GRANT + REVOKE statements).
    pub async fn apply_diff(&self, diff: &PermissionDiff) -> Result<(), PermissionError> {
        for grant in &diff.grants_to_add {
            let sql = format_grant_sql(grant)?;
            debug!(principal = grant.principal, permission = %grant.permission, "granting");
            self.connector.execute_statement(&sql).await?;
        }

        for grant in &diff.grants_to_revoke {
            let sql = format_revoke_sql(grant)?;
            debug!(principal = grant.principal, permission = %grant.permission, "revoking");
            self.connector.execute_statement(&sql).await?;
        }

        Ok(())
    }

    /// Full reconciliation: get current grants, compute diff, apply.
    pub async fn reconcile(
        &self,
        desired: &[Grant],
        target: &GrantTarget,
    ) -> Result<PermissionDiff, PermissionError> {
        let current = match target {
            GrantTarget::Catalog(cat) => self.get_catalog_grants(cat).await?,
            GrantTarget::Schema { catalog, schema } => {
                self.get_schema_grants(catalog, schema).await?
            }
        };

        let diff = Self::compute_diff(desired, &current);
        self.apply_diff(&diff).await?;
        Ok(diff)
    }

    async fn parse_grants_result(
        &self,
        sql: &str,
        target: GrantTarget,
    ) -> Result<Vec<Grant>, PermissionError> {
        let result = self.connector.execute_sql(sql).await?;
        let mut grants = Vec::new();

        for row in &result.rows {
            // SHOW GRANTS returns: (principal, action_type, object_type, object_name)
            let principal = row
                .first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| PermissionError::ParseError("missing principal".into()))?;

            let action_type = row
                .get(1)
                .and_then(|v| v.as_str())
                .ok_or_else(|| PermissionError::ParseError("missing action_type".into()))?;

            // Skip non-managed permissions
            if !MANAGED_PERMISSIONS.contains(&action_type) {
                continue;
            }

            if let Some(permission) = parse_permission(action_type) {
                grants.push(Grant {
                    principal: principal.to_string(),
                    permission,
                    target: target.clone(),
                });
            }
        }

        Ok(grants)
    }
}

fn parse_permission(action_type: &str) -> Option<Permission> {
    match action_type {
        "BROWSE" => Some(Permission::Browse),
        "USE CATALOG" => Some(Permission::UseCatalog),
        "USE SCHEMA" => Some(Permission::UseSchema),
        "SELECT" => Some(Permission::Select),
        "MODIFY" => Some(Permission::Modify),
        "MANAGE" => Some(Permission::Manage),
        _ => None,
    }
}

fn format_grant_sql(grant: &Grant) -> Result<String, PermissionError> {
    let principal = validation::format_principal(&grant.principal)?;
    let permission = &grant.permission;
    let target_sql = format_target(&grant.target)?;
    Ok(format!("GRANT {permission} ON {target_sql} TO {principal}"))
}

fn format_revoke_sql(grant: &Grant) -> Result<String, PermissionError> {
    let principal = validation::format_principal(&grant.principal)?;
    let permission = &grant.permission;
    let target_sql = format_target(&grant.target)?;
    Ok(format!(
        "REVOKE {permission} ON {target_sql} FROM {principal}"
    ))
}

fn format_target(target: &GrantTarget) -> Result<String, PermissionError> {
    match target {
        GrantTarget::Catalog(cat) => {
            validation::validate_identifier(cat)?;
            Ok(format!("CATALOG {cat}"))
        }
        GrantTarget::Schema { catalog, schema } => {
            validation::validate_identifier(catalog)?;
            validation::validate_identifier(schema)?;
            Ok(format!("SCHEMA {catalog}.{schema}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_managed_permissions() {
        assert!(matches!(
            parse_permission("BROWSE"),
            Some(Permission::Browse)
        ));
        assert!(matches!(
            parse_permission("USE CATALOG"),
            Some(Permission::UseCatalog)
        ));
        assert!(matches!(
            parse_permission("SELECT"),
            Some(Permission::Select)
        ));
    }

    #[test]
    fn test_parse_unmanaged_permissions() {
        assert!(parse_permission("OWNERSHIP").is_none());
        assert!(parse_permission("ALL PRIVILEGES").is_none());
        assert!(parse_permission("CREATE SCHEMA").is_none());
    }

    #[test]
    fn test_format_grant_catalog() {
        let grant = Grant {
            principal: "Data Engineers".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("acme_warehouse".into()),
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "GRANT BROWSE ON CATALOG acme_warehouse TO `Data Engineers`"
        );
    }

    #[test]
    fn test_format_grant_schema() {
        let grant = Grant {
            principal: "user@domain.com".into(),
            permission: Permission::Select,
            target: GrantTarget::Schema {
                catalog: "acme_warehouse".into(),
                schema: "staging__na__fb_ads".into(),
            },
        };
        let sql = format_grant_sql(&grant).unwrap();
        assert_eq!(
            sql,
            "GRANT SELECT ON SCHEMA acme_warehouse.staging__na__fb_ads TO `user@domain.com`"
        );
    }

    #[test]
    fn test_format_revoke() {
        let grant = Grant {
            principal: "old_group".into(),
            permission: Permission::Manage,
            target: GrantTarget::Catalog("catalog".into()),
        };
        let sql = format_revoke_sql(&grant).unwrap();
        assert_eq!(sql, "REVOKE MANAGE ON CATALOG catalog FROM `old_group`");
    }

    #[test]
    fn test_compute_diff_add() {
        let desired = vec![Grant {
            principal: "engineers".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("cat".into()),
        }];
        let current = vec![];

        let diff = PermissionManager::compute_diff(&desired, &current);
        assert_eq!(diff.grants_to_add.len(), 1);
        assert!(diff.grants_to_revoke.is_empty());
    }

    #[test]
    fn test_compute_diff_revoke() {
        let desired = vec![];
        let current = vec![Grant {
            principal: "old_user".into(),
            permission: Permission::Select,
            target: GrantTarget::Catalog("cat".into()),
        }];

        let diff = PermissionManager::compute_diff(&desired, &current);
        assert!(diff.grants_to_add.is_empty());
        assert_eq!(diff.grants_to_revoke.len(), 1);
    }

    #[test]
    fn test_compute_diff_no_change() {
        let grants = vec![Grant {
            principal: "engineers".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("cat".into()),
        }];

        let diff = PermissionManager::compute_diff(&grants, &grants);
        assert!(diff.grants_to_add.is_empty());
        assert!(diff.grants_to_revoke.is_empty());
    }

    #[test]
    fn test_compute_diff_case_insensitive_principal() {
        let desired = vec![Grant {
            principal: "Engineers".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("cat".into()),
        }];
        let current = vec![Grant {
            principal: "engineers".into(),
            permission: Permission::Browse,
            target: GrantTarget::Catalog("cat".into()),
        }];

        let diff = PermissionManager::compute_diff(&desired, &current);
        assert!(diff.grants_to_add.is_empty());
        assert!(diff.grants_to_revoke.is_empty());
    }
}
