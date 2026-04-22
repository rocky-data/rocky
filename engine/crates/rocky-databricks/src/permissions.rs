use rocky_core::ir::{Grant, GrantTarget, Permission, PermissionDiff};
use rocky_sql::validation;
use tracing::debug;

use crate::connector::{ConnectorError, DatabricksConnector};
use crate::workspace::{WorkspaceBinding, WorkspaceError, WorkspaceManager};

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

    #[error("workspace binding error: {0}")]
    Workspace(#[from] WorkspaceError),
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

/// Desired state for a single catalog-to-workspace binding.
///
/// Mirrors the config-side `WorkspaceBindingConfig` but lives on the
/// adapter seam so reconcile() can compare against the Unity Catalog API
/// response without dragging config types into rocky-databricks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkspaceBindingDesired {
    pub workspace_id: u64,
    /// Databricks API string, e.g. `"BINDING_TYPE_READ_WRITE"` or
    /// `"BINDING_TYPE_READ_ONLY"`.
    pub binding_type: String,
}

/// Diff between desired and current workspace bindings for one catalog.
#[derive(Debug, Clone, Default)]
pub struct WorkspaceBindingDiff {
    /// New bindings to add, or existing bindings whose access level changed.
    pub bindings_to_add: Vec<WorkspaceBindingDesired>,
    /// Bindings present on the catalog but not in the desired state.
    pub bindings_to_remove: Vec<WorkspaceBindingDesired>,
}

impl WorkspaceBindingDiff {
    pub fn is_empty(&self) -> bool {
        self.bindings_to_add.is_empty() && self.bindings_to_remove.is_empty()
    }
}

/// Combined reconcile result for a single catalog covering grants and
/// workspace bindings. `rocky plan` / `rocky run` group these deltas
/// together because both flow from the same governance block.
#[derive(Debug, Clone, Default)]
pub struct AccessDiff {
    pub permissions: PermissionDiff,
    pub bindings: WorkspaceBindingDiff,
}

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

    /// Computes the binding diff between desired and current workspace bindings.
    ///
    /// Key equality is `workspace_id`. A binding whose access level changed
    /// lands in both `bindings_to_remove` (old) and `bindings_to_add` (new);
    /// Unity Catalog's PATCH endpoint applies the remove + add atomically.
    pub fn compute_binding_diff(
        desired: &[WorkspaceBindingDesired],
        current: &[(u64, String)],
    ) -> WorkspaceBindingDiff {
        let mut to_add = Vec::new();
        let mut to_remove = Vec::new();

        for d in desired {
            let matching = current.iter().find(|(id, _)| *id == d.workspace_id);
            match matching {
                Some((_, cur_type)) if cur_type == &d.binding_type => {
                    // unchanged
                }
                Some((_, cur_type)) => {
                    // access level changed — remove old then add new
                    to_remove.push(WorkspaceBindingDesired {
                        workspace_id: d.workspace_id,
                        binding_type: cur_type.clone(),
                    });
                    to_add.push(d.clone());
                }
                None => {
                    to_add.push(d.clone());
                }
            }
        }

        for (cur_id, cur_type) in current {
            if !desired.iter().any(|d| d.workspace_id == *cur_id) {
                to_remove.push(WorkspaceBindingDesired {
                    workspace_id: *cur_id,
                    binding_type: cur_type.clone(),
                });
            }
        }

        WorkspaceBindingDiff {
            bindings_to_add: to_add,
            bindings_to_remove: to_remove,
        }
    }

    /// Applies a binding diff via a single PATCH to the Unity Catalog
    /// workspace-bindings endpoint.
    pub async fn apply_binding_diff(
        ws_mgr: &WorkspaceManager,
        catalog: &str,
        diff: &WorkspaceBindingDiff,
    ) -> Result<(), PermissionError> {
        if diff.is_empty() {
            return Ok(());
        }
        let add: Vec<WorkspaceBinding> = diff
            .bindings_to_add
            .iter()
            .map(|b| WorkspaceBinding {
                workspace_id: b.workspace_id,
                binding_type: Some(b.binding_type.clone()),
            })
            .collect();
        let remove: Vec<WorkspaceBinding> = diff
            .bindings_to_remove
            .iter()
            .map(|b| WorkspaceBinding {
                workspace_id: b.workspace_id,
                binding_type: None,
            })
            .collect();
        debug!(
            catalog,
            adds = add.len(),
            removes = remove.len(),
            "applying workspace binding diff"
        );
        ws_mgr.update_bindings(catalog, add, remove).await?;
        Ok(())
    }

    /// Unified reconcile pass for grants and workspace bindings on a single
    /// catalog.
    ///
    /// This is the entry point `rocky run` uses for Databricks-backed
    /// pipelines that declare workspace bindings in their governance config.
    /// When `ws_mgr` is `None` or `desired_bindings` is empty, the bindings
    /// leg is a no-op and only grants reconcile — callers using the previous
    /// grants-only reconcile are unaffected.
    pub async fn reconcile_access(
        &self,
        desired_grants: &[Grant],
        target: &GrantTarget,
        ws_mgr: Option<&WorkspaceManager>,
        desired_bindings: &[WorkspaceBindingDesired],
    ) -> Result<AccessDiff, PermissionError> {
        let permissions = self.reconcile(desired_grants, target).await?;

        let bindings = match (ws_mgr, target) {
            (Some(mgr), GrantTarget::Catalog(catalog)) => {
                let current = mgr
                    .get_bindings(catalog)
                    .await?
                    .into_iter()
                    .map(|b| {
                        let kind = b
                            .binding_type
                            .unwrap_or_else(|| "BINDING_TYPE_READ_WRITE".to_string());
                        (b.workspace_id, kind)
                    })
                    .collect::<Vec<_>>();
                let diff = Self::compute_binding_diff(desired_bindings, &current);
                Self::apply_binding_diff(mgr, catalog, &diff).await?;
                diff
            }
            _ => WorkspaceBindingDiff::default(),
        };

        Ok(AccessDiff {
            permissions,
            bindings,
        })
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

    // ---- Workspace binding diff ----

    #[test]
    fn test_binding_diff_add_when_missing() {
        let desired = vec![WorkspaceBindingDesired {
            workspace_id: 123,
            binding_type: "BINDING_TYPE_READ_WRITE".into(),
        }];
        let current = vec![];
        let diff = PermissionManager::compute_binding_diff(&desired, &current);
        assert_eq!(diff.bindings_to_add.len(), 1);
        assert_eq!(diff.bindings_to_add[0].workspace_id, 123);
        assert!(diff.bindings_to_remove.is_empty());
    }

    #[test]
    fn test_binding_diff_remove_when_undesired() {
        let desired = vec![];
        let current = vec![(999, "BINDING_TYPE_READ_WRITE".into())];
        let diff = PermissionManager::compute_binding_diff(&desired, &current);
        assert!(diff.bindings_to_add.is_empty());
        assert_eq!(diff.bindings_to_remove.len(), 1);
        assert_eq!(diff.bindings_to_remove[0].workspace_id, 999);
    }

    #[test]
    fn test_binding_diff_unchanged() {
        let desired = vec![WorkspaceBindingDesired {
            workspace_id: 123,
            binding_type: "BINDING_TYPE_READ_WRITE".into(),
        }];
        let current = vec![(123, "BINDING_TYPE_READ_WRITE".into())];
        let diff = PermissionManager::compute_binding_diff(&desired, &current);
        assert!(diff.is_empty());
    }

    #[test]
    fn test_binding_diff_access_level_change() {
        let desired = vec![WorkspaceBindingDesired {
            workspace_id: 123,
            binding_type: "BINDING_TYPE_READ_ONLY".into(),
        }];
        let current = vec![(123, "BINDING_TYPE_READ_WRITE".into())];
        let diff = PermissionManager::compute_binding_diff(&desired, &current);
        // Access level change = one remove + one add, not two adds.
        assert_eq!(diff.bindings_to_add.len(), 1);
        assert_eq!(
            diff.bindings_to_add[0].binding_type,
            "BINDING_TYPE_READ_ONLY"
        );
        assert_eq!(diff.bindings_to_remove.len(), 1);
        assert_eq!(
            diff.bindings_to_remove[0].binding_type,
            "BINDING_TYPE_READ_WRITE"
        );
        assert_eq!(diff.bindings_to_add[0].workspace_id, 123);
        assert_eq!(diff.bindings_to_remove[0].workspace_id, 123);
    }

    #[test]
    fn test_binding_diff_mixed() {
        // workspace 1 unchanged, workspace 2 added, workspace 3 removed.
        let desired = vec![
            WorkspaceBindingDesired {
                workspace_id: 1,
                binding_type: "BINDING_TYPE_READ_WRITE".into(),
            },
            WorkspaceBindingDesired {
                workspace_id: 2,
                binding_type: "BINDING_TYPE_READ_ONLY".into(),
            },
        ];
        let current = vec![
            (1, "BINDING_TYPE_READ_WRITE".into()),
            (3, "BINDING_TYPE_READ_WRITE".into()),
        ];
        let diff = PermissionManager::compute_binding_diff(&desired, &current);
        assert_eq!(diff.bindings_to_add.len(), 1);
        assert_eq!(diff.bindings_to_add[0].workspace_id, 2);
        assert_eq!(diff.bindings_to_remove.len(), 1);
        assert_eq!(diff.bindings_to_remove[0].workspace_id, 3);
    }

    // ---- Combined reconcile (AccessDiff) ----

    #[test]
    fn test_access_diff_default_is_empty() {
        let diff = AccessDiff::default();
        assert!(diff.permissions.grants_to_add.is_empty());
        assert!(diff.permissions.grants_to_revoke.is_empty());
        assert!(diff.bindings.is_empty());
    }
}
