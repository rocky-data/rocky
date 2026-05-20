use rocky_ir::{Grant, GrantTarget, Permission, PermissionDiff};
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

    /// One or more GRANT/REVOKE statements failed mid-batch. Surfaces
    /// from [`PermissionManager::apply_diff`] at call sites that want a
    /// `Result<(), _>`-shaped error instead of inspecting the full
    /// [`AppliedPermissionDiff`]. Carries a count + the per-grant error
    /// list so the failure is greppable in logs.
    #[error("{n_failed} grant statement(s) failed: {summary}")]
    PartialApply {
        n_failed: usize,
        summary: String,
        failures: Vec<FailedGrant>,
    },
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
///
/// `apply_diff` reports per-grant outcomes via [`AppliedPermissionDiff`]
/// (and surfaces failures as [`PermissionError::PartialApply`]) — those
/// outcomes are collapsed into a single error at the `reconcile` boundary
/// so `AccessDiff` only carries the desired-vs-current delta. If a future
/// caller wants the structured outcome, expose `applied` here and have
/// `reconcile_with_bindings` thread it through instead of calling
/// [`AppliedPermissionDiff::into_result`].
#[derive(Debug, Clone, Default)]
pub struct AccessDiff {
    pub permissions: PermissionDiff,
    pub bindings: WorkspaceBindingDiff,
}

/// A grant whose GRANT / REVOKE statement returned an error.
///
/// Surfaces alongside the successful grants in [`AppliedPermissionDiff`]
/// so callers can re-reconcile the failed subset (e.g. on the next
/// `rocky run`) without losing track of what already landed.
#[derive(Debug, Clone)]
pub struct FailedGrant {
    pub grant: Grant,
    /// Whether the grant was being applied (`true` = GRANT) or removed
    /// (`false` = REVOKE). Captured so the next reconcile pass knows
    /// which side it belongs to.
    pub adding: bool,
    /// The error message from the warehouse. Kept as `String` so the
    /// struct stays `Clone` and the result can flow up through
    /// `AccessDiff` into the run summary.
    pub error: String,
}

/// Outcome of applying a [`PermissionDiff`]: a record of which GRANT /
/// REVOKE statements actually landed, plus the ones that failed.
///
/// The previous [`PermissionManager::apply_diff`] returned
/// `Result<(), PermissionError>` and aborted on the first failure — that
/// left the catalog in a half-applied state with no record of which
/// rows had landed, making safe retries impossible. Returning this
/// structured result lets the caller surface the failed subset in the
/// run summary and feed it back into the next reconcile pass (the
/// diff is order-independent because [`compute_diff`] is idempotent).
#[derive(Debug, Clone, Default)]
pub struct AppliedPermissionDiff {
    /// GRANT statements that succeeded.
    pub granted: Vec<Grant>,
    /// REVOKE statements that succeeded.
    pub revoked: Vec<Grant>,
    /// Grants whose GRANT or REVOKE statement failed.
    pub failed: Vec<FailedGrant>,
}

impl AppliedPermissionDiff {
    /// True when every desired GRANT and REVOKE landed cleanly.
    pub fn is_clean(&self) -> bool {
        self.failed.is_empty()
    }

    /// True when no statements ran (either the diff was empty or every
    /// one failed without any successes).
    pub fn is_empty(&self) -> bool {
        self.granted.is_empty() && self.revoked.is_empty() && self.failed.is_empty()
    }

    /// Converts this outcome into a `Result<(), PermissionError>` for
    /// call sites that don't carry the structured diff up the stack
    /// (the `apply_grants` / `revoke_grants` reconciler entry points).
    /// Returns `Ok(())` if clean, otherwise [`PermissionError::PartialApply`].
    pub fn into_result(self) -> Result<(), PermissionError> {
        if self.failed.is_empty() {
            return Ok(());
        }
        let summary = self
            .failed
            .iter()
            .take(3)
            .map(|f| {
                let verb = if f.adding { "GRANT" } else { "REVOKE" };
                format!("{verb} {} to {}: {}", f.grant.permission, f.grant.principal, f.error)
            })
            .collect::<Vec<_>>()
            .join("; ");
        Err(PermissionError::PartialApply {
            n_failed: self.failed.len(),
            summary,
            failures: self.failed,
        })
    }
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

    /// Applies a permission diff (GRANT + REVOKE statements) and
    /// reports the outcome.
    ///
    /// Unlike the previous shape (`Result<(), PermissionError>`, abort
    /// on first error), this method runs every GRANT and REVOKE
    /// independently, collects the failures in
    /// [`AppliedPermissionDiff::failed`], and only returns `Err` for
    /// errors that surface *before* any warehouse statement runs
    /// (e.g. a SQL identifier failing validation in `format_grant_sql`
    /// / `format_revoke_sql`). Warehouse-side failures land in
    /// `failed` so the caller can surface them in the run summary and
    /// retry on the next reconcile.
    ///
    /// Atomicity rationale: Databricks does not expose a SQL-level
    /// transaction for GRANT/REVOKE on Unity Catalog, and the
    /// per-statement permissions PATCH endpoint
    /// ([`crate::unity_catalog_client::UnityCatalogClient::patch_permissions`])
    /// only covers the `table` securable today — catalog and schema
    /// grants still flow through `execute_statement`. The next-best
    /// guarantee is "no statement silently lost": run every change,
    /// report what failed.
    pub async fn apply_diff(
        &self,
        diff: &PermissionDiff,
    ) -> Result<AppliedPermissionDiff, PermissionError> {
        let mut applied = AppliedPermissionDiff::default();

        for grant in &diff.grants_to_add {
            // Validation errors (bad identifier shape) surface *before*
            // any statement reaches the warehouse — propagate them so
            // the caller fails fast on misconfigured input.
            let sql = format_grant_sql(grant)?;
            debug!(principal = grant.principal, permission = %grant.permission, "granting");
            match self.connector.execute_statement(&sql).await {
                Ok(_) => applied.granted.push(grant.clone()),
                Err(e) => {
                    debug!(
                        principal = grant.principal,
                        permission = %grant.permission,
                        error = %e,
                        "grant statement failed; continuing batch"
                    );
                    applied.failed.push(FailedGrant {
                        grant: grant.clone(),
                        adding: true,
                        error: e.to_string(),
                    });
                }
            }
        }

        for grant in &diff.grants_to_revoke {
            let sql = format_revoke_sql(grant)?;
            debug!(principal = grant.principal, permission = %grant.permission, "revoking");
            match self.connector.execute_statement(&sql).await {
                Ok(_) => applied.revoked.push(grant.clone()),
                Err(e) => {
                    debug!(
                        principal = grant.principal,
                        permission = %grant.permission,
                        error = %e,
                        "revoke statement failed; continuing batch"
                    );
                    applied.failed.push(FailedGrant {
                        grant: grant.clone(),
                        adding: false,
                        error: e.to_string(),
                    });
                }
            }
        }

        Ok(applied)
    }

    /// Full reconciliation: get current grants, compute diff, apply.
    ///
    /// Returns the desired-vs-current [`PermissionDiff`]. Per-grant
    /// warehouse failures are collapsed into
    /// [`PermissionError::PartialApply`] via [`apply_diff`]'s structured
    /// outcome; this surfaces the failure to the caller without leaking
    /// the catalog into a half-applied state silently. If a future caller
    /// needs the structured `AppliedPermissionDiff` outcome (e.g. to
    /// thread `granted`/`revoked`/`failed` into a run summary), call
    /// [`apply_diff`] directly instead of `reconcile`.
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
        // Collapse the structured `AppliedPermissionDiff` into a flat
        // `Result<(), _>`: per-grant failures surface as
        // `PermissionError::PartialApply` so the caller can't silently
        // ignore them. The desired-vs-current `diff` is still returned
        // for the `AccessDiff` summary.
        self.apply_diff(&diff).await?.into_result()?;
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
    fn applied_diff_into_result_clean_passes() {
        let applied = AppliedPermissionDiff {
            granted: vec![Grant {
                principal: "u".into(),
                permission: Permission::Select,
                target: GrantTarget::Catalog("c".into()),
            }],
            revoked: vec![],
            failed: vec![],
        };
        assert!(applied.is_clean());
        applied.into_result().expect("clean diff must be Ok");
    }

    #[test]
    fn applied_diff_into_result_failures_surface_as_partial_apply() {
        let grant = Grant {
            principal: "u@d.com".into(),
            permission: Permission::Modify,
            target: GrantTarget::Catalog("c".into()),
        };
        let applied = AppliedPermissionDiff {
            granted: vec![],
            revoked: vec![],
            failed: vec![FailedGrant {
                grant: grant.clone(),
                adding: true,
                error: "principal not found".into(),
            }],
        };
        assert!(!applied.is_clean());
        match applied.into_result().expect_err("failed diff must be Err") {
            PermissionError::PartialApply {
                n_failed,
                summary,
                failures,
            } => {
                assert_eq!(n_failed, 1);
                assert!(summary.contains("GRANT"));
                assert!(summary.contains("principal not found"));
                assert_eq!(failures.len(), 1);
                assert_eq!(failures[0].grant.principal, "u@d.com");
                assert!(failures[0].adding);
            }
            other => panic!("expected PartialApply, got {other:?}"),
        }
    }

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
