//! Databricks governance adapter implementing [`GovernanceAdapter`].
//!
//! Consolidates catalog management, permission reconciliation, workspace
//! isolation, column-level classification tags, and column masking policies
//! behind a single trait interface.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::debug;

use rocky_core::ir::{Grant, GrantTarget, PermissionDiff, ResolvedRole, TableRef};
use rocky_core::masking;
use rocky_core::traits::{
    AdapterError, AdapterResult, GovernanceAdapter, MaskStrategy, MaskingPolicy, TagTarget,
};

use crate::auth::Auth;
use crate::catalog::CatalogManager;
use crate::connector::DatabricksConnector;
use crate::permissions::PermissionManager;
use crate::workspace::WorkspaceManager;

/// Databricks governance adapter combining catalog management,
/// permission reconciliation, and workspace isolation.
///
/// Owns an [`Arc<DatabricksConnector>`] so it can be constructed by the
/// adapter registry and returned as a `Box<dyn GovernanceAdapter>` without
/// borrowing from a shorter-lived handle.
pub struct DatabricksGovernanceAdapter {
    connector: Arc<DatabricksConnector>,
    workspace_mgr: Option<WorkspaceManager>,
}

impl DatabricksGovernanceAdapter {
    pub fn new(connector: Arc<DatabricksConnector>, host: &str, auth: Auth) -> Self {
        Self {
            connector,
            workspace_mgr: Some(WorkspaceManager::new(host.to_string(), auth)),
        }
    }

    /// Create without workspace manager (for environments without isolation).
    pub fn without_workspace(connector: Arc<DatabricksConnector>) -> Self {
        Self {
            connector,
            workspace_mgr: None,
        }
    }
}

#[async_trait]
impl GovernanceAdapter for DatabricksGovernanceAdapter {
    async fn set_tags(
        &self,
        target: &TagTarget,
        tags: &BTreeMap<String, String>,
    ) -> AdapterResult<()> {
        let mgr = CatalogManager::new(&self.connector);
        match target {
            TagTarget::Catalog(catalog) => mgr
                .set_catalog_tags(catalog, tags)
                .await
                .map_err(AdapterError::new),
            TagTarget::Schema { catalog, schema } => mgr
                .set_schema_tags(catalog, schema, tags)
                .await
                .map_err(AdapterError::new),
            TagTarget::Table {
                catalog,
                schema,
                table,
            } => mgr
                .set_table_tags(catalog, schema, table, tags)
                .await
                .map_err(AdapterError::new),
        }
    }

    async fn get_grants(&self, target: &GrantTarget) -> AdapterResult<Vec<Grant>> {
        let perm_mgr = PermissionManager::new(&self.connector);
        match target {
            GrantTarget::Catalog(catalog) => perm_mgr
                .get_catalog_grants(catalog)
                .await
                .map_err(AdapterError::new),
            GrantTarget::Schema { catalog, schema } => perm_mgr
                .get_schema_grants(catalog, schema)
                .await
                .map_err(AdapterError::new),
        }
    }

    async fn apply_grants(&self, grants: &[Grant]) -> AdapterResult<()> {
        let perm_mgr = PermissionManager::new(&self.connector);
        let diff = PermissionDiff {
            grants_to_add: grants.to_vec(),
            grants_to_revoke: vec![],
        };
        perm_mgr.apply_diff(&diff).await.map_err(AdapterError::new)
    }

    async fn revoke_grants(&self, grants: &[Grant]) -> AdapterResult<()> {
        let perm_mgr = PermissionManager::new(&self.connector);
        let diff = PermissionDiff {
            grants_to_add: vec![],
            grants_to_revoke: grants.to_vec(),
        };
        perm_mgr.apply_diff(&diff).await.map_err(AdapterError::new)
    }

    async fn bind_workspace(
        &self,
        catalog: &str,
        workspace_id: u64,
        binding_type: &str,
    ) -> AdapterResult<()> {
        let ws_mgr = self
            .workspace_mgr
            .as_ref()
            .ok_or_else(|| AdapterError::msg("workspace manager not configured"))?;
        ws_mgr
            .bind_workspace(catalog, workspace_id, binding_type)
            .await
            .map_err(AdapterError::new)
    }

    async fn set_isolation(&self, catalog: &str, enabled: bool) -> AdapterResult<()> {
        if !enabled {
            return Ok(());
        }
        let ws_mgr = self
            .workspace_mgr
            .as_ref()
            .ok_or_else(|| AdapterError::msg("workspace manager not configured"))?;
        ws_mgr
            .set_catalog_isolated(catalog)
            .await
            .map_err(AdapterError::new)
    }

    async fn list_workspace_bindings(&self, catalog: &str) -> AdapterResult<Vec<(u64, String)>> {
        let ws_mgr = self
            .workspace_mgr
            .as_ref()
            .ok_or_else(|| AdapterError::msg("workspace manager not configured"))?;
        let bindings = ws_mgr
            .get_bindings(catalog)
            .await
            .map_err(AdapterError::new)?;
        Ok(bindings
            .into_iter()
            .map(|b| {
                let kind = b
                    .binding_type
                    .unwrap_or_else(|| "BINDING_TYPE_READ_WRITE".to_string());
                (b.workspace_id, kind)
            })
            .collect())
    }

    async fn remove_workspace_binding(
        &self,
        catalog: &str,
        workspace_id: u64,
    ) -> AdapterResult<()> {
        let ws_mgr = self
            .workspace_mgr
            .as_ref()
            .ok_or_else(|| AdapterError::msg("workspace manager not configured"))?;
        ws_mgr
            .update_bindings(
                catalog,
                vec![],
                vec![crate::workspace::WorkspaceBinding {
                    workspace_id,
                    binding_type: None,
                }],
            )
            .await
            .map_err(AdapterError::new)
    }

    async fn apply_column_tags(
        &self,
        table: &TableRef,
        column_tags: &BTreeMap<String, BTreeMap<String, String>>,
    ) -> AdapterResult<()> {
        if column_tags.is_empty() {
            return Ok(());
        }
        let mgr = CatalogManager::new(&self.connector);
        // One statement per column — Databricks rejects multi-column
        // ALTER COLUMN in a single DDL, so we can't coalesce.
        for (column, tags) in column_tags {
            mgr.set_column_tags(
                &table.catalog,
                &table.schema,
                table.table.as_str(),
                column,
                tags,
            )
            .await
            .map_err(AdapterError::new)?;
        }
        Ok(())
    }

    async fn reconcile_role_graph(
        &self,
        roles: &BTreeMap<String, ResolvedRole>,
    ) -> AdapterResult<()> {
        reconcile_role_graph_impl(&self.connector, roles).await
    }

    async fn apply_masking_policy(
        &self,
        table: &TableRef,
        policy: &MaskingPolicy,
        env: &str,
    ) -> AdapterResult<()> {
        if policy.is_empty() {
            return Ok(());
        }

        // Pass 1: ensure every distinct non-None strategy has a backing
        // function in the table's schema. CREATE OR REPLACE makes this
        // idempotent across repeated runs.
        let distinct_strategies: BTreeSet<MaskStrategy> = policy
            .column_strategies
            .values()
            .copied()
            .filter(|s| *s != MaskStrategy::None)
            .collect();

        for strategy in &distinct_strategies {
            let sql =
                masking::generate_create_mask_sql(&table.catalog, &table.schema, *strategy, env)
                    .map_err(AdapterError::new)?;
            if let Some(sql) = sql {
                debug!(
                    catalog = %table.catalog,
                    schema = %table.schema,
                    strategy = %strategy,
                    env = env,
                    "creating masking function"
                );
                self.connector
                    .execute_statement(&sql)
                    .await
                    .map_err(AdapterError::new)?;
            }
        }

        // Pass 2: bind each column to its strategy (or explicitly drop a
        // prior mask when the resolved strategy is None). Databricks has
        // no "IF EXISTS" form for DROP MASK, so a column without a prior
        // mask would error — but we only emit DROP when policy.None
        // explicitly overrides an otherwise-masked tag, which means a
        // prior run already set one.
        for (column, strategy) in &policy.column_strategies {
            let maybe_stmt = if *strategy == MaskStrategy::None {
                Some(
                    masking::generate_drop_mask_sql(
                        &table.catalog,
                        &table.schema,
                        table.table.as_str(),
                        column,
                    )
                    .map_err(AdapterError::new)?,
                )
            } else {
                masking::generate_set_mask_sql(
                    &table.catalog,
                    &table.schema,
                    table.table.as_str(),
                    column,
                    *strategy,
                    env,
                )
                .map_err(AdapterError::new)?
            };

            if let Some(sql) = maybe_stmt {
                debug!(
                    catalog = %table.catalog,
                    schema = %table.schema,
                    table = %table.table,
                    column = column.as_str(),
                    strategy = %strategy,
                    env = env,
                    "applying column mask"
                );
                self.connector
                    .execute_statement(&sql)
                    .await
                    .map_err(AdapterError::new)?;
            }
        }

        Ok(())
    }
}

/// Prefix Rocky prepends to every group it provisions for a role, to
/// keep its managed groups disjoint from user-created groups in the
/// Unity Catalog metastore.
///
/// `role.admin` -> `rocky_role_admin`. Changing this string is a
/// breaking change — existing grants in the wild reference the old
/// name.
const ROCKY_ROLE_GROUP_PREFIX: &str = "rocky_role_";

/// Compute the Databricks UC group name for a Rocky role.
///
/// See [`ROCKY_ROLE_GROUP_PREFIX`] for the naming convention. Exposed as
/// a free function so tests + downstream tooling can reason about the
/// mapping without instantiating an adapter.
pub fn role_group_name(role_name: &str) -> String {
    format!("{ROCKY_ROLE_GROUP_PREFIX}{role_name}")
}

/// v1 implementation of `reconcile_role_graph`.
///
/// Databricks / Unity Catalog does not have "roles" natively; Rocky
/// maps each role to a UC group (`rocky_role_<name>`). A fully-native
/// impl would:
///
/// 1. Create each group via the SCIM `POST /api/2.0/preview/scim/v2/Groups`
///    endpoint if missing.
/// 2. For every catalog Rocky manages, emit a `GRANT <permission> ON
///    CATALOG ... TO <group>` per flattened permission.
///
/// Neither piece exists in `rocky-databricks` yet — there's no SCIM
/// client, and the catalogs-to-apply set isn't plumbed through the
/// [`GovernanceAdapter::reconcile_role_graph`] signature. Both are
/// deliberate follow-ups (see `feat/governance-role-graph` PR body).
///
/// In v1 this function validates each group name against the principal
/// rules and emits a structured `debug` log of the reconciled role
/// graph. It returns `Ok(())` so pipelines that declare `[role.*]` in
/// their `rocky.toml` against a Databricks target don't abort — the
/// role graph is still flattened + validated by [`rocky_core::role_graph`]
/// at config-load time, which is where real failures surface.
async fn reconcile_role_graph_impl(
    _connector: &DatabricksConnector,
    roles: &BTreeMap<String, ResolvedRole>,
) -> AdapterResult<()> {
    if roles.is_empty() {
        return Ok(());
    }

    for (name, role) in roles {
        let group = role_group_name(name);
        // Validate the group name matches Databricks principal syntax
        // so the deferred GRANT-emission path can't silently generate
        // invalid SQL later.
        rocky_sql::validation::validate_principal(&group).map_err(AdapterError::new)?;

        debug!(
            role = name.as_str(),
            group = group.as_str(),
            inherits = ?role.inherits_from,
            permissions = ?role
                .flattened_permissions
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            "reconciled role graph entry (v1: log-only; SCIM group creation + per-catalog GRANT application are follow-ups)"
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::ir::Permission;

    #[test]
    fn role_group_name_prefixes_rocky_role() {
        assert_eq!(role_group_name("reader"), "rocky_role_reader");
        assert_eq!(
            role_group_name("analytics_engineer"),
            "rocky_role_analytics_engineer"
        );
    }

    #[tokio::test]
    async fn reconcile_role_graph_impl_accepts_empty_map() {
        // We don't actually use the connector in v1, but the signature
        // requires one. Construct a minimal one via the same test helpers
        // that wiremock_tests uses — except we never make a network call,
        // so the URL/token values are fake.
        let auth = crate::auth::Auth::from_config(crate::auth::AuthConfig {
            host: "example.databricks.com".into(),
            token: Some("fake".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let config = crate::connector::ConnectorConfig {
            host: "example.databricks.com".into(),
            warehouse_id: "w".into(),
            timeout: std::time::Duration::from_secs(1),
            retry: rocky_core::config::RetryConfig::default(),
        };
        let connector = crate::connector::DatabricksConnector::new(config, auth);

        let empty = BTreeMap::new();
        assert!(reconcile_role_graph_impl(&connector, &empty).await.is_ok());
    }

    #[tokio::test]
    async fn reconcile_role_graph_impl_accepts_valid_roles() {
        let auth = crate::auth::Auth::from_config(crate::auth::AuthConfig {
            host: "example.databricks.com".into(),
            token: Some("fake".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let config = crate::connector::ConnectorConfig {
            host: "example.databricks.com".into(),
            warehouse_id: "w".into(),
            timeout: std::time::Duration::from_secs(1),
            retry: rocky_core::config::RetryConfig::default(),
        };
        let connector = crate::connector::DatabricksConnector::new(config, auth);

        let mut roles = BTreeMap::new();
        roles.insert(
            "reader".to_string(),
            ResolvedRole {
                name: "reader".into(),
                flattened_permissions: vec![Permission::Select, Permission::UseCatalog],
                inherits_from: vec![],
            },
        );
        roles.insert(
            "admin".to_string(),
            ResolvedRole {
                name: "admin".into(),
                flattened_permissions: vec![
                    Permission::UseCatalog,
                    Permission::Select,
                    Permission::Manage,
                ],
                inherits_from: vec!["reader".to_string()],
            },
        );
        assert!(reconcile_role_graph_impl(&connector, &roles).await.is_ok());
    }

    #[tokio::test]
    async fn reconcile_role_graph_impl_rejects_invalid_group_name() {
        // A role name with characters outside the principal regex (e.g.
        // backtick or semicolon) would produce an invalid UC group name.
        // The v1 impl catches this via validate_principal so the deferred
        // GRANT path can't generate invalid SQL later.
        let auth = crate::auth::Auth::from_config(crate::auth::AuthConfig {
            host: "example.databricks.com".into(),
            token: Some("fake".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let config = crate::connector::ConnectorConfig {
            host: "example.databricks.com".into(),
            warehouse_id: "w".into(),
            timeout: std::time::Duration::from_secs(1),
            retry: rocky_core::config::RetryConfig::default(),
        };
        let connector = crate::connector::DatabricksConnector::new(config, auth);

        let mut roles = BTreeMap::new();
        roles.insert(
            "bad;name".to_string(),
            ResolvedRole {
                name: "bad;name".into(),
                flattened_permissions: vec![Permission::Select],
                inherits_from: vec![],
            },
        );
        assert!(reconcile_role_graph_impl(&connector, &roles).await.is_err());
    }
}
