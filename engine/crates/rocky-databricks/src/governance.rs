//! Databricks governance adapter implementing [`GovernanceAdapter`].
//!
//! Consolidates catalog management, permission reconciliation, workspace
//! isolation, column-level classification tags, and column masking policies
//! behind a single trait interface.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;
use tracing::{debug, warn};

use rocky_core::ir::{Grant, GrantTarget, Permission, PermissionDiff, ResolvedRole, TableRef};
use rocky_core::masking;
use rocky_core::retention::RetentionPolicy;
use rocky_core::traits::{
    AdapterError, AdapterResult, GovernanceAdapter, MaskStrategy, MaskingPolicy, TagTarget,
};

use crate::auth::Auth;
use crate::catalog::CatalogManager;
use crate::connector::DatabricksConnector;
use crate::permissions::PermissionManager;
use crate::scim::ScimClient;
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
    /// SCIM client for provisioning `rocky_role_*` groups during
    /// [`Self::reconcile_role_graph`]. `None` when the adapter was
    /// constructed via [`Self::without_workspace`] — SCIM shares the
    /// workspace-level host + auth path, so the two configurations
    /// naturally travel together. A `None` SCIM client falls back to
    /// log-only reconcile so pipelines configured without SCIM don't
    /// hard-fail on `[role.*]`.
    scim: Option<ScimClient>,
}

impl DatabricksGovernanceAdapter {
    pub fn new(connector: Arc<DatabricksConnector>, host: &str, auth: Auth) -> Self {
        Self {
            connector,
            workspace_mgr: Some(WorkspaceManager::new(host.to_string(), auth.clone())),
            scim: Some(ScimClient::new(host.to_string(), auth)),
        }
    }

    /// Create without workspace manager or SCIM client (for environments
    /// without isolation). Role-graph reconcile falls back to log-only
    /// under this configuration — same semantics as v1.
    pub fn without_workspace(connector: Arc<DatabricksConnector>) -> Self {
        Self {
            connector,
            workspace_mgr: None,
            scim: None,
        }
    }

    /// Override the SCIM client — used by wiremock tests to point at a
    /// mock server without instantiating a full `Auth` stack behind a
    /// real host.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_scim_client(mut self, scim: ScimClient) -> Self {
        self.scim = Some(scim);
        self
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
        catalogs: &[&str],
    ) -> AdapterResult<()> {
        reconcile_role_graph_impl(&self.connector, self.scim.as_ref(), roles, catalogs).await
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

    async fn apply_retention_policy(
        &self,
        table: &TableRef,
        retention: &RetentionPolicy,
    ) -> AdapterResult<()> {
        // Delta Lake uses a pair of TBLPROPERTIES for time-travel retention:
        // `delta.logRetentionDuration` governs how far back the version
        // history is readable, and `delta.deletedFileRetentionDuration`
        // governs when tombstoned files become VACUUM-eligible. Both are
        // set in a single ALTER TABLE so the pair stays consistent.
        let mgr = CatalogManager::new(&self.connector);
        mgr.set_delta_retention(
            &table.catalog,
            &table.schema,
            table.table.as_str(),
            retention.duration_days,
        )
        .await
        .map_err(AdapterError::new)
    }

    async fn read_retention_days(&self, table: &TableRef) -> AdapterResult<Option<u32>> {
        // Reads the same Delta TBLPROPERTIES pair that `apply_retention_policy`
        // writes (`delta.logRetentionDuration` +
        // `delta.deletedFileRetentionDuration`) via `SHOW TBLPROPERTIES`.
        // The probe is filtered to those two keys SQL-side so Delta doesn't
        // stream the full property set back; the parse is tolerant of the
        // runtime-version differences in value formatting (see
        // [`crate::catalog::CatalogManager::get_delta_retention_days`]).
        let mgr = CatalogManager::new(&self.connector);
        mgr.get_delta_retention_days(&table.catalog, &table.schema, table.table.as_str())
            .await
            .map_err(AdapterError::new)
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

/// Role-graph reconciliation for Databricks / Unity Catalog.
///
/// Databricks / Unity Catalog has no native "role" primitive; Rocky
/// maps each role to a UC group named `rocky_role_<name>`. The reconcile
/// is a two-pass operation, intentionally split so SCIM creates fire
/// once per role regardless of how many catalogs are in scope:
///
/// 1. **Group creation (catalog-independent).** For every `role` in
///    `roles`, call [`ScimClient::create_group`] with the mapped
///    `rocky_role_<name>` display name. Idempotent: a 409 Conflict
///    returns the existing group via a GET lookup fallback.
/// 2. **GRANT emission (per-catalog).** For every `(role, catalog,
///    permission)` triple, execute `GRANT <permission> ON CATALOG
///    <catalog> TO `rocky_role_<name>`` via the SQL Statement Execution
///    API. Idempotent at the warehouse — Databricks treats a GRANT on an
///    already-held permission as a no-op, so repeat runs are safe.
///
/// ## ADD-ONLY v1
///
/// Groups are never deleted and grants are never revoked by this path.
/// A role removed from `rocky.toml` leaves its `rocky_role_*` group +
/// grants in place; operators must clean up manually until a future
/// reconcile mode adds delete semantics. The decision is deliberate:
/// reversible creates are safer than accidental deletes while the
/// feature stabilises, and the same reasoning applies to the GRANT
/// side (the explicit-revoke path runs through
/// [`PermissionManager::apply_diff`], not this reconciler).
///
/// ## Fallback when SCIM is unavailable
///
/// If `scim` is `None` (e.g. the adapter was constructed via
/// [`DatabricksGovernanceAdapter::without_workspace`]), the reconciler
/// falls back to v1 log-only behaviour: validate names + emit a
/// `debug!` event, then return `Ok(())`. Same rationale as before —
/// pipelines targeting a Databricks endpoint without SCIM configured
/// shouldn't hard-fail on `[role.*]`.
///
/// ## Empty `catalogs`
///
/// SCIM group creation still fires (groups are a workspace-level
/// concept, independent of Unity Catalog); zero GRANTs are emitted.
/// Documented in [`GovernanceAdapter::reconcile_role_graph`].
async fn reconcile_role_graph_impl(
    connector: &DatabricksConnector,
    scim: Option<&ScimClient>,
    roles: &BTreeMap<String, ResolvedRole>,
    catalogs: &[&str],
) -> AdapterResult<()> {
    if roles.is_empty() {
        return Ok(());
    }

    // Pass 0: validate group names up-front so the whole reconcile
    // aborts on a bad principal syntax before any network calls fire.
    // Same contract as v1.
    let mut group_names: BTreeMap<&String, String> = BTreeMap::new();
    for name in roles.keys() {
        let group = role_group_name(name);
        rocky_sql::validation::validate_principal(&group).map_err(AdapterError::new)?;
        group_names.insert(name, group);
    }

    // Log-only fallback when SCIM isn't configured (matches v1 semantics).
    let Some(scim) = scim else {
        for (name, role) in roles {
            let group = &group_names[name];
            debug!(
                role = name.as_str(),
                group = group.as_str(),
                inherits = ?role.inherits_from,
                permissions = ?role
                    .flattened_permissions
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                "reconciled role graph entry (log-only; SCIM client not configured)"
            );
        }
        return Ok(());
    };

    // Pass 1: SCIM group creation. Best-effort per role — a failure on
    // one role warns-and-continues rather than aborting the whole
    // reconcile (the caller already treats this as best-effort, and we
    // don't want one flaky SCIM request to starve the others).
    for (name, role) in roles {
        let group = &group_names[name];
        match scim.create_group(group).await {
            Ok(g) => {
                debug!(
                    role = name.as_str(),
                    group = g.display_name.as_str(),
                    group_id = g.id.as_str(),
                    inherits = ?role.inherits_from,
                    permissions = ?role
                        .flattened_permissions
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>(),
                    "reconciled role graph entry — SCIM group ensured"
                );
            }
            Err(e) => {
                warn!(
                    role = name.as_str(),
                    group = group.as_str(),
                    error = %e,
                    "SCIM group create failed; skipping GRANTs for this role"
                );
                // Skip grants for this role — granting against a missing
                // group would just fail at the warehouse anyway.
                continue;
            }
        }
    }

    // Pass 2: per-catalog GRANT emission. One statement per
    // `(role, catalog, permission)` triple. Databricks rejects multi-
    // permission GRANT-ALL in the same SQL when the permission set is
    // heterogeneous, so we keep it one-statement-at-a-time for
    // simplicity — repeat GRANTs are no-ops on the warehouse side.
    //
    // `catalogs` can be empty (no managed catalogs this run, or an
    // early-return from the caller). That's a valid state; groups are
    // created regardless, no grants fire.
    for (name, role) in roles {
        let group = &group_names[name];
        for catalog in catalogs {
            for permission in &role.flattened_permissions {
                let sql =
                    format_role_grant_sql(catalog, permission, group).map_err(AdapterError::new)?;
                debug!(
                    role = name.as_str(),
                    group = group.as_str(),
                    catalog = *catalog,
                    permission = %permission,
                    "emitting role-graph GRANT"
                );
                if let Err(e) = connector.execute_statement(&sql).await {
                    // Best-effort per statement — log and keep going so
                    // one flaky GRANT doesn't prevent the rest from
                    // applying. The outer caller in `rocky run` is
                    // already in "warn-and-continue" mode for the whole
                    // reconcile.
                    warn!(
                        role = name.as_str(),
                        group = group.as_str(),
                        catalog = *catalog,
                        permission = %permission,
                        error = %e,
                        "role-graph GRANT failed"
                    );
                }
            }
        }
    }

    Ok(())
}

/// Format a role-graph GRANT. Kept separate from
/// [`crate::permissions::format_grant_sql`] because the principal here
/// is a `rocky_role_*` group (already validated by
/// [`role_group_name`] + [`rocky_sql::validation::validate_principal`])
/// and the target is always a catalog — we don't go through the full
/// [`Grant`] struct / [`GrantTarget`] enum.
fn format_role_grant_sql(
    catalog: &str,
    permission: &Permission,
    group: &str,
) -> Result<String, rocky_sql::validation::ValidationError> {
    rocky_sql::validation::validate_identifier(catalog)?;
    let principal = rocky_sql::validation::format_principal(group)?;
    Ok(format!(
        "GRANT {permission} ON CATALOG {catalog} TO {principal}"
    ))
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

    #[test]
    fn format_role_grant_sql_happy_path() {
        // Pin the exact SQL shape — role-graph GRANTs backtick-quote
        // the group principal + quote-free identifier for the catalog.
        let sql = format_role_grant_sql("acme_warehouse", &Permission::Select, "rocky_role_reader")
            .unwrap();
        assert_eq!(
            sql,
            "GRANT SELECT ON CATALOG acme_warehouse TO `rocky_role_reader`"
        );
    }

    #[test]
    fn format_role_grant_sql_rejects_invalid_catalog() {
        // A catalog containing characters outside the identifier regex
        // must fail validation before we interpolate it into SQL.
        assert!(
            format_role_grant_sql("bad;catalog", &Permission::Select, "rocky_role_reader").is_err()
        );
    }

    #[test]
    fn format_role_grant_sql_emits_use_catalog_with_space() {
        // USE CATALOG renders as "USE CATALOG" (with space) via the
        // Permission Display impl. Confirm the formatter doesn't mangle
        // it into e.g. "USE_CATALOG".
        let sql = format_role_grant_sql("wh", &Permission::UseCatalog, "rocky_role_admin").unwrap();
        assert_eq!(sql, "GRANT USE CATALOG ON CATALOG wh TO `rocky_role_admin`");
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
        assert!(
            reconcile_role_graph_impl(&connector, None, &empty, &[])
                .await
                .is_ok()
        );
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
        // No SCIM client + no catalogs → log-only fallback branch.
        assert!(
            reconcile_role_graph_impl(&connector, None, &roles, &[])
                .await
                .is_ok()
        );
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
        assert!(
            reconcile_role_graph_impl(&connector, None, &roles, &[])
                .await
                .is_err()
        );
    }
}
