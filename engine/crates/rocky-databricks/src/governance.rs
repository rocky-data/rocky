//! Databricks governance adapter implementing [`GovernanceAdapter`].
//!
//! Consolidates catalog management, permission reconciliation, and workspace
//! isolation behind a single trait interface.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use rocky_core::ir::{Grant, GrantTarget, PermissionDiff};
use rocky_core::traits::{AdapterError, AdapterResult, GovernanceAdapter, TagTarget};

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
}
