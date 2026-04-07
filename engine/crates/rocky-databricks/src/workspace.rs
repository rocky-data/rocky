use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::auth::Auth;

#[derive(Debug, thiserror::Error)]
pub enum WorkspaceError {
    #[error("auth error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("API error {status}: {body}")]
    ApiError { status: u16, body: String },
}

/// Manages Databricks workspace bindings for catalog isolation.
pub struct WorkspaceManager {
    host: String,
    auth: Auth,
    client: Client,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceBinding {
    pub workspace_id: u64,
    #[serde(default)]
    pub binding_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BindingsResponse {
    #[serde(default)]
    bindings: Vec<WorkspaceBinding>,
}

#[derive(Debug, Serialize)]
struct UpdateBindingsRequest {
    #[serde(skip_serializing_if = "Vec::is_empty")]
    add: Vec<WorkspaceBinding>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    remove: Vec<WorkspaceBinding>,
}

#[derive(Debug, Serialize)]
struct SetIsolationRequest {
    isolation_mode: String,
}

impl WorkspaceManager {
    pub fn new(host: String, auth: Auth) -> Self {
        WorkspaceManager {
            host,
            auth,
            client: Client::new(),
        }
    }

    /// Gets current workspace bindings for a catalog.
    pub async fn get_bindings(
        &self,
        catalog: &str,
    ) -> Result<Vec<WorkspaceBinding>, WorkspaceError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "https://{}/api/2.1/unity-catalog/bindings/catalog/{catalog}",
            self.host
        );

        let resp = self.client.get(&url).bearer_auth(&token).send().await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkspaceError::ApiError { status, body });
        }

        let bindings: BindingsResponse = resp.json().await?;
        Ok(bindings.bindings)
    }

    /// Updates workspace bindings (add/remove).
    pub async fn update_bindings(
        &self,
        catalog: &str,
        add: Vec<WorkspaceBinding>,
        remove: Vec<WorkspaceBinding>,
    ) -> Result<(), WorkspaceError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "https://{}/api/2.1/unity-catalog/bindings/catalog/{catalog}",
            self.host
        );

        debug!(
            catalog,
            add = add.len(),
            remove = remove.len(),
            "updating workspace bindings"
        );

        let body = UpdateBindingsRequest { add, remove };

        let resp = self
            .client
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkspaceError::ApiError { status, body });
        }

        Ok(())
    }

    /// Sets a catalog's isolation mode to ISOLATED.
    pub async fn set_catalog_isolated(&self, catalog: &str) -> Result<(), WorkspaceError> {
        let token = self.auth.get_token().await?;
        let url = format!(
            "https://{}/api/2.1/unity-catalog/catalogs/{catalog}",
            self.host
        );

        debug!(catalog, "setting catalog isolation mode");

        let body = SetIsolationRequest {
            isolation_mode: "ISOLATED".to_string(),
        };

        let resp = self
            .client
            .patch(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(WorkspaceError::ApiError { status, body });
        }

        Ok(())
    }

    /// Adds a single workspace binding with the specified access level.
    ///
    /// `binding_type` is the Databricks API string, e.g. `"BINDING_TYPE_READ_WRITE"`
    /// or `"BINDING_TYPE_READ_ONLY"`.
    pub async fn bind_workspace(
        &self,
        catalog: &str,
        workspace_id: u64,
        binding_type: &str,
    ) -> Result<(), WorkspaceError> {
        self.update_bindings(
            catalog,
            vec![WorkspaceBinding {
                workspace_id,
                binding_type: Some(binding_type.to_string()),
            }],
            vec![],
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workspace_binding_serialize() {
        let binding = WorkspaceBinding {
            workspace_id: 12345,
            binding_type: Some("BINDING_TYPE_READ_WRITE".into()),
        };
        let json = serde_json::to_string(&binding).unwrap();
        assert!(json.contains("12345"));
        assert!(json.contains("BINDING_TYPE_READ_WRITE"));
    }

    #[test]
    fn test_update_request_serialize() {
        let req = UpdateBindingsRequest {
            add: vec![WorkspaceBinding {
                workspace_id: 123,
                binding_type: Some("BINDING_TYPE_READ_WRITE".into()),
            }],
            remove: vec![],
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("add"));
        assert!(!json.contains("remove")); // empty vec skipped
    }

    #[test]
    fn test_isolation_request_serialize() {
        let req = SetIsolationRequest {
            isolation_mode: "ISOLATED".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        assert_eq!(json, r#"{"isolation_mode":"ISOLATED"}"#);
    }
}
