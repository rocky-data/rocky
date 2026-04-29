//! Databricks SCIM 2.0 client for workspace-level group management.
//!
//! Wraps the `/api/2.0/preview/scim/v2/Groups` surface so Rocky can
//! provision `rocky_role_*` groups that back
//! [`crate::governance::DatabricksGovernanceAdapter::reconcile_role_graph`].
//! Groups are the Unity Catalog primitive Rocky GRANTs permissions to
//! when a user declares `[role.*]` in their `rocky.toml`.
//!
//! ## Scope
//!
//! This module is deliberately minimal — just the two operations
//! `reconcile_role_graph` needs today:
//!
//! - [`ScimClient::create_group`] — idempotent create. POSTs to the
//!   SCIM `/Groups` endpoint; on `409 Conflict` falls back to a
//!   `GET /Groups?filter=displayName eq "<name>"` and returns the
//!   existing group. No retry loop on transient errors — SCIM sits
//!   behind the same auth + HTTP stack as the SQL connector, but the
//!   role-graph reconcile is best-effort in `rocky run` and the caller
//!   logs-and-moves-on.
//! - [`ScimClient::get_group_by_name`] — lookup-by-displayName, reused
//!   by the 409 fallback and exposed for completeness.
//!
//! ## Not yet supported (ADD-ONLY v1)
//!
//! The v1 role-graph reconciler never removes groups: if a role is
//! dropped from `rocky.toml`, the corresponding `rocky_role_*` group
//! stays around with whatever grants it had. Group deletion (`DELETE
//! /Groups/{id}`) is a follow-up when reconcile gets a delete mode;
//! until then, operators clean up unused groups by hand. This keeps
//! the first-ship semantics predictable — Rocky only ever makes groups
//! appear, never disappear.
//!
//! ## Auth
//!
//! The SCIM endpoint accepts the same PAT / OAuth M2M bearer token the
//! SQL Statement Execution API uses — [`crate::auth::Auth`] handles
//! both transparently. No separate SCIM auth path.

use std::time::Duration;

use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::auth::Auth;

/// Build the shared `reqwest::Client` used for every SCIM call.
/// `Client::new()` left both connect and request timeouts unset — a
/// stalled connection would block any SCIM-using path (group sync,
/// principal lookups) forever.
fn build_http_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()
        .expect("reqwest client builder is infallible with these options")
}

/// Errors from the Databricks SCIM API.
#[derive(Debug, thiserror::Error)]
pub enum ScimError {
    #[error("auth error: {0}")]
    Auth(#[from] crate::auth::AuthError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("SCIM API error {status}: {body}")]
    ApiError { status: u16, body: String },

    #[error("SCIM response missing field: {0}")]
    MissingField(&'static str),
}

/// Minimal SCIM 2.0 Group representation as returned by Databricks.
///
/// We only surface `id` + `display_name` — the two fields downstream
/// code needs. Extra fields on the wire (`schemas`, `members`, `meta`)
/// are ignored via serde's default leniency on `Deserialize`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct ScimGroup {
    pub id: String,
    #[serde(rename = "displayName")]
    pub display_name: String,
}

/// SCIM 2.0 Groups core schema URI. Databricks accepts requests without
/// this alongside `displayName`, but strict SCIM servers reject bodies
/// without a `schemas` array — include it for portability.
const SCIM_GROUP_SCHEMA: &str = "urn:ietf:params:scim:schemas:core:2.0:Group";

#[derive(Debug, Serialize)]
struct CreateGroupRequest<'a> {
    schemas: [&'static str; 1],
    #[serde(rename = "displayName")]
    display_name: &'a str,
}

/// SCIM `ListResponse` envelope used by the filter query endpoint.
#[derive(Debug, Deserialize)]
struct ListResponse {
    #[serde(rename = "Resources", default)]
    resources: Vec<ScimGroup>,
}

/// Databricks SCIM 2.0 client.
///
/// Cheap to construct (reqwest::Client / Auth are `Arc`-backed internally).
/// Wire-level retry / throttling is not layered in here — SCIM calls are
/// infrequent (one per role per `rocky run`) and the reconcile caller
/// treats failures as best-effort.
pub struct ScimClient {
    host: String,
    auth: Auth,
    client: Client,
    /// Override for the base URL scheme + host (used by tests to point at wiremock).
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

impl ScimClient {
    /// Construct a new SCIM client bound to `host` (e.g. `acme.cloud.databricks.com`)
    /// using the shared [`Auth`] provider.
    pub fn new(host: String, auth: Auth) -> Self {
        ScimClient {
            host,
            auth,
            client: build_http_client(),
            #[cfg(any(test, feature = "test-support"))]
            base_url_override: None,
        }
    }

    /// Overrides the base URL for API calls (for testing with wiremock).
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn with_base_url(mut self, base_url: String) -> Self {
        self.base_url_override = Some(base_url);
        self
    }

    fn api_base_url(&self) -> String {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(url) = &self.base_url_override {
            return url.clone();
        }
        format!("https://{}", self.host)
    }

    fn groups_url(&self) -> String {
        format!("{}/api/2.0/preview/scim/v2/Groups", self.api_base_url())
    }

    /// Idempotent group create.
    ///
    /// POST `/Groups` with `{"schemas": [...], "displayName": "<name>"}`.
    /// On `201 Created` the response body is the created group. On
    /// `409 Conflict` — the server's signal that a group with this
    /// `displayName` already exists — fall back to
    /// [`Self::get_group_by_name`] and return the existing group.
    ///
    /// Racy concurrent reconcilers can hit the POST-then-GET path: one
    /// caller creates, the other gets 409 and looks up. Both end up
    /// with the same group. This is why we POST-first rather than
    /// GET-first — POST-first avoids a TOCTOU window where two callers
    /// both GET nothing, both POST, and one eats a 409 anyway.
    ///
    /// # Errors
    ///
    /// - [`ScimError::Auth`] when the OAuth / PAT exchange fails.
    /// - [`ScimError::Http`] on transport errors.
    /// - [`ScimError::ApiError`] on any non-201 / non-409 response, or
    ///   a 409 whose follow-up GET didn't turn up a matching group.
    /// - [`ScimError::MissingField`] if the server returned 409 but
    ///   the subsequent lookup came back empty (should not happen
    ///   under normal SCIM semantics but is surfaced explicitly
    ///   rather than silently papered over).
    pub async fn create_group(&self, display_name: &str) -> Result<ScimGroup, ScimError> {
        let token = self.auth.get_token().await?;
        let url = self.groups_url();

        debug!(display_name, "SCIM create group");

        let body = CreateGroupRequest {
            schemas: [SCIM_GROUP_SCHEMA],
            display_name,
        };

        let resp = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        let status = resp.status();
        if status.is_success() {
            let group: ScimGroup = resp.json().await?;
            return Ok(group);
        }

        if status.as_u16() == 409 {
            // Group already exists — recover by looking it up.
            debug!(
                display_name,
                "SCIM create returned 409; looking up existing group"
            );
            return self
                .get_group_by_name(display_name)
                .await?
                .ok_or(ScimError::MissingField("Resources[0] on 409 fallback"));
        }

        let body = resp.text().await.unwrap_or_default();
        Err(ScimError::ApiError {
            status: status.as_u16(),
            body,
        })
    }

    /// Look up a group by its `displayName`.
    ///
    /// GET `/Groups?filter=displayName eq "<name>"`. Returns `Ok(None)`
    /// when the server responds with an empty `Resources` list — not
    /// an error, because the common use case is "does this group
    /// already exist?"
    ///
    /// # Errors
    ///
    /// - [`ScimError::Auth`] / [`ScimError::Http`] / [`ScimError::ApiError`]
    ///   same as [`Self::create_group`].
    pub async fn get_group_by_name(
        &self,
        display_name: &str,
    ) -> Result<Option<ScimGroup>, ScimError> {
        let token = self.auth.get_token().await?;
        // SCIM filter syntax: `displayName eq "<name>"`. The inner
        // quotes are part of the filter value; reqwest's query encoder
        // percent-escapes them.
        let filter = format!("displayName eq \"{display_name}\"");
        let url = self.groups_url();

        debug!(display_name, filter = %filter, "SCIM get group by name");

        let resp = self
            .client
            .get(&url)
            .bearer_auth(&token)
            .query(&[("filter", filter.as_str())])
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(ScimError::ApiError { status, body });
        }

        let list: ListResponse = resp.json().await?;
        Ok(list.resources.into_iter().next())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_group_request_serializes_with_schemas() {
        // The SCIM 2.0 spec requires `schemas` on every resource
        // representation. Verify our body carries it alongside
        // `displayName` so servers that strictly validate the schemas
        // envelope accept the request.
        let body = CreateGroupRequest {
            schemas: [SCIM_GROUP_SCHEMA],
            display_name: "rocky_role_reader",
        };
        let json = serde_json::to_value(&body).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
                "displayName": "rocky_role_reader"
            })
        );
    }

    #[test]
    fn scim_group_deserializes_ignoring_unknown_fields() {
        // Real SCIM responses carry `schemas`, `meta`, `members`, etc.
        // Our minimal `ScimGroup` only cares about id + displayName;
        // serde's default leniency must skip the rest.
        let json = r#"{
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "id": "123456",
            "displayName": "rocky_role_reader",
            "members": [],
            "meta": {"resourceType": "Group"}
        }"#;
        let group: ScimGroup = serde_json::from_str(json).unwrap();
        assert_eq!(group.id, "123456");
        assert_eq!(group.display_name, "rocky_role_reader");
    }

    #[test]
    fn list_response_deserializes_resources_capital_r() {
        // Databricks follows the SCIM 2.0 spec and emits `Resources`
        // (capital R). The list envelope's field rename must match.
        let json = r#"{
            "totalResults": 1,
            "Resources": [
                {"id": "abc", "displayName": "rocky_role_admin"}
            ]
        }"#;
        let list: ListResponse = serde_json::from_str(json).unwrap();
        assert_eq!(list.resources.len(), 1);
        assert_eq!(list.resources[0].id, "abc");
    }

    #[test]
    fn list_response_deserializes_empty() {
        // No matching group — `Resources` is absent or empty. Default
        // serde behaviour (our `#[serde(default)]`) should yield an
        // empty vec rather than erroring.
        let json = r#"{"totalResults": 0}"#;
        let list: ListResponse = serde_json::from_str(json).unwrap();
        assert!(list.resources.is_empty());
    }

    #[test]
    fn groups_url_uses_preview_scim_v2_path() {
        // Pin the endpoint path. If Databricks ever promotes SCIM out
        // of /preview, this test will catch the migration.
        let auth = Auth::from_config(crate::auth::AuthConfig {
            host: "example.databricks.com".into(),
            token: Some("fake".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let client = ScimClient::new("example.databricks.com".into(), auth);
        assert_eq!(
            client.groups_url(),
            "https://example.databricks.com/api/2.0/preview/scim/v2/Groups"
        );
    }
}
