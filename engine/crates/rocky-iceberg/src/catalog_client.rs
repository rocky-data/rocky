//! [`CatalogClient`] implementation backed by the Iceberg REST catalog.
//!
//! [`IcebergCatalogClientAdapter`] composes an [`IcebergCatalogClient`]
//! and projects its surface onto the catalog-agnostic
//! [`rocky_catalog_core::CatalogClient`] trait. The trait surface splits
//! cleanly along Iceberg REST availability:
//!
//! - [`CatalogClient::list_tables`] routes to the existing
//!   [`IcebergCatalogClient::list_tables`].
//! - [`CatalogClient::describe_table`] performs a fresh
//!   `GET /v1/namespaces/{ns}/tables/{name}` via
//!   [`IcebergCatalogClient::load_table`] and distills the current
//!   schema down to a [`TableSchema`].
//! - [`CatalogClient::create_table`], [`CatalogClient::drop_table`],
//!   [`CatalogClient::commit_transaction`] and
//!   [`CatalogClient::list_branches`] currently return
//!   [`CatalogError::UnsupportedOperation`]; they will be wired up in a
//!   follow-up that adds POST/DELETE HTTP support and full
//!   `TableMetadata` round-trips.
//! - The four governance methods ([`CatalogClient::tag_table`],
//!   [`CatalogClient::get_grants`], [`CatalogClient::apply_grant`],
//!   [`CatalogClient::revoke_grant`]) return
//!   [`CatalogError::UnsupportedOperation`] unconditionally — the
//!   Iceberg REST spec exposes no governance endpoints, so the gap is
//!   permanent rather than "not yet".

use async_trait::async_trait;

use rocky_catalog_core::{
    BranchRef, CatalogClient, CatalogError, CatalogResult, ColumnSchema, Grant, TableCommit,
    TableRef, TableSchema,
};

use crate::client::{IcebergCatalogClient, IcebergError};

/// [`CatalogClient`] implementation that delegates to an
/// [`IcebergCatalogClient`].
///
/// The adapter is single-catalog: the [`TableRef::catalog`] field is
/// ignored because the underlying Iceberg REST client is bound to a
/// fixed base URL and has no multi-catalog routing. Multi-catalog
/// support would belong on a higher-level facade rather than this
/// adapter.
pub struct IcebergCatalogClientAdapter {
    inner: IcebergCatalogClient,
}

impl IcebergCatalogClientAdapter {
    /// Wrap an existing [`IcebergCatalogClient`].
    pub fn new(inner: IcebergCatalogClient) -> Self {
        Self { inner }
    }

    /// Borrow the underlying [`IcebergCatalogClient`].
    pub fn inner(&self) -> &IcebergCatalogClient {
        &self.inner
    }
}

impl std::fmt::Debug for IcebergCatalogClientAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IcebergCatalogClientAdapter")
            .field("inner", &self.inner)
            .finish()
    }
}

#[async_trait]
impl CatalogClient for IcebergCatalogClientAdapter {
    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableRef>> {
        // The trait carries the spec-native multi-part namespace; the
        // existing client takes a dot-joined string. Join here so the
        // existing percent-encoding path (which `.split('.')` first)
        // continues to work.
        let ns_joined = namespace.join(".");
        let table_ids = self
            .inner
            .list_tables(&ns_joined)
            .await
            // 404 on a namespace-scoped endpoint means the namespace is
            // gone, not a table; rewrite the default (table-targeted)
            // mapping accordingly.
            .map_err(|e| match icebergerror_into_catalog(e) {
                CatalogError::TableNotFound(msg) => CatalogError::NamespaceNotFound(msg),
                other => other,
            })?;

        let mut out = Vec::with_capacity(table_ids.len());
        for id in table_ids {
            out.push(TableRef {
                catalog: None,
                namespace: id.namespace.split('.').map(str::to_owned).collect(),
                name: id.name,
            });
        }
        Ok(out)
    }

    async fn describe_table(&self, table: &TableRef) -> CatalogResult<TableSchema> {
        let resp = self
            .inner
            .load_table(&table.namespace, &table.name)
            .await
            .map_err(icebergerror_into_catalog)?;

        let metadata = resp.metadata;
        let current_id = metadata.current_schema_id;

        // Spec: pick the schemas entry whose `schema-id` matches
        // `current-schema-id`. Defensive fallback to the last schema in
        // the vec keeps us robust if a catalog returns slightly off
        // metadata; an empty `schemas` array is a hard error because
        // there is nothing to describe.
        let schema = metadata
            .schemas
            .iter()
            .find(|s| s.schema_id == current_id)
            .or_else(|| metadata.schemas.last())
            .ok_or_else(|| {
                CatalogError::InvalidResponse(format!(
                    "load_table response for {} carries no schemas",
                    format_table(table)
                ))
            })?;

        let columns = schema
            .fields
            .iter()
            .map(|f| ColumnSchema {
                name: f.name.clone(),
                type_str: render_type(&f.type_repr),
                nullable: !f.required,
            })
            .collect();

        Ok(TableSchema { columns })
    }

    async fn create_table(&self, _table: &TableRef, _schema: &TableSchema) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "create_table not yet implemented in IcebergCatalogClientAdapter; tracked for a follow-up that adds POST support",
        ))
    }

    async fn drop_table(&self, _table: &TableRef) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "drop_table not yet implemented in IcebergCatalogClientAdapter; tracked for a follow-up that adds DELETE support",
        ))
    }

    async fn commit_transaction(&self, _commits: &[TableCommit]) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "commit_transaction not yet implemented in IcebergCatalogClientAdapter; tracked for a follow-up that adds POST + TableMetadata round-trips",
        ))
    }

    async fn list_branches(&self, _table: &TableRef) -> CatalogResult<Vec<BranchRef>> {
        Err(CatalogError::UnsupportedOperation(
            "list_branches not yet implemented in IcebergCatalogClientAdapter; tracked for a follow-up that surfaces SnapshotReference entries from TableMetadata",
        ))
    }

    async fn tag_table(&self, _table: &TableRef, _key: &str, _value: &str) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "Iceberg REST spec exposes no tagging endpoint",
        ))
    }

    async fn get_grants(&self, _table: &TableRef) -> CatalogResult<Vec<Grant>> {
        Err(CatalogError::UnsupportedOperation(
            "Iceberg REST spec exposes no governance endpoints",
        ))
    }

    async fn apply_grant(&self, _table: &TableRef, _grant: &Grant) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "Iceberg REST spec exposes no governance endpoints",
        ))
    }

    async fn revoke_grant(&self, _table: &TableRef, _grant: &Grant) -> CatalogResult<()> {
        Err(CatalogError::UnsupportedOperation(
            "Iceberg REST spec exposes no governance endpoints",
        ))
    }
}

/// Render an Iceberg field-type payload as a flat string.
///
/// Iceberg's spec lets a column's `type` be either a primitive type name
/// (`"long"`, `"timestamp"`, `"string"`) or a nested JSON object that
/// describes a list / map / struct. The catalog-agnostic
/// [`ColumnSchema::type_str`] is a string, so we take whichever shape
/// arrived: bare strings pass through as-is, anything else is
/// serialised back to JSON.
fn render_type(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// Map an [`IcebergError`] onto the catalog-agnostic [`CatalogError`].
///
/// Rules:
///
/// - HTTP 404 → [`CatalogError::TableNotFound`] (the table-targeted
///   default; namespace-scoped call sites are expected to rewrite this
///   to [`CatalogError::NamespaceNotFound`] in a `.map_err()` after
///   conversion).
/// - HTTP 401 → [`CatalogError::AuthFailed`].
/// - HTTP 403 → [`CatalogError::PermissionDenied`].
/// - HTTP 409 → [`CatalogError::CommitConflict`].
/// - HTTP 429 and 5xx → [`CatalogError::Transport`] (the underlying
///   error retains the original detail; callers see it via the
///   `#[source]` chain).
/// - [`IcebergError::RateLimited`] (429 after retries exhausted) and
///   transport-level [`IcebergError::Http`] also become
///   [`CatalogError::Transport`].
/// - [`IcebergError::UnexpectedResponse`] → [`CatalogError::InvalidResponse`].
fn icebergerror_into_catalog(err: IcebergError) -> CatalogError {
    match err {
        IcebergError::Api { status, message } => match status {
            401 => CatalogError::AuthFailed(format!("HTTP 401: {message}")),
            403 => CatalogError::PermissionDenied(format!("HTTP 403: {message}")),
            404 => CatalogError::TableNotFound(format!("HTTP 404: {message}")),
            409 => CatalogError::CommitConflict(format!("HTTP 409: {message}")),
            429 | 500..=599 => {
                CatalogError::Transport(Box::new(IcebergError::Api { status, message }))
            }
            _ => CatalogError::InvalidResponse(format!("HTTP {status}: {message}")),
        },
        IcebergError::RateLimited => CatalogError::Transport(Box::new(IcebergError::RateLimited)),
        IcebergError::Http(e) => CatalogError::Transport(Box::new(e)),
        IcebergError::UnexpectedResponse(msg) => CatalogError::InvalidResponse(msg),
    }
}

impl From<IcebergError> for CatalogError {
    fn from(err: IcebergError) -> Self {
        icebergerror_into_catalog(err)
    }
}

fn format_table(table: &TableRef) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(cat) = &table.catalog {
        parts.push(cat.clone());
    }
    parts.extend(table.namespace.iter().cloned());
    parts.push(table.name.clone());
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_type_unwraps_primitive_string() {
        let v = serde_json::Value::String("long".to_string());
        assert_eq!(render_type(&v), "long");
    }

    #[test]
    fn render_type_serialises_nested_object() {
        let v = serde_json::json!({
            "type": "list",
            "element-id": 7,
            "element": "string",
            "element-required": true
        });
        let rendered = render_type(&v);
        assert!(rendered.starts_with('{') && rendered.ends_with('}'));
        assert!(rendered.contains("\"type\":\"list\""));
    }

    #[test]
    fn api_401_classifies_as_auth_failed() {
        let err = IcebergError::Api {
            status: 401,
            message: "unauthorized".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(
            matches!(mapped, CatalogError::AuthFailed(_)),
            "expected AuthFailed, got {mapped:?}"
        );
    }

    #[test]
    fn api_403_classifies_as_permission_denied() {
        let err = IcebergError::Api {
            status: 403,
            message: "forbidden".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(
            matches!(mapped, CatalogError::PermissionDenied(_)),
            "expected PermissionDenied, got {mapped:?}"
        );
    }

    #[test]
    fn api_404_classifies_as_table_not_found_by_default() {
        let err = IcebergError::Api {
            status: 404,
            message: "no such".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(
            matches!(mapped, CatalogError::TableNotFound(_)),
            "expected TableNotFound (default), got {mapped:?}"
        );
    }

    #[test]
    fn api_409_classifies_as_commit_conflict() {
        let err = IcebergError::Api {
            status: 409,
            message: "conflict".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(
            matches!(mapped, CatalogError::CommitConflict(_)),
            "expected CommitConflict, got {mapped:?}"
        );
    }

    #[test]
    fn api_5xx_classifies_as_transport() {
        for status in [500u16, 502, 503, 504] {
            let err = IcebergError::Api {
                status,
                message: format!("status {status}"),
            };
            let mapped: CatalogError = err.into();
            assert!(
                matches!(mapped, CatalogError::Transport(_)),
                "status {status} expected Transport, got {mapped:?}"
            );
        }
    }

    #[test]
    fn api_429_classifies_as_transport() {
        let err = IcebergError::Api {
            status: 429,
            message: "rate limited".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(matches!(mapped, CatalogError::Transport(_)));
    }

    #[test]
    fn rate_limited_variant_classifies_as_transport() {
        let mapped: CatalogError = IcebergError::RateLimited.into();
        assert!(matches!(mapped, CatalogError::Transport(_)));
    }

    #[test]
    fn unexpected_response_classifies_as_invalid_response() {
        let mapped: CatalogError = IcebergError::UnexpectedResponse("bad json".into()).into();
        assert!(matches!(mapped, CatalogError::InvalidResponse(_)));
    }

    #[test]
    fn unknown_api_status_classifies_as_invalid_response() {
        // 418 / 451 / etc. — not one of the explicitly classified
        // codes; surfaces as InvalidResponse rather than silently
        // becoming Transport.
        let err = IcebergError::Api {
            status: 418,
            message: "teapot".into(),
        };
        let mapped: CatalogError = err.into();
        assert!(matches!(mapped, CatalogError::InvalidResponse(_)));
    }
}
