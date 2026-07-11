//! [`CatalogClient`] implementation backed by Databricks Unity Catalog REST.
//!
//! [`UnityCatalogClient`] talks directly to Unity's REST surface
//! (`/api/2.1/unity-catalog/...`) for the catalog operations Unity
//! exposes natively. The shape mirrors
//! [`crate::workspace::WorkspaceManager`] and
//! [`crate::scim::ScimClient`] — a thin `host + Auth + reqwest::Client`
//! triple, no retry/circuit-breaker layer (those live on
//! [`crate::connector::DatabricksConnector`] for the SQL path), and a
//! single error type that maps cleanly onto
//! [`rocky_catalog_core::CatalogError`].
//!
//! ## Method coverage
//!
//! Operations split into three buckets:
//!
//! 1. **Unity REST native** — implemented as direct HTTP calls.
//!    - [`CatalogClient::describe_table`] → `GET /api/2.1/unity-catalog/tables/{full_name}`
//!    - [`CatalogClient::list_tables`] → `GET /api/2.1/unity-catalog/tables?catalog_name=X&schema_name=Y`
//!    - [`CatalogClient::create_table`] → `POST /api/2.1/unity-catalog/tables` (managed Delta)
//!    - [`CatalogClient::drop_table`] → `DELETE /api/2.1/unity-catalog/tables/{full_name}`
//!    - [`CatalogClient::get_grants`] → `GET /api/2.1/unity-catalog/permissions/table/{full_name}`
//!    - [`CatalogClient::apply_grant`] / [`CatalogClient::revoke_grant`] →
//!      `PATCH /api/2.1/unity-catalog/permissions/table/{full_name}` with
//!      one entry in `changes[]` carrying the add or remove list.
//! 2. **No Unity REST equivalent** — return
//!    [`CatalogError::UnsupportedOperation`] today.
//!    - [`CatalogClient::tag_table`] — Unity exposes tags via
//!      `ALTER TABLE SET TAGS` SQL only; there is no REST tagging
//!      endpoint on tables in Databricks runtime (confirmed against the
//!      catalog-first plan's risk-2). Callers that want the SQL fallback
//!      compose [`crate::catalog::CatalogManager::set_table_tags`].
//!    - [`CatalogClient::commit_transaction`] — Catalog-Managed Commits
//!      exist in Unity 0.4+ but are not surfaced as a generic multi-table
//!      commit REST endpoint.
//!    - [`CatalogClient::list_branches`] — Unity OSS does not model
//!      Iceberg branches/tags.
//!
//! ## Three-level naming
//!
//! Unity is rigidly `catalog.schema.table`. The catalog-agnostic
//! [`TableRef`] allows multi-part namespaces (Iceberg REST shape); this
//! adapter projects that onto Unity's three-level model:
//!
//! - [`TableRef::catalog`] must be `Some(_)` — the Unity catalog name.
//! - [`TableRef::namespace`] must contain exactly one part — the Unity
//!   schema name.
//! - [`TableRef::name`] is the Unity table name.
//!
//! Calls that violate this shape return
//! [`CatalogError::InvalidResponse`] with a precise diagnostic, before
//! any network hop. Future Polaris/Nessie implementations that accept
//! multi-level namespaces will not share this constraint.
//!
//! Namespace-listing operations
//! ([`CatalogClient::list_tables`](rocky_catalog_core::CatalogClient::list_tables))
//! take `namespace = [catalog, schema]` since the trait signature has
//! no separate catalog parameter. Per-table operations read catalog
//! from [`TableRef::catalog`]. The asymmetry is inherent to mapping
//! Unity's three-level model (multi-catalog per workspace) onto a
//! trait whose namespace surface is shared with single-catalog
//! Iceberg-style implementations.
//!
//! ## Error mapping
//!
//! HTTP status → [`CatalogError`] follows the rocky-iceberg precedent:
//!
//! - `401` → [`CatalogError::AuthFailed`]
//! - `403` → [`CatalogError::PermissionDenied`]
//! - `404` → [`CatalogError::TableNotFound`] by default; namespace-scoped
//!   call sites (e.g. [`list_tables`]) rewrite to
//!   [`CatalogError::NamespaceNotFound`].
//! - `409` → [`CatalogError::CommitConflict`]
//! - `429` and `5xx` → [`CatalogError::Transport`]
//! - all other 4xx → [`CatalogError::InvalidResponse`]
//! - transport-level failures → [`CatalogError::Transport`]
//!
//! [`CatalogClient`]: rocky_catalog_core::CatalogClient
//! [`CatalogClient::describe_table`]: rocky_catalog_core::CatalogClient::describe_table
//! [`CatalogClient::list_tables`]: rocky_catalog_core::CatalogClient::list_tables
//! [`CatalogClient::create_table`]: rocky_catalog_core::CatalogClient::create_table
//! [`CatalogClient::drop_table`]: rocky_catalog_core::CatalogClient::drop_table
//! [`CatalogClient::get_grants`]: rocky_catalog_core::CatalogClient::get_grants
//! [`CatalogClient::apply_grant`]: rocky_catalog_core::CatalogClient::apply_grant
//! [`CatalogClient::revoke_grant`]: rocky_catalog_core::CatalogClient::revoke_grant
//! [`CatalogClient::tag_table`]: rocky_catalog_core::CatalogClient::tag_table
//! [`CatalogClient::commit_transaction`]: rocky_catalog_core::CatalogClient::commit_transaction
//! [`CatalogClient::list_branches`]: rocky_catalog_core::CatalogClient::list_branches
//! [`list_tables`]: rocky_catalog_core::CatalogClient::list_tables
//! [`CatalogError`]: rocky_catalog_core::CatalogError
//! [`CatalogError::UnsupportedOperation`]: rocky_catalog_core::CatalogError::UnsupportedOperation
//! [`CatalogError::TableNotFound`]: rocky_catalog_core::CatalogError::TableNotFound
//! [`CatalogError::NamespaceNotFound`]: rocky_catalog_core::CatalogError::NamespaceNotFound
//! [`CatalogError::InvalidResponse`]: rocky_catalog_core::CatalogError::InvalidResponse
//! [`CatalogError::Transport`]: rocky_catalog_core::CatalogError::Transport
//! [`CatalogError::AuthFailed`]: rocky_catalog_core::CatalogError::AuthFailed
//! [`CatalogError::PermissionDenied`]: rocky_catalog_core::CatalogError::PermissionDenied
//! [`CatalogError::CommitConflict`]: rocky_catalog_core::CatalogError::CommitConflict
//! [`TableRef`]: rocky_catalog_core::TableRef
//! [`TableRef::catalog`]: rocky_catalog_core::TableRef::catalog
//! [`TableRef::namespace`]: rocky_catalog_core::TableRef::namespace
//! [`TableRef::name`]: rocky_catalog_core::TableRef::name

use std::time::Duration;

use async_trait::async_trait;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;

use std::collections::BTreeMap;

use rocky_catalog_core::{
    BranchRef, CatalogClient, CatalogError, CatalogResult, ColumnSchema, GovernanceCatalogClient,
    Grant, Securable, TableCommit, TableRef, TableSchema, TableStats,
};

use crate::auth::{Auth, AuthError};

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors returned by [`UnityCatalogClient`] internally.
///
/// This is an intermediate error type — every variant maps cleanly onto
/// [`CatalogError`] via the [`From<UnityRestError> for CatalogError`]
/// impl below. Kept distinct so the HTTP / serde / auth layer can keep
/// its own vocabulary (status + body) without leaking into the trait
/// surface, mirroring the way
/// [`crate::client::IcebergError`](../../rocky-iceberg/src/client.rs)
/// is structured.
#[derive(Debug, thiserror::Error)]
pub enum UnityRestError {
    /// Auth-layer failure (token mint, OAuth exchange).
    #[error("auth error: {0}")]
    Auth(#[from] AuthError),

    /// Transport-level HTTP error (connect, TLS, body parse, etc.).
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Non-2xx status from the Unity API.
    #[error("Unity API error {status}: {body}")]
    Api { status: u16, body: String },

    /// Response body parsed successfully but did not carry the expected shape.
    #[error("unexpected Unity response: {0}")]
    UnexpectedResponse(String),
}

// ---------------------------------------------------------------------------
// Wire types — request bodies
// ---------------------------------------------------------------------------

/// Body for `POST /api/2.1/unity-catalog/tables`.
///
/// We model only the fields Rocky writes today: name, parent catalog +
/// schema, the column list, and the managed-Delta combination
/// (`table_type = "MANAGED"`, `data_source_format = "DELTA"`). Storage
/// location, comment, properties, and partitions are spec-optional and
/// can be added by future callers without a trait change.
#[derive(Debug, Clone, Serialize)]
struct CreateTableBody<'a> {
    name: &'a str,
    catalog_name: &'a str,
    schema_name: &'a str,
    table_type: &'static str,
    data_source_format: &'static str,
    columns: Vec<CreateColumn<'a>>,
}

/// One column inside [`CreateTableBody::columns`].
///
/// `type_name`, `type_text` and `type_json` are all required by Unity's
/// table-create endpoint. `type_name` is the enum name
/// (`STRING`, `LONG`, etc.), `type_text` is the SQL spelling
/// (`"string"`, `"bigint"`), and `type_json` is a JSON-encoded
/// description that Unity validates against the other two.
#[derive(Debug, Clone, Serialize)]
struct CreateColumn<'a> {
    name: &'a str,
    type_name: String,
    type_text: String,
    type_json: String,
    position: i32,
    nullable: bool,
}

/// Body for `PATCH /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}`.
///
/// Unity's permission PATCH applies a list of `changes[]` per request.
/// Each change names one `principal` and an `add` and / or `remove`
/// list of privilege strings. The
/// [`CatalogClient::apply_grant`](rocky_catalog_core::CatalogClient::apply_grant)
/// and
/// [`CatalogClient::revoke_grant`](rocky_catalog_core::CatalogClient::revoke_grant)
/// methods both produce a single-change PATCH — the same endpoint, with
/// the privilege landing in the `add` or `remove` slot respectively.
#[derive(Debug, Clone, Serialize)]
struct UpdatePermissionsBody {
    changes: Vec<PermissionChange>,
}

#[derive(Debug, Clone, Serialize)]
struct PermissionChange {
    principal: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    add: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    remove: Vec<String>,
}

// ---------------------------------------------------------------------------
// Wire types — response bodies
// ---------------------------------------------------------------------------

/// Response body for `GET /api/2.1/unity-catalog/tables/{full_name}` and
/// `POST /api/2.1/unity-catalog/tables`.
///
/// Unity's table-info response is large (storage location, properties,
/// metadata, owner, etc.); only the unqualified `name` and the column
/// list are projected here. List responses use the same shape per
/// entry; `name` is the unqualified table name and is `None` on
/// describe responses that omit it (`full_name` carries the qualified
/// form in that case but is not modelled today).
#[derive(Debug, Clone, Deserialize)]
struct TableInfo {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    columns: Vec<TableColumn>,
}

/// One column inside [`TableInfo::columns`].
#[derive(Debug, Clone, Deserialize)]
struct TableColumn {
    name: String,
    #[serde(default)]
    type_text: Option<String>,
    #[serde(default)]
    type_name: Option<String>,
    #[serde(default)]
    nullable: Option<bool>,
}

/// Response body for `GET /api/2.1/unity-catalog/tables?...`.
///
/// Unity paginates via `next_page_token`; the in-memory unification of
/// pages happens in [`UnityCatalogClient::list_tables_inner`].
#[derive(Debug, Clone, Deserialize)]
struct ListTablesResponse {
    #[serde(default)]
    tables: Vec<TableInfo>,
    #[serde(default)]
    next_page_token: Option<String>,
}

/// Response body for `GET /api/2.1/unity-catalog/permissions/{type}/{fqn}`.
///
/// Unity returns one `privilege_assignment` per principal; each carries
/// the principal name and the list of privileges granted to it.
#[derive(Debug, Clone, Deserialize)]
struct PermissionsResponse {
    #[serde(default)]
    privilege_assignments: Vec<PrivilegeAssignment>,
}

#[derive(Debug, Clone, Deserialize)]
struct PrivilegeAssignment {
    principal: String,
    #[serde(default)]
    privileges: Vec<String>,
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Unity Catalog REST-backed [`CatalogClient`] implementation.
///
/// Composes a `host + Auth + reqwest::Client` triple and translates the
/// catalog-agnostic trait surface onto Unity's `/api/2.1/unity-catalog/`
/// endpoints. The struct is cheaply cloneable: `Auth` is `Clone` (Arc
/// inside), `reqwest::Client` is Arc-backed, and the host is a small
/// owned string. Multiple Rocky tasks may share the same instance
/// across async boundaries.
#[derive(Clone)]
pub struct UnityCatalogClient {
    host: String,
    auth: Auth,
    client: Client,
    /// Override for the base URL scheme + host (used by tests to point
    /// at wiremock). Matches the pattern used by
    /// [`crate::workspace::WorkspaceManager`].
    #[cfg(any(test, feature = "test-support"))]
    base_url_override: Option<String>,
}

impl UnityCatalogClient {
    /// Build a new client.
    ///
    /// `host` is the Databricks workspace host (no scheme), e.g.
    /// `"dbc-12345678-abcd.cloud.databricks.com"`. `auth` is the same
    /// [`Auth`] instance the SQL connector uses — both PAT and OAuth
    /// M2M flow through transparently.
    pub fn new(host: String, auth: Auth) -> Self {
        UnityCatalogClient {
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

    /// Returns the base URL for API calls.
    fn api_base_url(&self) -> String {
        #[cfg(any(test, feature = "test-support"))]
        if let Some(url) = &self.base_url_override {
            return url.clone();
        }
        format!("https://{}", self.host)
    }

    /// Sends `req` with bearer auth attached, awaits a response, and
    /// surfaces non-2xx status codes as [`UnityRestError::Api`] (with
    /// the body buffered as UTF-8 lossily so 4xx diagnostic strings are
    /// not lost). Successful 2xx responses are returned unchanged for
    /// the caller to body-parse.
    async fn send(
        &self,
        req: reqwest::RequestBuilder,
    ) -> Result<reqwest::Response, UnityRestError> {
        let token = self.auth.get_token().await?;
        let resp = req.bearer_auth(token).send().await?;
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(UnityRestError::Api { status, body });
        }
        Ok(resp)
    }
}

impl std::fmt::Debug for UnityCatalogClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnityCatalogClient")
            .field("host", &self.host)
            .field("auth", &self.auth)
            .finish_non_exhaustive()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build the shared `reqwest::Client` used for every Unity REST call.
fn build_http_client() -> Client {
    Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(120))
        .build()
        .expect("reqwest client builder is infallible with these options")
}

/// Byte set used when percent-encoding individual identifier parts in
/// the URL path. Allows RFC 3986 unreserved chars (`-`, `_`, `~`,
/// alphanumerics) and encodes everything else, including `.` so a name
/// with embedded dots cannot fake an extra level on the path. The
/// literal `.` separator between catalog/schema/table is *not* encoded
/// — it's inserted by [`build_full_name`] after each part has been
/// individually encoded.
const PATH_SAFE: &AsciiSet = &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'~');

/// Encode one identifier for use in a Unity REST URL path.
fn encode_part(part: &str) -> String {
    utf8_percent_encode(part, PATH_SAFE).to_string()
}

/// Project a catalog-agnostic [`TableRef`] onto Unity's three-level name.
///
/// Unity is rigidly `catalog.schema.table`. The mapping is:
///
/// - [`TableRef::catalog`] must be `Some(_)`.
/// - [`TableRef::namespace`] must contain exactly one entry (the schema).
/// - [`TableRef::name`] is the table.
///
/// Returns [`CatalogError::InvalidResponse`] with a precise diagnostic
/// when the shape doesn't match — caught before any network hop.
fn unity_parts(table: &TableRef) -> CatalogResult<(&str, &str, &str)> {
    let catalog = table.catalog.as_deref().ok_or_else(|| {
        CatalogError::InvalidResponse(
            "Unity Catalog requires TableRef.catalog to be Some(_)".to_string(),
        )
    })?;
    if table.namespace.len() != 1 {
        return Err(CatalogError::InvalidResponse(format!(
            "Unity Catalog requires exactly one namespace part (schema); got {} parts",
            table.namespace.len()
        )));
    }
    Ok((catalog, &table.namespace[0], &table.name))
}

/// Build the URL-path-safe Unity `full_name` (`{catalog}.{schema}.{table}`).
///
/// Each part is percent-encoded with [`PATH_SAFE`] before being joined
/// with a literal `.`. This prevents identifiers carrying `/`, `?`,
/// `#`, or extra `.` characters from forging paths on the Unity API
/// server.
fn build_full_name(catalog: &str, schema: &str, table: &str) -> String {
    format!(
        "{}.{}.{}",
        encode_part(catalog),
        encode_part(schema),
        encode_part(table),
    )
}

/// Map [`UnityRestError`] onto the catalog-agnostic [`CatalogError`].
///
/// Rules (mirror the rocky-iceberg precedent so cross-adapter behaviour
/// is uniform):
///
/// - 401 → [`CatalogError::AuthFailed`]
/// - 403 → [`CatalogError::PermissionDenied`]
/// - 404 → [`CatalogError::TableNotFound`] (table-targeted default;
///   namespace-scoped call sites rewrite to
///   [`CatalogError::NamespaceNotFound`] in a `map_err`).
/// - 409 → [`CatalogError::CommitConflict`]
/// - 429 or 5xx → [`CatalogError::Transport`]
/// - other 4xx → [`CatalogError::InvalidResponse`]
/// - transport-level [`UnityRestError::Http`],
///   [`UnityRestError::Auth`] → [`CatalogError::Transport`] (auth
///   failures bubble via the `From<AuthError>` chain to
///   `Transport` since `AuthError` is the runtime-layer wrapper around
///   refresh failures — the dedicated [`CatalogError::AuthFailed`]
///   variant is reserved for surfaced 401s).
/// - [`UnityRestError::UnexpectedResponse`] → [`CatalogError::InvalidResponse`].
fn unity_into_catalog(err: UnityRestError) -> CatalogError {
    match err {
        UnityRestError::Api { status, body } => match status {
            401 => CatalogError::AuthFailed(format!("HTTP 401: {body}")),
            403 => CatalogError::PermissionDenied(format!("HTTP 403: {body}")),
            404 => CatalogError::TableNotFound(format!("HTTP 404: {body}")),
            409 => CatalogError::CommitConflict(format!("HTTP 409: {body}")),
            429 | 500..=599 => {
                CatalogError::Transport(Box::new(UnityRestError::Api { status, body }))
            }
            _ => CatalogError::InvalidResponse(format!("HTTP {status}: {body}")),
        },
        UnityRestError::Http(e) => CatalogError::Transport(Box::new(e)),
        UnityRestError::Auth(e) => CatalogError::Transport(Box::new(e)),
        UnityRestError::UnexpectedResponse(msg) => CatalogError::InvalidResponse(msg),
    }
}

impl From<UnityRestError> for CatalogError {
    fn from(err: UnityRestError) -> Self {
        unity_into_catalog(err)
    }
}

/// Build the catalog-agnostic [`TableSchema`] from a Unity [`TableInfo`].
///
/// `type_text` is preferred (SQL spelling, `"string"`/`"bigint"`); falls
/// back to `type_name` (`"STRING"`/`"LONG"`) when `type_text` is absent.
/// Defaults to `nullable = true` when Unity omits the flag — Delta's
/// declarative nullability comes through this field but historical
/// table-info responses occasionally omit it.
fn schema_from_table_info(info: &TableInfo) -> TableSchema {
    let columns = info
        .columns
        .iter()
        .map(|c| ColumnSchema {
            name: c.name.clone(),
            type_str: c
                .type_text
                .clone()
                .or_else(|| c.type_name.clone())
                .unwrap_or_default(),
            nullable: c.nullable.unwrap_or(true),
        })
        .collect();
    TableSchema { columns }
}

/// Render Rocky's [`ColumnSchema::type_str`] into the trio of
/// `type_name` / `type_text` / `type_json` Unity's create-table
/// endpoint demands.
///
/// `type_str` is the SQL spelling Rocky carries on the trait surface
/// (`"string"`, `"bigint"`, `"decimal(10,2)"`, ...). For primitive types
/// we project onto the Unity enum name via [`unity_type_name`]; for
/// complex types (lists/maps/structs) we leave the enum as `STRING`
/// and let `type_text` carry the spec — Rocky's trait does not carry
/// rich enough information to round-trip complex types here, and
/// callers that need them should use a richer adapter surface.
fn create_columns_from_schema<'a>(schema: &'a TableSchema) -> Vec<CreateColumn<'a>> {
    schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            let type_text = col.type_str.clone();
            let type_name = unity_type_name(&col.type_str);
            // Unity validates `type_json` against the other two; the
            // minimal form is a JSON object with `name` / `type` /
            // `nullable` / `metadata`. Rocky carries no field metadata
            // on the trait, so emit an empty `metadata` object.
            let type_json = serde_json::json!({
                "name": col.name,
                "type": type_text.to_lowercase(),
                "nullable": col.nullable,
                "metadata": {},
            })
            .to_string();
            CreateColumn {
                name: &col.name,
                type_name,
                type_text,
                type_json,
                position: idx as i32,
                nullable: col.nullable,
            }
        })
        .collect()
}

/// Map a SQL-spelling type string (`"bigint"`, `"string"`, ...) to the
/// Unity enum name (`LONG`, `STRING`, ...).
///
/// Coverage is intentionally limited to the common cases Rocky's IR
/// emits today. Unknown spellings default to `STRING` — the trait does
/// not promise round-trip fidelity for warehouse-specific types, and
/// callers that need richer mapping should route through a non-trait
/// surface.
fn unity_type_name(type_str: &str) -> String {
    let lower = type_str.trim().to_ascii_lowercase();
    let head = lower.split('(').next().unwrap_or(&lower).trim();
    let name = match head {
        "boolean" | "bool" => "BOOLEAN",
        "byte" | "tinyint" => "BYTE",
        "short" | "smallint" => "SHORT",
        "int" | "integer" => "INT",
        "long" | "bigint" => "LONG",
        "float" => "FLOAT",
        "double" => "DOUBLE",
        "string" | "varchar" | "char" | "text" => "STRING",
        "binary" => "BINARY",
        "date" => "DATE",
        "timestamp" | "timestamp_ntz" => "TIMESTAMP",
        "decimal" | "numeric" => "DECIMAL",
        "array" => "ARRAY",
        "map" => "MAP",
        "struct" => "STRUCT",
        _ => "STRING",
    };
    name.to_string()
}

// ---------------------------------------------------------------------------
// CatalogClient impl
// ---------------------------------------------------------------------------

#[async_trait]
impl CatalogClient for UnityCatalogClient {
    async fn list_tables(&self, namespace: &[String]) -> CatalogResult<Vec<TableRef>> {
        // Unity needs both the catalog and schema names to list tables.
        // The trait gives us a multi-part namespace; project onto the
        // Unity two-of-three (catalog + schema) and fail before the
        // network hop if the shape is wrong.
        if namespace.len() != 2 {
            return Err(CatalogError::InvalidResponse(format!(
                "Unity Catalog list_tables requires namespace = [catalog, schema]; got {} parts",
                namespace.len()
            )));
        }
        let catalog = &namespace[0];
        let schema = &namespace[1];
        // 404 on this endpoint typically means the parent schema is
        // missing; rewrite the default `TableNotFound` accordingly.
        self.list_tables_inner(catalog, schema)
            .await
            .map_err(|e| match unity_into_catalog(e) {
                CatalogError::TableNotFound(msg) => CatalogError::NamespaceNotFound(msg),
                other => other,
            })
    }

    async fn describe_table(&self, table: &TableRef) -> CatalogResult<TableSchema> {
        let (catalog, schema, name) = unity_parts(table)?;
        let full = build_full_name(catalog, schema, name);
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.api_base_url(),
            full
        );
        let resp = self
            .send(self.client.get(&url))
            .await
            .map_err(unity_into_catalog)?;
        let info: TableInfo = resp
            .json()
            .await
            .map_err(|e| CatalogError::InvalidResponse(format!("describe_table body: {e}")))?;
        debug!(catalog, schema, name, "described Unity table");
        Ok(schema_from_table_info(&info))
    }

    async fn create_table(&self, table: &TableRef, schema: &TableSchema) -> CatalogResult<()> {
        let (cat, sch, name) = unity_parts(table)?;
        let body = CreateTableBody {
            name,
            catalog_name: cat,
            schema_name: sch,
            table_type: "MANAGED",
            data_source_format: "DELTA",
            columns: create_columns_from_schema(schema),
        };
        let url = format!("{}/api/2.1/unity-catalog/tables", self.api_base_url());
        // 404 on a parent-scoped endpoint means the catalog or schema
        // is missing, not the table; rewrite the default mapping.
        self.send(self.client.post(&url).json(&body))
            .await
            .map_err(|e| match unity_into_catalog(e) {
                CatalogError::TableNotFound(msg) => CatalogError::NamespaceNotFound(msg),
                other => other,
            })?;
        debug!(catalog = cat, schema = sch, name, "created Unity table");
        Ok(())
    }

    async fn drop_table(&self, table: &TableRef) -> CatalogResult<()> {
        let (catalog, schema, name) = unity_parts(table)?;
        let full = build_full_name(catalog, schema, name);
        let url = format!(
            "{}/api/2.1/unity-catalog/tables/{}",
            self.api_base_url(),
            full
        );
        self.send(self.client.delete(&url))
            .await
            .map_err(unity_into_catalog)?;
        debug!(catalog, schema, name, "dropped Unity table");
        Ok(())
    }

    async fn commit_transaction(&self, _commits: &[TableCommit]) -> CatalogResult<()> {
        // Unity exposes Catalog-Managed Commits internally (UC 0.4+,
        // 2026-02 Delta integration), but not as a generic multi-table
        // commit REST endpoint Rocky can target uniformly. Surface as
        // unsupported so callers route through warehouse-side execution.
        Err(CatalogError::UnsupportedOperation(
            "Unity Catalog REST exposes no generic multi-table commit endpoint",
        ))
    }

    async fn list_branches(&self, _table: &TableRef) -> CatalogResult<Vec<BranchRef>> {
        // Unity OSS does not model Iceberg branches/tags. Even reading
        // foreign Iceberg from Databricks pins to `main`. Surface as
        // unsupported so branch-aware callers fall back to per-adapter
        // primitives or skip the call.
        Err(CatalogError::UnsupportedOperation(
            "Unity Catalog does not expose Iceberg-style branches/tags",
        ))
    }

    async fn table_stats(&self, _table: &TableRef) -> CatalogResult<TableStats> {
        // Unity Catalog REST exposes no table-stats surface:
        //
        // - `GET /api/2.1/unity-catalog/tables/{full_name}` returns
        //   schema + storage metadata only (modelled by `TableInfo`
        //   above); no row count, no byte total, no file count.
        // - `INFORMATION_SCHEMA.TABLES` on Databricks does not carry
        //   `row_count` or `table_size_bytes` columns (verified live
        //   2026-05-20 against the live-verify sandbox catalog's
        //   information_schema).
        // - The only working paths to stats are
        //   `DESCRIBE DETAIL <table>.sizeInBytes` (bytes, no ANALYZE
        //   prereq) and `DESCRIBE EXTENDED <table>` (parses the
        //   `Statistics` row, present only after `ANALYZE TABLE
        //   COMPUTE STATISTICS`). Both run through the SQL Statement
        //   Execution API, which lives on
        //   [`crate::connector::DatabricksConnector`], not here. This
        //   client is REST-only by design (see module-level doc).
        //
        // Consistent with `tag_table` / `commit_transaction` /
        // `list_branches`: REST gap, caller routes via the SQL path.
        Err(CatalogError::UnsupportedOperation(
            "Unity Catalog REST exposes no stats endpoint; route via DatabricksConnector SQL (DESCRIBE DETAIL / ANALYZE TABLE)",
        ))
    }

    async fn tag_table(&self, _table: &TableRef, _key: &str, _value: &str) -> CatalogResult<()> {
        // Unity has no general-purpose table-tag REST endpoint in
        // Databricks runtime — `ALTER TABLE SET TAGS` SQL is the only
        // supported path. Callers that need the SQL fallback compose
        // [`crate::catalog::CatalogManager::set_table_tags`] directly;
        // this client stays SQL-free by design.
        Err(CatalogError::UnsupportedOperation(
            "Unity Catalog exposes no table-tag REST endpoint; use ALTER TABLE SET TAGS SQL",
        ))
    }

    async fn get_grants(&self, table: &TableRef) -> CatalogResult<Vec<Grant>> {
        let (catalog, schema, name) = unity_parts(table)?;
        let full = build_full_name(catalog, schema, name);
        let url = format!(
            "{}/api/2.1/unity-catalog/permissions/table/{}",
            self.api_base_url(),
            full
        );
        let resp = self
            .send(self.client.get(&url))
            .await
            .map_err(unity_into_catalog)?;
        let body: PermissionsResponse = resp
            .json()
            .await
            .map_err(|e| CatalogError::InvalidResponse(format!("get_grants body: {e}")))?;
        let mut out = Vec::new();
        for assignment in body.privilege_assignments {
            for privilege in assignment.privileges {
                out.push(Grant {
                    principal: assignment.principal.clone(),
                    privilege,
                });
            }
        }
        Ok(out)
    }

    async fn apply_grant(&self, table: &TableRef, grant: &Grant) -> CatalogResult<()> {
        self.patch_permissions(table, grant, /*adding=*/ true).await
    }

    async fn revoke_grant(&self, table: &TableRef, grant: &Grant) -> CatalogResult<()> {
        self.patch_permissions(table, grant, /*adding=*/ false)
            .await
    }
}

// ---------------------------------------------------------------------------
// Inner helpers (keep CatalogClient impl readable)
// ---------------------------------------------------------------------------

impl UnityCatalogClient {
    /// Paginated `GET /api/2.1/unity-catalog/tables?catalog_name=&schema_name=`.
    ///
    /// Drains `next_page_token` so the caller sees the full list without
    /// having to thread cursors through the trait surface.
    async fn list_tables_inner(
        &self,
        catalog: &str,
        schema: &str,
    ) -> Result<Vec<TableRef>, UnityRestError> {
        let mut out = Vec::new();
        let mut page_token: Option<String> = None;
        loop {
            let mut req = self
                .client
                .get(format!(
                    "{}/api/2.1/unity-catalog/tables",
                    self.api_base_url()
                ))
                .query(&[("catalog_name", catalog), ("schema_name", schema)]);
            if let Some(token) = &page_token {
                req = req.query(&[("page_token", token.as_str())]);
            }
            let resp = self.send(req).await?;
            let parsed: ListTablesResponse = resp.json().await.map_err(|e| {
                UnityRestError::UnexpectedResponse(format!("list_tables body: {e}"))
            })?;
            for info in parsed.tables {
                let Some(name) = info.name else {
                    continue;
                };
                if name.is_empty() {
                    continue;
                }
                out.push(TableRef {
                    catalog: Some(catalog.to_string()),
                    namespace: vec![schema.to_string()],
                    name,
                });
            }
            match parsed.next_page_token {
                Some(t) if !t.is_empty() => page_token = Some(t),
                _ => break,
            }
        }
        debug!(catalog, schema, count = out.len(), "listed Unity tables");
        Ok(out)
    }

    /// PATCH `/api/2.1/unity-catalog/permissions/table/{fqn}` with a
    /// single-change body. Used by both
    /// [`CatalogClient::apply_grant`](rocky_catalog_core::CatalogClient::apply_grant)
    /// (privilege lands in `add`) and
    /// [`CatalogClient::revoke_grant`](rocky_catalog_core::CatalogClient::revoke_grant)
    /// (privilege lands in `remove`) — Unity's permissions endpoint is
    /// a single PATCH that does both via the `add` / `remove` slots.
    async fn patch_permissions(
        &self,
        table: &TableRef,
        grant: &Grant,
        adding: bool,
    ) -> CatalogResult<()> {
        let (catalog, schema, name) = unity_parts(table)?;
        let full = build_full_name(catalog, schema, name);
        let url = format!(
            "{}/api/2.1/unity-catalog/permissions/table/{}",
            self.api_base_url(),
            full
        );
        let change = PermissionChange {
            principal: grant.principal.clone(),
            add: if adding {
                vec![grant.privilege.clone()]
            } else {
                Vec::new()
            },
            remove: if adding {
                Vec::new()
            } else {
                vec![grant.privilege.clone()]
            },
        };
        let body = UpdatePermissionsBody {
            changes: vec![change],
        };
        self.send(self.client.patch(&url).json(&body))
            .await
            .map_err(unity_into_catalog)?;
        debug!(
            catalog,
            schema,
            name,
            principal = %grant.principal,
            privilege = %grant.privilege,
            action = if adding { "grant" } else { "revoke" },
            "Unity permission change applied"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// GovernanceCatalogClient impl
// ---------------------------------------------------------------------------

/// Unity REST securable-type slug used in the `permissions/{slug}/{fqn}` path.
fn securable_path_slug(securable: &Securable) -> &'static str {
    match securable {
        Securable::Catalog { .. } => "catalog",
        Securable::Schema { .. } => "schema",
        Securable::Table { .. } => "table",
    }
}

/// Build the URL-path-safe Unity `full_name` for a securable.
///
/// - `Catalog` -> `{catalog}`
/// - `Schema`  -> `{catalog}.{schema}`
/// - `Table`   -> `{catalog}.{schema}.{table}`
///
/// Each part is percent-encoded individually before being joined with a
/// literal `.` — same convention as [`build_full_name`] for the table case,
/// so a name with embedded dots cannot forge an extra path segment.
fn securable_full_name(securable: &Securable) -> String {
    match securable {
        Securable::Catalog { name } => encode_part(name),
        Securable::Schema { catalog, name } => {
            format!("{}.{}", encode_part(catalog), encode_part(name))
        }
        Securable::Table {
            catalog,
            schema,
            name,
        } => build_full_name(catalog, schema, name),
    }
}

/// Coalesce a flat `Vec<Grant>` into the `Vec<PermissionChange>` shape
/// Unity's PATCH endpoint expects.
///
/// Grants targeting the same `principal` collapse into a single
/// [`PermissionChange`] whose `add` (or `remove`, when `adding == false`)
/// list carries every privilege for that principal. The grouping is
/// stable (`BTreeMap` preserves principal ordering) so test assertions
/// can match on the rendered body deterministically.
///
/// This is the multi-change batching win — collapsing N single-change
/// PATCH calls into one PATCH carrying every change in a single round-
/// trip per `Securable`.
fn group_grants_into_changes(grants: &[Grant], adding: bool) -> Vec<PermissionChange> {
    let mut by_principal: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for grant in grants {
        by_principal
            .entry(grant.principal.clone())
            .or_default()
            .push(grant.privilege.clone());
    }
    by_principal
        .into_iter()
        .map(|(principal, privileges)| PermissionChange {
            principal,
            add: if adding {
                privileges.clone()
            } else {
                Vec::new()
            },
            remove: if adding { Vec::new() } else { privileges },
        })
        .collect()
}

#[async_trait]
impl GovernanceCatalogClient for UnityCatalogClient {
    /// PATCH `/api/2.1/unity-catalog/permissions/{type}/{fqn}` with **one**
    /// multi-change body grouping every grant by principal.
    ///
    /// Where the table-only [`CatalogClient::apply_grant`] wraps a single
    /// `(principal, privilege)` pair in a one-entry `changes[]` array, this
    /// method coalesces an N-grant slice into one PATCH per `Securable`. A
    /// no-op call (empty `grants`) short-circuits before any HTTP traffic.
    async fn apply_grants(&self, securable: &Securable, grants: &[Grant]) -> CatalogResult<()> {
        if grants.is_empty() {
            return Ok(());
        }
        self.patch_permissions_multi(securable, grants, /*adding=*/ true)
            .await
    }

    /// Mirror of [`Self::apply_grants`] that lands the privilege list in
    /// `remove[]` rather than `add[]`.
    async fn revoke_grants(&self, securable: &Securable, grants: &[Grant]) -> CatalogResult<()> {
        if grants.is_empty() {
            return Ok(());
        }
        self.patch_permissions_multi(securable, grants, /*adding=*/ false)
            .await
    }

    /// GET `/api/2.1/unity-catalog/permissions/{type}/{fqn}` and flatten
    /// the per-principal assignment shape into `Vec<Grant>`.
    async fn list_grants(&self, securable: &Securable) -> CatalogResult<Vec<Grant>> {
        let slug = securable_path_slug(securable);
        let full = securable_full_name(securable);
        let url = format!(
            "{}/api/2.1/unity-catalog/permissions/{}/{}",
            self.api_base_url(),
            slug,
            full
        );
        let resp = self
            .send(self.client.get(&url))
            .await
            .map_err(unity_into_catalog)?;
        let body: PermissionsResponse = resp
            .json()
            .await
            .map_err(|e| CatalogError::InvalidResponse(format!("list_grants body: {e}")))?;
        let mut out = Vec::new();
        for assignment in body.privilege_assignments {
            for privilege in assignment.privileges {
                out.push(Grant {
                    principal: assignment.principal.clone(),
                    privilege,
                });
            }
        }
        Ok(out)
    }
}

impl UnityCatalogClient {
    /// PATCH `/api/2.1/unity-catalog/permissions/{type}/{fqn}` with a
    /// multi-change body coalescing every grant by principal.
    ///
    /// Shared between [`GovernanceCatalogClient::apply_grants`] (privilege
    /// list in `add`) and [`GovernanceCatalogClient::revoke_grants`]
    /// (privilege list in `remove`). One round-trip per `Securable`,
    /// regardless of how many `(principal, privilege)` pairs the caller
    /// passed — the multi-change batching win.
    async fn patch_permissions_multi(
        &self,
        securable: &Securable,
        grants: &[Grant],
        adding: bool,
    ) -> CatalogResult<()> {
        let slug = securable_path_slug(securable);
        let full = securable_full_name(securable);
        let url = format!(
            "{}/api/2.1/unity-catalog/permissions/{}/{}",
            self.api_base_url(),
            slug,
            full
        );
        let changes = group_grants_into_changes(grants, adding);
        let principal_count = changes.len();
        let body = UpdatePermissionsBody { changes };
        self.send(self.client.patch(&url).json(&body))
            .await
            .map_err(unity_into_catalog)?;
        debug!(
            securable_type = slug,
            securable = %securable.display_name(),
            grant_count = grants.len(),
            principal_count,
            action = if adding { "grant" } else { "revoke" },
            "Unity multi-change permission PATCH applied"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_table_ref() -> TableRef {
        TableRef {
            catalog: Some("cat_a".to_string()),
            namespace: vec!["sch_a".to_string()],
            name: "tbl_a".to_string(),
        }
    }

    fn sample_schema() -> TableSchema {
        TableSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    type_str: "bigint".into(),
                    nullable: false,
                },
                ColumnSchema {
                    name: "amount".into(),
                    type_str: "decimal(10,2)".into(),
                    nullable: true,
                },
            ],
        }
    }

    // ---- shape projection ----

    #[test]
    fn unity_parts_accepts_three_level_shape() {
        let table = sample_table_ref();
        let r = unity_parts(&table).expect("ok");
        assert_eq!(r, ("cat_a", "sch_a", "tbl_a"));
    }

    #[test]
    fn unity_parts_rejects_missing_catalog() {
        let t = TableRef {
            catalog: None,
            namespace: vec!["s".into()],
            name: "t".into(),
        };
        let err = unity_parts(&t).unwrap_err();
        assert!(
            matches!(err, CatalogError::InvalidResponse(_)),
            "expected InvalidResponse, got {err:?}"
        );
    }

    #[test]
    fn unity_parts_rejects_multi_part_namespace() {
        let t = TableRef {
            catalog: Some("c".into()),
            namespace: vec!["a".into(), "b".into()],
            name: "t".into(),
        };
        let err = unity_parts(&t).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidResponse(_)));
    }

    #[test]
    fn unity_parts_rejects_empty_namespace() {
        let t = TableRef {
            catalog: Some("c".into()),
            namespace: vec![],
            name: "t".into(),
        };
        let err = unity_parts(&t).unwrap_err();
        assert!(matches!(err, CatalogError::InvalidResponse(_)));
    }

    // ---- URL building ----

    #[test]
    fn build_full_name_joins_with_dot() {
        assert_eq!(build_full_name("cat", "sch", "tbl"), "cat.sch.tbl");
    }

    #[test]
    fn build_full_name_encodes_special_chars() {
        // `/` and `?` would forge new endpoints if left raw; `.`
        // would forge an extra level. Each is encoded individually
        // before the literal `.` joiner.
        assert_eq!(build_full_name("a/b", "s?x", "t.t"), "a%2Fb.s%3Fx.t%2Et");
    }

    #[test]
    fn build_full_name_preserves_alphanumerics_and_underscores() {
        assert_eq!(
            build_full_name("cat_a", "sch_a", "orders_a_2026"),
            "cat_a.sch_a.orders_a_2026"
        );
    }

    // ---- status mapping ----

    #[test]
    fn api_401_maps_to_auth_failed() {
        let mapped: CatalogError = UnityRestError::Api {
            status: 401,
            body: "unauthorized".into(),
        }
        .into();
        assert!(matches!(mapped, CatalogError::AuthFailed(_)));
    }

    #[test]
    fn api_403_maps_to_permission_denied() {
        let mapped: CatalogError = UnityRestError::Api {
            status: 403,
            body: "forbidden".into(),
        }
        .into();
        assert!(matches!(mapped, CatalogError::PermissionDenied(_)));
    }

    #[test]
    fn api_404_maps_to_table_not_found_by_default() {
        let mapped: CatalogError = UnityRestError::Api {
            status: 404,
            body: "no such".into(),
        }
        .into();
        assert!(matches!(mapped, CatalogError::TableNotFound(_)));
    }

    #[test]
    fn api_409_maps_to_commit_conflict() {
        let mapped: CatalogError = UnityRestError::Api {
            status: 409,
            body: "conflict".into(),
        }
        .into();
        assert!(matches!(mapped, CatalogError::CommitConflict(_)));
    }

    #[test]
    fn api_429_and_5xx_map_to_transport() {
        for status in [429u16, 500, 502, 503, 504] {
            let mapped: CatalogError = UnityRestError::Api {
                status,
                body: format!("status {status}"),
            }
            .into();
            assert!(
                matches!(mapped, CatalogError::Transport(_)),
                "status {status} should map to Transport, got {mapped:?}"
            );
        }
    }

    #[test]
    fn unknown_4xx_maps_to_invalid_response() {
        let mapped: CatalogError = UnityRestError::Api {
            status: 418,
            body: "teapot".into(),
        }
        .into();
        assert!(matches!(mapped, CatalogError::InvalidResponse(_)));
    }

    #[test]
    fn unexpected_response_maps_to_invalid_response() {
        let mapped: CatalogError =
            UnityRestError::UnexpectedResponse("bad json".to_string()).into();
        assert!(matches!(mapped, CatalogError::InvalidResponse(_)));
    }

    // ---- type projection ----

    #[test]
    fn unity_type_name_primitive_coverage() {
        assert_eq!(unity_type_name("bigint"), "LONG");
        assert_eq!(unity_type_name("BIGINT"), "LONG");
        assert_eq!(unity_type_name("string"), "STRING");
        assert_eq!(unity_type_name("varchar(255)"), "STRING");
        assert_eq!(unity_type_name("decimal(10,2)"), "DECIMAL");
        assert_eq!(unity_type_name("boolean"), "BOOLEAN");
        assert_eq!(unity_type_name("timestamp"), "TIMESTAMP");
        assert_eq!(unity_type_name("date"), "DATE");
    }

    #[test]
    fn unity_type_name_unknown_defaults_to_string() {
        // Trait does not promise round-trip fidelity for warehouse-
        // specific types; unknown spellings fall back to STRING.
        assert_eq!(unity_type_name("rocky_special_type"), "STRING");
    }

    #[test]
    fn create_columns_from_schema_assigns_position_zero_first() {
        let schema = sample_schema();
        let cols = create_columns_from_schema(&schema);
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].position, 0);
        assert_eq!(cols[1].position, 1);
    }

    #[test]
    fn create_columns_from_schema_preserves_nullability() {
        let schema = sample_schema();
        let cols = create_columns_from_schema(&schema);
        assert!(!cols[0].nullable, "id is non-null");
        assert!(cols[1].nullable, "amount is nullable");
    }

    #[test]
    fn create_columns_from_schema_picks_unity_enum_name() {
        let schema = sample_schema();
        let cols = create_columns_from_schema(&schema);
        assert_eq!(cols[0].type_name, "LONG");
        assert_eq!(cols[1].type_name, "DECIMAL");
    }

    #[test]
    fn schema_from_table_info_prefers_type_text() {
        let info = TableInfo {
            name: Some("orders".into()),
            columns: vec![TableColumn {
                name: "id".into(),
                type_text: Some("bigint".into()),
                type_name: Some("LONG".into()),
                nullable: Some(false),
            }],
        };
        let schema = schema_from_table_info(&info);
        assert_eq!(schema.columns[0].type_str, "bigint");
        assert!(!schema.columns[0].nullable);
    }

    #[test]
    fn schema_from_table_info_falls_back_to_type_name_when_text_absent() {
        let info = TableInfo {
            name: None,
            columns: vec![TableColumn {
                name: "id".into(),
                type_text: None,
                type_name: Some("LONG".into()),
                nullable: None,
            }],
        };
        let schema = schema_from_table_info(&info);
        assert_eq!(schema.columns[0].type_str, "LONG");
        assert!(
            schema.columns[0].nullable,
            "missing nullable defaults to true"
        );
    }

    // ---- request body serialisation ----

    #[test]
    fn create_table_body_serialises_managed_delta() {
        let schema = sample_schema();
        let body = CreateTableBody {
            name: "orders_a",
            catalog_name: "cat_a",
            schema_name: "sch_a",
            table_type: "MANAGED",
            data_source_format: "DELTA",
            columns: create_columns_from_schema(&schema),
        };
        let v: serde_json::Value = serde_json::to_value(&body).unwrap();
        assert_eq!(v["name"], "orders_a");
        assert_eq!(v["catalog_name"], "cat_a");
        assert_eq!(v["schema_name"], "sch_a");
        assert_eq!(v["table_type"], "MANAGED");
        assert_eq!(v["data_source_format"], "DELTA");
        assert_eq!(v["columns"][0]["name"], "id");
        assert_eq!(v["columns"][0]["type_name"], "LONG");
        assert_eq!(v["columns"][0]["nullable"], false);
        assert_eq!(v["columns"][1]["type_name"], "DECIMAL");
    }

    #[test]
    fn update_permissions_body_skips_empty_lists() {
        // `add` empty / `remove` set — the revoke shape. Empty `add`
        // is skipped on the wire so Unity doesn't see an empty array
        // it might reject.
        let body = UpdatePermissionsBody {
            changes: vec![PermissionChange {
                principal: "engineers".into(),
                add: vec![],
                remove: vec!["SELECT".into()],
            }],
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(!json.contains("\"add\""), "empty add should be skipped");
        assert!(json.contains("\"remove\""), "remove must be present");
        assert!(json.contains("\"principal\":\"engineers\""));
    }

    #[test]
    fn update_permissions_body_apply_shape() {
        // `add` set / `remove` empty — the apply shape.
        let body = UpdatePermissionsBody {
            changes: vec![PermissionChange {
                principal: "engineers".into(),
                add: vec!["SELECT".into()],
                remove: vec![],
            }],
        };
        let json = serde_json::to_string(&body).unwrap();
        assert!(json.contains("\"add\""));
        assert!(
            !json.contains("\"remove\""),
            "empty remove should be skipped"
        );
    }

    // ---- response body deserialisation ----

    #[test]
    fn table_info_deserialises_columns() {
        let json = r#"{
            "name": "orders",
            "columns": [
                {"name": "id", "type_text": "bigint", "type_name": "LONG", "nullable": false},
                {"name": "amount", "type_text": "decimal(10,2)", "type_name": "DECIMAL", "nullable": true}
            ]
        }"#;
        let info: TableInfo = serde_json::from_str(json).unwrap();
        assert_eq!(info.columns.len(), 2);
        assert_eq!(info.columns[0].name, "id");
        assert_eq!(info.columns[0].type_text.as_deref(), Some("bigint"));
        assert_eq!(info.columns[0].nullable, Some(false));
    }

    #[test]
    fn list_tables_response_handles_pagination_token() {
        let json = r#"{
            "tables": [
                {"name": "orders", "columns": []},
                {"name": "customers", "columns": []}
            ],
            "next_page_token": "token-abc"
        }"#;
        let resp: ListTablesResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.tables.len(), 2);
        assert_eq!(resp.next_page_token.as_deref(), Some("token-abc"));
    }

    #[test]
    fn list_tables_response_no_token_terminates_pagination() {
        let json = r#"{"tables": []}"#;
        let resp: ListTablesResponse = serde_json::from_str(json).unwrap();
        assert!(resp.next_page_token.is_none());
    }

    #[test]
    fn permissions_response_flattens_assignments_to_grants() {
        // Unity returns one assignment per principal with the
        // principal's list of privileges; the trait surface flattens
        // it to one Grant per (principal, privilege) pair.
        let json = r#"{
            "privilege_assignments": [
                {"principal": "engineers", "privileges": ["SELECT", "MODIFY"]},
                {"principal": "analysts", "privileges": ["SELECT"]}
            ]
        }"#;
        let resp: PermissionsResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.privilege_assignments.len(), 2);
        assert_eq!(resp.privilege_assignments[0].privileges.len(), 2);
    }

    // ---- unsupported operations ----

    #[tokio::test]
    async fn tag_table_returns_unsupported() {
        let client = make_offline_client();
        let err = client
            .tag_table(&sample_table_ref(), "k", "v")
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn table_stats_returns_unsupported() {
        // Unity Catalog REST exposes no stats surface. The trait
        // contract is to fail with UnsupportedOperation before any
        // network hop so callers route to the SQL-side stats path
        // (DESCRIBE DETAIL / ANALYZE TABLE on DatabricksConnector).
        // This is consistent with tag_table / commit_transaction /
        // list_branches — all surfaces Unity REST does not natively
        // serve.
        let client = make_offline_client();
        let err = client.table_stats(&sample_table_ref()).await.unwrap_err();
        assert!(
            matches!(err, CatalogError::UnsupportedOperation(_)),
            "expected UnsupportedOperation, got {err:?}"
        );
    }

    #[tokio::test]
    async fn list_branches_returns_unsupported() {
        let client = make_offline_client();
        let err = client.list_branches(&sample_table_ref()).await.unwrap_err();
        assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
    }

    #[tokio::test]
    async fn commit_transaction_returns_unsupported() {
        let client = make_offline_client();
        let err = client.commit_transaction(&[]).await.unwrap_err();
        assert!(matches!(err, CatalogError::UnsupportedOperation(_)));
    }

    // ---- shape gating fires before network ----

    #[tokio::test]
    async fn describe_table_with_invalid_shape_fails_offline() {
        let client = make_offline_client();
        let bad = TableRef {
            catalog: None,
            namespace: vec!["s".into()],
            name: "t".into(),
        };
        let err = client.describe_table(&bad).await.unwrap_err();
        // Hits the unity_parts gate before any HTTP request — no
        // network call required for this test to be deterministic.
        assert!(matches!(err, CatalogError::InvalidResponse(_)));
    }

    #[tokio::test]
    async fn list_tables_rejects_wrong_namespace_shape() {
        let client = make_offline_client();
        // One part instead of two — fails the shape gate.
        let err = client
            .list_tables(&["only_one".to_string()])
            .await
            .unwrap_err();
        assert!(matches!(err, CatalogError::InvalidResponse(_)));
    }

    // ---- governance multi-change batching helpers ----

    #[test]
    fn securable_path_slug_matches_unity_endpoints() {
        assert_eq!(
            securable_path_slug(&Securable::Catalog { name: "c".into() }),
            "catalog"
        );
        assert_eq!(
            securable_path_slug(&Securable::Schema {
                catalog: "c".into(),
                name: "s".into(),
            }),
            "schema"
        );
        assert_eq!(
            securable_path_slug(&Securable::Table {
                catalog: "c".into(),
                schema: "s".into(),
                name: "t".into(),
            }),
            "table"
        );
    }

    #[test]
    fn securable_full_name_renders_three_levels() {
        assert_eq!(
            securable_full_name(&Securable::Catalog {
                name: "cat_a".into()
            }),
            "cat_a"
        );
        assert_eq!(
            securable_full_name(&Securable::Schema {
                catalog: "cat_a".into(),
                name: "sch_a".into(),
            }),
            "cat_a.sch_a"
        );
        assert_eq!(
            securable_full_name(&Securable::Table {
                catalog: "cat_a".into(),
                schema: "sch_a".into(),
                name: "orders_a".into(),
            }),
            "cat_a.sch_a.orders_a"
        );
    }

    #[test]
    fn securable_full_name_percent_encodes_dots() {
        // A literal `.` inside an identifier must be percent-encoded so
        // it can't forge an extra path segment. The `.` between parts is
        // the only one left bare.
        let s = Securable::Schema {
            catalog: "weird.name".into(),
            name: "sch".into(),
        };
        assert_eq!(securable_full_name(&s), "weird%2Ename.sch");
    }

    #[test]
    fn group_grants_collapses_by_principal_into_one_change_each() {
        // Three grants across two principals — must collapse to two
        // PermissionChange entries, principals sorted (BTreeMap order).
        // This is the assertion that prevents an impl from looping
        // one-grant-per-call: the change set is N principals, not N
        // grants.
        let grants = vec![
            Grant {
                principal: "alice".into(),
                privilege: "SELECT".into(),
            },
            Grant {
                principal: "alice".into(),
                privilege: "MODIFY".into(),
            },
            Grant {
                principal: "bob".into(),
                privilege: "SELECT".into(),
            },
        ];
        let changes = group_grants_into_changes(&grants, /*adding=*/ true);
        assert_eq!(changes.len(), 2, "two distinct principals -> two changes");
        assert_eq!(changes[0].principal, "alice");
        assert_eq!(changes[0].add, vec!["SELECT", "MODIFY"]);
        assert!(changes[0].remove.is_empty());
        assert_eq!(changes[1].principal, "bob");
        assert_eq!(changes[1].add, vec!["SELECT"]);
    }

    #[test]
    fn group_grants_revoke_shape_lands_in_remove_slot() {
        let grants = vec![Grant {
            principal: "alice".into(),
            privilege: "SELECT".into(),
        }];
        let changes = group_grants_into_changes(&grants, /*adding=*/ false);
        assert_eq!(changes.len(), 1);
        assert!(changes[0].add.is_empty());
        assert_eq!(changes[0].remove, vec!["SELECT"]);
    }

    #[test]
    fn group_grants_empty_input_yields_no_changes() {
        let changes = group_grants_into_changes(&[], true);
        assert!(changes.is_empty());
    }

    #[tokio::test]
    async fn governance_apply_grants_with_empty_slice_is_noop() {
        // Empty input must short-circuit before any HTTP traffic — the
        // offline client has no base-url override but the call returns
        // Ok without ever touching the network.
        let client = make_offline_client();
        client
            .apply_grants(
                &Securable::Catalog {
                    name: "cat_a".into(),
                },
                &[],
            )
            .await
            .expect("empty apply_grants is a noop");
        client
            .revoke_grants(
                &Securable::Schema {
                    catalog: "cat_a".into(),
                    name: "sch_a".into(),
                },
                &[],
            )
            .await
            .expect("empty revoke_grants is a noop");
    }

    // ---- debug redaction ----

    #[test]
    fn debug_does_not_leak_secrets() {
        let auth = Auth::from_config(crate::auth::AuthConfig {
            host: "test.databricks.com".into(),
            token: Some("dapi_secret_value".into()),
            client_id: None,
            client_secret: None,
        })
        .unwrap();
        let client = UnityCatalogClient::new("test.databricks.com".into(), auth);
        let dbg = format!("{client:?}");
        assert!(!dbg.contains("dapi_secret_value"));
    }

    fn make_offline_client() -> UnityCatalogClient {
        let auth = Auth::from_config(crate::auth::AuthConfig {
            host: "offline.databricks.test".into(),
            token: Some("offline_test_token".into()),
            client_id: None,
            client_secret: None,
        })
        .expect("auth from PAT is infallible");
        UnityCatalogClient::new("offline.databricks.test".into(), auth)
    }
}

// ---------------------------------------------------------------------------
// Live integration test (gated)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod live_tests {
    //! Live integration tests against a real Databricks workspace.
    //!
    //! Gated on `ROCKY_TEST_DATABRICKS_HOST` + auth env vars per the
    //! sandbox-identifier-scrubbing convention; `#[ignore]` keeps them
    //! out of the default `cargo test` run. To exercise:
    //!
    //! ```sh
    //! ROCKY_TEST_DATABRICKS_HOST=... \
    //! ROCKY_TEST_DATABRICKS_TOKEN=... \
    //! ROCKY_TEST_DATABRICKS_CATALOG=hcv2_... \
    //! ROCKY_TEST_DATABRICKS_SCHEMA=hcv2_... \
    //! cargo test -p rocky-databricks --test unity_catalog_live -- --ignored
    //! ```
    //!
    //! Any resource the live tests create must use the `hcv2_` prefix
    //! convention to keep sandbox bookkeeping separable from production.

    use super::*;

    fn read_env() -> Option<(String, String, String, String)> {
        let host = std::env::var("ROCKY_TEST_DATABRICKS_HOST").ok()?;
        let token = std::env::var("ROCKY_TEST_DATABRICKS_TOKEN").ok()?;
        let catalog = std::env::var("ROCKY_TEST_DATABRICKS_CATALOG").ok()?;
        let schema = std::env::var("ROCKY_TEST_DATABRICKS_SCHEMA").ok()?;
        Some((host, token, catalog, schema))
    }

    /// Optional table name for the describe-table live test. If unset,
    /// the test no-ops with a `eprintln!` skip instead of failing —
    /// keeps the test surface usable against any sandbox where the
    /// caller has read-only catalog access but no known table.
    fn read_describe_target() -> Option<(String, String, String, String, String)> {
        let (host, token, catalog, schema) = read_env()?;
        let table = std::env::var("ROCKY_TEST_DATABRICKS_TABLE").ok()?;
        Some((host, token, catalog, schema, table))
    }

    fn live_client(host: String, token: String) -> UnityCatalogClient {
        let auth = Auth::from_config(crate::auth::AuthConfig {
            host: host.clone(),
            token: Some(token),
            client_id: None,
            client_secret: None,
        })
        .expect("PAT auth is infallible");
        UnityCatalogClient::new(host, auth)
    }

    #[tokio::test]
    #[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
    async fn live_list_tables_in_existing_schema() {
        let Some((host, token, catalog, schema)) = read_env() else {
            eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
            return;
        };
        let client = live_client(host, token);
        let tables = client
            .list_tables(&[catalog, schema])
            .await
            .expect("list_tables against live Unity Catalog");
        // We don't assert a count — the live schema may be empty.
        // The smoke check is that the call returns Ok and the
        // TableRef shape is well-formed.
        for t in tables.iter().take(5) {
            assert!(t.catalog.is_some());
            assert_eq!(t.namespace.len(), 1);
            assert!(!t.name.is_empty());
        }
    }

    #[tokio::test]
    #[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
    async fn live_table_stats_returns_unsupported_operation() {
        // Anchor the "Unity REST exposes no stats endpoint" contract
        // against a live workspace: the impl must short-circuit with
        // UnsupportedOperation before any HTTP traffic. Sandbox stats
        // (rows / bytes) come through the SQL path on
        // DatabricksConnector, not this client.
        let Some((host, token, catalog, schema)) = read_env() else {
            eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
            return;
        };
        let client = live_client(host, token);
        let table =
            std::env::var("ROCKY_TEST_DATABRICKS_TABLE").unwrap_or_else(|_| "hc_probe".into());
        let err = client
            .table_stats(&TableRef {
                catalog: Some(catalog),
                namespace: vec![schema],
                name: table,
            })
            .await
            .expect_err("Unity table_stats must surface as UnsupportedOperation");
        assert!(
            matches!(err, CatalogError::UnsupportedOperation(_)),
            "expected UnsupportedOperation, got {err:?}"
        );
    }

    #[tokio::test]
    #[ignore = "requires ROCKY_TEST_DATABRICKS_* env vars; run with --ignored"]
    async fn live_governance_list_grants_at_schema_returns_ok() {
        // Read-only smoke test for the new `GovernanceCatalogClient` impl:
        // listing grants on the sandbox schema must round-trip cleanly
        // (the schema may have zero principals assigned — we don't assert
        // a count, only that the call returns `Ok`). No PATCH is
        // exercised here to avoid mutating sandbox state without an
        // explicit cleanup; the multi-change PATCH receipt is covered by
        // the wiremock test in `tests/unity_catalog_client_wiremock.rs`.
        let Some((host, token, catalog, schema)) = read_env() else {
            eprintln!("skipping: ROCKY_TEST_DATABRICKS_* not set");
            return;
        };
        let client = live_client(host, token);
        let securable = Securable::Schema {
            catalog,
            name: schema,
        };
        let grants = client
            .list_grants(&securable)
            .await
            .expect("list_grants against live Unity Catalog");
        for g in grants.iter().take(5) {
            assert!(!g.principal.is_empty());
            assert!(!g.privilege.is_empty());
        }
    }

    #[tokio::test]
    #[ignore = "requires ROCKY_TEST_DATABRICKS_* + _TABLE env vars; run with --ignored"]
    async fn live_describe_table_returns_columns() {
        let Some((host, token, catalog, schema, table)) = read_describe_target() else {
            eprintln!("skipping: ROCKY_TEST_DATABRICKS_TABLE not set");
            return;
        };
        let client = live_client(host, token);
        let schema_resp = client
            .describe_table(&TableRef {
                catalog: Some(catalog),
                namespace: vec![schema],
                name: table,
            })
            .await
            .expect("describe_table against live Unity Catalog");
        // The smoke check is that the call returns Ok with a non-empty
        // column list — we don't assert exact contents (the sandbox
        // table may evolve).
        assert!(
            !schema_resp.columns.is_empty(),
            "expected at least one column on the live table"
        );
    }
}
