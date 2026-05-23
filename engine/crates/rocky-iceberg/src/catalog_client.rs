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
//! - [`CatalogClient::create_table`] issues
//!   `POST /v1/namespaces/{ns}/tables`. 404 from the parent endpoint is
//!   rewritten from the default `TableNotFound` to `NamespaceNotFound`
//!   because a missing-parent is the only plausible 404 on table
//!   creation.
//! - [`CatalogClient::drop_table`] issues
//!   `DELETE /v1/namespaces/{ns}/tables/{name}`. Default 404 mapping
//!   (`TableNotFound`) is the right one.
//! - [`CatalogClient::commit_transaction`] issues
//!   `POST /v1/transactions/commit` with one entry per [`TableCommit`].
//!   The trait's CAS contract is encoded as Iceberg
//!   `assert-ref-snapshot-id` (when `expected_snapshot_id` is set) or
//!   `assert-create` (when it is `None`) on the `main` branch, paired
//!   with a `set-snapshot-ref` update advancing `main` to
//!   `new_snapshot_id`. See [`CatalogClient::commit_transaction`] below
//!   for the v1 contract.
//! - [`CatalogClient::list_branches`] reuses `load_table`'s GET and
//!   projects the `refs` map embedded in `TableMetadata` onto
//!   [`Vec<BranchRef>`].
//! - The four governance methods ([`CatalogClient::tag_table`],
//!   [`CatalogClient::get_grants`], [`CatalogClient::apply_grant`],
//!   [`CatalogClient::revoke_grant`]) return
//!   [`CatalogError::UnsupportedOperation`] unconditionally — the
//!   Iceberg REST spec exposes no governance endpoints, so the gap is
//!   permanent rather than "not yet".

use async_trait::async_trait;
use serde_json::json;

use rocky_catalog_core::{
    BranchKind, BranchRef, CatalogClient, CatalogError, CatalogResult, ColumnSchema, Grant,
    TableCommit, TableRef, TableSchema, TableStats,
};

use crate::client::{
    CommitTableIdentifier, CommitTableRequest, CommitTransactionRequest, CreateTableField,
    CreateTableRequest, CreateTableSchema, IcebergCatalogClient, IcebergError,
};

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

    async fn create_table(&self, table: &TableRef, schema: &TableSchema) -> CatalogResult<()> {
        let request = build_create_table_request(&table.name, schema);
        self.inner
            .create_table(&table.namespace, &request)
            .await
            // The endpoint is namespace-scoped: a 404 here means the
            // parent namespace doesn't exist, not the table (which
            // could not exist anyway — that's the point of the call).
            // Rewrite the default `TableNotFound` mapping accordingly.
            .map_err(|e| match icebergerror_into_catalog(e) {
                CatalogError::TableNotFound(msg) => CatalogError::NamespaceNotFound(msg),
                other => other,
            })?;
        Ok(())
    }

    async fn drop_table(&self, table: &TableRef) -> CatalogResult<()> {
        self.inner
            .drop_table(&table.namespace, &table.name)
            .await
            .map_err(icebergerror_into_catalog)?;
        Ok(())
    }

    async fn commit_transaction(&self, commits: &[TableCommit]) -> CatalogResult<()> {
        // Empty slice short-circuits: the spec accepts an empty
        // `table-changes` list, but calling the network for a no-op is
        // wasteful and obscures the failure mode if the server rejects
        // it.
        if commits.is_empty() {
            return Ok(());
        }

        let table_changes = commits.iter().map(build_commit_table_request).collect();
        let request = CommitTransactionRequest { table_changes };

        self.inner
            .commit_transaction(&request)
            .await
            .map_err(icebergerror_into_catalog)?;
        Ok(())
    }

    async fn list_branches(&self, table: &TableRef) -> CatalogResult<Vec<BranchRef>> {
        let resp = self
            .inner
            .load_table(&table.namespace, &table.name)
            .await
            .map_err(icebergerror_into_catalog)?;

        // `refs` is an unordered map on the wire; collect first then
        // sort by name to keep the projection deterministic across
        // calls. Stable output matters for callers that diff
        // branch-set snapshots.
        let mut out: Vec<BranchRef> = resp
            .metadata
            .refs
            .into_iter()
            .map(|(name, snap_ref)| BranchRef {
                name,
                snapshot_id: Some(snap_ref.snapshot_id),
                kind: parse_branch_kind(&snap_ref.kind),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn table_stats(&self, table: &TableRef) -> CatalogResult<TableStats> {
        let resp = self
            .inner
            .load_table(&table.namespace, &table.name)
            .await
            .map_err(icebergerror_into_catalog)?;

        let metadata = resp.metadata;
        // Find the snapshot that matches `current-snapshot-id`. If the
        // table has no current snapshot (freshly created) or the
        // catalog elided the snapshots list, return empty stats — the
        // table exists in the catalog but no summary keys are
        // populated yet.
        let Some(current_id) = metadata.current_snapshot_id else {
            return Ok(TableStats::empty());
        };
        let Some(snapshot) = metadata
            .snapshots
            .iter()
            .find(|s| s.snapshot_id == current_id)
        else {
            return Ok(TableStats::empty());
        };
        Ok(parse_snapshot_summary(&snapshot.summary))
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

/// Build the Iceberg [`CreateTableRequest`] body from the
/// catalog-agnostic [`TableSchema`].
///
/// Field ids are assigned sequentially from 1 — Iceberg requires every
/// field to have a stable id and the trait-level [`TableSchema`] does
/// not carry one. `required` is the inverse of `nullable`. The
/// `type_str` is forwarded as-is into a JSON string value; Iceberg's
/// wire format accepts primitive type names (`"long"`, `"string"`,
/// `"double"`, ...) as bare strings, so the common case is a clean
/// round-trip. Type strings that were originally a nested JSON object
/// (lists, maps, structs) come through `describe_table` as a serialised
/// JSON string and would arrive here as that string — this is the
/// known one-way trip noted in [`render_type`]. Callers with richer
/// schema information (full Iceberg `Schema`) should call
/// [`IcebergCatalogClient::create_table`] directly.
fn build_create_table_request(name: &str, schema: &TableSchema) -> CreateTableRequest {
    let fields = schema
        .columns
        .iter()
        .enumerate()
        .map(|(idx, col)| CreateTableField {
            id: (idx as i64) + 1,
            name: col.name.clone(),
            required: !col.nullable,
            type_repr: serde_json::Value::String(col.type_str.clone()),
        })
        .collect();

    CreateTableRequest {
        name: name.to_owned(),
        schema: CreateTableSchema {
            schema_type: "struct",
            schema_id: 0,
            fields,
        },
    }
}

/// Build one `CommitTableRequest` from a catalog-agnostic [`TableCommit`].
///
/// The translation maps the trait's CAS contract onto two Iceberg
/// primitives:
///
/// - [`TableCommit::expected_snapshot_id`] = `Some(id)` → an
///   `assert-ref-snapshot-id` requirement on the selected ref pinning
///   the base snapshot to `id`. A concurrent writer that landed against
///   the same base fails this CAS and surfaces as
///   [`CatalogError::CommitConflict`].
/// - [`TableCommit::expected_snapshot_id`] = `None` → an `assert-create`
///   requirement, used when the commit creates the table.
///
/// The synthesized update is a `set-snapshot-ref` advancing the same
/// ref to [`TableCommit::new_snapshot_id`]. The ref defaults to `"main"`
/// when [`TableCommit::branch`] is `None`, preserving the v1
/// single-branch contract.
///
/// [`TableCommit::extra_requirements`] and [`TableCommit::extra_updates`]
/// are appended *after* the synthesized pair. The order is load-bearing:
/// the synthesized assert + advance always fire first so the CAS holds
/// regardless of whatever schema / partition-spec entries the caller
/// threaded through.
fn build_commit_table_request(commit: &TableCommit) -> CommitTableRequest {
    let ref_name = commit.branch.as_deref().unwrap_or("main");

    let synth_requirement = match commit.expected_snapshot_id {
        Some(snapshot_id) => json!({
            "type": "assert-ref-snapshot-id",
            "ref": ref_name,
            "snapshot-id": snapshot_id,
        }),
        None => json!({
            "type": "assert-create",
        }),
    };

    let synth_update = json!({
        "action": "set-snapshot-ref",
        "ref-name": ref_name,
        "type": "branch",
        "snapshot-id": commit.new_snapshot_id,
    });

    // Synthesised entries first, then caller-supplied passthrough
    // payload. `Vec::with_capacity` lets the buffer grow without
    // intermediate reallocations when callers thread schema /
    // partition-spec updates through.
    let mut requirements = Vec::with_capacity(1 + commit.extra_requirements.len());
    requirements.push(synth_requirement);
    requirements.extend(commit.extra_requirements.iter().cloned());

    let mut updates = Vec::with_capacity(1 + commit.extra_updates.len());
    updates.push(synth_update);
    updates.extend(commit.extra_updates.iter().cloned());

    CommitTableRequest {
        identifier: CommitTableIdentifier {
            namespace: commit.table.namespace.clone(),
            name: commit.table.name.clone(),
        },
        requirements,
        updates,
    }
}

/// Distil an Iceberg snapshot's `summary` map into a catalog-agnostic
/// [`TableStats`].
///
/// The well-known keys the Iceberg spec defines for a snapshot summary
/// are documented at
/// <https://iceberg.apache.org/spec/#snapshot-summary>:
/// `total-records` (cumulative row count), `total-files-size`
/// (cumulative bytes across all data files), `total-data-files`
/// (cumulative file count), plus delta keys (`added-records`,
/// `deleted-records`, etc.) describing the most recent op. Only the
/// cumulative keys go into [`TableStats`]; the delta keys are
/// per-snapshot relative metrics and would mislead a cost-model that
/// needs the current shape of the table.
///
/// Each key is independently optional — older writers may emit a
/// subset, and the Unity-foreign-Iceberg path is known to lag spec
/// compliance on summary completeness. Unparseable string values
/// silently produce `None` rather than failing the whole call; the
/// caller's cost-model treats `None` fields as "fall through to
/// upstream-inferred estimate" rather than a hard failure.
fn parse_snapshot_summary(summary: &std::collections::HashMap<String, String>) -> TableStats {
    fn read_u64(summary: &std::collections::HashMap<String, String>, key: &str) -> Option<u64> {
        summary.get(key).and_then(|s| s.parse::<u64>().ok())
    }
    TableStats {
        row_count: read_u64(summary, "total-records"),
        total_bytes: read_u64(summary, "total-files-size"),
        file_count: read_u64(summary, "total-data-files"),
    }
}

/// Project an Iceberg `SnapshotReference.type` string onto the
/// catalog-agnostic [`BranchKind`].
///
/// The spec defines `"branch"` and `"tag"`; future spec revisions could
/// in principle add more variants. We default unknown kinds to
/// `Branch` because that is the safer assumption for mutation-policy
/// callers — treating an unfamiliar ref as a *tag* (immutable) could
/// hide real divergence, whereas treating it as a branch surfaces it.
fn parse_branch_kind(s: &str) -> BranchKind {
    match s {
        "tag" => BranchKind::Tag,
        _ => BranchKind::Branch,
    }
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

    #[test]
    fn build_create_table_request_assigns_sequential_ids() {
        let schema = TableSchema {
            columns: vec![
                ColumnSchema {
                    name: "id".into(),
                    type_str: "long".into(),
                    nullable: false,
                },
                ColumnSchema {
                    name: "name".into(),
                    type_str: "string".into(),
                    nullable: true,
                },
            ],
        };
        let req = build_create_table_request("orders", &schema);
        assert_eq!(req.name, "orders");
        assert_eq!(req.schema.schema_type, "struct");
        assert_eq!(req.schema.schema_id, 0);
        assert_eq!(req.schema.fields.len(), 2);

        assert_eq!(req.schema.fields[0].id, 1);
        assert_eq!(req.schema.fields[0].name, "id");
        assert!(req.schema.fields[0].required, "non-nullable → required");
        assert_eq!(
            req.schema.fields[0].type_repr,
            serde_json::Value::String("long".into())
        );

        assert_eq!(req.schema.fields[1].id, 2);
        assert_eq!(req.schema.fields[1].name, "name");
        assert!(
            !req.schema.fields[1].required,
            "nullable → required = false"
        );
    }

    #[test]
    fn build_create_table_request_serialises_to_spec_wire_format() {
        let schema = TableSchema {
            columns: vec![ColumnSchema {
                name: "id".into(),
                type_str: "long".into(),
                nullable: false,
            }],
        };
        let req = build_create_table_request("orders", &schema);
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v["name"], "orders");
        assert_eq!(v["schema"]["type"], "struct");
        assert_eq!(v["schema"]["schema-id"], 0);
        assert_eq!(v["schema"]["fields"][0]["id"], 1);
        assert_eq!(v["schema"]["fields"][0]["name"], "id");
        assert_eq!(v["schema"]["fields"][0]["required"], true);
        assert_eq!(v["schema"]["fields"][0]["type"], "long");
    }

    #[test]
    fn build_commit_table_request_uses_assert_ref_when_base_pinned() {
        let commit = TableCommit::new(
            TableRef {
                catalog: None,
                namespace: vec!["analytics".into()],
                name: "orders".into(),
            },
            Some(42),
            43,
        );
        let req = build_commit_table_request(&commit);
        assert_eq!(req.identifier.namespace, vec!["analytics".to_string()]);
        assert_eq!(req.identifier.name, "orders");
        assert_eq!(req.requirements.len(), 1);
        assert_eq!(req.requirements[0]["type"], "assert-ref-snapshot-id");
        assert_eq!(req.requirements[0]["ref"], "main");
        assert_eq!(req.requirements[0]["snapshot-id"], 42);

        assert_eq!(req.updates.len(), 1);
        assert_eq!(req.updates[0]["action"], "set-snapshot-ref");
        assert_eq!(req.updates[0]["ref-name"], "main");
        assert_eq!(req.updates[0]["type"], "branch");
        assert_eq!(req.updates[0]["snapshot-id"], 43);
    }

    #[test]
    fn build_commit_table_request_uses_assert_create_when_no_base() {
        let commit = TableCommit::new(
            TableRef {
                catalog: None,
                namespace: vec!["analytics".into()],
                name: "orders".into(),
            },
            None,
            1,
        );
        let req = build_commit_table_request(&commit);
        assert_eq!(req.requirements[0]["type"], "assert-create");
    }

    #[test]
    fn build_commit_table_request_targets_named_branch() {
        // Passing `branch = Some("dev_feature")` pins the assertion against
        // that ref and points the `set-snapshot-ref` update at the same
        // name. The v1 contract (None → "main") is checked above; this
        // pins the new multi-ref path.
        let commit = TableCommit::new(
            TableRef {
                catalog: None,
                namespace: vec!["analytics".into()],
                name: "orders".into(),
            },
            Some(42),
            43,
        )
        .with_branch(Some("dev_feature".into()));

        let req = build_commit_table_request(&commit);
        assert_eq!(req.requirements[0]["type"], "assert-ref-snapshot-id");
        assert_eq!(req.requirements[0]["ref"], "dev_feature");
        assert_eq!(req.requirements[0]["snapshot-id"], 42);
        assert_eq!(req.updates[0]["action"], "set-snapshot-ref");
        assert_eq!(req.updates[0]["ref-name"], "dev_feature");
        assert_eq!(req.updates[0]["type"], "branch");
        assert_eq!(req.updates[0]["snapshot-id"], 43);
    }

    #[test]
    fn build_commit_table_request_threads_extra_payload() {
        // Schema + partition-spec mutations land as opaque JSON blobs
        // appended after the synthesized assert + set-snapshot-ref pair.
        // The order is load-bearing: the synthesized entries are always
        // emitted first so the assertion / advance fires whether or not
        // the caller adds extras.
        let extra_requirement = serde_json::json!({
            "type": "assert-last-assigned-field-id",
            "last-assigned-field-id": 7,
        });
        let extra_update_add_schema = serde_json::json!({
            "action": "add-schema",
            "schema": {
                "type": "struct",
                "schema-id": 1,
                "fields": [
                    { "id": 1, "name": "id", "required": true, "type": "long" },
                    { "id": 2, "name": "name", "required": false, "type": "string" }
                ]
            }
        });
        let extra_update_set_spec = serde_json::json!({
            "action": "add-partition-spec",
            "spec": {
                "spec-id": 1,
                "fields": [
                    { "source-id": 1, "field-id": 1000, "name": "id_bucket",
                      "transform": "bucket[16]" }
                ]
            }
        });

        let commit = TableCommit::new(
            TableRef {
                catalog: None,
                namespace: vec!["analytics".into()],
                name: "orders".into(),
            },
            Some(42),
            43,
        )
        .with_extra_requirements(vec![extra_requirement.clone()])
        .with_extra_updates(vec![
            extra_update_add_schema.clone(),
            extra_update_set_spec.clone(),
        ]);

        let req = build_commit_table_request(&commit);
        // Synthesised entries are first; extras follow in order.
        assert_eq!(req.requirements.len(), 2);
        assert_eq!(req.requirements[0]["type"], "assert-ref-snapshot-id");
        assert_eq!(req.requirements[1], extra_requirement);

        assert_eq!(req.updates.len(), 3);
        assert_eq!(req.updates[0]["action"], "set-snapshot-ref");
        assert_eq!(req.updates[1], extra_update_add_schema);
        assert_eq!(req.updates[2], extra_update_set_spec);
    }

    #[test]
    fn table_commit_new_defaults_branch_to_main() {
        // The constructor must leave `branch` as None so the existing
        // call sites keep targeting `main`. Spec: None default IS the
        // backward-compat behaviour.
        let commit = TableCommit::new(
            TableRef {
                catalog: None,
                namespace: vec!["analytics".into()],
                name: "orders".into(),
            },
            None,
            1,
        );
        assert!(commit.branch.is_none());
        assert!(commit.extra_requirements.is_empty());
        assert!(commit.extra_updates.is_empty());
    }

    #[test]
    fn parse_branch_kind_recognises_tag() {
        assert!(matches!(parse_branch_kind("tag"), BranchKind::Tag));
    }

    #[test]
    fn parse_branch_kind_defaults_unknown_to_branch() {
        assert!(matches!(parse_branch_kind("branch"), BranchKind::Branch));
        assert!(matches!(parse_branch_kind("rollback"), BranchKind::Branch));
        assert!(matches!(parse_branch_kind(""), BranchKind::Branch));
    }
}
