//! Errors returned by [`crate::CatalogClient`] implementations.
//!
//! [`CatalogError`] is a library-style error type using `thiserror`. Variants
//! are intentionally coarse-grained — the goal is "library-friendly error
//! type" rather than an exhaustive mapping of every REST status code. Catalog
//! implementations attach the underlying transport / serde / authentication
//! errors via [`CatalogError::Transport`] or [`CatalogError::InvalidResponse`]
//! when richer context is needed.

use std::error::Error as StdError;

/// Result alias used by all [`crate::CatalogClient`] methods.
pub type CatalogResult<T> = Result<T, CatalogError>;

/// Errors that can be returned from a catalog client.
///
/// The variant set is shaped to let callers branch on the cases that have
/// distinct recovery paths:
///
/// - [`CatalogError::TableNotFound`] / [`CatalogError::NamespaceNotFound`]
///   are routine for callers that probe before creating.
/// - [`CatalogError::CommitConflict`] signals a CAS failure on a multi-table
///   transaction; the typical recovery is to re-read state and retry.
/// - [`CatalogError::UnsupportedOperation`] tells callers that this catalog
///   does not expose the requested operation over REST (e.g. Iceberg REST has
///   no tag endpoint today); callers may fall back to an adapter-specific
///   path such as SQL DDL.
/// - The remaining variants ([`CatalogError::Transport`],
///   [`CatalogError::AuthFailed`], [`CatalogError::InvalidResponse`],
///   [`CatalogError::PermissionDenied`]) are general failure modes that
///   surface from the underlying transport or authorization layer.
#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    /// The catalog has no record of the requested table.
    #[error("table not found: {0}")]
    TableNotFound(String),

    /// The catalog has no record of the requested namespace.
    #[error("namespace not found: {0}")]
    NamespaceNotFound(String),

    /// A multi-table transaction failed to commit because the catalog's
    /// compare-and-swap check rejected the new metadata pointer. The
    /// underlying cause is typically a concurrent writer that committed
    /// against the same base snapshot.
    #[error("conflict on commit (CAS failed): {0}")]
    CommitConflict(String),

    /// The catalog does not expose this operation over its REST surface.
    /// Callers may fall back to an adapter-specific path (e.g. SQL DDL).
    #[error("operation not supported by this catalog: {0}")]
    UnsupportedOperation(&'static str),

    /// A transport-level failure (network, TLS, HTTP status code, body parse,
    /// etc.) bubbled up from the catalog client.
    #[error("transport error: {0}")]
    Transport(#[source] Box<dyn StdError + Send + Sync>),

    /// Authentication against the catalog failed. Distinct from
    /// [`CatalogError::PermissionDenied`]: this is "we could not prove who we
    /// are," not "we are not allowed to do this."
    #[error("authentication failed: {0}")]
    AuthFailed(String),

    /// The catalog returned a response that did not match the expected
    /// schema. Wraps a short diagnostic message; the original payload should
    /// be logged at trace level inside the adapter, not surfaced here.
    #[error("invalid response from catalog: {0}")]
    InvalidResponse(String),

    /// The catalog refused the operation on authorization grounds. The
    /// caller is who they say they are, but lacks the required privilege.
    #[error("permission denied: {0}")]
    PermissionDenied(String),
}
