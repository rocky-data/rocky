//! [`GovernanceCatalogClient`] — opt-in governance surface for catalogs that
//! expose RBAC across catalog / schema / table securables.
//!
//! `GovernanceCatalogClient` lives alongside [`crate::CatalogClient`] as a
//! parallel trait. The split is deliberate: the table-CRUD trait converges
//! across every catalog Rocky targets (Iceberg REST, Polaris, Nessie, Unity),
//! whereas RBAC mutation only converges on catalogs that expose a three-level
//! securable model over REST — Unity Catalog today, Polaris over its
//! subcatalog-role surface tomorrow. Adapters that don't serve grants over
//! REST (Iceberg REST today) simply don't implement this trait, and callers
//! detect the absence at the binding site rather than pattern-matching on
//! [`crate::CatalogError::UnsupportedOperation`].
//!
//! ## Why a separate trait
//!
//! - The governance call sites batch `Vec<Grant>` against catalog-, schema-,
//!   or table-shaped securables. Forcing that surface into the table-only
//!   [`crate::CatalogClient::apply_grant`] either loses the catalog/schema
//!   axis or unrolls a batch into N singular calls.
//! - Catalogs without REST RBAC (Iceberg) shouldn't have to ship
//!   [`crate::CatalogError::UnsupportedOperation`] stubs on the table-CRUD
//!   trait just because it grew a securable axis.
//! - A separate trait keeps the table-CRUD trait small and lets adapter
//!   authors opt into governance independently.
//!
//! ## Multi-change wire shape
//!
//! Unity's PATCH endpoint accepts a `changes: Vec<PermissionChange>` per
//! call, one entry per principal. Implementations are expected to coalesce
//! a `Vec<Grant>` into one HTTP request per `Securable`, grouping by
//! principal — see the `rocky-databricks` impl for the canonical shape.

use async_trait::async_trait;

use crate::error::CatalogResult;
use crate::types::Grant;

/// A target for a governance operation.
///
/// Unity Catalog and Apache Polaris both model securables as a three-level
/// namespace (`catalog.schema.table`); this enum is the lowest-common-
/// denominator projection of that model that Rocky's call sites need today.
/// Future variants (e.g. `Column`, `View`, `Function`) can be added without
/// breaking the trait — implementations that don't recognise a new variant
/// return [`crate::CatalogError::UnsupportedOperation`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Securable {
    /// A top-level catalog (`catalog`).
    Catalog {
        /// Catalog name as the implementing catalog stores it.
        name: String,
    },

    /// A schema inside a catalog (`catalog.schema`).
    Schema {
        /// Parent catalog name.
        catalog: String,
        /// Schema name as the implementing catalog stores it.
        name: String,
    },

    /// A table inside a catalog and schema (`catalog.schema.table`).
    Table {
        /// Parent catalog name.
        catalog: String,
        /// Parent schema name.
        schema: String,
        /// Table name as the implementing catalog stores it.
        name: String,
    },
}

impl Securable {
    /// Render `Securable` as a dotted three-level name for error messages
    /// and diagnostics. Not URL-safe and not for use in SQL — implementations
    /// that need a wire-safe rendering build it themselves.
    pub fn display_name(&self) -> String {
        match self {
            Self::Catalog { name } => name.clone(),
            Self::Schema { catalog, name } => format!("{catalog}.{name}"),
            Self::Table {
                catalog,
                schema,
                name,
            } => format!("{catalog}.{schema}.{name}"),
        }
    }
}

/// Governance-side companion to [`crate::CatalogClient`].
///
/// `GovernanceCatalogClient` is an **opt-in** trait. Adapters that don't
/// expose RBAC mutation over REST simply don't implement it; callers detect
/// the absence at the binding site and route through their adapter-specific
/// fallback (typically SQL `GRANT` / `REVOKE`).
///
/// Implementations targeting Unity Catalog, Apache Polaris, or any other
/// catalog that natively serves per-securable PATCH-style RBAC mutation are
/// expected to coalesce a `Vec<Grant>` into **one** REST round-trip per
/// `Securable` — grouping by principal so a single PATCH carries every
/// `(principal, [privilege ...])` pair this call needs. Batching is the
/// concrete win this trait exists to capture; an impl that loops one-grant-
/// per-call defeats the trait's purpose.
///
/// The trait is `Send + Sync` because adapters compose across async task
/// boundaries; `async_trait` keeps it object-safe so callers can hold
/// `Box<dyn GovernanceCatalogClient>` or `Arc<dyn GovernanceCatalogClient>`.
#[async_trait]
pub trait GovernanceCatalogClient: Send + Sync {
    /// Apply every grant in `grants` to `securable` in one round-trip per
    /// principal (or one round-trip per implementation, if the underlying
    /// catalog serves a flat multi-change endpoint).
    ///
    /// `grants` is a slice of `(principal, privilege)` pairs that all share
    /// `securable` as the target. Implementations group by `principal` and
    /// emit a single REST call carrying every `(principal, [privilege ...])`
    /// in the slice. Re-applying a grant that already exists must be a
    /// no-op (idempotent).
    async fn apply_grants(&self, securable: &Securable, grants: &[Grant]) -> CatalogResult<()>;

    /// Revoke every grant in `grants` from `securable` in one round-trip
    /// per principal (or one round-trip per implementation, if the
    /// underlying catalog serves a flat multi-change endpoint).
    ///
    /// Same shape as [`Self::apply_grants`]; the implementation lands the
    /// privilege list in the catalog's `remove` slot rather than `add`.
    /// Revoking a grant that does not exist must be a no-op.
    async fn revoke_grants(&self, securable: &Securable, grants: &[Grant]) -> CatalogResult<()>;

    /// List the grants currently in effect on `securable`.
    ///
    /// Implementations flatten the catalog's per-principal response into a
    /// single `Vec<Grant>` so the caller sees a uniform shape across all
    /// three securable types.
    async fn list_grants(&self, securable: &Securable) -> CatalogResult<Vec<Grant>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn securable_display_catalog() {
        let s = Securable::Catalog {
            name: "hcv2_cat".into(),
        };
        assert_eq!(s.display_name(), "hcv2_cat");
    }

    #[test]
    fn securable_display_schema() {
        let s = Securable::Schema {
            catalog: "hcv2_cat".into(),
            name: "hcv2_sch".into(),
        };
        assert_eq!(s.display_name(), "hcv2_cat.hcv2_sch");
    }

    #[test]
    fn securable_display_table() {
        let s = Securable::Table {
            catalog: "hcv2_cat".into(),
            schema: "hcv2_sch".into(),
            name: "hcv2_orders".into(),
        };
        assert_eq!(s.display_name(), "hcv2_cat.hcv2_sch.hcv2_orders");
    }

    #[test]
    fn securable_equality_distinguishes_variants() {
        let a = Securable::Catalog { name: "x".into() };
        let b = Securable::Schema {
            catalog: "x".into(),
            name: "y".into(),
        };
        assert_ne!(a, b);
    }
}
