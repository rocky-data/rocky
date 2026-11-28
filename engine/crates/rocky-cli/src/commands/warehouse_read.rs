//! What a warehouse-reading command shares with the `rocky serve` route that
//! wraps it: the consent gate, the way the adapters are opened, and the errors
//! the route maps onto HTTP.

use std::collections::BTreeSet;
use std::time::Instant;

use rocky_core::config::RockyConfig;

use super::PreviewFailure;
use super::preview_rows::require_warehouse_consent;
use crate::registry::{AdapterRegistry, WarehouseUnavailable};

/// Who is asking for a warehouse read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WarehouseAccess {
    /// A command line: every adapter is built as configured.
    Cli,
    /// A `GET` route a read-only viewer may call. Remote adapters need
    /// `allow_warehouse`, only the adapters the call can query are built,
    /// DuckDB is opened read-only, and the answer carries no warehouse error
    /// text.
    ReadRoute {
        allow_warehouse: bool,
        /// No new warehouse statement starts after this instant. The one in
        /// flight runs to its end.
        deadline: Option<Instant>,
    },
}

/// Why a read route could not answer, beyond an ordinary error.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ReadRouteError {
    /// A remote adapter and no consent.
    #[error("{}", .0.message)]
    Consent(PreviewFailure),
    /// An adapter could not be built.
    #[error("{0}")]
    Adapter(String),
    /// The requested pipeline cannot be used.
    #[error("{0}")]
    Pipeline(String),
    /// The route's deadline passed before the next statement.
    #[error("the read passed its deadline")]
    Deadline,
}

impl WarehouseAccess {
    /// Refuse to start another warehouse statement once the deadline passed,
    /// so a read the route gave up on does not keep querying.
    pub(crate) fn check_deadline(self) -> Result<(), ReadRouteError> {
        match self {
            Self::ReadRoute {
                deadline: Some(deadline),
                ..
            } if Instant::now() >= deadline => Err(ReadRouteError::Deadline),
            Self::Cli | Self::ReadRoute { .. } => Ok(()),
        }
    }

    /// Build the adapters for a call that can query `touched` and no others.
    ///
    /// For [`WarehouseAccess::ReadRoute`] the consent gate covers every name
    /// in `touched` and runs BEFORE any adapter is built.
    pub(crate) fn registry(
        self,
        config: &RockyConfig,
        touched: &BTreeSet<String>,
        action: &str,
    ) -> anyhow::Result<AdapterRegistry> {
        let Self::ReadRoute {
            allow_warehouse, ..
        } = self
        else {
            return AdapterRegistry::from_config(config);
        };
        for name in touched {
            let adapter_type = config
                .adapters
                .get(name)
                .map_or("unknown", |a| a.adapter_type.as_str());
            require_warehouse_consent(action, name, adapter_type, allow_warehouse)
                .map_err(ReadRouteError::Consent)?;
        }
        AdapterRegistry::from_config_read_only(config, touched).map_err(|e| {
            match e.downcast::<WarehouseUnavailable>() {
                Ok(unavailable) => anyhow::Error::new(unavailable),
                Err(other) => anyhow::Error::new(ReadRouteError::Adapter(format!("{other:#}"))),
            }
        })
    }
}

/// Test hook: hold a route's warehouse read of the named config for this long,
/// so a test can watch the route's deadline fire while the work goes on.
#[cfg(test)]
pub(crate) static READ_HOLD_FOR_TEST: std::sync::Mutex<
    Option<(std::path::PathBuf, std::time::Duration)>,
> = std::sync::Mutex::new(None);

#[cfg(test)]
pub(crate) async fn hold_read_for_test(config_path: &std::path::Path) {
    let hold = READ_HOLD_FOR_TEST
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone();
    if let Some((held_path, delay)) = hold
        && held_path == config_path
    {
        tokio::time::sleep(delay).await;
    }
}
