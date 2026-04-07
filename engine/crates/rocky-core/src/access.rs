//! Partial access tracking for catalog operations.
//!
//! When discovery or doctor encounters permission errors (403) on some
//! schemas or catalogs, it continues with accessible resources and
//! tracks the denied ones for reporting.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Tracks resources that were accessible vs denied during catalog operations.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct AccessReport {
    /// Resources that were successfully accessed.
    pub accessible: Vec<AccessedResource>,
    /// Resources where access was denied.
    pub denied: Vec<DeniedResource>,
    /// Total resources attempted.
    pub total_attempted: usize,
}

/// A successfully accessed resource.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct AccessedResource {
    /// Resource type (catalog, schema, table).
    pub resource_type: ResourceType,
    /// Fully qualified name.
    pub name: String,
}

/// A resource where access was denied.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DeniedResource {
    /// Resource type (catalog, schema, table).
    pub resource_type: ResourceType,
    /// Fully qualified name.
    pub name: String,
    /// The error message from the warehouse.
    pub error: String,
    /// Suggested remediation (e.g., "GRANT USE CATALOG ON catalog TO ...").
    pub remediation: Option<String>,
}

/// Type of catalog resource.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ResourceType {
    Catalog,
    Schema,
    Table,
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ResourceType::Catalog => write!(f, "catalog"),
            ResourceType::Schema => write!(f, "schema"),
            ResourceType::Table => write!(f, "table"),
        }
    }
}

impl AccessReport {
    /// Create a new empty report.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful access.
    pub fn record_access(&mut self, resource_type: ResourceType, name: impl Into<String>) {
        self.total_attempted += 1;
        self.accessible.push(AccessedResource {
            resource_type,
            name: name.into(),
        });
    }

    /// Record a denied access.
    pub fn record_denied(
        &mut self,
        resource_type: ResourceType,
        name: impl Into<String>,
        error: impl Into<String>,
        remediation: Option<String>,
    ) {
        self.total_attempted += 1;
        self.denied.push(DeniedResource {
            resource_type,
            name: name.into(),
            error: error.into(),
            remediation,
        });
    }

    /// Returns true if any resources were denied.
    pub fn has_denied(&self) -> bool {
        !self.denied.is_empty()
    }

    /// Returns true if all resources were accessible.
    pub fn all_accessible(&self) -> bool {
        self.denied.is_empty()
    }

    /// Returns the percentage of resources that were accessible.
    pub fn access_rate(&self) -> f64 {
        if self.total_attempted == 0 {
            return 100.0;
        }
        (self.accessible.len() as f64 / self.total_attempted as f64) * 100.0
    }

    /// Generate a human-readable summary.
    pub fn summary(&self) -> String {
        if self.all_accessible() {
            return format!("All {} resources accessible", self.total_attempted);
        }

        format!(
            "{} of {} resources accessible ({} denied)",
            self.accessible.len(),
            self.total_attempted,
            self.denied.len(),
        )
    }

    /// Check if an error message indicates a permission denial (403-like).
    pub fn is_permission_error(error_msg: &str) -> bool {
        let lower = error_msg.to_lowercase();
        lower.contains("permission denied")
            || lower.contains("access denied")
            || lower.contains("forbidden")
            || lower.contains("403")
            || lower.contains("insufficient privileges")
            || lower.contains("not authorized")
            || lower.contains("does not have")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mixed_access() {
        let mut report = AccessReport::new();
        report.record_access(ResourceType::Catalog, "cat_a");
        report.record_access(ResourceType::Schema, "cat_a.schema_1");
        report.record_denied(
            ResourceType::Schema,
            "cat_b.schema_private",
            "Permission denied: insufficient privileges",
            Some("GRANT USE SCHEMA ON cat_b.schema_private TO current_user".into()),
        );

        assert!(report.has_denied());
        assert_eq!(report.accessible.len(), 2);
        assert_eq!(report.denied.len(), 1);
        assert!((report.access_rate() - 66.67).abs() < 1.0);
    }

    #[test]
    fn test_all_accessible() {
        let mut report = AccessReport::new();
        report.record_access(ResourceType::Catalog, "cat");
        assert!(report.all_accessible());
        assert_eq!(report.summary(), "All 1 resources accessible");
    }

    #[test]
    fn test_permission_error_detection() {
        assert!(AccessReport::is_permission_error("Permission denied"));
        assert!(AccessReport::is_permission_error("HTTP 403 Forbidden"));
        assert!(AccessReport::is_permission_error(
            "User does not have USE CATALOG"
        ));
        assert!(AccessReport::is_permission_error(
            "Access denied to resource"
        ));
        assert!(!AccessReport::is_permission_error("Table not found"));
        assert!(!AccessReport::is_permission_error("Timeout"));
    }

    #[test]
    fn test_empty_report() {
        let report = AccessReport::new();
        assert!(report.all_accessible());
        assert_eq!(report.access_rate(), 100.0);
    }
}
