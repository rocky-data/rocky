//! Lakehouse-native materialization types.
//!
//! Pure data: format enum, options struct, and the error type the DDL
//! generators raise. The DDL generators themselves live in
//! `rocky_core::lakehouse` because they consume `SqlDialect`, which is
//! the dialect-trait surface in `rocky-core::traits`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use rocky_sql::validation;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors that can occur during lakehouse DDL generation.
#[derive(Debug, Error)]
pub enum LakehouseError {
    #[error("validation error: {0}")]
    Validation(#[from] validation::ValidationError),

    #[error("unsupported format for dialect: {0}")]
    UnsupportedFormat(String),

    /// The target dialect cannot render the lakehouse `format = X` DDL.
    /// Raised by `generate_lakehouse_ddl` when the dialect does not
    /// advertise `supports_lakehouse_format_ddl()` — today every dialect
    /// except Databricks. This is a known limitation of the generator,
    /// not a property of the format itself (Trino does Iceberg
    /// natively); the message names both the format and the dialect.
    #[error("lakehouse format '{format}' DDL is not implemented for adapter/dialect '{dialect}'")]
    DialectUnsupported { format: String, dialect: String },

    #[error("invalid option: {0}")]
    InvalidOption(String),

    /// Managed-Iceberg `format_options` declared a combination the Databricks
    /// warehouse rejects at execution.
    ///
    /// Raised by `generate_lakehouse_ddl` (and surfaced earlier as a
    /// `rocky compile` diagnostic, before any warehouse call) when an Iceberg
    /// model sets mutually-exclusive `partition_by` + `cluster_by`, or an
    /// engine-managed `write.format.*` table property. Without this guard
    /// Databricks rejects the generated DDL at the warehouse on the first run
    /// (`SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED` /
    /// `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`). The message names the
    /// offending option.
    #[error("invalid managed-Iceberg format_options: {0}")]
    ManagedIcebergUnsupported(String),
}

// ---------------------------------------------------------------------------
// Managed-Iceberg format_options validation
// ---------------------------------------------------------------------------

/// A managed-Iceberg `format_options` constraint that Databricks rejects at
/// the warehouse.
///
/// Each carries a ready-to-render `message` (which names the offending
/// option) and an actionable `suggestion`. The compiler maps these onto
/// `E035` diagnostics; the DDL generator turns the first one into a
/// [`LakehouseError::ManagedIcebergUnsupported`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagedIcebergViolation {
    /// Human-readable description that names the offending option(s).
    pub message: String,
    /// Actionable fix.
    pub suggestion: String,
}

/// Does `key` name an engine-managed Iceberg table property?
///
/// Databricks **managed Iceberg** owns the on-disk write format, so it rejects
/// any `write.format.*` table property (e.g. `write.format.default`) with
/// `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED`. The reserved set is deliberately
/// narrow — only the `write.format.` family that the warehouse controls — so
/// benign properties (`delta.*`, user-defined keys, `comment`) keep passing.
fn is_engine_managed_iceberg_property(key: &str) -> bool {
    key.starts_with("write.format.")
}

/// Validate managed-Iceberg `format_options` against the constraints
/// Databricks enforces at the warehouse, returning one
/// [`ManagedIcebergViolation`] per problem (an empty vec means valid).
///
/// Only [`LakehouseFormat::IcebergTable`] is checked; every other format
/// returns no violations. The two rules:
///
/// 1. `partition_by` and `cluster_by` are **mutually exclusive** on Iceberg —
///    Databricks rejects them together with
///    `SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED`.
/// 2. Engine-managed `write.format.*` table properties are rejected with
///    `MANAGED_ICEBERG_OPERATION_NOT_SUPPORTED` (see
///    [`is_engine_managed_iceberg_property`]).
///
/// This is the single source of truth shared by the `rocky compile`
/// diagnostic path and the `generate_lakehouse_ddl` run-path guard, so the
/// two checks can never drift.
#[must_use]
pub fn validate_managed_iceberg_options(
    format: &LakehouseFormat,
    options: &LakehouseOptions,
) -> Vec<ManagedIcebergViolation> {
    if !matches!(format, LakehouseFormat::IcebergTable) {
        return Vec::new();
    }

    let mut violations = Vec::new();

    if !options.partition_by.is_empty() && !options.cluster_by.is_empty() {
        violations.push(ManagedIcebergViolation {
            message: format!(
                "managed-Iceberg format sets both partition_by ({}) and cluster_by ({}); \
                 Databricks Iceberg rejects PARTITIONED BY together with CLUSTER BY \
                 (they are mutually exclusive)",
                options.partition_by.join(", "),
                options.cluster_by.join(", "),
            ),
            suggestion:
                "keep either partition_by or cluster_by on an iceberg_table model, not both"
                    .to_string(),
        });
    }

    for (key, _value) in &options.table_properties {
        if is_engine_managed_iceberg_property(key) {
            violations.push(ManagedIcebergViolation {
                message: format!(
                    "managed-Iceberg format sets the engine-managed table property '{key}'; \
                     Databricks manages the Iceberg write format itself and rejects it"
                ),
                suggestion: format!(
                    "remove the '{key}' table property — Databricks managed Iceberg controls \
                     the write.format.* family"
                ),
            });
        }
    }

    violations
}

// ---------------------------------------------------------------------------
// LakehouseFormat
// ---------------------------------------------------------------------------

/// The physical format used to materialize a model in the lakehouse.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LakehouseFormat {
    /// Delta Lake table (`CREATE TABLE ... USING DELTA`).
    DeltaTable,
    /// Apache Iceberg table (`CREATE TABLE ... USING ICEBERG`).
    IcebergTable,
    /// Warehouse-managed materialized view (`CREATE MATERIALIZED VIEW`).
    MaterializedView,
    /// Databricks-specific streaming table (`CREATE STREAMING TABLE`).
    StreamingTable,
    /// Plain managed table (default warehouse format).
    #[default]
    Table,
    /// SQL view (no physical storage).
    View,
}

impl std::fmt::Display for LakehouseFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DeltaTable => write!(f, "delta_table"),
            Self::IcebergTable => write!(f, "iceberg_table"),
            Self::MaterializedView => write!(f, "materialized_view"),
            Self::StreamingTable => write!(f, "streaming_table"),
            Self::Table => write!(f, "table"),
            Self::View => write!(f, "view"),
        }
    }
}

// ---------------------------------------------------------------------------
// LakehouseOptions
// ---------------------------------------------------------------------------

/// Format-specific DDL options for lakehouse materializations.
///
/// Not all options apply to every format — the `rocky_core::lakehouse`
/// DDL generator silently ignores options that are irrelevant for the
/// chosen format.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct LakehouseOptions {
    /// `PARTITIONED BY (col1, col2)` — Delta and Iceberg tables.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub partition_by: Vec<String>,

    /// `CLUSTER BY (col1, col2)` — Databricks liquid clustering (Delta)
    /// or Iceberg sorted clustering.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_by: Vec<String>,

    /// Arbitrary key-value table properties.
    /// Rendered as `TBLPROPERTIES ('key' = 'value', ...)` for Delta/Iceberg.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub table_properties: Vec<(String, String)>,

    /// Optional comment on the table/view.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,

    /// Amazon Redshift table attributes (`DISTSTYLE` / `DISTKEY` /
    /// `SORTKEY`), from a model sidecar's `[redshift]` block. Unlike the
    /// fields above it applies WITHOUT a lakehouse `format`: it shapes the
    /// plain `CREATE TABLE … AS` the Redshift dialect emits. Any other
    /// dialect refuses a model that sets it rather than dropping it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redshift: Option<RedshiftTableOptions>,

    /// ClickHouse table attributes (`ENGINE` / `PARTITION BY` / `ORDER BY`),
    /// from a model sidecar's `[clickhouse]` block. Like `redshift`, it
    /// applies without a lakehouse `format` and shapes the plain
    /// `CREATE TABLE … AS` the ClickHouse dialect emits; any other dialect
    /// refuses a model that sets it rather than dropping it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<ClickHouseTableOptions>,
}

// ---------------------------------------------------------------------------
// Redshift table attributes
// ---------------------------------------------------------------------------

/// Redshift distribution style (`DISTSTYLE`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum RedshiftDistStyle {
    Auto,
    Even,
    All,
    Key,
}

impl RedshiftDistStyle {
    /// The SQL keyword.
    #[must_use]
    pub fn as_sql(self) -> &'static str {
        match self {
            Self::Auto => "AUTO",
            Self::Even => "EVEN",
            Self::All => "ALL",
            Self::Key => "KEY",
        }
    }
}

/// Redshift sort-key style.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum RedshiftSortStyle {
    /// `COMPOUND SORTKEY (…)` — Redshift's default when columns are given.
    Compound,
    /// `INTERLEAVED SORTKEY (…)` — at most 8 columns.
    Interleaved,
    /// `SORTKEY AUTO` — Redshift chooses; takes no columns.
    Auto,
}

/// Redshift table attributes for a model's `CREATE TABLE … AS`.
///
/// ```toml
/// [redshift]
/// dist_style = "key"          # auto | even | all | key (implied by dist_key)
/// dist_key   = "customer_id"
/// sort_key   = ["order_date"]
/// sort_style = "compound"     # compound (default) | interleaved | auto
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RedshiftTableOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dist_style: Option<RedshiftDistStyle>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dist_key: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sort_style: Option<RedshiftSortStyle>,
}

/// Redshift's documented cap on interleaved sort-key columns.
pub const REDSHIFT_MAX_INTERLEAVED_SORT_KEYS: usize = 8;

impl RedshiftTableOptions {
    /// Every reason these options cannot render, as messages. Empty when
    /// they are valid. Shared by `rocky compile` (E052) and the Redshift
    /// dialect, so the compile-time and run-time checks cannot drift.
    #[must_use]
    pub fn violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(key) = &self.dist_key
            && validation::validate_identifier(key).is_err()
        {
            out.push(format!(
                "redshift.dist_key '{key}' is not a valid column name"
            ));
        }
        for col in &self.sort_key {
            if validation::validate_identifier(col).is_err() {
                out.push(format!(
                    "redshift.sort_key entry '{col}' is not a valid column name"
                ));
            }
        }
        match (self.dist_style, &self.dist_key) {
            (Some(RedshiftDistStyle::Key), None) => {
                out.push("redshift.dist_style = \"key\" needs a dist_key column".to_string())
            }
            (
                Some(
                    style @ (RedshiftDistStyle::Auto
                    | RedshiftDistStyle::Even
                    | RedshiftDistStyle::All),
                ),
                Some(_),
            ) => {
                out.push(format!(
                    "redshift.dist_key is only valid with dist_style = \"key\", not \"{}\"",
                    style.as_sql().to_ascii_lowercase()
                ));
            }
            _ => {}
        }
        match self.sort_style {
            Some(RedshiftSortStyle::Auto) if !self.sort_key.is_empty() => {
                out.push("redshift.sort_style = \"auto\" takes no sort_key columns".to_string())
            }
            Some(RedshiftSortStyle::Compound | RedshiftSortStyle::Interleaved)
                if self.sort_key.is_empty() =>
            {
                out.push("redshift.sort_style needs at least one sort_key column".to_string());
            }
            Some(RedshiftSortStyle::Interleaved)
                if self.sort_key.len() > REDSHIFT_MAX_INTERLEAVED_SORT_KEYS =>
            {
                out.push(format!(
                    "redshift interleaved sort keys allow at most {REDSHIFT_MAX_INTERLEAVED_SORT_KEYS} columns, got {}",
                    self.sort_key.len()
                ));
            }
            _ => {}
        }
        out
    }

    /// The table-attribute clause (`DISTSTYLE KEY DISTKEY (c) COMPOUND
    /// SORTKEY (d)`), or the violations when the options are invalid.
    ///
    /// # Errors
    ///
    /// Returns the [`Self::violations`] messages joined with `; `.
    pub fn to_sql(&self) -> Result<String, String> {
        let violations = self.violations();
        if !violations.is_empty() {
            return Err(violations.join("; "));
        }
        let mut parts = Vec::new();
        let style = self
            .dist_style
            .or(self.dist_key.as_ref().map(|_| RedshiftDistStyle::Key));
        if let Some(style) = style {
            parts.push(format!("DISTSTYLE {}", style.as_sql()));
        }
        if let Some(key) = &self.dist_key {
            parts.push(format!("DISTKEY ({key})"));
        }
        match self.sort_style {
            Some(RedshiftSortStyle::Auto) => parts.push("SORTKEY AUTO".to_string()),
            Some(RedshiftSortStyle::Interleaved) => {
                parts.push(format!(
                    "INTERLEAVED SORTKEY ({})",
                    self.sort_key.join(", ")
                ));
            }
            Some(RedshiftSortStyle::Compound) | None if !self.sort_key.is_empty() => {
                parts.push(format!("COMPOUND SORTKEY ({})", self.sort_key.join(", ")));
            }
            _ => {}
        }
        Ok(parts.join(" "))
    }
}

// ---------------------------------------------------------------------------
// ClickHouse table attributes
// ---------------------------------------------------------------------------

/// ClickHouse table attributes for a model's `CREATE TABLE … AS`.
///
/// ```toml
/// [clickhouse]
/// engine       = "MergeTree"               # a MergeTree-family engine, no parameters
/// order_by     = ["customer_id", "order_date"]
/// partition_by = "toYYYYMM(order_date)"    # a column, or fn(column)
/// ```
///
/// Every field is optional. The default table is
/// `ENGINE = MergeTree ORDER BY tuple()`: no sorting key, no partitions.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseTableOptions {
    /// Table engine. A MergeTree-family name without parameters
    /// (`MergeTree`, `ReplacingMergeTree`, `ReplicatedMergeTree`, …).
    /// Defaults to `MergeTree`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub engine: Option<String>,
    /// Sorting key columns (`ORDER BY (a, b)`). Empty means `tuple()`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
    /// Partition key: a column (`order_date`) or a single-argument function
    /// of one (`toYYYYMM(order_date)`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_by: Option<String>,
}

/// The engine ClickHouse tables get when `[clickhouse] engine` is unset.
pub const CLICKHOUSE_DEFAULT_ENGINE: &str = "MergeTree";

fn is_clickhouse_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl ClickHouseTableOptions {
    /// The column a `partition_by` expression reads: the expression itself
    /// when it is a bare column, the argument of `fn(column)` otherwise.
    /// `None` when the expression has neither shape.
    #[must_use]
    pub fn partition_column(&self) -> Option<&str> {
        let expr = self.partition_by.as_deref()?.trim();
        if is_clickhouse_name(expr) {
            return Some(expr);
        }
        let (func, rest) = expr.split_once('(')?;
        let arg = rest.strip_suffix(')')?.trim();
        (is_clickhouse_name(func.trim()) && is_clickhouse_name(arg)).then_some(arg)
    }

    /// Every reason these options cannot render, as messages. Empty when
    /// they are valid. Shared by `rocky compile` (E053) and the ClickHouse
    /// dialect, so the compile-time and run-time checks cannot drift.
    #[must_use]
    pub fn violations(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(engine) = &self.engine
            && !(is_clickhouse_name(engine) && engine.ends_with("MergeTree"))
        {
            out.push(format!(
                "clickhouse.engine '{engine}' is not a MergeTree-family engine name \
                 (parameters are not accepted)"
            ));
        }
        for col in &self.order_by {
            if !is_clickhouse_name(col) {
                out.push(format!(
                    "clickhouse.order_by entry '{col}' is not a valid column name"
                ));
            }
        }
        if let Some(expr) = &self.partition_by
            && self.partition_column().is_none()
        {
            out.push(format!(
                "clickhouse.partition_by '{expr}' must be a column or fn(column)"
            ));
        }
        out
    }

    /// The table-attribute clause (`ENGINE = MergeTree PARTITION BY
    /// toYYYYMM(d) ORDER BY (a, b)`), or the violations when the options are
    /// invalid.
    ///
    /// # Errors
    ///
    /// Returns the [`Self::violations`] messages joined with `; `.
    pub fn to_sql(&self) -> Result<String, String> {
        let violations = self.violations();
        if !violations.is_empty() {
            return Err(violations.join("; "));
        }
        let engine = self.engine.as_deref().unwrap_or(CLICKHOUSE_DEFAULT_ENGINE);
        let mut sql = format!("ENGINE = {engine}");
        if let Some(expr) = &self.partition_by {
            sql.push_str(&format!(" PARTITION BY {}", expr.trim()));
        }
        if self.order_by.is_empty() {
            sql.push_str(" ORDER BY tuple()");
        } else if let [col] = self.order_by.as_slice() {
            // One column renders bare, as ClickHouse itself spells the key.
            sql.push_str(&format!(" ORDER BY {col}"));
        } else {
            sql.push_str(&format!(" ORDER BY ({})", self.order_by.join(", ")));
        }
        Ok(sql)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redshift_options_render() {
        let opts = RedshiftTableOptions {
            dist_key: Some("customer_id".into()),
            sort_key: vec!["order_date".into(), "order_id".into()],
            ..RedshiftTableOptions::default()
        };
        assert_eq!(
            opts.to_sql().unwrap(),
            "DISTSTYLE KEY DISTKEY (customer_id) COMPOUND SORTKEY (order_date, order_id)"
        );
        let even = RedshiftTableOptions {
            dist_style: Some(RedshiftDistStyle::Even),
            sort_style: Some(RedshiftSortStyle::Auto),
            ..RedshiftTableOptions::default()
        };
        assert_eq!(even.to_sql().unwrap(), "DISTSTYLE EVEN SORTKEY AUTO");
        let inter = RedshiftTableOptions {
            sort_key: vec!["a".into(), "b".into()],
            sort_style: Some(RedshiftSortStyle::Interleaved),
            ..RedshiftTableOptions::default()
        };
        assert_eq!(inter.to_sql().unwrap(), "INTERLEAVED SORTKEY (a, b)");
        assert_eq!(RedshiftTableOptions::default().to_sql().unwrap(), "");
    }

    #[test]
    fn redshift_options_violations() {
        let bad = |o: RedshiftTableOptions| o.violations();
        assert_eq!(
            bad(RedshiftTableOptions {
                dist_style: Some(RedshiftDistStyle::Key),
                ..Default::default()
            })
            .len(),
            1
        );
        assert_eq!(
            bad(RedshiftTableOptions {
                dist_style: Some(RedshiftDistStyle::All),
                dist_key: Some("a".into()),
                ..Default::default()
            })
            .len(),
            1
        );
        assert_eq!(
            bad(RedshiftTableOptions {
                dist_key: Some("a; DROP".into()),
                sort_key: vec!["b c".into()],
                ..Default::default()
            })
            .len(),
            2
        );
        assert_eq!(
            bad(RedshiftTableOptions {
                sort_key: vec!["a".into()],
                sort_style: Some(RedshiftSortStyle::Auto),
                ..Default::default()
            })
            .len(),
            1
        );
        assert_eq!(
            bad(RedshiftTableOptions {
                sort_key: (0..9).map(|i| format!("c{i}")).collect(),
                sort_style: Some(RedshiftSortStyle::Interleaved),
                ..Default::default()
            })
            .len(),
            1
        );
        assert!(bad(RedshiftTableOptions::default()).is_empty());
    }

    #[test]
    fn redshift_options_parse_and_refuse_typos() {
        let opts: RedshiftTableOptions = serde_json::from_str(
            r#"{"dist_key": "c", "sort_key": ["d"], "sort_style": "interleaved"}"#,
        )
        .unwrap();
        assert_eq!(opts.sort_style, Some(RedshiftSortStyle::Interleaved));
        assert!(serde_json::from_str::<RedshiftTableOptions>(r#"{"distkey": "c"}"#).is_err());
    }

    #[test]
    fn clickhouse_options_render() {
        assert_eq!(
            ClickHouseTableOptions::default().to_sql().unwrap(),
            "ENGINE = MergeTree ORDER BY tuple()"
        );
        let opts = ClickHouseTableOptions {
            engine: Some("ReplacingMergeTree".into()),
            order_by: vec!["customer_id".into(), "order_date".into()],
            partition_by: Some("toYYYYMM(order_date)".into()),
        };
        assert_eq!(
            opts.to_sql().unwrap(),
            "ENGINE = ReplacingMergeTree PARTITION BY toYYYYMM(order_date) \
             ORDER BY (customer_id, order_date)"
        );
        assert_eq!(opts.partition_column(), Some("order_date"));
        let bare = ClickHouseTableOptions {
            partition_by: Some("region".into()),
            ..Default::default()
        };
        assert_eq!(bare.partition_column(), Some("region"));
        let one = ClickHouseTableOptions {
            order_by: vec!["id".into()],
            ..Default::default()
        };
        assert_eq!(one.to_sql().unwrap(), "ENGINE = MergeTree ORDER BY id");
    }

    #[test]
    fn clickhouse_options_violations() {
        let bad = |o: ClickHouseTableOptions| o.violations().len();
        // Engine parameters, a non-MergeTree engine, and injection are refused.
        for engine in ["ReplacingMergeTree(ver)", "Log", "MergeTree; DROP", ""] {
            assert_eq!(
                bad(ClickHouseTableOptions {
                    engine: Some(engine.into()),
                    ..Default::default()
                }),
                1,
                "{engine}"
            );
        }
        assert_eq!(
            bad(ClickHouseTableOptions {
                order_by: vec!["a b".into(), "1a".into(), "ok".into()],
                ..Default::default()
            }),
            2
        );
        for expr in ["toYYYYMM(d, 1)", "f(g(d))", "d)", "d + 1", "f()"] {
            assert_eq!(
                bad(ClickHouseTableOptions {
                    partition_by: Some(expr.into()),
                    ..Default::default()
                }),
                1,
                "{expr}"
            );
        }
        assert_eq!(bad(ClickHouseTableOptions::default()), 0);
    }

    #[test]
    fn clickhouse_options_parse_and_refuse_typos() {
        let opts: ClickHouseTableOptions =
            serde_json::from_str(r#"{"order_by": ["a"], "partition_by": "toDate(ts)"}"#).unwrap();
        assert_eq!(opts.order_by, vec!["a".to_string()]);
        assert!(serde_json::from_str::<ClickHouseTableOptions>(r#"{"orderby": ["a"]}"#).is_err());
    }

    #[test]
    fn iceberg_partition_and_cluster_together_is_rejected() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            cluster_by: vec!["user_id".into()],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 1, "exactly one violation expected");
        // The message must name both offending options.
        let msg = &violations[0].message;
        assert!(msg.contains("partition_by"), "names partition_by: {msg}");
        assert!(msg.contains("cluster_by"), "names cluster_by: {msg}");
        assert!(msg.contains("event_date"), "names the column: {msg}");
        assert!(msg.contains("user_id"), "names the column: {msg}");
    }

    #[test]
    fn iceberg_write_format_default_property_is_rejected() {
        let opts = LakehouseOptions {
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 1);
        assert!(
            violations[0].message.contains("write.format.default"),
            "names the offending key: {}",
            violations[0].message
        );
    }

    #[test]
    fn iceberg_partition_only_is_accepted() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            ..LakehouseOptions::default()
        };
        assert!(validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty());
    }

    #[test]
    fn iceberg_cluster_only_is_accepted() {
        let opts = LakehouseOptions {
            cluster_by: vec!["user_id".into()],
            ..LakehouseOptions::default()
        };
        assert!(validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty());
    }

    #[test]
    fn iceberg_benign_properties_are_accepted() {
        let opts = LakehouseOptions {
            partition_by: vec!["event_date".into()],
            table_properties: vec![
                ("delta.enableChangeDataFeed".into(), "true".into()),
                ("team".into(), "growth".into()),
            ],
            comment: Some("benign".into()),
            ..LakehouseOptions::default()
        };
        assert!(
            validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts).is_empty(),
            "benign delta.*/user properties must not be flagged"
        );
    }

    #[test]
    fn non_iceberg_formats_are_never_flagged() {
        // The same partition+cluster+write.format combo on Delta is fine —
        // the managed-Iceberg constraints apply only to Iceberg.
        let opts = LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            table_properties: vec![("write.format.default".into(), "parquet".into())],
            ..LakehouseOptions::default()
        };
        for format in [
            LakehouseFormat::DeltaTable,
            LakehouseFormat::Table,
            LakehouseFormat::StreamingTable,
            LakehouseFormat::MaterializedView,
            LakehouseFormat::View,
        ] {
            assert!(
                validate_managed_iceberg_options(&format, &opts).is_empty(),
                "format {format} must not be subject to managed-Iceberg rules"
            );
        }
    }

    #[test]
    fn iceberg_both_violations_reported() {
        let opts = LakehouseOptions {
            partition_by: vec!["region".into()],
            cluster_by: vec!["id".into()],
            table_properties: vec![("write.format.version".into(), "2".into())],
            ..LakehouseOptions::default()
        };
        let violations = validate_managed_iceberg_options(&LakehouseFormat::IcebergTable, &opts);
        assert_eq!(violations.len(), 2, "both rules fire independently");
    }
}
