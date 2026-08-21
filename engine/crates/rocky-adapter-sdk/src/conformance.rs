//! Conformance test harness for adapter validation.
//!
//! The conformance suite validates that an adapter implementation correctly
//! handles the full range of operations Rocky requires. Tests are categorized
//! and can be skipped based on the adapter's declared capabilities.
//!
//! # Usage
//!
//! ```ignore
//! let result = run_conformance(&manifest, Some(adapter.dialect()));
//! assert_eq!(result.tests_failed, 0);
//! ```
//!
//! Pass `None` for the dialect when no live adapter is available (for example,
//! the built-in path, `rocky test-adapter --adapter <name>`). Dialect-category
//! tests are reported as skipped in that mode rather than executed against a
//! stub.

use std::time::Instant;

use serde::{Deserialize, Serialize};

use crate::manifest::AdapterManifest;
use crate::traits::SqlDialect;

/// Result of running the conformance test suite against an adapter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConformanceResult {
    /// Adapter name.
    pub adapter: String,
    /// SDK version the adapter targets.
    pub sdk_version: String,
    /// Total tests executed.
    pub tests_run: usize,
    /// Tests that passed.
    pub tests_passed: usize,
    /// Tests that failed.
    pub tests_failed: usize,
    /// Tests skipped based on capabilities.
    pub tests_skipped: usize,
    /// Individual test results.
    pub results: Vec<TestResult>,
}

impl ConformanceResult {
    /// Format the result as a human-readable report.
    pub fn report(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!(
            "Adapter Conformance: {} (SDK {})\n",
            self.adapter, self.sdk_version
        ));
        out.push_str(&"=".repeat(50));
        out.push('\n');

        let mut current_category = None;
        for result in &self.results {
            if current_category != Some(&result.category) {
                out.push_str(&format!("\n{}:\n", result.category));
                current_category = Some(&result.category);
            }

            let status_icon = match result.status {
                TestStatus::Passed => "  + ",
                TestStatus::Failed => "  X ",
                TestStatus::Skipped => "  - ",
            };

            let suffix = match &result.status {
                TestStatus::Passed => format!("{}ms", result.duration_ms),
                TestStatus::Failed => {
                    format!(
                        "FAILED: {}",
                        result.message.as_deref().unwrap_or("unknown error")
                    )
                }
                TestStatus::Skipped => {
                    format!(
                        "SKIPPED{}",
                        result
                            .message
                            .as_ref()
                            .map(|m| format!(" ({m})"))
                            .unwrap_or_default()
                    )
                }
            };

            out.push_str(&format!("{}{:<30}{}\n", status_icon, result.name, suffix));
        }

        out.push_str(&format!(
            "\nResult: {} passed, {} failed, {} skipped\n",
            self.tests_passed, self.tests_failed, self.tests_skipped
        ));

        // Nothing ran. The exit status is still zero — it keys on failures,
        // and changing that would break pipelines — so the report has to be
        // the thing that says so. A conformance suite reporting no failures
        // while verifying nothing is the same silent-success shape the
        // `_ => Pass` catch-all had, one level up.
        if self.tests_passed == 0 && self.tests_failed == 0 {
            out.push_str(
                "\nWARNING: no conformance check actually ran. This is NOT a passing \
                 conformance run — every spec was skipped.\n",
            );
        }

        out
    }
}

/// Result of a single conformance test.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TestResult {
    /// Test name (e.g., "connect", "create_table").
    pub name: String,
    /// Category this test belongs to.
    pub category: TestCategory,
    /// Whether the test passed, failed, or was skipped.
    pub status: TestStatus,
    /// Error message (for failures) or reason (for skips).
    pub message: Option<String>,
    /// Execution time in milliseconds.
    pub duration_ms: u64,
}

/// Category of a conformance test.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum TestCategory {
    Connection,
    DDL,
    DML,
    Query,
    Types,
    Dialect,
    Governance,
    Discovery,
    BatchChecks,
}

impl std::fmt::Display for TestCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestCategory::Connection => write!(f, "Connection"),
            TestCategory::DDL => write!(f, "DDL"),
            TestCategory::DML => write!(f, "DML"),
            TestCategory::Query => write!(f, "Query"),
            TestCategory::Types => write!(f, "Types"),
            TestCategory::Dialect => write!(f, "Dialect"),
            TestCategory::Governance => write!(f, "Governance"),
            TestCategory::Discovery => write!(f, "Discovery"),
            TestCategory::BatchChecks => write!(f, "BatchChecks"),
        }
    }
}

/// Status of a single test.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
}

/// Description of a conformance test (used to build the test plan).
struct TestSpec {
    name: &'static str,
    category: TestCategory,
    /// If set, the test is skipped when this capability is false.
    requires_capability: Option<fn(&AdapterManifest) -> bool>,
}

/// Outcome of a single spec dispatch. Mapped to `TestStatus` by the runner.
enum TestOutcome {
    Pass,
    Fail(String),
    Skip(String),
}

/// Message used when a dialect-category spec is asked to run without a dialect.
const NO_DIALECT_SKIP_MESSAGE: &str = "dialect not available in this run mode";

/// Build the full list of conformance test specifications.
fn test_specs() -> Vec<TestSpec> {
    vec![
        // Connection
        TestSpec {
            name: "connect",
            category: TestCategory::Connection,
            requires_capability: None,
        },
        // DDL
        TestSpec {
            name: "create_table",
            category: TestCategory::DDL,
            requires_capability: None,
        },
        TestSpec {
            name: "drop_table",
            category: TestCategory::DDL,
            requires_capability: None,
        },
        TestSpec {
            name: "create_catalog",
            category: TestCategory::DDL,
            requires_capability: Some(|m| m.capabilities.create_catalog),
        },
        TestSpec {
            name: "create_schema",
            category: TestCategory::DDL,
            requires_capability: Some(|m| m.capabilities.create_schema),
        },
        // DML
        TestSpec {
            name: "insert_into",
            category: TestCategory::DML,
            requires_capability: None,
        },
        TestSpec {
            name: "merge_into",
            category: TestCategory::DML,
            requires_capability: Some(|m| m.capabilities.merge),
        },
        // Query
        TestSpec {
            name: "describe_table",
            category: TestCategory::Query,
            requires_capability: None,
        },
        TestSpec {
            name: "table_exists_true",
            category: TestCategory::Query,
            requires_capability: None,
        },
        TestSpec {
            name: "table_exists_false",
            category: TestCategory::Query,
            requires_capability: None,
        },
        TestSpec {
            name: "execute_query",
            category: TestCategory::Query,
            requires_capability: None,
        },
        // Types
        TestSpec {
            name: "type_string",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_integer",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_float",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_boolean",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_date",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_timestamp",
            category: TestCategory::Types,
            requires_capability: None,
        },
        TestSpec {
            name: "type_null",
            category: TestCategory::Types,
            requires_capability: None,
        },
        // Dialect
        TestSpec {
            name: "format_table_ref",
            category: TestCategory::Dialect,
            requires_capability: None,
        },
        TestSpec {
            name: "watermark_where",
            category: TestCategory::Dialect,
            requires_capability: None,
        },
        TestSpec {
            name: "row_hash",
            category: TestCategory::Dialect,
            requires_capability: None,
        },
        // Governance (optional)
        TestSpec {
            name: "set_tags",
            category: TestCategory::Governance,
            requires_capability: Some(|m| m.capabilities.governance),
        },
        TestSpec {
            name: "get_grants",
            category: TestCategory::Governance,
            requires_capability: Some(|m| m.capabilities.governance),
        },
        // Batch checks (optional)
        TestSpec {
            name: "batch_row_counts",
            category: TestCategory::BatchChecks,
            requires_capability: Some(|m| m.capabilities.batch_checks),
        },
        TestSpec {
            name: "batch_freshness",
            category: TestCategory::BatchChecks,
            requires_capability: Some(|m| m.capabilities.batch_checks),
        },
        // Discovery (optional)
        TestSpec {
            name: "discover",
            category: TestCategory::Discovery,
            requires_capability: Some(|m| m.capabilities.discovery),
        },
    ]
}

/// Run the conformance test suite against an adapter.
///
/// Builds the test plan from the adapter's manifest, executes each spec, and
/// collects results. Specs whose required capability is missing are reported
/// as `Skipped`.
///
/// When `dialect` is `Some`, the harness executes one real trait call
/// (`SqlDialect::format_table_ref`) as the first incremental live check.
/// When `dialect` is `None` — for example on the built-in path
/// (`rocky test-adapter --adapter <name>`), which validates the test plan
/// without constructing a live adapter — that spec is
/// reported as `Skipped` rather than executed against a stub. Remaining
/// specs return placeholder passes; broader live execution lands in future
/// SDK releases.
pub fn run_conformance(
    manifest: &AdapterManifest,
    dialect: Option<&dyn SqlDialect>,
) -> ConformanceResult {
    let specs = test_specs();
    let mut results = Vec::with_capacity(specs.len());
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped = 0usize;

    for spec in &specs {
        let start = Instant::now();

        // Check if this test's required capability is supported.
        let supported = spec
            .requires_capability
            .map(|check| check(manifest))
            .unwrap_or(true);

        if !supported {
            skipped += 1;
            results.push(TestResult {
                name: spec.name.to_string(),
                category: spec.category.clone(),
                status: TestStatus::Skipped,
                message: Some("not supported".into()),
                duration_ms: 0,
            });
            continue;
        }

        let outcome = run_test_spec(spec, dialect);
        let elapsed = start.elapsed();
        let (status, message) = match outcome {
            TestOutcome::Pass => {
                passed += 1;
                (TestStatus::Passed, None)
            }
            TestOutcome::Fail(msg) => {
                failed += 1;
                (TestStatus::Failed, Some(msg))
            }
            TestOutcome::Skip(msg) => {
                skipped += 1;
                (TestStatus::Skipped, Some(msg))
            }
        };
        results.push(TestResult {
            name: spec.name.to_string(),
            category: spec.category.clone(),
            status,
            message,
            duration_ms: elapsed.as_millis() as u64,
        });
    }

    ConformanceResult {
        adapter: manifest.name.clone(),
        sdk_version: manifest.sdk_version.clone(),
        tests_run: passed + failed,
        tests_passed: passed,
        tests_failed: failed,
        tests_skipped: skipped,
        results,
    }
}

fn run_test_spec(spec: &TestSpec, dialect: Option<&dyn SqlDialect>) -> TestOutcome {
    match spec.name {
        "format_table_ref" => {
            let Some(dialect) = dialect else {
                return TestOutcome::Skip(NO_DIALECT_SKIP_MESSAGE.into());
            };
            match dialect.format_table_ref("c", "s", "t") {
                Ok(formatted) if formatted.trim().is_empty() => {
                    TestOutcome::Fail("format_table_ref returned an empty table reference".into())
                }
                Ok(_) => TestOutcome::Pass,
                Err(e) => TestOutcome::Fail(e.to_string()),
            }
        }
        // Every spec without an arm above. Reporting `Pass` here meant
        // `rocky test-adapter` printed green for work it never did: only
        // `format_table_ref` is actually exercised, so connect, statement
        // execution, schema/table lifecycle, grants, batch checks and
        // discovery all counted as passing conformance (#475).
        //
        // `Skipped` is the honest answer — the spec is declared, the check
        // is not written. It also keeps `tests_run` (passed + failed) from
        // counting unimplemented specs as verified work.
        _ => TestOutcome::Skip("no conformance check is implemented for this spec yet".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::manifest::{AdapterCapabilities, AdapterManifest};
    use crate::traits::{AdapterError, AdapterResult, ColumnSelection, MetadataColumn};

    /// In-crate `SqlDialect` stub. `format_table_ref` is delegated through a
    /// function pointer so individual tests can swap in empty-string or `Err`
    /// behavior without duplicating the rest of the trait surface.
    struct TestDialect {
        format_table_ref_impl: fn(&str, &str, &str) -> AdapterResult<String>,
    }

    impl Default for TestDialect {
        fn default() -> Self {
            Self {
                format_table_ref_impl: |c, s, t| Ok(format!("{c}.{s}.{t}")),
            }
        }
    }

    impl TestDialect {
        fn with_format_table_ref(f: fn(&str, &str, &str) -> AdapterResult<String>) -> Self {
            Self {
                format_table_ref_impl: f,
            }
        }
    }

    impl SqlDialect for TestDialect {
        fn name(&self) -> &str {
            "test"
        }

        fn format_table_ref(
            &self,
            catalog: &str,
            schema: &str,
            table: &str,
        ) -> AdapterResult<String> {
            (self.format_table_ref_impl)(catalog, schema, table)
        }

        fn create_table_as(&self, target: &str, select_sql: &str) -> String {
            format!("CREATE TABLE {target} AS {select_sql}")
        }

        fn insert_into(&self, target: &str, select_sql: &str) -> String {
            format!("INSERT INTO {target} {select_sql}")
        }

        fn merge_into(
            &self,
            target: &str,
            source_sql: &str,
            _keys: &[String],
            _update_cols: Option<&[String]>,
        ) -> AdapterResult<String> {
            Ok(format!("MERGE INTO {target} USING {source_sql}"))
        }

        fn describe_table_sql(&self, table_ref: &str) -> String {
            format!("DESCRIBE {table_ref}")
        }

        fn drop_table_sql(&self, table_ref: &str) -> String {
            format!("DROP TABLE {table_ref}")
        }

        fn create_catalog_sql(&self, name: &str) -> Option<AdapterResult<String>> {
            Some(Ok(format!("CREATE CATALOG {name}")))
        }

        fn create_schema_sql(&self, catalog: &str, schema: &str) -> Option<AdapterResult<String>> {
            Some(Ok(format!("CREATE SCHEMA {catalog}.{schema}")))
        }

        fn row_hash_expr(&self, columns: &[String]) -> String {
            format!("hash({})", columns.join(", "))
        }

        fn tablesample_clause(&self, percent: u32) -> Option<String> {
            Some(format!("TABLESAMPLE ({percent})"))
        }

        fn select_clause(
            &self,
            columns: &ColumnSelection,
            metadata: &[MetadataColumn],
        ) -> AdapterResult<String> {
            let mut sql = match columns {
                ColumnSelection::All => "SELECT *".to_string(),
                ColumnSelection::Explicit(cols) => format!("SELECT {}", cols.join(", ")),
            };
            for column in metadata {
                sql.push_str(&format!(", {} AS {}", column.value, column.name));
            }
            Ok(sql)
        }

        fn watermark_where(
            &self,
            timestamp_col: &str,
            last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
        ) -> AdapterResult<String> {
            let literal = last_watermark
                .map(|t| t.format("%Y-%m-%d %H:%M:%S%.f").to_string())
                .unwrap_or_else(|| "1970-01-01 00:00:00".to_string());
            Ok(format!("WHERE {timestamp_col} > TIMESTAMP '{literal}'"))
        }

        fn insert_overwrite_partition(
            &self,
            target: &str,
            partition_filter: &str,
            select_sql: &str,
        ) -> AdapterResult<Vec<String>> {
            Ok(vec![format!(
                "INSERT OVERWRITE {target} WHERE {partition_filter} {select_sql}"
            )])
        }
    }

    fn run_test_conformance(manifest: &AdapterManifest) -> ConformanceResult {
        run_conformance(manifest, Some(&TestDialect::default()))
    }

    fn test_manifest(caps: AdapterCapabilities) -> AdapterManifest {
        AdapterManifest {
            name: "test-adapter".into(),
            version: "0.1.0".into(),
            sdk_version: crate::SDK_VERSION.into(),
            dialect: "test".into(),
            capabilities: caps,
            auth_methods: vec![],
            config_schema: serde_json::Value::Object(Default::default()),
        }
    }

    fn find_result<'a>(result: &'a ConformanceResult, name: &str) -> &'a TestResult {
        result
            .results
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("missing {name} result"))
    }

    /// A run that verified nothing must SAY so.
    ///
    /// The exit status keys on failures, so an all-skipped run exits zero —
    /// changing that would break pipelines. The report is therefore the only
    /// thing standing between "no failures" and "nothing was checked", which
    /// is the same silent-success shape the `_ => Pass` catch-all had.
    #[test]
    fn a_run_that_verified_nothing_says_so() {
        // No dialect: even `format_table_ref` — the one implemented spec —
        // skips, so nothing at all is verified. That is the real shape of a
        // run against an adapter that could not be reached.
        let manifest = test_manifest(AdapterCapabilities::full());
        let result = run_conformance(&manifest, None);

        assert_eq!(result.tests_passed, 0, "fixture precondition");
        assert_eq!(result.tests_failed, 0, "fixture precondition");
        let report = result.report();
        assert!(
            report.contains("NOT a passing conformance run"),
            "an all-skipped report must not read as success:\n{report}"
        );
    }

    /// The converse: a run that DID verify something must not carry the
    /// warning, or it becomes noise everyone learns to ignore.
    #[test]
    fn a_run_that_verified_something_carries_no_warning() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_conformance(&manifest, Some(&TestDialect::default()));

        assert!(result.tests_passed > 0, "fixture precondition");
        assert!(
            !result.report().contains("NOT a passing conformance run"),
            "the warning must not fire when checks actually ran"
        );
    }

    /// A spec only counts as RUN when a check actually executed.
    ///
    /// This test used to assert `tests_skipped == 0` and `tests_run == 26`,
    /// which encoded the bug: `run_test_spec` implements exactly one arm
    /// (`format_table_ref`) and every other spec fell through to `Pass`. A
    /// full-capability adapter therefore reported 26 passing conformance
    /// tests having executed one (#475).
    ///
    /// The honest counts are asserted instead, and the invariant that
    /// matters is asserted directly: nothing reports Passed unless its
    /// check ran.
    #[test]
    fn test_conformance_full_capabilities() {
        let manifest = test_manifest(AdapterCapabilities::full());
        let result = run_test_conformance(&manifest);

        assert_eq!(result.adapter, "test-adapter");
        assert_eq!(result.tests_failed, 0);

        // 26 specs are declared; only `format_table_ref` has an
        // implementation, and this run mode supplies no dialect — so it
        // skips too. `tests_run` counts passed + failed, i.e. real work.
        assert_eq!(
            result.tests_run + result.tests_skipped,
            26,
            "every declared spec must be accounted for"
        );
        assert!(
            result.tests_skipped >= 25,
            "specs with no implemented check must report Skipped, not Passed \
             (got {} skipped)",
            result.tests_skipped
        );

        // The property, stated without counting: a Passed result means a
        // check ran. Only `format_table_ref` can produce one today.
        for r in &result.results {
            assert!(
                r.status != TestStatus::Passed || r.name == "format_table_ref",
                "spec '{}' reported Passed but has no implemented check",
                r.name
            );
        }
    }

    #[test]
    fn test_conformance_executes_format_table_ref_pass() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_conformance(&manifest, Some(&TestDialect::default()));

        let format_result = find_result(&result, "format_table_ref");
        assert_eq!(format_result.status, TestStatus::Passed);
    }

    #[test]
    fn test_conformance_fails_format_table_ref_when_empty() {
        let dialect = TestDialect::with_format_table_ref(|_, _, _| Ok(String::new()));
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_conformance(&manifest, Some(&dialect));

        let format_result = find_result(&result, "format_table_ref");
        assert_eq!(format_result.status, TestStatus::Failed);
        assert_eq!(result.tests_failed, 1);
    }

    #[test]
    fn test_conformance_fails_format_table_ref_on_err() {
        let dialect =
            TestDialect::with_format_table_ref(|_, _, _| Err(AdapterError::msg("dialect blew up")));
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_conformance(&manifest, Some(&dialect));

        let format_result = find_result(&result, "format_table_ref");
        assert_eq!(format_result.status, TestStatus::Failed);
        assert_eq!(
            format_result.message.as_deref(),
            Some("dialect blew up"),
            "expected the adapter error message to propagate into the test result"
        );
    }

    #[test]
    fn test_conformance_skips_format_table_ref_when_no_dialect() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_conformance(&manifest, None);

        let format_result = find_result(&result, "format_table_ref");
        assert_eq!(format_result.status, TestStatus::Skipped);
        assert_eq!(
            format_result.message.as_deref(),
            Some(NO_DIALECT_SKIP_MESSAGE)
        );
        assert_eq!(result.tests_failed, 0);
    }

    #[test]
    fn test_conformance_warehouse_only() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_test_conformance(&manifest);

        // Optional tests should be skipped
        assert!(result.tests_skipped > 0);
        assert_eq!(result.tests_failed, 0);

        // Verify specific skips
        let skipped_names: Vec<&str> = result
            .results
            .iter()
            .filter(|r| r.status == TestStatus::Skipped)
            .map(|r| r.name.as_str())
            .collect();

        assert!(skipped_names.contains(&"create_catalog"));
        assert!(skipped_names.contains(&"create_schema"));
        assert!(skipped_names.contains(&"merge_into"));
        assert!(skipped_names.contains(&"set_tags"));
        assert!(skipped_names.contains(&"get_grants"));
        assert!(skipped_names.contains(&"batch_row_counts"));
        assert!(skipped_names.contains(&"batch_freshness"));
        assert!(skipped_names.contains(&"discover"));
    }

    #[test]
    fn test_conformance_result_serialization() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_test_conformance(&manifest);

        let json = serde_json::to_string(&result).unwrap();
        let deserialized: ConformanceResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.adapter, "test-adapter");
        assert_eq!(deserialized.tests_run, result.tests_run);
    }

    #[test]
    fn test_conformance_report_formatting() {
        let manifest = test_manifest(AdapterCapabilities::warehouse_only());
        let result = run_test_conformance(&manifest);
        let report = result.report();

        assert!(report.contains("Adapter Conformance: test-adapter"));
        assert!(report.contains("Connection:"));
        assert!(report.contains("DDL:"));
        assert!(report.contains("SKIPPED"));
        assert!(report.contains("passed"));
    }

    #[test]
    fn test_category_display() {
        assert_eq!(TestCategory::Connection.to_string(), "Connection");
        assert_eq!(TestCategory::DDL.to_string(), "DDL");
        assert_eq!(TestCategory::DML.to_string(), "DML");
        assert_eq!(TestCategory::Query.to_string(), "Query");
        assert_eq!(TestCategory::Types.to_string(), "Types");
        assert_eq!(TestCategory::Dialect.to_string(), "Dialect");
        assert_eq!(TestCategory::Governance.to_string(), "Governance");
        assert_eq!(TestCategory::Discovery.to_string(), "Discovery");
        assert_eq!(TestCategory::BatchChecks.to_string(), "BatchChecks");
    }
}
