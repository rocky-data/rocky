//! `rocky test-adapter` — run conformance tests against an adapter.
//!
//! This command validates that an adapter implementation correctly handles
//! the operations Rocky requires. It can test both compiled-in adapters
//! and process-based adapters.

use anyhow::{Context, Result};
use rocky_adapter_sdk::WarehouseAdapter;
use rocky_adapter_sdk::conformance::{self, ConformanceResult};
use rocky_adapter_sdk::manifest::{AdapterCapabilities, AdapterManifest};
use rocky_adapter_sdk::process::ProcessAdapter;

use crate::commands::adapter::resolve_adapter_command;
use crate::output::{TestAdapterOutput, TestAdapterTestResult};

/// Names recognised by the compiled-in / static dispatch arm in
/// [`run_test_adapter_builtin`]. Anything outside this list falls through to
/// the PATH-based `rocky-<name>` process-adapter resolution so the same
/// `--adapter <name>` flag works for both shipped adapters and installed
/// process adapters.
const BUILTIN_ADAPTERS: &[&str] = &["databricks", "snowflake", "duckdb"];

/// Run the conformance test suite against a process adapter.
///
/// The `adapter_command` is the path to the adapter binary. Rocky spawns it,
/// sends `initialize`, reads the manifest, and runs all applicable tests.
pub async fn run_test_adapter(
    adapter_command: &str,
    config_json: Option<&str>,
    json_output: bool,
) -> Result<()> {
    let config: serde_json::Value = match config_json {
        Some(s) => serde_json::from_str(s).context("failed to parse adapter config JSON")?,
        None => serde_json::json!({}),
    };

    // Attempt to spawn and initialize the process adapter.
    let adapter_result = ProcessAdapter::spawn(adapter_command, &[], &config).await;

    let result = match adapter_result {
        Ok(adapter) => {
            let manifest = adapter.manifest().clone();

            // Run conformance suite.
            let result = conformance::run_conformance(&manifest, Some(adapter.dialect()));

            // Clean up.
            let _ = adapter.close().await;

            result
        }
        Err(e) => {
            // If we can't connect, report a single connection failure.
            tracing::error!("failed to initialize adapter: {e}");

            // Build a minimal manifest for reporting.
            let manifest = AdapterManifest {
                name: adapter_command.to_string(),
                version: "unknown".into(),
                sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
                dialect: "unknown".into(),
                capabilities: AdapterCapabilities::warehouse_only(),
                auth_methods: vec![],
                config_schema: serde_json::Value::Null,
            };

            // No live dialect available — let the harness skip dialect-category
            // checks and override `connect` as failed below.
            let mut result = conformance::run_conformance(&manifest, None);
            // Override the first test (connect) as failed.
            mark_connect_failed(&mut result, &format!("failed to initialize: {e}"));

            result
        }
    };

    output_result(&result, json_output)?;

    if result.tests_failed > 0 {
        anyhow::bail!("{} conformance test(s) failed", result.tests_failed);
    }

    Ok(())
}

/// Run the conformance test suite against a built-in adapter by name.
///
/// This validates a compiled-in adapter without spawning a process.
pub async fn run_test_adapter_builtin(
    adapter_name: &str,
    config_path: Option<&str>,
    json_output: bool,
) -> Result<()> {
    // PATH-based fallback: an `--adapter foo` that isn't a known builtin is
    // resolved to a `rocky-foo` binary on `$PATH` and dispatched through the
    // process-adapter conformance path. This makes `--adapter` the single
    // user-facing surface for both shipped adapters and installed process
    // adapters — same flag, same convention as `cargo` subcommands.
    if !BUILTIN_ADAPTERS.contains(&adapter_name)
        && let Some(command) = resolve_adapter_command(adapter_name)
    {
        return run_test_adapter(&command.display().to_string(), config_path, json_output).await;
    }

    // For built-in adapters, we construct a manifest from the adapter name
    // and run the conformance suite. Actual execution requires a live warehouse,
    // so this mode validates the test plan and capability matching.
    let capabilities = match adapter_name {
        "databricks" => AdapterCapabilities::full(),
        "snowflake" => AdapterCapabilities {
            warehouse: true,
            discovery: false,
            governance: true,
            batch_checks: true,
            create_catalog: false,
            create_schema: true,
            merge: true,
            tablesample: true,
            file_load: false,
        },
        "duckdb" => AdapterCapabilities {
            warehouse: true,
            discovery: false,
            governance: false,
            batch_checks: false,
            create_catalog: false,
            create_schema: true,
            // DuckDB MERGE is supported when `update_columns` is enumerated
            // explicitly. Like Snowflake (which also reports `merge: true`),
            // DuckDB rejects the `UPDATE SET *` shorthand — callers must
            // declare the column list in the model TOML. See
            // `rocky-duckdb::dialect::merge_into`.
            merge: true,
            tablesample: true,
            file_load: true,
        },
        _ => {
            anyhow::bail!(
                "unknown adapter '{adapter_name}'. Known built-in adapters: {builtins}. \
                 To use a process adapter, install a `rocky-{adapter_name}` binary on $PATH \
                 (see `rocky adapter list`).",
                builtins = BUILTIN_ADAPTERS.join(", "),
            );
        }
    };

    let manifest = AdapterManifest {
        name: adapter_name.into(),
        version: env!("CARGO_PKG_VERSION").into(),
        sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
        dialect: adapter_name.into(),
        capabilities,
        auth_methods: vec![],
        config_schema: serde_json::Value::Null,
    };

    // No live adapter is constructed for the builtin path, so the harness
    // reports dialect-category checks as skipped.
    let result = conformance::run_conformance(&manifest, None);
    output_result(&result, json_output)?;

    Ok(())
}

fn output_result(result: &ConformanceResult, json_output: bool) -> Result<()> {
    if json_output {
        let typed = TestAdapterOutput {
            adapter: result.adapter.clone(),
            sdk_version: result.sdk_version.clone(),
            tests_run: result.tests_run,
            tests_passed: result.tests_passed,
            tests_failed: result.tests_failed,
            tests_skipped: result.tests_skipped,
            results: result
                .results
                .iter()
                .map(|t| TestAdapterTestResult {
                    name: t.name.clone(),
                    category: format!("{:?}", t.category).to_lowercase(),
                    status: format!("{:?}", t.status).to_lowercase(),
                    message: t.message.clone(),
                    duration_ms: t.duration_ms,
                })
                .collect(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&typed).context("failed to serialize result")?
        );
    } else {
        print!("{}", result.report());
    }
    Ok(())
}

/// Rewrite a conformance result's first spec (`connect`) as a failure.
///
/// Extracted so it can be tested against the REAL path. The bug it carried
/// was invisible inline: it did `tests_passed -= 1`, assuming `connect` had
/// passed. Every unimplemented spec passed unconditionally, so that held by
/// accident — and once unimplemented specs report `Skipped`, `tests_passed`
/// is 0 here. The subtraction then panics in debug and wraps in release,
/// corrupting the result before the `tests_failed > 0` contract fires.
///
/// It moves the counter the test ACTUALLY contributed to, and saturates.
fn mark_connect_failed(result: &mut conformance::ConformanceResult, reason: &str) {
    let Some(connect_test) = result.results.first_mut() else {
        return;
    };
    connect_test.status = conformance::TestStatus::Failed;
    connect_test.message = Some(reason.to_string());

    // RECOUNT from the results, rather than patching deltas.
    //
    // Delta-patching is what produced the underflow this function was
    // extracted to fix, and it had a second bug of the same family: the
    // `Failed` arm carried a comment saying "already counted, do not
    // double-count" while `tests_failed += 1` ran unconditionally below it,
    // so overriding an already-failed spec counted it twice. Recounting
    // makes every prior status correct by construction and removes the class
    // rather than the instance.
    result.tests_passed = result
        .results
        .iter()
        .filter(|r| r.status == conformance::TestStatus::Passed)
        .count();
    result.tests_failed = result
        .results
        .iter()
        .filter(|r| r.status == conformance::TestStatus::Failed)
        .count();
    result.tests_skipped = result
        .results
        .iter()
        .filter(|r| r.status == conformance::TestStatus::Skipped)
        .count();
    result.tests_run = result.tests_passed + result.tests_failed;
}

#[cfg(test)]
mod tests {
    use rocky_adapter_sdk::conformance::{self, TestStatus};
    use rocky_adapter_sdk::{AdapterCapabilities, AdapterManifest};

    /// Overriding a spec that ALREADY failed must not count it twice.
    ///
    /// The delta-patching version had a `Failed` arm commented "already
    /// counted, do not double-count" — while `tests_failed += 1` ran
    /// unconditionally below it. Unreachable today (the first spec always
    /// skips), so it was a latent defect whose comment claimed the opposite
    /// of the code. Recounting from `results` makes it correct by
    /// construction; this pins that.
    #[test]
    fn overriding_an_already_failed_connect_counts_it_once() {
        let manifest = AdapterManifest {
            name: "probe".into(),
            version: "0".into(),
            sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
            dialect: "unknown".into(),
            capabilities: AdapterCapabilities::warehouse_only(),
            auth_methods: vec![],
            config_schema: serde_json::Value::Null,
        };
        let mut result = conformance::run_conformance(&manifest, None);

        // Force the state the dead arm was written for.
        result.results[0].status = TestStatus::Failed;
        super::mark_connect_failed(&mut result, "failed to initialize: probe");

        assert_eq!(
            result.tests_failed, 1,
            "an already-failed spec must count once, not twice"
        );
        assert_eq!(
            result.tests_passed + result.tests_failed + result.tests_skipped,
            result.results.len(),
            "every result counted exactly once"
        );
    }

    /// The init-failure path must not underflow its counters.
    ///
    /// It used to do `tests_passed -= 1` on the assumption that `connect`
    /// had passed. Every unimplemented spec passed unconditionally, so that
    /// held by accident. Once an unimplemented spec reports Skipped,
    /// `tests_passed` is 0 there — and the subtraction panics in debug and
    /// wraps to `u64::MAX` in release, corrupting the result before the
    /// failure contract fires (#475 review).
    ///
    /// This reproduces the exact shape: take a real conformance result whose
    /// `connect` is NOT Passed, and apply the same override.
    #[test]
    fn overriding_connect_as_failed_never_underflows() {
        let manifest = AdapterManifest {
            name: "probe".into(),
            version: "0".into(),
            sdk_version: rocky_adapter_sdk::SDK_VERSION.into(),
            dialect: "unknown".into(),
            capabilities: AdapterCapabilities::warehouse_only(),
            auth_methods: vec![],
            config_schema: serde_json::Value::Null,
        };
        let mut result = conformance::run_conformance(&manifest, None);

        assert_ne!(
            result.results[0].status,
            TestStatus::Passed,
            "fixture precondition: connect must not be Passed, or this test \
             cannot exhibit the underflow"
        );
        let skipped_before = result.tests_skipped;

        // The PRODUCTION path, not a copy of it. An earlier version of this
        // test reimplemented the override inline, and therefore passed even
        // with the real code reverted to the underflowing form.
        super::mark_connect_failed(&mut result, "failed to initialize: probe");

        assert_eq!(result.tests_failed, 1);
        assert_eq!(
            result.tests_skipped,
            skipped_before - 1,
            "the skipped count must lose the test that became a failure"
        );
        // The accounting IDENTITY, not a bound. `tests_run <= results.len()`
        // was satisfied in release by a wrapped `usize::MAX + 1 == 0`, so it
        // could not see the underflow it was written for.
        assert_eq!(
            result.tests_passed + result.tests_failed + result.tests_skipped,
            result.results.len(),
            "every result must be counted exactly once"
        );
        assert_eq!(
            result.tests_run,
            result.tests_passed + result.tests_failed,
            "tests_run counts real work: passed + failed"
        );
    }
}
