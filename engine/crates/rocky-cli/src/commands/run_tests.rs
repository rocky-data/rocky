//! `[run] tests_on_run` / `rocky run --tests`: evaluate a model's `[[tests]]`
//! against the table the run just built (#2611).
//!
//! The evaluator is the one `rocky test --declarative` uses
//! ([`super::declarative_test::execute_model_tests`]), batching included
//! (`--batch-tests`, #2756). This module only decides when it runs and what a
//! result does to the run.
//!
//! - `off` never reaches here with work to do: no query, no output field.
//! - `report` records every result and never fails the model.
//! - `enforce` fails the model on an error-severity `fail` or `error` (the
//!   test query could not run, so the data is unproven). The caller treats
//!   the returned error exactly as it treats a volume breach: `model_error`
//!   hook, then contained or fail-fast. The table the model wrote stays in
//!   place; nothing is rolled back.
//!
//! A `severity = "warning"` test is recorded in every mode and never fails a
//! model.

use rocky_core::config::TestsOnRun;
use rocky_core::traits::WarehouseAdapter;
use tracing::{debug, info, warn};

use super::declarative_test::{TestBatching, execute_model_tests};
use crate::output::{RunOutput, RunTestResultOutput};

/// Run `model`'s `[[tests]]` against its target and record the results on
/// `output`. Returns the error the caller must treat as the model's failure,
/// only in `enforce` mode and only for an error-severity `fail` or `error`.
///
/// `batching` is `rocky run --batch-tests`; it changes how many queries run,
/// never a result.
pub(crate) async fn evaluate_model_tests(
    mode: TestsOnRun,
    batching: TestBatching,
    warehouse: &dyn WarehouseAdapter,
    model: &rocky_core::models::Model,
    output: &mut RunOutput,
) -> Option<anyhow::Error> {
    if !mode.is_on() || model.config.tests.is_empty() {
        return None;
    }
    let name = model.config.name.as_str();

    let mut blocking: Vec<String> = Vec::new();
    let run = execute_model_tests(
        &model.config.tests,
        name,
        &model.config.target,
        warehouse,
        batching,
    )
    .await;
    debug!(
        model = name,
        queries = run.queries,
        "tests_on_run queries sent"
    );
    for result in run.results {
        let record = RunTestResultOutput {
            model: result.model,
            test_type: result.test_type,
            column: result.column,
            severity: result.severity,
            status: result.status,
            detail: result.detail,
        };
        if record.status != "pass" {
            warn!(
                model = name,
                test = record.test_type.as_str(),
                status = record.status.as_str(),
                severity = record.severity.as_str(),
                "{}",
                record.detail.as_deref().unwrap_or("test did not pass")
            );
        }
        if record.severity != "warning" && record.status != "pass" {
            let column = record
                .column
                .as_deref()
                .map(|c| format!(".{c}"))
                .unwrap_or_default();
            blocking.push(format!(
                "{}{column} [{}]: {}",
                record.test_type,
                record.status,
                record.detail.as_deref().unwrap_or("no detail")
            ));
        }
        output.test_results.push(record);
    }

    if mode != TestsOnRun::Enforce || blocking.is_empty() {
        return None;
    }
    info!(model = name, "tests_on_run = enforce: failing the model");
    output.test_failed_models.insert(name.to_string());
    Some(anyhow::anyhow!(
        "model '{name}' failed {} error-severity test(s): {}",
        blocking.len(),
        blocking.join("; ")
    ))
}
