//! `rocky connector test <name>` — run a child connector against recorded
//! input, more than once, and check what it hands back.
//!
//! The runs are listed in `connector-test.toml` beside the connector's
//! manifest. Each `[[run]]` names option overrides, which a connector uses to
//! switch to its recorded input, and the rows it must write. The command:
//!
//! - runs the child through the same runner and verifier as `rocky load`, in
//!   a temporary landing directory, with no secrets;
//! - feeds each run the state the run before it returned;
//! - checks the row count, the declared state check, and that every Parquet
//!   file of every run has the same column names and types (the Arrow types
//!   Parquet's logical types map to, so `BIGINT` and `TIMESTAMP` differ).
//!
//! It takes no claim, opens no state store and loads nothing into the
//! warehouse. The child gets no secrets, but Rocky does not block the
//! network: a connector that ignores its replay option and can reach its
//! source without credentials runs against live data, and nothing here can
//! tell.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use tracing::warn;

use rocky_core::config::LoadedConfig;
use rocky_core::connector::{ConnectorOptions, MAX_OPTIONS_BYTES, StateEnvelope};

use crate::connector::runner::{ChildSpec, DEFAULT_OUTPUT_POLL, run_with_retries};
use crate::output::{
    ConnectorTestOutput, ConnectorTestRunOutput, ConnectorTestStateCheck, print_json,
};

use super::connector_init::{HELPER_FILE_NAME, HELPER_PY};
use super::connector_load::{prepare, same_parquet_schema};

/// The spec file `rocky connector test` reads, beside `connector.toml`.
pub const TEST_SPEC_FILE_NAME: &str = "connector-test.toml";

/// `connector-test.toml`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorTestSpec {
    /// The runs, in order. At least one.
    #[serde(rename = "run")]
    pub runs: Vec<ConnectorTestRun>,
}

/// One `[[run]]`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorTestRun {
    /// Merged over `[connector.<name>.options]` for this run.
    #[serde(default)]
    pub options: ConnectorOptions,
    /// The rows the run must write.
    pub rows: u64,
    /// Checked only when set.
    #[serde(default)]
    pub state: Option<ConnectorTestStateCheck>,
}

impl ConnectorTestSpec {
    /// Read and check a spec file.
    ///
    /// # Errors
    ///
    /// The file cannot be read, does not parse (an unknown key included), or
    /// lists no run.
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read the test spec {}", path.display()))?;
        let spec: Self = toml::from_str(&text)
            .with_context(|| format!("the test spec {} is not valid", path.display()))?;
        if spec.runs.is_empty() {
            bail!("the test spec {} lists no [[run]]", path.display());
        }
        Ok(spec)
    }
}

/// The helper version a copy of `rocky_connector.py` declares.
fn helper_version(text: &str) -> Option<u32> {
    text.lines()
        .find_map(|line| line.strip_prefix("HELPER_VERSION = "))
        .and_then(|v| v.trim().parse().ok())
}

/// A warning when the manifest directory holds an older helper copy.
fn stale_helper_warning(manifest_dir: &Path) -> Option<String> {
    let path = manifest_dir.join(HELPER_FILE_NAME);
    let copy = std::fs::read_to_string(&path).ok()?;
    let shipped = helper_version(HELPER_PY)?;
    let Some(found) = helper_version(&copy) else {
        return Some(format!(
            "{} declares no HELPER_VERSION, so it may be older than the helper this \
             Rocky ships (version {shipped}); copy the new one from a fresh \
             `rocky connector init`",
            path.display()
        ));
    };
    (found < shipped).then(|| {
        format!(
            "{} is helper version {found}, but this Rocky ships version {shipped}; \
             copy the new one from a fresh `rocky connector init`",
            path.display()
        )
    })
}

fn merged_options(base: &ConnectorOptions, run: &ConnectorTestRun) -> Result<Vec<u8>> {
    let mut map = base.as_map().clone();
    for (key, value) in run.options.as_map() {
        map.insert(key.clone(), value.clone());
    }
    let bytes = serde_json::to_vec(&map)?;
    if bytes.len() > MAX_OPTIONS_BYTES {
        bail!(
            "the merged options are {} bytes as JSON, but the most allowed is {MAX_OPTIONS_BYTES}",
            bytes.len()
        );
    }
    Ok(bytes)
}

/// Run the test and return its output. A failed check is reported in the
/// output, not as an error.
///
/// # Errors
///
/// The connector, its manifest or the spec cannot be read.
pub async fn execute_connector_test(
    config_path: &Path,
    loaded: &LoadedConfig,
    name: &str,
    spec_path: Option<&Path>,
) -> Result<ConnectorTestOutput> {
    let prepared = prepare(config_path, loaded, name)?;
    let manifest_dir = prepared
        .manifest_path
        .parent()
        .unwrap_or(Path::new("."))
        .to_path_buf();
    let spec_path: PathBuf =
        spec_path.map_or_else(|| manifest_dir.join(TEST_SPEC_FILE_NAME), Path::to_path_buf);
    let spec = ConnectorTestSpec::load(&spec_path)?;

    let mut warnings = Vec::new();
    if let Some(warning) = stale_helper_warning(&manifest_dir) {
        warn!(connector = name, "{warning}");
        warnings.push(warning);
    }

    let landing = tempfile::tempdir().context("cannot create a temporary landing directory")?;
    let mut state: Option<StateEnvelope> = None;
    let mut first_file: Option<PathBuf> = None;
    let mut runs = Vec::with_capacity(spec.runs.len());
    for (index, run) in spec.runs.iter().enumerate() {
        let number = u32::try_from(index + 1).unwrap_or(u32::MAX);
        let mut report = ConnectorTestRunOutput {
            run: number,
            expected_rows: run.rows,
            rows: None,
            files: 0,
            state_check: run.state,
            passed: false,
            error: None,
        };
        let outcome = run_one(
            name,
            &prepared,
            run,
            state.as_ref(),
            landing.path(),
            &mut first_file,
            &mut report,
        )
        .await;
        match outcome {
            Ok(next) => {
                report.passed = true;
                state = Some(next);
                runs.push(report);
            }
            Err(e) => {
                report.error = Some(format!("{e:#}"));
                runs.push(report);
                break;
            }
        }
    }
    let passed = runs.len() == spec.runs.len() && runs.iter().all(|r| r.passed);
    Ok(ConnectorTestOutput {
        version: env!("CARGO_PKG_VERSION").to_string(),
        command: "connector test".to_string(),
        connector: name.to_string(),
        spec: spec_path.display().to_string(),
        passed,
        runs,
        warnings,
    })
}

/// One run. Returns the state to feed the next run.
async fn run_one(
    name: &str,
    prepared: &super::connector_load::Prepared<'_>,
    run: &ConnectorTestRun,
    state: Option<&StateEnvelope>,
    landing: &Path,
    first_file: &mut Option<PathBuf>,
    report: &mut ConnectorTestRunOutput,
) -> Result<StateEnvelope> {
    let options = merged_options(&prepared.config.options, run)?;
    let run_id = uuid::Uuid::new_v4().simple().to_string();
    let done = run_with_retries(&ChildSpec {
        name,
        argv: &prepared.argv,
        // No secrets: a run that does not use its recorded input fails.
        secrets: &[],
        run_id: &run_id,
        limits: prepared.limits,
        state,
        state_schema_version: prepared.manifest.state_schema_version,
        landing_dir: landing,
        retries: 0,
        output_poll: DEFAULT_OUTPUT_POLL,
        options_json: &options,
    })
    .await
    .map_err(|failed| anyhow::anyhow!("{}", failed.error))?;
    let batch = done.batch;
    report.rows = Some(batch.total_rows());
    report.files = batch.files.len() as u64;
    if batch.total_rows() != run.rows {
        bail!(
            "the run wrote {} rows, but the spec expects {}",
            batch.total_rows(),
            run.rows
        );
    }
    for file in &batch.files {
        match first_file {
            None => *first_file = Some(file.path.clone()),
            Some(first) => same_parquet_schema(first, &file.path)
                .and_then(|()| same_arrow_schema(first, &file.path))
                .with_context(|| {
                    format!(
                        "{} does not have the schema of the first file",
                        file.relative
                    )
                })?,
        }
    }
    // No state (the first run) counts as a JSON null: a connector that
    // returns null has not advanced, and one that keeps null is unchanged.
    let null = serde_json::Value::Null;
    let given = state.map_or(&null, |s| &s.value);
    match run.state {
        Some(ConnectorTestStateCheck::Unchanged) if given != &batch.state.value => {
            bail!(
                "the spec expects the state unchanged, but the run returned {} for {given}",
                batch.state.value,
            );
        }
        Some(ConnectorTestStateCheck::Advanced) if given == &batch.state.value => {
            bail!(
                "the spec expects the state to advance, but the run returned it unchanged ({})",
                batch.state.value
            );
        }
        _ => {}
    }
    Ok(batch.state)
}

/// The column names and Arrow types of two Parquet files match. The Arrow
/// type carries the Parquet logical type, which the physical type does not:
/// `TIMESTAMP` and `BIGINT` are both INT64. Nullability is not compared.
fn same_arrow_schema(first: &Path, other: &Path) -> Result<()> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let columns = |path: &Path| -> Result<Vec<(String, String)>> {
        let builder = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path)?)?;
        Ok(builder
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().to_string()))
            .collect())
    };
    let (a, b) = (columns(first)?, columns(other)?);
    if a != b {
        bail!("expected {a:?}, found {b:?}");
    }
    Ok(())
}

fn print_text(output: &ConnectorTestOutput) {
    for warning in &output.warnings {
        println!("warning: {warning}");
    }
    for run in &output.runs {
        let rows = run.rows.map_or_else(|| "-".to_string(), |r| r.to_string());
        match &run.error {
            None => println!("run {}: ok, {rows} rows in {} file(s)", run.run, run.files),
            Some(e) => println!("run {}: FAILED: {e}", run.run),
        }
    }
    if output.passed {
        println!(
            "Connector `{}` passed all {} run(s) in {}",
            output.connector,
            output.runs.len(),
            output.spec
        );
    } else {
        println!(
            "Connector `{}` FAILED at run {} in {}; later runs did not start",
            output.connector,
            output.runs.len(),
            output.spec
        );
    }
}

/// `rocky connector test <name>`. Exits non-zero when a check fails.
///
/// # Errors
///
/// See [`execute_connector_test`], and any failed check.
pub async fn run_connector_test(
    config_path: &Path,
    loaded: &LoadedConfig,
    name: &str,
    spec_path: Option<&Path>,
    json: bool,
) -> Result<()> {
    let output = execute_connector_test(config_path, loaded, name, spec_path).await?;
    if json {
        print_json(&output)?;
    } else {
        print_text(&output);
    }
    if !output.passed {
        bail!("connector `{name}` failed its test");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_spec_refuses_unknown_keys() {
        let err = toml::from_str::<ConnectorTestSpec>("[[run]]\nrows = 1\nrow = 2\n").unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
        let err =
            toml::from_str::<ConnectorTestSpec>("extra = 1\n[[run]]\nrows = 1\n").unwrap_err();
        assert!(err.to_string().contains("unknown field"), "{err}");
    }

    #[test]
    fn a_spec_takes_options_rows_and_a_state_check() {
        let spec: ConnectorTestSpec = toml::from_str(
            "[[run]]\noptions = { replay = \"a.sql\", n = 2 }\nrows = 3\nstate = \"advanced\"\n\
             [[run]]\nrows = 0\nstate = \"unchanged\"\n[[run]]\nrows = 0\n",
        )
        .unwrap();
        assert_eq!(spec.runs.len(), 3);
        assert_eq!(spec.runs[0].options.as_map()["replay"], "a.sql");
        assert_eq!(spec.runs[0].state, Some(ConnectorTestStateCheck::Advanced));
        assert_eq!(spec.runs[1].state, Some(ConnectorTestStateCheck::Unchanged));
        assert_eq!(spec.runs[2].state, None);
        let err = toml::from_str::<ConnectorTestSpec>("[[run]]\nrows = 1\nstate = \"moved\"\n")
            .unwrap_err();
        assert!(err.to_string().contains("unknown variant"), "{err}");
    }

    #[test]
    fn a_spec_with_no_run_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(TEST_SPEC_FILE_NAME);
        std::fs::write(&path, "run = []\n").unwrap();
        let err = ConnectorTestSpec::load(&path).unwrap_err();
        assert!(err.to_string().contains("lists no [[run]]"), "{err}");
    }

    #[test]
    fn run_options_override_the_config_options() {
        let base: ConnectorOptions = toml::from_str("a = 1\nb = \"x\"\n").unwrap();
        let run: ConnectorTestRun =
            toml::from_str("rows = 0\noptions = { b = \"y\", c = true }\n").unwrap();
        let merged: serde_json::Value =
            serde_json::from_slice(&merged_options(&base, &run).unwrap()).unwrap();
        assert_eq!(merged, serde_json::json!({"a": 1, "b": "y", "c": true}));
    }

    #[test]
    fn a_run_table_option_replaces_the_config_table_whole() {
        let base: ConnectorOptions = toml::from_str("nested = { a = 1, b = 2 }\n").unwrap();
        let run: ConnectorTestRun =
            toml::from_str("rows = 0\noptions = { nested = { a = 9 } }\n").unwrap();
        let merged: serde_json::Value =
            serde_json::from_slice(&merged_options(&base, &run).unwrap()).unwrap();
        assert_eq!(merged, serde_json::json!({"nested": {"a": 9}}));
    }

    #[test]
    fn an_older_helper_copy_is_reported_and_a_current_one_is_not() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(stale_helper_warning(dir.path()), None);
        std::fs::write(dir.path().join(HELPER_FILE_NAME), HELPER_PY).unwrap();
        assert_eq!(stale_helper_warning(dir.path()), None);
        let old = HELPER_PY.replace(
            &format!("HELPER_VERSION = {}", helper_version(HELPER_PY).unwrap()),
            "HELPER_VERSION = 0",
        );
        std::fs::write(dir.path().join(HELPER_FILE_NAME), old).unwrap();
        let warning = stale_helper_warning(dir.path()).unwrap();
        assert!(warning.contains("helper version 0"), "{warning}");
        let unversioned = HELPER_PY.replace("HELPER_VERSION = ", "VERSION = ");
        std::fs::write(dir.path().join(HELPER_FILE_NAME), unversioned).unwrap();
        let warning = stale_helper_warning(dir.path()).unwrap();
        assert!(warning.contains("declares no HELPER_VERSION"), "{warning}");
    }
}
