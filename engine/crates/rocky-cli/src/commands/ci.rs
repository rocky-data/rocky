//! `rocky ci` — CI/CD runner (compile + test without warehouse).

use std::path::Path;

use anyhow::{Context, Result};
use rocky_engine::test_runner::{TestModels, TestRunInputs};

use super::ModelScope;
use crate::output::{CiOutput, TestFailure, print_json};

/// Execute `rocky ci`.
///
/// `scope` picks the models. [`ModelScope::WholeProject`] (no `--models`)
/// runs every transformation pipeline's models in one in-memory DuckDB, in
/// dependency order, so a model of one pipeline reads the outputs of the
/// pipelines it depends on. The seed file is `data/seed.sql` beside
/// `rocky.toml`. [`ModelScope::Dir`] runs the models under `models_dir`,
/// with the seed file beside that directory.
pub fn run_ci(
    config_path: &Path,
    models_dir: &Path,
    scope: ModelScope,
    contracts_dir: Option<&Path>,
    output_json: bool,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    let (models, project_root) = ci_models(config_path, models_dir, scope)?;
    let result = rocky_engine::ci::run_ci_with(TestRunInputs {
        models_dir,
        project_root: &project_root,
        models,
        contracts_dir,
        model_filter: None,
        run_vars,
    })?;

    if output_json {
        let failures: Vec<TestFailure> = result
            .failures
            .iter()
            .map(|(name, error)| TestFailure {
                name: name.clone(),
                error: error.clone(),
            })
            .collect();
        let output = CiOutput::new(
            result.compile_ok,
            result.tests_ok,
            result.models_compiled,
            result.tests_passed,
            result.tests_failed,
            result.exit_code(),
            result.diagnostics.clone(),
            failures,
        );
        print_json(&output)?;
    } else {
        println!("Rocky CI Pipeline");
        println!();
        println!(
            "  Compile: {} ({} models)",
            if result.compile_ok { "PASS" } else { "FAIL" },
            result.models_compiled
        );
        println!(
            "  Test:    {} ({} passed, {} failed)",
            if result.tests_ok { "PASS" } else { "FAIL" },
            result.tests_passed,
            result.tests_failed
        );

        for (name, err) in &result.failures {
            println!("    \u{2717} {name}: {err}");
        }

        println!();
        println!("  Exit code: {}", result.exit_code());
    }

    // The process exits with the code the output reports.
    let code = result.exit_code();
    if code != 0 {
        std::process::exit(code);
    }

    Ok(())
}

/// The models `rocky ci` runs, and the project root its seed file is read
/// from.
fn ci_models(
    config_path: &Path,
    models_dir: &Path,
    scope: ModelScope,
) -> Result<(TestModels, std::path::PathBuf)> {
    let dir_root = || {
        models_dir
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    };
    match scope {
        ModelScope::Dir => Ok((TestModels::Dir, dir_root())),
        ModelScope::WholeProject => {
            let project_config = rocky_core::config::load_optional_project_config(Some(
                config_path,
            ))
            .with_context(|| format!("failed to load config from {}", config_path.display()))?;
            let Some(project) = project_config else {
                return Ok((TestModels::Dir, dir_root()));
            };
            let config_root = config_path
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            match crate::models_loader::whole_project_models(config_path, &project)? {
                Some(models) => Ok((TestModels::Preloaded(models), config_root)),
                None => Ok((TestModels::Dir, config_root)),
            }
        }
    }
}
