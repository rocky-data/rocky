//! `rocky ci` — CI/CD runner (compile + test without warehouse).

use std::path::Path;

use anyhow::Result;

use crate::output::{CiOutput, TestFailure, print_json};

/// Execute `rocky ci`.
pub fn run_ci(models_dir: &Path, contracts_dir: Option<&Path>, output_json: bool) -> Result<()> {
    let result = rocky_engine::ci::run_ci(models_dir, contracts_dir)?;

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

    if !result.passed() {
        std::process::exit(result.exit_code());
    }

    Ok(())
}
