//! `rocky ci` — CI/CD runner (compile + test without warehouse).

use std::path::Path;

use anyhow::Result;

use crate::output::{CiOutput, TestFailure, print_json};

/// Execute `rocky ci`.
pub fn run_ci(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    output_json: bool,
    run_vars: &rocky_core::run_vars::RunVars,
) -> Result<()> {
    let result = rocky_engine::ci::run_ci(models_dir, contracts_dir, run_vars)?;

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

    // Exit with the code the result reports, not with a second opinion about
    // it. This gate used to be `if !result.passed()`, and `passed()` is
    // `compile_ok && tests_ok` — which a warnings-only run satisfies. So the
    // JSON said `"exit_code": 4` while the process exited 0, and a CI step
    // gating on `$?` disagreed with one gating on `jq .exit_code` about the
    // same run (#2030). `exit_code()` is the single definition of the
    // outcome; this is the only place that acts on it.
    let code = result.exit_code();
    if code != 0 {
        std::process::exit(code);
    }

    Ok(())
}
