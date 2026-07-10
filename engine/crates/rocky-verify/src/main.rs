//! `rocky-verify` — a standalone, offline verifier for `rocky-manifest`
//! attestations.
//!
//! It answers, with no producing engine installed: does this manifest conform
//! to the published schema, and — when it carries content-addressed output
//! hashes and you point at the artifact files — do the recorded hashes match
//! the actual bytes?
//!
//! Exit codes: `0` verified, `1` verification failed, `2` could not read or
//! parse the manifest.

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};
use rocky_verify::{MANIFEST_SCHEMA_V0_1, VerifyError, VerifyReport, verify_file};

#[derive(Parser)]
#[command(
    name = "rocky-verify",
    about = "Offline verifier for rocky-manifest attestations",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Verify a manifest against the rocky-manifest v0.1 schema, and
    /// optionally byte-check its output artifacts.
    ///
    /// When no artifact bytes are checked (no `--artifacts-dir`, or the
    /// manifest carries no `output_hashes`), a passing human-readable verdict
    /// reads "VERIFIED (schema only — no byte-identity attested)" rather than
    /// a bare "VERIFIED". The JSON report's `passed` field means the same
    /// thing in both cases — the document is schema-valid and every
    /// *requested* byte-check matched — and the exit code is unchanged.
    Verify {
        /// Path to the manifest JSON file.
        manifest: PathBuf,
        /// Directory holding the output artifact files, to byte-verify any
        /// `output_hashes` the manifest carries. Omit to check structure only.
        #[arg(long, value_name = "DIR")]
        artifacts_dir: Option<PathBuf>,
        /// Emit a JSON report instead of human-readable text.
        #[arg(long)]
        json: bool,
    },
    /// Print the embedded rocky-manifest v0.1 JSON Schema to stdout.
    Schema,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        Command::Schema => {
            println!("{MANIFEST_SCHEMA_V0_1}");
            ExitCode::SUCCESS
        }
        Command::Verify {
            manifest,
            artifacts_dir,
            json,
        } => match verify_file(&manifest, artifacts_dir.as_deref()) {
            Ok(report) => {
                if json {
                    print_json(&manifest, &report);
                } else {
                    print_human(&manifest, &report);
                }
                if report.passed() {
                    ExitCode::SUCCESS
                } else {
                    ExitCode::FAILURE
                }
            }
            Err(e) => {
                report_error(&e);
                ExitCode::from(2)
            }
        },
    }
}

fn report_error(e: &VerifyError) {
    eprintln!("rocky-verify: {e}");
}

fn print_human(manifest: &std::path::Path, report: &VerifyReport) {
    println!("manifest: {}", manifest.display());
    if report.schema_valid {
        println!("  schema:  OK (rocky-manifest v0.1)");
    } else {
        println!("  schema:  FAILED");
        for err in &report.schema_errors {
            println!("    - {err}");
        }
    }
    if report.artifact_checks.is_empty() {
        println!("  bytes:   (not checked — no output artifacts verified)");
    } else {
        for check in &report.artifact_checks {
            if check.matched {
                println!(
                    "  bytes:   OK  {} == {}",
                    check.expected,
                    check
                        .file
                        .as_ref()
                        .map(|p| p.display().to_string())
                        .unwrap_or_default()
                );
            } else {
                let note = check.note.as_deref().unwrap_or("mismatch");
                println!("  bytes:   FAILED  expected {} — {note}", check.expected);
                if let Some(actual) = &check.actual {
                    println!("             actual   {actual}");
                }
            }
        }
    }
    println!("\n{}", verdict_line(report));
}

/// The human-readable bottom-line verdict.
///
/// A pass in which no artifact byte-check ran is labelled schema-only, so a
/// bare `VERIFIED` can never be read as a byte-identity attestation. The JSON
/// report's `passed` field and the process exit code are identical in both
/// pass cases.
fn verdict_line(report: &VerifyReport) -> &'static str {
    if !report.passed() {
        "NOT VERIFIED"
    } else if report.artifact_checks.is_empty() {
        "VERIFIED (schema only — no byte-identity attested)"
    } else {
        "VERIFIED"
    }
}

fn print_json(manifest: &std::path::Path, report: &VerifyReport) {
    let artifacts: Vec<serde_json::Value> = report
        .artifact_checks
        .iter()
        .map(|c| {
            serde_json::json!({
                "expected": c.expected,
                "actual": c.actual,
                "file": c.file.as_ref().map(|p| p.display().to_string()),
                "matched": c.matched,
                "note": c.note,
            })
        })
        .collect();
    let out = serde_json::json!({
        "manifest": manifest.display().to_string(),
        "passed": report.passed(),
        "schema_valid": report.schema_valid,
        "schema_errors": report.schema_errors,
        "artifact_checks": artifacts,
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap_or_default());
}

#[cfg(test)]
mod tests {
    use rocky_verify::ArtifactCheck;

    use super::*;

    fn matched_check() -> ArtifactCheck {
        ArtifactCheck {
            expected: "aa".repeat(32),
            file: Some(PathBuf::from("aa.parquet")),
            actual: Some("aa".repeat(32)),
            matched: true,
            note: None,
        }
    }

    #[test]
    fn schema_only_pass_is_labelled_schema_only() {
        let report = VerifyReport {
            schema_valid: true,
            schema_errors: Vec::new(),
            artifact_checks: Vec::new(),
        };
        assert!(report.passed(), "JSON `passed` semantics must not change");
        assert_eq!(
            verdict_line(&report),
            "VERIFIED (schema only — no byte-identity attested)"
        );
    }

    #[test]
    fn byte_verified_pass_is_bare_verified() {
        let report = VerifyReport {
            schema_valid: true,
            schema_errors: Vec::new(),
            artifact_checks: vec![matched_check()],
        };
        assert!(report.passed());
        assert_eq!(verdict_line(&report), "VERIFIED");
    }

    #[test]
    fn schema_failure_is_not_verified() {
        let report = VerifyReport {
            schema_valid: false,
            schema_errors: vec!["<root>: missing program_hash".into()],
            artifact_checks: Vec::new(),
        };
        assert!(!report.passed());
        assert_eq!(verdict_line(&report), "NOT VERIFIED");
    }

    #[test]
    fn byte_mismatch_is_not_verified() {
        let mut check = matched_check();
        check.matched = false;
        check.note = Some("recorded hash does not match the file's bytes".into());
        let report = VerifyReport {
            schema_valid: true,
            schema_errors: Vec::new(),
            artifact_checks: vec![check],
        };
        assert!(!report.passed());
        assert_eq!(verdict_line(&report), "NOT VERIFIED");
    }
}
