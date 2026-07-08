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
    println!(
        "\n{}",
        if report.passed() {
            "VERIFIED"
        } else {
            "NOT VERIFIED"
        }
    );
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
