//! The structural half of the route-inventory invariant: `rocky-cli`'s raw
//! plan writers are `pub(crate)` and cannot be named from outside the crate.
//!
//! Proof shape — one out-of-crate compile, keyed on the stable error CODE
//! (`E0603`, "item is private"), never on version-specific diagnostic prose:
//!
//! ```text
//!   rocky-cli-compiletest depends on rocky-cli
//!            │  (the build leaves a fresh librocky_cli-*.rlib, plus every
//!            ▼   transitive dep rlib, in this test binary's own deps/ dir)
//!   locate that rlib ──▶ rustc ONCE on a probe that `use`s all five writers
//!            │             (--extern rocky_cli + -L dependency=<deps>, so
//!            ▼              rocky-cli is NEVER recompiled)
//!   assert: compile FAILS with exactly one `error[E0603]` per writer
//! ```
//!
//! Why not `trybuild` snapshots (the retired approach): each snapshot pinned
//! the FULL rendered diagnostic, so any rustc version whose wording or spans
//! differed from the committing dev's broke the snapshot on CI — and each of
//! the six cases recompiled all of rocky-cli, costing 20+ minutes. The error
//! CODE is stable across toolchains; the prose is not. This test reads only
//! the code, and reuses the already-built rlib, so it is both toolchain-robust
//! and cheap (a single metadata-only rustc invocation, no rocky-cli rebuild).
//!
//! The POSITIVE half — the sanctioned public surface stays nameable — is
//! enforced by `sanctioned_public_surface_stays_nameable` below at COMPILE
//! time, which is strictly stronger than grepping the probe's output.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The raw plan writers that MUST be `pub(crate)` in `rocky-cli`. Each one can
/// persist or pre-name a plan, so each must stay behind
/// `commands::propose_governed_run_plan`'s policy gate — unreachable from any
/// external consumer, the fulfillment driver included.
///
/// The probe source and the expected `E0603` count are both derived from this
/// array, so adding or removing a writer needs no other edit here.
const PRIVATE_WRITERS: [&str; 5] = [
    "write_plan",
    "write_plan_governed",
    "write_plan_with_principal",
    "write_plan_v2",
    "governed_plan_id",
];

/// Positive control for the sanctioned PUBLIC surface, enforced by the ordinary
/// build of this crate rather than by the subprocess below.
///
/// `rocky-cli-compiletest` is itself external to `rocky-cli`, so naming
/// `read_plan` (reading a plan grants no authority) and the one sanctioned
/// route to a persisted plan, `propose_governed_run_plan`, exercises the public
/// boundary directly. If either loses `pub`, this file fails to COMPILE with
/// `E0603` and CI goes red — a stronger guarantee than asserting the names are
/// absent from the probe's diagnostics, which would pass vacuously if the probe
/// ever dropped the reference. Naming `rocky_cli` here also keeps the
/// dependency edge live for `cargo-machete`; that edge is what co-locates the
/// rlib the negative test discovers.
#[test]
fn sanctioned_public_surface_stays_nameable() {
    let _read = rocky_cli::plan_store::read_plan;
    let _propose = rocky_cli::commands::propose_governed_run_plan;
}

/// The negative half: none of the five private writers is nameable from outside
/// `rocky-cli`. One out-of-crate `rustc` invocation, asserted on the count of
/// the stable `error[E0603]` code.
#[test]
fn plan_writers_are_not_nameable_externally() {
    let deps = deps_dir();
    let rlib = newest_rocky_cli_rlib(&deps);

    // Probe crate: `use` every private writer by name from outside rocky-cli.
    // A `use` of a private item yields a clean `error[E0603]` with no type-
    // inference noise (unlike a value reference to these generic fns).
    let mut src = String::from("#![allow(unused_imports)]\n");
    for writer in PRIVATE_WRITERS {
        src.push_str(&format!("use rocky_cli::plan_store::{writer};\n"));
    }

    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR")).join("plan_writer_privatization");
    std::fs::create_dir_all(&tmp).expect("create probe scratch dir");
    let probe_rs = tmp.join("probe.rs");
    std::fs::write(&probe_rs, &src).expect("write probe source");

    // `--emit=metadata` runs full analysis (resolution + privacy) but skips
    // codegen and linking; `--color=never` keeps ANSI out of the grep token
    // regardless of the caller's CARGO_TERM_COLOR. `-L dependency=<deps>` lets
    // rustc find rocky-cli's transitive dep rlibs, all co-located in `deps`.
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(&rustc)
        .args([
            "--edition",
            "2021",
            "--crate-type",
            "lib",
            "--crate-name",
            "plan_privacy_probe",
            "--emit=metadata",
            "--color=never",
        ])
        .arg("-o")
        .arg(tmp.join("probe.rmeta"))
        .arg("-L")
        .arg({
            let mut l = std::ffi::OsString::from("dependency=");
            l.push(&deps);
            l
        })
        .arg("--extern")
        .arg({
            let mut e = std::ffi::OsString::from("rocky_cli=");
            e.push(&rlib);
            e
        })
        .arg(&probe_rs)
        .output()
        .expect("spawn rustc for the privacy probe");

    let stderr = String::from_utf8_lossy(&output.stderr);
    let e0603 = stderr.matches("error[E0603]").count();

    assert!(
        !output.status.success(),
        "the privacy probe compiled — the raw plan writers are nameable from \
         outside rocky-cli.\nrlib: {}\n--- rustc stderr ---\n{stderr}",
        rlib.display(),
    );
    assert_eq!(
        e0603,
        PRIVATE_WRITERS.len(),
        "expected exactly {} `error[E0603]` (one per private writer), got {e0603}. \
         A wrong count means a writer lost `pub(crate)`, or the probe failed to \
         load rocky-cli's metadata.\nrlib: {}\n--- rustc stderr ---\n{stderr}",
        PRIVATE_WRITERS.len(),
        rlib.display(),
    );
    // Tie the count to the specific writers: each name must appear in a
    // diagnostic, so the five E0603s are these five items and not some other
    // privacy error that crept in.
    for writer in PRIVATE_WRITERS {
        assert!(
            stderr.contains(writer),
            "private writer `{writer}` did not appear in the compiler \
             diagnostics.\n--- rustc stderr ---\n{stderr}",
        );
    }
}

/// The directory holding this test binary — and, because `rocky-cli-compiletest`
/// depends on `rocky-cli`, a fresh `librocky_cli-*.rlib` alongside every
/// transitive dependency rlib.
fn deps_dir() -> PathBuf {
    std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("test binary has a parent deps/ dir")
        .to_path_buf()
}

/// The newest `librocky_cli-*.rlib` in `deps`.
///
/// A fresh CI build leaves exactly one; a local incremental target may keep
/// older hashes around, so pick the most recently written to match the current
/// source. Privacy is feature-invariant — every rocky-cli build makes these
/// writers `pub(crate)` — so any co-located rlib proves the invariant.
fn newest_rocky_cli_rlib(deps: &Path) -> PathBuf {
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(deps)
        .expect("read deps dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            name.starts_with("librocky_cli-") && name.ends_with(".rlib")
        })
        .collect();
    assert!(
        !candidates.is_empty(),
        "no librocky_cli-*.rlib in {} — the rocky-cli dependency edge is what \
         co-locates it; was the dependency dropped from Cargo.toml?",
        deps.display(),
    );
    candidates.sort_by_key(|path| std::fs::metadata(path).and_then(|m| m.modified()).ok());
    candidates
        .pop()
        .expect("at least one librocky_cli-*.rlib candidate")
}
