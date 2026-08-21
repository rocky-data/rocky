//! The structural half of the drafting-window reopen gate (#1493):
//! `rocky-core`'s raw manifest demotion is `pub(crate)` and cannot be named
//! from outside the crate, so the ONLY way to reopen a drafting window is the
//! evidence-gated [`reopen_for_drafting`], which demands the fulfillment
//! loop's compare-and-swapped record.
//!
//! Proof shape — one out-of-crate compile, keyed on the stable error CODE
//! (`E0603`, "item is private"), never on version-specific diagnostic prose.
//! This is the same mechanism `rocky-cli-compiletest`'s
//! `plan_writer_privatization` uses for the raw plan writers, deliberately
//! reused rather than re-invented:
//!
//! ```text
//!   rocky-core-compiletest depends on rocky-core
//!            │  (the build leaves a fresh librocky_core-*.rlib, plus every
//!            ▼   transitive dep rlib, in this test binary's own deps/ dir)
//!   locate that rlib ──▶ rustc ONCE on a probe that `use`s the raw demotion
//!            │             (--extern rocky_core + -L dependency=<deps>, so
//!            ▼              rocky-core is NEVER recompiled)
//!   assert: compile FAILS with exactly one `error[E0603]`
//! ```
//!
//! Why the error CODE and not `trybuild` (which the `must_use_guard` case in
//! this crate still uses, correctly, because it asserts a LINT fires): a
//! trybuild snapshot pins the full rendered diagnostic, so any rustc whose
//! wording or spans differ breaks it, and each case recompiles the dependency.
//! The code is stable across toolchains; the prose is not.
//!
//! The POSITIVE half — the sanctioned gated entry stays nameable AND keeps its
//! evidence parameter — is enforced by
//! `the_gated_reopen_keeps_its_evidence_parameter` below at COMPILE time,
//! which is strictly stronger than grepping the probe's output.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The raw authority transitions that MUST stay `pub(crate)` in `rocky-core`.
///
/// `demote_merged_manifest_to_phase_a` takes a committed MERGED generation —
/// whose sidecar bytes are pinned by the manifest — and returns that sidecar
/// to the writable drafting namespace. Reachable from outside, it would open
/// the Phase-A window on a legitimate merged generation with no fulfillment
/// round to fill it. The gated `reopen_for_drafting` is the one public route,
/// and it demands the loop's CAS'd record first.
///
/// The probe source and the expected `E0603` count are both derived from this
/// array, so adding or removing an entry needs no other edit here.
const PRIVATE_TRANSITIONS: [&str; 1] = ["demote_merged_manifest_to_phase_a"];

/// Positive control for the sanctioned PUBLIC entry, enforced by the ordinary
/// build of this crate rather than by the subprocess below.
///
/// Binding `reopen_for_drafting` to an explicitly typed function pointer pins
/// two things at COMPILE time: that the symbol is still public, and that it
/// still takes the evidence parameter (`&FulfillStateRecord`). Dropping the
/// evidence — the exact regression this gate exists to prevent — is a type
/// error here, not a silently passing test. Naming `rocky_core` also keeps the
/// dependency edge live for `cargo-machete`; that edge is what co-locates the
/// rlib the negative test discovers.
#[test]
fn the_gated_reopen_keeps_its_evidence_parameter() {
    let _reopen: fn(
        &Path,
        &str,
        &rocky_core::product::spec::ParsedSpec,
        &rocky_core::fulfill::FulfillStateRecord,
    ) -> rocky_core::product::spec::SpecResult<
        rocky_core::product::commit::ReopenOutcome,
    > = rocky_core::product::commit::reopen_for_drafting;
}

/// The negative half: the raw demotion is not nameable from outside
/// `rocky-core`. One out-of-crate `rustc` invocation, asserted on the count of
/// the stable `error[E0603]` code.
#[test]
fn the_raw_demotion_is_not_nameable_externally() {
    let deps = deps_dir();
    let rlib = newest_rocky_core_rlib(&deps);

    // Probe crate: `use` every private transition by name from outside
    // rocky-core. A `use` of a private item yields a clean `error[E0603]`
    // with no type-inference noise.
    let mut src = String::from("#![allow(unused_imports)]\n");
    for item in PRIVATE_TRANSITIONS {
        src.push_str(&format!("use rocky_core::product::commit::{item};\n"));
    }

    let tmp = Path::new(env!("CARGO_TARGET_TMPDIR")).join("reopen_privatization");
    std::fs::create_dir_all(&tmp).expect("create probe scratch dir");
    let probe_rs = tmp.join("probe.rs");
    std::fs::write(&probe_rs, &src).expect("write probe source");

    // `--emit=metadata` runs full analysis (resolution + privacy) but skips
    // codegen and linking; `--color=never` keeps ANSI out of the grep token
    // regardless of the caller's CARGO_TERM_COLOR. `-L dependency=<deps>` lets
    // rustc find rocky-core's transitive dep rlibs, all co-located in `deps`.
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let output = Command::new(&rustc)
        .args([
            "--edition",
            "2021",
            "--crate-type",
            "lib",
            "--crate-name",
            "reopen_privacy_probe",
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
            let mut e = std::ffi::OsString::from("rocky_core=");
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
        "the privacy probe compiled — the raw manifest demotion is nameable from \
         outside rocky-core, so the reopen's evidence gate can be walked around.\n\
         rlib: {}\n--- rustc stderr ---\n{stderr}",
        rlib.display(),
    );
    assert_eq!(
        e0603,
        PRIVATE_TRANSITIONS.len(),
        "expected exactly {} `error[E0603]` (one per private transition), got {e0603}. \
         A wrong count means a transition lost `pub(crate)`, or the probe failed to \
         load rocky-core's metadata.\nrlib: {}\n--- rustc stderr ---\n{stderr}",
        PRIVATE_TRANSITIONS.len(),
        rlib.display(),
    );
    // Tie the count to the specific item: its name must appear in a
    // diagnostic, so the E0603 is this item and not some other privacy error
    // that crept in.
    for item in PRIVATE_TRANSITIONS {
        assert!(
            stderr.contains(item),
            "private transition `{item}` did not appear in the compiler \
             diagnostics.\n--- rustc stderr ---\n{stderr}",
        );
    }
}

/// The directory holding this test binary — and, because
/// `rocky-core-compiletest` depends on `rocky-core`, a fresh
/// `librocky_core-*.rlib` alongside every transitive dependency rlib.
fn deps_dir() -> PathBuf {
    std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("test binary has a parent deps/ dir")
        .to_path_buf()
}

/// The newest `librocky_core-*.rlib` in `deps`.
///
/// A fresh CI build leaves exactly one; a local incremental target may keep
/// older hashes around, so pick the most recently written to match the current
/// source. Privacy is feature-invariant — every rocky-core build makes this
/// transition `pub(crate)` — so any co-located rlib proves the invariant.
fn newest_rocky_core_rlib(deps: &Path) -> PathBuf {
    let mut candidates: Vec<PathBuf> = std::fs::read_dir(deps)
        .expect("read deps dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            name.starts_with("librocky_core-") && name.ends_with(".rlib")
        })
        .collect();
    assert!(
        !candidates.is_empty(),
        "no librocky_core-*.rlib in {} — the rocky-core dependency edge is what \
         co-locates it; was the dependency dropped from Cargo.toml?",
        deps.display(),
    );
    candidates.sort_by_key(|path| std::fs::metadata(path).and_then(|m| m.modified()).ok());
    candidates
        .pop()
        .expect("at least one librocky_core-*.rlib candidate")
}
