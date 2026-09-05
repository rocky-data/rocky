//! `rocky init-adapter` emits a crate that COMPILES — checked here, not in a
//! new contributor's terminal (#1607).
//!
//! Before this test nothing in CI compiled the scaffold. The `#[cfg(test)]`
//! blocks inside the template are text, not code, and the render tests in
//! `commands/init_adapter.rs` assert on that text against a hand-maintained
//! list of method names. A required method added to `SqlDialect` and forgotten
//! in both places shipped a broken scaffold silently — twice, until #1605.
//!
//! Proof shape:
//!
//! ```text
//!   rocky init-adapter probe   (the REAL binary, cwd = a temp dir)
//!            │
//!            ▼
//!   <tmp>/crates/rocky-probe/src/{lib,dialect,adapter,types}.rs
//!            │
//!            ├─ as emitted      ──▶ rustc MUST FAIL, naming the scaffold's
//!            │                      own `compile_error!` message
//!            │
//!            └─ literal_escape   ──▶ rustc MUST SUCCEED
//!               := LiteralEscape::Standard
//! ```
//!
//! Two things this has to work around, both named in #1607:
//!
//! 1. The scaffold's `Cargo.toml` uses `edition.workspace = true` and
//!    `{ workspace = true }` dependencies, so `cargo` cannot build it
//!    standalone without a synthesized workspace root. We do not synthesize
//!    one: `rustc` is driven directly against the rlibs this test binary's own
//!    build already left in `deps/`, which is both cheaper (one metadata-only
//!    invocation, no rebuild of `rocky-core`) and free of a second dependency
//!    resolution that could drift from the workspace's.
//! 2. `literal_escape` is emitted as a deliberate `compile_error!` (#1605).
//!    The refusal IS the feature, so the negative half asserts it fires and
//!    the positive half substitutes exactly one variant before compiling.
//!
//! The negative half keys on the scaffold's **own** message text, not on rustc
//! prose or an error code: `compile_error!` has no stable code, and the text is
//! ours to keep stable. The positive half keys on nothing but exit status.
//!
//! Both halves ASSERT rather than skip when the environment is not there. A
//! skip that silently always skips is the vacuous pass this test exists to
//! replace.
//!
//! # What this does NOT cover
//!
//! - The emitted `Cargo.toml`. Driving `rustc` skips dependency resolution, so
//!   a dependency the template declares wrongly — or fails to declare — is not
//!   caught here. The three render tests in `commands/init_adapter.rs` and a
//!   human running `rocky init-adapter` still own that half.
//! - Provenance of the rlibs. The externs are picked newest-by-mtime out of the
//!   shared `deps/` directory, which is a heuristic, not a proof that they came
//!   from this build. The compile below is deliberately written so the only
//!   thing that heuristic can cause is a false FAILURE — see the comment at the
//!   positive half.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Crates the scaffold's `src/*.rs` name directly. Each needs an `--extern`;
/// edition-2018+ name resolution does not read `-L dependency` for these.
const SCAFFOLD_EXTERNS: [&str; 4] = ["rocky_core", "rocky_ir", "rocky_sql", "chrono"];

#[test]
fn the_scaffold_refuses_to_compile_until_literal_escape_is_chosen_and_compiles_after() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.path();

    // 1. Generate with the REAL binary, so this test reads the shipped output
    //    rather than a copy of the template. `run_init_adapter` resolves its
    //    output against a relative `crates/` path, which is exactly why the
    //    command runs with `current_dir(root)`.
    let out = Command::new(env!("CARGO_BIN_EXE_rocky"))
        .args(["init-adapter", "probe"])
        .current_dir(root)
        .output()
        .expect("rocky must launch");
    assert!(
        out.status.success(),
        "`rocky init-adapter probe` failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );

    let src = root.join("crates").join("rocky-probe").join("src");
    let lib_rs = src.join("lib.rs");
    let dialect_rs = src.join("dialect.rs");
    for path in [
        &lib_rs,
        &dialect_rs,
        &src.join("adapter.rs"),
        &src.join("types.rs"),
    ] {
        assert!(path.exists(), "scaffold did not emit {}", path.display());
    }

    let deps = deps_dir();
    let edition = workspace_edition();
    let scratch = Path::new(env!("CARGO_TARGET_TMPDIR")).join("init_adapter_scaffold");
    std::fs::create_dir_all(&scratch).expect("create scratch dir");

    // 2. Negative half — the emitted form must NOT compile, and must say why.
    let externs = newest_externs(&deps);
    let refusal = compile(&lib_rs, &deps, &externs, edition, &scratch);
    assert!(
        !refusal.status.success(),
        "the emitted scaffold compiled — `literal_escape` answered the question \
         for the adapter author instead of refusing (#1605)"
    );
    let refusal_stderr = String::from_utf8_lossy(&refusal.stderr);
    assert!(
        refusal_stderr.contains("pick LiteralEscape::Standard or LiteralEscape::Backslash"),
        "the scaffold must fail with its OWN `compile_error!` message, not some \
         other error. If this is a crate-loading error (E0460 / E0461 / E0463 / \
         E0464 / E0514) the rlibs in {} are inconsistent — an environment \
         problem, not a scaffold problem.\n--- rustc stderr ---\n{refusal_stderr}",
        deps.display(),
    );

    // 3. Positive half — substitute exactly one variant and compile.
    //
    //    The split is the same one `the_scaffolded_literal_escape_refuses_to_
    //    compile_until_it_is_chosen` uses in `commands/init_adapter.rs`, so if
    //    the template's shape changes the two tests break together.
    let rendered = std::fs::read_to_string(&dialect_rs).expect("read dialect.rs");
    const SIGNATURE: &str = "fn literal_escape(&self) -> LiteralEscape {";
    let (head, after) = rendered
        .split_once(SIGNATURE)
        .expect("dialect.rs renders literal_escape");
    let (body, rest) = after
        .split_once("\n    }")
        .expect("the literal_escape body closes");
    assert!(
        body.contains("compile_error!"),
        "expected the refusal in the emitted body, got: {body}"
    );
    std::fs::write(
        &dialect_rs,
        format!("{head}{SIGNATURE}\n        LiteralEscape::Standard\n    }}{rest}"),
    )
    .expect("write the substituted dialect.rs");

    // ONE compile, against the newest rlib for each crate. There is
    // deliberately no fallback to other candidates: "try combinations until one
    // compiles" would accept a STALE but self-consistent set, so a scaffold
    // that is broken against today's `SqlDialect` could still compile against
    // yesterday's and the test would go green on the exact drift it exists to
    // catch.
    //
    // The residual risk is therefore a false FAILURE on a `target/` holding
    // several builds, never a false PASS. That is the safe direction, and the
    // message below says how to clear it.
    let compiled = compile(&lib_rs, &deps, &externs, edition, &scratch);
    if compiled.status.success() {
        return;
    }
    let stderr = String::from_utf8_lossy(&compiled.stderr);
    assert!(
        !is_crate_loading_failure(&stderr),
        "rustc could not LOAD a dependency from {}. That is an environment \
         problem, not a scaffold problem: the directory holds rlibs from more \
         than one build and the newest-by-mtime pick is not self-consistent. \
         Re-run after a full `cargo build --tests`, or clear the stale \
         artifacts.\n--- rustc stderr ---\n{stderr}",
        deps.display(),
    );
    panic!(
        "the scaffolded crate does not compile once `literal_escape` is chosen \
         — `rocky init-adapter` emits a crate that cannot build. The usual \
         cause is a required `SqlDialect` method added without updating the \
         template in `commands/init_adapter.rs`.\n--- rustc stderr ---\n{stderr}"
    );
}

/// One metadata-only `rustc` over the scaffolded crate root.
///
/// `--emit=metadata` runs full analysis — name resolution, trait checking,
/// which is the whole question here — but skips codegen and linking.
fn compile(
    lib_rs: &Path,
    deps: &Path,
    externs: &[PathBuf],
    edition: &str,
    scratch: &Path,
) -> std::process::Output {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let mut cmd = Command::new(&rustc);
    cmd.args([
        "--edition",
        edition,
        "--crate-type",
        "lib",
        "--crate-name",
        "rocky_probe",
        "--emit=metadata",
        "--color=never",
    ])
    .arg("-o")
    .arg(scratch.join("probe.rmeta"))
    .arg("-L")
    .arg({
        let mut l = std::ffi::OsString::from("dependency=");
        l.push(deps);
        l
    });
    for (name, rlib) in SCAFFOLD_EXTERNS.iter().zip(externs) {
        cmd.arg("--extern").arg({
            let mut e = std::ffi::OsString::from(format!("{name}="));
            e.push(rlib);
            e
        });
    }
    cmd.arg(lib_rs)
        .output()
        .expect("spawn rustc for the scaffold probe")
}

/// rustc could not load a dependency's metadata — an inconsistent `deps/`,
/// never a defect in the code being compiled.
fn is_crate_loading_failure(stderr: &str) -> bool {
    ["E0460", "E0461", "E0463", "E0464", "E0514"]
        .iter()
        .any(|code| stderr.contains(&format!("error[{code}]")))
}

/// The directory holding this test binary — and every rlib the `rocky` binary
/// crate's build left behind, `rocky-core` / `rocky-ir` / `rocky-sql` /
/// `chrono` included (all four are in `rocky`'s own dependency graph).
fn deps_dir() -> PathBuf {
    std::env::current_exe()
        .expect("current_exe")
        .parent()
        .expect("test binary has a parent deps/ dir")
        .to_path_buf()
}

/// Candidate rlibs for one crate name, newest first.
fn rlib_candidates(deps: &Path, crate_name: &str) -> Vec<PathBuf> {
    let prefix = format!("lib{crate_name}-");
    let mut found: Vec<PathBuf> = std::fs::read_dir(deps)
        .expect("read deps dir")
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default();
            name.starts_with(&prefix) && name.ends_with(".rlib")
        })
        .collect();
    assert!(
        !found.is_empty(),
        "no {prefix}*.rlib in {} — the `rocky` crate depends on it, which is \
         what co-locates the rlib; was that dependency edge dropped?",
        deps.display(),
    );
    found.sort_by_key(|path| std::fs::metadata(path).and_then(|m| m.modified()).ok());
    found.reverse();
    found
}

/// The newest rlib for each crate the scaffold names.
fn newest_externs(deps: &Path) -> Vec<PathBuf> {
    SCAFFOLD_EXTERNS
        .iter()
        .map(|name| rlib_candidates(deps, name)[0].clone())
        .collect()
}

/// The workspace's Rust edition, read from `engine/Cargo.toml`.
///
/// The scaffold's `Cargo.toml` says `edition.workspace = true`, so pinning a
/// literal here would let the test compile the scaffold under an edition the
/// generated crate would never actually use — the exact drift this file is
/// about.
fn workspace_edition() -> &'static str {
    static EDITION: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    EDITION.get_or_init(|| {
        let manifest = Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("the rocky crate sits under the engine workspace root")
            .join("Cargo.toml");
        let text = std::fs::read_to_string(&manifest)
            .unwrap_or_else(|e| panic!("read {}: {e}", manifest.display()));
        let (_, after) = text
            .split_once("[workspace.package]")
            .expect("engine/Cargo.toml declares [workspace.package]");
        let line = after
            .lines()
            .find(|l| l.trim_start().starts_with("edition"))
            .expect("[workspace.package] declares an edition");
        line.split('"')
            .nth(1)
            .expect("edition is a quoted string")
            .to_string()
    })
}
