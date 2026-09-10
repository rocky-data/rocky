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
//! - Provenance of the rlibs. `deps/` is shared, and nothing in it records
//!   which build produced which file. The pick is the newest rlib per crate
//!   that is **not newer than this test binary**, which is a heuristic, not a
//!   proof. It rules out the states that arise on their own — a later `cargo`
//!   invocation with a different feature set leaves rlibs newer than this
//!   binary, and those are now skipped rather than mixed in. It does NOT rule
//!   out a `deps/` doctored after the build: rlibs copied in, `touch`ed, or
//!   written by another checkout sharing this target directory can carry a
//!   newer mtime than the artifacts this binary linked. In that state the
//!   scaffold could compile against an older `SqlDialect` and this test would
//!   pass when it should fail. Proving otherwise needs artifact identity that
//!   `cargo` does not expose to a test; a clean `target/` is the assumption.

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
    let built_at = self_mtime();
    let edition = workspace_edition();
    let scratch = Path::new(env!("CARGO_TARGET_TMPDIR")).join("init_adapter_scaffold");
    std::fs::create_dir_all(&scratch).expect("create scratch dir");

    // 2. Negative half — the emitted form must NOT compile, and must say why.
    let externs = newest_externs(&deps, built_at);
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

    // ONE compile, against one rlib set. There is deliberately no fallback to
    // other candidates: "try combinations until one compiles" would accept a
    // STALE but self-consistent set, so a scaffold broken against today's
    // `SqlDialect` could still compile against yesterday's and go green on the
    // exact drift this test exists to catch.
    //
    // What is left is the pick itself, and it is a heuristic — see "What this
    // does NOT cover" in the header for the state it cannot rule out.
    let compiled = compile(&lib_rs, &deps, &externs, edition, &scratch);
    if compiled.status.success() {
        return;
    }
    let stderr = String::from_utf8_lossy(&compiled.stderr);
    // Both classes mean the same thing — the rlib set is not self-consistent —
    // and both must be reported as the environment problem they are. Falling
    // through to the scaffold message names `init_adapter.rs` as the cause
    // with full confidence, which is how #1833 came to be filed against the
    // template when the template was correct.
    assert!(
        !is_crate_loading_failure(&stderr) && !is_duplicate_crate_failure(&stderr),
        "rustc could not build against a self-consistent set of rlibs from {}. \
         That is an environment problem, not a scaffold problem: the directory \
         holds rlibs from more than one build — a second checkout sharing this \
         target directory, or a build with a different feature set — and the \
         newest-by-mtime pick takes one crate from each. Re-run after a full \
         `cargo build --tests`, or clear the stale artifacts.\n\
         --- rustc stderr ---\n{stderr}",
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

/// rustc loaded TWO copies of one crate and the types from them do not unify.
///
/// This is the same inconsistent-`deps/` problem [`is_crate_loading_failure`]
/// describes, arriving as a different error class. When one rlib is missing or
/// unreadable rustc fails to LOAD it and emits E0463/E0464; when two are
/// present and both load, nothing fails until a type from one meets a type
/// from the other, which is an ordinary type error — **E0053** here, since the
/// mismatch surfaces on a trait method's signature (#1833).
///
/// So the loading codes cannot catch it, and E0053 alone must not be treated
/// as environmental: a scaffold that really does implement a `SqlDialect`
/// method with the wrong signature emits E0053 too, and that is precisely the
/// drift this test exists to catch. The two are separated by the shape of the
/// message rather than the code.
fn is_duplicate_crate_failure(stderr: &str) -> bool {
    // rustc's own phrasing when two instances of one type meet: "expected
    // `DateTime<Utc>`, found a different `DateTime<Utc>`". It says "a
    // different" only for this situation.
    if stderr.contains("found a different `") {
        return true;
    }
    // The other spelling, when the two instances are reached by different
    // paths: "expected `rocky_ir::ir::ColumnSelection`, found
    // `ColumnSelection`". One type named twice, so the last path segment
    // agrees while the paths do not. A template naming a genuinely wrong type
    // produces two DIFFERENT last segments, which is why the comparison is on
    // the segment and not on the whole path.
    stderr.lines().any(|line| {
        let Some((expected, found)) = expected_and_found(line) else {
            return false;
        };
        expected != found && last_path_segment(expected) == last_path_segment(found)
    })
}

/// The two backticked types out of a rustc `expected `X`, found `Y`` line.
fn expected_and_found(line: &str) -> Option<(&str, &str)> {
    let (_, rest) = line.split_once("expected `")?;
    let (expected, rest) = rest.split_once('`')?;
    let (_, rest) = rest.split_once("found `")?;
    let (found, _) = rest.split_once('`')?;
    Some((expected, found))
}

/// `rocky_ir::ir::ColumnSelection` -> `ColumnSelection`. Generic arguments are
/// left alone: they carry the same ambiguity and comparing them whole is the
/// conservative choice.
fn last_path_segment(path: &str) -> &str {
    path.rsplit("::").next().unwrap_or(path)
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

/// This test binary's own modification time.
///
/// Everything `cargo` linked into this binary was written before it, so it is
/// an upper bound on the rlibs that belong to this build.
fn self_mtime() -> std::time::SystemTime {
    std::fs::metadata(std::env::current_exe().expect("current_exe"))
        .and_then(|m| m.modified())
        .expect("test binary modification time")
}

/// The newest rlib per crate that is **not newer than `not_after`**.
///
/// The bound matters. A later `cargo` invocation with a different feature set
/// leaves a second, newer rlib for the same crate in the shared `deps/` while
/// this test binary is left untouched, and mixing that one in gives a rustc
/// crate-loading error. Skipping anything newer than this binary keeps the
/// pick inside the set of artifacts that could have been linked into it.
///
/// It is still not proof of identity — see the header. It bounds the pick from
/// above; it cannot tell two artifacts written before this binary apart.
fn newest_externs(deps: &Path, not_after: std::time::SystemTime) -> Vec<PathBuf> {
    SCAFFOLD_EXTERNS
        .iter()
        .map(|name| {
            let candidates = rlib_candidates(deps, name);
            candidates
                .iter()
                .find(|path| {
                    std::fs::metadata(path)
                        .and_then(|m| m.modified())
                        .is_ok_and(|t| t <= not_after)
                })
                .unwrap_or_else(|| {
                    panic!(
                        "every lib{name}-*.rlib in {} is newer than this test \
                         binary, so none of them can be what it linked. That is \
                         a stale or shared target directory; re-run after a full \
                         `cargo build --tests`.",
                        deps.display(),
                    )
                })
                .clone()
        })
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

#[cfg(test)]
mod classifier_tests {
    use super::{is_crate_loading_failure, is_duplicate_crate_failure};

    /// Real rustc output from a `deps/` holding two `chrono` rlibs — captured
    /// from the failure #1833 reports, which this test binary produced against
    /// a target directory shared with a second checkout.
    const DUPLICATE_CHRONO: &str = "\
error[E0053]: method `watermark_where` has an incompatible type for trait
  --> /tmp/x/crates/rocky-probe/src/dialect.rs:96:25
   |
96 |         last_watermark: Option<&chrono::DateTime<chrono::Utc>>,
   |                         ^^^^^^ expected `DateTime<Utc>`, found a different `DateTime<Utc>`
";

    /// The other spelling, from the same run: one type reached by two paths.
    const DUPLICATE_ROCKY_IR: &str = "\
error[E0053]: method `select_clause` has an incompatible type for trait
   |
59 |         columns: &ColumnSelection,
   |                  ^^^^^^^^^^^^^^^^ expected `rocky_ir::ir::ColumnSelection`, found `ColumnSelection`
";

    /// A GENUINE scaffold break: the template implements a method with the
    /// wrong return type. Same error CODE as the two above, and it must NOT be
    /// classified as an environment problem — this is the drift the whole test
    /// exists to catch, and misclassifying it would report the suite as green
    /// noise instead of a broken scaffold.
    const REAL_SIGNATURE_DRIFT: &str = "\
error[E0053]: method `literal_escape` has an incompatible type for trait
   |
42 |     fn literal_escape(&self) -> String {
   |                                 ^^^^^^ expected `LiteralEscape`, found `String`
";

    #[test]
    fn a_duplicate_crate_is_recognised_in_both_spellings() {
        assert!(is_duplicate_crate_failure(DUPLICATE_CHRONO));
        assert!(is_duplicate_crate_failure(DUPLICATE_ROCKY_IR));
    }

    #[test]
    fn a_real_signature_drift_is_not_blamed_on_the_environment() {
        assert!(
            !is_duplicate_crate_failure(REAL_SIGNATURE_DRIFT),
            "E0053 alone must not read as a duplicate-crate deps/: a scaffold \
             that implements a trait method with the wrong type emits E0053 \
             too, and that is the defect this test exists to catch"
        );
        assert!(!is_crate_loading_failure(REAL_SIGNATURE_DRIFT));
    }

    /// The loading codes keep their own arm — the two classes are separate
    /// causes with one remedy, and neither detector should absorb the other.
    #[test]
    fn a_missing_rlib_is_still_a_loading_failure() {
        let stderr = "error[E0463]: can't find crate for `rocky_ir`";
        assert!(is_crate_loading_failure(stderr));
        assert!(!is_duplicate_crate_failure(stderr));
    }

    /// A clean compile classifies as neither, so a passing run cannot be
    /// diverted into the environment message.
    #[test]
    fn clean_output_classifies_as_neither() {
        assert!(!is_crate_loading_failure(""));
        assert!(!is_duplicate_crate_failure(""));
    }
}
