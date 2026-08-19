//! End-to-end coverage for `rocky test` selector scope (#1428).
//!
//! `rocky test` had two independent ways to report success for work it never
//! did: the `--declarative` path accepted a models directory that does not
//! exist, and *neither* path validated `--model`. An unknown model name was
//! byte-identical to a real model that declares no tests — both `total: 0`,
//! both exit 0 — so a renamed model silently stopped being tested.
//!
//! The `a_model_with_no_tests_is_distinguishable_from_an_unknown_model` case
//! is the load-bearing one: it pins BOTH halves, that the legitimate empty
//! result still succeeds and that it is no longer confusable with a typo.

use std::fs;
use std::process::{Command, Output};

/// A models dir plus a `rocky.toml` with a constructible duckdb target.
///
/// The config matters: `run_declarative_tests` loads the config, resolves the
/// pipeline and builds the adapter registry *before* it loads any model, so a
/// project without a valid target fails earlier for an unrelated reason and a
/// test asserting the models-dir behaviour would pass vacuously.
fn scaffold(dir: &std::path::Path) -> std::path::PathBuf {
    let models = dir.join("models");
    fs::create_dir_all(&models).expect("create models dir");
    fs::write(
        dir.join("rocky.toml"),
        "[adapter]\ntype = \"duckdb\"\npath = \"test.duckdb\"\n\n\
         [pipeline.poc]\ntype = \"transformation\"\nmodels = \"models/**\"\n\n\
         [pipeline.poc.target.governance]\nauto_create_schemas = true\n",
    )
    .expect("write rocky.toml");
    models
}

/// Writes a model; `with_test` controls whether it declares a `[[tests]]`
/// block, which is what separates "no tests" from "no such model".
///
/// The target catalog is `test`, matching the `test.duckdb` file the scaffold
/// configures — DuckDB names the catalog after the database file, so a target
/// naming any other catalog makes `EXPLAIN` fail and `rocky estimate` produce
/// no estimates at all. That is not hypothetical: an earlier version used
/// `c`.`s`, which silently turned every estimate assertion into a test of the
/// empty path.
fn write_model(dir: &std::path::Path, name: &str, with_test: bool) {
    fs::write(dir.join(format!("{name}.sql")), "SELECT 1 AS id\n").expect("write model sql");
    let tests = if with_test {
        "\n[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n"
    } else {
        ""
    };
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ndepends_on = []\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"test\"\nschema = \"main\"\ntable = \"{name}\"\n{tests}"
        ),
    )
    .expect("write model sidecar");
}

fn test_cmd(root: &std::path::Path, models: &std::path::Path, args: &[&str]) -> Output {
    // `--output` and `--config` are global flags and go BEFORE the subcommand,
    // matching `plan_model_scope.rs`.
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .args(["--output", "json"])
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg("test")
        .arg("--models")
        .arg(models)
        .current_dir(root)
        .env("RUST_LOG", "error");
    for arg in args {
        command.arg(arg);
    }
    command.output().expect("spawn rocky test")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// The headline: one command, one flag apart, two answers to "this directory
/// does not exist". `compile` and `ci` both refuse it.
#[test]
fn declarative_refuses_a_missing_models_dir() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    scaffold(temp.path());
    let missing = temp.path().join("not_a_real_dir");

    let declarative = test_cmd(temp.path(), &missing, &["--declarative"]);
    assert!(
        !declarative.status.success(),
        "--declarative must refuse a models dir that does not exist; stdout: {}",
        String::from_utf8_lossy(&declarative.stdout)
    );
    assert!(
        stderr(&declarative).contains("no models found"),
        "the refusal must name the cause: {}",
        stderr(&declarative)
    );

    // And it must agree with its own sibling path, which already refused.
    let plain = test_cmd(temp.path(), &missing, &[]);
    assert!(
        !plain.status.success(),
        "the non-declarative path must still refuse it too"
    );
}

/// A directory that exists but holds no models is the same class of mistake.
#[test]
fn declarative_refuses_an_empty_models_dir() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());

    let output = test_cmd(temp.path(), &models, &["--declarative"]);
    assert!(
        !output.status.success(),
        "an empty models dir must be refused, not reported as zero tests"
    );
}

/// Neither path validated `--model`. Both must now.
#[test]
fn test_refuses_an_unknown_model_on_both_paths() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true);

    for extra in [
        vec!["--model", "ghost"],
        vec!["--declarative", "--model", "ghost"],
    ] {
        let label = extra.join(" ");
        let output = test_cmd(temp.path(), &models, &extra);
        assert!(
            !output.status.success(),
            "`rocky test {label}` must refuse an unknown model; stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            stderr(&output).contains("model 'ghost' not found"),
            "`rocky test {label}` must name the model it could not find: {}",
            stderr(&output)
        );
    }
}

/// The regression that matters most.
///
/// Before #1428 these two produced a byte-identical payload and the same exit
/// code, so a renamed model was undetectable. The fix must separate them
/// WITHOUT making the legitimate case fail: a real model that simply declares
/// no tests is not an error.
#[test]
fn a_model_with_no_tests_is_distinguishable_from_an_unknown_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true); // has a [[tests]] block
    write_model(&models, "beta", false); // exists, declares none

    let real = test_cmd(temp.path(), &models, &["--declarative", "--model", "beta"]);
    let unknown = test_cmd(temp.path(), &models, &["--declarative", "--model", "ghost"]);

    assert!(
        real.status.success(),
        "a real model with no tests must still succeed — it is not an error: {}",
        stderr(&real)
    );
    assert!(
        !unknown.status.success(),
        "an unknown model must not succeed"
    );
    assert_ne!(
        (real.status.code(), real.stdout.clone()),
        (unknown.status.code(), unknown.stdout.clone()),
        "the two cases must no longer be indistinguishable"
    );
}

/// `--model` naming a real model is unaffected by the guard.
///
/// Asserted two ways on purpose. A model with no tests must **succeed** — an
/// assertion the earlier version of this test lacked, so it passed while the
/// command exited 1. A model that HAS tests fails on this fixture, because its
/// `not_null` assertion runs against a target table that was never
/// materialised; that failure must be the execution error and never a
/// refusal, which is what distinguishes "the guard let it through" from "the
/// guard rejected it".
#[test]
fn a_known_model_is_not_refused() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true); // declares a [[tests]] block
    write_model(&models, "beta", false); // declares none

    let no_tests = test_cmd(temp.path(), &models, &["--declarative", "--model", "beta"]);
    assert!(
        no_tests.status.success(),
        "a known model with no tests must SUCCEED, not merely avoid the refusal: {}",
        stderr(&no_tests)
    );

    let with_tests = test_cmd(temp.path(), &models, &["--declarative", "--model", "alpha"]);
    assert!(
        !stderr(&with_tests).contains("not found"),
        "a known model must never be refused as unknown: {}",
        stderr(&with_tests)
    );
    assert!(
        stderr(&with_tests).contains("execution error")
            || stderr(&with_tests).contains("hard failure")
            || with_tests.status.success(),
        "if it fails it must be for test execution, not selector resolution: {}",
        stderr(&with_tests)
    );
}

// ---------------------------------------------------------------------------
// `rocky estimate` / `rocky retention-status` — same selector contract
// ---------------------------------------------------------------------------

fn cmd(root: &std::path::Path, models: &std::path::Path, verb: &str, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_rocky"));
    command
        .args(["--output", "json"])
        .arg("--config")
        .arg(root.join("rocky.toml"))
        .arg(verb)
        .arg("--models")
        .arg(models)
        .current_dir(root)
        .env("RUST_LOG", "error");
    for arg in args {
        command.arg(arg);
    }
    command.output().expect("spawn rocky")
}

/// Both commands refused nothing before #1428: an unknown `--model` produced an
/// empty result at exit 0, indistinguishable from a project with nothing to
/// report.
#[test]
fn estimate_and_retention_status_refuse_an_unknown_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true);

    for verb in ["estimate", "retention-status"] {
        let output = cmd(temp.path(), &models, verb, &["--model", "ghost"]);
        assert!(
            !output.status.success(),
            "`rocky {verb} --model ghost` must refuse an unknown model; stdout: {}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(
            stderr(&output).contains("model 'ghost' not found"),
            "`rocky {verb}` must name the model it could not find: {}",
            stderr(&output)
        );
    }
}

/// A known model is never caught by the guard above.
#[test]
fn estimate_and_retention_status_accept_a_known_model() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true);

    for verb in ["estimate", "retention-status"] {
        let output = cmd(temp.path(), &models, verb, &["--model", "alpha"]);
        assert!(
            !stderr(&output).contains("not found"),
            "`rocky {verb} --model alpha` must not be refused: {}",
            stderr(&output)
        );
    }
}

/// `retention-status` on a project with models still succeeds and reports them,
/// so the empty-result branch cannot swallow a normal run.
#[test]
fn retention_status_reports_models_when_the_project_has_them() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true);

    let output = cmd(temp.path(), &models, "retention-status", &[]);
    assert!(
        output.status.success(),
        "a healthy project must succeed: {}",
        stderr(&output)
    );
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim())
            .expect("retention-status emits JSON");
    assert!(
        !json["models"].as_array().expect("models array").is_empty(),
        "models must be reported, not swallowed by the empty branch: {json}"
    );
    assert!(
        json.get("message").is_none(),
        "a non-empty result must NOT carry the empty-result explanation: {json}"
    );
}

/// The `message` field must be EMITTED, not merely declared.
///
/// Before this, the only assertion about `message` anywhere was a negative one
/// (that a populated result omits it), so reverting either command to its
/// pre-fix empty payload stayed green — the field was published in the schema,
/// the SDK and the TS bindings with nothing pinning that it is ever set.
#[test]
fn an_empty_estimate_says_why_it_is_empty() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    // An EMPTY models dir reaches the empty branch deterministically. An
    // earlier version of this test scaffolded a model and then branched on
    // whether the payload happened to be empty — on this fixture it never was,
    // so the assertion that matters never ran and a mutation reverting the
    // fix stayed green. Assert the empty case unconditionally.
    let models = scaffold(temp.path());

    let output = cmd(temp.path(), &models, "estimate", &[]);
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim())
            .expect("estimate emits JSON");
    assert!(
        json["estimates"]
            .as_array()
            .expect("estimates array")
            .is_empty(),
        "fixture precondition: this project must produce no estimates: {json}"
    );
    let msg = json
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        !msg.is_empty(),
        "an empty estimates array MUST carry the explanation the schema promises: {json}"
    );
}

/// A populated result must NOT carry the empty-result explanation — the other
/// half of the contract, split out so neither case can silently not run.
#[test]
fn a_populated_estimate_carries_no_empty_message() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "alpha", true);

    let output = cmd(temp.path(), &models, "estimate", &[]);
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim())
            .expect("estimate emits JSON");
    assert!(
        !json["estimates"]
            .as_array()
            .expect("estimates array")
            .is_empty(),
        "fixture precondition: this project must produce an estimate: {json}"
    );
    assert!(
        json.get("message").is_none(),
        "a populated result must not carry the empty-result explanation: {json}"
    );
}

/// Same contract for `retention-status` under `--drift`, the one route that
/// can legitimately produce an empty `models` array.
#[test]
fn an_empty_drift_result_says_why_it_is_empty() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_model(&models, "beta", false); // declares no retention policy

    let output = cmd(temp.path(), &models, "retention-status", &["--drift"]);
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim())
            .expect("retention-status emits JSON");
    // Asserted unconditionally, and `expect` rather than `is_none_or`:
    // `is_none_or` is TRUE when `models` is missing or not an array, so the
    // precondition would pass vacuously if the command stopped emitting the
    // field at all. Two versions of this test have now been wrong in that
    // family — first branching on a condition that was never true, then
    // accepting an absent field as satisfying it.
    let models_reported = json["models"]
        .as_array()
        .expect("payload must carry a `models` array");
    assert!(
        models_reported.is_empty(),
        "fixture precondition: beta declares no retention policy, so --drift must retain nothing: {json}"
    );
    let msg = json
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        !msg.is_empty(),
        "an empty --drift result MUST explain itself: {json}"
    );
    assert!(
        msg.contains("retention policy"),
        "the explanation must name the reason, not be generic: {msg}"
    );
}

/// Writes a model whose target names a catalog DuckDB does not have, so
/// `EXPLAIN` fails and the model yields no estimate.
///
/// This reproduces a path that is otherwise unreachable in a healthy fixture:
/// models ARE selected, but every one of them fails before producing an
/// estimate. It was found by mutation — reverting the guard for this case left
/// the whole suite green, because a correct fixture never reaches it.
fn write_unestimable_model(dir: &std::path::Path, name: &str) {
    fs::write(dir.join(format!("{name}.sql")), "SELECT 1 AS id\n").expect("write model sql");
    fs::write(
        dir.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\ndepends_on = []\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"no_such_catalog\"\nschema = \"nope\"\ntable = \"{name}\"\n"
        ),
    )
    .expect("write model sidecar");
}

/// Models selected, none estimable: the payload must still explain itself.
///
/// Distinct from `an_empty_estimate_says_why_it_is_empty`, which exercises the
/// no-models-selected branch. Both produce `{"estimates": [], "total_models":
/// 0}` — the silent-empty shape #1428 exists to remove — so both need the
/// message, and each needs its own test because they are separate code paths.
#[test]
fn an_estimate_with_no_estimable_models_says_why_it_is_empty() {
    let temp = tempfile::TempDir::new().expect("temp dir");
    let models = scaffold(temp.path());
    write_unestimable_model(&models, "broken");

    let output = cmd(temp.path(), &models, "estimate", &[]);
    let json: serde_json::Value =
        serde_json::from_str(String::from_utf8_lossy(&output.stdout).trim())
            .expect("estimate emits JSON");
    assert!(
        json["estimates"]
            .as_array()
            .expect("estimates array")
            .is_empty(),
        "fixture precondition: no model may produce an estimate here: {json}"
    );
    let msg = json
        .get("message")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    assert!(
        !msg.is_empty(),
        "models were selected but none estimable — the payload MUST say so: {json}"
    );
}
