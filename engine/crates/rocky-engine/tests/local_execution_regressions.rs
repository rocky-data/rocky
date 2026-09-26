//! Regression fixtures for local execution: `rocky test` (#2045).
//!
//! PR #1840 changed what `rocky test` and `rocky ci` execute, and the whole
//! in-repo corpus stayed green beside seven defects. Its tests asserted where
//! tables landed, not what a consumer read. Each fixture here asserts a
//! `rocky test` RESULT instead: the value a consumer model reads, or a named
//! refusal. They drive `rocky_engine::test_runner::run_tests`, the core
//! `rocky test` calls.
//!
//! A consumer reads its value through a guard column. The guard raises a
//! DuckDB `error(...)` that names the value when the value is wrong, so the
//! consumer's pass or fail in the `rocky test` result IS the value it read.
//!
//! Shapes 1 and 2 assert the invariant only where #2044 decision 1 is still
//! open. Shape 2 asserts E038: #2044 decision 2 made `type = "ephemeral"` a
//! compile error (#1996). Shape 6 pins the `D012` text against what the
//! binary does today; the step-1 rebuild of #1354 flips both halves together.

use std::path::{Path, PathBuf};

use rocky_engine::test_runner::{ModelTestStatus, TestResult, run_tests};

/// A temp project root with an empty `models/` directory.
fn project() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let models = dir.path().join("models");
    std::fs::create_dir_all(&models).expect("mkdir models");
    (dir, models)
}

/// Write model `name` with its SQL and a sidecar that targets
/// `catalog.schema.table`. `extra` goes into the sidecar before `[target]`.
fn model(models: &Path, name: &str, sql: &str, target: [&str; 3], extra: &str) {
    let [catalog, schema, table] = target;
    std::fs::write(models.join(format!("{name}.sql")), format!("{sql}\n")).expect("write sql");
    std::fs::write(
        models.join(format!("{name}.toml")),
        format!(
            "name = \"{name}\"\n{extra}\n\
             [target]\ncatalog = \"{catalog}\"\nschema = \"{schema}\"\ntable = \"{table}\"\n"
        ),
    )
    .expect("write sidecar");
}

/// SQL for a consumer that reads column `v` from `relation` and fails, naming
/// what it read, unless every row it reads carries `expected`.
fn consumer_sql(relation: &str, expected: i64) -> String {
    format!(
        "SELECT v, CASE WHEN v = {expected} THEN 1 \
         ELSE error('consumer read ' || CAST(v AS VARCHAR) || ', expected {expected}') END AS ok \
         FROM {relation}"
    )
}

/// `data/seed.sql` beside `models/`: `rocky test` loads it before any model.
fn seed(root: &Path, sql: &str) {
    std::fs::create_dir_all(root.join("data")).expect("mkdir data");
    std::fs::write(root.join("data/seed.sql"), sql).expect("write seed.sql");
}

fn test(models: &Path, filter: Option<&str>) -> TestResult {
    run_tests(models, None, filter, &rocky_core::run_vars::RunVars::new())
        .expect("rocky test produces a result")
}

/// The status and error `rocky test` reports for `name`, or `None` when the
/// result does not list it (it did not execute).
fn outcome<'a>(result: &'a TestResult, name: &str) -> Option<(ModelTestStatus, Option<&'a str>)> {
    result
        .model_results
        .iter()
        .find(|r| r.model == name)
        .map(|r| (r.status, r.error.as_deref()))
}

fn passed(result: &TestResult, name: &str) -> bool {
    matches!(outcome(result, name), Some((ModelTestStatus::Pass, _)))
}

/// A consumer that executed and read the wrong value. The guard's own error
/// text is the only way a consumer fails with "consumer read".
fn read_a_wrong_value(result: &TestResult, name: &str) -> bool {
    matches!(
        outcome(result, name),
        Some((ModelTestStatus::Fail, Some(error))) if error.contains("consumer read")
    )
}

fn has_error_code(result: &TestResult, model: &str, code: &str) -> bool {
    result
        .diagnostics
        .iter()
        .any(|d| d.is_error() && d.model == model && &*d.code == code)
}

/// Shape 1: two catalogs, one `schema.table`. Two distinct warehouse objects
/// never silently become one: both values survive, or the run refuses.
#[test]
fn shape1_two_catalogs_one_schema_table_never_merge() {
    let (_root, models) = project();
    model(&models, "a", "SELECT 1 AS v", ["cat1", "s", "t"], "");
    model(&models, "b", "SELECT 2 AS v", ["cat2", "s", "t"], "");
    model(
        &models,
        "read_a",
        &consumer_sql("a", 1),
        ["cat1", "s", "read_a"],
        "",
    );
    model(
        &models,
        "read_b",
        &consumer_sql("b", 2),
        ["cat1", "s", "read_b"],
        "",
    );

    let result = test(&models, None);
    let both_survive = passed(&result, "read_a") && passed(&result, "read_b");
    let refused = !result.failures.is_empty()
        && !read_a_wrong_value(&result, "read_a")
        && !read_a_wrong_value(&result, "read_b")
        && !passed(&result, "read_a")
        && !passed(&result, "read_b");
    assert!(
        both_survive || refused,
        "both values must survive, or the run must refuse by name: {:?}",
        result.model_results
    );
}

/// Shape 2: an ephemeral model shares a real model's target. E038 refuses the
/// ephemeral model, so `real`'s data is never overwritten by it, and no
/// consumer of `eph` reads anything but a refusal.
#[test]
fn shape2_an_ephemeral_model_on_a_real_target_is_refused_with_e038() {
    let (_root, models) = project();
    model(
        &models,
        "real",
        "SELECT 1 AS v",
        ["", "s", "t"],
        "[strategy]\ntype = \"full_refresh\"\n",
    );
    model(
        &models,
        "eph",
        "SELECT 2 AS v",
        ["", "s", "t"],
        "[strategy]\ntype = \"ephemeral\"\n",
    );
    model(
        &models,
        "read_real",
        &consumer_sql("real", 1),
        ["", "s", "read_real"],
        "",
    );
    model(
        &models,
        "read_eph",
        &consumer_sql("eph", 2),
        ["", "s", "read_eph"],
        "",
    );

    let result = test(&models, None);
    assert!(
        has_error_code(&result, "eph", "E038"),
        "the ephemeral model is refused as E038: {:?}",
        result.diagnostics
    );
    assert!(
        matches!(outcome(&result, "eph"), Some((ModelTestStatus::Fail, _))),
        "the result reports eph as failed: {:?}",
        result.model_results
    );
    assert!(
        !passed(&result, "read_eph"),
        "a consumer of eph gets a refusal, never data: {:?}",
        result.model_results
    );
    assert!(
        !read_a_wrong_value(&result, "read_real"),
        "real's data is never overwritten by eph's: {:?}",
        result.model_results
    );
}

/// Shape 3: a bare read, the same table name in two schemas. `summary` reads
/// the value of the model the compiler bound (`events`, value 2), whichever
/// way the two schemas sort.
#[test]
fn shape3_a_bare_read_binds_the_compiled_model_whatever_the_schema_order() {
    for (events_schema, other_schema) in [("z", "a"), ("a", "z")] {
        let (_root, models) = project();
        model(
            &models,
            "events",
            "SELECT 2 AS v",
            ["", events_schema, "events"],
            "",
        );
        model(
            &models,
            "other",
            "SELECT 1 AS v",
            ["", other_schema, "events"],
            "",
        );
        model(
            &models,
            "summary",
            &consumer_sql("events", 2),
            ["", "marts", "summary"],
            "",
        );

        let result = test(&models, None);
        assert!(
            passed(&result, "summary"),
            "events in schema {events_schema}, other in {other_schema}: summary reads 2: {:?}",
            result.model_results
        );
    }
}

/// Shape 4: the producer's target schema fails validation. `consumer` never
/// runs on the stale seed value 7. And `rocky test --model consumer` still
/// reports the producer's failure when there is one.
#[test]
fn shape4_a_consumer_never_reads_a_stale_seed_after_its_producer_failed() {
    let (root, models) = project();
    seed(root.path(), "CREATE TABLE main.source AS SELECT 7 AS v;\n");
    model(
        &models,
        "source",
        "SELECT 42 AS v",
        ["", "bad-name", "source"],
        "",
    );
    model(
        &models,
        "consumer",
        &consumer_sql("source", 42),
        ["", "main", "consumer"],
        "depends_on = [\"source\"]\n",
    );

    let result = test(&models, None);
    assert!(
        !read_a_wrong_value(&result, "consumer"),
        "consumer must never read the stale seed value 7: {:?}",
        result.model_results
    );
    let producer_failed = !passed(&result, "source");
    if producer_failed {
        assert!(
            !passed(&result, "consumer"),
            "a consumer of a failed producer does not pass: {:?}",
            result.model_results
        );
        let scoped = test(&models, Some("consumer"));
        assert!(
            !scoped.failures.is_empty() && scoped.passed == 0,
            "--model consumer still reports the producer's failure: {:?}",
            scoped.model_results
        );
    } else {
        assert!(
            passed(&result, "consumer"),
            "the producer built, so consumer reads its 42: {:?}",
            result.model_results
        );
    }
}

/// Shape 5: a schema Rocky accepts but DuckDB cannot parse unquoted. The bad
/// schema fails its own model only, never every model in the project.
#[test]
fn shape5_an_unparseable_schema_fails_only_its_own_model() {
    for bad_schema in ["123stage", "select"] {
        let (_root, models) = project();
        model(&models, "bad", "SELECT 1 AS v", ["", bad_schema, "bad"], "");
        model(&models, "good", "SELECT 5 AS v", ["", "stage", "good"], "");
        model(
            &models,
            "read_good",
            &consumer_sql("good", 5),
            ["", "marts", "read_good"],
            "",
        );

        let result = test(&models, None);
        assert!(
            passed(&result, "good") && passed(&result, "read_good"),
            "schema {bad_schema} must not take down the other models: {:?}",
            result.model_results
        );
    }
}

/// Shape 6: a renamed target and a bare read of the model's name. The `D012`
/// text says what `rocky test` does: it materializes each model under its own
/// name. So the bare read reaches the model. When #1354's step 1 changes the
/// binary, the text and this behaviour change together.
#[test]
fn shape6_the_d012_text_describes_what_rocky_test_does() {
    let (_root, models) = project();
    model(&models, "m", "SELECT 3 AS v", ["", "main", "renamed"], "");
    model(
        &models,
        "reader",
        &consumer_sql("m", 3),
        ["", "main", "reader"],
        "",
    );

    let result = test(&models, None);
    let d012 = result
        .diagnostics
        .iter()
        .find(|d| &*d.code == "D012" && d.model == "reader")
        .unwrap_or_else(|| panic!("D012 on reader: {:?}", result.diagnostics));
    assert!(
        d012.message
            .contains("`rocky test` and `rocky ci` materialize each model under its own name"),
        "D012 names the local behaviour: {}",
        d012.message
    );
    assert!(
        passed(&result, "reader"),
        "the behaviour D012 describes: the bare read of m reaches m: {:?}",
        result.model_results
    );
}

/// Shape 7: the default schema spelled in a different case. A bare read of
/// `t`, present in both `main` and `zeta`, resolves the same way whether a
/// model targets `Main` or `main`: the comparison folds the way DuckDB does.
#[test]
fn shape7_the_default_schema_in_another_case_resolves_like_main() {
    let run = |default_schema: &str| {
        let (root, models) = project();
        seed(
            root.path(),
            "CREATE SCHEMA zeta;\n\
             CREATE TABLE main.t AS SELECT 1 AS v;\n\
             CREATE TABLE zeta.t AS SELECT 2 AS v;\n",
        );
        model(
            &models,
            "x",
            "SELECT 10 AS v",
            ["", default_schema, "x"],
            "",
        );
        model(&models, "y", "SELECT 20 AS v", ["", "zeta", "y"], "");
        model(
            &models,
            "reader",
            &consumer_sql("t", 1),
            ["", "marts", "reader"],
            "",
        );
        let result = test(&models, None);
        (
            passed(&result, "reader"),
            outcome(&result, "reader").map(|(status, error)| (status, error.map(str::to_string))),
        )
    };
    let upper = run("Main");
    let lower = run("main");
    assert_eq!(upper, lower, "`Main` and `main` resolve identically");
    assert!(upper.0, "the bare read of t reads main.t (1): {upper:?}");
}
