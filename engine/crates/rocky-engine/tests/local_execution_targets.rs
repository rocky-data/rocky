//! Local execution materializes each model at its configured target (#1354
//! step 1, #2044 decision 1).
//!
//! Like `local_execution_regressions.rs`, each test asserts what a consumer
//! READS in a `rocky test` result, or a named refusal, never where a table
//! landed alone. A consumer's guard column raises a DuckDB `error(...)` that
//! names the value it read when the value is wrong.

use std::path::{Path, PathBuf};

use rocky_engine::test_runner::{ModelTestStatus, TestResult, run_tests};

fn project() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let models = dir.path().join("models");
    std::fs::create_dir_all(&models).expect("mkdir models");
    (dir, models)
}

/// Write model `name` targeting `catalog.schema.table`. `extra` goes into the
/// sidecar before `[target]`.
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

fn consumer_sql(relation: &str, expected: i64) -> String {
    format!(
        "SELECT v, CASE WHEN v = {expected} THEN 1 \
         ELSE error('consumer read ' || CAST(v AS VARCHAR) || ', expected {expected}') END AS ok \
         FROM {relation}"
    )
}

fn seed(root: &Path, sql: &str) {
    std::fs::create_dir_all(root.join("data")).expect("mkdir data");
    std::fs::write(root.join("data/seed.sql"), sql).expect("write seed.sql");
}

fn test(models: &Path, filter: Option<&str>) -> TestResult {
    run_tests(models, None, filter, &rocky_core::run_vars::RunVars::new())
        .expect("rocky test produces a result")
}

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

fn failed_with(result: &TestResult, name: &str, text: &str) -> bool {
    matches!(
        outcome(result, name),
        Some((ModelTestStatus::Fail, Some(error))) if error.contains(text)
    )
}

/// A catalog DuckDB reserves is refused for its own model, by name, with the
/// remedy. A case variant is refused too: DuckDB attaches `Main`, and then
/// every `main.x` read in the session is ambiguous. Other models still run.
#[test]
fn a_reserved_catalog_is_refused_for_its_own_model_only() {
    let (_root, models) = project();
    for (name, catalog) in [
        ("in_main", "main"),
        ("in_memory", "Memory"),
        ("in_system", "system"),
        ("in_temp", "TEMP"),
        ("in_main_upper", "Main"),
    ] {
        model(&models, name, "SELECT 1 AS v", [catalog, "s", name], "");
    }
    model(&models, "good", "SELECT 5 AS v", ["wh", "s", "good"], "");
    model(
        &models,
        "read_good",
        &consumer_sql("good", 5),
        ["wh", "s", "read_good"],
        "",
    );

    let result = test(&models, None);
    for (name, catalog) in [
        ("in_main", "main"),
        ("in_memory", "Memory"),
        ("in_system", "system"),
        ("in_temp", "TEMP"),
        ("in_main_upper", "Main"),
    ] {
        assert!(
            failed_with(
                &result,
                name,
                &format!("target catalog '{catalog}' is a name DuckDB reserves")
            ) && failed_with(&result, name, "Rename the catalog"),
            "{name}: {:?}",
            result.model_results
        );
    }
    assert!(
        passed(&result, "good") && passed(&result, "read_good"),
        "a reserved catalog must not take down other models: {:?}",
        result.model_results
    );
}

/// The seed loader: with one target catalog, that catalog is the default one,
/// as on the DuckDB adapter's database file. A seed that writes without a
/// catalog, or with it, lands where a model's two-part read finds it.
#[test]
fn with_one_catalog_the_seed_and_two_part_reads_resolve_inside_it() {
    let (root, models) = project();
    seed(
        root.path(),
        "CREATE SCHEMA raw;\n\
         CREATE TABLE raw.orders AS SELECT 4 AS v;\n\
         CREATE SCHEMA IF NOT EXISTS poc.landing;\n\
         CREATE TABLE poc.landing.events AS SELECT 6 AS v;\n",
    );
    model(
        &models,
        "stg_orders",
        &consumer_sql("raw.orders", 4),
        ["poc", "staging", "stg_orders"],
        "",
    );
    model(
        &models,
        "stg_events",
        &consumer_sql("landing.events", 6),
        ["poc", "staging", "stg_events"],
        "",
    );
    // A two-part read of another model's target: external to the compiler,
    // so the edge is declared.
    model(
        &models,
        "fct",
        &consumer_sql("staging.stg_orders", 4),
        ["poc", "marts", "fct"],
        "depends_on = [\"stg_orders\"]\n",
    );

    let result = test(&models, None);
    for name in ["stg_orders", "stg_events", "fct"] {
        assert!(passed(&result, name), "{name}: {:?}", result.model_results);
    }
}

/// With two catalogs there is no single default. A two-part read of a table
/// both catalogs hold does not silently pick one: it fails.
#[test]
fn with_two_catalogs_a_two_part_read_does_not_pick_one() {
    let (_root, models) = project();
    model(&models, "a", "SELECT 1 AS v", ["c1", "s", "t"], "");
    model(&models, "b", "SELECT 2 AS v", ["c2", "s", "t"], "");
    model(
        &models,
        "reader",
        &consumer_sql("s.t", 1),
        ["c1", "s", "reader"],
        "depends_on = [\"a\", \"b\"]\n",
    );

    let result = test(&models, None);
    assert!(
        matches!(outcome(&result, "reader"), Some((ModelTestStatus::Fail, Some(e))) if !e.contains("consumer read")),
        "the read fails, and never reads either value: {:?}",
        result.model_results
    );
}

/// A CTE that shares an upstream model's name hides it, as the compiler
/// decided: the model reads its CTE, not the model. `depends_on` puts `a` in
/// the binding, so only the CTE scope keeps the read on the CTE.
#[test]
fn a_cte_named_like_a_model_reads_the_cte() {
    let (_root, models) = project();
    model(&models, "a", "SELECT 1 AS v", ["", "s", "a_target"], "");
    model(
        &models,
        "reader",
        &format!("WITH a AS (SELECT 9 AS v) {}", consumer_sql("a", 9)),
        ["", "s", "reader"],
        "depends_on = [\"a\"]\n",
    );

    let result = test(&models, None);
    assert!(passed(&result, "reader"), "{:?}", result.model_results);
}

/// A consumer of a failed model does not run. Its result names the upstream
/// and the upstream's error, also under `--model` for the consumer alone.
#[test]
fn a_consumer_of_a_failed_model_names_the_upstream_and_its_error() {
    let (_root, models) = project();
    model(
        &models,
        "broken",
        "SELECT 1/0 AS v, error('boom') AS x",
        ["", "s", "broken"],
        "",
    );
    model(
        &models,
        "consumer",
        &consumer_sql("broken", 1),
        ["", "s", "consumer"],
        "",
    );

    for filter in [None, Some("consumer")] {
        let result = test(&models, filter);
        assert!(
            failed_with(
                &result,
                "consumer",
                "not run: upstream model 'broken' failed"
            ) && failed_with(&result, "consumer", "boom"),
            "{filter:?}: {:?}",
            result.model_results
        );
    }
}
