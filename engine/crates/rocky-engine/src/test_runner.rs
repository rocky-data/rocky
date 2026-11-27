//! `rocky test` — local model testing.
//!
//! Compiles models, executes them locally via DuckDB, validates
//! output against contracts, and reports pass/fail.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use rocky_compiler::compile::{CompilerConfig, default_type_mapper};
use rocky_compiler::operand_check::{OperandDialect, OperandTarget, TargetDialects};
use rocky_compiler::source_refs::{SourceProvenance, SourceSchemaOrigin};
use rocky_compiler::types::TypedColumn;
use rocky_core::models::load_unit_tests_from_tree;
use rocky_core::unit_test::{
    MismatchKind, RowMismatch, UnitTestDef, UnitTestDialect, UnitTestDialectStatus, UnitTestResult,
    fixture_to_sql, json_to_sql_literal,
};
use rocky_duckdb::dialect::DuckDbSqlDialect;
use rocky_duckdb::{DuckDbConnector, ExtensionOptions};
use rocky_sql::validation::validate_identifier;
use tracing::info;

/// Per-model outcome of a local test run.
///
/// Surfaces passes alongside failures so consumers (the VS Code Inspector
/// Tests tab, the dagster integration) can render "good_mart: pass" instead
/// of inferring it from `total - failures`. `error` is populated only for
/// `status = "fail"`.
#[derive(Debug, Clone)]
pub struct ModelTestResult {
    pub model: String,
    pub status: ModelTestStatus,
    pub error: Option<String>,
}

/// Outcome of executing one model locally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ModelTestStatus {
    Pass,
    Fail,
}

/// Result of a test run.
#[derive(Debug)]
pub struct TestResult {
    /// Total models tested (post-filter).
    pub total: usize,
    /// Models that passed all checks (post-filter).
    pub passed: usize,
    /// Models that failed (name, reason) (post-filter).
    pub failures: Vec<(String, String)>,
    /// Per-model outcomes (post-filter). Includes passes — the only way to
    /// surface "good_mart: pass" without inferring from total - failures.
    pub model_results: Vec<ModelTestResult>,
    /// Compilation diagnostics.
    pub diagnostics: Vec<rocky_compiler::diagnostic::Diagnostic>,
    /// Every model the compile loaded, **pre-filter** — the only field here
    /// that is not narrowed by `model_filter`.
    ///
    /// Exists so a caller can tell "the filter named a model that does not
    /// exist" from "the filter matched a model with nothing to report": every
    /// other field is post-filter, so both cases otherwise look like `total:
    /// 0`. `rocky test --model <typo>` used to be indistinguishable from
    /// `rocky test --model <a real model with no tests>` for exactly this
    /// reason (#1428).
    pub all_models: Vec<String>,
}

/// The models a test run compiles and executes.
#[derive(Debug)]
pub enum TestModels {
    /// Every model under the run's `models_dir`.
    Dir,
    /// A model set the caller already loaded, such as every transformation
    /// pipeline's models joined into one project graph. The run's
    /// `models_dir` still anchors `functions/` and the compiler config.
    Preloaded(Vec<rocky_core::models::Model>),
}

/// Everything one local test run reads.
pub struct TestRunInputs<'a> {
    /// The models directory. Anchors `functions/` beside it.
    pub models_dir: &'a Path,
    /// The project root. The seed file is `<project_root>/data/seed.sql`.
    pub project_root: &'a Path,
    /// Which models to compile and execute.
    pub models: TestModels,
    /// An explicit contracts directory, if any.
    pub contracts_dir: Option<&'a Path>,
    /// Report only this model (its dependencies still execute).
    pub model_filter: Option<&'a str>,
    /// Per-run `@var(name)` substitutions.
    pub run_vars: &'a rocky_core::run_vars::RunVars,
    /// The project's compile checks, run on the compile result before any
    /// model executes. An error they add fails the run without executing a
    /// model, as a compiler error does.
    ///
    /// The result still holds each model's authored SQL: ephemeral upstreams
    /// are inlined after the checks run, as `rocky compile` orders them.
    /// `rocky ci` and `rocky test` pass the per-model-target checks of
    /// `rocky compile` here; `None` runs none.
    pub gates: Option<&'a CompileGates<'a>>,
    /// Checks that judge the SQL each model executes, run after ephemeral
    /// upstreams are inlined (`rocky compile`'s `E054` on SQL Server). An
    /// error they add fails the run without executing a model. `None` runs
    /// none.
    pub inlined_gates: Option<&'a CompileGates<'a>>,
    /// Refuse a contract column whose declared type Rocky cannot check
    /// (`E059` in place of the `I003` note). `rocky ci` sets it from
    /// `--strict-contracts` or `[contracts] strict`.
    pub strict_contracts: bool,
    /// The warehouse each model runs on in production, which types a `CAST`
    /// whose width differs between warehouses (#2333). The models still
    /// execute on DuckDB here. The default knows none.
    pub target_dialects: rocky_compiler::operand_check::TargetDialects,
    /// The project's DuckDB extension settings, for the in-memory database
    /// the models run in. `offline` and `extension_directory` reach it here;
    /// the default is DuckDB's own behaviour.
    pub extensions: ExtensionOptions,
}

/// Checks a caller runs over a test run's compile result. See
/// [`TestRunInputs::gates`].
pub type CompileGates<'a> = dyn Fn(&mut rocky_compiler::compile::CompileResult) + 'a;

impl std::fmt::Debug for TestRunInputs<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestRunInputs")
            .field("models_dir", &self.models_dir)
            .field("project_root", &self.project_root)
            .field("models", &self.models)
            .field("contracts_dir", &self.contracts_dir)
            .field("model_filter", &self.model_filter)
            .field("run_vars", &self.run_vars)
            .field("gates", &self.gates.is_some())
            .field("inlined_gates", &self.inlined_gates.is_some())
            .finish()
    }
}

/// The seed file of a project: `<project_root>/data/seed.sql`.
pub fn seed_path(project_root: &Path) -> PathBuf {
    project_root.join("data").join("seed.sql")
}

/// Read the column schema of every user table in `db`, keyed
/// `"<schema>.<table>"`.
///
/// The key is the shape the SQL lineage extractor produces from a model's
/// `FROM <schema>.<table>` clause, so the compiler lands the types on the
/// source a model reads. Called on a database a seed file was just run in,
/// so a compile is typed from the same tables the models execute on.
///
/// # Errors
///
/// Returns an error when the `information_schema` query fails.
pub fn source_schemas_from_db(
    db: &DuckDbConnector,
) -> anyhow::Result<HashMap<String, Vec<TypedColumn>>> {
    // One round-trip pulls every (schema, table, column, type, nullable)
    // tuple. Filtering out DuckDB's internal schemas keeps the map scoped to
    // user-created tables.
    let info_sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable \
                    FROM information_schema.columns \
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
                    ORDER BY table_schema, table_name, ordinal_position";
    let result = db
        .execute_sql(info_sql)
        .map_err(|e| anyhow::anyhow!("information_schema query failed: {e}"))?;

    let mut by_table: HashMap<String, Vec<TypedColumn>> = HashMap::new();
    for row in &result.rows {
        let schema = row[0].as_str().unwrap_or_default();
        let table = row[1].as_str().unwrap_or_default();
        let column = row[2].as_str().unwrap_or_default();
        let data_type = row[3].as_str().unwrap_or_default();
        let nullable = row[4]
            .as_str()
            .is_none_or(|s| s.eq_ignore_ascii_case("yes") || s == "true" || s == "1");
        if schema.is_empty() || table.is_empty() || column.is_empty() {
            continue;
        }
        by_table
            .entry(format!("{schema}.{table}"))
            .or_default()
            .push(TypedColumn {
                name: column.to_string(),
                data_type: default_type_mapper(data_type),
                nullable,
            });
    }
    // DuckDB resolves a bare name (`FROM orders`) to its default schema,
    // `main`, so a model that reads a seed table without a schema prefix is
    // typed from it too (#2657). A table only in another schema gets no bare
    // key: DuckDB would not resolve the bare name to it.
    let bare: Vec<(String, Vec<TypedColumn>)> = by_table
        .iter()
        .filter_map(|(key, columns)| {
            let table = key.strip_prefix(DUCKDB_DEFAULT_SCHEMA)?;
            Some((table.to_string(), columns.clone()))
        })
        .collect();
    for (table, columns) in bare {
        by_table.entry(table).or_insert(columns);
    }
    Ok(by_table)
}

/// The names, as [`rocky_compiler::function_check::seed_name_key`], of the
/// functions and macros a seed created in `db` (#2691).
///
/// Read from DuckDB's own catalog (`duckdb_functions()`, the entries that are
/// not internal), so a macro the seed's text hides from a scan, or brings in
/// with `ATTACH` or `IMPORT DATABASE`, is found too. Built-ins and the
/// functions of loaded extensions are internal.
///
/// # Errors
///
/// Returns an error when the catalog query fails.
pub fn seed_function_names_from_db(
    db: &DuckDbConnector,
) -> anyhow::Result<std::collections::HashSet<String>> {
    // `system.main.` so a seed macro cannot shadow the catalog function
    // (#2688); the names are folded in Rust, not with a SQL `lower` a seed
    // macro could shadow.
    let result = db
        .execute_sql("SELECT function_name FROM system.main.duckdb_functions() WHERE NOT internal")
        .map_err(|e| anyhow::anyhow!("duckdb_functions() query failed: {e}"))?;
    Ok(result
        .rows
        .iter()
        .filter_map(|row| row.first()?.as_str())
        .map(rocky_compiler::function_check::seed_name_key)
        .collect())
}

/// The prefix of a key in DuckDB's default schema, which a bare table name
/// resolves to.
const DUCKDB_DEFAULT_SCHEMA: &str = "main.";

/// Run tests on a project.
///
/// 1. Run the project's seed file, when it has one, in a fresh in-memory
///    DuckDB, and read the schemas of the tables it made
/// 2. Compile the models, typed from those schemas, so a contract type
///    mismatch or a missing source column is found before execution
/// 3. Execute each model in that same database (dependencies always run,
///    even when `model_filter` is set, so the filtered model's SQL can
///    resolve)
/// 4. Report results (filtered to `model_filter` when set)
///
/// `run_vars` supplies per-run `@var(name)` substitutions so a required-var
/// model compiles under `rocky test --var name=value`; pass
/// [`rocky_core::run_vars::RunVars::new`] when the caller has none.
pub fn run_tests_with(inputs: TestRunInputs<'_>) -> anyhow::Result<TestResult> {
    let TestRunInputs {
        models_dir,
        project_root,
        models,
        contracts_dir,
        model_filter,
        run_vars,
        gates,
        inlined_gates,
        strict_contracts,
        target_dialects,
        extensions,
    } = inputs;

    // The seed runs before the compile, so the compile is typed from the
    // tables the models then execute on. A seed that fails is reported after
    // any compile error, as it was when the seed ran second.
    let db = DuckDbConnector::in_memory_with(&extensions)?;
    let seed_file = seed_path(project_root);
    let seeded = seed_file.exists();
    let seed_error: Option<String> = if seeded {
        // A seed that is there but does not read refuses the run, naming the
        // file: its tables and macros are unknown (#2753).
        let seed_sql = std::fs::read_to_string(&seed_file).map_err(|e| {
            anyhow::anyhow!("failed to read the seed file {}: {e}", seed_file.display())
        })?;
        match db.execute_statement(&seed_sql) {
            Ok(()) => {
                info!(path = %seed_file.display(), "loaded seed data");
                None
            }
            Err(e) => Some(format!("failed to load data/seed.sql: {e}")),
        }
    } else {
        None
    };
    // A failure to read the seeded schemas is reported like a seed that
    // failed, not as an error of the whole run.
    let (source_schemas, seed_function_names, seed_error) = match (seeded, seed_error) {
        (true, None) => match source_schemas_from_db(&db)
            .and_then(|schemas| Ok((schemas, seed_function_names_from_db(&db)?)))
        {
            Ok((schemas, names)) => (schemas, names, None),
            Err(e) => (
                HashMap::new(),
                std::collections::HashSet::new(),
                Some(format!("failed to read the seeded tables: {e:#}")),
            ),
        },
        (_, error) => (HashMap::new(), std::collections::HashSet::new(), error),
    };
    let source_provenance =
        SourceProvenance::uniform(source_schemas.keys(), &SourceSchemaOrigin::Seed);

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: contracts_dir.map(std::path::Path::to_path_buf),
        source_schemas,
        source_provenance,
        run_vars: run_vars.clone(),
        // The checks in `gates` judge the authored SQL; ephemeral upstreams
        // are inlined after them, below.
        preserve_authored_sql: true,
        strict_contracts,
        target_dialects,
        ..Default::default()
    };

    let compiled = match models {
        TestModels::Dir => rocky_compiler::compile::compile(&config),
        TestModels::Preloaded(models) => {
            rocky_compiler::compile::compile_preloaded_models(models, &config)
        }
    };
    let mut compile_result = match compiled {
        Ok(result) => result,
        // A dependency cycle leaves no execution order: report its E058
        // diagnostics as compile errors, and execute nothing.
        Err(error) => match error.cycle_diagnostics() {
            Some(diagnostics) => {
                let all_models = error.cycle_models().unwrap_or_default().to_vec();
                return Ok(cycle_result(diagnostics, all_models, model_filter));
            }
            None => return Err(error.into()),
        },
    };
    // The seed that typed the compile created these; the gates read them
    // from the result (E057 / E068 skip them on DuckDB).
    compile_result.seed_function_names = seed_function_names;
    if let Some(gates) = gates {
        gates(&mut compile_result);
        compile_result.has_errors |= compile_result
            .diagnostics
            .iter()
            .any(rocky_compiler::diagnostic::Diagnostic::is_error);
    }
    // The statement each model executes inlines its ephemeral upstreams. The
    // E038 diagnostics this returns were already reported by the compile.
    let _already_reported =
        rocky_compiler::ephemeral::apply_ephemerals(&mut compile_result.project, true);
    if let Some(gates) = inlined_gates {
        gates(&mut compile_result);
        compile_result.has_errors |= compile_result
            .diagnostics
            .iter()
            .any(rocky_compiler::diagnostic::Diagnostic::is_error);
    }

    let mut result = TestResult {
        total: 0,
        passed: 0,
        failures: Vec::new(),
        model_results: Vec::new(),
        diagnostics: compile_result.diagnostics.clone(),
        // Captured before any filtering, and before the `has_errors` early
        // return below, so an unknown `--model` is still detectable on a
        // project that failed to compile.
        all_models: compile_result
            .project
            .models
            .iter()
            .map(|m| m.config.name.clone())
            .collect(),
    };

    // Check for compilation errors. A consumer record problem (`E060`) is not
    // a model failure: it stays in `diagnostics` (so `rocky test` and
    // `rocky ci` still fail on it) but never becomes a `model_results` entry
    // and never stops the model tests from running.
    if rocky_compiler::consumers::has_model_errors(&compile_result) {
        for d in &compile_result.diagnostics {
            if d.is_error()
                && !rocky_compiler::consumers::is_consumer_diagnostic(d)
                && include_model(model_filter, &d.model)
            {
                result
                    .failures
                    .push((d.model.clone(), d.message.to_string()));
                result.model_results.push(ModelTestResult {
                    model: d.model.clone(),
                    status: ModelTestStatus::Fail,
                    error: Some(d.message.to_string()),
                });
            }
        }
        result.total = result.model_results.len();
        return Ok(result);
    }

    // A seed that failed leaves nothing for the models to read.
    if let Some(error) = seed_error {
        result.failures.push(("seed".to_string(), error.clone()));
        result.model_results.push(ModelTestResult {
            model: "seed".to_string(),
            status: ModelTestStatus::Fail,
            error: Some(error),
        });
        result.total = result.model_results.len();
        return Ok(result);
    }

    // Always execute every model so a filtered model's upstream dependencies
    // resolve. The filter is applied below when assembling the reported
    // results, so passes/failures match the requested scope.
    let exec_result = crate::executor::execute_locally(&compile_result, &db);

    for name in &exec_result.succeeded {
        if include_model(model_filter, name) {
            result.model_results.push(ModelTestResult {
                model: name.clone(),
                status: ModelTestStatus::Pass,
                error: None,
            });
        }
    }
    for (name, err) in &exec_result.failed {
        if include_model(model_filter, name) {
            result.failures.push((name.clone(), err.clone()));
            result.model_results.push(ModelTestResult {
                model: name.clone(),
                status: ModelTestStatus::Fail,
                error: Some(err.clone()),
            });
        }
    }
    result.total = result.model_results.len();
    result.passed = result
        .model_results
        .iter()
        .filter(|m| m.status == ModelTestStatus::Pass)
        .count();

    info!(
        total = result.total,
        passed = result.passed,
        failed = result.failures.len(),
        filter = model_filter.unwrap_or("<none>"),
        "test run complete"
    );

    Ok(result)
}

/// The result of a run refused by a dependency cycle: every E058 diagnostic,
/// and a failure for each model on the cycle (filtered to `model_filter`).
fn cycle_result(
    diagnostics: &[rocky_compiler::diagnostic::Diagnostic],
    all_models: Vec<String>,
    model_filter: Option<&str>,
) -> TestResult {
    let mut result = TestResult {
        total: 0,
        passed: 0,
        failures: Vec::new(),
        model_results: Vec::new(),
        diagnostics: diagnostics.to_vec(),
        all_models,
    };
    for d in diagnostics {
        if include_model(model_filter, &d.model) {
            result
                .failures
                .push((d.model.clone(), d.message.to_string()));
            result.model_results.push(ModelTestResult {
                model: d.model.clone(),
                status: ModelTestStatus::Fail,
                error: Some(d.message.to_string()),
            });
        }
    }
    result.total = result.model_results.len();
    result
}

/// Filter helper: include a model when there's no filter, or when the filter
/// matches the model name exactly. Centralized so callers can't accidentally
/// substring-match.
fn include_model(filter: Option<&str>, model: &str) -> bool {
    match filter {
        None => true,
        Some(target) => target == model,
    }
}

/// Outcome of running all fixture-driven unit tests (`[[test]]` blocks) in a
/// project. Each [`UnitTestResult`] records one test's pass/fail against its
/// mock-input → expected-output expectation.
#[derive(Debug)]
pub struct UnitTestRun {
    pub results: Vec<UnitTestResult>,
}

impl UnitTestRun {
    /// Total unit tests executed.
    pub fn total(&self) -> usize {
        self.results.len()
    }

    /// Unit tests that passed.
    pub fn passed(&self) -> usize {
        self.results.iter().filter(|r| r.passed).count()
    }
}

/// Run every fixture-driven unit test (`[[test]]` blocks) declared in the
/// project's model sidecars.
///
/// For each test a fresh in-memory DuckDB is seeded with the `given` fixtures,
/// the model's compiled SQL is materialized against them, and the output is
/// compared to `expect` — a multiset comparison by default, positional when
/// `expect.ordered` is set. Models are filtered by `model_filter` when set;
/// models declaring no `[[test]]` blocks are skipped entirely (no compile).
///
/// `targets` are the warehouses each model runs on in production (the
/// project resolver, [`rocky_compiler::model_targets::target_dialects`]).
/// They do not change the compile or the SQL. They only decide the
/// [`UnitTestDialect`] notice on each result: a model whose only target is
/// DuckDB gets none, every other model's tests carry one, and with
/// `strict_dialect` those tests are refused instead of run.
pub fn run_unit_tests(
    models_dir: &Path,
    model_filter: Option<&str>,
    extensions: &ExtensionOptions,
    targets: &TargetDialects,
    strict_dialect: bool,
) -> anyhow::Result<UnitTestRun> {
    let unit_tests = load_unit_tests_from_tree(models_dir)?;
    if unit_tests.is_empty() {
        return Ok(UnitTestRun {
            results: Vec::new(),
        });
    }

    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        ..Default::default()
    };
    let compiled = rocky_compiler::compile::compile(&config);

    // Stable, name-sorted iteration so output order is deterministic.
    let mut names: Vec<&String> = unit_tests.keys().collect();
    names.sort();

    // A dependency cycle fails every unit test. [`run_tests_with`] reports
    // the cycle itself, as E058 diagnostics.
    let compile_result = match compiled {
        Ok(result) => result,
        Err(error) if error.cycle_diagnostics().is_some() => {
            let results = names
                .into_iter()
                .filter(|name| include_model(model_filter, name))
                .flat_map(|name| {
                    let dialect = not_run_notice(targets.for_model(name));
                    unit_tests[name].iter().map(move |test| UnitTestResult {
                        model: name.clone(),
                        test: test.name.clone(),
                        passed: false,
                        error: Some(format!(
                            "model '{name}' is not run: the project has a dependency cycle \
                             (E058)"
                        )),
                        mismatches: Vec::new(),
                        dialect: dialect.clone(),
                    })
                })
                .collect();
            return Ok(UnitTestRun { results });
        }
        Err(error) => return Err(error.into()),
    };

    let mut results = Vec::new();
    for name in names {
        if !include_model(model_filter, name) {
            continue;
        }
        let plan = plan_model_unit_tests(
            name,
            &compile_result,
            targets.for_model(name),
            strict_dialect,
        );
        for test in &unit_tests[name] {
            match &plan {
                ModelUnitTestPlan::Run { sql, dialect } => {
                    let mut reached_model = false;
                    let mut result = run_one_unit_test(
                        name,
                        sql,
                        test,
                        &compile_result,
                        extensions,
                        &mut reached_model,
                    );
                    // A test that failed before its model's SQL reached
                    // DuckDB (a fixture, a function, the database) did not
                    // run on DuckDB: say so.
                    result.dialect = dialect.clone().map(|mut d| {
                        d.ran_on_duckdb = reached_model;
                        d
                    });
                    results.push(result);
                }
                ModelUnitTestPlan::Refuse { error, dialect } => results.push(UnitTestResult {
                    model: name.clone(),
                    test: test.name.clone(),
                    passed: false,
                    error: Some(error.clone()),
                    mismatches: Vec::new(),
                    dialect: Some(dialect.clone()),
                }),
                ModelUnitTestPlan::NotCompiled { dialect } => results.push(UnitTestResult {
                    model: name.clone(),
                    test: test.name.clone(),
                    passed: false,
                    error: Some(format!(
                        "model '{name}' did not compile — cannot run its unit tests"
                    )),
                    mismatches: Vec::new(),
                    dialect: dialect.clone(),
                }),
            }
        }
    }

    info!(
        total = results.len(),
        passed = results.iter().filter(|r| r.passed).count(),
        "unit test run complete"
    );
    Ok(UnitTestRun { results })
}

/// What [`run_unit_tests`] does with one model's unit tests.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ModelUnitTestPlan {
    /// Run each test on DuckDB with `sql`. `dialect` is the notice each
    /// result carries: `None` for a model whose only target is DuckDB.
    Run {
        sql: String,
        dialect: Option<UnitTestDialect>,
    },
    /// `--strict-dialect` refuses each test without running it.
    Refuse {
        error: String,
        dialect: UnitTestDialect,
    },
    /// The model did not compile, so no test runs.
    NotCompiled { dialect: Option<UnitTestDialect> },
}

/// Decide what happens to the unit tests of `model`: the SQL they run on
/// DuckDB and the notice they carry, or the refusal. The SQL does not depend
/// on the target. The target only adds a notice or a refusal.
fn plan_model_unit_tests(
    model: &str,
    compile_result: &rocky_compiler::compile::CompileResult,
    target: &OperandTarget,
    strict_dialect: bool,
) -> ModelUnitTestPlan {
    let Some(ir) = compile_result.project.model(model) else {
        return ModelUnitTestPlan::NotCompiled {
            dialect: not_run_notice(target),
        };
    };
    match target_notice(target) {
        Some((status, targets)) if strict_dialect => ModelUnitTestPlan::Refuse {
            error: strict_dialect_refusal(status, &targets),
            dialect: UnitTestDialect {
                status,
                targets,
                ran_on_duckdb: false,
                refused: true,
            },
        },
        notice => ModelUnitTestPlan::Run {
            sql: rocky_core::sql_gen::local_test_sql(ir).into_owned(),
            dialect: notice.map(|(status, targets)| UnitTestDialect {
                status,
                targets,
                ran_on_duckdb: true,
                refused: false,
            }),
        },
    }
}

/// The notice for a test that did not run for a reason other than
/// `--strict-dialect`: a dependency cycle or a compile failure.
fn not_run_notice(target: &OperandTarget) -> Option<UnitTestDialect> {
    target_notice(target).map(|(status, targets)| UnitTestDialect {
        status,
        targets,
        ran_on_duckdb: false,
        refused: false,
    })
}

/// How `target` relates to DuckDB, the dialect unit tests run on. `None`
/// when DuckDB is the model's only target.
///
/// ```text
///   Unconfigured                     -> unresolved_target, []
///   Targets [DuckDB]                 -> None (no notice)
///   one target that is not DuckDB    -> non_duckdb_target, [it]
///   more than one target, any mix    -> multiple_targets, [all of them]
/// ```
///
/// An adapter with no dialect table (`unruled`, such as ClickHouse) is still
/// a named target that is not DuckDB.
pub fn target_notice(target: &OperandTarget) -> Option<(UnitTestDialectStatus, Vec<String>)> {
    let (dialects, unruled) = match target {
        OperandTarget::Unconfigured => {
            return Some((UnitTestDialectStatus::UnresolvedTarget, Vec::new()));
        }
        OperandTarget::Targets { dialects, unruled } => (dialects, unruled),
    };
    if dialects.as_slice() == [OperandDialect::DuckDb] && unruled.is_empty() {
        return None;
    }
    let names: Vec<String> = dialects
        .iter()
        .map(|d| d.name().to_string())
        .chain(unruled.iter().cloned())
        .collect();
    let status = match names.len() {
        0 => UnitTestDialectStatus::UnresolvedTarget,
        1 => UnitTestDialectStatus::NonDuckdbTarget,
        _ => UnitTestDialectStatus::MultipleTargets,
    };
    Some((status, names))
}

/// The error a `--strict-dialect` refusal reports.
fn strict_dialect_refusal(status: UnitTestDialectStatus, targets: &[String]) -> String {
    let why = match status {
        UnitTestDialectStatus::NonDuckdbTarget => {
            format!("its model targets {}", targets.join(", "))
        }
        UnitTestDialectStatus::MultipleTargets => format!(
            "its model targets more than one warehouse ({})",
            targets.join(", ")
        ),
        UnitTestDialectStatus::UnresolvedTarget => {
            "no target warehouse is resolved for its model".to_string()
        }
    };
    format!(
        "not run: --strict-dialect refuses this unit test, because it would run on DuckDB and {why}"
    )
}

/// Execute one unit test: seed `given` fixtures, materialize the model against
/// them, and compare the output to `expect`.
fn run_one_unit_test(
    model: &str,
    compiled_sql: &str,
    test: &UnitTestDef,
    compile_result: &rocky_compiler::compile::CompileResult,
    extensions: &ExtensionOptions,
    reached_model: &mut bool,
) -> UnitTestResult {
    let fail = |msg: String, mismatches: Vec<RowMismatch>| UnitTestResult {
        model: model.to_string(),
        test: test.name.clone(),
        passed: false,
        error: Some(msg),
        mismatches,
        dialect: None,
    };
    let pass = || UnitTestResult {
        model: model.to_string(),
        test: test.name.clone(),
        passed: true,
        error: None,
        mismatches: Vec::new(),
        dialect: None,
    };

    let db = match DuckDbConnector::in_memory_with(extensions) {
        Ok(d) => d,
        Err(e) => return fail(format!("DuckDB init failed: {e}"), Vec::new()),
    };

    // User-defined functions the model may call.
    if let Some((function, e)) = crate::executor::create_local_functions(compile_result, &db)
        .into_iter()
        .next()
    {
        return fail(
            format!("failed to create function '{function}': {e}"),
            Vec::new(),
        );
    }

    // Seed the mock input fixtures.
    for fx in &test.given {
        if validate_identifier(&fx.model_ref).is_err() {
            return fail(
                format!("invalid fixture ref '{}'", fx.model_ref),
                Vec::new(),
            );
        }
        if let Some(sql) = fixture_to_sql(fx, &DuckDbSqlDialect::plain())
            && let Err(e) = db.execute_statement(&sql)
        {
            return fail(
                format!("failed to seed fixture '{}': {e}", fx.model_ref),
                Vec::new(),
            );
        }
    }

    // Materialize the model output against the mocked inputs. From here on
    // the model's SQL has been sent to DuckDB.
    let sql = compiled_sql.trim().trim_end_matches(';');
    *reached_model = true;
    if let Err(e) =
        db.execute_statement(&format!("CREATE OR REPLACE TABLE __rocky_actual AS\n{sql}"))
    {
        return fail(format!("model execution failed: {e}"), Vec::new());
    }

    // Empty expectation ⇒ the model must produce no rows.
    if test.expect.rows.is_empty() {
        return match query_scalar_u64(&db, "SELECT COUNT(*) AS n FROM __rocky_actual") {
            Ok(0) => pass(),
            Ok(n) => fail(format!("expected 0 rows, got {n}"), Vec::new()),
            Err(e) => fail(format!("row count query failed: {e}"), Vec::new()),
        };
    }

    // Comparison columns come from the expected rows — only the columns the
    // author asserts on are compared (extra model columns are ignored).
    let cols: Vec<String> = match test.expect.rows[0].as_object() {
        Some(obj) => obj.keys().cloned().collect(),
        None => {
            return fail(
                "expect rows must be tables (key = value)".into(),
                Vec::new(),
            );
        }
    };
    for c in &cols {
        if validate_identifier(c).is_err() {
            return fail(format!("invalid column name in expect: '{c}'"), Vec::new());
        }
    }
    if let Err(e) = db.execute_statement(&expected_table_sql(&test.expect.rows, &cols)) {
        return fail(format!("failed to build expected table: {e}"), Vec::new());
    }
    let col_list = cols.join(", ");

    if test.expect.ordered {
        // Positional comparison: actual in the model's output order (re-run as a
        // subquery so its ORDER BY survives), expected by declaration order.
        let actual =
            match db.execute_sql(&format!("SELECT {col_list} FROM (\n{sql}\n) AS __rocky_m")) {
                Ok(r) => r.rows,
                Err(e) => return fail(format!("actual query failed: {e}"), Vec::new()),
            };
        let expected = match db.execute_sql(&format!(
            "SELECT {col_list} FROM __rocky_expected ORDER BY __rocky_ord"
        )) {
            Ok(r) => r.rows,
            Err(e) => return fail(format!("expected query failed: {e}"), Vec::new()),
        };
        if actual == expected {
            return pass();
        }
        let mismatches = ordered_mismatches(&cols, &expected, &actual);
        return fail(
            format!(
                "ordered output mismatch ({} expected vs {} actual row(s))",
                expected.len(),
                actual.len()
            ),
            mismatches,
        );
    }

    // Default: multiset comparison via EXCEPT ALL both directions.
    let missing = match query_scalar_u64(
        &db,
        &format!(
            "SELECT COUNT(*) AS n FROM (SELECT {col_list} FROM __rocky_expected \
             EXCEPT ALL SELECT {col_list} FROM __rocky_actual)"
        ),
    ) {
        Ok(n) => n,
        Err(e) => return fail(format!("diff query failed: {e}"), Vec::new()),
    };
    let extra = match query_scalar_u64(
        &db,
        &format!(
            "SELECT COUNT(*) AS n FROM (SELECT {col_list} FROM __rocky_actual \
             EXCEPT ALL SELECT {col_list} FROM __rocky_expected)"
        ),
    ) {
        Ok(n) => n,
        Err(e) => return fail(format!("diff query failed: {e}"), Vec::new()),
    };

    if missing == 0 && extra == 0 {
        return pass();
    }
    let mismatches = unordered_mismatches(&db, &cols, &col_list);
    fail(
        format!("output mismatch: {missing} expected row(s) missing, {extra} unexpected row(s)"),
        mismatches,
    )
}

/// Build a `CREATE TABLE __rocky_expected` statement from the expected rows,
/// carrying a `__rocky_ord` ordinal so ordered comparison can recover the
/// declaration order. Values are rendered identically to the `given` fixtures.
fn expected_table_sql(rows: &[serde_json::Value], cols: &[String]) -> String {
    let selects: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, row)| {
            let obj = row.as_object();
            let vals: Vec<String> = cols
                .iter()
                .map(|c| {
                    let lit = obj.and_then(|o| o.get(c)).map_or_else(
                        || "NULL".to_string(),
                        |v| json_to_sql_literal(v, &DuckDbSqlDialect::plain()),
                    );
                    format!("{lit} AS {c}")
                })
                .collect();
            format!("SELECT {i} AS __rocky_ord, {}", vals.join(", "))
        })
        .collect();
    format!(
        "CREATE OR REPLACE TABLE __rocky_expected AS\n{}",
        selects.join("\nUNION ALL\n")
    )
}

/// Run a single-cell `COUNT(*)`-style query and parse the result as `u64`.
fn query_scalar_u64(db: &DuckDbConnector, sql: &str) -> Result<u64, String> {
    let r = db.execute_sql(sql).map_err(|e| e.to_string())?;
    let cell = r
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| "query returned no rows".to_string())?;
    // `execute_sql` returns every cell stringified, so a `COUNT(*)` comes back
    // as e.g. `"0"`; accept both the string and native-number forms.
    let text = match cell {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    };
    text.trim()
        .parse::<u64>()
        .map_err(|e| format!("expected an integer count, got '{text}': {e}"))
}

/// Render a single result cell for diagnostics (strings unquoted, null as NULL).
fn value_to_display(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "NULL".to_string(),
        other => other.to_string(),
    }
}

/// Render a result row as `col=val, col=val` for diagnostics.
fn render_row(cols: &[String], row: &[serde_json::Value]) -> String {
    cols.iter()
        .zip(row.iter())
        .map(|(c, v)| format!("{c}={}", value_to_display(v)))
        .collect::<Vec<_>>()
        .join(", ")
}

/// Sample up to 10 missing + 10 unexpected rows for an unordered mismatch.
fn unordered_mismatches(db: &DuckDbConnector, cols: &[String], col_list: &str) -> Vec<RowMismatch> {
    let mut out = Vec::new();
    if let Ok(r) = db.execute_sql(&format!(
        "SELECT * FROM (SELECT {col_list} FROM __rocky_expected \
         EXCEPT ALL SELECT {col_list} FROM __rocky_actual) AS __d LIMIT 10"
    )) {
        for row in &r.rows {
            let row_index = out.len();
            out.push(RowMismatch {
                row_index,
                expected: render_row(cols, row),
                actual: None,
                kind: MismatchKind::Missing,
            });
        }
    }
    if let Ok(r) = db.execute_sql(&format!(
        "SELECT * FROM (SELECT {col_list} FROM __rocky_actual \
         EXCEPT ALL SELECT {col_list} FROM __rocky_expected) AS __d LIMIT 10"
    )) {
        for row in &r.rows {
            let row_index = out.len();
            out.push(RowMismatch {
                row_index,
                expected: String::new(),
                actual: Some(render_row(cols, row)),
                kind: MismatchKind::Extra,
            });
        }
    }
    out
}

/// Build positional mismatches for an ordered comparison (up to 10).
fn ordered_mismatches(
    cols: &[String],
    expected: &[Vec<serde_json::Value>],
    actual: &[Vec<serde_json::Value>],
) -> Vec<RowMismatch> {
    let mut out = Vec::new();
    for i in 0..expected.len().max(actual.len()) {
        match (expected.get(i), actual.get(i)) {
            (Some(e), Some(a)) if e == a => {}
            (Some(e), Some(a)) => out.push(RowMismatch {
                row_index: i,
                expected: render_row(cols, e),
                actual: Some(render_row(cols, a)),
                kind: MismatchKind::ValueDiff,
            }),
            (Some(e), None) => out.push(RowMismatch {
                row_index: i,
                expected: render_row(cols, e),
                actual: None,
                kind: MismatchKind::Missing,
            }),
            (None, Some(a)) => out.push(RowMismatch {
                row_index: i,
                expected: String::new(),
                actual: Some(render_row(cols, a)),
                kind: MismatchKind::Extra,
            }),
            (None, None) => {}
        }
        if out.len() >= 10 {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The seed's function and macro names come from DuckDB's catalog, so a
    /// comment marker or a `CREATE MACRO` inside a string literal neither
    /// hides a real one nor makes a false one, and a seed table macro named
    /// `duckdb_functions` does not shadow the catalog query (#2691).
    #[test]
    fn seed_function_names_are_read_from_duckdb() {
        let db = DuckDbConnector::in_memory().expect("db");
        db.execute_statement(
            "CREATE TABLE paths AS SELECT 'raw/orders/*.csv' AS p, 'a--b' AS q; \
             CREATE MACRO lower(a, b) AS a || b; \
             SELECT 'raw/events/*/part.parquet', 'CREATE MACRO fake(x) AS x'; \
             CREATE OR REPLACE TEMP MACRO My_Macro(x) AS x + 1; \
             CREATE SCHEMA util; CREATE FUNCTION util.\"Shout\"(s) AS upper(s); \
             CREATE MACRO rows_of() AS TABLE SELECT 1 AS one; \
             CREATE MACRO duckdb_functions() AS TABLE SELECT 'x' AS function_name; \
             -- CREATE MACRO commented(x) AS x\n\
             /* CREATE MACRO also_commented(x) AS x */ SELECT 1",
        )
        .expect("seed");
        let mut names: Vec<String> = seed_function_names_from_db(&db)
            .expect("names")
            .into_iter()
            .collect();
        names.sort_unstable();
        assert_eq!(
            names,
            ["duckdb_functions", "lower", "my_macro", "rows_of", "shout"]
        );
        let empty = DuckDbConnector::in_memory().expect("db");
        assert!(
            seed_function_names_from_db(&empty)
                .expect("names")
                .is_empty(),
            "a seed with no macros creates no names"
        );
    }

    /// #2657: a table in DuckDB's default schema is also keyed by its bare
    /// name, as DuckDB resolves `FROM orders`; one only in another schema is not.
    #[test]
    fn a_default_schema_table_is_also_keyed_by_its_bare_name() {
        let db = DuckDbConnector::in_memory().expect("db");
        db.execute_statement(
            "CREATE TABLE orders (id INTEGER NOT NULL); \
             CREATE SCHEMA raw; CREATE TABLE raw.orders (other VARCHAR); \
             CREATE TABLE raw.events (id INTEGER)",
        )
        .expect("seed");
        let schemas = source_schemas_from_db(&db).expect("schemas");
        let bare = schemas.get("orders").expect("bare key for main.orders");
        assert_eq!(
            bare[0].name, "id",
            "the bare name is main's table: {bare:?}"
        );
        assert!(!bare[0].nullable);
        assert!(schemas.contains_key("main.orders"));
        assert!(schemas.contains_key("raw.orders"));
        assert!(
            !schemas.contains_key("events"),
            "raw.events gets no bare key"
        );
    }

    /// Test helper: [`run_tests_with`] on `models_dir` with default extension
    /// options. Lives here so production code cannot reach DuckDB's autoinstall.
    fn run_tests(
        models_dir: &Path,
        contracts_dir: Option<&Path>,
        model_filter: Option<&str>,
        run_vars: &rocky_core::run_vars::RunVars,
    ) -> anyhow::Result<TestResult> {
        run_tests_with(TestRunInputs {
            models_dir,
            project_root: models_dir.parent().unwrap_or_else(|| Path::new(".")),
            models: TestModels::Dir,
            contracts_dir,
            model_filter,
            run_vars,
            gates: None,
            inlined_gates: None,
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
    }

    /// Set up a temp project with two SQL models (`raw_orders`, `good_mart`)
    /// where `good_mart` reads from `raw_orders`. Returns the temp dir guard
    /// so the caller can drop it.
    fn scaffold_two_model_project() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(
            models.join("raw_orders.sql"),
            "SELECT 1 AS id, 'a' AS status",
        )
        .unwrap();
        std::fs::write(
            models.join("raw_orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        std::fs::write(
            models.join("good_mart.sql"),
            "SELECT id, status FROM raw_orders",
        )
        .unwrap();
        std::fs::write(
            models.join("good_mart.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        (dir, models)
    }

    /// A model that calls a user-defined function (`functions/`) runs under
    /// `rocky test`: the function is created as a macro first.
    #[test]
    fn model_calling_a_udf_executes_locally() {
        let (tmp, models) = scaffold_two_model_project();
        let functions = tmp.path().join("functions");
        std::fs::create_dir_all(&functions).unwrap();
        std::fs::write(
            functions.join("double_it.toml"),
            "returns = \"BIGINT\"\n[[arguments]]\nname = \"x\"\ntype = \"BIGINT\"\n",
        )
        .unwrap();
        std::fs::write(functions.join("double_it.sql"), "x * 2").unwrap();
        std::fs::write(
            models.join("good_mart.sql"),
            "SELECT double_it(id) AS doubled, status FROM raw_orders",
        )
        .unwrap();
        let result = run_tests(&models, None, None, &rocky_core::run_vars::RunVars::new()).unwrap();
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.passed, 2);
    }

    /// Unfiltered run reports every model — passes too, not just failures —
    /// so the Inspector Tests tab can render "raw_orders: pass" without
    /// inferring it from `total - failures`.
    #[test]
    fn full_run_itemizes_passes() {
        let (_tmp, models) = scaffold_two_model_project();
        let result = run_tests(&models, None, None, &rocky_core::run_vars::RunVars::new()).unwrap();
        assert_eq!(result.total, 2);
        assert_eq!(result.passed, 2);
        assert!(result.failures.is_empty());
        let names: Vec<_> = result
            .model_results
            .iter()
            .map(|m| (m.model.as_str(), m.status))
            .collect();
        assert!(names.contains(&("raw_orders", ModelTestStatus::Pass)));
        assert!(names.contains(&("good_mart", ModelTestStatus::Pass)));
    }

    /// #2045 shape 4 through the `rocky test` entry point: a producer whose
    /// target schema fails validation, a stale same-named seed table, and a
    /// consumer scoped with `--model`. The consumer never passes on the stale
    /// seed value, and the scoped run still reports it as failed.
    #[test]
    fn scoped_run_reports_a_consumer_whose_producer_failed() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(
            dir.path().join("data").join("seed.sql"),
            "CREATE TABLE main.source AS SELECT 7 AS v",
        )
        .unwrap();
        std::fs::write(models.join("source.sql"), "SELECT 42 AS v").unwrap();
        std::fs::write(
            models.join("source.toml"),
            "name = \"source\"\n[strategy]\ntype = \"full_refresh\"\n\
             [target]\ncatalog = \"memory\"\nschema = \"bad-name\"\ntable = \"source\"\n",
        )
        .unwrap();
        std::fs::write(models.join("consumer.sql"), "SELECT v FROM source").unwrap();
        std::fs::write(
            models.join("consumer.toml"),
            "name = \"consumer\"\ndepends_on = [\"source\"]\n[strategy]\ntype = \"full_refresh\"\n\
             [target]\ncatalog = \"memory\"\nschema = \"out\"\ntable = \"consumer\"\n",
        )
        .unwrap();
        let result = run_tests(
            &models,
            None,
            Some("consumer"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert_eq!(result.passed, 0, "{:?}", result.model_results);
        assert_eq!(result.total, 1, "{:?}", result.model_results);
        let (name, why) = &result.failures[0];
        assert_eq!(name, "consumer");
        assert!(why.contains("upstream 'source'"), "{why}");
    }

    /// A consumer record with a bad `depends_on` (E060) is a diagnostic, not a
    /// failed model: it never appears in `model_results` or `failures`, and
    /// the model tests still run.
    #[test]
    fn a_bad_consumer_record_is_a_diagnostic_and_the_model_tests_still_run() {
        let (tmp, models) = scaffold_two_model_project();
        let consumers = tmp.path().join("consumers");
        std::fs::create_dir_all(&consumers).unwrap();
        std::fs::write(consumers.join("board.toml"), "depends_on = [\"nowhere\"]\n").unwrap();
        let result = run_tests(&models, None, None, &rocky_core::run_vars::RunVars::new()).unwrap();
        assert!(
            result
                .diagnostics
                .iter()
                .any(rocky_compiler::consumers::is_consumer_diagnostic),
            "{:?}",
            result.diagnostics
        );
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.total, 2, "{:?}", result.model_results);
        assert_eq!(result.passed, 2, "{:?}", result.model_results);
        assert!(
            result
                .model_results
                .iter()
                .all(|m| !m.model.starts_with("consumer:")),
            "{:?}",
            result.model_results
        );
    }

    /// `--model good_mart` filters the reported results to one model. The
    /// upstream `raw_orders` still executes (so good_mart's SQL resolves)
    /// but doesn't appear in `model_results`. Closes the TODO that had
    /// `rocky test --model X` silently testing every model.
    #[test]
    fn model_filter_scopes_results_to_one_model() {
        let (_tmp, models) = scaffold_two_model_project();
        let result = run_tests(
            &models,
            None,
            Some("good_mart"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.passed, 1);
        assert_eq!(result.model_results.len(), 1);
        assert_eq!(result.model_results[0].model, "good_mart");
        assert_eq!(result.model_results[0].status, ModelTestStatus::Pass);
    }

    /// Scaffold a project where `flagged` filters an upstream `orders` model
    /// and a `[[test]]` mocks `orders` with fixture rows. `expect_match`
    /// toggles whether the asserted output matches the model's real output.
    fn scaffold_unit_test_project(expect_match: bool) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        // Upstream model the unit test mocks.
        std::fs::write(models.join("orders.sql"), "SELECT 0 AS id, 0 AS amount").unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        // Model under test: keep only high-value orders.
        std::fs::write(
            models.join("flagged.sql"),
            "SELECT id, amount, true AS is_high FROM orders WHERE amount > 100",
        )
        .unwrap();
        let expected_amount = if expect_match { 150 } else { 999 };
        std::fs::write(
            models.join("flagged.toml"),
            format!(
                "[strategy]\ntype = \"full_refresh\"\n\
                 [target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
                 [[test]]\nname = \"flags_high_value\"\n\n\
                 [[test.given]]\nref = \"orders\"\n\
                 rows = [ {{ id = 1, amount = 150 }}, {{ id = 2, amount = 50 }} ]\n\n\
                 [test.expect]\n\
                 rows = [ {{ id = 1, amount = {expected_amount}, is_high = true }} ]\n"
            ),
        )
        .unwrap();
        (dir, models)
    }

    /// Scaffold a `time_interval` model whose body filters on `@start_date` /
    /// `@end_date`, with a `[[test]]` whose fixture rows sit near both ends
    /// of the calendar (#2020).
    fn scaffold_time_interval_project() -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        std::fs::write(
            models.join("orders.sql"),
            "SELECT 1 AS id, TIMESTAMP '2026-01-01 00:00:00' AS order_at",
        )
        .unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        std::fs::write(
            models.join("daily.sql"),
            "SELECT id, order_at FROM orders \
             WHERE order_at >= @start_date AND order_at < @end_date",
        )
        .unwrap();
        std::fs::write(
            models.join("daily.toml"),
            "[strategy]\ntype = \"time_interval\"\ntime_column = \"order_at\"\n\
             granularity = \"day\"\n\
             [target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[test]]\nname = \"keeps_every_row\"\n\n\
             [[test.given]]\nref = \"orders\"\n\
             rows = [ { id = 1, order_at = \"0001-01-02 00:00:00\" }, \
             { id = 2, order_at = \"9999-12-30 00:00:00\" } ]\n\n\
             [test.expect]\n\
             rows = [ { id = 1 }, { id = 2 } ]\n",
        )
        .unwrap();
        (dir, models)
    }

    /// `rocky test` runs a `time_interval` model with the placeholders
    /// substituted, instead of failing on the bare `@start_date` (#2020).
    #[test]
    fn time_interval_model_executes_locally() {
        let (_tmp, models) = scaffold_time_interval_project();
        let result = run_tests(
            &models,
            None,
            Some("daily"),
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert_eq!(result.total, 1);
        assert_eq!(result.passed, 1, "failures: {:?}", result.failures);
    }

    /// A `[[test]]` on a `time_interval` model keeps fixture rows at both
    /// ends of the calendar: the local window drops none of them (#2020).
    #[test]
    fn time_interval_unit_test_keeps_rows_at_calendar_edges() {
        let (_tmp, models) = scaffold_time_interval_project();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.total(), 1);
        assert!(
            run.results[0].passed,
            "error: {:?}, mismatches: {:?}",
            run.results[0].error, run.results[0].mismatches
        );
    }

    /// A unit test passes when the model's output against the mocked inputs
    /// matches the expectation (the high-value filter keeps only `id = 1`).
    #[test]
    fn unit_test_passes_when_output_matches() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(run.passed(), 1, "error: {:?}", run.results[0].error);
        assert!(run.results[0].passed);
    }

    /// A unit test fails and reports row-level diagnostics when the model's
    /// output diverges from the expectation.
    #[test]
    fn unit_test_fails_and_reports_mismatch() {
        let (_tmp, models) = scaffold_unit_test_project(false);
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(run.passed(), 0);
        assert!(!run.results[0].passed);
        assert!(
            !run.results[0].mismatches.is_empty(),
            "expected mismatch diagnostics"
        );
    }

    /// A project whose models declare no `[[test]]` blocks runs zero unit
    /// tests (and skips compilation entirely).
    #[test]
    fn unit_tests_empty_when_none_declared() {
        let (_tmp, models) = scaffold_two_model_project();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.total(), 0);
    }

    /// FR-045: the fixture sidecar omits a `note` key on row 1 (a null cell
    /// dropped on emit) but sets it on row 2. The run-side builder must UNION
    /// the column set across rows so `note` is present, materializing the
    /// absent row-1 cell as SQL NULL. The model under test does null-handling
    /// (`COALESCE(note, 'EMPTY')`); the test passes only if the built fixture
    /// table actually carries a SQL NULL in row 1. The old `rows[0]`-only
    /// column logic would have dropped the `note` column and failed model
    /// execution outright.
    #[test]
    fn unit_test_union_of_keys_materializes_absent_cell_as_null() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();

        std::fs::write(models.join("orders.sql"), "SELECT 0 AS id, '' AS note").unwrap();
        std::fs::write(
            models.join("orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();

        // Model under test exercises NULL handling on the mocked column.
        std::fs::write(
            models.join("filled.sql"),
            "SELECT id, COALESCE(note, 'EMPTY') AS note_filled FROM orders",
        )
        .unwrap();
        // Row 1 OMITS `note` (the post-emit shape of a null cell); row 2 sets
        // it. Only the union builder keeps the `note` column for both rows.
        std::fs::write(
            models.join("filled.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\
             [target]\ncatalog = \"wh\"\nschema = \"main\"\n\n\
             [[test]]\nname = \"null_coalesces\"\n\n\
             [[test.given]]\nref = \"orders\"\n\
             rows = [ { id = 1 }, { id = 2, note = \"hi\" } ]\n\n\
             [test.expect]\n\
             rows = [ { id = 1, note_filled = \"EMPTY\" }, { id = 2, note_filled = \"hi\" } ]\n",
        )
        .unwrap();

        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(
            run.passed(),
            1,
            "union-of-keys NULL handling failed: {:?}",
            run.results[0].error
        );
        assert!(run.results[0].passed);
    }

    fn write_full_refresh(dir: &Path, name: &str, sql: &str) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        std::fs::write(
            dir.join(format!("{name}.toml")),
            "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
    }

    /// A seed with one `src.orders` table, and `models/stg` reading it.
    fn scaffold_seeded_project() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(
            seed_path(dir.path()),
            "CREATE SCHEMA src;\n\
             CREATE TABLE src.orders AS SELECT 1::BIGINT AS id, 'a' AS status;\n",
        )
        .unwrap();
        write_full_refresh(
            &dir.path().join("models"),
            "stg",
            "SELECT id, status FROM src.orders",
        );
        dir
    }

    /// Two model roots joined into one preloaded set run in one database, in
    /// dependency order: `reporting/rep` reads `models/stg`'s output. Alone,
    /// `reporting/` cannot see it.
    #[test]
    fn preloaded_models_from_two_roots_run_in_one_database() {
        let dir = scaffold_seeded_project();
        let reporting = dir.path().join("reporting");
        write_full_refresh(&reporting, "rep", "SELECT id FROM stg");
        let models_dir = dir.path().join("models");

        let mut models = rocky_compiler::models_loader::load_project_models(&models_dir, None)
            .expect("load models");
        models.extend(
            rocky_compiler::models_loader::load_project_models(&reporting, None)
                .expect("load reporting"),
        );
        let result = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Preloaded(models),
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: None,
            inlined_gates: None,
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
        .unwrap();
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.passed, 2, "{:?}", result.model_results);

        let alone = run_tests(
            &reporting,
            None,
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert!(
            alone.failures.iter().any(|(name, _)| name == "rep"),
            "reporting/ alone should not see stg: {:?}",
            alone.model_results
        );
    }

    /// The compile is typed from the seed the models run on, so a contract
    /// that declares the wrong type is E011 before anything executes.
    /// Without the seed every column was Unknown and the type check skipped.
    #[test]
    fn contract_type_mismatch_is_found_from_the_seed() {
        let dir = scaffold_seeded_project();
        let contracts = dir.path().join("contracts");
        std::fs::create_dir_all(&contracts).unwrap();
        std::fs::write(
            contracts.join("stg.contract.toml"),
            "[[columns]]\nname = \"id\"\ntype = \"String\"\nnullable = true\n",
        )
        .unwrap();
        let models_dir = dir.path().join("models");

        let result = run_tests(
            &models_dir,
            Some(&contracts),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert!(
            result.diagnostics.iter().any(|d| &*d.code == "E011"),
            "{:?}",
            result.diagnostics
        );
        assert!(!result.failures.is_empty());
    }

    /// The control: the contract's type matches the seed, so the grounded
    /// compile is clean and both models pass.
    #[test]
    fn contract_with_the_seed_type_passes() {
        let dir = scaffold_seeded_project();
        let contracts = dir.path().join("contracts");
        std::fs::create_dir_all(&contracts).unwrap();
        std::fs::write(
            contracts.join("stg.contract.toml"),
            "[[columns]]\nname = \"id\"\ntype = \"Int64\"\nnullable = true\n",
        )
        .unwrap();
        let models_dir = dir.path().join("models");

        let result = run_tests(
            &models_dir,
            Some(&contracts),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert!(
            !result
                .diagnostics
                .iter()
                .any(rocky_compiler::diagnostic::Diagnostic::is_error),
            "{:?}",
            result.diagnostics
        );
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.passed, 1);
    }

    /// A stale seed table with a model's name does not shadow the model: a
    /// reader of `stg` gets the model's columns, not the seed table's.
    #[test]
    fn a_seed_table_named_like_a_model_does_not_shadow_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();
        std::fs::write(
            seed_path(dir.path()),
            "CREATE TABLE main.stg AS SELECT 'stale' AS other;\n",
        )
        .unwrap();
        let models_dir = dir.path().join("models");
        write_full_refresh(&models_dir, "stg", "SELECT 1::BIGINT AS id");
        write_full_refresh(&models_dir, "rep", "SELECT id FROM stg");
        let contracts = dir.path().join("contracts");
        std::fs::create_dir_all(&contracts).unwrap();
        std::fs::write(
            contracts.join("rep.contract.toml"),
            "[[columns]]\nname = \"id\"\ntype = \"Int64\"\nnullable = true\n\n\
             [rules]\nrequired = [\"id\"]\n",
        )
        .unwrap();

        let result = run_tests(
            &models_dir,
            Some(&contracts),
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert!(
            !result
                .diagnostics
                .iter()
                .any(rocky_compiler::diagnostic::Diagnostic::is_error),
            "{:?}",
            result.diagnostics
        );
        assert!(result.failures.is_empty(), "{:?}", result.failures);
    }

    /// A model reading a column the seed's table lacks gets W041 from the
    /// grounded compile, which names the missing column.
    #[test]
    fn a_column_the_seed_lacks_is_w041() {
        let dir = scaffold_seeded_project();
        let models_dir = dir.path().join("models");
        write_full_refresh(&models_dir, "stg", "SELECT id, segment FROM src.orders");

        let result = run_tests(
            &models_dir,
            None,
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        assert!(
            result
                .diagnostics
                .iter()
                .any(|d| &*d.code == "W041" && d.message.contains("segment")),
            "{:?}",
            result.diagnostics
        );
    }

    /// A dependency cycle is reported as E058 compile errors, one per model
    /// on it, and no model executes.
    #[test]
    fn a_dependency_cycle_is_e058_and_executes_nothing() {
        let dir = scaffold_seeded_project();
        let models_dir = dir.path().join("models");
        write_full_refresh(
            &models_dir,
            "fct",
            "SELECT id FROM stg WHERE id IN (SELECT id FROM ltv)",
        );
        write_full_refresh(&models_dir, "ltv", "SELECT id FROM fct");

        let result = run_tests(
            &models_dir,
            None,
            None,
            &rocky_core::run_vars::RunVars::new(),
        )
        .unwrap();
        let mut cycle: Vec<&str> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == rocky_compiler::diagnostic::E058 && d.is_error())
            .map(|d| d.model.as_str())
            .collect();
        cycle.sort_unstable();
        assert_eq!(cycle, ["fct", "ltv"], "{:?}", result.diagnostics);
        assert_eq!(result.passed, 0);
        assert_eq!(result.failures.len(), 2, "{:?}", result.failures);
        assert!(
            result.all_models.iter().any(|m| m == "stg"),
            "every loaded model is listed: {:?}",
            result.all_models
        );
    }

    /// On a dependency cycle every unit test fails, naming E058, instead of
    /// the whole run failing with the bare cycle error.
    #[test]
    fn a_dependency_cycle_fails_every_unit_test() {
        let (_dir, models) = scaffold_unit_test_project(true);
        std::fs::write(models.join("orders.sql"), "SELECT id, amount FROM flagged").unwrap();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.results.len(), 1, "{:?}", run.results);
        let result = &run.results[0];
        assert_eq!(result.model, "flagged");
        assert!(!result.passed);
        assert!(
            result.error.as_deref().is_some_and(|e| e.contains("E058")),
            "{:?}",
            result.error
        );
    }

    /// Offline mode reaches the runner's own in-memory database. The empty
    /// extension directory holds no `icu`, so the connect fails with the
    /// offline error instead of running `INSTALL icu` over the network.
    #[test]
    fn offline_mode_reaches_the_test_runner_connection() {
        let dir = scaffold_seeded_project();
        let models_dir = dir.path().join("models");
        let ext_dir = tempfile::tempdir().unwrap();
        let offline = ExtensionOptions {
            directory: Some(ext_dir.path().to_path_buf()),
            offline: true,
        };
        let err = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Dir,
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: None,
            inlined_gates: None,
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: offline.clone(),
        })
        .unwrap_err();
        assert!(
            matches!(
                err.downcast_ref::<rocky_duckdb::DuckDbError>(),
                Some(rocky_duckdb::DuckDbError::OfflineExtensionMissing { .. })
            ),
            "{err:#}"
        );

        let (_unit_dir, unit_models) = scaffold_unit_test_project(true);
        let run = run_unit_tests(&unit_models, None, &offline, &duckdb_target(), false).unwrap();
        assert!(!run.results.is_empty());
        assert!(
            run.results.iter().all(|r| !r.passed
                && r.error
                    .as_deref()
                    .is_some_and(|e| e.contains("offline mode needs the `icu` extension"))),
            "{:?}",
            run.results
        );
    }

    /// An error the caller's gates add fails the run before any model
    /// executes; a gate that adds nothing leaves the run as it was.
    #[test]
    fn a_gate_error_fails_the_run_before_execution() {
        let dir = scaffold_seeded_project();
        let models_dir = dir.path().join("models");
        let refuse = |result: &mut rocky_compiler::compile::CompileResult| {
            result
                .diagnostics
                .push(rocky_compiler::diagnostic::Diagnostic::error(
                    "E042", "stg", "refused",
                ));
        };
        let result = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Dir,
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: Some(&refuse),
            inlined_gates: None,
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
        .unwrap();
        assert_eq!(result.passed, 0, "{:?}", result.model_results);
        assert_eq!(
            result.failures,
            [("stg".to_string(), "refused".to_string())]
        );
        assert!(result.diagnostics.iter().any(|d| &*d.code == "E042"));

        let silent = |_: &mut rocky_compiler::compile::CompileResult| {};
        let result = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Dir,
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: Some(&silent),
            inlined_gates: None,
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
        .unwrap();
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert_eq!(result.passed, 1);

        // An error the inlined gates add fails the run the same way.
        let result = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Dir,
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: None,
            inlined_gates: Some(&refuse),
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
        .unwrap();
        assert_eq!(result.passed, 0, "{:?}", result.model_results);
        assert_eq!(
            result.failures,
            [("stg".to_string(), "refused".to_string())]
        );
    }

    /// The gates see each model's authored SQL; the inlined gates see the
    /// SQL the model executes, with its ephemeral upstream inlined.
    #[test]
    fn gates_see_authored_sql_and_inlined_gates_see_the_executed_sql() {
        let dir = scaffold_seeded_project();
        let models_dir = dir.path().join("models");
        std::fs::write(models_dir.join("eph.sql"), "SELECT id FROM stg").unwrap();
        std::fs::write(
            models_dir.join("eph.toml"),
            "[strategy]\ntype = \"ephemeral\"\n[target]\ncatalog=\"wh\"\nschema=\"main\"\n",
        )
        .unwrap();
        write_full_refresh(&models_dir, "mart", "SELECT id FROM eph");
        let seen = std::cell::RefCell::new(String::new());
        let record = |result: &mut rocky_compiler::compile::CompileResult| {
            let mart = result.project.model("mart").unwrap();
            *seen.borrow_mut() = mart.sql.clone();
        };
        let seen_inlined = std::cell::RefCell::new(String::new());
        let record_inlined = |result: &mut rocky_compiler::compile::CompileResult| {
            let mart = result.project.model("mart").unwrap();
            *seen_inlined.borrow_mut() = mart.sql.clone();
        };
        let result = run_tests_with(TestRunInputs {
            models_dir: &models_dir,
            project_root: dir.path(),
            models: TestModels::Dir,
            contracts_dir: None,
            model_filter: None,
            run_vars: &rocky_core::run_vars::RunVars::new(),
            gates: Some(&record),
            inlined_gates: Some(&record_inlined),
            strict_contracts: false,
            target_dialects: Default::default(),
            extensions: ExtensionOptions::default(),
        })
        .unwrap();
        assert_eq!(seen.borrow().trim(), "SELECT id FROM eph");
        assert!(
            seen_inlined.borrow().contains("WITH"),
            "the inlined gates see the ephemeral inlined: {}",
            seen_inlined.borrow()
        );
        assert!(result.failures.is_empty(), "{:?}", result.failures);
        assert!(
            result
                .model_results
                .iter()
                .any(|m| m.model == "mart" && m.status == ModelTestStatus::Pass),
            "{:?}",
            result.model_results
        );
    }

    /// Every model runs on DuckDB alone: the target of a DuckDB project.
    fn duckdb_target() -> TargetDialects {
        TargetDialects::uniform(Some(OperandDialect::DuckDb).into())
    }

    /// Every model runs on Snowflake alone.
    fn snowflake_target() -> TargetDialects {
        TargetDialects::uniform(Some(OperandDialect::Snowflake).into())
    }

    /// The exact SQL `flagged` authors in [`scaffold_unit_test_project`].
    const FLAGGED_SQL: &str = "SELECT id, amount, true AS is_high FROM orders WHERE amount > 100";

    fn compile_models(models: &Path) -> rocky_compiler::compile::CompileResult {
        rocky_compiler::compile::compile(&CompilerConfig {
            models_dir: models.to_path_buf(),
            ..Default::default()
        })
        .expect("compiles")
    }

    #[test]
    fn target_notice_classifies_each_target_shape() {
        use UnitTestDialectStatus::*;
        let targets = |dialects: Vec<OperandDialect>, unruled: Vec<&str>| OperandTarget::Targets {
            dialects,
            unruled: unruled.into_iter().map(String::from).collect(),
        };
        assert_eq!(
            target_notice(&targets(vec![OperandDialect::DuckDb], vec![])),
            None
        );
        assert_eq!(
            target_notice(&OperandTarget::Unconfigured),
            Some((UnresolvedTarget, vec![]))
        );
        assert_eq!(
            target_notice(&targets(vec![OperandDialect::Snowflake], vec![])),
            Some((NonDuckdbTarget, vec!["Snowflake".to_string()]))
        );
        assert_eq!(
            target_notice(&targets(vec![], vec!["clickhouse"])),
            Some((NonDuckdbTarget, vec!["clickhouse".to_string()]))
        );
        assert_eq!(
            target_notice(&targets(
                vec![OperandDialect::DuckDb, OperandDialect::Snowflake],
                vec![]
            )),
            Some((
                MultipleTargets,
                vec!["DuckDB".to_string(), "Snowflake".to_string()]
            ))
        );
        assert_eq!(
            target_notice(&targets(vec![OperandDialect::DuckDb], vec!["clickhouse"])),
            Some((
                MultipleTargets,
                vec!["DuckDB".to_string(), "clickhouse".to_string()]
            ))
        );
        assert_eq!(
            target_notice(&targets(vec![], vec![])),
            Some((UnresolvedTarget, vec![]))
        );
    }

    /// A DuckDB project's unit test runs the model's SQL text unchanged, and
    /// its JSON result carries no `dialect` key. A Snowflake target runs the
    /// same SQL: the target adds a notice, never a different query.
    #[test]
    fn a_duckdb_target_runs_the_authored_sql_with_no_marker() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let compiled = compile_models(&models);
        let duckdb = plan_model_unit_tests(
            "flagged",
            &compiled,
            duckdb_target().for_model("flagged"),
            true,
        );
        assert_eq!(
            duckdb,
            ModelUnitTestPlan::Run {
                sql: FLAGGED_SQL.to_string(),
                dialect: None,
            }
        );
        let snowflake = plan_model_unit_tests(
            "flagged",
            &compiled,
            snowflake_target().for_model("flagged"),
            false,
        );
        let ModelUnitTestPlan::Run { sql, dialect } = snowflake else {
            panic!("a default-mode Snowflake target runs: {snowflake:?}");
        };
        assert_eq!(sql, FLAGGED_SQL);
        assert!(dialect.is_some());

        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &duckdb_target(),
            true,
        )
        .unwrap();
        assert_eq!(run.passed(), 1, "{:?}", run.results);
        let json = serde_json::to_string(&run.results).unwrap();
        assert!(!json.contains("\"dialect\""), "{json}");
    }

    /// A Snowflake project's unit test still runs on DuckDB by default, and
    /// says so.
    #[test]
    fn a_snowflake_target_marks_each_result() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &snowflake_target(),
            false,
        )
        .unwrap();
        assert_eq!(run.passed(), 1, "{:?}", run.results);
        assert_eq!(
            run.results[0].dialect,
            Some(UnitTestDialect {
                status: UnitTestDialectStatus::NonDuckdbTarget,
                targets: vec!["Snowflake".to_string()],
                ran_on_duckdb: true,
                refused: false,
            })
        );
        let json = serde_json::to_value(&run.results[0]).unwrap();
        assert_eq!(json["dialect"]["status"], "non_duckdb_target");
    }

    /// `--strict-dialect` refuses a Snowflake project's unit test instead of
    /// running it, and the refusal fails the test.
    #[test]
    fn strict_dialect_refuses_a_snowflake_target() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &snowflake_target(),
            true,
        )
        .unwrap();
        assert_eq!(run.total(), 1);
        assert_eq!(run.passed(), 0);
        let result = &run.results[0];
        let error = result.error.as_deref().unwrap_or_default();
        assert!(error.contains("--strict-dialect"), "{error}");
        assert!(error.contains("Snowflake"), "{error}");
        let dialect = result.dialect.as_ref().expect("marked");
        assert!(dialect.refused && !dialect.ran_on_duckdb, "{dialect:?}");
    }

    /// A test that fails before its model's SQL reaches DuckDB (here, a
    /// fixture ref that is not an identifier) is marked as not run.
    #[test]
    fn a_test_that_fails_before_the_model_runs_is_marked_not_run() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let sidecar = models.join("flagged.toml");
        let text = std::fs::read_to_string(&sidecar).unwrap();
        std::fs::write(
            &sidecar,
            text.replace("ref = \"orders\"", "ref = \"or-ders\""),
        )
        .unwrap();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &snowflake_target(),
            false,
        )
        .unwrap();
        let result = &run.results[0];
        assert!(!result.passed);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("invalid fixture ref"),
            "{result:?}"
        );
        let dialect = result.dialect.as_ref().expect("marked");
        assert!(!dialect.ran_on_duckdb && !dialect.refused, "{dialect:?}");
    }

    /// A model whose SQL DuckDB rejects did reach DuckDB: `ran_on_duckdb` is
    /// `true` once the SQL is sent, whatever DuckDB answers.
    #[test]
    fn a_model_duckdb_rejects_is_marked_ran_on_duckdb() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        std::fs::write(
            models.join("flagged.sql"),
            "SELECT id, CAST('not a number' AS INTEGER) AS amount, true AS is_high FROM orders",
        )
        .unwrap();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &snowflake_target(),
            false,
        )
        .unwrap();
        let result = &run.results[0];
        assert!(!result.passed);
        assert!(
            result
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("model execution failed"),
            "{result:?}"
        );
        let dialect = result.dialect.as_ref().expect("marked");
        assert!(dialect.ran_on_duckdb && !dialect.refused, "{dialect:?}");
    }

    /// No resolved target is never taken as DuckDB: the default mode runs and
    /// marks the test, strict mode refuses it.
    #[test]
    fn an_unresolved_target_is_marked_or_refused() {
        let (_tmp, models) = scaffold_unit_test_project(true);
        let unresolved = TargetDialects::default();
        let run = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &unresolved,
            false,
        )
        .unwrap();
        assert_eq!(run.passed(), 1, "{:?}", run.results);
        let dialect = run.results[0].dialect.as_ref().expect("marked");
        assert_eq!(dialect.status, UnitTestDialectStatus::UnresolvedTarget);
        assert!(dialect.ran_on_duckdb);

        let strict = run_unit_tests(
            &models,
            None,
            &ExtensionOptions::default(),
            &unresolved,
            true,
        )
        .unwrap();
        assert_eq!(strict.passed(), 0);
        let error = strict.results[0].error.as_deref().unwrap_or_default();
        assert!(error.contains("no target warehouse"), "{error}");
    }
}
