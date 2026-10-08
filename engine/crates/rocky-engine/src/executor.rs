//! Local SQL execution via DuckDB.
//!
//! Executes compiled models locally against a DuckDB instance,
//! either with sampled data or in-memory test data.
//!
//! # Where a model lands, and what a bare read reaches (#1354, #2044)
//!
//! Every model materializes at its configured `[target]`, as a warehouse run
//! does — not under its model name. So the compile graph encodes one
//! execution semantics for both paths:
//!
//! - Each project catalog is attached as an in-memory database
//!   (`ATTACH ':memory:' AS <catalog>`). A catalog name that already resolves
//!   in the local database (`memory`, or one attached earlier) is reused, not
//!   attached again. A name DuckDB reserves and that does not resolve
//!   (`main`, `system`, `temp`) is refused for every model that targets it,
//!   with the fix: map it to another name for local runs.
//! - A target that names no catalog lands in the local database's default
//!   catalog.
//! - A bare read resolves through DuckDB's `search_path`. Before each model,
//!   the schemas of the producers the compiler bound its bare reads to
//!   ([`rocky_compiler::resolve::BareReadIndex`]) go first on the path, then
//!   the default schema. After the path is set, every bare read is checked
//!   against what DuckDB will actually bind; a read that would reach any other
//!   table fails the model instead of running it on the wrong data.
//! - A model whose upstream failed, or was itself skipped for that reason,
//!   fails without running: the table it would read is stale or absent.
//! - An `ephemeral` model builds nothing. The compiler inlined it into every
//!   model that reads it, so it is skipped here.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::path::Path;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_compiler::resolve::{BareBinding, BareReadIndex};
use rocky_core::models::{Model, StrategyConfig};
use rocky_core::physical_edges::fold_identifier;
use rocky_duckdb::DuckDbConnector;
use rocky_ir::dag::DagNode;
use rocky_sql::validation::validate_identifier;
use tracing::{info, warn};

/// Result of local execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Models executed successfully.
    pub succeeded: Vec<String>,
    /// Models that failed (name, error).
    pub failed: Vec<(String, String)>,
    /// Models with nothing to build locally (name, reason) — today, the
    /// `ephemeral` models, which the compiler inlined into their readers.
    pub skipped: Vec<(String, String)>,
}

/// Catalog names DuckDB reserves. `ATTACH ... AS` refuses `main`, `system`
/// and `temp`; `memory` is the in-memory default database and already exists.
const RESERVED_CATALOGS: [&str; 4] = ["main", "system", "temp", "memory"];

/// Quote one identifier for DuckDB.
fn quote(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Quote one value as a DuckDB string literal.
fn literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// A `(catalog, schema)` location, both folded the way DuckDB compares
/// identifiers (case-insensitively).
type Location = (String, String);

/// Execute compiled models locally using DuckDB.
///
/// Models run in dependency order: the compile graph plus the physical-read
/// edges a run derives (a model reading another model's target by its
/// qualified name). See the module docs for where each model lands.
///
/// # Divergence: surrogate keys are not applied here
///
/// Unlike `rocky run` and `rocky emit-sql` — which call
/// [`rocky_core::models::apply_surrogate_keys`] — this path materializes each
/// model from its compiled SQL alone. A model that declares a `[[surrogate_key]]`
/// block is built here *without* the surrogate column. This backs `rocky test`'s
/// model-execution check ([`crate::test_runner::run_tests`]), so a test that
/// depends on a surrogate column (e.g. a `unique` assertion on it) would behave
/// differently from a real run. Aligning the two would change existing
/// `rocky test` outcomes, so it is left as a known divergence pending a decision
/// on whether `rocky test` should mirror run-time surrogate keys.
pub fn execute_locally(compile_result: &CompileResult, db: &DuckDbConnector) -> ExecutionResult {
    let mut result = ExecutionResult {
        succeeded: Vec::new(),
        failed: Vec::new(),
        skipped: Vec::new(),
    };

    // User-defined functions first, as `rocky run` does, so models that call
    // them resolve. A function that fails is reported under its own name.
    result
        .failed
        .extend(create_local_functions(compile_result, db));

    let models = &compile_result.project.models;
    let default_catalog = match current_database(db) {
        Ok(name) => name,
        Err(e) => {
            for m in models {
                result
                    .failed
                    .push((m.config.name.clone(), format!("local database: {e}")));
            }
            return result;
        }
    };

    let refused_catalogs = attach_catalogs(models, db);
    let order = match execution_order(compile_result, &default_catalog) {
        Ok(order) => order,
        Err(e) => {
            for m in models {
                result.failed.push((m.config.name.clone(), e.clone()));
            }
            return result;
        }
    };

    let index = BareReadIndex::new(models);
    // Where each model that writes a table lands, by name.
    let location_of: HashMap<&str, Location> = models
        .iter()
        .filter(|m| !is_ephemeral(m))
        .map(|m| (m.config.name.as_str(), location(m, &default_catalog)))
        .collect();
    // Every table this run writes, for the check that an external bare read
    // does not land on one of them.
    let written: HashSet<(Location, String)> = models
        .iter()
        .filter(|m| !is_ephemeral(m))
        .map(|m| {
            (
                location(m, &default_catalog),
                fold_identifier(&m.config.target.table),
            )
        })
        .collect();

    // The compile graph's own dependencies, which already dropped any
    // table-only binding that would close a cycle (D013).
    let compiled_deps: HashMap<&str, &[String]> = compile_result
        .project
        .dag_nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();
    // The compiler inlined an ephemeral model's SQL into its readers, so its
    // bare reads belong to each reader. A reader depends on what the inlined
    // body reads, through any chain of ephemeral models (#2316).
    let ephemeral_names: HashSet<&str> = models
        .iter()
        .filter(|m| is_ephemeral(m))
        .map(|m| m.config.name.as_str())
        .collect();
    // Models that failed or were withheld, with the reason a dependent cites.
    let mut broken: HashMap<String, String> = HashMap::new();

    for (model_name, depends_on) in &order {
        let Some(model) = compile_result.project.model(model_name) else {
            continue;
        };
        if is_ephemeral(model) {
            // Inlined into its readers: if what it reads did not build, its
            // readers must not run either.
            if let Some((upstream, why)) = depends_on
                .iter()
                .find_map(|d| broken.get(d).map(|why| (d.clone(), why.clone())))
            {
                broken.insert(
                    model_name.clone(),
                    format!("upstream '{upstream}' did not build ({why})"),
                );
            }
            result.skipped.push((
                model_name.clone(),
                "ephemeral: inlined into each model that reads it, nothing to build".to_string(),
            ));
            continue;
        }

        let outcome = (|| -> Result<(), String> {
            if let Some((upstream, why)) = depends_on
                .iter()
                .find_map(|d| broken.get(d).map(|why| (d, why)))
            {
                return Err(format!(
                    "not run: upstream '{upstream}' did not build ({why})"
                ));
            }
            let t = &model.config.target;
            for component in [&t.catalog, &t.schema, &t.table]
                .into_iter()
                .filter(|c| !c.is_empty())
            {
                validate_identifier(component).map_err(|e| e.to_string())?;
            }
            if let Some(refusal) = refused_catalogs.get(&fold_identifier(&t.catalog)) {
                return Err(refusal.clone());
            }
            let (catalog, schema) = target_parts(model, &default_catalog);
            db.execute_statement(&format!(
                "CREATE SCHEMA IF NOT EXISTS {}.{}",
                quote(&catalog),
                quote(&schema)
            ))
            .map_err(|e| e.to_string())?;
            let sql = rocky_core::sql_gen::local_test_sql(model);
            set_search_path(
                model,
                &sql,
                &through_ephemerals(model_name, &compiled_deps, &ephemeral_names),
                &index,
                &location_of,
                &written,
                &default_catalog,
                db,
            )?;
            // Every component was validated above and is quoted here;
            // `sql` is compiler-emitted.
            db.execute_statement(&format!(
                "CREATE OR REPLACE TABLE {}.{}.{} AS\n{sql}",
                quote(&catalog),
                quote(&schema),
                quote(&t.table)
            ))
            .map_err(|e| e.to_string())
        })();

        match outcome {
            Ok(()) => {
                info!(model = model_name.as_str(), "model executed locally");
                result.succeeded.push(model_name.clone());
            }
            Err(e) => {
                broken.insert(model_name.clone(), first_line(&e));
                result.failed.push((model_name.clone(), e));
            }
        }
    }

    // Leave the session as it found it for whatever the caller runs next.
    if let Err(e) = db.execute_statement("RESET search_path") {
        warn!(error = %e, "could not reset the local search path");
    }

    result
}

fn is_ephemeral(model: &Model) -> bool {
    matches!(model.config.strategy, StrategyConfig::Ephemeral)
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or_default().to_string()
}

/// The local database's default catalog (`memory` for an in-memory one).
fn current_database(db: &DuckDbConnector) -> Result<String, String> {
    let r = db
        .execute_sql("SELECT current_database()")
        .map_err(|e| e.to_string())?;
    r.rows
        .first()
        .and_then(|row| row.first())
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| "current_database() returned nothing".to_string())
}

/// The `(catalog, schema)` a model's table is created in, as spelled.
fn target_parts(model: &Model, default_catalog: &str) -> (String, String) {
    let t = &model.config.target;
    let catalog = if t.catalog.is_empty() {
        default_catalog.to_string()
    } else {
        t.catalog.clone()
    };
    (catalog, t.schema.clone())
}

fn location(model: &Model, default_catalog: &str) -> Location {
    let (catalog, schema) = target_parts(model, default_catalog);
    (fold_identifier(&catalog), fold_identifier(&schema))
}

/// Attach every catalog a model targets that does not already resolve.
/// Returns the refusal for each catalog that cannot be attached, keyed by its
/// folded name; every model targeting it fails with that message.
fn attach_catalogs(models: &[Model], db: &DuckDbConnector) -> HashMap<String, String> {
    let mut refused = HashMap::new();
    let existing: HashSet<String> =
        match db.execute_sql("SELECT database_name FROM duckdb_databases()") {
            Ok(r) => r
                .rows
                .iter()
                .filter_map(|row| row.first().and_then(|v| v.as_str()))
                .map(fold_identifier)
                .collect(),
            Err(e) => {
                let msg = format!("could not list local databases: {e}");
                for m in models
                    .iter()
                    .filter(|m| !m.config.target.catalog.is_empty())
                {
                    refused.insert(fold_identifier(&m.config.target.catalog), msg.clone());
                }
                return refused;
            }
        };
    let catalogs: BTreeSet<&str> = models
        .iter()
        .filter(|m| !is_ephemeral(m))
        .map(|m| m.config.target.catalog.as_str())
        .filter(|c| !c.is_empty())
        .collect();
    let mut attached: HashSet<String> = HashSet::new();
    for catalog in catalogs {
        let folded = fold_identifier(catalog);
        // `memory` is the default in-memory database: reused. Every other
        // reserved name is refused even when DuckDB lists it (`system`,
        // `temp`): those are not databases a model may write into.
        if folded == "memory" || !attached.insert(folded.clone()) {
            continue;
        }
        if RESERVED_CATALOGS.contains(&folded.as_str()) {
            refused.insert(
                folded,
                format!(
                    "catalog '{catalog}' is a name DuckDB reserves, so local execution cannot \
                     create it. Map it to another name for local runs, for example \
                     `catalog = \"${{ROCKY_CATALOG:-local_{catalog}}}\"` in rocky.toml"
                ),
            );
            continue;
        }
        if let Err(e) = validate_identifier(catalog) {
            refused.insert(folded, e.to_string());
            continue;
        }
        if existing.contains(&folded) {
            continue;
        }
        if let Err(e) = db.execute_statement(&format!("ATTACH ':memory:' AS {}", quote(catalog))) {
            refused.insert(
                folded,
                format!("could not attach catalog '{catalog}' locally: {e}"),
            );
        }
    }
    refused
}

/// Every model in dependency order, with the models it depends on: the
/// compile graph plus the edges a run derives for a read of another model's
/// target by its qualified name (`rocky_core::physical_edges`). A
/// catalogless target lives in `default_catalog` here.
fn execution_order(
    compile_result: &CompileResult,
    default_catalog: &str,
) -> Result<Vec<(String, Vec<String>)>, String> {
    let project = &compile_result.project;
    let mut nodes: Vec<DagNode> = project.dag_nodes.clone();
    let existing: Vec<(String, String)> = nodes
        .iter()
        .flat_map(|n| n.depends_on.iter().map(|d| (n.name.clone(), d.clone())))
        .collect();
    let inputs: Vec<rocky_core::physical_edges::PhysicalEdgeModel<'_>> = project
        .models
        .iter()
        .map(|m| {
            rocky_core::physical_edges::PhysicalEdgeModel::from_model(m)
                .with_effective_catalog(Some(default_catalog))
        })
        .collect();
    let derived = rocky_core::physical_edges::derive_physical_edges(&inputs, &existing);
    for w in rocky_core::physical_edges::derivation_warnings(&derived) {
        warn!(warning = w.as_str(), "local execution ordering");
    }
    for (consumer, producer) in derived.edges {
        if let Some(node) = nodes.iter_mut().find(|n| n.name == consumer)
            && !node.depends_on.contains(&producer)
        {
            node.depends_on.push(producer);
        }
    }
    let layers = rocky_ir::dag::execution_layers(&nodes).map_err(|e| e.to_string())?;
    let deps: HashMap<&str, &Vec<String>> = nodes
        .iter()
        .map(|n| (n.name.as_str(), &n.depends_on))
        .collect();
    Ok(layers
        .into_iter()
        .flatten()
        .map(|name| {
            let d = deps
                .get(name.as_str())
                .map(|d| (*d).clone())
                .unwrap_or_default();
            (name, d)
        })
        .collect())
}

/// The compile graph's dependencies of `model`, with each ephemeral
/// dependency replaced by what its inlined body reads, transitively.
fn through_ephemerals(
    model: &str,
    compiled_deps: &HashMap<&str, &[String]>,
    ephemeral: &HashSet<&str>,
) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();
    let mut stack: Vec<&str> = vec![model];
    while let Some(name) = stack.pop() {
        for dep in compiled_deps.get(name).copied().unwrap_or(&[]) {
            if ephemeral.contains(dep.as_str()) {
                if seen.insert(dep.as_str()) {
                    stack.push(dep.as_str());
                }
            } else if !out.contains(dep) {
                out.push(dep.clone());
            }
        }
    }
    out
}

/// Put the schemas of the producers `model`'s bare reads bind to first on
/// the search path, then the default schema, and check that DuckDB binds
/// every bare read to the table the compiler chose.
///
/// A bare read the compiler bound to a model must reach that model's table.
/// An external bare read must not reach a table this run writes. Anything
/// else is refused, naming the read and both tables, so the model never
/// runs on data the compile graph did not order it after.
#[allow(clippy::too_many_arguments)]
fn set_search_path(
    model: &Model,
    sql: &str,
    compiled_deps: &[String],
    index: &BareReadIndex<'_>,
    location_of: &HashMap<&str, Location>,
    written: &HashSet<(Location, String)>,
    default_catalog: &str,
    db: &DuckDbConnector,
) -> Result<(), String> {
    let lineage = rocky_sql::lineage::extract_lineage(sql)
        .map_err(|e| format!("could not read the model's table references: {e}"))?;
    let bare_reads: Vec<String> = {
        let mut seen = HashSet::new();
        lineage
            .source_tables
            .iter()
            .filter(|t| matches!(t.binding, rocky_sql::lineage::TableBinding::Physical))
            .map(|t| t.name.clone())
            .chain(lineage.nested_sources.iter().cloned())
            .filter(|n| !n.contains('.'))
            .filter(|n| seen.insert(n.clone()))
            .collect()
    };

    let two_part_reads: Vec<(String, String)> = lineage
        .source_tables
        .iter()
        .filter(|t| matches!(t.binding, rocky_sql::lineage::TableBinding::Physical))
        .map(|t| t.name.clone())
        .chain(lineage.nested_sources.iter().cloned())
        .filter_map(|n| {
            let parts: Vec<&str> = n.split('.').collect();
            match parts.as_slice() {
                [schema, table] => Some((fold_identifier(schema), fold_identifier(table))),
                _ => None,
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();

    let mut expected: Vec<(String, Option<Location>)> = Vec::new();
    let mut path: Vec<Location> = Vec::new();
    for read in &bare_reads {
        match index.bind(read, model) {
            // A binding the compile graph dropped (D013) is an external read.
            BareBinding::Model(producer) if !compiled_deps.contains(&producer) => {
                expected.push((read.clone(), None));
            }
            BareBinding::Model(producer) => {
                let Some(loc) = location_of.get(producer.as_str()) else {
                    // An ephemeral producer: the compiler inlined it, so no
                    // bare read of it is left to resolve.
                    continue;
                };
                if !path.contains(loc) {
                    path.push(loc.clone());
                }
                expected.push((read.clone(), Some(loc.clone())));
            }
            BareBinding::External => expected.push((read.clone(), None)),
            BareBinding::Ambiguous(candidates) => {
                return Err(format!(
                    "bare read of '{read}' is ambiguous between models {} (E056)",
                    candidates.join(", ")
                ));
            }
        }
    }
    let default_location = (fold_identifier(default_catalog), "main".to_string());
    if !path.contains(&default_location) {
        path.push(default_location);
    }

    let spelled = path
        .iter()
        .map(|(c, s)| format!("{}.{}", quote(c), quote(s)))
        .collect::<Vec<_>>()
        .join(",");
    db.execute_statement(&format!("SET search_path = {}", literal(&spelled)))
        .map_err(|e| format!("could not set the local search path: {e}"))?;

    for (read, want) in expected {
        let got = first_on_path(db, &path, &read)?;
        let ok = match (&want, &got) {
            (Some(want), Some(got)) => want == got,
            // External: fine unless it lands on a table this run writes.
            (None, Some(got)) => !written.contains(&(got.clone(), fold_identifier(&read))),
            (None, None) => true,
            (Some(_), None) => false,
        };
        if !ok {
            let show = |l: &Option<Location>| {
                l.as_ref().map_or_else(
                    || "no table".to_string(),
                    |(c, s)| format!("{c}.{s}.{read}"),
                )
            };
            return Err(format!(
                "bare read of '{read}' would reach {} locally, but the compiler bound it to {}. \
                 Qualify the read with its schema so both agree",
                show(&got),
                match &want {
                    Some(_) => show(&want),
                    None => "no model (an external table)".to_string(),
                }
            ));
        }
    }

    // A two-part `schema.table` read resolves in the first catalog on the
    // path that has that schema and table. Putting a producer's catalog on
    // the path must not move a read that, with the default path, reaches a
    // table in the default catalog: refuse instead of reading another table.
    let default_catalog = fold_identifier(default_catalog);
    let mut path_catalogs: Vec<&str> = Vec::new();
    for (catalog, _) in &path {
        if !path_catalogs.contains(&catalog.as_str()) {
            path_catalogs.push(catalog);
        }
    }
    for (schema, table) in two_part_reads {
        let holders = catalogs_holding(db, &schema, &table)?;
        let by_path = path_catalogs.iter().find(|c| holders.contains(**c));
        if holders.contains(&default_catalog) && by_path.is_some_and(|c| **c != default_catalog) {
            return Err(format!(
                "read of '{schema}.{table}' would reach {}.{schema}.{table} locally, where a \
                 bare read in the same model put that catalog on the search path; without \
                 it the read reaches {default_catalog}.{schema}.{table}. Qualify the read \
                 with its catalog so both agree",
                by_path.map_or("", |c| *c)
            ));
        }
    }
    Ok(())
}

/// The catalogs holding a table or view `schema.table`, folded.
fn catalogs_holding(
    db: &DuckDbConnector,
    schema: &str,
    table: &str,
) -> Result<HashSet<String>, String> {
    let r = db
        .execute_sql(&format!(
            "SELECT lower(database_name) FROM duckdb_tables() \
             WHERE lower(schema_name) = {s} AND lower(table_name) = {t} \
             UNION ALL \
             SELECT lower(database_name) FROM duckdb_views() \
             WHERE lower(schema_name) = {s} AND lower(view_name) = {t}",
            s = literal(schema),
            t = literal(table)
        ))
        .map_err(|e| format!("could not inspect local tables: {e}"))?;
    Ok(r.rows
        .iter()
        .filter_map(|row| row.first()?.as_str().map(str::to_string))
        .collect())
}

/// The first `(catalog, schema)` on `path` holding a table or view called
/// `name`, the way DuckDB's search path resolves a bare name.
fn first_on_path(
    db: &DuckDbConnector,
    path: &[Location],
    name: &str,
) -> Result<Option<Location>, String> {
    let r = db
        .execute_sql(&format!(
            "SELECT lower(database_name), lower(schema_name) FROM duckdb_tables() \
             WHERE lower(table_name) = {n} \
             UNION ALL \
             SELECT lower(database_name), lower(schema_name) FROM duckdb_views() \
             WHERE lower(view_name) = {n}",
            n = literal(&fold_identifier(name))
        ))
        .map_err(|e| format!("could not inspect local tables: {e}"))?;
    let holders: HashSet<Location> = r
        .rows
        .iter()
        .filter_map(|row| {
            Some((
                row.first()?.as_str()?.to_string(),
                row.get(1)?.as_str()?.to_string(),
            ))
        })
        .collect();
    Ok(path.iter().find(|loc| holders.contains(*loc)).cloned())
}

/// Create every valid user-defined function (`functions/`) as a DuckDB macro,
/// callees first. Returns `(function, error)` for each one that failed.
pub fn create_local_functions(
    compile_result: &CompileResult,
    db: &DuckDbConnector,
) -> Vec<(String, String)> {
    let registry = compile_result.semantic_graph.functions();
    let names: Vec<String> = registry.functions().map(|f| f.def.name.clone()).collect();
    let mut failed = Vec::new();
    for sig in registry.creation_order(names.iter().map(String::as_str)) {
        let created = rocky_core::functions::create_function_sql(
            &sig.def,
            rocky_core::functions::FunctionDialect::DuckDb,
        )
        .map_err(|e| e.to_string())
        .and_then(|sql| db.execute_statement(&sql).map_err(|e| e.to_string()));
        if let Err(e) = created {
            failed.push((sig.def.name.clone(), e));
        }
    }
    failed
}

/// Compile and execute a project locally.
pub fn compile_and_execute(models_dir: &Path) -> anyhow::Result<ExecutionResult> {
    let config = CompilerConfig {
        models_dir: models_dir.to_path_buf(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };

    let compile_result = rocky_compiler::compile::compile(&config)?;

    if compile_result.has_errors {
        anyhow::bail!("compilation has errors — cannot execute");
    }

    let db = DuckDbConnector::in_memory()?;
    Ok(execute_locally(&compile_result, &db))
}

#[cfg(test)]
mod tests {
    //! The #2045 shapes: each asserts what a consumer READS (or a named
    //! refusal), never where a table landed alone.

    use super::*;

    /// One model file pair. `extra` is appended to the sidecar verbatim.
    struct M<'a> {
        name: &'a str,
        sql: &'a str,
        target: (&'a str, &'a str, &'a str),
        extra: &'a str,
    }

    fn m<'a>(name: &'a str, sql: &'a str, target: (&'a str, &'a str, &'a str)) -> M<'a> {
        M {
            name,
            sql,
            target,
            extra: "",
        }
    }

    fn project(models: &[M<'_>]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for model in models {
            let (catalog, schema, table) = model.target;
            std::fs::write(
                dir.path().join(format!("{}.toml", model.name)),
                format!(
                    "name = \"{}\"\n{}\n[target]\ncatalog = \"{catalog}\"\nschema = \"{schema}\"\n\
                     table = \"{table}\"\n",
                    model.name, model.extra
                ),
            )
            .unwrap();
            std::fs::write(dir.path().join(format!("{}.sql", model.name)), model.sql).unwrap();
        }
        dir
    }

    /// Compile `models`, run `seed` (if any) and execute locally.
    fn run(models: &[M<'_>], seed: &str) -> (ExecutionResult, DuckDbConnector, CompileResult) {
        let dir = project(models);
        let config = CompilerConfig {
            models_dir: dir.path().to_path_buf(),
            ..Default::default()
        };
        let compiled = rocky_compiler::compile::compile(&config).unwrap();
        assert!(!compiled.has_errors, "{:?}", compiled.diagnostics);
        let db = DuckDbConnector::in_memory().unwrap();
        if !seed.is_empty() {
            db.execute_statement(seed).unwrap();
        }
        let result = execute_locally(&compiled, &db);
        (result, db, compiled)
    }

    fn value(db: &DuckDbConnector, table: &str) -> String {
        let r = db.execute_sql(&format!("SELECT v FROM {table}")).unwrap();
        assert_eq!(r.rows.len(), 1, "{table}: {:?}", r.rows);
        r.rows[0][0].as_str().unwrap().to_string()
    }

    fn failure<'r>(result: &'r ExecutionResult, model: &str) -> Option<&'r str> {
        result
            .failed
            .iter()
            .find(|(n, _)| n == model)
            .map(|(_, e)| e.as_str())
    }

    /// Shape 1: two catalogs, one `schema.table`. Both objects survive, and
    /// each consumer reads its own.
    #[test]
    fn two_catalogs_with_one_schema_table_stay_distinct() {
        let (result, db, _) = run(
            &[
                m("a", "SELECT 1 AS v", ("cat1", "s", "t")),
                m("b", "SELECT 2 AS v", ("cat2", "s", "t")),
                m("read_a", "SELECT v FROM cat1.s.t", ("cat1", "s", "read_a")),
                m("read_b", "SELECT v FROM cat2.s.t", ("cat1", "s", "read_b")),
            ],
            "",
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "cat1.s.read_a"), "1");
        assert_eq!(value(&db, "cat1.s.read_b"), "2");
    }

    /// Shape 2: an ephemeral model shares a real model's target. The real
    /// table is never overwritten; the ephemeral consumer reads the
    /// ephemeral's rows through the inlined CTE.
    #[test]
    fn an_ephemeral_model_never_overwrites_a_real_one() {
        let eph = M {
            name: "eph",
            sql: "SELECT 2 AS v",
            target: ("cat", "s", "t"),
            extra: "[strategy]\ntype = \"ephemeral\"",
        };
        let (result, db, _) = run(
            &[
                m("real", "SELECT 1 AS v", ("cat", "s", "t")),
                eph,
                m(
                    "reads_real",
                    "SELECT v FROM t",
                    ("cat", "out", "reads_real"),
                ),
                m(
                    "reads_eph",
                    "SELECT v FROM eph",
                    ("cat", "out", "reads_eph"),
                ),
            ],
            "",
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert!(result.skipped.iter().any(|(n, _)| n == "eph"), "{result:?}");
        assert_eq!(value(&db, "cat.s.t"), "1");
        assert_eq!(value(&db, "cat.out.reads_real"), "1");
        assert_eq!(value(&db, "cat.out.reads_eph"), "2");
    }

    /// #2316: an ephemeral model reads a real model by bare name. The
    /// consumer's inlined SQL carries that read, so it must resolve to the
    /// real model's table, in a catalog other than the default one.
    #[test]
    fn an_ephemeral_models_bare_read_reaches_the_model_the_compiler_bound() {
        let eph = M {
            name: "eph",
            sql: "SELECT v FROM base",
            target: ("t", "main", "eph"),
            extra: "[strategy]\ntype = \"ephemeral\"",
        };
        let (result, db, _) = run(
            &[
                m("base", "SELECT 7 AS v", ("t", "main", "base")),
                eph,
                m("use_eph", "SELECT v FROM eph", ("t", "main", "use_eph")),
            ],
            "",
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "t.main.use_eph"), "7");
    }

    /// #2316: the same read through a chain of two ephemeral models.
    #[test]
    fn a_bare_read_through_chained_ephemerals_reaches_the_bound_model() {
        let eph = |name, sql| M {
            name,
            sql,
            target: ("t", "main", name),
            extra: "[strategy]\ntype = \"ephemeral\"",
        };
        let (result, db, _) = run(
            &[
                m("base", "SELECT 7 AS v", ("t", "main", "base")),
                eph("eph_a", "SELECT v FROM base"),
                eph("eph_b", "SELECT v FROM eph_a"),
                m("use_eph", "SELECT v FROM eph_b", ("t", "main", "use_eph")),
            ],
            "",
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "t.main.use_eph"), "7");
    }

    /// Shape 3: the same table name in two schemas. The bare read reaches the
    /// model the compiler bound, whatever order the schemas sort in.
    #[test]
    fn a_bare_read_reaches_the_model_the_compiler_bound() {
        for (bound, other) in [("z", "a"), ("a", "z")] {
            let (result, db, _) = run(
                &[
                    m("events", "SELECT 2 AS v", ("cat", bound, "events")),
                    m("other", "SELECT 1 AS v", ("cat", other, "events")),
                    m(
                        "summary",
                        "SELECT v FROM events",
                        ("cat", "marts", "summary"),
                    ),
                ],
                "",
            );
            assert!(result.failed.is_empty(), "{:?}", result.failed);
            assert_eq!(value(&db, "cat.marts.summary"), "2", "{bound}/{other}");
        }
    }

    /// Shape 4: a producer whose target fails validation. Its consumer never
    /// runs on the stale seed table of the same name, and says why.
    #[test]
    fn a_consumer_never_runs_after_its_producer_failed() {
        let consumer = M {
            name: "consumer",
            sql: "SELECT v FROM source",
            target: ("memory", "out", "consumer"),
            extra: "depends_on = [\"source\"]",
        };
        let dir = project(&[
            m("source", "SELECT 42 AS v", ("memory", "bad-name", "source")),
            consumer,
        ]);
        let compiled = rocky_compiler::compile::compile(&CompilerConfig {
            models_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();
        let db = DuckDbConnector::in_memory().unwrap();
        db.execute_statement("CREATE TABLE main.source AS SELECT 7 AS v")
            .unwrap();
        let result = execute_locally(&compiled, &db);
        assert!(failure(&result, "source").is_some(), "{result:?}");
        let why = failure(&result, "consumer").expect("consumer must not succeed");
        assert!(
            why.contains("upstream 'source'"),
            "the consumer names its failed producer: {why}"
        );
        assert!(
            db.execute_sql("SELECT v FROM memory.out.consumer").is_err(),
            "nothing was built from the stale seed"
        );
    }

    /// Shape 5: a schema DuckDB cannot parse unquoted (`123stage`, `select`)
    /// fails at most its own model, never every model.
    #[test]
    fn an_awkward_schema_name_affects_only_its_own_model() {
        let (result, db, _) = run(
            &[
                m("digits", "SELECT 1 AS v", ("cat", "123stage", "digits")),
                m("reserved", "SELECT 2 AS v", ("cat", "select", "reserved")),
                m("plain", "SELECT 3 AS v", ("cat", "s", "plain")),
            ],
            "",
        );
        assert!(failure(&result, "plain").is_none(), "{result:?}");
        assert_eq!(value(&db, "cat.s.plain"), "3");
        // Quoted, both awkward names build too.
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "cat.\"123stage\".digits"), "1");
    }

    /// Shape 6: a bare read of a renamed-target model's NAME reaches no model
    /// locally, as on a warehouse — the model lands at its target, not under
    /// its name. The compiler says so (D012) and the reader fails on the
    /// missing table rather than reading the model.
    #[test]
    fn a_bare_read_of_a_renamed_targets_name_reaches_nothing() {
        let (result, db, compiled) = run(
            &[
                m("m", "SELECT 5 AS v", ("cat", "s", "renamed")),
                m("consumer", "SELECT v FROM m", ("cat", "s", "consumer")),
            ],
            "",
        );
        assert!(
            compiled.diagnostics.iter().any(|d| &*d.code == "D012"),
            "{:?}",
            compiled.diagnostics
        );
        assert_eq!(value(&db, "cat.s.renamed"), "5");
        let why = failure(&result, "consumer").expect("the read reaches no table");
        assert!(why.contains("Table with name m does not exist"), "{why}");
        assert!(db.execute_sql("SELECT v FROM cat.s.m").is_err());
    }

    /// Shape 7: the default schema spelled `Main`. Precedence is the same as
    /// for `main`, and the comparison folds case the way DuckDB does.
    #[test]
    fn a_schema_spelled_in_another_case_resolves_the_same() {
        for (schema_of_t, other) in [("Main", "zeta"), ("zeta", "Main")] {
            let (result, db, _) = run(
                &[
                    m("t", "SELECT 1 AS v", ("cat", schema_of_t, "t")),
                    m("t_other", "SELECT 2 AS v", ("cat", other, "t")),
                    m("reader", "SELECT v FROM t", ("cat", "out", "reader")),
                ],
                "",
            );
            assert!(result.failed.is_empty(), "{:?}", result.failed);
            assert_eq!(value(&db, "cat.out.reader"), "1", "{schema_of_t}/{other}");
        }
    }

    /// Two bare reads whose producers' schemas shadow each other: the first
    /// producer's schema also holds a table named like the second read. DuckDB
    /// would bind the second read to the wrong table, so the model is refused
    /// instead of run.
    #[test]
    fn a_bare_read_duckdb_would_bind_elsewhere_is_refused() {
        let (result, _db, _) = run(
            &[
                m("a", "SELECT 1 AS v", ("cat", "s1", "a")),
                m("b", "SELECT 2 AS v", ("cat", "s2", "b")),
                m("decoy", "SELECT 3 AS v", ("cat", "s1", "b")),
                m(
                    "reader",
                    "SELECT a.v + b.v AS v FROM a CROSS JOIN b",
                    ("cat", "out", "reader"),
                ),
            ],
            "",
        );
        let why = failure(&result, "reader").expect("the shadowed read is refused");
        assert!(
            why.contains("bare read of 'b' would reach cat.s1.b") && why.contains("cat.s2.b"),
            "{why}"
        );
    }

    /// A two-part read that the default path sends to the default catalog
    /// must not be redirected to an attached catalog because a bare read in
    /// the same model put it on the path. Refused, not run on another table.
    #[test]
    fn a_two_part_read_the_path_would_redirect_is_refused() {
        let (result, _db, _) = run(
            &[
                m("p", "SELECT 1 AS v", ("cat", "s", "t")),
                m(
                    "reader",
                    "SELECT t.v + seed.v AS v FROM t CROSS JOIN s.t AS seed",
                    ("cat", "out", "reader"),
                ),
            ],
            "CREATE SCHEMA memory.s; CREATE TABLE memory.s.t AS SELECT 7 AS v",
        );
        let why = failure(&result, "reader").expect("the redirected read is refused");
        assert!(
            why.contains("'s.t' would reach cat.s.t") && why.contains("memory.s.t"),
            "{why}"
        );
    }

    /// A failed upstream withholds the readers of an ephemeral model that
    /// reads it, though the ephemeral model itself never runs.
    #[test]
    fn a_failure_propagates_through_an_ephemeral_model() {
        let eph = M {
            name: "eph",
            sql: "SELECT v FROM cat.s.u",
            target: ("cat", "s", "eph"),
            extra: "depends_on = [\"u\"]\n[strategy]\ntype = \"ephemeral\"",
        };
        let reader = M {
            name: "reader",
            sql: "SELECT v FROM eph",
            target: ("cat", "s", "reader"),
            extra: "",
        };
        let (result, db, _) = run(
            &[m("u", "SELECT 1/'x' AS v", ("cat", "s", "u")), eph, reader],
            "",
        );
        assert!(failure(&result, "u").is_some(), "{result:?}");
        let why = failure(&result, "reader").expect("the reader is withheld");
        assert!(why.contains("upstream 'eph'"), "{why}");
        assert!(db.execute_sql("SELECT v FROM cat.s.reader").is_err());
    }

    /// A catalog DuckDB reserves is refused for the models that target it,
    /// with the fix; `memory` already exists and is reused.
    #[test]
    fn a_reserved_catalog_is_refused_and_an_existing_one_reused() {
        let (result, db, _) = run(
            &[
                m("in_main", "SELECT 1 AS v", ("main", "s", "in_main")),
                m("in_memory", "SELECT 2 AS v", ("memory", "s", "in_memory")),
            ],
            "",
        );
        let why = failure(&result, "in_main").expect("main is reserved");
        assert!(
            why.contains("catalog 'main'") && why.contains("rocky.toml"),
            "{why}"
        );
        // `system` exists in DuckDB but is no database a model may write.
        let (result, _db, _) = run(&[m("in_system", "SELECT 1 AS v", ("system", "s", "x"))], "");
        let why = failure(&result, "in_system").expect("system is reserved");
        assert!(why.contains("catalog 'system'"), "{why}");
        assert_eq!(value(&db, "memory.s.in_memory"), "2");
    }

    /// A catalog that already resolves is reused, not attached again: data
    /// already in it survives, and no refusal is raised.
    #[test]
    fn a_catalog_that_already_resolves_is_reused_not_reattached() {
        let dir = project(&[m("kept", "SELECT 2 AS v", ("pre", "s", "kept"))]);
        let compiled = rocky_compiler::compile::compile(&CompilerConfig {
            models_dir: dir.path().to_path_buf(),
            ..Default::default()
        })
        .unwrap();
        let db = DuckDbConnector::in_memory().unwrap();
        db.execute_statement("ATTACH ':memory:' AS pre").unwrap();
        db.execute_statement("CREATE TABLE pre.main.marker AS SELECT 9 AS v")
            .unwrap();
        let result = execute_locally(&compiled, &db);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "pre.s.kept"), "2");
        // A second ATTACH would have failed or replaced the database.
        assert_eq!(value(&db, "pre.main.marker"), "9");
    }

    /// A model that reads another model's target by its qualified name runs
    /// after it — the local order includes the physical-read edges — and a
    /// failed producer withholds that reader too.
    #[test]
    fn qualified_reads_of_a_model_target_are_ordered_and_withheld() {
        let (result, db, _) = run(
            &[
                m("zz_producer", "SELECT 4 AS v", ("cat", "s", "p")),
                m("aa_reader", "SELECT v FROM cat.s.p", ("cat", "s", "r")),
            ],
            "",
        );
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(value(&db, "cat.s.r"), "4");

        let (result, _db, _) = run(
            &[
                m("zz_producer", "SELECT 1/'x' AS v", ("cat", "s", "p")),
                m("aa_reader", "SELECT v FROM cat.s.p", ("cat", "s", "r")),
            ],
            "",
        );
        assert!(failure(&result, "zz_producer").is_some(), "{result:?}");
        assert!(
            failure(&result, "aa_reader").is_some_and(|w| w.contains("upstream 'zz_producer'")),
            "{result:?}"
        );
    }
}
