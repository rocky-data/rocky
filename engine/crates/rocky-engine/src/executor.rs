//! Local SQL execution via DuckDB.
//!
//! Executes compiled models locally against a DuckDB instance,
//! either with sampled data or in-memory test data.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_core::models::Model;
use rocky_duckdb::DuckDbConnector;
use rocky_sql::defer::{
    DeferTarget, IdentifierCaseRules, RecursiveCteVisibility, qualify_deferred_refs,
};
use rocky_sql::validation::validate_identifier;
use tracing::info;

/// Result of local execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Models executed successfully.
    pub succeeded: Vec<String>,
    /// Models that failed (name, error).
    pub failed: Vec<(String, String)>,
}

/// Execute compiled models locally using DuckDB.
///
/// Models are executed in DAG layer order. Each model materializes at its
/// configured target, `catalog.schema.table`, not under its own name (#1354
/// step 1): the local run writes the object a warehouse run writes.
///
/// # Catalogs are attached in-memory databases
///
/// DuckDB names a catalog by an attached database, the way the DuckDB adapter
/// does on `rocky run` (`rocky_duckdb::dialect::catalog_name_for_path`). So
/// each target catalog is `ATTACH ':memory:' AS <catalog>`, and two models
/// that differ only by catalog stay two tables (#2044, decision 1). An empty
/// catalog is the connection's default catalog, which
/// [`prepare_local_catalogs`] sets. A catalog DuckDB reserves is refused for
/// that model; see [`RESERVED_CATALOGS`].
///
/// # A consumer reads through the compiler's binding
///
/// A model reads an upstream model by bare name (`FROM orders`). The compiler
/// binds that name to model `orders` and derives the edge. Here the read is
/// rewritten to that model's local relation, with the same rewrite
/// `rocky run --defer` uses, so the consumer reads exactly the model the
/// compiler bound. Nothing else in the SQL changes: a qualified or external
/// read resolves in the connection's default catalog and schema, where
/// `data/seed.sql` writes. No list of schemas is searched, so no schema order
/// can pick a different producer (#2045, shape 3).
///
/// # A consumer of a failed model does not run
///
/// When a model fails, every model that depends on it is reported failed and
/// not run, naming the upstream and its error. A consumer that ran anyway
/// could read a stale table at the same relation, such as a seed (#2045,
/// shape 4).
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
    };
    let project = &compile_result.project;

    let default_catalog = match default_catalog(db) {
        Ok(catalog) => catalog,
        Err(e) => {
            // Without the default catalog no relation can be named, so report
            // the cause against every model rather than guess one.
            for layer in &project.layers {
                for model_name in layer {
                    result.failed.push((model_name.clone(), e.clone()));
                }
            }
            return result;
        }
    };

    let relations: HashMap<&str, Result<LocalRelation, String>> = project
        .models
        .iter()
        .map(|m| (m.config.name.as_str(), local_relation(m, &default_catalog)))
        .collect();
    let depends_on: HashMap<&str, &[String]> = project
        .dag_nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();
    // Failed model name → its error, so a consumer can name both.
    let mut failed: HashMap<String, String> = HashMap::new();
    // `catalog.schema` pairs already attached and created, folded to lower
    // case as DuckDB folds them, so each costs its statements once.
    let mut prepared: HashSet<String> = HashSet::new();

    for layer in &project.layers {
        for model_name in layer {
            let Some(model) = project.model(model_name) else {
                continue;
            };
            let upstreams = depends_on.get(model_name.as_str()).copied().unwrap_or(&[]);
            let outcome = match upstreams
                .iter()
                .find_map(|up| failed.get(up).map(|e| (up, e)))
            {
                Some((up, e)) => Err(format!(
                    "not run: upstream model '{up}' failed, so this model could only read \
                     stale or missing data: {e}"
                )),
                None => materialize(db, model, upstreams, &relations, &mut prepared),
            };
            match outcome {
                Ok(relation) => {
                    info!(
                        model = model_name.as_str(),
                        relation = relation.as_str(),
                        "model executed locally"
                    );
                    result.succeeded.push(model_name.clone());
                }
                Err(e) => {
                    failed.insert(model_name.clone(), e.clone());
                    result.failed.push((model_name.clone(), e));
                }
            }
        }
    }

    result
}

/// Catalog names DuckDB reserves, compared case-insensitively.
///
/// `main` and `temp` fail to attach. `memory` is the in-memory default catalog
/// and `system` is DuckDB's own; `ATTACH IF NOT EXISTS` would silently reuse
/// them, so a model's table would land beside unrelated objects. A case
/// variant is refused too: DuckDB attaches `Main`, and then every `main.x`
/// read in the session fails as ambiguous.
pub const RESERVED_CATALOGS: [&str; 4] = ["main", "memory", "system", "temp"];

/// Where a model materializes locally. Each part passed
/// [`rocky_sql::validation::validate_identifier`], the rule the DuckDB
/// adapter's `format_table_ref` applies on `rocky run`.
#[derive(Debug, Clone)]
struct LocalRelation {
    catalog: String,
    /// `false` for the connection's default catalog, which exists already.
    attach: bool,
    schema: String,
    table: String,
}

impl LocalRelation {
    fn render(&self) -> String {
        format!("{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// The connection's default catalog: `memory` for an in-memory database.
fn default_catalog(db: &DuckDbConnector) -> Result<String, String> {
    let rows = db
        .execute_sql("SELECT current_database()")
        .map_err(|e| format!("could not read the local default catalog: {e}"))?
        .rows;
    match rows.first().and_then(|row| row.first()) {
        Some(serde_json::Value::String(name)) => Ok(name.clone()),
        other => Err(format!(
            "could not read the local default catalog: current_database() returned {other:?}"
        )),
    }
}

/// The local relation for `model`'s configured target.
///
/// Always three parts. An empty catalog becomes the default catalog, written
/// out: a two-part `schema.table` is ambiguous in DuckDB when an attached
/// catalog shares the schema's name.
fn local_relation(model: &Model, default_catalog: &str) -> Result<LocalRelation, String> {
    let target = &model.config.target;
    let invalid = |part: &str, value: &str, e: rocky_sql::validation::ValidationError| {
        format!(
            "target {part} '{value}' is not a valid identifier ({e}); `rocky test` \
             materializes each model at its configured target and cannot name this one"
        )
    };
    validate_identifier(&target.schema).map_err(|e| invalid("schema", &target.schema, e))?;
    validate_identifier(&target.table).map_err(|e| invalid("table", &target.table, e))?;
    if target.catalog.is_empty() {
        return Ok(LocalRelation {
            catalog: default_catalog.to_string(),
            attach: false,
            schema: target.schema.clone(),
            table: target.table.clone(),
        });
    }
    validate_identifier(&target.catalog).map_err(|e| invalid("catalog", &target.catalog, e))?;
    if RESERVED_CATALOGS
        .iter()
        .any(|reserved| reserved.eq_ignore_ascii_case(&target.catalog))
    {
        return Err(format!(
            "target catalog '{}' is a name DuckDB reserves ({}). `rocky test` emulates \
             each target catalog as an attached in-memory DuckDB database and cannot \
             attach one under this name. Rename the catalog in this model's [target]",
            target.catalog,
            RESERVED_CATALOGS.join(", ")
        ));
    }
    Ok(LocalRelation {
        catalog: target.catalog.clone(),
        attach: true,
        schema: target.schema.clone(),
        table: target.table.clone(),
    })
}

/// Attach every target catalog, and make a project's only catalog the
/// connection's default one.
///
/// Call this on a fresh connection, before anything else writes to it.
/// `data/seed.sql` may create `poc.raw.orders`, which needs catalog `poc`.
///
/// # One catalog is the default catalog
///
/// The DuckDB adapter's database file IS the catalog on `rocky run`
/// (`rocky_duckdb::dialect::catalog_name_for_path`), so there a read without
/// a catalog — `staging.orders`, or a seed's `CREATE TABLE raw.orders` —
/// resolves inside it. When every model that names a catalog names the same
/// one, this does `USE <catalog>` so the local run resolves those the same way.
/// With two or more catalogs no single default is right. The connection keeps
/// its in-memory default, and a read without a catalog into one of them fails
/// rather than picks one.
///
/// A catalog that fails to attach here is left for its model to report:
/// [`execute_locally`] attaches each model's catalog again.
pub fn prepare_local_catalogs(compile_result: &CompileResult, db: &DuckDbConnector) {
    let Ok(default_catalog) = default_catalog(db) else {
        return;
    };
    let mut named: Vec<String> = Vec::new();
    for model in &compile_result.project.models {
        if let Ok(relation) = local_relation(model, &default_catalog)
            && relation.attach
            && attach(db, &relation.catalog).is_ok()
            && !named
                .iter()
                .any(|c| c.eq_ignore_ascii_case(&relation.catalog))
        {
            named.push(relation.catalog);
        }
    }
    if let [only] = named.as_slice() {
        // `only` passed `validate_identifier` and attached.
        let _ = db.execute_statement(&format!("USE {only}"));
    }
}

fn attach(db: &DuckDbConnector, catalog: &str) -> Result<(), String> {
    // `catalog` passed `validate_identifier` and is not a reserved name.
    db.execute_statement(&format!("ATTACH IF NOT EXISTS ':memory:' AS {catalog}"))
        .map_err(|e| format!("failed to attach local catalog '{catalog}': {e}"))
}

/// Materialize one model at its local relation and return the relation.
fn materialize(
    db: &DuckDbConnector,
    model: &Model,
    upstreams: &[String],
    relations: &HashMap<&str, Result<LocalRelation, String>>,
    prepared: &mut HashSet<String>,
) -> Result<String, String> {
    let relation = match relations.get(model.config.name.as_str()) {
        Some(Ok(relation)) => relation,
        Some(Err(e)) => return Err(e.clone()),
        None => return Err("no local relation was computed for this model".to_string()),
    };
    let schema = format!("{}.{}", relation.catalog, relation.schema);
    if !prepared.contains(&schema.to_lowercase()) {
        if relation.attach {
            attach(db, &relation.catalog)?;
        }
        db.execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .map_err(|e| format!("failed to create local schema '{schema}': {e}"))?;
        prepared.insert(schema.to_lowercase());
    }

    let sql = bind_upstream_reads(&local_model_sql(model), upstreams, relations)?;
    let rendered = relation.render();
    // Every part of `rendered` was validated; `sql` is compiler-emitted SQL
    // with upstream reads qualified to validated relations.
    db.execute_statement(&format!("CREATE OR REPLACE TABLE {rendered} AS\n{sql}"))
        .map_err(|e| e.to_string())?;
    Ok(rendered)
}

/// Rewrite each bare read of an upstream model to that model's local relation.
///
/// `upstreams` is the model's compiled dependency list. A bare name that
/// matches an upstream model's name is that model — the compiler's binding
/// (`rocky_compiler::resolve::classify_table_ref`) — and a CTE of the same
/// name in scope hides it. The rewrite is `rocky_sql::defer::qualify_deferred_refs`,
/// with DuckDB's case rules. A model with no upstream model keeps its SQL
/// text exactly.
fn bind_upstream_reads(
    sql: &str,
    upstreams: &[String],
    relations: &HashMap<&str, Result<LocalRelation, String>>,
) -> Result<String, String> {
    let bound: HashMap<String, DeferTarget> = upstreams
        .iter()
        .filter_map(|up| match relations.get(up.as_str()) {
            Some(Ok(r)) => Some((
                up.clone(),
                DeferTarget {
                    catalog: r.catalog.clone(),
                    schema: r.schema.clone(),
                    table: r.table.clone(),
                    quote_style: None,
                },
            )),
            // An upstream without a relation failed, and a failed upstream
            // stops its consumers before they get here. A name that is not a
            // model has nothing to bind to.
            Some(Err(_)) | None => None,
        })
        .collect();
    let outcome = qualify_deferred_refs(
        sql,
        &bound,
        IdentifierCaseRules::uniform(false),
        RecursiveCteVisibility::PrecedingAndSelf,
    )
    .map_err(|e| {
        format!(
            "could not bind this model's reads of its upstream models: its SQL did not \
             parse ({e})"
        )
    })?;
    // Filled only under Snowflake's upper-casing rule, which DuckDB's rules
    // do not set. Refused rather than trusted if that ever changes.
    if !outcome.setting_dependent_refs.is_empty() {
        return Err(format!(
            "cannot tell whether {:?} read a CTE or the upstream model of that name",
            outcome.setting_dependent_refs
        ));
    }
    Ok(outcome.sql)
}

/// The SQL a local run executes for `model`.
///
/// A `time_interval` model carries `@start_date` / `@end_date` (E024). A local
/// run selects no partition, so it substitutes one fixed wide window through
/// the same function `rocky run` uses (#2020). Other models run as compiled.
pub(crate) fn local_model_sql(model: &Model) -> String {
    if matches!(
        model.config.strategy,
        rocky_core::models::StrategyConfig::TimeInterval { .. }
    ) {
        rocky_core::sql_gen::substitute_wide_test_window(&model.sql)
    } else {
        model.sql.clone()
    }
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
    prepare_local_catalogs(&compile_result, &db);
    Ok(execute_locally(&compile_result, &db))
}
