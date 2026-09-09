//! Local SQL execution via DuckDB.
//!
//! Executes compiled models locally against a DuckDB instance,
//! either with sampled data or in-memory test data.

use std::collections::HashMap;
use std::path::Path;

use rocky_compiler::compile::{CompileResult, CompilerConfig};
use rocky_duckdb::DuckDbConnector;
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
/// Models are executed in DAG layer order. Each model's compiled SQL
/// is run against the DuckDB instance.
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

    // Every schema any selected model writes into, created up front and put on
    // the search path. Both halves are load-bearing; see
    // [`prepare_target_schemas`].
    if let Err(e) = prepare_target_schemas(compile_result, db) {
        // One failure here breaks every model, so report it against each
        // rather than silently materializing into the wrong place.
        for layer in &compile_result.project.layers {
            for model_name in layer {
                result.failed.push((model_name.clone(), e.clone()));
            }
        }
        return result;
    }

    for layer in &compile_result.project.layers {
        for model_name in layer {
            if let Some(model) = compile_result.project.model(model_name) {
                let relation = match local_relation(
                    &model.config.name,
                    &model.config.target.schema,
                    &model.config.target.table,
                ) {
                    Ok(r) => r,
                    Err(e) => {
                        result.failed.push((model_name.clone(), e));
                        continue;
                    }
                };
                // `relation` is validated component by component above;
                // `model.sql` is compiler-emitted SQL.
                let exec_sql = format!("CREATE OR REPLACE TABLE {relation} AS\n{}", model.sql);

                match db.execute_statement(&exec_sql) {
                    Ok(()) => {
                        info!(
                            model = model_name.as_str(),
                            relation = relation.as_str(),
                            "model executed locally"
                        );
                        result.succeeded.push(model_name.clone());
                    }
                    Err(e) => {
                        result.failed.push((model_name.clone(), e.to_string()));
                    }
                }
            }
        }
    }

    result
}

/// The relation a model materializes into locally: its configured
/// `schema.table`, defaulting the schema to [`DEFAULT_LOCAL_SCHEMA`].
///
/// # Always qualified, including the default
///
/// An UNqualified `CREATE TABLE` follows `search_path` and lands in its first
/// entry. Since [`prepare_target_schemas`] puts every target schema on that
/// path, a model that configures no schema would be created inside whichever
/// schema happened to sort first — some other model's. Naming `main`
/// explicitly is what keeps "no configured schema" meaning the default schema
/// rather than an arbitrary one.
///
/// # The catalog is deliberately dropped
///
/// DuckDB resolves a three-part name only against an `ATTACH`-ed database, and
/// nothing here attaches one, so including the catalog would fail every model
/// in a project that names one — which is most of them. Local execution
/// reproduces the target's schema and table, not its catalog. A model whose
/// SQL *reads* a three-part name is unaffected and still does not resolve
/// locally, exactly as before.
///
/// # On the validation
///
/// Each component is checked separately, and this is defence in depth rather
/// than a reachable path: model loading already refuses an unsubstituted
/// `${VAR}` in a sidecar (`ModelLoadError::EnvSubstitution`), so a placeholder
/// target never reaches a compile result. The check stays because this builds
/// SQL by interpolation and the cost of being wrong there is not a broken
/// table name.
fn local_relation(name: &str, schema: &str, table: &str) -> Result<String, String> {
    let table = if table.is_empty() { name } else { table };
    validate_identifier(table).map_err(|e| e.to_string())?;

    let schema = if schema.is_empty() {
        DEFAULT_LOCAL_SCHEMA
    } else {
        schema
    };
    validate_identifier(schema).map_err(|e| e.to_string())?;
    Ok(format!("{schema}.{table}"))
}

/// DuckDB's own default schema, and where a model that configures none lands.
const DEFAULT_LOCAL_SCHEMA: &str = "main";

/// Create every schema the selected models write into, then put them all on
/// the connection's `search_path`.
///
/// # Why both halves
///
/// Materializing at the configured `schema.table` is what makes local
/// execution agree with a warehouse run about *which object* a model writes
/// (#1354 step 1). Before this, every model was `CREATE OR REPLACE TABLE
/// <model name>`, so three models targeting `account_acme.events`,
/// `account_beta.events` and `account_ceres.events` — the `10-route-by-tenant`
/// shape — all wrote one table and silently clobbered each other. Rocky's own
/// `E036` does not catch that, because it keys on the full
/// `catalog.schema.table` identity and those three are genuinely distinct
/// objects.
///
/// But moving the tables out of the default schema alone would break the
/// dominant idiom: 24 of the in-repo example projects layer their models
/// across two or more schemas (`staging` → `marts`), and read each other by
/// **bare name**. `FROM stg_orders` searches only the current schema, so a
/// consumer in `marts` would stop seeing a producer in `staging`.
///
/// The search path is what reconciles them. With every target schema on it, a
/// bare read resolves to a physical table again — which is precisely the
/// resolution rule #1354 wants local execution to honour.
///
/// # The ambiguity this does not resolve
///
/// When two schemas on the path hold a table of the same name, DuckDB takes
/// the first one and says nothing. That is not a gap being papered over: it is
/// what a warehouse does with an unqualified read, so local execution now
/// mirrors it instead of hiding it behind one flat namespace. Which model a
/// bare read binds to when two could answer is #1632, and it belongs there
/// rather than in an executor that would have to invent an answer.
///
/// Order is sorted, so a project that hits the ambiguity gets the same answer
/// on every run rather than one that shifts with hash iteration order.
fn prepare_target_schemas(
    compile_result: &CompileResult,
    db: &DuckDbConnector,
) -> Result<(), String> {
    let mut schemas: Vec<&str> = compile_result
        .project
        .models
        .iter()
        .map(|m| m.config.target.schema.as_str())
        .filter(|s| !s.is_empty() && validate_identifier(s).is_ok())
        .collect();
    schemas.sort_unstable();
    schemas.dedup();

    for schema in &schemas {
        db.execute_statement(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
            .map_err(|e| format!("failed to create local schema '{schema}': {e}"))?;
    }

    if schemas.is_empty() {
        return Ok(());
    }

    // The default schema stays on the path so seeds, fixtures and any model
    // without a configured schema keep resolving. It goes LAST: a configured
    // target outranks whatever happens to share its name in the default
    // schema.
    let path = schemas
        .iter()
        .copied()
        .filter(|s| *s != DEFAULT_LOCAL_SCHEMA)
        .chain(std::iter::once(DEFAULT_LOCAL_SCHEMA))
        .collect::<Vec<_>>()
        .join(",");
    db.execute_statement(&format!("SET search_path = '{path}'"))
        .map_err(|e| format!("failed to set the local search path to '{path}': {e}"))
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
    use std::io::Write;

    use super::*;

    /// Write a project whose models are `(name, schema, table, sql)`, compile
    /// it, and execute it locally. Returns the DuckDB handle so a test can ask
    /// the database what actually landed, rather than trusting the SQL string.
    fn run_project(
        models: &[(&str, &str, &str, &str)],
    ) -> (tempfile::TempDir, DuckDbConnector, ExecutionResult) {
        let tmp = tempfile::tempdir().unwrap();
        let models_dir = tmp.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        for (name, schema, table, sql) in models {
            let mut f = std::fs::File::create(models_dir.join(format!("{name}.sql"))).unwrap();
            write!(f, "{sql}").unwrap();
            let mut t = std::fs::File::create(models_dir.join(format!("{name}.toml"))).unwrap();
            write!(
                t,
                "name = \"{name}\"\n\n[target]\ncatalog = \"demo\"\nschema = \"{schema}\"\ntable = \"{table}\"\n"
            )
            .unwrap();
        }

        let config = CompilerConfig {
            models_dir: models_dir.clone(),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            ..Default::default()
        };
        let compile_result = rocky_compiler::compile::compile(&config).unwrap();
        let db = DuckDbConnector::in_memory().unwrap();
        let result = execute_locally(&compile_result, &db);
        (tmp, db, result)
    }

    fn scalar(db: &DuckDbConnector, sql: &str) -> Option<String> {
        db.execute_sql(sql)
            .ok()
            .and_then(|r| r.rows.first().and_then(|row| row.first().cloned()))
            .map(|v| match v {
                serde_json::Value::String(s) => s,
                other => other.to_string(),
            })
    }

    /// #1354 step 1: a model materializes at its CONFIGURED target, not at its
    /// own name.
    ///
    /// Asserted on the database, not on the SQL: the table has to be findable
    /// at `staging.orders_v2`, and NOT at `orders` — the name the old
    /// `CREATE OR REPLACE TABLE <model name>` would have used.
    #[test]
    fn a_model_materializes_at_its_configured_schema_and_table() {
        let (_tmp, db, result) =
            run_project(&[("orders", "staging", "orders_v2", "SELECT 7 AS id")]);
        assert!(result.failed.is_empty(), "{:?}", result.failed);

        assert_eq!(
            scalar(&db, "SELECT id FROM staging.orders_v2").as_deref(),
            Some("7"),
            "the model must land at its configured target"
        );
        assert!(
            db.execute_sql("SELECT id FROM main.orders").is_err(),
            "nothing may be left at the model's NAME in the default schema; that \
             equivalence is exactly what #1354 is about"
        );
    }

    /// The defect this closes, on the shape that exhibits it in-repo
    /// (`10-route-by-tenant`): three models with distinct names and distinct
    /// target schemas, all writing a table called `events`.
    ///
    /// Under `CREATE OR REPLACE TABLE <model name>` these were three separate
    /// tables and the collision was invisible. Under a naive
    /// `CREATE OR REPLACE TABLE <target table>` they would be ONE table and
    /// silently clobber — and `E036` does not fire, because it keys on the
    /// full `catalog.schema.table` and these three are genuinely different
    /// objects. Only the schema-qualified form is correct.
    #[test]
    fn three_tenants_writing_one_table_name_land_in_three_schemas() {
        let (_tmp, db, result) = run_project(&[
            ("events_acme", "account_acme", "events", "SELECT 1 AS id"),
            ("events_beta", "account_beta", "events", "SELECT 2 AS id"),
            ("events_ceres", "account_ceres", "events", "SELECT 3 AS id"),
        ]);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(result.succeeded.len(), 3);

        for (schema, expected) in [
            ("account_acme", "1"),
            ("account_beta", "2"),
            ("account_ceres", "3"),
        ] {
            assert_eq!(
                scalar(&db, &format!("SELECT id FROM {schema}.events")).as_deref(),
                Some(expected),
                "{schema}.events must hold its OWN tenant's row; one shared table \
                 would leave whichever model ran last"
            );
        }
    }

    /// The other half, and the reason the schema move alone is not enough.
    ///
    /// The dominant idiom in the example projects is a layered read by BARE
    /// name — `staging.stg_orders` consumed as `FROM stg_orders` from a model
    /// targeting `marts`. Moving tables out of the default schema breaks that
    /// unless every target schema is on the search path. Deleting the
    /// `SET search_path` fails here and nowhere else.
    #[test]
    fn a_bare_read_still_reaches_a_producer_in_another_schema() {
        let (_tmp, db, result) = run_project(&[
            ("stg_orders", "staging", "stg_orders", "SELECT 5 AS amount"),
            (
                "fct_revenue",
                "marts",
                "fct_revenue",
                "SELECT SUM(amount) AS total FROM stg_orders",
            ),
        ]);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(
            scalar(&db, "SELECT total FROM marts.fct_revenue").as_deref(),
            Some("5"),
            "a consumer in one schema must still resolve a bare read of a producer \
             in another; without the search path this model cannot compile at all"
        );
    }

    /// A model with no configured schema keeps landing in the default one, so
    /// `main` has to stay on the path for the mixed case to work at all.
    #[test]
    fn a_model_without_a_schema_still_lands_in_the_default_one() {
        let (_tmp, db, result) = run_project(&[
            ("raw_orders", "", "raw_orders", "SELECT 2 AS id"),
            (
                "stg_orders",
                "staging",
                "stg_orders",
                "SELECT id FROM raw_orders",
            ),
        ]);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert_eq!(
            scalar(&db, "SELECT id FROM main.raw_orders").as_deref(),
            Some("2")
        );
        assert_eq!(
            scalar(&db, "SELECT id FROM staging.stg_orders").as_deref(),
            Some("2"),
            "a schema-qualified model must still read an unqualified one"
        );
    }

    /// `local_relation` refuses a target it cannot safely interpolate.
    ///
    /// Driven directly rather than through a project, because it is NOT
    /// reachable that way: model loading refuses an unsubstituted `${VAR}` in
    /// a sidecar before a compile result exists. Said here so the guard is not
    /// mistaken for a live path — it is defence in depth on a function that
    /// builds SQL by interpolation.
    #[test]
    fn local_relation_refuses_a_target_it_cannot_interpolate() {
        assert!(
            local_relation("bad", "staging", "${ROCKY_TABLE_OVERRIDE}").is_err(),
            "a target table that is not a valid identifier must not reach the SQL string"
        );
        assert!(
            local_relation("bad", "${ROCKY_SCHEMA}", "bad").is_err(),
            "nor a target schema"
        );
        assert_eq!(
            local_relation("orders", "", "").as_deref(),
            Ok("main.orders"),
            "an unconfigured target falls back to the model name in the default schema"
        );
    }

    /// The search path is what makes a bare read work, and it is also what
    /// makes an unqualified CREATE land in the wrong place — so the relation
    /// is always fully qualified.
    ///
    /// Dropping `main` from `local_relation` makes this fail: `raw_orders`
    /// would be created in `staging`, the first entry on the path, because it
    /// sorts before the default schema.
    #[test]
    fn a_model_with_no_schema_is_not_captured_by_another_models_schema() {
        let (_tmp, db, result) = run_project(&[
            ("raw_orders", "", "raw_orders", "SELECT 2 AS id"),
            ("other", "aaa_first_on_path", "other", "SELECT 1 AS id"),
        ]);
        assert!(result.failed.is_empty(), "{:?}", result.failed);
        assert!(
            db.execute_sql("SELECT id FROM aaa_first_on_path.raw_orders")
                .is_err(),
            "a model with no configured schema must not be created inside the \
             schema that happens to sort first on the search path"
        );
        assert_eq!(
            scalar(&db, "SELECT id FROM main.raw_orders").as_deref(),
            Some("2")
        );
    }
}
