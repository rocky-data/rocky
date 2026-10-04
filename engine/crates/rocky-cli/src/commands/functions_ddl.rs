//! Materializing user-defined functions (`functions/`) — shared by
//! `rocky run` (which executes the DDL) and `rocky plan` (which previews it).
//!
//! A function is created when at least one model the invocation would build
//! calls it, directly or through another function. Functions are created
//! before any model runs, callees first, so every dependent model finds them.

use anyhow::Result;

use rocky_compiler::compile::CompileResult;
use rocky_compiler::diagnostic::E051;
use rocky_core::functions::{FunctionDialect, create_function_sql};

/// One `CREATE OR REPLACE` statement for one function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FunctionStatement {
    /// The function's name as declared.
    pub name: String,
    /// `[catalog.][schema.]name`, for display.
    pub target: String,
    pub sql: String,
}

/// The function statements a run of the models accepted by `selected` needs,
/// in creation order.
///
/// `dialect_name` is [`rocky_core::traits::SqlDialect::name`].
///
/// # Errors
///
/// An `[E051]`-prefixed error when a needed function cannot be created on
/// this warehouse (Trino, a BigQuery function without a dataset, an adapter
/// with no function DDL).
pub(crate) fn function_statements(
    compile_result: &CompileResult,
    selected: impl Fn(&str) -> bool,
    dialect_name: &str,
) -> Result<Vec<FunctionStatement>> {
    let registry = compile_result.semantic_graph.functions();
    if registry.is_empty() {
        return Ok(Vec::new());
    }
    let usage = rocky_compiler::udf::function_usage(&compile_result.project.models, registry);
    let needed: Vec<&str> = usage
        .iter()
        .filter(|(_, callers)| callers.iter().any(|model| selected(model)))
        .map(|(name, _)| name.as_str())
        .collect();
    statements_for(compile_result, needed, dialect_name)
}

/// Statements for the named functions (and the functions they call).
pub(crate) fn statements_for<'a>(
    compile_result: &CompileResult,
    names: impl IntoIterator<Item = &'a str>,
    dialect_name: &str,
) -> Result<Vec<FunctionStatement>> {
    let registry = compile_result.semantic_graph.functions();
    let order = registry.creation_order(names);
    if order.is_empty() {
        return Ok(Vec::new());
    }
    let Some(dialect) = FunctionDialect::from_dialect_name(dialect_name) else {
        anyhow::bail!(
            "[{E051}] the '{dialect_name}' adapter has no support for creating user-defined \
             functions, but models call: {}",
            order
                .iter()
                .map(|s| s.def.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        );
    };
    order
        .into_iter()
        .map(|sig| {
            let sql = create_function_sql(&sig.def, dialect)
                .map_err(|e| anyhow::anyhow!("[{E051}] {e}"))?;
            let target = [
                sig.def.config.target.catalog.as_deref(),
                sig.def.config.target.schema.as_deref(),
                Some(sig.def.name.as_str()),
            ]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join(".");
            Ok(FunctionStatement {
                name: sig.def.name.clone(),
                target,
                sql,
            })
        })
        .collect()
}

/// Execute `statements` in order and return the functions that could not
/// be created, with the error. A function whose callee failed is not
/// attempted; it fails too.
pub(crate) async fn create_functions(
    warehouse: &dyn rocky_core::traits::WarehouseAdapter,
    compile_result: &CompileResult,
    statements: &[FunctionStatement],
) -> std::collections::BTreeMap<String, String> {
    let registry = compile_result.semantic_graph.functions();
    let mut failed = std::collections::BTreeMap::new();
    for stmt in statements {
        let blocked_by = registry.get(&stmt.name).and_then(|sig| {
            sig.calls
                .iter()
                .find(|callee| failed.contains_key(callee.as_str()))
                .cloned()
        });
        if let Some(callee) = blocked_by {
            failed.insert(
                stmt.name.clone(),
                format!("not created: it calls '{callee}', which failed"),
            );
            continue;
        }
        tracing::info!(function = %stmt.name, target = %stmt.target, "creating user-defined function");
        if let Err(e) = warehouse.execute_statement(&stmt.sql).await {
            failed.insert(stmt.name.clone(), format!("failed to create function: {e}"));
        }
    }
    failed
}

/// Models in `models` whose calls (directly or through other functions)
/// reach a function in `failed`, mapped to the failed function they need.
pub(crate) fn callers_of_failed(
    compile_result: &CompileResult,
    failed: &std::collections::BTreeMap<String, String>,
) -> std::collections::BTreeMap<String, String> {
    let registry = compile_result.semantic_graph.functions();
    let mut out = std::collections::BTreeMap::new();
    if failed.is_empty() {
        return out;
    }
    for model in &compile_result.project.models {
        let calls = rocky_compiler::udf::model_calls(&model.sql, registry);
        if let Some(sig) = registry
            .creation_order(calls.iter().map(String::as_str))
            .into_iter()
            .find(|sig| failed.contains_key(&sig.def.name))
        {
            out.insert(model.config.name.clone(), sig.def.name.clone());
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::{Path, PathBuf};

    use crate::commands::plan::plan_preview_output;

    const MODEL_SIDECAR: &str = "[strategy]\ntype = \"full_refresh\"\n[target]\ncatalog = \"warehouse\"\nschema = \"main\"\n";

    /// A project whose model calls `cents_to_dollars`, which calls `scale`.
    fn project(root: &Path, adapter: &str) -> (PathBuf, PathBuf) {
        let models = root.join("models");
        let functions = root.join("functions");
        fs::create_dir_all(&models).unwrap();
        fs::create_dir_all(&functions).unwrap();
        let cfg = root.join("rocky.toml");
        fs::write(
            &cfg,
            format!(
                "{adapter}\n[pipeline.t]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
                 [pipeline.t.target]\nadapter = \"default\"\n"
            ),
        )
        .unwrap();
        fs::write(
            functions.join("scale.toml"),
            "returns = \"DOUBLE\"\n[[arguments]]\nname = \"x\"\ntype = \"DOUBLE\"\n",
        )
        .unwrap();
        fs::write(functions.join("scale.sql"), "x / 100.0").unwrap();
        fs::write(
            functions.join("cents_to_dollars.toml"),
            "returns = \"DOUBLE\"\n[[arguments]]\nname = \"cents\"\ntype = \"BIGINT\"\n",
        )
        .unwrap();
        fs::write(functions.join("cents_to_dollars.sql"), "scale(cents)").unwrap();
        fs::write(
            models.join("fct.sql"),
            "SELECT cents_to_dollars(amount) AS usd FROM raw.orders",
        )
        .unwrap();
        fs::write(models.join("fct.toml"), MODEL_SIDECAR).unwrap();
        fs::write(models.join("other.sql"), "SELECT 1 AS id").unwrap();
        fs::write(models.join("other.toml"), MODEL_SIDECAR).unwrap();
        (cfg, models)
    }

    #[test]
    fn plan_preview_creates_functions_first_callees_before_callers() {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, models) = project(tmp.path(), "[adapter]\ntype = \"duckdb\"\n");
        let out = plan_preview_output(Some(&cfg), &models, None, None).unwrap();
        let purposes: Vec<&str> = out.statements.iter().map(|s| s.purpose.as_str()).collect();
        assert_eq!(&purposes[..2], ["create_function", "create_function"]);
        assert_eq!(
            out.statements[0].sql,
            "CREATE OR REPLACE MACRO scale(x) AS CAST((x / 100.0\n) AS DOUBLE)"
        );
        assert_eq!(out.statements[1].target, "cents_to_dollars");
        assert!(
            out.statements[2..]
                .iter()
                .all(|s| s.purpose != "create_function")
        );

        // A model that calls no function previews no function DDL.
        let only_other = plan_preview_output(Some(&cfg), &models, Some("other"), None).unwrap();
        assert!(
            only_other
                .statements
                .iter()
                .all(|s| s.purpose != "create_function")
        );
        // Selecting a function by name previews it and its callees only.
        let by_name =
            plan_preview_output(Some(&cfg), &models, Some("cents_to_dollars"), None).unwrap();
        assert_eq!(by_name.statements.len(), 2);
    }

    #[test]
    fn plan_preview_uses_the_target_dialect_and_refuses_trino_with_e051() {
        let tmp = tempfile::tempdir().unwrap();
        let (cfg, models) = project(
            tmp.path(),
            "[adapter]\ntype = \"snowflake\"\naccount = \"example\"\n",
        );
        let out = plan_preview_output(Some(&cfg), &models, None, None).unwrap();
        assert!(
            out.statements[0].sql.starts_with(
                "CREATE OR REPLACE FUNCTION scale(x DOUBLE)\n  RETURNS DOUBLE\n  LANGUAGE SQL"
            ),
            "{}",
            out.statements[0].sql
        );

        let tmp = tempfile::tempdir().unwrap();
        let (cfg, models) = project(
            tmp.path(),
            "[adapter]\ntype = \"trino\"\nhost = \"localhost\"\n",
        );
        let out = plan_preview_output(Some(&cfg), &models, None, None).unwrap();
        assert!(
            out.statements
                .iter()
                .all(|s| s.purpose != "create_function")
        );
        let refusal = out
            .skipped
            .iter()
            .find(|s| s.model == "functions")
            .expect("the refusal is reported");
        assert!(refusal.reason.starts_with("[E051]"), "{}", refusal.reason);
        assert!(refusal.reason.contains("Trino"), "{}", refusal.reason);
    }
}
