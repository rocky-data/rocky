//! Ephemeral models: inline them into their consumers, and refuse the uses
//! that cannot work (E038).
//!
//! An ephemeral model (`type = "ephemeral"`) is never materialized. Every
//! model that reads it executes with the ephemeral model's SQL spliced in as a
//! CTE named `__rocky_ephemeral__<model>` (see [`rocky_sql::ephemeral`]).
//!
//! [`apply_ephemerals`] runs at the end of `compile`, AFTER type checking:
//!
//! - Type inference, contracts and column lineage run on the SQL as authored.
//!   A consumer's `FROM eph_orders` resolves to the ephemeral model's typed
//!   output through the model graph, exactly as for any other upstream model,
//!   so diagnostics keep pointing at the lines the author wrote.
//! - Then each consumer's [`Model::sql`] is replaced with the inlined SQL.
//!   Every command that renders or executes SQL from the compile result —
//!   `rocky run`, `plan`, `emit-sql`, `compile --expand-macros`, `preview` —
//!   therefore sees the inlined statement. An ephemeral model keeps its own
//!   SQL; nothing executes it.
//!
//! E038 marks an ephemeral use that cannot work:
//!
//! - `[[tests]]` on an ephemeral model. The tests run against the model's
//!   table, and an ephemeral model has none.
//! - A qualified read of an ephemeral model's nominal target
//!   (`main.eph_orders`). No table backs that name, so the read hits a
//!   catalog error or a stale table. Read the model by its bare name.
//! - A consumer whose SQL the inliner cannot rewrite (it does not parse as
//!   one `SELECT`, or a `WITH RECURSIVE` CTE would capture a name the inlined
//!   SQL reads).
//!
//! `rocky run --model <ephemeral>` is refused with E038 by the CLI: there is
//! nothing to build. Contracts are allowed: compile-time contract checks
//! validate the inferred schema, which an ephemeral model has.

use std::collections::{BTreeMap, HashMap, HashSet};

use rocky_core::models::StrategyConfig;
use rocky_sql::ephemeral::{EphemeralModel, InlineError, inline_ephemeral_refs};

use crate::diagnostic::{Diagnostic, E038};
use crate::project::Project;

/// Validate ephemeral uses and, when `write_back`, replace each consumer's
/// SQL with its inlined form.
///
/// `write_back = false` is for the language server, which maps diagnostics
/// and symbols onto the authored text; it still gets every E038.
pub fn apply_ephemerals(project: &mut Project, write_back: bool) -> Vec<Diagnostic> {
    let ephemerals: BTreeMap<String, EphemeralModel> = project
        .models
        .iter()
        .filter(|m| matches!(m.config.strategy, StrategyConfig::Ephemeral))
        .map(|m| {
            (
                m.config.name.clone(),
                EphemeralModel {
                    sql: m.sql.clone(),
                    catalog: m.config.target.catalog.clone(),
                    schema: m.config.target.schema.clone(),
                    table: m.config.target.table.clone(),
                },
            )
        })
        .collect();
    if ephemerals.is_empty() {
        return Vec::new();
    }

    let mut diagnostics = Vec::new();
    for model in &project.models {
        if ephemerals.contains_key(&model.config.name) && !model.config.tests.is_empty() {
            let name = model.config.name.as_str();
            diagnostics.push(
                Diagnostic::error(
                    E038,
                    name,
                    format!(
                        "ephemeral model '{name}' declares [[tests]], but an ephemeral model has \
                         no table for them to run against"
                    ),
                )
                .with_suggestion(
                    "Move the tests to a model that reads this one, or give this model a \
                     materialized strategy such as `type = \"view\"`",
                ),
            );
        }
    }

    let depends_on: HashMap<&str, &[String]> = project
        .dag_nodes
        .iter()
        .map(|n| (n.name.as_str(), n.depends_on.as_slice()))
        .collect();
    let target_tables: HashSet<String> = ephemerals
        .values()
        .map(|e| e.table.to_lowercase())
        .collect();

    let mut rewritten: Vec<(usize, String)> = Vec::new();
    for (index, model) in project.models.iter().enumerate() {
        let name = model.config.name.as_str();
        let is_ephemeral = ephemerals.contains_key(name);
        let reads_ephemeral = depends_on
            .get(name)
            .is_some_and(|deps| deps.iter().any(|d| ephemerals.contains_key(d)));
        // Cheap text pre-filter: only a statement that mentions an ephemeral
        // target's table name can spell a qualified read of it.
        let lower = model.sql.to_lowercase();
        let may_read_target = target_tables.iter().any(|t| lower.contains(t.as_str()));
        if !reads_ephemeral && !may_read_target {
            continue;
        }
        match inline_ephemeral_refs(name, &model.sql, &ephemerals) {
            Ok(outcome) => {
                for (ephemeral, spelled) in &outcome.target_refs {
                    diagnostics.push(
                        Diagnostic::error(
                            E038,
                            name,
                            format!(
                                "model '{name}' reads `{spelled}`, the nominal target of \
                                 ephemeral model '{ephemeral}'; an ephemeral model has no table, \
                                 so this read hits a missing or stale table"
                            ),
                        )
                        .with_suggestion(format!(
                            "Read the model by its bare name, `FROM {ephemeral}`, so Rocky \
                             inlines it"
                        )),
                    );
                }
                if !is_ephemeral && let Some(sql) = outcome.sql {
                    rewritten.push((index, sql));
                }
            }
            // An ephemeral model's own SQL is only ever executed nested in a
            // consumer; any real problem resurfaces there.
            Err(_) if is_ephemeral => {}
            Err(error) => diagnostics.push(inline_failure(name, &error)),
        }
    }

    if write_back {
        for (index, sql) in rewritten {
            project.models[index].sql = sql;
        }
    }
    diagnostics
}

fn inline_failure(model: &str, error: &InlineError) -> Diagnostic {
    let suggestion = match error {
        InlineError::RecursiveCapture { name, .. } => format!(
            "Rename the `{name}` CTE, or have the ephemeral model read a differently named relation"
        ),
        InlineError::Cycle { .. } => "Break the cycle between the ephemeral models".to_string(),
        InlineError::Parse { .. } | InlineError::NotAQuery { .. } => {
            "Give the ephemeral model a materialized strategy such as `type = \"view\"`, or \
             rewrite the SQL as one SELECT the parser accepts"
                .to_string()
        }
    };
    Diagnostic::error(
        E038,
        model,
        format!("model '{model}' reads an ephemeral model that cannot be inlined into it: {error}"),
    )
    .with_suggestion(suggestion)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compile::{CompilerConfig, compile};

    fn write(dir: &std::path::Path, name: &str, sql: &str, toml: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        std::fs::write(dir.join(format!("{name}.toml")), toml).unwrap();
    }

    fn sidecar(name: &str, strategy: &str) -> String {
        format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"{strategy}\"\n\n\
             [target]\ncatalog = \"\"\nschema = \"main\"\ntable = \"{name}\"\n"
        )
    }

    fn compile_dir(dir: &std::path::Path) -> crate::compile::CompileResult {
        compile(&CompilerConfig {
            models_dir: dir.to_path_buf(),
            ..Default::default()
        })
        .unwrap()
    }

    fn e038(result: &crate::compile::CompileResult) -> Vec<&Diagnostic> {
        result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == E038)
            .collect()
    }

    #[test]
    fn a_consumer_compiles_clean_and_executes_the_inlined_sql() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "eph_orders",
            "SELECT order_id, amount FROM raw.orders",
            &sidecar("eph_orders", "ephemeral"),
        );
        write(
            tmp.path(),
            "fct",
            "SELECT order_id FROM eph_orders",
            &sidecar("fct", "full_refresh"),
        );
        let result = compile_dir(tmp.path());
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        let fct = result.project.model("fct").unwrap();
        assert_eq!(
            fct.sql,
            "WITH __rocky_ephemeral__eph_orders AS (SELECT order_id, amount FROM raw.orders) \
             SELECT order_id FROM __rocky_ephemeral__eph_orders AS eph_orders"
        );
        // The ephemeral model keeps its own SQL, and stays in the DAG.
        let eph = result.project.model("eph_orders").unwrap();
        assert_eq!(eph.sql, "SELECT order_id, amount FROM raw.orders");
        assert!(
            result
                .project
                .execution_order
                .iter()
                .any(|n| n == "eph_orders")
        );
        // Typed through the model graph.
        assert!(result.type_check.typed_models.contains_key("eph_orders"));
        assert!(result.type_check.typed_models.contains_key("fct"));
    }

    #[test]
    fn write_back_off_keeps_the_authored_sql() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "eph",
            "SELECT 1 AS id",
            &sidecar("eph", "ephemeral"),
        );
        write(
            tmp.path(),
            "fct",
            "SELECT id FROM eph",
            &sidecar("fct", "full_refresh"),
        );
        let result = compile(&CompilerConfig {
            models_dir: tmp.path().to_path_buf(),
            preserve_authored_sql: true,
            ..Default::default()
        })
        .unwrap();
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert_eq!(
            result.project.model("fct").unwrap().sql,
            "SELECT id FROM eph"
        );
    }

    #[test]
    fn tests_on_an_ephemeral_model_are_e038() {
        let tmp = tempfile::tempdir().unwrap();
        let toml = format!(
            "{}\n[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n",
            sidecar("eph", "ephemeral")
        );
        write(tmp.path(), "eph", "SELECT 1 AS id", &toml);
        let result = compile_dir(tmp.path());
        let diags = e038(&result);
        assert_eq!(diags.len(), 1, "{:?}", result.diagnostics);
        assert!(diags[0].message.contains("[[tests]]"));
    }

    #[test]
    fn a_qualified_read_of_the_nominal_target_is_e038() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "eph",
            "SELECT 1 AS id",
            &sidecar("eph", "ephemeral"),
        );
        write(
            tmp.path(),
            "fct",
            "SELECT id FROM main.eph",
            &sidecar("fct", "full_refresh"),
        );
        let result = compile_dir(tmp.path());
        let diags = e038(&result);
        assert_eq!(diags.len(), 1, "{:?}", result.diagnostics);
        assert_eq!(diags[0].model, "fct");
        assert!(diags[0].message.contains("main.eph"));
    }

    #[test]
    fn a_plain_ephemeral_model_is_no_longer_refused() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "eph",
            "SELECT 1 AS id",
            &sidecar("eph", "ephemeral"),
        );
        let result = compile_dir(tmp.path());
        assert!(e038(&result).is_empty(), "{:?}", result.diagnostics);
        assert!(!result.has_errors, "{:?}", result.diagnostics);
    }

    #[test]
    fn a_chain_of_ephemerals_inlines_transitively() {
        let tmp = tempfile::tempdir().unwrap();
        write(
            tmp.path(),
            "a",
            "SELECT 1 AS id",
            &sidecar("a", "ephemeral"),
        );
        write(
            tmp.path(),
            "b",
            "SELECT id FROM a",
            &sidecar("b", "ephemeral"),
        );
        write(
            tmp.path(),
            "fct",
            "SELECT id FROM b",
            &sidecar("fct", "full_refresh"),
        );
        let result = compile_dir(tmp.path());
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert_eq!(
            result.project.model("fct").unwrap().sql,
            "WITH __rocky_ephemeral__a AS (SELECT 1 AS id), \
             __rocky_ephemeral__b AS (SELECT id FROM __rocky_ephemeral__a AS a) \
             SELECT id FROM __rocky_ephemeral__b AS b"
        );
    }
}
