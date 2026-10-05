//! E053 / W053 — validate a model's `[clickhouse]` table attributes.
//!
//! `[clickhouse]` sets `ENGINE` / `PARTITION BY` / `ORDER BY` on the table a
//! model's `CREATE TABLE … AS` builds on ClickHouse. Two checks, mirroring
//! the `[redshift]` ones (E052 / W052):
//!
//! - **E053 (error)** — the options cannot render (an engine that is not a
//!   parameterless MergeTree-family name, an invalid `order_by` column, a
//!   `partition_by` that is not a column or `fn(column)`), or a strategy with
//!   no table CREATE to carry them, or a lakehouse `format`. The option logic
//!   is [`rocky_ir::ClickHouseTableOptions::violations`], the same function
//!   the ClickHouse dialect refuses with at SQL generation.
//! - **W053 (warning)** — an `order_by` / `partition_by` column the model
//!   does not output, checked only against a provably complete output schema.
//!
//! Neither check knows the target adapter (the compiler has none). The
//! adapter-dependent half of E053 — a `merge` model on a ClickHouse-only
//! project — lives in the CLI, which reads `rocky.toml`.

use indexmap::IndexMap;
use rocky_core::models::{Model, StrategyConfig};

use crate::diagnostic::{Diagnostic, E053, W053};
use crate::semantic::SemanticGraph;
use crate::types::TypedColumn;

/// Run E053 / W053 over every model that declares `[clickhouse]`.
#[must_use]
pub fn check_clickhouse_table_options(
    models: &[Model],
    typed_models: &IndexMap<String, Vec<TypedColumn>>,
    graph: &SemanticGraph,
) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    for model in models {
        let Some(opts) = model
            .config
            .format_options
            .as_ref()
            .and_then(|o| o.clickhouse.as_ref())
        else {
            continue;
        };
        let name = model.config.name.as_str();

        for violation in opts.violations() {
            diagnostics.push(
                Diagnostic::error(E053, name, violation)
                    .with_suggestion("fix the `[clickhouse]` block in the model sidecar"),
            );
        }

        // Exhaustive on purpose: a new strategy must decide whether it
        // creates a table these attributes can shape.
        let no_table_create = match &model.config.strategy {
            StrategyConfig::FullRefresh
            | StrategyConfig::Incremental { .. }
            | StrategyConfig::Merge { .. }
            | StrategyConfig::TimeInterval { .. }
            | StrategyConfig::DeleteInsert { .. }
            | StrategyConfig::Microbatch { .. }
            | StrategyConfig::Snapshot { .. } => None,
            StrategyConfig::View => Some("view"),
            StrategyConfig::MaterializedView => Some("materialized_view"),
            StrategyConfig::DynamicTable { .. } => Some("dynamic_table"),
            StrategyConfig::ContentAddressed { .. } => Some("content_addressed"),
            StrategyConfig::Ephemeral => Some("ephemeral"),
        };
        if let Some(strategy) = no_table_create {
            diagnostics.push(
                Diagnostic::error(
                    E053,
                    name,
                    format!(
                        "`[clickhouse]` table options apply to a table built with CREATE TABLE \
                         AS; strategy `{strategy}` creates none"
                    ),
                )
                .with_suggestion("remove the `[clickhouse]` block or use a table strategy"),
            );
        }
        if model.config.format.is_some() {
            diagnostics.push(
                Diagnostic::error(
                    E053,
                    name,
                    "`[clickhouse]` table options cannot be combined with a lakehouse `format`",
                )
                .with_suggestion("remove either `format` or the `[clickhouse]` block"),
            );
        }

        // W053 — only against a provably complete output schema.
        let complete = graph
            .models
            .get(name)
            .is_some_and(crate::semantic::ModelSchema::schema_is_complete);
        let Some(cols) = typed_models.get(name).filter(|c| !c.is_empty()) else {
            continue;
        };
        if !complete {
            continue;
        }
        // ClickHouse column names are case-sensitive.
        let outputs = |col: &str| cols.iter().any(|c| c.name == col);
        let referenced = opts
            .order_by
            .iter()
            .map(|c| ("order_by", c.as_str()))
            .chain(opts.partition_column().map(|c| ("partition_by", c)));
        for (field, col) in referenced {
            if !outputs(col) {
                diagnostics.push(
                    Diagnostic::warning(
                        W053,
                        name,
                        format!("clickhouse.{field} column '{col}' is not in the model's output"),
                    )
                    .with_suggestion("name a column the model's SELECT produces"),
                );
            }
        }
    }
    diagnostics
}
