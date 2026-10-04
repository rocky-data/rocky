//! E052 / W052 — validate a model's `[redshift]` table attributes.
//!
//! `[redshift]` sets `DISTSTYLE` / `DISTKEY` / `SORTKEY` on the table a
//! model's `CREATE TABLE … AS` builds on Redshift. Two checks:
//!
//! - **E052 (error)** — the options cannot render: an invalid column name,
//!   a contradictory combination (`dist_style = "key"` with no `dist_key`,
//!   `dist_key` with `dist_style = "all"`, `sort_style = "auto"` with
//!   columns, more than 8 interleaved sort columns), or a strategy with no
//!   table CREATE to carry them (`view`, `materialized_view`,
//!   `dynamic_table`, `content_addressed`, `ephemeral`) or a lakehouse
//!   `format`. The option logic is
//!   [`rocky_ir::RedshiftTableOptions::violations`], the same function the
//!   Redshift dialect refuses with at SQL generation, so the compile-time
//!   and run-time checks cannot drift.
//! - **W052 (warning)** — a `dist_key` / `sort_key` column the model does not
//!   output. An absence check, so it runs only when the model's output
//!   columns are provably complete (`ModelSchema::schema_is_complete`) —
//!   the same guard W006 uses — and it warns rather than errors because that
//!   enumeration comes from lineage extraction. Redshift itself rejects the
//!   `CREATE TABLE` at run time if the column is truly missing.
//!
//! Neither check knows the target adapter (the compiler has none), so a
//! model that sets `[redshift]` and targets another warehouse passes here
//! and is refused at SQL generation by that dialect, with a message naming
//! the option.

use indexmap::IndexMap;
use rocky_core::models::{Model, StrategyConfig};

use crate::diagnostic::{Diagnostic, E052, W052};
use crate::semantic::SemanticGraph;
use crate::types::TypedColumn;

/// Run E052 / W052 over every model that declares `[redshift]`.
#[must_use]
pub fn check_redshift_table_options(
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
            .and_then(|o| o.redshift.as_ref())
        else {
            continue;
        };
        let name = model.config.name.as_str();

        for violation in opts.violations() {
            diagnostics.push(
                Diagnostic::error(E052, name, violation)
                    .with_suggestion("fix the `[redshift]` block in the model sidecar"),
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
            | StrategyConfig::Microbatch { .. } => None,
            StrategyConfig::View => Some("view"),
            StrategyConfig::MaterializedView => Some("materialized_view"),
            StrategyConfig::DynamicTable { .. } => Some("dynamic_table"),
            StrategyConfig::ContentAddressed { .. } => Some("content_addressed"),
            StrategyConfig::Ephemeral => Some("ephemeral"),
        };
        if let Some(strategy) = no_table_create {
            diagnostics.push(
                Diagnostic::error(
                    E052,
                    name,
                    format!(
                        "`[redshift]` table options apply to a table built with CREATE TABLE AS; \
                         strategy `{strategy}` creates none"
                    ),
                )
                .with_suggestion("remove the `[redshift]` block or use a table strategy"),
            );
        }
        if model.config.format.is_some() {
            diagnostics.push(
                Diagnostic::error(
                    E052,
                    name,
                    "`[redshift]` table options cannot be combined with a lakehouse `format`",
                )
                .with_suggestion("remove either `format` or the `[redshift]` block"),
            );
        }

        // W052 — only against a provably complete output schema.
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
        let outputs = |col: &str| cols.iter().any(|c| c.name.eq_ignore_ascii_case(col));
        let referenced = opts
            .dist_key
            .iter()
            .map(|c| ("dist_key", c))
            .chain(opts.sort_key.iter().map(|c| ("sort_key", c)));
        for (field, col) in referenced {
            if !outputs(col) {
                diagnostics.push(
                    Diagnostic::warning(
                        W052,
                        name,
                        format!("redshift.{field} column '{col}' is not in the model's output"),
                    )
                    .with_suggestion("name a column the model's SELECT produces"),
                );
            }
        }
    }
    diagnostics
}
