//! A one-part read that names nothing Rocky knows but is a near-miss of a
//! model name (`E064`, #2620).
//!
//! `FROM stg_ordrs` for a model `stg_orders` binds to no model, so it does
//! not become a dependency. It is read as a warehouse table instead, and the
//! mistake shows only when `rocky run` fails on a missing table.
//!
//! # Only fire when certain enough
//!
//! A one-part name can be a real warehouse table in the default schema that
//! Rocky has no schema for, so the check fires only when all of these hold:
//!
//! - the read is a plain one-part table (`FROM name`): not quoted, not a
//!   table function (`FROM range(10)`), not a `WITH` name;
//! - it is not a model's name, a model's target table, the last part of a
//!   known source schema key, or a name the caller knows from outside the
//!   compiled set (`other_names`: the rest of the project's models when only
//!   part of it is compiled, and resolved external dependencies such as seeds);
//! - it is close to a model name: an optimal-string-alignment distance of at
//!   most 2, or 1 when the name has fewer than 5 characters. Names compare
//!   ignoring ASCII case.
//!
//! The escape for a real table with such a name is to qualify it
//! (`main.stg_ordrs`); a two-part read is never checked here.
//!
//! The answer depends on every model's name, so it is recomputed over the
//! whole project on every compile, full and incremental alike.

use std::collections::{BTreeSet, HashMap};
use std::ops::ControlFlow;

use rocky_core::models::Model;
use sqlparser::ast::{TableFactor, Visit, Visitor};
use sqlparser::parser::Parser;

use crate::diagnostic::{Diagnostic, E064};
use crate::types::TypedColumn;

/// One `E064` per model and misspelled name.
pub fn check_unknown_relations(
    models: &[Model],
    source_schemas: &HashMap<String, Vec<TypedColumn>>,
    other_names: &BTreeSet<String>,
) -> Vec<Diagnostic> {
    let model_names: Vec<&str> = models.iter().map(|m| m.config.name.as_str()).collect();
    let mut known: BTreeSet<String> = model_names.iter().map(|n| n.to_ascii_lowercase()).collect();
    known.extend(
        models
            .iter()
            .map(|m| m.config.target.table.to_ascii_lowercase()),
    );
    known.extend(
        source_schemas
            .keys()
            .chain(other_names.iter())
            .filter_map(|key| last_part(key)),
    );

    let mut diagnostics = Vec::new();
    for model in models {
        let Ok(lineage) = rocky_sql::lineage::extract_lineage(&model.sql) else {
            continue;
        };
        // Lineage removes `WITH` names; the parse keeps only plain base-table
        // reads. A read must be both.
        let plain = plain_table_reads(&model.sql);
        let reads: BTreeSet<String> = lineage
            .source_tables
            .iter()
            .filter(|t| t.binding == rocky_sql::lineage::TableBinding::Physical)
            .map(|t| t.name.clone())
            .chain(lineage.nested_sources.iter().cloned())
            .map(|name| name.to_ascii_lowercase())
            .filter(|name| plain.contains(name))
            .collect();
        for read in reads {
            if known.contains(&read) {
                continue;
            }
            let near = nearest_models(&read, &model_names);
            if near.is_empty() {
                continue;
            }
            let list = near
                .iter()
                .map(|n| format!("`{n}`"))
                .collect::<Vec<_>>()
                .join(" or ");
            diagnostics.push(
                Diagnostic::error(
                    E064,
                    model.config.name.as_str(),
                    format!(
                        "`{read}` names no model, no `WITH` name and no known source table; \
                         did you mean model {list}?"
                    ),
                )
                .with_suggestion(format!(
                    "fix the name, or qualify it with its schema (`main.{read}`) if it is a \
                     warehouse table Rocky does not know"
                )),
            );
        }
    }
    diagnostics
}

/// The last part of a dotted name, unquoted and lower-cased.
fn last_part(name: &str) -> Option<String> {
    name.rsplit('.').next().map(|table| {
        table
            .trim_matches(['"', '`', '[', ']'])
            .to_ascii_lowercase()
    })
}

/// Lower-cased one-part table names the SQL reads with no quotes and no
/// arguments: `FROM orders`, not `FROM "orders"`, `FROM range(10)` or
/// `FROM 'data'`. Unparsable SQL reads none.
fn plain_table_reads(sql: &str) -> BTreeSet<String> {
    struct Reads(BTreeSet<String>);
    impl Visitor for Reads {
        type Break = ();
        fn pre_visit_table_factor(&mut self, factor: &TableFactor) -> ControlFlow<()> {
            if let TableFactor::Table {
                name, args: None, ..
            } = factor
                && let [part] = name.0.as_slice()
                && let Some(ident) = part.as_ident()
                && ident.quote_style.is_none()
            {
                self.0.insert(ident.value.to_ascii_lowercase());
            }
            ControlFlow::Continue(())
        }
    }
    let Ok(statements) = Parser::parse_sql(&rocky_sql::dialect::DatabricksDialect, sql) else {
        return BTreeSet::new();
    };
    let mut reads = Reads(BTreeSet::new());
    for statement in &statements {
        let _ = statement.visit(&mut reads);
    }
    reads.0
}

/// The model names closest to `read` within the distance limit, every one
/// that ties for nearest, sorted.
fn nearest_models<'a>(read: &str, model_names: &[&'a str]) -> Vec<&'a str> {
    let limit = if read.chars().count() < 5 { 1 } else { 2 };
    let mut best = usize::MAX;
    let mut near: Vec<&str> = Vec::new();
    for name in model_names {
        let distance = strsim::osa_distance(read, &name.to_ascii_lowercase());
        if distance == 0 || distance > limit {
            continue;
        }
        if distance < best {
            best = distance;
            near.clear();
        }
        if distance == best {
            near.push(name);
        }
    }
    near.sort_unstable();
    near.dedup();
    near
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{ModelConfig, StrategyConfig, TargetConfig};

    fn model(name: &str, sql: &str) -> Model {
        Model {
            drop_existing_kind: None,
            config: ModelConfig {
                name: name.to_string(),
                depends_on: vec![],
                strategy: StrategyConfig::default(),
                target: TargetConfig {
                    catalog: "c".to_string(),
                    schema: "s".to_string(),
                    table: name.to_string(),
                },
                sources: vec![],
                adapter: None,
                intent: None,
                freshness: None,
                tests: vec![],
                format: None,
                format_options: None,
                classification: Default::default(),
                tags: Default::default(),
                governance: Default::default(),
                retention: None,
                budget: None,
                skip: None,
                name_declared: String::new(),
                target_table_declared: String::new(),
            },
            sql: sql.to_string(),
            file_path: format!("models/{name}.sql").into(),
            contract_path: None,
        }
    }

    fn e064(reader_sql: &str, sources: &[&str]) -> Vec<Diagnostic> {
        let models = vec![
            model("stg_orders", "SELECT 1 AS id"),
            model("ord", "SELECT 1 AS id"),
            model("m", reader_sql),
        ];
        let schemas = sources
            .iter()
            .map(|s| (s.to_string(), Vec::new()))
            .collect();
        check_unknown_relations(&models, &schemas, &BTreeSet::new())
    }

    /// #2620: the issue's repro, and the same read nested in a sub-query.
    #[test]
    fn a_near_miss_of_a_model_name_is_e064() {
        for sql in [
            "SELECT id FROM stg_ordrs",
            "SELECT id FROM STG_ORDRS",
            "SELECT * FROM (SELECT id FROM stg_ordrs) AS x",
            "SELECT id FROM stg_ordres",
        ] {
            let diags = e064(sql, &[]);
            assert_eq!(diags.len(), 1, "{sql}: {diags:?}");
            assert_eq!(&*diags[0].code, E064);
            assert_eq!(diags[0].model, "m");
            assert!(diags[0].message.contains("`stg_orders`"), "{diags:?}");
        }
    }

    /// Reads that must stay silent: the model itself, a `WITH` name, a known
    /// source, a two-part name, a name far from every model, and a model's
    /// target table.
    #[test]
    fn known_or_distant_reads_are_not_e064() {
        for sql in [
            "SELECT id FROM stg_orders",
            "WITH stg_ordrs AS (SELECT 1 AS id) SELECT id FROM stg_ordrs",
            "SELECT id FROM main.stg_ordrs",
            "SELECT id FROM raw_events",
            "SELECT id FROM stg_o",
        ] {
            assert!(e064(sql, &[]).is_empty(), "{sql}");
        }
        assert!(e064("SELECT id FROM stg_ordrs", &["main.stg_ordrs"]).is_empty());
        let mut renamed = model("orders_model", "SELECT 1 AS id");
        renamed.config.target.table = "stg_ordrs".to_string();
        let models = vec![
            model("stg_orders", "SELECT 1 AS id"),
            renamed,
            model("m", "SELECT id FROM stg_ordrs"),
        ];
        assert!(check_unknown_relations(&models, &HashMap::new(), &BTreeSet::new()).is_empty());
    }

    /// #2620 red team: a quoted read, a table function, and a name the
    /// caller knows from outside the compiled set never fire.
    #[test]
    fn quoted_function_and_outside_names_are_not_e064() {
        for sql in [
            "SELECT * FROM \"stg_ordrs\"",
            "SELECT * FROM `stg_ordrs`",
            "SELECT * FROM \"stg_orders\"",
            "SELECT * FROM stg_ordrs(10)",
            "SELECT * FROM orf(10)",
        ] {
            assert!(e064(sql, &[]).is_empty(), "{sql}");
        }
        let models = vec![
            model("stg_orders", "SELECT 1 AS id"),
            model("m", "SELECT id FROM stg_ordrs"),
        ];
        for outside in ["stg_ordrs", "seeds.stg_ordrs"] {
            let names = BTreeSet::from([outside.to_string()]);
            assert!(
                check_unknown_relations(&models, &HashMap::new(), &names).is_empty(),
                "{outside}"
            );
        }
    }

    /// A name under 5 characters allows one edit, not two. Ties list every
    /// nearest model.
    #[test]
    fn short_names_allow_one_edit_and_ties_are_listed() {
        assert_eq!(e064("SELECT id FROM orf", &[]).len(), 1);
        assert!(e064("SELECT id FROM oxy", &[]).is_empty());
        let models = vec![
            model("stg_ordera", "SELECT 1 AS id"),
            model("stg_orderb", "SELECT 1 AS id"),
            model("m", "SELECT id FROM stg_orderc"),
        ];
        let diags = check_unknown_relations(&models, &HashMap::new(), &BTreeSet::new());
        assert_eq!(diags.len(), 1, "{diags:?}");
        assert!(
            diags[0].message.contains("`stg_ordera` or `stg_orderb`"),
            "{diags:?}"
        );
    }
}
