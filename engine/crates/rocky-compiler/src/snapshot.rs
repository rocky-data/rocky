//! Compile-time checks for `type = "snapshot"` models (E049 / W049).
//!
//! The structural checks (a key and a change column are declared, names are
//! valid) come from [`rocky_core::models::StrategyConfig::snapshot_lowered`],
//! the same lowering the IR uses. The schema checks compare the declared
//! columns against the model's typed output.
//!
//! False refusals are worse than misses, so an absent column is an error only
//! when the model's own SELECT lists its columns. Under `SELECT *` the
//! compile-time schema can come from a stale seed or cache, and the same
//! finding is a warning.

use std::ops::ControlFlow;

use rocky_ir::{
    RockyType, SnapshotChangeStrategy, SnapshotCheckColumns, SnapshotHardDeletes, TypedColumn,
};
use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement, visit_expressions};

use crate::diagnostic::{Diagnostic, E049, W049};

/// A `check` strategy comparing more columns than this gets W049.
pub const MANY_CHECK_COLUMNS: usize = 20;

/// Functions whose value changes between evaluations. A unique key computed
/// from one of these can never match the previous run's rows.
const NON_DETERMINISTIC_FUNCTIONS: &[&str] = &[
    "random",
    "rand",
    "uuid",
    "uuid_string",
    "gen_random_uuid",
    "generate_uuid",
    "newid",
    "now",
    "current_timestamp",
    "localtimestamp",
    "sysdate",
    "systimestamp",
    "getdate",
];

/// E049 / W049 for one model. Returns nothing for a non-snapshot model.
///
/// `schema_complete` is the semantic graph's answer to "is `typed_cols` the
/// model's whole output" and `has_star` whether the projection used `*`.
pub fn check_snapshot_strategy(
    model: &rocky_core::models::Model,
    typed_cols: &[TypedColumn],
    schema_complete: bool,
    has_star: bool,
) -> Vec<Diagnostic> {
    let Some(lowered) = model.config.strategy.snapshot_lowered() else {
        return Vec::new();
    };
    let model_name = model.config.name.as_str();
    let mut diags: Vec<Diagnostic> = lowered
        .problems
        .iter()
        .map(|p| {
            Diagnostic::error(
                E049,
                model_name,
                format!("snapshot model '{model_name}': {p}"),
            )
            .with_suggestion(SNAPSHOT_HELP)
        })
        .collect();
    let spec = &lowered.spec;

    diags.extend(non_deterministic_keys(model_name, &model.sql, spec));

    // Everything below needs the model's output columns.
    if !schema_complete || typed_cols.is_empty() {
        return diags;
    }
    let find = |name: &str| {
        typed_cols
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(name.trim()))
    };
    let available = || {
        typed_cols
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    };
    // Absent from an explicit projection is certain; under `*` the schema may
    // be stale.
    let absent = |what: String| {
        if has_star {
            Diagnostic::warning(
                W049,
                model_name,
                format!(
                    "{what} is not in the compile-time schema of snapshot model \
                     '{model_name}', which reads `*` and may be stale"
                ),
            )
        } else {
            Diagnostic::error(
                E049,
                model_name,
                format!("{what} is not an output column of snapshot model '{model_name}'"),
            )
        }
        .with_suggestion(format!("Available columns: {}", available()))
    };

    // A key absent from the SELECT is only a warning (like merge's W006): a
    // `[[surrogate_key]]` column is added at run time, after this check.
    for key in &spec.unique_key {
        if !key.trim().is_empty() && find(key).is_none() {
            diags.push(
                Diagnostic::warning(
                    W049,
                    model_name,
                    format!(
                        "unique_key '{key}' is not an output column of snapshot model \
                         '{model_name}' as compiled; the run fails unless it is a \
                         `[[surrogate_key]]` column"
                    ),
                )
                .with_suggestion(format!("Available columns: {}", available())),
            );
        }
    }
    match &spec.change {
        SnapshotChangeStrategy::Timestamp { updated_at } => {
            if !updated_at.trim().is_empty() {
                match find(updated_at) {
                    None => diags.push(absent(format!("updated_at '{updated_at}'"))),
                    Some(col) => {
                        if !matches!(
                            col.data_type,
                            RockyType::Timestamp
                                | RockyType::TimestampNtz
                                | RockyType::Date
                                | RockyType::Unknown
                        ) {
                            diags.push(
                                Diagnostic::warning(
                                    W049,
                                    model_name,
                                    format!(
                                        "updated_at '{updated_at}' of snapshot model \
                                         '{model_name}' is {}, not a timestamp: it is compared \
                                         with `>` and cast to TIMESTAMP for valid_from",
                                        col.data_type
                                    ),
                                )
                                .with_suggestion(
                                    "Cast it to TIMESTAMP in the model SQL, or use \
                                     `strategy = \"check\"`",
                                ),
                            );
                        }
                    }
                }
            }
        }
        SnapshotChangeStrategy::Check {
            check_cols,
            updated_at,
        } => {
            if let Some(updated_at) = updated_at
                && !updated_at.trim().is_empty()
                && find(updated_at).is_none()
            {
                diags.push(absent(format!("updated_at '{updated_at}'")));
            }
            let compared = match check_cols {
                SnapshotCheckColumns::All => typed_cols
                    .iter()
                    .filter(|c| {
                        !spec
                            .unique_key
                            .iter()
                            .any(|k| c.name.eq_ignore_ascii_case(k.trim()))
                    })
                    .count(),
                SnapshotCheckColumns::Explicit(cols) => {
                    for col in cols {
                        if !col.trim().is_empty() && find(col).is_none() {
                            diags.push(absent(format!("check_cols entry '{col}'")));
                        }
                    }
                    cols.len()
                }
            };
            if compared > MANY_CHECK_COLUMNS {
                diags.push(
                    Diagnostic::warning(
                        W049,
                        model_name,
                        format!(
                            "snapshot model '{model_name}' compares {compared} columns with \
                             `strategy = \"check\"`; every run compares each one for every key"
                        ),
                    )
                    .with_suggestion(
                        "List only the columns whose changes matter in `check_cols`, or use \
                         `strategy = \"timestamp\"` if the source has a reliable updated_at",
                    ),
                );
            }
        }
    }

    let reserved = spec.meta_columns.written(spec.hard_deletes);
    for col in typed_cols {
        if reserved.iter().any(|m| m.eq_ignore_ascii_case(&col.name)) {
            let what = format!(
                "output column '{}' of snapshot model '{model_name}' has the name of a snapshot \
                 metadata column",
                col.name
            );
            let diag = if has_star {
                Diagnostic::warning(W049, model_name, what)
            } else {
                Diagnostic::error(E049, model_name, what)
            };
            diags.push(diag.with_suggestion(
                "Rename the column in the model SQL, or rename the metadata column with \
                 `snapshot_meta_column_names`",
            ));
        }
    }

    diags
}

/// Add a snapshot model's metadata columns (`valid_from`, `valid_to`,
/// `is_current`, ...) to its typed output, so contracts, readers' type
/// inference and `rocky compile` see the table the run actually builds.
/// No-op for other strategies, or when a name is already present.
pub fn append_snapshot_metadata_columns(
    model: &rocky_core::models::Model,
    typed_cols: &mut Vec<TypedColumn>,
) {
    // Only a resolved output schema gains columns; an empty one stays empty
    // (unknown), so nothing downstream mistakes it for a complete schema.
    if typed_cols.is_empty() {
        return;
    }
    let Some(lowered) = model.config.strategy.snapshot_lowered() else {
        return;
    };
    let meta = &lowered.spec.meta_columns;
    let mut add = |name: &str, data_type: RockyType, nullable: bool| {
        if !typed_cols.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
            typed_cols.push(TypedColumn {
                name: name.to_string(),
                data_type,
                nullable,
            });
        }
    };
    // CAST(... AS TIMESTAMP) is a timestamp without time zone on every
    // supported warehouse. `valid_from` is nullable when it comes from a
    // nullable `updated_at`; `valid_to` is NULL on current versions.
    add(&meta.valid_from, RockyType::TimestampNtz, true);
    add(&meta.valid_to, RockyType::TimestampNtz, true);
    if let Some(flag) = meta.is_current.name() {
        add(flag, RockyType::Boolean, false);
    }
    add(&meta.scd_id, RockyType::String, false);
    if let Some(updated_at) = &meta.updated_at {
        add(updated_at, RockyType::TimestampNtz, true);
    }
    if lowered.spec.hard_deletes == SnapshotHardDeletes::NewRecord {
        add(&meta.is_deleted, RockyType::Boolean, true);
    }
}

const SNAPSHOT_HELP: &str = "A snapshot sidecar needs `[strategy] type = \"snapshot\"`, \
     `unique_key`, and either `strategy = \"timestamp\"` with `updated_at` or \
     `strategy = \"check\"` with `check_cols`";

/// E049 for a unique key the model's top-level projection computes from a
/// non-deterministic function. Unparseable SQL and set operations are left
/// alone: other passes report those, and a guess here would be a false
/// refusal.
fn non_deterministic_keys(
    model_name: &str,
    sql: &str,
    spec: &rocky_ir::SnapshotSpec,
) -> Vec<Diagnostic> {
    let Ok(Statement::Query(query)) = rocky_sql::parser::parse_single_statement(sql) else {
        return Vec::new();
    };
    let SetExpr::Select(select) = query.body.as_ref() else {
        return Vec::new();
    };
    let mut diags = Vec::new();
    for item in &select.projection {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };
        let Some(key) = spec
            .unique_key
            .iter()
            .find(|k| alias.value.eq_ignore_ascii_case(k.trim()))
        else {
            continue;
        };
        if let Some(func) = first_non_deterministic_call(expr) {
            diags.push(
                Diagnostic::error(
                    E049,
                    model_name,
                    format!(
                        "unique_key '{key}' of snapshot model '{model_name}' is computed with \
                         `{func}()`, which returns a new value every run, so no row would ever \
                         match its previous version"
                    ),
                )
                .with_suggestion(
                    "Key the snapshot on columns that identify the row in the source, or a \
                     deterministic hash of them",
                ),
            );
        }
    }
    diags
}

fn first_non_deterministic_call(expr: &Expr) -> Option<String> {
    let mut found = None;
    let _ = visit_expressions(expr, |e| {
        if let Expr::Function(f) = e {
            let name = f.name.to_string().to_ascii_lowercase();
            let last = name.rsplit('.').next().unwrap_or(&name).to_string();
            if NON_DETERMINISTIC_FUNCTIONS.contains(&last.as_str()) {
                found = Some(last);
                return ControlFlow::Break(());
            }
        }
        ControlFlow::Continue(())
    });
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::models::{Model, ModelConfig, StrategyConfig, TargetConfig};

    fn model(strategy_toml: &str, sql: &str) -> Model {
        let strategy: StrategyConfig = toml::from_str(strategy_toml).expect("strategy toml");
        Model {
            drop_existing_kind: None,
            config: ModelConfig {
                name: "snap".to_string(),
                depends_on: vec![],
                strategy,
                target: TargetConfig {
                    catalog: "c".to_string(),
                    schema: "s".to_string(),
                    table: "snap".to_string(),
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
            file_path: "models/snap.sql".into(),
            contract_path: None,
        }
    }

    fn col(name: &str, ty: RockyType) -> TypedColumn {
        TypedColumn {
            name: name.into(),
            data_type: ty,
            nullable: true,
        }
    }

    fn cols() -> Vec<TypedColumn> {
        vec![
            col("id", RockyType::Int64),
            col("name", RockyType::String),
            col("updated_at", RockyType::Timestamp),
        ]
    }

    fn codes(d: &[Diagnostic]) -> Vec<&str> {
        d.iter().map(|d| d.code.as_ref()).collect()
    }

    #[test]
    fn a_valid_timestamp_snapshot_is_clean() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\nupdated_at = \"updated_at\"",
            "SELECT id, name, updated_at FROM raw.customers",
        );
        assert!(check_snapshot_strategy(&m, &cols(), true, false).is_empty());
    }

    #[test]
    fn a_valid_check_all_snapshot_is_clean_even_without_schema() {
        let m = model(
            "type = \"snapshot\"\nunique_key = [\"id\", \"name\"]\nstrategy = \"check\"\ncheck_cols = \"all\"",
            "SELECT * FROM raw.customers",
        );
        assert!(check_snapshot_strategy(&m, &[], false, true).is_empty());
    }

    #[test]
    fn missing_unique_key_is_e049() {
        let m = model(
            "type = \"snapshot\"\nstrategy = \"timestamp\"\nupdated_at = \"updated_at\"",
            "SELECT id, updated_at FROM t",
        );
        let d = check_snapshot_strategy(&m, &cols(), true, false);
        assert_eq!(codes(&d), vec![E049]);
        assert!(d[0].message.contains("unique_key"));
    }

    #[test]
    fn timestamp_without_updated_at_is_e049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"",
            "SELECT id FROM t",
        );
        let d = check_snapshot_strategy(&m, &[], false, false);
        assert_eq!(codes(&d), vec![E049]);
        assert!(d[0].message.contains("updated_at"));
    }

    #[test]
    fn check_cols_naming_an_absent_column_is_e049_for_explicit_projection() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"check\"\ncheck_cols = [\"name\", \"email\"]",
            "SELECT id, name, updated_at FROM t",
        );
        let d = check_snapshot_strategy(&m, &cols(), true, false);
        assert_eq!(codes(&d), vec![E049]);
        assert!(d[0].message.contains("email"));
    }

    #[test]
    fn absent_column_under_star_is_only_w049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"check\"\ncheck_cols = [\"email\"]",
            "SELECT * FROM t",
        );
        let d = check_snapshot_strategy(&m, &cols(), true, true);
        assert_eq!(codes(&d), vec![W049]);
    }

    #[test]
    fn non_deterministic_key_is_e049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"sk\"\nstrategy = \"check\"\ncheck_cols = \"all\"",
            "SELECT uuid() AS sk, name FROM t",
        );
        let d = check_snapshot_strategy(&m, &[], false, false);
        assert_eq!(codes(&d), vec![E049]);
        assert!(d[0].message.contains("uuid()"), "{}", d[0].message);
        // A deterministic hash key stays clean.
        let m = model(
            "type = \"snapshot\"\nunique_key = \"sk\"\nstrategy = \"check\"\ncheck_cols = \"all\"",
            "SELECT md5(CAST(id AS VARCHAR)) AS sk, name FROM t",
        );
        assert!(check_snapshot_strategy(&m, &[], false, false).is_empty());
    }

    #[test]
    fn non_timestamp_updated_at_and_many_check_columns_are_w049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"timestamp\"\nupdated_at = \"name\"",
            "SELECT id, name FROM t",
        );
        let d = check_snapshot_strategy(&m, &cols(), true, false);
        assert_eq!(codes(&d), vec![W049]);

        let mut wide = vec![col("id", RockyType::Int64)];
        for i in 0..=MANY_CHECK_COLUMNS {
            wide.push(col(&format!("c{i}"), RockyType::String));
        }
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"check\"\ncheck_cols = \"all\"",
            "SELECT * FROM t",
        );
        let d = check_snapshot_strategy(&m, &wide, true, true);
        assert_eq!(codes(&d), vec![W049]);
    }

    #[test]
    fn output_column_named_like_metadata_is_e049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"check\"\ncheck_cols = [\"name\"]",
            "SELECT id, name, valid_from FROM t",
        );
        let mut c = cols();
        c.push(col("valid_from", RockyType::Timestamp));
        let d = check_snapshot_strategy(&m, &c, true, false);
        assert_eq!(codes(&d), vec![E049]);
    }

    #[test]
    fn unique_key_absent_from_select_is_only_w049() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"customer_sk\"\nstrategy = \"check\"\ncheck_cols = [\"name\"]",
            "SELECT id, name FROM t",
        );
        let d = check_snapshot_strategy(&m, &cols(), true, false);
        assert_eq!(codes(&d), vec![W049]);
    }

    #[test]
    fn metadata_columns_join_the_typed_output() {
        let m = model(
            "type = \"snapshot\"\nunique_key = \"id\"\nstrategy = \"check\"\ncheck_cols = \"all\"\nhard_deletes = \"new_record\"",
            "SELECT id, name FROM t",
        );
        let mut typed = cols();
        append_snapshot_metadata_columns(&m, &mut typed);
        let names: Vec<&str> = typed.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "id",
                "name",
                "updated_at",
                "valid_from",
                "valid_to",
                "is_current",
                "snapshot_id",
                "is_deleted"
            ]
        );
        let mut unknown = Vec::new();
        append_snapshot_metadata_columns(&m, &mut unknown);
        assert!(unknown.is_empty(), "an unknown schema stays unknown");
    }

    #[test]
    fn non_snapshot_models_are_ignored() {
        let m = model("type = \"full_refresh\"", "SELECT 1 AS id");
        assert!(check_snapshot_strategy(&m, &[], false, false).is_empty());
    }
}
