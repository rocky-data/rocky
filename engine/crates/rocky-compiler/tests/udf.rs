//! User-defined functions (`functions/`) through the full compile pipeline.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use rocky_compiler::compile::{CompilerConfig, compile, compile_incremental};
use rocky_compiler::types::{RockyType, TypedColumn};

const CENTS_TOML: &str = r#"
returns = "DOUBLE"

[[arguments]]
name = "cents"
type = "BIGINT"
"#;

fn write_model(root: &Path, name: &str, sql: &str) {
    fs::write(root.join(format!("models/{name}.sql")), sql).unwrap();
    fs::write(
        root.join(format!("models/{name}.toml")),
        format!(
            "[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\n\
             schema = \"s\"\ntable = \"{name}\"\n"
        ),
    )
    .unwrap();
}

fn write_function(root: &Path, name: &str, toml: &str, body: &str) {
    fs::write(root.join(format!("functions/{name}.toml")), toml).unwrap();
    fs::write(root.join(format!("functions/{name}.sql")), body).unwrap();
}

fn project(root: &Path) {
    fs::create_dir_all(root.join("models")).unwrap();
    fs::create_dir_all(root.join("functions")).unwrap();
    write_function(root, "cents_to_dollars", CENTS_TOML, "cents / 100.0");
}

fn config(root: &Path, with_source: bool) -> CompilerConfig {
    let mut source_schemas = HashMap::new();
    if with_source {
        source_schemas.insert(
            "raw.orders".to_string(),
            vec![
                TypedColumn {
                    name: "order_id".to_string(),
                    data_type: RockyType::Int64,
                    nullable: false,
                },
                TypedColumn {
                    name: "amount_cents".to_string(),
                    data_type: RockyType::Int64,
                    nullable: true,
                },
            ],
        );
    }
    CompilerConfig {
        models_dir: root.join("models"),
        source_schemas,
        ..Default::default()
    }
}

fn column_type(
    result: &rocky_compiler::compile::CompileResult,
    model: &str,
    column: &str,
) -> RockyType {
    result.type_check.typed_models[model]
        .iter()
        .find(|c| c.name == column)
        .unwrap_or_else(|| panic!("{model}.{column} missing"))
        .data_type
        .clone()
}

#[test]
fn udf_return_type_flows_downstream_and_lineage_reaches_the_argument() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    project(root);
    write_model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders",
    );
    write_model(
        root,
        "rpt_orders",
        "SELECT order_id, amount_usd FROM fct_orders",
    );

    let result = compile(&config(root, true)).unwrap();
    assert!(!result.has_errors, "{:#?}", result.diagnostics);
    assert!(
        result.diagnostics.iter().all(|d| !d.code.ends_with("051")),
        "a typed, well-formed call emits nothing: {:#?}",
        result.diagnostics
    );
    assert_eq!(
        column_type(&result, "fct_orders", "amount_usd"),
        RockyType::Float64
    );
    assert_eq!(
        column_type(&result, "rpt_orders", "amount_usd"),
        RockyType::Float64,
        "the declared return type propagates to downstream models"
    );

    // Column lineage: the UDF output derives from its argument column.
    let edge = result
        .semantic_graph
        .producing_edge("fct_orders", "amount_usd")
        .expect("the UDF output has a lineage edge");
    assert_eq!(&*edge.source.model, "raw.orders");
    assert_eq!(&*edge.source.column, "amount_cents");
}

#[test]
fn unknown_argument_type_is_w051_and_still_returns_the_declared_type() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    project(root);
    write_model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders",
    );

    let result = compile(&config(root, false)).unwrap();
    assert!(!result.has_errors, "{:#?}", result.diagnostics);
    let w051: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "W051")
        .collect();
    assert_eq!(w051.len(), 1, "{:#?}", result.diagnostics);
    assert_eq!(w051[0].model, "fct_orders");
    assert!(w051[0].message.contains("cannot verify argument 1"));
    assert_eq!(
        column_type(&result, "fct_orders", "amount_usd"),
        RockyType::Float64
    );
}

#[test]
fn without_a_functions_dir_unknown_calls_stay_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    fs::create_dir_all(root.join("models")).unwrap();
    write_model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders",
    );
    let result = compile(&config(root, true)).unwrap();
    assert!(!result.has_errors);
    assert_eq!(
        column_type(&result, "fct_orders", "amount_usd"),
        RockyType::Unknown
    );
}

/// A function edit changes the type of every caller, but the incremental
/// path's affected set tracks only model files — so it must fall through to
/// a full compile and match it.
#[test]
fn incremental_compile_sees_a_function_change() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    project(root);
    write_model(
        root,
        "fct_orders",
        "SELECT order_id, cents_to_dollars(amount_cents) AS amount_usd FROM raw.orders",
    );
    // Enough unrelated models that the incremental path does not bail on size.
    for i in 0..12 {
        write_model(root, &format!("m{i}"), "SELECT order_id FROM raw.orders");
    }
    let cfg = config(root, true);
    let first = compile(&cfg).unwrap();
    assert_eq!(
        column_type(&first, "fct_orders", "amount_usd"),
        RockyType::Float64
    );

    write_function(
        root,
        "cents_to_dollars",
        &CENTS_TOML.replace("DOUBLE", "VARCHAR"),
        "CAST(cents AS VARCHAR)",
    );
    let edited_model = root.join("models/m0.sql");
    fs::write(&edited_model, "SELECT order_id FROM raw.orders -- edit").unwrap();
    let incremental =
        compile_incremental(&cfg, std::slice::from_ref(&edited_model), &first).unwrap();
    assert_eq!(
        column_type(&incremental, "fct_orders", "amount_usd"),
        RockyType::String,
        "a stale return type must not survive a function edit"
    );
}
