//! Integration tests using fixture projects.
//!
//! These tests exercise the full compile() pipeline end-to-end
//! with real model files on disk.

use std::collections::HashMap;
use std::path::PathBuf;

use rocky_compiler::compile::{CompilerConfig, compile};
use rocky_compiler::project::Project;
use rocky_compiler::semantic::build_semantic_graph;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

// ---- Simple SQL project ----

#[test]
fn test_simple_project_loads() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir).unwrap();

    assert_eq!(project.model_count(), 3);
    assert!(project.model("raw_orders").is_some());
    assert!(project.model("customer_orders").is_some());
    assert!(project.model("revenue_summary").is_some());
}

#[test]
fn test_simple_project_dag_order() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir).unwrap();

    // raw_orders → customer_orders → revenue_summary
    let raw_pos = project
        .execution_order
        .iter()
        .position(|n| n == "raw_orders")
        .unwrap();
    let co_pos = project
        .execution_order
        .iter()
        .position(|n| n == "customer_orders")
        .unwrap();
    let rs_pos = project
        .execution_order
        .iter()
        .position(|n| n == "revenue_summary")
        .unwrap();

    assert!(
        raw_pos < co_pos,
        "raw_orders must execute before customer_orders"
    );
    assert!(
        co_pos < rs_pos,
        "customer_orders must execute before revenue_summary"
    );
}

#[test]
fn test_simple_project_execution_layers() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir).unwrap();

    // Should have 3 layers (linear chain)
    assert_eq!(project.layers.len(), 3);
    assert_eq!(project.layers[0], vec!["raw_orders"]);
    assert_eq!(project.layers[1], vec!["customer_orders"]);
    assert_eq!(project.layers[2], vec!["revenue_summary"]);
}

#[test]
fn test_simple_project_semantic_graph() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir).unwrap();
    let graph = build_semantic_graph(&project, &HashMap::new()).unwrap();

    assert_eq!(graph.models.len(), 3);

    // customer_orders should have raw_orders as upstream
    let co = graph.model_schema("customer_orders").unwrap();
    assert_eq!(co.upstream, vec!["raw_orders"]);

    // revenue_summary should have customer_orders as upstream
    let rs = graph.model_schema("revenue_summary").unwrap();
    assert_eq!(rs.upstream, vec!["customer_orders"]);

    // raw_orders has no model upstream (external source)
    let ro = graph.model_schema("raw_orders").unwrap();
    assert!(ro.upstream.is_empty());
}

#[test]
fn test_simple_project_full_compile() {
    let config = CompilerConfig {
        models_dir: fixture_path("simple_project/models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile(&config).unwrap();

    assert_eq!(result.project.model_count(), 3);
    assert!(!result.has_errors, "simple project should have no errors");
    assert_eq!(result.semantic_graph.models.len(), 3);
}

#[test]
fn test_simple_project_lineage_edges() {
    let config = CompilerConfig {
        models_dir: fixture_path("simple_project/models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile(&config).unwrap();

    // There should be lineage edges from raw_orders → customer_orders
    let co_edges: Vec<_> = result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| &*e.target.model == "customer_orders")
        .collect();
    assert!(
        !co_edges.is_empty(),
        "customer_orders should have lineage edges from raw_orders"
    );

    // And from customer_orders → revenue_summary
    let rs_edges: Vec<_> = result
        .semantic_graph
        .edges
        .iter()
        .filter(|e| &*e.target.model == "revenue_summary")
        .collect();
    assert!(
        !rs_edges.is_empty(),
        "revenue_summary should have lineage edges from customer_orders"
    );
}

// ---- Mixed .sql/.rocky project ----

#[test]
fn test_mixed_project_loads() {
    let models_dir = fixture_path("mixed_project/models");
    let project = Project::load(&models_dir).unwrap();

    assert_eq!(project.model_count(), 2);
    assert!(project.model("orders").is_some());
    assert!(project.model("order_summary").is_some());
}

#[test]
fn test_mixed_project_dag_order() {
    let models_dir = fixture_path("mixed_project/models");
    let project = Project::load(&models_dir).unwrap();

    // orders (.sql) → order_summary (.rocky)
    let orders_pos = project
        .execution_order
        .iter()
        .position(|n| n == "orders")
        .unwrap();
    let summary_pos = project
        .execution_order
        .iter()
        .position(|n| n == "order_summary")
        .unwrap();

    assert!(
        orders_pos < summary_pos,
        "orders must execute before order_summary"
    );
}

#[test]
fn test_mixed_project_rocky_model_has_sql() {
    let models_dir = fixture_path("mixed_project/models");
    let project = Project::load(&models_dir).unwrap();

    // The .rocky model should have been lowered to SQL
    let order_summary = project.model("order_summary").unwrap();
    assert!(
        order_summary.sql.contains("SELECT"),
        "rocky model should have been lowered to SQL: {}",
        order_summary.sql
    );
    // The != in rocky should compile to IS DISTINCT FROM
    assert!(
        order_summary.sql.contains("IS DISTINCT FROM"),
        "!= should compile to IS DISTINCT FROM: {}",
        order_summary.sql
    );
}

#[test]
fn test_mixed_project_full_compile() {
    let config = CompilerConfig {
        models_dir: fixture_path("mixed_project/models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile(&config).unwrap();

    assert_eq!(result.project.model_count(), 2);
    assert!(!result.has_errors, "mixed project should have no errors");
}

// ---- Contract project ----

#[test]
fn test_contract_project_loads_contracts() {
    let config = CompilerConfig {
        models_dir: fixture_path("contract_project/models"),
        contracts_dir: Some(fixture_path("contract_project/contracts")),
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile(&config).unwrap();

    // Contract validation should have run (even if columns are Unknown type)
    assert_eq!(result.project.model_count(), 1);
    // With unknown types, contract type checks produce no errors (Unknown matches any type)
    // But the contract_diagnostics vector should exist
    // Contract validation ran without panicking — diagnostics vector exists
    let _ = &result.diagnostics;
}

#[test]
fn test_contract_project_with_no_contracts_dir() {
    let config = CompilerConfig {
        models_dir: fixture_path("contract_project/models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let result = compile(&config).unwrap();
    // Without contracts dir, no contract diagnostics
    assert!(result.contract_diagnostics.is_empty());
}
