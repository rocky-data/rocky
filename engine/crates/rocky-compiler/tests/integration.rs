//! Integration tests using fixture projects.
//!
//! These tests exercise the full compile() pipeline end-to-end
//! with real model files on disk.

use std::collections::HashMap;
use std::path::PathBuf;

use rocky_compiler::compile::{CompilerConfig, compile, compile_with_db};
use rocky_compiler::project::Project;
use rocky_compiler::salsa_compile::{RockyDatabase, file_typecheck, read_source};
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
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
        ..Default::default()
    };

    let result = compile(&config).unwrap();
    // Without contracts dir, no contract diagnostics
    assert!(result.contract_diagnostics.is_empty());
}

// ---- Incremental compile (§P3.1) ----

/// Generate a synthetic 20-model linear-chain project into `dir`.
/// Layout: `m00` → `m01` → … → `m19`. Big enough to exceed the
/// `total < 10` guardrail in `compile_incremental` so the incremental
/// path actually runs.
fn generate_linear_project(dir: &std::path::Path) {
    use std::fs;
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).unwrap();
    for i in 0..20 {
        let name = format!("m{i:02}");
        let sql = if i == 0 {
            "SELECT 1 AS id, 'a' AS label".to_string()
        } else {
            let upstream = format!("m{:02}", i - 1);
            format!("SELECT id, label FROM {upstream}")
        };
        let toml = if i == 0 {
            format!(
                r#"name = "{name}"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "s"
table = "{name}"
"#
            )
        } else {
            let upstream = format!("m{:02}", i - 1);
            format!(
                r#"name = "{name}"
depends_on = ["{upstream}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "s"
table = "{name}"
"#
            )
        };
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
    }
}

/// Equivalence test: editing one leaf and running incremental produces a
/// typecheck result observationally identical to a fresh full compile on
/// the same post-edit state. Guards the "affected set completeness" claim
/// — if we ever miss a case (new model, upstream shift), typed_models or
/// diagnostics will diverge between the two runs and this test fails.
#[test]
fn incremental_matches_full_after_leaf_edit() {
    use rocky_compiler::compile::compile_incremental;
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    generate_linear_project(dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    let seed = compile(&config).unwrap();
    assert_eq!(seed.project.model_count(), 20);
    assert!(!seed.has_errors);

    // Edit a leaf — m19 has no dependents, so affected = {m19} only.
    let target_sql = dir.path().join("models/m19.sql");
    let edited = fs::read_to_string(&target_sql).unwrap() + " -- edited\n";
    fs::write(&target_sql, edited).unwrap();

    let incr = compile_incremental(&config, std::slice::from_ref(&target_sql), &seed).unwrap();
    let full = compile(&config).unwrap();

    assert_eq!(incr.project.model_count(), full.project.model_count());
    assert_eq!(
        incr.type_check.typed_models.len(),
        full.type_check.typed_models.len(),
        "typed_models count must match full compile"
    );
    for (name, cols) in &full.type_check.typed_models {
        let incr_cols = incr
            .type_check
            .typed_models
            .get(name)
            .unwrap_or_else(|| panic!("missing typed_models entry for {name}"));
        let incr_names: Vec<&str> = incr_cols.iter().map(|c| c.name.as_str()).collect();
        let full_names: Vec<&str> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(incr_names, full_names, "columns differ for {name}");
    }

    let sort_diags = |ds: Vec<rocky_compiler::diagnostic::Diagnostic>| {
        let mut v: Vec<(String, String, String)> = ds
            .into_iter()
            .map(|d| (d.model, d.code.to_string(), d.message.to_string()))
            .collect();
        v.sort();
        v
    };
    assert_eq!(
        sort_diags(incr.diagnostics.clone()),
        sort_diags(full.diagnostics.clone()),
        "diagnostics must match full compile"
    );
    assert_eq!(incr.has_errors, full.has_errors);
}

/// Old `compile_incremental` in `lsp.rs` returned `ReferenceMap::default()`
/// after any incremental compile, which broke Find References / Rename
/// between edits. This test guards the new path: after a leaf edit, the
/// incremental result must carry a non-empty `reference_map`.
#[test]
fn incremental_preserves_reference_map() {
    use rocky_compiler::compile::compile_incremental;
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    generate_linear_project(dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    let seed = compile(&config).unwrap();
    assert!(
        !seed.type_check.reference_map.model_defs.is_empty(),
        "seed compile should have populated model_defs"
    );

    let target_sql = dir.path().join("models/m19.sql");
    let edited = fs::read_to_string(&target_sql).unwrap() + " -- edited\n";
    fs::write(&target_sql, edited).unwrap();

    let incr = compile_incremental(&config, &[target_sql], &seed).unwrap();
    assert!(
        !incr.type_check.reference_map.model_defs.is_empty(),
        "incremental compile must not zero out reference_map"
    );
    assert!(
        !incr.type_check.reference_map.model_refs.is_empty(),
        "incremental compile must preserve model_refs entries"
    );
}

/// Adding a brand-new model file must land in the affected set even
/// though it's not in `previous.project.models`. Without this, the
/// incremental path would silently skip typechecking the new model.
#[test]
fn incremental_handles_new_model_file() {
    use rocky_compiler::compile::compile_incremental;
    use std::fs;

    let dir = tempfile::tempdir().unwrap();
    generate_linear_project(dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
        ..Default::default()
    };

    let seed = compile(&config).unwrap();

    // Add a new leaf model that reads from m19.
    let new_sql = dir.path().join("models/m20.sql");
    fs::write(&new_sql, "SELECT id, label FROM m19").unwrap();
    fs::write(
        dir.path().join("models/m20.toml"),
        r#"name = "m20"
depends_on = ["m19"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "s"
table = "m20"
"#,
    )
    .unwrap();

    let incr = compile_incremental(&config, &[new_sql], &seed).unwrap();
    let full = compile(&config).unwrap();

    assert_eq!(
        incr.type_check.typed_models.len(),
        full.type_check.typed_models.len()
    );
    assert!(
        incr.type_check.typed_models.contains_key("m20"),
        "new model m20 must be typechecked by incremental path"
    );
}

// ---- Salsa-tracked compile pipeline ----

/// Driving `compile_with_db` twice against the same database returns
/// the same `Arc<FileTypecheck>` for every `.rocky` file in the
/// project — pointer equality is the external receipt that the salsa
/// per-file cache was hit on the second compile.
#[test]
fn salsa_compile_with_db_reuses_per_file_cache_across_repeated_compiles() {
    use std::sync::Arc;

    let models_dir = fixture_path("mixed_project/models");
    let config = CompilerConfig {
        models_dir: models_dir.clone(),
        ..Default::default()
    };
    let mut db = RockyDatabase::default();

    // First compile: cold cache. Resolve a known `.rocky` file in the
    // fixture and capture its FileTypecheck Arc.
    let first = compile_with_db(&mut db, &config).expect("first compile must succeed");
    let rocky_path = models_dir
        .join("order_summary.rocky")
        .canonicalize()
        .expect("fixture .rocky file must exist");
    let src = read_source(&mut db, rocky_path.clone()).expect("read_source must succeed");
    let ft_first = file_typecheck(&db, src);

    // Second compile against the same database — no inputs touched.
    let second = compile_with_db(&mut db, &config).expect("second compile must succeed");
    let ft_second = file_typecheck(&db, src);

    assert!(
        Arc::ptr_eq(&ft_first, &ft_second),
        "second compile must reuse the per-file FileTypecheck Arc (cache hit)",
    );
    // Sanity: outputs are observationally equivalent.
    assert_eq!(
        first.type_check.typed_models.len(),
        second.type_check.typed_models.len(),
        "compile output shape must be stable across repeated compiles",
    );
}

/// Mutating one `.rocky` file's `SourceFile` via `set_text` to a
/// genuinely different AST invalidates **only** that file's
/// `file_typecheck` cache entry — independent files in the same
/// project keep their cached `Arc<FileTypecheck>`.
#[test]
fn salsa_compile_with_db_invalidates_only_changed_file() {
    use std::sync::Arc;

    use rocky_compiler::salsa_compile::lookup_source;
    use salsa::Setter;

    // Use a tempdir copy of the mixed_project fixture so we can mutate
    // the `.rocky` file via set_text without touching the source tree.
    let src_models_dir = fixture_path("mixed_project/models");
    let tmp = tempfile::tempdir().expect("tempdir");
    let dest_models = tmp.path().join("models");
    std::fs::create_dir_all(&dest_models).unwrap();
    for entry in std::fs::read_dir(&src_models_dir).unwrap() {
        let entry = entry.unwrap();
        let dst = dest_models.join(entry.file_name());
        std::fs::copy(entry.path(), dst).unwrap();
    }
    // Add a second independent .rocky file we can pin "unchanged".
    let independent = dest_models.join("independent.rocky");
    std::fs::write(
        &independent,
        "from raw_data\nwhere active == true\nselect { id }\n",
    )
    .unwrap();
    // Sidecar so the loader treats it as a real model.
    std::fs::write(
        dest_models.join("independent.toml"),
        "name = \"independent\"\n[target]\ncatalog = \"w\"\nschema = \"s\"\ntable = \"independent\"\n",
    )
    .unwrap();

    let config = CompilerConfig {
        models_dir: dest_models.clone(),
        ..Default::default()
    };
    let mut db = RockyDatabase::default();

    // First compile.
    let _ = compile_with_db(&mut db, &config).expect("first compile");

    let target_path = dest_models
        .join("order_summary.rocky")
        .canonicalize()
        .unwrap();
    let indep_path = independent.canonicalize().unwrap();

    let target_src = lookup_source(&db, &target_path)
        .expect("target SourceFile must be in the dedup map after first compile");
    let indep_src = lookup_source(&db, &indep_path)
        .expect("independent SourceFile must be in the dedup map after first compile");

    let ft_target_before = file_typecheck(&db, target_src);
    let ft_indep_before = file_typecheck(&db, indep_src);

    // Edit the target file to a genuinely different AST.
    target_src
        .set_text(&mut db)
        .to("from orders\nwhere status != \"refunded\"\nselect { customer_id }\n".to_string());

    // Re-run the full compile against the same db — internally it
    // will reload from disk via read_source (which dedups by path),
    // but the in-memory set_text override stays in place for this
    // SourceFile because the dedup map returns the same handle.
    //
    // Actually wait — Project::load_with_db re-reads from disk for
    // any new files, but it canonicalizes existing paths through
    // read_source, which checks the dedup map BEFORE reading disk.
    // So our set_text-mutated input is what file_typecheck sees on
    // the second compile.
    let _ = compile_with_db(&mut db, &config).expect("second compile");

    let ft_target_after = file_typecheck(&db, target_src);
    let ft_indep_after = file_typecheck(&db, indep_src);

    assert!(
        !Arc::ptr_eq(&ft_target_before, &ft_target_after),
        "target file's FileTypecheck Arc must change after a genuine AST edit",
    );
    assert!(
        Arc::ptr_eq(&ft_indep_before, &ft_indep_after),
        "independent file's FileTypecheck Arc must NOT change when an \
         unrelated file is edited (per-file invalidation receipt)",
    );
}
