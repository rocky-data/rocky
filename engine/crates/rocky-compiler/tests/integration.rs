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
    let project = Project::load(&models_dir, None).unwrap();

    assert_eq!(project.model_count(), 3);
    assert!(project.model("raw_orders").is_some());
    assert!(project.model("customer_orders").is_some());
    assert!(project.model("revenue_summary").is_some());
}

#[test]
fn test_simple_project_dag_order() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir, None).unwrap();

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
    let project = Project::load(&models_dir, None).unwrap();

    // Should have 3 layers (linear chain)
    assert_eq!(project.layers.len(), 3);
    assert_eq!(project.layers[0], vec!["raw_orders"]);
    assert_eq!(project.layers[1], vec!["customer_orders"]);
    assert_eq!(project.layers[2], vec!["revenue_summary"]);
}

#[test]
fn test_simple_project_semantic_graph() {
    let models_dir = fixture_path("simple_project/models");
    let project = Project::load(&models_dir, None).unwrap();
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
    let project = Project::load(&models_dir, None).unwrap();

    assert_eq!(project.model_count(), 2);
    assert!(project.model("orders").is_some());
    assert!(project.model("order_summary").is_some());
}

#[test]
fn test_mixed_project_dag_order() {
    let models_dir = fixture_path("mixed_project/models");
    let project = Project::load(&models_dir, None).unwrap();

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
    let project = Project::load(&models_dir, None).unwrap();

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
        ..Default::default()
    };

    let result = compile(&config).unwrap();

    // Contract validation should have run (even if columns are Unknown type)
    assert_eq!(result.project.model_count(), 1);

    // No source schemas, so every column infers `Unknown` and the E011 type
    // check cannot run. That must not raise E011 — and must not be silent
    // either: one I003 per contract column that declares a type (#1240).
    // (The fixture's own `nullable = false` columns still raise E012; that is
    // a separate check and unrelated to the type gate.)
    assert!(
        result
            .contract_diagnostics
            .iter()
            .all(|d| &*d.code != "E011"),
        "an unresolved type must not raise E011: {:?}",
        result.contract_diagnostics
    );
    let i003: Vec<_> = result
        .contract_diagnostics
        .iter()
        .filter(|d| &*d.code == "I003")
        .collect();
    assert_eq!(
        i003.len(),
        2,
        "one I003 per typed contract column (customer_id, total_revenue): {:?}",
        result.contract_diagnostics
    );
    assert!(
        i003.iter()
            .all(|d| d.severity == rocky_compiler::diagnostic::Severity::Info),
        "I003 must be info severity: {i003:?}"
    );
}

#[test]
fn test_contract_project_reads_its_contracts_dir_without_a_flag() {
    let discovered = CompilerConfig {
        models_dir: fixture_path("contract_project/models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };
    let explicit = CompilerConfig {
        contracts_dir: Some(fixture_path("contract_project/contracts")),
        ..discovered.clone()
    };

    let codes = |config: &CompilerConfig| {
        let mut codes: Vec<(String, String)> = compile(config)
            .unwrap()
            .contract_diagnostics
            .iter()
            .map(|d| (d.model.clone(), d.code.to_string()))
            .collect();
        codes.sort();
        codes
    };
    // The project `contracts/` beside `models/` is read with no flag, and
    // gives the same contract diagnostics as passing it explicitly.
    let found = codes(&discovered);
    assert!(!found.is_empty(), "the project contracts dir must be read");
    assert_eq!(found, codes(&explicit));
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

// ---- Per-run variables (`@var()` substitution at compile time) ----

/// Write a single `.sql` model (plus a minimal `.toml` sidecar) into a fresh
/// temp models dir and return the dir handle so it stays alive for the test.
fn temp_model_project(sql: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let models = dir.path().join("models");
    std::fs::create_dir_all(&models).unwrap();
    std::fs::write(models.join("m.sql"), sql).unwrap();
    std::fs::write(
        models.join("m.toml"),
        "name = \"m\"\n\n[target]\ncatalog = \"cat\"\nschema = \"sch\"\ntable = \"m\"\n",
    )
    .unwrap();
    dir
}

#[test]
fn run_var_substituted_into_model_sql_at_compile_time() {
    let dir = temp_model_project("SELECT * FROM raw.base WHERE region = '@var(region)'");
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        run_vars: rocky_core::run_vars::RunVars::parse_pairs(["region=us"]).unwrap(),
        ..Default::default()
    };

    let result = compile(&config).unwrap();

    let model = result.project.model("m").unwrap();
    assert_eq!(
        model.sql, "SELECT * FROM raw.base WHERE region = 'us'",
        "the @var(region) marker must resolve to the supplied value"
    );
    assert!(
        !result.diagnostics.iter().any(|d| &*d.code == "E028"),
        "no missing-var diagnostic when the value is supplied"
    );
}

#[test]
fn missing_required_run_var_is_a_named_compile_error() {
    let dir = temp_model_project("SELECT * FROM raw.base WHERE region = '@var(region)'");
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        // No `region` supplied and no inline default.
        ..Default::default()
    };

    let result = compile(&config).unwrap();

    assert!(
        result.has_errors,
        "a missing required var must fail compile"
    );
    let e028: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E028")
        .collect();
    assert_eq!(e028.len(), 1, "exactly one E028 for the one missing var");
    assert!(
        e028[0].message.contains("region"),
        "the error must name the missing variable: {}",
        e028[0].message
    );
}

#[test]
fn run_var_inline_default_used_when_unset() {
    let dir = temp_model_project("SELECT * FROM raw.base WHERE d = '@var(drop_date, 2024-01-01)'");
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        // `drop_date` not supplied → falls back to the inline default.
        ..Default::default()
    };

    let result = compile(&config).unwrap();

    let model = result.project.model("m").unwrap();
    assert_eq!(model.sql, "SELECT * FROM raw.base WHERE d = '2024-01-01'");
    assert!(
        !result.has_errors,
        "an inline default satisfies the reference; no E028"
    );
}

/// Write a two-model project: a clean root `up` and a downstream `down` whose
/// SQL the caller supplies (it should reference `up`). Neither sidecar sets
/// `depends_on`, so the `up → down` edge can only come from lineage extraction
/// over `down`'s SQL — which is exactly what we want to prove survives `@var`
/// substitution.
fn temp_linear_two_model_project(down_sql: &str) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let models = dir.path().join("models");
    std::fs::create_dir_all(&models).unwrap();
    std::fs::write(models.join("up.sql"), "SELECT 1 AS id, 100 AS amount").unwrap();
    std::fs::write(
        models.join("up.toml"),
        "name = \"up\"\n\n[target]\ncatalog = \"cat\"\nschema = \"sch\"\ntable = \"up\"\n",
    )
    .unwrap();
    std::fs::write(models.join("down.sql"), down_sql).unwrap();
    std::fs::write(
        models.join("down.toml"),
        "name = \"down\"\n\n[target]\ncatalog = \"cat\"\nschema = \"sch\"\ntable = \"down\"\n",
    )
    .unwrap();
    dir
}

/// Regression: a BARE `@var(...)` marker outside any string literal
/// (`WHERE amount >= @var(threshold)`) must compile clean. Before run-var
/// substitution was moved ahead of lineage extraction, this crashed compile
/// with a sqlparser `Expected: end of statement, found: (` error during
/// dependency resolution — the raw `@var(` text reached the parser. The
/// existing `run_vars` unit test never caught it because it exercised
/// `substitute_run_vars` in isolation, not the lineage → compile path.
#[test]
fn bare_run_var_outside_string_literal_compiles_and_keeps_dependency() {
    let dir =
        temp_linear_two_model_project("SELECT id, amount FROM up WHERE amount >= @var(threshold)");
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        run_vars: rocky_core::run_vars::RunVars::parse_pairs(["threshold=100"]).unwrap(),
        ..Default::default()
    };

    // The crux: this used to return `Err(CompileError::Project(..))`.
    let result = compile(&config).unwrap();

    assert!(
        !result.has_errors,
        "a bare @var must compile clean: {:?}",
        result.diagnostics
    );

    let down = result.project.model("down").unwrap();
    assert_eq!(
        down.sql, "SELECT id, amount FROM up WHERE amount >= 100",
        "the bare @var(threshold) marker must resolve to the supplied value"
    );
    assert!(
        !down.sql.contains("@var("),
        "no @var() marker may remain in the executable SQL"
    );

    // The `up → down` dependency must still be auto-resolved from the
    // substituted SQL: a bare @var sitting next to a real table ref doesn't
    // drop the edge.
    let up_pos = result
        .project
        .execution_order
        .iter()
        .position(|n| n == "up")
        .expect("up must be in the execution order");
    let down_pos = result
        .project
        .execution_order
        .iter()
        .position(|n| n == "down")
        .expect("down must be in the execution order");
    assert!(
        up_pos < down_pos,
        "up must resolve as a dependency of down and execute first"
    );
}

/// A missing required BARE `@var` renders to the parseable `NULL` sentinel, so
/// compile returns `Ok` with an E028 diagnostic naming the variable rather
/// than crashing in lineage extraction.
#[test]
fn missing_bare_run_var_yields_e028_not_a_parser_crash() {
    let dir =
        temp_linear_two_model_project("SELECT id, amount FROM up WHERE amount >= @var(threshold)");
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        // No `threshold` supplied and no inline default.
        ..Default::default()
    };

    let result = compile(&config).unwrap();

    assert!(
        result.has_errors,
        "a missing required bare var must fail compile"
    );
    let e028: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E028")
        .collect();
    assert_eq!(e028.len(), 1, "exactly one E028 for the one missing var");
    assert!(
        e028[0].message.contains("threshold"),
        "the error must name the missing variable: {}",
        e028[0].message
    );
}

/// A leaf `SELECT *` over an EXTERNAL source must expand to that source's
/// columns once its schema is known.
///
/// `semantic.rs`'s external-source star-expansion arm read a map that no
/// production caller ever filled, so the arm was dead code and such a model
/// reported ZERO columns in `rocky catalog` and `rocky docs` (#1484). The map
/// is now derived from `source_schemas`, which the compile path does populate.
#[test]
fn leaf_star_over_a_known_external_source_expands_to_its_columns() {
    use rocky_compiler::types::TypedColumn;
    use rocky_ir::RockyType;

    // A model whose whole body is a star over a table no model produces.
    let dir = tempfile::tempdir().expect("tempdir");
    let models = dir.path().join("models");
    std::fs::create_dir(&models).expect("models dir");
    std::fs::write(
        models.join("leaf.sql"),
        "SELECT * FROM warehouse.main.raw_orders\n",
    )
    .expect("sql");
    std::fs::write(
        models.join("leaf.toml"),
        "name = \"leaf\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
         [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"leaf\"\n",
    )
    .expect("sidecar");

    let source_cols = vec![
        TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: false,
        },
        TypedColumn {
            name: "amount".to_string(),
            data_type: RockyType::Float64,
            nullable: true,
        },
    ];

    // Baseline: with NO source schema the star cannot expand, and the model
    // reports nothing. This is what every production caller used to get.
    let bare = CompilerConfig {
        models_dir: models.clone(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };
    let without = compile(&bare).unwrap();
    let cols_without = without
        .semantic_graph
        .model_schema("leaf")
        .map(|m| m.columns.len())
        .unwrap_or(0);

    // With the source schema known, the columns must appear.
    let mut source_schemas = HashMap::new();
    source_schemas.insert("warehouse.main.raw_orders".to_string(), source_cols);
    let config = CompilerConfig {
        models_dir: models,
        contracts_dir: None,
        source_schemas,
        ..Default::default()
    };
    let with = compile(&config).unwrap();
    let schema = with
        .semantic_graph
        .model_schema("leaf")
        .expect("the leaf model must be in the graph");
    let names: Vec<String> = schema.columns.iter().map(|c| c.name.clone()).collect();

    assert!(
        names.contains(&"id".to_string()) && names.contains(&"amount".to_string()),
        "a known external source's columns must expand the star; got {names:?} \
         (without a source schema it reported {cols_without})"
    );
    assert!(
        cols_without < names.len(),
        "the source schema must be what makes the difference — bare compile \
         reported {cols_without}, schema-aware reported {}",
        names.len()
    );
}

// ---- #1990: `incremental` is refused on transformation models ----

/// Write a two-model project whose leaf declares `leaf_strategy`.
fn write_strategy_project(dir: &std::path::Path, leaf_strategy: &str) {
    write_strategy_project_with_sql(dir, leaf_strategy, "SELECT id, updated_at FROM src");
}

/// [`write_strategy_project`] with the leaf's SQL given.
fn write_strategy_project_with_sql(dir: &std::path::Path, leaf_strategy: &str, leaf_sql: &str) {
    use std::fs;
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).unwrap();
    fs::write(
        models_dir.join("src.sql"),
        "SELECT 1 AS id, CAST('2026-01-01' AS DATE) AS updated_at",
    )
    .unwrap();
    fs::write(
        models_dir.join("src.toml"),
        "name = \"src\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"src\"\n",
    )
    .unwrap();
    fs::write(models_dir.join("leaf.sql"), leaf_sql).unwrap();
    fs::write(
        models_dir.join("leaf.toml"),
        format!(
            "name = \"leaf\"\ndepends_on = [\"src\"]\n\n[strategy]\n{leaf_strategy}\n\n[target]\ncatalog = \"warehouse\"\nschema = \"s\"\ntable = \"leaf\"\n"
        ),
    )
    .unwrap();
}

fn compile_strategy_project(leaf_strategy: &str) -> rocky_compiler::compile::CompileResult {
    let dir = tempfile::tempdir().unwrap();
    write_strategy_project(dir.path(), leaf_strategy);
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };
    compile(&config).unwrap()
}

/// The refusal must be an ERROR on the model that declares the strategy:
/// `rocky run` excludes a model from execution only for error-severity
/// diagnostics keyed on its name, and `rocky test` / `emit-sql` refuse on
/// `has_errors`. A warning would leave the duplicating INSERT running.
fn compile_leaf(leaf_strategy: &str, leaf_sql: &str) -> rocky_compiler::compile::CompileResult {
    let dir = tempfile::tempdir().unwrap();
    write_strategy_project_with_sql(dir.path(), leaf_strategy, leaf_sql);
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };
    compile(&config).unwrap()
}

fn codes_on_leaf(result: &rocky_compiler::compile::CompileResult) -> Vec<String> {
    result
        .diagnostics
        .iter()
        .filter(|d| d.model == "leaf")
        .map(|d| d.code.to_string())
        .collect()
}

#[test]
fn an_incremental_transformation_model_is_refused_with_e037() {
    // No watermark: the only SQL left would be an unfiltered INSERT.
    let result = compile_strategy_project("type = \"incremental\"");

    let e037: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E037")
        .collect();
    assert_eq!(
        e037.len(),
        1,
        "exactly one E037, got: {:?}",
        result.diagnostics
    );
    let d = e037[0];
    assert!(
        d.is_error(),
        "E037 must be an error so the model is excluded from execution"
    );
    assert_eq!(
        d.model, "leaf",
        "the diagnostic names the model that declares the strategy"
    );
    let suggestion = d.suggestion.as_deref().unwrap_or_default();
    for working in ["merge", "delete_insert", "time_interval", "full_refresh"] {
        assert!(
            suggestion.contains(working),
            "suggestion names `{working}`: {suggestion}"
        );
    }
    assert!(
        !suggestion.contains("microbatch"),
        "the suggestion uses the canonical time_interval spelling"
    );
    assert!(
        suggestion.contains("timestamp_column") && suggestion.contains("@incremental_filter"),
        "the suggestion points at the watermark config: {suggestion}"
    );
    assert!(result.has_errors, "an E037 must make the compile fail");
}

// ---- WP6: incremental transformation models with a watermark ----

const INCREMENTAL_WM: &str = "type = \"incremental\"\ntimestamp_column = \"updated_at\"";

/// Valid controls: a placeholder, a passthrough watermark without one, the
/// `watermark` alias, and a keyed lookback all compile with no E037/E046/W046.
#[test]
fn incremental_models_with_a_safe_watermark_compile_clean() {
    let cases = [
        (
            INCREMENTAL_WM,
            "SELECT id, updated_at FROM src WHERE @incremental_filter",
        ),
        (INCREMENTAL_WM, "SELECT id, updated_at FROM src"),
        (
            "type = \"incremental\"\nwatermark = \"updated_at\"",
            "SELECT s.id, s.updated_at FROM src AS s",
        ),
        (
            "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nunique_key = [\"id\"]\n\
             lookback = \"3 days\"\nfilter_column = \"s.updated_at\"",
            "SELECT s.id, s.updated_at FROM src AS s WHERE @incremental_filter AND s.id > 0",
        ),
    ];
    for (strategy, sql) in cases {
        let result = compile_leaf(strategy, sql);
        let codes = codes_on_leaf(&result);
        assert!(
            !codes
                .iter()
                .any(|c| matches!(c.as_str(), "E037" | "E046" | "W046")),
            "{sql}: unexpected {codes:?} in {:?}",
            result.diagnostics
        );
        assert!(!result.has_errors, "{sql}: {:?}", result.diagnostics);
    }
}

/// Without a placeholder, filtering the output is only sound when the
/// watermark is copied unchanged from one physical input table. An aggregate,
/// a cast, a column read through a CTE or derived table, or a top-level LIMIT
/// is refused with E046, also when a placeholder sits only in a comment.
#[test]
fn incremental_without_a_provable_filter_place_is_refused_with_e046() {
    for sql in [
        "SELECT id, MAX(updated_at) AS updated_at FROM src GROUP BY id",
        "SELECT id, CAST(updated_at AS TIMESTAMP) AS updated_at FROM src",
        "SELECT id, updated_at + INTERVAL 1 DAY AS updated_at FROM src -- WHERE @incremental_filter",
        // Direct at the top, but the CTE body aggregates.
        "WITH s AS (SELECT id, MAX(updated_at) AS updated_at FROM src GROUP BY id) \
         SELECT id, updated_at FROM s",
        // Same through a derived table.
        "SELECT d.id, d.updated_at FROM (SELECT id, MAX(updated_at) AS updated_at FROM src \
         GROUP BY id) AS d",
        // LIMIT picks rows before the output filter would.
        "SELECT id, updated_at FROM src ORDER BY updated_at LIMIT 10",
    ] {
        let result = compile_leaf(INCREMENTAL_WM, sql);
        let e046: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "E046" && d.model == "leaf")
            .collect();
        assert_eq!(e046.len(), 1, "{sql}: {:?}", result.diagnostics);
        assert!(
            e046[0].is_error(),
            "{sql}: E046 must exclude the model from runs"
        );
        assert!(
            e046[0]
                .suggestion
                .as_deref()
                .unwrap_or_default()
                .contains("@incremental_filter"),
            "{sql}: the suggestion says where the placeholder goes"
        );
        assert!(result.has_errors);
    }
}

#[test]
fn incremental_watermark_must_be_an_output_column() {
    let result = compile_leaf(
        INCREMENTAL_WM,
        "SELECT id FROM src WHERE @incremental_filter",
    );
    let e046: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E046" && d.model == "leaf")
        .collect();
    assert_eq!(e046.len(), 1, "{:?}", result.diagnostics);
    assert!(
        e046[0].message.contains("not an output column"),
        "{}",
        e046[0].message
    );
}

#[test]
fn placeholder_under_another_strategy_is_refused_with_e046() {
    let result = compile_leaf(
        "type = \"full_refresh\"",
        "SELECT id, updated_at FROM src WHERE @incremental_filter",
    );
    assert!(
        codes_on_leaf(&result).contains(&"E046".to_string()),
        "{:?}",
        result.diagnostics
    );
    // A literal or commented placeholder is not a placeholder.
    let result = compile_leaf(
        "type = \"full_refresh\"",
        "SELECT id, '@incremental_filter' AS note FROM src -- @incremental_filter",
    );
    assert!(
        !codes_on_leaf(&result).contains(&"E046".to_string()),
        "{:?}",
        result.diagnostics
    );
}

#[test]
fn incremental_filter_column_must_be_a_column_reference() {
    let result = compile_leaf(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nfilter_column = \"a.b.c\"",
        "SELECT id, updated_at FROM src WHERE @incremental_filter",
    );
    assert!(
        codes_on_leaf(&result).contains(&"E046".to_string()),
        "{:?}",
        result.diagnostics
    );
}

/// A lookback re-reads rows already in the target; without a key to merge
/// on, they are appended again. Warning, not error.
#[test]
fn lookback_without_unique_key_warns_w046() {
    let result = compile_leaf(
        "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nlookback = \"1 day\"",
        "SELECT id, updated_at FROM src WHERE @incremental_filter",
    );
    let w046: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "W046" && d.model == "leaf")
        .collect();
    assert_eq!(w046.len(), 1, "{:?}", result.diagnostics);
    assert!(!w046[0].is_error());
    assert!(!result.has_errors, "{:?}", result.diagnostics);
}

/// The strict `>` watermark skips a late row whose timestamp equals the
/// target's `MAX`. With no `lookback` the compiler warns (W056), with or
/// without `unique_key`: a key only merges rows the filter reads, and the
/// filter never reads that row. A lookback silences it. Warning, not error.
#[test]
fn append_only_incremental_without_lookback_warns_w056() {
    let sql = "SELECT id, updated_at FROM src WHERE @incremental_filter";
    let w056 = |strategy: &str| {
        compile_leaf(strategy, sql)
            .diagnostics
            .iter()
            .filter(|d| &*d.code == "W056" && d.model == "leaf")
            .count()
    };
    assert_eq!(w056(INCREMENTAL_WM), 1);
    assert_eq!(
        w056("type = \"incremental\"\ntimestamp_column = \"updated_at\"\nlookback = \"0 days\""),
        1,
        "a zero lookback is no lookback"
    );
    assert_eq!(
        w056("type = \"incremental\"\ntimestamp_column = \"updated_at\"\nlookback = \"1 day\""),
        0
    );
    assert_eq!(
        w056("type = \"incremental\"\ntimestamp_column = \"updated_at\"\nunique_key = [\"id\"]"),
        1,
        "unique_key without lookback still loses the late row"
    );
    assert_eq!(
        w056(
            "type = \"incremental\"\ntimestamp_column = \"updated_at\"\nunique_key = [\"id\"]\n\
             lookback = \"1 day\""
        ),
        0
    );
    assert!(!compile_leaf(INCREMENTAL_WM, sql).has_errors);
}

/// #1996: `type = "ephemeral"` used to be refused outright with E038, because
/// nothing inlined it. Consumers now inline it as a CTE, so a plain ephemeral
/// model compiles clean; E038 only marks the uses inlining cannot serve
/// (`ephemeral.rs`).
#[test]
fn an_ephemeral_model_compiles_clean() {
    let result = compile_strategy_project("type = \"ephemeral\"");
    assert!(
        !result.diagnostics.iter().any(|d| &*d.code == "E038"),
        "a plain ephemeral model is not an invalid use: {:?}",
        result.diagnostics
    );
    assert!(!result.has_errors, "{:?}", result.diagnostics);
}

/// Boundary: no other strategy a model can declare produces E038.
#[test]
fn e038_does_not_fire_for_other_strategies() {
    for strategy in [
        "type = \"full_refresh\"",
        "type = \"view\"",
        "type = \"merge\"\nunique_key = [\"id\"]",
    ] {
        let result = compile_strategy_project(strategy);
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "E038"),
            "{strategy}: unexpected E038 in {:?}",
            result.diagnostics
        );
    }
}

/// E037 is scoped to `incremental`. A `microbatch` model is checked as
/// `time_interval` and gets E024 if it omits its window placeholders.
#[test]
fn e037_does_not_fire_for_other_strategies() {
    for strategy in [
        "type = \"full_refresh\"",
        "type = \"microbatch\"\ntimestamp_column = \"updated_at\"\ngranularity = \"day\"",
    ] {
        let result = compile_strategy_project(strategy);
        assert!(
            !result.diagnostics.iter().any(|d| &*d.code == "E037"),
            "{strategy}: unexpected E037 in {:?}",
            result.diagnostics
        );
    }
}

/// #2233: placeholders that sit only in a comment bound nothing. `rocky run`
/// would copy every source row on each partition run, so compile refuses
/// the model with E024 before anything runs.
#[test]
fn time_interval_with_comment_only_placeholders_is_refused_with_e024() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("fct_events.toml"),
        "[strategy]\ntype = \"time_interval\"\ntime_column = \"event_at\"\n\
         granularity = \"day\"\n\n[target]\ncatalog = \"\"\nschema = \"main\"\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("fct_events.sql"),
        "SELECT id, event_at FROM raw.events /* @start_date @end_date */",
    )
    .unwrap();
    let result = compile(&CompilerConfig {
        models_dir: dir.path().to_path_buf(),
        ..Default::default()
    })
    .unwrap();
    assert!(result.has_errors, "{:?}", result.diagnostics);
    assert!(
        result
            .diagnostics
            .iter()
            .any(|d| &*d.code == "E024" && d.model == "fct_events" && d.is_error()),
        "comment-only placeholders must fail with E024: {:?}",
        result.diagnostics
    );
}

#[test]
fn microbatch_without_window_is_refused_with_e024() {
    let result = compile_strategy_project(
        "type = \"microbatch\"\ntimestamp_column = \"updated_at\"\ngranularity = \"day\"",
    );
    let e024: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E024" && d.model == "leaf")
        .collect();
    assert_eq!(e024.len(), 1, "expected E024: {:?}", result.diagnostics);
    assert!(e024[0].is_error());
    assert!(result.has_errors);
}

#[test]
fn microbatch_with_window_compiles_as_time_interval() {
    use rocky_core::models::StrategyConfig;
    use rocky_ir::{MaterializationStrategy, TimeGrain};

    let dir = tempfile::tempdir().unwrap();
    write_strategy_project(
        dir.path(),
        "type = \"microbatch\"\ntimestamp_column = \"updated_at\"\ngranularity = \"day\"",
    );
    std::fs::write(
        dir.path().join("models/leaf.sql"),
        "SELECT id, updated_at FROM src WHERE updated_at >= @start_date AND updated_at < @end_date",
    )
    .unwrap();
    let result = compile(&CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    })
    .unwrap();
    assert!(!result.has_errors, "{:?}", result.diagnostics);
    let model = result.project.model("leaf").unwrap();
    assert!(matches!(
        &model.config.strategy,
        StrategyConfig::TimeInterval { time_column, granularity: TimeGrain::Day, .. }
            if time_column == "updated_at"
    ));
    assert!(matches!(
        model.to_model_ir().materialization,
        MaterializationStrategy::TimeInterval { time_column, granularity: TimeGrain::Day, window: None }
            if time_column == "updated_at"
    ));
}

#[test]
fn defaulted_dsl_microbatch_is_refused_and_defaulted_sql_window_compiles() {
    use rocky_core::models::StrategyConfig;
    use rocky_ir::{MaterializationStrategy, TimeGrain};

    let dir = tempfile::tempdir().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir(&models_dir).unwrap();
    std::fs::write(
        models_dir.join("_defaults.toml"),
        "[target]\ncatalog = \"warehouse\"\nschema = \"s\"\n\
         [strategy]\ntype = \"microbatch\"\ntimestamp_column = \"updated_at\"\n\
         granularity = \"day\"\n",
    )
    .unwrap();
    std::fs::write(
        models_dir.join("unbounded.rocky"),
        "from src\nselect { id, updated_at }\n",
    )
    .unwrap();
    std::fs::write(
        models_dir.join("bounded.sql"),
        "SELECT id, updated_at FROM src WHERE updated_at >= @start_date AND updated_at < @end_date",
    )
    .unwrap();
    std::fs::write(models_dir.join("bounded.toml"), "name = \"bounded\"\n").unwrap();

    let loaded = rocky_compiler::project::load_dir_models(&models_dir, None).unwrap();
    for name in ["unbounded", "bounded"] {
        let model = loaded
            .iter()
            .find(|model| model.config.name == name)
            .unwrap();
        assert!(
            matches!(
                &model.config.strategy,
                StrategyConfig::TimeInterval { time_column, granularity: TimeGrain::Day, .. }
                    if time_column == "updated_at"
            ),
            "{name} must normalize during loading"
        );
    }

    let result = compile(&CompilerConfig {
        models_dir: models_dir.clone(),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    })
    .unwrap();
    let e024: Vec<_> = result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E024" && d.model == "unbounded")
        .collect();
    assert_eq!(e024.len(), 1, "expected DSL E024: {:?}", result.diagnostics);
    assert!(e024[0].is_error());

    let bounded = result.project.model("bounded").unwrap();
    assert!(matches!(
        &bounded.config.strategy,
        StrategyConfig::TimeInterval { time_column, granularity: TimeGrain::Day, .. }
            if time_column == "updated_at"
    ));
    assert!(matches!(
        bounded.to_model_ir().materialization,
        MaterializationStrategy::TimeInterval { time_column, granularity: TimeGrain::Day, window: None }
            if time_column == "updated_at"
    ));
    assert!(
        !result
            .diagnostics
            .iter()
            .any(|d| d.model == "bounded" && d.is_error()),
        "bounded inherited SQL must compile: {:?}",
        result.diagnostics
    );

    std::fs::remove_file(models_dir.join("unbounded.rocky")).unwrap();
    let bounded_only = compile(&CompilerConfig {
        models_dir,
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    })
    .unwrap();
    assert!(
        !bounded_only.has_errors,
        "bounded inherited SQL must compile cleanly: {:?}",
        bounded_only.diagnostics
    );
}

// ---- E041 / W041: missing external source columns ----

mod source_column_refs {
    use std::collections::HashMap;
    use std::path::Path;

    use rocky_compiler::compile::{CompilerConfig, compile};
    use rocky_compiler::diagnostic::Severity;
    use rocky_compiler::source_refs::{SourceProvenance, SourceSchemaOrigin};
    use rocky_compiler::types::{RockyType, TypedColumn};

    fn write_model(dir: &Path, name: &str, sql: &str) {
        std::fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\n\
                 catalog = \"c\"\nschema = \"s\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    fn cols(names: &[&str]) -> Vec<TypedColumn> {
        names
            .iter()
            .map(|name| TypedColumn {
                name: (*name).to_string(),
                data_type: RockyType::Unknown,
                nullable: true,
            })
            .collect()
    }

    /// The brief's seed: `raw.orders` and `raw.customers`.
    fn sources() -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([
            (
                "raw.orders".to_string(),
                cols(&["order_id", "customer_id", "amount", "status", "order_date"]),
            ),
            (
                "raw.customers".to_string(),
                cols(&["customer_id", "customer_name", "email"]),
            ),
        ])
    }

    fn compile_with(
        models: &[(&str, &str)],
        origin: Option<SourceSchemaOrigin>,
    ) -> rocky_compiler::compile::CompileResult {
        let dir = tempfile::TempDir::new().unwrap();
        for (name, sql) in models {
            write_model(dir.path(), name, sql);
        }
        let source_schemas = sources();
        let source_provenance = origin
            .map(|origin| SourceProvenance::uniform(source_schemas.keys(), &origin))
            .unwrap_or_default();
        compile(&CompilerConfig {
            models_dir: dir.path().to_path_buf(),
            source_schemas,
            source_provenance,
            ..Default::default()
        })
        .unwrap()
    }

    fn codes(result: &rocky_compiler::compile::CompileResult, code: &str) -> usize {
        result
            .diagnostics
            .iter()
            .filter(|d| d.code.as_ref() == code)
            .count()
    }

    const D1: &str = "SELECT order_id, customer_id, order_total FROM raw.orders";

    #[test]
    fn d1_against_live_schema_fails_compile_with_e041() {
        let result = compile_with(&[("stg_orders", D1)], Some(SourceSchemaOrigin::Live));
        assert!(result.has_errors);
        let e041: Vec<_> = result
            .diagnostics
            .iter()
            .filter(|d| d.code.as_ref() == "E041")
            .collect();
        assert_eq!(e041.len(), 1, "{:?}", result.diagnostics);
        assert_eq!(e041[0].severity, Severity::Error);
        assert_eq!(e041[0].model, "stg_orders");
        assert!(e041[0].message.contains("order_total"));
        assert!(e041[0].message.contains("raw.orders"));
    }

    #[test]
    fn d1_against_seed_schema_warns_and_compiles() {
        let result = compile_with(&[("stg_orders", D1)], Some(SourceSchemaOrigin::Seed));
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert_eq!(codes(&result, "W041"), 1);
    }

    #[test]
    fn d1_without_provenance_is_unchanged() {
        let result = compile_with(&[("stg_orders", D1)], None);
        assert!(!result.has_errors, "{:?}", result.diagnostics);
        assert_eq!(codes(&result, "E041") + codes(&result, "W041"), 0);
    }

    #[test]
    fn valid_controls_stay_clean_against_live_schema() {
        let result = compile_with(
            &[
                (
                    "stg_orders",
                    "SELECT order_id, customer_id, amount FROM raw.orders",
                ),
                (
                    "fct_revenue",
                    "SELECT c.customer_name, SUM(o.amount) AS total FROM stg_orders o \
                     JOIN raw.customers c ON o.customer_id = c.customer_id \
                     GROUP BY c.customer_name",
                ),
                (
                    "v1",
                    "SELECT order_id AS id2, id2 + 1 AS next_id FROM raw.orders",
                ),
                ("v2", "SELECT 10::BIGINT = '10'::VARCHAR AS equal_value"),
                (
                    "v3",
                    "SELECT scoped.order_id FROM (SELECT order_id FROM raw.orders) AS scoped",
                ),
                (
                    "v4",
                    "SELECT sha256(customer_name) AS customer_hash FROM raw.customers",
                ),
                (
                    "g1_s2",
                    "WITH stg_orders AS (SELECT order_id, amount FROM raw.orders) \
                     SELECT stg_orders.amount FROM stg_orders",
                ),
                ("stg_star", "SELECT * FROM raw.orders"),
                ("g1_s3", "SELECT s.amount FROM stg_star AS s"),
            ],
            Some(SourceSchemaOrigin::Live),
        );
        assert_eq!(
            codes(&result, "E041") + codes(&result, "W041"),
            0,
            "{:?}",
            result.diagnostics
        );
    }
}

// ---- DSL `in [...]` / `not in [...]` (plan-15) ----

/// A `.rocky` model using `in` / `not in` parses, lowers, and type-checks to
/// a Boolean whose nullability follows SQL 3VL: non-null operand and items
/// give a non-nullable result; a NULL in the list makes it nullable.
#[test]
fn dsl_in_list_compiles_to_boolean_with_3vl_nullability() {
    use rocky_compiler::types::TypedColumn;
    use rocky_ir::RockyType;

    let dir = tempfile::tempdir().expect("tempdir");
    let models = dir.path().join("models");
    std::fs::create_dir(&models).expect("models dir");
    std::fs::write(
        models.join("flags.rocky"),
        "from warehouse.main.raw_orders\n\
         derive {\n    hot: status in [\"a\", \"b\"],\n    cold: status not in [\"x\", null]\n}\n",
    )
    .expect("rocky");
    std::fs::write(
        models.join("flags.toml"),
        "name = \"flags\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
         [target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"flags\"\n",
    )
    .expect("sidecar");

    let mut source_schemas = HashMap::new();
    source_schemas.insert(
        "warehouse.main.raw_orders".to_string(),
        vec![TypedColumn {
            name: "status".to_string(),
            data_type: RockyType::String,
            nullable: false,
        }],
    );
    let result = compile(&CompilerConfig {
        models_dir: models,
        contracts_dir: None,
        source_schemas,
        ..Default::default()
    })
    .unwrap();
    assert!(
        !result.has_errors,
        "in/not in must compile cleanly: {:?}",
        result.diagnostics
    );
    let sql = &result.project.model("flags").expect("model").sql;
    assert!(
        sql.contains("status IN ('a', 'b') AS hot")
            && sql.contains("status NOT IN ('x', NULL) AS cold"),
        "got: {sql}"
    );
    // Expression-level inference over the lowered SQL: the project pass
    // leaves expression columns `Unknown`, so assert through the same
    // inference entry point `rocky compile` exposes for model SQL.
    let mut scope = HashMap::new();
    scope.insert(
        "warehouse.main.raw_orders".to_string(),
        result.type_check.typed_models["warehouse.main.raw_orders"].clone(),
    );
    let cols = rocky_compiler::typecheck::infer_select_types(sql, &scope, "flags")
        .unwrap_or_else(|e| panic!("inference failed: {e}"));
    let col = |n: &str| cols.iter().find(|c| c.name == n).expect(n).clone();
    assert_eq!(col("hot").data_type, RockyType::Boolean);
    assert!(!col("hot").nullable, "non-null operand and items");
    assert_eq!(col("cold").data_type, RockyType::Boolean);
    assert!(
        col("cold").nullable,
        "a NULL in the list makes NOT IN nullable"
    );
}

/// Dependency and name-resolution checks across the whole compile: a cycle
/// through a `WHERE` sub-query, a missing source table (E045 / W045), and an
/// ambiguous bare column (E029).
mod dependency_and_name_checks {
    use std::collections::HashMap;
    use std::path::Path;

    use rocky_compiler::compile::{CompilerConfig, compile};
    use rocky_compiler::source_refs::{SourceProvenance, SourceSchemaOrigin};
    use rocky_compiler::types::{RockyType, TypedColumn};

    fn write_model(dir: &Path, name: &str, sql: &str, depends_on: &[&str]) {
        std::fs::write(dir.join(format!("{name}.sql")), sql).unwrap();
        let deps = depends_on
            .iter()
            .map(|d| format!("\"{d}\""))
            .collect::<Vec<_>>()
            .join(", ");
        std::fs::write(
            dir.join(format!("{name}.toml")),
            format!(
                "name = \"{name}\"\ndepends_on = [{deps}]\n\n[strategy]\ntype = \"full_refresh\"\n\n\
                 [target]\ncatalog = \"c\"\nschema = \"main\"\ntable = \"{name}\"\n"
            ),
        )
        .unwrap();
    }

    fn cols(names: &[&str]) -> Vec<TypedColumn> {
        names
            .iter()
            .map(|name| TypedColumn {
                name: (*name).to_string(),
                data_type: RockyType::Unknown,
                nullable: true,
            })
            .collect()
    }

    fn sources() -> HashMap<String, Vec<TypedColumn>> {
        HashMap::from([
            (
                "shop.orders".to_string(),
                cols(&["order_id", "customer_id", "amount", "status"]),
            ),
            (
                "shop.customers".to_string(),
                cols(&["customer_id", "name", "email"]),
            ),
        ])
    }

    fn compile_models(
        models: &[(&str, &str, &[&str])],
        origin: SourceSchemaOrigin,
        strict: bool,
    ) -> Result<rocky_compiler::compile::CompileResult, String> {
        let dir = tempfile::TempDir::new().unwrap();
        for (name, sql, deps) in models {
            write_model(dir.path(), name, sql, deps);
        }
        let source_schemas = sources();
        let source_provenance =
            SourceProvenance::uniform(source_schemas.keys(), &origin).with_strict(strict);
        compile(&CompilerConfig {
            models_dir: dir.path().to_path_buf(),
            source_schemas,
            source_provenance,
            ..Default::default()
        })
        .map_err(|e| e.to_string())
    }

    fn codes(result: &rocky_compiler::compile::CompileResult) -> Vec<&str> {
        let mut codes: Vec<&str> = result
            .diagnostics
            .iter()
            .filter(|d| d.severity != rocky_compiler::diagnostic::Severity::Info)
            .map(|d| d.code.as_ref())
            .collect();
        codes.sort_unstable();
        codes
    }

    const STG_ORDERS: &str = "SELECT order_id, customer_id, amount, status FROM shop.orders";
    const STG_CUSTOMERS: &str = "SELECT customer_id, name, email FROM shop.customers";
    const LTV: &str = "SELECT customer_id, SUM(amount) AS lifetime_value FROM fct_orders \
                       GROUP BY customer_id";
    const FCT: &str = "SELECT order_id, customer_id, amount FROM stg_orders \
                       WHERE status = 'completed'";

    #[test]
    fn a_cycle_through_a_where_subquery_is_refused_naming_both_models() {
        let fct_reads_ltv = "SELECT order_id, customer_id, amount FROM stg_orders \
                             WHERE status = 'completed' \
                             AND customer_id IN (SELECT customer_id FROM customer_ltv)";
        let err = compile_models(
            &[
                ("stg_orders", STG_ORDERS, &[]),
                ("fct_orders", fct_reads_ltv, &["stg_orders"]),
                ("customer_ltv", LTV, &["fct_orders"]),
                ("top_customers", "SELECT customer_id FROM customer_ltv", &[]),
            ],
            SourceSchemaOrigin::Seed,
            false,
        )
        .err()
        .expect("fct_orders and customer_ltv read each other");
        assert!(err.starts_with("circular dependency"), "{err}");
        assert!(
            err.contains("\"fct_orders\"") && err.contains("\"customer_ltv\""),
            "{err}"
        );
        assert!(
            !err.contains("top_customers"),
            "a model that only reads the cycle is not named: {err}"
        );

        let ok = compile_models(
            &[
                ("stg_orders", STG_ORDERS, &[]),
                ("fct_orders", FCT, &["stg_orders"]),
                ("customer_ltv", LTV, &["fct_orders"]),
            ],
            SourceSchemaOrigin::Seed,
            false,
        )
        .expect("without the sub-query read there is no cycle");
        assert!(!ok.has_errors, "{:?}", ok.diagnostics);
    }

    #[test]
    fn a_missing_source_table_warns_and_strict_refuses() {
        let models: &[(&str, &str, &[&str])] = &[(
            "stg_orders",
            "SELECT order_id, customer_id FROM shop.orderz",
            &[],
        )];
        let seed = compile_models(models, SourceSchemaOrigin::Seed, false).unwrap();
        assert_eq!(codes(&seed), ["W045"], "{:?}", seed.diagnostics);
        assert!(!seed.has_errors);

        let strict = compile_models(models, SourceSchemaOrigin::Seed, true).unwrap();
        assert_eq!(codes(&strict), ["E045"], "{:?}", strict.diagnostics);
        assert!(strict.has_errors);

        let live = compile_models(models, SourceSchemaOrigin::Live, false).unwrap();
        assert_eq!(codes(&live), ["E045"], "{:?}", live.diagnostics);
    }

    const DIM: &str = "SELECT c.customer_id, c.name, COALESCE(l.lifetime_value, 0) AS ltv \
                       FROM stg_customers AS c LEFT JOIN customer_ltv AS l \
                       ON c.customer_id = l.customer_id";

    fn project_with_dim(dim: &str) -> rocky_compiler::compile::CompileResult {
        compile_models(
            &[
                ("stg_orders", STG_ORDERS, &[]),
                ("stg_customers", STG_CUSTOMERS, &[]),
                ("fct_orders", FCT, &[]),
                ("customer_ltv", LTV, &[]),
                ("dim_customers", dim, &[]),
            ],
            SourceSchemaOrigin::Seed,
            true,
        )
        .unwrap()
    }

    #[test]
    fn an_ambiguous_bare_column_over_two_upstream_models_is_refused() {
        let dim = DIM.replacen("c.customer_id, c.name", "customer_id, c.name", 1);
        let result = project_with_dim(&dim);
        assert_eq!(codes(&result), ["E029"], "{:?}", result.diagnostics);
        let e029 = &result
            .diagnostics
            .iter()
            .find(|d| &*d.code == "E029")
            .unwrap();
        assert_eq!(e029.model, "dim_customers");
        assert!(e029.message.contains("customer_id"));
    }

    #[test]
    fn valid_joins_over_upstream_models_stay_clean() {
        for dim in [
            DIM.to_string(),
            // USING merges the key.
            "SELECT customer_id, c.name FROM stg_customers AS c \
             LEFT JOIN customer_ltv AS l USING (customer_id)"
                .to_string(),
            // A correlated sub-query reading a third model.
            DIM.replacen(
                "COALESCE(l.lifetime_value, 0) AS ltv",
                "COALESCE(l.lifetime_value, 0) AS ltv, (SELECT COUNT(*) FROM stg_orders AS so \
                 WHERE so.customer_id = c.customer_id) AS all_orders",
                1,
            ),
            // One side unknown: an external table Rocky has no schema for.
            "SELECT customer_id, c.name FROM stg_customers AS c \
             LEFT JOIN ext.unknown_table AS u ON c.customer_id = u.customer_id"
                .to_string(),
        ] {
            let result = project_with_dim(&dim);
            assert!(codes(&result).is_empty(), "{dim}: {:?}", result.diagnostics);
        }
    }
}

// ---- downstream consumers (`consumers/`, E060) ----

fn write_consumer(dir: &std::path::Path, file: &str, body: &str) {
    let consumers = dir.join("consumers");
    std::fs::create_dir_all(&consumers).unwrap();
    std::fs::write(consumers.join(file), body).unwrap();
}

fn e060_messages(result: &rocky_compiler::compile::CompileResult) -> Vec<String> {
    result
        .diagnostics
        .iter()
        .filter(|d| &*d.code == "E060")
        .map(|d| d.message.to_string())
        .collect()
}

/// A consumer over real models compiles clean and appears on the result; one
/// over a missing model is an E060 error that sets `has_errors`, and the full
/// and incremental paths agree.
#[test]
fn consumer_depends_on_must_name_a_model() {
    let dir = tempfile::tempdir().unwrap();
    write_strategy_project(dir.path(), "type = \"full_refresh\"");
    write_consumer(
        dir.path(),
        "board.toml",
        "kind = \"dashboard\"\nowner = \"finance\"\ndepends_on = [\"leaf\", \"src\"]\n",
    );
    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        ..Default::default()
    };
    let clean = compile(&config).unwrap();
    assert!(e060_messages(&clean).is_empty(), "{:?}", clean.diagnostics);
    assert!(!clean.has_errors, "{:?}", clean.diagnostics);
    assert_eq!(clean.consumers.len(), 1);
    assert_eq!(clean.consumers[0].name, "board");
    assert_eq!(clean.consumers[0].depends_on, vec!["leaf", "src"]);

    // Point it at a model that does not exist.
    write_consumer(
        dir.path(),
        "board.toml",
        "kind = \"dashboard\"\ndepends_on = [\"lef\", \"src\"]\n",
    );
    let broken = compile(&config).unwrap();
    let messages = e060_messages(&broken);
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert!(messages[0].contains("`lef`"), "{}", messages[0]);
    assert!(broken.has_errors);
    // The valid edge stays visible.
    assert_eq!(broken.consumers[0].depends_on, vec!["src"]);

    // The incremental path reports the same thing.
    let incremental = rocky_compiler::compile::compile_incremental(&config, &[], &broken).unwrap();
    assert_eq!(e060_messages(&incremental), messages);
    assert!(incremental.has_errors);
    assert_eq!(incremental.consumers, broken.consumers);
}

/// A compile that covers part of a project must judge `consumers/` against the
/// whole project: the directory is the project root's, and a `depends_on`
/// entry is valid when it names a model anywhere in the project. Only a name
/// that is a model nowhere is an E060.
#[test]
fn a_scoped_compile_judges_consumers_against_the_whole_project() {
    use rocky_compiler::compile::{ProjectContext, compile_preloaded_models};
    let dir = tempfile::tempdir().unwrap();
    write_strategy_project(dir.path(), "type = \"full_refresh\"");
    // The compile covers `src` only, and its models directory is a
    // subdirectory, so the sibling `../consumers` is not the project's.
    let scoped_dir = dir.path().join("models").join("marts");
    std::fs::create_dir_all(&scoped_dir).unwrap();
    let only_src = || {
        let all = rocky_compiler::project::Project::load_models(&dir.path().join("models"), None)
            .unwrap();
        all.into_iter()
            .filter(|m| m.config.name == "src")
            .collect::<Vec<_>>()
    };
    let project = ProjectContext {
        root: dir.path().to_path_buf(),
        model_names: ["src", "leaf"].into_iter().map(String::from).collect(),
    };
    let config = CompilerConfig {
        models_dir: scoped_dir,
        project: Some(project),
        ..Default::default()
    };

    // `leaf` is outside this compile and inside the project: clean.
    write_consumer(
        dir.path(),
        "board.toml",
        "depends_on = [\"leaf\", \"src\"]\n",
    );
    let clean = compile_preloaded_models(only_src(), &config).unwrap();
    assert!(e060_messages(&clean).is_empty(), "{:?}", clean.diagnostics);
    assert!(!clean.has_errors, "{:?}", clean.diagnostics);
    // Read from the project root, not from `models/marts/../consumers`.
    assert_eq!(clean.consumers.len(), 1);
    assert_eq!(clean.consumers[0].depends_on, vec!["leaf", "src"]);

    // A name that is a model nowhere is still refused.
    write_consumer(dir.path(), "board.toml", "depends_on = [\"nowhere\"]\n");
    let broken = compile_preloaded_models(only_src(), &config).unwrap();
    let messages = e060_messages(&broken);
    assert_eq!(messages.len(), 1, "{messages:?}");
    assert!(messages[0].contains("`nowhere`"), "{}", messages[0]);
}
