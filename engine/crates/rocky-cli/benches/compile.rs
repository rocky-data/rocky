//! Criterion benchmarks for Rocky's core compile operations.
//!
//! Measures cold compile, DAG resolution, type checking at various scales.
//! Run with: `cargo bench --bench compile`

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use tempfile::TempDir;

use rocky_compiler::compile::{CompilerConfig, compile};
use rocky_core::dag::{DagNode, execution_layers, topological_sort};
use rocky_core::ir::*;
use rocky_core::sql_gen;
use rocky_duckdb::dialect::DuckDbSqlDialect;

// ---------------------------------------------------------------------------
// Synthetic project generation
// ---------------------------------------------------------------------------

/// Generate a synthetic Rocky project with the given number of models.
///
/// DAG shape:
/// - 30% source models (no dependencies)
/// - 30% staging models (depends on 1 source)
/// - 25% intermediate models (depends on 2-3 upstream, diamond deps)
/// - 15% mart models (depends on 1-2 intermediate)
fn generate_synthetic_project(size: usize, dir: &Path) {
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).unwrap();

    let n_sources = (size as f64 * 0.30).ceil() as usize;
    let n_staging = (size as f64 * 0.30).ceil() as usize;
    let n_intermediate = (size as f64 * 0.25).ceil() as usize;
    let n_marts = size.saturating_sub(n_sources + n_staging + n_intermediate);

    let mut model_idx = 0;

    // Source models (no deps)
    let mut source_names = Vec::new();
    for _ in 0..n_sources {
        let name = format!("src_{model_idx:04}");
        let ncols = 10 + (model_idx % 20);
        let cols: Vec<String> = (0..ncols).map(|c| format!("col_{c}")).collect();
        let select = cols.join(", ");
        let sql = format!("SELECT {select} FROM raw_data.source_{model_idx}");
        let toml = format!(
            r#"name = "{name}"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "sources"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        source_names.push(name);
        model_idx += 1;
    }

    // Staging models (depends on 1 source each)
    let mut staging_names = Vec::new();
    for i in 0..n_staging {
        let name = format!("stg_{model_idx:04}");
        let dep = &source_names[i % source_names.len()];
        let sql = format!(
            "SELECT\n    col_0,\n    col_1,\n    UPPER(col_2) AS col_2_upper,\n    \
             COALESCE(col_3, 'unknown') AS col_3_clean\nFROM {dep}"
        );
        let toml = format!(
            r#"name = "{name}"
depends_on = ["{dep}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "staging"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        staging_names.push(name);
        model_idx += 1;
    }

    // Intermediate models (2-3 deps from staging, diamond pattern)
    let mut intermediate_names = Vec::new();
    for i in 0..n_intermediate {
        let name = format!("int_{model_idx:04}");
        let dep1 = &staging_names[i % staging_names.len()];
        let dep2 = &staging_names[(i + 1) % staging_names.len()];
        let sql = format!(
            "SELECT\n    a.col_0,\n    a.col_1,\n    b.col_2_upper,\n    \
             COUNT(*) AS row_count,\n    SUM(a.col_1) AS total\n\
             FROM {dep1} a\nJOIN {dep2} b ON a.col_0 = b.col_0\n\
             GROUP BY a.col_0, a.col_1, b.col_2_upper"
        );
        let toml = format!(
            r#"name = "{name}"
depends_on = ["{dep1}", "{dep2}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "intermediate"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        intermediate_names.push(name);
        model_idx += 1;
    }

    // Mart models (1-2 deps from intermediate)
    for i in 0..n_marts {
        let name = format!("fct_{model_idx:04}");
        let dep = &intermediate_names[i % intermediate_names.len().max(1)];
        let sql = format!(
            "SELECT\n    col_0,\n    total,\n    row_count,\n    \
             CASE WHEN total > 1000 THEN 'high' ELSE 'low' END AS tier,\n    \
             ROW_NUMBER() OVER (ORDER BY total DESC) AS rank\n\
             FROM {dep}"
        );
        let toml = format!(
            r#"name = "{name}"
depends_on = ["{dep}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "marts"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        model_idx += 1;
    }
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

fn bench_cold_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("cold_compile");

    // Sizes through 1k always run; the 10k case is gated on release builds
    // because debug builds take ~30 s per iteration with the C++/DuckDB heap
    // and would dominate every PR's CI bench job.
    let mut sizes: Vec<usize> = vec![10, 100, 1_000];
    if !cfg!(debug_assertions) {
        sizes.push(10_000);
    }

    // Larger sizes need fewer iterations or criterion takes forever.
    group.sample_size(10);

    for size in sizes {
        let dir = TempDir::new().unwrap();
        generate_synthetic_project(size, dir.path());

        let config = CompilerConfig {
            models_dir: dir.path().join("models"),
            contracts_dir: None,
            source_schemas: HashMap::new(),
            source_column_info: HashMap::new(),
        };

        group.bench_with_input(BenchmarkId::new("models", size), &config, |b, config| {
            b.iter(|| {
                compile(config).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_dag_resolution(c: &mut Criterion) {
    let mut group = c.benchmark_group("dag_resolution");

    for size in [10, 100, 500, 1000] {
        // Build a diamond-shaped DAG
        let mut nodes = Vec::with_capacity(size);
        let n_sources = (size as f64 * 0.3) as usize;

        for i in 0..size {
            let deps = if i < n_sources {
                vec![]
            } else {
                // Depend on 1-2 earlier nodes
                let dep1 = format!("node_{}", i % n_sources);
                let dep2 = format!("node_{}", (i + 1) % n_sources);
                if i % 3 == 0 {
                    vec![dep1, dep2]
                } else {
                    vec![dep1]
                }
            };
            nodes.push(DagNode {
                name: format!("node_{i}"),
                depends_on: deps,
            });
        }

        group.bench_with_input(BenchmarkId::new("topo_sort", size), &nodes, |b, nodes| {
            b.iter(|| {
                topological_sort(nodes).unwrap();
            });
        });

        group.bench_with_input(BenchmarkId::new("exec_layers", size), &nodes, |b, nodes| {
            b.iter(|| {
                execution_layers(nodes).unwrap();
            });
        });
    }
    group.finish();
}

fn bench_sql_generation(c: &mut Criterion) {
    let mut group = c.benchmark_group("sql_generation");
    let dialect = DuckDbSqlDialect;

    for size in [10, 100, 500] {
        // Pre-build replication plans
        let plans: Vec<ReplicationPlan> = (0..size)
            .map(|i| ReplicationPlan {
                source: SourceRef {
                    catalog: String::new(),
                    schema: "source".into(),
                    table: format!("table_{i}"),
                },
                target: TargetRef {
                    catalog: String::new(),
                    schema: "target".into(),
                    table: format!("table_{i}"),
                },
                strategy: if i % 3 == 0 {
                    MaterializationStrategy::FullRefresh
                } else {
                    MaterializationStrategy::Incremental {
                        timestamp_column: "_fivetran_synced".into(),
                        watermark: None,
                    }
                },
                columns: ColumnSelection::All,
                metadata_columns: vec![MetadataColumn {
                    name: "_loaded_by".into(),
                    data_type: "VARCHAR".into(),
                    value: "NULL".into(),
                }],
                governance: GovernanceConfig {
                    permissions_file: None,
                    auto_create_catalogs: false,
                    auto_create_schemas: false,
                },
            })
            .collect();

        group.bench_with_input(BenchmarkId::new("ctas", size), &plans, |b, plans| {
            b.iter(|| {
                for plan in plans {
                    sql_gen::generate_create_table_as_sql(plan, &dialect).unwrap();
                }
            });
        });

        // Only bench incremental plans
        let inc_plans: Vec<&ReplicationPlan> = plans
            .iter()
            .filter(|p| matches!(p.strategy, MaterializationStrategy::Incremental { .. }))
            .collect();

        group.bench_with_input(
            BenchmarkId::new("incremental", inc_plans.len()),
            &inc_plans,
            |b, plans| {
                b.iter(|| {
                    for plan in plans.iter() {
                        sql_gen::generate_insert_sql(plan, &dialect).unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

/// Generate a synthetic project with a fixed 4-layer DAG for the sub-second
/// compile benchmark.
///
/// Layer 0: 50 source models (no dependencies)
/// Layer 1: 150 staging models (each depends on 1 source)
/// Layer 2: 200 intermediate models (each depends on 2 staging models)
/// Layer 3: 100 mart models (each depends on 1-2 intermediate models)
/// Total: 500 models
fn generate_layered_project(dir: &Path) {
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).unwrap();

    let mut model_idx = 0;

    // Layer 0: 50 source models
    let mut source_names = Vec::with_capacity(50);
    for _ in 0..50 {
        let name = format!("src_{model_idx:04}");
        let ncols = 8 + (model_idx % 12);
        let cols: Vec<String> = (0..ncols).map(|c| format!("col_{c}")).collect();
        let select = cols.join(", ");
        let sql = format!("SELECT {select} FROM raw_data.source_{model_idx}");
        let toml = format!(
            r#"name = "{name}"

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "sources"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        source_names.push(name);
        model_idx += 1;
    }

    // Layer 1: 150 staging models (each depends on 1 source)
    let mut staging_names = Vec::with_capacity(150);
    for i in 0..150 {
        let name = format!("stg_{model_idx:04}");
        let dep = &source_names[i % source_names.len()];
        let sql = format!(
            "SELECT\n    col_0,\n    col_1,\n    UPPER(col_2) AS col_2_upper,\n    \
             COALESCE(col_3, 'unknown') AS col_3_clean\nFROM {dep}"
        );
        let toml = format!(
            r#"name = "{name}"
depends_on = ["{dep}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "staging"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        staging_names.push(name);
        model_idx += 1;
    }

    // Layer 2: 200 intermediate models (each depends on 2 staging models)
    let mut intermediate_names = Vec::with_capacity(200);
    for i in 0..200 {
        let name = format!("int_{model_idx:04}");
        let dep1 = &staging_names[i % staging_names.len()];
        let dep2 = &staging_names[(i + 7) % staging_names.len()];
        let sql = format!(
            "SELECT\n    a.col_0,\n    a.col_1,\n    b.col_2_upper,\n    \
             COUNT(*) AS row_count,\n    SUM(a.col_1) AS total\n\
             FROM {dep1} a\nJOIN {dep2} b ON a.col_0 = b.col_0\n\
             GROUP BY a.col_0, a.col_1, b.col_2_upper"
        );
        let toml = format!(
            r#"name = "{name}"
depends_on = ["{dep1}", "{dep2}"]

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "intermediate"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        intermediate_names.push(name);
        model_idx += 1;
    }

    // Layer 3: 100 mart models (each depends on 1-2 intermediate models)
    for i in 0..100 {
        let name = format!("fct_{model_idx:04}");
        let dep1 = &intermediate_names[i % intermediate_names.len()];
        let has_second_dep = i % 3 == 0;
        let deps_toml;
        let sql;
        if has_second_dep {
            let dep2 = &intermediate_names[(i + 13) % intermediate_names.len()];
            deps_toml = format!(r#"depends_on = ["{dep1}", "{dep2}"]"#);
            sql = format!(
                "SELECT\n    a.col_0,\n    a.total,\n    a.row_count,\n    \
                 b.col_2_upper AS secondary_dim,\n    \
                 CASE WHEN a.total > 1000 THEN 'high' ELSE 'low' END AS tier,\n    \
                 ROW_NUMBER() OVER (ORDER BY a.total DESC) AS rank\n\
                 FROM {dep1} a\nJOIN {dep2} b ON a.col_0 = b.col_0"
            );
        } else {
            deps_toml = format!(r#"depends_on = ["{dep1}"]"#);
            sql = format!(
                "SELECT\n    col_0,\n    total,\n    row_count,\n    \
                 CASE WHEN total > 1000 THEN 'high' ELSE 'low' END AS tier,\n    \
                 ROW_NUMBER() OVER (ORDER BY total DESC) AS rank\n\
                 FROM {dep1}"
            );
        }
        let toml = format!(
            r#"name = "{name}"
{deps_toml}

[strategy]
type = "full_refresh"

[target]
catalog = "warehouse"
schema = "marts"
table = "{name}"
"#
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        model_idx += 1;
    }
}

/// Benchmark 500-model compile targeting sub-1s.
///
/// Uses `generate_layered_project` to produce a realistic 4-layer DAG
/// (50 sources → 150 staging → 200 intermediate → 100 marts).
fn bench_sub_second_compile(c: &mut Criterion) {
    let mut group = c.benchmark_group("sub_second_compile");
    group.sample_size(10);

    let dir = TempDir::new().unwrap();
    generate_layered_project(dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    group.bench_function("500_models_4_layers", |b| {
        b.iter(|| {
            compile(&config).unwrap();
        });
    });

    group.finish();
}

fn bench_startup(c: &mut Criterion) {
    // Measure subprocess startup time (binary must be built first)
    c.bench_function("binary_startup", |b| {
        b.iter(|| {
            let output = std::process::Command::new("cargo")
                .args(["run", "-q", "-p", "rocky", "--", "--version"])
                .output();
            // Don't assert — if binary isn't built, just measure what we can
            let _ = output;
        });
    });
}

/// §P4.4 — single-file-change incremental recompile bench. Locks in the
/// gains expected from §P3.1 (finish incremental compiler) and §P3.2
/// (didChange buffer-hash short-circuit). Runs a full compile once to
/// seed `previous`, edits one mart model's SQL in place, then measures
/// `compile_incremental` against that single changed file.
///
/// Expected envelope on a 500-model layered project:
/// - full cold compile: hundreds of ms (see `sub_second_compile`)
/// - `single_file_change`: single-digit ms (only the edited model +
///   dependents re-typecheck)
///
/// If that ratio collapses, something in the incremental path regressed —
/// criterion's default 120 % alert threshold fires on the perf-label CI.
fn bench_single_file_change(c: &mut Criterion) {
    use rocky_compiler::compile::compile;
    use rocky_server::lsp::compile_incremental;

    let mut group = c.benchmark_group("single_file_change");
    group.sample_size(30);

    let dir = TempDir::new().unwrap();
    generate_layered_project(dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    // Seed the incremental cache with a full cold compile.
    let previous = compile(&config).expect("seed compile must succeed");

    // Pick a mart model (leaf of the DAG) — `fct_0500` is the first one
    // `generate_layered_project` produces after 50+150+200 = 400 upstream
    // models. Editing a leaf minimises downstream reflow so we're really
    // measuring the incremental path's per-change overhead.
    let models_dir = dir.path().join("models");
    let target_sql = models_dir.join("fct_0500.sql");
    let baseline =
        fs::read_to_string(&target_sql).expect("layered project must contain fct_0500.sql");

    group.bench_function("500_models_edit_one_mart", |b| {
        let mut counter: u32 = 0;
        b.iter(|| {
            // Mutate the file each iteration so fs metadata genuinely changes
            // (counter avoids a CPU cache / OS-level no-op on identical
            // writes).
            counter = counter.wrapping_add(1);
            let mutated = format!("{baseline}\n-- bench-iter-{counter}\n");
            fs::write(&target_sql, mutated).unwrap();
            let changed = vec![target_sql.clone()];
            compile_incremental(&changed, &previous, &config).unwrap();
        });
    });

    // Restore the original file so subsequent benches see the seeded project.
    fs::write(&target_sql, baseline).unwrap();
    group.finish();
}

criterion_group!(
    benches,
    bench_cold_compile,
    bench_dag_resolution,
    bench_sql_generation,
    bench_sub_second_compile,
    bench_single_file_change,
    bench_startup,
);
criterion_main!(benches);
