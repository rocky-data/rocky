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

criterion_group!(
    benches,
    bench_cold_compile,
    bench_dag_resolution,
    bench_sql_generation,
    bench_startup,
);
criterion_main!(benches);
