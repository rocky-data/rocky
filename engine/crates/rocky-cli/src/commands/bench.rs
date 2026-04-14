//! `rocky bench` — run performance benchmarks and report results.
//!
//! Runs internal benchmarks for compile, DAG resolution, SQL generation,
//! and binary startup. Outputs results in human-readable or JSON format.
//! Supports saving baselines and comparing against previous runs.

use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use rocky_compiler::compile::{CompilerConfig, compile};
use rocky_core::dag::{DagNode, execution_layers, topological_sort};
use rocky_core::ir::*;
use rocky_core::sql_gen;
use rocky_duckdb::dialect::DuckDbSqlDialect;

// ---------------------------------------------------------------------------
// Output types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchReport {
    pub version: String,
    pub timestamp: String,
    pub system: SystemInfo,
    pub results: Vec<BenchResult>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    pub os: String,
    pub arch: String,
    pub cpus: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchResult {
    pub group: String,
    pub name: String,
    pub iterations: usize,
    pub mean_us: f64,
    pub median_us: f64,
    pub min_us: f64,
    pub max_us: f64,
    pub throughput: Option<String>,
}

// ---------------------------------------------------------------------------
// Synthetic project generation (shared with criterion benches)
// ---------------------------------------------------------------------------

pub fn generate_synthetic_project(size: usize, dir: &Path) {
    let models_dir = dir.join("models");
    fs::create_dir_all(&models_dir).unwrap();

    let n_sources = (size as f64 * 0.30).ceil() as usize;
    let n_staging = (size as f64 * 0.30).ceil() as usize;
    let n_intermediate = (size as f64 * 0.25).ceil() as usize;
    let n_marts = size.saturating_sub(n_sources + n_staging + n_intermediate);

    let mut model_idx = 0;
    let mut source_names = Vec::new();
    let mut staging_names = Vec::new();
    let mut intermediate_names = Vec::new();

    // --- Sources: SELECT with CAST/ROUND expressions, 7-15 columns ---
    for _ in 0..n_sources {
        let name = format!("src_{model_idx:05}");
        let ncols = 7 + (model_idx % 9); // 7-15 columns
        let mut cols = Vec::new();
        for c in 0..ncols {
            let col = match c % 7 {
                0 => format!("col_{c} AS id"),
                1 => format!("col_{c} AS name"),
                2 => format!("CAST(col_{c} AS VARCHAR) AS category"),
                3 => format!("ROUND(CAST(col_{c} AS DECIMAL(10,2)), 2) AS amount"),
                4 => format!("col_{c} AS status"),
                5 => format!("col_{c} AS created_at"),
                6 => format!("col_{c} AS updated_at"),
                _ => format!("col_{c}"),
            };
            cols.push(col);
        }
        let sql = format!(
            "SELECT {} FROM raw_data.source_{model_idx}",
            cols.join(", ")
        );
        let toml = format!(
            "name = \"{name}\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"sources\"\ntable = \"{name}\"\n"
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        source_names.push(name);
        model_idx += 1;
    }

    // --- Staging: clean + filter + window functions ---
    // ~20% use Rocky DSL, rest use SQL
    for i in 0..n_staging {
        let name = format!("stg_{model_idx:05}");
        let dep = &source_names[i % source_names.len()];
        let use_rocky = i % 5 == 0; // 20% Rocky DSL

        // Vary strategy: 80% full_refresh, 15% incremental, 5% merge
        let strategy_toml = match model_idx % 20 {
            0..=15 => "type = \"full_refresh\"".to_string(),
            16..=18 => "type = \"incremental\"\ntimestamp_column = \"updated_at\"".to_string(),
            _ => "type = \"merge\"\nunique_key = [\"id\"]".to_string(),
        };

        let toml = format!(
            "name = \"{name}\"\ndepends_on = [\"{dep}\"]\n\n[strategy]\n{strategy_toml}\n\n[target]\ncatalog = \"warehouse\"\nschema = \"staging\"\ntable = \"{name}\"\n"
        );

        if use_rocky {
            // Rocky DSL: from + where + derive with window function
            let rocky = format!(
                "-- Staging: clean {dep}\nfrom {dep}\nwhere status != \"deleted\"\nwhere amount > 0\nderive {{\n    name_clean: upper(name),\n    category_clean: coalesce(category, \"unknown\"),\n    rn: row_number() over (partition id, sort -updated_at)\n}}\n"
            );
            fs::write(models_dir.join(format!("{name}.rocky")), &rocky).unwrap();
        } else {
            let sql = format!(
                "SELECT\n    id,\n    UPPER(TRIM(name)) AS name_clean,\n    COALESCE(category, 'unknown') AS category_clean,\n    amount,\n    LOWER(status) AS status,\n    created_at,\n    updated_at,\n    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\nFROM {dep}\nWHERE status != 'deleted'\n  AND amount > 0"
            );
            fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        }
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        staging_names.push(name);
        model_idx += 1;
    }

    // --- Intermediate: CTEs, JOINs, GROUP BY with 3-6 aggregates ---
    // ~20% use Rocky DSL (group + where for HAVING)
    for i in 0..n_intermediate {
        let name = format!("int_{model_idx:05}");
        let dep1 = &staging_names[i % staging_names.len()];
        let dep2 = &staging_names[(i + 1) % staging_names.len()];
        // Every 10th model gets a 3rd dependency for more fan-in
        let dep3 = if i % 10 == 0 && staging_names.len() > 2 {
            Some(staging_names[(i + 2) % staging_names.len()].clone())
        } else {
            None
        };
        let use_rocky = i % 5 == 1; // 20% Rocky DSL (offset from staging)

        let strategy_toml = match model_idx % 20 {
            0..=15 => "type = \"full_refresh\"".to_string(),
            16..=18 => "type = \"incremental\"\ntimestamp_column = \"updated_at\"".to_string(),
            _ => "type = \"merge\"\nunique_key = [\"id\"]".to_string(),
        };

        let deps_toml = if let Some(ref d3) = dep3 {
            format!("depends_on = [\"{dep1}\", \"{dep2}\", \"{d3}\"]")
        } else {
            format!("depends_on = [\"{dep1}\", \"{dep2}\"]")
        };

        let toml = format!(
            "name = \"{name}\"\n{deps_toml}\n\n[strategy]\n{strategy_toml}\n\n[target]\ncatalog = \"warehouse\"\nschema = \"intermediate\"\ntable = \"{name}\"\n"
        );

        if use_rocky {
            // Rocky DSL: from + where + group with aggregations + HAVING
            let rocky = format!(
                "-- Intermediate: aggregate {dep1}\nfrom {dep1}\nwhere rn == 1\ngroup id {{\n    record_count: count(),\n    total_amount: sum(amount),\n    avg_amount: avg(amount),\n    first_seen: min(created_at),\n    last_seen: max(created_at)\n}}\nwhere record_count >= 2\n"
            );
            fs::write(models_dir.join(format!("{name}.rocky")), &rocky).unwrap();
        } else {
            let extra_join = dep3
                .as_ref()
                .map(|d3| format!("\n    LEFT JOIN {d3} c ON a.id = c.id"))
                .unwrap_or_default();
            let extra_col = if dep3.is_some() {
                ",\n        COALESCE(c.category_clean, b.category_clean) AS category"
            } else {
                ",\n        b.category_clean AS category"
            };
            let sql = format!(
                "WITH base AS (\n    SELECT\n        a.id,\n        a.name_clean AS name,\n        a.amount,\n        a.created_at{extra_col}\n    FROM {dep1} a\n    JOIN {dep2} b ON a.id = b.id{extra_join}\n    WHERE a.rn = 1\n)\nSELECT\n    id,\n    name,\n    category,\n    COUNT(*) AS record_count,\n    SUM(amount) AS total_amount,\n    AVG(amount) AS avg_amount,\n    MIN(created_at) AS first_seen,\n    MAX(created_at) AS last_seen\nFROM base\nGROUP BY id, name, category"
            );
            fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        }
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        intermediate_names.push(name);
        model_idx += 1;
    }

    // --- Marts: CASE WHEN, window functions with frames, LEFT JOIN ---
    for i in 0..n_marts {
        let name = format!("fct_{model_idx:05}");
        let int_dep = &intermediate_names[i % intermediate_names.len().max(1)];
        let stg_dep = &staging_names[i % staging_names.len()];
        let toml = format!(
            "name = \"{name}\"\ndepends_on = [\"{int_dep}\", \"{stg_dep}\"]\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"warehouse\"\nschema = \"marts\"\ntable = \"{name}\"\n"
        );
        let sql = format!(
            "SELECT\n    i.id,\n    i.name,\n    i.total_amount,\n    i.record_count,\n    i.avg_amount,\n    CASE\n        WHEN i.total_amount > 10000 THEN 'high'\n        WHEN i.total_amount > 1000 THEN 'medium'\n        ELSE 'low'\n    END AS tier,\n    SUM(i.total_amount) OVER (ORDER BY i.first_seen ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total,\n    RANK() OVER (ORDER BY i.total_amount DESC) AS amount_rank\nFROM {int_dep} i\nLEFT JOIN {stg_dep} s ON i.id = s.id\nWHERE i.record_count >= 2"
        );
        fs::write(models_dir.join(format!("{name}.sql")), &sql).unwrap();
        fs::write(models_dir.join(format!("{name}.toml")), &toml).unwrap();
        model_idx += 1;
    }
}

// ---------------------------------------------------------------------------
// Benchmark runners
// ---------------------------------------------------------------------------

fn measure(iterations: usize, mut f: impl FnMut()) -> Vec<Duration> {
    // Warmup
    for _ in 0..3 {
        f();
    }
    let mut times = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        times.push(start.elapsed());
    }
    times
}

fn durations_to_result(
    group: &str,
    name: &str,
    times: &[Duration],
    throughput: Option<String>,
) -> BenchResult {
    let us: Vec<f64> = times
        .iter()
        .map(|t| t.as_secs_f64() * 1_000_000.0)
        .collect();
    let mut sorted = us.clone();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let mean = us.iter().sum::<f64>() / us.len() as f64;
    let median = sorted[sorted.len() / 2];
    BenchResult {
        group: group.into(),
        name: name.into(),
        iterations: times.len(),
        mean_us: (mean * 100.0).round() / 100.0,
        median_us: (median * 100.0).round() / 100.0,
        min_us: (sorted[0] * 100.0).round() / 100.0,
        max_us: (sorted.last().copied().unwrap_or(0.0) * 100.0).round() / 100.0,
        throughput,
    }
}

fn bench_compile(model_count: usize, iterations: usize) -> Vec<BenchResult> {
    let dir = tempfile::TempDir::new().unwrap();
    generate_synthetic_project(model_count, dir.path());

    let config = CompilerConfig {
        models_dir: dir.path().join("models"),
        contracts_dir: None,
        source_schemas: HashMap::new(),
        source_column_info: HashMap::new(),
    };

    let times = measure(iterations, || {
        compile(&config).unwrap();
    });

    let mean_ms = times.iter().map(|t| t.as_secs_f64() * 1000.0).sum::<f64>() / times.len() as f64;
    let throughput = format!("{:.0} models/sec", model_count as f64 / (mean_ms / 1000.0));

    vec![durations_to_result(
        "cold_compile",
        &format!("{model_count}_models"),
        &times,
        Some(throughput),
    )]
}

fn bench_dag(iterations: usize) -> Vec<BenchResult> {
    let mut results = Vec::new();

    for size in [10, 100, 1000, 5000, 10000, 20000] {
        let n_sources = (size as f64 * 0.3) as usize;
        let nodes: Vec<DagNode> = (0..size)
            .map(|i| DagNode {
                name: format!("node_{i}"),
                depends_on: if i < n_sources {
                    vec![]
                } else if i % 10 == 0 {
                    // Every 10th node: 3 deps (higher fan-in)
                    vec![
                        format!("node_{}", i % n_sources),
                        format!("node_{}", (i + 1) % n_sources),
                        format!("node_{}", (i + 2) % n_sources),
                    ]
                } else if i % 3 == 0 {
                    vec![
                        format!("node_{}", i % n_sources),
                        format!("node_{}", (i + 1) % n_sources),
                    ]
                } else {
                    vec![format!("node_{}", i % n_sources)]
                },
            })
            .collect();

        // Reduce iterations for large sizes to keep runtime reasonable
        let iter = if size >= 10000 {
            iterations.min(10)
        } else {
            iterations
        };

        let times = measure(iter, || {
            topological_sort(&nodes).unwrap();
        });
        results.push(durations_to_result(
            "dag",
            &format!("topo_sort_{size}"),
            &times,
            None,
        ));

        let times = measure(iter, || {
            execution_layers(&nodes).unwrap();
        });
        results.push(durations_to_result(
            "dag",
            &format!("exec_layers_{size}"),
            &times,
            None,
        ));
    }

    results
}

fn bench_sql_gen(iterations: usize) -> Vec<BenchResult> {
    let dialect = DuckDbSqlDialect;
    let mut results = Vec::new();

    for size in [10, 100, 500, 1000, 5000, 10000] {
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
                strategy: MaterializationStrategy::FullRefresh,
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

        let times = measure(iterations, || {
            for plan in &plans {
                sql_gen::generate_create_table_as_sql(plan, &dialect).unwrap();
            }
        });
        let mean_ms =
            times.iter().map(|t| t.as_secs_f64() * 1000.0).sum::<f64>() / times.len() as f64;
        let throughput = format!("{:.0} tables/sec", size as f64 / (mean_ms / 1000.0));
        results.push(durations_to_result(
            "sql_gen",
            &format!("ctas_{size}"),
            &times,
            Some(throughput),
        ));
    }

    results
}

fn bench_startup(iterations: usize) -> Vec<BenchResult> {
    let times = measure(iterations.min(5), || {
        let _ = std::process::Command::new("cargo")
            .args(["run", "-q", "-p", "rocky", "--", "--version"])
            .output();
    });
    vec![durations_to_result(
        "startup",
        "binary_version",
        &times,
        None,
    )]
}

// ---------------------------------------------------------------------------
// Main entry point
// ---------------------------------------------------------------------------

pub fn run_bench(
    group: &str,
    model_count: Option<usize>,
    format: &str,
    save_path: Option<&str>,
    compare_path: Option<&str>,
) -> Result<()> {
    let iterations = 20;
    let model_count = model_count.unwrap_or(100);

    let mut results = Vec::new();

    let groups: Vec<&str> = if group == "all" {
        vec!["compile", "dag", "sql_gen"]
    } else {
        vec![group]
    };

    for g in &groups {
        match *g {
            "compile" => results.extend(bench_compile(model_count, iterations)),
            "dag" => results.extend(bench_dag(iterations)),
            "sql_gen" => results.extend(bench_sql_gen(iterations)),
            "startup" => results.extend(bench_startup(iterations)),
            other => anyhow::bail!("unknown benchmark group: {other}"),
        }
    }

    let report = BenchReport {
        version: env!("CARGO_PKG_VERSION").into(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        system: SystemInfo {
            os: std::env::consts::OS.into(),
            arch: std::env::consts::ARCH.into(),
            cpus: std::thread::available_parallelism()
                .map(std::num::NonZero::get)
                .unwrap_or(1),
        },
        results: results.clone(),
    };

    // Save baseline if requested
    if let Some(path) = save_path {
        let json = serde_json::to_string_pretty(&report)?;
        fs::write(path, &json).with_context(|| format!("failed to write baseline to {path}"))?;
        eprintln!("Saved baseline to {path}");
    }

    // Load and compare against baseline if requested
    let baseline: Option<BenchReport> = compare_path
        .map(|path| {
            let content = fs::read_to_string(path)
                .with_context(|| format!("failed to read baseline from {path}"))?;
            serde_json::from_str(&content)
                .with_context(|| format!("failed to parse baseline from {path}"))
        })
        .transpose()?;

    let baseline_map: HashMap<String, f64> = baseline
        .as_ref()
        .map(|b| {
            b.results
                .iter()
                .map(|r| (format!("{}/{}", r.group, r.name), r.mean_us))
                .collect()
        })
        .unwrap_or_default();

    // Output
    if format == "json" {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!();
        println!("  Rocky Performance Benchmarks");
        println!("  ════════════════════════════════════════════════════════════════════");
        println!(
            "  System: {} {} ({} CPUs)",
            report.system.os, report.system.arch, report.system.cpus
        );
        println!("  Iterations: {iterations}");
        println!();

        let mut current_group = String::new();
        for r in &results {
            if r.group != current_group {
                current_group = r.group.clone();
                println!("  ── {} ──", current_group);
            }

            let time_str = format_time(r.mean_us);
            let throughput_str = r
                .throughput
                .as_deref()
                .map(|t| format!("  ({t})"))
                .unwrap_or_default();

            let change_str = baseline_map
                .get(&format!("{}/{}", r.group, r.name))
                .map(|baseline_us| {
                    let pct = ((r.mean_us - baseline_us) / baseline_us) * 100.0;
                    if pct.abs() < 1.0 {
                        "  (unchanged)".to_string()
                    } else if pct > 0.0 {
                        format!("  (+{pct:.1}% slower)")
                    } else {
                        format!("  ({pct:.1}% faster)")
                    }
                })
                .unwrap_or_default();

            println!(
                "    {:<30} {:>12}{throughput_str}{change_str}",
                r.name, time_str
            );
        }
        println!();
    }

    Ok(())
}

fn format_time(us: f64) -> String {
    if us < 1.0 {
        format!("{:.0}ns", us * 1000.0)
    } else if us < 1000.0 {
        format!("{:.1}µs", us)
    } else if us < 1_000_000.0 {
        format!("{:.2}ms", us / 1000.0)
    } else {
        format!("{:.2}s", us / 1_000_000.0)
    }
}
