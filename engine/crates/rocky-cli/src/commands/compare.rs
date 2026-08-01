//! `rocky compare` — compare shadow tables against production targets.
//!
//! Connects to the warehouse, finds table pairs (shadow + production),
//! compares row counts and schemas, and outputs per-table verdicts.

use std::path::Path;

use anyhow::{Context, Result};
use tracing::info;

use rocky_core::compare::{self, ComparisonResult, ComparisonThresholds, ComparisonVerdict};
use rocky_core::shadow::ShadowConfig;
use rocky_core::traits::WarehouseAdapter;
use rocky_ir::TargetRef;

use crate::output::{CompareOutput, TableCompareResult, print_json};
use crate::registry::{self, AdapterRegistry};

use super::{filter_table_matches, matches_filter, parse_filter};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Production targets for a replication pipeline, from source discovery.
///
/// Unchanged behaviour, lifted out so the two enumerations sit side by side
/// and the shared comparison below reads as one loop.
async fn replication_prod_targets(
    registry: &AdapterRegistry,
    pipeline: &rocky_core::config::ReplicationPipelineConfig,
    filter: Option<&str>,
) -> Result<Vec<TargetRef>> {
    let pattern = pipeline.schema_pattern()?;
    let parsed_filter = filter.map(parse_filter).transpose()?;

    let Some(ref disc) = pipeline.source.discovery else {
        anyhow::bail!("no discovery adapter configured — compare requires source discovery");
    };
    let discovery_adapter = registry.discovery_adapter(&disc.adapter)?;
    let connectors = discovery_adapter
        .discover(&pattern.prefix)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?
        .connectors;

    let mut targets = Vec::new();
    for conn in &connectors {
        let Ok(parsed) = pattern.parse(&conn.schema) else {
            continue;
        };
        if let Some((ref filter_key, ref filter_value)) = parsed_filter
            && !matches_filter(conn, &parsed, filter_key, filter_value)
        {
            continue;
        }
        let target_sep = pipeline
            .target
            .separator
            .as_deref()
            .unwrap_or(&pattern.separator);
        let target_catalog = parsed.resolve_template(&pipeline.target.catalog_template, target_sep);
        let target_schema = parsed.resolve_template(&pipeline.target.schema_template, target_sep);

        for table in &conn.tables {
            if !filter_table_matches(parsed_filter.as_ref(), &table.name) {
                continue;
            }
            targets.push(TargetRef {
                catalog: target_catalog.clone(),
                schema: target_schema.clone(),
                table: table.name.clone(),
            });
        }
    }
    Ok(targets)
}

pub async fn compare(
    config_path: &Path,
    filter: Option<&str>,
    pipeline_name_arg: Option<&str>,
    shadow_config: &ShadowConfig,
    thresholds: &ComparisonThresholds,
    output_json: bool,
) -> Result<()> {
    let rocky_cfg = rocky_core::config::load_rocky_config(config_path).context(format!(
        "failed to load config from {}",
        config_path.display()
    ))?;
    let (resolved_pipeline_name, pipeline_cfg) =
        registry::resolve_pipeline(&rocky_cfg, pipeline_name_arg)?;

    // Reject an unsupported pipeline type BEFORE touching any adapter.
    //
    // `AdapterRegistry::from_config` opens every configured adapter eagerly,
    // so building it first meant a quality pipeline with a typo'd adapter —
    // or any unrelated adapter that fails to open — reported that instead of
    // the thing actually wrong with the command. The type is knowable from
    // the config alone; nothing about it needs a warehouse.
    if !matches!(
        pipeline_cfg,
        rocky_core::config::PipelineConfig::Replication(_)
            | rocky_core::config::PipelineConfig::Transformation(_)
    ) {
        anyhow::bail!(
            "`compare` supports replication and transformation pipelines; \
             '{resolved_pipeline_name}' is a {} pipeline. Shadow tables are only produced for \
             those two.",
            pipeline_cfg.pipeline_type_str()
        );
    }

    let registry = AdapterRegistry::from_config(&rocky_cfg)?;
    let adapter = registry.warehouse_adapter(pipeline_cfg.target_adapter())?;

    // Enumerate the production targets to compare.
    //
    // Replication discovers them from the source; transformation reads them
    // off its models. Only the enumeration differs — the shadow name, the
    // per-pair comparison, the thresholds and the output shape are shared,
    // because `compare` already derived shadow names with the same
    // `shadow_target` the run path uses (#1274).
    let prod_targets: Vec<TargetRef> = match pipeline_cfg {
        rocky_core::config::PipelineConfig::Replication(pipeline) => {
            replication_prod_targets(&registry, pipeline, filter).await?
        }
        rocky_core::config::PipelineConfig::Transformation(pipeline) => {
            super::transformation_prod_targets(pipeline, config_path, filter, "compare")?
                .into_iter()
                .map(|(_model, target)| target)
                .collect()
        }
        // Unreachable: the guard above rejects every other kind before any
        // adapter is opened. Kept exhaustive rather than `unreachable!` so a
        // new pipeline type is a compile error here, not a panic in the field.
        other => anyhow::bail!(
            "`compare` supports replication and transformation pipelines; \
             '{resolved_pipeline_name}' is a {} pipeline.",
            other.pipeline_type_str()
        ),
    };

    let mut output = CompareOutput {
        version: VERSION.to_string(),
        command: "compare".to_string(),
        filter: filter.unwrap_or("").to_string(),
        tables_compared: 0,
        tables_passed: 0,
        tables_warned: 0,
        tables_failed: 0,
        results: vec![],
        overall_verdict: "pass".to_string(),
    };

    for prod_target in prod_targets {
        let shadow_target = rocky_core::shadow::shadow_target(&prod_target, shadow_config);

        // Get row counts
        let (prod_count, prod_count_error) = read_or_default(
            get_row_count(&*adapter, &prod_target).await,
            &format!(
                "failed to read production row count for {}",
                prod_target.full_name()
            ),
        );
        let (shadow_count, shadow_count_error) = read_or_default(
            get_row_count(&*adapter, &shadow_target).await,
            &format!(
                "failed to read shadow row count for {}",
                shadow_target.full_name()
            ),
        );
        let counts_read = prod_count_error.is_none() && shadow_count_error.is_none();

        let (row_count_match, row_count_diff, row_count_diff_pct) =
            compare::compare_row_counts(shadow_count, prod_count);
        let row_count_match = counts_read && row_count_match;

        // Get schemas
        let prod_table_ref = rocky_ir::TableRef {
            catalog: prod_target.catalog.clone(),
            schema: prod_target.schema.clone(),
            table: prod_target.table.clone(),
        };
        let shadow_table_ref = rocky_ir::TableRef {
            catalog: shadow_target.catalog.clone(),
            schema: shadow_target.schema.clone(),
            table: shadow_target.table.clone(),
        };

        let (prod_cols, prod_schema_error) = read_or_default(
            adapter.describe_table(&prod_table_ref).await,
            &format!(
                "failed to read production schema for {}",
                prod_target.full_name()
            ),
        );
        let (shadow_cols, shadow_schema_error) = read_or_default(
            adapter.describe_table(&shadow_table_ref).await,
            &format!(
                "failed to read shadow schema for {}",
                shadow_target.full_name()
            ),
        );
        let schemas_read = prod_schema_error.is_none() && shadow_schema_error.is_none();

        let schema_diffs = compare::compare_schemas(&shadow_cols, &prod_cols);
        let schema_match = schemas_read && schema_diffs.is_empty();

        // Build comparison result and evaluate
        let comparison = ComparisonResult {
            table: prod_target.full_name(),
            row_count_match,
            shadow_count,
            production_count: prod_count,
            row_count_diff,
            row_count_diff_pct,
            schema_match,
            schema_diffs: schema_diffs.clone(),
            sample_match: None,
            sample_mismatches: vec![],
        };

        let read_failures: Vec<String> = [
            prod_count_error,
            shadow_count_error,
            prod_schema_error,
            shadow_schema_error,
        ]
        .into_iter()
        .flatten()
        .collect();
        let verdict = if read_failures.is_empty() {
            compare::evaluate_comparison(&comparison, thresholds)
        } else {
            ComparisonVerdict::Fail(read_failures)
        };

        let verdict_str = match &verdict {
            ComparisonVerdict::Pass => "pass",
            ComparisonVerdict::Warn(_) => "warn",
            ComparisonVerdict::Fail(_) => "fail",
        };

        match verdict {
            ComparisonVerdict::Pass => output.tables_passed += 1,
            ComparisonVerdict::Warn(_) => output.tables_warned += 1,
            ComparisonVerdict::Fail(_) => output.tables_failed += 1,
        }

        let schema_diff_strs: Vec<String> = schema_diffs.iter().map(|d| format!("{d:?}")).collect();

        output.results.push(TableCompareResult {
            production_table: prod_target.full_name(),
            shadow_table: shadow_target.full_name(),
            row_count_match,
            production_count: prod_count,
            shadow_count,
            row_count_diff_pct,
            schema_match,
            schema_diffs: schema_diff_strs,
            verdict: verdict_str.to_string(),
        });

        output.tables_compared += 1;

        info!(
            production = prod_target.full_name(),
            shadow = shadow_target.full_name(),
            verdict = verdict_str,
            "compared table"
        );
    }

    // Overall verdict
    output.overall_verdict = if output.tables_failed > 0 {
        "fail".to_string()
    } else if output.tables_warned > 0 {
        "warn".to_string()
    } else {
        "pass".to_string()
    };

    if output_json {
        print_json(&output)?;
    } else {
        println!("  Rocky Compare");
        println!();
        println!(
            "  Tables: {} compared, {} passed, {} warned, {} failed",
            output.tables_compared,
            output.tables_passed,
            output.tables_warned,
            output.tables_failed
        );
        println!("  Overall: {}", output.overall_verdict.to_uppercase());
        println!();
        for result in &output.results {
            let icon = match result.verdict.as_str() {
                "pass" => "  OK",
                "warn" => "WARN",
                "fail" => "FAIL",
                _ => "  ??",
            };
            println!(
                "  [{icon}] {} (prod={}, shadow={}, diff={:.2}%)",
                result.production_table,
                result.production_count,
                result.shadow_count,
                result.row_count_diff_pct * 100.0,
            );
        }
    }

    if output.tables_failed > 0 {
        anyhow::bail!("{} table(s) failed comparison", output.tables_failed);
    }

    Ok(())
}

fn read_or_default<T: Default, E: std::fmt::Display>(
    result: std::result::Result<T, E>,
    context: &str,
) -> (T, Option<String>) {
    match result {
        Ok(value) => (value, None),
        Err(error) => (T::default(), Some(format!("{context}: {error}"))),
    }
}

/// Get the row count for a target table.
///
/// Returns `Err` when the `COUNT(*)` result is absent or not a non-negative
/// integer, so a garbled read fails the comparison closed instead of silently
/// counting as zero. Parsing goes through `cell_as_u64`, which handles the
/// JSON-integer (Trino), integer-as-string (Databricks/Snowflake/DuckDB), and
/// integral-float cell shapes adapters return — a bare `as_str()` would drop
/// numeric cells to `0` and let a real mismatch pass.
async fn get_row_count(adapter: &dyn WarehouseAdapter, target: &TargetRef) -> Result<u64> {
    let table_name = target
        .validated_full_name()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let sql = format!("SELECT COUNT(*) FROM {table_name}");
    let result = adapter
        .execute_query(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    rocky_core::checks::cell_as_u64(result.rows.first().and_then(|row| row.first()))
        .ok_or_else(|| anyhow::anyhow!("COUNT(*) for {table_name} returned no parseable row count"))
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;
    use rocky_duckdb::adapter::DuckDbWarehouseAdapter;

    /// The headline of #1274: `compare` reaches a transformation pipeline's
    /// models and pairs each production target with its shadow.
    ///
    /// Before this it resolved through `resolve_replication_pipeline` and then
    /// demanded a discovery adapter, so a transformation-only project failed
    /// before looking at any model target — making `--shadow` a write-only
    /// feature for exactly the pipeline type the shadow docs call primary.
    #[tokio::test]
    async fn transformation_pipeline_compares_model_targets_against_shadows() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let db_path = temp.path().join("compare.duckdb");

        // Production and its shadow, with a deliberate one-row difference so
        // the pair is genuinely compared rather than merely enumerated.
        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("open DuckDB");
            for stmt in [
                "CREATE SCHEMA mart",
                "CREATE TABLE mart.orders (id INTEGER)",
                "INSERT INTO mart.orders VALUES (1), (2)",
                "CREATE TABLE mart.orders_rocky_shadow (id INTEGER)",
                "INSERT INTO mart.orders_rocky_shadow VALUES (1)",
            ] {
                adapter
                    .execute_statement(stmt)
                    .await
                    .unwrap_or_else(|e| panic!("{stmt}: {e}"));
            }
        }

        let models = temp.path().join("models");
        std::fs::create_dir_all(&models).expect("mkdir models");
        std::fs::write(models.join("orders.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("orders.toml"),
            "name = \"orders\"\n\n[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"compare\"\nschema = \"mart\"\ntable = \"orders\"\n",
        )
        .expect("write sidecar");

        let config_path = temp.path().join("rocky.toml");
        let escaped_db_path = db_path.to_string_lossy().replace('\\', "\\\\");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{escaped_db_path}"

[pipeline.marts]
type = "transformation"
models = "models/**"

[pipeline.marts.target.governance]
auto_create_schemas = true
"#
            ),
        )
        .expect("write config");

        // Row counts differ (2 vs 1), so the pair must FAIL rather than pass —
        // an enumeration that found nothing would report zero tables and exit
        // clean, which is the bug this replaces.
        let error = compare(
            &config_path,
            None,
            None,
            &ShadowConfig::default(),
            &ComparisonThresholds::default(),
            false,
        )
        .await
        .expect_err("a shadow with fewer rows than production must not pass");

        assert!(
            error.to_string().contains("1 table(s) failed comparison"),
            "the model's target must have been compared, not skipped: {error:#}"
        );
    }

    /// A pipeline type that produces no shadow tables says so, rather than
    /// failing with a discovery-adapter message that does not apply to it.
    #[tokio::test]
    async fn an_unsupported_pipeline_type_is_named() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let db_path = temp.path().join("compare.duckdb");
        {
            DuckDbWarehouseAdapter::open(&db_path).expect("open DuckDB");
        }
        let config_path = temp.path().join("rocky.toml");
        let escaped_db_path = db_path.to_string_lossy().replace('\\', "\\\\");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{escaped_db_path}"

[pipeline.checks]
type = "quality"
checks = []

[pipeline.checks.target]
adapter = "default"
"#
            ),
        )
        .expect("write config");

        let error = compare(
            &config_path,
            None,
            None,
            &ShadowConfig::default(),
            &ComparisonThresholds::default(),
            false,
        )
        .await
        .expect_err("a quality pipeline has no shadow tables to compare");
        let msg = format!("{error:#}");
        assert!(msg.contains("quality"), "must name the kind: {msg}");
        assert!(
            msg.contains("replication and transformation"),
            "must say what is supported: {msg}"
        );
    }

    /// The unsupported-type refusal must not depend on the adapters opening.
    ///
    /// `AdapterRegistry::from_config` opens every configured adapter eagerly,
    /// so building it before the type check meant a quality pipeline with a
    /// typo'd adapter reported *that* instead of the thing actually wrong with
    /// the command. A valid-adapter fixture cannot see the ordering, which is
    /// how the first version of this shipped past its own test.
    #[tokio::test]
    async fn an_unsupported_pipeline_type_is_named_before_adapters_are_opened() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let config_path = temp.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            r#"
[adapter]
type = "duckdb"
path = "/nonexistent/directory/that/cannot/be/opened/x.duckdb"

[pipeline.checks]
type = "quality"
checks = []

[pipeline.checks.target]
adapter = "missing_adapter"
"#,
        )
        .expect("write config");

        let error = compare(
            &config_path,
            None,
            None,
            &ShadowConfig::default(),
            &ComparisonThresholds::default(),
            false,
        )
        .await
        .expect_err("a quality pipeline is refused regardless of its adapter");
        let msg = format!("{error:#}");
        assert!(
            msg.contains("quality"),
            "the pipeline type is the problem, not the adapter: {msg}"
        );
        assert!(
            !msg.contains("missing_adapter"),
            "must not report an adapter it never needed to open: {msg}"
        );
    }

    #[tokio::test]
    async fn unreadable_target_pair_fails_comparison() {
        let temp = tempfile::tempdir().expect("create temp dir");
        let db_path = temp.path().join("compare.duckdb");

        {
            let adapter = DuckDbWarehouseAdapter::open(&db_path).expect("open DuckDB");
            adapter
                .execute_statement("CREATE SCHEMA raw__orders")
                .await
                .expect("create source schema");
            adapter
                .execute_statement("CREATE TABLE raw__orders.orders (id INTEGER)")
                .await
                .expect("create source table");
        }

        let config_path = temp.path().join("rocky.toml");
        let escaped_db_path = db_path.to_string_lossy().replace('\\', "\\\\");
        std::fs::write(
            &config_path,
            format!(
                r#"
[adapter]
type = "duckdb"
path = "{escaped_db_path}"

[pipeline.poc]
strategy = "full_refresh"

[pipeline.poc.source.schema_pattern]
prefix = "raw__"
separator = "__"
components = ["source"]

[pipeline.poc.target]
catalog_template = "compare"
schema_template = "staging__{{source}}"
"#
            ),
        )
        .expect("write config");

        let error = compare(
            &config_path,
            Some("source=orders"),
            None,
            &ShadowConfig::default(),
            &ComparisonThresholds::default(),
            false,
        )
        .await
        .expect_err("two unreadable targets must not pass comparison");

        assert!(
            error.to_string().contains("1 table(s) failed comparison"),
            "unexpected error: {error:#}"
        );
    }
}
