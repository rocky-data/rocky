//! `rocky profile <model> [--column <col>]` — observed per-column data profile.
//!
//! Runs one aggregate query per column (row / null / distinct counts, min / max,
//! and a bounded low-cardinality domain) against the model's target table.
//! DuckDB only this release — the same profiling primitive `rocky ai-contract`
//! uses to ground its drafts, exposed without the LLM round-trip.

use std::path::Path;

use anyhow::Result;

use crate::output::{ProfileColumnStats, ProfileOutput, print_json};

use super::ai_contract::{
    FallbackPolicy, PreparedKind, compile_project, prepare_table_query, profile_column,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Build the profile payload for `model_name`, optionally narrowed to one
/// column. Returns a [`ProfileOutput`] carrying either the per-column stats or
/// an `unavailable` reason (a non-DuckDB target this release).
pub async fn build_profile_output(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: &str,
    column: Option<&str>,
    cache_ttl_override: Option<u64>,
) -> Result<ProfileOutput> {
    let compile_result = compile_project(config_path, state_path, models_dir, cache_ttl_override)?;

    let inferred_schema = compile_result
        .type_check
        .typed_models
        .get(model_name)
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "model '{model_name}' not found in compiled project (or has no inferred schema)"
            )
        })?;

    // `SourceFallback`: if the model's declared target isn't materialized
    // (agentic authoring loop pre-`rocky run`, or a replication-pipeline POC
    // that doesn't run the transformation models), profile its first
    // resolvable source instead so the caller still gets observed data. The
    // fallback is labelled via `profiled_table` + `fell_back_from` so the JSON
    // tells the truth.
    let prepared = match prepare_table_query(
        config_path,
        &compile_result,
        model_name,
        FallbackPolicy::SourceFallback,
    )
    .await?
    {
        PreparedKind::Ready(p) => p,
        PreparedKind::Unavailable(reason) => {
            return Ok(ProfileOutput {
                version: VERSION.to_string(),
                command: "profile".to_string(),
                model: model_name.to_string(),
                profiled_table: None,
                fell_back_from: None,
                columns: Vec::new(),
                unavailable: Some(reason),
            });
        }
    };

    let targets: Vec<_> = match column {
        Some(name) => inferred_schema.iter().filter(|c| c.name == name).collect(),
        None => inferred_schema.iter().collect(),
    };
    if let Some(name) = column
        && targets.is_empty()
    {
        anyhow::bail!("column '{name}' not found in model '{model_name}'");
    }

    let fell_back = prepared.fell_back_from.is_some();
    let mut columns = Vec::with_capacity(targets.len());
    for col in targets {
        // On the source-fallback path, a column from the model's inferred
        // schema may not exist on the source (the model added/renamed it).
        // Skip such columns rather than aborting the whole profile — the
        // caller gets whatever overlap exists, plus `fell_back_from` to
        // signal "this is a source preview." On the target path we keep
        // strict behaviour: any per-column error is a real problem.
        let result = profile_column(prepared.adapter.as_ref(), &prepared.table_ref, col).await;
        match result {
            Ok(p) => columns.push(ProfileColumnStats {
                name: p.name,
                type_name: p.type_name,
                rows: p.rows,
                nulls: p.nulls,
                null_rate: p.null_rate,
                distinct: p.distinct,
                observed_values: p.observed_values,
                min: p.min,
                max: p.max,
            }),
            Err(e) if fell_back => {
                tracing::debug!(column = %col.name, error = %e, "skipping column in source-fallback profile");
            }
            Err(e) => return Err(e),
        }
    }

    Ok(ProfileOutput {
        version: VERSION.to_string(),
        command: "profile".to_string(),
        model: model_name.to_string(),
        profiled_table: Some(prepared.table_ref),
        fell_back_from: prepared.fell_back_from,
        columns,
        unavailable: None,
    })
}

/// Execute `rocky profile <model>` — print the observed per-column profile.
pub async fn run_profile(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: &str,
    column: Option<&str>,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let output = build_profile_output(
        config_path,
        state_path,
        models_dir,
        model_name,
        column,
        cache_ttl_override,
    )
    .await?;

    if output_json {
        print_json(&output)?;
    } else if let Some(reason) = &output.unavailable {
        println!("Profile unavailable: {reason}");
    } else {
        println!("Profile for model: {}", output.model);
        for col in &output.columns {
            println!(
                "  {} ({}): {} rows, {} nulls ({:.1}%), {} distinct",
                col.name,
                col.type_name,
                col.rows,
                col.nulls,
                col.null_rate * 100.0,
                col.distinct,
            );
        }
    }
    Ok(())
}

#[cfg(all(test, feature = "duckdb"))]
mod tests {
    use super::*;
    use rocky_compiler::types::{RockyType, TypedColumn};

    /// Live DuckDB profiling of a seeded table column — exercises the profiling
    /// primitive `rocky profile` reuses (the aggregate query + low-cardinality
    /// domain). No credentials or LLM needed.
    #[tokio::test]
    async fn profiles_a_seeded_duckdb_column() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("warehouse.duckdb");
        let config_path = dir.path().join("rocky.toml");
        std::fs::write(
            &config_path,
            format!(
                "[adapter.warehouse]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.main]\ntype = \"transformation\"\n\n\
                 [pipeline.main.target]\nadapter = \"warehouse\"\n",
                db_path.display()
            ),
        )
        .unwrap();

        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let registry = crate::registry::AdapterRegistry::from_config(&cfg).unwrap();
        let adapter = registry.warehouse_adapter("warehouse").unwrap();
        for stmt in [
            "CREATE SCHEMA IF NOT EXISTS main",
            "CREATE TABLE main.orders (id BIGINT, status VARCHAR)",
            "INSERT INTO main.orders VALUES (1,'a'),(2,'b'),(NULL,'b')",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }

        let status_col = TypedColumn {
            name: "status".to_string(),
            data_type: RockyType::String,
            nullable: true,
        };
        let profile = profile_column(adapter.as_ref(), "main.orders", &status_col)
            .await
            .expect("profile_column should succeed on duckdb");
        assert_eq!(profile.rows, 3);
        assert_eq!(profile.distinct, 2);
        assert_eq!(profile.nulls, 0);
        // Low-cardinality column: the observed domain is surfaced as evidence.
        assert_eq!(profile.observed_values.len(), 2);

        let id_col = TypedColumn {
            name: "id".to_string(),
            data_type: RockyType::Int64,
            nullable: true,
        };
        let id_profile = profile_column(adapter.as_ref(), "main.orders", &id_col)
            .await
            .unwrap();
        assert_eq!(id_profile.rows, 3);
        assert_eq!(id_profile.nulls, 1);
    }

    /// Set up a temp DuckDB + rocky.toml + a `raw_orders` model whose SQL
    /// reads `FROM raw__orders.orders`. The seed creates the source table;
    /// the caller chooses whether to also materialize the declared target,
    /// which exercises the source-fallback vs no-fallback paths in one
    /// shared scaffold.
    async fn scaffold_fallback_poc(
        materialize_target: bool,
    ) -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("wh.duckdb");
        let config_path = dir.path().join("rocky.toml");
        let models_dir = dir.path().join("models");
        std::fs::create_dir_all(&models_dir).unwrap();
        std::fs::write(
            &config_path,
            format!(
                "[adapter.warehouse]\ntype = \"duckdb\"\npath = \"{}\"\n\n\
                 [pipeline.t]\ntype = \"transformation\"\n\n\
                 [pipeline.t.target]\nadapter = \"warehouse\"\n",
                db_path.display()
            ),
        )
        .unwrap();
        std::fs::write(
            models_dir.join("raw_orders.sql"),
            "SELECT order_id, amount FROM raw__orders.orders",
        )
        .unwrap();
        std::fs::write(
            models_dir.join("raw_orders.toml"),
            "[strategy]\ntype = \"full_refresh\"\n\n\
             [target]\ncatalog = \"wh\"\nschema = \"staging\"\n",
        )
        .unwrap();

        let cfg = rocky_core::config::load_rocky_config(&config_path).unwrap();
        let registry = crate::registry::AdapterRegistry::from_config(&cfg).unwrap();
        let adapter = registry.warehouse_adapter("warehouse").unwrap();
        for stmt in [
            "CREATE SCHEMA IF NOT EXISTS raw__orders",
            "CREATE TABLE raw__orders.orders (order_id BIGINT, amount DOUBLE)",
            "INSERT INTO raw__orders.orders VALUES (1, 10.0), (2, 20.0), (3, 30.0)",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }
        if materialize_target {
            for stmt in [
                "CREATE SCHEMA IF NOT EXISTS staging",
                "CREATE TABLE staging.raw_orders AS \
                 SELECT order_id, amount FROM raw__orders.orders",
            ] {
                adapter.execute_statement(stmt).await.unwrap();
            }
        }
        (dir, config_path, models_dir)
    }

    /// Target table absent + a source table exists → profile falls back to
    /// the source and labels the fallback. The agentic authoring loop and
    /// replication-pipeline demos rely on this branch.
    #[tokio::test]
    async fn falls_back_to_source_when_target_missing() {
        let (_tmp, config_path, models_dir) = scaffold_fallback_poc(false).await;
        let state_path = config_path.parent().unwrap().join(".rocky_state");
        let output = build_profile_output(
            &config_path,
            &state_path,
            models_dir.to_str().unwrap(),
            "raw_orders",
            None,
            None,
        )
        .await
        .expect("profile should succeed via source fallback");
        assert_eq!(output.profiled_table.as_deref(), Some("raw__orders.orders"));
        assert_eq!(output.fell_back_from.as_deref(), Some("staging.raw_orders"));
        assert_eq!(output.unavailable, None);
        // Both source columns exist on the model → both should profile.
        let names: Vec<_> = output.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"order_id"));
        assert!(names.contains(&"amount"));
        assert_eq!(output.columns[0].rows, 3);
    }

    /// Target table materialized → profile uses it directly, no fallback.
    /// Guards against accidentally falling back when the model is healthy.
    #[tokio::test]
    async fn no_fallback_when_target_materialized() {
        let (_tmp, config_path, models_dir) = scaffold_fallback_poc(true).await;
        let state_path = config_path.parent().unwrap().join(".rocky_state");
        let output = build_profile_output(
            &config_path,
            &state_path,
            models_dir.to_str().unwrap(),
            "raw_orders",
            None,
            None,
        )
        .await
        .expect("profile should succeed against the materialized target");
        assert_eq!(output.profiled_table.as_deref(), Some("staging.raw_orders"));
        assert_eq!(output.fell_back_from, None);
        assert_eq!(output.columns[0].rows, 3);
    }
}
