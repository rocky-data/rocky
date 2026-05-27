//! `rocky profile <model> [--column <col>]` — observed per-column data profile.
//!
//! Runs one aggregate query per column (row / null / distinct counts, min / max,
//! and a bounded low-cardinality domain) against the model's target table.
//! DuckDB only this release — the same profiling primitive `rocky ai-contract`
//! uses to ground its drafts, exposed without the LLM round-trip.

use std::path::Path;

use anyhow::Result;

use crate::output::{ProfileColumnStats, ProfileOutput, print_json};

use super::ai_contract::{PreparedKind, compile_project, prepare_table_query, profile_column};

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

    let prepared = match prepare_table_query(config_path, &compile_result, model_name)? {
        PreparedKind::Ready(p) => p,
        PreparedKind::Unavailable(reason) => {
            return Ok(ProfileOutput {
                version: VERSION.to_string(),
                command: "profile".to_string(),
                model: model_name.to_string(),
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

    let mut columns = Vec::with_capacity(targets.len());
    for col in targets {
        let p = profile_column(prepared.adapter.as_ref(), &prepared.table_ref, col).await?;
        columns.push(ProfileColumnStats {
            name: p.name,
            type_name: p.type_name,
            rows: p.rows,
            nulls: p.nulls,
            null_rate: p.null_rate,
            distinct: p.distinct,
            observed_values: p.observed_values,
            min: p.min,
            max: p.max,
        });
    }

    Ok(ProfileOutput {
        version: VERSION.to_string(),
        command: "profile".to_string(),
        model: model_name.to_string(),
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
}
