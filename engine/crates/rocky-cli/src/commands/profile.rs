//! `rocky profile <model> [--column <col>]` — observed per-column data profile.
//!
//! Runs one aggregate query per column (row / null / distinct counts, min / max,
//! and a bounded low-cardinality domain) against the model's target table.
//! DuckDB only this release — the same profiling primitive `rocky ai-contract`
//! uses to ground its drafts, exposed without the LLM round-trip.

use std::collections::HashMap;
use std::path::Path;

use anyhow::Result;

use rocky_core::traits::WarehouseAdapter;

use crate::output::{ProfileColumnStats, ProfileOutput, print_json};

use super::ai_contract::{
    FallbackPolicy, PreparedKind, compile_project, prepare_table_query, profile_column, str_cell,
};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Upper bound for `rocky profile --sample N`. The CLI enforces it too; this
/// guards library callers.
pub const MAX_SAMPLE_VALUES: u32 = 100;

/// Up to `n` distinct non-null values of `column`, sorted.
///
/// The values are the `n` with the lowest `hash(value)`: a pseudo-random
/// choice that is the same on every run while the column's values are
/// unchanged, whatever order the scan returns rows in (a seeded `USING
/// SAMPLE` fixes the generator but not the input order of a parallel
/// `DISTINCT`, so it is not repeatable on large tables). New rows only change
/// the sample when one of them hashes lower.
///
/// DuckDB syntax: `rocky profile` is DuckDB-only this release (see
/// `duckdb_only_refusal`). The `DISTINCT` scans the whole column. SQL is built
/// from the already-validated `table_ref` and a freshly-validated column
/// identifier.
async fn sample_column_values(
    adapter: &dyn WarehouseAdapter,
    table_ref: &str,
    column: &str,
    n: u32,
) -> Result<Vec<String>> {
    let col = rocky_sql::validation::validate_identifier(column)
        .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;
    let n = n.min(MAX_SAMPLE_VALUES);
    if n == 0 {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT v FROM (SELECT DISTINCT CAST({col} AS VARCHAR) AS v FROM {table_ref} \
         WHERE {col} IS NOT NULL) AS s ORDER BY hash(v), v LIMIT {n}"
    );
    let qr = adapter
        .execute_query(&sql)
        .await
        .map_err(|e| anyhow::anyhow!("sample query failed for column '{column}': {e}"))?;
    let mut values: Vec<String> = qr.rows.iter().filter_map(|r| str_cell(r.first())).collect();
    values.sort();
    Ok(values)
}

/// Observed warehouse types for a `schema.table` (or `catalog.schema.table`)
/// ref, keyed by lowercased column name. Profile reports these instead of the
/// compiler's inferred types, which come back `Unknown` for raw-SQL models
/// whose source schemas weren't resolved at compile time — the warehouse is
/// the authoritative source for a *profile* of what's actually there. Returns
/// an empty map on any describe error (profile then falls back to the inferred
/// type), so a describe failure never aborts the profile.
async fn observed_column_types(
    adapter: &dyn WarehouseAdapter,
    table_ref: &str,
) -> HashMap<String, String> {
    let parts: Vec<&str> = table_ref.split('.').collect();
    let tref = match parts.as_slice() {
        [schema, table] => rocky_ir::TableRef {
            catalog: String::new(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        },
        [catalog, schema, table] => rocky_ir::TableRef {
            catalog: (*catalog).to_string(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        },
        _ => return HashMap::new(),
    };
    match adapter.describe_table(&tref).await {
        Ok(cols) => cols
            .into_iter()
            .map(|c| (c.name.to_lowercase(), c.data_type))
            .collect(),
        Err(_) => HashMap::new(),
    }
}

/// Build the profile payload for `model_name`, optionally narrowed to one
/// column. Returns a [`ProfileOutput`] carrying either the per-column stats or
/// an `unavailable` reason (a non-DuckDB target this release). `sample > 0`
/// adds up to that many random distinct values per column
/// (`sample_values`), capped at [`MAX_SAMPLE_VALUES`].
pub async fn build_profile_output(
    config_path: &Path,
    state_path: &Path,
    models_dir: &str,
    model_name: &str,
    column: Option<&str>,
    sample: u32,
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

    // Observed warehouse types for the table actually profiled — preferred
    // over the compiler's inferred types (which are `Unknown` for raw-SQL
    // models on a cold source-schema cache).
    let observed_types =
        observed_column_types(prepared.adapter.as_ref(), &prepared.table_ref).await;

    let fell_back = prepared.fell_back_from.is_some();
    let mut columns = Vec::with_capacity(targets.len());
    for col in targets {
        // On the source-fallback path, a column from the model's inferred
        // schema may not exist on the source (the model added/renamed it).
        // Skip such columns rather than aborting the whole profile — the
        // caller gets whatever overlap exists, plus `fell_back_from` to
        // signal "this is a source preview." On the target path we keep
        // strict behaviour: any per-column error is a real problem.
        // `rocky profile` prints min/max/distinct to the user locally — no LLM
        // egress — so it always wants the full per-column values.
        let result =
            profile_column(prepared.adapter.as_ref(), &prepared.table_ref, col, true).await;
        let result = match result {
            Ok(p) if sample > 0 => sample_column_values(
                prepared.adapter.as_ref(),
                &prepared.table_ref,
                &p.name,
                sample,
            )
            .await
            .map(|values| (p, Some(values))),
            Ok(p) => Ok((p, None)),
            Err(e) => Err(e),
        };
        match result {
            Ok((p, sample_values)) => columns.push(ProfileColumnStats {
                // Prefer the observed warehouse type; fall back to the
                // compiler's inferred name when the column isn't in the
                // describe (or describe failed).
                type_name: observed_types
                    .get(&p.name.to_lowercase())
                    .cloned()
                    .unwrap_or(p.type_name),
                name: p.name,
                rows: p.rows,
                nulls: p.nulls,
                null_rate: p.null_rate,
                distinct: p.distinct,
                observed_values: p.observed_values,
                min: p.min,
                max: p.max,
                sample_values,
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
    sample: u32,
    output_json: bool,
    cache_ttl_override: Option<u64>,
) -> Result<()> {
    let output = build_profile_output(
        config_path,
        state_path,
        models_dir,
        model_name,
        column,
        sample,
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
            if let Some(values) = &col.sample_values {
                println!("    sample: {}", values.join(", "));
            }
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
        let profile = profile_column(adapter.as_ref(), "main.orders", &status_col, true)
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
        let id_profile = profile_column(adapter.as_ref(), "main.orders", &id_col, true)
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
            0,
            None,
        )
        .await
        .expect("profile should succeed via source fallback");
        assert_eq!(output.profiled_table.as_deref(), Some("raw__orders.orders"));
        assert_eq!(output.fell_back_from.as_deref(), Some("staging.raw_orders"));
        assert_eq!(output.unavailable, None);
        // Both source columns exist on the model → both should profile.
        let order_id = output
            .columns
            .iter()
            .find(|c| c.name == "order_id")
            .expect("order_id profiled");
        assert!(output.columns.iter().any(|c| c.name == "amount"));
        assert_eq!(order_id.rows, 3);
        // The observed warehouse type is reported, not the compiler's
        // `Unknown` (the source schema isn't resolved at compile time here).
        assert_eq!(order_id.type_name, "BIGINT");
    }

    /// `--sample N` returns N distinct non-null values from the column,
    /// sorted, the same set on a re-run, and every value in the column's
    /// domain. Without `--sample` the field is absent.
    #[tokio::test]
    async fn sample_returns_stable_distinct_non_null_values() {
        let (_tmp, config_path, models_dir) = scaffold_fallback_poc(true).await;
        let state_path = config_path.parent().unwrap().join(".rocky_state");
        let run = |sample| {
            build_profile_output(
                &config_path,
                &state_path,
                models_dir.to_str().unwrap(),
                "raw_orders",
                Some("order_id"),
                sample,
                None,
            )
        };

        let first = run(2).await.expect("profile with --sample 2");
        let values = first.columns[0]
            .sample_values
            .clone()
            .expect("sample_values set");
        assert_eq!(values.len(), 2);
        assert!(
            values.windows(2).all(|w| w[0] < w[1]),
            "sorted and distinct: {values:?}"
        );
        assert!(values.iter().all(|v| ["1", "2", "3"].contains(&v.as_str())));

        let again = run(2).await.unwrap();
        assert_eq!(
            again.columns[0].sample_values.as_ref(),
            Some(&values),
            "same values on a re-run"
        );

        // Asking for more than the column holds returns every distinct value.
        let all = run(10).await.unwrap();
        assert_eq!(
            all.columns[0].sample_values.as_deref(),
            Some(&["1".to_string(), "2".to_string(), "3".to_string()][..])
        );

        let none = run(0).await.unwrap();
        assert_eq!(none.columns[0].sample_values, None);
    }

    /// NULLs never appear in the sample, and a column of only NULLs gives an
    /// empty sample rather than an error.
    #[tokio::test]
    async fn sample_skips_nulls() {
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
            "CREATE TABLE main.t (a VARCHAR, b VARCHAR)",
            "INSERT INTO main.t VALUES ('x', NULL), (NULL, NULL), ('y', NULL)",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }
        let a = sample_column_values(adapter.as_ref(), "main.t", "a", 5)
            .await
            .unwrap();
        assert_eq!(a, vec!["x".to_string(), "y".to_string()]);
        let b = sample_column_values(adapter.as_ref(), "main.t", "b", 5)
            .await
            .unwrap();
        assert!(b.is_empty());
        let zero = sample_column_values(adapter.as_ref(), "main.t", "a", 0)
            .await
            .unwrap();
        assert!(zero.is_empty());

        // The choice depends on the values, not on the order rows arrive in:
        // the same 5,000 values loaded ascending and descending give one sample.
        for stmt in [
            "CREATE TABLE main.up AS SELECT CAST(i AS VARCHAR) AS v FROM range(5000) t(i) ORDER BY i",
            "CREATE TABLE main.down AS SELECT CAST(i AS VARCHAR) AS v FROM range(5000) t(i) ORDER BY i DESC",
        ] {
            adapter.execute_statement(stmt).await.unwrap();
        }
        let up = sample_column_values(adapter.as_ref(), "main.up", "v", 7)
            .await
            .unwrap();
        let down = sample_column_values(adapter.as_ref(), "main.down", "v", 7)
            .await
            .unwrap();
        assert_eq!(up.len(), 7);
        assert_eq!(up, down);
        // Not just the first rows of the scan.
        assert_ne!(up, (0..7).map(|i| i.to_string()).collect::<Vec<_>>());
        // A hostile identifier is refused before any SQL is built.
        assert!(
            sample_column_values(adapter.as_ref(), "main.t", "a; DROP TABLE main.t", 5)
                .await
                .is_err()
        );
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
            0,
            None,
        )
        .await
        .expect("profile should succeed against the materialized target");
        assert_eq!(output.profiled_table.as_deref(), Some("staging.raw_orders"));
        assert_eq!(output.fell_back_from, None);
        assert_eq!(output.columns[0].rows, 3);
    }
}
