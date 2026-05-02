use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::config::load_rocky_config;
use rocky_core::retention::StateRetentionConfig;
use rocky_core::state::StateStore;

use crate::output::*;

/// Execute `rocky state show`.
pub fn state_show(state_path: &Path, output_json: bool) -> Result<()> {
    let store = StateStore::open_read_only(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;
    let watermarks = store
        .list_watermarks()
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    let entries: Vec<WatermarkEntry> = watermarks
        .into_iter()
        .map(|(table, wm)| WatermarkEntry {
            table,
            last_value: wm.last_value,
            updated_at: wm.updated_at,
        })
        .collect();

    if output_json {
        print_json(&StateOutput::new(entries))?;
    } else {
        for e in &entries {
            println!("{} | {} | {}", e.table, e.last_value, e.updated_at);
        }
        if entries.is_empty() {
            println!("No watermarks stored.");
        }
    }
    Ok(())
}

/// Execute `rocky state clear-schema-cache`.
///
/// The explicit-flush path for the DESCRIBE cache. Counterpart to the
/// TTL auto-eviction baked into the read path: users who want a cache
/// refresh *now* (e.g. after a manual warehouse DDL change, or during
/// strict-CI debugging) use this command.
///
/// Behaviour:
/// - No prompt (the cache is cheap to rebuild via the next `rocky run`
///   or `rocky discover --with-schemas` — explicit opt-in is sufficient).
/// - `dry_run = true` reports what *would* be removed without touching
///   redb. Useful for automation scripts that want to assert emptiness
///   before a scheduled flush.
/// - Does NOT fail when the state store is missing (fresh clone, nothing
///   to flush) — emits an empty result and exits zero. Failing here would
///   be user-hostile for the "make sure the cache is clear before this
///   CI run" use case on an ephemeral runner.
pub fn state_clear_schema_cache(state_path: &Path, dry_run: bool, output_json: bool) -> Result<()> {
    // Missing state.redb → nothing to flush. Report 0 and exit cleanly so
    // CI pipelines that run `rocky state clear-schema-cache` unconditionally
    // before a build don't fail on a fresh runner.
    if !state_path.exists() {
        return emit_result(0, dry_run, output_json);
    }

    let store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    // Gather the current key set up front. `list_schema_cache` already
    // tolerates an empty table, so an uninitialised cache returns
    // `entries_deleted = 0` without ever reaching the delete loop.
    let entries = store
        .list_schema_cache()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let count = entries.len();

    if !dry_run {
        for (key, _entry) in &entries {
            store
                .delete_schema_cache_entry(key)
                .map_err(|e| anyhow::anyhow!("{e}"))?;
        }
    }

    emit_result(count, dry_run, output_json)
}

/// Execute `rocky state retention sweep`.
///
/// Loads the [`StateRetentionConfig`] from the project's `[state.retention]`
/// block (falling back to the defaults — `max_age_days = 365`,
/// `min_runs_kept = 100`, `applies_to = [history, lineage, audit]` —
/// when the section is absent), then sweeps run history, DAG snapshots,
/// and quality snapshots accordingly.
///
/// Behaviour:
/// - Missing `state.redb` → emits a zero-count report and exits cleanly,
///   matching `state clear-schema-cache` (CI-safe on ephemeral runners).
/// - `dry_run = true` runs the planner but skips every write transaction
///   so the store is left untouched. The reported counts match what an
///   apply run would produce, modulo concurrent writers.
pub fn state_retention_sweep(
    config_path: &Path,
    state_path: &Path,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    // Read the policy from rocky.toml so the same sweep semantics apply
    // whether the operator runs the command manually or it's wired into a
    // future scheduled hook. `config_path` may not exist (init flow) — in
    // that case we sweep with the defaults.
    let policy = if config_path.exists() {
        let cfg = load_rocky_config(config_path)
            .with_context(|| format!("loading rocky config at {}", config_path.display()))?;
        cfg.state.retention
    } else {
        StateRetentionConfig::default()
    };

    if !state_path.exists() {
        emit_sweep_report(&Default::default(), &policy, dry_run, output_json)?;
        return Ok(());
    }

    let store = StateStore::open(state_path).context(format!(
        "failed to open state store at {}",
        state_path.display()
    ))?;

    let report = if dry_run {
        store
            .sweep_retention_dry_run(&policy)
            .map_err(|e| anyhow::anyhow!("{e}"))?
    } else {
        store
            .sweep_retention(&policy)
            .map_err(|e| anyhow::anyhow!("{e}"))?
    };

    emit_sweep_report(&report, &policy, dry_run, output_json)
}

fn emit_sweep_report(
    report: &rocky_core::retention::SweepReport,
    policy: &StateRetentionConfig,
    dry_run: bool,
    output_json: bool,
) -> Result<()> {
    if output_json {
        print_json(&RetentionSweepOutput::new(report, policy, dry_run))?;
    } else {
        let prefix = if dry_run { "[dry-run] would remove" } else { "Removed" };
        println!(
            "{prefix} {} run records, {} dag snapshots, {} quality snapshots ({} ms)",
            report.runs_deleted, report.lineage_deleted, report.audit_deleted, report.duration_ms,
        );
        println!(
            "Kept: {} runs, {} dag snapshots, {} quality snapshots",
            report.runs_kept, report.lineage_kept, report.audit_kept,
        );
    }
    Ok(())
}

fn emit_result(count: usize, dry_run: bool, output_json: bool) -> Result<()> {
    if output_json {
        print_json(&ClearSchemaCacheOutput::new(count, dry_run))?;
    } else if dry_run {
        println!(
            "[dry-run] would remove {count} schema cache entr{}",
            if count == 1 { "y" } else { "ies" }
        );
    } else {
        println!(
            "Removed {count} schema cache entr{}",
            if count == 1 { "y" } else { "ies" }
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use rocky_core::schema_cache::{SchemaCacheEntry, StoredColumn, schema_cache_key};
    use tempfile::TempDir;

    fn seed_cache(store: &StateStore, catalog: &str, schema: &str, table: &str) {
        let entry = SchemaCacheEntry {
            columns: vec![StoredColumn {
                name: "id".into(),
                data_type: "BIGINT".into(),
                nullable: false,
            }],
            cached_at: Utc::now(),
        };
        store
            .write_schema_cache_entry(&schema_cache_key(catalog, schema, table), &entry)
            .unwrap();
    }

    #[test]
    fn clear_removes_all_entries() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "c", "staging", "orders");
            seed_cache(&store, "c", "staging", "customers");
            seed_cache(&store, "other", "staging", "events");
            assert_eq!(store.list_schema_cache().unwrap().len(), 3);
        }

        state_clear_schema_cache(&path, false, false).unwrap();

        let store = StateStore::open(&path).unwrap();
        assert!(
            store.list_schema_cache().unwrap().is_empty(),
            "all entries should be deleted"
        );
    }

    #[test]
    fn dry_run_reports_but_does_not_delete() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let store = StateStore::open(&path).unwrap();
            seed_cache(&store, "c", "staging", "orders");
            seed_cache(&store, "c", "staging", "customers");
        }

        state_clear_schema_cache(&path, true, false).unwrap();

        let store = StateStore::open(&path).unwrap();
        assert_eq!(
            store.list_schema_cache().unwrap().len(),
            2,
            "dry-run must leave entries intact"
        );
    }

    #[test]
    fn missing_state_path_is_not_an_error() {
        // Ephemeral CI runners don't have `.rocky-state.redb` before the
        // first `rocky run`. The clear command should succeed (nothing to
        // flush) rather than erroring — otherwise "flush before CI" is
        // impossible to automate.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        assert!(!path.exists());

        state_clear_schema_cache(&path, false, false).unwrap();
        state_clear_schema_cache(&path, true, false).unwrap();

        // Must not have been created as a side effect.
        assert!(!path.exists(), "clear must not create state.redb");
    }

    #[test]
    fn clear_on_empty_cache_returns_zero() {
        // `rocky run` has opened state.redb but nothing has written to
        // SCHEMA_CACHE yet (PR 2 write tap not exercised). Clearing should
        // be a no-op that reports 0.
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("state.redb");
        {
            let _store = StateStore::open(&path).unwrap();
        }
        // File exists but the table is empty.
        assert!(path.exists());
        state_clear_schema_cache(&path, false, false).unwrap();
    }
}
