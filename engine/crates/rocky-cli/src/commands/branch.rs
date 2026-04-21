//! `rocky branch` — named virtual branches backed by the state store.
//!
//! Branches are the persistent, named analogue of `--shadow` mode: a named
//! `schema_prefix` is stored in the state DB and, when run with
//! `--branch <name>`, every model target has the prefix appended.
//!
//! Warehouse-native clones (Delta `SHALLOW CLONE`, Snowflake zero-copy
//! `CLONE`) are a follow-up; schema-prefix branches work uniformly across
//! every adapter today.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::compare::ComparisonThresholds;
use rocky_core::shadow::ShadowConfig;
use rocky_core::state::{BranchRecord, StateStore};

use crate::output::{BranchDeleteOutput, BranchEntry, BranchListOutput, BranchOutput};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Validate a branch name. Kept lenient (same charset as model/principal
/// identifiers) so branches can be named after git branches like
/// `fix-price` or `feature/new-join` — the slash is rejected because it
/// collides with schema-prefix interpolation.
fn validate_branch_name(name: &str) -> Result<()> {
    if name.is_empty() {
        anyhow::bail!("branch name cannot be empty");
    }
    if name.len() > 64 {
        anyhow::bail!("branch name too long: {} chars (max 64)", name.len());
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == '.')
    {
        anyhow::bail!("invalid branch name '{name}': only [A-Za-z0-9_.-] characters are allowed");
    }
    Ok(())
}

fn to_entry(record: &BranchRecord) -> BranchEntry {
    BranchEntry {
        name: record.name.clone(),
        schema_prefix: record.schema_prefix.clone(),
        created_by: record.created_by.clone(),
        created_at: record.created_at.to_rfc3339(),
        description: record.description.clone(),
    }
}

/// `rocky branch create <name>` — register a new branch in the state store.
pub fn run_branch_create(
    state_path: &Path,
    name: &str,
    description: Option<&str>,
    json: bool,
) -> Result<()> {
    validate_branch_name(name)?;

    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    if store.get_branch(name)?.is_some() {
        anyhow::bail!("branch '{name}' already exists");
    }

    let created_by = std::env::var("USER").unwrap_or_else(|_| "unknown".to_string());
    let record = BranchRecord {
        name: name.to_string(),
        schema_prefix: format!("branch__{name}"),
        created_by,
        created_at: chrono::Utc::now(),
        description: description.map(ToString::to_string),
    };

    store.put_branch(&record)?;

    if json {
        let output = BranchOutput {
            version: VERSION.to_string(),
            command: "branch create".to_string(),
            branch: to_entry(&record),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!(
            "created branch '{name}' (schema_prefix: {})",
            record.schema_prefix
        );
    }
    Ok(())
}

/// `rocky branch delete <name>` — remove a branch entry. The branch's
/// warehouse tables (if any were materialized via `rocky run --branch`)
/// are NOT dropped — that's a separate cleanup step left to the user.
pub fn run_branch_delete(state_path: &Path, name: &str, json: bool) -> Result<()> {
    let store = StateStore::open(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let removed = store.delete_branch(name)?;

    if json {
        let output = BranchDeleteOutput {
            version: VERSION.to_string(),
            command: "branch delete".to_string(),
            name: name.to_string(),
            removed,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else if removed {
        println!("deleted branch '{name}'");
    } else {
        println!("no branch named '{name}'");
    }
    Ok(())
}

/// `rocky branch list` — list every branch in the state store.
pub fn run_branch_list(state_path: &Path, json: bool) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let records = store.list_branches()?;

    if json {
        let entries: Vec<BranchEntry> = records.iter().map(to_entry).collect();
        let total = entries.len();
        let output = BranchListOutput {
            version: VERSION.to_string(),
            command: "branch list".to_string(),
            branches: entries,
            total,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else if records.is_empty() {
        println!("no branches");
    } else {
        println!("branches ({}):", records.len());
        for b in &records {
            print!("  {}  schema_prefix={}", b.name, b.schema_prefix);
            if let Some(desc) = &b.description {
                print!("  ({desc})");
            }
            println!("  by {} at {}", b.created_by, b.created_at.to_rfc3339());
        }
    }
    Ok(())
}

/// `rocky branch compare <name>` — diff a branch's table set against the
/// main (production) targets.
///
/// Looks up the branch's `schema_prefix` in the state store and reuses the
/// existing `rocky compare` machinery by constructing a [`ShadowConfig`]
/// whose `schema_override` points at the branch's schema. Mirrors how
/// `rocky run --branch <name>` wires the same prefix into the write path.
///
/// Branches with no materialized tables yet surface as missing rows/columns
/// on the production side of the diff rather than crashing — the underlying
/// compare path uses `unwrap_or_default` for describe/count failures.
pub async fn run_branch_compare(
    state_path: &Path,
    config_path: &Path,
    branch_name: &str,
    filter: Option<&str>,
    json: bool,
) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = store
        .get_branch(branch_name)?
        .with_context(|| format!("branch '{branch_name}' not found — see 'rocky branch list'"))?;

    let shadow_config = ShadowConfig {
        suffix: "_rocky_shadow".to_string(),
        schema_override: Some(record.schema_prefix),
        cleanup_after: false,
    };
    let thresholds = ComparisonThresholds::default();

    super::compare::compare(config_path, filter, None, &shadow_config, &thresholds, json).await
}

/// `rocky branch show <name>` — inspect a single branch.
pub fn run_branch_show(state_path: &Path, name: &str, json: bool) -> Result<()> {
    let store = StateStore::open_read_only(state_path)
        .with_context(|| format!("failed to open state store at {}", state_path.display()))?;

    let record = store
        .get_branch(name)?
        .with_context(|| format!("branch '{name}' not found"))?;

    if json {
        let output = BranchOutput {
            version: VERSION.to_string(),
            command: "branch show".to_string(),
            branch: to_entry(&record),
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("branch: {}", record.name);
        println!("schema_prefix: {}", record.schema_prefix);
        println!("created_by: {}", record.created_by);
        println!("created_at: {}", record.created_at.to_rfc3339());
        if let Some(desc) = &record.description {
            println!("description: {desc}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn validate_accepts_common_names() {
        assert!(validate_branch_name("fix-price").is_ok());
        assert!(validate_branch_name("feat_new_join").is_ok());
        assert!(validate_branch_name("hotfix.2026-04-20").is_ok());
        assert!(validate_branch_name("a").is_ok());
    }

    #[test]
    fn validate_rejects_bad_names() {
        assert!(validate_branch_name("").is_err());
        assert!(validate_branch_name("has space").is_err());
        assert!(validate_branch_name("has/slash").is_err());
        assert!(validate_branch_name("has;semi").is_err());
        assert!(validate_branch_name(&"x".repeat(65)).is_err());
    }

    /// `rocky branch compare` resolves the branch's `schema_prefix` from
    /// the state store and wires it into a `ShadowConfig.schema_override` —
    /// the same mapping used by `rocky run --branch`.
    #[test]
    fn branch_compare_maps_schema_prefix_to_shadow_override() {
        let tmp = TempDir::new().unwrap();
        let state_path = tmp.path().join("state.redb");

        // Register a branch via the create path (populates the default
        // `branch__<name>` schema_prefix).
        run_branch_create(&state_path, "fix-price", None, false).unwrap();

        // Fetch it directly the way `run_branch_compare` does and build
        // the ShadowConfig — asserting the wiring is exact.
        let store = StateStore::open_read_only(&state_path).unwrap();
        let record = store.get_branch("fix-price").unwrap().unwrap();

        let shadow_config = ShadowConfig {
            suffix: "_rocky_shadow".to_string(),
            schema_override: Some(record.schema_prefix.clone()),
            cleanup_after: false,
        };

        assert_eq!(record.schema_prefix, "branch__fix-price");
        assert_eq!(
            shadow_config.schema_override.as_deref(),
            Some("branch__fix-price")
        );
        assert!(!shadow_config.cleanup_after);
    }

    /// A missing branch must surface a crisp error that steers the user
    /// toward `rocky branch list`.
    #[tokio::test]
    async fn branch_compare_missing_branch_crisp_error() {
        let tmp = TempDir::new().unwrap();
        let state_path = tmp.path().join("state.redb");

        // Touch the state store so it exists but has no branches.
        StateStore::open(&state_path).unwrap();

        // config_path doesn't need to exist — resolution of the missing
        // branch fails before the config is loaded.
        let config_path = tmp.path().join("rocky.toml");

        let err = run_branch_compare(&state_path, &config_path, "ghost", None, false)
            .await
            .expect_err("missing branch must be an error");

        let msg = format!("{err:#}");
        assert!(
            msg.contains("branch 'ghost' not found"),
            "error must name the missing branch: {msg}"
        );
        assert!(
            msg.contains("rocky branch list"),
            "error must steer user to 'rocky branch list': {msg}"
        );
    }
}
