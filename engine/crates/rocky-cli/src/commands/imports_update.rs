//! `rocky imports update` — advance vendored import baselines to the current
//! snapshot (the explicit "I reviewed and accept the producer's current
//! state" gesture).
//!
//! A consumer project vendors a producer's published `ProjectIr` snapshot and
//! declares it via `[imports.<name>]` (see [`rocky_core::config::ImportEntry`]).
//! The `baseline` file is the "before" image the cross-team-contract checks
//! diff the current `snapshot` against (E030/E031/E032/W030/W031). Nothing
//! advances that baseline automatically — once a producer change is reviewed
//! and accepted, the consumer runs this command to record the current snapshot
//! as the new baseline so the diagnostics clear.
//!
//! Recompute-only: this never fetches from git. The user updates the vendored
//! snapshot (e.g. `git submodule update --remote`) and then runs this. The
//! `--check` flag is a read-only CI guard that fails when a baseline is behind
//! or a pin is stale, writing nothing.

use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::config::{ImportEntry, load_rocky_config};
use rocky_core::imports::{PinStatus, load_snapshot, verify_pin};

/// What advancing an import's baseline did (or would do).
#[derive(Debug, PartialEq, Eq)]
enum BaselineState {
    /// No `baseline` is configured for this import.
    NotConfigured,
    /// The baseline already matches the snapshot — nothing to do.
    AlreadyCurrent,
    /// The baseline was behind and was advanced (apply mode).
    Advanced,
    /// The baseline is behind and would advance (`--check` mode).
    WouldAdvance,
}

/// Outcome of processing one `[imports.<name>]` block.
struct ImportUpdate {
    baseline: BaselineState,
    /// `Some((configured_pin, actual_hash))` when a concrete pin is stale.
    pin_stale: Option<(String, String)>,
}

impl ImportUpdate {
    /// Whether this import is out of date (baseline behind or pin stale).
    fn out_of_date(&self) -> bool {
        matches!(
            self.baseline,
            BaselineState::Advanced | BaselineState::WouldAdvance
        ) || self.pin_stale.is_some()
    }
}

/// Process one import: advance its baseline (unless `check`) and detect a
/// stale pin. Pure of stdout so it is unit-testable.
fn update_one_import(
    name: &str,
    entry: &ImportEntry,
    dir: &Path,
    check: bool,
) -> Result<ImportUpdate> {
    let current = load_snapshot(dir, &entry.snapshot)
        .with_context(|| format!("import '{name}': failed to load snapshot"))?;

    let baseline = match entry.baseline.as_deref() {
        None => BaselineState::NotConfigured,
        Some(baseline_file) => {
            let snapshot_path = dir.join(&entry.snapshot);
            let baseline_path = dir.join(baseline_file);
            let differs = match std::fs::read(&baseline_path) {
                Ok(existing) => std::fs::read(&snapshot_path)
                    .map(|snap| snap != existing)
                    .unwrap_or(true),
                // No baseline on disk yet — establishing one counts as an advance.
                Err(_) => true,
            };
            if !differs {
                BaselineState::AlreadyCurrent
            } else if check {
                BaselineState::WouldAdvance
            } else {
                std::fs::copy(&snapshot_path, &baseline_path).with_context(|| {
                    format!(
                        "import '{name}': failed to advance baseline {}",
                        baseline_path.display()
                    )
                })?;
                BaselineState::Advanced
            }
        }
    };

    let pin_stale = match verify_pin(&current, entry.pin.as_deref()) {
        PinStatus::Mismatch { expected, actual } => Some((expected, actual)),
        PinStatus::Trusted | PinStatus::Clean => None,
    };

    Ok(ImportUpdate {
        baseline,
        pin_stale,
    })
}

/// Execute `rocky imports update`.
///
/// For each `[imports.<name>]` block: advances the configured `baseline` file
/// to the current `snapshot` (a file copy) and reports whether a configured
/// concrete `pin` has gone stale. With `check = true` nothing is written and
/// the command fails if anything is out of date.
///
/// # Errors
///
/// Returns an error if the config cannot be loaded, a vendored snapshot cannot
/// be read, a baseline cannot be written, or (in `--check` mode) any import is
/// out of date.
pub fn run_imports_update(config_path: &Path, check: bool) -> Result<()> {
    let config = load_rocky_config(config_path)
        .with_context(|| format!("failed to load {}", config_path.display()))?;

    if config.imports.is_empty() {
        println!("no [imports.<name>] blocks configured; nothing to update");
        return Ok(());
    }

    let config_dir = match config_path.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    };

    let mut out_of_date = false;
    let mut advanced = 0usize;

    for (name, entry) in &config.imports {
        let dir = config_dir.join(&entry.path);
        let result = update_one_import(name, entry, &dir, check)?;
        out_of_date |= result.out_of_date();

        match result.baseline {
            BaselineState::Advanced => {
                advanced += 1;
                println!("import '{name}': advanced baseline to the current snapshot");
            }
            BaselineState::WouldAdvance => {
                println!("import '{name}': baseline is BEHIND the snapshot (would advance)");
            }
            BaselineState::AlreadyCurrent => println!("import '{name}': baseline already current"),
            BaselineState::NotConfigured => {}
        }
        if let Some((expected, actual)) = result.pin_stale {
            println!(
                "import '{name}': pin {expected} is STALE (snapshot recipe hash is {actual}); \
                 set pin = \"{actual}\" to re-pin, or pin = \"*\" to trust the vendored snapshot"
            );
        }
    }

    if check {
        if out_of_date {
            anyhow::bail!(
                "imports are out of date — run `rocky imports update` to advance baselines, then re-pin if needed"
            );
        }
        println!("all imports are up to date");
        return Ok(());
    }

    println!("imports update complete: {advanced} baseline(s) advanced");
    if advanced > 0 {
        println!(
            "note: advancing a baseline records the producer's current schema as accepted; \
             run `rocky compile` first to see what a baseline advance would silence"
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::imports::write_snapshot;
    use rocky_ir::{
        GovernanceConfig, MaterializationStrategy, ModelIr, ProjectIr, RockyType, TargetRef,
        TypedColumn,
    };

    fn snapshot_with(columns: &[&str]) -> ProjectIr {
        let mut model = ModelIr::transformation(
            TargetRef {
                catalog: "shop".to_string(),
                schema: "core".to_string(),
                table: "orders".to_string(),
            },
            MaterializationStrategy::FullRefresh,
            Vec::new(),
            "SELECT * FROM raw.orders".to_string(),
            GovernanceConfig {
                permissions_file: None,
                auto_create_catalogs: false,
                auto_create_schemas: false,
            },
            None,
            None,
        );
        model.typed_columns = columns
            .iter()
            .map(|c| TypedColumn {
                name: (*c).to_string(),
                data_type: RockyType::Int64,
                nullable: false,
            })
            .collect();
        ProjectIr {
            models: vec![model],
            dag: Vec::new(),
            lineage_edges: Vec::new(),
        }
    }

    fn entry(baseline: Option<&str>, pin: Option<&str>) -> ImportEntry {
        ImportEntry {
            path: ".".to_string(),
            snapshot: "current.json".to_string(),
            baseline: baseline.map(str::to_string),
            pin: pin.map(str::to_string),
        }
    }

    #[test]
    fn advances_a_behind_baseline_and_then_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        // current snapshot has an extra column vs the baseline.
        write_snapshot(
            &snapshot_with(&["id", "amount"]),
            &dir.path().join("current.json"),
        )
        .unwrap();
        write_snapshot(&snapshot_with(&["id"]), &dir.path().join("baseline.json")).unwrap();
        let e = entry(Some("baseline.json"), None);

        let r = update_one_import("orders", &e, dir.path(), false).unwrap();
        assert_eq!(r.baseline, BaselineState::Advanced);
        // The baseline file now equals the snapshot byte-for-byte.
        assert_eq!(
            std::fs::read(dir.path().join("baseline.json")).unwrap(),
            std::fs::read(dir.path().join("current.json")).unwrap()
        );
        // Re-running is a no-op.
        let r2 = update_one_import("orders", &e, dir.path(), false).unwrap();
        assert_eq!(r2.baseline, BaselineState::AlreadyCurrent);
        assert!(!r2.out_of_date());
    }

    #[test]
    fn check_mode_reports_would_advance_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        write_snapshot(
            &snapshot_with(&["id", "amount"]),
            &dir.path().join("current.json"),
        )
        .unwrap();
        write_snapshot(&snapshot_with(&["id"]), &dir.path().join("baseline.json")).unwrap();
        let before = std::fs::read(dir.path().join("baseline.json")).unwrap();
        let e = entry(Some("baseline.json"), None);

        let r = update_one_import("orders", &e, dir.path(), true).unwrap();
        assert_eq!(r.baseline, BaselineState::WouldAdvance);
        assert!(r.out_of_date());
        // --check never writes.
        assert_eq!(
            std::fs::read(dir.path().join("baseline.json")).unwrap(),
            before
        );
    }

    #[test]
    fn detects_a_stale_concrete_pin() {
        let dir = tempfile::tempdir().unwrap();
        write_snapshot(&snapshot_with(&["id"]), &dir.path().join("current.json")).unwrap();
        let e = entry(None, Some("deadbeef"));

        let r = update_one_import("orders", &e, dir.path(), false).unwrap();
        assert_eq!(r.baseline, BaselineState::NotConfigured);
        assert!(r.out_of_date());
        let (expected, _actual) = r.pin_stale.expect("stale pin detected");
        assert_eq!(expected, "deadbeef");
    }

    #[test]
    fn star_pin_is_never_stale() {
        let dir = tempfile::tempdir().unwrap();
        write_snapshot(&snapshot_with(&["id"]), &dir.path().join("current.json")).unwrap();
        let e = entry(None, Some("*"));
        let r = update_one_import("orders", &e, dir.path(), false).unwrap();
        assert!(r.pin_stale.is_none());
        assert!(!r.out_of_date());
    }
}
