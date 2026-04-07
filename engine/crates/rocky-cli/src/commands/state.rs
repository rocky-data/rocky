use std::path::Path;

use anyhow::{Context, Result};

use rocky_core::state::StateStore;

use crate::output::*;

/// Execute `rocky state show`.
pub fn state_show(state_path: &Path, output_json: bool) -> Result<()> {
    let store = StateStore::open(state_path).context(format!(
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
