//! The exact seam shape the remote-state callers use: `download_state(...)?;`
//! in statement position. The `?` returns a `#[must_use] StateAuthority`
//! that this statement discards — `deny(unused_must_use)` (CI's
//! `-D warnings`) must refuse to compile it, forcing every seam to bind the
//! authority explicitly.
#![deny(unused_must_use)]

use std::path::Path;

use rocky_core::config::StateConfig;
use rocky_core::state_sync::StateSyncError;

#[allow(dead_code)]
async fn seam(cfg: &StateConfig, state_path: &Path) -> Result<(), StateSyncError> {
    rocky_core::state_sync::download_state(cfg, state_path).await?;
    Ok(())
}

fn main() {}
