//! `rocky serve` — HTTP API server exposing the compiler's semantic graph.

use std::path::Path;

use anyhow::Result;

/// Execute `rocky serve`.
pub async fn run_serve(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    config_path: Option<&Path>,
    port: u16,
    watch: bool,
) -> Result<()> {
    let state = rocky_server::state::ServerState::new(
        models_dir.to_path_buf(),
        contracts_dir.map(|p| p.to_path_buf()),
        config_path.map(|p| p.to_path_buf()),
    );

    // Start filesystem watcher if requested
    let _watcher = if watch {
        Some(rocky_server::watch::start_watcher(
            state.clone(),
            models_dir,
        )?)
    } else {
        None
    };

    // Wait for initial compilation
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    rocky_server::api::serve(state, port).await
}
