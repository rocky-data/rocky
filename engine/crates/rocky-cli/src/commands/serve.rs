//! `rocky serve` — HTTP API server exposing the compiler's semantic graph.
//!
//! # Security defaults
//!
//! The server binds `127.0.0.1:8080` by default. Binding a non-loopback
//! host (e.g. `--host 0.0.0.0`) requires a Bearer token — otherwise the
//! server refuses to start so model SQL, file paths, and run history
//! don't leak to the LAN. Token sources, in priority order:
//!
//! 1. `--token <secret>` flag
//! 2. `ROCKY_SERVE_TOKEN` env var
//!
//! Cross-origin clients must be enumerated via `--allowed-origin`. The
//! default allowlist is empty (same-origin only); the dashboard at `/`
//! is server-rendered HTML and doesn't need cross-origin XHR.

use std::path::Path;

use anyhow::Result;

/// Execute `rocky serve`.
#[allow(clippy::too_many_arguments)]
pub async fn run_serve(
    models_dir: &Path,
    contracts_dir: Option<&Path>,
    config_path: Option<&Path>,
    host: String,
    port: u16,
    watch: bool,
    auth_token: Option<String>,
    allowed_origins: Vec<String>,
) -> Result<()> {
    // Token resolution: --token takes precedence over the env var so
    // CI / scripts can override an inherited environment.
    let token = auth_token.or_else(|| std::env::var("ROCKY_SERVE_TOKEN").ok());

    let state = rocky_server::state::ServerState::with_auth(
        models_dir.to_path_buf(),
        contracts_dir.map(std::path::Path::to_path_buf),
        config_path.map(std::path::Path::to_path_buf),
        token,
        allowed_origins,
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

    rocky_server::api::serve(state, rocky_server::api::ServeConfig { host, port }).await
}
