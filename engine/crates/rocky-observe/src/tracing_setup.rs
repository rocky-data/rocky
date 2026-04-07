use tracing_subscriber::EnvFilter;
use tracing_subscriber::fmt;
use tracing_subscriber::prelude::*;

/// Initializes structured logging.
///
/// - `json=true`: JSON output (for machine consumption, dagster-rocky)
/// - `json=false`: human-readable output (for local dev)
///
/// Log level controlled by `RUST_LOG` env var (default: `info`).
pub fn init_tracing(json: bool) {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Always write logs to stderr so stdout is reserved for structured JSON output
    // (discover, plan, run commands write their result JSON to stdout).
    if json {
        let fmt_layer = fmt::layer()
            .json()
            .with_target(true)
            .with_thread_ids(false)
            .with_writer(std::io::stderr);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    } else {
        let fmt_layer = fmt::layer()
            .with_target(false)
            .with_thread_ids(false)
            .compact()
            .with_writer(std::io::stderr);

        tracing_subscriber::registry()
            .with(filter)
            .with(fmt_layer)
            .init();
    }
}
