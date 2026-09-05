pub mod api;
pub mod commands;
pub mod deprecation;
pub mod error_reporter;
pub mod models_loader;
pub mod otel_guard;
pub mod output;
pub mod pipes;
pub mod plan_store;
pub mod registry;
pub(crate) mod schema_cache_writer;
pub(crate) mod scope;
pub(crate) mod source_schemas;
pub mod ui;

/// Test-only warehouse doubles. Compiled only under `cfg(test)`, so a released
/// binary contains none of it — see the module docs and the matching
/// `cfg(test)` arm in [`registry::AdapterRegistry::from_config`].
#[cfg(test)]
pub(crate) mod testing;
