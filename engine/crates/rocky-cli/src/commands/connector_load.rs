//! `rocky load --connector <name>`: run a child connector and load what it
//! writes.

use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, bail};
use tracing::info;

use rocky_core::config::LoadedConfig;
use rocky_core::connector::{
    ConnectorConfig, ConnectorLimitsConfig, ConnectorManifest, EffectiveLimits, local_output_dir,
};

use crate::connector::runner::{ChildSpec, DEFAULT_OUTPUT_POLL, run_with_retries};
use crate::output::{LoadFileOutput, LoadOutput, print_json};

const VERSION: &str = env!("CARGO_PKG_VERSION");

/// A connector with its config and manifest read and checked, before any
/// child runs.
struct Prepared<'a> {
    config: &'a ConnectorConfig,
    manifest: ConnectorManifest,
    argv: Vec<std::ffi::OsString>,
    limits: EffectiveLimits,
    landing_dir: PathBuf,
}

fn prepare<'a>(config_path: &Path, loaded: &'a LoadedConfig, name: &str) -> Result<Prepared<'a>> {
    let Some(config) = loaded.config.connectors.get(name) else {
        let known: Vec<&str> = loaded
            .config
            .connectors
            .keys()
            .map(String::as_str)
            .collect();
        if known.is_empty() {
            bail!("no [connector.{name}] in the config; the config declares no connectors");
        }
        bail!(
            "no [connector.{name}] in the config; declared connectors: {}",
            known.join(", ")
        );
    };
    let config_dir = match config_path.parent() {
        Some(dir) if !dir.as_os_str().is_empty() => dir,
        _ => Path::new("."),
    };
    let manifest_path = config_dir.join(&config.manifest);
    let manifest_path = manifest_path.canonicalize().with_context(|| {
        format!(
            "cannot find the manifest for connector `{name}` at {}",
            manifest_path.display()
        )
    })?;
    let manifest = ConnectorManifest::load(&manifest_path)?;
    let manifest_dir = manifest_path.parent().unwrap_or(Path::new("."));
    let argv = manifest.resolved_command(manifest_dir);
    let limits = ConnectorLimitsConfig::effective(&manifest.limits, &config.limits);
    let landing_dir = local_output_dir(&config.output_uri, config_dir)
        .map_err(|e| anyhow::anyhow!("connector `{name}`: {e}"))?;
    Ok(Prepared {
        config,
        manifest,
        argv,
        limits,
        landing_dir,
    })
}

/// Read each declared secret from Rocky's own environment and register its
/// value for redaction. An unset variable is an error: the connector would
/// otherwise run without a credential it was promised.
fn resolve_secrets(names: &[String]) -> Result<Vec<(String, String)>> {
    let mut secrets = Vec::with_capacity(names.len());
    for name in names {
        let value = match std::env::var(name) {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => bail!(
                "the connector declares the secret `{name}`, but that environment variable is not set"
            ),
            Err(std::env::VarError::NotUnicode(_)) => bail!(
                "the connector declares the secret `{name}`, but its value is not valid UTF-8"
            ),
        };
        rocky_core::secret_registry::register_substitution(name, &value);
        secrets.push((name.clone(), value));
    }
    Ok(secrets)
}

/// Execute `rocky load --connector <name>`.
pub async fn run_connector_load(
    config_path: &Path,
    loaded: &LoadedConfig,
    _state_path: &Path,
    name: &str,
    _durability: rocky_core::state_sync::FinalizeDurability,
    json: bool,
) -> Result<()> {
    let start = Instant::now();
    let prepared = prepare(config_path, loaded, name)?;
    let secrets = resolve_secrets(&prepared.config.secrets)?;
    let run_id = uuid::Uuid::new_v4().simple().to_string();

    info!(
        connector = name,
        run_id = %run_id,
        egress_hosts = ?prepared.manifest.egress_hosts,
        "running the child connector (egress hosts are declared, not enforced)"
    );

    let spec = ChildSpec {
        name,
        argv: &prepared.argv,
        secrets: &secrets,
        run_id: &run_id,
        limits: prepared.limits,
        state: None,
        state_schema_version: prepared.manifest.state_schema_version,
        landing_dir: &prepared.landing_dir,
        retries: prepared.config.retries,
        output_poll: DEFAULT_OUTPUT_POLL,
    };
    let ran = run_with_retries(&spec).await?;

    let target = &prepared.config.target;
    let target_display = format!("{}.{}.{}", target.catalog, target.schema, target.table);
    let files: Vec<LoadFileOutput> = ran
        .batch
        .files
        .iter()
        .map(|f| LoadFileOutput {
            file: f.path.display().to_string(),
            target: target_display.clone(),
            rows_loaded: f.rows,
            bytes_read: f.bytes,
            duration_ms: 0,
            error: None,
            contract: None,
        })
        .collect();
    let duration_ms = start.elapsed().as_millis() as u64;
    if json {
        print_json(&LoadOutput {
            version: VERSION.into(),
            command: "load".into(),
            source_dir: ran.batch.scratch.display().to_string(),
            format: "parquet".into(),
            files_loaded: files.len(),
            files_failed: 0,
            total_rows: ran.batch.total_rows(),
            total_bytes: ran.batch.total_bytes(),
            files,
            duration_ms,
        })?;
    } else {
        println!(
            "Connector `{name}` wrote {} file(s), {} rows, {} bytes ({} attempt(s), {duration_ms} ms)",
            files.len(),
            ran.batch.total_rows(),
            ran.batch.total_bytes(),
            ran.attempts,
        );
    }
    Ok(())
}
