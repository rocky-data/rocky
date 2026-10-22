//! Child connectors: config, manifest, limits and the stored state envelope.
//!
//! A child connector is a program the user supplies. `rocky load --connector
//! <name>` runs it once, as a child process, and loads the Parquet files it
//! writes into one table. This module holds the data types and the pure
//! rules. Running the child and loading the files live in `rocky-cli`.
//!
//! Two files describe a connector:
//!
//! - `[connector.<name>]` in `rocky.toml`: where it writes, which secrets it
//!   may see, where the rows go ([`ConnectorConfig`]).
//! - `connector.toml`, next to the connector's code: what it is and how to
//!   start it ([`ConnectorManifest`]).

use std::path::{Path, PathBuf};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::contracts::ContractConfig;

/// The only protocol version this build speaks.
pub const PROTOCOL_VERSION: u32 = 0;

/// Name of the result file the connector writes into its output directory.
pub const RESULT_FILE_NAME: &str = "_rocky_result.json";

/// Default retries after a failed attempt.
pub const DEFAULT_RETRIES: u32 = 1;

/// Largest `retries` value the config accepts.
pub const MAX_RETRIES: u32 = 10;

/// Default wall-clock limit for one attempt, in seconds.
pub const DEFAULT_WALL_TIME_SECS: u64 = 3600;

/// Default cap on the bytes one attempt may write: 5 GiB.
pub const DEFAULT_OUTPUT_BYTES: u64 = 5 * 1024 * 1024 * 1024;

/// Largest serialized connector state Rocky accepts: 1 MiB.
pub const MAX_STATE_BYTES: usize = 1024 * 1024;

/// Environment variable names Rocky sets for the child itself. A declared
/// secret may not reuse one of them.
pub const RESERVED_ENV_NAMES: &[&str] = &["PATH", "LANG", "HOME", "TMPDIR", "ROCKY_RUN_ID"];

fn default_retries() -> u32 {
    DEFAULT_RETRIES
}

fn default_adapter_name() -> String {
    "default".to_string()
}

fn is_default_retries(retries: &u32) -> bool {
    *retries == DEFAULT_RETRIES
}

/// `[connector.<name>]`: one child connector.
///
/// ```toml
/// [connector.stripe_charges]
/// manifest   = "connectors/stripe/connector.toml"
/// output_uri = "file://./.rocky/landing/stripe"
/// secrets    = ["STRIPE_API_KEY"]
/// target     = { catalog = "raw", schema = "stripe", table = "charges" }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectorConfig {
    /// Path to the connector's `connector.toml`, relative to the directory
    /// that holds `rocky.toml`.
    pub manifest: String,

    /// Where the connector writes. Only `file://` is supported. A relative
    /// path is relative to the directory that holds `rocky.toml`. Each
    /// attempt gets its own new directory inside it.
    pub output_uri: String,

    /// Names of environment variables to hand to the connector. Rocky reads
    /// the values from its own environment. The child sees no other
    /// variable except `PATH`, `LANG`, `HOME`, `TMPDIR` and `ROCKY_RUN_ID`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub secrets: Vec<String>,

    /// The table the rows land in.
    pub target: ConnectorTargetConfig,

    /// How many times to run the connector again after a failed attempt.
    /// Default 1. At most 10.
    #[serde(
        default = "default_retries",
        skip_serializing_if = "is_default_retries"
    )]
    pub retries: u32,

    /// Optional data contract, checked on the staging table before the
    /// rows reach the target. The same type load pipelines use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract: Option<ContractConfig>,

    /// Limits for one attempt. The smaller of this and the manifest's
    /// limits wins for each key.
    #[serde(default, skip_serializing_if = "ConnectorLimitsConfig::is_unset")]
    pub limits: ConnectorLimitsConfig,
}

/// `target` of a connector: the table it fills.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectorTargetConfig {
    /// Name of the warehouse adapter (a key under `[adapter.*]`).
    #[serde(default = "default_adapter_name")]
    pub adapter: String,
    /// Target catalog.
    pub catalog: String,
    /// Target schema.
    pub schema: String,
    /// Target table.
    pub table: String,
}

/// The four limits for one attempt. Every key is optional.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectorLimitsConfig {
    /// Wall-clock seconds one attempt may run. Default 3600.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wall_time_secs: Option<u64>,
    /// CPU seconds one attempt may use. Default: no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_time_secs: Option<u64>,
    /// Address-space bytes. Default: no limit. Linux enforces it. macOS
    /// often refuses the limit, and Rocky then runs the child without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    /// Bytes one attempt may write in total. Default 5 GiB.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_bytes: Option<u64>,
}

impl ConnectorLimitsConfig {
    /// True when no key is set.
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }

    /// The limits one attempt runs under: for each key, the smaller of the
    /// manifest's and the user's value; where only one sets it, that one;
    /// else the default (or none).
    pub fn effective(manifest: &Self, user: &Self) -> EffectiveLimits {
        fn smaller(a: Option<u64>, b: Option<u64>) -> Option<u64> {
            match (a, b) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            }
        }
        EffectiveLimits {
            wall_time_secs: smaller(manifest.wall_time_secs, user.wall_time_secs)
                .unwrap_or(DEFAULT_WALL_TIME_SECS),
            cpu_time_secs: smaller(manifest.cpu_time_secs, user.cpu_time_secs),
            memory_bytes: smaller(manifest.memory_bytes, user.memory_bytes),
            output_bytes: smaller(manifest.output_bytes, user.output_bytes)
                .unwrap_or(DEFAULT_OUTPUT_BYTES),
        }
    }

    /// Names of keys set to zero. A zero limit would stop every attempt.
    fn zero_keys(&self) -> Vec<&'static str> {
        let mut keys = Vec::new();
        for (name, value) in [
            ("wall_time_secs", self.wall_time_secs),
            ("cpu_time_secs", self.cpu_time_secs),
            ("memory_bytes", self.memory_bytes),
            ("output_bytes", self.output_bytes),
        ] {
            if value == Some(0) {
                keys.push(name);
            }
        }
        keys
    }
}

/// The limits one attempt runs under, after the manifest and the user's
/// config are merged. Also the `limits` object in the request the child
/// reads on stdin.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EffectiveLimits {
    pub wall_time_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_time_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    pub output_bytes: u64,
}

/// `connector.toml`: what the connector author ships.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConnectorManifest {
    /// Must be 0.
    pub protocol_version: u32,
    /// The connector's own name.
    pub name: String,
    /// The program and its arguments. Not empty. A first element that is a
    /// relative path with a slash is relative to the manifest's directory.
    /// A first element without a slash is looked up on `PATH`. Other
    /// arguments are passed unchanged, and the child's working directory is
    /// its scratch directory.
    pub command: Vec<String>,
    /// The version of the state shape the connector reads and writes.
    pub state_schema_version: u32,
    /// Hosts the connector says it will call. Required: write `[]` for none.
    /// Rocky prints the list. It does not enforce it.
    pub egress_hosts: Vec<String>,
    /// Limits the author suggests. The user's limits can only lower them.
    #[serde(default, skip_serializing_if = "ConnectorLimitsConfig::is_unset")]
    pub limits: ConnectorLimitsConfig,
}

/// Why a manifest was refused.
#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("cannot read the connector manifest {}: {source}", .path.display())]
    Read {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("the connector manifest {} is not valid: {detail}", .path.display())]
    Parse { path: PathBuf, detail: String },
    #[error("the connector manifest {}: {detail}", .path.display())]
    Invalid { path: PathBuf, detail: String },
}

impl ConnectorManifest {
    /// Read and check a manifest.
    ///
    /// # Errors
    ///
    /// [`ManifestError`] when the file is unreadable, is not valid TOML for
    /// this shape (an unknown key, or a missing `egress_hosts`, is refused),
    /// or breaks a rule: `protocol_version` must be 0, `command` must not be
    /// empty, a limit must not be 0.
    pub fn load(path: &Path) -> Result<Self, ManifestError> {
        let raw = std::fs::read_to_string(path).map_err(|source| ManifestError::Read {
            path: path.to_path_buf(),
            source,
        })?;
        let manifest: Self = toml::from_str(&raw).map_err(|e| ManifestError::Parse {
            path: path.to_path_buf(),
            detail: e.message().trim().to_string(),
        })?;
        manifest.check().map_err(|detail| ManifestError::Invalid {
            path: path.to_path_buf(),
            detail,
        })?;
        Ok(manifest)
    }

    fn check(&self) -> Result<(), String> {
        if self.protocol_version != PROTOCOL_VERSION {
            return Err(format!(
                "protocol_version is {}, but this build of Rocky speaks only {PROTOCOL_VERSION}",
                self.protocol_version
            ));
        }
        if self.name.trim().is_empty() {
            return Err("name must not be empty".to_string());
        }
        if self.command.is_empty() || self.command[0].trim().is_empty() {
            return Err("command must name a program".to_string());
        }
        let zero = self.limits.zero_keys();
        if !zero.is_empty() {
            return Err(format!(
                "[limits] {} must be greater than 0",
                zero.join(", ")
            ));
        }
        Ok(())
    }

    /// The program and arguments to run. A first element that is a relative
    /// path with a slash is resolved against `manifest_dir`.
    pub fn resolved_command(&self, manifest_dir: &Path) -> Vec<std::ffi::OsString> {
        let mut argv: Vec<std::ffi::OsString> = Vec::with_capacity(self.command.len());
        for (index, part) in self.command.iter().enumerate() {
            let is_relative_path = index == 0
                && (part.contains('/') || (cfg!(windows) && part.contains('\\')))
                && !Path::new(part).is_absolute();
            if is_relative_path {
                argv.push(manifest_dir.join(part).into_os_string());
            } else {
                argv.push(part.into());
            }
        }
        argv
    }
}

/// The connector's saved state: a schema version and an opaque JSON value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StateEnvelope {
    pub schema_version: u32,
    pub value: serde_json::Value,
}

impl StateEnvelope {
    /// Size of the envelope's value as compact JSON, in bytes.
    pub fn value_size(&self) -> usize {
        serde_json::to_vec(&self.value).map_or(usize::MAX, |bytes| bytes.len())
    }
}

/// What the state store keeps for one connector: the saved state and the
/// number of batches committed so far.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectorStateRecord {
    /// The state the connector last returned.
    pub envelope: StateEnvelope,
    /// Batches committed. The next claim key names this number.
    pub commit_seq: u64,
}

/// The local directory behind a `file://` output URI.
///
/// `file://./x` and `file://x` are relative to `config_dir`. `file:///x` is
/// the absolute path `/x`.
///
/// # Errors
///
/// A message in plain words when the URI is not `file://` or has no path.
pub fn local_output_dir(output_uri: &str, config_dir: &Path) -> Result<PathBuf, String> {
    let Some(rest) = output_uri.strip_prefix("file://") else {
        let scheme = output_uri.split("://").next().unwrap_or("");
        return if output_uri.contains("://") {
            Err(format!(
                "output_uri scheme `{scheme}://` is not supported yet; use `file://`"
            ))
        } else {
            Err("output_uri must start with `file://`".to_string())
        };
    };
    if rest.is_empty() {
        return Err("output_uri has no path after `file://`".to_string());
    }
    let path = Path::new(rest);
    Ok(if path.is_absolute() {
        path.to_path_buf()
    } else {
        config_dir.join(path)
    })
}

/// Problems with one `[connector.<name>]` block that need no other config.
/// Returns plain-word messages; the config validator adds the block name.
pub fn check_connector_block(
    name: &str,
    connector: &ConnectorConfig,
    config_dir: &Path,
) -> Vec<String> {
    let mut problems = Vec::new();
    if let Err(e) = rocky_sql::validation::validate_identifier(name) {
        problems.push(format!("the name is not a plain identifier: {e}"));
    }
    if connector.manifest.trim().is_empty() {
        problems.push("manifest must not be empty".to_string());
    }
    if let Err(e) = local_output_dir(&connector.output_uri, config_dir) {
        problems.push(e);
    }
    if connector.retries > MAX_RETRIES {
        problems.push(format!(
            "retries is {}, but the most allowed is {MAX_RETRIES}",
            connector.retries
        ));
    }
    let zero = connector.limits.zero_keys();
    if !zero.is_empty() {
        problems.push(format!("limits {} must be greater than 0", zero.join(", ")));
    }
    for secret in &connector.secrets {
        if !is_env_name(secret) {
            problems.push(format!(
                "secrets entry `{secret}` is not an environment variable name \
                 (letters, digits and underscores, not starting with a digit)"
            ));
        } else if RESERVED_ENV_NAMES.contains(&secret.as_str()) {
            problems.push(format!(
                "secrets entry `{secret}` is a variable Rocky sets for the connector itself"
            ));
        }
    }
    let mut seen = std::collections::HashSet::new();
    for secret in &connector.secrets {
        if !seen.insert(secret.as_str()) {
            problems.push(format!("secrets entry `{secret}` is listed more than once"));
        }
    }
    for (field, value) in [
        ("schema", &connector.target.schema),
        ("table", &connector.target.table),
    ] {
        if let Err(e) = rocky_sql::validation::validate_identifier(value) {
            problems.push(format!("target.{field}: {e}"));
        }
    }
    problems
}

fn is_env_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write_manifest(dir: &Path, body: &str) -> PathBuf {
        let path = dir.join("connector.toml");
        std::fs::write(&path, body).unwrap();
        path
    }

    const GOOD: &str = r#"
protocol_version = 0
name = "demo"
command = ["./run.sh"]
state_schema_version = 1
egress_hosts = []
"#;

    #[test]
    fn a_good_manifest_loads() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = ConnectorManifest::load(&write_manifest(dir.path(), GOOD)).unwrap();
        assert_eq!(manifest.name, "demo");
        assert!(manifest.egress_hosts.is_empty());
        assert!(manifest.limits.is_unset());
    }

    #[test]
    fn a_manifest_without_egress_hosts_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let body = GOOD.replace("egress_hosts = []\n", "");
        let err = ConnectorManifest::load(&write_manifest(dir.path(), &body)).unwrap_err();
        assert!(err.to_string().contains("egress_hosts"), "{err}");
    }

    #[test]
    fn an_unknown_manifest_key_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let body = format!("{GOOD}\nsurprise = 1\n");
        let err = ConnectorManifest::load(&write_manifest(dir.path(), &body)).unwrap_err();
        assert!(err.to_string().contains("surprise"), "{err}");
    }

    #[test]
    fn a_manifest_protocol_other_than_zero_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let body = GOOD.replace("protocol_version = 0", "protocol_version = 1");
        let err = ConnectorManifest::load(&write_manifest(dir.path(), &body)).unwrap_err();
        assert!(err.to_string().contains("protocol_version"), "{err}");
    }

    #[test]
    fn an_empty_command_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let body = GOOD.replace(r#"command = ["./run.sh"]"#, "command = []");
        let err = ConnectorManifest::load(&write_manifest(dir.path(), &body)).unwrap_err();
        assert!(err.to_string().contains("command"), "{err}");
    }

    #[test]
    fn only_a_relative_path_with_a_slash_is_resolved() {
        let manifest = ConnectorManifest {
            protocol_version: 0,
            name: "x".into(),
            command: vec!["./bin/run".into(), "main.py".into()],
            state_schema_version: 1,
            egress_hosts: vec![],
            limits: ConnectorLimitsConfig::default(),
        };
        let argv = manifest.resolved_command(Path::new("/m"));
        assert_eq!(argv[0], std::ffi::OsString::from("/m/./bin/run"));
        assert_eq!(argv[1], std::ffi::OsString::from("main.py"));

        let by_name = ConnectorManifest {
            command: vec!["python3".into(), "main.py".into()],
            ..manifest.clone()
        };
        assert_eq!(
            by_name.resolved_command(Path::new("/m"))[0],
            std::ffi::OsString::from("python3")
        );
        let absolute = ConnectorManifest {
            command: vec!["/usr/bin/env".into()],
            ..manifest
        };
        assert_eq!(
            absolute.resolved_command(Path::new("/m"))[0],
            std::ffi::OsString::from("/usr/bin/env")
        );
    }

    #[test]
    fn the_smaller_limit_wins_and_a_lone_limit_stands() {
        let manifest = ConnectorLimitsConfig {
            wall_time_secs: Some(100),
            cpu_time_secs: Some(30),
            memory_bytes: None,
            output_bytes: Some(10),
        };
        let user = ConnectorLimitsConfig {
            wall_time_secs: Some(50),
            cpu_time_secs: None,
            memory_bytes: Some(4096),
            output_bytes: Some(20),
        };
        let effective = ConnectorLimitsConfig::effective(&manifest, &user);
        assert_eq!(effective.wall_time_secs, 50);
        assert_eq!(effective.cpu_time_secs, Some(30));
        assert_eq!(effective.memory_bytes, Some(4096));
        assert_eq!(effective.output_bytes, 10);

        let defaults = ConnectorLimitsConfig::effective(
            &ConnectorLimitsConfig::default(),
            &ConnectorLimitsConfig::default(),
        );
        assert_eq!(defaults.wall_time_secs, DEFAULT_WALL_TIME_SECS);
        assert_eq!(defaults.cpu_time_secs, None);
        assert_eq!(defaults.memory_bytes, None);
        assert_eq!(defaults.output_bytes, DEFAULT_OUTPUT_BYTES);
    }

    #[test]
    fn output_uri_accepts_only_file() {
        let base = Path::new("/proj");
        assert_eq!(
            local_output_dir("file://./.rocky/landing/x", base).unwrap(),
            PathBuf::from("/proj/./.rocky/landing/x")
        );
        assert_eq!(
            local_output_dir("file:///var/landing", base).unwrap(),
            PathBuf::from("/var/landing")
        );
        for bad in ["s3://bucket/x", "gs://bucket/x"] {
            let err = local_output_dir(bad, base).unwrap_err();
            assert!(err.contains("not supported yet"), "{err}");
        }
        assert!(local_output_dir("/plain/path", base).is_err());
        assert!(local_output_dir("file://", base).is_err());
    }
}
