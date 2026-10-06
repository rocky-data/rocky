//! Connection configuration for the PostgreSQL and Redshift adapters.
//!
//! Built by the CLI registry from the shared `[adapter]` slots (`host`,
//! `username`, `password`, `database`, `timeout_secs`) plus a small set of
//! adapter-specific keys under `[adapter.<name>.extra]` (`port`, `sslmode`,
//! `sslrootcert`, `max_connections`, `merge_mode`). Unknown `extra` keys are
//! refused by [`PgConfig::apply_extra`] so a typo fails loudly instead of
//! silently using a default.

use std::time::Duration;

use crate::connector::PgError;

/// Which warehouse the connection speaks to. Both use the PostgreSQL wire
/// protocol; the flavor picks the SQL dialect, the default port, and the
/// catalog views used for introspection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Flavor {
    /// PostgreSQL (MERGE needs 15+; `merge_mode = "on_conflict"` for older).
    Postgres,
    /// Amazon Redshift (provisioned or Serverless).
    Redshift,
}

impl Flavor {
    /// The adapter `type` string this flavor registers under.
    #[must_use]
    pub fn adapter_type(self) -> &'static str {
        match self {
            Flavor::Postgres => "postgres",
            Flavor::Redshift => "redshift",
        }
    }

    /// The server's default listening port.
    #[must_use]
    pub fn default_port(self) -> u16 {
        match self {
            Flavor::Postgres => 5432,
            Flavor::Redshift => 5439,
        }
    }
}

/// TLS policy, spelled like libpq's `sslmode` so existing connection
/// settings carry over. `allow` and `verify-ca` are not offered: `allow`
/// tries plaintext first, and `verify-ca` checks the chain but not the host
/// name, which is the weaker half of `verify-full`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SslMode {
    /// Plaintext only.
    Disable,
    /// Try TLS, fall back to plaintext if the server refuses it. The
    /// certificate is not verified (libpq semantics). The default.
    Prefer,
    /// TLS required; the certificate is not verified (libpq semantics).
    Require,
    /// TLS required; the chain is verified against the Mozilla root set
    /// (plus `sslrootcert` when given) and the certificate must name `host`.
    VerifyFull,
}

impl SslMode {
    /// Parse a libpq `sslmode` value.
    ///
    /// # Errors
    ///
    /// Returns [`PgError::Config`] for anything outside the four supported
    /// spellings.
    pub fn parse(raw: &str) -> Result<Self, PgError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "disable" => Ok(Self::Disable),
            "prefer" => Ok(Self::Prefer),
            "require" => Ok(Self::Require),
            "verify-full" | "verify_full" => Ok(Self::VerifyFull),
            other => Err(PgError::Config(format!(
                "unsupported sslmode '{other}': expected one of disable, prefer, require, verify-full"
            ))),
        }
    }
}

/// How the Postgres dialect renders `strategy = "merge"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MergeMode {
    /// `MERGE INTO ... USING ... WHEN MATCHED ... WHEN NOT MATCHED`
    /// (PostgreSQL 15+, every Redshift version that has MERGE).
    #[default]
    Merge,
    /// `INSERT ... ON CONFLICT (keys) DO UPDATE` for PostgreSQL 9.5–14.
    /// Requires a unique index or constraint on exactly the `unique_key`
    /// columns; Postgres rejects the statement otherwise. Not available on
    /// Redshift, which has no `ON CONFLICT`.
    OnConflict,
}

impl MergeMode {
    /// Parse a `merge_mode` value.
    ///
    /// # Errors
    ///
    /// Returns [`PgError::Config`] for anything but `merge` / `on_conflict`.
    pub fn parse(raw: &str) -> Result<Self, PgError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "merge" => Ok(Self::Merge),
            "on_conflict" | "on-conflict" => Ok(Self::OnConflict),
            other => Err(PgError::Config(format!(
                "unsupported merge_mode '{other}': expected 'merge' or 'on_conflict'"
            ))),
        }
    }
}

/// Everything needed to open a connection.
#[derive(Clone)]
pub struct PgConfig {
    pub flavor: Flavor,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub sslmode: SslMode,
    /// Extra PEM root certificate(s) trusted under `verify-full`, e.g. the
    /// Amazon RDS / Redshift CA bundle.
    pub sslrootcert: Option<String>,
    /// Upper bound on concurrently open connections.
    pub max_connections: usize,
    /// Applied as `statement_timeout` on every session and as the connect
    /// timeout.
    pub timeout: Duration,
    pub merge_mode: MergeMode,
}

impl std::fmt::Debug for PgConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgConfig")
            .field("flavor", &self.flavor)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("sslmode", &self.sslmode)
            .field("sslrootcert", &self.sslrootcert)
            .field("max_connections", &self.max_connections)
            .field("timeout", &self.timeout)
            .field("merge_mode", &self.merge_mode)
            .finish()
    }
}

/// Default connection-pool size.
pub const DEFAULT_MAX_CONNECTIONS: usize = 8;

/// The `[adapter.<name>.extra]` keys this adapter reads.
pub const EXTRA_KEYS: &[&str] = &[
    "port",
    "sslmode",
    "sslrootcert",
    "max_connections",
    "merge_mode",
];

impl PgConfig {
    /// Build a config from the shared adapter slots.
    ///
    /// `host` may carry a port (`db.example.com:5439`); an explicit `port`
    /// in `extra` wins over it.
    ///
    /// # Errors
    ///
    /// Returns [`PgError::Config`] when `host`, `database` or `username` is
    /// missing or the host's port does not parse.
    pub fn new(
        flavor: Flavor,
        host: Option<&str>,
        database: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, PgError> {
        let kind = flavor.adapter_type();
        let raw_host = host
            .filter(|h| !h.trim().is_empty())
            .ok_or_else(|| PgError::Config(format!("host is required for {kind}")))?;
        let (host, port) = split_host_port(raw_host, flavor.default_port())?;
        let database = database
            .filter(|d| !d.trim().is_empty())
            .ok_or_else(|| PgError::Config(format!("database is required for {kind}")))?;
        let user = user
            .filter(|u| !u.trim().is_empty())
            .ok_or_else(|| PgError::Config(format!("username is required for {kind}")))?;
        Ok(Self {
            flavor,
            host,
            port,
            database: database.to_string(),
            user: user.to_string(),
            password: password.map(str::to_string),
            sslmode: SslMode::Prefer,
            sslrootcert: None,
            max_connections: DEFAULT_MAX_CONNECTIONS,
            timeout,
            merge_mode: MergeMode::Merge,
        })
    }

    /// Apply `[adapter.<name>.extra]` keys.
    ///
    /// # Errors
    ///
    /// Returns [`PgError::Config`] for an unknown key, a value of the wrong
    /// shape, or `merge_mode = "on_conflict"` on Redshift.
    pub fn apply_extra(
        mut self,
        extra: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, PgError> {
        for (key, value) in extra {
            match key.as_str() {
                "port" => {
                    self.port = value_as_u64(key, value)?
                        .try_into()
                        .map_err(|_| PgError::Config("extra.port is out of range".into()))?;
                }
                "sslmode" => self.sslmode = SslMode::parse(value_as_str(key, value)?)?,
                "sslrootcert" => self.sslrootcert = Some(value_as_str(key, value)?.to_string()),
                "max_connections" => {
                    let n = value_as_u64(key, value)?;
                    if n == 0 || n > 256 {
                        return Err(PgError::Config(
                            "extra.max_connections must be between 1 and 256".into(),
                        ));
                    }
                    self.max_connections = n as usize;
                }
                "merge_mode" => self.merge_mode = MergeMode::parse(value_as_str(key, value)?)?,
                other => {
                    return Err(PgError::Config(format!(
                        "unknown extra key '{other}' for {}; supported keys: {}",
                        self.flavor.adapter_type(),
                        EXTRA_KEYS.join(", ")
                    )));
                }
            }
        }
        if self.flavor == Flavor::Redshift && self.merge_mode == MergeMode::OnConflict {
            return Err(PgError::Config(
                "merge_mode = \"on_conflict\" is not available on redshift: Redshift has no \
                 INSERT ... ON CONFLICT; use the default merge_mode = \"merge\""
                    .into(),
            ));
        }
        Ok(self)
    }
}

fn value_as_str<'a>(key: &str, value: &'a serde_json::Value) -> Result<&'a str, PgError> {
    value
        .as_str()
        .ok_or_else(|| PgError::Config(format!("extra.{key} must be a string")))
}

fn value_as_u64(key: &str, value: &serde_json::Value) -> Result<u64, PgError> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    // Env-var substitution produces strings (`port = "${PGPORT:-5432}"`).
    value
        .as_str()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| PgError::Config(format!("extra.{key} must be a non-negative integer")))
}

/// Split `host[:port]`. A scheme prefix (`postgres://`) is refused — this
/// slot names a host, and a URL would carry credentials Rocky must not log.
fn split_host_port(raw: &str, default_port: u16) -> Result<(String, u16), PgError> {
    let raw = raw.trim();
    if raw.contains("://") || raw.contains('@') || raw.contains('/') {
        return Err(PgError::Config(
            "host must be a bare host name or host:port, not a connection URL".into(),
        ));
    }
    // A bracketed IPv6 literal: `[::1]:5432` or `[::1]`.
    if let Some(rest) = raw.strip_prefix('[') {
        let (addr, tail) = rest
            .split_once(']')
            .ok_or_else(|| PgError::Config("unterminated IPv6 literal in host".into()))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => parse_port(p)?,
            None if tail.is_empty() => default_port,
            None => return Err(PgError::Config(format!("invalid host '{raw}'"))),
        };
        return Ok((addr.to_string(), port));
    }
    match raw.rsplit_once(':') {
        // More than one colon without brackets: a bare IPv6 address.
        Some((h, _)) if h.contains(':') => Ok((raw.to_string(), default_port)),
        Some((h, p)) => Ok((h.to_string(), parse_port(p)?)),
        None => Ok((raw.to_string(), default_port)),
    }
}

fn parse_port(p: &str) -> Result<u16, PgError> {
    p.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| PgError::Config(format!("invalid port '{p}' in host")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(flavor: Flavor, host: &str) -> Result<PgConfig, PgError> {
        PgConfig::new(
            flavor,
            Some(host),
            Some("analytics"),
            Some("rocky"),
            Some("pw"),
            Duration::from_secs(30),
        )
    }

    #[test]
    fn default_ports_follow_flavor() {
        assert_eq!(base(Flavor::Postgres, "db").unwrap().port, 5432);
        assert_eq!(base(Flavor::Redshift, "rs").unwrap().port, 5439);
    }

    #[test]
    fn host_may_carry_a_port() {
        let cfg = base(Flavor::Postgres, "db.example.com:6543").unwrap();
        assert_eq!(cfg.host, "db.example.com");
        assert_eq!(cfg.port, 6543);
        let cfg = base(Flavor::Postgres, "[::1]:7000").unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port), ("::1", 7000));
        let cfg = base(Flavor::Postgres, "::1").unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port), ("::1", 5432));
    }

    #[test]
    fn urls_and_bad_ports_are_refused() {
        assert!(base(Flavor::Postgres, "postgres://u:p@db/x").is_err());
        assert!(base(Flavor::Postgres, "db:notaport").is_err());
        assert!(base(Flavor::Postgres, "db:0").is_err());
    }

    #[test]
    fn missing_required_fields_are_refused() {
        let t = Duration::from_secs(1);
        assert!(PgConfig::new(Flavor::Postgres, None, Some("d"), Some("u"), None, t).is_err());
        assert!(PgConfig::new(Flavor::Postgres, Some("h"), None, Some("u"), None, t).is_err());
        assert!(PgConfig::new(Flavor::Postgres, Some("h"), Some("d"), Some(" "), None, t).is_err());
    }

    #[test]
    fn extra_keys_apply_and_typos_fail() {
        let mut extra = std::collections::BTreeMap::new();
        extra.insert("port".into(), serde_json::json!("6000"));
        extra.insert("sslmode".into(), serde_json::json!("verify-full"));
        extra.insert("max_connections".into(), serde_json::json!(3));
        extra.insert("merge_mode".into(), serde_json::json!("on_conflict"));
        let cfg = base(Flavor::Postgres, "db")
            .unwrap()
            .apply_extra(&extra)
            .unwrap();
        assert_eq!(cfg.port, 6000);
        assert_eq!(cfg.sslmode, SslMode::VerifyFull);
        assert_eq!(cfg.max_connections, 3);
        assert_eq!(cfg.merge_mode, MergeMode::OnConflict);

        let mut typo = std::collections::BTreeMap::new();
        typo.insert("ssl_mode".into(), serde_json::json!("require"));
        let err = base(Flavor::Postgres, "db")
            .unwrap()
            .apply_extra(&typo)
            .unwrap_err();
        assert!(
            err.to_string().contains("unknown extra key 'ssl_mode'"),
            "{err}"
        );
    }

    #[test]
    fn on_conflict_is_refused_on_redshift() {
        let mut extra = std::collections::BTreeMap::new();
        extra.insert("merge_mode".into(), serde_json::json!("on_conflict"));
        let err = base(Flavor::Redshift, "rs")
            .unwrap()
            .apply_extra(&extra)
            .unwrap_err();
        assert!(
            err.to_string().contains("not available on redshift"),
            "{err}"
        );
    }

    #[test]
    fn debug_redacts_the_password() {
        let cfg = base(Flavor::Postgres, "db").unwrap();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("\"pw\""), "{shown}");
        assert!(shown.contains("***"));
    }
}
