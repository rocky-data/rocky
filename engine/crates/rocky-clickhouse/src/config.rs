//! Connection configuration for the ClickHouse adapter.
//!
//! Built by the CLI registry from the shared `[adapter]` slots (`host`,
//! `database`, `username`, `password`, `timeout_secs`) plus a small set of
//! adapter-specific keys under `[adapter.<name>.extra]` (`port`, `secure`,
//! `ca_cert`). Unknown `extra` keys are refused by
//! [`ChConfig::apply_extra`] so a typo fails loudly instead of silently
//! using a default.

use std::time::Duration;

use crate::connector::ChError;

/// The HTTP interface's default plaintext port.
pub const DEFAULT_HTTP_PORT: u16 = 8123;

/// The HTTP interface's default TLS port.
pub const DEFAULT_HTTPS_PORT: u16 = 8443;

/// The `[adapter.<name>.extra]` keys this adapter reads.
pub const EXTRA_KEYS: &[&str] = &["port", "secure", "ca_cert"];

/// Everything needed to reach a ClickHouse server over its HTTP interface.
#[derive(Clone)]
pub struct ChConfig {
    pub host: String,
    /// `None` until resolved: the default depends on `secure`.
    port: Option<u16>,
    /// HTTPS instead of HTTP. The certificate is always verified, against
    /// the Mozilla root set plus `ca_cert` when given.
    pub secure: bool,
    /// Extra PEM root certificate(s) to trust, e.g. a self-signed server's
    /// CA. Read when the client is built.
    pub ca_cert: Option<String>,
    /// The session's default database: where an unqualified table name in
    /// model SQL resolves. Targets always render `database.table`.
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    /// Per-request timeout, also sent as the server's
    /// `max_execution_time`.
    pub timeout: Duration,
}

impl std::fmt::Debug for ChConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChConfig")
            .field("host", &self.host)
            .field("port", &self.port())
            .field("secure", &self.secure)
            .field("ca_cert", &self.ca_cert)
            .field("database", &self.database)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl ChConfig {
    /// Build a config from the shared adapter slots.
    ///
    /// `host` may carry a port (`ch.example.com:8443`); an explicit `port`
    /// in `extra` wins over it. `database` defaults to `default` and
    /// `username` to `default`, ClickHouse's own defaults.
    ///
    /// # Errors
    ///
    /// Returns [`ChError::Config`] when `host` is missing, is a URL, or its
    /// port does not parse.
    pub fn new(
        host: Option<&str>,
        database: Option<&str>,
        user: Option<&str>,
        password: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, ChError> {
        let raw_host = host
            .filter(|h| !h.trim().is_empty())
            .ok_or_else(|| ChError::Config("host is required for clickhouse".into()))?;
        let (host, port) = split_host_port(raw_host)?;
        let database = database
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .unwrap_or("default");
        rocky_sql::validation::validate_identifier(database)
            .map_err(|e| ChError::Config(format!("database: {e}")))?;
        let user = user
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .unwrap_or("default");
        Ok(Self {
            host,
            port,
            secure: false,
            ca_cert: None,
            database: database.to_string(),
            user: user.to_string(),
            password: password.map(str::to_string),
            timeout,
        })
    }

    /// Apply `[adapter.<name>.extra]` keys.
    ///
    /// # Errors
    ///
    /// Returns [`ChError::Config`] for an unknown key or a value of the
    /// wrong shape.
    pub fn apply_extra(
        mut self,
        extra: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, ChError> {
        for (key, value) in extra {
            match key.as_str() {
                "port" => {
                    let port: u16 = value_as_u64(key, value)?
                        .try_into()
                        .ok()
                        .filter(|p| *p != 0)
                        .ok_or_else(|| ChError::Config("extra.port is out of range".into()))?;
                    self.port = Some(port);
                }
                "secure" => self.secure = value_as_bool(key, value)?,
                "ca_cert" => self.ca_cert = Some(value_as_str(key, value)?.to_string()),
                other => {
                    return Err(ChError::Config(format!(
                        "unknown extra key '{other}' for clickhouse; supported keys: {}",
                        EXTRA_KEYS.join(", ")
                    )));
                }
            }
        }
        if self.ca_cert.is_some() && !self.secure {
            return Err(ChError::Config(
                "extra.ca_cert needs extra.secure = true: a CA certificate only applies to an \
                 HTTPS connection"
                    .into(),
            ));
        }
        Ok(self)
    }

    /// The resolved port: the configured one, else 8443 under `secure`,
    /// else 8123.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(if self.secure {
            DEFAULT_HTTPS_PORT
        } else {
            DEFAULT_HTTP_PORT
        })
    }

    /// The HTTP interface's base URL (`http://host:8123/`). Carries no
    /// credentials: they travel in headers.
    #[must_use]
    pub fn base_url(&self) -> String {
        let scheme = if self.secure { "https" } else { "http" };
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("{scheme}://{host}:{}/", self.port())
    }
}

fn value_as_str<'a>(key: &str, value: &'a serde_json::Value) -> Result<&'a str, ChError> {
    value
        .as_str()
        .ok_or_else(|| ChError::Config(format!("extra.{key} must be a string")))
}

fn value_as_bool(key: &str, value: &serde_json::Value) -> Result<bool, ChError> {
    match value {
        serde_json::Value::Bool(b) => Ok(*b),
        // Env-var substitution produces strings (`secure = "${CH_TLS:-true}"`).
        serde_json::Value::String(s) if s.trim() == "true" => Ok(true),
        serde_json::Value::String(s) if s.trim() == "false" => Ok(false),
        _ => Err(ChError::Config(format!(
            "extra.{key} must be true or false"
        ))),
    }
}

fn value_as_u64(key: &str, value: &serde_json::Value) -> Result<u64, ChError> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    value
        .as_str()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| ChError::Config(format!("extra.{key} must be a non-negative integer")))
}

/// Split `host[:port]`. A URL is refused — this slot names a host, and a URL
/// could carry credentials Rocky must not log. TLS is `extra.secure`.
fn split_host_port(raw: &str) -> Result<(String, Option<u16>), ChError> {
    let raw = raw.trim();
    if raw.contains("://") || raw.contains('@') || raw.contains('/') {
        return Err(ChError::Config(
            "host must be a bare host name or host:port, not a URL; set extra.secure = true \
             for HTTPS"
                .into(),
        ));
    }
    // A bracketed IPv6 literal: `[::1]:8123` or `[::1]`.
    if let Some(rest) = raw.strip_prefix('[') {
        let (addr, tail) = rest
            .split_once(']')
            .ok_or_else(|| ChError::Config("unterminated IPv6 literal in host".into()))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => Some(parse_port(p)?),
            None if tail.is_empty() => None,
            None => return Err(ChError::Config(format!("invalid host '{raw}'"))),
        };
        return Ok((addr.to_string(), port));
    }
    match raw.rsplit_once(':') {
        // More than one colon without brackets: a bare IPv6 address.
        Some((h, _)) if h.contains(':') => Ok((raw.to_string(), None)),
        Some((h, p)) => Ok((h.to_string(), Some(parse_port(p)?))),
        None => Ok((raw.to_string(), None)),
    }
}

fn parse_port(p: &str) -> Result<u16, ChError> {
    p.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| ChError::Config(format!("invalid port '{p}' in host")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base(host: &str) -> Result<ChConfig, ChError> {
        ChConfig::new(
            Some(host),
            None,
            Some("rocky"),
            Some("pw"),
            Duration::from_secs(30),
        )
    }

    fn extra(
        pairs: &[(&str, serde_json::Value)],
    ) -> std::collections::BTreeMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn defaults_follow_clickhouse() {
        let cfg = ChConfig::new(Some("ch"), None, None, None, Duration::from_secs(1)).unwrap();
        assert_eq!(cfg.database, "default");
        assert_eq!(cfg.user, "default");
        assert_eq!(cfg.base_url(), "http://ch:8123/");
    }

    #[test]
    fn secure_switches_scheme_and_default_port() {
        let cfg = base("ch.example.com")
            .unwrap()
            .apply_extra(&extra(&[("secure", serde_json::json!(true))]))
            .unwrap();
        assert_eq!(cfg.base_url(), "https://ch.example.com:8443/");
        let cfg = base("ch.example.com:9443")
            .unwrap()
            .apply_extra(&extra(&[("secure", serde_json::json!("true"))]))
            .unwrap();
        assert_eq!(cfg.base_url(), "https://ch.example.com:9443/");
    }

    #[test]
    fn host_may_carry_a_port_and_extra_port_wins() {
        let cfg = base("[::1]:7000").unwrap();
        assert_eq!(cfg.base_url(), "http://[::1]:7000/");
        let cfg = base("ch:7000")
            .unwrap()
            .apply_extra(&extra(&[("port", serde_json::json!("9000"))]))
            .unwrap();
        assert_eq!(cfg.port(), 9000);
    }

    #[test]
    fn urls_bad_ports_and_bad_databases_are_refused() {
        assert!(base("https://u:p@ch/x").is_err());
        assert!(base("ch:notaport").is_err());
        assert!(base("ch:0").is_err());
        assert!(
            ChConfig::new(Some("ch"), Some("a-b"), None, None, Duration::from_secs(1)).is_err()
        );
        assert!(ChConfig::new(None, None, None, None, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn typos_and_ca_cert_without_tls_fail() {
        let err = base("ch")
            .unwrap()
            .apply_extra(&extra(&[("tls", serde_json::json!(true))]))
            .unwrap_err();
        assert!(err.to_string().contains("unknown extra key 'tls'"), "{err}");
        let err = base("ch")
            .unwrap()
            .apply_extra(&extra(&[("ca_cert", serde_json::json!("/ca.pem"))]))
            .unwrap_err();
        assert!(err.to_string().contains("secure = true"), "{err}");
    }

    #[test]
    fn debug_redacts_the_password() {
        let shown = format!("{:?}", base("ch").unwrap());
        assert!(!shown.contains("\"pw\""), "{shown}");
        assert!(shown.contains("***"));
    }
}
