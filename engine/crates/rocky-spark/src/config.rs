//! Connection configuration for the Spark adapter.
//!
//! Built by the CLI registry from the shared `[adapter]` slots (`host`,
//! `token`, `username`, `timeout_secs`) plus adapter-specific keys under
//! `[adapter.<name>.extra]` (`port`, `use_ssl`). Unknown `extra` keys are
//! refused by [`SparkConfig::apply_extra`], so a typo fails loudly instead of
//! silently using a default.

use std::time::Duration;

use crate::connector::SparkError;

/// The Spark Connect server's default port.
pub const DEFAULT_PORT: u16 = 15002;

/// The `[adapter.<name>.extra]` keys this adapter reads.
pub const EXTRA_KEYS: &[&str] = &["port", "use_ssl"];

/// The user id sent when `username` is unset.
pub const DEFAULT_USER_ID: &str = "rocky";

/// Everything needed to reach a Spark Connect server.
#[derive(Clone)]
pub struct SparkConfig {
    pub host: String,
    port: Option<u16>,
    /// TLS to the server. Always on when a `token` is set: a bearer token is
    /// never sent in clear text.
    use_ssl: Option<bool>,
    /// Sent as `authorization: Bearer <token>` on every call.
    pub token: Option<String>,
    /// The Spark Connect `user_id` (shown in the Spark UI).
    pub user_id: String,
    /// Per-statement timeout.
    pub timeout: Duration,
}

impl std::fmt::Debug for SparkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SparkConfig")
            .field("host", &self.host)
            .field("port", &self.port())
            .field("use_ssl", &self.use_ssl())
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("user_id", &self.user_id)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl SparkConfig {
    /// Build a config from the shared adapter slots.
    ///
    /// `host` is `host`, `host:port`, or `sc://host[:port]`. Connection-string
    /// parameters (`sc://host/;token=…`) are refused: the token belongs in the
    /// redacted `token` slot, never in a string Rocky may print.
    ///
    /// # Errors
    ///
    /// Returns [`SparkError::Config`] when `host` is missing or malformed.
    pub fn new(
        host: Option<&str>,
        token: Option<&str>,
        user_id: Option<&str>,
        timeout: Duration,
    ) -> Result<Self, SparkError> {
        let raw = host
            .map(str::trim)
            .filter(|h| !h.is_empty())
            .ok_or_else(|| SparkError::Config("host is required for spark".into()))?;
        let (host, port) = split_host_port(raw)?;
        let user_id = user_id
            .map(str::trim)
            .filter(|u| !u.is_empty())
            .unwrap_or(DEFAULT_USER_ID);
        let token = token
            .map(str::trim)
            .filter(|t| !t.is_empty())
            .map(str::to_string);
        Ok(Self {
            host,
            port,
            use_ssl: None,
            token,
            user_id: user_id.to_string(),
            timeout,
        })
    }

    /// Apply `[adapter.<name>.extra]` keys.
    ///
    /// # Errors
    ///
    /// Returns [`SparkError::Config`] for an unknown key, a value of the wrong
    /// shape, or `use_ssl = false` together with a `token`.
    pub fn apply_extra(
        mut self,
        extra: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, SparkError> {
        for (key, value) in extra {
            match key.as_str() {
                "port" => {
                    let port: u16 = value_as_u64(key, value)?
                        .try_into()
                        .ok()
                        .filter(|p| *p != 0)
                        .ok_or_else(|| SparkError::Config("extra.port is out of range".into()))?;
                    self.port = Some(port);
                }
                "use_ssl" => self.use_ssl = Some(value_as_bool(key, value)?),
                other => {
                    return Err(SparkError::Config(format!(
                        "unknown extra key '{other}' for spark; supported keys: {}",
                        EXTRA_KEYS.join(", ")
                    )));
                }
            }
        }
        if self.token.is_some() && self.use_ssl == Some(false) {
            return Err(SparkError::Config(
                "extra.use_ssl = false with a token would send the bearer token in clear \
                 text; remove use_ssl or the token"
                    .into(),
            ));
        }
        Ok(self)
    }

    /// The resolved port: the configured one, else 15002.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port.unwrap_or(DEFAULT_PORT)
    }

    /// Whether the connection uses TLS: `extra.use_ssl` when set, else on
    /// exactly when a token is set.
    #[must_use]
    pub fn use_ssl(&self) -> bool {
        self.use_ssl.unwrap_or(self.token.is_some())
    }

    /// The gRPC endpoint URL (`http://host:15002` or `https://…`). Carries no
    /// credentials: the token travels in a header.
    #[must_use]
    pub fn endpoint_url(&self) -> String {
        let scheme = if self.use_ssl() { "https" } else { "http" };
        let host = if self.host.contains(':') {
            format!("[{}]", self.host)
        } else {
            self.host.clone()
        };
        format!("{scheme}://{host}:{}", self.port())
    }
}

fn value_as_bool(key: &str, value: &serde_json::Value) -> Result<bool, SparkError> {
    match value {
        serde_json::Value::Bool(b) => Ok(*b),
        // Env-var substitution produces strings (`use_ssl = "${SPARK_TLS:-true}"`).
        serde_json::Value::String(s) if s.trim() == "true" => Ok(true),
        serde_json::Value::String(s) if s.trim() == "false" => Ok(false),
        _ => Err(SparkError::Config(format!(
            "extra.{key} must be true or false"
        ))),
    }
}

fn value_as_u64(key: &str, value: &serde_json::Value) -> Result<u64, SparkError> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    value
        .as_str()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| SparkError::Config(format!("extra.{key} must be a non-negative integer")))
}

/// Split `[sc://]host[:port]`.
fn split_host_port(raw: &str) -> Result<(String, Option<u16>), SparkError> {
    let rest = raw.strip_prefix("sc://").unwrap_or(raw);
    if rest.contains("://") {
        return Err(SparkError::Config(format!(
            "host '{raw}' must be host[:port] or sc://host[:port]"
        )));
    }
    // `sc://host:15002/` is how PySpark prints a bare remote; a trailing
    // slash is harmless. Anything after it is a connection-string parameter.
    let (authority, params) = match rest.split_once('/') {
        Some((a, p)) => (a, p),
        None => (rest, ""),
    };
    if !params.is_empty() {
        return Err(SparkError::Config(
            "connection-string parameters in host are not supported; set the token in \
             `token`, the user in `username` and TLS in `extra.use_ssl`"
                .into(),
        ));
    }
    if authority.contains('@') {
        return Err(SparkError::Config(
            "credentials in host are not supported; set the token in `token`".into(),
        ));
    }
    let (host, port) = if let Some(inner) = authority.strip_prefix('[') {
        // `[::1]:15002`
        let (h, tail) = inner
            .split_once(']')
            .ok_or_else(|| SparkError::Config(format!("host '{raw}' has an unclosed '['")))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => Some(parse_port(raw, p)?),
            None if tail.is_empty() => None,
            None => return Err(SparkError::Config(format!("host '{raw}' is malformed"))),
        };
        (h.to_string(), port)
    } else {
        match authority.rsplit_once(':') {
            Some((h, p)) if !h.contains(':') => (h.to_string(), Some(parse_port(raw, p)?)),
            Some(_) => {
                return Err(SparkError::Config(format!(
                    "host '{raw}': write an IPv6 address as [addr]:port"
                )));
            }
            None => (authority.to_string(), None),
        }
    };
    if host.is_empty() {
        return Err(SparkError::Config(format!("host '{raw}' names no host")));
    }
    Ok((host, port))
}

fn parse_port(raw: &str, p: &str) -> Result<u16, SparkError> {
    p.parse::<u16>()
        .ok()
        .filter(|p| *p != 0)
        .ok_or_else(|| SparkError::Config(format!("host '{raw}' has an invalid port")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(host: &str) -> Result<SparkConfig, SparkError> {
        SparkConfig::new(Some(host), None, None, Duration::from_secs(1))
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
    fn host_forms_resolve_to_one_endpoint() {
        for host in [
            "spark.example.com",
            "sc://spark.example.com",
            "sc://spark.example.com/",
        ] {
            assert_eq!(
                cfg(host).unwrap().endpoint_url(),
                "http://spark.example.com:15002"
            );
        }
        assert_eq!(cfg("sc://h:443").unwrap().endpoint_url(), "http://h:443");
        assert_eq!(
            cfg("[::1]:15003").unwrap().endpoint_url(),
            "http://[::1]:15003"
        );
    }

    #[test]
    fn malformed_hosts_are_refused() {
        for bad in [
            "",
            "https://h:15002",
            "sc://h/;token=secret",
            "sc://user@h",
            "h:notaport",
            "h:0",
            "::1",
            "[::1",
        ] {
            assert!(
                SparkConfig::new(Some(bad), None, None, Duration::from_secs(1)).is_err(),
                "{bad:?} must be refused"
            );
        }
        assert!(SparkConfig::new(None, None, None, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn a_token_turns_tls_on_and_cannot_be_sent_in_clear_text() {
        let with_token =
            SparkConfig::new(Some("h"), Some("t"), None, Duration::from_secs(1)).unwrap();
        assert!(with_token.use_ssl());
        assert_eq!(with_token.endpoint_url(), "https://h:15002");
        let err = with_token
            .clone()
            .apply_extra(&extra(&[("use_ssl", serde_json::json!(false))]))
            .unwrap_err();
        assert!(err.to_string().contains("clear text"), "{err}");
        // TLS without a token is allowed (a server behind a TLS proxy).
        let tls = cfg("h")
            .unwrap()
            .apply_extra(&extra(&[("use_ssl", serde_json::json!("true"))]))
            .unwrap();
        assert_eq!(tls.endpoint_url(), "https://h:15002");
    }

    #[test]
    fn extra_port_wins_and_unknown_keys_are_refused() {
        let c = cfg("h:1")
            .unwrap()
            .apply_extra(&extra(&[("port", serde_json::json!("15010"))]))
            .unwrap();
        assert_eq!(c.port(), 15010);
        let err = cfg("h")
            .unwrap()
            .apply_extra(&extra(&[("cluster", serde_json::json!("x"))]))
            .unwrap_err();
        assert!(
            err.to_string().contains("supported keys: port, use_ssl"),
            "{err}"
        );
        assert!(
            cfg("h")
                .unwrap()
                .apply_extra(&extra(&[("port", serde_json::json!(70000))]))
                .is_err()
        );
    }

    #[test]
    fn debug_redacts_the_token() {
        let c = SparkConfig::new(
            Some("h"),
            Some("s3cret"),
            Some("ana"),
            Duration::from_secs(1),
        )
        .unwrap();
        let shown = format!("{c:?}");
        assert!(!shown.contains("s3cret"), "{shown}");
        assert!(shown.contains("ana"), "{shown}");
    }
}
