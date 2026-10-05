//! Connection configuration for the SQL Server adapter.
//!
//! Built by the CLI registry from the shared `[adapter]` slots (`host`,
//! `database`, `username`, `password`, `oauth_token`, `client_id`,
//! `client_secret`, `timeout_secs`) plus adapter-specific keys under
//! `[adapter.<name>.extra]` (see [`EXTRA_KEYS`]). Unknown `extra` keys are
//! refused by [`SqlServerConfig::apply_extra`], so a typo fails loudly
//! instead of silently using a default.

use std::time::Duration;

use crate::connector::SqlServerError;

/// Which Microsoft warehouse the connection speaks to. All three speak TDS
/// and T-SQL; the flavor only changes the few renderings Fabric Warehouse
/// does not accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Flavor {
    /// SQL Server 2016 SP1+ (on-premises or a VM), Azure SQL Database and
    /// Azure SQL Managed Instance.
    #[default]
    SqlServer,
    /// Microsoft Fabric Warehouse (and the Fabric SQL analytics endpoint):
    /// no `NVARCHAR`, no table hints, no `TABLESAMPLE`, Entra ID auth only.
    Fabric,
}

impl Flavor {
    /// Parse an `extra.flavor` value.
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Config`] for anything but `sqlserver` / `azure_sql`
    /// / `fabric`.
    pub fn parse(raw: &str) -> Result<Self, SqlServerError> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "sqlserver" | "sql_server" | "azure_sql" | "azuresql" => Ok(Self::SqlServer),
            "fabric" => Ok(Self::Fabric),
            other => Err(SqlServerError::Config(format!(
                "unsupported flavor '{other}': expected 'sqlserver' (also Azure SQL) or 'fabric'"
            ))),
        }
    }
}

/// TDS encryption policy, spelled like the ODBC / ADO.NET `Encrypt` keyword.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Encrypt {
    /// TLS for the whole session; the connection fails if the server
    /// cannot encrypt. The certificate is verified unless
    /// `trust_server_certificate = true`. The default (ODBC 18 default).
    #[default]
    Mandatory,
    /// TDS 8.0 strict mode: TLS starts before the prelogin, like HTTPS.
    /// SQL Server 2022+ and Azure SQL. Always verifies the certificate.
    Strict,
    /// Only the login packet is encrypted (`Encrypt=no`). Data, including
    /// query text, crosses the network in plaintext.
    Optional,
}

impl Encrypt {
    /// Parse an `extra.encrypt` value.
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Config`] for an unknown spelling.
    pub fn parse(value: &serde_json::Value) -> Result<Self, SqlServerError> {
        let raw = match value {
            serde_json::Value::Bool(true) => "mandatory".to_string(),
            serde_json::Value::Bool(false) => "optional".to_string(),
            serde_json::Value::String(s) => s.trim().to_ascii_lowercase(),
            _ => String::new(),
        };
        match raw.as_str() {
            "mandatory" | "true" | "yes" | "required" => Ok(Self::Mandatory),
            "strict" => Ok(Self::Strict),
            "optional" | "false" | "no" => Ok(Self::Optional),
            other => Err(SqlServerError::Config(format!(
                "unsupported encrypt '{other}': expected 'mandatory', 'strict' or 'optional'"
            ))),
        }
    }
}

/// How the session authenticates.
#[derive(Clone, PartialEq, Eq)]
pub enum Auth {
    /// SQL Server authentication (`username` + `password`). Not available on
    /// Fabric.
    SqlPassword { user: String, password: String },
    /// A pre-acquired Microsoft Entra ID access token for the
    /// `https://database.windows.net/` resource (`oauth_token`), e.g. from
    /// `az account get-access-token --resource https://database.windows.net/`.
    /// Not refreshed: a run that outlives the token fails at its next new
    /// connection.
    AccessToken(String),
    /// Microsoft Entra ID service principal (`client_id`, `client_secret`
    /// and `extra.tenant_id`), OAuth 2.0 client-credentials flow. Tokens
    /// are cached and refreshed before they expire.
    ServicePrincipal {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
}

impl Auth {
    /// Short name for messages and logs. Never the secret.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Auth::SqlPassword { .. } => "sql_password",
            Auth::AccessToken(_) => "access_token",
            Auth::ServicePrincipal { .. } => "service_principal",
        }
    }
}

impl std::fmt::Debug for Auth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Auth::SqlPassword { user, .. } => f
                .debug_struct("SqlPassword")
                .field("user", user)
                .field("password", &"***")
                .finish(),
            Auth::AccessToken(_) => f.debug_tuple("AccessToken").field(&"***").finish(),
            Auth::ServicePrincipal {
                tenant_id,
                client_id,
                ..
            } => f
                .debug_struct("ServicePrincipal")
                .field("tenant_id", tenant_id)
                .field("client_id", client_id)
                .field("client_secret", &"***")
                .finish(),
        }
    }
}

/// The credential slots of an `[adapter]` block, before an auth method is
/// chosen.
#[derive(Default)]
pub struct Credentials<'a> {
    pub username: Option<&'a str>,
    pub password: Option<&'a str>,
    pub access_token: Option<&'a str>,
    pub client_id: Option<&'a str>,
    pub client_secret: Option<&'a str>,
}

/// Everything needed to open a connection.
#[derive(Debug, Clone)]
pub struct SqlServerConfig {
    pub flavor: Flavor,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub auth: Auth,
    pub encrypt: Encrypt,
    /// Accept any server certificate (ODBC `TrustServerCertificate=yes`).
    pub trust_server_certificate: bool,
    /// Extra PEM / DER CA certificate trusted on top of the Mozilla root set.
    pub ca_cert: Option<String>,
    /// Upper bound on concurrently open connections.
    pub max_connections: usize,
    /// Per-statement timeout, and the connect / login timeout.
    pub timeout: Duration,
    /// Entra ID authority host. `login.microsoftonline.com` unless a
    /// sovereign cloud (`login.microsoftonline.us`, …) is configured.
    pub authority_host: String,
}

/// Default connection-pool size.
pub const DEFAULT_MAX_CONNECTIONS: usize = 8;

/// The server's default listening port.
pub const DEFAULT_PORT: u16 = 1433;

/// The `[adapter.<name>.extra]` keys this adapter reads.
pub const EXTRA_KEYS: &[&str] = &[
    "port",
    "flavor",
    "encrypt",
    "trust_server_certificate",
    "ca_cert",
    "tenant_id",
    "authority_host",
    "max_connections",
];

/// The Entra ID authority used when `extra.authority_host` is not set.
pub const DEFAULT_AUTHORITY_HOST: &str = "https://login.microsoftonline.com";

impl SqlServerConfig {
    /// Build a config from the shared adapter slots and the `extra` table.
    ///
    /// `host` may carry a port (`db.example.com,1433` in the SQL Server
    /// spelling, or `db.example.com:1433`); an explicit `extra.port` wins.
    ///
    /// Exactly one auth method must be configured: `username` + `password`,
    /// `oauth_token`, or `client_id` + `client_secret` (+ `extra.tenant_id`).
    ///
    /// # Errors
    ///
    /// [`SqlServerError::Config`] for a missing `host` / `database`, no or
    /// several auth methods, an unknown `extra` key, or a malformed value.
    pub fn new(
        host: Option<&str>,
        database: Option<&str>,
        creds: &Credentials<'_>,
        timeout: Duration,
        extra: &std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<Self, SqlServerError> {
        let raw_host = host
            .filter(|h| !h.trim().is_empty())
            .ok_or_else(|| SqlServerError::Config("host is required for sqlserver".into()))?;
        let (host, mut port) = split_host_port(raw_host)?;
        let database = database
            .filter(|d| !d.trim().is_empty())
            .ok_or_else(|| {
                SqlServerError::Config(
                    "database is required for sqlserver (Rocky never writes to `master`)".into(),
                )
            })?
            .to_string();

        let mut flavor = Flavor::SqlServer;
        let mut encrypt = Encrypt::Mandatory;
        let mut trust_server_certificate = false;
        let mut ca_cert = None;
        let mut tenant_id = None;
        let mut authority_host = DEFAULT_AUTHORITY_HOST.to_string();
        let mut max_connections = DEFAULT_MAX_CONNECTIONS;
        for (key, value) in extra {
            match key.as_str() {
                "port" => {
                    port = value_as_u64(key, value)?
                        .try_into()
                        .ok()
                        .filter(|p| *p != 0)
                        .ok_or_else(|| {
                            SqlServerError::Config("extra.port is out of range".into())
                        })?;
                }
                "flavor" => flavor = Flavor::parse(value_as_str(key, value)?)?,
                "encrypt" => encrypt = Encrypt::parse(value)?,
                "trust_server_certificate" => {
                    trust_server_certificate = value_as_bool(key, value)?;
                }
                "ca_cert" => ca_cert = Some(value_as_str(key, value)?.to_string()),
                "tenant_id" => tenant_id = Some(value_as_str(key, value)?.trim().to_string()),
                "authority_host" => {
                    let raw = value_as_str(key, value)?.trim().trim_end_matches('/');
                    // Plain HTTP only for a loopback stand-in (tests): the
                    // client secret is posted to this host.
                    let loopback = raw.strip_prefix("http://127.0.0.1").is_some_and(|rest| {
                        rest.is_empty()
                            || rest
                                .strip_prefix(':')
                                .is_some_and(|p| p.parse::<u16>().is_ok())
                    });
                    if !raw.starts_with("https://") && !loopback {
                        return Err(SqlServerError::Config(
                            "extra.authority_host must be an https:// URL".into(),
                        ));
                    }
                    authority_host = raw.to_string();
                }
                "max_connections" => {
                    let n = value_as_u64(key, value)?;
                    if n == 0 || n > 256 {
                        return Err(SqlServerError::Config(
                            "extra.max_connections must be between 1 and 256".into(),
                        ));
                    }
                    max_connections = n as usize;
                }
                other => {
                    return Err(SqlServerError::Config(format!(
                        "unknown extra key '{other}' for sqlserver; supported keys: {}",
                        EXTRA_KEYS.join(", ")
                    )));
                }
            }
        }
        if trust_server_certificate && ca_cert.is_some() {
            return Err(SqlServerError::Config(
                "extra.trust_server_certificate and extra.ca_cert are mutually exclusive: \
                 pin the CA (ca_cert) or skip verification, not both"
                    .into(),
            ));
        }
        if trust_server_certificate && encrypt == Encrypt::Strict {
            return Err(SqlServerError::Config(
                "encrypt = \"strict\" always verifies the server certificate; remove \
                 trust_server_certificate or use encrypt = \"mandatory\""
                    .into(),
            ));
        }

        let auth = choose_auth(creds, tenant_id)?;
        if flavor == Flavor::Fabric && matches!(auth, Auth::SqlPassword { .. }) {
            return Err(SqlServerError::Config(
                "Fabric Warehouse accepts Microsoft Entra ID authentication only: set \
                 oauth_token, or client_id + client_secret + extra.tenant_id"
                    .into(),
            ));
        }
        Ok(Self {
            flavor,
            host,
            port,
            database,
            auth,
            encrypt,
            trust_server_certificate,
            ca_cert,
            max_connections,
            timeout,
            authority_host,
        })
    }
}

fn non_empty(v: Option<&str>) -> Option<&str> {
    v.filter(|s| !s.trim().is_empty())
}

fn choose_auth(creds: &Credentials<'_>, tenant_id: Option<String>) -> Result<Auth, SqlServerError> {
    let password = non_empty(creds.password);
    let user = non_empty(creds.username);
    let token = non_empty(creds.access_token);
    let client_id = non_empty(creds.client_id);
    let client_secret = non_empty(creds.client_secret);

    let mut methods = Vec::new();
    if password.is_some() {
        methods.push("username + password");
    }
    if token.is_some() {
        methods.push("oauth_token");
    }
    if client_secret.is_some() {
        methods.push("client_id + client_secret");
    }
    if methods.len() > 1 {
        return Err(SqlServerError::Config(format!(
            "sqlserver: several auth methods are configured ({}); set exactly one",
            methods.join(", ")
        )));
    }
    if let Some(token) = token {
        return Ok(Auth::AccessToken(token.to_string()));
    }
    if let Some(secret) = client_secret {
        let client_id = client_id.ok_or_else(|| {
            SqlServerError::Config("sqlserver: client_secret is set but client_id is not".into())
        })?;
        let tenant_id = tenant_id.filter(|t| !t.is_empty()).ok_or_else(|| {
            SqlServerError::Config(
                "sqlserver: service principal auth needs extra.tenant_id (the Entra ID \
                 directory id)"
                    .into(),
            )
        })?;
        if !tenant_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.')
        {
            return Err(SqlServerError::Config(
                "sqlserver: extra.tenant_id must be a directory id or domain".into(),
            ));
        }
        return Ok(Auth::ServicePrincipal {
            tenant_id,
            client_id: client_id.to_string(),
            client_secret: secret.to_string(),
        });
    }
    match (user, password) {
        (Some(user), Some(password)) => Ok(Auth::SqlPassword {
            user: user.to_string(),
            password: password.to_string(),
        }),
        (None, Some(_)) => Err(SqlServerError::Config(
            "sqlserver: password is set but username is not".into(),
        )),
        _ => Err(SqlServerError::Config(
            "sqlserver: no credentials; set username + password (SQL authentication), \
             oauth_token (an Entra ID access token), or client_id + client_secret + \
             extra.tenant_id (an Entra ID service principal)"
                .into(),
        )),
    }
}

fn value_as_str<'a>(key: &str, value: &'a serde_json::Value) -> Result<&'a str, SqlServerError> {
    value
        .as_str()
        .ok_or_else(|| SqlServerError::Config(format!("extra.{key} must be a string")))
}

fn value_as_bool(key: &str, value: &serde_json::Value) -> Result<bool, SqlServerError> {
    match value {
        serde_json::Value::Bool(b) => Ok(*b),
        // Env-var substitution produces strings.
        serde_json::Value::String(s) if s.trim().eq_ignore_ascii_case("true") => Ok(true),
        serde_json::Value::String(s) if s.trim().eq_ignore_ascii_case("false") => Ok(false),
        _ => Err(SqlServerError::Config(format!(
            "extra.{key} must be true or false"
        ))),
    }
}

fn value_as_u64(key: &str, value: &serde_json::Value) -> Result<u64, SqlServerError> {
    if let Some(n) = value.as_u64() {
        return Ok(n);
    }
    value
        .as_str()
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| {
            SqlServerError::Config(format!("extra.{key} must be a non-negative integer"))
        })
}

/// Split `host[,port]` / `host[:port]`. A connection string or URL is
/// refused — it would carry credentials Rocky must not log. A named instance
/// (`host\instance`) is refused too: resolving it needs the SQL Browser
/// service; configure the instance's port instead.
fn split_host_port(raw: &str) -> Result<(String, u16), SqlServerError> {
    let raw = raw.trim();
    if raw.contains("://") || raw.contains('@') || raw.contains('/') || raw.contains(';') {
        return Err(SqlServerError::Config(
            "host must be a bare host name or host,port — not a URL or connection string".into(),
        ));
    }
    if raw.contains('\\') {
        return Err(SqlServerError::Config(
            "named instances (host\\instance) are not supported; set extra.port to the \
             instance's TCP port"
                .into(),
        ));
    }
    let raw = raw.strip_prefix("tcp:").unwrap_or(raw);
    if let Some((h, p)) = raw.rsplit_once(',') {
        return Ok((h.trim().to_string(), parse_port(p.trim())?));
    }
    if let Some(rest) = raw.strip_prefix('[') {
        let (addr, tail) = rest
            .split_once(']')
            .ok_or_else(|| SqlServerError::Config("unterminated IPv6 literal in host".into()))?;
        let port = match tail.strip_prefix(':') {
            Some(p) => parse_port(p)?,
            None if tail.is_empty() => DEFAULT_PORT,
            None => return Err(SqlServerError::Config(format!("invalid host '{raw}'"))),
        };
        return Ok((addr.to_string(), port));
    }
    match raw.rsplit_once(':') {
        Some((h, _)) if h.contains(':') => Ok((raw.to_string(), DEFAULT_PORT)),
        Some((h, p)) => Ok((h.to_string(), parse_port(p)?)),
        None => Ok((raw.to_string(), DEFAULT_PORT)),
    }
}

fn parse_port(p: &str) -> Result<u16, SqlServerError> {
    p.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| SqlServerError::Config(format!("invalid port '{p}' in host")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn sql_creds() -> Credentials<'static> {
        Credentials {
            username: Some("rocky"),
            password: Some("pw"),
            ..Credentials::default()
        }
    }

    fn build(
        host: &str,
        creds: &Credentials<'_>,
        extra: &BTreeMap<String, serde_json::Value>,
    ) -> Result<SqlServerConfig, SqlServerError> {
        SqlServerConfig::new(
            Some(host),
            Some("analytics"),
            creds,
            Duration::from_secs(30),
            extra,
        )
    }

    #[test]
    fn host_spellings() {
        let none = BTreeMap::new();
        let cfg = build("db.example.com", &sql_creds(), &none).unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port), ("db.example.com", 1433));
        let cfg = build("tcp:srv.database.windows.net,1433", &sql_creds(), &none).unwrap();
        assert_eq!(
            (cfg.host.as_str(), cfg.port),
            ("srv.database.windows.net", 1433)
        );
        let cfg = build("db:14330", &sql_creds(), &none).unwrap();
        assert_eq!(cfg.port, 14330);
        let cfg = build("[::1]:1500", &sql_creds(), &none).unwrap();
        assert_eq!((cfg.host.as_str(), cfg.port), ("::1", 1500));
        assert!(build("sqlserver://u:p@db", &sql_creds(), &none).is_err());
        assert!(build("Server=db;User=x", &sql_creds(), &none).is_err());
        let err = build("db\\SQLEXPRESS", &sql_creds(), &none).unwrap_err();
        assert!(err.to_string().contains("named instances"), "{err}");
        assert!(build("db,0", &sql_creds(), &none).is_err());
    }

    #[test]
    fn database_is_required() {
        let err = SqlServerConfig::new(
            Some("db"),
            None,
            &sql_creds(),
            Duration::from_secs(1),
            &BTreeMap::new(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("database is required"), "{err}");
    }

    #[test]
    fn exactly_one_auth_method() {
        let none = BTreeMap::new();
        let err = build("db", &Credentials::default(), &none).unwrap_err();
        assert!(err.to_string().contains("no credentials"), "{err}");

        let both = Credentials {
            username: Some("u"),
            password: Some("p"),
            access_token: Some("tok"),
            ..Credentials::default()
        };
        let err = build("db", &both, &none).unwrap_err();
        assert!(err.to_string().contains("several auth methods"), "{err}");

        let token = Credentials {
            access_token: Some("tok"),
            ..Credentials::default()
        };
        assert_eq!(
            build("db", &token, &none).unwrap().auth,
            Auth::AccessToken("tok".into())
        );

        let sp = Credentials {
            client_id: Some("app"),
            client_secret: Some("s3cret"),
            ..Credentials::default()
        };
        let err = build("db", &sp, &none).unwrap_err();
        assert!(err.to_string().contains("tenant_id"), "{err}");
        let mut extra = BTreeMap::new();
        extra.insert(
            "tenant_id".into(),
            serde_json::json!("contoso.onmicrosoft.com"),
        );
        assert_eq!(
            build("db", &sp, &extra).unwrap().auth.kind(),
            "service_principal"
        );
        extra.insert("tenant_id".into(), serde_json::json!("x/../y"));
        assert!(build("db", &sp, &extra).is_err());
    }

    #[test]
    fn extra_keys_apply_and_typos_fail() {
        let mut extra = BTreeMap::new();
        extra.insert("port".into(), serde_json::json!("14333"));
        extra.insert("encrypt".into(), serde_json::json!("strict"));
        extra.insert("max_connections".into(), serde_json::json!(4));
        extra.insert("flavor".into(), serde_json::json!("azure_sql"));
        let cfg = build("db", &sql_creds(), &extra).unwrap();
        assert_eq!(cfg.port, 14333);
        assert_eq!(cfg.encrypt, Encrypt::Strict);
        assert_eq!(cfg.max_connections, 4);
        assert_eq!(cfg.flavor, Flavor::SqlServer);

        let mut typo = BTreeMap::new();
        typo.insert("trust_cert".into(), serde_json::json!(true));
        let err = build("db", &sql_creds(), &typo).unwrap_err();
        assert!(
            err.to_string().contains("unknown extra key 'trust_cert'"),
            "{err}"
        );

        let mut bad = BTreeMap::new();
        bad.insert("encrypt".into(), serde_json::json!("sometimes"));
        assert!(build("db", &sql_creds(), &bad).is_err());
    }

    #[test]
    fn encrypt_spellings() {
        assert_eq!(
            Encrypt::parse(&serde_json::json!(true)).unwrap(),
            Encrypt::Mandatory
        );
        assert_eq!(
            Encrypt::parse(&serde_json::json!("no")).unwrap(),
            Encrypt::Optional
        );
        assert_eq!(
            Encrypt::parse(&serde_json::json!("Strict")).unwrap(),
            Encrypt::Strict
        );
    }

    #[test]
    fn conflicting_tls_settings_are_refused() {
        let mut extra = BTreeMap::new();
        extra.insert("trust_server_certificate".into(), serde_json::json!(true));
        extra.insert("ca_cert".into(), serde_json::json!("/tmp/ca.pem"));
        assert!(build("db", &sql_creds(), &extra).is_err());
        let mut strict = BTreeMap::new();
        strict.insert("trust_server_certificate".into(), serde_json::json!("true"));
        strict.insert("encrypt".into(), serde_json::json!("strict"));
        assert!(build("db", &sql_creds(), &strict).is_err());
    }

    #[test]
    fn fabric_refuses_sql_auth() {
        let mut extra = BTreeMap::new();
        extra.insert("flavor".into(), serde_json::json!("fabric"));
        let err = build("x.datawarehouse.fabric.microsoft.com", &sql_creds(), &extra).unwrap_err();
        assert!(err.to_string().contains("Entra ID"), "{err}");
        let token = Credentials {
            access_token: Some("tok"),
            ..Credentials::default()
        };
        assert_eq!(build("x", &token, &extra).unwrap().flavor, Flavor::Fabric);
    }

    #[test]
    fn authority_host_must_be_https() {
        let mut extra = BTreeMap::new();
        extra.insert("authority_host".into(), serde_json::json!("http://evil"));
        assert!(build("db", &sql_creds(), &extra).is_err());
        extra.insert(
            "authority_host".into(),
            serde_json::json!("http://127.0.0.1.attacker.example"),
        );
        assert!(build("db", &sql_creds(), &extra).is_err());
        extra.insert(
            "authority_host".into(),
            serde_json::json!("http://127.0.0.1:8080"),
        );
        assert!(build("db", &sql_creds(), &extra).is_ok());
        extra.insert(
            "authority_host".into(),
            serde_json::json!("https://login.microsoftonline.us/"),
        );
        assert_eq!(
            build("db", &sql_creds(), &extra).unwrap().authority_host,
            "https://login.microsoftonline.us"
        );
    }

    #[test]
    fn debug_redacts_secrets() {
        let cfg = build("db", &sql_creds(), &BTreeMap::new()).unwrap();
        let shown = format!("{cfg:?}");
        assert!(!shown.contains("\"pw\""), "{shown}");
        assert!(shown.contains("***"));
        let token = Auth::AccessToken("eyJhbGciOi".into());
        assert!(!format!("{token:?}").contains("eyJ"));
    }
}
