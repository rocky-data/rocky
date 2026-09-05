//! The browser UI's server-side contract: where its files come from, which
//! `Host` and `Origin` values a `--ui` server accepts, and the headers every
//! UI response carries.
//!
//! The routes themselves live in `rocky_cli::ui`, next to the API router,
//! and the embedded files live there too, behind the `ui` cargo feature.
//! This module has no feature gate, so the guard and the headers are
//! testable in every build, with an in-memory file set.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::Arc;

/// One file the UI serves, with the type the browser must be told.
pub struct UiFile {
    pub bytes: Cow<'static, [u8]>,
    pub content_type: &'static str,
}

/// Where the UI's files come from. The `ui` feature embeds `engine/ui/dist`;
/// tests use [`InMemoryAssets`].
pub trait UiAssetSource: Send + Sync {
    /// The file at `path` (relative to `dist/`, no leading slash), or `None`.
    fn read(&self, path: &str) -> Option<Cow<'static, [u8]>>;
}

/// An in-memory file set, for tests and for embedders that build their own.
pub struct InMemoryAssets(pub BTreeMap<String, Vec<u8>>);

impl UiAssetSource for InMemoryAssets {
    fn read(&self, path: &str) -> Option<Cow<'static, [u8]>> {
        self.0.get(path).map(|bytes| Cow::Owned(bytes.clone()))
    }
}

/// The `--ui` configuration on the server state. `None` on the state means
/// the UI routes do not exist and the host guard is off.
pub struct UiConfig {
    /// The `--host` the server bound; a `Host` header naming it is accepted.
    pub bind_host: String,
    /// Extra `Host` values to accept (`--allowed-host`), for a reverse proxy.
    pub allowed_hosts: Vec<String>,
    /// The files under `/ui/`.
    pub assets: Arc<dyn UiAssetSource>,
}

impl UiConfig {
    /// Whether a `Host` header value names this server: loopback by any of
    /// its names, the bind host, or an `--allowed-host` entry. The port is
    /// ignored; a proxy may rewrite it.
    pub fn host_allowed(&self, host_header: &str) -> bool {
        let host = host_without_port(host_header.trim()).to_ascii_lowercase();
        if host.is_empty() {
            return false;
        }
        if matches!(host.as_str(), "localhost" | "127.0.0.1" | "[::1]" | "::1") {
            return true;
        }
        if host == host_without_port(&self.bind_host).to_ascii_lowercase() {
            return true;
        }
        self.allowed_hosts
            .iter()
            .any(|allowed| host_without_port(allowed).eq_ignore_ascii_case(&host))
    }

    /// Whether a present `Origin` header may reach this server: an exact
    /// `--allowed-origin` entry, or an `http`/`https` origin whose host
    /// passes [`Self::host_allowed`] (the page the server itself served).
    /// The opaque origin `null` never passes.
    pub fn origin_allowed(&self, origin: &str, allowed_origins: &[String]) -> bool {
        let origin = origin.trim();
        if origin.is_empty() || origin.eq_ignore_ascii_case("null") {
            return false;
        }
        if allowed_origins
            .iter()
            .any(|allowed| allowed.trim_end_matches('/').eq_ignore_ascii_case(origin))
        {
            return true;
        }
        let Some((scheme, authority)) = origin.split_once("://") else {
            return false;
        };
        if !(scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")) {
            return false;
        }
        // An origin has no path, but be strict about a trailing one.
        let authority = authority.trim_end_matches('/');
        if authority.contains('/') {
            return false;
        }
        self.host_allowed(authority)
    }

    /// The file at `path`, typed by its extension.
    pub fn file(&self, path: &str) -> Option<UiFile> {
        self.assets.read(path).map(|bytes| UiFile {
            bytes,
            content_type: content_type_for(path),
        })
    }
}

/// Strip an optional `:port` from a host or authority, keeping IPv6
/// brackets: `127.0.0.1:8080` → `127.0.0.1`, `[::1]:8080` → `[::1]`.
pub fn host_without_port(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        return match rest.find(']') {
            Some(end) => &authority[..end + 2],
            None => authority,
        };
    }
    match authority.rsplit_once(':') {
        // A bare IPv6 literal without brackets has several colons; leave it.
        Some((host, port)) if !host.contains(':') && port.bytes().all(|b| b.is_ascii_digit()) => {
            host
        }
        _ => authority,
    }
}

/// The `Content-Type` for a file the UI serves, by extension. Unknown
/// extensions are served as bytes, never sniffed (the responses say so).
pub fn content_type_for(path: &str) -> &'static str {
    match path
        .rsplit('.')
        .next()
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("html") => "text/html; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("css") => "text/css; charset=utf-8",
        Some("json" | "map" | "webmanifest") => "application/json; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        Some("woff") => "font/woff",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}

/// The headers every UI response carries. Scripts, styles, images, fonts and
/// connections come from the server's own origin only; the page cannot be
/// framed; nothing is sniffed; no referrer leaves. The one allowance, inline
/// styles, is for React Flow's positioned nodes.
pub const UI_SECURITY_HEADERS: &[(&str, &str)] = &[
    (
        "content-security-policy",
        "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
         img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'; \
         base-uri 'none'; form-action 'self'; object-src 'none'",
    ),
    ("x-frame-options", "DENY"),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "no-referrer"),
];

#[cfg(test)]
mod tests {
    use super::*;

    fn config(bind_host: &str, allowed_hosts: &[&str]) -> UiConfig {
        UiConfig {
            bind_host: bind_host.to_string(),
            allowed_hosts: allowed_hosts.iter().map(ToString::to_string).collect(),
            assets: Arc::new(InMemoryAssets(BTreeMap::new())),
        }
    }

    #[test]
    fn host_without_port_keeps_ipv6_brackets_and_bare_literals() {
        assert_eq!(host_without_port("127.0.0.1:8080"), "127.0.0.1");
        assert_eq!(host_without_port("localhost"), "localhost");
        assert_eq!(host_without_port("[::1]:8080"), "[::1]");
        assert_eq!(host_without_port("[::1]"), "[::1]");
        assert_eq!(host_without_port("::1"), "::1");
        assert_eq!(host_without_port("ui.internal:443"), "ui.internal");
    }

    #[test]
    fn loopback_names_the_bind_host_and_allowed_hosts_pass_everything_else_fails() {
        let ui = config("127.0.0.1", &["ui.internal"]);
        for ok in [
            "localhost",
            "LOCALHOST:8080",
            "127.0.0.1:8080",
            "[::1]:8080",
            "ui.internal",
            "ui.internal:8443",
        ] {
            assert!(ui.host_allowed(ok), "{ok}");
        }
        for bad in [
            "evil.example",
            "evil.example:8080",
            "",
            "127.0.0.1.evil.example",
        ] {
            assert!(!ui.host_allowed(bad), "{bad}");
        }
        // A named bind host is accepted without being listed.
        assert!(config("rocky.internal", &[]).host_allowed("rocky.internal:8080"));
    }

    #[test]
    fn origins_pass_by_exact_allowlist_or_by_an_allowed_host() {
        let ui = config("127.0.0.1", &["ui.internal"]);
        let allowed = vec!["https://app.example".to_string()];
        for ok in [
            "http://127.0.0.1:8080",
            "http://localhost:5173",
            "https://ui.internal",
            "https://app.example",
            "HTTPS://APP.EXAMPLE",
        ] {
            assert!(ui.origin_allowed(ok, &allowed), "{ok}");
        }
        for bad in [
            "http://evil.example",
            "null",
            "",
            "ftp://127.0.0.1",
            "127.0.0.1:8080",
            "http://127.0.0.1:8080/path",
            "https://app.example.evil",
        ] {
            assert!(!ui.origin_allowed(bad, &allowed), "{bad}");
        }
    }

    #[test]
    fn content_types_follow_the_extension_and_never_sniff() {
        assert_eq!(content_type_for("index.html"), "text/html; charset=utf-8");
        assert_eq!(
            content_type_for("assets/index-abc.js"),
            "text/javascript; charset=utf-8"
        );
        assert_eq!(
            content_type_for("assets/index-abc.css"),
            "text/css; charset=utf-8"
        );
        assert_eq!(content_type_for("assets/a.woff2"), "font/woff2");
        assert_eq!(content_type_for("weird.bin"), "application/octet-stream");
    }
}
