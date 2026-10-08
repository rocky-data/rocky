//! `rocky serve --ui`: the routes that serve the browser UI, the file set
//! embedded at build time (cargo feature `ui`), and the request-body bound
//! every mode enforces.
//!
//! The files are public: they carry no data, and a browser must be able to
//! load the page before it has a token to send. So the UI router is merged
//! into the app *outside* the bearer layer and *inside* the host guard
//! (`rocky_server::auth::require_known_host`). Every API call the page then
//! makes goes through the bearer layer like any other client's.
//!
//! The page never holds the token. `rocky serve --ui` prints
//! `http://127.0.0.1:<port>/login?t=<token>`; `GET /login` checks the token
//! and answers `303 /ui/` with the session cookie
//! ([`rocky_server::ui_session`]). The page's API calls then carry the
//! cookie. A wrong or stale link gets a small page with a token field that
//! posts to `POST /login`. The token is never echoed and never logged.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, RawQuery, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use rocky_server::state::ServerState;
use rocky_server::ui::{UI_SECURITY_HEADERS, UiAssetSource, UiFile};
use rocky_server::ui_session;

/// The largest request body any route reads. Job submissions are a few
/// hundred bytes; a body over this answers `413` with the envelope before a
/// handler sees it.
pub const MAX_REQUEST_BODY_BYTES: usize = 1024 * 1024;

#[cfg(feature = "ui")]
mod embedded {
    use std::borrow::Cow;

    use rocky_server::ui::UiAssetSource;

    // `static DIST`: the `include_dir!` of `engine/ui/dist` when it existed
    // at build time, an empty directory otherwise. Written by `build.rs`, so
    // a build with the feature and no `dist/` still compiles (CI's
    // `--all-features` lint) and `--ui` refuses the empty embed at start.
    include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

    pub struct EmbeddedAssets;

    impl UiAssetSource for EmbeddedAssets {
        fn read(&self, path: &str) -> Option<Cow<'static, [u8]>> {
            DIST.get_file(path)
                .map(|file| Cow::Borrowed(file.contents()))
        }
    }

    /// Whether the embed holds a page at all.
    pub fn has_index() -> bool {
        DIST.get_file("index.html").is_some()
    }
}

/// Whether this binary was built with the `ui` feature.
pub const fn built_with_ui() -> bool {
    cfg!(feature = "ui")
}

/// The embedded file set, or `None` when this build carries no page: a
/// build without the `ui` feature, or one made with the feature before
/// `npm run build` wrote `engine/ui/dist`.
pub fn embedded_assets() -> Option<Arc<dyn UiAssetSource>> {
    #[cfg(feature = "ui")]
    {
        if embedded::has_index() {
            Some(Arc::new(embedded::EmbeddedAssets))
        } else {
            None
        }
    }
    #[cfg(not(feature = "ui"))]
    {
        None
    }
}

/// The `/ui` routes. Merged into the app only when `state.ui` is set, so a
/// server started without `--ui` has no `/ui` path at all.
pub(crate) fn ui_router(state: Arc<ServerState>) -> Router {
    Router::new()
        // The retired dashboard's address: with `--ui`, the page is here.
        .route("/", get(redirect_to_ui))
        .route("/ui", get(redirect_to_ui))
        .route("/ui/", get(serve_index))
        .route("/ui/{*path}", get(serve_asset))
        .route("/login", get(login_link).post(login_form))
        .with_state(state)
}

/// The value of form field / query parameter `t`, percent-decoded.
fn login_param(encoded: &[u8]) -> Option<String> {
    url::form_urlencoded::parse(encoded)
        .find(|(name, _)| name == "t")
        .map(|(_, value)| value.into_owned())
}

/// `GET /login?t=<token>` — the link `rocky serve --ui` prints.
async fn login_link(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let presented = query.and_then(|q| login_param(q.as_bytes()));
    login_response(&state, &headers, presented.as_deref())
}

/// `POST /login` — the token field on the failure page, form field `t`.
///
/// Not behind the bearer layer (it is how a browser gets a session), but it
/// must carry an allowed `Origin`: present AND allowed, like a cookie write.
async fn login_form(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let Some(ui) = state.ui.as_ref() else {
        return ui_disabled();
    };
    let origin_ok = headers
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().ok())
        .is_some_and(|o| ui.origin_allowed(o, &state.allowed_origins));
    if !origin_ok {
        let body = serde_json::json!({
            "code": "origin_not_allowed",
            "message": "a login form post must carry this server's Origin",
            "remediation_hint": "submit the form on the page this server serves",
        });
        return no_store((StatusCode::FORBIDDEN, Json(body)).into_response());
    }
    let presented = login_param(&body);
    login_response(&state, &headers, presented.as_deref())
}

/// Check `presented` against the configured token: `303 /ui/` with the
/// session cookie, or the `401` page. Both carry `Cache-Control: no-store`
/// and `Referrer-Policy: no-referrer`, so the token in the URL is neither
/// cached nor sent on as a referrer.
fn login_response(state: &ServerState, headers: &HeaderMap, presented: Option<&str>) -> Response {
    if state.ui.is_none() {
        return ui_disabled();
    }
    let token = state.auth.as_ref();
    let matched = match (token, presented) {
        (Some(token), Some(presented)) => ui_session::login_token_matches(token, presented),
        (Some(_), None) | (None, _) => false,
    };
    let (Some(token), true) = (token, matched) else {
        let mut response = (
            StatusCode::UNAUTHORIZED,
            [(
                header::CONTENT_TYPE,
                HeaderValue::from_static("text/html; charset=utf-8"),
            )],
            LOGIN_FAILED_PAGE,
        )
            .into_response();
        apply_security_headers(response.headers_mut());
        return no_store(response);
    };
    // `Secure` when a TLS proxy in front says the browser used https. A
    // spoofed header can only make the browser withhold the cookie over
    // plain http (a failed login), never send it anywhere new.
    let secure = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("https"));
    let cookie = ui_session::set_cookie_header(&state.ui_session_key, token, secure);
    let mut response = Redirect::to("/ui/").into_response();
    if let Ok(value) = HeaderValue::from_str(&cookie) {
        response.headers_mut().insert(header::SET_COOKIE, value);
    }
    apply_security_headers(response.headers_mut());
    no_store(response)
}

fn no_store(mut response: Response) -> Response {
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::REFERRER_POLICY,
        HeaderValue::from_static("no-referrer"),
    );
    response
}

/// The page a wrong or stale login link gets. It echoes nothing from the
/// request. No inline script, so the UI's content security policy holds.
const LOGIN_FAILED_PAGE: &str = "<!doctype html>
<html lang=\"en\">
<head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>Rocky UI sign-in</title></head>
<body>
<main>
<h1>This link is stale or wrong</h1>
<p>Each start of <code>rocky serve --ui</code> makes a new token. Open the newest
<code>Rocky UI:</code> link from the console, or paste its token here.</p>
<form method=\"post\" action=\"/login\">
<label for=\"t\">Token</label>
<input id=\"t\" name=\"t\" type=\"password\" autocomplete=\"off\" required>
<button type=\"submit\">Sign in</button>
</form>
</main>
</body>
</html>
";

async fn redirect_to_ui() -> Redirect {
    Redirect::permanent("/ui/")
}

async fn serve_index(State(state): State<Arc<ServerState>>) -> Response {
    index_response(&state)
}

/// A file the build emitted, by its path; anything else is a client route
/// and gets the shell so a deep link loads — except under `assets/`, the
/// namespace every hashed file lives in, where a miss is a stale bundle and
/// a `404`.
///
/// The discriminator is the namespace, not the last segment. It used to be
/// "does the last segment contain a dot", which made every client route
/// whose last segment carries one a `404` instead of the page: a custody
/// link for a dotted model name (`/ui/governor/custody/corp.prod.orders`)
/// or for a freeze plan id, whose timestamp has fractional seconds
/// (`freeze:global:2026-09-15T21:00:00.123Z`). The SPA's own contract
/// (`engine/ui/src/router.ts`) has said "the server answers the shell for
/// every `/ui/*` path" since U2-P1; the server did not honour it (C3-P0).
/// The `assets/`-only layout is the build's, pinned by
/// `engine/ui/scripts/check-no-external.mjs`.
async fn serve_asset(State(state): State<Arc<ServerState>>, Path(path): Path<String>) -> Response {
    let Some(ui) = state.ui.as_ref() else {
        return ui_disabled();
    };
    let path = path.trim_start_matches('/');
    if let Some(file) = ui.file(path) {
        return file_response(file, path.starts_with("assets/"));
    }
    if !path.starts_with("assets/") {
        return index_response(&state);
    }
    let body = serde_json::json!({
        "code": "asset_not_found",
        "message": format!("no UI file at /ui/{path}"),
        "remediation_hint": "the UI's files are hashed; reload the page to pick up the current build",
    });
    with_security_headers((StatusCode::NOT_FOUND, Json(body)).into_response())
}

fn index_response(state: &ServerState) -> Response {
    let Some(ui) = state.ui.as_ref() else {
        return ui_disabled();
    };
    match ui.file("index.html") {
        Some(file) => file_response(file, false),
        None => {
            let body = serde_json::json!({
                "code": "ui_not_built",
                "message": "the embedded UI has no index.html",
                "remediation_hint": "rebuild rocky with `npm run build` in engine/ui and `--features ui`",
            });
            with_security_headers((StatusCode::INTERNAL_SERVER_ERROR, Json(body)).into_response())
        }
    }
}

fn ui_disabled() -> Response {
    let body = serde_json::json!({
        "code": "ui_disabled",
        "message": "this server was started without --ui",
        "remediation_hint": "start it with `rocky serve --ui`",
    });
    (StatusCode::NOT_FOUND, Json(body)).into_response()
}

/// The file with its type, the security headers, and a cache policy:
/// hashed assets are immutable, the shell is revalidated on every load.
fn file_response(file: UiFile, immutable: bool) -> Response {
    let cache = if immutable {
        "public, max-age=31536000, immutable"
    } else {
        "no-cache"
    };
    let mut response = (
        StatusCode::OK,
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static(file.content_type),
            ),
            (header::CACHE_CONTROL, HeaderValue::from_static(cache)),
        ],
        file.bytes.into_owned(),
    )
        .into_response();
    apply_security_headers(response.headers_mut());
    response
}

fn with_security_headers(mut response: Response) -> Response {
    apply_security_headers(response.headers_mut());
    response
}

fn apply_security_headers(headers: &mut header::HeaderMap) {
    for (name, value) in UI_SECURITY_HEADERS {
        headers.insert(
            header::HeaderName::from_static(name),
            HeaderValue::from_static(value),
        );
    }
}

/// Rewrite axum's plain-text `413` (a body over [`MAX_REQUEST_BODY_BYTES`])
/// into the error envelope, so no refusal on this API is bodiless.
pub(crate) async fn envelope_payload_too_large(response: Response) -> Response {
    if response.status() != StatusCode::PAYLOAD_TOO_LARGE {
        return response;
    }
    let body = serde_json::json!({
        "code": "payload_too_large",
        "message": format!("the request body exceeds the {MAX_REQUEST_BODY_BYTES}-byte limit"),
        "remediation_hint": "send a smaller body; a job request is a few hundred bytes",
    });
    (StatusCode::PAYLOAD_TOO_LARGE, Json(body)).into_response()
}
