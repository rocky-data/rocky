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
//! The page learns its token from the address `rocky serve --ui` prints,
//! `http://127.0.0.1:<port>/ui/#token=<secret>`: the fragment never reaches
//! the server, and the page clears it after reading it once.

use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use rocky_server::state::ServerState;
use rocky_server::ui::{UI_SECURITY_HEADERS, UiAssetSource, UiFile};

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
        .with_state(state)
}

async fn redirect_to_ui() -> Redirect {
    Redirect::permanent("/ui/")
}

async fn serve_index(State(state): State<Arc<ServerState>>) -> Response {
    index_response(&state)
}

/// A hashed asset by its path; a client route (no extension in its last
/// segment) gets the shell so a deep link loads; a missing file is a `404`.
async fn serve_asset(State(state): State<Arc<ServerState>>, Path(path): Path<String>) -> Response {
    let Some(ui) = state.ui.as_ref() else {
        return ui_disabled();
    };
    let path = path.trim_start_matches('/');
    if let Some(file) = ui.file(path) {
        return file_response(file, path.starts_with("assets/"));
    }
    let last = path.rsplit('/').next().unwrap_or(path);
    if !last.contains('.') {
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
        "remediation_hint": "start it with `rocky serve --ui --token <secret> --token-scope read-only`",
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
