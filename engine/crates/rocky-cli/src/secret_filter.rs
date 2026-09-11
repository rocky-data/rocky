//! The outermost response filter: no registered secret leaves `rocky serve`.
//!
//! `${VAR}` is expanded before the config is parsed, so a resolved value is
//! ordinary bytes by the time a producer carries it. Auditing every producer
//! does not terminate — there are eight substitution sites in `models.rs`
//! alone, twenty-odd `ConfigError` variants interpolating a `String`, and a
//! successful `GET /api/v1/policy` that copies resolved scope fields with no
//! error involved at all. So the guarantee is made at the boundary instead:
//! whatever any producer built, the bytes on the wire carry no registered
//! value.
//!
//! ## Why outermost, and what that buys
//!
//! This is the LAST layer applied in [`crate::api::router`], so on the
//! response path it runs after every other layer. That covers what a filter
//! installed at a responder type cannot:
//!
//! ```text
//!   413 payload_too_large   (rewritten by a map_response layer)
//!   421 host_not_allowed    (require_known_host, outside the bearer layer)
//!   401 / 403               (require_bearer_token)
//!   405 / 404 fallbacks
//!   a handler that builds its own Response instead of using PrettyJson
//! ```
//!
//! `POST /api/v1/compile` is the last of those and it is not hypothetical: it
//! hand-builds its body with axum's `Json` and inserts `config_error` verbatim.
//! A filter at `PrettyJson` would have missed it while looking correct.
//!
//! ## JSON only, deliberately
//!
//! The filter rewrites bodies whose `content-type` is JSON and passes
//! everything else through untouched. With `--ui` the same router serves the
//! embedded JS, CSS and fonts, and a byte-level replacement inside a minified
//! bundle that happened to contain a registered value would corrupt the asset
//! silently — plausible, since the 8-byte floor admits short sequences.
//!
//! That is safe because the UI files carry no config text: `crate::ui`'s
//! `index_response` serves the embedded `index.html` byte for byte, and
//! `serve_asset` serves hashed files, with no templating and no state
//! injection. The only dynamic text in that module is fixed literals and an
//! echo of the requested path. Anything config-derived reaches the browser
//! through the JSON API, which this filter covers.
//!
//! ## Redact BEFORE you truncate
//!
//! A rule for every site that shortens a string which can reach a response.
//! This filter matches WHOLE values, so a cut that lands mid-value leaves a
//! prefix it cannot see, and a partial credential ships. `truncate_error` in
//! `commands::resilience` follows the rule; [`redact_truncated_tail`] is the
//! backstop for records an older binary already wrote.
//!
//! ## Fail closed
//!
//! A body that cannot be read, is not UTF-8, or no longer parses as JSON after
//! replacement is answered with the `secret_redaction_unavailable` envelope
//! rather than forwarded. The filter refuses on doubt, not only on a match.

use axum::body::Body;
use axum::extract::Request;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rocky_core::secret_registry;

/// Bodies larger than this are refused rather than buffered.
///
/// The filter has to hold the whole body to scan it. A response is a rendered
/// document — the largest realistic one is a full DAG or a run list, far below
/// this — so the cap is a guard against unbounded memory, not a limit anyone
/// should meet. Meeting it fails closed.
const MAX_SCANNED_BODY_BYTES: usize = 64 * 1024 * 1024;

/// The response when filtering could not be completed.
///
/// Deliberately fixed text: it must not echo anything about the body it
/// refused, since that body is the thing suspected of carrying a secret.
fn redaction_unavailable() -> Response {
    let body = serde_json::json!({
        "code": "secret_redaction_unavailable",
        "message": "the response could not be checked for resolved environment values, so it was withheld",
        "remediation_hint": "retry; if this persists, check the server log for the response that could not be read",
    });
    (StatusCode::INTERNAL_SERVER_ERROR, axum::Json(body)).into_response()
}

/// Whether this response's body is JSON we should scan.
fn is_json(response: &Response) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            let v = v.split(';').next().unwrap_or(v).trim();
            v.eq_ignore_ascii_case("application/json")
                || (v.starts_with("application/") && v.ends_with("+json"))
        })
}

/// Rewrite every registered value in `text` to its `${NAME}`.
///
/// Each value is replaced in two forms: raw, and as `serde_json` would escape
/// it inside a string. The escaped pass is what catches a secret containing a
/// quote, a backslash or a newline — serde writes `a"b` as `a\"b`, which a raw
/// search would walk straight past.
///
/// Longest value first. Replacing a shorter value that is contained in a
/// longer one first would leave the longer one's tail behind — a leak the
/// filter itself created.
pub fn redact(text: &str) -> String {
    let mut out = text.to_string();
    for (value, replacement) in secret_registry::substitutions() {
        out = out.replace(&value, &replacement);

        // `to_string` on a &str yields a quoted JSON string; the interior is
        // the escaped form that actually appears in a serialized body.
        let escaped_value = serde_json::to_string(&value).unwrap_or_default();
        let escaped_value = escaped_value.trim_matches('"');
        if !escaped_value.is_empty() && escaped_value != value {
            let escaped_replacement = serde_json::to_string(&replacement).unwrap_or_default();
            let escaped_replacement = escaped_replacement.trim_matches('"').to_string();
            out = out.replace(escaped_value, &escaped_replacement);
        }
    }
    out
}

/// The shortest truncated tail that is rewritten.
///
/// Distinct from [`secret_registry::SECRET_LENGTH_FLOOR`] and deliberately so:
/// the floor decides which VALUES are secret; this decides how much of a known
/// secret may survive a cut. Anchored at the truncation sentinel, a short
/// minimum over-redacts rarely, so the worst case is 3 leaked bytes instead of
/// 7.
const MIN_TRUNCATED_TAIL: usize = 4;

/// The marker [`crate::commands::resilience`] appends when it shortens an
/// error. Its presence is what makes a tail interpretable as a cut rather than
/// as ordinary prose.
const TRUNCATION_SENTINEL: char = '…';

/// Rewrite a secret PREFIX left behind by a truncation that happened before
/// [`redact`] ran.
///
/// Records written by an older binary hold `…`-terminated errors that were cut
/// mid-value, and [`redact`] cannot match them because it searches for whole
/// values. This reads the bytes immediately before the sentinel — a cut we
/// made ourselves — and asks whether they are the start of a registered value.
///
/// Deliberately NOT a general fragment search. Looking for pieces of secrets
/// anywhere in prose cannot distinguish a fragment from ordinary text; this is
/// anchored, so a false positive needs a string that both ends in the sentinel
/// and whose tail begins a registered value.
pub fn redact_truncated_tail(text: &str) -> String {
    if !text.contains(TRUNCATION_SENTINEL) {
        return text.to_string();
    }
    let pairs = secret_registry::substitutions();
    let segments: Vec<&str> = text.split(TRUNCATION_SENTINEL).collect();
    let last = segments.len() - 1;
    let mut out = String::with_capacity(text.len());
    for (i, segment) in segments.iter().enumerate() {
        if i > 0 {
            out.push(TRUNCATION_SENTINEL);
        }
        out.push_str(segment);
        // Only a segment FOLLOWED by the sentinel ends at a cut. The final
        // segment is ordinary trailing text — `truncate_detail` writes
        // `…  [evidence truncated at N bytes]`, so examining its tail would
        // over-redact every string that merely contains a sentinel.
        if i == last {
            continue;
        }
        // The tail of THIS segment is what the sentinel cut. Longest first, so
        // a longer surviving prefix wins over a shorter one.
        let Some((value, replacement)) = longest_tail_prefix(segment, &pairs) else {
            continue;
        };
        let keep = out.len() - value.len();
        out.truncate(keep);
        out.push_str(replacement);
    }
    out
}

/// The longest tail of `segment` that begins some registered value, at least
/// [`MIN_TRUNCATED_TAIL`] bytes.
fn longest_tail_prefix<'a>(
    segment: &str,
    pairs: &'a [(String, String)],
) -> Option<(String, &'a str)> {
    let mut best: Option<(String, &str)> = None;
    for (value, replacement) in pairs {
        // A whole value is `redact`'s job, so the tail must be a STRICT
        // prefix — hence `value.len() - 1`. But it may be the whole segment:
        // the cut can land anywhere, including right where the value began.
        // Subtracting from the MIN instead would miss exactly that case.
        let max = (value.len() - 1).min(segment.len());
        let mut take = max;
        while take >= MIN_TRUNCATED_TAIL {
            if segment.is_char_boundary(segment.len() - take)
                && segment.ends_with(&value[..take])
                && best.as_ref().is_none_or(|(b, _)| take > b.len())
            {
                best = Some((value[..take].to_string(), replacement.as_str()));
                break;
            }
            take -= 1;
        }
    }
    best
}

/// The middleware. Applied last in [`crate::api::router`], so it is outermost.
pub async fn redact_response_secrets(request: Request, next: Next) -> Response {
    let response = next.run(request).await;

    // Nothing was ever expanded from the environment: there is nothing this
    // filter could match, so skip the buffering entirely.
    if secret_registry::is_empty() {
        return response;
    }
    if !is_json(&response) {
        return response;
    }

    let (mut parts, body) = response.into_parts();
    let bytes = match axum::body::to_bytes(body, MAX_SCANNED_BODY_BYTES).await {
        Ok(bytes) => bytes,
        // Unreadable, or over the cap. Either way the body cannot be checked,
        // so it is not forwarded.
        Err(_) => return redaction_unavailable(),
    };
    let Ok(text) = std::str::from_utf8(&bytes) else {
        // A JSON body is UTF-8 by definition; one that is not cannot be
        // scanned, and guessing is not an option here.
        return redaction_unavailable();
    };

    let redacted = redact_truncated_tail(&redact(text));

    // `redact` is a TEXT replacement and knows nothing about JSON structure,
    // so a registered value in a non-string position — a numeric field fed
    // from `${VAR}` — becomes `{"account_id":${ACCOUNT_ID}}`, which is not
    // JSON. The body parsed on the way in, so if it does not parse now the
    // replacement broke it. Refuse rather than ship something unparseable.
    //
    // Not fixed by walking the parsed value instead: `PrettyJson` emits
    // `to_string_pretty` and the API's byte-parity tests compare those bytes
    // against the CLI's, so re-serializing risks key order and whitespace.
    if serde_json::from_str::<serde_json::Value>(&redacted).is_err() {
        return redaction_unavailable();
    }

    // The body length changes whenever a replacement lands, and a stale
    // content-length makes the response unparseable to the client.
    parts.headers.remove(header::CONTENT_LENGTH);
    parts.headers.insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(redacted.len() as u64),
    );
    Response::from_parts(parts, Body::from(redacted))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocky_core::secret_registry::register_substitution;

    #[test]
    fn a_registered_value_becomes_its_variable_name() {
        register_substitution("ROCKY_FILTER_TOKEN", "FILTER-SECRET-8e26660e-aaaa");
        let out = redact(r#"{"message":"failed: FILTER-SECRET-8e26660e-aaaa"}"#);
        assert!(!out.contains("FILTER-SECRET-8e26660e-aaaa"), "{out}");
        assert!(out.contains("${ROCKY_FILTER_TOKEN}"), "{out}");
    }

    /// The escaping case #1897 names. A raw substring search walks past a
    /// value that serde had to escape, which is a silent leak.
    #[test]
    fn a_value_containing_a_quote_is_caught_in_its_escaped_form() {
        let raw = r#"FILTER-QUOTE-"b-8e26660e"#;
        register_substitution("ROCKY_FILTER_QUOTE", raw);

        // Exactly how the value appears once serde has written it into a body.
        let body =
            serde_json::to_string(&serde_json::json!({ "message": raw })).expect("serializes");
        assert!(
            body.contains(r#"FILTER-QUOTE-\"b-8e26660e"#),
            "PRECONDITION: serde must have escaped the quote: {body}"
        );

        let out = redact(&body);
        assert!(
            !out.contains(r#"FILTER-QUOTE-\"b-8e26660e"#),
            "the escaped form must be caught: {out}"
        );
        assert!(out.contains("${ROCKY_FILTER_QUOTE}"), "{out}");
    }

    /// The line-splitting case. A multi-line value is contiguous once serde
    /// escapes the newline, so the escaped pass catches it.
    #[test]
    fn a_multi_line_value_is_caught_once_serde_escapes_the_newline() {
        let raw = "FILTER-LINE-8e26660e\nsecond-line-of-the-secret";
        register_substitution("ROCKY_FILTER_MULTILINE", raw);

        let body =
            serde_json::to_string(&serde_json::json!({ "message": raw })).expect("serializes");
        assert!(
            body.contains("FILTER-LINE-8e26660e\\nsecond-line-of-the-secret"),
            "PRECONDITION: the newline must be escaped: {body}"
        );

        let out = redact(&body);
        assert!(
            !out.contains("second-line-of-the-secret"),
            "a split value must not survive: {out}"
        );
    }

    /// ui-revamp's overlapping-pair case, at the level where it matters: the
    /// REPLACEMENT, not just the ordering. Shortest-first would rewrite the
    /// inner value and leave the longer secret's tail on the wire.
    #[test]
    fn an_overlapping_pair_leaves_no_fragment_of_the_longer_secret() {
        let short = "FILTER-OVERLAP-8e26660e";
        let long = "FILTER-OVERLAP-8e26660e-AND-THE-REST-OF-IT";
        assert!(long.contains(short), "PRECONDITION: the pair overlaps");
        register_substitution("ROCKY_FILTER_SHORT", short);
        register_substitution("ROCKY_FILTER_LONG", long);

        let out = redact(&format!(r#"{{"message":"{long}"}}"#));
        assert!(
            !out.contains("-AND-THE-REST-OF-IT"),
            "the longer secret's tail survived — the redactor created this leak: {out}"
        );
    }

    /// The collision Hugo's floor ruling accepts, pinned so it is a decision
    /// and not a surprise: a non-secret value at or above the floor is
    /// rewritten inside a longer identifier too.
    #[test]
    fn a_value_inside_a_longer_identifier_is_rewritten_and_names_its_variable() {
        register_substitution("ROCKY_FILTER_CATALOG", "filteranalytics");
        let out = redact(r#"{"model":"filteranalytics_orders"}"#);
        assert_eq!(
            out, r#"{"model":"${ROCKY_FILTER_CATALOG}_orders"}"#,
            "mangling is the accepted cost; naming the variable is what keeps \
             the diagnostic usable"
        );
    }

    #[test]
    fn a_value_below_the_floor_is_left_alone() {
        register_substitution("ROCKY_FILTER_SHORTVAL", "abc");
        let out = redact(r#"{"message":"abc is shown"}"#);
        assert!(out.contains("abc is shown"), "{out}");
    }

    /// #1897. A fragment left by a truncation that happened BEFORE the
    /// redaction — i.e. a record an older binary wrote — is rewritten.
    /// `redact` cannot match it, because it searches for whole values.
    #[test]
    fn a_truncated_prefix_before_the_sentinel_is_rewritten() {
        let secret = "FILTER-PREFIX-8e26660e-THE-REST-IS-CUT";
        register_substitution("ROCKY_FILTER_PREFIX", secret);

        // Exactly what an older binary stored: the value cut mid-way, then
        // the sentinel.
        let stored = format!("{}…", &secret[..20]);
        assert!(
            !stored.contains(secret),
            "PRECONDITION: the whole value must NOT be present, or this test \
             is only exercising `redact`"
        );

        let out = redact_truncated_tail(&stored);
        assert!(
            !out.contains(&secret[..20]),
            "the surviving prefix must be gone: {out}"
        );
        assert!(out.contains("${ROCKY_FILTER_PREFIX}"), "{out}");
    }

    /// The final segment is ordinary trailing text, not a cut. This is the
    /// shape `truncate_detail` writes — `… [evidence truncated at N bytes]` —
    /// and examining its tail would over-redact any string that merely
    /// contains a sentinel.
    #[test]
    fn text_after_the_last_sentinel_is_not_treated_as_a_cut() {
        let secret = "FILTER-TRAILING-8e26660e-VALUE";
        register_substitution("ROCKY_FILTER_TRAILING", secret);

        // The tail here IS a prefix of the registered value, but it is not
        // followed by a sentinel, so it is not a cut.
        let stored = format!("something…{}", &secret[..12]);
        let out = redact_truncated_tail(&stored);
        assert_eq!(
            out, stored,
            "trailing text after the last sentinel must be left alone"
        );
    }

    /// A tail shorter than the minimum is left alone: below it, a
    /// coincidental match is likelier than a real cut.
    #[test]
    fn a_tail_shorter_than_the_minimum_is_left_alone() {
        let secret = "FILTER-SHORTTAIL-8e26660e";
        register_substitution("ROCKY_FILTER_SHORTTAIL", secret);
        let stored = format!("{}…", &secret[..MIN_TRUNCATED_TAIL - 1]);
        assert_eq!(redact_truncated_tail(&stored), stored);
    }

    #[test]
    fn json_content_types_are_recognised_including_suffixed_ones() {
        let json = Response::builder()
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap();
        assert!(is_json(&json));

        let charset = Response::builder()
            .header(header::CONTENT_TYPE, "application/json; charset=utf-8")
            .body(Body::empty())
            .unwrap();
        assert!(is_json(&charset), "a charset parameter must not defeat it");

        let problem = Response::builder()
            .header(header::CONTENT_TYPE, "application/problem+json")
            .body(Body::empty())
            .unwrap();
        assert!(is_json(&problem));

        let js = Response::builder()
            .header(header::CONTENT_TYPE, "text/javascript")
            .body(Body::empty())
            .unwrap();
        assert!(
            !is_json(&js),
            "a UI bundle must not be byte-replaced — that corrupts the asset"
        );
    }
}
