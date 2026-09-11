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

/// The escaped forms of a value that can appear in a response body.
///
/// **Two levels, and that is a stated limit rather than a closed class.**
/// A value can be escaped more than once on its way to the wire: a
/// `ConfigError` variant `Debug`-escapes its field (`config.rs:333`), and the
/// response then JSON-serializes that diagnostic. A secret containing a
/// backslash therefore arrives doubly escaped, and a search for the raw and
/// singly-escaped forms walks straight past it.
///
/// These four forms cover the raw value and every combination of those two
/// layers. A third escaping layer, if one is ever introduced, would pass —
/// which is why this is documented as a depth rather than as "escaping is
/// handled".
fn escaped_forms(value: &str) -> Vec<String> {
    let json = |v: &str| {
        serde_json::to_string(v)
            .ok()
            .map(|q| q.trim_matches('"').to_string())
            .unwrap_or_default()
    };
    let debug = format!("{value:?}");
    let debug = debug.trim_matches('"').to_string();

    let mut forms = vec![value.to_string(), json(value), debug.clone(), json(&debug)];
    forms.retain(|f| !f.is_empty());
    forms.sort();
    forms.dedup();
    forms
}

/// What is emitted when a variable NAME would itself disclose a value.
///
/// The ordinary replacement is `${NAME}`, which is useful because it tells an
/// operator which variable to look up. But a NAME can contain another
/// registered value — `ROCKY_<some other secret>` — and emitting it would
/// disclose that value in the act of hiding this one. Rare, and cheap to
/// close: fall back to a token that names nothing.
const NEUTRAL_REPLACEMENT: &str = "${REDACTED}";

/// The replacement to emit for a match, with the name dropped when the name
/// itself carries a registered value (#1897, Codex C4).
fn safe_replacement(replacement: &str, pairs: &[(String, String)]) -> String {
    if pairs
        .iter()
        .any(|(value, _)| replacement.contains(value.as_str()))
    {
        return NEUTRAL_REPLACEMENT.to_string();
    }
    replacement.to_string()
}

/// One matched span of one registered value.
struct Span {
    start: usize,
    end: usize,
    replacement: String,
}

/// Rewrite every registered value in `text` to its `${NAME}`.
///
/// **One pass over merged spans, not a sequence of replacements.** Replacing
/// values one at a time is wrong in three separate ways, all reported against
/// the earlier implementation:
///
/// ```text
/// overlap        A=ABCDEFGH1234, B=12345678XYZ, body ABCDEFGH12345678XYZ
///                replacing A first destroys B's prefix -> 7 bytes of B ship
/// reintroduction a replacement ${NAME} can itself contain a value whose
///                pass has already run
/// escaping       a raw match can consume a value plus the escape slash that
///                followed it, leaving invalid JSON
/// ```
///
/// Collecting every match of every form as a span, merging overlaps, and
/// rewriting each merged span once fixes all three. Overlapping secrets are
/// replaced as a UNIT, so an overlap over-redacts rather than leaking the
/// part that was not covered.
pub fn redact(text: &str) -> String {
    let pairs = secret_registry::substitutions();
    if pairs.is_empty() {
        return text.to_string();
    }

    let mut spans: Vec<Span> = Vec::new();
    for (value, replacement) in &pairs {
        for form in escaped_forms(value) {
            let mut from = 0;
            while let Some(found) = text[from..].find(&form) {
                let start = from + found;
                spans.push(Span {
                    start,
                    end: start + form.len(),
                    replacement: safe_replacement(replacement, &pairs),
                });
                // Advance by one byte, not by the match length: overlapping
                // occurrences of the SAME form must all be found, and the
                // merge below is what collapses them.
                from = start + 1;
                if from >= text.len() {
                    break;
                }
            }
        }
    }
    if spans.is_empty() {
        return text.to_string();
    }

    spans.sort_by_key(|s| (s.start, std::cmp::Reverse(s.end)));

    let mut out = String::with_capacity(text.len());
    let mut cursor = 0usize;
    let mut i = 0usize;
    while i < spans.len() {
        let mut end = spans[i].end;
        let start = spans[i].start;
        // Every replacement whose span touches this run, in first-seen order,
        // so an overlap names both variables rather than silently dropping
        // one.
        let mut names: Vec<String> = vec![spans[i].replacement.clone()];
        let mut j = i + 1;
        while j < spans.len() && spans[j].start < end {
            if spans[j].end > end {
                end = spans[j].end;
            }
            if !names.contains(&spans[j].replacement) {
                names.push(spans[j].replacement.clone());
            }
            j += 1;
        }
        if start >= cursor {
            out.push_str(&text[cursor..start]);
            out.push_str(&names.concat());
            cursor = end;
        } else if end > cursor {
            // A run that began inside an already-rewritten region: keep the
            // tail covered rather than re-emitting the raw bytes.
            out.push_str(&names.concat());
            cursor = end;
        }
        i = j;
    }
    out.push_str(&text[cursor..]);
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
            // BOTH sides need a boundary check. `value[..take]` panics inside
            // a multi-byte character just as surely as slicing the segment
            // does — the same defect this filter fixes in `truncate_error`,
            // reintroduced here in the checking code (#1897).
            if value.is_char_boundary(take)
                && segment.is_char_boundary(segment.len() - take)
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
    // Axum answers HEAD on every `get(...)` route and strips the body inside
    // the router, so this layer sees an empty body with a JSON content type.
    // Parsing "" fails, and the fail-closed branch would manufacture a 500 —
    // wrong status, wrong body and wrong length on every HEAD as soon as
    // anything is registered. A body with no bytes carries no value.
    let is_head = request.method() == axum::http::Method::HEAD;
    let response = next.run(request).await;
    if is_head {
        return response;
    }

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
    // An empty body is not a failure to read one: 204, 304 and a stripped
    // response all reach here with nothing to scan.
    if bytes.is_empty() {
        return Response::from_parts(parts, Body::from(bytes));
    }
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

    /// Codex C3, its exact example. Two registered values that OVERLAP
    /// without either containing the other.
    ///
    /// Sequential replacement redacted `A` and resumed after it, destroying
    /// `B`'s prefix and shipping its last seven bytes. Longest-first ordering
    /// does not help — the leak is not a sequencing artefact. Merging the
    /// spans and replacing them as a unit is what fixes it, and it errs
    /// toward over-redaction.
    #[test]
    fn two_overlapping_values_leave_neither_partially_exposed() {
        let a = "ABCDEFGH1234";
        let b = "12345678XYZ";
        assert!(!a.contains(b) && !b.contains(a), "PRECONDITION: not nested");
        register_substitution("ROCKY_OVERLAP_A", a);
        register_substitution("ROCKY_OVERLAP_B", b);

        let out = redact(r#"{"m":"ABCDEFGH12345678XYZ"}"#);
        assert!(!out.contains("5678XYZ"), "B's tail survived: {out}");
        assert!(!out.contains("ABCDEFGH"), "A survived: {out}");
    }

    /// Codex C2. A value ending in a backslash: the raw form consumed the
    /// value plus the escape slash that followed it, leaving a dangling
    /// quote and invalid JSON — which the validity check then turned into a
    /// 500 on an otherwise fine response.
    #[test]
    fn a_value_ending_in_a_backslash_leaves_valid_json() {
        let secret = "ABCDEFGH\\";
        register_substitution("ROCKY_TRAILING_SLASH", secret);

        let body =
            serde_json::to_string(&serde_json::json!({ "message": secret })).expect("serializes");
        let out = redact(&body);

        assert!(!out.contains(secret), "the value survived: {out}");
        serde_json::from_str::<serde_json::Value>(&out)
            .unwrap_or_else(|e| panic!("redaction produced invalid JSON ({e}): {out}"));
    }

    /// Codex C1. A value escaped TWICE on its way to the wire: a
    /// `ConfigError` variant `Debug`-escapes its field, and the response then
    /// JSON-serializes that diagnostic. A search for the raw and singly
    /// escaped forms walks past the result.
    #[test]
    fn a_doubly_escaped_value_is_still_caught() {
        let secret = "ABCD\\EFGH-8e26660e";
        register_substitution("ROCKY_DOUBLE_ESCAPE", secret);

        // Exactly the shape: Debug first, then JSON.
        let diagnostic = format!("invalid window {secret:?}");
        let body = serde_json::to_string(&serde_json::json!({ "message": diagnostic }))
            .expect("serializes");

        let out = redact(&body);
        assert!(
            out.contains("${ROCKY_DOUBLE_ESCAPE}"),
            "the doubly-escaped form was not matched: {out}"
        );
    }

    /// Codex C4. A replacement can itself contain a registered value when a
    /// VARIABLE NAME equals another value. With sequential passes the later
    /// replacement reintroduced a value whose pass had already run; one pass
    /// over merged spans never re-scans emitted text.
    #[test]
    fn a_replacement_cannot_reintroduce_an_already_scanned_value() {
        let first = "ABCDEFGHIJ-c4";
        register_substitution("ROCKY_C4_FIRST", first);
        // A variable whose NAME contains the other value.
        register_substitution(&format!("ROCKY_{first}"), "ZZZZZZZZ-c4");

        let out = redact(r#"{"m":"ZZZZZZZZ-c4"}"#);
        assert!(
            !out.contains(first),
            "the replacement reintroduced an already-scanned value: {out}"
        );
    }

    /// Codex C5. `longest_tail_prefix` sliced `value[..take]` without
    /// checking the VALUE's char boundary — a panic, not a leak, and the
    /// same defect this filter fixes in `truncate_error`.
    #[test]
    fn a_multibyte_value_does_not_panic_the_tail_search() {
        register_substitution("ROCKY_C5_MULTIBYTE", "abcdéXYZ-8e26660e");
        // An ASCII tail that matches nothing: the loop must walk the whole
        // range and return, crossing the multi-byte boundary on the way.
        let out = redact_truncated_tail("some unrelated ascii tail…");
        assert_eq!(out, "some unrelated ascii tail…");
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
