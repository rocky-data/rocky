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
///
/// **Last resort: if this envelope ITSELF carries a registered value, the
/// bare status is sent with no body at all.** An operator's value can collide
/// with this fixed text — `secret_redaction_unavailable` is a registerable
/// string — and this response is built by the outermost layer, so nothing
/// scans it afterwards.
///
/// Not solved by refusing to register colliding values. That would mean one
/// rare collision leaves a real secret unregistered, and therefore unredacted
/// in EVERY response for the life of the process — fail-open, on the one
/// mechanism whose job is to fail closed. An empty body cannot leak, and
/// there is no third level to recurse into.
fn redaction_unavailable() -> Response {
    let body = serde_json::json!({
        "code": "secret_redaction_unavailable",
        "message": "the response could not be checked for resolved environment values, so it was withheld",
        "remediation_hint": "retry; if this persists, check the server log for the response that could not be read",
    });
    let rendered = serde_json::to_string(&body).unwrap_or_default();
    if rendered.is_empty() || any_value_survives(&rendered) {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
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

    let spans = discover_spans(text, &pairs);
    if spans.is_empty() {
        return text.to_string();
    }
    rewrite_merged(text, spans)
}

/// Every occurrence of every form, with overlapping matches of the SAME form
/// already collapsed.
///
/// Separate from [`redact`] because the collapse is invisible in the output:
/// the merge step produces identical bytes whether or not this collapses, so
/// only the span COUNT distinguishes them, and a test needs to see it. That
/// count is the difference between one span and ~62.9M for a
/// repeated-character value in a large body.
fn discover_spans(text: &str, pairs: &[(String, String)]) -> Vec<Span> {
    let mut spans: Vec<Span> = Vec::new();
    for (value, replacement) in pairs {
        let replacement = safe_replacement(replacement, pairs);
        for form in escaped_forms(value) {
            let mut from = 0;
            // The open run of this form, collapsed as it is found.
            //
            // Advancing one character at a time finds every overlapping
            // occurrence, which is needed — but RETAINING each one is what
            // made a repeated-character value catastrophic: `AAAAAAAA` in a
            // 60 MiB run of `A` produced ~62.9M spans, about 2.3 GiB, inside
            // a 64 MiB body cap. The cap bounds the body, not the number of
            // matches derived from it.
            //
            // Matches of one form arrive in increasing order, so an
            // overlapping one extends the open run instead of adding a span.
            // A run of `A` collapses to a single span whatever its length.
            let mut run: Option<(usize, usize)> = None;
            while let Some(found) = text[from..].find(&form) {
                let start = from + found;
                let end = start + form.len();
                run = match run {
                    Some((s, e)) if start <= e => Some((s, e.max(end))),
                    Some((s, e)) => {
                        spans.push(Span {
                            start: s,
                            end: e,
                            replacement: replacement.clone(),
                        });
                        Some((start, end))
                    }
                    None => Some((start, end)),
                };
                // Advance past this match's first CHARACTER, not its first
                // byte. Overlapping occurrences of the same form must all be
                // found — the merge below collapses them — but `start + 1`
                // lands inside a multi-byte character whenever a value begins
                // with one, and the next `text[from..]` panics.
                //
                // Third instance of this class in this change. Byte arithmetic
                // on a `&str` needs a boundary check every time, and a
                // registered value is arbitrary UTF-8 from the environment.
                from = start + 1;
                while from < text.len() && !text.is_char_boundary(from) {
                    from += 1;
                }
                if from >= text.len() {
                    break;
                }
            }
            if let Some((s, e)) = run {
                spans.push(Span {
                    start: s,
                    end: e,
                    replacement: replacement.clone(),
                });
            }
        }
    }
    spans
}

/// Merge overlapping spans and rewrite each run once.
fn rewrite_merged(text: &str, mut spans: Vec<Span>) -> String {
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

/// Whether any registered value survives in a finished body.
///
/// **The backstop, and the reason there is one.** Every place that GENERATES a
/// replacement is a chance to emit something that still carries a value: a
/// neutral token that is itself a registered value, a variable name that
/// contains one, a name whose escaped form decodes to one, or a path that
/// forgets to call [`safe_replacement`] at all. Each of those was a separate
/// reported defect, and patching them one at a time closes only the ones
/// somebody found.
///
/// So the finished body is checked rather than trusted. If a value survives,
/// the caller refuses the response — which holds no matter how a replacement
/// was produced, including by code written after this.
///
/// Checked in two forms, and **the decoded half is load-bearing** — I
/// believed it redundant and was wrong:
///
/// - the **wire form**, as the bytes about to be sent;
/// - every **JSON string value**, decoded.
///
/// The case that settles it: a value `ABCDEFGH/` in a body
/// `{"m":"ABCDEFGH\/"}`. JSON permits `\/` as an escape for `/`, but neither
/// `serde_json` nor Rust `Debug` ever GENERATES it, so every form
/// [`escaped_forms`] produces collapses to the unescaped value and none of
/// them is in the wire bytes. Decoding recovers it.
///
/// The reasoning that missed it is worth recording: I checked which escapes
/// our SERIALIZER emits and concluded no other form could appear. But this
/// filter reads bodies, and a body may contain any escape the JSON SPEC
/// allows. Generalising from the writer to the reader is the error.
///
/// Read-only: the body is parsed to inspect it and never re-serialized, so
/// the byte-parity between this API and the CLI is untouched.
pub fn any_value_survives(body: &str) -> bool {
    let pairs = secret_registry::substitutions();
    if pairs.is_empty() {
        return false;
    }

    // Built ONCE. Rebuilding per decoded string turned a 300 KiB body with
    // 100,000 strings and a 1,000-entry registry into ~100M `escaped_forms`
    // calls, each allocating four owned strings — a CPU and allocator denial
    // of service on an ordinary-looking response.
    let forms: Vec<String> = pairs
        .iter()
        .flat_map(|(value, _)| escaped_forms(value))
        .collect();

    let survives_in = |text: &str| forms.iter().any(|form| text.contains(form.as_str()));

    if survives_in(body) {
        return true;
    }

    // The decoded view. `${ROCKY_A\"B}` is not the value `A"B` in the wire
    // bytes, but it is once a client parses the JSON.
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) else {
        // Unparseable here means the caller is about to refuse anyway.
        return false;
    };
    let mut found = false;
    walk_strings(&parsed, &mut |text| {
        if !found && survives_in(text) {
            found = true;
        }
    });
    found
}

/// Visit every string in a JSON document, including object keys.
fn walk_strings(value: &serde_json::Value, visit: &mut impl FnMut(&str)) {
    match value {
        serde_json::Value::String(text) => visit(text),
        serde_json::Value::Array(items) => {
            for item in items {
                walk_strings(item, visit);
            }
        }
        serde_json::Value::Object(map) => {
            for (key, item) in map {
                visit(key);
                walk_strings(item, visit);
            }
        }
        serde_json::Value::Null | serde_json::Value::Bool(_) | serde_json::Value::Number(_) => {}
    }
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
        // Through the same generator as `redact`, so a name that carries a
        // value cannot be emitted here either. Two replacement paths meant
        // two chances to be wrong; there is one now.
        out.push_str(&safe_replacement(replacement, &pairs));
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
    // Every FORM of the value, not just the raw one. A pre-filter truncation
    // stored `ABCD\"EFGH…`, whose tail is a prefix of the value's ESCAPED
    // form; comparing only raw prefixes left the decoded `ABCD"EFGH` on the
    // wire, contradicting this helper's whole purpose.
    let pairs: Vec<(String, &str)> = pairs
        .iter()
        .flat_map(|(value, replacement)| {
            escaped_forms(value)
                .into_iter()
                .map(move |form| (form, replacement.as_str()))
        })
        .collect();
    for (value, replacement) in &pairs {
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
                best = Some((value[..take].to_string(), replacement));
                break;
            }
            take -= 1;
        }
    }
    best
}

/// The middleware. Applied last in [`crate::api::router`], so it is outermost.
pub async fn redact_response_secrets(request: Request, next: Next) -> Response {
    // PRECAUTIONARY, and it does not currently fire. Axum 0.8.9 answers HEAD
    // on every `get(...)` route but strips the body at the top-level
    // `RouteFuture`, OUTSIDE this layer — so the filter sees the full body and
    // the empty-body path is never reached on HEAD. A reported defect claiming
    // otherwise did not reproduce (`a_head_request_is_not_turned_into_a_
    // manufactured_error`). Kept in case a future axum moves the stripping
    // inward; not mutation-checked, because there is no failure to pin.
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
    // An empty body is not a failure to read one. No current route produces
    // an empty-bodied JSON response, so this is precautionary too — but it is
    // one comparison, and the alternative is a 500 manufactured from nothing.
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

    // Fail closed on the finished body. Everything above is replacement
    // GENERATION, and each generator is a chance to emit something that still
    // carries a value. This is the check that does not depend on any of them
    // being right.
    if any_value_survives(&redacted) {
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
        assert!(
            !out.contains("FILTER-SECRET-8e26660e-aaaa"),
            "redacted body, {} bytes",
            out.len()
        );
        assert!(
            out.contains("${ROCKY_FILTER_TOKEN}"),
            "redacted body, {} bytes",
            out.len()
        );
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
            "PRECONDITION: serde must have escaped the quote: body of {} bytes",
            body.len()
        );

        let out = redact(&body);
        assert!(
            !out.contains(r#"FILTER-QUOTE-\"b-8e26660e"#),
            "the escaped form must be caught: body of {} bytes",
            out.len()
        );
        assert!(
            out.contains("${ROCKY_FILTER_QUOTE}"),
            "redacted body, {} bytes",
            out.len()
        );
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
            "PRECONDITION: the newline must be escaped: body of {} bytes",
            body.len()
        );

        let out = redact(&body);
        assert!(
            !out.contains("second-line-of-the-secret"),
            "a split value must not survive: body of {} bytes",
            out.len()
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
            "the longer secret's tail survived — the redactor created this leak: body of {} bytes",
            out.len()
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
        assert!(
            out.contains("abc is shown"),
            "redacted body, {} bytes",
            out.len()
        );
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
            "the surviving prefix must be gone: body of {} bytes",
            out.len()
        );
        assert!(
            out.contains("${ROCKY_FILTER_PREFIX}"),
            "redacted body, {} bytes",
            out.len()
        );
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
        assert!(
            !out.contains("5678XYZ"),
            "B's tail survived: body of {} bytes",
            out.len()
        );
        assert!(
            !out.contains("ABCDEFGH"),
            "A survived: body of {} bytes",
            out.len()
        );
        // Absence alone would pass for a filter that ate the whole body, so
        // pin what it produced as well.
        assert!(
            out.contains("${ROCKY_OVERLAP_A}") && out.contains("${ROCKY_OVERLAP_B}"),
            "an overlap must name BOTH variables, not silently drop one: body of {} bytes",
            out.len()
        );
        serde_json::from_str::<serde_json::Value>(&out).unwrap_or_else(|e| {
            panic!(
                "overlap redaction produced invalid JSON ({e}), {} bytes",
                out.len()
            )
        });
    }

    /// Codex, delta pass. The occurrence scan advanced `from = start + 1` and
    /// then sliced `text[from..]`. A value that BEGINS with a multi-byte
    /// character puts that index inside the character, and the next slice
    /// panics.
    ///
    /// The earlier multi-byte test only drove `redact_truncated_tail`, so it
    /// could not see this. This one goes through `redact`.
    #[test]
    fn a_value_starting_with_a_multibyte_character_does_not_panic() {
        let secret = "éABCDEFGH-8e26660e";
        assert!(
            !secret.is_char_boundary(1),
            "PRECONDITION: byte 1 is inside é"
        );
        register_substitution("ROCKY_MULTIBYTE_LEAD", secret);

        let out = redact(&format!(r#"{{"m":"{secret} and again {secret}"}}"#));
        assert!(
            !out.contains(secret),
            "the value survived: body of {} bytes",
            out.len()
        );
        assert!(
            out.contains("${ROCKY_MULTIBYTE_LEAD}"),
            "redacted body, {} bytes",
            out.len()
        );
    }

    /// Codex, delta pass. The backstop: a finished body that still carries a
    /// registered value is refused, however the replacement was generated.
    #[test]
    fn a_surviving_value_is_detected_in_the_wire_form() {
        register_substitution("ROCKY_RESCAN_WIRE", "RESCAN-WIRE-8e26660e");
        assert!(
            any_value_survives(r#"{"m":"RESCAN-WIRE-8e26660e"}"#),
            "a value present in the bytes must be detected"
        );
        assert!(
            !any_value_survives(r#"{"m":"nothing here"}"#),
            "a clean body must not be refused"
        );
    }

    /// A value whose wire representation is ESCAPED is still detected.
    ///
    /// Note what this does NOT prove. The rescan also walks the JSON-decoded
    /// strings, and I could not construct a case where that view fires and
    /// the wire scan misses — `escaped_forms` already generates the escaped
    /// representations, so the wire pass subsumes it for every producer in
    /// this codebase. The decoded view is kept as defence against an escaping
    /// this code does not generate, and it is deliberately NOT claimed as a
    /// tested path: removing it leaves every test green.
    #[test]
    fn a_value_escaped_on_the_wire_is_still_detected() {
        let secret = "RESCAN-DECODED-8e26660e";
        register_substitution("ROCKY_RESCAN_DECODED", secret);

        // The value is not contiguous in the wire bytes — it is split by an
        // escape — but `serde_json` decodes it back.
        let body = serde_json::to_string(&serde_json::json!({ "m": secret })).expect("serializes");
        assert!(
            any_value_survives(&body),
            "a value recoverable by decoding must be detected: body of {} bytes",
            body.len()
        );
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

        assert!(
            !out.contains(secret),
            "the value survived: body of {} bytes",
            out.len()
        );
        serde_json::from_str::<serde_json::Value>(&out).unwrap_or_else(|e| {
            panic!("redaction produced invalid JSON ({e}), {} bytes", out.len())
        });
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
            "the doubly-escaped form was not matched: body of {} bytes",
            out.len()
        );
        assert!(
            !any_value_survives(&out),
            "the backstop must agree the value is gone: body of {} bytes",
            out.len()
        );
        serde_json::from_str::<serde_json::Value>(&out)
            .unwrap_or_else(|e| panic!("produced invalid JSON ({e}), {} bytes", out.len()));
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
            "the replacement reintroduced an already-scanned value: body of {} bytes",
            out.len()
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

    /// Codex A. **The decoded view is load-bearing, and I claimed it was
    /// redundant.** This is the construction I could not find.
    ///
    /// JSON permits `\/` as an escape for `/`, but neither `serde_json` nor
    /// Rust `Debug` ever generates it — so every form `escaped_forms`
    /// produces collapses to the unescaped value, and none of them appears in
    /// these wire bytes. Only decoding recovers it.
    ///
    /// Deleting the decoded traversal must make this fail.
    #[test]
    fn a_value_reachable_only_by_decoding_is_detected() {
        let secret = "ABCDEFGH/";
        register_substitution("ROCKY_SOLIDUS", secret);

        // `\/` is a legal JSON escape for `/`. We never emit it; a body may
        // still contain it.
        let body = r#"{"m":"ABCDEFGH\/"}"#;
        assert!(
            !body.contains(secret),
            "PRECONDITION: the raw value must be ABSENT from the wire bytes, \
             or the wire scan would catch it and this proves nothing: body of {} bytes",
            body.len()
        );

        assert!(
            any_value_survives(body),
            "a value recoverable only by JSON decoding must be detected"
        );
    }

    /// Codex C. A pre-filter truncation stored the value's ESCAPED form, so
    /// the surviving tail is a prefix of that form rather than of the raw
    /// value. Comparing only raw prefixes left the decoded value on the wire,
    /// which is the opposite of this helper's purpose.
    #[test]
    fn a_truncated_prefix_of_an_escaped_form_is_rewritten() {
        let secret = "ABCD\"EFGH-REST-8e26660e";
        register_substitution("ROCKY_ESCAPED_TAIL", secret);

        // What an older binary stored: the JSON-escaped value, cut, then the
        // sentinel.
        let escaped = serde_json::to_string(secret).expect("serializes");
        let escaped = escaped.trim_matches('"');
        let stored = format!("{}…", &escaped[..12]);

        let out = redact_truncated_tail(&stored);
        assert!(
            out.contains("${ROCKY_ESCAPED_TAIL}"),
            "an escaped truncated prefix must be rewritten: body of {} bytes",
            out.len()
        );
    }

    /// Codex B. The refusal envelope is fixed text, but an operator's value
    /// can BE that text — and this response is built by the outermost layer,
    /// so nothing scans it afterwards. The last resort is a bare status with
    /// no body, which cannot leak and cannot recurse.
    ///
    /// Not a registration denylist: refusing to register a colliding value
    /// would leave a real secret unredacted everywhere for the life of the
    /// process.
    #[tokio::test]
    async fn a_refusal_that_would_carry_a_value_is_sent_with_no_body() {
        register_substitution("ROCKY_ENVELOPE_COLLIDE", "secret_redaction_unavailable");

        let response = redaction_unavailable();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        // The envelope names the code verbatim, so with that string
        // registered the body must be dropped entirely.
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("a bare status has a readable body");
        assert!(
            bytes.is_empty(),
            "the refusal must carry no body when its own text would leak: {:?}",
            String::from_utf8_lossy(&bytes)
        );
    }

    /// Codex E. A repeated-character value produced one span per offset —
    /// ~62.9M spans for a 60 MiB run, inside a 64 MiB body cap. Overlapping
    /// matches of the same form now collapse as they are found.
    ///
    /// Asserted through the OUTPUT rather than a span count: a run collapses
    /// to a single replacement, which is the observable consequence.
    #[test]
    fn a_repeated_character_run_collapses_to_one_replacement() {
        register_substitution("ROCKY_RUN", "AAAAAAAA");
        let body = format!(r#"{{"m":"{}"}}"#, "A".repeat(64));

        // The COUNT is the thing the fix changes. The merge downstream
        // produces identical output either way, so asserting on `redact`'s
        // result cannot see this — without the seam the fix is unguardable.
        let spans = discover_spans(&body, &secret_registry::substitutions());
        let run_spans = spans.iter().filter(|s| s.end - s.start >= 8).count();
        assert!(
            run_spans <= 2,
            "a 64-character run produced {run_spans} spans; retaining every \
             overlapping offset is what reached ~62.9M spans on a large body"
        );

        let out = redact(&body);
        assert_eq!(
            out.matches("${ROCKY_RUN}").count(),
            1,
            "a run of one character must collapse to a single span: body of {} bytes",
            out.len()
        );
        assert!(
            !out.contains("AAAAAAAA"),
            "the run survived: body of {} bytes",
            out.len()
        );
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
