//! The route sweep: no resolved secret reaches any `rocky serve` response.
//!
//! `${VAR}` is substituted **before** the TOML parse, so a resolved value can
//! travel into a parse error, a validation error, a model's frontmatter error,
//! or a field that is simply rendered back. Any of those can reach a route.
//! This module puts one distinctive value into every field it can, drives
//! every route in [`crate::api::api_v1_routes`], and asserts the bytes appear
//! in no response.
//!
//! Three things make it a sweep rather than a spot check:
//!
//! 1. **It walks the declared table, and the table is now complete.** That is
//!    only true since `router_registers_no_undeclared_route` began counting
//!    every method. Until 2026-09-11 it counted mutating registrations only,
//!    so a `GET` could sit on the router and in no table — and `GET` is the
//!    read surface this sweep is about. **If that guard is ever narrowed
//!    again, this sweep silently shrinks with it.**
//!
//! 2. **The substitution is proved before the responses are read.** A fixture
//!    that failed to load for an unrelated reason would sweep clean and prove
//!    nothing. [`assert_substituted`] fails the test unless the resolved bytes
//!    are really in what the loader produced.
//!
//! 3. **The needle is distinctive.** A short or common value ("secret", "1")
//!    matches by accident, turning a real leak into noise or hiding one.
//!
//! The sweep asserts the VALUE is absent, never that a message has some shape.
//! A reworded error that still interpolates would pass a wording assertion and
//! leak just the same.
//!
//! The UI routes are deliberately out of scope: `ui::index_response` serves
//! the embedded `index.html` byte for byte, with no templating and no
//! bootstrap state, so it carries nothing from the config.

#![cfg(test)]

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocky_server::state::ServerState;

/// The needle. Long and unique, so a match is a leak and not a coincidence.
const PROBE: &str = "ROCKYPROBE-c7f1a93e5d2b4806-LEAKED-SECRET-VALUE";

/// The variable every fixture field references.
const PROBE_VAR: &str = "ROCKY_PROBE_SECRET";

/// The floor Hugo ruled on 2026-09-11: a resolved value of 8 bytes or more is
/// redacted everywhere; a shorter one is shown.
const REDACTION_FLOOR_BYTES: usize = 8;

/// A value BELOW the floor, which must be SHOWN. It pins the rule from the
/// other side: without it, a later change could redact everything and every
/// "absent" assertion would still pass.
const PROBE_SHORT: &str = "ab7";
const PROBE_SHORT_VAR: &str = "ROCKY_PROBE_SHORT";

/// A resolved value that is also a legitimate substring of a model's name.
/// `${ROCKY_PROBE_CATALOG}` resolves to `analytics`, and the project contains a
/// model called `analytics_orders`. A redaction done by blind substring
/// replacement would eat the model name too.
///
/// Ruled on 2026-09-11: the value is redacted like any other, and the
/// replacement names the variable, so `${ROCKY_PROBE_CATALOG}_orders` still
/// tells an operator which model failed.
///
/// A value that lands in a NUMERIC JSON position. `redact()` is a text
/// replacement over the whole body, so rewriting `1234567890` to
/// `${ROCKY_PROBE_NUMERIC}` produces `{"max_downstreams":${...}}` — not valid
/// JSON. The ruling (drain, 2026-09-11) is that the response is then REFUSED
/// with `secret_redaction_unavailable`, deterministically and at the response
/// level, never partially rendered.
///
/// `policy.rules[].scope.max_downstreams` is `Option<u64>` on both sides
/// (`config.rs:3128`, `policy.rs:454`) and renders on `GET /api/v1/policy`, a
/// route this sweep already drives. Unquoted in TOML it is a valid integer, so
/// the value really does arrive in a numeric position — a field the schema
/// typed as a string would arrive quoted and could not corrupt anything.
const PROBE_NUMERIC: &str = "12345678901";
const PROBE_NUMERIC_VAR: &str = "ROCKY_PROBE_NUMERIC";

const PROBE_COLLIDE: &str = "analytics";
const PROBE_COLLIDE_VAR: &str = "ROCKY_PROBE_CATALOG";
const COLLIDING_MODEL: &str = "analytics_orders";

/// Every probe meant to be redacted must clear the floor, and must really be
/// in the value before redaction.
///
/// Without the first half, a post-fix sweep goes green for the wrong reason:
/// the rule says "show a short value", so the grep finds nothing and the test
/// reads as proof of redaction. Without the second half, a fixture that never
/// carried the value sweeps clean too.
fn assert_probe_is_redactable() {
    assert!(
        PROBE.len() >= REDACTION_FLOOR_BYTES,
        "the probe is {} bytes, below the {REDACTION_FLOOR_BYTES}-byte floor: \
         the rule would SHOW it, and every 'absent' assertion below would pass \
         without redaction happening at all",
        PROBE.len()
    );
    assert!(
        PROBE_SHORT.len() < REDACTION_FLOOR_BYTES,
        "the short probe is {} bytes, at or above the floor, so it would be \
         redacted and could not pin the 'shown' half of the rule",
        PROBE_SHORT.len()
    );
    assert!(
        PROBE_COLLIDE.len() >= REDACTION_FLOOR_BYTES,
        "the collision probe must clear the floor, or nothing would try to \
         redact it and the collision could not arise"
    );
    assert!(
        COLLIDING_MODEL.contains(PROBE_COLLIDE),
        "the collision case needs the model name to CONTAIN the resolved value"
    );
}

/// Substitute the `{param}` placeholders so a declared path can be requested.
///
/// Mirrors `probe_url` in `api.rs`'s own probes. The test below asserts no
/// placeholder survives, so a new parameter name fails here rather than
/// quietly sending the request to the fallback and reading as swept.
fn probe_path(path: &str) -> String {
    path.replace("{name}", "probe_model")
        .replace("{column}", "probe_column")
        .replace("{id}", "probe_job")
        .replace("{pipeline}", "probe_pipeline")
        .replace("{plan_id}", "probe_plan")
        .replace("{subject}", "probe_subject")
}

/// What a fixture is trying to make the server say.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Fixture {
    /// Everything substitutes and the project loads. The secret sits in
    /// ordinary fields a route may render back.
    Valid,
    /// `autonomy_budget.window` resolves to the secret and fails validation.
    /// `ConfigError::PolicyBudgetInvalidWindow` interpolates `{window:?}`.
    InvalidPolicyWindow,
    /// `models/_defaults.toml` is valid before substitution and broken after.
    /// `load_dir_defaults` substitutes, then parses (`models.rs:691,706`), so
    /// `ModelError::ParseFrontmatter` carries a toml error whose span holds
    /// the resolved value.
    BrokenDefaults,
    /// The same shape one directory down: `models/groups/*.toml`, through
    /// `load_groups_from_dir` (`models.rs:803`).
    BrokenGroup,
    /// The same shape again: `models/test_definitions.toml`, through
    /// `load_test_definitions_from_dir` (`models.rs:881`).
    BrokenTestDefinitions,
    /// The resolved value is bare where TOML needs a quoted string, so the
    /// config parse itself fails with the value inside the reported span.
    BrokenConfigParse,
    /// A leak needs no syntax error. `GroupConfig` DOES reject unknown fields,
    /// and the toml error echoes the whole offending line — quotes and value
    /// included. A filter keyed to parse failures would walk past this one.
    UnknownKeyInGroup,
    /// The short value, which the rule says to SHOW. Asserted present, not
    /// absent. This is the deliberate exception to the sweep's contract.
    ShortValueShown,
    /// `${ROCKY_PROBE_CATALOG}` resolves to `analytics`, and the project has a
    /// model named `analytics_orders`. Recorded, not asserted.
    CollisionWithModelName,
    /// The value lands in a numeric JSON position, where a text replacement
    /// cannot produce valid JSON. The response must be REFUSED whole.
    NumericPosition,
}

impl Fixture {
    fn all() -> [Fixture; 10] {
        [
            Fixture::Valid,
            Fixture::InvalidPolicyWindow,
            Fixture::BrokenDefaults,
            Fixture::BrokenGroup,
            Fixture::BrokenTestDefinitions,
            Fixture::BrokenConfigParse,
            Fixture::UnknownKeyInGroup,
            Fixture::ShortValueShown,
            Fixture::CollisionWithModelName,
            Fixture::NumericPosition,
        ]
    }

    fn name(self) -> &'static str {
        match self {
            Fixture::Valid => "valid",
            Fixture::InvalidPolicyWindow => "invalid_policy_window",
            Fixture::BrokenDefaults => "broken_defaults",
            Fixture::BrokenGroup => "broken_group",
            Fixture::BrokenTestDefinitions => "broken_test_definitions",
            Fixture::BrokenConfigParse => "broken_config_parse",
            Fixture::UnknownKeyInGroup => "unknown_key_in_group",
            Fixture::ShortValueShown => "short_value_shown",
            Fixture::CollisionWithModelName => "collision_with_model_name",
            Fixture::NumericPosition => "numeric_position",
        }
    }

    /// What the sweep does with a match for this fixture.
    fn expectation(self) -> Expect {
        match self {
            Fixture::ShortValueShown => Expect::Shown,
            Fixture::CollisionWithModelName => Expect::ReplacedByName,
            Fixture::NumericPosition => Expect::Absent,
            _ => Expect::Absent,
        }
    }

    /// The value this fixture puts into the project.
    fn probe(self) -> &'static str {
        match self {
            Fixture::ShortValueShown => PROBE_SHORT,
            Fixture::CollisionWithModelName => PROBE_COLLIDE,
            Fixture::NumericPosition => PROBE_NUMERIC,
            _ => PROBE,
        }
    }

    /// The variable that carries it.
    fn var(self) -> &'static str {
        match self {
            Fixture::ShortValueShown => PROBE_SHORT_VAR,
            Fixture::CollisionWithModelName => PROBE_COLLIDE_VAR,
            Fixture::NumericPosition => PROBE_NUMERIC_VAR,
            _ => PROBE_VAR,
        }
    }
}

/// What a match means for a given fixture.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    /// A match is a leak. The contract.
    Absent,
    /// A match is REQUIRED: the value is below the floor and the rule says to
    /// show it. Its absence would mean over-redaction.
    Shown,
    /// Two assertions, not one. The value must be GONE, and the variable's
    /// NAME must stand in its place.
    ///
    /// A filter that replaced with `***` would satisfy the first and fail the
    /// second, and the second is what keeps the output usable: `${VAR}_orders`
    /// tells an operator which model failed, `[REDACTED]_orders` does not.
    /// The variable name is not a secret — `format_env_var_hint` already
    /// prints names in config diagnostics, and the ruling is about values.
    ReplacedByName,
}

/// A bare `${VAR}` is valid TOML on the page and invalid once it resolves —
/// but only if the resolved text is not itself a TOML scalar. A probe of `1`
/// or `true` would parse, the file would load, and the fixture would prove
/// nothing while looking like it had.
fn bare_probe_is_not_valid_toml() {
    assert!(
        toml::from_str::<toml::Value>(&format!("k = {PROBE}")).is_err(),
        "the probe parses as bare TOML, so an 'unquoted' fixture would load \
         cleanly and every broken_* case would be vacuous"
    );
}

/// Write a project whose fields reference the probe variable.
///
/// The value goes everywhere a `${VAR}` can go and a route can reach: the
/// adapter, the pipeline, governance, the policy rules, `_defaults.toml`, a
/// model sidecar and a model's frontmatter — not only `rocky.toml`.
fn write_fixture(dir: &Path, fixture: Fixture) -> PathBuf {
    let var = fixture.var();
    let models = dir.join("models");
    std::fs::create_dir_all(&models).unwrap();

    let policy = match fixture {
        Fixture::InvalidPolicyWindow => format!(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = {{ any = true }}
effect = "allow"
autonomy_budget = {{ failures = 2, window = "${{{var}}}" }}
"#
        ),
        // Unquoted, so it is a TOML integer and arrives in a NUMERIC position.
        Fixture::NumericPosition => format!(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
effect = "allow"

[policy.rules.scope]
max_downstreams = ${{{var}}}
"#
        ),
        // `principal` is an enum (`human` or `agent`) and `capability` is
        // closed, so neither can carry a probe. `scope.models` is a free list
        // of patterns, and `GET /api/v1/policy` renders the scope back.
        _ => format!(
            r#"
[policy]
version = 1

[[policy.rules]]
principal = "agent"
capability = "apply"
scope = {{ models = ["${{{var}}}"] }}
effect = "allow"
"#
        ),
    };

    // Valid TOML before substitution, broken after: the value lands unquoted.
    let adapter_path = if fixture == Fixture::BrokenConfigParse {
        format!("path = ${{{var}}}")
    } else {
        format!("path = \"{}\"", dir.join("probe.duckdb").display())
    };

    let config = format!(
        r#"
[adapter]
type = "duckdb"
{adapter_path}

[pipeline.probe]
type = "transformation"
models = "models/**"
description = "${{{var}}}"

[pipeline.probe.target.governance]
auto_create_schemas = true
tag_prefix = "${{{var}}}"
{policy}
"#
    );
    let config_path = dir.join("rocky.toml");
    std::fs::write(&config_path, config).unwrap();

    // `_defaults.toml`, `groups/*.toml` and `test_definitions.toml` each
    // substitute and THEN parse, so an unquoted reference is valid on the page
    // and invalid once resolved — with the resolved text inside the toml
    // error's span. Quoted, the same field just carries the value.
    //
    // `target.schema` is a real defaults key: an unknown key would be dropped
    // silently, because `RawModelConfig` has no `deny_unknown_fields`, and the
    // fixture would prove nothing.
    let defaults = if fixture == Fixture::BrokenDefaults {
        format!("[target]\nschema = ${{{var}}}\n")
    } else {
        format!("[target]\nschema = \"${{{var}}}\"\n")
    };
    std::fs::write(models.join("_defaults.toml"), defaults).unwrap();

    if fixture == Fixture::BrokenGroup {
        std::fs::create_dir_all(models.join("groups")).unwrap();
        std::fs::write(
            models.join("groups").join("probe_group.toml"),
            format!("name = ${{{var}}}\n"),
        )
        .unwrap();
    }

    // The same file, QUOTED and therefore valid TOML — but `name` is not a
    // `GroupConfig` field, and that struct DOES reject unknown fields. The
    // resulting error echoes the whole source line, value included. A leak
    // needs no syntax error.
    if fixture == Fixture::UnknownKeyInGroup {
        std::fs::create_dir_all(models.join("groups")).unwrap();
        std::fs::write(
            models.join("groups").join("probe_group.toml"),
            format!("name = \"${{{var}}}\"\n"),
        )
        .unwrap();
    }

    if fixture == Fixture::BrokenTestDefinitions {
        std::fs::write(
            models.join("test_definitions.toml"),
            format!("[probe_test]\nsql = ${{{var}}}\n"),
        )
        .unwrap();
    }

    std::fs::write(models.join("probe_model.sql"), "select 1 as id\n").unwrap();
    std::fs::write(
        models.join("probe_model.toml"),
        format!("name = \"probe_model\"\ndescription = \"${{{var}}}\"\n"),
    )
    .unwrap();

    // The collision: a model whose NAME contains the resolved value. A blind
    // substring redaction would eat the name along with the secret.
    if fixture == Fixture::CollisionWithModelName {
        std::fs::write(
            models.join(format!("{COLLIDING_MODEL}.sql")),
            "select 1 as id\n",
        )
        .unwrap();
        std::fs::write(
            models.join(format!("{COLLIDING_MODEL}.toml")),
            format!("name = \"{COLLIDING_MODEL}\"\n"),
        )
        .unwrap();
    }

    config_path
}

/// Each broken_* fixture must really break the loader it targets.
///
/// Without this, "no secret appeared" and "no error happened" are the same
/// observation, and a fixture that silently loaded would read as a clean
/// sweep. This asserts the parse FAILED, at the site the fixture names.
fn assert_site_is_exercised(models_dir: &Path, fixture: Fixture) {
    use rocky_core::models::{
        load_dir_defaults, load_groups_from_dir, load_test_definitions_from_dir,
    };

    let failed = match fixture {
        Fixture::BrokenDefaults => load_dir_defaults(&models_dir.join("_defaults.toml")).err(),
        Fixture::BrokenGroup => load_groups_from_dir(models_dir).err(),
        Fixture::BrokenTestDefinitions => load_test_definitions_from_dir(models_dir).err(),
        _ => return,
    };

    let err = failed.unwrap_or_else(|| {
        panic!(
            "{}: the loader accepted the fixture, so this site was never \
             exercised — 'no leak' here would mean nothing",
            fixture.name()
        )
    });
    assert!(
        matches!(err, rocky_core::models::ModelError::ParseFrontmatter { .. }),
        "{}: expected a ParseFrontmatter failure at the site this fixture \
         targets, got {err:?}",
        fixture.name()
    );
}

/// Prove the substitution really happened before any response is read.
///
/// Without this, a fixture that failed to load for an unrelated reason — a
/// renamed key, a typo, a variable the loader never expanded — sweeps clean
/// and proves nothing at all.
fn assert_substituted(config_path: &Path, fixture: Fixture) {
    let (probe, var) = (fixture.probe(), fixture.var());
    let raw = std::fs::read_to_string(config_path).unwrap();
    assert!(
        raw.contains(&format!("${{{var}}}")),
        "{}: the fixture on disk must reference {var}, or the sweep is vacuous",
        fixture.name()
    );
    assert!(
        !raw.contains(probe),
        "{}: the fixture on disk must NOT contain the resolved value — the whole \
         point is that substitution is what puts it there",
        fixture.name()
    );
    assert_eq!(
        std::env::var(var).as_deref(),
        Ok(probe),
        "{}: {var} is unset in this process, so nothing can substitute",
        fixture.name()
    );

    let loaded = rocky_core::config::load_rocky_config(config_path);
    match fixture {
        // These must LOAD. The resolved value must then be findable in what
        // the loader produced — that is the substitution, proved. The
        // broken_* model-side fixtures break a MODEL file, not rocky.toml, so
        // the config half still has to load; `assert_site_is_exercised`
        // proves the model half really broke.
        Fixture::Valid
        | Fixture::UnknownKeyInGroup
        | Fixture::ShortValueShown
        | Fixture::CollisionWithModelName
        | Fixture::NumericPosition
        | Fixture::BrokenDefaults
        | Fixture::BrokenGroup
        | Fixture::BrokenTestDefinitions => {
            let cfg = loaded.unwrap_or_else(|e| {
                panic!(
                    "{}: the config must load, or the sweep proves nothing: {e:#}",
                    fixture.name()
                )
            });
            assert!(
                format!("{cfg:?}").contains(probe),
                "{}: the loaded config does not carry the resolved value, so the \
                 substitution this sweep is about never happened",
                fixture.name()
            );
        }
        // These two must FAIL, and fail for the reason the fixture intends.
        //
        // The precondition here is which code path ran, NOT whether the
        // message contains the value: asserting the value would bake today's
        // leak into the fixture and break the moment it is fixed.
        Fixture::InvalidPolicyWindow => {
            let err = loaded
                .err()
                .unwrap_or_else(|| panic!("{}: this fixture must fail validation", fixture.name()));
            let rendered = format!("{err:#}");
            assert!(
                rendered.contains("autonomy_budget.window"),
                "{}: expected the window validation to reject the resolved \
                 value, got: {rendered}",
                fixture.name()
            );
        }
        Fixture::BrokenConfigParse => {
            let err = loaded
                .err()
                .unwrap_or_else(|| panic!("{}: this fixture must fail the parse", fixture.name()));
            assert!(
                format!("{err:#}").contains(var),
                "{}: the failure must at least name the variable, or this \
                 fixture is failing for some other reason",
                fixture.name()
            );
        }
    }
}

async fn spawn(state: Arc<ServerState>) -> String {
    let app = crate::api::router(state);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// Where a leak was found.
#[derive(Debug)]
struct Leak {
    fixture: &'static str,
    entry: String,
    status: u16,
    where_: &'static str,
    excerpt: String,
}

/// The text around the first match, so a failure names the field instead of
/// dumping a whole body.
fn excerpt_of(haystack: &str, probe: &str) -> String {
    match haystack.find(probe) {
        Some(i) => {
            let from = i.saturating_sub(90);
            let to = (i + probe.len() + 90).min(haystack.len());
            haystack[from..to].replace('\n', " ")
        }
        None => String::new(),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn no_serve_route_renders_a_resolved_secret() {
    // SAFETY: the value is a constant set once, before any server is built,
    // and never removed. A concurrent reader in this binary sees either unset
    // or this exact value; neither makes another test's assertion wrong.
    unsafe {
        std::env::set_var(PROBE_VAR, PROBE);
        std::env::set_var(PROBE_SHORT_VAR, PROBE_SHORT);
        std::env::set_var(PROBE_COLLIDE_VAR, PROBE_COLLIDE);
        std::env::set_var(PROBE_NUMERIC_VAR, PROBE_NUMERIC);
    }
    bare_probe_is_not_valid_toml();
    assert_probe_is_redactable();

    let declared = crate::api::api_v1_routes();
    let mut leaks: Vec<Leak> = Vec::new();
    // Routes where the BELOW-FLOOR value really was shown. Empty means it was
    // redacted everywhere, which is over-redaction.
    let mut shown_somewhere: Vec<String> = Vec::new();
    // Responses where the replacement token stands in for the collided value.
    let mut name_shown: Vec<String> = Vec::new();
    // Responses refused whole because redaction could not produce valid JSON.
    let mut refusals: Vec<String> = Vec::new();
    let mut swept: BTreeSet<String> = BTreeSet::new();
    let client = reqwest::Client::new();

    for fixture in Fixture::all() {
        let dir = tempfile::tempdir().unwrap();
        let config_path = write_fixture(dir.path(), fixture);
        assert_substituted(&config_path, fixture);

        let models_dir = dir.path().join("models");
        assert_site_is_exercised(&models_dir, fixture);
        let state_path = models_dir.join(rocky_core::state::STATE_FILE_NAME);
        drop(rocky_core::state::StateStore::open(&state_path).unwrap());

        let state = ServerState::with_auth(
            models_dir,
            None,
            Some(config_path.clone()),
            None,
            Vec::new(),
            Some(state_path.clone()),
        );
        state.recompile().await;
        let base = spawn(state).await;

        for entry in &declared {
            let (method, path) = entry.split_once(' ').expect("entries are 'METHOD /path'");
            let resp = client
                .request(
                    reqwest::Method::from_bytes(method.as_bytes()).unwrap(),
                    format!("{base}{}", probe_path(path)),
                )
                // Short-circuits the POST /jobs/* routes with a 400 before any
                // permit, persist or subprocess: the sweep needs "the route
                // answered", not a submitted job. GET routes ignore it.
                .header("X-Rocky-Principal", "bad/slash")
                .send()
                .await
                .unwrap_or_else(|e| panic!("{entry} did not answer: {e}"));
            swept.insert(entry.clone());

            let status = resp.status().as_u16();
            let headers = format!("{:?}", resp.headers());
            let body = resp.text().await.unwrap_or_default();

            let probe = fixture.probe();
            if fixture == Fixture::NumericPosition && body.contains("secret_redaction_unavailable")
            {
                refusals.push(format!("  {entry} -> {status}"));
            }
            if fixture.expectation() == Expect::ReplacedByName
                && body.contains(&format!("${{{PROBE_COLLIDE_VAR}}}"))
            {
                name_shown.push(format!("  {entry} -> {status}"));
            }
            for (where_, hay) in [("headers", &headers), ("body", &body)] {
                let found = hay.contains(probe);
                match (fixture.expectation(), found) {
                    // The contract: a match is a leak.
                    (Expect::Absent, true) => leaks.push(Leak {
                        fixture: fixture.name(),
                        entry: entry.clone(),
                        status,
                        where_,
                        excerpt: excerpt_of(hay, probe),
                    }),
                    // Below the floor, so the rule says SHOW it. Recorded
                    // per route; the assertion is made once, after the loop,
                    // against the WHOLE set. Demanding it in every response
                    // would be wrong — /health has no config in it to show.
                    (Expect::Shown, true) => shown_somewhere.push(format!(
                        "  [{}] {} -> {} ({})",
                        fixture.name(),
                        entry,
                        status,
                        where_
                    )),
                    // The value must be gone here too.
                    (Expect::ReplacedByName, true) => leaks.push(Leak {
                        fixture: fixture.name(),
                        entry: entry.clone(),
                        status,
                        where_,
                        excerpt: excerpt_of(hay, probe),
                    }),
                    (Expect::Absent, false)
                    | (Expect::Shown, false)
                    | (Expect::ReplacedByName, false) => {}
                }
            }
        }
    }

    // A route that was skipped is a route that was never checked.
    let expected: BTreeSet<String> = declared.iter().cloned().collect();
    assert_eq!(
        expected, swept,
        "the sweep did not drive every declared route"
    );

    assert!(
        leaks.is_empty(),
        "the resolved value of {PROBE_VAR} reached {} response(s):\n{}",
        leaks.len(),
        leaks
            .iter()
            .map(|l| format!(
                "  [{}] {} -> {} ({})\n      …{}…",
                l.fixture, l.entry, l.status, l.where_, l.excerpt
            ))
            .collect::<Vec<_>>()
            .join("\n")
    );

    // What this sweep does NOT reach, printed every run. A loud gap is worth
    // more than a quiet pass, and the shape of this one is known:
    // fragment recovery is anchored on the `…` sentinel `truncate_error`
    // appends, so it only sees cuts that announce themselves.
    println!(
        "\nNOT COVERED: a truncation with no `…` sentinel would leave a fragment\n\
         \x20            this sweep cannot see. TWO known sites, both marked and\n\
         \x20            both covered; others unproven.\n\
         NOT COVERED: GET /api/v1/jobs/{{id}} is driven with a synthetic id, so no\n\
         \x20            job output exists to leak. Untested, not clean.\n\
         NOT COVERED: every fixture builds a fresh store, so nothing written\n\
         \x20            before a restart is ever read back."
    );

    // The numeric position. A registered value in a non-string slot makes the
    // text replacement produce invalid JSON, so the response is refused WHOLE
    // rather than partially rendered — response level, not field level, so
    // everything else that response carried is gone with it. Asserting the
    // envelope specifically, not merely "well-formed": a body that stayed
    // valid JSON would mean the corruption never arose and the fixture proved
    // nothing.
    println!(
        "\nrefused whole ({PROBE_NUMERIC_VAR} in a numeric position):\n{}",
        if refusals.is_empty() {
            "  (nowhere)".to_string()
        } else {
            refusals.join("\n")
        }
    );
    assert!(
        !refusals.is_empty(),
        "{PROBE_NUMERIC_VAR} resolves to {PROBE_NUMERIC} in \
         policy.rules[].scope.max_downstreams, a u64 that renders as a JSON \
         NUMBER on GET /api/v1/policy. Replacing it with ${{{PROBE_NUMERIC_VAR}}} \
         cannot produce valid JSON, so the response must be refused with \
         secret_redaction_unavailable — and no response carried that envelope."
    );

    // The collision's second half. The first half — that the value is gone —
    // is asserted with every other leak below.
    //
    // `analytics` is a legitimate substring of the model `analytics_orders`,
    // so redaction by value necessarily mangles the model name. Ruled
    // acceptable on 2026-09-11 BECAUSE the replacement names the variable:
    // `${ROCKY_PROBE_CATALOG}_orders` still tells an operator which model
    // failed. A filter replacing with `***` would pass the absence assertion
    // and fail this one.
    //
    // Set-level on purpose: most routes render no config, so requiring the
    // token on every route would assert a coincidence rather than the rule.
    println!(
        "\nreplacement token seen on:\n{}",
        if name_shown.is_empty() {
            "  (nowhere)".to_string()
        } else {
            name_shown.join("\n")
        }
    );
    assert!(
        !name_shown.is_empty(),
        "the resolved value of {PROBE_COLLIDE_VAR} collides with the model name \
         {COLLIDING_MODEL}, so redaction must mangle it — but the replacement \
         must NAME the variable, and ${{{PROBE_COLLIDE_VAR}}} appeared in no \
         response. An opaque token leaves an operator unable to tell which \
         model failed."
    );

    // The floor, pinned from the other side. A value below it must be shown
    // SOMEWHERE — not everywhere, since most routes render no config at all.
    //
    // DO NOT "TIGHTEN" THIS to every route. The first draft did, and reported
    // 67 failures against routes like /health that render no config and never
    // could have shown the value. A route that legitimately contains nothing
    // is not evidence the floor stopped working.
    // If it appears nowhere, redaction has swallowed values the rule says to
    // keep, and every "absent" assertion above would pass without proving
    // that anything is redacted.
    assert!(
        !shown_somewhere.is_empty(),
        "the {}-byte value of {PROBE_SHORT_VAR} is BELOW the \
         {REDACTION_FLOOR_BYTES}-byte floor, so the rule says to show it — and \
         it appeared in NO response. That is over-redaction, and it would make \
         every absence assertion in this sweep pass without proving anything.",
        PROBE_SHORT.len()
    );
}

/// No `{param}` placeholder may survive into a requested URL. A surviving
/// brace sends the request to the fallback, and the route would read as swept
/// while never being reached.
#[test]
fn every_placeholder_has_a_probe_value() {
    for entry in crate::api::api_v1_routes() {
        let (_, path) = entry.split_once(' ').unwrap();
        let url = probe_path(path);
        assert!(
            !url.contains('{') && !url.contains('}'),
            "{entry} still has an unsubstituted placeholder: {url}"
        );
    }
}

/// `POST /api/v1/compile` must be in the sweep. It is the one route that
/// answers through axum's `Json` rather than the crate's `PrettyJson`, so it
/// does not share the renderer every other route goes through — a redaction
/// applied in `PrettyJson` alone would miss exactly this route.
#[test]
fn the_sweep_covers_the_route_that_bypasses_the_canonical_renderer() {
    let declared = crate::api::api_v1_routes();
    assert!(
        declared.iter().any(|e| e == "POST /api/v1/compile"),
        "POST /api/v1/compile is not declared, so the sweep never drives it"
    );
}

// ---------------------------------------------------------------------------
// Truncation: the fragment a cut leaves behind
// ---------------------------------------------------------------------------

/// How much of a known secret may survive a cut. Deliberately a DIFFERENT
/// number from the 8-byte floor: the floor decides which values are secret,
/// this decides how much of one may remain after truncation.
const MAX_SURVIVING_FRAGMENT: usize = 4;

/// The variable name is deliberately long. `${NAME}` is inserted by redaction
/// and then cut by truncation, so the placeholder's length depends on the
/// NAME, not on the secret — and operators really do write
/// `ROCKY_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`, not `T`.
const PROBE_LONGNAME_VAR: &str = "ROCKY_PROBE_SNOWFLAKE_PRIVATE_KEY_PASSPHRASE";

/// The longest prefix of `probe` that appears in `hay`, or 0.
///
/// A whole-value search cannot see a truncated secret: the tail is gone, so
/// there is no whole value to find. This looks for what a cut actually leaves.
fn longest_surviving_prefix(hay: &str, probe: &str) -> usize {
    (MAX_SURVIVING_FRAGMENT..=probe.len())
        .rev()
        .find(|&n| hay.contains(&probe[..n]))
        .unwrap_or(0)
}

/// A truncated attempt error must not ship a fragment of a resolved secret.
///
/// `truncate_error` (`commands/resilience.rs`) cuts at a byte boundary and
/// appends `…`. A value straddling the cut ships as a PREFIX, and a filter
/// that searches for whole values matches nothing — the tail it needs is the
/// part that was cut off.
///
/// The record persisted here is shaped as an OLDER binary would have written
/// it: the raw fragment, unredacted. That is deliberate. A binary with the fix
/// redacts before truncating and never writes this, so the only way a fragment
/// reaches the wire is from a record already on disk — which is exactly the
/// case a read-path filter has to handle.
///
/// The probe is repeated at staggered offsets rather than placed at a computed
/// position. `MAX_ATTEMPT_ERROR_LEN` is private, and hardcoding its value
/// would stop straddling the moment someone tunes it — passing while proving
/// nothing, which is the failure this whole sweep exists to avoid.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_attempt_error_ships_no_fragment_of_a_secret() {
    // SAFETY: a constant, set before the server is built. See the sweep above.
    unsafe { std::env::set_var(PROBE_VAR, PROBE) };

    let dir = tempfile::tempdir().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let state_path = models_dir.join(rocky_core::state::STATE_FILE_NAME);

    // A cut somewhere in this string bisects one of the repeats, whatever the
    // limit is. Whole occurrences before the cut are ordinary leaks the
    // value-level filter already handles; the straddled one is the point.
    let mut long = String::new();
    while long.len() < 4000 {
        long.push_str("filler-");
        long.push_str(PROBE);
    }
    // Shaped like the producer's output: a cut, then the sentinel.
    let truncated = format!("{}…", &long[..600]);
    assert!(
        truncated.contains(&PROBE[..MAX_SURVIVING_FRAGMENT]),
        "the fixture must actually straddle a repeat, or it proves nothing"
    );

    let run = serde_json::json!({
        "run_id": "run-truncation-probe",
        "started_at": "2026-09-11T00:00:00Z",
        "finished_at": "2026-09-11T00:00:01Z",
        "status": "Failure",
        "trigger": "Manual",
        "config_hash": "h",
        "models_executed": [{
            "model_name": "probe_model",
            "started_at": "2026-09-11T00:00:00Z",
            "finished_at": "2026-09-11T00:00:01Z",
            "duration_ms": 1000,
            "rows_affected": null,
            "status": "failed",
            "sql_hash": "h",
            "attempts": [{
                "attempt": 1,
                "outcome": "failed",
                "failure_class": "transient",
                "error": truncated,
                "duration_ms": 1000
            }]
        }]
    });
    let run: rocky_core::state::RunRecord =
        serde_json::from_value(run).expect("the record shape is the producer's");

    {
        let store = rocky_core::state::StateStore::open(&state_path).unwrap();
        store.record_run(&run).expect("persisted");
    }

    let state = ServerState::with_auth(
        models_dir,
        None,
        None,
        None,
        Vec::new(),
        Some(state_path.clone()),
    );
    let base = spawn(state).await;
    let body = reqwest::get(format!("{base}/api/v1/runs"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();

    // Positive control: the record really is on the wire.
    assert!(
        body.contains("run-truncation-probe"),
        "the persisted run is not in the response, so this proves nothing: {body:.400}"
    );

    // THE ATTEMPT TRAIL DOES NOT REACH THIS ROUTE, and that is the finding
    // this test pins rather than a leak.
    //
    // `GET /api/v1/runs` answers `HistoryOutput` -> `RunHistoryRecord` ->
    // `RunModelRecord`, and that last struct carries exactly model_name,
    // duration_ms, rows_affected, status and recipe_identity
    // (`output.rs:2487-2497`). No `attempts`, no `error`. So a truncated
    // attempt error cannot ship here however long it is.
    //
    // The trail reaches the wire by a different road:
    //
    //     AttemptRecord.error
    //       -> MaterializationOutput.attempts   (output.rs:1061, on RunOutput)
    //       -> `rocky run --output json` stdout
    //       -> captured as PersistedJob.result
    //       -> GET /api/v1/jobs/{id}
    //
    // which is the jobs surface, not the history surface. The fragment
    // fixture therefore belongs on the job-result path, and is owed there.
    //
    // Asserting the absence here KEEPS THIS HONEST: without it the test would
    // pass because the data never arrives, and read as proof that a filter
    // handled it. If `attempts` is ever added to `RunModelRecord`, this fails
    // and the fragment assertion below becomes live and meaningful.
    assert!(
        !body.contains('…'),
        "GET /api/v1/runs now carries a truncation sentinel, so the attempt \
         trail has reached this route. The fragment assertion below is now \
         live: make it the real check and delete this guard."
    );
    let survived = longest_surviving_prefix(&body, PROBE);
    assert_eq!(
        survived, 0,
        "GET /api/v1/runs carried {survived} bytes of the resolved value of \
         {PROBE_VAR}, which its response structs have no field to hold. \
         Something else on this route is rendering the state record."
    );
}

// ---------------------------------------------------------------------------
// Jobs: what a persisted job result may carry back
// ---------------------------------------------------------------------------

/// The version below which a record is not trusted to have been redacted.
/// Mirrors `MIN_TRUSTED_REDACTION_VERSION`, which does not exist yet — the
/// records below are built as JSON, so the key is simply ignored until the
/// field lands and starts populating. Forward-compatible by the same property
/// `PersistedJob`'s own doc relies on: every field defaults.
const MIN_TRUSTED_REDACTION_VERSION: u32 = 1;

/// Build a persisted job carrying the probe in both `result` and `error`.
///
/// JSON rather than a struct literal so `redaction_version` can be set before
/// the field exists: an unknown key is ignored today and populates the moment
/// `PersistedJob` gains it. No stub, no waiting, and the test is red for the
/// right reason in between.
fn planted_job(job_id: &str, redaction_version: Option<u32>, payload: &str) -> serde_json::Value {
    let mut job = serde_json::json!({
        "job_id": job_id,
        "kind": "run",
        "state": "failed",
        "submitted_at": "2026-09-11T00:00:00Z",
        "started_at": "2026-09-11T00:00:00Z",
        "finished_at": "2026-09-11T00:00:05Z",
        "principal": "probe",
        "error": format!("the run failed: {payload}"),
        "result": {
            "version": "1.73.0",
            "command": "run",
            "materializations": [{ "model": "probe_model", "detail": payload }]
        }
    });
    if let Some(v) = redaction_version {
        job["redaction_version"] = serde_json::json!(v);
    }
    job
}

/// A persisted job must not hand back a resolved secret, and a record written
/// before the redaction existed must hand back no payload at all.
///
/// Four records, covering both sides of the legacy comparison. Planting only
/// the absent case would leave the `< MIN_TRUSTED` half untested while the
/// test passed — the same half-covered branch that made the first frontmatter
/// fixture prove nothing.
///
///     None      legacy    the real pre-fix shape
///     Some(0)   legacy    below the floor; absent-only would miss it
///     Some(1)   trusted   current
///     Some(999) trusted   written by a NEWER engine
///
/// `Some(999)` is trusted on purpose: versions are monotonically
/// non-decreasing in strictness, so a newer engine redacted at least as hard,
/// and refusing its records would make a downgrade lose data that is not at
/// risk. That rule is a contract, not an observation — a future rule that
/// SHOWS more needs a different mechanism, not a higher number.
#[tokio::test(flavor = "multi_thread")]
async fn a_persisted_job_hands_back_no_secret_and_no_legacy_payload() {
    // SAFETY: a constant, set before the server is built. See the sweep above.
    unsafe { std::env::set_var(PROBE_VAR, PROBE) };

    let dir = tempfile::tempdir().unwrap();
    let models_dir = dir.path().join("models");
    std::fs::create_dir_all(&models_dir).unwrap();
    let state_path = models_dir.join(rocky_core::state::STATE_FILE_NAME);

    // A whole value, and a value cut by the truncation sentinel. The second is
    // the shape a filter searching for WHOLE values cannot see.
    let whole = PROBE.to_string();
    let fragment = format!("{}…", &PROBE[..PROBE.len() - 6]);

    let planted = [
        ("job-legacy-none", None, &whole),
        ("job-legacy-zero", Some(0), &whole),
        ("job-trusted-one", Some(1), &whole),
        ("job-trusted-future", Some(999), &whole),
        ("job-legacy-fragment", None, &fragment),
    ];

    {
        let store = rocky_core::state::StateStore::open(&state_path).unwrap();
        for (id, version, payload) in &planted {
            let job: rocky_core::state::PersistedJob =
                serde_json::from_value(planted_job(id, *version, payload))
                    .expect("the record shape is the producer's");
            store.record_job(&job).expect("persisted");
        }
    }

    // A SECOND open, so every read below crosses a restart: the store is
    // reopened and the server is built from scratch, exactly as a restarted
    // sidecar would find it.
    let state = ServerState::with_auth(
        models_dir,
        None,
        None,
        None,
        Vec::new(),
        Some(state_path.clone()),
    );
    let base = spawn(state).await;
    let client = reqwest::Client::new();

    for (id, version, payload) in &planted {
        let resp = client
            .get(format!("{base}/api/v1/jobs/{id}"))
            .send()
            .await
            .unwrap_or_else(|e| panic!("{id} did not answer: {e}"));
        assert_eq!(
            resp.status(),
            200,
            "{id}: the planted record must survive the restart and be readable"
        );
        let body = resp.text().await.unwrap();

        // Positive control: the record really came back. Without it, a 404 or
        // an empty body would satisfy every absence assertion below.
        assert!(
            body.contains(id),
            "{id}: the response does not name the job, so nothing below proves \
             anything: {body:.300}"
        );
        assert!(
            body.contains("\"state\""),
            "{id}: lifecycle metadata must always come back: {body:.300}"
        );

        let legacy = version.is_none_or(|v| v < MIN_TRUSTED_REDACTION_VERSION);
        if legacy {
            // Hugo's ruling: a record written before the redaction existed
            // returns lifecycle metadata only. `error` is refused, not shown.
            assert!(
                !body.contains("\"result\""),
                "{id}: redaction_version {version:?} is legacy, so `result` must \
                 be omitted — its contents were never scrubbed: {body:.300}"
            );
            assert!(
                !body.contains("\"error\""),
                "{id}: redaction_version {version:?} is legacy, so `error` must \
                 be omitted rather than shown: {body:.300}"
            );
        }

        // Whatever the version, no secret and no fragment of one may come back.
        assert!(
            !body.contains(PROBE),
            "{id}: the resolved value of {PROBE_VAR} came back on \
             GET /api/v1/jobs/{{id}}: {}",
            excerpt_of(&body, PROBE)
        );
        let survived = longest_surviving_prefix(&body, PROBE);
        assert!(
            survived < MAX_SURVIVING_FRAGMENT,
            "{id}: {survived} bytes of the resolved value came back. A \
             whole-value filter cannot see a truncated one — the tail it needs \
             is the part that was cut off. Payload was {payload:.60}"
        );
    }
}
