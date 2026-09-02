//! The Rocky MCP server: tool definitions, the `ServerHandler` impl, and the
//! projection helpers that map Rocky's typed `*Output` cores into the lite,
//! schemars-1.x result types in [`crate::result_types`].

use std::path::{Path, PathBuf};

use rmcp::ErrorData as McpError;
use rmcp::RoleServer;
use rmcp::handler::server::router::prompt::PromptRouter;
use rmcp::handler::server::router::tool::ToolRouter;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    GetPromptResult, Implementation, PromptMessage, ProtocolVersion, Role, ServerCapabilities,
    ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{
    Json, ServerHandler, prompt, prompt_handler, prompt_router, tool, tool_handler, tool_router,
};

use rocky_cli::commands;
use rocky_compiler::compile::{self, CompileResult as CompilerResult, CompilerConfig};
use rocky_core::product::commit::{read_no_follow_bytes, write_no_follow};

use crate::error::{ToolError, ToolResult};
use crate::result_types::*;

/// The server's `instructions` — the agent-authoring workflow. Sourced from
/// the single canonical skill so the MCP guidance never drifts from the
/// `rocky-ai-workflow` skill. Path is relative to this source file:
/// `crates/rocky-mcp/src` → repo root is four `..` segments.
///
/// The default profile serves this verbatim; the worker profile serves
/// [`WORKER_INSTRUCTIONS_BANNER`] + this (see
/// [`RockyMcpServer::get_info`]) — the skill file itself stays canonical and
/// untouched.
const INSTRUCTIONS: &str = include_str!("../../../../.claude/skills/rocky-ai-workflow/SKILL.md");

/// Prepended to the served `instructions` under the worker profile.
///
/// DERIVED from the excluded set, not written out. The old banner named six
/// tools by hand while the profile excluded nineteen — the same hand-picked
/// literal this round removed from the wire test one surface over. A tool
/// that leaves [`WORKER_PROFILE_TOOLS`] now appears here automatically.
///
/// The banner is the ONE worker surface that names excluded tools on
/// purpose: saying "`propose` is not available" is the opposite of steering
/// at it. Everything below the banner is the projected skill body, which
/// names none of them ([`WORKER_INSTRUCTIONS_REWRITES`]).
///
/// It leads with what is absent, then states the two things the projection
/// cannot state as forcefully in flowing prose: checks / contracts /
/// metadata are spec-owned by ANY route, and every workflow ends at the
/// typed hand-off.
fn worker_instructions_banner(excluded: &[String]) -> String {
    let names = excluded.join(", ");
    format!(
        "WORKER PROFILE ACTIVE. This server serves the minimal drafting allowlist. These \
         tools are NOT available in this session: {names}. The workflow below has been \
         rewritten for this profile. CHECKS, CONTRACTS AND METADATA ARE SPEC-OWNED HERE: \
         do not author one, and do not append one to a sidecar by any other route either. \
         Report what you would assert — name the column, the invariant, and the evidence \
         — and let the trusted runner encode it. Where the workflow reaches recording a \
         plan, reviewing it, approving it, or applying it, STOP. End every workflow at \
         the typed hand-off to the trusted runner: report the drafted files, what you \
         verified, and anything you flagged. The runner records, reviews, and applies.\n\n"
    )
}

/// The worker-profile projection of the served [`INSTRUCTIONS`], as
/// `(sentence to replace, replacement)`.
///
/// WHY THIS EXISTS. The instructions were the one surface left EXEMPT from
/// the excluded-name sweep, on the argument that a disclaiming banner over
/// verbatim text is honest. The F3 red team round 2 rejected that, and was
/// right: the banner stopped the worker at contract authorship, metadata
/// authorship, and the record/review/apply chain — but NOT at CHECK
/// authorship, while the text below it actively told the worker to
/// strengthen assertions, append tests through the draft tools, and encode
/// invariants as checks. So the largest guidance surface instructed exactly
/// the thing removing `draft_check` from the allowlist exists to stop, and a
/// worker-written check is raw SQL the fulfillment loop later runs with
/// warehouse credentials.
///
/// Two facts make it worse than a stale sentence. `rocky mcp --profile
/// worker` is directly invocable, and the driver's task brief is
/// out-of-band and optional — so the served instructions are the ONLY
/// guidance surface GUARANTEED to reach a worker session. And "honest, not
/// safe" is not a status this list should ever carry: every other row is
/// held to what the text SAYS, not to whether a disclaimer precedes it.
///
/// WHAT THE NINTH ROUND ADDED, and the reason it is the most useful entry
/// in this table's history. The first pass rewrote every passage that NAMED
/// an excluded tool, and a name-based sweep then read the result as clean.
/// It was not: two sentences that name no tool at all still told the worker
/// to write a model's `.toml` sidecar "for materialization", strategy and
/// target included. That contradicts the banner's spec-owned-metadata
/// prohibition sitting three paragraphs above it, and it routes around
/// `draft_model`, which deliberately writes only a minimal `name` + `intent`
/// document and never invents routing.
///
/// THE LESSON, stated because it generalises past this table: a rewrite that
/// strips a tool NAME while leaving the INSTRUCTION passes every name-based
/// sweep and changes nothing. Read each entry by asking what the served text
/// now tells the worker to DO — not which words it no longer contains. The
/// three further entries that came out of re-reading on that lens are the
/// evidence it is not a one-off: the umbrella sentence granted the whole
/// `rocky` CLI, the sampling step sent the worker at a raw database query,
/// and the product section handed it the runner's posture verbs. One of
/// those three quotes `propose_only`, which is exactly the string the
/// identifier rule was fixed NOT to match — a name-based sweep could never
/// have reached it.
///
/// WHAT THE TENTH ROUND ADDED — the CONTRADICTION, which is a different
/// failure from a stale steer. The ninth round's umbrella rewrite forbids
/// shell routes CATEGORICALLY ("a shell, a file you write yourself, a
/// direct warehouse connection — is out of bounds, even where nothing stops
/// you"), and four `rocky <verb>` imperatives were then deliberately KEPT,
/// on the argument that they name actions this profile serves and only the
/// ROUTE differs. That discriminator is true and it does not survive its
/// own banner: served text that forbids a route and then instructs it four
/// times is not followable, whichever half is right.
///
/// It is closed by REWRITING them to the served action rather than by
/// narrowing the banner, and the reason is the reader again. A banner that
/// forbade shell routes only to WITHHELD actions would make the worker
/// classify each route at read time, and nothing served to it supports that
/// classification: the banner lists withheld TOOLS, not withheld CLI verbs,
/// and the mapping between the two is not served at all. Rewriting needs no
/// judgement from the reader. The projected body now carries NO `rocky
/// <verb>` invocation, which is what makes the banner true rather than
/// nearly true, and a derived scan asserts it.
///
/// WHAT THE SCAN CHECKS, stated here because the eleventh round's finding 3
/// was that this paragraph out-claimed it. It takes every
/// identifier-bounded `rocky` in the lowercased body — so unbackticked and
/// any case — and refuses the word that follows unless a reviewed prose
/// list exempts it. It does NOT know the CLI's verb set: that enum is
/// private to the `rocky` binary crate, which depends on this one. The
/// guarantee is therefore "no unexempted word follows `rocky`", which is
/// wider than the old literal and still not the same sentence as "the CLI
/// is unreachable".
///
/// A PROJECTION, NOT A FORK. The canonical `.claude/skills/rocky-ai-workflow`
/// file is untouched — it is correct guidance for the default profile, where
/// the record/review/apply chain is the real workflow, and it is mirrored
/// byte-identically into `.agents/skills/` under a CI drift gate. What
/// changes is what the WORKER is served. Twenty-seven passages are rewritten
/// out of a 74-line document; the authoring loop itself — inspect, sample,
/// write, compile-loop, preview, read the JSON, the anti-patterns — is
/// served unchanged, because that part is the same job in both profiles.
/// What the worker may no longer do is reach any of those steps by a route
/// this server does not serve.
///
/// Both drift directions REFUSE at CONSTRUCTION, exactly like
/// [`WORKER_TOOL_DESCRIPTIONS`]: a needle that no longer matches aborts
/// startup rather than silently serving the default sentence to a worker,
/// and a needle that matches more than once aborts rather than rewriting a
/// passage nobody reviewed. An edit to the skill therefore forces a
/// conscious re-projection instead of quietly re-opening this hole.
///
/// CONSTRUCTION, not compile time. The operands are compile-time constants,
/// so nothing here depends on user input — but nothing verifies the match
/// until a server is built. An edit to the skill compiles and then refuses
/// at `rocky mcp --profile worker` startup. A TEST is what keeps the frozen
/// constants lined up; see [`RockyMcpServer::try_new_with_profile`].
///
/// # The rule a replacement has to satisfy
///
/// A needle usually spans a WHOLE passage, so a replacement inherits every
/// sentence in it — including the ones that WARN. Round seventeen, finding 2
/// is what this rule is made of: the step-5 passage carried "the preview
/// omits declared surrogate-key columns", the worker replacement did not,
/// and a table built to remove FALSE claims had silently removed a TRUE
/// warning. A worker then approved preview SQL with no caveat at all.
///
/// So, per pair: a caveat in the needle that still applies to the worker
/// must appear in the replacement. Only a caveat about something the worker
/// cannot reach may be dropped, and dropping it is then not a loss.
///
/// The whole table was swept against that rule when the rule was written.
/// Every remaining drop is a caveat about a surface this profile does not
/// serve — `rocky review`'s approval marker, `rocky product verify`,
/// `draft_metadata`'s parse guard, `rocky plan`'s replication-only preview
/// artefacts. The one real loss was step 5's, and it was fixed by removing
/// the divergence rather than by copying the sentence.
const WORKER_INSTRUCTIONS_REWRITES: &[(&str, &str)] = &[
    // The frontmatter `description` — the first thing in the served text.
    (
        "compile-loop → plan → propose → review → apply workflow",
        "compile-loop → plan → hand-off workflow",
    ),
    // The umbrella licence. It grants the CLI, and the CLI is every verb
    // this profile withholds — so the sentence hands back by one route what
    // the allowlist removed by another.
    (
        "It assumes you can run the `rocky` CLI (or call the equivalent tools) and read its \
         `--output json`.",
        "It assumes you call the tools this server serves and read their JSON results. Those \
         tools are your whole surface here. Reaching the same effect by another route — a \
         shell, a file you write yourself, a direct warehouse connection — is out of bounds, \
         even where nothing stops you.",
    ),
    // TENTH ROUND, finding 1D — the CLI pointer the umbrella rewrite above
    // left standing. That rewrite now says a shell is out of bounds; the
    // very next clause sent the worker off to read the full CLI command
    // surface.
    (
        "For the config format see the `rocky-config` skill; for the full command surface \
         see the `rocky` skill.",
        "For the config format see the `rocky-config` skill.",
    ),
    // The thesis sentence.
    (
        "The shape of the job: **you propose, Rocky's compiler verifies, an approval marker \
         gates the apply.**",
        "The shape of the job: **you draft, Rocky's compiler verifies, and the trusted runner \
         takes it from there.**",
    ),
    // NINTH ROUND, finding 1 — the first of two sidecar-authorship steers,
    // and the reason the round blocked. Every rewrite above strips a tool
    // NAME; this sentence names none and still tells the worker to write a
    // sidecar "for materialization". It contradicts the banner's
    // spec-owned-metadata prohibition and bypasses `draft_model`, which
    // deliberately writes only a minimal `name` + `intent` document and
    // never invents routing.
    (
        "Write models as **raw SQL** (`models/<name>.sql` + a `<name>.toml` sidecar for \
         materialization).",
        "Write models as **raw SQL**, and write them with the `draft_model` tool rather than \
         by editing files yourself. It writes `models/<name>.sql` for you, and creates only a \
         minimal `name` + `intent` sidecar — it never invents a strategy or a target, because \
         routing is spec-owned here.",
    ),
    // TENTH ROUND, finding 1D — the four surviving CLI imperatives, and
    // the reason they are REWRITTEN rather than excused.
    //
    // The previous round kept them on the argument that `rocky compile`,
    // `rocky plan` and `rocky test` name actions this profile SERVES, so
    // only the ROUTE differed. That discriminator is real, and it is not
    // available here: the umbrella rewrite four entries up forbids shell
    // routes CATEGORICALLY — "a shell, a file you write yourself, a direct
    // warehouse connection — is out of bounds, even where nothing stops
    // you". Text that forbids a route and then instructs it four times is
    // not a projection a worker can follow, whichever half is right.
    //
    // Of the two ways to close it, this is the one that REMOVES the
    // contradiction. Narrowing the banner to forbid shell routes only to
    // WITHHELD actions relocates it: the worker would have to classify
    // each route at read time, and the served text gives it nothing to
    // classify against — the banner names withheld TOOLS, not withheld
    // CLI verbs, and the mapping between them is not served at all.
    // Rewriting to the served action needs no such judgement, and it is
    // the rule every other entry in this table already follows.
    //
    // The projected body now contains NO `rocky <verb>` invocation at all,
    // which is what makes the banner true rather than nearly true. That is
    // asserted as a derived scan, not as a per-string check — see
    // `worker_instructions_are_projected_and_default_stays_verbatim`, and
    // read the bound stated there: the scan refuses any unexempted word
    // after `rocky`, in any case and backticked or not, but it does not
    // know the CLI's verb set.
    (
        "Run `rocky compile --output json`. The result gives you every existing model and \
         source table with its typed columns.",
        "Call the `inspect_schema` tool. It returns every existing model and source table \
         with its typed columns.",
    ),
    // The sampling route. The named alternatives are a direct database
    // connection and a raw SQL shell, which is the capability the whole
    // allowlist exists to withhold — `sample_rows` and `profile_column` are
    // the bounded reads this profile serves instead.
    (
        "On the DuckDB playground that's a direct query (`duckdb <path> \"SELECT * FROM \
         <table> USING SAMPLE 20 ROWS\"`) or `rocky shell`; against a warehouse, sample \
         through the adapter.",
        "Use the `sample_rows` tool for that, and `profile_column` to measure one column's \
         null rate, distinct count, and range. Those two are the whole sampling surface here: \
         do not open a database connection of your own, and do not run SQL you wrote against \
         the warehouse.",
    ),
    // NINTH ROUND, finding 1 — the second sidecar-authorship steer, and the
    // more explicit of the two: it names the strategy and the target.
    (
        "3. **Write the model.** Author the SQL and its `.toml` sidecar (materialization \
         strategy, target). Keep it minimal and readable.",
        "3. **Write the model.** Author the SQL, and hand it to `draft_model`. The SQL is your \
         whole surface — do not write the `.toml` sidecar, and do not choose a materialization \
         strategy or a target. Both are spec-owned here, and `draft_model` resolves them from \
         the project's conventions. Keep the SQL minimal and readable.",
    ),
    // TENTH ROUND, finding 1D — imperatives two and three.
    (
        "Run `rocky compile --output json` and read `diagnostics`",
        "Call the `compile` tool and read its `diagnostics`",
    ),
    // FIFTEENTH ROUND, finding 1 — the round-fourteen sweep narrowed
    // `plan_preview`'s exactness claim on the tool description, both
    // `build_model` bodies and the published tool table, and MISSED this
    // one. The rewrite carried "exactly what would execute" straight from
    // the CLI sentence onto the tool, which is the surface the claim is
    // least true of: `commands::plan_preview_output` renders offline and
    // DROPS any model `sql_gen` cannot render, and `PlanPreviewResult` has
    // no field that names one.
    //
    // The needle is now the whole step, because the default sentence it
    // replaces carries three CLI routes (`rocky emit-sql`, `rocky plan` and
    // `rocky apply`) and a partial rewrite would leave one standing for the
    // verb scan in
    // `worker_instructions_are_projected_and_default_stays_verbatim`.
    //
    // SIXTEENTH ROUND, finding 2 — the DEFAULT sentence changed under this
    // needle, and the change is bigger than the wording. The old step told
    // every default and approver client to "Run `rocky plan`", called the
    // result offline, and said a model it cannot render is skipped. Three
    // things were wrong at once, and only the first was reported:
    //
    //  - `rocky plan` is NOT offline. `commands::plan` builds an
    //    `AdapterRegistry`, calls `discovery_adapter.discover()`, and its
    //    own budget-check comment says plan "already performs live
    //    warehouse I/O (discovery, governance)".
    //  - Bare `rocky plan` never prints a TRANSFORMATION model's SQL at
    //    all. Every `output.statements` push in `plan()` is in the
    //    replication loop; the one transformation site is gated behind
    //    `if let Some(model) = run_options.model`.
    //  - On a transformation-only project — the shape steps 1-4 teach you
    //    to build — `rocky plan` REFUSES, with or without `--model`:
    //    `registry::resolve_replication_pipeline` rejects a non-replication
    //    pipeline. So the step sent an agent at a command that exits 1.
    //
    // Verified by running the binary against
    // `examples/playground/pocs/00-foundations/00-playground-default`, not
    // by reading the call graph. The skipping claim was true of the OFFLINE
    // preview core the step never actually invoked.
    //
    // `rocky emit-sql` is the verb that does what the step asks: offline,
    // the models in dependency order, works on a transformation-only
    // project, and reports what it could not render on stderr instead of
    // dropping it silently. The default profile has no `emit_sql` TOOL, so
    // the sentence naming the MCP equivalent sits directly beside it.
    //
    // AND THE FIRST DRAFT OF THAT SENTENCE CALLED THEM EQUIVALENT, which
    // was a fresh over-claim inside the commit removing one. They did NOT
    // render the same SQL: `emit_sql::emit_models` called
    // `rocky_core::models::apply_surrogate_keys` per model and
    // `plan_preview_output` never did, so on a model with
    // `[[surrogate_key]] name = "order_key"`:
    //
    //   emit-sql      SELECT *, CAST(md5(...) AS VARCHAR) AS order_key
    //                 FROM ( SELECT ... ) AS __rocky_keyed
    //   plan_preview  SELECT ...                     (no order_key)
    //
    // SEVENTEENTH ROUND, finding 2 — that divergence is now FIXED at the
    // source rather than described here. `plan_preview_output` applies the
    // declared keys, like `emit-sql` and like the run, and
    // `emit_sql::tests::plan_preview_and_emit_sql_render_the_same_keyed_sql`
    // asserts the two renderings are EQUAL. The warning sentence this
    // comment justified is gone from the served text.
    //
    // Why parity rather than a warning: the warning lasted one round. It
    // sat inside the block this table replaces for workers, and the
    // replacement did not carry it — so a worker approved preview SQL with
    // no caveat at all. Any true statement about a divergence has to be
    // repeated on every surface that serves the preview; removing the
    // divergence has nothing to repeat.
    (
        "Read your model's generated SQL before you ship it. `rocky emit-sql` renders it \
         offline: no live source schema, no compute warehouse. It prints the models in \
         dependency order and reports on stderr any it could not render. Over MCP the nearest \
         tool is `plan_preview`. It renders offline too, but it drops what it cannot render \
         without naming it. `rocky plan` is a different command, not this step. It \
         needs a replication pipeline, connects to the source to discover tables, and prints \
         replication SQL. It refuses a transformation-only project. Bare `rocky plan` never \
         prints a transformation model's SQL; `rocky plan --model <name>` does, through that \
         same preview core. In replication SQL an incremental table previews the 1970 sentinel \
         watermark, not the real one. A `MERGE` on any dialect but Databricks previews a \
         canonical shape, not the column list the runner resolves at execute time. And `rocky \
         apply` recompiles the project rather than replaying the file. Confirm the SQL you read \
         matches your intent.",
        "Call the `plan_preview` tool and read the SQL it returns. It renders offline. It \
         is not the whole plan: a model whose SQL cannot be rendered offline is SKIPPED, \
         and the result does not name it. So a model missing from the statements means \
         'not renderable offline', never 'nothing to do'. Three are skipped by \
         construction: a Snowflake dynamic table needs a live compute warehouse, a \
         time-interval model needs a runtime window, and a content-addressed model never \
         goes through SQL generation. Confirm the SQL it does return matches your intent.",
    ),
    // TENTH ROUND, findings 1B and 1D together — the fourth imperative,
    // whose replacement also has to be exact about WHICH suite it runs.
    // `rocky test` and the `test` tool are the same code path
    // (`commands::test_output` → `rocky_engine::test_runner`), and neither
    // "exercises assertions (uniqueness, not-null, accepted values,
    // ranges)": `run_tests` compiles the project and materializes every
    // model against an in-memory DuckDB, and `run_unit_tests` runs the
    // fixture-driven `[[test]]` blocks. The declarative check set is
    // `rocky test --declarative`, a different runner, and its checks need
    // an applied table besides. Rewriting the route while carrying the
    // wrong claim forward would ship a fresh false promise on the surface
    // this table exists to remove them from.
    (
        "**Test.** Run `rocky test` to exercise assertions (uniqueness, not-null, accepted \
         values, ranges).",
        "**Test.** Call the `test` tool. It compiles the project, materializes every model \
         against a local DuckDB, and runs the fixture-driven `[[test]]` blocks. That local \
         suite is the only one you can run here — the checks the product spec declares are \
         evaluated by the trusted runner after an apply.",
    ),
    // Step 6 — the first of the three check-authorship steers.
    (
        "Add or strengthen assertions that encode what you learned from sampling — they become \
         the contract that protects the model from future drift.",
        "Do NOT add or strengthen assertions here — checks are spec-owned in this session, and \
         the spec's grain and `checks` already lower into the sidecar for you. Report the \
         assertions your sampling justifies instead: the column, the invariant, and the \
         evidence.",
    ),
    (
        "## Shipping safely: propose → review → apply",
        "## Shipping safely: draft → verify → hand off",
    ),
    // TENTH ROUND, finding 1D — the section's opening paragraph, which
    // survived every previous pass because it names no excluded tool and
    // reads as a PROHIBITION. It still carries a `rocky <verb>` route, and
    // its last clause ("treat the review as yours to surface") casts the
    // worker as the one who surfaces the review — a role the three
    // rewrites below take away from it.
    (
        "**Never apply an AI-authored change directly.** A bare `rocky apply` of an \
         AI-authored plan is refused by design — an agent can confidently write a model \
         that drops a column or rewrites a result, so the apply waits on a review step. The \
         engine checks that an approval marker parses and names that exact plan. It does \
         not check who wrote the marker, so treat the review as yours to surface, not yours \
         to satisfy.",
        "**Nothing you draft is applied from this session.** An AI-authored change is \
         refused a bare apply by design — an agent can confidently write a model that drops \
         a column or rewrites a result, so the change waits on a human approval marker. The \
         engine checks that the marker parses and names that exact plan; it does not check \
         who wrote it. Obtaining that marker, and everything after it, belongs to the \
         trusted runner and happens outside this session.",
    ),
    // The numbered chain. Replaced whole: every step of it is the runner's.
    (
        "1. **Propose.** Generate the plan that materializes your change (it is recorded as an \
         *AI-authored* plan with a `plan_id`). A propose can also bind the plan to a product \
         identity — `product_id` plus `spec_digest`, both together or neither. A product-bound \
         plan refuses a bare `rocky apply`; the applier must pass `rocky apply <plan-id> \
         --expect-spec-digest <digest>` with the digest of the approved spec. When you do not \
         work for a product runner, omit both fields.",
        "1. **Hand off.** Report the drafted files, the compile result, the SQL you previewed, \
         and every invariant you would have encoded. That report is your last step.",
    ),
    (
        "2. **Review.** Run `rocky review <plan-id>`. This compiles your working tree against \
         the base ref and runs the semantic breaking-change classifier, then reports the delta \
         — added/removed/retyped columns, anything downstream consumers depend on. Read it.",
        "2. **The runner takes it from there.** Recording the plan, running the breaking-change \
         classifier over it, obtaining the human approval marker, and executing the change all \
         happen outside this session.",
    ),
    (
        "3. **Approve.** `rocky review <plan-id> --approve` writes the approval marker. \
         Approving over breaking changes is allowed. The marker is written even when the \
         classifier could not run: if either tree fails to compile, findings are absent and \
         `breaking_change_count` falls back to 0. So a marker is not evidence a delta was \
         computed — raise the findings explicitly.\n4. **Apply.** Only after the approval marker \
         exists does `rocky apply <plan-id>` execute.",
        "3. **Nothing you can do stands in for that.** A clean compile is not approval, and no \
         report you write is a sign-off. If you believe a change is urgent, say so in the \
         report.",
    ),
    (
        "Your job ends at *propose* and at *surfacing the review report clearly*. The approval \
         is a human decision; do not approve on the user's behalf unless they explicitly tell \
         you to.",
        "Your job ends at the typed hand-off, and at surfacing what you found clearly. The \
         approval is a human decision made outside this session.",
    ),
    // The second check-authorship steer — and the most direct one.
    (
        "Hand-editing them is detected as tampering. Your surface is the SQL, plus tests you \
         append through the draft tools.",
        "Hand-editing them is detected as tampering. Your surface is the SQL, and only the SQL. \
         Checks are spec-owned the same way: the spec's declared grain and `checks` lower into \
         the sidecar's `[[tests]]`, so there is nothing for you to append, by this server or by \
         any file you can write. An invariant the spec does not state belongs in your report.",
    ),
    // TENTH ROUND, finding 1D — the section's own opening sentence names a
    // CLI verb family. Descriptive rather than imperative, and rewritten
    // anyway: the derived scan below admits no `rocky <verb>` in the
    // projected body, and an exception "because this one is only context"
    // is the shape of argument the round rejected.
    (
        "drive fulfillment through `rocky product <verb>`.",
        "drive fulfillment through the product verbs.",
    ),
    // The product-posture verbs. Both are the runner's, neither is served
    // here, and the sentence is a live demonstration of the lesson this
    // round is about: `propose_only` does NOT match the identifier rule
    // (`_` is an identifier byte), so it passed every name-based sweep
    // while telling the worker to go inspect the gate posture.
    (
        "- `rocky product verify <name>` tells you (and the runner) whether the frozen \
         `propose_only` posture is in place before any drafting starts; `rocky product status \
         <name>` reports the lowering, approval, and state without writing.",
        "- The runner checks the frozen posture and the lowering state before your drafting \
         starts. Neither check is yours to run, and neither is served here. Work from the \
         files and the tool results in front of you.",
    ),
    (
        "- A product-bound propose carries `product_id` + `spec_digest` of the **approved** \
         revision, and the apply requires `--expect-spec-digest`. If the spec moves after your \
         draft, the generation is superseded — expect a refusal, not a merge of generations.",
        "- The runner binds its plan to the `spec_digest` of the **approved** revision. If the \
         spec moves after your draft, the generation is superseded — expect a refusal, not a \
         merge of generations.",
    ),
    // TENTH ROUND, finding 1D — "Every command takes `--output json`" is
    // the CLI framing of the machine-readable section, and the sentence
    // stops parsing once no CLI command is reachable.
    (
        "Every command takes `--output json`, backed by a typed schema.",
        "Every tool returns typed JSON, backed by a schema.",
    ),
    // TENTH ROUND, finding 1C — the retry instruction. It tells the worker
    // to branch on a run error's `failure_kind` and RETRY a `Transient`,
    // which presumes materializing a pipeline through a route this profile
    // does not serve: no worker tool runs one, so no run error can occur to
    // retry. Naming an outcome that cannot occur is the same defect class
    // as naming a tool that is not served, and the remedy is the same —
    // say what is actually true here.
    //
    // ELEVENTH ROUND, finding 2 — and the tenth round's replacement said
    // something ELSE that is not true. It read "no tool this profile serves
    // executes against the warehouse". Three do: `sample_rows` and
    // `profile_column` both require live credentials and issue queries, and
    // `inspect_schema` lists the warehouse's tables best-effort to ground a
    // source the project never declared. The correction to a contradiction
    // reached for a stronger claim than the contradiction needed, which is
    // this branch's signature defect.
    //
    // HAND-REVIEWED FROM THE ALLOWLIST, entry by entry, not read off the
    // sentence it replaces — and NOT derived, which the word "derived" here
    // used to imply. The same review now lives as a checked table,
    // [`WORKER_TOOL_EFFECTS`], so this prose has something to drift AGAINST
    // rather than being the only copy. Nine of the twelve reach no adapter —
    // `breaking_change` (git + an in-process compile), `catalog`,
    // `compile`, `dependents`, `draft_model`, `lineage`, `list`,
    // `plan_preview` (offline; it reads the config only to pick a dialect)
    // and `test` (its own in-memory DuckDB). The other three read. None of
    // the twelve runs or materializes a PIPELINE, and that — not "no
    // warehouse access" — is what makes a run error unreachable here.
    // (`sample_rows` and `profile_column` do run QUERIES; an earlier draft of
    // this note said "runs or materializes anything", which the served
    // sentence below had already been corrected away from.)
    //
    // TWELFTH ROUND, finding 2 — this enumeration stays; the COUNT that used
    // to open the served sentence does not. "Three tools do READ the
    // warehouse" asserts exhaustivity over the allowlist, and whether a tool
    // opens an adapter is a fact about its body, not about this list. It was
    // guarded with `WORKER_PROFILE_TOOLS.len() == 12`, which passes through
    // any behaviour change and any one-for-one swap. The sentence now says
    // "Some tools DO read the warehouse" and then names the ones a worker
    // actually reaches for, which is everything the number bought.
    //
    // THIRTEENTH ROUND, finding 2 — and deleting that length check left the
    // bullet's LEADING sentence, "No tool this profile serves runs or
    // materializes a pipeline", with no tripwire at all. The check held
    // nothing, but it did fail when the surface grew. [`WORKER_TOOL_EFFECTS`]
    // replaces it with a reviewed effect per served tool, cross-checked
    // against the router: a thirteenth tool fails the test until someone
    // reads its body and classifies it. Hand-reviewed, and labelled as such
    // — the opposite mistake to the three lists on this branch that looked
    // derived and were not.
    //
    // TWELFTH ROUND, finding 1 — and the round-eleven replacement then said
    // something ELSE that is not true, one sentence later. It promised that
    // a failed read "comes back as that tool's own error", which is a
    // universal over the three readers it had just named. Two of them keep
    // it: `sample_rows` and `profile_column` both propagate a failed read as
    // `ToolError::warehouse_error` — though only for their PRIMARY query;
    // see the thirteenth-round note below, which narrows this sentence.
    // `inspect_schema` does not. It is best-effort by design: a warehouse
    // Rocky cannot reach must not fail the tool, because `models` and the
    // compile-derived `sources` are exact on their own. Until #1565 it was
    // also SILENT — the call site discarded the `Err` arm and
    // `discover_source_tables` returned `Vec::new()` on a failed query, so a
    // worker holding the old promise read an empty `sources` as "no such
    // table". Since #1565 the result carries `discovery_incomplete` and
    // `discovery_error`, so the degradation is REPORTED; the tool still
    // returns success, so the served text still has to tell the worker to
    // read the flag rather than the list.
    //
    // THE CLAIM IS WHAT MOVES HERE, NOT THE TOOL. The text describes the
    // tool honestly — inconclusive, not authoritative, when the flag is set
    // — and points at the reader that DOES fail loudly for the same table.
    //
    // THE REMEDY SENTENCE RESTS ON A BEHAVIOUR, so name where that behaviour
    // is proven. "Ask `sample_rows` for that table" only works because
    // `prepare_table_query` routes a DOTTED target down the qualified-raw-ref
    // branch with no compile — a bare name would be looked up as a model and
    // refused. That is covered live over the wire by
    // `sample_rows_reaches_raw_source_by_qualified_ref` (tests/roundtrip.rs),
    // which samples `seeds.orders` when no model declares it. If that test
    // goes, or `sample_rows` starts requiring a compiled model name, this
    // sentence becomes wrong advice at the exact moment a worker needs it —
    // worse than the promise it replaced, because it is the fallback.
    //
    // THIRTEENTH ROUND, finding 1 — and the sentence that replaced the
    // over-claim carried a smaller one of its own. It said `sample_rows` and
    // `profile_column` BOTH "surface a failed read as that tool's own
    // error". True of `profile_column`'s counts; false of its `top_values`,
    // which is a SECOND warehouse query taking `Err(_) => Vec::new()` before
    // the tool returns success. So the same shape a third time, on the tool
    // named as the counter-example to it:
    //
    //   sample_rows      read fails ─▶ ToolError::warehouse_error
    //   profile_column   counts fail ─▶ ToolError::warehouse_error
    //                    top_values fail ─▶ empty list, Ok(success)
    //   inspect_schema   adapter or discovery fails ─▶ no tables, Ok(success),
    //                                                 discovery_incomplete = true
    //
    // Same scope rule as the round above: the CLAIM moves, not the tool.
    // `ProfileColumnResult` already carries `unavailable`/`reason`, so wiring
    // this one would be cheap — and it is still a change to the tool's
    // contract, which is not what this branch does. The text now calls
    // `top_values` best-effort and names the three states an empty list does
    // not tell apart, so a worker cannot read it as a fact about the column.
    (
        "Run **errors** carry a `failure_kind` (`Transient`, `AuthFailed`, `QueryRejected`, \
         `QuotaExceeded`, …) and sometimes a `cooldown_seconds`. Branch on *why* something \
         failed: retry a `Transient`, stop and surface an `AuthFailed`.",
        "Run **errors** are not something you will see. No tool this profile serves runs or \
         materializes a pipeline, so there is no run to retry and no `failure_kind` to branch \
         on. Some tools DO read the warehouse. `sample_rows` and `profile_column` query it \
         directly, and `inspect_schema` lists its tables when it can. Not every one of those \
         reads reports its own failure. `sample_rows` does: a read it cannot complete comes \
         back as that tool's own error, not as a run outcome. `profile_column` does for the \
         counts it returns, but its `top_values` is a second query and is best-effort — when \
         that query fails the tool still returns success with an empty list, and nothing in \
         the result says so. An empty `top_values` does not distinguish a high-cardinality \
         column, an all-null one, and a failed query. `inspect_schema` is best-effort too, \
         but it says so: when the warehouse cannot be read it reports none of its physical \
         tables, still returns success, and sets `discovery_incomplete` with the reason in \
         `discovery_error`. When that flag is set, a table missing from `sources` is \
         inconclusive, not proof the table is absent. Ask `sample_rows` for that table before \
         you conclude it is absent — it either reads the table or fails with a readable \
         error. A tool \
         result that reports a failure is a finding for your report — read it, name it, and \
         do not work around it.",
    ),
    // The third check-authorship steer.
    (
        "encode it as a **contract** (`required`/`protected` columns) or a **check** \
         (assertion), not just as a `WHERE` clause. That moves the invariant into the typed \
         substrate, so the human reviews *the invariant* and the compiler enforces it on every \
         future run.",
        "REPORT it. Name the column, the invariant, and the evidence. Do not bury it in a \
         `WHERE` clause, and do not encode it yourself: contracts and checks are spec-owned in \
         this session. The runner moves the invariant into the typed substrate, where the human \
         reviews *the invariant* and the compiler enforces it on every future run.",
    ),
    // The metadata section. Replaced WHOLE — the paragraph is a how-to for
    // a tool this profile does not serve, so trimming its first sentence
    // would leave the rest dangling off a tool that is no longer named.
    (
        "## Metadata is a governed write too\n\nFreshness expectations and column \
         classifications live in the model's sidecar (`models/<model>.toml`). To author them as \
         an agent, use the `draft_metadata` MCP tool — never string-append to the sidecar. It \
         takes a structured patch: a `freshness` block (`expected_lag_seconds`, optional \
         `time_column` and `severity`), a `classifications` map (column → tag, e.g. `email = \
         \"pii\"`), or both. The tool parses the sidecar as TOML and merges the patch \
         (`freshness` replaces the whole table; `classifications` merges per column), compiles \
         with the write, and checks your policy rules against the model **as patched** — a \
         patch that adds the first `pii` tag is judged by that tag. A denied patch restores the \
         prior sidecar exactly. A sidecar the tool cannot parse is never overwritten. Note the \
         trade: comments in the sidecar are dropped when it is re-serialized.",
        "## Metadata is spec-owned here\n\nFreshness expectations and column classifications \
         live in the model's sidecar (`models/<model>.toml`). They are lowered from the product \
         spec, so they are not yours to write in this session — not through this server, and \
         not by editing the file. If a column looks like it carries personal data, or a table \
         looks staler than the spec assumes, put that in your report and name the column. The \
         runner owns the write.",
    ),
    (
        "- Applying without review, or approving your own AI-authored plan.",
        "- Doing anything past the hand-off: recording, reviewing, approving, or applying.\n- \
         Writing a check, a contract, or a metadata block. Report the invariant instead.",
    ),
];

/// Worker-profile `prompts/list` descriptions (FF-WP1 fix round 2, item 5b):
/// the static `#[prompt(description = ...)]` strings instruct the DEFAULT
/// workflow (they name `propose`, contract authorship, and the `ai_*`
/// generators), so the worker profile rewrites every listed description at
/// construction to the drafting-loop shape that ends at the trusted-runner
/// hand-off. `summarize_project` is here too: its default description says
/// "no propose", and the worker surface must not name excluded verbs at all.
const WORKER_PROMPT_DESCRIPTIONS: &[(&str, &str)] = &[
    (
        "build_model",
        "Guide the authoring of one Rocky model from a plain-language intent: inspect schema -> \
         sample rows -> profile columns -> draft_model -> compile-loop -> plan preview -> \
         test. Worker profile: checks are spec-owned here, so report what you would assert \
         rather than writing it; ends at the typed hand-off to the trusted runner.",
    ),
    (
        "find_untested_models",
        "Find models with no declarative tests: catalog -> identify untested models -> ground \
         with sample_rows / profile_column -> describe the checks each one needs. Worker \
         profile: checks are spec-owned here, so this ends in a REPORT, not a write — name \
         the models, the columns, and the assertion each needs, and end at the typed \
         hand-off to the trusted runner.",
    ),
    (
        "add_tests_to_pks",
        "Identify a model's primary-key / unique columns and the uniqueness + not-null tests \
         they need: inspect_schema -> confirm the keys with profile_column. Worker profile: \
         checks are spec-owned here, so this ends in a REPORT, not a write — name the keys \
         you confirmed and the assertions they need, and end at the typed hand-off to the \
         trusted runner.",
    ),
    (
        "summarize_project",
        "Produce a structured, read-only summary of the Rocky project: catalog + lineage -> \
         grouped overview of models, their grain, governance, tests, and DAG shape. Read-only — \
         no edits, nothing recorded.",
    ),
    // TENTH ROUND, finding 1. This said "failing declarative tests: run
    // `test`", and the `test` tool does not run them — it runs the project's
    // LOCAL model and unit tests (`commands::test_output`). The declarative
    // check set is `rocky test --declarative`, a different path this profile
    // does not serve. Same false promise `WORKER_DRAFT_NEXT_STEPS` already
    // corrects one surface over; see that constant for the full reasoning.
    (
        "fix_failing_test",
        "Diagnose and fix failing local tests: run `test` — the project's LOCAL model and \
         unit tests, the only suite served here — then for each failure profile_column the \
         implicated columns to ground the cause -> redraft the model SQL with draft_model \
         where the SQL is wrong. Worker profile: ends at the typed hand-off to the trusted \
         runner.",
    ),
];

/// Stateless Rocky MCP server. Holds only the project locators; every tool
/// call recompiles from the current on-disk files (correctness over a warm
/// cache — caching is a deferred optimization).
#[derive(Clone)]
pub struct RockyMcpServer {
    config_path: PathBuf,
    models_dir: PathBuf,
    root: PathBuf,
    /// Which tool surface this server serves. Also read by the workflow
    /// prompts: the worker profile serves variants that end at the handoff to
    /// the trusted runner instead of instructing tools the profile excludes.
    profile: McpProfile,
    /// The `instructions` this profile serves, resolved at construction.
    ///
    /// Built here rather than in [`RockyMcpServer::get_info`] because the
    /// worker projection is a CHECKED rewrite that panics on drift, and it
    /// belongs at the same point as the prompt- and tool-description
    /// rewrites: one place where every profile's guidance is decided, and
    /// `get_info` stays a plain read.
    instructions: String,
    tool_router: ToolRouter<Self>,
    prompt_router: PromptRouter<Self>,
}

/// Which tool surface `rocky mcp` serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum McpProfile {
    /// Every tool, with ONE action withheld: `review_queue` lists the pending
    /// queue but REFUSES to approve (#1517). Approving writes the human
    /// sign-off marker `rocky apply` requires, so it is not a capability the
    /// no-flag command should hand an agent; [`Self::Approver`] is the
    /// explicit opt-in. Every other tool behaves exactly as before.
    #[default]
    Default,
    /// [`Self::Default`]'s tools, plus the `review_queue` APPROVE action
    /// (`rocky mcp --profile approver`). Serves the same tool NAMES as the
    /// default profile — the opt-in enables an action, it does not add a
    /// tool. Choose it only when the operator intends this server to be able
    /// to write sign-off markers; approval is still attributed to the
    /// operator's git identity, never to a verified human.
    Approver,
    /// The minimal drafting-worker allowlist (`--profile worker`): read /
    /// inspect grounding tools, the compile/test/breaking-change/dependents
    /// verification loop, `draft_model`, and the prompts.
    /// Everything else — including `draft_contract`, `draft_check`,
    /// `draft_metadata`,
    /// `review_queue`, `pause_schedule`, `propose`, and any FUTURE tool not
    /// explicitly allowlisted — is absent from the listing and returns
    /// tool-not-found when called.
    Worker,
}

/// The worker-profile tool ALLOWLIST — exhaustively enumerated, never derived
/// by exclusion, so a future tool addition is excluded by default and must
/// consciously opt in here (the golden profile tests pin both surfaces).
///
/// Rationale (FF-DESIGN D7 ⟦RTL-1,3⟧): the untrusted drafting worker needs
/// grounding reads and the compile/test loop for `models/<model>.sql`.
/// Contracts and metadata are spec-owned in the fulfillment loop — a
/// worker-writable contract detaches artifacts from the spec — and
/// approval/propose/schedule surfaces must never reach it. The in-engine LLM
/// generator tools (`ai_*`, `suggest_freshness_block`, `explain_model`) are
/// omitted too: the worker brings its own model, and the governed metadata
/// path is the runner's, not the worker's.
///
/// `draft_check` is DELIBERATELY absent, and the reason is worth stating
/// because the tool is harmless on the Default profile and was allowlisted
/// here originally. A `[[tests]]` block's `expression` is raw-interpolated
/// into `SELECT COUNT(*) FROM t WHERE NOT (<expression>)`
/// (`rocky_core::tests`), whose own comment says the caller is responsible
/// for sandboxing execution. That was satisfiable while the only caller was
/// a human typing `rocky test --declarative`. It is not satisfiable now that
/// the fulfillment loop evaluates the declared checks automatically after
/// every apply (FF-WP-F3), holding warehouse credentials with no human in
/// the loop — an untrusted worker able to append checks would be able to
/// author SQL the loop then runs unattended.
///
/// **What this closes, exactly.** Removing `draft_check` closes the MCP
/// route it was served over. It does NOT make a worker unable to author a
/// check: the subprocess driver runs an arbitrary command with the project
/// root as its working directory and no filesystem confinement, and Phase B
/// PRESERVES a worker-added `[[tests]]` block rather than discarding it
/// (`rocky_core::product::lowering`). A worker with a file writer can still
/// write the sidecar. That is the conceded local-process boundary, tracked
/// by #1491 (an OS sandbox for the worker) and #1515 (trusted custody) —
/// not something this allowlist claims to solve.
///
/// Nothing is lost by removing it: the product spec's declared grain and
/// `checks` already lower into the sidecar's `[[tests]]`, so the worker has
/// no need to append one. Hand-appending them was redundant.
///
/// Note what that does NOT say. It does not say the evaluated checks are
/// only the ones the operator approved — the paragraph above is the reason
/// why. A check written straight to the sidecar is PINNED, not rejected:
/// Phase B preserves it, and the digest the loop pins at `verifying` is
/// taken AFTER that merge, so the worker's check is inside the set the
/// generation verifies and is evaluated like any other.
///
/// The post-apply custody digest does not close that either, and claiming
/// it does would be the same over-claim one paragraph later. It compares
/// what is on disk against what the generation verified, so it catches a
/// set that MOVED after verification — not one that was already wrong when
/// the digest was taken. The allowlist closes a route; the digest closes a
/// window. Neither judges a check's content; that is #1515.
///
/// The tool remains on the Default profile, where the caller is an
/// operator-driven session rather than an untrusted drafting worker.
const WORKER_PROFILE_TOOLS: &[&str] = &[
    "breaking_change",
    "catalog",
    "compile",
    "dependents",
    "draft_model",
    "inspect_schema",
    "lineage",
    "list",
    "plan_preview",
    "profile_column",
    "sample_rows",
    "test",
];

/// What a worker-served tool does to the CONFIGURED TARGET warehouse.
///
/// The axis is the warehouse only. `Offline` does not mean "no side effects":
/// `draft_model` writes files under the models directory, and `test` runs a
/// DuckDB of its own. It means the tool never opens the adapter the project's
/// `rocky.toml` points at.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerToolEffect {
    /// Never opens the target adapter — compile, file, local-state or
    /// in-memory work only.
    Offline,
    /// Opens the target adapter and runs a READ query. No writes, no DDL.
    ReadsWarehouse,
    /// Executes or materializes a pipeline node against the target.
    RunsPipeline,
}

/// The reviewed effect of every tool the worker profile serves.
///
/// THIRTEENTH ROUND, finding 2. The worker instructions open with a
/// universal — "No tool this profile serves runs or materializes a pipeline"
/// — over twelve hand-chosen names. Round twelve guarded it with
/// `WORKER_PROFILE_TOOLS.len() == 12`, which held nothing (a swap or a
/// behaviour change keeps the length), and deleting that guard left the
/// universal with no tripwire at all. This is the replacement: a classified
/// entry per served tool, cross-checked against the ROUTER by
/// `every_worker_served_tool_is_classified_and_none_runs_a_pipeline`. A
/// thirteenth tool reaching the worker surface fails that test until someone
/// classifies it.
///
/// HAND-REVIEWED, AND SAYING SO IS THE POINT. Nothing derives this. Each
/// entry was read against its tool body: the three `ReadsWarehouse` ones
/// reach `warehouse_adapter()` / `prepare_table_query()`; the nine `Offline`
/// ones call a sync `commands::*_output` helper over the models directory,
/// the config, or the local state store. The sync signature is NOT the
/// argument — the `duckdb` crate is synchronous, so sync does not imply
/// offline. The bodies are.
///
/// WHAT THIS GUARD DOES NOT HOLD, stated rather than left to be found: that
/// each classification is TRUE. It holds that nothing served is
/// unclassified. If `commands::test_output` ever executes against the
/// configured target instead of its own in-memory DuckDB, this table is
/// silently wrong and the test stays green. Deriving the answer would take a
/// call graph over this file, which is a fourth thing that would look
/// derived and go wrong at the first indirection.
#[cfg(test)]
const WORKER_TOOL_EFFECTS: &[(&str, WorkerToolEffect)] = &[
    // Compile + `breaking_change` classifier over the models directory.
    ("breaking_change", WorkerToolEffect::Offline),
    // `compute_catalog_output` — models directory plus the local state store.
    ("catalog", WorkerToolEffect::Offline),
    ("compile", WorkerToolEffect::Offline),
    ("dependents", WorkerToolEffect::Offline),
    // Writes `models/<name>.sql` and its sidecar. A file effect, not a
    // warehouse one.
    ("draft_model", WorkerToolEffect::Offline),
    // `warehouse_adapter()` then `discover_source_tables` — both best-effort,
    // which is the defect the worker text discloses rather than fixes.
    ("inspect_schema", WorkerToolEffect::ReadsWarehouse),
    ("lineage", WorkerToolEffect::Offline),
    // `list_{models,pipelines,adapters,sources}_output` — config and files.
    ("list", WorkerToolEffect::Offline),
    // Reads the config only to pick a dialect; generates SQL, submits none.
    ("plan_preview", WorkerToolEffect::Offline),
    // `prepare_table_query` plus TWO reads: the aggregate, then `top_values`.
    ("profile_column", WorkerToolEffect::ReadsWarehouse),
    // `prepare_table_query` plus one `SELECT ... LIMIT` read.
    ("sample_rows", WorkerToolEffect::ReadsWarehouse),
    // `commands::test_output` — the compiled model tests and the fixture
    // tests, in a DuckDB of its own, not the configured target.
    ("test", WorkerToolEffect::Offline),
];

/// The entries in `table` that execute or materialize a pipeline.
///
/// Split out so the test can show the check BITES — running it over a
/// deliberately bad table is the difference between a guard and another
/// assertion that only looks like one.
#[cfg(test)]
fn worker_tools_that_run_a_pipeline<'a>(table: &[(&'a str, WorkerToolEffect)]) -> Vec<&'a str> {
    table
        .iter()
        .filter(|(_, effect)| *effect == WorkerToolEffect::RunsPipeline)
        .map(|(name, _)| *name)
        .collect()
}

/// The entries in `table` that read the target warehouse.
#[cfg(test)]
fn worker_tools_that_read_the_warehouse<'a>(table: &[(&'a str, WorkerToolEffect)]) -> Vec<&'a str> {
    table
        .iter()
        .filter(|(_, effect)| *effect == WorkerToolEffect::ReadsWarehouse)
        .map(|(name, _)| *name)
        .collect()
}

// ---------------------------------------------------------------------------
// Worker-profile guidance surfaces — the enumeration, the rewrite table,
// and the matching rule (F3 red team, finding 3)
// ---------------------------------------------------------------------------

/// Every guidance surface an MCP worker session is served, counted —
/// because this defect class has now been found SIX times, and every fix
/// was believed complete when it shipped.
///
/// The history is the reason this is a list rather than a habit. Round 1
/// fixed the prompt `description`s. Round 2 found the prompt BODIES.
/// Round 2's own follow-up found the draft `next_steps`. The F3 red team
/// then found `tools/list` TOOL DESCRIPTIONS, three of them still
/// steering the worker at `propose`. Extending the sweep to cover those
/// found a fifth instance on a surface swept since round 2: the
/// `add_tests_to_pks` worker prompt body said "Proposing", which an
/// exact-name rule cannot see. And F3 round 2 found the sixth in the one
/// place this list did not look at all — the text a tool carries when it
/// SUCCEEDS. Four E027 budget constructors suggested "or optimize the
/// query", and `optimize` is a tool this profile does not serve.
///
/// ```text
///   #  surface                                    status
///   1  initialize   -> THE WHOLE RESULT            swept: worker_instructions_are_
///        (protocolVersion, capabilities,            projected_and_default_stays_verbatim
///         serverInfo, instructions, _meta —
///         the banner is spliced out, being
///         the one surface that names
///         excluded tools on purpose)
///   2  prompts/list -> THE WHOLE Prompt            swept: worker_profile_guidance_
///        (name, title, description,                 surfaces_name_no_excluded_tool
///         arguments, icons, _meta)
///   3  prompts/get  -> THE WHOLE RESULT             swept: worker_profile_prompts_end_
///        (resultType, description, messages,         at_the_runner_handoff
///         _meta)
///   4  tools/list   -> tool description            swept: worker_profile_guidance_
///   5  tools/list   -> input_schema text            surfaces_name_no_excluded_tool,
///        (4 and 5 are two FIELDS of one             as THE WHOLE Tool — which also
///         channel; the sweep takes the whole        covers title, annotations,
///         Tool, so both are covered at once)       icons and _meta
///   6  tools/list   -> output_schema text          NOT SERVED — pinned absent by
///                                                   worker_result_text_names_no_
///                                                   excluded_tool
///   7  tools/call   -> ok: result next_steps       swept: draft_next_steps_are_
///        (a FIELD of row 8's channel)               profile_selected; also GOLDEN
///                                                   (worker) under its own key
///   8  tools/call   -> ok: THE WHOLE CallToolResult PARTIAL — two DIFFERENT
///        (structured_content AND content AND        reasons, split below;
///         is_error AND result_type AND _meta)       all 12 worker tools GOLDEN
///   9  tools/call   -> err: THE WHOLE CallToolResult PARTIAL — argument arms
///        (the ToolError body — message,             swept AND GOLDEN (worker),
///         remediation_hint, policy_rule, the        the rest OPEN
///         plan fields — AND the envelope)
/// ```
///
/// THE ROWS ARE NOT ALL CHANNELS, and pretending they are is what the
/// heading below used to do. Rows 4 and 5 are two FIELDS of one `tools/list`
/// entry, and row 7 is one field of row 8's result. They keep their numbers,
/// because `WORKER_GUIDANCE_SURFACES` counts the places a worker is served
/// text and renumbering would break every reference to "surface 9" in this
/// crate for no safety gain. What changed is the SWEEP: each of those field
/// rows is now covered by a sweep over its whole channel, so the guarantee
/// holds even where the row label does not.
///
/// NINE SURFACES: SIX SWEPT, TWO PARTIAL, ONE NOT SERVED. Not "all swept" —
/// that sentence has now been believed five times about a set that was
/// incomplete, which is the whole reason for the count.
///
/// EVERY ROW'S SWEEP READS A WHOLE CHANNEL, NOT A FIELD, and the ninth
/// round is why. Rows 3
/// and 9 used to name one field each — `prompts/get`'s message bodies, and
/// `ToolError`'s `remediation_hint`. Both were wrong by omission.
/// `GetPromptResult` also carries a `description`, which no sweep read, and
/// which is where two worker prompts went on promising a write their bodies
/// withheld. `ToolError` also carries `message`, `policy_rule` and the
/// flattened plan-handoff fields, all of them served text.
///
/// The fix is structural rather than additive. A row now covers everything
/// its channel returns, and its sweep matches the WHOLE serialized payload,
/// so a field added later is covered without any test knowing the shape.
/// Adding a "surface 10" for `description` would have bought a surface 11
/// for the next field: enumerating fields is precisely what lost here.
///
/// WHAT THE PARENTHESISED FIELD LISTS ARE, and are not. They are a reading
/// aid for rmcp 3.1.2's shapes, not the coverage rule — the sweep is the
/// serialized value, and it reads whatever serde emits. The eleventh
/// round's finding 4 was that three of them had gone stale against the
/// crate: row 1 omitted `_meta`, row 2 omitted `name`, and row 3 omitted
/// both `resultType` and `_meta`. No coverage hole, because the sweeps
/// already read the whole value; the LISTS were wrong, which is the same
/// defect class as a claim that out-runs its check.
///
/// Most of those fields carry `skip_serializing_if = "Option::is_none"`, so
/// while they are `None` they are absent from the payload and the sweep
/// reads nothing there. That is the intended behaviour rather than a gap:
/// an absent field serves a worker no text, and the first populated value
/// is swept without anyone editing a test. `Prompt::name` and
/// `GetPromptResult::messages` are the two that are always present.
///
/// `resultType` IS STRIPPED PER PEER, not absent from this server. rmcp's
/// constructors set `Some(ResultType::COMPLETE)`, and the server handler
/// then calls `strip_result_type_for_legacy_peer()` for any peer whose
/// NEGOTIATED protocol version is older than `2026-07-28`. It applies to
/// row 8's `result_type` as well, for the same reason and by the same call.
/// Reading a field off the struct is not evidence it reaches a worker.
///
/// The first attempt at this correction wrote the opposite of the strip —
/// caught by pinning the value rather than by reading the type.
///
/// "FOR ANY CLIENT TODAY", NOT "BY CONSTRUCTION", and the fifteenth round
/// is why the qualifier is here. This used to argue that
/// [`RockyMcpServer::get_info`] "pins `ProtocolVersion::V_2024_11_05`, so
/// every result this server sends is stripped". `get_info` does not pin the
/// wire version. It supplies the server's FALLBACK, and rmcp's
/// `serve_server` then overwrites `init_response.protocol_version` with
/// `negotiate_protocol_version(client_requested, server_fallback,
/// supported)` — which returns the CLIENT's request whenever the server
/// supports it. `RockyMcpServer` does not override
/// `Service::supported_protocol_versions`, so it advertises rmcp's whole
/// `KNOWN_VERSIONS` list, `V_2026_07_28` included. A client that asks for
/// `2026-07-28` is given it, `sep_2322_supported` is then true, the strip
/// call is skipped, and `resultType` DOES reach that client.
///
/// The stripping therefore holds because no PRODUCTION client asks for
/// `2026-07-28` yet, not because this server refuses to speak it. The
/// negotiated version is `2025-11-25` against rmcp 3.1.2's own client —
/// which is now BLESSED, as part of row 1's `initialize` payload in
/// `served_text_golden_pins_every_worded_surface`, so the day it moves the
/// golden moves with it and this paragraph gets re-read. Closing the gap by
/// construction would mean narrowing `supported_protocol_versions`, which is
/// a behaviour change to what this server speaks and is not made here.
///
/// SIXTEENTH ROUND, finding 3 — THAT IS NOW GUARDED, NOT MERELY OBSERVED.
/// The two paragraphs above were correct and completely unexercised: every
/// roundtrip connected with rmcp's default `()` handler, so the branch they
/// describe — a peer that DOES negotiate `2026-07-28` — was reached by no
/// test. `result_type_reaches_a_2026_07_28_client_and_no_other` (in
/// `tests/roundtrip.rs`) now drives both peers and asserts the negotiated
/// version on each before reading `result_type`, so "stripped for the
/// default client, served to a modern one" is a checked claim.
///
/// It asserts BOTH directions on purpose, and each one covers the extreme
/// the other cannot see. Present-only survives the field being ON
/// EVERYWHERE — drop the strip call and every peer keeps `resultType`, and
/// that half still passes. Absent-only survives the field being OFF
/// EVERYWHERE — serde emitting nothing, or this server narrowing
/// `supported_protocol_versions` so no peer can negotiate `2026-07-28`, and
/// that half still passes. Only the pair distinguishes "negotiated per peer"
/// from either extreme.
///
/// The value is the fixed string `complete`, so a modern client learns
/// nothing from it — this is about the CLAIM, as everything on this list is.
///
/// The value of `resultType` is the fixed string `complete`, so nothing
/// about this is a guidance LEAK. The defect was the CLAIM — a justification
/// that named a mechanism the code does not have — which is the same class
/// every round of this branch has found in served text.
///
/// THAT SENTENCE WAS TRUE OF ROW 3 AND OF NO OTHER ROW, and the tenth round
/// is why it is worth writing down twice. Row 3 did serialise the whole
/// `GetPromptResult`. Rows 1, 2, 4, 5, 8 and 9 kept SELECTING fields under
/// the same heading: `initialize` read `instructions`, `prompts/list` read
/// `description`, `tools/list` read `description` + `input_schema`, and both
/// call sweeps read `structured_content` and dropped the `CallToolResult`
/// around it. Against frozen rmcp 3.1.2 the omitted fields are real —
/// `InitializeResult` also carries `protocolVersion` / `capabilities` /
/// `serverInfo` / `_meta` (and its `Implementation` carries `title` /
/// `description` / `icons` / `websiteUrl`), `Prompt` also carries `title` /
/// `arguments` / `icons` / `_meta`, `Tool` also carries `title` /
/// `output_schema` / `annotations` / `icons` / `_meta`, and
/// `CallToolResult` also carries `content` / `is_error` / `result_type` /
/// `_meta`. No leak was demonstrated in any of them; the FALSE GUARANTEE
/// was the finding, and a guarantee is exactly the kind of claim this list
/// exists to keep honest.
///
/// ROW 1 WAS FOUND LAST AND BY THE AUTHOR, on the work the other rows'
/// correction produced — writing "every row is covered by a sweep over the
/// whole payload of its channel" and then checking row 1 against it. That
/// ordering is the point, not a boast: the general defence on this list has
/// always been "read what the served text DOES", and the sibling defence is
/// to read every claim this file makes back against the code it describes,
/// including a claim written five minutes ago.
///
/// One of those omissions was not merely theoretical. `content` is filled by
/// `CallToolResult::structured` with `value.to_string()` — a second
/// rendering of the same guidance, and the one a client that ignores
/// structured output shows the worker. It is now swept, and asserted
/// non-empty so the sweep cannot degrade into reading an empty vector.
///
/// AND THE HONEST LIMIT OF THAT: because rmcp derives `content` from the
/// same `Value`, the bytes are identical today, so widening rows 8 and 9
/// found nothing and could not have. What it buys is the guarantee the
/// heading claims — a field added to the envelope, or a result constructed
/// with `content` that is NOT a copy of the structured half, is covered
/// without this test being edited. Rows 2 and 4/5 are different: their
/// omitted fields (`title`, `arguments[].description`, `annotations`) are
/// independently settable, and a mutation into `title` is caught by the
/// widened sweep and was invisible to the field-selecting one. Row 1 sits
/// with rows 8 and 9 on this axis rather than with 2 and 4/5: its newly
/// covered fields are all `None` under
/// `Implementation::from_build_env()`, so widening it found nothing either.
/// The mutation that proves the sweep works has to POPULATE one first.
///
/// So the honest form of the guarantee is about the SWEEPS, not the row
/// labels: every row is covered by a sweep over the whole serialized payload
/// of the channel that carries it. Rows 4, 5 and 7 name fields; their
/// channels (`Tool`, and row 8's `CallToolResult`) are what the sweeps read.
/// Row 1 removes exactly one thing before matching — the banner, which names
/// excluded tools deliberately — and nothing else.
///
/// (1) WAS EXEMPT AND IS NOW SWEPT, and how it fell is worth keeping. The
/// argument for exempting it was that the instructions are the canonical
/// authoring skill served VERBATIM under a disclaiming banner, so forking
/// them would let the guidance drift from the canonical file. That made the
/// row HONEST. It did not make it SAFE, and the F3 red team round 2 said so:
/// the banner stopped the worker at contract authorship, metadata
/// authorship and the record/review/apply chain, but NOT at CHECK
/// authorship — while the skill below it told the worker to strengthen
/// assertions, append tests through the draft tools, and encode invariants
/// as checks. The largest guidance surface instructed exactly the thing
/// removing `draft_check` exists to stop.
///
/// The fix keeps what the exemption was protecting. The canonical skill is
/// untouched and still correct for the default profile; what forks is what
/// the WORKER is served, through [`WORKER_INSTRUCTIONS_REWRITES`] — the
/// same checked-rewrite mechanism as rows 2 and 4, where a needle that
/// stops matching panics at construction instead of silently serving the
/// default sentence.
///
/// The banner stays, and it is the ONE worker surface that names excluded
/// tools on purpose — saying "`propose` is not available" is the opposite
/// of steering at it. It is derived from the routers now, so it can no
/// longer name six of nineteen.
///
/// (6) IS NOT SERVED, and is listed precisely because it nearly was. rmcp
/// emits no `output_schema` for any tool here, so the result-type doc
/// comments schemars would put there never reach a worker — and those doc
/// comments name excluded tools freely (`DraftModelResult::next_steps`
/// spells out the whole `propose` chain). Opting in would turn all of
/// `result_types.rs` into worker-served text in one commit. Pinned absent
/// so that commit fails a test.
///
/// (8) IS PARTIAL, and the word is chosen carefully. It covers the free
/// text a SUCCESSFUL result carries besides `next_steps`: diagnostic
/// `message`/`suggestion` (from `compile` AND `draft_model` — two routes
/// to the same text), breaking-change finding messages, `skipped_reason`,
/// test-failure text, unavailability `reason`s. Its sweep drives all 12
/// worker-served tools and serialises each WHOLE result, with the compile
/// forced RED so the diagnostic path is really exercised.
///
/// What it CANNOT claim is completeness — for two reasons that are NOT the
/// same kind of thing, and the ninth review round asked for them split
/// because the second was laundering the first:
///
///  - UNFINISHED AUDIT COVERAGE (fixable, nobody has done it). Rocky-authored
///    STATIC templates are written per call site across rocky-compiler,
///    rocky-core and rocky-cli, for consumers that are mostly not this
///    worker. There is no table to audit, so reaching all of them means
///    driving every constructor — which this harness does not. That is work
///    not yet done, the same shape as (9). It is not a property of the
///    problem, and it must not inherit the next bullet's excuse.
///  - A REAL LEXICAL BOUNDARY (unfixable by any rule Rocky ships). A
///    diagnostic interpolates the user's own model and column names. If a
///    project contains an identifier that IS an excluded tool name —
///    a model literally called `propose` — the diagnostic quoting it names
///    an excluded tool, and no rule Rocky ships can reword someone's model.
///
///    The collision is narrower than it was, and the example this comment
///    used to give was WRONG: `propose_v2` does NOT collide, because `_` is
///    an identifier byte, so `contains_identifier` rejects it at the
///    boundary exactly as it rejects `proposal_id` and `propose_only`. Only
///    an EXACT identifier collides. A wrong example makes a true boundary
///    look invented, which is why it is corrected rather than dropped.
///
/// The distinction matters operationally: the first bullet closes by doing
/// the audit, the second never closes. Reporting them as one PARTIAL let
/// the unfinished half borrow the finished half's excuse.
///
/// (9) IS PARTIAL, and was previously inventoried as one field of an
/// envelope that has four. A `ToolError` carries `message` AND
/// `remediation_hint`, plus `policy_rule` and the flattened plan-handoff
/// fields; all of them are guidance, and all of them reach the worker.
///
/// What is swept is the ARGUMENT-VALIDATION arm of the NINE worker-served
/// tools that have one, each as a WHOLE serialized envelope, with the
/// failure asserted first so a call that silently succeeded cannot pass as
/// coverage.
///
/// NINE, not eight, and the correction matters more than the number. The
/// list of tools EXCUSED from this row said `inspect_schema`, `catalog`,
/// `test` and `breaking_change` "ignore their arguments (or take none)".
/// `test` does not: it takes an optional `model` and rejects an unknown one
/// as `model_not_found`, through `commands::test_output`'s
/// `reject_unknown_model`. A written-out excuse read as coverage for a
/// reachable arm nobody drove — which is the failure mode the excuse itself
/// was added to prevent. The other three were re-verified rather than
/// inherited: `inspect_schema` and `catalog` bind `_params` and never read
/// them, and `breaking_change`'s bad-`base` path returns a SUCCESSFUL result
/// with `skipped_reason` set, not an error.
///
/// What is NOT swept is every other arm — policy denials,
/// warehouse failures, internal errors — because reaching them means
/// driving every error path of every served tool, which no harness here
/// does, and the hints are written per call site so there is no table to
/// audit instead. That residue is UNFINISHED AUDIT COVERAGE, the same
/// category as (8)'s first bullet: it closes by doing the work.
///
/// THE CHANNELS ARE CLOSED AT THE PROTOCOL LEVEL; THE FIELDS ARE NOT.
/// [`RockyMcpServer::get_info`] enables `tools` and `prompts` and nothing
/// else, so there is no `resources/read`, no completion and no logging
/// channel able to carry a tenth KIND of text, and
/// `the_server_opens_no_guidance_channel_beyond_tools_and_prompts` pins
/// that — enabling one fails a test and forces a revisit of this comment
/// instead of silently opening surface 10.
///
/// That bound is real and it is narrower than it once read here. A
/// capability gate closes the set of CHANNELS. It cannot close the set of
/// FIELDS inside a channel: `GetPromptResult` grew a `description` this
/// enumeration never counted, and no capability flag would have announced
/// it. Nothing at the protocol level stops a struct gaining a field.
///
/// Which is exactly why every row above matches the WHOLE serialized
/// payload of its channel rather than a field it went looking for. The
/// capability bound and the whole-payload sweeps are two halves of one
/// argument: the first closes the channels, the second is what covers the
/// fields the first cannot see.
///
/// SCOPE: this counts the MCP SESSION. A worker MAY also receive the
/// driver's TASK BRIEF, which is out-of-band — written to the task outbox,
/// not served over this protocol — and has its own gate in
/// `rocky_fulfill::briefs`: an override naming an excluded tool is
/// REFUSED, not swept.
///
/// "MAY" is load-bearing, and it is why row 1 could not stay exempt. The
/// worker profile is directly invocable as `rocky mcp --profile worker`,
/// and no brief is guaranteed to accompany it. So the served instructions
/// are the ONLY guidance surface a worker session is certain to read, and
/// "the brief also says not to" was never a defense available to this list.
///
/// The two rules now agree on identifier boundaries and differ on ONE axis:
/// the brief gate matches EXACT names, this one derives inflections. The
/// jobs differ, and so does the cost of a false positive — here it costs a
/// reword of text Rocky owns, there it rejects a legitimate operator
/// config. The shipped default brief texts carry no inflection of an
/// excluded name.
///
/// Surface 7 has one worker-served producer: `draft_model`. `draft_check`
/// also carries `next_steps`, but it left [`WORKER_PROFILE_TOOLS`], so a
/// worker session cannot reach it — its worker text is still kept correct
/// (see [`RockyMcpServer::draft_check_next_steps`]) because it is what
/// would be wrong first if the tool were re-admitted.
///
/// A GOLDEN NOW SITS UNDER ROWS 1–5, 7, 8 AND 9, and it is a new CHECK over
/// existing rows — NOT a tenth row. The count below stays 9.
///
/// `served_text_golden_pins_every_worded_surface` (in `tests/roundtrip.rs`)
/// digests the whole serialized payload of rows 1, 2, 3, 4 and 5, for the
/// DEFAULT and WORKER profiles, plus rows 7, 8 and 9 for the WORKER
/// profile, into `tests/fixtures/served_text.golden`. Any edit to any of
/// that text fails the test until someone re-blesses the file.
///
/// ROWS 7–9 WERE EXCLUDED FROM IT, and the fifteenth round is why they are
/// not. The exclusion read: "their payloads embed run-dependent values, so a
/// digest over them would drift every run and get blessed reflexively." That
/// is true of `tools/call` in general. It is not true of the WORKER set, and
/// checking that was the step the exclusion skipped — the plan- and
/// timestamp-producing tools are `propose`, `optimize` and the rest of the
/// withheld list, and this profile serves none of them.
///
/// Grounded rather than argued: all 21 worker call payloads (12 successes,
/// 9 argument failures) were dumped and read. Not one carries an absolute
/// path, a timestamp, a duration or an id. `draft_model` reports a bare
/// model NAME, `test` reports counts with no timings, and
/// `breaking_change`'s `skipped_reason` names no path. The temp-root
/// normalizer in the harness is defence for a field not yet added, not
/// something that fires today.
///
/// WORKER ONLY for those three, and the asymmetry is deliberate: the
/// DEFAULT profile serves the plan-producing tools the exclusion was really
/// about, so pinning its call results would import the drift that is
/// genuinely absent here. Row 9 also stays PARTIAL in the golden for the
/// same reason it is partial in the sweep — only the argument-validation
/// arm is reachable offline.
///
/// "WHOLE SERIALIZED PAYLOAD" WAS NOT TRUE OF ROW 1 WHEN IT WAS FIRST
/// WRITTEN, and the fifteenth round is why it is called out rather than
/// quietly corrected. The golden hashed `instructions` alone — one field of
/// an `InitializeResult` that also carries `protocolVersion`,
/// `capabilities`, `serverInfo` and `_meta` — while its heading claimed the
/// channel. That is the SAME field-selection defect the eleventh round
/// found in rows 1, 2, 4, 5, 8 and 9 of the sweeps above, reproduced inside
/// the guard built to catch it. It now hashes the serialized
/// `InitializeResult`.
///
/// The reason it exists is the limit of every other rule on this list. All
/// of them are LEXICAL — they look for a word. An arbitrary paraphrase
/// defeats a negative-substring pin, and no lexical rule can catch a
/// reworded semantic overclaim without pretending to understand meaning.
/// The golden does not read the text at all, so a paraphrase cannot dodge
/// it.
///
/// AND ITS OWN LIMIT, stated here so it is not read as more: it catches
/// UNREVIEWED wording changes, not FALSE ones. It cannot tell a true
/// sentence from a false one, and a wrong claim blessed once stays
/// blessed. It converts "is every served sentence true?" — unbounded —
/// into "is this one changed sentence true?" — bounded, and still a
/// person's job. What remains outside it is stated above: the default
/// profile's call results, and every error arm of row 9 that an offline
/// harness cannot reach.
///
/// Test-gated because nothing in the server reads the number — the value
/// is the enumeration above it and the anchor it gives the capability
/// test. It is a constant rather than a comment so that grepping
/// `WORKER_GUIDANCE_SURFACES` reaches the list from either end.
#[cfg(test)]
const WORKER_GUIDANCE_SURFACES: usize = 9;

/// The worker-profile rewrites of `#[tool(description = ...)]` text, as
/// `(tool, sentence to replace, replacement)`.
///
/// A REWRITE, not a second copy of the description. The prompt side
/// duplicates whole strings ([`WORKER_PROMPT_DESCRIPTIONS`]), which works
/// there because those strings are one sentence each; `draft_model`'s
/// description is a paragraph, and a duplicated paragraph is a paragraph
/// that will drift. Only the steering sentence is stated here.
///
/// Both drift directions REFUSE at CONSTRUCTION, not at review:
///
///  - a tool that leaves [`WORKER_PROFILE_TOOLS`] (or is renamed) orphans
///    its entry, and the lookup refuses;
///  - an edit to the steering sentence makes the needle miss, and the
///    replacement refuses rather than silently serving the default text to
///    a worker — which is exactly how a checked `replace` differs from a
///    plain one.
///
/// "BOTH DIRECTIONS" WAS TRUE OF ZERO MATCHES ONLY, until the tenth round.
/// This path tested `contains` and then replaced EVERY occurrence, while
/// [`WORKER_INSTRUCTIONS_REWRITES`] one surface over required exactly one
/// match. So a duplicated needle here silently rewrote a second passage
/// nobody reviewed — the failure the instruction path refuses — and the
/// sentence above described a guarantee only one of the two surfaces had.
/// [`worker_tool_description`] now enforces the same exactly-once rule, and
/// it is a free function so a test can drive the duplicate case.
///
/// Construction, not compile time — the same correction as
/// [`WORKER_INSTRUCTIONS_REWRITES`]. Drift here compiles and then aborts
/// `rocky mcp --profile worker` at startup.
///
/// The direction neither guard covers is a NEW worker-served tool whose
/// description names an excluded verb. That is what the sweep is for.
const WORKER_TOOL_DESCRIPTIONS: &[(&str, &str, &str)] = &[
    (
        "breaking_change",
        "Self-check blast radius BEFORE propose.",
        "Self-check blast radius BEFORE you hand off to the trusted runner.",
    ),
    (
        "plan_preview",
        "before proposing a materialization.",
        "before you hand off to the trusted runner.",
    ),
    (
        "draft_model",
        "a draft is inert until you `propose` it and a human reviews it.",
        "a draft is inert until the trusted runner records a plan for it and a human \
         reviews it.",
    ),
];

/// The forms of an excluded tool name that no worker-served guidance
/// string may contain: the exact name plus mechanically derived English
/// inflections (`propose` → `proposing`, `proposed`, `proposes`,
/// `proposal`).
///
/// DERIVED, not listed. A hand-written variant table would be the same
/// defect as a hand-written excluded-tool list, one level down — and that
/// list is precisely how `draft_check` slipped past a green sweep.
///
/// The inflections are what made this rule find anything. `plan_preview`
/// said "before proposing a materialization" and the `add_tests_to_pks`
/// worker prompt said "Proposing a wrong key invariant"; both steer at a
/// verb the profile does not serve, and an exact-name sweep read both as
/// clean.
///
/// IT IS A WORD RULE, NOT A SEMANTIC ONE, and it can fire on ordinary
/// English — that "Proposing" meant "suggesting". The remedy when it does
/// is to REWORD, never to relax the rule, and the reason is the reader
/// rather than the matcher: a worker that has just been told `propose` is
/// not available cannot tell the two senses apart either.
///
/// A BLANK tool name yields NO forms. It cannot arrive over MCP — both
/// routers are built from `#[tool]` attributes — but this is a `pub fn` on
/// a library crate, so a caller can pass one. Deriving from `""` would
/// otherwise hand the sweep `"ing"`, `"ed"`, `"es"`, `"s"` and `"al"` as
/// live matchers, and `"s"` is an identifier in plenty of ordinary English.
/// A name that names nothing must match nothing.
pub fn excluded_mention_forms(tool: &str) -> Vec<String> {
    if tool.trim().is_empty() {
        return Vec::new();
    }
    let stem = tool.strip_suffix('e').unwrap_or(tool);
    let mut forms = vec![tool.to_string()];
    forms.extend(
        ["ing", "ed", "es", "s", "al"]
            .into_iter()
            .map(|suffix| format!("{stem}{suffix}"))
            .filter(|form| form != tool),
    );
    forms
}

/// The `instructions` a worker session is served: the derived banner, then
/// the skill body with [`WORKER_INSTRUCTIONS_REWRITES`] applied.
///
/// REFUSES if any needle does not match exactly once. Both halves matter:
///
///  - ZERO matches means the skill was edited under the projection. A
///    silent no-op replace would serve the DEFAULT sentence to a worker,
///    which is the hole this whole table closes.
///  - MORE THAN ONE match means `replace` would rewrite a second passage
///    nobody reviewed. A projection that edits text its author did not read
///    is not a projection.
///
/// WHEN THE CHECK RUNS, corrected. Every operand is a compile-time constant
/// — [`INSTRUCTIONS`] is an `include_str!` of the skill file — but the
/// match itself is never verified at compile time. It runs at server
/// CONSTRUCTION, on the live `rocky mcp --profile worker` path. An edit to
/// the skill compiles cleanly and then refuses at startup. The guarantee
/// that the frozen constants still line up is a TEST, not a build
/// invariant, and calling it one overstated it.
///
/// The refusal aborts startup; it never serves a partial projection. See
/// [`RockyMcpServer::try_new_with_profile`] for why that is the fail-closed
/// choice and not the softened one.
///
/// `rewrites` is a parameter rather than a direct read of
/// [`WORKER_INSTRUCTIONS_REWRITES`] so a test can hand it a table that HAS
/// drifted. Otherwise the refusal is unreachable in-process — the real
/// table matches, which is the point — and "it refuses on drift" would be
/// an untested claim about a path this round exists to correct claims on.
fn worker_instructions(excluded: &[String], rewrites: &[(&str, &str)]) -> Result<String, String> {
    let mut body = INSTRUCTIONS.to_string();
    for (needle, replacement) in rewrites {
        let hits = body.matches(needle).count();
        if hits != 1 {
            return Err(format!(
                "WORKER_INSTRUCTIONS_REWRITES needle matched {hits} times, not once — the \
                 rocky-ai-workflow skill changed under the worker projection. Re-project it \
                 deliberately; this refuses at construction so a worker is never served the \
                 default sentence. Needle: {needle:?}"
            ));
        }
        body = body.replace(needle, replacement);
    }
    Ok(format!("{}{body}", worker_instructions_banner(excluded)))
}

/// One [`WORKER_TOOL_DESCRIPTIONS`] rewrite applied to a tool's served
/// description.
///
/// REFUSES unless the needle matches EXACTLY ONCE — the same rule
/// [`worker_instructions`] enforces, and the tenth round's finding 3 is that
/// the two did not agree. This path required only `contains` and then
/// replaced EVERY occurrence, so a needle that appeared twice silently
/// rewrote a second passage nobody reviewed. Instruction rewrites refused
/// that and tool-description rewrites did not, which made "the projections
/// fail closed" true of one of the two surfaces it was claimed for.
///
/// Both directions, and both matter for the same reason as one surface over:
///
///  - ZERO matches means the description was edited under the projection. A
///    silent no-op replace serves the DEFAULT text — which names `propose` —
///    to a worker.
///  - MORE THAN ONE means `replace` edits a passage the table's author did
///    not read. A projection that rewrites text nobody reviewed is not a
///    projection.
///
/// A FREE FUNCTION rather than an inline loop body, for the reason
/// [`worker_instructions`] takes its table as a parameter: the real
/// descriptions match once, so the refusal is unreachable in-process and
/// "it fails closed" would be an untested claim about the very path this
/// round exists to correct claims on. Taking `current` as an argument lets a
/// test hand it a description that HAS drifted.
fn worker_tool_description(
    tool: &str,
    current: &str,
    needle: &str,
    replacement: &str,
) -> Result<String, String> {
    let hits = current.matches(needle).count();
    if hits != 1 {
        return Err(format!(
            "WORKER_TOOL_DESCRIPTIONS rewrite for '{tool}' matched {hits} times, not once — \
             zero means the default description was edited under the projection and a \
             no-op replace would serve the DEFAULT text to a worker; more than one means \
             the replace would rewrite a passage nobody reviewed. Re-project it \
             deliberately. Needle: {needle:?}"
        ));
    }
    Ok(current.replace(needle, replacement))
}

/// Whether `needle` occurs in `haystack` at IDENTIFIER BOUNDARIES — neither
/// neighbouring byte is `[a-z0-9_]`. Both arguments must already be
/// lowercase.
///
/// This is the F3 round-2 fix. The rule was a raw `contains`, which is not
/// an identifier detector: it read `proposal` inside a user's column named
/// `proposal_id` and `propose` inside the config literal `propose_only`.
/// That matters more the wider the swept surface gets — a compiler
/// diagnostic quotes the user's own model and column names back at the
/// worker, so a raw-substring rule turns every unlucky identifier in the
/// user's project into a guidance violation Rocky cannot reword.
///
/// What it does NOT buy: a legitimate English word that IS the tool name
/// still matches, because it is byte-identical at both boundaries. The
/// E027 budget diagnostic said "or optimize the query" and `optimize` is
/// an excluded tool; boundaries left that hit exactly where it was, and it
/// was closed by rewording E027 (`rocky_compiler::diagnostic`), which is
/// the remedy the rule's own doc prescribes — not a narrower matcher.
///
/// Deliberately a SECOND implementation of the same primitive
/// `rocky_fulfill::briefs` uses, and not a shared one — see the SCOPE
/// paragraph on [`WORKER_GUIDANCE_SURFACES`] for why the two rules stay
/// separate. (The dependency runs rocky-fulfill → rocky-mcp, so sharing
/// would mean this crate importing that one, backwards.) The two now agree
/// on boundaries and still differ on inflections: refusing a valid
/// operator config costs more than rewording a sentence Rocky owns.
///
/// An EMPTY needle returns `false`, and the guard is a panic fix rather
/// than a taste call. `"".find("")` succeeds at every byte offset, so the
/// scan advanced `from` past the end of `haystack` and the next
/// `haystack[from..]` panicked with an out-of-range index — but only when
/// the last byte was an identifier byte, which is why it read as a
/// harmless edge case. `contains_identifier("abc ", "")` returned `true`;
/// `contains_identifier("abc", "")` aborted the process.
///
/// It is not reachable over MCP (every excluded name comes from a router),
/// but [`names_excluded_tool`] is public API and a caller can supply the
/// empty string. An empty needle is not an identifier, so it matches
/// nothing.
fn contains_identifier(haystack: &str, needle: &str) -> bool {
    if needle.is_empty() {
        return false;
    }
    let bytes = haystack.as_bytes();
    let is_ident = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
    let mut from = 0;
    while let Some(offset) = haystack[from..].find(needle) {
        let start = from + offset;
        let end = start + needle.len();
        let before_ok = start == 0 || !is_ident(bytes[start - 1]);
        let after_ok = end == bytes.len() || !is_ident(bytes[end]);
        if before_ok && after_ok {
            return true;
        }
        from = start + 1;
    }
    false
}

/// The excluded tool a guidance string names, and the form it used —
/// `None` when the string names none of them.
///
/// Case-insensitive: a sentence-initial "Proposing" is the same steer as
/// a mid-sentence one, and only the second would survive an exact match.
/// Matched at identifier boundaries ([`contains_identifier`]), so the rule
/// detects an identifier rather than a byte run.
pub fn names_excluded_tool(haystack: &str, excluded: &[String]) -> Option<(String, String)> {
    let lower = haystack.to_lowercase();
    excluded.iter().find_map(|tool| {
        excluded_mention_forms(tool)
            .into_iter()
            .find(|form| contains_identifier(&lower, &form.to_lowercase()))
            .map(|form| (tool.clone(), form))
    })
}

// ---------------------------------------------------------------------------
// Tool input parameter structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CompileArgs {
    /// Optional single-model filter; compile-checks the whole project for type
    /// context but scopes the returned result to this model when set.
    #[serde(default)]
    pub model: Option<String>,
    /// Optional portability target dialect — one of `"databricks"`,
    /// `"snowflake"`, `"bigquery"`, or `"duckdb"`. When set, the P001
    /// dialect-divergence lint runs against it on demand: each model's SQL is
    /// checked for constructs that won't port to the named dialect, surfaced as
    /// P001 diagnostics. When absent, behaviour is unchanged — the lint runs
    /// only if `rocky.toml` declares `[portability] target_dialect`.
    #[serde(default)]
    pub target_dialect: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PlanPreviewArgs {
    /// Optional single-model filter. When unset, previews every model.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct LineageArgs {
    /// The focal model.
    pub model: String,
    /// When set, scope lineage to this column (column-level trace).
    #[serde(default)]
    pub column: Option<String>,
    /// When `true`, trace downstream consumers instead of upstream sources.
    #[serde(default)]
    pub downstream: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ListArgs {
    /// What to list: `"models"`, `"pipelines"`, `"adapters"`, or `"sources"`.
    pub kind: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct InspectSchemaArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SampleRowsArgs {
    /// What to sample: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table. A dotted
    /// reference resolves directly against the warehouse and needs no compiled
    /// model, so it also works at cold start (a project with zero models yet).
    pub model: String,
    /// Random-sample percentage (1–100). Omit to return the first rows
    /// deterministically — the right default for small tables, where a low
    /// percentage sample can return zero rows.
    #[serde(default)]
    pub percent: Option<u32>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProfileColumnArgs {
    /// What to profile: a compiled model name, OR a qualified `schema.table`
    /// (or `catalog.schema.table`) reference to a raw source table.
    pub model: String,
    /// The column to profile.
    pub column: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct TestArgs {
    /// Optional single-model scope: run only this model's declarative tests.
    /// When unset, runs the whole project's tests (unchanged behavior).
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ProposeArgs {
    /// Single model to materialize. When unset, the plan covers every model.
    #[serde(default)]
    pub model: Option<String>,
    /// Product identity this plan fulfils (e.g. `"product:revenue_daily"`).
    /// Opaque to the engine — carried in the hashed plan payload and echoed
    /// back; never parsed. Must be set together with `spec_digest` or not at
    /// all. A plan carrying it refuses a bare `rocky apply` — the applier
    /// must pass `--expect-spec-digest`.
    #[serde(default)]
    pub product_id: Option<String>,
    /// Digest of the approved product spec this plan was authored against
    /// (e.g. `"sha256:<hex>"`). Opaque to the engine. Must be set together
    /// with `product_id` or not at all.
    #[serde(default)]
    pub spec_digest: Option<String>,
    /// Caller-supplied idempotency key threaded into the plan payload so a
    /// re-apply of the same key dedups. When absent and the product fields
    /// are present, the engine derives `"<product_id>@<spec_digest>"` — note
    /// that derived key aliases every attempt for the same spec revision, so
    /// a runner that re-proposes should supply its own per-attempt key.
    #[serde(default)]
    pub idempotency_key: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftModelArgs {
    /// The model name. Becomes `models/<name>.sql` + a `models/<name>.toml`
    /// sidecar. Must be a bare identifier — no path separators, no `..`, no
    /// extension, not absolute. A name that would escape the models directory
    /// is refused with an `invalid_argument` error.
    pub name: String,
    /// The model's SQL body, written verbatim to `models/<name>.sql`. Raw SQL is
    /// first-class in Rocky — write real SQL grounded in the sampled data.
    pub sql: String,
    /// A plain-language statement of what the model is for, persisted to the
    /// sidecar's `intent` field (surfaced by `catalog` and lineage). Ground it
    /// in the intent you were given; it is the reviewer's context for the draft.
    pub intent: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct BreakingChangeArgs {
    /// Git ref to compare the working tree against. Defaults to `"HEAD"`.
    #[serde(default = "default_base_ref")]
    pub base: String,
}

fn default_base_ref() -> String {
    "HEAD".to_string()
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DependentsArgs {
    /// The focal model whose downstream consumers to resolve.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct CatalogArgs {}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct HistoryArgs {
    /// When set, return that model's execution history instead of the
    /// project-level run summary.
    #[serde(default)]
    pub model: Option<String>,
    /// When set (project-level form only), return only runs whose trigger
    /// matches — e.g. `"Schedule"` for scheduler-submitted runs. The filter
    /// applies BEFORE the recency cap, so a busy project's manual runs cannot
    /// crowd scheduler runs out of the window.
    #[serde(default)]
    pub trigger: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct PauseScheduleArgs {
    /// The pipeline whose schedule to pause. Must carry a `[schedule]` block.
    pub pipeline: String,
    /// Explicit confirmation. Pausing suppresses every demand source for the
    /// pipeline until a human resumes it; the tool refuses without
    /// `confirm: true`.
    #[serde(default)]
    pub confirm: bool,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct MetricsArgs {
    /// The model whose quality metrics to read.
    pub model: String,
    /// When set, also return a per-run trend for this single column.
    #[serde(default)]
    pub column: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct OptimizeArgs {
    /// Substring filter on model name. When unset, analyses every model with
    /// run history.
    #[serde(default)]
    pub model: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct SuggestFreshnessBlockArgs {
    /// The model the `[freshness]` block is for (used in the prompt context).
    pub model: String,
    /// Candidate temporal columns (timestamp/date) the block's `time_column`
    /// may be chosen from — typically the model's date/timestamp columns.
    pub temporal_columns: Vec<String>,
    /// The model's current sidecar `.toml` text, so the draft does not
    /// duplicate or conflict with an existing block. Optional.
    #[serde(default)]
    pub current_sidecar: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AiContractArgs {
    /// The model to draft a `.contract.toml` for. Its target table must be
    /// materialized in the warehouse (run the model first) — the contract is
    /// grounded in the table's observed per-column profile.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AiTestArgs {
    /// The model to draft test assertions for, from its intent + schema + SQL.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftContractArgs {
    /// The model to write a contract for. Its `.sql` (or `.rocky`) source must
    /// already exist under `models/` — the contract is written to the sibling
    /// `models/<model>.contract.toml` that compile auto-discovers.
    pub model: String,
    /// The contract's `.contract.toml` body you authored, written verbatim.
    /// Compile validates it against the model's inferred schema in the same call
    /// (a column the model doesn't produce comes back as a `W010` diagnostic).
    /// When omitted, the call is treated as a mis-dispatch to the generator and
    /// returns an actionable error pointing at the `ai_contract` tool.
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftCheckArgs {
    /// The model to write a check for. Its `.sql` (or `.rocky`) source must
    /// already exist under `models/`; the check is merged into the model's
    /// sidecar (`models/<model>.toml`).
    pub model: String,
    /// One or more declarative `[[tests]]` blocks you authored, appended to the
    /// model's sidecar verbatim. Each block is a Rocky data-quality check
    /// (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`,
    /// range, …). Compile proves the merged sidecar is structurally sound; the
    /// check executes via the `test` tool. When omitted, the call is treated as
    /// a mis-dispatch to the generator and returns an actionable error pointing
    /// at the `ai_test` tool.
    #[serde(default)]
    pub spec: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DraftMetadataArgs {
    /// The model whose sidecar metadata to patch. Its `.sql` (or `.rocky`)
    /// source must already exist under `models/`; the patch is merged into
    /// the model's sidecar (`models/<model>.toml`).
    pub model: String,
    /// Freshness expectation to set. Replaces the sidecar's whole
    /// `[freshness]` table when present.
    #[serde(default)]
    pub freshness: Option<FreshnessPatch>,
    /// Per-column classification tags to merge into the sidecar's
    /// `[classification]` table. Keys are column names, values are tags
    /// (e.g. `email = "pii"`). Listed columns are set/replaced; other
    /// columns' existing tags are preserved.
    #[serde(default)]
    pub classifications: Option<std::collections::BTreeMap<String, String>>,
}

/// The `[freshness]` block `draft_metadata` writes.
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct FreshnessPatch {
    /// Maximum lag in seconds before the model counts as stale. Written to
    /// the sidecar as `expected_lag_seconds`.
    pub expected_lag_seconds: u64,
    /// Timestamp column used to evaluate freshness at runtime. When unset
    /// the runtime falls back to the last-materialization timestamp.
    #[serde(default)]
    pub time_column: Option<String>,
    /// Severity when the freshness check trips: `"warning"` (the engine
    /// default) or `"error"`.
    #[serde(default)]
    pub severity: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ExplainModelArgs {
    /// The model to draft an intent description for, from its SQL, output
    /// schema, and upstream dependencies.
    pub model: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct GovernancePreviewArgs {
    /// Optional environment name (mirrors `rocky plan --env <name>`). When
    /// set, masking policies resolve `[mask.<env>]` overrides on top of the
    /// workspace `[mask]` defaults. Classification + retention previews are
    /// env-invariant.
    #[serde(default)]
    pub env: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct DriftPreviewArgs {
    /// The source table to compare — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference. Both tables are `DESCRIBE`d and
    /// their warehouse-reported types compared.
    pub source_table: String,
    /// The target table to compare against — a qualified `schema.table` (or
    /// `catalog.schema.table`) reference.
    pub target_table: String,
}

// ---------------------------------------------------------------------------
// Governor tool parameter structs.
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct EstateBriefArgs {
    /// Time window for the digest: `"last"`, `"24h"`, or `"7d"`. Defaults to
    /// `"7d"`. `"last"` reads the digest cursor **read-only** and never advances
    /// it, so a conversational query does not consume the Slack/email hook's
    /// `--since last` cursor.
    #[serde(default)]
    pub since: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct AuditQueryArgs {
    /// The subject to trace the custody chain for: a model name, a run id, or a
    /// 64-character plan id. The chain resolves principal → decision → plan →
    /// diff → run → downstream blast radius, with each link failing closed to
    /// `unavailable` rather than fabricating a value.
    pub subject: String,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ScorecardArgs {
    /// Grouping dimension: `"principal"`, `"rule"`, or `"scope"`. Defaults to
    /// `"principal"`.
    #[serde(default)]
    pub by: Option<String>,
    /// Window: `"all"` or a `"<N>d"` / `"<N>h"` duration (e.g. `"30d"`).
    /// Defaults to all-time.
    #[serde(default)]
    pub window: Option<String>,
}

#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
pub struct ReviewQueueArgs {
    /// When set, APPROVE this pending plan_id instead of listing the queue.
    /// Served only when the operator started the server as `rocky mcp
    /// --profile approver`; on any other profile this field is refused with
    /// `approve_not_enabled` and nothing is written. The plan must also be one
    /// currently awaiting review — call with this unset first to see the
    /// pending plan_ids.
    #[serde(default)]
    pub approve_plan_id: Option<String>,
    /// Explicit confirmation for the approve action. Approving writes a human
    /// sign-off marker that unblocks `rocky apply`, so it is refused unless this
    /// is `true`. Set it ONLY when the human has explicitly authorized approving
    /// this exact plan — it stands in for that human intent. It cannot unlock
    /// the approve action itself: a server without `--profile approver` refuses
    /// regardless of this flag.
    #[serde(default)]
    pub confirm: bool,
    /// List mode only: keep only pending plans whose payload carries this
    /// `product_id` (each candidate plan is read integrity-checked). A pending
    /// plan whose file cannot be read or fails its integrity check surfaces as
    /// a `warning` entry in `pending` — never silently dropped. `total`
    /// reflects the filtered list. Mutually exclusive with `approve_plan_id`.
    #[serde(default)]
    pub product_id: Option<String>,
}

// ---------------------------------------------------------------------------
// Prompt argument structs (schemars 1.x — rmcp's `Parameters<T>` bound).
// MCP prompt arguments are string-typed on the wire; `Serialize` is part of
// the prompt-macro contract (mirrors rmcp's own examples).
// ---------------------------------------------------------------------------

#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct BuildModelArgs {
    /// What the user wants to build — the model's purpose in their own words
    /// (e.g. "daily completed-orders revenue by region"). The prompt threads
    /// this intent through Rocky's authoring loop.
    pub intent: String,
}

/// No-argument prompt args for the project-wide trajectories
/// (`find_untested_models`, `summarize_project`). MCP prompts must declare a
/// `Parameters<T>` type even when they take no input.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct NoArgs {}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ScopedModelArgs {
    /// Optional single-model scope. When set, the trajectory focuses on this
    /// model; when omitted, it sweeps the whole project.
    #[serde(default)]
    pub model: Option<String>,
}

// ---------------------------------------------------------------------------
// Caps for the data-grounding tools.
// ---------------------------------------------------------------------------

const SAMPLE_MAX_ROWS: usize = 50;
const SAMPLE_MAX_BYTES: usize = 16 * 1024;
const CELL_MAX_CHARS: usize = 256;
/// Max distinct values `profile_column` lists in `top_values`; above this the
/// column is treated as high-cardinality and the value list is omitted.
const PROFILE_TOP_VALUES_MAX: usize = 25;

#[tool_router(router = tool_router)]
impl RockyMcpServer {
    /// Build a server rooted at `config_path`'s directory; the models
    /// directory is `<config-dir>/models` (the CLI's top-level convention).
    /// Serves the [`McpProfile::Default`] surface: every tool, with the
    /// `review_queue` approve action refused (#1517).
    pub fn new(config_path: PathBuf) -> Self {
        Self::new_with_profile(config_path, McpProfile::Default)
    }

    /// The tool names this server actually serves, sorted — the
    /// authoritative registry view for cross-crate parity tests
    /// (rocky-fulfill pins its excluded-tool brief golden against
    /// default-profile-minus-worker-profile). Reads the same router the
    /// constructor filtered, so it can never disagree with what
    /// `tools/list` serves.
    pub fn tool_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .tool_router
            .list_all()
            .into_iter()
            .map(|tool| tool.name.to_string())
            .collect();
        names.sort();
        names
    }

    /// Build a server serving `profile`'s tool surface.
    ///
    /// The worker profile filters the full router down to
    /// [`WORKER_PROFILE_TOOLS`] by REMOVING every route not on the allowlist —
    /// an excluded tool is absent from `tools/list` and a call to it gets
    /// rmcp's standard tool-not-found error. The prompt NAMES are served in
    /// both profiles, but the workflow prompts branch on the profile: the
    /// worker variants end at the handoff to the trusted runner and never
    /// instruct a tool the profile excludes, and the `prompts/list`
    /// descriptions are rewritten here to the
    /// [`WORKER_PROMPT_DESCRIPTIONS`] variants for the same reason.
    pub fn new_with_profile(config_path: PathBuf, profile: McpProfile) -> Self {
        Self::try_new_with_profile(config_path, profile).unwrap_or_else(|drift| {
            panic!(
                "the {profile:?} profile's guidance projection no longer matches its source: \
                 {drift}"
            )
        })
    }

    /// [`Self::new_with_profile`], refusing instead of panicking when the
    /// worker projection has drifted from its source.
    ///
    /// WHY THIS EXISTS, stated precisely because the claim it corrects was
    /// wrong. The three worker projections are CHECKED rewrites: a needle
    /// that stops matching, or matches twice, or names a route that is no
    /// longer served, must never be applied silently — a no-op replace
    /// serves the DEFAULT sentence to a worker, which is the hole the whole
    /// [`WORKER_INSTRUCTIONS_REWRITES`] table closes.
    ///
    /// The comments here used to call that a build invariant. It is not.
    /// Every operand IS a compile-time constant — [`INSTRUCTIONS`] is an
    /// `include_str!` of the skill file — but nothing verifies the match at
    /// compile time. The check runs at server CONSTRUCTION, which
    /// [`crate::serve_stdio`] reaches on the live `rocky mcp --profile
    /// worker` path. An edit to the skill therefore COMPILES, and then
    /// fails when the server starts. What actually guarantees the frozen
    /// constants still line up is a TEST
    /// (`worker_instructions_are_projected_and_default_stays_verbatim`),
    /// which is a weaker guarantee than the word "invariant" implies. They
    /// match today; nothing triggers this at runtime as things stand.
    ///
    /// Both failure modes ABORT STARTUP. That is deliberate and is not the
    /// part being softened: a server that starts with degraded guidance is
    /// strictly worse than one that does not start, because the degradation
    /// is quiet and this defect class has been found nine times. What
    /// changes is only the diagnostic — an operator gets a named refusal
    /// instead of a Rust backtrace.
    ///
    /// [`Self::new_with_profile`] keeps panicking, and every test builds
    /// through it, so the drift still fails loudly wherever it is checked.
    pub fn try_new_with_profile(config_path: PathBuf, profile: McpProfile) -> Result<Self, String> {
        let root = config_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let models_dir = root.join("models");
        let mut tool_router = Self::tool_router();
        let mut prompt_router = Self::prompt_router();
        let mut instructions = INSTRUCTIONS.to_string();
        if profile == McpProfile::Worker {
            let mut all: Vec<String> = tool_router
                .list_all()
                .into_iter()
                .map(|t| t.name.to_string())
                .collect();
            all.sort();
            // DERIVED here, from the full router, before anything is
            // removed — this is the only point where both surfaces exist at
            // once. The banner reads it, so a tool that leaves the
            // allowlist is named as unavailable without anyone editing a
            // literal.
            let excluded: Vec<String> = all
                .iter()
                .filter(|name| !WORKER_PROFILE_TOOLS.contains(&name.as_str()))
                .cloned()
                .collect();
            for name in all {
                if !WORKER_PROFILE_TOOLS.contains(&name.as_str()) {
                    tool_router.remove_route(&name);
                }
            }
            // F3 red team round 2 (finding 1): the served `instructions`
            // were EXEMPT from the sweep on the argument that a disclaiming
            // banner over verbatim text is honest. It was honest and not
            // safe — the banner never stopped the worker at CHECK
            // authorship, and the text below it told the worker to
            // strengthen assertions and append tests. Projected now, by the
            // same checked-rewrite mechanism as the descriptions above.
            instructions = worker_instructions(&excluded, WORKER_INSTRUCTIONS_REWRITES)?;
            // FF-WP1 fix round 2 (item 5b): the static prompt descriptions
            // instruct the default workflow (they name tools this profile
            // excludes) — swap in the worker descriptions. A rename that
            // orphans an entry is refused HERE, at construction, so every
            // test that builds a worker server catches the drift.
            for (name, description) in WORKER_PROMPT_DESCRIPTIONS {
                let route = prompt_router.map.get_mut(*name).ok_or_else(|| {
                    format!("WORKER_PROMPT_DESCRIPTIONS names unrouted prompt '{name}'")
                })?;
                route.attr.description = Some((*description).to_string());
            }
            // F3 red team (finding 3): the same problem one surface over.
            // `tools/list` descriptions are static too, and three of them
            // steered the worker at `propose`. Rewritten AFTER the removals
            // above, so the table can only name a tool this profile still
            // serves — an entry for a removed tool orphans and is refused
            // here.
            for (name, needle, replacement) in WORKER_TOOL_DESCRIPTIONS {
                let route = tool_router.map.get_mut(*name).ok_or_else(|| {
                    format!("WORKER_TOOL_DESCRIPTIONS names unserved tool '{name}'")
                })?;
                let current = route.attr.description.as_deref().ok_or_else(|| {
                    format!("WORKER_TOOL_DESCRIPTIONS names undescribed tool '{name}'")
                })?;
                route.attr.description =
                    Some(worker_tool_description(name, current, needle, replacement)?.into());
            }
        }
        Ok(Self {
            config_path,
            models_dir,
            root,
            profile,
            instructions,
            tool_router,
            prompt_router,
        })
    }

    fn state_path(&self) -> PathBuf {
        rocky_core::state::resolve_state_path(None, &self.models_dir).path
    }

    /// Whether this server serves the `review_queue` APPROVE action — writing
    /// the human sign-off marker that unblocks `rocky apply` (#1517).
    ///
    /// ONLY [`McpProfile::Approver`] does. Written as an exhaustive match, not
    /// `!= Default`, so a future profile has to state its answer here instead
    /// of inheriting one: a new variant fails to compile until someone
    /// decides, and the decision defaults to nothing.
    fn approve_action_served(&self) -> bool {
        match self.profile {
            McpProfile::Default | McpProfile::Worker => false,
            McpProfile::Approver => true,
        }
    }

    /// The `next_steps` reminder a successful `draft_model` result carries.
    /// The worker profile's variant ends at the trusted-runner hand-off and
    /// never instructs `propose` (FF-WP1 fix round 2, item 5c). The approver
    /// profile is the default surface plus one action, so it shares the
    /// default text — which already ends at the human's `rocky review`.
    fn draft_model_next_steps(&self) -> &'static str {
        match self.profile {
            McpProfile::Default | McpProfile::Approver => DRAFT_NEXT_STEPS,
            McpProfile::Worker => WORKER_DRAFT_NEXT_STEPS,
        }
    }

    /// The `next_steps` reminder a successful `draft_check` result carries —
    /// profile-selected like [`Self::draft_model_next_steps`].
    ///
    /// The worker arm is unreachable in practice: `draft_check` left
    /// `WORKER_PROFILE_TOOLS`, so a worker session cannot call the tool that
    /// would produce this text. Kept, and kept correct, because the arm is
    /// the thing that would be wrong first if the tool were ever
    /// re-allowlisted — a defaulted or stale arm is how a re-admitted tool
    /// would ship worker-facing text naming excluded verbs.
    fn draft_check_next_steps(&self) -> &'static str {
        match self.profile {
            McpProfile::Default | McpProfile::Approver => DRAFT_CHECK_NEXT_STEPS,
            McpProfile::Worker => WORKER_DRAFT_CHECK_NEXT_STEPS,
        }
    }

    /// Path to the project's `data/seed.sql`, if it exists. The playground
    /// convention is `<project>/models/` + `<project>/data/seed.sql`, so the
    /// parent of the models dir is the place to look.
    fn seed_file(&self) -> Option<PathBuf> {
        let p = self
            .models_dir
            .parent()
            .unwrap_or(Path::new("."))
            .join("data")
            .join("seed.sql");
        p.is_file().then_some(p)
    }

    /// Compile the project in-process, returning the raw compiler result for
    /// the lineage / inspect tools. Source schemas come from the warm schema
    /// cache when one exists, degrading to empty on a cold cache (typecheck
    /// then falls back to `Unknown` for source-leaf columns — the same
    /// behaviour as `rocky compile` without a warm cache).
    fn compile_full(&self) -> anyhow::Result<CompilerResult> {
        let source_schemas = self.load_source_schemas();
        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas,
            ..Default::default()
        };
        compile::compile(&config).map_err(|e| anyhow::anyhow!("compile failed: {e}"))
    }

    /// Load typed source schemas from the persisted schema cache, honouring
    /// `[cache.schemas]`. Returns an empty map on a cold cache / missing
    /// config / disabled cache — the typecheck degrades to `Unknown`.
    fn load_source_schemas(
        &self,
    ) -> std::collections::HashMap<String, Vec<rocky_compiler::types::TypedColumn>> {
        use rocky_compiler::schema_cache::load_source_schemas_from_cache;
        use rocky_core::state::StateStore;

        let Ok(cfg) = rocky_core::config::load_rocky_config(&self.config_path) else {
            return std::collections::HashMap::new();
        };
        if !cfg.cache.schemas.enabled {
            return std::collections::HashMap::new();
        }
        let Ok(store) = StateStore::open_read_only(&self.state_path()) else {
            return std::collections::HashMap::new();
        };
        load_source_schemas_from_cache(&store, chrono::Utc::now(), cfg.cache.schemas.ttl())
            .unwrap_or_default()
    }

    /// Classify the semantic breaking changes between the working tree (HEAD
    /// of the on-disk files) and the models at `base_ref`. Reuses the exact
    /// compile + classify path `rocky review` runs: compile HEAD with the warm
    /// source-schema cache, `extract_base_compile` the base ref, lower both to
    /// `ProjectIr`, and `diff_project_ir`.
    ///
    /// On any step that prevents the gate from running (HEAD or base fails to
    /// compile — typically because the project isn't a git repo), returns a
    /// result with `skipped_reason` set and zeroed counts so the caller can
    /// distinguish "clean diff" from "gate didn't run".
    fn compute_breaking_change(&self, base_ref: &str) -> BreakingChangeResult {
        let source_schemas = self.load_source_schemas();

        let config = CompilerConfig {
            models_dir: self.models_dir.clone(),
            contracts_dir: None,
            source_schemas: source_schemas.clone(),
            ..Default::default()
        };
        let head_compile = match compile::compile(&config) {
            Ok(r) => r,
            Err(e) => {
                return BreakingChangeResult {
                    skipped_reason: Some(format!("HEAD compile failed: {e}")),
                    ..Default::default()
                };
            }
        };

        let base_compile =
            match commands::extract_base_compile(base_ref, &self.models_dir, source_schemas) {
                Ok(r) => r,
                Err(reason) => {
                    return BreakingChangeResult {
                        skipped_reason: Some(format!("base ref '{base_ref}': {reason}")),
                        ..Default::default()
                    };
                }
            };

        let base_ir = commands::project_ir_from_compile(&base_compile);
        let head_ir = commands::project_ir_from_compile(&head_compile);
        let findings = rocky_core::breaking_change::diff_project_ir(&base_ir, &head_ir);

        let breaking_count = findings.iter().filter(|f| f.is_breaking()).count();
        let lite = findings.iter().map(breaking_finding_lite).collect();
        BreakingChangeResult {
            has_breaking: breaking_count > 0,
            breaking_count,
            findings: lite,
            skipped_reason: None,
        }
    }

    // -------------------------- MUST tools ---------------------------------

    #[tool(
        description = "Type-check the Rocky project and return diagnostics (errors/warnings) \
         plus model count. Always reflects the current on-disk models. Read diagnostics' \
         code/span/suggestion and fix against them — this is the fast feedback loop. Pass \
         `target_dialect` (databricks/snowflake/bigquery/duckdb) to additionally run the P001 \
         portability lint on demand: SQL that won't port to that dialect surfaces as P001."
    )]
    async fn compile(&self, params: Parameters<CompileArgs>) -> ToolResult<CompileResult> {
        let args = params.0;
        let model = args.model.as_deref();
        // On-demand portability lint: parse the requested dialect (case-
        // insensitive, matching the `Dialect` serde vocabulary). When absent,
        // pass `None` so the lint stays driven solely by `[portability]` in
        // rocky.toml — i.e. behaviour is unchanged.
        let target_dialect = match args.target_dialect.as_deref() {
            Some(d) => Some(parse_target_dialect(d)?),
            None => None,
        };
        // `--with-seed` hard-fails when `data/seed.sql` is absent, so opt in
        // only when the project actually ships a seed (the playground does);
        // otherwise rely on the warm schema cache / cold-cache degradation.
        let with_seed = self.seed_file().is_some();
        let output = commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            model,
            false,
            target_dialect,
            with_seed,
            None,
        )
        .map_err(|e| match e.downcast_ref::<commands::ModelNotFound>() {
            Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
            None => ToolError::compile_failed(format!("{e:#}")),
        })?;
        Ok(Json(project_compile_result(&output)))
    }

    // FOURTEENTH ROUND, finding 1 — this said "the exact SQL Rocky would
    // execute", and the preview is offline: it passes no warehouse to
    // `sql_gen::generate_transformation_sql_with_warehouse`, and
    // `commands::plan_preview_output` logs and SKIPS any model whose SQL
    // that call cannot render. `PlanPreviewResult` carries `statements` and
    // nothing else, so a skipped model leaves no trace in the result at
    // all. Three strategies are skipped by construction — a Snowflake
    // `DynamicTable` needs a compute warehouse, a `TimeInterval` model
    // needs a runtime window that static planning leaves `None`, and
    // `ContentAddressed` never reaches SQL generation — and any other
    // render failure is swallowed the same way.
    #[tool(
        description = "Render the SQL Rocky generates for the project's transformation models, \
         offline and with no warehouse connection. It is not the whole plan: a model whose SQL \
         cannot be rendered offline is SKIPPED, and the result does not name it, so a short or \
         empty statement list is not proof the project has nothing else to do. Skipped by \
         construction: a Snowflake dynamic table (it needs a live compute warehouse), a \
         time-interval model (it needs a runtime window), and a content-addressed model (it \
         never goes through SQL generation). Read the statements it does return to confirm the \
         generated SQL matches intent before proposing a materialization."
    )]
    async fn plan_preview(
        &self,
        params: Parameters<PlanPreviewArgs>,
    ) -> ToolResult<PlanPreviewResult> {
        let model = params.0.model.as_deref();
        let output =
            commands::plan_preview_output(Some(&self.config_path), &self.models_dir, model, None)
                .map_err(|e| match e.downcast_ref::<commands::ModelNotFound>() {
                // Preserve the stable taxonomy: an unknown `model` is
                // `model_not_found` (with its "list the models, retry" hint),
                // not the generic compile-failure bucket — so an agent that
                // typo'd or hallucinated a model name recovers correctly.
                Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
                None => ToolError::compile_failed(format!("{e:#}")),
            })?;
        let statements = output
            .statements
            .into_iter()
            .map(|s| PlannedStatementLite {
                purpose: s.purpose,
                target: s.target,
                sql: s.sql,
            })
            .collect();
        Ok(Json(PlanPreviewResult { statements }))
    }

    #[tool(
        description = "Explore column-level lineage for a model. Without `column`, returns the \
         model's columns plus upstream/downstream models and the model-level edge set. With \
         `column`, returns the column trace; set `downstream` to trace consumers instead of sources."
    )]
    async fn lineage(&self, params: Parameters<LineageArgs>) -> ToolResult<LineageResult> {
        let args = params.0;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        if let Some(column) = args.column.as_deref() {
            let out =
                commands::column_lineage_output(&result, &args.model, column, args.downstream)
                    .map_err(|_| ToolError::model_not_found(&args.model))?;
            let edges = out.trace.iter().map(edge_lite).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: Some(out.column),
                direction: Some(out.direction),
                columns: vec![],
                upstream: vec![],
                downstream: vec![],
                edges,
            }))
        } else {
            let out = commands::lineage_output(&result, &args.model)
                .map_err(|_| ToolError::model_not_found(&args.model))?;
            let edges = out.edges.iter().map(edge_lite).collect();
            let columns = out.columns.into_iter().map(|c| c.name).collect();
            Ok(Json(LineageResult {
                model: out.model,
                column: None,
                direction: None,
                columns,
                upstream: out.upstream,
                downstream: out.downstream,
                edges,
            }))
        }
    }

    #[tool(
        description = "Run the project's DuckDB-backed local tests and return pass/fail counts \
         plus per-failure detail. Covers BOTH local suites: executing each model, and the \
         fixture-driven `[[test]]` blocks declared in model sidecars. `failures` carries both, \
         each tagged with its `suite`; `models` and `unit_tests` hold the per-suite counts. \
         Branch on `all_passed` — it is true only when both suites are clean. Use after writing \
         or changing a model. Pass `model` to scope the run to one model's tests."
    )]
    async fn test(&self, params: Parameters<TestArgs>) -> ToolResult<TestResult> {
        let output = commands::test_output(&self.models_dir, None, params.0.model.as_deref())
            .map_err(|e| {
                // Preserve the stable taxonomy the way `compile` and
                // `plan_preview` do: an unknown `model` filter is
                // `model_not_found` (with its "list the models, retry" hint),
                // not the generic internal bucket.
                match e.downcast_ref::<commands::ModelNotFound>() {
                    Some(commands::ModelNotFound(name)) => ToolError::model_not_found(name),
                    None => ToolError::internal(
                        format!("{e:#}"),
                        "The local test runner could not execute; confirm the project compiles \
                         (the `compile` tool) and any `data/seed.sql` the tests need is present.",
                    ),
                }
            })?;
        // BOTH suites, aggregated. `test_output` records the fixture
        // `[[test]]` run in a SEPARATE `unit_tests` summary, and this result
        // used to drop it: a project whose models all execute but whose
        // fixture test fails came back as `failures: []`, which is the
        // vacuous pass this work package exists to remove. The worker prompt
        // tells a worker to stop when the tests pass, so the empty list
        // stopped it on a failing test.
        let models = TestSuiteCounts {
            total: output.total,
            passed: output.passed,
            failed: output.failures.len(),
        };
        let mut failures: Vec<TestFailureLite> = output
            .failures
            .into_iter()
            .map(|f| TestFailureLite {
                name: f.name,
                error: f.error,
                suite: "model".to_string(),
            })
            .collect();
        // Absent `unit_tests` means the project declares no `[[test]]`
        // block. That is zero tests, not zero failures of an unrun suite,
        // and the counts say so rather than the field going missing.
        let unit_tests = match &output.unit_tests {
            Some(summary) => TestSuiteCounts {
                total: summary.total,
                passed: summary.passed,
                failed: summary.failed,
            },
            None => TestSuiteCounts {
                total: 0,
                passed: 0,
                failed: 0,
            },
        };
        if let Some(summary) = output.unit_tests {
            for result in summary.results.into_iter().filter(|r| !r.passed) {
                // `run_one_unit_test` sets `error` on every failure path, so
                // the fallback is unreachable today. It is here because a
                // failure that reports no reason at all is worse than one
                // that reports a row count — the worker has to see SOMETHING
                // to act on.
                let mismatches = result.mismatches.len();
                let error = result
                    .error
                    .unwrap_or_else(|| format!("{mismatches} row(s) did not match `expect`"));
                failures.push(TestFailureLite {
                    name: format!("{}::{}", result.model, result.test),
                    error,
                    suite: "unit".to_string(),
                });
            }
        }
        let all_passed = failures.is_empty();
        Ok(Json(TestResult {
            total: models.total + unit_tests.total,
            passed: models.passed + unit_tests.passed,
            failures,
            all_passed,
            models,
            unit_tests,
        }))
    }

    #[tool(
        description = "List project entities. `kind` is one of: models, pipelines, adapters, sources."
    )]
    async fn list(&self, params: Parameters<ListArgs>) -> ToolResult<ListResult> {
        let kind = params.0.kind;
        let entries = match kind.as_str() {
            "models" => {
                let out = commands::list_models_output(&self.models_dir)
                    .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
                out.models
                    .into_iter()
                    .map(|m| ListEntry {
                        name: m.name,
                        target: Some(m.target),
                        strategy: Some(m.strategy),
                        depends_on: m.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "pipelines" => {
                let out = commands::list_pipelines_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.pipelines
                    .into_iter()
                    .map(|p| ListEntry {
                        name: p.name,
                        pipeline_type: Some(p.pipeline_type),
                        target_adapter: Some(p.target_adapter),
                        depends_on: p.depends_on,
                        ..Default::default()
                    })
                    .collect()
            }
            "adapters" => {
                let out = commands::list_adapters_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.adapters
                    .into_iter()
                    .map(|a| ListEntry {
                        name: a.name,
                        adapter_type: Some(a.adapter_type),
                        host: a.host,
                        ..Default::default()
                    })
                    .collect()
            }
            "sources" => {
                let out = commands::list_sources_output(&self.config_path)
                    .map_err(|e| ToolError::config_invalid(format!("{e:#}")))?;
                out.sources
                    .into_iter()
                    .map(|s| ListEntry {
                        name: s.pipeline,
                        adapter: Some(s.adapter),
                        catalog: s.catalog,
                        ..Default::default()
                    })
                    .collect()
            }
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown kind '{other}'"),
                    "Pass one of: models, pipelines, adapters, sources.",
                ));
            }
        };
        Ok(Json(ListResult { kind, entries }))
    }

    #[tool(
        description = "Return the typed columns of every model and source table in the project. \
         Use this to learn what's available to select from and the upstream types — never guess \
         column names. Models and declared sources are exact; physical warehouse tables are \
         appended best-effort, so CHECK `discovery_incomplete` before concluding the warehouse \
         holds nothing else — when it is true the append did not run, `discovery_error` says \
         why, and a table missing from `sources` is inconclusive, not absent."
    )]
    async fn inspect_schema(
        &self,
        _params: Parameters<InspectSchemaArgs>,
    ) -> ToolResult<InspectSchemaResult> {
        let to_entries = |buckets: Vec<(String, Vec<rocky_compiler::types::TypedColumn>)>| {
            buckets
                .into_iter()
                .map(|(name, cols)| SchemaEntry {
                    name,
                    columns: cols
                        .into_iter()
                        .map(|c| ColumnLite {
                            name: c.name,
                            data_type: c.data_type.to_string(),
                            nullable: c.nullable,
                        })
                        .collect(),
                })
                .collect::<Vec<_>>()
        };

        // Compile to learn the project's models. Tolerate a models-less project
        // (cold start) — there, the source discovery below is the whole point.
        let (models, mut sources, model_targets) = match self.compile_full() {
            Ok(result) => {
                let (model_schemas, source_tables) = commands::build_schema_context(&result);
                let targets: std::collections::HashSet<String> = result
                    .project
                    .models
                    .iter()
                    .map(|m| format!("{}.{}", m.config.target.schema, m.config.target.table))
                    .collect();
                (
                    to_entries(model_schemas),
                    to_entries(source_tables),
                    targets,
                )
            }
            Err(e) if e.to_string().contains("no models found") => {
                (Vec::new(), Vec::new(), std::collections::HashSet::new())
            }
            Err(e) => return Err(ToolError::compile_failed(format!("{e:#}"))),
        };

        // Surface the physical warehouse tables so an agent can ground a raw
        // source the project never declared — and at cold start, before any
        // model exists. Skip a table that is a model's target or is already
        // reported as a compile-derived source.
        //
        // Still best-effort: a warehouse Rocky cannot reach must not fail the
        // whole tool, because `models` and the compile-derived `sources` are
        // exact and useful on their own. What changed is that the degradation
        // is now REPORTED. Every arm is handled — the old `if let Ok(Some(..))`
        // dropped the `Err`, so a resolution failure and an empty warehouse
        // returned the same thing (#1533).
        let (discovery_incomplete, discovery_error) = match self.warehouse_adapter() {
            Ok(Some(adapter)) => {
                let seen: std::collections::HashSet<String> =
                    sources.iter().map(|s| s.name.clone()).collect();
                match discover_source_tables(adapter.as_ref()).await {
                    Ok(entries) => {
                        for entry in entries {
                            if model_targets.contains(&entry.name) || seen.contains(&entry.name) {
                                continue;
                            }
                            sources.push(entry);
                        }
                        (false, None)
                    }
                    Err(e) => (true, Some(format!("the discovery query failed: {e}"))),
                }
            }
            // Not reachable today — `warehouse_adapter` never returns `Ok(None)`
            // — but handled rather than lumped in with success, so it cannot
            // become another silent empty if that ever changes.
            Ok(None) => (
                true,
                Some("no target warehouse adapter is configured".to_string()),
            ),
            Err(e) => (
                true,
                Some(format!(
                    "the target warehouse adapter did not resolve: {e:#}"
                )),
            ),
        };

        Ok(Json(InspectSchemaResult {
            models,
            sources,
            discovery_incomplete,
            discovery_error,
        }))
    }

    #[tool(
        description = "Classify the semantic breaking changes between the working-tree models \
         and the models at a base git ref (default HEAD). Reuses the exact compile + typed-IR \
         classifier that `rocky review` and the branch-promote gate run. Self-check blast radius \
         BEFORE propose. Returns {has_breaking, breaking_count, findings:[{change, severity, \
         model, column?, message}]}. When the gate can't run (non-git project, or either side \
         fails to compile), `skipped_reason` is set and the counts are zero."
    )]
    async fn breaking_change(
        &self,
        params: Parameters<BreakingChangeArgs>,
    ) -> ToolResult<BreakingChangeResult> {
        let base = params.0.base;
        Ok(Json(self.compute_breaking_change(&base)))
    }

    #[tool(
        description = "List the downstream models that depend on a given model (the reverse of \
         `lineage`). For each dependent, returns the focal model's columns it reads via \
         `via_columns`. Use to gauge the blast radius of changing a model before editing it."
    )]
    async fn dependents(&self, params: Parameters<DependentsArgs>) -> ToolResult<DependentsResult> {
        let model = params.0.model;
        let result = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;

        // Assert the focal model exists in the semantic graph — same
        // not-found contract as `lineage_output`.
        let schema = result
            .semantic_graph
            .model_schema(&model)
            .ok_or_else(|| ToolError::model_not_found(&model))?;

        // Downstream model names come straight from the model schema; the
        // per-dependent `via_columns` are the focal model's columns that feed
        // each dependent, collected from the column-level edge set (the
        // reverse direction of the `lineage` edge filter).
        let mut dependents: Vec<DependentEntry> = schema
            .downstream
            .iter()
            .map(|dep| {
                let mut via_columns: Vec<String> = result
                    .semantic_graph
                    .edges
                    .iter()
                    .filter(|e| *e.source.model == *model && *e.target.model == **dep)
                    .map(|e| e.source.column.to_string())
                    .collect();
                via_columns.sort();
                via_columns.dedup();
                DependentEntry {
                    model: dep.clone(),
                    via_columns,
                }
            })
            .collect();
        dependents.sort_by(|a, b| a.model.cmp(&b.model));

        Ok(Json(DependentsResult { model, dependents }))
    }

    #[tool(
        description = "Return the project-wide asset catalog in one call: every model and source \
         with its typed columns and upstream/downstream model lists. Use to orient on the whole \
         project at once. For the column-level edge trace of a single model use `lineage`; for \
         typed columns alone use `inspect_schema`; for one model's consumers use `dependents`."
    )]
    async fn catalog(&self, _params: Parameters<CatalogArgs>) -> ToolResult<CatalogResult> {
        let output = commands::compute_catalog_output(
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        Ok(Json(catalog_result(output)))
    }

    #[tool(
        description = "Read run history from the state store. Without `model`, returns the recent \
         project-level runs (id, status, trigger, duration). With `model`, returns that model's \
         executions (duration, rows, status, sql_hash) newest-first. Grounds proposals in \
         operational reality — is this model flaky, slow, recently changed? Empty when nothing has \
         been run yet."
    )]
    async fn history(&self, params: Parameters<HistoryArgs>) -> ToolResult<HistoryResult> {
        let state_path = self.state_path();
        match params.0.model {
            Some(model) => {
                let out = commands::model_history_output(&state_path, &model, None, false, 20)
                    .map_err(|e| {
                        ToolError::internal(
                            format!("{e:#}"),
                            "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                        )
                    })?;
                let executions = out
                    .executions
                    .into_iter()
                    .map(|e| ModelExecutionLite {
                        started_at: e.started_at.to_rfc3339(),
                        duration_ms: e.duration_ms,
                        rows_affected: e.rows_affected,
                        status: e.status,
                        sql_hash: e.sql_hash,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: Some(out.model),
                    runs: vec![],
                    executions,
                }))
            }
            None => {
                let out = commands::history_runs_output_filtered(
                    &state_path,
                    None,
                    false,
                    params.0.trigger.as_deref(),
                )
                .map_err(|e| {
                    ToolError::internal(
                        format!("{e:#}"),
                        "Could not read the run history from the state store; ensure the \
                             project has been run at least once (history is empty, not an error, \
                             before the first run).",
                    )
                })?;
                let runs = out
                    .runs
                    .into_iter()
                    .map(|r| RunHistoryLite {
                        run_id: r.run_id,
                        started_at: r.started_at.to_rfc3339(),
                        status: r.status,
                        trigger: r.trigger,
                        models_executed: r.models_executed,
                        duration_ms: r.duration_ms,
                    })
                    .collect();
                Ok(Json(HistoryResult {
                    model: None,
                    runs,
                    executions: vec![],
                }))
            }
        }
    }

    #[tool(
        description = "Read a model's quality-metric snapshots from the state store: row count, \
         freshness lag, and per-column null rates over recent runs, plus derived freshness / \
         null-rate alerts. Pass `column` to also get that column's per-run trend. `message` is set \
         (and snapshots empty) when the model has no recorded metrics yet."
    )]
    async fn metrics(&self, params: Parameters<MetricsArgs>) -> ToolResult<MetricsResult> {
        let args = params.0;
        let out = commands::metrics_output(
            &self.state_path(),
            &args.model,
            true,
            args.column.as_deref(),
            true,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not read quality metrics from the state store; ensure the project has been \
                 run at least once (a model with no recorded metrics returns an empty result with \
                 a `message`, not an error).",
            )
        })?;

        let snapshots = out
            .snapshots
            .into_iter()
            .map(|s| MetricsSnapshotLite {
                run_id: s.run_id,
                timestamp: s.timestamp.to_rfc3339(),
                row_count: s.row_count,
                freshness_lag_seconds: s.freshness_lag_seconds,
                null_rates: s
                    .null_rates
                    .into_iter()
                    .map(|(column, null_rate)| ColumnNullRateLite { column, null_rate })
                    .collect(),
            })
            .collect();
        let alerts = out
            .alerts
            .into_iter()
            .map(|a| MetricsAlertLite {
                kind: a.kind,
                severity: a.severity,
                message: a.message,
                column: a.column,
            })
            .collect();
        Ok(Json(MetricsResult {
            model: out.model,
            snapshots,
            alerts,
            message: out.message,
        }))
    }

    #[tool(
        description = "Cost-model materialization recommendations from run history + the on-disk \
         DAG: for each model, the current vs recommended strategy, projected monthly savings, and \
         the reasoning. Use to reason about materialization with Rocky's cost model rather than \
         guessing. `message` is set (and recommendations empty) when there's no run history yet."
    )]
    async fn optimize(&self, params: Parameters<OptimizeArgs>) -> ToolResult<OptimizeResult> {
        let out = commands::optimize_output(
            &self.state_path(),
            Some(&self.models_dir),
            params.0.model.as_deref(),
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not compute optimization recommendations; ensure the project compiles and \
                 has run history (no history returns an empty result with a `message`, not an \
                 error).",
            )
        })?;
        let recommendations = out
            .recommendations
            .into_iter()
            .map(|r| OptimizeRecommendationLite {
                model_name: r.model_name,
                current_strategy: r.current_strategy,
                recommended_strategy: r.recommended_strategy,
                estimated_monthly_savings: r.estimated_monthly_savings,
                reasoning: r.reasoning,
                downstream_references: r.downstream_references,
            })
            .collect();
        Ok(Json(OptimizeResult {
            recommendations,
            message: out.message,
        }))
    }

    #[tool(
        description = "Draft a `[freshness]` TOML block for a model with temporal columns (the \
         W005 fix): an LLM picks a sensible `expected_lag_seconds` TTL and a `time_column` from \
         the supplied candidates. Returns the ready-to-paste block directly (NOT a TextEdit); the \
         caller appends it to the model's sidecar. Requires ANTHROPIC_API_KEY in the server \
         environment — without it, `freshness_block` is null and `message` explains why."
    )]
    async fn suggest_freshness_block(
        &self,
        params: Parameters<SuggestFreshnessBlockArgs>,
    ) -> ToolResult<SuggestFreshnessBlockResult> {
        let args = params.0;

        // Gate on the API key the same way the LSP's freshness arm does;
        // degrade to a null block + message rather than erroring.
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => {
                return Ok(Json(SuggestFreshnessBlockResult {
                    freshness_block: None,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                }));
            }
        };

        let sidecar_text = args.current_sidecar.unwrap_or_default();
        let (system_prompt, user_prompt) = rocky_ai::prompt::build_freshness_fix_prompt(
            &args.model,
            &args.temporal_columns,
            &sidecar_text,
        );

        // Mirror the LSP's AiConfig: anthropic / sonnet / TOML / single attempt.
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "toml".to_string(),
            max_attempts: 1,
            max_tokens: rocky_ai::client::DEFAULT_MAX_TOKENS,
        };
        let client = rocky_ai::client::LlmClient::new(ai_config)
            .map_err(|e| ToolError::ai_error(format!("AI client init failed: {e}")))?;
        let response = client
            .generate(&system_prompt, &user_prompt, None)
            .await
            .map_err(|e| ToolError::ai_error(format!("AI request failed: {e}")))?;

        let extracted = rocky_ai::generate::extract_code(&response.content);
        let snippet = extracted.trim();
        if snippet.is_empty() {
            return Ok(Json(SuggestFreshnessBlockResult {
                freshness_block: None,
                message: Some("AI response did not contain a TOML code block".to_string()),
            }));
        }

        Ok(Json(SuggestFreshnessBlockResult {
            freshness_block: Some(snippet.to_string()),
            message: None,
        }))
    }

    // ------------------------- generator tools -----------------------------
    // These wrap the existing `rocky-ai` generators (the CLI's `rocky ai-*`
    // commands). Each is an LLM/BYOK tool gated on ANTHROPIC_API_KEY, exactly
    // like `suggest_freshness_block`. They return DRAFTS — the agent then runs
    // `compile` / `propose` to act on them; nothing here mutates the warehouse
    // or applies anything.

    #[tool(
        description = "GENERATE a `.contract.toml` for a model from the aggregate per-column \
         profile of its target table with an LLM (the `rocky ai-contract` generator). Proposes \
         required/protected columns and per-column types; the draft is compile-verified against \
         the model's inferred schema before it is returned. Returns the contract TOML as a DRAFT \
         — hand it to `draft_contract` to write + policy-gate it, or save it next to the model and \
         run `compile`; it mutates nothing itself. The model's target table must be materialized. \
         Egress: only aggregate STATISTICS (row/null/distinct counts) are sent to the LLM — no raw \
         cell values. Requires ANTHROPIC_API_KEY in the server environment — without it (or when \
         the target isn't reachable), `contract_toml` is null and `message` explains why."
    )]
    async fn ai_contract(
        &self,
        params: Parameters<AiContractArgs>,
    ) -> ToolResult<AiContractResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(AiContractResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        // The model's inferred output schema — the basis for compile-verifying
        // the drafted contract.
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let inferred_schema: Vec<rocky_compiler::types::TypedColumn> = compiled
            .type_check
            .typed_models
            .get(&model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(&model_name))?;

        // Profile each column against the live target table.
        let profile = match self
            .profile_table_columns(&model_name, &inferred_schema)
            .await
        {
            Ok(p) => p,
            Err(e) => {
                return Ok(Json(AiContractResult {
                    model: model_name,
                    message: Some(format!("could not profile the target table: {e:#}")),
                    ..Default::default()
                }));
            }
        };

        let drafted = rocky_ai::contract::draft_contract(&profile, &inferred_schema, &client, 3)
            .await
            .map_err(|e| ToolError::ai_error(format!("contract draft failed: {e}")))?;

        Ok(Json(AiContractResult {
            model: model_name,
            contract_toml: Some(drafted.toml),
            attempts: Some(drafted.attempts),
            message: None,
        }))
    }

    #[tool(
        description = "GENERATE test assertions for a model from its intent, schema, and SQL with \
         an LLM (the `rocky ai-test` generator). Proposes SQL assertions that each return 0 rows \
         when the invariant holds (not-null, grain uniqueness, value ranges, referential \
         integrity). Returns the assertions as DRAFTS — encode them as declarative `[[tests]]` \
         checks (or hand them to `draft_check` to write + policy-gate) and run them via the `test` \
         tool; it mutates nothing itself. Requires ANTHROPIC_API_KEY in the server environment — \
         without it, `assertions` is empty and `message` explains why."
    )]
    async fn ai_test(&self, params: Parameters<AiTestArgs>) -> ToolResult<AiTestResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(AiTestResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let assertions = rocky_ai::testgen::generate_tests(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("test generation failed: {e}")))?;

        let assertions = assertions
            .into_iter()
            .map(|a| TestAssertionLite {
                name: a.name,
                sql: a.sql,
                description: a.description,
            })
            .collect();

        Ok(Json(AiTestResult {
            model: model_name,
            assertions,
            message: None,
        }))
    }

    #[tool(
        description = "Draft an intent description for a model from its SQL, output schema, and \
         upstream dependencies (the `rocky ai-explain` generator). An LLM writes a 2-3 sentence \
         business-logic summary (grain, key filters/joins/aggregations). Returns the description \
         as a DRAFT — save it to the model's sidecar as `intent = \"...\"` if useful; it mutates \
         nothing. Requires ANTHROPIC_API_KEY in the server environment — without it, `intent` is \
         null and `message` explains why."
    )]
    async fn explain_model(
        &self,
        params: Parameters<ExplainModelArgs>,
    ) -> ToolResult<ExplainModelResult> {
        let model_name = params.0.model;

        let client = match self.make_ai_client() {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Ok(Json(ExplainModelResult {
                    model: model_name,
                    message: Some(format!(
                        "{} not set in the server environment",
                        rocky_ai::client::AI_API_KEY_ENV
                    )),
                    ..Default::default()
                }));
            }
            Err(e) => return Err(ToolError::ai_error(format!("AI client init failed: {e}"))),
        };

        let (compiled, model) = self.compile_and_find_model(&model_name)?;
        let intent = rocky_ai::explain::explain_model(&model, &compiled, &client)
            .await
            .map_err(|e| ToolError::ai_error(format!("explain failed: {e}")))?;

        Ok(Json(ExplainModelResult {
            model: model_name,
            intent: Some(intent),
            message: None,
        }))
    }

    /// Build an [`LlmClient`](rocky_ai::client::LlmClient) for the generator
    /// tools, BYOK via `ANTHROPIC_API_KEY`. Returns `Ok(None)` when the key is
    /// unset so each tool degrades to a null draft + explanatory message (the
    /// same graceful no-op as `suggest_freshness_block`). `[ai] max_tokens`
    /// from `rocky.toml` is honoured when the config loads.
    fn make_ai_client(&self) -> anyhow::Result<Option<rocky_ai::client::LlmClient>> {
        let api_key = match std::env::var(rocky_ai::client::AI_API_KEY_ENV) {
            Ok(v) if !v.is_empty() => v,
            _ => return Ok(None),
        };
        let max_tokens = rocky_core::config::load_rocky_config(&self.config_path)
            .map(|cfg| cfg.ai.max_tokens)
            .unwrap_or(rocky_ai::client::DEFAULT_MAX_TOKENS);
        let ai_config = rocky_ai::client::AiConfig {
            provider: "anthropic".to_string(),
            model: "claude-sonnet-4-6".to_string(),
            api_key: rocky_core::redacted::RedactedString::new(api_key),
            default_format: "rocky".to_string(),
            max_attempts: 3,
            max_tokens,
        };
        rocky_ai::client::LlmClient::new(ai_config)
            .map(Some)
            .map_err(|e| anyhow::anyhow!("{e}"))
    }

    /// Compile the project and resolve `model_name` to its loaded
    /// [`Model`](rocky_core::models::Model). The generators that read source +
    /// intent (`ai_test`, `explain_model`) need both the compile result
    /// and the owned model.
    fn compile_and_find_model(
        &self,
        model_name: &str,
    ) -> Result<(CompilerResult, rocky_core::models::Model), Json<ToolError>> {
        let compiled = self
            .compile_full()
            .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        let model = compiled
            .project
            .models
            .iter()
            .find(|m| m.config.name == model_name)
            .cloned()
            .ok_or_else(|| ToolError::model_not_found(model_name))?;
        Ok((compiled, model))
    }

    /// Profile each column of a model's target table into a
    /// [`TableProfile`](rocky_ai::contract::TableProfile) for `ai_contract`.
    ///
    /// Reuses the grounding path (`prepare_table_query` + `query_grounding`), so
    /// it works on any configured warehouse, not just DuckDB.
    ///
    /// # Egress
    ///
    /// Issues **aggregate statistics only** — `COUNT(*)`, `COUNT(col)`,
    /// `COUNT(DISTINCT col)` — and never selects `MIN`/`MAX` or a domain sample.
    /// No raw cell value leaves the machine; the prompt the LLM sees carries
    /// counts, not data. This mirrors the default of the `rocky ai-contract`
    /// generator this tool wraps (whose `--with-data` opt-in, which would send
    /// observed min/max and low-cardinality samples, is intentionally NOT
    /// exposed over MCP). `null_rate` + `distinct` are enough to draft the
    /// nullable / required / protected constraints; `min`/`max`/`observed_values`
    /// are left empty. SQL is built only from validated identifiers.
    async fn profile_table_columns(
        &self,
        model_name: &str,
        schema: &[rocky_compiler::types::TypedColumn],
    ) -> anyhow::Result<rocky_ai::contract::TableProfile> {
        let prepared = self.prepare_table_query(model_name).await?;

        let mut columns = Vec::with_capacity(schema.len());
        for typed_col in schema {
            let col = rocky_sql::validation::validate_identifier(&typed_col.name)
                .map_err(|e| anyhow::anyhow!("invalid column identifier: {e}"))?;
            // Statistics only — counts, never raw cell values. No MIN/MAX, no
            // domain query, so nothing observable from the table's contents
            // reaches the LLM prompt.
            let agg_sql = column_stats_sql(&prepared.table_ref, col);
            let qr = query_grounding(prepared.adapter.as_ref(), &agg_sql)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("profile query failed for column '{}': {e}", typed_col.name)
                })?;
            let row = qr.rows.first().ok_or_else(|| {
                anyhow::anyhow!("profile query returned no rows for '{}'", typed_col.name)
            })?;

            let total = row.first().map(json_as_u64).unwrap_or(0);
            let non_null = row.get(1).map(json_as_u64).unwrap_or(0);
            let distinct = row.get(2).map(json_as_u64).unwrap_or(0);
            let nulls = total.saturating_sub(non_null);
            let null_rate = if total == 0 {
                0.0
            } else {
                nulls as f64 / total as f64
            };

            columns.push(rocky_ai::contract::ColumnProfile {
                name: typed_col.name.clone(),
                type_name: rocky_ai::contract::contract_type_name(typed_col),
                rows: total,
                nulls,
                null_rate,
                distinct,
                // Raw cell values are never sent over MCP (see # Egress).
                observed_values: Vec::new(),
                min: None,
                max: None,
            });
        }

        Ok(rocky_ai::contract::TableProfile {
            model: model_name.to_string(),
            columns,
        })
    }

    // ----------------- governance + drift preview tools --------------------
    // These let an agent see the full enforcement picture in-loop. Both are
    // read-only DRY-RUNs — neither applies anything. `governance_preview` is
    // offline (compile + sidecar read, the same core `rocky plan` uses);
    // `drift_preview` hits the configured warehouse via the same adapter path
    // as the grounding tools.

    #[tool(
        description = "Preview the pre-apply governance actions a subsequent `rocky run` would \
         reconcile: classification tags, masking policies, and retention policies declared across \
         the project's model sidecars. This is the same control-plane work `rocky plan` previews \
         — a DRY-RUN computed offline from the compiled models + their `[classification]` / `mask` \
         / `retention` config. It performs NO warehouse I/O and applies nothing. Empty action \
         lists mean the project declares no governance for that surface. Pass `env` to resolve \
         `[mask.<env>]` overrides (classification + retention are env-invariant). Use this to \
         confirm a model's PII / masking / retention is wired before proposing — encode an \
         invariant as governance, not just a WHERE clause."
    )]
    async fn governance_preview(
        &self,
        params: Parameters<GovernancePreviewArgs>,
    ) -> ToolResult<GovernancePreviewResult> {
        let env = params.0.env;

        let cfg = rocky_core::config::load_rocky_config(&self.config_path)
            .map_err(|e| ToolError::config_invalid(format!("could not load rocky.toml: {e:#}")))?;
        // Resolve the active pipeline's target adapter type — the same input
        // `rocky plan` feeds `populate_governance_actions` so retention's
        // `warehouse_preview` renders the warehouse-native form. This is the
        // ONLY thing the adapter type drives; classification + masking don't
        // touch it, and retention already degrades to `None` on an unknown
        // type. So a pipeline that won't resolve must not fail this offline
        // tool — degrade to "" and the preview still reports every declared
        // action, just without the warehouse-native retention rendering.
        let adapter_type = rocky_cli::registry::resolve_pipeline(&cfg, None)
            .ok()
            .and_then(|(_, pipeline)| {
                cfg.adapters
                    .get(pipeline.target_adapter())
                    .map(|a| a.adapter_type.clone())
            })
            .unwrap_or_default();

        // Reuse the exact offline governance-preview core `rocky plan` uses —
        // it compiles the models dir and reads each sidecar's governance
        // config, populating a PlanOutput. No discovery, no adapter call.
        let mut output = rocky_cli::output::PlanOutput::new(String::new());
        output.env = env.clone();
        commands::populate_governance_actions(
            &cfg,
            &self.models_dir,
            env.as_deref(),
            &adapter_type,
            &mut output,
        )
        .map_err(|e| ToolError::compile_failed(format!("governance preview failed: {e:#}")))?;

        Ok(Json(GovernancePreviewResult {
            env,
            classification_actions: output
                .classification_actions
                .into_iter()
                .map(|a| ClassificationActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                })
                .collect(),
            mask_actions: output
                .mask_actions
                .into_iter()
                .map(|a| MaskActionLite {
                    model: a.model,
                    column: a.column,
                    tag: a.tag,
                    resolved_strategy: a.resolved_strategy,
                })
                .collect(),
            retention_actions: output
                .retention_actions
                .into_iter()
                .map(|a| RetentionActionLite {
                    model: a.model,
                    duration_days: a.duration_days,
                    warehouse_preview: a.warehouse_preview,
                })
                .collect(),
        }))
    }

    #[tool(
        description = "Preview source-vs-target schema drift between two warehouse tables — the \
         same apples-to-apples comparison `rocky run` performs before an incremental load. Both \
         tables are `DESCRIBE`d and their warehouse-reported column types compared via the engine's \
         drift detector. Read-only: it applies nothing. Pass `source_table` and `target_table` as \
         qualified `schema.table` (or `catalog.schema.table`) references. Returns drifted columns \
         (type changed), added columns (in source, missing from target — a run would ADD COLUMN), \
         and the action the runtime would take (`ignore` / `add_columns` / `alter_column_types` / \
         `drop_and_recreate`). When the target doesn't exist yet, `target_exists` is false and the \
         lists are empty. Hits the configured warehouse — requires live credentials."
    )]
    async fn drift_preview(
        &self,
        params: Parameters<DriftPreviewArgs>,
    ) -> ToolResult<DriftPreviewResult> {
        let args = params.0;

        let adapter = self
            .warehouse_adapter()
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("could not resolve the warehouse adapter: {e:#}"),
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?
            .ok_or_else(|| {
                ToolError::warehouse_error(
                    "could not resolve the target warehouse adapter",
                    "Check the [adapter] block in rocky.toml and that the target warehouse's \
                     credentials are set in the server environment.",
                )
            })?;

        let source_ref = parse_table_ref(&args.source_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid source_table reference '{}'", args.source_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;
        let target_ref = parse_table_ref(&args.target_table).ok_or_else(|| {
            ToolError::invalid_argument(
                format!("invalid target_table reference '{}'", args.target_table),
                "Pass a qualified `schema.table` or `catalog.schema.table` reference.",
            )
        })?;

        // DESCRIBE both tables. A failed describe on the TARGET means it is not
        // materialized yet (the first run would create it) — that's a clean
        // "no drift, target absent" answer, not an error. A failed describe on
        // the SOURCE is a genuine error (you asked to compare against a table
        // that isn't there).
        let source_cols = adapter.describe_table(&source_ref).await.map_err(|e| {
            ToolError::warehouse_error(
                format!(
                    "could not describe source_table '{}': {e}",
                    args.source_table
                ),
                "Confirm the source table exists and the target adapter's credentials can read it.",
            )
        })?;
        // Most adapters `Err` on a missing table, but some report an empty
        // column set instead; treat an empty source as not-found rather than
        // letting it produce a vacuously "no drift" answer that would lie.
        if source_cols.is_empty() {
            return Err(ToolError::warehouse_error(
                format!(
                    "source_table '{}' has no columns (table not found or empty schema)",
                    args.source_table
                ),
                "Confirm the source table exists and is not empty.",
            ));
        }
        let target_cols = adapter
            .describe_table(&target_ref)
            .await
            .unwrap_or_default();
        let target_exists = !target_cols.is_empty();

        if !target_exists {
            return Ok(Json(DriftPreviewResult {
                source_table: args.source_table,
                target_table: args.target_table,
                target_exists: false,
                action: drift_action_wire_name(&rocky_ir::DriftAction::Ignore).to_string(),
                ..Default::default()
            }));
        }

        let result = rocky_core::drift::detect_drift(
            &target_ref,
            &source_cols,
            &target_cols,
            adapter.dialect(),
        );

        // `detect_drift` returns `DriftAction::Ignore` whenever there are no
        // type-changed columns — INCLUDING the added-columns-only case. But
        // `rocky run` does NOT ignore that case: its `else if
        // !added_columns.is_empty()` branch (commands/run.rs) issues
        // `ALTER TABLE ADD COLUMN` and reports the action as `add_columns`.
        // Mirror the runtime's emitted action here so the preview doesn't tell
        // an agent "no action" for a run that would actually ALTER the target.
        let action =
            if result.action == rocky_ir::DriftAction::Ignore && !result.added_columns.is_empty() {
                "add_columns".to_string()
            } else {
                drift_action_wire_name(&result.action).to_string()
            };

        Ok(Json(DriftPreviewResult {
            source_table: args.source_table,
            target_table: args.target_table,
            target_exists: true,
            drifted_columns: result
                .drifted_columns
                .into_iter()
                .map(|c| DriftedColumnLite {
                    name: c.name,
                    source_type: c.source_type,
                    target_type: c.target_type,
                })
                .collect(),
            added_columns: result.added_columns.into_iter().map(|c| c.name).collect(),
            action,
        }))
    }

    // ------------------------- SHOULD tools --------------------------------

    #[tool(
        description = "Sample real rows from a model's target table OR a qualified `schema.table` \
         source reference. Look at literal values, units, and null patterns the schema can't tell \
         you. Omit `percent` to get the first rows (the right default for small tables); set 1–100 \
         for a random-percentage sample. Capped at 50 rows / 16 KB; long cells truncated. Requires \
         live warehouse credentials in the target adapter (rocky.toml)."
    )]
    async fn sample_rows(
        &self,
        params: Parameters<SampleRowsArgs>,
    ) -> ToolResult<SampleRowsResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

        // Build: SELECT * FROM <ref> [tablesample] LIMIT n. The ref is built
        // only from validated identifiers; never `format!`'d from raw input.
        // With no `percent`, return the first rows deterministically — a low
        // percentage sample returns ~0 rows on a small table, which is the most
        // common grounding case. `percent`, when given, is a clamped integer.
        let sample = args
            .percent
            .and_then(|p| prepared.dialect_tablesample(p.clamp(1, 100)))
            .map(|s| format!(" {s}"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM {}{} LIMIT {}",
            prepared.table_ref, sample, SAMPLE_MAX_ROWS
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("sample query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;

        let columns = qr.columns.clone();
        let mut rows: Vec<Vec<String>> = Vec::new();
        let mut truncated = qr.rows.len() > SAMPLE_MAX_ROWS;
        let mut bytes = 0usize;
        for row in qr.rows.into_iter().take(SAMPLE_MAX_ROWS) {
            let cells: Vec<String> = row.into_iter().map(render_cell).collect();
            bytes += cells.iter().map(String::len).sum::<usize>();
            if bytes > SAMPLE_MAX_BYTES {
                truncated = true;
                break;
            }
            rows.push(cells);
        }

        Ok(Json(SampleRowsResult {
            unavailable: false,
            reason: None,
            columns,
            rows,
            truncated,
        }))
    }

    #[tool(
        description = "Profile one column of a model's target table OR a qualified `schema.table` \
         source: row count, nulls, null rate, distinct count, min, max — and, for a \
         low-cardinality column (≤25 distinct), the distinct values with their counts \
         (`top_values`), which surfaces exact literals (e.g. a status string) that min/max hide. \
         `top_values` comes from a second query and is best-effort: when that query fails the \
         list is empty and this still succeeds. Requires live warehouse credentials in the \
         target adapter (rocky.toml)."
    )]
    async fn profile_column(
        &self,
        params: Parameters<ProfileColumnArgs>,
    ) -> ToolResult<ProfileColumnResult> {
        let args = params.0;

        let prepared = self.prepare_table_query(&args.model).await.map_err(|e| {
            ToolError::warehouse_error(
                format!("{e:#}"),
                "Confirm the model name or `schema.table` reference exists and the target \
                     adapter in rocky.toml has live warehouse credentials.",
            )
        })?;

        let col = rocky_sql::validation::validate_identifier(&args.column).map_err(|e| {
            ToolError::invalid_argument(
                format!("invalid column identifier: {e}"),
                "Pass a valid column name (letters, digits, and underscores); verify it with \
                 `inspect_schema`.",
            )
        })?;

        // Cast to the dialect's string type — `VARCHAR` everywhere except
        // BigQuery, where it is `STRING` (BigQuery rejects `CAST(... AS VARCHAR)`).
        let string_type = prepared.adapter.dialect().string_type_name();
        let sql = format!(
            "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n, \
             CAST(MIN({col}) AS {string_type}) AS min_v, \
             CAST(MAX({col}) AS {string_type}) AS max_v \
             FROM {}",
            prepared.table_ref
        );

        let qr = query_grounding(prepared.adapter.as_ref(), &sql)
            .await
            .map_err(|e| {
                ToolError::warehouse_error(
                    format!("profile query failed: {e}"),
                    "Confirm the table is materialized and the target adapter's credentials can \
                     read it.",
                )
            })?;
        let row = qr.rows.first().ok_or_else(|| {
            ToolError::warehouse_error(
                "profile query returned no rows",
                "Confirm the target table is materialized and non-empty.",
            )
        })?;

        let as_u64 = |v: &serde_json::Value| -> u64 {
            match v {
                serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
                serde_json::Value::String(s) => s.parse().unwrap_or(0),
                _ => 0,
            }
        };
        let total = row.first().map(as_u64).unwrap_or(0);
        let non_null = row.get(1).map(as_u64).unwrap_or(0);
        let distinct = row.get(2).map(as_u64).unwrap_or(0);
        let nulls = total.saturating_sub(non_null);
        let null_rate = if total == 0 {
            0.0
        } else {
            nulls as f64 / total as f64
        };
        let str_cell = |v: Option<&serde_json::Value>| -> Option<String> {
            match v {
                Some(serde_json::Value::Null) | None => None,
                Some(serde_json::Value::String(s)) => Some(s.clone()),
                Some(other) => Some(other.to_string()),
            }
        };

        // For a low-cardinality column, surface the distinct values + their
        // counts — what `min`/`max` alone can't reveal (e.g. that `status`
        // holds 'COMPLETE', not 'completed'). One extra grouped query, run only
        // when the cardinality makes it cheap.
        //
        // THIS SECOND READ IS BEST-EFFORT, AND SAYS SO NOWHERE IN THE RESULT.
        // The primary query above surfaces its failure as
        // `ToolError::warehouse_error`. This one takes `Err(_) => Vec::new()`
        // and the tool then returns SUCCESS: a transient failure here yields a
        // non-zero `distinct` beside an empty `top_values`, which is also what
        // a high-cardinality column and an all-null one produce. `unavailable`
        // and `reason` below are set to `false`/`None` unconditionally, so
        // nothing distinguishes the three.
        //
        // That made this the THIRD best-effort warehouse read on these tools
        // that reported success on failure — the two in `inspect_schema` were
        // the others, until #1565 made those two REPORT it
        // (`discovery_incomplete` / `discovery_error`). This one is still
        // silent. It is a product pattern, not three accidents, and it is
        // filed as one defect. NOT fixed here: wiring `unavailable`/`reason`
        // is cheap but it is a change to the tool's contract, and this branch
        // corrects CLAIMS about behaviour rather than behaviour. The worker
        // guidance in `WORKER_INSTRUCTIONS_REWRITES` describes `top_values` as
        // best-effort instead of promising an error it does not raise.
        let top_values = if distinct > 0 && distinct <= PROFILE_TOP_VALUES_MAX as u64 {
            let q = format!(
                "SELECT CAST({col} AS {string_type}) AS v, COUNT(*) AS c FROM {} \
                 GROUP BY {col} ORDER BY c DESC, v LIMIT {}",
                prepared.table_ref, PROFILE_TOP_VALUES_MAX
            );
            match query_grounding(prepared.adapter.as_ref(), &q).await {
                Ok(r) => r
                    .rows
                    .into_iter()
                    .map(|row| ValueCount {
                        value: str_cell(row.first()),
                        count: row.get(1).map(as_u64).unwrap_or(0),
                    })
                    .collect(),
                Err(_) => Vec::new(),
            }
        } else {
            Vec::new()
        };

        Ok(Json(ProfileColumnResult {
            unavailable: false,
            reason: None,
            rows: total,
            nulls,
            null_rate,
            distinct,
            min: str_cell(row.get(3)),
            max: str_cell(row.get(4)),
            top_values,
        }))
    }

    /// Validate a draft model `name` and resolve its `models/<name>.sql` +
    /// sidecar paths, refusing any name that could escape the models directory.
    ///
    /// Mirrors the import-dbt `safe_join_under` path guard (the traversal fix
    /// that hardened untrusted `model-paths`): reject an absolute name or any
    /// path-traversal component syntactically. A draft name is a bare
    /// identifier, so a separator, `..`, or extension is refused. Then the
    /// two up-front halves of the no-follow guard run, before any snapshot or
    /// write: the models directory must resolve inside the project root
    /// ([`Self::refuse_redirected_models_dir`]), and each draft path must be
    /// absent or a regular file ([`refuse_non_regular_draft_target`]). What
    /// appears at a leaf AFTER these checks meets a no-follow open on both
    /// sides — the snapshot's read (`read_no_follow_bytes`) and every write
    /// and rollback (`write_no_follow`) — not a follow.
    fn resolve_draft_paths(&self, name: &str) -> Result<DraftPaths, Json<ToolError>> {
        use std::path::Component;

        let bad = |msg: String| {
            ToolError::invalid_argument(
                msg,
                "Pass a bare model name — a single identifier like \"completed_revenue\" — so it \
                 maps to exactly one models/<name>.sql draft under the project.",
            )
        };

        let stem = name.trim();
        if stem.is_empty() {
            return Err(bad("model name is empty".to_string()));
        }
        // A draft name is a single path segment with no extension: reject
        // separators, `..`, and `.` up front (syntactic, no filesystem access).
        if stem.contains('/') || stem.contains('\\') || stem.contains('.') {
            return Err(bad(format!(
                "model name '{stem}' must be a bare identifier: no path separators, '..', or \
                 extension (it becomes models/<name>.sql)"
            )));
        }
        // Belt-and-braces: the name must be exactly one normal path component.
        let mut comps = Path::new(stem).components();
        if !matches!(comps.next(), Some(Component::Normal(_))) || comps.next().is_some() {
            return Err(bad(format!(
                "model name '{stem}' is not a single path segment"
            )));
        }

        // The ancestor half of the no-follow guard: `models/` itself reached
        // through a link would redirect every draft write with no race at
        // all, and neither the leaf check below nor the no-follow writes look
        // above the leaf.
        self.refuse_redirected_models_dir()?;

        let sql_path = self.models_dir.join(format!("{stem}.sql"));
        let sidecar_path = self.models_dir.join(format!("{stem}.toml"));
        let contract_path = self.models_dir.join(format!("{stem}.contract.toml"));

        // The leaf half, up front: a symlink at any draft path — dangling or
        // not; `symlink_metadata` does not follow, where the `exists()` of the
        // canonicalize check this replaced did — or anything else that is not
        // a regular file refuses before a snapshot or a write. A link swapped
        // in AFTER this check meets a no-follow open on both sides: the
        // snapshot's read (`read_no_follow_bytes`) and every write and
        // rollback restore (`write_no_follow`), never a follow.
        for p in [&sql_path, &sidecar_path, &contract_path] {
            refuse_non_regular_draft_target(&self.root, p)?;
        }

        Ok(DraftPaths {
            stem: stem.to_string(),
            sql_path,
            sidecar_path,
            contract_path,
        })
    }

    /// Refuse to draft while `models/` resolves outside the project root.
    ///
    /// The leaf guard and the no-follow writes only look at the final path
    /// component, so `proj/models -> <outside>/models_real` redirected every
    /// draft write with no race at all. Both directories are canonicalized
    /// here and the models directory must sit under the root. A models
    /// directory that does not exist yet passes — `draft_model` creates it
    /// under the root just resolved — unless its name is taken by a dangling
    /// link. This is a check at one point in time: a link swapped into an
    /// ANCESTOR after it cannot be closed by path-based syscalls at all, only
    /// by dirfd-relative opens — the residual #1500 records.
    fn refuse_redirected_models_dir(&self) -> Result<(), Json<ToolError>> {
        let resolved_root = match self.root.canonicalize() {
            Ok(resolved) => resolved,
            // No root on disk means nothing under it either: there is nothing
            // to resolve yet, and `draft_model` creates the models directory
            // under the root as before.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(ToolError::internal(
                    format!(
                        "failed to resolve the project root {}: {e}",
                        self.root.display()
                    ),
                    "Ensure the project directory is readable, then retry.",
                ));
            }
        };
        let resolved_models = match self.models_dir.canonicalize() {
            Ok(resolved) => resolved,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return match std::fs::symlink_metadata(&self.models_dir) {
                    Ok(meta) if meta.file_type().is_symlink() => Err(ToolError::invalid_argument(
                        format!(
                            "the models directory {}/ is a symlink whose target does not \
                             exist; refusing to draft through it",
                            rel_display(&self.root, &self.models_dir)
                        ),
                        "Replace the link with a real models directory inside the project, \
                         then retry. Drafts are written only under the project root.",
                    )),
                    Ok(_) => Ok(()),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
                    Err(e) => Err(ToolError::internal(
                        format!(
                            "failed to inspect the models directory {}: {e}",
                            self.models_dir.display()
                        ),
                        "Ensure the project directory is readable, then retry.",
                    )),
                };
            }
            Err(e) => {
                return Err(ToolError::internal(
                    format!(
                        "failed to resolve the models directory {}: {e}",
                        self.models_dir.display()
                    ),
                    "Ensure the models directory is readable, then retry.",
                ));
            }
        };
        if !resolved_models.starts_with(&resolved_root) {
            return Err(ToolError::invalid_argument(
                format!(
                    "the models directory {}/ resolves to {}, outside the project root {}; \
                     refusing to draft through it",
                    rel_display(&self.root, &self.models_dir),
                    resolved_models.display(),
                    resolved_root.display()
                ),
                "Make models/ a real directory inside the project (or a link that stays inside \
                 it), then retry. Drafts are written only under the project root.",
            ));
        }
        Ok(())
    }

    /// Whether the model `stem` already has a source file under `models/`
    /// (`.sql` or `.rocky`). The write-path contract/check tools refuse to write
    /// a sidecar artifact for a model that does not exist — author the model
    /// first with `draft_model`.
    fn model_source_exists(&self, stem: &str) -> bool {
        self.models_dir.join(format!("{stem}.sql")).exists()
            || self.models_dir.join(format!("{stem}.rocky")).exists()
    }

    /// Consult the agent-policy plane for a `propose`-class authorship of `stem`,
    /// scoped to a stable `decision_id`. Mirrors the gate `draft_model` and the
    /// `propose` tool share (`evaluate_apply_policy`) so a write into a governed
    /// scope gets a structured verdict WITH the write. Absent a `[policy]` block
    /// this resolves to `NotConfigured` and behaviour is unchanged.
    ///
    /// `marker_freezes` is the durable freeze-marker set hoisted by the async
    /// tool body (via [`Self::draft_marker_freezes`]) — the evaluation itself
    /// is synchronous.
    fn evaluate_draft_policy(
        &self,
        stem: &str,
        decision_id: &str,
        marker_freezes: &[rocky_core::freeze_marker::ActiveMarkerFreeze],
    ) -> rocky_cli::commands::PolicyGate {
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                stem.to_string(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        rocky_cli::commands::evaluate_apply_policy(
            &self.config_path,
            decision_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &self.state_path(),
            marker_freezes,
        )
    }

    /// Durable freeze-marker LIST for a draft-class gate over `stem` — a
    /// frozen agent must not keep minting drafts, so the draft tools consult
    /// the same marker set the propose/apply gates enforce. Bounded by the
    /// shared gate guard (no `[policy]` ⇒ no LIST ⇒ zero behavior change; an
    /// unloadable config resolves to `PolicyGate::Unloadable`, which every
    /// draft gate refuses — it used to resolve to `NotConfigured` and read no
    /// markers, which is the fail-open #1559 fixed). Fail-closed on a transport
    /// failure, mirroring the governed apply seams.
    async fn draft_marker_freezes(
        &self,
        stem: &str,
    ) -> Result<
        Vec<rocky_core::freeze_marker::ActiveMarkerFreeze>,
        rmcp::handler::server::wrapper::Json<ToolError>,
    > {
        let Ok(cfg) = rocky_core::config::load_rocky_config(&self.config_path) else {
            return Ok(Vec::new());
        };
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                stem.to_string(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        rocky_cli::commands::marker_freezes_before_gate(&cfg, &touched)
            .await
            .map_err(|e| {
                ToolError::internal(
                    format!("failed to list durable freeze markers before the policy gate: {e:#}"),
                    "The durable `[state]` tier must be reachable so an active freeze marker is \
                     enforced before authoring into a governed scope (fail-closed).",
                )
            })
    }

    /// Compile the project scoped to `stem` and reduce it to the lite
    /// [`CompileResult`] the draft tools return inline. Shared by `draft_model`,
    /// `draft_contract`, and `draft_check` — the "compile with the write" step.
    fn compile_drafted(&self, stem: &str) -> Result<CompileResult, Json<ToolError>> {
        let with_seed = self.seed_file().is_some();
        let output = commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            Some(stem),
            false,
            None,
            with_seed,
            None,
        )
        .map_err(|e| ToolError::compile_failed(format!("{e:#}")))?;
        Ok(project_compile_result(&output))
    }

    #[tool(
        description = "Draft a Rocky transformation model into the project working tree and \
         compile it in the SAME call — the safe write path for an agent. Writes the SQL to \
         models/<name>.sql plus a sidecar carrying the intent, then compiles and returns the \
         diagnostics, so you get the type-check WITH the write (no separate round-trip). On an \
         EXISTING model it replaces the SQL body but PRESERVE-MERGES the sidecar: only `name` \
         and `intent` are replaced, every other key (classification, freshness, tests, target, \
         strategy, tags, ...) is kept — spec-owned metadata cannot be erased through this tool. \
         The merge re-serializes the sidecar, so TOML comments in an existing sidecar are lost; \
         an existing sidecar that does not parse as TOML refuses (never clobbered). It does \
         NOT run, apply, or touch the warehouse; a draft is inert until you `propose` it and a \
         human reviews it. Path-gated to the models directory (a name with separators/`..` is \
         refused) and policy-aware: authoring into a governed scope returns a structured \
         policy_denied / policy_review_required error, and a denied draft is not left on disk. \
         Use this instead of raw file writes so your edits flow through compile feedback + policy."
    )]
    async fn draft_model(
        &self,
        params: Parameters<DraftModelArgs>,
    ) -> ToolResult<DraftModelResult> {
        let args = params.0;
        let paths = self.resolve_draft_paths(&args.name)?;

        // A cold project may not have a models/ directory yet.
        std::fs::create_dir_all(&self.models_dir).map_err(|e| {
            ToolError::internal(
                format!("failed to create the models directory: {e}"),
                "Ensure the project directory is writable so drafts can be written.",
            )
        })?;

        // Snapshot prior on-disk state so a policy DENY (or a write failure, or
        // a panic anywhere before the verdict) rolls back to leave NO new
        // artifact — a draft the policy plane refuses must not linger on disk
        // (mirrors the propose gate's deny → no plan written). A drop-guard,
        // not a manual closure: unwinding restores too.
        //
        // An EXISTS-but-unreadable file REFUSES here, for the SQL and the
        // sidecar alike, mirroring the unparseable-sidecar refusal below. Read
        // as "absent", the draft would treat the model as NEW — overwriting
        // the sidecar's spec-owned metadata, evaluating policy with no prior
        // classifications, and, on a deny or a failed compile, "restoring"
        // the absent prior by DELETING the file. The sidecar used to have a
        // guard for that shape after the snapshot; the SQL file had none, so
        // the snapshot itself now refuses (#1572 follow-up).
        let rollback =
            DraftRollback::snapshot_async(vec![paths.sql_path.clone(), paths.sidecar_path.clone()])
                .await
                .map_err(|e| e.into_tool_error(&self.root))?;

        // FF-WP1 fix round (finding 2): build the sidecar to write, and
        // collect the PRIOR sidecar's classifications for the policy
        // pre-image/post-image dual evaluation below.
        //
        // - NO existing sidecar → the minimal `name` + `intent` document,
        //   exactly as before (target/strategy resolve from the project's
        //   conventions; the draft tool never invents routing).
        // - EXISTING sidecar → preserve-merge: parse it as TOML (an
        //   unparseable sidecar REFUSES — spec-owned metadata is never
        //   clobbered, mirroring draft_metadata), replace ONLY `name` and
        //   `intent`, and keep every other key (classification, freshness,
        //   tests, target, strategy, tags, ...).
        let (sidecar_bytes, prior_classifications): (String, Vec<String>) =
            match rollback.prior(&paths.sidecar_path) {
                None => (draft_sidecar(&paths.stem, args.intent.trim()), Vec::new()),
                Some(prior_bytes) => {
                    let text = std::str::from_utf8(prior_bytes).map_err(|_| {
                        ToolError::invalid_argument(
                            format!(
                                "the sidecar at {} is not valid UTF-8; refusing to rewrite it",
                                rel_display(&self.root, &paths.sidecar_path)
                            ),
                            "Fix the sidecar file by hand (it must be UTF-8 TOML), then retry. \
                             draft_model never overwrites a sidecar it cannot parse.",
                        )
                    })?;
                    let mut table: toml::Table = toml::from_str(text).map_err(|e| {
                        ToolError::invalid_argument(
                            format!(
                                "the sidecar at {} does not parse as TOML ({e}); refusing to \
                                 rewrite it",
                                rel_display(&self.root, &paths.sidecar_path)
                            ),
                            "Fix the sidecar so it parses (rocky compile will point at the same \
                             problem), then retry. draft_model never overwrites a sidecar it \
                             cannot parse — an existing model's metadata is preserved, not \
                             replaced.",
                        )
                    })?;
                    let prior_classifications: Vec<String> = table
                        .get("classification")
                        .and_then(|v| v.as_table())
                        .map(|t| {
                            t.values()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    table.insert("name".to_string(), toml::Value::String(paths.stem.clone()));
                    let intent = args.intent.trim();
                    if intent.is_empty() {
                        table.remove("intent");
                    } else {
                        table.insert(
                            "intent".to_string(),
                            toml::Value::String(intent.to_string()),
                        );
                    }
                    let serialized = toml::to_string(&table).map_err(|e| {
                        ToolError::internal(
                            format!("failed to re-serialize the merged sidecar: {e}"),
                            "Retry; if it persists this is an internal TOML serialization bug.",
                        )
                    })?;
                    (ensure_trailing_newline(&serialized), prior_classifications)
                }
            };

        // Write the draft: the SQL body verbatim + the sidecar built above.
        // No-follow at the leaf (`write_no_follow`): a link swapped in at a
        // draft path after `resolve_draft_paths` looked fails the write
        // instead of carrying it out of the models directory.
        let sql = ensure_trailing_newline(&args.sql);
        if let Err(e) = write_no_follow(&paths.sql_path, sql.as_bytes()) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft SQL to {}: {e}",
                    paths.sql_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }
        if let Err(e) = write_no_follow(&paths.sidecar_path, sidecar_bytes.as_bytes()) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft sidecar to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile immediately — the agent gets the type-check with the write.
        // Scope the returned diagnostics to the drafted model (the whole project
        // is still checked, so a fatal error anywhere surfaces).
        let with_seed = self.seed_file().is_some();
        let output = match commands::compile_output(
            Some(&self.config_path),
            &self.state_path(),
            &self.models_dir,
            None,
            Some(&paths.stem),
            false,
            None,
            with_seed,
            None,
        ) {
            Ok(o) => o,
            Err(e) => {
                return Err(ToolError::compile_failed(format!("{e:#}")));
            }
        };
        let compiled = project_compile_result(&output);

        // A draft is a `propose`-class authorship. Map the drafted model to the
        // `propose` capability and consult the SAME agent-policy plane the
        // propose/apply gates use (the shared `evaluate_apply_policy`) — so an
        // agent authoring into a governed scope gets a structured verdict WITH
        // the write, not later at apply. Absent a `[policy]` block this resolves
        // to `NotConfigured` and behaviour is byte-identical to no policy plane.
        // A config that EXISTS but fails to load is `Unloadable` instead, and
        // is refused — "no policy" and "could not read the policy" are
        // different answers, and only the first is permission (#1559).
        let state_path = self.state_path();
        let touched: std::collections::BTreeMap<String, rocky_core::config::PolicyCapability> =
            std::iter::once((
                paths.stem.clone(),
                rocky_core::config::PolicyCapability::Propose,
            ))
            .collect();
        // A draft has no plan; the decision is recorded against a draft-scoped id
        // so the audit ledger stays honest about what it is.
        let decision_id = format!("draft:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted here (the gate is synchronous) —
        // a frozen agent must not keep minting drafts. Fail-closed; bounded by
        // the shared guard (no `[policy]` ⇒ no LIST ⇒ zero behavior change).
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        // FF-WP1 fix round 2 (item 1): classification-sensitive scope is
        // DUAL-evaluated — once over the on-disk (post-merge) attributes and
        // once over the pre-image (the classifications the prior sidecar
        // carried), with the most restrictive verdict governing — so no edit
        // through this tool can de-scope a classification-matched rule NOR
        // escape an exclusion-matched one. Under the preserve-merge above
        // pre ⊆ post; the explicit dual evaluation keeps the property
        // STRUCTURAL rather than an artifact of the merge staying correct.
        let prior_classifications_by_model: std::collections::BTreeMap<String, Vec<String>> =
            std::iter::once((paths.stem.clone(), prior_classifications)).collect();
        let gate = rocky_cli::commands::evaluate_apply_policy_with_extra_classifications(
            &self.config_path,
            &decision_id,
            rocky_core::config::PolicyPrincipal::Agent,
            &touched,
            &self.models_dir,
            &state_path,
            &marker_freezes,
            &prior_classifications_by_model,
        );

        match gate {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). Rolled back EXPLICITLY — matching the `Deny`
            // arm below — so a failed cleanup is reported, never claimed
            // clean (#1561).
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "The draft was rolled back.",
                    "Rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). {disposition} Cause: {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                    rollback_failed_paths,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftModelResult {
                    model: paths.stem.clone(),
                    sql_path: rel_display(&self.root, &paths.sql_path),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: self.draft_model_next_steps().to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                // Mirrors the propose gate's require_review: the draft is the
                // reviewable artifact, so it PERSISTS; the structured signal
                // routes the agent to human review before it takes the change
                // further in this governed scope.
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring in this scope: model \
                         '{model}'{named} — {reason}. The draft was written to {} for a human to \
                         review.",
                        rel_display(&self.root, &paths.sql_path)
                    ),
                    "A human must review this draft before it goes further; do not plan, propose, \
                     or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                // A deny cannot be satisfied by review — the draft is rolled
                // back EXPLICITLY so NO artifact lingers on disk (the
                // decision is already in the ledger), consistent with the
                // propose gate's deny semantics; a failed cleanup is
                // reported, never claimed clean (#1561).
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "so the draft was not kept.",
                    "but rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "policy denies authoring this model: '{model}'{named} — {reason}. A deny \
                         cannot be satisfied by human review, {disposition}"
                    ),
                    "Re-scope the draft — author it under a different, ungoverned name, or drop \
                     it. A denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                    rollback_failed_paths,
                ))
            }
        }
    }

    #[tool(
        description = "Write an agent-authored data CONTRACT for an existing model into the \
         project working tree and compile-validate it in the SAME call — the safe write path for \
         a contract. Writes your `spec` verbatim to models/<model>.contract.toml (the sibling \
         compile auto-discovers), then compiles so the contract is checked against the model's \
         inferred schema and returns the diagnostics (a column the model doesn't produce comes \
         back as a `W010` diagnostic). It does NOT run, apply, or touch the warehouse. Path-gated \
         to the models directory and policy-aware: authoring into a governed scope returns a \
         structured policy_denied / policy_review_required error, and a denied draft leaves no \
         file. Omit `spec` and this returns an error pointing you at `ai_contract`, the LLM \
         generator that drafts a contract for you to pass here."
    )]
    async fn draft_contract(
        &self,
        params: Parameters<DraftContractArgs>,
    ) -> ToolResult<DraftContractResult> {
        let args = params.0;
        // Redirect a mis-dispatch: a call with no `spec` is someone expecting the
        // old generator. Point them at `ai_contract` in a single, actionable hop.
        let Some(spec) = args.spec else {
            return Err(ToolError::invalid_argument(
                "draft_contract writes an agent-authored contract; its `spec` (the \
                 `.contract.toml` body) is required and was not provided",
                "This is the write path: pass `spec` with the contract you authored and it is \
                 written + compile-validated + policy-gated. To GENERATE a contract from the \
                 target table's profile with an LLM, call the `ai_contract` tool instead.",
            ));
        };
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot so a policy DENY (or a write/compile failure, or a panic
        // before the verdict) rolls back to leave NO new artifact — mirrors
        // `draft_model` and the propose gate. Drop-guard: unwinding restores.
        let rollback = DraftRollback::snapshot_async(vec![paths.contract_path.clone()])
            .await
            .map_err(|e| e.into_tool_error(&self.root))?;

        let contract = ensure_trailing_newline(&spec);
        if let Err(e) = write_no_follow(&paths.contract_path, contract.as_bytes()) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft contract to {}: {e}",
                    paths.contract_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile with the write — the contract is validated against the model's
        // inferred schema. A hard compile failure rolls the draft back.
        let compiled = self.compile_drafted(&paths.stem)?;

        let decision_id = format!("draft-contract:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted in the async body (the gate is
        // synchronous). Fail-closed; no `[policy]` ⇒ no LIST.
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). Rolled back EXPLICITLY — matching the `Deny`
            // arm below — so a failed cleanup is reported, never claimed
            // clean (#1561).
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "The draft was rolled back.",
                    "Rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). {disposition} Cause: {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                    rollback_failed_paths,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftContractResult {
                    model: paths.stem.clone(),
                    contract_path: rel_display(&self.root, &paths.contract_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: DRAFT_CONTRACT_NEXT_STEPS.to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring a contract in this scope: \
                         model '{model}'{named} — {reason}. The contract was written to {} for a \
                         human to review.",
                        rel_display(&self.root, &paths.contract_path)
                    ),
                    "A human must review this contract before it goes further; do not plan, \
                     propose, or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "so the contract was not kept.",
                    "but rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "policy denies authoring a contract for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, {disposition}"
                    ),
                    "Re-scope — write the contract for a different, ungoverned model, or drop it. \
                     A denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                    rollback_failed_paths,
                ))
            }
        }
    }

    #[tool(
        description = "Write an agent-authored data-quality CHECK for an existing model into the \
         project working tree and compile-validate it in the SAME call — the safe write path for \
         a check. Appends your `spec` (one or more declarative `[[tests]]` blocks — not_null, \
         unique, accepted_values, relationships, expression, range, …) to the model's sidecar \
         (models/<model>.toml), then compiles so a malformed block fails structurally and returns \
         the diagnostics. The check EXECUTES via the `test` tool (compile proves structure; the \
         data-level assertion runs under `test`). It does NOT run, apply, or touch the warehouse. \
         Path-gated to the models directory and policy-aware: a governed scope returns a \
         structured policy_denied / policy_review_required error, and a denied draft restores the \
         prior sidecar. Omit `spec` and this returns an error pointing you at `ai_test`, the LLM \
         generator that drafts assertions for you to pass here."
    )]
    async fn draft_check(
        &self,
        params: Parameters<DraftCheckArgs>,
    ) -> ToolResult<DraftCheckResult> {
        let args = params.0;
        let Some(spec) = args.spec else {
            return Err(ToolError::invalid_argument(
                "draft_check writes an agent-authored check; its `spec` (one or more `[[tests]]` \
                 blocks) is required and was not provided",
                "This is the write path: pass `spec` with the `[[tests]]` check you authored and \
                 it is written + compile-validated + policy-gated. To GENERATE assertions from a \
                 model's intent and schema with an LLM, call the `ai_test` tool instead.",
            ));
        };
        // Guard against a spec that would attach to the sidecar's last table
        // (e.g. `[target]`) and corrupt it — a check is a `[[tests]]` block.
        if !spec.contains("[[tests]]") {
            return Err(ToolError::invalid_argument(
                "draft_check `spec` must contain one or more `[[tests]]` blocks",
                "Author the check as a declarative `[[tests]]` block, e.g.\n[[tests]]\ntype = \
                 \"not_null\"\ncolumn = \"id\"\nThen pass it as `spec`.",
            ));
        }
        // Structural gate: the spec parses as TOML and carries NOTHING but the
        // `tests` array-of-tables — a `[target]`/`[strategy]` override (or a
        // bare top-level key) smuggled alongside a valid `[[tests]]` block is
        // rejected instead of being appended verbatim into the model's sidecar.
        validate_check_spec(&spec)?;
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot the sidecar so a DENY (or a failure/panic before the
        // verdict) restores the model's PRIOR sidecar (the name/intent
        // draft_model wrote), never deletes it — the check is what rolls back,
        // not the model. A model with no sidecar yet snapshots None; a sidecar
        // that exists but cannot be read refuses.
        let rollback = DraftRollback::snapshot_async(vec![paths.sidecar_path.clone()])
            .await
            .map_err(|e| e.into_tool_error(&self.root))?;

        // Merge: append the `[[tests]]` block(s) to the existing sidecar, or seed
        // a minimal sidecar (`name = "<stem>"`) when the model is a bare `.sql`.
        let merged = match rollback.prior(&paths.sidecar_path) {
            Some(bytes) => {
                let prior_text = String::from_utf8_lossy(bytes);
                format!(
                    "{}\n\n{}",
                    prior_text.trim_end(),
                    spec.trim_start_matches('\n')
                )
            }
            None => format!("name = {}\n\n{}", toml_basic_string(&paths.stem), spec),
        };
        let merged = ensure_trailing_newline(&merged);
        if let Err(e) = write_no_follow(&paths.sidecar_path, merged.as_bytes()) {
            return Err(ToolError::internal(
                format!(
                    "failed to write draft check to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        let compiled = self.compile_drafted(&paths.stem)?;

        let decision_id = format!("draft-check:{}", paths.stem);
        // Durable freeze-marker LIST, hoisted in the async body (the gate is
        // synchronous). Fail-closed; no `[policy]` ⇒ no LIST.
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). Rolled back EXPLICITLY — matching the `Deny`
            // arm below — so a failed cleanup is reported, never claimed
            // clean (#1561).
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "The draft was rolled back.",
                    "Rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). {disposition} Cause: {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                    rollback_failed_paths,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftCheckResult {
                    model: paths.stem.clone(),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: self.draft_check_next_steps().to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring a check in this scope: \
                         model '{model}'{named} — {reason}. The check was written to {} for a \
                         human to review.",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "A human must review this check before it goes further; do not plan, propose, \
                     or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "so the check was not kept (the model's prior sidecar is restored).",
                    "but rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "policy denies authoring a check for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, {disposition}"
                    ),
                    "Re-scope — write the check for a different, ungoverned model, or drop it. A \
                     denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                    rollback_failed_paths,
                ))
            }
        }
    }

    #[tool(
        description = "Write governed sidecar METADATA for an existing model — freshness and/or \
         column classifications — as a structured patch, compile-validated in the SAME call. The \
         sidecar (models/<model>.toml) is parsed as TOML and re-serialized: `freshness` replaces \
         the whole [freshness] table, `classifications` merges per-column tags into \
         [classification] (other columns' tags are preserved). Comments in the sidecar are \
         dropped and key order may normalize on re-serialization; the data round-trips, the \
         formatting does not. An unparseable sidecar is never clobbered — the call fails naming \
         the file. At least one of `freshness` / `classifications` is required. Path-gated to the \
         models directory and policy-aware: the policy gate evaluates the model's attributes AS \
         PATCHED (a patch that first adds a governed classification is gated by that \
         classification), and a denied patch restores the prior sidecar bytes. It does NOT run, \
         apply, or touch the warehouse."
    )]
    async fn draft_metadata(
        &self,
        params: Parameters<DraftMetadataArgs>,
    ) -> ToolResult<DraftMetadataResult> {
        let args = params.0;
        if args.freshness.is_none() && args.classifications.is_none() {
            return Err(ToolError::invalid_argument(
                "draft_metadata needs at least one of `freshness` / `classifications`",
                "Pass `freshness` (expected_lag_seconds + optional time_column/severity), \
                 `classifications` (column -> tag map), or both. An empty patch writes nothing.",
            ));
        }
        // Validate the patch shape up front, before any filesystem access, so
        // a malformed patch is a crisp invalid_argument rather than a compile
        // diagnostic on a half-written sidecar.
        let freshness_table = match &args.freshness {
            Some(patch) => Some(build_freshness_table(patch)?),
            None => None,
        };
        if let Some(classifications) = &args.classifications {
            if classifications.is_empty() {
                return Err(ToolError::invalid_argument(
                    "draft_metadata `classifications` is present but empty",
                    "List at least one column -> tag pair (e.g. { \"email\": \"pii\" }), or omit \
                     the field.",
                ));
            }
            for (column, tag) in classifications {
                if column.trim().is_empty() || tag.trim().is_empty() {
                    return Err(ToolError::invalid_argument(
                        "draft_metadata classification columns and tags must be non-empty",
                        "Every entry maps a real column name to a non-empty tag, e.g. \
                         { \"email\": \"pii\" }.",
                    ));
                }
            }
        }
        let paths = self.resolve_draft_paths(&args.model)?;
        if !self.model_source_exists(&paths.stem) {
            return Err(ToolError::model_not_found(&paths.stem));
        }

        // Snapshot the sidecar so a DENY (or a write/compile failure, or a
        // panic before the verdict) restores the model's PRIOR sidecar bytes.
        // A sidecar that exists but cannot be read refuses.
        let rollback = DraftRollback::snapshot_async(vec![paths.sidecar_path.clone()])
            .await
            .map_err(|e| e.into_tool_error(&self.root))?;

        // Parse-merge, never string-append: the existing sidecar must parse as
        // TOML or the call fails naming it — an unparseable sidecar is never
        // clobbered (nothing has been written yet; the guard restores
        // identical bytes).
        let mut sidecar: toml::Table = match rollback.prior(&paths.sidecar_path) {
            Some(bytes) => {
                let text = std::str::from_utf8(bytes).map_err(|_| {
                    ToolError::invalid_argument(
                        format!(
                            "the sidecar at {} is not valid UTF-8; refusing to rewrite it",
                            rel_display(&self.root, &paths.sidecar_path)
                        ),
                        "Fix the sidecar file by hand (it must be UTF-8 TOML), then retry.",
                    )
                })?;
                toml::from_str(text).map_err(|e| {
                    ToolError::invalid_argument(
                        format!(
                            "the sidecar at {} does not parse as TOML ({e}); refusing to \
                             rewrite it",
                            rel_display(&self.root, &paths.sidecar_path)
                        ),
                        "Fix the sidecar so it parses (rocky compile will point at the same \
                         problem), then retry. draft_metadata never overwrites a file it \
                         cannot parse.",
                    )
                })?
            }
            None => {
                // A bare `.sql`/`.rocky` model with no sidecar yet: seed the
                // minimal sidecar `draft_check` also seeds.
                let mut table = toml::Table::new();
                table.insert("name".to_string(), toml::Value::String(paths.stem.clone()));
                table
            }
        };

        if let Some(fresh) = freshness_table {
            sidecar.insert("freshness".to_string(), toml::Value::Table(fresh));
        }
        if let Some(classifications) = &args.classifications {
            let entry = sidecar
                .entry("classification".to_string())
                .or_insert_with(|| toml::Value::Table(toml::Table::new()));
            let Some(class_table) = entry.as_table_mut() else {
                return Err(ToolError::invalid_argument(
                    format!(
                        "the sidecar at {} declares `classification` as a non-table value; \
                         refusing to rewrite it",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "Fix the sidecar so `[classification]` is a table of column = \"tag\" \
                     pairs, then retry.",
                ));
            };
            for (column, tag) in classifications {
                class_table.insert(column.clone(), toml::Value::String(tag.clone()));
            }
        }

        let serialized = toml::to_string(&sidecar).map_err(|e| {
            ToolError::internal(
                format!("failed to re-serialize the patched sidecar: {e}"),
                "Retry; if it persists this is an internal TOML serialization bug.",
            )
        })?;
        let serialized = ensure_trailing_newline(&serialized);
        if let Err(e) = write_no_follow(&paths.sidecar_path, serialized.as_bytes()) {
            return Err(ToolError::internal(
                format!(
                    "failed to write patched sidecar to {}: {e}",
                    paths.sidecar_path.display()
                ),
                "Ensure the models directory is writable.",
            ));
        }

        // Compile with the write — a hard failure rolls the patch back.
        let compiled = self.compile_drafted(&paths.stem)?;

        // ⟦RTL-2⟧ the policy gate runs AFTER the write, so the evaluation
        // compiles the model's attributes AS PATCHED from disk — a patch that
        // first ADDS a governed classification is gated by that
        // classification, not by the pre-patch attribute set.
        let decision_id = format!("draft-metadata:{}", paths.stem);
        let marker_freezes = self.draft_marker_freezes(&paths.stem).await?;
        match self.evaluate_draft_policy(&paths.stem, &decision_id, &marker_freezes) {
            // NOT grouped with NotConfigured. A config that failed to LOAD may
            // carry a `[policy]` block denying exactly this write; treating it
            // as "no policy configured" is what let a configured deny stop
            // denying (#1559). Rolled back EXPLICITLY — matching the `Deny`
            // arm below — so a failed cleanup is reported, never claimed
            // clean (#1561).
            rocky_cli::commands::PolicyGate::Unloadable { reason } => {
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "The draft was rolled back.",
                    "Rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). {disposition} Cause: {reason}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to author under a policy it cannot evaluate."
                        .to_string(),
                    None,
                    rollback_failed_paths,
                ))
            }
            rocky_cli::commands::PolicyGate::NotConfigured
            | rocky_cli::commands::PolicyGate::Allow => {
                rollback.defuse();
                Ok(Json(DraftMetadataResult {
                    model: paths.stem.clone(),
                    sidecar_path: rel_display(&self.root, &paths.sidecar_path),
                    has_errors: compiled.has_errors,
                    error_count: compiled.error_count,
                    warning_count: compiled.warning_count,
                    diagnostics: compiled.diagnostics,
                    next_steps: DRAFT_METADATA_NEXT_STEPS.to_string(),
                }))
            }
            rocky_cli::commands::PolicyGate::RequireReview {
                model,
                rule_id,
                reason,
            } => {
                rollback.defuse();
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                Err(ToolError::policy_review_required(
                    format!(
                        "policy requires human review before authoring metadata in this scope: \
                         model '{model}'{named} — {reason}. The patched sidecar was written to \
                         {} for a human to review.",
                        rel_display(&self.root, &paths.sidecar_path)
                    ),
                    "A human must review this metadata change before it goes further; do not \
                     plan, propose, or apply it in this governed scope on your own."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                ))
            }
            rocky_cli::commands::PolicyGate::Deny {
                model,
                rule_id,
                reason,
            } => {
                let named = rule_id.map(|r| format!(" (rule {r})")).unwrap_or_default();
                let (disposition, rollback_failed_paths) = rollback_disposition(
                    &self.root,
                    rollback,
                    "so the patch was not kept (the model's prior sidecar is restored).",
                    "but rolling it back FAILED",
                );
                Err(ToolError::policy_denied_after_rollback(
                    format!(
                        "policy denies authoring metadata for this model: '{model}'{named} — \
                         {reason}. A deny cannot be satisfied by human review, {disposition}"
                    ),
                    "Re-scope — patch a different, ungoverned model, or drop the change. A \
                     denied authorship cannot be applied even after review."
                        .to_string(),
                    rule_id.map(|r| r.to_string()),
                    rollback_failed_paths,
                ))
            }
        }
    }

    #[tool(
        description = "Propose materializing the model(s) as an AI-AUTHORED plan. This does NOT \
         execute anything. It records a plan that a human must review and approve \
         (`rocky review <plan_id> --approve`) before `rocky apply <plan_id>` will run it. Surface \
         the plan_id and the review/apply path to the user; never approve on their behalf. \
         Optionally binds the plan to a product identity (`product_id` + `spec_digest`, both or \
         neither): a product-bound plan additionally refuses a bare apply — the applier must pass \
         `rocky apply --expect-spec-digest <digest>`."
    )]
    async fn propose(&self, params: Parameters<ProposeArgs>) -> ToolResult<ProposeResult> {
        let args = params.0;
        // Product identity is all-or-nothing: exactly one of the pair is a
        // caller bug, and an empty string is not an identity. Validated before
        // any compile work so the refusal is immediate and structured.
        let product = match (args.product_id.as_deref(), args.spec_digest.as_deref()) {
            (Some(p), _) | (_, Some(p)) if p.trim().is_empty() => {
                return Err(ToolError::invalid_argument(
                    "product_id / spec_digest must be non-empty when present",
                    "Pass both fields with real values (e.g. product_id = \
                     \"product:revenue_daily\", spec_digest = \"sha256:<hex>\"), or omit both.",
                ));
            }
            (Some(_), None) | (None, Some(_)) => {
                return Err(ToolError::invalid_argument(
                    "product_id and spec_digest must be set together or not at all",
                    "Pass both fields (the plan binds to a product AND its approved spec \
                     revision), or omit both for a non-product plan.",
                ));
            }
            (Some(product_id), Some(spec_digest)) => {
                Some(rocky_cli::commands::fulfill_api::ProductBinding {
                    product_id: product_id.to_string(),
                    spec_digest: spec_digest.to_string(),
                })
            }
            (None, None) => None,
        };

        // The `propose` tool is the sole MCP writer of plans; it always
        // authors an AI-authored plan and therefore always acts as the
        // `agent` principal. The whole gate sequence — compile, plan build,
        // capability classification, deterministic id, authoritative ledger
        // sync, durable freeze markers, the policy gate, and the
        // deny-persists-nothing rule — lives in ONE shared helper
        // (`propose_governed_run_plan`), which the fulfillment loop also
        // drives; this tool only maps the typed outcome back onto its wire
        // envelopes (pinned byte-for-byte by the wire-parity goldens).
        let state_path = self.state_path();
        let outcome = rocky_cli::commands::propose_governed_run_plan(
            rocky_cli::commands::fulfill_api::ProposeRequest {
                root: &self.root,
                config_path: &self.config_path,
                models_dir: &self.models_dir,
                state_path: &state_path,
                model: args.model.clone(),
                product,
                idempotency_key: args.idempotency_key.clone(),
            },
        )
        .await;

        use rocky_cli::commands::fulfill_api::{ProposeError, ProposeOutcome};
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(ProposeError::Compile(inner)) => {
                return Err(ToolError::compile_failed(inner));
            }
            Err(ProposeError::EmptyProject) => {
                return Err(ToolError::empty_project(
                    "project has no compiled models to propose",
                ));
            }
            Err(ProposeError::ModelNotFound(model)) => {
                return Err(ToolError::model_not_found(&model));
            }
            Err(ProposeError::PlanId(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to compute plan id: {inner}"),
                    "Retry the propose; if it persists, verify the project compiles cleanly.",
                ));
            }
            Err(ProposeError::PolicyUnreadable(inner)) => {
                // policy_denied, not internal: the propose was REFUSED by the
                // policy plane's fail-closed rule, and the agent must be told
                // that plainly rather than reading it as a transient fault to
                // retry (#1559).
                return Err(ToolError::policy_denied(
                    format!(
                        "the project config failed to load, so any configured [policy] rules \
                         cannot be enforced (fail-closed). No plan was written. Cause: {inner}"
                    ),
                    "Fix the project config so its policy can be read, then retry. Rocky refuses \
                     to propose under a policy it cannot evaluate."
                        .to_string(),
                    None,
                ));
            }
            Err(ProposeError::LedgerDownload(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to download remote state before the policy gate: {inner}"),
                    "The remote [state] backend must be reachable so a cross-pod freeze is \
                     enforced before proposing a plan.",
                ));
            }
            Err(ProposeError::MarkerList(inner)) => {
                return Err(ToolError::internal(
                    format!(
                        "failed to list durable freeze markers before the policy gate: {inner}"
                    ),
                    "The durable `[state]` tier must be reachable so an active freeze marker \
                     is enforced before proposing a plan (fail-closed).",
                ));
            }
            Err(ProposeError::PlanWrite(inner)) => {
                return Err(ToolError::internal(
                    format!("failed to write AI-authored plan: {inner}"),
                    "Ensure the project directory is writable so the plan store can persist the \
                     plan.",
                ));
            }
        };

        match outcome {
            ProposeOutcome::Written {
                plan_id,
                models,
                product_id,
                spec_digest,
            } => Ok(Json(ProposeResult {
                plan_id,
                models,
                product_id,
                spec_digest,
            })),
            ProposeOutcome::ReviewRequired {
                plan_id,
                product_id,
                spec_digest,
                refusal,
            } => {
                // Headed to human review — the plan is recorded; return the
                // structured signal the agent parses. The recorded plan's id
                // (and its product binding, when the propose carried one)
                // ride as TYPED envelope fields — the machine handoff a
                // fulfillment runner branches on; the prose repeats them for
                // humans only.
                let named = refusal
                    .rule_id
                    .map(|r| format!(" (rule {r})"))
                    .unwrap_or_default();
                Err(ToolError::policy_review_required_for_plan(
                    format!(
                        "policy requires human review before this change can apply: \
                         model '{}'{named} — {}. The plan was recorded as {plan_id}.",
                        refusal.model, refusal.reason
                    ),
                    format!(
                        "A human must run `rocky review {plan_id} --approve` then \
                         `rocky apply {plan_id}`; never approve on the user's behalf."
                    ),
                    refusal.rule_id.map(|r| r.to_string()),
                    plan_id,
                    product_id,
                    spec_digest,
                ))
            }
            ProposeOutcome::Denied { refusal } => {
                // A deny cannot be satisfied by review — no plan was
                // recorded; the decision is already in the audit ledger.
                let named = refusal
                    .rule_id
                    .map(|r| format!(" (rule {r})"))
                    .unwrap_or_default();
                Err(ToolError::policy_denied(
                    format!(
                        "policy denies proposing this change: model '{}'{named} — {}. \
                         A deny cannot be satisfied by human review, so no plan was recorded.",
                        refusal.model, refusal.reason
                    ),
                    "Re-scope the change so it no longer touches the denied model — propose to a \
                     branch, or drop that model from the change. A denied change cannot be applied \
                     even after review."
                        .to_string(),
                    refusal.rule_id.map(|r| r.to_string()),
                ))
            }
        }
    }

    // ------------------------- governor tools ------------------------------
    // The human-oversight surface for an agent-operated estate — typed
    // projections of the same decision/run ledger the worker-agent tools write
    // to, so "what did agents do this week, and why was that apply allowed?" is
    // a cited, conversational query. `estate_brief` / `audit_query` /
    // `scorecard` are read-only; `review_queue` reads the pending queue on
    // every profile and — ONLY on `--profile approver`, and then only behind an
    // explicit `confirm` — writes the human sign-off marker (#1517). Every
    // projection reuses the shipped `brief` / `audit` / `review` cores, so a
    // section whose underlying query fails renders `unavailable` rather than a
    // smoothed-over narrative — the ledger grounds, no LLM narrates here.

    #[tool(
        description = "The governor's estate digest: agent activity by principal (proposals, \
         applies, denials with rule names), pending review escalations ranked, runs needing \
         attention, drift observed vs auto-handled, freshness/quality, cost + autonomy-budget \
         burn, and degraded/frozen rules — every line carrying a ledger citation \
         (run_id/plan_id/decision_ref). Template-first: a section whose query fails renders \
         `unavailable`, never a fabricated summary. `since` is `last` | `24h` | `7d` (default \
         `7d`); reads are side-effect-free (never advances the `--since last` cursor)."
    )]
    async fn estate_brief(
        &self,
        params: Parameters<EstateBriefArgs>,
    ) -> ToolResult<serde_json::Value> {
        let since = match params.0.since.as_deref().unwrap_or("7d") {
            "last" => commands::BriefSince::Last,
            "24h" => commands::BriefSince::Hours24,
            "7d" => commands::BriefSince::Days7,
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown since window '{other}'"),
                    "Pass one of: last, 24h, 7d.",
                ));
            }
        };
        let output = commands::compute_brief(
            &self.root,
            &self.state_path(),
            &self.config_path,
            since,
            chrono::Utc::now(),
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not read the state store to compose the digest; ensure the project's \
                 state store is present and readable.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the estate brief: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "Read-only scheduler snapshot: per-pipeline cron/after/freshness cursors, \
         last submission and outcome, consecutive failures, active claims, and tick-lock state. \
         Reports stored state only — it does NOT evaluate demand (side-effect free; \
         `rocky tick --dry-run` is the evaluation). `next_fire_at` in the past means an overdue \
         pipeline, not a future promise. Reads the project's canonical state path: a scheduler \
         started with an explicit `--state-path` override is not visible here — query that \
         server's GET /api/v1/schedule instead."
    )]
    async fn schedule_status(
        &self,
        Parameters(_args): Parameters<NoArgs>,
    ) -> ToolResult<serde_json::Value> {
        let config_path = self.config_path.clone();
        let state_path = self.state_path();
        // The SAME `.rocky` derivation the API route and the reconciler use,
        // so this snapshot reports against the tick lock a `serve --scheduler`
        // or a cron `rocky tick` actually holds.
        let rocky_dir = commands::scheduler::rocky_dir_for_config(&config_path);
        let output = tokio::task::spawn_blocking(move || {
            commands::schedule_status::schedule_status_output(
                &config_path,
                &state_path,
                &rocky_dir,
                chrono::Utc::now(),
            )
        })
        .await
        .map_err(|e| {
            ToolError::internal(
                format!("schedule status task failed: {e}"),
                "Retry; if it persists this is an internal join error.",
            )
        })?
        .map_err(|e| {
            ToolError::internal(
                // Top-level message only — the alternate chain carries absolute
                // project paths, which do not belong on the wire.
                format!("could not read the schedule state: {e}"),
                "Ensure the project config parses and the state store is readable. A project \
                 with no [schedule] blocks returns an empty snapshot, not an error.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the schedule snapshot: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "Pause a pipeline's schedule at runtime (MUTATING, safe-direction). Sets \
         a durable hold that suppresses every demand source — cron, after, freshness, webhook — \
         until resumed, recording a `paused` skip each tick. Requires `confirm: true`. Reaches a \
         RUNNING scheduler immediately (unlike a config edit, which a resident `serve \
         --scheduler` cannot see until restart). Resume is deliberately not exposed to agents: \
         a human runs `rocky state schedule resume <pipeline>`. Reads/writes the project's \
         canonical state path — a scheduler on an explicit `--state-path` override is not \
         reachable from this tool."
    )]
    async fn pause_schedule(
        &self,
        params: Parameters<PauseScheduleArgs>,
    ) -> ToolResult<serde_json::Value> {
        let args = params.0;
        if !args.confirm {
            return Err(ToolError::invalid_argument(
                "pause_schedule requires confirm: true".to_string(),
                "Pausing is a durable mutation: the pipeline stops firing until a human resumes \
                 it. Pass confirm: true to proceed.",
            ));
        }
        // Refuse unknown pipelines rather than writing a stray cursor: the
        // hold must attach to something the reconciler will actually consult.
        let config = rocky_core::config::load_rocky_config(&self.config_path).map_err(|e| {
            ToolError::internal(
                format!("could not load the project config: {e}"),
                "Fix the config parse error, then retry.",
            )
        })?;
        let known = config
            .pipelines
            .get(&args.pipeline)
            .map(|p| p.schedule().is_some())
            .unwrap_or(false);
        if !known {
            return Err(ToolError::invalid_argument(
                format!(
                    "pipeline '{}' has no [schedule] block (or does not exist)",
                    args.pipeline
                ),
                "Pass a pipeline name that carries a [schedule] block; see schedule_status for \
                 the scheduled set.",
            ));
        }
        let state_path = self.state_path();
        let pipeline = args.pipeline.clone();
        let changed = tokio::task::spawn_blocking(move || {
            let store = rocky_core::state::StateStore::open(&state_path)?;
            store.set_schedule_paused(&pipeline, true)
        })
        .await
        .map_err(|e| {
            ToolError::internal(
                format!("pause task failed: {e}"),
                "Retry; if it persists this is an internal join error.",
            )
        })?
        .map_err(|e| {
            ToolError::internal(
                format!("could not persist the pause: {e}"),
                "The state store may be held by a writer; retry shortly.",
            )
        })?;
        Ok(Json(serde_json::json!({
            "pipeline": args.pipeline,
            "paused": true,
            "changed": changed,
            // The acted-on store, so a wrong-instance pause can never be
            // silently "successful" — a scheduler is controlled by this hold
            // only if it reads the SAME state file.
            "state_path": self.state_path().display().to_string(),
            "resume": "rocky state schedule resume <pipeline> (human CLI)",
        })))
    }

    #[tool(
        description = "Trace the custody chain for a subject: a model name, a run id, or a \
         64-character plan id. Returns the one-query drill-down — who proposed it (principal), \
         what the policy plane decided (rule id + effect), what the plan changed (typed diff), \
         which runs materialized it, what post-apply verification found, and the downstream \
         blast radius. Each link fails closed: a link whose signal is genuinely not recorded \
         renders `unavailable` with a note rather than a fabricated value. Read-only."
    )]
    async fn audit_query(
        &self,
        params: Parameters<AuditQueryArgs>,
    ) -> ToolResult<serde_json::Value> {
        let subject = params.0.subject;
        if subject.trim().is_empty() {
            return Err(ToolError::invalid_argument(
                "subject is empty",
                "Pass a model name, a run id, or a 64-character plan id to trace.",
            ));
        }
        let output = commands::compute_audit_for(
            &self.root,
            &self.config_path,
            &self.state_path(),
            &self.models_dir,
            &subject,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not assemble the custody chain; ensure the project's state store is \
                 present and readable.",
            )
        })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the custody chain: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "The trust scorecard: a decisions-by-group aggregation over the policy \
         ledger — acceptance rate, denial rate, and require-review rate per group. `by` is \
         `principal` | `rule` | `scope` (default `principal`); `window` is `all` or a `<N>d` / \
         `<N>h` duration (e.g. `30d`, default all-time). Only metrics the ledger actually \
         persists are computed; verify-after / revert / escalation-latency metrics are declared \
         `unavailable` with a reason, never faked. This informs human judgment — nothing here is \
         wired to any automatic policy change. Read-only."
    )]
    async fn scorecard(&self, params: Parameters<ScorecardArgs>) -> ToolResult<serde_json::Value> {
        let by = match params.0.by.as_deref().unwrap_or("principal") {
            "principal" => rocky_cli::output::ScorecardDimension::Principal,
            "rule" => rocky_cli::output::ScorecardDimension::Rule,
            "scope" => rocky_cli::output::ScorecardDimension::Scope,
            other => {
                return Err(ToolError::invalid_argument(
                    format!("unknown scorecard dimension '{other}'"),
                    "Pass one of: principal, rule, scope.",
                ));
            }
        };
        // The only error path is a malformed `window` (a usage error); a ledger
        // read failure renders the scorecard `unavailable` inside the core.
        let output =
            commands::compute_audit_scorecard(&self.state_path(), by, params.0.window.as_deref())
                .map_err(|e| {
                ToolError::invalid_argument(
                    format!("{e:#}"),
                    "Pass `window` as 'all' or a '<N>d' / '<N>h' duration (e.g. 30d).",
                )
            })?;
        let value = serde_json::to_value(&output).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the scorecard: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(value))
    }

    #[tool(
        description = "The ranked pending-review queue, and an OPT-IN approve action. With no \
         `approve_plan_id`, lists every `require_review` escalation not yet signed off, ranked by \
         blast_radius × classification × staleness, each carrying its decision_ref, plan_id, and \
         `approve_command`. Listing works on every profile. APPROVING is different: it writes the \
         human sign-off marker that unblocks `rocky apply`, and MOST SERVERS DO NOT SERVE IT — it \
         is refused with `approve_not_enabled` unless the operator started this server as `rocky \
         mcp --profile approver`. Where it is served, `approve_plan_id` + `confirm=true` is still \
         refused unless the plan is actually in the pending queue AND `confirm` is set (the \
         require-review-grade confirmation stands in for explicit human intent). Policy applies to \
         the governor's agent too: the approval is attributed to the operator's git identity, not \
         a cryptographically bound principal (a signed human confirmation is a later step). Never \
         approve on the user's behalf; the normal path is the human running `rocky review \
         <plan_id> --approve` in their own terminal."
    )]
    async fn review_queue(
        &self,
        params: Parameters<ReviewQueueArgs>,
    ) -> ToolResult<ReviewQueueResult> {
        let args = params.0;
        let state_path = self.state_path();

        // #1517 — the availability gate, deliberately the FIRST thing an
        // approve meets. Ahead of the queue read so the refusal never depends
        // on the state store being healthy or on what the queue happens to
        // hold: on a profile that does not serve approving, the answer is the
        // same one every time, and it names the opt-in.
        if args.approve_plan_id.is_some() && !self.approve_action_served() {
            // Echo the caller's plan id, not a validated one — validating it
            // first would leak queue contents to a session that may not
            // approve, and the recovery text is identical either way.
            return Err(ToolError::approve_not_enabled(
                args.approve_plan_id.as_deref().unwrap_or_default(),
            ));
        }

        // Always compute the current queue first — it is both the read result
        // and the guard that an approve targets a genuinely pending escalation.
        let queue = commands::compute_review_queue(
            &self.root,
            &self.config_path,
            &state_path,
            &self.models_dir,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "Could not build the review queue; ensure the project's state store is present \
                 and readable.",
            )
        })?;

        let Some(plan_id) = args.approve_plan_id.as_deref() else {
            // Read mode, optionally product-filtered.
            let (pending, total) = match args.product_id.as_deref() {
                None => {
                    let pending = serde_json::to_value(&queue.pending).map_err(|e| {
                        ToolError::internal(
                            format!("failed to serialize the review queue: {e}"),
                            "Retry; if it persists this is an internal serialization bug.",
                        )
                    })?;
                    (pending, queue.total)
                }
                Some(product) => {
                    if product.trim().is_empty() {
                        return Err(ToolError::invalid_argument(
                            "review_queue `product_id` must be non-empty when present",
                            "Pass the product identity to filter on (e.g. \
                             \"product:revenue_daily\"), or omit the field for the full queue.",
                        ));
                    }
                    filter_pending_by_product(&self.root, &queue.pending, product)?
                }
            };
            return Ok(Json(ReviewQueueResult {
                total,
                ranking: queue.ranking,
                pending,
                approval: None,
            }));
        };

        if args.product_id.is_some() {
            return Err(ToolError::invalid_argument(
                "`product_id` filters the queue LISTING; it cannot combine with \
                 `approve_plan_id`",
                "Approve by exact plan_id alone — list with the product filter first, then \
                 approve the specific plan.",
            ));
        }

        // The plan must be an outstanding escalation in THIS queue — not an
        // arbitrary reviewable plan.
        if !queue.pending.iter().any(|e| e.plan_id == plan_id) {
            return Err(ToolError::invalid_argument(
                format!("plan '{plan_id}' is not in the pending review queue"),
                "Call review_queue with no approve_plan_id to see the plan_ids currently awaiting \
                 review, then approve one of those.",
            ));
        }

        // The gate: approving writes a human sign-off marker, so it requires an
        // explicit, require-review-grade confirmation.
        if !args.confirm {
            return Err(ToolError::policy_review_required(
                format!(
                    "approving '{plan_id}' writes a human sign-off marker that unblocks \
                     `rocky apply`; it requires explicit confirmation."
                ),
                "Re-call review_queue with confirm=true ONLY when the human has explicitly \
                 authorized approving this exact plan. The approval is attributed to the \
                 operator's git identity — never approve on the user's behalf.",
                None,
            ));
        }

        // Write the sign-off marker (the artifact `rocky apply` checks),
        // attributed to the operator running this server. Reuses the exact
        // `rocky review --approve` core; the breaking-change gate is best-effort
        // and the marker writes regardless.
        let review = commands::compute_review(&self.root, &self.config_path, plan_id, "HEAD", true)
            .await
            .map_err(|e| {
                ToolError::internal(
                    format!("{e:#}"),
                    "Confirm the plan is an AI-authored or agent-authored plan and the project \
                     directory is writable so the sign-off marker can be persisted.",
                )
            })?;

        let breaking_change_count = review
            .breaking_changes
            .as_ref()
            .map(|f| f.iter().filter(|x| x.is_breaking()).count() as u64)
            .unwrap_or(0);
        let approval = ReviewApprovalOutcome {
            plan_id: plan_id.to_string(),
            marker_written: review.marker_written,
            breaking_change_count,
            message: review.message.unwrap_or_default(),
            attribution: "Recorded via the governor MCP surface and attributed to the operator's \
                 git identity (name/email/host), not a cryptographically bound principal. A signed \
                 human confirmation is a later step; the confirm flag stands in for explicit human \
                 intent today."
                .to_string(),
        };

        // Re-list the queue post-approval so the caller sees this escalation
        // cleared by the marker just written.
        let queue_after = commands::compute_review_queue(
            &self.root,
            &self.config_path,
            &state_path,
            &self.models_dir,
        )
        .map_err(|e| {
            ToolError::internal(
                format!("{e:#}"),
                "The sign-off marker was written, but re-listing the queue failed; re-call \
                 review_queue to see the current state.",
            )
        })?;
        let pending = serde_json::to_value(&queue_after.pending).map_err(|e| {
            ToolError::internal(
                format!("failed to serialize the review queue: {e}"),
                "Retry; if it persists this is an internal serialization bug.",
            )
        })?;
        Ok(Json(ReviewQueueResult {
            total: queue_after.total,
            ranking: queue_after.ranking,
            pending,
            approval: Some(approval),
        }))
    }

    /// Resolve the project's target warehouse adapter from `rocky.toml`.
    ///
    /// Returns the configured target adapter for the resolved pipeline — any
    /// warehouse (DuckDB, Snowflake, BigQuery, Databricks, Trino). The data
    /// grounding tools (`sample_rows`, `profile_column`, and `inspect_schema`'s
    /// source discovery) reach the live warehouse through it.
    ///
    /// The `Result<Option<...>>` shape is historical: this never returns
    /// `Ok(None)` today. It used to justify `inspect_schema` discarding the
    /// `Err` arm with `if let Ok(Some(_))`, which is what made a resolution
    /// failure look like an empty warehouse (#1533). Callers must now handle
    /// every arm.
    ///
    /// `prepare_table_query` — the path `sample_rows` and `profile_column`
    /// take — propagates a resolution failure. Read that as a claim about
    /// `prepare_table_query`, not about those two tools end to end:
    /// `profile_column` runs a SECOND query after it, and that one takes
    /// `Err(_) => Vec::new()` before the tool returns success (see the note
    /// at its `top_values` block). Spelled out because this sentence sits one
    /// clause from the swallow it contrasts with, and the same over-reading
    /// is what round thirteen came back for.
    fn warehouse_adapter(
        &self,
    ) -> anyhow::Result<Option<std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>>> {
        let cfg = rocky_core::config::load_rocky_config(&self.config_path)?;
        let registry = commands_adapter_registry(&cfg)?;
        let (_, pipeline) = rocky_cli::registry::resolve_pipeline(&cfg, None)?;
        let target_adapter = pipeline.target_adapter().to_string();
        Ok(Some(registry.warehouse_adapter(&target_adapter)?))
    }

    /// Resolve a grounding-tool target into a runnable, validated table ref plus
    /// the warehouse adapter.
    ///
    /// The target is either a **compiled model name** (resolved to its target
    /// table, which requires the models to compile) or a **qualified
    /// `schema.table` / `catalog.schema.table` source reference** (any dotted
    /// name — resolved directly with no compile, so it reaches raw sources the
    /// project never declared and works at cold start, before any model exists).
    async fn prepare_table_query(&self, target: &str) -> anyhow::Result<Prepared> {
        let adapter = self
            .warehouse_adapter()?
            .ok_or_else(|| anyhow::anyhow!("could not resolve the target warehouse adapter"))?;

        let dialect = adapter.dialect();
        let table_ref = if target.contains('.') {
            // Qualified raw reference — validate each segment and quote it
            // dialect-correctly. No compile required: this is how an agent
            // grounds a source before (or without) authoring any model. The
            // dialect decides validation + quoting (e.g. BigQuery allows a
            // hyphenated project segment and backtick-quotes the ref).
            let parts: Vec<&str> = target.split('.').collect();
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid table reference '{target}': {e}"))?
        } else {
            // Bare name — resolve the model's target coordinates by compiling
            // the models dir. Emit `catalog.schema.table` when the target
            // carries a catalog (Snowflake/BigQuery/Databricks); DuckDB has no
            // catalog level so it stays a two-part `schema.table` name.
            let result = self.compile_full()?;
            let model = result
                .project
                .models
                .iter()
                .find(|m| m.config.name == target)
                .ok_or_else(|| anyhow::anyhow!("model '{target}' not found in project"))?;
            let t = &model.config.target;
            let parts: Vec<&str> = if t.catalog.is_empty() {
                vec![&t.schema, &t.table]
            } else {
                vec![&t.catalog, &t.schema, &t.table]
            };
            dialect
                .ground_table_ref(&parts)
                .map_err(|e| anyhow::anyhow!("invalid model target reference: {e}"))?
        };

        Ok(Prepared { adapter, table_ref })
    }
}

// `prompt_router`'s `router` arg takes a string ident (unlike `tool_router`);
// the default generated fn is already named `prompt_router`, so no arg needed.
#[prompt_router]
impl RockyMcpServer {
    /// The actionable, intent-parameterized form of the server `instructions`
    /// (the `rocky-ai-workflow` skill). Walks a connected agent through
    /// Rocky's authoring loop for one concrete model, ending at *propose* —
    /// the human runs `rocky review --approve` + `rocky apply`.
    #[prompt(
        name = "build_model",
        description = "Guide the authoring of one Rocky model from a plain-language intent: \
         inspect schema -> sample rows -> profile columns -> write SQL -> compile-loop -> \
         plan preview -> propose. Stops at the human approval gate."
    )]
    async fn build_model(
        &self,
        Parameters(args): Parameters<BuildModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let intent = args.intent.trim();
        // FF-WP1 fix round (finding 7): the worker profile serves a variant
        // that ends at the handoff to the trusted runner — it never instructs
        // `propose`, contract authorship, or any tool the profile excludes.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll author this Rocky model SQL-first, grounding every decision in the \
                     real data, and end with a clean, tested draft handed off to the trusted \
                     runner. I draft; the runner reviews and applies.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Build a Rocky model for this intent:\n\n  {intent}\n\n\
                         Follow Rocky's authoring loop, using the MCP tools at each step:\n\n\
                         1. inspect_schema — read the project's models and source tables with \
                         their typed columns. Never guess column names; select only what it \
                         shows. Its physical warehouse tables are best-effort: when the result's \
                         `discovery_incomplete` is true, a table missing from `sources` is \
                         inconclusive, not absent. Ask sample_rows for that table before you \
                         conclude it does not exist.\n\
                         2. sample_rows — look at real rows before writing any filter or cast. \
                         The schema tells you a column exists; it does not tell you its literal \
                         values, its units, or its null rate.\n\
                         3. profile_column — for any column you filter, cast, or aggregate on, \
                         check distinct values, null rate, and domain.\n\
                         4. draft_model — write the model as raw SQL. SQL is first-class in \
                         Rocky — do NOT reach for the .rocky DSL unless explicitly asked. The \
                         draft compiles in the same call; on an existing model it preserves \
                         the sidecar's spec-owned metadata.\n\
                         5. compile — read the diagnostics (each carries a code, a span, and \
                         often a suggestion), fix against them, and loop until clean.\n\
                         6. plan_preview — read the SQL Rocky generates offline and confirm \
                         it matches the intent. It is not the whole plan: any model it \
                         cannot render offline is skipped, and the result does not name it. \
                         A model missing from the statements means 'not renderable offline', \
                         never 'nothing to do'.\n\
                         7. test — run the project's LOCAL tests (the compiled model tests \
                         and unit tests). That is the only suite you can run here. The \
                         checks the product spec declares — its grain, its not-null columns, \
                         its `checks` list — are lowered into the sidecar for you and need \
                         the applied table to run against, so they are deferred until after \
                         the apply and cannot pass or fail during drafting. They are \
                         SPEC-OWNED, and so are the contract and the model metadata: do not \
                         author any of them. Report an assertion you believe is missing in \
                         your handoff instead.\n\n\
                         RECONCILE DISCIPLINE (the step that separates a model that compiles \
                         from a model that is correct): check literal values and units against \
                         the sampled data, not just the schema. A `WHERE status = 'completed'` \
                         that returns zero rows because the data actually holds 'COMPLETE' \
                         compiles perfectly and is wrong.\n\n\
                         STOP when the draft compiles clean and the local tests pass, and \
                         HAND OFF to the trusted runner: report the drafted files, what you \
                         verified in the data, and anything you flagged. Do not record plans, \
                         approve changes, or apply anything on your own — those verbs belong \
                         to the trusted runner and are not served in this profile."
                    ),
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "Rocky model-drafting loop (worker profile, ends at the runner handoff) for: \
                 {intent}"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll author this Rocky model SQL-first, grounding every decision in the \
                 real data, and stop at a proposed plan for you to review and apply. \
                 The substrate trusts my edits because the compiler checked them and you \
                 sign off the invariants — never because they merely compiled.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Build a Rocky model for this intent:\n\n  {intent}\n\n\
                     Follow Rocky's authoring loop, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read the project's models and source tables with their \
                     typed columns. Never guess column names; select only what it shows. Its \
                     physical warehouse tables are best-effort: when the result's \
                     `discovery_incomplete` is true, a table missing from `sources` is \
                     inconclusive, not absent. Ask sample_rows for that table before you conclude \
                     it does not exist.\n\
                     2. sample_rows — look at real rows before writing any filter or cast. The \
                     schema tells you a column exists; it does not tell you its literal values, \
                     its units, or its null rate.\n\
                     3. profile_column — for any column you filter, cast, or aggregate on, \
                     check distinct values, null rate, and domain.\n\
                     4. Write the model as raw SQL (models/<name>.sql + a <name>.toml sidecar \
                     for strategy + target). SQL is first-class in Rocky — do NOT reach for the \
                     .rocky DSL unless the user explicitly asks. Keep it minimal and readable.\n\
                     5. compile — type-check and read the diagnostics. Each carries a code, a \
                     span, and often a suggestion. Fix against the diagnostic and recompile; \
                     loop until clean. The compiler is your fast feedback loop — lean on it \
                     instead of reasoning about correctness in your head.\n\
                     6. plan_preview — read the SQL Rocky generates offline and confirm it \
                     matches the intent before proposing. It is not the whole plan: any model \
                     it cannot render offline is skipped, and the result does not name it. A \
                     model missing from the statements means 'not renderable offline', never \
                     'nothing to do'.\n\
                     7. Encode what you learned while sampling as a contract (required/protected \
                     columns) or a check (assertion), not just a WHERE clause — that moves the \
                     invariant into the typed substrate so the compiler enforces it on every \
                     future run.\n\
                     8. propose — generate the materialization plan. It is recorded as an \
                     AI-authored plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE (the step that separates a model that compiles from a \
                     model that is correct): check literal values and units against the sampled \
                     data, not just the schema. A `WHERE status = 'completed'` that returns zero \
                     rows because the data actually holds 'COMPLETE' compiles perfectly and is \
                     wrong. Confirm dollars-vs-cents and UTC-vs-local from real rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply \
                     is refused by design. Surface the plan_id and the review report clearly, \
                     then the human runs `rocky review <plan-id> --approve` to sign off the \
                     invariants and `rocky apply <plan-id>` to execute. Do not approve on the \
                     user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Rocky model-authoring loop for: {intent}")))
    }

    /// Sweep the project for models with no declarative tests and draft tests
    /// for them. Orchestrates the read-only catalog + generator tools and stops
    /// at *propose* — never applies.
    #[prompt(
        name = "find_untested_models",
        description = "Find models with no declarative tests and draft tests for them: catalog \
         -> identify untested models -> ai_test / ai_contract -> draft_check / draft_contract -> \
         propose. Stops at the human approval gate."
    )]
    async fn find_untested_models(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        // FF-WP1 fix round (finding 7): the worker variant drafts the checks
        // itself (no LLM generator tools, no contract authorship — both are
        // outside the profile) and ends at the handoff to the trusted runner.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll find the models that carry no declarative tests and say exactly \
                     what each one needs, grounded in its real data. Checks are spec-owned \
                     here, so I report; I do not write them.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    "Find the untested models in this Rocky project and REPORT what each one \
                     needs, using the MCP tools at each step:\n\n\
                     1. catalog — enumerate every model with its declared tests, checks, and \
                     contract. Treat a model with no checks, no contract, and no test files as \
                     untested. Prioritise leaf/marts models and anything carrying a primary key \
                     or a grain you can name.\n\
                     2. For each untested model, ground before you assert: sample_rows to see \
                     real values, and profile_column on any key, status, or amount column to \
                     learn its null rate, distinct count, and domain. The schema says a column \
                     exists; only the data tells you whether it is unique, non-null, or \
                     bounded.\n\
                     3. Write down, per model, the assertion the data supports — grain \
                     uniqueness, not-null, value ranges, referential integrity — and the \
                     numbers you saw. Do NOT write any of it into the project. Checks, \
                     contracts, and model metadata are all SPEC-OWNED in this profile: they \
                     come from the product spec, and an assertion nobody approved would run \
                     unattended against the warehouse after every apply.\n\
                     4. Use the `test` tool to run the project's LOCAL tests, so your report \
                     says whether the project is green as it stands today.\n\n\
                     RECONCILE DISCIPLINE: an invariant you name that is wrong is worse than \
                     none — someone will approve it. Confirm the grain, the not-null columns, \
                     and the value domain against the sampled data before you name them; do \
                     not assume `id` is unique or `status` is non-null without checking.\n\n\
                     STOP when the report is complete, and HAND OFF to the trusted runner: \
                     name the models you covered, the assertion each one needs with the \
                     evidence behind it, and anything you flagged as contract-shaped. Do not \
                     record plans, approve changes, or apply anything on your own — those \
                     verbs belong to the trusted runner and are not served in this profile.",
                ),
            ];
            // NINTH ROUND, finding 2. This said "draft tests" while the
            // body above it is report-only — the description promised a
            // write the prompt withholds. It names no excluded tool, so
            // the name-based sweep read it as clean, and the sweep did not
            // read this field at all.
            return Ok(GetPromptResult::new(messages).with_description(
                "Find untested Rocky models and REPORT the assertions each one needs \
                 (worker profile, ends at the runner handoff)",
            ));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll find the models that carry no declarative tests, draft tests grounded in \
                 their real data, and stop at a proposed plan for you to review and apply. A model \
                 that compiles is not the same as a model that is checked — tests are what make \
                 the substrate trust a future run.",
            ),
            PromptMessage::new_text(
                Role::User,
                "Find the untested models in this Rocky project and draft tests for them, using \
                 the MCP tools at each step:\n\n\
                 1. catalog — enumerate every model with its declared tests, checks, and \
                 contract. Treat a model with no checks, no contract, and no test files as \
                 untested. Prioritise leaf/marts models and anything carrying a primary key or a \
                 grain you can name.\n\
                 2. For each untested model, ground before you assert: sample_rows to see real \
                 values, and profile_column on any key, status, or amount column to learn its null \
                 rate, distinct count, and domain. The schema says a column exists; only the data \
                 tells you whether it is unique, non-null, or bounded.\n\
                 3. Draft the checks. For a data-quality assertion (not-null, grain uniqueness, \
                 value ranges, referential integrity), call ai_test to have an LLM draft it from \
                 what you observed, then write it with draft_check — it appends the `[[tests]]` \
                 block to the model and compiles in the same call. For invariants better expressed \
                 as required/protected columns, call ai_contract to draft the contract, then write \
                 it with draft_contract. Both write tools compile-validate and policy-gate the \
                 write; you can also author the check/contract yourself and pass it straight to \
                 the write tool.\n\
                 4. compile — the write tools already type-check; run the new checks via the \
                 `test` tool. Fix against any diagnostic and re-run until clean.\n\
                 5. propose — generate the plan that records the new tests/contracts. It is an \
                 AI-authored plan with a plan_id.\n\n\
                 RECONCILE DISCIPLINE: a test that asserts the wrong invariant passes and is still \
                 wrong. Confirm the grain, the not-null columns, and the value domain against the \
                 sampled data before you encode them — do not assume `id` is unique or `status` is \
                 non-null without checking.\n\n\
                 STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                 refused by design. Surface the plan_id and the review report, then the human runs \
                 `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not approve on \
                 the user's behalf unless they explicitly tell you to.",
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(
            "Find untested Rocky models and draft tests, stopping at the approval gate",
        ))
    }

    /// Add uniqueness + not-null tests to a model's primary-key / unique
    /// columns. Inspects the schema, identifies the key columns, drafts tests,
    /// and stops at *propose*.
    #[prompt(
        name = "add_tests_to_pks",
        description = "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
         inspect_schema -> identify key columns -> ai_test / author the checks -> draft_check -> \
         propose. Stops at the human approval gate."
    )]
    async fn add_tests_to_pks(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "every model".to_string(),
        };
        // FF-WP1 fix round (finding 7): the worker variant authors the checks
        // itself (no `ai_test`, no `propose` — both outside the profile) and
        // ends at the handoff to the trusted runner.
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll identify the primary-key and unique columns and prove them against \
                     the real data, then report the uniqueness and not-null assertions they \
                     need. Checks are spec-owned here, so I report; I do not write them. A \
                     declared key is a claim; the data is what proves it.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Identify the key columns of {scope} in this Rocky project and REPORT \
                         the uniqueness + not-null assertions they need, using the MCP tools \
                         at each step:\n\n\
                         1. inspect_schema — read the typed columns. Identify the primary-key / \
                         unique / grain columns: an explicit key in the sidecar, an `id`-shaped \
                         column, or the columns that define the model's grain.\n\
                         2. profile_column — for each candidate key column, confirm it is \
                         actually unique (distinct count == row count) and non-null before you \
                         assert it. A column named `id` that has duplicates or nulls is not a \
                         key — find that out now, from the data.\n\
                         3. Write down the uniqueness and not-null assertion each confirmed \
                         key column needs, with the distinct count, row count, and null count \
                         you measured. Do NOT write any of it into the project: checks are \
                         SPEC-OWNED in this profile, they come from the product spec, and an \
                         assertion nobody approved would run unattended against the warehouse \
                         after every apply.\n\
                         4. Use the `test` tool to run the project's LOCAL tests, so your \
                         report says whether the project is green as it stands today.\n\n\
                         RECONCILE DISCIPLINE: only name uniqueness/not-null on columns the \
                         profile actually shows to be unique/non-null. Naming a wrong key \
                         invariant is worse than naming none — someone will approve it, and \
                         it green-lights a future run that should have failed.\n\n\
                         STOP when the report is complete, and HAND OFF to the trusted \
                         runner: report the key columns you confirmed, the evidence behind \
                         each one, and the assertions they need. Do not record plans, approve \
                         changes, or apply anything on your own — those verbs belong to the \
                         trusted runner and are not served in this profile."
                    ),
                ),
            ];
            // NINTH ROUND, finding 2 — the sibling of the one on
            // `find_untested_models`. "Add key tests to X" promised the
            // write; the body reports the assertions and stops.
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "REPORT the key assertions {scope} needs (worker profile, ends at the \
                 runner handoff)"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll identify the primary-key and unique columns, draft uniqueness and not-null \
                 tests grounded in the real data, and stop at a proposed plan for you to review \
                 and apply. A declared key is a claim; the data is what proves it.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Add uniqueness + not-null tests to the key columns of {scope} in this Rocky \
                     project, using the MCP tools at each step:\n\n\
                     1. inspect_schema — read the typed columns. Identify the primary-key / unique \
                     / grain columns: an explicit key in the sidecar, an `id`-shaped column, or \
                     the columns that define the model's grain.\n\
                     2. profile_column — for each candidate key column, confirm it is actually \
                     unique (distinct count == row count) and non-null before you assert it. A \
                     column named `id` that has duplicates or nulls is not a key, and a test that \
                     claims it is will fail on the next run — find that out now, from the data.\n\
                     3. Draft a uniqueness check and a not-null check for each confirmed key \
                     column (each `[[tests]]` block passes when the invariant holds). Author them \
                     directly, or call ai_test to draft them, then write them with draft_check — \
                     it merges the `[[tests]]` blocks into the model and compiles in the same \
                     call, policy-gated.\n\
                     4. run the new checks via the `test` tool. Loop until clean.\n\
                     5. propose — generate the plan recording the new tests. It is an AI-authored \
                     plan with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: only assert uniqueness/not-null on columns the profile \
                     actually shows to be unique/non-null. Encoding a wrong key invariant is worse \
                     than none — it green-lights a future run that should have failed.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages).with_description(format!(
            "Add uniqueness + not-null tests to the keys of {scope}"
        )))
    }

    /// Produce a structured, read-only summary of the project from the catalog
    /// and lineage. No edits, no propose — purely informational.
    #[prompt(
        name = "summarize_project",
        description = "Produce a structured, read-only summary of the Rocky project: catalog + \
         lineage -> grouped overview of models, their grain, governance, tests, and DAG shape. \
         Read-only — no edits, no propose."
    )]
    async fn summarize_project(
        &self,
        Parameters(_args): Parameters<NoArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll summarize this Rocky project from the catalog and lineage. This is a \
                 read-only orientation — I will not edit anything, record a plan, or apply \
                 anything.",
            ),
            PromptMessage::new_text(
                Role::User,
                "Summarize this Rocky project, using only the read-only MCP tools:\n\n\
                 1. catalog — enumerate every model with its target table, materialization \
                 strategy, declared tests/checks, contract, and governance (classification / mask \
                 / retention).\n\
                 2. lineage — for the key models (sources, marts/leaf models), trace upstream \
                 dependencies to understand the DAG shape and how data flows.\n\
                 3. Group the result into a structured summary: sources and raw inputs; \
                 intermediate transforms; marts / leaf outputs. For each, note its grain (one row \
                 per what?), its materialization strategy, whether it carries tests / a contract / \
                 governance, and its place in the DAG.\n\
                 4. Call out gaps an owner would care about: untested leaf models, PII columns \
                 with no mask, models with no contract, or long undocumented dependency chains. \
                 Frame these as observations, not actions.\n\n\
                 This is purely informational — do NOT write SQL, draft tests, record a plan, or \
                 apply anything. If the user then wants to act on a gap, the find_untested_models \
                 or build_model trajectory is the next step.",
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description("Read-only structured summary of the Rocky project"))
    }

    /// Diagnose and fix failing LOCAL tests: run the tests, ground each
    /// failure with profile_column, propose a fix. Stops at *propose*.
    ///
    /// FOURTEENTH ROUND — "declarative" was wrong here, and round ten fixed
    /// it on the worker surface ONLY. The `test` tool calls
    /// `commands::test_output` on EVERY profile, which runs
    /// `test_runner::run_tests` (model execution) plus
    /// `test_runner::run_unit_tests` (sidecar fixture `[[test]]` blocks). It
    /// never calls `run_declarative_tests` — the `rocky test --declarative`
    /// path that evaluates sidecar `[[tests]]` against the WAREHOUSE. No
    /// profile reaches it, so this was never a profile-shaped defect and
    /// round ten's profile-shaped fix left the default half standing.
    #[prompt(
        name = "fix_failing_test",
        description = "Diagnose and fix failing LOCAL tests: run `test` — the project's model \
         and unit tests, not the warehouse-run `--declarative` set — then for each failure \
         profile_column the implicated columns to ground the cause -> propose a fix. Stops at \
         the human approval gate."
    )]
    async fn fix_failing_test(
        &self,
        Parameters(args): Parameters<ScopedModelArgs>,
        _ctx: RequestContext<RoleServer>,
    ) -> Result<GetPromptResult, McpError> {
        let scope = match args.model.as_deref().map(str::trim) {
            Some(m) if !m.is_empty() => format!("the model `{m}`"),
            _ => "the project".to_string(),
        };
        // FF-WP1 fix round (finding 7): the worker variant fixes model SQL via
        // draft_model, never weakens tests, and ends at the handoff to the
        // trusted runner (no `propose` in this profile).
        if self.profile == McpProfile::Worker {
            let messages = vec![
                PromptMessage::new_text(
                    Role::Assistant,
                    "I'll run the tests, ground each failure in the real data before changing \
                     anything, and end with the fix drafted and handed off to the trusted \
                     runner. A failing test is a signal — I will find out whether the test is \
                     wrong or the data is wrong before I touch either.",
                ),
                PromptMessage::new_text(
                    Role::User,
                    format!(
                        "Diagnose and fix the failing tests in {scope}, using the MCP tools at \
                         each step:\n\n\
                         1. test — run the project's LOCAL model and unit tests, and read \
                         `failures`: each entry gives the failing test's `name`, its `error` \
                         text, and the `suite` it came from. That is everything it carries. \
                         There is no failing-row count field, and several failure paths — a \
                         compile error, a seed that will not load, a model that will not \
                         execute — report no row numbers in the error text either. \
                         That local suite is the only one you can run here: the checks the \
                         product spec declares are evaluated by the trusted runner after an \
                         apply, not by this tool.\n\
                         2. For each failure, ground the cause before deciding the fix: \
                         profile_column the implicated columns to see their actual null rate, \
                         distinct count, and value domain, and sample_rows to look at \
                         representative rows. sample_rows takes no predicate — it returns an \
                         unfiltered sample, so it is NOT failure-local evidence and a sparse \
                         bad row can be missing from it. Rows it does not show are not rows \
                         that do not exist; profile_column's whole-column counts are the \
                         stronger signal. The failure tells you WHAT broke; the data tells \
                         you WHY.\n\
                         3. Decide which side is wrong. If the model SQL is wrong (it produces \
                         duplicates / nulls / out-of-domain values it shouldn't), redraft it \
                         with draft_model — on an existing model it replaces the SQL and \
                         preserves the sidecar's metadata. If the TEST encodes a wrong \
                         invariant, do NOT weaken it, rewrite it, or append a new one: EVERY \
                         test edit is the trusted runner's here, and checks are spec-owned \
                         by any route, this server or a file you can write. Record the \
                         finding (which assertion, what the data actually holds) in your \
                         handoff.\n\
                         4. compile, then re-run the `test` tool. Read `all_passed`, not the \
                         model counts: it is true only when the model runs AND the fixture \
                         `[[test]]` blocks are both clean, and each entry in `failures` says \
                         which suite it came from. Loop until the failure is genuinely \
                         resolved, not silenced.\n\n\
                         RECONCILE DISCIPLINE: the whole point is to check the data, not just \
                         the schema. A uniqueness test failing because the grain is actually \
                         composite (two columns, not one) is a real finding you can only see \
                         in the rows.\n\n\
                         STOP when `all_passed` is true (or the remaining failures are \
                         diagnosed as wrong tests), and HAND OFF to the trusted runner: \
                         report what you \
                         fixed and what you diagnosed. Do not record plans, approve changes, \
                         or apply anything on your own — those verbs belong to the trusted \
                         runner and are not served in this profile."
                    ),
                ),
            ];
            return Ok(GetPromptResult::new(messages).with_description(format!(
                "Diagnose and fix failing tests in {scope} (worker profile, ends at the \
                 runner handoff)"
            )));
        }
        let messages = vec![
            PromptMessage::new_text(
                Role::Assistant,
                "I'll run the tests, ground each failure in the real data before changing \
                 anything, and stop at a proposed fix for you to review and apply. A failing test \
                 is a signal — I will find out whether the test is wrong or the data is wrong \
                 before I touch either.",
            ),
            PromptMessage::new_text(
                Role::User,
                format!(
                    "Diagnose and fix the failing tests in {scope}, using the MCP tools at each \
                     step:\n\n\
                     1. test — run the project's LOCAL suites and read which ones fail, on which \
                     model. The tool runs each model against DuckDB AND the fixture `[[test]]` \
                     blocks in the sidecars; every entry in `failures` says which suite it came \
                     from.\n\
                     2. For each failure, ground the cause before deciding the fix: profile_column \
                     the implicated columns (the ones the assertion references) to see their \
                     actual null rate, distinct count, and value domain, and sample_rows to look \
                     at representative rows. sample_rows takes no predicate — it returns an \
                     unfiltered sample, so it is NOT failure-local evidence and a sparse bad row \
                     can be missing from it. Rows it does not show are not rows that do not \
                     exist; profile_column's whole-column counts are the stronger signal. The \
                     failure tells you WHAT broke; the data tells you WHY.\n\
                     3. Decide which side is wrong. Either the model SQL is wrong (it produces \
                     duplicates / nulls / out-of-domain values it shouldn't) — fix the SQL — or \
                     the test encodes an invariant the data was never meant to hold — fix the \
                     assertion. Do not weaken a test just to make it pass; that hides the \
                     defect.\n\
                     4. compile, then re-run the `test` tool. Read `all_passed`, not the model \
                     counts: it is true only when both suites are clean. Loop until the failure \
                     is genuinely resolved, not silenced.\n\
                     5. propose — generate the plan recording the fix. It is an AI-authored plan \
                     with a plan_id.\n\n\
                     RECONCILE DISCIPLINE: the whole point is to check the data, not just the \
                     schema. A uniqueness test failing because the grain is actually composite \
                     (two columns, not one) is a real finding you can only see in the rows.\n\n\
                     STOP at propose. Never apply an AI-authored change directly — a bare apply is \
                     refused by design. Surface the plan_id and the review report, then the human \
                     runs `rocky review <plan-id> --approve` and `rocky apply <plan-id>`. Do not \
                     approve on the user's behalf unless they explicitly tell you to."
                ),
            ),
        ];

        Ok(GetPromptResult::new(messages)
            .with_description(format!("Diagnose and fix failing tests in {scope}")))
    }
}

#[tool_handler(router = self.tool_router)]
#[prompt_handler(router = self.prompt_router)]
impl ServerHandler for RockyMcpServer {
    fn get_info(&self) -> ServerInfo {
        // FF-WP1 fix round 2 (item 5a): the compiled skill is the FULL
        // authoring workflow, served to both profiles so the guidance never
        // forks from the canonical file — but under the worker profile it is
        // prefixed with the banner naming the tools this session does not
        // serve and redirecting every ending to the trusted-runner hand-off.
        // The approver profile serves the same text as the default one: the
        // skill already ends every workflow at the human's `rocky review`, and
        // announcing "you may approve here" to the agent would push the wrong
        // way (#1517). The capability is discoverable where it is used — in
        // `review_queue`'s own description.
        // Resolved at construction (see the `instructions` field): the
        // default and approver profiles carry the skill text byte-unchanged,
        // the worker carries the derived banner + the projected body.
        let instructions = self.instructions.clone();
        ServerInfo::new(
            ServerCapabilities::builder()
                .enable_tools()
                .enable_prompts()
                .build(),
        )
        .with_server_info(Implementation::from_build_env())
        .with_protocol_version(ProtocolVersion::V_2024_11_05)
        .with_instructions(instructions)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A validated, runnable table reference plus the warehouse adapter to run it on.
struct Prepared {
    adapter: std::sync::Arc<dyn rocky_core::traits::WarehouseAdapter>,
    table_ref: String,
}

impl Prepared {
    fn dialect_tablesample(&self, percent: u32) -> Option<String> {
        self.adapter.dialect().tablesample_clause(percent)
    }
}

/// Run a grounding query, preferring the columnar Arrow path and falling back
/// to the row-based JSON path.
///
/// `fetch_arrow_batch` is implemented on DuckDB / BigQuery / Databricks / Trino;
/// Snowflake inherits the trait default that errors before running any SQL, so
/// it always falls back to [`WarehouseAdapter::execute_query`]. A genuine SQL
/// error on an Arrow-capable adapter re-surfaces with its real message via the
/// `execute_query` arm — nothing is swallowed, just one extra round-trip on a
/// real failure. The inner conversion `Err` (an unformattable Arrow type) also
/// falls back rather than hard-erroring.
async fn query_grounding(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
    sql: &str,
) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
    if let Ok(batch) = adapter.fetch_arrow_batch(sql).await
        && let Ok(qr) = record_batch_to_query_result(&batch)
    {
        return Ok(qr);
    }
    adapter.execute_query(sql).await
}

/// Convert an Arrow [`RecordBatch`](arrow::record_batch::RecordBatch) into the
/// row-based [`QueryResult`](rocky_core::traits::QueryResult) the grounding
/// tools consume.
///
/// Each cell renders to text via `arrow`'s `ArrayFormatter`, EXCEPT SQL NULL:
/// the default `FormatOptions` renders NULL as the empty string, which would be
/// indistinguishable from an empty value, so NULL is emitted as
/// `serde_json::Value::Null` explicitly (checked via `Array::is_null`). All
/// other cells become `Value::String`, matching the JSON path's effective shape
/// for the grounding tools (which render every cell to a display string and
/// parse aggregates back out of strings).
fn record_batch_to_query_result(
    batch: &arrow::record_batch::RecordBatch,
) -> Result<rocky_core::traits::QueryResult, arrow::error::ArrowError> {
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    let schema = batch.schema();
    let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();

    let options = FormatOptions::default();
    // One formatter per column, built once, then indexed per row.
    let formatters: Vec<ArrayFormatter> = batch
        .columns()
        .iter()
        .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
        .collect::<Result<_, _>>()?;

    let mut rows: Vec<Vec<serde_json::Value>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let mut cells: Vec<serde_json::Value> = Vec::with_capacity(batch.num_columns());
        for (col_idx, fmt) in formatters.iter().enumerate() {
            let cell = if batch.column(col_idx).is_null(row) {
                serde_json::Value::Null
            } else {
                serde_json::Value::String(fmt.value(row).to_string())
            };
            cells.push(cell);
        }
        rows.push(cells);
    }

    Ok(rocky_core::traits::QueryResult { columns, rows })
}

/// Build the `AdapterRegistry` from the loaded config. Thin wrapper so the
/// call site reads clearly; the registry constructor lives in rocky-cli.
fn commands_adapter_registry(
    cfg: &rocky_core::config::RockyConfig,
) -> anyhow::Result<rocky_cli::registry::AdapterRegistry> {
    rocky_cli::registry::AdapterRegistry::from_config(cfg)
}

/// Build the per-column **statistics** query for `ai_contract`'s profiler.
///
/// Aggregate counts only — `COUNT(*)`, `COUNT(col)`, `COUNT(DISTINCT col)`.
/// Deliberately selects no `MIN`/`MAX` and issues no domain query, so no raw
/// cell value can reach the LLM prompt (the egress contract the MCP
/// `ai_contract` tool upholds — see `profile_table_columns`). `table_ref`
/// and `col` are already validated by the caller.
fn column_stats_sql(table_ref: &str, col: &str) -> String {
    format!(
        "SELECT COUNT(*) AS n, COUNT({col}) AS non_null, COUNT(DISTINCT {col}) AS distinct_n \
         FROM {table_ref}"
    )
}

/// Parse a qualified `schema.table` / `catalog.schema.table` reference into a
/// [`TableRef`](rocky_ir::TableRef) for `drift_preview`'s `describe_table`
/// calls.
///
/// Mirrors `commands/profile.rs::observed_column_types`: a two-part name has an
/// empty catalog (DuckDB / catalog-less dialects), a three-part name carries
/// one. Any other arity is rejected (returns `None`). Segments are not
/// validated here — `describe_table` is parameter-safe (the adapter quotes the
/// ref); a bad name surfaces as a describe error, not SQL injection.
fn parse_table_ref(reference: &str) -> Option<rocky_ir::TableRef> {
    let parts: Vec<&str> = reference.split('.').collect();
    match parts.as_slice() {
        [schema, table] => Some(rocky_ir::TableRef {
            catalog: String::new(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        [catalog, schema, table] => Some(rocky_ir::TableRef {
            catalog: (*catalog).to_string(),
            schema: (*schema).to_string(),
            table: (*table).to_string(),
        }),
        _ => None,
    }
}

/// Stable wire name for a [`DriftAction`](rocky_ir::DriftAction) in a
/// `drift_preview` result — snake_case, matching the strings `rocky run`
/// emits in `DriftActionOutput.action`.
fn drift_action_wire_name(action: &rocky_ir::DriftAction) -> &'static str {
    match action {
        rocky_ir::DriftAction::DropAndRecreate => "drop_and_recreate",
        rocky_ir::DriftAction::AlterColumnTypes => "alter_column_types",
        rocky_ir::DriftAction::Ignore => "ignore",
    }
}

/// Read a `serde_json::Value` grounding cell as a `u64`, tolerating the
/// string-encoded integers some adapters return.
fn json_as_u64(v: &serde_json::Value) -> u64 {
    match v {
        serde_json::Value::Number(n) => n.as_u64().unwrap_or(0),
        serde_json::Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// Parse a `target_dialect` tool argument into the engine's [`Dialect`].
///
/// Accepts the `Dialect` serde vocabulary case-insensitively
/// (`databricks`/`snowflake`/`bigquery`/`duckdb`). An unrecognised value is a
/// caller error returned as an [`InvalidArgument`](crate::error::ToolErrorCode)
/// envelope naming the accepted values, rather than silently ignoring the
/// request.
fn parse_target_dialect(raw: &str) -> Result<rocky_sql::transpile::Dialect, rmcp::Json<ToolError>> {
    use rocky_sql::transpile::Dialect;
    match raw.trim().to_ascii_lowercase().as_str() {
        "databricks" => Ok(Dialect::Databricks),
        "snowflake" => Ok(Dialect::Snowflake),
        "bigquery" => Ok(Dialect::BigQuery),
        "duckdb" => Ok(Dialect::DuckDB),
        other => Err(ToolError::invalid_argument(
            format!("unknown target_dialect '{other}'"),
            "Pass one of: databricks, snowflake, bigquery, duckdb.",
        )),
    }
}

/// Project a `CompileOutput` into the trimmed [`CompileResult`].
fn project_compile_result(output: &rocky_cli::output::CompileOutput) -> CompileResult {
    use rocky_compiler::diagnostic::Severity;
    let error_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Error)
        .count();
    let warning_count = output
        .diagnostics
        .iter()
        .filter(|d| d.severity == Severity::Warning)
        .count();
    let diagnostics = output
        .diagnostics
        .iter()
        .map(|d| DiagnosticLite {
            code: d.code.to_string(),
            severity: format!("{:?}", d.severity),
            model: d.model.clone(),
            message: d.message.to_string(),
            suggestion: d.suggestion.clone(),
            span: d
                .span
                .as_ref()
                .map(|s| format!("{}:{}:{}", s.file, s.line, s.col)),
        })
        .collect();
    CompileResult {
        has_errors: output.has_errors,
        error_count,
        warning_count,
        model_count: output.models,
        diagnostics,
    }
}

/// Project a `CatalogOutput` into the lite [`CatalogResult`], dropping the
/// (token-heavy) column-level edge set in favour of the per-asset
/// upstream/downstream model lists plus the aggregate counts. Agents that
/// need the edge trace use the `lineage` tool.
fn catalog_result(output: rocky_cli::output::CatalogOutput) -> CatalogResult {
    use rocky_cli::output::AssetKind;
    let assets = output
        .assets
        .into_iter()
        .map(|a| {
            let kind = match a.kind {
                AssetKind::Source => "source",
                AssetKind::Model => "model",
                AssetKind::View => "view",
                AssetKind::MaterializedView => "materialized_view",
            }
            .to_string();
            let columns = a
                .columns
                .into_iter()
                .map(|c| CatalogColumnLite {
                    name: c.name,
                    data_type: c.data_type,
                    nullable: c.nullable,
                })
                .collect();
            CatalogAssetLite {
                fqn: a.fqn,
                model_name: a.model_name,
                kind,
                columns,
                upstream_models: a.upstream_models,
                downstream_models: a.downstream_models,
                intent: a.intent,
            }
        })
        .collect();
    CatalogResult {
        project_name: output.project_name,
        assets,
        asset_count: output.stats.asset_count,
        column_count: output.stats.column_count,
        edge_count: output.stats.edge_count,
    }
}

/// Project a borrowed `LineageEdgeRecord` into the lite edge shape.
fn edge_lite(e: &rocky_cli::output::LineageEdgeRecord) -> LineageEdgeLite {
    LineageEdgeLite {
        source_model: e.source.model.clone(),
        source_column: e.source.column.clone(),
        target_model: e.target.model.clone(),
        target_column: e.target.column.clone(),
        transform: e.transform.clone(),
    }
}

/// Project a `BreakingFinding` into the lite, schemars-1.x shape.
///
/// `change` is the snake_case `kind` discriminant of the tagged
/// [`rocky_core::breaking_change::BreakingChange`] enum (e.g.
/// `"column_dropped"`); `model` and the optional `column` are pulled from the
/// variant; `message` is the debug rendering of the change, matching the
/// human-readable line `rocky review` emits.
fn breaking_finding_lite(f: &rocky_core::breaking_change::BreakingFinding) -> BreakingFindingLite {
    use rocky_core::breaking_change::BreakingSeverity;
    let severity = match f.severity {
        BreakingSeverity::Breaking => "breaking",
        BreakingSeverity::Warning => "warning",
        BreakingSeverity::Info => "info",
    }
    .to_string();

    // The enum is `#[serde(tag = "kind", rename_all = "snake_case")]`, so the
    // serialized object carries the discriminant under `kind` and the variant
    // fields (incl. `model` and, where present, `column`) at the top level.
    let value = serde_json::to_value(&f.change).unwrap_or_default();
    let change = value
        .get("kind")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();
    let model = value
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();
    let column = value
        .get("column")
        .and_then(|v| v.as_str())
        .map(str::to_string);

    BreakingFindingLite {
        change,
        severity,
        model,
        column,
        message: format!("{:?}", f.change),
    }
}

/// Discover the physical tables in the target warehouse as schema-qualified
/// source entries (best-effort — returns empty on any query error). Excludes
/// the system schemas. Lets `inspect_schema` show an agent the raw sources the
/// project never declared, including at cold start.
///
/// # The query is unqualified, and that is not portable
///
/// This said "the DuckDB warehouse", but the only caller hands it whatever
/// its `warehouse_adapter` resolved — any of DuckDB, Snowflake, BigQuery,
/// Databricks or Trino. The `FROM information_schema.columns` below carries
/// no catalog, and THREE of those five qualify that view when they build the
/// equivalent query themselves: `rocky-snowflake/src/batch.rs` scopes it to
/// `<database>.`, `rocky-databricks/src/batch.rs` to `<catalog>.`, and
/// `rocky-bigquery/src/dialect.rs` states outright that a bare
/// `INFORMATION_SCHEMA.COLUMNS` does not resolve there. The other two do not,
/// for their own reasons: DuckDB's catalog is flat and un-prefixed on purpose
/// (`rocky-duckdb/src/dialect.rs` pushes the catalog into a `WHERE` filter),
/// and Trino never reads `information_schema` at all — it describes columns
/// with `DESCRIBE` (`rocky-trino/src/adapter.rs`).
///
/// So the honest reading is NON-PORTABLE AND MAY FAIL off DuckDB — not
/// "returns empty on every non-DuckDB target", which is a step too strong.
/// Snowflake submits the adapter's configured `database` and `schema` with
/// every statement (`rocky-snowflake/src/connector.rs`, `SubmitRequest`), so
/// where those are set a bare `information_schema.columns` can resolve there.
/// Only the DuckDB path is verified; what the other four do with THIS exact
/// statement is untested, and the resolved-vs-failed distinction is the part
/// that varies. What does NOT vary is the caller: empty rows and a failed
/// query both leave it reporting success with no physical tables.
///
/// Named here rather than fixed: widening the query is a product change, and
/// it belongs with the silent-degradation defect the caller's note points at.
async fn discover_source_tables(
    adapter: &dyn rocky_core::traits::WarehouseAdapter,
) -> Result<Vec<SchemaEntry>, String> {
    let sql = "SELECT table_schema, table_name, column_name, data_type, is_nullable \
               FROM information_schema.columns \
               WHERE table_schema NOT IN ('information_schema', 'pg_catalog') \
               ORDER BY table_schema, table_name, ordinal_position";
    // The error is REPORTED, not swallowed. Returning an empty list here made a
    // failed query indistinguishable from a warehouse with nothing to find
    // (#1533) — and on BigQuery a bare `information_schema.columns` does not
    // resolve at all, so the empty return was the ordinary outcome there.
    let qr = adapter
        .execute_query(sql)
        .await
        .map_err(|e| format!("{e:#}"))?;
    let cell = |v: Option<&serde_json::Value>| -> String {
        match v {
            Some(serde_json::Value::String(s)) => s.clone(),
            Some(serde_json::Value::Null) | None => String::new(),
            Some(other) => other.to_string(),
        }
    };
    // Group columns under their `schema.table`, preserving first-seen order.
    let mut order: Vec<String> = Vec::new();
    let mut columns: std::collections::HashMap<String, Vec<ColumnLite>> =
        std::collections::HashMap::new();
    for row in qr.rows {
        let schema = cell(row.first());
        let table = cell(row.get(1));
        if schema.is_empty() || table.is_empty() {
            continue;
        }
        let key = format!("{schema}.{table}");
        if !columns.contains_key(&key) {
            order.push(key.clone());
        }
        columns.entry(key).or_default().push(ColumnLite {
            name: cell(row.get(2)),
            data_type: cell(row.get(3)),
            nullable: !cell(row.get(4)).eq_ignore_ascii_case("NO"),
        });
    }
    Ok(order
        .into_iter()
        .map(|name| {
            let cols = columns.remove(&name).unwrap_or_default();
            SchemaEntry {
                name,
                columns: cols,
            }
        })
        .collect())
}

/// Render one query cell as a display string, truncating long values.
fn render_cell(v: serde_json::Value) -> String {
    let s = match v {
        serde_json::Value::Null => "NULL".to_string(),
        serde_json::Value::String(s) => s,
        other => other.to_string(),
    };
    if s.chars().count() > CELL_MAX_CHARS {
        let mut out: String = s.chars().take(CELL_MAX_CHARS).collect();
        out.push('…');
        out
    } else {
        s
    }
}

/// The authoring-loop reminder every successful `draft_model` response carries.
/// A draft is written and compiled, never applied — this restates the flow so
/// the agent never mistakes a written draft for a materialized change.
/// Default profile only; the worker profile serves
/// [`WORKER_DRAFT_NEXT_STEPS`].
///
/// FIFTEENTH ROUND, finding 1 — this said `plan_preview` reads "the SQL
/// Rocky would run", on BOTH variants, and the round-fourteen sweep did not
/// reach either. The harm is concrete and this is the surface that delivers
/// it: a dynamic-table draft SUCCEEDS, receives this guidance, and is then
/// absent from the preview it was just told to read, because
/// `commands::plan_preview_output` skips what it cannot render offline and
/// `PlanPreviewResult` carries no field naming a skipped model. The agent
/// is holding a successful draft and an empty preview with nothing to tell
/// it the two are about the same model.
const DRAFT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched the \
     warehouse. Continue the authoring loop: fix any error diagnostics above and re-draft (or \
     `compile`) until it is clean, `plan_preview` to read the SQL that renders offline, then \
     `propose` to record an AI-authored plan for a human to `rocky review <plan_id> --approve` \
     and `rocky apply`. The preview is not the whole plan: a model it cannot render offline is \
     skipped and is not named, so a draft that succeeded here and is missing from the preview \
     is unrenderable offline, not absent from the project. Never apply a draft directly.";

/// The worker-profile variant of [`DRAFT_NEXT_STEPS`] (FF-WP1 fix round 2,
/// item 5c): the default reminder instructs `propose`, a tool this profile
/// does not serve — the worker's loop ends at the typed hand-off to the
/// trusted runner instead.
///
/// It also has to be exact about which suite the `test` tool runs, because
/// the two are easy to conflate and only one is reachable here. The `test`
/// tool runs `commands::test_output`, i.e. `rocky_engine::test_runner`'s
/// compiled model tests plus the unit tests. It does NOT run the
/// declarative check set — that is `rocky test --declarative`, a different
/// path, and the product's declared checks live there. Those checks also
/// need the applied table to exist, so the fulfillment loop reports them
/// DEFERRED at verify and evaluates them only at post-apply observation
/// (FF-WP-F3). Telling the worker to loop until they pass names an
/// outcome that cannot occur during drafting, which is the same defect
/// class as naming a tool the profile does not serve.
const WORKER_DRAFT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. Continue the drafting loop: fix any error diagnostics above and re-draft \
     (or `compile`) until it is clean, `plan_preview` to read the SQL that renders offline \
     (the preview is not the whole plan — a model it cannot render offline is skipped and is \
     not named, so a draft that succeeded here and is missing from the preview is \
     unrenderable offline, not absent from the project), and the \
     `test` tool to run the project's LOCAL tests. Those local tests are the only suite you \
     can run here. The checks the product spec declares — its grain, its not-null columns, its \
     `checks` list — are lowered into the sidecar for you and need the applied table to run \
     against, so they are deferred until after the apply and cannot pass or fail while you are \
     drafting. They are spec-owned: do not add one of your own. If the data needs an invariant \
     the spec does not state, say so in the SQL's comments. When the draft compiles clean and \
     the local tests pass, STOP and end at the typed hand-off to the trusted runner: report the \
     drafted files, what you verified, and anything you flagged. Recording, review, and apply \
     belong to the trusted runner — never act on them yourself.";

/// The authoring-loop reminder every successful `draft_contract` response
/// carries. The contract is written and compile-validated, never applied.
const DRAFT_CONTRACT_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. The contract is written and compile-validated against the model's schema \
     (read any `W010`-class diagnostic above and re-draft to fix a column mismatch). When it is \
     clean, `propose` to record an AI-authored plan for a human to `rocky review <plan_id> \
     --approve` and `rocky apply`. Never apply a draft directly.";

/// The authoring-loop reminder every successful `draft_check` response carries.
/// The check is written and structurally compiled, then executed via `test`.
/// Default profile only; the worker profile serves
/// [`WORKER_DRAFT_CHECK_NEXT_STEPS`].
const DRAFT_CHECK_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched the \
     warehouse. The check is merged into the model's sidecar and the project compiles; run the \
     `test` tool to EXECUTE the check against the data and confirm it passes. When it is clean, \
     `propose` to record an AI-authored plan for a human to `rocky review <plan_id> --approve` \
     and `rocky apply`. Never apply a draft directly.";

/// The worker-profile variant of [`DRAFT_CHECK_NEXT_STEPS`] (FF-WP1 fix
/// round 2, item 5c): ends at the typed hand-off to the trusted runner
/// instead of instructing `propose`.
const WORKER_DRAFT_CHECK_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or \
     touched the warehouse. The check is merged into the model's sidecar and the project \
     compiles; run the `test` tool to EXECUTE the check against the data and confirm it passes. \
     When it is clean, STOP and end at the typed hand-off to the trusted runner: report the \
     model, the invariants you encoded, and anything you flagged. Recording, review, and apply \
     belong to the trusted runner — never act on them yourself.";

/// The authoring-loop reminder every successful `draft_metadata` response
/// carries. The patched sidecar is written and compile-validated, never
/// applied.
const DRAFT_METADATA_NEXT_STEPS: &str = "This is a draft — Rocky has NOT applied it or touched \
     the warehouse. The metadata patch is merged into the model's sidecar and the project \
     compiles; freshness and classifications take effect when the model is next materialized \
     and reconciled. If this metadata change should ship with a model change, continue the \
     loop: `compile`, then `propose` for a human to `rocky review <plan_id> --approve` and \
     `rocky apply`. Never apply a draft directly.";

/// The validated on-disk targets a draft writes to.
struct DraftPaths {
    /// The model name (bare file stem).
    stem: String,
    /// Absolute path of `models/<stem>.sql`.
    sql_path: PathBuf,
    /// Absolute path of `models/<stem>.toml`.
    sidecar_path: PathBuf,
    /// Absolute path of `models/<stem>.contract.toml`.
    contract_path: PathBuf,
}

/// Restore `path` to its snapshotted `prior` bytes, or remove it when it had no
/// prior content. The rollback primitive for a policy-denied (or failed) draft:
/// a freshly written draft is removed entirely; a re-draft over an existing
/// model is restored to the model's prior content, so a deny never corrupts nor
/// leaves a new artifact. Removing an already-absent file is success (the
/// desired end state holds); any other failure comes back to the caller — the
/// refusal must say what it left behind (#1561).
///
/// Neither arm follows a link at the leaf. `remove_file` unlinks the leaf
/// itself; the restore goes through `write_no_follow`, so a link swapped in
/// at the path since the snapshot fails the restore — reported by the caller
/// — instead of receiving the prior bytes at its target.
fn restore_or_remove(path: &Path, prior: Option<&[u8]>) -> std::io::Result<()> {
    match prior {
        Some(bytes) => write_no_follow(path, bytes),
        None => match std::fs::remove_file(path) {
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            other => other,
        },
    }
}

/// Refuse a draft path occupied by anything but a regular file — a symlink
/// (dangling or not), a directory, a FIFO, a socket, a device — before
/// anything is snapshotted or written through it. Every draft tool resolves
/// its paths through this (via `resolve_draft_paths`): the up-front leaf half
/// of the no-follow guard. `symlink_metadata` does not follow, so a dangling
/// link is seen as what it is; a FIFO is refused here so the refusal names
/// its kind rather than surfacing as an unreadable prior. An absent path is
/// fine; any other inspection failure refuses too, because the write would
/// land somewhere the tool could not inspect. What appears AFTER this check
/// meets a no-follow open on both sides — the snapshot's read
/// (`read_no_follow_bytes`) and every write and rollback (`write_no_follow`).
///
/// Residual: the refusal's own remediation text still describes only the
/// write half. It is one of the pinned wire strings, so it is left as it
/// stands rather than reworded here.
fn refuse_non_regular_draft_target(root: &Path, path: &Path) -> Result<(), Json<ToolError>> {
    let meta = match std::fs::symlink_metadata(path) {
        Ok(meta) => meta,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(ToolError::internal(
                format!(
                    "failed to inspect draft target {}: {e}",
                    rel_display(root, path)
                ),
                "Ensure the models directory is readable, then retry.",
            ));
        }
    };
    let file_type = meta.file_type();
    if file_type.is_file() {
        return Ok(());
    }
    let (what, remedy) = if file_type.is_symlink() {
        (
            "is a symlink; refusing to write through it".to_string(),
            "Replace the symlink with a regular file, or remove it,",
        )
    } else {
        (
            format!(
                "is a {}, not a regular file; refusing to write to it",
                describe_non_regular(file_type)
            ),
            "Remove it,",
        )
    };
    Err(ToolError::invalid_argument(
        format!("draft target {} {what}", rel_display(root, path)),
        format!(
            "{remedy} so the draft lands in a regular file inside the models directory, then \
             retry. Every draft path is checked before the write, and on unix each write and \
             rollback opens the leaf without following a link, so a link swapped in after this \
             check fails the write instead of redirecting it (elsewhere this check is the only \
             leaf guard)."
        ),
    ))
}

/// Name a non-regular file type for a refusal message.
fn describe_non_regular(file_type: std::fs::FileType) -> &'static str {
    if file_type.is_dir() {
        return "directory";
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileTypeExt as _;
        if file_type.is_fifo() {
            return "FIFO";
        }
        if file_type.is_socket() {
            return "socket";
        }
        if file_type.is_block_device() || file_type.is_char_device() {
            return "device";
        }
    }
    "non-regular file"
}

/// A path [`DraftRollback::snapshot`] found on disk but could not read.
/// Refused before anything is written: the draft tools never overwrite a file
/// they cannot read back, because its content is what the rollback would have
/// to restore.
#[derive(Debug)]
struct UnreadablePrior {
    path: PathBuf,
    error: std::io::Error,
}

impl UnreadablePrior {
    fn into_tool_error(self, root: &Path) -> Json<ToolError> {
        ToolError::invalid_argument(
            format!(
                "the file at {} exists but cannot be read ({}); refusing to draft over it",
                rel_display(root, &self.path),
                self.error
            ),
            "Fix the file's permissions (it must be readable so its prior content can be \
             preserved and restored), then retry. The draft tools never overwrite a file they \
             cannot read back.",
        )
    }
}

/// Panic-safe rollback guard for the `draft_*` write tools.
///
/// Snapshots each path's prior bytes at construction and restores them (via
/// [`restore_or_remove`]) when dropped — on an error return, a policy deny,
/// **or a panic anywhere between the write and the verdict** (e.g. inside
/// compile). A manual rollback closure only runs on the arms that call it;
/// unwinding past it would leave a denied/broken draft on disk, violating the
/// "a denied draft leaves NO file" contract. Call [`defuse`](Self::defuse) on
/// the keep paths: success, or require-review (where the draft IS the
/// reviewable artifact). The deliberate refusal arms (deny / unloadable
/// policy) call [`rollback`](Self::rollback) instead of dropping the guard —
/// a `Drop` cannot return the outcome, and a refusal whose cleanup failed
/// must not claim the draft was removed (#1561).
struct DraftRollback {
    /// `(path, prior bytes)` — `None` when the file did not exist.
    entries: Vec<(PathBuf, Option<Vec<u8>>)>,
    defused: bool,
}

impl DraftRollback {
    /// Snapshot `paths` before the draft writes them. Only a NotFound read is
    /// "absent" (`None`). Any other read error — permission denied, a
    /// directory at the path — refuses the whole snapshot: a path that EXISTS
    /// but cannot be read back has no prior to restore, so the rollback would
    /// take the remove arm and delete it (#1572 follow-up).
    ///
    /// The read goes through `read_no_follow_bytes`, the read counterpart of
    /// the no-follow writes. `std::fs::read` re-resolved the path, so a link
    /// or FIFO swapped in at a leaf between `resolve_draft_paths` and here
    /// was read through — the prior content could come from OUTSIDE the
    /// project and then ride the merge and the rollback — or parked the
    /// request forever. The open now carries `O_NOFOLLOW | O_NONBLOCK` on
    /// unix, the regular-file check is on the DESCRIPTOR, and the read is
    /// bounded: a file over `MAX_BACKUP_BYTES` (16 MiB) refuses rather than
    /// being buffered whole.
    fn snapshot<I, P>(paths: I) -> Result<Self, UnreadablePrior>
    where
        I: IntoIterator<Item = P>,
        P: Into<PathBuf>,
    {
        let mut entries = Vec::new();
        for p in paths {
            let path: PathBuf = p.into();
            let prior = match read_no_follow_bytes(&path) {
                Ok(bytes) => Some(bytes),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => return Err(UnreadablePrior { path, error }),
            };
            entries.push((path, prior));
        }
        Ok(Self {
            entries,
            defused: false,
        })
    }

    /// Async wrapper over [`snapshot`](Self::snapshot) that runs the prior-bytes
    /// reads on the blocking pool. The async draft handlers use this so the
    /// snapshot reads don't block the tokio worker; the sync `snapshot` stays
    /// for the `catch_unwind`-based restore-on-panic unit test.
    async fn snapshot_async(paths: Vec<PathBuf>) -> Result<Self, UnreadablePrior> {
        // `snapshot` returns every read error, so the closure can't panic and
        // the `JoinError` arm is unreachable in practice.
        tokio::task::spawn_blocking(move || Self::snapshot(paths))
            .await
            .expect("DraftRollback::snapshot does not panic")
    }

    /// The snapshotted prior bytes for `path` (`None` = the file did not
    /// exist, or the path was never snapshotted).
    fn prior(&self, path: &Path) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(p, _)| p == path)
            .and_then(|(_, prior)| prior.as_deref())
    }

    /// Keep the draft on disk: consume the guard without restoring.
    fn defuse(mut self) {
        self.defused = true;
    }

    /// Roll back NOW and report the outcome: restore or remove every
    /// snapshotted path and return the ones that FAILED — artifacts still on
    /// disk in their drafted state (empty = clean). The deliberate refusal
    /// arms call this so a failed cleanup reaches the response instead of
    /// being discarded (#1561); the `Drop` impl stays the unwind-path net.
    fn rollback(mut self) -> Vec<RollbackFailure> {
        self.defused = true;
        self.entries
            .iter()
            .filter_map(|(path, prior)| {
                let error = restore_or_remove(path, prior.as_deref()).err()?;
                let arm = match prior {
                    Some(_) => RollbackArm::Restore,
                    None => RollbackArm::Remove,
                };
                Some(RollbackFailure {
                    path: path.clone(),
                    arm,
                    error,
                })
            })
            .collect()
    }
}

impl Drop for DraftRollback {
    fn drop(&mut self) {
        if self.defused {
            return;
        }
        for (path, prior) in &self.entries {
            if let Err(error) = restore_or_remove(path, prior.as_deref()) {
                // The unwind path cannot return the outcome; the deliberate
                // refusal arms use `rollback` for that. Log so the leftover
                // is at least visible to the operator — checked, not assumed:
                // a failed restore can leave the path gone, or uninspectable.
                let left_behind = LeftBehind::inspect(path);
                tracing::warn!(
                    path = %path.display(),
                    %error,
                    %left_behind,
                    "draft rollback failed"
                );
            }
        }
    }
}

/// Which arm a snapshotted path's rollback took.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RollbackArm {
    /// The path had no prior content: the draft is unlinked.
    Remove,
    /// The path had prior content: it is written back, no-follow.
    Restore,
}

impl RollbackArm {
    /// Completes "could not be …" in the refusal message.
    fn failed_verb(self) -> &'static str {
        match self {
            RollbackArm::Remove => "removed",
            RollbackArm::Restore => "restored to its prior content",
        }
    }
}

/// One path a [`DraftRollback::rollback`] could not clean up: the path, the
/// arm that failed, and the I/O cause.
struct RollbackFailure {
    path: PathBuf,
    arm: RollbackArm,
    error: std::io::Error,
}

/// What a failed rollback left at a path — CHECKED, never assumed.
enum LeftBehind {
    /// Something is at the path. `symlink_metadata` does not follow, so a
    /// link swapped in at the leaf is reported as the link it is.
    Present { symlink: bool },
    /// Nothing is at the path.
    Absent,
    /// The path could not be inspected at all — an unsearchable parent, say —
    /// so whether anything is there is unknown. Before this state existed,
    /// every inspection error read as "absent".
    Uninspectable(std::io::Error),
}

impl LeftBehind {
    fn inspect(path: &Path) -> Self {
        match std::fs::symlink_metadata(path) {
            Ok(meta) => LeftBehind::Present {
                symlink: meta.file_type().is_symlink(),
            },
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => LeftBehind::Absent,
            Err(e) => LeftBehind::Uninspectable(e),
        }
    }

    /// The operator-facing sentence for `name` after `arm` failed, worded by
    /// what the arm was trying to do and by what is actually there now.
    fn sentence(&self, name: &str, arm: RollbackArm) -> String {
        match (arm, self) {
            (RollbackArm::Remove, LeftBehind::Present { symlink }) => format!(
                "{name} is STILL ON DISK{}, remove it manually",
                link_note(*symlink)
            ),
            (RollbackArm::Restore, LeftBehind::Present { symlink }) => format!(
                "{name} is STILL ON DISK{} without its prior content, put that content back \
                 manually from version control or a backup",
                link_note(*symlink)
            ),
            (RollbackArm::Remove, LeftBehind::Absent) => format!(
                "{name} could not be removed yet is now absent, check the models directory by \
                 hand"
            ),
            (RollbackArm::Restore, LeftBehind::Absent) => format!(
                "the prior content of {name} could not be restored and the path is now absent, \
                 recover it from version control or a backup"
            ),
            (RollbackArm::Remove | RollbackArm::Restore, LeftBehind::Uninspectable(e)) => {
                format!(
                    "{name} could not be inspected after the failed rollback ({e}), so whether \
                     it is on disk is unknown; check the models directory by hand"
                )
            }
        }
    }
}

impl std::fmt::Display for LeftBehind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LeftBehind::Present { symlink } => write!(f, "present{}", link_note(*symlink)),
            LeftBehind::Absent => f.write_str("absent"),
            LeftBehind::Uninspectable(e) => write!(f, "uninspectable ({e})"),
        }
    }
}

fn link_note(symlink: bool) -> &'static str {
    if symlink { " (a symlink)" } else { "" }
}

/// Roll a refused draft back and word the outcome for the refusal message:
/// `clean` verbatim when every snapshotted path restored, or a sentence
/// naming each path the cleanup FAILED on (with its I/O cause) and what that
/// left behind — a refusal must never claim a rollback it did not perform
/// (#1561). `failed_lead` opens the failure sentence so it reads in the arm's
/// grammar (e.g. "but rolling it back FAILED" mid-sentence). A failure also
/// returns the project-relative paths for the envelope's
/// `rollback_failed_paths` field.
///
/// What was left behind is CHECKED, not assumed, and has three states, each
/// worded by the arm that failed: present (a leftover to remove, or a path
/// whose prior content is not back — a swapped-in link is named as one),
/// absent (a failed restore whose target is gone: the prior content is what
/// was lost), and uninspectable (the check itself failed, so nothing is
/// claimed either way and the error is named).
fn rollback_disposition(
    root: &Path,
    rollback: DraftRollback,
    clean: &str,
    failed_lead: &str,
) -> (String, Option<Vec<String>>) {
    let failures = rollback.rollback();
    if failures.is_empty() {
        return (clean.to_string(), None);
    }
    let listed = failures
        .iter()
        .map(|f| {
            format!(
                "{} could not be {} ({})",
                rel_display(root, &f.path),
                f.arm.failed_verb(),
                f.error
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    let paths = failures
        .iter()
        .map(|f| rel_display(root, &f.path))
        .collect();
    let outcome = failures
        .iter()
        .map(|f| LeftBehind::inspect(&f.path).sentence(&rel_display(root, &f.path), f.arm))
        .collect::<Vec<_>>()
        .join("; ");
    (format!("{failed_lead} — {listed}; {outcome}."), Some(paths))
}

/// Filter the pending review queue to plans whose payload carries
/// `product_id == product`, reading each candidate plan integrity-checked.
///
/// The queue's ledger rows do not carry plan payloads (`compute_review_queue`
/// reads decisions + marker state only), so the product filter is the point
/// where plan files get read. Fail-open is not an option here: a pending plan
/// whose file is missing, unreadable, or fails its integrity re-hash CANNOT
/// prove which product it belongs to, so it surfaces as a `warning` entry —
/// never a silent drop that would hide a possibly-matching escalation from
/// the runner. Returns the filtered `pending` value plus its entry count.
fn filter_pending_by_product(
    root: &Path,
    pending: &[rocky_cli::output::ReviewQueueEntry],
    product: &str,
) -> Result<(serde_json::Value, u64), Json<ToolError>> {
    let mut entries: Vec<serde_json::Value> = Vec::new();
    for entry in pending {
        match rocky_cli::plan_store::read_plan(root, &entry.plan_id) {
            Ok(plan) => {
                if plan.payload.get("product_id").and_then(|v| v.as_str()) == Some(product) {
                    let value = serde_json::to_value(entry).map_err(|e| {
                        ToolError::internal(
                            format!("failed to serialize a review queue entry: {e}"),
                            "Retry; if it persists this is an internal serialization bug.",
                        )
                    })?;
                    entries.push(value);
                }
            }
            Err(e) => entries.push(serde_json::json!({
                "plan_id": entry.plan_id,
                "warning": format!(
                    "pending plan could not be read for product filtering ({e:#}); it may or \
                     may not belong to '{product}' — inspect it directly, it remains pending"
                ),
            })),
        }
    }
    let total = entries.len() as u64;
    Ok((serde_json::Value::Array(entries), total))
}

/// Build the `[freshness]` TOML table a validated [`FreshnessPatch`] writes.
///
/// Validates the patch shape (a positive lag that fits TOML's i64 integers, a
/// non-empty `time_column`, a `severity` the engine's `TestSeverity` accepts)
/// so a malformed patch refuses as `invalid_argument` before any file I/O.
fn build_freshness_table(patch: &FreshnessPatch) -> Result<toml::Table, Json<ToolError>> {
    if patch.expected_lag_seconds == 0 {
        return Err(ToolError::invalid_argument(
            "freshness.expected_lag_seconds must be greater than zero",
            "Pass the maximum acceptable staleness in seconds (e.g. 86400 for 24h).",
        ));
    }
    let lag: i64 = patch.expected_lag_seconds.try_into().map_err(|_| {
        ToolError::invalid_argument(
            "freshness.expected_lag_seconds exceeds the TOML integer range",
            "Pass a realistic lag in seconds (TOML integers are 64-bit signed).",
        )
    })?;
    let mut table = toml::Table::new();
    table.insert(
        "expected_lag_seconds".to_string(),
        toml::Value::Integer(lag),
    );
    if let Some(time_column) = &patch.time_column {
        if time_column.trim().is_empty() {
            return Err(ToolError::invalid_argument(
                "freshness.time_column must be non-empty when present",
                "Name the model's timestamp column, or omit the field to fall back to the \
                 last-materialization timestamp.",
            ));
        }
        table.insert(
            "time_column".to_string(),
            toml::Value::String(time_column.clone()),
        );
    }
    if let Some(severity) = &patch.severity {
        if severity != "warning" && severity != "error" {
            return Err(ToolError::invalid_argument(
                format!("freshness.severity '{severity}' is not a valid severity"),
                "Pass \"warning\" (non-blocking, the engine default) or \"error\".",
            ));
        }
        table.insert(
            "severity".to_string(),
            toml::Value::String(severity.clone()),
        );
    }
    Ok(table)
}

/// Structural gate for a `draft_check` spec: parse it as TOML and require
/// every top-level key to be the `tests` array-of-tables.
///
/// The spec is appended verbatim to the model's sidecar, so any other
/// top-level table or key — `[target]`, `[strategy]`, or a bare `key = value`
/// that would attach to the sidecar's last table — is model config smuggled
/// through the check write path. Rejected with a structured
/// `invalid_argument` naming the offending key.
fn validate_check_spec(spec: &str) -> Result<(), Json<ToolError>> {
    let parsed: toml::Table = toml::from_str(spec).map_err(|e| {
        ToolError::invalid_argument(
            format!("draft_check `spec` is not valid TOML: {e}"),
            "Author the check as one or more declarative `[[tests]]` blocks, e.g.\n[[tests]]\n\
             type = \"not_null\"\ncolumn = \"id\"\nThen pass it as `spec`.",
        )
    })?;
    for (key, value) in &parsed {
        if key != "tests" {
            return Err(ToolError::invalid_argument(
                format!(
                    "draft_check `spec` may only contain `[[tests]]` blocks; found top-level \
                     `{key}`"
                ),
                "A check spec cannot carry model config: keys like `[target]` or `[strategy]` \
                 belong to the model's own sidecar. Drop them from the spec; to change the model \
                 itself, use draft_model.",
            ));
        }
        if !value.is_array() {
            return Err(ToolError::invalid_argument(
                "draft_check `spec` must declare `tests` as an array of tables (`[[tests]]`), \
                 not a single table or value",
                "Use the array-of-tables header form:\n[[tests]]\ntype = \"not_null\"\n\
                 column = \"id\"",
            ));
        }
    }
    Ok(())
}

/// Ensure the drafted SQL ends in exactly one trailing newline (POSIX text
/// file), without disturbing a body that already does.
fn ensure_trailing_newline(sql: &str) -> String {
    let trimmed = sql.trim_end_matches('\n');
    format!("{trimmed}\n")
}

/// Build the draft sidecar TOML: `name` (matching the file stem so the L001
/// name lint stays quiet) plus the `intent`, both TOML-escaped. Target and
/// strategy are intentionally omitted — they resolve from the project's
/// conventions, keeping the draft tool from inventing routing the agent never
/// asked for.
fn draft_sidecar(stem: &str, intent: &str) -> String {
    let header = "# Draft authored via the Rocky MCP `draft_model` tool. Target and strategy \
                  resolve\n# from the project's conventions (rocky.toml pipeline + \
                  _defaults.toml).\n";
    if intent.is_empty() {
        format!("{header}name = {}\n", toml_basic_string(stem))
    } else {
        format!(
            "{header}name = {}\nintent = {}\n",
            toml_basic_string(stem),
            toml_basic_string(intent)
        )
    }
}

/// Render `s` as a TOML basic string (double-quoted, with the escapes TOML
/// requires) so an arbitrary intent embeds safely in the sidecar.
fn toml_basic_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04X}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// Display `path` relative to the project `root` with forward slashes, falling
/// back to the absolute path when it is not under the root.
fn rel_display(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every word that may follow the word "rocky" in the projected worker
    /// instructions WITHOUT being a CLI route.
    ///
    /// Each one is the second half of a phrase in the skill, and none is an
    /// invocation:
    ///
    /// - `and` — "SQL is first-class in Rocky **and** you are fluent in it."
    /// - `data` — "…build or change a Rocky **data** model."
    /// - `dsl` — "the `.rocky` **DSL** exists and is fully supported". The
    ///   file extension, not the CLI name.
    /// - `model` — "…asked to build or change a Rocky **model**."
    /// - `models` — "# Authoring Rocky **models** as an agent"
    /// - `rather` — "…authoring on Rocky **rather** than emitting bare SQL."
    ///
    /// A PROSE LIST, NOT A VERB LIST — see the scan's own comment in
    /// `worker_instructions_are_projected_and_default_stays_verbatim` for
    /// why the verb set is unreachable from this crate. The consequence to
    /// keep in mind when this test fails: the fix is usually to rewrite the
    /// sentence as a served action, and only sometimes to add a word here.
    /// Add one only after reading the sentence it came from.
    ///
    /// The list is kept as bare strings on purpose. Per-entry comments here
    /// get reflowed onto the wrong neighbour by rustfmt, which turns the
    /// justification into a lie about the word above it.
    const ROCKY_PROSE_FOLLOWERS: &[&str] = &["and", "data", "dsl", "model", "models", "rather"];

    /// Every word that follows an identifier-bounded `rocky` in `lower`, in
    /// order of appearance. `lower` must already be lowercase.
    ///
    /// Identifier-bounded on both sides, by the same rule
    /// [`contains_identifier`] uses, so `rocky_sdk` and `unrocky` do not
    /// match. "Follows" skips the markup that sits between a word and its
    /// neighbour — spaces, backticks, asterisks — then takes the leading
    /// run of `[a-z-]`. A `rocky` followed by punctuation or by the end of
    /// the text contributes nothing: there is no word to judge.
    ///
    /// THE RIGHT BOUNDARY ALSO COUNTS `-`, and only the right one. A
    /// hyphen binds the next word into a compound NAME — `rocky-config`,
    /// `rocky-ai-workflow`, both skills this document cites — and a
    /// compound name is not an invocation: the CLI form is always
    /// `rocky<space><verb>`. Without this the scan reported `-config` as a
    /// route. The left boundary keeps the plain identifier rule, so a
    /// hyphenated word ENDING in "rocky" cannot hide a route behind it.
    ///
    /// Backtick-agnostic and, because the input is lowercased, also
    /// case-insensitive. Those are exactly the two holes the eleventh
    /// round's finding 3 named in the literal `` "`rocky " `` scan this
    /// replaces.
    fn rocky_followers(lower: &str) -> Vec<&str> {
        let bytes = lower.as_bytes();
        let is_ident = |b: u8| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_';
        let mut out = Vec::new();
        let mut from = 0;
        while let Some(offset) = lower[from..].find("rocky") {
            let start = from + offset;
            let end = start + "rocky".len();
            from = start + 1;
            let before_ok = start == 0 || !is_ident(bytes[start - 1]);
            let after_ok = end == bytes.len() || !(is_ident(bytes[end]) || bytes[end] == b'-');
            if !before_ok || !after_ok {
                continue;
            }
            let rest = &lower[end..];
            let skipped = rest
                .find(|c: char| !matches!(c, ' ' | '\t' | '\n' | '\r' | '`' | '*'))
                .unwrap_or(rest.len());
            let rest = &rest[skipped..];
            let word_end = rest
                .find(|c: char| !(c.is_ascii_lowercase() || c == '-'))
                .unwrap_or(rest.len());
            if word_end > 0 {
                out.push(&rest[..word_end]);
            }
        }
        out
    }

    #[test]
    fn render_cell_passes_short_strings_through() {
        assert_eq!(
            render_cell(serde_json::Value::String("hello".into())),
            "hello"
        );
        assert_eq!(render_cell(serde_json::Value::Null), "NULL");
        assert_eq!(render_cell(serde_json::json!(42)), "42");
    }

    #[test]
    fn render_cell_truncates_long_strings_with_ellipsis() {
        let long = "a".repeat(CELL_MAX_CHARS + 100);
        let out = render_cell(serde_json::Value::String(long));
        // Truncated to the cap plus a single ellipsis char.
        assert_eq!(out.chars().count(), CELL_MAX_CHARS + 1);
        assert!(out.ends_with('…'));
    }

    #[test]
    fn caps_are_within_spec() {
        // Hard caps from the tool spec.
        assert_eq!(SAMPLE_MAX_ROWS, 50);
        assert_eq!(SAMPLE_MAX_BYTES, 16 * 1024);
        assert_eq!(CELL_MAX_CHARS, 256);
    }

    #[test]
    fn parse_target_dialect_accepts_known_values_case_insensitively() {
        use rocky_sql::transpile::Dialect;
        // `Json<ToolError>` is not `Debug`, so match rather than `.expect()`.
        let ok = |s: &str| match parse_target_dialect(s) {
            Ok(d) => d,
            Err(_) => panic!("'{s}' should parse to a known dialect"),
        };
        assert_eq!(ok("bigquery"), Dialect::BigQuery);
        assert_eq!(ok("BigQuery"), Dialect::BigQuery);
        assert_eq!(ok(" snowflake "), Dialect::Snowflake);
        assert_eq!(ok("DATABRICKS"), Dialect::Databricks);
        assert_eq!(ok("duckdb"), Dialect::DuckDB);
    }

    #[test]
    fn parse_target_dialect_rejects_unknown_value() {
        let err = parse_target_dialect("redshift").expect_err("unknown dialect must error");
        // The failure is the structured envelope: an invalid_argument code, the
        // offending value in the message, and the accepted set in the hint.
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("redshift"),
            "message should name the input: {:?}",
            err.0
        );
        assert!(
            err.0.remediation_hint.contains("bigquery"),
            "hint should list the accepted values: {:?}",
            err.0
        );
    }

    /// Egress contract: the `ai_contract` profiler issues STATISTICS only —
    /// it must never select raw cell values (`MIN`/`MAX`) nor a domain sample,
    /// matching the default of the `rocky ai-contract` generator it wraps.
    #[test]
    fn column_stats_sql_sends_no_raw_cell_values() {
        let sql = column_stats_sql("out.orders", "status");
        assert!(
            sql.contains("COUNT(DISTINCT status)"),
            "distinct COUNT is a statistic and is expected: {sql}"
        );
        let upper = sql.to_uppercase();
        assert!(
            !upper.contains("MIN(") && !upper.contains("MAX("),
            "statistics-only query must not select MIN/MAX: {sql}"
        );
        assert!(
            !upper.contains("DISTINCT CAST"),
            "statistics-only query must not issue the domain-values query: {sql}"
        );
    }

    #[test]
    fn json_as_u64_handles_null_number_and_string() {
        assert_eq!(json_as_u64(&serde_json::json!(42)), 42);
        assert_eq!(json_as_u64(&serde_json::json!("17")), 17);
        assert_eq!(json_as_u64(&serde_json::json!(null)), 0);
        assert_eq!(json_as_u64(&serde_json::json!("nope")), 0);
    }

    #[test]
    fn server_resolves_models_dir_beside_config() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        assert_eq!(server.models_dir, PathBuf::from("/tmp/proj/models"));
        assert_eq!(server.root, PathBuf::from("/tmp/proj"));
    }

    /// `plan_preview` with an unknown model must surface the stable
    /// `model_not_found` error class (with its "list the models, retry" hint),
    /// not the generic `compile_failed` bucket — so an agent branches correctly.
    #[tokio::test]
    async fn plan_preview_unknown_model_is_model_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"duckdb\"\ndatabase = \":memory:\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        // `Json<PlanPreviewResult>` is not `Debug`, so match rather than `expect_err`.
        let err = match server
            .plan_preview(Parameters(PlanPreviewArgs {
                model: Some("missing".into()),
            }))
            .await
        {
            Ok(_) => panic!("unknown model must error"),
            Err(e) => e,
        };
        assert_eq!(err.0.code, crate::error::ToolErrorCode::ModelNotFound);
        assert!(
            err.0.message.contains("missing"),
            "message should name the model: {:?}",
            err.0
        );
    }

    /// A warehouse Rocky cannot reach must say so, not return an empty list.
    ///
    /// `inspect_schema` is the grounding tool. An agent reading empty `sources`
    /// at cold start concludes there is nothing to ground against — which is
    /// correct only if discovery actually ran (#1533).
    #[tokio::test]
    async fn inspect_schema_reports_discovery_it_could_not_run() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        // A Databricks target with no credentials: the adapter does not resolve,
        // so discovery cannot run.
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"databricks\"\n\
             \n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             \n[pipeline.p.target]\nadapter = \"default\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        let result = server
            .inspect_schema(Parameters(InspectSchemaArgs {}))
            .await
            .map_err(|_| "inspect_schema returned an error")
            .expect("inspect_schema must still succeed — models are exact")
            .0;

        assert!(
            result.discovery_incomplete,
            "discovery could not run, so it must be reported, not returned as an empty list"
        );
        let why = result
            .discovery_error
            .expect("an incomplete discovery must say why");
        // Must fail on the ADAPTER, not on a malformed fixture. Both produce an
        // error, so without this the test would pass on a typo in the config
        // above and prove nothing about the path it claims to cover.
        assert!(
            !why.contains("parse TOML") && !why.contains("no pipelines"),
            "the fixture is broken, so this is not testing adapter resolution: {why}"
        );
        // The exact half is unaffected.
        assert!(
            result.models.iter().any(|m| m.name == "known"),
            "compile-derived models must still be reported: {:?}",
            result.models.iter().map(|m| &m.name).collect::<Vec<_>>()
        );
    }

    /// A warehouse adapter whose every query fails — enough to exercise the
    /// discovery arm without a live warehouse.
    struct FailingAdapter;

    #[async_trait::async_trait]
    impl rocky_core::traits::WarehouseAdapter for FailingAdapter {
        fn dialect(&self) -> &dyn rocky_core::traits::SqlDialect {
            unimplemented!("not reached: discovery fails at execute_query")
        }
        async fn execute_statement(&self, _sql: &str) -> rocky_core::traits::AdapterResult<()> {
            unimplemented!("not reached: discovery only queries")
        }
        async fn execute_query(
            &self,
            _sql: &str,
        ) -> rocky_core::traits::AdapterResult<rocky_core::traits::QueryResult> {
            Err(rocky_core::traits::AdapterError::msg(
                "warehouse unreachable",
            ))
        }
        async fn describe_table(
            &self,
            _table: &rocky_ir::TableRef,
        ) -> rocky_core::traits::AdapterResult<Vec<rocky_ir::ColumnInfo>> {
            unimplemented!("not reached: discovery only queries")
        }
    }

    /// The OTHER swallow point: the adapter resolves, but the discovery query
    /// itself fails. `discover_source_tables` used to return an empty `Vec`
    /// here — the same silence from a different cause (#1533).
    ///
    /// Mutation-checking found this gap: the adapter-resolution test stayed
    /// green when this arm alone was reverted to swallowing.
    #[tokio::test]
    async fn discover_source_tables_reports_a_failed_query() {
        let err = discover_source_tables(&FailingAdapter)
            .await
            .expect_err("a failed discovery query must be reported, not returned as empty");
        assert!(
            err.contains("warehouse unreachable"),
            "the reason must reach the caller: {err}"
        );
    }

    /// The positive control: when discovery DOES run, nothing is flagged.
    /// Without this, the field could be hardcoded true and the test above
    /// would still pass.
    #[tokio::test]
    async fn inspect_schema_flags_nothing_when_discovery_runs() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"duckdb\"\ndatabase = \":memory:\"\n\
             \n[pipeline.p]\ntype = \"transformation\"\nmodels = \"models/**\"\n\
             \n[pipeline.p.target]\nadapter = \"default\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        let result = server
            .inspect_schema(Parameters(InspectSchemaArgs {}))
            .await
            .map_err(|_| "inspect_schema returned an error")
            .expect("inspect_schema")
            .0;

        assert!(
            !result.discovery_incomplete,
            "discovery ran against in-memory DuckDB, so nothing should be flagged: {:?}",
            result.discovery_error
        );
        assert!(result.discovery_error.is_none());
    }

    #[tokio::test]
    async fn compile_unknown_model_is_model_not_found() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        std::fs::write(
            root.join("rocky.toml"),
            "[adapter.default]\ntype = \"duckdb\"\ndatabase = \":memory:\"\n",
        )
        .expect("write config");
        let models = root.join("models");
        std::fs::create_dir(&models).expect("create models");
        std::fs::write(models.join("known.sql"), "SELECT 1 AS id").expect("write sql");
        std::fs::write(
            models.join("known.toml"),
            "name = \"known\"\n\n[strategy]\ntype = \"full_refresh\"\n\n[target]\ncatalog = \"c\"\nschema = \"s\"\ntable = \"known\"\n",
        )
        .expect("write sidecar");

        let server = RockyMcpServer::new(root.join("rocky.toml"));
        let err = match server
            .compile(Parameters(CompileArgs {
                model: Some("missing".into()),
                target_dialect: None,
            }))
            .await
        {
            Ok(_) => panic!("unknown model must error"),
            Err(e) => e,
        };
        assert_eq!(err.0.code, crate::error::ToolErrorCode::ModelNotFound);
        assert!(err.0.message.contains("missing"));
    }

    #[test]
    fn resolve_draft_paths_accepts_a_bare_name_and_refuses_traversal() {
        let server = RockyMcpServer::new(PathBuf::from("/tmp/proj/rocky.toml"));
        let Ok(ok) = server.resolve_draft_paths("completed_revenue") else {
            panic!("a bare name should resolve");
        };
        assert_eq!(ok.stem, "completed_revenue");
        assert_eq!(
            ok.sql_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.sql")
        );
        assert_eq!(
            ok.sidecar_path,
            PathBuf::from("/tmp/proj/models/completed_revenue.toml")
        );
        for bad in [
            "../evil",
            "/etc/passwd",
            "sub/model",
            "..\\win",
            "revenue.sql",
            "..",
            "",
        ] {
            assert!(
                server.resolve_draft_paths(bad).is_err(),
                "name '{bad}' must be refused as a path-escape / non-bare name"
            );
        }
    }

    #[test]
    fn draft_sidecar_toml_escapes_the_intent() {
        let sidecar = draft_sidecar("orders", "revenue for \"COMPLETE\" orders\nline two");
        assert!(sidecar.contains("name = \"orders\""));
        // Quotes and newlines in the intent are TOML-escaped so an arbitrary
        // intent embeds as a valid TOML basic string.
        assert!(sidecar.contains("intent = \"revenue for \\\"COMPLETE\\\" orders\\nline two\""));
        // An empty intent omits the key entirely (still a valid sidecar).
        let empty = draft_sidecar("orders", "");
        assert!(empty.contains("name = \"orders\""));
        assert!(
            !empty.contains("intent ="),
            "empty intent omits the intent key"
        );
    }

    #[test]
    fn ensure_trailing_newline_normalizes() {
        assert_eq!(ensure_trailing_newline("SELECT 1"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n"), "SELECT 1\n");
        assert_eq!(ensure_trailing_newline("SELECT 1\n\n"), "SELECT 1\n");
    }

    // --- DraftRollback (panic-safe draft rollback) ---

    /// The drop-guard contract under a PANIC between the write and the
    /// verdict (e.g. inside compile): unwinding drops the guard, which
    /// restores the pre-existing file byte-for-byte and removes the fresh
    /// artifact — "a denied draft leaves NO file" holds even when no error
    /// arm ever ran.
    #[test]
    fn draft_rollback_restores_on_panic() {
        let dir = tempfile::tempdir().unwrap();
        let existing = dir.path().join("model.toml");
        std::fs::write(&existing, "original").unwrap();
        let fresh = dir.path().join("fresh.sql");

        let guard = DraftRollback::snapshot([&existing, &fresh]).expect("snapshot");
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = guard;
            std::fs::write(&existing, "clobbered").unwrap();
            std::fs::write(&fresh, "new artifact").unwrap();
            panic!("simulated panic between the write and the policy verdict");
        }));
        assert!(unwound.is_err(), "the panic propagates to the caller");

        assert_eq!(
            std::fs::read_to_string(&existing).unwrap(),
            "original",
            "a pre-existing file is restored byte-for-byte"
        );
        assert!(!fresh.exists(), "a freshly written draft is removed");
    }

    /// A plain (non-panic) drop without `defuse` — the shape every `?` /
    /// early-`return Err` path takes — restores exactly like the panic path.
    #[test]
    fn draft_rollback_restores_on_err_return_drop() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh.sql");
        {
            let _guard = DraftRollback::snapshot([&fresh]).expect("snapshot");
            std::fs::write(&fresh, "draft body").unwrap();
            // The guard drops here without defuse, as on an Err return.
        }
        assert!(!fresh.exists(), "the un-defused drop rolls the write back");
    }

    /// `defuse` is the keep path (success / require-review): the write
    /// persists. Also pins the `prior` accessor `draft_check` merges with.
    #[test]
    fn draft_rollback_defused_keeps_the_write() {
        let dir = tempfile::tempdir().unwrap();
        let sidecar = dir.path().join("model.toml");
        std::fs::write(&sidecar, "name = \"m\"").unwrap();

        let guard = DraftRollback::snapshot([&sidecar]).expect("snapshot");
        assert_eq!(
            guard.prior(&sidecar),
            Some("name = \"m\"".as_bytes()),
            "the snapshot exposes the prior bytes for the merge"
        );
        assert_eq!(
            guard.prior(&dir.path().join("other.toml")),
            None,
            "an unsnapshotted path has no prior"
        );

        std::fs::write(&sidecar, "name = \"m\"\n\n[[tests]]\n").unwrap();
        guard.defuse();
        assert_eq!(
            std::fs::read_to_string(&sidecar).unwrap(),
            "name = \"m\"\n\n[[tests]]\n",
            "a defused guard keeps the draft"
        );
    }

    /// The clean half of the refusal disposition (#1561): every path
    /// restored → the arm's claim comes back verbatim (the refusal message
    /// is byte-identical to before the fix) and no machine-readable paths
    /// ride the envelope.
    #[test]
    fn rollback_disposition_clean_keeps_the_claim() {
        let dir = tempfile::tempdir().unwrap();
        let fresh = dir.path().join("fresh.sql");
        let guard = DraftRollback::snapshot([&fresh]).expect("snapshot");
        std::fs::write(&fresh, "draft body").unwrap();

        let (disposition, failed) = rollback_disposition(
            dir.path(),
            guard,
            "so the draft was not kept.",
            "but rolling it back FAILED",
        );
        assert_eq!(disposition, "so the draft was not kept.");
        assert_eq!(failed, None);
        assert!(!fresh.exists(), "the clean rollback removed the draft");
    }

    /// #1561 fault injection, the new-artifact + deny shape: `remove_file`
    /// cannot clean the fresh draft up, so the refusal disposition names the
    /// leftover path and says it could not be removed — never the clean
    /// "was not kept" claim. The fault: the snapshotted path is occupied by
    /// a non-empty directory at rollback time, which `remove_file` refuses
    /// for every user (the read-only-directory fault in the sibling test
    /// does not fire for root, and local runs may be root).
    #[test]
    fn draft_rollback_failure_is_reported_and_names_the_path() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let fresh = root.join("models").join("shadow.sql");
        std::fs::create_dir_all(fresh.parent().unwrap()).unwrap();

        let guard = DraftRollback::snapshot([&fresh]).expect("snapshot");
        std::fs::create_dir_all(fresh.join("occupied")).unwrap();

        let (disposition, failed) = rollback_disposition(
            root,
            guard,
            "so the draft was not kept.",
            "but rolling it back FAILED",
        );
        assert_eq!(failed, Some(vec!["models/shadow.sql".to_string()]));
        assert!(
            disposition.contains("models/shadow.sql could not be removed"),
            "the refusal names the leftover artifact: {disposition}"
        );
        assert!(
            disposition.contains("STILL ON DISK"),
            "the refusal says the artifact remains: {disposition}"
        );
        assert!(
            !disposition.contains("not kept"),
            "a failed cleanup must not claim the draft was removed: {disposition}"
        );
        assert!(fresh.exists(), "the artifact really is still on disk");
    }

    /// #1561 fault injection with the real permission fault: a read-only
    /// models directory refuses the unlink, and `rollback` reports the
    /// artifact with the I/O cause. Root bypasses directory permissions
    /// (CAP_DAC_OVERRIDE), so the test probes that the fault arms and
    /// stands down when it cannot — the sibling test above injects a fault
    /// that fires for every user. Permissions are restored afterwards so
    /// the tempdir cleans up.
    #[cfg(unix)]
    #[test]
    fn draft_rollback_reports_a_read_only_directory() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir(&models).unwrap();
        let fresh = models.join("shadow.sql");
        let probe = models.join("probe");

        let guard = DraftRollback::snapshot([&fresh]).expect("snapshot");
        std::fs::write(&fresh, "SELECT 1 AS id").unwrap();
        std::fs::write(&probe, "x").unwrap();
        std::fs::set_permissions(&models, std::fs::Permissions::from_mode(0o555)).unwrap();
        if std::fs::remove_file(&probe).is_ok() {
            // This process bypasses the permission check; the fault cannot
            // arm here.
            std::fs::set_permissions(&models, std::fs::Permissions::from_mode(0o755)).unwrap();
            guard.defuse();
            return;
        }

        let failures = guard.rollback();
        std::fs::set_permissions(&models, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert_eq!(failures.len(), 1, "the failed unlink is reported");
        assert_eq!(failures[0].path, fresh);
        assert_eq!(failures[0].arm, RollbackArm::Remove);
        assert!(fresh.exists(), "the artifact really is still on disk");
    }

    /// The snapshot refuses a path that EXISTS but cannot be read, instead of
    /// recording it as absent — the shape that had the rollback DELETE a
    /// user's file (the remove arm, for a path with "no prior"). A directory
    /// at the path is the root-proof way to make the read fail; an absent
    /// sibling still snapshots as `None`.
    #[test]
    fn draft_rollback_snapshot_refuses_an_existing_path_it_cannot_read() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let occupied = root.join("models").join("orders.sql");
        std::fs::create_dir_all(&occupied).unwrap();
        let absent = root.join("models").join("fresh.sql");

        let refused = DraftRollback::snapshot([&absent, &occupied])
            .err()
            .expect("an existing path that cannot be read refuses the snapshot");
        assert_eq!(refused.path, occupied);
        let err = refused.into_tool_error(root);
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("models/orders.sql") && err.0.message.contains("cannot be read"),
            "the refusal names the path: {}",
            err.0.message
        );
        assert!(occupied.is_dir(), "nothing touched the path");

        let guard = DraftRollback::snapshot([&absent]).expect("an absent path is a None prior");
        assert_eq!(guard.prior(&absent), None);
        guard.defuse();
    }

    /// The snapshot's read is guarded at the DESCRIPTOR, like every draft
    /// write: a leaf swapped for a symlink AFTER the up-front check — the
    /// window `resolve_draft_paths` cannot close, because a path syscall
    /// re-traverses the path — refuses instead of being read through. Before
    /// the fix `std::fs::read` followed it, so a file OUTSIDE the project
    /// became the "prior content" that the merge folds into the user's
    /// sidecar and the rollback would restore.
    #[cfg(unix)]
    #[test]
    fn draft_rollback_snapshot_refuses_a_leaf_swapped_for_a_symlink_out_of_the_project() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("proj");
        let models = root.join("models");
        std::fs::create_dir_all(&models).unwrap();
        let outside = dir.path().join("outside.toml");
        std::fs::write(&outside, "marker = \"OUTSIDE-SECRET\"\n").unwrap();
        let sidecar = models.join("orders.toml");
        std::os::unix::fs::symlink(&outside, &sidecar).unwrap();

        let refused = DraftRollback::snapshot([&sidecar])
            .err()
            .expect("a symlinked leaf refuses the snapshot instead of being read through");
        assert_eq!(refused.path, sidecar);
        let err = refused.into_tool_error(&root);
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("models/orders.toml")
                && err.0.message.contains("cannot be read"),
            "the refusal names the path: {}",
            err.0.message
        );
        assert!(
            !err.0.message.contains("OUTSIDE-SECRET"),
            "the outside file's content never reaches the caller: {}",
            err.0.message
        );
        assert_eq!(
            std::fs::read_to_string(&outside).unwrap(),
            "marker = \"OUTSIDE-SECRET\"\n",
            "the file outside the project is untouched"
        );
    }

    /// A FIFO swapped in at a snapshot path is refused, and refused without
    /// blocking. Before the fix the snapshot's `std::fs::read` opened it and
    /// waited for a writer that never comes, parking the draft request
    /// forever. The read now carries `O_NONBLOCK` with `O_NOFOLLOW` and
    /// checks the descriptor. The timeout turns a regression into a failure
    /// instead of a hung suite.
    #[cfg(unix)]
    #[test]
    fn draft_rollback_snapshot_refuses_a_fifo_instead_of_blocking() {
        let dir = tempfile::tempdir().unwrap();
        let models = dir.path().join("models");
        std::fs::create_dir_all(&models).unwrap();
        let sidecar = models.join("orders.toml");
        make_fifo(&sidecar);

        let (tx, rx) = std::sync::mpsc::channel();
        let probe = sidecar.clone();
        std::thread::spawn(move || {
            let refused = match DraftRollback::snapshot([&probe]) {
                Ok(guard) => {
                    guard.defuse();
                    false
                }
                Err(_) => true,
            };
            let _ = tx.send(refused);
        });
        let refused = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("the snapshot returned instead of parking on the FIFO");
        assert!(refused, "a FIFO at a snapshot path refuses");
        assert!(
            std::fs::symlink_metadata(&sidecar).is_ok(),
            "the FIFO is left in place, neither read nor replaced"
        );
    }

    /// A failed restore whose target is GONE says so: the prior file was
    /// removed and its parent went with it, so `std::fs::write` cannot put
    /// the bytes back and there is nothing on disk to "remove". Before the
    /// fix the refusal asserted "STILL ON DISK" for every failure.
    #[test]
    fn rollback_disposition_says_the_path_is_absent_when_the_restore_target_is_gone() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models = root.join("models");
        std::fs::create_dir_all(&models).unwrap();
        let sidecar = models.join("orders.toml");
        std::fs::write(&sidecar, "name = \"orders\"\n").unwrap();

        let guard = DraftRollback::snapshot([&sidecar]).expect("snapshot");
        std::fs::remove_dir_all(&models).unwrap();

        let (disposition, failed) = rollback_disposition(
            root,
            guard,
            "so the check was not kept.",
            "but rolling it back FAILED",
        );
        assert_eq!(failed, Some(vec!["models/orders.toml".to_string()]));
        assert!(
            disposition.contains("models/orders.toml could not be restored to its prior content"),
            "the refusal names the path and what failed: {disposition}"
        );
        assert!(
            disposition.contains(
                "the prior content of models/orders.toml could not be restored and the path is \
                 now absent"
            ),
            "the refusal says the path is gone: {disposition}"
        );
        assert!(
            !disposition.contains("STILL ON DISK"),
            "nothing is on disk; the refusal must not claim otherwise: {disposition}"
        );
        assert!(
            !disposition.contains("not kept"),
            "a failed rollback must not read as a clean refusal: {disposition}"
        );
        assert!(
            std::fs::symlink_metadata(&sidecar).is_err(),
            "the path really is absent"
        );
    }

    /// Mixed outcome: a path still on disk and a path now gone are each
    /// reported in their own words, and both ride `rollback_failed_paths`.
    #[test]
    fn rollback_disposition_reports_on_disk_and_absent_paths_separately() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let leftover = root.join("models").join("shadow.sql");
        std::fs::create_dir_all(leftover.parent().unwrap()).unwrap();
        let gone = root.join("elsewhere").join("orders.toml");
        std::fs::create_dir_all(gone.parent().unwrap()).unwrap();
        std::fs::write(&gone, "name = \"orders\"\n").unwrap();

        let guard = DraftRollback::snapshot([&leftover, &gone]).expect("snapshot");
        std::fs::create_dir_all(leftover.join("occupied")).unwrap();
        std::fs::remove_dir_all(gone.parent().unwrap()).unwrap();

        let (disposition, failed) =
            rollback_disposition(root, guard, "clean", "but rolling it back FAILED");
        assert_eq!(
            failed,
            Some(vec![
                "models/shadow.sql".to_string(),
                "elsewhere/orders.toml".to_string()
            ])
        );
        assert!(
            disposition.contains("models/shadow.sql could not be removed")
                && disposition.contains("STILL ON DISK"),
            "the leftover is reported as on disk: {disposition}"
        );
        assert!(
            disposition.contains(
                "the prior content of elsewhere/orders.toml could not be restored and the path \
                 is now absent"
            ),
            "the missing file is reported as absent: {disposition}"
        );
    }

    /// Put a directory's mode back on every exit path, so the tempdir cleans
    /// up and a byte comparison can read it again.
    #[cfg(unix)]
    struct RestoreMode(PathBuf, u32);

    #[cfg(unix)]
    impl Drop for RestoreMode {
        fn drop(&mut self) {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(&self.0, std::fs::Permissions::from_mode(self.1));
        }
    }

    /// Create a FIFO at `path`.
    #[cfg(unix)]
    fn make_fifo(path: &Path) {
        let status = std::process::Command::new("mkfifo")
            .arg(path)
            .status()
            .expect("mkfifo runs");
        assert!(status.success(), "mkfifo {}", path.display());
    }

    /// The third disposition state: the leftover could not even be
    /// inspected. The parent is unsearchable (mode 000), so the unlink fails
    /// AND the after-the-fact `symlink_metadata` fails; before the fix any
    /// inspection error read as "absent". Root ignores directory modes, so
    /// the fault probes itself and stands down when it cannot arm.
    #[cfg(unix)]
    #[test]
    fn rollback_disposition_says_the_leftover_could_not_be_inspected() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models = root.join("models");
        std::fs::create_dir(&models).unwrap();
        let fresh = models.join("shadow.sql");

        let guard = DraftRollback::snapshot([&fresh]).expect("snapshot");
        std::fs::write(&fresh, "SELECT 1 AS id").unwrap();
        std::fs::set_permissions(&models, std::fs::Permissions::from_mode(0o000)).unwrap();
        let restore = RestoreMode(models.clone(), 0o755);
        if std::fs::symlink_metadata(&fresh).is_ok() {
            // This process ignores the directory mode; the fault cannot arm.
            drop(restore);
            guard.defuse();
            return;
        }

        let (disposition, failed) = rollback_disposition(
            root,
            guard,
            "so the draft was not kept.",
            "but rolling it back FAILED",
        );
        drop(restore);
        assert_eq!(failed, Some(vec!["models/shadow.sql".to_string()]));
        assert!(
            disposition.contains("models/shadow.sql could not be removed"),
            "the refusal names the path and what failed: {disposition}"
        );
        assert!(
            disposition
                .contains("models/shadow.sql could not be inspected after the failed rollback")
                && disposition.contains("Permission denied"),
            "the refusal says the check itself failed, and why: {disposition}"
        );
        assert!(
            !disposition.contains("STILL ON DISK") && !disposition.contains("now absent"),
            "an uninspectable path is claimed neither present nor absent: {disposition}"
        );
        assert!(
            fresh.exists(),
            "the artifact really is still there once the directory is searchable again"
        );
    }

    /// The restore arm meets a link swapped in at the leaf after the
    /// snapshot — the race the up-front check cannot close. `std::fs::write`
    /// followed it and put the PRIOR bytes at the link's target, outside the
    /// models directory, while the refusal claimed a clean rollback. The
    /// no-follow restore fails instead: the target is untouched, the
    /// disposition names the path, and it says a symlink is what is on disk.
    #[cfg(unix)]
    #[test]
    fn rollback_disposition_never_restores_through_a_swapped_in_symlink() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models = root.join("models");
        std::fs::create_dir(&models).unwrap();
        let sidecar = models.join("orders.toml");
        std::fs::write(&sidecar, "name = \"orders\"\n").unwrap();
        let outside = tempfile::tempdir().unwrap();
        let leak = outside.path().join("leak.toml");

        let guard = DraftRollback::snapshot([&sidecar]).expect("snapshot");
        std::fs::write(&sidecar, "name = \"orders\"\nintent = \"draft\"\n").unwrap();
        std::fs::remove_file(&sidecar).unwrap();
        std::os::unix::fs::symlink(&leak, &sidecar).unwrap();

        let (disposition, failed) = rollback_disposition(
            root,
            guard,
            "so the draft was not kept.",
            "but rolling it back FAILED",
        );
        assert!(
            !leak.exists(),
            "the prior bytes were written through the link to {}",
            leak.display()
        );
        assert_eq!(failed, Some(vec!["models/orders.toml".to_string()]));
        assert!(
            disposition.contains("models/orders.toml could not be restored to its prior content"),
            "the refusal names the path and what failed: {disposition}"
        );
        assert!(
            disposition.contains("models/orders.toml is STILL ON DISK (a symlink)"),
            "the refusal says a link is what is on disk: {disposition}"
        );
        assert!(
            !disposition.contains("not kept"),
            "a failed rollback must not read as a clean refusal: {disposition}"
        );
        assert!(
            std::fs::symlink_metadata(&sidecar)
                .unwrap()
                .file_type()
                .is_symlink(),
            "the swapped-in link is left for the operator to see, not replaced"
        );
    }

    /// Every draft write and the rollback's restore go through the no-follow
    /// helper — this guards the WIRE; `rocky-core` proves the helper. The
    /// banned and required strings are assembled at runtime so this test's
    /// own source cannot satisfy the search it performs.
    #[test]
    fn every_draft_write_and_the_restore_use_the_no_follow_helper() {
        let source = include_str!("tools.rs");
        let banned = format!("std::fs::{}(&paths.", "write");
        assert!(
            !source.contains(&banned),
            "a draft write uses a plain std::fs::write, which follows a link swapped in at \
             the leaf"
        );
        let banned = format!("Some(bytes) => std::fs::{}(path, bytes)", "write");
        assert!(
            !source.contains(&banned),
            "the rollback restore uses a plain std::fs::write"
        );
        let required = format!("write_no_{}(&paths.", "follow");
        assert_eq!(
            source.matches(&required).count(),
            5,
            "the five draft write sites (draft_model SQL + sidecar, draft_contract, \
             draft_check, draft_metadata) call write_no_follow"
        );
        let required = format!("Some(bytes) => write_no_{}(path, bytes)", "follow");
        assert!(
            source.contains(&required),
            "the rollback restore calls write_no_follow"
        );
        let banned = format!("match std::fs::{}(&path)", "read");
        assert!(
            !source.contains(&banned),
            "the snapshot reads a prior with a plain std::fs::read, which follows a link \
             swapped in at the leaf and parks on a FIFO"
        );
        let required = format!("read_no_follow_{}(&path)", "bytes");
        assert!(
            source.contains(&required),
            "the snapshot reads each prior through read_no_follow_bytes"
        );
    }

    /// The ancestor guard: `models/` resolving outside the project root
    /// refuses, naming both ends; a link that stays inside the root, a real
    /// directory, and a models directory that does not exist yet pass; a
    /// dangling link where the models directory would go refuses.
    #[cfg(unix)]
    #[test]
    fn refuse_redirected_models_dir_refuses_a_models_link_that_leaves_the_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("proj");
        std::fs::create_dir(&root).unwrap();
        let server = RockyMcpServer::new(root.join("rocky.toml"));
        let models = root.join("models");

        assert!(
            server.refuse_redirected_models_dir().is_ok(),
            "an absent models directory has nothing to redirect"
        );

        let outside = dir.path().join("elsewhere");
        std::fs::create_dir(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, &models).unwrap();
        let err = server
            .refuse_redirected_models_dir()
            .expect_err("a models link that leaves the root refuses");
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        let resolved_outside = outside.canonicalize().unwrap().display().to_string();
        let resolved_root = root.canonicalize().unwrap().display().to_string();
        assert!(
            err.0.message.contains("models/ resolves to")
                && err.0.message.contains(&resolved_outside)
                && err.0.message.contains(&resolved_root),
            "the refusal names where models/ resolves and the root it left: {}",
            err.0.message
        );

        std::fs::remove_file(&models).unwrap();
        std::os::unix::fs::symlink(root.join("gone"), &models).unwrap();
        let err = server
            .refuse_redirected_models_dir()
            .expect_err("a dangling models link refuses");
        assert!(
            err.0.message.contains("target does not exist"),
            "the refusal says the link dangles: {}",
            err.0.message
        );

        std::fs::remove_file(&models).unwrap();
        let inside = root.join("src").join("models_real");
        std::fs::create_dir_all(&inside).unwrap();
        std::os::unix::fs::symlink(&inside, &models).unwrap();
        assert!(
            server.refuse_redirected_models_dir().is_ok(),
            "a models link that stays inside the root passes"
        );

        std::fs::remove_file(&models).unwrap();
        std::fs::create_dir(&models).unwrap();
        assert!(
            server.refuse_redirected_models_dir().is_ok(),
            "a real models directory passes"
        );
    }

    /// The up-front leaf guard: a symlink at a draft path refuses — dangling
    /// or resolving — naming the path, and so does any other non-regular
    /// file (a directory, a FIFO — which would park the snapshot's read); a
    /// regular file and an absent path pass.
    #[cfg(unix)]
    #[test]
    fn refuse_non_regular_draft_target_refuses_links_and_non_regular_files() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let models = root.join("models");
        std::fs::create_dir_all(&models).unwrap();
        let regular = models.join("orders.sql");
        std::fs::write(&regular, "SELECT 1 AS id\n").unwrap();
        let dangling = models.join("shadow.sql");
        std::os::unix::fs::symlink(root.join("nowhere.sql"), &dangling).unwrap();
        let resolving = models.join("alias.sql");
        std::os::unix::fs::symlink(&regular, &resolving).unwrap();
        let directory = models.join("nested.sql");
        std::fs::create_dir(&directory).unwrap();
        let fifo = models.join("pipe.sql");
        make_fifo(&fifo);

        assert!(refuse_non_regular_draft_target(root, &regular).is_ok());
        assert!(refuse_non_regular_draft_target(root, &models.join("absent.sql")).is_ok());
        for link in [&dangling, &resolving] {
            let err = refuse_non_regular_draft_target(root, link).expect_err("a symlink refuses");
            assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
            assert!(
                err.0.message.contains(&rel_display(root, link))
                    && err.0.message.contains("symlink"),
                "the refusal names the link: {}",
                err.0.message
            );
        }
        for (path, kind) in [(&directory, "directory"), (&fifo, "FIFO")] {
            let err = refuse_non_regular_draft_target(root, path)
                .expect_err("a non-regular file refuses");
            assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
            assert!(
                err.0.message.contains(&rel_display(root, path))
                    && err.0.message.contains(kind)
                    && err.0.message.contains("not a regular file"),
                "the refusal names the path and its kind: {}",
                err.0.message
            );
        }
    }

    // --- validate_check_spec (draft_check structural gate) ---

    /// A `[target]` (or any non-`tests`) table smuggled alongside a valid
    /// `[[tests]]` block is rejected with a structured `invalid_argument`
    /// naming the offending key — the check write path cannot override model
    /// config.
    #[test]
    fn check_spec_rejects_smuggled_config_tables() {
        let spec = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n\n\
                    [target]\nschema = \"prod\"\n";
        let err = validate_check_spec(spec).expect_err("a [target] override must be rejected");
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(
            err.0.message.contains("`target`"),
            "the offending key is named: {}",
            err.0.message
        );

        // A bare top-level key BEFORE the first [[tests]] header would attach
        // to the prior sidecar's last table when appended — same rejection.
        let spec = "path = \"evil\"\n\n[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
        let err = validate_check_spec(spec).expect_err("a bare top-level key must be rejected");
        assert!(
            err.0.message.contains("`path`"),
            "the offending key is named: {}",
            err.0.message
        );

        // `[strategy]` is config, exactly like `[target]`.
        let spec = "[[tests]]\ntype = \"unique\"\ncolumn = \"id\"\n\n\
                    [strategy]\ntype = \"full_refresh\"\n";
        assert!(validate_check_spec(spec).is_err());
    }

    /// A pure `[[tests]]` spec (one or many blocks) passes the gate.
    #[test]
    fn check_spec_accepts_pure_tests_blocks() {
        let single = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n";
        assert!(validate_check_spec(single).is_ok());

        let many = "[[tests]]\ntype = \"not_null\"\ncolumn = \"id\"\n\n\
                    [[tests]]\ntype = \"accepted_values\"\ncolumn = \"status\"\n\
                    values = [\"COMPLETE\", \"PENDING\"]\n";
        assert!(validate_check_spec(many).is_ok());
    }

    /// Degenerate shapes: invalid TOML, and a `[tests]` TABLE (with the
    /// literal `[[tests]]` hidden inside a string so the substring pre-check
    /// passes) — both are structured `invalid_argument`s, not writes.
    #[test]
    fn check_spec_rejects_invalid_toml_and_non_array_tests() {
        let err = validate_check_spec("[[tests]\ntype =").expect_err("invalid TOML must fail");
        assert_eq!(err.0.code, crate::error::ToolErrorCode::InvalidArgument);
        assert!(err.0.message.contains("not valid TOML"));

        let table_form = "[tests]\nnote = \"[[tests]]\"\n";
        let err = validate_check_spec(table_form).expect_err("a `[tests]` table must fail");
        assert!(
            err.0.message.contains("array of tables"),
            "unexpected message: {}",
            err.0.message
        );
    }

    #[test]
    fn breaking_finding_lite_projects_column_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ColumnDropped {
                model: "c.s.orders".to_string(),
                column: "legacy_flag".to_string(),
                data_type: "String".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "column_dropped");
        assert_eq!(lite.severity, "breaking");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column.as_deref(), Some("legacy_flag"));
        assert!(lite.message.contains("ColumnDropped"));
    }

    #[test]
    fn ground_table_ref_default_emits_unquoted_segments() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        // The grounding path routes a parsed table ref through the target
        // dialect's `ground_table_ref`. The default (DuckDB/Snowflake/
        // Databricks) joins validated segments unquoted — Snowflake relies on
        // this to fold to its default uppercase casing rather than locking in
        // a case-sensitive quoted name.
        let d = DuckDbSqlDialect;
        // Three-part name (catalog.schema.table).
        assert_eq!(
            d.ground_table_ref(&["analytics", "raw", "orders"]).unwrap(),
            "analytics.raw.orders"
        );
        // Two-part name (schema.table).
        assert_eq!(
            d.ground_table_ref(&["raw", "orders"]).unwrap(),
            "raw.orders"
        );
    }

    #[test]
    fn ground_table_ref_default_rejects_bad_identifier_and_arity() {
        use rocky_core::traits::SqlDialect;
        use rocky_duckdb::dialect::DuckDbSqlDialect;
        let d = DuckDbSqlDialect;
        // Injection in any segment is rejected.
        assert!(
            d.ground_table_ref(&["raw", "orders; DROP TABLE x"])
                .is_err()
        );
        // A four-part ref (or a single bare name) is out of range.
        assert!(d.ground_table_ref(&["a", "b", "c", "d"]).is_err());
        assert!(d.ground_table_ref(&["orders"]).is_err());
    }

    #[test]
    fn record_batch_to_query_result_renders_null_as_json_null() {
        use std::sync::Arc;

        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;

        // A 2-row batch where row 1 is NULL in both columns. The default
        // `FormatOptions` renders NULL as "", so the converter MUST emit
        // `Value::Null` for those cells (checked via `is_null`), not "".
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
        ]));
        let ints = Int64Array::from(vec![Some(42), None]);
        let strs = StringArray::from(vec![Some("hello"), None]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(ints), Arc::new(strs)]).unwrap();

        let qr = record_batch_to_query_result(&batch).unwrap();
        assert_eq!(qr.columns, vec!["n".to_string(), "s".to_string()]);
        assert_eq!(qr.rows.len(), 2);
        // Row 0: non-null cells render to strings.
        assert_eq!(qr.rows[0][0], serde_json::Value::String("42".to_string()));
        assert_eq!(
            qr.rows[0][1],
            serde_json::Value::String("hello".to_string())
        );
        // Row 1: SQL NULL → JSON null, NOT the empty string.
        assert_eq!(qr.rows[1][0], serde_json::Value::Null);
        assert_eq!(qr.rows[1][1], serde_json::Value::Null);
    }

    #[test]
    fn breaking_finding_lite_omits_column_for_model_scoped_change() {
        use rocky_core::breaking_change::{BreakingChange, BreakingFinding, BreakingSeverity};
        let finding = BreakingFinding {
            change: BreakingChange::ModelRemoved {
                model: "c.s.orders".to_string(),
            },
            severity: BreakingSeverity::Breaking,
        };
        let lite = breaking_finding_lite(&finding);
        assert_eq!(lite.change, "model_removed");
        assert_eq!(lite.model, "c.s.orders");
        assert_eq!(lite.column, None);
    }

    // --- FF-WP1 fix round 2 (item 5) — worker-profile guidance surfaces ----

    /// Tools the worker profile does not serve — no worker-served guidance
    /// surface may name them (the instructions BANNER is the one deliberate
    /// exception: naming them as absent is its job).
    ///
    /// DERIVED from the two real routers, not hand-picked. It used to be a
    /// literal list, and that is exactly how `draft_check` slipped: this
    /// work package removed it from `WORKER_PROFILE_TOOLS` while
    /// `WORKER_DRAFT_NEXT_STEPS` still told the worker to call it, and the
    /// hand-picked list did not name it, so the sweep below went green over
    /// a message instructing a tool that answers tool-not-found. A list that
    /// has to be edited in lockstep with the allowlist is a list that will
    /// not be. `briefs.rs` already derives its twin the same way and for the
    /// same reason.
    fn worker_excluded_tool_mentions() -> Vec<String> {
        let served: std::collections::BTreeSet<String> = server_with(McpProfile::Worker)
            .tool_names()
            .into_iter()
            .collect();
        server_with(McpProfile::Default)
            .tool_names()
            .into_iter()
            .filter(|name| !served.contains(name))
            .collect()
    }

    fn server_with(profile: McpProfile) -> RockyMcpServer {
        // `get_info` and the routers never touch the filesystem, so an
        // arbitrary path is fine here.
        RockyMcpServer::new_with_profile(PathBuf::from("rocky.toml"), profile)
    }

    /// Every `paths:` list in a workflow, as its quoted entries. Enough YAML
    /// to check a trigger filter without taking a parser dependency: a
    /// `paths:` line, then the `- '...'` entries indented under it.
    fn workflow_path_lists(workflow: &str) -> Vec<Vec<String>> {
        let mut lists = Vec::new();
        let mut lines = workflow.lines().peekable();
        while let Some(line) = lines.next() {
            if line.trim() != "paths:" {
                continue;
            }
            let mut entries = Vec::new();
            while let Some(next) = lines.peek() {
                let t = next.trim();
                if t.starts_with('#') {
                    lines.next();
                    continue;
                }
                let Some(value) = t.strip_prefix("- ") else {
                    break;
                };
                entries.push(value.trim_matches('\'').trim_matches('"').to_string());
                lines.next();
            }
            lists.push(entries);
        }
        lists
    }

    /// `include_str!` reaches OUT of `engine/` for the AI-workflow skill, so
    /// that file is part of the engine build: editing it changes what `rocky
    /// mcp` serves, and renaming or deleting it fails compilation.
    ///
    /// `engine-ci.yml` must watch every such path, in BOTH places that route
    /// it since #1563: the `push` trigger still carries a `paths:` filter,
    /// while `pull_request` runs unfiltered and the `changes` job's
    /// `ENGINE_PATHS_RE` decides whether the required jobs do real work. A
    /// path the regex misses skips engine CI for exactly the change that
    /// needs it — #1557's failure mode, moved one level down (#1557, #1563).
    #[test]
    fn every_out_of_tree_include_is_watched_by_engine_ci() {
        let manifest = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
        let repo_root = manifest
            .join("../../..")
            .canonicalize()
            .expect("repo root from the crate manifest");
        let src = std::fs::read_to_string(manifest.join("src/tools.rs")).expect("read tools.rs");
        let workflow = std::fs::read_to_string(repo_root.join(".github/workflows/engine-ci.yml"))
            .expect("read engine-ci.yml");

        // The change-detection regex the `changes` job matches PR diffs
        // against. One line, `ENGINE_PATHS_RE: <regex>`.
        let engine_paths_re = workflow
            .lines()
            .find_map(|line| line.trim().strip_prefix("ENGINE_PATHS_RE:"))
            .expect(
                "engine-ci.yml no longer defines ENGINE_PATHS_RE — the \
                 change-detection mechanism moved, and this guard needs to \
                 move with it",
            )
            .trim();
        // The workflow hands that line to `grep -E`, so it is a POSIX ERE. It
        // uses only anchors (`^`, `$`), one group, alternation (`|`) and
        // escaped dots (`\.`). The `regex` crate reads that subset the same
        // way, so matching here reruns the CI check instead of guessing at
        // it from the regex's text.
        let engine_paths_re = regex::Regex::new(engine_paths_re).unwrap_or_else(|err| {
            panic!("ENGINE_PATHS_RE in engine-ci.yml does not compile: {err}\n{engine_paths_re}")
        });

        // Paths that climb above `engine/` — `src/` is three levels below the
        // repo root, so four or more `../` escapes the engine tree.
        let mut checked = 0;
        for (at, _) in src.match_indices("include_str!(\"") {
            let rest = &src[at + "include_str!(\"".len()..];
            let path = &rest[..rest.find('"').expect("unterminated include_str! path")];
            if !path.starts_with("../../../../") {
                continue;
            }
            let repo_relative = path.trim_start_matches("../");
            assert!(
                repo_root.join(repo_relative).exists(),
                "include_str! names a file that does not exist: {repo_relative}"
            );
            // The `push` trigger's `paths:` list is the only one left since
            // #1563 removed the pull_request filter. Pin that count so a
            // reintroduced or dropped list changes this guard deliberately.
            let lists = workflow_path_lists(&workflow);
            assert_eq!(
                lists.len(),
                1,
                "expected engine-ci.yml to carry exactly one `paths:` list \
                 (push — pull_request is deliberately unfiltered since #1563); \
                 found {}. If the triggers changed, this guard needs to change \
                 with them.",
                lists.len()
            );
            assert!(
                lists[0].iter().any(|entry| entry == repo_relative),
                "`{repo_relative}` is compiled into the engine but is missing \
                 from engine-ci.yml's push `paths:` list. A push touching it \
                 would run no engine CI (#1557)."
            );
            // The PR side: run the regex against the path, the way the
            // `changes` job does. Searching the regex's text for the path
            // proved nothing — `|x\.claude/...$|` and `|\|\.claude/...$|`
            // both contain it and neither matches it.
            assert!(
                engine_paths_re.is_match(repo_relative),
                "`{repo_relative}` is compiled into the engine but \
                 ENGINE_PATHS_RE in engine-ci.yml does not match it. A PR \
                 touching it would skip the required engine jobs (#1557, \
                 #1563). ENGINE_PATHS_RE: {engine_paths_re}"
            );
            checked += 1;
        }

        // A zero here would pass vacuously — the assertion above never runs if
        // the scan finds nothing, which is exactly how this guard would rot.
        assert!(
            checked > 0,
            "found no out-of-tree include_str! paths to check — the scan broke, \
             or the coupling moved and this test now proves nothing"
        );
    }

    /// Surface 1 — the served `instructions`, per profile.
    ///
    /// The default and approver profiles get the skill text byte-identical
    /// to the compiled file. The worker gets a DERIVED banner plus a
    /// PROJECTED body.
    ///
    /// This asserts the PROPERTY, not the presence of words, and the
    /// distinction is the finding it replaces. The old version looped five
    /// hardcoded names — omitting `draft_check`, the one tool whose removal
    /// started this round — and its per-name assertion was
    /// `contains(tool) && contains("not available")`, whose second conjunct
    /// does not move with `tool`. It proved the phrase "not available"
    /// appeared once, anywhere. Both halves are derived from the routers
    /// now, so no name can be omitted and no conjunct is loop-invariant.
    #[test]
    fn worker_instructions_are_projected_and_default_stays_verbatim() {
        for profile in [McpProfile::Default, McpProfile::Approver] {
            assert_eq!(
                server_with(profile).get_info().instructions.as_deref(),
                Some(INSTRUCTIONS),
                "{profile:?}-profile instructions are the skill text, byte-unchanged"
            );
        }

        // FIFTEENTH ROUND, finding 1, and the half the finding did not name.
        // The worker rewrite carried "exactly what would execute" onto
        // `plan_preview`; the sentence it was rewriting made the SAME claim
        // about `rocky plan`, and the DEFAULT and APPROVER profiles serve
        // that sentence verbatim. `INSTRUCTIONS` is an `include_str!` of the
        // authoring skill, so the skill file IS a served surface.
        //
        // The claim is false for the CLI too, on the same grounding the
        // reviewer used against `docs/reference/glossary.md`: `rocky plan`
        // renders its replication SQL STATICALLY, before any of it runs
        // (`plan.rs::replication_copy_sql` degrades a non-Databricks
        // `MERGE` to a canonical shape via `preview_merge_shape`, and an
        // incremental table previews the 1970 sentinel watermark),
        // `rocky plan --model` routes through `plan_preview_output` and
        // skips what it cannot render, and `rocky apply` RECOMPILES the
        // project rather than replaying the persisted plan (`plan.rs`'s
        // run-plan blueprint doc).
        //
        // SIXTEENTH ROUND, finding 2 — this justification USED TO SAY
        // "`rocky plan` renders offline", and that is the over-correction
        // the finding names. `commands::plan` builds an `AdapterRegistry`
        // and calls `discovery_adapter.discover()`; its own budget-check
        // comment says plan "already performs live warehouse I/O
        // (discovery, governance)". Statically-rendered is the property
        // that makes the canonical `MERGE` and the 1970 sentinel true.
        // Offline is a different property, and `rocky plan` does not have
        // it. Correcting an over-claim is where this branch keeps
        // manufacturing the opposite one.
        //
        // Pinned on the served text rather than on the file, because the
        // file is only a defect while the server serves it.
        assert!(
            !INSTRUCTIONS.contains("exactly what would execute"),
            "the served instructions promise `rocky plan` shows EXACTLY what would execute; \
             the plan renders offline and `rocky apply` recompiles rather than replaying it"
        );

        let excluded = worker_excluded_tool_mentions();
        let banner = worker_instructions_banner(&excluded);
        let worker_info = server_with(McpProfile::Worker).get_info();
        let worker = worker_info
            .instructions
            .as_deref()
            .expect("worker profile serves instructions");
        assert!(
            worker.starts_with(&banner),
            "worker instructions start with the banner"
        );
        let body = &worker[banner.len()..];

        // ROW 1 IS THE WHOLE `initialize` RESULT, not the `instructions`
        // field. Found while correcting finding 2, on the work that
        // correction produced — the enumeration's new sentence says every
        // row is covered by a sweep over the whole serialized payload of
        // its channel, and this row read one field of five.
        //
        // rmcp 3.1.2's `InitializeResult` also carries `protocolVersion`,
        // `capabilities`, `serverInfo` and `_meta`, and `Implementation`
        // carries `title`, `description`, `icons` and `websiteUrl` besides
        // `name` and `version` — four free-text fields on the channel a
        // worker reads at handshake, before it reads anything else.
        // `Implementation::from_build_env()` leaves all four `None`, so
        // nothing leaks today. The UNBACKED GUARANTEE was the defect, the
        // same shape as rows 2 and 4/5: no leak, a claim the sweep did not
        // support.
        //
        // THE BANNER IS SPLICED OUT, and it is the only thing removed. The
        // banner names every excluded tool ON PURPOSE — saying `propose` is
        // not available is the opposite of steering at it — so sweeping it
        // would fire on the one surface designed to name them. Everything
        // else the handshake serves is swept as-is, so a `title` or a
        // `description` set on `Implementation` later is covered without
        // this test knowing the shape.
        let mut handshake =
            serde_json::to_value(&worker_info).expect("initialize result serializes");
        handshake["instructions"] = serde_json::Value::String(body.to_string());
        assert!(
            handshake.get("serverInfo").is_some() && handshake.get("capabilities").is_some(),
            "the handshake must carry serverInfo and capabilities, or this sweeps an \
             envelope with the newly-covered fields missing: {handshake}"
        );
        let handshake = handshake.to_string();
        assert_eq!(
            names_excluded_tool(&handshake, &excluded),
            None,
            "the worker `initialize` result must not name an excluded tool anywhere \
             outside the banner: {handshake}"
        );

        // PROPERTY 1 — the projected body names NO excluded tool, in any
        // inflection. This is the same assertion every other swept surface
        // runs, and it is why the row moved from EXEMPT to swept. It fails
        // on the unprojected skill, which names `propose` six times and
        // `draft_metadata` once.
        assert_eq!(
            names_excluded_tool(body, &excluded),
            None,
            "the projected instructions body must not name an excluded tool in any form"
        );
        assert_ne!(
            body, INSTRUCTIONS,
            "the body must actually BE projected — an unchanged body means the rewrites \
             silently no-opped"
        );

        // PROPERTY 2 — the body stops the worker at CHECK authorship, which
        // is the specific hole. The old banner stopped it at contract and
        // metadata authorship and at the record/review/apply chain, while
        // the text below told it to strengthen assertions and append tests.
        let body_lower = body.to_lowercase();
        assert!(
            body_lower.contains("checks are spec-owned"),
            "the projected body must say checks are spec-owned: {body}"
        );
        assert!(
            body_lower.contains("do not add or strengthen assertions"),
            "the projected body must reverse the `strengthen assertions` steer: {body}"
        );
        assert!(
            !body_lower.contains("tests you append"),
            "the `tests you append through the draft tools` steer must be gone: {body}"
        );
        assert!(
            body_lower.contains("hand-off") || body_lower.contains("hand off"),
            "the projected body ends at the hand-off: {body}"
        );

        // PROPERTY 3 — the body does not INSTRUCT a withheld action, even
        // where it names no withheld tool. This is the ninth round's
        // finding, and it is a different assertion from PROPERTY 1 on
        // purpose: every needle below names no tool at all, so PROPERTY 1
        // read the unprojected text as clean while it told the worker to
        // write a model's `.toml` sidecar including strategy and target —
        // contradicting the banner three paragraphs up, and routing around
        // `draft_model`, which writes only a minimal `name` + `intent`
        // document and never invents routing.
        //
        // Asserted as the ABSENCE of the steer plus the PRESENCE of the
        // redirect, because absence alone is satisfiable by deleting the
        // paragraph, and a worker with no instruction is not the outcome
        // wanted here.
        for steer in [
            // Finding 1: the two sidecar-authorship sentences.
            "sidecar for materialization",
            "author the sql and its `.toml` sidecar",
            // Re-reading the whole body on the same lens found three more.
            // Each licenses a route this profile withholds, and each named
            // no excluded tool — the last one quotes `propose_only`, the
            // exact string the identifier rule was fixed NOT to match.
            "you can run the `rocky` cli",
            "`rocky shell`",
            "rocky product verify",
        ] {
            assert!(
                !body_lower.contains(steer),
                "the projected body still instructs `{steer}` — a withheld action the \
                 name-based sweep cannot see: {body}"
            );
        }
        assert!(
            body_lower.contains("do not write the `.toml` sidecar"),
            "the body must redirect sidecar authorship at `draft_model`, not merely drop \
             the sentence: {body}"
        );
        assert!(
            body_lower.contains("hand it to `draft_model`"),
            "the body must name the tool that performs the write it just withheld: {body}"
        );
        assert!(
            body_lower.contains("do not open a database connection of your own"),
            "the body must replace the raw-query sampling route with the served tools: \
             {body}"
        );
        // TENTH ROUND, finding 1D — this assertion is INVERTED, and the
        // comment it replaces argued the opposite.
        //
        // It used to pin `rocky compile` as PRESENT, on the argument that
        // `rocky compile` / `rocky plan` / `rocky test` name actions this
        // profile SERVES and only the ROUTE differs. That is true, and it
        // does not survive the banner sitting above it: the umbrella
        // rewrite forbids shell routes CATEGORICALLY. Text that forbids a
        // route and then instructs it four times is not followable.
        //
        // DERIVED, not per-string, and that is the whole upgrade. The old
        // pin named one sentence, so the other three imperatives — and any
        // fifth the skill grows — were invisible to it.
        //
        // ELEVENTH ROUND, finding 3 — the tenth round's scan matched the
        // literal "`rocky ": lowercase, and backticked. `Run rocky compile`
        // and ``Run `Rocky compile` `` both preserve every needle above and
        // pass it clean. No leak today, and the bound was stated honestly;
        // the defect was the sentence around it, which claimed any new CLI
        // invocation fails here. It did not. Widened rather than narrowed,
        // because the scan is cheap and it is the CLAIM that keeps going
        // stale.
        //
        // WHAT THE WIDENED SCAN ACTUALLY DOES, so the claim and the code
        // say the same thing: it finds every identifier-bounded `rocky` in
        // the lowercased body — case-insensitive and backtick-agnostic —
        // takes the word that follows, and refuses any word not on
        // `ROCKY_PROSE_FOLLOWERS`.
        //
        // A PROSE ALLOWLIST, NOT A VERB LIST, and the reason is a
        // dependency direction rather than a preference. The clap `Command`
        // enum is private to the `rocky` binary crate
        // (`engine/rocky/src/main.rs`), and that crate depends on this one.
        // Deriving the verb set here would invert the dependency. So the
        // rule fails CLOSED on an unknown follower instead: a new
        // `rocky <verb>` fails without anyone listing the verb, and a new
        // English phrase after "Rocky" fails until someone reads it and
        // adds the word deliberately. The cost is a test edit on an
        // innocent rewording; the alternative is a hand-maintained verb
        // list that goes stale silently, which is the failure this round
        // exists to stop repeating.
        let followers = rocky_followers(&body_lower);
        let routes: Vec<&str> = followers
            .iter()
            .copied()
            .filter(|word| !ROCKY_PROSE_FOLLOWERS.contains(word))
            .collect();
        assert!(
            routes.is_empty(),
            "the projected body still routes the worker through the CLI ({routes:?}), \
             while the umbrella rewrite above it forbids shell routes categorically — \
             rewrite each one as the served action, or, if the word is prose rather than a \
             verb, add it to ROCKY_PROSE_FOLLOWERS after reading the sentence: {body}"
        );
        // The scan must actually SEE the word — a body where "rocky" never
        // occurs would satisfy the assertion above vacuously, and so would
        // a broken matcher.
        assert!(
            !followers.is_empty(),
            "the scan found no `rocky` at all in the projected body, so the assertion above \
             proved nothing: {body}"
        );
        // NO DEAD ENTRIES. Every allowed word must still occur in the body.
        // Two things fall out of this, and the second is the point:
        //
        //  - A sentence the skill drops takes its exemption with it,
        //    instead of leaving a hole for a later route of the same word.
        //  - A CLI verb cannot be PRE-AUTHORIZED. Adding "compile" here to
        //    smooth a future edit fails immediately, because no such
        //    follower exists yet. The exemption and the sentence have to
        //    arrive together, where a reviewer sees both.
        for allowed in ROCKY_PROSE_FOLLOWERS {
            assert!(
                followers.contains(allowed),
                "ROCKY_PROSE_FOLLOWERS still exempts `{allowed}`, which no longer follows \
                 `rocky` anywhere in the projected body — drop the entry rather than \
                 leaving a standing exemption: {followers:?}"
            );
        }
        // And it must catch what the old literal scan missed. Both forms
        // preserve every needle in this test; only the widened rule sees
        // them.
        for missed in [
            "Run rocky compile to check your work.",
            "Run `Rocky compile` to check your work.",
            "ROCKY COMPILE is the fast feedback loop.",
        ] {
            let probe = format!("{body}\n\n{missed}").to_lowercase();
            let probe_followers = rocky_followers(&probe);
            assert!(
                probe_followers.contains(&"compile"),
                "the widened scan must see `{missed}` — the literal it replaced did not"
            );
        }
        // And the served actions REPLACED them, rather than the sentences
        // being deleted: a worker with no instruction is not the outcome
        // wanted here, exactly as in PROPERTY 3 above.
        for served in [
            "call the `inspect_schema` tool",
            "call the `compile` tool and read its `diagnostics`",
            "call the `plan_preview` tool",
            "call the `test` tool",
        ] {
            assert!(
                body_lower.contains(served),
                "the body must name the served action that replaced the CLI route \
                 (`{served}`): {body}"
            );
        }
        // FIFTEENTH ROUND, finding 1 — and the served action is not enough
        // on its own. The rewrite that named `plan_preview` also carried
        // the CLI sentence's exactness claim onto it, which is the surface
        // it is least true of: the preview renders with no warehouse and
        // DROPS what it cannot render, unnamed. Pinned in both directions
        // so neither the removal nor the disclosure can be undone alone.
        assert!(
            !body_lower.contains("exactly what would execute"),
            "the projected body promises `plan_preview` shows EXACTLY what would execute; \
             it renders offline and silently drops what it cannot render: {body}"
        );
        assert!(
            body.contains("SKIPPED, and the result does not name it"),
            "the projected body must say a model the preview cannot render offline is \
             dropped WITHOUT being named, or an empty preview reads as an empty project: \
             {body}"
        );
        // FINDING 1C — the retry steer. It presumed materializing a pipeline
        // through a route this profile does not serve; no worker tool runs
        // one, so no run error can occur to retry.
        assert!(
            !body_lower.contains("retry a `transient`"),
            "the projected body must not tell the worker to retry a run error it cannot \
             produce: {body}"
        );
        assert!(
            body_lower.contains("there is no run to retry"),
            "and it must say why, rather than dropping the bullet: {body}"
        );
        // ELEVENTH ROUND, finding 2 — and the reason it says must be the
        // TRUE one. The tenth round's replacement justified the bullet with
        // "no tool this profile serves executes against the warehouse",
        // which is false: `sample_rows`, `profile_column` and
        // `inspect_schema` are all on the allowlist and all read it. Pinned
        // in BOTH directions, because dropping the over-claim without
        // stating what replaces it would let the next rewrite reinstate it.
        assert!(
            !body_lower.contains("serves executes against the warehouse"),
            "the projected body must not claim no served tool reaches the warehouse — \
             `sample_rows`, `profile_column` and `inspect_schema` all do: {body}"
        );
        //
        // THIRTEENTH ROUND, finding 2 — the three names were a literal here.
        // They come from [`WORKER_TOOL_EFFECTS`] now, so a thirteenth tool
        // classified as a reader has to be NAMED in this bullet before this
        // test goes green. That is the half of the reader claim that can be
        // held mechanically; which tools read is hand-reviewed, over there.
        let readers = worker_tools_that_read_the_warehouse(WORKER_TOOL_EFFECTS);
        assert!(
            !readers.is_empty(),
            "the classification must name at least one reader or the sweep below proves \
             nothing: {WORKER_TOOL_EFFECTS:?}"
        );
        for reader in readers {
            assert!(
                WORKER_PROFILE_TOOLS.contains(&reader),
                "`{reader}` must still be on the allowlist, or the sentence naming it as a \
                 warehouse reader is stale"
            );
            assert!(
                body_lower.contains(reader),
                "the body must name `{reader}` as a tool that reads the warehouse, rather \
                 than denying that any tool does: {body}"
            );
        }
        // TWELFTH ROUND, finding 1 — and the REASON the bullet gives has to
        // hold for every reader it names. It did not. `sample_rows` and
        // `profile_column` propagate a failed read as
        // `ToolError::warehouse_error`; `inspect_schema` returns SUCCESS with
        // no physical tables (since #1565 it also sets `discovery_incomplete`
        // and says why in `discovery_error` — reported, but still not an
        // error). A worker holding the old universal reads an empty `sources`
        // as "no such table".
        //
        // Pinned in BOTH directions, for the reason the pair above is:
        // dropping the caveat is the cheap edit, and it re-promises
        // silently.
        assert!(
            !body_lower.contains("a read that fails comes back as that tool's own error"),
            "the projected body must not promise EVERY named reader surfaces a failed read \
             as its own error — `inspect_schema` reports no physical tables and still \
             returns success: {body}"
        );
        assert!(
            body_lower.contains("not proof the table is absent"),
            "and it must say what that costs the worker: a table missing from \
             `inspect_schema`'s `sources` is inconclusive, not absent. Dropping the caveat \
             re-promises what the tool does not do: {body}"
        );
        // AND IT MUST HAND THE WORKER A WAY OUT. A caveat with no remedy
        // leaves a worker stuck at the exact point it needs to act, so the
        // discriminator is pinned as its own assertion rather than left to
        // the reader loop above — that loop is satisfied by the earlier
        // mention of `sample_rows` and cannot see this sentence.
        //
        // The behaviour under it is proven elsewhere, not here:
        // `prepare_table_query` routes a DOTTED target down the
        // qualified-raw-ref branch with no compile, covered live over the
        // wire by `sample_rows_reaches_raw_source_by_qualified_ref`
        // (tests/roundtrip.rs). This assertion pins the ADVICE; that test
        // pins the behaviour the advice depends on.
        assert!(
            body_lower.contains("ask `sample_rows` for that table"),
            "the body must name the reader that DOES fail loudly for the same table, or the \
             caveat above leaves the worker with no way to tell inconclusive from absent: \
             {body}"
        );
        // THIRTEENTH ROUND, finding 1 — the same defect one tool over, in the
        // sentence that fixed the one above it. `profile_column` was named as
        // a counter-example to `inspect_schema`, and it is one only for its
        // PRIMARY query. Its `top_values` is a second warehouse query taking
        // `Err(_) => Vec::new()`, after which the tool returns success — so a
        // worker reading an empty list as a fact about the column is reading
        // a possible failure as data. Pinned in both directions, like the
        // pair above: dropping the caveat is the cheap edit, and the
        // over-claim came back the moment nothing held it.
        assert!(
            !body_lower.contains(
                "`sample_rows` and `profile_column` surface a failed read as that tool's own \
                 error"
            ),
            "the projected body must not promise `profile_column` surfaces EVERY failed read \
             as its own error — its `top_values` query returns an empty list and the tool \
             still succeeds: {body}"
        );
        assert!(
            body_lower.contains("`top_values` is a second query and is best-effort"),
            "the body must disclose `profile_column`'s optional second read as best-effort, \
             or the worker has no reason to distrust an empty `top_values`: {body}"
        );
        // AND the caveat has to say what the empty list fails to tell apart.
        // "Best-effort" alone reads as "sometimes missing"; the actionable
        // fact is that a failure is indistinguishable from two ordinary
        // outcomes, because nothing in the result separates them.
        assert!(
            body_lower.contains("an empty `top_values` does not distinguish"),
            "and it must say what an empty `top_values` does NOT tell apart — a \
             high-cardinality column, an all-null one, and a failed query all produce it: \
             {body}"
        );
        // TWELFTH ROUND, finding 2 — NO COUNT IS PINNED HERE, DELIBERATELY.
        //
        // The bullet used to open "Three tools do READ the warehouse", and
        // that number was guarded with `WORKER_PROFILE_TOOLS.len() == 12`.
        // The assertion READS as semantic assurance and is not: changing an
        // existing tool's body to open an adapter, or swapping one
        // allowlisted tool for another, preserves the length and passes. A
        // guard that looks derived and proves nothing is the exact defect
        // this branch has produced three times, so the CLAIM went instead of
        // the guard being propped up. The loop above still holds what is
        // holdable — each named reader is on the allowlist AND named in the
        // body — and it never claimed exhaustivity.
        //
        // WHAT ROUND TWELVE LEFT UNGUARDED — and what now holds it. The
        // bullet's leading sentence, "No tool this profile serves runs or
        // materializes a pipeline", is a universal over twelve hand-chosen
        // names. PROPERTY 4 below derives the EXCLUDED set — the complement
        // of the allowlist — so it cannot see the interior, and a thirteenth
        // allowlisted tool that ran a pipeline would falsify the sentence
        // with every assertion in this test green.
        //
        // Round twelve deleted the length check and volunteered that gap
        // rather than replacing it. The reviewer ruled the universal is not
        // acceptable uninstrumented, and it was right: the length check held
        // nothing, but it WAS an extension tripwire, and dropping it left
        // the sentence with none.
        //
        // [`WORKER_TOOL_EFFECTS`] is the replacement — a reviewed effect per
        // served tool, cross-checked against the router by
        // `every_worker_served_tool_is_classified_and_none_runs_a_pipeline`.
        // It is hand-reviewed and says so, which is the distinction this
        // branch keeps getting wrong in the other direction: it does not
        // pretend to derive what a call graph would have to answer.
        assert!(
            worker_tools_that_run_a_pipeline(WORKER_TOOL_EFFECTS).is_empty(),
            "the bullet opens by denying any served tool runs or materializes a pipeline, \
             and the reviewed classification now contradicts it: {WORKER_TOOL_EFFECTS:?}"
        );

        // PROPERTY 4 — the banner names EVERY excluded tool, derived. The
        // banner is the one worker surface that names them deliberately:
        // saying a tool is unavailable is the opposite of steering at it.
        let banner_lower = banner.to_lowercase();
        let (prohibition, _) = banner_lower
            .split_once("not available in this session:")
            .expect("the banner states the prohibition once, before the name list");
        assert!(
            prohibition.contains("worker profile"),
            "the banner says which profile is active: {banner}"
        );
        assert!(
            !excluded.is_empty(),
            "the derived excluded set must be non-empty or this proves nothing"
        );
        for tool in &excluded {
            assert!(
                banner_lower.contains(&tool.to_lowercase()),
                "the banner must name `{tool}` — derived from the routers, so a tool that \
                 leaves the allowlist cannot be omitted by forgetting to list it: {banner}"
            );
        }
        assert!(
            banner_lower.contains("spec-owned"),
            "the banner names checks/contracts/metadata as spec-owned: {banner}"
        );
        assert!(
            banner_lower.contains("hand-off") && banner_lower.contains("trusted runner"),
            "the banner redirects every ending to the trusted-runner hand-off: {banner}"
        );
    }

    /// THIRTEENTH ROUND, finding 2 — the extension tripwire under the worker
    /// bullet's leading universal, "No tool this profile serves runs or
    /// materializes a pipeline".
    ///
    /// Cross-checks [`WORKER_TOOL_EFFECTS`] against the ROUTER, not against
    /// [`WORKER_PROFILE_TOOLS`]. The allowlist is a list of strings and a
    /// name that matches no route is silently inert; the router is what a
    /// worker session can actually call. Both directions, so a thirteenth
    /// served tool fails here until someone classifies it, and a stale entry
    /// for a tool that left the surface fails too.
    ///
    /// What it does NOT prove is that a classification is correct — see the
    /// note on the table. It proves nothing served is unclassified, and that
    /// no reviewed entry contradicts the sentence.
    #[test]
    fn every_worker_served_tool_is_classified_and_none_runs_a_pipeline() {
        use std::collections::BTreeSet;

        let server = server_with(McpProfile::Worker);
        let served: BTreeSet<String> = server
            .tool_router
            .list_all()
            .iter()
            .map(|t| t.name.to_string())
            .collect();
        let classified: BTreeSet<String> = WORKER_TOOL_EFFECTS
            .iter()
            .map(|(name, _)| (*name).to_string())
            .collect();
        assert_eq!(
            WORKER_TOOL_EFFECTS.len(),
            classified.len(),
            "a tool is classified twice, and the duplicate could disagree with itself: \
             {WORKER_TOOL_EFFECTS:?}"
        );
        assert_eq!(
            served, classified,
            "every tool the worker router SERVES carries a reviewed effect, and nothing else \
             does. Add the new tool to WORKER_TOOL_EFFECTS — after reading its body — or \
             drop the stale entry"
        );

        // THE CLAIM the worker instructions make about this surface.
        assert!(
            worker_tools_that_run_a_pipeline(WORKER_TOOL_EFFECTS).is_empty(),
            "the worker bullet denies that any served tool runs or materializes a pipeline: \
             {WORKER_TOOL_EFFECTS:?}"
        );

        // AND THE READER SET IS PINNED to the three that were audited.
        // Feeding the body sweep from this table removed a stale literal and
        // added a RELAXATION in its place: reclassify `inspect_schema` to
        // `Offline` and the bullet no longer has to name it, with every
        // assertion green. Narrowing the set now takes a deliberate two-line
        // edit — the table AND this line — which is the "someone has to
        // look" property the guard exists for. Not circular: this pins the
        // SHAPE of the reviewed answer, the table holds the answer.
        assert_eq!(
            worker_tools_that_read_the_warehouse(WORKER_TOOL_EFFECTS),
            vec!["inspect_schema", "profile_column", "sample_rows"],
            "the three audited warehouse readers. Re-read the tool body before changing this \
             — the worker bullet has to NAME every reader, and dropping one here drops it \
             from that sweep too"
        );

        // AND THE CHECK HAS TO BITE. Round twelve's guard read as semantic
        // assurance and passed through the thing it claimed to hold, so this
        // one is run over a table that DOES violate the sentence. Without
        // this, `is_empty()` above is green on a filter that never matches —
        // indistinguishable from a guard that works.
        assert_eq!(
            worker_tools_that_run_a_pipeline(&[
                ("compile", WorkerToolEffect::Offline),
                ("run_pipeline", WorkerToolEffect::RunsPipeline),
                ("sample_rows", WorkerToolEffect::ReadsWarehouse),
            ]),
            vec!["run_pipeline"],
            "the classification check must catch a tool that runs a pipeline, or the \
             assertion above proves only that the filter never matches"
        );
    }

    /// THIRTEENTH ROUND — the SIBLING SURFACE to finding 1. The served
    /// instructions now describe both best-effort reads honestly, and the
    /// `tools/list` descriptions of the same two tools did not.
    ///
    /// `inspect_schema` was the worse of the pair: "the typed columns of
    /// every model and source table in the project" is an unqualified
    /// universal, and "never guess column names" tells the worker to treat
    /// the answer as complete — over a tool whose physical-table half is
    /// best-effort. Since #1565 its two failure paths are REPORTED
    /// (`discovery_incomplete` / `discovery_error`), but the call still
    /// succeeds, so the description still has to say what a missing table
    /// means.
    ///
    /// Swept on BOTH profiles because neither description is rewritten for
    /// the worker (`WORKER_TOOL_DESCRIPTIONS` covers `breaking_change`,
    /// `plan_preview` and `draft_model` only), so one string serves both and
    /// a default-profile caller reads the same promise.
    #[test]
    fn the_reader_tool_descriptions_disclose_their_best_effort_reads() {
        for profile in [McpProfile::Default, McpProfile::Worker] {
            let server = server_with(profile);
            let description = |name: &str| -> String {
                server
                    .tool_router
                    .map
                    .get(name)
                    .unwrap_or_else(|| panic!("{profile:?} serves '{name}'"))
                    .attr
                    .description
                    .as_deref()
                    .unwrap_or_default()
                    .to_string()
            };
            let inspect = description("inspect_schema");
            assert!(
                inspect.contains("best-effort"),
                "{profile:?}: `inspect_schema` promises the project's tables without saying \
                 its physical-table discovery can report none of them and still succeed: \
                 {inspect}"
            );
            assert!(
                inspect.contains("inconclusive, not absent"),
                "{profile:?}: and it must say what that costs the caller, or 'best-effort' \
                 reads as 'sometimes slow': {inspect}"
            );
            let profile_column = description("profile_column");
            assert!(
                profile_column.contains("second query and is best-effort"),
                "{profile:?}: `profile_column`'s description offers `top_values` without \
                 saying the query behind it can fail into an empty list on a successful \
                 call: {profile_column}"
            );
            // FOURTEENTH ROUND, finding 1 — the same defect on a tool that
            // RENDERS rather than reads, which is why it sat outside this
            // test's original family. `plan_preview` called its output "the
            // exact SQL Rocky would execute" while
            // `commands::plan_preview_output` passes no warehouse and skips
            // every model `sql_gen` cannot render offline, and
            // `PlanPreviewResult` has no field that names a skipped model.
            //
            // Pinned in BOTH directions and on BOTH profiles: the removed
            // exactness claim must stay gone, and the disclosure that
            // replaced it must stay present. Only the trailing sentence
            // differs per profile ([`WORKER_TOOL_DESCRIPTIONS`]), so the
            // disclosure is shared text and a one-sided edit fails here.
            let plan_preview = description("plan_preview");
            assert!(
                !plan_preview.contains("exact SQL Rocky would execute"),
                "{profile:?}: `plan_preview`'s description promises the EXACT execution SQL; \
                 the preview is offline and silently drops what it cannot render: \
                 {plan_preview}"
            );
            assert!(
                plan_preview.contains("SKIPPED, and the result does not name it"),
                "{profile:?}: `plan_preview`'s description must say that a model it cannot \
                 render offline is dropped WITHOUT being named, or an empty preview reads as \
                 an empty project: {plan_preview}"
            );
        }
    }

    /// Item 5b — the worker `prompts/list` surface: EVERY listed prompt
    /// description (the sweep is over the whole router, so a future prompt
    /// cannot dodge it) names none of the excluded tools and the four
    /// workflow prompts say they end at the trusted-runner hand-off.
    #[test]
    fn worker_prompt_descriptions_name_no_excluded_tool() {
        let server = server_with(McpProfile::Worker);
        let prompts = server.prompt_router.list_all();
        assert_eq!(prompts.len(), 5, "the worker profile keeps all 5 prompts");
        for prompt in &prompts {
            let description = prompt
                .description
                .as_deref()
                .unwrap_or_else(|| panic!("prompt '{}' has a description", prompt.name));
            assert_eq!(
                names_excluded_tool(description, &worker_excluded_tool_mentions()),
                None,
                "worker-profile description of '{}' must not name an excluded tool in any \
                 form: {description}",
                prompt.name
            );
            if prompt.name != "summarize_project" {
                assert!(
                    description.contains("hand-off to the trusted runner"),
                    "worker-profile description of '{}' ends at the runner hand-off: \
                     {description}",
                    prompt.name
                );
            }
            // The same write-promise backstop the `prompts/get` description
            // carries (ninth round, finding 2). These two surfaces are one
            // field apart and say nearly the same thing; guarding only the
            // one that was caught is how the next round finds the sibling.
            //
            // Hand-written, and deliberately so: deriving the promises from
            // the withheld capabilities (`draft_check` ⇒ "draft a check")
            // was tried against the two strings that shipped and matched
            // NEITHER — `draft_check` writes a `[[tests]]` block, so the
            // tool noun and the artifact noun are different words, and
            // "Add key tests" uses a verb no tool name carries. The full
            // reasoning is on the sibling sweep in
            // `worker_profile_prompts_end_at_the_runner_handoff`; keep the
            // two lists identical.
            let description_lower = description.to_lowercase();
            for promise in [
                "draft tests",
                "draft the tests",
                "add tests",
                "add key tests",
                "write tests",
                "draft checks",
                "add checks",
                "write checks",
                "draft a contract",
                "add a contract",
                "write metadata",
            ] {
                assert!(
                    !description_lower.contains(promise),
                    "worker-profile `prompts/list` description of '{}' promises \
                     `{promise}`, which is spec-owned in this profile: {description}",
                    prompt.name
                );
            }
            // TENTH ROUND, finding 1 — the same false promise about which
            // suite `test` runs, on the LIST description rather than the
            // body. `fix_failing_test` said "failing declarative tests: run
            // `test`", and `test` calls `commands::test_output` — the
            // compiled model tests plus the unit tests. The declarative set
            // is `rocky test --declarative`, a path this profile does not
            // serve. Pinned here because this field is one hop from the
            // prompt body and was fixed separately from it.
            assert!(
                !description_lower.contains("declarative tests: run `test`"),
                "worker-profile `prompts/list` description of '{}' claims the `test` tool \
                 runs the declarative check set; it runs the LOCAL model + unit tests: \
                 {description}",
                prompt.name
            );
        }
    }

    /// Item 5b, the other half — the DEFAULT `prompts/list` descriptions are
    /// byte-unchanged: pinned against the exact pre-worker-profile strings,
    /// so the worker rewrite provably never leaks into the default surface.
    #[test]
    fn default_prompt_descriptions_are_byte_unchanged() {
        let expected: &[(&str, &str)] = &[
            (
                "add_tests_to_pks",
                "Add uniqueness + not-null tests to a model's primary-key / unique columns: \
                 inspect_schema -> identify key columns -> ai_test / author the checks -> \
                 draft_check -> propose. Stops at the human approval gate.",
            ),
            (
                "build_model",
                "Guide the authoring of one Rocky model from a plain-language intent: inspect \
                 schema -> sample rows -> profile columns -> write SQL -> compile-loop -> plan \
                 preview -> propose. Stops at the human approval gate.",
            ),
            (
                "find_untested_models",
                "Find models with no declarative tests and draft tests for them: catalog -> \
                 identify untested models -> ai_test / ai_contract -> draft_check / \
                 draft_contract -> propose. Stops at the human approval gate.",
            ),
            (
                // FOURTEENTH ROUND — the one entry on this list that is NOT
                // byte-unchanged from the pre-worker-profile string, and the
                // reason is written down so it is not read as a leak.
                //
                // The old text said "declarative tests: run `test`". The
                // `test` tool runs `commands::test_output` — model execution
                // plus sidecar fixture `[[test]]` blocks — on every profile,
                // and never `run_declarative_tests`. Round ten found that and
                // fixed the WORKER copy, pinning its absence in
                // `worker_prompt_descriptions_name_no_excluded_tool`; the
                // defect was never profile-shaped, so the default copy stayed
                // false for four rounds.
                //
                // This test's job is to stop the worker rewrite LEAKING into
                // the default surface, not to freeze a false sentence. A
                // deliberate correctness fix updates the literal here, in the
                // same commit, on purpose.
                "fix_failing_test",
                "Diagnose and fix failing LOCAL tests: run `test` — the project's model and \
                 unit tests, not the warehouse-run `--declarative` set — then for each \
                 failure profile_column the implicated columns to ground the cause -> \
                 propose a fix. Stops at the human approval gate.",
            ),
            (
                "summarize_project",
                "Produce a structured, read-only summary of the Rocky project: catalog + \
                 lineage -> grouped overview of models, their grain, governance, tests, and DAG \
                 shape. Read-only — no edits, no propose.",
            ),
        ];
        let server = server_with(McpProfile::Default);
        let listed: std::collections::BTreeMap<String, Option<String>> = server
            .prompt_router
            .list_all()
            .into_iter()
            .map(|p| (p.name.to_string(), p.description.clone()))
            .collect();
        assert_eq!(listed.len(), expected.len(), "all prompts accounted for");
        for (name, description) in expected {
            assert_eq!(
                listed.get(*name).and_then(|d| d.as_deref()),
                Some(*description),
                "default-profile description of '{name}' is byte-unchanged"
            );
        }
        // FOURTEENTH ROUND — the byte pin above already covers this, and it
        // is asserted separately anyway because a byte pin records WHAT, not
        // WHY. Someone re-blessing the literal to make a build pass would
        // put the false sentence back and this line is what stops them.
        //
        // The claim: no default-profile description may say the `test` tool
        // runs the declarative set. It does not, on any profile —
        // `commands::test_output` runs model execution plus sidecar fixture
        // `[[test]]` blocks, never `run_declarative_tests`. The mirror of
        // this assertion has guarded the worker surface since round ten;
        // the defect was never profile-shaped, so the guard should not be
        // either.
        for (name, description) in &listed {
            let lower = description.as_deref().unwrap_or_default().to_lowercase();
            assert!(
                !lower.contains("declarative tests: run `test`"),
                "default-profile description of '{name}' claims the `test` tool runs the \
                 declarative check set; it runs the LOCAL model + unit tests: {description:?}"
            );
        }
    }

    /// F3 red team, finding 3 — SURFACE 4: every `tools/list` description
    /// the worker profile serves. Swept over the whole filtered router, so
    /// a tool added to [`WORKER_PROFILE_TOOLS`] later cannot dodge it.
    ///
    /// This is the surface the previous three rounds missed. Three served
    /// descriptions steered at `propose`: `breaking_change` and
    /// `draft_model` by name, and `plan_preview` as "proposing" — which
    /// only the inflection half of [`names_excluded_tool`] can see.
    #[test]
    fn worker_tool_descriptions_name_no_excluded_tool() {
        let server = server_with(McpProfile::Worker);
        let excluded = worker_excluded_tool_mentions();
        let tools = server.tool_router.list_all();
        assert_eq!(
            tools.len(),
            WORKER_PROFILE_TOOLS.len(),
            "the sweep covers the whole worker surface, not a sample"
        );
        for tool in &tools {
            let description = tool
                .description
                .as_deref()
                .unwrap_or_else(|| panic!("tool '{}' has a description", tool.name));
            assert_eq!(
                names_excluded_tool(description, &excluded),
                None,
                "worker-profile description of '{}' must not name an excluded tool in any \
                 form: {description}",
                tool.name
            );
        }
    }

    /// The other half of the rewrite — the DEFAULT descriptions keep the
    /// sentences the worker profile replaces, and never gain the
    /// replacements.
    ///
    /// Asserted as needle-present + replacement-absent rather than as a
    /// byte pin of three paragraphs. A byte pin of prose that long rots
    /// into a copy nobody rereads; this checks the two facts that matter
    /// and stays true across an unrelated edit. The construction-time
    /// `assert!` in `new_with_profile` covers the same needle from the
    /// other side, so a reworded sentence cannot silently become a no-op.
    #[test]
    fn the_worker_description_rewrites_do_not_leak_into_the_default_surface() {
        let default_server = server_with(McpProfile::Default);
        for (name, needle, replacement) in WORKER_TOOL_DESCRIPTIONS {
            let route = default_server
                .tool_router
                .map
                .get(*name)
                .unwrap_or_else(|| panic!("default profile serves '{name}'"));
            let description = route.attr.description.as_deref().unwrap_or_default();
            assert!(
                description.contains(needle),
                "the default description of '{name}' still carries the sentence the worker \
                 profile rewrites: {description}"
            );
            assert!(
                !description.contains(replacement),
                "and never the worker replacement: {description}"
            );
        }
    }

    /// The guidance-surface count is closed at the PROTOCOL level.
    ///
    /// [`WORKER_GUIDANCE_SURFACES`] enumerates nine places a worker is
    /// served text. That enumeration is only trustworthy if no tenth
    /// CHANNEL can open without anyone noticing — so this pins the served
    /// capabilities: `tools` and `prompts`, and nothing else. Enabling
    /// resources, completions or logging fails here and forces a revisit
    /// of the count instead of quietly invalidating it.
    ///
    /// TENTH ROUND, finding 3 — asserted as the whole serialized KEY SET,
    /// not as three `is_none()` checks. Those named `resources`,
    /// `completions` and `logging` and stopped there, while rmcp 3.1.2's
    /// `ServerCapabilities` also carries `experimental` and `extensions`
    /// (SEP-1724, keyed by extension id — the tasks extension lives there).
    /// Enumerating the fields a test knows about is the same defect as
    /// enumerating the fields a sweep knows about, one struct over, so the
    /// fix is the same: read what the value actually serializes. Every
    /// field is `skip_serializing_if = "Option::is_none"`, so an absent
    /// capability contributes no key and a NEW one added by a future rmcp
    /// fails here the moment it is enabled.
    #[test]
    fn the_server_opens_no_guidance_channel_beyond_tools_and_prompts() {
        let next = WORKER_GUIDANCE_SURFACES + 1;
        for profile in [
            McpProfile::Default,
            McpProfile::Approver,
            McpProfile::Worker,
        ] {
            let capabilities = server_with(profile).get_info().capabilities;
            let serialized = serde_json::to_value(&capabilities).expect("capabilities serialize");
            let mut keys: Vec<&str> = serialized
                .as_object()
                .expect("capabilities serialize as an object")
                .keys()
                .map(String::as_str)
                .collect();
            keys.sort_unstable();
            assert_eq!(
                keys,
                ["prompts", "tools"],
                "the {profile:?} profile declares a capability beyond tools and prompts. \
                 Any other channel — resources, completions, logging, experimental, an \
                 SEP-1724 extension — carries a tenth KIND of served text and would be \
                 guidance surface {next}; extend WORKER_GUIDANCE_SURFACES and its sweeps \
                 rather than this assertion: {serialized}"
            );
        }
    }

    /// The matching rule itself, in both directions — the exact name and
    /// the inflections, and no match on an unrelated word.
    ///
    /// Driven on the two strings that were actually wrong, so this test
    /// fails if the rule is ever narrowed back to an exact-name compare.
    #[test]
    fn the_mention_rule_reads_inflections_not_just_the_bare_name() {
        let excluded = vec!["propose".to_string()];
        assert_eq!(
            names_excluded_tool("then `propose` to record a plan", &excluded),
            Some(("propose".to_string(), "propose".to_string())),
            "the exact name still matches"
        );
        let (tool, form) = names_excluded_tool("before proposing a materialization", &excluded)
            .expect(
                "`proposing` steers at `propose`; an exact-name sweep read this as clean and \
                 shipped it to the worker",
            );
        assert_eq!((tool.as_str(), form.as_str()), ("propose", "proposing"));
        assert!(
            names_excluded_tool("Proposing a wrong key invariant", &excluded).is_some(),
            "and case-insensitively, or a sentence-initial verb slips through"
        );
        assert_eq!(
            names_excluded_tool("preview the generated SQL", &excluded),
            None,
            "unrelated prose does not match"
        );
    }

    /// The rule is an IDENTIFIER detector, not a byte-run detector (F3
    /// round 2, finding 3).
    ///
    /// The raw `contains` it replaces read every excluded name inside any
    /// longer identifier. That was tolerable while the swept surface was
    /// text Rocky writes, and is not now that the sweep reaches compiler
    /// diagnostics, which quote the USER's model and column names back at
    /// the worker — Rocky cannot reword someone's column.
    ///
    /// Both directions are asserted, because a matcher that stops firing
    /// is the failure mode the sweep exists to catch.
    #[test]
    fn the_mention_rule_matches_identifiers_not_byte_runs() {
        let excluded = vec!["propose".to_string(), "optimize".to_string()];

        for user_text in [
            "column `proposal_id` not found on model `orders`",
            "unknown column: proposed_amount",
            "model `proposer` has no unique key",
            "the config literal propose_only is frozen",
            // The corrected boundary example. Two review rounds cited
            // `propose_v2` as the collision no rule can fix; `_` is an
            // identifier byte, so it never collided. Pinned here so the
            // comment that now says so cannot drift back.
            "column not found on model `propose_v2`",
            "unoptimized scan on `events`",
        ] {
            assert_eq!(
                names_excluded_tool(user_text, &excluded),
                None,
                "'{user_text}' names no tool — the name is inside a longer identifier"
            );
        }

        for steering in [
            "then `propose` to record a plan",
            "before proposing a materialization",
            "a plan you already proposed",
            "write the proposal, then stop",
            "or optimize the query to reduce scan volume",
            // The collision that IS real: an EXACT user identifier. This is
            // what the boundary paragraph on `WORKER_GUIDANCE_SURFACES`
            // describes, and it fires because there is nothing lexical left
            // to tell it apart from the tool name.
            "column not found on model `propose`",
        ] {
            assert!(
                names_excluded_tool(steering, &excluded).is_some(),
                "'{steering}' names an excluded tool at identifier boundaries and must fire"
            );
        }
    }

    /// The boundary rule is byte-exact about the neighbours, including the
    /// ends of the string and non-identifier punctuation.
    #[test]
    fn identifier_boundaries_are_the_neighbouring_bytes() {
        assert!(contains_identifier("propose", "propose"), "whole string");
        assert!(contains_identifier("`propose`", "propose"), "backticks");
        assert!(contains_identifier("propose.", "propose"), "trailing stop");
        assert!(contains_identifier("(propose)", "propose"), "parenthesised");
        assert!(contains_identifier("re-propose", "propose"), "hyphen");
        assert!(!contains_identifier("propose_only", "propose"), "suffix _");
        assert!(!contains_identifier("xpropose", "propose"), "prefix alpha");
        assert!(!contains_identifier("propose2", "propose"), "suffix digit");
        // A later occurrence still matches when an earlier one is embedded:
        // the scan advances rather than stopping at the first byte run.
        assert!(
            contains_identifier("propose_only, then propose", "propose"),
            "the scan does not stop at the first embedded occurrence"
        );
    }

    /// The projection REFUSES on drift, in both directions, and every
    /// profile builds cleanly today (ninth review round).
    ///
    /// This check is not a build invariant, and the comments that called it
    /// one were wrong. Every operand is a compile-time constant, but
    /// nothing verifies the match until a server is CONSTRUCTED — which
    /// `serve_stdio` does on the live `rocky mcp --profile worker` path. An
    /// edit to the skill compiles and then fails at startup. This test is
    /// the guarantee that the frozen constants still line up, so it is
    /// worth stating that a test is all it is.
    ///
    /// Driven through a deliberately-drifted table, because the real one
    /// matches: without that, "it refuses on drift" would be an untested
    /// claim on the exact path this round is correcting claims about.
    #[test]
    fn the_worker_projection_refuses_on_drift_rather_than_panicking() {
        let excluded = worker_excluded_tool_mentions();

        // ZERO matches — the skill was edited under the projection. The
        // silent no-op replace this prevents would serve the DEFAULT
        // sentence to a worker.
        let gone = worker_instructions(
            &excluded,
            &[("a sentence the skill file does not contain", "…")],
        )
        .expect_err("a needle that matches nothing must refuse");
        assert!(
            gone.contains("matched 0 times"),
            "the refusal says how many times it matched: {gone}"
        );

        // MORE THAN ONE match — `replace` would rewrite a passage nobody
        // reviewed. `SQL` occurs many times in the skill body.
        let many = worker_instructions(&excluded, &[("SQL", "…")])
            .expect_err("a needle that matches more than once must refuse");
        assert!(
            !many.contains("matched 0 times") && many.contains("not once"),
            "the refusal distinguishes the two directions: {many}"
        );

        // And the real tables build every profile, today. `try_*` is the
        // live path; `new_with_profile` keeps panicking so tests still fail
        // loudly, and both must agree that nothing has drifted.
        for profile in [
            McpProfile::Default,
            McpProfile::Approver,
            McpProfile::Worker,
        ] {
            RockyMcpServer::try_new_with_profile(PathBuf::from("rocky.toml"), profile)
                .unwrap_or_else(|e| panic!("{profile:?} profile must build today: {e}"));
        }
    }

    /// TENTH ROUND, finding 3 — the OTHER rewrite path refuses the same
    /// way, and this test is the reason the check moved into a free
    /// function.
    ///
    /// The tool-description rewrite required only `contains` and then
    /// replaced EVERY occurrence, while the instruction rewrite one surface
    /// over required exactly one match. So "both projections fail closed"
    /// was true of one of them: a duplicated needle here rewrote a second
    /// passage nobody reviewed, silently.
    ///
    /// Driven on a synthetic description rather than a real one, for the
    /// same reason the sibling test above drives a synthetic table: the
    /// shipped descriptions match once, so the refusal is unreachable
    /// in-process and the claim would be untested on exactly the path this
    /// round exists to correct claims about.
    #[test]
    fn the_tool_description_projection_refuses_on_drift_in_both_directions() {
        // ZERO — the default description was edited under the projection.
        let gone = worker_tool_description(
            "plan_preview",
            "Preview the SQL Rocky would run.",
            "before proposing a materialization.",
            "before you hand off to the trusted runner.",
        )
        .expect_err("a needle that matches nothing must refuse");
        assert!(
            gone.contains("matched 0 times"),
            "the refusal says how many times it matched: {gone}"
        );

        // MORE THAN ONE — the failure this path used to take silently. The
        // old code called `contains`, saw true, and then replaced BOTH
        // occurrences; the second one is a passage the table's author never
        // read.
        let many = worker_tool_description(
            "plan_preview",
            "Read the SQL first. Read the SQL after the fix too.",
            "Read the SQL",
            "Call `plan_preview`",
        )
        .expect_err("a needle that matches more than once must refuse");
        assert!(
            many.contains("matched 2 times") && many.contains("not once"),
            "the refusal distinguishes the two directions: {many}"
        );

        // EXACTLY ONE — the live shape, and it rewrites only that
        // occurrence.
        let ok = worker_tool_description(
            "plan_preview",
            "Preview the SQL before proposing a materialization.",
            "before proposing a materialization.",
            "before you hand off to the trusted runner.",
        )
        .expect("the matching case still rewrites");
        assert_eq!(
            ok,
            "Preview the SQL before you hand off to the trusted runner."
        );

        // And the two paths now agree. Neither table may carry a needle
        // that matches its source more than once — this drives the real
        // tables, so an entry added later with a duplicated needle fails
        // here rather than at a worker's startup.
        let default = RockyMcpServer::new(PathBuf::from("rocky.toml"));
        for (name, needle, _) in WORKER_TOOL_DESCRIPTIONS {
            let description = default
                .tool_router
                .map
                .get(*name)
                .unwrap_or_else(|| panic!("default profile serves '{name}'"))
                .attr
                .description
                .as_deref()
                .unwrap_or_default();
            assert_eq!(
                description.matches(needle).count(),
                1,
                "the WORKER_TOOL_DESCRIPTIONS needle for '{name}' must match its default \
                 description exactly once, or the rewrite edits a passage nobody reviewed: \
                 {needle:?}"
            );
        }
    }

    /// An EMPTY needle matches nothing and, more to the point, does not
    /// abort the process (ninth review round).
    ///
    /// `"".find("")` succeeds at every offset, so the unguarded scan walked
    /// `from` one past the end and indexed `haystack[from..]` out of range.
    /// The shape matters: the run-off only happens when NO offset satisfies
    /// the boundary test, which needs a haystack whose LAST byte is an
    /// identifier byte. `"abc "` returned `true` at the space and never
    /// reached the end; `"abc"` panicked. A probe on the first shape would
    /// have reported the bug absent.
    ///
    /// Not reachable over MCP — both routers supply real tool names — but
    /// [`names_excluded_tool`] is exported from the crate root, so a caller
    /// can hand it one.
    #[test]
    fn an_empty_needle_matches_nothing_and_does_not_panic() {
        // The shape that PANICKED: every byte fails the boundary test, so
        // the scan ran off the end.
        assert!(
            !contains_identifier("abc", ""),
            "an empty needle is not an identifier"
        );
        // The shape that did NOT panic, kept so a future narrowing cannot
        // reintroduce the run-off by only fixing the loud case.
        assert!(
            !contains_identifier("abc ", ""),
            "and it does not match at a non-identifier byte either"
        );
        assert!(!contains_identifier("", ""), "empty haystack, empty needle");

        // One level up: a blank tool name derives NO forms. Without this,
        // the stem-plus-suffix rule yields `ing`/`ed`/`es`/`s`/`al`, and
        // `s` is an identifier in ordinary English — every swept surface
        // would start failing on prose.
        assert!(
            excluded_mention_forms("").is_empty(),
            "a blank tool name names nothing"
        );
        assert!(excluded_mention_forms("  ").is_empty(), "whitespace too");
        assert_eq!(
            names_excluded_tool("the s in this sentence", &["".to_string()]),
            None,
            "a blank excluded name matches no prose, and does not panic"
        );
    }

    /// Item 5c — the profile-selected draft `next_steps`: the worker variants
    /// name no excluded tool and end at the trusted-runner hand-off; the
    /// default variants are byte-unchanged (pinned), still ending at
    /// `propose` + human review.
    #[test]
    fn draft_next_steps_are_profile_selected() {
        let default_server = server_with(McpProfile::Default);
        assert_eq!(
            default_server.draft_model_next_steps(),
            "This is a draft — Rocky has NOT applied it or touched the warehouse. Continue the \
             authoring loop: fix any error diagnostics above and re-draft (or `compile`) until \
             it is clean, `plan_preview` to read the SQL that renders offline, then `propose` \
             to record an AI-authored plan for a human to `rocky review <plan_id> --approve` \
             and `rocky apply`. The preview is not the whole plan: a model it cannot render \
             offline is skipped and is not named, so a draft that succeeded here and is \
             missing from the preview is unrenderable offline, not absent from the project. \
             Never apply a draft directly.",
            "default draft_model next_steps are pinned byte-for-byte"
        );
        assert_eq!(
            default_server.draft_check_next_steps(),
            "This is a draft — Rocky has NOT applied it or touched the warehouse. The check is \
             merged into the model's sidecar and the project compiles; run the `test` tool to \
             EXECUTE the check against the data and confirm it passes. When it is clean, \
             `propose` to record an AI-authored plan for a human to `rocky review <plan_id> \
             --approve` and `rocky apply`. Never apply a draft directly.",
            "default draft_check next_steps are byte-unchanged"
        );

        let worker_server = server_with(McpProfile::Worker);
        for next_steps in [
            worker_server.draft_model_next_steps(),
            worker_server.draft_check_next_steps(),
        ] {
            assert_eq!(
                names_excluded_tool(next_steps, &worker_excluded_tool_mentions()),
                None,
                "worker next_steps must not name an excluded tool in any form: {next_steps}"
            );
            assert!(
                next_steps.contains("hand-off to the trusted runner"),
                "worker next_steps end at the runner hand-off: {next_steps}"
            );
        }

        // FIFTEENTH ROUND, finding 1 — the `plan_preview` exactness claim
        // on BOTH `draft_model` variants, which the round-fourteen sweep of
        // the description, the two prompt bodies and the docs table did not
        // reach.
        //
        // Pinned in BOTH directions and on BOTH profiles, for the reason
        // this whole test exists: the two bodies are near-identical, and a
        // one-sided edit is how round thirteen's `build_model` variants
        // nearly slipped. The default variant's byte-pin above already
        // catches its half; this catches the worker's, and it states WHICH
        // property is being held rather than leaving it to a diff.
        //
        // The harm is specific to this surface. A dynamic-table draft
        // SUCCEEDS, carries this text, and is then absent from the preview
        // it names — `commands::plan_preview_output` passes no warehouse and
        // skips what `sql_gen` cannot render, and `PlanPreviewResult` has no
        // field that names a skipped model.
        for (profile, next_steps) in [
            (McpProfile::Default, default_server.draft_model_next_steps()),
            (McpProfile::Worker, worker_server.draft_model_next_steps()),
        ] {
            assert!(
                !next_steps.contains("SQL Rocky would run"),
                "{profile:?}: `draft_model`'s next_steps call the preview the SQL Rocky WOULD \
                 RUN; it renders offline and silently drops what it cannot render: \
                 {next_steps}"
            );
            assert!(
                next_steps.contains("skipped and is not named"),
                "{profile:?}: `draft_model`'s next_steps must say a model the preview cannot \
                 render offline is dropped WITHOUT being named — a draft can succeed here \
                 and then be missing from the preview this text sends the agent to read: \
                 {next_steps}"
            );
        }
    }

    /// #1517 — the decision table for "may this server write a sign-off
    /// marker?", enumerated over EVERY profile rather than sampled. Approving
    /// is off unless the operator asked for it, and the `#[default]` variant
    /// is one of the profiles that cannot.
    ///
    /// The `McpProfile::default()` assertion is the load-bearing one: the
    /// whole issue was that the no-flag command pointed the wrong way, and
    /// `#[derive(Default)]` + `#[default]` means moving that attribute one
    /// variant down would silently arm approving for every existing agent.
    #[test]
    fn only_the_approver_profile_serves_the_approve_action() {
        assert_eq!(
            McpProfile::default(),
            McpProfile::Default,
            "the profile served with no flag is the one that cannot approve"
        );
        assert!(
            !server_with(McpProfile::Default).approve_action_served(),
            "default profile: approving is refused"
        );
        assert!(
            !server_with(McpProfile::Worker).approve_action_served(),
            "worker profile: approving is refused"
        );
        assert!(
            server_with(McpProfile::Approver).approve_action_served(),
            "approver profile: approving is served — the opt-in does something"
        );
    }

    /// #1517 — the opt-in enables an ACTION, it does not add a TOOL.
    ///
    /// Two things ride on this. The `briefs.rs` excluded-tool golden derives
    /// its list as default-minus-worker, so a refactor that tried to express
    /// the approve opt-in by adding or removing a ROUTE would silently move
    /// that golden. And the split itself: `review_queue` must still be served
    /// on the default profile, because listing the queue stays available.
    #[test]
    fn approver_profile_adds_an_action_not_a_tool() {
        let default_tools = server_with(McpProfile::Default).tool_names();
        let approver_tools = server_with(McpProfile::Approver).tool_names();
        assert_eq!(
            default_tools, approver_tools,
            "the approver profile serves exactly the default profile's tools"
        );
        assert!(
            default_tools.iter().any(|t| t == "review_queue"),
            "`review_queue` is still served on the default profile — listing is not gated"
        );

        // The worker profile is untouched by #1517: still the smaller
        // allowlist, still with no `review_queue` at all.
        let worker_tools = server_with(McpProfile::Worker).tool_names();
        assert!(
            worker_tools.len() < default_tools.len(),
            "the worker profile is still a strict subset"
        );
        assert!(
            !worker_tools.iter().any(|t| t == "review_queue"),
            "the worker profile still serves no `review_queue` at all"
        );
    }

    /// FF-WP-F3 — no worker-profile MCP route authors a declarative
    /// check, and nothing worker-facing invites the worker to try.
    ///
    /// This is a SECURITY boundary, not tidiness. A `[[tests]]` block's
    /// `expression` is raw-interpolated into `SELECT COUNT(*) FROM t WHERE
    /// NOT (<expression>)`, and `rocky_core::tests` says in so many words
    /// that the caller must sandbox execution. That contract held while the
    /// only caller was a human typing `rocky test --declarative`. F3 made
    /// the caller an unattended loop holding warehouse credentials, which
    /// no sandbox backs — so a check served to an untrusted worker is SQL
    /// the loop later executes after every apply.
    ///
    /// BE EXACT ABOUT THE SCOPE. This proves the MCP route is gone. It
    /// does not prove a worker cannot author a check: the subprocess
    /// driver runs an arbitrary command in the project root with no
    /// filesystem confinement, and Phase B preserves a worker-added
    /// `[[tests]]` block. A worker holding a file writer can still write
    /// the sidecar — the conceded local-process boundary, tracked by
    /// #1491 and #1515. The post-apply custody digest is what catches a
    /// sidecar edited after the generation was verified.
    ///
    /// Asserted on the ROUTED surface rather than on the allowlist constant,
    /// because the allowlist is an input to route removal and asserting it
    /// against itself would prove nothing.
    #[test]
    fn the_worker_profile_neither_serves_draft_check_nor_names_it() {
        let worker_tools = server_with(McpProfile::Worker).tool_names();
        assert!(
            !worker_tools.iter().any(|t| t == "draft_check"),
            "no worker-profile route may serve check authorship (a file writer still \
             can — #1491/#1515); served tools were {worker_tools:?}"
        );
        // Still served where the caller is an operator, not an untrusted
        // worker — the fix narrows a profile, it does not delete a tool.
        for profile in [McpProfile::Default, McpProfile::Approver] {
            assert!(
                server_with(profile)
                    .tool_names()
                    .iter()
                    .any(|t| t == "draft_check"),
                "{profile:?} keeps `draft_check`"
            );
        }

        // And no worker-facing TEXT steers toward it. A tool that is absent
        // from the listing but still named in the instructions or a prompt
        // description is the drift the brief validator exists to catch, one
        // layer earlier.
        let info = server_with(McpProfile::Worker).get_info();
        let instructions = info.instructions.unwrap_or_default();
        assert!(
            instructions.contains("draft_check"),
            "the worker banner must NAME the tool as absent, so a worker reading \
             the full authoring map knows where it stops"
        );
        for (name, description) in WORKER_PROMPT_DESCRIPTIONS {
            assert!(
                !description.contains("draft_check"),
                "worker prompt `{name}` steers toward a tool this profile does not \
                 serve: {description}"
            );
        }
    }
}
