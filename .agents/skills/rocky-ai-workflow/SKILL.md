---
name: rocky-ai-workflow
description: How an AI agent should author or modify a Rocky data model. Use when building, fixing, or evolving a model on behalf of a user — covers the inspect → sample → write SQL → compile-loop → plan → propose → review → apply workflow, the reconcile discipline (check the data, not just the schema), and the AI-authored-plan safety gate. SQL-first.
---

# Authoring Rocky models as an agent

This is the workflow for an AI agent that has been asked to build or change a Rocky model. It assumes you can run the `rocky` CLI (or call the equivalent tools) and read its `--output json`. For the config format see the `rocky-config` skill; for the full command surface see the `rocky` skill. This skill is specifically about *how to converge on a correct model* and *how to ship it safely*.

The shape of the job: **you propose, Rocky's compiler verifies, a human approves the invariants.** Your edits are not trusted because they compiled — they're trusted because the typed substrate checked them and a person signed off.

## Author SQL, not the DSL

Write models as **raw SQL** (`models/<name>.sql` + a `<name>.toml` sidecar for materialization). SQL is first-class in Rocky and you are fluent in it. The `.rocky` DSL exists and is fully supported, but it is a niche surface — reach for it only when the user explicitly asks. Defaulting to SQL gets you correct models faster.

## The loop

1. **Inspect the schema.** Run `rocky compile --output json`. The result gives you every existing model and source table with its typed columns. Use this to learn what's available to select from and what the upstream types are — never guess column names.

2. **Sample the data — do not trust the schema alone.** This is the step that separates a model that *compiles* from a model that is *correct*. Before you write a filter or a cast, look at real rows. On the DuckDB playground that's a direct query (`duckdb <path> "SELECT * FROM <table> USING SAMPLE 20 ROWS"`) or `rocky shell`; against a warehouse, sample through the adapter. Check the things a schema can't tell you:
   - **Literal values.** Does `status` actually contain `'completed'`, or `'COMPLETE'`, or `'C'`? A `WHERE status = 'completed'` that returns zero rows compiles perfectly.
   - **Units and scale.** Is `amount` in dollars or cents? Is a timestamp UTC or local?
   - **Null rates and domains.** How often is a column null? What are its distinct values?

3. **Write the model.** Author the SQL and its `.toml` sidecar (materialization strategy, target). Keep it minimal and readable.

4. **Compile-loop on diagnostics.** Run `rocky compile --output json` and read `diagnostics`: each carries a `code` (e.g. `E001`, `W003`), a `message`, a source `span`, the `model`, and often a `suggestion`. Fix against the diagnostic, recompile, repeat until clean. The compiler is your fast feedback loop — lean on it instead of reasoning about correctness in your head.

5. **Preview the SQL.** Run `rocky plan` to see exactly what would execute. Read it. Confirm the generated SQL matches your intent.

6. **Test.** Run `rocky test` to exercise assertions (uniqueness, not-null, accepted values, ranges). Add or strengthen assertions that encode what you learned from sampling — they become the contract that protects the model from future drift.

## Shipping safely: propose → review → apply

**Never apply an AI-authored change directly.** A bare `rocky apply` of an AI-authored plan is refused by design — an agent can confidently write a model that drops a column or rewrites a result, so a human checkpoint is mandatory.

The path:

1. **Propose.** Generate the plan that materializes your change (it is recorded as an *AI-authored* plan with a `plan_id`).
2. **Review.** Run `rocky review <plan-id>`. This compiles your working tree against the base ref and runs the semantic breaking-change classifier, then reports the delta — added/removed/retyped columns, anything downstream consumers depend on. Read it.
3. **Approve.** A human runs `rocky review <plan-id> --approve` to sign off. Approving over breaking changes is allowed, but the report makes those changes loud — the sign-off is informed, never silent.
4. **Apply.** Only after the approval marker exists does `rocky apply <plan-id>` execute.

Your job ends at *propose* and at *surfacing the review report clearly*. The approval is a human decision; do not approve on the user's behalf unless they explicitly tell you to.

## Reading the machine-readable surface

- Every command takes `--output json`, backed by a typed schema. That JSON — not the human text — is your contract. Parse it.
- Compile **diagnostics** carry `code` / `span` / `suggestion`: act on the suggestion.
- Run **errors** carry a `failure_kind` (`Transient`, `AuthFailed`, `QueryRejected`, `QuotaExceeded`, …) and sometimes a `cooldown_seconds`. Branch on *why* something failed: retry a `Transient`, stop and surface an `AuthFailed`.

## Let the compiler hold the invariants

When you learn something durable about the data while sampling — a column is never null, a status takes a fixed set of values, a key is unique — encode it as a **contract** (`required`/`protected` columns) or a **check** (assertion), not just as a `WHERE` clause. That moves the invariant into the typed substrate, so the human reviews *the invariant* and the compiler enforces it on every future run. This is the whole point of authoring on Rocky rather than emitting bare SQL: the guardrails are part of the artifact.

## Anti-patterns

- Writing a filter from the column name without sampling the values. (The reconcile bug.)
- Treating "it compiled" as "it's correct."
- Applying without review, or approving your own AI-authored plan.
- Reaching for the `.rocky` DSL when SQL would do.
- Reading the human-text output when the JSON is right there.
