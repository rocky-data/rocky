# ADR-FRAMEWORK — A declarative fulfillment framework on Rocky (spec → reconciler → feedback)

**Status: Proposed** — design gate: blocks implementation until accepted. Authored 2026-08-17. This ADR records product/architecture direction (repo placement, licensing, layer boundaries), not an engine internal; the engine-facing consequences are called out explicitly.

## Context

Rocky today is imperative infrastructure with guardrails: a human or agent authors models, configs, and contracts; the engine checks (`rocky compile`), gates (`plan` → `[policy]` → `apply`), and records (`rocky audit`). Exploration of four verticals (advertising agencies, health, carbon/energy reporting, sports) surfaced one invariant pattern: **a small team plus an AI agent operating a governed warehouse over drifting third-party sources, producing numbers that must survive outside scrutiny**. In every vertical the adoption bottleneck is trust, not capability — which is exactly the surface Rocky already enforces.

Every verb of an autonomous authoring loop already exists as an MCP tool in `engine/crates/rocky-mcp/src/tools.rs`:

- inspect: `inspect_schema`, `sample_rows`, `profile_column`, `profile_table_columns`
- draft: `draft_model`, `draft_contract`, `draft_check`, `ai_contract`, `ai_test`
- verify: `compile`, `test`, `breaking_change`, `dependents`, `lineage`
- gate: `propose`, `plan_preview` (write paths pass the `[policy]` plane)
- observe: `drift_preview`, `metrics`, `history`, `audit_query`
- repair: `fix_failing_test`

The loop itself is documented as the `rocky-ai-workflow` skill (inspect → sample → SQL → compile-loop → plan → propose → review → apply). So the framework exists today **as convention**: a document an agent follows if asked. What is missing is (1) a machine-readable goal artifact, (2) a runtime that drives the loop toward the goal continuously, and (3) feedback that compounds instead of evaporating after each review.

## Decision

Build the framework as three thin layers above the engine, plus vertical packs.

**Layer 1 — outcome spec (`product.toml`).** A declarative artifact stating the *what* the way `rocky.toml` states the *how*: source (discovery adapter ref + include patterns), expected output (grain, columns/types/nullability, freshness, checks, classifications), and trust posture. The spec is simultaneously the agent's prompt and its acceptance test. **Compile-down rule (hard constraint):** every spec field lowers 1:1 onto existing primitives — `[pipeline.*.contract]`, `[pipeline.*.checks]`, classifications, `[policy]` defaults. The spec introduces **no new runtime semantics**; this is the guard against growing a second config language.

**Layer 2 — reconciler runtime.** A loop runner (`fulfill <product>`; later a daemon) that drives the MCP tools: discover → sample → *propose the precise spec for human approval* → draft → compile-loop → generate tests from intent → `propose` → review per policy → apply → keep watching. Continuous operation wires `[hook.on_drift_detected]` and `[hook.on_check_fail]` to repair proposals instead of alerts. Spec elicitation is deliberately the loop's **first** job: users cannot state grain/timezone/currency semantics up front, so the agent derives a candidate spec from source samples and the human approves the spec before any SQL exists. The runtime lives **above the engine** (Python, on `rocky-sdk`), is agent-agnostic (assumes only MCP), and the engine stays deterministic Rust with no LLM calls — probabilistic above the line, deterministic below, the lower gating the upper.

**Layer 3 — feedback surface.** Every human review decision produces a durable artifact diff: reject-with-reason → proposed `[policy]` tightening or added check; approved edit → spec update or a note in the product's knowledge file loaded next run. Steering changes are themselves plans passing the same plan/apply gate — the mechanism that governs the agent also governs re-governing it. A trust ratchet *suggests* (never applies) widening a product's trust posture after N approved plans, operationalizing the existing "an agent earns freedom one step at a time" stance.

**Packs.** A vertical = a spec template + seed contracts + a policy preset + a domain knowledge doc. The framework is horizontal; packs are thin skins (agency and carbon first).

**Placement: this monorepo.** New top-level subproject mirroring `integrations/dagster`: own `AGENTS.md` + `CLAUDE.md` shim, `pyproject.toml` with a `[tool.uv.sources]` path dependency on `rocky-sdk`, own path-filtered CI workflow, own `framework-v*` tag namespace. The Phase-1 walking skeleton starts as a credential-free DuckDB POC under `examples/playground/pocs/`. **Extraction seam (hard constraint):** the framework depends only on the published interface — the `rocky` binary over subprocess/MCP and `rocky-sdk` — never on Rust source or engine internals, keeping a future repo split mechanical.

**License: Apache 2.0, permanently, for everything in this repo.** Born-open/born-closed rule: any component intended for commercialization (a hosted control plane: multi-tenant review console, fleet management, certified pack subscriptions) is *born* closed in a separate repository when demand is proven; nothing shipped Apache is ever relicensed. Trademark and governance of the spec format are retained. Rationale: adoption is the scarce resource; the product's trust argument requires an inspectable gate; the spec is a standard play and standards must be open to win; the relicensing asymmetry (HashiCorp/BSL precedent) makes born-closed the only safe way to hold a commercial option.

## Consequences

- One artifact proves the pattern across all four verticals; onboarding a use-case becomes writing (or eliciting) a spec.
- Human review shifts from SQL to outcomes-vs-spec and trust budgets; SQL review remains available.
- Release/CI machinery is reused; expected lockstep engine changes (spec registration, plan↔product linkage) land as single PRs under the `codegen-drift` gate.
- **Semantic-correctness gap remains and is stated, not marketed away:** the engine proves types/refs/contracts, never business meaning. `checks` + `ai_test` assertions narrow the gap; human review closes it. The review UX (samples, diffs, lineage — the Inspector's ingredients) is a first-class part of the framework.
- New maintenance surface; fast-churning LLM/agent Python dependencies are confined to the subproject's own lockfile, outside the Cargo workspace.
- **Not closed by this ADR:** the framework/product name; the exact spec schema; whether existing `propose`/`draft_*` argument surfaces suffice or new engine verbs are needed (untraced — first implementation task); the commercial control plane's design (deliberately out of scope; separate repo when earned).

## Alternatives considered

- **Separate repo now** — rejected while the engine⇄framework surface still moves (`rocky-mcp` and `schemas/` each touched in 6 of the last 50 commits over 3 months) and lockstep verbs are expected; `AGENTS.md` names lockstep cross-project change as the reason this is one repo. Recorded extraction triggers: (1) license/commercial divergence, (2) a pack-author audience for whom a monorepo checkout is friction, (3) the MCP surface + spec format declared stable/versioned.
- **Inside the engine (Rust)** — rejected: violates deterministic-below-the-line; couples engine releases to agent-stack churn.
- **Commercial/BSL license on the framework** — rejected: paywalls a product with zero users; chills the pack ecosystem; undermines the inspectable-gate argument; the born-closed rule preserves the commercial option without ever relicensing.
- **Remain a convention (skill + MCP, no runtime)** — rejected as an end state: conventions don't compound feedback and can't carry a product surface. But it is the valid Phase 0 and ships value today.

## Validation

- **Phase 0 (now):** the loop is exercisable manually via the `rocky-ai-workflow` skill + `rocky mcp` with a capable agent; policy + review queue + audit close the gate.
- **Phase 1 gate:** spec format + `fulfill` walking skeleton as a credential-free DuckDB POC. Acceptance: a spec → agent-produced pipeline passing its compiled contract/checks, with the propose/review path exercised end-to-end; verify whether existing MCP argument surfaces suffice.
- **Phase 2:** feedback compounding (review reasons → proposed spec/policy diffs) + agency and carbon packs.
- **Phase 3:** continuous daemon + trust-ratchet suggestions.
- **Demand validation precedes any commercial-adjacent work:** two design-partner conversations in one vertical; a component is commercialized only after someone has asked to pay for it twice.

## Standing questions (per `AGENTS.md`)

- **Least confident:** sufficiency of current MCP tool argument surfaces for spec-level operations (untraced); whether the pattern's invariance holds at implementation depth in each vertical (claims formats and emissions factors may resist the shared template more than the synthesis suggests); market-timing claims rest on January-2026 knowledge.
- **Most important possibly-missing thing:** naming/positioning — whether this ships as *part of Rocky* (`rocky fulfill`) or as a product *on* Rocky with its own name determines where issues, docs, and community accrete, and precedes the first public commit. Secondarily: procurement behavior of vertical buyers (who signs, what triggers purchase), which decides whether "one pattern, N skins" works commercially.

## Review disposition

Authored in a session where the independent red team required by `AGENTS.md` (the Codex plugin per `CLAUDE.md`; profile in `AGENT_REVIEW.md`) was unavailable — stated here rather than skipped. **This ADR stays Proposed until an independent-model review runs and its findings are dispositioned in this file.** Per house convention, Proposed blocks implementation; the Phase-1 POC may be prototyped for review purposes but nothing lands as accepted direction before that pass.
