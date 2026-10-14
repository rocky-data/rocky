# Architecture Decision Records

This directory holds **ADRs** — durable records of significant, hard-to-reverse engine design decisions. An ADR states the *context* (the problem + what's already true in-tree), the *decision* (the design, grounded in real symbols/paths), its *consequences* (including what it does **not** close), the *alternatives* considered, and how the decision is *validated*.

Convention: one file per decision, `ADR-<TOPIC>.md`, status one of **Proposed** → **Accepted** → **Superseded**. Code claims are symbol-anchored; line numbers are approximate. A design gate ("stops for sign-off") is a Proposed ADR that blocks implementation until accepted.

## WP-01 — Remote-state protocol redesign (PR-0 design gate)

Closes the 3 open Critical audit findings (RD-001/002/003) plus #1120 (freeze kill-switch / config-swap TOCTOU) and #1093 (governance-from-gated-snapshot), on a **live, concurrent multi-pod** `[state]` deployment. Read in this order:

| # | ADR | Status | PR | Closes |
|---|---|---|---|---|
| 1 | [`ADR-AUTHORITY.md`](ADR-AUTHORITY.md) | Accepted (2026-07-17) | PR-A | RD-001 — typed `StateAuthority` (Authoritative / FreshStart / Indeterminate); the safe-standalone keystone. |
| 2 | [`ADR-STATE-SESSION.md`](ADR-STATE-SESSION.md) | Accepted (2026-07-17) | PR-B | RD-003 (bypass), #1120 (config-swap TOCTOU incl. promote), #1093 (`GovernanceSnapshot`) — the `RemoteStateSession` spine. |
| 3 | [`ADR-CONCURRENCY.md`](ADR-CONCURRENCY.md) | Accepted (2026-07-17) | PR-C→PR-D (+ spine-urgent PR-F) | RD-002 (CAS: retry-seams / refuse-runs) + the rollout-independent add-wins freeze marker. |

Staging: **spine-first** (PR-A → PR-B → PR-F) then the CAS **fast-follow** (PR-C consistent snapshot → PR-D CAS), closed by an operational **rollout gate** (fleet-wide deploy → `concurrency_control = "cas"` on every live prefix → doctor-verified effective-CAS). Until that gate completes, RD-002 exposure is bounded by orchestrator-level per-`[state]`-prefix writer serialization.

These ADRs went through three adversarial review rounds (a strategic-plan red team, an independent per-ADR second review, and a red team over the implementation plan); the corrections from all three are folded in.

These three ADRs were authored under adversarial review (Codex, 10 findings dispositioned) and a cross-consistency pass. **Status: Accepted (2026-07-17) — implementation in progress.**

## WP-03 / WP-04 — Contract semantics (design gate)

| ADR | Status | Closes |
|---|---|---|
| [`ADR-CONTRACTS.md`](ADR-CONTRACTS.md) | Accepted (ratified 2026-10-08) | RD-011 (recursive contract compatibility, `Unknown` policy), RD-012 (default-breaking classification), RD-013 (promotion fails closed). Sets the contract semantics WP-04 builds on. |

## WP-00 / WP-04 / WP-07 / WP-08 — Identity, approval and trust boundaries (design gate)

| ADR | Status | Closes |
|---|---|---|
| [`ADR-TRUST.md`](ADR-TRUST.md) | Accepted (ratified 2026-10-08) | The design half of RD-026, RD-035, RD-043 and RD-045. Defines verified principals, signed approvals, the worker / loop / operator boundary, and break-glass (ADR-CONTRACTS Open question E). |

## WP-03 — Type inference semantics (design gate)

| ADR | Status | Closes |
|---|---|---|
| [`ADR-TYPES.md`](ADR-TYPES.md) | Accepted (ratified 2026-10-08) | RD-010 (decimal arithmetic, literal typing, digit validation), the inference half of RD-028. Defines the sound-bound rule, `Unknown` in inference, nullability rules, cross-dialect mapping classes, and inference versioning. Decides the unmerged WP-03 decimal branch (Open question F). |
