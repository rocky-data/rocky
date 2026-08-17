# Architecture Decision Records

This directory holds **ADRs** — durable records of significant, hard-to-reverse engine design decisions. An ADR states the *context* (the problem + what's already true in-tree), the *decision* (the design, grounded in real symbols/paths), its *consequences* (including what it does **not** close), the *alternatives* considered, and how the decision is *validated*.

Convention: one file per decision, `ADR-<TOPIC>.md`, status one of **Proposed** → **Accepted** → **Superseded**. Code claims are symbol-anchored; line numbers are approximate. A design gate ("stops for sign-off") is a Proposed ADR that blocks implementation until accepted.

## WP-01 — Remote-state protocol redesign (PR-0 design gate)

Closes the 3 open Critical audit findings (RD-001/002/003) plus #1120 (freeze kill-switch / config-swap TOCTOU) and #1093 (governance-from-gated-snapshot), on a **live, concurrent multi-pod** `[state]` deployment. Read in this order:

| # | ADR | PR | Closes |
|---|---|---|---|
| 1 | [`ADR-AUTHORITY.md`](ADR-AUTHORITY.md) | PR-A | RD-001 — typed `StateAuthority` (Authoritative / FreshStart / Indeterminate); the safe-standalone keystone. |
| 2 | [`ADR-STATE-SESSION.md`](ADR-STATE-SESSION.md) | PR-B | RD-003 (bypass), #1120 (config-swap TOCTOU incl. promote), #1093 (`GovernanceSnapshot`) — the `RemoteStateSession` spine. |
| 3 | [`ADR-CONCURRENCY.md`](ADR-CONCURRENCY.md) | PR-C→PR-D (+ spine-urgent PR-F) | RD-002 (CAS: retry-seams / refuse-runs) + the rollout-independent add-wins freeze marker. |

Staging: **spine-first** (PR-A → PR-B → PR-F) then the CAS **fast-follow** (PR-C consistent snapshot → PR-D CAS), closed by an operational **rollout gate** (fleet-wide deploy → `concurrency_control = "cas"` on every live prefix → doctor-verified effective-CAS). Until that gate completes, RD-002 exposure is bounded by orchestrator-level per-`[state]`-prefix writer serialization.

These ADRs went through three adversarial review rounds (a strategic-plan red team, an independent per-ADR second review, and a red team over the implementation plan); the corrections from all three are folded in.

These three ADRs were authored under adversarial review (Codex, 10 findings dispositioned) and a cross-consistency pass. **Status: Accepted (2026-07-17) — implementation in progress.**
