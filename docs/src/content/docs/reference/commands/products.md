---
title: Product Commands
description: Spec-driven data products — verify the trust posture, lower a spec onto engine primitives, approve a revision, and read product status
sidebar:
  order: 7
---

A product spec is one TOML file at `products/<name>.toml`. It declares WHAT a data product must be: its grain, columns, checks, freshness, and classifications. The spec adds no new runtime machinery. Every field lowers onto something the engine already enforces — a contract, declarative tests, sidecar metadata, or the policy posture. A field that cannot lower is refused at parse time, never shimmed.

The verbs on this page are the deterministic half of spec fulfillment. They parse, verify, lower, and approve. The agent loop that drafts SQL is [`rocky fulfill`](/reference/commands/fulfill/), and it is gated by the same policy plane as every other agent action.

```
products/<name>.toml ──verify──▶ posture + tags + collisions
        │
        ├──approve──▶ immutable snapshot + approval record (one transaction)
        │
        └──compile──▶ Phase A: models/<model>.contract.toml
                      Phase B: metadata merged into models/<model>.toml
                      (staged writes; the manifest rename commits)
```

---

## `rocky product verify`

Check that a product may be fulfilled, without writing anything.

```bash
rocky product verify revenue_daily
```

Three checks run in order, fail-closed:

1. **Trust posture.** `rocky.toml` must carry the frozen `propose_only` posture: a `[policy]` block with `default_agent_effect = "require_review"`, an agent `propose` rule that resolves `allow` and is scoped to exactly this product's output model (no `any`, no glob, no extra predicates, no autonomy budget), and agent `apply` resolving `require_review` or `deny`. A policy that merely *happens* to resolve safely is rejected — authority that reaches past the product is global agent authority. The verdicts come from the engine's own policy evaluator, run against the attributes the lowering is *about to create* (the post-image), so the gate cannot be evaded by the change under review.
2. **Classification tags.** Every tag in `output.classifications` must resolve to a `[mask]` strategy, a `[mask.<env>]` override, or `[classifications].allow_unmasked`. This is an error here; plain `rocky compile` only warns (W004). Resolution only: whether masking is applied is warehouse-dependent.
3. **Identity collisions.** The product name must not be claimed by another spec file, and the output model must not be claimed by another product.

Exit codes: `0` pass · `1` needs input (a paste-ready `[policy]` block is printed) · `2` fail.

### JSON output

`ProductVerifyOutput`: `status` (`pass` | `needs_input` | `fail`), `reason`, `paste_block`, and the resolved `propose_effect` / `apply_effect`.

---

## `rocky product compile`

Verify, then lower the spec. Refuses unless `verify` passes.

```bash
rocky product compile revenue_daily
```

Lowering runs in two phases because the drafting tool rewrites the model sidecar wholesale — metadata lowered before drafting would be destroyed:

- **Phase A** (before drafting) renders `models/<model>.contract.toml` — the spec-owned contract the draft must compile against.
- **Phase B** (after the drafted sidecar exists) merges the spec-owned metadata into `models/<model>.toml`: sources, the product tag, classifications, freshness, and the generated tests (grain uniqueness, `not_null` per non-nullable column, one `expression` test per check). The worker's `name`, `intent`, and appended tests survive.

The command picks the phase itself: Phase A on a fresh product, Phase B once `models/<model>.toml` exists. Re-running is safe — the merge is idempotent.

Every generation commits through staged same-directory writes, journaled, with the lowering manifest renamed **last** as the commit marker. A crash at any point rolls back on the next run: the previous generation is restored exactly. The journal is treated as untrusted input during recovery — forged, traversing, symlinked, or foreign entries are refused before anything mutates.

If an approval exists, compile re-verifies the snapshot's bytes against the approval digest before doing anything. A mismatch is tamper, and nothing proceeds.

### JSON output

`ProductCompileOutput`: the committed `phase`, each artifact with its `sha256`, the `manifest_path`, and the approval echo (`spec_matches_approval` is `false` when the working spec has moved past the approved revision — not an error; the loop treats it as supersession).

---

## `rocky product approve`

Record a human's approval of the current spec revision.

```bash
rocky product approve revenue_daily
```

Approval is one authority transition, in a fixed order:

1. The approved bytes are written to `.rocky/fulfillment/<name>/approved-<digest-hex>.toml` — immutable and digest-addressed. A digest-named file is never overwritten, so no reader can ever observe new-digest-old-bytes.
2. One state-store transaction records the approval, moves the product's fulfillment state to `spec_approved`, and appends the journal row. All or nothing.

A crash between step 1 and step 2 leaves only an orphan snapshot file — harmless; the next approve completes over it. Approving the already-approved digest is a no-op. When two people race, the second approve loses its compare-and-swap cleanly and prints the winning digest.

The approver identity is the same best-effort git identity the review marker records. It is an attribution, not an authentication.

### JSON output

`ProductApproveOutput`: `spec_digest`, `approver`, `approved_at`, `snapshot_path`, `previous_state`, and `already_approved`.

---

## `rocky product status`

Read-only report. Writes nothing, recovers nothing.

```bash
rocky product status revenue_daily
```

Reports: the spec's identity (or its parse error), the committed lowering phase, byte-verification of every committed artifact against the manifest, whether a crashed commit's journal is pending (the next `compile` resolves it), the approval record with snapshot integrity, whether the working spec still matches the approved revision, and the persisted fulfillment state with its journal row count.

### JSON output

`ProductStatusOutput` — every field above, machine-readable.

---

## Validation codes

`rocky validate` checks `products/` offline with its own code band:

| Code | Severity | Meaning |
|------|----------|---------|
| `V050` | ok | Every spec in `products/` parsed. |
| `V051` | error | A spec fails the strict parser; the message carries the parser's stable reject code. |
| `V052` | error | A spec's `product.name` disagrees with its file name. |
| `V053` | error | Two specs claim the same output model. |
