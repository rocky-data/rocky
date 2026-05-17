---
title: Plan Store v1 to v2 — Migration Guide
description: Opt in to the typed-IR plan-store format on `rocky plan`, and how the dual-reader migration window works
sidebar:
  order: 17
---

`rocky plan` persists each plan to `.rocky/plans/<plan-id>.json` for `rocky apply` to read back. As of engine v1.33.0 there are **two on-disk formats** for that artefact — the legacy v1 envelope and the new typed-IR v2 envelope — selectable via a single config bit. The default is v1; v2 is opt-in for this release.

This page explains what changed, how to opt in, and how the migration window is designed so you can flip formats without breaking existing plans on disk.

For the field-level reference of the config block itself, see [`[plan_store]`](/reference/configuration/#plan_store).

## What changed

The plan store is the on-disk envelope that `rocky plan` writes and `rocky apply` reads back. Both formats live at the same path (`.rocky/plans/<plan-id>.json`) and both are keyed by the same blake3 content hash — only the body changes:

- **v1 (legacy)** — the envelope ships **inline SQL** as the canonical payload. `rocky apply` reads the SQL out of the plan and submits it to the warehouse verbatim.
- **v2 (typed IR)** — the envelope ships the **typed-IR payload** (`CompactPlanIr` for `rocky compact` plans, `ArchivePlanIr` for `rocky archive` plans). `rocky apply` regenerates SQL from the IR at execution time via the `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}` helpers in the `rocky-ir` crate.

Only `CompactPlan` and `ArchivePlan` are affected. `PromotePlan` (governance, kept as a documented exception so the audit-grep surface stays intact) and `RunPlan` (already IR-only — `rocky apply` recompiles from `rocky.toml` + `models/` + flags) are untouched by this format switch.

The stdout JSON (`rocky plan --output json`, `rocky compact --output json`, `rocky archive --output json`) is **unchanged in both formats** — inline SQL is always present for human and CI consumers. The format switch only affects what's written to `.rocky/plans/`.

## The `[plan_store]` config block

```toml
[plan_store]
format = "v2"
```

Valid values are `"v1"` (default) and `"v2"`. The block is optional; omitting it picks the v1 default. See the [`[plan_store]` reference](/reference/configuration/#plan_store) for the full field table.

## Dual-reader migration window

The reader accepts **both formats unconditionally** during the migration window. That means:

- A binary configured to **write v2** can still **read existing v1 plans** sitting in `.rocky/plans/` from before the flip. No re-plan needed for in-flight workflows.
- A binary configured to **write v1** can still **read v2 plans** if one of your runners has flipped ahead. Mixing formats across a fleet during a rollout is safe.
- The format key controls the **writer** only. There is no reader-side `[plan_store]` toggle in the migration window — the reader looks at the envelope's `kind` discriminator and dispatches accordingly.

Practical consequence: you can flip a single project, runner, or CI lane to `format = "v2"` to soak the new path without coordinating a fleet-wide switch.

## Recommended migration path

**Most users should stick with the default (`format = "v1"`)** until the default flips in a future minor release. Nothing about your pipeline changes by waiting.

**Early adopters** who want to validate the v2 writer against their own plans can opt in now by adding `[plan_store] format = "v2"` to `rocky.toml`. A useful smoke test:

1. Run `rocky plan` with `format = "v1"` and stash the resulting `.rocky/plans/<plan-id>.json`.
2. Run `rocky plan` with `format = "v2"` against the same models / config.
3. Inspect the v2 envelope (typed IR is human-readable JSON — same shape as the inline-SQL one would be, minus the rendered SQL).
4. Run `rocky apply <plan-id>` against both — the warehouse outcomes should match. If they don't, the v2 writer has a regression worth reporting before the default flip lands.

For projects with custom dialects or non-trivial materialization strategies (especially `compact` / `archive` over partitioned tables), early-opt-in is the cheapest way to surface a regression in your own environment before the default change makes it the only path.

## When the default flips

The default will flip from `"v1"` to `"v2"` in a future minor release; the v1 reader will be dropped in a subsequent minor release after that. The exact versions aren't yet pinned and will be announced in the corresponding release notes.

When that happens:

- **Users on `format = "v2"` already** see no change — their writes are unaffected, and their existing v2 plans on disk keep reading cleanly.
- **Users still on the default** will start writing v2 envelopes after the flip. Plans written **before** the flip stay readable through the rest of the dual-reader window; plans written **after** the flip require a binary that still has the v2 reader (every version from v1.33.0 onward).
- **Users who reach the v1-reader-drop release** without having migrated will need to **re-plan** any stale v1 envelopes still sitting in `.rocky/plans/`. There is no automatic in-place upgrade — a stale v1 envelope after the drop is simply not readable, and `rocky plan` will produce a fresh v2 envelope on the next invocation.

The conservative path is to flip to v2 well before the default change, validate against your own pipeline, and then ride the default-flip release as a no-op.

## Related

- [`[plan_store]` reference](/reference/configuration/#plan_store) — field-level config reference.
- [Content-Addressed Materialization](/concepts/content-addressed/) — another writer surface where blake3 content hashing keys the on-disk artefact.
