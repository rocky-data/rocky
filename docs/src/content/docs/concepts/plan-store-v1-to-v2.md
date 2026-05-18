---
title: Plan Store v1 to v2 — Migration Guide
description: The typed-IR plan-store format is the default as of engine v1.35.0; this page covers the dual-reader migration window and how to opt back into v1 if you need it.
sidebar:
  order: 17
---

`rocky plan` persists each plan to `.rocky/plans/<plan-id>.json` for `rocky apply` to read back. As of engine v1.35.0 the **default** on-disk format is the typed-IR v2 envelope; the legacy v1 envelope is still selectable for projects that need it. Both formats share the same path and the same `plan-id` digest scheme — only the body changes.

This page explains what changed, what to do if you want to stay on v1, and how the dual-reader window works so existing plans on disk keep applying cleanly.

For the field-level reference of the config block itself, see [`[plan_store]`](/reference/configuration/#plan_store).

## What's changed in v1.35.0

The default value of `[plan_store] format` flipped from `"v1"` to `"v2"`. Concretely:

- Projects without an explicit `[plan_store]` block now write **v2** envelopes for `rocky compact` / `rocky archive`.
- Projects that already set `format = "v2"` see no change (the explicit value already resolved to v2).
- Projects that want to stay on the legacy on-disk shape must set `[plan_store] format = "v1"` explicitly.
- **Plans persisted under v1 before this upgrade keep reading cleanly.** The reader still accepts both formats unconditionally — see [Dual-reader migration window](#dual-reader-migration-window).

Stdout JSON (`rocky plan --output json`, `rocky compact --output json`, `rocky archive --output json`) is **unchanged** in both formats — inline SQL is always present for human and CI consumers. The format switch only affects what's written to `.rocky/plans/`.

## What changed in the previous release (v1.33.0)

The plan store is the on-disk envelope that `rocky plan` writes and `rocky apply` reads back. Both formats live at the same path (`.rocky/plans/<plan-id>.json`) and both are keyed by the same blake3 content hash — only the body changes:

- **v1 (legacy)** — the envelope ships **inline SQL** as the canonical payload. `rocky apply` reads the SQL out of the plan and submits it to the warehouse verbatim.
- **v2 (typed IR)** — the envelope ships the **typed-IR payload** (`CompactPlanIr` for `rocky compact` plans, `ArchivePlanIr` for `rocky archive` plans). `rocky apply` regenerates SQL from the IR at execution time via the `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}` helpers in the `rocky-ir` crate.

Only `CompactPlan` and `ArchivePlan` are affected. `PromotePlan` (governance, kept as a documented exception so the audit-grep surface stays intact) and `RunPlan` (already IR-only — `rocky apply` recompiles from `rocky.toml` + `models/` + flags) are untouched by this format switch.

## The `[plan_store]` config block

```toml
[plan_store]
format = "v1"  # opt back into the legacy on-disk shape
```

Valid values are `"v1"` and `"v2"` (default as of v1.35.0). The block is optional; omitting it selects v2. See the [`[plan_store]` reference](/reference/configuration/#plan_store) for the full field table.

## Dual-reader migration window

The reader accepts **both formats unconditionally** during the migration window. That means:

- A binary writing **v2** (the v1.35.0+ default) can still **read existing v1 plans** sitting in `.rocky/plans/` from before the flip. No re-plan is needed for in-flight workflows when you upgrade.
- A binary writing **v1** (explicit opt-back-in) can still **read v2 plans** if one of your runners has the default behaviour. Mixing formats across a fleet during a rollout is safe.
- The format key controls the **writer** only. There is no reader-side `[plan_store]` toggle in the migration window — the reader looks at the persisted plan's `format_version` tag and dispatches accordingly.

Practical consequence: upgrading the engine to v1.35.0 does not require you to re-plan anything that's already sitting on disk. CI artifacts, PR-review-held plans, Dagster-orchestrated `RockyResource.plan()` outputs from earlier runs — all keep applying cleanly.

## Recommended migration path

**Most users should accept the new v2 default** — it's been the recommended path since v1.33.0, ships a smaller and more auditable on-disk artefact, and is the only format that will survive the eventual v1 reader removal. No code or config changes are needed.

**Projects with custom dialects or non-trivial materialization strategies** (especially `compact` / `archive` over partitioned tables) that haven't yet validated against v2 in their own environment should do so before the v1 reader retires. A useful smoke test:

1. Generate a v1 plan by setting `[plan_store] format = "v1"` and running `rocky plan`. Stash the resulting `.rocky/plans/<plan-id>.json`.
2. Switch back to the default (or set `[plan_store] format = "v2"`) and run `rocky plan` against the same models / config.
3. Inspect the v2 envelope (typed IR is human-readable JSON — same shape as the inline-SQL one would be, minus the rendered SQL).
4. Run `rocky apply <plan-id>` against both — the warehouse outcomes should match. If they don't, the v2 writer has a regression worth reporting before v1 retires.

**Projects that need to stay on v1 for now** can set `[plan_store] format = "v1"` explicitly. The legacy shape is still fully supported in v1.35.0 — but plan to migrate before the v1 reader is removed (see below).

## When the v1 reader is removed

The v1 reader will be dropped in a subsequent minor release after v1.35.0; the exact version isn't yet pinned and will be announced in the corresponding release notes. After that:

- **Plans written under `format = "v1"`** in that release or later will be **unreadable** by `rocky apply`. There is no automatic in-place upgrade — a stale v1 envelope after the drop is simply not parseable, and `rocky plan` will produce a fresh v2 envelope on the next invocation.
- **Projects still pinning `format = "v1"`** will need to flip that to `"v2"` (or remove the explicit setting) before upgrading.
- **Existing v1 plans on disk** generated before the v1-reader-drop release will need to be re-planned to a v2 envelope before they can be applied.

The conservative path is to drop any explicit `format = "v1"` setting now, validate against your own pipeline on v1.35.0, and ride the future v1-reader-drop release as a no-op.

## Related

- [`[plan_store]` reference](/reference/configuration/#plan_store) — field-level config reference.
- [Content-Addressed Materialization](/concepts/content-addressed/) — another writer surface where blake3 content hashing keys the on-disk artefact.
