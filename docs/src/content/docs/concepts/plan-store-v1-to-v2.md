---
title: "Plan Store v1 to v2: Migration Guide"
description: The typed-IR plan-store format is the only loadable shape for compact / archive plans; the v1 inline-SQL envelope was retired and the [plan_store] config block was removed.
sidebar:
  order: 17
---

`rocky plan` persists each plan to `.rocky/plans/<plan-id>.json` for `rocky apply` to read back. The typed-IR **v2** envelope is the only loadable shape for `rocky compact` / `rocky archive` plans; the legacy **v1** inline-SQL envelope has been retired and the `[plan_store]` config block that selected between them has been removed.

## What's changed

- **The v1 inline-SQL envelope is no longer readable.** Compact / archive plans written by Rocky < engine-v1.35.0 (the v1 default era) error out at `rocky apply <plan-id>` with a clear migration message. Re-run `rocky compact` / `rocky archive` to write a fresh v2 plan.
- **The `[plan_store]` config block has been removed.** Projects still carrying it (typically pinned to `format = "v1"` after the v1.35.0 default flip) fail to parse `rocky.toml` with an "unknown field" error. Remove the block entirely.
- **`Run` / `Replication` / `Promote` plans are untouched.** These never used the inline-SQL envelope; they ship operational metadata (run/replication) or per-target SQL as a documented governance-audit exception (promote). Their on-disk shape is unchanged.

Stdout JSON (`rocky plan --output json`, `rocky compact --output json`, `rocky archive --output json`) is **unchanged**: inline SQL is always present for human and CI consumers. Only the on-disk persisted shape was simplified.

## Format recap

The path (`.rocky/plans/<plan-id>.json`) and the blake3-content-addressed `plan-id` are unchanged; only the body shape changed:

- **v1 (legacy, retired)** — the envelope shipped **inline SQL** as the canonical payload. `rocky apply` read the SQL out of the plan and submitted it to the warehouse verbatim. This is the shape Rocky < engine-v1.35.0 produced by default.
- **v2 (typed IR, now the only loadable shape)** — the envelope ships the **typed-IR payload** (`CompactPlanIr` for `rocky compact` plans, `ArchivePlanIr` for `rocky archive` plans). `rocky apply` regenerates SQL from the IR at execution time via the `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}` helpers in the `rocky-ir` crate.

## Migration recipe

If `rocky apply <plan-id>` fails with a "plan is in format v1" error after upgrading:

1. **Drop any `[plan_store]` block from your `rocky.toml`.** The block no longer parses; `rocky.toml` will fail on first load if it's still there. The previous `format = "v2"` or `format = "v1"` setting is now a no-op (v2 is the only shape).
2. **Re-run `rocky plan`** (or `rocky compact <model>` / `rocky archive <model>`). This produces a fresh v2 envelope with a new `plan-id`, content-addressed, so identical intent against an unchanged source state yields a stable id across machines.
3. **Apply the new plan**: `rocky apply <new-plan-id>`. The v2 reader regenerates SQL from the typed IR at execution time, producing the same warehouse outcomes the retired v1 path would have.

There is no automatic in-place upgrade: a stale v1 envelope is simply not parseable. Re-plan is the only path forward, and it's a cheap operation on the same `rocky.toml` + `models/` you already have.

## Timeline

- **engine-v1.33.0** — shipped `format = "v2"` as opt-in alongside the v1 default. Adopters could validate v2 against their pipeline before the default flip.
- **engine-v1.35.0** — flipped the writer default to `"v2"`; the reader still accepted both formats. Operators who wanted the legacy shape pinned `format = "v1"` explicitly.
- **A later minor (this drop)** — retired the v1 reader for compact / archive plans and removed the `[plan_store]` block. v2 is the only loadable shape; the version that ships this change is announced in the release notes alongside this guide.

## Related

- [Content-Addressed Materialization](/concepts/content-addressed/) — another writer surface where blake3 content hashing keys the on-disk artefact.
