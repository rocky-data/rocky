---
title: "Plan Store v1 to v2: Migration Guide"
description: Compact and archive plans now load only in the typed-IR v2 format. The v1 inline-SQL format and the [plan_store] config block are gone.
sidebar:
  order: 17
---

`rocky plan` writes each plan to `.rocky/plans/<plan-id>.json` for `rocky apply` to read back. For `rocky compact` and `rocky archive` plans, the typed-IR **v2** envelope is now the only shape that loads. The legacy **v1** inline-SQL envelope is retired, and the `[plan_store]` config block that chose between them has been removed.

## What's changed

- **A v1 plan no longer loads.** Compact and archive plans written before engine-v1.35.0, the v1 default era, fail at `rocky apply <plan-id>`. The error carries a migration message. Re-run `rocky compact` or `rocky archive` to write a fresh v2 plan.
- **The `[plan_store]` block has been removed.** A project that still carries it fails to parse `rocky.toml` with an "unknown field" error. That usually means a project pinned to `format = "v1"` after the v1.35.0 default flip. Delete the block.
- **`Run` / `Replication` / `Promote` plans are untouched.** They never used the inline-SQL envelope. They carry operational metadata (run and replication), or per-target SQL as a documented governance-audit exception (promote). Their on-disk shape is unchanged.

Stdout JSON is **unchanged**. `rocky plan --output json`, `rocky compact --output json`, and `rocky archive --output json` all still carry inline SQL for human and CI consumers. Only the persisted on-disk shape was simplified.

## What each format holds

The file path (`.rocky/plans/<plan-id>.json`) and the blake3 content-addressed `plan-id` are unchanged. Only the body shape changed:

- **v1 (retired).** The envelope carried **inline SQL** as the payload. `rocky apply` read the SQL out of the plan and sent it to the warehouse verbatim.
- **v2 (the only loadable shape).** The envelope carries the **typed-IR payload**: `CompactPlanIr` for `rocky compact` plans, `ArchivePlanIr` for `rocky archive` plans. `rocky apply` regenerates the SQL from that IR at execution time, through the `rocky_core::sql_gen::{compact_from_ir, archive_from_ir}` helpers in the `rocky-core` crate. The IR types themselves live in `rocky-ir`.

## Migration recipe

Follow this when `rocky apply <plan-id>` fails with a "plan is in format v1" error after an upgrade.

1. **Delete any `[plan_store]` block from your `rocky.toml`.** The block no longer parses, so `rocky.toml` fails on first load while it is still there. Its old `format` setting is now a no-op, because v2 is the only shape.
2. **Re-plan.** Run `rocky plan`, or `rocky compact <model>`, or `rocky archive --older-than <age>`. This writes a fresh v2 envelope under a new `plan-id`. The id is content-addressed, so the same intent against an unchanged source state yields the same id on any machine.
3. **Apply the new plan.** Run `rocky apply <new-plan-id>`. The v2 reader regenerates SQL from the typed IR and produces the same warehouse outcome the retired v1 path would have.

There is no in-place upgrade. A stale v1 envelope is simply not parseable. Re-planning is the only path, and it is cheap: it reads the same `rocky.toml` and `models/` you already have.

## Timeline

```
  engine-v1.33.0        engine-v1.35.0        this release
  ──────────────        ──────────────        ────────────
  writer: v1            writer: v2            writer: v2
  reader: v1 and v2     reader: v1 and v2     reader: v2 only
  v2 opt-in through     v1 pinnable through   [plan_store] block
  [plan_store]          [plan_store]          removed
        │                     │                     │
        ▼                     ▼                     ▼
  adopters could        the legacy shape      a v1 plan on disk
  validate v2 before    stayed available      no longer loads;
  the default flip      on request            re-plan to move on
```

The release notes that ship this change name the version it lands in.

## Related

- [Content-Addressed Materialization](/concepts/content-addressed/) — another writer surface where blake3 content hashing keys the on-disk artefact.
