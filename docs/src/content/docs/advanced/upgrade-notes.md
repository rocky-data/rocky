---
title: Upgrade Notes
description: One-time migration notes for breaking changes. Each note says what broke and the steps to fix it.
sidebar:
  order: 9
---

This page collects one-time migration notes. Each note names the release that broke something and the steps to fix it.

## Plan store: v1 to v2 (engine-v1.37.0)

`rocky plan` writes each plan to `.rocky/plans/<plan-id>.json`. `rocky apply` reads it back. For `rocky compact` and `rocky archive` plans, only the typed-IR **v2** envelope loads since engine-v1.37.0. The v1 inline-SQL envelope is retired. The `[plan_store]` config block is removed.

| What | Now |
|---|---|
| A v1 compact or archive plan | Fails at `rocky apply <plan-id>` with a migration message. |
| A `[plan_store]` block in `rocky.toml` | Fails to parse with an "unknown field" error. |
| `Run`, `Replication` and `Promote` plans | Unchanged. They never used the inline-SQL envelope. |
| `--output json` on `plan`, `compact`, `archive` | Unchanged. It still carries inline SQL. |

The plan path and the blake3 content-addressed `plan-id` are unchanged. Only the body changed. v2 holds a typed IR (`CompactPlanIr` or `ArchivePlanIr`, in `rocky-ir`). `rocky apply` regenerates the SQL from it at execution time.

### Fix a failing `rocky apply`

1. Delete any `[plan_store]` block from `rocky.toml`. The block no longer parses. Its old `format` setting did nothing after the change.
2. Re-plan. Run `rocky plan`, `rocky compact <model>`, or `rocky archive --older-than <age>`. This writes a v2 plan under a new `plan-id`.
3. Run `rocky apply <new-plan-id>`.

There is no in-place upgrade. A stale v1 file cannot be parsed. Re-planning reads the same `rocky.toml` and `models/`, so it is cheap.

```
  engine-v1.33.0        engine-v1.35.0        engine-v1.37.0
  writer: v1            writer: v2            writer: v2
  reader: v1 and v2     reader: v1 and v2     reader: v2 only
  v2 opt-in             v1 pinnable           [plan_store] removed
```
