---
title: Contributing
description: Where each kind of change belongs in the Rocky monorepo, and where the build, test, CI and release steps live
sidebar:
  order: 6
---

Rocky lives in one repository: the Rust engine and its browser UI, two Python packages, a VS Code extension, a sample pipeline, and this docs site. One pull request can change several of them.

[`CONTRIBUTING.md`](https://github.com/rocky-data/rocky/blob/main/CONTRIBUTING.md) is the canonical guide. It covers setup, build and test commands, code style, commit messages, CI, merge strategy and releases. This page answers one question: where does my change belong?

You can also contribute without code. Use the [first-run feedback form](https://github.com/rocky-data/rocky/issues/new?template=first_run_feedback.yml). Use [private security reporting](https://github.com/rocky-data/rocky/security/advisories/new) for vulnerabilities.

## Where to start

The right entry point depends on what you want to change.

| You want to change | Start here |
|---|---|
| How a warehouse executes SQL | `engine/crates/rocky-<name>/` — the adapter crate. Implement `WarehouseAdapter` / `SqlDialect` from `rocky-core::traits`. An out-of-tree process adapter uses `rocky-adapter-sdk` instead. |
| How SQL is generated from the IR | `engine/crates/rocky-core/src/sql_gen.rs` |
| What the IR looks like (model shape) | `engine/crates/rocky-ir/src/ir.rs` |
| The type checker or semantic graph | `engine/crates/rocky-compiler/src/` |
| A diagnostic code (error/warning) | `engine/crates/rocky-compiler/src/diagnostic.rs` |
| Incremental / watermark logic | `engine/crates/rocky-core/src/state.rs` + `sql_gen.rs` |
| The `rocky run` execution loop | `engine/crates/rocky-cli/src/commands/run.rs` |
| DAG / dependency resolution | `engine/crates/rocky-ir/src/dag.rs` |
| Data contracts enforcement | `engine/crates/rocky-core/src/contracts.rs` |
| Schema drift detection | `engine/crates/rocky-core/src/drift.rs` |
| Masking strategies | `engine/crates/rocky-core/src/masking.rs` |
| Permission / role graph | `engine/crates/rocky-core/src/role_graph.rs` |
| SCD-2 snapshot logic | `engine/crates/rocky-core/src/snapshots.rs` |
| Quality checks | `engine/crates/rocky-core/src/checks.rs` |
| Hooks / webhooks | `engine/crates/rocky-core/src/hooks/` |
| Column lineage extraction | `engine/crates/rocky-sql/src/lineage.rs` |
| Skip-unchanged gate / hashing | `engine/crates/rocky-sql/src/determinism.rs` + `rocky-ir/src/ir.rs::skip_hash()` |
| CLI JSON output shape | `engine/crates/rocky-cli/src/output.rs` — then run `just codegen` |
| A new CLI subcommand | See the `rocky-new-cli-command` skill in `.agents/skills/` |
| The Rocky DSL (`.rocky` files) | `engine/crates/rocky-lang/src/` — then update VS Code grammar too |
| The browser UI (`rocky serve --ui`) | `engine/ui/` (React + Vite). Run `just ui-dev` for the dev loop. |
| The LSP server | `engine/crates/rocky-server/src/lsp.rs` |
| The Dagster integration | `integrations/dagster/src/dagster_rocky/` |
| The Python SDK | `sdk/python/src/rocky_sdk/client.py` |
| The VS Code extension | `editors/vscode/src/` |

The IR named above is the [intermediate representation](/reference/glossary/#ir-intermediate-representation): the typed blueprint Rocky builds from your SQL before it emits any warehouse dialect.

### Key invariants before you commit

- **Never** commit a Rust `*Output` change without running `just codegen` — `codegen-drift.yml` CI will fail.
- **Never** `format!()` untrusted input into SQL — use `rocky-sql/validation.rs` to validate identifiers first.
- **Never** hand-edit files under `*/types_generated/` or `*/types/generated/` — those are codegen outputs.
- **Never** skip hooks (`--no-verify`). Fix the underlying issue instead.

## Changes that cross subprojects

A schema or DSL change lands in several subprojects as one pull request, so CI checks the whole cascade together.

- **CLI JSON output schema:** edit the `*Output` struct, run `just codegen`, add new fields by hand to `sdk/python/src/rocky_sdk/types.py`, run `just regen-fixtures`. See the [JSON contract](/advanced/json-contract/).
- **Rocky DSL syntax:** `engine/crates/rocky-lang/`, then `engine/crates/rocky-compiler/`, then `editors/vscode/syntaxes/rocky.tmLanguage.json` and `editors/vscode/snippets/rocky.json`, then `docs/src/content/docs/concepts/rocky-dsl.md` and `docs/rocky-lang-spec.md`.

For the crate-level breakdown of `engine/`, see [Architecture](/concepts/architecture/).
