# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Rocky MCP server registered for agent mode** — the extension now registers `rocky mcp` as a Model Context Protocol server (one per workspace folder that has a `rocky.toml`), so the editor's agent mode can drive Rocky through the engine's typed read-only tools (`compile`, `lineage`, `inspect_schema`, `sample_rows`, `breaking_change`, `catalog`, `history`, `metrics`, `optimize`, and more). The tool surface is defined once in the Rust server, so the editor's AI stays in lockstep with whatever the installed `rocky` binary exposes — no tool definitions are duplicated in the extension. Honours `rocky.server.path` for the binary and refreshes when workspace folders or `rocky.toml` files change. The `@rocky` chat participant keeps its four single-shot slash commands; this adds the full typed toolset to agent mode. Degrades to a no-op on editor builds that predate the MCP definition-provider API (VS Code 1.101+).

## [1.28.0] — 2026-05-25

A wave of new panels and views surfacing the engine's trust + AI features directly in the editor — eight new commands (50 → 58).

### Added

- **Plan review / apply gate UI** (`rocky.reviewPlan`) — review an AI-authored plan's breaking-change report in a panel, then approve + apply from the editor.
- **Inline row preview + live compiled-SQL panel** (`rocky.previewModel`, `rocky.showCompiledSql`) — preview a model's or CTE's output rows inline and open the live compiled SQL for the active model, with cursor-aware preview and result-grid polish.
- **Ad-hoc SQL selection preview** — preview a highlighted SQL selection, governance-safe (masks applied).
- **Governance compliance panel** (`rocky.compliance`) — per-model mask / grant verdicts.
- **Execution-plan panel** (`rocky.dag`) — renders `rocky dag`.
- **Branches view** (`rocky.refreshBranches`) — sidebar listing virtual branches.
- **Run-trace timeline panel** (`rocky.trace`) — timeline of the latest run.
- **Lineage-diff panel** (`rocky.lineageDiff`) — per-changed-column downstream blast radius vs a base ref.

### Changed

- **`src/types/generated/{ai_contract,preview_rows,review}.ts`** regenerated for the engine's new `rocky ai-contract`, `rocky preview rows`, and `rocky review` output surface.
- Documentation counts and the VS Code API-minimum string refreshed to match the current extension surface.

## [1.27.0] — 2026-05-24

Codegen-companion release to engine `v1.43.0`. The regenerated TypeScript bindings under `src/types/generated/` and the `rocky-project` JSON schema surfaced via `jsonValidation` pick up the engine's new config and output surface. No new commands, settings, or LSP features.

### Changed

- **`src/types/generated/{compile,dag,rocky_project}.ts`** + **`schemas/rocky-project.schema.json`** pick up the `[freshness]` config block (project default + per-model `expected_lag_seconds`) and the W005 freshness-coverage diagnostic now present in compile/dag output — visible on hover when editing `rocky.toml` and model sidecars.
- **`src/types/generated/adapter_config.ts`** picks up the new `[adapter.<name>.extra]` escape-hatch field.
- **`src/types/generated/run.ts`** picks up the warehouse-side `cooldown_seconds` field on `TableErrorOutput`.

## [1.26.0] — 2026-05-21

Codegen-companion release to engine `v1.42.0`. The regenerated TypeScript bindings under `src/types/generated/` pick up the engine's new optional `FailedSourceOutput.cooldown_seconds` field from PR [#624](https://github.com/rocky-data/rocky/pull/624) (Fivetran 429 self-healing). The `rocky-project` JSON schema surfaced via `jsonValidation` also picks up the improved `max_retries` doc-comment recommending `≤ 4` when the Fivetran shared circuit breaker is enabled — visible on hover in `rocky.toml` editing. No new commands, settings, or LSP features.

### Changed

- **`src/types/generated/discover.ts`** picks up the new optional `cooldown_seconds: number | null` field on `FailedSourceOutput`.
- **`src/types/generated/{adapter_config,rocky_project}.ts`** + **`schemas/rocky-project.schema.json`** pick up the refreshed `max_retries` doc-comment text.

## [1.25.0] — 2026-05-21

Codegen-companion release to engine `v1.41.0`. The regenerated TypeScript bindings under `src/types/generated/` pick up the engine's new `SweepReport.traces_deleted` field from PR [#616](https://github.com/rocky-data/rocky/pull/616) (Arc 4 span retention). The engine also shipped two new LSP AI code-action arms (W004 unresolved classification + new E028 DSL parse errors) in PR [#620](https://github.com/rocky-data/rocky/pull/620); no extension-side dispatch change is required because `vscode-languageclient` already passes `CodeAction.data` through verbatim — the new arms work end-to-end once a 1.41 engine is on `$PATH` against this extension.

### Changed

- **Codegen pickup of `SweepReport.traces_deleted`** (engine `v1.41.0` — [#616](https://github.com/rocky-data/rocky/pull/616)). Regenerated `src/types/generated/state_retention_sweep.ts` adds the new `traces_deleted: number` field on `SweepReport`. Surfaces alongside the existing `runs_deleted` / `lineage_deleted` / `audit_deleted` counters; extension code consuming the state-retention sweep result via `rockyJson.ts` now type-checks against the new field without further hand-edits.

## [1.24.0] — 2026-05-20

Codegen-companion release to engine `v1.40.0`. The regenerated TypeScript bindings under `src/types/generated/` pick up the engine's per-model cost-ceiling surface from PR [#606](https://github.com/rocky-data/rocky/pull/606). No new extension commands, UI, or settings — pure codegen cascade.

### Changed

- **Codegen pickup of `PlanOutput.budget_diagnostics` + `has_budget_errors`** (engine `v1.40.0` — [#606](https://github.com/rocky-data/rocky/pull/606)). Regenerated `src/types/generated/plan.ts` adds optional `budget_diagnostics?: DiagnosticPayload[] | null` and `has_budget_errors?: boolean | null` fields on `PlanOutput`. The fields surface per-model `[budget]` ceiling breaches that `rocky plan` now emits when projected scan size (from real Iceberg snapshot summaries or Databricks `DESCRIBE DETAIL`) exceeds the declared ceiling. Extension code consuming `rocky plan --output json` via `rockyJson.ts` now type-checks against the new fields without further hand-edits.

## [1.23.0] — 2026-05-19

Codegen-companion release to engine `v1.39.0`. The regenerated TypeScript bindings under `src/types/generated/` pick up the engine's `rocky import-dbt` unit-test bridge ([#594](https://github.com/rocky-data/rocky/pull/594)). No new extension commands, UI, or settings — pure codegen cascade.

### Changed

- **Codegen pickup of unit-test counters on `ImportDbtOutput`** (engine `v1.39.0` — [#594](https://github.com/rocky-data/rocky/pull/594)). Regenerated `src/types/generated/import_dbt.ts` adds optional `unit_tests_found?: number | null` / `unit_tests_converted?: number | null` / `unit_tests_skipped?: number | null` on `ImportDbtOutput`. The `ImportWarningCategory` union gains two new variants (`orphan_unit_test`, `unsupported_unit_test_format`). Extension code that consumes `rocky import-dbt --output json` via `rockyJson.ts` now type-checks against the new fields without further hand-edits.

## [1.22.0] — 2026-05-19

Codegen-companion release to engine `v1.38.0`. The regenerated TypeScript bindings under `src/types/generated/` and the schema mirrors under `editors/vscode/schemas/` pick up six engine-side shape changes from this triple-cut. No new extension commands, UI, or settings — pure codegen cascade.

### Changed

- **Codegen pickup of `FivetranStateEnvelope`** (engine `v1.38.0` FR-C — [#583](https://github.com/rocky-data/rocky/pull/583)). New `src/types/generated/rocky_fivetran_state.ts` + schema mirror at `editors/vscode/schemas/rocky-fivetran-state.schema.json`. The canonical envelope shape Rocky writes via `rocky discover --emit-fivetran-state-to <PATH>` is now type-checkable from extension TypeScript via the `rockyJson.ts` compat shim.
- **Codegen pickup of `FivetranCacheConfig`** (engine `v1.38.0` FR-A — [#584](https://github.com/rocky-data/rocky/pull/584)). Regenerated `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json`: Fivetran adapter blocks now accept an optional `cache?: FivetranCacheConfig | null` field with the 5-backend enum. Extension `rocky.toml` validation now permits the new block.
- **Codegen pickup of `StrategyConfig::View` / `MaterializedView` / `DynamicTable`** (engine `v1.38.0` Wave 1 — [#585](https://github.com/rocky-data/rocky/pull/585)). Regenerated `rocky_project.ts` adds three new variants on the model `[strategy]` union, plus the `DynamicTable.target_lag` field. Snippet metadata at `editors/vscode/snippets/rocky.json` is untouched (snippets stay generic — no per-strategy snippet expansion); users authoring sidecar TOMLs declaring `type = "materialized_view"` / `"dynamic_table"` no longer trip schema-validation errors.
- **Codegen pickup of `FivetranRatelimitConfig` / `FivetranStampedeConfig` / `FivetranCircuitBreakerConfig`** (engine `v1.38.0` resilience layers — [#589](https://github.com/rocky-data/rocky/pull/589)). Three new optional config blocks on Fivetran adapter variants in `rocky_project.ts` + schema mirror. Plus typed `AdapterConfig.retry?: RetryConfig | null` field.
- **Codegen pickup of `ImportDbtStructuredWarning`** (engine `v1.38.0` Wave 2 — [#590](https://github.com/rocky-data/rocky/pull/590)). New `src/types/generated/import_dbt.ts` carries the `ImportDbtStructuredWarning` discriminated-union (6 variants) + `ImportDbtHookKind` enum. Extension code that consumes `rocky import-dbt --output json` via `rockyJson.ts` now type-checks against the structured warning surface.

## [1.21.0] — 2026-05-19

Two batches in this cut. **VS Code engines triangle bumped to 1.120** ([#581](https://github.com/rocky-data/rocky/pull/581)). `engines.vscode`, `@types/vscode`, and the `runTest.ts` VS Code pin all move to `1.120` in lockstep, closing the drift that left the types one step ahead of the declared minimum compatibility. `@vscode/test-electron` stays at `^2.5.2` — already at the latest published version. **Codegen pickup of engine `v1.37.0`**: the regenerated TypeScript bindings under `src/types/generated/` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json` pick up three engine-side shape changes — `PlanStoreConfig` / `plan_store` field removed from `rocky_project.ts` (engine Cluster 3 C — C-7); new `[[table_overrides]]` array on `ReplicationPipelineConfig`; `target_schema` + `source_id` on lineage nodes and `data_type` on `LineageColumnDef` in `lineage.ts`. No new extension commands, UI, or settings — pure codegen cascade + engines bump.

### Changed

- **Engines triangle bumped to vscode 1.120** ([#581](https://github.com/rocky-data/rocky/pull/581)). `engines.vscode` in `package.json` moves from `^1.116.0` to `^1.120.0`; `devDependencies."@types/vscode"` moves from `1.116.0` to `1.120.0`; the VS Code pin in `src/test/runTest.ts` (the `version` constant passed to `runTests({ version: "..." })`) moves from `"1.116.0"` to `"1.120.0"`; `@vscode/test-electron` stays at `^2.5.2`. `editors/vscode/CLAUDE.md`'s "VS Code API minimum" doc note is refreshed to `1.120.0`. The `package-lock.json` is regenerated in lockstep so the build is reproducible against the bumped versions. No LSP-client compatibility break — `vscode-languageclient ^9.0.1` already supports the 1.120 range.
- **Codegen pickup of engine `v1.37.0` lineage enrichment.** Regenerated `src/types/generated/lineage.ts` (and the schema mirror): `LineageNode` gains optional `target_schema?: string | null` and `source_id?: string | null`; `LineageColumnDef` gains optional `data_type?: string | null`. Extension code that consumes `rocky lineage --output json` via the generated TypeScript types — including the `rockyJson.ts` compat shim — type-checks against the new optional fields without further hand-edits.
- **Codegen pickup of engine `v1.37.0` replication `[[table_overrides]]`.** Regenerated `src/types/generated/rocky_project.ts` (and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json`): `ReplicationPipelineConfig` gains an optional `table_overrides?: TableOverride[] | null` field; new `TableOverride` interface exposes the `match` block (table / schema / connector glob patterns) plus the per-field overrides for `strategy`, `merge_keys`, `merge_keys_fallback`, `timestamp_column`, `target_table`, `target_schema`. The `rocky.toml` JSON Schema validation now permits the new array.
- **Codegen pickup of engine `[plan_store]` removal (Cluster 3 C — C-7).** The regenerated `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json` drop the `PlanStoreConfig` / `PlanStoreFormat` TypeScript types and the `plan_store` field on the root `RockyConfig`. Extension code that consumes `rocky.toml` via the generated TypeScript types no longer offers `[plan_store]` completions (the engine now rejects the block at parse time). Pure codegen cascade; no behavioural change in the extension itself.

## [1.20.0] — 2026-05-18

Codegen-companion release to engine `v1.36.0`. The regenerated TypeScript interface in `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json` pick up the new `merge_keys` and `merge_keys_fallback` fields on `ReplicationPipelineConfig` from engine v1.36.0's replication `strategy = "merge"` (engine [#561](https://github.com/rocky-data/rocky/pull/561)). No new extension commands, UI, or settings — pure codegen cascade.

### Changed

- **Codegen pickup of engine `v1.36.0` replication `strategy = "merge"`** (engine [#561](https://github.com/rocky-data/rocky/pull/561)). Regenerated `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json` add `merge_keys?: string[] | null` and `merge_keys_fallback?: string[] | null` as optional fields on `ReplicationPipelineConfig`, plus a description tweak on the `strategy` literal. Extension code that consumes `rocky.toml` via the generated TypeScript types — including the `rockyJson.ts` compat shim — now type-checks against the merge-strategy config surface. No behavioural change in the extension itself.

## [1.19.1] — 2026-05-18

Codegen-companion patch release for engine `v1.35.0` and dagster `v1.33.0`. The regenerated TypeScript interface in `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json` pick up the new `[plan_store] format` default (`"v1"` → `"v2"`) plus a doc-comment refresh on `PlanStoreFormat` / `PlanStoreConfig` reflecting the post-flip migration cycle. No new extension commands, UI, or settings — the new engine `PlanKind::Replication` variant doesn't change the TypeScript surface (`plan_kind` is a plain string).

### Changed

- **Codegen pickup of engine `v1.35.0` plan-store v2 default flip.** Regenerated `src/types/generated/rocky_project.ts` and the schema mirror at `editors/vscode/schemas/rocky-project.schema.json`: the default value of `PlanStoreConfig.format` flips from `"v1"` to `"v2"`, and the surrounding doc-comments are refreshed to describe the post-flip migration window (v1 is now opt-in only; the v1 reader stays through at least v1.35 + v1.36 before C-7 drops it). Extension code that consumes `rocky.toml` via the generated TypeScript types now matches the engine's persisted-plan default. No behavioural change in the extension itself.

## [1.19.0] — 2026-05-18

Two small additive changes. The TypeScript bindings under `src/types/generated/` pick up the new `failure_kind` discriminator on `RunOutput.errors[*]` (engine `v1.34.0`); the `@rocky` chat-participant system prompt is reframed to "typed-program layer above the warehouse" so the chat participant's self-introduction matches the public README and docs site copy.

### Added

- **`RunOutput.errors[*].failure_kind`** typed discriminator (engine `v1.34.0`). TypeScript interface regenerated in `src/types/generated/run.ts`; the field is a union with values `"connection-failed" | "auth-failed" | "query-rejected" | "transient" | "quota-exceeded" | "not-found" | "unknown"`. Extension code consuming `rocky run --output json` via `rockyJson.ts` now type-checks against the discriminator without further hand-edits.

### Changed

- **`@rocky` chat-participant framing**. System prompt in `src/chatParticipant.ts` updated from "Rust-based control plane for warehouse SQL pipelines" to "typed-program layer above the warehouse" so the chat participant's self-introduction matches the README, docs site, and `CLAUDE.md` copy across the monorepo. No behavioural change for slash commands; only the natural-language fallback prompt is reframed.

## [1.18.1] — 2026-05-17

Codegen-companion patch release for engine `v1.33.0`. The TypeScript bindings under `src/types/generated/` are regenerated to pick up the new `NamedStatement.sql` optionality (`compact.ts` / `archive.ts` — today's emission path still writes `sql`, so consumer code sees no behaviour change) and the new top-level `[plan_store] format = "v1" | "v2"` field on `rocky_project.ts`. No new extension commands, UI, or settings — types are available for future extension wiring.

### Changed

- **Codegen pickup of the engine Cluster 3 C IR foundation (engine v1.33.0).** Regenerated TypeScript interfaces in `src/types/generated/`: `NamedStatement.sql` is now optional on `compact.ts` / `archive.ts` (today's engine always writes `sql`, so existing consumer code is unaffected); `rocky_project.ts` picks up the new top-level `plan_store: { format: "v1" | "v2" }` config field with default `"v1"`. No new commands or sidebar entries — types are available for future PRs.

## [1.18.0] — 2026-05-15

Lineage rail redesign and SVG export, plus four lineage rendering bug fixes from [#524](https://github.com/rocky-data/rocky/pull/524). The collapsible right-docked rail replaces the old toolbar + side panel layout, and the SVG exporter now produces standalone-readable files with VS Code colours resolved to concrete values. Also picks up the TypeScript codegen for the engine's Cluster 3 B plan/apply spine — `compact_apply`, `archive_apply`, `plan`, `apply`, and `promote_plan` typed surfaces are available in `src/types/generated/` for future extension wiring; no new commands are registered in this release.

### Added

- **Collapsible right-docked rail for the lineage webview.** Replaces the previous horizontal toolbar + 320px right-side Node Details panel with a single 240px vertical rail docked on the right. Three top-level collapsible sections — **Controls** (View / Focus / Cluster / Layout / Search / Zoom subgrouped inline), **Node Details**, **Export** — each with a clickable header and chevron. A `‹/›` button at the top of the rail collapses it to a 28px strip. Per-section collapse state and rail-collapsed state both persist across reloads via `vscode.setState`. The graph auto-fits after the rail-width animation finishes so the viewport recenters.
- **Standalone-readable lineage SVG export.** The exporter clones the live SVG, sets `xmlns` (so OS image viewers will render it), resets the d3-zoom transform on the cloned graph group (export shows the full graph regardless of current pan/zoom), paints the editor background underneath, and inlines a stylesheet with VS Code CSS variables resolved to concrete colours (foreground, badge, button, charts-blue/green, panel-border, etc). Previously the exported file referenced unresolved `var(--vscode-*)` colours and external CSS rules, so opening it outside the webview produced an unreadable SVG.

### Fixed

- **Lineage panel stuck on "Rendering…".** The search-box regex `/^\/(.*)\/([gimsuy]*)$/` lives inside the inline-script TS template literal. The `\/` escape was stripped by the template evaluation, producing the invalid runtime regex `/^/(.*)/(...$/` and a `SyntaxError: Unexpected token '.'` that killed the main IIFE before it could draw the graph. Doubled the backslash (`\\/(.*)\\/`) so the runtime sees the intended `\/`.
- **Lineage panel uses the wrong `rocky.toml`.** Previously hard-coded `${workspaceFolder}/rocky.toml` and crashed with `no models found in models` if the workspace root wasn't the project root (e.g. a multi-folder workspace, or a SQL file opened from a sibling folder). Now walks up from the active file's directory to find the nearest `rocky.toml` and runs the CLI from that directory.
- **`Run Health Check` ("No workspace folder open") in the Get Started view.** `rocky.doctor` was gated by `ensureWorkspace()`, but the Get Started welcome links there precisely so the user can verify their CLI installation **before** there's a project. Removed the gate — doctor runs fine without a workspace and reports `critical` for the missing `rocky.toml`, which is the expected output for that flow.
- **Webview-script errors are now surfaced.** A pair of `window.addEventListener('error', …)` / `unhandledrejection` listeners live in their own `<script>` tag so a parse error in the main inline script can't prevent them from registering. Any uncaught exception now writes `Lineage error: <message> (<file>:<line>:<col>)` into the status bar instead of leaving the panel stuck on `Rendering…`.

### Changed

- **Codegen pickup of the engine plan/apply spine (engine v1.32.0, upcoming).** Regenerated TypeScript interfaces in `src/types/generated/` for the Cluster 3 B plan/apply surface: `compact_apply.ts` (`CompactApplyOutput`), `archive_apply.ts` (`ArchiveApplyOutput`), `apply.ts` (`ApplyOutput` envelope with `plan_id` + `plan_kind` + `inner`), `plan.ts` (additive `plan_id` / `plan_kind` / `created_at` / `models` / `execution_layers` fields on `PlanOutput`), and `plan_promote.ts` (`PromotePlan` / `PromoteTargetPlan`). No new extension commands or UI wiring — the types are available for future PRs that surface `rocky plan`, `rocky apply`, and `rocky plan promote` in the sidebar or command palette.

## [1.17.0] — 2026-05-14

Builds on `1.16.0`'s lineage Tier 2/3 work. Two batches: PR [#519](https://github.com/rocky-data/rocky/pull/519) finishes Lineage Tier 2 + adds the inline cost CodeLens and the Schema sidebar; PR [#520](https://github.com/rocky-data/rocky/pull/520) adds the `@rocky` chat participant, branded file icons, and the Previews sidebar. Eight new surfaces total — all vscode-only, no engine cascades.

### Added

- **`@rocky` chat participant.** Wires the existing `aiGenerate` / `aiExplain` / `aiSync` / `aiTest` commands into VS Code's chat panel via `vscode.chat.createChatParticipant`. Slash commands `/generate <description>`, `/explain`, `/sync`, `/test [model]` route to extracted pure helpers (`runAiGenerate` / `runAiExplain` / `runAiSync` / `runAiTest`) so the chat path never opens an interactive input box. Natural-language fallback uses `vscode.lm.selectChatModels({ vendor: "copilot" })` when available; falls back to a help message listing the four slash commands when no language model is registered. A followup provider suggests next-turn prompts (e.g. after `/generate`, suggests `/explain` and `/test` for the new model). Streams responses via `stream.markdown`; `/generate` opens the result in the editor with `preserveFocus`. Requires `engines.vscode ≥ 1.116.0` (already pinned).
- **Lineage view-mode toggle (Model ↔ Column).** Toolbar control on the lineage webview switches between **Model** view (default — aggregates column-level edges into model-to-model pairs) and **Column** view. View mode persists across reloads via the `WebviewPanelSerializer`.
- **Lineage layout toggle (Horizontal ↔ Vertical).** Toolbar `<select>` swaps dagre's `rankdir` between `LR` and `TB` and re-runs layout + fit. Cluster labels still anchor at the bbox top-left in both directions. Persisted in `SerializedState.layout`.
- **Lineage search / filter box.** Debounced text input (250ms) on the toolbar. Plain substring match by default; `/regex/flags` syntax for power users (validates; on parse error, falls back to substring and flags the input red). Matched nodes highlighted; non-matched dimmed via the existing dim-mode CSS. Edges dim only when both endpoints are non-matched. Persisted in `SerializedState.searchQuery`. Keyboard shortcuts (`+` / `-` / `0` / `F`) suppressed while typing.
- **Lineage focus mode (upstream / downstream / selected).** New `Focus: [All | Upstream | Downstream | Selected only]` toolbar control. Click-a-node sets focus; BFS reachability against pre-built adjacency maps determines which nodes stay highlighted. Orthogonal to cluster focus and search dim — separate CSS classes that can stack. Background click clears both focus types. Persisted (`focusMode` + `focusNode`) with graph-membership validation on restore.
- **Lineage node details side panel.** Click any node to open a 320px right-side panel showing model name, last-run status / when / duration / row count (from `rocky history --model NAME --output json`), and the focal model's columns. New "Open in Editor" button replaces the previous direct click-to-open behavior; click on a node now opens the panel.
- **Lineage subgraph drill-in.** Toolbar `[None | Schema | Source]` cluster mode draws labelled bounding boxes around grouped nodes. Schema mode parses from the qualified model name (`catalog.schema.model` → `catalog.schema`); Source mode groups all nodes with no upstream edges under a single "Sources" cluster. Click a cluster header to focus + dim non-members; click background to clear. Cluster mode persists in `SerializedState`.
- **Inline cost annotations** via CodeLens. Above every model file in `**/models/**/*.{rocky,sql}`, shows `~$X/run · $Y/mo storage · save $Z/mo` from `rocky optimize --output json` (`OptimizeRecommendation` already exposes `compute_cost_per_run` / `storage_cost_per_month` / `estimated_monthly_savings`). 5-minute cache; new commands `rocky.refreshCosts` and `rocky.showCostDetail`. Setting `rocky.costAnnotations.enabled` (default `true`) toggles them off entirely. Silent no-op if `rocky optimize` fails (no warehouse, no auth) — error logged to the output channel; lenses just don't appear.
- **Schema sidebar view.** New `Schema` tree in the Rocky activity bar sourced from `rocky catalog --output json`. Two-level: model → columns (label = name; description = `<data_type><?` for nullable; tooltip = full data_type). Click a column → opens the model file and applies a subtle word-boundary highlight to matching column references. Right-click → "Copy column reference" writes `<model>.<col>` to the clipboard. View-title refresh button. Empty state via `viewsWelcome` gated on `rocky.hasProject`.
- **Previews sidebar view.** New `Previews` tree in the Rocky activity bar showing active preview branches (those the user creates via `rocky.previewCreate` in the workspace; tracked in `workspaceState`). Each branch expands to three sub-categories: **Cost** (per-model delta from `rocky preview cost`), **Diff** (per-model structural diff from `rocky preview diff`; click → opens an in-memory markdown document), **Models Changed** (status icons added/modified/removed). New commands: `rocky.refreshPreviews` (view-title button), `rocky.removePreview` (right-click on a branch node; UI-only — does not delete the engine-side branch).
- **Branded file icons.** Opt-in `Rocky File Icons` theme (`contributes.iconThemes`) brands both `.rocky` extension files and `rocky.toml` (the latter being TOML, not in the `rocky` language scope, can only be reached via an iconThemes contribution). Asset reuses the existing `icons/rocky-icon.svg`. The existing `contributes.languages[].icon` already brands `.rocky` files for all users regardless of theme; this adds the only path that reaches `rocky.toml`.

### Notes

- Tier 3 punts from `1.16.0` (column types in lineage, upstream/downstream column lists, engine-provided cluster keys, nested clusters via elk.js) remain deferred — they require engine cascades that aren't justified by current usage.
- Chat participant punts: token-streaming from the Rocky CLI (subprocess resolves on exit only); structured tool-calling/dispatch loops; selecting a specific Copilot model when multiple are returned (always picks `[0]`).
- Previews view limitations: no engine `preview list` command exists, so the view only tracks branches the user explicitly created in this workspace; row-only changes show as `unchanged` in the Models Changed sub-tree.

## [1.16.0] — 2026-05-14

Audit follow-up release ([#517](https://github.com/rocky-data/rocky/pull/517)) — closes the 2026-05-13 wide-sweep punch list. Three real bug fixes, nine UX polish items, and the Lineage panel's renderer + Tier 3 features (view-mode toggle, node details side panel, subgraph drill-in).

### Added

- **Lineage view-mode toggle (Model ↔ Column).** New toolbar control on the lineage webview switches between **Model** view (default — aggregates column-level edges into model-to-model pairs) and **Column** view (column-level qualified nodes). View mode persists across reloads via the `WebviewPanelSerializer`.
- **Lineage node details side panel.** Click any node to open a 320px right-side panel showing model name, last-run status / when / duration / row count (from `rocky history --model NAME --output json`), and the focal model's columns. New "Open in Editor" button replaces the previous click-to-open behavior; the click now opens the panel.
- **Lineage subgraph drill-in.** New toolbar `[None | Schema | Source]` cluster mode draws labelled bounding boxes around grouped nodes. Schema clusters parse from the qualified model name (`catalog.schema.model` → `catalog.schema`); Source mode groups all nodes with no upstream edges under a single "Sources" cluster. Click a cluster header to focus + dim non-members; click background to clear. Cluster mode persists in `SerializedState`.
- **Status bar enrichment** (opt-in via `rocky.statusBar.segments`). New array setting accepts any of `warehouse`, `lastRunAge`, `driftCount`, `branchState` to append segments alongside the existing LSP state + error count. Segment values cache for 30s; `driftCount` derives live from diagnostics. Setting changes invalidate the cache immediately.
- **Drift diagnostic quick fixes.** New `DriftCodeActionProvider` registered via `vscode.languages.registerCodeActionsProvider` adds light-bulb actions on drift diagnostics: "Run compile to refresh" (runs `rocky.compile`) and "Accept schema change" (placeholder until upstream support lands).
- **Doctor webview command-URI links.** Failed checks (critical / warning) and suggestion text now render as clickable `command:` URIs that open settings or jump to the relevant configuration. `enableCommandUris: true` is set on the doctor webview panel.
- **Sources view grouping.** The Sources sidebar now groups connectors by `source_type` under collapsible parent nodes that surface connector count, total table count, and most-recent `last_sync_at`. Single-member groups collapse directly to tables.
- **Runs view partial-failure tooltips.** Tooltips on partial-failure runs now list the failed model names and their statuses; full-failure and success tooltips unchanged.
- **Get Started CTAs sequencing.** The quick-actions block on initialized projects now shows a one-line description per CTA and a "New? Try Compile first." hint above the block.
- **Onboarding walkthrough** (`contributes.walkthroughs`). 5-step flow for first-time users: Install CLI → Initialize Project → Compile Models → View Lineage → Run Pipeline. Each step links to an existing command.
- **Command-palette keybinding hint.** The first Pipeline item in the Rocky command palette now surfaces the platform-appropriate chord (`⌘⇧R` on macOS, `Ctrl+Shift+R` on Windows/Linux) so users discover the keybinding.
- **Four new settings**: `rocky.defaultWarehouse` (string), `rocky.defaultBranch` (string), `rocky.diagnostics.enabled` (boolean, default true; wired into drift diagnostics — no-ops + clears existing diagnostics when toggled off), `rocky.autoRunOnSave` (boolean, default false; read-only contract for future use).

### Changed

- **Lineage renderer swapped from viz.js to `@dagrejs/dagre` v3 + `d3` v7.** The viz.js path used Graphviz/DOT and required `wasm-unsafe-eval` in the webview CSP. The new renderer reads `LineageOutput` JSON directly, lays out with dagre, and renders SVG via d3 with d3-zoom for pan/zoom. The CSP now uses strict `script-src ${webview.cspSource} 'nonce-${nonce}'` (no `unsafe-eval`). Bundled into `media/lineage-graph.js` via esbuild. The library choice avoids `dagre-d3`'s 9 high-severity CVEs in its bundled outdated d3 v6. Existing interactivity from `1.15.x` (click-to-open, hover tooltip, Fit + keyboard shortcuts `+` `-` `0` `F`, source/leaf node coloring, `WebviewPanelSerializer` state persistence) is preserved on the new renderer.

### Fixed

- **`rockyCli.ts` abort-listener leak.** When `runRocky` was passed an `AbortSignal` and `execFile` threw synchronously, the `abort` listener attached on the signal was never removed and accumulated across reuses. The listener is now removed on both success and error completion paths.
- **`getStartedView` `EventEmitter` disposal.** The module-level `projectChangeEmitter` was never explicitly disposed; rapid extension-host reloads could race a disposed extension. Now pushed into `context.subscriptions` so it's disposed on extension deactivation.
- **`testExplorer` raw JSON in error messages.** When `runRockyJson` failed on `JSON.parse`, the throw carried the unparseable stdout as `stderr`, so the test explorer surfaced raw JSON as the failure detail. `RockyCliError` now distinguishes `kind: "exit"` from `kind: "parse"`; parse errors carry an empty `stderr` and a clean message ("Rocky CLI returned malformed JSON: …").

## [1.15.3] — 2026-05-13

Companion release to engine `v1.31.0`. Patch cycle: the regenerated TypeScript interfaces in `src/types/generated/ci_diff.ts` and `src/types/generated/branch_promote.ts` pick up the typed `BreakingChange` / `BreakingFinding` / `BreakingSeverity` surface (engine [#508](https://github.com/rocky-data/rocky/pull/508) / [#509](https://github.com/rocky-data/rocky/pull/509) / [#510](https://github.com/rocky-data/rocky/pull/510)), so any extension code that consumes `rocky ci-diff --semantic` or `rocky branch promote` JSON output now type-checks against the full discriminated union. No extension feature changes — no new commands, no UI changes, no setting changes.

## [1.15.2] — 2026-05-12

Companion release to engine `v1.30.0`. Patch cycle: the regenerated TypeScript interfaces in `src/types/generated/` pick up the new `content_addressed` value on the `MaterializationStrategy` union (engine [#496](https://github.com/rocky-data/rocky/pull/496) + [#497](https://github.com/rocky-data/rocky/pull/497)), so any extension code that destructures a model sidecar via `rockyJson.ts` now type-checks against the full strategy surface. No extension feature changes — no new commands, no UI changes, no setting changes. Dev-dependency refresh: vitest `4.1.5 → 4.1.6` ([#462](https://github.com/rocky-data/rocky/pull/462)), typescript-eslint bump ([#461](https://github.com/rocky-data/rocky/pull/461)), `@types/node` bump ([#460](https://github.com/rocky-data/rocky/pull/460)).

## [1.15.1] — 2026-05-07

Patch release fixing the new "Run Health Check" leaf in the **Extension Info → Logs** sidebar section. Clicking it surfaced "Doctor failed: …" toasts and threw the doctor JSON away — `rocky doctor` exits 2 when any check is `critical` (see `engine/crates/rocky-cli/src/commands/doctor.rs`) and the vscode handler treated any non-zero exit as a hard failure. Same bug existed in 1.14.x but was harder to hit because doctor wasn't exposed in the sidebar.

### Fixed

- **`rocky.doctor` command recovers the JSON payload on exit 2.** `RockyCliError` now captures `stdout` alongside `stderr`, so callers that know certain commands signal status via exit code can re-read the payload from the thrown error. The doctor handler (`src/commands/ops.ts`) checks for `exitCode === 2`, parses `err.stdout` as `DoctorResult`, and renders the doctor webview just as it does on a healthy run — so users see exactly which checks failed instead of a generic "Doctor failed" notification. Falls through to the existing error path when the stdout payload is missing or unparseable (e.g., a true CLI panic).

## [1.15.0] — 2026-05-07

Activity bar UX overhaul. The Rocky sidebar now leads with a **Get Started** welcome panel and a structured **Extension Info** tree (extension version, Rocky CLI version, language-server status, configuration, detected `rocky.toml`, log shortcuts), and ends with a **Help** panel linking out to docs / issues / marketplace / releases / source. Existing Models, Runs, and Sources panels stay in place but switch their inline empty-state messages to `viewsWelcome` markdown gated by a new `rocky.hasProject` context — so a workspace with no `rocky.toml` shows orientation instead of CLI errors.

### Added

- **Activity bar — `Get Started` panel** (markdown via `viewsWelcome`). Three context variants: empty workspace prompts to open a folder; workspace without `rocky.toml` shows `Initialize Rocky Project` / `Try Playground` / `Open Documentation`; workspace with a Rocky project shows quick actions (`Compile`, `Plan`, `Run`, `Discover Sources`, `Run Health Check`).
- **Activity bar — `Extension Info` panel** (`src/views/extensionInfoView.ts`). Four sections: About (extension + CLI version, LSP status), Configuration (server path, inlay hints, extra args — each leaf opens its setting), Project (detected `rocky.toml` paths, workspace folder), Logs & Diagnostics (Show Output Channel / Run Doctor / Restart Language Server). LSP status updates live via a new lifecycle emitter; CLI version cached for 60s.
- **Activity bar — `Help` panel** (`src/views/helpView.ts`). Five fixed leaves: Documentation, Report a Bug, View on Marketplace, Releases & Changelog, Source Code. Each opens the URL via the built-in `vscode.open` command.
- **Empty-state `viewsWelcome` for Models / Runs / Sources** — gated by `rocky.hasProject` so the copy and CTAs match whether a workspace has a Rocky project. `rocky.runs` and `rocky.sources` no longer shell out to the CLI on workspaces with no `rocky.toml`.
- **New commands**: `rocky.openOutputChannel`, `rocky.openDocumentation`, `rocky.reportBug`, `rocky.viewMarketplace`, `rocky.refreshInfo`, `rocky.init`, `rocky.playground`. The last two open an integrated terminal at the workspace root and run `rocky init` / `rocky playground`.
- **`getCliVersion()` helper** (`src/rockyCli.ts`) — runs `rocky --version`, caches for 60s, swallows errors. Used by the Extension Info panel's About section.
- **`onDidChangeLspState` emitter + `getLspState()`** (`src/lspClient.ts`). New `LspStatus` (`Starting | Restarting | Ready | Stopped | Failed`) fired from the actual lifecycle transitions (start, restart, stop, startup-failure) — independent of the diagnostic-driven status bar updates.

### Changed

- **`rocky.models` file watcher** debounced to 500ms; `onDidChange` removed (saves no longer trigger a refresh) since the file list is unaffected by content changes.
- **Activity bar order**: `Get Started → Extension Info → Models → Runs → Sources → Help`. New informational panels start collapsed; Models stays expanded as the default landing target.

## [1.14.2] — 2026-05-05

Patch release fixing a broken brand image on the VS Code Marketplace listing. v1.14.1 shipped successfully but rendered a broken image because `vsce` rewrites relative image URLs in the extension README to `<repo>/raw/HEAD/<README-relative-path>` and ignores `package.json`'s `repository.directory` field — so `media/rocky-readme-light.png` resolved to `https://github.com/rocky-data/rocky/raw/HEAD/media/rocky-readme-light.png` (404) instead of the actual file at `https://github.com/rocky-data/rocky/raw/HEAD/editors/vscode/media/rocky-readme-light.png`. Replaced the relative paths in `<picture>` with absolute `raw.githubusercontent.com` URLs that bypass `vsce`'s rewriter entirely. No extension feature changes.

### Changed

- **`editors/vscode/README.md` brand `<picture>` element switched to absolute URLs** (`https://raw.githubusercontent.com/rocky-data/rocky/main/editors/vscode/media/rocky-readme-{light,dark}.png`). `vsce` only rewrites relative URLs, so absolute ones pass through unchanged and the marketplace fetches the correct path. Both VS Code's in-extension README viewer and the marketplace web listing render fine against the raw GitHub URL.

## [1.14.1] — 2026-05-05

Companion patch release recovering the failed v1.14.0 publish. `vsce package` rejects SVG references in extension READMEs (even remote URLs) under the VS Code Marketplace's security policy, which blocked the v1.14.0 release at the package step before it reached publish. Rasterized the two `rocky-readme-{light,dark}.svg` brand assets to 2560×640 PNG counterparts via `rsvg-convert` (path-flattened source from #410, so rendering is deterministic) and swapped the references in `editors/vscode/README.md`. No extension feature changes.

### Changed

- **`editors/vscode/README.md` brand `<picture>` element switched from SVG to PNG** to satisfy `vsce package`'s SVG-restriction policy. The new PNGs sit alongside the SVGs at `editors/vscode/media/` (`rocky-readme-{light,dark}.png`); the SVG sources are kept in place for parity with the docs site, where browsers render them natively without the marketplace constraint.

## [1.14.0] — 2026-05-05

Tracks engine `v1.26.0`. Regenerated TypeScript bindings for the new `AiGenerateOutput.body_path` / `.sidecar_path` fields from engine [#414](https://github.com/rocky-data/rocky/pull/414) — `rocky ai` now writes both the body and a sidecar to disk and reports the resulting paths in the typed JSON output. Engine [#413](https://github.com/rocky-data/rocky/pull/413) (AI prompt tightening) and engine [#415](https://github.com/rocky-data/rocky/pull/415) (env-var substitution in model sidecars) are source-invisible to the extension. No extension feature changes this cycle.

### Changed

- **Regenerated `src/types/generated/ai_generate.ts`** (engine [#414](https://github.com/rocky-data/rocky/pull/414)) for the new `body_path` and `sidecar_path` fields on `AiGenerateOutput`. Both are optional (`string | null`) and only populated when the engine successfully wrote the generated body + sidecar to disk; strictly additive, so existing consumers of `AiGenerateOutput` continue to compile. Barrel `index.ts` updated; `src/types/rockyJson.ts` shim picks up the new fields.

## [1.13.0] — 2026-05-04

Tracks engine `v1.25.0`. Surfaces the new branch approval lifecycle in the VS Code extension and regenerates TypeScript bindings for the `BranchApproveOutput` / `BranchPromoteOutput` families plus the small description-text changes propagated by engine [#386](https://github.com/rocky-data/rocky/pull/386).

### Added

- **`rocky.branchApprove` + `rocky.branchPromote` commands** (engine [#388](https://github.com/rocky-data/rocky/pull/388)). Two new commands registered in `package.json`'s `contributes.commands`, surfaced in the command palette under the Branch group, and implemented in `src/commands/branch.ts`. Wraps the engine's new file-based branch approval gate — `Approve Branch` invokes `rocky branch approve` to sign an approval artifact under `./.rocky/approvals/<branch>/` and surfaces the resulting `BranchApproveOutput` in a JSON editor. `Promote Branch` prompts for a branch name (and optional filter), shows a confirmation dialog before dispatching, then invokes `rocky branch promote` and reports success / failure based on `result.success` and the per-target dispatch outcomes from the typed JSON output. The commands consume the regenerated `BranchApproveOutput` / `BranchPromoteOutput` TypeScript interfaces so the typed surface matches the engine's JSON output exactly.
- **TS interfaces for `rocky branch approve` + `rocky branch promote`** (engine [#388](https://github.com/rocky-data/rocky/pull/388)). Regenerated `src/types/generated/branch_approve.ts` + `branch_promote.ts` from engine v1.25.0 schemas — full typed surface (`BranchApproveOutput`, `BranchPromoteOutput`, plus `ApprovalArtifact`, `RejectedApproval`, `PromoteTarget`, and `AuditEvent` types covering `branch_state_hash`, `approvals_used`, `approvals_rejected`, `targets`, `audit`, and `success`). Barrel `index.ts` updated; `src/types/rockyJson.ts` shim picks up the new types.

### Changed

- **Regenerated `src/types/generated/cost.ts` + `rocky_project.ts`** (engine [#386](https://github.com/rocky-data/rocky/pull/386)). Description-text diff propagated from the engine's doc-comment Arc-framing strip; no shape change.
- **Regenerated `schemas/rocky-project.schema.json`** (engine [#388](https://github.com/rocky-data/rocky/pull/388)). Picks up the new branch lifecycle additions, syncing the bundled JSON schema with the engine.

## [1.12.0] — 2026-05-02

Tracks engine `v1.23.0`. Surfaces the new `rocky catalog` command in the VS Code extension and regenerates TypeScript bindings for the `CatalogOutput` family, the additive `sweep_interval_seconds` / `sweep_budget_ms` fields on `StateRetentionConfig`, and the additive `projected_per_model_budget_breaches` field plus `PerModelBudgetBreachOutput` interface from per-model `[budget]` blocks.

### Added

- **`rocky catalog` command surface** (engine [#356](https://github.com/rocky-data/rocky/pull/356)). New `rocky.catalog` command ("Build Project Catalog (Column Lineage Snapshot)") registered in `package.json`'s `contributes.commands`, surfaced in the command palette as a "Catalog" entry, and implemented in `src/commands/inspect.ts`. Invokes `rocky catalog` against the active project and renders the resulting column-level lineage snapshot. The command picks up the regenerated `CatalogOutput` TypeScript interfaces so the typed surface matches the engine's JSON output exactly.
- **TS interfaces for `rocky catalog`** (engine [#356](https://github.com/rocky-data/rocky/pull/356)). Regenerated `src/types/generated/catalog.ts` from engine v1.23.0 schemas — full typed surface (`CatalogOutput`, `CatalogAsset`, `CatalogColumn`, `CatalogEdge`, `CatalogStats`, `AssetKind`, `EdgeConfidence`). Barrel `index.ts` updated; `src/types/rockyJson.ts` shim picks up the new types.

### Changed

- **Regenerated `src/types/generated/rocky_project.ts`** (engine [#355](https://github.com/rocky-data/rocky/pull/355)) for the two additive fields on `StateRetentionConfig` (`sweep_interval_seconds`, `sweep_budget_ms`). Both default-populated and additive, so parsing a pre-1.23 `rocky.toml` still round-trips cleanly.
- **Regenerated `src/types/generated/preview_cost.ts`** (engine [#361](https://github.com/rocky-data/rocky/pull/361)) for the additive `projected_per_model_budget_breaches` field plus the new `PerModelBudgetBreachOutput` interface. Strictly additive — existing consumers of `projected_budget_breaches` (which continues to surface only project-level breaches) are unaffected.

## [1.11.0] — 2026-04-30

Tracks engine `v1.19.0`. Regenerated TypeScript bindings for the new `rocky lineage-diff` command surface. No extension feature changes.

### Added

- **TS interface for `rocky lineage-diff`** ([#298](https://github.com/rocky-data/rocky/pull/298)). Regenerated `src/types/generated/lineage_diff.ts` from engine v1.19.0 schemas — full typed surface (`LineageDiffOutput`, `LineageDiffResult`, `LineageColumnChange`) for any future webview / command that wants to consume `rocky lineage-diff`. Barrel `index.ts` updated.

## [1.10.0] — 2026-04-29

Tracks engine `v1.18.0`. Regenerated TypeScript bindings for the new `rocky preview` command surface plus the additive `[budget].max_bytes_scanned` field on `RunOutput`. No extension feature changes.

### Added

- **TS interfaces for `rocky preview`** ([#279](https://github.com/rocky-data/rocky/pull/279), [#280](https://github.com/rocky-data/rocky/pull/280)). Regenerated `src/types/generated/preview_create.ts`, `preview_diff.ts`, `preview_cost.ts` from engine v1.18.0 schemas — full typed surface for any future webview / command that wants to consume `rocky preview`.

### Changed

- **Regenerated `src/types/generated/run.ts`** ([#288](https://github.com/rocky-data/rocky/pull/288)) for engine 1.18.0's `RunCostSummary.total_bytes_scanned` and the `MaxBytesScanned` `BudgetLimitType` variant. Both fields are optional / additive, so parsing a pre-1.18 `rocky run --output json` still round-trips cleanly.

## [1.9.1] — 2026-04-24

Patch release. Fixes the "Show Lineage" command producing an error webview complaining about character `{` (#260). The extension now passes `-o table` alongside `--format dot` so it works against the already-shipped `rocky 1.17.0` binary without waiting for the engine patch (engine 1.17.1 fixes the same bug at the CLI layer for terminal users).

## [1.9.0] — 2026-04-24

Tracks engine 1.17.0 (governance-waveplan polish wave). Regenerated TypeScript bindings for the expanded `PlanOutput` surface; no extension feature changes.

### Changed

- **Regenerated `src/types/generated/plan.ts`** for engine 1.17.0's `rocky plan` governance preview: `PlanOutput` gains an `env` field plus three additive collections — `classification_actions`, `mask_actions`, `retention_actions` — with matching `ClassificationAction` / `MaskAction` / `RetentionAction` interfaces. All fields are optional (`skip_serializing_if = "Vec::is_empty"` on the Rust side), so parsing a pre-1.17 `rocky plan --output json` still round-trips cleanly.

## [1.8.0] — 2026-04-23

Tracks engine 1.16.0 (governance waveplan). Regenerated TypeScript bindings + `rocky-project.schema.json` for classification / masking / role-graph / retention / compliance / audit config surfaces; no extension feature changes.

### Added

- **New TS interfaces** for engine 1.16.0 outputs: `ComplianceOutput` + subtypes (`ComplianceSummary`, `ColumnClassificationStatus`, `EnvMaskingStatus`, `ComplianceException`) from engine [#242](https://github.com/rocky-data/rocky/pull/242); `RetentionStatusOutput` + `ModelRetentionStatus` from engine [#244](https://github.com/rocky-data/rocky/pull/244).

### Changed

- **Regenerated `src/types/generated/`** for engine 1.16.0:
  - `RunOutput`/`HistoryOutput` gain the 8-field governance audit trail (`triggering_identity` / `session_source` / `git_commit` / `git_branch` / `idempotency_key` / `target_catalog` / `hostname` / `rocky_version`, engine [#240](https://github.com/rocky-data/rocky/pull/240)).
  - `MaskStrategy` union surfaced (`"hash" | "redact" | "partial" | "none"`).
- **`schemas/rocky-project.schema.json`** re-exported from the engine's autogenerated source — IDE validation now covers `[classification]` model sidecar blocks, `[mask]` + `[mask.<env>]` project blocks, `[classifications].allow_unmasked`, `[role.<name>]` inheritance blocks, and the `retention = "<N>[dy]"` model-sidecar field.

## [1.7.0] — 2026-04-23

Tracks engine 1.15.0. Regenerated TypeScript bindings + `rocky-project.schema.json` to surface FR-004's new types in editor validation and hover; no extension feature changes.

### Changed

- **Regenerated `src/types/generated/`** for engine 1.15.0: `RunOutput` gains `status` / `skipped_by_run_id` / `idempotency_key` fields; `RunStatus` union extended with `SkippedIdempotent` + `SkippedInFlight`. New `IdempotencyConfig` / `DedupPolicy` types in the project schema. TS types rebuilt via `just codegen-vscode`.
- **`schemas/rocky-project.schema.json`** re-exported from the engine's autogenerated source — IDE validation for the new `[state.idempotency]` TOML block + the three new `retention_days` / `dedup_on` / `in_flight_ttl_hours` fields.

## [1.6.4] — 2026-04-23

Tracks engine 1.14.0. Regenerated TypeScript bindings + `rocky-project.schema.json`; no extension feature changes.

### Changed

- **Regenerated `src/types/generated/`** for engine 1.14.0: new `ClearSchemaCacheOutput` (from `rocky state clear-schema-cache`, engine #232), new `schemas_cached` field on `DiscoverOutput` (from `rocky discover --with-schemas`, engine #231), new `metadata` field on `SourceOutput` (adapter-namespaced, engine #225), new `bytes_scanned` / `bytes_written` on `MaterializationOutput` (Arc 2 wave 3, engine #219/#221).
- **`schemas/rocky-project.schema.json`** re-exported from the engine's autogenerated source so IDE validation stays in lockstep with the 1.14.0 project schema surface.

## [1.6.3] — 2026-04-21

### Fixed

- **`rocky-project.schema.json` + `StateConfig` TS interface** — restore `transfer_timeout_seconds` validation field that was missing from `vscode-v1.6.2` (#210). Root cause: `just codegen-vscode` fails silently when `editors/vscode/node_modules` is absent, so the release build regenerated the engine schemas but left the vscode mirror stale. `vscode-v1.6.2` ships a working extension; this patch just reintroduces IDE validation for the one missing field.

## [1.6.2] — 2026-04-21

Tracks engine 1.12.0 (Arc 1 wave 2 + cleanup cascade). Regenerated TypeScript bindings + `rocky-project.schema.json`; no feature changes to the extension itself.

### Changed

- **Regenerated `src/types/generated/`** for the engine-side additions in this cycle: `CostOutput` + `PerModelCostHistorical` (new `rocky cost` command, #202), `OptimizeRecommendation` cost fields (`compute_cost_per_run` / `storage_cost_per_month` / `downstream_references`, #203), and `MaterializationOutput.started_at: string` (real per-model wall-clock timestamp, #206).
- **`schemas/rocky-project.schema.json`** re-exported from the engine's autogenerated source so IDE validation stays in lockstep with the new 1.12.0 project schema surface.

## [1.6.1] — 2026-04-20

### Fixed

Bumps `engines.vscode` to `^1.116.0` to match `@types/vscode` (dependabot PR #182 bumped the types without the engines field). The `vscode-v1.6.0` build failed in `vsce package` with `@types/vscode ^1.116.0 greater than engines.vscode ^1.105.0`; this patch release unblocks the Marketplace publish path. No feature changes vs. v1.6.0 — same regenerated bindings + project schema.

The `vscode-v1.6.0` GitHub Release exists but has no VSIX attached and Marketplace was not published; users should consume v1.6.1 as the first shippable release of the trust-system arc bundle.

## [1.6.0] — 2026-04-20

Tracks engine 1.11.0. Regenerated TypeScript bindings + `rocky-project.schema.json` for the trust-system arcs (Arcs 1–7 first waves + Arc 6 wave 2 + Arc 7 wave 2 wave-1).

### Added — Trust-system Arc 1 types

TypeScript interfaces for the four new engine schemas:

- `BranchOutput` / `BranchEntry`, `BranchListOutput`, `BranchDeleteOutput`
- `ReplayOutput` / `ReplayModelOutput`

`ColumnLineageOutput` gains a `direction` field (`"upstream"` / `"downstream"`) — additive, no consumer changes required.

### Added — Trust-system Arc 2 types

- `RunOutput.cost_summary`, per-materialization `cost_usd` on `MaterializationMetadata`.
- `BudgetConfig` (from `rocky.toml [budget]`) and `budget_breach` PipelineEvent shape typed.

### Added — Trust-system Arc 3 types

- `circuit_breaker_tripped` / `_recovered` PipelineEvent variants in the regenerated event union.

### Added — Trust-system Arc 4 types

- `TraceOutput` / `TraceLane` / `TraceMaterialization` for `rocky trace <run_id|latest>` JSON consumers.

### Added — Trust-system Arc 5 types

- `AiGenerateOutput.models` field for scoped AI prompt context.

### Added — Trust-system Arc 6 wave 2 types + project schema

- `PortabilityConfig` block added to `rocky-project.schema.json` (jsonValidation surface for `rocky.toml`). Lowercase `Dialect` enum (`databricks` / `snowflake` / `bigquery` / `duckdb`) round-trips through the `target_dialect` field.
- IDE autocomplete + validation now covers `[portability]` and the `allow = [...]` list.

### Added — Trust-system Arc 7 wave 2 wave-1

- No new TypeScript types; `--with-seed` is a CLI-only flag with no output schema impact.

### Note

Arc 6 wave 1 (P001) and Arc 7 wave 1 (P002) lint diagnostics flow through the existing `Diagnostic` interface — codes appear as plain strings in the `code` field.

## [1.5.0] — 2026-04-20

### Added — Prefer the standalone `rocky-lsp` binary when available

The LSP client now probes PATH (or the directory of an explicit `rocky.server.path`) for a `rocky-lsp` binary and uses it directly when present, falling back to `rocky lsp` otherwise. `rocky-lsp` is a purpose-built ~6 MB binary (vs. ~47 MB for the full `rocky` CLI) that skips the full adapter graph, so language-server spawn is faster and the on-disk footprint for IDE-only installs drops.

**Resolution rules:**

- If `rocky.server.path` is an absolute/relative path containing a separator (e.g. `/opt/rocky/bin/rocky`), the client looks for a sibling `rocky-lsp(.exe)` in the same directory.
- If `rocky.server.path` is just `"rocky"` (the default), the client walks `$PATH` and returns the first `rocky-lsp` it finds.
- If nothing is found, the client falls back to the existing `rocky lsp` invocation unchanged.

Users without the split binary installed see no behaviour change — resolution is transparent.

`rocky-lsp` ships alongside `rocky` from engine-v1.10.0 onwards. Install it via `engine/install.sh` (which will be taught about the new archive in a follow-up) or by downloading the `rocky-lsp-<target>.tar.gz` archive from the GitHub Release. Until then, building locally (`cargo build --release --bin rocky-lsp` from the engine workspace) produces the binary.

### Added — Regenerated TypeScript bindings for engine 1.10.0

Engine 1.10.0 closes the perf-resilience roadmap's active arc. The regenerated interfaces in `src/types/generated/` surface additive schema changes from that push:

- **`PipelineEvent.attempt` / `error_class`** on the retry-event shapes emitted by Databricks / Snowflake / Fivetran adapter retry loops.
- **Hook context schemas** for the 15 lifecycle events wired into `rocky run`.
- **Top-level `[retry]` config** for cross-adapter shared `RetryBudget`.
- Additive `CompileOutput` timing fields from the incremental compiler path.

No source-code changes in the extension aside from the LSP-client integration above — existing commands continue to work unchanged.

## [1.4.2] — 2026-04-19

### Changed — Bundle extension with esbuild

The extension is now bundled with esbuild into a single `dist/extension.js` (~400 KB) instead of shipping `out/` plus the entire `node_modules/` tree. The VSIX drops from ~300 files to ~20, which clears the `vsce` packaging warning and speeds up activation. No runtime behavior changes — `vscode-languageclient` and its transitive deps are pulled into the bundle. Webview assets (viz.js) still ship as-is under `media/`.

## [1.4.1] — 2026-04-19

### Changed — Tighter logo crop

Cosmetic refresh of the bundled icons. The Rocky mark now uses a tighter SVG viewBox (`223.63 222.92 580.06 580.06`) that crops the empty margin around the glyph, so the icon reads larger in the marketplace tile, the activity bar, and the file-icon gutter. The activity bar icon (`rocky-activity-bar.svg`) stays monochrome so VS Code theme colors keep applying. No code changes.

## [1.4.0] — 2026-04-17

### Added — Regenerated TypeScript bindings for engine 1.8.0

Engine 1.8.0 added a new required field on the run payload — \`RunOutput.interrupted: bool\` — as part of the graceful SIGINT / SIGTERM handling work (engine #128). The regenerated TypeScript interface in \`src/types/generated/run.ts\` now includes the field.

No source code changes in the extension itself. Existing commands (`rocky.run`, result decorations, run summary webview) continue to work unchanged — the type shim just knows about one more field if a caller reaches for it.

## [1.3.0] — 2026-04-17

### Added — Fully-autogenerated `rocky-project.schema.json`

The IDE validation schema for `rocky.toml` is now **fully generated from Rust types** by `just codegen` in the monorepo, up from a 696-line hand-maintained file to 2462 lines of complete coverage. The previous file had rotted — it knew only `replication` pipelines and was missing newer fields across every section.

What's new in IDE validation:

- **All 5 pipeline types** — `replication` (back-compat default when `type` is omitted), `transformation`, `quality`, `snapshot`, `load` — with per-variant required/optional field tracking.
- **DQX assertion kinds** — `not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`. Comparisons (`<`, `<=`, `>`, `>=`, `==`, `!=`) and their long-form aliases (`lt`, `lte`, `gt`, `gte`, `eq`, `ne`) both accepted.
- **Row quarantine** — `[pipeline.x.checks.quarantine]` with `enabled`, `mode` (`split`/`tag`/`drop`), `suffix_valid`, `suffix_quarantine`.
- **Governance sub-blocks** — `auto_create_catalogs`, `auto_create_schemas`, `tag_prefix`, `tags`, `isolation` (workspace bindings), `grants`, `schema_grants`.
- **Load pipelines** — `source_dir`, `format`, `target`, `options` (batch_size, create_table, truncate_first, csv_delimiter, csv_has_header).
- **Schema evolution** — `grace_period_days` under `[schema_evolution]`.
- **Hook types** — 17 known lifecycle events (`on_pipeline_start`, `on_materialize_error`, `on_after_checks`, etc.) with `command`, `timeout_ms`, `on_failure`, `env` fields; webhook variants with `url`, `preset` (slack/teams/pagerduty/datadog/generic), `body_template`, `secret`, `retry_count`, `retry_delay_ms`, `headers`, `async`.
- **Typo detection** — unknown fields in `[state]`, `[cost]`, `[adapter.*]`, `[pipeline.*.execution]`, `[pipeline.*.target.governance]`, and every other non-pipeline-variant section now surface as IDE errors instead of being silently ignored.

Pipeline-variant typos still slip past the IDE schema due to a JSON Schema Draft-07 interaction between `additionalProperties: false` and the `type`-discriminator injection; the engine continues to reject them at parse time.

No source code changes in the extension itself — the schema file is regenerated by `just codegen` in the monorepo and copied into `editors/vscode/schemas/` with a generated-file banner. LSP client, syntax highlighting, commands, lineage webview, and hover behavior are all unchanged.

## [1.2.0] — 2026-04-17

### Added — Regenerated TypeScript bindings for engine 1.5.0 + 1.6.0

Engine-side DQX parity work (Phases 1–4) and the `AdapterConfig` JsonSchema derive landed a large crop of new fields in the typed output surface. The regenerated bindings (via `just codegen`) now include:

- **`RunOutput.quarantine: QuarantineOutput[]`** — row-quarantine outcomes per table.
- **`CheckResult.severity`** — `"error" | "warning"` on every quality check.
- **`CheckDetails.Assertion`** — new variant covering all row-level assertion kinds (`not_null`, `unique`, `accepted_values`, `relationships`, `expression`, `row_count_range`, `in_range`, `regex_match`, `aggregate`, `composite`, `not_in_future`, `older_than_n_days`).
- **`AdapterConfig`, `AdapterKind`, `RetryConfig`** — new `adapter_config.ts` module with the typed adapter-config shape.

No source code changes in the extension itself — pure binding regeneration plus the version bump and the `engines.vscode` bound fix from PR #112 (`^1.105.0` so CI works against the minimum VS Code version the extension supports). LSP client, syntax highlighting, commands, lineage webview, and hover behavior are all unchanged.

### Fixed

- `engines.vscode` bound lowered to `^1.105.0` (PR #112) — was `^1.99.0`, causing CI to reject the package against the vscode test runner's minimum supported version. No effect on end users; `^1.105.0` matches what the extension actually requires.

## [1.1.1] — 2026-04-16

### Fixed

- Lineage webview no longer fails to load: added `wasm-unsafe-eval` to the
  webview's Content Security Policy directive (PR #84). The lineage view
  loads a WASM bundle for column-lineage extraction; without this CSP, the
  bundle was rejected and the panel rendered blank in production builds.

### Added — `DagRunOutput` TypeScript interface

Regenerated `src/types/generated/dag_run.ts` (via `just codegen`) exposes
the typed shape of `rocky run --dag --output json` from engine 1.2.0:
`DagRunOutput` and `DagRunNodeOutput` (per-node id, kind, label, layer,
status, duration_ms, error). Consumers can now parse the new DAG run
output without manual typing.

No source code changes in the extension itself for this addition — pure
binding regeneration to keep the typed surface in sync with the engine.

## [1.0.1] — 2026-04-14

### Fixed

- Fix extension failing to activate — `.vscodeignore` was stripping the `vscode-languageclient` runtime dependency from the VSIX package, causing a crash on `require("vscode-languageclient/node")` before `activate()` could run. No commands, output channel, or LSP client would register.
- Add dedicated monochrome activity bar icon (`rocky-activity-bar.svg`) so the sidebar icon renders correctly with VS Code theme colors instead of showing a solid dark square.

## [1.0.0] — 2026-04-13

### Changed — Phase 2 schema codegen integration

`src/types/rockyJson.ts` is now a 100% type-alias shim over the autogenerated TypeScript interfaces in `src/types/generated/`. The interfaces are generated from `../../schemas/*.schema.json` (committed) via `json-schema-to-typescript`, with the schemas themselves derived from typed Rust output structs in the engine via `schemars`. To regenerate, run `just codegen` from the monorepo root.

The 28 generated interfaces cover every Rocky CLI command that emits `--output json`. Running the regen pipeline against a CLI source change automatically updates `src/types/generated/<command>.ts` and the `src/types/generated/index.ts` barrel.

### Fixed — consumer code aligned to real CLI output shapes

The `rockyJson.ts` swap surfaced real bugs in three consumers that had been hidden by permissive `[k: string]: unknown` index signatures on the old hand-written interfaces:

- `commands/compile.ts` was comparing diagnostic severity to lowercase strings (`"error"`, `"warning"`) but the actual JSON is PascalCase (`"Error"`, `"Warning"`). The hand-written `CompileDiagnostic.severity` was typed as `"error" | "warning" | "info" | string`, where the trailing `string` swallowed every real value. Fixed.
- `commands/compile.ts` `ci()` was reading nested `result.compile?.models` and `result.tests?.passed`. The actual `CiOutput` has flat `models_compiled` / `tests_passed` / `tests_failed` fields with no nested sub-objects. Fixed.
- `commands/test.ts` and `testExplorer.ts` were destructuring `result.failures` as `[name, message]` tuples. The actual `TestFailure` is an object `{name, error}`. Fixed.
- `commands/migration.ts` was reading `result.models_imported` and `result.warnings?.length`. The actual `ImportDbtOutput` renames `models_imported` → `imported` and types `warnings` as a count (the array of warning structs is in `warning_details`). Fixed.

These bugs were the kind of drift Phase 2 was specifically designed to catch.

See [GitHub Releases (filtered to vscode)](https://github.com/rocky-data/rocky/releases?q=vscode) for detailed release notes. The Rocky VS Code extension is released from the `rocky-data/rocky` monorepo under the `vscode-v*` tag prefix.
