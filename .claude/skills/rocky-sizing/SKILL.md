---
name: rocky-sizing
description: Velocity calibration for Rocky engineering estimates. Use when sizing a PR, writing a design memo's effort line, answering "how long will X take?", or evaluating whether an advisor / agent estimate feels inflated. Captures the observed throughput on rocky-data (started late March 2026) and the wall-clock-bound exceptions.
---

# Rocky sizing calibration

Conventional software-engineering sizing under-counts Rocky's actual velocity by **2–3×**. This skill captures the observed throughput, the wall-clock-bound exceptions that don't shrink, and concrete reference PRs as size anchors. Use it before quoting "days" in any memo or response.

## Why this skill exists

Average-engineer estimates assume:
- Design pass is a separate calendar item (half-day to a day).
- Test writing is separate from coding (adds ~30% time).
- Code review introduces queue time (hours to days).
- Wall-clock-bound steps (CI, sandbox roundtrips) blend into general sizing.

None of those hold on Rocky. Hugo writes design memos in the same flow as code; tests ship in the same PR as the feature; solo project means zero review queue; only true external-I/O steps (Databricks audit-log inspection, BigQuery sandbox calls) cost real wall-clock.

External advisors and agents don't know this. They will routinely return "3–5 days" for work that ships in a day. Push back, anchored on the reference PRs below.

## Reference PRs as size anchors

These all shipped **same-day** (2026-05-20, single batch). Use them as size anchors when sizing similar-shape work.

| Anchor PR | Surface | LOC | Tests | One-day? |
|---|---|---|---|---|
| **#600** D-2 spike | New trait method (`CatalogClient::table_stats`) + Iceberg impl + Unity `UnsupportedOperation` stub | 8 files, +530/-6 | 7 (5 wiremock + 1 unit + 1 live `#[ignore]`) | Yes |
| **#602** D-1 IR field | `CostBudget` struct + `cost_ceiling` field on `ModelIr` + diagnostic | 3 files, +296 | 11 (5 IR + 3 sidecar + 3 diagnostic) | Yes |
| **#604** Arc 4 artifact ledger | `OUTPUT_ARTIFACTS` redb side-table + `ArtifactRecord` struct + 4 query methods + schema v6→v7 | 4 files, +622 | 11 (8 unit + 3 integration) | Yes |
| **#605** D-3 stage 1 | New `cost_check.rs` module + `rocky compile` wiring + Databricks `DESCRIBE DETAIL` parsing | 6 files, +745/-45 | 16 (10 unit + 4 wiring + 4 parsing + 2 live) | Yes |
| **#606** D-3 stage 2 | Plan-time real catalog stats wiring + `on_breach` policy + codegen cascade | 10 files, +677/-4 | (cascade only) | Yes (3 retries) |
| **#601** PipelineEvent fill | `bus.emit` at 7 gap sites surfaced by inverse-finding | (small) | — | Yes |

**Aggregate for the day:** ~2,800 LOC of new feature code, ~40 tests, across 4 substantive PRs. Even with two watchdog retries on #606.

Earlier batches scale similarly: **Wave 2 Phase 1–5 shipped 13 PRs (#488–#503)** in a single phase (per `project_wave2_phase1_shipped`).

## The recalibration rule

When sizing a PR, find the closest **anchor** above and use its day-count as a ceiling. Most Rocky work fits inside one of these shapes:

| Anchor shape | Day-count |
|---|---|
| Single new trait method + 1 adapter impl + tests | 0.5–1 day (like #600) |
| New IR field + diagnostic + sidecar plumbing + tests | 0.5–1 day (like #602) |
| New crate-internal module + wiring into existing CLI command + tests | 1 day (like #605) |
| Cross-crate wiring + codegen cascade | 1 day (like #606, +25% for watchdog retries) |
| New side-table + redb schema bump + query API + tests | 1 day (like #604) |

If your initial estimate is **more than 2× the closest anchor**, recalibrate before quoting.

## What stays wall-clock-bound (does NOT shrink)

These cost real time regardless of coding velocity. Add them on top of the anchor.

| Wall-clock cost | Time | Why |
|---|---|---|
| **Databricks sandbox live-verify** (audit-log inspection, governance round-trip) | 0.5–1 day | Unity audit logs have propagation delay; inspection is a separate REST cycle. Required per `feedback_bq_live_verify_required` analog. |
| **BigQuery sandbox live-verify** | 0.5–1 day | Required for any BQ adapter change per `feedback_bq_live_verify_required`. Window closes 2026-06-30. |
| **Snowflake sandbox live-verify** | 0.5 day | Per `reference_snowflake_sandbox`. PAT-based, fewer surprises than BQ/Databricks. |
| **Iceberg REST live integration** | 0.5 day | Live wiremock can substitute for most paths; only the credential vending + commit-conflict paths need real catalog. |
| **CI matrix run (Windows / cross-platform)** | ~30 min × N PRs | Asynchronous; doesn't block coding but adds calendar gap if it surfaces a regression. |
| **Triple-cut release tag + publish** | 0.5 day | Multi-artifact tag race, `gh release edit --latest` fix, PyPI / Marketplace propagation. Per `feedback_triple_cut_release_convention`. |

## What does NOT cost real time at Hugo's velocity

Push back if an estimate includes any of these as separate line items:

- **"Design pass / spike memo"** — Hugo writes design memos *in the same flow* as code (see Arc 4 + catalog-first design memos shipped alongside their reframe commits).
- **"Test writing"** — already counted in the anchor LOC + test counts above. Tests ship in the same PR.
- **"Code review cycle"** — solo private repo, no review queue. PR open → merge can happen in minutes.
- **"PR description writing"** — Hugo writes detailed PR bodies during commit, not as a separate phase.
- **"Documentation update"** — usually a same-PR docstring edit; CHANGELOG bumps happen at release-cut time as a separate small PR.

## Watchdog-retry buffer (agent execution only)

PR #606 took **3 execution attempts** at the 600s watchdog. The pattern: tasks requiring **10+ file Reads + iterative `cargo build` cycles** hit it reliably (per `feedback_check_target_version_before_bundling`).

If sizing an *agent-executed* task in that shape, add **+25%** to the day-count purely for tooling-retry friction. This is not engineering scope — it's harness friction. When Hugo executes the same task directly (no agent dispatch), the buffer disappears.

Better dispatch shape that avoids the stall: narrow agent scope to "implement X in file Y," not "salvage WIP from worktree Z."

## When NOT to apply this calibration

- **Pure research / strategy work.** Brainstorms, Phase-0 landscape memos, exploration plans — these are wall-clock-bound by reading speed + thinking time, not coding velocity. Size at conventional rates.
- **Cross-customer coordination** (GOLD integration cuts, customer-signal-gated work). Wall-clock is determined by the customer side, not Rocky's velocity.
- **Multi-day investigations** that involve real exploration (e.g., why does a Snowflake live test fail intermittently). Those are debug-shaped, not feature-shaped — sizing inflates honestly.
- **First time touching a new external API** (e.g., when the BQ adapter first shipped). Learning curve dominates the first PR; subsequent PRs revert to anchor sizing.

## Sample recalibrations

Two recent examples (2026-05-20):

| Item | Average-engineer estimate | Recalibrated at Rocky velocity |
|---|---|---|
| Arc 4 span retention (JSONL `Layer` + `read_trace` + sweep entry + tests) | ~1 day | **0.5 day** — shape matches #604 (single side-table + query API + tests) shipped same-day. |
| Catalog-first Phase 1 wiring + Phase 0 spike measurement (compose `UnityCatalogClient` into `DatabricksWarehouseAdapter` + caller-grep + deprecation annotations + Databricks live-verify) | ~5.5 days | **2–2.5 days** — coding shape matches #600 / #605; +0.5–1 day wall-clock for Databricks audit-log inspection. |

The advisor pushed the catalog-first estimate to 4–6 days based on conventional norms; checking against the anchor PRs collapsed it to ~2.5.

## Process

When you're about to write a day-estimate in a memo / response / PR description:

1. **Find the closest anchor PR** above for the shape you're sizing.
2. **Take its day-count as the ceiling** for coding work.
3. **Add only the wall-clock-bound items** that genuinely apply (live-verify, CI matrix, release-cut).
4. **Add +25% if this is an agent-executed task** with 10+ file Reads in the iteration loop.
5. **If your number is more than 2× the anchor, stop and reconsider.** You're either missing simplification or padding with average-engineer overhead.

## Pointers

- `project_rocky_dev_start_and_prod_status` — Rocky started 2026-03-28; ~8 weeks of dev as of 2026-05-20.
- `project_wave2_phase1_shipped` — 13 PRs in one phase, reference for batched velocity.
- `feedback_bq_live_verify_required` / `reference_databricks_sandbox` / `reference_snowflake_sandbox` — where the wall-clock costs come from.
- `feedback_check_target_version_before_bundling` — watchdog-retry pattern source.
- `feedback_triple_cut_release_convention` — release-cut wall-clock anchor.
