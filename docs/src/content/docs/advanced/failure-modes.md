---
title: Failure modes
description: Taxonomy of how Rocky pipelines fail and the recovery playbook for each category
sidebar:
  order: 2
---

When a Rocky pipeline misbehaves, the symptom you see (a stack trace, a stuck run, a wrong number) almost always falls into one of nine categories. This page lists them with the **detection signal** (what Rocky surfaces in the CLI / JSON output / dagster fixture) and a **recovery playbook** (the canonical sequence of steps to get back to green).

For symptom-first lookup ("I got error X, what do I do?"), see [Troubleshooting](./troubleshooting). This page is the inverse: start from the category, end at the action.

## Quick taxonomy

| Category | Detection signal | Surface |
|---|---|---|
| [Compile-time](#1-compile-time-failures) | `severity: Error` diagnostic with code `E001`, `E020`–`E028` | `rocky compile`, `rocky ci`, LSP red squiggles |
| [Contract violations](#2-contract-violations) | Diagnostic codes `E010`–`E013` | `rocky compile`, `rocky ci`, `rocky apply` (pre-flight) |
| [Schema drift](#3-schema-drift) | `DriftAction` enum on `rocky drift` / `rocky apply` output | `rocky drift`, `rocky apply` materialisation block |
| [Quality check failures](#4-quality-check-failures) | `check_results[].status == "Failed"` | `rocky apply --output json` |
| [Adapter / runtime failures](#5-adapter--runtime-failures) | Non-zero `rocky apply` exit + `error` field on materialisation | `rocky apply`, `rocky doctor` |
| [State store failures](#6-state-store-failures) | `state_sync.status: failed`, lockfile errors, missing watermarks | `rocky doctor`, `rocky state` |
| [Hook failures](#7-hook-failures) | `hook_results[].status == "Failed"`; `on_failure: error` aborts the run | `rocky apply --output json`, `rocky hooks test` |
| [Cost / budget violations](#8-cost--budget-violations) | `budget_violation` field on materialisation; `--enforce-budgets` flips to non-zero exit | `rocky cost`, `rocky apply` |
| [Governance failures](#9-governance-failures) | `permissions_diff` returns errors, `mask_actions` lists unresolved tags (`W004`) | `rocky apply`, `rocky plan --env` |

The categories are **independent**: a single pipeline can hit several at once, and the recovery for each is independent of the others. When triaging, work down the list in order: compile-time failures fail fast and cheap, runtime failures cost warehouse credits, governance failures land at the very end of a successful materialisation.

---

## 1. Compile-time failures

**Definition.** Anything caught by `rocky compile` (or `rocky ci`, which wraps compile) before any warehouse call. No credentials needed; no money spent. Diagnostics use the standard severity / code / span shape and are emitted as JSON, terminal-rendered miette reports, or LSP diagnostics depending on caller.

**Detection signal.** A `Diagnostic` with `severity: Error` in the `diagnostics` array on `CompileOutput` / `CiOutput`. Error codes used today:

| Code | Failure |
|---|---|
| `E001` | Type mismatch on a column reference |
| `E020`–`E026` | `time_interval` model misconfiguration (placeholders, granularity, nullability) |

(See [Contract violations](#2-contract-violations) for `E010`–`E013`; they are formally compile-time but get their own section because the recovery is contract-shaped, not type-system-shaped.)

**Recovery playbook.**

1. Run `rocky compile --output table` to see the diagnostic in context with source span underline.
2. If you're in VS Code with the [Rocky extension](../../guides/ide-setup), the LSP already shows the same diagnostic with hover detail and a `Quick Fix` action where one is available (`E010` / `E013` ship deterministic fixes; everything else may surface an AI-generated fix when `ANTHROPIC_API_KEY` is set).
3. Fix the model SQL or the upstream contract that triggered the diagnostic.
4. Re-run `rocky compile` until clean.

A `rocky apply` against a project with compile errors aborts before any warehouse work, so fix red diagnostics before debugging runtime symptoms.

**Per-model compile failure during a run.** The whole-project abort above is the common case. A model that compiles in isolation but fails to compile when its turn comes during a run (for example after an upstream change shifts a type) is now contained at the table boundary rather than passed over: it's counted in `tables_failed`, gets an `errors[*]` entry with [`failure_kind: "compile-error"`](./per-table-error-containment#failure_kind-taxonomy) carrying the diagnostic, and the run exits non-zero (status `Failure`, or `PartialFailure` when other models succeeded). Earlier engine versions skipped the model and still reported the run as a success.

---

## 2. Contract violations

**Definition.** A model's output schema doesn't match its data contract (`<model>.contract.toml`). The contract specifies required columns, protected columns, and expected types / nullability; violations are caught at compile time, before any warehouse work.

**Detection signal.** Diagnostic codes `E010`–`E013`:

| Code | Severity | Meaning |
|---|---|---|
| `E010` | Error | Required column missing from model output |
| `E011` | Error | Column type mismatch (contract vs model output) |
| `E012` | Error | Nullability violation (contract says non-nullable, model says nullable) |
| `E013` | Error | Protected column has been removed |

**Recovery playbook.**

1. Open the affected model. The diagnostic message names the column verbatim.
2. For `E010` / `E013`, the LSP code-action surface offers a deterministic `Add` / `Restore` fix when an upstream model exposes the column. When it can't (multi-statement SQL, `SELECT *`, or the column needs derivation), an AI-powered fallback proposes a rewrite if `ANTHROPIC_API_KEY` is set.
3. For `E011` / `E012`, decide whether to:
   - update the model SQL to produce the contracted type / nullability (the common case), or
   - update the contract, only if the schema change is intentional and downstream consumers have been migrated.
4. Re-run `rocky compile` to confirm.

Contract violations are the load-bearing trust signal for downstream consumers: a passing contract is the lever that lets you refactor a model's internals without breaking everyone reading from it.

---

## 3. Schema drift

**Definition.** The source schema differs from the target table's current schema. Rocky's [graduated drift handling](../../concepts/schema-drift) tries to handle the divergence in place (`ALTER COLUMN TYPE` for safe widenings, `ALTER TABLE ADD COLUMN` for new columns) and falls back to drop-and-recreate only when it can't.

**Detection signal.** The `DriftResult` struct on `rocky drift` / `rocky apply --output json` carries `action: DriftAction`. Three possible actions:

| `action` | Meaning |
|---|---|
| `Ignore` | Drift detected but a `[drift] mode = "warn"` policy says to log and continue |
| `AlterColumnTypes` | Safe in-place widening planned (e.g. INT → BIGINT) |
| `DropAndRecreate` | Source/target diverged in a way Rocky can't widen; full refresh next run |

The `drifted_columns` array names which columns changed and what the divergence looks like; `added_columns` lists columns present upstream but missing in target (will get an `ADD COLUMN`); `columns_to_drop` lists target columns whose grace period has expired and will be removed.

**Recovery playbook.**

- **`Ignore`**: no action needed; the pipeline already chose to surface drift as a warning. Audit `[drift] mode` in `rocky.toml` if the policy doesn't match your team's appetite.
- **`AlterColumnTypes`**: let the next `rocky apply` apply the `ALTER`. Verify in your warehouse afterwards that downstream tables / views / dashboards still parse the widened type correctly.
- **`DropAndRecreate`**: Rocky will full-refresh the target on the next run. If the table is large or downstream consumers can't tolerate the temporary unavailability, schedule the next run during a maintenance window.
- For columns in the **grace period** (`grace_period_columns`), decide before the deadline whether to keep them (re-adding upstream restores the column) or accept the drop.

Schema drift is the only category where the runtime takes a corrective action *automatically*. The playbook is mostly "audit Rocky's plan, then let it run."

---

## 4. Quality check failures

**Definition.** An [inline data quality check](../../concepts/data-quality-checks) declared in `rocky.toml` (`[checks.<name>]`) failed against the materialised data. Checks run after each model materialises; a failed check does not abort the run by default but is surfaced in the run output and dagster Pipes events.

**Detection signal.** `RunOutput.check_results[]` contains a `CheckResult` per declared check, each with `status: "Passed" | "Failed" | "Skipped"`, `failure_count: u64`, and `failed_sample` rows.

**Recovery playbook.**

1. Identify the failing check from `rocky apply --output json | jq '.check_results[] | select(.status == "Failed")'`. Each failure carries the failing row sample so you can reproduce in the warehouse.
2. Decide whether the failure is a **data issue** or a **check-definition issue**:
   - **Data issue** (the upstream data violated an expectation the check was right to enforce): triage upstream, replay or backfill the offending partition, then re-run.
   - **Check-definition issue** (the check assertion is stricter than reality should be): adjust the check threshold / predicate in `rocky.toml`. Re-run.
3. For checks that are *advisory* rather than gating, set `severity = "warn"` on the check so it lands in the output as a warning instead of a failure. This preserves the signal without flipping the run status.

**Why checks don't abort by default.** A pipeline with one failed check on `model_A` shouldn't block downstream materialisations of unrelated models. To make checks fail the run hard, set `[execution] fail_on_check_error = true` in `rocky.toml` or pass `--fail-on-check-error` to `rocky apply`.

---

## 5. Adapter / runtime failures

**Definition.** A warehouse call (compile-passing, contract-passing, drift-handled) failed at execution time. Network errors, auth errors, quota errors, statement timeouts, and deadlocks: anything that originates inside the adapter rather than the engine.

**Detection signal.** Non-zero `rocky apply` exit code, an entry on `RunOutput.errors[*]` per failed table (with a typed [`failure_kind`](./per-table-error-containment#failure_kind-taxonomy) discriminator the orchestrator can branch on), plus a transient/rate-limit classification on the underlying error. Other tables in the same run continue; see [Per-table error containment](./per-table-error-containment).

The dispatched adapter classifies its own failures:

| Adapter | Common failure modes |
|---|---|
| Databricks | `401 Unauthorized` (PAT expired / OAuth M2M misconfigured), statement timeout, rate-limit on `information_schema` queries |
| Snowflake | Auth chain rejection (OAuth → JWT → password), warehouse suspended, query result-size cap |
| BigQuery | Quota exceeded, auth scope mismatch, BIGNUMERIC type drift |
| DuckDB | File lock contention, out-of-memory on large CTAS |
| Fivetran / Airbyte | `403 Forbidden` (missing API scope), connector currently syncing |

### Classified retry

Since engine 1.58.0 the run loop retries proven-transient failures itself, **on by default**. A model whose materialization fails is classified via the adapter's own retryable judgement into `Transient`, `Permanent`, or `Unknown`; only a *proven* transient failure (a 429, a connection reset, a warehouse warming up, a lock conflict) is re-run, with capped exponential backoff. `Permanent` and `Unknown` failures are never retried, and auth errors are never retried even when an adapter labels them transient — expired credentials don't heal on a second attempt. Every retry is recorded as an attempt trail on the execution record and surfaced in the run's JSON output.

```toml
[resilience]
transient_max_retries = 2   # default; at most three attempts per model. 0 opts out.
```

A run-loop circuit breaker backs this up: after several consecutive transient model failures (default 3) no further model is retried for the rest of the run, so a systemically unhealthy warehouse fails fast instead of multiplying the retry budget across the DAG. Set `transient_max_retries = 0` (or `[resilience] enabled = false`) to restore the prior single-attempt behavior, for example in CI where a fast fail is preferred.

The consequence for orchestrators: by the time a `failure_kind: "transient"` entry reaches your `errors[*]`, the engine has already retried it within the run. An immediate external retry duplicates work; prefer a delayed re-run or `--resume-latest`.

**Recovery playbook.**

1. Run `rocky doctor --output json` first. The `adapters[]` block tells you which adapter Rocky thinks should work and which it currently can't reach. Treat doctor as a credentials / connectivity smoke test.
2. For **transient** failures (entries with `failure_kind: "transient"` or `"connection-failed"` on `errors[*]`), the engine has already retried them in-run (see [Classified retry](#classified-retry)) — a failure that still surfaced exhausted its retry budget. Once the underlying condition clears, use `rocky plan --resume-latest && rocky apply <plan-id>` (or the single-step `rocky run --resume-latest` alias) to pick up where the failed run left off rather than restarting from scratch.
3. For **auth** failures, walk the adapter's auth chain (e.g. Snowflake: OAuth → JWT → password) and verify the env-vars / config in `rocky.toml`. The [authentication guide](../../reference/authentication) has the per-adapter checklist.
4. For **quota** failures, check the warehouse-side quota dashboard. Rocky's adaptive concurrency (Databricks AIMD throttle) automatically backs off, but a hard quota reset is a warehouse-side action.
5. For **statement timeouts**, increase `timeout_secs` on the adapter, or better, re-evaluate whether the model's materialization strategy is right (a multi-hour `FullRefresh` is often a missed `Merge` or `Incremental` opportunity; `rocky optimize` will surface the recommendation).

---

## Failure containment across the model graph

By default a transformation run **fails fast**: the first model that fails stops the run, and models not yet built are skipped. Opt into *containment* to continue disjoint work instead:

```toml
[resilience]
contain_failures = true   # default: false
```

With containment on, a failed model and its **downstream closure** are withheld while unrelated subtrees still materialize. The run reports `PartialFailure`, listing the withheld models on `RunOutput.contained[*]` (each naming what blocked it, with an unblock hint) and the failure causes on `RunOutput.errors[*]`. For a partitioned (`time_interval`) model, a failed partition withholds the model's downstream while its healthy partitions still land.

**Guarantee scope.** Containment is *guaranteed* for dependencies declared via `ref()` and for physical reads Rocky can statically resolve — `schema.table`, `catalog.schema.table`, quoted or unquoted. Those are folded into both the withholding closure and the execution ordering, so a downstream of a failure is never built on its stale or missing output, and under `--parallel` a reader is scheduled strictly after every producer it reads.

Reads Rocky **cannot enumerate** — a model built on a CTE, sub-query, or set operation — are handled on a **best-effort** basis identical to a normal fail-fast run. Such a model is still contained when it has a *known* failed upstream, but because its reads can't be resolved into an ordering edge, under `--parallel` a same-layer reader of a failing producer can materialize on stale data — exactly as a fail-fast run does in that case. This is a documented boundary, not a regression: containment never materializes anything a fail-fast run wouldn't. **Declare the dependency with `ref()` for a hard containment guarantee.**

Default is off; the fail-fast behavior described in the sections above is unchanged unless you set `contain_failures = true`.

---

## 6. State store failures

**Definition.** Rocky's embedded state store (redb at `<models>/.rocky-state.redb` by default) holds watermarks, run history, branch state, and partition progress. Failures here either prevent a run from starting (lock contention, corruption) or quietly degrade an incremental run to an unintended full refresh (missing watermark).

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| `state file locked` | `rocky apply` aborts immediately; `rocky doctor.state.status: failed` |
| `state file corrupted` | `rocky doctor.state.status: failed`; the structured error names the corrupted table |
| Missing watermark | `rocky state --output json` shows `watermarks: []` for a model that should have one; the next run becomes a `FullRefresh` |
| `state_sync` upload failure | `rocky doctor.state_sync.status: failed`; the local state still works but the remote backup is stale |

**Recovery playbook.**

1. **Locked.** Run `ps aux | grep rocky` to find the holder. Real concurrency? Kill the second invocation. Stale lock from a crashed run? `rm <models>/.rocky-state.redb-lock` (the file extension may vary by redb version; `rocky doctor` will name it).
2. **Corrupted.** Restore from your `state_sync` backup if you have one (`rocky state restore --from <backend>`); otherwise `rm <models>/.rocky-state.redb` and accept that the next run will be a full refresh of every incremental model.
3. **Missing watermark.** Run `rocky state set <model> --watermark <iso8601>` to seed the watermark from a known-good prior run, or accept a one-off full refresh.
4. **`state_sync` failed.** The local state is fine; check the backend's credentials (S3, Valkey) and re-run when ready. The state will sync on the next successful run.

**Why state failures are rare but high-impact.** A corrupted state file isn't a Rocky bug; it's usually disk full or a process killed mid-write. But the blast radius is large because every incremental model degrades to full refresh until state is restored. Wire `state_sync` for any production deployment.

---

## 7. Hook failures

**Definition.** A pipeline lifecycle hook (`on_pipeline_start`, `on_pipeline_end`, `on_model_success`, `on_model_failure`, etc.), whether a shell command, webhook, or templated payload, failed.

**Detection signal.** `RunOutput.hook_results[]` contains a `HookResult` per fired hook, with `status: "Succeeded" | "Failed" | "Skipped"`, `duration_ms`, and (for command hooks) `stdout` / `stderr` snippets. The hook's `on_failure` setting decides whether the run aborts (`error`) or continues with a warning (`warn`).

**Recovery playbook.**

1. Reproduce the hook locally with `rocky hooks test <event> --output json`; this fires the hook in isolation against a dummy event payload.
2. If the hook is a **shell command** that exits non-zero, fix the script (or its env-var assumptions; hooks inherit the run's env, not your shell).
3. If the hook is a **webhook**, check the receiver's logs for the actual rejection. Rocky surfaces only the HTTP status; the receiver's body usually has the actionable message.
4. If a hook is **flaky** (network blip, third-party rate limit), set `on_failure = "warn"` so transient failures don't gate the run, and rely on `hook_results[]` in your dagster fixture / observability stack to flag the regression.

Hook failures look like runtime failures, but the fix is in your hook script or webhook receiver, not in the pipeline.

---

## 8. Cost / budget violations

**Definition.** A model's actual run cost exceeded the per-model `[budget]` block in its sidecar `.toml`, or the project-level cost-projection (`rocky cost --output json`) flagged a PR as over-budget vs. the base ref.

**Detection signal.** `MaterializationOutput.budget_violation` (post-run) or `CostOutput.summary.delta_usd` (pre-run, branch-vs-base). The `--enforce-budgets` flag flips per-model violations from warnings into a non-zero `rocky apply` exit.

**Recovery playbook.**

1. Run `rocky cost --output json` to see the current cost projection, broken down per model.
2. For a violation that's **expected** (model intentionally got more expensive, backfilling a wider date range), bump the `[budget].max_usd_per_run` in the model's sidecar.
3. For a violation that's **unexpected** (model cost spiked without an obvious cause), check:
   - Did a `MaterializationStrategy` change recently (e.g. `Merge` → `FullRefresh`)? `rocky optimize --output json` will recommend a cheaper strategy if one fits.
   - Did the upstream row count grow significantly? `rocky history --model <name>` will show row-count history.
   - Is the SQL doing a cross-join or other antipattern? `rocky lineage --column` can help identify which upstream column is the cost driver.
4. For PR-time violations, the [`rocky-preview` GitHub Action](../../guides/preview-a-pr) renders the cost delta in the PR comment so reviewers see it before merge.

**Why budget violations are advisory by default.** Cost is signal, not gate, until you've calibrated budgets against real usage. Switch to `--enforce-budgets` once your `[budget]` blocks reflect reality; until then, `rocky cost` warnings on every PR are the calibration loop.

---

## 9. Governance failures

**Definition.** Anything in Rocky's [governance layer](../../guides/governance) (permissions, classification, masking, retention) that didn't apply cleanly. Permission diffs that the warehouse rejected, mask classifications that didn't resolve to a strategy, retention sweeps that couldn't acquire a target.

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| Permission grant rejected | `RunOutput.permissions_diff`, entries with non-zero `errors` |
| Unresolved classification | `W004` warning at compile time per `(model, column, tag)` triple |
| Mask resolution mismatch | `rocky plan --env <name>` `mask_actions` shows the resolved strategy is `None` for a column that shouldn't be unmasked |
| Retention sweep failure | `rocky retention-sweep --output json` carries per-model `success: false` |

**Recovery playbook.**

1. **Permission rejected.** Usually a missing principal (group / user not in the warehouse) or a missing parent grant (`USE CATALOG` before `USE SCHEMA`). The error text from the warehouse is verbatim in the diff entry; act on it directly.
2. **Unresolved classification (`W004`).** Either add the tag to a `[mask]` / `[mask.<env>]` block in `rocky.toml`, or list it in `[classifications.allow_unmasked]` to opt out explicitly. The implicit-allow path is denied by design: Rocky surfaces unresolved tags rather than silently leaking the column.
3. **Mask mismatch.** Re-run `rocky plan --env <env>` to preview what Rocky would apply. The active env's `[mask.<env>]` overrides the workspace `[mask]` defaults; if the override isn't taking effect, double-check the env name spelling and the inheritance order documented in the [governance guide](../../guides/governance).
4. **Retention sweep failure.** Usually a missing partition column or a permissions issue on the target. Run `rocky doctor --output json` to confirm the adapter has the right grants on the target schema.

Permissions and masking apply *after* materialisation succeeded, so a governance failure means the data landed but isn't fully wired into your access model. Recovery is rarely time-critical, but the failure must close before the next compliance audit.

---

## See also

- [Per-table error containment](./per-table-error-containment): how the run path isolates failures at the table boundary and how to consume the `failure_kind` discriminator
- [Troubleshooting](./troubleshooting): symptom-first lookup ("I got error X")
- [`rocky doctor`](../../reference/cli#doctor): aggregate health check across config, state, adapters, pipelines
- [`rocky plan --resume-latest`](../../reference/cli#run): resume a failed run from its checkpoint (canonical, auditable form; the single-step `rocky run --resume-latest` alias does the same in one invocation)
- [Schema drift](../../concepts/schema-drift): graduated drift handling deep dive
- [Data quality checks](../../concepts/data-quality-checks): inline check authoring + result shape
- [Governance guide](../../guides/governance): permissions, classification, masking, retention
