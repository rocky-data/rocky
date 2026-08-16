---
title: Failure modes
description: The nine ways a Rocky pipeline fails, and the recovery steps for each one
sidebar:
  order: 2
---

A Rocky pipeline fails in one of nine ways. This page names each one, shows what Rocky prints when it happens, and gives you the steps back to green.

Every section has two parts. The **detection signal** is what you see in the CLI, in the JSON output, or in a Dagster fixture. The **recovery playbook** is the sequence of steps that fixes it.

Use this page when you know the category. Use [Troubleshooting](/advanced/troubleshooting/) when all you have is an error message.

## Find your category from the signal

Read whichever signal you already have. Then follow the branch to a section.

```
 WHAT YOU CAN SEE                     CATEGORY            START WITH
 ─────────────────────────────────    ────────────────    ─────────────────

 Rocky never called the warehouse
   ├─ code E010–E013                  2. Contracts        the contract file
   └─ any other error code            1. Compile-time     rocky compile

 The run printed JSON: which block is populated?
   ├─ drift.actions_taken[]           3. Schema drift     the drift action
   ├─ check_results[].checks[]        4. Quality checks   the failing check
   ├─ errors[*].failure_kind          5. Adapter/runtime  rocky doctor
   ├─ budget_breaches[]               8. Cost / budget    rocky cost
   └─ permissions                     9. Governance       the rejection text

 No JSON block names the fault
   ├─ rocky doctor says critical      6. State store      rocky doctor
   └─ only the run stderr shows it    7. Hooks            rocky hooks test
```

Two entries cross the branches, both of them on `errors[*]`.

An entry whose `failure_kind` is `"compile-error"` is a compile-time failure that Rocky caught during the run. [Section 1](#1-compile-time-failures) covers it.

An entry can also be a governance failure. Those classify as `failure_kind: "unknown"`, so check `permissions` before you treat one as an adapter failure. [Section 9](#9-governance-failures) covers it.

Entries on `contained[*]` are not a failure of their own. They name the models Rocky withheld because an upstream model failed. See [Failure containment across the model graph](#failure-containment-across-the-model-graph).

## The nine categories

| Category | Detection signal | Surface |
|---|---|---|
| [Compile-time](#1-compile-time-failures) | `severity: Error` diagnostic with code `E001`, `E020`–`E028` | `rocky compile`, `rocky ci`, LSP red squiggles |
| [Contract violations](#2-contract-violations) | Diagnostic codes `E010`–`E013` | `rocky compile`, `rocky ci`, `rocky apply` (pre-flight) |
| [Schema drift](#3-schema-drift) | `drift.actions_taken[].action` on `rocky run` / `rocky apply` output | `rocky run`, `rocky apply` drift block |
| [Quality check failures](#4-quality-check-failures) | `check_results[].checks[].passed == false` | `rocky run` / `rocky apply --output json` |
| [Adapter / runtime failures](#5-adapter--runtime-failures) | Non-zero `rocky apply` exit + entry on `errors[*]` with typed `failure_kind` | `rocky apply`, `rocky doctor` |
| [State store failures](#6-state-store-failures) | `state` / `state_sync` doctor checks report `critical`, lockfile errors, missing watermarks | `rocky doctor`, `rocky state` |
| [Hook failures](#7-hook-failures) | Hook failure in run stderr / logs; `on_failure: abort` aborts the run | `rocky hooks test`, run stderr |
| [Cost / budget violations](#8-cost--budget-violations) | `RunOutput.budget_breaches[]`; `on_breach = "error"` flips to non-zero exit | `rocky cost`, `rocky apply` |
| [Governance failures](#9-governance-failures) | warehouse-rejected grants in `errors[]`, `mask_actions` lists unresolved tags (`W004`) | `rocky apply`, `rocky plan --env` |

The categories are independent. One pipeline can hit several at once, and each recovery stands on its own.

---

## 1. Compile-time failures

**Definition.** A compile-time failure is anything `rocky compile` catches before Rocky calls the warehouse. `rocky ci` wraps compile, so it catches the same set. No credentials are needed and no warehouse credits are spent.

Every finding is a [diagnostic](/reference/glossary/#diagnostic-code): a stable code, a severity, and the source span that triggered it. Rocky renders diagnostics as JSON, as terminal reports, or as LSP squiggles, depending on the caller.

**Detection signal.** A `Diagnostic` with `severity: Error` in the `diagnostics` array on `CompileOutput` / `CiOutput`. Error codes used today:

| Code | Failure |
|---|---|
| `E001` | Type mismatch on a column reference |
| `E020`–`E026` | `time_interval` model misconfiguration (placeholders, granularity, nullability) |
| `E027` | Model's projected cost exceeds its `[budget]` ceiling |
| `E028` | Unresolved `@var` reference |

This table is not exhaustive. The compiler also emits `E010`–`E013` and `E030`–`E035`.

Codes `E010`–`E013` are formally compile-time failures. They get their own section because the fix is contract-shaped rather than type-shaped. See [Contract violations](#2-contract-violations).

**Recovery playbook.**

1. Run `rocky compile --output table`. It underlines the source span that triggered the diagnostic.
2. Read the same diagnostic in VS Code if you use the [Rocky extension](../../guides/ide-setup). The LSP adds hover detail and a `Quick Fix` action where one exists. `E010` / `E013` and the type-mismatch codes `E001`–`E003` ship deterministic fixes. Other codes may offer an AI-generated fix when `ANTHROPIC_API_KEY` is set.
3. Fix the model SQL, or fix the upstream contract that triggered the diagnostic.
4. Re-run `rocky compile` until it is clean.

`rocky apply` aborts before any warehouse work when the project has compile errors. Fix red diagnostics before you debug a runtime symptom.

**Per-model compile failure during a run.** The whole-project abort above is the common case. A model can also compile in isolation and then fail when its turn comes during the run. An upstream change that shifts a type does exactly this.

Rocky contains that failure at the table boundary instead of passing over the model. The model counts towards `tables_failed` and gets an `errors[*]` entry with [`failure_kind: "compile-error"`](/advanced/per-table-error-containment/#failure_kind-taxonomy) carrying the diagnostic. The run exits non-zero, with status `Failure`, or `PartialFailure` when other models succeeded. Earlier engine versions skipped the model and still called the run a success.

---

## 2. Contract violations

**Definition.** A model's output schema does not match its data contract (`<model>.contract.toml`). The contract declares required columns, protected columns, and the expected types and nullability. Rocky checks it at compile time, before any warehouse work.

**Detection signal.** Diagnostic codes `E010`–`E013`:

| Code | Severity | Meaning |
|---|---|---|
| `E010` | Error | Required column missing from model output |
| `E011` | Error | Column type mismatch (contract vs model output) |
| `E012` | Error | Nullability violation (contract says non-nullable, model says nullable) |
| `E013` | Error | Protected column has been removed |

**Recovery playbook.**

1. Open the model named in the diagnostic. The message names the offending column verbatim.
2. For `E010` / `E013`, take the LSP code action. It offers a deterministic `Add` / `Restore` fix when an upstream model exposes the column. It falls back to an AI-proposed rewrite when the column needs derivation, when the SQL is multi-statement, or when the model uses `SELECT *`. The fallback needs `ANTHROPIC_API_KEY`.
3. For `E011` / `E012`, choose one of two fixes. Change the model SQL to produce the contracted type or nullability, which is the common case. Or change the contract, but only when the schema change is intentional and downstream consumers have already migrated.
4. Re-run `rocky compile` to confirm.

A passing contract is what lets you refactor a model's internals without breaking the consumers that read from it.

---

## 3. Schema drift

**Definition.** [Drift](/reference/glossary/#drift) is a mismatch between the source schema and the target table's current schema. Rocky's [graduated drift handling](../../concepts/schema-drift) tries to fix the divergence in place. It runs `ALTER COLUMN TYPE` for a safe widening and `ALTER TABLE ADD COLUMN` for a new column. It drops and recreates the table only when it cannot do either.

**Detection signal.** The `drift` block (a `DriftSummary`) on `rocky run` / `rocky apply --output json`. It reports `tables_checked`, `tables_drifted`, and one `actions_taken[]` entry per drifted table. Each entry carries `table`, `action`, and a human-readable `reason`. Rocky adds an entry only when it changed the target, so a table with no drift produces no entry. Three actions reach the wire:

| `action` | Meaning |
|---|---|
| `add_columns` | The source has columns the target lacked, and no type drifted. Rocky ran `ALTER TABLE ADD COLUMN` for each one. |
| `alter_column_types` | Every changed type was a safe widening (e.g. INT → BIGINT). Rocky ran `ALTER` on each drifted column, plus any new columns. |
| `drop_and_recreate` | A type change was not a safe widening. Rocky dropped the target and rebuilt it with a full refresh. |

Rocky applies each action during the run that reports it. The entry records what already happened. It is not a plan for the next run.

Each entry's `reason` names the columns behind the action, one phrase per column. The JSON carries no other column-level detail.

Rocky does not detect a column that disappeared from the source. No action covers a removed column, and no grace period runs today.

**Recovery playbook.**

- **`add_columns`**: Rocky already added the columns. They are nullable, so historical rows hold NULL. Check any downstream model that reads them.
- **`alter_column_types`**: Rocky already widened the columns. Check that downstream tables, views, and dashboards still parse the wider type.
- **`drop_and_recreate`**: Rocky already dropped the target and rebuilt it. Consumers saw the table disappear and come back mid-run. Check whether the source type change was intended.
- **Governing the auto-apply**: these mutations apply on Rocky's own authority by default. Set `auto_apply_additive_drift = true` under `[resilience]` in `rocky.toml` to route each one through the policy plane instead. Rocky then applies only a provably additive change that a `[policy]` rule allows, and refuses the rest with a require-review failure.

Rocky corrects drift inside the run rather than waiting for you. Your job is to read what it did, then check the consumers downstream.

---

## 4. Quality check failures

**Definition.** An [inline data quality check](../../concepts/data-quality-checks) declared under `[pipeline.<name>.checks]` in `rocky.toml` failed against the data Rocky just materialised. Checks run after each model materialises.

An error-severity check failure fails the run by default, because `fail_on_error = true`. A warning-severity check is advisory. It appears in the run output and in Dagster Pipes events, and it does not fail the run.

**Detection signal.** `RunOutput.check_results[]` holds one `TableCheckOutput` per asset, with an `asset_key` and a `checks[]` array. Each entry in `checks[]` is a `CheckResult` with `name`, `passed: bool`, and `severity` (`error` | `warning`). Each entry also carries detail fields for its check type, such as `source_count` / `target_count`, `failing_rows`, or `null_rate`.

**Recovery playbook.**

1. Find the failing check with `rocky run --output json | jq '.check_results[].checks[] | select(.passed == false)'`. On `rocky apply` output the run payload is nested under `.result`. The detail fields let you reproduce the failure in the warehouse.
2. Decide whether the failure is a data issue or a check-definition issue.
   - **Data issue**: the upstream data broke an expectation the check was right to enforce. Triage upstream, replay or backfill the offending [partition](/reference/glossary/#partition), then re-run.
   - **Check-definition issue**: the check is stricter than reality should be. Adjust its threshold or predicate in `rocky.toml`, then re-run.
3. Set `severity = "warning"` on a check that should inform rather than gate. It then lands in the output as a warning and leaves the run status alone.

**Gating vs. advisory checks.** Error-severity failures fail the run by default, through `fail_on_error = true` under `[pipeline.<name>.checks]`. Set `fail_on_error = false` to stop gating on any check. Set `severity = "warning"` to downgrade one check at a time.

---

## 5. Adapter / runtime failures

**Definition.** A warehouse call failed at execution time, after compile, contracts, and drift all passed. This category covers network errors, auth errors, quota errors, statement timeouts, and deadlocks. In short: anything that starts inside the [adapter](/reference/glossary/#adapter) rather than the engine.

**Detection signal.** A non-zero `rocky apply` exit code, plus one entry on `RunOutput.errors[*]` per failed table. Each entry carries a typed [`failure_kind`](/advanced/per-table-error-containment/#failure_kind-taxonomy) discriminator that an orchestrator can branch on, and the underlying error carries a transient / rate-limit classification. The other tables in the same run keep going. See [Per-table error containment](/advanced/per-table-error-containment/).

The dispatched adapter classifies its own failures:

| Adapter | Common failure modes |
|---|---|
| Databricks | `401 Unauthorized` (PAT expired / OAuth M2M misconfigured), statement timeout, rate-limit on `information_schema` queries |
| Snowflake | Auth chain rejection (OAuth → JWT → password), warehouse suspended, query result-size cap |
| BigQuery | Quota exceeded, auth scope mismatch, BIGNUMERIC type drift |
| DuckDB | File lock contention, out-of-memory on large CTAS |
| Fivetran / Airbyte | `403 Forbidden` (missing API scope), connector currently syncing |

### Classified retry

Since engine 1.58.0 the run loop retries proven-transient failures itself. This is on by default.

When a model's materialization fails, the adapter's own retryable judgement classifies it as `Transient`, `Permanent`, or `Unknown`. Rocky re-runs only a proven transient failure, with capped exponential backoff. A 429, a connection reset, a warehouse warming up, and a lock conflict all qualify.

Rocky never retries a `Permanent` or `Unknown` failure. It also never retries an auth error, even when an adapter labels it transient, because expired credentials do not heal on a second attempt. Every retry is recorded as an attempt trail on the execution record and shows up in the run's JSON output.

```toml
[resilience]
transient_max_retries = 2   # default; at most three attempts per model. 0 opts out.
```

A run-loop circuit breaker backs this up. After several consecutive transient model failures (default 3), Rocky retries no further model for the rest of that run. A systemically unhealthy warehouse then fails fast instead of spending the retry budget across the whole DAG.

Set `transient_max_retries = 0`, or `[resilience] enabled = false`, to restore the earlier single-attempt behaviour. That suits CI, where a fast failure beats a slow one.

There is a consequence for orchestrators. By the time a `failure_kind: "transient"` entry reaches your `errors[*]`, the engine has already retried it inside the run. Retrying immediately from outside duplicates work. Prefer a delayed re-run, or `--resume-latest`.

**Recovery playbook.**

1. Run `rocky doctor --output json` first. Its `checks[]` entry named `"adapters"` reports `status: "healthy" | "warning" | "critical"`. It tells you which adapter Rocky expects to work and which one it cannot currently reach. Treat doctor as a credentials and connectivity smoke test.
2. For a **transient** failure, entries with `failure_kind: "transient"` or `"connection-failed"` on `errors[*]`, note that the engine already retried it in-run (see [Classified retry](#classified-retry)). A failure that still surfaced has exhausted its retry budget. Once the underlying condition clears, run `rocky plan --resume-latest && rocky apply <plan-id>` to pick up where the run stopped. The single-step `rocky run --resume-latest` alias does the same thing.
3. For an **auth** failure, walk the adapter's auth chain, for example Snowflake's OAuth → JWT → password order. Check the env vars and the `rocky.toml` config against the [authentication guide](../../reference/authentication), which has a per-adapter checklist.
4. For a **quota** failure, check the warehouse's own quota dashboard. Rocky's adaptive concurrency (the Databricks AIMD throttle) backs off automatically, but a hard quota reset happens warehouse-side.
5. For a **statement timeout**, raise `timeout_secs` on the adapter. Better, ask whether the model's [materialization strategy](/reference/glossary/#materialization-strategy) is right. A multi-hour `FullRefresh` is often a missed `Merge` or `Incremental`; `rocky optimize` surfaces the recommendation.

---

## Failure containment across the model graph

By default a transformation run **fails fast**. The first model that fails stops the run, and Rocky skips every model it has not yet built. Turn on containment to let unrelated work continue:

```toml
[resilience]
contain_failures = true   # default: false
```

With containment on, Rocky withholds the failed model and its whole downstream closure. Unrelated subtrees still materialize. The run reports `PartialFailure`. It lists the withheld models on `RunOutput.contained[*]`, each naming what blocked it plus an unblock hint, and the causes on `RunOutput.errors[*]`. For a partitioned (`time_interval`) model, a failed partition withholds the downstream while the healthy partitions still land.

**Guarantee scope.** Containment is *guaranteed* for two kinds of dependency. The first is a dependency declared with `ref()`. The second is a physical read Rocky can resolve statically: `schema.table` or `catalog.schema.table`, quoted or unquoted.

Rocky folds both kinds into the withholding closure and into the execution order. So a downstream model is never built on the stale or missing output of a failure. Under `--parallel`, a reader is scheduled strictly after every producer it reads.

Some reads Rocky **cannot enumerate**: a model built on a CTE, a sub-query, or a set operation. Those get best-effort handling, identical to a normal fail-fast run.

Such a model is still withheld when a *known* upstream of it failed. But its reads do not resolve into an ordering edge. So under `--parallel`, a same-layer reader of a failing producer can materialize on stale data, exactly as it would in a fail-fast run.

This is a documented boundary, not a regression. Containment never materializes anything a fail-fast run would not. **Declare the dependency with `ref()` when you need a hard containment guarantee.**

Containment is off by default. The fail-fast behaviour described elsewhere on this page is unchanged unless you set `contain_failures = true`.

---

## 6. State store failures

**Definition.** The [state store](/reference/glossary/#state-store) is Rocky's embedded redb database, at `<models>/.rocky-state.redb` by default. It holds [watermarks](/reference/glossary/#watermark), run history, branch state, and partition progress.

A failure here does one of two things. It stops the run from starting, through lock contention or corruption. Or it quietly degrades an incremental run into a full refresh, because a watermark is missing.

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| `state file locked` | `rocky apply` aborts immediately; the `state` check in `rocky doctor` reports `critical` |
| `state file corrupted` | the `state` check in `rocky doctor` reports `critical`; the structured error names the corrupted table |
| Missing watermark | `rocky state --output json` shows `watermarks: []` for a model that should have one; the next run becomes a `FullRefresh` |
| `state_sync` upload failure | the `state_sync` check in `rocky doctor` reports `critical`; the local state still works but the remote backup is stale |

**Recovery playbook.**

1. **Locked.** Run `ps aux | grep rocky` to find the process holding the lock. Kill the second invocation if two runs really are concurrent. If the lock is stale after a crash, delete it with `rm <models>/.rocky-state.redb.lock`. The file extension can vary by redb version, and `rocky doctor` names the actual file.
2. **Corrupted.** Restore `.rocky-state.redb` from your `state_sync` backend backup. Without a backup, run `rm <models>/.rocky-state.redb` and accept that the next run full-refreshes every incremental model.
3. **Missing watermark.** Accept one full refresh, after which the next incremental run reseeds the watermark from the materialised data. Or restore `.rocky-state.redb` from a `state_sync` backup taken after the last known-good run.
4. **`state_sync` failed.** The local state is fine. Check the backend credentials (S3, Valkey) and re-run when they work. The state syncs on the next successful run.

**Why these failures are rare but expensive.** A corrupted state file is almost never a Rocky bug. It is usually a full disk or a process killed mid-write. The blast radius is large because every incremental model degrades to a full refresh until you restore the state. Wire up `state_sync` on any production deployment.

---

## 7. Hook failures

**Definition.** A pipeline lifecycle hook failed. Hooks fire on events such as `on_pipeline_start`, `on_pipeline_complete`, `on_after_model_run`, and `on_model_error`. A hook can be a shell command, a webhook, or a templated payload.

**Detection signal.** Hook failures have no dedicated field on the run output. They appear in the run's stderr and logs. Reproduce one in isolation with `rocky hooks test <event> --output json`, whose `status` is `no_hooks`, `continue`, or `abort`. The hook's `on_failure` setting decides whether a failure aborts the run (`abort`) or continues with a warning (`warn`).

**Recovery playbook.**

1. Reproduce the hook with `rocky hooks test <event> --output json`. This fires it in isolation against a dummy event payload.
2. If the hook is a **shell command** that exits non-zero, fix the script. Check its env-var assumptions too: hooks inherit the run's environment, not your shell's.
3. If the hook is a **webhook**, read the receiver's logs for the real rejection. Rocky surfaces only the HTTP status, and the receiver's body usually holds the actionable message.
4. If the hook is **flaky**, from a network blip or a third-party rate limit, set `on_failure = "warn"`. The transient failure then stops gating the run, and the run's stderr and logs still flag it in your observability stack.

Hook failures look like runtime failures. The fix is in your hook script or webhook receiver, not in the pipeline.

---

## 8. Cost / budget violations

**Definition.** A model cost more to run than the `[budget]` block in its sidecar `.toml` allows. Or the project-level cost projection flagged a pull request as over budget against the base ref.

**Detection signal.** Two signals, one after the run and one before it. After a run, `RunOutput.budget_breaches[]` holds a `BudgetBreachOutput` per breach, with `actual`, `limit`, and `limit_type`. Before a run, the `rocky preview cost` output's `summary.delta_usd` compares the branch with the base. Setting `on_breach = "error"` in the `[budget]` block turns a per-model breach from a warning into a non-zero `rocky apply` exit.

**Recovery playbook.**

1. Run `rocky cost --output json` for the current projection, broken down per model.
2. For an **expected** violation, where the model deliberately got more expensive or you are backfilling a wider date range, raise `[budget].max_usd` in the model's sidecar.
3. For an **unexpected** violation, where the cost spiked without a known cause, check three things:
   - Did the `MaterializationStrategy` change recently, for example `Merge` → `FullRefresh`? `rocky optimize --output json` recommends a cheaper strategy when one fits.
   - Did the upstream row count grow? `rocky history --model <name>` shows the row-count history.
   - Is the SQL doing a cross-join or another antipattern? Run `rocky lineage <model> --column <name>`. It traces that column back through the upstream columns feeding it, and labels each edge with the transform. It reports no cost of its own, so you read the path and judge which join is doing the work.
4. For PR-time violations, the [`rocky-preview` GitHub Action](../../guides/preview-a-pr) renders the cost delta in the PR comment, so reviewers see it before merge.

**Why budgets warn by default.** Cost is a signal, not a gate, until you have calibrated budgets against real usage. Switch to `on_breach = "error"` once your `[budget]` blocks reflect reality. Until then, the `rocky cost` warning on every PR is the calibration loop.

---

## 9. Governance failures

**Definition.** Something in Rocky's [governance layer](../../guides/governance) did not apply cleanly. That layer covers permissions, classification, masking, and retention. Three cases are typical. The warehouse rejected a permission diff. A mask classification did not resolve to a strategy. A retention sweep could not acquire its target.

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| Permission grant rejected | rejection text in `rocky apply` stderr / `errors[]`; `RunOutput.permissions` (`PermissionSummary`) summarises what did apply |
| Unresolved classification | `W004` warning at compile time per `(model, column, tag)` triple |
| Mask resolution mismatch | `rocky plan --env <name>` `mask_actions` shows the resolved strategy is `None` for a column that shouldn't be unmasked |
| Retention drift / sweep failure | `rocky retention-status --output json` shows per-model `in_sync: false`; `rocky state retention sweep` sweeps the state store's history tables |

**Recovery playbook.**

1. **Permission rejected.** The cause is usually a missing principal: a group or user the warehouse does not know. It can also be a missing parent grant, such as `USE CATALOG` before `USE SCHEMA`. The warehouse's own error text sits verbatim in the diff entry. Act on it directly.
2. **Unresolved classification (`W004`).** Add the tag to a `[mask]` or `[mask.<env>]` block in `rocky.toml`. Or list it under `[classifications.allow_unmasked]` to opt out on the record. Rocky denies the implicit-allow path by design, so it surfaces the unresolved tag rather than leaking the column.
3. **Mask mismatch.** Re-run `rocky plan --env <env>` to preview what Rocky would apply. The active env's `[mask.<env>]` block overrides the workspace `[mask]` defaults. If the override does not take effect, check the env name spelling and the inheritance order in the [governance guide](../../guides/governance).
4. **Retention sweep failure.** The cause is usually a missing partition column or a permissions problem on the target. Run `rocky doctor --output json` to confirm the adapter holds the right grants on the target schema.

Permissions and masking apply *after* a successful materialisation. So a governance failure means the data landed but is not fully wired into your access model. Recovery is rarely urgent, but it must close before your next compliance audit.

---

## See also

- [Per-table error containment](/advanced/per-table-error-containment/): how the run path isolates failures at the table boundary and how to consume the `failure_kind` discriminator
- [Troubleshooting](/advanced/troubleshooting/): symptom-first lookup ("I got error X")
- [`rocky doctor`](../../reference/cli#doctor): aggregate health check across config, state, adapters, pipelines
- [`rocky plan --resume-latest`](../../reference/cli#run): resume a failed run from its checkpoint (canonical, auditable form; the single-step `rocky run --resume-latest` alias does the same in one invocation)
- [Schema drift](../../concepts/schema-drift): graduated drift handling deep dive
- [Data quality checks](../../concepts/data-quality-checks): inline check authoring + result shape
- [Governance guide](../../guides/governance): permissions, classification, masking, retention
