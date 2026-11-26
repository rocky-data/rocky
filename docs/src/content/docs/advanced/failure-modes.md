---title: Failure modes
description: The nine ways a Rocky pipeline fails, and the recovery steps for each one
sidebar:
  order: 2
---

A Rocky pipeline fails in one of nine ways. Each section gives the **detection signal** (the CLI, JSON or Dagster fixture output) and the **recovery playbook** (the steps back to green).

Use this page when you know the category. Use [Troubleshooting](/advanced/troubleshooting/) when all you have is an error message. The categories are independent: one pipeline can hit several at once.

## Find your category from the signal

```
 WHAT YOU CAN SEE                     CATEGORY            START WITH
 ─────────────────────────────────    ────────────────    ─────────────────

 Rocky never called the warehouse
   ├─ E010–E014, E062, E063           2. Contracts        the contract file
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

Two entries on `errors[*]` cross the branches. A `failure_kind` of `"compile-error"` is a compile failure caught during the run ([section 1](#1-compile-time-failures)). A governance failure classifies as `failure_kind: "unknown"`, so check `permissions` before you treat it as an adapter failure ([section 9](#9-governance-failures)).

Entries on `contained[*]` are not failures. They name models Rocky withheld after an upstream failure. See [Failure containment across the model graph](#failure-containment-across-the-model-graph).

---

## 1. Compile-time failures

**Definition.** `rocky compile` catches the failure before Rocky calls the warehouse. `rocky ci` wraps compile and catches the same set. No credentials and no warehouse credits are needed. Every finding is a [diagnostic](/reference/glossary/#diagnostic-code): a stable code, a severity and a source span.

**Detection signal.** A `Diagnostic` with `severity: Error` in `diagnostics` on `CompileOutput` / `CiOutput`:

| Code | Failure |
|---|---|
| `E001` | Join key type mismatch between two upstream models, with no common type |
| `E020`-`E026` | `time_interval` model misconfiguration (placeholders, granularity, nullability) |
| `E027` | Model's projected cost exceeds its `[budget]` ceiling |
| `E028` | Unresolved `@var` reference |

The compiler also emits `E010`-`E014`, `E029`-`E061`, `E062`, `E063`, `E064`, `E065`-`E068`, `E071` and `E073`. [Diagnostic codes](/concepts/compiler/#diagnostic-codes) lists them all. `E010`-`E014`, `E062` and `E063` have their own section because the fix is contract-shaped.

**Recovery playbook.**

1. Run `rocky compile --output table`. It underlines the source span.
2. In VS Code ([extension](/guides/ide-setup/)) the LSP adds hover detail and a `Quick Fix` where one exists. `E010` / `E013` and `E001` (a `CAST` wrap) ship deterministic fixes. Other codes may offer an AI fix when `ANTHROPIC_API_KEY` is set.
3. Fix the model SQL, or the upstream contract.
4. Re-run `rocky compile` until it is clean.

`rocky apply` aborts before any warehouse work when the project has compile errors.

**Per-model compile failure during a run.** A model can compile in isolation and fail when its turn comes, for example after an upstream type shift. Rocky contains it at the table boundary: it counts toward `tables_failed` and gets an `errors[*]` entry with [`failure_kind: "compile-error"`](/advanced/per-table-error-containment/#failure_kind-taxonomy) carrying the diagnostic. The run exits non-zero with status `Failure`, or `PartialFailure` when other models succeeded.

---

## 2. Contract violations

**Definition.** A model's output schema does not match its contract (`<model>.contract.toml`: required and protected columns, types, nullability). Rocky checks it at compile time.

**Detection signal.**

| Code | Meaning |
|---|---|
| `E010` | Required column missing from model output |
| `E011` | Column type mismatch (contract vs model output) |
| `E012` | Nullability violation (contract says non-nullable, model says nullable) |
| `E013` | Protected column removed |
| `E014` | A nullable column the contract does not declare, under `[rules] no_new_nullable` |
| `E062` | The model output repeats a name the contract constrains |
| `E063` | An output column the contract does not declare, or a column list Rocky cannot prove, under `[rules] no_extra_columns` |

All are `Error` severity.

**Recovery playbook.**

1. Open the model named in the diagnostic. The message names the column.
2. `E010` / `E013`: take the LSP code action. It offers a deterministic `Add` / `Restore` fix when an upstream model exposes the column. It falls back to an AI rewrite (needs `ANTHROPIC_API_KEY`) when the column needs derivation, the SQL is multi-statement, or the model uses `SELECT *`.
3. `E011` / `E012`: change the model SQL to produce the contracted type or nullability (the common case). Change the contract only when the schema change is intentional and consumers have migrated.
   For `E012` on a column the SQL cannot prove non-null (kept non-null only by `WHERE col IS NOT NULL`), set `nullable = true` and add a `not_null` test in the sidecar. It runs under `rocky run --tests enforce` or `[run] tests_on_run = "enforce"`, and `rocky plan promote` refuses a branch when the model's newest `rocky run --branch` evidence is that failure. See [When Rocky cannot prove a column is never NULL](/concepts/data-quality-checks/#when-rocky-cannot-prove-a-column-is-never-null).
4. Re-run `rocky compile`.

---

## 3. Schema drift

**Definition.** [Drift](/reference/glossary/#drift) is a mismatch between the source schema and the target table. [Graduated drift handling](/concepts/schema-drift/) fixes it in place: `ALTER COLUMN TYPE` for a safe widening, `ALTER TABLE ADD COLUMN` for a new column. It drops and recreates the table only when it cannot do either.

**Detection signal.** The `drift` block (`DriftSummary`) on `rocky run` / `rocky apply --output json`: `tables_checked`, `tables_drifted`, and one `actions_taken[]` entry (`table`, `action`, `reason`) per table Rocky changed:

| `action` | Meaning |
|---|---|
| `add_columns` | The source has columns the target lacked, and no type drifted. Rocky ran `ALTER TABLE ADD COLUMN` for each. |
| `alter_column_types` | Every changed type was a safe widening (for example INT to BIGINT). Rocky ran `ALTER` on each, plus any new columns. |
| `drop_and_recreate` | A type change was not a safe widening. Rocky dropped the target and rebuilt it with a full refresh. |

The entry records what already happened, not a plan. Rocky records it after the table's copy succeeds. If the rebuild after a `drop_and_recreate` fails, the entry is lost although the drop ran, and a retry (`table_retries`) can then report the table as materialized with no sign of the drop. A missing entry does not prove the target was untouched. `reason` names the columns, one phrase each. Rocky does not detect a column that disappeared from the source.

**Recovery playbook.**

- **`add_columns`**: the new columns are nullable, so historical rows hold NULL. Check downstream models that read them.
- **`alter_column_types`**: check that downstream tables, views and dashboards parse the wider type.
- **`drop_and_recreate`**: consumers saw the table disappear mid-run. Check whether the source type change was intended.
- **Governing the auto-apply**: by default these apply on Rocky's own authority. Set `auto_apply_additive_drift = true` under `[resilience]` to route each through the policy plane. Rocky then applies only a provably additive change that policy resolves to `allow` (a matching rule or `default_agent_effect`) and refuses the rest with a require-review failure.

---

## 4. Quality check failures

**Definition.** An [inline check](/concepts/data-quality-checks/) under `[pipeline.<name>.checks]` failed against the data Rocky just materialised. An error-severity failure fails the run (`fail_on_error = true` by default). A warning-severity check is advisory: it appears in the output and in Dagster Pipes events and leaves the run status alone.

**Detection signal.** `RunOutput.check_results[]` holds one `TableCheckOutput` per asset (`asset_key`, `checks[]`). Each `CheckResult` has `name`, `passed: bool`, `severity` (`error` | `warning`) and type-specific detail such as `source_count` / `target_count`, `failing_rows` or `null_rate`.

**Recovery playbook.**

1. Find the failing check: `rocky run --output json | jq '.check_results[].checks[] | select(.passed == false)'`. On `rocky apply` output the run payload is under `.result`.
2. Decide the cause:
   - **Data issue**: triage upstream, replay or backfill the offending [partition](/reference/glossary/#partition), re-run.
   - **Check-definition issue**: adjust the threshold or predicate in `rocky.toml`, re-run.
3. Set `severity = "warning"` on one check to inform rather than gate. Set `fail_on_error = false` to stop gating on every check.

---

## 5. Adapter / runtime failures

**Definition.** A warehouse call failed at execution time, after compile, contracts and drift passed: network, auth, quota, statement timeout, deadlock. Anything that starts inside the [adapter](/reference/glossary/#adapter).

**Detection signal.** A non-zero `rocky apply` exit plus one `RunOutput.errors[*]` entry per failed table, with a typed [`failure_kind`](/advanced/per-table-error-containment/#failure_kind-taxonomy). The other tables keep going. See [Per-table error containment](/advanced/per-table-error-containment/). Typical causes: Databricks `401 Unauthorized` (expired PAT, misconfigured OAuth M2M), Snowflake auth rejection or a suspended warehouse, BigQuery quota or scope mismatch, DuckDB lock contention or out-of-memory, Fivetran / Airbyte `403 Forbidden` (missing API scope) or a connector mid-sync.

### Classified retry

The run loop retries proven-transient failures itself, on by default. The adapter classifies a failed materialization as `Transient`, `Permanent` or `Unknown`. Rocky re-runs only a `Transient` one (a 429, connection reset, warming warehouse, lock conflict), with capped exponential backoff. It never retries `Permanent` or `Unknown`, and never an auth error, even if an adapter labels it transient. Each retry is recorded as an attempt trail in the JSON output.

```toml
[resilience]
transient_max_retries = 2   # default; at most three attempts per model. 0 opts out.
```

A run-loop circuit breaker stops retries for the rest of the run after several consecutive transient model failures (default 3). Set `transient_max_retries = 0`, or `[resilience] enabled = false`, for single attempts, which suits CI.

A `failure_kind: "transient"` entry in `errors[*]` has already been retried in the run. Retrying at once from outside duplicates work. Prefer a delayed re-run or `--resume-latest`.

**Recovery playbook.**

1. Run `rocky doctor --output json`. Its `"adapters"` check reports `healthy`, `warning` or `critical`. Use it as a credentials and connectivity smoke test.
2. **Transient** (`failure_kind: "transient"` or `"connection-failed"`): the retry budget is exhausted. When the condition clears, run `rocky plan --resume-latest && rocky apply <plan-id>`. The single-step `rocky run --resume-latest` does the same.
3. **Auth**: walk the adapter's auth order and check the env vars and `rocky.toml` against [Authentication](/reference/authentication/).
4. **Quota**: check the warehouse's quota dashboard. Adaptive concurrency (AIMD: add one slot at a time, halve on a rate limit) backs off by itself, but a hard quota reset happens warehouse-side.
5. **Statement timeout**: raise `timeout_secs` on the adapter. Better, check the [materialization strategy](/reference/glossary/#materialization-strategy). A multi-hour `FullRefresh` is often a missed `Merge` or `Incremental`; `rocky optimize` recommends one.

---

## Failure containment across the model graph

By default a transformation run **contains failures** (`[resilience] contain_failures = true`). A model that fails is withheld with its downstream closure. Unrelated branches still build, and the run reports `PartialFailure`. Set `contain_failures = false` to **fail fast**: a failing model stops the run and no later layer starts. Models already running beside it finish, because `--parallel` is 4 by default. Only DuckDB runs one model at a time.

```toml
[resilience]
contain_failures = false   # default: true
```

```
model has compile error    ->  model fails; declared descendants are withheld
project does not compile   ->  nothing builds (one error keyed <compile>)
model fails while running  ->  it and its downstream are withheld; the rest builds
                               (contain_failures = false: no later layer starts)
```

**Compile errors.** A selected model with an `Error` diagnostic records `failure_kind: "compile-error"`. Rocky withholds its declared DAG descendants and lists them in `contained[*]`. Healthy branches build. Targets of failed or withheld models stay unchanged. If `--model` selects only a withheld descendant, the run records `compile-error` and `tables_failed`, so it cannot report `Success`. This containment is always on and follows declared edges: add `depends_on` when an undeclared read must be withheld. `rocky run --dag` runs each model as a separate sub-run.

A project that cannot compile builds nothing. Causes: SQL that does not parse, a `.rocky` file Rocky cannot lower, broken or missing frontmatter, a duplicate model name, or a `depends_on` that names a missing model.

**Runtime failures.** `RunOutput.contained[*]` lists each withheld model with what blocked it and an unblock hint. `RunOutput.errors[*]` lists the causes. For a `time_interval` model, a failed partition withholds the downstream while healthy partitions land.

This is the model graph only. Replicated tables use [`[execution] fail_fast`](/reference/configuration/#pipelinenameexecution) (default `false`). `error_rate_abort_pct` (default 50) aborts the rest once 4 or more tables finish at that failure rate.

**Guarantee scope.** Containment holds for a `depends_on` entry and for a physical read Rocky can resolve statically (`schema.table` or `catalog.schema.table`, quoted or not). Rocky folds both into the withholding closure and the execution order, so under `--parallel` a reader runs after every producer it reads.

Rocky cannot enumerate reads in a CTE, sub-query or set operation. Such a model is still withheld when a known upstream failed, but its reads add no ordering edge. Under `--parallel`, a same-layer reader of a failing producer can then materialize on stale data, as in a fail-fast run. **List the dependency in `depends_on` when you need a hard guarantee.**

---

## 6. State store failures

**Definition.** The [state store](/reference/glossary/#state-store) is Rocky's embedded redb database, at `<models>/.rocky-state.redb` by default. It holds [watermarks](/reference/glossary/#watermark), run history, branch state and partition progress. A failure stops the run (lock contention, corruption) or quietly degrades an incremental run into a full refresh (missing watermark).

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| `state file locked` | `rocky apply` aborts; the `state` check in `rocky doctor` is `critical` |
| `state file corrupted` | the `state` check is `critical`; the structured error names the corrupted table |
| Missing watermark | `rocky state --output json` shows `watermarks: []` for a model that should have one; the next run is a `FullRefresh` |
| `state_sync` upload failure | the `state_sync` check is `critical`; local state works but the remote backup is stale |

**Recovery playbook.**

1. **Locked.** Find the holder with `ps aux | grep rocky`. Kill the second invocation if two runs are concurrent. If the lock is stale after a crash, run `rm <models>/.rocky-state.redb.lock`. The extension can vary by redb version: `rocky doctor` names the file.
2. **Corrupted.** Restore `.rocky-state.redb` from your `state_sync` backup. Without one, run `rm <models>/.rocky-state.redb`: the next run full-refreshes every incremental model.
3. **Missing watermark.** Accept one full refresh, after which the next incremental run reseeds the watermark. Or restore from a `state_sync` backup taken after the last good run.
4. **`state_sync` failed.** Local state is fine. Fix the backend credentials (S3, Valkey). State syncs on the next successful run.

Corruption is usually a full disk or a process killed mid-write, not a Rocky bug. The blast radius is large: every incremental model degrades to a full refresh. Wire up `state_sync` on any production deployment.

---

## 7. Hook failures

**Definition.** A lifecycle hook failed (`on_pipeline_start`, `on_pipeline_complete`, `on_after_model_run`, `on_model_error`, ...). A hook is a shell command, a webhook or a templated payload.

**Detection signal.** No dedicated output field: look in the run's stderr and logs. `rocky hooks test <event> --output json` reproduces one, with `status` of `no_hooks`, `continue` or `abort`. Only `rocky hooks test` acts on `on_failure = "abort"`. `rocky run` ignores every hook outcome and continues.

**Recovery playbook.**

1. Reproduce with `rocky hooks test <event> --output json`. It fires the hook against a dummy payload.
2. **Shell command** exits non-zero: fix the script and its env-var assumptions. Hooks inherit the run's environment, not your shell's.
3. **Webhook**: read the receiver's logs. Rocky surfaces only the HTTP status.
4. **Flaky** hook (network blip, third-party rate limit): set `on_failure = "warn"`. Stderr and logs still flag it.

---

## 8. Cost / budget violations

**Definition.** A run cost, ran or scanned more than the project [`[budget]`](/reference/configuration/#budget) allows, or a model's projected cost is over its sidecar `[budget]` (`E027` at compile time), or the cost projection flagged a pull request as over budget against the base ref.

**Detection signal.** After a run, `RunOutput.budget_breaches[]` holds a `BudgetBreachOutput` per breach (`actual`, `limit`, `limit_type`). Before a run, `rocky preview cost` reports `summary.delta_usd` against the base. `on_breach = "error"` in `[budget]` turns a breach from a warning into a non-zero `rocky apply` exit.

**Recovery playbook.**

1. Run `rocky cost --output json` for the per-model projection.
2. **Expected** violation (a deliberate cost increase, a wider backfill): raise `max_usd` in the project `[budget]`. For `E027`, raise it in the model's sidecar `[budget]`.
3. **Unexpected** spike, check:
   - A recent `MaterializationStrategy` change, such as `Merge` to `FullRefresh`. `rocky optimize --output json` recommends a cheaper one.
   - Upstream row growth: `rocky history --model <name>`.
   - A cross-join or other antipattern: `rocky lineage <model> --column <name>` traces the column back through its upstream columns, edge by edge. It reports no cost: you read the path.
4. For PR-time violations, the [`rocky-preview` GitHub Action](/guides/preview-a-pr/) renders the cost delta in the PR comment.

Budgets warn by default, because cost is a signal until you calibrate against real usage. Switch to `on_breach = "error"` once `[budget]` reflects reality.

---

## 9. Governance failures

**Definition.** Something in the [governance layer](/guides/governance/) (permissions, classification, masking, retention) did not apply cleanly: the warehouse rejected a permission diff, a mask classification did not resolve to a strategy, or a retention sweep could not acquire its target.

**Detection signal.**

| Symptom | Where it surfaces |
|---|---|
| Permission grant rejected | rejection text in `rocky apply` stderr / `errors[]`; `RunOutput.permissions` (`PermissionSummary`) summarises what applied |
| Unresolved classification | `W004` warning at compile time per `(model, column, tag)` |
| Mask resolution mismatch | `rocky plan --env <name>` `mask_actions` shows strategy `None` for a column that should be masked |
| Retention drift / sweep failure | `rocky retention-status --output json` shows per-model `in_sync: false`; `rocky state retention sweep` sweeps the state store's history tables |

**Recovery playbook.**

1. **Permission rejected.** Usually a principal the warehouse does not know, or a missing parent grant (`USE CATALOG` before `USE SCHEMA`). The warehouse's error text is verbatim in the diff entry.
2. **Unresolved classification (`W004`).** Add the tag to a `[mask]` or `[mask.<env>]` block, or list it in `[classifications] allow_unmasked` to opt out on the record. Rocky denies the implicit-allow path by design.
3. **Mask mismatch.** Re-run `rocky plan --env <env>`. The active env's `[mask.<env>]` overrides the workspace `[mask]`. If it does not take effect, check the env name and the inheritance order in the [governance guide](/guides/governance/).
4. **Retention sweep failure.** Usually a missing partition column or a permissions problem on the target. Run `rocky doctor --output json` to check the grants.

Permissions and masking apply after a successful materialisation, so a governance failure means the data landed but is not fully wired into your access model.

---

## See also

- [Per-table error containment](/advanced/per-table-error-containment/): the `failure_kind` discriminator.
- [Troubleshooting](/advanced/troubleshooting/): symptom-first lookup.
- [`rocky doctor`](/reference/commands/development/#rocky-doctor): aggregate health check.
- [`rocky plan --resume-latest`](/reference/commands/core-pipeline/#rocky-plan): resume a failed run from its checkpoint.
- [`rocky run --retry-failed`](/reference/commands/core-pipeline/#retrying-a-failed-run): rerun the failed and never-built models of a transformation run. `--resume` is for replication runs only.
- [Schema drift](/concepts/schema-drift/), [Data quality checks](/concepts/data-quality-checks/), [Governance guide](/guides/governance/).
