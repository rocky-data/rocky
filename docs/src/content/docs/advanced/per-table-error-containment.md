---
title: Per-table error containment
description: How rocky run keeps one failed table from killing the whole run, and how to branch on failure_kind
sidebar:
  order: 3
---

`rocky run` (and the `rocky plan` plus `rocky apply` flow behind it) treats each table as its own unit of work. One table failing does not crash the run. The other tables keep going, the run exits with the partial-success code, and every failure lands on `RunOutput.errors[*]` with a typed `failure_kind`. An orchestrator can branch on the kind instead of parsing the `error` string.

## How the run loop contains a failure

The loop in `commands/run.rs` dispatches each table through `process_table` in a `tokio::JoinSet`. Each finished task takes one of three branches:

```
                 one table  ──►  process_table  ──►  task Result
                                                          │
       ┌──────────────────────────┬────────────────────────┘
       ▼                          ▼                        ▼
   success                 Ok((idx, Err(e)))        Err(JoinError)
   the write landed        the adapter failed       the task panicked
       │                          │                        │
       ▼                          ▼                        ▼
   append to               classify into a          capture the panic
   materializations        FailureKind FIRST,       message as a
   + checkpoint            then push a              TableError with
   progress                TableError               failure_kind
                                  │                 "unknown"
                                  │                        │
       └──────────────────────────┴────────────────────────┘
                                  │
                                  ▼
                        keep going with the next table
                                  │
              ┌───────────────────┴───────────────────┐
              │ unless an abort path fires:           │
              │   fail_fast = true       (first error)│
              │   error_rate_abort_pct   (too many)   │
              │ both call JoinSet::abort_all()        │
              └───────────────────────────────────────┘
```

Rocky classifies the adapter error into a [`FailureKind`](#failure_kind-taxonomy) *before* stringifying it, which keeps the typed connector variant. The `TableError` holds `asset_key`, `error` and `failure_kind`. The loop catches `anyhow::Error` from any source (connector, drift, governance, panic) on every adapter.

Two `[execution]` keys abort the run early. Neither is a CLI flag.

- `fail_fast = true` (default `false`) aborts on the first error.
- `error_rate_abort_pct` (default `50`, `0` disables it) aborts once the share of failed tables reaches that percentage, checked after at least 4 tables complete.

Without an abort, the run finishes and exits non-zero, and the JSON stays well-formed.

### What `errors[*]` looks like

```json
{
  "version": "1.34.0",
  "command": "run",
  "tables_copied": 18,
  "tables_failed": 2,
  "materializations": [ /* 18 successful entries */ ],
  "errors": [
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "orders"],
      "error": "Databricks statement failed: TABLE_OR_VIEW_NOT_FOUND ...",
      "failure_kind": "query-rejected"
    },
    {
      "asset_key": ["fivetran", "acme", "us_west", "shopify", "refunds"],
      "error": "Databricks API error 429: rate limit exceeded",
      "failure_kind": "quota-exceeded"
    }
  ]
}
```

## `failure_kind` taxonomy

`failure_kind` is a coarse classifier over the failure surface. Most variants partition the connector error spaces for Databricks, Snowflake, BigQuery and Trino. `compile-error` is the exception: it covers a model that fails to compile mid-run, which is not a connector failure at all. `unknown` is the fallback for a failure that reaches the output layer type-erased.

| Variant | Meaning | Retry-safe? |
|---|---|---|
| `connection-failed` | TCP / TLS / DNS / connection-establishment failure -- the request never reached the warehouse. | Yes, with backoff. |
| `auth-failed` | Credentials rejected or token expired -- 401, 403, or an auth-chain rejection. | No. Fix credentials before retrying. |
| `query-rejected` | Warehouse parsed and rejected the SQL -- syntax error, missing column, missing permission, semantic analysis failure. | No. The SQL needs fixing. |
| `transient` | Retry-worthy failure -- 5xx, network glitch, statement aborted by a transient warehouse condition, a transport timeout, a statement timeout on PostgreSQL. | Yes. |
| `quota-exceeded` | Rate limit hit or a configured cap reached -- 429, retry-budget exhaustion, account-level quota. | Yes, with extended backoff and an alert. |
| `not-found` | Requested catalog / schema / table not present -- 404 from the warehouse, often an upstream rename. | No. Re-discovery or human triage needed. |
| `compile-error` | The selected model has an `Error`-severity compile diagnostic, or Rocky withheld it after an upstream compile error. Rocky makes no warehouse call for it. Its declared descendants are withheld and listed in `contained[]`. Healthy branches can still run. Targets of failed or withheld models stay unchanged. The diagnostic or block reason is in `error`. | No. Fix the model SQL or its upstream; re-running without a change will not help. |
| `unknown` | The failure could not be classified -- e.g. errors raised outside the connector layer that reach the output struct type-erased. Also a Databricks, Snowflake, BigQuery or Trino statement that passed its client deadline (`timeout_secs`) or that Rocky stopped waiting on: it may still be running. When Rocky had the statement's id, it sent a cancel first. Also a BigQuery job that finished without error but whose result Rocky could not read: its changes are applied. | Depends. Surface the raw `error` string. Never retry a statement that may still run. |

The classifier walks the `anyhow::Error` chain, downcasts to `AdapterError`, then probes `.inner()` for the typed connector enum. An error built with `anyhow::anyhow!("...{e}")` drops the type and becomes `unknown`. An error propagated with `?` or `.context(...)` keeps it.

### Recommended consumer policy

Map each variant to one of four actions:

| Action | Variants |
|---|---|
| **Retry with backoff** | `transient`, `connection-failed` |
| **Retry with extended backoff and alert** | `quota-exceeded` |
| **Don't retry; alert the model owner** | `auth-failed`, `query-rejected`, `not-found`, `compile-error` |
| **Surface raw `error` for triage** | `unknown` |

"Retry with backoff" needs one qualification. The run loop already retries proven-transient failures itself, on by default. See [Classified retry](/advanced/failure-modes/#classified-retry) and `[resilience] transient_max_retries`, which defaults to 2.

A `transient` entry in `errors[*]` has already used its in-run retry budget. At the orchestrator, retry means a *delayed* re-run or `--resume-latest` (replication runs). For a transformation run, `rocky run --retry-failed` reruns the failed and never-built models and their downstream models, and rebuilds a model it cannot prove is current.

`connection-failed` is retry-safe although the warehouse never saw the request. `reqwest::is_connect()` is the main signal (TCP, TLS, DNS failures), and other non-timeout transport errors also classify here. A transport timeout (`reqwest::is_timeout()`) is `transient`. A Databricks or Snowflake statement deadline is `unknown`, not `transient`: the statement may still be running, so a retry could run it twice. A credential problem is `auth-failed`.

## Consuming from Dagster

Branch on `failure_kind` after the rocky call returns. The kebab-case values are the stable wire contract, so use a set-membership check. `dagster-rocky` surfaces `failure_kind` on `RunResult.errors[*]`.

```python
import dagster as dg
from dagster_rocky import RockyResource

# `unknown` is left out on purpose: it covers a Databricks or Snowflake
# statement that may still be running, which a retry could run twice.
RETRY_KINDS = {"transient", "connection-failed", "quota-exceeded"}
ALERT_KINDS = {"auth-failed", "query-rejected", "not-found", "compile-error"}


@dg.asset(
    retry_policy=dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
)
def replicated_tables(
    context: dg.AssetExecutionContext,
    rocky: RockyResource,
) -> dg.MaterializeResult:
    result = rocky.run(filter="tenant=acme")

    retryable, alertable = [], []
    for err in result.errors:
        target = "/".join(err.asset_key)
        if err.failure_kind in RETRY_KINDS:
            retryable.append((target, err.failure_kind))
        elif err.failure_kind in ALERT_KINDS:
            alertable.append((target, err.failure_kind, err.error))
        else:
            context.log.warning(f"unclassified failure on {target}: {err.error}")

    for target, kind, msg in alertable:
        context.log.error(f"alert: {target} -- {kind} -- {msg}")  # hand off to your alerting

    if retryable:
        # A dg.Failure lets the retry_policy above back the asset off.
        raise dg.Failure(
            description=f"{len(retryable)} table(s) failed transiently; retrying.",
            metadata={"retryable": dg.MetadataValue.json(
                [{"target": t, "kind": k} for t, k in retryable])},
        )

    return dg.MaterializeResult(metadata={"tables_copied": result.tables_copied})
```

Without Dagster, branch on the first column of this output: send `transient` and `quota-exceeded` to a retry loop and page the on-call for the rest.

```bash
rocky --config rocky.toml run --output json \
  | jq -r '.errors[] | "\(.failure_kind)\t\(.asset_key | join("/"))\t\(.error)"'
```

## When `failure_kind` is `unknown`

`unknown` means the classifier found no typed connector variant, or found one that leaves the outcome open. Three cases produce it:

1. **Non-adapter errors**: drift reconciliation, governance and state-store failures at the table level. The `error` string is your only signal. Triage it by hand.
2. **Worker-task panics**: a `JoinError` yields `failure_kind = "unknown"` with the panic message in `error`. File a bug, do not retry.
3. **A statement that may still run**: a Databricks or Snowflake statement passed its client deadline (`timeout_secs`), or a poll error persisted. Rocky sent a cancel first, and the `error` says what came of it. The submit itself may also have timed out before the warehouse returned an id, so nothing could be cancelled. Rocky does not resubmit the statement, and `table_retries` does not re-run the table. Check the warehouse's query history before you re-run: the statement may have finished or may still be running.

Treat `unknown` as surface-and-triage, never as retry-safe.

## See also

- [Failure modes](/advanced/failure-modes/) -- the nine-category taxonomy and recovery playbook for every kind of Rocky failure.
- [JSON output](/reference/json-output/) -- the full versioned schema for `rocky run` and every other command.
- [`rocky plan --resume-latest`](/reference/commands/core-pipeline/#rocky-plan) -- resume a failed run from its last checkpoint; per-table progress is recorded for every success and every classified failure.
