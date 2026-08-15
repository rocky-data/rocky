---
title: Per-table error containment
description: How rocky run keeps one failed table from killing the whole run, and how to branch on failure_kind
sidebar:
  order: 4
---

`rocky run` treats each table as its own unit of work. The canonical `rocky plan` plus `rocky apply` flow behind it does the same. One table failing does not crash the run.

The other tables in that invocation keep going. The run then finishes with a partial-success exit code, and lists every per-table failure on `RunOutput.errors[*]`. Each entry carries a typed `failure_kind` discriminator, so an orchestrator can branch on the kind of failure instead of parsing the free-form `error` string.

## How the run loop contains a failure

The per-table loop in `commands/run.rs` dispatches every table through `process_table` inside a `tokio::JoinSet`. Each finished task lands on one of three branches:

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

Classifying the adapter error into a [`FailureKind`](#failure_kind-taxonomy) *before* stringifying it is what preserves the typed connector variant. The `TableError` pushed onto `table_errors` holds `asset_key`, `error`, and `failure_kind`.

Two config keys abort the run early. Both live under `[execution]`, and neither is a CLI flag.

- `fail_fast = true` (default `false`) aborts on the first error.
- `error_rate_abort_pct` (default `50`, `0` disables it) aborts once more than that percentage of completed tables have failed. Rocky checks the rate only after at least 4 tables complete.

Without an abort, the run finishes and exits non-zero. That lets a caller tell partial success from clean success, while the JSON output stays well-formed.

The loop is agnostic to the [adapter](/reference/glossary/#adapter), the plugin that runs Rocky's SQL on one particular warehouse. So this holds for Databricks, Snowflake, BigQuery, and DuckDB alike. The loop catches `anyhow::Error` from any source: connector errors, schema-drift failures, governance reconciliation errors, and worker-task panics.

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

`failure_kind` is a coarse classifier over the failure surface. Most variants partition the connector error spaces for Databricks and Snowflake. `compile-error` is the exception: it covers a model that fails to compile mid-run, which is not a connector failure at all. `unknown` is the fallback for a failure that reaches the output layer type-erased.

| Variant | Meaning | Retry-safe? |
|---|---|---|
| `connection-failed` | TCP / TLS / DNS / connection-establishment failure -- the request never reached the warehouse. | Yes, with backoff. |
| `auth-failed` | Credentials rejected or token expired -- 401, 403, or an auth-chain rejection. | No. Fix credentials before retrying. |
| `query-rejected` | Warehouse parsed and rejected the SQL -- syntax error, missing column, missing permission, semantic analysis failure. | No. The SQL needs fixing. |
| `transient` | Retry-worthy failure -- 5xx, network glitch, statement aborted by a transient warehouse condition, statement timeout, circuit-breaker open. | Yes. |
| `quota-exceeded` | Rate limit hit or a configured cap reached -- 429, retry-budget exhaustion, account-level quota. | Yes, with extended backoff and an alert. |
| `not-found` | Requested catalog / schema / table not present -- 404 from the warehouse, often an upstream rename. | No. Re-discovery or human triage needed. |
| `compile-error` | The model failed to compile during the run -- a type error, unresolved reference, or other `Error`-severity diagnostic surfaced while building this model. No warehouse call was attempted. The diagnostic is carried in `error`. | No. Fix the model SQL or its upstream; re-running won't help. |
| `unknown` | The failure could not be classified -- e.g. errors raised outside the connector layer that reach the output struct type-erased. | Depends. Surface the raw `error` string. |

The classifier walks the `anyhow::Error` chain on each per-table failure. It downcasts to `AdapterError`, then probes `.inner()` for the typed connector enum.

How the error was built decides whether that works. An error built with `anyhow::anyhow!("...{e}")` stringifies its source and drops the type, so it falls through to `unknown`. An error propagated with `?` or `.context(...)` keeps the typed source and classifies correctly.

Engine `v1.34` converted the 23 sites in `run.rs` that used to stringify adapter errors into type-preserving wraps. Since then, `failure_kind` returns a non-`unknown` value for every real production adapter error.

### Recommended consumer policy

Map each variant to one of four actions:

| Action | Variants |
|---|---|
| **Retry with backoff** | `transient`, `connection-failed` |
| **Retry with extended backoff and alert** | `quota-exceeded` |
| **Don't retry; alert the model owner** | `auth-failed`, `query-rejected`, `not-found`, `compile-error` |
| **Surface raw `error` for triage** | `unknown` |

"Retry with backoff" needs one qualification. Since engine 1.58.0 the run loop already retries proven-transient failures itself, on by default. See [Classified retry](/advanced/failure-modes/#classified-retry) and `[resilience] transient_max_retries`, which defaults to 2.

So a `transient` entry that reaches your `errors[*]` has already exhausted its in-run retry budget. At the orchestrator level, retry means a *delayed* re-run or `--resume-latest`. It does not mean a tight-loop retry, which only doubles the engine's own attempts.

Treat `connection-failed` as retry-safe even though the warehouse never saw the request. `reqwest::is_connect()` is the primary signal, and it fires on real TCP, TLS, and DNS failures. Other non-timeout transport errors also classify as `connection-failed`. A timeout classifies as `transient` instead. A credential problem lands on `auth-failed`, via the typed `Auth` variant or a 401 or 403, and never here.

## Consuming from Dagster

Branch on `failure_kind` inside the asset / op body after the rocky call returns. The kebab-case string values (`"transient"`, `"auth-failed"`, ...) are the stable wire contract, so a set-membership check is the safest pattern:

```python
import dagster as dg
from dagster_rocky import RockyResource

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

    retryable: list[tuple[str, str, str]] = []
    alertable: list[tuple[str, str, str]] = []
    for err in result.errors:
        target = "/".join(err.asset_key)
        if err.failure_kind in RETRY_KINDS:
            retryable.append((target, err.failure_kind, err.error))
        elif err.failure_kind in ALERT_KINDS:
            alertable.append((target, err.failure_kind, err.error))
        else:
            context.log.warning(f"unclassified failure on {target}: {err.error}")

    if alertable:
        # Hand off to your alerting layer (PagerDuty, Slack, Sentry, ...).
        for target, kind, msg in alertable:
            context.log.error(f"alert: {target} -- {kind} -- {msg}")

    if retryable:
        # Surface as a non-fatal failure so Dagster's retry policy picks it up.
        raise dg.Failure(
            description=f"{len(retryable)} table(s) failed transiently; retrying.",
            metadata={
                "retryable": dg.MetadataValue.json(
                    [{"target": t, "kind": k} for t, k, _ in retryable]
                ),
            },
        )

    return dg.MaterializeResult(
        metadata={"tables_copied": result.tables_copied}
    )
```

The `retry_policy=` set on the `@dg.asset` decorator above backs the asset off on `dg.Failure`. Wire it into your `Definitions`:

```python
defs = dg.Definitions(
    assets=[replicated_tables],
    resources={"rocky": RockyResource(config_path="rocky.toml")},
)
```

This needs two versions. Engine `v1.34+` emits the discriminator on the wire, and `dagster-rocky` `v1.35+` surfaces `failure_kind` directly on `RunResult.errors[*]`. Older bindings default the field to `"unknown"` when they parse a newer engine's output.

For a non-Dagster consumer, `rocky run --output json | jq` gives the same shape:

```bash
rocky --config rocky.toml run --output json \
  | jq -r '.errors[] | "\(.failure_kind)\t\(.asset_key | join("/"))\t\(.error)"'
```

Branch on the first column in your shell pipeline. Send `transient` and `quota-exceeded` into a retry loop, and page the on-call for everything else.

## When `failure_kind` is `unknown`

`unknown` is the fallback when the classifier cannot reach a typed connector variant on the error chain. Two cases produce it today:

1. **Non-adapter errors** -- drift reconciliation failures, governance errors, and state-store failures that surface at the per-table level. The error is real and well-formed. But the type-erased `anyhow::Error` exposes no connector variant, so the free-form `error` string is your only signal. Triage it by hand.
2. **Worker-task panics** -- a `JoinError` from a panicked task produces a `TableError` with `failure_kind = "unknown"`. The panic message is in `error`. This is rare, and it is almost always a bug to file rather than a failure to retry.

Treat `unknown` as a surface-and-triage signal, never as silently retry-safe.

## See also

- [Failure modes](/advanced/failure-modes/) -- the nine-category taxonomy and recovery playbook for every kind of Rocky failure.
- [JSON output](../../reference/json-output) -- the full versioned schema for `rocky run` and every other command.
- [`rocky plan --resume-latest`](../../reference/cli) -- resume a failed run from its last checkpoint; per-table progress is recorded for every success and every classified failure.
