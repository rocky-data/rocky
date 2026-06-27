---
title: Per-table error containment
description: How rocky run isolates failures at the table boundary and how to consume the failure_kind discriminator
sidebar:
  order: 4
---

`rocky run` (and the canonical `rocky plan` + `rocky apply` flow it backs) treats each table as an isolated unit of work. One table failing does not crash the run. The other tables in the same invocation keep going, the run finishes with a partial-success exit code, and the per-table failures are enumerated on `RunOutput.errors[*]` with a typed `failure_kind` discriminator so orchestrators can branch on the kind of failure without parsing the free-form `error` string.

## The containment property

The per-table loop in `commands/run.rs` dispatches every table through `process_table` inside a `tokio::JoinSet`. The match arm that handles a task's `Result` has three branches:

- **Success** -- the materialization is appended to `RunOutput.materializations` and progress is checkpointed.
- **Adapter error** (`Ok((idx, Err(e)))`) -- the error is classified into a [`FailureKind`](#failure_kind-taxonomy) **before** stringification (so the typed connector variant is preserved), then a `TableError { asset_key, error, failure_kind }` is pushed onto `table_errors`. The loop continues with the remaining tables.
- **Task panic** (`Err(JoinError)`) -- the panic message is captured into a `TableError` with `failure_kind = "unknown"`. The loop continues.

The only path that aborts the run early is `--fail-fast` (which calls `JoinSet::abort_all()` on the first error). Otherwise the run finishes with a non-zero exit code so callers can distinguish partial success from clean success, while the JSON output stays well-formed.

This is true for every adapter (Databricks, Snowflake, BigQuery, DuckDB) -- the loop is adapter-agnostic and catches `anyhow::Error` from any source, including connector errors, schema-drift failures, governance reconciliation errors, and worker-task panics.

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

`failure_kind` is a coarse classifier over the failure surface. Most variants partition the connector error spaces for Databricks and Snowflake; `compile-error` covers a model that fails to compile mid-run (not a connector failure at all); and `unknown` is the fallback for failures that reach the output layer type-erased.

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

The classifier walks the `anyhow::Error` chain on each per-table failure, downcasts to `AdapterError`, and probes `.inner()` for the typed connector enum. Errors built via `anyhow::anyhow!("...{e}")` (which stringify and drop the type) fall through to `unknown`; errors propagated via `?` / `.context(...)` preserve the typed source and classify correctly. As of engine `v1.34`, the 23 sites in `run.rs` that previously stringified adapter errors have been converted to type-preserving wraps, so `failure_kind` returns a non-`unknown` value for every real production adapter error.

### Recommended consumer policy

Map each variant to one of four actions:

| Action | Variants |
|---|---|
| **Retry with backoff** | `transient`, `connection-failed` |
| **Retry with extended backoff and alert** | `quota-exceeded` |
| **Don't retry; alert the model owner** | `auth-failed`, `query-rejected`, `not-found`, `compile-error` |
| **Surface raw `error` for triage** | `unknown` |

Treat `connection-failed` as retry-safe even though the warehouse never saw the request: `reqwest::is_connect()` is the discriminator, which fires on actual TCP / TLS / DNS failures, not on credentials issues (which land on `auth-failed` instead).

## Consuming from Dagster

Branch on `failure_kind` inside the asset / op body after the rocky call returns. The kebab-case string values (`"transient"`, `"auth-failed"`, ...) are the stable wire contract, so a set-membership check is the safest pattern:

```python
import dagster as dg
from dagster_rocky import RockyResource

RETRY_KINDS = {"transient", "connection-failed", "quota-exceeded"}
ALERT_KINDS = {"auth-failed", "query-rejected", "not-found", "compile-error"}


@dg.asset
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

Pair the asset with a Dagster `RetryPolicy` to back off on `dg.Failure`:

```python
defs = dg.Definitions(
    assets=[replicated_tables.with_retry_policy(
        dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL),
    )],
    resources={"rocky": RockyResource(config_path="rocky.toml")},
)
```

Requires engine `v1.34+` (which emits the discriminator on the wire) and `dagster-rocky` `v1.35+` (which surfaces `failure_kind` directly on `RunResult.errors[*]`). Older bindings default the field to `"unknown"` when parsing a newer engine's output.

For non-Dagster consumers, `rocky run --output json | jq` gives the same shape:

```bash
rocky run --output json --config rocky.toml \
  | jq -r '.errors[] | "\(.failure_kind)\t\(.asset_key | join("/"))\t\(.error)"'
```

Branch on the first column in your shell pipeline -- `transient` and `quota-exceeded` go into a retry loop, everything else pages the on-call.

## When `failure_kind` is `unknown`

`unknown` is the fallback when the classifier can't reach a typed connector variant on the error chain. Two cases produce it today:

1. **Non-adapter errors** -- drift reconciliation failures, governance errors, state-store failures that surface at the per-table level. The error is real and well-formed, but the type-erased `anyhow::Error` doesn't expose a connector variant. The free-form `error` string is the only signal; triage manually.
2. **Worker-task panics** -- a `JoinError` from a panicked task produces a `TableError` with `failure_kind = "unknown"`. The panic message is in `error`. This is rare and almost always a bug to file rather than retry.

Treat `unknown` as a surface-and-triage signal, never as silently retry-safe.

## See also

- [Failure modes](./failure-modes) -- the nine-category taxonomy and recovery playbook for every kind of Rocky failure.
- [JSON output](../../reference/json-output) -- the full versioned schema for `rocky run` and every other command.
- [`rocky plan --resume-latest`](../../reference/cli) -- resume a failed run from its last checkpoint; per-table progress is recorded for every success and every classified failure.
