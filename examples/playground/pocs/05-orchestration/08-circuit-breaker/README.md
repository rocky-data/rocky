# 08-circuit-breaker — retry policy + three-state circuit breaker

> **Category:** 05-orchestration
> **Credentials:** none to validate the config; Databricks/Snowflake to observe the breaker fire
> **Runtime:** < 2s
> **Rocky features:** `[adapter.retry]`, `circuit_breaker_{tripped,recovered}` events

## What it shows

Adapter operations run behind exponential-backoff retry and a three-state circuit
breaker: after N consecutive failures it stops calling the warehouse, then tries
again after a timeout.

```toml
[adapter.retry]
max_retries = 3
initial_backoff_ms = 1000
max_backoff_ms = 30000
backoff_multiplier = 2.0
jitter = true
circuit_breaker_threshold = 5           # trip after N consecutive failures
circuit_breaker_recovery_timeout_secs = 60  # auto-advance Open → HalfOpen
```

**States:** `Closed` → (N failures) → `Open` → (timeout) → `HalfOpen`
→ (success → Closed) / (failure → Open).

**Events** on the run's event bus: `circuit_breaker_tripped` (Closed/HalfOpen →
Open) and `circuit_breaker_recovered` (HalfOpen → Closed).

## Credentials

- **None** for this POC, which only validates the config.
- **To watch the breaker fire:** set `[adapter] type = "databricks"`
  with a bad token, run a few retries against the real adapter, and
  inspect the events in the run JSON. The
  [`07-adapters/02-databricks-materialized-view/`](../../07-adapters/02-databricks-materialized-view/)
  POC has a working Databricks config to fork from.

## Run

```bash
./run.sh
```

It runs `rocky validate`, then prints the state diagram and event names.

## Related

- `[state.retry]` has the same shape and semantics as `[adapter.NAME.retry]`
  (same `RetryConfig`, breaker, events). See
  [`03-remote-state-s3/`](../03-remote-state-s3/) for a live SeaweedFS demo.
- [`04-checkpoint-resume/`](../04-checkpoint-resume/): resume after a partial
  failure.
