# 08-circuit-breaker — Trust arc 3: retry policy + three-state circuit breaker

> **Category:** 05-orchestration
> **Credentials:** none to validate the config; Databricks/Snowflake to observe the breaker fire
> **Runtime:** < 2s
> **Rocky features:** `[adapter.retry]`, `circuit_breaker_{tripped,recovered}` events

## What it shows

The deploy-safety side of Arc 3: adapter operations run behind an
exponential-backoff retry policy and a three-state circuit breaker.

**TOML surface:**

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

**Events:** state transitions fire `PipelineEvent`s on the run's
event bus — `circuit_breaker_tripped` (Closed/HalfOpen → Open) and
`circuit_breaker_recovered` (HalfOpen → Closed).

## Why it's distinctive

- **Trust-grade resilience is a warehouse-adapter concern, not a user
  concern.** The breaker lives in the Rust crates; you configure it
  declaratively in `rocky.toml`.
- **The retry policy and breaker are observable.** Transitions emit
  events and can feed a hook (future `event_hooks` bridge, Arc 3
  wave 2). You get back pressure, not silent failure.

## Layout

```
.
├── README.md    this file
├── rocky.toml   [adapter.retry] block with all knobs explicit
└── run.sh       validate the config; print the state diagram
```

## Credentials

- **None** for this POC (validates the config).
- **To watch the breaker fire:** set `[adapter] type = "databricks"`
  with a bad token, run a few retries against the real adapter, and
  inspect the events in the run JSON. The
  [`07-adapters/02-databricks-materialized-view/`](../../07-adapters/02-databricks-materialized-view/)
  POC has a working Databricks config to fork from.

## Run

```bash
./run.sh
```

## What happened

1. `rocky validate` confirms the `[adapter.retry]` block parses and all
   keys are recognized.
2. The state diagram + event names are printed.

## Related

- Engine source: `engine/crates/rocky-core/src/circuit_breaker.rs`,
  `engine/crates/rocky-core/src/config.rs` (`RetryConfig`)
- Adapters wired: `rocky-databricks/src/connector.rs`,
  `rocky-snowflake/src/connector.rs`
- Sibling POC: [`04-checkpoint-resume/`](../04-checkpoint-resume/)
  covers the other half of Arc 3 — state-store-backed resume.
