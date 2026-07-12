# 03-anomaly-detection — Row count anomalies via state history

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run` row-count anomaly detection (`[pipeline.poc.checks]`), `rocky history`, state-store-backed row count baselines

## What it shows

Demonstrates anomaly detection via the embedded state store. The POC:

1. Runs the same pipeline 3 times against the normal 500-row seed.
2. Each run records a row count snapshot in the redb state store.
3. The 4th run uses **truncated** source data (5 rows instead of 500).
4. The incident run emits a `row count anomaly detected` warning and an
   `anomalies` block in its `--output json`, comparing the current count
   against the rolling baseline. `rocky history` lists all four runs from
   the local `.rocky-state.redb`.

## Why it's distinctive

- **Stateful** anomaly tracking that persists between runs without an
  external metrics store. Anomaly detection runs inline during `rocky run`
  (enabled by `[pipeline.poc.checks]`), comparing against the row-count
  baseline held in the local `.rocky-state.redb`.

## Layout

```
.
├── README.md
├── rocky.toml
├── run.sh                  Runs the pipeline 4 times, then queries history
└── data/
    ├── seed.sql            Generates 500 rows
    └── seed_truncated.sql  Simulated incident — only 5 rows
```

## Run

```bash
./run.sh
```

## Expected output

The incident (4th) run detects the drop against the baseline of the first
three runs. On stderr you'll see:

```
WARN row count anomaly detected  table: poc.staging__events.events
     reason: "row count dropped 98.7% (expected ~376, got 5)"
```

and `expected/run-incident.json` carries the same finding in structured form:

```json
"anomalies": [
  {
    "table": "poc.staging__events.events",
    "current_count": 5,
    "baseline_avg": 376.25,
    "deviation_pct": 98.67,
    "reason": "row count dropped 98.7% (expected ~376, got 5)"
  }
]
```

`run.sh` grep-asserts on the warning, so the run fails if the anomaly does
not fire. `rocky history` then lists the four runs (IDs, status, duration,
recipe hashes) from the local state store; it does not itself print row
counts — the deviation lives in the run's `anomalies` block above.
