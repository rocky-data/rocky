# 03-anomaly-detection — Row count anomalies via state history

> **Category:** 01-quality
> **Credentials:** none (DuckDB)
> **Runtime:** < 10s
> **Rocky features:** `rocky run` row-count anomaly detection (`[pipeline.poc.checks]`), `rocky history`, state-store-backed row count baselines

## What it shows

Row-count anomaly detection inside `rocky run` (enabled by `[pipeline.poc.checks]`), with baselines kept in the local `.rocky-state.redb` and no external metrics store. The POC runs three times on a 500-row seed, then a fourth time on a source truncated to 5 rows.

## Run

```bash
./run.sh
```

## Expected output

The fourth run's stderr (captured by `run.sh` in `expected/run-incident.log`) reports:

```
WARN row count anomaly detected  table: poc.staging__events.events
     reason: "row count dropped 98.7% (expected ~376, got 5)"
```

`expected/run-incident.json` carries the same finding:

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

`run.sh` asserts on the warning, so it fails if the anomaly does not fire. `rocky history` then lists the four runs (IDs, status, duration, recipe hashes); it does not print row counts. The deviation is in the run's `anomalies` block.
