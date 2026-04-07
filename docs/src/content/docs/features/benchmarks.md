---
title: Performance Benchmarks
description: Rocky vs dbt-core vs dbt-fusion — compilation speed, memory usage, and cost analysis
sidebar:
  order: 10
---

Rocky 0.3.0 (post-optimization) compiles 10,000 models in **1.00 second** — **34x faster** than dbt-core and **38x faster** than dbt-fusion, while using **4-7x less memory**.

## Headline Numbers (10k models)

| Metric | Rocky | dbt-core 1.11.8 | dbt-fusion 2.0.0 |
|---|---:|---:|---:|
| **Compile** | **1.00 s** | 34.62 s (34x) | 38.43 s (38x) |
| **Peak memory** | **147 MB** | 629 MB (4.3x) | 1,063 MB (7.2x) |
| **Lineage** | **0.84 s** | 35.36 s (42x) | N/A |
| **Startup** | **14 ms** | 896 ms (64x) | 12 ms |
| **DAG resolution** | **0.36 s** | 9.12 s (25x) | 2.73 s (7x) |
| **Warm compile (1 file)** | **0.72 s** | 33.12 s (46x) | 37.16 s (52x) |
| **SQL generation** | **200 ms** | N/A | N/A |
| **Config validation** | **15 ms** | 2,187 ms (146x) | 1,473 ms (98x) |

Benchmarked on Apple Silicon (12-core, 36 GB RAM) with a synthetic 4-layer medallion DAG. 3 iterations per benchmark, mean reported. Full methodology in `examples/playground/benchmarks/REPORT_CURRENT.md`.

## What These Numbers Mean

### For CI/CD

Rocky compiles and traces lineage in **1.84 seconds**. dbt-core takes **70 seconds**. For a 10-engineer team running 5 PR iterations/day each:

| Tool | Annual developer wait time | Cost at $75/hr |
|---|---:|---:|
| **Rocky** | 9.3 hours | $700 |
| dbt-core | 354.8 hours | $26,608 |
| dbt-fusion | 194.8 hours | $14,610 |

### For Orchestration

Rocky enables 60-second Dagster sensor intervals. dbt-core needs 2-5 minute intervals because its 35-second compile consumes most of the cycle. Rocky uses **under 1%** of a 2-minute sensor cycle for compilation; dbt-core uses **29%**.

### For Container Sizing

| Scale | Rocky | dbt-core | dbt-fusion |
|---:|:---:|:---:|:---:|
| Fits in 512 MB pod | **10k** | No | No |
| Fits in 1 GB pod | **10k** | 10k | No |
| Needs 2 GB pod | Never | Never | **10k** |

### For Iterative Development

Rocky's warm compile (after changing 1 file) takes **0.72 seconds** — 28% faster than a cold compile. dbt-core and dbt-fusion show no warm compile benefit (their partial parse doesn't help because the bottleneck is SQL generation, not parsing).

## Scaling Behavior

Rocky compiles linearly. Per-model cost is flat at **~100 µs** from 1k to 50k models. Verified across prior benchmark rounds:

```
            10k (measured)   50k (extrapolated)
Rocky       1.00 s           ~5.0 s
dbt-core    34.62 s          ~173 s
dbt-fusion  38.43 s          ~192 s
```

Memory also scales linearly:

```
            10k (measured)   50k (extrapolated)
Rocky       147 MB           ~735 MB
dbt-core    629 MB           ~3,145 MB
dbt-fusion  1,063 MB         ~5,315 MB
```

At 50k models, Rocky stays well under 1 GB. dbt-fusion would need ~5 GB — exceeding standard EKS pod limits.

## Annual Cost Model (10k models, 10 engineers)

Same 5-minute sensor interval, Databricks SQL Classic with auto-stop, EKS Fargate:

| Cost component | Rocky | dbt-core | dbt-fusion |
|---|---:|---:|---:|
| Shared infrastructure | $2,847 | $2,847 | $2,847 |
| **Tool-dependent costs** | | | |
| Fargate (orchestration + CI) | $25 | $50 | $99 |
| Databricks idle burn | $21 | $894 | $988 |
| Developer wait time (CI) | $700 | $26,608 | $14,610 |
| **Tool subtotal** | **$749** | **$27,353** | **$15,338** |
| **Grand total** | **$3,596** | **$30,200** | **$18,185** |

Rocky saves **$26,604/year** vs dbt-core regardless of Databricks compute strategy. At 25 engineers, savings reach **$65k/year**.

## Reproducing the Benchmarks

```bash
cd examples/playground/benchmarks

# Build Rocky release binary
cd ../../../engine && cargo build --release && cd -

# Setup Python env
python3 -m venv .venv
.venv/bin/pip install dbt-core dbt-duckdb psutil matplotlib

# Generate 10k-model project
python generate_dbt_project.py --scale 10000 --output-dir .

# Run full suite
python run_benchmark.py \
  --scale 10000 --iterations 3 \
  --tool all --benchmark-type all \
  --rocky-bin ../../../engine/target/release/rocky

# Generate charts
python visualize.py results/benchmark_*.json
```

## Round-by-Round Improvement

| Version | Compile (10k) | Per-model | Peak RSS |
|---|---:|---:|---:|
| Rocky 0.1.0 | 1.33 s | 133 µs | 116 MB |
| Rocky 0.3.0 | 1.20 s | 120 µs | 125 MB |
| Rocky 0.3.0 (optimized) | **1.00 s** | **100 µs** | 147 MB |

Cumulative: **25% faster** compile since v0.1.0. The memory increase (116 → 147 MB) is a deliberate tradeoff — caching and pre-allocation that trade ~31 MB for 39% faster warm compiles.
