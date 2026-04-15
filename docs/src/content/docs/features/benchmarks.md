---
title: Performance Benchmarks
description: Rocky vs dbt-core vs dbt-fusion — compilation speed, memory usage, and cost analysis
sidebar:
  order: 10
---

Rocky compiles 10,000 models in **1.00 second** — **34x faster** than dbt-core and **38x faster** than dbt-fusion, while using **4-7x less memory**.

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

## Why Rocky Is Faster Than dbt

Rocky doesn't make the warehouse faster — it makes everything *around* the warehouse faster. The actual SQL execution time is the same. What's different is the compile-plan-execute overhead.

**No Jinja, no manifest, no parse step.** dbt's core loop is: parse Jinja templates, build a manifest JSON (often 50MB+), resolve the DAG, generate SQL. Rocky replaces all of that with a single compiled Rust binary that goes straight from TOML + SQL to an execution plan. There's no templating engine sitting between you and your SQL. This is why startup is 64x faster and config validation is 146x faster.

**Parallel compilation by execution layer.** Rocky groups models into DAG layers (models within a layer have no interdependencies) and type-checks each layer in parallel across all CPU cores via [rayon](https://github.com/rayon-rs/rayon). dbt's graph traversal is single-threaded Python. This drives the 34x compile speedup.

**Compile-time analysis, not runtime discovery.** Type checking, lineage extraction, schema drift detection, and contract validation all happen at compile time. dbt discovers many of these issues only at runtime, which means slower feedback loops and wasted warehouse compute on runs that were going to fail anyway.

**SQL-native incrementals.** Rocky generates watermark-based `WHERE` clauses directly in SQL, pushing the filter to the warehouse. dbt merges state in Python, then builds the `INSERT` — an extra round-trip.

## Why Rocky Needs Less CPU and Memory

This is the more interesting story, because it's about architectural choices, not just "Rust is faster than Python."

**No Python runtime overhead.** dbt loads the entire Python interpreter, its dependency tree (Jinja2, agate, networkx, protobuf, etc.), and the adapter plugin system before it does anything useful. Rocky is a single ~15 MB static binary compiled with `opt-level = 3`, thin LTO, and `panic = "abort"` — startup is 14ms with no interpreter, no GC, no import chain.

**String interning.** In a large project, identifiers like `catalog.schema.table` and column names repeat thousands of times across model metadata. Rocky interns them via [lasso](https://github.com/Kixiron/lasso) — each unique string is stored once, and every reference is a cheap integer handle. dbt duplicates these strings across Python dicts and dataclass instances, ballooning memory proportionally to project size. This is why Rocky uses 4.3x less memory.

**Memory-mapped file I/O.** For projects with thousands of SQL files, Rocky uses `mmap` for files larger than 4 KB, letting the OS handle page-level scheduling. Python's buffered `read()` allocates a new string per file, all resident in the GC heap.

**No manifest file.** dbt writes and re-reads a serialized manifest (JSON or msgpack) that grows with your project — 50 MB+ is common at scale. This is pure I/O and memory overhead that Rocky eliminates entirely. Rocky's `Project` struct lives in-process memory with pre-computed execution layers; nothing is serialized to disk between phases.

**Embedded state store instead of YAML files.** Rocky tracks incremental state in an embedded [redb](https://github.com/cberner/redb) transactional database. dbt uses YAML/JSON files that must be fully loaded and parsed — more memory, more I/O, no atomicity. Atomic reads from redb are why warm compiles (0.72s) are 28% faster than cold compiles.

**No GC pressure.** Python's garbage collector must periodically scan and collect dbt's in-memory graph. Rocky uses Rust's ownership model — memory is freed deterministically at scope exit. No GC pauses, no memory fragmentation over long runs.

> dbt's resource consumption scales with project size (more models → bigger manifest → more memory → slower parsing). Rocky's resource consumption scales with execution complexity (more parallel layers → more threads, but memory stays flat because of interning and mmap). A 10,000-model Rocky project compiles in roughly the same memory as a 100-model one.

This matters most in CI (where you're paying per vCPU-minute) and in orchestrators like Dagster (where the Rocky subprocess is a lightweight sidecar, not a memory-hungry Python process competing with the scheduler for resources).

## How Does SQLMesh Compare?

SQLMesh is architecturally closer to Rocky than dbt — it uses [SQLGlot](https://github.com/tobymao/sqlglot) for static SQL analysis instead of Jinja templates. But it's still Python-bound:

- **Single-threaded analysis.** SQLGlot parses and type-checks models sequentially in Python. Rocky parallelizes type checking across DAG layers via rayon, using all available CPU cores.
- **Python AST overhead.** SQLGlot represents every SQL AST node as a Python object with full string attributes. Rocky interns identifiers and uses Rust's zero-cost abstractions — same analysis, a fraction of the memory.
- **Snapshot state.** SQLMesh maintains "snapshots" (similar to dbt's manifest) that track environment state and grow with project size. Rocky keeps the project graph in-memory with no serialization step, and persists only incremental watermarks in an embedded redb database.
- **Runtime overhead.** SQLMesh loads the Python interpreter, SQLGlot, and its dependency tree on every invocation. Rocky's static binary starts in 14ms.

The result: at 10k models, Rocky compiles in ~1 second using ~147 MB. Python-based tools — whether they use Jinja (dbt) or SQLGlot (SQLMesh) — operate in the 30-40 second range with 4-7x the memory footprint.

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

| | 10k (measured) | 50k (extrapolated) |
|---|---:|---:|
| **Rocky** | **1.00 s** | **~5.0 s** |
| dbt-core | 34.62 s | ~173 s |
| dbt-fusion | 38.43 s | ~192 s |

Memory also scales linearly:

| | 10k (measured) | 50k (extrapolated) |
|---|---:|---:|
| **Rocky** | **147 MB** | **~735 MB** |
| dbt-core | 629 MB | ~3,145 MB |
| dbt-fusion | 1,063 MB | ~5,315 MB |

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
| Rocky 0.3.0 (optimized) | 1.00 s | 100 µs | 147 MB |
| Rocky 1.0.3 (current) | **1.00 s** | **100 µs** | 147 MB |

Cumulative: **25% faster** compile since v0.1.0. Performance has held steady through 1.0 as new features (drift detection, lineage, cost estimation) were added without regressing the compile path. The memory increase (116 → 147 MB) is a deliberate tradeoff — caching and pre-allocation that trade ~31 MB for 39% faster warm compiles.
