---
title: Performance Benchmarks
description: How fast Rocky compiles, how much memory it uses, and how to reproduce the numbers yourself.
sidebar:
  order: 4.6
---

Rocky compiles 10,000 models in **1.00 second** and holds peak memory at **147 MB**. This page gives the measurements, what makes the compile loop fast, and the commands that reproduce them.

## Headline numbers (10k models)

| Metric | Rocky |
|---|---:|
| **Compile** | **1.00 s** |
| **Peak memory** | **147 MB** |
| **Lineage** | **0.84 s** |
| **Startup** | **14 ms** |
| **DAG resolution** | **0.36 s** |
| **Warm compile (1 file)** | **0.72 s** |
| **SQL generation** | **200 ms** |
| **Config validation** | **15 ms** |

Benchmarked on Apple Silicon (12-core, 36 GB RAM) with a synthetic 4-layer medallion DAG, 3 iterations, mean reported. Methodology: `examples/playground/benchmarks/REPORT_CURRENT.md`. Rocky cuts the compile-plan-execute overhead around the warehouse. SQL execution time is unchanged.

## How Rocky's compile loop stays fast

Rocky goes from your files to an execution plan in one process, with nothing
written to disk in between.

```
 TOML + SQL ──► parse ──► DAG layers ──► type-check ──► dialect SQL
  (no                        │             (rayon)            │
   templating)               ▼                                ▼
                    layer 0 [a, b, c]  ← no deps        execution plan
                    layer 1 [d, e]     ← needs layer 0
                    layer 2 [f]        ← needs layer 1

  Models inside one layer are type-checked in parallel, across all cores.
```

- **No templating step, no manifest.** One Rust binary reads TOML and SQL and produces the plan, and nothing is serialized to disk between phases. The `Project` struct stays in memory with the layers computed, so startup is 14 ms and config validation 15 ms.
- **Parallel type checking by layer.** Models in a layer are independent, so Rocky type-checks a whole layer across every core with [rayon](https://github.com/rayon-rs/rayon).
- **Compile-time analysis.** Type checking, lineage, drift detection and contract validation run at compile time, so a doomed run fails before it spends warehouse compute.
- **SQL-native incrementals.** Rocky writes the watermark `WHERE` clause into the generated SQL, so the warehouse filters with no extra round-trip.

## Why Rocky's memory stays flat

- **A single static binary.** `opt-level = 3`, `codegen-units = 1`, `panic = "abort"`. No interpreter, import chain or garbage collector.
- **String interning.** Identifiers like `catalog.schema.table` repeat thousands of times. Rocky interns them with [lasso](https://github.com/Kixiron/lasso): each unique string is stored once and referenced by an integer handle.
- **Memory-mapped file I/O.** Files over 4 KB use `mmap`, with no per-file string allocation.
- **An embedded state store.** Incremental state lives in an embedded [redb](https://github.com/cberner/redb) database, not in files to parse. That is why a warm compile (0.72 s) is 28% faster than a cold one.
- **Deterministic memory release.** Rust frees at scope exit: no collector pauses, no fragmentation.

Resource use scales with execution complexity, not project size.

## How Rocky and SQLMesh differ on speed

SQLMesh is architecturally closer to Rocky than most tools: it analyzes SQL
statically with [SQLGlot](https://github.com/tobymao/sqlglot) rather than
templating it. Four differences remain.

- **Single-threaded analysis.** SQLGlot parses and type-checks models
  sequentially in Python. Rocky type-checks a DAG layer in parallel with rayon.
- **Python AST overhead.** SQLGlot represents every SQL node as a Python object
  holding full strings. Rocky interns identifiers and uses Rust's zero-cost
  abstractions for the same analysis.
- **Snapshot state.** SQLMesh keeps snapshots that track environment state and
  grow with the project. Rocky keeps the graph in memory and persists only
  incremental watermarks in redb.
- **Per-invocation startup.** SQLMesh loads the Python interpreter, SQLGlot, and
  its dependency tree on every call. Rocky's static binary starts in 14 ms.

The suite above does not run SQLMesh. Treat these four points as architectural
differences, not as benchmarked ones.

## What these numbers mean

- **CI/CD.** Compile plus lineage takes **1.84 seconds**. For 10 engineers at 5 PR iterations a day, that is **9.3 hours** of waiting a year, about **$700** at $75/hr.
- **Orchestration.** Compilation uses **under 1%** of a 2-minute Dagster sensor cycle.
- **Container sizing.** A 10k-model compile fits a **512 MB** pod.
- **Iterative development.** After one file changes, a warm compile takes **0.72 seconds**.

## Scaling behavior

Rocky compiles linearly. Per-model cost is flat at **~100 µs** from 1k to 50k
models, verified across prior benchmark rounds. Memory scales linearly too.

| | 10k (measured) | 50k (extrapolated) |
|---|---:|---:|
| **Compile** | **1.00 s** | **~5.0 s** |
| **Peak memory** | **147 MB** | **~735 MB** |

At 50k models, Rocky stays well under 1 GB.

## Reproducing the benchmarks

The harness generates a synthetic project at the scale you ask for and exits with an error if a required tool is missing.

```bash
cd examples/playground/benchmarks
(cd ../../../engine && cargo build --release)
python3 -m venv .venv && .venv/bin/pip install dbt-core dbt-duckdb psutil matplotlib
python generate_dbt_project.py --scale 10000 --output-dir .
python run_benchmark.py --scale 10000 --iterations 3 --tool all --benchmark-type all \
  --rocky-bin ../../../engine/target/release/rocky
python visualize.py results/benchmark_*.json
```

## Round-by-round improvement

| Version | Compile (10k) | Per-model | Peak RSS |
|---|---:|---:|---:|
| Rocky 0.1.0 (Round 2) | 1.33 s | 133 µs | 116 MB |
| Rocky 0.3.0 (Round 3) | 1.20 s | 120 µs | 125 MB |
| Rocky 0.3.0 optimized (Round 4) | **1.00 s** | **100 µs** | 147 MB |

Compile time fell **25%** from Round 2 (v0.1.0) to Round 4 (v0.3.0 optimized).
The memory increase from 116 MB to 147 MB is a deliberate trade: caching and
pre-allocation buy 39% faster warm compiles for about 31 MB.

These figures were last captured at v0.3.0 (see
`examples/playground/benchmarks/REPORT_CURRENT.md`). The shipped engine has
advanced well beyond that release since. Re-run the suite against your current
engine (`engine/target/release/rocky`) to get numbers pinned to your release.
