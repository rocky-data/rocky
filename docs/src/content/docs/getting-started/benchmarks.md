---
title: Performance Benchmarks
description: How fast Rocky compiles, how much memory it uses, and how to reproduce the numbers yourself.
sidebar:
  order: 4.6
---

Rocky compiles 10,000 models in **1.00 second** and holds peak memory at
**147 MB**. This page gives the full measurement table, explains what makes the
compile loop fast, and shows the commands that reproduce every number.

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

Benchmarked on Apple Silicon (12-core, 36 GB RAM) with a synthetic 4-layer
medallion DAG. 3 iterations per benchmark, mean reported. Full methodology in
`examples/playground/benchmarks/REPORT_CURRENT.md`.

Rocky does not make the warehouse faster. It cuts the compile-plan-execute
overhead around it. The SQL execution time itself is unchanged.

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

**No templating step, no manifest.** A single compiled Rust binary reads TOML
and SQL and produces the execution plan. Nothing sits between you and your SQL,
and nothing is serialized to disk between phases. Rocky's `Project` struct stays
in process memory with the execution layers already computed. Startup is 14 ms
and config validation is 15 ms because there is nothing to load first.

**Parallel type checking by execution layer.** Models inside a layer have no
dependencies on each other, so Rocky type-checks a whole layer at once across
every CPU core, using [rayon](https://github.com/rayon-rs/rayon).

**Compile-time analysis, not runtime discovery.** Type checking, lineage
extraction, schema drift detection, and contract validation all run at compile
time. A run that was going to fail fails before it spends warehouse compute.

**SQL-native incrementals.** Rocky writes the watermark `WHERE` clause straight
into the generated SQL, so the warehouse does the filtering. There is no extra
round-trip to compute the boundary first.

## Why Rocky's memory stays flat

**A single static binary.** Rocky compiles with `opt-level = 3`,
`codegen-units = 1`, and `panic = "abort"`. There is no interpreter to load, no
import chain, and no garbage collector.

**String interning.** In a large project, identifiers like
`catalog.schema.table` repeat thousands of times across model metadata. Rocky
interns them with [lasso](https://github.com/Kixiron/lasso). Each unique string
is stored once, and every reference is a cheap integer handle.

**Memory-mapped file I/O.** For files larger than 4 KB, Rocky uses `mmap` and
lets the operating system schedule the pages. No per-file string allocation.

**An embedded state store.** Rocky keeps incremental state in an embedded
[redb](https://github.com/cberner/redb) transactional database, not in files it
must load and parse. Atomic reads from redb are why a warm compile (0.72 s) is
28% faster than a cold one.

**Deterministic memory release.** Rust frees memory at scope exit. There are no
collector pauses and no fragmentation over a long run.

Rocky's resource use scales with execution complexity, not project size. More
parallel layers means more threads, but memory stays flat because of interning
and `mmap`.

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

### For CI/CD

Rocky compiles and traces lineage in **1.84 seconds**. For a 10-engineer team
running 5 PR iterations a day each, that is **9.3 hours** of waiting a year,
about **$700** at $75/hr.

### For orchestration

Rocky supports 60-second Dagster sensor intervals. Compilation uses **under 1%**
of a 2-minute sensor cycle, so the sensor spends its time on work rather than on
startup.

### For container sizing

A 10k-model Rocky compile fits inside a **512 MB** pod.

### For iterative development

After you change one file, a warm compile takes **0.72 seconds**, 28% faster
than a cold compile.

## Scaling behavior

Rocky compiles linearly. Per-model cost is flat at **~100 µs** from 1k to 50k
models, verified across prior benchmark rounds. Memory scales linearly too.

| | 10k (measured) | 50k (extrapolated) |
|---|---:|---:|
| **Compile** | **1.00 s** | **~5.0 s** |
| **Peak memory** | **147 MB** | **~735 MB** |

At 50k models, Rocky stays well under 1 GB.

## Annual cost model (10k models, 10 engineers)

The scenario: a 5-minute sensor interval, Databricks SQL Classic with auto-stop,
and EKS Fargate.

| Cost component | Rocky |
|---|---:|
| Fargate (orchestration + CI) | $25 |
| Databricks idle burn | $21 |
| Developer wait time (CI) | $700 |
| **Tool-dependent subtotal** | **$749** |
| Shared infrastructure | $2,847 |
| **Total** | **$3,596** |

Shared infrastructure is the part any transformation tool pays. The subtotal is
the part that depends on how fast the tool is. Every figure here comes from
`examples/playground/benchmarks/REPORT_CURRENT.md`, including the subtotal; it
is that report's number, not a sum of the three rows above it.

## Reproducing the benchmarks

The harness generates a synthetic project at the scale you ask for, then runs
the benchmarks against it. The run exits with an error if a required tool is
missing.

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
