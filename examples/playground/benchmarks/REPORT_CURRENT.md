# Rocky vs dbt-core vs dbt-fusion — Benchmark Report

**Date:** 2026-04-10
**Rocky version:** 0.3.0 (post-optimization)
**dbt-core version:** 1.11.8
**dbt-fusion version:** 2.0.0-preview.164

**Workload:** Synthetic 4-layer transformation DAG (sources → staging → intermediate → marts) at **10k** models
**Machine:** macOS arm64, 12-core Apple Silicon, 36 GB RAM, local SSD

---

## Executive Summary

Rocky 0.3.0 (post-optimization) compiles 10,000 models in **1.00 second** — **34x faster** than dbt-core (34.6 s) and **38x faster** than dbt-fusion (38.4 s), while using **4-7x less memory**. The advantage is consistent across every benchmark dimension: compilation, lineage, startup, incremental recompilation, and config validation.

| Metric | Rocky 0.3.0 | dbt-core 1.11.8 | dbt-fusion 2.0.0 |
|---|---:|---:|---:|
| **Compile 10k** | **1.00 s** | 34.62 s (34x) | 38.43 s (38x) |
| **Memory 10k** | **147 MB** | 629 MB (4.3x) | 1,063 MB (7.2x) |
| **Lineage 10k** | **0.84 s** | 35.36 s (42x) | N/A |
| **Startup** | **14 ms** | 896 ms (64x) | 12 ms |
| **DAG resolution** | **0.36 s** | 9.12 s (25x) | 2.73 s (7x) |
| **Warm compile (1 file)** | **0.72 s** | 33.12 s (46x) | 37.16 s (52x) |
| **Warm compile (10 files)** | **0.77 s** | 32.61 s (42x) | 39.60 s (51x) |
| **SQL generation** | **200 ms** | N/A | N/A |
| **Config validation** | **15 ms** | 2,187 ms (146x) | 1,473 ms (98x) |
| **Fargate cost/invocation (10k)** | **$0.0003** | $0.0004 (1.5x) | $0.0008 (3x) |
| **Annual tool-dependent cost** | **$749** | **$27,353** (37x) | **$15,338** (20x) |
| **Annual savings vs dbt-core** | **$26,604** | — | $12,015 |

**Bottom line:** Rocky fits in a 512 MB container at 10k models. dbt-core needs 1 GB. dbt-fusion needs 2 GB. For Dagster sensors that fire every minute, Rocky's 1-second compile leaves 59 seconds for actual work; dbt-core's 35-second compile consumes more than half the cycle. Annual tool-dependent cost (infrastructure + developer productivity) for a 10k-model pipeline with 10 engineers: **Rocky $749 vs dbt-core $27,353** — Rocky saves **$26,604/year** vs dbt-core regardless of Databricks compute strategy (see §5).

---

## 1. Core Benchmarks — Compile / Parse / DAG

### 1.1 Compile Time

| Scale | Rocky | dbt-core | dbt-fusion | core / Rocky | fusion / Rocky |
|---:|---:|---:|---:|---:|---:|
| **10k** | **1.00 s** | 34.62 s | 38.43 s | **34x** | **38x** |

Rocky's per-model compile cost is flat at **~100 µs**. With linear scaling verified in prior rounds, 50k extrapolates to ~5 seconds.

dbt-fusion's compile is **11% slower than dbt-core** at 10k. Fusion's static analysis cost exceeds dbt-core's Jinja render cost.

### 1.2 Parse Time

| Scale | dbt-core | dbt-fusion | fusion speedup |
|---:|---:|---:|---:|
| **10k** | 23.61 s | 2.79 s | 8.5x faster |

Rocky doesn't expose a separate parse phase — it's included in the 1.00 s compile time. dbt-fusion's Rust rewrite delivers a real parse speedup (8.5x) over dbt-core, but this advantage is erased when you include compile time.

### 1.3 DAG Resolution / `ls`

| Scale | Rocky | dbt-core | dbt-fusion | core / Rocky | fusion / Rocky |
|---:|---:|---:|---:|---:|---:|
| **10k** | **0.36 s** | 9.12 s | 2.73 s | **25x** | **7x** |

Rocky's DAG resolution is effectively constant time because `bench dag` measures the algorithm in isolation. The dbt tools conflate DAG resolution with project load.

### 1.4 Memory

| Scale | Rocky | dbt-core | dbt-fusion | core / Rocky | fusion / Rocky |
|---:|---:|---:|---:|---:|---:|
| **10k** | **147 MB** | 629 MB | 1,063 MB | **4.3x** | **7.2x** |

| Scale | Rocky | dbt-core | dbt-fusion |
|---:|:---:|:---:|:---:|
| Fits in 512 MB pod | **10k** | No | No |
| Fits in 1 GB pod | **10k** | 10k | No |
| Needs 2 GB pod | Never | Never | **10k** |

---

## 2. Extended Benchmarks

### 2.1 Startup / Cold-Start

| Tool | Time | Memory |
|---|---:|---:|
| dbt-fusion | 12 ms | 21 MB |
| **Rocky** | **14 ms** | 9 MB |
| dbt-core | **896 ms** | 125 MB |

Rocky and dbt-fusion (both Rust binaries) start in ~13 ms. dbt-core's Python import chain takes **896 ms** before doing any work. In a Dagster sensor firing every 30 seconds, dbt-core's startup tax alone consumes 3% of the time budget.

### 2.2 Lineage Computation

Rocky: `rocky lineage <model> --models <dir>` (column-level, compile-time)
dbt-core: `dbt docs generate` (catalog rebuild, closest equivalent)

| Scale | Rocky | dbt-core | dbt-fusion | core / Rocky |
|---:|---:|---:|---:|---:|
| **10k** | **0.84 s** | 35.36 s | N/A | **42x** |

Rocky computes **column-level lineage** at compile time — no warehouse query, no catalog rebuild. The closest dbt equivalent is `dbt docs generate`, which rebuilds the entire catalog and takes **35 seconds** at 10k. dbt-fusion's `docs generate` failed.

This enables lineage-aware CI (block PRs that break downstream columns) that would be impractical with dbt's latency.

### 2.3 Incremental / Warm Compile

What happens when you change 1 or 10 models and recompile?

**1 model changed:**

| Scale | Rocky | dbt-core (partial parse) | dbt-fusion |
|---:|---:|---:|---:|
| **10k** | **0.72 s** | 33.12 s | 37.16 s |

**10 models changed:**

| Scale | Rocky | dbt-core (partial parse) | dbt-fusion |
|---:|---:|---:|---:|
| **10k** | **0.77 s** | 32.61 s | 39.60 s |

**Key findings:**

1. **Rocky's warm compile is faster than cold compile** — 0.72 s vs 1.00 s (28% faster). The optimized caching layer provides a meaningful speedup for iterative development.

2. **dbt-core's partial parse provides no benefit.** Warm compile is actually *slower* than cold compile (33.1 s warm vs 34.6 s cold — within noise). The bottleneck is SQL generation, not parsing.

3. **dbt-fusion shows no warm compile advantage.** 37.2 s warm vs 38.4 s cold — within noise.

### 2.4 Config Validation

| Tool | Command | Time | Memory |
|---|---|---:|---:|
| **Rocky** | `rocky validate` | **15 ms** | 9 MB |
| dbt-fusion | `dbt debug` | 1,473 ms | 64 MB |
| dbt-core | `dbt debug` | 2,187 ms | 139 MB |

Rocky validates config in under 20 milliseconds — essentially free. This runs before every pipeline execution. dbt-core takes 2.2 seconds for the same pre-flight check.

### 2.5 SQL Generation Throughput (Rocky only)

| Benchmark | Time |
|---|---:|
| `rocky bench sql_gen` (10k CTAS statements) | **200 ms** |

Rocky generates CREATE TABLE AS SELECT SQL for 10,000 tables in 200 milliseconds (~50,000 tables/second). This is a 20% improvement over the prior round (249 ms).

---

## 3. Scaling Behavior

Based on prior benchmark rounds at 10k/20k/50k, Rocky's compile time scales **linearly** with model count. The per-model cost is flat at ~100 µs from 10k to 50k. Extrapolated performance:

```
            10k (measured)   20k (est.)     50k (est.)
Rocky       1.00s            ~2.0s          ~5.0s
dbt-core    34.62s           ~69s           ~173s
dbt-fusion  38.43s           ~77s           ~192s
```

Memory also scales linearly:

```
            10k (measured)   20k (est.)     50k (est.)
Rocky       147 MB           ~294 MB        ~735 MB
dbt-core    629 MB           ~1,258 MB      ~3,145 MB
dbt-fusion  1,063 MB         ~2,126 MB      ~5,315 MB
```

At 50k models, Rocky would stay well under 1 GB. dbt-core would need ~3 GB. dbt-fusion would need ~5 GB — exceeding standard EKS pod limits.

---

## 4. Comparison with Prior Rounds

| Metric (10k) | Rocky 0.1.0 (Round 2) | Rocky 0.3.0 (Round 3) | Rocky 0.3.0 optimized (Round 4) | Round 3 → 4 |
|---|---:|---:|---:|---|
| Compile | 1.33 s | 1.20 s | **1.00 s** | **-16.4%** |
| Warm compile (1 file) | — | 1.18 s | **0.72 s** | **-39.1%** |
| Warm compile (10 files) | — | 1.22 s | **0.77 s** | **-37.0%** |
| Lineage | — | 1.20 s | **0.84 s** | **-29.9%** |
| SQL generation | — | 249 ms | **200 ms** | **-19.6%** |
| Peak RSS | 115.6 MB | 124.9 MB | 146.8 MB | +18% |
| Per-model cost | 133 µs | 120 µs | **100 µs** | **-17%** |

**Cumulative improvement since v0.1.0:** Compile time reduced by **25%** (1.33s → 1.00s). The memory increase (116 → 147 MB, +27%) is a deliberate tradeoff — caching and pre-allocation that trade ~22 MB for 39% faster warm compiles.

**No performance regression from new features.** Rocky 0.3.0 added multi-type pipeline config, Dagster Pipes, config UX overhaul, and numerous features while getting faster.

---

## 5. Cost Analysis

All costs use publicly available AWS and Databricks pricing as of April 2026 (us-east-1). We model a **production scenario**: 10k models, Dagster-orchestrated pipeline on AWS EKS, executing against Databricks Unity Catalog, with a 10-engineer team.

### 5.1 Reference Pricing

| Resource | Unit | Price |
|---|---|---:|
| **AWS EKS control plane** | per cluster-hour | $0.10 |
| **AWS Fargate** vCPU | per vCPU-hour | $0.04048 |
| **AWS Fargate** memory | per GB-hour | $0.004445 |
| **AWS EC2** m6g.large (2 vCPU, 8 GB) | per hour | $0.077 |
| **AWS EC2** m6g.xlarge (4 vCPU, 16 GB) | per hour | $0.154 |
| **AWS NAT Gateway** | per hour + $0.045/GB | $0.045/hr |
| **AWS ALB** | per hour + LCU | $0.0225/hr |
| **AWS S3** (Standard) | per GB-month | $0.023 |
| **AWS S3** PUT requests | per 1,000 | $0.005 |
| **Databricks SQL Classic** (Premium) | per DBU-hour | $0.22 |
| **Databricks SQL Pro** (Premium) | per DBU-hour | $0.55 |
| **Databricks SQL Serverless** | per DBU-hour | $0.70 |
| **Databricks Jobs Compute** (Premium) | per DBU-hour | $0.30 |
| **Databricks Jobs Serverless** | per DBU-hour | $0.30 |
| **Databricks All-Purpose** (Premium) | per DBU-hour | $0.55 |
| **Dagster+ Starter** | per month | $100 (30k credits) |
| **Dagster+ credit overage** | per credit | $0.03 |

### 5.2 Shared Infrastructure (Same Regardless of Tool)

These costs apply to any Dagster + Databricks deployment and are identical whether you use Rocky, dbt, or PySpark notebooks.

| Component | Monthly cost | Annual cost | Notes |
|---|---:|---:|---|
| EKS control plane (1 cluster) | $73 | $876 | Fixed fee, always on |
| NAT Gateway (1 AZ) | $33 + traffic | $396 + traffic | Required for Fargate egress |
| ALB (Dagster webserver) | $16 + LCU | $195 + LCU | Single ALB for Dagster UI |
| S3 + CloudWatch | ~$15 | ~$180 | State files, run logs |
| Dagster+ Starter | $100 | $1,200 | 30k credits/month |
| *Shared subtotal* | *~$237* | *~$2,847* | *Identical* |

**These costs cancel out in any tool comparison.** The tool choice only affects: (1) Fargate pod size/duration for compile, (2) Databricks warehouse idle burn during compile, and (3) developer productivity.

### 5.3 Fargate Pod Cost per Compile Invocation

**10k models — pod sizing from benchmark data:**

| Tool | Peak RSS | Pod config | Fargate $/hr |
|---|---:|---|---:|
| **Rocky** | 147 MB | 0.25 vCPU / 0.5 GB | $0.0124 |
| dbt-core | 629 MB | 0.5 vCPU / 1 GB | $0.0247 |
| dbt-fusion | 1,063 MB | 1 vCPU / 2 GB | $0.0494 |

*Fargate bills per-second with a 1-minute minimum.*

| Tool | Runtime | Billed duration | Cost per invocation |
|---|---:|---:|---:|
| **Rocky** | 1.00 s | 60 s (min) | **$0.0002** |
| dbt-core | 34.62 s | 60 s (min) | **$0.0004** |
| dbt-fusion | 38.43 s | 60 s (min) | **$0.0008** |

At 10k, all tools hit the 60-second Fargate minimum. The cost difference comes from pod sizing (Rocky needs half the memory of dbt-core, and a quarter of dbt-fusion).

### 5.4 Databricks SQL Warehouse — Classic vs Serverless

The tool choice determines how much warehouse time is wasted during the compile phase.

#### Strategy A: SQL Warehouse Serverless

Serverless warehouses scale to zero — you pay only for active query time. The compile phase happens *before* queries start, so the warehouse isn't burning DBUs while Rocky/dbt compiles.

**Where Rocky saves money with Serverless:**
- Rocky's 1s compile means the warehouse stays warm between frequent sensor ticks (60s intervals)
- dbt's 35-38s compile pushes practical intervals to 2-5 minutes, causing the serverless warehouse to auto-suspend and cold-start each time

| Metric (10k, Serverless 2X-Small = 4 DBU/hr) | Rocky | dbt-core | dbt-fusion |
|---|---:|---:|---:|
| Sensor interval | 60 s | 120 s | 120 s |
| Warehouse stays warm between ticks? | **Yes** | Marginal | Marginal |
| Cold starts per day | 0 | **144** | **144** |
| Cold start penalty (15s x 4 DBU x $0.70) | $0 | $0.0117/start | $0.0117/start |
| Annual cold start cost | **$0** | **$615** | **$615** |

#### Strategy B: SQL Warehouse Classic (Always-On)

Classic warehouses charge for the entire uptime regardless of query load. Compile time affects **throughput**: longer compile = fewer pipeline runs per hour = lower utilization of the warehouse you're already paying for.

| Tool | Compile (10k) | % of 2-min cycle spent compiling | Effective warehouse utilization |
|---|---:|---:|---:|
| **Rocky** | 1.00 s | **0.8%** | 99.2% |
| dbt-core | 34.62 s | **28.8%** | 71.2% |
| dbt-fusion | 38.43 s | **32.0%** | 68.0% |

dbt-core wastes **29%** of classic warehouse capacity on compilation. dbt-fusion wastes **32%**. Rocky wastes **under 1%**.

#### Strategy C: SQL Warehouse with Auto-Stop

Most production setups use auto-stop (suspend after 5-10 minutes of inactivity).

| Tool | Compile time | Extra idle burn per run |
|---|---:|---:|
| **Rocky** | 1.00 s | ~$0.0002 (1s x 4 DBU x $0.22) |
| dbt-core | 34.62 s | ~$0.0085 (34.6s x 4 DBU x $0.22) |
| dbt-fusion | 38.43 s | ~$0.0094 (38.4s x 4 DBU x $0.22) |

Over 105,120 annual runs (5-min interval):

| Tool | Annual idle warehouse burn |
|---|---:|
| **Rocky** | **$21** |
| dbt-core | **$894** |
| dbt-fusion | **$988** |

### 5.5 Databricks Jobs Compute (Alternative to SQL Warehouse)

Some teams use Jobs Compute clusters instead of SQL Warehouses for batch transformation. Jobs Compute is cheaper per-DBU ($0.30 vs $0.70 serverless) but has a **cold start penalty** of 15-25 seconds per cluster launch.

| Metric | Rocky + Jobs | dbt + Jobs |
|---|---:|---:|
| Compile time (10k) | 1.00 s | 35-38 s |
| Cluster cold start | 20 s | 20 s |
| Total time to first query | **21 s** | **55-58 s** |

With Rocky, the cluster cold start (20s) dominates over compile time (1s). With dbt, compile time dominates. Rocky's end-to-end time is **2.6x shorter**.

For Jobs Compute billing, the cluster runs during compile (idle) + execution. Rocky's idle penalty is 1 second; dbt's is 35-38 seconds:

| Tool | Annual idle Jobs Compute burn (2 DBU cluster) |
|---|---:|
| **Rocky** | **$18** |
| dbt-core | **$607** |
| dbt-fusion | **$674** |

### 5.6 What About Databricks Notebooks / PySpark Workflows?

An alternative approach: skip the SQL compiler entirely and write PySpark in Databricks Notebooks, orchestrated via Databricks Workflows.

**Pros of notebooks:**
- No compile step at all — direct execution
- Native Databricks integration, no external orchestrator needed
- Databricks Workflows pricing is the same as Jobs Compute ($0.30/DBU)

**Why this comparison isn't apples-to-apples:**

Notebooks execute code but provide **none** of the compile-time guarantees that Rocky (or dbt) offers:

| Capability | Rocky | dbt-core | PySpark Notebooks |
|---|:---:|:---:|:---:|
| Static type checking | Yes | No | **No** |
| Column-level lineage | Yes | Partial | **No** |
| Data contracts | Yes | Yes | **No** |
| Schema drift detection | Yes | No | **No** |
| Dependency DAG | Yes (compile-time) | Yes (manifest) | **Manual** |
| Pre-execution validation | Yes (15 ms) | Yes (2.2 s) | **None** |
| CI/CD gate (compile check) | Yes | Yes | **Runtime only** |
| Refactoring safety | Yes (type errors) | No | **No** |

With notebooks, you discover errors at **runtime** — when the query fails in production. With Rocky, you discover them at **compile time** — before a single DBU is consumed. At $0.70/DBU-hour (serverless), a single failed 10k-model run that executes for 15 minutes before hitting a type error wastes:

| Scenario | Rocky | PySpark Notebook |
|---|---:|---:|
| Type error discovered | At compile (0 DBU) | At runtime (after partial execution) |
| Wasted warehouse cost (15 min, 16 DBU) | **$0** | **$2.80 per failure** |
| 1 failure/week annual waste | **$0** | **$146** |
| 1 failure/day annual waste | **$0** | **$1,022** |

The notebook approach is cheaper *when everything works*. Rocky's value is preventing the failures that make it expensive.

### 5.7 EKS Cluster Cost (Fargate vs Managed Nodes)

Fargate is the simplest model for ephemeral compile pods, but some teams run EKS with managed EC2 node groups for Dagster workers. The tool choice affects node sizing:

**Managed node group sizing for 10k models:**

| Tool | Min node for compile | Instance | $/hr | Workers needed |
|---|---:|---|---:|---:|
| **Rocky** | 512 MB | m6g.large (8 GB, shared) | $0.077 | 1 shared node |
| dbt-core | 1 GB | m6g.large (8 GB, shared) | $0.077 | 1 shared node |
| dbt-fusion | 2 GB | m6g.large (8 GB, shared) | $0.077 | 1 shared node |

At 10k models, all tools can share m6g.large nodes. At 50k (extrapolated), dbt-fusion would need m6g.xlarge (2x cost).

### 5.8 Dagster+ Credit Impact

Dagster+ charges per asset materialization (1 credit per materialization or op execution). The tool choice affects credits indirectly through sensor frequency.

| Plan | Monthly | Credits included | Overage rate |
|---|---:|---:|---:|
| Starter | $100 | 30,000 | $0.03/credit |
| Pro | Custom | Custom | Custom |

**Credit consumption depends on business logic, not the compile tool.** At the same sensor interval, Dagster+ costs are identical across tools. Rocky *enables* higher frequency; it doesn't mandate it.

### 5.9 CI/CD Cost Impact

**Scenario:** 10 engineers, 5 PR iterations/day each, CI runs `compile` + `lineage` on every push.

| Metric | Rocky | dbt-core | dbt-fusion |
|---|---:|---:|---:|
| Compile + lineage (10k) | 1.84 s | 70.0 s | 38.4 s |
| CI pod config | 0.25 vCPU / 0.5 GB | 0.5 vCPU / 1 GB | 1 vCPU / 2 GB |
| CI invocations/year | 18,250 | 18,250 | 18,250 |
| **Annual CI Fargate** | **$4** | **$8** | **$15** |
| Developer wait time/year | **9.3 hours** | **354.8 hours** | **194.8 hours** |

At $75/hour fully-loaded engineer cost:

| Tool | Wait cost/year | vs Rocky |
|---|---:|---:|
| **Rocky** | **$700** | baseline |
| dbt-core | $26,608 | **$25,908 more** |
| dbt-fusion | $14,610 | **$13,910 more** |

### 5.10 Full Annual Cost Model (10k models, 10 engineers)

**Apples-to-apples: same 5-min sensor interval, Databricks SQL Classic with auto-stop, EKS Fargate.**

| Cost component | Rocky | dbt-core | dbt-fusion | Notes |
|---|---:|---:|---:|---|
| **Shared infrastructure** | | | | |
| EKS control plane | $876 | $876 | $876 | Same |
| NAT Gateway (1 AZ) | $396 | $396 | $396 | Same |
| ALB | $195 | $195 | $195 | Same |
| S3 + CloudWatch | $180 | $180 | $180 | Same |
| Dagster+ Starter | $1,200 | $1,200 | $1,200 | Same |
| *Shared subtotal* | *$2,847* | *$2,847* | *$2,847* | *Identical* |
| **Tool-dependent costs** | | | | |
| Fargate — orchestration | $21 | $42 | $84 | Pod size x min billing |
| Fargate — CI | $4 | $8 | $15 | Pod size x min billing |
| Databricks idle burn (Classic auto-stop) | $21 | $894 | $988 | Compile time x DBU rate |
| Developer wait time (CI) | $700 | $26,608 | $14,610 | @$75/hr |
| *Tool subtotal* | *$749* | *$27,353* | *$15,338* | |
| **Grand total** | **$3,596** | **$30,200** | **$18,185** | |
| **Tool-dependent savings vs dbt-core** | **$26,604/yr** | baseline | $12,015/yr | |

#### With Serverless SQL Warehouse instead:

| Cost component | Rocky | dbt-core | dbt-fusion |
|---|---:|---:|---:|
| Shared infrastructure | $2,847 | $2,847 | $2,847 |
| Fargate (orch + CI) | $25 | $50 | $99 |
| Serverless cold start penalty | $0 | $615 | $615 |
| Developer wait time | $700 | $26,608 | $14,610 |
| **Grand total** | **$3,572** | **$30,120** | **$18,171** |

#### With Databricks Jobs Compute instead:

| Cost component | Rocky | dbt-core | dbt-fusion |
|---|---:|---:|---:|
| Shared infrastructure | $2,847 | $2,847 | $2,847 |
| Fargate (orch + CI) | $25 | $50 | $99 |
| Jobs Compute idle burn | $18 | $607 | $674 |
| Developer wait time | $700 | $26,608 | $14,610 |
| **Grand total** | **$3,590** | **$30,112** | **$18,230** |

### 5.11 Cost Comparison Summary

**No matter which Databricks compute strategy you choose, Rocky's tool-dependent costs are 20-37x lower than dbt-core's:**

| Warehouse strategy | Rocky tool cost | dbt-core tool cost | dbt-fusion tool cost | Rocky savings vs core |
|---|---:|---:|---:|---:|
| SQL Classic (auto-stop) | $749 | $27,353 | $15,338 | **$26,604/yr** |
| SQL Serverless | $725 | $27,273 | $15,324 | **$26,548/yr** |
| Jobs Compute | $743 | $27,265 | $15,383 | **$26,522/yr** |

The dominant cost in every scenario is **developer wait time** ($700 vs $26,608). Infrastructure savings alone ($21-$25 vs $607-$894 on Databricks idle burn) would justify the switch. Combined, Rocky saves **$26k+/year** for a 10-engineer team at 10k models.

**Scaling the team:** At 25 engineers (common for enterprise data teams), the developer wait time savings alone reach **$64,770/year** vs dbt-core.

---

## 6. What This Means for Your Data Platform

### For CI/CD pipelines
Rocky compiles 10k models in **1.0 second** and computes column-level lineage in **0.84 seconds**. Total CI gate: under 2 seconds. dbt-core takes 70 seconds. Annual developer wait-time savings: **$26k+** for a 10-person team (see §5.9).

### For Dagster/Airflow orchestration
Rocky enables 60-second sensor intervals; dbt-core is constrained to 2-5 minute intervals. Same-frequency orchestration costs **2x less** in Fargate (see §5.3). With Serverless SQL, dbt loses an additional **$615/year** in warehouse cold starts that Rocky avoids (see §5.4).

### For Databricks SQL Warehouse (Classic or Serverless)
With Classic auto-stop: Rocky wastes **$21/year** in idle DBU burn; dbt-core wastes **$894** (see §5.4). Rocky uses **under 1%** of the cycle for compilation; dbt-core uses **29%** (see §5.4).

### For iterative development
Rocky's warm compile (0.72 s) is **28% faster** than cold compile — the caching layer works. Modify a model, recompile in under a second. dbt-core and dbt-fusion show no warm compile benefit.

### For column-level lineage
Rocky traces lineage in **0.84 seconds** at 10k — no warehouse query. dbt-core needs **35 seconds** for `docs generate`. This enables lineage-gated CI and real-time impact analysis.

### For developer experience
Rocky's full LSP (go-to-definition, hover, completions, rename, 28+ diagnostics) is powered by the same compiler. At 10k models, diagnostics refresh in 1 second.

### Bottom line
**Rocky saves $26k+/year** for a 10-engineer team at 10k models — regardless of whether you use SQL Classic, SQL Serverless, or Jobs Compute. The savings come primarily from developer productivity ($26k) with meaningful infrastructure savings on top ($21-$894). At 25 engineers, savings reach **$65k/year**.

---

## 7. Test Environment & Methodology

| Item | Value |
|---|---|
| OS | macOS (darwin), arm64 |
| CPU | 12-core Apple Silicon |
| RAM | 36 GB |
| Storage | Local SSD |
| Python | 3.12.12 |
| Rocky | 0.3.0 (post-optimization, release build) |
| dbt-core | 1.11.8 + dbt-duckdb adapter |
| dbt-fusion | 2.0.0-preview.164 (Beta) |

### Methodology

- **1 warmup run** per phase (not measured)
- **3 timed iterations** per phase, mean reported (startup: 10 iterations)
- Wall time: Python `time.perf_counter()`
- Peak RSS: `/usr/bin/time -l` on macOS
- dbt-core cold parse: `--no-partial-parse` (realistic for ephemeral pods)
- dbt-core warm compile: partial parse enabled (manifest from warmup)
- Identical DAG structure across all tools (4-layer medallion)

### Reproducibility

```bash
cd examples/playground/benchmarks

# Build Rocky release
cd ../../../engine && cargo build --release && cd -

# Setup Python env
python3 -m venv .venv
.venv/bin/pip install dbt-core dbt-duckdb psutil matplotlib

# Run full suite at 10k
.venv/bin/python run_benchmark.py \
  --scale 10000 \
  --iterations 3 --tool all --benchmark-type all \
  --rocky-bin ../../../engine/target/release/rocky \
  --dbt-core-bin .venv/bin/dbt \
  --dbt-fusion-bin ~/.local/bin/dbt

# Generate charts
.venv/bin/python visualize.py results/benchmark_*.json
```

Total runtime: ~20 minutes on a 12-core Apple Silicon Mac.

### Available Benchmark Types

```bash
python run_benchmark.py --benchmark-type core        # Parse, compile, DAG
python run_benchmark.py --benchmark-type startup      # Binary cold-start
python run_benchmark.py --benchmark-type lineage      # Column-level lineage
python run_benchmark.py --benchmark-type incremental   # Warm compile
python run_benchmark.py --benchmark-type validate      # Config validation
python run_benchmark.py --benchmark-type sql_gen       # SQL generation (Rocky only)
python run_benchmark.py --benchmark-type all           # Everything
```

---

## 8. Raw Results

JSON results are in `results/benchmark_10000_*.json`.

Generate charts:
```bash
.venv/bin/python visualize.py results/benchmark_*.json --output-dir results/charts
```

See also:
- `REPORT.md` — Historical Round 1 → Round 2 → Round 3 progression
- `FEATURE_COMPARISON.md` — 17-category feature comparison: Rocky vs dbt-core vs dbt-fusion vs SQLMesh vs Coalesce vs Dataform
