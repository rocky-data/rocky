#!/usr/bin/env python3
"""PySpark logical plan construction benchmark.

PySpark is fundamentally a *runtime*, not a *compiler*. There is no separate
"compile project" phase like dbt or Rocky have. The closest analog is:

  1. Build N DataFrames programmatically with dependencies (matches our DAG)
  2. Force the Catalyst optimizer to construct logical/optimized/physical plans
  3. Measure how long that takes

This is conceptually different from what dbt/Rocky benchmark — they parse files
from disk and produce SQL strings. PySpark builds an in-process DataFrame DAG
and produces an executable Spark plan. But it's the closest "build the dependency
graph + validate + plan" workload Spark exposes.

We measure two things:

  - **Plan construction time**: Time to call `.queryExecution.optimizedPlan` on
    each leaf DataFrame. This is what Spark does at "compile time" — analyzing
    the logical plan and running Catalyst optimizations.
  - **Peak memory**: How much RSS Spark holds for N DataFrames.

We do NOT execute any of these DataFrames (no `.collect()`, `.show()`,
`.write()`). We only build the plan tree. This is the fairest analog to what
dbt/Rocky measure.

Usage:
    JAVA_HOME=/opt/homebrew/opt/openjdk@17 python3 bench_pyspark.py --scale 10000
"""

import argparse
import gc
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

# Set Spark to local mode with minimal logging
os.environ.setdefault("PYSPARK_SUBMIT_ARGS", "--master local[*] pyspark-shell")
os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# Import after env vars are set
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType, DecimalType


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("rocky-bench-pyspark")
        .master("local[*]")
        # Disable everything we don't need to keep this fair vs the others
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "1g")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def make_source_schema() -> StructType:
    """Schema matching the synthetic source tables in our generator."""
    return StructType([
        StructField("col_0", IntegerType(), True),
        StructField("col_1", StringType(), True),
        StructField("col_2", StringType(), True),
        StructField("col_3", DecimalType(10, 2), True),
        StructField("col_4", StringType(), True),
        StructField("col_5", TimestampType(), True),
        StructField("col_6", TimestampType(), True),
    ])


def build_dag(spark: SparkSession, n: int) -> dict[str, DataFrame]:
    """Build a 4-layer DataFrame DAG mirroring the dbt/Rocky synthetic project.

    All DataFrames are based on a single empty in-memory source schema (we don't
    care about data — only plan construction). Dependencies are chained via
    transformations.
    """
    dfs: dict[str, DataFrame] = {}
    schema = make_source_schema()

    n_sources = int(n * 0.30)
    n_staging = int(n * 0.30)
    n_intermediate = int(n * 0.25)
    n_marts = n - n_sources - n_staging - n_intermediate

    # Sources: empty DataFrame with the source schema
    # Each source applies CAST/aliasing matching our SQL templates
    base = spark.createDataFrame([], schema)
    for i in range(n_sources):
        df = base.select(
            F.col("col_0").alias("id"),
            F.col("col_1").alias("name"),
            F.col("col_2").cast("string").alias("category"),
            F.round(F.col("col_3").cast("decimal(10,2)"), 2).alias("amount"),
            F.col("col_4").alias("status"),
            F.col("col_5").alias("created_at"),
            F.col("col_6").alias("updated_at"),
        )
        dfs[f"src_{i:05d}"] = df

    # Staging: clean + filter + window function
    source_names = list(dfs.keys())
    staging_names = []
    from pyspark.sql.window import Window
    for i in range(n_staging):
        dep = source_names[i % len(source_names)]
        src = dfs[dep]
        df = (
            src
            .filter((F.col("status") != "deleted") & (F.col("amount") > 0))
            .select(
                F.col("id"),
                F.upper(F.trim(F.col("name"))).alias("name_clean"),
                F.coalesce(F.col("category"), F.lit("unknown")).alias("category_clean"),
                F.col("amount"),
                F.lower(F.col("status")).alias("status"),
                F.col("created_at"),
                F.col("updated_at"),
                F.row_number().over(Window.partitionBy("id").orderBy(F.col("updated_at").desc())).alias("rn"),
            )
        )
        name = f"stg_{n_sources + i:05d}"
        dfs[name] = df
        staging_names.append(name)

    # Intermediate: CTE-style join + group by with multiple aggregates
    intermediate_names = []
    for i in range(n_intermediate):
        dep1 = staging_names[i % len(staging_names)]
        dep2 = staging_names[(i + 1) % len(staging_names)]
        a = dfs[dep1].alias("a").filter(F.col("rn") == 1)
        b = dfs[dep2].alias("b").select(F.col("id"), F.col("category_clean"))

        # Every 10th intermediate has a 3rd dep
        if i % 10 == 0 and len(staging_names) > 2:
            dep3 = staging_names[(i + 2) % len(staging_names)]
            c = dfs[dep3].alias("c").select(F.col("id").alias("id_c"), F.col("category_clean").alias("cat_c"))
            joined = (
                a.join(b, "id", "inner")
                 .join(c, a["id"] == c["id_c"], "left")
                 .select(
                     a["id"],
                     a["name_clean"].alias("name"),
                     a["amount"],
                     a["created_at"],
                     F.coalesce(c["cat_c"], b["category_clean"]).alias("category"),
                 )
            )
        else:
            joined = (
                a.join(b, "id", "inner")
                 .select(
                     a["id"],
                     a["name_clean"].alias("name"),
                     a["amount"],
                     a["created_at"],
                     b["category_clean"].alias("category"),
                 )
            )

        df = (
            joined
            .groupBy("id", "name", "category")
            .agg(
                F.count("*").alias("record_count"),
                F.sum("amount").alias("total_amount"),
                F.avg("amount").alias("avg_amount"),
                F.min("created_at").alias("first_seen"),
                F.max("created_at").alias("last_seen"),
            )
        )
        name = f"int_{n_sources + n_staging + i:05d}"
        dfs[name] = df
        intermediate_names.append(name)

    # Marts: window functions + CASE + LEFT JOIN
    for i in range(n_marts):
        int_dep = intermediate_names[i % max(len(intermediate_names), 1)]
        stg_dep = staging_names[i % len(staging_names)]
        idx = n_sources + n_staging + n_intermediate + i

        i_df = dfs[int_dep].alias("i")
        s_df = dfs[stg_dep].alias("s").select("id")
        df = (
            i_df.join(s_df, "id", "left")
                .filter(F.col("record_count") >= 2)
                .select(
                    F.col("id"),
                    F.col("name"),
                    F.col("total_amount"),
                    F.col("record_count"),
                    F.col("avg_amount"),
                    F.when(F.col("total_amount") > 10000, "high")
                     .when(F.col("total_amount") > 1000, "medium")
                     .otherwise("low").alias("tier"),
                    F.sum("total_amount").over(
                        Window.orderBy("first_seen").rowsBetween(Window.unboundedPreceding, Window.currentRow)
                    ).alias("cumulative_total"),
                    F.rank().over(Window.orderBy(F.col("total_amount").desc())).alias("amount_rank"),
                )
        )
        dfs[f"fct_{idx:05d}"] = df

    return dfs


def force_plan_construction(dfs: dict[str, DataFrame]) -> int:
    """Force Spark to build the optimized logical plan for every DataFrame.

    This is the work that dbt/Rocky do at compile time: walk the DAG, validate
    references, type check, optimize.

    We access ._jdf.queryExecution().optimizedPlan() which forces Catalyst to
    analyze + optimize each plan without executing it.

    Returns count of plans built (for sanity check).
    """
    count = 0
    for name, df in dfs.items():
        # This walks: parsed -> analyzed -> optimized
        plan = df._jdf.queryExecution().optimizedPlan()
        # Touch the plan to make sure it's not lazy
        _ = plan.toString()
        count += 1
    return count


def measure_peak_rss_mb() -> float:
    """Get current process peak RSS in MB."""
    try:
        import resource
        # macOS reports in bytes, Linux in kilobytes
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        import platform
        if platform.system() == "Darwin":
            return rss / (1024 * 1024)
        return rss / 1024
    except Exception:
        return 0.0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scale", type=int, required=True, help="Number of DataFrames to build")
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--output-dir", type=str, default="results/round2_post_optimization")
    args = parser.parse_args()

    print(f"Starting Spark (this takes ~5-10s for JVM)...")
    spark_start = time.perf_counter()
    spark = make_spark()
    spark.sparkContext.setLogLevel("ERROR")
    spark_startup_s = time.perf_counter() - spark_start
    print(f"Spark started in {spark_startup_s:.2f}s")

    results = {
        "metadata": {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scale": args.scale,
            "iterations": args.iterations,
            "tool": "pyspark",
            "version": spark.version,
            "java_version": os.popen("java -version 2>&1").read().split("\n")[0] if os.path.exists("/opt/homebrew/opt/openjdk@17/bin/java") else "unknown",
            "spark_startup_s": round(spark_startup_s, 3),
            "note": "PySpark builds DataFrame plans in-process. This is conceptually different from dbt/Rocky which parse files from disk. The 'compile' here means: walk the DataFrame DAG, run Catalyst's analyzer + optimizer, and produce an optimized logical plan tree. We do NOT execute any DataFrames.",
        },
        "iterations": [],
    }

    # Warmup: build a smaller DAG to JIT-warm Catalyst
    print(f"Warmup (1k DataFrames)...")
    warmup_start = time.perf_counter()
    warmup_dfs = build_dag(spark, 1000)
    force_plan_construction(warmup_dfs)
    warmup_s = time.perf_counter() - warmup_start
    print(f"Warmup done in {warmup_s:.2f}s")
    del warmup_dfs
    gc.collect()

    print(f"\nBuilding + planning {args.scale:,} DataFrames × {args.iterations} iterations...")
    times = []
    build_times = []
    plan_times = []
    peak_rss = 0.0

    for i in range(args.iterations):
        gc.collect()

        # Build DAG
        t0 = time.perf_counter()
        dfs = build_dag(spark, args.scale)
        build_s = time.perf_counter() - t0

        # Force plan construction (the "compile" work)
        t1 = time.perf_counter()
        count = force_plan_construction(dfs)
        plan_s = time.perf_counter() - t1

        total_s = build_s + plan_s
        times.append(total_s)
        build_times.append(build_s)
        plan_times.append(plan_s)
        rss = measure_peak_rss_mb()
        peak_rss = max(peak_rss, rss)

        print(f"  iter {i+1}: build {build_s:.2f}s + plan {plan_s:.2f}s = {total_s:.2f}s  ({count} plans, RSS {rss:.0f} MB)")
        results["iterations"].append({
            "iteration": i,
            "build_s": round(build_s, 4),
            "plan_s": round(plan_s, 4),
            "total_s": round(total_s, 4),
            "plans_built": count,
            "peak_rss_mb": round(rss, 1),
        })
        del dfs
        gc.collect()

    results["mean_s"] = round(sum(times) / len(times), 4)
    results["min_s"] = round(min(times), 4)
    results["max_s"] = round(max(times), 4)
    results["mean_build_s"] = round(sum(build_times) / len(build_times), 4)
    results["mean_plan_s"] = round(sum(plan_times) / len(plan_times), 4)
    results["peak_rss_mb"] = round(peak_rss, 1)

    out = Path(args.output_dir) / f"pyspark_{args.scale}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(results, indent=2))

    print(f"\nResults: mean {results['mean_s']:.2f}s (build {results['mean_build_s']:.2f}s + plan {results['mean_plan_s']:.2f}s), peak RSS {results['peak_rss_mb']:.0f} MB")
    print(f"Saved to: {out}")

    spark.stop()


if __name__ == "__main__":
    main()
