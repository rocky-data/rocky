#!/usr/bin/env python3
"""Execution benchmarks — measure actual SQL execution against DuckDB.

Unlike the core benchmarks (which only measure compile time), this suite
measures end-to-end performance: compile -> SQL gen -> execute against DuckDB.

Usage:
    python bench_execution.py --scale 1000 --rocky-bin ../../../engine/target/release/rocky
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def generate_execution_project(output_dir: str, num_models: int) -> None:
    """Generate a Rocky project with DuckDB-executable models."""
    os.makedirs(output_dir, exist_ok=True)

    # rocky.toml
    config = f"""[adapter.local]
type = "duckdb"
path = "{output_dir}/bench.duckdb"

[pipeline.bench]
type = "transformation"
models = "models/**"

[pipeline.bench.target]
adapter = "local"

[pipeline.bench.execution]
concurrency = 8
"""
    with open(os.path.join(output_dir, "rocky.toml"), "w") as f:
        f.write(config)

    # Seed data
    seed_dir = os.path.join(output_dir, "data")
    os.makedirs(seed_dir, exist_ok=True)
    with open(os.path.join(seed_dir, "seed.sql"), "w") as f:
        f.write("""
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.orders AS
SELECT
    i AS id,
    'customer_' || (i % 100) AS customer_id,
    (random() * 1000)::DECIMAL(10,2) AS amount,
    CASE WHEN random() > 0.1 THEN 'completed' ELSE 'cancelled' END AS status,
    CURRENT_TIMESTAMP - INTERVAL (i || ' hours') AS created_at
FROM generate_series(1, 10000) AS t(i);

CREATE TABLE IF NOT EXISTS raw.customers AS
SELECT
    'customer_' || i AS id,
    'Customer ' || i AS name,
    CASE WHEN random() > 0.5 THEN 'active' ELSE 'inactive' END AS status,
    CURRENT_TIMESTAMP - INTERVAL (i || ' days') AS created_at
FROM generate_series(1, 100) AS t(i);
""")

    # Models directory
    models_dir = os.path.join(output_dir, "models")
    os.makedirs(models_dir, exist_ok=True)

    # Defaults
    with open(os.path.join(models_dir, "_defaults.toml"), "w") as f:
        f.write("""[target]
catalog = "memory"
schema = "main"
""")

    # Generate models — 4 layers
    layer_sizes = {
        "staging": int(num_models * 0.30),
        "intermediate": int(num_models * 0.30),
        "marts": int(num_models * 0.25),
        "metrics": num_models - int(num_models * 0.85),
    }

    model_names = {"staging": [], "intermediate": [], "marts": [], "metrics": []}

    # Staging models (read from raw)
    for i in range(layer_sizes["staging"]):
        name = f"stg_model_{i}"
        model_names["staging"].append(name)
        toml_content = f"""name = "{name}"
depends_on = []

[target]
table = "{name}"

[[sources]]
catalog = "memory"
schema = "raw"
table = "orders"
"""
        sql_content = f"""SELECT
    id,
    customer_id,
    amount,
    status,
    created_at,
    amount * 1.{i % 10} AS adjusted_amount
FROM memory.raw.orders
WHERE status = 'completed'
"""
        with open(os.path.join(models_dir, f"{name}.toml"), "w") as f:
            f.write(toml_content)
        with open(os.path.join(models_dir, f"{name}.sql"), "w") as f:
            f.write(sql_content)

    # Intermediate models (join staging)
    for i in range(layer_sizes["intermediate"]):
        name = f"int_model_{i}"
        model_names["intermediate"].append(name)
        dep_idx = i % len(model_names["staging"])
        dep = model_names["staging"][dep_idx]
        toml_content = f"""name = "{name}"
depends_on = ["{dep}"]

[target]
table = "{name}"
"""
        sql_content = f"""SELECT
    customer_id,
    COUNT(*) AS order_count,
    SUM(adjusted_amount) AS total_amount,
    AVG(adjusted_amount) AS avg_amount,
    MIN(created_at) AS first_order,
    MAX(created_at) AS last_order
FROM memory.main.{dep}
GROUP BY customer_id
HAVING COUNT(*) >= 1
"""
        with open(os.path.join(models_dir, f"{name}.toml"), "w") as f:
            f.write(toml_content)
        with open(os.path.join(models_dir, f"{name}.sql"), "w") as f:
            f.write(sql_content)

    # Marts (aggregate intermediates)
    for i in range(layer_sizes["marts"]):
        name = f"mart_model_{i}"
        model_names["marts"].append(name)
        dep_idx = i % len(model_names["intermediate"])
        dep = model_names["intermediate"][dep_idx]
        toml_content = f"""name = "{name}"
depends_on = ["{dep}"]

[target]
table = "{name}"
"""
        sql_content = f"""SELECT
    customer_id,
    total_amount,
    order_count,
    avg_amount,
    CASE
        WHEN total_amount > 5000 THEN 'platinum'
        WHEN total_amount > 1000 THEN 'gold'
        WHEN total_amount > 100 THEN 'silver'
        ELSE 'bronze'
    END AS tier,
    RANK() OVER (ORDER BY total_amount DESC) AS revenue_rank
FROM memory.main.{dep}
"""
        with open(os.path.join(models_dir, f"{name}.toml"), "w") as f:
            f.write(toml_content)
        with open(os.path.join(models_dir, f"{name}.sql"), "w") as f:
            f.write(sql_content)

    # Metrics (final aggregation)
    for i in range(layer_sizes["metrics"]):
        name = f"metric_model_{i}"
        model_names["metrics"].append(name)
        dep_idx = i % len(model_names["marts"])
        dep = model_names["marts"][dep_idx]
        toml_content = f"""name = "{name}"
depends_on = ["{dep}"]

[target]
table = "{name}"
"""
        sql_content = f"""SELECT
    tier,
    COUNT(*) AS customer_count,
    SUM(total_amount) AS tier_revenue,
    AVG(total_amount) AS avg_revenue,
    MIN(revenue_rank) AS best_rank,
    MAX(revenue_rank) AS worst_rank
FROM memory.main.{dep}
GROUP BY tier
"""
        with open(os.path.join(models_dir, f"{name}.toml"), "w") as f:
            f.write(toml_content)
        with open(os.path.join(models_dir, f"{name}.sql"), "w") as f:
            f.write(sql_content)

    print(f"  Generated {num_models} executable models in {output_dir}")


def run_execution_benchmark(
    rocky_bin: str,
    project_dir: str,
    num_models: int,
    iterations: int = 3,
) -> dict:
    """Run the execution benchmark."""
    results = {
        "scale": num_models,
        "iterations": iterations,
        "compile_times": [],
        "execution_times": [],
        "total_times": [],
    }

    # Seed the database
    seed_path = os.path.join(project_dir, "data", "seed.sql")
    db_path = os.path.join(project_dir, "bench.duckdb")

    for iteration in range(iterations):
        # Remove old DB to start fresh
        if os.path.exists(db_path):
            os.remove(db_path)

        # Seed
        subprocess.run(
            ["duckdb", db_path],
            input=open(seed_path).read(),
            capture_output=True,
            text=True,
        )

        # Compile
        t0 = time.perf_counter()
        compile_result = subprocess.run(
            [
                rocky_bin,
                "-c", os.path.join(project_dir, "rocky.toml"),
                "compile",
                "--models", os.path.join(project_dir, "models"),
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        compile_time = time.perf_counter() - t0

        if compile_result.returncode != 0:
            print(f"  Compile failed (iteration {iteration}): {compile_result.stderr[:200]}")
            continue

        results["compile_times"].append(compile_time)

        # Execute (run the pipeline)
        t1 = time.perf_counter()
        run_result = subprocess.run(
            [
                rocky_bin,
                "-c", os.path.join(project_dir, "rocky.toml"),
                "run",
                "--output", "json",
            ],
            capture_output=True,
            text=True,
        )
        exec_time = time.perf_counter() - t1
        total_time = time.perf_counter() - t0

        results["execution_times"].append(exec_time)
        results["total_times"].append(total_time)

        status = "OK" if run_result.returncode == 0 else f"FAIL({run_result.returncode})"
        print(
            f"  Iteration {iteration + 1}: compile={compile_time:.2f}s "
            f"exec={exec_time:.2f}s total={total_time:.2f}s [{status}]"
        )

    # Calculate means
    if results["compile_times"]:
        results["mean_compile"] = sum(results["compile_times"]) / len(results["compile_times"])
        results["mean_execution"] = sum(results["execution_times"]) / len(results["execution_times"])
        results["mean_total"] = sum(results["total_times"]) / len(results["total_times"])
        results["models_per_second"] = num_models / results["mean_total"]

    return results


def main():
    parser = argparse.ArgumentParser(description="Rocky execution benchmarks")
    parser.add_argument("--scale", type=int, nargs="+", default=[100, 500, 1000],
                        help="Number of models (default: 100 500 1000)")
    parser.add_argument("--iterations", type=int, default=3,
                        help="Iterations per benchmark (default: 3)")
    parser.add_argument("--rocky-bin", type=str,
                        default="../../../engine/target/release/rocky",
                        help="Path to rocky binary")
    parser.add_argument("--output-dir", type=str, default="results",
                        help="Output directory for results")
    args = parser.parse_args()

    rocky_bin = os.path.abspath(args.rocky_bin)
    if not os.path.exists(rocky_bin):
        print(f"Error: Rocky binary not found at {rocky_bin}")
        sys.exit(1)

    os.makedirs(args.output_dir, exist_ok=True)

    all_results = []
    for scale in args.scale:
        print(f"\n{'='*60}")
        print(f"Execution benchmark: {scale} models")
        print(f"{'='*60}")

        project_dir = tempfile.mkdtemp(prefix=f"rocky_exec_bench_{scale}_")
        try:
            generate_execution_project(project_dir, scale)
            results = run_execution_benchmark(
                rocky_bin, project_dir, scale, args.iterations
            )
            all_results.append(results)

            if "mean_total" in results:
                print(f"\n  Summary ({scale} models):")
                print(f"    Compile:   {results['mean_compile']:.2f}s")
                print(f"    Execute:   {results['mean_execution']:.2f}s")
                print(f"    Total:     {results['mean_total']:.2f}s")
                print(f"    Throughput: {results['models_per_second']:.0f} models/sec")
        finally:
            shutil.rmtree(project_dir, ignore_errors=True)

    # Save results
    output_path = os.path.join(args.output_dir, "execution_benchmark.json")
    with open(output_path, "w") as f:
        json.dump(all_results, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
