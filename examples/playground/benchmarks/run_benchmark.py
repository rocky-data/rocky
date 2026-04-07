#!/usr/bin/env python3
"""Benchmark runner: Rocky vs dbt-core vs dbt-fusion comparison.

Runs parse/compile/DAG benchmarks plus extended benchmarks (startup, lineage,
incremental compile, validation, SQL generation throughput) for all three tools
at specified scale and writes results to JSON for visualization.

Usage:
    python run_benchmark.py --scale 10000
    python run_benchmark.py --scale 20000 --iterations 3
    python run_benchmark.py --scale 10000 --tool rocky
    python run_benchmark.py --scale 10000 --tool dbt-core
    python run_benchmark.py --scale 10000 --tool dbt-fusion
    python run_benchmark.py --scale 10000 --benchmark-type startup
    python run_benchmark.py --scale 10000 --benchmark-type all
"""

import argparse
import json
import os
import platform
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import psutil
    HAS_PSUTIL = True
except ImportError:
    HAS_PSUTIL = False

# dbt-fusion binary (Rust-based, installed via shell script)
DBT_FUSION_BIN = os.path.expanduser("~/.local/bin/dbt")


def get_system_info() -> dict:
    return {
        "os": platform.system().lower(),
        "arch": platform.machine(),
        "cpus": os.cpu_count() or 1,
        "ram_gb": round(psutil.virtual_memory().total / (1024**3), 1) if HAS_PSUTIL else None,
        "python_version": platform.python_version(),
    }


def find_rocky_binary() -> str | None:
    """Find the rocky binary — prefer release build, fall back to debug."""
    candidates = [
        Path(__file__).resolve().parents[3] / "engine" / "target" / "release" / "rocky",
        Path(__file__).resolve().parents[2] / "rocky" / "target" / "release" / "rocky",
        Path(__file__).resolve().parents[2] / "rocky" / "target" / "debug" / "rocky",
        shutil.which("rocky"),
    ]
    for c in candidates:
        if c and Path(c).exists():
            return str(c)
    return None


def find_dbt_core_binary() -> str | None:
    """Find the Python dbt-core binary (in venv)."""
    bench_dir = Path(__file__).resolve().parent
    venv_dbt = bench_dir / ".venv" / "bin" / "dbt"
    if venv_dbt.exists():
        return str(venv_dbt)
    # Fall back to PATH, but verify it's Python dbt-core not fusion
    dbt = shutil.which("dbt")
    if dbt and dbt != DBT_FUSION_BIN:
        return dbt
    return None


def find_dbt_fusion_binary() -> str | None:
    """Find the dbt-fusion binary (Rust-based)."""
    if Path(DBT_FUSION_BIN).exists():
        return DBT_FUSION_BIN
    return None


def get_tool_version(binary: str) -> str:
    try:
        proc = subprocess.run(
            [binary, "--version"], capture_output=True, text=True, timeout=10
        )
        return proc.stdout.strip().split("\n")[0]
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Timing helpers
# ---------------------------------------------------------------------------

def measure_with_time(
    cmd: list[str],
    cwd: str | Path,
    label: str,
    iterations: int = 3,
    timeout: int = 1800,
    env_override: dict | None = None,
    warmup: bool = True,
) -> dict:
    """Use /usr/bin/time to capture peak RSS on macOS/Linux."""
    results = []
    env = {**os.environ, **(env_override or {})}

    # Warmup run (not measured)
    if warmup:
        try:
            subprocess.run(cmd, cwd=cwd, capture_output=True, timeout=timeout, env=env)
        except (subprocess.TimeoutExpired, FileNotFoundError):
            pass

    for i in range(iterations):
        if platform.system() == "Darwin":
            time_cmd = ["/usr/bin/time", "-l"] + cmd
        elif platform.system() == "Linux":
            time_cmd = ["/usr/bin/time", "-v"] + cmd
        else:
            time_cmd = cmd

        start = time.perf_counter()
        try:
            proc = subprocess.run(
                time_cmd, cwd=cwd, capture_output=True, text=True,
                timeout=timeout, env=env,
            )
            elapsed = time.perf_counter() - start
            peak_rss_kb = _parse_rss(proc.stderr, platform.system())

            results.append({
                "iteration": i,
                "wall_time_s": round(elapsed, 4),
                "exit_code": proc.returncode,
                "peak_rss_mb": round(peak_rss_kb / 1024, 1) if peak_rss_kb else None,
                "stderr_tail": (proc.stderr or "")[-500:] if proc.returncode != 0 else None,
            })
        except subprocess.TimeoutExpired:
            elapsed = time.perf_counter() - start
            results.append({
                "iteration": i,
                "wall_time_s": round(elapsed, 4),
                "exit_code": -1,
                "error": "timeout",
            })

    ok_results = [r for r in results if r.get("exit_code", -1) == 0]
    times = [r["wall_time_s"] for r in (ok_results or results)]
    rss_values = [r["peak_rss_mb"] for r in results if r.get("peak_rss_mb")]

    return {
        "label": label,
        "command": " ".join(cmd),
        "iterations": results,
        "mean_s": round(sum(times) / len(times), 4) if times else None,
        "min_s": round(min(times), 4) if times else None,
        "max_s": round(max(times), 4) if times else None,
        "peak_rss_mb": round(max(rss_values), 1) if rss_values else None,
        "success": len(ok_results) == len(results),
    }


def _parse_rss(stderr: str, os_name: str) -> float | None:
    if not stderr:
        return None
    for line in stderr.splitlines():
        if os_name == "Darwin" and "maximum resident set size" in line:
            parts = line.strip().split()
            try:
                return int(parts[0]) / 1024  # bytes -> KB
            except (ValueError, IndexError):
                pass
        elif os_name == "Linux" and "Maximum resident set size" in line:
            parts = line.strip().split()
            try:
                return int(parts[-1])
            except (ValueError, IndexError):
                pass
    return None


# ---------------------------------------------------------------------------
# Core benchmarks: Rocky
# ---------------------------------------------------------------------------

def bench_rocky(rocky_bin: str, scale: int, models_dir: str, iterations: int) -> dict:
    """Run core Rocky benchmarks (compile + DAG)."""
    results = {}

    print("  [1/2] rocky compile (external wall clock) ...")
    results["rocky_compile"] = measure_with_time(
        [rocky_bin, "compile", "--models", models_dir],
        cwd=".", label="rocky_compile", iterations=iterations,
        env_override={"RUST_LOG": "warn"},
    )
    _print_result(results["rocky_compile"])

    print("  [2/2] rocky bench dag ...")
    r = measure_with_time(
        [rocky_bin, "bench", "dag", "--format", "json"],
        cwd=".", label="rocky_dag", iterations=1,
        env_override={"RUST_LOG": "warn"},
    )
    # Capture internal JSON output
    proc = subprocess.run(
        [rocky_bin, "bench", "dag", "--format", "json"],
        capture_output=True, text=True, timeout=300,
    )
    if proc.returncode == 0 and proc.stdout:
        try:
            r["internal_results"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            pass
    results["rocky_dag"] = r
    print("        Done.")

    return results


# ---------------------------------------------------------------------------
# Core benchmarks: dbt-core (Python)
# ---------------------------------------------------------------------------

def bench_dbt_core(dbt_bin: str, dbt_dir: str, iterations: int) -> dict:
    """Run core dbt-core benchmarks (parse + compile + ls)."""
    results = {}

    print("  [1/3] dbt-core parse (--no-partial-parse) ...")
    results["dbt_core_parse"] = measure_with_time(
        [dbt_bin, "parse", "--no-partial-parse"],
        cwd=dbt_dir, label="dbt_core_parse", iterations=iterations,
    )
    _print_result(results["dbt_core_parse"])

    print("  [2/3] dbt-core compile ...")
    results["dbt_core_compile"] = measure_with_time(
        [dbt_bin, "compile"],
        cwd=dbt_dir, label="dbt_core_compile", iterations=iterations,
    )
    _print_result(results["dbt_core_compile"])

    print("  [3/3] dbt-core ls ...")
    results["dbt_core_ls"] = measure_with_time(
        [dbt_bin, "ls", "--output", "json"],
        cwd=dbt_dir, label="dbt_core_ls", iterations=iterations,
    )
    _print_result(results["dbt_core_ls"])

    # Manifest size
    manifest = Path(dbt_dir) / "target" / "manifest.json"
    if manifest.exists():
        results["dbt_core_manifest_mb"] = round(manifest.stat().st_size / (1024 * 1024), 1)

    return results


# ---------------------------------------------------------------------------
# Core benchmarks: dbt-fusion (Rust)
# ---------------------------------------------------------------------------

def bench_dbt_fusion(fusion_bin: str, dbt_dir: str, iterations: int) -> dict:
    """Run core dbt-fusion benchmarks (parse + compile + ls)."""
    results = {}

    # Clean target dir to avoid stale state between tools
    target_dir = Path(dbt_dir) / "target"
    if target_dir.exists():
        shutil.rmtree(target_dir)

    print("  [1/3] dbt-fusion parse ...")
    results["dbt_fusion_parse"] = measure_with_time(
        [fusion_bin, "parse", "--project-dir", dbt_dir],
        cwd=dbt_dir, label="dbt_fusion_parse", iterations=iterations,
    )
    _print_result(results["dbt_fusion_parse"])

    print("  [2/3] dbt-fusion compile ...")
    results["dbt_fusion_compile"] = measure_with_time(
        [fusion_bin, "compile", "--project-dir", dbt_dir],
        cwd=dbt_dir, label="dbt_fusion_compile", iterations=iterations,
    )
    _print_result(results["dbt_fusion_compile"])

    print("  [3/3] dbt-fusion ls ...")
    results["dbt_fusion_ls"] = measure_with_time(
        [fusion_bin, "ls", "--project-dir", dbt_dir],
        cwd=dbt_dir, label="dbt_fusion_ls", iterations=iterations,
    )
    _print_result(results["dbt_fusion_ls"])

    return results


# ---------------------------------------------------------------------------
# Extended benchmark: Startup / cold-start
# ---------------------------------------------------------------------------

def bench_startup(
    rocky_bin: str | None,
    dbt_core_bin: str | None,
    dbt_fusion_bin: str | None,
    iterations: int = 10,
) -> dict:
    """Measure binary cold-start time (--version invocation)."""
    results = {}

    if rocky_bin:
        print("  [startup] rocky --version ...")
        results["rocky_startup"] = measure_with_time(
            [rocky_bin, "--version"],
            cwd=".", label="rocky_startup", iterations=iterations,
            warmup=False,  # We want cold-start behavior
        )
        _print_result(results["rocky_startup"])

    if dbt_core_bin:
        print("  [startup] dbt-core --version ...")
        results["dbt_core_startup"] = measure_with_time(
            [dbt_core_bin, "--version"],
            cwd=".", label="dbt_core_startup", iterations=iterations,
            warmup=False,
        )
        _print_result(results["dbt_core_startup"])

    if dbt_fusion_bin:
        print("  [startup] dbt-fusion --version ...")
        results["dbt_fusion_startup"] = measure_with_time(
            [dbt_fusion_bin, "--version"],
            cwd=".", label="dbt_fusion_startup", iterations=iterations,
            warmup=False,
        )
        _print_result(results["dbt_fusion_startup"])

    return results


# ---------------------------------------------------------------------------
# Extended benchmark: Lineage computation
# ---------------------------------------------------------------------------

def bench_lineage(
    rocky_bin: str | None,
    dbt_core_bin: str | None,
    dbt_fusion_bin: str | None,
    rocky_dir: str,
    dbt_dir: str,
    scale: int,
    iterations: int,
) -> dict:
    """Measure lineage computation: rocky lineage vs dbt docs generate."""
    results = {}
    models_dir = str(Path(rocky_dir) / "models")

    # Pick a mart model near the end of the DAG for maximum lineage depth
    n_sources = int(scale * 0.30)
    n_staging = int(scale * 0.30)
    n_intermediate = int(scale * 0.25)
    n_marts = scale - n_sources - n_staging - n_intermediate
    last_mart_idx = n_sources + n_staging + n_intermediate + n_marts - 1
    target_model = f"fct_{last_mart_idx:05d}"

    if rocky_bin:
        print(f"  [lineage] rocky lineage {target_model} ...")
        results["rocky_lineage"] = measure_with_time(
            [rocky_bin, "lineage", target_model, "--models", models_dir],
            cwd=".", label="rocky_lineage", iterations=iterations,
            env_override={"RUST_LOG": "warn"},
        )
        _print_result(results["rocky_lineage"])

    if dbt_core_bin:
        print("  [lineage] dbt-core docs generate ...")
        results["dbt_core_docs_generate"] = measure_with_time(
            [dbt_core_bin, "docs", "generate"],
            cwd=dbt_dir, label="dbt_core_docs_generate", iterations=iterations,
        )
        _print_result(results["dbt_core_docs_generate"])

    if dbt_fusion_bin:
        print("  [lineage] dbt-fusion docs generate ...")
        results["dbt_fusion_docs_generate"] = measure_with_time(
            [dbt_fusion_bin, "docs", "generate", "--project-dir", dbt_dir],
            cwd=dbt_dir, label="dbt_fusion_docs_generate", iterations=iterations,
        )
        _print_result(results["dbt_fusion_docs_generate"])

    return results


# ---------------------------------------------------------------------------
# Extended benchmark: Incremental / warm compile
# ---------------------------------------------------------------------------

def bench_incremental_compile(
    rocky_bin: str | None,
    dbt_core_bin: str | None,
    dbt_fusion_bin: str | None,
    rocky_dir: str,
    dbt_dir: str,
    scale: int,
    iterations: int,
) -> dict:
    """Measure warm compile: recompile after modifying 1 and 10 models.

    For dbt-core, this tests partial parse (warm manifest reuse).
    For Rocky and dbt-fusion, this tests full recompile speed after changes.
    """
    results = {}
    models_dir = Path(rocky_dir) / "models"
    dbt_models_dir = Path(dbt_dir) / "models"

    # Pick random staging models to modify (deterministic seed)
    rng = random.Random(42)
    n_sources = int(scale * 0.30)
    staging_indices = list(range(n_sources, n_sources + int(scale * 0.30)))
    modify_1 = rng.sample(staging_indices, 1)
    modify_10 = rng.sample(staging_indices, min(10, len(staging_indices)))

    for change_count, indices in [("1", modify_1), ("10", modify_10)]:
        tag = f"warm_compile_{change_count}"
        print(f"\n  [incremental] Modifying {change_count} model(s)...")

        # --- Rocky ---
        if rocky_bin:
            # Save originals, modify, benchmark, restore
            rocky_originals = {}
            for idx in indices:
                name = f"stg_{idx:05d}"
                for ext in (".sql", ".rocky"):
                    path = models_dir / f"{name}{ext}"
                    if path.exists():
                        rocky_originals[path] = path.read_text()
                        path.write_text(rocky_originals[path] + f"\n-- modified {time.time()}\n")

            # Ensure compile cache is warm (one warmup compile first)
            subprocess.run(
                [rocky_bin, "compile", "--models", str(models_dir)],
                capture_output=True, timeout=600,
                env={**os.environ, "RUST_LOG": "warn"},
            )

            print(f"  [incremental] rocky compile (after {change_count} change(s)) ...")
            results[f"rocky_{tag}"] = measure_with_time(
                [rocky_bin, "compile", "--models", str(models_dir)],
                cwd=".", label=f"rocky_{tag}", iterations=iterations,
                env_override={"RUST_LOG": "warn"},
                warmup=False,  # We already did a manual warmup
            )
            _print_result(results[f"rocky_{tag}"])

            # Restore originals
            for path, content in rocky_originals.items():
                path.write_text(content)

        # --- dbt-core (partial parse: no --no-partial-parse flag) ---
        if dbt_core_bin:
            # Warmup: first compile to build manifest + partial_parse.msgpack
            subprocess.run(
                [dbt_core_bin, "compile"],
                cwd=dbt_dir, capture_output=True, timeout=600,
            )

            dbt_originals = {}
            for idx in indices:
                name = f"stg_{idx:05d}"
                path = dbt_models_dir / "staging" / f"{name}.sql"
                if path.exists():
                    dbt_originals[path] = path.read_text()
                    path.write_text(dbt_originals[path] + f"\n-- modified {time.time()}\n")

            print(f"  [incremental] dbt-core compile (partial parse, {change_count} change(s)) ...")
            results[f"dbt_core_{tag}"] = measure_with_time(
                [dbt_core_bin, "compile"],
                cwd=dbt_dir, label=f"dbt_core_{tag}", iterations=iterations,
                warmup=False,
            )
            _print_result(results[f"dbt_core_{tag}"])

            # Restore originals
            for path, content in dbt_originals.items():
                path.write_text(content)

        # --- dbt-fusion ---
        if dbt_fusion_bin:
            # Warmup compile
            subprocess.run(
                [dbt_fusion_bin, "compile", "--project-dir", dbt_dir],
                cwd=dbt_dir, capture_output=True, timeout=600,
            )

            fusion_originals = {}
            for idx in indices:
                name = f"stg_{idx:05d}"
                path = dbt_models_dir / "staging" / f"{name}.sql"
                if path.exists():
                    fusion_originals[path] = path.read_text()
                    path.write_text(fusion_originals[path] + f"\n-- modified {time.time()}\n")

            print(f"  [incremental] dbt-fusion compile ({change_count} change(s)) ...")
            results[f"dbt_fusion_{tag}"] = measure_with_time(
                [dbt_fusion_bin, "compile", "--project-dir", dbt_dir],
                cwd=dbt_dir, label=f"dbt_fusion_{tag}", iterations=iterations,
                warmup=False,
            )
            _print_result(results[f"dbt_fusion_{tag}"])

            # Restore originals
            for path, content in fusion_originals.items():
                path.write_text(content)

    return results


# ---------------------------------------------------------------------------
# Extended benchmark: Validation
# ---------------------------------------------------------------------------

def bench_validate(
    rocky_bin: str | None,
    dbt_core_bin: str | None,
    dbt_fusion_bin: str | None,
    rocky_dir: str,
    dbt_dir: str,
    iterations: int,
) -> dict:
    """Measure config validation speed (no warehouse access)."""
    results = {}

    rocky_toml = Path(rocky_dir) / "rocky.toml"
    if rocky_bin and rocky_toml.exists():
        print("  [validate] rocky validate ...")
        results["rocky_validate"] = measure_with_time(
            [rocky_bin, "validate", "-c", str(rocky_toml)],
            cwd=rocky_dir, label="rocky_validate", iterations=iterations,
            env_override={"RUST_LOG": "warn"},
        )
        _print_result(results["rocky_validate"])

    if dbt_core_bin:
        print("  [validate] dbt-core debug ...")
        results["dbt_core_debug"] = measure_with_time(
            [dbt_core_bin, "debug"],
            cwd=dbt_dir, label="dbt_core_debug", iterations=iterations,
        )
        _print_result(results["dbt_core_debug"])

    if dbt_fusion_bin:
        print("  [validate] dbt-fusion debug ...")
        results["dbt_fusion_debug"] = measure_with_time(
            [dbt_fusion_bin, "debug", "--project-dir", dbt_dir],
            cwd=dbt_dir, label="dbt_fusion_debug", iterations=iterations,
        )
        _print_result(results["dbt_fusion_debug"])

    return results


# ---------------------------------------------------------------------------
# Extended benchmark: SQL generation throughput (Rocky only)
# ---------------------------------------------------------------------------

def bench_sql_gen(rocky_bin: str, iterations: int = 1) -> dict:
    """Measure Rocky's raw SQL generation throughput."""
    results = {}

    print("  [sql_gen] rocky bench sql_gen ...")
    r = measure_with_time(
        [rocky_bin, "bench", "sql_gen", "--format", "json"],
        cwd=".", label="rocky_sql_gen", iterations=iterations,
        env_override={"RUST_LOG": "warn"},
    )
    # Capture internal JSON
    proc = subprocess.run(
        [rocky_bin, "bench", "sql_gen", "--format", "json"],
        capture_output=True, text=True, timeout=300,
    )
    if proc.returncode == 0 and proc.stdout:
        try:
            r["internal_results"] = json.loads(proc.stdout)
        except json.JSONDecodeError:
            pass
    results["rocky_sql_gen"] = r
    print("        Done.")

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _print_result(r: dict):
    rss = f" (RSS: {r.get('peak_rss_mb', '?')} MB)" if r.get("peak_rss_mb") else ""
    ok = "" if r.get("success", True) else " [ERRORS]"
    print(f"        {r.get('mean_s', '?')}s{rss}{ok}")


def print_summary(report: dict):
    """Print summary table with speedup ratios."""
    benchmarks = report["benchmarks"]
    scale = report["metadata"]["scale"]

    print(f"\n{'='*78}")
    print(f"  Results: {scale:,} models")
    print(f"{'='*78}")

    # Core benchmarks
    print(f"\n  {'--- Core Benchmarks ---':^72s}")
    print(f"  {'Phase':<25s}  {'Rocky':>10s}  {'dbt-core':>10s}  {'dbt-fusion':>10s}")
    print(f"  {'-'*72}")

    phases = [
        ("parse/compile", "rocky_compile", "dbt_core_parse", "dbt_fusion_parse"),
        ("compile (SQL gen)", None, "dbt_core_compile", "dbt_fusion_compile"),
        ("ls / DAG", "rocky_dag", "dbt_core_ls", "dbt_fusion_ls"),
    ]

    for phase_name, rk, dk, fk in phases:
        rv = benchmarks.get(rk, {}).get("mean_s") if rk else None
        dv = benchmarks.get(dk, {}).get("mean_s") if dk else None
        fv = benchmarks.get(fk, {}).get("mean_s") if fk else None
        rs = f"{rv:.2f}s" if rv is not None else "-"
        ds = f"{dv:.2f}s" if dv is not None else "-"
        fs = f"{fv:.2f}s" if fv is not None else "-"
        print(f"  {phase_name:<25s}  {rs:>10s}  {ds:>10s}  {fs:>10s}")

    # Memory
    print(f"  {'-'*72}")
    print(f"  {'Peak RSS':<25s}", end="")
    for key in ["rocky_compile", "dbt_core_compile", "dbt_fusion_compile"]:
        rss = benchmarks.get(key, {}).get("peak_rss_mb")
        s = f"{rss:.0f} MB" if rss else "-"
        print(f"  {s:>10s}", end="")
    print()

    # Manifest
    manifest_mb = benchmarks.get("dbt_core_manifest_mb")
    if manifest_mb:
        print(f"  {'Manifest size':<25s}  {'-':>10s}  {manifest_mb:.0f} MB{'':>6s}  {'-':>10s}")

    # Speedup ratios
    rocky_t = benchmarks.get("rocky_compile", {}).get("mean_s")
    print(f"\n  {'Speedup vs Rocky':<25s}", end="")
    print(f"  {'baseline':>10s}", end="")
    for key in ["dbt_core_parse", "dbt_fusion_parse"]:
        t = benchmarks.get(key, {}).get("mean_s")
        if rocky_t and t and rocky_t > 0:
            ratio = t / rocky_t
            print(f"  {ratio:>9.1f}x", end="")
        else:
            print(f"  {'-':>10s}", end="")
    print(" (slower)")

    # dbt-fusion vs dbt-core
    core_parse = benchmarks.get("dbt_core_parse", {}).get("mean_s")
    fusion_parse = benchmarks.get("dbt_fusion_parse", {}).get("mean_s")
    if core_parse and fusion_parse and fusion_parse > 0:
        ratio = core_parse / fusion_parse
        print(f"  {'dbt-core / dbt-fusion':<25s}  {'-':>10s}  {'-':>10s}  {ratio:>9.1f}x (fusion faster)")

    # Extended benchmarks summary
    ext_keys = [
        ("Startup", "rocky_startup", "dbt_core_startup", "dbt_fusion_startup"),
        ("Lineage / docs gen", "rocky_lineage", "dbt_core_docs_generate", "dbt_fusion_docs_generate"),
        ("Warm compile (1 chg)", "rocky_warm_compile_1", "dbt_core_warm_compile_1", "dbt_fusion_warm_compile_1"),
        ("Warm compile (10 chg)", "rocky_warm_compile_10", "dbt_core_warm_compile_10", "dbt_fusion_warm_compile_10"),
        ("Validate / debug", "rocky_validate", "dbt_core_debug", "dbt_fusion_debug"),
    ]

    has_ext = any(
        benchmarks.get(rk) or benchmarks.get(dk) or benchmarks.get(fk)
        for _, rk, dk, fk in ext_keys
    )
    if has_ext:
        print(f"\n  {'--- Extended Benchmarks ---':^72s}")
        print(f"  {'Phase':<25s}  {'Rocky':>10s}  {'dbt-core':>10s}  {'dbt-fusion':>10s}")
        print(f"  {'-'*72}")
        for phase_name, rk, dk, fk in ext_keys:
            rv = benchmarks.get(rk, {}).get("mean_s")
            dv = benchmarks.get(dk, {}).get("mean_s")
            fv = benchmarks.get(fk, {}).get("mean_s")
            if rv is None and dv is None and fv is None:
                continue
            rs = f"{rv:.3f}s" if rv is not None else "-"
            ds = f"{dv:.3f}s" if dv is not None else "-"
            fs = f"{fv:.3f}s" if fv is not None else "-"
            print(f"  {phase_name:<25s}  {rs:>10s}  {ds:>10s}  {fs:>10s}")

    print(f"{'='*78}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

TOOLS = ["rocky", "dbt-core", "dbt-fusion", "all"]
BENCH_TYPES = ["core", "startup", "lineage", "incremental", "validate", "sql_gen", "all"]


def main():
    parser = argparse.ArgumentParser(
        description="Rocky vs dbt-core vs dbt-fusion benchmark runner"
    )
    parser.add_argument("--scale", type=int, nargs="+", required=True,
                        help="Model counts to benchmark (e.g., --scale 10000 20000)")
    parser.add_argument("--iterations", type=int, default=3,
                        help="Timing iterations per phase")
    parser.add_argument("--tool", choices=TOOLS, default="all",
                        help="Which tool(s) to benchmark")
    parser.add_argument("--benchmark-type", choices=BENCH_TYPES, default="all",
                        dest="bench_type",
                        help="Which benchmark type(s) to run (default: all)")
    parser.add_argument("--output-dir", type=str, default="results")
    parser.add_argument("--rocky-bin", type=str, help="Path to rocky binary")
    parser.add_argument("--dbt-core-bin", type=str, help="Path to dbt-core binary")
    parser.add_argument("--dbt-fusion-bin", type=str, help="Path to dbt-fusion binary")
    args = parser.parse_args()

    bench_dir = Path(__file__).resolve().parent
    results_dir = bench_dir / args.output_dir
    results_dir.mkdir(parents=True, exist_ok=True)

    # Locate binaries (resolve to absolute paths)
    rocky_bin = args.rocky_bin or find_rocky_binary()
    dbt_core_bin = args.dbt_core_bin or find_dbt_core_binary()
    dbt_fusion_bin = args.dbt_fusion_bin or find_dbt_fusion_binary()
    if rocky_bin:
        rocky_bin = str(Path(rocky_bin).resolve())
    if dbt_core_bin:
        dbt_core_bin = str(Path(dbt_core_bin).resolve())
    if dbt_fusion_bin:
        dbt_fusion_bin = str(Path(dbt_fusion_bin).resolve())

    run_rocky = args.tool in ("rocky", "all")
    run_dbt_core = args.tool in ("dbt-core", "all")
    run_dbt_fusion = args.tool in ("dbt-fusion", "all")

    bt = args.bench_type
    run_core = bt in ("core", "all")
    run_startup = bt in ("startup", "all")
    run_lineage = bt in ("lineage", "all")
    run_incremental = bt in ("incremental", "all")
    run_validate = bt in ("validate", "all")
    run_sql_gen = bt in ("sql_gen", "all")

    if run_rocky and not rocky_bin:
        print("ERROR: rocky binary not found. Build with: cd engine && cargo build --release")
        sys.exit(1)
    if run_dbt_core and not dbt_core_bin:
        print("ERROR: dbt-core not found. Install: uv pip install dbt-core dbt-duckdb")
        sys.exit(1)
    if run_dbt_fusion and not dbt_fusion_bin:
        print("WARNING: dbt-fusion not found. Skipping.")
        run_dbt_fusion = False

    # Print tool versions
    print(f"\n  Tool versions:")
    if rocky_bin:
        print(f"    Rocky:      {get_tool_version(rocky_bin)}")
    if dbt_core_bin:
        print(f"    dbt-core:   {get_tool_version(dbt_core_bin)}")
    if dbt_fusion_bin:
        print(f"    dbt-fusion: {get_tool_version(dbt_fusion_bin)}")

    # --- Startup benchmark (scale-independent) ---
    if run_startup:
        print(f"\n{'#'*78}")
        print(f"  Benchmark: Startup / Cold-Start")
        print(f"{'#'*78}")
        startup_results = bench_startup(
            rocky_bin if run_rocky else None,
            dbt_core_bin if run_dbt_core else None,
            dbt_fusion_bin if run_dbt_fusion else None,
            iterations=10,
        )

    # --- SQL generation throughput (Rocky only, scale-independent) ---
    sql_gen_results = {}
    if run_sql_gen and rocky_bin and run_rocky:
        print(f"\n{'#'*78}")
        print(f"  Benchmark: SQL Generation Throughput (Rocky)")
        print(f"{'#'*78}")
        sql_gen_results = bench_sql_gen(rocky_bin)

    all_reports = []

    for scale in args.scale:
        print(f"\n{'#'*78}")
        print(f"  Benchmark: {scale:,} models")
        print(f"{'#'*78}")

        # Generate projects
        rocky_dir = bench_dir / f"rocky_{scale}"
        dbt_dir = bench_dir / f"dbt_{scale}"

        if not rocky_dir.exists() or not dbt_dir.exists():
            print(f"\n  Generating projects at scale={scale}...")
            subprocess.run(
                [sys.executable, str(bench_dir / "generate_dbt_project.py"),
                 "--scale", str(scale), "--output-dir", str(bench_dir)],
                check=True,
            )

        report = {
            "metadata": {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "scale": scale,
                "iterations": args.iterations,
                "benchmark_type": args.bench_type,
                "system": get_system_info(),
                "versions": {
                    "rocky": get_tool_version(rocky_bin) if rocky_bin else None,
                    "dbt_core": get_tool_version(dbt_core_bin) if dbt_core_bin else None,
                    "dbt_fusion": get_tool_version(dbt_fusion_bin) if dbt_fusion_bin else None,
                },
            },
            "benchmarks": {},
        }

        # Inject startup results (scale-independent, included in every report)
        if run_startup:
            report["benchmarks"].update(startup_results)

        # Inject SQL gen results (scale-independent)
        if sql_gen_results:
            report["benchmarks"].update(sql_gen_results)

        # --- Core benchmarks ---
        if run_core:
            if run_rocky:
                print(f"\n  --- Rocky core (scale={scale}) ---")
                models_dir = str(rocky_dir / "models")
                report["benchmarks"].update(
                    bench_rocky(rocky_bin, scale, models_dir, args.iterations)
                )

            if run_dbt_core:
                print(f"\n  --- dbt-core core (scale={scale}) ---")
                report["benchmarks"].update(
                    bench_dbt_core(dbt_core_bin, str(dbt_dir), args.iterations)
                )

            if run_dbt_fusion:
                print(f"\n  --- dbt-fusion core (scale={scale}) ---")
                report["benchmarks"].update(
                    bench_dbt_fusion(dbt_fusion_bin, str(dbt_dir), args.iterations)
                )

        # --- Lineage benchmark ---
        if run_lineage:
            print(f"\n  --- Lineage (scale={scale}) ---")
            report["benchmarks"].update(bench_lineage(
                rocky_bin if run_rocky else None,
                dbt_core_bin if run_dbt_core else None,
                dbt_fusion_bin if run_dbt_fusion else None,
                str(rocky_dir), str(dbt_dir), scale, args.iterations,
            ))

        # --- Incremental compile benchmark ---
        if run_incremental:
            print(f"\n  --- Incremental compile (scale={scale}) ---")
            report["benchmarks"].update(bench_incremental_compile(
                rocky_bin if run_rocky else None,
                dbt_core_bin if run_dbt_core else None,
                dbt_fusion_bin if run_dbt_fusion else None,
                str(rocky_dir), str(dbt_dir), scale, args.iterations,
            ))

        # --- Validate benchmark ---
        if run_validate:
            print(f"\n  --- Validate (scale={scale}) ---")
            report["benchmarks"].update(bench_validate(
                rocky_bin if run_rocky else None,
                dbt_core_bin if run_dbt_core else None,
                dbt_fusion_bin if run_dbt_fusion else None,
                str(rocky_dir), str(dbt_dir), args.iterations,
            ))

        print_summary(report)

        # Save
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = results_dir / f"benchmark_{scale}_{ts}.json"
        output_file.write_text(json.dumps(report, indent=2))
        print(f"\n  Results saved to: {output_file}")
        all_reports.append(report)


if __name__ == "__main__":
    main()
