#!/usr/bin/env python3
"""Visualize benchmark results: Rocky vs dbt-core vs dbt-fusion.

Usage:
    python visualize.py results/benchmark_*.json
"""

import argparse
import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")

COLORS = {
    "Rocky": "#2563eb",
    "dbt-core": "#dc2626",
    "dbt-fusion": "#f59e0b",
    "PySpark": "#16a34a",
}


def load_results(paths: list[str]) -> list[dict]:
    results = []
    for p in paths:
        for f in sorted(Path(".").glob(p)) if "*" in p else [Path(p)]:
            with open(f) as fh:
                results.append(json.load(fh))
    return sorted(results, key=lambda x: x["metadata"]["scale"])


def _bar_chart(
    ax, scales: list[str], data: dict[str, list[float | None]],
    ylabel: str, title: str, log_scale: bool = False,
    value_fmt: str = ".1f", value_suffix: str = "s",
):
    """Generic grouped bar chart for tools."""
    tools = [t for t in COLORS if t in data and any(v is not None for v in data[t])]
    if not tools:
        return

    n = len(scales)
    width = 0.8 / len(tools)
    x = list(range(n))

    for j, tool in enumerate(tools):
        vals = [v or 0 for v in data[tool]]
        offset = (j - len(tools) / 2 + 0.5) * width
        bars = ax.bar(
            [i + offset for i in x], vals, width,
            label=tool, color=COLORS[tool], alpha=0.9,
        )
        for bar, v in zip(bars, data[tool]):
            if v is not None and v > 0:
                label = f"{v:{value_fmt}}{value_suffix}"
                ax.text(
                    bar.get_x() + bar.get_width() / 2, bar.get_height(),
                    label, ha="center", va="bottom", fontsize=8,
                )

    ax.set_xlabel("Model Count")
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.set_xticks(x)
    ax.set_xticklabels(scales)
    ax.legend()
    if log_scale:
        ax.set_yscale("log")


def plot_parse_compile(results: list[dict], output_dir: Path):
    """Parse/compile time comparison."""
    scales = []
    data = {"Rocky": [], "dbt-core": [], "dbt-fusion": []}

    for r in results:
        scale = r["metadata"]["scale"]
        scales.append(f"{scale // 1000}K")
        b = r["benchmarks"]
        data["Rocky"].append(b.get("rocky_compile", {}).get("mean_s"))
        data["dbt-core"].append(b.get("dbt_core_parse", {}).get("mean_s"))
        data["dbt-fusion"].append(b.get("dbt_fusion_parse", {}).get("mean_s"))

    if not scales:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    _bar_chart(ax, scales, data, "Time (seconds, log scale)",
               "Parse + Compile: Rocky vs dbt-core vs dbt-fusion", log_scale=True)
    plt.tight_layout()
    fig.savefig(output_dir / "parse_compile.png", dpi=150)
    plt.close()
    print(f"  Saved: parse_compile.png")


def plot_memory(results: list[dict], output_dir: Path):
    """Peak memory comparison."""
    scales = []
    data = {"Rocky": [], "dbt-core": [], "dbt-fusion": []}

    for r in results:
        scale = r["metadata"]["scale"]
        scales.append(f"{scale // 1000}K")
        b = r["benchmarks"]
        data["Rocky"].append(b.get("rocky_compile", {}).get("peak_rss_mb"))
        data["dbt-core"].append(b.get("dbt_core_compile", {}).get("peak_rss_mb"))
        data["dbt-fusion"].append(b.get("dbt_fusion_compile", {}).get("peak_rss_mb"))

    if not any(v for vals in data.values() for v in vals if v is not None):
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    _bar_chart(ax, scales, data, "Peak RSS (MB)",
               "Memory Usage: Rocky vs dbt-core vs dbt-fusion",
               value_fmt=".0f", value_suffix=" MB")
    plt.tight_layout()
    fig.savefig(output_dir / "memory.png", dpi=150)
    plt.close()
    print(f"  Saved: memory.png")


def plot_scaling(results: list[dict], output_dir: Path):
    """Scaling curve: time vs model count."""
    tool_data = {
        "Rocky": ("rocky_compile", "#2563eb", "o-"),
        "dbt-core": ("dbt_core_parse", "#dc2626", "s-"),
        "dbt-fusion": ("dbt_fusion_parse", "#f59e0b", "^-"),
    }

    fig, ax = plt.subplots(figsize=(10, 6))
    for tool, (key, color, marker) in tool_data.items():
        points = []
        for r in results:
            t = r["benchmarks"].get(key, {}).get("mean_s")
            if t is not None:
                points.append((r["metadata"]["scale"], t))
        if len(points) >= 1:
            xs, ys = zip(*points)
            ax.plot(xs, ys, marker, label=tool, color=color, linewidth=2, markersize=8)

    ax.set_xlabel("Model Count")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("Scaling: Parse/Compile Time vs Model Count")
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    fig.savefig(output_dir / "scaling.png", dpi=150)
    plt.close()
    print(f"  Saved: scaling.png")


def plot_speedup(results: list[dict], output_dir: Path):
    """Speedup ratios vs Rocky."""
    scales = []
    core_ratios = []
    fusion_ratios = []

    for r in results:
        rocky_t = r["benchmarks"].get("rocky_compile", {}).get("mean_s")
        if not rocky_t or rocky_t <= 0:
            continue
        scales.append(f"{r['metadata']['scale'] // 1000}K")
        core_t = r["benchmarks"].get("dbt_core_parse", {}).get("mean_s")
        fusion_t = r["benchmarks"].get("dbt_fusion_parse", {}).get("mean_s")
        core_ratios.append(core_t / rocky_t if core_t else None)
        fusion_ratios.append(fusion_t / rocky_t if fusion_t else None)

    if not scales:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    x = list(range(len(scales)))
    width = 0.35

    if any(v for v in core_ratios if v):
        vals = [v or 0 for v in core_ratios]
        bars = ax.bar([i - width/2 for i in x], vals, width,
                      label="dbt-core / Rocky", color=COLORS["dbt-core"], alpha=0.9)
        for bar, v in zip(bars, core_ratios):
            if v:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                        f"{v:.0f}x", ha="center", va="bottom", fontsize=10, fontweight="bold")

    if any(v for v in fusion_ratios if v):
        vals = [v or 0 for v in fusion_ratios]
        bars = ax.bar([i + width/2 for i in x], vals, width,
                      label="dbt-fusion / Rocky", color=COLORS["dbt-fusion"], alpha=0.9)
        for bar, v in zip(bars, fusion_ratios):
            if v:
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                        f"{v:.1f}x", ha="center", va="bottom", fontsize=10, fontweight="bold")

    ax.axhline(y=1, color="gray", linestyle="--", alpha=0.5, label="Rocky baseline (1x)")
    ax.set_xlabel("Model Count")
    ax.set_ylabel("Slowdown vs Rocky (higher = slower)")
    ax.set_title("How Much Slower Than Rocky?")
    ax.set_xticks(x)
    ax.set_xticklabels(scales)
    ax.legend()
    plt.tight_layout()
    fig.savefig(output_dir / "speedup.png", dpi=150)
    plt.close()
    print(f"  Saved: speedup.png")


def plot_startup(results: list[dict], output_dir: Path):
    """Startup / cold-start time comparison."""
    # Use the first result that has startup data
    r = None
    for result in results:
        b = result["benchmarks"]
        if any(b.get(k) for k in ["rocky_startup", "dbt_core_startup", "dbt_fusion_startup"]):
            r = result
            break
    if not r:
        return

    b = r["benchmarks"]
    tools = []
    times = []
    colors = []

    for tool, key, color in [
        ("Rocky", "rocky_startup", COLORS["Rocky"]),
        ("dbt-core", "dbt_core_startup", COLORS["dbt-core"]),
        ("dbt-fusion", "dbt_fusion_startup", COLORS["dbt-fusion"]),
    ]:
        t = b.get(key, {}).get("mean_s")
        if t is not None:
            tools.append(tool)
            times.append(t)
            colors.append(color)

    if not tools:
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    bars = ax.bar(tools, times, color=colors, alpha=0.9)
    for bar, t in zip(bars, times):
        label = f"{t*1000:.0f}ms" if t < 1 else f"{t:.2f}s"
        ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                label, ha="center", va="bottom", fontsize=11, fontweight="bold")

    ax.set_ylabel("Time (seconds)")
    ax.set_title("Cold-Start Time: rocky --version vs dbt --version")
    ax.set_yscale("log")
    plt.tight_layout()
    fig.savefig(output_dir / "startup.png", dpi=150)
    plt.close()
    print(f"  Saved: startup.png")


def plot_lineage(results: list[dict], output_dir: Path):
    """Lineage computation time comparison."""
    scales = []
    data = {"Rocky": [], "dbt-core": [], "dbt-fusion": []}

    for r in results:
        b = r["benchmarks"]
        if not any(b.get(k) for k in ["rocky_lineage", "dbt_core_docs_generate", "dbt_fusion_docs_generate"]):
            continue
        scale = r["metadata"]["scale"]
        scales.append(f"{scale // 1000}K")
        data["Rocky"].append(b.get("rocky_lineage", {}).get("mean_s"))
        data["dbt-core"].append(b.get("dbt_core_docs_generate", {}).get("mean_s"))
        data["dbt-fusion"].append(b.get("dbt_fusion_docs_generate", {}).get("mean_s"))

    if not scales:
        return

    fig, ax = plt.subplots(figsize=(10, 6))
    _bar_chart(ax, scales, data, "Time (seconds, log scale)",
               "Lineage: rocky lineage vs dbt docs generate", log_scale=True)
    plt.tight_layout()
    fig.savefig(output_dir / "lineage.png", dpi=150)
    plt.close()
    print(f"  Saved: lineage.png")


def plot_incremental(results: list[dict], output_dir: Path):
    """Incremental / warm compile comparison."""
    scales = []
    data_1 = {"Rocky": [], "dbt-core": [], "dbt-fusion": []}
    data_10 = {"Rocky": [], "dbt-core": [], "dbt-fusion": []}

    for r in results:
        b = r["benchmarks"]
        has_data = any(b.get(k) for k in [
            "rocky_warm_compile_1", "dbt_core_warm_compile_1",
            "rocky_warm_compile_10", "dbt_core_warm_compile_10",
        ])
        if not has_data:
            continue

        scale = r["metadata"]["scale"]
        scales.append(f"{scale // 1000}K")
        data_1["Rocky"].append(b.get("rocky_warm_compile_1", {}).get("mean_s"))
        data_1["dbt-core"].append(b.get("dbt_core_warm_compile_1", {}).get("mean_s"))
        data_1["dbt-fusion"].append(b.get("dbt_fusion_warm_compile_1", {}).get("mean_s"))
        data_10["Rocky"].append(b.get("rocky_warm_compile_10", {}).get("mean_s"))
        data_10["dbt-core"].append(b.get("dbt_core_warm_compile_10", {}).get("mean_s"))
        data_10["dbt-fusion"].append(b.get("dbt_fusion_warm_compile_10", {}).get("mean_s"))

    if not scales:
        return

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    _bar_chart(ax1, scales, data_1, "Time (seconds, log scale)",
               "Warm Compile After 1 Model Changed", log_scale=True)
    _bar_chart(ax2, scales, data_10, "Time (seconds, log scale)",
               "Warm Compile After 10 Models Changed", log_scale=True)
    plt.tight_layout()
    fig.savefig(output_dir / "incremental_compile.png", dpi=150)
    plt.close()
    print(f"  Saved: incremental_compile.png")


def print_summary(results: list[dict]):
    """Print text summary table."""
    print(f"\n  {'='*90}")
    print(f"  {'Scale':>8s}  {'Phase':<25s}  {'Rocky':>10s}  {'dbt-core':>10s}  {'dbt-fusion':>10s}  {'core/rocky':>10s}  {'fusion/rocky':>12s}")
    print(f"  {'-'*90}")

    for r in results:
        scale = r["metadata"]["scale"]
        b = r["benchmarks"]

        # Core: compile
        rocky_t = b.get("rocky_compile", {}).get("mean_s")
        core_t = b.get("dbt_core_parse", {}).get("mean_s")
        fusion_t = b.get("dbt_fusion_parse", {}).get("mean_s")

        rs = f"{rocky_t:.2f}s" if rocky_t else "-"
        cs = f"{core_t:.2f}s" if core_t else "-"
        fs = f"{fusion_t:.2f}s" if fusion_t else "-"
        cr = f"{core_t/rocky_t:.0f}x" if core_t and rocky_t else "-"
        fr = f"{fusion_t/rocky_t:.1f}x" if fusion_t and rocky_t else "-"
        print(f"  {scale:>8d}  {'parse/compile':<25s}  {rs:>10s}  {cs:>10s}  {fs:>10s}  {cr:>10s}  {fr:>12s}")

        # Memory row
        rm = b.get("rocky_compile", {}).get("peak_rss_mb")
        cm = b.get("dbt_core_compile", {}).get("peak_rss_mb") or b.get("dbt_core_parse", {}).get("peak_rss_mb")
        fm = b.get("dbt_fusion_compile", {}).get("peak_rss_mb") or b.get("dbt_fusion_parse", {}).get("peak_rss_mb")
        rms = f"{rm:.0f} MB" if rm else "-"
        cms = f"{cm:.0f} MB" if cm else "-"
        fms = f"{fm:.0f} MB" if fm else "-"
        print(f"  {'':>8s}  {'peak memory':<25s}  {rms:>10s}  {cms:>10s}  {fms:>10s}")

        # Extended: startup (only once)
        startup_r = b.get("rocky_startup", {}).get("mean_s")
        startup_c = b.get("dbt_core_startup", {}).get("mean_s")
        startup_f = b.get("dbt_fusion_startup", {}).get("mean_s")
        if startup_r or startup_c or startup_f:
            srs = f"{startup_r*1000:.0f}ms" if startup_r else "-"
            scs = f"{startup_c*1000:.0f}ms" if startup_c else "-"
            sfs = f"{startup_f*1000:.0f}ms" if startup_f else "-"
            print(f"  {'':>8s}  {'startup':<25s}  {srs:>10s}  {scs:>10s}  {sfs:>10s}")

        # Lineage
        lin_r = b.get("rocky_lineage", {}).get("mean_s")
        lin_c = b.get("dbt_core_docs_generate", {}).get("mean_s")
        if lin_r or lin_c:
            lrs = f"{lin_r:.2f}s" if lin_r else "-"
            lcs = f"{lin_c:.2f}s" if lin_c else "-"
            print(f"  {'':>8s}  {'lineage / docs gen':<25s}  {lrs:>10s}  {lcs:>10s}")

    print(f"  {'='*90}")


def main():
    parser = argparse.ArgumentParser(description="Visualize benchmark results")
    parser.add_argument("files", nargs="+", help="Result JSON files")
    parser.add_argument("--output-dir", type=str, default="results/charts")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = load_results(args.files)
    if not results:
        print("No result files found")
        sys.exit(1)

    print(f"  Loaded {len(results)} result file(s)")
    plot_parse_compile(results, output_dir)
    plot_memory(results, output_dir)
    plot_scaling(results, output_dir)
    plot_speedup(results, output_dir)
    plot_startup(results, output_dir)
    plot_lineage(results, output_dir)
    plot_incremental(results, output_dir)
    print_summary(results)


if __name__ == "__main__":
    main()
