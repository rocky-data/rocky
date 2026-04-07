#!/usr/bin/env python3
"""Generate equivalent Rocky and dbt projects for benchmarking.

Creates two projects with identical SQL logic and dependency graphs:
- Rocky: .sql/.rocky + .toml sidecar pairs
- dbt: .sql with {{ ref() }} + schema.yml + dbt_project.yml

Usage:
    python generate_dbt_project.py --scale 10000 --output-dir ./
    python generate_dbt_project.py --scale 20000 --output-dir ./ --tool both
"""

import argparse
import os
import random
from pathlib import Path


def generate_dag(n: int, seed: int = 42) -> list[dict]:
    """Generate a layered DAG with realistic fan-in patterns.

    Returns a list of model dicts: {name, layer, deps, sql_variant, strategy}.
    """
    random.seed(seed)

    n_sources = int(n * 0.30)
    n_staging = int(n * 0.30)
    n_intermediate = int(n * 0.25)
    n_marts = n - n_sources - n_staging - n_intermediate

    models = []
    source_names = []
    staging_names = []
    intermediate_names = []

    idx = 0

    # Sources
    for _ in range(n_sources):
        name = f"src_{idx:05d}"
        models.append({
            "name": name,
            "layer": "sources",
            "deps": [],
            "ncols": 7 + (idx % 9),
            "strategy": "full_refresh",
            "idx": idx,
        })
        source_names.append(name)
        idx += 1

    # Staging
    for i in range(n_staging):
        name = f"stg_{idx:05d}"
        dep = source_names[i % len(source_names)]
        strategy = _pick_strategy(idx)
        models.append({
            "name": name,
            "layer": "staging",
            "deps": [dep],
            "use_rocky": i % 5 == 0,
            "strategy": strategy,
            "idx": idx,
        })
        staging_names.append(name)
        idx += 1

    # Intermediate
    for i in range(n_intermediate):
        name = f"int_{idx:05d}"
        dep1 = staging_names[i % len(staging_names)]
        dep2 = staging_names[(i + 1) % len(staging_names)]
        deps = [dep1, dep2]
        if i % 10 == 0 and len(staging_names) > 2:
            deps.append(staging_names[(i + 2) % len(staging_names)])
        strategy = _pick_strategy(idx)
        models.append({
            "name": name,
            "layer": "intermediate",
            "deps": deps,
            "use_rocky": i % 5 == 1,
            "strategy": strategy,
            "idx": idx,
        })
        intermediate_names.append(name)
        idx += 1

    # Marts
    for i in range(n_marts):
        name = f"fct_{idx:05d}"
        int_dep = intermediate_names[i % max(len(intermediate_names), 1)]
        stg_dep = staging_names[i % len(staging_names)]
        models.append({
            "name": name,
            "layer": "marts",
            "deps": [int_dep, stg_dep],
            "strategy": "full_refresh",
            "idx": idx,
        })
        idx += 1

    return models


def _pick_strategy(idx: int) -> str:
    r = idx % 20
    if r <= 15:
        return "full_refresh"
    if r <= 18:
        return "incremental"
    return "merge"


# ---------------------------------------------------------------------------
# SQL templates (shared logic, different syntax per tool)
# ---------------------------------------------------------------------------

def _source_sql(model: dict) -> str:
    cols = []
    for c in range(model["ncols"]):
        col = {
            0: f"col_{c} AS id",
            1: f"col_{c} AS name",
            2: f"CAST(col_{c} AS VARCHAR) AS category",
            3: f"ROUND(CAST(col_{c} AS DECIMAL(10,2)), 2) AS amount",
            4: f"col_{c} AS status",
            5: f"col_{c} AS created_at",
            6: f"col_{c} AS updated_at",
        }.get(c % 7, f"col_{c}")
        cols.append(col)
    return f"SELECT {', '.join(cols)} FROM raw_data.source_{model['idx']}"


def _staging_sql(dep: str) -> str:
    return (
        f"SELECT\n"
        f"    id,\n"
        f"    UPPER(TRIM(name)) AS name_clean,\n"
        f"    COALESCE(category, 'unknown') AS category_clean,\n"
        f"    amount,\n"
        f"    LOWER(status) AS status,\n"
        f"    created_at,\n"
        f"    updated_at,\n"
        f"    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\n"
        f"FROM {dep}\n"
        f"WHERE status != 'deleted'\n"
        f"  AND amount > 0"
    )


def _staging_rocky(dep: str) -> str:
    return (
        f"-- Staging: clean {dep}\n"
        f"from {dep}\n"
        f"where status != \"deleted\"\n"
        f"where amount > 0\n"
        f"derive {{\n"
        f"    name_clean: upper(name),\n"
        f"    category_clean: coalesce(category, \"unknown\"),\n"
        f"    rn: row_number() over (partition id, sort -updated_at)\n"
        f"}}\n"
    )


def _intermediate_sql(deps: list[str]) -> str:
    dep1, dep2 = deps[0], deps[1]
    extra_join = ""
    extra_col = f"b.category_clean AS category"
    if len(deps) > 2:
        extra_join = f"\n    LEFT JOIN {deps[2]} c ON a.id = c.id"
        extra_col = f"COALESCE(c.category_clean, b.category_clean) AS category"

    return (
        f"WITH base AS (\n"
        f"    SELECT\n"
        f"        a.id,\n"
        f"        a.name_clean AS name,\n"
        f"        a.amount,\n"
        f"        a.created_at,\n"
        f"        {extra_col}\n"
        f"    FROM {dep1} a\n"
        f"    JOIN {dep2} b ON a.id = b.id{extra_join}\n"
        f"    WHERE a.rn = 1\n"
        f")\n"
        f"SELECT\n"
        f"    id,\n"
        f"    name,\n"
        f"    category,\n"
        f"    COUNT(*) AS record_count,\n"
        f"    SUM(amount) AS total_amount,\n"
        f"    AVG(amount) AS avg_amount,\n"
        f"    MIN(created_at) AS first_seen,\n"
        f"    MAX(created_at) AS last_seen\n"
        f"FROM base\n"
        f"GROUP BY id, name, category"
    )


def _intermediate_rocky(dep: str) -> str:
    return (
        f"-- Intermediate: aggregate {dep}\n"
        f"from {dep}\n"
        f"where rn == 1\n"
        f"group id {{\n"
        f"    record_count: count(),\n"
        f"    total_amount: sum(amount),\n"
        f"    avg_amount: avg(amount),\n"
        f"    first_seen: min(created_at),\n"
        f"    last_seen: max(created_at)\n"
        f"}}\n"
        f"where record_count >= 2\n"
    )


def _mart_sql(int_dep: str, stg_dep: str) -> str:
    return (
        f"SELECT\n"
        f"    i.id,\n"
        f"    i.name,\n"
        f"    i.total_amount,\n"
        f"    i.record_count,\n"
        f"    i.avg_amount,\n"
        f"    CASE\n"
        f"        WHEN i.total_amount > 10000 THEN 'high'\n"
        f"        WHEN i.total_amount > 1000 THEN 'medium'\n"
        f"        ELSE 'low'\n"
        f"    END AS tier,\n"
        f"    SUM(i.total_amount) OVER (ORDER BY i.first_seen ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total,\n"
        f"    RANK() OVER (ORDER BY i.total_amount DESC) AS amount_rank\n"
        f"FROM {int_dep} i\n"
        f"LEFT JOIN {stg_dep} s ON i.id = s.id\n"
        f"WHERE i.record_count >= 2"
    )


# ---------------------------------------------------------------------------
# Rocky project generator
# ---------------------------------------------------------------------------

def generate_rocky_project(models: list[dict], output_dir: Path):
    """Generate a Rocky project from the model DAG."""
    models_dir = output_dir / "models"
    models_dir.mkdir(parents=True, exist_ok=True)

    # Generate rocky.toml for validate benchmarks
    (output_dir / "rocky.toml").write_text(
        '[adapter]\n'
        'type = "duckdb"\n'
        'path = "benchmark.duckdb"\n\n'
        '[pipeline.benchmark]\n'
        'strategy = "full_refresh"\n\n'
        '[pipeline.benchmark.source]\n'
        'schema_pattern = { prefix = "raw__", separator = "__", components = ["source"] }\n\n'
        '[pipeline.benchmark.target]\n'
        'catalog_template = "warehouse"\n'
        'schema_template = "staging"\n'
    )

    for m in models:
        name = m["name"]
        layer = m["layer"]
        deps = m["deps"]

        # Generate TOML sidecar
        strategy_toml = _strategy_toml(m["strategy"])
        if deps:
            quoted = ", ".join(f'"{d}"' for d in deps)
            deps_line = f"depends_on = [{quoted}]"
        else:
            deps_line = ""
        toml_content = (
            f'name = "{name}"\n'
            f'{deps_line}\n\n'
            f'[strategy]\n{strategy_toml}\n\n'
            f'[target]\ncatalog = "warehouse"\nschema = "{layer}"\ntable = "{name}"\n'
        )
        (models_dir / f"{name}.toml").write_text(toml_content)

        # Generate SQL or Rocky DSL
        if layer == "sources":
            sql = _source_sql(m)
            (models_dir / f"{name}.sql").write_text(sql)
        elif layer == "staging":
            if m.get("use_rocky"):
                (models_dir / f"{name}.rocky").write_text(_staging_rocky(deps[0]))
            else:
                (models_dir / f"{name}.sql").write_text(_staging_sql(deps[0]))
        elif layer == "intermediate":
            if m.get("use_rocky"):
                (models_dir / f"{name}.rocky").write_text(_intermediate_rocky(deps[0]))
            else:
                (models_dir / f"{name}.sql").write_text(_intermediate_sql(deps))
        elif layer == "marts":
            (models_dir / f"{name}.sql").write_text(_mart_sql(deps[0], deps[1]))


def _strategy_toml(strategy: str) -> str:
    if strategy == "incremental":
        return 'type = "incremental"\ntimestamp_column = "updated_at"'
    if strategy == "merge":
        return 'type = "merge"\nunique_key = ["id"]'
    return 'type = "full_refresh"'


# ---------------------------------------------------------------------------
# dbt project generator
# ---------------------------------------------------------------------------

def generate_dbt_project(models: list[dict], output_dir: Path, threads: int = 4):
    """Generate an equivalent dbt project from the model DAG."""
    output_dir.mkdir(parents=True, exist_ok=True)

    # dbt_project.yml
    (output_dir / "dbt_project.yml").write_text(
        f"name: benchmark\n"
        f"version: '1.0.0'\n"
        f"config-version: 2\n"
        f"profile: benchmark\n\n"
        f"model-paths: ['models']\n\n"
        f"models:\n"
        f"  benchmark:\n"
        f"    sources:\n"
        f"      +materialized: table\n"
        f"    staging:\n"
        f"      +materialized: table\n"
        f"    intermediate:\n"
        f"      +materialized: table\n"
        f"    marts:\n"
        f"      +materialized: table\n"
    )

    # profiles.yml
    (output_dir / "profiles.yml").write_text(
        f"benchmark:\n"
        f"  target: dev\n"
        f"  outputs:\n"
        f"    dev:\n"
        f"      type: duckdb\n"
        f"      path: benchmark.duckdb\n"
        f"      threads: {threads}\n"
    )

    # Generate models per layer
    layers = {"sources": [], "staging": [], "intermediate": [], "marts": []}
    for m in models:
        layers[m["layer"]].append(m)

    for layer, layer_models in layers.items():
        layer_dir = output_dir / "models" / layer
        layer_dir.mkdir(parents=True, exist_ok=True)
        schema_models = []

        for m in layer_models:
            name = m["name"]
            deps = m["deps"]

            # Convert SQL: replace direct table refs with {{ ref() }}
            if layer == "sources":
                # Sources reference raw_data tables, not other models
                sql = _source_sql(m)
                dbt_sql = "{{ config(materialized='table') }}\n\n" + sql
            elif layer == "staging":
                if m.get("use_rocky"):
                    # dbt can't do Rocky DSL — generate equivalent SQL
                    dbt_sql = _dbt_staging_sql(deps[0])
                else:
                    dbt_sql = _dbt_staging_sql(deps[0])
            elif layer == "intermediate":
                if m.get("use_rocky"):
                    dbt_sql = _dbt_intermediate_sql_simple(deps[0])
                else:
                    dbt_sql = _dbt_intermediate_sql(deps)
            elif layer == "marts":
                dbt_sql = _dbt_mart_sql(deps[0], deps[1])

            (layer_dir / f"{name}.sql").write_text(dbt_sql)
            schema_models.append({"name": name, "columns": _schema_columns(layer)})

        # Write schema.yml per layer
        _write_schema_yml(layer_dir / "schema.yml", schema_models)


def _dbt_staging_sql(dep: str) -> str:
    return (
        "{{ config(materialized='table') }}\n\n"
        f"SELECT\n"
        f"    id,\n"
        f"    UPPER(TRIM(name)) AS name_clean,\n"
        f"    COALESCE(category, 'unknown') AS category_clean,\n"
        f"    amount,\n"
        f"    LOWER(status) AS status,\n"
        f"    created_at,\n"
        f"    updated_at,\n"
        f"    ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn\n"
        f"FROM {{{{ ref('{dep}') }}}}\n"
        f"WHERE status != 'deleted'\n"
        f"  AND amount > 0"
    )


def _dbt_intermediate_sql(deps: list[str]) -> str:
    dep1, dep2 = deps[0], deps[1]
    extra_join = ""
    extra_col = "b.category_clean AS category"
    if len(deps) > 2:
        extra_join = f"\n    LEFT JOIN {{{{ ref('{deps[2]}') }}}} c ON a.id = c.id"
        extra_col = "COALESCE(c.category_clean, b.category_clean) AS category"

    return (
        "{{ config(materialized='table') }}\n\n"
        f"WITH base AS (\n"
        f"    SELECT\n"
        f"        a.id,\n"
        f"        a.name_clean AS name,\n"
        f"        a.amount,\n"
        f"        a.created_at,\n"
        f"        {extra_col}\n"
        f"    FROM {{{{ ref('{dep1}') }}}} a\n"
        f"    JOIN {{{{ ref('{dep2}') }}}} b ON a.id = b.id{extra_join}\n"
        f"    WHERE a.rn = 1\n"
        f")\n"
        f"SELECT\n"
        f"    id,\n"
        f"    name,\n"
        f"    category,\n"
        f"    COUNT(*) AS record_count,\n"
        f"    SUM(amount) AS total_amount,\n"
        f"    AVG(amount) AS avg_amount,\n"
        f"    MIN(created_at) AS first_seen,\n"
        f"    MAX(created_at) AS last_seen\n"
        f"FROM base\n"
        f"GROUP BY id, name, category"
    )


def _dbt_intermediate_sql_simple(dep: str) -> str:
    """For models that were Rocky DSL — generate equivalent dbt SQL."""
    return (
        "{{ config(materialized='table') }}\n\n"
        f"SELECT\n"
        f"    id,\n"
        f"    COUNT(*) AS record_count,\n"
        f"    SUM(amount) AS total_amount,\n"
        f"    AVG(amount) AS avg_amount,\n"
        f"    MIN(created_at) AS first_seen,\n"
        f"    MAX(created_at) AS last_seen\n"
        f"FROM {{{{ ref('{dep}') }}}}\n"
        f"WHERE rn = 1\n"
        f"GROUP BY id\n"
        f"HAVING COUNT(*) >= 2"
    )


def _dbt_mart_sql(int_dep: str, stg_dep: str) -> str:
    return (
        "{{ config(materialized='table') }}\n\n"
        f"SELECT\n"
        f"    i.id,\n"
        f"    i.name,\n"
        f"    i.total_amount,\n"
        f"    i.record_count,\n"
        f"    i.avg_amount,\n"
        f"    CASE\n"
        f"        WHEN i.total_amount > 10000 THEN 'high'\n"
        f"        WHEN i.total_amount > 1000 THEN 'medium'\n"
        f"        ELSE 'low'\n"
        f"    END AS tier,\n"
        f"    SUM(i.total_amount) OVER (ORDER BY i.first_seen ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total,\n"
        f"    RANK() OVER (ORDER BY i.total_amount DESC) AS amount_rank\n"
        f"FROM {{{{ ref('{int_dep}') }}}} i\n"
        f"LEFT JOIN {{{{ ref('{stg_dep}') }}}} s ON i.id = s.id\n"
        f"WHERE i.record_count >= 2"
    )


def _schema_columns(layer: str) -> list[dict]:
    """Return representative schema columns per layer for schema.yml."""
    if layer == "sources":
        return [{"name": "id"}, {"name": "name"}]
    if layer == "staging":
        return [{"name": "id"}, {"name": "name_clean"}, {"name": "rn"}]
    if layer == "intermediate":
        return [{"name": "id"}, {"name": "record_count"}, {"name": "total_amount"}]
    return [{"name": "id"}, {"name": "tier"}, {"name": "amount_rank"}]


def _write_schema_yml(path: Path, models: list[dict]):
    """Write a dbt schema.yml file."""
    lines = ["version: 2", "", "models:"]
    for m in models:
        lines.append(f"  - name: {m['name']}")
        if m.get("columns"):
            lines.append("    columns:")
            for col in m["columns"]:
                lines.append(f"      - name: {col['name']}")
    path.write_text("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Generate benchmark projects")
    parser.add_argument("--scale", type=int, required=True, help="Number of models (e.g., 10000)")
    parser.add_argument("--output-dir", type=str, default=".", help="Base output directory")
    parser.add_argument("--tool", choices=["rocky", "dbt", "both"], default="both")
    parser.add_argument("--threads", type=int, default=4, help="dbt thread count")
    args = parser.parse_args()

    output = Path(args.output_dir)
    models = generate_dag(args.scale)

    total_sql = sum(1 for m in models if not m.get("use_rocky"))
    total_rocky = sum(1 for m in models if m.get("use_rocky"))
    print(f"Generated DAG: {len(models)} models ({total_sql} SQL, {total_rocky} Rocky DSL)")

    if args.tool in ("rocky", "both"):
        rocky_dir = output / f"rocky_{args.scale}"
        generate_rocky_project(models, rocky_dir)
        file_count = sum(1 for _ in (rocky_dir / "models").iterdir())
        print(f"Rocky project: {rocky_dir} ({file_count} files)")

    if args.tool in ("dbt", "both"):
        dbt_dir = output / f"dbt_{args.scale}"
        generate_dbt_project(models, dbt_dir, threads=args.threads)
        file_count = sum(
            1 for _ in (dbt_dir / "models").rglob("*.sql")
        )
        print(f"dbt project:   {dbt_dir} ({file_count} SQL files)")


if __name__ == "__main__":
    main()
