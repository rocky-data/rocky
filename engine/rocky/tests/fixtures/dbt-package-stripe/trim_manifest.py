"""Trim a dbt compile's target/ to the fields `rocky package` reads.

Usage: python trim_manifest.py <dbt project dir> <fixture compiled/ dir>

Keeps the model, snapshot and test nodes and the sources, with only the keys
the importer parses (see rocky-compiler `dbt_manifest.rs` and
`dbt_package.rs`), and the run_results.json evidence the incremental gate
checks. Everything else (macros, docs, column docs, analyses) is dropped.
"""
import json
import pathlib
import shutil
import sys

src, dst = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2])
m = json.loads((src / "target/manifest.json").read_text())
NODE_KEYS = ["unique_id", "name", "resource_type", "package_name", "compiled_code",
             "depends_on", "schema", "database", "relation_name", "test_metadata",
             "attached_node", "column_name", "tags", "access", "group", "version",
             "latest_version"]
CONFIG_KEYS = ["materialized", "schema", "alias", "enabled", "severity", "where",
               "full_refresh", "unique_key", "incremental_strategy", "on_schema_change"]
nodes = {}
for uid, n in m["nodes"].items():
    if n["resource_type"] not in ("model", "snapshot", "test"):
        continue
    out = {k: n[k] for k in NODE_KEYS if n.get(k) not in (None, [], {})}
    out["depends_on"] = {"nodes": n["depends_on"]["nodes"], "macros": []}
    out["config"] = {k: n["config"][k] for k in CONFIG_KEYS if n["config"].get(k) is not None}
    if n["config"].get("materialized") in ("incremental", "snapshot"):
        out["raw_code"] = n.get("raw_code", "")
    nodes[uid] = out
sources = {uid: {k: s.get(k) for k in ["unique_id", "name", "source_name", "package_name",
                                         "database", "schema", "identifier", "relation_name"]}
           for uid, s in m["sources"].items()}
meta = {k: m["metadata"][k] for k in ["dbt_schema_version", "dbt_version", "generated_at",
                                      "invocation_id", "project_name", "adapter_type"]}
(dst / "target").mkdir(parents=True, exist_ok=True)
(dst / "target/manifest.json").write_text(
    json.dumps({"metadata": meta, "nodes": nodes, "sources": sources}, indent=1, sort_keys=True) + "\n")
rr = json.loads((src / "target/run_results.json").read_text())
(dst / "target/run_results.json").write_text(json.dumps({
    "metadata": {"invocation_id": rr["metadata"]["invocation_id"]},
    "args": {"full_refresh": rr["args"].get("full_refresh")},
    "results": [{"unique_id": r["unique_id"], "status": r["status"]} for r in rr["results"]],
}, indent=1, sort_keys=True) + "\n")
shutil.copy(src / "package-lock.yml", dst / "package-lock.yml")
