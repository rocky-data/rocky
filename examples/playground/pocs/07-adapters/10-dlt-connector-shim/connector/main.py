"""Connector shim: a dlt source -> Parquet, with dlt's own state kept by Rocky.

`rocky load --connector <name>` starts this program (through launch.sh, which
picks the Python that has dlt installed) once. It reads the request JSON from
stdin and runs one user dlt source with the `filesystem` destination, writing
Parquet. It then lists the Parquet files of ONE stream in
`_rocky_result.json`.

Rocky ships no extractors. This file only adapts dlt to the protocol:

* Options (`[connector.<name>.options]`, read from options.json):
    source_dir     directory of the user's source module, relative to this file
    module         module name inside source_dir
    source         name of the function that returns a dlt source
    stream         the one resource to load. It must write exactly one table
                   (dlt may rename the table; nested lists make child tables,
                   which fail the run); the shim fails when the
                   source has no such resource
    source_args    optional table of keyword arguments for the function
    pipeline_name  optional, default "rocky_dlt"
* State (schema version 1):
    {"dlt_version": "<x.y.z>", "sources": <dlt pipeline state["sources"]>}
  That is dlt's source and incremental state: the cursors of every
  `dlt.sources.incremental`. It is restored with the public
  `Pipeline.managed_state()` before the run and read back from
  `Pipeline.state` after it. dlt's own pipeline directory is private to the
  run, removed when it ends and never read again, and dlt is told not to
  restore state from the destination. Rocky's commit fence is the only thing that moves the
  cursors, so a crash before the state commit replays the batch (at least
  once) instead of skipping it.

The module needs dlt and the standalone `duckdb` CLI (to count Parquet rows).
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

# Set before dlt is imported. The shim must call no host.
os.environ["RUNTIME__DLTHUB_TELEMETRY"] = "false"
os.environ["RESTORE_FROM_DESTINATION"] = "false"
os.umask(0o077)

HERE = os.path.dirname(os.path.abspath(__file__))
PROTOCOL_VERSION = 0
STATE_SCHEMA_VERSION = 1
MAX_STATE_BYTES = 900 * 1024
RESULT_FILE = "_rocky_result.json"


def log(message):
    print("dlt-shim: " + message, file=sys.stderr, flush=True)


def fail(message):
    log("error: " + message)
    sys.exit(1)


def literal(text):
    return "'" + text.replace("'", "''") + "'"


def load_options(request):
    path = request.get("options_path")
    if not isinstance(path, str) or not path:
        fail("the request has no options_path; this Rocky is too old")
    try:
        with open(path, encoding="utf-8") as handle:
            options = json.load(handle)
    except (OSError, ValueError) as error:
        fail("cannot read %s: %s" % (path, error))
    for key in ("source_dir", "module", "source", "stream"):
        if not isinstance(options.get(key), str) or not options[key]:
            fail("[connector.<name>.options] needs a non-empty string %r" % key)
    args = options.get("source_args", {})
    if not isinstance(args, dict):
        fail("source_args must be a table")
    options["source_args"] = args
    name = options.get("pipeline_name", "rocky_dlt")
    if not isinstance(name, str) or not name:
        fail("pipeline_name must be a non-empty string")
    options["pipeline_name"] = name
    return options


def read_saved_sources(request):
    state = request.get("state")
    if state is None:
        return None
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        fail("unknown state schema version %r" % state.get("schema_version"))
    value = state.get("value") or {}
    sources = value.get("sources")
    if sources is not None and not isinstance(sources, dict):
        fail("the saved state has a malformed 'sources' entry")
    return sources


def build_source(options):
    import importlib

    source_dir = os.path.normpath(os.path.join(HERE, options["source_dir"]))
    sys.path.insert(0, source_dir)
    try:
        module = importlib.import_module(options["module"])
    except ImportError as error:
        fail("cannot import module %r from %s: %s" % (options["module"], source_dir, error))
    factory = getattr(module, options["source"], None)
    if factory is None:
        fail("module %r has no function %r" % (options["module"], options["source"]))
    produced = factory(**options["source_args"])
    resources = getattr(produced, "resources", None)
    if resources is None:
        fail("%r did not return a dlt source (no .resources)" % options["source"])
    names = sorted(resources.keys())
    if options["stream"] not in names:
        fail(
            "the source has no stream %r; its streams are: %s. "
            "Rocky loads one stream per connector: set options.stream to one of them."
            % (options["stream"], ", ".join(names))
        )
    log("streams in the source: %s; loading only %r" % (", ".join(names), options["stream"]))
    return produced.with_resources(options["stream"])


def count_rows(path):
    try:
        done = subprocess.run(
            ["duckdb", "-json", "-bail"],
            input="SELECT CAST(count(*) AS VARCHAR) AS n FROM read_parquet(%s);\n" % literal(path),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            check=False,
        )
    except FileNotFoundError:
        fail("the duckdb CLI is not on PATH")
    if done.returncode != 0:
        for line in done.stderr.splitlines():
            log("duckdb: " + line)
        fail("duckdb could not read %s" % path)
    try:
        return int(json.loads(done.stdout)[0]["n"])
    except (ValueError, KeyError, IndexError) as error:
        fail("cannot parse the duckdb row count: %s" % error)


def write_result(output_dir, files, state_value):
    encoded = json.dumps(state_value)
    if len(encoded.encode("utf-8")) > MAX_STATE_BYTES:
        fail("the dlt state is larger than 900 KiB; Rocky refuses state over 1 MiB")
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "files": files,
        "state": {"schema_version": STATE_SCHEMA_VERSION, "value": state_value},
    }
    with open(os.path.join(output_dir, RESULT_FILE), "w", encoding="utf-8") as handle:
        json.dump(result, handle)


def loaded_tables(pipeline, stream):
    """Names of the tables the selected resource writes, by dlt's own schema.

    dlt normalizes names (`order-items` becomes `order_items`), a resource can
    set `table_name`, and nested lists become child tables (`orders__items`).
    """
    data_tables = pipeline.default_schema.data_tables()
    names = set(t["name"] for t in data_tables if t.get("resource") == stream)
    # A child table has a `parent`, not a `resource`. Follow the parents.
    grew = True
    while grew:
        grew = False
        for table in data_tables:
            if table["name"] not in names and table.get("parent") in names:
                names.add(table["name"])
                grew = True
    return sorted(names)


def run_pipeline(dlt, filesystem, options, saved, source, dest_dir, work_dir):
    stream = options["stream"]
    pipeline = dlt.pipeline(
        pipeline_name=options["pipeline_name"],
        destination=filesystem(bucket_url="file://" + dest_dir),
        dataset_name="rocky",
        pipelines_dir=os.path.join(work_dir, "pipelines"),
    )
    if saved:
        # Restore dlt's incremental state from Rocky's saved state.
        with pipeline.managed_state(extract_state=False) as state:
            state["sources"] = saved
        log("restored dlt state for source(s): %s" % ", ".join(sorted(saved)))
    else:
        log("no saved state; first run")

    try:
        info = pipeline.run(source, loader_file_format="parquet")
        info.raise_on_failed_jobs()
    except Exception as error:  # dlt wraps every step failure in its own types
        fail("dlt run failed: %s: %s" % (type(error).__name__, error))

    # Rows dlt normalized for this resource, from its trace. Child tables count.
    row_counts = {}
    normalize = getattr(pipeline.last_trace, "last_normalize_info", None)
    if normalize is not None:
        row_counts = dict(normalize.row_counts)
    tables = loaded_tables(pipeline, stream)
    written = [t for t in tables if row_counts.get(t, 0) > 0]
    extracted = sum(row_counts.get(t, 0) for t in tables)

    if len(written) > 1:
        fail(
            "stream %r wrote %d tables with rows (%s). Rocky loads one table per "
            "connector. Flatten the nested lists (max_table_nesting=0), or give the "
            "resource one fixed table_name."
            % (stream, len(written), ", ".join(sorted(written)))
        )
    files = []
    table_name = written[0] if written else None
    if table_name is not None:
        table_dir = os.path.join(dest_dir, "rocky", table_name)
        names = sorted(n for n in os.listdir(table_dir) if n.endswith(".parquet")) if os.path.isdir(table_dir) else []
        for name in names:
            path = os.path.join(table_dir, name)
            files.append(
                {
                    "path": os.path.relpath(path, os.path.dirname(dest_dir)),
                    "bytes": os.path.getsize(path),
                    "rows": count_rows(path),
                }
            )
    loaded = sum(item["rows"] for item in files)
    if extracted > 0 and loaded == 0:
        fail(
            "dlt extracted %d row(s) for stream %r but the shim found no Parquet rows "
            "for it; refusing to move the cursor past a lost batch" % (extracted, stream)
        )
    if extracted != loaded:
        fail("dlt extracted %d row(s) for stream %r but the Parquet files hold %d" % (extracted, stream, loaded))

    new_sources = pipeline.state.get("sources", {})
    if loaded == 0 and new_sources != (saved or {}):
        log("warning: the dlt state moved with 0 rows; the source advanced its cursor without records")
    state_value = {"dlt_version": dlt.__version__, "sources": new_sources}
    log(
        "dlt %s wrote %d file(s), %d row(s) for table %s"
        % (dlt.__version__, len(files), loaded, table_name)
    )
    return files, state_value


def main():
    try:
        request = json.load(sys.stdin)
    except ValueError as error:
        fail("the request on stdin is not JSON: %s" % error)
    if request.get("protocol_version") != PROTOCOL_VERSION:
        fail("unknown protocol version %r" % request.get("protocol_version"))
    output_dir = request.get("output_dir")
    if not isinstance(output_dir, str) or not os.path.isdir(output_dir):
        fail("the request has no usable output_dir")

    options = load_options(request)
    saved = read_saved_sources(request)

    try:
        import dlt
        from dlt.destinations import filesystem
    except ImportError as error:
        fail("dlt is not installed in this Python: %s" % error)

    source = build_source(options)

    dest_dir = os.path.join(output_dir, "dlt_destination")
    # dlt keeps its resolved config and a trace of the run in the pipelines
    # directory. It is private, and removed when the run ends, whatever the
    # outcome (a SIGKILL can still leave it behind).
    work_dir = tempfile.mkdtemp(prefix="dlt-work-", dir=output_dir)
    try:
        files, state_value = run_pipeline(dlt, filesystem, options, saved, source, dest_dir, work_dir)
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)
    write_result(output_dir, files, state_value)


if __name__ == "__main__":
    main()
