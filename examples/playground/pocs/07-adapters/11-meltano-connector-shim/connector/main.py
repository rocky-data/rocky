#!/usr/bin/env python3
"""Connector shim: a Singer tap run by Meltano -> Parquet, state kept by Rocky.

`rocky load --connector <name>` starts this program once. It reads the request
JSON from stdin, runs one Singer tap of a Meltano project with
`meltano invoke`, and writes the records of ONE stream to one Parquet file.

Rocky ships no extractors and no catalog. This file only adapts Singer and
Meltano to the Rocky child connector protocol:

  1. `meltano invoke <tap> --discover` prints the tap's catalog.
  2. The shim marks only the stream named in the options as selected. The
     stream must exist, or the run fails and lists the choices.
  3. `meltano invoke <tap> --catalog <file> --state <file>` runs the tap.
     `--state` is always passed: the saved state, or `{}` when Rocky has none.
     Without it Meltano would pick up a stale state file of its own.
  4. RECORD messages of the stream go to JSONL, then to Parquet through the
     duckdb CLI. SCHEMA messages are ignored (DuckDB infers the types).
  5. The LAST STATE message is the Rocky state. A non-zero exit fails the
     run and the state does not move. So does a run where the tap emitted
     records but none for the selected stream.

The shim runs `meltano invoke` only. It never uses `meltano run` or
`meltano elt`, which keep Singer state in Meltano's own state backend
(`.meltano/`). That state would live where Rocky's commit fence cannot see
it. The only Singer state is the one in Rocky state.

Meltano writes the tap's resolved config (secrets included) to a file under
its system directory on every invoke. The shim gives each run a private
system directory (mode 0700, in the temp dir) with MELTANO_SYS_DIR_ROOT, links
the installed plugins into it, and removes it when the run ends. Nothing is
written into the project directory.

Options (`[connector.<name>.options]`, read from options.json):
    project_dir   Meltano project directory, relative to this file (required)
    tap           the plugin name in meltano.yml (required)
    stream        the one stream to load (required)
    meltano_bin   the meltano executable, relative to this file
                  (default "../.venv/bin/meltano")

Tap credentials: declare the environment variables under `secrets` in
rocky.toml. Meltano reads a plugin setting from the variable
`<PLUGIN>_<SETTING>` (for example TAP_ORDERS_API_TOKEN). Rocky hands those
variables to this program, and this program leaves them in the environment of
`meltano invoke`.

State (schema version 1): the Singer STATE `value`, for example
    {"bookmarks": {"orders": {"replication_key": "id", "replication_key_value": 8}}}
A `bookmarks` map is cut down to the loaded stream. Any other keys stay.

Python 3.8 or newer, standard library only, plus the duckdb CLI.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

PROTOCOL_VERSION = 0
STATE_SCHEMA_VERSION = 1
RESULT_FILE = "_rocky_result.json"
BATCH_FILE = "batch-0.parquet"
MAX_STATE_BYTES = 900 * 1024
HERE = os.path.dirname(os.path.abspath(__file__))
os.umask(0o077)


def log(message):
    print("meltano-shim: " + message, file=sys.stderr, flush=True)


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
    for key in ("project_dir", "tap", "stream"):
        if not isinstance(options.get(key), str) or not options[key]:
            fail("[connector.<name>.options] needs a non-empty string %r" % key)
    options["project_dir"] = os.path.normpath(os.path.join(HERE, options["project_dir"]))
    options["meltano_bin"] = os.path.normpath(
        os.path.join(HERE, options.get("meltano_bin") or "../.venv/bin/meltano")
    )
    if not os.path.isfile(os.path.join(options["project_dir"], "meltano.yml")):
        fail("no meltano.yml in %s" % options["project_dir"])
    if not os.path.isfile(options["meltano_bin"]):
        fail("no meltano executable at %s; install it first" % options["meltano_bin"])
    return options


def read_saved_state(request):
    state = request.get("state")
    if state is None:
        return None
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        fail("unknown state schema version %r" % state.get("schema_version"))
    value = state.get("value")
    if value is not None and not isinstance(value, dict):
        fail("the saved state is not a JSON object")
    return value


def make_sys_dir(options):
    """A private Meltano system directory for this run.

    Plugin virtualenvs are linked in from the project's own `.meltano`. The
    run directory (where Meltano writes the resolved tap config) is new.
    """
    sys_dir = tempfile.mkdtemp(prefix="meltano-sys-")
    project_sys = os.path.join(options["project_dir"], ".meltano")
    if os.path.isdir(project_sys):
        for name in os.listdir(project_sys):
            if name not in ("run", "logs"):
                os.symlink(os.path.join(project_sys, name), os.path.join(sys_dir, name))
    return sys_dir


def invoke(options, sys_dir, args, on_line):
    """Run `meltano invoke <tap> args`, calling on_line(line) per stdout line.

    Returns the exit code. Stdout is read as it arrives, never held whole.
    """
    env = dict(os.environ)
    env["MELTANO_PROJECT_ROOT"] = options["project_dir"]
    env["MELTANO_SYS_DIR_ROOT"] = sys_dir
    env["MELTANO_DISABLE_TRACKING"] = "true"
    command = [options["meltano_bin"], "invoke", options["tap"]] + args
    err_path = os.path.join(sys_dir, "stderr.log")
    # The tap resolves relative paths in its settings against the project.
    try:
        with open(err_path, "w", encoding="utf-8") as err:
            proc = subprocess.Popen(
                command, cwd=options["project_dir"], env=env, stdout=subprocess.PIPE,
                stderr=err, universal_newlines=True,
            )
            for line in proc.stdout:
                on_line(line)
            code = proc.wait()
    except OSError as error:
        fail("cannot start meltano: %s" % error)
    with open(err_path, encoding="utf-8", errors="replace") as err:
        for line in err.read().splitlines():
            log("meltano: " + line)
    return code


def select_stream(options, sys_dir, work_dir):
    chunks = []
    code = invoke(options, sys_dir, ["--discover"], chunks.append)
    stdout = "".join(chunks)
    if code != 0:
        fail("`meltano invoke --discover` exited with code %d" % code)
    try:
        catalog = json.loads(stdout)
        streams = catalog["streams"]
    except (ValueError, KeyError, TypeError) as error:
        fail("the discover output is not a Singer catalog: %s" % error)
    names = sorted(s.get("tap_stream_id") or s.get("stream") for s in streams)
    if options["stream"] not in names:
        fail(
            "the tap has no stream %r; its streams are: %s. Rocky loads one "
            "stream per connector: set options.stream to one of them."
            % (options["stream"], ", ".join(names))
        )
    for entry in streams:
        selected = (entry.get("tap_stream_id") or entry.get("stream")) == options["stream"]
        metadata = entry.setdefault("metadata", [])
        top = [m for m in metadata if m.get("breadcrumb") == []]
        if top:
            top[0].setdefault("metadata", {})["selected"] = selected
        else:
            metadata.append({"breadcrumb": [], "metadata": {"selected": selected}})
    log("streams in the tap: %s; loading only %r" % (", ".join(names), options["stream"]))
    path = os.path.join(work_dir, "catalog.json")
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(catalog, handle)
    return path


def cut_state(value, stream):
    if not isinstance(value, dict):
        return {}
    value = dict(value)
    bookmarks = value.get("bookmarks")
    if isinstance(bookmarks, dict):
        value["bookmarks"] = dict((k, v) for k, v in bookmarks.items() if k == stream)
    return value


def to_parquet(jsonl_path, parquet_path):
    sql = (
        "COPY (SELECT * FROM read_json(%s, format = 'newline_delimited', "
        "sample_size = -1, union_by_name = true)) TO %s (FORMAT parquet);\n"
        "SELECT CAST(count(*) AS VARCHAR) AS n FROM read_parquet(%s);\n"
        % (literal(jsonl_path), literal(parquet_path), literal(parquet_path))
    )
    try:
        done = subprocess.run(
            ["duckdb", "-json", "-bail"], input=sql, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, universal_newlines=True, check=False,
        )
    except FileNotFoundError:
        fail("the duckdb CLI is not on PATH")
    if done.returncode != 0:
        for line in done.stderr.splitlines():
            log("duckdb: " + line)
        fail("duckdb could not convert the records to Parquet")
    try:
        return int(json.loads(done.stdout)[0]["n"])
    except (ValueError, KeyError, IndexError) as error:
        fail("cannot parse the duckdb row count: %s" % error)


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
    saved = read_saved_state(request)

    sys_dir = make_sys_dir(options)
    try:
        run_tap(options, saved, output_dir, sys_dir)
    finally:
        shutil.rmtree(sys_dir, ignore_errors=True)


def run_tap(options, saved, output_dir, sys_dir):
    catalog_path = select_stream(options, sys_dir, output_dir)
    # Always pass --state: with none, Meltano may read a stale state.json of
    # its own from an earlier run. Rocky state is the only source of truth.
    state_path = os.path.join(output_dir, "state.json")
    with open(state_path, "w", encoding="utf-8") as handle:
        json.dump(saved if saved is not None else {}, handle)
    args = ["--catalog", catalog_path, "--state", state_path]
    log("resuming from saved state (--state)" if saved is not None
        else "no saved state; first run (--state {})")

    records_path = os.path.join(output_dir, "records.jsonl")
    counts = {"records": 0, "seen": 0}
    other = {}
    box = {"state": None}

    def on_line(line):
        line = line.strip()
        if not line:
            return
        try:
            message = json.loads(line)
        except ValueError:
            log("ignored a non-JSON line on stdout (%d bytes)" % len(line))
            return
        kind = message.get("type") if isinstance(message, dict) else None
        if kind == "RECORD":
            counts["seen"] += 1
            if message.get("stream") == options["stream"]:
                out.write(json.dumps(message["record"]) + "\n")
                counts["records"] += 1
            else:
                name = str(message.get("stream"))
                other[name] = other.get(name, 0) + 1
        elif kind == "STATE":
            box["state"] = message.get("value")
        elif kind == "BATCH":
            fail("the tap emitted BATCH messages; this shim reads RECORD messages only")

    with open(records_path, "w", encoding="utf-8") as out:
        code = invoke(options, sys_dir, args, on_line)
    if code != 0:
        fail("`meltano invoke %s` exited with code %d" % (options["tap"], code))
    records, last_state = counts["records"], box["state"]
    if other:
        log("ignored records of other streams: %s" % ", ".join(
            "%s=%d" % (k, v) for k, v in sorted(other.items())))
    if counts["seen"] > 0 and records == 0:
        fail(
            "the tap emitted %d record(s) but none for stream %r (it emitted: %s); "
            "refusing to move the state past a lost batch"
            % (counts["seen"], options["stream"], ", ".join(sorted(other)))
        )

    if last_state is None:
        # A tap with no state support emits no STATE. Carry the old state.
        last_state = saved if saved is not None else {}
        log("the tap emitted no STATE message; keeping the saved state")
    state_value = cut_state(last_state, options["stream"])
    if len(json.dumps(state_value).encode("utf-8")) > MAX_STATE_BYTES:
        fail("the Singer state is larger than 900 KiB; Rocky refuses state over 1 MiB")

    files = []
    if records > 0:
        batch_path = os.path.join(output_dir, BATCH_FILE)
        rows = to_parquet(records_path, batch_path)
        files.append({"path": BATCH_FILE, "bytes": os.path.getsize(batch_path), "rows": rows})
    os.remove(records_path)
    log("read %d record(s) of stream %r" % (records, options["stream"]))

    result = {
        "protocol_version": PROTOCOL_VERSION,
        "files": files,
        "state": {"schema_version": STATE_SCHEMA_VERSION, "value": state_value},
    }
    with open(os.path.join(output_dir, RESULT_FILE), "w", encoding="utf-8") as handle:
        json.dump(result, handle)


if __name__ == "__main__":
    main()
