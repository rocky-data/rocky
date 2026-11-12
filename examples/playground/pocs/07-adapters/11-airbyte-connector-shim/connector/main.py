#!/usr/bin/env python3
"""Connector shim: an Airbyte source image -> Parquet, state kept by Rocky.

`rocky load --connector <name>` starts this program once. It reads the request
JSON from stdin, runs one open-source Airbyte source image with docker, and
writes the records of ONE stream to one Parquet file.

Rocky ships no extractors and no catalog. This file only adapts the Airbyte
protocol to the Rocky child connector protocol:

  1. `spec`      logs the connector name and whether it supports incremental.
  2. `discover`  lists the streams. The stream named in the options must be
                 one of them, or the run fails and lists the choices.
  3. The shim builds a configured catalog with that one stream:
                 incremental + append if the stream supports incremental,
                 else full_refresh + append. (`sync_mode = "full_refresh"` in
                 the options forces full refresh.)
  4. `read --config --catalog [--state]` runs. RECORD.data lines of the
     stream go to JSONL, then to Parquet through the duckdb CLI.
  5. The LAST STATE message becomes the Rocky state. A TRACE message of type
     ERROR, or a non-zero exit, fails the run and the state does not move.

Options (`[connector.<name>.options]`, read from options.json):
    image            Docker image with a pinned tag (required)
    stream           the one stream to load (required)
    config           table: the source's connection config (no secrets)
    config_env       table: config key -> NAME of an environment variable.
                     NAME must be declared under `secrets` in rocky.toml.
                     Use it for tokens and passwords.
    namespace        optional; the stream's namespace, when two streams share a name
    sync_mode        "auto" (default) or "full_refresh"
    docker_network   passed to `docker run --network`. Omit for docker's
                     default. "none" cuts the container off (offline sources).

State (schema version 1):
    {"state_type": "STREAM" | "GLOBAL" | "LEGACY", "message": <Airbyte state>}
`message` is the state object as the source emitted it (without
`sourceStats`). On the next run it is written back to the `--state` file in
the shape the source expects: a one-element list for STREAM and GLOBAL, the
bare `data` object for LEGACY.

The shim never pulls an image (`docker run --pull never`): pull it yourself.
Python 3.8 or newer, standard library only, plus the docker and duckdb CLIs.
"""

import json
import os
import re
import shutil
import signal
import subprocess
import sys
import tempfile

PROTOCOL_VERSION = 0
STATE_SCHEMA_VERSION = 1
RESULT_FILE = "_rocky_result.json"
BATCH_FILE = "batch-0.parquet"
MOUNT = "/rocky_airbyte"
MAX_STATE_BYTES = 900 * 1024
# Names of the containers this run started. They are removed when the run ends.
CONTAINERS = []
CONTAINER_PREFIX = "rocky-airbyte"


def log(message):
    print("airbyte-shim: " + message, file=sys.stderr, flush=True)


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
    for key in ("image", "stream"):
        if not isinstance(options.get(key), str) or not options[key]:
            fail("[connector.<name>.options] needs a non-empty string %r" % key)
    options.setdefault("config", {})
    options.setdefault("config_env", {})
    if not isinstance(options["config"], dict) or not isinstance(options["config_env"], dict):
        fail("config and config_env must be tables")
    options.setdefault("sync_mode", "auto")
    if options["sync_mode"] not in ("auto", "full_refresh"):
        fail("sync_mode must be 'auto' or 'full_refresh'")
    if ":" not in options["image"].rsplit("/", 1)[-1] and "@" not in options["image"]:
        fail("pin the image tag or digest, for example 'airbyte/source-faker:6.2.21'")
    return options


def build_config(options):
    config = dict(options["config"])
    for key, name in options["config_env"].items():
        value = os.environ.get(name)
        if value is None:
            fail("config_env needs the environment variable %s; declare it under secrets" % name)
        config[key] = value
    return config


def write_private(path, value):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        json.dump(value, handle)


def read_state(request):
    """Return (state_type, message) from the Rocky state, or None."""
    state = request.get("state")
    if state is None:
        return None
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        fail("unknown state schema version %r" % state.get("schema_version"))
    value = state.get("value") or {}
    if value.get("message") is None:
        return None
    if value.get("state_type") not in ("STREAM", "GLOBAL", "LEGACY"):
        fail("the saved state has an unknown state_type %r" % value.get("state_type"))
    return value["state_type"], value["message"]


def remove_containers():
    """`docker rm -f` every container this run named. Errors are ignored."""
    for name in CONTAINERS:
        try:
            subprocess.run(
                ["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL, check=False,
            )
        except OSError:
            pass


def on_sigterm(signum, frame):
    # Rocky stops a run with SIGTERM (a terminal Ctrl-C sends SIGINT). The docker client
    # dies, but the container would go on, so remove it. SIGKILL cannot be
    # caught: after a SIGKILL the container (named rocky-<run>-<attempt>-<step>)
    # can outlive the run until it ends by itself.
    remove_containers()
    sys.exit(128 + signum)


def docker_run(options, work_dir, args, step, capture_to=None):
    """Run the image with `args`. Returns (exit code, stdout lines or None)."""
    name = "%s-%s" % (CONTAINER_PREFIX, step)
    CONTAINERS.append(name)
    # A container left by an earlier run with the same name would block this one.
    subprocess.run(["docker", "rm", "-f", name], stdout=subprocess.DEVNULL,
                   stderr=subprocess.DEVNULL, check=False)
    command = [
        "docker", "run", "--rm", "--pull", "never", "--name", name,
        # Files in work_dir are readable only by this user.
        "--user", "%d:%d" % (os.getuid(), os.getgid()),
        "-v", "%s:%s:ro" % (work_dir, MOUNT),
    ]
    if options.get("docker_network"):
        command += ["--network", str(options["docker_network"])]
    command += [options["image"]] + args
    try:
        if capture_to is None:
            done = subprocess.run(
                command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                universal_newlines=True, check=False,
            )
            for line in done.stderr.splitlines():
                log("docker: " + line)
            return done.returncode, done.stdout.splitlines()
        with open(capture_to, "w", encoding="utf-8") as out:
            done = subprocess.run(
                command, stdout=out, stderr=subprocess.PIPE,
                universal_newlines=True, check=False,
            )
        for line in done.stderr.splitlines():
            log("docker: " + line)
        return done.returncode, None
    except FileNotFoundError:
        fail("the docker CLI is not on PATH")


def messages(lines):
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            message = json.loads(line)
        except ValueError:
            log("ignored a non-JSON line on stdout (%d bytes)" % len(line))
            continue
        if isinstance(message, dict) and "type" in message:
            yield message


def check_trace_errors(message):
    if message["type"] == "TRACE" and message.get("trace", {}).get("type") == "ERROR":
        error = message["trace"].get("error", {})
        fail("the source reported an error: %s" % error.get("message", "(no message)"))
    if message["type"] == "LOG" and message.get("log", {}).get("level") in ("FATAL", "ERROR"):
        text = str(message["log"].get("message", "")).splitlines()
        log("source %s: %s" % (message["log"]["level"], text[0] if text else ""))


def run_spec(options, work_dir):
    code, lines = docker_run(options, work_dir, ["spec"], "spec")
    if code != 0:
        fail("`spec` exited with code %d" % code)
    for message in messages(lines):
        check_trace_errors(message)
        if message["type"] == "SPEC":
            spec = message["spec"]
            log("spec: supportsIncremental=%s" % spec.get("supportsIncremental", False))
            return spec
    fail("`spec` printed no SPEC message")


def run_discover(options, work_dir):
    code, lines = docker_run(options, work_dir, ["discover", "--config", MOUNT + "/config.json"], "discover")
    streams = None
    for message in messages(lines):
        check_trace_errors(message)
        if message["type"] == "CATALOG":
            streams = message["catalog"]["streams"]
    if code != 0:
        fail("`discover` exited with code %d" % code)
    if streams is None:
        fail("`discover` printed no CATALOG message")
    return streams


def configured_catalog(options, streams):
    wanted = options["stream"]
    names = sorted(set(s["name"] for s in streams))
    matching = [s for s in streams if s["name"] == wanted]
    if not matching:
        fail(
            "the source has no stream %r; its streams are: %s. Rocky loads one "
            "stream per connector: set options.stream to one of them."
            % (wanted, ", ".join(names))
        )
    if options.get("namespace") is not None:
        matching = [s for s in matching if s.get("namespace") == options["namespace"]]
        if not matching:
            fail("the source has no stream %r in namespace %r" % (wanted, options["namespace"]))
    if len(matching) > 1:
        fail(
            "the source has %d streams named %r, in namespaces %s. Set options.namespace."
            % (len(matching), wanted, ", ".join(repr(s.get("namespace")) for s in matching))
        )
    stream = matching[0]
    modes = stream.get("supported_sync_modes", [])
    if options["sync_mode"] == "auto" and "incremental" in modes:
        mode = "incremental"
    elif "full_refresh" in modes:
        mode = "full_refresh"
    else:
        fail("stream %r supports neither incremental nor full_refresh (%r)" % (wanted, modes))
    log("streams in the source: %s; loading only %r (%s + append)"
        % (", ".join(names), wanted, mode))
    entry = {"stream": stream, "sync_mode": mode, "destination_sync_mode": "append"}
    if mode == "incremental":
        entry["cursor_field"] = stream.get("default_cursor_field") or []
    if stream.get("source_defined_primary_key"):
        entry["primary_key"] = stream["source_defined_primary_key"]
    return {"streams": [entry]}, mode, stream.get("namespace")


def state_file_value(state_type, message):
    """The shape `read --state` expects."""
    if state_type == "LEGACY":
        return message
    return [dict(message, type=state_type)]


def split_state(message):
    """(state_type, state object without sourceStats) of a STATE message."""
    state = message.get("state") or {}
    if "data" in state and "type" not in state:
        return "LEGACY", state["data"]
    state_type = state.get("type") or ("LEGACY" if "data" in state else None)
    if state_type == "LEGACY":
        return "LEGACY", state.get("data")
    if state_type not in ("STREAM", "GLOBAL"):
        fail("a STATE message has an unknown type %r" % state_type)
    kept = dict((k, v) for k, v in state.items() if k not in ("type", "sourceStats"))
    return state_type, kept


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


def write_result(output_dir, files, state_type, message):
    value = {"state_type": state_type, "message": message}
    if len(json.dumps(value).encode("utf-8")) > MAX_STATE_BYTES:
        fail("the Airbyte state is larger than 900 KiB; Rocky refuses state over 1 MiB")
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "files": files,
        "state": {"schema_version": STATE_SCHEMA_VERSION, "value": value},
    }
    with open(os.path.join(output_dir, RESULT_FILE), "w", encoding="utf-8") as handle:
        json.dump(result, handle)


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
    saved = read_state(request)
    config = build_config(options)

    # The work directory holds the config (it may hold secrets), the catalog
    # and the state. It is removed when the run ends. mkdtemp makes it 0700.
    work_dir = tempfile.mkdtemp(prefix="airbyte-work-", dir=output_dir)
    global CONTAINER_PREFIX
    CONTAINER_PREFIX = "rocky-" + re.sub(r"[^A-Za-z0-9_.-]", "_", os.path.basename(output_dir.rstrip("/")))
    signal.signal(signal.SIGTERM, on_sigterm)
    signal.signal(signal.SIGINT, on_sigterm)
    try:
        write_private(os.path.join(work_dir, "config.json"), config)
        run_spec(options, work_dir)
        streams = run_discover(options, work_dir)
        catalog, mode, namespace = configured_catalog(options, streams)
        write_private(os.path.join(work_dir, "catalog.json"), catalog)

        args = ["read", "--config", MOUNT + "/config.json", "--catalog", MOUNT + "/catalog.json"]
        if saved is not None and mode == "incremental":
            state_type, message = saved
            write_private(os.path.join(work_dir, "state.json"), state_file_value(state_type, message))
            args += ["--state", MOUNT + "/state.json"]
            log("resuming from saved %s state" % state_type)
        elif saved is not None:
            log("full refresh: the saved state is carried but not passed to the source")
        else:
            log("no saved state; first run")

        raw_path = os.path.join(work_dir, "stdout.jsonl")
        code, _ = docker_run(options, work_dir, args, "read", capture_to=raw_path)

        records_path = os.path.join(output_dir, "records.jsonl")
        records = 0
        seen = 0
        last_state = None
        with open(raw_path, encoding="utf-8") as raw, open(records_path, "w", encoding="utf-8") as out:
            for message in messages(raw):
                check_trace_errors(message)
                if message["type"] == "RECORD":
                    record = message["record"]
                    seen += 1
                    # The namespace of a record must match the selected stream's.
                    # A record with no namespace field is accepted.
                    if record.get("stream") == options["stream"] and (
                        namespace is None or record.get("namespace") in (None, namespace)
                    ):
                        out.write(json.dumps(record["data"]) + "\n")
                        records += 1
                elif message["type"] == "STATE":
                    last_state = split_state(message)
        if code != 0:
            fail("`read` exited with code %d" % code)
        if seen > 0 and records == 0:
            fail(
                "the source emitted %d record(s) but none for stream %r (namespace %r); "
                "refusing to move the state past a lost batch" % (seen, options["stream"], namespace)
            )

        if last_state is None:
            # Full refresh sources emit no state. Carry the old state forward.
            last_state = saved if saved is not None else ("STREAM", None)
            log("the source emitted no STATE message; keeping the saved state")
        state_type, state_message = last_state
        if records == 0 and saved is not None and last_state != saved:
            log("warning: the state moved with 0 records; the source advanced its state without records")

        files = []
        if records > 0:
            batch_path = os.path.join(output_dir, BATCH_FILE)
            rows = to_parquet(records_path, batch_path)
            files.append({"path": BATCH_FILE, "bytes": os.path.getsize(batch_path), "rows": rows})
        os.remove(records_path)
        log("read %d record(s) of stream %r; state type %s" % (records, options["stream"], state_type))
        write_result(output_dir, files, state_type, state_message)
    finally:
        remove_containers()
        shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
