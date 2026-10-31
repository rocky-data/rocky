#!/usr/bin/env python3
"""Reference child connector: Postgres -> Parquet, cursor on a timestamp.

`rocky load --connector <name>` starts this program once. It reads the
request JSON from stdin, extracts the new rows of one Postgres table into one
Parquet file, and writes `_rocky_result.json` with the new cursor.

Python 3.8 or newer, standard library only. All SQL runs in the standalone
`duckdb` CLI with its `postgres` extension. The connection settings come from
the PG* environment variables (PGHOST, PGPORT, PGUSER, PGPASSWORD,
PGDATABASE), which Rocky hands over as declared secrets. libpq reads them.
They never appear in SQL, in argv, or in the output.

The cursor contract (state schema version 1):

    {"cursor": "<ISO-8601 UTC timestamp, microseconds>" | null}

Each run reads the source maximum M of the cursor column first. It then
extracts the rows with  cursor < value < M  (no lower bound on the first run;
NULL values are skipped). Rows at M wait for the next run, so rows that tie
at M are never split across two runs. The new cursor is the maximum of the
extracted rows. When no row is extracted, the cursor does not change and no
file is written.
"""

import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
PROTOCOL_VERSION = 0
STATE_SCHEMA_VERSION = 1
EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
CURSOR_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
RESULT_FILE = "_rocky_result.json"
BATCH_FILE = "batch-0.parquet"


def log(message):
    # Rocky logs stderr at info level. Print counts and cursors only.
    print("pg-connector: " + message, file=sys.stderr, flush=True)


def fail(message):
    log("error: " + message)
    sys.exit(1)


def ident(name):
    return '"' + name.replace('"', '""') + '"'


def literal(text):
    return "'" + text.replace("'", "''") + "'"


def cursor_to_us(text):
    try:
        moment = datetime.strptime(text, CURSOR_FORMAT).replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        fail("the saved cursor %r is not in the format %s" % (text, CURSOR_FORMAT))
    delta = moment - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1000000 + delta.microseconds


def us_to_cursor(micros):
    return (EPOCH + timedelta(microseconds=micros)).strftime(CURSOR_FORMAT)


def load_source():
    path = os.path.join(HERE, "source.json")
    try:
        with open(path, encoding="utf-8") as handle:
            source = json.load(handle)
    except (OSError, ValueError) as error:
        fail("cannot read %s: %s" % (path, error))
    for key in ("schema", "table", "cursor_column"):
        if not isinstance(source.get(key), str) or not source[key]:
            fail("source.json needs a non-empty string %r" % key)
    ext_dir = source.get("extension_directory") or ".duckdb-extensions"
    # A relative directory is relative to this file, not to the scratch
    # directory, so the extension downloads once and stays out of the landing.
    source["extension_directory"] = os.path.join(HERE, ext_dir)
    return source


def parse_results(stdout):
    """Every JSON array the CLI printed, in order.

    The CLI prints one array per statement that returns rows. It prints
    nothing at all (not `[]`) for a statement with 0 rows.
    """
    decoder = json.JSONDecoder()
    results = []
    index = 0
    while True:
        while index < len(stdout) and stdout[index].isspace():
            index += 1
        if index >= len(stdout):
            return results
        value, index = decoder.raw_decode(stdout, index)
        results.append(value)


def run_duckdb(source, sql):
    preamble = "\n".join(
        [
            "SET TimeZone = 'UTC';",
            "SET extension_directory = %s;" % literal(source["extension_directory"]),
            "INSTALL postgres;",
            "LOAD postgres;",
            # An empty connection string: libpq reads the PG* variables.
            "ATTACH '' AS pg (TYPE postgres, READ_ONLY);",
        ]
    )
    try:
        done = subprocess.run(
            ["duckdb", "-json", "-bail"],
            input=preamble + "\n" + sql + "\n",
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
        fail("duckdb exited with code %d" % done.returncode)
    try:
        return parse_results(done.stdout)
    except ValueError as error:
        fail("cannot parse the duckdb output: %s" % error)


def last_row(results):
    if not results or not results[-1]:
        return {}
    return results[-1][-1]


def read_cursor(request):
    state = request.get("state")
    if state is None:
        return None
    if state.get("schema_version") != STATE_SCHEMA_VERSION:
        fail("unknown state schema version %r" % state.get("schema_version"))
    value = state.get("value") or {}
    cursor = value.get("cursor")
    if cursor is None:
        return None
    return cursor_to_us(cursor)


def write_result(output_dir, files, cursor_us):
    cursor = None if cursor_us is None else us_to_cursor(cursor_us)
    result = {
        "protocol_version": PROTOCOL_VERSION,
        "files": files,
        "state": {"schema_version": STATE_SCHEMA_VERSION, "value": {"cursor": cursor}},
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

    source = load_source()
    lower_us = read_cursor(request)
    table = "pg.%s.%s" % (ident(source["schema"]), ident(source["table"]))
    column = ident(source["cursor_column"])

    # Step 1: one MAX query fixes the upper bound for this run.
    row = last_row(
        run_duckdb(
            source,
            "SELECT CAST(epoch_us(max(%s)) AS VARCHAR) AS max_us FROM %s;" % (column, table),
        )
    )
    max_us = row.get("max_us")
    if max_us is None:
        log("the source table has no cursor values; extracted 0 rows")
        write_result(output_dir, [], lower_us)
        return
    upper_us = int(max_us)
    if lower_us is not None and lower_us >= upper_us:
        log("no row is newer than the cursor; extracted 0 rows")
        write_result(output_dir, [], lower_us)
        return

    # Step 2: extract the rows strictly between the cursor and the maximum.
    # The bounds are integers, so no timestamp text reaches the SQL.
    conditions = ["%s IS NOT NULL" % column, "epoch_us(%s) < %d" % (column, upper_us)]
    if lower_us is not None:
        conditions.append("epoch_us(%s) > %d" % (column, lower_us))
    batch_path = os.path.join(output_dir, BATCH_FILE)
    row = last_row(
        run_duckdb(
            source,
            "COPY (SELECT * FROM %s WHERE %s ORDER BY %s) TO %s (FORMAT parquet);\n"
            "SELECT CAST(count(*) AS VARCHAR) AS n, "
            "CAST(epoch_us(max(%s)) AS VARCHAR) AS max_us FROM read_parquet(%s);"
            % (
                table,
                " AND ".join(conditions),
                column,
                literal(batch_path),
                column,
                literal(batch_path),
            ),
        )
    )
    rows = int(row.get("n") or 0)
    if rows == 0:
        # A result with no files still commits the (unchanged) state.
        if os.path.exists(batch_path):
            os.remove(batch_path)
        log("extracted 0 rows; the cursor does not change")
        write_result(output_dir, [], lower_us)
        return

    new_cursor_us = int(row["max_us"])
    files = [{"path": BATCH_FILE, "bytes": os.path.getsize(batch_path), "rows": rows}]
    log("extracted %d rows; rows at the source maximum wait for the next run" % rows)
    write_result(output_dir, files, new_cursor_us)


if __name__ == "__main__":
    main()
