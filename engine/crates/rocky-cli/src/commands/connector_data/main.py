#!/usr/bin/env python3
"""__NAME__: a Rocky child connector.

`rocky load --connector __NAME__` runs this program. Replace `fetch()` with
code that reads your source. Keep the cursor logic: it makes each run read
only the rows the last run did not.

The state this connector saves (state schema version 1):

    {"cursor": <the largest id loaded so far> | null}
"""

import rocky_connector

# Must equal `state_schema_version` in connector.toml. Raise both when the
# shape of the state changes.
STATE_SCHEMA_VERSION = 1


def fetch(options, cursor):
    """Return the source rows newer than `cursor`, as a list of dicts.

    `options` holds [connector.__NAME__.options] from rocky.toml. Secrets
    are not in it: read them from the environment variables listed under
    `secrets` in rocky.toml.
    """
    # Example source: a fixed list of rows. Replace it.
    count = int(options.get("example_rows", 5))
    rows = [{"id": i, "name": "row %d" % i} for i in range(1, count + 1)]
    return [row for row in rows if cursor is None or row["id"] > cursor]


def main():
    run = rocky_connector.start(STATE_SCHEMA_VERSION)
    cursor = run.state.get("cursor") if run.state else None
    rows = fetch(run.options, cursor)
    if rows:
        # One DuckDB type per column. Keep it in step with the keys fetch()
        # returns: a row with a key not listed here stops the run.
        run.write(rows, columns={"id": "BIGINT", "name": "VARCHAR"})
        cursor = max(row["id"] for row in rows)
    run.finish({"cursor": cursor})


if __name__ == "__main__":
    main()
