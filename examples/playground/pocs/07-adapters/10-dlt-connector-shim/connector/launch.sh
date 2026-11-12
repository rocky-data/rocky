#!/bin/sh
# Starts main.py with the POC-local virtual environment, which holds dlt.
# Rocky runs this with a cleared environment, so the path comes from this file.
here="$(cd "$(dirname "$0")" && pwd)"
exec "$here/../.venv/bin/python" "$here/main.py"
