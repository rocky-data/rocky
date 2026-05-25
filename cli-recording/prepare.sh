#!/usr/bin/env bash
# prepare.sh <demo> — set up a clean scratch workspace for a vhs tape.
#
# Each tape records inside cli-recording/scratch/<demo>/. This script builds
# that directory from a known-good source (a playground POC, or a fresh
# `rocky playground` scaffold) and strips any state that would make the
# recording non-deterministic (state store, local DuckDB file). The visible
# `rocky` commands stay in the .tape; this is the silent setup.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
POCS="$REPO_ROOT/examples/playground/pocs"

demo="${1:?usage: prepare.sh <demo>}"
scratch="$HERE/scratch/$demo"

rm -rf "$scratch"
mkdir -p "$scratch"

clean_state() {
    find "$1" -name '.rocky-state.redb*' -delete 2>/dev/null || true
    find "$1" -name '*.duckdb' -delete 2>/dev/null || true
    rm -rf "$1/expected" "$1/.rocky" 2>/dev/null || true
}

case "$demo" in
    quickstart)
        # Fresh scaffold, exactly as a new user would get it. The tape itself
        # runs `rocky playground rocky-playground`, so we only need an empty
        # working directory here.
        ;;
    column-lineage)
        cp -r "$POCS/06-developer-experience/01-lineage-column-level/." "$scratch/"
        clean_state "$scratch"
        ;;
    *)
        echo "prepare.sh: unknown demo '$demo'" >&2
        exit 1
        ;;
esac

echo "prepared scratch/$demo"
