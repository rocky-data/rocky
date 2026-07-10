#!/usr/bin/env bash
# 11-state-namespacing — give each pipeline / client its own state file so
# concurrent runs don't contend on redb's single writer-per-file lock.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

# Binary resolution: honor $ROCKY_BIN; inside the monorepo prefer the
# freshly-built engine binary (the smoke runner's bare `rocky` on PATH is
# often a stale install that predates --state-namespace); outside a git
# checkout fall back to `rocky` on PATH.
if [ -z "${ROCKY_BIN:-}" ]; then
  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)" && [ -x "$repo_root/engine/target/release/rocky" ]; then
    ROCKY_BIN="$repo_root/engine/target/release/rocky"
  else
    ROCKY_BIN="$(command -v rocky || true)"
  fi
fi
if [ -z "$ROCKY_BIN" ]; then
  echo "error: no rocky binary found. Set ROCKY_BIN, build the engine (cd engine && cargo build --release), or install rocky on PATH." >&2
  exit 1
fi

mkdir -p expected
# Wipe state from previous runs. Namespaced files live under the models-dir
# default `models/.rocky-state/`; the shared default file is `models/.rocky-state.redb`.
rm -rf models/.rocky-state models/.rocky-state.redb models/.rocky-state.redb.lock
rm -rf .rocky poc.duckdb

run() { "$ROCKY_BIN" "$@" -c rocky.toml -o json run --filter source=events; }

echo "=== seed raw source ==="
duckdb poc.duckdb < data/seed.sql

# Two independent namespaces. Each routes to its own state file + its own
# advisory lock, so these two runs share no writer-lock surface — fan out one
# per pipeline or per client without serializing on a single global file.
echo
echo "=== run with --state-namespace foo ==="
run --state-namespace foo > expected/run_foo.json && echo "    ok"
echo "=== run with --state-namespace bar ==="
run --state-namespace bar > expected/run_bar.json && echo "    ok"

# The default invocation (no flag) uses the single shared global state file —
# byte-identical to a project that never sets namespacing.
echo "=== run with no namespace (shared default file) ==="
run > expected/run_default.json && echo "    ok"

echo
echo "=== state files on disk ==="
FOO="models/.rocky-state/foo.redb"
BAR="models/.rocky-state/bar.redb"
DEF="models/.rocky-state.redb"
find models -name '*.redb' | sort | sed 's/^/    /'

# Structural proof: foo and bar each got their own namespaced state file under
# models/.rocky-state/, distinct from each other and from the shared default.
for f in "$FOO" "$BAR" "$DEF"; do
    [ -f "$f" ] || { echo "FAIL: expected state file $f to exist" >&2; exit 1; }
done
# Distinct inodes ⇒ genuinely separate files (separate redb single-writer locks).
inode() { stat -f %i "$1" 2>/dev/null || stat -c %i "$1"; }
[ "$(inode "$FOO")" != "$(inode "$BAR")" ] || { echo "FAIL: foo and bar must be separate files" >&2; exit 1; }

echo
echo "    --state-namespace foo  -> $FOO"
echo "    --state-namespace bar  -> $BAR"
echo "    (no namespace)         -> $DEF"
echo
echo "POC complete: two namespaces ⇒ two independent state files; default ⇒ shared file."
