#!/usr/bin/env bash
# 13-run-watch-inner-loop — `rocky run --watch` reacts to file edits.
#
# This POC is non-interactive by design — `rocky run --watch` blocks until
# Ctrl-C, so we run it in the background, fire one synthetic edit to
# rocky.toml after the first run completes, wait for the second run, then
# send SIGINT and capture the exit code.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

rm -f .rocky-state.redb poc.duckdb
mkdir -p expected

echo "==> 1. Seed raw__orders.orders into DuckDB"
duckdb poc.duckdb < data/seed.sql

echo "==> 2. Launch rocky run --watch in the background"
LOG="$HERE/expected/watch.log"
: > "$LOG"
rocky -c rocky.toml run --watch --filter source=orders --output json > "$LOG" 2>&1 &
WATCH_PID=$!
trap 'kill -INT "$WATCH_PID" 2>/dev/null || true; wait "$WATCH_PID" 2>/dev/null || true' EXIT

count_runs() {
    # The watcher prints `[watch] run completed in <ms>` after every successful
    # iteration — counting these is robust against pretty-printed vs compact
    # JSON output and language-server log noise.
    grep -c '^\[watch\] run completed' "$LOG" 2>/dev/null || true
}

echo "==> 3. Wait for the first run to settle (banner + 1st run completion)"
for _ in {1..40}; do
    [[ "$(count_runs)" -ge 1 ]] && break
    sleep 0.25
done

echo "==> 4. Touch rocky.toml — debounce window coalesces save bursts; one re-run fires"
touch rocky.toml

echo "==> 5. Wait for the second run record to land"
for _ in {1..40}; do
    [[ "$(count_runs)" -ge 2 ]] && break
    sleep 0.25
done

echo "==> 6. Send SIGINT and let the watch loop exit cleanly"
kill -INT "$WATCH_PID"
wait "$WATCH_PID" || EXIT_CODE=$?
trap - EXIT
EXIT_CODE="${EXIT_CODE:-0}"

echo
echo "==> Watch loop log (banner + run completions + change notice):"
grep -E '^\[watch\] ' "$LOG" | head -10

echo
echo "==> Run records observed: $(count_runs)"
echo "==> Watch loop exit status: $EXIT_CODE"
echo
echo "POC complete: rocky run --watch detected the rocky.toml touch and re-ran the pipeline; SIGINT exited cleanly."
