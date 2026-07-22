#!/usr/bin/env bash
# 24h soak for `rocky serve --scheduler` — the resident reconciler loop.
#
# Promotion gate, not a CI job. Runs a real `serve --scheduler` against a
# freshly scaffolded DuckDB playground on a 1-minute cron for 24 hours, then
# emits a machine-checkable verdict covering:
#
#   * zero missed fires        — every due occurrence produced a run
#   * zero duplicate fires     — exactly one submission per occurrence
#   * flat RSS                 — no memory growth
#   * flat fd count            — the sharp one; a leaked redb handle or lock
#                                sentinel does NOT move RSS
#   * no zombie children       — one subprocess per fire, all reaped
#
# The resident loop is Rocky's first long-lived process, so the fd and zombie
# gates matter more than RSS: ~5760 idle-tick redb open/close cycles at the
# shipped 15s poll, plus ~1440 child runs each of which closes and reopens the
# store around the spawn.
#
# Usage:  bash scripts/soak-scheduler.sh [OUT_DIR]
#         ROCKY_BIN=... DURATION_HOURS=24 SAMPLE_SECONDS=30 PORT=8420
#
# Exit 0 = PASS, 1 = FAIL, 2 = INCOMPLETE/INVALID, 3 = preflight aborted.
#
# NOT wired into CI: 24h exceeds every workflow budget in this repo.

# NOT `set -e`: `rocky doctor` exits nonzero on a Critical check and `curl`
# exits nonzero on a 503, both of which are handled inline.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROCKY_BIN="${ROCKY_BIN:-$REPO/engine/target/release/rocky}"
DURATION_HOURS="${DURATION_HOURS:-24}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-30}"
# Bash arithmetic is integer-only, so DURATION_HOURS must be whole. Set
# DURATION_SECONDS directly to smoke-test the harness over a few minutes.
if [[ -n "${DURATION_SECONDS:-}" ]]; then
  DURATION="$DURATION_SECONDS"
else
  case "$DURATION_HOURS" in
    ''|*[!0-9]*) echo "[soak] FATAL: DURATION_HOURS must be a whole number (got '$DURATION_HOURS'); use DURATION_SECONDS to smoke-test" >&2; exit 3 ;;
  esac
  DURATION=$(( DURATION_HOURS * 3600 ))
fi
PORT="${PORT:-8420}"
POLL_SECONDS="${POLL_SECONDS:-15}"
DRAIN_SECONDS="${DRAIN_SECONDS:-60}"
BASE="http://127.0.0.1:$PORT"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${1:-$REPO/.soak/$STAMP}"

die() { printf '\n[soak] FATAL: %s\n' "$*" >&2; exit 3; }
say() { printf '[soak %s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# ---------------------------------------------------------------------------
# Phase 0 — preconditions. Every one of these has cost a real debugging hour.
# ---------------------------------------------------------------------------
mkdir -p "$OUT" || die "cannot create $OUT"

[[ -x "$ROCKY_BIN" ]] || die "no binary at $ROCKY_BIN — run: cargo build --release -p rocky"

# The PATH rocky is routinely a stale release. `--scheduler` landed in #1199, so
# its absence is a precise, early signal that ROCKY_BIN predates the feature.
"$ROCKY_BIN" serve --help 2>&1 | grep -q -- '--scheduler' \
  || die "$ROCKY_BIN has no 'serve --scheduler' — it predates #1199. Build from this branch."

command -v jq      >/dev/null || die "jq is required"
command -v python3 >/dev/null || die "python3 is required"
command -v lsof    >/dev/null || die "lsof is required"

# An exported serve token would 401 every harvest (health is auth-exempt, so the
# server would look fine while the ledger stayed empty). Neutralise it and say so.
TOKEN_WAS_SET=false
if [[ -n "${ROCKY_SERVE_TOKEN:-}" ]]; then
  TOKEN_WAS_SET=true
  unset ROCKY_SERVE_TOKEN
  say "unset an inherited ROCKY_SERVE_TOKEN (it would 401 every harvest)"
fi

# macOS defaults to 256 open files. A 1-fd-per-tick leak would crash the process
# in ~1h as confusing redb-open errors instead of showing a measurable slope —
# raise the ceiling so the gate observes the leak rather than its side effect.
ulimit -n 10240 2>/dev/null || say "WARNING: could not raise ulimit -n (currently $(ulimit -n))"

if lsof -i ":$PORT" >/dev/null 2>&1; then die "port $PORT is already in use"; fi

# Hold a power assertion for the harness's lifetime. `-w $$` (not `caffeinate
# <cmd>`) so $! below is rocky's pid, which ps/lsof must target.
if command -v caffeinate >/dev/null; then
  caffeinate -dimsu -w $$ &
  say "caffeinate holding a power assertion"
fi

# ---------------------------------------------------------------------------
# Phase 1 — scaffold
# ---------------------------------------------------------------------------
scaffold() {
  local dest="$1"
  rm -rf "$dest"
  "$ROCKY_BIN" playground "$dest" >/dev/null 2>&1 || die "rocky playground failed"
  cat >>"$dest/rocky.toml" <<'TOML'

[pipeline.playground.schedule]
cron = "* * * * *"
timezone = "UTC"
catchup = "latest"

[pipeline.playground.schedule.retry]
max = 0
TOML
}

# `retry.max = 0` is load-bearing, not incidental: with retries off, one
# occurrence yields exactly one submission and one run record, which is what
# makes the duplicate gate a clean equality. It does leave the in-tick retry
# path unexercised by this soak.
#
# `timezone = "UTC"` avoids a DST transition mid-soak confounding occurrence
# arithmetic. `catchup = "latest"` is the default, pinned because the missed-fire
# oracle depends on it.

scaffold "$OUT/project"
say "scaffolded $OUT/project"

# The child `rocky run` inherits serve's cwd and is spawned with NO current_dir
# and WITHOUT --models. The playground adapter path is relative
# (`path = "playground.duckdb"`), so the child resolves BOTH its DuckDB file and
# its default models/ dir against that cwd. Running from the project root is
# required for the children to work at all — not merely tidier for .rocky/traces.
cd "$OUT/project" || die "cannot cd into the project"

# Deliberately OUTSIDE models/, where the implicit resolution would land it.
# `serve` silently ignored `--state-path` before the #1199 review fixes, so a
# state file appearing here is positive proof the binary carries them — a
# sharper freshness check than "does `--scheduler` exist", which an older
# build of the same feature branch also passes.
STATE_PATH="$OUT/soak-state.redb"

start_serve() {
  local stderr_path="$1"
  # `-c` and `--state-path` must precede the `serve` token (neither is a global
  # arg). `--state-path` is a hard override that disables namespacing, so serve,
  # every child, and the API provably converge on one file.
  # `2>>` (O_APPEND): the child's stderr is inherited into this same file, so two
  # producers interleave and appends must be atomic.
  RUST_LOG="${RUST_LOG:-info,salsa=warn,rocky_cli::commands::scheduler=debug}" \
  "$ROCKY_BIN" -c "$OUT/project/rocky.toml" --state-path "$STATE_PATH" --output json \
    serve --models models --port "$PORT" --scheduler \
    --poll-interval-seconds "$POLL_SECONDS" --drain-timeout-seconds "$DRAIN_SECONDS" \
    >>"$OUT/serve.stdout" 2>>"$stderr_path" &
  echo $!
}

wait_for_health() {
  local deadline=$(( $(date +%s) + 60 ))
  while (( $(date +%s) < deadline )); do
    if [[ "$(curl -sS -o /dev/null -w '%{http_code}' --max-time 5 "$BASE/api/v1/health" 2>/dev/null)" == "200" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# ---------------------------------------------------------------------------
# Phase 2 — preflight (~3 min). Proves the evidence chain end to end BEFORE the
# 24h clock starts. Without this the gate can "pass" on an empty ledger, which
# is exactly what a misplaced config produces: serve treats a missing or invalid
# config as an idle loop rather than an error.
# ---------------------------------------------------------------------------
PRE_OK=true
pre_assert() { # name, ok(0/1), detail
  local ok=$2
  printf '{"assertion":"%s","pass":%s,"detail":%s}\n' \
    "$1" "$([[ $ok -eq 0 ]] && echo true || echo false)" "$(jq -Rn --arg d "$3" '$d')" \
    >>"$OUT/preflight.json"
  if [[ $ok -eq 0 ]]; then say "  preflight OK   — $1"; else say "  preflight FAIL — $1: $3"; PRE_OK=false; fi
}

say "preflight: starting (~3 min)"
: >"$OUT/preflight.json"

out=$("$ROCKY_BIN" -c "$OUT/project/rocky.toml" --state-path "$STATE_PATH" --output json tick --dry-run 2>&1)
echo "$out" | jq -e '.evaluated[] | select(.pipeline=="playground") | .sources[] | select(.source=="cron")' >/dev/null 2>&1
pre_assert "schedule_block_attaches" $? "tick --dry-run must see a cron source for playground"

PRE_PID=$(start_serve "$OUT/preflight.stderr.jsonl")
wait_for_health
pre_assert "server_binds_and_answers_health" $? "no 200 from /api/v1/health within 60s"

# A cron anchors on first sight and fires on the NEXT boundary, so allow >2 min.
say "preflight: waiting up to 150s for the first scheduled fire"
found=1; deadline=$(( $(date +%s) + 150 ))
while (( $(date +%s) < deadline )); do
  body=$(curl -sS --max-time 10 "$BASE/api/v1/runs" 2>/dev/null)
  if echo "$body" | jq -e '.runs[] | select(.trigger=="Schedule") | select(.submission_id != null)' >/dev/null 2>&1; then
    found=0; break
  fi
  sleep 5
done
pre_assert "scheduled_fire_reaches_run_history" $found \
  "no run with trigger=Schedule and a submission_id within 150s — check the config path and ROCKY_SERVE_TOKEN"

# The missed-fire gate buckets by floor-to-minute of started_at. Prove the
# assumption holds before relying on it for 24h.
if [[ $found -eq 0 ]]; then
  skew=$(echo "$body" | jq -r '[.runs[] | select(.trigger=="Schedule") | .started_at][0]' \
    | python3 -c 'import sys,datetime;t=datetime.datetime.fromisoformat(sys.stdin.read().strip().replace("Z","+00:00"));print(int(t.second+t.microsecond/1e6))')
  if [[ -n "$skew" && "$skew" -le 30 ]]; then ok=0; else ok=1; fi
  pre_assert "fire_buckets_to_its_minute" "$ok" "started_at is ${skew}s past the minute; >30s breaks minute bucketing"
fi

# The explicit path is outside models/, so its existence proves serve honoured
# --state-path rather than silently re-resolving. A binary predating the #1199
# review fixes writes models/.rocky-state.redb instead and fails here.
if [[ -f "$STATE_PATH" ]]; then ok=0; else ok=1; fi
pre_assert "serve_honours_explicit_state_path" "$ok" \
  "no state file at $STATE_PATH — this binary predates the serve --state-path fix; rebuild from this branch"

# Wait for ticks rather than sampling immediately: a cron can fire within one
# poll of startup, so asserting right after the fire would race the tick count.
tick_deadline=$(( $(date +%s) + 4 * POLL_SECONDS + 10 ))
ticks=0
while (( $(date +%s) < tick_deadline )); do
  ticks=$(grep -c 'scheduler: tick complete' "$OUT/preflight.stderr.jsonl" 2>/dev/null || echo 0)
  [[ "$ticks" -ge 3 ]] && break
  sleep 2
done
if [[ "$ticks" -ge 3 ]]; then ok=0; else ok=1; fi
pre_assert "tick_lines_are_parseable" "$ok" "only $ticks 'scheduler: tick complete' lines in stderr"

# G6 reads `.fields.executed` off those lines, so prove that field is actually
# there — a line count alone would not catch a renamed or restructured field.
grep 'scheduler: tick complete' "$OUT/preflight.stderr.jsonl" 2>/dev/null | tail -1 \
  | jq -e 'select(.fields.executed != null) | .fields.executed' >/dev/null 2>&1
pre_assert "tick_line_carries_executed_field" $? \
  "the tick line has no .fields.executed — the log-corroboration gate would read nothing"

"$ROCKY_BIN" -c "$OUT/project/rocky.toml" --state-path "$STATE_PATH" --output json \
  doctor --check scheduler 2>/dev/null | jq -e '.checks[] | select(.name=="scheduler")' >/dev/null 2>&1
pre_assert "doctor_exposes_the_scheduler_check" $? "doctor --check scheduler emitted no 'scheduler' check"

kill -TERM "$PRE_PID" 2>/dev/null
wait "$PRE_PID" 2>/dev/null
say "preflight: complete"

if [[ "$PRE_OK" != true ]]; then
  die "preflight failed — see $OUT/preflight.json. Aborting BEFORE the 24h clock."
fi

# Re-scaffold so the preflight's own fires never pollute the soak ledger.
# The state file lives OUTSIDE the project (see STATE_PATH above), so
# `scaffold`'s `rm -rf` does not reach it — clear it explicitly. Without this
# the preflight's runs are harvested as measured fires, and the re-scaffold gap
# between them and the first real fire reads as a missed occurrence.
cd "$REPO" || die "cd"
scaffold "$OUT/project"
rm -f "$STATE_PATH" "$STATE_PATH".lock "$STATE_PATH"-lock
cd "$OUT/project" || die "cd"
say "re-scaffolded clean for the measured run (state reset)"

# ---------------------------------------------------------------------------
# Phase 3 — launch
# ---------------------------------------------------------------------------
T0=$(date +%s)
END=$(( T0 + DURATION ))
SERVE_PID=$(start_serve "$OUT/serve.stderr.jsonl")
if ! wait_for_health; then
  kill -TERM "$SERVE_PID" 2>/dev/null
  die "server did not answer health within 60s"
fi
say "serve --scheduler running (pid $SERVE_PID), until $(date -u -r "$END" +%H:%M:%SZ 2>/dev/null || echo "+${DURATION}s")"

jq -n \
  --arg bin "$ROCKY_BIN" \
  --arg version "$("$ROCKY_BIN" --version 2>/dev/null | head -1)" \
  --arg sha "$(git -C "$REPO" rev-parse HEAD 2>/dev/null)" \
  --arg branch "$(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null)" \
  --arg state_path "$STATE_PATH" \
  --argjson t0 "$T0" --argjson end "$END" --argjson pid "$SERVE_PID" \
  --argjson sample "$SAMPLE_SECONDS" --argjson poll "$POLL_SECONDS" \
  --argjson drain "$DRAIN_SECONDS" --argjson port "$PORT" \
  --argjson token_was_set "$TOKEN_WAS_SET" \
  --argjson ulimit "$(ulimit -n)" \
  '{binary:$bin, version:$version, git_sha:$sha, branch:$branch, state_path:$state_path,
    t0:$t0, planned_end:$end, pid:$pid, sample_interval:$sample, poll_interval:$poll,
    drain_timeout:$drain, port:$port, cron:"* * * * *", timezone:"UTC", catchup:"latest",
    retry_max:0, serve_token_was_unset:$token_was_set, ulimit_n:$ulimit}' \
  >"$OUT/meta.json"

# ---------------------------------------------------------------------------
# Phase 4 — unified sample + harvest loop.
# Order matters: cheap and time-critical first, slow lsof last so it can never
# delay the harvest. Absolute scheduling so per-iteration cost never drifts the
# cadence.
# ---------------------------------------------------------------------------
harvest() { # -> appends projected Schedule runs, tagged with harvest time
  local t=$1 body code
  body=$(curl -sS --max-time 20 -w '\n%{http_code}' "$BASE/api/v1/runs" 2>/dev/null)
  code="${body##*$'\n'}"
  if [[ "$code" != "200" ]]; then
    # A 503 is the documented-retryable engine_busy; try once more.
    sleep 2
    body=$(curl -sS --max-time 20 -w '\n%{http_code}' "$BASE/api/v1/runs" 2>/dev/null)
    code="${body##*$'\n'}"
  fi
  if [[ "$code" == "200" ]]; then
    printf '%s' "${body%$'\n'*}" | jq -c --argjson h "$t" '.runs[]
      | select(.trigger=="Schedule")
      | {h:$h, run_id, started_at, status, pipeline, submission_id, duration_ms}' \
      >>"$OUT/harvest.jsonl" 2>/dev/null
    return 0
  fi
  printf '{"t":%s,"code":"%s"}\n' "$t" "$code" >>"$OUT/harvest_errors.jsonl"
  return 1
}

say "sampling every ${SAMPLE_SECONDS}s"
n=0
while (( $(date +%s) < END )); do
  T=$(date +%s)

  if ! kill -0 "$SERVE_PID" 2>/dev/null; then
    printf '{"t":%s,"alive":0}\n' "$T" >>"$OUT/samples.jsonl"
    say "PROCESS DIED at $(date -u +%H:%M:%SZ)"
    break
  fi

  HCODE=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "$BASE/api/v1/health" 2>/dev/null)
  harvest "$T" && HARV=ok || HARV=err

  read -r RSS VSZ STAT < <(ps -o rss=,vsz=,stat= -p "$SERVE_PID" 2>/dev/null)
  read -r CHILD ZOMB < <(ps -A -o pid=,ppid=,stat= 2>/dev/null |
    awk -v p="$SERVE_PID" '$2==p{c++; if($3 ~ /^Z/) z++} END{print c+0, z+0}')

  # Classified so a leak is attributed to the redb handle / lock sentinel /
  # trace writer rather than being an anonymous count.
  read -r FTOT FREDB FLOCK FTRC FTCP FOTH < <(lsof -p "$SERVE_PID" -n -P 2>/dev/null | awk '
    NR>1 && $4 ~ /^[0-9]+/ { tot++
      if      ($0 ~ /\.redb\.lock/)    lock++
      else if ($0 ~ /\.redb/)          redb++
      else if ($0 ~ /\.rocky\/traces/) tr++
      else if ($5=="IPv4" || $5=="IPv6") tcp++
      else oth++ }
    END{printf "%d %d %d %d %d %d\n", tot+0, redb+0, lock+0, tr+0, tcp+0, oth+0}')

  printf '{"t":%s,"alive":1,"health":"%s","harvest":"%s","rss_kb":%s,"vsz_kb":%s,"stat":"%s","children":%s,"zombies":%s,"fd_total":%s,"fd_redb":%s,"fd_lock":%s,"fd_traces":%s,"fd_tcp":%s,"fd_other":%s}\n' \
    "$T" "${HCODE:-000}" "$HARV" "${RSS:-null}" "${VSZ:-null}" "${STAT:-?}" \
    "${CHILD:-null}" "${ZOMB:-null}" "${FTOT:-null}" "${FREDB:-null}" \
    "${FLOCK:-null}" "${FTRC:-null}" "${FTCP:-null}" "${FOTH:-null}" \
    >>"$OUT/samples.jsonl"

  n=$(( n + 1 ))
  NEXT=$(( T0 + n * SAMPLE_SECONDS ))
  NOW=$(date +%s)
  (( NEXT > NOW )) && sleep $(( NEXT - NOW ))
done

# ---------------------------------------------------------------------------
# Phase 5 — final harvest, shutdown, verdict
# ---------------------------------------------------------------------------
# The loop's last harvest can be up to SAMPLE_SECONDS stale; harvest again
# BEFORE the SIGTERM so a trailing fire is not lost to the ledger.
ALIVE_AT_END=false
if kill -0 "$SERVE_PID" 2>/dev/null; then
  ALIVE_AT_END=true
  harvest "$(date +%s)"
  say "draining (SIGTERM)"
  kill -TERM "$SERVE_PID" 2>/dev/null
fi

deadline=$(( $(date +%s) + DRAIN_SECONDS + 30 ))
while kill -0 "$SERVE_PID" 2>/dev/null && (( $(date +%s) < deadline )); do sleep 1; done
wait "$SERVE_PID" 2>/dev/null; EXIT_CODE=$?

printf '{"exit":%s,"t":%s,"expected":%s}\n' \
  "$EXIT_CODE" "$(date +%s)" "$ALIVE_AT_END" >"$OUT/serve.exit"

# DONE is written ONLY on the planned path — its absence is how the verdict
# tells "24h clean" from "died at hour 12".
if [[ "$ALIVE_AT_END" == true ]]; then
  printf '{"end":%s}\n' "$(date +%s)" >"$OUT/DONE"
fi

say "computing verdict"
python3 "$REPO/scripts/soak_verdict.py" "$OUT"
exit $?
