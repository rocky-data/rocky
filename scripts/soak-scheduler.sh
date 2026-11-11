#!/usr/bin/env bash
# 24h soak for `rocky serve --scheduler` — the resident reconciler loop.
#
# Promotion gate, not a CI job. Runs a real `serve --scheduler` against a
# freshly scaffolded DuckDB playground on a 1-minute cron for 24 hours, then
# emits a machine-checkable verdict covering:
#
#   * zero missed fires        — every due occurrence produced a run
#   * zero duplicate fires     — exactly one submission per occurrence
#   * bounded RSS              — NOT "no memory growth". The claim is narrower:
#                                the final quarter's least-squares slope is
#                                under 200 KB/h, and the RSS series actually
#                                covered that quarter. A bounded structure fills
#                                once and then holds, so growth still present at
#                                hour 18-24 is a leak however small the absolute
#                                delta is. Growth slower than the threshold, and
#                                growth that starts late enough to be diluted by
#                                the rest of the quarter, fall outside this
#                                gate's sensitivity rather than being ruled out
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
#         HARVEST_MODE=end  (unpolled arm: no /api/v1/runs read while sampling)
#         bash scripts/soak-scheduler.sh --self-check [OUT_DIR]
#             a 20-minute run (SELF_CHECK_MINUTES overrides) that proves the
#             harness end to end. Too short for the memory gates, so a PASS
#             says the harness works, never that memory is flat.
#         bash scripts/soak-scheduler.sh --finish OUT_DIR
#             finish a run whose harness was killed before its end step: read
#             the ledger back from the state file, write the sentinels, and
#             compute the verdict. serve must no longer be running.
#
# Exit 0 = PASS, 1 = FAIL, 2 = INCOMPLETE/INVALID, 3 = preflight aborted.
#
# STOPS. SIGTERM, SIGINT or SIGHUP end the sampling loop and run the same end
# step a planned finish runs: a final harvest, a drain of serve, the end-mode
# ledger read, the sentinels and the verdict. The DONE sentinel is written in
# both cases, atomically, and says which it was (`"complete": false` and the
# signal on a stop), so the verdict reports INCOMPLETE with the hours observed
# and the gates computed over them. The end step needs time: give a container
# `--stop-timeout 900` (Docker's default is 10 s, after which it sends
# SIGKILL). `scripts/soak-docker.sh` does. If SIGKILL lands anyway, run
# `--finish OUT_DIR`.
#
# SLEEP. Every sample records the wall clock and, on Linux, CLOCK_BOOTTIME
# (`/proc/uptime`). A sampling gap past 3x the interval, or a wall-clock step
# the boot clock did not take, is written to gaps.jsonl as it happens, and the
# verdict marks the run INVALID with each gap's size. Inside a container the
# harness cannot hold the HOST awake: `scripts/soak-docker.sh` runs
# `caffeinate` on a macOS host for that.
#
# NOT a 24h CI job: 24h exceeds every workflow budget in this repo. The
# self-check runs in CI (.github/workflows/soak-selfcheck.yml).

# NOT `set -e`: `rocky doctor` exits nonzero on a Critical check and `curl`
# exits nonzero on a 503, both of which are handled inline.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ROCKY_BIN="${ROCKY_BIN:-$REPO/engine/target/release/rocky}"

MODE=soak
case "${1:-}" in
  --self-check)
    MODE=self-check
    shift
    DURATION_SECONDS=$(( ${SELF_CHECK_MINUTES:-20} * 60 ))
    ;;
  --finish)
    MODE=finish
    shift
    ;;
  -*)
    echo "[soak] FATAL: unknown option '$1' (expected --self-check or --finish)" >&2
    exit 3
    ;;
esac
DURATION_HOURS="${DURATION_HOURS:-24}"
SAMPLE_SECONDS="${SAMPLE_SECONDS:-30}"
# Harvest far less often than we sample, because harvesting is NOT free and the
# harness was measuring itself (#1256).
#
# `GET /api/v1/runs` -> `StateStore::list_runs` deserialises the WHOLE
# RUN_HISTORY table and only then truncates to 50 (#1304), so its working set
# scales with total history: ~8.59 KB per row, measured. Two controlled
# experiments established that at a FIXED history size RSS steps up once and
# then holds — an allocator high-water mark, not a leak — but this soak adds a
# run per minute, so every 30s harvest scanned a slightly larger table and
# pushed that high-water mark up monotonically. That produced a linear
# ~480-500 KB/h RSS climb which was twice mistaken for a product leak.
# It accounted for 92-96% of the growth in both 24h runs.
#
# HOW LOW CAN THE CADENCE GO? It is bounded by the 50-row response cap, not by
# taste: a harvest sees only the newest 50 runs, so at one run per minute we
# must poll at least every 50 minutes or runs vanish between harvests. 600s
# gives a 5x margin at that rate. RAISE THIS ONLY WITH THE CAP IN MIND — and if
# the fixture's run cadence ever goes above one per minute, recompute it
# (safe_interval < 50 / runs_per_minute, in minutes).
#
# G6's log-corroboration still cross-checks harvested runs against the tick
# log, so a cadence mistake surfaces as a verdict failure rather than silently.
HARVEST_SECONDS="${HARVEST_SECONDS:-600}"
# HARVEST_MODE=poll (default) harvests `/api/v1/runs` every HARVEST_SECONDS, as
# above. HARVEST_MODE=end is the "unpolled" arm of the #1416 discriminating
# experiment: ZERO `/runs` requests while the clock runs, so the `/health` poll
# is the only observer. A lower cadence cannot stand in for it, because the RSS
# high-water mark is set by the LARGEST scan and the table grows every minute.
# The ledger is then read ONCE, after serve has exited (see harvest_from_state).
HARVEST_MODE="${HARVEST_MODE:-poll}"
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

case "$HARVEST_MODE" in
  poll|end) ;;
  *) die "HARVEST_MODE must be 'poll' or 'end' (got '$HARVEST_MODE')" ;;
esac

# Write a file so a reader never sees half of it: a stop can land mid-write,
# and a torn DONE would read as no DONE at all.
write_atomic() { # PATH CONTENT
  printf '%s\n' "$2" >"$1.tmp" && sync "$1.tmp" 2>/dev/null; mv -f "$1.tmp" "$1"
}

# Is this process running under a CPU translator (Rosetta, qemu-user)? The
# kernel's /proc/cpuinfo describes the real CPU while `uname -m` reports the
# emulated one, so a mismatch gives it away. A translator adds its own memory
# to the RSS this soak measures, so the verdict refuses to judge such a run.
# SOAK_EMULATED=1 (set by soak-docker.sh for a non-native platform) also counts.
detect_emulation() {
  local machine
  machine=$(uname -m)
  [[ "${SOAK_EMULATED:-}" == 1 ]] && { echo true; return; }
  if [[ -r /proc/cpuinfo ]]; then
    case "$machine" in
      x86_64|i?86) grep -q '^CPU implementer' /proc/cpuinfo && { echo true; return; } ;;
      aarch64|arm64) grep -q '^vendor_id' /proc/cpuinfo && { echo true; return; } ;;
    esac
  fi
  echo false
}

# CLOCK_BOOTTIME in whole seconds, or empty where /proc/uptime does not exist
# (macOS). It keeps counting through a suspend, while a paused VM's boot clock
# stops and its wall clock jumps when it resyncs, so the pair tells the two
# apart (see the gap check in the sampling loop).
boot_seconds() {
  local up _rest
  [[ -r /proc/uptime ]] || return 0
  read -r up _rest </proc/uptime && printf '%s' "${up%.*}"
}

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

# ---------------------------------------------------------------------------
# Shared functions: the ledger reads, used by the soak and by --finish.
# ---------------------------------------------------------------------------
# Run ids the children logged on their `run` span, one per line. `-R` +
# `fromjson?` skips a torn or plain-text line instead of stopping there: parent
# and child output interleave in one file (see soak_verdict.py `read_jsonl`), and
# plain `jq` drops every line after the first one it cannot parse. The `run` span
# may be the event's own span or an outer one in `spans`.
logged_run_ids() { # FILE -> sorted unique run ids
  jq -rR 'fromjson? | (.span, .spans[]?) | select(type=="object" and .name=="run") | .run_id // empty' \
    "$1" 2>/dev/null | grep -E '^run-[0-9-]+$' | sort -u
}

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

# HARVEST_MODE=end only; runs AFTER serve has exited, so nothing here can touch
# serve's RSS. No CLI lists the whole run history (`rocky history` and
# `GET /runs` both stop at the newest 50), so the ids come from the child runs'
# own log spans and each row is then read back from the state file with
# `history --run`. The newest-50 window is added too, so a recent row the log
# never mentioned still lands in the ledger. A logged id the state file cannot
# produce is counted in harvest_errors.jsonl and surfaces in G6 as an
# execution with no durable record.
harvest_from_state() { # -> same harvest.jsonl shape as harvest()
  local t=$1 id raw="$OUT/harvest.raw" ids
  : >"$raw"
  ids=$(logged_run_ids "$OUT/serve.stderr.jsonl")
  "$ROCKY_BIN" -c "$OUT/project/rocky.toml" --state-path "$STATE_PATH" --output json history \
    >>"$raw" 2>/dev/null
  for id in $ids; do
    "$ROCKY_BIN" -c "$OUT/project/rocky.toml" --state-path "$STATE_PATH" --output json \
      history --run "$id" >>"$raw" 2>/dev/null \
      || printf '{"t":%s,"run_id":"%s","code":"history_run_failed"}\n' "$t" "$id" \
        >>"$OUT/harvest_errors.jsonl"
  done
  jq -c --argjson h "$t" '.runs[]
    | select(.trigger=="Schedule")
    | {h:$h, run_id, started_at, status, pipeline, submission_id, duration_ms}' \
    "$raw" >>"$OUT/harvest.jsonl"
  say "read the run rows back from the state file ($(wc -w <<<"$ids") ids from the log)"
}

# The sentinels the verdict reads. DONE is written on EVERY ending the
# harness lives to see, planned or not; `complete` is what tells "24h clean"
# from "stopped at hour 21". Its absence now means the harness itself was
# killed (SIGKILL, power loss) before its end step, which `--finish` repairs.
write_done() { # END_EPOCH COMPLETE(true|false) REASON
  local planned_end
  planned_end=$(jq -r '.planned_end // 0' "$OUT/meta.json" 2>/dev/null || echo 0)
  write_atomic "$OUT/DONE" "$(jq -cn --argjson end "$1" --argjson complete "$2" \
    --arg reason "$3" --argjson planned_end "${planned_end:-0}" \
    '{end:$end, complete:$complete, reason:$reason, planned_end:$planned_end}')"
}

if [[ "$MODE" == finish ]]; then
  [[ -n "${1:-}" ]] || die "--finish needs the run directory"
  [[ -s "$OUT/meta.json" ]] || die "no meta.json in $OUT — not a soak run directory"
  STATE_PATH="$OUT/soak-state.redb"
  [[ -f "$STATE_PATH" ]] || die "no state file at $STATE_PATH"
  if [[ -s "$OUT/DONE" ]]; then
    say "DONE already present; recomputing the verdict only"
  else
    # The state file must be quiet. serve holds the redb lock while it runs,
    # so a live serve would make every read below fail or race.
    pid=$(jq -r '.pid // empty' "$OUT/meta.json")
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null \
      && ps -o args= -p "$pid" 2>/dev/null | grep -q -- 'serve'; then
      die "serve (pid $pid) is still running; stop it first"
    fi
    # Both modes read the state file: in poll mode it recovers the fires the
    # last /runs poll never saw. harvest.jsonl is deduplicated by run id.
    harvest_from_state "$(date +%s)"
    last_t=$(tail -n 1 "$OUT/samples.jsonl" 2>/dev/null | jq -r '.t // empty' 2>/dev/null)
    [[ -s "$OUT/serve.exit" ]] || write_atomic "$OUT/serve.exit" \
      "$(jq -cn --argjson t "${last_t:-$(date +%s)}" '{exit:null, t:$t, expected:false}')"
    write_done "${last_t:-$(date +%s)}" false "finished_offline"
    say "wrote DONE (complete=false, finished offline)"
  fi
  python3 "$REPO/scripts/soak_verdict.py" "$OUT"
  exit $?
fi
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
# A stop during preflight has nothing to measure yet: stop serve, then abort.
trap 'kill -TERM "$PRE_PID" 2>/dev/null; wait "$PRE_PID" 2>/dev/null; die "stopped by a signal during preflight"' TERM INT HUP
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


# The end-mode ledger is rebuilt from the run ids the children log, so prove the
# log carries them BEFORE the clock starts, with the same extraction the ledger
# uses. `poll` mode never reads them.
if [[ "$HARVEST_MODE" == end ]]; then
  if [[ -n "$(logged_run_ids "$OUT/preflight.stderr.jsonl")" ]]; then ok=0; else ok=1; fi
  pre_assert "child_log_carries_run_id" "$ok" \
    "no run_id span in serve stderr — HARVEST_MODE=end could not find the runs to read back"
fi

kill -TERM "$PRE_PID" 2>/dev/null
wait "$PRE_PID" 2>/dev/null
trap - TERM INT HUP
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
  --argjson t0 "$T0" --argjson end_ts "$END" --argjson pid "$SERVE_PID" \
  --argjson sample "$SAMPLE_SECONDS" --argjson poll "$POLL_SECONDS" \
  --argjson harvest "$([[ "$HARVEST_MODE" == end ]] && echo 0 || echo "$HARVEST_SECONDS")" \
  --arg harvest_mode "$HARVEST_MODE" \
  --argjson drain "$DRAIN_SECONDS" --argjson port "$PORT" \
  --argjson token_was_set "$TOKEN_WAS_SET" \
  --argjson ulimit "$(ulimit -n)" \
  --arg machine "$(uname -m)" --argjson emulated "$(detect_emulation)" \
  '{binary:$bin, version:$version, git_sha:$sha, branch:$branch, state_path:$state_path,
    t0:$t0, planned_end:$end_ts, pid:$pid, sample_interval:$sample, harvest_interval:$harvest, harvest_mode:$harvest_mode,
    poll_interval:$poll,
    drain_timeout:$drain, port:$port, cron:"* * * * *", timezone:"UTC", catchup:"latest",
    retry_max:0, serve_token_was_unset:$token_was_set, ulimit_n:$ulimit,
    machine:$machine, emulated:$emulated}' \
  >"$OUT/meta.json" || die "meta.json write failed (jq)"
# The verdict hard-depends on meta.json; an empty file must abort the soak
# NOW, not surface as INVALID after 24 hours.
[[ -s "$OUT/meta.json" ]] || die "meta.json is empty"

# ---------------------------------------------------------------------------
# Phase 4 — unified sample + harvest loop.
# Order matters: cheap and time-critical first, slow lsof last so it can never
# delay the harvest. Absolute scheduling so per-iteration cost never drifts the
# cadence.
# ---------------------------------------------------------------------------


# A stop only sets a flag; the loop sees it within one interval and leaves
# through the same end step as a planned finish. Bash runs a trap between
# commands, so the sleep below runs in the background and `wait` (which a
# signal interrupts) waits for it; a foreground `sleep 30` would hold the
# trap for up to 30 s.
STOP_SIGNAL=""
trap 'STOP_SIGNAL=TERM' TERM
trap 'STOP_SIGNAL=INT' INT
trap 'STOP_SIGNAL=HUP' HUP

say "sampling every ${SAMPLE_SECONDS}s, harvesting: $([[ "$HARVEST_MODE" == end ]] && echo "never (end-of-run read)" || echo "every ${HARVEST_SECONDS}s")"
: >"$OUT/gaps.jsonl"
n=0
LAST_HARVEST=0
PREV_T=""
PREV_UP=""
# A gap is a sampling spacing past 3x the interval: the verdict's own G0
# threshold. A clock step is the wall clock moving 10 s more (or less) than
# the boot clock across one spacing; a 30 s spacing never drifts that far.
GAP_LIMIT=$(( 3 * SAMPLE_SECONDS ))
while [[ -z "$STOP_SIGNAL" ]] && (( $(date +%s) < END )); do
  T=$(date +%s)
  UP=$(boot_seconds)

  # The sleep detector. It writes as it goes, so a run that dies after a sleep
  # still carries the evidence, and a reader can see a sleep mid-run.
  if [[ -n "$PREV_T" ]]; then
    wall_gap=$(( T - PREV_T ))
    boot_gap=""
    [[ -n "$UP" && -n "$PREV_UP" ]] && boot_gap=$(( UP - PREV_UP ))
    kind=""
    if (( wall_gap > GAP_LIMIT )); then
      kind=sampling_gap
    elif [[ -n "$boot_gap" ]] && (( wall_gap - boot_gap > 10 || boot_gap - wall_gap > 10 )); then
      kind=clock_step
    fi
    if [[ -n "$kind" ]]; then
      printf '{"kind":"%s","from":%s,"to":%s,"wall_gap_s":%s,"boot_gap_s":%s}\n' \
        "$kind" "$PREV_T" "$T" "$wall_gap" "${boot_gap:-null}" >>"$OUT/gaps.jsonl"
      say "WARNING: $kind of ${wall_gap}s wall / ${boot_gap:-?}s boot clock — host sleep or a paused VM; this run will be INVALID"
    fi
  fi
  PREV_T=$T
  PREV_UP=$UP

  if ! kill -0 "$SERVE_PID" 2>/dev/null; then
    printf '{"t":%s,"alive":0}\n' "$T" >>"$OUT/samples.jsonl"
    say "PROCESS DIED at $(date -u +%H:%M:%SZ)"
    break
  fi

  HCODE=$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 "$BASE/api/v1/health" 2>/dev/null)
  # Sample every iteration; harvest on its own slower cadence (see
  # HARVEST_SECONDS). `skip` is recorded distinctly from `ok`/`err` so a reader
  # can tell "not harvested this iteration" from "harvest failed".
  if [[ "$HARVEST_MODE" == poll ]] && (( T - LAST_HARVEST >= HARVEST_SECONDS )); then
    harvest "$T" && HARV=ok || HARV=err
    LAST_HARVEST="$T"
  else
    HARV=skip
  fi

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

  printf '{"t":%s,"up":%s,"alive":1,"health":"%s","harvest":"%s","rss_kb":%s,"vsz_kb":%s,"stat":"%s","children":%s,"zombies":%s,"fd_total":%s,"fd_redb":%s,"fd_lock":%s,"fd_traces":%s,"fd_tcp":%s,"fd_other":%s}\n' \
    "$T" "${UP:-null}" "${HCODE:-000}" "$HARV" "${RSS:-null}" "${VSZ:-null}" "${STAT:-?}" \
    "${CHILD:-null}" "${ZOMB:-null}" "${FTOT:-null}" "${FREDB:-null}" \
    "${FLOCK:-null}" "${FTRC:-null}" "${FTCP:-null}" "${FOTH:-null}" \
    >>"$OUT/samples.jsonl"

  # Absolute scheduling. After a gap, jump to the next slot instead of firing
  # every missed slot back to back: a burst of 1 s spacings is noise in the
  # series, and the run is already INVALID for the gap.
  NOW=$(date +%s)
  n=$(( (NOW - T0) / SAMPLE_SECONDS + 1 ))
  NEXT=$(( T0 + n * SAMPLE_SECONDS ))
  if (( NEXT > NOW )) && [[ -z "$STOP_SIGNAL" ]]; then
    sleep $(( NEXT - NOW )) &
    SLEEP_PID=$!
    wait "$SLEEP_PID" 2>/dev/null
    # A signal interrupts `wait`; do not leave the sleep behind.
    [[ -n "$STOP_SIGNAL" ]] && kill "$SLEEP_PID" 2>/dev/null
  fi
done

# ---------------------------------------------------------------------------
# Phase 5 — the end step: final harvest, shutdown, sentinels, verdict. A
# planned finish and a stop take this same path.
# ---------------------------------------------------------------------------
# A second signal must not cut the end step short; the container runtime's own
# SIGKILL at the end of its grace is the hard limit (see the header).
trap '' TERM INT HUP
STOP_AT=$(date +%s)
if [[ -n "$STOP_SIGNAL" ]]; then
  say "stopped by SIG$STOP_SIGNAL $(( (STOP_AT - T0) / 60 )) min into a $(( DURATION / 60 )) min run; running the end step"
  # Say so on disk before anything slow, so even a SIGKILL during the drain
  # leaves the cause behind.
  write_atomic "$OUT/STOPPED" "$(jq -cn --argjson t "$STOP_AT" --arg sig "SIG$STOP_SIGNAL" '{t:$t, signal:$sig}')"
fi

# The loop's last harvest can be up to SAMPLE_SECONDS stale; harvest again
# BEFORE the SIGTERM so a trailing fire is not lost to the ledger.
ALIVE_AT_END=false
if kill -0 "$SERVE_PID" 2>/dev/null; then
  ALIVE_AT_END=true
  [[ "$HARVEST_MODE" == poll ]] && harvest "$(date +%s)"
  say "draining (SIGTERM)"
  kill -TERM "$SERVE_PID" 2>/dev/null
fi

deadline=$(( $(date +%s) + DRAIN_SECONDS + 30 ))
while kill -0 "$SERVE_PID" 2>/dev/null && (( $(date +%s) < deadline )); do sleep 1; done
wait "$SERVE_PID" 2>/dev/null; EXIT_CODE=$?

# The unpolled arm's only ledger read: serve is gone, the state file is quiet.
if [[ "$HARVEST_MODE" == end ]]; then
  harvest_from_state "$(date +%s)"
fi

printf '{"exit":%s,"t":%s,"expected":%s}\n' \
  "$EXIT_CODE" "$(date +%s)" "$ALIVE_AT_END" >"$OUT/serve.exit"

# DONE is complete ONLY on the planned path, with serve alive to the end.
if [[ -n "$STOP_SIGNAL" ]]; then
  write_done "$STOP_AT" false "signal:SIG$STOP_SIGNAL"
elif [[ "$ALIVE_AT_END" == true ]]; then
  write_done "$STOP_AT" true "planned"
else
  write_done "$STOP_AT" false "serve_died"
fi

say "computing verdict"
python3 "$REPO/scripts/soak_verdict.py" "$OUT"
VERDICT_EXIT=$?
if [[ "$MODE" == self-check ]]; then
  say "self-check ($(( DURATION / 60 )) min): verdict exit $VERDICT_EXIT — a PASS proves the harness, not flat memory"
fi
exit "$VERDICT_EXIT"
