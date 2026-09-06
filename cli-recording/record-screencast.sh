#!/usr/bin/env bash
# record-screencast.sh — the U3-A3 fulfillment screencast: three terminal tapes
# and three browser scenes, filmed against ONE workspace.
#
#   ./record-screencast.sh            # record everything into out/screencast/
#   ./record-screencast.sh --keep     # leave the workspace and server up after
#
# Why one workspace. A plan id is 64 hex characters on screen. If the terminal
# shows one and the browser another, a viewer who reads carefully sees a staged
# artifact and stops trusting the rest. So `prepare.sh` runs ONCE, a single
# `rocky serve --ui` stays up throughout, and the tapes and scenes interleave:
#
#   prepare  ─▶  scratch/fulfillment-review/       one workspace, one store
#                       │
#                       ├── rocky serve --ui ──────────────────┐  up throughout
#   tape 1 (a sentence → a plan) ─┤                            │
#                       ├──▶ browser: review                   │  the plan, pending
#   tape 2 (the deny, the approve) ┤                           │
#                       ├──▶ browser: samples                  │  rows, now real
#   tape 3 (staleness) ─┤                                      │
#                       └──▶ browser: journal                  ┘  the custody chain
#
# Requires: `rocky` on $PATH built with `--features ui`, plus vhs, duckdb, jq,
# node, and `npm install` already run in browser/. Credential-free — the POC's
# fulfillment driver is `replay`.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE"

DEMO=fulfillment-review
WS="$HERE/scratch/$DEMO"
OUT="$HERE/out/screencast"
PORT="${ROCKY_SCREENCAST_PORT:-18730}"
# An obviously fake token. It is printed in the terminal beat that starts the
# server, so it must not look like a real secret to anyone copying the frame.
TOKEN="screencast-demo-token-not-a-secret"
KEEP=0
[ "${1:-}" = "--keep" ] && KEEP=1

need() { command -v "$1" >/dev/null 2>&1 || { echo "record-screencast.sh: $1 not found" >&2; exit 1; }; }
need rocky; need vhs; need duckdb; need jq; need node

if ! rocky serve --help 2>&1 | grep -q -- "--ui"; then
    echo "record-screencast.sh: this rocky has no browser UI." >&2
    echo "  build it with:  (cd engine/ui && npm ci && npm run build)" >&2
    echo "                  cargo build --release --features ui" >&2
    exit 1
fi
if [ ! -d "$HERE/browser/node_modules" ]; then
    echo "record-screencast.sh: run 'npm install' in browser/ first" >&2
    exit 1
fi

mkdir -p "$OUT"

echo "▶ preparing the workspace (once — every tape and scene shares it)"
./prepare.sh "$DEMO" || exit 1

# Refuse to start on a port something else already answers. `/health` needs no
# token, so a stale server from a previous run answers it happily — and the
# recording would then film a DIFFERENT workspace than the tapes are driving,
# which is the one failure this whole script exists to prevent. It cost a run
# to learn: "Address already in use" went to the log, the health poll passed
# against the old process, and the browser filmed an empty review queue.
if curl -fsS --max-time 2 "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1; then
    echo "record-screencast.sh: something already answers on :$PORT." >&2
    echo "  stop it, or set ROCKY_SCREENCAST_PORT to a free port." >&2
    exit 1
fi

echo "▶ starting rocky serve --ui on :$PORT"
# `exec` so $! is the rocky process itself, not a subshell whose child would
# survive the trap.
(
    cd "$WS" || exit 1
    exec env ROCKY_SERVE_TOKEN="$TOKEN" ROCKY_SERVE_TOKEN_SCOPE=read-only \
        rocky serve --ui --port "$PORT"
) > "$OUT/serve.log" 2>&1 &
SERVER=$!
cleanup() {
    kill "$SERVER" 2>/dev/null
    [ "$KEEP" = "1" ] && echo "workspace kept at $WS"
}
trap cleanup EXIT

for _ in $(seq 1 60); do
    kill -0 "$SERVER" 2>/dev/null || break
    curl -fsS "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1 && break
    sleep 0.5
done
if ! kill -0 "$SERVER" 2>/dev/null; then
    echo "record-screencast.sh: the server exited — see $OUT/serve.log" >&2
    tail -3 "$OUT/serve.log" >&2
    exit 1
fi
if ! curl -fsS "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1; then
    echo "record-screencast.sh: the server never answered — see $OUT/serve.log" >&2
    exit 1
fi
URL="http://127.0.0.1:$PORT/ui/#token=$TOKEN"

tape() {
    echo "▶ tape $1"
    vhs "tapes/$DEMO-$1.tape" || { echo "tape $1 failed" >&2; exit 1; }
    mv "out/$DEMO-$1.gif" "$OUT/terminal-$1.gif" 2>/dev/null
}

scene() {
    echo "▶ browser scene $1"
    (cd browser && node record.mjs "$@" --url "$URL" --out "$OUT") \
        || { echo "browser scene $1 failed" >&2; exit 1; }
}

tape 1

# The plan id, read from the queue the tape just printed rather than
# hard-coded: the tape types `$PLAN`, so the id the viewer saw in the queue
# output is the id the shell expands. It is also what the later browser scenes
# navigate to — after tape 2's approve the review queue is EMPTY, because an
# approval marker resolves the escalation, so a scene that filmed after the
# approve could not find the plan by clicking it.
PLAN=$(cd "$WS" && rocky --output json review --queue 2>/dev/null | jq -r '.pending[0].plan_id')
if [ -z "$PLAN" ] || [ "$PLAN" = "null" ]; then
    echo "record-screencast.sh: no plan is waiting for review after tape 1." >&2
    echo "  the loop did not reach 'proposed' — see $OUT/serve.log and rerun tape 1 by hand" >&2
    exit 1
fi
echo "  plan $PLAN"
printf 'export PLAN=%s\n' "$PLAN" > "$WS/.screencast-env"

scene review
tape 2
scene samples --plan "$PLAN"

tape 3
scene journal

echo
echo "✓ recorded into $OUT"
ls -la "$OUT"
echo
echo "Next: intercut in the order terminal-1, review, terminal-2, samples,"
echo "terminal-3, journal. The browser scenes are .webm; the tapes are .gif."
echo "The cut is an mp4, not a GIF — minutes of screen content."
