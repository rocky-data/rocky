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

echo "▶ starting rocky serve --ui on :$PORT"
(
    cd "$WS" || exit 1
    ROCKY_SERVE_TOKEN="$TOKEN" ROCKY_SERVE_TOKEN_SCOPE=read-only \
        rocky serve --ui --port "$PORT"
) > "$OUT/serve.log" 2>&1 &
SERVER=$!
cleanup() {
    kill "$SERVER" 2>/dev/null
    [ "$KEEP" = "1" ] && echo "workspace kept at $WS"
}
trap cleanup EXIT

for _ in $(seq 1 60); do
    curl -fsS "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1 && break
    sleep 0.5
done
if ! curl -fsS "http://127.0.0.1:$PORT/api/v1/health" >/dev/null 2>&1; then
    echo "record-screencast.sh: the server never came up — see $OUT/serve.log" >&2
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
    (cd browser && node record.mjs "$1" --url "$URL" --out "$OUT") \
        || { echo "browser scene $1 failed" >&2; exit 1; }
}

tape 1
scene review

# The deny and the approve name a plan id. Take it from the queue the tape just
# printed rather than hard-coding one: the tape types `$PLAN`, and the id the
# viewer saw in the queue output is the id the shell expands.
PLAN=$(cd "$WS" && rocky --output json review --queue 2>/dev/null | jq -r '.pending[0].plan_id')
if [ -z "$PLAN" ] || [ "$PLAN" = "null" ]; then
    echo "record-screencast.sh: no plan is waiting for review after tape 1." >&2
    echo "  the loop did not reach 'proposed' — see $OUT/serve.log and rerun tape 1 by hand" >&2
    exit 1
fi
echo "  plan $PLAN"
printf 'export PLAN=%s\n' "$PLAN" > "$WS/.screencast-env"

tape 2
scene samples

tape 3
scene journal

echo
echo "✓ recorded into $OUT"
ls -la "$OUT"
echo
echo "Next: intercut in the order terminal-1, review, terminal-2, samples,"
echo "terminal-3, journal. The browser scenes are .webm; the tapes are .gif."
echo "The cut is an mp4, not a GIF — minutes of screen content."
