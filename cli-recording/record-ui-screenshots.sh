#!/usr/bin/env bash
# The browser UI screenshots and tour GIF in docs/public/ (ui-*.png,
# demo-ui-tour.gif), rebuilt from scratch against the rocky on PATH.
#
#   ./record-ui-screenshots.sh            # -> out/ui/
#   ./record-ui-screenshots.sh --publish  # ...and copy into docs/public/
#
# Three workspaces, because no single POC fills every screen:
#   estate    the `rocky playground` quickstart after one run (a 3-model DAG)
#   review    04-governance/11-agent-policy in a git repo: an agent drops the
#             PII column from dim_customer, and policy sends the plan to a human
#   governor  03-ai/08-fulfillment-walking-skeleton after its run.sh (policy
#             decisions, a custody chain, a product journal)
#
# Everything runs under a neutral identity. Run records store $USER and an
# approval marker stores the git email; both appear on screen, and a
# published screenshot must not carry whoever ran this script.
#
# Needs: rocky, duckdb, jq, git, node (+ `npm ci` in browser/), ffmpeg, curl.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
POCS="$REPO/examples/playground/pocs"
SCRATCH="$HERE/scratch/ui-screenshots"
OUT="$HERE/out/ui"
PUBLISH=0
[ "${1:-}" = "--publish" ] && PUBLISH=1

for tool in rocky duckdb jq git node ffmpeg curl; do
  command -v "$tool" >/dev/null || { echo "FAIL: $tool is not on PATH" >&2; exit 1; }
done
[ -d "$HERE/browser/node_modules/playwright" ] || {
  echo "FAIL: run 'npm ci && npx playwright install chromium' in cli-recording/browser first" >&2
  exit 1
}

rm -rf "$SCRATCH" "$OUT"
mkdir -p "$SCRATCH/home" "$OUT"
printf '[user]\n\tname = Rocky Demo\n\temail = demo@example.com\n' > "$SCRATCH/home/gitconfig"
export USER=demo USERNAME=demo LOGNAME=demo
export GIT_CONFIG_GLOBAL="$SCRATCH/home/gitconfig" GIT_CONFIG_NOSYSTEM=1
export RUST_LOG=error ROCKY_SUPPRESS_DEPRECATION=1

echo "▶ workspace 1: playground quickstart"
(cd "$SCRATCH" && rocky playground estate >/dev/null && cd estate && rocky --output json run >/dev/null)

echo "▶ workspace 2: an agent's breaking change waiting for review"
mkdir -p "$SCRATCH/review"
cp "$POCS/04-governance/11-agent-policy/rocky.toml" "$SCRATCH/review/"
cp -R "$POCS/04-governance/11-agent-policy/models" "$SCRATCH/review/"
(
  cd "$SCRATCH/review"
  git init -q -b main && git add -A && git commit -q -m baseline
  rocky --output json run >/dev/null
  printf 'SELECT 1 AS id\n' > models/dim_customer.sql   # the agent drops `email`
  rocky --output json plan --principal agent --base HEAD --model dim_customer > plan.json
  plan="$(jq -r '.plan_id' plan.json)"
  # The apply is refused on purpose: that refusal is what queues the plan.
  if ROCKY_PRINCIPAL=agent rocky apply "$plan" > apply.txt 2>&1; then
    echo "FAIL: the agent's apply was not sent to review" >&2
    exit 1
  fi
)

echo "▶ workspace 3: fulfillment walking skeleton"
cp -R "$POCS/03-ai/08-fulfillment-walking-skeleton" "$SCRATCH/governor"
(cd "$SCRATCH/governor" && bash run.sh >run.log 2>&1) || {
  echo "FAIL: the fulfillment POC did not complete; see $SCRATCH/governor/run.log" >&2
  exit 1
}

TOKEN="screenshots-read-only"
AUTH="Authorization: Bearer $TOKEN"
PIDS=()
cleanup() { for pid in ${PIDS[@]+"${PIDS[@]}"}; do kill "$pid" 2>/dev/null || true; done; }
trap cleanup EXIT

serve() { # <dir> <port>
  (cd "$1" && exec rocky serve --ui --token "$TOKEN" --token-scope read-only --port "$2") >"$1/serve.log" 2>&1 &
  PIDS+=("$!")
  # Probe an authenticated route: /health is token-exempt, so a stale server
  # left on the port by an earlier run would pass a health probe.
  for _ in $(seq 1 50); do
    curl -fsS -H "$AUTH" "http://127.0.0.1:$2/api/v1/meta" >/dev/null 2>&1 && return 0
    sleep 0.2
  done
  echo "FAIL: rocky serve in $1 did not come up; see $1/serve.log" >&2
  exit 1
}
serve "$SCRATCH/estate" 18751
serve "$SCRATCH/review" 18752
serve "$SCRATCH/governor" 18753

PLAN="$(curl -fsS -H "$AUTH" http://127.0.0.1:18752/api/v1/review/queue | jq -r '.pending[0].plan_id // empty')"
[ -n "$PLAN" ] || { echo "FAIL: the review queue is empty; the review shot needs a pending plan" >&2; exit 1; }

# Fail on a leak of the real identity or a local path into any route a shot
# renders. The body is captured first, so a failed request fails the script
# instead of reading as "no leak".
leak_check() { # <port> <route>...
  local port="$1" route body
  shift
  for route in "$@"; do
    # A 503 (engine_not_ready while the startup compile runs, or engine_busy)
    # is retryable; anything still failing after ~10 s fails the script.
    body=""
    for _ in $(seq 1 20); do
      body="$(curl -fsS -H "$AUTH" "http://127.0.0.1:$port/api/v1/$route" 2>/dev/null)" && break
      body=""
      sleep 0.5
    done
    [ -n "$body" ] || { echo "FAIL: GET /api/v1/$route on :$port did not answer 200" >&2; exit 1; }
    if printf '%s' "$body" | grep -q -e "$HOME" -e "/Users/" -e "/home/" -e "$REPO"; then
      echo "FAIL: /api/v1/$route on :$port carries a local path or home directory" >&2
      exit 1
    fi
  done
}
# The routes the UI calls (engine/ui/src: every apiGet), per workspace.
leak_check 18751 meta project dag runs schedule models/customer_orders
leak_check 18752 meta review/queue "review/$PLAN" "review/$PLAN/status" models/dim_customer
leak_check 18753 meta brief audit "audit/scorecard?by=principal&window=all" custody/revenue_daily products \
  products/revenue_daily products/revenue_daily/journal

SHOTS="$HERE/browser/screenshots.mjs"
node "$SHOTS" --url "http://127.0.0.1:18751/ui/#token=$TOKEN" --out "$OUT" \
  ui-estate=/ui/estate
node "$SHOTS" --url "http://127.0.0.1:18752/ui/#token=$TOKEN" --out "$OUT" \
  ui-review-queue=/ui/review \
  ui-review="/ui/review/$PLAN"
node "$SHOTS" --url "http://127.0.0.1:18753/ui/#token=$TOKEN" --out "$OUT" \
  ui-governor-brief=/ui/governor \
  ui-governor-custody=/ui/governor/custody/revenue_daily \
  ui-governor-product=/ui/governor/products/revenue_daily

echo "▶ tour GIF"
FRAMES="$SCRATCH/frames"
mkdir -p "$FRAMES"
i=0
for shot in ui-estate ui-review-queue ui-review ui-governor-brief ui-governor-custody ui-governor-product; do
  i=$((i + 1))
  cp "$OUT/$shot.png" "$FRAMES/$(printf '%03d' "$i").png"
done
# A fresh palette per frame, so no still carries remnants of the one before.
ffmpeg -loglevel error -y -framerate 1/3 -i "$FRAMES/%03d.png" \
  -vf "scale=1200:-1:flags=lanczos,split[a][b];[a]palettegen=stats_mode=single:max_colors=128[p];[b][p]paletteuse=new=1:dither=none" \
  -loop 0 "$OUT/demo-ui-tour.gif"

ls -la "$OUT"
if [ "$PUBLISH" = 1 ]; then
  cp "$OUT"/ui-estate.png "$OUT"/ui-review.png "$OUT"/ui-governor-*.png "$OUT"/demo-ui-tour.gif "$REPO/docs/public/"
  echo "✓ published to docs/public/ — look at every image before you commit it"
fi
