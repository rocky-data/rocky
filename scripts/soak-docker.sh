#!/usr/bin/env bash
# Run the #1416 scheduler soak in Docker: both arms, on one or more platforms.
#
# Runs on the HOST (macOS or Linux), not in a container. For every
# (platform, arm) pair it starts one container that runs
# scripts/soak-scheduler.sh against the binary from a published engine image,
# then waits for them all and prints each verdict.
#
#   arm a  HARVEST_MODE=poll  reads /api/v1/runs every 600 s while it samples
#   arm b  HARVEST_MODE=end   reads the ledger once, after serve has stopped
#
# Usage:
#   bash scripts/soak-docker.sh VERSION [OUT_DIR]
#     VERSION   an engine release, e.g. 1.92.0 (pulls ghcr.io/rocky-data/rocky:VERSION)
#   PLATFORMS="linux/arm64 linux/amd64"   default: the host's native platform
#   ARMS="a b"                            default: both
#   DURATION_HOURS=24                     passed to the harness
#   SELF_CHECK=1                          a 20-minute run of each container instead
#   ROCKY_IMAGE=...                       use another image (a local build)
#
# Exit: 0 when every container's verdict is PASS, else the highest verdict exit.
#
# What this wrapper adds over `docker run` by hand, and why each one matters:
#
#   --stop-timeout 900   Docker's default grace is 10 s, then SIGKILL. On a stop
#                        the harness runs its end step (final harvest, a drain
#                        of up to 60 s, the end-mode ledger read of every run,
#                        the verdict), which takes minutes on a 24 h ledger.
#   --init               PID 1 forwards signals and reaps orphans, so the
#                        harness, not the kernel's PID-1 rules, sees SIGTERM.
#   --restart no         a restarted container would start a second soak in the
#                        same directory.
#   caffeinate           on macOS the harness's own `caffeinate` is a no-op:
#                        it runs inside the Linux VM. This holds the HOST awake
#                        for as long as the wrapper runs. Keep the wrapper in
#                        the foreground (tmux, or `nohup ... &`).
#   docker-events.jsonl  every stop, kill and die event for these containers,
#                        with the signal, so the next early stop has a cause
#                        on record. The 1.91.0 soak got SIGTERM in both
#                        containers in the same minute and nothing recorded who
#                        sent it.
#
# The x86_64 arm: run with PLATFORMS=linux/amd64 on a NATIVE x86_64 host. On an
# Apple-silicon Mac the same flag runs the binary under emulation, which adds
# the translator's own memory to the RSS the soak measures; the wrapper warns.

set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VERSION="${1:-}"
[[ -n "$VERSION" ]] || { echo "usage: bash scripts/soak-docker.sh VERSION [OUT_DIR]" >&2; exit 3; }
VERSION="${VERSION#engine-v}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="${2:-$REPO/.soak/docker-$STAMP}"
ROCKY_IMAGE="${ROCKY_IMAGE:-ghcr.io/rocky-data/rocky:$VERSION}"
ARMS="${ARMS:-a b}"
DURATION_HOURS="${DURATION_HOURS:-24}"
LABEL="rocky-soak=$STAMP"

say() { printf '[soak-docker %s] %s\n' "$(date -u +%H:%M:%S)" "$*"; }
die() { printf '[soak-docker] FATAL: %s\n' "$*" >&2; exit 3; }

command -v docker >/dev/null || die "docker is required"
docker info >/dev/null 2>&1 || die "the Docker daemon is not reachable"

case "$(uname -m)" in
  arm64|aarch64) NATIVE=linux/arm64 ;;
  x86_64|amd64) NATIVE=linux/amd64 ;;
  *) NATIVE="" ;;
esac
PLATFORMS="${PLATFORMS:-$NATIVE}"
[[ -n "$PLATFORMS" ]] || die "cannot tell the native platform; set PLATFORMS"

mkdir -p "$OUT" || die "cannot create $OUT"
OUT="$(cd "$OUT" && pwd)"

# The harness image: the release binary on a Debian base with the tools the
# harness needs. The binary is copied from the release image for the same
# platform, so the soak measures exactly what was shipped.
build_image() { # PLATFORM -> tag
  local platform="$1" tag
  tag="rocky-soak:${VERSION}-${platform##*/}"
  docker build --quiet --platform "$platform" --build-arg ROCKY_IMAGE="$ROCKY_IMAGE" \
    -t "$tag" -f - "$REPO/scripts" >/dev/null <<'DOCKERFILE' || return 1
ARG ROCKY_IMAGE
FROM ${ROCKY_IMAGE} AS rocky
FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash ca-certificates curl git jq lsof procps python3 \
 && rm -rf /var/lib/apt/lists/* \
 && git config --system --add safe.directory /repo
COPY --from=rocky /usr/local/bin/rocky /usr/local/bin/rocky
DOCKERFILE
  printf '%s' "$tag"
}

{
  printf '{"stamp":"%s","version":"%s","image":"%s","platforms":"%s","arms":"%s",' \
    "$STAMP" "$VERSION" "$ROCKY_IMAGE" "$PLATFORMS" "$ARMS"
  printf '"host_uname":"%s","docker":%s}\n' "$(uname -a)" \
    "$(docker info --format '{{json .}}' 2>/dev/null | python3 -c 'import json,sys
d=json.load(sys.stdin)
print(json.dumps({k:d.get(k) for k in ("ServerVersion","OperatingSystem","Architecture","NCPU","MemTotal","KernelVersion")}))' 2>/dev/null || echo null)"
} >"$OUT/host.json"

# Record every lifecycle event of these containers until the wrapper exits.
docker events --filter "label=$LABEL" --format '{{json .}}' >>"$OUT/docker-events.jsonl" 2>&1 &
EVENTS_PID=$!

if command -v caffeinate >/dev/null; then
  caffeinate -dimsu -w $$ &
  say "caffeinate holding the host awake while this wrapper runs"
elif command -v systemd-inhibit >/dev/null && [[ -n "${XDG_SESSION_ID:-}" ]]; then
  systemd-inhibit --what=sleep:idle --why="rocky scheduler soak" --mode=block \
    tail --pid=$$ -f /dev/null &
  say "systemd-inhibit holding off sleep while this wrapper runs"
else
  say "WARNING: no caffeinate or systemd-inhibit; make sure this host cannot sleep"
fi

CONTAINERS=()
for platform in $PLATFORMS; do
  if [[ "$platform" != "$NATIVE" ]]; then
    say "WARNING: $platform is emulated on this host; its RSS includes the translator's memory, so its verdict is INVALID (a harness check only)"
  fi
  say "building the harness image for $platform from $ROCKY_IMAGE"
  tag=$(build_image "$platform") || die "image build failed for $platform"
  for arm in $ARMS; do
    case "$arm" in
      a) mode=poll ;;
      b) mode=end ;;
      *) die "ARMS takes a and b (got '$arm')" ;;
    esac
    name="rocky-soak-$STAMP-${platform##*/}-$arm"
    dir="${platform##*/}-$arm"
    args=(bash /repo/scripts/soak-scheduler.sh)
    [[ -n "${SELF_CHECK:-}" ]] && args+=(--self-check)
    args+=("/out/$dir")
    docker run -d --name "$name" --label "$LABEL" --platform "$platform" \
      --init --restart no --stop-timeout 900 \
      -e HARVEST_MODE="$mode" -e DURATION_HOURS="$DURATION_HOURS" \
      -e ROCKY_BIN=/usr/local/bin/rocky -e SOAK_EMULATED="$([[ "$platform" == "$NATIVE" ]] && echo 0 || echo 1)" \
      -v "$REPO:/repo:ro" -v "$OUT:/out" \
      "$tag" "${args[@]}" >/dev/null || die "docker run failed for $name"
    CONTAINERS+=("$name")
    say "started $name (arm $arm, HARVEST_MODE=$mode) -> $OUT/$dir"
  done
done

say "waiting for ${#CONTAINERS[@]} container(s); follow with: docker logs -f <name>"
worst=0
for name in "${CONTAINERS[@]}"; do
  code=$(docker wait "$name" 2>/dev/null || echo 3)
  docker logs "$name" >"$OUT/$name.log" 2>&1
  say "$name exited $code"
  (( code > worst )) && worst=$code
done

kill "$EVENTS_PID" 2>/dev/null
for dir in "$OUT"/*/; do
  [[ -f "$dir/verdict.json" ]] || continue
  printf '%s: %s\n' "$(basename "$dir")" "$(jq -r '.verdict' "$dir/verdict.json")"
done
say "results in $OUT (docker-events.jsonl has every stop and kill)"
exit "$worst"
