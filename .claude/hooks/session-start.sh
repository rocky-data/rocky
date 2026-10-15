#!/bin/bash
# SessionStart hook for Claude Code cloud sessions.
#
# Installs the two tools AGENTS.md and the justfile assume but cloud
# containers lack: `just` (the monorepo task runner) and `cargo-nextest`
# (what engine-ci.yml runs). Versions are pinned and checksum-verified.
#
# It never fails the session: a failed install prints a warning, and the
# fallback in AGENTS.md ("run the commands inside the justfile recipe
# directly") still applies. It does not enable .git-hooks/: pre-push runs a
# cold clippy build, which is too slow for an agent's `git push`.
#
# To bump a tool: change its version, download the x86_64 Linux tarball from
# the URL below, take `sha256sum` of it, cross-check it against the
# publisher's checksum file, and replace the SHA256 value.
set -uo pipefail

if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi

# The pinned tarballs are x86_64 Linux builds.
if [ "$(uname -sm)" != "Linux x86_64" ]; then
  echo "session-start: not Linux x86_64, skipping tool install" >&2
  exit 0
fi

JUST_VERSION="1.40.0"
JUST_SHA256="181b91d0ceebe8a57723fb648ed2ce1a44d849438ce2e658339df4f8db5f1263"
NEXTEST_VERSION="0.9.140"
NEXTEST_SHA256="4ee9aaa0d0171a985a5d0eb735b87355894c1c455972e9674fb9fdbd1387c9a3"

BIN_DIR="${CARGO_HOME:-$HOME/.cargo}/bin"
mkdir -p "$BIN_DIR"
export PATH="$BIN_DIR:$PATH"

warn() { echo "session-start: $*" >&2; }

# install_tool NAME VERSION_CMD WANT URL SHA256 MEMBER
install_tool() {
  local name="$1" have_cmd="$2" want="$3" url="$4" sha="$5" member="$6"
  # Match the exact version: "$want" followed by end of line, a space or "(".
  if bash -c "$have_cmd" 2>/dev/null | head -n1 | grep -qE "^${want//./\\.}( |\(|\$)"; then
    return 0
  fi
  local tmp
  tmp="$(mktemp -d)" || { warn "$name: mktemp failed"; return 0; }
  if ! curl -sSLf --retry 3 --max-time 120 -o "$tmp/pkg.tgz" "$url"; then
    warn "$name: download failed ($url)"; rm -rf "$tmp"; return 0
  fi
  if ! echo "$sha  $tmp/pkg.tgz" | sha256sum -c --status; then
    warn "$name: checksum mismatch, not installing"; rm -rf "$tmp"; return 0
  fi
  if tar -xzf "$tmp/pkg.tgz" -C "$tmp" "$member" && install -m 0755 "$tmp/$member" "$BIN_DIR/$member"; then
    echo "session-start: installed $want"
  else
    warn "$name: unpack failed"
  fi
  rm -rf "$tmp"
  return 0
}

install_tool just "just --version" "just $JUST_VERSION" \
  "https://github.com/casey/just/releases/download/${JUST_VERSION}/just-${JUST_VERSION}-x86_64-unknown-linux-musl.tar.gz" \
  "$JUST_SHA256" just

install_tool cargo-nextest "cargo nextest --version" "cargo-nextest $NEXTEST_VERSION" \
  "https://github.com/nextest-rs/nextest/releases/download/cargo-nextest-${NEXTEST_VERSION}/cargo-nextest-${NEXTEST_VERSION}-x86_64-unknown-linux-gnu.tar.gz" \
  "$NEXTEST_SHA256" cargo-nextest

if [ -n "${CLAUDE_ENV_FILE:-}" ]; then
  line="export PATH=\"$BIN_DIR:\$PATH\""
  grep -qxF "$line" "$CLAUDE_ENV_FILE" 2>/dev/null || echo "$line" >> "$CLAUDE_ENV_FILE"
fi
exit 0
