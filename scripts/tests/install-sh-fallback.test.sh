#!/usr/bin/env bash
#
# Pin the version-resolution fallback in engine/install.sh (#2684).
#
# install.sh asks the GitHub REST API for the latest engine release, with no
# token. When that call fails (a 403 from the anonymous rate limit), it reads
# the tags with `git ls-remote` and takes the newest tag whose archive answers
# 200. This test runs the real install.sh with stub `curl` and `git` first on
# PATH, so it needs no network and no GitHub token. The stubs log every call;
# the test checks which path ran, which version was installed, and that no
# token reached either tool.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
readonly SCRIPT_DIR
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
readonly REPO_ROOT
readonly UNDER_TEST="$REPO_ROOT/engine/install.sh"

fail() {
    echo "FAIL: $1" >&2
    if [[ -n "${2:-}" && -f "$2" ]]; then
        echo "--- output ---" >&2
        cat "$2" >&2
    fi
    exit 1
}

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

readonly STUB_BIN="$WORK/bin"
readonly FIXTURE="$WORK/fixture"
mkdir -p "$STUB_BIN" "$FIXTURE/pkg"

# A release archive whose `rocky` prints a version.
printf '#!/bin/sh\necho "rocky 0.0.0-test"\n' >"$FIXTURE/pkg/rocky"
chmod +x "$FIXTURE/pkg/rocky"
tar czf "$FIXTURE/rocky.tar.gz" -C "$FIXTURE/pkg" rocky

# Stub curl. Behaviour comes from env vars set per case:
#   STUB_API          ok | fail   (the api.github.com call)
#   STUB_API_TAG      the tag_name the API returns when ok
#   STUB_HEAD         space-separated tag=status pairs for HEAD requests
cat >"$STUB_BIN/curl" <<'STUB'
#!/usr/bin/env bash
printf 'curl' >>"$STUB_LOG"
printf ' %q' "$@" >>"$STUB_LOG"
printf '\n' >>"$STUB_LOG"
out="" head=0 url="" fmt=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        -o) out="$2"; shift 2 ;;
        -w) fmt="$2"; shift 2 ;;
        --retry) shift 2 ;;
        -sIL) head=1; shift ;;
        -*) shift ;;
        *) url="$1"; shift ;;
    esac
done
case "$url" in
    https://api.github.com/*)
        if [[ "$STUB_API" == "ok" ]]; then
            printf '[{"tag_name": "dagster-v9.9.9"}, {"tag_name": "%s"}]\n' "$STUB_API_TAG"
            exit 0
        fi
        echo "curl: (22) The requested URL returned error: 403" >&2
        exit 22 ;;
esac
if [[ "$head" == 1 ]]; then
    tag="$(printf '%s\n' "$url" | sed 's#.*/releases/download/##; s#/.*##')"
    status=404
    for pair in $STUB_HEAD; do
        [[ "${pair%%=*}" == "$tag" ]] && status="${pair#*=}"
    done
    # Like real curl, print the status only when asked for it with -w.
    [[ "$fmt" == '%{http_code}' ]] && printf '%s' "$status"
    exit 0
fi
case "$url" in
    */checksums.txt)
        sum="$(shasum -a 256 "$STUB_FIXTURE" 2>/dev/null || sha256sum "$STUB_FIXTURE")"
        name="$(ls "$(dirname "$out")" | grep '^rocky-.*\.tar\.gz$' | head -1)"
        printf '%s  %s\n' "${sum%% *}" "$name" >"$out" ;;
    */rocky-lsp-*) exit 22 ;;
    */rocky-*.tar.gz) cp "$STUB_FIXTURE" "$out" ;;
    *) echo "stub curl: unexpected URL $url" >&2; exit 99 ;;
esac
STUB

# Stub git. STUB_GIT is ok | fail. It also logs the working directory and the
# config isolation the fallback promises.
cat >"$STUB_BIN/git" <<'STUB'
#!/usr/bin/env bash
printf 'git cwd=%s global=%s nosystem=%s prompt=%s home=%s xdg=%s gitdir=%s params=%s count=%s' \
    "$PWD" "${GIT_CONFIG_GLOBAL:-}" "${GIT_CONFIG_NOSYSTEM:-}" "${GIT_TERMINAL_PROMPT:-}" \
    "${HOME:-}" "${XDG_CONFIG_HOME:-}" "${GIT_DIR-unset}" "${GIT_CONFIG_PARAMETERS-unset}" \
    "${GIT_CONFIG_COUNT-unset}" >>"$STUB_LOG"
printf ' %q' "$@" >>"$STUB_LOG"
printf '\n' >>"$STUB_LOG"
if [[ "$STUB_GIT" != "ok" ]]; then
    echo "fatal: unable to access 'https://github.com/rocky-data/rocky.git/'" >&2
    exit 128
fi
for tag in engine-v1.2.0 engine-v1.10.0 engine-v1.9.0 engine-v1.10.0-rc1; do
    printf '0000000000000000000000000000000000000000\trefs/tags/%s\n' "$tag"
done
STUB
chmod +x "$STUB_BIN/curl" "$STUB_BIN/git"

readonly TOKEN_SENTINEL="ghp_install_sh_must_not_send_this"

# run_case NAME then env assignments; sets $OUT, $LOG, $DEST, $RC.
run_case() {
    local name="$1"
    shift
    OUT="$WORK/$name.out"
    LOG="$WORK/$name.log"
    DEST="$WORK/$name.dest"
    : >"$LOG"
    set +e
    env -u ROCKY_VERSION "$@" \
        PATH="$STUB_BIN:$PATH" \
        STUB_LOG="$LOG" STUB_FIXTURE="$FIXTURE/rocky.tar.gz" \
        GITHUB_TOKEN="$TOKEN_SENTINEL" \
        GIT_DIR="$WORK/inherited.git" GIT_CONFIG_COUNT=1 \
        GIT_CONFIG_KEY_0=http.extraheader GIT_CONFIG_VALUE_0="AUTHORIZATION: bearer $TOKEN_SENTINEL" \
        GIT_CONFIG_PARAMETERS="'http.extraheader'='AUTHORIZATION: bearer $TOKEN_SENTINEL'" \
        ROCKY_INSTALL_DIR="$DEST" \
        bash "$UNDER_TEST" </dev/null >"$OUT" 2>&1
    RC=$?
    set -e
    if grep -q "$TOKEN_SENTINEL" "$LOG"; then
        fail "$name: GITHUB_TOKEN reached curl or git" "$LOG"
    fi
}

# 1. API works: its answer wins and git is never called.
run_case api_ok STUB_API=ok STUB_API_TAG=engine-v1.4.0 STUB_GIT=ok STUB_HEAD=""
[[ "$RC" == 0 ]] || fail "api_ok: exit $RC" "$OUT"
grep -q "Installing Rocky engine-v1.4.0" "$OUT" || fail "api_ok: wrong version" "$OUT"
! grep -q '^git' "$LOG" || fail "api_ok: git ran although the API call worked" "$LOG"
[[ -x "$DEST/rocky" ]] || fail "api_ok: rocky not installed" "$OUT"
echo "ok: API path wins when the API call works"

# 2. API fails: the highest tag (by version, not text) that answers 200.
run_case fallback STUB_API=fail STUB_GIT=ok STUB_HEAD="engine-v1.10.0=200 engine-v1.9.0=200"
[[ "$RC" == 0 ]] || fail "fallback: exit $RC" "$OUT"
grep -q "Installing Rocky engine-v1.10.0" "$OUT" || fail "fallback: wrong version" "$OUT"
grep -q "git ls-remote" "$OUT" || fail "fallback: no notice of the fallback" "$OUT"
[[ -x "$DEST/rocky" ]] || fail "fallback: rocky not installed" "$OUT"
grep -q '^git cwd=/ global=/dev/null nosystem=1 prompt=0 home=/nonexistent-rocky-install xdg=/nonexistent-rocky-install gitdir=unset params=unset count=unset ls-remote --tags --refs https://github.com/rocky-data/rocky.git engine-v\\\*$' "$LOG" \
    || fail "fallback: git ran without the config isolation" "$LOG"
echo "ok: API failure falls back to the newest tag with assets"

# 3. The newest tag has no assets (404): skip it, take the next.
run_case skip_404 STUB_API=fail STUB_GIT=ok STUB_HEAD="engine-v1.10.0=404 engine-v1.9.0=200"
[[ "$RC" == 0 ]] || fail "skip_404: exit $RC" "$OUT"
grep -q "Installing Rocky engine-v1.9.0" "$OUT" || fail "skip_404: wrong version" "$OUT"
grep -q "Skipping engine-v1.10.0" "$OUT" || fail "skip_404: no skip notice" "$OUT"
echo "ok: a tag without assets (404) is skipped"

# 4. Any other status stops the script; it never installs an older engine.
run_case status_500 STUB_API=fail STUB_GIT=ok STUB_HEAD="engine-v1.10.0=500 engine-v1.9.0=200"
[[ "$RC" != 0 ]] || fail "status_500: exit 0" "$OUT"
grep -q "returned HTTP 500" "$OUT" || fail "status_500: no reason given" "$OUT"
[[ ! -e "$DEST/rocky" ]] || fail "status_500: installed anyway" "$OUT"
echo "ok: a non-404 error stops the fallback"

# 5. No tag has assets: fail with a message.
run_case none_found STUB_API=fail STUB_GIT=ok STUB_HEAD=""
[[ "$RC" != 0 ]] || fail "none_found: exit 0" "$OUT"
grep -q "no engine release with assets" "$OUT" || fail "none_found: no reason given" "$OUT"
echo "ok: no tag with assets fails"

# 6. git ls-remote fails: fail with a message, install nothing.
run_case git_fail STUB_API=fail STUB_GIT=fail STUB_HEAD="engine-v1.10.0=200"
[[ "$RC" != 0 ]] || fail "git_fail: exit 0" "$OUT"
grep -q "git ls-remote against https://github.com/rocky-data/rocky.git failed" "$OUT" \
    || fail "git_fail: no reason given" "$OUT"
[[ ! -e "$DEST/rocky" ]] || fail "git_fail: installed anyway" "$OUT"
echo "ok: a git ls-remote failure fails"

# 7. ROCKY_VERSION set: neither the API nor git is asked.
run_case pinned ROCKY_VERSION=engine-v1.2.0 STUB_API=fail STUB_GIT=fail STUB_HEAD=""
[[ "$RC" == 0 ]] || fail "pinned: exit $RC" "$OUT"
grep -q "Installing Rocky engine-v1.2.0" "$OUT" || fail "pinned: wrong version" "$OUT"
! grep -q 'api.github.com' "$LOG" || fail "pinned: API called" "$LOG"
! grep -q '^git' "$LOG" || fail "pinned: git called" "$LOG"
echo "ok: ROCKY_VERSION skips both lookups"

echo "install.sh fallback: all cases pass"
