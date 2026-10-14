#!/usr/bin/env bash
# Smoke-test the Linux release binaries before engine-release.yml publishes
# them.
#
# For each binary (`rocky` and `rocky-lsp`):
#   1. glibc floor: no glibc symbol newer than 2.28.
#   2. C++ runtime floor: no GLIBCXX symbol newer than 3.4.25 and no CXXABI
#      symbol newer than 1.3.11. These are the versions in gcc 8's
#      libstdc++.so.6, the system one on a glibc 2.28 distribution. The
#      binaries link the host's libstdc++.so.6, so a newer symbol would break
#      them on older distributions. The smoke in a new image cannot see that.
#   3. Every shared library it needs resolves.
#
# Then, for `rocky` only:
#   4. A real DuckDB extension downloads and loads. A playground model that
#      needs `icu` (`current_date + 1`, a time-zone conversion) must run.
#      Engine 1.81.0 crashed on Linux exactly here (#2367), and no release
#      step ran the Linux binary before it was published.
#
# And for `rocky-lsp`: it starts and exits on a closed stdin without a crash.
#
# Needs network access: DuckDB downloads the icu extension.
#
# Usage: scripts/linux_release_smoke.sh <path-to-rocky> <path-to-rocky-lsp>

set -euo pipefail

rocky="$(realpath "${1:?usage: linux_release_smoke.sh <path-to-rocky> <path-to-rocky-lsp>}")"
rocky_lsp="$(realpath "${2:?usage: linux_release_smoke.sh <path-to-rocky> <path-to-rocky-lsp>}")"

# Fail if the binary needs a symbol version above the ceiling.
#   $1 binary, $2 symbol prefix (GLIBC_, GLIBCXX_, CXXABI_), $3 ceiling
check_symbol_ceiling() {
  local bin="$1" prefix="$2" ceiling="$3" highest
  highest="$(objdump -T "$bin" | grep -o "${prefix}[0-9][0-9.]*" | sed "s/^${prefix}//" | sort -uV | tail -1 || true)"
  echo "$(basename "$bin"): highest ${prefix%_} symbol version: ${highest:-none}"
  if [ -n "$highest" ] && [ "$(printf '%s\n' "$highest" "$ceiling" | sort -V | tail -1)" != "$ceiling" ]; then
    echo "::error::$bin needs ${prefix%_} $highest; the release ceiling is $ceiling"
    exit 1
  fi
}

for bin in "$rocky" "$rocky_lsp"; do
  check_symbol_ceiling "$bin" GLIBC_ 2.28
  check_symbol_ceiling "$bin" GLIBCXX_ 3.4.25
  check_symbol_ceiling "$bin" CXXABI_ 1.3.11
  if ldd "$bin" | grep -q "not found"; then
    ldd "$bin"
    echo "::error::$bin needs a shared library that is not installed"
    exit 1
  fi
done

# rocky-lsp serves on stdio and takes no flags. With stdin closed it should
# exit; `timeout` (status 124) covers a server that waits instead. A status
# of 126 or 127 (cannot execute) or above 128 (killed by a signal) fails.
status=0
timeout 10 "$rocky_lsp" < /dev/null > /dev/null 2>&1 || status=$?
echo "rocky-lsp on a closed stdin: exit status $status"
if [ "$status" -eq 126 ] || [ "$status" -eq 127 ] || [ "$status" -gt 128 ]; then
  echo "::error::rocky-lsp failed to start (exit status $status)"
  exit 1
fi

"$rocky" --version

work="$(mktemp -d)"
"$rocky" playground "$work/pg" --template quickstart > /dev/null
cd "$work/pg"
cp models/raw_orders.toml models/icu_probe.toml
printf "SELECT current_date + 1 AS tomorrow, now() AT TIME ZONE 'Europe/Lisbon' AS lisbon_now\n" \
  > models/icu_probe.sql

status=0
"$rocky" run > run.json 2> run.err || status=$?
if [ "$status" -ne 0 ]; then
  tail -50 run.err
  echo "::error::rocky run exited with status $status"
  exit 1
fi
if grep -q "could not load the DuckDB icu extension" run.err; then
  grep "icu" run.err
  echo "::error::the DuckDB icu extension did not load"
  exit 1
fi
if ! grep -q '"status": *"Success"' run.json || ! grep -q '"tables_failed": *0' run.json \
  || ! grep -q '"icu_probe"' run.json; then
  cat run.json
  echo "::error::rocky run did not materialize every model, icu_probe included"
  exit 1
fi

echo "Smoke passed: glibc <= 2.28, GLIBCXX <= 3.4.25, CXXABI <= 1.3.11, libraries resolve, rocky-lsp starts, the icu extension loads"
