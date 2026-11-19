#!/usr/bin/env bash
# TEMPORARY (#2630 experiment, removed before this PR leaves draft).
#
# Samples memory once a second while a cargo step runs. Every 10 s it
# prints the extremes of that window, and when it is stopped (TERM) it
# prints the extremes of the whole step:
# - anon:      AnonPages, the memory that only swap can free
# - swap_used: SwapTotal - SwapFree
# - min_avail: lowest MemAvailable
# - psi_full:  highest "full" memory pressure (10 s average), in percent
# - links:     most rust-lld processes running at the same time
# It never stops or interrupts anything.
set -u
peak_anon=0 peak_swap=0 peak_psi=0 peak_links=0 min_avail=999999
w_anon=0 w_swap=0 w_psi=0 w_links=0 w_avail=999999
t0=$(date +%s)
n=0

report() {
  echo "[memtrace peak] anon=${peak_anon}M swap_used=${peak_swap}M min_avail=${min_avail}M psi_full=${peak_psi}% links=${peak_links}"
  swapon --show 2>/dev/null | sed 's/^/[memtrace swap] /'
  exit 0
}
trap report TERM INT

while sleep 1; do
  read -r anon swap avail < <(awk '
    /^AnonPages:/ { a = int($2 / 1024) }
    /^SwapTotal:/ { st = $2 }
    /^SwapFree:/ { sf = $2 }
    /^MemAvailable:/ { m = int($2 / 1024) }
    END { print a, int((st - sf) / 1024), m }' /proc/meminfo)
  psi=$(awk '/^full/ { split($2, a, "="); print int(a[2]) }' /proc/pressure/memory 2>/dev/null || echo 0)
  links=$(pgrep -cx rust-lld || true)

  [ "$anon" -gt "$w_anon" ] && w_anon=$anon
  [ "$swap" -gt "$w_swap" ] && w_swap=$swap
  [ "$psi" -gt "$w_psi" ] && w_psi=$psi
  [ "$links" -gt "$w_links" ] && w_links=$links
  [ "$avail" -lt "$w_avail" ] && w_avail=$avail

  [ "$anon" -gt "$peak_anon" ] && peak_anon=$anon
  [ "$swap" -gt "$peak_swap" ] && peak_swap=$swap
  [ "$psi" -gt "$peak_psi" ] && peak_psi=$psi
  [ "$links" -gt "$peak_links" ] && peak_links=$links
  [ "$avail" -lt "$min_avail" ] && min_avail=$avail

  n=$((n + 1))
  if [ $((n % 10)) -eq 0 ]; then
    echo "[memtrace +$(($(date +%s) - t0))s] anon=${w_anon}M swap_used=${w_swap}M min_avail=${w_avail}M psi_full=${w_psi}% links=${w_links} load=$(cut -d' ' -f1 /proc/loadavg)"
    w_anon=0 w_swap=0 w_psi=0 w_links=0 w_avail=999999
  fi
done
