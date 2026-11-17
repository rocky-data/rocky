#!/usr/bin/env bash
# Background memory monitor for the engine-ci Test job (#2630).
#
# Samples every 5 s and prints memory, swap, memory pressure and load once
# a minute. It interrupts cargo when either:
# - available memory plus free swap drops below GUARD_MB, or
# - the kernel's "full" memory pressure (share of time every task stalled
#   on memory, 10 s average) goes above GUARD_PSI percent.
# It first prints the largest processes, so the step fails with its log
# kept instead of the hosted runner dying with no log at all.
set -u
guard_mb=${GUARD_MB:-300}
guard_psi=${GUARD_PSI:-40}
n=0
while sleep 5; do
  avail=$(awk '/^MemAvailable:/{print int($2/1024)}' /proc/meminfo)
  swapfree=$(awk '/^SwapFree:/{print int($2/1024)}' /proc/meminfo)
  psi=$(awk '/^full/{split($2,a,"="); print int(a[2])}' /proc/pressure/memory 2>/dev/null || echo 0)
  load=$(cut -d' ' -f1 /proc/loadavg)
  line="mem_avail=${avail}M swap_free=${swapfree}M psi_full10=${psi}% load=${load}"
  if [ $((n % 12)) -eq 0 ]; then
    echo "[memory-guard $(date -u +%T)] $line"
  fi
  n=$((n + 1))
  if [ $((avail + swapfree)) -lt "$guard_mb" ] || [ "${psi:-0}" -gt "$guard_psi" ]; then
    echo "::error::memory-guard: runner near out of memory ($line); interrupting cargo"
    ps -eo pid,rss,etime,args --sort=-rss | head -15
    pkill -INT -f cargo-nextest || true
    pkill -INT -x cargo || true
    exit 0
  fi
done
