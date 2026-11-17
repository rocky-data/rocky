#!/usr/bin/env bash
# Background memory monitor for the engine-ci Test job (#2630).
#
# Prints memory, swap and load once a minute. When available memory plus
# free swap drops below GUARD_MB, it prints the largest processes and
# interrupts cargo, so the step fails with its log kept instead of the
# hosted runner dying with no log at all.
set -u
guard_mb=${GUARD_MB:-300}
while sleep "${INTERVAL_S:-60}"; do
  avail=$(awk '/^MemAvailable:/{print int($2/1024)}' /proc/meminfo)
  swapfree=$(awk '/^SwapFree:/{print int($2/1024)}' /proc/meminfo)
  load=$(cut -d' ' -f1 /proc/loadavg)
  echo "[memory-guard $(date -u +%T)] mem_avail=${avail}M swap_free=${swapfree}M load=${load}"
  if [ $((avail + swapfree)) -lt "$guard_mb" ]; then
    echo "::error::memory-guard: under ${guard_mb} MB left (mem_avail=${avail}M swap_free=${swapfree}M); interrupting cargo"
    ps -eo pid,rss,etime,args --sort=-rss | head -15
    pkill -INT -f cargo-nextest || true
    pkill -INT -x cargo || true
    exit 0
  fi
done
