#!/usr/bin/env python3
"""Analyse a `scripts/soak-scheduler.sh` run and emit a machine-checkable verdict.

Stdlib only. Reads the run directory produced by the harness, applies gates
G0-G7, writes ``verdict.json``, prints a summary.

Exit codes: 0 = PASS, 1 = FAIL, 2 = INCOMPLETE / INVALID / CRASHED.

The verdict defaults to INCOMPLETE and only reaches PASS on positive evidence
of a full run. A soak that cannot tell "24h clean" from "died at hour 12" is
worse than no soak, so every completeness signal is default-deny.

Host sleep is classified INVALID rather than FAIL: a sleeping laptop makes the
next tick see N missed occurrences, which ``catchup = "latest"`` collapses into
a single fire. A naive checker reports that as hundreds of missed fires. Sleep
must never be able to masquerade as a scheduler bug.
"""

from __future__ import annotations

import datetime as dt
import json
import pathlib
import sys

# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def read_jsonl(path: pathlib.Path) -> list[dict]:
    """Parse a JSONL file, skipping unparseable lines rather than aborting.

    ``serve.stderr.jsonl`` interleaves two producers — the parent's tracing
    output and every child's inherited stderr — so partial lines are expected
    and must not kill the analysis.
    """
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rows.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return rows


def parse_ts(value: str) -> dt.datetime:
    return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))


def floor_minute(moment: dt.datetime) -> dt.datetime:
    return moment.replace(second=0, microsecond=0)


def median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return (ordered[mid - 1] + ordered[mid]) / 2.0


def ols_slope_per_hour(points: list[tuple[float, float]]) -> float | None:
    """Least-squares slope in units-per-hour. `points` are (epoch_s, value)."""
    if len(points) < 3:
        return None
    n = float(len(points))
    mean_x = sum(p[0] for p in points) / n
    mean_y = sum(p[1] for p in points) / n
    num = sum((x - mean_x) * (y - mean_y) for x, y in points)
    den = sum((x - mean_x) ** 2 for x, _ in points)
    if den == 0:
        return None
    return (num / den) * 3600.0


def window(samples: list[dict], key: str, lo: float, hi: float) -> list[float]:
    out = []
    for s in samples:
        if s.get(key) is None or not lo <= s["t"] <= hi:
            continue
        out.append(float(s[key]))
    return out


# --------------------------------------------------------------------------
# gates
# --------------------------------------------------------------------------


def gate_completeness(run: dict) -> list[dict]:
    """G0 — blocking, default-deny. Short-circuits every later gate."""
    findings = []
    meta, samples = run["meta"], run["samples"]

    if not run["done"]:
        exitinfo = run["exit"] or {}
        findings.append(
            {
                "gate": "G0",
                "status": "INCOMPLETE",
                "detail": "no DONE sentinel — the harness did not reach its planned end",
                "serve_exit": exitinfo,
            }
        )

    dead = [s for s in samples if s.get("alive") == 0]
    if dead:
        findings.append(
            {
                "gate": "G0",
                "status": "CRASHED",
                "detail": f"process died at epoch {dead[0]['t']}",
                "serve_exit": run["exit"],
            }
        )

    if not samples:
        findings.append({"gate": "G0", "status": "INVALID", "detail": "no samples recorded"})
        return findings

    interval = meta.get("sample_interval", 30)
    times = [s["t"] for s in samples]
    gaps = [(b - a) for a, b in zip(times, times[1:])]
    worst = max(gaps) if gaps else 0
    if worst > 3 * interval:
        idx = gaps.index(worst)
        findings.append(
            {
                "gate": "G0",
                "status": "INVALID",
                "detail": (
                    f"sampling gap of {worst}s (>3x the {interval}s interval) "
                    f"between epoch {times[idx]} and {times[idx + 1]} — "
                    "almost certainly host sleep, which coalesces occurrences "
                    "under catchup=latest and is indistinguishable from missed fires"
                ),
            }
        )

    observed = times[-1] - times[0]
    planned = meta.get("planned_end", 0) - meta.get("t0", 0)
    if planned and observed < planned - 2 * interval:
        findings.append(
            {
                "gate": "G0",
                "status": "INVALID",
                "detail": f"observed {observed}s of samples vs {planned}s planned",
            }
        )

    null_fd = sum(1 for s in samples if s.get("fd_total") is None)
    if samples and null_fd / len(samples) > 0.05:
        findings.append(
            {
                "gate": "G0",
                "status": "INVALID",
                "detail": f"{null_fd}/{len(samples)} samples have no fd count (lsof failing)",
            }
        )

    return findings


def dedup_fires(harvest: list[dict]) -> list[dict]:
    """The harvest is a repeated 50-record window, so records repeat by design."""
    seen: dict[str, dict] = {}
    for row in harvest:
        rid = row.get("run_id")
        if rid and rid not in seen:
            seen[rid] = row
    return sorted(seen.values(), key=lambda r: r["started_at"])


def gate_fires(run: dict) -> list[dict]:
    """G1 missed + G2 duplicate, over the deduped harvest ledger."""
    findings = []
    fires = run["fires"]
    if not fires:
        return [
            {
                "gate": "G1",
                "status": "FAIL",
                "detail": "no scheduled fires harvested at all — the loop never fired, "
                "or the harvest never succeeded (check harvest_errors.jsonl)",
            }
        ]

    # Attribution guard BEFORE trusting the bucketing. Minute bucketing is valid
    # only because catchup=latest coalescing needs a >60s tick-to-tick gap while
    # this guard catches delays in [30,60)s. A >=60s delay would both misbucket a
    # fire and coalesce the skipped occurrence — indistinguishable from a real
    # miss. At playground speed (~100ms child, 15s poll) only host sleep can do
    # that, and G0 catches host sleep.
    skewed = []
    for f in fires:
        started = parse_ts(f["started_at"])
        skew = (started - floor_minute(started)).total_seconds()
        if skew > 30:
            skewed.append({"run_id": f["run_id"], "started_at": f["started_at"], "skew_s": skew})
    if skewed:
        return [
            {
                "gate": "G1",
                "status": "INVALID",
                "detail": "ATTRIBUTION_UNSAFE: fires land >30s past their minute, "
                "so minute bucketing cannot attribute them",
                "examples": skewed[:5],
            }
        ]

    occurrences = [floor_minute(parse_ts(f["started_at"])) for f in fires]

    # G1 — every minute between the first and last fire must be represented.
    expected, cursor = [], occurrences[0]
    while cursor <= occurrences[-1]:
        expected.append(cursor)
        cursor += dt.timedelta(minutes=1)
    missing = sorted(set(expected) - set(occurrences))
    if missing:
        findings.append(
            {
                "gate": "G1",
                "status": "FAIL",
                "detail": f"{len(missing)} occurrence(s) came due with no run",
                "missing": [m.isoformat() for m in missing[:20]],
                "truncated": len(missing) > 20,
            }
        )

    # The loop can wedge while the HTTP server stays up; without this, a soak
    # that stopped firing at hour 12 still shows a gapless ledger for hours 0-12.
    meta = run["meta"]
    last, first = occurrences[-1], occurrences[0]
    end_epoch = (run["done"] or {}).get("end") or meta.get("planned_end", 0)
    if end_epoch:
        end = dt.datetime.fromtimestamp(end_epoch, dt.timezone.utc)
        if (end - last).total_seconds() > 180:
            findings.append(
                {
                    "gate": "G1",
                    "status": "FAIL",
                    "detail": f"fires stopped at {last.isoformat()} but the run ended "
                    f"{end.isoformat()} — the loop wedged while the server stayed up",
                }
            )
    t0 = meta.get("t0")
    if t0:
        started = dt.datetime.fromtimestamp(t0, dt.timezone.utc)
        if (first - started).total_seconds() > 240:
            findings.append(
                {
                    "gate": "G1",
                    "status": "FAIL",
                    "detail": f"first fire at {first.isoformat()} is >4min after start "
                    f"{started.isoformat()} — the loop started late",
                }
            )

    # G2 — one submission per occurrence.
    by_occ: dict[dt.datetime, list[dict]] = {}
    for occ, fire in zip(occurrences, fires):
        by_occ.setdefault(occ, []).append(fire)
    dupes = {occ: rows for occ, rows in by_occ.items() if len(rows) > 1}
    if dupes:
        findings.append(
            {
                "gate": "G2",
                "status": "FAIL",
                "detail": f"{len(dupes)} occurrence(s) fired more than once",
                "duplicates": [
                    {
                        "occurrence": occ.isoformat(),
                        "run_ids": [r["run_id"] for r in rows],
                        "submission_ids": [r.get("submission_id") for r in rows],
                    }
                    for occ, rows in sorted(dupes.items())[:10]
                ],
            }
        )

    # A repeated submission_id is a DIFFERENT bug (two run records for one
    # submission) and is reported separately from an occurrence firing twice.
    subs = [f.get("submission_id") for f in fires if f.get("submission_id")]
    if len(set(subs)) != len(subs):
        findings.append(
            {
                "gate": "G2",
                "status": "FAIL",
                "detail": "a submission_id appears on more than one run record — "
                "distinct from a duplicate fire",
            }
        )

    return findings


def gate_resources(run: dict) -> list[dict]:
    """G3 RSS (loose), G4 fd (sharp), G5 zombies."""
    findings = []
    samples = [s for s in run["samples"] if s.get("alive") == 1]
    if len(samples) < 10:
        return [{"gate": "G3", "status": "INVALID", "detail": "too few live samples"}]

    t_start, t_end = samples[0]["t"], samples[-1]["t"]
    warm = t_start + 600  # exclude a 10-minute warmup
    base_hi = min(warm + 3600, t_end)
    tail_lo = max(t_end - 3600, warm)

    # A run shorter than the warmup leaves the baseline and tail windows empty,
    # which would silently skip the two sharpest gates and still report PASS.
    # Never emit a green verdict for a gate that did not evaluate.
    if t_end <= warm:
        return [
            {
                "gate": "G3/G4",
                "status": "INVALID",
                "detail": f"run spans {t_end - t_start}s, within the 600s warmup — the RSS "
                "and fd gates cannot evaluate. Fine for smoke-testing the harness; "
                "it is NOT a soak result.",
            }
        ]

    # G3 — RSS. Conjunction of percentage AND absolute so drift on a small
    # baseline cannot trip it.
    base_rss = median(window(samples, "rss_kb", warm, base_hi))
    tail_rss = median(window(samples, "rss_kb", tail_lo, t_end))
    rss_slope = ols_slope_per_hour(
        [(s["t"], float(s["rss_kb"])) for s in samples if s.get("rss_kb") is not None and s["t"] >= warm]
    )
    if base_rss and tail_rss:
        growth_pct = (tail_rss - base_rss) / base_rss * 100.0
        delta_mb = (tail_rss - base_rss) / 1024.0
        run["metrics"]["rss"] = {
            "baseline_kb": base_rss,
            "final_kb": tail_rss,
            "growth_pct": round(growth_pct, 2),
            "delta_mb": round(delta_mb, 2),
            "slope_kb_per_h": round(rss_slope, 1) if rss_slope else None,
        }
        if growth_pct > 10 and delta_mb > 20:
            findings.append(
                {
                    "gate": "G3",
                    "status": "FAIL",
                    "detail": f"RSS grew {growth_pct:.1f}% (+{delta_mb:.1f} MB)",
                }
            )

    # G4 — fd. An integer, and a leak is monotonic, so this is far tighter than
    # RSS and is the gate most likely to catch a real bug.
    base_fd = median(window(samples, "fd_total", warm, base_hi))
    tail_fd = median(window(samples, "fd_total", tail_lo, t_end))
    fd_slope = ols_slope_per_hour(
        [(s["t"], float(s["fd_total"])) for s in samples if s.get("fd_total") is not None and s["t"] >= warm]
    )
    if base_fd is not None and tail_fd is not None:
        classes = {}
        for cls in ("fd_redb", "fd_lock", "fd_traces", "fd_tcp", "fd_other"):
            b = median(window(samples, cls, warm, base_hi))
            e = median(window(samples, cls, tail_lo, t_end))
            if b is not None and e is not None:
                classes[cls] = {"baseline": b, "final": e, "delta": e - b}
        run["metrics"]["fd"] = {
            "baseline": base_fd,
            "final": tail_fd,
            "delta": tail_fd - base_fd,
            "slope_per_h": round(fd_slope, 2) if fd_slope else None,
            "by_class": classes,
        }
        leaking = [c for c, v in classes.items() if v["delta"] > 4]
        if (tail_fd - base_fd) > 8 or (fd_slope is not None and fd_slope > 0.5):
            findings.append(
                {
                    "gate": "G4",
                    "status": "FAIL",
                    "detail": f"fd count grew {tail_fd - base_fd:+.0f} "
                    f"(slope {fd_slope:.2f}/h) — attributed to: "
                    f"{', '.join(leaking) if leaking else 'no single class'}",
                    "by_class": classes,
                }
            )

    if run["fd_limit_errors"]:
        findings.append(
            {
                "gate": "G4",
                "status": "FAIL",
                "detail": f"{run['fd_limit_errors']} 'too many open files' error(s) in serve stderr",
            }
        )

    # G5 — zombies. `children == 0` in nearly every sample is EXPECTED (a ~100ms
    # child against 30s sampling) and is NOT evidence for "one subprocess per
    # fire" — that claim rests entirely on G2. Defunct entries, by contrast,
    # persist until reaped, so this is the part ps can genuinely prove.
    zombie_samples = [s for s in samples if (s.get("zombies") or 0) > 0]
    run["metrics"]["max_zombies"] = max((s.get("zombies") or 0) for s in samples)
    if zombie_samples:
        findings.append(
            {
                "gate": "G5",
                "status": "FAIL",
                "detail": f"{len(zombie_samples)} sample(s) observed unreaped children",
                "first_at": zombie_samples[0]["t"],
            }
        )

    return findings


def gate_log_corroboration(run: dict) -> list[dict]:
    """G6 — the tick log and the run history are independent witnesses.

    ONE-SIDED. The log undercounting is a documented benign path: when the store
    cannot be reopened after the child has already run, the demand is reported as
    skipped with ``state_busy`` set and is never pushed to ``executed`` — while
    the child's run record exists. The harness's own periodic ``GET
    /api/v1/runs`` is exactly the kind of third-party opener that induces it, so
    a two-sided equality would hard-FAIL a 24h gate on the harness's own
    measurement.

    The bug-bearing direction is the log claiming MORE executions than the
    history has durable records for.
    """
    findings = []
    ticks = run["ticks"]
    fires = run["fires"]
    if not ticks:
        return [
            {
                "gate": "G6",
                "status": "INVALID",
                "detail": "no 'scheduler: tick complete' lines parsed from serve stderr",
            }
        ]

    sum_executed = sum(int(t.get("executed") or 0) for t in ticks)
    run["metrics"]["ticks"] = {
        "count": len(ticks),
        "sum_executed": sum_executed,
        "state_busy": sum(1 for t in ticks if t.get("state_busy")),
        "lock_overridden": sum(1 for t in ticks if t.get("lock_overridden")),
        "drained": sum(1 for t in ticks if t.get("drained")),
        "sum_failed": sum(int(t.get("failed") or 0) for t in ticks),
        "harvested_fires": len(fires),
    }

    if sum_executed > len(fires):
        findings.append(
            {
                "gate": "G6",
                "status": "FAIL",
                "detail": f"the loop logged {sum_executed} executions but only "
                f"{len(fires)} durable run records exist — an execution left no record",
            }
        )
    elif sum_executed < len(fires):
        busy = run["metrics"]["ticks"]["state_busy"]
        cause = (
            f"consistent with the {busy} tick(s) that could not reopen the store "
            "after their child ran"
            if busy
            else "NOT explained by a store-reopen skip (0 state_busy ticks) — if this "
            "run began with a non-empty state store, the extra runs predate it"
        )
        findings.append(
            {
                "gate": "G6",
                "status": "INFO",
                "detail": f"logged executions ({sum_executed}) < harvested runs "
                f"({len(fires)}): {cause}. The run history is the source of truth.",
            }
        )

    return findings


def gate_child_outcomes(run: dict) -> list[dict]:
    """G7 — the playground is hermetic, so any non-Success child is real signal."""
    bad = [f for f in run["fires"] if f.get("status") not in (None, "Success")]
    run["metrics"]["child_failures"] = len(bad)
    if not bad:
        return []
    return [
        {
            "gate": "G7",
            "status": "FAIL",
            "detail": f"{len(bad)} of {len(run['fires'])} child run(s) did not succeed — "
            "distinct from a missed or duplicate fire; the loop fired correctly",
            "examples": [{"run_id": b["run_id"], "status": b.get("status")} for b in bad[:10]],
        }
    ]


# --------------------------------------------------------------------------
# driver
# --------------------------------------------------------------------------

TICK_MESSAGE = "scheduler: tick complete"


def load(run_dir: pathlib.Path) -> dict:
    meta_path = run_dir / "meta.json"
    meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
    done_path = run_dir / "DONE"
    exit_path = run_dir / "serve.exit"

    stderr_rows = read_jsonl(run_dir / "serve.stderr.jsonl")
    ticks = []
    for row in stderr_rows:
        fields = row.get("fields") or {}
        if fields.get("message") == TICK_MESSAGE:
            ticks.append(fields)

    raw = (run_dir / "serve.stderr.jsonl").read_text(errors="replace") if (
        run_dir / "serve.stderr.jsonl"
    ).exists() else ""

    harvest = read_jsonl(run_dir / "harvest.jsonl")
    return {
        "dir": str(run_dir),
        "meta": meta,
        "samples": read_jsonl(run_dir / "samples.jsonl"),
        "harvest": harvest,
        "fires": dedup_fires(harvest),
        "ticks": ticks,
        "done": json.loads(done_path.read_text()) if done_path.exists() else None,
        "exit": json.loads(exit_path.read_text()) if exit_path.exists() else None,
        "harvest_errors": read_jsonl(run_dir / "harvest_errors.jsonl"),
        "fd_limit_errors": raw.count("too many open files"),
        "metrics": {},
    }


def analyse(run: dict) -> dict:
    findings = gate_completeness(run)
    blocking = [f for f in findings if f["status"] in ("INCOMPLETE", "CRASHED", "INVALID")]
    if not blocking:
        findings += gate_fires(run)
        findings += gate_resources(run)
        findings += gate_log_corroboration(run)
        findings += gate_child_outcomes(run)

    if any(f["status"] in ("INCOMPLETE", "CRASHED", "INVALID") for f in findings):
        verdict = next(
            f["status"] for f in findings if f["status"] in ("INCOMPLETE", "CRASHED", "INVALID")
        )
    elif any(f["status"] == "FAIL" for f in findings):
        verdict = "FAIL"
    else:
        verdict = "PASS"

    return {
        "verdict": verdict,
        "run_dir": run["dir"],
        "meta": run["meta"],
        "metrics": run["metrics"],
        "findings": findings,
        "harvest_error_count": len(run["harvest_errors"]),
    }


EXIT = {"PASS": 0, "FAIL": 1, "INCOMPLETE": 2, "INVALID": 2, "CRASHED": 2}


def self_test() -> int:
    """Exercise the gates against synthetic fixtures.

    Cheap insurance that the classification rules mean what they say — the
    verdict logic is where this harness's difficulty lives, and it would
    otherwise get its first real exercise 24 hours into a run.
    """
    # Must be minute-aligned: the fires below sit at +3s, and an unaligned base
    # would push them past the 30s attribution guard and make every fixture
    # INVALID for a reason that has nothing to do with the gate under test.
    base = 1_000_000_020
    assert base % 60 == 0, "self-test base must be minute-aligned"
    failures = []

    def check(name: str, got: str, want: str) -> None:
        if got != want:
            failures.append(f"{name}: expected {want}, got {got}")

    def synth(minutes: int, *, drop: set[int] = frozenset(), dup: set[int] = frozenset()) -> dict:
        harvest, n = [], 0
        for m in range(minutes):
            if m in drop:
                continue
            for k in range(2 if m in dup else 1):
                started = dt.datetime.fromtimestamp(base + m * 60 + 3, dt.timezone.utc)
                n += 1
                harvest.append(
                    {
                        "run_id": f"run-{n}",
                        "started_at": started.isoformat().replace("+00:00", "Z"),
                        "status": "Success",
                        "submission_id": f"sub-{n}",
                    }
                )
        samples = [
            {
                "t": base + i * 30,
                "alive": 1,
                "rss_kb": 50_000,
                "fd_total": 42,
                "fd_redb": 2,
                "fd_lock": 1,
                "fd_traces": 1,
                "fd_tcp": 3,
                "fd_other": 35,
                "zombies": 0,
            }
            for i in range(minutes * 2)
        ]
        end = base + minutes * 60
        return {
            "dir": "synthetic",
            "meta": {"t0": base, "planned_end": end, "sample_interval": 30},
            "samples": samples,
            "harvest": harvest,
            "fires": dedup_fires(harvest),
            "ticks": [{"executed": 1, "message": TICK_MESSAGE} for _ in harvest],
            "done": {"end": end},
            "exit": {"exit": 0, "expected": True},
            "harvest_errors": [],
            "fd_limit_errors": 0,
            "metrics": {},
        }

    check("clean run passes", analyse(synth(180))["verdict"], "PASS")
    check("missed fire fails", analyse(synth(180, drop={50}))["verdict"], "FAIL")
    check("duplicate fire fails", analyse(synth(180, dup={50}))["verdict"], "FAIL")

    no_done = synth(180)
    no_done["done"] = None
    check("missing DONE is INCOMPLETE", analyse(no_done)["verdict"], "INCOMPLETE")

    crashed = synth(180)
    crashed["samples"][100]["alive"] = 0
    check("death is CRASHED", analyse(crashed)["verdict"], "CRASHED")

    slept = synth(180)
    del slept["samples"][40:70]  # a gap far larger than 3x the interval
    check("host sleep is INVALID not FAIL", analyse(slept)["verdict"], "INVALID")

    leak = synth(180)
    for i, s in enumerate(leak["samples"]):
        s["fd_total"] = 42 + i  # monotonic leak
        s["fd_redb"] = 2 + i
    check("fd leak fails", analyse(leak)["verdict"], "FAIL")

    # The regression this guards: a two-sided G6 equality would FAIL here, on a
    # path the reconciler documents as benign and the harness itself induces.
    undercount = synth(180)
    undercount["ticks"] = undercount["ticks"][:-3]
    check("log undercount does NOT fail", analyse(undercount)["verdict"], "PASS")

    overcount = synth(180)
    overcount["ticks"].append({"executed": 5, "message": TICK_MESSAGE})
    check("log overcount fails", analyse(overcount)["verdict"], "FAIL")

    zombie = synth(180)
    zombie["samples"][77]["zombies"] = 1
    check("zombie fails", analyse(zombie)["verdict"], "FAIL")

    child_fail = synth(180)
    child_fail["fires"][9]["status"] = "Failure"
    check("child failure fails", analyse(child_fail)["verdict"], "FAIL")

    # A run inside the warmup must never report PASS: the RSS and fd gates
    # cannot evaluate, and a green verdict there would claim evidence the run
    # does not contain.
    check("run shorter than warmup is INVALID", analyse(synth(5))["verdict"], "INVALID")

    for line in failures:
        print(f"  SELF-TEST FAIL: {line}")
    print(f"self-test: {'PASS' if not failures else str(len(failures)) + ' FAILURE(S)'}")
    return 1 if failures else 0


def main() -> int:
    if len(sys.argv) > 1 and sys.argv[1] == "--self-test":
        return self_test()
    if len(sys.argv) != 2:
        print(__doc__)
        print("usage: soak_verdict.py <run-dir> | --self-test", file=sys.stderr)
        return 2

    run_dir = pathlib.Path(sys.argv[1])
    if not run_dir.is_dir():
        print(f"not a directory: {run_dir}", file=sys.stderr)
        return 2

    result = analyse(load(run_dir))
    (run_dir / "verdict.json").write_text(json.dumps(result, indent=2, default=str) + "\n")

    print(f"\n=== soak verdict: {result['verdict']} ===")
    metrics = result["metrics"]
    if "ticks" in metrics:
        t = metrics["ticks"]
        print(f"  ticks={t['count']} executions={t['sum_executed']} fires={t['harvested_fires']} "
              f"state_busy={t['state_busy']} drained={t['drained']}")
    if "rss" in metrics:
        r = metrics["rss"]
        print(f"  rss {r['baseline_kb']:.0f} -> {r['final_kb']:.0f} KB "
              f"({r['growth_pct']:+.1f}%, slope {r['slope_kb_per_h']} KB/h)")
    if "fd" in metrics:
        f = metrics["fd"]
        print(f"  fd  {f['baseline']:.0f} -> {f['final']:.0f} "
              f"({f['delta']:+.0f}, slope {f['slope_per_h']}/h)")
        for cls, v in f.get("by_class", {}).items():
            if v["delta"]:
                print(f"        {cls}: {v['delta']:+.0f}")
    for finding in result["findings"]:
        print(f"  [{finding['status']}] {finding['gate']}: {finding['detail']}")
    print(f"\n  full report: {run_dir / 'verdict.json'}\n")

    return EXIT[result["verdict"]]


if __name__ == "__main__":
    sys.exit(main())
