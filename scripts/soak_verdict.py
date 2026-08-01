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
# G3b calibration — the final-quarter RSS slope
# --------------------------------------------------------------------------
#
# G3's percentage-AND-absolute conjunction is a catastrophic-growth trip. It
# cannot see the leak class this harness exists to hunt: two real 24h runs grew
# 45.0% / +11.77 MB and 40.45% / +11.16 MB and both cleared it, including the
# run whose curve was the JobRegistry leak's existence proof. Neither reached
# the 20 MB floor, so the conjunction never fired.
#
# The discriminating measurement is the FINAL QUARTER. A bounded structure
# fills once — the job cache reaches its cap around hour 8.5 at one job per
# minute — and must then be flat. Growth still present at hour 18-24 is not a
# fill, it is a leak. Nothing here looks for an inflection at the fill point:
# on both reference runs the curve is linear end to end (pre-fill +506.9 KB/h,
# post-fill +481.9 KB/h), so there is no knee to detect.
#
# THRESHOLD, from the data. Least-squares slope over the last 25% of samples:
#
#   leaking runs, measured                 +526.9 and +456.0 KB/h
#   noise band, both runs detrended and    <=130.1 KB/h over 4h windows
#     surveyed over late-run windows        <=58.2 KB/h over 6h windows
#
# 200 KB/h sits ~1.5x above the worst noise slope a 4h window produced and
# ~2.3x below the smaller of the two real leaks. In absolute terms it is
# ~4.7 MB/day still accumulating past hour 18, which no bounded structure
# should show.
#
# THE THRESHOLD AND THE WINDOW FLOOR ARE ONE CONSTANT, NOT TWO. The noise band
# widens sharply as the window shrinks — the same survey gives ~188 KB/h at 3h,
# ~1900-2900 KB/h at 1h. Lowering the floor without raising the threshold turns
# G3b into a false-positive generator. The floor cannot be a clean 6h either:
# a nominal 24h run samples 23.99h, so its final quarter is 21592s (5.998h).
#
# EVIDENCE, not just arithmetic. A slope is only as good as its coverage, so
# this is default-deny: on a run long enough to judge, an RSS series that does
# not cover the window is INVALID, never a quiet PASS. Three readings four hours
# apart fit a perfect flat line while leaving two hours unobserved.
#
# The slope must be fitted to the harness's own sampling grid, whole. Anything
# less is a subset, and NO statistic over timestamps can detect a subset chosen
# in a way that correlates with the values it contains: retain one jitter phase
# through the first half of the window and another through the second, and a
# real +220 KB/h leak fits as -448 KB/h with every gap still inside the cap.
# Regular spacing does not rescue it — that construction is perfectly regular.
# So the rule is not a density heuristic; it removes the freedom to fit a subset
# at all, from either direction:
#
#   * every recorded row in the window must be alive and carry an RSS reading,
#     which forecloses selection by nulling the reading; and
#   * every spacing must sit within 25% of the declared sampling interval, which
#     forecloses selection by omitting the whole row. A maximum-gap rule is not
#     enough on its own: a series can switch phase using one short catch-up gap
#     and stay under any cap. Requiring UNIFORM spacing is what pins the fitted
#     points to the grid the harness actually scheduled, since a phase switch
#     costs a spacing that is too short exactly where a dropped slot costs one
#     that is too long.
#
# The scan starts one interval BEFORE the window so that a slot omitted right at
# the boundary shows up as a double spacing rather than as a short run-in.
#
# Both reference runs are rigid grids: spacings of 28-31s at a declared 30s
# interval, never more than 2s off a perfect grid across 24h, 720 of 720
# readings present in the final quarter. So none of this costs anything that has
# ever been observed, and an INVALID here means re-run, not "leaking".
#
# The gap is ALSO capped absolutely at 90s, never inferred from the declared
# interval alone, because `SAMPLE_SECONDS` is a harness knob a coarse run could
# turn down to widen its own gaps. 90s is 3x the harness's default sampling, the
# same tolerance G0 already applies to sample spacing. It bounds the cadence the
# slope is fitted at, which matters: re-running the survey above on both traces
# decimated to each cadence, sweeping every phase offset (a fixed every-Nth
# alignment can alias periodic jitter, and phase 0 alone understates the band by
# up to 60%), gives
#
#   cadence     30s    60s    90s   120s   240s   300s   600s
#   4h window  130.1  137.7  146.9  164.3  197.8  201.5  342.8   KB/h
#   6h window   58.2   60.1   63.9   71.7   86.0   88.0  153.1   KB/h
#
# so the cap and the window floor together are what keep 200 KB/h above the
# noise. The binding case is the coarsest cadence at the shortest window — 90s
# over 4h, 146.9 KB/h, a 1.36x margin — against 3.4x in the nominal case of a
# 24h run sampled at 30s. Widening either constant eats that margin directly:
# at 300s over 4h the noise band has already crossed the threshold.
#
# Calibration provenance: n=2, same host, same workload, and the noise band was
# measured by detrending runs that were themselves leaking. Re-derive it if the
# workload changes.
FINAL_QUARTER_SLOPE_MAX_KB_H = 200.0
FINAL_QUARTER_MIN_SPAN_S = 4 * 3600
FINAL_QUARTER_MAX_GAP_S = 90
FINAL_QUARTER_GRID_TOLERANCE = 0.25


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

    # `alive` is a two-valued liveness flag. Anything else is a malformed row,
    # and it must not be quietly dropped: every later gate filters on
    # ``alive == 1``, so an unrecognised value would silently shorten the
    # timeline those gates believe they judged.
    odd = [s for s in samples if s.get("alive") not in (0, 1)]
    if odd:
        findings.append(
            {
                "gate": "G0",
                "status": "INVALID",
                "detail": f"{len(odd)} sample(s) carry an alive flag that is neither 0 nor 1 "
                f"(first {odd[0].get('alive')!r} at epoch {odd[0]['t']}) — the liveness signal "
                "cannot be read, so no later gate's timeline can be trusted",
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

    # Proximity warning for the bucketing assumption — NOT a safety check.
    #
    # `floor_minute` truncates, so this skew is always in [0,60) and can never
    # observe the failure it guards against directly. The actual failure is a
    # fire delayed >=60s past its occurrence: it buckets into the NEXT minute,
    # which surfaces as a duplicate there plus a miss where it belonged. What
    # this measures is how close the run runs to that cliff.
    #
    # So it is graduated rather than absolute: one late fire in 1440 is noise
    # and must not invalidate a 24h run, while a systematic drift toward the
    # 60s boundary means G1/G2 verdicts can no longer be trusted.
    skewed = []
    for f in fires:
        started = parse_ts(f["started_at"])
        skew = (started - floor_minute(started)).total_seconds()
        if skew > 30:
            skewed.append({"run_id": f["run_id"], "started_at": f["started_at"], "skew_s": skew})

    if skewed:
        worst = max(s["skew_s"] for s in skewed)
        rate = len(skewed) / len(fires)
        run["metrics"]["skew"] = {
            "late_fires": len(skewed),
            "rate_pct": round(rate * 100, 2),
            "max_skew_s": worst,
        }
        if rate > 0.01 or worst > 50:
            return [
                {
                    "gate": "G1",
                    "status": "INVALID",
                    "detail": f"ATTRIBUTION_UNSAFE: {len(skewed)}/{len(fires)} fires "
                    f"({rate * 100:.1f}%) land >30s past their minute, worst {worst:.0f}s. "
                    "Fires approaching 60s late bucket into the following minute, which "
                    "reads as a duplicate there and a miss where they belonged — so the "
                    "missed/duplicate verdicts cannot be trusted.",
                    "examples": skewed[:5],
                }
            ]
        findings.append(
            {
                "gate": "G1",
                "status": "INFO",
                "detail": f"{len(skewed)}/{len(fires)} fires landed >30s past their minute "
                f"(worst {worst:.0f}s). Tolerated — attribution is still correct below 60s.",
            }
        )

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
    """G3a RSS growth (loose), G3b final-quarter RSS slope, G4 fd (sharp), G5 zombies."""
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

    # G3a — RSS, catastrophic growth. Conjunction of percentage AND absolute so
    # drift on a small baseline cannot trip it. Deliberately blind to a slow
    # leak; G3b below is the gate for that and fires independently of this one.
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
                    "gate": "G3a",
                    "status": "FAIL",
                    "detail": f"RSS grew {growth_pct:.1f}% (+{delta_mb:.1f} MB)",
                }
            )

    # G3b — final-quarter RSS slope. See the calibration block above. This is
    # the gate for a slow leak and is evaluated INDEPENDENTLY of G3a: the two
    # real leaks that motivated it clear G3a's conjunction comfortably.
    #
    # ONE-SIDED, deliberately. Falling RSS is a pass: freed entries return to
    # allocator free lists rather than to the OS, so a bounded structure holding
    # at high-water — or dipping, as one reference run does twice inside its own
    # final quarter — is exactly what "flat" looks like in practice.
    # The window is cut from the SAMPLE timeline, never from where RSS readings
    # happen to exist: a run whose rss_kb went null at hour 18 must not be able
    # to shrink its own final quarter down to nothing and out of the gate.
    quarter_lo = max(t_end - (t_end - t_start) * 0.25, warm)
    quarter_h = (t_end - quarter_lo) / 3600.0
    interval = run["meta"].get("sample_interval") or 30
    tight, loose = (
        interval * (1 - FINAL_QUARTER_GRID_TOLERANCE),
        interval * (1 + FINAL_QUARTER_GRID_TOLERANCE),
    )
    # Scan from one interval before the window; fit only from the window itself.
    scan = [s for s in run["samples"] if quarter_lo - loose <= s["t"] <= t_end]
    # Counted over EVERY recorded row in the window, not the alive-filtered
    # view: a row that is not alive, or carries no reading, is a hole either way.
    rows = [s for s in scan if s["t"] >= quarter_lo]
    quarter = [
        (s["t"], float(s["rss_kb"]))
        for s in rows
        if s.get("alive") == 1 and s.get("rss_kb") is not None
    ]
    dropped = len(rows) - len(quarter)
    spacings = [b["t"] - a["t"] for a, b in zip(scan, scan[1:])]
    if scan:
        spacings.append(t_end - scan[-1]["t"])
    off_grid = [g for g in spacings if not tight <= g <= loose and g > 0]
    widest = max(spacings) if spacings else (t_end - quarter_lo)
    fq = run["metrics"].setdefault("rss", {})
    fq["final_quarter"] = {
        "from_epoch": quarter_lo,
        "window_h": round(quarter_h, 2),
        "samples": len(quarter),
        "dropped_readings": dropped,
        "grid_interval_s": interval,
        "off_grid_spacings": len(off_grid),
        "worst_spacing_s": round(widest),
        "slope_kb_per_h": None,
        "threshold_kb_per_h": FINAL_QUARTER_SLOPE_MAX_KB_H,
        "min_window_h": FINAL_QUARTER_MIN_SPAN_S / 3600.0,
        "max_gap_s": FINAL_QUARTER_MAX_GAP_S,
    }

    if t_end - quarter_lo < FINAL_QUARTER_MIN_SPAN_S:
        # A genuinely short run — harness smoke-testing, not a soak. Say so out
        # loud so a green verdict is never read as "memory is flat" when it only
        # means "no gate fired".
        findings.append(
            {
                "gate": "G3b",
                "status": "INFO",
                "detail": f"final-quarter slope NOT EVALUATED: this run's last 25% is "
                f"{quarter_h:.2f}h, short of the {FINAL_QUARTER_MIN_SPAN_S / 3600.0:.0f}h floor "
                "the threshold is calibrated for. A PASS below means no gate fired — it is NOT "
                "evidence that RSS is flat.",
            }
        )
    elif dropped or off_grid or widest > FINAL_QUARTER_MAX_GAP_S:
        # Long enough to judge, so the evidence is owed. Default-deny: a fitted
        # line through a series that does not cover the window is not a
        # measurement of that window.
        why = []
        if dropped:
            why.append(f"{dropped} of {len(rows)} recorded sample(s) are not live or carry no "
                       "RSS reading")
        if off_grid:
            why.append(
                f"{len(off_grid)} spacing(s) are off the declared {interval}s grid "
                f"(worst {max(off_grid):.0f}s, band {tight:.0f}-{loose:.0f}s)"
            )
        if widest > FINAL_QUARTER_MAX_GAP_S:
            why.append(
                f"the widest spacing is {widest:.0f}s, past the {FINAL_QUARTER_MAX_GAP_S}s "
                "cadence the threshold is calibrated for"
            )
        findings.append(
            {
                "gate": "G3b",
                "status": "INVALID",
                "detail": f"the final quarter is {quarter_h:.2f}h — long enough to judge — but the "
                f"RSS series is not the grid the harness scheduled: {'; '.join(why)}. A slope "
                "fitted to a subset cannot see a leak hiding in what it skipped, and a subset "
                "that was not chosen independently of the values it contains can invert the sign "
                "of a real one.",
            }
        )
    else:
        # Coverage holds at <=90s across a >=4h window, so OLS has >=3 distinct x.
        quarter_slope = ols_slope_per_hour(quarter)
        fq["final_quarter"]["slope_kb_per_h"] = round(quarter_slope, 1)
        if quarter_slope > FINAL_QUARTER_SLOPE_MAX_KB_H:
            findings.append(
                {
                    "gate": "G3b",
                    "status": "FAIL",
                    "detail": f"RSS gained ground across the final quarter: least-squares slope "
                    f"{quarter_slope:+.0f} KB/h over {quarter_h:.2f}h ({len(quarter)} readings), "
                    f"above the {FINAL_QUARTER_SLOPE_MAX_KB_H:.0f} KB/h ceiling. A bounded "
                    "structure fills once and then holds, so this window is neither flat nor a "
                    "fill that finished before it. Read the series before promoting — a steady "
                    "climb and a single large late step both land here.",
                }
            )
        else:
            findings.append(
                {
                    "gate": "G3b",
                    "status": "INFO",
                    "detail": f"final-quarter RSS slope {quarter_slope:+.0f} KB/h over "
                    f"{quarter_h:.2f}h ({len(quarter)} readings) — within the "
                    f"{FINAL_QUARTER_SLOPE_MAX_KB_H:.0f} KB/h band.",
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

    def synth(
        minutes: int,
        *,
        drop: set[int] = frozenset(),
        dup: set[int] = frozenset(),
        rss=None,
        interval: int = 30,
        omit=None,
    ) -> dict:
        """`rss` maps seconds-since-start to an RSS in KB, or to None for a
        sample where the reading is missing; the default is dead flat.
        `interval` is the sampling cadence the whole run was recorded at, and
        `omit` drops whole sample rows — a different hole from a null reading."""
        rss = rss or (lambda _elapsed: 50_000)
        omit = omit or (lambda _elapsed: False)
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
                "t": base + s,
                "alive": 1,
                "rss_kb": None if rss(s) is None else round(rss(s)),
                "fd_total": 42,
                "fd_redb": 2,
                "fd_lock": 1,
                "fd_traces": 1,
                "fd_tcp": 3,
                "fd_other": 35,
                "zombies": 0,
            }
            for s in range(0, minutes * 60, interval)
            if not omit(s)
        ]
        end = base + minutes * 60
        return {
            "dir": "synthetic",
            "meta": {"t0": base, "planned_end": end, "sample_interval": interval},
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

    # G3b — the final-quarter slope. Sized like the two real 24h runs the old
    # conjunction could not see: ~490 KB/h, ~11 MB total, which never reaches
    # the 20 MB absolute floor.
    day = 24 * 60

    def gate_status(result: dict, gate: str) -> str:
        hits = [f["status"] for f in result["findings"] if f["gate"] == gate]
        return hits[0] if hits else "SILENT"

    check("a flat 24h run passes", analyse(synth(day))["verdict"], "PASS")

    slow_leak = analyse(synth(day, rss=lambda s: 50_000 + 490.0 * s / 3600.0))
    check("a ~490 KB/h leak fails", slow_leak["verdict"], "FAIL")
    check("...and it is G3b that catches it", gate_status(slow_leak, "G3b"), "FAIL")
    # The regression this pins: the leak must be invisible to G3a, otherwise the
    # fixture is not reproducing the class of leak that reported PASS.
    check("...while G3a stays silent on it", gate_status(slow_leak, "G3a"), "SILENT")

    # A correctly bounded structure fills once — the job cache caps around hour
    # 8.5 at one job per minute — and then holds. That ramp is entirely outside
    # the final quarter, so it must not fail. This is the false positive that
    # would sink the gate, and the reason no inflection detection is needed.
    fill_s = 8.53 * 3600
    filled = analyse(synth(day, rss=lambda s: 50_000 + 490.0 * min(s, fill_s) / 3600.0))
    check("a one-time cache fill then flat passes", filled["verdict"], "PASS")
    check("...with G3b evaluated, not skipped", gate_status(filled, "G3b"), "INFO")

    # Freed entries return to allocator free lists, not to the OS, so falling
    # RSS is a pass — the gate is one-sided by design.
    check(
        "declining RSS passes",
        analyse(synth(day, rss=lambda s: 60_000 - 490.0 * s / 3600.0))["verdict"],
        "PASS",
    )

    # Below the calibrated window floor the slope is noise-dominated, so it is
    # not evaluated — and the report must say so rather than let PASS be read
    # as "RSS is flat".
    short = analyse(synth(180))
    check("a 3h run still passes", short["verdict"], "PASS")
    check("...and declares G3b unevaluated", gate_status(short, "G3b"), "INFO")
    check(
        "...saying so explicitly",
        "stated"
        if any("NOT EVALUATED" in f["detail"] for f in short["findings"] if f["gate"] == "G3b")
        else "silent",
        "stated",
    )

    # On a run LONG ENOUGH to judge, the RSS evidence is owed. Each of these
    # reported PASS before the coverage requirements existed: the window is cut
    # from the sample timeline, so a series that stops early cannot shrink the
    # quarter out of the gate, and a series too sparse to see between its own
    # points cannot certify the gaps it never looked at.
    hour = 3600
    check(
        "a 24h run whose RSS readings stop at hour 18 is INVALID",
        analyse(synth(day, rss=lambda s: 50_000 if s < 18 * hour else None))["verdict"],
        "INVALID",
    )
    check(
        "a 24h run with no RSS readings at all is INVALID",
        analyse(synth(day, rss=lambda _s: None))["verdict"],
        "INVALID",
    )
    check(
        "three readings spanning 4h do not certify the quarter",
        analyse(
            synth(day, rss=lambda s: 50_000 if s in (18 * hour, 20 * hour, 22 * hour) else None)
        )["verdict"],
        "INVALID",
    )
    check(
        "sampling too sparse to cover the quarter is INVALID",
        analyse(synth(day, rss=lambda s: 50_000 if s % 1800 == 0 else None))["verdict"],
        "INVALID",
    )

    # The cadence boundary, both sides. A run sampled every 90s sits exactly on
    # the cap and must still evaluate and still catch the leak; 120s is past it,
    # and the survey in the calibration block puts the noise band there within
    # 1.2x of the threshold at the window floor, so it is INVALID not judged.
    coarse = analyse(synth(day, interval=90))
    check("a 90s sampling cadence still evaluates", gate_status(coarse, "G3b"), "INFO")
    check("...and passes when flat", coarse["verdict"], "PASS")
    coarse_leak = analyse(synth(day, interval=90, rss=lambda s: 50_000 + 490.0 * s / 3600.0))
    check("a leak at a 90s cadence is still caught", gate_status(coarse_leak, "G3b"), "FAIL")
    check(
        "a 120s cadence is past the calibrated range",
        analyse(synth(day, interval=120))["verdict"],
        "INVALID",
    )

    # Selection bias, the hole no timestamp statistic can see: which samples the
    # slope is fitted to must not depend on what those samples say. Both routes
    # into a subset are closed, and the pair below also pins the boundary — the
    # last sample outside the window versus the first one inside it.
    last_out = int((day * 60 - 30) * 0.75) // 30 * 30
    check(
        "a single missing RSS reading inside the final quarter is INVALID",
        analyse(synth(day, rss=lambda s: None if s == last_out + 30 else 50_000))["verdict"],
        "INVALID",
    )
    check(
        "...while the same hole one sample earlier is outside the window",
        analyse(synth(day, rss=lambda s: None if s == last_out else 50_000))["verdict"],
        "PASS",
    )
    # Omitting the row entirely leaves no null to count, so the grid rule is
    # what catches it: at a declared 30s cadence a 90s spacing means scheduled
    # samples are missing, and that is the spacing a phase-switch attack needs.
    check(
        "whole sample rows omitted from the final quarter is INVALID",
        analyse(synth(day, omit=lambda s: s > last_out and s % 90 != 0))["verdict"],
        "INVALID",
    )
    check(
        "...including a single row at the window's leading edge",
        analyse(synth(day, omit=lambda s: s == last_out + 30))["verdict"],
        "INVALID",
    )
    # The attack a maximum-gap rule cannot see: switch sampling phase using ONE
    # short catch-up spacing, and every gap stays under any cap while the fitted
    # subset correlates with a periodic component of the values. Uniform spacing
    # is what rejects it — the catch-up spacing is too SHORT for the grid.
    def phase_switch(s: int) -> bool:
        if s <= last_out:
            return False
        return (s % 90 != 30) if s < last_out + 8100 else (s % 90 != 0)

    check(
        "a mid-window sampling phase switch is INVALID",
        analyse(synth(day, interval=30, omit=phase_switch))["verdict"],
        "INVALID",
    )
    # An unreadable liveness flag must not silently shorten the timeline every
    # later gate believes it judged.
    odd_alive = synth(day)
    for s in odd_alive["samples"][2200:]:
        s["alive"] = 2
    check("an alive flag that is neither 0 nor 1 is INVALID", analyse(odd_alive)["verdict"], "INVALID")

    # One late fire in a long run is noise and must not throw away 24h of data;
    # a systematic drift toward the 60s misattribution cliff must.
    one_late = synth(180)
    one_late["fires"][20]["started_at"] = one_late["fires"][20]["started_at"].replace(":03Z", ":35Z")
    check("a single late fire is tolerated", analyse(one_late)["verdict"], "PASS")

    many_late = synth(180)
    for f in many_late["fires"][::10]:
        f["started_at"] = f["started_at"].replace(":03Z", ":35Z")
    check("systematic lateness is INVALID", analyse(many_late)["verdict"], "INVALID")

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
        if "baseline_kb" in r:
            print(f"  rss {r['baseline_kb']:.0f} -> {r['final_kb']:.0f} KB "
                  f"({r['growth_pct']:+.1f}%, slope {r['slope_kb_per_h']} KB/h)")
        q = r.get("final_quarter")
        if q:
            got = "NOT MEASURED" if q["slope_kb_per_h"] is None else f"{q['slope_kb_per_h']:+.1f} KB/h"
            print(f"      final quarter ({q['window_h']:.2f}h, n={q['samples']}): {got} "
                  f"(fail above {q['threshold_kb_per_h']:.0f} KB/h)")
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
