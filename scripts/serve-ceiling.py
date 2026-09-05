#!/usr/bin/env python3
"""Measure the one-process ceiling of `rocky serve` under the browser UI's reads.

Stdlib only. A promotion gate, not a CI job: the number is machine-specific by
construction.

The browser UI is a stream of GETs against one resident process, so the
ceiling is defined on the read routes the screens use. A staircase of
concurrent clients loops over that route mix with keep-alive connections,
holding each step for a fixed time. Per step the script reports requests,
req/s, p50/p95/p99 latency, errors (a non-200 or a timeout) and the server's
RSS before and after. The ceiling is the last step with zero errors and a p99
under the threshold; the first breaching step is reported beside it.

Two guards keep the number honest:

* Warm-up. Every route in the mix must answer 200 once before any load is
  applied. A route the server lacks makes the run INVALID (exit 2) and names
  the route. A silent skip would measure a cheaper mix than the one claimed.
  `/api/v1/dag/status` is not in the default mix for that reason: it answers
  503 until a DAG run has been recorded, and a 503 is cheap, so it would add
  a route the playground cannot answer without adding load. Add it with
  `--mix` on a project that has one.
* Calibration. The top step is also run against `/api/v1/health` alone, the
  cheapest route, to measure the harness's own limit. A ceiling within 80 % of
  that limit is client-bound: the number would be Python's, not the server's,
  and the run is INVALID.

Soak mode (`--soak-minutes M --concurrency C`) holds one load and samples RSS
every `--sample-seconds` on a grid, then reports the least-squares slope in
KB/h over the run and over its final quarter, the same statistic
`scripts/soak_verdict.py` gates on. No pass mark: the threshold belongs to the
issue the slope feeds (#1416).

Usage:

    rocky serve --port 8420 --token T &
    python3 scripts/serve-ceiling.py --url http://127.0.0.1:8420 --token T \
        --pid $! --out .ceiling

Exit codes: 0 = a verdict was produced (VALID or CLIENT_BOUND is stated in
it), 2 = INVALID (server unreachable, a route failed warm-up, or the ceiling
was client-bound).
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import pathlib
import platform
import subprocess
import sys
import threading
import time
import urllib.parse

DEFAULT_MIX = [
    "/api/v1/models",
    "/api/v1/models/{first_model}",
    "/api/v1/dag/layers",
    "/api/v1/runs",
    "/api/v1/schedule",
    "/api/v1/review/queue",
    "/api/v1/brief",
    "/api/v1/audit",
    "/api/v1/products",
]
DEFAULT_STEPS = "1,2,4,8,16,32,64,128"
REQUEST_TIMEOUT_S = 5.0
CLIENT_BOUND_RATIO = 0.8


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------


def percentile(values: list[float], q: float) -> float | None:
    """The q-quantile of `values` by nearest rank; None on an empty list."""
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(q * len(ordered)))
    return ordered[index]


def ols_slope_per_hour(points: list[tuple[float, float]]) -> float | None:
    """Least-squares slope in units-per-hour. `points` are (epoch_s, value)."""
    if len(points) < 2:
        return None
    n = len(points)
    mean_t = sum(t for t, _ in points) / n
    mean_v = sum(v for _, v in points) / n
    var = sum((t - mean_t) ** 2 for t, _ in points)
    if var == 0:
        return None
    cov = sum((t - mean_t) * (v - mean_v) for t, v in points)
    return cov / var * 3600.0


def rss_kb(pid: int | None) -> int | None:
    """Resident set size in KB from `ps`, the same reading the soak takes."""
    if pid is None:
        return None
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True,
            text=True,
            check=False,
            timeout=5,
        ).stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return None
    return int(out) if out.isdigit() else None


class Target:
    """Where the requests go, and how to open a connection there."""

    def __init__(self, url: str, token: str | None) -> None:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "http" or not parsed.hostname:
            raise SystemExit(f"INVALID: --url must be http://host:port, got {url!r}")
        self.host = parsed.hostname
        self.port = parsed.port or 80
        self.headers = {"Accept": "application/json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

    def connect(self) -> http.client.HTTPConnection:
        return http.client.HTTPConnection(
            self.host, self.port, timeout=REQUEST_TIMEOUT_S
        )

    def get_once(self, path: str) -> tuple[int, bytes]:
        conn = self.connect()
        try:
            conn.request("GET", path, headers=self.headers)
            resp = conn.getresponse()
            return resp.status, resp.read()
        finally:
            conn.close()


# --------------------------------------------------------------------------
# load
# --------------------------------------------------------------------------


def worker(
    target: Target,
    mix: list[str],
    offset: int,
    deadline: float,
    latencies: list[float],
    errors: list[str],
) -> None:
    """One client: loop over the mix on a keep-alive connection until the deadline."""
    conn = target.connect()
    i = offset
    while time.monotonic() < deadline:
        path = mix[i % len(mix)]
        i += 1
        started = time.perf_counter()
        try:
            conn.request("GET", path, headers=target.headers)
            resp = conn.getresponse()
            resp.read()
            status = resp.status
        except (OSError, http.client.HTTPException) as exc:
            errors.append(f"{path}: {type(exc).__name__}")
            conn.close()
            conn = target.connect()
            continue
        latencies.append((time.perf_counter() - started) * 1000.0)
        if status != 200:
            errors.append(f"{path}: HTTP {status}")
    conn.close()


def run_step(
    target: Target, mix: list[str], concurrency: int, seconds: float, pid: int | None
) -> dict:
    """Hold `concurrency` clients on the mix for `seconds`; return the step's row."""
    rss_before = rss_kb(pid)
    deadline = time.monotonic() + seconds
    per_thread_latencies: list[list[float]] = [[] for _ in range(concurrency)]
    per_thread_errors: list[list[str]] = [[] for _ in range(concurrency)]
    threads = [
        threading.Thread(
            target=worker,
            args=(
                target,
                mix,
                n,
                deadline,
                per_thread_latencies[n],
                per_thread_errors[n],
            ),
            daemon=True,
        )
        for n in range(concurrency)
    ]
    started = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - started
    latencies = [x for chunk in per_thread_latencies for x in chunk]
    errors = [x for chunk in per_thread_errors for x in chunk]
    rss_after = rss_kb(pid)
    return {
        "concurrency": concurrency,
        "seconds": round(elapsed, 2),
        "requests": len(latencies),
        "rps": round(len(latencies) / elapsed, 1) if elapsed > 0 else 0.0,
        "p50_ms": round(percentile(latencies, 0.50) or 0.0, 2),
        "p95_ms": round(percentile(latencies, 0.95) or 0.0, 2),
        "p99_ms": round(percentile(latencies, 0.99) or 0.0, 2),
        "max_ms": round(max(latencies), 2) if latencies else 0.0,
        "errors": len(errors),
        "error_sample": sorted(set(errors))[:5],
        "rss_kb_before": rss_before,
        "rss_kb_after": rss_after,
    }


def warm_up(target: Target, mix: list[str]) -> tuple[list[str], dict, list[str]]:
    """Resolve placeholders and require 200 from every route once.

    Returns the resolved mix, the server's `/api/v1/meta` document, and the
    routes that failed (empty on success).
    """
    status, body = target.get_once("/api/v1/meta")
    if status != 200:
        return mix, {}, [f"/api/v1/meta: HTTP {status}"]
    meta = json.loads(body)

    resolved = list(mix)
    if any("{first_model}" in path for path in resolved):
        status, body = target.get_once("/api/v1/models")
        first = None
        if status == 200:
            models = json.loads(body).get("models") or []
            if models:
                first = models[0].get("name")
        if first is None:
            return (
                resolved,
                meta,
                ["/api/v1/models/{first_model}: no model to resolve it with"],
            )
        resolved = [path.replace("{first_model}", first) for path in resolved]

    failed = []
    for path in resolved:
        status, _ = target.get_once(path)
        if status != 200:
            failed.append(f"{path}: HTTP {status}")
    return resolved, meta, failed


# --------------------------------------------------------------------------
# modes
# --------------------------------------------------------------------------


def staircase(
    args: argparse.Namespace, target: Target, mix: list[str], meta: dict
) -> dict:
    steps = [int(s) for s in args.steps.split(",") if s.strip()]
    rows = []
    breach = None
    for concurrency in steps:
        row = run_step(target, mix, concurrency, args.step_seconds, args.pid)
        rows.append(row)
        print_row(row)
        if row["errors"] > 0 or row["p99_ms"] > args.p99_ms:
            breach = row
            break

    passing = [r for r in rows if r["errors"] == 0 and r["p99_ms"] <= args.p99_ms]
    ceiling = passing[-1] if passing else None

    top = max(steps)
    calibration = run_step(target, ["/api/v1/health"], top, args.step_seconds, args.pid)
    calibration["route"] = "/api/v1/health"
    print_row(calibration, label="calibration")

    verdict: dict = {"threshold_p99_ms": args.p99_ms}
    if ceiling is None:
        verdict["state"] = "NO_CEILING"
        verdict["reason"] = (
            "the first step already breached; lower the ladder or raise the threshold"
        )
    elif breach is None:
        verdict["state"] = "NOT_REACHED"
        verdict["reason"] = (
            f"no step breached; the ceiling is at or above {ceiling['concurrency']} clients"
        )
    else:
        verdict["state"] = "VALID"
        verdict["reason"] = (
            f"{ceiling['concurrency']} clients held p99 {ceiling['p99_ms']} ms; "
            f"{breach['concurrency']} clients broke it "
            f"(p99 {breach['p99_ms']} ms, errors {breach['errors']})"
        )
    if ceiling is not None:
        verdict["ceiling_concurrency"] = ceiling["concurrency"]
        verdict["ceiling_rps"] = ceiling["rps"]
        verdict["ceiling_p99_ms"] = ceiling["p99_ms"]
        if (
            calibration["rps"] > 0
            and ceiling["rps"] >= CLIENT_BOUND_RATIO * calibration["rps"]
        ):
            verdict["state"] = "CLIENT_BOUND"
            verdict["reason"] = (
                f"the ceiling's {ceiling['rps']} req/s is within {int(CLIENT_BOUND_RATIO * 100)}% of "
                f"the harness's own {calibration['rps']} req/s on /api/v1/health; "
                "the number is the harness's, not the server's"
            )
    if breach is not None:
        verdict["breach_concurrency"] = breach["concurrency"]
        verdict["breach_p99_ms"] = breach["p99_ms"]
        verdict["breach_errors"] = breach["errors"]
    verdict["client_bound"] = verdict["state"] == "CLIENT_BOUND"

    return {
        "mode": "staircase",
        "steps": rows,
        "calibration": calibration,
        "verdict": verdict,
    }


def soak(args: argparse.Namespace, target: Target, mix: list[str], meta: dict) -> dict:
    """Hold one load for the soak and sample RSS on a grid."""
    concurrency = args.concurrency
    total = args.soak_minutes * 60.0
    grid = args.sample_seconds
    samples: list[dict] = []
    stop = threading.Event()

    def sampler() -> None:
        next_at = time.monotonic()
        while not stop.is_set():
            now = time.monotonic()
            if now >= next_at:
                samples.append({"t": time.time(), "rss_kb": rss_kb(args.pid)})
                next_at += grid
            stop.wait(0.2)

    sampler_thread = threading.Thread(target=sampler, daemon=True)
    sampler_thread.start()
    rows = []
    started = time.monotonic()
    while time.monotonic() - started < total:
        remaining = total - (time.monotonic() - started)
        row = run_step(target, mix, concurrency, min(60.0, remaining), None)
        rows.append(row)
        print_row(row)
    stop.set()
    sampler_thread.join()

    points = [(s["t"], float(s["rss_kb"])) for s in samples if s["rss_kb"] is not None]
    quarter = (
        points[len(points) - max(2, len(points) // 4) :] if len(points) >= 8 else []
    )
    requests = sum(r["requests"] for r in rows)
    errors = sum(r["errors"] for r in rows)
    result = {
        "mode": "soak",
        "concurrency": concurrency,
        "minutes": args.soak_minutes,
        "requests": requests,
        "errors": errors,
        "p99_ms_worst_minute": max((r["p99_ms"] for r in rows), default=0.0),
        "rss_samples": len(points),
        "rss_kb_first": points[0][1] if points else None,
        "rss_kb_last": points[-1][1] if points else None,
        "rss_slope_kb_per_h": ols_slope_per_hour(points),
        "rss_slope_kb_per_h_final_quarter": ols_slope_per_hour(quarter),
        "samples": samples,
        "minutes_rows": rows,
    }
    print(
        f"soak: {requests} requests, {errors} errors, worst-minute p99 "
        f"{result['p99_ms_worst_minute']} ms; RSS {result['rss_kb_first']} -> "
        f"{result['rss_kb_last']} KB over {len(points)} samples; slope "
        f"{fmt(result['rss_slope_kb_per_h'])} KB/h, final quarter "
        f"{fmt(result['rss_slope_kb_per_h_final_quarter'])} KB/h"
    )
    return result


def fmt(value: float | None) -> str:
    return "n/a" if value is None else f"{value:+.1f}"


def print_row(row: dict, label: str = "step") -> None:
    rss = ""
    if row.get("rss_kb_before") is not None and row.get("rss_kb_after") is not None:
        rss = f"  rss {row['rss_kb_before']}->{row['rss_kb_after']} KB"
    print(
        f"{label:>11} c={row['concurrency']:<4} {row['requests']:>7} req "
        f"{row['rps']:>8} req/s  p50 {row['p50_ms']:>7} ms  p95 {row['p95_ms']:>7} ms  "
        f"p99 {row['p99_ms']:>7} ms  max {row['max_ms']:>7} ms  errors {row['errors']}{rss}"
    )


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://127.0.0.1:8420")
    parser.add_argument("--token", default=os.environ.get("ROCKY_SERVE_TOKEN"))
    parser.add_argument(
        "--pid", type=int, default=None, help="server pid, for RSS readings"
    )
    parser.add_argument(
        "--mix",
        nargs="*",
        default=None,
        help="routes to load (default: the UI's reads)",
    )
    parser.add_argument("--steps", default=DEFAULT_STEPS, help="concurrency ladder")
    parser.add_argument("--step-seconds", type=float, default=20.0)
    parser.add_argument("--p99-ms", type=float, default=250.0, help="latency threshold")
    parser.add_argument(
        "--soak-minutes", type=float, default=None, help="soak mode: hold one load"
    )
    parser.add_argument(
        "--concurrency", type=int, default=16, help="soak mode: the load to hold"
    )
    parser.add_argument(
        "--sample-seconds", type=float, default=30.0, help="soak mode: RSS grid"
    )
    parser.add_argument("--out", default=".ceiling", help="directory for ceiling.json")
    args = parser.parse_args()

    target = Target(args.url, args.token)
    mix = args.mix if args.mix else list(DEFAULT_MIX)
    try:
        mix, meta, failed = warm_up(target, mix)
    except (OSError, http.client.HTTPException, ValueError) as exc:
        print(f"INVALID: cannot reach {args.url}: {exc}", file=sys.stderr)
        return 2
    if failed:
        print(
            "INVALID: routes in the mix did not answer 200 in warm-up:", file=sys.stderr
        )
        for item in failed:
            print(f"  {item}", file=sys.stderr)
        return 2

    record = {
        "host": {
            "platform": platform.platform(),
            "machine": platform.machine(),
            "cpu_count": os.cpu_count(),
            "python": platform.python_version(),
        },
        "server": {
            "url": args.url,
            "engine_version": meta.get("engine_version"),
            "state_schema_version": meta.get("state_schema_version"),
            "pid": args.pid,
        },
        "mix": mix,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    print(
        f"server rocky {record['server']['engine_version']} at {args.url}; mix of {len(mix)} routes"
    )

    if args.soak_minutes:
        record.update(soak(args, target, mix, meta))
        state = "VALID"
    else:
        record.update(staircase(args, target, mix, meta))
        state = record["verdict"]["state"]
        print(f"verdict: {state}: {record['verdict']['reason']}")

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "ceiling.json"
    out_path.write_text(json.dumps(record, indent=2) + "\n")
    print(f"wrote {out_path}")
    return 2 if state == "CLIENT_BOUND" else 0


if __name__ == "__main__":
    sys.exit(main())
