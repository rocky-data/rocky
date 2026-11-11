#!/usr/bin/env python3
"""Assertions for the open-stack acceptance journey.

Each subcommand reads one JSON output that run.sh saved, checks the facts
the journey expects, prints one line per fact, and exits 1 on the first
failed step with every problem it found. Python 3.8+, standard library only.

    check.py load    FILE ROWS SEQ
    check.py run     FILE STEP           STEP is one of: first, quiet, late
    check.py status  FILE STEP           STEP is one of: first, late
    check.py cost    FILE RUN1_ID
    check.py api-cost   API_FILE CLI_FILE
    check.py api-product API_FILE STATUS_FILE
    check.py api-schedule FILE
    check.py tick    FILE
    check.py hook    FILE
"""

import json
import sys

PRODUCT = "events_by_type"
SOURCE_TABLE = "poc.main.events"

problems = []


def expect(cond, message):
    if not cond:
        problems.append(message)
    return cond


def load_json(path):
    with open(path) as fh:
        return json.load(fh)


def finish(summary):
    if problems:
        print("FAIL: " + "; ".join(problems))
        sys.exit(1)
    for line in summary:
        print("    " + line)


def one(items, what):
    expect(len(items) == 1, "expected exactly one %s, found %d" % (what, len(items)))
    return items[0] if items else {}


# ---------------------------------------------------------------- load


def check_load(path, rows, seq):
    out = load_json(path)
    c = out.get("connector", {})
    expect(c.get("status") == "loaded", "status is %r (error: %r)" % (c.get("status"), c.get("error")))
    expect(out.get("total_rows") == rows, "total_rows is %r, expected %d" % (out.get("total_rows"), rows))
    expect(c.get("commit_seq_after") == seq, "commit_seq_after is %r, expected %d" % (c.get("commit_seq_after"), seq))
    finish(["status=loaded total_rows=%s commit_seq %s -> %s"
            % (out.get("total_rows"), c.get("commit_seq_before"), c.get("commit_seq_after"))])


# ----------------------------------------------------------------- run


def output_volume(out):
    mats = [m for m in out.get("materializations", [])
            if (m.get("asset_key") or [None])[-1] == PRODUCT]
    mat = one(mats, "materialization of %s" % PRODUCT)
    return mat.get("volume") or {}


def check_run(path, step):
    out = load_json(path)
    lines = []
    expect(out.get("status") == "Success", "run status is %r, errors: %r" % (out.get("status"), out.get("errors")))

    # Source range: counted when the run starts.
    src = one([s for s in out.get("source_volumes", []) if s.get("table") == SOURCE_TABLE],
              "source_volumes row for %s" % SOURCE_TABLE)
    want_src = {"first": (19, "in_range"), "quiet": (19, "in_range"), "late": (24, "above_max")}[step]
    expect(src.get("product") == PRODUCT, "source range product is %r" % src.get("product"))
    expect(src.get("row_count") == want_src[0], "source row_count is %r, expected %d" % (src.get("row_count"), want_src[0]))
    expect(src.get("verdict") == want_src[1], "source verdict is %r, expected %s" % (src.get("verdict"), want_src[1]))
    warnings = out.get("volume_warnings", [])
    if want_src[1] == "above_max":
        expect(any(SOURCE_TABLE in w for w in warnings), "no volume_warnings line names %s: %r" % (SOURCE_TABLE, warnings))
    else:
        expect(not warnings, "unexpected volume_warnings: %r" % warnings)
    lines.append("source range: %s rows=%s (%s)%s" % (
        SOURCE_TABLE, src.get("row_count"), src.get("verdict"),
        ", warning line printed" if warnings else ""))

    # Output range: counted after the build.
    vol = output_volume(out)
    expect(vol.get("row_count") == 3, "output row_count is %r, expected 3" % vol.get("row_count"))
    expect(vol.get("verdict") == "in_range", "output verdict is %r" % vol.get("verdict"))
    lines.append("output range: %s rows=%s (%s)" % (PRODUCT, vol.get("row_count"), vol.get("verdict")))

    # Budget: checked at the end of every production run.
    pb = one([b for b in out.get("product_budgets", []) if b.get("product") == PRODUCT],
             "product_budgets entry for %s" % PRODUCT)
    verdict = (pb.get("budget") or {}).get("verdict")
    expect(verdict == "breached", "budget verdict is %r, expected breached (%r)" % (verdict, pb.get("budget")))
    event = pb.get("breach_event")
    if step == "first":
        expect(bool(event), "the first run reports no breach_event")
    else:
        expect(not event, "a run after the first reports a new breach_event: %r" % event)
    lines.append("budget: %s%s" % (verdict, ", breach event at this run" if event else ", no new event"))
    finish(lines)


# -------------------------------------------------------------- status


def check_status(path, step):
    out = load_json(path)
    lines = []
    d = out.get("delivery") or {}
    expect(bool(d), "status has no delivery block")
    expect(d.get("deliver_by") == "* * * * *", "deliver_by is %r" % d.get("deliver_by"))
    est = d.get("estimate") or {}
    if step == "first":
        expect(d.get("on_time") is True, "on_time is %r, expected true" % d.get("on_time"))
        expect(d.get("late_by_seconds") in (0, None), "late_by_seconds is %r, expected 0" % d.get("late_by_seconds"))
        expect(est.get("state") == "unknown", "estimate.state is %r with one run" % est.get("state"))
        expect(d.get("at_risk") is None, "at_risk is %r with no estimate, expected null" % d.get("at_risk"))
    else:
        expect(d.get("on_time") is False, "on_time is %r, expected false" % d.get("on_time"))
        late = d.get("late_by_seconds")
        expect(isinstance(late, int) and late > 0, "late_by_seconds is %r, expected > 0" % late)
        expect(est.get("state") == "known", "estimate.state is %r after %r runs" % (est.get("state"), est.get("n")))
        expect(isinstance(d.get("at_risk"), bool), "at_risk is %r, expected a boolean" % d.get("at_risk"))
    for key in ("last_due_at", "last_delivered_at", "next_due_at"):
        expect(bool(d.get(key)), "delivery.%s is missing" % key)
    lines.append("delivery: on_time=%s late_by_seconds=%s at_risk=%s estimate=%s (n=%s)" % (
        d.get("on_time"), d.get("late_by_seconds"), d.get("at_risk"), est.get("state"), est.get("n")))

    vol = out.get("volume") or {}
    expect(vol.get("verdict") == "in_range" and vol.get("row_count") == 3,
           "status volume is %r" % vol)
    src = one([s for s in out.get("source_volumes", []) if s.get("table") == SOURCE_TABLE],
              "status source_volumes row")
    want = "in_range" if step == "first" else "above_max"
    expect(src.get("verdict") == want, "status source verdict is %r, expected %s" % (src.get("verdict"), want))
    lines.append("volume: output %s, source %s" % (vol.get("verdict"), src.get("verdict")))

    cm = out.get("cost_month") or {}
    expect(bool(cm), "status has no cost_month (error: %r)" % out.get("cost_month_error"))
    budget = ((cm.get("cost") or {}).get("budget") or {}).get("verdict")
    expect(budget == "breached", "cost_month budget verdict is %r" % budget)
    lines.append("cost_month: %s budget %s" % (cm.get("month"), budget))
    finish(lines)


# ---------------------------------------------------------------- cost


def product_row(out):
    return one([p for p in out.get("products", []) if p.get("product") == PRODUCT],
               "cost row for %s" % PRODUCT)


def units(entries):
    """[{"unit": u, "amount": n}, ...] -> {u: n}."""
    return {e.get("unit"): e.get("amount") for e in entries or []}


def check_cost(path, run1_id):
    out = load_json(path)
    lines = []
    expect(out.get("history_complete") is True, "history_complete is %r" % out.get("history_complete"))
    row = product_row(out)
    charged = units(row.get("charged"))
    compute = charged.get("compute_ms")
    expect(isinstance(compute, int) and compute > 0, "charged compute_ms is %r, expected > 0" % compute)
    expect(row.get("money_status") == "priced", "money_status is %r, expected priced" % row.get("money_status"))
    # [cost] compute_cost_per_machine_hour = 3.6 is $0.000001 per millisecond.
    parts = {p.get("figure"): p for p in row.get("money_parts") or []}
    usd = (parts.get("compute") or {}).get("usd")
    expect(isinstance(usd, (int, float)) and isinstance(compute, int) and abs(usd - compute * 1e-6) < 1e-12,
           "compute money is %r, expected compute_ms x $0.000001 = %r" % (usd, (compute or 0) * 1e-6))
    for figure in ("egress", "ai_tokens"):
        expect((parts.get(figure) or {}).get("status") == "not_measured",
               "%s money status is %r, expected not_measured" % (figure, (parts.get(figure) or {}).get("status")))
    lines.append("charged: compute_ms=%s, money $%.6f (%s), compute priced at the [cost] rate"
                 % (compute, row.get("money") or 0, row.get("money_status")))

    budget = row.get("budget") or {}
    expect(budget.get("verdict") == "breached", "budget verdict is %r" % budget.get("verdict"))
    checks = [c for c in budget.get("checks", []) if c.get("limit_type") == "max_compute_hours"]
    check = one(checks, "max_compute_hours check")
    expect(check.get("verdict") == "breached", "max_compute_hours check is %r" % check)
    events = row.get("breach_events") or []
    ev = one(events, "breach event")
    expect(ev.get("run_id") == run1_id, "breach event run_id is %r, expected the first run %r" % (ev.get("run_id"), run1_id))
    lines.append("budget: %s, %d breach event, at run %s" % (budget.get("verdict"), len(events), ev.get("run_id")))

    # The identity the command promises: products' charged + unallocated = total.
    total = units(out.get("measured_total"))
    unalloc = units(out.get("unallocated"))
    for unit, amount in total.items():
        summed = sum(units(p.get("charged")).get(unit, 0) for p in out.get("products", []))
        expect(summed + unalloc.get(unit, 0) == amount,
               "%s: products %d + unallocated %d != measured_total %d" % (unit, summed, unalloc.get(unit, 0), amount))
    expect(bool(total), "measured_total is empty")
    lines.append("identity: charged + unallocated == measured_total for %s" % ", ".join(sorted(total)))
    finish(lines)


# ----------------------------------------------------------------- API


def strip_volatile(doc):
    doc = dict(doc)
    doc.pop("as_of", None)
    return doc


def check_api_cost(api_path, cli_path):
    api = load_json(api_path)
    cli = load_json(cli_path)
    expect(api.get("command") == cli.get("command"), "command differs: %r vs %r" % (api.get("command"), cli.get("command")))
    expect(api.get("month") == cli.get("month"), "month differs: %r vs %r" % (api.get("month"), cli.get("month")))
    a, c = product_row(api), product_row(cli)
    # Storage byte-hours of the current month accrue up to now, so they grow
    # between the two calls. Model time does not.
    for key in ("charged", "kept", "built"):
        expect(units(a.get(key)).get("compute_ms") == units(c.get(key)).get("compute_ms"),
               "products[%s].%s compute_ms differs between the API and the CLI" % (PRODUCT, key))
    for key in ("money_status", "breach_events"):
        expect(a.get(key) == c.get(key), "products[%s].%s differs between the API and the CLI" % (PRODUCT, key))
    expect((a.get("budget") or {}).get("verdict") == (c.get("budget") or {}).get("verdict"),
           "products[%s].budget.verdict differs between the API and the CLI" % PRODUCT)
    finish(["GET /api/v1/cost/month?month=%s matches rocky cost --month --by product "
            "(compute charged, kept and built; budget verdict; breach events)" % api.get("month")])


def check_api_product(api_path, status_path):
    api = load_json(api_path)
    st = load_json(status_path)
    for key in ("delivery", "volume", "source_volumes"):
        a, s = api.get(key), st.get(key)
        if key == "delivery" and a and s:
            # next_due_at and at_risk move with the clock; compare the facts.
            a = {k: v for k, v in a.items() if k not in ("next_due_at", "at_risk")}
            s = {k: v for k, v in s.items() if k not in ("next_due_at", "at_risk")}
        expect(a == s, "GET /api/v1/products/%s %s differs from rocky product status" % (PRODUCT, key))
    budget = (((api.get("cost_month") or {}).get("cost") or {}).get("budget") or {}).get("verdict")
    expect(budget == "breached", "API cost_month budget verdict is %r" % budget)
    finish(["GET /api/v1/products/%s matches rocky product status (delivery, volume, source_volumes); budget %s"
            % (PRODUCT, budget)])


def check_api_schedule(path):
    out = load_json(path)
    pipes = out.get("pipelines") or []
    p = one([x for x in pipes if x.get("pipeline") == "events" or x.get("name") == "events"],
            "schedule entry for pipeline events")
    d = p.get("delivery") or {}
    expect(d.get("product") == PRODUCT, "schedule delivery product is %r" % d.get("product"))
    expect(d.get("on_time") is False, "schedule delivery on_time is %r, expected false" % d.get("on_time"))
    ls = p.get("latest_start") or {}
    expect(bool(ls), "pipeline events has no latest_start")
    expect(ls.get("product") == PRODUCT, "latest_start product is %r" % ls.get("product"))
    # A deadline every minute and a cron at 23:59: the scheduler must warn.
    expect(ls.get("state") == "known", "latest_start state is %r (%r)" % (ls.get("state"), ls.get("reasons")))
    expect(ls.get("cron_check") == "too_late" and ls.get("cron_too_late") is True,
           "cron_check is %r, cron_too_late %r; expected too_late, true" % (ls.get("cron_check"), ls.get("cron_too_late")))
    expect(bool(ls.get("warning")), "latest_start has no warning")
    finish(["GET /api/v1/schedule: pipeline events delivers %s, on_time=%s, latest_start state=%s cron_check=%s"
            % (d.get("product"), d.get("on_time"), ls.get("state"), ls.get("cron_check"))])


# ---------------------------------------------------------------- tick


def check_tick(path):
    out = load_json(path)
    ev = one([e for e in out.get("evaluated", []) if e.get("pipeline") == "events"],
             "evaluated entry for pipeline events")
    expect(ev.get("due") is False, "pipeline events is due at a tick: %r" % ev)
    expect(not out.get("executed"), "the tick ran something: %r" % out.get("executed"))
    finish(["tick evaluated pipeline events: not due (cron 59 23 * * *); nothing executed"])


# ---------------------------------------------------------------- hook


def check_hook(path):
    events = []
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    ev = one(events, "on_budget_breach hook call")
    meta = ev.get("metadata") or {}
    expect(ev.get("event") == "budget_breach", "hook event is %r" % ev.get("event"))
    expect(meta.get("scope") == "product_month", "hook scope is %r" % meta.get("scope"))
    expect(meta.get("product") == PRODUCT, "hook product is %r" % meta.get("product"))
    expect(meta.get("limit_type") == "max_compute_hours", "hook limit_type is %r" % meta.get("limit_type"))
    finish(["on_budget_breach fired once: product=%s month=%s limit_type=%s"
            % (meta.get("product"), meta.get("month"), meta.get("limit_type"))])


def main(argv):
    cmd, args = argv[1], argv[2:]
    if cmd == "load":
        check_load(args[0], int(args[1]), int(args[2]))
    elif cmd == "run":
        check_run(args[0], args[1])
    elif cmd == "status":
        check_status(args[0], args[1])
    elif cmd == "cost":
        check_cost(args[0], args[1])
    elif cmd == "api-cost":
        check_api_cost(args[0], args[1])
    elif cmd == "api-product":
        check_api_product(args[0], args[1])
    elif cmd == "api-schedule":
        check_api_schedule(args[0])
    elif cmd == "tick":
        check_tick(args[0])
    elif cmd == "hook":
        check_hook(args[0])
    else:
        print("unknown check %r" % cmd)
        sys.exit(2)


if __name__ == "__main__":
    main(sys.argv)
