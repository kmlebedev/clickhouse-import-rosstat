#!/usr/bin/env python3
"""Arithmetic helpers for the key-rate forecast skill (standard library only).

avg    Annual average key rate implied by ONE path of decisions, checked
       against the Bank of Russia's published range for the year.
bound  Every path over the given meetings and step sizes: how many keep the
       annual average inside the range, and the least and most total change
       among those that do. Use it before writing "at least", "only if" or
       "requires" about a path: a few paths tried with `avg` prove nothing
       about the ones not tried.
paper  Result of one KEYRATE contract per outcome and its expectation, kept
       apart: the result IF the Board holds is not the expected result.
score  Brier scores of a decision forecast against the market-implied
       distribution and the naive "no change" forecast.

All print JSON to stdout.
"""
import argparse
import itertools
import json
import sys
from datetime import date, timedelta


def _d(s):
    return date.fromisoformat(s)


def _walk(rate, through, meetings, year_end, lag):
    """Rate-days from the day after `through` to the end of the year.

    A decision announced on day D takes effect on the following Monday, which
    `lag` approximates (default 3: Friday -> Monday).
    """
    cursor, weighted, segments = through + timedelta(days=1), 0.0, []
    for d, r in meetings:
        eff = d + timedelta(days=lag)
        if eff > year_end:
            break
        n = (eff - cursor).days
        if n > 0:
            weighted += rate * n
            segments.append({"from": str(cursor), "days": n, "rate": rate})
        rate, cursor = r, max(eff, cursor)
    n = (year_end - cursor).days + 1
    if n > 0:
        weighted += rate * n
        segments.append({"from": str(cursor), "days": n, "rate": rate})
    return weighted, rate, segments


def _year_start(a):
    """(year_end, days_total, through, days_ytd, ytd_avg) from the shared options."""
    year_start, year_end = date(a.year, 1, 1), date(a.year, 12, 31)
    days_total = (year_end - year_start).days + 1
    if (a.ytd_avg is None) != (a.ytd_through is None):
        sys.exit("--ytd-avg and --ytd-through go together; omit both for a future year")
    if a.ytd_through is None:
        return year_end, days_total, year_start - timedelta(days=1), 0, 0.0
    through = _d(a.ytd_through)
    if not year_start <= through <= year_end:
        sys.exit("--ytd-through must fall inside --year")
    return year_end, days_total, through, (through - year_start).days + 1, a.ytd_avg


def cmd_bound(a):
    year_end, days_total, through, days_ytd, ytd_avg = _year_start(a)
    dates = sorted(_d(m) for m in a.meeting)
    if dates[0] <= through:
        sys.exit(f"meeting {dates[0]} is not after {through}")
    steps = sorted({int(x) for x in a.steps.split(",")})
    if len(steps) ** len(dates) > 2_000_000:
        sys.exit("too many paths: use fewer step sizes or fewer meetings")
    lo, _, hi = a.range.partition(":")
    lo, hi = float(lo), float(hi)

    inside, total = [], 0
    for combo in itertools.product(steps, repeat=len(dates)):
        total += 1
        rate, meetings = a.rate, []
        for d, st in zip(dates, combo):
            rate = round(rate + st / 100, 4)
            meetings.append((d, rate))
        weighted, end_rate, _ = _walk(a.rate, through, meetings, year_end, a.effective_lag_days)
        annual = (ytd_avg * days_ytd + weighted) / days_total
        if lo - 1e-9 <= round(annual, a.round_to) <= hi + 1e-9:
            inside.append((sum(combo), combo, round(annual, 4), end_rate))

    def show(x):
        return {"total_change_bp": x[0], "annual_avg": x[2], "year_end_rate": x[3],
                "steps_bp": dict(zip(map(str, dates), x[1]))}

    out = {
        "year": a.year, "start_rate": a.rate, "range": [lo, hi],
        "steps_allowed_bp": steps, "meetings": [str(d) for d in dates],
        "paths_checked": total, "paths_inside": len(inside),
    }
    if inside:
        least = min(inside, key=lambda x: (abs(x[0]), x[2]))
        most = min(inside, key=lambda x: (x[0], x[2]))
        out["least_change_inside"] = show(least)
        out["most_easing_inside"] = show(most)
        out["year_end_rate_inside"] = [min(x[3] for x in inside), max(x[3] for x in inside)]
    out["scope"] = ("Exhaustive only for these meetings, these step sizes and the rounding "
                    f"of the average to {a.round_to} decimal(s); quote the claim with those conditions.")
    print(json.dumps(out, ensure_ascii=False, indent=2))


def cmd_paper(a):
    probs = _norm(json.loads(a.probs), "--probs")
    rows, exp_rate = [], 0.0
    for k in sorted(probs, key=int):
        announced = round(a.rate + int(k) / 100, 4)
        exp_rate += probs[k] * announced
        rows.append({"outcome_bp": int(k), "probability": round(probs[k], 4),
                     "announced_rate": announced,
                     "long_rub": round((announced - a.entry) * 1000, 2)})
    gap = round((exp_rate - a.entry) * 100, 2)
    direction = a.direction or ("none" if abs(gap) < a.threshold else ("long" if gap > 0 else "short"))
    sign = {"long": 1, "short": -1, "none": 0}[direction]
    for r in rows:
        r["position_rub"] = round(sign * r["long_rub"], 2) if sign else 0.0
    print(json.dumps({
        "entry": a.entry, "current_rate": a.rate,
        "expected_rate": round(exp_rate, 4), "gap_bp": gap,
        "threshold_bp": a.threshold, "direction": direction,
        "per_outcome": rows,
        "expected_long_rub": round(sum(r["probability"] * r["long_rub"] for r in rows), 2),
        "expected_position_rub": round(sum(r["probability"] * r["position_rub"] for r in rows), 2),
        "note": ("per_outcome is the result IF that outcome happens; expected_* is the "
                 "probability-weighted sum over all outcomes. With direction 'none' no "
                 "position is recorded and its result is zero; long_rub then only shows "
                 "what a hypothetical long would make. Costs and margin ignored."),
    }, ensure_ascii=False, indent=2))


def cmd_avg(a):
    year_end, days_total, through, days_ytd, ytd_avg = _year_start(a)

    meetings = []
    for m in a.meeting or []:
        d, _, r = m.partition(":")
        if not r:
            sys.exit(f"--meeting expects DATE:RATE, got {m!r}")
        meetings.append((_d(d), float(r)))
    meetings.sort()
    for d, _ in meetings:
        if d <= through:
            sys.exit(f"meeting {d} is not after {through}; a decision of the previous "
                     "year belongs in --rate")

    weighted, rate, segments = _walk(a.rate, through, meetings, year_end, a.effective_lag_days)

    days_rest = days_total - days_ytd
    annual = (ytd_avg * days_ytd + weighted) / days_total
    out = {
        "year": a.year,
        "days_ytd": days_ytd,
        "days_rest": days_rest,
        "ytd_avg": a.ytd_avg,
        "rest_avg": round(weighted / days_rest, 4) if days_rest else None,
        "annual_avg": round(annual, 4),
        "year_end_rate": rate,
        "segments": segments,
    }
    if a.range:
        lo, _, hi = a.range.partition(":")
        lo, hi = float(lo), float(hi)
        out["range"] = [lo, hi]
        out["inside_range"] = lo - 1e-9 <= round(annual, 1) <= hi + 1e-9
        if days_rest:
            # Average over the remaining days that would put the year at each
            # edge of the range: the constraint on the remaining decisions.
            out["rest_avg_needed"] = [
                round((x * days_total - ytd_avg * days_ytd) / days_rest, 2)
                for x in (lo, hi)
            ]
    print(json.dumps(out, ensure_ascii=False, indent=2))


def _norm(p, name):
    p = {str(int(float(k))): float(v) for k, v in p.items()}
    s = sum(p.values())
    if s <= 0:
        sys.exit(f"{name}: probabilities sum to zero")
    if abs(s - 1) > 0.02 and abs(s - 100) > 2:
        sys.exit(f"{name}: probabilities sum to {s}, expected 1 or 100")
    return {k: v / s for k, v in p.items()}


def _brier(p, outcome):
    keys = set(p) | {outcome}
    return sum((p.get(k, 0.0) - (1.0 if k == outcome else 0.0)) ** 2 for k in keys)


def cmd_score(a):
    outcome = str(int(a.outcome))
    own = _norm(json.loads(a.probs), "--probs")
    res = {"outcome_bp": int(outcome), "brier": {"own": round(_brier(own, outcome), 4)}}
    naive = _brier({"0": 1.0}, outcome)
    res["brier"]["naive"] = round(naive, 4)
    if a.market:
        res["brier"]["market"] = round(_brier(_norm(json.loads(a.market), "--market"), outcome), 4)
    res["skill"] = {
        k: (round(1 - res["brier"]["own"] / v, 4) if v > 0 else None)
        for k, v in res["brier"].items() if k != "own"
    }
    res["prob_on_outcome"] = round(own.get(outcome, 0.0), 4)
    print(json.dumps(res, ensure_ascii=False, indent=2))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    a = sub.add_parser("avg", help="annual average implied by a path")
    a.add_argument("--year", type=int, required=True)
    a.add_argument("--ytd-avg", type=float,
                   help="calendar-day average rate from 1 Jan through --ytd-through; "
                        "omit with --ytd-through for a year that has not started")
    a.add_argument("--ytd-through", help="YYYY-MM-DD, last day covered by --ytd-avg")
    a.add_argument("--rate", type=float, required=True,
                   help="rate in force after --ytd-through (or on 1 January for a future year)")
    a.add_argument("--meeting", action="append", help="DATE:RATE after that meeting; repeatable")
    a.add_argument("--range", help="LO:HI — the Bank's range for the annual average")
    a.add_argument("--effective-lag-days", type=int, default=3)
    a.set_defaults(fn=cmd_avg)

    b = sub.add_parser("bound", help="all paths over given meetings and step sizes")
    b.add_argument("--year", type=int, required=True)
    b.add_argument("--ytd-avg", type=float)
    b.add_argument("--ytd-through")
    b.add_argument("--rate", type=float, required=True,
                   help="rate in force before the first listed meeting takes effect")
    b.add_argument("--meeting", action="append", required=True, help="DATE; repeatable")
    b.add_argument("--steps", default="-50,-25,0",
                   help="step sizes in bp the Board may take at each meeting (default -50,-25,0)")
    b.add_argument("--range", required=True, help="LO:HI — the Bank's range for the annual average")
    b.add_argument("--round-to", type=int, default=1,
                   help="decimals the Bank publishes the average to (default 1)")
    b.add_argument("--effective-lag-days", type=int, default=3)
    b.set_defaults(fn=cmd_bound)

    pp = sub.add_parser("paper", help="one KEYRATE contract: result per outcome and expectation")
    pp.add_argument("--entry", type=float, required=True, help="settle of the series on the run date")
    pp.add_argument("--rate", type=float, required=True, help="current key rate")
    pp.add_argument("--probs", required=True, help="JSON, step in bp -> probability (the forecast row)")
    pp.add_argument("--direction", choices=["long", "short", "none"],
                    help="override; by default from the gap and --threshold")
    pp.add_argument("--threshold", type=float, default=25.0, help="no-disagreement band in bp")
    pp.set_defaults(fn=cmd_paper)

    s = sub.add_parser("score", help="Brier scores for a resolved meeting")
    s.add_argument("--probs", required=True, help='JSON, step in bp -> probability')
    s.add_argument("--market", help="JSON, market-implied distribution")
    s.add_argument("--outcome", required=True, help="realised step in bp, e.g. -50")
    s.set_defaults(fn=cmd_score)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
