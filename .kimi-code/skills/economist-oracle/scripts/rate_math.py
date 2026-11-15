#!/usr/bin/env python3
"""Arithmetic helpers for the key-rate forecast skill (standard library only).

avg    Annual average key rate implied by a path of decisions, checked against
       the Bank of Russia's published range for the year.
score  Brier scores of a decision forecast against the market-implied
       distribution and the naive "no change" forecast.

Both print JSON to stdout.
"""
import argparse
import json
import sys
from datetime import date, timedelta


def _d(s):
    return date.fromisoformat(s)


def cmd_avg(a):
    year_start, year_end = date(a.year, 1, 1), date(a.year, 12, 31)
    days_total = (year_end - year_start).days + 1
    if (a.ytd_avg is None) != (a.ytd_through is None):
        sys.exit("--ytd-avg and --ytd-through go together; omit both for a future year")
    if a.ytd_through is None:
        # A year that has not started: --rate is the rate in force on 1 January
        # (the last decision of the previous year, already effective).
        through, days_ytd, ytd_avg = year_start - timedelta(days=1), 0, 0.0
    else:
        through, ytd_avg = _d(a.ytd_through), a.ytd_avg
        if not year_start <= through <= year_end:
            sys.exit("--ytd-through must fall inside --year")
        days_ytd = (through - year_start).days + 1

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

    # A decision announced on day D applies from the next calendar day; the
    # Bank's rate changes take effect on the following Monday, which the
    # --effective-lag-days option approximates (default 3: Friday -> Monday).
    rate, cursor, weighted = a.rate, through + timedelta(days=1), 0.0
    segments = []
    for d, r in meetings:
        eff = d + timedelta(days=a.effective_lag_days)
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

    s = sub.add_parser("score", help="Brier scores for a resolved meeting")
    s.add_argument("--probs", required=True, help='JSON, step in bp -> probability')
    s.add_argument("--market", help="JSON, market-implied distribution")
    s.add_argument("--outcome", required=True, help="realised step in bp, e.g. -50")
    s.set_defaults(fn=cmd_score)

    args = p.parse_args()
    args.fn(args)


if __name__ == "__main__":
    main()
