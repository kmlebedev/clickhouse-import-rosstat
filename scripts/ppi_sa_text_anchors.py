#!/usr/bin/env python3
"""Сверка `ppi_sa` с текстовыми якорями 12 выпусков «Инфляции в России».

T3/T4 использовали 10 точных якорей из 5 выпусков (2026-04…2026-08). Этот
скрипт разбирает все 12 выпусков 2025-10…2026-09: точные числа там, где они
есть, и качественные/граничные формулировки («вблизи ноля», «ниже 4%»,
«снижались») — как отдельный, более слабый класс проверок.

Скрипт только читает `ppi_sa` (через `scripts/lib/clickhouse.py`) и пишет CSV/
JSON в `docs/research/`. В ClickHouse не пишет, DDL/GRANT не трогает.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "lib"))

import clickhouse  # noqa: E402

OUT_CSV = ROOT.parent / "docs" / "research" / "2026-10-21-ppi-sa-text-anchors-12.csv"
OUT_JSON = ROOT.parent / "docs" / "research" / "2026-10-21-ppi-sa-text-anchors-12.json"
TOLERANCE_PP = 0.3
# Границы качественных формулировок — предрегистрированы до сверки.
THRESHOLDS = {
    "near_zero": "|x| <= 1.0",
    "near_4": "3.5 <= x <= 4.5",
    "below_4": "x < 4.0",
    "declining": "x < 0",
    "almost_flat": "|x| <= 1.0",
}

# Каждый выпуск: issue/url/target (месяц PPI-окна) и утверждения.
# `kind`: exact | near_zero | near_4 | below_4 | declining | almost_flat |
#         direction_up | not_stated.
ISSUES = [
    {
        "issue": "2025-10",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2025-10/",
        "target": "2025-09-01",
        "window": "в сентябре",
        "statements": [
            {
                "series": "nonfood",
                "kind": "near_zero",
                "text": "Приросты цен производителей непродовольственных товаров без учета бензина и табака находились вблизи ноля (рис. 44).",
            },
            {"series": "food", "kind": "not_stated", "text": "Числовое значение по продовольствию не приведено."},
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2025-11",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2025-11/",
        "target": "2025-10-01",
        "window": "в октябре, в среднем за 3 месяца",
        "statements": [
            {
                "series": "nonfood",
                "kind": "direction_up",
                "text": "Повысились темпы удорожания непродовольственных товаров без бензина и табачных изделий (в среднем за 3 месяца) после околонулевого роста или снижения в предыдущие месяцы.",
                "prev_target": "2025-09-01",
            },
            {
                "series": "food",
                "kind": "below_4",
                "text": "Прирост цен производителей продовольственных товаров без алкоголя, овощей и фруктов (в среднем за 3 месяца) замедлился и опустился ниже 4% (с.к.г.) впервые с февраля 2024 г.",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2025-12",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2025-12/",
        "target": "2025-11-01",
        "window": "сентябрь – ноябрь 2025 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "near_4",
                "text": "В среднем за 3 месяца (сентябрь – ноябрь 2025 г.) цены производителей непродовольственных товаров росли темпами вблизи 4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "declining",
                "text": "…а цены на продовольственные товары снижались (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-1",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-1/",
        "target": "2025-12-01",
        "window": "октябрь – декабрь 2025 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "near_4",
                "text": "В среднем за последние 3 месяца (октябрь – декабрь 2025 г.) цены производителей непродовольственных товаров росли темпом вблизи 4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "declining",
                "text": "…а цены на продовольственные товары снижались (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-2",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-2/",
        "target": "2026-02-01",
        "window": "декабрь 2025 – февраль 2026 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "below_4",
                "text": "В среднем за последние 3 месяца (декабрь 2025 – февраль 2026 г.) цены производителей непродовольственных товаров росли темпами ниже 4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "declining",
                "text": "…а отпускные цены на продовольственные товары снижались (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-3",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-3/",
        "target": "2026-02-01",
        "window": "декабрь 2025 – февраль 2026 г. (как в 2026-2)",
        "statements": [
            {
                "series": "nonfood",
                "kind": "below_4",
                "text": "В среднем за 3 месяца (декабрь 2025 – февраль 2026 г.) цены производителей непродовольственных товаров росли темпами ниже 4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "declining",
                "text": "…а цены на продовольственные товары снижались (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
        "note": "Окно совпадает с выпуском 2026-2; в тексте, вероятно, не обновлён PPI-абзац.",
    },
    {
        "issue": "2026-4",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-4/",
        "target": "2026-03-01",
        "window": "январь – март 2026 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "below_4",
                "text": "В среднем за 3 месяца (январь – март 2026 г.) цены производителей непродовольственных товаров росли темпами ниже 4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "declining",
                "text": "…а цены на продовольственные товары снижались (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-5",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-5/",
        "target": "2026-04-01",
        "window": "февраль – апрель 2026 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "exact",
                "value": 2.9,
                "text": "В среднем за 3 месяца (февраль – апрель 2026 г.) цены производителей непродовольственных товаров росли темпом 2,9% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "exact",
                "value": -4.1,
                "text": "…а цены на продовольственные товары снижались темпом -4,1% (с.к.г.) (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-6",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-6/",
        "target": "2026-05-01",
        "window": "март – май 2026 г.",
        "statements": [
            {
                "series": "core",
                "kind": "exact",
                "value": 2.9,
                "text": "Прирост цен производителей потребительских товаров (без овощей и фруктов, алкоголя, бензина и табака) в среднем за 3 месяца (март – май 2026 г.) немного ускорился и составил 2,9% (с.к.г.).",
            },
            {
                "series": "core",
                "kind": "exact",
                "value": 2.3,
                "target": "2026-04-01",
                "text": "…после 2,3% (с.к.г.) в феврале – апреле 2026 года.",
            },
            {
                "series": "food",
                "kind": "almost_flat",
                "text": "…в то время как прирост цен производителей продовольственных товаров почти не изменился (рис. 44).",
            },
            {"series": "nonfood", "kind": "not_stated", "text": "Числовое значение по непродовольствию не приведено."},
        ],
    },
    {
        "issue": "2026-7",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-7/",
        "target": "2026-06-01",
        "window": "апрель – июнь 2026 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "exact",
                "value": 5.4,
                "text": "В среднем за 3 месяца (апрель – июнь 2026 г.) цены производителей непродовольственных товаров увеличивались темпом 5,4% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "exact",
                "value": 0.7,
                "text": "…а цены производителей продовольственных товаров – темпом 0,7% (с.к.г.) (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-8",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-8/",
        "target": "2026-07-01",
        "window": "май – июль 2026 г.",
        "statements": [
            {
                "series": "nonfood",
                "kind": "exact",
                "value": 3.6,
                "text": "В мае – июле 2026 г. цены производителей непродовольственных товаров росли темпом 3,6% (с.к.г.).",
            },
            {
                "series": "food",
                "kind": "exact",
                "value": 0.8,
                "text": "…а цены производителей продовольственных товаров – темпом 0,8% (с.к.г.) (рис. 44).",
            },
            {"series": "core", "kind": "not_stated", "text": "Числовое значение по всем товарам/core не приведено."},
        ],
    },
    {
        "issue": "2026-9",
        "url": "https://www.cbr.ru/analytics/dkp/dinamic/CPD_2026-9/",
        "target": "2026-08-01",
        "window": "июнь – август 2026 г.",
        "statements": [
            {
                "series": "core",
                "kind": "exact",
                "value": 8.1,
                "text": "В среднем за 3 месяца (июнь – август) он составил 8,1% с.к.г.",
            },
            {
                "series": "food",
                "kind": "exact",
                "value": 9.9,
                "text": "…в том числе 9,9% с.к.г. для продовольственных товаров.",
            },
            {
                "series": "nonfood",
                "kind": "exact",
                "value": 6.5,
                "text": "…и 6,5% для непродовольственных (рис. 44).",
            },
        ],
    },
]


def load_saar3() -> dict[tuple[str, str], float]:
    sql = """SELECT series,
                    target,
                    argMax(saar3, loaded_at) AS saar3
             FROM (
                 SELECT series,
                        toString(toStartOfMonth(date)) AS target,
                        saar3,
                        loaded_at
                 FROM ppi_sa FINAL
                 WHERE series IN ('food','nonfood','core','all') AND saar3 IS NOT NULL
             )
             GROUP BY series, target
             ORDER BY series, target
             FORMAT JSON"""
    rows = clickhouse.ch_query(sql)
    return {(row["series"], row["target"]): float(row["saar3"]) for row in rows}


def evaluate(kind: str, value: float, prev: float | None = None) -> tuple[str, str]:
    if kind == "not_stated":
        return "n/a", ""
    if kind == "exact":
        return "", ""
    if kind == "near_zero":
        ok = abs(value) <= 1.0
    elif kind == "near_4":
        ok = 3.5 <= value <= 4.5
    elif kind == "below_4":
        ok = value < 4.0
    elif kind == "declining":
        ok = value < 0
    elif kind == "almost_flat":
        ok = abs(value) <= 1.0
    elif kind == "direction_up":
        if prev is None:
            return "n/a", "нет предыдущей точки"
        ok = value > prev
    else:
        raise ValueError(f"неизвестный kind: {kind}")
    return ("pass" if ok else "fail"), THRESHOLDS.get(kind, "")


def build_rows(saar3: dict[tuple[str, str], float]) -> list[dict]:
    rows: list[dict] = []
    for issue in ISSUES:
        for st in issue["statements"]:
            target = st.get("target", issue["target"])
            ours = saar3.get((st["series"], target))
            if ours is None:
                raise ValueError(f"нет saar3 для {st['series']} {target}")
            prev = None
            if st["kind"] == "direction_up":
                prev = saar3.get((st["series"], st["prev_target"]))
            result, threshold = evaluate(st["kind"], ours, prev)
            row = {
                "issue": issue["issue"],
                "url": issue["url"],
                "target": target,
                "window": issue["window"],
                "series": st["series"],
                "kind": st["kind"],
                "text": st["text"],
                "anchor_value": st.get("value", ""),
                "ours": ours,
                "diff": "" if st["kind"] != "exact" else ours - st["value"],
                "threshold": threshold,
                "result": result,
                "note": issue.get("note", ""),
            }
            rows.append(row)
    return rows


def stats(rows: list[dict]) -> dict:
    if not rows:
        return {"n": 0}
    diffs = [r["diff"] for r in rows]
    anchors = [r["anchor_value"] for r in rows]
    ours = [r["ours"] for r in rows]
    corr = None
    if len(rows) > 1:
        ma = sum(anchors) / len(anchors)
        mo = sum(ours) / len(ours)
        cov = sum((a - ma) * (o - mo) for a, o in zip(anchors, ours))
        va = sum((a - ma) ** 2 for a in anchors)
        vo = sum((o - mo) ** 2 for o in ours)
        if va and vo:
            corr = cov / math.sqrt(va * vo)
    return {
        "n": len(rows),
        "mae": sum(abs(d) for d in diffs) / len(diffs),
        "max_abs": max(abs(d) for d in diffs),
        "bias": sum(diffs) / len(diffs),
        "corr": corr,
    }


def build_report(rows: list[dict]) -> dict:
    exact = [r for r in rows if r["kind"] == "exact"]
    qualitative = [r for r in rows if r["kind"] not in ("exact", "not_stated")]
    by_series = {s: stats([r for r in exact if r["series"] == s]) for s in ("food", "nonfood", "core")}
    summary = {"pass": 0, "fail": 0, "n/a": 0}
    for r in qualitative:
        summary[r["result"]] = summary.get(r["result"], 0) + 1
    issues = []
    for issue in ISSUES:
        issue_rows = [r for r in rows if r["issue"] == issue["issue"]]
        issues.append(
            {
                "issue": issue["issue"],
                "url": issue["url"],
                "target": issue["target"],
                "window": issue["window"],
                "exact": sum(1 for r in issue_rows if r["kind"] == "exact"),
                "qualitative": sum(1 for r in issue_rows if r["kind"] not in ("exact", "not_stated")),
                "note": issue.get("note", ""),
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tolerance_pp": TOLERANCE_PP,
        "thresholds": THRESHOLDS,
        "exact": {"rows": exact, "by_series": by_series, "overall": stats(exact)},
        "qualitative": {"rows": qualitative, "summary": summary},
        "issues": issues,
    }


def write_csv(rows: list[dict]) -> None:
    fields = [
        "issue", "url", "target", "window", "series", "kind", "text",
        "anchor_value", "ours", "diff", "threshold", "result", "note",
    ]
    with OUT_CSV.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_json(report: dict) -> None:
    OUT_JSON.write_text(json.dumps(report, ensure_ascii=False, indent=1) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--print", action="store_true", help="напечатать сводку")
    args = parser.parse_args()
    saar3 = load_saar3()
    rows = build_rows(saar3)
    report = build_report(rows)
    write_csv(rows)
    write_json(report)
    if args.print:
        print(json.dumps(report["exact"]["overall"], ensure_ascii=False))
        print(json.dumps(report["exact"]["by_series"], ensure_ascii=False))
        print(json.dumps(report["qualitative"]["summary"], ensure_ascii=False))
    print(f"written {OUT_CSV} ({len(rows)} rows) and {OUT_JSON}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
