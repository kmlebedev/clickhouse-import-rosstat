#!/usr/bin/env python3
"""Недельный агрегат ИПЦ 2009+: аудит источников и стыков (#396, Task 1).

Пункт `key-rate/phase-4-cpi-v4-weekly-aggregate-core`, роль ML. Спека
`docs/superpowers/specs/2026-10-28-cpi-v4-weekly-aggregate-core-design.md` §1
(«Не проверено») и план Task 1. Модуль отвечает на три вопроса:

1. непрерывен ли ряд «Индекс потребительских цен (недельная оценка)» в
   `v_rosstat_macro` с 2009-04 и нет ли дублей/расхождений на стыках трёх
   источников (бюллетени 2009–2021, BI 2019+, XLSX 2022+);
2. сколько товарных недельных рядов лежит в витрине по годам (состав
   `wk_share_rising` в выборке `long` до 2022);
3. какие интервалы между датами недельных точек (семантика даты — конец
   недели) и как неделя на границе месяцев раскладывается по дням.

Скрипт только читает ClickHouse (`lib.clickhouse.ch_query`), в БД не пишет.
Признаки `wkagg_*` — Task 2 и живут в этом же модуле позже.

Запуск — `make cpi-weekly-aggregate ARGS="--out-dir build/cpi-v4"`,
тесты — `make cpi-weekly-aggregate-test`.
"""

import argparse
import json
import math
import sys
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

WEEKLY_AGGREGATE = "Индекс потребительских цен (недельная оценка)"
SEAM_TOLERANCE_PP = 0.05
SEAM_MAX_OFFSET_DAYS = 3

# Источники агрегата: (метка, SQL). Чтение — FINAL, чтобы не видеть дубли
# ReplacingMergeTree; дубли самой витрины ищутся отдельно по `VIEW_SQL`.
_SERIES = WEEKLY_AGGREGATE.replace("'", "''")
SOURCE_SQL = {
    "bulletins": (
        "SELECT date, toFloat64(percent) AS value FROM rosstat_ipc_weeks_bulletins FINAL "
        f"WHERE name = '{_SERIES}' ORDER BY date FORMAT JSON"
    ),
    "bi": (
        "SELECT date, value FROM rosstat_bi_cpi_weekly FINAL "
        f"WHERE series = '{_SERIES}' ORDER BY date FORMAT JSON"
    ),
    "xlsx": (
        "SELECT date, toFloat64(percent) AS value FROM rosstat_ipc_weeks FINAL "
        f"WHERE name = '{_SERIES}' ORDER BY date FORMAT JSON"
    ),
}
VIEW_SQL = (
    "SELECT date, value FROM v_rosstat_macro "
    f"WHERE series = '{_SERIES}' ORDER BY date FORMAT JSON"
)
PRODUCTS_BY_YEAR_SQL = f"""
SELECT toYear(date) AS year, uniqExact(series) AS n_series, count() AS n_points
FROM v_rosstat_macro
WHERE series IN (
    SELECT series FROM v_series_catalog WHERE source = 'rosstat' AND frequency = 'W'
) AND series != '{_SERIES}'
GROUP BY year ORDER BY year
FORMAT JSON
"""


# ---------------------------------------------------------------------------
# Чистые функции
# ---------------------------------------------------------------------------


def _month_start(day: date) -> date:
    return day.replace(day=1)


def _next_month(month: date) -> date:
    return (month.replace(day=28) + timedelta(days=4)).replace(day=1)


def split_week_by_month(week_end: date, length_days: int, index: float) -> list:
    """Раскладывает недельный индекс по месяцам пропорционально дням (лог-шкала).

    Дата точки — конец периода; период — `length_days` дней, оканчивающихся
    `week_end`. Возвращает `[(первое число месяца, индекс части)]` по
    возрастанию; произведение `part/100` равно `index/100`.
    """
    if length_days <= 0:
        raise ValueError(f"длина периода должна быть > 0: {length_days}")
    if index <= 0:
        raise ValueError(f"индекс должен быть > 0: {index}")
    days_by_month = Counter()
    for offset in range(length_days):
        days_by_month[_month_start(week_end - timedelta(days=offset))] += 1
    if len(days_by_month) == 1:
        return [(next(iter(days_by_month)), index)]
    log_index = math.log(index / 100.0)
    return [
        (month, 100.0 * math.exp(log_index * days_by_month[month] / length_days))
        for month in sorted(days_by_month)
    ]


def find_duplicate_dates(rows: list) -> dict:
    """`{дата: [значения]}` для дат, у которых больше одной строки."""
    by_date = {}
    for day, value in rows:
        by_date.setdefault(day, []).append(value)
    return {day: values for day, values in sorted(by_date.items()) if len(values) > 1}


def interval_histogram(dates: list) -> dict:
    """`{длина интервала в днях: число}` между соседними уникальными датами."""
    unique = sorted(set(dates))
    gaps = Counter((later - earlier).days for earlier, later in zip(unique, unique[1:]))
    return dict(sorted(gaps.items()))


def long_gaps(dates: list, min_days: int = 15) -> list:
    """`[(до, после, дней)]` для интервалов не короче `min_days` (дыры ряда)."""
    unique = sorted(set(dates))
    return [
        (earlier, later, (later - earlier).days)
        for earlier, later in zip(unique, unique[1:])
        if (later - earlier).days >= min_days
    ]


def weekday_histogram(dates: list) -> dict:
    """`{день недели (пн=0): число}` — семантика даты недельной точки."""
    return dict(sorted(Counter(day.weekday() for day in set(dates)).items()))


def coverage_by_year(dates: list) -> dict:
    """`{год: число уникальных дат}`."""
    by_year = Counter(day.year for day in set(dates))
    return dict(sorted(by_year.items()))


def seam_report(
    a: dict,
    b: dict,
    tol_pp: float = SEAM_TOLERANCE_PP,
    max_offset: int = SEAM_MAX_OFFSET_DAYS,
) -> dict:
    """Сверка двух источников на пересечении.

    `n_common` — точные совпадения дат; `over_tol` — совпавшие даты, где
    значения расходятся больше `tol_pp`; `offset_pairs` — даты, различающиеся
    на 1…`max_offset` дней: их дедуп витрины по `(series, date)` не склеит, и
    та же неделя может попасть в витрину дважды.
    """
    common = sorted(set(a) & set(b))
    diffs = [abs(a[day] - b[day]) for day in common]
    over_tol = [(day, a[day], b[day]) for day in common if abs(a[day] - b[day]) > tol_pp]
    offset_pairs = []
    for day_a in sorted(a):
        if day_a in b:
            continue
        for shift in range(1, max_offset + 1):
            for day_b in (day_a - timedelta(days=shift), day_a + timedelta(days=shift)):
                if day_b in b and day_b not in a:
                    offset_pairs.append((day_a, day_b))
    offset_pairs = sorted(set(offset_pairs))
    return {
        "n_common": len(common),
        "max_abs_diff_pp": max(diffs) if diffs else float("nan"),
        "over_tol": over_tol,
        "offset_pairs": offset_pairs,
    }


# ---------------------------------------------------------------------------
# Аудит на ClickHouse (только чтение)
# ---------------------------------------------------------------------------


def _load(sql: str) -> list:
    from lib.clickhouse import ch_query

    return [(date.fromisoformat(str(row["date"])[:10]), float(row["value"])) for row in ch_query(sql)]


def _source_summary(rows: list) -> dict:
    dates = [day for day, _ in rows]
    return {
        "n": len(rows),
        "first": min(dates).isoformat() if dates else None,
        "last": max(dates).isoformat() if dates else None,
        "by_year": coverage_by_year(dates),
        "intervals": interval_histogram(dates),
        "weekdays": weekday_histogram(dates),
        "long_gaps": [(a.isoformat(), b.isoformat(), n) for a, b, n in long_gaps(dates)],
        "duplicates": {day.isoformat(): values for day, values in find_duplicate_dates(rows).items()},
    }


def run_audit() -> dict:
    from lib.clickhouse import ch_query

    sources = {name: _load(sql) for name, sql in SOURCE_SQL.items()}
    view_rows = _load(VIEW_SQL)
    maps = {name: dict(rows) for name, rows in sources.items()}
    seams = {}
    for left, right in (("bulletins", "bi"), ("bi", "xlsx"), ("bulletins", "xlsx")):
        report = seam_report(maps[left], maps[right])
        seams[f"{left}|{right}"] = {
            "n_common": report["n_common"],
            "max_abs_diff_pp": report["max_abs_diff_pp"],
            "over_tol": [(d.isoformat(), x, y) for d, x, y in report["over_tol"]],
            "offset_pairs": [(x.isoformat(), y.isoformat()) for x, y in report["offset_pairs"]],
        }
    view_dates = [day for day, _ in view_rows]
    products = [
        {"year": int(row["year"]), "n_series": int(row["n_series"]), "n_points": int(row["n_points"])}
        for row in ch_query(PRODUCTS_BY_YEAR_SQL)
    ]
    return {
        "series": WEEKLY_AGGREGATE,
        "tolerance_pp": SEAM_TOLERANCE_PP,
        "view": _source_summary(view_rows),
        "view_first_ge_2009_04_06": bool(view_dates) and min(view_dates) <= date(2009, 4, 6),
        "sources": {name: _source_summary(rows) for name, rows in sources.items()},
        "seams": seams,
        "products_by_year": products,
    }


def write_report(path: Path, audit: dict) -> None:
    lines = [
        "# Аудит недельного агрегата ИПЦ 2009+ (#396, Task 1)",
        "",
        f"Ряд: «{audit['series']}». Допуск на стыке: {audit['tolerance_pp']} п.п.",
        "",
        "## Витрина `v_rosstat_macro`",
        "",
        f"- точек {audit['view']['n']}, {audit['view']['first']} … {audit['view']['last']};",
        f"- дубли дат: {len(audit['view']['duplicates'])};",
        f"- интервалы (дней: число): {audit['view']['intervals']};",
        f"- дни недели (пн=0): {audit['view']['weekdays']};",
        f"- дыры ≥ 15 дней (до, после, дней): {audit['view']['long_gaps']}.",
        "",
        "## Источники",
        "",
        "| источник | точек | с | по | дублей |",
        "|---|---:|---|---|---:|",
    ]
    for name, item in audit["sources"].items():
        lines.append(f"| {name} | {item['n']} | {item['first']} | {item['last']} | {len(item['duplicates'])} |")
    lines += ["", "## Стыки", "", "| пара | совпавших дат | max Δ, п.п. | > допуска | сдвиг 1–3 дня |", "|---|---:|---:|---:|---:|"]
    for name, item in audit["seams"].items():
        lines.append(
            f"| {name} | {item['n_common']} | {item['max_abs_diff_pp']:.4f} | "
            f"{len(item['over_tol'])} | {len(item['offset_pairs'])} |"
        )
    lines += ["", "## Покрытие по годам (витрина)", "", "| год | дат |", "|---|---:|"]
    for year, count in audit["view"]["by_year"].items():
        lines.append(f"| {year} | {count} |")
    lines += ["", "## Товарные ряды по годам (витрина)", "", "| год | рядов | точек |", "|---|---:|---:|"]
    for item in audit["products_by_year"]:
        lines.append(f"| {item['year']} | {item['n_series']} | {item['n_points']} |")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out-dir", default="build/cpi-v4", type=Path)
    args = parser.parse_args(argv)
    audit = run_audit()
    out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "weekly_aggregate_audit.json").write_text(
        json.dumps(audit, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n",
        encoding="utf-8",
    )
    write_report(out_dir / "weekly_aggregate_audit.md", audit)
    print(f"Аудит записан в {out_dir}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
