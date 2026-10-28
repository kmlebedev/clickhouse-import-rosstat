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
Task 2 — признаки `wkagg_*` (накопленный индекс месяца, покрытие, последняя
неделя, 4-нед. накопленный, тарифный календарь) и сборка выборок v4
(`S_W22`/`S_L13`/`S_L09`); чистые функции без ClickHouse, загрузчик — отдельно.

Запуск — `make cpi-weekly-aggregate ARGS="--out-dir build/cpi-v4"`,
тесты — `make cpi-weekly-aggregate-test`.
"""

import argparse
import dataclasses
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
# Task 2: признаки wkagg_* (спека §3)
# ---------------------------------------------------------------------------

WKAGG_FEATURES = (
    "wkagg_mtd",
    "wkagg_cov",
    "wkagg_mtd_scaled",
    "wkagg_last",
    "wkagg_4w",
    "tariff_week",
)

# Публикация недельной точки — `date + 2` (среда), как в v3 (`available_from_weekly`).
AVAILABLE_LAG_DAYS = 2
# Допустимая длина периода недельной точки: до 10 дней — период равен интервалу
# между датами; новогодний разрыв (через границу года, до 28 дней) — тоже;
# иной длинный разрыв — пропущенные документы, период считается неделей (7).
PERIOD_MAX_GAP_DAYS = 10
PERIOD_NEW_YEAR_MAX_GAP_DAYS = 28
DEFAULT_PERIOD_DAYS = 7
COV_MIN_FOR_SCALED = 0.5
WINDOW_DAYS = 28
WINDOW_MIN_COVERED_DAYS = 21

# Календарь индексации тарифов ЖКУ — как в `weekly_core_proxy.TARIFF_DATES`
# (спека §3: июль 2022–2025, октябрь 2026) плюс 1 января любого года. До 2022
# других дат не вводим: непроверенный календарь был бы выдумкой, а не данными.
TARIFF_DATES = (
    date(2022, 7, 1),
    date(2023, 7, 1),
    date(2024, 7, 1),
    date(2025, 7, 1),
    date(2026, 10, 1),
)

SAMPLE_BOUNDS_V4 = {
    "S_W22": (date(2022, 1, 1), date(2026, 8, 1)),
    "S_L13": (date(2013, 10, 1), date(2026, 8, 1)),
    "S_L09": (date(2009, 6, 1), date(2026, 8, 1)),
}
# `usd_mm`/`key_rate` стартуют в 2013: на S_L09 их нет у ВСЕХ моделей (спека §3).
EXCLUDED_BASE_FEATURES = {"S_L09": ("usd_mm", "key_rate")}

AGGREGATE_POINTS_SQL = (
    "SELECT date, value FROM v_rosstat_macro "
    f"WHERE series = '{_SERIES}' AND date >= '2009-04-06' ORDER BY date FORMAT JSON"
)


def period_length(day: date, previous) -> int:
    """Длина периода точки в днях по интервалу до предыдущей даты.

    Недельная точка — изменение «к предыдущей дате регистрации цен», поэтому
    при нормальном интервале (≤ 10 дней) период равен ему; новогодний разрыв
    через границу года (≤ 28 дней) — одна точка за весь разрыв; прочий
    длинный разрыв — пропущенные документы, период неизвестен и берётся
    равным неделе (непокрытые дни честно отражает `wkagg_cov`).
    """
    if previous is None:
        return DEFAULT_PERIOD_DAYS
    gap = (day - previous).days
    if gap <= PERIOD_MAX_GAP_DAYS:
        return gap
    if previous.year < day.year and gap <= PERIOD_NEW_YEAR_MAX_GAP_DAYS:
        return gap
    return DEFAULT_PERIOD_DAYS


def _periods(points: list) -> list:
    """`[(начало, конец, индекс, длина)]` по возрастанию дат (дубли дат — последняя)."""
    unique = dict(sorted((day, float(value)) for day, value in points))
    days = sorted(unique)
    out = []
    previous = None
    for day in days:
        length = period_length(day, previous)
        out.append((day - timedelta(days=length - 1), day, unique[day], length))
        previous = day
    return out


def _overlap_days(start: date, end: date, window_start: date, window_end: date) -> int:
    low = max(start, window_start)
    high = min(end, window_end)
    return max(0, (high - low).days + 1)


def _log_and_days(periods: list, window_start: date, window_end: date) -> tuple:
    """`(лог-прирост, покрытые дни)` периодов в окне; индекс делится по дням."""
    log_sum = 0.0
    covered = 0
    for start, end, index, length in periods:
        days = _overlap_days(start, end, window_start, window_end)
        if days:
            log_sum += math.log(index / 100.0) * days / length
            covered += days
    return log_sum, covered


def _has_tariff_day(month: date) -> bool:
    if month.month == 1:
        return True
    return any(day.year == month.year and day.month == month.month for day in TARIFF_DATES)


def wkagg_features(points: list, month: date, cutoff: date) -> dict:
    """Признаки недельного агрегата месяца `M` на `cutoff` (спека §3).

    `points` — `[(дата, индекс)]`, 100 = без изменения. Используются только
    точки с `date + 2 ≤ cutoff`; точка с предшественником считается по его
    дате (он всегда раньше, утечки нет). Недоступный признак — NaN, не 0.
    `tariff_week` — календарный флаг месяца (известен заранее, от данных не
    зависит): месяц содержит дату индексации тарифов или 1 января.
    """
    nan = float("nan")
    result = {name: nan for name in WKAGG_FEATURES}
    result["wkagg_cov"] = 0.0
    result["tariff_week"] = int(_has_tariff_day(month))
    boundary = cutoff - timedelta(days=AVAILABLE_LAG_DAYS)
    periods = [period for period in _periods(points) if period[1] <= boundary]
    if not periods:
        return result
    month_start = month.replace(day=1)
    month_end = (month_start.replace(day=28) + timedelta(days=4)).replace(day=1) - timedelta(days=1)
    days_in_month = month_end.day
    log_sum, covered = _log_and_days(periods, month_start, month_end)
    cov = min(1.0, covered / days_in_month)
    result["wkagg_cov"] = cov
    if covered:
        result["wkagg_mtd"] = (math.exp(log_sum) - 1.0) * 100.0
        if cov >= COV_MIN_FOR_SCALED:
            result["wkagg_mtd_scaled"] = result["wkagg_mtd"] / cov
    result["wkagg_last"] = periods[-1][2] - 100.0
    last_day = periods[-1][1]
    window_start = last_day - timedelta(days=WINDOW_DAYS - 1)
    window_log, window_covered = _log_and_days(periods, window_start, last_day)
    if window_covered >= WINDOW_MIN_COVERED_DAYS:
        result["wkagg_4w"] = (math.exp(window_log) - 1.0) * 100.0
    return result


def _validate_sample(sample: str) -> str:
    if sample not in SAMPLE_BOUNDS_V4:
        raise ValueError(f"неизвестная выборка {sample!r}; доступны {list(SAMPLE_BOUNDS_V4)}")
    return sample


def feature_names_v4(sample: str, with_diffusion: bool = False) -> list:
    """Порядок признаков v4: база v2 (без исключённых для выборки), `wkagg_*`, [diffusion], `month`."""
    _validate_sample(sample)
    from models.cpi import cpi_monthly as cm

    excluded = set(EXCLUDED_BASE_FEATURES.get(sample, ()))
    names = [name for name in cm.feature_names(with_mp=True) if name not in excluded]
    extra = list(WKAGG_FEATURES)
    if with_diffusion:
        from models.cpi import cpi_monthly_v3 as v3

        extra += list(v3.WEEKLY_FEATURES)
    return names[:-1] + extra + [names[-1]]


def attach_wkagg(rows: list, points: list, sample: str) -> list:
    """Новые `Row` с `wkagg_*` на cutoff месяца; исключённые признаки выборки убираются."""
    _validate_sample(sample)
    from models.cpi import cpi_monthly as cm

    excluded = set(EXCLUDED_BASE_FEATURES.get(sample, ()))
    out = []
    for row in rows:
        features = {name: value for name, value in row.features.items() if name not in excluded}
        features.update(wkagg_features(points, row.month, cm.cutoff_for(row.month)))
        out.append(dataclasses.replace(row, features=features))
    return out


def wkagg_summary(rows: list) -> dict:
    """Сводка по строкам выборки: число NaN в `wkagg_*` и месяцев с `cov ≥ 0,5` по годам."""
    by_year = {}
    nan_counts = {name: 0 for name in WKAGG_FEATURES}
    for row in sorted(rows, key=lambda r: r.month):
        for name in WKAGG_FEATURES:
            value = row.features[name]
            if isinstance(value, float) and math.isnan(value):
                nan_counts[name] += 1
        by_year.setdefault(row.month.year, 0)
        if row.features["wkagg_cov"] >= COV_MIN_FOR_SCALED:
            by_year[row.month.year] += 1
    months = [row.month for row in rows]
    return {
        "n_rows": len(rows),
        "first": min(months).isoformat() if months else None,
        "last": max(months).isoformat() if months else None,
        "nan_counts": nan_counts,
        "cov_ge_half_by_year": dict(sorted(by_year.items())),
    }


def load_aggregate_points() -> list:
    """Недельный агрегат из `v_rosstat_macro`: `[(дата, индекс)]` (только чтение)."""
    return _load(AGGREGATE_POINTS_SQL)


def build_sample_v4(sample: str, with_diffusion: bool = False) -> tuple:
    """Строки v4 `([Row], [(месяц, причина)])`: база/МП из `cpi_monthly`, затем `wkagg_*`.

    Границы — `SAMPLE_BOUNDS_V4`; пропуски месяцев — только по обязательным
    признакам v2 (`y_lag*`, `fao_mm`, МП). Отсутствие недельных точек — NaN,
    не пропуск месяца. Требует ClickHouse.
    """
    first, last = SAMPLE_BOUNDS_V4[_validate_sample(sample)]
    from models.cpi import cpi_monthly as cm

    targets = cm.load_targets()
    survey = cm.load_survey()
    usd = cm.load_usd()
    key_rate = cm.load_key_rate()
    fao = cm.load_fao()
    index = cm.target_index(targets)
    panel = None
    if with_diffusion:
        from models.cpi import cpi_monthly_v3 as v3

        panel = v3.weekly_panel(v3.load_weekly())
    rows = []
    skipped = []
    for month in cm.sample_months(targets):
        if month < first or month > last:
            continue
        cutoff = cm.cutoff_for(month)
        base, reason = cm.base_features(month, index, usd, key_rate, fao, cutoff)
        if base is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = cm.mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        features = {**base, **mp_part}
        if panel is not None:
            features.update(v3.weekly_features(panel, cutoff))
        y = {code: values[cm.month_end(month)] for code, values in index.items()}
        rows.append(cm.Row(month=month, y=y, features=features))
    return attach_wkagg(rows, load_aggregate_points(), sample), skipped


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
    parser.add_argument(
        "--samples",
        action="store_true",
        help="сводка wkagg_* по выборкам S_W22/S_L13/S_L09 вместо аудита источников",
    )
    args = parser.parse_args(argv)
    if args.samples:
        out_dir = args.out_dir if args.out_dir.is_absolute() else ROOT / args.out_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        result = {}
        for sample in SAMPLE_BOUNDS_V4:
            rows, skipped = build_sample_v4(sample)
            result[sample] = {**wkagg_summary(rows), "n_skipped": len(skipped)}
            print(sample, json.dumps(result[sample], ensure_ascii=False), flush=True)
        (out_dir / "wkagg_samples_summary.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return 0
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
