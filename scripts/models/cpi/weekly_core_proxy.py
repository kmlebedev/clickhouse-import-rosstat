#!/usr/bin/env python3
"""Недельный прокси базовой инфляции: сезонное сглаживание 4-нед. средней.

Пункт `key-rate/phase-4-weekly-core-proxy` (issue #348), роль research.
Вопрос: можно ли из недельной 4-недельной средней (панель 36) получить
оценку, сопоставимую с месячным «Базовым ИПЦ» ЦБ, и какая корректировка —
календарная регрессия, STL с недельным периодом или X-13 — даёт меньшую
ошибку на месячной сетке 2022-01 … 2026-09.

Скрипт только читает ClickHouse (`v_rosstat_macro`, `v_series_catalog`,
`v_cbr_macro`) и пишет локальный JSON; в БД не пишет. Артефакт —
`docs/research/2026-10-22-weekly-core-proxy.json`, отчёт —
`docs/research/2026-10-22-weekly-core-proxy.md`.

Методы и критерий предрегистрированы в
`docs/superpowers/specs/2026-10-22-weekly-core-proxy-design.md` §3:
baseline — дословно формула панели 36; CAL — расширяющаяся OLS недельного
лог-прироста на календарные регрессоры; STL — period=52 на 7-дневной сетке
лог-уровня; X-13 — `seats_log_cal` через `Rscript` + `seasonal` (Gate 0:
X-13 отвергает frequency=52, статус фиксируется как infeasible).

Запуск — `make weekly-core-proxy`, тесты — `make weekly-core-proxy-test`.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import holidays
import numpy as np
import statsmodels
from statsmodels.tsa.seasonal import STL

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu
from lib.clickhouse import ch_query

WEEKLY_AGGREGATE = "Индекс потребительских цен (недельная оценка)"
CORE_SERIES = "Базовый ИПЦ"
HEADLINE_SERIES = "Индексы потребительских цен на товары и услуги по Российской Федерации"

START = date(2022, 1, 10)
WEEKS = 4
ANNUAL_WEEKS = 52
STL_PERIOD = 52
STL_MIN_POINTS = 2 * STL_PERIOD
CAL_MIN_WEEKS = 26

TARIFF_DATES = (
    date(2022, 7, 1),
    date(2023, 7, 1),
    date(2024, 7, 1),
    date(2025, 7, 1),
    date(2026, 10, 1),
)
TARIFF_MONTHS = ("2022-07", "2023-07", "2024-07", "2025-07", "2026-10")
TARIFF_MONTHS_SENSITIVITY = TARIFF_MONTHS + ("2026-07",)

FEATURES = ("days", "holidays", "tariff", "jan_first", "may")

DEFAULT_OUT = ROOT / "docs" / "research" / "2026-10-22-weekly-core-proxy.json"

WEEKLY_SQL = """
SELECT series, date, value
FROM v_rosstat_macro
WHERE date >= '2022-01-10'
  AND series IN (
      SELECT series FROM v_series_catalog
      WHERE source = 'rosstat' AND frequency = 'W'
  )
ORDER BY date, series
FORMAT JSON
"""

CORE_SQL = """
SELECT date, value
FROM v_cbr_macro
WHERE series = 'Базовый ИПЦ' AND date >= '2021-12-01'
ORDER BY date
FORMAT JSON
"""

HEADLINE_SQL = """
SELECT date, value
FROM v_rosstat_macro
WHERE series = 'Индексы потребительских цен на товары и услуги по Российской Федерации'
  AND date >= '2021-12-01'
ORDER BY date
FORMAT JSON
"""


@dataclass(frozen=True)
class WeeklyRow:
    """Одна неделя ряда: дата регистрации, индекс и число рядов в среднем."""

    date: date
    idx: float
    n_series: int


@dataclass(frozen=True)
class GrowthRow:
    """Недельный лог-прирост и календарные регрессоры интервала."""

    date: date
    days: int
    holidays: int
    tariff: int
    jan_first: int
    may: int
    g: float


def month_key(day: date) -> str:
    return f"{day.year:04d}-{day.month:02d}"


def month_end(key: str) -> date:
    year, month = (int(part) for part in key.split("-"))
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def months_between(first: str, last: str) -> list:
    year, month = (int(part) for part in first.split("-"))
    end_year, end_month = (int(part) for part in last.split("-"))
    keys = []
    while (year, month) <= (end_year, end_month):
        keys.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return keys


def four_week_average(rows: list, weeks: int = WEEKS) -> list:
    """4-нед. средняя панели 36: геометрическое среднее `weeks` индексов, степень 52."""
    if len(rows) < weeks:
        return []
    out = []
    for index in range(weeks - 1, len(rows)):
        window = rows[index - weeks + 1 : index + 1]
        log_mean = sum(math.log(row.idx / 100.0) for row in window) / weeks
        out.append((rows[index].date, (math.exp(log_mean * ANNUAL_WEEKS) - 1.0) * 100.0))
    return out


def four_week_index(rows: list, weeks: int = WEEKS) -> list:
    """4-нед. накопленный индекс: 100 = без изменения за `weeks` недель."""
    if len(rows) < weeks:
        return []
    out = []
    for index in range(weeks - 1, len(rows)):
        window = rows[index - weeks + 1 : index + 1]
        level = math.exp(sum(math.log(row.idx / 100.0) for row in window)) * 100.0
        out.append((rows[index].date, level))
    return out


def regular_grid(rows: list, step_days: int = 7) -> list:
    """7-дневная сетка лог-уровня; недостающие недели — равномерный прирост по дням."""
    if len(rows) < 2:
        return []
    cumulative = [0.0]
    for previous, current in zip(rows, rows[1:]):
        cumulative.append(cumulative[-1] + math.log(current.idx / 100.0))

    grid = []
    cursor = rows[0].date
    index = 0
    while cursor <= rows[-1].date:
        while index + 1 < len(rows) and rows[index + 1].date <= cursor:
            index += 1
        if cursor == rows[index].date:
            level = cumulative[index]
        elif index + 1 < len(rows):
            span = (rows[index + 1].date - rows[index].date).days
            offset = (cursor - rows[index].date).days
            level = cumulative[index] + (cumulative[index + 1] - cumulative[index]) * offset / span
        else:
            level = cumulative[index]
        grid.append((cursor, level))
        cursor += timedelta(days=step_days)
    return grid


def _overlaps_may(previous: date, current: date) -> bool:
    for year in (previous.year, current.year):
        if previous <= date(year, 5, 9) and current >= date(year, 5, 1):
            return True
    return False


def _date_range(start: date, end: date):
    cursor = start
    while cursor <= end:
        yield cursor
        cursor += timedelta(days=1)


def growth_rows(rows: list, calendar, tariff_dates=TARIFF_DATES) -> list:
    """Лог-приросты между соседними публикациями и календарные регрессоры."""
    first_january = {}
    for row in rows:
        if row.date.month == 1 and row.date.year not in first_january:
            first_january[row.date.year] = row.date

    out = []
    for previous, current in zip(rows, rows[1:]):
        days = (current.date - previous.date).days
        holiday_count = sum(
            1 for day in _date_range(previous.date + timedelta(days=1), current.date) if day in calendar
        )
        tariff = int(any(previous.date < day <= current.date for day in tariff_dates))
        jan_first = int(first_january.get(current.date.year) == current.date)
        may = int(_overlaps_may(previous.date, current.date))
        out.append(
            GrowthRow(
                date=current.date,
                days=days,
                holidays=holiday_count,
                tariff=tariff,
                jan_first=jan_first,
                may=may,
                g=math.log(current.idx / 100.0),
            )
        )
    return out


def _design(row: GrowthRow) -> list:
    return [1.0, float(row.days), float(row.holidays), float(row.tariff), float(row.jan_first), float(row.may)]


def calendar_adjust(rows: list, cutoff: date, min_weeks: int = CAL_MIN_WEEKS):
    """As-of календарная корректировка: OLS по данным ≤ cutoff, последние 4 недели."""
    sample = [row for row in rows if row.date <= cutoff]
    if len(sample) < min_weeks:
        return None
    design = np.array([_design(row) for row in sample], dtype=float)
    target = np.array([row.g for row in sample], dtype=float)
    keep = [0] + [column for column in range(1, design.shape[1]) if float(np.std(design[:, column])) > 0]
    beta, *_ = np.linalg.lstsq(design[:, keep], target, rcond=None)
    mean = design[:, keep].mean(axis=0)
    last = sample[-WEEKS:]
    if len(last) < WEEKS:
        return None
    adjusted = []
    for row in last:
        point = np.array(_design(row), dtype=float)[keep]
        adjusted.append(row.g - float((point - mean) @ beta))
    value = (math.exp(sum(adjusted) / WEEKS * ANNUAL_WEEKS) - 1.0) * 100.0
    return value, last[-1].date, len(sample), [FEATURES[column - 1] for column in keep[1:]]


def calendar_adjust_full(rows: list):
    """In-sample диагностика: одни коэффициенты на всё окно, затем 4-нед. средняя."""
    if len(rows) < CAL_MIN_WEEKS:
        return []
    design = np.array([_design(row) for row in rows], dtype=float)
    target = np.array([row.g for row in rows], dtype=float)
    keep = [0] + [column for column in range(1, design.shape[1]) if float(np.std(design[:, column])) > 0]
    beta, *_ = np.linalg.lstsq(design[:, keep], target, rcond=None)
    mean = design[:, keep].mean(axis=0)
    adjusted = [row.g - float((design[index, keep] - mean) @ beta) for index, row in enumerate(rows)]
    out = []
    for index in range(WEEKS - 1, len(rows)):
        window = adjusted[index - WEEKS + 1 : index + 1]
        out.append((rows[index].date, (math.exp(sum(window) / WEEKS * ANNUAL_WEEKS) - 1.0) * 100.0))
    return out


def stl_adjust(grid: list, cutoff: date, min_points: int = STL_MIN_POINTS):
    """As-of STL: лог-уровень, period=52, последние 4 недельных прироста."""
    part = [(day, level) for day, level in grid if day <= cutoff]
    if len(part) < min_points:
        return None
    values = np.array([level for _, level in part], dtype=float)
    result = STL(values, period=STL_PERIOD, robust=True).fit()
    adjusted = values - result.seasonal
    if len(adjusted) < WEEKS + 1:
        return None
    growth = [adjusted[index] - adjusted[index - 1] for index in range(len(adjusted) - WEEKS, len(adjusted))]
    value = (math.exp(sum(growth) / WEEKS * ANNUAL_WEEKS) - 1.0) * 100.0
    return value, part[-1][0], len(part)


def align_last(series: list) -> dict:
    out = {}
    for day, value in series:
        out[month_key(day)] = value
    return out


def align_mean(series: list) -> dict:
    sums = {}
    counts = {}
    for day, value in series:
        key = month_key(day)
        sums[key] = sums.get(key, 0.0) + value
        counts[key] = counts.get(key, 0) + 1
    return {key: sums[key] / counts[key] for key in sums}


def align_first_next(series: list, keys: list) -> dict:
    out = {}
    for key in keys:
        boundary = month_end(key)
        for day, value in series:
            if day > boundary:
                out[key] = value
                break
    return out


def _corr(left: list, right: list) -> float:
    if len(left) < 2 or float(np.std(left)) == 0 or float(np.std(right)) == 0:
        return float("nan")
    return float(np.corrcoef(left, right)[0, 1])


def metrics(actual: dict, prediction: dict, baseline: dict) -> dict | None:
    keys = sorted(set(actual) & set(prediction) & set(baseline))
    if not keys:
        return None
    errors = [prediction[key] - actual[key] for key in keys]
    base_errors = [baseline[key] - actual[key] for key in keys]
    stat, p_value = eu.diebold_mariano(errors, base_errors, h=1)
    return {
        "n": len(keys),
        "window": [keys[0], keys[-1]],
        "mae": eu.mae(errors),
        "rmse": eu.rmse(errors),
        "bias": sum(errors) / len(errors),
        "corr": _corr([prediction[key] for key in keys], [actual[key] for key in keys]),
        "baseline": {
            "mae": eu.mae(base_errors),
            "rmse": eu.rmse(base_errors),
            "bias": sum(base_errors) / len(base_errors),
            "corr": _corr([baseline[key] for key in keys], [actual[key] for key in keys]),
        },
        "dm_vs_b0": {"stat": stat, "p": p_value},
    }


def load_weekly() -> list:
    rows = ch_query(WEEKLY_SQL)
    by_date = {}
    for row in rows:
        day = date.fromisoformat(row["date"])
        value = float(row["value"])
        if value <= 0:
            raise ValueError(f"недельный индекс ≤ 0: {row['series']} {row['date']}: {value}")
        by_date.setdefault(day, {})[row["series"]] = value
    goods = []
    aggregate = []
    for day in sorted(by_date):
        values = {series: value for series, value in by_date[day].items() if series != WEEKLY_AGGREGATE}
        if values:
            goods.append(WeeklyRow(day, sum(values.values()) / len(values), len(values)))
        if WEEKLY_AGGREGATE in by_date[day]:
            aggregate.append(WeeklyRow(day, by_date[day][WEEKLY_AGGREGATE], 1))
    return goods, aggregate


def load_monthly(sql: str, as_index: bool) -> dict:
    out = {}
    for row in ch_query(sql):
        value = float(row["value"])
        annualized = (math.pow(value / 100.0, 12) - 1.0) * 100.0 if as_index else (math.pow(1.0 + value / 100.0, 12) - 1.0) * 100.0
        out[month_key(date.fromisoformat(row["date"]))] = annualized
    return out


def x13_probe() -> dict:
    env = dict(os.environ)
    env["R_LIBS_USER"] = str(ROOT / "build" / "weekly-core-proxy" / "r-lib")
    script = ROOT / "scripts" / "models" / "cpi" / "weekly_core_proxy_x13.R"
    try:
        completed = subprocess.run(
            ["Rscript", str(script), "--probe"],
            capture_output=True, text=True, timeout=120, env=env,
        )
    except FileNotFoundError:
        return {"status": "unavailable", "detail": "Rscript не найден"}
    detail = (completed.stdout + completed.stderr).strip()
    if completed.returncode == 0:
        return {"status": "supported", "detail": detail}
    if "Seasonal period too large" in detail:
        return {"status": "infeasible", "detail": detail}
    return {"status": "unavailable", "detail": detail}


def x13_run(grid: list, keys: list, probe: dict) -> tuple:
    """As-of X-13 по сетке; при infeasible/unavailable возвращает пустой ряд."""
    if probe["status"] != "supported":
        return [], probe
    env = dict(os.environ)
    env["R_LIBS_USER"] = str(ROOT / "build" / "weekly-core-proxy" / "r-lib")
    script = ROOT / "scripts" / "models" / "cpi" / "weekly_core_proxy_x13.R"
    with tempfile.TemporaryDirectory() as directory:
        grid_path = Path(directory) / "grid.csv"
        cutoffs_path = Path(directory) / "cutoffs.txt"
        output_path = Path(directory) / "sa.csv"
        with grid_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(["date", "level"])
            for day, level in grid:
                writer.writerow([day.isoformat(), math.exp(level)])
        cutoffs_path.write_text("\n".join(month_end(key).isoformat() for key in keys), encoding="utf-8")
        completed = subprocess.run(
            ["Rscript", str(script), "--run", str(grid_path), str(cutoffs_path), str(output_path)],
            capture_output=True, text=True, timeout=300, env=env,
        )
        if completed.returncode != 0 or not output_path.exists():
            return [], {"status": "unavailable", "detail": (completed.stdout + completed.stderr).strip()}
        by_cutoff = {}
        with output_path.open(encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("error"):
                    continue
                by_cutoff.setdefault(row["cutoff"], []).append((date.fromisoformat(row["date"]), float(row["sa_level"])))
    series = []
    for cutoff, points in by_cutoff.items():
        points.sort()
        if len(points) < WEEKS + 1:
            continue
        values = [math.log(level) for _, level in points[-(WEEKS + 1):]]
        growth = [values[index] - values[index - 1] for index in range(1, len(values))]
        value = (math.exp(sum(growth) / WEEKS * ANNUAL_WEEKS) - 1.0) * 100.0
        series.append((points[-1][0], value))
    return series, probe


def _x13_monthly_script() -> Path:
    return ROOT / "scripts" / "models" / "cpi" / "weekly_core_proxy_x13_monthly.R"


def _x13_env() -> dict:
    env = dict(os.environ)
    env["R_LIBS_USER"] = str(ROOT / "build" / "weekly-core-proxy" / "r-lib")
    return env


def x13_monthly_probe() -> dict:
    """Gate 0 месячной сетки: frequency=12 на синтетическом ряду."""
    try:
        completed = subprocess.run(
            ["Rscript", str(_x13_monthly_script()), "--probe"],
            capture_output=True, text=True, timeout=120, env=_x13_env(),
        )
    except FileNotFoundError:
        return {"status": "unavailable", "detail": "Rscript не найден"}
    detail = (completed.stdout + completed.stderr).strip()
    if completed.returncode == 0:
        return {"status": "supported", "detail": detail}
    return {"status": "unavailable", "detail": detail}


def _x13_monthly_csv(series: list, keys: list, path: Path) -> None:
    by_month = align_last(series)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["date", "level"])
        for key in keys:
            if key in by_month:
                writer.writerow([month_end(key).isoformat(), by_month[key]])


def _x13_monthly_from_sa(rows: list) -> dict:
    out = {}
    for row in rows:
        if row.get("error") or not row.get("sa_level"):
            continue
        level = float(row["sa_level"])
        value = (math.pow(level / 100.0, 13) - 1.0) * 100.0
        out[month_key(date.fromisoformat(row["date"]))] = value
    return out


def x13_monthly_fit(series: list, keys: list, probe: dict) -> tuple:
    """X-13 на полной месячной выборке 4-нед. индекса (frequency=12, in-sample)."""
    if probe["status"] != "supported":
        return {}, probe, []
    with tempfile.TemporaryDirectory() as directory:
        input_path = Path(directory) / "months.csv"
        output_path = Path(directory) / "sa.csv"
        _x13_monthly_csv(series, keys, input_path)
        completed = subprocess.run(
            ["Rscript", str(_x13_monthly_script()), "--fit", str(input_path), str(output_path)],
            capture_output=True, text=True, timeout=300, env=_x13_env(),
        )
        if completed.returncode != 0 or not output_path.exists():
            return {}, {"status": "unavailable", "detail": (completed.stdout + completed.stderr).strip()}, []
        with output_path.open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    errors = sorted({row["error"] for row in rows if row.get("error")})
    return _x13_monthly_from_sa(rows), probe, errors


def x13_monthly_asof(series: list, keys: list, probe: dict, min_months: int = 36) -> tuple:
    """X-13 на месячной сетке as-of: для каждого cutoff — только данные ≤ cutoff."""
    if probe["status"] != "supported":
        return {}, probe, []
    with tempfile.TemporaryDirectory() as directory:
        input_path = Path(directory) / "months.csv"
        cutoffs_path = Path(directory) / "cutoffs.txt"
        output_path = Path(directory) / "sa.csv"
        _x13_monthly_csv(series, keys, input_path)
        cutoffs_path.write_text("\n".join(month_end(key).isoformat() for key in keys), encoding="utf-8")
        completed = subprocess.run(
            [
                "Rscript", str(_x13_monthly_script()), "--asof",
                str(input_path), str(cutoffs_path), str(output_path),
            ],
            capture_output=True, text=True, timeout=600, env=_x13_env(),
        )
        if completed.returncode != 0 or not output_path.exists():
            return {}, {"status": "unavailable", "detail": (completed.stdout + completed.stderr).strip()}, []
        with output_path.open(encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    errors = sorted({row["error"] for row in rows if row.get("error")})
    return _x13_monthly_from_sa(rows), probe, errors


def series_for_months(keys: list, function) -> list:
    out = []
    for key in keys:
        result = function(month_end(key))
        if result is not None:
            out.append((result[1], result[0]))
    return out


def git_sha() -> str:
    try:
        return subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def evaluate_basis(actual: dict, baseline: dict, predictions: dict, keys: list | None = None) -> dict:
    if keys is not None:
        allowed = set(keys)
        actual = {key: value for key, value in actual.items() if key in allowed}
        baseline = {key: value for key, value in baseline.items() if key in allowed}
        predictions = {
            name: {key: value for key, value in series.items() if key in allowed}
            for name, series in predictions.items()
        }
    block = {"b0": metrics(actual, baseline, baseline)}
    for name, series in predictions.items():
        block[name] = metrics(actual, series, baseline)
    return block


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Недельный прокси базовой инфляции: сравнение корректировок (#348)")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="путь JSON-артефакта")
    args = parser.parse_args(argv)

    goods, aggregate = load_weekly()
    core = load_monthly(CORE_SQL, as_index=False)
    headline = load_monthly(HEADLINE_SQL, as_index=True)
    keys = [key for key in months_between("2022-01", "2026-09") if key in core]

    calendar = holidays.Russia(years=range(2022, 2028))

    b0_goods = align_last(four_week_average(goods))
    b0_aggregate = align_last(four_week_average(aggregate))

    growth_goods = growth_rows(goods, calendar)
    growth_aggregate = growth_rows(aggregate, calendar)

    cal_goods_raw = series_for_months(keys, lambda cutoff: calendar_adjust(growth_goods, cutoff))
    cal_aggregate_raw = series_for_months(keys, lambda cutoff: calendar_adjust(growth_aggregate, cutoff))
    cal_goods = align_last(cal_goods_raw)
    cal_aggregate = align_last(cal_aggregate_raw)

    cal_full_goods = align_last(calendar_adjust_full(growth_goods))
    cal_full_aggregate = align_last(calendar_adjust_full(growth_aggregate))

    grid_goods = regular_grid(goods)
    grid_aggregate = regular_grid(aggregate)
    stl_goods_raw = series_for_months(keys, lambda cutoff: stl_adjust(grid_goods, cutoff))
    stl_aggregate_raw = series_for_months(keys, lambda cutoff: stl_adjust(grid_aggregate, cutoff))
    stl_goods = align_last(stl_goods_raw)
    stl_aggregate = align_last(stl_aggregate_raw)

    probe = x13_probe()
    x13_goods_raw, probe = x13_run(grid_goods, keys, probe)
    x13_aggregate_raw, probe = x13_run(grid_aggregate, keys, probe)
    x13_goods = align_last(x13_goods_raw)
    x13_aggregate = align_last(x13_aggregate_raw)

    index_goods = four_week_index(goods)
    index_aggregate = four_week_index(aggregate)
    monthly_probe = x13_monthly_probe()
    x13m_goods, monthly_probe, x13m_goods_errors = x13_monthly_fit(index_goods, keys, monthly_probe)
    x13m_aggregate, monthly_probe, x13m_aggregate_errors = x13_monthly_fit(index_aggregate, keys, monthly_probe)
    x13m_asof_goods, monthly_probe, x13m_asof_goods_errors = x13_monthly_asof(index_goods, keys, monthly_probe)
    x13m_asof_aggregate, monthly_probe, x13m_asof_aggregate_errors = x13_monthly_asof(index_aggregate, keys, monthly_probe)

    predictions_goods = {"cal": cal_goods, "stl": stl_goods}
    predictions_aggregate = {"cal": cal_aggregate, "stl": stl_aggregate}
    if x13_goods:
        predictions_goods["x13"] = x13_goods
    if x13_aggregate:
        predictions_aggregate["x13"] = x13_aggregate

    core_goods = evaluate_basis(core, b0_goods, predictions_goods)
    core_aggregate = evaluate_basis(core, b0_aggregate, predictions_aggregate)
    headline_goods = evaluate_basis(headline, b0_goods, predictions_goods)
    headline_aggregate = evaluate_basis(headline, b0_aggregate, predictions_aggregate)

    core_x13m_goods = metrics(core, x13m_goods, b0_goods)
    core_x13m_aggregate = metrics(core, x13m_aggregate, b0_aggregate)
    core_x13m_asof_goods = metrics(core, x13m_asof_goods, b0_goods)
    core_x13m_asof_aggregate = metrics(core, x13m_asof_aggregate, b0_aggregate)
    headline_x13m_goods = metrics(headline, x13m_goods, b0_goods)
    headline_x13m_aggregate = metrics(headline, x13m_aggregate, b0_aggregate)
    headline_x13m_asof_goods = metrics(headline, x13m_asof_goods, b0_goods)
    headline_x13m_asof_aggregate = metrics(headline, x13m_asof_aggregate, b0_aggregate)

    ex_tariff = [key for key in keys if key not in TARIFF_MONTHS]
    sensitivity = [key for key in keys if key not in TARIFF_MONTHS_SENSITIVITY]
    core_ex_tariff_goods = evaluate_basis(core, b0_goods, predictions_goods, ex_tariff)
    core_ex_tariff_aggregate = evaluate_basis(core, b0_aggregate, predictions_aggregate, ex_tariff)
    core_sensitivity_goods = evaluate_basis(core, b0_goods, predictions_goods, sensitivity)
    core_sensitivity_aggregate = evaluate_basis(core, b0_aggregate, predictions_aggregate, sensitivity)

    mean_goods = align_mean(four_week_average(goods))
    mean_aggregate = align_mean(four_week_average(aggregate))
    first_goods = align_first_next(four_week_average(goods), keys)
    first_aggregate = align_first_next(four_week_average(aggregate), keys)

    accepted = []
    for basis, block in (("goods", core_goods), ("agg", core_aggregate)):
        for method in ("cal", "stl", "x13"):
            entry = block.get(method)
            if (
                entry
                and entry["mae"] < entry["baseline"]["mae"]
                and entry["dm_vs_b0"]["p"] < 0.05
            ):
                accepted.append(f"{basis}:{method}")

    usable = []
    for basis, block in (("goods", core_goods), ("agg", core_aggregate)):
        headline_block = headline_goods if basis == "goods" else headline_aggregate
        for method in ("cal", "stl", "x13"):
            if f"{basis}:{method}" not in accepted:
                continue
            entry = block[method]
            headline_entry = headline_block.get(method)
            tracking_ok = entry["corr"] >= entry["baseline"]["corr"] - 0.10
            headline_ok = (
                headline_entry is None
                or headline_entry["mae"] <= headline_entry["baseline"]["mae"]
                or headline_entry["dm_vs_b0"]["p"] >= 0.05
            )
            if tracking_ok and headline_ok:
                usable.append(f"{basis}:{method}")

    monthly = []
    for key in keys:
        monthly.append(
            {
                "month": key,
                "core_pp": core.get(key),
                "headline_pp": headline.get(key),
                "b0_goods": b0_goods.get(key),
                "b0_agg": b0_aggregate.get(key),
                "cal_goods": cal_goods.get(key),
                "cal_agg": cal_aggregate.get(key),
                "stl_goods": stl_goods.get(key),
                "stl_agg": stl_aggregate.get(key),
                "x13_goods": x13_goods.get(key),
                "x13_agg": x13_aggregate.get(key),
                "x13m_goods": x13m_goods.get(key),
                "x13m_agg": x13m_aggregate.get(key),
                "x13m_asof_goods": x13m_asof_goods.get(key),
                "x13m_asof_agg": x13m_asof_aggregate.get(key),
            }
        )

    payload = {
        "decision": "accepted" if accepted else "indicative",
        "accepted": accepted,
        "post_hoc": {
            "tracking_tolerance": 0.10,
            "usable_as_proxy": usable,
            "best_baseline": {
                "basis": "agg",
                "core_mae": core_aggregate["b0"]["mae"],
                "core_corr": core_aggregate["b0"]["corr"],
                "headline_mae": headline_aggregate["b0"]["mae"],
            },
            "note": (
                "Формальный критерий §3.5 — MAE ниже baseline и DM p < 0,05; "
                "usable_as_proxy — post-hoc диагностика после просмотра результатов: "
                "corr не ниже baseline более чем на 0,10 и headline не значимо хуже. "
                "Критерий приёмки она не заменяет."
            ),
        },
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "git_sha": git_sha(),
        "package_versions": {
            "statsmodels": statsmodels.__version__,
            "numpy": np.__version__,
            "holidays": getattr(holidays, "__version__", ""),
        },
        "inputs": {
            "weekly": {
                "source": "v_rosstat_macro",
                "catalog": "v_series_catalog",
                "frequency": "W",
                "start": START.isoformat(),
                "goods_range": [goods[0].date.isoformat(), goods[-1].date.isoformat()],
                "goods_dates": len(goods),
                "goods_series_per_date": [min(row.n_series for row in goods), max(row.n_series for row in goods)],
                "aggregate_range": [aggregate[0].date.isoformat(), aggregate[-1].date.isoformat()],
                "aggregate_dates": len(aggregate),
            },
            "target_core": {
                "source": "v_cbr_macro",
                "series": CORE_SERIES,
                "range": [keys[0] + "-01", month_end(keys[-1]).isoformat()],
                "months": len(keys),
                "unit": "% годовых из м/м: pow(1+v/100,12)-1",
            },
            "target_headline": {
                "source": "v_rosstat_macro",
                "series": HEADLINE_SERIES,
                "unit": "% годовых из индекса: pow(v/100,12)-1",
            },
        },
        "spec": {
            "baseline": "pow(exp(avg(ln(idx/100)) по 4 неделям), 52)-1, n=4",
            "alignment_primary": "last_in_month",
            "alignment_robustness": ["mean_in_month", "first_next_month"],
            "tariff_months": list(TARIFF_MONTHS),
            "tariff_months_in_grid": [key for key in TARIFF_MONTHS if key in keys],
            "tariff_months_sensitivity": list(TARIFF_MONTHS_SENSITIVITY),
            "tariff_months_sensitivity_in_grid": [key for key in TARIFF_MONTHS_SENSITIVITY if key in keys],
            "cal": {"features": list(FEATURES), "min_weeks": CAL_MIN_WEEKS, "as_of": True},
            "stl": {"period": STL_PERIOD, "robust": True, "min_points": STL_MIN_POINTS},
            "x13": {"spec": "seats_log_cal", "transform": "log", "aictest": "td"},
        },
        "x13": probe,
        "x13_monthly": {
            "status": monthly_probe["status"],
            "detail": monthly_probe["detail"],
            "spec": {
                "frequency": 12,
                "transform": "log",
                "aictest": "td",
                "alignment": "last_in_month",
                "annualization": "pow(index/100, 13)-1",
                "asof_min_months": 36,
            },
            "full_sample": {
                "core": {"goods": core_x13m_goods, "agg": core_x13m_aggregate},
                "headline": {"goods": headline_x13m_goods, "agg": headline_x13m_aggregate},
                "errors": {"goods": x13m_goods_errors, "agg": x13m_aggregate_errors},
            },
            "as_of": {
                "core": {"goods": core_x13m_asof_goods, "agg": core_x13m_asof_aggregate},
                "headline": {"goods": headline_x13m_asof_goods, "agg": headline_x13m_asof_aggregate},
                "errors": {"goods": x13m_asof_goods_errors, "agg": x13m_asof_aggregate_errors},
            },
        },
        "monthly": monthly,
        "metrics": {
            "core": {"goods": core_goods, "agg": core_aggregate},
            "headline": {"goods": headline_goods, "agg": headline_aggregate},
        },
        "metrics_ex_tariff": {
            "core": {"goods": core_ex_tariff_goods, "agg": core_ex_tariff_aggregate},
            "sensitivity_plus_2026_07": {"goods": core_sensitivity_goods, "agg": core_sensitivity_aggregate},
        },
        "robustness": {
            "mean_in_month": {
                "b0_goods": metrics(core, mean_goods, b0_goods),
                "b0_agg": metrics(core, mean_aggregate, b0_aggregate),
                "cal_goods": metrics(core, align_mean(cal_goods_raw), mean_goods),
                "cal_agg": metrics(core, align_mean(cal_aggregate_raw), mean_aggregate),
                "stl_goods": metrics(core, align_mean(stl_goods_raw), mean_goods),
                "stl_agg": metrics(core, align_mean(stl_aggregate_raw), mean_aggregate),
            },
            "first_next_month": {
                "b0_goods": metrics(core, first_goods, b0_goods),
                "b0_agg": metrics(core, first_aggregate, b0_aggregate),
                "cal_goods": metrics(core, align_first_next(cal_goods_raw, keys), first_goods),
                "cal_agg": metrics(core, align_first_next(cal_aggregate_raw, keys), first_aggregate),
                "stl_goods": metrics(core, align_first_next(stl_goods_raw, keys), first_goods),
                "stl_agg": metrics(core, align_first_next(stl_aggregate_raw, keys), first_aggregate),
            },
            "cal_full_sample": {
                "goods": metrics(core, cal_full_goods, b0_goods),
                "agg": metrics(core, cal_full_aggregate, b0_aggregate),
            },
        },
        "notes": [
            "Товарная средняя — невзвешенное среднее доступных товарных рядов; агрегат исключён.",
            "Цель — «Базовый ИПЦ» (core), товарная средняя и агрегат — прокси текущего роста цен.",
            "X-13: Gate 0 фиксирует отказ X-13 на frequency=52; числа не подставляются.",
            "X-13 monthly (x13_monthly) — дополнительная проверка: frequency=12 на 4-нед. накопленном индексе, вне предрегистрированного критерия; full_sample — in-sample, as_of — расширяющееся окно с 36 месяцев.",
        ],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    print(f"decision={payload['decision']} accepted={accepted}")
    for basis, block in (("goods", core_goods), ("agg", core_aggregate)):
        for name, entry in block.items():
            if not entry:
                continue
            print(
                f"core {basis:5s} {name:3s}: n={entry['n']:2d} "
                f"MAE={entry['mae']:.2f} (B0 {entry['baseline']['mae']:.2f}) "
                f"RMSE={entry['rmse']:.2f} DM p={entry['dm_vs_b0']['p']:.4f} "
                f"corr={entry['corr']:.2f}"
            )
    print(f"x13={probe['status']}")
    print(f"x13_monthly={monthly_probe['status']}")
    for basis, entry, asof in (
        ("goods", core_x13m_goods, core_x13m_asof_goods),
        ("agg", core_x13m_aggregate, core_x13m_asof_aggregate),
    ):
        if entry:
            print(
                f"core {basis:5s} x13m     : n={entry['n']:2d} "
                f"MAE={entry['mae']:.2f} (B0 {entry['baseline']['mae']:.2f}) "
                f"DM p={entry['dm_vs_b0']['p']:.4f} corr={entry['corr']:.2f}"
            )
        if asof:
            print(
                f"core {basis:5s} x13m-asof: n={asof['n']:2d} "
                f"MAE={asof['mae']:.2f} (B0 {asof['baseline']['mae']:.2f}) "
                f"DM p={asof['dm_vs_b0']['p']:.4f} corr={asof['corr']:.2f}"
            )
    print(f"written {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
