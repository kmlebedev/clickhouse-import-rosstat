#!/usr/bin/env python3
"""Недельная компонентная панель ИПЦ: месячный m/m товаров и компонент.

Task 3 пункта `key-rate/phase-4-ml-components` (issue #236). Скрипт только
читает ClickHouse и публичные веса/словарь из архива CB_COMP; в БД не пишет.
Внутренняя отраслевая панель ЦБ не используется.

Идея: недельные индексы Росстата — «% к предыдущей дате регистрации».
Месячный m/m товара — произведение значений недель, зарегистрированных в
месяце; месяцы с <3 неделями, без предыдущей точки, с первой/последней
точкой далеко от границ или с разрывом >21 дня дают NaN. Сопоставленные с весами товары реконструируют food/nonfood/services;
покрытие весов решает, допустим ли полный bottom-up.

Запуск — через `make cpi-weekly-panel` (env подключает Makefile), тесты —
`make cpi-weekly-panel-test`.
"""

import argparse
import bisect
import csv
import json
import math
import platform
import subprocess
import sys
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_public_panel_audit as audit

SCHEMA = "cpi-weekly-component-panel/v1"
COMPONENTS = ("food", "nonfood", "services")
COMPONENT_LABELS = {
    "food": "Продовольствие",
    "nonfood": "Непродовольствие",
    "services": "Услуги",
}
SERIES_ORDER = ("food", "nonfood", "services", "all_products", "all_components")
SERIES_LABELS = {
    **COMPONENT_LABELS,
    "all_products": "Все товары (взвешенно по покрытым)",
    "all_components": "Все товары (по компонентам)",
}
OFFICIAL_KEYS = {
    "food": "cpi_food",
    "nonfood": "cpi_nonfood",
    "services": "cpi_services",
    "all_products": "cpi_all",
    "all_components": "cpi_all",
}

MIN_WEEKS = 3
MAX_GAP_DAYS = 21
MAX_FIRST_OFFSET_DAYS = 10
MAX_LAST_OFFSET_DAYS = 10
CUTOFF_DAY = audit.CUTOFF_DAY
WEEKLY_LAG_DAYS = audit.WEEKLY_PUBLICATION_LAG_DAYS
COVERAGE_GATE = audit.COVERAGE_GATE
WEIGHT_YEARS = tuple(audit.WEIGHT_YEARS)
DEFAULT_WEIGHTS_YEAR = audit.DEFAULT_WEIGHTS_YEAR

PERIODS = {
    "dense_2022": (date(2022, 1, 1), date(2026, 9, 1)),
    "model_2013_2026_08": (date(2013, 10, 1), date(2026, 8, 1)),
    "pre2022": (date(2013, 10, 1), date(2021, 12, 1)),
    "all": (date(2009, 4, 1), date(2026, 9, 1)),
}
TASK1_DIAGNOSTIC_PERIOD = (date(2022, 1, 1), date(2026, 8, 1))
TASK1_DIAGNOSTIC = {
    "n_months": 56,
    "mae_pp": 0.40888808446990144,
    "rmse_pp": 0.5346891947822022,
    "max_abs_pp": 1.2580295110590718,
    "mean_pp": 0.19561506419126043,
}
TASK1_AGGREGATE = {
    "n_months": 146,
    "min_month": "2009-04-01",
    "max_month": "2021-12-01",
    "mae_pp": 0.16468811664720465,
    "rmse_pp": 0.2580806456288507,
    "max_abs_pp": 1.7353648741210703,
    "mean_pp": -0.08979483280725227,
}
TASK1_COVERAGE_2024 = 0.39299
TASK1_COMPONENT_COVERAGE = {
    "food": 0.4807586369717478,
    "nonfood": 0.37868414093032826,
    "services": 0.29032605573111175,
}
TASK1_MATCHED = 118
TASK1_UNMATCHED = 30

PRODUCT_COLUMNS = (
    "month",
    "series",
    "component",
    "group_code",
    "group_name",
    "weight_code",
    "weight_2024",
    "weight_used",
    "weight_year",
    "status",
    "n_weeks",
    "first_week",
    "last_week",
    "prev_week",
    "max_gap_days",
    "mm_pp",
)
COMPONENT_COLUMNS = (
    "month",
    "component",
    "recon_mm_pp",
    "official_mm_pp",
    "error_pp",
    "weight_year",
    "component_weight",
    "matched_weight",
    "covered_weight",
    "covered_share",
    "n_products",
)
STATUSES = (
    "ok",
    "no_points",
    "no_previous",
    "lt_min_weeks",
    "incomplete_month",
    "gap_too_large",
)
SAMPLE_KEYS = (
    "month",
    "series",
    "status",
    "mm_pp",
    "n_weeks",
    "first_week",
    "last_week",
    "prev_week",
    "max_gap_days",
)


# ---------------------------------------------------------------------------
# Даты, числа, загрузка
# ---------------------------------------------------------------------------


def iso(value) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (date, datetime)):
        return value.isoformat()[:10]
    return str(value)


def finite_positive(value, context: str) -> float:
    number = float(value)
    if not math.isfinite(number) or number <= 0:
        raise ValueError(f"{context}: значение {value!r} не положительное конечное число")
    return number


def load_weekly_products() -> tuple[dict, list]:
    """148 товарных W-рядов и агрегат для сверки компаундирования."""
    weekly = audit.load_weekly()
    aggregate = weekly.get(audit.WEEKLY_AGGREGATE)
    if aggregate is None:
        raise ValueError(f"нет агрегатного ряда {audit.WEEKLY_AGGREGATE!r}")
    products = {
        series: points
        for series, points in weekly.items()
        if series != audit.WEEKLY_AGGREGATE
    }
    for series, points in products.items():
        if not points:
            raise ValueError(f"пустой недельный ряд {series!r}")
        seen = set()
        for day, value in points:
            if day in seen:
                raise ValueError(f"{series}: дата {day.isoformat()} встречается дважды")
            seen.add(day)
            finite_positive(value, f"{series}@{day.isoformat()}")
    if len(products) != 148:
        raise ValueError(f"ожидалось 148 товарных W-рядов, получено {len(products)}")
    return products, aggregate


def load_monthly_components() -> tuple[dict, dict]:
    """4 официальных ряда: `{key: {month_start: pp}}` и `{key: [(date, index)]}`."""
    monthly = audit.load_monthly()
    if set(monthly) != set(audit.MONTHLY_SERIES):
        raise ValueError(
            f"ожидались 4 официальных ряда, получено {sorted(monthly)}"
        )
    pp = {}
    raw = {}
    for key, points in monthly.items():
        if not points:
            raise ValueError(f"пустой официальный ряд {key!r}")
        by_month = {}
        for day, value in points:
            month = audit.month_start(day)
            if month in by_month:
                raise ValueError(
                    f"{key}: месяц {month.isoformat()} встречается дважды"
                )
            index = finite_positive(value, f"{key}@{day.isoformat()}")
            by_month[month] = index - 100.0
        pp[key] = by_month
        raw[key] = sorted(points)
    return pp, raw


@lru_cache(maxsize=None)
def cached_weights(archive: str, year: int) -> tuple:
    return tuple(audit.load_weights(Path(archive), year))


@lru_cache(maxsize=None)
def cached_dictionary(archive: str, year: int) -> dict:
    return audit.load_dictionary(Path(archive), year)


def component_denominators(weights: list, dictionary: dict) -> dict:
    by_code = audit.weights_by_code(weights)
    result = {component: 0.0 for component in COMPONENTS}
    for code, meta in dictionary.items():
        if meta.get("level") != 7:
            continue
        component = meta.get("component_key")
        if component in result:
            result[component] += by_code.get(code, 0.0)
    return result


def total_denominator(weights: list, dictionary: dict) -> float:
    by_code = audit.weights_by_code(weights)
    return sum(
        by_code.get(code, 0.0)
        for code, meta in dictionary.items()
        if meta.get("level") == 7
    )


def build_weights_matrix(archive: Path, matched_rows: list) -> dict:
    """`{year: {by_code, denominators, total, codes_missing}}`."""
    matched_codes = {row["weight_code"] for row in matched_rows}
    matrix = {}
    for year in WEIGHT_YEARS:
        weights = list(cached_weights(str(archive), year))
        dictionary = cached_dictionary(str(archive), year)
        by_code = audit.weights_by_code(weights)
        matrix[year] = {
            "by_code": by_code,
            "leaf_codes": {
                code
                for code, meta in dictionary.items()
                if meta.get("level") == 7
            },
            "denominators": component_denominators(weights, dictionary),
            "total": total_denominator(weights, dictionary),
            "codes_missing": sorted(
                code for code in matched_codes if code not in by_code
            ),
        }
    return matrix


def weight_year_for(month: date) -> int:
    year = month.year
    if year < min(WEIGHT_YEARS):
        return min(WEIGHT_YEARS)
    if year > max(WEIGHT_YEARS):
        return max(WEIGHT_YEARS)
    return year


def coverage_by_year(weights_matrix: dict, matched_rows: list) -> list:
    matched_codes = {row["weight_code"] for row in matched_rows}
    result = []
    for year in WEIGHT_YEARS:
        item = weights_matrix[year]
        numerator = sum(item["by_code"].get(code, 0.0) for code in matched_codes)
        denominator = item["total"]
        result.append(
            {
                "year": year,
                "matched_codes": sum(
                    1 for code in matched_codes if code in item["leaf_codes"]
                ),
                "numerator": numerator,
                "denominator": denominator,
                "coverage": numerator / denominator if denominator else None,
            }
        )
    return result


# ---------------------------------------------------------------------------
# Месячный m/m товара
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MonthlyCell:
    month: date
    status: str
    mm_pp: Optional[float]
    n_weeks: int
    first_week: Optional[date]
    last_week: Optional[date]
    prev_week: Optional[date]
    max_gap_days: Optional[int]


def month_points(
    series_points: list, month: date, cutoff: date
) -> list:
    start = audit.month_start(month)
    result = []
    seen = set()
    for day, value in series_points:
        if audit.month_start(day) != start:
            continue
        if audit.available_from_weekly(day) > cutoff:
            continue
        if day in seen:
            raise ValueError(f"дубль даты {day.isoformat()} в месяце {start.isoformat()}")
        seen.add(day)
        result.append((day, value))
    result.sort()
    return result


def previous_point(
    series_points: list, first_date: date, cutoff: date
) -> Optional[tuple]:
    dates = [day for day, _ in series_points]
    position = bisect.bisect_left(dates, first_date) - 1
    if position < 0:
        return None
    day, value = series_points[position]
    if audit.available_from_weekly(day) > cutoff:
        return None
    return day, value


def monthly_mm(
    series_points: list,
    month: date,
    cutoff: date,
    min_weeks: int = MIN_WEEKS,
    max_gap_days: int = MAX_GAP_DAYS,
) -> tuple:
    """Месячный m/m товара; NaN-статус, если месяц неполный."""
    start = audit.month_start(month)
    points = month_points(series_points, start, cutoff)
    if not points:
        return None, MonthlyCell(
            start, "no_points", None, 0, None, None, None, None
        )
    first_week = points[0][0]
    last_week = points[-1][0]
    prev = previous_point(series_points, first_week, cutoff)
    gaps = []
    if prev is not None:
        gaps.append((first_week - prev[0]).days)
    for (left, _), (right, _) in zip(points, points[1:]):
        gaps.append((right - left).days)
    max_gap = max(gaps) if gaps else None
    if prev is None:
        return None, MonthlyCell(
            start,
            "no_previous",
            None,
            len(points),
            first_week,
            last_week,
            None,
            max_gap,
        )
    if len(points) < min_weeks:
        return None, MonthlyCell(
            start,
            "lt_min_weeks",
            None,
            len(points),
            first_week,
            last_week,
            prev[0],
            max_gap,
        )
    first_offset = (first_week - start).days
    last_offset = (audit.month_end(start) - last_week).days
    if (
        first_offset > MAX_FIRST_OFFSET_DAYS
        or last_offset > MAX_LAST_OFFSET_DAYS
    ):
        return None, MonthlyCell(
            start,
            "incomplete_month",
            None,
            len(points),
            first_week,
            last_week,
            prev[0],
            max_gap,
        )
    if max_gap is None or max_gap > max_gap_days:
        return None, MonthlyCell(
            start,
            "gap_too_large",
            None,
            len(points),
            first_week,
            last_week,
            prev[0],
            max_gap,
        )
    index = math.prod(value / 100.0 for _, value in points)
    return (index - 1.0) * 100.0, MonthlyCell(
        start,
        "ok",
        (index - 1.0) * 100.0,
        len(points),
        first_week,
        last_week,
        prev[0],
        max_gap,
    )


def product_row(
    month: date,
    series: str,
    meta: Optional[dict],
    cell: MonthlyCell,
    weights_matrix: dict,
) -> dict:
    year = weight_year_for(month)
    weight_2024 = meta["weight_2024"] if meta else None
    weight_used = (
        weights_matrix[year]["by_code"].get(meta["weight_code"])
        if meta
        else None
    )
    return {
        "month": month,
        "series": series,
        "component": meta["component"] if meta else None,
        "group_code": meta["group_code"] if meta else None,
        "group_name": meta["group_name"] if meta else None,
        "weight_code": meta["weight_code"] if meta else None,
        "weight_2024": weight_2024,
        "weight_used": weight_used,
        "weight_year": year,
        "status": cell.status,
        "n_weeks": cell.n_weeks,
        "first_week": cell.first_week,
        "last_week": cell.last_week,
        "prev_week": cell.prev_week,
        "max_gap_days": cell.max_gap_days,
        "mm_pp": cell.mm_pp,
    }


def build_product_panel(
    products: dict,
    matched_rows: list,
    weights_matrix: dict,
    months: list,
    min_weeks: int = MIN_WEEKS,
    max_gap_days: int = MAX_GAP_DAYS,
) -> list:
    meta_by_series = {row["series"]: row for row in matched_rows}
    rows = []
    for series in sorted(products):
        points = products[series]
        meta = meta_by_series.get(series)
        for month in months:
            cutoff = audit.cutoff_for(month)
            _, cell = monthly_mm(
                points, month, cutoff, min_weeks, max_gap_days
            )
            rows.append(product_row(month, series, meta, cell, weights_matrix))
    return rows


def as_of_check(
    products: dict,
    monthly_pp: dict,
    months: list,
) -> dict:
    used_points = 0
    excluded_points = 0
    excluded_dates = set()
    violations = []
    official_months = set(monthly_pp["cpi_all"])
    for month in months:
        start = audit.month_start(month)
        cutoff = audit.cutoff_for(month)
        for series, points in products.items():
            for day, _ in points:
                if day < start or day > cutoff:
                    continue
                if audit.available_from_weekly(day) <= cutoff:
                    if audit.month_start(day) == start:
                        used_points += 1
                else:
                    excluded_points += 1
                    excluded_dates.add(day)
        if start in official_months:
            if audit.available_from_mes(audit.month_end(start)) <= cutoff:
                violations.append(
                    {
                        "kind": "monthly_label_before_cutoff",
                        "month": start.isoformat(),
                        "cutoff": cutoff.isoformat(),
                        "available_from": audit.available_from_mes(
                            audit.month_end(start)
                        ).isoformat(),
                    }
                )
    return {
        "cutoff_rule": f"{CUTOFF_DAY}-е число M+1",
        "weekly_available_from": f"date + {WEEKLY_LAG_DAYS}",
        "monthly_available_from": "конец M+1",
        "n_months": len(months),
        "n_used_weekly_points": used_points,
        "n_excluded_weekly_points": excluded_points,
        "n_excluded_weekly_dates": len(excluded_dates),
        "n_violations": len(violations),
        "violations": violations[:20],
    }


# ---------------------------------------------------------------------------
# Компонентная панель
# ---------------------------------------------------------------------------


def row_weight(row: dict, weights_matrix: dict, weight_mode: str) -> float:
    if weight_mode == "fixed_2024":
        value = row["weight_2024"]
    else:
        value = row["weight_used"]
    return float(value) if value is not None else 0.0


def build_component_panel(
    product_rows: list,
    official_pp: dict,
    weights_matrix: dict,
    months: list,
    weight_mode: str = "year_specific",
) -> list:
    by_month = defaultdict(list)
    for row in product_rows:
        by_month[row["month"]].append(row)
    panel = []
    for month in months:
        year = (
            DEFAULT_WEIGHTS_YEAR
            if weight_mode == "fixed_2024"
            else weight_year_for(month)
        )
        weights = weights_matrix[year]
        month_rows = [
            row
            for row in by_month.get(month, [])
            if row["component"] in COMPONENTS
        ]
        component_values = {}
        for component in COMPONENTS:
            rows = [row for row in month_rows if row["component"] == component]
            weights_by_row = [
                (row, row_weight(row, weights_matrix, weight_mode))
                for row in rows
            ]
            matched = math.fsum(weight for _, weight in weights_by_row if weight > 0)
            valid = [
                (row, weight)
                for row, weight in weights_by_row
                if row["status"] == "ok" and weight > 0
            ]
            covered = math.fsum(weight for _, weight in valid)
            denominator = weights["denominators"].get(component, 0.0)
            recon = (
                math.fsum(weight * row["mm_pp"] for row, weight in valid)
                / covered
                if covered > 0
                else None
            )
            official = official_pp[OFFICIAL_KEYS[component]].get(month)
            component_values[component] = recon
            panel.append(
                {
                    "month": month,
                    "component": component,
                    "recon_mm_pp": recon,
                    "official_mm_pp": official,
                    "error_pp": (
                        recon - official
                        if recon is not None and official is not None
                        else None
                    ),
                    "weight_year": year,
                    "component_weight": denominator,
                    "matched_weight": matched,
                    "covered_weight": covered,
                    "covered_share": covered / denominator if denominator else 0.0,
                    "n_products": len(valid),
                }
            )

        valid_all = [
            (row, row_weight(row, weights_matrix, weight_mode))
            for row in month_rows
            if row["status"] == "ok"
            and row_weight(row, weights_matrix, weight_mode) > 0
        ]
        covered_all = math.fsum(weight for _, weight in valid_all)
        matched_all = math.fsum(
            weight
            for row, weight in (
                (row, row_weight(row, weights_matrix, weight_mode))
                for row in month_rows
            )
            if weight > 0
        )
        recon_all = (
            math.fsum(weight * row["mm_pp"] for row, weight in valid_all)
            / covered_all
            if covered_all > 0
            else None
        )
        official_all = official_pp["cpi_all"].get(month)
        panel.append(
            {
                "month": month,
                "component": "all_products",
                "recon_mm_pp": recon_all,
                "official_mm_pp": official_all,
                "error_pp": (
                    recon_all - official_all
                    if recon_all is not None and official_all is not None
                    else None
                ),
                "weight_year": year,
                "component_weight": weights["total"],
                "matched_weight": matched_all,
                "covered_weight": covered_all,
                "covered_share": covered_all / weights["total"] if weights["total"] else 0.0,
                "n_products": len(valid_all),
            }
        )

        if all(component_values.get(component) is not None for component in COMPONENTS):
            denominators = weights["denominators"]
            total_component_weight = math.fsum(
                denominators.get(component, 0.0) for component in COMPONENTS
            )
            recon_components = (
                math.fsum(
                    denominators.get(component, 0.0)
                    * component_values[component]
                    for component in COMPONENTS
                )
                / total_component_weight
                if total_component_weight
                else None
            )
        else:
            recon_components = None
        panel.append(
            {
                "month": month,
                "component": "all_components",
                "recon_mm_pp": recon_components,
                "official_mm_pp": official_all,
                "error_pp": (
                    recon_components - official_all
                    if recon_components is not None and official_all is not None
                    else None
                ),
                "weight_year": year,
                "component_weight": weights["total"],
                "matched_weight": math.fsum(
                    row["matched_weight"]
                    for row in panel
                    if row["month"] == month
                    and row["component"] in COMPONENTS
                ),
                "covered_weight": math.fsum(
                    row["covered_weight"]
                    for row in panel
                    if row["month"] == month
                    and row["component"] in COMPONENTS
                ),
                "covered_share": (
                    math.fsum(
                        row["covered_weight"]
                        for row in panel
                        if row["month"] == month
                        and row["component"] in COMPONENTS
                    )
                    / weights["total"]
                    if weights["total"]
                    else 0.0
                ),
                "n_products": sum(
                    row["n_products"]
                    for row in panel
                    if row["month"] == month
                    and row["component"] in COMPONENTS
                ),
            }
        )
    return panel


def series_metrics(
    panel: list, component: str, first: date, last: date
) -> dict:
    rows = [
        row
        for row in panel
        if row["component"] == component and first <= row["month"] <= last
    ]
    evaluation = [row for row in rows if row["official_mm_pp"] is not None]
    errors = [
        row["error_pp"] for row in evaluation if row["error_pp"] is not None
    ]
    if errors:
        absolute = [abs(value) for value in errors]
        metrics = {
            "n": len(errors),
            "rmse_pp": math.sqrt(
                math.fsum(value * value for value in errors) / len(errors)
            ),
            "mae_pp": math.fsum(absolute) / len(absolute),
            "max_abs_pp": max(absolute),
            "bias_pp": math.fsum(errors) / len(errors),
        }
    else:
        metrics = {
            "n": 0,
            "rmse_pp": None,
            "mae_pp": None,
            "max_abs_pp": None,
            "bias_pp": None,
        }
    metrics["n_months"] = len(evaluation)
    metrics["mean_covered_share"] = (
        math.fsum(row["covered_share"] for row in evaluation) / len(evaluation)
        if evaluation
        else None
    )
    metrics["mean_products"] = (
        math.fsum(row["n_products"] for row in evaluation) / len(evaluation)
        if evaluation
        else None
    )
    return metrics


def reconstruction_metrics(panel: list) -> dict:
    periods = {}
    for name, (first, last) in PERIODS.items():
        periods[name] = {
            component: series_metrics(panel, component, first, last)
            for component in SERIES_ORDER
        }
    by_year = []
    for year in range(2009, 2027):
        first = date(year, 1, 1)
        last = date(year, 12, 1)
        if not any(
            row["component"] == "all_components" and first <= row["month"] <= last
            for row in panel
        ):
            continue
        by_year.append(
            {
                "year": year,
                "metrics": {
                    component: series_metrics(panel, component, first, last)
                    for component in SERIES_ORDER
                },
            }
        )
    return {"periods": periods, "by_year": by_year}


def aggregate_validation(
    aggregate_points: list, official_pp: dict
) -> dict:
    by_month = defaultdict(list)
    for day, value in aggregate_points:
        by_month[audit.month_start(day)].append((day, value))
    official_index = {
        month: 1.0 + value / 100.0 for month, value in official_pp.items()
    }
    common = sorted(set(by_month) & set(official_index))
    errors = [
        (audit.compound_month(by_month[month]) - official_index[month]) * 100.0
        for month in common
    ]
    if errors:
        absolute = [abs(value) for value in errors]
        result = {
            "n_months": len(errors),
            "min_month": common[0].isoformat(),
            "max_month": common[-1].isoformat(),
            "mae_pp": math.fsum(absolute) / len(absolute),
            "rmse_pp": math.sqrt(
                math.fsum(value * value for value in errors) / len(errors)
            ),
            "max_abs_pp": max(absolute),
            "mean_pp": math.fsum(errors) / len(errors),
        }
    else:
        result = {"n_months": 0}
    result["expected_task1"] = dict(TASK1_AGGREGATE)
    result["delta_vs_task1"] = {
        key: (
            result[key] - TASK1_AGGREGATE[key]
            if key in result and key in TASK1_AGGREGATE
            else None
        )
        for key in ("mae_pp", "rmse_pp", "max_abs_pp")
    }
    result["matches_task1"] = (
        result.get("n_months") == TASK1_AGGREGATE["n_months"]
        and all(
            result.get(key) is not None
            and abs(result[key] - TASK1_AGGREGATE[key]) < 1e-3
            for key in ("mae_pp", "rmse_pp", "max_abs_pp")
        )
    )
    return result


def task1_diagnostic_comparison(panel: list) -> dict:
    first, last = TASK1_DIAGNOSTIC_PERIOD
    metrics = series_metrics(panel, "all_products", first, last)
    return {
        "period": [first.isoformat(), last.isoformat()],
        "metrics": metrics,
        "expected_task1": dict(TASK1_DIAGNOSTIC),
        "delta_vs_task1": {
            key: (
                metrics[key] - TASK1_DIAGNOSTIC[key]
                if metrics.get(key) is not None
                else None
            )
            for key in ("mae_pp", "rmse_pp", "max_abs_pp")
        },
    }


def decision(coverage_2024: float, component_coverage: dict) -> dict:
    gate_all_weights = coverage_2024 >= COVERAGE_GATE
    gate_all_components = all(
        component_coverage.get(component, 0.0) >= COVERAGE_GATE
        for component in COMPONENTS
    )
    bottomup_full = gate_all_weights and gate_all_components
    return {
        "coverage_gate": COVERAGE_GATE,
        "coverage_2024": coverage_2024,
        "coverage_by_component_2024": component_coverage,
        "gate_all_weights": gate_all_weights,
        "gate_all_components": gate_all_components,
        "bottomup_full": bottomup_full,
        "mode": "bottomup_full" if bottomup_full else "hybrid_4_components_plus_features",
        "partial_bottomup": "allowed" if bottomup_full else "robustness_only",
        "reason": (
            f"Покрытие весов {coverage_2024:.3%} "
            f"{'≥' if gate_all_weights else '<'} гейта {COVERAGE_GATE:.0%}; "
            f"услуги {component_coverage.get('services', 0.0):.1%}; "
            + (
                "полный 88-групповой bottom-up разрешён"
                if bottomup_full
                else "полный 88-групповой bottom-up на публичных данных не заявляется"
            )
        ),
    }


# ---------------------------------------------------------------------------
# Сводки, паспорт, артефакты
# ---------------------------------------------------------------------------


def month_span(first: date, last: date) -> int:
    return (last.year - first.year) * 12 + (last.month - first.month) + 1


def panel_summary(product_rows: list, months: list, matched_rows: list) -> dict:
    counts = Counter(row["status"] for row in product_rows)
    valid = counts.get("ok", 0)
    first_by_series = {}
    last_by_series = {}
    for row in product_rows:
        if row["n_weeks"] <= 0:
            continue
        series = row["series"]
        if series not in first_by_series or row["month"] < first_by_series[series]:
            first_by_series[series] = row["month"]
        if series not in last_by_series or row["month"] > last_by_series[series]:
            last_by_series[series] = row["month"]
    active_cells = sum(
        month_span(first, last_by_series[series])
        for series, first in first_by_series.items()
    )
    active_ok = sum(
        1
        for row in product_rows
        if row["status"] == "ok"
        and first_by_series.get(row["series"]) is not None
        and first_by_series[row["series"]]
        <= row["month"]
        <= last_by_series[row["series"]]
    )
    by_year = []
    for year in range(2009, 2027):
        rows = [row for row in product_rows if row["month"].year == year]
        if not rows:
            continue
        ok = sum(1 for row in rows if row["status"] == "ok")
        mapped_ok = sum(
            1
            for row in rows
            if row["status"] == "ok" and row["component"] in COMPONENTS
        )
        by_year.append(
            {
                "year": year,
                "cells": len(rows),
                "ok": ok,
                "ok_share": ok / len(rows),
                "mapped_ok": mapped_ok,
                "mapped_ok_share": mapped_ok
                / sum(1 for row in rows if row["component"] in COMPONENTS)
                if any(row["component"] in COMPONENTS for row in rows)
                else 0.0,
            }
        )
    return {
        "months": {
            "first": months[0].isoformat() if months else None,
            "last": months[-1].isoformat() if months else None,
            "n": len(months),
        },
        "products": {
            "n_total": len({row["series"] for row in product_rows}),
            "n_mapped": len({row["series"] for row in matched_rows}),
            "n_unmapped": len(
                {row["series"] for row in product_rows}
                - {row["series"] for row in matched_rows}
            ),
        },
        "cells": {
            "total": len(product_rows),
            "ok": valid,
            "ok_share": valid / len(product_rows) if product_rows else 0.0,
            "active_span_cells": active_cells,
            "active_span_ok": active_ok,
            "active_span_ok_share": active_ok / active_cells if active_cells else 0.0,
            "status_counts": {
                status: counts.get(status, 0) for status in STATUSES
            },
        },
        "by_year": by_year,
    }


def component_coverage_summary(panel: list) -> dict:
    result = {}
    for name, (first, last) in PERIODS.items():
        result[name] = {}
        for component in SERIES_ORDER:
            rows = [
                row
                for row in panel
                if row["component"] == component
                and first <= row["month"] <= last
                and row["official_mm_pp"] is not None
            ]
            result[name][component] = {
                "n_months": len(rows),
                "mean_covered_share": (
                    math.fsum(row["covered_share"] for row in rows) / len(rows)
                    if rows
                    else None
                ),
                "mean_products": (
                    math.fsum(row["n_products"] for row in rows) / len(rows)
                    if rows
                    else None
                ),
            }
    return result


def canonical_json(value) -> str:
    return json.dumps(
        json_safe(value),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def sha256_text(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def git_info() -> dict:
    try:
        sha = subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "HEAD"], text=True
        ).strip()
        branch = subprocess.check_output(
            ["git", "-C", str(ROOT), "rev-parse", "--abbrev-ref", "HEAD"],
            text=True,
        ).strip()
        dirty = bool(
            subprocess.check_output(
                ["git", "-C", str(ROOT), "status", "--porcelain"], text=True
            ).strip()
        )
        return {"sha": sha, "branch": branch, "dirty": int(dirty)}
    except (OSError, subprocess.CalledProcessError):
        return {"sha": "", "branch": "", "dirty": 0}


def build_passport(
    config: dict,
    product_rows: list,
    config_hash: str,
    sample_hash: str,
    data_asof: Optional[date],
    max_official_month: Optional[date],
    artifacts: list,
    status: str,
) -> dict:
    run_id = str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"cpi-weekly-component-panel:{config_hash}:{sample_hash}",
        )
    )
    spec_path = (
        ROOT
        / "docs/superpowers/specs/2026-10-19-cpi-weekly-component-panel-design.md"
    )
    plan_path = (
        ROOT
        / "docs/superpowers/plans/2026-10-19-cpi-weekly-component-panel.md"
    )
    return {
        "run_id": run_id,
        "experiment": "cpi_weekly_component_panel",
        "run_name": "weekly_panel__weights-yearly__min3__gap14",
        "status": status,
        "git": git_info(),
        "config_hash": config_hash,
        "sample_hash": sample_hash,
        "data_asof": iso(data_asof),
        "max_official_month": iso(max_official_month),
        "n_sample_rows": len(product_rows),
        "spec_path": str(spec_path.relative_to(ROOT)),
        "spec_sha256": audit.sha256_file(spec_path) if spec_path.exists() else "",
        "plan_path": str(plan_path.relative_to(ROOT)),
        "plan_sha256": audit.sha256_file(plan_path) if plan_path.exists() else "",
        "environment": {
            "python": platform.python_version(),
            "openpyxl": getattr(__import__("openpyxl"), "__version__", ""),
        },
        "artifacts": artifacts,
        "ml_log": "disabled: модель не обучается; паспорт данных — JSON, в ClickHouse не пишем",
    }


def json_safe(value):
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def artifact_entry(path: Path) -> dict:
    return {
        "name": str(path.relative_to(ROOT)) if path.is_relative_to(ROOT) else str(path),
        "sha256": audit.sha256_file(path),
        "bytes": path.stat().st_size,
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(json_safe(payload), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )


def write_products_csv(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=PRODUCT_COLUMNS, lineterminator="\n"
        )
        writer.writeheader()
        for row in sorted(rows, key=lambda item: (item["month"], item["series"])):
            writer.writerow(
                {
                    "month": iso(row["month"]),
                    "series": row["series"],
                    "component": row["component"] or "",
                    "group_code": row["group_code"] or "",
                    "group_name": row["group_name"] or "",
                    "weight_code": row["weight_code"] or "",
                    "weight_2024": number(row["weight_2024"]),
                    "weight_used": number(row["weight_used"]),
                    "weight_year": row["weight_year"],
                    "status": row["status"],
                    "n_weeks": row["n_weeks"],
                    "first_week": iso(row["first_week"]) or "",
                    "last_week": iso(row["last_week"]) or "",
                    "prev_week": iso(row["prev_week"]) or "",
                    "max_gap_days": (
                        row["max_gap_days"] if row["max_gap_days"] is not None else ""
                    ),
                    "mm_pp": number(row["mm_pp"]),
                }
            )


def write_components_csv(path: Path, rows: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=COMPONENT_COLUMNS, lineterminator="\n"
        )
        writer.writeheader()
        order = {name: index for index, name in enumerate(SERIES_ORDER)}
        for row in sorted(
            rows, key=lambda item: (item["month"], order[item["component"]])
        ):
            writer.writerow(
                {
                    "month": iso(row["month"]),
                    "component": row["component"],
                    "recon_mm_pp": number(row["recon_mm_pp"]),
                    "official_mm_pp": number(row["official_mm_pp"]),
                    "error_pp": number(row["error_pp"]),
                    "weight_year": row["weight_year"],
                    "component_weight": number(row["component_weight"]),
                    "matched_weight": number(row["matched_weight"]),
                    "covered_weight": number(row["covered_weight"]),
                    "covered_share": number(row["covered_share"]),
                    "n_products": row["n_products"],
                }
            )


def number(value, digits: int = 6) -> str:
    if value is None:
        return ""
    return f"{float(value):.{digits}f}"


def fmt(value, digits: int = 6) -> str:
    return "—" if value is None else f"{value:.{digits}f}"


def pct(value) -> str:
    return "—" if value is None else f"{value:.3%}"


def report_table(headers: list, rows: list) -> list:
    lines = [
        "| " + " | ".join(headers) + " |",
        "|" + "|".join("---" for _ in headers) + "|",
    ]
    lines.extend("| " + " | ".join(str(cell) for cell in row) + " |" for row in rows)
    return lines


def write_report(path: Path, payload: dict) -> None:
    decision = payload["decision"]
    periods = payload["reconstruction"]["periods"]
    status = payload["status"]
    lines = [
        "# Недельная компонентная панель ИПЦ (2026-10-19)",
        "",
        "> **Объект:** Task 3 пункта `key-rate/phase-4-ml-components` "
        "(issue [#236](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/236), "
        "родитель [#76](https://github.com/kmlebedev/clickhouse-import-rosstat/issues/76)).",
        "> **Задача:** собрать месячный m/m товаров из публичных недельных индексов "
        "Росстата, реконструировать `food`/`nonfood`/`services` и общий ИПЦ, "
        "сравнить с `rosstat_ipc_mes`; гейт покрытия весов ≥50 % решает судьбу bottom-up.",
        "> **Как проверялось:** `scripts/models/cpi/cpi_weekly_component_panel.py` "
        "(цель `make cpi-weekly-panel`), офлайн-тесты "
        "`scripts/models/cpi/cpi_weekly_component_panel_test.py`; данные — `v_rosstat_macro` "
        "(148 товарных W-рядов + агрегат), 4 официальных ряда `rosstat_ipc_mes`, "
        "веса/словарь из архива CB_COMP. Скрипт только читает ClickHouse, в БД не пишет.",
        "> **Спека:** [2026-10-19-cpi-weekly-component-panel-design.md]"
        "(../superpowers/specs/2026-10-19-cpi-weekly-component-panel-design.md); "
        "**план:** [2026-10-19-cpi-weekly-component-panel.md]"
        "(../superpowers/plans/2026-10-19-cpi-weekly-component-panel.md); "
        "**артефакты:** `2026-10-19-cpi-weekly-component-panel-{products,components}.csv`, "
        "`2026-10-19-cpi-weekly-component-panel.json`.",
        "",
        f"**Статус: `{status}`.** "
        f"Гейт покрытия весов: {pct(decision['coverage_2024'])} при пороге "
        f"{pct(decision['coverage_gate'])}; режим — **`{decision['mode']}`**. "
        "3-компонентная панель — частичная реконструкция, не полный 88-групповой "
        "bottom-up и не замена официальных компонент.",
        "",
        "---",
        "",
        "## 1. Короткий ответ",
        "",
    ]
    dense = periods["dense_2022"]
    all_components = dense["all_components"]
    all_products = dense["all_products"]
    services = dense["services"]
    lines.extend(
        [
            f"1. **Mapping:** {payload['mapping']['matched_series']} из "
            f"{payload['mapping']['n_product_series']} товарных рядов сопоставлены "
            f"с весами; {payload['mapping']['unmatched_series']} — нет. "
            f"Покрытие весов 2024 — **{pct(decision['coverage_2024'])}** "
            f"(food {pct(decision['coverage_by_component_2024']['food'])}, "
            f"nonfood {pct(decision['coverage_by_component_2024']['nonfood'])}, "
            f"services {pct(decision['coverage_by_component_2024']['services'])}).",
            f"2. **Панель:** {payload['panel']['cells']['total']} ячеек "
            f"товар×месяц; `ok` — {payload['panel']['cells']['ok']} "
            f"({pct(payload['panel']['cells']['ok_share'])}); в активных периодах "
            f"серий — {payload['panel']['cells']['active_span_ok']} из "
            f"{payload['panel']['cells']['active_span_cells']} "
            f"({pct(payload['panel']['cells']['active_span_ok_share'])}). "
            "Остальные — NaN по правилам полноты (`no_points`/`no_previous`/"
            "`lt_min_weeks`/`incomplete_month`/`gap_too_large`). 2026-10 неполный "
            "(2 недели), 2026-01 — `incomplete_month` (первая точка 12-го); "
            "в dense-периоде 56 из 57 месяцев.",
            f"3. **Реконструкция `dense_2022` (2022-01…2026-09):** "
            f"`all_components` RMSE **{fmt(all_components['rmse_pp'])}** п.п., "
            f"MAE {fmt(all_components['mae_pp'])}, max {fmt(all_components['max_abs_pp'])}, "
            f"n={all_components['n']}; `all_products` RMSE {fmt(all_products['rmse_pp'])}, "
            f"MAE {fmt(all_products['mae_pp'])}; `services` RMSE "
            f"{fmt(services['rmse_pp'])}, MAE {fmt(services['mae_pp'])} "
            f"(покрытие услуг — {pct(decision['coverage_by_component_2024']['services'])}).",
            f"4. **Сверка компаундирования:** агрегатный W-ряд 2009-04…2021-12 — "
            f"{payload['aggregate_validation']['n_months']} месяцев, MAE "
            f"{fmt(payload['aggregate_validation'].get('mae_pp'))} п.п., "
            f"RMSE {fmt(payload['aggregate_validation'].get('rmse_pp'))}, "
            f"max {fmt(payload['aggregate_validation'].get('max_abs_pp'))} "
            f"(Task 1: MAE {fmt(TASK1_AGGREGATE['mae_pp'])}, "
            f"RMSE {fmt(TASK1_AGGREGATE['rmse_pp'])}, "
            f"max {fmt(TASK1_AGGREGATE['max_abs_pp'])}); "
            f"совпадение — {payload['aggregate_validation']['matches_task1']}.",
            f"5. **Гейт:** `bottomup_full={str(decision['bottomup_full']).lower()}`; "
            f"`gate_all_weights={str(decision['gate_all_weights']).lower()}`, "
            f"`gate_all_components={str(decision['gate_all_components']).lower()}`. "
            f"Режим — **`{decision['mode']}`**, частичный bottom-up — "
            f"`{decision['partial_bottomup']}`. {decision['reason']}.",
            "6. **Ограничения:** панель покрывает 39,3 % весов, услуги — 29,0 %; "
            "винтажей нет, as-of — правило cutoff; официальные и недельные значения "
            "округлены. Полный 88-групповой паритет и сравнение с CB_COMP — не Task 3.",
            "",
            "---",
            "",
            "## 2. Входы и mapping",
            "",
        ]
    )
    lines.extend(
        report_table(
            ["Компонент", "Сопоставлено кодов", "Вес 2024", "Покрытие 2024"],
            [
                [
                    COMPONENT_LABELS[component],
                    payload["mapping"]["by_component"][component]["matched_codes"],
                    fmt(payload["mapping"]["by_component"][component]["numerator"], 5),
                    pct(payload["mapping"]["by_component"][component]["coverage"]),
                ]
                for component in COMPONENTS
            ]
            + [
                [
                    "Всего",
                    payload["mapping"]["matched_series"],
                    fmt(decision["coverage_2024"], 5),
                    pct(decision["coverage_2024"]),
                ]
            ],
        )
    )
    lines.extend(
        [
            "",
            f"Несопоставленных рядов — {payload['mapping']['unmatched_series']}; "
            "они остаются в product CSV как диагностика, но не входят в компонентную "
            "реконструкцию. Полный список — в JSON (`mapping.unmatched`).",
            "",
            "Покрытие по годам (те же 118 кодов, веса соответствующего года):",
            "",
        ]
    )
    lines.extend(
        report_table(
            ["Год", "Сопоставлено кодов", "Покрытие"],
            [
                [item["year"], item["matched_codes"], pct(item["coverage"])]
                for item in payload["mapping"]["coverage_by_year"]
            ],
        )
    )
    lines.extend(
        [
            "",
            "## 3. Метод",
            "",
            "- Недельное значение — индекс «% к предыдущей дате регистрации»; "
            "месячный m/m — `Π(value/100) − 1` по неделям, зарегистрированным в месяце.",
            "- Полнота месяца: `n_weeks ≥ 3`, первая точка не позже 10-го числа, "
            "последняя — не раньше чем за 10 дней до конца месяца, максимальный "
            "разрыв ≤21 дня; праздничный сдвиг 15–17 дней допускается, иначе NaN.",
            "- Месяц — по дате регистрации недели, а не по началу интервала; "
            "это не точный календарный месяц (ошибка измеряется сверкой).",
            "- Веса — годовые 2016–2024 (2013–2015 → 2016, 2025–2026 → 2024); "
            "fixed-2024 — робастность.",
            "- As-of: недельная точка `date + 2`, cutoff 8-е M+1; официальная метка "
            "M доступна не раньше конца M+1 и используется только для сверки.",
            f"- Проверка as-of: {payload['as_of']['n_violations']} нарушений; "
            f"использовано {payload['as_of']['n_used_weekly_points']} недельных точек, "
            f"исключено {payload['as_of']['n_excluded_weekly_points']} "
            f"({payload['as_of']['n_excluded_weekly_dates']} уникальных дат) "
            "с доступностью после cutoff.",
            "",
            "## 4. Панель и покрытие",
            "",
        ]
    )
    lines.extend(
        report_table(
            ["Статус", "Ячеек", "Доля"],
            [
                [
                    status_name,
                    payload["panel"]["cells"]["status_counts"][status_name],
                    pct(
                        payload["panel"]["cells"]["status_counts"][status_name]
                        / payload["panel"]["cells"]["total"]
                    )
                    if payload["panel"]["cells"]["total"]
                    else "—",
                ]
                for status_name in STATUSES
            ],
        )
    )
    lines.extend(
        [
            "",
            "Динамическое покрытие (средняя доля весов компонента с валидным m/m "
            "по месяцам периода):",
            "",
        ]
    )
    coverage_rows = []
    for name in PERIODS:
        for component in SERIES_ORDER:
            item = payload["coverage"]["periods"][name][component]
            coverage_rows.append(
                [
                    name,
                    SERIES_LABELS[component],
                    item["n_months"],
                    pct(item["mean_covered_share"]),
                    fmt(item["mean_products"], 1),
                ]
            )
    lines.extend(
        report_table(
            ["Период", "Ряд", "Месяцев", "Ср. покрытие", "Ср. товаров"],
            coverage_rows,
        )
    )
    lines.extend(
        [
            "",
            "Покрытие считается по всем месяцам периода с официальной меткой "
            "(57 в `dense_2022`), включая месяцы, где реконструкция NaN; `n` "
            "в таблице реконструкции — месяцы с ненулевой ошибкой.",
            "",
            "## 5. Реконструкция",
            "",
            "Ошибка — `recon − official`, п.п. м/м; NaN-месяцы не импутируются.",
            "",
        ]
    )
    reconstruction_rows = []
    for name in PERIODS:
        for component in SERIES_ORDER:
            item = periods[name][component]
            reconstruction_rows.append(
                [
                    name,
                    SERIES_LABELS[component],
                    item["n"],
                    fmt(item["rmse_pp"]),
                    fmt(item["mae_pp"]),
                    fmt(item["max_abs_pp"]),
                    fmt(item["bias_pp"]),
                    pct(item["mean_covered_share"]),
                ]
            )
    lines.extend(
        report_table(
            ["Период", "Ряд", "n", "RMSE", "MAE", "max abs", "bias", "Покрытие"],
            reconstruction_rows,
        )
    )
    lines.extend(
        [
            "",
            "`all_components` считается только при валидных всех трёх компонентах: "
            f"в `dense_2022` — {periods['dense_2022']['all_components']['n']} месяцев, "
            f"в `model_2013_2026_08` — "
            f"{periods['model_2013_2026_08']['all_components']['n']}.",
            "",
            "По годам (только `all_components` и `services`; полная таблица — в JSON):",
            "",
        ]
    )
    year_rows = []
    for item in payload["reconstruction"]["by_year"]:
        metrics = item["metrics"]
        year_rows.append(
            [
                item["year"],
                metrics["all_components"]["n"],
                fmt(metrics["all_components"]["rmse_pp"]),
                fmt(metrics["all_components"]["mae_pp"]),
                fmt(metrics["services"]["rmse_pp"]),
                pct(metrics["all_components"]["mean_covered_share"]),
            ]
        )
    lines.extend(
        report_table(
            ["Год", "n", "RMSE all", "MAE all", "RMSE services", "Покрытие all"],
            year_rows,
        )
    )
    lines.extend(
        [
            "",
            "Робастность fixed-2024 (те же месяцы, веса 2024 для всех лет):",
            "",
        ]
    )
    fixed_rows = []
    for name in PERIODS:
        for component in SERIES_ORDER:
            item = payload["reconstruction"]["fixed_2024"]["periods"][name][component]
            fixed_rows.append(
                [
                    name,
                    SERIES_LABELS[component],
                    item["n"],
                    fmt(item["rmse_pp"]),
                    fmt(item["mae_pp"]),
                    fmt(item["max_abs_pp"]),
                ]
            )
    lines.extend(
        report_table(
            ["Период", "Ряд", "n", "RMSE", "MAE", "max abs"],
            fixed_rows,
        )
    )
    lines.extend(
        [
            "",
            "## 6. Сверка компаундирования",
            "",
        ]
    )
    aggregate = payload["aggregate_validation"]
    lines.extend(
        report_table(
            ["Метрика", "Панель", "Task 1", "Δ"],
            [
                [
                    "Месяцев",
                    aggregate["n_months"],
                    TASK1_AGGREGATE["n_months"],
                    aggregate["n_months"] - TASK1_AGGREGATE["n_months"],
                ],
                [
                    "MAE, п.п.",
                    fmt(aggregate.get("mae_pp")),
                    fmt(TASK1_AGGREGATE["mae_pp"]),
                    fmt(aggregate["delta_vs_task1"].get("mae_pp")),
                ],
                [
                    "RMSE, п.п.",
                    fmt(aggregate.get("rmse_pp")),
                    fmt(TASK1_AGGREGATE["rmse_pp"]),
                    fmt(aggregate["delta_vs_task1"].get("rmse_pp")),
                ],
                [
                    "max abs, п.п.",
                    fmt(aggregate.get("max_abs_pp")),
                    fmt(TASK1_AGGREGATE["max_abs_pp"]),
                    fmt(aggregate["delta_vs_task1"].get("max_abs_pp")),
                ],
            ],
        )
    )
    diagnostic = payload["reconstruction"]["task1_diagnostic"]
    lines.extend(
        [
            "",
            f"Диагностика Task 1 для `all_products` 2022-01…2026-08: "
            f"MAE {fmt(diagnostic['metrics']['mae_pp'])} против "
            f"{fmt(diagnostic['expected_task1']['mae_pp'])} "
            f"(Δ {fmt(diagnostic['delta_vs_task1']['mae_pp'])}), "
            f"RMSE {fmt(diagnostic['metrics']['rmse_pp'])} против "
            f"{fmt(diagnostic['expected_task1']['rmse_pp'])}. Отличия ожидаемы: "
            "годовые веса вместо фиксированных 2024 и другое правило полноты "
            "(границы месяца и разрыв 21 день).",
            "",
            "## 7. Гейт и решение",
            "",
            f"- Покрытие 2024: **{pct(decision['coverage_2024'])}**; "
            f"компоненты: food {pct(decision['coverage_by_component_2024']['food'])}, "
            f"nonfood {pct(decision['coverage_by_component_2024']['nonfood'])}, "
            f"services {pct(decision['coverage_by_component_2024']['services'])}.",
            f"- `gate_all_weights={str(decision['gate_all_weights']).lower()}`, "
            f"`gate_all_components={str(decision['gate_all_components']).lower()}`.",
            f"- **Решение:** `{decision['mode']}`; частичный bottom-up — "
            f"`{decision['partial_bottomup']}`. {decision['reason']}.",
            "- 3-компонентная панель — частичная реконструкция; полный 88-групповой "
            "bottom-up и паритет с CB_COMP не заявляются.",
            "",
            "## 8. Ограничения и что не проверено",
            "",
            "- 39,3 % весов: полный 88-групповой bottom-up не построен; услуги — 29,0 %.",
            "- Веса только 2016–2024; для 2025–2026 используется 2024, для 2013–2015 — 2016.",
            "- Недельная панель разрежена: 122 из 148 товарных рядов имеют разрыв >14 дней; "
            "месяцы с <3 неделями, первой/последней точкой далеко от границ или "
            "разрывом >21 дня — NaN.",
            "- Месячный m/m — цепочка недель по дате регистрации, а не точный календарный "
            "месяц; ошибка аппроксимации измерена сверкой агрегата.",
            "- Винтажей/ревизий нет: as-of — правило cutoff, а не фактические даты публикаций.",
            "- Официальные и недельные значения округлены; ошибка включает округление.",
            "- Task 2 (отраслевые МП), Task 4 (модель v3) и Task 5 (сравнение с CB_COMP) "
            "в этот PR не входят.",
            "- MLflow/`ML_LOG` не используются: модель не обучается; паспорт данных — JSON.",
            "",
            "## 9. Воспроизводимость",
            "",
            "```bash",
            "make cpi-weekly-panel",
            "make cpi-weekly-panel-test",
            "make all",
            "make docs-check",
            "```",
            "",
            f"Паспорт: `run_id={payload['passport']['run_id']}`, "
            f"`config_hash={payload['passport']['config_hash']}`, "
            f"`sample_hash={payload['passport']['sample_hash']}`. "
            "CSV/JSON пишутся в `docs/research/`; повтор даёт те же числа и CSV.",
            "",
        ]
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def build_payload(
    archive: Path,
    products: dict,
    aggregate: list,
    monthly_pp: dict,
    min_weeks: int,
    max_gap_days: int,
) -> dict:
    weights = list(cached_weights(str(archive), DEFAULT_WEIGHTS_YEAR))
    dictionary = cached_dictionary(str(archive), DEFAULT_WEIGHTS_YEAR)
    mapping = audit.build_mapping(
        archive, sorted(products), weights, dictionary
    )
    matched_rows = mapping["matched_rows"]
    weights_matrix = build_weights_matrix(archive, matched_rows)

    weekly_dates = [day for points in products.values() for day, _ in points]
    months = audit.sample_months(min(weekly_dates), max(weekly_dates))
    product_rows = build_product_panel(
        products, matched_rows, weights_matrix, months, min_weeks, max_gap_days
    )
    component_panel = build_component_panel(
        product_rows, monthly_pp, weights_matrix, months, "year_specific"
    )
    fixed_matrix = {
        year: weights_matrix[DEFAULT_WEIGHTS_YEAR] for year in WEIGHT_YEARS
    }
    fixed_panel = build_component_panel(
        product_rows, monthly_pp, fixed_matrix, months, "fixed_2024"
    )

    component_coverage = {
        component: (
            mapping["coverage_by_component_2024"][component]["coverage"] or 0.0
        )
        for component in COMPONENTS
    }
    coverage = mapping["coverage_2024"]["coverage"] or 0.0
    as_of = as_of_check(products, monthly_pp, months)
    max_official_month = max(monthly_pp["cpi_all"])
    config = {
        "min_weeks": min_weeks,
        "max_gap_days": max_gap_days,
        "max_first_offset_days": MAX_FIRST_OFFSET_DAYS,
        "max_last_offset_days": MAX_LAST_OFFSET_DAYS,
        "cutoff_day": CUTOFF_DAY,
        "weekly_lag_days": WEEKLY_LAG_DAYS,
        "weight_mode": "year_specific_nearest",
        "weight_years": list(WEIGHT_YEARS),
        "default_weights_year": DEFAULT_WEIGHTS_YEAR,
        "coverage_gate": COVERAGE_GATE,
        "periods": {
            name: [first.isoformat(), last.isoformat()]
            for name, (first, last) in PERIODS.items()
        },
    }
    config_hash = "sha256:" + sha256_text(canonical_json(config))
    sample_rows = [
        {
            key: iso(row[key]) if key in {
                "month",
                "first_week",
                "last_week",
                "prev_week",
            } else row[key]
            for key in SAMPLE_KEYS
        }
        for row in product_rows
    ]
    sample_hash = "sha256:" + sha256_text(canonical_json(sample_rows))
    result_decision = decision(coverage, component_coverage)

    concerns = []
    if not result_decision["bottomup_full"]:
        concerns.append(
            f"gate: покрытие {coverage:.3%} < {COVERAGE_GATE:.0%}; "
            f"режим {result_decision['mode']}"
        )
    if len(matched_rows) != TASK1_MATCHED:
        concerns.append(
            f"mapping: {len(matched_rows)} совпадений вместо {TASK1_MATCHED}"
        )
    if abs(coverage - TASK1_COVERAGE_2024) > 1e-6:
        concerns.append(
            f"coverage 2024 {coverage:.6f} вместо {TASK1_COVERAGE_2024:.6f}"
        )
    for component, expected in TASK1_COMPONENT_COVERAGE.items():
        if abs(component_coverage[component] - expected) > 1e-6:
            concerns.append(
                f"coverage {component} {component_coverage[component]:.6f} "
                f"вместо {expected:.6f}"
            )
    aggregate_result = aggregate_validation(aggregate, monthly_pp["cpi_all"])
    if not aggregate_result["matches_task1"]:
        concerns.append(
            "aggregate validation отличается от Task 1: "
            f"MAE {aggregate_result.get('mae_pp')} против "
            f"{TASK1_AGGREGATE['mae_pp']}"
        )
    if as_of["n_violations"]:
        concerns.append(f"as-of: {as_of['n_violations']} нарушений")
    if not any(
        row["component"] == "all_components" and row["error_pp"] is not None
        for row in component_panel
    ):
        concerns.append("реконструкция all_components пуста")

    status = "DONE" if result_decision["bottomup_full"] else "DONE_WITH_CONCERNS"
    if not any(
        row["component"] == "all_components" and row["error_pp"] is not None
        for row in component_panel
    ):
        status = "BLOCKED"

    return {
        "schema": SCHEMA,
        "generated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "git": git_info(),
        "config": config,
        "inputs": {
            "archive": archive.name,
            "weekly": {
                "sql": "v_rosstat_macro + v_series_catalog (rosstat/W)",
                "n_catalog_series": len(products) + 1,
                "n_product_series": len(products),
                "aggregate_series": audit.WEEKLY_AGGREGATE,
            },
            "monthly": {
                "source": "rosstat_ipc_mes via v_rosstat_macro",
                "series": list(audit.MONTHLY_SERIES.values()),
            },
            "weights": {
                "path": audit.display_path(
                    audit.weights_path(archive), archive
                ),
                "sha256": audit.sha256_file(audit.weights_path(archive)),
                "default_year": DEFAULT_WEIGHTS_YEAR,
                "years": list(WEIGHT_YEARS),
            },
            "dictionary": {
                "path": audit.display_path(
                    audit.dictionary_path(archive), archive
                ),
                "sha256": audit.sha256_file(audit.dictionary_path(archive)),
                "default_year": DEFAULT_WEIGHTS_YEAR,
            },
        },
        "mapping": {
            "normalizer": mapping["normalizer"],
            "n_product_series": len(products),
            "matched_series": len(matched_rows),
            "unmatched_series": len(mapping["unmatched"]),
            "matched_weight_codes": mapping["matched_weight_codes"],
            "by_component": mapping["coverage_by_component_2024"],
            "coverage_2024": mapping["coverage_2024"],
            "coverage_by_year": coverage_by_year(weights_matrix, matched_rows),
            "matched_rows": matched_rows,
            "unmatched": mapping["unmatched"],
            "weights_total_2024": mapping["weights_total_2024"],
        },
        "panel": panel_summary(product_rows, months, matched_rows),
        "coverage": {
            "periods": component_coverage_summary(component_panel),
            "fixed_2024": component_coverage_summary(fixed_panel),
        },
        "as_of": as_of,
        "aggregate_validation": aggregate_result,
        "reconstruction": {
            **reconstruction_metrics(component_panel),
            "fixed_2024": reconstruction_metrics(fixed_panel),
            "task1_diagnostic": task1_diagnostic_comparison(component_panel),
            "component_panel": [
                {
                    **{key: value for key, value in row.items()},
                }
                for row in component_panel
            ],
        },
        "decision": result_decision,
        "status": status,
        "concerns": concerns,
        "limitations": [
            "39,3 % весов: полный 88-групповой bottom-up не построен; услуги 29,0 %.",
            "Веса 2016–2024; 2013–2015 → 2016, 2025–2026 → 2024; месячной привязки весов нет.",
            "Недельная панель разрежена; месяцы с <3 неделями, далёкой от границ "
            "первой/последней точкой или разрывом >21 дня — NaN.",
            "Месячный m/m — цепочка недель по дате регистрации, не точный календарный месяц.",
            "Винтажей/ревизий нет; as-of — правило cutoff, не фактические даты публикаций.",
            "Официальные и недельные значения округлены; ошибка включает округление.",
        ],
        "passport": build_passport(
            config,
            product_rows,
            config_hash,
            sample_hash,
            max(weekly_dates),
            max_official_month,
            [],
            status,
        ),
        "product_rows": product_rows,
        "component_panel": component_panel,
    }


def run(
    archive: Optional[Path],
    json_path: Path,
    report_path: Path,
    products_csv: Path,
    components_csv: Path,
    min_weeks: int = MIN_WEEKS,
    max_gap_days: int = MAX_GAP_DAYS,
) -> dict:
    archive = archive or audit.resolve_archive()
    products, aggregate = load_weekly_products()
    monthly_pp, _ = load_monthly_components()
    payload = build_payload(
        archive, products, aggregate, monthly_pp, min_weeks, max_gap_days
    )
    product_rows = payload.pop("product_rows")
    component_panel = payload.pop("component_panel")
    write_products_csv(products_csv, product_rows)
    write_components_csv(components_csv, component_panel)
    artifacts = [
        artifact_entry(products_csv),
        artifact_entry(components_csv),
    ]
    payload["passport"]["artifacts"] = artifacts
    write_report(report_path, payload)
    payload["passport"]["artifacts"].append(artifact_entry(report_path))
    write_json(json_path, payload)
    print(
        f"W: {payload['inputs']['weekly']['n_catalog_series']} "
        f"({payload['inputs']['weekly']['n_product_series']} товарных + агрегат); "
        f"месяцев: {payload['panel']['months']['n']}"
    )
    print(
        f"Mapping: {payload['mapping']['matched_series']} совпадений, "
        f"покрытие {payload['decision']['coverage_2024']:.3%}, "
        f"unmatched {payload['mapping']['unmatched_series']}"
    )
    print(
        f"Панель: {payload['panel']['cells']['ok']} ok из "
        f"{payload['panel']['cells']['total']}; "
        f"as-of нарушений: {payload['as_of']['n_violations']}"
    )
    dense = payload["reconstruction"]["periods"]["dense_2022"]
    print(
        "dense_2022: all_components RMSE "
        f"{fmt(dense['all_components']['rmse_pp'])}, "
        f"MAE {fmt(dense['all_components']['mae_pp'])}; "
        f"services RMSE {fmt(dense['services']['rmse_pp'])}"
    )
    print(
        f"Агрегат: {payload['aggregate_validation']['n_months']} мес., "
        f"MAE {fmt(payload['aggregate_validation'].get('mae_pp'))}; "
        f"Task 1 match={payload['aggregate_validation']['matches_task1']}"
    )
    print(
        f"Решение: {payload['decision']['mode']} "
        f"(bottomup_full={payload['decision']['bottomup_full']})"
    )
    print(
        f"Артефакты: {products_csv}, {components_csv}, {report_path}, {json_path}"
    )
    return payload


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Weekly CPI component panel (issue #236)"
    )
    parser.add_argument("--archive", help="распакованный архив CB_COMP")
    parser.add_argument(
        "--json",
        default=str(
            ROOT / "docs/research/2026-10-19-cpi-weekly-component-panel.json"
        ),
        help="куда записать JSON",
    )
    parser.add_argument(
        "--report",
        default=str(
            ROOT / "docs/research/2026-10-19-cpi-weekly-component-panel.md"
        ),
        help="куда записать отчёт",
    )
    parser.add_argument(
        "--products-csv",
        default=str(
            ROOT
            / "docs/research/2026-10-19-cpi-weekly-component-panel-products.csv"
        ),
        help="product panel CSV",
    )
    parser.add_argument(
        "--components-csv",
        default=str(
            ROOT
            / "docs/research/2026-10-19-cpi-weekly-component-panel-components.csv"
        ),
        help="component panel CSV",
    )
    parser.add_argument("--min-weeks", type=int, default=MIN_WEEKS)
    parser.add_argument("--max-gap-days", type=int, default=MAX_GAP_DAYS)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    run(
        archive=Path(args.archive) if args.archive else None,
        json_path=Path(args.json),
        report_path=Path(args.report),
        products_csv=Path(args.products_csv),
        components_csv=Path(args.components_csv),
        min_weeks=args.min_weeks,
        max_gap_days=args.max_gap_days,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
