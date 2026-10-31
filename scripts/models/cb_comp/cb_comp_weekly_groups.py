#!/usr/bin/env python3
"""Недельный ИПЦ → 88 групп CB_COMP: маппинг и as-of признаки — Task 4 #424.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h1-weekly-design.md` §3–§4.

* **маппинг** — 148 недельных позиций (`v_rosstat_macro`, `source='rosstat'`,
  `frequency='W'`) сопоставляются с листовыми весами 2024 тем же extended-
  нормализатором, что в Task 3 #236, затем лист → 88 групп по иерархии
  `dictionary_cpi.xlsx` (глубочайший предок среди кодов `model.xlsx`, не `l3`);
* **признаки** — `wk_g_mtd` (накопленное недельное изменение месяца M) и
  `wk_g_cov` (доля веса группы, покрытая недельными точками) на cutoff строки;
* **агрегат** — обёртка над `cpi_weekly_aggregate.wkagg_features`.

Только чтение ClickHouse и локального архива CB_COMP; в БД не пишет.
Тесты — `scripts/models/cb_comp/cb_comp_weekly_groups_test.py`.
"""

import math
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_groups_panel as gp
from models.cpi import cpi_public_panel_audit as audit
from models.cpi import cpi_weekly_aggregate as agg

WEEKLY_LAG_DAYS = 2
WEIGHT_YEARS = tuple(range(2016, 2025))
DICTIONARY_YEAR = 2024
NAN = float("nan")


# ---------------------------------------------------------------------------
# Годы весов и окно месяца
# ---------------------------------------------------------------------------


def weight_year_for(month: date) -> int:
    """Год весов для месяца: сам год в 2016–2024, иначе ближайший (как Task 3)."""
    if month.year < min(WEIGHT_YEARS):
        return min(WEIGHT_YEARS)
    if month.year > max(WEIGHT_YEARS):
        return max(WEIGHT_YEARS)
    return month.year


def month_window(month: date) -> tuple:
    """`(первое, последнее число)` месяца `M`."""
    start = month.replace(day=1)
    return start, bu.month_end(start)


def year_table(table: dict, year: int) -> dict:
    """Таблица года: сам год, иначе ближайший более ранний, иначе самый ранний."""
    if year in table:
        return table[year]
    earlier = [value for value in sorted(table) if value <= year]
    return table[earlier[-1]] if earlier else table[min(table)]


# ---------------------------------------------------------------------------
# Маппинг 148 → 88 групп
# ---------------------------------------------------------------------------


def group_leaf_map(dictionary: "gp.Dictionary", group_codes) -> dict:
    """`лист → группа`: глубочайший предок среди 88 кодов (вложенных групп нет).

    Группа уровня 7 — сама лист; для остальных берутся листья из
    `leaves_by_ancestor`. Лист, попавший в две группы, — ошибка конфигурации.
    """
    out = {}
    for code in group_codes:
        level = dictionary.levels.get(code)
        if level is None:
            raise ValueError(f"группа {code}: нет уровня в dictionary_cpi")
        leaves = [code] if level == 7 else dictionary.leaves_by_ancestor.get(code, [])
        for leaf in leaves:
            previous = out.get(leaf)
            if previous is not None and previous != code:
                raise ValueError(f"лист {leaf} принадлежит и {previous}, и {code}")
            out[leaf] = code
    return out


def map_weekly_series(products: list, weights: list, dictionary: "gp.Dictionary", group_codes) -> tuple:
    """`(строки маппинга, несопоставленные позиции)`.

    Каждая недельная позиция сопоставляется с листом веса 2024 (extended-
    нормализация Task 3) и с группой по `group_leaf_map`. Позиция без
    единственного кода — ошибка: молчаливый выбор кода запрещён.
    """
    leaf_group = group_leaf_map(dictionary, group_codes)
    matched, unmatched = audit.match_mode(list(products), weights, "extended")
    by_series = defaultdict(list)
    for item in matched:
        by_series[item["series"]].append(item["weight"])
    rows = []
    for series in products:
        entries = by_series.get(series, [])
        codes = sorted({entry["code"] for entry in entries})
        if len(codes) > 1:
            raise ValueError(f"позиция {series!r} матчится на несколько кодов: {codes}")
        code = codes[0] if codes else ""
        group = leaf_group.get(code, "")
        rows.append(
            {
                "series": series,
                "weight_code": code,
                "weight_2024": sum(entry["weight"] for entry in entries),
                "level": dictionary.levels.get(code),
                "group_code": group,
                "group_name": dictionary.names.get(group, "") if group else "",
                "matched": bool(entries),
                "in_group": bool(group),
            }
        )
    return rows, unmatched


def mapping_summary(rows: list, group_codes) -> dict:
    """Счётчики и списки R11: 118 сопоставленных, 112 в группах, 52 группы и т.д."""
    matched = [row for row in rows if row["matched"]]
    in_group = [row for row in matched if row["in_group"]]
    groups = sorted({row["group_code"] for row in in_group})
    outside = sorted(row["series"] for row in matched if not row["in_group"])
    unmatched = sorted(row["series"] for row in rows if not row["matched"])
    uncovered = sorted(set(group_codes) - set(groups))
    return {
        "n_series": len(rows),
        "n_matched": len(matched),
        "n_in_group": len(in_group),
        "n_groups": len(groups),
        "n_groups_without_weekly": len(uncovered),
        "n_outside_groups": len(outside),
        "n_unmatched": len(unmatched),
        "groups": groups,
        "groups_without_weekly": uncovered,
        "outside_groups": outside,
        "unmatched": unmatched,
    }


def load_group_dictionary(archive: Path, year: int = DICTIONARY_YEAR) -> "gp.Dictionary":
    """Словарь года (`уровень_1..7`) для маппинга лист → группа."""
    frame = pd.read_excel(bu.dictionary_path(archive), sheet_name=str(year))
    return gp.dictionary_from_frame(frame)


def load_leaf_weights(archive: Path, years=WEIGHT_YEARS) -> dict:
    """`{год: {лист: вес}}` — как `cb_comp_groups_panel.load_leaf_weights`."""
    return gp.load_leaf_weights(archive) if tuple(years) == WEIGHT_YEARS else {
        year: gp.leaf_weights_from_frame(pd.read_excel(bu.weights_path(archive), sheet_name=str(year)))
        for year in years
    }


def group_coverage(rows: list, group_codes, group_weights: dict, leaf_weights: dict) -> list:
    """Покрытие веса группы недельными позициями 2024: `[группа → {…}]`."""
    by_group = defaultdict(list)
    for row in rows:
        if row["in_group"]:
            by_group[row["group_code"]].append(row)
    out = []
    for code in group_codes:
        items = by_group.get(code, [])
        weight = float(group_weights.get(code, 0.0))
        covered = sum(float(leaf_weights.get(row["weight_code"], 0.0)) for row in items)
        out.append(
            {
                "group_code": code,
                "n_series": len(items),
                "weight": weight,
                "covered_weight": covered,
                "coverage": covered / weight if weight else 0.0,
                "series": sorted(row["series"] for row in items),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Признаки
# ---------------------------------------------------------------------------


def periods_by_series(points_by_series: dict) -> dict:
    """`{ряд: [(начало, конец, индекс, длина)]}` — периоды недельных точек."""
    return {series: agg._periods(points) for series, points in points_by_series.items()}


def mapping_by_group(rows: list) -> dict:
    """`{группа: [строка маппинга]}` только для позиций внутри 88 групп."""
    out = defaultdict(list)
    for row in rows:
        if row["in_group"]:
            out[row["group_code"]].append(row)
    return dict(out)


def weekly_group_features(
    periods: dict,
    by_group: dict,
    leaf_weights: dict,
    group_weights: dict,
    month: date,
    cutoff: date,
    group_codes=None,
) -> dict:
    """`{группа: {wk_g_mtd, wk_g_cov, n_series, covered_weight}}` за месяц `M`.

    Окно — календарный месяц; используются только периоды с `конец + 2 ≤ cutoff`.
    Изменение — взвешенное геометрическое по покрытым позициям с ренормировкой
    на покрытый вес; без покрытия — `NaN`, покрытие `0.0`.
    """
    boundary = cutoff - timedelta(days=WEEKLY_LAG_DAYS)
    start, end = month_window(month)
    codes = list(group_codes) if group_codes is not None else sorted(by_group)
    out = {}
    for code in codes:
        numerator = denominator = 0.0
        n_series = 0
        for row in by_group.get(code, ()):
            series_periods = periods.get(row["series"])
            if not series_periods:
                continue
            available = [item for item in series_periods if item[1] <= boundary]
            log_sum, covered = agg._log_and_days(available, start, end)
            if covered <= 0:
                continue
            weight = float(leaf_weights.get(row["weight_code"], 0.0))
            if weight <= 0:
                continue
            numerator += weight * log_sum
            denominator += weight
            n_series += 1
        if denominator > 0:
            mtd = (math.exp(numerator / denominator) - 1.0) * 100.0
            total = float(group_weights.get(code, 0.0))
            coverage = denominator / total if total > 0 else 0.0
        else:
            mtd, coverage = NAN, 0.0
        out[code] = {"wk_g_mtd": mtd, "wk_g_cov": coverage, "n_series": n_series, "covered_weight": denominator}
    return out


def aggregate_features(points: list, month: date, cutoff: date) -> dict:
    """Агрегатные `wkagg_*` месяца `M` на cutoff (официальный недельный ряд)."""
    return agg.wkagg_features(points, month, cutoff)


def last_used_point(periods: list, month: date, cutoff: date):
    """Последняя дата периода, попадающего в окно месяца и доступного к cutoff."""
    boundary = cutoff - timedelta(days=WEEKLY_LAG_DAYS)
    start, end = month_window(month)
    dates = [
        item[1]
        for item in periods
        if item[1] <= boundary and agg._overlap_days(item[0], item[1], start, end) > 0
    ]
    return max(dates) if dates else None
