#!/usr/bin/env python3
"""Live-панель 88 групп CB_COMP из ЕМИСС 31074 (`v_fedstat_ipc`) — Task 1 #421.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Спека
`docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §3–§7.

* **Q1 маппинг** — 88 групп `model.xlsx` → ряды 31074: `direct` (готовый ряд
  группы с тем же названием; цепочка — основной ряд и ручные псевдонимы
  прежних названий), `aggregated` (листья словаря `dictionary_cpi` уровня 7,
  найденные в 31074, весами `cpi_weights_tovar`), `missing`;
* **Q2 NSA** — м/м группы за месяц: первый ряд цепочки с точкой; если ни у
  одного точки нет — Ласпейрес от декабря прошлого года по листьям с полными
  данными с января (резерв, число таких месяцев пишется);
  сверка с архивным `mom` (`cpi_sa_db_with_forecast.csv`, Россия);
* **Q3 SA** — X-13 (R `seasonal`, мост `cpi_sa_reconciliation.make_r_runner`)
  на уровне группы с 2010-01: «вся выборка» и, по флагу `--asof`, as-of с
  2019-01; сверка с архивным `mom_sa`;
* **Q4 сборка** — функция гейта #190 (`cb_comp_bottomup.reconstruction`) на
  наших SA-группах против `TARGET_FULL`; веса — лаг год;
* **Q6 швы** — NSA-ошибка до 2023 и 2023+ (РФ без новых субъектов), смены
  состава листьев по годам словаря.

Пороги — спека §7, константы ниже; после первого прогона не меняются.
Только чтение ClickHouse и локального архива CB_COMP; в БД не пишет.
Запуск — `make cb-comp-groups-panel`, тесты — `make cb-comp-groups-panel-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_parity as cp

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §4, §6, §7)
# ---------------------------------------------------------------------------

SERIES_PREFIX = "ipc31074/"
PANEL_START = date(2010, 1, 1)
NSA_WINDOW = (date(2010, 1, 1), date(2024, 9, 1))
SA_WINDOW = (date(2016, 1, 1), date(2024, 7, 1))
SEAM_START = date(2023, 1, 1)
ASOF_START = date(2019, 1, 1)
WEIGHT_YEARS = tuple(range(2016, 2025))
DICTIONARY_YEARS = tuple(range(2010, 2025))
COVERAGE_YEAR = 2024
MIN_SA_MONTHS = 36

COVERAGE_MIN = 0.85
NSA_RMSE_MAX = 0.05
NSA_GROUP_FLAG = 0.10
SA_RMSE_MAX = 0.15
SA_CORR_MIN = 0.90
ASSEMBLY_RMSE_H1_MAX = 0.10

SERVICES_CODE = "9000"

# Ручные псевдонимы группы: прежние названия ряда в 31074, когда Росстат
# переименовал позицию. Каждый — с причиной; fuzzy-подбор запрещён (спека §5.1).
# Псевдоним используется только в месяцы, где основного ряда нет.
MANUAL_ALIASES: dict = {
    "803": (("Масло подсолнечное, кг", "единица в 31074 сменилась с кг на л в 2022: «, кг» 2002–2021, «, л» 2022+"),),
    "930.АГ": (("Услуги электросвязи", "«Услуги телекоммуникационные» в 31074 — с 2023; до этого телеком — «Услуги электросвязи» (2006–2022); «Услуги связи» включают почту"),),
}

OUT_DIR = ROOT / "build" / "cb-comp-groups"


# ---------------------------------------------------------------------------
# Месяцы и названия
# ---------------------------------------------------------------------------


def month_index(day: date) -> int:
    return day.year * 12 + day.month - 1


def month_from_index(index: int) -> date:
    return date(index // 12, index % 12 + 1, 1)


def month_grid(first: date, last: date) -> list:
    return [month_from_index(i) for i in range(month_index(first), month_index(last) + 1)]


def normalize_name(text) -> str:
    """Название позиции для сравнения: регистр, ё, кавычки, пробелы, хвостовая точка."""
    if text is None or (isinstance(text, float) and math.isnan(text)):
        return ""
    value = str(text).lower().replace("ё", "е")
    value = re.sub(r"[«»\"“”„']", "", value)
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\s*,\s*", ", ", value)
    value = re.sub(r"\(\s+", "(", value)
    value = re.sub(r"\s+\)", ")", value)
    return value.strip().rstrip(".").strip()


# ---------------------------------------------------------------------------
# Словарь, веса, маппинг (Q1)
# ---------------------------------------------------------------------------


@dataclass
class Dictionary:
    """Словарь года: код → название/уровень и листья (уровень 7) по предкам."""

    names: dict
    levels: dict
    leaves_by_ancestor: dict


def dictionary_from_frame(frame: pd.DataFrame) -> Dictionary:
    names, levels, leaves = {}, {}, {}
    for _, row in frame.iterrows():
        code = bu.normalize_code(row["код_товара_услуги"])
        if not code:
            continue
        names[code] = str(row["наименование_товара_услуги"])
        level = int(row["уровень"])
        levels[code] = level
        if level != 7:
            continue
        for ancestor_level in range(1, 7):
            ancestor = bu.normalize_code(row.get(f"уровень_{ancestor_level}"))
            if ancestor and ancestor != "<==>":
                leaves.setdefault(ancestor, []).append(code)
    return Dictionary(names=names, levels=levels, leaves_by_ancestor=leaves)


def load_dictionaries(archive: Path) -> dict:
    book = pd.ExcelFile(bu.dictionary_path(archive))
    return {year: dictionary_from_frame(pd.read_excel(book, str(year))) for year in DICTIONARY_YEARS}


def leaf_weights_from_frame(frame: pd.DataFrame) -> dict:
    """`{код: вес}` листовых позиций России (`окато_код=643`)."""
    frame = frame.copy()
    frame["code"] = frame["код_товара_услуги"].map(bu.normalize_code)
    frame["okato"] = frame["окато_код"].map(bu.normalize_code)
    frame = frame[frame["okato"] == "643"]
    return {code: float(weight) for code, weight in zip(frame["code"], frame["вес_товара"]) if code}


def load_leaf_weights(archive: Path) -> dict:
    book = pd.ExcelFile(bu.weights_path(archive))
    return {year: leaf_weights_from_frame(pd.read_excel(book, str(year))) for year in WEIGHT_YEARS}


def year_key(table: dict, year: int) -> int:
    """Год таблицы для месяца: сам год, иначе ближайший более ранний, иначе самый ранний."""
    if year in table:
        return year
    earlier = [value for value in sorted(table) if value <= year]
    return earlier[-1] if earlier else min(table)


@dataclass
class GroupMap:
    """Результат маппинга группы: метод, ряд/листья по годам, покрытие весом 2024."""

    code: str
    name: str
    method: str
    chain: tuple = ()
    leaves_by_year: dict = field(default_factory=dict)
    coverage_2024: float = 0.0
    manual: list = field(default_factory=list)


def resolve_name(name: str, products: dict) -> str:
    """Ряд 31074 по нормализованному названию; `""` — не найдено."""
    return products.get(normalize_name(name), "")


def map_group(code: str, dictionaries: dict, leaf_weights: dict, products: dict) -> GroupMap:
    latest = dictionaries[max(dictionaries)]
    name = latest.names.get(code) or next(
        (dictionaries[y].names[code] for y in sorted(dictionaries, reverse=True) if code in dictionaries[y].names),
        "",
    )
    chain, manual = [], []
    primary = resolve_name(name, products)
    if primary:
        chain.append(primary)
    for alias, _reason in MANUAL_ALIASES.get(code, ()):
        series = resolve_name(alias, products)
        if series and series not in chain:
            chain.append(series)
            manual.append(alias)
    leaves_by_year = {}
    for year, dictionary in dictionaries.items():
        matched = {}
        for leaf in dictionary.leaves_by_ancestor.get(code, []):
            leaf_series = resolve_name(dictionary.names.get(leaf, ""), products)
            if leaf_series:
                matched[leaf] = leaf_series
        leaves_by_year[year] = matched
    if chain:
        return GroupMap(code=code, name=name, method="direct", chain=tuple(chain),
                        leaves_by_year=leaves_by_year, coverage_2024=1.0, manual=manual)
    coverage = 0.0
    weights = leaf_weights.get(COVERAGE_YEAR, {})
    all_leaves = dictionaries[COVERAGE_YEAR].leaves_by_ancestor.get(code, []) if COVERAGE_YEAR in dictionaries else []
    total = sum(weights.get(leaf, 0.0) for leaf in all_leaves)
    if total > 0:
        covered = sum(weights.get(leaf, 0.0) for leaf in leaves_by_year.get(COVERAGE_YEAR, {}))
        coverage = covered / total
    method = "aggregated" if any(leaves_by_year.values()) else "missing"
    return GroupMap(code=code, name=name, method=method, leaves_by_year=leaves_by_year,
                    coverage_2024=coverage)


def composition_changes(group: GroupMap) -> int:
    """Число смен набора найденных листьев между соседними годами словаря (Q6)."""
    years = sorted(group.leaves_by_year)
    return sum(
        1 for left, right in zip(years, years[1:])
        if set(group.leaves_by_year[left]) != set(group.leaves_by_year[right])
    )


# ---------------------------------------------------------------------------
# NSA групп (Q2)
# ---------------------------------------------------------------------------


def aggregated_mm(group: GroupMap, product_mm: dict, leaf_weights: dict, months: list) -> dict:
    """М/м группы: Ласпейрес от декабря прошлого года по листьям с полными данными.

    Для месяца M года Y набор S — листья словаря года Y (иначе ближайшего
    раннего) с весом и с данными за все месяцы январь…M. `L_i(M)` — накопленный
    рост с декабря Y−1; `mm(M) = Σ_S w·L(M) / Σ_S w·L(M−1) − 1` (`L(дек) = 1`).
    Пустой S — месяц без значения (NaN не заменяется).
    """
    out = {}
    for month in months:
        year = month.year
        leaves = group.leaves_by_year.get(year_key(group.leaves_by_year, year), {})
        weights = leaf_weights[year_key(leaf_weights, year)]
        year_months = [date(year, m, 1) for m in range(1, month.month + 1)]
        numerator = denominator = 0.0
        for leaf, series in leaves.items():
            weight = weights.get(leaf)
            values = product_mm.get(series, {})
            if not weight or any(m not in values for m in year_months):
                continue
            level_prev = 1.0
            for m in year_months[:-1]:
                level_prev *= 1 + values[m] / 100
            level_now = level_prev * (1 + values[month] / 100)
            numerator += weight * level_now
            denominator += weight * level_prev
        if denominator:
            out[month] = (numerator / denominator - 1) * 100
    return out


def group_nsa(group: GroupMap, product_mm: dict, leaf_weights: dict, months: list) -> tuple:
    """`(mm, sources)`: м/м группы и источник каждого месяца (`chain:i` | `leaves`)."""
    out, sources = {}, {}
    fallback = aggregated_mm(group, product_mm, leaf_weights, months) if group.method != "missing" else {}
    for month in months:
        for position, series in enumerate(group.chain):
            values = product_mm.get(series, {})
            if month in values:
                out[month], sources[month] = values[month], f"chain:{position}"
                break
        else:
            if month in fallback:
                out[month], sources[month] = fallback[month], "leaves"
    return out, sources


def contiguous_tail(mm: dict, last: date) -> list:
    """Самый длинный непрерывный отрезок месяцев с данными, заканчивающийся в `last`."""
    if last not in mm:
        return []
    index = month_index(last)
    while month_from_index(index - 1) in mm and month_from_index(index - 1) >= PANEL_START:
        index -= 1
    return month_grid(month_from_index(index), last)


# ---------------------------------------------------------------------------
# SA групп (Q3)
# ---------------------------------------------------------------------------


def group_sa(mm: dict, last: date, runner_factory, asof: bool) -> dict:
    """SA м/м группы X-13: `{"full": {...}, "rt": {...}, "status": ...}`.

    Ряд короче `MIN_SA_MONTHS` или неудачный фит — `sa_failed`: NaN, NSA не
    подставляется. `runner_factory(start)` — мост X-13 для ряда с первого месяца `start`.
    """
    # Хвост ряда — от его собственного последнего месяца в пределах `last`:
    # ряд, снятый с публикации раньше (9260 — до 2024-10), не теряет историю.
    own_last = max((m for m in mm if m <= last), default=None)
    grid = contiguous_tail(mm, own_last) if own_last else []
    if len(grid) < MIN_SA_MONTHS:
        return {"full": {}, "rt": {}, "status": "sa_failed", "months": len(grid)}
    levels = [1.0]
    for month in grid:
        levels.append(levels[-1] * (1 + mm[month] / 100))
    levels = levels[1:]
    first_rt = len(grid) + 1
    if asof:
        first_rt = next((i + 1 for i, m in enumerate(grid) if m >= ASOF_START), len(grid) + 1)
        first_rt = max(first_rt, MIN_SA_MONTHS)
    rows = runner_factory(grid[0])(levels, first_rt)
    full = {m: float(r[0]) for m, r in zip(grid, rows) if not math.isnan(float(r[0]))}
    rt = {m: float(r[1]) for m, r in zip(grid, rows) if not math.isnan(float(r[1]))}
    status = "ok" if full else "sa_failed"
    return {"full": full, "rt": rt, "status": status, "months": len(grid)}


# ---------------------------------------------------------------------------
# Метрики и сборка (Q2–Q4)
# ---------------------------------------------------------------------------


def compare(ours: dict, ref: dict, first: date, last: date) -> dict:
    months = [m for m in month_grid(first, last) if m in ours and m in ref]
    if not months:
        return {"n": 0, "rmse": float("nan"), "mae": float("nan"), "bias": float("nan"), "corr": float("nan")}
    diff = np.array([ours[m] - ref[m] for m in months])
    a = np.array([ours[m] for m in months])
    b = np.array([ref[m] for m in months])
    corr = float(np.corrcoef(a, b)[0, 1]) if len(months) > 2 and a.std() > 0 and b.std() > 0 else float("nan")
    return {
        "n": len(months),
        "rmse": float(np.sqrt(np.mean(diff ** 2))),
        "mae": float(np.mean(np.abs(diff))),
        "bias": float(np.mean(diff)),
        "corr": corr,
    }


def weighted_rmse(per_group: dict, weights: dict) -> tuple:
    """`(sqrt(Σ w·MSE / Σ w), Σ w)` по группам с метрикой; вес — 2024."""
    numerator = denominator = 0.0
    for code, metric in per_group.items():
        weight = weights.get(code, 0.0)
        if not weight or not metric.get("n") or math.isnan(metric["rmse"]):
            continue
        numerator += weight * metric["rmse"] ** 2
        denominator += weight
    return (math.sqrt(numerator / denominator) if denominator else float("nan")), denominator


def weighted_mean(per_group: dict, weights: dict, key: str) -> float:
    numerator = denominator = 0.0
    for code, metric in per_group.items():
        weight = weights.get(code, 0.0)
        value = metric.get(key, float("nan"))
        if not weight or value is None or math.isnan(value):
            continue
        numerator += weight * value
        denominator += weight
    return numerator / denominator if denominator else float("nan")


def log_diff_frame(series_by_code: dict, months: list) -> pd.DataFrame:
    """SA м/м, % → лог-прирост `log(1 + x/100)` (вход `component_targets`)."""
    data = {
        code: [math.log1p(values[m] / 100) if m in values else np.nan for m in months]
        for code, values in series_by_code.items()
    }
    return pd.DataFrame(data, index=pd.Index(months, name="month"))


def assembly(series_by_code: dict, target_full: pd.DataFrame, group_weights: dict, months: list) -> dict:
    """Гейт #190 на наших SA-группах: `{окно: {h: метрики}}`."""
    grid = month_grid(PANEL_START, max(months))
    targets = bu.component_targets(log_diff_frame(series_by_code, grid))
    normal = cp.normal_months()
    return {
        "test_67": bu.reconstruction(targets, target_full, group_weights, months),
        "normal_54": bu.reconstruction(targets, target_full, group_weights, normal),
    }


def decisions(coverage: float, nsa_rmse: float, sa_rmse: float, sa_corr: float, assembly_h1: float) -> dict:
    """Решения спеки §7 (кроме весов live — Q5 решается в отчёте)."""
    return {
        "mapping_ok": coverage >= COVERAGE_MIN,
        "nsa_ok": nsa_rmse <= NSA_RMSE_MAX,
        "sa_ok": sa_rmse <= SA_RMSE_MAX and sa_corr >= SA_CORR_MIN,
        "assembly_ok": assembly_h1 <= ASSEMBLY_RMSE_H1_MAX and coverage >= COVERAGE_MIN,
    }


# ---------------------------------------------------------------------------
# Загрузка данных
# ---------------------------------------------------------------------------


def load_products() -> tuple:
    """`(product_mm, regions)` из `v_fedstat_ipc`: `{series: {месяц: м/м, %}}`.

    На одну пару (ряд, месяц) ожидается один регион (до 2023 — РФ, с 2023 — РФ
    без новых субъектов); дубль — ошибка, а не молчаливый выбор.
    """
    from lib.clickhouse import ch_query

    rows = ch_query(
        "SELECT series, region, toString(date) AS d, value FROM v_fedstat_ipc "
        f"WHERE date >= '{PANEL_START.isoformat()}' ORDER BY series, date FORMAT JSON"
    )
    product_mm, regions, seen = {}, {}, set()
    for row in rows:
        key = (row["series"], row["d"][:10])
        if key in seen:
            raise ValueError(f"v_fedstat_ipc: два региона на {key}")
        seen.add(key)
        month = date.fromisoformat(row["d"][:10]).replace(day=1)
        product_mm.setdefault(row["series"], {})[month] = float(row["value"]) - 100.0
        regions.setdefault(str(row["region"]), 0)
        regions[str(row["region"])] += 1
    return product_mm, regions


def product_index(product_mm: dict) -> dict:
    """Нормализованное название → ряд; коллизия названий — ошибка."""
    index = {}
    for series in product_mm:
        name = series[len(SERIES_PREFIX):] if series.startswith(SERIES_PREFIX) else series
        key = normalize_name(name)
        if key in index and index[key] != series:
            raise ValueError(f"31074: два ряда с названием {key!r}")
        index[key] = series
    return index


def sa_db_path(archive: Path) -> Path:
    return archive / "Models/CpiComponents/StfCpi_External_data/databases/StfCpiR_db/output/cpi_sa_db_with_forecast.csv"


def load_archive_reference(archive: Path) -> tuple:
    """`(mom, mom_sa)` архива по России: `{код: {месяц: %}}`."""
    frame = pd.read_csv(sa_db_path(archive), sep=";", decimal=",", dtype=str)
    frame = frame[frame["регион"] == "Россия"]
    mom, mom_sa = {}, {}
    for code, day, raw_mom, raw_sa in zip(frame["код_товара_услуги"], frame["дата"], frame["mom"], frame["mom_sa"]):
        code = bu.normalize_code(code)
        month = date.fromisoformat(str(day)[:10]).replace(day=1)
        for raw, table in ((raw_mom, mom), (raw_sa, mom_sa)):
            if isinstance(raw, str) and raw.strip():
                table.setdefault(code, {})[month] = float(raw.replace(",", "."))
    return mom, mom_sa


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, out_dir: Path, runner_factory, asof: bool, jobs: int) -> dict:
    specs = bu.load_specs(archive)
    codes = [spec.code for spec in specs]
    dictionaries = load_dictionaries(archive)
    leaf_weights = load_leaf_weights(archive)
    group_weights = bu.load_weights(archive, WEIGHT_YEARS, specs)
    weights_2024 = {code: group_weights[COVERAGE_YEAR].get(code, 0.0) for code in codes}
    product_mm, regions = load_products()
    products = product_index(product_mm)
    last = max(m for values in product_mm.values() for m in values)
    months = month_grid(PANEL_START, last)

    # Q1/Q6 — маппинг, покрытие, смены состава.
    groups = {code: map_group(code, dictionaries, leaf_weights, products) for code in codes}
    total_weight = sum(weights_2024.values())
    coverage_abs = sum(weights_2024[c] * groups[c].coverage_2024 for c in codes if groups[c].method != "missing")
    services_codes = {c for c in codes if is_within(dictionaries[COVERAGE_YEAR], c, SERVICES_CODE)}

    # Q2 — NSA.
    nsa, sources = {}, {}
    for code in codes:
        nsa[code], sources[code] = group_nsa(groups[code], product_mm, leaf_weights, months)
    mom, mom_sa = load_archive_reference(archive)
    nsa_metrics = {c: compare(nsa[c], mom.get(c, {}), *NSA_WINDOW) for c in codes}
    nsa_pre = {c: compare(nsa[c], mom.get(c, {}), NSA_WINDOW[0], date(2022, 12, 1)) for c in codes}
    nsa_seam = {c: compare(nsa[c], mom.get(c, {}), SEAM_START, NSA_WINDOW[1]) for c in codes}
    nsa_rmse, _ = weighted_rmse(nsa_metrics, weights_2024)

    # Q3 — X-13 по группам.
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futures = {c: pool.submit(group_sa, nsa[c], last, runner_factory, asof) for c in codes}
        sa = {c: futures[c].result() for c in codes}
    sa_full = {c: sa[c]["full"] for c in codes}
    sa_metrics = {c: compare(sa_full[c], mom_sa.get(c, {}), *SA_WINDOW) for c in codes}
    sa_rmse, _ = weighted_rmse(sa_metrics, weights_2024)
    sa_corr = weighted_mean(sa_metrics, weights_2024, "corr")
    services = {c: sa_metrics[c] for c in codes if c in services_codes}
    services_rmse, _ = weighted_rmse(services, weights_2024)

    # Q4 — сборка; справочно — сборка архивного mom_sa теми же функциями.
    target_full = cp.read_target_full(archive)
    test_months = cp.month_range(cp.TEST_START, cp.TEST_END)
    ours = assembly(sa_full, target_full, group_weights, test_months)
    reference = assembly({c: mom_sa.get(c, {}) for c in codes}, target_full, group_weights, test_months)
    ours_asof = None
    if asof:
        ours_asof = assembly({c: sa[c]["rt"] for c in codes}, target_full, group_weights, test_months)

    decision = decisions(coverage_abs, nsa_rmse, sa_rmse, sa_corr, ours["test_67"][1]["rmse"])
    result = {
        "spec": "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md",
        "thresholds": {
            "coverage_min": COVERAGE_MIN, "nsa_rmse_max": NSA_RMSE_MAX, "nsa_group_flag": NSA_GROUP_FLAG,
            "sa_rmse_max": SA_RMSE_MAX, "sa_corr_min": SA_CORR_MIN, "assembly_rmse_h1_max": ASSEMBLY_RMSE_H1_MAX,
        },
        "inputs": {
            "archive_sa_db_sha256": sha256(sa_db_path(archive)),
            "model_xlsx_sha256": sha256(bu.model_path(archive)),
            "weights_sha256": sha256(bu.weights_path(archive)),
            "dictionary_sha256": sha256(bu.dictionary_path(archive)),
            "fedstat_series": len(product_mm),
            "fedstat_regions": regions,
            "last_month": last.isoformat(),
        },
        "mapping": {
            "groups": len(codes),
            "methods": {m: sum(1 for g in groups.values() if g.method == m) for m in ("direct", "aggregated", "missing")},
            "coverage_abs_2024": coverage_abs,
            "coverage_rel_2024": coverage_abs / total_weight if total_weight else float("nan"),
            "group_weight_total_2024": total_weight,
            "manual_aliases": {c: [list(a) for a in v] for c, v in MANUAL_ALIASES.items()},
        },
        "nsa": {"weighted_rmse": nsa_rmse, "flagged": sorted(c for c in codes if nsa_metrics[c]["n"] and nsa_metrics[c]["rmse"] > NSA_GROUP_FLAG)},
        "sa": {"weighted_rmse": sa_rmse, "weighted_corr": sa_corr, "services_weighted_rmse": services_rmse,
               "failed": sorted(c for c in codes if sa[c]["status"] != "ok")},
        "assembly": {"ours_full": _jsonable(ours), "archive_mom_sa": _jsonable(reference),
                     "ours_asof": _jsonable(ours_asof) if ours_asof else None},
        "decisions": decision,
        "groups": {
            c: {
                "name": groups[c].name, "method": groups[c].method, "chain": list(groups[c].chain),
                "sources": {k: sum(1 for v in sources[c].values() if v == k) for k in sorted(set(sources[c].values()))},
                "leaves_2024": len(groups[c].leaves_by_year.get(COVERAGE_YEAR, {})),
                "coverage_2024": groups[c].coverage_2024, "weight_2024": weights_2024[c],
                "manual": groups[c].manual, "composition_changes": composition_changes(groups[c]),
                "services": c in services_codes,
                "nsa": nsa_metrics[c], "nsa_pre2023": nsa_pre[c], "nsa_2023": nsa_seam[c],
                "sa": sa_metrics[c], "sa_status": sa[c]["status"], "sa_months": sa[c]["months"],
            }
            for c in codes
        },
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    write_panel(out_dir / "panel.csv", codes, months, nsa, sa)
    (out_dir / "result.json").write_text(json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def is_within(dictionary: Dictionary, code: str, ancestor: str) -> bool:
    """Группа `code` лежит внутри `ancestor` (по листьям словаря)."""
    leaves = set(dictionary.leaves_by_ancestor.get(code, []))
    return bool(leaves) and leaves <= set(dictionary.leaves_by_ancestor.get(ancestor, []))


def write_panel(path: Path, codes: list, months: list, nsa: dict, sa: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["code", "month", "nsa_mm", "sa_full_mm", "sa_asof_mm"])
        for code in codes:
            for month in months:
                values = (nsa[code].get(month), sa[code]["full"].get(month), sa[code]["rt"].get(month))
                if all(v is None for v in values):
                    continue
                writer.writerow([code, month.isoformat()] + ["" if v is None else repr(v) for v in values])


def _jsonable(value):
    if isinstance(value, dict):
        return {str(k.isoformat() if isinstance(k, date) else k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (np.floating,)):
        return _jsonable(float(value))
    if isinstance(value, (np.integer,)):
        return int(value)
    return value


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None, help="каталог архива CB_COMP (иначе CB_COMP_ARCHIVE / известные пути)")
    parser.add_argument("--out", default=str(OUT_DIR))
    parser.add_argument("--asof", action="store_true", help="дополнительно X-13 as-of с 2019-01 (для Task 2)")
    parser.add_argument("--jobs", type=int, default=4)
    args = parser.parse_args(argv)
    from models.cpi import cpi_sa_reconciliation as sar

    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.out), sar.make_r_runner, args.asof, args.jobs)
    print(json.dumps({k: _jsonable(result[k]) for k in ("mapping", "nsa", "sa", "decisions")}, ensure_ascii=False, indent=2))
    print(json.dumps(_jsonable(result["assembly"]["ours_full"]["test_67"]), ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
