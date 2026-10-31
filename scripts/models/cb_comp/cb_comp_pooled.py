#!/usr/bin/env python3
"""Pooled CatBoost на 88 группах, окно 2016+ — Task 10 #438.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-pooled-88-groups-design.md`.

* **строки** — `(группа, целевой месяц)`; цель — накопленный SA-прирост
  группы за h месяцев (`cb_comp_bottomup.component_targets`), признаки —
  база `live_cb` + id/вес группы + собственный листовой блок МП;
* **окна** — main 2016-01+, control 2010-03+; годовая переподготовка на
  расширяющемся окне в первом origin года (no-leakage);
* **кандидаты** — `pooled_main` (гейт), `pooled_control`, `combo3` (Task 9,
  пересчитывается тем же кодом), `combo3_pooled`, `live_cb`, `cb_comp`;
* **оценка** — `TARGET_FULL` normal-54: W5b против CB_COMP, попарно DM-HLN +
  95 % CI против `combo3`, MCS 90 %;
* **гейт G10** — ниже `combo3` на h3 или h6 при DM p < 0,10 и не хуже на h1.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-pooled`, тесты — `make cb-comp-pooled-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import subprocess
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from catboost import CatBoostRegressor

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as h1
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en as en
from models.cb_comp import cb_comp_parity as cp
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §3–§9)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
WINDOW_MAIN = "main"
WINDOW_CONTROL = "control"
WINDOW_START = {WINDOW_MAIN: date(2016, 1, 1), WINDOW_CONTROL: date(2010, 3, 1)}
TEST_YEARS = tuple(range(cp.TEST_START.year, cp.TEST_END.year + 1))
ARTICLE_OUTLIERS = cp.TRAIN_OUTLIER_MONTHS | cp.OUTLIER_MONTHS
SHEET_INDICATORS = ("ibk", "costs", "price_exp")
CAT_FEATURES = ("month", "group_code", "sheet_profile")
BASE_NUMERIC = ("y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm")

MODEL_POOLED_MAIN = "pooled_main"
MODEL_POOLED_CONTROL = "pooled_control"
MODEL_COMBO = "combo3"
MODEL_COMBO_POOLED = "combo3_pooled"
MODEL_CB = "live_cb"
CB_COMP = "cb_comp"
CANDIDATES = (MODEL_POOLED_MAIN, MODEL_POOLED_CONTROL, MODEL_COMBO, MODEL_COMBO_POOLED, MODEL_CB)
G10_ALPHA = 0.10
R10_TOLERANCE = 1e-3
GROUPS_WITHOUT_SA = ("9218",)
GROUPS_EXPECTED = 87

# Task 3 (#423) / Task 9 (#436): пины для R10a/b.
TASK3_LIVE_CB_RATIOS = {
    1: 1.1928505290859988,
    2: 1.0127041023069754,
    3: 1.0758201741459672,
    6: 1.0616906586271115,
}
TASK9_COMBO_RATIOS = {
    1: 1.0347684348311972,
    2: 0.9571087729271289,
    3: 1.0101981596603171,
    6: 0.8940353315189836,
}
EXPECTED_INPUTS = {
    "panel_csv": "aec91f993f85489b1f951cd4d23abea0e1d67639fbbed932974099f8d7826112",
    "public_panel_csv": "47176baa290eb21e1f70ac658e1c2f2ddd5131c489b3fcaed09a5c1f8d08efa8",
    "sheet_map_csv": "0d8a6ab5b932e57a312adac1d430089bca8dd5ba6c2b7b22c8591f21a6156116",
    "target_xlsx": "d5278124c736410e08aefacf35f8113facaa220d88d7dba19c4a44bb3b8f685c",
}

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
OUT_DIR = ROOT / "build" / "cb-comp-pooled"


# ---------------------------------------------------------------------------
# Признаки pooled-строки
# ---------------------------------------------------------------------------


def mp_block_names(sheet_slugs) -> list:
    """66 колонок листового блока МП: уровень и `_chg` по 3 индикаторам."""
    names = []
    for slug in sorted(sheet_slugs):
        for indicator in SHEET_INDICATORS:
            base = f"mp_sheet_{bu.feature_code(slug)}_{indicator}"
            names.extend([base, f"{base}_chg"])
    return names


def pooled_feature_names(sheet_slugs) -> list:
    """Общая схема pooled-модели: 5 база + 66 МП + id/лист/вес + month."""
    return list(BASE_NUMERIC) + mp_block_names(sheet_slugs) + ["group_code", "sheet_profile", "weight_share", "month"]


def sheet_profile(spec) -> str:
    return "|".join(spec.sheet_slugs) if spec.sheet_slugs else "none"


def own_mp_names(spec) -> frozenset:
    return frozenset(bu.mp_feature_names(spec, mode="per_sheet"))


def weight_shares(weights: dict, sa_full: dict, months: list) -> dict:
    """`{месяц: {код: доля веса}}` строго за год `M.year−1`; иначе NaN.

    Fallback на другой год не используется: для 2016 и control-окна весов
    года `M−1` в архиве нет, и подстановка будущего года была бы утечкой.
    """
    out = {}
    for month in months:
        weight = weights.get(month.year - 1)
        if weight is None:
            out[month] = {}
            continue
        denominator = sum(
            weight.get(code, 0.0)
            for code, values in sa_full.items()
            if month in values and code != bu.UTILITIES_CODE
        )
        out[month] = {
            code: (float("nan") if code == bu.UTILITIES_CODE or not denominator or code not in weight
                   else weight[code] / denominator)
            for code in sa_full
        }
    return out


@dataclass(frozen=True)
class PooledRow:
    month: date
    code: str
    origin: date
    features: dict
    target: float
    own_mp: frozenset
    profile: str
    weight: float


def row_value(row: PooledRow, name: str):
    if name == "group_code":
        return row.code
    if name == "sheet_profile":
        return row.profile
    if name == "weight_share":
        return row.weight
    if name == "month":
        return row.month.month
    if name.startswith("mp_sheet_"):
        return row.features.get(name, float("nan")) if name in row.own_mp else float("nan")
    return row.features.get(name, float("nan"))


def feature_matrix(rows: list, names: list) -> np.ndarray:
    return np.asarray([[row_value(row, name) for name in names] for row in rows], dtype=object)


# ---------------------------------------------------------------------------
# Окна и строки обучения
# ---------------------------------------------------------------------------


def training_cutoff(window_start: date, origin_min: date) -> date:
    """Последний целевой месяц, доступный к cutoff первого origin года.

    `available_from_mes(month_end(t)) < cutoff_for(O_min)` (конвенция `late`):
    таргет строки `t` опубликован до момента прогноза. Условие монотонно,
    поэтому первый недоступный месяц обрывает окно.
    """
    last = None
    for month in cp.month_range(window_start, origin_min):
        if month in ARTICLE_OUTLIERS:
            continue
        if cm.available_from_mes(cm.month_end(month)) < cm.cutoff_for(origin_min):
            last = month
        else:
            break
    return last


def annual_windows(window_start: date, horizon: int, years=TEST_YEARS) -> dict:
    """`{год: {origin_min, cutoff}}` — один фит на год и горизонт."""
    out = {}
    for year in years:
        origin_min = cp.shift_month(date(year, 1, 1), -(horizon - 1))
        out[year] = {"origin_min": origin_min, "cutoff": training_cutoff(window_start, origin_min)}
    return out


def train_months(window_start: date, horizon: int, year: int) -> list:
    """Целевые месяцы обучения года `Y`: от начала окна до cutoff, без шоков."""
    cutoff = annual_windows(window_start, horizon, (year,))[year]["cutoff"]
    if cutoff is None:
        return []
    return [month for month in cp.month_range(window_start, cutoff) if month not in ARTICLE_OUTLIERS]


def training_rows(specs: list, features: dict, targets: dict, shares: dict, horizon: int, months: list) -> list:
    """Строки `(группа, месяц)` с известным таргетом и признаками origin."""
    target = targets[horizon]
    rows = []
    for spec in specs:
        own = own_mp_names(spec)
        profile = sheet_profile(spec)
        if spec.code not in target.columns:
            continue
        months_by_code = features.get(spec.code, {})
        for month in months:
            if month not in target.index:
                continue
            value = target.at[month, spec.code]
            if pd.isna(value):
                continue
            origin = cp.shift_month(month, -(horizon - 1))
            origin_features = months_by_code.get(origin)
            if origin_features is None:
                continue
            rows.append(
                PooledRow(
                    month=month,
                    code=spec.code,
                    origin=origin,
                    features=origin_features,
                    target=float(value),
                    own_mp=own,
                    profile=profile,
                    weight=shares.get(month, {}).get(spec.code, float("nan")),
                )
            )
    return rows


def prediction_rows(specs: list, features: dict, shares: dict, horizon: int, months: list) -> list:
    """Строки теста: признаки origin `M−(h−1)`, таргет неизвестен (NaN)."""
    rows = []
    for spec in specs:
        own = own_mp_names(spec)
        profile = sheet_profile(spec)
        months_by_code = features.get(spec.code, {})
        for month in months:
            origin = cp.shift_month(month, -(horizon - 1))
            origin_features = months_by_code.get(origin)
            if origin_features is None:
                continue
            rows.append(
                PooledRow(
                    month=month,
                    code=spec.code,
                    origin=origin,
                    features=origin_features,
                    target=float("nan"),
                    own_mp=own,
                    profile=profile,
                    weight=shares.get(month, {}).get(spec.code, float("nan")),
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Модель
# ---------------------------------------------------------------------------


def cat_indices(names: list) -> list:
    return [names.index(name) for name in CAT_FEATURES]


def fit_pooled(rows: list, names: list, overrides: dict = None) -> CatBoostRegressor:
    params = dict(bu.CATBOOST_DEFAULTS)
    params["cat_features"] = cat_indices(names)
    params.update(overrides or {})
    model = CatBoostRegressor(**params)
    model.fit(feature_matrix(rows, names), np.asarray([row.target for row in rows], dtype=float))
    return model


def predict_pooled(model: CatBoostRegressor, rows: list, names: list) -> dict:
    """`{код: {M: прогноз}}`."""
    if not rows:
        return {}
    values = model.predict(feature_matrix(rows, names))
    out = {}
    for row, value in zip(rows, values):
        out.setdefault(row.code, {})[row.month] = float(value)
    return out


def fit_predict_pooled(specs: list, features: dict, targets: dict, weights: dict, sa_full: dict,
                       window_start: date, test_months: list, overrides: dict = None) -> tuple:
    """`({h: {код: {M: прогноз}}}, отчёт по фитам, имена, листы)`."""
    all_sheets = sorted({slug for spec in specs for slug in spec.sheet_slugs})
    names = pooled_feature_names(all_sheets)
    shares = weight_shares(weights, sa_full, cp.month_range(window_start, max(test_months)))
    out = {}
    report = {}
    for horizon in HORIZONS:
        out[horizon] = {}
        report[str(horizon)] = {}
        windows = annual_windows(window_start, horizon, sorted({month.year for month in test_months}))
        for year in sorted(windows):
            window = windows[year]
            months = train_months(window_start, horizon, year)
            rows = training_rows(specs, features, targets, shares, horizon, months)
            if len(rows) < bu.MIN_TRAIN_ROWS:
                raise SystemExit(f"pooled {window_start} h{horizon} {year}: всего {len(rows)} строк обучения")
            model = fit_pooled(rows, names, overrides)
            year_months = [month for month in test_months if month.year == year]
            predictions = predict_pooled(model, prediction_rows(specs, features, shares, horizon, year_months), names)
            for code, series in predictions.items():
                out[horizon].setdefault(code, {}).update(series)
            report[str(horizon)][str(year)] = {
                "origin_min": window["origin_min"],
                "cutoff": window["cutoff"],
                "train_months": months,
                "test_months": year_months,
                "max_target": max(months) if months else None,
                "n_rows": len(rows),
                "n_groups": len({row.code for row in rows}),
            }
    return out, report, names, all_sheets


# ---------------------------------------------------------------------------
# Агрегат и оценка
# ---------------------------------------------------------------------------


def aggregate_pooled(predictions: dict, weights: dict, months: list) -> tuple:
    """`({h: {M: агрегат}}, {h: {M: число групп}})`, веса СПР с лагом год."""
    aggregated, coverage = {}, {}
    for horizon in HORIZONS:
        by_group = predictions.get(horizon, {})
        values, _denominator = bu.aggregate(by_group, weights, months)
        aggregated[horizon] = {month: value for month, value in values.items() if not math.isnan(value)}
        per_month = {}
        for month in months:
            weight = bu.weight_for_month(weights, month)
            per_month[month] = sum(1 for code, series in by_group.items() if month in series and weight.get(code))
        coverage[horizon] = per_month
    return aggregated, coverage


def combo_with_pooled(pooled_main: dict, bs: dict) -> dict:
    """`combo3_pooled`: среднее агрегированного pooled и двух BS-моделей."""
    return {
        horizon: h1.combine_mean([pooled_main[horizon], bs[h1.MODEL_ECON][horizon], bs[h1.MODEL_ROZN][horizon]])
        for horizon in HORIZONS
    }


def reference_forecasts(data: dict, public_dir: Path) -> tuple:
    """`live_cb`, BS-модели и `combo3` — кодом Task 3/9."""
    groups = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"],
                            data["train_months"], data["test_months"], with_mp=True)
    live_cb = lb.aggregate_predictions(groups, data["weights"], data["test_months"])
    regressors = h1.load_regressors(public_dir)
    bs = {
        model: {horizon: h1.bs_predict(data["t_live"][horizon], regressors[model], horizon, data["test_months"])
                for horizon in HORIZONS}
        for model in (h1.MODEL_ECON, h1.MODEL_ROZN)
    }
    combo = {
        horizon: h1.combine_mean([live_cb[horizon], bs[h1.MODEL_ECON][horizon], bs[h1.MODEL_ROZN][horizon]])
        for horizon in HORIZONS
    }
    return live_cb, bs, combo


def evaluate(forecasts: dict, data: dict) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    normal = data["normal"]
    target_full = data["target_full"]
    truth = {horizon: target_full[horizon].dropna().to_dict() for horizon in HORIZONS}
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    cb_errors = {horizon: cp._cb_errors(data["archive_data"]["sheet"], target_full, horizon, normal)
                 for horizon in HORIZONS}
    errors, per_model = {}, {}
    for model, by_horizon in forecasts.items():
        errors[model], per_model[model] = {}, {}
        for horizon in HORIZONS:
            months, error = lb.errors_on(by_horizon[horizon], truth[horizon], normal)
            if months != normal:
                raise SystemExit(f"{model} h{horizon}: нет прогноза на части normal-месяцев")
            errors[model][horizon] = error
            _, error_live = lb.errors_on(by_horizon[horizon], data["t_live"][horizon], normal)
            _, error_all = lb.errors_on(by_horizon[horizon], truth[horizon], all_test)
            early = [value for month, value in zip(normal, error) if month.year <= 2021]
            late = [value for month, value in zip(normal, error) if month.year >= 2023]
            per_model[model][horizon] = {
                "vs_cb_target_full": w5b.compare_to_cb(error, cb_errors[horizon], horizon),
                "rmse_t_live": lb.rmse(error_live),
                "rmse_target_full_all67": lb.rmse(error_all),
                "rmse_2019_2021": lb.rmse(early),
                "rmse_2023_2024": lb.rmse(late),
            }
    pairs = {
        "pooled_main_vs_combo3": {horizon: v4.compare_pair(errors[MODEL_POOLED_MAIN][horizon], errors[MODEL_COMBO][horizon], horizon)
                                  for horizon in HORIZONS},
        "pooled_main_vs_live_cb": {horizon: v4.compare_pair(errors[MODEL_POOLED_MAIN][horizon], errors[MODEL_CB][horizon], horizon)
                                   for horizon in HORIZONS},
        "combo3_pooled_vs_combo3": {horizon: v4.compare_pair(errors[MODEL_COMBO_POOLED][horizon], errors[MODEL_COMBO][horizon], horizon)
                                    for horizon in HORIZONS},
        "pooled_control_vs_combo3": {horizon: v4.compare_pair(errors[MODEL_POOLED_CONTROL][horizon], errors[MODEL_COMBO][horizon], horizon)
                                     for horizon in HORIZONS},
    }
    mcs = {}
    for horizon in PRIMARY_HORIZONS:
        losses = {model: [value * value for value in errors[model][horizon]]
                  for model in (MODEL_POOLED_MAIN, MODEL_COMBO, MODEL_CB)}
        losses[CB_COMP] = [value * value for value in cb_errors[horizon]]
        included, pvalues = v4.mcs_included(losses)
        mcs[horizon] = {"included": sorted(included), "pvalues": pvalues}
    ratios = {model: {horizon: per_model[model][horizon]["vs_cb_target_full"]["ratio"] for horizon in HORIZONS}
              for model in forecasts}
    return {"per_model": per_model, "pairs": pairs, "mcs": mcs, "ratios": ratios}


def g10_check(pairs: dict) -> dict:
    """G10: ниже `combo3` на h3 или h6 при DM p < 0,10 и не хуже на h1."""
    def lower_pair(horizon):
        pair = pairs["pooled_main_vs_combo3"][horizon]
        lower = pair["rmse_left"] < pair["rmse_right"]
        return {
            "pooled": pair["rmse_left"], "combo3": pair["rmse_right"], "delta": pair["delta"],
            "delta_ci": pair["delta_ci"], "dm_p": pair["dm_p"], "lower": lower,
            "ok": lower and pair["dm_p"] < G10_ALPHA,
        }

    h1_pair = pairs["pooled_main_vs_combo3"][1]
    h3, h6 = lower_pair(3), lower_pair(6)
    h1 = {
        "pooled": h1_pair["rmse_left"], "combo3": h1_pair["rmse_right"],
        "delta": h1_pair["delta"], "delta_ci": h1_pair["delta_ci"], "dm_p": h1_pair["dm_p"],
        "ok": h1_pair["rmse_left"] <= h1_pair["rmse_right"],
    }
    return {"alpha": G10_ALPHA, "h1": h1, "h3": h3, "h6": h6,
            "ok": (h3["ok"] or h6["ok"]) and h1["ok"]}


def leak_check(reports: dict) -> dict:
    """R10c: ни один тестовый месяц года не входит в обучение; max(t) ≤ cutoff."""
    windows = {}
    for window, by_horizon in reports.items():
        for horizon, by_year in by_horizon.items():
            for year, info in by_year.items():
                train = set(info["train_months"])
                test = set(info["test_months"])
                overlap = sorted(train & test)
                max_target = info["max_target"]
                cutoff = info["cutoff"]
                ok = not overlap and max_target is not None and cutoff is not None and max_target <= cutoff
                windows[f"{window}:h{horizon}:{year}"] = {
                    "max_target": max_target, "cutoff": cutoff, "overlap": overlap, "ok": ok,
                }
    return {"windows": windows, "ok": all(value["ok"] for value in windows.values())}


def r10_check(ratios: dict, reports: dict, inputs: dict, groups_without_sa: list, min_coverage: int) -> dict:
    r10a = {
        "horizons": {horizon: {"ratio": ratios[MODEL_CB][horizon], "task3": TASK3_LIVE_CB_RATIOS[horizon],
                               "ok": abs(ratios[MODEL_CB][horizon] - TASK3_LIVE_CB_RATIOS[horizon]) <= R10_TOLERANCE}
                     for horizon in HORIZONS},
    }
    r10a["ok"] = all(value["ok"] for value in r10a["horizons"].values())
    r10b = {
        "horizons": {horizon: {"ratio": ratios[MODEL_COMBO][horizon], "task9": TASK9_COMBO_RATIOS[horizon],
                               "ok": abs(ratios[MODEL_COMBO][horizon] - TASK9_COMBO_RATIOS[horizon]) <= R10_TOLERANCE}
                     for horizon in HORIZONS},
    }
    r10b["ok"] = all(value["ok"] for value in r10b["horizons"].values())
    r10c = leak_check(reports)
    inputs_ok = {name: {"sha256": inputs.get(name, ""), "expected": expected, "ok": inputs.get(name) == expected}
                 for name, expected in EXPECTED_INPUTS.items()}
    r10d = {
        "inputs": inputs_ok,
        "groups_without_sa": groups_without_sa,
        "groups_without_sa_ok": list(groups_without_sa) == list(GROUPS_WITHOUT_SA),
        "min_coverage": min_coverage,
        "coverage_ok": min_coverage == GROUPS_EXPECTED,
    }
    r10d["ok"] = all(value["ok"] for value in inputs_ok.values()) and r10d["groups_without_sa_ok"] and r10d["coverage_ok"]
    return {"r10a": r10a, "r10b": r10b, "r10c": r10c, "r10d": r10d,
            "ok": r10a["ok"] and r10b["ok"] and r10c["ok"] and r10d["ok"]}


# ---------------------------------------------------------------------------
# Паспорт и прогон
# ---------------------------------------------------------------------------


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def git_sha() -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def sample_hash(reports: dict) -> str:
    digest = hashlib.sha256()
    for window in sorted(reports):
        for horizon in sorted(reports[window], key=int):
            for year in sorted(reports[window][horizon]):
                info = reports[window][horizon][year]
                for month in info["train_months"]:
                    digest.update(f"{window}|h{horizon}|{year}|{month.isoformat()}\n".encode())
    return digest.hexdigest()


def _jsonable(value):
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key.isoformat() if isinstance(key, date) else key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset)):
        return [_jsonable(item) for item in value]
    if isinstance(value, np.floating):
        value = float(value)
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def write_predictions(path: Path, forecasts: dict, data: dict, normal: list) -> None:
    target_full = {horizon: data["target_full"][horizon].dropna().to_dict() for horizon in HORIZONS}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "normal", "y_pred", "target_full", "t_live"])
        for model in sorted(forecasts):
            for horizon in HORIZONS:
                for month in sorted(forecasts[model][horizon]):
                    writer.writerow([
                        model, horizon, month.isoformat(), int(month in normal),
                        repr(forecasts[model][horizon][month]),
                        repr(target_full[horizon].get(month, float("nan"))),
                        repr(data["t_live"][horizon].get(month, float("nan"))),
                    ])


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, iterations: int = None) -> dict:
    data = en.prepare(archive, panel_csv, public_dir)
    sa_full, _sa_asof = lb.load_group_panel(panel_csv)
    normal, test_months = data["normal"], data["test_months"]
    overrides = {"iterations": iterations} if iterations else None

    pooled = {}
    reports = {}
    names = None
    sheets = None
    for window in (WINDOW_MAIN, WINDOW_CONTROL):
        pooled[window], reports[window], names, sheets = fit_predict_pooled(
            data["specs"], data["features"], data["targets"], data["weights"], sa_full,
            WINDOW_START[window], test_months, overrides,
        )

    pooled_main, coverage_main = aggregate_pooled(pooled[WINDOW_MAIN], data["weights"], test_months)
    pooled_control, coverage_control = aggregate_pooled(pooled[WINDOW_CONTROL], data["weights"], test_months)
    live_cb, bs, combo = reference_forecasts(data, public_dir)
    combo_pooled = combo_with_pooled(pooled_main, bs)
    forecasts = {
        MODEL_POOLED_MAIN: pooled_main,
        MODEL_POOLED_CONTROL: pooled_control,
        MODEL_COMBO: combo,
        MODEL_COMBO_POOLED: combo_pooled,
        MODEL_CB: live_cb,
    }

    evaluation = evaluate(forecasts, data)
    coverage = {MODEL_POOLED_MAIN: coverage_main, MODEL_POOLED_CONTROL: coverage_control}
    min_coverage = min(
        coverage[MODEL_POOLED_MAIN][horizon][month]
        for horizon in HORIZONS for month in normal
    )
    inputs = {
        "panel_csv": sha256(panel_csv),
        "public_panel_csv": sha256(public_dir / "public_panel.csv"),
        "sheet_map_csv": sha256(public_dir / "sheet_map.csv"),
        "target_xlsx": sha256(archive / cp.TARGET_XLSX),
    }
    r10 = r10_check(evaluation["ratios"], reports, inputs, data["groups_without_sa"], min_coverage)
    gate = g10_check(evaluation["pairs"])
    config = {
        "seed": bu.SEED,
        "iterations": iterations,
        "quick": bool(iterations),
        "horizons": list(HORIZONS),
        "windows": {window: WINDOW_START[window].isoformat() for window in WINDOW_START},
        "test": [test_months[0].isoformat(), test_months[-1].isoformat(), len(test_months)],
        "normal": len(normal),
        "features": {
            "base_numeric": list(BASE_NUMERIC),
            "block_columns": len(mp_block_names(sheets or [])),
            "cat_features": list(CAT_FEATURES),
            "weight": "share_strict_lag_no_fallback",
        },
        "catboost": {**bu.CATBOOST_DEFAULTS, "cat_features": list(CAT_FEATURES)},
        "annual_refit": "first origin of year; train t: available_from_mes(t) < cutoff(origin_min)",
        "g10_alpha": G10_ALPHA,
    }
    result = {
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-pooled-88-groups-design.md",
        "git_sha": git_sha(),
        "config": config,
        "config_hash": hashlib.sha256(json.dumps(_jsonable(config), sort_keys=True).encode()).hexdigest(),
        "inputs": inputs,
        "groups_without_sa": data["groups_without_sa"],
        "features": {"names": names, "sheets": sheets, "n_features": len(names or [])},
        "sample": reports,
        "sample_hash": sample_hash(reports),
        "coverage": {
            "pooled_main_min_groups": min_coverage,
            "pooled_main": {horizon: coverage[MODEL_POOLED_MAIN][horizon] for horizon in HORIZONS},
            "pooled_control_min_groups": min(
                coverage[MODEL_POOLED_CONTROL][horizon][month]
                for horizon in HORIZONS for month in normal
            ),
        },
        "r10": r10,
        "ratios": evaluation["ratios"],
        "per_model": evaluation["per_model"],
        "pairs": evaluation["pairs"],
        "mcs": evaluation["mcs"],
        "gate": gate,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    write_predictions(out_dir / "predictions.csv", forecasts, data, normal)
    (out_dir / "result.json").write_text(
        json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    if not r10["ok"]:
        raise SystemExit(f"R10 не пройдена: {json.dumps(_jsonable(r10), ensure_ascii=False)}")
    return result


def check_data(archive: Path, panel_csv: Path, public_dir: Path) -> dict:
    """Только входы и окна: без моделей и без записи (диагностика перед прогоном)."""
    data = en.prepare(archive, panel_csv, public_dir)
    sa_full, _sa_asof = lb.load_group_panel(panel_csv)
    output = {"groups_without_sa": data["groups_without_sa"], "windows": {}}
    for window in (WINDOW_MAIN, WINDOW_CONTROL):
        output["windows"][window] = {}
        for horizon in HORIZONS:
            output["windows"][window][str(horizon)] = {}
            shares = weight_shares(data["weights"], sa_full, cp.month_range(WINDOW_START[window], max(data["test_months"])))
            for year in sorted({month.year for month in data["test_months"]}):
                months = train_months(WINDOW_START[window], horizon, year)
                rows = training_rows(data["specs"], data["features"], data["targets"], shares, horizon, months)
                output["windows"][window][str(horizon)][str(year)] = {
                    "cutoff": annual_windows(WINDOW_START[window], horizon, (year,))[year]["cutoff"],
                    "n_months": len(months), "n_rows": len(rows), "n_groups": len({row.code for row in rows}),
                }
    return output


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(PANEL_CSV))
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR))
    parser.add_argument("--out", default=str(OUT_DIR))
    parser.add_argument("--iterations", type=int, default=None,
                        help="smoke-прогон: меньше итераций CatBoost; в паспорте quick=true")
    parser.add_argument("--check-data", action="store_true", help="только проверить входы и окна, без моделей")
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    if args.check_data:
        print(json.dumps(_jsonable(check_data(archive, Path(args.panel), Path(args.public_dir))),
                         ensure_ascii=False, indent=2))
        return 0
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.out), args.iterations)
    summary = {
        "r10": {"r10a": result["r10"]["r10a"]["ok"], "r10b": result["r10"]["r10b"]["ok"],
                "r10c": result["r10"]["r10c"]["ok"], "r10d": result["r10"]["r10d"]["ok"]},
        "ratios": {model: {str(horizon): round(value, 4) for horizon, value in per.items()}
                   for model, per in result["ratios"].items()},
        "gate": result["gate"],
        "mcs": result["mcs"],
        "coverage": result["coverage"]["pooled_main_min_groups"],
    }
    print(json.dumps(_jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
