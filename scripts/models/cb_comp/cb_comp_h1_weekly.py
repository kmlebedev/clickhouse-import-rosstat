#!/usr/bin/env python3
"""h1: недельный ИПЦ поверх `combo3` — Task 4 #424.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h1-weekly-design.md`.

* **база** — `combo3` Task 9: `mean(live_cb, bs_econ16, bs_rozn16)`;
* **`live_cb_wk`** — CatBoost группы с недельными признаками `wk_g_mtd`,
  `wk_g_cov` (маппинг 148 недельных позиций → 88 групп);
* **`combo3_wk`** — `mean(live_cb_wk, bs_econ16, bs_rozn16)`;
* **`wk_agg`** — OLS на агрегатных `wkagg_mtd`/`wkagg_4w`, цель `T_LIVE[1]`;
* **`wk_mix`** — `0,5·combo3 + 0,5·wk_agg`;
* **оценка** — окно CB_COMP (frozen, normal-54, справочно) и walk-forward
  2022+ (ежемесячный expanding, истина `T_LIVE[1]`, primary); G10 с Holm.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-h1-weekly`, тесты — `make cb-comp-h1-weekly-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as bs9
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cb_comp import cb_comp_weekly_groups as wg
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_public_panel_audit as audit

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §2–§8)
# ---------------------------------------------------------------------------

HORIZON = 1
HORIZONS = (HORIZON,)
MODEL_LIVE_CB = "live_cb"
MODEL_LIVE_CB_WK = "live_cb_wk"
MODEL_ECON = "bs_econ16"
MODEL_ROZN = "bs_rozn16"
MODEL_COMBO = "combo3"
MODEL_COMBO_WK = "combo3_wk"
MODEL_WK_AGG = "wk_agg"
MODEL_WK_MIX = "wk_mix"
CB_COMP = "cb_comp"
CANDIDATES = (
    MODEL_LIVE_CB, MODEL_LIVE_CB_WK, MODEL_ECON, MODEL_ROZN,
    MODEL_COMBO, MODEL_COMBO_WK, MODEL_WK_AGG, MODEL_WK_MIX,
)
CO_PRIMARY = (MODEL_COMBO_WK, MODEL_WK_MIX)
WEEKLY_NAMES = ("wk_g_mtd", "wk_g_cov")
AGG_NAMES = ("wkagg_mtd", "wkagg_4w")

WF_START = date(2022, 1, 1)
SHOCK_MONTHS = frozenset(bs9.ARTICLE_OUTLIERS)
HOLM_ALPHA = 0.025
R10_TOL = 1e-6
R10_PINNED = {
    MODEL_LIVE_CB: 0.1894308454893399,
    MODEL_COMBO: 0.16432658972448905,
    MODEL_ECON: 0.16670577961045163,
    MODEL_ROZN: 0.16892971554783076,
}
# Task 14: пины R10 по версии цели; @2009 заполняется после прогона Task 9 @2009.
VERSIONS = lb.VERSIONS
DEFAULT_VERSION = lb.DEFAULT_VERSION
R10_PINS = {
    "2010": R10_PINNED,
    "2009": {
        MODEL_LIVE_CB: 0.19789285710086799,
        MODEL_COMBO: 0.16523960261042756,
        MODEL_ECON: 0.1665537602380189,
        MODEL_ROZN: 0.167724527009985,
    },
}
R11_EXPECTED = {
    "n_matched": 118, "n_in_group": 112, "n_groups": 52,
    "n_groups_without_weekly": 36, "n_outside_groups": 6,
}

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
OUT_DIR = ROOT / "build" / "cb-comp-h1-weekly"


# ---------------------------------------------------------------------------
# Данные: расширенная подготовка Task 3/9
# ---------------------------------------------------------------------------


def prepare(archive: Path, panel_csv: Path, public_dir: Path) -> dict:
    """Данные Task 3/9 + признаки на всём окне до последнего месяца панели."""
    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    feature_rows, _ = cp.load_feature_rows()
    parity_rows, _ = cp.build_rows(feature_rows, target_full)
    cp.assert_no_leakage(parity_rows)
    train_rows, test_rows = cp.split_rows(parity_rows)
    macro = {row.month: row.features for row in feature_rows}
    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    specs = bu.load_specs(archive, sheet_map)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, lb.WEIGHT_YEARS, specs, source="archive")
    sa_full, _sa_asof = lb.load_group_panel(panel_csv)
    last = max(m for values in sa_full.values() for m in values)
    first = lb.first_sa_month(sa_full)
    months = cp.month_range(first, last)
    diff_full = lb.log_diff_frame(sa_full, codes, months)
    feature_months = cp.month_range(date(2010, 3, 1), last)
    features = bu.feature_table(
        specs, feature_months, diff_full, macro, bu._sheet_lookup(public_panel, sheet_map), feature_mode="per_sheet"
    )
    return {
        "archive_data": archive_data, "target_full": target_full, "normal": archive_data["normal"],
        "specs": specs, "codes": codes, "weights": weights, "months": months,
        "targets": bu.component_targets(diff_full),
        "t_live": lb.t_live({c: sa_full.get(c, {}) for c in codes}, weights, months),
        "features": features, "feature_months": feature_months,
        "train_months": [row.month for row in train_rows], "test_months": [row.month for row in test_rows],
        "groups_without_sa": sorted(c for c in codes if not sa_full.get(c)),
    }


# ---------------------------------------------------------------------------
# Недельные данные и признаки
# ---------------------------------------------------------------------------


def weekly_inputs(archive: Path, specs: list) -> dict:
    """Маппинг, периоды и веса — один раз на прогон."""
    codes = [spec.code for spec in specs]
    weekly = audit.load_weekly()
    aggregate = weekly.get(audit.WEEKLY_AGGREGATE, [])
    if not aggregate:
        raise SystemExit(f"нет агрегатного недельного ряда {audit.WEEKLY_AGGREGATE!r}")
    products = sorted(series for series in weekly if series != audit.WEEKLY_AGGREGATE)
    dictionary = wg.load_group_dictionary(archive)
    weights_2024 = audit.load_weights(archive, wg.DICTIONARY_YEAR)
    rows, unmatched = wg.map_weekly_series(products, weights_2024, dictionary, codes)
    return {
        "weekly": weekly, "aggregate": aggregate, "products": products,
        "mapping": rows, "unmatched": unmatched,
        "summary": wg.mapping_summary(rows, codes),
        "by_group": wg.mapping_by_group(rows),
        "periods": wg.periods_by_series({s: weekly[s] for s in products}),
        "leaf_weights": wg.load_leaf_weights(archive),
        "group_weights": bu.load_weights(archive, lb.WEIGHT_YEARS, specs, source="archive"),
        "first": min(day for points in weekly.values() for day, _ in points),
        "last": max(day for points in weekly.values() for day, _ in points),
        "n_points": sum(len(points) for points in weekly.values()),
    }


def monthly_weekly_features(weekly: dict, months: list, codes: list) -> dict:
    """`{месяц: {код: {wk_g_mtd, wk_g_cov}}}` на активном cutoff (`late`/`publication`)."""
    out = {}
    for month in months:
        cutoff = cm.cutoff_for(month)
        leaf = wg.year_table(weekly["leaf_weights"], wg.weight_year_for(month))
        groups = bu.weight_for_month(weekly["group_weights"], month)
        out[month] = wg.weekly_group_features(
            weekly["periods"], weekly["by_group"], leaf, groups, month, cutoff, codes
        )
    return out


def aggregate_monthly(weekly: dict, months: list) -> dict:
    return {month: wg.aggregate_features(weekly["aggregate"], month, cm.cutoff_for(month)) for month in months}


def attach_weekly(features: dict, weekly_features: dict) -> dict:
    """Копия таблицы признаков с добавленными `wk_g_mtd`/`wk_g_cov`."""
    out = {}
    for code, per_month in features.items():
        out[code] = {}
        for month, values in per_month.items():
            item = weekly_features.get(month, {}).get(code, {})
            out[code][month] = {**values, "wk_g_mtd": item.get("wk_g_mtd", float("nan")), "wk_g_cov": item.get("wk_g_cov", 0.0)}
    return out


def coverage_by_year(weekly_features: dict, weights: dict, months: list) -> list:
    """Доля групп и веса с непустым `wk_g_mtd` по годам."""
    rows = []
    for year in sorted({m.year for m in months}):
        year_months = [m for m in months if m.year == year]
        total = covered = 0.0
        n_groups = n_covered = 0
        for month in year_months:
            group_weights = bu.weight_for_month(weights, month)
            for code, values in weekly_features.get(month, {}).items():
                weight = float(group_weights.get(code, 0.0))
                total += weight
                n_groups += 1
                if not math.isnan(values["wk_g_mtd"]):
                    covered += weight
                    n_covered += 1
        rows.append({
            "year": year, "months": len(year_months),
            "group_months": n_groups, "group_months_covered": n_covered,
            "weight_share": covered / total if total else 0.0,
            "group_share": n_covered / n_groups if n_groups else 0.0,
        })
    return rows


# ---------------------------------------------------------------------------
# Модели: frozen и ежемесячный walk-forward
# ---------------------------------------------------------------------------


def model_names(spec, with_mp: bool, extra=()) -> list:
    use_mp = with_mp and bool(spec.bsu_codes)
    base = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
    return list(base) + list(extra)


def fit_predict_frozen(specs: list, features: dict, targets: dict, train_months: list, test_months: list,
                       with_mp: bool, extra=()) -> dict:
    """Одна подгонка на горизонт h1 (как `cb_comp_live_bottomup.fit_predict`)."""
    out = {}
    for spec in specs:
        names = model_names(spec, with_mp, extra)
        bu.assert_no_leakage_bottomup(test_months, HORIZON)
        train_rows = [
            row for row in bu.model_rows(spec.code, features.get(spec.code, {}), targets[HORIZON], HORIZON, train_months)
            if not pd.isna(row[2][HORIZON])
        ]
        if len(train_rows) < bu.MIN_TRAIN_ROWS:
            continue
        test_rows = bu.model_rows(spec.code, features.get(spec.code, {}), targets[HORIZON], HORIZON, test_months)
        if not test_rows:
            continue
        model = bu.fit_component(train_rows, names, HORIZON)
        predictions = bu.predict_component(model, test_rows, names)
        out.setdefault(spec.code, {})[HORIZON] = {row[0]: value for row, value in zip(test_rows, predictions)}
    return out


def wf_train_months(train_months: list, month: date) -> list:
    """Train walk-forward: месяцы строго до M и без шоков статьи."""
    return [t for t in train_months if t < month and t not in SHOCK_MONTHS]


def _group_walk_forward(spec, features: dict, targets: dict, train_months: list, test_months: list,
                        with_mp: bool, extra: tuple) -> tuple:
    names = model_names(spec, with_mp, extra)
    out = {}
    for month in test_months:
        train = wf_train_months(train_months, month)
        train_rows = [
            row for row in bu.model_rows(spec.code, features.get(spec.code, {}), targets[HORIZON], HORIZON, train)
            if not pd.isna(row[2][HORIZON])
        ]
        if len(train_rows) < bu.MIN_TRAIN_ROWS:
            continue
        test_rows = bu.model_rows(spec.code, features.get(spec.code, {}), targets[HORIZON], HORIZON, [month])
        if not test_rows:
            continue
        model = bu.fit_component(train_rows, names, HORIZON, thread_count=1)
        out[month] = bu.predict_component(model, test_rows, names)[0]
    return spec.code, {HORIZON: out}


def fit_predict_wf(specs: list, features: dict, targets: dict, train_months: list, test_months: list,
                   with_mp: bool, extra=(), jobs: int = 4) -> dict:
    """Ежемесячный expanding walk-forward: train `t ≤ M−1` без шоков, перефит на каждом M."""
    out = {}
    with ThreadPoolExecutor(max_workers=max(1, jobs)) as pool:
        futures = [
            pool.submit(_group_walk_forward, spec, features, targets, train_months, test_months, with_mp, tuple(extra))
            for spec in specs
        ]
        for future in futures:
            code, values = future.result()
            out[code] = values
    return out


def aggregate_h1(predictions: dict, weights: dict, months: list) -> dict:
    by_group = {code: per_h[HORIZON] for code, per_h in predictions.items() if per_h.get(HORIZON)}
    values, _coverage = bu.aggregate(by_group, weights, months)
    return {m: v for m, v in values.items() if not math.isnan(v)}


def bs_forecasts(target: dict, regressors: dict, months: list) -> dict:
    return {name: bs9.bs_predict(target, regressors[name], HORIZON, months) for name in (MODEL_ECON, MODEL_ROZN)}


def ols_agg_predict(features: dict, target: dict, train_months: list, test_months: list) -> dict:
    """OLS `T_LIVE[1] ~ wkagg_mtd + wkagg_4w`; NaN — медиана train, пустая колонка — 0."""
    train = [m for m in train_months if m in features and m in target and m not in SHOCK_MONTHS]
    if len(train) < bu.MIN_TRAIN_ROWS:
        return {}
    x_train = np.asarray([[features[m].get(name, float("nan")) for name in AGG_NAMES] for m in train], dtype=float)
    medians = np.nanmedian(x_train, axis=0)
    medians = np.where(np.isnan(medians), 0.0, medians)
    x_train = np.where(np.isnan(x_train), medians, x_train)
    y_train = np.asarray([target[m] for m in train], dtype=float)
    model = LinearRegression().fit(x_train, y_train)
    out = {}
    for month in test_months:
        if month not in features:
            continue
        row = np.asarray([[features[month].get(name, float("nan")) for name in AGG_NAMES]], dtype=float)
        out[month] = float(model.predict(np.where(np.isnan(row), medians, row))[0])
    return out


def ols_agg_wf(features: dict, target: dict, train_months: list, test_months: list) -> dict:
    out = {}
    for month in test_months:
        pred = ols_agg_predict(features, target, [t for t in train_months if t < month], [month])
        if month in pred:
            out[month] = pred[month]
    return out


def combine_mean(predictions: list) -> dict:
    common = set(predictions[0])
    for pred in predictions[1:]:
        common &= set(pred)
    return {month: sum(p[month] for p in predictions) / len(predictions) for month in sorted(common)}


def mix(first: dict, second: dict) -> dict:
    common = sorted(set(first) & set(second))
    return {m: 0.5 * first[m] + 0.5 * second[m] for m in common}


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def evaluate(models: dict, truth: dict, months_required: list, cb_errors: dict, base: str = MODEL_COMBO) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    errors, per_model, pairs = {}, {}, {}
    for name, values in models.items():
        if not values:
            continue
        got_months, err = lb.errors_on(values, truth, months_required)
        if got_months != months_required:
            raise SystemExit(f"{name}: нет прогноза на {len(months_required) - len(got_months)} месяцах из {len(months_required)}")
        errors[name] = err
        per_model[name] = {
            "rmse": lb.rmse(err), "mae": float(np.mean(np.abs(err))), "bias": float(np.mean(err)),
            "n": len(err), "vs_cb": w5b.compare_to_cb(err, cb_errors, HORIZON) if cb_errors else None,
        }
    for name in models:
        if name in errors and base in errors:
            pairs[name] = v4.compare_pair(errors[name], errors[base], HORIZON) if name != base else None
    return {"per_model": per_model, "pairs_vs_combo3": pairs, "errors": errors}


def mcs(models_errors: dict, cb_errors=None) -> dict:
    import cpi_v4_leaderboard as v4

    losses = {name: [e * e for e in errors] for name, errors in models_errors.items()}
    if cb_errors:
        losses[CB_COMP] = [e * e for e in cb_errors]
    included, pvalues = v4.mcs_included(losses)
    return {"included": sorted(included), "pvalues": pvalues}


def subperiods(models: dict, truth: dict, months: list) -> dict:
    out = {}
    for label, test in (
        ("2022", lambda m: m.year == 2022),
        ("2023_2024", lambda m: 2023 <= m.year <= 2024),
        ("2025_2026", lambda m: m.year >= 2025),
    ):
        subset = [m for m in months if test(m)]
        out[label] = {
            name: lb.rmse([values[m] - truth[m] for m in subset if m in values and m in truth])
            for name, values in models.items() if values
        }
    return out


UTIL_MONTHS = frozenset({date(2022, 12, 1), date(2024, 7, 1), date(2025, 7, 1), date(2026, 1, 1)})


def sensitivity_slices(months: list) -> dict:
    """Пост-хок срезы ревью 2026-10-31; в G10 не участвуют."""
    return {
        "all": list(months),
        "no_2022_03": [m for m in months if m != date(2022, 3, 1)],
        "util_months": [m for m in months if m in UTIL_MONTHS],
        "aug_sep_2023": [m for m in months if m.year >= 2023 and m.month in (8, 9)],
    }


def sensitivity(models: dict, truth: dict, months: list) -> dict:
    """RMSE по срезам (2022-03, месяцы ЖКУ, август–сентябрь) и 56-месячная пара."""
    subsets = sensitivity_slices(months)
    out = {"note": "пост-хок диагностика по ревью 2026-10-31; в G10 не участвует"}
    for label, subset in subsets.items():
        if not subset:
            continue
        out[label] = {
            "n": len(subset),
            "rmse": {
                name: lb.rmse([values[m] - truth[m] for m in subset if m in values and m in truth])
                for name, values in models.items() if values
            },
        }
    month = date(2022, 3, 1)
    if month in months:
        out["errors_2022_03"] = {
            name: values[month] - truth[month] for name, values in models.items() if month in values and month in truth
        }
    if subsets["no_2022_03"]:
        out["no_2022_03"]["pairs_vs_combo3"] = evaluate(models, truth, subsets["no_2022_03"], None)["pairs_vs_combo3"]
    return out


def holm(pvalues: dict) -> dict:
    """Holm: p_(1)·m, далее max(предыдущий, p_(i)·(m−i+1)); не Бонферрони."""
    ordered = sorted(pvalues.items(), key=lambda item: item[1])
    total = len(ordered)
    adjusted, running = {}, 0.0
    for index, (name, value) in enumerate(ordered):
        candidate = min(1.0, (total - index) * value)
        running = max(running, candidate)
        adjusted[name] = min(1.0, running)
    return adjusted


def g10_check(pairs: dict, n_required: int) -> dict:
    pvalues = {name: pairs[name]["dm_p"] for name in CO_PRIMARY if pairs.get(name)}
    adjusted = holm(pvalues)
    winner = min(pvalues, key=pvalues.get) if pvalues else None
    pair = pairs.get(winner) if winner else None
    ok = bool(
        winner and pair and pair["n"] == n_required
        and adjusted[winner] < 2 * HOLM_ALPHA
        and pair["delta_ci"][1] < 0
    )
    return {
        "winner": winner, "pvalues": pvalues, "holm": adjusted,
        "alpha": HOLM_ALPHA, "n_required": n_required,
        "delta_ci": pair["delta_ci"] if pair else None, "ok": ok,
    }


def r10_check(per_model: dict, pins: dict = None) -> dict:
    pins = pins or R10_PINNED
    out = {}
    for name, expected in pins.items():
        got = per_model[name]["rmse"]
        out[name] = {"rmse": got, "expected": expected, "abs_diff": abs(got - expected), "ok": abs(got - expected) <= R10_TOL}
    return {"models": out, "ok": all(item["ok"] for item in out.values())}


def r11_check(weekly: dict, months: list) -> dict:
    counts = {key: weekly["summary"][key] for key in R11_EXPECTED}
    availability = {}
    for month in months:
        boundary = cm.cutoff_for(month) - timedelta(days=wg.WEEKLY_LAG_DAYS)
        start, end = wg.month_window(month)
        dates = [day for points in weekly["weekly"].values() for day, _value in points if start <= day <= end]
        last = max(dates) if dates else None
        availability[month.isoformat()] = {
            "last_point": last.isoformat() if last else None,
            "boundary": boundary.isoformat(),
            "ok": last is None or last <= boundary,
        }
    return {
        "counts": counts, "expected": R11_EXPECTED,
        "counts_ok": all(counts[key] == R11_EXPECTED[key] for key in R11_EXPECTED),
        "availability_ok": all(item["ok"] for item in availability.values()),
        "availability": availability,
        "weekly_first": weekly["first"].isoformat(), "weekly_last": weekly["last"].isoformat(),
        "aggregate_last": max(day for day, _ in weekly["aggregate"]).isoformat(),
    }


def r12_check(late: dict, publication: dict, agg_late: dict, agg_publication: dict, codes: list) -> dict:
    pairs = []
    for month in sorted(set(late) & set(publication)):
        for code in codes:
            left = late[month].get(code, {}).get("wk_g_mtd", float("nan"))
            right = publication[month].get(code, {}).get("wk_g_mtd", float("nan"))
            if not math.isnan(left) and not math.isnan(right):
                pairs.append((left, right))
    if len(pairs) > 2:
        left = np.asarray([item[0] for item in pairs])
        right = np.asarray([item[1] for item in pairs])
        corr = float(np.corrcoef(left, right)[0, 1]) if np.std(left) > 0 and np.std(right) > 0 else float("nan")
        mae = float(np.mean(np.abs(left - right)))
    else:
        corr = mae = float("nan")
    agg_pairs = [
        (agg_late[m]["wkagg_mtd"], agg_publication[m]["wkagg_mtd"])
        for m in sorted(set(agg_late) & set(agg_publication))
        if not math.isnan(agg_late[m]["wkagg_mtd"]) and not math.isnan(agg_publication[m]["wkagg_mtd"])
    ]
    return {
        "groups_n": len(pairs), "groups_corr": corr, "groups_mae_pp": mae,
        "agg_n": len(agg_pairs),
        "agg_mae_pp": float(np.mean([abs(a - b) for a, b in agg_pairs])) if agg_pairs else float("nan"),
        "note": "в моделях используется late; publication — только диагностика",
    }


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, jobs: int = 4,
        frozen_only: bool = False, version: str = DEFAULT_VERSION) -> dict:
    data = prepare(archive, panel_csv, public_dir)
    regressors = bs9.load_regressors(public_dir)
    weekly = weekly_inputs(archive, data["specs"])
    summary = weekly["summary"]
    if not all(summary[key] == R11_EXPECTED[key] for key in R11_EXPECTED):
        raise SystemExit(f"R11: маппинг не совпал: {summary}")
    r11 = r11_check(weekly, data["feature_months"])
    weekly_features = monthly_weekly_features(weekly, data["feature_months"], data["codes"])
    agg_features = aggregate_monthly(weekly, data["feature_months"])
    with cm.asof_convention("publication"):
        weekly_publication = monthly_weekly_features(weekly, data["feature_months"], data["codes"])
        agg_publication = aggregate_monthly(weekly, data["feature_months"])
    r12 = r12_check(weekly_features, weekly_publication, agg_features, agg_publication, data["codes"])

    features_wk = attach_weekly(data["features"], weekly_features)
    truth_full = {HORIZON: {m: float(v) for m, v in data["target_full"][HORIZON].dropna().items()}}
    t_live = data["t_live"][HORIZON]
    cb_errors = cp._cb_errors(data["archive_data"]["sheet"], data["target_full"], HORIZON, data["normal"])

    # --- Окно CB_COMP: frozen, как Task 9 (R10) ---
    groups_cb = lb.fit_predict(data["specs"], data["features"], data["features"], data["targets"], data["train_months"], data["test_months"], with_mp=True)
    live_cb = aggregate_h1(groups_cb, data["weights"], data["test_months"])
    bs = bs_forecasts(t_live, regressors, data["test_months"])
    frozen = {
        MODEL_LIVE_CB: live_cb,
        MODEL_ECON: bs[MODEL_ECON],
        MODEL_ROZN: bs[MODEL_ROZN],
        MODEL_COMBO: combine_mean([live_cb, bs[MODEL_ECON], bs[MODEL_ROZN]]),
    }
    groups_wk = fit_predict_frozen(data["specs"], features_wk, data["targets"], data["train_months"], data["test_months"], with_mp=True, extra=WEEKLY_NAMES)
    live_cb_wk = aggregate_h1(groups_wk, data["weights"], data["test_months"])
    wk_agg_frozen = ols_agg_predict(agg_features, t_live, data["train_months"], data["test_months"])
    frozen_all = {
        **frozen,
        MODEL_LIVE_CB_WK: live_cb_wk,
        MODEL_COMBO_WK: combine_mean([live_cb_wk, bs[MODEL_ECON], bs[MODEL_ROZN]]),
        MODEL_WK_AGG: wk_agg_frozen,
        MODEL_WK_MIX: mix(frozen[MODEL_COMBO], wk_agg_frozen),
    }
    frozen_eval = evaluate(frozen_all, truth_full[HORIZON], data["normal"], cb_errors)
    r10 = r10_check(frozen_eval["per_model"], R10_PINS[version])
    if not r10["ok"]:
        raise SystemExit(f"R10 не пройден: {r10}")

    result = {
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h1-weekly-design.md",
        "version": version,
        "git_sha": lb.git_sha(),
        "inputs": {
            "panel_csv": sha256(panel_csv), "public_panel_csv": sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": sha256(public_dir / "sheet_map.csv"), "target_xlsx": sha256(archive / cp.TARGET_XLSX),
        },
        "weekly": {
            "n_products": len(weekly["products"]), "first": r11["weekly_first"], "last": r11["weekly_last"],
            "aggregate_first": min(day for day, _ in weekly["aggregate"]).isoformat(),
            "aggregate_last": r11["aggregate_last"], "n_points": weekly["n_points"],
        },
        "mapping": {
            "summary": summary,
            "coverage": wg.group_coverage(
                weekly["mapping"], data["codes"], data["weights"][max(data["weights"])],
                wg.year_table(weekly["leaf_weights"], wg.DICTIONARY_YEAR),
            ),
        },
        "r10": r10,
        "r11": r11,
        "r12": r12,
        "coverage_by_year": coverage_by_year(weekly_features, data["weights"], data["feature_months"]),
        "frozen": {
            "evaluation": frozen_eval,
            "mcs": mcs(frozen_eval["errors"], cb_errors),
        },
        "groups_without_sa": data["groups_without_sa"],
        "limitations": [
            "недельные ряды — последняя редакция, винтажей нет",
            "лаги SA — всей выборки (как combo3), не as-of",
            "маппинг фиксирован словарём 2024",
            "36 групп без недельных данных — NaN",
            "late-cutoff; publication — только R12",
        ],
    }
    wf_models = {}
    wf_months = []
    if not frozen_only:
        wf_months = [m for m in data["feature_months"] if m >= WF_START and m in t_live]
        all_train = [m for m in data["feature_months"] if m >= date(2010, 3, 1)]
        groups_wf = fit_predict_wf(data["specs"], data["features"], data["targets"], all_train, wf_months, True, (), jobs)
        live_wf = aggregate_h1(groups_wf, data["weights"], wf_months)
        groups_wk_wf = fit_predict_wf(data["specs"], features_wk, data["targets"], all_train, wf_months, True, WEEKLY_NAMES, jobs)
        live_wk_wf = aggregate_h1(groups_wk_wf, data["weights"], wf_months)
        bs_wf = bs_forecasts(t_live, regressors, wf_months)
        wf_models = {
            MODEL_LIVE_CB: live_wf, MODEL_LIVE_CB_WK: live_wk_wf,
            MODEL_ECON: bs_wf[MODEL_ECON], MODEL_ROZN: bs_wf[MODEL_ROZN],
            MODEL_COMBO: combine_mean([live_wf, bs_wf[MODEL_ECON], bs_wf[MODEL_ROZN]]),
            MODEL_COMBO_WK: combine_mean([live_wk_wf, bs_wf[MODEL_ECON], bs_wf[MODEL_ROZN]]),
        }
        wf_models[MODEL_WK_AGG] = ols_agg_wf(agg_features, t_live, all_train, wf_months)
        wf_models[MODEL_WK_MIX] = mix(wf_models[MODEL_COMBO], wf_models[MODEL_WK_AGG])
        wf_eval = evaluate(wf_models, t_live, wf_months, None)
        ref_months = [m for m in wf_months if m in frozen_all[MODEL_COMBO] and m in t_live]
        result["walk_forward"] = {
            "months": [m.isoformat() for m in wf_months],
            "n_months": len(wf_months),
            "evaluation": wf_eval,
            "mcs": mcs(wf_eval["errors"]),
            "subperiods": subperiods(wf_models, t_live, wf_months),
            "sensitivity": sensitivity(wf_models, t_live, wf_months),
            "g10": g10_check(wf_eval["pairs_vs_combo3"], len(wf_months)),
            "frozen_reference": {
                "n_months": len(ref_months),
                "rmse_t_live": {
                    name: lb.rmse([values[m] - t_live[m] for m in ref_months if m in values])
                    for name, values in frozen_all.items()
                },
            },
        }
    out_dir.mkdir(parents=True, exist_ok=True)
    write_predictions(out_dir / "predictions.csv", frozen_all, wf_models, data, wf_months)
    write_mapping(out_dir / "mapping.csv", weekly["mapping"])
    (out_dir / "result.json").write_text(json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def write_predictions(path: Path, frozen: dict, wf_models: dict, data: dict, wf_months: list) -> None:
    normal = set(data["normal"])
    t_live = data["t_live"][HORIZON]
    target_full = {m: float(v) for m, v in data["target_full"][HORIZON].dropna().items()}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["arena", "model", "month", "y_pred", "t_live", "target_full", "normal"])
        for model, values in sorted(frozen.items()):
            for month, value in sorted(values.items()):
                writer.writerow(["cb_comp", model, month.isoformat(), repr(value),
                                 repr(t_live.get(month, float("nan"))), repr(target_full.get(month, float("nan"))),
                                 int(month in normal)])
        for model, values in sorted(wf_models.items()):
            for month, value in sorted(values.items()):
                writer.writerow(["wf2022", model, month.isoformat(), repr(value),
                                 repr(t_live.get(month, float("nan"))), "", 0])


def write_mapping(path: Path, rows: list) -> None:
    columns = ("series", "weight_code", "weight_2024", "level", "group_code", "group_name", "matched", "in_group")
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="по умолчанию — панель версии")
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR))
    parser.add_argument("--out", default=None, help="по умолчанию build/cb-comp-h1-weekly[-<версия>]")
    parser.add_argument("--jobs", type=int, default=4)
    parser.add_argument("--version", choices=VERSIONS, default=DEFAULT_VERSION,
                        help="версия цели: 2010 — канон, 2009 — T_LIVE с 2009-01")
    parser.add_argument("--frozen-only", action="store_true", help="только R10 и окно CB_COMP; проверка перед long-run")
    args = parser.parse_args(argv)
    panel = Path(args.panel) if args.panel else lb.default_panel(args.version)
    lb.check_panel_version(lb.load_group_panel(panel)[0], args.version)
    out_dir = Path(args.out) if args.out else lb.versioned_dir(OUT_DIR, args.version)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, panel, Path(args.public_dir), out_dir, args.jobs, args.frozen_only, args.version)
    summary = {
        "r10": result["r10"]["ok"], "r11": result["r11"]["counts_ok"] and result["r11"]["availability_ok"],
        "frozen": {
            name: {"rmse": per["rmse"], "ratio": (per["vs_cb"] or {}).get("ratio"), "class": (per["vs_cb"] or {}).get("class")}
            for name, per in result["frozen"]["evaluation"]["per_model"].items()
        },
    }
    if "walk_forward" in result:
        summary["g10"] = result["walk_forward"]["g10"]
        summary["wf"] = {name: {"rmse": per["rmse"], "n": per["n"]} for name, per in result["walk_forward"]["evaluation"]["per_model"].items()}
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
