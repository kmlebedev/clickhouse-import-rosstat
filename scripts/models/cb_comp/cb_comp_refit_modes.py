#!/usr/bin/env python3
"""Task 5 #425: режим переобучения `live_cb` — frozen / annual / rolling и гипотеза W5b.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md` §17
(записан до кода и результатов). Решение владельца Q1 = A: база — `live_cb`,
`combo3` — справочная строка.

* **кандидаты** — `frozen`, `annual`, `annual_shock`, `annual_dummy`,
  `rolling96`, `rolling120`; контроли — `frozen_honest`, `frozen_asof`,
  `annual_asof`, `wf_shock`, `wf_noshock`;
* **годовой перефит** — в начале года, cutoff по первому origin года
  (`late`: 8-е `M+1`), train без окон `t−h+1…t`, пересекающих шоки
  (2014-12…2015-03, 2020-04, 2022) — кроме `annual_shock`/`annual_dummy`;
* **гипотеза W5b** — месячный expanding wf на `live_cb` (h1/h6) и прямой тест
  на `v3_direct`/`v5_direct` (`cpi_two_stage_cb_comp.hypothesis_check`);
* **гейт** — no-harm к `frozen` на normal-54 и 2023–24 (2 %), все 67 месяцев
  (10 %), DM-HLN p ≥ 0,05, MCS 90 %; решение `adopt:<mode>` или `keep_frozen`.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-refit-modes`, тесты — `make cb-comp-refit-modes-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as h1
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_parity as cp
from models.cb_comp import cb_comp_pooled as pl
from models.cpi import cpi_monthly as cm

# ---------------------------------------------------------------------------
# Предзарегистрированные константы (спека §17)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
WINDOW_START = date(2010, 3, 1)
SHOCK_MONTHS = cp.TRAIN_OUTLIER_MONTHS | cp.OUTLIER_MONTHS
SHOCK_FEATURE = "shock"
ROLLING_MONTHS = {"rolling96": 96, "rolling120": 120}

MODE_FROZEN = "frozen"
MODE_FROZEN_HONEST = "frozen_honest"
MODE_ANNUAL = "annual"
MODE_ANNUAL_SHOCK = "annual_shock"
MODE_ANNUAL_DUMMY = "annual_dummy"
MODE_FROZEN_ASOF = "frozen_asof"
MODE_ANNUAL_ASOF = "annual_asof"
MODE_WF_SHOCK = "wf_shock"
MODE_WF_NOSHOCK = "wf_noshock"
MODE_COMBO = "combo3"
CB_COMP = "cb_comp"

CANDIDATES = (MODE_FROZEN, MODE_ANNUAL, MODE_ANNUAL_SHOCK, MODE_ANNUAL_DUMMY, "rolling96", "rolling120")
CONTROLS = (MODE_FROZEN_HONEST, MODE_FROZEN_ASOF, MODE_ANNUAL_ASOF, MODE_WF_SHOCK, MODE_WF_NOSHOCK)
WF_HORIZONS = (1, 6)
WF_MODES = (MODE_WF_SHOCK, MODE_WF_NOSHOCK)
# Простота режима — tie-break выбора (меньше — проще).
MODE_RANK = {MODE_FROZEN: 0, MODE_ANNUAL: 1, MODE_ANNUAL_SHOCK: 2, MODE_ANNUAL_DUMMY: 3, "rolling96": 4, "rolling120": 5}

NOHARM_MARGIN = 0.02
SHOCK_WINDOW_MARGIN = 0.10
DM_ALPHA = 0.05
SUB_PERIODS = {"2019": (2019,), "2020_2021": (2020, 2021), "2023_2024": (2023, 2024)}

PIN_TOLERANCE = 1e-6
MODEL_ECON = h1.MODEL_ECON
MODEL_ROZN = h1.MODEL_ROZN
CB_COMP_RMSE = {1: 0.158805, 2: 0.296655, 3: 0.443623, 6: 0.971459}

PANEL_CSV = ROOT / "build" / "cb-comp-groups" / "panel.csv"
PUBLIC_DIR = ROOT / "build" / "cb-comp-parity"
OUT_DIR = ROOT / "build" / "cb-comp-refit-modes"
W5B_CACHE = ROOT / "build" / "cpi-two-stage" / "w5b-cache-task5"


# ---------------------------------------------------------------------------
# Окна и train-месяцы
# ---------------------------------------------------------------------------


def window_touches(month: date, horizon: int, excluded=SHOCK_MONTHS) -> bool:
    """Окно накопленной цели `t−h+1…t` пересекает исключённый месяц."""
    return any(cp.shift_month(month, -offset) in excluded for offset in range(horizon))


def train_months(cutoff: date, horizon: int, window_start: date = WINDOW_START,
                 exclude_shocks: bool = True, rolling: int = None) -> list:
    """Целевые месяцы обучения: `[start, cutoff]`, без окон с шоками.

    `rolling` — длина окна в месяцах; начало не раньше `window_start`.
    """
    if cutoff is None:
        return []
    start = window_start if rolling is None else max(window_start, cp.shift_month(cutoff, -(rolling - 1)))
    months = cp.month_range(start, cutoff)
    if exclude_shocks:
        months = [month for month in months if not window_touches(month, horizon)]
    return months


def training_cutoff(window_start: date, origin_min: date) -> date:
    """Последний целевой месяц, опубликованный к cutoff первого origin года.

    `available_from_mes(month_end(t)) < cutoff_for(origin_min)` (конвенция `late`).
    В отличие от `cb_comp_pooled.training_cutoff`, шоковые месяцы не пропускаются:
    иначе `annual_shock` терял бы весь 2022 и тренировался бы до 2021-12.
    """
    last = None
    for month in cp.month_range(window_start, origin_min):
        if cm.available_from_mes(cm.month_end(month)) < cm.cutoff_for(origin_min):
            last = month
        else:
            break
    return last


def annual_windows(horizon: int, years: list, mode: str) -> dict:
    """`{год: {origin_min, cutoff, train}}` — один фит на год и горизонт."""
    rolling = ROLLING_MONTHS.get(mode)
    exclude = mode not in (MODE_ANNUAL_SHOCK, MODE_ANNUAL_DUMMY)
    out = {}
    for year in years:
        origin_min = cp.shift_month(date(year, 1, 1), -(horizon - 1))
        cutoff = training_cutoff(WINDOW_START, origin_min)
        out[year] = {
            "origin_min": origin_min,
            "cutoff": cutoff,
            "train": train_months(cutoff, horizon, exclude_shocks=exclude, rolling=rolling),
        }
    return out


def assert_no_leakage(months: list, cutoff: date, horizon: int) -> None:
    """Train-месяцы не позже cutoff (origin окна — `M−(h−1)`, как в Task 2)."""
    if cutoff is None:
        raise AssertionError("cutoff не определён")
    later = [month for month in months if month > cutoff]
    if later:
        raise AssertionError(f"утечка: train-месяцы после cutoff {cutoff}: {later[:3]}")


# ---------------------------------------------------------------------------
# Строки и модели
# ---------------------------------------------------------------------------


def model_names(spec, with_mp: bool, extra=()) -> list:
    use_mp = with_mp and bool(spec.bsu_codes)
    names = bu.feature_names(with_mp=use_mp, mode="per_sheet", spec=spec) if use_mp else bu.feature_names(with_mp=False)
    return list(names) + list(extra)


def train_rows(spec, features: dict, targets: dict, horizon: int, months: list, shock_feature: bool = False) -> list:
    rows = [
        row for row in bu.model_rows(spec.code, features.get(spec.code, {}), targets[horizon], horizon, months)
        if not pd.isna(row[2][horizon])
    ]
    if shock_feature:
        rows = [(month, {**feat, SHOCK_FEATURE: float(window_touches(month, horizon))}, value)
                for month, feat, value in rows]
    return rows


def test_rows(spec, features: dict, targets: dict, horizon: int, months: list, shock_feature: bool = False) -> list:
    rows = bu.model_rows(spec.code, features.get(spec.code, {}), targets[horizon], horizon, months)
    if shock_feature:
        rows = [(month, {**feat, SHOCK_FEATURE: 0.0}, value) for month, feat, value in rows]
    return rows


def _per_group(pool: int, specs: list, worker) -> dict:
    out = {}
    with ThreadPoolExecutor(max_workers=max(1, pool)) as executor:
        futures = [executor.submit(worker, spec) for spec in specs]
        for future in futures:
            code, values = future.result()
            if values:
                out[code] = values
    return out


def fit_predict_frozen(data: dict, features_test: dict, test_months: list) -> dict:
    """Canonical frozen — тем же кодом, что Task 3 (дефолты, без thread_count)."""
    return lb.fit_predict(data["specs"], data["features"], features_test, data["targets"],
                          data["train_months"], test_months, with_mp=True)


def fit_predict_frozen_honest(data: dict, test_months: list, pool: int = 8) -> dict:
    """Frozen с cutoff по первому origin года для каждого h — контроль асимметрии 2019."""
    first = min(test_months)

    def worker(spec):
        out = {}
        names = model_names(spec, True)
        for horizon in HORIZONS:
            origin_min = cp.shift_month(first, -(horizon - 1))
            cutoff = training_cutoff(WINDOW_START, origin_min)
            months = train_months(cutoff, horizon, exclude_shocks=True)
            assert_no_leakage(months, cutoff, horizon)
            rows = train_rows(spec, data["features"], data["targets"], horizon, months)
            if len(rows) < bu.MIN_TRAIN_ROWS:
                continue
            rows_test = test_rows(spec, data["features"], data["targets"], horizon, test_months)
            if not rows_test:
                continue
            model = bu.fit_component(rows, names, horizon, thread_count=1)
            values = bu.predict_component(model, rows_test, names)
            out[horizon] = {row[0]: value for row, value in zip(rows_test, values)}
        return spec.code, out

    return _per_group(pool, data["specs"], worker)


def fit_predict_annual(data: dict, test_months: list, mode: str, pool: int = 8,
                       features_test: dict = None) -> dict:
    """Годовой перефит: один фит на (группа, h, год), прогноз месяцев своего года."""
    features_test = features_test if features_test is not None else data["features"]
    years = sorted({month.year for month in test_months})
    windows = {horizon: annual_windows(horizon, years, mode) for horizon in HORIZONS}
    by_year = {year: [month for month in test_months if month.year == year] for year in years}
    shock_feature = mode == MODE_ANNUAL_DUMMY

    def worker(spec):
        out = {}
        extra = [SHOCK_FEATURE] if shock_feature else ()
        names = model_names(spec, True, extra)
        for horizon in HORIZONS:
            for year in years:
                window = windows[horizon][year]
                months = window["train"]
                assert_no_leakage(months, window["cutoff"], horizon)
                rows = train_rows(spec, data["features"], data["targets"], horizon, months, shock_feature)
                if len(rows) < bu.MIN_TRAIN_ROWS:
                    continue
                rows_test = test_rows(spec, features_test, data["targets"], horizon, by_year[year], shock_feature)
                if not rows_test:
                    continue
                model = bu.fit_component(rows, names, horizon, thread_count=1)
                values = bu.predict_component(model, rows_test, names)
                out.setdefault(horizon, {}).update({row[0]: value for row, value in zip(rows_test, values)})
        return spec.code, out

    return _per_group(pool, data["specs"], worker)


def fit_predict_wf(data: dict, test_months: list, exclude_shocks: bool, pool: int = 8,
                   horizons=WF_HORIZONS) -> dict:
    """Месячный expanding wf: train `[WINDOW_START, cutoff(origin)]`, перефит на каждом M."""
    def worker(spec):
        out = {}
        names = model_names(spec, True)
        for horizon in horizons:
            bu.assert_no_leakage_bottomup(test_months, horizon)
            for month in test_months:
                origin = cp.shift_month(month, -(horizon - 1))
                cutoff = training_cutoff(WINDOW_START, origin)
                months = train_months(cutoff, horizon, exclude_shocks=exclude_shocks)
                assert_no_leakage(months, cutoff, horizon)
                rows = train_rows(spec, data["features"], data["targets"], horizon, months)
                if len(rows) < bu.MIN_TRAIN_ROWS:
                    continue
                rows_test = test_rows(spec, data["features"], data["targets"], horizon, [month])
                if not rows_test:
                    continue
                model = bu.fit_component(rows, names, horizon, thread_count=1)
                out.setdefault(horizon, {})[month] = bu.predict_component(model, rows_test, names)[0]
        return spec.code, out

    return _per_group(pool, data["specs"], worker)


def aggregate(predictions: dict, data: dict, test_months: list) -> dict:
    out = {}
    for horizon in HORIZONS:
        by_group = {code: per_h[horizon] for code, per_h in predictions.items() if per_h.get(horizon)}
        if not by_group:
            continue
        values, _coverage = bu.aggregate(by_group, data["weights"], test_months)
        out[horizon] = {month: value for month, value in values.items() if not math.isnan(value)}
    return out


# ---------------------------------------------------------------------------
# Данные
# ---------------------------------------------------------------------------


def prepare(archive: Path, panel_csv: Path, public_dir: Path) -> dict:
    """Те же шаги, что `cb_comp_live_en.prepare`, плюс as-of признаки для контролей."""
    archive_data = cp.check_archive(archive)
    target_full = archive_data["target"]
    feature_rows, _ = cp.load_feature_rows()
    parity_rows, _ = cp.build_rows(feature_rows, target_full)
    cp.assert_no_leakage(parity_rows)
    split_train, split_test = cp.split_rows(parity_rows)
    train_months_ = [row.month for row in split_train]
    test_months = [row.month for row in split_test]
    macro = {row.month: row.features for row in feature_rows}
    public_panel = bu.load_public_panel(public_dir / "public_panel.csv")
    sheet_map = bu.load_sheet_map(public_dir / "sheet_map.csv")
    specs = bu.load_specs(archive, sheet_map)
    codes = [spec.code for spec in specs]
    weights = bu.load_weights(archive, lb.WEIGHT_YEARS, specs)
    sa_full, sa_asof = lb.load_group_panel(panel_csv)
    lb.check_panel_version(sa_full, lb.DEFAULT_VERSION)
    groups_without_sa = sorted(code for code in codes if not sa_full.get(code))
    if len(groups_without_sa) > 2:
        raise SystemExit(f"в панели нет SA у {groups_without_sa}: панель не та, что в Task 1")
    months = cp.month_range(lb.first_sa_month(sa_full), max(m for values in sa_full.values() for m in values))
    diff_full = lb.log_diff_frame(sa_full, codes, months)
    mp_lookup = bu._sheet_lookup(public_panel, sheet_map)
    feature_months = sorted(set(train_months_) | set(test_months))
    features = bu.feature_table(specs, feature_months, diff_full, macro, mp_lookup, feature_mode="per_sheet")
    features_asof = None
    if sa_asof:
        diff_asof = lb.log_diff_frame(lb.asof_series(sa_full, sa_asof), codes, months)
        features_asof = bu.feature_table(specs, feature_months, diff_asof, macro, mp_lookup, feature_mode="per_sheet")
    return {
        "archive_data": archive_data, "target_full": target_full, "normal": archive_data["normal"],
        "specs": specs, "weights": weights, "targets": bu.component_targets(diff_full),
        "t_live": lb.t_live({code: sa_full.get(code, {}) for code in codes}, weights, months),
        "features": features, "features_asof": features_asof,
        "train_months": train_months_, "test_months": test_months,
        "groups_without_sa": groups_without_sa,
    }


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def mean_abs(errors: list) -> float:
    return float(np.mean([abs(value) for value in errors])) if errors else float("nan")


def mean(errors: list) -> float:
    return float(np.mean(errors)) if errors else float("nan")


def subperiod(errors: list, months: list, years: tuple) -> dict:
    picked = [error for error, month in zip(errors, months) if month.year in years]
    return {"n": len(picked), "rmse": lb.rmse(picked), "mae": mean_abs(picked), "bias": mean(picked)}


def error_sets(aggregated: dict, data: dict) -> dict:
    """`{mode: {h: {month: error}}}` на normal-54 и на всех 67 месяцах."""
    normal = data["normal"]
    truth = {horizon: data["target_full"][horizon].dropna().to_dict() for horizon in HORIZONS}
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    out = {}
    for mode, by_h in aggregated.items():
        out[mode] = {}
        for horizon in HORIZONS:
            preds = by_h.get(horizon) or {}
            months, errors = lb.errors_on(preds, truth[horizon], normal)
            all_months, errors_all = lb.errors_on(preds, truth[horizon], all_test)
            out[mode][horizon] = {
                "normal": {month: error for month, error in zip(months, errors)},
                "all67": {month: error for month, error in zip(all_months, errors_all)},
            }
    return out


def evaluate_modes(raw: dict, data: dict, cb_errors: dict) -> dict:
    """Метрики режимов: RMSE/MAE/bias, отношения, DM/CI к frozen, подпериоды."""
    import cpi_v4_leaderboard as v4

    if MODE_FROZEN not in raw:
        raise SystemExit("в пуле нет frozen — не с чем сравнивать")
    normal = data["normal"]
    out = {}
    for mode in raw:
        out[mode] = {}
        for horizon in HORIZONS:
            frozen_errors = raw[MODE_FROZEN][horizon]["normal"]
            if not frozen_errors:
                continue
            errors = raw[mode][horizon]["normal"]
            if not errors:
                continue
            months = [month for month in normal if month in errors]
            if mode in CANDIDATES and months != [month for month in normal if month in frozen_errors]:
                raise SystemExit(f"{mode} h{horizon}: нет прогноза на части normal-месяцев")
            values = [errors[month] for month in months]
            frozen_values = [frozen_errors[month] for month in months]
            rmse = lb.rmse(values)
            rmse_frozen = lb.rmse(frozen_values)
            rmse_cb = lb.rmse(cb_errors[horizon])
            all67 = raw[mode][horizon]["all67"]
            frozen_all67 = raw[MODE_FROZEN][horizon]["all67"]
            common_all = [month for month in all67 if month in frozen_all67]
            pair = ({"n": len(values), "rmse_left": rmse, "rmse_right": rmse, "delta": 0.0,
                     "delta_ci": [0.0, 0.0], "dm_stat": 0.0, "dm_p": 1.0, "mde_ratio": 0.0}
                    if mode == MODE_FROZEN else v4.compare_pair(values, frozen_values, horizon))
            out[mode][horizon] = {
                "n": len(values),
                "rmse": rmse,
                "mae": mean_abs(values),
                "bias": mean(values),
                "rmse_frozen": rmse_frozen,
                "ratio_frozen": rmse / rmse_frozen if rmse_frozen else float("nan"),
                "rmse_cb": rmse_cb,
                "ratio_cb": rmse / rmse_cb if rmse_cb else float("nan"),
                "n_all67": len(common_all),
                "rmse_all67": lb.rmse([all67[month] for month in common_all]),
                "ratio_all67_frozen": lb.rmse([all67[month] for month in common_all]) / lb.rmse(
                    [frozen_all67[month] for month in common_all]),
                "pair_vs_frozen": pair,
                "subperiods": {name: subperiod(values, months, years) for name, years in SUB_PERIODS.items()},
            }
    return out


def mcs_from_errors(raw: dict, cb_errors: dict) -> dict:
    """90 % MCS по кандидатам и CB_COMP на normal-54."""
    import cpi_v4_leaderboard as v4

    out = {}
    for horizon in PRIMARY_HORIZONS:
        months = sorted(raw[MODE_FROZEN][horizon]["normal"])
        losses = {}
        for mode in CANDIDATES:
            if mode not in raw or horizon not in raw[mode]:
                raise SystemExit(f"MCS: у {mode} нет метрик h{horizon}")
            values = raw[mode][horizon]["normal"]
            if sorted(values) != months:
                raise SystemExit(f"MCS: {mode} h{horizon} не на всех normal-месяцах")
            losses[mode] = [values[month] ** 2 for month in months]
        if len(cb_errors[horizon]) != len(months):
            raise SystemExit(f"MCS: длины CB_COMP и кандидатов не совпадают на h{horizon}")
        losses[CB_COMP] = [error * error for error in cb_errors[horizon]]
        included, pvalues = v4.mcs_included(losses)
        out[horizon] = {"n": len(months), "included": sorted(included), "pvalues": pvalues}
    return out


# ---------------------------------------------------------------------------
# Гейт и гипотеза
# ---------------------------------------------------------------------------


def pin_check(values: dict, expected: dict) -> dict:
    per_h = {horizon: {"ratio": values[horizon], "pin": expected[horizon],
                       "diff": values[horizon] - expected[horizon],
                       "ok": abs(values[horizon] - expected[horizon]) <= PIN_TOLERANCE}
             for horizon in expected}
    return {"horizons": per_h, "ok": all(item["ok"] for item in per_h.values())}


def gate(per_mode: dict, mcs_by_h: dict) -> dict:
    """Предзарегистрированный гейт §17.3; `keep_frozen`, если не прошёл никто."""
    checks = {}
    for mode in CANDIDATES:
        if mode not in per_mode:
            checks[mode] = {"ok": False, "reason": "нет метрик"}
            continue
        per_h, scores = {}, []
        for horizon in PRIMARY_HORIZONS:
            item = per_mode[mode][horizon]
            pair = item["pair_vs_frozen"]
            frozen_2023 = per_mode[MODE_FROZEN][horizon]["subperiods"]["2023_2024"]["rmse"]
            passed = {
                "p1_normal": item["ratio_frozen"] <= 1 + NOHARM_MARGIN + 1e-12,
                "p2_2023_2024": item["subperiods"]["2023_2024"]["rmse"] <= (1 + NOHARM_MARGIN + 1e-12) * frozen_2023,
                "p3_all67": item["ratio_all67_frozen"] <= 1 + SHOCK_WINDOW_MARGIN + 1e-12,
                "p4_dm_ci": pair["dm_p"] >= DM_ALPHA and pair["delta_ci"][0] <= 0,
                "p5_mcs": mode in mcs_by_h[horizon]["included"],
            }
            per_h[horizon] = {**passed, "ok": all(passed.values())}
            scores.append(item["ratio_frozen"])
        checks[mode] = {
            "horizons": per_h,
            "ok": all(item["ok"] for item in per_h.values()),
            "score": float(np.mean(scores)),
        }
    passed = [mode for mode in CANDIDATES if checks[mode]["ok"]]
    if passed:
        best = min(passed, key=lambda mode: (round(checks[mode]["score"], 12), MODE_RANK[mode]))
        decision = "keep_frozen" if best == MODE_FROZEN else f"adopt:{best}"
    else:
        best, decision = MODE_FROZEN, "keep_frozen"
    return {"candidates": checks, "passed": passed, "selected": best, "decision": decision}


def hypothesis_live_cb(per_mode: dict) -> dict:
    """Тест B: месячный wf на `live_cb` с шоками и без — правило §17.5."""
    import cpi_two_stage_cb_comp as w5b

    horizons = {}
    for horizon in WF_HORIZONS:
        if MODE_WF_SHOCK not in per_mode or horizon not in per_mode[MODE_WF_SHOCK]:
            continue
        horizons[str(horizon)] = w5b.hypothesis_verdict(
            per_mode[MODE_WF_SHOCK][horizon]["rmse"],
            per_mode[MODE_WF_NOSHOCK][horizon]["rmse"],
            per_mode[MODE_FROZEN][horizon]["rmse"],
        )
    primary = horizons.get(str(WF_HORIZONS[-1]), {})
    return {"horizons": horizons, "verdict": primary.get("verdict", "not_tested")}


def hypothesis_overall(w5b_body: dict, live_body: dict) -> dict:
    a = w5b_body.get("verdict") if w5b_body else None
    b = live_body.get("verdict")
    if a is None:
        overall = b if b in ("support", "reject") else "partial"
        note = "тест A не выполнялся (--skip-w5b)"
    else:
        overall = "support" if a == "support" and b == "support" else (
            "reject" if a == "reject" and b == "reject" else "partial")
        note = ""
    return {"A_w5b": a, "B_live_cb": b, "verdict": overall, "note": note}


# ---------------------------------------------------------------------------
# Паспорт
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


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path, jobs: int = 8,
        skip_w5b: bool = False, skip_asof: bool = False, cache_dir: Path = None) -> dict:
    import cpi_two_stage_cb_comp as w5b

    data = prepare(archive, panel_csv, public_dir)
    test_months = data["test_months"]

    fits = {
        MODE_FROZEN: fit_predict_frozen(data, data["features"], test_months),
        MODE_FROZEN_HONEST: fit_predict_frozen_honest(data, test_months, jobs),
    }
    for mode in CANDIDATES:
        if mode == MODE_FROZEN:
            continue
        fits[mode] = fit_predict_annual(data, test_months, mode, jobs)
    if not skip_asof and data["features_asof"] is not None:
        fits[MODE_FROZEN_ASOF] = fit_predict_frozen(data, data["features_asof"], test_months)
        fits[MODE_ANNUAL_ASOF] = fit_predict_annual(data, test_months, MODE_ANNUAL, jobs, data["features_asof"])
    for mode, exclude in ((MODE_WF_SHOCK, False), (MODE_WF_NOSHOCK, True)):
        fits[mode] = fit_predict_wf(data, test_months, exclude_shocks=exclude, pool=jobs)

    aggregated = {mode: aggregate(predictions, data, test_months) for mode, predictions in fits.items()}
    regressors = h1.load_regressors(public_dir)
    bs = {horizon: {name: h1.bs_predict(data["t_live"][horizon], regressors[name], horizon, test_months)
                    for name in (MODEL_ECON, MODEL_ROZN)}
          for horizon in HORIZONS}
    combos = {}
    for mode, by_h in aggregated.items():
        if mode in WF_MODES:
            continue
        combos[mode] = {
            horizon: h1.combine_mean([by_h[horizon], bs[horizon][MODEL_ECON], bs[horizon][MODEL_ROZN]])
            for horizon in HORIZONS if by_h.get(horizon)
        }

    cb_errors = {horizon: cp._cb_errors(data["archive_data"]["sheet"], data["target_full"], horizon, data["normal"])
                 for horizon in HORIZONS}
    raw = error_sets(aggregated, data)
    per_mode = evaluate_modes(raw, data, cb_errors)
    per_combo = evaluate_modes(error_sets(combos, data), data, cb_errors)
    mcs_by_h = mcs_from_errors(raw, cb_errors)
    gate_body = gate(per_mode, mcs_by_h)

    r14a = pin_check({horizon: per_mode[MODE_FROZEN][horizon]["ratio_cb"] for horizon in HORIZONS},
                     pl.TASK3_LIVE_CB_RATIOS)
    r14b = pin_check({horizon: per_combo[MODE_FROZEN][horizon]["ratio_cb"] for horizon in HORIZONS},
                     pl.TASK9_COMBO_RATIOS)
    if not r14a["ok"]:
        raise SystemExit(f"R14a не пройден (live_cb@frozen ≠ Task 3): {r14a}")
    if not r14b["ok"]:
        raise SystemExit(f"R14b не пройден (combo3@frozen ≠ Task 9): {r14b}")

    live_hyp = hypothesis_live_cb(per_mode)
    w5b_body = None
    if not skip_w5b:
        w5b_body = w5b.hypothesis_check(archive, cache_dir=cache_dir)
        if not all(model["pin_ok"] for model in w5b_body["models"].values()):
            raise SystemExit(f"R15 не пройден (W5b wf не воспроизведён): {w5b_body}")
    hypothesis = {"w5b": w5b_body, "live_cb": live_hyp,
                  "overall": hypothesis_overall(w5b_body, live_hyp)}

    config = {
        "seed": bu.SEED, "horizons": list(HORIZONS), "primary": list(PRIMARY_HORIZONS),
        "window_start": WINDOW_START.isoformat(), "shocks": [month.isoformat() for month in sorted(SHOCK_MONTHS)],
        "candidates": list(CANDIDATES), "controls": list(CONTROLS), "wf_horizons": list(WF_HORIZONS),
        "margins": {"noharm": NOHARM_MARGIN, "all67": SHOCK_WINDOW_MARGIN, "dm_alpha": DM_ALPHA},
        "rolling": ROLLING_MONTHS, "skip_asof": skip_asof, "skip_w5b": skip_w5b,
    }
    inputs = {
        "panel_csv": sha256(panel_csv),
        "public_panel_csv": sha256(public_dir / "public_panel.csv"),
        "sheet_map_csv": sha256(public_dir / "sheet_map.csv"),
        "target_xlsx": sha256(archive / cp.TARGET_XLSX),
        "series2016_xlsx": sha256(archive / h1.SERIES2016),
        "series2009_xlsx": sha256(archive / h1.SERIES2009),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = out_dir / "predictions.csv"
    write_predictions(predictions_path, aggregated, combos, data, test_months)
    result = {
        "spec": "docs/superpowers/specs/2026-10-30-cb-comp-88-groups-live-design.md §17",
        "git_sha": git_sha(),
        "config": config,
        "config_hash": hashlib.sha256(json.dumps(_jsonable(config), sort_keys=True).encode()).hexdigest(),
        "inputs": inputs,
        "pins": {"r14a_live_cb_frozen": r14a, "r14b_combo3_frozen": r14b},
        "groups_without_sa": data["groups_without_sa"],
        "modes": per_mode,
        "combos": per_combo,
        "mcs": mcs_by_h,
        "gate": gate_body,
        "hypothesis": hypothesis,
        "predictions_sha256": sha256(predictions_path),
    }
    (out_dir / "result.json").write_text(
        json.dumps(_jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result


def write_predictions(path: Path, aggregated: dict, combos: dict, data: dict, test_months: list) -> None:
    truth = {horizon: data["target_full"][horizon].dropna().to_dict() for horizon in HORIZONS}
    normal = set(data["normal"])
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["kind", "model", "horizon", "month", "normal", "y_pred", "target_full", "t_live"])
        for kind, pool in (("live_cb", aggregated), ("combo3", combos)):
            for mode in sorted(pool):
                for horizon in HORIZONS:
                    for month in sorted(pool[mode].get(horizon, {})):
                        writer.writerow([kind, mode, horizon, month.isoformat(), int(month in normal),
                                         repr(pool[mode][horizon][month]),
                                         repr(truth[horizon].get(month, float("nan"))),
                                         repr(data["t_live"][horizon].get(month, float("nan")))])


def summary(result: dict) -> dict:
    return {
        "gate": {"decision": result["gate"]["decision"], "selected": result["gate"]["selected"],
                 "passed": result["gate"]["passed"],
                 "scores": {mode: round(body.get("score", float("nan")), 4)
                            for mode, body in result["gate"]["candidates"].items()}},
        "modes": {mode: {horizon: {"rmse": round(body["rmse"], 4), "ratio_frozen": round(body["ratio_frozen"], 4),
                                   "ratio_cb": round(body["ratio_cb"], 4), "n": body["n"]}
                         for horizon, body in per.items()}
                  for mode, per in result["modes"].items()},
        "combos": {mode: {horizon: {"rmse": round(body["rmse"], 4), "ratio_cb": round(body["ratio_cb"], 4)}
                          for horizon, body in per.items()}
                   for mode, per in result["combos"].items()},
        "hypothesis": {"overall": result["hypothesis"]["overall"],
                       "A": result["hypothesis"]["w5b"]["verdict"] if result["hypothesis"]["w5b"] else None,
                       "B": result["hypothesis"]["live_cb"]["verdict"]},
        "pins": {"r14a": result["pins"]["r14a_live_cb_frozen"]["ok"],
                 "r14b": result["pins"]["r14b_combo3_frozen"]["ok"]},
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=None, help="panel.csv (C0, спека §13.1); по умолчанию build/cb-comp-groups/panel.csv")
    parser.add_argument("--public-dir", default=str(PUBLIC_DIR), help="каталог public_panel.csv и sheet_map.csv")
    parser.add_argument("--out", default=None, help=f"по умолчанию {OUT_DIR}")
    parser.add_argument("--jobs", type=int, default=8, help="параллелизм по группам (фиты thread_count=1)")
    parser.add_argument("--cache-dir", default=str(W5B_CACHE), help="кэш wf-предсказаний W5b")
    parser.add_argument("--skip-w5b", action="store_true", help="без прямого теста гипотезы на v3/v5_direct")
    parser.add_argument("--skip-asof", action="store_true", help="без контролей frozen_asof/annual_asof")
    parser.add_argument("--w5b-only", action="store_true", help="только диагностика W5b")
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    out_dir = Path(args.out) if args.out else OUT_DIR
    cache_dir = Path(args.cache_dir) if args.cache_dir else None
    if args.w5b_only:
        import cpi_two_stage_cb_comp as w5b

        body = w5b.hypothesis_check(archive, cache_dir=cache_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "hypothesis_w5b.json").write_text(
            json.dumps(_jsonable(body), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        print(json.dumps(_jsonable({"verdict": body["verdict"]}), ensure_ascii=False))
        return 0
    panel = Path(args.panel) if args.panel else PANEL_CSV
    result = run(archive, panel, Path(args.public_dir), out_dir, args.jobs, args.skip_w5b, args.skip_asof, cache_dir)
    print(json.dumps(_jsonable(summary(result)), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
