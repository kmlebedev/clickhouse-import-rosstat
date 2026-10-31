#!/usr/bin/env python3
"""h3/h6: агрегатный VAR/BVAR (ставка, курс, ценовые ожидания) в комбинации — Task 12 #440.

Пункт `key-rate/phase-4-cb-comp-88-groups-live`, зонтик #419. Протокол —
спека `docs/superpowers/specs/2026-10-31-cb-comp-h3-h6-var-design.md`.

* **`var16`** — VAR по `[log(1 + SA ИПЦ без КУ м/м / 100), Δставка, Δlog USD, SA ценовые ожидания]`,
  окно 2016+, лаг 1…6 по BIC внутри train, помесячное переобучение, накопление
  лог-ИПЦ на h шагов;
* **`bvar16`** — тот же VAR с Minnesota prior (λ = 0,2, posterior mean) — справочная линия;
* **`combo3`** — Task 9 без изменений (`live_cb` + BS_ECON16 + BS_ROZN16);
* **`combo3_var16`** — среднее `combo3` и `var16` с равными весами;
* **R12** — сверка `combo3`/`live_cb` с Task 9, аудит панели и полноты прогноза;
* **оценка** (normal-54) — против CB_COMP, попарно против `combo3`/`live_cb`, MCS 90 %, гейт G12.

Только чтение ClickHouse и локальных файлов; в БД не пишет. Запуск —
`make cb-comp-h3-h6-var`, тесты — `make cb-comp-h3-h6-var-test`.
"""

import argparse
import csv
import hashlib
import json
import math
import statistics
import subprocess
import sys
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR

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
# Предзарегистрированные константы (спека §3–§7)
# ---------------------------------------------------------------------------

HORIZONS = bu.HORIZONS
PRIMARY_HORIZONS = (1, 3, 6)
PANEL_START = date(2015, 12, 1)
PANEL_END = date(2024, 6, 1)
VAR_START = date(2016, 1, 1)
MAX_LAG = 6
MIN_LAG_OBS = 24
ENDOG_COLUMNS = ("cpi", "d_rate", "d_fx", "pe")
CPI_COLUMN = 0
BVAR_LAMBDA = 0.2

MODEL_CB = "live_cb"
MODEL_COMBO3 = "combo3"
MODEL_VAR = "var16"
MODEL_BVAR = "bvar16"
MODEL_COMBO4 = "combo3_var16"
CANDIDATES = (MODEL_CB, MODEL_COMBO3, MODEL_VAR, MODEL_BVAR, MODEL_COMBO4)
CB_COMP = "cb_comp"

G12_ALPHA = 0.10
NOHARM_MARGIN = 0.02
R12A_TOL = 1e-6
R12A_PINNED = {
    "combo3": {1: 1.0347684348311972, 2: 0.9571087729271289, 3: 1.0101981596603171, 6: 0.8940353315189836},
    "live_cb": {1: 1.1928505290859988, 2: 1.0127041023069754, 3: 1.0758201741459672, 6: 1.0616906586271115},
}

OUT_DIR = ROOT / "build" / "cb-comp-h3-h6-var"


# ---------------------------------------------------------------------------
# Панель: ИПЦ, ставка, курс, ценовые ожидания
# ---------------------------------------------------------------------------


def monthly_rate(points: list, months: list) -> dict:
    """Последняя действующая ставка на конец каждого месяца."""
    out = {}
    for month in months:
        value = cm.asof(points, cm.available_from_rate, cm.month_end(month))
        if value is None:
            raise SystemExit(f"cbr_key_rate: нет ставки на конец {month}")
        out[month] = float(value)
    return out


def monthly_usd_log(points: list, months: list) -> dict:
    """`log(среднее дневных USD за календарный месяц)`."""
    baskets = {month: [] for month in months}
    for day, value in points:
        key = cm.month_of(day)
        if key in baskets:
            baskets[key].append(float(value))
    out = {}
    for month in months:
        if not baskets[month]:
            raise SystemExit(f"cbr_currency_usd: нет точек за {month}")
        out[month] = math.log(statistics.mean(baskets[month]))
    return out


def monthly_pe(survey: dict, months: list) -> dict:
    """Уровень SA ценовых ожиданий ЦБ по месяцам."""
    points = dict(survey["price_expectations_sa"])
    out = {}
    for month in months:
        if month not in points:
            raise SystemExit(f"cbr_survey/price_expectations_sa: нет точки за {month}")
        out[month] = float(points[month])
    return out


def build_panel(mm: dict, rate: dict, usd_log: dict, pe: dict, months: list) -> tuple:
    """`(panel, endo, dummies)`: уровни и разности, эндогенные строки 2016-01+."""
    index = pd.date_range(pd.Timestamp(months[0]), pd.Timestamp(months[-1]), freq="MS")
    if len(index) != len(months) or [day.date() for day in index] != list(months):
        raise SystemExit("панель VAR: месяцы не непрерывны")
    missing = [month for month in months if month not in mm]
    if missing:
        raise SystemExit(f"T_LIVE[1]: нет месяцев {missing[:3]}…")
    frame = pd.DataFrame(
        {
            "cpi": [math.log1p(mm[month] / 100) for month in months],
            "rate": [rate[month] for month in months],
            "fx": [usd_log[month] for month in months],
            "pe": [pe[month] for month in months],
        },
        index=index,
    )
    frame["d_rate"] = frame["rate"].diff()
    frame["d_fx"] = frame["fx"].diff()
    endo = frame.loc[pd.Timestamp(VAR_START):, list(ENDOG_COLUMNS)]
    if endo.isna().any().any():
        raise SystemExit("панель VAR: NaN в эндогенных строках")
    dummies = pd.DataFrame(
        {
            f"outlier_{month.isoformat()}": (frame.index == pd.Timestamp(month)).astype(float)
            for month in sorted(cp.OUTLIER_MONTHS)
            if pd.Timestamp(month) in frame.index
        },
        index=frame.index,
    )
    return frame, endo, dummies


def load_var_panel(data: dict) -> tuple:
    """Панель из ClickHouse и `T_LIVE`; `(panel, endo, dummies, audit)`."""
    months = cp.month_range(PANEL_START, PANEL_END)
    mm = data["t_live"][1]
    rate = monthly_rate(cm.load_key_rate(), months)
    usd_log = monthly_usd_log(cm.load_usd(), months)
    pe = monthly_pe(cm.load_survey(), months)
    panel, endo, dummies = build_panel(mm, rate, usd_log, pe, months)
    audit = {
        "first": panel.index[0].date().isoformat(),
        "last": panel.index[-1].date().isoformat(),
        "n_panel": len(panel),
        "n_endo": len(endo),
        "columns": list(panel.columns),
        "ranges": {column: [float(panel[column].min()), float(panel[column].max())] for column in ("cpi", "rate", "fx", "pe")},
    }
    return panel, endo, dummies, audit


# ---------------------------------------------------------------------------
# VAR и BVAR
# ---------------------------------------------------------------------------


def eligible_lag(n_obs: int, lag: int, n_exog: int) -> bool:
    """Лаг идентифицируем: наблюдений больше числа параметров уравнения."""
    return n_obs - lag > len(ENDOG_COLUMNS) * lag + 1 + n_exog and n_obs - lag >= MIN_LAG_OBS


def training_exog(dummies: pd.DataFrame, index: pd.Index):
    """Dummies только для выбросов, уже наблюдённых в train; `None`, если их нет."""
    if dummies.empty:
        return None
    part = dummies.loc[index]
    keep = [column for column in part.columns if part[column].abs().sum() > 0]
    return part[keep] if keep else None


def select_lag(train: pd.DataFrame, exog) -> tuple:
    """`(лаг, BIC по лагам, причины пропуска)` — 1…6, BIC внутри train."""
    n_obs = len(train)
    n_exog = 0 if exog is None else exog.shape[1]
    best, bics, errors = None, {}, {}
    for lag in range(1, MAX_LAG + 1):
        if not eligible_lag(n_obs, lag, n_exog):
            bics[lag] = None
            errors[lag] = "not identified: n - p <= 4p + 1 + n_exog or n - p < 24"
            continue
        try:
            result = VAR(train[list(ENDOG_COLUMNS)], exog=exog).fit(lag, trend="c")
            value = float(result.bic)
        except Exception as exc:
            bics[lag] = None
            errors[lag] = f"{type(exc).__name__}: {exc}"
            continue
        bics[lag] = value
        errors[lag] = ""
        if best is None or value < best[1] - 1e-12:
            best = (lag, value)
    if best is None:
        raise SystemExit(f"VAR: ни один лаг 1…{MAX_LAG} не идентифицируется (n = {n_obs}, exog = {n_exog})")
    return best[0], bics, errors


def var_forecast(train: pd.DataFrame, lag: int, steps: int, exog) -> np.ndarray:
    """6-шаговый прогноз VAR; `exog` — train-dummies, будущие — нули."""
    model = VAR(train[list(ENDOG_COLUMNS)], exog=exog).fit(lag, trend="c")
    last = train[list(ENDOG_COLUMNS)].to_numpy(dtype=float)[-lag:]
    future = np.zeros((steps, exog.shape[1])) if exog is not None else None
    path = np.asarray(model.forecast(last, steps=steps, exog_future=future), dtype=float)
    if path.shape != (steps, len(ENDOG_COLUMNS)) or not np.isfinite(path).all():
        raise SystemExit("VAR: прогноз не конечен или не той формы")
    return path


def ar1_sigma(values: np.ndarray) -> float:
    """Остаточное СКО AR(1) — масштаб cross-lag prior BVAR."""
    if len(values) < 4:
        return float(np.std(values)) or 1.0
    x, y = values[:-1], values[1:]
    x_mean, y_mean = float(x.mean()), float(y.mean())
    var = float(np.sum((x - x_mean) ** 2))
    beta = 0.0 if var == 0 else float(np.sum((x - x_mean) * (y - y_mean)) / var)
    alpha = y_mean - beta * x_mean
    sigma = float(np.std(y - (alpha + beta * x)))
    return sigma if sigma > 1e-9 else (float(np.std(values)) or 1.0)


def design_matrix(frame: pd.DataFrame, lag: int, exog=None) -> tuple:
    """`X` = `[const, y(t−1)…y(t−p), exog]`, `Y` = `y(t)`."""
    values = frame[list(ENDOG_COLUMNS)].to_numpy(dtype=float)
    exog_values = None
    if exog is not None:
        exog_values = exog.to_numpy(dtype=float) if isinstance(exog, pd.DataFrame) else np.asarray(exog, dtype=float)
    rows = []
    for t in range(lag, len(values)):
        row = [1.0]
        for step in range(1, lag + 1):
            row.extend(values[t - step])
        if exog_values is not None:
            row.extend(exog_values[t])
        rows.append(row)
    return np.asarray(rows, dtype=float), values[lag:]


def minnesota_prior(frame: pd.DataFrame, lag: int, n_exog: int, scale: float = 1.0) -> tuple:
    """`(b0, omega)`: prior mean и diagonal prior variance для каждого уравнения."""
    values = frame[list(ENDOG_COLUMNS)].to_numpy(dtype=float)
    sigmas = [ar1_sigma(values[:, column]) for column in range(len(ENDOG_COLUMNS))]
    size = 1 + len(ENDOG_COLUMNS) * lag + n_exog
    b0 = np.zeros((size, len(ENDOG_COLUMNS)))
    omega = np.zeros((size, len(ENDOG_COLUMNS)))
    omega[0, :] = 1e6
    for equation in range(len(ENDOG_COLUMNS)):
        for step in range(1, lag + 1):
            for column in range(len(ENDOG_COLUMNS)):
                index = 1 + (step - 1) * len(ENDOG_COLUMNS) + column
                if step == 1 and equation == column:
                    b0[index, equation] = 1.0
                if equation == column:
                    omega[index, equation] = (BVAR_LAMBDA / step) ** 2
                else:
                    omega[index, equation] = (BVAR_LAMBDA * sigmas[equation] / (step * sigmas[column])) ** 2
    if n_exog:
        omega[1 + len(ENDOG_COLUMNS) * lag:, :] = 1e6
    return b0, omega * scale


def bvar_coefficients(train: pd.DataFrame, lag: int, exog, prior_scale: float = 1.0) -> np.ndarray:
    """Posterior mean Minnesota BVAR; likelihood масштабирован на σ² остатков.

    `σ_i²` — остаточная дисперсия OLS-уравнения i на том же train и том же
    дизайне. Без деления на неё `X'X` в масштабе лог м/м (~1e-3) проигрывает
    prior precision (~25) и prior подавляет данные. `prior_scale` — только для
    теста диффузного prior: при больших значениях posterior mean = OLS.
    """
    x_matrix, y_matrix = design_matrix(train, lag, exog)
    b0, omega = minnesota_prior(train, lag, 0 if exog is None else exog.shape[1], scale=prior_scale)
    coefficients = np.zeros((x_matrix.shape[1], len(ENDOG_COLUMNS)))
    for equation in range(len(ENDOG_COLUMNS)):
        target = y_matrix[:, equation]
        ols, *_ = np.linalg.lstsq(x_matrix, target, rcond=None)
        residual = target - x_matrix @ ols
        dof = max(1, len(target) - x_matrix.shape[1])
        sigma2 = float(residual @ residual / dof)
        if not math.isfinite(sigma2) or sigma2 <= 0:
            sigma2 = 1.0
        precision = 1.0 / omega[:, equation]
        lhs = x_matrix.T @ x_matrix / sigma2 + np.diag(precision)
        rhs = x_matrix.T @ target / sigma2 + precision * b0[:, equation]
        coefficients[:, equation] = np.linalg.solve(lhs, rhs)
    return coefficients


def bvar_forecast(train: pd.DataFrame, lag: int, steps: int, exog) -> np.ndarray:
    """6-шаговый рекурсивный прогноз posterior mean; будущие dummies — нули."""
    coefficients = bvar_coefficients(train, lag, exog)
    history = train[list(ENDOG_COLUMNS)].to_numpy(dtype=float)[-lag:].copy()
    path = []
    for step in range(steps):
        row = [1.0]
        for back in range(1, lag + 1):
            row.extend(history[-back])
        if exog is not None:
            row.extend(np.zeros(exog.shape[1]))
        next_row = np.asarray(row, dtype=float) @ coefficients
        path.append(next_row)
        history = np.vstack([history[1:], next_row])
    result = np.asarray(path, dtype=float)
    if result.shape != (steps, len(ENDOG_COLUMNS)) or not np.isfinite(result).all():
        raise SystemExit("BVAR: прогноз не конечен или не той формы")
    return result


def cumulative(path: np.ndarray, horizon: int) -> float:
    """Накопленный прирост за первые `horizon` шагов, %."""
    value = (math.exp(sum(float(path[step][CPI_COLUMN]) for step in range(horizon))) - 1) * 100
    if not math.isfinite(value):
        raise SystemExit("VAR/BVAR: накопленный прогноз не конечен")
    return value


def var_forecasts(endo: pd.DataFrame, dummies: pd.DataFrame, test_months: list) -> tuple:
    """`({h: {M: var}}, {h: {M: bvar}}, lag_info)` — фит на каждом origin."""
    test_set = set(test_months)
    var = {horizon: {} for horizon in HORIZONS}
    bvar = {horizon: {} for horizon in HORIZONS}
    lag_info = {}
    origins = sorted({cp.shift_month(month, -(horizon - 1)) for month in test_months for horizon in HORIZONS})
    for origin in origins:
        train_end = cp.shift_month(origin, -1)
        train = endo.loc[:pd.Timestamp(train_end)]
        if len(train) < MIN_LAG_OBS + 1:
            raise SystemExit(f"VAR: слишком короткий train на origin {origin}: {len(train)}")
        exog = training_exog(dummies, train.index)
        lag, bics, lag_errors = select_lag(train, exog)
        path_var = var_forecast(train, lag, max(HORIZONS), exog)
        path_bvar = bvar_forecast(train, lag, max(HORIZONS), exog)
        lag_info[origin] = {"lag": lag, "n_train": len(train), "bics": bics, "lag_errors": lag_errors}
        for horizon in HORIZONS:
            month = cp.shift_month(origin, horizon - 1)
            if month in test_set:
                var[horizon][month] = cumulative(path_var, horizon)
                bvar[horizon][month] = cumulative(path_bvar, horizon)
    return var, bvar, lag_info


# ---------------------------------------------------------------------------
# combo3 (Task 9) и комбинация
# ---------------------------------------------------------------------------


def combo3_forecasts(data: dict, regressors: dict) -> tuple:
    """`(live_cb, {bs: {h: {M: y}}}, combo3)` — тем же кодом, что Task 9."""
    groups = lb.fit_predict(
        data["specs"], data["features"], data["features"], data["targets"],
        data["train_months"], data["test_months"], with_mp=True,
    )
    live_cb = lb.aggregate_predictions(groups, data["weights"], data["test_months"])
    bs = {
        model: {horizon: h1.bs_predict(data["t_live"][horizon], regressors[model], horizon, data["test_months"])
                for horizon in HORIZONS}
        for model in (h1.MODEL_ECON, h1.MODEL_ROZN)
    }
    combo3 = {horizon: h1.combine_mean([live_cb[horizon], bs[h1.MODEL_ECON][horizon], bs[h1.MODEL_ROZN][horizon]])
              for horizon in HORIZONS}
    return live_cb, bs, combo3


def combine_combo3_var16(combo3: dict, var: dict) -> dict:
    """Среднее `combo3` и `var16` с равными весами; общие месяцы."""
    return {horizon: h1.combine_mean([combo3[horizon], var[horizon]]) for horizon in HORIZONS}


# ---------------------------------------------------------------------------
# Оценка
# ---------------------------------------------------------------------------


def compute_metrics(forecasts: dict, data: dict) -> dict:
    import cpi_two_stage_cb_comp as w5b
    import cpi_v4_leaderboard as v4

    normal = data["normal"]
    target_full = data["target_full"]
    truth = {horizon: target_full[horizon].dropna().to_dict() for horizon in HORIZONS}
    all_test = cp.month_range(cp.TEST_START, cp.TEST_END)
    cb_err = {horizon: cp._cb_errors(data["archive_data"]["sheet"], target_full, horizon, normal) for horizon in HORIZONS}
    errors, per_model = {}, {}
    for model in CANDIDATES:
        errors[model], per_model[model] = {}, {}
        for horizon in HORIZONS:
            months, err = lb.errors_on(forecasts[model][horizon], truth[horizon], normal)
            if months != normal:
                raise SystemExit(f"{model} h{horizon}: нет прогноза на части normal-месяцев")
            errors[model][horizon] = err
            _, err_live = lb.errors_on(forecasts[model][horizon], data["t_live"][horizon], normal)
            _, err_all = lb.errors_on(forecasts[model][horizon], truth[horizon], all_test)
            early = [error for month, error in zip(normal, err) if month.year <= 2021]
            late = [error for month, error in zip(normal, err) if month.year >= 2023]
            per_model[model][horizon] = {
                "vs_cb_target_full": w5b.compare_to_cb(err, cb_err[horizon], horizon),
                "rmse_t_live": lb.rmse(err_live),
                "rmse_target_full_all67": lb.rmse(err_all),
                "rmse_2019_2021": lb.rmse(early),
                "rmse_2023_2024": lb.rmse(late),
            }
    pairs_combo3 = {
        model: {horizon: v4.compare_pair(errors[model][horizon], errors[MODEL_COMBO3][horizon], horizon) for horizon in HORIZONS}
        for model in CANDIDATES if model != MODEL_COMBO3
    }
    pairs_live_cb = {
        model: {horizon: v4.compare_pair(errors[model][horizon], errors[MODEL_CB][horizon], horizon) for horizon in HORIZONS}
        for model in CANDIDATES if model != MODEL_CB
    }
    mcs = {}
    for horizon in PRIMARY_HORIZONS:
        losses = {model: [error * error for error in errors[model][horizon]] for model in CANDIDATES}
        losses[CB_COMP] = [error * error for error in cb_err[horizon]]
        included, pvalues = v4.mcs_included(losses)
        mcs[horizon] = {"included": sorted(included), "pvalues": pvalues}
    ratios = {model: {horizon: per_model[model][horizon]["vs_cb_target_full"]["ratio"] for horizon in HORIZONS}
              for model in CANDIDATES}
    error_correlation = {
        horizon: {
            "var16_combo3": float(np.corrcoef(errors[MODEL_VAR][horizon], errors[MODEL_COMBO3][horizon])[0, 1]),
            "var16_live_cb": float(np.corrcoef(errors[MODEL_VAR][horizon], errors[MODEL_CB][horizon])[0, 1]),
            "combo3_live_cb": float(np.corrcoef(errors[MODEL_COMBO3][horizon], errors[MODEL_CB][horizon])[0, 1]),
            "combo4_combo3": float(np.corrcoef(errors[MODEL_COMBO4][horizon], errors[MODEL_COMBO3][horizon])[0, 1]),
        }
        for horizon in PRIMARY_HORIZONS
    }
    return {"per_model": per_model, "pairs_vs_combo3": pairs_combo3, "pairs_vs_live_cb": pairs_live_cb,
            "mcs": mcs, "ratios": ratios, "error_correlation": error_correlation}


def better_than(pair: dict) -> bool:
    """«Лучше `combo3`»: ΔRMSE < 0 и DM-HLN p < 0,10 (порог issue)."""
    return pair["delta"] < 0 and pair["dm_p"] < G12_ALPHA


def g12_check(metrics: dict) -> dict:
    """G12: улучшение на h3/h6, не хуже на h1, combo4 в MCS 90 %."""
    ratios = metrics["ratios"]
    pairs = metrics["pairs_vs_combo3"]
    mcs = metrics["mcs"]
    improvement = {
        horizon: {"pair": pairs[MODEL_COMBO4][horizon], "better": better_than(pairs[MODEL_COMBO4][horizon])}
        for horizon in (3, 6)
    }
    better_h3_or_h6 = any(value["better"] for value in improvement.values())
    h1 = {
        "combo4": ratios[MODEL_COMBO4][1],
        "combo3": ratios[MODEL_COMBO3][1],
        "limit": ratios[MODEL_COMBO3][1] + NOHARM_MARGIN,
        "ok": ratios[MODEL_COMBO4][1] <= ratios[MODEL_COMBO3][1] + NOHARM_MARGIN + 1e-12,
    }
    mcs_in = {horizon: MODEL_COMBO4 in set(mcs[horizon]["included"]) for horizon in PRIMARY_HORIZONS}
    return {"improvement": improvement, "better_h3_or_h6": better_h3_or_h6, "h1": h1,
            "mcs_combo4": mcs_in, "mcs_ok": all(mcs_in.values()),
            "ok": better_h3_or_h6 and h1["ok"] and all(mcs_in.values())}


# ---------------------------------------------------------------------------
# R12
# ---------------------------------------------------------------------------


def r12a_check(ratios: dict) -> dict:
    models = {}
    for model, pinned in R12A_PINNED.items():
        models[model] = {
            horizon: {"ratio": ratios[model][horizon], "pinned": pinned[horizon],
                      "ok": abs(ratios[model][horizon] - pinned[horizon]) <= R12A_TOL}
            for horizon in HORIZONS
        }
    return {"models": models, "ok": all(value["ok"] for model in models.values() for value in model.values())}


def r12b_check(panel: pd.DataFrame, endo: pd.DataFrame) -> dict:
    raw = ("cpi", "rate", "fx", "pe")
    expected = pd.date_range(panel.index[0], panel.index[-1], freq="MS")
    contiguous = len(expected) == len(panel) and bool((panel.index == expected).all())
    raw_na = int(panel[list(raw)].isna().sum().sum())
    endo_na = int(endo.isna().sum().sum())
    return {
        "first": panel.index[0].date().isoformat(), "last": panel.index[-1].date().isoformat(),
        "n_panel": len(panel), "n_endo": len(endo), "contiguous": contiguous,
        "raw_na": raw_na, "endo_na": endo_na,
        "ranges": {column: [float(panel[column].min()), float(panel[column].max())] for column in raw},
        "ok": bool(contiguous and raw_na == 0 and endo_na == 0 and len(endo) == len(panel) - 1),
    }


def r12c_check(forecasts: dict, data: dict, lag_info: dict) -> dict:
    normal = set(data["normal"])
    test = set(data["test_months"])
    models = {}
    for model in CANDIDATES:
        models[model] = {}
        for horizon in HORIZONS:
            months = set(forecasts[model][horizon])
            missing_normal = sorted(normal - months)
            missing_test = sorted(test - months)
            models[model][horizon] = {
                "n": len(months),
                "missing_normal": [month.isoformat() for month in missing_normal],
                "missing_test": [month.isoformat() for month in missing_test],
                "ok": not missing_normal and not missing_test,
            }
    lag_counts = {}
    for info in lag_info.values():
        key = str(info["lag"])
        lag_counts[key] = lag_counts.get(key, 0) + 1
    lengths = [info["n_train"] for info in lag_info.values()]
    return {"models": models, "lag_counts": lag_counts, "n_train_min": min(lengths), "n_train_max": max(lengths),
            "ok": all(value["ok"] for model in models.values() for value in model.values())}


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def config() -> dict:
    return {
        "panel": [PANEL_START.isoformat(), PANEL_END.isoformat()],
        "var_start": VAR_START.isoformat(), "horizons": list(HORIZONS),
        "endogenous": list(ENDOG_COLUMNS), "max_lag": MAX_LAG, "min_lag_obs": MIN_LAG_OBS,
        "bvar_lambda": BVAR_LAMBDA, "bvar_likelihood_scale": "ols_residual_sigma2",
        "models": list(CANDIDATES),
        "g12_alpha": G12_ALPHA, "noharm_margin": NOHARM_MARGIN,
    }


def write_predictions(path: Path, forecasts: dict) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["model", "horizon", "month", "y_pred"])
        for model in CANDIDATES:
            for horizon in HORIZONS:
                for month in sorted(forecasts[model][horizon]):
                    writer.writerow([model, horizon, month.isoformat(), repr(forecasts[model][horizon][month])])


def run(archive: Path, panel_csv: Path, public_dir: Path, out_dir: Path) -> dict:
    data = en.prepare(archive, panel_csv, public_dir)
    panel, endo, dummies, audit = load_var_panel(data)

    live_cb, _bs, combo3 = combo3_forecasts(data, h1.load_regressors(public_dir))
    var, bvar, lag_info = var_forecasts(endo, dummies, data["test_months"])
    forecasts = {
        MODEL_CB: live_cb,
        MODEL_COMBO3: combo3,
        MODEL_VAR: var,
        MODEL_BVAR: bvar,
        MODEL_COMBO4: combine_combo3_var16(combo3, var),
    }

    metrics = compute_metrics(forecasts, data)
    checks = {
        "a": r12a_check(metrics["ratios"]),
        "b": r12b_check(panel, endo),
        "c": r12c_check(forecasts, data, lag_info),
    }
    for name, value in checks.items():
        if not value["ok"]:
            raise SystemExit(f"R12{name} не пройден: {value}")
    gate = g12_check(metrics)

    configuration = config()
    result = {
        "spec": "docs/superpowers/specs/2026-10-31-cb-comp-h3-h6-var-design.md",
        "git_sha": lb.git_sha(),
        "config": configuration,
        "config_hash": hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest(),
        "inputs": {
            "panel_csv": lb.sha256(panel_csv),
            "public_panel_csv": lb.sha256(public_dir / "public_panel.csv"),
            "sheet_map_csv": lb.sha256(public_dir / "sheet_map.csv"),
            "target_xlsx": lb.sha256(archive / cp.TARGET_XLSX),
        },
        "panel": audit,
        "r12": checks,
        "lag_selection": {origin.isoformat(): info for origin, info in lag_info.items()},
        "per_model": metrics["per_model"],
        "pairs_vs_combo3": metrics["pairs_vs_combo3"],
        "pairs_vs_live_cb": metrics["pairs_vs_live_cb"],
        "mcs": metrics["mcs"],
        "ratios": metrics["ratios"],
        "error_correlation": metrics["error_correlation"],
        "g12": gate,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = out_dir / "predictions.csv"
    write_predictions(predictions_path, forecasts)
    result["predictions_sha256"] = lb.sha256(predictions_path)
    (out_dir / "result.json").write_text(
        json.dumps(lb._jsonable(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--archive", default=None)
    parser.add_argument("--panel", default=str(lb.PANEL_CSV))
    parser.add_argument("--public-dir", default=str(lb.PUBLIC_DIR))
    parser.add_argument("--out", default=str(OUT_DIR))
    args = parser.parse_args(argv)
    archive = cp.resolve_archive(args.archive)
    result = run(archive, Path(args.panel), Path(args.public_dir), Path(args.out))
    summary = {
        "r12": {name: value["ok"] for name, value in result["r12"].items()},
        "g12": result["g12"],
        "ratios": {model: {horizon: round(ratio, 3) for horizon, ratio in per.items()}
                   for model, per in result["ratios"].items()},
        "classes": {model: {horizon: value["vs_cb_target_full"]["class"] for horizon, value in per.items()}
                    for model, per in result["per_model"].items()},
        "mcs": result["mcs"],
        "lag_counts": result["r12"]["c"]["lag_counts"],
    }
    print(json.dumps(lb._jsonable(summary), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
