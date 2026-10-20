#!/usr/bin/env python3
"""Task 3: ядро ordered logit/ML + модель дисперсии для веера ставки.

Спека: `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v2-design.md`
План: `docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v2.md`
Issue: #286 (`key-rate/phase-4-cbr-scenario-fan`, Task 3).

Отличие от Task 1: центр веера — не `cbr_rule`, а `ordered_logit` (primary)
или `ElasticNetCV` (challenger). Сценарии КПМ дают центрированные дельты
`δ_s` (`Σδ_s = 0`), остатки ядра `e = факт − core` дают квантили `q_α(t)`,
а модель дисперсии задаёт ширину:

    point = core_step + δ_s + q_α(t)

Три модели дисперсии: `disp_global` (ablation), `disp_scale` (primary:
`log|e| ~ 1−w + p`), `disp_conformal` (robustness).

**В `forecast_log` не пишет, в ClickHouse не пишет.** Артефакты локальны;
`ML_LOG=1` — паспорт run через `ml_experiment`.

Запуск — только через `make`:

    make key-rate-scenario-fan-v2
    ML_LOG=1 make key-rate-scenario-fan-v2
    make key-rate-scenario-fan-v2-test
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.linear_model import ElasticNetCV

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import key_rate_cbr_rule as cr  # noqa: E402
import key_rate_model as krm  # noqa: E402
import key_rate_scenario_fan as fan1  # noqa: E402
import ml_experiment as mlx  # noqa: E402
import shock_classifier as sc  # noqa: E402

log = logging.getLogger("key_rate_scenario_fan_v2")

SPEC_PATH = "docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v2-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v2.md"
REPORT_NAME = "key_rate_scenario_fan_v2_report.md"
PREDICTIONS_NAME = "key_rate_scenario_fan_v2_predictions.csv"
DIAGNOSTICS_NAME = "key_rate_scenario_fan_v2_diagnostics.csv"
FORECAST_NAME = "key_rate_scenario_fan_v2_forecast.json"

WINDOW_EXPANDING = krm.WINDOW_EXPANDING
ROLLING_WINDOW = krm.ROLLING_WINDOW
WINDOWS = (WINDOW_EXPANDING, krm.window_label(ROLLING_WINDOW))
WINDOW_PAIRS = ((WINDOW_EXPANDING, None), (krm.window_label(ROLLING_WINDOW), ROLLING_WINDOW))
MIN_TRAIN = krm.MIN_TRAIN
MIN_RESIDUALS = 12
ALPHAS = (0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95)
FAN_ORDER = (
    "core_fan_global",
    "core_fan_scale",
    "core_fan_conformal",
    "core_fan_ml",
)
PRIMARY_FAN = "core_fan_scale"
PRIMARY_WINDOW = WINDOW_EXPANDING
PRIMARY_LEVEL = fan1.PRIMARY_LEVEL
BENCHMARK_ORDER = ("hold", "repeat", "ar1", "ordered_logit")
TASK1_FAN = "fan_shock_weighted"
SEED = 20261020
EPS = 1e-9


# --- Ядро ----------------------------------------------------------------------


def _num(value) -> float:
    if value is None:
        return float("nan")
    return float(value)


def core_matrix(rows: list) -> np.ndarray:
    """Матрица признаков ядра; отсутствующий признак — NaN (не ноль)."""
    data = [
        [
            _num(row.decision.rate_before),
            _num(row.pi_saar),
            _num(row.gap),
            _num(row.e3_core),
            _num(row.e3_headline_adaptive),
            _num(row.gdp_gap),
            _num(row.ibk_gap),
        ]
        for row in rows
    ]
    return np.asarray(data, dtype=float)


def _impute(matrix: np.ndarray, medians: np.ndarray) -> np.ndarray:
    return np.where(np.isnan(matrix), medians, matrix)


def fit_enet_core(train: list, row) -> krm.Forecast:
    """ML-челленджер: ElasticNetCV на признаках `CbrRow`, CV только внутри train.

    CatBoost на 108 решениях не используется: правило KEY_RATE_MODEL.md §4 —
    CatBoost на компонентах, не на решении. Модель детерминирована сидом.
    """
    if len(train) < 8:
        raise krm.DegenerateWindowError(
            f"ML-ядро: обучающее окно {len(train)} < 8"
        )
    y = np.asarray([row_.decision.step_bp for row_ in train], dtype=float)
    if len(set(y.tolist())) < 2:
        raise krm.DegenerateWindowError("ML-ядро: шаг в окне не меняется")
    matrix = core_matrix(train)
    medians = np.nanmedian(matrix, axis=0)
    medians = np.where(np.isfinite(medians), medians, 0.0)
    matrix = _impute(matrix, medians)
    row_vector = np.asarray(
        [
            _num(row.decision.rate_before),
            _num(row.pi_saar),
            _num(row.gap),
            _num(row.e3_core),
            _num(row.e3_headline_adaptive),
            _num(row.gdp_gap),
            _num(row.ibk_gap),
        ],
        dtype=float,
    )
    row_vector = _impute(row_vector, medians)
    cv = min(5, len(train))
    if cv < 2:
        raise krm.DegenerateWindowError("ML-ядро: CV < 2")
    model = ElasticNetCV(
        l1_ratio=[0.1, 0.5, 0.9],
        cv=cv,
        random_state=SEED,
        max_iter=10000,
    )
    try:
        model.fit(matrix, y)
        step = float(model.predict(row_vector.reshape(1, -1))[0])
    except Exception as error:  # noqa: BLE001 — вырожденное окно, не порча данных
        raise krm.DegenerateWindowError(f"ML-ядро: {error}") from error
    if not math.isfinite(step):
        raise krm.DegenerateWindowError("ML-ядро: неконечный прогноз")
    return krm.Forecast(step, None)


def core_predictions(rows: list, method: str, fit_predict, rolling: Optional[int]) -> tuple:
    """Walk-forward прогнозы ядра: dict по дате + число пропущенных фолдов."""
    predictions, skipped = krm.walk_forward(
        rows,
        method,
        fit_predict,
        min_train=MIN_TRAIN,
        rolling=rolling,
    )
    return {item.meeting_date: item for item in predictions}, skipped


def core_history(core_by_date: dict, day: date) -> tuple:
    """Прошлые прогнозы ядра и остатки `факт − прогноз`; строго `date < day`."""
    past = sorted(
        (item for item in core_by_date.values() if item.meeting_date < day),
        key=lambda item: item.meeting_date,
    )
    return past, [item.actual_step_bp - item.step_bp for item in past]


# --- Сценарии и дисперсия ------------------------------------------------------


def centered_deltas(row, impulse: Optional[float], budget_z: float) -> tuple:
    """5 шагов Task 1 и центрированные дельты `δ_s = step_s − mean(step_s)`."""
    steps = fan1.scenario_steps(row, impulse, budget_z)
    base = steps[0].components.step_bp
    raw = [item.components.step_bp - base for item in steps]
    mean = sum(raw) / len(raw)
    return steps, tuple(value - mean for value in raw)


@dataclass(frozen=True)
class Dispersion:
    """Квантили дисперсии и метаданные модели."""

    name: str
    quantiles: tuple
    scale: float
    coefficients: tuple = ()
    fallback: str = ""


def disp_global(residuals: list) -> Dispersion:
    """Эмпирические квантили остатков обучающего окна (ablation)."""
    if len(residuals) < MIN_RESIDUALS:
        raise krm.DegenerateWindowError(
            f"disp_global: остатков {len(residuals)} < {MIN_RESIDUALS}"
        )
    values = np.asarray(residuals, dtype=float)
    return Dispersion(
        "disp_global",
        tuple(float(np.quantile(values, alpha)) for alpha in ALPHAS),
        1.0,
    )


def disp_scale(
    residuals: list,
    past_weights: list,
    past_probabilities: list,
    current_weight: float,
    current_probability: float,
) -> Dispersion:
    """Лог-линейная модель масштаба: `log(|e|+1) = γ0+γ1(1−w)+γ2p`.

    Коэффициенты — только по прошлым остаткам; `scale_t` — по `w_t`/`p_t`
    текущего заседания. `scale_t ≤ 0` или неконечные коэффициенты — fallback
    на `disp_global` (вызывающий считает его отдельно).
    """
    if len(residuals) < MIN_RESIDUALS:
        raise krm.DegenerateWindowError(
            f"disp_scale: остатков {len(residuals)} < {MIN_RESIDUALS}"
        )
    if not (len(residuals) == len(past_weights) == len(past_probabilities)):
        raise ValueError("disp_scale: длины остатков/весов/вероятностей не совпадают")
    values = np.asarray(residuals, dtype=float)
    design = np.column_stack(
        [
            np.ones(len(values)),
            1.0 - np.asarray(past_weights, dtype=float),
            np.asarray(past_probabilities, dtype=float),
        ]
    )
    target = np.log(np.abs(values) + 1.0)
    coefficients, *_ = np.linalg.lstsq(design, target, rcond=None)
    if not np.all(np.isfinite(coefficients)):
        raise krm.DegenerateWindowError("disp_scale: неконечные коэффициенты")
    scales = np.exp(design @ coefficients) - 1.0
    scales = np.maximum(scales, EPS)
    standardized = values / scales
    quantiles = np.quantile(standardized, ALPHAS)
    current = float(
        np.dot(
            np.asarray([1.0, 1.0 - current_weight, current_probability]),
            coefficients,
        )
    )
    scale = math.exp(current) - 1.0
    if not math.isfinite(scale) or scale <= 0.0:
        raise krm.DegenerateWindowError("disp_scale: scale_t <= 0")
    return Dispersion(
        "disp_scale",
        tuple(scale * float(value) for value in quantiles),
        scale,
        tuple(float(value) for value in coefficients),
    )


def disp_conformal(residuals: list) -> Dispersion:
    """Конечновыборочный conformal-индекс: `⌈(n+1)α⌉`-я порядковая статистика."""
    if len(residuals) < MIN_RESIDUALS:
        raise krm.DegenerateWindowError(
            f"disp_conformal: остатков {len(residuals)} < {MIN_RESIDUALS}"
        )
    ordered = sorted(float(value) for value in residuals)
    size = len(ordered)
    quantiles = []
    for alpha in ALPHAS:
        index = min(size, max(1, math.ceil((size + 1) * alpha)))
        quantiles.append(ordered[index - 1])
    return Dispersion("disp_conformal", tuple(quantiles), 1.0)


# --- Веера ---------------------------------------------------------------------


def make_fan(
    name: str,
    day: date,
    window: str,
    core_step: float,
    steps: list,
    deltas: tuple,
    dispersion: Dispersion,
    actual_step: float,
    emergency: bool,
    p_shock: float,
    weight: float,
) -> fan1.FanPrediction:
    """35 точек: 5 сценариев × 7 квантилей; веса 1/35."""
    points = []
    labels = []
    fallbacks = []
    point_weight = 1.0 / (len(steps) * len(ALPHAS))
    for scenario, delta in zip(steps, deltas):
        for alpha, quantile in zip(ALPHAS, dispersion.quantiles):
            points.append((float(core_step + delta + quantile), point_weight))
            labels.append(f"{scenario.name}|q{alpha}")
            fallbacks.append(scenario.fallback or "")
    return fan1.FanPrediction(
        meeting_date=day,
        window=window,
        fan=name,
        steps=tuple(points),
        labels=tuple(labels),
        actual_step_bp=float(actual_step),
        emergency=bool(emergency),
        p_shock=float(p_shock),
        weight=float(weight),
        fallbacks=tuple(fallbacks),
    )


# --- Диагностика ---------------------------------------------------------------


@dataclass(frozen=True)
class DiagnosticRow:
    meeting_date: date
    window: str
    core: str
    core_step: float
    probs: Optional[tuple]
    scale: float
    quantiles: tuple
    deltas: tuple


# --- Walk-forward --------------------------------------------------------------


BENCHMARKS = {
    "hold": krm.predict_hold,
    "repeat": krm.predict_repeat,
    "ar1": krm.fit_ar1,
}


def run_v2_walk_forward(
    rows: list,
    panel_by_date: dict,
    weight_by_date: dict,
    probability_by_date: dict,
    impulse_by_date: dict,
) -> tuple:
    """Все v2-веера, бенчмарки, Task 1 reference и диагностика."""
    task1_fans, _, _ = fan1.run_walk_forward(
        rows,
        panel_by_date,
        weight_by_date,
        probability_by_date,
        impulse_by_date,
    )
    cores = {}
    skips = Counter()
    for window, rolling in WINDOW_PAIRS:
        ordered, ordered_skip = core_predictions(
            rows, "ordered_logit", krm.fit_ordered_logit, rolling
        )
        ml, ml_skip = core_predictions(rows, "ml_core", fit_enet_core, rolling)
        cores[window] = {
            "ordered": ordered,
            "ml": ml,
            "ordered_skip": ordered_skip,
            "ml_skip": ml_skip,
        }
        skips[f"core_ordered__{window}"] = ordered_skip
        skips[f"core_ml__{window}"] = ml_skip

    fans = []
    benchmarks = []
    diagnostics = []
    scale_coefficients = []
    for window, rolling in WINDOW_PAIRS:
        start = MIN_TRAIN if rolling is None else max(MIN_TRAIN, rolling)
        for index in range(start, len(rows)):
            train = rows[:index] if rolling is None else rows[index - rolling:index]
            row = rows[index]
            day = row.decision.meeting_date
            impulse = impulse_by_date.get(day)
            median, scale = fan1.train_budget_stats(train, impulse_by_date)
            budget_z = fan1.budget_z_score(impulse, median, scale)
            try:
                steps, deltas = centered_deltas(row, impulse, budget_z)
            except krm.DegenerateWindowError:
                skips["fan_fold"] += 1
                continue
            ordered = cores[window]["ordered"].get(day)
            if ordered is None:
                skips["core_ordered_missing"] += 1
                continue
            past, residuals = core_history(cores[window]["ordered"], day)
            if len(residuals) < MIN_RESIDUALS:
                skips["residuals"] += 1
                continue
            past_weights = [weight_by_date.get(item.meeting_date, 1.0) for item in past]
            past_probabilities = [
                probability_by_date.get(item.meeting_date, 0.0) for item in past
            ]
            current_weight = weight_by_date.get(day, 1.0)
            current_probability = probability_by_date.get(day, 0.0)
            global_disp = disp_global(residuals)
            try:
                scale_disp = disp_scale(
                    residuals,
                    past_weights,
                    past_probabilities,
                    current_weight,
                    current_probability,
                )
            except krm.DegenerateWindowError:
                scale_disp = global_disp
                skips["scale_fallback"] += 1
            conformal_disp = disp_conformal(residuals)
            specs = [
                ("core_fan_global", ordered, global_disp),
                ("core_fan_scale", ordered, scale_disp),
                ("core_fan_conformal", ordered, conformal_disp),
            ]
            ml = cores[window]["ml"].get(day)
            if ml is not None:
                ml_past, ml_residuals = core_history(cores[window]["ml"], day)
                if len(ml_residuals) >= MIN_RESIDUALS:
                    ml_weights = [
                        weight_by_date.get(item.meeting_date, 1.0) for item in ml_past
                    ]
                    ml_probabilities = [
                        probability_by_date.get(item.meeting_date, 0.0)
                        for item in ml_past
                    ]
                    try:
                        ml_disp = disp_scale(
                            ml_residuals,
                            ml_weights,
                            ml_probabilities,
                            current_weight,
                            current_probability,
                        )
                    except krm.DegenerateWindowError:
                        ml_disp = disp_global(ml_residuals)
                        skips["ml_scale_fallback"] += 1
                    specs.append(("core_fan_ml", ml, ml_disp))
                else:
                    skips["ml_residuals"] += 1
            else:
                skips["ml_core_missing"] += 1
            emergency = bool(panel_by_date[day].emergency)
            for name, core, dispersion in specs:
                fans.append(
                    make_fan(
                        name,
                        day,
                        window,
                        core.step_bp,
                        steps,
                        deltas,
                        dispersion,
                        row.decision.step_bp,
                        emergency,
                        current_probability,
                        current_weight,
                    )
                )
                if name == "core_fan_scale":
                    scale_coefficients.append(dispersion.coefficients)
            diagnostics.append(
                DiagnosticRow(
                    meeting_date=day,
                    window=window,
                    core="ordered_logit",
                    core_step=ordered.step_bp,
                    probs=ordered.probs,
                    scale=scale_disp.scale,
                    quantiles=scale_disp.quantiles,
                    deltas=deltas,
                )
            )
            for method in BENCHMARK_ORDER[:-1]:
                try:
                    forecast = BENCHMARKS[method](train, row)
                except krm.DegenerateWindowError:
                    skips[f"bench_{method}"] += 1
                    continue
                benchmarks.append(
                    fan1.BenchmarkPrediction(
                        meeting_date=day,
                        window=window,
                        method=method,
                        step_bp=float(forecast.step_bp),
                        actual_step_bp=float(row.decision.step_bp),
                        probs=forecast.probs,
                    )
                )
            benchmarks.append(
                fan1.BenchmarkPrediction(
                    meeting_date=day,
                    window=window,
                    method="ordered_logit",
                    step_bp=float(ordered.step_bp),
                    actual_step_bp=float(row.decision.step_bp),
                    probs=ordered.probs,
                )
            )
    return fans, benchmarks, task1_fans, diagnostics, scale_coefficients, skips


# --- Сравнения и решение -------------------------------------------------------


def make_comparisons(fans: list, benchmarks: list, task1_fans: list) -> dict:
    comparisons = {}
    for window in WINDOWS:
        task1_group = [
            item
            for item in task1_fans
            if item.fan == TASK1_FAN and item.window == window
        ]
        for fan_name in FAN_ORDER:
            fan_group = [
                item for item in fans if item.fan == fan_name and item.window == window
            ]
            comparisons[f"{fan_name}_vs_task1__{window}"] = fan1.compare_fans(
                fan_group, task1_group
            )
            for method in BENCHMARK_ORDER:
                bench_group = [
                    item
                    for item in benchmarks
                    if item.method == method and item.window == window
                ]
                comparisons[f"{fan_name}_vs_{method}__{window}"] = (
                    fan1.compare_fan_to_benchmark(fan_group, bench_group, method)
                )
    return comparisons


def decide(summary: dict, comparisons: dict) -> tuple:
    primary = summary.get((PRIMARY_FAN, PRIMARY_WINDOW))
    if primary is None:
        return "reject", "нет фолдов primary"
    coverage = primary["coverage"][PRIMARY_LEVEL]
    coverage_ok = (
        0.70 <= coverage["value"] <= 0.90
        and coverage["interval"][0] <= PRIMARY_LEVEL <= coverage["interval"][1]
    )
    task1 = comparisons.get(f"{PRIMARY_FAN}_vs_task1__{PRIMARY_WINDOW}")
    task1_ok = bool(
        task1
        and task1["crps_diff"] < 0.0
        and task1["crps_high"] < 0.0
    )
    ordered = comparisons.get(f"{PRIMARY_FAN}_vs_ordered_logit__{PRIMARY_WINDOW}")
    crps_ok = (
        ordered is None
        or primary["crps"] <= ordered["bench_crps"] * 1.10
    )
    rmse_ok = (
        ordered is None
        or primary["rmse"] <= ordered["bench_rmse"] * 1.10
    )
    reason = (
        f"CRPS {primary['crps']:.2f}; coverage80 {coverage['value']:.3f} "
        f"[{coverage['interval'][0]:.3f}; {coverage['interval'][1]:.3f}]; "
        f"Task 1 ΔCRPS {task1['crps_diff']:.2f} "
        f"[{task1['crps_low']:.2f}; {task1['crps_high']:.2f}]; "
        f"ordered CRPS {ordered['bench_crps']:.2f}, RMSE {ordered['bench_rmse']:.2f}"
        if ordered
        else f"CRPS {primary['crps']:.2f}; coverage80 {coverage['value']:.3f}"
    )
    if coverage_ok and task1_ok and crps_ok and rmse_ok:
        return "promote", reason
    if coverage_ok or task1_ok or crps_ok:
        return "iterate", reason
    return "reject", reason


# --- Артефакты -----------------------------------------------------------------


def write_diagnostics(path: Path, diagnostics: list) -> None:
    columns = (
        "meeting_date",
        "window",
        "core",
        "core_step",
        "prob_cut",
        "prob_hold",
        "prob_hike",
        "scale",
        "q05",
        "q10",
        "q25",
        "q50",
        "q75",
        "q90",
        "q95",
        "delta_inertial",
        "delta_rational",
        "delta_hard",
        "delta_budget",
        "delta_no_market_loans",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for item in diagnostics:
            probs = item.probs or (None, None, None)
            writer.writerow(
                [
                    item.meeting_date.isoformat(),
                    item.window,
                    item.core,
                    f"{item.core_step:.6f}",
                    "" if probs[0] is None else f"{probs[0]:.6f}",
                    "" if probs[1] is None else f"{probs[1]:.6f}",
                    "" if probs[2] is None else f"{probs[2]:.6f}",
                    f"{item.scale:.6f}",
                    *[f"{value:.6f}" for value in item.quantiles],
                    *[f"{value:.6f}" for value in item.deltas],
                ]
            )


# --- Текущий прогноз -----------------------------------------------------------


def current_forecast(
    rows: list,
    key_rate: list,
    cpd_core: list,
    cpd_all: list,
    ibk: list,
    gdp: list,
    consensus: list,
    budget: fan1.BudgetPoint,
    panel: list,
    channels: dict,
    shock_meetings: list,
    impulse_by_date: dict,
    ordered_by_date: dict,
    weight_by_date: dict,
    probability_by_date: dict,
    as_of: date,
) -> Optional[dict]:
    future = sorted({day for day, _ in shock_meetings if day > as_of})
    if not future:
        return None
    meeting = future[0]
    inputs = krm.forecast_inputs(as_of, key_rate, cpd_core, ibk)
    base = krm.forecast_row(meeting, inputs)
    if base is None:
        return None
    last_trend = next(
        (row.r_star_trend for row in reversed(rows) if row.r_star_trend is not None),
        None,
    )
    current_row = cr.CbrRow(
        decision=base.decision,
        pi_saar=base.pi_saar,
        gap=base.gap,
        e3_core=cr.e3_adaptive_yoy(cpd_core, as_of),
        e3_headline_adaptive=cr.e3_adaptive_yoy(cpd_all, as_of),
        e3_headline_consensus=cr.e3_consensus(consensus, as_of),
        gdp_gap=cr.hp_gap(
            gdp,
            cr.available_from_gdp,
            as_of,
            lamb=1600.0,
            min_points=cr.MIN_GDP_QUARTERS,
            percent=True,
        ),
        ibk_gap=cr.hp_gap(
            ibk,
            krm.available_from_survey,
            as_of,
            lamb=14400.0,
            min_points=cr.MIN_IBK_MONTHS,
            percent=False,
        ),
        r_star_trend=last_trend,
        e3_core_m3=cr.e3_adaptive(cpd_core, as_of),
        e3_headline_m3=cr.e3_adaptive(cpd_all, as_of),
    )
    try:
        core = krm.fit_ordered_logit(rows, current_row)
        impulse = fan1.budget_impulse(budget, as_of)
        median, scale = fan1.train_budget_stats(rows, impulse_by_date)
        budget_z = fan1.budget_z_score(impulse, median, scale)
        steps, deltas = centered_deltas(current_row, impulse, budget_z)
        past, residuals = core_history(ordered_by_date, as_of)
        past_weights = [weight_by_date.get(item.meeting_date, 1.0) for item in past]
        past_probabilities = [
            probability_by_date.get(item.meeting_date, 0.0) for item in past
        ]
        next_forecast = sc.next_meeting_forecast(
            panel, shock_meetings, key_rate, channels, as_of
        )
        current_weight = next_forecast.weight if next_forecast is not None else 1.0
        current_probability = (
            min(fan1.P_SHOCK_MAX, max(0.0, next_forecast.shock_prob))
            if next_forecast is not None and next_forecast.shock_prob is not None
            else 0.0
        )
        try:
            dispersion = disp_scale(
                residuals,
                past_weights,
                past_probabilities,
                current_weight,
                current_probability,
            )
        except krm.DegenerateWindowError:
            dispersion = disp_global(residuals)
        points = [
            core.step_bp + delta + quantile
            for delta in deltas
            for quantile in dispersion.quantiles
        ]
        weights = [1.0 / len(points)] * len(points)
        fan_quantiles = {
            str(level): fan1.weighted_quantile(points, weights, level)
            for level in (0.10, 0.50, 0.90)
        }
    except (krm.DegenerateWindowError, ValueError) as error:
        return {"skipped": str(error)}
    return {
        "as_of": as_of.isoformat(),
        "meeting_date": meeting.isoformat(),
        "core": "ordered_logit",
        "core_step": core.step_bp,
        "probs": list(core.probs) if core.probs else None,
        "budget_impulse": impulse,
        "scenarios": [
            {
                "name": item.name,
                "delta": delta,
                "fallback": item.fallback,
            }
            for item, delta in zip(steps, deltas)
        ],
        "dispersion": dispersion.name,
        "scale": dispersion.scale,
        "quantiles": list(dispersion.quantiles),
        "fan_quantiles": fan_quantiles,
        "p_shock": current_probability,
        "weight": current_weight,
        "note": "информационно; в forecast_log не записывается до подтверждения",
    }


# --- Отчёт ---------------------------------------------------------------------


def _fmt(value: Optional[float], places: int = 2) -> str:
    if value is None:
        return "—"
    return f"{value:.{places}f}"


def _fmt_p(value: Optional[float]) -> str:
    if value is None:
        return "—"
    if value < 0.0001:
        return "<0.0001"
    return f"{value:.4f}"


def render_report(
    as_of: date,
    rows: list,
    summary: dict,
    benchmarks: dict,
    comparisons: dict,
    episodes: dict,
    skips: Counter,
    scale_coefficients: list,
    decision: str,
    reason: str,
    current: Optional[dict],
) -> str:
    lines = [
        "# Веер v2: ядро ordered logit/ML + модель дисперсии",
        "",
        f"**Дата:** {as_of.isoformat()}",
        "**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 3 (issue #286)",
        f"**Спека:** [{SPEC_PATH}](../superpowers/specs/2026-10-20-cbr-scenario-fan-v2-design.md)",
        f"**План:** [{PLAN_PATH}](../superpowers/plans/2026-10-20-cbr-scenario-fan-v2.md)",
        "",
        "## 1. Выборка",
        "",
        "| Показатель | Значение |",
        "|---|---:|",
        f"| заседаний | {len(rows)} |",
        f"| первое / последнее | {rows[0].decision.meeting_date.isoformat()} / {rows[-1].decision.meeting_date.isoformat()} |",
        f"| фан-фолды primary | {summary.get((PRIMARY_FAN, WINDOW_EXPANDING), {}).get('n', 0)} expanding / {summary.get((PRIMARY_FAN, krm.window_label(ROLLING_WINDOW)), {}).get('n', 0)} rolling |",
        f"| residual warm-up (skip) | {skips.get('residuals', 0)} (фолды до {MIN_RESIDUALS} остатков) |",
        f"| scale fallback | {skips.get('scale_fallback', 0)} |",
        f"| fan_fold skip | {skips.get('fan_fold', 0)} |",
        "",
        "## 2. Ядро и дисперсия",
        "",
        "Ядро — `ordered_logit` (primary), ML-челленджер — `ElasticNetCV`; сценарии КПМ — центрированные дельты `Σδ=0`.",
        "",
    ]
    if scale_coefficients:
        matrix = np.asarray(scale_coefficients, dtype=float)
        mean = matrix.mean(axis=0)
        lines.append(
            f"`disp_scale` коэффициенты (среднее по фолдам): "
            f"γ0={mean[0]:.4f}, γ1={mean[1]:.4f}, γ2={mean[2]:.4f}; "
            f"фолдов {len(scale_coefficients)}."
        )
    else:
        lines.append("`disp_scale` коэффициенты: — (все фолды — fallback).")
    lines.extend(
        [
            "",
            "## 3. Веер × окно",
            "",
            "| веер | окно | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | Brier | log loss | RMSE | MAE |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for fan_name in FAN_ORDER:
        for window in WINDOWS:
            item = summary.get((fan_name, window))
            if item is None:
                continue
            cov = item["coverage"]
            lines.append(
                f"| {fan_name} | {window} | {item['n']} | {_fmt(item['crps'], 3)} "
                f"| {_fmt(cov[0.50]['value'], 3)} | {_fmt(cov[0.80]['value'], 3)} "
                f"| {_fmt(cov[0.90]['value'], 3)} | {_fmt(item['width'][0.80], 2)} "
                f"| {_fmt(item['brier'], 4)} | {_fmt(item['log_loss'], 4)} "
                f"| {_fmt(item['rmse'])} | {_fmt(item['mae'])} |"
            )
    lines.extend(
        [
            "",
            "## 4. Сравнение с Task 1 и бенчмарками",
            "",
            "| пара | n | ΔCRPS | 95 % CI | p (бутстр.) | DM p | CRPS бенч. | RMSE веера | RMSE бенч. |",
            "|---|---:|---:|---|---:|---:|---:|---:|---:|",
        ]
    )
    for key, item in comparisons.items():
        if item is None:
            lines.append(f"| {key} | — | — | — | — | — | — | — | — |")
            continue
        lines.append(
            f"| {key} | {item.get('n', '—')} | {_fmt(item.get('crps_diff'), 3)} "
            f"| [{_fmt(item.get('crps_low'), 3)}; {_fmt(item.get('crps_high'), 3)}] "
            f"| {_fmt_p(item.get('crps_p'))} | {_fmt_p(item.get('dm_p'))} "
            f"| {_fmt(item.get('bench_crps'), 3)} | {_fmt(item.get('fan_rmse'))} "
            f"| {_fmt(item.get('bench_rmse'))} |"
        )
    lines.extend(
        [
            "",
            "## 5. Эпизоды",
            "",
            "| веер | окно | эпизод | n | CRPS | coverage 80 | ширина 80 |",
            "|---|---|---|---:|---:|---:|---:|",
        ]
    )
    for (fan_name, window, episode), item in sorted(
        episodes.items(), key=lambda pair: (pair[0][0], pair[0][1], pair[0][2])
    ):
        lines.append(
            f"| {fan_name} | {window} | {episode} | {item['n']} | {_fmt(item['crps'], 3)} "
            f"| {_fmt(item['coverage80'], 3)} | {_fmt(item['width80'], 2)} |"
        )
    lines.extend(
        [
            "",
            "Эпизод 2014-12…2015 в тестовые фолды не попадает: он в обучающем окне; "
            "тестируются 2022–2023 и 2024–2026.",
            "",
            "## 6. Текущий прогноз",
            "",
        ]
    )
    if current is None:
        lines.append("Будущих заседаний нет или входы недоступны.")
    elif "skipped" in current:
        lines.append(f"Пропуск: {current['skipped']}")
    else:
        lines.append(
            f"Заседание **{current['meeting_date']}**, core `{current['core']}` "
            f"{_fmt(current['core_step'])} б.п., дисперсия `{current['dispersion']}` "
            f"(scale {_fmt(current['scale'], 3)}), `p_t` {_fmt(current['p_shock'], 3)}, "
            f"`w_t` {_fmt(current['weight'], 3)}."
        )
        lines.extend(
            [
                "",
                "| сценарий | δ, б.п. | fallback |",
                "|---|---:|---|",
            ]
        )
        for item in current["scenarios"]:
            lines.append(
                f"| {item['name']} | {_fmt(item['delta'])} | {item['fallback'] or '—'} |"
            )
        lines.append(
            f"\nКвантили веера 10/50/90: "
            f"{_fmt(current['fan_quantiles']['0.1'])} / "
            f"{_fmt(current['fan_quantiles']['0.5'])} / "
            f"{_fmt(current['fan_quantiles']['0.9'])} б.п."
        )
    lines.extend(
        [
            "",
            "## 7. Вердикт",
            "",
            f"**`{decision}`** — {reason}.",
            "",
            "Правило — спека §8.3. Primary: `core_fan_scale`, expanding, coverage 80 % + CRPS; "
            "Task 1 `fan_shock_weighted` — reference; `core_fan_ml` — challenger.",
            "",
            "## 8. Ограничения",
            "",
            "- Ordered logit слаб в 2022; дисперсия может накрыть факт, но медиана смещена.",
            "- Сценарные дельты — reduced-form прокси КПМ; центрирование убирает уровень, не шум.",
            "- `disp_scale` не улучшил `disp_global`: CRPS 60.76 vs 60.36, coverage 0.736 vs 0.764; γ2<0 — модель масштаба не подтверждена. `core_fan_conformal` — лучший coverage 0.806, следующий кандидат.",
            "- `disp_scale` — 2 признака; conformal — robustness, не гарантия.",
            "- As-of без PIT; CPD/ВВП/Минфин без винтажей.",
            "- 2014/2015 — в train, не в тесте.",
            "- В `forecast_log` не пишется; Go/DDL/витрины/гранты не менялись.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


# --- Журнал эксперимента -------------------------------------------------------


def _log_run(
    rows: list,
    fans: list,
    summary: dict,
    decision: str,
    as_of: date,
    artifacts: list,
    enabled: Optional[bool] = None,
) -> str:
    config = {
        "core": "ordered_logit",
        "ml_challenger": "ElasticNetCV",
        "dispersion": ["disp_global", "disp_scale", "disp_conformal"],
        "alphas": list(ALPHAS),
        "min_train": MIN_TRAIN,
        "min_residuals": MIN_RESIDUALS,
        "primary_fan": PRIMARY_FAN,
        "primary_window": PRIMARY_WINDOW,
        "as_of": as_of.isoformat(),
    }
    sample_meta = {
        "rows": len(rows),
        "first": rows[0].decision.meeting_date.isoformat() if rows else None,
        "last": rows[-1].decision.meeting_date.isoformat() if rows else None,
        "fans": len(fans),
    }
    sample = [
        {
            "meeting_date": row.decision.meeting_date.isoformat(),
            "rate_before": row.decision.rate_before,
            "step_bp": row.decision.step_bp,
        }
        for row in rows
    ]
    logger = mlx.start_run(
        experiment="key_rate_scenario_fan_v2",
        run_name="scenario_fan_v2__rate__start24",
        config=config,
        data_asof=as_of,
        sample_meta=sample_meta,
        sample=sample,
        spec_path=SPEC_PATH,
        enabled=enabled,
    )
    logger.log_metrics(
        {
            "primary_metric": "crps",
            "decision": decision,
            "fans": {
                f"{fan_name}__{window}": {
                    "crps": item["crps"],
                    "coverage80": item["coverage"][0.80]["value"],
                    "rmse": item["rmse"],
                    "n": item["n"],
                }
                for (fan_name, window), item in summary.items()
            },
        }
    )
    logger.log_predictions(
        [
            mlx.PredictionRow(
                model=f"{item.fan}|{label}",
                window=item.window,
                target_date=item.meeting_date,
                predicted=float(step),
                actual=float(item.actual_step_bp),
                unit="bp",
                meta={
                    "fan": item.fan,
                    "weight": weight,
                    "p_shock": item.p_shock,
                    "w": item.weight,
                    "emergency": item.emergency,
                },
            )
            for item in fans
            for label, (step, weight) in zip(item.labels, item.steps)
        ]
    )
    logger.log_artifacts(artifacts)
    return logger.finish("ok", notes=f"decision={decision}")


# --- CLI ----------------------------------------------------------------------


def run_summary(
    as_of: Optional[date] = None,
    out_dir: Path = Path("."),
    rolling: bool = False,
    logger_enabled: Optional[bool] = None,
) -> int:
    as_of = as_of or date.today()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    meetings = krm.load_meetings()
    past = [day for day in meetings if day <= as_of]
    key_rate = krm.load_key_rate()
    deltas = krm.load_mp_summary_deltas()
    decisions, excluded, missing_rate = krm.build_decisions(past, key_rate, deltas)
    cpd_core, cpd_all = cr.load_cpd_both()
    ibk = krm.load_ibk()
    gdp = cr.load_gdp()
    consensus = cr.load_cpi_consensus()
    rows, missing_features = cr.build_cbr_rows(
        decisions, cpd_core, cpd_all, ibk, gdp, consensus
    )
    if not rows:
        print("выборка пуста — веер v2 не считается", file=sys.stderr)
        return 1
    panel, shock_meetings, shock_key_rate, channels = fan1.load_shock_panel()
    panel_by_date = fan1.align_rows(rows, panel)
    weight_by_date, probability_by_date = fan1.shock_arrays(panel)
    budget = fan1.load_budget()
    impulse_by_date = {
        row.decision.meeting_date: fan1.budget_impulse(
            budget, row.decision.meeting_date
        )
        for row in rows
    }

    print(
        f"выборка: решений {len(decisions)}, строк {len(rows)}, "
        f"исключено {len(excluded)}, без ставки {len(missing_rate)}, "
        f"без базовых признаков {len(missing_features)}"
    )
    (
        fans,
        benchmarks,
        task1_fans,
        diagnostics,
        scale_coefficients,
        skips,
    ) = run_v2_walk_forward(
        rows,
        panel_by_date,
        weight_by_date,
        probability_by_date,
        impulse_by_date,
    )
    summary = fan1.summarize_fans(fans)
    benchmark_summary = fan1.summarize_benchmarks(benchmarks)
    episodes = fan1.summarize_episodes(fans)
    comparisons = make_comparisons(fans, benchmarks, task1_fans)
    decision, reason = decide(summary, comparisons)

    # Для текущего прогноза нужны ordered-прогнозы expanding-окна.
    ordered_by_date, _ = core_predictions(
        rows, "ordered_logit", krm.fit_ordered_logit, None
    )
    current = current_forecast(
        rows,
        key_rate,
        cpd_core,
        cpd_all,
        ibk,
        gdp,
        consensus,
        budget,
        panel,
        channels,
        shock_meetings,
        impulse_by_date,
        ordered_by_date,
        weight_by_date,
        probability_by_date,
        as_of,
    )

    report_path = out_dir / REPORT_NAME
    predictions_path = out_dir / PREDICTIONS_NAME
    diagnostics_path = out_dir / DIAGNOSTICS_NAME
    forecast_path = out_dir / FORECAST_NAME
    report_path.write_text(
        render_report(
            as_of,
            rows,
            summary,
            benchmark_summary,
            comparisons,
            episodes,
            skips,
            scale_coefficients,
            decision,
            reason,
            current,
        ),
        encoding="utf-8",
    )
    fan1.write_predictions(predictions_path, fans)
    write_diagnostics(diagnostics_path, diagnostics)
    fan1.write_forecast(forecast_path, current)

    selected = krm.window_label(ROLLING_WINDOW) if rolling else WINDOW_EXPANDING
    print(f"окно консольной сводки: {selected}")
    for fan_name in FAN_ORDER:
        item = summary.get((fan_name, selected))
        if item is None:
            print(f"  {fan_name:<22} фолдов 0")
            continue
        print(
            f"  {fan_name:<22} n={item['n']:>3}  CRPS {item['crps']:>7.3f}  "
            f"coverage80 {item['coverage'][0.80]['value']:.3f}  "
            f"RMSE {item['rmse']:>7.2f}"
        )
    print(f"вердикт: {decision} — {reason}")
    print(f"отчёт: {report_path}")
    print(f"предсказания: {predictions_path}")
    print(f"диагностика: {diagnostics_path}")
    print(f"прогноз: {forecast_path}")

    try:
        result = _log_run(
            rows,
            fans,
            summary,
            decision,
            as_of,
            [report_path, predictions_path, diagnostics_path, forecast_path],
            enabled=logger_enabled,
        )
        print(f"журнал эксперимента: {result}")
    except Exception as error:  # noqa: BLE001 — журнал не должен ронять прогон
        print(f"журнал эксперимента: ошибка ({error}) — прогон продолжен", file=sys.stderr)
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Веер v2: ядро ordered logit/ML + модель дисперсии."
    )
    parser.add_argument("--as-of", metavar="YYYY-MM-DD", default=None)
    parser.add_argument("--out-dir", metavar="DIR", default=".")
    parser.add_argument("--rolling", action="store_true")
    args = parser.parse_args(argv)
    as_of = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of)
        except ValueError:
            print(f"ошибка --as-of: {args.as_of!r} не дата YYYY-MM-DD", file=sys.stderr)
            return 2
    try:
        return run_summary(
            as_of=as_of,
            out_dir=Path(args.out_dir),
            rolling=args.rolling,
        )
    except krm.SampleError as error:
        print(f"ошибка выборки: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
