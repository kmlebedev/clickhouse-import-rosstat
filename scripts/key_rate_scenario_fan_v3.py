#!/usr/bin/env python3
"""Task 4: channel-complete core + dispersion recalibration (issue #289).

Спека: `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v3-design.md`
План: `docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v3.md`

Отличие от Task 3: ядро — ElasticNetCV на всех доступных каналах
`KEY_RATE_CHANNELS.md` §3 (shock-панель + savings, OFZ, Brent, BOP, ccyb, text);
дисперсия пересчитана под channel-core — volatility-scaled conformal.

**В `forecast_log` не пишет, в ClickHouse не пишет.** Артефакты локальны;
`ML_LOG=1` — паспорт run через `ml_experiment`.
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
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.linear_model import ElasticNetCV

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import key_rate_cbr_rule as cr  # noqa: E402
import key_rate_model as krm  # noqa: E402
import key_rate_scenario_fan as fan1  # noqa: E402
import key_rate_scenario_fan_v2 as v2  # noqa: E402
import ml_experiment as mlx  # noqa: E402
import shock_classifier as sc  # noqa: E402

log = logging.getLogger("key_rate_scenario_fan_v3")

SPEC_PATH = "docs/superpowers/specs/2026-10-20-cbr-scenario-fan-v3-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-20-cbr-scenario-fan-v3.md"
REPORT_NAME = "key_rate_scenario_fan_v3_report.md"
PREDICTIONS_NAME = "key_rate_scenario_fan_v3_predictions.csv"
DIAGNOSTICS_NAME = "key_rate_scenario_fan_v3_diagnostics.csv"
FORECAST_NAME = "key_rate_scenario_fan_v3_forecast.json"

WINDOW_EXPANDING = krm.WINDOW_EXPANDING
ROLLING_WINDOW = krm.ROLLING_WINDOW
WINDOWS = (WINDOW_EXPANDING, krm.window_label(ROLLING_WINDOW))
WINDOW_PAIRS = ((WINDOW_EXPANDING, None), (krm.window_label(ROLLING_WINDOW), ROLLING_WINDOW))
MIN_TRAIN = krm.MIN_TRAIN
MIN_RESIDUALS = v2.MIN_RESIDUALS
ALPHAS = v2.ALPHAS
FAN_ORDER = (
    "channel_fan_conformal",
    "channel_fan_vol",
    "channel_fan_calibrated",
)
PRIMARY_FAN = "channel_fan_vol"
PRIMARY_WINDOW = WINDOW_EXPANDING
PRIMARY_LEVEL = fan1.PRIMARY_LEVEL
REFERENCE_FAN = "core_fan_conformal"
SEED = 20261020
EPS = 1e-9
DEPOSITS_SERIES = "Депозиты"
CONSUMER_SERIES = "потребительские ссуды - всего"
MORTGAGE_SERIES = "ипотечные жилищные кредиты - всего"
BOP_SERIES = "Счет текущих операций (квартал)"


# --- As-of ---------------------------------------------------------------------


def _month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def monthly_asof(points: list, day: date) -> Optional[float]:
    """Последняя месячная точка, доступная к концу M+1 (конвенция v0)."""
    best = None
    for point_date, value in points:
        if krm.available_from_cpd(_month_end(point_date)) <= day:
            best = value
    return best


def daily_asof(points: list, day: date) -> Optional[float]:
    best = None
    for point_date, value in points:
        if point_date <= day:
            best = value
    return best


def quarterly_asof(points: list, day: date) -> Optional[float]:
    """Последняя квартальная точка, доступная к концу квартала + 3 месяца.

    Точки `v_cbr_bop` датированы концом квартала (2026-03-31), а
    `available_from_gdp` ждёт первое число месяца после отчётного квартала.
    """
    best = None
    for point_date, value in points:
        if cr.available_from_gdp(point_date + timedelta(days=1)) <= day:
            best = value
    return best


def monthly_yoy(points: list, day: date) -> Optional[float]:
    current = monthly_asof(points, day)
    previous = monthly_asof(points, day - timedelta(days=365))
    if current is None or previous is None or previous == 0:
        return None
    return (current / previous - 1.0) * 100.0


def daily_change_3m(points: list, day: date) -> Optional[float]:
    current = daily_asof(points, day)
    previous = daily_asof(points, day - timedelta(days=90))
    if current is None or previous is None or current <= 0 or previous <= 0:
        return None
    return 100.0 * math.log(current / previous)


# --- Загрузчики ----------------------------------------------------------------


def _points(rows: list) -> list:
    return sorted((krm.row_date(row["date"]), krm.row_float(row["value"])) for row in rows)


def load_ofz() -> dict:
    sql = """
SELECT date, ofz_10y_minus_key_rate_pp, ofz_2y_minus_key_rate_pp, slope_10y_1y_pp
FROM v_ofz_signal ORDER BY date FORMAT JSON
"""
    rows = krm.ch_query(sql)
    result = {"ofz10_spread": [], "ofz2_spread": [], "slope_10y_1y": []}
    for row in rows:
        day = krm.row_date(row["date"])
        for key, column in (
            ("ofz10_spread", "ofz_10y_minus_key_rate_pp"),
            ("ofz2_spread", "ofz_2y_minus_key_rate_pp"),
            ("slope_10y_1y", "slope_10y_1y_pp"),
        ):
            value = row.get(column)
            if value is not None:
                result[key].append((day, float(value)))
    return result


def load_brent() -> list:
    sql = """
SELECT date, value FROM v_moex_market WHERE series = 'brent_front' ORDER BY date FORMAT JSON
"""
    return _points(krm.ch_query(sql))


def load_bop() -> list:
    sql = f"""
SELECT date, value FROM v_cbr_bop
WHERE source = 'cbr_bop_standard' AND series = '{BOP_SERIES}'
ORDER BY date FORMAT JSON
"""
    return _points(krm.ch_query(sql))


def load_savings() -> dict:
    sql = f"""
SELECT series, date, value FROM v_cbr_macro
WHERE source = 'cbr'
  AND series IN ('{DEPOSITS_SERIES}', '{CONSUMER_SERIES}', '{MORTGAGE_SERIES}')
ORDER BY series, date FORMAT JSON
"""
    rows = krm.ch_query(sql)
    result = {DEPOSITS_SERIES: [], CONSUMER_SERIES: [], MORTGAGE_SERIES: []}
    for row in rows:
        result.setdefault(str(row["series"]), []).append(
            (krm.row_date(row["date"]), krm.row_float(row["value"]))
        )
    return result


def load_ccyb() -> dict:
    sql = """
SELECT series, date, value FROM v_cbr_ccyb_indicators
WHERE source = 'cbr_ccyb_indicators'
  AND series IN ('credit_gap_broad', 'hh_yoy', 'nfo_rub_yoy')
ORDER BY series, date FORMAT JSON
"""
    try:
        rows = krm.ch_query(sql)
    except Exception as error:  # noqa: BLE001 — витрина может быть недоступна
        log.warning("ccyb: витрина недоступна (%s) — missing", error)
        return {}
    result = {}
    for row in rows:
        result.setdefault(str(row["series"]), []).append(
            (krm.row_date(row["date"]), krm.row_float(row["value"]))
        )
    return result


def load_text() -> dict:
    sql = """
SELECT meeting_date, channel, method FROM v_cbr_mp_summary_channels
WHERE method = 'regex' ORDER BY meeting_date FORMAT JSON
"""
    rows = krm.ch_query(sql)
    by_meeting = {}
    for row in rows:
        day = krm.row_date(row["meeting_date"])
        by_meeting.setdefault(day, set()).add(str(row["channel"]))
    return {day: len(channels) / 11.0 for day, channels in by_meeting.items()}


def text_asof(scores: dict, day: date) -> Optional[float]:
    """Последнее резюме с meeting_date + 14 дней <= day; иначе None."""
    best = None
    best_day = None
    for meeting, score in scores.items():
        if meeting + timedelta(days=14) <= day and (best_day is None or meeting > best_day):
            best = score
            best_day = meeting
    return best


# --- Признаки ------------------------------------------------------------------


@dataclass
class ChannelRow:
    decision: krm.Decision
    features: dict


EXTERNAL_KEYS = (
    "deposit_yoy",
    "deposit_share",
    "ofz10_spread",
    "ofz2_spread",
    "slope_10y_1y",
    "brent_3m",
    "bop_ca_q",
    "credit_gap_broad",
    "hh_yoy",
    "nfo_rub_yoy",
    "text_channel_score",
    "text_available",
    "has_expectations",
    "has_savings",
    "has_ccyb",
    "has_text",
    "has_bop",
)


def _has(value) -> int:
    return 0 if value is None else 1


def channel_features(decision, base_features: dict, external: dict) -> dict:
    """Признаки каналов на дату `decision.meeting_date` из базовой панели."""
    day = decision.meeting_date
    features = dict(base_features)
    deposits = monthly_asof(external["savings"][DEPOSITS_SERIES], day)
    consumer = monthly_asof(external["savings"][CONSUMER_SERIES], day)
    mortgage = monthly_asof(external["savings"][MORTGAGE_SERIES], day)
    deposit_share = None
    if deposits is not None and consumer is not None and mortgage is not None:
        denominator = deposits + consumer + mortgage
        if denominator > 0:
            deposit_share = 100.0 * deposits / denominator
    features["deposit_yoy"] = monthly_yoy(external["savings"][DEPOSITS_SERIES], day)
    features["deposit_share"] = deposit_share
    features["ofz10_spread"] = daily_asof(external["ofz"]["ofz10_spread"], day)
    features["ofz2_spread"] = daily_asof(external["ofz"]["ofz2_spread"], day)
    features["slope_10y_1y"] = daily_asof(external["ofz"]["slope_10y_1y"], day)
    features["brent_3m"] = daily_change_3m(external["brent"], day)
    features["bop_ca_q"] = quarterly_asof(external["bop"], day)
    features["credit_gap_broad"] = monthly_asof(
        external["ccyb"].get("credit_gap_broad", []), day
    )
    features["hh_yoy"] = monthly_asof(external["ccyb"].get("hh_yoy", []), day)
    features["nfo_rub_yoy"] = monthly_asof(
        external["ccyb"].get("nfo_rub_yoy", []), day
    )
    text_score = text_asof(external["text"], day)
    features["text_channel_score"] = text_score
    features["text_available"] = _has(text_score)
    infl_exp = features.get("infl_exp_expected")
    price_exp = features.get("price_expectations")
    features["has_expectations"] = _has(
        infl_exp if infl_exp is not None else price_exp
    )
    features["has_savings"] = _has(features["deposit_yoy"])
    features["has_ccyb"] = _has(features["credit_gap_broad"])
    features["has_text"] = _has(text_score)
    features["has_bop"] = _has(features["bop_ca_q"])
    return features


def _cbr_features(row) -> dict:
    return {
        "gdp_gap": row.gdp_gap,
        "ibk_gap": row.ibk_gap,
    }


def build_channel_rows(rows: list, panel: list, external: dict) -> list:
    panel_by_date = {item.decision.meeting_date: item for item in panel}
    result = []
    for row in rows:
        features = channel_features(
            row.decision, panel_by_date[row.decision.meeting_date].features, external
        )
        features.update(_cbr_features(row))
        result.append(ChannelRow(decision=row.decision, features=features))
    return result


# --- Ядро ----------------------------------------------------------------------


def _matrix(rows: list, features: tuple) -> np.ndarray:
    data = [
        [
            np.nan if row.features.get(name) is None else float(row.features[name])
            for name in features
        ]
        for row in rows
    ]
    return np.asarray(data, dtype=float)


def _column_medians(matrix: np.ndarray) -> np.ndarray:
    medians = np.zeros(matrix.shape[1], dtype=float)
    for column in range(matrix.shape[1]):
        values = matrix[:, column]
        finite = values[np.isfinite(values)]
        if finite.size:
            medians[column] = float(np.median(finite))
    return medians


def fit_enet(features: tuple):
    def fit(train: list, row: ChannelRow) -> krm.Forecast:
        train_matrix = _matrix(train, features)
        target = np.asarray([item.decision.step_bp for item in train], dtype=float)
        if len(train) < 8 or len(set(target.tolist())) < 2:
            raise krm.DegenerateWindowError("channel_enet: вырожденное окно")
        medians = _column_medians(train_matrix)
        train_matrix = np.where(np.isnan(train_matrix), medians, train_matrix)
        row_vector = np.asarray(
            [
                np.nan if row.features.get(name) is None else float(row.features[name])
                for name in features
            ],
            dtype=float,
        )
        row_vector = np.where(np.isnan(row_vector), medians, row_vector)
        mean = train_matrix.mean(axis=0)
        scale = train_matrix.std(axis=0)
        scale = np.where(scale > 0, scale, 1.0)
        train_matrix = (train_matrix - mean) / scale
        row_vector = (row_vector - mean) / scale
        try:
            model = ElasticNetCV(
                l1_ratio=[0.1, 0.5, 0.9],
                cv=min(5, len(train)),
                random_state=SEED,
                max_iter=10000,
            )
            model.fit(train_matrix, target)
            step = float(model.predict(row_vector.reshape(1, -1))[0])
        except Exception as error:  # noqa: BLE001 — вырожденное окно
            raise krm.DegenerateWindowError(f"channel_enet: {error}") from error
        if not math.isfinite(step):
            raise krm.DegenerateWindowError("channel_enet: неконечный прогноз")
        return krm.Forecast(step, None)
    return fit


# --- Волновая дисперсия --------------------------------------------------------


def _robust_z(values: list, current: Optional[float]) -> Optional[float]:
    clean = [value for value in values if value is not None and math.isfinite(value)]
    if current is None or not clean:
        return None
    array = np.asarray(clean, dtype=float)
    median = float(np.median(array))
    q75, q25 = np.percentile(array, [75.0, 25.0])
    scale = float(q75 - q25)
    if scale <= 0.0:
        scale = float(np.std(array))
    if scale <= 0.0:
        return 0.0
    return max(-2.0, min(2.0, (current - median) / scale))


def volatility_index(features: dict, train_features: list) -> Optional[float]:
    components = []
    for name in ("usdrub_3m", "slope_10y_1y", "brent_3m"):
        current = features.get(name)
        if current is None:
            continue
        history = [item.get(name) for item in train_features]
        z = _robust_z([abs(value) for value in history if value is not None], abs(current))
        if z is not None:
            components.append(z)
    if not components:
        return None
    return sum(components) / len(components)


def disp_vol(residuals: list, past_vol: list, current_vol: Optional[float]) -> v2.Dispersion:
    if current_vol is None:
        raise krm.DegenerateWindowError("disp_vol: нет текущей vol")
    if len(residuals) != len(past_vol):
        raise ValueError("disp_vol: длины остатков и vol не совпадают")
    pairs = [
        (residual, vol)
        for residual, vol in zip(residuals, past_vol)
        if vol is not None and math.isfinite(vol)
    ]
    if len(pairs) < MIN_RESIDUALS:
        raise krm.DegenerateWindowError("disp_vol: мало данных")
    pair_residuals, pair_vols = zip(*pairs)
    design = np.column_stack([np.ones(len(pairs)), np.asarray(pair_vols, dtype=float)])
    target = np.log(np.abs(np.asarray(pair_residuals, dtype=float)) + 1.0)
    coefficients, *_ = np.linalg.lstsq(design, target, rcond=None)
    if not np.all(np.isfinite(coefficients)):
        raise krm.DegenerateWindowError("disp_vol: неконечные коэффициенты")
    scales = np.exp(design @ coefficients) - 1.0
    scales = np.maximum(scales, EPS)
    standardized = np.asarray(pair_residuals, dtype=float) / scales
    quantiles = np.quantile(standardized, ALPHAS)
    current = float(np.dot([1.0, current_vol], coefficients))
    scale = math.exp(current) - 1.0
    if not math.isfinite(scale) or scale <= 0.0:
        raise krm.DegenerateWindowError("disp_vol: scale_t <= 0")
    return v2.Dispersion(
        "disp_vol",
        tuple(scale * float(value) for value in quantiles),
        scale,
        tuple(float(value) for value in coefficients),
    )


def disp_calibrated(
    residuals: list, past_intervals: list
) -> v2.Dispersion:
    base = v2.disp_conformal(residuals)
    if not past_intervals:
        return v2.Dispersion("disp_calibrated", base.quantiles, 1.0, (1.0,))
    hits = sum(
        1.0 for actual, low, high in past_intervals if low <= actual <= high
    ) / len(past_intervals)
    factor = min(1.5, max(0.8, 1.0 + 2.0 * (0.8 - hits)))
    return v2.Dispersion(
        "disp_calibrated",
        tuple(factor * value for value in base.quantiles),
        factor,
        (factor,),
    )


# --- Диагностика ---------------------------------------------------------------


@dataclass(frozen=True)
class DiagnosticRow:
    meeting_date: date
    window: str
    core_step: float
    vol: Optional[float]
    scale: float
    quantiles: tuple
    feature_coverage: int
    feature_total: int


# --- Walk-forward --------------------------------------------------------------


def run_v3_walk_forward(
    rows: list,
    panel: list,
    panel_by_date: dict,
    weight_by_date: dict,
    probability_by_date: dict,
    impulse_by_date: dict,
    external: dict,
) -> tuple:
    channel_rows = build_channel_rows(rows, panel, external)
    channel_by_date = {item.decision.meeting_date: item for item in channel_rows}
    features = tuple(channel_rows[0].features)
    reference_fans, benchmarks, _, _, _, _ = v2.run_v2_walk_forward(
        rows,
        panel_by_date,
        weight_by_date,
        probability_by_date,
        impulse_by_date,
    )
    cores = {}
    skips = Counter()
    for window, rolling in WINDOW_PAIRS:
        predictions, skipped = krm.walk_forward(
            channel_rows, "channel_enet", fit_enet(features), min_train=MIN_TRAIN, rolling=rolling
        )
        cores[window] = {item.meeting_date: item for item in predictions}
        skips[f"core__{window}"] = skipped

    fans = []
    diagnostics = []
    for window, rolling in WINDOW_PAIRS:
        start = MIN_TRAIN if rolling is None else max(MIN_TRAIN, rolling)
        past_intervals = []
        for index in range(start, len(rows)):
            train = channel_rows[:index] if rolling is None else channel_rows[index - rolling:index]
            row = channel_rows[index]
            day = row.decision.meeting_date
            core = cores[window].get(day)
            if core is None:
                skips["core_missing"] += 1
                continue
            past = sorted(
                (
                    item
                    for item in cores[window].values()
                    if item.meeting_date < day
                ),
                key=lambda item: item.meeting_date,
            )
            residuals = [item.actual_step_bp - item.step_bp for item in past]
            if len(residuals) < MIN_RESIDUALS:
                skips["residuals"] += 1
                continue
            impulse = impulse_by_date.get(day)
            median, scale = fan1.train_budget_stats(rows[:index], impulse_by_date)
            budget_z = fan1.budget_z_score(impulse, median, scale)
            try:
                steps, deltas = v2.centered_deltas(rows[index], impulse, budget_z)
            except krm.DegenerateWindowError:
                skips["fan_fold"] += 1
                continue
            past_features = [item.features for item in train]
            past_vol = [
                volatility_index(
                    channel_by_date[past_item.meeting_date].features,
                    past_features,
                )
                for past_item in past
            ]
            current_vol = volatility_index(row.features, past_features)
            conformal = v2.disp_conformal(residuals)
            try:
                vol_disp = disp_vol(residuals, past_vol, current_vol)
            except (krm.DegenerateWindowError, ValueError):
                vol_disp = None
                skips["vol_fallback"] += 1
            calibrated = disp_calibrated(residuals, past_intervals)
            specs = [
                ("channel_fan_conformal", conformal),
                ("channel_fan_calibrated", calibrated),
            ]
            if vol_disp is not None:
                specs.insert(1, ("channel_fan_vol", vol_disp))
            emergency = bool(panel_by_date[day].emergency)
            for name, dispersion in specs:
                fans.append(
                    v2.make_fan(
                        name,
                        day,
                        window,
                        core.step_bp,
                        steps,
                        deltas,
                        dispersion,
                        row.decision.step_bp,
                        emergency,
                        probability_by_date.get(day, 0.0),
                        weight_by_date.get(day, 1.0),
                    )
                )
            conformal_points = v2.make_fan(
                "tmp",
                day,
                window,
                core.step_bp,
                steps,
                deltas,
                conformal,
                row.decision.step_bp,
                emergency,
                0.0,
                1.0,
            )
            con_points = [point for point, _ in conformal_points.steps]
            con_weights = [weight for _, weight in conformal_points.steps]
            low = fan1.weighted_quantile(con_points, con_weights, 0.10)
            high = fan1.weighted_quantile(con_points, con_weights, 0.90)
            past_intervals.append((row.decision.step_bp, low, high))
            diagnostics.append(
                DiagnosticRow(
                    meeting_date=day,
                    window=window,
                    core_step=core.step_bp,
                    vol=current_vol,
                    scale=vol_disp.scale if vol_disp is not None else 1.0,
                    quantiles=(vol_disp or conformal).quantiles,
                    feature_coverage=sum(1 for value in row.features.values() if value is not None),
                    feature_total=len(row.features),
                )
            )
    return fans, reference_fans, benchmarks, diagnostics, skips, cores


# --- Текущий прогноз -----------------------------------------------------------


def current_forecast(
    rows: list,
    channel_rows: list,
    panel: list,
    channels: dict,
    shock_meetings: list,
    key_rate: list,
    cpd_core: list,
    cpd_all: list,
    ibk: list,
    gdp: list,
    consensus: list,
    budget,
    external: dict,
    cores: dict,
    impulse_by_date: dict,
    weight_by_date: dict,
    probability_by_date: dict,
    as_of: date,
) -> Optional[dict]:
    """Информационный as-of прогноз на ближайшее заседание; в forecast_log не идёт."""
    future = sorted({day for day, _ in shock_meetings if day > as_of})
    if not future:
        return None
    meeting = future[0]
    inputs = krm.forecast_inputs(as_of, key_rate, cpd_core, ibk)
    base = krm.forecast_row(meeting, inputs)
    if base is None:
        return {"skipped": "нет входов as-of"}
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
    feature_decision = krm.Decision(
        meeting_date=as_of,
        rate_before=base.decision.rate_before,
        rate_after=krm.FORECAST_UNKNOWN_FACT,
        step_bp=krm.FORECAST_UNKNOWN_FACT,
        y3=krm.FORECAST_UNKNOWN_FACT,
    )
    base_features = sc._base_features(feature_decision, channels)
    prior = panel[-1].features
    for name in sc.DELTA_FEATURES:
        current = base_features.get(name)
        previous = prior.get(name)
        base_features[f"{name}_chg"] = (
            None if current is None or previous is None else current - previous
        )
    future_features = channel_features(feature_decision, base_features, external)
    future_features.update(_cbr_features(current_row))
    channel_by_date = {item.decision.meeting_date: item for item in channel_rows}
    past = sorted(cores[WINDOW_EXPANDING].values(), key=lambda item: item.meeting_date)
    residuals = [item.actual_step_bp - item.step_bp for item in past]
    train_features = [item.features for item in channel_rows]
    current_vol = volatility_index(future_features, train_features)
    past_vol = [
        volatility_index(channel_by_date[item.meeting_date].features, train_features)
        for item in past
    ]
    try:
        core = fit_enet(tuple(channel_rows[0].features))(
            channel_rows, ChannelRow(base.decision, future_features)
        )
        impulse = fan1.budget_impulse(budget, as_of)
        median, scale = fan1.train_budget_stats(rows, impulse_by_date)
        budget_z = fan1.budget_z_score(impulse, median, scale)
        steps, deltas = v2.centered_deltas(current_row, impulse, budget_z)
        try:
            dispersion = disp_vol(residuals, past_vol, current_vol)
        except (krm.DegenerateWindowError, ValueError):
            dispersion = v2.disp_conformal(residuals)
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
    next_forecast = sc.next_meeting_forecast(
        panel, shock_meetings, key_rate, channels, as_of
    )
    current_weight = next_forecast.weight if next_forecast is not None else 1.0
    current_probability = (
        min(fan1.P_SHOCK_MAX, max(0.0, next_forecast.shock_prob))
        if next_forecast is not None and next_forecast.shock_prob is not None
        else 0.0
    )
    return {
        "as_of": as_of.isoformat(),
        "meeting_date": meeting.isoformat(),
        "core": "channel_enet",
        "core_step": core.step_bp,
        "probs": None,
        "budget_impulse": impulse,
        "scenarios": [
            {"name": item.name, "delta": delta, "fallback": item.fallback}
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


# --- Сравнения и решение -------------------------------------------------------


def make_comparisons(fans: list, reference_fans: list, benchmarks: list) -> dict:
    primary_group = [item for item in fans if item.fan == PRIMARY_FAN and item.window == PRIMARY_WINDOW]
    reference_group = [
        item for item in reference_fans if item.fan == REFERENCE_FAN and item.window == PRIMARY_WINDOW
    ]
    comparisons = {
        "primary_vs_reference": fan1.compare_fans(primary_group, reference_group),
    }
    for method in ("hold", "repeat", "ar1", "ordered_logit"):
        bench_group = [
            item for item in benchmarks if item.method == method and item.window == PRIMARY_WINDOW
        ]
        comparisons[f"primary_vs_{method}"] = fan1.compare_fan_to_benchmark(
            primary_group, bench_group, method
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
    reference = comparisons.get("primary_vs_reference")
    reference_ok = bool(
        reference and reference["crps_diff"] < 0.0 and reference["crps_high"] < 0.0
    )
    ordered = comparisons.get("primary_vs_ordered_logit")
    rmse_ok = ordered is None or primary["rmse"] <= ordered["bench_rmse"] * 1.10
    reason = (
        f"CRPS {primary['crps']:.2f}; coverage80 {coverage['value']:.3f} "
        f"[{coverage['interval'][0]:.3f}; {coverage['interval'][1]:.3f}]; "
        f"vs reference ΔCRPS {reference['crps_diff']:.2f} "
        f"[{reference['crps_low']:.2f}; {reference['crps_high']:.2f}]"
        if reference
        else f"CRPS {primary['crps']:.2f}; coverage80 {coverage['value']:.3f}"
    )
    if coverage_ok and reference_ok and rmse_ok:
        return "promote", reason
    if coverage_ok or reference_ok:
        return "iterate", reason
    return "reject", reason


# --- Артефакты -----------------------------------------------------------------


def write_diagnostics(path: Path, diagnostics: list) -> None:
    columns = (
        "meeting_date",
        "window",
        "core_step",
        "vol",
        "scale",
        *[f"q{alpha}" for alpha in ALPHAS],
        "feature_coverage",
        "feature_total",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for item in diagnostics:
            writer.writerow(
                [
                    item.meeting_date.isoformat(),
                    item.window,
                    f"{item.core_step:.6f}",
                    "" if item.vol is None else f"{item.vol:.6f}",
                    f"{item.scale:.6f}",
                    *[f"{value:.6f}" for value in item.quantiles],
                    item.feature_coverage,
                    item.feature_total,
                ]
            )


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
    comparisons: dict,
    episodes: dict,
    skips: Counter,
    decision: str,
    reason: str,
    current: Optional[dict],
) -> str:
    lines = [
        "# Channel-complete веер: все каналы + volatility-дисперсия",
        "",
        f"**Дата:** {as_of.isoformat()}",
        "**Пункт:** `key-rate/phase-4-cbr-scenario-fan`, Task 4 (issue #289)",
        f"**Спека:** [{SPEC_PATH}](../superpowers/specs/2026-10-20-cbr-scenario-fan-v3-design.md)",
        f"**План:** [{PLAN_PATH}](../superpowers/plans/2026-10-20-cbr-scenario-fan-v3.md)",
        "",
        "## 1. Выборка",
        "",
        "| Показатель | Значение |",
        "|---|---:|",
        f"| заседаний | {len(rows)} |",
        f"| фан-фолды primary | {summary.get((PRIMARY_FAN, WINDOW_EXPANDING), {}).get('n', 0)} expanding / {summary.get((PRIMARY_FAN, krm.window_label(ROLLING_WINDOW)), {}).get('n', 0)} rolling |",
        f"| residual warm-up | {skips.get('residuals', 0)} |",
        f"| vol fallback | {skips.get('vol_fallback', 0)} |",
        "",
        "## 2. Каналы",
        "",
        "| Канал | Ряд | Признак(и) |",
        "|---|---|---|",
        "| Текущая/базовая инфляция | `cbr_indicators_cpd` | `pi_saar`, `cpi_saar` (+ дельты) |",
        "| Ожидания | `cbr_infl_exp`, `cbr_survey`, `cbr_mo_br` | `infl_exp_expected`, `infl_exp_observed`, `price_expectations` (+ дельты) |",
        "| Рынок труда | `rosstat_labour_force_mes`, `rosstat_salaries_mes` | `unemployment`, `wage_yoy` (+ дельты) |",
        "| Кредит/трансмиссия | `cbr_credit_m2x`, `cbr_ruania`, ccyb | `credit_yoy`, `ruania_spread`, `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` |",
        "| Разрыв выпуска | `rosstat_vvp_kvartal`, `cbr_survey` | `gdp_gap`, `ibk_gap`, `business_climate` (+ дельта) |",
        "| Бюджет | `minfin_fed_bud_mes` | `budget_balance`; `budget_impulse` — сценарная дельта |",
        "| Деньги | `cbr_m2` | `m2_yoy` |",
        "| Внешние условия | `cbr_currency_usd`, `moex_market`, `v_cbr_bop` | `usdrub_3m`, `brent_3m`, `bop_ca_q` |",
        "| Сбережения | `cbr_households_b_mes` | `deposit_yoy`, `deposit_share` |",
        "| Макропруденциальные | `v_cbr_ccyb_indicators` | `credit_gap_broad`, `hh_yoy`, `nfo_rub_yoy` |",
        "| Рынок ОФЗ | `v_ofz_signal` | `ofz10_spread`, `ofz2_spread`, `slope_10y_1y` |",
        "| KEYRATE | — | **недоступен**: история с 2026-09 |",
        "| Текст | `v_cbr_mp_summary_channels` | `text_channel_score`, `text_available`; 2024+, +14 дней |",
        "",
        "## 3. Веер × окно",
        "",
        "| веер | окно | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | RMSE | MAE |",
        "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
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
                f"| {_fmt(item['rmse'])} | {_fmt(item['mae'])} |"
            )
    lines.extend(
        [
            "",
            "## 4. Сравнение с Task 3 и бенчмарками",
            "",
            "| пара | n | ΔCRPS | 95 % CI | p (бутстр.) | DM p |",
            "|---|---:|---:|---|---:|---:|",
        ]
    )
    for key, item in comparisons.items():
        if item is None:
            lines.append(f"| {key} | — | — | — | — | — |")
            continue
        lines.append(
            f"| {key} | {item.get('n', '—')} | {_fmt(item.get('crps_diff'), 3)} "
            f"| [{_fmt(item.get('crps_low'), 3)}; {_fmt(item.get('crps_high'), 3)}] "
            f"| {_fmt_p(item.get('crps_p'))} | {_fmt_p(item.get('dm_p'))} |"
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
            "## 6. Вердикт",
            "",
            f"**`{decision}`** — {reason}.",
            "",
            "Primary — `channel_fan_vol`; `channel_fan_conformal` — ablation; "
            "`channel_fan_calibrated` — robustness. Сравнение — с `core_fan_conformal` (Task 3).",
            "",
            "## 7. Текущий прогноз",
            "",
        ]
    )
    if current is None:
        lines.append("Будущих заседаний в календаре нет — прогноз не считается.")
    elif "skipped" in current:
        lines.append(f"Пропущен: {current['skipped']}.")
    else:
        lines.extend(
            [
                f"- as-of: {current['as_of']}; заседание: {current['meeting_date']}",
                f"- ядро: `{current['core']}`, шаг {_fmt(current['core_step'])} б.п.; "
                f"дисперсия `{current['dispersion']}` (scale {_fmt(current['scale'], 3)})",
                f"- веер 10/50/90: {_fmt(current['fan_quantiles']['0.1'])} / "
                f"{_fmt(current['fan_quantiles']['0.5'])} / "
                f"{_fmt(current['fan_quantiles']['0.9'])} б.п.",
                f"- вес {current['weight']:.3f}, p(шок) {current['p_shock']:.3f}",
            ]
        )
    lines.extend(
        [
            "",
            "## 8. Ограничения",
            "",
            "- PIT/винтажей savings/ccyb/OFZ/BOP нет; as-of консервативный.",
            "- Text 2024+, KEYRATE 2026+ — missing-индикаторы; KEYRATE не используется.",
            "- `v_cbr_ccyb_indicators`: credit_gap_broad — квартальный, последняя точка 2024-07; "
            "hh_yoy/nfo_rub_yoy — 2024-10; дальше missing.",
            "- `v_cbr_mp_summary_channels` не содержит `published`: text as-of — "
            "`meeting_date + 14 дней`.",
            "- 108 решений, ~41 признак: ElasticNet-регуляризация; ablations.",
            "- 2022–2023 может требовать отдельной regime-дисперсии.",
            "- В `forecast_log` не пишется; Go/DDL/витрины/гранты не менялись.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


# --- Журнал --------------------------------------------------------------------


def _log_run(
    rows: list,
    fans: list,
    summary: dict,
    decision: str,
    as_of: date,
    artifacts: list,
    enabled: Optional[bool] = None,
) -> str:
    logger = mlx.start_run(
        experiment="key_rate_scenario_fan_v3",
        run_name="scenario_fan_v3__rate__start24",
        config={
            "core": "ElasticNetCV",
            "features": "channel-complete",
            "dispersion": ["conformal", "vol", "calibrated"],
            "primary_fan": PRIMARY_FAN,
            "as_of": as_of.isoformat(),
        },
        data_asof=as_of,
        sample_meta={
            "rows": len(rows),
            "first": rows[0].decision.meeting_date.isoformat() if rows else None,
            "last": rows[-1].decision.meeting_date.isoformat() if rows else None,
            "fans": len(fans),
        },
        sample=[
            {
                "meeting_date": row.decision.meeting_date.isoformat(),
                "step_bp": row.decision.step_bp,
            }
            for row in rows
        ],
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
                meta={"fan": item.fan, "weight": weight},
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
        print("выборка пуста — веер v3 не считается", file=sys.stderr)
        return 1
    panel, shock_meetings, shock_key_rate, channels = fan1.load_shock_panel()
    panel_by_date = fan1.align_rows(rows, panel)
    weight_by_date, probability_by_date = fan1.shock_arrays(panel)
    budget = fan1.load_budget()
    impulse_by_date = {
        row.decision.meeting_date: fan1.budget_impulse(budget, row.decision.meeting_date)
        for row in rows
    }
    external = {
        "ofz": load_ofz(),
        "brent": load_brent(),
        "bop": load_bop(),
        "savings": load_savings(),
        "ccyb": load_ccyb(),
        "text": load_text(),
    }

    print(
        f"выборка: решений {len(decisions)}, строк {len(rows)}, "
        f"исключено {len(excluded)}, без ставки {len(missing_rate)}, "
        f"без базовых признаков {len(missing_features)}"
    )
    fans, reference_fans, benchmarks, diagnostics, skips, cores = run_v3_walk_forward(
        rows,
        panel,
        panel_by_date,
        weight_by_date,
        probability_by_date,
        impulse_by_date,
        external,
    )
    summary = fan1.summarize_fans(fans)
    episodes = fan1.summarize_episodes(fans)
    comparisons = make_comparisons(fans, reference_fans, benchmarks)
    decision, reason = decide(summary, comparisons)
    channel_rows = build_channel_rows(rows, panel, external)
    current = current_forecast(
        rows,
        channel_rows,
        panel,
        channels,
        shock_meetings,
        shock_key_rate,
        cpd_core,
        cpd_all,
        ibk,
        gdp,
        consensus,
        budget,
        external,
        cores,
        impulse_by_date,
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
            as_of, rows, summary, comparisons, episodes, skips, decision, reason, current
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
            print(f"  {fan_name:<24} фолдов 0")
            continue
        print(
            f"  {fan_name:<24} n={item['n']:>3}  CRPS {item['crps']:>7.3f}  "
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
    except Exception as error:  # noqa: BLE001
        print(f"журнал эксперимента: ошибка ({error}) — прогон продолжен", file=sys.stderr)
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Channel-complete веер + volatility-дисперсия (Task 4 #289)."
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
        return run_summary(as_of=as_of, out_dir=Path(args.out_dir), rolling=args.rolling)
    except krm.SampleError as error:
        print(f"ошибка выборки: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
