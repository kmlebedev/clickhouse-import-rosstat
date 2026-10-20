#!/usr/bin/env python3
"""Сценарный веер ставки с shock-aware слоем (Task 1 issue #272).

Спека: `docs/superpowers/specs/2026-10-20-cbr-scenario-fan-design.md`
План: `docs/superpowers/plans/2026-10-20-cbr-scenario-fan.md`

Скрипт строит распределение шага ставки на заседании по 5 reduced-form
сценариям КПМ и добавляет shock-aware слой:

* `w_t` — причинные веса `shock_classifier` (аномальность каналов);
* `p_t` — вероятность явного шока (описательный классификатор, клип 0–0.25);
* emergency-ветка — шаги внеочередных заседаний только из обучающего окна.

Веера: `fan_raw` (ablation), `fan_shock` (primary), `fan_shock_weighted`
(вторичная гипотеза, `w_repeat` из #283 §6.1), `fan_emergency_ex_post`
(диагностика). Метрики — coverage 50/80/90, CRPS, ширина, Brier/log loss,
RMSE/MAE медианы, DM/бутстрап; разбивка по эпизодам.

**В `forecast_log` не пишет, в ClickHouse не пишет.** Артефакты локальны;
`ML_LOG=1` — паспорт run через `ml_experiment`.

Запуск — только через `make`:

    make key-rate-scenario-fan
    ML_LOG=1 make key-rate-scenario-fan
    make key-rate-scenario-fan-test
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import eval_utils as eu  # noqa: E402
import key_rate_cbr_rule as cr  # noqa: E402
import key_rate_model as krm  # noqa: E402
import ml_experiment as mlx  # noqa: E402
import shock_classifier as sc  # noqa: E402

log = logging.getLogger("key_rate_scenario_fan")

# --- Константы ----------------------------------------------------------------

SPEC_PATH = "docs/superpowers/specs/2026-10-20-cbr-scenario-fan-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-20-cbr-scenario-fan.md"
REPORT_NAME = "key_rate_scenario_fan_report.md"
PREDICTIONS_NAME = "key_rate_scenario_fan_predictions.csv"
DIAGNOSTICS_NAME = "key_rate_scenario_fan_diagnostics.csv"
FORECAST_NAME = "key_rate_scenario_fan_forecast.json"

WINDOW_EXPANDING = krm.WINDOW_EXPANDING
ROLLING_WINDOW = krm.ROLLING_WINDOW
WINDOWS = (WINDOW_EXPANDING, krm.window_label(ROLLING_WINDOW))
MIN_TRAIN = krm.MIN_TRAIN

FAN_ORDER = (
    "fan_raw",
    "fan_shock",
    "fan_shock_weighted",
    "fan_emergency_ex_post",
)
BENCHMARK_ORDER = ("hold", "repeat", "ar1", "ordered_logit", "climatology", "cbr_rule")
FAN_LEVELS = (0.50, 0.80, 0.90)
PRIMARY_LEVEL = 0.80
PRIMARY_FAN = "fan_shock"
PRIMARY_WINDOW = WINDOW_EXPANDING
BOOTSTRAP_BLOCK = 4
BOOTSTRAP_RUNS = 2000
BOOTSTRAP_SEED = 20261020

BUDGET_INCOME = "Доходы, всего"
BUDGET_DEFICIT = "Дефицит (-)/Профицит (+)"
BUDGET_SQL = f"""
SELECT series AS name, date, toFloat64(value) AS value
FROM v_minfin_budget
WHERE source = 'minfin'
  AND series IN ('{BUDGET_INCOME}', '{BUDGET_DEFICIT}')
ORDER BY date
FORMAT JSON
"""
BUDGET_A7_BASE = 0.10
BUDGET_A7_SCENARIO = 0.40
BUDGET_DELTA = BUDGET_A7_SCENARIO - BUDGET_A7_BASE
NML_A3_BASE = 0.15
NML_A3_SCENARIO = 0.05
NML_FACTOR = NML_A3_SCENARIO / NML_A3_BASE
P_SHOCK_MAX = 0.25
BUDGET_Z_CLIP = 2.0

EPISODES = (
    ("2014-12…2015", date(2014, 12, 1), date(2015, 12, 31)),
    ("2022–2023", date(2022, 1, 1), date(2023, 12, 31)),
    ("2024–2026", date(2024, 1, 1), date(2026, 12, 31)),
)


# --- Сценарии -----------------------------------------------------------------


@dataclass(frozen=True)
class Scenario:
    """Один сценарий веера: имя, спецификация правила и способ расчёта."""

    name: str
    spec: cr.RuleSpec
    kind: str  # "base" | "rational" | "budget" | "nml"


SCENARIOS = (
    Scenario("inertial", cr.BASE_SPEC, "base"),
    Scenario(
        "rational",
        cr.RuleSpec("fan_rational", e3_source="consensus"),
        "rational",
    ),
    Scenario("hard", cr.RuleSpec("fan_hard", gamma2=1.9), "base"),
    Scenario("budget", cr.BASE_SPEC, "budget"),
    Scenario("no_market_loans", cr.BASE_SPEC, "nml"),
)


@dataclass(frozen=True)
class ScenarioStep:
    """Шаг одного сценария и метка fallback (если признак заменён)."""

    name: str
    components: cr.RuleComponents
    fallback: Optional[str] = None


@dataclass(frozen=True)
class BudgetPoint:
    """Месячные ряды бюджета: доходы и сальдо по ключу (год, месяц)."""

    income: dict
    deficit: dict


@dataclass(frozen=True)
class FanPrediction:
    """Фолд веера: дата, окно, имя веера, точки (шаг, вес), факт и шок-мета."""

    meeting_date: date
    window: str
    fan: str
    steps: tuple
    labels: tuple
    actual_step_bp: float
    emergency: bool
    p_shock: float
    weight: float
    fallbacks: tuple = ()


@dataclass(frozen=True)
class BenchmarkPrediction:
    """Фолд точечного/вероятностного бенчмарка на общих датах."""

    meeting_date: date
    window: str
    method: str
    step_bp: float
    actual_step_bp: float
    probs: Optional[tuple]


# --- Загрузка данных ----------------------------------------------------------


def _month_key(day: date) -> tuple:
    return (day.year, day.month)


def _month_end(day: date) -> date:
    if day.month == 12:
        return date(day.year, 12, 31)
    return date(day.year, day.month + 1, 1) - timedelta(days=1)


def _shift_month_key(key: tuple, months: int) -> tuple:
    year, month = key
    index = year * 12 + (month - 1) + months
    return (index // 12, index % 12 + 1)


def _month_end_key(key: tuple) -> date:
    return _month_end(date(key[0], key[1], 1))


def budget_from_rows(rows: list) -> BudgetPoint:
    """Разбирает строки `minfin_fed_bud_mes` в два словаря по месяцам.

    Дубликат месяца внутри одного ряда — ошибка данных: `FINAL` должен был
    оставить одну версию, а молчаливый выбор последней скрыл бы дефект.
    """
    income = {}
    deficit = {}
    for row in rows:
        name = str(row["name"])
        if name not in (BUDGET_INCOME, BUDGET_DEFICIT):
            continue
        key = _month_key(krm.row_date(row["date"]))
        value = krm.row_float(row["value"])
        target = income if name == BUDGET_INCOME else deficit
        if key in target:
            raise krm.SampleError(f"бюджет: дубликат месяца {key} в ряду «{name}»")
        target[key] = value
    return BudgetPoint(income=income, deficit=deficit)


def load_budget() -> BudgetPoint:
    """Живые месячные ряды бюджета из `minfin_fed_bud_mes`."""
    return budget_from_rows(krm.ch_query(BUDGET_SQL))


def budget_impulse(budget: BudgetPoint, meeting_date: date) -> Optional[float]:
    """Фискальный импульс as-of заседания: `−дефицит_12м / доходы_12м · 100`.

    Последний доступный месяц — максимальный M, для которого
    `available_from_cpd(end_of_month(M)) <= meeting_date` (консервативный лаг
    «конец M+1», как у CPD). Нужны 12 последовательных месяцев обоих рядов;
    иначе `None` — «измеренного» импульса нет, а не ноль.
    """
    last = None
    for key in sorted(budget.income):
        if key not in budget.deficit:
            continue
        if krm.available_from_cpd(_month_end_key(key)) <= meeting_date:
            last = key
    if last is None:
        return None
    months = [_shift_month_key(last, -offset) for offset in range(11, -1, -1)]
    if any(key not in budget.income or key not in budget.deficit for key in months):
        return None
    income = sum(budget.income[key] for key in months)
    deficit = sum(budget.deficit[key] for key in months)
    if income <= 0.0:
        return None
    return -deficit / income * 100.0


def load_shock_panel() -> tuple:
    """Панель `shock_classifier` и её входы для текущего прогноза."""
    meetings = sc.load_meetings_with_emergency()
    key_rate = sc.load_key_rate()
    channels = sc.load_channels()
    deltas = sc.load_mp_summary_deltas()
    panel = sc.build_panel(meetings, key_rate, channels, deltas)
    return panel, meetings, key_rate, channels


def shock_arrays(panel: list) -> tuple:
    """Причинные веса `w_t` и вероятности шока `p_t`, выровненные по датам.

    `p_t` клипуется `[0; P_SHOCK_MAX]`; `None` (первые фолды/вырожденное окно)
    даёт 0. Клип фиксирован в спеке §5.2 и не подбирается по тесту.
    """
    weights = sc.walk_forward_weights(panel)
    probabilities = sc.walk_forward_classifier(panel)
    weight_by_date = {}
    prob_by_date = {}
    for row, weight, probability in zip(panel, weights, probabilities):
        day = row.decision.meeting_date
        weight_by_date[day] = float(weight)
        if probability is None:
            prob_by_date[day] = 0.0
        else:
            prob_by_date[day] = min(P_SHOCK_MAX, max(0.0, float(probability)))
    return weight_by_date, prob_by_date


def align_rows(rows: list, panel: list) -> dict:
    """Проверяет 1:1 совпадение дат `CbrRow` и шоковой панели."""
    panel_by_date = {row.decision.meeting_date: row for row in panel}
    row_dates = [row.decision.meeting_date for row in rows]
    if len(row_dates) != len(set(row_dates)):
        raise krm.SampleError("веер: дубликаты дат в выборке решений")
    if set(row_dates) != set(panel_by_date):
        missing = sorted(set(row_dates) - set(panel_by_date))
        extra = sorted(set(panel_by_date) - set(row_dates))
        raise krm.SampleError(
            "веер: даты выборки и шоковой панели не совпадают 1:1; "
            f"нет в панели {missing[:3]}…, лишние {extra[:3]}…"
        )
    return panel_by_date


# --- Признаки сценариев -------------------------------------------------------


def _components(
    row: cr.CbrRow,
    spec: cr.RuleSpec,
    headline: Optional[float] = None,
    gap: Optional[float] = None,
) -> cr.RuleComponents:
    """Компоненты правила с необязательными заменами headline/gap.

    Отличие от `cr.rule_components` — управляемый fallback для `rational`
    (консенсус → адаптивный headline) и сценарные сдвиги gap. Базовая
    спецификация с `headline=None`, `gap=None` совпадает с `cr.rule_components`.
    """
    if spec.e3_source == "consensus":
        e3_core = row.e3_core
        e3_headline = row.e3_headline_consensus
        if e3_headline is None:
            e3_headline = row.e3_headline_adaptive
    elif spec.e3_source == "m3":
        e3_core = row.e3_core_m3
        e3_headline = row.e3_headline_m3
    else:
        e3_core = row.e3_core
        e3_headline = row.e3_headline_adaptive
    if headline is not None:
        e3_headline = headline
    gap_value = row.ibk_gap if spec.gap_source == "ibk" else row.gdp_gap
    if gap is not None:
        gap_value = gap
    r_star = spec.r_star if spec.r_star is not None else row.r_star_trend
    missing = [
        name
        for name, value in (
            ("E3 core", e3_core),
            ("E3 headline", e3_headline),
            ("gap", gap_value),
            ("r*", r_star),
        )
        if value is None
    ]
    if missing:
        raise krm.DegenerateWindowError(
            f"{spec.name}: нет признака «{', '.join(missing)}» на "
            f"{row.decision.meeting_date.isoformat()}"
        )
    values = (e3_core, e3_headline, gap_value, r_star)
    if any(not math.isfinite(float(value)) for value in values):
        raise ValueError(f"{spec.name}: неконечный признак на {row.decision.meeting_date}")
    e3_mp = cr.E3_CORE_WEIGHT * float(e3_core) + cr.E3_HEADLINE_WEIGHT * float(e3_headline)
    rate_before = row.decision.rate_before
    target = cr.GAMMA1 * rate_before + (1.0 - cr.GAMMA1) * (
        float(r_star)
        + e3_mp
        + spec.gamma2 * (e3_mp - cr.TARGET_INFLATION)
        + cr.GAMMA3 * float(gap_value)
    )
    return cr.RuleComponents(
        e3_core=float(e3_core),
        e3_headline=float(e3_headline),
        e3_mp=e3_mp,
        gap=float(gap_value),
        r_star=float(r_star),
        target_level=target,
        step_bp=(target - rate_before) * 100.0,
    )


def scenario_steps(
    row: cr.CbrRow,
    budget_impulse_value: Optional[float],
    budget_z: float,
) -> list:
    """Пять шагов сценариев с fallback-метками.

    `budget` при отсутствии импульса и `rational` при отсутствии консенсуса
    используют базу и помечаются fallback'ом; молчаливая подмена одного
    сценария другим запрещена. Если базовый признак отсутствует, исключение
    `DegenerateWindowError` уходит вызывающему — фолд пропускается со счётчиком.
    """
    result = []
    for scenario in SCENARIOS:
        fallback = None
        if scenario.kind == "rational" and row.e3_headline_consensus is None:
            components = _components(row, cr.BASE_SPEC)
            fallback = "rational_fallback"
        elif scenario.kind == "budget":
            base = _components(row, cr.BASE_SPEC)
            if budget_impulse_value is None:
                components = base
                fallback = "budget_fallback"
            else:
                components = _components(
                    row, cr.BASE_SPEC, gap=base.gap + BUDGET_DELTA * budget_z
                )
        elif scenario.kind == "nml":
            base = _components(row, cr.BASE_SPEC)
            components = _components(row, cr.BASE_SPEC, gap=base.gap * NML_FACTOR)
        else:
            components = _components(row, scenario.spec)
        result.append(
            ScenarioStep(name=scenario.name, components=components, fallback=fallback)
        )
    return result


def _train_budget_stats(
    train: list, impulse_by_date: dict
) -> tuple:
    """Медиана/IQR фискального импульса обучающего окна; `None` не считается."""
    values = [
        impulse_by_date[row.decision.meeting_date]
        for row in train
        if impulse_by_date.get(row.decision.meeting_date) is not None
    ]
    if not values:
        return 0.0, 1.0
    array = np.asarray(values, dtype=float)
    median = float(np.median(array))
    q75, q25 = np.percentile(array, [75.0, 25.0])
    scale = float(q75 - q25)
    if scale <= 0.0:
        scale = float(np.std(array))
    if scale <= 0.0:
        scale = 1.0
    return median, scale


def _budget_z(value: Optional[float], median: float, scale: float) -> float:
    if value is None:
        return 0.0
    z = (value - median) / scale
    return min(BUDGET_Z_CLIP, max(-BUDGET_Z_CLIP, z))


# --- Веера --------------------------------------------------------------------


def blend_step(step: float, last_step: Optional[float], weight: float) -> float:
    """`w·step + (1−w)·last_step`; без прошлого шага — сам шаг."""
    if last_step is None:
        return step
    return weight * step + (1.0 - weight) * last_step


def build_fans(
    scenario_steps_list: list,
    emergency_steps: list,
    p_shock: float,
    weight: float,
    last_step: Optional[float],
    emergency_now: bool,
) -> dict:
    """Четыре веера одного фолда; веса нормированы, порядок фиксирован."""
    raw = tuple(
        (item.components.step_bp, 1.0 / len(scenario_steps_list))
        for item in scenario_steps_list
    )
    branch = p_shock > 0.0 and bool(emergency_steps)
    fans = {"fan_raw": raw}
    scenario_weight = (
        (1.0 - p_shock) / len(scenario_steps_list)
        if branch
        else 1.0 / len(scenario_steps_list)
    )
    scenario_points = tuple(
        (item.components.step_bp, scenario_weight)
        for item in scenario_steps_list
        if scenario_weight > 0.0
    )
    if branch:
        emergency_weight = p_shock / len(emergency_steps)
        emergency_points = tuple(
            (step, emergency_weight)
            for step in emergency_steps
            if emergency_weight > 0.0
        )
        fans["fan_shock"] = scenario_points + emergency_points
    else:
        fans["fan_shock"] = raw
    weighted_scenarios = tuple(
        (blend_step(item.components.step_bp, last_step, weight), scenario_weight)
        for item in scenario_steps_list
        if scenario_weight > 0.0
    )
    if branch:
        weighted_emergency = tuple(
            (blend_step(step, last_step, weight), p_shock / len(emergency_steps))
            for step in emergency_steps
            if p_shock > 0.0
        )
        fans["fan_shock_weighted"] = weighted_scenarios + weighted_emergency
    else:
        fans["fan_shock_weighted"] = weighted_scenarios
    if emergency_now and emergency_steps:
        fans["fan_emergency_ex_post"] = tuple(
            (step, 1.0 / len(emergency_steps)) for step in emergency_steps
        )
    else:
        fans["fan_emergency_ex_post"] = raw
    return fans


# --- Метрики ------------------------------------------------------------------


def weighted_quantile(points: list, weights: list, q: float) -> float:
    """Квантиль взвешенного эмпирического распределения.

    Каждая точка считается центром своего весового интервала
    (`cum_{i-1} + w_i/2`), между центрами — линейная интерполяция. Для двух
    равных точек медиана — их середина, для 5–6 точек это coarse-оценка,
    которую спека §6.1 называет явно.
    """
    if len(points) != len(weights) or not points:
        raise ValueError("квантиль: пустые точки или разная длина весов")
    total = sum(weights)
    if total <= 0.0 or not math.isfinite(total):
        raise ValueError("квантиль: сумма весов не положительна")
    pairs = sorted(zip(points, weights), key=lambda pair: pair[0])
    if q <= 0.0:
        return float(pairs[0][0])
    if q >= 1.0:
        return float(pairs[-1][0])
    positions = []
    cumulative = 0.0
    for point, weight in pairs:
        share = weight / total
        positions.append((cumulative + share / 2.0, float(point)))
        cumulative += share
    if q <= positions[0][0]:
        return positions[0][1]
    if q >= positions[-1][0]:
        return positions[-1][1]
    for index in range(1, len(positions)):
        left_position, left_point = positions[index - 1]
        right_position, right_point = positions[index]
        if q <= right_position:
            width = right_position - left_position
            if width <= 0.0:
                return right_point
            fraction = (q - left_position) / width
            return left_point + fraction * (right_point - left_point)
    return positions[-1][1]


def crps_weighted(points: list, weights: list, actual: float) -> float:
    """CRPS взвешенного эмпирического распределения (спека §6.2)."""
    if len(points) != len(weights) or not points:
        raise ValueError("CRPS: пустые точки или разная длина весов")
    total = sum(weights)
    if total <= 0.0:
        raise ValueError("CRPS: сумма весов не положительна")
    normalized = [weight / total for weight in weights]
    first = sum(
        weight * abs(float(point) - actual)
        for point, weight in zip(points, normalized)
    )
    second = 0.0
    for i, (point_i, weight_i) in enumerate(zip(points, normalized)):
        for point_j, weight_j in zip(points, normalized):
            second += weight_i * weight_j * abs(float(point_i) - float(point_j))
    return first - 0.5 * second


def class_probabilities(points: list, weights: list) -> tuple:
    """Масса веера в классах cut/hold/hike по границе ±12.5 б.п."""
    total = sum(weights)
    if total <= 0.0:
        raise ValueError("вероятности классов: сумма весов не положительна")
    cut = sum(
        weight
        for point, weight in zip(points, weights)
        if point < -cr.HOLD_BAND_BP
    )
    hike = sum(
        weight
        for point, weight in zip(points, weights)
        if point > cr.HOLD_BAND_BP
    )
    hold = total - cut - hike
    return (cut / total, hold / total, hike / total)


def _actual_class(step_bp: float) -> int:
    if step_bp < -cr.HOLD_BAND_BP:
        return 0
    if step_bp > cr.HOLD_BAND_BP:
        return 2
    return 1


def _brier_logloss(probs: tuple, actual_step_bp: float) -> tuple:
    actual = _actual_class(actual_step_bp)
    one_hot = [0.0, 0.0, 0.0]
    one_hot[actual] = 1.0
    brier = sum((probability - target) ** 2 for probability, target in zip(probs, one_hot))
    logloss = -math.log(max(probs[actual], 1e-12))
    return brier, logloss


def fan_metrics(prediction: FanPrediction) -> dict:
    """Метрики одного фолда: CRPS, coverage/ширина, медиана, классы."""
    points = [point for point, _ in prediction.steps]
    weights = [weight for _, weight in prediction.steps]
    coverage = {}
    width = {}
    for level in FAN_LEVELS:
        low = weighted_quantile(points, weights, (1.0 - level) / 2.0)
        high = weighted_quantile(points, weights, 1.0 - (1.0 - level) / 2.0)
        coverage[level] = bool(low <= prediction.actual_step_bp <= high)
        width[level] = high - low
    probs = class_probabilities(points, weights)
    brier, logloss = _brier_logloss(probs, prediction.actual_step_bp)
    median = weighted_quantile(points, weights, 0.5)
    return {
        "crps": crps_weighted(points, weights, prediction.actual_step_bp),
        "coverage": coverage,
        "width": width,
        "median": median,
        "probs": probs,
        "brier": brier,
        "log_loss": logloss,
        "error": median - prediction.actual_step_bp,
    }


def wilson_interval(successes: int, n: int, z: float = 1.96) -> tuple:
    """Wilson 95%-интервал доли; `n=0` — `(0, 1)`."""
    if n <= 0:
        return (0.0, 1.0)
    p = successes / n
    denominator = 1.0 + z * z / n
    centre = (p + z * z / (2.0 * n)) / denominator
    half = z * math.sqrt(p * (1.0 - p) / n + z * z / (4.0 * n * n)) / denominator
    return (max(0.0, centre - half), min(1.0, centre + half))


@dataclass(frozen=True)
class MeanDiff:
    """Блочный бутстрап разницы средних потерь (CRPS)."""

    mean_diff: float
    low: float
    high: float
    p_value: float


def block_bootstrap_mean_diff(
    loss_a: list,
    loss_b: list,
    block: int = BOOTSTRAP_BLOCK,
    n: int = BOOTSTRAP_RUNS,
    seed: int = BOOTSTRAP_SEED,
) -> MeanDiff:
    """95%-интервал и p-value разницы средних потерь (парные блоки)."""
    if len(loss_a) != len(loss_b) or not loss_a:
        raise ValueError("бутстрап: пустые или разные по длине ряды потерь")
    if block < 1 or n < 1:
        raise ValueError("бутстрап: block и n должны быть положительными")
    size = len(loss_a)
    blocks_per_sample = -(-size // block)
    generator = np.random.default_rng(seed)
    deltas = []
    for _ in range(n):
        indices = []
        for _ in range(blocks_per_sample):
            start = int(generator.integers(0, size))
            indices.extend((start + offset) % size for offset in range(block))
        del indices[size:]
        deltas.append(
            sum(loss_a[index] for index in indices) / size
            - sum(loss_b[index] for index in indices) / size
        )
    values = np.asarray(deltas)
    low = float(np.quantile(values, 0.025))
    high = float(np.quantile(values, 0.975))
    below = float(np.mean(values <= 0.0))
    above = float(np.mean(values >= 0.0))
    return MeanDiff(
        mean_diff=sum(loss_a) / size - sum(loss_b) / size,
        low=low,
        high=high,
        p_value=min(1.0, 2.0 * min(below, above)),
    )


# --- Walk-forward -------------------------------------------------------------


BENCHMARKS = {
    "hold": krm.predict_hold,
    "repeat": krm.predict_repeat,
    "ar1": krm.fit_ar1,
    "ordered_logit": krm.fit_ordered_logit,
    "climatology": krm.fit_climatology,
    "cbr_rule": lambda train, row: cr.fit_cbr_rule(train, row, cr.BASE_SPEC),
}


def run_walk_forward(
    rows: list,
    panel_by_date: dict,
    weight_by_date: dict,
    probability_by_date: dict,
    impulse_by_date: dict,
) -> tuple:
    """Все веера и бенчмарки на двух окнах; фолды выровнены по датам."""
    fans = []
    benchmarks = []
    skips = Counter()
    for window, rolling in (
        (WINDOW_EXPANDING, None),
        (krm.window_label(ROLLING_WINDOW), ROLLING_WINDOW),
    ):
        start = MIN_TRAIN if rolling is None else max(MIN_TRAIN, rolling)
        for index in range(start, len(rows)):
            train = rows[:index] if rolling is None else rows[index - rolling:index]
            row = rows[index]
            day = row.decision.meeting_date
            impulse = impulse_by_date.get(day)
            median, scale = _train_budget_stats(train, impulse_by_date)
            budget_z = _budget_z(impulse, median, scale)
            try:
                steps = scenario_steps(row, impulse, budget_z)
            except krm.DegenerateWindowError:
                skips["fan_fold"] += 1
                continue
            emergency_steps = [
                float(item.decision.step_bp)
                for item in train
                if panel_by_date[item.decision.meeting_date].emergency
            ]
            p_shock = probability_by_date.get(day, 0.0)
            weight = weight_by_date.get(day, 1.0)
            last_step = float(train[-1].decision.step_bp) if train else None
            emergency_now = bool(panel_by_date[day].emergency)
            fan_map = build_fans(
                steps,
                emergency_steps,
                p_shock,
                weight,
                last_step,
                emergency_now,
            )
            scenario_fallbacks = tuple(item.fallback or "" for item in steps)
            emergency_fallbacks = ("",) * len(emergency_steps)
            scenario_labels = tuple(item.name for item in steps)
            emergency_labels = tuple(
                f"emergency_{index}" for index in range(len(emergency_steps))
            )
            branch = p_shock > 0.0 and bool(emergency_steps)
            labels = {
                "fan_raw": scenario_labels,
                "fan_shock": scenario_labels + emergency_labels if branch else scenario_labels,
                "fan_shock_weighted": scenario_labels + emergency_labels if branch else scenario_labels,
                "fan_emergency_ex_post": (
                    emergency_labels
                    if emergency_now and emergency_steps
                    else scenario_labels
                ),
            }
            fallbacks_by_fan = {
                "fan_raw": scenario_fallbacks,
                "fan_shock": scenario_fallbacks + emergency_fallbacks if branch else scenario_fallbacks,
                "fan_shock_weighted": scenario_fallbacks + emergency_fallbacks if branch else scenario_fallbacks,
                "fan_emergency_ex_post": (
                    emergency_fallbacks
                    if emergency_now and emergency_steps
                    else scenario_fallbacks
                ),
            }
            for fan_name in FAN_ORDER:
                fans.append(
                    FanPrediction(
                        meeting_date=day,
                        window=window,
                        fan=fan_name,
                        steps=fan_map[fan_name],
                        labels=labels[fan_name],
                        actual_step_bp=float(row.decision.step_bp),
                        emergency=emergency_now,
                        p_shock=p_shock,
                        weight=weight,
                        fallbacks=fallbacks_by_fan[fan_name],
                    )
                )
            for method in BENCHMARK_ORDER:
                try:
                    forecast = BENCHMARKS[method](train, row)
                except krm.DegenerateWindowError:
                    skips[f"bench_{method}"] += 1
                    continue
                benchmarks.append(
                    BenchmarkPrediction(
                        meeting_date=day,
                        window=window,
                        method=method,
                        step_bp=float(forecast.step_bp),
                        actual_step_bp=float(row.decision.step_bp),
                        probs=forecast.probs,
                    )
                )
    return fans, benchmarks, skips


# --- Агрегация ----------------------------------------------------------------


def _group(predictions: list, key) -> dict:
    grouped = defaultdict(list)
    for prediction in predictions:
        grouped[key(prediction)].append(prediction)
    return dict(grouped)


def _episode(day: date) -> str:
    for name, start, end in EPISODES:
        if start <= day <= end:
            return name
    return "прочее"


def summarize_fans(fans: list) -> dict:
    """Сводка «веер × окно»: CRPS, coverage, ширина, классы, медиана."""
    result = {}
    grouped = _group(fans, lambda item: (item.fan, item.window))
    for (fan, window), group in grouped.items():
        metrics = [fan_metrics(item) for item in group]
        coverage = {}
        width = {}
        for level in FAN_LEVELS:
            covered = sum(1 for metric in metrics if metric["coverage"][level])
            coverage[level] = {
                "value": covered / len(group),
                "interval": wilson_interval(covered, len(group)),
                "n": len(group),
            }
            width[level] = sum(metric["width"][level] for metric in metrics) / len(group)
        result[(fan, window)] = {
            "n": len(group),
            "crps": sum(metric["crps"] for metric in metrics) / len(group),
            "coverage": coverage,
            "width": width,
            "brier": sum(metric["brier"] for metric in metrics) / len(group),
            "log_loss": sum(metric["log_loss"] for metric in metrics) / len(group),
            "rmse": eu.rmse([metric["error"] for metric in metrics]),
            "mae": eu.mae([metric["error"] for metric in metrics]),
            "median": [metric["median"] for metric in metrics],
        }
    return result


def summarize_benchmarks(benchmarks: list) -> dict:
    """Сводка бенчмарков; вероятностные метрики — только при наличии probs."""
    result = {}
    grouped = _group(benchmarks, lambda item: (item.method, item.window))
    for (method, window), group in grouped.items():
        errors = [item.step_bp - item.actual_step_bp for item in group]
        probs = [item.probs for item in group if item.probs is not None]
        brier = None
        log_loss = None
        if probs:
            values = [
                _brier_logloss(item.probs, item.actual_step_bp)
                for item in group
                if item.probs is not None
            ]
            brier = sum(value[0] for value in values) / len(values)
            log_loss = sum(value[1] for value in values) / len(values)
        result[(method, window)] = {
            "n": len(group),
            "rmse": eu.rmse(errors),
            "mae": eu.mae(errors),
            "brier": brier,
            "log_loss": log_loss,
            "error": errors,
            "step": [item.step_bp for item in group],
        }
    return result


def summarize_episodes(fans: list) -> dict:
    result = {}
    grouped = _group(fans, lambda item: (item.fan, item.window, _episode(item.meeting_date)))
    for key, group in grouped.items():
        metrics = [fan_metrics(item) for item in group]
        covered = sum(
            1 for metric in metrics if metric["coverage"][PRIMARY_LEVEL]
        )
        result[key] = {
            "n": len(group),
            "crps": sum(metric["crps"] for metric in metrics) / len(group),
            "coverage80": covered / len(group),
            "width80": sum(metric["width"][PRIMARY_LEVEL] for metric in metrics) / len(group),
        }
    return result


def compare_fan_to_benchmark(
    fan_group: list, benchmark_group: list, method: str
) -> Optional[dict]:
    """CRPS-разница веера и точечного бенчмарка + DM по медианам."""
    fan_by_date = {item.meeting_date: item for item in fan_group}
    bench_by_date = {item.meeting_date: item for item in benchmark_group}
    dates = sorted(set(fan_by_date) & set(bench_by_date))
    if len(dates) < 3:
        return None
    fan_losses = []
    bench_losses = []
    fan_errors = []
    bench_errors = []
    for day in dates:
        fan = fan_by_date[day]
        bench = bench_by_date[day]
        fan_metrics_value = fan_metrics(fan)
        fan_losses.append(fan_metrics_value["crps"])
        bench_losses.append(abs(bench.step_bp - bench.actual_step_bp))
        fan_errors.append(fan_metrics_value["error"])
        bench_errors.append(bench.step_bp - bench.actual_step_bp)
    bootstrap = block_bootstrap_mean_diff(fan_losses, bench_losses)
    dm_stat, dm_p = eu.diebold_mariano(fan_errors, bench_errors, h=1)
    return {
        "method": method,
        "n": len(dates),
        "crps_diff": bootstrap.mean_diff,
        "crps_low": bootstrap.low,
        "crps_high": bootstrap.high,
        "crps_p": bootstrap.p_value,
        "dm_stat": dm_stat,
        "dm_p": dm_p,
        "fan_rmse": eu.rmse(fan_errors),
        "bench_rmse": eu.rmse(bench_errors),
        "fan_mae": eu.mae(fan_errors),
        "bench_mae": eu.mae(bench_errors),
    }


def compare_fans(fan_a: list, fan_b: list) -> Optional[dict]:
    """CRPS-разница двух вееров + DM по медианам на общих датах."""
    a_by_date = {item.meeting_date: item for item in fan_a}
    b_by_date = {item.meeting_date: item for item in fan_b}
    dates = sorted(set(a_by_date) & set(b_by_date))
    if len(dates) < 3:
        return None
    a_losses = []
    b_losses = []
    a_errors = []
    b_errors = []
    for day in dates:
        a_metric = fan_metrics(a_by_date[day])
        b_metric = fan_metrics(b_by_date[day])
        a_losses.append(a_metric["crps"])
        b_losses.append(b_metric["crps"])
        a_errors.append(a_metric["error"])
        b_errors.append(b_metric["error"])
    bootstrap = block_bootstrap_mean_diff(a_losses, b_losses)
    dm_stat, dm_p = eu.diebold_mariano(a_errors, b_errors, h=1)
    return {
        "n": len(dates),
        "crps_diff": bootstrap.mean_diff,
        "crps_low": bootstrap.low,
        "crps_high": bootstrap.high,
        "crps_p": bootstrap.p_value,
        "dm_stat": dm_stat,
        "dm_p": dm_p,
    }


# --- Решение ------------------------------------------------------------------


def decide(summary: dict, comparisons: dict) -> tuple:
    """Предзарегистрированное правило решения (спека §7)."""
    primary = summary.get((PRIMARY_FAN, PRIMARY_WINDOW))
    raw = summary.get(("fan_raw", PRIMARY_WINDOW))
    if primary is None or raw is None:
        return "reject", "нет фолдов primary или fan_raw на expanding"
    raw_compare = comparisons.get("fan_shock_vs_fan_raw")
    crps_better_raw = bool(
        raw_compare
        and raw_compare["crps_diff"] < 0.0
        and raw_compare["crps_high"] < 0.0
    )
    benchmark_ok = all(
        (
            comparisons.get(f"fan_shock_vs_{method}") is not None
            and comparisons[f"fan_shock_vs_{method}"]["crps_diff"] < 0.0
            and comparisons[f"fan_shock_vs_{method}"]["crps_high"] < 0.0
        )
        for method in ("hold", "ar1", "ordered_logit")
    )
    coverage = primary["coverage"][PRIMARY_LEVEL]
    coverage_ok = (
        0.70 <= coverage["value"] <= 0.90
        and coverage["interval"][0] <= PRIMARY_LEVEL <= coverage["interval"][1]
    )
    ordered = comparisons.get("fan_shock_vs_ordered_logit")
    rmse_ok = ordered is None or primary["rmse"] <= ordered["bench_rmse"] * 1.10
    reason = (
        f"CRPS {primary['crps']:.2f} (raw {raw['crps']:.2f}); "
        f"coverage80 {coverage['value']:.3f} "
        f"[{coverage['interval'][0]:.3f}; {coverage['interval'][1]:.3f}]; "
        f"RMSE {primary['rmse']:.2f} vs ordered "
        f"{ordered['bench_rmse']:.2f}" if ordered else
        f"CRPS {primary['crps']:.2f}; coverage80 {coverage['value']:.3f}; ordered нет"
    )
    if crps_better_raw and benchmark_ok and coverage_ok and rmse_ok:
        return "promote", reason
    if crps_better_raw or benchmark_ok or coverage_ok:
        return "iterate", reason
    return "reject", reason


# --- Артефакты ----------------------------------------------------------------


def write_predictions(path: Path, fans: list) -> None:
    columns = (
        "meeting_date",
        "window",
        "fan",
        "scenario",
        "step_bp",
        "weight",
        "actual_step_bp",
        "emergency",
        "shock_prob",
        "w",
        "fallback",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for item in fans:
            if not (
                len(item.labels) == len(item.steps) == len(item.fallbacks)
            ):
                raise ValueError(
                    f"CSV: у веера {item.fan} длины labels/steps/fallbacks "
                    f"не совпадают: {len(item.labels)}/{len(item.steps)}/{len(item.fallbacks)}"
                )
            for label, (step, weight), fallback in zip(
                item.labels, item.steps, item.fallbacks
            ):
                writer.writerow(
                    [
                        item.meeting_date.isoformat(),
                        item.window,
                        item.fan,
                        label,
                        f"{step:.6f}",
                        f"{weight:.6f}",
                        f"{item.actual_step_bp:.6f}",
                        int(item.emergency),
                        f"{item.p_shock:.6f}",
                        f"{item.weight:.6f}",
                        fallback,
                    ]
                )


def write_diagnostics(path: Path, rows: list, impulse_by_date: dict) -> None:
    columns = (
        "meeting_date",
        "scenario",
        "e3_core",
        "e3_headline",
        "e3_mp",
        "gap",
        "r_star",
        "step_bp",
        "fallback",
        "budget_impulse",
        "budget_z",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for index, row in enumerate(rows):
            impulse = impulse_by_date.get(row.decision.meeting_date)
            median, scale = _train_budget_stats(rows[:index], impulse_by_date)
            budget_z = _budget_z(impulse, median, scale)
            try:
                steps = scenario_steps(row, impulse, budget_z)
            except krm.DegenerateWindowError:
                continue
            for item in steps:
                components = item.components
                writer.writerow(
                    [
                        row.decision.meeting_date.isoformat(),
                        item.name,
                        f"{components.e3_core:.6f}",
                        f"{components.e3_headline:.6f}",
                        f"{components.e3_mp:.6f}",
                        f"{components.gap:.6f}",
                        f"{components.r_star:.6f}",
                        f"{components.step_bp:.6f}",
                        item.fallback or "",
                        "" if impulse is None else f"{impulse:.6f}",
                        f"{budget_z:.6f}",
                    ]
                )


def write_forecast(path: Path, current: Optional[dict]) -> None:
    if current is None:
        if path.exists():
            path.unlink()
        return
    path.write_text(
        json.dumps(current, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def _current_forecast(
    rows: list,
    key_rate: list,
    cpd_core: list,
    cpd_all: list,
    ibk: list,
    gdp: list,
    consensus: list,
    budget: BudgetPoint,
    panel: list,
    channels: dict,
    shock_meetings: list,
    impulse_by_date: dict,
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
    impulse = budget_impulse(budget, as_of)
    median, scale = _train_budget_stats(rows, impulse_by_date)
    budget_z = _budget_z(impulse, median, scale)
    try:
        steps = scenario_steps(current_row, impulse, budget_z)
    except krm.DegenerateWindowError as error:
        return {
            "as_of": as_of.isoformat(),
            "meeting_date": meeting.isoformat(),
            "skipped": str(error),
            "note": "информационно; в forecast_log не записывается",
        }
    fan_map = build_fans(steps, [], 0.0, 1.0, None, False)
    next_forecast = sc.next_meeting_forecast(
        panel, shock_meetings, key_rate, channels, as_of
    )
    weight = next_forecast.weight if next_forecast is not None else 1.0
    p_shock = (
        min(P_SHOCK_MAX, max(0.0, next_forecast.shock_prob))
        if next_forecast is not None and next_forecast.shock_prob is not None
        else 0.0
    )
    return {
        "as_of": as_of.isoformat(),
        "meeting_date": meeting.isoformat(),
        "rate_before": current_row.decision.rate_before,
        "budget_impulse": impulse,
        "p_shock": p_shock,
        "weight": weight,
        "scenarios": [
            {
                "name": item.name,
                "step_bp": item.components.step_bp,
                "e3_mp": item.components.e3_mp,
                "gap": item.components.gap,
                "r_star": item.components.r_star,
                "fallback": item.fallback,
            }
            for item in steps
        ],
        "fan_raw": [
            {"step_bp": step, "weight": weight_} for step, weight_ in fan_map["fan_raw"]
        ],
        "note": "информационно; в forecast_log не записывается до подтверждения",
    }


# --- Отчёт --------------------------------------------------------------------


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
    coverage_info: dict,
    summary: dict,
    benchmarks: dict,
    comparisons: dict,
    episodes: dict,
    skips: Counter,
    fallback_counts: Counter,
    decision: str,
    reason: str,
    current: Optional[dict],
) -> str:
    lines = [
        "# Сценарный веер ставки с shock-aware слоем",
        "",
        f"**Дата:** {as_of.isoformat()}",
        f"**Пункт:** `key-rate/phase-4-cbr-scenario-fan` (issue #272), Task 1",
        f"**Спека:** [{SPEC_PATH}](../superpowers/specs/2026-10-20-cbr-scenario-fan-design.md)",
        f"**План:** [{PLAN_PATH}](../superpowers/plans/2026-10-20-cbr-scenario-fan.md)",
        "",
        "## 1. Выборка",
        "",
        "| Показатель | Значение |",
        "|---|---:|",
        f"| заседаний | {len(rows)} |",
        f"| первое / последнее | {rows[0].decision.meeting_date.isoformat()} / {rows[-1].decision.meeting_date.isoformat()} |",
        f"| пропущено фолдов веера | {skips.get('fan_fold', 0)} |",
        f"| пропущено бенчмарков | {sum(value for key, value in skips.items() if key.startswith('bench_'))} |",
        "",
        "## 2. Сценарии",
        "",
        "| Сценарий | fallback |",
        "|---|---|",
    ]
    fallback_for = {"rational": "rational_fallback", "budget": "budget_fallback"}
    for scenario in SCENARIOS:
        lines.append(
            f"| {scenario.name} | {fallback_counts.get(fallback_for.get(scenario.name, ''), 0)} |"
        )
    lines.extend(
        [
            "",
            "Сценарии — reduced-form прокси калибровок КПМ; `budget` и `no_market_loans` меняют только gap (спека §4).",
            "",
            "## 3. Веер × окно",
            "",
            "| веер | окно | n | CRPS | coverage 50 | coverage 80 | coverage 90 | ширина 80 | Brier | log loss | RMSE | MAE |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for fan in FAN_ORDER:
        for window in WINDOWS:
            item = summary.get((fan, window))
            if item is None:
                continue
            cov = item["coverage"]
            lines.append(
                f"| {fan} | {window} | {item['n']} | {_fmt(item['crps'], 3)} "
                f"| {_fmt(cov[0.50]['value'], 3)} | {_fmt(cov[0.80]['value'], 3)} "
                f"| {_fmt(cov[0.90]['value'], 3)} | {_fmt(item['width'][0.80], 2)} "
                f"| {_fmt(item['brier'], 4)} | {_fmt(item['log_loss'], 4)} "
                f"| {_fmt(item['rmse'])} | {_fmt(item['mae'])} |"
            )
    lines.extend(
        [
            "",
            "## 4. Сравнение с бенчмарками",
            "",
            "| пара | n | ΔCRPS | 95 % CI | p (бутстр.) | DM p | RMSE веера | RMSE бенч. |",
            "|---|---:|---:|---|---:|---:|---:|---:|",
        ]
    )
    for name, item in comparisons.items():
        if item is None:
            lines.append(f"| {name} | — | — | — | — | — | — | — |")
            continue
        lines.append(
            f"| {name} | {item.get('n', '—')} | {_fmt(item.get('crps_diff'), 3)} "
            f"| [{_fmt(item.get('crps_low'), 3)}; {_fmt(item.get('crps_high'), 3)}] "
            f"| {_fmt_p(item.get('crps_p'))} | {_fmt_p(item.get('dm_p'))} "
            f"| {_fmt(item.get('fan_rmse'))} | {_fmt(item.get('bench_rmse'))} |"
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
    for (fan, window, episode), item in sorted(episodes.items(), key=lambda pair: (pair[0][0], pair[0][1], pair[0][2])):
        lines.append(
            f"| {fan} | {window} | {episode} | {item['n']} | {_fmt(item['crps'], 3)} "
            f"| {_fmt(item['coverage80'], 3)} | {_fmt(item['width80'], 2)} |"
        )
    lines.extend(
        [
            "",
            "Эпизод 2014-12…2015 в тестовые фолды не попадает: первые 24 заседания — обучающее окно expanding, "
            "и шок 2014-12/2015 используется только в `train`.",
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
            f"Заседание **{current['meeting_date']}**, ставка до {_fmt(current['rate_before'])} %, "
            f"фискальный импульс {_fmt(current['budget_impulse'], 3)}, "
            f"`p_t` {_fmt(current['p_shock'], 3)}, `w_t` {_fmt(current['weight'], 3)}."
        )
        lines.extend(
            [
                "",
                "| сценарий | шаг, б.п. | E3π_MP | gap | r* | fallback |",
                "|---|---:|---:|---:|---:|---|",
            ]
        )
        for item in current["scenarios"]:
            lines.append(
                f"| {item['name']} | {_fmt(item['step_bp'])} | {_fmt(item['e3_mp'])} "
                f"| {_fmt(item['gap'])} | {_fmt(item['r_star'])} | {item['fallback'] or '—'} |"
            )
    lines.extend(
        [
            "",
            "## 7. Вердикт",
            "",
            f"**`{decision}`** — {reason}.",
            "",
            "Правило решения — спека §7. Primary: `fan_shock`, expanding, CRPS + coverage 80 %; "
            "`fan_shock_weighted` — вторичная гипотеза, не независимое подтверждение.",
            "",
            "## 8. Ограничения",
            "",
            "- 5–6 точек на фолд: интервалы coarse, CRPS учитывает дискретность.",
            "- 6 шоковых меток; `p_t` описательный, клип 0.25.",
            "- `budget`/`no_market_loans` — reduced-form прокси, не QPM.",
            "- As-of приближение, не PIT: CPD/ВВП/Минфин без винтажей.",
            "- §6.1 #283 — post-hoc на той же выборке; подтверждение не независимо.",
            "- В `forecast_log` не пишется; Go/DDL/витрины/гранты не менялись.",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


# --- Журнал эксперимента -------------------------------------------------------


def _log_run(
    rows: list,
    fans: list,
    benchmarks: list,
    summary: dict,
    decision: str,
    as_of: date,
    artifacts: list,
    enabled: Optional[bool] = None,
) -> str:
    config = {
        "scenarios": [scenario.name for scenario in SCENARIOS],
        "budget_a7_base": BUDGET_A7_BASE,
        "budget_a7_scenario": BUDGET_A7_SCENARIO,
        "nml_a3_base": NML_A3_BASE,
        "nml_a3_scenario": NML_A3_SCENARIO,
        "p_shock_max": P_SHOCK_MAX,
        "fan_levels": list(FAN_LEVELS),
        "primary_fan": PRIMARY_FAN,
        "primary_window": PRIMARY_WINDOW,
        "bootstrap_block": BOOTSTRAP_BLOCK,
        "bootstrap_runs": BOOTSTRAP_RUNS,
        "as_of": as_of.isoformat(),
    }
    sample_meta = {
        "rows": len(rows),
        "first": rows[0].decision.meeting_date.isoformat() if rows else None,
        "last": rows[-1].decision.meeting_date.isoformat() if rows else None,
        "fans": len(fans),
        "benchmarks": len(benchmarks),
    }
    sample = [
        {
            "meeting_date": row.decision.meeting_date.isoformat(),
            "rate_before": row.decision.rate_before,
            "step_bp": row.decision.step_bp,
            "e3_core": row.e3_core,
            "e3_headline_adaptive": row.e3_headline_adaptive,
            "e3_headline_consensus": row.e3_headline_consensus,
            "gdp_gap": row.gdp_gap,
            "ibk_gap": row.ibk_gap,
        }
        for row in rows
    ]
    logger = mlx.start_run(
        experiment="key_rate_scenario_fan",
        run_name="scenario_fan__rate__start24",
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
                f"{fan}__{window}": {
                    "crps": item["crps"],
                    "coverage80": item["coverage"][0.80]["value"],
                    "rmse": item["rmse"],
                    "n": item["n"],
                }
                for (fan, window), item in summary.items()
            },
        }
    )
    logger.log_predictions(
        [
            mlx.PredictionRow(
                model=f"{item.fan}__scenario_{index}",
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
            for index, (step, weight) in enumerate(item.steps)
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
    coverage_info = cr.coverage(rows)
    if not rows:
        print("выборка пуста — веер не считается", file=sys.stderr)
        return 1

    panel, shock_meetings, shock_key_rate, channels = load_shock_panel()
    panel_by_date = align_rows(rows, panel)
    weight_by_date, probability_by_date = shock_arrays(panel)
    budget = load_budget()
    impulse_by_date = {
        row.decision.meeting_date: budget_impulse(budget, row.decision.meeting_date)
        for row in rows
    }

    print(
        f"выборка: решений {len(decisions)}, строк {len(rows)}, "
        f"исключено {len(excluded)}, без ставки {len(missing_rate)}, "
        f"без базовых признаков {len(missing_features)}"
    )
    fans, benchmarks, skips = run_walk_forward(
        rows,
        panel_by_date,
        weight_by_date,
        probability_by_date,
        impulse_by_date,
    )
    summary = summarize_fans(fans)
    benchmark_summary = summarize_benchmarks(benchmarks)
    episodes = summarize_episodes(fans)
    fallback_counts = Counter(
        fallback
        for item in fans
        if item.fan == "fan_raw"
        for fallback in item.fallbacks
        if fallback
    )
    comparisons = {}
    for window in WINDOWS:
        raw_group = [item for item in fans if item.fan == "fan_raw" and item.window == window]
        for fan_name in FAN_ORDER:
            fan_group = [item for item in fans if item.fan == fan_name and item.window == window]
            if fan_name != "fan_raw":
                comparisons[f"{fan_name}_vs_fan_raw__{window}"] = compare_fans(
                    fan_group, raw_group
                )
            for method in ("hold", "ar1", "ordered_logit"):
                bench_group = [
                    item
                    for item in benchmarks
                    if item.method == method and item.window == window
                ]
                comparisons[f"{fan_name}_vs_{method}__{window}"] = compare_fan_to_benchmark(
                    fan_group, bench_group, method
                )
    decision, reason = decide(
        summary,
        {
            "fan_shock_vs_fan_raw": comparisons.get(f"fan_shock_vs_fan_raw__{PRIMARY_WINDOW}"),
            **{
                f"fan_shock_vs_{method}": comparisons.get(
                    f"fan_shock_vs_{method}__{PRIMARY_WINDOW}"
                )
                for method in ("hold", "ar1", "ordered_logit")
            },
        },
    )
    current = _current_forecast(
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
            coverage_info,
            summary,
            benchmark_summary,
            comparisons,
            episodes,
            skips,
            fallback_counts,
            decision,
            reason,
            current,
        ),
        encoding="utf-8",
    )
    write_predictions(predictions_path, fans)
    write_diagnostics(diagnostics_path, rows, impulse_by_date)
    write_forecast(forecast_path, current)

    selected = krm.window_label(ROLLING_WINDOW) if rolling else WINDOW_EXPANDING
    print(f"окно консольной сводки: {selected}")
    for fan in FAN_ORDER:
        item = summary.get((fan, selected))
        if item is None:
            print(f"  {fan:<24} фолдов 0")
            continue
        print(
            f"  {fan:<24} n={item['n']:>3}  CRPS {item['crps']:>7.3f}  "
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
            benchmarks,
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
        description="Сценарный веер ставки с shock-aware слоем (Task 1 #272)."
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
