#!/usr/bin/env python3
"""`cbr_rule` — калиброванное правило ДДКП ЦБ как отдельный трек модели ставки.

Спека: `docs/superpowers/specs/2026-10-20-cbr-rule-design.md`
План: `docs/superpowers/plans/2026-10-20-cbr-rule.md`
Issue: #268 (`key-rate/phase-4-cbr-reaction-function`).

Скрипт НЕ меняет `key_rate_model.py`: выборка решений, as-of слой, walk-forward,
бенчмарки и метрики берутся из него импортом. Новое — только признаки правила
(E3 core/headline, HP-разрыв ВВП/ИБК, r*), само правило и вероятности классов.

Правило (ДДКП, research §7 P0.1):

    i_t = γ1·i_{t−1} + (1−γ1)·(r* + E3π_MP + γ2·(E3π_MP − 4) + γ3·gap)
    E3π_MP = 0.75·E3(core) + 0.25·E3(headline)

`i_{t−1}` — фактическая ставка накануне заседания (известна), поэтому это
одношаговое инерционное правило от фактической ставки, а не симуляция пути.

**В `forecast_log` скрипт не пишет** и публикационного payload не готовит:
до подтверждения владельцем трек живёт только в отчёте и `ML_LOG`-журнале.

Запуск — только через `make` (переменные окружения подключает Makefile):

    make key-rate-cbr-rule
    ML_LOG=1 make key-rate-cbr-rule
    make key-rate-cbr-rule ARGS="--as-of 2026-10-20"
    make key-rate-cbr-rule-test
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
import re
import sys
from calendar import monthrange
from dataclasses import dataclass, replace
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from scipy.stats import norm
from statsmodels.tsa.filters.hp_filter import hpfilter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import eval_utils as eu  # noqa: E402
import key_rate_model as krm  # noqa: E402
import ml_experiment as mlx  # noqa: E402

log = logging.getLogger("cbr_rule")

# --- Константы ----------------------------------------------------------------

ONE_DAY = timedelta(days=1)

CPD_CORE = "Базовый ИПЦ"
CPD_ALL = "Все товары и услуги"
SURVEY_SOURCE = "cbr_survey"
IBK_SERIES = "business_climate_sa"
GDP_SERIES = "10 Валовой внутренний продукт"

# --- Спека §3: предзарегистрированные параметры -------------------------------

GAMMA1 = 0.75
GAMMA3 = 0.5
TARGET_INFLATION = 4.0
R_STAR_BASE = 3.0
R_STAR_GRID = (2.0, 3.0, 4.0)
GAMMA2_GRID = (1.5, 1.9)
E3_CORE_WEIGHT = 0.75
E3_HEADLINE_WEIGHT = 0.25
MIN_TRAIN = krm.MIN_TRAIN  # 24
ROLLING_WINDOW = krm.ROLLING_WINDOW  # 40
MIN_GDP_QUARTERS = 8
MIN_IBK_MONTHS = 24
MIN_RULE_RESIDUALS = 8
MIN_R_TREND = 8
R_TREND_WINDOW = 20
PROB_FLOOR_BP = 25.0
HOLD_BAND_BP = 12.5
PROB_EPS = 1e-6
FINITE_EPS = 1e-12

SPEC_PATH = "docs/superpowers/specs/2026-10-20-cbr-rule-design.md"
PLAN_PATH = "docs/superpowers/plans/2026-10-20-cbr-rule.md"
REPORT_NAME = "key_rate_cbr_rule_report.md"
PREDICTIONS_NAME = "key_rate_cbr_rule_predictions.csv"
DIAGNOSTICS_NAME = "key_rate_cbr_rule_diagnostics.csv"
FORECAST_NAME = "key_rate_cbr_rule_forecast.json"
ARTIFACT_NAMES = (REPORT_NAME, PREDICTIONS_NAME, DIAGNOSTICS_NAME, FORECAST_NAME)

WINDOW_EXPANDING = krm.WINDOW_EXPANDING
WINDOWS = (WINDOW_EXPANDING, krm.window_label(ROLLING_WINDOW))

METHOD_ORDER = (
    "hold",
    "repeat",
    "ar1",
    "ordered_logit",
    "climatology",
    "cbr_rule",
    "cbr_rule_hard",
    "cbr_rule_ibk",
    "cbr_rule_m3",
    "cbr_rule_consensus",
    "cbr_rule_rtrend",
)
PROBABILISTIC_METHODS = (
    "ordered_logit",
    "climatology",
    "cbr_rule",
    "cbr_rule_hard",
    "cbr_rule_ibk",
    "cbr_rule_m3",
    "cbr_rule_consensus",
    "cbr_rule_rtrend",
)
BENCHMARK_METHODS = ("hold", "repeat", "ar1")

CPD_SQL = f"""
SELECT date, name, value
FROM cbr_indicators_cpd FINAL
WHERE name IN ('{CPD_CORE}', '{CPD_ALL}')
ORDER BY date, name
FORMAT JSON
"""

GDP_SQL = f"""
SELECT date, vvp AS value
FROM rosstat_vvp_kvartal FINAL
WHERE name = '{GDP_SERIES}'
ORDER BY date
FORMAT JSON
"""

CPI_CONSENSUS_SQL = """
SELECT date, series, value
FROM v_cbr_mo_br
WHERE series LIKE 'cpi_dec_%_median' OR series LIKE 'cpi_avg_%_median'
ORDER BY date, series
FORMAT JSON
"""


# --- Типы ----------------------------------------------------------------------


@dataclass(frozen=True)
class RuleSpec:
    """Один предзарегистрированный вариант правила (спека §4)."""

    name: str
    gamma2: float = GAMMA2_GRID[0]
    gap_source: str = "gdp"  # "gdp" | "ibk"
    e3_source: str = "adaptive"  # "adaptive" (12m y/y) | "m3" | "consensus"
    r_star: Optional[float] = R_STAR_BASE  # None — тренд из row.r_star_trend


BASE_SPEC = RuleSpec("cbr_rule")
VARIANTS = (
    BASE_SPEC,
    RuleSpec("cbr_rule_hard", gamma2=1.9),
    RuleSpec("cbr_rule_ibk", gap_source="ibk"),
    RuleSpec("cbr_rule_m3", e3_source="m3"),
    RuleSpec("cbr_rule_consensus", e3_source="consensus"),
    RuleSpec("cbr_rule_rtrend", r_star=None),
)


@dataclass(frozen=True)
class CbrRow:
    """Строка выборки: решение, признаки v0 и новые признаки правила.

    `pi_saar`/`gap` — признаки существующей модели (`ordered_logit`), `gap` —
    ИБК SA. `e3_core`/`e3_headline_adaptive` — 12-месячный y/y (база §3.1);
    `e3_core_m3`/`e3_headline_m3` — 3-месячный annualized (robustness
    `cbr_rule_m3`); `e3_headline_consensus` — прогноз из макроопроса.
    `None` означает «на дату заседания недоступно», а не ноль.
    """

    decision: krm.Decision
    pi_saar: float
    gap: float
    e3_core: Optional[float]
    e3_headline_adaptive: Optional[float]
    e3_headline_consensus: Optional[float]
    gdp_gap: Optional[float]
    ibk_gap: Optional[float]
    r_star_trend: Optional[float]
    e3_core_m3: Optional[float] = None
    e3_headline_m3: Optional[float] = None

    @property
    def x(self) -> list:
        """Вектор регрессоров v0 для `ordered_logit`."""
        return [self.decision.rate_before, self.pi_saar, self.gap]


@dataclass(frozen=True)
class CpiConsensus:
    """Волна макроопроса по ИПЦ: `cpi_(dec|avg)_<год>_median`."""

    wave_date: date
    available_from: date
    year: int
    kind: str  # "dec" | "avg"
    median: float


@dataclass(frozen=True)
class RuleComponents:
    """Разложение прогноза правила на компоненты — для отчёта и CSV."""

    e3_core: float
    e3_headline: float
    e3_mp: float
    gap: float
    r_star: float
    target_level: float
    step_bp: float


# --- As-of и преобразования ----------------------------------------------------


def add_months(day: date, months: int) -> date:
    """Дата через `months` календарных месяцев (день обрезается по месяцу)."""
    month = day.month - 1 + months
    year = day.year + month // 12
    month = month % 12 + 1
    return date(year, month, min(day.day, monthrange(year, month)[1]))


def _month_end(day: date) -> date:
    """Последний день месяца, в котором лежит `day`."""
    return date(day.year, day.month, monthrange(day.year, day.month)[1])


def available_from_gdp(period_date: date) -> date:
    """Доступность квартального ВВП: конец квартала + 3 месяца.

    `period_date` — дата точки в `rosstat_vvp_kvartal`: первое число месяца,
    следующего за отчётным кварталом (I квартал → 1 апреля). Первая оценка
    квартального ВВП выходит примерно через 2,5 месяца после конца квартала;
    +3 месяца — консервативное правило, исключающее look-ahead.
    """
    quarter_end = period_date - ONE_DAY
    return _month_end(add_months(quarter_end, 3))


def asof_window(points: list, available, asof_date: date, count: int) -> list:
    """Последние `count` доступных точек ряда (по возрастанию даты).

    Точка участвует, только если `available(period) <= asof_date` — та же
    включительная граница, что у `key_rate_model.asof`. Меньше `count` точек —
    возвращаются все доступные: вызывающий решает, достаточно ли их.
    """
    if count < 1:
        raise ValueError(f"asof_window: count={count} — меньше одного")
    candidates = [
        (period, value)
        for period, value in points
        if available(period) <= asof_date
    ]
    candidates.sort(key=lambda point: point[0])
    return [value for _, value in candidates[-count:]]


def annualize_trailing(values: list) -> Optional[float]:
    """Годовой темп из `n` месячных темпов: `(Π(1+r/100))^(12/n) − 1`, в %.

    Для трёх месяцев показатель степени 12/3 = 4; для одного — 12. Это
    геометрическое среднее месячных темпов, а не сумма: месячный темп — прирост,
    и годовой получается произведением множителей.
    """
    if not values:
        return None
    gross = 1.0
    for value in values:
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"годовой темп: значение {value!r} — не конечное число")
        gross *= 1.0 + number / 100.0
        if gross <= 0.0:
            raise ValueError(f"годовой темп: произведение множителей {gross} ≤ 0")
    return (gross ** (12.0 / len(values)) - 1.0) * 100.0


def e3_adaptive(points: list, asof_date: date) -> Optional[float]:
    """Robustness `m3`: годовой темп трёх последних доступных SA м/м."""
    values = asof_window(points, krm.available_from_cpd, asof_date, 3)
    return annualize_trailing(values)


def e3_adaptive_yoy(points: list, asof_date: date) -> Optional[float]:
    """Базовые адаптивные ожидания: 12-месячный y/y из SA м/м.

    Требуется ровно 12 доступных значений: усечённое окно смешивало бы
    горизонты и выглядело бы измеренным. Это прокси ожиданий, а не прогноз.
    """
    values = asof_window(points, krm.available_from_cpd, asof_date, 12)
    if len(values) < 12:
        return None
    return annualize_trailing(values)


def headline_yoy(points: list, asof_date: date) -> Optional[float]:
    """Годовой headline-темп (12-месячное компаундирование SA м/м), или None."""
    return e3_adaptive_yoy(points, asof_date)


def hp_gap(
    points: list,
    available,
    asof_date: date,
    *,
    lamb: float,
    min_points: int,
    percent: bool,
) -> Optional[float]:
    """Односторонний HP-разрыв на дату `asof_date`.

    Берутся только точки, доступные по `available`; меньше `min_points` —
    `None` (не подставляем ноль). `percent=True` — разрыв в процентах от
    тренда (`(level/trend − 1)·100`), иначе — в единицах ряда (`level − trend`).
    """
    candidates = [
        (period, value)
        for period, value in points
        if available(period) <= asof_date
    ]
    candidates.sort(key=lambda point: point[0])
    if len(candidates) < min_points:
        return None
    values = np.asarray([value for _, value in candidates], dtype=float)
    if not np.all(np.isfinite(values)):
        raise ValueError("HP-разрыв: в окне есть не конечное значение")
    _, trend = hpfilter(values, lamb=lamb)
    last_value = float(values[-1])
    last_trend = float(trend[-1])
    if not math.isfinite(last_trend) or abs(last_trend) <= FINITE_EPS:
        return None
    if percent:
        return (last_value / last_trend - 1.0) * 100.0
    return last_value - last_trend


def r_star_trend_series(rows: list, cpd_all: list) -> list:
    """`r*` как среднее реальной ставки по прошлым заседаниям.

    Для каждой строки — среднее `(rate_before − headline_yoy)` по последним
    `R_TREND_WINDOW` заседаниям, у которых headline_yoy доступен; минимум
    `MIN_R_TREND`. Clip [0; 6]. Используются только прошлые строки, поэтому
    look-ahead нет.
    """
    real_rates = []
    result = []
    for row in rows:
        yoy = headline_yoy(cpd_all, row.decision.meeting_date)
        if yoy is not None:
            real_rates.append(row.decision.rate_before - yoy)
        if len(real_rates) < MIN_R_TREND:
            result.append(None)
            continue
        window = real_rates[-R_TREND_WINDOW:]
        result.append(min(6.0, max(0.0, sum(window) / len(window))))
    return result


# --- Данные -------------------------------------------------------------------


def _finite(value, what: str) -> float:
    number = krm.row_float(value)
    if not math.isfinite(number):
        raise ValueError(f"{what}: {number} — не конечное число")
    return number


def cpd_from_rows(rows: list) -> tuple:
    """Два ряда CPD: `(core, all)` как пары `(дата, SA м/м, %)`."""
    core = []
    headline = []
    for row in rows:
        name = str(row.get("name", ""))
        point = (krm.row_date(row["date"]), _finite(row["value"], f"CPD {name}"))
        if name == CPD_CORE:
            core.append(point)
        elif name == CPD_ALL:
            headline.append(point)
        else:
            raise ValueError(f"CPD: неизвестная серия {name!r}")
    return core, headline


def gdp_from_rows(rows: list) -> list:
    """Ряд SA-ВВП в ценах 2021: пары `(дата, млрд руб.)`."""
    return [(krm.row_date(row["date"]), _finite(row["value"], "ВВП")) for row in rows]


CPI_CONSENSUS_RE = re.compile(r"^cpi_(dec|avg)_(\d{4})_median$")


def cpi_consensus_from_rows(rows: list) -> list:
    """Волны прогноза ИПЦ из `v_cbr_mo_br`; дубль — ошибка, а не «последний победил»."""
    grouped = {}
    for row in rows:
        match = CPI_CONSENSUS_RE.match(str(row.get("series", "")))
        if match is None:
            continue
        wave_date = krm.row_date(row["date"])
        year = int(match.group(2))
        kind = match.group(1)
        key = (wave_date, year, kind)
        if key in grouped:
            raise ValueError(
                f"консенсус ИПЦ: дубль {kind} у волны {wave_date.isoformat()} за {year}"
            )
        grouped[key] = _finite(row["value"], f"консенсус ИПЦ {kind} {year}")
    result = [
        CpiConsensus(
            wave_date=wave_date,
            available_from=wave_date + timedelta(days=krm.CONSENSUS_PUBLICATION_LAG_DAYS),
            year=year,
            kind=kind,
            median=median,
        )
        for (wave_date, year, kind), median in sorted(grouped.items())
    ]
    return result


def e3_consensus(consensus: list, asof_date: date) -> Optional[float]:
    """Ожидаемый headline на год точки `t + 3 квартала` из макроопроса.

    Сначала `cpi_dec_<Y>_median` (ожидаемый декабрьский г/г), при отсутствии —
    `cpi_avg_<Y>_median`. Нет волны на этот год — `None`: фолд варианта
    пропускается, а не подменяется прошлогодним прогнозом.
    """
    target_year = add_months(asof_date, 9).year
    available = [
        wave
        for wave in consensus
        if wave.available_from <= asof_date and wave.year == target_year
    ]
    if not available:
        return None
    dec = [wave for wave in available if wave.kind == "dec"]
    pool = dec or [wave for wave in available if wave.kind == "avg"]
    if not pool:
        return None
    return max(pool, key=lambda wave: (wave.wave_date, wave.kind)).median


def load_cpd_both() -> tuple:
    """Оба ряда CPD одним запросом: core и headline."""
    return cpd_from_rows(krm.ch_query(CPD_SQL))


def load_gdp() -> list:
    """SA-ВВП в ценах 2021 (серия `GDP_SERIES`)."""
    return gdp_from_rows(krm.ch_query(GDP_SQL))


def load_cpi_consensus() -> list:
    """Волны прогноза ИПЦ из витрины макроопроса."""
    return cpi_consensus_from_rows(krm.ch_query(CPI_CONSENSUS_SQL))


def build_cbr_rows(
    decisions: list,
    cpd_core: list,
    cpd_all: list,
    ibk: list,
    gdp: list,
    consensus: list,
) -> tuple:
    """Строки выборки: признаки v0 + новые признаки правила as-of.

    Базовая часть (`pi_saar`, ИБК) строится `key_rate_model.build_rows` — это
    гарантирует тот же состав 108 строк, что у существующей модели. Новые
    признаки снимаются на дату заседания; отсутствие — `None`, а не ноль.
    """
    base_rows, missing_base = krm.build_rows(decisions, cpd_core, ibk)
    rows = []
    for base in base_rows:
        meeting = base.decision.meeting_date
        rows.append(
            CbrRow(
                decision=base.decision,
                pi_saar=base.pi_saar,
                gap=base.gap,
                e3_core=e3_adaptive_yoy(cpd_core, meeting),
                e3_headline_adaptive=e3_adaptive_yoy(cpd_all, meeting),
                e3_headline_consensus=e3_consensus(consensus, meeting),
                gdp_gap=hp_gap(
                    gdp,
                    available_from_gdp,
                    meeting,
                    lamb=1600.0,
                    min_points=MIN_GDP_QUARTERS,
                    percent=True,
                ),
                ibk_gap=hp_gap(
                    ibk,
                    krm.available_from_survey,
                    meeting,
                    lamb=14400.0,
                    min_points=MIN_IBK_MONTHS,
                    percent=False,
                ),
                r_star_trend=None,
                e3_core_m3=e3_adaptive(cpd_core, meeting),
                e3_headline_m3=e3_adaptive(cpd_all, meeting),
            )
        )
    trends = r_star_trend_series(rows, cpd_all)
    rows = [replace(row, r_star_trend=trend) for row, trend in zip(rows, trends)]
    return rows, missing_base


def coverage(rows: list) -> dict:
    """Счётчики пропусков новых признаков — для отчёта и паспорта."""
    fields = (
        "e3_core",
        "e3_headline_adaptive",
        "e3_headline_consensus",
        "e3_core_m3",
        "e3_headline_m3",
        "gdp_gap",
        "ibk_gap",
        "r_star_trend",
    )
    result = {"rows": len(rows)}
    for field in fields:
        result[f"{field}_missing"] = sum(1 for row in rows if getattr(row, field) is None)
    return result


# --- Правило ------------------------------------------------------------------


def _feature(row: CbrRow, spec: RuleSpec) -> tuple:
    """(E3 core, E3 headline, gap, r*) варианта; отсутствие — DegenerateWindowError."""
    if spec.e3_source == "consensus":
        e3_core = row.e3_core
        e3_headline = row.e3_headline_consensus
    elif spec.e3_source == "m3":
        e3_core = row.e3_core_m3
        e3_headline = row.e3_headline_m3
    else:
        e3_core = row.e3_core
        e3_headline = row.e3_headline_adaptive
    gap = row.ibk_gap if spec.gap_source == "ibk" else row.gdp_gap
    r_star = spec.r_star if spec.r_star is not None else row.r_star_trend
    missing = [
        name
        for name, value in (
            ("E3 core", e3_core),
            (f"E3 headline ({spec.e3_source})", e3_headline),
            (f"gap ({spec.gap_source})", gap),
            ("r*", r_star),
        )
        if value is None
    ]
    if missing:
        raise krm.DegenerateWindowError(
            f"{spec.name}: нет признака «{', '.join(missing)}» на "
            f"{row.decision.meeting_date.isoformat()}"
        )
    for name, value in (
        ("E3 core", e3_core),
        ("E3 headline", e3_headline),
        ("gap", gap),
        ("r*", r_star),
    ):
        if not math.isfinite(float(value)):
            raise ValueError(f"{spec.name}: {name}={value} — не конечное число")
    return float(e3_core), float(e3_headline), float(gap), float(r_star)


def rule_components(row: CbrRow, spec: RuleSpec) -> RuleComponents:
    """Разложение правила: E3π_MP, gap, r*, целевой уровень и шаг."""
    e3_core, e3_headline, gap, r_star = _feature(row, spec)
    e3_mp = E3_CORE_WEIGHT * e3_core + E3_HEADLINE_WEIGHT * e3_headline
    target = GAMMA1 * row.decision.rate_before + (1.0 - GAMMA1) * (
        r_star
        + e3_mp
        + spec.gamma2 * (e3_mp - TARGET_INFLATION)
        + GAMMA3 * gap
    )
    step_bp = (target - row.decision.rate_before) * 100.0
    return RuleComponents(
        e3_core=e3_core,
        e3_headline=e3_headline,
        e3_mp=e3_mp,
        gap=gap,
        r_star=r_star,
        target_level=target,
        step_bp=step_bp,
    )


def rule_residuals(train: list, spec: RuleSpec) -> list:
    """Остатки правила на обучающем окне: `факт − шаг правила`, б.п."""
    residuals = []
    for row in train:
        try:
            components = rule_components(row, spec)
        except krm.DegenerateWindowError:
            continue
        residuals.append(row.decision.step_bp - components.step_bp)
    return residuals


def class_probabilities(step_bp: float, sigma_bp: float) -> tuple:
    """Вероятности {cut, hold, hike} из N(step, σ) с границей ±12.5 б.п.

    Это соглашение о дискретизации непрерывного шага в три класса (спека §5),
    а не структурное распределение. Вероятности клипуются и нормируются,
    чтобы log loss не получал ноль.
    """
    sigma = max(float(sigma_bp), PROB_FLOOR_BP)
    if not math.isfinite(sigma) or sigma <= 0.0:
        raise ValueError(f"вероятности: σ={sigma} — не положительное число")
    p_cut = norm.cdf((-HOLD_BAND_BP - step_bp) / sigma)
    p_hike = 1.0 - norm.cdf((HOLD_BAND_BP - step_bp) / sigma)
    p_cut = min(1.0 - PROB_EPS, max(PROB_EPS, float(p_cut)))
    p_hike = min(1.0 - PROB_EPS, max(PROB_EPS, float(p_hike)))
    p_hold = max(PROB_EPS, 1.0 - p_cut - p_hike)
    total = p_cut + p_hold + p_hike
    return (p_cut / total, p_hold / total, p_hike / total)


def fit_cbr_rule(train: list, row: CbrRow, spec: RuleSpec) -> krm.Forecast:
    """Прогноз одного фолда: шаг правила и вероятности N(step, σ_train)."""
    components = rule_components(row, spec)
    residuals = rule_residuals(train, spec)
    if len(residuals) < MIN_RULE_RESIDUALS:
        raise krm.DegenerateWindowError(
            f"{spec.name}: остатков {len(residuals)} < {MIN_RULE_RESIDUALS} "
            f"(окно {len(train)})"
        )
    sigma = math.sqrt(sum(value * value for value in residuals) / len(residuals))
    probs = class_probabilities(components.step_bp, sigma)
    return krm.Forecast(components.step_bp, probs)


# --- Walk-forward -------------------------------------------------------------


def method_factories() -> list:
    """Бенчмарки v0 + варианты cbr_rule в порядке отчёта."""
    factories = [
        ("hold", krm.predict_hold),
        ("repeat", krm.predict_repeat),
        ("ar1", krm.fit_ar1),
        ("ordered_logit", krm.fit_ordered_logit),
        ("climatology", krm.fit_climatology),
    ]
    for spec in VARIANTS:
        factories.append(
            (
                spec.name,
                lambda train, row, spec=spec: fit_cbr_rule(train, row, spec),
            )
        )
    return factories


def run_walk_forward(rows: list) -> tuple:
    """Все методы × оба окна; возвращает (predictions, fold_skips)."""
    predictions = []
    fold_skips = {}
    for window, rolling in ((WINDOW_EXPANDING, None), (krm.window_label(ROLLING_WINDOW), ROLLING_WINDOW)):
        for method, fit_predict in method_factories():
            folds, skipped = krm.walk_forward(
                rows,
                method,
                fit_predict,
                min_train=MIN_TRAIN,
                rolling=rolling,
            )
            predictions.extend(folds)
            fold_skips[(method, window)] = skipped
    return predictions, fold_skips


def group_predictions(predictions: list) -> dict:
    """«метод × окно» → фолды в порядке `METHOD_ORDER`."""
    grouped = {}
    for prediction in predictions:
        grouped.setdefault((prediction.method, prediction.window), []).append(prediction)
    return {
        (method, window): grouped[(method, window)]
        for method in METHOD_ORDER
        for window in WINDOWS
        if (method, window) in grouped
    }


def errors(group: list) -> list:
    return [prediction.step_bp - prediction.actual_step_bp for prediction in group]


def steps(group: list) -> tuple:
    return (
        [prediction.step_bp for prediction in group],
        [prediction.actual_step_bp for prediction in group],
    )


def probabilistic(group: list) -> Optional[dict]:
    if not group or any(prediction.probs is None for prediction in group):
        return None
    probs = [prediction.probs for prediction in group]
    outcomes = [prediction.actual_y3 for prediction in group]
    return {
        "brier": krm.brier_multi(probs, outcomes),
        "log_loss": krm.log_loss_multi(probs, outcomes),
        "bins": krm.reliability_bins(probs, outcomes),
    }


def aligned_errors(group_a: list, group_b: list) -> tuple:
    """Ошибки двух методов на ОБЩИХ заседаниях (парность DM)."""
    errors_a = {
        prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
        for prediction in group_a
    }
    errors_b = {
        prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
        for prediction in group_b
    }
    shared = sorted(set(errors_a) & set(errors_b))
    return [errors_a[day] for day in shared], [errors_b[day] for day in shared]


def dm_pair(group_a: list, group_b: list) -> Optional[dict]:
    """DM-HLN и блочный бутстрап пары; None — нет общих заседаний."""
    errors_a, errors_b = aligned_errors(group_a, group_b)
    if not errors_a:
        return None
    statistic, pvalue = krm.diebold_mariano(errors_a, errors_b)
    bootstrap = krm.block_bootstrap_rmse_diff(errors_a, errors_b)
    return {
        "n": len(errors_a),
        "rmse_a": eu.rmse(errors_a),
        "rmse_b": eu.rmse(errors_b),
        "statistic": statistic,
        "pvalue": pvalue,
        "bootstrap": bootstrap,
    }


# --- Метрики и решение ---------------------------------------------------------


def fmt(value: Optional[float], places: int = 2) -> str:
    if value is None:
        return "—"
    if isinstance(value, float) and math.isinf(value):
        return "+inf" if value > 0 else "−inf"
    return f"{value:.{places}f}"


def fmt_p(value: Optional[float]) -> str:
    if value is None:
        return "—"
    if value < 0.0001:
        return "<0.0001"
    return f"{value:.4f}"


def _sign(value: float) -> int:
    """Знак шага как класс `y3`: −1 / 0 / +1."""
    return (value > 0) - (value < 0)


def table(header: tuple, rows: list) -> str:
    return "\n".join(["", *header, *rows, ""])


def metric_row(method: str, window: str, group: list) -> str:
    if not group:
        return f"| {method} | {window} | 0 фолдов | — | — | — | — | — | — |"
    predicted, actual = steps(group)
    error_values = errors(group)
    prob = probabilistic(group)
    return (
        f"| {method} | {window} | {len(group)} "
        f"| {fmt(krm.rmse_bp(error_values))} | {fmt(krm.mae_bp(error_values))} "
        f"| {fmt(krm.hit_rate(predicted, actual), 3)} "
        f"| {fmt(prob['brier'], 4) if prob else '—'} "
        f"| {fmt(prob['log_loss'], 4) if prob else '—'} |"
    )


def significance_row(method: str, window: str, reference: str, pair: Optional[dict]) -> str:
    if pair is None:
        return f"| {method} | {window} | {reference} | — | — | — | — | — |"
    bootstrap = pair["bootstrap"]
    return (
        f"| {method} | {window} | {reference} "
        f"| {fmt(bootstrap.rmse_diff)} | {fmt(pair['statistic'])} | {fmt_p(pair['pvalue'])} "
        f"| [{fmt(bootstrap.low)}, {fmt(bootstrap.high)}] | {fmt_p(bootstrap.p_value)} |"
    )


def decide(groups: dict) -> tuple:
    """Предзарегистрированное правило решения (спека §10)."""
    base = groups.get(("cbr_rule", WINDOW_EXPANDING), [])
    ordered = groups.get(("ordered_logit", WINDOW_EXPANDING), [])
    if not base or not ordered:
        return "reject", "нет фолдов cbr_rule или ordered_logit на expanding"
    base_rmse = krm.rmse_bp(errors(base))
    ordered_rmse = krm.rmse_bp(errors(ordered))
    pair_ordered = dm_pair(base, ordered)
    pairs = {}
    for benchmark in ("hold", "ar1"):
        reference = groups.get((benchmark, WINDOW_EXPANDING), [])
        pairs[benchmark] = dm_pair(base, reference) if reference else None
    base_prob = probabilistic(base)
    ordered_prob = probabilistic(ordered)
    base_brier = base_prob["brier"] if base_prob else None
    ordered_brier = ordered_prob["brier"] if ordered_prob else None
    def better(pair) -> bool:
        """Значимо лучше: DM p<0.05 И бутстрап-CI строго ниже нуля."""
        if pair is None or base_rmse >= pair["rmse_b"]:
            return False
        bootstrap = pair["bootstrap"]
        return (
            pair["pvalue"] < 0.05
            and bootstrap.p_value < 0.05
            and bootstrap.high < 0.0
        )

    def worse(pair) -> bool:
        """Значимо хуже: DM p<0.05 ИЛИ бутстрап-CI строго выше нуля.

        Асимметрия осознанная: для «promote» нужны оба свидетельства, для
        «reject» достаточно одного — тяжёлые хвосты занижают мощность DM, и
        блочный бутстрап ловит разницу, которую DM-HLN не видит.
        """
        if pair is None or base_rmse <= pair["rmse_b"]:
            return False
        bootstrap = pair["bootstrap"]
        return pair["pvalue"] < 0.05 or (
            bootstrap.p_value < 0.05 and bootstrap.low > 0.0
        )

    better_ordered = better(pair_ordered)
    better_benchmark = any(better(pair) for pair in pairs.values())
    worse_ordered = worse(pair_ordered)
    worse_benchmark = any(worse(pair) for pair in pairs.values())
    brier_ok = (
        base_brier is None
        or ordered_brier is None
        or base_brier <= ordered_brier
    )
    reason = (
        f"RMSE cbr_rule {fmt(base_rmse)} vs ordered_logit {fmt(ordered_rmse)} "
        f"(DM p={fmt_p(pair_ordered['pvalue']) if pair_ordered else '—'}, "
        f"boot p={fmt_p(pair_ordered['bootstrap'].p_value) if pair_ordered else '—'}); "
        f"Brier {fmt(base_brier, 4) if base_brier is not None else '—'} vs "
        f"{fmt(ordered_brier, 4) if ordered_brier is not None else '—'}"
    )
    if better_ordered and better_benchmark and brier_ok:
        return "promote", reason
    if worse_ordered or worse_benchmark:
        return "reject", reason
    return "iterate", reason


# --- Отчёт --------------------------------------------------------------------


def _rule_parameter_rows() -> list:
    return [
        f"| γ1 (сглаживание) | {GAMMA1:.2f} |",
        f"| γ2 (реакция на инфляцию) | {GAMMA2_GRID[0]:.1f}; {GAMMA2_GRID[1]:.1f} — hard |",
        f"| γ3 (реакция на gap) | {GAMMA3:.1f} |",
        f"| цель по инфляции | {TARGET_INFLATION:.1f} % |",
        f"| вес core в E3π_MP | {E3_CORE_WEIGHT:.2f} |",
        f"| E3 (база) | 12-месячный y/y SA м/м; robustness `m3` — 3 месяца |",
        f"| r* (база) | {R_STAR_BASE:.1f} %; чувствительность {R_STAR_GRID} |",
    ]


def _coverage_rows(cov: dict) -> list:
    return [
        f"| строк выборки | {cov['rows']} |",
        f"| нет E3 core | {cov['e3_core_missing']} |",
        f"| нет E3 headline (адаптивный) | {cov['e3_headline_adaptive_missing']} |",
        f"| нет E3 headline (консенсус) | {cov['e3_headline_consensus_missing']} |",
        f"| нет E3 core (m3) | {cov['e3_core_m3_missing']} |",
        f"| нет E3 headline (m3) | {cov['e3_headline_m3_missing']} |",
        f"| нет HP-разрыва ВВП | {cov['gdp_gap_missing']} |",
        f"| нет HP-разрыва ИБК | {cov['ibk_gap_missing']} |",
        f"| нет r*-тренда | {cov['r_star_trend_missing']} |",
    ]


def _metric_rows(groups: dict) -> list:
    return [
        metric_row(method, window, groups.get((method, window), []))
        for method in METHOD_ORDER
        for window in WINDOWS
    ]


def _fold_rows(groups: dict, fold_skips: dict) -> list:
    return [
        f"| {method} | {window} | {len(groups.get((method, window), []))} "
        f"| {fold_skips.get((method, window), 0)} |"
        for method in METHOD_ORDER
        for window in WINDOWS
    ]


def _significance_rows(groups: dict) -> list:
    rows = []
    for method in METHOD_ORDER:
        for window in WINDOWS:
            group = groups.get((method, window), [])
            if not group:
                continue
            for benchmark in BENCHMARK_METHODS:
                if benchmark == method:
                    continue
                reference = groups.get((benchmark, window), [])
                rows.append(
                    significance_row(
                        method,
                        window,
                        benchmark,
                        dm_pair(group, reference) if reference else None,
                    )
                )
    return rows


def _ordered_rows(groups: dict) -> list:
    rows = []
    reference = groups.get(("ordered_logit", WINDOW_EXPANDING), [])
    for spec in VARIANTS:
        group = groups.get((spec.name, WINDOW_EXPANDING), [])
        rows.append(
            significance_row(
                spec.name,
                WINDOW_EXPANDING,
                "ordered_logit",
                dm_pair(group, reference) if group and reference else None,
            )
        )
    return rows


def _mz_rows(groups: dict) -> list:
    rows = []
    for method in METHOD_ORDER:
        for window in WINDOWS:
            group = groups.get((method, window), [])
            if not group:
                continue
            predicted, actual = steps(group)
            mz = krm.mincer_zarnowitz(actual, predicted)
            statistic, pvalue = krm.pesaran_timmermann(
                [_sign(value) for value in predicted],
                [_sign(value) for value in actual],
            )
            rows.append(
                f"| {method} | {window} | {len(group)} "
                f"| {fmt(mz.intercept if mz else None, 3)} "
                f"| {fmt(mz.slope if mz else None, 3)} "
                f"| {fmt_p(mz.p_value if mz else None)} "
                f"| {fmt(statistic, 3)} | {fmt_p(pvalue)} |"
            )
    return rows


def _reliability_rows(groups: dict) -> list:
    rows = []
    for method in PROBABILISTIC_METHODS:
        group = groups.get((method, WINDOW_EXPANDING), [])
        prob = probabilistic(group)
        if prob is None:
            continue
        for item in prob["bins"]:
            rows.append(
                f"| {method} | [{fmt(item.lo, 2)}; {fmt(item.hi, 2)}) | {item.count} "
                f"| {fmt(item.mean_prob, 3)} | {fmt(item.accuracy, 3)} |"
            )
    return rows


def _regime_rows(groups: dict) -> list:
    regimes = (
        ("2014–2021", date(2014, 1, 1), date(2021, 12, 31)),
        ("2022–2023", date(2022, 1, 1), date(2023, 12, 31)),
        ("2024–2026", date(2024, 1, 1), date(2026, 12, 31)),
        ("2018+", date(2018, 1, 1), date(2026, 12, 31)),
    )
    rows = []
    for method in ("hold", "ar1", "ordered_logit", "cbr_rule"):
        group = groups.get((method, WINDOW_EXPANDING), [])
        for label, start, end in regimes:
            subset = [
                prediction
                for prediction in group
                if start <= prediction.meeting_date <= end
            ]
            if not subset:
                rows.append(f"| {method} | {label} | 0 | — | — | — |")
                continue
            predicted, actual = steps(subset)
            error_values = errors(subset)
            rows.append(
                f"| {method} | {label} | {len(subset)} "
                f"| {fmt(krm.rmse_bp(error_values))} | {fmt(krm.mae_bp(error_values))} "
                f"| {fmt(krm.hit_rate(predicted, actual), 3)} |"
            )
    return rows


def _residual_rows(rows: list, spec: RuleSpec) -> tuple:
    diagnostics = []
    for row in rows:
        try:
            components = rule_components(row, spec)
        except krm.DegenerateWindowError:
            continue
        residual = row.decision.step_bp - components.step_bp
        diagnostics.append(
            (
                row.decision.meeting_date,
                row.decision.rate_before,
                components.e3_mp,
                components.gap,
                components.step_bp,
                float(row.decision.step_bp),
                residual,
            )
        )
    if not diagnostics:
        return [], None
    values = [item[6] for item in diagnostics]
    mean = sum(values) / len(values)
    std = math.sqrt(sum((value - mean) ** 2 for value in values) / len(values))
    direction = sum(
        1 for item in diagnostics if _sign(item[4]) == _sign(item[5])
    ) / len(diagnostics)
    top = sorted(diagnostics, key=lambda item: abs(item[6]), reverse=True)[:10]
    table_rows = [
        f"| {day.isoformat()} | {fmt(rate)} | {fmt(e3_mp)} | {fmt(gap)} "
        f"| {fmt(rule_step)} | {fmt(actual)} | {fmt(residual)} |"
        for day, rate, e3_mp, gap, rule_step, actual, residual in top
    ]
    summary = {
        "n": len(diagnostics),
        "mean": mean,
        "std": std,
        "direction_hit": direction,
    }
    return table_rows, summary


def _sensitivity_rows(rows: list) -> list:
    rows_out = []
    for r_star in R_STAR_GRID:
        for gamma2 in GAMMA2_GRID:
            spec = RuleSpec(
                f"sens_r{r_star:g}_g{gamma2:g}",
                gamma2=gamma2,
                r_star=r_star,
            )
            for window, rolling in (
                (WINDOW_EXPANDING, None),
                (krm.window_label(ROLLING_WINDOW), ROLLING_WINDOW),
            ):
                folds, _ = krm.walk_forward(
                    rows,
                    spec.name,
                    lambda train, row, spec=spec: fit_cbr_rule(train, row, spec),
                    min_train=MIN_TRAIN,
                    rolling=rolling,
                )
                if not folds:
                    rows_out.append(f"| {r_star:g} | {gamma2:g} | {window} | 0 | — | — |")
                    continue
                predicted, actual = steps(folds)
                error_values = errors(folds)
                rows_out.append(
                    f"| {r_star:g} | {gamma2:g} | {window} | {len(folds)} "
                    f"| {fmt(krm.rmse_bp(error_values))} | {fmt(krm.mae_bp(error_values))} |"
                )
    return rows_out


def _current_forecast(
    rows: list,
    meetings: list,
    key_rate: list,
    cpd_core: list,
    cpd_all: list,
    ibk: list,
    gdp: list,
    consensus: list,
    as_of: date,
) -> Optional[dict]:
    """Информационный прогноз на ближайшее заседание; в forecast_log не идёт."""
    future = [day for day in meetings if day > as_of]
    if not future:
        return None
    meeting = min(future)
    inputs = krm.forecast_inputs(as_of, key_rate, cpd_core, ibk)
    base = krm.forecast_row(meeting, inputs)
    if base is None:
        return None
    last_trend = next(
        (row.r_star_trend for row in reversed(rows) if row.r_star_trend is not None),
        None,
    )
    current = CbrRow(
        decision=base.decision,
        pi_saar=base.pi_saar,
        gap=base.gap,
        e3_core=e3_adaptive_yoy(cpd_core, as_of),
        e3_headline_adaptive=e3_adaptive_yoy(cpd_all, as_of),
        e3_headline_consensus=e3_consensus(consensus, as_of),
        gdp_gap=hp_gap(
            gdp, available_from_gdp, as_of,
            lamb=1600.0, min_points=MIN_GDP_QUARTERS, percent=True,
        ),
        ibk_gap=hp_gap(
            ibk, krm.available_from_survey, as_of,
            lamb=14400.0, min_points=MIN_IBK_MONTHS, percent=False,
        ),
        r_star_trend=last_trend,
        e3_core_m3=e3_adaptive(cpd_core, as_of),
        e3_headline_m3=e3_adaptive(cpd_all, as_of),
    )
    methods = {}
    for spec in VARIANTS:
        try:
            forecast = fit_cbr_rule(rows, current, spec)
            components = rule_components(current, spec)
        except krm.DegenerateWindowError as error:
            methods[spec.name] = {"skipped": str(error)}
            continue
        methods[spec.name] = {
            "step_bp": forecast.step_bp,
            "probs": list(forecast.probs) if forecast.probs else None,
            "e3_core": components.e3_core,
            "e3_headline": components.e3_headline,
            "e3_mp": components.e3_mp,
            "gap": components.gap,
            "r_star": components.r_star,
        }
    return {
        "as_of": as_of.isoformat(),
        "meeting_date": meeting.isoformat(),
        "rate_before": current.decision.rate_before,
        "methods": methods,
        "note": "информационно; в forecast_log не записывается до подтверждения",
    }


def _current_rows(current: Optional[dict]) -> list:
    if current is None:
        return ["| — | — | — | — | — | — |"]
    rows = []
    for spec in VARIANTS:
        item = current["methods"].get(spec.name, {})
        if "skipped" in item:
            rows.append(f"| {spec.name} | — | — | — | — | пропуск: {item['skipped']} |")
            continue
        probs = item["probs"]
        prob_text = (
            "/".join(fmt(value, 3) for value in probs) if probs else "—"
        )
        rows.append(
            f"| {spec.name} | {fmt(item['step_bp'])} | {prob_text} "
            f"| {fmt(item['e3_mp'])} | {fmt(item['gap'])} | {fmt(item['r_star'])} |"
        )
    return rows


def render_report(
    as_of: date,
    rows: list,
    cov: dict,
    groups: dict,
    fold_skips: dict,
    decision: str,
    decision_reason: str,
    current: Optional[dict],
    residual_rows: list,
    residual_summary: Optional[dict],
    sensitivity_rows: list,
    sample_notes: dict,
) -> str:
    lines = [
        "# cbr_rule: калиброванное правило ЦБ — walk-forward",
        "",
        f"**Дата прогона:** {as_of.isoformat()}",
        f"**Спека:** `{SPEC_PATH}`",
        f"**План:** `{PLAN_PATH}`",
        "**В `forecast_log` не записано:** трек экспериментальный, до подтверждения владельцем.",
        "",
        "## 1. Правило и параметры",
        "",
        "```",
        "i_t = γ1·i_{t−1} + (1−γ1)·(r* + E3π_MP + γ2·(E3π_MP − 4) + γ3·gap)",
        "E3π_MP = 0.75·E3(core) + 0.25·E3(headline)",
        "```",
        "",
        "`i_{t−1}` — фактическая ставка накануне заседания; это одношаговое",
        "инерционное правило от фактической ставки, а не симуляция пути.",
        "",
        table(("| параметр | значение |", "|---|---|"), _rule_parameter_rows()),
        "## 2. Выборка и покрытие",
        "",
        f"**Решений:** {sample_notes['decisions']}; **строк:** {len(rows)}; "
        f"**исключено:** {sample_notes['excluded']}; "
        f"**пропущено без ставки:** {sample_notes['missing_rate']}; "
        f"**пропущено без базовых признаков:** {sample_notes['missing_features']}.",
        "",
        f"Walk-forward: min_train={MIN_TRAIN}; расширяющееся окно — "
        f"{sample_notes['folds_expanding']} фолдов; rolling{ROLLING_WINDOW} — "
        f"{sample_notes['folds_rolling']}.",
        "",
        table(("| признак | значение |", "|---|---|"), _coverage_rows(cov)),
        "## 3. Метрики",
        "",
        "Primary metric — RMSE шага в б.п. на расширяющемся окне; Brier/log loss —",
        "только у методов с вероятностями.",
        "",
        table(
            (
                "| метод | окно | n | RMSE, б.п. | MAE, б.п. | hit rate | Brier | log loss |",
                "|---|---|---|---|---|---|---|---|",
            ),
            _metric_rows(groups),
        ),
        "## 4. Пропуски фолдов",
        "",
        table(
            ("| метод | окно | фолдов | пропущено |", "|---|---|---|---|"),
            _fold_rows(groups, fold_skips),
        ),
        "## 5. Значимость: DM-HLN и блочный бутстрап",
        "",
        "DM — против B1 hold, B2 repeat, B3 AR(1); ΔRMSE = RMSE метода − RMSE бенчмарка.",
        "",
        table(
            (
                "| метод | окно | против | ΔRMSE, б.п. | DM | DM p | 95% ДИ ΔRMSE | бутстр. p |",
                "|---|---|---|---|---|---|---|---|",
            ),
            _significance_rows(groups),
        ),
        "### 5.1. cbr_rule против ordered_logit",
        "",
        table(
            (
                "| метод | окно | против | ΔRMSE, б.п. | DM | DM p | 95% ДИ ΔRMSE | бутстр. p |",
                "|---|---|---|---|---|---|---|---|",
            ),
            _ordered_rows(groups),
        ),
        "## 6. Калибровка вероятностей",
        "",
        "Brier и log loss — в таблице метрик; ниже — надёжностные бины",
        "(расширяющееся окно, уверенность argmax-класса).",
        "",
        table(
            (
                "| метод | бин уверенности | n | средняя уверенность | точность |",
                "|---|---|---|---|---|",
            ),
            _reliability_rows(groups),
        ),
        "## 7. Mincer–Zarnowitz и Pesaran–Timmermann",
        "",
        table(
            (
                "| метод | окно | n | MZ a | MZ b | MZ p (a=0 ∧ b=1) | PT | PT p |",
                "|---|---|---|---|---|---|---|---|",
            ),
            _mz_rows(groups),
        ),
        "## 8. Режимы",
        "",
        table(
            ("| метод | режим | n | RMSE, б.п. | MAE, б.п. | hit rate |", "|---|---|---|---|---|---|"),
            _regime_rows(groups),
        ),
        "## 9. Остаток правила",
        "",
        "`actual − cbr_rule` по всем строкам выборки; top-10 по |остатку|.",
        "",
        (
            f"Средний остаток: {fmt(residual_summary['mean'])} б.п.; "
            f"σ: {fmt(residual_summary['std'])} б.п.; "
            f"совпадение направления: {fmt(residual_summary['direction_hit'], 3)}."
            if residual_summary
            else "Остатки не посчитаны: нет полных строк."
        ),
        "",
        table(
            (
                "| заседание | rate_before | E3π_MP | gap | шаг правила | факт | остаток |",
                "|---|---|---|---|---|---|---|",
            ),
            residual_rows,
        ),
        "## 10. Чувствительность r* и γ2",
        "",
        table(
            (
                "| r* | γ2 | окно | n | RMSE, б.п. | MAE, б.п. |",
                "|---|---|---|---|---|---|",
            ),
            sensitivity_rows,
        ),
        "## 11. Текущий прогноз (информационно)",
        "",
        (
            f"Ближайшее заседание: **{current['meeting_date']}**, срез "
            f"**{current['as_of']}**, ставка до: {fmt(current['rate_before'])} %."
            if current
            else "Будущих заседаний или входов на дату среза нет."
        ),
        "",
        table(
            (
                "| вариант | шаг, б.п. | P(cut/hold/hike) | E3π_MP | gap | r* |",
                "|---|---|---|---|---|---|",
            ),
            _current_rows(current),
        ),
        "## 12. Вывод",
        "",
        f"**Вердикт: `{decision}`.** {decision_reason}.",
        "",
        "Правило решения — спека §10: `promote` требует значимого выигрыша по RMSE",
        "против `ordered_logit` и хотя бы одного из B1/B3 при Brier не хуже;",
        "`reject` — значимого проигрыша; иначе `iterate`.",
        "",
        "## 13. Ограничения",
        "",
        "- Нет PIT/винтажей по ВВП и CPD: ревизии не учтены, as-of — лаг публикации.",
        "- HP-разрыв односторонний, ранние фолды (2014–2017) опираются на 8–20 кварталов.",
        "- E3 — адаптивный прокси; консенсус-вариант — гибрид и подвыборка с 2021.",
        "- Правило одношаговое от фактической ставки; вероятности — соглашение N(step, σ_train).",
        "- В `forecast_log` не пишем, не публикуем.",
        "",
    ]
    return "\n".join(lines)


# --- Артефакты ----------------------------------------------------------------


def _csv_cell(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def write_predictions(path: Path, predictions: list, rows: list) -> None:
    row_by_date = {row.decision.meeting_date: row for row in rows}
    columns = (
        "meeting_date",
        "method",
        "window",
        "step_bp",
        "actual_step_bp",
        "error_bp",
        "probs",
        "e3_core",
        "e3_headline",
        "e3_mp",
        "gdp_gap",
        "ibk_gap",
        "r_star",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for prediction in predictions:
            row = row_by_date.get(prediction.meeting_date)
            spec = next(
                (item for item in VARIANTS if item.name == prediction.method),
                None,
            )
            e3_headline = None
            e3_mp = None
            gap = None
            r_star = None
            if row is not None and spec is not None:
                try:
                    components = rule_components(row, spec)
                    e3_headline = components.e3_headline
                    e3_mp = components.e3_mp
                    gap = components.gap
                    r_star = components.r_star
                except krm.DegenerateWindowError:
                    pass
            writer.writerow(
                [
                    _csv_cell(prediction.meeting_date.isoformat()),
                    prediction.method,
                    prediction.window,
                    _csv_cell(prediction.step_bp),
                    _csv_cell(prediction.actual_step_bp),
                    _csv_cell(prediction.step_bp - prediction.actual_step_bp),
                    _csv_cell(
                        json.dumps(list(prediction.probs), ensure_ascii=False)
                        if prediction.probs
                        else None
                    ),
                    _csv_cell(row.e3_core if row else None),
                    _csv_cell(e3_headline),
                    _csv_cell(e3_mp),
                    _csv_cell(row.gdp_gap if row else None),
                    _csv_cell(row.ibk_gap if row else None),
                    _csv_cell(r_star),
                ]
            )


def write_diagnostics(path: Path, rows: list, spec: RuleSpec) -> None:
    columns = (
        "meeting_date",
        "rate_before",
        "e3_core",
        "e3_headline",
        "e3_mp",
        "gdp_gap",
        "ibk_gap",
        "r_star",
        "rule_step_bp",
        "actual_step_bp",
        "residual_bp",
    )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(columns)
        for row in rows:
            try:
                components = rule_components(row, spec)
            except krm.DegenerateWindowError:
                continue
            writer.writerow(
                [
                    row.decision.meeting_date.isoformat(),
                    _csv_cell(row.decision.rate_before),
                    _csv_cell(components.e3_core),
                    _csv_cell(components.e3_headline),
                    _csv_cell(components.e3_mp),
                    _csv_cell(row.gdp_gap),
                    _csv_cell(row.ibk_gap),
                    _csv_cell(components.r_star),
                    _csv_cell(components.step_bp),
                    _csv_cell(row.decision.step_bp),
                    _csv_cell(row.decision.step_bp - components.step_bp),
                ]
            )


def write_forecast(path: Path, current: Optional[dict]) -> None:
    if current is None:
        if path.exists():
            path.unlink()
        return
    path.write_text(
        json.dumps(current, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _sample_notes(decisions: list, excluded: list, missing_rate: list, missing_features: list, groups: dict) -> dict:
    return {
        "decisions": len(decisions),
        "excluded": len(excluded),
        "missing_rate": len(missing_rate),
        "missing_features": len(missing_features),
        "folds_expanding": max(
            (len(group) for (method, window), group in groups.items() if window == WINDOW_EXPANDING),
            default=0,
        ),
        "folds_rolling": max(
            (
                len(group)
                for (method, window), group in groups.items()
                if window == krm.window_label(ROLLING_WINDOW)
            ),
            default=0,
        ),
    }


# --- Журнал эксперимента -------------------------------------------------------


def _metrics_payload(groups: dict, fold_skips: dict, decision: str) -> dict:
    windows = []
    for window in WINDOWS:
        models = {}
        for method in METHOD_ORDER:
            group = groups.get((method, window), [])
            if group:
                predicted, actual = steps(group)
                error_values = errors(group)
                prob = probabilistic(group)
                models[method] = {
                    "rmse": krm.rmse_bp(error_values),
                    "mae": krm.mae_bp(error_values),
                    "hit": krm.hit_rate(predicted, actual),
                    "n": len(group),
                    "brier": prob["brier"] if prob else None,
                    "log_loss": prob["log_loss"] if prob else None,
                }
            else:
                models[method] = {
                    "rmse": None, "mae": None, "hit": None, "n": 0,
                    "brier": None, "log_loss": None,
                }
        windows.append(
            {
                "window": window,
                "folds": max((value["n"] for value in models.values()), default=0),
                "models": models,
            }
        )
    skips = {}
    for (method, window), count in fold_skips.items():
        skips.setdefault(window, {})[method] = int(count)
    return {
        "primary_metric": "rmse_bp",
        "decision": decision,
        "windows": windows,
        "fold_skips": skips,
    }


def _log_run(
    rows: list,
    predictions: list,
    groups: dict,
    fold_skips: dict,
    decision: str,
    as_of: date,
    artifacts: list,
    enabled: Optional[bool] = None,
) -> str:
    config = {
        "gamma1": GAMMA1,
        "gamma2_grid": list(GAMMA2_GRID),
        "gamma3": GAMMA3,
        "target_inflation": TARGET_INFLATION,
        "r_star_base": R_STAR_BASE,
        "r_star_grid": list(R_STAR_GRID),
        "e3_core_weight": E3_CORE_WEIGHT,
        "e3_headline_weight": E3_HEADLINE_WEIGHT,
        "min_train": MIN_TRAIN,
        "rolling_window": ROLLING_WINDOW,
        "min_gdp_quarters": MIN_GDP_QUARTERS,
        "min_ibk_months": MIN_IBK_MONTHS,
        "min_rule_residuals": MIN_RULE_RESIDUALS,
        "prob_floor_bp": PROB_FLOOR_BP,
        "hold_band_bp": HOLD_BAND_BP,
        "as_of": as_of.isoformat(),
    }
    sample_meta = {
        "rows": len(rows),
        "first": rows[0].decision.meeting_date.isoformat() if rows else None,
        "last": rows[-1].decision.meeting_date.isoformat() if rows else None,
        "folds_expanding": max(
            (
                len(group)
                for (method, window), group in groups.items()
                if window == WINDOW_EXPANDING
            ),
            default=0,
        ),
        "folds_rolling": max(
            (
                len(group)
                for (method, window), group in groups.items()
                if window == krm.window_label(ROLLING_WINDOW)
            ),
            default=0,
        ),
        "fold_skips": sum(fold_skips.values()),
    }
    sample = [
        {
            "meeting_date": row.decision.meeting_date.isoformat(),
            "rate_before": row.decision.rate_before,
            "pi_saar": row.pi_saar,
            "gap": row.gap,
            "e3_core": row.e3_core,
            "e3_headline_adaptive": row.e3_headline_adaptive,
            "e3_headline_consensus": row.e3_headline_consensus,
            "gdp_gap": row.gdp_gap,
            "ibk_gap": row.ibk_gap,
            "r_star_trend": row.r_star_trend,
        }
        for row in rows
    ]
    logger = mlx.start_run(
        experiment="key_rate_cbr_rule",
        run_name="cbr_rule__all__start24",
        config=config,
        data_asof=as_of,
        sample_meta=sample_meta,
        sample=sample,
        spec_path=SPEC_PATH,
        enabled=enabled,
    )
    logger.log_metrics(_metrics_payload(groups, fold_skips, decision))
    logger.log_predictions(
        [
            mlx.PredictionRow(
                model=prediction.method,
                window=prediction.window,
                target_date=prediction.meeting_date,
                predicted=prediction.step_bp,
                actual=prediction.actual_step_bp,
                unit="bp",
                meta={
                    "probs": list(prediction.probs) if prediction.probs else None,
                },
            )
            for prediction in predictions
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
    """Полный прогон: выборка, walk-forward, отчёт, артефакты, журнал."""
    as_of = as_of or date.today()
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    meetings = krm.load_meetings()
    past = [day for day in meetings if day <= as_of]
    key_rate = krm.load_key_rate()
    deltas = krm.load_mp_summary_deltas()
    decisions, excluded, missing_rate = krm.build_decisions(past, key_rate, deltas)

    cpd_core, cpd_all = load_cpd_both()
    ibk = krm.load_ibk()
    gdp = load_gdp()
    consensus = load_cpi_consensus()
    rows, missing_features = build_cbr_rows(
        decisions, cpd_core, cpd_all, ibk, gdp, consensus
    )
    cov = coverage(rows)

    print(
        f"выборка: решений {len(decisions)}, строк {len(rows)}, "
        f"исключено {len(excluded)}, без ставки {len(missing_rate)}, "
        f"без базовых признаков {len(missing_features)}"
    )
    if not rows:
        print("выборка пуста — walk-forward не считается", file=sys.stderr)
        return 1

    predictions, fold_skips = run_walk_forward(rows)
    groups = group_predictions(predictions)
    decision, decision_reason = decide(groups)
    current = _current_forecast(
        rows, meetings, key_rate, cpd_core, cpd_all, ibk, gdp, consensus, as_of
    )
    residual_rows, residual_summary = _residual_rows(rows, BASE_SPEC)
    sensitivity_rows = _sensitivity_rows(rows)
    notes = _sample_notes(decisions, excluded, missing_rate, missing_features, groups)

    report_path = out_dir / REPORT_NAME
    predictions_path = out_dir / PREDICTIONS_NAME
    diagnostics_path = out_dir / DIAGNOSTICS_NAME
    forecast_path = out_dir / FORECAST_NAME
    report_path.write_text(
        render_report(
            as_of,
            rows,
            cov,
            groups,
            fold_skips,
            decision,
            decision_reason,
            current,
            residual_rows,
            residual_summary,
            sensitivity_rows,
            notes,
        ),
        encoding="utf-8",
    )
    write_predictions(predictions_path, predictions, rows)
    write_diagnostics(diagnostics_path, rows, BASE_SPEC)
    write_forecast(forecast_path, current)

    selected = krm.window_label(ROLLING_WINDOW) if rolling else WINDOW_EXPANDING
    print(f"окно консольной сводки: {selected}")
    for method in METHOD_ORDER:
        group = groups.get((method, selected), [])
        if not group:
            print(f"  {method:<20} фолдов 0")
            continue
        predicted, actual = steps(group)
        error_values = errors(group)
        prob = probabilistic(group)
        print(
            f"  {method:<20} n={len(group):>3}  "
            f"RMSE {krm.rmse_bp(error_values):>8.2f}  "
            f"MAE {krm.mae_bp(error_values):>8.2f}  "
            f"hit {krm.hit_rate(predicted, actual):.3f}"
            + (f"  Brier {prob['brier']:.4f}  logloss {prob['log_loss']:.4f}" if prob else "")
        )
    print(f"вердикт: {decision} — {decision_reason}")
    print(f"отчёт: {report_path}")
    print(f"предсказания: {predictions_path}")
    print(f"диагностика: {diagnostics_path}")
    print(f"текущий прогноз: {forecast_path}")

    try:
        result = _log_run(
            rows,
            predictions,
            groups,
            fold_skips,
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
        description="cbr_rule: калиброванное правило ЦБ как трек модели ставки."
    )
    parser.add_argument(
        "--as-of",
        metavar="YYYY-MM-DD",
        default=None,
        help="дата среза (по умолчанию сегодня); фиксирует as-of и прогноз",
    )
    parser.add_argument(
        "--out-dir",
        metavar="DIR",
        default=".",
        help="каталог локальных артефактов (по умолчанию текущий)",
    )
    parser.add_argument(
        "--rolling",
        action="store_true",
        help="консольная сводка по rolling40 (в отчёт и CSV — оба окна)",
    )
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
        print(f"ошибка выборки решений: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    sys.exit(main())
