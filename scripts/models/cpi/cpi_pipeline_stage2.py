#!/usr/bin/env python3
"""Stage 2 конвейера CPI → SA/ожидания → ставка: SA-путь и cumulative h1/h3/h6.

Пункт `key-rate/phase-4-cpi-key-rate-pipeline`, спека
`docs/superpowers/specs/2026-10-20-cpi-key-rate-pipeline-design.md` §4, §6.2.

Идея: stage 1 даёт OOF-прогноз NSA m/m для `M0`/`M0+1`; stage 2 строит
сезонно скорректированный путь `M0…M0+5` и накопленный SA-рост h1/h3/h6.
Информационное множество stage 2 — OOF stage 1, real-time SA, официальный SA
ЦБ (когда вышел), инфляционные ожидания населения, ценовые ожидания/издержки
и МП, недельная diffusion-статистика и номер месяца. Политического блока
(труд, кредит, бюджет, деньги, рынок, текст) здесь нет — он reserved for
stage 3.

As-of: заседание — 13:30 МСК. Доступность месячного ИПЦ, SA ЦБ и ожиданий
населения проверяется календарём публикаций (`PublicationCalendar`), а не
фиксированным «8-е M+1». Ряды МП ЦБ точных дат в экспериментальном календаре
не имеют, поэтому используется консервативное правило
`cpi_monthly.available_from_survey` (конец `P+1`) — оно может задержать
признак на дни, но не пропускает будущее.

Цели: SA m/m `M0…M0+5` строятся по фактической NSA-истории через последний
целевой месяц (в бэктесте метки известны). Признаки же используют только
историю, доступную к заседанию: сезонные факторы и SA-лаги отсекаются
последним опубликованным месяцем (`seasonal_asof`), а целевой снимок
`seasonal` — только сторона метки/диагностики и в признаки не входит. Для
текущего/будущего заседания целевой путь — `None`. NSA back-transform
прогноза использует `seasonal_asof`, факта — `seasonal`; выбор снимка
передаётся в `nsa_backtransform` явно.

`cpi_sa_available=1` только если SA-история к заседанию — не меньше двух
полных периодов (24 месяца) и OOF stage 1 присутствует; иначе 0. Это
закрывает разрыв Task 2: пустые факторы STL неотличимы от настоящей SA без
явного индикатора.

Обучение walk-forward дополнительно фильтруется календарём: train-строка
входит, только если конец её целевого пути (`M0+5`) уже опубликован к
тестовому заседанию; иначе метки обучения заглядывали бы в будущее теста.

Модели: direct CatBoost на каждый месяц пути; опционально bottom-up по трём
компонентам (food/nonfood/services) + NNLS; ансамбль 0,5/0,5 direct и
bottom-up — заявленный кандидат, правило «только если не хуже» проверяется в
`evaluate` на общих фолдах, а не выбирается по тесту.

NSA back-transform (SA-путь + сезонные факторы) и Gate 2 (cumulative h1/h3/h6
не хуже `cpi_live3_horizons` + 5 %) — диагностика/приёмка; +5 % — допуск
плана (в спеке его нет). SA-путь отдельно сверяется с официальным SA ЦБ.

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make
cpi-pipeline-stage2`, тесты — `scripts/models/cpi/cpi_pipeline_stage2_test.py`.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from catboost import CatBoostError, CatBoostRegressor

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu  # noqa: E402
from lib import ml_experiment as mlx  # noqa: E402
from lib.publication_calendar import PublicationCalendar  # noqa: E402
from models.cpi import cpi_monthly as cm  # noqa: E402
from models.cpi import cpi_monthly_v3 as cmv3  # noqa: E402
from models.cpi import cpi_pipeline_sa as sa  # noqa: E402
from models.cpi import cpi_pipeline_stage1 as stage1  # noqa: E402

# Ряд-«голова» stage 2: общий ИПЦ Росстата (тот же, что у stage 1).
SOURCE = stage1.SOURCE
SERIES = stage1.SERIES

# Заседание ЦБ — 13:30 МСК; релизы Росстата в 19:00 того же дня позже.
MEETING_TIME = stage1.MEETING_TIME

# Целевой путь stage 2: SA m/m для M0…M0+5 (шесть месяцев).
PATH_MONTHS = 6
HORIZONS = (1, 3, 6)

# SA ЦБ: официальный ряд — признак, не таргет (спека §6.2). В ClickHouse —
# `cbr_indicators_cpd` «Все товары и услуги» (месячный SA-темп, %), в
# `shock_classifier.load_channels` — канал `cpi`.
CBR_SA_SOURCE = "cbr"
CBR_SA_SERIES = "cpi_sa"
CBR_SA_CHANNEL = "cpi"

# Инфляционные ожидания населения: отдельное расписание (21-е, 15:00 МСК).
INFL_EXP_SOURCE = "cbr"
INFL_EXP_EXPECTED = "infl_exp_expected"
INFL_EXP_OBSERVED = "infl_exp_observed"

# Ряды МП ЦБ: уровни и изменения. `price_growth_expected_sa` — ожидаемый
# прирост отпускных цен (издержки/ценовые ожидания бизнеса), с 2020 г.
MP_PRICE_EXPECTATIONS = "price_expectations_sa"
MP_IBK = "business_climate_sa"
MP_COSTS = "price_growth_expected_sa"

# Недельная diffusion-статистика stage 1 — единственный разрешённый источник
# недельных признаков stage 2 (считается на cutoff заседания − 1 день).
WEEKLY_FEATURES = tuple(cmv3.WEEKLY_FEATURES)

# Порядок признаков — контракт со stage 3 и отчётом. Политического блока нет.
FEATURE_NAMES = [
    "stage1_m0",
    "stage1_m1",
    "stage1_m0_sa",
    "stage1_m1_sa",
    "sa_lag1",
    "sa_lag2",
    "sa_lag3",
    "cbr_sa",
    "cbr_sa_chg",
    "cbr_sa_available",
    "infl_exp_expected",
    "infl_exp_expected_chg",
    "infl_exp_observed",
    "infl_exp_observed_chg",
    "price_expectations",
    "price_expectations_chg",
    "costs",
    "costs_chg",
    "ibk",
    "ibk_chg",
    *WEEKLY_FEATURES,
    "cpi_sa_available",
    "month",
]

SEED = cm.SEED
DEFAULT_START = 24
# Минимальное обучающее окно stage 2 — 24 строки (план: «24+ train-origin»);
# меньшее окно не идентифицирует сезонность и не даёт стабильный CatBoost.
MIN_TRAIN = 24
# Task 2: меньше двух полных сезонных периодов (24 месяца) — факторов нет.
SA_MIN_HISTORY = sa.MIN_PERIODS * sa.DEFAULT_PERIOD
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000

# Gate 2 (Task 3 acceptance): NSA back-transform cumulative h1/h3/h6 не хуже
# текущего `cpi_live3_horizons` более чем на 5 %. Это предрегистрация плана:
# в спеке допуска нет (там «не хуже live3»), +5 % — явная поблажка.
GATE2_MODEL = "catboost_direct"
GATE2_TOLERANCE = 1.05
GATE2_BASELINE = "cpi_live3_horizons:cpi_v3_h1/h3/h6"
LIVE3_CACHE_NAME = "stage2_live3_cache.json"

# Пары DM: direct/bottom-up и ансамбль против одиночных моделей.
PAIR_SPECS = (
    ("catboost_direct", "catboost_bottomup"),
    ("catboost_direct", "ensemble"),
    ("catboost_bottomup", "ensemble"),
)


@dataclass(frozen=True)
class Stage2Row:
    """Строка stage 2: заседание, SA-путь, накопленный рост и признаки.

    `sa_path` — SA m/m (`п.п.`) для месяцев `M0…M0+5` (ключи — первое число
    месяца) или `None`, если хотя бы один целевой месяц ещё не опубликован к
    `as_of` либо SA-история к целевому концу короче двух полных периодов.
    `cum[h]` — накопленный SA-рост за первые `h` месяцев пути
    (`100·(Π(1+s/100)−1)`, h ∈ {1, 3, 6}); `None` вместе с `sa_path`.

    `features` — только доступные к заседанию данные; отсутствующее значение —
    `None`, а не ноль. Вспомогательные поля (в матрицу признаков не входят):

    - `component_paths` — SA-путь трёх компонент bottom-up
      (food/nonfood/services), метки для bottom-up-модели;
    - `seasonal` — аддитивные сезонные факторы снимка `M0+5` (целевая
      сторона); нужны только NSA back-transform фактического пути
      (`actual_nsa_path`) и никогда не участвуют в признаках или прогнозе;
    - `seasonal_asof` — сезонные факторы снимка заседания (история до
      последнего опубликованного календарём месяца); это as-of величина,
      не видящая будущее. Именно она используется для NSA back-transform
      прогноза (`nsa_backtest`, `gate2_report`); пустой снимок — пропуск
      сверки со счётчиком `no_seasonal_asof`, а не подмена целевым.
    """

    meeting_date: date
    m0: date
    features: dict
    sa_path: Optional[dict]
    cum: Optional[dict]
    component_paths: dict = field(default_factory=dict)
    seasonal: dict = field(default_factory=dict)
    seasonal_asof: dict = field(default_factory=dict)


@dataclass(frozen=True)
class Stage2Prediction:
    """Фолд stage 2: заседание, месяц пути, модель, горизонт и прогноз.

    `horizon` — 1…6 (месяц `M0+horizon−1`); `target_month` — первое число
    целевого месяца. `cpi_sa_available` дублирует признак строки, чтобы
    stage 3 мог отличить настоящую SA от короткой истории.
    """

    meeting_date: date
    target_month: date
    model: str
    horizon: int
    y_true: float
    y_pred: float
    cpi_sa_available: int = 0


# ---------------------------------------------------------------------------
# Календарная арифметика
# ---------------------------------------------------------------------------


def shift_month(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    return stage1.shift_month(day, months)


def meeting_datetime(day: date) -> datetime:
    """Момент заседания: 13:30 МСК, наивное время."""
    return stage1.meeting_datetime(day)


def _month_of(day: date) -> date:
    return date(day.year, day.month, 1)


def _month_end(month: date) -> date:
    return shift_month(month, 1) - timedelta(days=1)


def target_months(m0: date) -> list:
    """Месяцы SA-пути `M0…M0+5` (первое число), по возрастанию."""
    return [shift_month(m0, index) for index in range(PATH_MONTHS)]


def compound(values: list) -> float:
    """Накопленный рост из месячных m/m: `100·(Π(1+v/100)−1)`, п.п."""
    product = 1.0
    for value in values:
        product *= 1.0 + float(value) / 100.0
    return (product - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Загрузчики: тонкие обёртки, чтобы тесты подменяли данные, а не транспорт
# ---------------------------------------------------------------------------


def load_cpi_series() -> dict:
    """Четыре ряда ИПЦ Росстата (NSA m/m, `percent − 100`)."""
    return cm.load_targets()


def load_expectations() -> dict:
    """Каналы ожиданий: `shock_classifier.load_channels()` (ленивый импорт).

    Используются `infl_exp_expected`, `infl_exp_observed` и `cpi` — SA ЦБ
    «Все товары и услуги». Импорт ленивый: офлайн-тесты не тянут key-rate
    зависимости.
    """
    from models.key_rate import shock_classifier as sc

    return sc.load_channels()


COSTS_SQL = """
SELECT date, value
FROM macro_series FINAL
WHERE source = 'cbr_survey' AND series = 'price_growth_expected_sa'
ORDER BY date
FORMAT JSON
"""


def _points_from_rows(rows: list, name: str) -> list:
    """`[(date, value)]` из строк `date`/`value`; NULL — ошибка, не ноль."""
    points = []
    for row in rows:
        day = cm.row_date(row["date"])
        value = cm.row_float(row["value"])
        points.append((day, value))
    points.sort(key=lambda item: item[0])
    for (previous, _), (current, _) in zip(points, points[1:]):
        if previous == current:
            raise ValueError(f"{name}: дубликат даты {current.isoformat()}")
    return points


def load_costs() -> list:
    """Ожидаемый прирост отпускных цен (МП ЦБ, `price_growth_expected_sa`)."""
    return _points_from_rows(cm.ch_query(COSTS_SQL), MP_COSTS)


def load_mp() -> dict:
    """Ряды МП ЦБ: пять рядов `cpi_monthly.load_survey` плюс издержки."""
    survey = cm.load_survey()
    survey[MP_COSTS] = load_costs()
    return survey


def load_meetings() -> list:
    """Даты канонических заседаний ЦБ (общий источник со stage 1)."""
    return stage1.load_meetings()


def load_calendar() -> PublicationCalendar:
    """Календарь публикаций из экспериментальной CSV-фикстуры Task 0."""
    return stage1.load_calendar()


# ---------------------------------------------------------------------------
# Нормализация точек и as-of
# ---------------------------------------------------------------------------


def _month_values(points, name: str) -> dict:
    """`{первое число месяца: значение}` с отказом на дубли и NaN."""
    values = {}
    for day, value in points or []:
        month = _month_of(day)
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(
                f"{name}: нечисловое значение {value!r} на {day.isoformat()}"
            )
        if month in values:
            raise ValueError(f"{name}: дубликат месяца {month.isoformat()}")
        values[month] = numeric
    return values


def _month_points(values: dict) -> list:
    """`[(конец месяца, значение)]` по возрастанию для `cpi_pipeline_sa`.

    Непрерывность проверяет Task 2 на уже отсечённом окне: точки после
    `as_of` среза не должны ронять сборку признаков/целей.
    """
    return [
        (_month_end(month), value)
        for month, value in sorted(values.items())
    ]


def _optional(value) -> Optional[float]:
    """`None`/NaN/Inf → `None`; иначе float. Ноль остаётся нулём."""
    if value is None:
        return None
    numeric = float(value)
    return numeric if math.isfinite(numeric) else None


def _asof_pair(points, available, name: str) -> tuple:
    """`(уровень, изменение)` последней доступной точки и предыдущей.

    `available(month_start) -> bool` задаёт правило доступности. Нет точек —
    `(None, None)`; одна точка — изменение `None` (не ноль).
    """
    if not points:
        return None, None
    eligible = []
    seen = set()
    for day, value in points:
        month = _month_of(day)
        numeric = float(value)
        if not math.isfinite(numeric):
            raise ValueError(
                f"{name}: нечисловое значение {value!r} на {day.isoformat()}"
            )
        if month in seen:
            raise ValueError(f"{name}: дубликат месяца {month.isoformat()}")
        seen.add(month)
        if available(month):
            eligible.append((month, numeric))
    if not eligible:
        return None, None
    eligible.sort(key=lambda item: item[0])
    level = eligible[-1][1]
    change = level - eligible[-2][1] if len(eligible) >= 2 else None
    return level, change


def _calendar_available(
    calendar, source: str, series: str, meeting_at: datetime
):
    """Правило доступности точки по календарю публикаций."""

    def available(month: date) -> bool:
        return calendar.is_available(source, series, month, meeting_at)

    return available


def _survey_available(meeting_day: date):
    """Доступность ряда МП: консервативный конец `P+1` (`cpi_monthly`)."""

    def available(month: date) -> bool:
        return cm.available_from_survey(month) <= meeting_day

    return available


def _sa_factors(
    points: list, history_cut: Optional[date]
) -> tuple:
    """Сезонные факторы и SA-ряд на срезе `history_cut` (без будущего)."""
    if history_cut is None:
        return {}, []
    factors = sa.seasonal_factors(points, history_cut)
    if not factors:
        return {}, []
    return factors, sa.sa_series(points, history_cut)


def _sa_path(values: dict, months: list, name: str) -> tuple:
    """SA-путь по фактической истории через последний целевой месяц.

    Возврат `(path, factors)`: `path` — `{месяц: SA m/m}`, `factors` — снимок
    сезонных факторов на конец пути. Пустые факторы (короткая история) —
    `(None, {})`: NSA-ряд, выданный Task 2 как identity, за SA не выдаётся.
    """
    if any(month not in values for month in months):
        return None, {}
    points = _month_points(values)
    factors = sa.seasonal_factors(points, _month_end(months[-1]))
    if not factors:
        return None, {}
    adjusted = dict(sa.sa_series(points, _month_end(months[-1])))
    path = {month: float(adjusted[_month_end(month)]) for month in months}
    return path, factors


def _target_path(
    m0: date,
    total_values: dict,
    component_values: dict,
    calendar: PublicationCalendar,
    as_of_at: datetime,
) -> tuple:
    """Целевой SA-путь и cumulative, если все метки известны к `as_of`."""
    months = target_months(m0)
    published = all(
        month in total_values
        and calendar.is_available(SOURCE, SERIES, _month_end(month), as_of_at)
        for month in months
    )
    if not published:
        return None, {}, {}, None
    path, factors = _sa_path(total_values, months, cm.TARGET_ALL)
    if path is None:
        return None, {}, {}, None
    components = {}
    for code, values in component_values.items():
        component_path, _ = _sa_path(values, months, code)
        if component_path is not None:
            components[code] = component_path
    cum = {
        horizon: compound([path[month] for month in months[:horizon]])
        for horizon in HORIZONS
    }
    return path, components, factors, cum


# ---------------------------------------------------------------------------
# Сборка строк
# ---------------------------------------------------------------------------


def build_rows(
    stage1_rows: list,
    stage1_oof: dict,
    cpi_series: dict,
    expectations: dict,
    mp: dict,
    calendar: PublicationCalendar,
    as_of: date,
) -> list:
    """Строки stage 2 для заседаний `stage1_rows` на срезе `as_of`.

    `stage1_oof` — `{дата заседания: {"m0": float, "m1": float}}` из OOF
    stage 1 (не фактические метки). `cpi_series` — `{code: [(дата, NSA m/m)]}`,
    `expectations` — каналы `shock_classifier.load_channels`, `mp` — ряды МП
    (`cpi_monthly.load_survey` плюс `price_growth_expected_sa`).

    Признаки каждого заседания строятся на его момент 13:30; целевой путь —
    только если все месяцы `M0…M0+5` опубликованы к `as_of` и SA-история к
    концу пути достаточна.
    """
    as_of_at = meeting_datetime(as_of)
    ordered = sorted(stage1_rows, key=lambda row: row.meeting_date)
    for previous, current in zip(ordered, ordered[1:]):
        if previous.meeting_date >= current.meeting_date:
            raise ValueError(
                "stage 2: заседания не уникальны/не упорядочены "
                f"({previous.meeting_date} >= {current.meeting_date})"
            )

    expectations = expectations or {}
    mp = mp or {}
    oof = stage1_oof or {}
    total_values = _month_values(
        cpi_series.get(cm.TARGET_ALL, []), cm.TARGET_ALL
    )
    total_points = _month_points(total_values)
    component_values = {
        code: _month_values(cpi_series[code], code)
        for code in cm.BOTTOMUP_COMPONENTS
        if code in cpi_series
    }

    rows = []
    for source_row in ordered:
        meeting_at = meeting_datetime(source_row.meeting_date)
        m0 = calendar.next_unpublished_month(SOURCE, SERIES, meeting_at)
        latest = calendar.latest_published_month(SOURCE, SERIES, meeting_at)
        history_cut = _month_end(latest) if latest is not None else None
        # As-of снимок сезонности: только история до последнего опубликованного
        # месяца. Целевой снимок `seasonal` считается отдельно в `_target_path`
        # и в признаки не попадает.
        factors, history = _sa_factors(total_points, history_cut)

        entry = oof.get(source_row.meeting_date) or {}
        stage1_m0 = _optional(entry.get("m0"))
        stage1_m1 = _optional(entry.get("m1"))
        lags = [float(value) for _, value in history[-3:]]
        lags = [None] * (3 - len(lags)) + lags

        cbr_sa, cbr_sa_chg = _asof_pair(
            expectations.get(CBR_SA_SERIES)
            or expectations.get(CBR_SA_CHANNEL)
            or [],
            _calendar_available(
                calendar, CBR_SA_SOURCE, CBR_SA_SERIES, meeting_at
            ),
            "cbr_sa",
        )
        infl_expected, infl_expected_chg = _asof_pair(
            expectations.get(INFL_EXP_EXPECTED, []),
            _calendar_available(
                calendar, INFL_EXP_SOURCE, INFL_EXP_EXPECTED, meeting_at
            ),
            INFL_EXP_EXPECTED,
        )
        infl_observed, infl_observed_chg = _asof_pair(
            expectations.get(INFL_EXP_OBSERVED, []),
            _calendar_available(
                calendar, INFL_EXP_SOURCE, INFL_EXP_OBSERVED, meeting_at
            ),
            INFL_EXP_OBSERVED,
        )
        survey_available = _survey_available(source_row.meeting_date)
        price_exp, price_exp_chg = _asof_pair(
            mp.get(MP_PRICE_EXPECTATIONS, []),
            survey_available,
            MP_PRICE_EXPECTATIONS,
        )
        costs, costs_chg = _asof_pair(
            mp.get(MP_COSTS, []), survey_available, MP_COSTS
        )
        ibk, ibk_chg = _asof_pair(
            mp.get(MP_IBK, []), survey_available, MP_IBK
        )

        sa_available = int(
            bool(factors)
            and len(history) >= SA_MIN_HISTORY
            and stage1_m0 is not None
            and stage1_m1 is not None
        )
        features = {
            "stage1_m0": stage1_m0,
            "stage1_m1": stage1_m1,
            "stage1_m0_sa": (
                None
                if stage1_m0 is None or not factors
                else stage1_m0 - factors[m0.month]
            ),
            "stage1_m1_sa": (
                None
                if stage1_m1 is None or not factors
                else stage1_m1 - factors[shift_month(m0, 1).month]
            ),
            "sa_lag1": lags[0],
            "sa_lag2": lags[1],
            "sa_lag3": lags[2],
            "cbr_sa": cbr_sa,
            "cbr_sa_chg": cbr_sa_chg,
            "cbr_sa_available": int(cbr_sa is not None),
            "infl_exp_expected": infl_expected,
            "infl_exp_expected_chg": infl_expected_chg,
            "infl_exp_observed": infl_observed,
            "infl_exp_observed_chg": infl_observed_chg,
            "price_expectations": price_exp,
            "price_expectations_chg": price_exp_chg,
            "costs": costs,
            "costs_chg": costs_chg,
            "ibk": ibk,
            "ibk_chg": ibk_chg,
            **{
                name: _optional(source_row.features.get(name))
                for name in WEEKLY_FEATURES
            },
            "month": m0.month,
            "cpi_sa_available": sa_available,
        }

        path, components, seasonal, cum = _target_path(
            m0, total_values, component_values, calendar, as_of_at
        )
        rows.append(
            Stage2Row(
                meeting_date=source_row.meeting_date,
                m0=m0,
                features=features,
                sa_path=path,
                cum=cum,
                component_paths=components,
                seasonal=seasonal,
                seasonal_asof=factors,
            )
        )
    return rows


def assert_no_lookahead(rows: list, calendar: PublicationCalendar) -> None:
    """Контроль часов и согласованности пути после сборки строк.

    `m0` обязан совпадать с календарём, SA-путь — содержать ровно
    `M0…M0+5`, а `cum` — пересчитываться из пути. Нарушение — `AssertionError`:
    это ошибка сборки, а не данных.
    """
    for row in rows:
        expected = calendar.next_unpublished_month(
            SOURCE, SERIES, meeting_datetime(row.meeting_date)
        )
        if row.m0 != expected:
            raise AssertionError(
                f"утечка часов: заседание {row.meeting_date}, m0={row.m0}, "
                f"календарь даёт {expected}"
            )
        if (row.sa_path is None) != (row.cum is None):
            raise AssertionError(
                f"путь и cumulative не согласованы у {row.meeting_date}"
            )
        if row.sa_path is None:
            continue
        months = target_months(row.m0)
        if sorted(row.sa_path) != months:
            raise AssertionError(
                f"путь {row.meeting_date} не равен M0…M0+5: "
                f"{sorted(row.sa_path)}"
            )
        for horizon in HORIZONS:
            expected_cum = compound(
                [row.sa_path[month] for month in months[:horizon]]
            )
            if not math.isclose(
                row.cum[horizon], expected_cum, rel_tol=0.0, abs_tol=1e-9
            ):
                raise AssertionError(
                    f"cum h{horizon} у {row.meeting_date} не согласован "
                    "с путём"
                )


# ---------------------------------------------------------------------------
# Модели: direct CatBoost, bottom-up + NNLS, ансамбль
# ---------------------------------------------------------------------------


def _row_available(row: Stage2Row) -> bool:
    """Строка пригодна для обучения/прогноза: путь есть, SA и OOF доступны."""
    return (
        row.sa_path is not None
        and row.features.get("cpi_sa_available") == 1
    )


def _feature_matrix(rows: list) -> np.ndarray:
    """Матрица CatBoost: `month` — целый код категории, остальное float/NaN."""
    matrix = np.empty((len(rows), len(FEATURE_NAMES)), dtype=object)
    for position, name in enumerate(FEATURE_NAMES):
        if name == "month":
            matrix[:, position] = [int(row.features[name]) for row in rows]
        else:
            matrix[:, position] = [
                float("nan")
                if row.features.get(name) is None
                else float(row.features[name])
                for row in rows
            ]
    return matrix


def _catboost_model() -> CatBoostRegressor:
    return CatBoostRegressor(
        loss_function="RMSE",
        random_seed=SEED,
        verbose=False,
        allow_writing_files=False,
        cat_features=[FEATURE_NAMES.index("month")],
    )


def _direct_train(train: list) -> list:
    return [row for row in train if _row_available(row)]


def _bottomup_train(train: list) -> list:
    return [
        row
        for row in _direct_train(train)
        if all(code in row.component_paths for code in cm.BOTTOMUP_COMPONENTS)
    ]


def _fit_target(
    subset: list, matrix: np.ndarray, target: list, row: Stage2Row
) -> float:
    """Один CatBoost на цель `target`; прогноз для строки `row`."""
    values = np.asarray(target, dtype=float)
    if not np.all(np.isfinite(values)):
        raise cm.DegenerateWindowError(
            "stage 2: нечисловая метка в обучающем окне"
        )
    model = _catboost_model()
    try:
        model.fit(matrix, values)
    except CatBoostError as error:
        # Вырожденное окно (например, все метки равны) — пропуск фолда, а не
        # падение прогона: та же политика, что у `DegenerateWindowError`.
        raise cm.DegenerateWindowError(
            f"stage 2: CatBoost не обучился — {error}"
        ) from error
    return float(model.predict(_feature_matrix([row]))[0])


def _weights_row(row: Stage2Row, position: int) -> cm.Row:
    """`cm.Row` для `nnls_weights`: метки компонент и ИПЦ на горизонте."""
    month = shift_month(row.m0, position)
    labels = {
        code: row.component_paths[code][month]
        for code in cm.BOTTOMUP_COMPONENTS
    }
    labels[cm.TARGET_ALL] = row.sa_path[month]
    return cm.Row(month=row.m0, y=labels, features=row.features)


def fit_catboost_direct(train: list, row: Stage2Row) -> dict:
    """Direct CatBoost на каждый горизонт пути: `{месяц: SA m/m}`.

    Цель горизонта `k` — `s_{M0'+k}` обучающей строки, а не календарный месяц
    теста: путь каждой строки заканчивается на её собственном `M0+5`.
    """
    subset = _direct_train(train)
    if len(subset) < MIN_TRAIN:
        raise cm.DegenerateWindowError(
            f"direct: обучающее окно {len(subset)} строк короче {MIN_TRAIN}"
        )
    matrix = _feature_matrix(subset)
    path = {}
    months = target_months(row.m0)
    for position, month in enumerate(months):
        values = [
            item.sa_path[shift_month(item.m0, position)]
            for item in subset
        ]
        path[month] = _fit_target(subset, matrix, values, row)
    return path


def fit_catboost_bottomup(train: list, row: Stage2Row) -> dict:
    """Bottom-up: три компонентных CatBoost на горизонт + NNLS-веса."""
    subset = _bottomup_train(train)
    if len(subset) < MIN_TRAIN:
        raise cm.DegenerateWindowError(
            f"bottom-up: обучающее окно {len(subset)} строк короче {MIN_TRAIN}"
        )
    matrix = _feature_matrix(subset)
    path = {}
    months = target_months(row.m0)
    for position, month in enumerate(months):
        weights = cm.nnls_weights(
            [_weights_row(item, position) for item in subset]
        )
        total = 0.0
        for index, code in enumerate(cm.BOTTOMUP_COMPONENTS):
            values = [
                item.component_paths[code][shift_month(item.m0, position)]
                for item in subset
            ]
            total += weights[index] * _fit_target(
                subset, matrix, values, row
            )
        path[month] = float(total)
    return path


def fit_ensemble(train: list, row: Stage2Row) -> dict:
    """Ансамбль 0,5/0,5 direct и bottom-up по каждому месяцу пути."""
    direct = fit_catboost_direct(train, row)
    bottomup = fit_catboost_bottomup(train, row)
    return {
        month: 0.5 * (direct[month] + bottomup[month])
        for month in direct
    }


BASE_MODELS = {
    "catboost_direct": fit_catboost_direct,
    "catboost_bottomup": fit_catboost_bottomup,
}
MODELS = {**BASE_MODELS, "ensemble": fit_ensemble}


def fit_predict(train: list, row: Stage2Row, model: str) -> dict:
    """Прогноз модели `model` по SA-пути: `{месяц: SA m/m}`."""
    if model not in MODELS:
        raise ValueError(
            f"неизвестная модель {model!r}: доступны {sorted(MODELS)}"
        )
    return MODELS[model](train, row)


# ---------------------------------------------------------------------------
# Walk-forward и оценка
# ---------------------------------------------------------------------------


def _ensemble_predictions(predictions: list) -> list:
    """Пост-фактум ансамбль 0,5/0,5 из direct и bottom-up без повторного фита."""
    direct = {
        (item.meeting_date, item.target_month): item
        for item in predictions
        if item.model == "catboost_direct"
    }
    bottomup = {
        (item.meeting_date, item.target_month): item
        for item in predictions
        if item.model == "catboost_bottomup"
    }
    ensemble = []
    for key in sorted(set(direct) & set(bottomup)):
        first = direct[key]
        second = bottomup[key]
        ensemble.append(
            Stage2Prediction(
                meeting_date=first.meeting_date,
                target_month=first.target_month,
                model="ensemble",
                horizon=first.horizon,
                y_true=first.y_true,
                y_pred=0.5 * (first.y_pred + second.y_pred),
                cpi_sa_available=min(
                    first.cpi_sa_available, second.cpi_sa_available
                ),
            )
        )
    return ensemble


def target_published(
    row: Stage2Row, calendar: PublicationCalendar, as_of
) -> bool:
    """Опубликован ли конец целевого пути строки (`M0+5`) к `as_of`.

    `as_of` — `date` (тогда берётся 13:30 МСК) или наивный `datetime`.
    Нужен для обучения: train-строка, чей целевой конец ещё не вышел к
    тестовому заседанию, информационно недоступна и в обучение не входит.
    """
    if row.sa_path is None:
        return False
    moment = (
        as_of if isinstance(as_of, datetime) else meeting_datetime(as_of)
    )
    end = shift_month(row.m0, PATH_MONTHS - 1)
    return calendar.is_available(SOURCE, SERIES, _month_end(end), moment)


def walk_forward(
    rows: list,
    start: int = DEFAULT_START,
    models: Optional[dict] = None,
    calendar: Optional[PublicationCalendar] = None,
    skipped: Optional[dict] = None,
) -> list:
    """Walk-forward по заседаниям: train `rows[:i]`, прогноз `rows[i]`.

    Обучающее окно — только строки с OOF stage 1 и достаточной SA-историей
    (`cpi_sa_available=1`), чей целевой конец `M0+5` ОПУБЛИКОВАН к тестовому
    заседанию по `PublicationCalendar`: иначе метки обучения заглядывали бы
    в будущее теста. Строка без целевого пути (текущее/будущее заседание)
    пропускается. `DegenerateWindowError` модели — пропуск фолда этой модели.

    `calendar` обязателен: без него фильтр публикаций не построить (ValueError).
    `skipped` — необязательный счётчик причин пропуска (заполняется
    вызывающим, как в `cpi_monthly.walk_forward`): `fold_target_unavailable`,
    `fold_train_below_min`, `train_rows_excluded_unpublished`,
    `<model>:degenerate`, `<model>:prediction_missing`. По умолчанию direct и
    bottom-up считаются один раз, ансамбль — их среднее (без повторного фита).
    """
    if start < 1:
        raise ValueError(f"walk-forward: start={start} — меньше одной строки")
    if calendar is None:
        raise ValueError(
            "walk-forward: нужен PublicationCalendar — без него нельзя "
            "проверить публикацию целевого конца train-строк"
        )
    for previous, current in zip(rows, rows[1:]):
        if previous.meeting_date >= current.meeting_date:
            raise ValueError(
                "walk-forward: строки не упорядочены по meeting_date "
                f"({previous.meeting_date} >= {current.meeting_date})"
            )
    counts = skipped if skipped is not None else {}

    def bump(key: str) -> None:
        counts[key] = counts.get(key, 0) + 1

    selected = dict(BASE_MODELS) if models is None else dict(models)
    predictions = []
    for index in range(start, len(rows)):
        row = rows[index]
        if not _row_available(row):
            bump("fold_target_unavailable")
            continue
        test_at = meeting_datetime(row.meeting_date)
        train = [
            item
            for item in rows[:index]
            if target_published(item, calendar, test_at)
        ]
        excluded = index - len(train)
        if excluded:
            key = "train_rows_excluded_unpublished"
            counts[key] = counts.get(key, 0) + excluded
        if len([item for item in train if _row_available(item)]) < MIN_TRAIN:
            bump("fold_train_below_min")
            continue
        months = target_months(row.m0)
        for name, fit in selected.items():
            try:
                values = fit(train, row)
            except cm.DegenerateWindowError:
                bump(f"{name}:degenerate")
                continue
            for position, month in enumerate(months):
                predicted = values.get(month)
                actual = row.sa_path.get(month)
                if predicted is None or actual is None:
                    bump(f"{name}:prediction_missing")
                    continue
                if not math.isfinite(float(predicted)):
                    bump(f"{name}:prediction_missing")
                    continue
                predictions.append(
                    Stage2Prediction(
                        meeting_date=row.meeting_date,
                        target_month=month,
                        model=name,
                        horizon=position + 1,
                        y_true=float(actual),
                        y_pred=float(predicted),
                        cpi_sa_available=int(
                            row.features.get("cpi_sa_available", 0)
                        ),
                    )
                )
    if models is None:
        predictions.extend(_ensemble_predictions(predictions))
    return predictions


def _cumulative_by_meeting(
    predictions: list, model: str, horizon: int
) -> dict:
    """`{заседание: (факт_cum, прогноз_cum)}` из первых `horizon` месяцев."""
    grouped: dict = {}
    for item in predictions:
        if item.model != model or item.horizon > horizon:
            continue
        grouped.setdefault(item.meeting_date, {})[item.horizon] = item
    result = {}
    for meeting, items in grouped.items():
        if not all(position in items for position in range(1, horizon + 1)):
            continue
        true_path = [
            items[position].y_true for position in range(1, horizon + 1)
        ]
        pred_path = [
            items[position].y_pred for position in range(1, horizon + 1)
        ]
        result[meeting] = (compound(true_path), compound(pred_path))
    return result


def _mde_ratio(err_a: list, err_b: list) -> float:
    """MDE отношения RMSE (как в `cpi_monthly_v3`), при n < 2 — NaN."""
    if len(err_a) < 2:
        return float("nan")
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    sd = float(np.std(differences, ddof=1))
    rmse_a = eu.rmse(err_a)
    rmse_b = eu.rmse(err_b)
    mde_mse = 2.802 * sd / math.sqrt(len(differences))
    return mde_mse / (rmse_a + rmse_b) if (rmse_a + rmse_b) else float("nan")


def evaluate(predictions: list) -> dict:
    """RMSE/MAE пути и cumulative h1/h3/h6, DM/CI и правило ансамбля.

    `summary` — по модели × горизонту пути (1…6), `cumulative` — по модели ×
    h ∈ {1, 3, 6}; cumulative считается компаундированием ТОГО ЖЕ
    предсказанного пути, поэтому согласован с ним по построению. `pairs` —
    DM-HLN и блочный бутстрап по парам `PAIR_SPECS` на общих заседаниях.
    `ensemble[h].not_worse` — предрегистрированное правило «ансамбль только
    если не хуже лучшей одиночной модели» на тех же фолдах.
    """
    models = sorted({item.model for item in predictions})
    summary = {}
    for model in models:
        summary[model] = {}
        for horizon in range(1, PATH_MONTHS + 1):
            group = [
                item
                for item in predictions
                if item.model == model and item.horizon == horizon
            ]
            if not group:
                summary[model][horizon] = {"n": 0}
                continue
            errors = [item.y_pred - item.y_true for item in group]
            summary[model][horizon] = {
                "n": len(group),
                "rmse": eu.rmse(errors),
                "mae": eu.mae(errors),
            }

    cumulative = {}
    for model in models:
        cumulative[model] = {}
        for horizon in HORIZONS:
            values = _cumulative_by_meeting(predictions, model, horizon)
            if not values:
                cumulative[model][horizon] = {"n": 0}
                continue
            errors = [
                predicted - actual for actual, predicted in values.values()
            ]
            cumulative[model][horizon] = {
                "n": len(values),
                "rmse": eu.rmse(errors),
                "mae": eu.mae(errors),
                "meetings": [
                    meeting.isoformat() for meeting in sorted(values)
                ],
            }

    pairs = {}
    for left, right in PAIR_SPECS:
        for horizon in HORIZONS:
            key = f"{left} vs {right}:h{horizon}"
            by_left = _cumulative_by_meeting(predictions, left, horizon)
            by_right = _cumulative_by_meeting(predictions, right, horizon)
            common = sorted(set(by_left) & set(by_right))
            if len(common) < 2:
                pairs[key] = {"n": len(common)}
                continue
            err_left = [by_left[day][1] - by_left[day][0] for day in common]
            err_right = [by_right[day][1] - by_right[day][0] for day in common]
            dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, 1)
            boot = eu.block_bootstrap_rmse_diff(
                err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED
            )
            pairs[key] = {
                "n": len(common),
                "rmse_left": eu.rmse(err_left),
                "rmse_right": eu.rmse(err_right),
                "delta": boot.rmse_diff,
                "delta_ci": [boot.low, boot.high],
                "dm_stat": dm_stat,
                "dm_p": dm_p,
                "mde_ratio": _mde_ratio(err_left, err_right),
            }

    ensemble = {}
    singles = ("catboost_direct", "catboost_bottomup")
    for horizon in HORIZONS:
        available = all(
            name in cumulative
            and cumulative[name][horizon].get("n")
            for name in (*singles, "ensemble")
        )
        if not available:
            ensemble[horizon] = {"n": 0, "not_worse": None}
            continue
        groups = {
            name: _cumulative_by_meeting(predictions, name, horizon)
            for name in (*singles, "ensemble")
        }
        common = sorted(
            set(groups["ensemble"])
            & set(groups["catboost_direct"])
            & set(groups["catboost_bottomup"])
        )
        if len(common) < 2:
            ensemble[horizon] = {"n": len(common), "not_worse": None}
            continue
        errors = {
            name: [
                groups[name][day][1] - groups[name][day][0]
                for day in common
            ]
            for name in groups
        }
        rmse = {name: eu.rmse(values) for name, values in errors.items()}
        best_single = min(rmse[singles[0]], rmse[singles[1]])
        ensemble[horizon] = {
            "n": len(common),
            "rmse": rmse["ensemble"],
            "best_single": best_single,
            "not_worse": rmse["ensemble"] <= best_single + 1e-12,
        }
    return {
        "summary": summary,
        "cumulative": cumulative,
        "pairs": pairs,
        "ensemble": ensemble,
    }


# ---------------------------------------------------------------------------
# NSA back-transform и Gate 2
# ---------------------------------------------------------------------------


def nsa_backtransform(
    row: Stage2Row, sa_path: dict, seasonal: Optional[dict]
) -> dict:
    """SA-путь → NSA m/m: `SA + сезонный фактор месяца`.

    Снимок факторов передаётся явно и не выбирается внутри:

    - **прогноз** (Gate 2, `nsa_backtest`) — `row.seasonal_asof`, снимок
      заседания; иначе будущие факторы `M0+5` попадали бы в forecast;
    - **факт** (`actual_nsa_path`) — `row.seasonal`, снимок конца целевого
      пути, который и породил `sa_path`.

    Пустой/`None` снимок — `ValueError`: молчаливый fallback на другой
    снимок и был бы утечкой.
    """
    if not seasonal:
        raise ValueError(
            f"NSA back-transform {row.meeting_date}: пустой сезонный снимок"
        )
    return {
        month: float(value) + seasonal[month.month]
        for month, value in sa_path.items()
    }


def actual_nsa_path(row: Stage2Row) -> dict:
    """Фактический NSA-путь целевых месяцев (SA-путь + целевой снимок)."""
    return nsa_backtransform(row, row.sa_path, row.seasonal)


def _nsa_cum(path: dict, months: list, horizon: int) -> float:
    return compound([path[month] for month in months[:horizon]])


def nsa_backtest(
    predictions: list, rows: list, model: str = GATE2_MODEL
) -> dict:
    """NSA back-transform cumulative h1/h3/h6: RMSE/MAE против факта.

    Прогноз back-transform использует as-of снимок `seasonal_asof`; факт —
    целевой `seasonal`. `skipped` — счётчик причин, почему заседание не вошло
    в сверку (`row_missing`, `no_target_path`, `no_seasonal_asof`,
    `path_incomplete`), чтобы пропуски не были молчаливыми.
    """
    rows_by_meeting = {row.meeting_date: row for row in rows}
    grouped: dict = {}
    for item in predictions:
        if item.model != model:
            continue
        grouped.setdefault(item.meeting_date, {})[item.target_month] = item
    errors = {horizon: [] for horizon in HORIZONS}
    skipped = {
        "row_missing": 0,
        "no_target_path": 0,
        "no_seasonal_asof": 0,
        "path_incomplete": 0,
    }
    for meeting, path in grouped.items():
        row = rows_by_meeting.get(meeting)
        if row is None:
            skipped["row_missing"] += 1
            continue
        if row.sa_path is None or not row.seasonal:
            skipped["no_target_path"] += 1
            continue
        if not row.seasonal_asof:
            skipped["no_seasonal_asof"] += 1
            continue
        months = target_months(row.m0)
        if any(month not in path for month in months):
            skipped["path_incomplete"] += 1
            continue
        predicted = nsa_backtransform(
            row,
            {month: path[month].y_pred for month in months},
            row.seasonal_asof,
        )
        actual = actual_nsa_path(row)
        for horizon in HORIZONS:
            errors[horizon].append(
                _nsa_cum(predicted, months, horizon)
                - _nsa_cum(actual, months, horizon)
            )
    result = {}
    for horizon in HORIZONS:
        values = errors[horizon]
        if not values:
            result[f"h{horizon}"] = {"n": 0}
            continue
        result[f"h{horizon}"] = {
            "n": len(values),
            "rmse": eu.rmse(values),
            "mae": eu.mae(values),
        }
    result["skipped"] = skipped
    return result


def _cbr_sa_value(
    points: list,
    month: date,
    calendar: PublicationCalendar,
    as_of_at: datetime,
) -> Optional[float]:
    """Официальный SA ЦБ за `month`, если он опубликован к `as_of`."""
    for day, value in points or []:
        if _month_of(day) != month:
            continue
        available = calendar.is_available(
            CBR_SA_SOURCE, CBR_SA_SERIES, month, as_of_at
        )
        if available:
            return float(value)
    return None


def cbr_sa_backtest(
    predictions: list,
    rows: list,
    cbr_sa_points: list,
    calendar: PublicationCalendar,
    as_of: date,
    model: str = GATE2_MODEL,
) -> dict:
    """SA-путь против официального SA ЦБ по месяцам `M0…M0+5`.

    Это отдельная сверка спеки §8.3: SA-путь сравнивается с SA ЦБ, а не с
    live3. Точки ЦБ берутся только опубликованные к `as_of` (календарь
    `cbr/cpi_sa`), поэтому для свежего заседания сравнение короче пути.
    `skipped` — счётчик причин (`row_missing`, `path_incomplete`,
    `cbr_sa_missing`), чтобы пропуски не были молчаливыми.
    """
    as_of_at = meeting_datetime(as_of)
    rows_by_meeting = {row.meeting_date: row for row in rows}
    grouped: dict = {}
    for item in predictions:
        if item.model != model:
            continue
        grouped.setdefault(item.meeting_date, {})[item.target_month] = item
    errors = {position: [] for position in range(1, PATH_MONTHS + 1)}
    skipped = {
        "row_missing": 0,
        "path_incomplete": 0,
        "cbr_sa_missing": 0,
    }
    for meeting, path in grouped.items():
        row = rows_by_meeting.get(meeting)
        if row is None:
            skipped["row_missing"] += 1
            continue
        months = target_months(row.m0)
        if any(month not in path for month in months):
            skipped["path_incomplete"] += 1
            continue
        for position, month in enumerate(months):
            official = _cbr_sa_value(
                cbr_sa_points, month, calendar, as_of_at
            )
            if official is None:
                skipped["cbr_sa_missing"] += 1
                continue
            errors[position + 1].append(path[month].y_pred - official)
    result = {}
    for horizon in range(1, PATH_MONTHS + 1):
        values = errors[horizon]
        if not values:
            result[f"h{horizon}"] = {"n": 0}
            continue
        result[f"h{horizon}"] = {
            "n": len(values),
            "rmse": eu.rmse(values),
            "mae": eu.mae(values),
        }
    result["skipped"] = skipped
    return result


def _normalize_live3_path(value) -> dict:
    """Нормализует значение live3-кэша: `{h1, h3, h6, origin}`."""
    entry = {"h1": None, "h3": None, "h6": None, "origin": None}
    if value is None:
        return entry
    if isinstance(value, dict):
        for horizon in HORIZONS:
            key = f"h{horizon}"
            raw = value.get(key)
            entry[key] = None if raw is None else float(raw)
        origin = value.get("origin")
        if isinstance(origin, str):
            origin = date.fromisoformat(origin)
        entry["origin"] = origin
        return entry
    entry["h1"] = float(value)
    return entry


def live3_paths_features(meetings: list, as_of: date) -> dict:
    """`cpi_live3_horizons` h1/h3/h6 и origin для заседаний (read-only)."""
    from models.cpi import cpi_live3_horizons as cph

    rows_by_month, _skipped = cph.load_rows(as_of)
    if not rows_by_month:
        raise ValueError("cpi_live3_horizons: пустая feature-вселенная")
    features = cph.build_cpi_features(sorted(set(meetings)), rows_by_month)
    return {
        day: {
            "h1": item.get("cpi_v3_h1"),
            "h3": item.get("cpi_v3_h3"),
            "h6": item.get("cpi_v3_h6"),
            "origin": item.get("cpi_v3_origin"),
        }
        for day, item in features.items()
    }


def load_live3_paths(
    meetings: list,
    as_of: date,
    cache_path: Optional[Path] = None,
    builder=None,
) -> dict:
    """`cpi_live3_horizons` h1/h3/h6 по заседаниям с кэшем на диске.

    Кэш — JSON `{as_of, meetings, values}`; переиспользуется только при
    совпадении `as_of` и списка заседаний. Битый JSON — `ValueError`.
    """
    ordered = sorted(set(meetings))
    keys = [day.isoformat() for day in ordered]
    path = Path(cache_path) if cache_path is not None else None
    if path is not None and path.exists():
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("as_of") == as_of.isoformat()
            and payload.get("meetings") == keys
        ):
            return {
                date.fromisoformat(day): _normalize_live3_path(value)
                for day, value in payload.get("values", {}).items()
            }
    values = (builder or live3_paths_features)(ordered, as_of)
    result = {
        day: _normalize_live3_path(values.get(day)) for day in ordered
    }
    if path is not None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "as_of": as_of.isoformat(),
                    "meetings": keys,
                    "values": {
                        day.isoformat(): {
                            **{
                                f"h{horizon}": entry[f"h{horizon}"]
                                for horizon in HORIZONS
                            },
                            "origin": (
                                None
                                if entry["origin"] is None
                                else entry["origin"].isoformat()
                            ),
                        }
                        for day, entry in result.items()
                    },
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return result


def gate2_report(
    predictions: list,
    rows: list,
    live3_values: dict,
    model: str = GATE2_MODEL,
) -> dict:
    """Gate 2: NSA back-transform cumulative h1/h3/h6 против live3.

    Сравниваются только заседания, где origin live3 даёт тот же `M0`
    (`origin + 1 = M0`): у live3 фиксированные часы (8-е число), у stage 2 —
    точный календарь публикаций. RMSE/MAE, DM-HLN и блочный бутстрап — по
    каждому h; `PASS`, если все три RMSE не больше live3 × 1.05
    (предрегистрация плана; в спеке допуска нет). `skipped` — счётчик причин,
    почему заседание не вошло в общие фолды.
    """
    rows_by_meeting = {row.meeting_date: row for row in rows}
    grouped: dict = {}
    for item in predictions:
        if item.model != model:
            continue
        grouped.setdefault(item.meeting_date, {})[item.target_month] = item
    common = []
    excluded = []
    skipped = {
        "row_missing": 0,
        "no_target_path": 0,
        "no_seasonal_asof": 0,
        "live3_missing": 0,
        "path_incomplete": 0,
    }
    for meeting in sorted(grouped):
        row = rows_by_meeting.get(meeting)
        if row is None:
            skipped["row_missing"] += 1
            continue
        if row.sa_path is None or not row.seasonal:
            skipped["no_target_path"] += 1
            continue
        if not row.seasonal_asof:
            skipped["no_seasonal_asof"] += 1
            continue
        entry = _normalize_live3_path(live3_values.get(meeting))
        if any(entry[f"h{horizon}"] is None for horizon in HORIZONS):
            skipped["live3_missing"] += 1
            continue
        if (
            entry["origin"] is not None
            and shift_month(entry["origin"], 1) != row.m0
        ):
            excluded.append(meeting.isoformat())
            continue
        months = target_months(row.m0)
        if any(month not in grouped[meeting] for month in months):
            skipped["path_incomplete"] += 1
            continue
        common.append(meeting)
    base = {
        "model": model,
        "baseline": GATE2_BASELINE,
        "n": len(common),
        "meetings": [meeting.isoformat() for meeting in common],
        "excluded_target_mismatch": sorted(excluded),
        "skipped": skipped,
    }
    if len(common) < 2:
        return {**base, "decision": "INSUFFICIENT", "horizons": {}}
    predicted = {horizon: {} for horizon in HORIZONS}
    actual = {horizon: {} for horizon in HORIZONS}
    for meeting in common:
        row = rows_by_meeting[meeting]
        months = target_months(row.m0)
        path = {
            month: grouped[meeting][month].y_pred for month in months
        }
        back = nsa_backtransform(row, path, row.seasonal_asof)
        fact = actual_nsa_path(row)
        for horizon in HORIZONS:
            predicted[horizon][meeting] = _nsa_cum(back, months, horizon)
            actual[horizon][meeting] = _nsa_cum(fact, months, horizon)
    horizons = {}
    passed = True
    for horizon in HORIZONS:
        err_stage = []
        err_live = []
        for meeting in common:
            fact = actual[horizon][meeting]
            err_stage.append(predicted[horizon][meeting] - fact)
            err_live.append(
                _normalize_live3_path(live3_values[meeting])[
                    f"h{horizon}"
                ]
                - fact
            )
        rmse_stage = eu.rmse(err_stage)
        rmse_live = eu.rmse(err_live)
        threshold = rmse_live * GATE2_TOLERANCE
        dm_stat, dm_p = eu.diebold_mariano(err_stage, err_live, 1)
        boot = eu.block_bootstrap_rmse_diff(
            err_stage, err_live, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED
        )
        ok = rmse_stage <= threshold
        passed = passed and ok
        horizons[f"h{horizon}"] = {
            "n": len(common),
            "rmse_stage2": rmse_stage,
            "mae_stage2": eu.mae(err_stage),
            "rmse_live3": rmse_live,
            "mae_live3": eu.mae(err_live),
            "threshold": threshold,
            "ratio": rmse_stage / rmse_live if rmse_live else float("nan"),
            "delta": boot.rmse_diff,
            "delta_ci": [boot.low, boot.high],
            "dm_stat": dm_stat,
            "dm_p": dm_p,
            "decision": "PASS" if ok else "FAIL",
        }
    return {
        **base,
        "horizons": horizons,
        "decision": "PASS" if passed else "FAIL",
    }


# ---------------------------------------------------------------------------
# Stage 1 OOF
# ---------------------------------------------------------------------------


def stage1_oof_from_predictions(
    predictions: list, model: str = "catboost_direct"
) -> dict:
    """`{заседание: {"m0", "m1"}}` из fold-предсказаний stage 1."""
    result: dict = {}
    for item in predictions:
        if item.model != model:
            continue
        entry = result.setdefault(item.meeting_date, {})
        if item.target == "y0":
            entry["m0"] = float(item.y_pred)
        elif item.target == "y1":
            entry["m1"] = float(item.y_pred)
    return result


def build_stage1_oof(
    stage1_rows: list, start: int = stage1.DEFAULT_START
) -> dict:
    """OOF stage 1 по уже собранным строкам (CatBoost direct, как Gate 1)."""
    predictions = stage1.walk_forward(
        stage1_rows,
        start,
        models={"catboost_direct": stage1.fit_catboost},
    )
    return stage1_oof_from_predictions(predictions)


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------

PREDICTIONS_NAME = "stage2_predictions.csv"
RESULT_NAME = "stage2_result.json"
REPORT_NAME = "stage2_report.md"
ARTIFACT_NAMES = (PREDICTIONS_NAME, RESULT_NAME, REPORT_NAME)
PREDICTIONS_COLUMNS = (
    "meeting_date",
    "target_month",
    "model",
    "horizon",
    "y_true",
    "y_pred",
    "cpi_sa_available",
)


def write_predictions(path: Path, predictions: list) -> None:
    """Fold-предсказания в CSV: заседание, месяц пути, модель и горизонт."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(PREDICTIONS_COLUMNS)
        for item in predictions:
            writer.writerow(
                [
                    item.meeting_date.isoformat(),
                    item.target_month.isoformat(),
                    item.model,
                    item.horizon,
                    f"{item.y_true:.6f}",
                    f"{item.y_pred:.6f}",
                    item.cpi_sa_available,
                ]
            )


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_report(path: Path, payload: dict) -> None:
    """Markdown-отчёт прогона; числа не редактируются руками."""
    lines = ["# Stage 2 CPI SA-путь и cumulative h1/h3/h6 — отчёт прогона", ""]
    lines.append(
        "> `scripts/models/cpi/cpi_pipeline_stage2.py`; пункт "
        "`key-rate/phase-4-cpi-key-rate-pipeline`. Только чтение ClickHouse."
    )
    lines.append("")
    lines.append(f"- As-of: {payload.get('as_of')}")
    lines.append(
        f"- Заседаний: {payload.get('n_meetings', 0)}; прогнозов: "
        f"{payload.get('n_predictions', 0)}; start={payload.get('start')}"
    )
    if payload.get("first_meeting"):
        lines.append(
            f"- Окно: {payload['first_meeting']} … {payload['last_meeting']}"
        )
    lines.append(f"- Seed: {payload.get('seed')}")
    lines.append(f"- MIN_TRAIN: {payload.get('min_train', MIN_TRAIN)}")
    skipped = payload.get("skipped") or {}
    if skipped:
        lines.append(
            "- Пропуски walk-forward: "
            + ", ".join(
                f"`{name}`={count}"
                for name, count in sorted(skipped.items())
                if count
            )
        )
    lines.append("")
    lines.append("## SA-путь (m/m, п.п.)")
    lines.append("")
    lines.append("| Модель | Горизонт | n | RMSE | MAE |")
    lines.append("|---|---:|---:|---:|---:|")
    for model, by_horizon in payload.get("summary", {}).items():
        for horizon, item in sorted(by_horizon.items()):
            if not item.get("n"):
                continue
            lines.append(
                f"| {model} | {horizon} | {item['n']} | "
                f"{item['rmse']:.6f} | {item['mae']:.6f} |"
            )
    lines.append("")
    lines.append("## Cumulative SA h1/h3/h6")
    lines.append("")
    lines.append("| Модель | h | n | RMSE | MAE |")
    lines.append("|---|---:|---:|---:|---:|")
    for model, by_horizon in payload.get("cumulative", {}).items():
        for horizon, item in sorted(by_horizon.items()):
            if not item.get("n"):
                continue
            lines.append(
                f"| {model} | h{horizon} | {item['n']} | "
                f"{item['rmse']:.6f} | {item['mae']:.6f} |"
            )
    lines.append("")
    lines.append("## Ансамбль 0,5/0,5 (только если не хуже)")
    lines.append("")
    for horizon, item in sorted(payload.get("ensemble", {}).items()):
        if item.get("not_worse") is None:
            lines.append(f"- h{horizon}: недостаточно общих фолдов")
            continue
        verdict = "не хуже" if item["not_worse"] else "хуже"
        lines.append(
            f"- h{horizon}: ансамбль {item['rmse']:.6f} vs лучшая "
            f"одиночная {item['best_single']:.6f} — {verdict}"
        )
    lines.append("")
    lines.append("## SA-путь vs официальный SA ЦБ (диагностика)")
    lines.append("")
    cbr_sa = payload.get("cbr_sa_backtest") or {}
    if not cbr_sa:
        lines.append("- Не рассчитан.")
    for horizon, item in sorted(cbr_sa.items()):
        if item.get("n"):
            lines.append(
                f"- {horizon}: n={item['n']}, RMSE {item['rmse']:.6f}, "
                f"MAE {item['mae']:.6f}"
            )
    cbr_skipped = {
        name: count
        for name, count in (cbr_sa.get("skipped") or {}).items()
        if count
    }
    if cbr_skipped:
        lines.append(
            "- Пропуски CBR-SA сверки: "
            + ", ".join(
                f"`{name}`={count}"
                for name, count in sorted(cbr_skipped.items())
            )
        )
    lines.append("")
    lines.append("## NSA back-transform (sanity)")
    lines.append("")
    backtest = payload.get("nsa_backtest") or {}
    if not backtest:
        lines.append("- Не рассчитан.")
    for horizon, item in sorted(backtest.items()):
        if item.get("n"):
            lines.append(
                f"- {horizon}: n={item['n']}, RMSE {item['rmse']:.6f}, "
                f"MAE {item['mae']:.6f}"
            )
    back_skipped = {
        name: count
        for name, count in (backtest.get("skipped") or {}).items()
        if count
    }
    if back_skipped:
        lines.append(
            "- Пропуски NSA-сверки: "
            + ", ".join(
                f"`{name}`={count}"
                for name, count in sorted(back_skipped.items())
            )
        )
    lines.append("")
    lines.append("## Gate 2 (NSA back-transform vs cpi_live3_horizons)")
    lines.append("")
    gate = payload.get("gate2")
    if not gate:
        lines.append("- Gate 2 не рассчитан (запуск без `--gate2`).")
    else:
        lines.append(
            f"- Модель: `{gate['model']}`; "
            f"baseline: `{gate['baseline']}`"
        )
        lines.append(f"- Общих фолдов: {gate['n']}")
        gate_skipped = {
            name: count
            for name, count in (gate.get("skipped") or {}).items()
            if count
        }
        if gate_skipped:
            lines.append(
                "- Пропуски Gate 2: "
                + ", ".join(
                    f"`{name}`={count}"
                    for name, count in sorted(gate_skipped.items())
                )
            )
        if gate.get("excluded_target_mismatch"):
            lines.append(
                "- Исключены (origin live3 + 1 ≠ `M0`): "
                + ", ".join(gate["excluded_target_mismatch"])
            )
        if gate["decision"] == "INSUFFICIENT":
            lines.append("- Решение: `INSUFFICIENT` (< 2 общих заседаний).")
        else:
            for name, item in sorted(gate["horizons"].items()):
                lines.append(
                    f"- {name}: stage 2 {item['rmse_stage2']:.6f} vs live3 "
                    f"{item['rmse_live3']:.6f} "
                    f"(порог {item['threshold']:.6f}, "
                    f"DM p={item['dm_p']:.4f}) — {item['decision']}"
                )
            lines.append(f"- Решение: **{gate['decision']}**")
    lines.append("")
    lines.append("## Признаки")
    lines.append("")
    lines.append("`" + "`, `".join(payload.get("features", [])) + "`")
    lines.append("")
    lines.append("## Что не проверено")
    lines.append("")
    lines.append(
        "- SA-путь отдельно сверяется с официальным SA ЦБ (`cbr_sa`), а не "
        "с live3: `cbr_sa` — признак stage 2, не таргет."
    )
    lines.append(
        "- Ансамбль — заявленный кандидат; правило «только если не хуже» "
        "проверяется на тех же фолдах как предрегистрированная диагностика, "
        "а не как выбор модели по тесту."
    )
    lines.append(
        "- Ряды МП ЦБ не имеют точных дат публикации в экспериментальном "
        "календаре: используется консервативное правило "
        "`available_from_survey` (конец `P+1`)."
    )
    lines.append(
        "- Исторические даты публикации — fallback-правила календаря; доля "
        "`precision=fallback` в паспорт не вынесена."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _log_experiment(
    payload: dict,
    rows: list,
    predictions: list,
    paths: list,
    as_of: date,
) -> None:
    """Паспорт run (`ML_LOG=1`): ошибки журнала не роняют прогон."""
    try:
        logger = mlx.start_run(
            experiment="cpi_pipeline_stage2",
            run_name=(
                f"cpi_pipeline_stage2__asof{as_of.isoformat()}"
                f"__start{payload['start']}"
            ),
            config={
                "features": list(FEATURE_NAMES),
                "models": list(MODELS),
                "start": payload["start"],
                "seed": SEED,
            },
            data_asof=as_of,
            sample_meta={
                "rows": len(rows),
                "predictions": len(predictions),
                "first_meeting": payload.get("first_meeting"),
                "last_meeting": payload.get("last_meeting"),
            },
            sample=[
                {
                    "meeting_date": row.meeting_date.isoformat(),
                    "m0": row.m0.isoformat(),
                    "cpi_sa_available": row.features.get("cpi_sa_available"),
                    "sa_path": mlx.json_safe(row.sa_path),
                    "cum": row.cum,
                    "features": mlx.json_safe(row.features),
                }
                for row in rows
            ],
            spec_path=(
                "docs/superpowers/specs/"
                "2026-10-20-cpi-key-rate-pipeline-design.md"
            ),
            enabled=True,
        )
        logger.log_metrics(
            {
                "summary": payload["summary"],
                "cumulative": payload["cumulative"],
                "pairs": payload["pairs"],
                "ensemble": payload["ensemble"],
                "nsa_backtest": payload.get("nsa_backtest"),
                "gate2": payload.get("gate2"),
            }
        )
        logger.log_predictions(
            [
                mlx.PredictionRow(
                    model=item.model,
                    window=f"h{item.horizon}",
                    target_date=item.target_month,
                    predicted=item.y_pred,
                    actual=item.y_true,
                    unit="pp",
                    meta={
                        "meeting_date": item.meeting_date.isoformat(),
                        "cpi_sa_available": item.cpi_sa_available,
                    },
                )
                for item in predictions
            ]
        )
        logger.log_artifacts(paths)
        logger.finish(
            "finished",
            f"as_of={as_of}; rows={len(rows)}; predictions={len(predictions)}",
        )
    except Exception as exc:  # noqa: BLE001 — журнал best-effort, но не молчит
        # Явно сообщаем в stdout (паспорт run не записан), не глотаем ошибку:
        # это осознанная граница «артефакты важнее журнала». `BaseException`
        # (KeyboardInterrupt/SystemExit) сюда не попадает.
        print(
            f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(
    as_of: date,
    out_dir: Path,
    start: int = DEFAULT_START,
    meetings: Optional[list] = None,
    calendar: Optional[PublicationCalendar] = None,
    stage1_rows: Optional[list] = None,
    stage1_oof: Optional[dict] = None,
    cpi_series: Optional[dict] = None,
    expectations: Optional[dict] = None,
    mp: Optional[dict] = None,
    gate2: bool = False,
    live3_cache: Optional[Path] = None,
) -> dict:
    """Собрать строки stage 2, прогнать walk-forward и записать артефакты."""
    calendar = calendar if calendar is not None else load_calendar()
    all_meetings = load_meetings() if meetings is None else meetings
    selected = [day for day in sorted(set(all_meetings)) if day <= as_of]
    if not selected:
        raise SystemExit(f"нет заседаний ЦБ по {as_of.isoformat()}")
    if stage1_rows is None:
        stage1_rows = stage1.build_rows(selected, calendar, as_of)
    if stage1_oof is None:
        stage1_oof = build_stage1_oof(stage1_rows)
    cpi_series = load_cpi_series() if cpi_series is None else cpi_series
    expectations = (
        load_expectations() if expectations is None else expectations
    )
    mp = load_mp() if mp is None else mp
    rows = build_rows(
        stage1_rows,
        stage1_oof,
        cpi_series,
        expectations,
        mp,
        calendar,
        as_of,
    )
    assert_no_lookahead(rows, calendar)
    skipped: dict = {}
    predictions = walk_forward(
        rows, start, calendar=calendar, skipped=skipped
    )
    result = evaluate(predictions)
    cbr_sa_points = (
        expectations.get(CBR_SA_SERIES)
        or expectations.get(CBR_SA_CHANNEL)
        or []
    )
    payload = {
        "as_of": as_of.isoformat(),
        "start": start,
        "seed": SEED,
        "n_meetings": len(rows),
        "n_predictions": len(predictions),
        "first_meeting": rows[0].meeting_date.isoformat(),
        "last_meeting": rows[-1].meeting_date.isoformat(),
        "features": list(FEATURE_NAMES),
        "models": list(MODELS),
        "min_train": MIN_TRAIN,
        "skipped": skipped,
        "summary": result["summary"],
        "cumulative": result["cumulative"],
        "pairs": result["pairs"],
        "ensemble": result["ensemble"],
        "nsa_backtest": nsa_backtest(predictions, rows),
        "cbr_sa_backtest": cbr_sa_backtest(
            predictions, rows, cbr_sa_points, calendar, as_of
        ),
        "gate2": None,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    if gate2:
        cache_path = (
            Path(live3_cache)
            if live3_cache is not None
            else out_dir / LIVE3_CACHE_NAME
        )
        live3_values = load_live3_paths(
            [row.meeting_date for row in rows],
            as_of,
            cache_path=cache_path,
        )
        payload["gate2"] = gate2_report(predictions, rows, live3_values)

    predictions_path = out_dir / PREDICTIONS_NAME
    result_path = out_dir / RESULT_NAME
    report_path = out_dir / REPORT_NAME
    write_predictions(predictions_path, predictions)
    write_json(result_path, payload)
    write_report(report_path, payload)

    if os.environ.get("ML_LOG") == "1":
        _log_experiment(
            payload,
            rows,
            predictions,
            [predictions_path, result_path, report_path],
            as_of,
        )

    print(
        f"Заседаний {len(rows)} ({rows[0].meeting_date}…"
        f"{rows[-1].meeting_date}), прогнозов {len(predictions)}, "
        f"start={start}",
        flush=True,
    )
    for model, by_horizon in result["cumulative"].items():
        for horizon, item in sorted(by_horizon.items()):
            if item.get("n"):
                print(
                    f"  {model}/cum h{horizon}: n={item['n']}, "
                    f"RMSE {item['rmse']:.6f}, MAE {item['mae']:.6f}",
                    flush=True,
                )
    gate = payload.get("gate2")
    if gate:
        if gate["decision"] == "INSUFFICIENT":
            print(f"  Gate 2: INSUFFICIENT (n={gate['n']})", flush=True)
        else:
            print(
                f"  Gate 2: {gate['decision']} — "
                + ", ".join(
                    f"{name} {item['rmse_stage2']:.6f} vs "
                    f"{item['rmse_live3']:.6f}"
                    for name, item in sorted(gate["horizons"].items())
                ),
                flush=True,
            )
    print(
        f"Артефакты: {predictions_path}, {result_path}, {report_path}",
        flush=True,
    )
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Stage 2 CPI SA-путь M0…M0+5 и cumulative h1/h3/h6 "
            "(пункт key-rate/phase-4-cpi-key-rate-pipeline)"
        )
    )
    parser.add_argument(
        "--as-of",
        default=None,
        help="дата среза, YYYY-MM-DD (по умолчанию — сегодня)",
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cpi-pipeline-stage2"),
        help="каталог артефактов",
    )
    parser.add_argument("--start", type=int, default=DEFAULT_START)
    parser.add_argument(
        "--gate2",
        action="store_true",
        help=(
            "посчитать Gate 2: NSA back-transform cumulative h1/h3/h6 "
            "против cpi_live3_horizons"
        ),
    )
    parser.add_argument(
        "--live3-cache",
        default=None,
        help=(
            "файл кэша live3 (по умолчанию "
            "<out-dir>/stage2_live3_cache.json)"
        ),
    )
    args = parser.parse_args(argv)
    try:
        as_of = (
            date.fromisoformat(args.as_of) if args.as_of else date.today()
        )
    except ValueError:
        print(f"ошибка --as-of: {args.as_of!r}", file=sys.stderr)
        return 2
    run(
        as_of=as_of,
        out_dir=Path(args.out_dir),
        start=args.start,
        gate2=args.gate2,
        live3_cache=Path(args.live3_cache) if args.live3_cache else None,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
