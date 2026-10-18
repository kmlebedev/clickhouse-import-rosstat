#!/usr/bin/env python3
"""Панель заседаний ЦБ для классификатора шоков ключевой ставки (Task 2).

Спека: `docs/superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md`
§3–4. Срез даёт измерительную основу классификатора и НЕ трогает
`key_rate_model`:

* **метка явного шока** — `events_calendar.emergency` (Task 1) вместе с шагом
  решения из `cbr_key_rate`: `explicit_shock = emergency ∧ step_bp ≠ 0`
  (спека §3.2). Внеочередное заседание без изменения ставки остаётся
  `emergency=True`, `explicit_shock=False` — правило не должно молча ломаться;
* **панель заседаний** — строка на каждое РЕШЕНИЕ `key_rate_model`
  (`build_decisions`): уровни до/после, шаг, класс `y3`, флаг внеочередного,
  явная метка и вектор каналов;
* **as-of каналов** — та же консервативная конвенция, что у v0: месячная
  точка отчётного месяца M доступна в конце M+1
  (`key_rate_model.available_from_cpd`), дневная — в день наблюдения
  (`date <= meeting_date`). Точка, опубликованная после заседания, не
  участвует;
* **пропуски** — отсутствующая точка даёт `None`, а не ноль;
  `missing_count` считает отсутствующие БАЗОВЫЕ признаки строки (дельты —
  производные и счётчик не раздувают);
* **дельты `*_chg`** — к ПРЕДЫДУЩЕМУ ЗАСЕДАНИЮ панели, а не к предыдущему
  месяцу: шок — это сдвиг между решениями (спека §4.2). У первой строки
  дельты `None`: предыдущего решения в панели нет.

Каналы и преобразования (спека §4.2):

==========================  =========================================  =========
признак                     источник                                   единицы
==========================  =========================================  =========
``rate_before``             ``cbr_key_rate``                           %
``pi_saar``                 ``cbr_indicators_cpd`` «Базовый ИПЦ»       % годовых
``cpi_saar``                ``cbr_indicators_cpd`` «Все товары…»       % годовых
``infl_exp_expected``       ``cbr_infl_exp`` «ожидаемая инфляция»      %
``infl_exp_observed``       ``cbr_infl_exp`` «наблюдаемая инфляция»    %
``unemployment``            ``rosstat_labour_force_mes``               %
``wage_yoy``                ``rosstat_salaries_mes``                   % г/г
``credit_yoy``              ``cbr_credit_m2x`` «Кредит экономике»      % г/г
``m2_yoy``                  ``cbr_m2`` «М2 SA»                         % г/г
``budget_balance``          ``minfin_fed_bud_mes``                     % доходов
``usdrub_3m``               ``cbr_currency_usd``                       % (лог)
``ruania_spread``           ``cbr_ruonia.ruo``                         п.п.
``price_expectations``      ``macro_series``/``cbr_survey``            п.
``business_climate``        ``macro_series``/``cbr_survey``            п.
==========================  =========================================  =========

`pi_saar`/`cpi_saar` получаются годовым пересчётом месячного SA-темпа
(`key_rate_model.annualize_saar`); `wage_yoy`, `credit_yoy`, `m2_yoy` —
отношение уровня к точке того же месяца годом ранее; `budget_balance` —
сальдо за 12 месяцев / доходы за 12 месяцев; `usdrub_3m` —
`100·ln(P_t / P_{t−3 мес.})`; `ruania_spread` — RUONIA минус `rate_before`.

**Особенности дат источников.** `rosstat_salaries_mes` хранит дату 1-го числа
СЛЕДУЮЩЕГО за отчётным месяца (`salariesMesPoint.date`), поэтому загрузчик
приводит её к отчётному месяцу (`normalize_salary_points`). У `cbr_m2`
значение лежит в колонке `balance`, у `cbr_currency_usd` — в `price`, у
`cbr_ruonia` — в `ruo` (не `rui`): имена колонок взяты из DDL импортёров, а
SQL-константы переименовывают их в общий `value`. У Минфина отдельного ряда
«сальдо» нет — `budget_balance` считается из «Доходы, всего» и
«Расходы, всего» (значения — месячные потоки, не накопление).

**Task 3 — причинные веса и слой валидации (спека §5–6).** На каждом
заседании `t` статистики аномальной модели оцениваются ТОЛЬКО по прошлым
заседаниям:

1. признаки стандартизуются по медиане/IQR обучающего окна (`IQR=0` → std,
   затем 1; пропуск → 0 после стандартизации);
2. `LedoitWolf` даёт сжатую ковариацию, по ней считается расстояние
   Махаланобиса `d_t`;
3. `r_t = d_t / median(d_train)`, вес `w_t = 1/(1 + max(0, r_t − 1)²)`,
   клип `[0.05, 1]`;
4. первые `MIN_TRAIN` заседаний получают `w = 1`: статистик ещё нет.

`walk_forward_weights(rows, min_train)` возвращает as-of веса в порядке строк;
строка `t` не участвует в оценке своих статистик. `FEATURES_ALL` — основной
набор §4.2, `FEATURES_CORE` — проверка робастности на каналах, покрывающих
2014+.

Описательный supervised-слой — `LogisticRegression` (L2, `C=1.0`,
`class_weight='balanced'`, `max_iter=1000`) на ТЕХ ЖЕ стандартизованных
признаках, walk-forward с `MIN_TRAIN`: фолд без двух классов (в частности, без
положительных) пропускается как `None` и считается вызывающим. Метрики —
`classifier_metrics`: ROC-AUC, PR-AUC, Brier, precision/recall при пороге 0.5 и
матрица ошибок; при отсутствии положительных неопределённые метрики — явные
`None`, а не исключение. Веса НЕ берутся из классификатора: при горстке явных
шоков он описательный и служит проверкой отделимости (спека §6, §11).

Офлайн-тесты (`shock_classifier_test.py`) работают на синтетических CSV
`scripts/testdata/shock_classifier/`: `export_fixtures(directory)` пишет
детерминированный набор (8 заседаний 2021–2022, 2 внеочередных, разные даты
публикации каналов, `infl_exp_*` отсутствуют на первом заседании), а
`read_fixtures(directory)` читает его обратно. ClickHouse и сеть в тестах не
нужны.

Живой прогон (Task 5) читает ClickHouse только через `key_rate_model.ch_query`
(транспорт `scripts/extrapolation_aisc.py`); этот срез в ClickHouse не пишет.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import logging
import math
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
from sklearn.covariance import LedoitWolf
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score

ROOT = Path(__file__).resolve().parent.parent

# Модуль-сосед: переиспользуем выборку решений, as-of и разбор строк, а не
# копируем их. `key_rate_model` сам добавляет `scripts/` в `sys.path`.
sys.path.insert(0, str(ROOT / "scripts"))

import key_rate_model as krm

log = logging.getLogger(__name__)

# --- Константы среза (спека §3–4, §8) ----------------------------------------

# Первые MIN_TRAIN заседаний не имеют статистик аномальной модели (Task 3);
# значение фиксирует спека §5 и повторяется здесь, чтобы панель и отчёт
# ссылались на одно число.
MIN_TRAIN = 24

# Сид воспроизводимости (бутстрап/перестановки) и версия набора Task 3–4.
SEED = 20261019

# --- Константы аномальной модели и классификатора (спека §5–6) ---------------

# Клип причинного веса: ниже 0.05 наблюдение не опускается — вес мягко
# понижает вклад, а не исключает заседание (спека §5, §11).
WEIGHT_MIN = 0.05
WEIGHT_MAX = 1.0

# Гиперпараметры описательного классификатора фиксированы спекой §6; они не
# подбираются ни на какой выборке. L2 — умолчание `LogisticRegression` во всех
# поддерживаемых версиях scikit-learn (в 1.8+ явный `penalty='l2'` уже
# предупреждает об устаревании, а `l1_ratio=0` — та же L2).
CLASSIFIER_C = 1.0
CLASSIFIER_MAX_ITER = 1000
CLASSIFIER_THRESHOLD = 0.5

# Спека прогона — путь от корня репозитория для паспорта журнала (Task 5).
SPEC_PATH = "docs/superpowers/specs/2026-10-19-key-rate-shock-classifier-design.md"

# Артефакты §8. Имена объявлены здесь, хотя пишутся в Task 5: ранние выходы
# обязаны убирать устаревшие файлы, и список нужен целиком.
REPORT_NAME = "shock_classifier_report.md"
WEIGHTS_NAME = "shock_classifier_weights.csv"
FORECAST_NAME = "shock_classifier_forecast.json"
ARTIFACT_NAMES = (REPORT_NAME, WEIGHTS_NAME, FORECAST_NAME)

# Канонический title заседания — тот же, что у `key_rate_model`: ручные
# строки-триггеры сида лежат в категории `cbr` с другими title и отсекаются.
SAMPLE_TITLE = krm.SAMPLE_TITLE

# --- SQL источников ----------------------------------------------------------
#
# `FINAL` обязателен во всех запросах: таблицы — ReplacingMergeTree, и без него
# вытесненные копии строк дали бы дубли точек (и дубли заседаний).
# `FORMAT JSON` — требование транспорта: `ch_query` разбирает JSON-ответ.

MEETINGS_SQL = """
SELECT event_date, category, title, emergency
FROM events_calendar FINAL
WHERE category = 'cbr'
ORDER BY event_date
FORMAT JSON
"""

# Ряд ставки и резюме ЦБ переиспользуются у v0: правила `rate_before`/
# `rate_after` и сверки ярлыков уже проверены там.
KEY_RATE_SQL = krm.KEY_RATE_SQL
MP_SUMMARY_SQL = krm.MP_SUMMARY_SQL

# Стабильные имена рядов. Для CPD/инфляционных ожиданий/безработицы/Минфина
# это `name` таблицы; для `macro_series` — `series` при `source='cbr_survey'`.
CPD_CORE_SERIES = krm.CPD_CORE_SERIES
CPD_ALL_SERIES = "Все товары и услуги"
INFL_EXP_EXPECTED_SERIES = "ожидаемая инфляция"
INFL_EXP_OBSERVED_SERIES = "наблюдаемая инфляция"
UNEMPLOYMENT_SERIES = "Уровень безработицы"
SALARIES_SERIES = (
    "Среднемесячная номинальная начисленная заработная плата работников "
    "по полному кругу организаций"
)
CREDIT_SERIES = "Кредит экономике"
M2_SERIES = "М2 SA"
BUDGET_INCOME_SERIES = "Доходы, всего"
BUDGET_EXPENSE_SERIES = "Расходы, всего"
SURVEY_SOURCE = krm.SURVEY_SOURCE
PRICE_EXPECTATIONS_SERIES = "price_expectations_sa"
BUSINESS_CLIMATE_SERIES = "business_climate_sa"

# По одному SQL на ряд (спека §4.2): у каждого канала своя таблица, колонка
# значения и, где нужно, имя ряда. `value` — общее имя на выходе запроса.
CPD_CORE_SQL = f"""
SELECT date, value
FROM cbr_indicators_cpd FINAL
WHERE name = '{CPD_CORE_SERIES}'
ORDER BY date
FORMAT JSON
"""

CPD_ALL_SQL = f"""
SELECT date, value
FROM cbr_indicators_cpd FINAL
WHERE name = '{CPD_ALL_SERIES}'
ORDER BY date
FORMAT JSON
"""

INFL_EXP_EXPECTED_SQL = f"""
SELECT date, value
FROM cbr_infl_exp FINAL
WHERE name = '{INFL_EXP_EXPECTED_SERIES}'
ORDER BY date
FORMAT JSON
"""

INFL_EXP_OBSERVED_SQL = f"""
SELECT date, value
FROM cbr_infl_exp FINAL
WHERE name = '{INFL_EXP_OBSERVED_SERIES}'
ORDER BY date
FORMAT JSON
"""

UNEMPLOYMENT_SQL = f"""
SELECT date, value
FROM rosstat_labour_force_mes FINAL
WHERE name = '{UNEMPLOYMENT_SERIES}'
ORDER BY date
FORMAT JSON
"""

# Колонка зарплаты — `salary`, а не `value` (DDL `rosstat/salaries_mes.go`).
SALARIES_SQL = f"""
SELECT date, salary AS value
FROM rosstat_salaries_mes FINAL
WHERE name = '{SALARIES_SERIES}'
ORDER BY date
FORMAT JSON
"""

CREDIT_SQL = f"""
SELECT date, value
FROM cbr_credit_m2x FINAL
WHERE name = '{CREDIT_SERIES}'
ORDER BY date
FORMAT JSON
"""

# Колонка М2 — `balance` (DDL `cbr/m2.go`).
M2_SQL = f"""
SELECT date, balance AS value
FROM cbr_m2 FINAL
WHERE name = '{M2_SERIES}'
ORDER BY date
FORMAT JSON
"""

BUDGET_INCOME_SQL = f"""
SELECT date, value
FROM minfin_fed_bud_mes FINAL
WHERE name = '{BUDGET_INCOME_SERIES}'
ORDER BY date
FORMAT JSON
"""

BUDGET_EXPENSE_SQL = f"""
SELECT date, value
FROM minfin_fed_bud_mes FINAL
WHERE name = '{BUDGET_EXPENSE_SERIES}'
ORDER BY date
FORMAT JSON
"""

# Курс USD: одна колонка `price` (DDL `cbr/currency_usd.go`).
USDRUB_SQL = """
SELECT date, price AS value
FROM cbr_currency_usd FINAL
ORDER BY date
FORMAT JSON
"""

# RUONIA: DDL `cbr/ruonia.go` называет средневзвешенную ставку `ruo`
# (переменная разбора `rui` — не имя колонки).
RUONIA_SQL = """
SELECT date, ruo AS value
FROM cbr_ruonia FINAL
ORDER BY date
FORMAT JSON
"""

PRICE_EXPECTATIONS_SQL = f"""
SELECT date, value
FROM macro_series FINAL
WHERE source = '{SURVEY_SOURCE}' AND series = '{PRICE_EXPECTATIONS_SERIES}'
ORDER BY date
FORMAT JSON
"""

BUSINESS_CLIMATE_SQL = f"""
SELECT date, value
FROM macro_series FINAL
WHERE source = '{SURVEY_SOURCE}' AND series = '{BUSINESS_CLIMATE_SERIES}'
ORDER BY date
FORMAT JSON
"""

# Канал → SQL. Порядок задаёт порядок колонок фикстур и обхода загрузчика; он
# же используется в `fixture_channel_points`.
CHANNEL_SQL = {
    "pi": CPD_CORE_SQL,
    "cpi": CPD_ALL_SQL,
    "infl_exp_expected": INFL_EXP_EXPECTED_SQL,
    "infl_exp_observed": INFL_EXP_OBSERVED_SQL,
    "unemployment": UNEMPLOYMENT_SQL,
    "wage": SALARIES_SQL,
    "credit": CREDIT_SQL,
    "m2": M2_SQL,
    "budget_income": BUDGET_INCOME_SQL,
    "budget_expense": BUDGET_EXPENSE_SQL,
    "usdrub": USDRUB_SQL,
    "ruonia": RUONIA_SQL,
    "price_expectations": PRICE_EXPECTATIONS_SQL,
    "business_climate": BUSINESS_CLIMATE_SQL,
}
CHANNEL_KEYS = tuple(CHANNEL_SQL)

# --- Признаки панели (спека §4.2) --------------------------------------------

# Базовые признаки строки в порядке таблицы спеки. `rate_before` — уровень из
# решения; остальные — as-of каналы.
BASE_FEATURES = (
    "rate_before",
    "pi_saar",
    "cpi_saar",
    "infl_exp_expected",
    "infl_exp_observed",
    "unemployment",
    "wage_yoy",
    "credit_yoy",
    "m2_yoy",
    "budget_balance",
    "usdrub_3m",
    "ruania_spread",
    "price_expectations",
    "business_climate",
)

# Для этих признаков добавляется изменение к предыдущему заседанию панели.
DELTA_FEATURES = (
    "pi_saar",
    "cpi_saar",
    "infl_exp_expected",
    "infl_exp_observed",
    "unemployment",
    "wage_yoy",
    "price_expectations",
    "business_climate",
)

# Полный набор `all`: базовые признаки и их дельты.
FEATURES_ALL = BASE_FEATURES + tuple(f"{name}_chg" for name in DELTA_FEATURES)

# `core` — признаки, покрывающие весь период 2014+ (спека §4.2, проверка
# робастности). Дельты в `core` не входят: это уровни/темпы, а не сдвиги.
FEATURES_CORE = (
    "rate_before",
    "pi_saar",
    "cpi_saar",
    "wage_yoy",
    "m2_yoy",
    "budget_balance",
    "usdrub_3m",
    "business_climate",
)

FEATURE_SETS = {"all": FEATURES_ALL, "core": FEATURES_CORE}

# --- Фикстуры офлайн-тестов ---------------------------------------------------

FIXTURE_MEETINGS = "meetings.csv"
FIXTURE_KEY_RATE = "key_rate.csv"
FIXTURE_CPD = "cpd.csv"
FIXTURE_CHANNELS = "channels.csv"

FIXTURE_COLUMNS = {
    FIXTURE_MEETINGS: ("event_date", "category", "title", "emergency"),
    FIXTURE_KEY_RATE: ("date", "rate"),
    FIXTURE_CPD: ("name", "date", "value"),
    FIXTURE_CHANNELS: ("channel", "date", "value"),
}

# 8 канонических заседаний 2021–2022, из них 2 внеочередных (2022-02-28 и
# 2022-04-08 — реальные даты календаря ЦБ). Пары «дата, emergency».
FIXTURE_CANONICAL_MEETINGS = (
    (date(2021, 2, 12), False),
    (date(2021, 3, 19), False),
    (date(2021, 4, 23), False),
    (date(2021, 6, 11), False),
    (date(2021, 7, 23), False),
    (date(2022, 2, 28), True),
    (date(2022, 3, 18), False),
    (date(2022, 4, 8), True),
)

# Ручные строки-триггеры: одна делит дату с каноническим заседанием и несёт
# emergency=1 (фильтр по title обязан её отсечь, а не «поднять» флаг), вторая
# имеет уникальную дату и проверяет, что без фильтра в панели было бы 9
# заседаний.
FIXTURE_MANUAL_MEETINGS = (
    (
        date(2021, 3, 19),
        "СД ЦБ РФ: тест-фикстура — ручной триггер",
        1,
    ),
    (
        date(2021, 5, 14),
        "СД ЦБ РФ: тест-фикстура — ручной триггер с уникальной датой",
        0,
    ),
)

# Ставка: разреженные точки на даты вступления в силу. Шаги заседаний:
# +25, +25, +25, +50, +100, +1350 (внеочередное), 0, −300 (внеочередное).
FIXTURE_KEY_RATE_POINTS = (
    (date(2021, 1, 1), 4.25),
    (date(2021, 2, 13), 4.50),
    (date(2021, 3, 20), 4.75),
    (date(2021, 4, 24), 5.00),
    (date(2021, 6, 12), 5.50),
    (date(2021, 7, 24), 6.50),
    (date(2022, 2, 28), 20.00),
    (date(2022, 3, 19), 20.00),
    (date(2022, 4, 9), 17.00),
    (date(2022, 6, 30), 17.00),
)


# --- Разбор строк: чистые функции ---------------------------------------------


def row_bool(value) -> bool:
    """Флаг из значения строки ClickHouse/CSV: `1`, `"1"`, `"true"`, `True`.

    `NULL` (`None`) и пустая ячейка — «нет флага», а не ошибка: у старых
    ответов колонки `emergency` может не быть вовсе (до Task 1).
    """
    if value is None or value == "":
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return bool(value)


def _meeting_pairs(meetings: list) -> list[tuple[date, bool]]:
    """Нормализует вход `build_panel`: пары `(дата, emergency)` или даты.

    Даты без флага трактуются как плановые заседания (`emergency=False`) —
    так функцию можно звать из старых тестов/срезов, где флага ещё нет.
    """
    pairs = []
    for meeting in meetings:
        if isinstance(meeting, (tuple, list)) and len(meeting) == 2:
            day, emergency = meeting
        else:
            day, emergency = meeting, False
        pairs.append((krm.row_date(day), bool(emergency)))
    return pairs


def meetings_with_emergency_from_rows(rows: list) -> list[tuple[date, bool]]:
    """Канонические заседания `events_calendar`: `(дата, emergency)`.

    Отбор — `category='cbr' ∧ title=SAMPLE_TITLE` (фильтр по title отсекает
    ручные строки-триггеры). Дедуп по дате: каноническая строка одна, но
    повторный импорт/дубль ответа не должен дать два заседания; `emergency`
    при дублях складывается через OR — флаг не теряется.
    """
    flags: dict[date, bool] = {}
    for row in rows:
        if str(row.get("category", "")) != "cbr":
            continue
        if str(row.get("title", "")) != SAMPLE_TITLE:
            continue
        day = krm.row_date(row["event_date"])
        flags[day] = flags.get(day, False) or row_bool(row.get("emergency"))
    return sorted(flags.items())


def load_meetings_with_emergency() -> list[tuple[date, bool]]:
    """Живой список канонических заседаний с флагом внеочередного."""
    return meetings_with_emergency_from_rows(krm.ch_query(MEETINGS_SQL))


def points_from_rows(rows: list, series: str = "ряд") -> list[tuple[date, float]]:
    """Точки `(дата, значение)` из строк с общими колонками `date`/`value`.

    `NULL`/пустая ячейка значения — ошибка, а не ноль (`krm.row_float`):
    молчаливый ноль выглядел бы измеренным. Повтор даты — тоже ошибка
    (`krm._unique_dates`): as-of выбрал бы одну из двух точек по порядку.
    """
    points = [(krm.row_date(row["date"]), krm.row_float(row["value"])) for row in rows]
    return sorted(krm._unique_dates(points, series))


def normalize_salary_points(points: list) -> list[tuple[date, float]]:
    """Приводит даты зарплаты к ОТЧЁТНОМУ месяцу.

    `rosstat_salaries_mes` пишет дату 1-го числа следующего месяца
    (`salariesMesPoint.date`): значение за январь лежит на 1 февраля. Для
    as-of и г/г нужен месяц наблюдения, поэтому дата сдвигается на месяц
    назад (до первого числа отчётного месяца).
    """
    normalized = []
    for day, value in points:
        day = krm.row_date(day)
        normalized.append((_shift_months(date(day.year, day.month, 1), -1), value))
    return sorted(krm._unique_dates(normalized, "rosstat_salaries_mes"))


def load_channels() -> dict[str, list[tuple[date, float]]]:
    """Все каналы панели: по одному SQL на ряд, даты — период наблюдения.

    У зарплаты дата нормализуется к отчётному месяцу
    (`normalize_salary_points`); остальные ряды отдают период как в источнике
    (месячные — дата отчётного месяца/последний день, дневные — дата
    наблюдения).
    """
    channels = {}
    for name, sql in CHANNEL_SQL.items():
        points = points_from_rows(krm.ch_query(sql), name)
        if name == "wage":
            points = normalize_salary_points(points)
        channels[name] = points
        log.info("канал %s: %d точек", name, len(points))
    return channels


def load_key_rate() -> list[tuple[date, float]]:
    """Дневной ряд действующих ставок — тот же загрузчик, что у v0."""
    return krm.load_key_rate()


def load_mp_summary_deltas() -> list[tuple[date, Optional[int]]]:
    """Шаги решений из резюме ЦБ — сверка ярлыков, как у v0."""
    return krm.load_mp_summary_deltas()


# --- As-of слой (спека §4.1) --------------------------------------------------


def _available_on_the_day(period: date) -> date:
    """Дневной ряд доступен в день наблюдения (лага нет)."""
    return period


def _asof_point(
    points: list, asof_date, monthly: bool
) -> Optional[tuple]:
    """Последняя доступная на `asof_date` точка `(период, значение)` или None.

    Повторяет правило `key_rate_model.asof` (выбирается точка с наибольшим
    периодом среди `available(period) <= asof_date`), но возвращает и дату
    периода: она нужна для г/г и лог-изменения за 3 месяца. Месячная
    доступность — конец M+1 (`krm.available_from_cpd`), дневная — день
    наблюдения.
    """
    asof_date = krm.row_date(asof_date)
    available = krm.available_from_cpd if monthly else _available_on_the_day
    best = None
    for point in points:
        period = krm.row_date(point[0])
        if available(period) <= asof_date and (best is None or period > best[0]):
            best = (period, point[1])
    return best


def feature_asof(points: list, meeting_date, monthly: bool = True) -> Optional[float]:
    """Значение признака as-of даты заседания (спека §4.1).

    `monthly=True` — месячный ряд: точка отчётного месяца M участвует, только
    если `end_of_month(M+1) <= meeting_date` (публикация во второй декаде
    M+1; конвенция консервативная). `monthly=False` — дневной ряд: `date <=
    meeting_date`. Пустой результат — `None`: вызывающий решает, пропуск это
    или нет, но НЕ подставляет ноль.
    """
    point = _asof_point(points, meeting_date, monthly)
    return None if point is None else point[1]


def _shift_months(day: date, months: int) -> date:
    """Сдвиг даты на месяцы с зажимом дня в конце короткого месяца.

    `date.replace` на 31 марта минус месяц дал бы 31 февраля и исключение;
    здесь день зажимается до последнего дня целевого месяца.
    """
    index = day.year * 12 + (day.month - 1) + months
    year, month_index = divmod(index, 12)
    month = month_index + 1
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, min(day.day, last_day))


def _month_key(day: date) -> tuple[int, int]:
    """Ключ месяца `(год, месяц)` для г/г и 12-месячных сумм."""
    return day.year, day.month


def _shift_month_key(key: tuple[int, int], months: int) -> tuple[int, int]:
    """Сдвиг ключа месяца без привязки к дню."""
    index = key[0] * 12 + (key[1] - 1) + months
    year, month_index = divmod(index, 12)
    return year, month_index + 1


def _monthly_by_month(points: list, series: str) -> dict[tuple[int, int], float]:
    """Точки месячного ряда по ключу месяца; дубль месяца — ошибка.

    Два значения одного месяца означали бы неоднозначный г/г: какое из них
    «то же месяц годом ранее» — не определено.
    """
    by_month = {}
    for day, value in points:
        key = _month_key(krm.row_date(day))
        if key in by_month:
            raise ValueError(
                f"{series}: месяц {key[0]}-{key[1]:02d} встречается дважды — "
                f"годовой темп/12-месячная сумма неоднозначны"
            )
        by_month[key] = value
    return by_month


def _yoy_percent(points: list, meeting_date, series: str) -> Optional[float]:
    """Г/г, %: текущий as-of уровень к точке того же месяца годом ранее.

    Текущая точка выбирается по правилу §4.1; старая — строго тот же месяц
    прошлого года. Нет старой точки или она нулевая — `None`, а не 0.
    """
    current = _asof_point(points, meeting_date, monthly=True)
    if current is None:
        return None
    by_month = _monthly_by_month(points, series)
    previous_key = _shift_month_key(_month_key(current[0]), -12)
    if previous_key not in by_month:
        return None
    previous = by_month[previous_key]
    if previous == 0:
        return None
    return (current[1] / previous - 1.0) * 100.0


def _trailing_sum(points: list, meeting_date, series: str, months: int = 12) -> Optional[float]:
    """Сумма месячного ряда за `months` месяцев, заканчивая as-of месяцем.

    Набор месяцев проверяется целиком: дыра в середине окна — `None`, а не
    сумма по меньшему числу месяцев (иначе «12-месячное» сальдо молча
    считалось бы по 9 месяцам).
    """
    current = _asof_point(points, meeting_date, monthly=True)
    if current is None:
        return None
    by_month = _monthly_by_month(points, series)
    keys = [
        _shift_month_key(_month_key(current[0]), -offset) for offset in range(months)
    ]
    if any(key not in by_month for key in keys):
        return None
    return sum(by_month[key] for key in keys)


def _usdrub_3m(points: list, meeting_date) -> Optional[float]:
    """Лог-изменение курса за 3 месяца, %: `100·ln(P_t / P_{t−3})`.

    Текущая точка — as-of заседания; база — последняя точка не позже даты
    «ровно 3 месяца назад» (курс не публикуется в выходные). Неположительный
    курс/база — `None`: логарифм не определён.
    """
    current = _asof_point(points, meeting_date, monthly=False)
    if current is None:
        return None
    target = _shift_months(current[0], -3)
    previous = _asof_point(points, target, monthly=False)
    if previous is None or current[1] <= 0 or previous[1] <= 0:
        return None
    return 100.0 * math.log(current[1] / previous[1])


# --- Метка шока (спека §3.2) --------------------------------------------------


def label_shock(emergency: bool, step_bp: int) -> bool:
    """Явный шок: внеочередное заседание И изменение ставки.

    Внеочередное без изменения — `False` (метка `emergency` при этом
    сохраняется): правило спеки не должно молча превращать hold в шок.
    """
    return bool(emergency) and int(step_bp) != 0


# --- Панель заседаний (спека §4.1–4.2) ----------------------------------------


@dataclass(frozen=True)
class PanelRow:
    """Строка панели: решение, метки, признаки as-of и число пропусков.

    `features` содержит базовые признаки в порядке `BASE_FEATURES` и дельты
    `*_chg` в порядке `DELTA_FEATURES`; отсутствующая точка — `None`.
    `missing_count` — число отсутствующих БАЗОВЫХ признаков (дельты
    производные и счётчик не раздувают).
    """

    decision: krm.Decision
    emergency: bool
    explicit_shock: bool
    features: dict[str, Optional[float]]
    missing_count: int


def _base_features(
    decision: krm.Decision,
    channel_points: dict[str, list[tuple[date, float]]],
) -> dict[str, Optional[float]]:
    """Базовые признаки одного заседания, снятые as-of (спека §4.2).

    Каждый канал берётся из `channel_points` по имени (см. `CHANNEL_SQL`);
    отсутствующий канал — `None`. `rate_before` — из решения, потому что
    ставка «до» уже посчитана правилом v0 и повторно из ряда не снимается.
    """
    def level(name: str, monthly: bool = True) -> Optional[float]:
        return feature_asof(
            channel_points.get(name, []), decision.meeting_date, monthly=monthly
        )

    pi = level("pi")
    cpi = level("cpi")

    income = _trailing_sum(
        channel_points.get("budget_income", []), decision.meeting_date, "budget_income"
    )
    expenses = _trailing_sum(
        channel_points.get("budget_expense", []), decision.meeting_date, "budget_expense"
    )
    budget_balance = None
    if income is not None and expenses is not None and income != 0:
        budget_balance = (income - expenses) / income * 100.0

    ruania = level("ruonia", monthly=False)

    features = {
        "rate_before": decision.rate_before,
        "pi_saar": None if pi is None else krm.annualize_saar(pi),
        "cpi_saar": None if cpi is None else krm.annualize_saar(cpi),
        "infl_exp_expected": level("infl_exp_expected"),
        "infl_exp_observed": level("infl_exp_observed"),
        "unemployment": level("unemployment"),
        "wage_yoy": _yoy_percent(
            channel_points.get("wage", []), decision.meeting_date, "wage"
        ),
        "credit_yoy": _yoy_percent(
            channel_points.get("credit", []), decision.meeting_date, "credit"
        ),
        "m2_yoy": _yoy_percent(
            channel_points.get("m2", []), decision.meeting_date, "m2"
        ),
        "budget_balance": budget_balance,
        "usdrub_3m": _usdrub_3m(
            channel_points.get("usdrub", []), decision.meeting_date
        ),
        "ruania_spread": None if ruania is None else ruania - decision.rate_before,
        "price_expectations": level("price_expectations"),
        "business_climate": level("business_climate"),
    }
    return features


def build_panel(
    meetings: list,
    key_rate: list,
    channel_points: dict[str, list[tuple[date, float]]],
    mp_deltas: list,
) -> list[PanelRow]:
    """Панель заседаний: решения v0 + метки + каналы as-of.

    `meetings` — пары `(дата, emergency)` (`load_meetings_with_emergency`) или
    просто даты; `key_rate`/`mp_deltas` — как у `key_rate_model`. Решения
    собирает `krm.build_decisions`: заседание с ненаблюдаемым окном исхода
    («вне выборки») и заседание без ставки на d−1 («пропущено») в панель не
    попадают — строка панели обязана быть наблюдённым решением.

    Дельты `*_chg` считаются к предыдущей СТРОКЕ панели (предыдущему
    решению), а не к предыдущему месяцу: шок — сдвиг между решениями.
    У первой строки дельты `None`.
    """
    pairs = _meeting_pairs(meetings)
    flags: dict[date, bool] = {}
    for day, emergency in pairs:
        flags[day] = flags.get(day, False) or emergency

    decisions, _, _ = krm.build_decisions(
        [day for day, _ in pairs], key_rate, mp_deltas
    )

    rows = []
    previous: Optional[PanelRow] = None
    for decision in decisions:
        features = _base_features(decision, channel_points)
        for name in DELTA_FEATURES:
            current = features[name]
            prior = None if previous is None else previous.features.get(name)
            features[f"{name}_chg"] = (
                None if current is None or prior is None else current - prior
            )
        missing_count = sum(
            1 for name in BASE_FEATURES if features[name] is None
        )
        row = PanelRow(
            decision=decision,
            emergency=flags.get(decision.meeting_date, False),
            explicit_shock=label_shock(
                flags.get(decision.meeting_date, False), decision.step_bp
            ),
            features=features,
            missing_count=missing_count,
        )
        rows.append(row)
        previous = row
    return rows


# --- Аномальная модель и причинные веса (спека §5) ----------------------------


@dataclass(frozen=True)
class AnomalyModel:
    """Статистики аномальной модели, оценённые на одном обучающем окне.

    Все поля — ТОЛЬКО по обучающему окну (`rows[:t]` для строки `t`): медиана
    и масштаб стандартизации, среднее (`location`) и точность (`precision`)
    сжатой ковариации Ledoit–Wolf, а также медиана расстояний Махаланобиса
    обучающих строк — знаменатель `r_t` (спека §5). Тестовая строка не
    участвует ни в одном из них.

    `features` — порядок столбцов: он один для подгонки и для `anomaly_score`,
    иначе признаки перепутались бы местами.
    """

    features: tuple
    median: np.ndarray
    scale: np.ndarray
    location: np.ndarray
    precision: np.ndarray
    median_distance: float


def _feature_matrix(rows: list, features: Sequence[str]) -> np.ndarray:
    """Матрица признаков `rows × features`; пропуск — `NaN`, а не ноль.

    Ноль на месте пропуска выглядел бы измеренным значением и исказил бы и
    медиану, и ковариацию. `NaN` до стандартизации означает «точки нет», а
    после (`_standardize`) — «заполнено медианой обучающего окна».
    """
    matrix = np.empty((len(rows), len(features)), dtype=float)
    for row_index, row in enumerate(rows):
        for column_index, name in enumerate(features):
            value = row.features.get(name)
            matrix[row_index, column_index] = (
                np.nan if value is None else float(value)
            )
    return matrix


def _robust_center_scale(
    matrix: np.ndarray,
) -> tuple[np.ndarray, np.ndarray]:
    """Медиана и IQR каждого столбца по ОБУЧАЮЩЕМУ окну (спека §5).

    `IQR = 0` (константа, мало точек) → стандартное отклонение, затем 1:
    делить на ноль нельзя, а масштаб 1 оставляет признак как есть. Столбец
    без единого наблюдения получает медиану 0 и масштаб 1 — стандартизация
    такого столбца всё равно даст нули (все значения пропущены).
    """
    center = np.zeros(matrix.shape[1], dtype=float)
    scale = np.ones(matrix.shape[1], dtype=float)
    for column_index in range(matrix.shape[1]):
        observed = matrix[:, column_index]
        observed = observed[np.isfinite(observed)]
        if observed.size == 0:
            continue
        center[column_index] = float(np.median(observed))
        q25, q75 = np.percentile(observed, [25, 75])
        spread = float(q75 - q25)
        if not math.isfinite(spread) or spread == 0.0:
            spread = float(np.std(observed, ddof=0))
        if not math.isfinite(spread) or spread == 0.0:
            spread = 1.0
        scale[column_index] = spread
    return center, scale


def _standardize(
    matrix: np.ndarray, center: np.ndarray, scale: np.ndarray
) -> np.ndarray:
    """Стандартизация и заполнение пропусков: `NaN` → 0 (спека §5).

    После вычитания медианы и деления на IQR пропуск становится ровно нулём —
    то есть медианой обучающего окна в стандартизованном пространстве. Это и
    есть «медианная импутация», не создающая измеренного значения.
    """
    standardized = (matrix - center) / scale
    return np.where(np.isfinite(standardized), standardized, 0.0)


def _mahalanobis(
    matrix: np.ndarray, location: np.ndarray, precision: np.ndarray
) -> np.ndarray:
    """Расстояния Махаланобиса строк до `location` с точностью `precision`.

    Квадратичная форма может оказаться чуть отрицательной из-за округления у
    почти сингулярной точности; `max(·, 0)` не даёт `sqrt` вернуть `NaN`.
    """
    differences = matrix - location
    squared = np.einsum("ij,jk,ik->i", differences, precision, differences)
    return np.sqrt(np.maximum(squared, 0.0))


def fit_anomaly(
    train_rows: list, features: Sequence[str] = FEATURES_ALL
) -> AnomalyModel:
    """Оценивает аномальную модель на обучающем окне (спека §5).

    Порядок — как в спеке: медиана/IQR → стандартизация с заполнением
    пропусков нулём → `LedoitWolf` на стандартизованных строках → медиана
    расстояний Махаланобиса обучающих строк. Сжатие Ledoit–Wolf нужно, потому
    что признаки коррелированы, а наблюдений немного; обычная выборочная
    ковариация была бы сингулярной.

    Окно из одной строки не даёт ковариации: используется единичная точность,
    и все расстояния (включая медиану) нулевые — вес такой строки будет 1.
    Пустое окно — ошибка вызывающего: статистик нет вовсе.
    """
    if not train_rows:
        raise ValueError("аномальная модель: пустое обучающее окно")
    matrix = _feature_matrix(train_rows, features)
    center, scale = _robust_center_scale(matrix)
    standardized = _standardize(matrix, center, scale)
    if standardized.shape[0] < 2:
        location = np.zeros(standardized.shape[1], dtype=float)
        precision = np.eye(standardized.shape[1], dtype=float)
    else:
        estimator = LedoitWolf().fit(standardized)
        location = np.asarray(estimator.location_, dtype=float)
        precision = np.asarray(estimator.precision_, dtype=float)
    distances = _mahalanobis(standardized, location, precision)
    return AnomalyModel(
        features=tuple(features),
        median=center,
        scale=scale,
        location=location,
        precision=precision,
        median_distance=float(np.median(distances)),
    )


def anomaly_score(model: AnomalyModel, row: PanelRow) -> float:
    """Расстояние Махаланобиса строки `row` до статистик `model` (спека §5).

    Строка стандартизуется медианой/IQR ОБУЧАЮЩЕГО окна (`model`), поэтому её
    пропуски заполняются медианой обучения, а не её собственными значениями.
    """
    matrix = _feature_matrix([row], model.features)
    standardized = _standardize(matrix, model.median, model.scale)
    return float(_mahalanobis(standardized, model.location, model.precision)[0])


def weight_from_score(score: float, median_train: float) -> float:
    """Причинный вес по аномальному скору (спека §5).

    `r = d_t / median(d_train)`; `w = 1/(1 + max(0, r − 1)²)`, клип
    `[WEIGHT_MIN, WEIGHT_MAX]`. Вес монотонно убывает с аномальностью без
    жёсткого порога: `r ≤ 1` — вес 1, дальше плавное «естественное
    понижение». Неизмеримый скор (NaN/inf) или нулевая медиана обучения —
    вес 1: понижать наблюдение не за что.
    """
    if (
        not math.isfinite(score)
        or not math.isfinite(median_train)
        or median_train <= 0.0
    ):
        return WEIGHT_MAX
    ratio = score / median_train
    excess = max(0.0, ratio - 1.0)
    weight = 1.0 / (1.0 + excess * excess)
    return min(WEIGHT_MAX, max(WEIGHT_MIN, weight))


def walk_forward_weights(
    rows: list,
    min_train: int = MIN_TRAIN,
    features: Sequence[str] = FEATURES_ALL,
) -> list[float]:
    """As-of веса всех заседаний: `t` не участвует в своих статистиках (§5).

    Для строки `t ≥ min_train` модель обучается на `rows[:t]` — строго прошлых
    заседаниях; строка `t` подаётся только в `anomaly_score`. Первые
    `min_train` строк получают `1.0`: статистик ещё нет. Возврат — веса в
    порядке входа, поэтому `weights[t]` принадлежит `rows[t]`.
    """
    if min_train < 1:
        raise ValueError(f"walk-forward весов: min_train = {min_train} < 1")
    weights = []
    for index, row in enumerate(rows):
        if index < min_train:
            weights.append(WEIGHT_MAX)
            continue
        model = fit_anomaly(rows[:index], features)
        weights.append(
            weight_from_score(anomaly_score(model, row), model.median_distance)
        )
    return weights


# --- Описательный классификатор шоков (спека §6) ------------------------------


def fit_shock_classifier(
    train_rows: list, features: Sequence[str] = FEATURES_ALL
) -> LogisticRegression:
    """Логистическая регрессия явного шока на обучающем окне (спека §6).

    L2, `C=CLASSIFIER_C`, `class_weight='balanced'`, `max_iter=...`; `random_state`
    фиксирован, хотя lbfgs детерминирован и без него. Признаки стандартизуются
    ровно теми же медианой/IQR, что у аномальной модели (пропуск → 0):
    коэффициенты тогда сравнимы между признаками, а не измеряются в их
    исходных единицах.

    Окно без двух классов (в частности, без единого положительного шока)
    вырождено: `LogisticRegression` описал бы один класс, и вероятности шока
    были бы фикцией. Это свойство окна, а не ошибка данных, поэтому
    поднимается `DegenerateWindowError` — `walk_forward_classifier` пропускает
    фолд как `None`.

    Статистики стандартизации кладутся в fitted-атрибуты
    `feature_median_`/`feature_scale_`/`feature_names_`: вызывающему нужно
    преобразовать тестовую строку теми же числами, а не переоценивать их на
    тесте.
    """
    if not train_rows:
        raise krm.DegenerateWindowError(
            "классификатор шоков: пустое обучающее окно — классы не оценены"
        )
    labels = np.asarray(
        [1 if row.explicit_shock else 0 for row in train_rows], dtype=int
    )
    if labels.min() == labels.max():
        raise krm.DegenerateWindowError(
            f"классификатор шоков: в обучающем окне один класс "
            f"({int(labels[0])}) — положительных шоков "
            f"{int(labels.sum())} из {len(labels)}, распределение не оценено"
        )
    matrix = _feature_matrix(train_rows, features)
    center, scale = _robust_center_scale(matrix)
    standardized = _standardize(matrix, center, scale)
    model = LogisticRegression(
        C=CLASSIFIER_C,
        class_weight="balanced",
        max_iter=CLASSIFIER_MAX_ITER,
        random_state=SEED,
    )
    model.fit(standardized, labels)
    model.feature_median_ = center
    model.feature_scale_ = scale
    model.feature_names_ = tuple(features)
    return model


def shock_probability(model: LogisticRegression, row: PanelRow) -> float:
    """Вероятность явного шока для одной строки по fitted-модели (спека §6).

    Строка стандартизуется статистиками ОБУЧАЮЩЕГО окна, сохранёнными в
    `fit_shock_classifier` (`feature_median_`/`feature_scale_`): тестовая
    строка не переоценивает их на себе. Пропуск заполняется медианой обучения
    (→ 0), как и у аномальной модели.
    """
    matrix = _feature_matrix([row], model.feature_names_)
    standardized = _standardize(
        matrix, model.feature_median_, model.feature_scale_
    )
    positive_column = list(model.classes_).index(1)
    return float(model.predict_proba(standardized)[0, positive_column])


def walk_forward_classifier(
    rows: list,
    min_train: int = MIN_TRAIN,
    features: Sequence[str] = FEATURES_ALL,
) -> list[Optional[float]]:
    """Walk-forward вероятность явного шока для каждого заседания (спека §6).

    Фолд `t` обучается на `rows[:t]` (только прошлое) и предсказывает
    вероятность `explicit_shock` строки `t`. Первые `min_train` строк и
    вырожденные фолды (один класс в обучении) — `None`: вызывающий считает их
    число как `sum(probability is None for probability in result)`. Возврат —
    список длины `len(rows)`, выровненный по строкам.
    """
    if min_train < 1:
        raise ValueError(f"walk-forward классификатора: min_train = {min_train} < 1")
    probabilities: list[Optional[float]] = [None] * len(rows)
    for index in range(min_train, len(rows)):
        try:
            model = fit_shock_classifier(rows[:index], features)
        except krm.DegenerateWindowError:
            continue
        probabilities[index] = shock_probability(model, rows[index])
    return probabilities


def classifier_metrics(labels: list, probs: list) -> dict:
    """Метрики вероятностного классификатора шоков (спека §6).

    `labels` — 0/1, `probs` — вероятность класса 1; `None` в `probs` (фолд
    пропущен) исключается из метрик и считается в `skipped`. ROC-AUC
    не определён без обоих классов, PR-AUC — без положительных; в этих
    случаях возвращается явный `None`, а не исключение и не NaN. Brier
    определён всегда (при непустой выборке), precision/recall — из матрицы
    ошибок при пороге `CLASSIFIER_THRESHOLD`; нулевой знаменатель даёт `None`.

    `confusion` — строки «факт 0/1», столбцы «прогноз 0/1»:
    `[[TN, FP], [FN, TP]]`.
    """
    if len(labels) != len(probs):
        raise ValueError(
            f"метрики классификатора: {len(labels)} меток против {len(probs)} вероятностей"
        )
    pairs = [
        (int(bool(label)), float(probability))
        for label, probability in zip(labels, probs)
        if probability is not None
    ]
    result = {
        "n": len(pairs),
        "skipped": len(labels) - len(pairs),
        "positives": sum(label for label, _ in pairs),
        "threshold": CLASSIFIER_THRESHOLD,
        "roc_auc": None,
        "pr_auc": None,
        "brier": None,
        "precision": None,
        "recall": None,
        "confusion": [[0, 0], [0, 0]],
    }
    if not pairs:
        return result
    outcomes = [label for label, _ in pairs]
    probabilities = [probability for _, probability in pairs]
    result["brier"] = sum(
        (probability - label) ** 2 for label, probability in pairs
    ) / len(pairs)
    confusion = [[0, 0], [0, 0]]
    for label, probability in pairs:
        predicted = 1 if probability >= CLASSIFIER_THRESHOLD else 0
        confusion[label][predicted] += 1
    result["confusion"] = confusion
    _, false_positive = confusion[0]
    false_negative, true_positive = confusion[1]
    if true_positive + false_positive > 0:
        result["precision"] = true_positive / (true_positive + false_positive)
    if true_positive + false_negative > 0:
        result["recall"] = true_positive / (true_positive + false_negative)
    positives = result["positives"]
    if 0 < positives < len(pairs):
        result["roc_auc"] = float(roc_auc_score(outcomes, probabilities))
    if positives > 0:
        result["pr_auc"] = float(average_precision_score(outcomes, probabilities))
    return result


# --- Синтетические фикстуры и их чтение ---------------------------------------


def _month_end(month: date) -> date:
    """Последний день месяца, в котором лежит `month`."""
    return date(
        month.year, month.month, calendar.monthrange(month.year, month.month)[1]
    )


def _months(start: date, end: date) -> list[date]:
    """Первые числа месяцев `start..end` включительно."""
    keys = []
    key = (start.year, start.month)
    end_key = (end.year, end.month)
    while key <= end_key:
        keys.append(date(key[0], key[1], 1))
        key = _shift_month_key(key, 1)
    return keys


def fixture_cpd_points() -> dict[str, list[tuple[date, float]]]:
    """Синтетические точки CPD: `pi` и `cpi` — месячные SA-темпы, %.

    Ряд идёт с декабря 2020 по март 2022. Март 2022 намеренно резко
    отличается: тест as-of проверяет, что заседание 2022-04-08 видит только
    февральскую точку (март станет доступен в конце апреля).
    """
    pi_points = []
    cpi_points = []
    for index, month in enumerate(_months(date(2020, 12, 1), date(2022, 3, 1))):
        period = _month_end(month)
        if (month.year, month.month) == (2022, 3):
            pi_points.append((period, 9.99))
            cpi_points.append((period, 8.88))
            continue
        pi_points.append((period, round(0.15 + 0.01 * index, 4)))
        cpi_points.append((period, round(0.25 + 0.015 * index, 4)))
    return {"pi": pi_points, "cpi": cpi_points}


def fixture_channel_points() -> dict[str, list[tuple[date, float]]]:
    """Синтетические точки всех каналов панели (14 ключей `CHANNEL_KEYS`).

    Даты — в конвенции `load_channels`: месячные ряды отдают период
    наблюдения (у зарплаты — уже приведённый отчётный месяц), дневные — дату
    наблюдения. `infl_exp_expected`/`infl_exp_observed` начинаются с января
    2021 и на первом заседании (2021-02-12) недоступны: январь становится
    доступен в конце февраля. Это и есть «один канал отсутствует на первом
    заседании» из брифа Task 2.
    """
    channels = fixture_cpd_points()

    inflation_months = _months(date(2021, 1, 1), date(2022, 2, 1))
    channels["infl_exp_expected"] = [
        (date(month.year, month.month, 14), round(9.0 + 0.1 * index, 2))
        for index, month in enumerate(inflation_months)
    ]
    channels["infl_exp_observed"] = [
        (date(month.year, month.month, 14), round(8.0 + 0.1 * index, 2))
        for index, month in enumerate(inflation_months)
    ]

    channels["unemployment"] = [
        (_month_end(month), round(4.5 + 0.01 * index, 3))
        for index, month in enumerate(
            _months(date(2020, 12, 1), date(2022, 2, 1))
        )
    ]

    # Уровни для г/г: с декабря 2019, чтобы у первого заседания (2021-02-12)
    # нашлась точка декабря 2019. Даты — отчётный месяц (первое число).
    level_months = _months(date(2019, 12, 1), date(2022, 2, 1))
    channels["wage"] = [
        (month, 50_000.0 + 500.0 * index) for index, month in enumerate(level_months)
    ]
    channels["credit"] = [
        (month, 40_000_000.0 + 250_000.0 * index)
        for index, month in enumerate(level_months)
    ]
    channels["m2"] = [
        (month, 60_000.0 + 300.0 * index) for index, month in enumerate(level_months)
    ]
    channels["budget_income"] = [
        (month, 2_000.0 + 10.0 * index) for index, month in enumerate(level_months)
    ]
    channels["budget_expense"] = [
        (month, 1_800.0 + 12.0 * index) for index, month in enumerate(level_months)
    ]

    survey_months = _months(date(2020, 12, 1), date(2022, 2, 1))
    channels["price_expectations"] = [
        (month, round(20.0 + 0.2 * index, 2))
        for index, month in enumerate(survey_months)
    ]
    channels["business_climate"] = [
        (month, round(-5.0 + 0.1 * index, 2))
        for index, month in enumerate(survey_months)
    ]

    # Дневные ряды: раз в неделю с 2020-09-01 по 2022-04-15. As-of выбирает
    # последнюю точку не позже даты, поэтому недельного шага достаточно.
    usdrub = []
    ruonia = []
    day = date(2020, 9, 1)
    index = 0
    while day <= date(2022, 4, 15):
        usdrub.append((day, round(70.0 + 0.04 * index, 2)))
        ruonia.append((day, round(4.25 + 0.02 * index, 2)))
        day += timedelta(days=7)
        index += 1
    channels["usdrub"] = usdrub
    channels["ruonia"] = ruonia

    return {name: channels[name] for name in CHANNEL_KEYS}


def _csv_cell(value) -> str:
    """Ячейка CSV: `None` — пустая, `date` — ISO, прочее — `str`."""
    if value is None:
        return ""
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _write_csv(path: Path, columns: tuple, rows: list) -> None:
    """Пишет CSV с LF-окончаниями (как фикстуры `key_rate`)."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle, fieldnames=columns, lineterminator="\n"
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: _csv_cell(row.get(column)) for column in columns})


def _read_csv(path: Path) -> list:
    """Читает CSV-фикстуру в строки-словари (как ответ `ch_query`)."""
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return rows


def _fixture_meeting_rows() -> list:
    """Строки `meetings.csv`: канонические + ручные trigger-строки."""
    rows = [
        {
            "event_date": day.isoformat(),
            "category": "cbr",
            "title": SAMPLE_TITLE,
            "emergency": int(emergency),
        }
        for day, emergency in FIXTURE_CANONICAL_MEETINGS
    ]
    rows.extend(
        {
            "event_date": day.isoformat(),
            "category": "cbr",
            "title": title,
            "emergency": emergency,
        }
        for day, title, emergency in FIXTURE_MANUAL_MEETINGS
    )
    return sorted(rows, key=lambda row: (row["event_date"], row["title"]))


def _fixture_key_rate_rows() -> list:
    """Строки `key_rate.csv` в форме ответа `cbr_key_rate`."""
    return [
        {"date": day.isoformat(), "rate": rate}
        for day, rate in sorted(FIXTURE_KEY_RATE_POINTS)
    ]


def _fixture_cpd_rows() -> list:
    """Строки `cpd.csv` в форме ответа `cbr_indicators_cpd`."""
    names = {"pi": CPD_CORE_SERIES, "cpi": CPD_ALL_SERIES}
    rows = [
        {"name": names[key], "date": day.isoformat(), "value": value}
        for key, points in fixture_cpd_points().items()
        for day, value in points
    ]
    return sorted(rows, key=lambda row: (row["name"], row["date"]))


def _fixture_channel_rows() -> list:
    """Строки `channels.csv`: `channel,date,value` для всех каналов, кроме CPD.

    CPD лежит отдельным `cpd.csv` в форме ответа своего SQL (имя ряда +
    значение); `read_fixtures` складывает оба источника.
    """
    rows = [
        {"channel": key, "date": day.isoformat(), "value": value}
        for key, points in fixture_channel_points().items()
        if key not in ("pi", "cpi")
        for day, value in points
    ]
    return sorted(rows, key=lambda row: (row["channel"], row["date"]))


def export_fixtures(directory) -> int:
    """Пишет синтетические CSV офлайн-тестов в `directory`.

    Набор детерминирован: повторный экспорт даёт те же байты (LF, порядок
    строк по ключу). Состав: 8 заседаний 2021–2022 (2 внеочередных, плюс
    ручные строки-триггеры для проверки фильтра по title), ряд ставки,
    CPD-ряды и все остальные каналы. ClickHouse не нужен.
    """
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    exports = (
        (FIXTURE_MEETINGS, _fixture_meeting_rows()),
        (FIXTURE_KEY_RATE, _fixture_key_rate_rows()),
        (FIXTURE_CPD, _fixture_cpd_rows()),
        (FIXTURE_CHANNELS, _fixture_channel_rows()),
    )
    for name, rows in exports:
        _write_csv(directory / name, FIXTURE_COLUMNS[name], rows)
    print(f"Фикстуры классификатора шоков записаны в {directory}:")
    for name, rows in exports:
        print(f"  {name:<16} {len(rows):>5} строк")
    return 0


def cpd_channels_from_rows(rows: list) -> dict[str, list[tuple[date, float]]]:
    """CPD-строки → каналы `pi`/`cpi` (имена рядов источника → ключи панели)."""
    names = {CPD_CORE_SERIES: "pi", CPD_ALL_SERIES: "cpi"}
    grouped = {key: [] for key in names.values()}
    for row in rows:
        name = str(row.get("name", ""))
        if name not in names:
            raise ValueError(
                f"cpd: неизвестный ряд {name!r}; ожидались {sorted(names)}"
            )
        grouped[names[name]].append(row)
    return {key: points_from_rows(rows, key) for key, rows in grouped.items()}


def channels_from_rows(rows: list) -> dict[str, list[tuple[date, float]]]:
    """Строки `channel,date,value` → словарь точек каналов.

    Отсутствующий в файле канал остаётся в словаре пустым списком (а не
    исчезает): `build_panel` тогда даст `None` и посчитает пропуск.
    """
    grouped = {key: [] for key in CHANNEL_KEYS}
    for row in rows:
        channel = str(row.get("channel", ""))
        if channel not in grouped:
            raise ValueError(
                f"channels: неизвестный канал {channel!r}; ожидались {CHANNEL_KEYS}"
            )
        grouped[channel].append(row)
    return {key: points_from_rows(rows, key) for key, rows in grouped.items()}


def read_fixtures(
    directory,
) -> tuple[
    list[tuple[date, bool]],
    list[tuple[date, float]],
    dict[str, list[tuple[date, float]]],
]:
    """Читает фикстуры `export_fixtures` обратно в формы живых загрузчиков.

    Возврат — `(meetings, key_rate, channel_points)`: ровно те же формы, что
    отдают `load_meetings_with_emergency`, `load_key_rate` и `load_channels`,
    поэтому тесты гоняют `build_panel` тем же путём, что и живой прогон.
    """
    directory = Path(directory)
    meetings = meetings_with_emergency_from_rows(
        _read_csv(directory / FIXTURE_MEETINGS)
    )
    key_rate = krm.key_rate_from_rows(_read_csv(directory / FIXTURE_KEY_RATE))
    # CPD лежит отдельным файлом, а `channels_from_rows` возвращает все ключи
    # (отсутствующие — пустыми). Поэтому CPD накладывается ПОСЛЕ: иначе пустые
    # `pi`/`cpi` из channels.csv затёрли бы реальные точки cpd.csv.
    channels = channels_from_rows(_read_csv(directory / FIXTURE_CHANNELS))
    channels.update(cpd_channels_from_rows(_read_csv(directory / FIXTURE_CPD)))
    return meetings, key_rate, channels


def main(argv: Optional[list] = None) -> int:
    """CLI среза: Task 2 умеет только писать офлайн-фикстуры.

    Веса и классификатор Task 3 — библиотечный слой (`walk_forward_weights`,
    `walk_forward_classifier`, `classifier_metrics`); полный CLI (артефакты,
    `--features`, `--min-train`, `ML_LOG`) — Task 5. Здесь ранний каркас,
    чтобы фикстуры регенерировались той же командой, что у `key_rate_model`.
    """
    parser = argparse.ArgumentParser(
        description="Классификатор шоков ключевой ставки: панель заседаний, "
        "as-of каналы, причинные веса и классификатор (Tasks 2–3)."
    )
    parser.add_argument(
        "--export-fixtures",
        metavar="DIR",
        default=None,
        help="записать синтетические CSV-фикстуры офлайн-тестов в каталог DIR",
    )
    args = parser.parse_args(argv)
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
