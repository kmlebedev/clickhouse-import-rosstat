#!/usr/bin/env python3
"""Тесты scripts/models/cpi/cpi_monthly.py (Task 1: данные, as-of, выборка, признаки).

Запуск из корня репозитория (pytest не нужен):

    uv run --with "catboost>=1.2" --with scikit-learn --with pandas \
        --with "statsmodels>=0.15" --with scipy --python 3.12 \
        python3 scripts/models/cpi/cpi_monthly_test.py -v

Набор ОФЛАЙН: обращения к ClickHouse подменяются `unittest.mock`, данные берутся
из фикстур `scripts/testdata/cpi_monthly/*.csv`. Живая проверка — отдельный шаг
`make cpi-monthly`.

ФИКСТУРЫ ПЕРЕСОБИРАЮТСЯ ИЗ ЖИВОГО DEV-CLICKHOUSE (пользователь default):

    make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"

Окна срезов: 2013-01…2014-12 (`*_2013_2014.csv`) — горизонт начала выборки
(2013-10 требует меток 2013-08 и 2013-09, МП за 2013-08/2013-09 и квартальной
точки Q2-2013) и 2026-07…2026-09 (`*_recent.csv`) — свежая граница выборки
(месяц 2026-08 и квартальная точка Q2-2026). Файлы коммитятся: тесты обязаны
воспроизводиться без сети и БД.
"""

import contextlib
import csv
import hashlib
import io
import json
import math
import multiprocessing
import os
import pickle
import random
import statistics
import sys
import tempfile
import unittest
import unittest.mock
import warnings
from concurrent.futures import ProcessPoolExecutor
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV as SkElasticNetCV

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_monthly as cm

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "testdata" / "cpi_monthly"

FIXTURE_NAMES = (
    "ipc_mes_2013_2014.csv",
    "survey_2013_2014.csv",
    "usd_2013_2014.csv",
    "key_rate_2013_2014.csv",
    "fao_2013_2014.csv",
    "ipc_mes_recent.csv",
    "survey_recent.csv",
    "usd_recent.csv",
    "key_rate_recent.csv",
    "fao_recent.csv",
)

ONE_DAY = timedelta(days=1)


# --- Фикстуры и транспорт -----------------------------------------------------


def fixture_rows(name: str, directory: Path = FIXTURE_DIR) -> list:
    """Строки CSV-фикстуры словарями — в той же форме, что отдаёт ch_query."""
    with (directory / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fixture_for_window(sql: str, stem: str) -> list:
    """Фикстура окна по датам в SQL: `*_2013_2014.csv` или `*_recent.csv`.

    Живой SQL загрузчиков окна не содержит (читает ряд целиком) — ему отвечает
    основная фикстура 2013–2014; фикстурный SQL экспорта несёт границы окна.
    """
    if "date >= '2026-07-01'" in sql:
        return fixture_rows(f"{stem}_recent.csv")
    return fixture_rows(f"{stem}_2013_2014.csv")


def ch_query_from_fixtures(sql: str) -> list:
    """Транспорт-заглушка: живой SQL → строки соответствующей фикстуры."""
    if "FROM rosstat_ipc_mes" in sql:
        return fixture_for_window(sql, "ipc_mes")
    if "FROM macro_series" in sql:
        return fixture_for_window(sql, "survey")
    if "FROM cbr_currency_usd" in sql:
        return fixture_for_window(sql, "usd")
    if "FROM cbr_key_rate" in sql:
        return fixture_for_window(sql, "key_rate")
    if "FROM fao_food_price_Indices_monthly" in sql:
        return fixture_for_window(sql, "fao")
    raise AssertionError(f"неизвестный SQL: {sql}")


@contextlib.contextmanager
def fixture_transport():
    """Подменяет ch_query фикстурным транспортом на время блока."""
    with unittest.mock.patch.object(cm, "ch_query", ch_query_from_fixtures):
        yield


def fixture_targets() -> dict:
    return cm.targets_from_rows(fixture_rows("ipc_mes_2013_2014.csv"))


def fixture_survey() -> dict:
    return cm.survey_from_rows(fixture_rows("survey_2013_2014.csv"))


def fixture_usd() -> list:
    return cm.usd_from_rows(fixture_rows("usd_2013_2014.csv"))


def fixture_key_rate() -> list:
    return cm.key_rate_from_rows(fixture_rows("key_rate_2013_2014.csv"))


def fixture_fao() -> list:
    return cm.fao_from_rows(fixture_rows("fao_2013_2014.csv"))


def fixture_sample() -> tuple:
    return cm.build_sample(
        fixture_targets(), fixture_survey(), fixture_usd(), fixture_key_rate(), fixture_fao()
    )


def summary_value(text: str, label: str) -> str:
    """Значение строки сводки по её подписи (без привязки к отступам)."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(label):
            return stripped[len(label):].strip()
    raise AssertionError(f"нет строки {label!r} в сводке:\n{text}")


# --- Синтетический мир --------------------------------------------------------


def month_range(first: date, last: date) -> list:
    """Первые числа месяцев от `first` до `last` включительно."""
    months = []
    day = cm.month_of(first)
    while day <= last:
        months.append(day)
        day = cm._month_shift(day, 1)
    return months


def synthetic_targets(months: list = None) -> dict:
    """Таргет четырёх рядов; по умолчанию — полный горизонт выборки.

    Горизонт начинается на два месяца раньше `SAMPLE_FIRST`: первому месяцу
    выборки нужны `y_lag1`/`y_lag2`.
    """
    months = months or month_range(cm._month_shift(cm.SAMPLE_FIRST, -2), cm.SAMPLE_LAST)
    targets = {}
    for code_index, code in enumerate(cm.TARGET_SERIES):
        targets[code] = [
            (cm.month_end(month), 0.5 + 0.001 * position + 0.01 * code_index)
            for position, month in enumerate(months)
        ]
    return targets


def synthetic_survey(months: list = None) -> dict:
    """Пять исходных рядов МП: три месячных и два квартальных.

    Месячные — с `SAMPLE_FIRST − 2` (первому месяцу нужны уровни M−1 и M−2),
    квартальные — с 2013-07-01 (Q2-2013) до 2026-07-01 (Q2-2026) шагом квартал.
    """
    months = months or month_range(cm._month_shift(cm.SAMPLE_FIRST, -2), cm.SAMPLE_LAST)
    survey = {name: [] for name in cm.MP_SOURCE_SERIES}
    for series_index, name in enumerate(
        ("price_expectations_sa", "business_climate_sa", "credit_conditions")
    ):
        for position, month in enumerate(months):
            survey[name].append((month, 10.0 + series_index + 0.1 * position))
    quarterly = month_range(date(2013, 7, 1), cm.SAMPLE_LAST)
    for series_index, name in enumerate(
        ("capacity_utilization_sa", "headcount_expectations_sa")
    ):
        for position, month in enumerate(quarterly[::3]):
            survey[name].append((month, 60.0 + series_index + 0.01 * position))
    return {name: sorted(points) for name, points in survey.items()}


def synthetic_usd() -> list:
    """Дневной курс от 2013-08-01 до конца горизонта выборки."""
    start = date(2013, 8, 1)
    end = cm.month_end(cm.SAMPLE_LAST)
    points = []
    day = start
    while day <= end:
        points.append((day, 60.0 + 0.001 * len(points)))
        day += ONE_DAY
    return points


def synthetic_key_rate() -> list:
    """Месячные точки ставки с 2013-09-01 (первый cutoff — 2013-11-08)."""
    return [
        (month, 8.0 + 0.01 * position)
        for position, month in enumerate(
            month_range(date(2013, 9, 1), cm.SAMPLE_LAST)
        )
    ]


def synthetic_fao() -> list:
    """Месячные точки FAO: дата — 1-е число публикации, значение за M−1."""
    return [
        (month, 100.0 + position)
        for position, month in enumerate(month_range(date(2013, 1, 1), date(2026, 9, 1)))
    ]


def synthetic_world(**overrides) -> dict:
    world = {
        "targets": synthetic_targets(),
        "survey": synthetic_survey(),
        "usd": synthetic_usd(),
        "key_rate": synthetic_key_rate(),
        "fao": synthetic_fao(),
    }
    world.update(overrides)
    return world


def build_world(world: dict) -> tuple:
    return cm.build_sample(
        world["targets"], world["survey"], world["usd"], world["key_rate"], world["fao"]
    )


# --- Синтетический long-мир (спека §8.2) --------------------------------------


def long_month_span() -> list:
    """Месяцы long-выборки: 2002-05…2026-08 (292 месяца)."""
    first, last = cm.SAMPLE_BOUNDS["long"]
    return month_range(first, last)


def synthetic_long_usd() -> list:
    """Дневной курс long-мира: с 2013-01-01 (как живой `cbr_currency_usd`).

    Поэтому `usd_mm` первого месяца 2013-03 (корзины января и февраля), а до
    него признак обязан быть NaN, а не причиной пропуска.
    """
    points = []
    day = date(2013, 1, 1)
    end = cm.month_end(cm.SAMPLE_BOUNDS["long"][1])
    while day <= end:
        points.append((day, 60.0 + 0.001 * len(points)))
        day += ONE_DAY
    return points


def synthetic_long_key_rate() -> list:
    """Ставка long-мира: первая точка 2013-09-17 (как живой `cbr_key_rate`).

    До этой даты `key_rate` — NaN; первый месяц, чей cutoff её видит, —
    2013-09 (cutoff 2013-10-08).
    """
    points = [(date(2013, 9, 17), 8.0)]
    for position, month in enumerate(
        month_range(date(2013, 10, 1), cm.SAMPLE_BOUNDS["long"][1])
    ):
        points.append((month, 8.0 + 0.01 * (position + 1)))
    return points


def synthetic_long_world() -> dict:
    """Полный синтетический long-мир: 292 месяца, 0 пропусков.

    Таргет и МП начинаются за два месяца до старта long (2002-03), FAO — с
    2001-10 (первому cutoff нужна пара точек), квартальные МП — с 2001-10
    (Q1-2002 доступна 2002-05-31, то есть ровно к cutoff первого месяца).
    Контроли `usd`/`key_rate` намеренно короткие (2013), как живые ряды: их
    отсутствие даёт NaN, а не пропуск месяца.
    """
    first = cm._month_shift(cm.SAMPLE_BOUNDS["long"][0], -2)
    last = cm.SAMPLE_BOUNDS["long"][1]
    targets = synthetic_targets(month_range(first, last))
    survey = synthetic_survey(month_range(first, last))
    survey["capacity_utilization_sa"] = [
        (cm._month_shift(date(2001, 10, 1), 3 * position), 60.0 + 0.01 * position)
        for position in range(len(month_range(date(2001, 10, 1), last)) // 3)
    ]
    survey["headcount_expectations_sa"] = [
        (month, 1.0 + 0.01 * position)
        for position, (month, _) in enumerate(survey["capacity_utilization_sa"])
    ]
    fao = [
        (month, 100.0 + position)
        for position, month in enumerate(
            month_range(date(2001, 10, 1), cm._month_shift(last, 1))
        )
    ]
    return {
        "targets": targets,
        "survey": {name: sorted(points) for name, points in survey.items()},
        "usd": synthetic_long_usd(),
        "key_rate": synthetic_long_key_rate(),
        "fao": fao,
    }


def build_long_world(world: dict = None) -> tuple:
    """`build_sample(..., sample="long")` на синтетическом long-мире."""
    world = world or synthetic_long_world()
    return cm.build_sample(
        world["targets"],
        world["survey"],
        world["usd"],
        world["key_rate"],
        world["fao"],
        sample="long",
    )


def same_row(left: cm.Row, right: cm.Row) -> bool:
    """Равенство строк с NaN-осознанным сравнением (`nan == nan` — True).

    Обычное равенство `Row` не подходит: два независимо созданных NaN не равны
    друг другу, а детерминизм long-выборки проверяется именно на строках с NaN.
    """
    def same(value, other) -> bool:
        if isinstance(value, float) and math.isnan(value):
            return isinstance(other, float) and math.isnan(other)
        return value == other

    return (
        left.month == right.month
        and left.y == right.y
        and left.features.keys() == right.features.keys()
        and all(same(left.features[name], right.features[name]) for name in left.features)
    )


def survey_world(months: list, quarterly: dict = None) -> dict:
    """Полный набор пяти рядов МП для ручных проверок `mp_features`.

    Месячные уровни — `10 + 0.1·позиция` (`price_expectations_sa`), `1 + 0.1·`
    (`business_climate_sa`), `−5 − 0.1·` (`credit_conditions`).
    """
    survey = {name: [] for name in cm.MP_SOURCE_SERIES}
    for series_index, name in enumerate(
        ("price_expectations_sa", "business_climate_sa", "credit_conditions")
    ):
        for position, month in enumerate(months):
            base = (10.0, 1.0, -5.0)[series_index]
            survey[name].append((month, base + 0.1 * position))
    for name, points in (quarterly or {}).items():
        survey[name] = list(points)
    return survey


class TestConstants(unittest.TestCase):
    """Константы-контракт с Tasks 2–4 (спека 2026-10-18, §2)."""

    def test_seed_and_sample_bounds_are_pinned(self):
        self.assertEqual(cm.SEED, 20261018)
        self.assertEqual(cm.SAMPLE_FIRST, date(2013, 10, 1))
        self.assertEqual(cm.SAMPLE_LAST, date(2026, 8, 1))
        self.assertEqual(len(month_range(cm.SAMPLE_FIRST, cm.SAMPLE_LAST)), 155)

    def test_target_series_are_pinned_verbatim(self):
        self.assertEqual(cm.TARGET_SERIES, {
            "cpi_all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
            "cpi_food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
            "cpi_nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
            "cpi_services": "Индексы потребительских цен на услуги по Российской Федерации",
        })
        self.assertEqual(cm.TARGET_ALL, "cpi_all")
        self.assertEqual(
            cm.BOTTOMUP_COMPONENTS, ("cpi_food", "cpi_nonfood", "cpi_services")
        )

    def test_mp_features_and_source_series_are_pinned(self):
        # 7 признаков МП (порядок — контракт `feature_names(with_mp=True)`).
        self.assertEqual(tuple(cm.MP_SERIES), (
            "price_expectations_sa",
            "price_expectations_sa_chg",
            "business_climate_sa",
            "business_climate_sa_chg",
            "capacity_utilization_sa",
            "headcount_expectations_sa",
            "credit_conditions",
        ))
        # 5 исходных рядов cbr_survey: `_chg` — производные, их в SQL нет.
        self.assertEqual(tuple(cm.MP_SOURCE_SERIES), (
            "price_expectations_sa",
            "business_climate_sa",
            "capacity_utilization_sa",
            "headcount_expectations_sa",
            "credit_conditions",
        ))
        self.assertEqual(len(cm.MP_SERIES), 7)
        self.assertEqual(len(cm.MP_SOURCE_SERIES), 5)
        for derived in ("price_expectations_sa_chg", "business_climate_sa_chg"):
            self.assertNotIn(derived, cm.MP_SOURCE_SERIES)
        self.assertIn("capacity_utilization_sa", cm.MP_QUARTERLY)
        self.assertIn("headcount_expectations_sa", cm.MP_QUARTERLY)

    def test_feature_names_order(self):
        # Базовый набор — §2.4: 5 числовых + `month` последним.
        self.assertEqual(cm.feature_names(with_mp=False), [
            "y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm", "month",
        ])
        # MP-набор добавляет 7 имён ПЕРЕД `month`; базовые не сдвигаются.
        self.assertEqual(cm.feature_names(with_mp=True), [
            "y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm",
            "price_expectations_sa", "price_expectations_sa_chg",
            "business_climate_sa", "business_climate_sa_chg",
            "capacity_utilization_sa", "headcount_expectations_sa",
            "credit_conditions", "month",
        ])
        self.assertEqual(
            cm.feature_names(with_mp=True),
            cm.feature_names(with_mp=False)[:-1] + list(cm.MP_SERIES) + ["month"],
        )

    def test_fao_series_is_pinned(self):
        self.assertEqual(cm.FAO_SERIES, "Food Price Index")


class TestMonthArithmetic(unittest.TestCase):
    """Календарная арифметика месяцев (спека §2.2)."""

    def test_month_helpers(self):
        self.assertEqual(cm.month_of(date(2022, 1, 15)), date(2022, 1, 1))
        self.assertEqual(cm.month_end(date(2022, 1, 15)), date(2022, 1, 31))
        self.assertEqual(cm.month_end(date(2022, 2, 1)), date(2022, 2, 28))
        self.assertEqual(cm.month_end(date(2020, 2, 10)), date(2020, 2, 29))
        self.assertEqual(cm.month_end(date(2022, 12, 31)), date(2022, 12, 31))
        self.assertEqual(cm.previous_month(date(2022, 1, 1)), date(2021, 12, 1))
        self.assertEqual(cm.previous_month(date(2022, 3, 31)), date(2022, 2, 1))

    def test_cutoff_is_eighth_of_next_month(self):
        self.assertEqual(cm.cutoff_for(date(2022, 1, 1)), date(2022, 2, 8))
        # Декабрь → январь следующего года.
        self.assertEqual(cm.cutoff_for(date(2022, 12, 1)), date(2023, 1, 8))
        self.assertEqual(cm.cutoff_for(date(2026, 8, 1)), date(2026, 9, 8))
        # День внутри месяца значения не имеет.
        self.assertEqual(cm.cutoff_for(date(2022, 1, 31)), date(2022, 2, 8))


class TestAvailabilityRules(unittest.TestCase):
    """Правила §2.6 и их границы."""

    def test_available_from_pinned(self):
        day = date(2022, 3, 10)
        self.assertEqual(cm.available_from_mes(date(2022, 1, 31)), date(2022, 2, 28))
        self.assertEqual(cm.available_from_mes(date(2022, 12, 31)), date(2023, 1, 31))
        self.assertEqual(cm.available_from_usd(day), date(2022, 3, 11))
        self.assertEqual(cm.available_from_rate(day), day)
        self.assertEqual(cm.available_from_fao(day), date(2022, 3, 16))

    def test_survey_available_from_end_of_next_month(self):
        # Точка P=2013-09-01 доступна с конца P+1 — 2013-10-31; граница включительна.
        self.assertEqual(
            cm.available_from_survey(date(2013, 9, 1)), date(2013, 10, 31)
        )
        self.assertEqual(
            cm.available_from_survey(date(2013, 12, 1)), date(2014, 1, 31)
        )
        self.assertEqual(
            cm.available_from_survey(date(2013, 1, 15)), date(2013, 2, 28)
        )
        # `asof`-граница: на 2013-10-31 точка участвует, на 2013-10-30 — нет.
        points = [(date(2013, 9, 1), 7.0)]
        self.assertEqual(
            cm.asof(points, cm.available_from_survey, date(2013, 10, 31)), 7.0
        )
        self.assertIsNone(cm.asof(points, cm.available_from_survey, date(2013, 10, 30)))

    def test_survey_month_m_not_available_at_cutoff_m_plus_1(self):
        # Точка M не проходит available_from_survey ≤ cutoff_for(M):
        # конец M+1 > 8-е M+1 (именно это защищает от утечки МП месяца M).
        point = date(2026, 7, 1)
        cutoff = cm.cutoff_for(date(2026, 7, 1))
        self.assertEqual(cutoff, date(2026, 8, 8))
        self.assertGreater(cm.available_from_survey(point), cutoff)
        self.assertIsNone(cm.asof([(point, 1.0)], cm.available_from_survey, cutoff))
        # А точка M−1 к этому же cutoff уже доступна.
        previous = date(2026, 6, 1)
        self.assertLessEqual(cm.available_from_survey(previous), cutoff)
        self.assertEqual(
            cm.asof([(previous, 2.0)], cm.available_from_survey, cutoff), 2.0
        )


class TestRowParsing(unittest.TestCase):
    """Разбор строк и отказы на противоречивых данных (fail-fast)."""

    def test_row_date_handles_iso_datetime_and_date(self):
        self.assertEqual(cm.row_date("2013-10-31"), date(2013, 10, 31))
        self.assertEqual(cm.row_date("2013-10-31T00:00:00"), date(2013, 10, 31))
        self.assertEqual(cm.row_date(datetime(2013, 10, 31, 12, 30)), date(2013, 10, 31))
        self.assertEqual(cm.row_date(date(2013, 10, 31)), date(2013, 10, 31))

    def test_row_float_rejects_empty_cell(self):
        self.assertEqual(cm.row_float("100.5"), 100.5)
        with self.assertRaises(ValueError):
            cm.row_float("")
        with self.assertRaises(ValueError):
            cm.row_float(None)

    def test_two_rows_per_date_is_error(self):
        # Дубль даты ряда означает две точки одного дня: as-of выбрал бы одну по
        # порядку ответа. Это отказ, а не «последний победил».
        duplicate_usd = [
            {"date": "2013-10-01", "price": "70.0"},
            {"date": "2013-10-01", "price": "71.0"},
        ]
        with self.assertRaises(ValueError):
            cm.usd_from_rows(duplicate_usd)
        duplicate_survey = [
            {"series": "price_expectations_sa", "date": "2013-10-01", "value": "10.0"},
            {"series": "price_expectations_sa", "date": "2013-10-01", "value": "11.0"},
        ]
        with self.assertRaises(ValueError):
            cm.survey_from_rows(duplicate_survey)

    def test_percent_minus_100(self):
        rows = [
            {"name": name, "date": "2013-10-31", "percent": "100.57"}
            for name in cm.TARGET_SERIES.values()
        ]
        targets = cm.targets_from_rows(rows)
        for code in cm.TARGET_SERIES:
            self.assertEqual(len(targets[code]), 1)
            point_date, value = targets[code][0]
            self.assertEqual(point_date, date(2013, 10, 31))
            self.assertAlmostEqual(value, 0.57, places=9)

    def test_targets_require_all_four_series(self):
        rows = [
            {"name": name, "date": "2013-10-31", "percent": "100.5"}
            for code, name in cm.TARGET_SERIES.items()
            if code != "cpi_services"
        ]
        with self.assertRaises(ValueError):
            cm.targets_from_rows(rows)

    def test_survey_requires_all_five_source_series(self):
        rows = [
            {"series": name, "date": "2013-10-01", "value": "1.0"}
            for name in cm.MP_SOURCE_SERIES
            if name != "credit_conditions"
        ]
        with self.assertRaises(ValueError):
            cm.survey_from_rows(rows)

    def test_foreign_rows_are_ignored(self):
        # SQL фильтрует ряды, Python-фильтр — страховка от чужой фикстуры.
        survey = cm.survey_from_rows([
            {"series": name, "date": "2013-10-01", "value": "1.0"}
            for name in cm.MP_SOURCE_SERIES
        ] + [{"series": "staff_availability", "date": "2013-10-01", "value": "999.0"}])
        self.assertNotIn("staff_availability", survey)
        fao = cm.fao_from_rows([
            {"name": cm.FAO_SERIES, "date": "2013-10-01", "price_index": "120.0"},
            {"name": "Cereals", "date": "2013-10-01", "price_index": "999.0"},
        ])
        self.assertEqual(fao, [(date(2013, 10, 1), 120.0)])


class TestBaseFeatures(unittest.TestCase):
    """Базовые признаки §2.4 — формула и границы доступности."""

    def test_lags_are_m_minus_1_and_2(self):
        months = month_range(date(2013, 8, 1), date(2013, 11, 1))
        targets = {
            code: [(cm.month_end(month), 0.1 * position) for position, month in enumerate(months)]
            for code in cm.TARGET_SERIES
        }
        features, reason = cm.base_features(
            date(2013, 10, 1),
            cm.target_index(targets),
            synthetic_usd(),
            synthetic_key_rate(),
            synthetic_fao(),
            cm.cutoff_for(date(2013, 10, 1)),
        )
        self.assertEqual(reason, "")
        self.assertAlmostEqual(features["y_lag1"], 0.1 * 1)  # 2013-09
        self.assertAlmostEqual(features["y_lag2"], 0.1 * 0)  # 2013-08
        self.assertEqual(features["month"], 10)

    def test_usd_mm_uses_monthly_averages(self):
        # Август: (60 + 62) / 2 = 61; сентябрь: (70 + 72) / 2 = 71.
        usd = [
            (date(2013, 8, 1), 60.0),
            (date(2013, 8, 20), 62.0),
            (date(2013, 9, 2), 70.0),
            (date(2013, 9, 30), 72.0),
            # Точка октября в средние не попадает вовсе.
            (date(2013, 10, 1), 999.0),
        ]
        self.assertAlmostEqual(
            cm.usd_mm(date(2013, 10, 1), usd), 100.0 * math.log(71.0 / 61.0), places=12
        )

    def test_usd_mm_requires_both_baskets(self):
        usd = [(date(2013, 9, 2), 70.0), (date(2013, 9, 30), 72.0)]
        self.assertIsNone(cm.usd_mm(date(2013, 10, 1), usd))

    def test_fao_mm_uses_publication_dates(self):
        # Значение под датой 1-го числа месяца публикации — индекс за M−1;
        # для месяца 2013-10 последняя доступная пара к cutoff 2013-11-08 —
        # 2013-10-01/2013-11-01 (публикация 11-01 доступна 11-07).
        points = [
            (date(2013, 10, 1), 120.0),
            (date(2013, 11, 1), 126.0),
            (date(2013, 12, 1), 999.0),  # доступна 12-07 > cutoff — не участвует
        ]
        self.assertAlmostEqual(
            cm.fao_mm(points, cm.cutoff_for(date(2013, 10, 1))),
            (126.0 / 120.0 - 1.0) * 100.0,
            places=12,
        )
        self.assertIsNone(cm.fao_mm(points[:1], cm.cutoff_for(date(2013, 10, 1))))

    def test_missing_base_point_returns_reason(self):
        targets = {
            code: [(cm.month_end(date(2013, 8, 1)), 0.5)]
            for code in cm.TARGET_SERIES
        }
        features, reason = cm.base_features(
            date(2013, 10, 1),
            cm.target_index(targets),
            synthetic_usd(),
            synthetic_key_rate(),
            synthetic_fao(),
            cm.cutoff_for(date(2013, 10, 1)),
        )
        self.assertIsNone(features)
        self.assertIn("y_lag1", reason)


class TestMpFeatures(unittest.TestCase):
    """Признаки МП §2.5: уровни M−1, `_chg`, квартальный перенос вперёд."""

    MONTHS = month_range(date(2019, 12, 1), date(2020, 8, 1))
    QUARTERLY = {
        "capacity_utilization_sa": [
            (date(2020, 1, 1), 70.0),   # Q4-2019, доступна 2020-02-29
            (date(2020, 4, 1), 71.0),   # Q1-2020, доступна 2020-05-31
            (date(2020, 7, 1), 72.0),   # Q2-2020, доступна 2020-08-31
        ],
        "headcount_expectations_sa": [
            (date(2020, 1, 1), 1.0),
            (date(2020, 4, 1), 2.0),
            (date(2020, 7, 1), 3.0),
        ],
    }

    def features_for(self, month, survey, cutoff=None):
        return cm.mp_features(
            month, survey, cutoff or cm.cutoff_for(month)
        )

    def test_levels_and_changes_from_m_minus_1(self):
        survey = survey_world(self.MONTHS, self.QUARTERLY)
        features, reason = self.features_for(date(2020, 4, 1), survey)
        self.assertEqual(reason, "")
        # Месячные уровни — март (M−1) и февраль (M−2); значения из survey_world.
        self.assertAlmostEqual(features["price_expectations_sa"], 10.3)
        self.assertAlmostEqual(features["price_expectations_sa_chg"], 0.1)
        self.assertAlmostEqual(features["business_climate_sa"], 1.3)
        self.assertAlmostEqual(features["business_climate_sa_chg"], 0.1)
        self.assertAlmostEqual(features["credit_conditions"], -4.7)
        # Квартальная точка Q1-2020 (2020-04-01) ещё не опубликована на
        # cutoff 2020-05-08 — переносится вперёд Q4-2019 (2020-01-01).
        self.assertAlmostEqual(features["capacity_utilization_sa"], 70.0)
        self.assertAlmostEqual(features["headcount_expectations_sa"], 1.0)
        self.assertEqual(set(features), set(cm.MP_SERIES))

    def test_quarterly_series_forward_filled(self):
        survey = survey_world(self.MONTHS, self.QUARTERLY)
        # Q1-2020 (2020-04-01) доступна с 2020-05-31: месяц 2020-06 её видит,
        # месяц 2020-05 — ещё нет (cutoff 2020-06-08 < 2020-05-31? нет:
        # 2020-05-31 ≤ 2020-06-08 — видит; проверим границу на Q2-2020).
        june = self.features_for(date(2020, 6, 1), survey)[0]
        july = self.features_for(date(2020, 7, 1), survey)[0]
        august = self.features_for(date(2020, 8, 1), survey)[0]
        self.assertAlmostEqual(june["capacity_utilization_sa"], 71.0)
        # Q2-2020 (2020-07-01) доступна 2020-08-31 > cutoff 2020-08-08: в июле
        # держится Q1-2020 — квартальный перенос вперёд.
        self.assertAlmostEqual(july["capacity_utilization_sa"], 71.0)
        self.assertAlmostEqual(july["headcount_expectations_sa"], 2.0)
        # В августе Q2 уже доступна (2020-08-31 ≤ cutoff 2020-09-08).
        self.assertAlmostEqual(august["capacity_utilization_sa"], 72.0)
        self.assertAlmostEqual(august["headcount_expectations_sa"], 3.0)

    def test_quarterly_series_not_taken_after_cutoff(self):
        survey = survey_world(self.MONTHS, self.QUARTERLY)
        month = date(2020, 7, 1)
        cutoff = cm.cutoff_for(month)
        features, _ = self.features_for(month, survey, cutoff)
        # Точка 2020-07-01 (Q2) доступна 2020-08-31, cutoff 2020-08-08 — не участвует.
        self.assertGreater(cm.available_from_survey(date(2020, 7, 1)), cutoff)
        self.assertAlmostEqual(features["capacity_utilization_sa"], 71.0)

    def test_missing_monthly_point_returns_reason(self):
        survey = survey_world(self.MONTHS, self.QUARTERLY)
        survey["price_expectations_sa"] = [
            point for point in survey["price_expectations_sa"]
            if point[0] != date(2020, 3, 1)
        ]
        features, reason = self.features_for(date(2020, 4, 1), survey)
        self.assertIsNone(features)
        self.assertIn("price_expectations_sa", reason)
        # Без точки марта не собирается и `_chg` следующего месяца (M−2 = март).
        features, reason = self.features_for(date(2020, 5, 1), survey)
        self.assertIsNone(features)
        self.assertIn("price_expectations_sa_chg", reason)

    def test_missing_quarterly_point_returns_reason(self):
        survey = survey_world(self.MONTHS, self.QUARTERLY)
        survey["capacity_utilization_sa"] = [
            point for point in survey["capacity_utilization_sa"]
            if point[0] > date(2020, 4, 1)
        ]
        features, reason = self.features_for(date(2020, 4, 1), survey)
        self.assertIsNone(features)
        self.assertIn("capacity_utilization_sa", reason)


class TestBuildSample(unittest.TestCase):
    """Выборка: общие строки, пропуски с причиной, горизонт 155 месяцев."""

    def test_sample_covers_155_months(self):
        rows, skipped = build_world(synthetic_world())
        self.assertEqual(len(rows), 155)
        self.assertEqual(rows[0].month, date(2013, 10, 1))
        self.assertEqual(rows[-1].month, date(2026, 8, 1))
        self.assertEqual(skipped, [])
        self.assertEqual(
            [row.month for row in rows], month_range(date(2013, 10, 1), date(2026, 8, 1))
        )

    def test_months_outside_sample_bounds_are_not_candidates(self):
        # Таргет продолжается на сентябрь-2026, но выборка кончается 2026-08;
        # раньше 2013-10 — тоже не кандидаты.
        world = synthetic_world()
        world["targets"] = {
            code: points + [(cm.month_end(date(2026, 9, 1)), 0.9)]
            for code, points in world["targets"].items()
        }
        rows, _ = build_world(world)
        self.assertEqual(rows[-1].month, date(2026, 8, 1))
        self.assertEqual(rows[0].month, date(2013, 10, 1))

    def test_missing_survey_skips_month_for_both_sets(self):
        # Месяц без МП отсутствует в ОБЩЕМ списке строк: набор «без МП» не
        # получает его «в подарок» — иначе фолды наборов разъехались бы.
        world = synthetic_world()
        world["survey"]["price_expectations_sa"] = [
            point for point in world["survey"]["price_expectations_sa"]
            if point[0] != date(2020, 3, 1)
        ]
        rows, skipped = build_world(world)
        months = [row.month for row in rows]
        self.assertNotIn(date(2020, 4, 1), months)
        self.assertNotIn(date(2020, 5, 1), months)
        self.assertEqual([month for month, _ in skipped], [date(2020, 4, 1), date(2020, 5, 1)])
        self.assertTrue(all(reason for _, reason in skipped))
        self.assertEqual(len(rows), 153)

    def test_missing_usd_gives_nan_not_a_skipped_month(self):
        # Осознанная смена политики (спека §8.2): отсутствие usd_mm/key_rate —
        # NaN признака, а не причина пропуска месяца. Обязательными остаются
        # y_lag1/y_lag2/fao_mm (и признаки МП).
        world = synthetic_world()
        world["usd"] = [point for point in world["usd"] if cm.month_of(point[0]) != date(2020, 3, 1)]
        rows, skipped = build_world(world)
        self.assertEqual(skipped, [])
        self.assertEqual(len(rows), 155)
        by_month = {row.month: row for row in rows}
        # Апрелю нужны март и февраль, маю — апрель и март: без марта оба NaN.
        for month in (date(2020, 4, 1), date(2020, 5, 1)):
            self.assertTrue(
                math.isnan(by_month[month].features["usd_mm"]),
                f"{month.isoformat()}: usd_mm должен быть NaN, а не пропуск",
            )
        # Июню хватает мая и апреля — признак снова заполнен.
        self.assertFalse(math.isnan(by_month[date(2020, 6, 1)].features["usd_mm"]))

    def test_missing_key_rate_gives_nan_not_a_skipped_month(self):
        # Та же NaN-политика §8.2 для второго контроля: пустой ряд ставки не
        # выбрасывает месяцы, а даёт NaN в key_rate.
        world = synthetic_world(key_rate=[])
        rows, skipped = build_world(world)
        self.assertEqual(skipped, [])
        self.assertEqual(len(rows), 155)
        self.assertTrue(all(math.isnan(row.features["key_rate"]) for row in rows))

    def test_rows_carry_both_feature_sets_and_all_labels(self):
        rows, _ = build_world(synthetic_world())
        for row in rows:
            self.assertEqual(sorted(row.features), sorted(cm.feature_names(with_mp=True)))
            self.assertEqual(sorted(row.y), sorted(cm.TARGET_SERIES))
            self.assertIn("y_lag1", row.features)
            self.assertIn("price_expectations_sa_chg", row.features)

    def test_lags_are_taken_from_previous_rows(self):
        rows, _ = build_world(synthetic_world())
        for previous, current in zip(rows, rows[1:]):
            self.assertAlmostEqual(current.features["y_lag1"], previous.y[cm.TARGET_ALL])
        for previous, current in zip(rows, rows[2:]):
            self.assertAlmostEqual(current.features["y_lag2"], previous.y[cm.TARGET_ALL])

    def test_assert_months_purged(self):
        rows, _ = build_world(synthetic_world())
        cm.assert_months_purged(rows)
        with self.assertRaises(AssertionError):
            cm.assert_months_purged(list(rows) + [rows[0]])

    def test_mutated_cutoff_is_caught_by_purge(self):
        rows, _ = build_world(synthetic_world())
        with unittest.mock.patch.object(
            cm, "cutoff_for", lambda month: cm.available_from_mes(cm.month_end(month))
        ):
            with self.assertRaises(AssertionError):
                cm.assert_months_purged(rows)


class TestLongSample(unittest.TestCase):
    """Long-sample §8.2: 292 месяца, NaN-контроли до 2013, сплит 200/67."""

    def test_short_sample_is_unchanged(self):
        # Принятая выборка 155 месяцев не меняется от появления long-sample.
        self.assertEqual(
            cm.SAMPLE_BOUNDS["short"], (date(2013, 10, 1), date(2026, 8, 1))
        )
        self.assertEqual(cm.SAMPLE_FIRST, date(2013, 10, 1))
        self.assertEqual(cm.SAMPLE_LAST, date(2026, 8, 1))
        rows, skipped = build_world(synthetic_world())  # default sample="short"
        self.assertEqual(skipped, [])
        self.assertEqual(len(rows), 155)
        self.assertEqual(rows[0].month, date(2013, 10, 1))
        self.assertEqual(rows[-1].month, date(2026, 8, 1))
        # В short-выборке оба контроля заполнены всегда.
        self.assertEqual(cm.nan_control_counts(rows), (0, 0))

    def test_long_sample_covers_292_months(self):
        self.assertEqual(
            cm.SAMPLE_BOUNDS["long"], (date(2002, 5, 1), date(2026, 8, 1))
        )
        rows, skipped = build_long_world()
        self.assertEqual(skipped, [])
        self.assertEqual(len(rows), 292)
        self.assertEqual(rows[0].month, date(2002, 5, 1))
        self.assertEqual(rows[-1].month, date(2026, 8, 1))
        self.assertEqual([row.month for row in rows], long_month_span())
        cm.assert_months_purged(rows)

    def test_long_sample_nan_controls_before_2013(self):
        rows, _ = build_long_world()
        by_month = {row.month: row for row in rows}
        # USD (живой ряд с 2013-01): первая пара корзин M−1/M−2 — у 2013-03;
        # 2013-02 не пропущен, а имеет NaN в usd_mm.
        self.assertIn(date(2013, 2, 1), by_month)
        for month in long_month_span():
            self.assertEqual(
                math.isnan(by_month[month].features["usd_mm"]),
                month < date(2013, 3, 1),
                f"usd_mm месяца {month.isoformat()}",
            )
        # Ставка: первая точка 2013-09-17; месяц 2013-09 — первый, чей cutoff
        # (2013-10-08) её видит. До этого — NaN, а не пропуск месяца.
        for month in long_month_span():
            self.assertEqual(
                math.isnan(by_month[month].features["key_rate"]),
                cm.cutoff_for(month) < date(2013, 9, 17),
                f"key_rate месяца {month.isoformat()}",
            )
        self.assertTrue(math.isnan(by_month[date(2013, 2, 1)].features["key_rate"]))

    def test_long_sample_controls_after_2013_are_filled(self):
        rows, _ = build_long_world()
        by_month = {row.month: row for row in rows}
        months = long_month_span()
        for month in months[months.index(date(2013, 3, 1)):]:
            self.assertFalse(
                math.isnan(by_month[month].features["usd_mm"]),
                f"usd_mm месяца {month.isoformat()}",
            )
        for month in months[months.index(date(2013, 9, 1)):]:
            self.assertFalse(
                math.isnan(by_month[month].features["key_rate"]),
                f"key_rate месяца {month.isoformat()}",
            )
        # NaN ровно в префиксах: 2002-05…2013-02 (130) и 2002-05…2013-08 (136).
        self.assertEqual(cm.nan_control_counts(rows), (130, 136))

    def test_long_enet_imputes_empty_control_columns(self):
        # Раннее long-окно (train до 2013): usd_mm и key_rate в train целиком
        # NaN. B3 обязан импутировать пустые колонки нулём (ENET_MEDIAN_FALLBACK),
        # а не падать: это то же NaN-правило §8.2, что у ofz_1y в v1.
        rows, _ = build_long_world()
        train = [row for row in rows if row.month < date(2013, 1, 1)]
        self.assertGreaterEqual(len(train), cm.ENET_MIN_TRAIN)
        value = cm._fit_enet(train, rows[len(train)], with_mp=False)
        self.assertTrue(math.isfinite(value))
        fit = cm._fit_enet.last_fit
        for name in ("usd_mm", "key_rate"):
            index = fit.features.index(name)
            self.assertEqual(fit.medians[index], cm.ENET_MEDIAN_FALLBACK)

    def test_long_default_start_is_48(self):
        self.assertEqual(cm.DEFAULT_START, 24)
        self.assertEqual(cm.DEFAULT_LONG_START, 48)
        self.assertEqual(cm.resolve_start("short"), 24)
        self.assertEqual(cm.resolve_start("long"), 48)
        # Явный --start переопределяет sample-дефолт.
        self.assertEqual(cm.resolve_start("long", 24), 24)
        self.assertEqual(cm.resolve_start("short", 60), 60)
        # Основное окно long — 48/0, робастность — 48/1 (дубликат отброшен).
        self.assertEqual(cm.window_specs(cm.resolve_start("long"), 0), [(48, 0), (48, 1)])
        with self.assertRaises(ValueError):
            cm.resolve_start("bogus")
        with self.assertRaises(ValueError):
            cm.validate_sample("bogus")

    def test_long_article_split_is_200_67(self):
        rows, _ = build_long_world()
        train = [
            row for row in rows if cm.month_of(row.month) <= cm.ARTICLE_TRAIN_END
        ]
        test = [
            row
            for row in rows
            if cm.ARTICLE_TRAIN_END < cm.month_of(row.month) <= cm.ARTICLE_TEST_END
        ]
        self.assertEqual(len(train), 200)
        self.assertEqual(len(test), 67)
        self.assertEqual(train[0].month, date(2002, 5, 1))
        self.assertEqual(train[-1].month, date(2018, 12, 1))
        self.assertEqual(test[0].month, date(2019, 1, 1))
        self.assertEqual(test[-1].month, date(2024, 7, 1))
        # fixed_split не менялся: на long он сам даёт 67 прогнозов-месяцев.
        predictions = cm.fixed_split(rows, {"stub": lambda fit_rows, row: 0.5})
        self.assertEqual(len(predictions), 67)
        self.assertEqual(
            {prediction.month for prediction in predictions},
            {row.month for row in test},
        )

    def test_long_sample_deterministic(self):
        first, first_skipped = build_long_world()
        second, second_skipped = build_long_world()
        self.assertEqual(first_skipped, second_skipped)
        self.assertEqual([row.month for row in first], [row.month for row in second])
        self.assertTrue(
            all(same_row(left, right) for left, right in zip(first, second))
        )
        self.assertEqual(cm.nan_control_counts(first), cm.nan_control_counts(second))

    def test_unknown_sample_is_rejected(self):
        world = synthetic_world()
        with self.assertRaises(ValueError):
            cm.build_sample(
                world["targets"],
                world["survey"],
                world["usd"],
                world["key_rate"],
                world["fao"],
                sample="quarterly",
            )
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cm.main(["--sample", "quarterly"])

    def test_long_report_names_sample_nan_and_split(self):
        rows, skipped = build_long_world()
        report = cm.render_report(
            rows, skipped, [], today=date(2026, 10, 18), sample="long"
        )
        self.assertIn("(long-sample)", report)
        self.assertIn("| sample | long (2002-05 … 2026-08) |", report)
        self.assertIn(
            "| NaN-контроли (`usd_mm` / `key_rate`) | 130 / 136 |", report
        )
        self.assertIn("200 мес.", report)
        self.assertIn("67 мес.", report)
        self.assertIn("NaN", report)
        # Short-отчёт остаётся прежним: другие строка sample/NaN и оговорка.
        short_rows, _ = build_world(synthetic_world())
        short_report = cm.render_report(
            short_rows, [], [], today=date(2026, 10, 18)
        )
        self.assertNotIn("(long-sample)", short_report)
        self.assertIn("| sample | short (2013-10 … 2026-08) |", short_report)
        self.assertIn(
            "| NaN-контроли (`usd_mm` / `key_rate`) | 0 / 0 |", short_report
        )
        self.assertIn("train короче статьи (63 против 120 месяцев)", short_report)


class TestFixturesSample(unittest.TestCase):
    """Выборка на закоммиченных фикстурах (тот же путь, что у живого прогона)."""

    def test_fixture_inventory(self):
        for name in FIXTURE_NAMES:
            rows = fixture_rows(name)
            self.assertTrue(rows, f"{name}: пустая фикстура")
            self.assertEqual(
                tuple(rows[0].keys()), cm.FIXTURE_COLUMNS[name], name
            )
        self.assertEqual(fixture_rows("ipc_mes_2013_2014.csv")[0]["date"], "2013-01-31")
        self.assertEqual(fixture_rows("ipc_mes_2013_2014.csv")[-1]["date"], "2014-12-31")
        self.assertEqual(fixture_rows("ipc_mes_recent.csv")[0]["date"], "2026-07-31")
        self.assertEqual(fixture_rows("ipc_mes_recent.csv")[-1]["date"], "2026-09-30")
        self.assertEqual(fixture_rows("survey_recent.csv")[0]["series"], "business_climate_sa")
        self.assertEqual(fixture_rows("survey_recent.csv")[-1]["series"], "price_expectations_sa")

    def test_sample_months_and_no_skips(self):
        rows, skipped = fixture_sample()
        self.assertEqual(
            [row.month for row in rows],
            month_range(date(2013, 10, 1), date(2014, 12, 1)),
        )
        self.assertEqual(len(rows), 15)
        self.assertEqual(skipped, [])

    def test_target_is_percent_minus_100(self):
        rows, _ = fixture_sample()
        october = rows[0]
        raw = [
            row for row in fixture_rows("ipc_mes_2013_2014.csv")
            if row["name"] == cm.TARGET_SERIES[cm.TARGET_ALL] and row["date"] == "2013-10-31"
        ]
        self.assertEqual(len(raw), 1)
        self.assertAlmostEqual(october.y[cm.TARGET_ALL], float(raw[0]["percent"]) - 100.0, places=9)
        self.assertAlmostEqual(october.y[cm.TARGET_ALL], 0.57, places=9)

    def test_lags_are_m_minus_1_and_2(self):
        rows, _ = fixture_sample()
        october = rows[0]
        raw = {
            row["date"]: float(row["percent"]) - 100.0
            for row in fixture_rows("ipc_mes_2013_2014.csv")
            if row["name"] == cm.TARGET_SERIES[cm.TARGET_ALL]
        }
        self.assertAlmostEqual(october.features["y_lag1"], raw["2013-09-30"], places=9)
        self.assertAlmostEqual(october.features["y_lag2"], raw["2013-08-31"], places=9)

    def test_usd_mm_uses_monthly_averages(self):
        rows, _ = fixture_sample()
        october = rows[0]
        points = fixture_usd()
        september = [value for day, value in points if cm.month_of(day) == date(2013, 9, 1)]
        august = [value for day, value in points if cm.month_of(day) == date(2013, 8, 1)]
        expected = 100.0 * math.log(statistics.mean(september) / statistics.mean(august))
        self.assertAlmostEqual(october.features["usd_mm"], expected, places=9)

    def test_fao_mm_uses_publication_dates(self):
        rows, _ = fixture_sample()
        october = rows[0]
        cutoff = cm.cutoff_for(october.month)
        points = fixture_fao()
        eligible = [
            (day, value) for day, value in points if cm.available_from_fao(day) <= cutoff
        ]
        self.assertEqual(eligible[-1][0], date(2013, 11, 1))
        self.assertEqual(eligible[-2][0], date(2013, 10, 1))
        self.assertNotIn(date(2013, 12, 1), [day for day, _ in eligible])
        expected = (eligible[-1][1] / eligible[-2][1] - 1.0) * 100.0
        self.assertAlmostEqual(october.features["fao_mm"], expected, places=9)

    def test_quarterly_series_forward_filled(self):
        rows, _ = fixture_sample()
        by_month = {row.month: row for row in rows}
        capacity = dict(fixture_survey()["capacity_utilization_sa"])
        # Q3-2013 (2013-10-01) доступна с 2013-11-30: октябрь видит Q2-2013
        # (2013-07-01), ноябрь — уже Q3; Q4 (2014-01-01) доступна 2014-02-28,
        # поэтому в декабре Q3 держится — перенос вперёд до публикации.
        self.assertAlmostEqual(
            by_month[date(2013, 10, 1)].features["capacity_utilization_sa"],
            capacity[date(2013, 7, 1)],
        )
        self.assertAlmostEqual(
            by_month[date(2013, 11, 1)].features["capacity_utilization_sa"],
            capacity[date(2013, 10, 1)],
        )
        self.assertAlmostEqual(
            by_month[date(2013, 12, 1)].features["capacity_utilization_sa"],
            capacity[date(2013, 10, 1)],
        )

    def test_quarterly_series_not_taken_after_cutoff(self):
        rows, _ = fixture_sample()
        october = rows[0]
        capacity = dict(fixture_survey()["capacity_utilization_sa"])
        self.assertNotAlmostEqual(
            october.features["capacity_utilization_sa"], capacity[date(2013, 10, 1)]
        )
        self.assertAlmostEqual(
            october.features["capacity_utilization_sa"], capacity[date(2013, 7, 1)]
        )

    def test_missing_survey_skips_month_for_both_sets(self):
        survey = dict(fixture_survey())
        survey["price_expectations_sa"] = [
            point for point in survey["price_expectations_sa"]
            if point[0] != date(2014, 3, 1)
        ]
        rows, skipped = cm.build_sample(
            fixture_targets(), survey, fixture_usd(), fixture_key_rate(), fixture_fao()
        )
        months = [row.month for row in rows]
        self.assertNotIn(date(2014, 4, 1), months)
        self.assertNotIn(date(2014, 5, 1), months)
        self.assertEqual([month for month, _ in skipped], [date(2014, 4, 1), date(2014, 5, 1)])
        self.assertEqual(len(rows), 13)


class TestExportFixtures(unittest.TestCase):
    """Экспорт фикстур: имена, колонки, LF, детерминированность."""

    def export_to(self, directory: Path) -> int:
        with fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
            return cm.export_fixtures(directory)

    def test_writes_all_ten_files_with_lf(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertEqual(self.export_to(directory), 0)
            self.assertEqual(
                sorted(path.name for path in directory.iterdir()), sorted(FIXTURE_NAMES)
            )
            for name in FIXTURE_NAMES:
                raw = (directory / name).read_bytes()
                self.assertNotIn(b"\r", raw, f"{name}: CRLF в фикстуре")
                self.assertTrue(raw, name)
                self.assertEqual(
                    tuple(fixture_rows(name, directory)[0].keys()),
                    cm.FIXTURE_COLUMNS[name],
                )

    def test_export_reproduces_committed_fixtures(self):
        # Идемпотентность: экспорт из тех же строк даёт те же байты (LF, порядок
        # колонок и строк — из ORDER BY SQL).
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.export_to(directory)
            for name in FIXTURE_NAMES:
                self.assertEqual(
                    (directory / name).read_bytes(),
                    (FIXTURE_DIR / name).read_bytes(),
                    f"{name}: повторный экспорт дал другой файл",
                )

    def test_fixture_sql_matches_live_columns(self):
        for sql in (
            cm.TARGETS_SQL, cm.SURVEY_SQL, cm.USD_SQL, cm.KEY_RATE_SQL, cm.FAO_SQL
        ):
            self.assertIn("FINAL", sql)
            self.assertIn("FORMAT JSON", sql)
        windowed = cm._window_sql(cm.SURVEY_SQL, date(2013, 1, 1), date(2014, 12, 31))
        self.assertIn("FINAL", windowed)
        self.assertIn("date >= '2013-01-01'", windowed)
        self.assertIn("date <= '2014-12-31'", windowed)
        self.assertIn("source = 'cbr_survey'", windowed)

    def test_main_export_fixtures(self):
        with tempfile.TemporaryDirectory() as tmp:
            with fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cm.main(["--export-fixtures", tmp]), 0)
            for name in FIXTURE_NAMES:
                self.assertTrue((Path(tmp) / name).exists(), name)


class TestRunSummaryOffline(unittest.TestCase):
    """Офлайн-прогон сводки на фикстурах — тот же путь, что живой `make`."""

    def run_summary(self, argv=None, out_dir=None) -> tuple:
        output = io.StringIO()
        if out_dir is None:
            out_dir = Path(tempfile.mkdtemp())
        with unittest.mock.patch.object(cm, "ARTIFACTS_DIR", Path(out_dir)):
            with fixture_transport(), contextlib.redirect_stdout(output):
                code = cm.main(argv or [])
        return code, output.getvalue()

    def test_summary_reports_fixture_sample(self):
        code, text = self.run_summary()
        self.assertEqual(code, 0)
        self.assertEqual(
            summary_value(text, "выборка:"),
            "15 месяцев (2013-10 … 2014-12), пропущено 0",
        )
        self.assertTrue(
            summary_value(text, "таргет rosstat_ipc_mes:").startswith(
                "4 ряда, 2013-01 … 2014-12"
            )
        )
        self.assertTrue(
            summary_value(text, "МП cbr_survey:").startswith("5 рядов")
        )
        self.assertEqual(summary_value(text, "purge-проверка (структурная):"), "OK")
        self.assertEqual(
            summary_value(text, "признаки (без МП):"),
            "y_lag1, y_lag2, usd_mm, key_rate, fao_mm, month",
        )
        self.assertEqual(
            summary_value(text, "признаки (с МП):"),
            ", ".join(cm.feature_names(with_mp=True)),
        )
        self.assertIn("walk-forward", text)
        self.assertIn("B1 last value", text)
        self.assertIn("B2 AR(1)", text)
        self.assertIn("B3 ElasticNetCV", text)
        # Выборки 15 месяцев не хватает на основное окно start=24.
        self.assertIn("не пишутся", text)
        # Short: sample и NaN-контроли печатаются, контроли заполнены.
        self.assertTrue(summary_value(text, "sample:").startswith("short "))
        self.assertEqual(summary_value(text, "NaN usd_mm / key_rate:"), "0 / 0")

    def test_sample_long_resolves_default_start_48(self):
        # CLI long: sample-дефолт старта — 48 (спека §8.2). Фикстуры короче
        # long-старта, поэтому walk-forward прогнозов не даёт, но разрешение
        # окна видно в сводке.
        with tempfile.TemporaryDirectory() as tmp:
            code, text = self.run_summary(["--sample", "long"], out_dir=tmp)
        self.assertEqual(code, 0)
        self.assertTrue(summary_value(text, "sample:").startswith("long (2002-05 … 2026-08)"))
        self.assertTrue(summary_value(text, "--start:").startswith("48 "))
        self.assertIn("не пишутся", text)

    def test_run_summary_long_specs_use_start_48(self):
        captured = {}

        def fake_evaluate(rows, models, specs, **kwargs):
            captured["specs"] = list(specs)
            captured["rows"] = list(rows)
            return []

        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(cm, "evaluate_windows", fake_evaluate), \
                    fixture_transport(), contextlib.redirect_stdout(output):
                code = cm.run_summary(
                    out_dir=Path(tmp), today=date(2026, 10, 18), sample="long"
                )
        self.assertEqual(code, 0)
        # Основное окно 48/0 + робастность 48/1; short-окно 24 не появляется.
        self.assertEqual(captured["specs"], [(48, 0), (48, 1)])
        # Фикстуры покрывают 2013-01…2014-12: long-кандидаты начинаются на
        # два месяца позже (y_lag1/y_lag2) и до 2026-08 не дотягиваются.
        self.assertEqual(captured["rows"][0].month, date(2013, 3, 1))
        self.assertEqual(captured["rows"][-1].month, date(2014, 12, 1))

    def test_start_and_embargo_are_accepted_and_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, text = self.run_summary(
                ["--start", "3", "--embargo-months", "1"], out_dir=tmp
            )
            self.assertEqual(code, 0)
            self.assertTrue(summary_value(text, "--start:").startswith("3 "))
            self.assertTrue(summary_value(text, "--embargo-months:").startswith("1 "))
            # Выборка флагами не меняется; меняется основное окно прогона.
            self.assertEqual(
                summary_value(text, "выборка:"),
                "15 месяцев (2013-10 … 2014-12), пропущено 0",
            )
            report_path = Path(tmp) / "cpi_monthly_report.md"
            predictions_path = Path(tmp) / "cpi_monthly_predictions.csv"
            self.assertTrue(report_path.exists())
            self.assertTrue(predictions_path.exists())
            self.assertIn(
                "start=3, embargo=1", report_path.read_text(encoding="utf-8")
            )
            with predictions_path.open(newline="", encoding="utf-8") as handle:
                records = list(csv.DictReader(handle))
            self.assertTrue(records)
            self.assertEqual(
                tuple(records[0]), ("month", "window", "model", "y_true", "y_pred")
            )
            self.assertIn(
                "start=3, embargo=1", {record["window"] for record in records}
            )
            self.assertIn(cm.MODEL_LAST, {record["model"] for record in records})
            # JSON живого nowcast пишется тем же прогоном (спека §5.1).
            forecast_path = Path(tmp) / cm.FORECAST_NAME
            self.assertTrue(forecast_path.exists())
            payload = json.loads(forecast_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["target_month"], "2015-01-01")
            for name in cm.CATBOOST_MODELS:
                self.assertEqual(set(payload[name]), {"forecast", "metadata"})

    def test_loaders_go_through_transport(self):
        with fixture_transport():
            self.assertEqual(set(cm.load_targets()), set(cm.TARGET_SERIES))
            self.assertEqual(set(cm.load_survey()), set(cm.MP_SOURCE_SERIES))
            self.assertTrue(cm.load_usd())
            self.assertTrue(cm.load_key_rate())
            self.assertTrue(cm.load_fao())


# --- Task 2: harness walk-forward ---------------------------------------------


def harness_rows(values: list, first: date = date(2010, 1, 1)) -> list:
    """Строки для тестов harness: месяцы подряд, метка и минимальные признаки.

    `features` — полный набор `feature_names(with_mp=True)` нулями, затем
    `y_lag1` = метка предыдущего месяца и `month`: этого достаточно B1/B2/B3
    (числовые признаки нулевые, вариация — у лага и метки).
    """
    rows = []
    for position, value in enumerate(values):
        month = cm._month_shift(first, position)
        features = {name: 0.0 for name in cm.feature_names(with_mp=True)}
        features["month"] = month.month
        features["y_lag1"] = values[position - 1] if position else 0.0
        rows.append(
            cm.Row(
                month=month,
                y={code: value for code in cm.TARGET_SERIES},
                features=features,
            )
        )
    return rows


def harness_values(count: int) -> list:
    """Метки с вариацией: `0.1 + 0.01·позиция` (не вырождены)."""
    return [0.1 + 0.01 * position for position in range(count)]


def catboost_rows(count: int = 30, weights: tuple = (0.4, 0.35, 0.25)) -> list:
    """Строки с известной смесью компонент и варьирующими признаками.

    Компоненты независимы и варьируют (у `harness_rows` все четыре метки
    равны — на такой синтетике NNLS не идентифицирован), таргет общего ИПЦ
    собирается ровно из `weights`: тест восстановления весов знает ответ
    заранее. Числовые признаки случайны с детерминированным генератором
    `SEED`; `month` — настоящий код месяца (категориальный признак CatBoost).
    """
    generator = random.Random(cm.SEED)
    rows = []
    for index in range(count):
        month = cm._month_shift(date(2020, 1, 1), index)
        features = {
            name: generator.uniform(-1.0, 1.0)
            for name in cm.feature_names(with_mp=True)
        }
        features["month"] = month.month
        components = [generator.uniform(-1.0, 2.0) for _ in cm.BOTTOMUP_COMPONENTS]
        y = dict(zip(cm.BOTTOMUP_COMPONENTS, components))
        y[cm.TARGET_ALL] = sum(weight * value for weight, value in zip(weights, components))
        rows.append(cm.Row(month=month, y=y, features=features))
    return rows


class TestWindowSpecs(unittest.TestCase):
    """Окна прогона: основное плюс робастность, дубликаты и embargo."""

    def test_default_specs(self):
        self.assertEqual(cm.window_specs(), [(24, 0), (48, 0), (24, 1)])
        self.assertEqual(cm.DEFAULT_START, 24)
        self.assertEqual(cm.DEFAULT_EMBARGO, 0)
        self.assertEqual(cm.ROBUST_START, 48)
        self.assertEqual(cm.ROBUST_EMBARGO, 1)

    def test_duplicates_are_dropped(self):
        # start=48 уже даёт робастный старт; embargo=1 добавляет лишь его.
        self.assertEqual(cm.window_specs(48, 0), [(48, 0), (48, 1)])
        self.assertEqual(cm.window_specs(24, 1), [(24, 1), (48, 1)])

    def test_embargo_not_less_than_start_is_not_added(self):
        # Робастное окно (start=1, embargo=1) обучать не на чем.
        self.assertEqual(cm.window_specs(1, 0), [(1, 0), (48, 0)])

    def test_custom_start_and_embargo_are_first(self):
        self.assertEqual(cm.window_specs(30, 2), [(30, 2), (48, 2), (30, 1)])

    def test_quick_window_specs_only_main(self):
        # Спека quick §2.1: quick — ровно основное окно, robustness нет.
        self.assertEqual(cm.window_specs(24, 0, quick=True), [(24, 0)])
        self.assertEqual(cm.window_specs(48, 0, True), [(48, 0)])
        self.assertEqual(cm.window_specs(24, 1, True), [(24, 1)])

    def test_full_window_specs_unchanged(self):
        # Без флага окна прежние: short 3 (24/0, 48/0, 24/1), long 2 (48/0, 48/1).
        self.assertEqual(cm.window_specs(24, 0), [(24, 0), (48, 0), (24, 1)])
        self.assertEqual(cm.window_specs(48, 0), [(48, 0), (48, 1)])
        self.assertEqual(
            cm.window_specs(24, 0, quick=False), [(24, 0), (48, 0), (24, 1)]
        )


class TestWalkForward(unittest.TestCase):
    """Расширяющееся окно: только прошлое, embargo, вырождение, счётчики."""

    def test_trains_only_on_past_and_first_fold_is_rows_start(self):
        rows = harness_rows(harness_values(60))
        calls = []

        def stub(train, row):
            calls.append((len(train), train[-1].month, row.month))
            return 0.0

        predictions = cm.walk_forward(rows, {"stub": stub}, start=24, embargo=0)
        self.assertEqual(len(predictions), 36)
        self.assertEqual(calls[0], (24, rows[23].month, rows[24].month))
        for position, (size, last_train, test_month) in enumerate(calls):
            self.assertEqual(size, 24 + position)
            self.assertLess(last_train, test_month)
        self.assertEqual(predictions[0].month, rows[24].month)
        self.assertEqual(predictions[0].model, "stub")
        self.assertEqual(predictions[0].y_true, rows[24].y[cm.TARGET_ALL])
        self.assertEqual(predictions[0].y_pred, 0.0)

    def test_embargo_drops_the_month_before_the_test(self):
        rows = harness_rows(harness_values(60))
        calls = []

        def stub(train, row):
            calls.append((len(train), train[-1].month, row.month))
            return 0.0

        cm.walk_forward(rows, {"stub": stub}, start=24, embargo=1)
        self.assertEqual(calls[0], (23, rows[22].month, rows[24].month))
        for position, (size, last_train, test_month) in enumerate(calls):
            self.assertEqual(size, 23 + position)
            self.assertLess(last_train, test_month)

    def test_degenerate_window_is_counted_and_run_continues(self):
        rows = harness_rows(harness_values(30))

        def fragile(train, row):
            if len(train) < 26:
                raise cm.DegenerateWindowError("короткое окно")
            return 1.0

        skipped = {}
        predictions = cm.walk_forward(
            rows, {"fragile": fragile}, start=24, embargo=0, skipped=skipped
        )
        # Фолды с train 24, 25 вырождены; 26–29 — прогнозы.
        self.assertEqual(skipped, {"fragile": 2})
        self.assertEqual(len(predictions), 4)

    def test_other_exceptions_are_not_swallowed(self):
        rows = harness_rows(harness_values(30))

        def broken(train, row):
            raise ValueError("это баг, а не вырождение")

        with self.assertRaises(ValueError):
            cm.walk_forward(rows, {"broken": broken}, start=24, embargo=0)

    def test_fold_notes_are_aggregated(self):
        rows = harness_rows(harness_values(30))

        def noted(train, row):
            noted.fold_notes = {cm.NON_CONVERGED: int(len(train) % 2 == 0)}
            return 0.0

        notes = {}
        cm.walk_forward(rows, {"noted": noted}, start=24, embargo=0, notes=notes)
        expected = sum(1 for size in range(24, 30) if size % 2 == 0)
        self.assertEqual(notes, {"noted": {cm.NON_CONVERGED: expected}})

    def test_invalid_windows_are_rejected(self):
        rows = harness_rows(harness_values(30))
        model = {"stub": lambda train, row: 0.0}
        with self.assertRaises(ValueError):
            cm.walk_forward(rows, model, start=0, embargo=0)
        with self.assertRaises(ValueError):
            cm.walk_forward(rows, model, start=24, embargo=-1)
        with self.assertRaises(ValueError):
            cm.walk_forward(rows, model, start=24, embargo=24)


class TestEvaluateWindows(unittest.TestCase):
    """Счётчики нулей, порядок моделей и роль основного окна."""

    def test_runs_follow_specs_and_keep_model_order(self):
        rows = harness_rows(harness_values(60))
        models = {"first": lambda train, row: 0.0, "second": lambda train, row: 1.0}
        runs = cm.evaluate_windows(rows, models, specs=[(24, 0), (48, 1)])
        self.assertEqual([(run.start, run.embargo) for run in runs], [(24, 0), (48, 1)])
        self.assertEqual([run.main for run in runs], [True, False])
        self.assertEqual([run.folds for run in runs], [36, 12])
        self.assertEqual(list(runs[0].skipped), ["first", "second"])
        self.assertEqual(runs[0].skipped, {"first": 0, "second": 0})
        self.assertEqual(runs[0].notes, {"first": {}, "second": {}})

    def test_default_specs_match_window_specs(self):
        rows = harness_rows(harness_values(60))
        runs = cm.evaluate_windows(rows, {"stub": lambda train, row: 0.0})
        self.assertEqual(
            [(run.start, run.embargo) for run in runs], cm.window_specs()
        )


# --- Task 2: бенчмарки B1–B3, метрики, отчёт ----------------------------------


def ar1_values(count: int, intercept: float = 1.0, phi: float = 0.5) -> list:
    """Ряд `y_M = c + φ·y_{M−1}`: OLS обязан восстановить φ точно."""
    values = [0.0]
    for _ in range(count - 1):
        values.append(intercept + phi * values[-1])
    return values


class TestBenchmarkConstants(unittest.TestCase):
    """Имена, порядок и минимальные окна бенчмарков (спека §3.4)."""

    def test_model_names(self):
        self.assertEqual(cm.MODEL_LAST, "b1_last_value")
        self.assertEqual(cm.MODEL_AR1, "b2_ar1_seasonal")
        self.assertEqual(cm.MODEL_ENET_BASE, "b3_elastic_net")
        self.assertEqual(cm.MODEL_ENET_MP, "b3_elastic_net_mp")
        self.assertEqual(cm.MODEL_CATBOOST_DIRECT, "catboost_direct")
        self.assertEqual(cm.MODEL_CATBOOST_BOTTOMUP, "catboost_bottomup")
        self.assertEqual(cm.MODEL_CATBOOST_DIRECT_MP, "catboost_direct_mp")
        self.assertEqual(cm.MODEL_CATBOOST_BOTTOMUP_MP, "catboost_bottomup_mp")

    def test_registry_order_defines_report_rows(self):
        # Порядок §3.3–3.4: бенчмарки B1, B2, B3-base, B3-mp, затем четыре
        # CatBoost-варианта (direct/bottom-up × без МП/с МП).
        self.assertEqual(
            list(cm.MODELS),
            [
                cm.MODEL_LAST,
                cm.MODEL_AR1,
                cm.MODEL_ENET_BASE,
                cm.MODEL_ENET_MP,
                cm.MODEL_CATBOOST_DIRECT,
                cm.MODEL_CATBOOST_BOTTOMUP,
                cm.MODEL_CATBOOST_DIRECT_MP,
                cm.MODEL_CATBOOST_BOTTOMUP_MP,
            ],
        )
        self.assertEqual(cm.MODELS[cm.MODEL_LAST], cm.predict_last)
        self.assertEqual(cm.MODELS[cm.MODEL_AR1], cm.fit_ar1)
        self.assertEqual(cm.MODELS[cm.MODEL_ENET_BASE], cm.fit_enet_base)
        self.assertEqual(cm.MODELS[cm.MODEL_ENET_MP], cm.fit_enet_mp)
        self.assertEqual(cm.MODELS[cm.MODEL_CATBOOST_DIRECT], cm.fit_catboost_direct)
        self.assertEqual(cm.MODELS[cm.MODEL_CATBOOST_BOTTOMUP], cm.fit_catboost_bottomup)
        self.assertEqual(
            cm.MODELS[cm.MODEL_CATBOOST_DIRECT_MP], cm.fit_catboost_direct_mp
        )
        self.assertEqual(
            cm.MODELS[cm.MODEL_CATBOOST_BOTTOMUP_MP], cm.fit_catboost_bottomup_mp
        )

    def test_windows_and_hac_config(self):
        self.assertEqual(cm.AR1_MIN_TRAIN, 14)
        self.assertEqual(cm.ENET_MIN_TRAIN, 12)
        self.assertEqual(cm.HAC_MAXLAGS, 3)
        self.assertEqual(tuple(cm.AR1_DUMMY_MONTHS), tuple(range(2, 13)))
        self.assertEqual(len(cm.AR1_DUMMY_MONTHS), 11)
        self.assertEqual(cm.LAST_FOLDS, 12)


class TestPredictLast(unittest.TestCase):
    """B1: прогноз — `y_lag1`, а не последняя метка обучения."""

    def test_returns_lag1_from_test_row(self):
        rows = harness_rows(harness_values(30))
        rows[10].features["y_lag1"] = 7.5
        # Обучающее окно намеренно «не то»: B1 не читает его вовсе.
        self.assertEqual(cm.predict_last(rows[:3], rows[10]), 7.5)


class TestFitAr1(unittest.TestCase):
    """B2: OLS `y_M` на `y_{M−1}` + 11 дамми, HAC-ковариация, минимум 14 строк."""

    def test_recovers_phi(self):
        values = ar1_values(60)
        rows = harness_rows(values)
        # Первая строка ряда — начальное условие, а не следствие рекурсии:
        # окно обучения начинается со второй строки, иначе OLS видит выброс.
        prediction = cm.fit_ar1(rows[1:51], rows[51])
        result = cm.fit_ar1.last_result
        self.assertAlmostEqual(float(result.params[1]), 0.5, delta=1e-6)
        self.assertAlmostEqual(prediction, 1.0 + 0.5 * values[50], delta=1e-6)
        self.assertEqual(result.cov_type, "HAC")
        self.assertEqual(result.cov_kwds["maxlags"], 3)
        self.assertTrue(result.cov_kwds["use_correction"])

    def test_thirteen_rows_are_degenerate(self):
        rows = harness_rows(ar1_values(20))
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_ar1(rows[:13], rows[13])

    def test_fourteen_rows_are_enough(self):
        rows = harness_rows(ar1_values(20))
        value = cm.fit_ar1(rows[1:15], rows[15])
        self.assertIsInstance(value, float)

    def test_constant_lag_is_degenerate(self):
        rows = harness_rows([0.5] * 20)
        # Ряд меток постоянен, поэтому y_lag1 в окне без вариации (первая
        # строка с нулём в окно не входит).
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_ar1(rows[1:15], rows[15])


class TestFitEnet(unittest.TestCase):
    """B3: два пространства, детерминизм, train-статистика, недосход."""

    def test_feature_sets(self):
        self.assertEqual(
            cm.enet_features(False),
            ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm"],
        )
        self.assertEqual(
            cm.enet_features(True),
            cm.feature_names(with_mp=False)[:-1]
            + list(cm.MP_SERIES),
        )
        self.assertNotIn("month", cm.enet_features(False))
        self.assertNotIn("month", cm.enet_features(True))

    def test_twelve_rows_minimum(self):
        rows = harness_rows(harness_values(20))
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_enet_base(rows[:11], rows[11])
        self.assertIsInstance(cm.fit_enet_base(rows[:12], rows[12]), float)

    def test_deterministic(self):
        rows = harness_rows(harness_values(40))
        first = cm.fit_enet_base(rows[:30], rows[30])
        second = cm.fit_enet_base(rows[:30], rows[30])
        self.assertEqual(first, second)
        first_mp = cm.fit_enet_mp(rows[:30], rows[30])
        second_mp = cm.fit_enet_mp(rows[:30], rows[30])
        self.assertEqual(first_mp, second_mp)

    def test_base_ignores_mp_features(self):
        rows = harness_rows(harness_values(40))
        altered = []
        for position, row in enumerate(rows):
            features = dict(row.features)
            for name in cm.MP_SERIES:
                features[name] = 100.0 + 10.0 * position
            altered.append(
                cm.Row(month=row.month, y=dict(row.y), features=features)
            )
        self.assertEqual(
            cm.fit_enet_base(rows[:30], rows[30]),
            cm.fit_enet_base(altered[:30], altered[30]),
        )
        # Пространства различаются числом колонок: база — 5, с МП — 12.
        self.assertEqual(
            cm.fit_enet_base.last_fit.imputed.shape[1],
            len(cm.enet_features(False)),
        )
        cm.fit_enet_mp(rows[:30], rows[30])
        self.assertEqual(
            cm.fit_enet_mp.last_fit.imputed.shape[1],
            len(cm.enet_features(True)),
        )

    def test_nan_is_imputed_with_train_median_only(self):
        rows = harness_rows(harness_values(30))
        for position, row in enumerate(rows):
            row.features["usd_mm"] = float(position)
        rows[5].features["usd_mm"] = float("nan")
        rows[9].features["usd_mm"] = float("nan")
        train = rows[:20]
        # Тестовая строка — экстремум: её статистика не должна попадать в
        # импутацию и стандартизацию.
        rows[20].features["usd_mm"] = 1_000_000.0
        cm.fit_enet_base(train, rows[20])
        fit = cm.fit_enet_base.last_fit
        index = cm.enet_features(False).index("usd_mm")
        known = [position for position in range(20) if position not in (5, 9)]
        median = statistics.median(float(position) for position in known)
        self.assertAlmostEqual(float(fit.medians[index]), median)
        imputed = [median if position in (5, 9) else float(position) for position in range(20)]
        scaler = fit.model.named_steps["scaler"]
        self.assertAlmostEqual(float(scaler.mean_[index]), statistics.mean(imputed))

    def test_convergence_warning_is_fold_note_not_degeneracy(self):
        class WarnEnet(SkElasticNetCV):
            def fit(self, matrix, targets):
                warnings.warn("objective did not converge", ConvergenceWarning)
                self.coef_ = [0.0] * matrix.shape[1]
                self.intercept_ = 0.0
                return self

            def predict(self, matrix):
                return [0.0 for _ in range(len(matrix))]

        rows = harness_rows(harness_values(21))
        with unittest.mock.patch.object(cm, "ElasticNetCV", WarnEnet):
            value = cm.fit_enet_base(rows[:20], rows[20])
        self.assertEqual(value, 0.0)
        self.assertEqual(
            cm.fit_enet_base.fold_notes, {cm.NON_CONVERGED: 1}
        )

    def test_degenerate_b3_folds_are_counted(self):
        rows = harness_rows(harness_values(20))
        skipped = {}
        predictions = cm.walk_forward(
            rows,
            {cm.MODEL_ENET_BASE: cm.fit_enet_base},
            start=11,
            embargo=0,
            skipped=skipped,
        )
        # Фолд с train=11 вырожден; 12–19 — прогнозы.
        self.assertEqual(skipped, {cm.MODEL_ENET_BASE: 1})
        self.assertEqual(len(predictions), 8)


# --- Task 3: CatBoost direct/bottom-up и веса NNLS ----------------------------


class TestCatBoostConfiguration(unittest.TestCase):
    """Конфигурация CatBoost (§3.1): loss/seed/monotone/cat и дефолты библиотеки."""

    def test_params_are_exactly_the_spec_for_both_sets(self):
        # Дословно §3.1 плюс флаг побочного эффекта: без `allow_writing_files=False`
        # CatBoost пишет каталог catboost_info/ в рабочий каталог прогона.
        self.assertEqual(cm.catboost_params(False), {
            "loss_function": "RMSE",
            "random_seed": cm.SEED,
            "verbose": False,
            "monotone_constraints": [0, 0, 1, 0, 0, 0],
            "cat_features": [5],
            "allow_writing_files": False,
        })
        self.assertEqual(cm.catboost_params(True), {
            "loss_function": "RMSE",
            "random_seed": cm.SEED,
            "verbose": False,
            "monotone_constraints": [0, 0, 1, 0, 0, 1, 1, 0, 0, 0, 0, 0, 0],
            "cat_features": [12],
            "allow_writing_files": False,
        })

    def test_monotone_constraints_by_name(self):
        # Знаки §3.1 заданы ИМЕНАМИ признаков, а CatBoost ждёт список по позициям
        # `feature_names(with_mp)`: сдвиг порядка обязан быть виден тестом.
        self.assertEqual(cm.CATBOOST_MONOTONE, {
            "usd_mm": 1,
            "price_expectations_sa": 1,
            "price_expectations_sa_chg": 1,
        })
        self.assertEqual(cm.CATBOOST_CAT_FEATURES, ("month",))
        cases = (
            (False, {"usd_mm"}),
            (True, {"usd_mm", "price_expectations_sa", "price_expectations_sa_chg"}),
        )
        for with_mp, positive in cases:
            names = cm.feature_names(with_mp)
            constraints = cm.monotone_constraints(with_mp)
            self.assertEqual(len(constraints), len(names))
            self.assertEqual(
                {name for name, value in zip(names, constraints) if value},
                positive,
            )
            # Категориальный месяц — 0: CatBoost допускает ограничения только
            # на float-признаках.
            self.assertEqual(constraints[names.index("month")], 0)
            # Все числовые признаки, кроме перечисленных, — 0.
            self.assertTrue(
                all(
                    value == 0
                    for name, value in zip(names, constraints)
                    if name not in positive
                )
            )

    def test_model_carries_the_spec_params(self):
        for with_mp in (False, True):
            model = cm.catboost_model(with_mp)
            self.assertIsInstance(model, cm.CatBoostRegressor)
            params = model.get_params()
            self.assertEqual(set(params), set(cm.catboost_params(with_mp)))
            self.assertEqual(params["loss_function"], "RMSE")
            self.assertEqual(params["random_seed"], cm.SEED)
            self.assertEqual(params["verbose"], False)
            self.assertEqual(
                params["monotone_constraints"], cm.monotone_constraints(with_mp)
            )
            self.assertEqual(
                params["cat_features"], [len(cm.feature_names(with_mp)) - 1]
            )
            self.assertFalse(params["allow_writing_files"])

    def test_metadata_is_json_ready_for_the_run_artifact(self):
        # Task 4 кладёт эти метаданные в отчёт/JSON прогона: словарь обязан быть
        # сериализуемым как есть (имена признаков, а не индексы).
        for with_mp in (False, True):
            metadata = cm.catboost_metadata(with_mp)
            self.assertEqual(metadata["loss_function"], "RMSE")
            self.assertEqual(metadata["random_seed"], cm.SEED)
            self.assertEqual(metadata["cat_features"], ["month"])
            # Имена — для JSON, индексы — представление в numpy-матрице
            # (CatBoost на numpy принимает только позиции).
            self.assertEqual(
                metadata["cat_feature_indices"],
                cm.catboost_cat_indices(with_mp),
            )
            self.assertEqual(
                metadata["cat_feature_indices"],
                [len(cm.feature_names(with_mp)) - 1],
            )
            self.assertEqual(
                metadata["num_features"],
                [name for name in cm.feature_names(with_mp) if name != "month"],
            )
            self.assertEqual(
                list(metadata["monotone_constraints"]), cm.feature_names(with_mp)
            )
            self.assertEqual(
                {name for name, value in metadata["monotone_constraints"].items() if value},
                {
                    name
                    for name, value in zip(
                        cm.feature_names(with_mp), cm.monotone_constraints(with_mp)
                    )
                    if value
                },
            )
            self.assertFalse(metadata["allow_writing_files"])
            self.assertEqual(metadata["defaults"], cm.CATBOOST_DEFAULTS)
            self.assertEqual(json.loads(json.dumps(metadata)), metadata)

    def test_library_defaults_match_the_metadata(self):
        # Дефолты библиотеки — часть конфигурации (§3.1): метаданные печатают их,
        # тест сверяет с `get_all_params()` ОБУЧЕННОЙ модели — расхождение версии
        # CatBoost видно сразу.
        rows = catboost_rows(30)
        cm.fit_catboost_direct(rows[:25], rows[25])
        all_params = cm.fit_catboost_direct.last_fit.model.get_all_params()
        for name, value in cm.CATBOOST_DEFAULTS.items():
            self.assertEqual(all_params[name], value, f"дефолт {name}")


class TestCatBoostMatrix(unittest.TestCase):
    """Матрица CatBoost: numpy object, числовые float + int-код месяца (§3.1)."""

    def test_order_and_types_for_both_sets(self):
        rows = catboost_rows(5)
        for with_mp in (False, True):
            names = cm.feature_names(with_mp)
            numeric = [name for name in names if name != "month"]
            self.assertEqual(numeric, cm.catboost_numeric_features(with_mp))
            matrix = cm.catboost_matrix(rows, with_mp)
            self.assertIsInstance(matrix, np.ndarray)
            self.assertEqual(matrix.shape, (5, len(names)))
            self.assertEqual(matrix.dtype, np.dtype(object))
            for position, row in enumerate(rows):
                for column, name in enumerate(numeric):
                    value = matrix[position][column]
                    self.assertIsInstance(value, float)
                    self.assertAlmostEqual(
                        float(value), float(row.features[name]), places=12
                    )
                month = matrix[position][len(numeric)]
                self.assertIsInstance(month, (int, np.integer))
                self.assertNotIsInstance(month, np.floating)
                self.assertEqual(int(month), row.features["month"])

    def test_nan_of_a_numeric_column_is_passed_through(self):
        # NaN числового признака идёт в CatBoost как есть (nan_mode по
        # умолчанию), в отличие от B3, которая импутирует медианой train.
        rows = catboost_rows(4)
        rows[1].features["usd_mm"] = float("nan")
        for with_mp in (False, True):
            matrix = cm.catboost_matrix(rows, with_mp)
            column = cm.catboost_numeric_features(with_mp).index("usd_mm")
            self.assertTrue(math.isnan(float(matrix[1][column])))
            self.assertFalse(math.isnan(float(matrix[0][column])))

    def test_matrix_of_an_empty_window_has_no_rows(self):
        for with_mp in (False, True):
            matrix = cm.catboost_matrix([], with_mp)
            self.assertEqual(matrix.shape, (0, len(cm.feature_names(with_mp))))


class TestBottomUpWeights(unittest.TestCase):
    """NNLS-веса bottom-up (§3.2): синтетика, неотрицательность, обучающее окно."""

    def test_recovers_the_known_mixture(self):
        rows = catboost_rows(40)
        weights = cm.nnls_weights(rows[:30])
        self.assertIsInstance(weights, tuple)
        self.assertEqual(len(weights), 3)
        for value, expected in zip(weights, (0.4, 0.35, 0.25)):
            self.assertAlmostEqual(value, expected, delta=1e-6)

    def test_weights_are_non_negative_and_normalized(self):
        # Смеси с отрицательным весом неотрицательная регрессия соответствовать
        # не может: ответ обязан остаться в симплексе (w ≥ 0, Σw = 1).
        rows = catboost_rows(40, weights=(0.9, 0.6, -0.5))
        weights = cm.nnls_weights(rows[:30])
        self.assertGreaterEqual(min(weights), 0.0)
        self.assertAlmostEqual(sum(weights), 1.0, places=12)

    def test_weights_use_only_the_training_window(self):
        # Утечка через веса: мутация БУДУЩИХ месяцев не имеет права менять веса
        # фолда (они считаются только по обучающему окну — §3.2).
        rows = catboost_rows(40)
        before = cm.nnls_weights(rows[:30])
        for row in rows[30:]:
            for code in cm.BOTTOMUP_COMPONENTS:
                row.y[code] = row.y[code] * 10.0 + 5.0
            row.y[cm.TARGET_ALL] = -7.0
        self.assertEqual(cm.nnls_weights(rows[:30]), before)
        # ...и наоборот: подмена ОБУЧАЮЩЕЙ строки веса меняет (регрессия читается).
        rows[29].y[cm.BOTTOMUP_COMPONENTS[0]] += 3.0
        self.assertNotEqual(cm.nnls_weights(rows[:30]), before)

    def test_empty_and_zero_windows_are_degenerate(self):
        rows = catboost_rows(10)
        with self.assertRaises(cm.DegenerateWindowError):
            cm.nnls_weights([])
        zero = [
            cm.Row(
                month=row.month,
                y={code: 0.0 for code in cm.TARGET_SERIES},
                features=dict(row.features),
            )
            for row in rows[:5]
        ]
        with self.assertRaises(cm.DegenerateWindowError):
            cm.nnls_weights(zero)


class TestCatBoostModels(unittest.TestCase):
    """direct и bottom-up (§3.1–3.2): детерминизм, Σw·ŷ, вырожденные окна."""

    def test_direct_prediction_is_deterministic(self):
        rows = catboost_rows(30)
        first = cm.fit_catboost_direct(rows[:25], rows[25])
        second = cm.fit_catboost_direct(rows[:25], rows[25])
        self.assertEqual(first, second)  # побитово: SEED и дефолты, без внешнего рандома
        self.assertTrue(math.isfinite(first))
        self.assertEqual(
            cm.fit_catboost_direct.last_fit.matrix.shape[1],
            len(cm.feature_names(with_mp=False)),
        )
        self.assertFalse(cm.fit_catboost_direct.last_fit.with_mp)

    def test_direct_mp_prediction_is_deterministic(self):
        rows = catboost_rows(30)
        first = cm.fit_catboost_direct_mp(rows[:25], rows[25])
        second = cm.fit_catboost_direct_mp(rows[:25], rows[25])
        self.assertEqual(first, second)
        self.assertTrue(math.isfinite(first))
        self.assertTrue(cm.fit_catboost_direct_mp.last_fit.with_mp)
        self.assertEqual(
            cm.fit_catboost_direct_mp.last_fit.matrix.shape[1],
            len(cm.feature_names(with_mp=True)),
        )

    def test_bottomup_is_the_weighted_sum_of_component_forecasts(self):
        # Ручная сверка §3.2 на заглушечных компонентах: агрегат — Σ w_j·ŷ_j с
        # весами NNLS обучающего окна, без собственного «третьего» прогноза.
        rows = catboost_rows(30)
        train, test = rows[:25], rows[25]
        stubs = {"cpi_food": 1.5, "cpi_nonfood": -0.5, "cpi_services": 0.25}

        def stub(train_rows, row, code, with_mp=False):
            self.assertFalse(with_mp)
            return stubs[code]

        weights = cm.nnls_weights(train)
        with unittest.mock.patch.object(cm, "fit_catboost_component", stub):
            value = cm.fit_catboost_bottomup(train, test)
        expected = sum(
            weight * stubs[code] for weight, code in zip(weights, cm.BOTTOMUP_COMPONENTS)
        )
        self.assertAlmostEqual(value, expected, places=12)
        # Веса последнего фолда остаются доступны отчёту (спека §3.2).
        self.assertEqual(cm.fit_catboost_bottomup.last_weights, weights)
        self.assertEqual(cm.fit_catboost_bottomup.last_components, (1.5, -0.5, 0.25))

    def test_bottomup_mp_uses_mp_features_and_keeps_diagnostics(self):
        rows = catboost_rows(30)
        train, test = rows[:25], rows[25]
        seen = []

        def stub(train_rows, row, code, with_mp=False):
            seen.append(with_mp)
            return {"cpi_food": 1.0, "cpi_nonfood": 2.0, "cpi_services": 3.0}[code]

        weights = cm.nnls_weights(train)
        with unittest.mock.patch.object(cm, "fit_catboost_component", stub):
            value = cm.fit_catboost_bottomup_mp(train, test)
        self.assertEqual(seen, [True, True, True])
        self.assertAlmostEqual(
            value, sum(w * v for w, v in zip(weights, (1.0, 2.0, 3.0))), places=12
        )
        self.assertEqual(cm.fit_catboost_bottomup_mp.last_weights, weights)
        self.assertEqual(
            cm.fit_catboost_bottomup_mp.last_components, (1.0, 2.0, 3.0)
        )

    def test_bottomup_prediction_is_deterministic(self):
        rows = catboost_rows(20)
        first = cm.fit_catboost_bottomup(rows[:15], rows[15])
        second = cm.fit_catboost_bottomup(rows[:15], rows[15])
        self.assertEqual(first, second)
        self.assertEqual(
            cm.fit_catboost_bottomup.last_weights, cm.nnls_weights(rows[:15])
        )

    def test_degenerate_component_skips_the_whole_fold(self):
        # §3.2: вырожденность ЛЮБОЙ компоненты — DegenerateWindowError всего
        # фолда (walk-forward пропускает его со счётчиком, а не считает
        # частичный агрегат).
        rows = catboost_rows(30)

        def degenerate(train_rows, row, code, with_mp=False):
            if code == "cpi_nonfood":
                raise cm.DegenerateWindowError("компонента вырождена")
            return 0.1

        with unittest.mock.patch.object(cm, "fit_catboost_component", degenerate):
            with self.assertRaises(cm.DegenerateWindowError):
                cm.fit_catboost_bottomup(rows[:25], rows[25])

    def test_degenerate_weights_skip_the_whole_fold(self):
        rows = catboost_rows(30)
        with unittest.mock.patch.object(
            cm, "nnls_weights", side_effect=cm.DegenerateWindowError("веса не определены")
        ):
            with self.assertRaises(cm.DegenerateWindowError):
                cm.fit_catboost_bottomup(rows[:25], rows[25])

    def test_degenerate_bottomup_fold_produces_no_prediction(self):
        # Вырожденный фолд — не частичный агрегат: walk-forward не создаёт
        # Prediction модели вовсе и увеличивает счётчик пропусков; диагностика
        # весов последнего успешного фолда не перезаписывается.
        rows = catboost_rows(20)

        def degenerate(train_rows, row, code, with_mp=False):
            if code == "cpi_nonfood":
                raise cm.DegenerateWindowError("компонента вырождена")
            return 0.1

        cm.fit_catboost_bottomup.last_weights = ()
        skipped = {}
        with unittest.mock.patch.object(cm, "fit_catboost_component", degenerate):
            predictions = cm.walk_forward(
                rows,
                {cm.MODEL_CATBOOST_BOTTOMUP: cm.fit_catboost_bottomup},
                start=15,
                embargo=0,
                skipped=skipped,
            )
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, {cm.MODEL_CATBOOST_BOTTOMUP: 5})
        self.assertEqual(cm.fit_catboost_bottomup.last_weights, ())

    def test_degenerate_direct_fold_produces_no_prediction(self):
        rows = catboost_rows(20)
        for row in rows:
            row.y[cm.TARGET_ALL] = 1.0
        skipped = {}
        predictions = cm.walk_forward(
            rows,
            {cm.MODEL_CATBOOST_DIRECT: cm.fit_catboost_direct},
            start=15,
            embargo=0,
            skipped=skipped,
        )
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, {cm.MODEL_CATBOOST_DIRECT: 5})

    def test_constant_target_window_is_degenerate(self):
        # Окно с неизменным таргетом CatBoost не оценивает («All train targets
        # are equal»): это вырожденное окно, а не порча данных.
        rows = catboost_rows(30)
        for row in rows[:25]:
            row.y[cm.TARGET_ALL] = 1.0
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_catboost_direct(rows[:25], rows[25])

    def test_constant_component_target_is_degenerate(self):
        rows = catboost_rows(30)
        for row in rows[:25]:
            row.y["cpi_food"] = 1.0
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_catboost_component(rows[:25], rows[25], "cpi_food", False)

    def test_empty_window_is_degenerate(self):
        rows = catboost_rows(5)
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_catboost_direct([], rows[0])
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_catboost_component([], rows[0], "cpi_food", True)


class TestEvaluateWindowsWeights(unittest.TestCase):
    """Веса bottom-up снимаются с последнего успешного фолда КАЖДОГО окна."""

    def test_weights_are_captured_per_window_and_reset_between_windows(self):
        rows = harness_rows(harness_values(30))

        def fragile(train, row):
            if len(train) >= 20:
                raise cm.DegenerateWindowError("длинное окно вырождено")
            fragile.last_weights = (float(len(train)), 0.0, 0.0)
            return 0.0

        fragile.last_weights = ()
        runs = cm.evaluate_windows(
            rows, {"fragile": fragile}, specs=[(10, 0), (20, 0)]
        )
        # Первое окно: последний успешный фолд — train длины 19.
        self.assertEqual(runs[0].weights, {"fragile": (19.0, 0.0, 0.0)})
        # Второе окно целиком вырождено: веса НЕ перетекают из первого окна.
        self.assertEqual(runs[1].weights, {})
        self.assertEqual(runs[1].skipped, {"fragile": 10})


class TestMetrics(unittest.TestCase):
    """RMSE/MAE в п.п. и отрезок последних 12 фолдов."""

    def test_errors_rmse_mae_on_manual_numbers(self):
        predictions = [
            cm.Prediction(month=date(2020, 1, 1), model="m", y_true=1.0, y_pred=2.0),
            cm.Prediction(month=date(2020, 2, 1), model="m", y_true=3.0, y_pred=1.0),
        ]
        self.assertEqual(cm.errors(predictions), [1.0, -2.0])
        self.assertAlmostEqual(cm.rmse_pp(predictions), math.sqrt(5.0 / 2.0))
        self.assertAlmostEqual(cm.mae_pp(predictions), 1.5)

    def test_empty_group_is_an_error(self):
        with self.assertRaises(ValueError):
            cm.rmse_pp([])

    def test_last_fold_months(self):
        months = month_range(date(2019, 1, 1), date(2020, 12, 1))
        predictions = [
            cm.Prediction(month=month, model="m", y_true=0.0, y_pred=0.0)
            for month in months
        ]
        self.assertEqual(cm.last_fold_months(predictions), set(months[-12:]))
        self.assertEqual(cm.last_fold_months(predictions, 5), set(months[-5:]))
        self.assertEqual(cm.last_fold_months([]), set())


class TestReportAndArtifacts(unittest.TestCase):
    """Скелет отчёта и CSV прогнозов: разделы, строки, колонки, LF."""

    def make_run(self) -> tuple:
        rows = harness_rows(harness_values(6))
        predictions = [
            cm.Prediction(month=rows[3].month, model=cm.MODEL_LAST, y_true=0.2, y_pred=0.3),
            cm.Prediction(month=rows[4].month, model=cm.MODEL_LAST, y_true=0.3, y_pred=0.1),
            cm.Prediction(
                month=rows[4].month, model=cm.MODEL_ENET_BASE, y_true=0.3, y_pred=0.25
            ),
        ]
        skipped = {name: 0 for name in cm.MODELS}
        skipped[cm.MODEL_ENET_BASE] = 1
        skipped[cm.MODEL_ENET_MP] = 3
        notes = {name: {} for name in cm.MODELS}
        notes[cm.MODEL_ENET_BASE] = {cm.NON_CONVERGED: 1}
        notes[cm.MODEL_ENET_MP] = {cm.NON_CONVERGED: 0}
        run = cm.WindowRun(
            start=24,
            embargo=0,
            main=True,
            folds=3,
            predictions=predictions,
            skipped=skipped,
            notes=notes,
            weights={cm.MODEL_CATBOOST_BOTTOMUP: (0.4, 0.35, 0.25)},
        )
        return rows, run

    def test_report_sections(self):
        rows, run = self.make_run()
        report = cm.render_report(
            rows,
            [(rows[1].month, "нет точки")],
            [run],
            today=date(2026, 10, 18),
        )
        self.assertIn("# CPI monthly (ML-трек v2)", report)
        self.assertIn("Дата прогона: 2026-10-18", report)
        self.assertIn("6 (2010-01 … 2010-06)", report)
        self.assertIn("пропущено (признак не собран) | 1 (2010-02)", report)
        self.assertIn("purge-проверка (структурная) | OK", report)
        self.assertIn("| B1 last value | start=24 (основное) | 2 |", report)
        self.assertIn("B3 ElasticNetCV (без МП)", report)
        self.assertIn("B3 ElasticNetCV (с МП)", report)
        # Четыре CatBoost-варианта — строки таблицы «модель × окно» (§3.3).
        self.assertIn("CatBoost direct (без МП)", report)
        self.assertIn("CatBoost bottom-up (без МП)", report)
        self.assertIn("CatBoost direct (с МП)", report)
        self.assertIn("CatBoost bottom-up (с МП)", report)
        self.assertIn("1 из 1", report)
        self.assertIn("0 фолдов", report)
        self.assertIn("## Веса NNLS (bottom-up)", report)
        self.assertIn(
            "| start=24 (основное) | CatBoost bottom-up (без МП) | "
            "0.400 | 0.350 | 0.250 | 1.000 |",
            report,
        )
        self.assertIn("## Конфигурация CatBoost", report)
        self.assertIn("`allow_writing_files`", report)
        self.assertIn("## Последние 12 фолдов (основное окно start=24)", report)
        self.assertIn("## DM-HLN", report)
        self.assertIn("## Что не проверено", report)

    def test_write_artifacts_predictions_csv(self):
        rows, run = self.make_run()
        report = cm.render_report(rows, [], [run], today=date(2026, 10, 18))
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            report_path, predictions_path = cm.write_artifacts(directory, report, [run])
            self.assertEqual(report_path.name, "cpi_monthly_report.md")
            self.assertEqual(predictions_path.name, "cpi_monthly_predictions.csv")
            self.assertEqual(report_path.read_text(encoding="utf-8"), report)
            raw = predictions_path.read_bytes()
            self.assertNotIn(b"\r", raw)
            with predictions_path.open(newline="", encoding="utf-8") as handle:
                records = list(csv.DictReader(handle))
            self.assertEqual(
                tuple(records[0]), ("month", "window", "model", "y_true", "y_pred")
            )
            self.assertEqual(
                [record["model"] for record in records],
                [cm.MODEL_LAST, cm.MODEL_LAST, cm.MODEL_ENET_BASE],
            )
            self.assertEqual(records[0]["window"], "start=24")
            self.assertEqual(records[0]["month"], rows[3].month.isoformat())
            self.assertAlmostEqual(float(records[0]["y_true"]), 0.2)
            self.assertAlmostEqual(float(records[0]["y_pred"]), 0.3)

    def test_embargo_window_key(self):
        self.assertEqual(cm.window_key(24, 0), "start=24")
        self.assertEqual(cm.window_key(24, 1), "start=24, embargo=1")


# --- Task 4: DM-HLN, сплит статьи, «2022 исключён» и живой nowcast ------------


def stub_models(values: dict = None) -> dict:
    """Модели-заглушки: каждая возвращает фиксированное число.

    Живой nowcast в проде обучает четыре CatBoost-варианта и четыре бенчмарка на
    полной выборке; офлайн-тестам DM/JSON/пометок достаточно детерминированных
    значений: проверяется обвязка (признаки, note, JSON), а не качество модели.
    """
    values = values or {}
    return {
        name: (lambda train, row, value=values.get(name, 0.5): value)
        for name in cm.MODELS
    }


def prediction(model: str, month: date, error: float) -> cm.Prediction:
    """`Prediction` с нулевым фактом: `y_pred` численно равен ошибке фолда."""
    return cm.Prediction(month=month, model=model, y_true=0.0, y_pred=error)


class TestPairTests(unittest.TestCase):
    """DM-HLN и блочный бутстрап: пары, общие месяцы, знак и p-value (§4.3)."""

    def test_dm_sign_and_pvalue(self):
        months = month_range(date(2020, 1, 1), date(2020, 5, 1))
        good = [0.1, -0.2, 0.3, -0.15, 0.2]
        bad = [1.0, -0.9, 1.1, -1.2, 0.8]
        predictions = []
        for month, error_good, error_bad in zip(months, good, bad):
            predictions.append(prediction("good", month, error_good))
            predictions.append(prediction("bad", month, error_bad))
        tests = cm.pairwise_tests(predictions, [("good", "bad")])
        self.assertEqual(len(tests), 1)
        test = tests[0]
        self.assertEqual((test.model_a, test.model_b), ("good", "bad"))
        self.assertLess(test.diff, 0.0)  # отрицательное — модель A точнее
        self.assertLess(test.p_value, 0.05)
        self.assertIn("good", test.note)
        self.assertIn("значимо", test.note)
        # Одинаковые ошибки: разницы нет, p = 1 (вырожденная дисперсия).
        equal = [
            prediction("a", month, 0.5) for month in months
        ] + [
            prediction("b", month, 0.5) for month in months
        ]
        test = cm.pairwise_tests(equal, [("a", "b")])[0]
        self.assertEqual(test.diff, 0.0)
        self.assertEqual(test.p_value, 1.0)
        self.assertIn("не значимо", test.note)
        self.assertIn("разницы RMSE нет", test.note)

    def test_pairs_use_common_months(self):
        months = month_range(date(2020, 1, 1), date(2020, 4, 1))
        predictions = [
            # A: январь — огромная ошибка, которой нет у B; в пару не попадёт.
            prediction("a", months[0], 100.0),
            prediction("a", months[1], 2.0),
            prediction("a", months[2], 3.0),
            prediction("b", months[1], 2.0),
            prediction("b", months[2], 2.0),
            prediction("b", months[3], 2.0),
        ]
        tests = cm.pairwise_tests(predictions, [("a", "b")])
        self.assertEqual(len(tests), 1)
        test = tests[0]
        self.assertEqual(test.diff, math.sqrt(13.0 / 2.0) - 2.0)
        # Пара без общих месяцев не строится вовсе.
        self.assertEqual(
            cm.pairwise_tests(predictions, [("a", "absent")]), []
        )

    def test_primary_pairs_are_mp_vs_base(self):
        self.assertEqual(cm.PRIMARY_PAIRS, (
            (cm.MODEL_CATBOOST_DIRECT_MP, cm.MODEL_CATBOOST_DIRECT),
            (cm.MODEL_CATBOOST_BOTTOMUP_MP, cm.MODEL_CATBOOST_BOTTOMUP),
            (cm.MODEL_ENET_MP, cm.MODEL_ENET_BASE),
        ))

    def test_secondary_pairs_context(self):
        self.assertEqual(cm.SECONDARY_PAIRS, (
            (cm.MODEL_CATBOOST_DIRECT_MP, cm.MODEL_LAST),
            (cm.MODEL_CATBOOST_BOTTOMUP_MP, cm.MODEL_LAST),
            (cm.MODEL_ENET_MP, cm.MODEL_LAST),
            (cm.MODEL_CATBOOST_DIRECT_MP, cm.MODEL_ENET_MP),
            (cm.MODEL_CATBOOST_BOTTOMUP_MP, cm.MODEL_ENET_MP),
            (cm.MODEL_CATBOOST_DIRECT, cm.MODEL_CATBOOST_BOTTOMUP),
        ))

    def test_bootstrap_primary_pairs(self):
        months = month_range(date(2019, 1, 1), date(2020, 12, 1))
        predictions = []
        for position, month in enumerate(months):
            for name in {name for pair in cm.PRIMARY_PAIRS for name in pair}:
                predictions.append(
                    prediction(name, month, 0.1 + 0.01 * position)
                )
        results = cm.bootstrap_pairs(predictions, cm.PRIMARY_PAIRS)
        self.assertEqual(len(results), len(cm.PRIMARY_PAIRS))
        for (model_a, model_b), result in zip(cm.PRIMARY_PAIRS, results):
            self.assertEqual((result.model_a, result.model_b), (model_a, model_b))
            self.assertLessEqual(result.low, result.high)
            self.assertGreaterEqual(result.p_value, 0.0)
            self.assertLessEqual(result.p_value, 1.0)
            self.assertAlmostEqual(result.rmse_diff, 0.0)


class TestArticleSplit(unittest.TestCase):
    """Фиксированный сплит статьи: train ≤ 2018-12, test 2019-01…2024-07 (§4.4)."""

    def test_split_boundaries_and_no_future(self):
        rows, _ = build_world(synthetic_world())
        self.assertEqual(len(rows), 155)
        calls = []

        def model(train, row):
            calls.append(
                (
                    len(train),
                    train[0].month,
                    train[-1].month,
                    row.month,
                )
            )
            return float(row.features["y_lag1"])

        predictions = cm.fixed_split(rows, {"stub": model})
        self.assertEqual(len(predictions), 67)
        self.assertEqual(predictions[0].month, date(2019, 1, 1))
        self.assertEqual(predictions[-1].month, date(2024, 7, 1))
        self.assertEqual(len(calls), 67)
        for train_size, first_train, last_train, _ in calls:
            self.assertEqual(train_size, 63)
            self.assertEqual(first_train, date(2013, 10, 1))
            self.assertEqual(last_train, date(2018, 12, 1))
        self.assertEqual(
            {prediction.model for prediction in predictions}, {"stub"}
        )

    def test_degenerate_model_is_skipped(self):
        rows, _ = build_world(synthetic_world())

        def degenerate(train, row):
            raise cm.DegenerateWindowError("окно вырождено")

        def healthy(train, row):
            return 0.5

        predictions = cm.fixed_split(
            rows, {"bad": degenerate, "good": healthy}
        )
        self.assertEqual(len(predictions), 67)
        self.assertEqual(
            {prediction.model for prediction in predictions}, {"good"}
        )

    def test_split_constants(self):
        self.assertEqual(cm.ARTICLE_TRAIN_END, date(2018, 12, 1))
        self.assertEqual(cm.ARTICLE_TEST_END, date(2024, 7, 1))


class TestMetricsWithout2022(unittest.TestCase):
    """Строка «2022 исключён»: отдельная метрика, не замена основной (§4.5)."""

    def test_no_shock_excludes_2022(self):
        predictions = [
            prediction("m", date(2021, 12, 1), 0.1),
            prediction("m", date(2022, 1, 1), 10.0),
            prediction("m", date(2022, 12, 1), -10.0),
            prediction("m", date(2023, 1, 1), 0.3),
        ]
        rmse, mae = cm.metrics_without_2022(predictions)
        self.assertAlmostEqual(rmse, math.sqrt((0.01 + 0.09) / 2))
        self.assertAlmostEqual(mae, (0.1 + 0.3) / 2)
        with self.assertRaises(ValueError):
            cm.metrics_without_2022([prediction("m", date(2022, 5, 1), 1.0)])


class TestLiveNowcast(unittest.TestCase):
    """Живой nowcast: цель по данным, признаки as-of run date, МП §2.6 (§5.3)."""

    def test_live_target_is_month_after_last_published(self):
        targets = cm.targets_from_rows(fixture_rows("ipc_mes_recent.csv"))
        self.assertEqual(cm.latest_published_month(targets), date(2026, 9, 1))
        month, reason = cm.nowcast_target_month(targets, date(2026, 10, 18))
        self.assertEqual((month, reason), (date(2026, 10, 1), ""))

    def test_no_target_reason(self):
        self.assertIsNone(cm.latest_published_month({}))
        month, reason = cm.nowcast_target_month(
            {cm.TARGET_ALL: []}, date(2026, 10, 18)
        )
        self.assertIsNone(month)
        self.assertIn("нет ни одной метки", reason)

    def test_live_base_features_allow_published_lag(self):
        world = synthetic_world()
        index = cm.target_index(world["targets"])
        month = date(2026, 9, 1)
        today = date(2026, 9, 18)
        strict, reason = cm.base_features(
            month, index, world["usd"], world["key_rate"], world["fao"], today
        )
        self.assertIsNone(strict)
        self.assertIn("y_lag1", reason)
        live, reason = cm.base_features(
            month, index, world["usd"], world["key_rate"], world["fao"], today,
            live=True,
        )
        self.assertIsNotNone(live, reason)
        labels = dict(world["targets"][cm.TARGET_ALL])
        self.assertAlmostEqual(live["y_lag1"], labels[date(2026, 8, 31)])
        self.assertAlmostEqual(live["y_lag2"], labels[date(2026, 7, 31)])

    def test_live_mp_features_take_last_available(self):
        world = synthetic_world()
        month = date(2026, 9, 1)
        today = date(2026, 9, 18)
        strict, reason = cm.mp_features(month, world["survey"], today)
        self.assertIsNone(strict)
        self.assertIn("не опубликован", reason)
        live, reason = cm.mp_features(
            month, world["survey"], today, live=True
        )
        self.assertIsNotNone(live, reason)
        points = dict(world["survey"]["price_expectations_sa"])
        # Точка августа доступна с конца сентября, поэтому на 18.09 — июль.
        self.assertAlmostEqual(
            live["price_expectations_sa"], points[date(2026, 7, 1)]
        )
        self.assertAlmostEqual(
            live["price_expectations_sa_chg"],
            points[date(2026, 7, 1)] - points[date(2026, 6, 1)],
        )

    def test_live_mp_lag_note(self):
        world = synthetic_world()
        rows, _ = build_world(world)
        today = date(2026, 9, 18)
        with unittest.mock.patch.object(cm, "MODELS", stub_models()):
            nowcast, reason = cm.current_nowcast(
                rows,
                world["targets"],
                world["survey"],
                world["usd"],
                world["key_rate"],
                world["fao"],
                today,
            )
        self.assertIsNotNone(nowcast, reason)
        self.assertEqual(nowcast.target_month, date(2026, 9, 1))
        self.assertEqual(nowcast.forecast_date, today)
        self.assertEqual(nowcast.data_asof, today)
        self.assertIn("МП за 2026-07", nowcast.note)
        self.assertIn("глубже бэктеста на 1 мес.", nowcast.note)
        self.assertEqual(set(nowcast.models), set(cm.CATBOOST_MODELS))
        self.assertEqual(set(nowcast.benchmarks), set(cm.BENCHMARKS))
        self.assertEqual(len(nowcast.weights), len(cm.BOTTOMUP_COMPONENTS))
        self.assertAlmostEqual(sum(nowcast.weights), 1.0)
        self.assertEqual(nowcast.sample_size, len(rows))

    def test_forecast_json_deterministic(self):
        world = synthetic_world()
        rows, _ = build_world(world)
        today = date(2026, 9, 18)
        values = {
            cm.MODEL_CATBOOST_DIRECT: 0.41,
            cm.MODEL_CATBOOST_BOTTOMUP: 0.42,
            cm.MODEL_CATBOOST_DIRECT_MP: 0.43,
            cm.MODEL_CATBOOST_BOTTOMUP_MP: 0.44,
        }
        with unittest.mock.patch.object(cm, "MODELS", stub_models(values)):
            first, reason = cm.current_nowcast(
                rows, world["targets"], world["survey"], world["usd"],
                world["key_rate"], world["fao"], today,
            )
            second, _ = cm.current_nowcast(
                rows, world["targets"], world["survey"], world["usd"],
                world["key_rate"], world["fao"], today,
            )
        self.assertIsNotNone(first, reason)
        self.assertEqual(cm.nowcast_json(first), cm.nowcast_json(second))
        payload = json.loads(cm.nowcast_json(first))
        self.assertEqual(
            list(payload)[:3], ["forecast_date", "target_month", "data_asof"]
        )
        self.assertEqual(payload["forecast_date"], "2026-09-18")
        self.assertEqual(payload["target_month"], "2026-09-01")
        self.assertEqual(payload["data_asof"], "2026-09-18")
        for name in cm.CATBOOST_MODELS:
            self.assertEqual(set(payload[name]), {"forecast", "metadata"})
            self.assertEqual(payload[name]["forecast"], values[name])
        self.assertIn(
            "price_expectations_sa",
            payload[cm.MODEL_CATBOOST_DIRECT_MP]["metadata"]["num_features"],
        )
        self.assertNotIn(
            "price_expectations_sa",
            payload[cm.MODEL_CATBOOST_DIRECT]["metadata"]["num_features"],
        )
        self.assertEqual(set(payload["benchmarks"]), set(cm.BENCHMARKS))
        self.assertEqual(set(payload["bottomup_weights"]), set(cm.BOTTOMUP_MODELS))
        for name in cm.BOTTOMUP_MODELS:
            weights = payload["bottomup_weights"][name]
            self.assertEqual(set(weights), set(cm.BOTTOMUP_COMPONENTS))
            self.assertAlmostEqual(sum(weights.values()), 1.0)
        self.assertEqual(payload["sample_size"], len(rows))
        self.assertIn("живой nowcast", payload["note"])
        self.assertTrue(cm.nowcast_json(first).endswith("\n"))

    def test_nowcast_without_models(self):
        world = synthetic_world()
        rows, _ = build_world(world)

        def degenerate(train, row):
            raise cm.DegenerateWindowError("окно вырождено")

        with unittest.mock.patch.object(
            cm, "MODELS", {name: degenerate for name in cm.MODELS}
        ):
            nowcast, reason = cm.current_nowcast(
                rows, world["targets"], world["survey"], world["usd"],
                world["key_rate"], world["fao"], date(2026, 9, 18),
            )
        self.assertIsNone(nowcast)
        self.assertIn("ни один метод не посчитан", reason)

    def test_write_and_remove_forecast_artifact(self):
        today = date(2026, 10, 18)
        nowcast = cm.Nowcast(
            forecast_date=today,
            target_month=date(2026, 10, 1),
            data_asof=today,
            models={name: 0.5 for name in cm.CATBOOST_MODELS},
            benchmarks={name: 0.4 for name in cm.BENCHMARKS},
            weights=(0.4, 0.35, 0.25),
            sample_size=155,
            note="тестовая пометка",
        )
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / cm.FORECAST_NAME
            self.assertTrue(cm.write_nowcast_artifact(path, nowcast))
            payload = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(payload["target_month"], "2026-10-01")
            self.assertFalse(cm.write_nowcast_artifact(path, None))
            self.assertFalse(path.exists())

    def test_clear_stale_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for name in cm.ARTIFACT_NAMES:
                (directory / name).write_text("old", encoding="utf-8")
            removed = cm.clear_stale_artifacts(directory)
            self.assertEqual(set(removed), set(cm.ARTIFACT_NAMES))
            for name in cm.ARTIFACT_NAMES:
                self.assertFalse((directory / name).exists())

    def test_render_nowcast_sections(self):
        today = date(2026, 10, 18)
        nowcast = cm.Nowcast(
            forecast_date=today,
            target_month=date(2026, 10, 1),
            data_asof=today,
            models={
                cm.MODEL_CATBOOST_DIRECT: 0.4,
                cm.MODEL_CATBOOST_BOTTOMUP: 0.35,
                cm.MODEL_CATBOOST_DIRECT_MP: 0.3,
                cm.MODEL_CATBOOST_BOTTOMUP_MP: None,
            },
            benchmarks={name: 0.5 for name in cm.BENCHMARKS},
            weights=(0.4, 0.35, 0.25),
            sample_size=155,
            note="пометка",
        )
        text = cm.render_nowcast(nowcast)
        self.assertIn("2026-10", text)
        self.assertIn("CatBoost direct (с МП)", text)
        self.assertIn("0.400", text)
        self.assertIn("0.400", text)
        self.assertIn("пометка", text)
        absent = cm.render_nowcast_absent("нет цели (тест)")
        self.assertIn("нет цели (тест)", absent)
        self.assertIn(cm.FORECAST_NAME, absent)


class TestReportTask4(unittest.TestCase):
    """Полный отчёт: DM-блок, бутстрап, сплит статьи, «2022 исключён», nowcast."""

    def full_run(self) -> cm.WindowRun:
        months = month_range(date(2021, 1, 1), date(2023, 12, 1))
        predictions = []
        for position, month in enumerate(months):
            for name in cm.MODELS:
                bias = 0.01 if name.endswith("_mp") else 0.02
                predictions.append(
                    cm.Prediction(
                        month=month,
                        model=name,
                        y_true=0.5 + 0.001 * position,
                        y_pred=0.5 + 0.001 * position + bias,
                    )
                )
        notes = {name: {} for name in cm.MODELS}
        notes[cm.MODEL_ENET_MP] = {cm.NON_CONVERGED: 43}
        return cm.WindowRun(
            start=24,
            embargo=0,
            main=True,
            folds=len(months),
            predictions=predictions,
            skipped={name: 0 for name in cm.MODELS},
            notes=notes,
            weights={
                cm.MODEL_CATBOOST_BOTTOMUP: (0.4, 0.35, 0.25),
                cm.MODEL_CATBOOST_BOTTOMUP_MP: (0.4, 0.35, 0.25),
            },
        )

    def test_full_report_sections(self):
        rows, _ = build_world(synthetic_world())
        run = self.full_run()
        split_predictions = cm.fixed_split(
            rows, {name: (lambda train, row: 0.5) for name in cm.MODELS}
        )
        report = cm.render_report(
            rows,
            [],
            [run],
            today=date(2026, 10, 18),
            split_predictions=split_predictions,
        )
        self.assertIn("## DM-HLN", report)
        self.assertIn("## DM-HLN: с МП vs без МП", report)
        self.assertIn("secondary", report)
        self.assertIn("## Блочный бутстрап", report)
        self.assertIn("| start=24 (основное) | primary |", report)
        self.assertIn("43 из 36", report)
        self.assertIn("не доказательство", report)
        self.assertIn("## Сплит статьи", report)
        self.assertIn("63", report)
        self.assertIn("67", report)
        self.assertIn("таблица сопоставимости", report)
        self.assertIn("2022 исключён", report)
        self.assertIn("## Текущий nowcast", report)
        self.assertIn("Текущий nowcast не рассчитан", report)

    def test_2022_row_is_a_separate_metric(self):
        rows, _ = build_world(synthetic_world())
        run = self.full_run()
        report = cm.render_report(
            rows, [], [run], today=date(2026, 10, 18)
        )
        # 36 тестовых месяцев 2021-01…2023-12, из них 2022 — 12: в строке 24.
        self.assertIn(
            "| B1 last value (2022 исключён) | start=24 (основное) | 24 |",
            report,
        )


class TestRunSummaryTask4(unittest.TestCase):
    """Ветки артефактов: нет цели → stale JSON удалён; нет прогнозов → все три."""

    def test_no_predictions_clears_all(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for name in cm.ARTIFACT_NAMES:
                (directory / name).write_text("old", encoding="utf-8")
            output = io.StringIO()
            with unittest.mock.patch.object(cm, "ARTIFACTS_DIR", directory), \
                    unittest.mock.patch.object(cm, "load_targets", return_value={}), \
                    unittest.mock.patch.object(cm, "load_survey", return_value={}), \
                    unittest.mock.patch.object(cm, "load_usd", return_value=[]), \
                    unittest.mock.patch.object(cm, "load_key_rate", return_value=[]), \
                    unittest.mock.patch.object(cm, "load_fao", return_value=[]), \
                    contextlib.redirect_stdout(output):
                code = cm.main([])
            self.assertEqual(code, 0)
            self.assertIn("не пишутся", output.getvalue())
            for name in cm.ARTIFACT_NAMES:
                self.assertFalse((directory / name).exists(), name)

    def test_stale_forecast_removed_without_target(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / cm.FORECAST_NAME).write_text("{}", encoding="utf-8")
            output = io.StringIO()
            with unittest.mock.patch.object(cm, "ARTIFACTS_DIR", directory), \
                    unittest.mock.patch.object(
                        cm,
                        "current_nowcast",
                        return_value=(None, "нет цели (тест)"),
                    ), \
                    fixture_transport(), contextlib.redirect_stdout(output):
                code = cm.main(["--start", "3"])
            self.assertEqual(code, 0)
            report_path = directory / cm.REPORT_NAME
            predictions_path = directory / cm.PREDICTIONS_NAME
            self.assertTrue(report_path.exists())
            self.assertTrue(predictions_path.exists())
            self.assertFalse((directory / cm.FORECAST_NAME).exists())
            self.assertIn(
                "Текущий nowcast не рассчитан: нет цели (тест)",
                report_path.read_text(encoding="utf-8"),
            )


# --- Task 7: журнал ML-экспериментов (ML_LOG=1) -------------------------------


class FakeLogger:
    """Журнал-заглушка: запоминает вызовы и умеет падать по требованию.

    Контракт — duck typing `ExperimentRun`: тест читает `metrics`/`predictions`/
    `artifacts`, а `finish` фиксирует вызов ДО возможного сбоя, чтобы проверка
    `finish("failed")` работала и на сломанном журнале.
    """

    def __init__(self, fail_on=()):
        self.fail_on = set(fail_on)
        self.metrics = None
        self.predictions = []
        self.artifacts = []
        self.finished = []

    def _fail(self, name: str) -> None:
        if name in self.fail_on:
            raise RuntimeError(f"{name} сломан (тест)")

    def log_metrics(self, metrics: dict) -> None:
        self.metrics = metrics
        self._fail("log_metrics")

    def log_predictions(self, rows) -> None:
        self.predictions.extend(rows)
        self._fail("log_predictions")

    def log_artifacts(self, paths) -> None:
        self.artifacts.extend(paths)
        self._fail("log_artifacts")

    def finish(self, status: str = "finished", notes: str = "") -> None:
        self.finished.append((status, notes))
        self._fail("finish")


def fake_window_runs(specs, folds: int = 4) -> list:
    """Детерминированные окна walk-forward: `folds` месяцев × все `MODELS`.

    Тесты журнала не должны зависеть от скорости CatBoost: `evaluate_windows`
    подменяется этими окнами с той же формой `WindowRun` (метки окон, счётчики
    пропусков, веса NNLS). Bias зависит от модели, чтобы DM-пары в отчёте не
    были вырожденными.
    """
    runs = []
    for position, (start, embargo) in enumerate(specs):
        predictions = [
            cm.Prediction(
                month=cm._month_shift(date(2013, 1, 1), offset),
                model=name,
                y_true=0.5 + 0.001 * offset,
                y_pred=0.6 + 0.01 * model_index + 0.001 * offset,
            )
            for offset in range(folds)
            for model_index, name in enumerate(cm.MODELS)
        ]
        runs.append(
            cm.WindowRun(
                start=start,
                embargo=embargo,
                main=position == 0,
                folds=folds,
                predictions=predictions,
                skipped={name: 0 for name in cm.MODELS},
                notes={name: {} for name in cm.MODELS},
                weights={cm.MODEL_CATBOOST_BOTTOMUP: (0.4, 0.35, 0.25)},
            )
        )
    return runs


def clean_ml_env(**extra) -> dict:
    """Окружение без `ML_LOG`/`ML_RUN_NAME`: тест не зависит от shell."""
    env = {
        key: value
        for key, value in os.environ.items()
        if key not in ("ML_LOG", "ML_RUN_NAME")
    }
    env.update(extra)
    return env


class TestQuickMode(unittest.TestCase):
    """`--quick`: только основное окно, без robustness и фиксированного сплита."""

    def run_summary(self, quick: bool, directory: Path) -> tuple:
        """Офлайн-прогон сводки с подменённым walk-forward и nowcast."""
        split_mock = unittest.mock.Mock(return_value=[])
        captured = {}

        def fake_evaluate(rows, models, specs, **kwargs):
            captured["specs"] = list(specs)
            return fake_window_runs(specs)

        with unittest.mock.patch.dict(
                os.environ, clean_ml_env(), clear=True
        ), unittest.mock.patch.object(
                cm, "evaluate_windows", fake_evaluate
        ), unittest.mock.patch.object(
                cm, "fixed_split", split_mock
        ), unittest.mock.patch.object(
                cm, "current_nowcast", return_value=(None, "нет цели (тест)")
        ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
            code = cm.run_summary(
                out_dir=directory,
                today=date(2026, 10, 18),
                start=3,
                quick=quick,
            )
        report = (directory / cm.REPORT_NAME).read_text(encoding="utf-8")
        return code, split_mock, captured["specs"], report

    def test_quick_skips_fixed_split(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, split_mock, specs, report = self.run_summary(True, Path(tmp))
        self.assertEqual(code, 0)
        # quick: одно основное окно и ни одного вызова fixed_split.
        self.assertEqual(specs, [(3, 0)])
        split_mock.assert_not_called()
        self.assertIn("(quick)", report)
        self.assertIn("quick: фиксированный сплит не считался", report)

    def test_full_run_still_calls_fixed_split(self):
        # Регрессия полного пути: без --quick окна и сплит прежние.
        with tempfile.TemporaryDirectory() as tmp:
            code, split_mock, specs, report = self.run_summary(False, Path(tmp))
        self.assertEqual(code, 0)
        self.assertEqual(specs, [(3, 0), (48, 0), (3, 1)])
        split_mock.assert_called_once()
        self.assertNotIn("(quick)", report)
        self.assertIn("## Сплит статьи (сопоставимость, не выбор модели)", report)

    def test_quick_default_specs_only_main(self):
        # API без явных specs: quick не должен подхватывать полный набор окон.
        rows = catboost_rows(30)
        models = {"stub": lambda train, row: float(len(train))}
        quick = cm.evaluate_windows(rows, models, quick=True, jobs=1)
        self.assertEqual(len(quick), 1)
        self.assertEqual((quick[0].start, quick[0].embargo), (24, 0))
        self.assertTrue(quick[0].main)
        full = cm.evaluate_windows(rows, models, quick=False, jobs=0)
        self.assertEqual(
            [(run.start, run.embargo) for run in full],
            [(24, 0), (48, 0), (24, 1)],
        )

    def main_run_with_2022(self) -> cm.WindowRun:
        """Main-окно с тестовыми месяцами 2021–2023 (включая 2022)."""
        months = month_range(date(2021, 1, 1), date(2023, 12, 1))
        predictions = [
            cm.Prediction(
                month=month,
                model=name,
                y_true=0.5 + 0.001 * offset,
                y_pred=0.5 + 0.001 * offset + 0.01 * model_index,
            )
            for offset, month in enumerate(months)
            for model_index, name in enumerate(cm.MODELS)
        ]
        return cm.WindowRun(
            start=24,
            embargo=0,
            main=True,
            folds=len(months),
            predictions=predictions,
            skipped={name: 0 for name in cm.MODELS},
            notes={name: {} for name in cm.MODELS},
        )

    def test_quick_report_marks_quick_and_no_split(self):
        rows, _ = build_world(synthetic_world())
        report = cm.render_report(
            rows,
            [],
            [self.main_run_with_2022()],
            today=date(2026, 10, 18),
            quick=True,
        )
        self.assertIn("(quick)", report)
        self.assertIn(
            "quick: только основное окно; robustness/сплит не считались", report
        )
        self.assertIn("## Сплит статьи (quick: не рассчитывался)", report)
        self.assertIn("quick: фиксированный сплит не считался", report)
        self.assertNotIn("## Сплит статьи (сопоставимость", report)
        self.assertNotIn("таблица сопоставимости", report)
        # DM primary main-окна, «2022 исключён» и живой nowcast остаются.
        self.assertIn("## DM-HLN", report)
        self.assertIn("## DM-HLN: с МП vs без МП", report)
        self.assertIn("2022 исключён", report)
        self.assertIn("## Текущий nowcast (live)", report)

    def test_full_report_has_split(self):
        rows, _ = build_world(synthetic_world())
        split_predictions = cm.fixed_split(
            rows, {name: (lambda train, row: 0.5) for name in cm.MODELS}
        )
        report = cm.render_report(
            rows,
            [],
            [self.main_run_with_2022()],
            today=date(2026, 10, 18),
            split_predictions=split_predictions,
        )
        self.assertIn("## Сплит статьи (сопоставимость, не выбор модели)", report)
        self.assertIn("таблица сопоставимости", report)
        self.assertNotIn("(quick)", report)
        self.assertNotIn("quick: фиксированный сплит не считался", report)

    def test_quick_ml_log_config_and_run_name(self):
        fake = FakeLogger()
        captured = {}

        def start_run(**kwargs):
            captured.update(kwargs)
            return fake

        def fake_evaluate(rows, models, specs, **kwargs):
            captured["specs"] = list(specs)
            return fake_window_runs(specs)

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(ML_LOG="1"), clear=True
            ), unittest.mock.patch.object(
                    cm.mlx, "start_run", start_run
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", fake_evaluate
            ), unittest.mock.patch.object(
                    cm, "fixed_split",
                    side_effect=AssertionError("quick не вызывает fixed_split"),
            ), unittest.mock.patch.object(
                    cm, "current_nowcast", return_value=(None, "нет цели (тест)")
            ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=Path(tmp),
                    today=date(2026, 10, 18),
                    start=3,
                    quick=True,
                )
        self.assertEqual(code, 0)
        self.assertEqual(captured["specs"], [(3, 0)])
        self.assertTrue(captured["config"]["quick"])
        self.assertEqual(captured["config"]["windows"], [[3, 0]])
        self.assertEqual(
            captured["run_name"], "cpi_monthly__short__quick__start3__embargo0"
        )
        # metrics/predictions — только main-окно; сплит не рассчитан (null).
        self.assertEqual(
            [window["window"] for window in fake.metrics["windows"]],
            ["start=3"],
        )
        self.assertIsNone(fake.metrics["article_split"])
        self.assertEqual({row.window for row in fake.predictions}, {"start=3"})
        self.assertTrue(all(row.meta["main"] for row in fake.predictions))
        self.assertEqual(fake.finished[0][0], "finished")

    def test_quick_cli_flag(self):
        with unittest.mock.patch.object(
                cm, "run_summary", return_value=0
        ) as summary:
            code = cm.main(
                ["--quick", "--sample", "long", "--start", "48",
                 "--embargo-months", "1"]
            )
        self.assertEqual(code, 0)
        self.assertEqual(
            summary.call_args.kwargs,
            {"start": 48, "embargo_months": 1, "sample": "long", "quick": True,
             "jobs": 0},
        )
        # Без флага quick выключен: полный путь не меняется.
        with unittest.mock.patch.object(
                cm, "run_summary", return_value=0
        ) as summary:
            self.assertEqual(cm.main([]), 0)
        self.assertFalse(summary.call_args.kwargs["quick"])


class TestQuickParallelJobs(unittest.TestCase):
    """Task 2: `--jobs N` — пул фолдов quick, детерминизм и полный путь.

    Пул использует `spawn` (macOS), поэтому задачи несут только данные
    (`Row`) и имена моделей: функции берутся из `MODELS` в воркере. Тест на
    фикстурах сравнивает `jobs 1` и `jobs 2/4` — прогнозы обязаны совпасть
    побитно (CatBoost `thread_count=1` в обоих путях).
    """

    def test_quick_parallel_matches_sequential(self):
        rows = catboost_rows(22)
        specs = [(15, 0)]
        sequential = cm.evaluate_windows(
            rows, cm.MODELS, specs=specs, quick=True, jobs=1
        )[0]
        self.assertTrue(sequential.predictions)
        for jobs in (2, 4):
            parallel = cm.evaluate_windows(
                rows, cm.MODELS, specs=specs, quick=True, jobs=jobs
            )[0]
            self.assertEqual(
                [(p.month, p.model, p.y_pred) for p in sequential.predictions],
                [(p.month, p.model, p.y_pred) for p in parallel.predictions],
                f"jobs={jobs}",
            )
            # Счётчики, диагностика и веса NNLS — тоже фолд-в-фолд.
            self.assertEqual(sequential.skipped, parallel.skipped, f"jobs={jobs}")
            self.assertEqual(sequential.notes, parallel.notes, f"jobs={jobs}")
            self.assertEqual(sequential.weights, parallel.weights, f"jobs={jobs}")

    def test_quick_parallel_repeat_deterministic(self):
        # Два полных quick-прогона с пулом: артефакты обязаны совпасть sha256.
        digests = []
        for _ in range(2):
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                with unittest.mock.patch.dict(
                        os.environ, clean_ml_env(), clear=True
                ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                    code = cm.run_summary(
                        out_dir=directory,
                        today=date(2026, 10, 18),
                        start=3,
                        quick=True,
                        jobs=2,
                    )
                self.assertEqual(code, 0)
                digests.append({
                    name: hashlib.sha256(
                        (directory / name).read_bytes()
                    ).hexdigest()
                    for name in (
                        cm.REPORT_NAME,
                        cm.PREDICTIONS_NAME,
                        cm.FORECAST_NAME,
                    )
                })
        self.assertEqual(digests[0], digests[1])

    def test_parallel_worker_picklable(self):
        self.assertEqual(cm._run_fold_worker.__module__, cm.__name__)
        # Не замыкание: `spawn` pickle'ит ссылку, а не замыкание теста.
        self.assertIsNone(cm._run_fold_worker.__closure__)
        # Сама функция тоже обязана пережить pickle (ссылка module.qualname).
        restored_worker = pickle.loads(pickle.dumps(cm._run_fold_worker))
        self.assertIs(restored_worker, cm._run_fold_worker)
        self.assertEqual(restored_worker.__module__, cm.__name__)
        self.assertEqual(restored_worker.__name__, "_run_fold_worker")
        rows = catboost_rows(20)
        task = (7, rows[:5], rows[7], list(cm.MODELS))
        restored = pickle.loads(pickle.dumps(task))
        self.assertEqual(restored[0], 7)
        self.assertEqual(restored[3], list(cm.MODELS))
        self.assertEqual(
            [row.month for row in restored[1]], [row.month for row in rows[:5]]
        )
        self.assertEqual(restored[2].month, rows[7].month)

    def test_spawn_worker_resolves_global_model(self):
        # Не in-process: настоящий spawn-воркер импортирует cpi_monthly заново
        # и обязан сам найти модель в глобальном MODELS.
        rows = catboost_rows(6)
        task = (2, rows[:2], rows[2], [cm.MODEL_LAST])
        context = multiprocessing.get_context("spawn")
        with ProcessPoolExecutor(max_workers=1, mp_context=context) as pool:
            index, fold = pool.submit(cm._run_fold_worker, task).result(
                timeout=180
            )
        self.assertEqual(index, 2)
        result = fold[cm.MODEL_LAST]
        self.assertFalse(result["skipped"])
        # B1 = y_lag1 тестовой строки (признак, а не y предыдущей строки).
        self.assertAlmostEqual(
            result["pred"], rows[2].features["y_lag1"], places=12
        )

    def test_parallel_skipped_and_notes(self):
        rows = catboost_rows(12)

        def degenerate(train, row):
            raise cm.DegenerateWindowError("окно вырождено (тест)")

        def noted(train, row):
            noted.fold_notes = {cm.NON_CONVERGED: 1}
            return 0.5

        models = {"degenerate": degenerate, "noted": noted}
        with unittest.mock.patch.object(cm, "MODELS", models):
            index, fold = cm._run_fold_worker(
                (0, rows[:5], rows[5], list(models))
            )
        self.assertEqual(index, 0)
        self.assertTrue(fold["degenerate"]["skipped"])
        self.assertIsNone(fold["degenerate"]["pred"])
        self.assertFalse(fold["noted"]["skipped"])
        self.assertEqual(fold["noted"]["pred"], 0.5)
        self.assertEqual(fold["noted"]["notes"], {cm.NON_CONVERGED: 1})

    def test_parallel_skipped_matches_sequential(self):
        # Короткие окна — реальные вырожденные фолды B2/B3: счётчики и notes
        # пула обязаны совпасть с последовательным путём.
        rows = catboost_rows(18)
        specs = [(10, 0)]
        sequential = cm.evaluate_windows(
            rows, cm.MODELS, specs=specs, quick=True, jobs=1
        )[0]
        parallel = cm.evaluate_windows(
            rows, cm.MODELS, specs=specs, quick=True, jobs=2
        )[0]
        self.assertGreater(sum(sequential.skipped.values()), 0)
        self.assertEqual(sequential.skipped, parallel.skipped)
        self.assertEqual(sequential.notes, parallel.notes)
        self.assertEqual(
            [(p.month, p.model, p.y_pred) for p in sequential.predictions],
            [(p.month, p.model, p.y_pred) for p in parallel.predictions],
        )

    def test_parallel_rejects_non_global_models(self):
        # Воркер получает только имена и резолвит их в глобальном MODELS:
        # произвольный словарь функций обязан дать ValueError ДО пула, а не
        # KeyError/подмену модели в дочернем процессе.
        with self.assertRaisesRegex(ValueError, "глобальной MODELS"):
            cm.evaluate_windows(
                catboost_rows(20),
                {"stub": lambda train, row: 0.0},
                specs=[(10, 0)],
                quick=True,
                jobs=2,
            )
        # Даже имя реальной модели с ДРУГОЙ функцией — ошибка, а не тихая
        # подмена на глобальную.
        with self.assertRaisesRegex(ValueError, "b1_last_value"):
            cm.walk_forward_parallel(
                catboost_rows(20),
                {cm.MODEL_LAST: lambda train, row: 0.0},
                start=10,
                embargo=0,
                jobs=2,
            )

    def test_jobs_requires_quick(self):
        for jobs in ("2", "1", "0"):
            with contextlib.redirect_stderr(io.StringIO()) as stderr:
                with self.assertRaises(SystemExit):
                    cm.main(["--jobs", jobs])
            self.assertIn("--quick", stderr.getvalue(), jobs)

    def test_negative_jobs_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cm.main(["--quick", "--jobs", "-1"])
        with self.assertRaises(ValueError):
            cm.evaluate_windows(
                catboost_rows(20),
                {"stub": lambda train, row: 0.0},
                specs=[(10, 0)],
                quick=True,
                jobs=-1,
            )

    def test_jobs_0_1_sequential(self):
        rows = catboost_rows(20)
        models = {"stub": lambda train, row: float(len(train))}
        with unittest.mock.patch.object(
                cm,
                "walk_forward_parallel",
                side_effect=AssertionError("jobs 0/1 не поднимают пул"),
        ):
            for jobs in (0, 1):
                runs = cm.evaluate_windows(
                    rows, models, specs=[(10, 0)], quick=True, jobs=jobs
                )
                self.assertEqual(len(runs[0].predictions), 10)

    def test_quick_thread_count_only_in_quick_context(self):
        with cm.catboost_thread_count(cm.QUICK_CATBOOST_THREAD_COUNT):
            params = cm.catboost_params(False)
            metadata = cm.catboost_metadata(True)
            model = cm.catboost_model(False)
        self.assertEqual(params["thread_count"], 1)
        self.assertEqual(metadata["thread_count"], 1)
        self.assertEqual(model.get_params()["thread_count"], 1)
        # Контекст снят: полный путь снова не задаёт thread_count.
        self.assertNotIn("thread_count", cm.catboost_params(False))
        self.assertNotIn("thread_count", cm.catboost_metadata(False))

    def test_full_mode_unchanged_after_jobs(self):
        # `--jobs` без quick отвергается и CLI, и API (любое значение, 0/1/4):
        # полный путь не меняется и не параллелится.
        for jobs in ("0", "1", "4"):
            with contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    cm.main(["--jobs", jobs])
        for jobs in (1, 4):
            with self.assertRaises(ValueError):
                cm.evaluate_windows(
                    catboost_rows(20),
                    {"stub": lambda train, row: 0.0},
                    specs=[(10, 0)],
                    quick=False,
                    jobs=jobs,
                )
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):
                cm.run_summary(
                    out_dir=Path(tmp), jobs=1, quick=False, start=3
                )
        self.assertEqual(cm.window_specs(), [(24, 0), (48, 0), (24, 1)])
        self.assertEqual(cm.window_specs(48, 0), [(48, 0), (48, 1)])
        self.assertNotIn("thread_count", cm.catboost_params(False))
        self.assertNotIn("thread_count", cm.catboost_params(True))
        with unittest.mock.patch.object(
                cm,
                "walk_forward_parallel",
                side_effect=AssertionError("полный путь не использует пул"),
        ):
            runs = cm.evaluate_windows(
                catboost_rows(20),
                {"stub": lambda train, row: 0.0},
                specs=[(10, 0)],
                quick=False,
                jobs=0,
            )
        self.assertEqual(len(runs[0].predictions), 10)

    def test_jobs_cli_flag_passes_jobs(self):
        with unittest.mock.patch.object(
                cm, "run_summary", return_value=0
        ) as summary:
            self.assertEqual(cm.main(["--quick", "--jobs", "8"]), 0)
        self.assertEqual(summary.call_args.kwargs["jobs"], 8)
        # `--jobs 1` и `--jobs 0` с quick — последовательный quick.
        for jobs in ("1", "0"):
            with unittest.mock.patch.object(
                    cm, "run_summary", return_value=0
            ) as summary:
                self.assertEqual(cm.main(["--quick", "--jobs", jobs]), 0)
            self.assertEqual(summary.call_args.kwargs["jobs"], int(jobs))
        # Без флага и без quick — штатный полный прогон с jobs=0.
        with unittest.mock.patch.object(
                cm, "run_summary", return_value=0
        ) as summary:
            self.assertEqual(cm.main([]), 0)
        self.assertEqual(summary.call_args.kwargs["jobs"], 0)
        # Сам флаг --jobs без quick отвергается (в том числе 0 и 1).
        with contextlib.redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                cm.main(["--jobs", "1"])

    def test_quick_ml_log_jobs_config_and_run_name(self):
        fake = FakeLogger()
        captured = {}

        def start_run(**kwargs):
            captured.update(kwargs)
            return fake

        def fake_evaluate(rows, models, specs, **kwargs):
            captured["evaluate"] = dict(kwargs)
            return fake_window_runs(specs)

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(ML_LOG="1"), clear=True
            ), unittest.mock.patch.object(
                    cm.mlx, "start_run", start_run
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", fake_evaluate
            ), unittest.mock.patch.object(
                    cm, "fixed_split",
                    side_effect=AssertionError("quick не вызывает fixed_split"),
            ), unittest.mock.patch.object(
                    cm, "current_nowcast",
                    return_value=(None, "нет цели (тест)"),
            ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=Path(tmp),
                    today=date(2026, 10, 18),
                    start=3,
                    quick=True,
                    jobs=8,
                )
        self.assertEqual(code, 0)
        self.assertTrue(captured["config"]["quick"])
        self.assertEqual(captured["config"]["jobs"], 8)
        self.assertEqual(captured["evaluate"], {"quick": True, "jobs": 8})
        self.assertEqual(
            captured["run_name"],
            "cpi_monthly__short__quick__jobs8__start3__embargo0",
        )

    def test_quick_jobs_1_ml_log_and_report_no_pool(self):
        # `jobs=1` — последовательный quick: фактическое значение в паспорте,
        # без сегмента jobs1 в run_name и без строки про пул в отчёте.
        fake = FakeLogger()
        captured = {}

        def start_run(**kwargs):
            captured.update(kwargs)
            return fake

        def fake_evaluate(rows, models, specs, **kwargs):
            captured["evaluate"] = dict(kwargs)
            return fake_window_runs(specs)

        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(ML_LOG="1"), clear=True
            ), unittest.mock.patch.object(
                    cm.mlx, "start_run", start_run
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", fake_evaluate
            ), unittest.mock.patch.object(
                    cm, "fixed_split",
                    side_effect=AssertionError("quick не вызывает fixed_split"),
            ), unittest.mock.patch.object(
                    cm, "current_nowcast",
                    return_value=(None, "нет цели (тест)"),
            ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=directory,
                    today=date(2026, 10, 18),
                    start=3,
                    quick=True,
                    jobs=1,
                )
            report = (directory / cm.REPORT_NAME).read_text(encoding="utf-8")
        self.assertEqual(code, 0)
        self.assertTrue(captured["config"]["quick"])
        self.assertEqual(captured["config"]["jobs"], 1)
        self.assertEqual(captured["evaluate"], {"quick": True, "jobs": 1})
        self.assertEqual(
            captured["run_name"],
            "cpi_monthly__short__quick__start3__embargo0",
        )
        self.assertNotIn("пул фолдов", report)
        self.assertNotIn("--jobs:", report)


class TestRunSummaryMlLog(unittest.TestCase):
    """Task 7: журнал `ml_runs`/`ml_predictions` — опт-ин и не меняет артефакты."""

    def test_ml_log_disabled_does_not_touch_ch_exec(self):
        calls = []

        def forbidden(*args, **kwargs):
            calls.append((args, kwargs))
            raise AssertionError("ch_exec вызван без ML_LOG")

        def empty_windows(rows, models, specs, **kwargs):
            return []

        # Два пути «журнал выключен»: явный ml_log=False и env без ML_LOG.
        for explicit in (False, None):
            with tempfile.TemporaryDirectory() as tmp:
                with unittest.mock.patch.dict(
                        os.environ, clean_ml_env(), clear=True
                ), unittest.mock.patch.object(
                        cm.mlx, "ch_exec", forbidden
                ), unittest.mock.patch.object(
                        cm, "evaluate_windows", empty_windows
                ), fixture_transport(), \
                        contextlib.redirect_stdout(io.StringIO()):
                    code = cm.run_summary(
                        ml_log=explicit,
                        out_dir=Path(tmp),
                        today=date(2026, 10, 18),
                    )
            self.assertEqual(code, 0)
        self.assertEqual(calls, [])

    def test_ml_log_logs_short_run(self):
        fake = FakeLogger()
        captured = {}

        def start_run(**kwargs):
            captured.update(kwargs)
            return fake

        def fake_evaluate(rows, models, specs, **kwargs):
            return fake_window_runs(specs)

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(ML_LOG="1"), clear=True
            ), unittest.mock.patch.object(
                    cm.mlx, "start_run", start_run
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", fake_evaluate
            ), unittest.mock.patch.object(
                    cm, "current_nowcast",
                    return_value=(None, "нет цели (тест)"),
            ), fixture_transport(), \
                    contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=Path(tmp),
                    today=date(2026, 10, 18),
                    start=3,
                    embargo_months=1,
                    sample="short",
                )
        self.assertEqual(code, 0)
        self.assertEqual(captured["experiment"], "cpi_monthly")
        self.assertIn("short", captured["run_name"])
        self.assertEqual(captured["config"]["sample"], "short")
        self.assertEqual(captured["config"]["start"], 3)
        self.assertEqual(captured["config"]["embargo_months"], 1)
        # Окна прогона — в паспорте, а не только в metrics.
        self.assertEqual(
            captured["config"]["windows"], [[3, 1], [48, 1]]
        )
        self.assertEqual(captured["config"]["models"], list(cm.MODELS))
        self.assertEqual(
            captured["config"]["features_base"], cm.feature_names(False)
        )
        self.assertEqual(
            captured["config"]["features_mp"], cm.feature_names(True)
        )
        self.assertEqual(captured["sample_meta"]["rows"], 15)
        self.assertEqual(captured["sample_meta"]["first"], "2013-10-01")
        self.assertEqual(captured["sample_meta"]["last"], "2014-12-01")
        self.assertEqual(captured["sample_meta"]["months"], 24)
        self.assertEqual(captured["data_asof"], date(2026, 10, 18))
        self.assertTrue(
            captured["spec_path"].endswith("cpi-monthly-mp-design.md")
        )

        self.assertEqual(fake.metrics["primary_metric"], "rmse_pp")
        self.assertEqual(fake.metrics["sample"], "short")
        self.assertEqual(fake.metrics["models"], list(cm.MODELS))
        self.assertEqual(
            [window["window"] for window in fake.metrics["windows"]],
            ["start=3, embargo=1", "start=48, embargo=1"],
        )
        self.assertEqual(
            fake.metrics["weights"]["start=3, embargo=1"][
                cm.MODEL_CATBOOST_BOTTOMUP
            ],
            dict(zip(cm.BOTTOMUP_COMPONENTS, (0.4, 0.35, 0.25))),
        )
        # На фикстурах нет test-месяцев статьи 2019–2024 — сплит пуст.
        self.assertEqual(fake.metrics["article_split"], {})

        folds = 4
        self.assertEqual(
            len(fake.predictions), len(cm.MODELS) * folds * 2
        )
        for row in fake.predictions:
            self.assertEqual(row.unit, "pp")
            self.assertEqual(row.meta["sample"], "short")
            self.assertIn(
                row.window, {"start=3, embargo=1", "start=48, embargo=1"}
            )
            self.assertIn(row.meta["start"], (3, 48))
        self.assertEqual(fake.finished[0][0], "finished")
        self.assertTrue(fake.finished[0][1].startswith("walk-forward:"))

    def test_ml_log_logs_long_run(self):
        world = synthetic_long_world()
        fake = FakeLogger()
        captured = {}

        def start_run(**kwargs):
            captured.update(kwargs)
            return fake

        def fake_evaluate(rows, models, specs, **kwargs):
            return fake_window_runs(specs)

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(ML_LOG="1"), clear=True
            ), unittest.mock.patch.object(
                    cm, "load_targets", return_value=world["targets"]
            ), unittest.mock.patch.object(
                    cm, "load_survey", return_value=world["survey"]
            ), unittest.mock.patch.object(
                    cm, "load_usd", return_value=world["usd"]
            ), unittest.mock.patch.object(
                    cm, "load_key_rate", return_value=world["key_rate"]
            ), unittest.mock.patch.object(
                    cm, "load_fao", return_value=world["fao"]
            ), unittest.mock.patch.object(
                    cm.mlx, "start_run", start_run
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", fake_evaluate
            ), unittest.mock.patch.object(
                    cm, "fixed_split", return_value=[]
            ), unittest.mock.patch.object(
                    cm, "current_nowcast",
                    return_value=(None, "нет цели (тест)"),
            ), contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=Path(tmp),
                    today=date(2026, 10, 18),
                    sample="long",
                )
        self.assertEqual(code, 0)
        self.assertEqual(captured["config"]["sample"], "long")
        self.assertEqual(captured["config"]["start"], 48)
        self.assertEqual(captured["config"]["embargo_months"], 0)
        self.assertEqual(captured["config"]["windows"], [[48, 0], [48, 1]])
        self.assertEqual(captured["sample_meta"]["rows"], 292)
        self.assertEqual(captured["sample_meta"]["first"], "2002-05-01")
        self.assertEqual(captured["sample_meta"]["last"], "2026-08-01")
        self.assertIn("long", captured["run_name"])
        self.assertIn("start48", captured["run_name"])
        # Основное окно long — 48/0, робастность — 48/1 (спека §8.2).
        self.assertEqual(
            [window["window"] for window in fake.metrics["windows"]],
            ["start=48", "start=48, embargo=1"],
        )
        self.assertEqual(fake.metrics["sample"], "long")
        self.assertEqual(
            {row.window for row in fake.predictions},
            {"start=48", "start=48, embargo=1"},
        )
        self.assertTrue(all(row.meta["sample"] == "long" for row in fake.predictions))
        self.assertEqual(fake.finished[0][0], "finished")

    def test_ml_log_finishes_empty_run(self):
        # Явный logger используется и при выключенном ML_LOG: env чистый, но
        # прогон всё равно получает статус empty.
        fake = FakeLogger()

        def empty_windows(rows, models, specs, **kwargs):
            return []

        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            for name in cm.ARTIFACT_NAMES:
                (directory / name).write_text("old", encoding="utf-8")
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(), clear=True
            ), unittest.mock.patch.object(
                    cm, "evaluate_windows", empty_windows
            ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                code = cm.run_summary(
                    out_dir=directory,
                    today=date(2026, 10, 18),
                    logger=fake,
                )
            self.assertEqual(code, 0)
            self.assertEqual(fake.finished[0][0], "empty")
            self.assertIn("ни одного прогноза", fake.finished[0][1])
            # Ветка empty по-прежнему подчищает все три stale-артефакта.
            for name in cm.ARTIFACT_NAMES:
                self.assertFalse((directory / name).exists(), name)

    def test_ml_log_failed_run_reraises(self):
        fake = FakeLogger()

        def broken_windows(rows, models, specs, **kwargs):
            raise RuntimeError("walk-forward сломан (тест)")

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(
                    cm, "evaluate_windows", broken_windows
            ), fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                with self.assertRaises(RuntimeError):
                    cm.run_summary(
                        out_dir=Path(tmp),
                        today=date(2026, 10, 18),
                        logger=fake,
                    )
        self.assertEqual(fake.finished[0][0], "failed")
        self.assertIn("RuntimeError", fake.finished[0][1])
        self.assertIn("walk-forward сломан (тест)", fake.finished[0][1])

    def test_ml_log_does_not_change_artifacts(self):

        def run_once(directory: Path, logger) -> int:
            with unittest.mock.patch.dict(
                    os.environ, clean_ml_env(), clear=True
            ), unittest.mock.patch.object(
                    cm, "ARTIFACTS_DIR", directory
            ), fixture_transport(), \
                    contextlib.redirect_stdout(io.StringIO()):
                return cm.run_summary(
                    out_dir=directory,
                    today=date(2026, 10, 18),
                    start=3,
                    embargo_months=1,
                    sample="short",
                    logger=logger,
                )

        with tempfile.TemporaryDirectory() as left_tmp, \
                tempfile.TemporaryDirectory() as right_tmp:
            left, right = Path(left_tmp), Path(right_tmp)
            self.assertEqual(run_once(left, None), 0)
            self.assertEqual(run_once(right, FakeLogger()), 0)
            self.assertTrue((right / cm.FORECAST_NAME).exists())
            for name in (cm.REPORT_NAME, cm.PREDICTIONS_NAME, cm.FORECAST_NAME):
                left_hash = hashlib.sha256((left / name).read_bytes()).hexdigest()
                right_hash = hashlib.sha256((right / name).read_bytes()).hexdigest()
                self.assertEqual(left_hash, right_hash, name)

    def test_ml_log_error_falls_back(self):
        def fake_evaluate(rows, models, specs, **kwargs):
            return fake_window_runs(specs)

        # log_predictions падает → журнал помечается failed, прогон успешен.
        # Второй вариант — сломан и сам finish: код возврата тоже не меняется.
        for fail_on in (("log_predictions",), ("log_predictions", "finish")):
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                fake = FakeLogger(fail_on=fail_on)
                with unittest.mock.patch.object(
                        cm, "ARTIFACTS_DIR", directory
                ), fixture_transport(), unittest.mock.patch.object(
                        cm, "evaluate_windows", fake_evaluate
                ), unittest.mock.patch.object(
                        cm, "current_nowcast",
                        return_value=(None, "нет цели (тест)"),
                ), contextlib.redirect_stdout(io.StringIO()):
                    code = cm.run_summary(
                        out_dir=directory,
                        today=date(2026, 10, 18),
                        start=3,
                        embargo_months=1,
                        sample="short",
                        logger=fake,
                    )
                self.assertEqual(code, 0, fail_on)
                self.assertTrue((directory / cm.REPORT_NAME).exists(), fail_on)
                self.assertTrue(
                    (directory / cm.PREDICTIONS_NAME).exists(), fail_on
                )
                self.assertEqual(
                    [status for status, _ in fake.finished], ["failed"], fail_on
                )
                self.assertIn("log_predictions", fake.finished[0][1], fail_on)


if __name__ == "__main__":
    unittest.main(verbosity=2)
