#!/usr/bin/env python3
"""Тесты scripts/cpi_monthly.py (Task 1: данные, as-of, выборка, признаки).

Запуск из корня репозитория (pytest не нужен):

    uv run --with "catboost>=1.2" --with scikit-learn --with pandas \
        --with "statsmodels>=0.15" --with scipy --python 3.12 \
        python3 scripts/cpi_monthly_test.py -v

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
import io
import math
import statistics
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date, datetime, timedelta
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_monthly as cm

FIXTURE_DIR = Path(__file__).resolve().parent / "testdata" / "cpi_monthly"

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

    def test_missing_base_feature_skips_month_with_reason(self):
        world = synthetic_world()
        world["usd"] = [point for point in world["usd"] if cm.month_of(point[0]) != date(2020, 3, 1)]
        rows, skipped = build_world(world)
        # Без марта не собирается средняя M−1 для апреля и M−2 для мая.
        self.assertNotIn(date(2020, 4, 1), [row.month for row in rows])
        self.assertNotIn(date(2020, 5, 1), [row.month for row in rows])
        self.assertEqual(
            [month for month, _ in skipped], [date(2020, 4, 1), date(2020, 5, 1)]
        )
        self.assertIn("USD", skipped[0][1])

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

    def run_summary(self, argv=None) -> tuple:
        output = io.StringIO()
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

    def test_start_and_embargo_are_accepted_and_reported(self):
        code, text = self.run_summary(["--start", "3", "--embargo-months", "1"])
        self.assertEqual(code, 0)
        self.assertTrue(summary_value(text, "--start:").startswith("3 "))
        self.assertTrue(summary_value(text, "--embargo-months:").startswith("1 "))
        # Выборка флагами не меняется: модели — Task 2.
        self.assertEqual(
            summary_value(text, "выборка:"),
            "15 месяцев (2013-10 … 2014-12), пропущено 0",
        )

    def test_loaders_go_through_transport(self):
        with fixture_transport():
            self.assertEqual(set(cm.load_targets()), set(cm.TARGET_SERIES))
            self.assertEqual(set(cm.load_survey()), set(cm.MP_SOURCE_SERIES))
            self.assertTrue(cm.load_usd())
            self.assertTrue(cm.load_key_rate())
            self.assertTrue(cm.load_fao())


if __name__ == "__main__":
    unittest.main(verbosity=2)
