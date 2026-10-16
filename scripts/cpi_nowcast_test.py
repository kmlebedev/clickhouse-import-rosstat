#!/usr/bin/env python3
"""Тесты scripts/cpi_nowcast.py.

Запуск из корня репозитория (pytest не нужен):

    uv run --with "catboost>=1.2" --with scikit-learn --with pandas \
        --with "statsmodels>=0.15" --with scipy --python 3.12 \
        python3 scripts/cpi_nowcast_test.py -v

(`--python 3.12` — конвенция репозитория для uv-прогонов; Makefile пинует тот же
интерпретатор.)

Набор ОФЛАЙН: обращения к ClickHouse подменяются `unittest.mock`, данные берутся
из фикстур `scripts/testdata/cpi_nowcast/*.csv`. Живая проверка — отдельный ручной
шаг `make cpi-nowcast`.

ФИКСТУРЫ ПЕРЕСОБИРАЮТСЯ ИЗ ЖИВОГО DEV-CLICKHOUSE (пользователь default):

    make cpi-nowcast ARGS="--export-fixtures scripts/testdata/cpi_nowcast"

Окна срезов: `weekly_2022.csv` — 2021-12-01…2022-05-31 (фактически 2022-01-10…
2022-05-27: глубже недельных данных нет); `weekly_recent.csv` — 2026-09-14…
2026-10-05 (четыре публикации к cutoff сентября-2026); остальные — 2021–2022
(2021-12 — y_lag1 первого месяца выборки). `ofz_2021_2022.csv` пуст НАМЕРЕННО
(только заголовок) и пинован пустым (тест `test_fixture_inventory`): источник
не отдаёт историю — у G-curve МосБиржи (`zcyc`) только снимок текущего дня,
окно 2021–2022 пусто, и экспорт пишет это предупреждением, а не ошибкой. Живой
прогон видит ровно то же самое (единственная точка ряда — 2026-10-09), поэтому
тесты пинуют отсутствие точки как NaN и то, что месяц НЕ уходит в пропущенные
(`test_ofz_missing_is_nan_and_month_is_kept`, `test_ofz_is_nan_for_fixture_months`),
а формулу признака — на точках в памяти (`test_ofz_available_point_is_used`,
`test_ofz_available_from_next_day`). Это исключение из правила «нет данных →
месяц пропускается» зафиксировано в спеке §2.3/§2.4.
Файлы коммитятся: тесты обязаны воспроизводиться без сети и БД.
"""

import contextlib
import csv
import io
import math
import random
import statistics
import sys
import tempfile
import unittest
import unittest.mock
import warnings
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_nowcast as cm
import eval_utils as eu

FIXTURE_DIR = Path(__file__).resolve().parent / "testdata" / "cpi_nowcast"

FIXTURE_NAMES = (
    "weekly_2022.csv",
    "weekly_recent.csv",
    "ipc_mes_2021_2022.csv",
    "usd_2021_2022.csv",
    "ofz_2021_2022.csv",
    "fao_2021_2022.csv",
)

# Фикстуры ставки нет (её нет и в списке фикстур задачи): офлайн-прогон
# `run_summary` берёт синтетический ряд — признак `key_rate` проверяется на нём
# же (правило «без лага» не зависит от значений).
KEY_RATE_ROWS = [
    {"date": "2021-01-01", "rate": 8.5},
    {"date": "2022-02-28", "rate": 20.0},
    {"date": "2022-05-04", "rate": 14.0},
]

ONE_DAY = timedelta(days=1)


def fixture_rows(name: str, directory: Path = FIXTURE_DIR) -> list:
    """Строки CSV-фикстуры словарями — в той же форме, что отдаёт ch_query."""
    with (directory / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def ch_query_from_fixtures(sql: str) -> list:
    """Транспорт-заглушка: живой SQL → строки соответствующей фикстуры.

    Прогоняет загрузчики и `run_summary` целиком офлайн — тем же путём, что и
    живой прогон, но на закоммиченных срезах.
    """
    if "FROM rosstat_ipc_mes" in sql:
        return fixture_rows("ipc_mes_2021_2022.csv")
    if "FROM rosstat_ipc_weeks" in sql:
        if "2026-09-14" in sql:
            return fixture_rows("weekly_recent.csv")
        return fixture_rows("weekly_2022.csv")
    if "FROM cbr_currency_usd" in sql:
        return fixture_rows("usd_2021_2022.csv")
    if "FROM cbr_key_rate" in sql:
        return list(KEY_RATE_ROWS)
    if "FROM ofz_curve" in sql:
        return fixture_rows("ofz_2021_2022.csv")
    if "FROM fao_food_price_Indices_monthly" in sql:
        return fixture_rows("fao_2021_2022.csv")
    raise AssertionError(f"неизвестный SQL: {sql}")


@contextlib.contextmanager
def fixture_transport():
    """Подменяет ch_query фикстурным транспортом на время блока."""
    with unittest.mock.patch.object(cm, "ch_query", ch_query_from_fixtures):
        yield


def fixture_weekly(name: str = "weekly_2022.csv") -> list:
    return cm.weekly_from_rows(fixture_rows(name))


def fixture_targets() -> dict:
    return cm.targets_from_rows(fixture_rows("ipc_mes_2021_2022.csv"))


def fixture_usd() -> list:
    return cm.usd_from_rows(fixture_rows("usd_2021_2022.csv"))


def fixture_ofz() -> list:
    return cm.ofz_from_rows(fixture_rows("ofz_2021_2022.csv"))


def fixture_fao() -> list:
    return cm.fao_from_rows(fixture_rows("fao_2021_2022.csv"))


def fixture_key_rate() -> list:
    return cm.key_rate_from_rows(KEY_RATE_ROWS)


def fixture_sample() -> tuple:
    return cm.build_sample(
        fixture_targets(),
        fixture_weekly(),
        fixture_usd(),
        fixture_key_rate(),
        fixture_ofz(),
        fixture_fao(),
    )


def summary_value(text: str, label: str) -> str:
    """Значение строки сводки по её подписи (без привязки к отступам)."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(label):
            return stripped[len(label):].strip()
    raise AssertionError(f"нет строки {label!r} в сводке:\n{text}")


# --- Синтетический мир для выборки (не зависит от фикстур) --------------------


def synthetic_targets(months: list = None) -> dict:
    """Таргет 4 рядов: y cpi_all — 0.5/0.6/0.7/..., компоненты — рядом."""
    months = months or [date(2021, 12, 1), date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)]
    targets = {}
    for index, code in enumerate(cm.TARGET_SERIES):
        targets[code] = [
            (cm.month_end(month), 0.5 + 0.1 * position + 0.01 * index)
            for position, month in enumerate(months)
        ]
    return targets


def synthetic_weekly(days: list) -> list:
    """Публикации по дням: три позиции, одна из них — ряд бензина."""
    points = []
    for position, day in enumerate(days):
        points.append(("Товар A", day, 100.0 + position))
        points.append(("Товар B", day, 99.0 + position))
        points.append((cm.WEEKLY_GASOLINE, day, 100.0 + position / 2))
    return points


def month_days(months: list) -> list:
    """Четыре еженедельные регистрации в каждом месяце (5/12/19/26-е)."""
    days = []
    for month in months:
        days += [date(month.year, month.month, day) for day in (5, 12, 19, 26)]
    return days


def synthetic_usd() -> list:
    """Дневной курс: заведомо больше 21 наблюдения к любому cutoff мира."""
    start = date(2021, 11, 1)
    return [(start + ONE_DAY * i, 70.0 + i * 0.01) for i in range(0, 181)]


def synthetic_fao() -> list:
    """Месячные точки FAO: 1-е число месяца публикации, значение за M−1."""
    return [
        (date(2021, 11, 1), 110.0),
        (date(2021, 12, 1), 111.0),
        (date(2022, 1, 1), 112.0),
        (date(2022, 2, 1), 113.0),
        (date(2022, 3, 1), 114.0),
        (date(2022, 4, 1), 115.0),
    ]


def synthetic_world(months: list = None, **overrides) -> dict:
    months = months or [date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)]
    world = {
        # Таргет длиннее выборки на месяц: предыдущий месяц нужен как y_lag1
        # первого месяца (для января-2022 — декабрь-2021).
        "targets": synthetic_targets([cm.previous_month(months[0])] + months),
        "weekly": synthetic_weekly(month_days(months)),
        "usd": synthetic_usd(),
        "key_rate": [(date(2021, 12, 1), 8.0), (date(2022, 2, 15), 20.0)],
        "ofz": [],
        "fao": synthetic_fao(),
    }
    world.update(overrides)
    return world


def build_world(world: dict) -> tuple:
    return cm.build_sample(
        world["targets"], world["weekly"], world["usd"], world["key_rate"],
        world["ofz"], world["fao"],
    )


class TestConstants(unittest.TestCase):
    """Константы-контракт с Tasks 3–6 (спека §2)."""

    def test_seed_is_pinned(self):
        self.assertEqual(cm.SEED, 20261016)

    def test_target_series_are_pinned_verbatim(self):
        # Дословные имена рядов rosstat_ipc_mes: расхождение (переименование
        # карточки в импортёре) молча обнулило бы выборку.
        self.assertEqual(cm.TARGET_SERIES, {
            "cpi_all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
            "cpi_food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
            "cpi_nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
            "cpi_services": "Индексы потребительских цен на услуги по Российской Федерации",
        })
        self.assertEqual(cm.TARGET_ALL, "cpi_all")

    def test_weekly_and_fao_series_are_pinned(self):
        self.assertEqual(cm.WEEKLY_GASOLINE, "Бензин автомобильный")
        self.assertEqual(cm.FAO_SERIES, "Food Price Index")
        self.assertEqual(cm.OFZ_TENOR, "1y")

    def test_feature_names_order(self):
        # Порядок §2.3 — контракт матрицы признаков и monotone-ограничений.
        self.assertEqual(cm.feature_names(), [
            "share_rising_wk", "median_wk", "median_wk4", "gasoline_4w", "usd_20d",
            "key_rate", "ofz_1y", "fao_mm", "y_lag1", "month",
        ])

    def test_degenerate_window_error_is_value_error(self):
        # Task 3 будет ловить его отдельной веткой walk-forward.
        self.assertTrue(issubclass(cm.DegenerateWindowError, ValueError))

    def test_walk_forward_window_defaults(self):
        # Спека §4: старт 24 — основной, 48 и embargo=1 — робастность.
        self.assertEqual((cm.MAIN_START, cm.MAIN_EMBARGO), (24, 0))
        self.assertEqual((cm.ROBUST_START, cm.ROBUST_EMBARGO), (48, 1))

    def test_model_names_and_labels_are_pinned(self):
        self.assertEqual(
            (cm.MODEL_LAST, cm.MODEL_AR1, cm.MODEL_ENET),
            ("b1_last_value", "b2_ar1_seasonal", "b3_elastic_net"),
        )
        self.assertEqual(list(cm.BENCHMARKS), [cm.MODEL_LAST, cm.MODEL_AR1, cm.MODEL_ENET])
        self.assertIn("B1", cm.model_label(cm.MODEL_LAST))
        self.assertIn("ElasticNetCV", cm.model_label(cm.MODEL_ENET))
        # Незарегистрированное имя (модели Task 4) не подменяется меткой бенчмарка.
        self.assertEqual(cm.model_label("catboost_direct"), "catboost_direct")

    def test_significance_benchmark_and_artifact_constants(self):
        # Спека §4: блок 3 месяца, n=2000, seed=SEED; §5 — точные имена артефактов.
        self.assertEqual((cm.BOOTSTRAP_BLOCK, cm.BOOTSTRAP_N), (3, 2000))
        self.assertEqual((cm.LAST_FOLDS, cm.ENET_MIN_TRAIN), (12, 12))
        self.assertEqual(cm.HAC_MAXLAGS, 3)
        self.assertEqual(len(cm.AR1_DUMMY_MONTHS), 11)
        self.assertEqual(cm.REPORT_NAME, "cpi_nowcast_report.md")
        self.assertEqual(cm.PREDICTIONS_NAME, "cpi_nowcast_predictions.csv")

    def test_enet_configuration_matches_the_spec(self):
        # Спека §3.3, дословно; конфигурация, подставленная в модель, пинуется
        # ещё и по атрибутам подобранного пайплайна (TestBenchmarkEnet).
        np.testing.assert_allclose(cm.ENET_ALPHAS, np.logspace(-4, 1, 20))
        self.assertEqual(list(cm.ENET_L1_RATIOS), [0.1, 0.5, 0.9])
        self.assertEqual(cm.ENET_CV_SPLITS, 3)
        self.assertEqual(cm.ENET_MIN_TRAIN, 12)
        self.assertNotIn("month", cm.ENET_FEATURES)
        self.assertEqual(len(cm.ENET_FEATURES), 9)


class TestMonthArithmetic(unittest.TestCase):
    """cutoff и календарная арифметика месяцев (спека §2.2)."""

    def test_month_end(self):
        self.assertEqual(cm.month_end(date(2022, 1, 15)), date(2022, 1, 31))
        self.assertEqual(cm.month_end(date(2022, 2, 1)), date(2022, 2, 28))
        self.assertEqual(cm.month_end(date(2020, 2, 10)), date(2020, 2, 29))
        self.assertEqual(cm.month_end(date(2022, 12, 31)), date(2022, 12, 31))

    def test_previous_month(self):
        self.assertEqual(cm.previous_month(date(2022, 1, 1)), date(2021, 12, 1))
        self.assertEqual(cm.previous_month(date(2022, 3, 31)), date(2022, 2, 1))

    def test_cutoff_is_eighth_of_next_month(self):
        self.assertEqual(cm.cutoff_for(date(2022, 1, 1)), date(2022, 2, 8))
        self.assertEqual(cm.cutoff_for(date(2022, 12, 1)), date(2023, 1, 8))
        self.assertEqual(cm.cutoff_for(date(2026, 9, 1)), date(2026, 10, 8))
        # День внутри месяца значения не имеет: месяц — первое число, но день
        # входа не должен менять окно.
        self.assertEqual(cm.cutoff_for(date(2022, 1, 31)), date(2022, 2, 8))

    def test_available_from_mes_is_end_of_next_month(self):
        self.assertEqual(cm.available_from_mes(date(2021, 12, 31)), date(2022, 1, 31))
        self.assertEqual(cm.available_from_mes(date(2022, 1, 31)), date(2022, 2, 28))
        self.assertEqual(cm.available_from_mes(date(2022, 11, 30)), date(2022, 12, 31))
        self.assertEqual(cm.available_from_mes(date(2022, 12, 31)), date(2023, 1, 31))


class TestAvailabilityRules(unittest.TestCase):
    """Правила §2.4 и их границы."""

    def test_rules_are_pinned(self):
        day = date(2022, 3, 10)
        self.assertEqual(cm.available_from_weekly(day), date(2022, 3, 12))
        self.assertEqual(cm.available_from_usd(day), date(2022, 3, 11))
        self.assertEqual(cm.available_from_rate(day), day)
        self.assertEqual(cm.available_from_ofz(day), date(2022, 3, 11))
        self.assertEqual(cm.available_from_fao(day), date(2022, 3, 16))

    def test_weekly_point_participates_exactly_on_publication_day(self):
        points = [("Товар A", date(2022, 3, 4), 100.0)]
        self.assertEqual(
            [day for day, _ in cm.eligible_weeks(points, date(2022, 3, 6))],
            [date(2022, 3, 4)],
        )
        # На день раньше публикации точка ещё не участвует.
        self.assertEqual(cm.eligible_weeks(points, date(2022, 3, 5)), [])

    def test_weekly_point_published_after_cutoff_does_not_participate(self):
        points = [
            ("Товар A", date(2022, 3, 4), 100.0),
            ("Товар A", date(2022, 3, 11), 150.0),  # доступна 03-13 > cutoff
        ]
        weeks = cm.eligible_weeks(points, date(2022, 3, 12))
        self.assertEqual([day for day, _ in weeks], [date(2022, 3, 4)])

    def test_asof_takes_latest_available_and_ignores_order(self):
        points = [
            (date(2022, 5, 10), 10.0),
            (date(2022, 5, 1), 1.0),
            (date(2022, 5, 20), 20.0),
        ]
        self.assertEqual(cm.asof(points, cm.available_from_usd, date(2022, 5, 12)), 10.0)
        self.assertIsNone(cm.asof(points, cm.available_from_usd, date(2022, 5, 1)))


class TestWeeklyFeatures(unittest.TestCase):
    """Недельные признаки §2.3 пп. 1–4 — руками на маленьком срезе."""

    # Четыре недели, три позиции (A, B, бензин). «Рукой»: изменения по неделям
    # [0, 0, 0]; [+2, −2, +0.5]; [+4, 0, +1]; [+6, −1, +2.5] → медианы недель
    # [0, 0.5, 1, 2.5], последняя неделя: растущих 2 из 3 (A и бензин), медиана
    # [6, −1, 2.5] = 2.5, газ: 0 + 0.5 + 1 + 2.5.
    SLICE = {
        date(2022, 3, 4): (100.0, 100.0, 100.0),
        date(2022, 3, 11): (102.0, 98.0, 100.5),
        date(2022, 3, 18): (104.0, 100.0, 101.0),
        date(2022, 3, 25): (106.0, 99.0, 102.5),
    }
    FIVE_WEEKS = {**SLICE, date(2022, 4, 1): (100.0, 100.0, 99.0)}

    def slice_points(self, days: dict) -> list:
        points = []
        for day, (a, b, gasoline) in sorted(days.items()):
            points.append(("Товар A", day, a))
            points.append(("Товар B", day, b))
            points.append((cm.WEEKLY_GASOLINE, day, gasoline))
        return points

    def test_share_median_and_gasoline_are_hand_computed(self):
        features = cm.weekly_features(self.slice_points(self.SLICE), date(2022, 3, 27))
        self.assertAlmostEqual(features["share_rising_wk"], 2.0 / 3.0)
        self.assertAlmostEqual(features["median_wk"], 2.5)
        self.assertAlmostEqual(features["median_wk4"], (0.0 + 0.5 + 1.0 + 2.5) / 4)
        self.assertAlmostEqual(features["gasoline_4w"], 0.0 + 0.5 + 1.0 + 2.5)

    def test_window_is_exactly_the_last_four_weeks(self):
        # Пятая публикация доступна к новому cutoff — в окно входит ОНА, а не
        # первая из четырёх: последняя неделя меняется ([0, 0, −1] → ноль
        # растущих, медиана 0), медианы окна [0.5, 1, 2.5, 0], газ 3.0.
        features = cm.weekly_features(self.slice_points(self.FIVE_WEEKS), date(2022, 4, 3))
        self.assertAlmostEqual(features["share_rising_wk"], 0.0)
        self.assertAlmostEqual(features["median_wk"], 0.0)
        self.assertAlmostEqual(features["median_wk4"], (0.5 + 1.0 + 2.5 + 0.0) / 4)
        self.assertAlmostEqual(features["gasoline_4w"], 0.5 + 1.0 + 2.5 - 1.0)

    def test_incomplete_window_returns_none(self):
        short = {day: self.SLICE[day] for day in list(self.SLICE)[:3]}
        self.assertIsNone(cm.weekly_features(self.slice_points(short), date(2022, 3, 27)))

    def test_missing_gasoline_in_window_returns_none(self):
        points = [
            point for point in self.slice_points(self.SLICE) if not (
                point[0] == cm.WEEKLY_GASOLINE and point[1] == date(2022, 3, 18)
            )
        ]
        self.assertIsNone(cm.weekly_features(points, date(2022, 3, 27)))

    def test_mutant_point_after_cutoff_does_not_change_features(self):
        base = cm.weekly_features(self.slice_points(self.SLICE), date(2022, 3, 27))
        # Точка следующей недели опубликована ПОЗЖЕ cutoff: как бы ни было
        # дико её значение, она не участвует.
        leaked = self.slice_points(self.SLICE)
        leaked += [("Товар A", date(2022, 4, 1), 999.0), ("Товар B", date(2022, 4, 1), 1.0),
                   (cm.WEEKLY_GASOLINE, date(2022, 4, 1), 500.0)]
        self.assertEqual(cm.weekly_features(leaked, date(2022, 3, 27)), base)

    def test_mutant_point_shifted_into_window_changes_features(self):
        # Та же неделя, сдвинутая внутрь окна (01-18 → 03-18... нет: 03-18 уже
        # есть; берём 03-14 — между публикациями): состав окна меняется, и
        # признаки это чувствуют.
        base = cm.weekly_features(self.slice_points(self.SLICE), date(2022, 3, 27))
        shifted = self.slice_points(self.SLICE)
        shifted += [("Товар A", date(2022, 3, 14), 200.0), ("Товар B", date(2022, 3, 14), 50.0),
                    (cm.WEEKLY_GASOLINE, date(2022, 3, 14), 50.0)]
        moved = cm.weekly_features(shifted, date(2022, 3, 27))
        self.assertNotEqual(moved["median_wk4"], base["median_wk4"])
        self.assertNotEqual(moved["gasoline_4w"], base["gasoline_4w"])


class TestDriverFeatures(unittest.TestCase):
    """Признаки-«драйверы» §2.3 пп. 5–8 — формула и границы доступности."""

    def test_usd_20d_formula_and_depth(self):
        points = [(date(2022, 1, 3) + ONE_DAY * i, 50.0 + i) for i in range(21)]
        cutoff = points[-1][0] + ONE_DAY  # последняя точка доступна
        self.assertAlmostEqual(
            cm.usd_features(points, cutoff), 100.0 * math.log(70.0 / 50.0), places=9
        )
        # 20 наблюдений — не хватает одного шага: признак не собирается.
        self.assertIsNone(cm.usd_features(points[:-1], cutoff))

    def test_usd_20d_ignores_points_after_cutoff(self):
        points = [(date(2022, 1, 3) + ONE_DAY * i, 50.0 + i) for i in range(21)]
        cutoff = points[-1][0] + ONE_DAY
        leaked = points + [(points[-1][0] + 5 * ONE_DAY, 999.0)]
        self.assertAlmostEqual(
            cm.usd_features(leaked, cutoff), 100.0 * math.log(70.0 / 50.0), places=9
        )

    def test_usd_last_point_exactly_at_cutoff_is_not_available(self):
        points = [(date(2022, 1, 3) + ONE_DAY * i, 50.0 + i) for i in range(21)]
        # date + 1 > cutoff: последняя точка ещё не опубликована — глубины нет.
        self.assertIsNone(cm.usd_features(points, points[-1][0]))

    def test_rate_has_no_lag(self):
        points = [(date(2022, 2, 14), 9.5), (date(2022, 2, 28), 20.0)]
        self.assertEqual(cm.rate_feature(points, date(2022, 2, 28)), 20.0)
        self.assertEqual(cm.rate_feature(points, date(2022, 2, 27)), 9.5)
        self.assertIsNone(cm.rate_feature([], date(2022, 2, 28)))

    def test_ofz_available_from_next_day(self):
        points = [(date(2026, 10, 9), 13.7592)]
        self.assertIsNone(cm.ofz_feature(points, date(2026, 10, 9)))
        self.assertEqual(cm.ofz_feature(points, date(2026, 10, 10)), 13.7592)

    def test_ofz_without_history_is_none_not_future_point(self):
        # Живой срез: единственная точка G-curve (2026-10-09) НЕ должна попасть
        # в признаки месяцев бэктеста — для них доступной точки нет вовсе.
        points = [(date(2026, 10, 9), 13.7592)]
        self.assertIsNone(cm.ofz_feature(points, cm.cutoff_for(date(2022, 1, 1))))
        self.assertIsNone(cm.ofz_feature(points, cm.cutoff_for(date(2026, 9, 1))))

    def test_fao_publication_date_semantics(self):
        # Значение под датой 1-го числа месяца публикации — индекс за предыдущий
        # месяц. Для февраля-2022 (cutoff 2022-03-08) последняя доступная точка —
        # 2022-03-01 (значение за февраль), м/м — к 2022-02-01 (значение за январь).
        points = [(date(2022, 2, 1), 120.0), (date(2022, 3, 1), 126.0)]
        self.assertAlmostEqual(
            cm.fao_features(points, cm.cutoff_for(date(2022, 2, 1))),
            (126.0 / 120.0 - 1.0) * 100.0,
            places=9,
        )

    def test_fao_boundary_and_leak(self):
        points = [
            (date(2022, 1, 1), 118.0),
            (date(2022, 2, 1), 120.0),
            (date(2022, 3, 1), 126.0),
            (date(2022, 4, 1), 999.0),  # публикация апреля — доступна 04-07
        ]
        # 03-06: точка 03-01 (доступна 03-07) ещё не участвует; пара —
        # 01-01/02-01.
        self.assertAlmostEqual(
            cm.fao_features(points, date(2022, 3, 6)), (120.0 / 118.0 - 1.0) * 100.0,
            places=9,
        )
        # 03-07: доступна ровно в день публикации (+6) — участвует.
        self.assertAlmostEqual(
            cm.fao_features(points, date(2022, 3, 7)), (126.0 / 120.0 - 1.0) * 100.0,
            places=9,
        )
        # Будущая точка 04-01 не попадает в признаки 03-08, хотя её значение дико.
        self.assertAlmostEqual(
            cm.fao_features(points, cm.cutoff_for(date(2022, 2, 1))),
            (126.0 / 120.0 - 1.0) * 100.0,
            places=9,
        )

    def test_fao_needs_two_points(self):
        self.assertIsNone(cm.fao_features([(date(2022, 2, 1), 120.0)], date(2022, 3, 8)))


class TestBuildSampleSynthetic(unittest.TestCase):
    """Выборка: месяцы, пропуски, y_lag1, NaN у ofz (синтетика)."""

    def test_sample_months_and_skips(self):
        rows, skipped = build_world(synthetic_world())
        self.assertEqual([row.month for row in rows], [
            date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1),
        ])
        self.assertEqual(skipped, [])
        # Месяцы до первой недельной публикации в выборку не входят вовсе:
        # таргет 2021-12 есть, но признаков для него нет.
        self.assertNotIn(date(2021, 12, 1), [row.month for row in rows])

    def test_y_is_percent_minus_100_and_lag_is_previous_month(self):
        rows, _ = build_world(synthetic_world())
        # synthetic_targets: 2021-12 → 0.5, 2022-01 → 0.6, 2022-02 → 0.7.
        january, february = rows[0], rows[1]
        self.assertAlmostEqual(january.y["cpi_all"], 0.6, places=9)
        self.assertAlmostEqual(february.y["cpi_all"], 0.7, places=9)
        self.assertAlmostEqual(february.features["y_lag1"], january.y["cpi_all"], places=9)
        self.assertAlmostEqual(february.features["month"], 2)

    def test_own_label_does_not_leak_into_lag(self):
        world = synthetic_world()
        # Правка метки самого тестового месяца не должна менять y_lag1: лаг
        # берётся из M−1.
        february = [row for row in world["targets"]["cpi_all"] if row[0] == cm.month_end(date(2022, 2, 1))]
        self.assertEqual(len(february), 1)
        world["targets"]["cpi_all"] = [
            (day, 99.0) if day == february[0][0] else (day, value)
            for day, value in world["targets"]["cpi_all"]
        ]
        rows, _ = build_world(world)
        self.assertAlmostEqual(rows[1].features["y_lag1"], rows[0].y["cpi_all"], places=9)

    def test_features_match_feature_names(self):
        rows, _ = build_world(synthetic_world())
        for row in rows:
            self.assertEqual(sorted(row.features), sorted(cm.feature_names()))

    def test_ofz_missing_is_nan_and_month_is_kept(self):
        # Решение контроллера (спека §2.3/§2.4): отсутствие точки ofz_1y ≤ cutoff —
        # ЕДИНСТВЕННОЕ исключение из «нет данных → месяц пропускается». Источник
        # (G-curve, zcyc) хранит только снимок, истории нет; месяц остаётся,
        # признак честно помечен NaN.
        rows, skipped = build_world(synthetic_world(ofz=[]))
        self.assertEqual(skipped, [])
        self.assertEqual([row.month for row in rows], [
            date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1),
        ])
        for row in rows:
            self.assertTrue(math.isnan(row.features["ofz_1y"]))

    def test_ofz_available_point_is_used(self):
        # Обратная сторона политики: есть точка ≤ cutoff — берётся последняя
        # доступная, месяцы не пропускаются.
        world = synthetic_world(
            ofz=[(date(2021, 12, 15), 8.0), (date(2022, 1, 20), 9.0), (date(2022, 2, 20), 10.0)]
        )
        rows, skipped = build_world(world)
        self.assertEqual(skipped, [])
        self.assertAlmostEqual(rows[0].features["ofz_1y"], 9.0, places=9)
        self.assertAlmostEqual(rows[1].features["ofz_1y"], 10.0, places=9)
        # Для марта доступна та же последняя точка (новых не появилось).
        self.assertAlmostEqual(rows[2].features["ofz_1y"], 10.0, places=9)

    def test_missing_fao_puts_months_into_skipped(self):
        rows, skipped = build_world(synthetic_world(fao=[(date(2022, 1, 1), 112.0)]))
        self.assertEqual(skipped, [date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)])

    def test_missing_usd_depth_puts_months_into_skipped(self):
        rows, skipped = build_world(synthetic_world(usd=synthetic_usd()[:15]))
        self.assertEqual(rows, [])
        self.assertEqual(skipped, [date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)])

    def test_missing_rate_puts_months_into_skipped(self):
        rows, skipped = build_world(synthetic_world(key_rate=[]))
        self.assertEqual(rows, [])
        self.assertEqual(skipped, [date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1)])

    def test_stale_weekly_window_puts_months_into_skipped(self):
        # Недельные публикации есть в январе и в апреле; для февраля-марта
        # последняя доступная неделя — январская, и окно не описывает месяц:
        # месяцы пропускаются, а не считаются по устаревшим неделям.
        days = [date(2022, 1, day) for day in (5, 12, 19, 26)]
        days += [date(2022, 4, day) for day in (5, 12, 19, 26)]
        _, skipped = build_world(synthetic_world(weekly=synthetic_weekly(days)))
        self.assertEqual(skipped, [date(2022, 2, 1), date(2022, 3, 1)])

    def test_missing_lag_puts_the_first_month_into_skipped(self):
        world = synthetic_world()
        world["targets"] = {
            code: points[1:] for code, points in world["targets"].items()
        }
        rows, skipped = build_world(world)
        self.assertEqual(skipped, [date(2022, 1, 1)])
        self.assertEqual([row.month for row in rows], [date(2022, 2, 1), date(2022, 3, 1)])


class TestRowParsing(unittest.TestCase):
    """Разбор строк и отказы на противоречивых данных (fail-fast)."""

    def test_row_date_handles_iso_datetime_and_date(self):
        self.assertEqual(cm.row_date("2022-01-05"), date(2022, 1, 5))
        self.assertEqual(cm.row_date("2022-01-05T00:00:00"), date(2022, 1, 5))
        # datetime — подкласс date; без явной ветки время протащилось бы в ключ.
        self.assertEqual(cm.row_date(datetime(2022, 1, 5, 12, 30)), date(2022, 1, 5))
        self.assertEqual(cm.row_date(date(2022, 1, 5)), date(2022, 1, 5))

    def test_row_float_rejects_empty_cell(self):
        self.assertEqual(cm.row_float("100.5"), 100.5)
        with self.assertRaises(ValueError):
            cm.row_float("")
        with self.assertRaises(ValueError):
            cm.row_float(None)

    def test_weekly_duplicate_position_on_same_date_fails(self):
        rows = [
            {"name": "Товар A", "date": "2022-01-10", "percent": "100.1"},
            {"name": "Товар A", "date": "2022-01-10", "percent": "100.2"},
        ]
        with self.assertRaises(ValueError):
            cm.weekly_from_rows(rows)

    def test_targets_require_all_four_series(self):
        rows = [
            {"name": name, "date": "2022-01-31", "percent": "100.5"}
            for code, name in cm.TARGET_SERIES.items() if code != "cpi_services"
        ]
        with self.assertRaises(ValueError):
            cm.targets_from_rows(rows)

    def test_row_series_filters_ignore_foreign_rows(self):
        fao = cm.fao_from_rows([
            {"name": cm.FAO_SERIES, "date": "2022-02-01", "price_index": "120.0"},
            {"name": "Cereals", "date": "2022-02-01", "price_index": "999.0"},
        ])
        self.assertEqual(fao, [(date(2022, 2, 1), 120.0)])
        ofz = cm.ofz_from_rows([
            {"date": "2026-10-09", "tenor": "1y", "yield": "13.7592"},
            {"date": "2026-10-09", "tenor": "5y", "yield": "16.568"},
        ])
        self.assertEqual(ofz, [(date(2026, 10, 9), 13.7592)])


class TestFixturesSample(unittest.TestCase):
    """Выборка на закоммиченных фикстурах (тот же путь, что у живого прогона)."""

    def test_fixture_inventory(self):
        self.assertEqual(fixture_rows("weekly_2022.csv")[0]["date"], "2022-01-10")
        self.assertEqual(fixture_rows("weekly_2022.csv")[-1]["date"], "2022-05-27")
        self.assertEqual(fixture_rows("weekly_recent.csv")[0]["date"], "2026-09-14")
        self.assertEqual(fixture_rows("weekly_recent.csv")[-1]["date"], "2026-10-05")
        self.assertEqual(len(fixture_rows("ipc_mes_2021_2022.csv")), 96)
        self.assertEqual(len(fixture_rows("fao_2021_2022.csv")), 24)
        # ОФЗ: у G-curve нет истории — окно 2021–2022 пусто, файл с заголовком.
        self.assertEqual(fixture_rows("ofz_2021_2022.csv"), [])

    def test_sample_months_and_no_skips(self):
        rows, skipped = fixture_sample()
        self.assertEqual([row.month for row in rows], [
            date(2022, 1, 1), date(2022, 2, 1), date(2022, 3, 1),
            date(2022, 4, 1), date(2022, 5, 1),
        ])
        self.assertEqual(skipped, [])
        # Июнь-декабрь 2022 в выборке нет вовсе: недельные данные фикстуры
        # кончаются на 2022-05-27 (горизонт, а не «пропуск»).
        self.assertEqual(rows[-1].month, date(2022, 5, 1))

    def test_y_units_are_percent_minus_100(self):
        rows, _ = fixture_sample()
        january = rows[0]
        self.assertAlmostEqual(january.y["cpi_all"], 0.99, places=6)
        self.assertAlmostEqual(january.y["cpi_food"], 1.44, places=6)
        self.assertAlmostEqual(january.y["cpi_nonfood"], 0.67, places=6)
        self.assertAlmostEqual(january.y["cpi_services"], 0.76, places=6)

    def test_y_lag1_is_previous_month_value(self):
        rows, _ = fixture_sample()
        # y_lag1 февраля — метка января (и это именно M−1, а не текущий месяц).
        self.assertAlmostEqual(rows[1].features["y_lag1"], 0.99, places=6)
        self.assertAlmostEqual(rows[1].features["y_lag1"], rows[0].y["cpi_all"], places=6)

    def test_weekly_window_is_selected_by_cutoff(self):
        # Ручная сверка выбора окна: cutoff(2022-01) = 2022-02-08, доступны
        # публикации ≤ 2022-02-06 → последние четыре: 01-14, 01-21, 01-28, 02-04.
        rows, _ = fixture_sample()
        january = rows[0]
        weekly = fixture_weekly()
        eligible = sorted({day for _, day, _ in weekly if cm.available_from_weekly(day) <= cm.cutoff_for(date(2022, 1, 1))})
        self.assertEqual(eligible, [
            date(2022, 1, 10), date(2022, 1, 14), date(2022, 1, 21),
            date(2022, 1, 28), date(2022, 2, 4),
        ])
        window = eligible[-cm.WEEKLY_WINDOW_WEEKS:]
        last_week = [float(row["percent"]) - 100.0 for row in fixture_rows("weekly_2022.csv") if row["date"] == "2022-02-04"]
        self.assertAlmostEqual(january.features["median_wk"], statistics.median(last_week), places=6)
        self.assertAlmostEqual(
            january.features["share_rising_wk"],
            sum(1 for change in last_week if change > 0) / len(last_week),
            places=9,
        )
        medians = []
        for day in window:
            percents = [float(row["percent"]) for row in fixture_rows("weekly_2022.csv") if row["date"] == day.isoformat()]
            medians.append(statistics.median([percent - 100.0 for percent in percents]))
        self.assertAlmostEqual(january.features["median_wk4"], sum(medians) / 4, places=6)
        gasoline = [
            float(row["percent"]) - 100.0
            for row in fixture_rows("weekly_2022.csv")
            if row["date"] in {day.isoformat() for day in window} and row["name"] == cm.WEEKLY_GASOLINE
        ]
        self.assertEqual(len(gasoline), 4)
        self.assertAlmostEqual(january.features["gasoline_4w"], sum(gasoline), places=6)

    def test_ofz_is_nan_for_fixture_months(self):
        # Фикстура ОФЗ пуста — как и живой срез для бэктеста; месяцы не выброшены.
        rows, _ = fixture_sample()
        for row in rows:
            self.assertTrue(math.isnan(row.features["ofz_1y"]))

    def test_fixture_mutant_week_after_cutoff_does_not_change_sample(self):
        base_rows, _ = fixture_sample()
        leaked = fixture_weekly() + [
            ("Товар-мутант", date(2022, 3, 11), 999.0),
            (cm.WEEKLY_GASOLINE, date(2022, 3, 11), 500.0),
        ]
        rows, _ = cm.build_sample(
            fixture_targets(), leaked, fixture_usd(), fixture_key_rate(), fixture_ofz(), fixture_fao()
        )
        self.assertEqual(
            [row.features["median_wk"] for row in rows],
            [row.features["median_wk"] for row in base_rows],
        )

    def test_fixture_mutant_week_shifted_into_window_changes_sample(self):
        base_rows, _ = fixture_sample()
        shifted = fixture_weekly() + [
            ("Товар-мутант", date(2022, 1, 18), 999.0),
            (cm.WEEKLY_GASOLINE, date(2022, 1, 18), 500.0),
        ]
        rows, _ = cm.build_sample(
            fixture_targets(), shifted, fixture_usd(), fixture_key_rate(), fixture_ofz(), fixture_fao()
        )
        self.assertNotEqual(
            rows[0].features["median_wk4"], base_rows[0].features["median_wk4"]
        )

    def test_recent_weeks_gasoline_sum(self):
        # Четыре публикации свежего среза: сумма (percent − 100) по позиции
        # «Бензин автомобильный» = 0.36 + 0.05 + 0.00 + 0.29.
        features = cm.weekly_features(fixture_weekly("weekly_recent.csv"), date(2026, 10, 8))
        self.assertAlmostEqual(features["gasoline_4w"], 0.36 + 0.05 + 0.0 + 0.29, places=9)
        # К cutoff 2026-10-04 доступны только три публикации — окно неполно.
        self.assertIsNone(cm.weekly_features(fixture_weekly("weekly_recent.csv"), date(2026, 10, 4)))


class TestPurge(unittest.TestCase):
    """Структурная purge-проверка (спека §2.4, Review Focus 2)."""

    def test_fixture_sample_is_purged(self):
        rows, _ = fixture_sample()
        cm.assert_months_purged(rows)

    def test_synthetic_world_is_purged(self):
        rows, _ = build_world(synthetic_world())
        cm.assert_months_purged(rows)

    def test_duplicate_month_fails(self):
        rows, _ = fixture_sample()
        with self.assertRaises(AssertionError):
            cm.assert_months_purged(list(rows) + [rows[0]])

    def test_mutated_cutoff_is_caught(self):
        # Мутант: cutoff сдвинут на публикацию метки — признаки месяца читались
        # бы вместе с его собственной меткой. Проверка обязана это поймать.
        rows, _ = fixture_sample()
        with unittest.mock.patch.object(
            cm, "cutoff_for", lambda month: cm.available_from_mes(cm.month_end(month))
        ):
            with self.assertRaises(AssertionError):
                cm.assert_months_purged(rows)


class TestExportFixtures(unittest.TestCase):
    """Экспорт фикстур: имена, колонки, LF, детерминированность."""

    def export_to(self, directory: Path) -> int:
        with fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
            return cm.export_fixtures(directory)

    def test_writes_all_six_files_with_lf(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertEqual(self.export_to(directory), 0)
            self.assertEqual(sorted(path.name for path in directory.iterdir()), sorted(FIXTURE_NAMES))
            for name, columns in cm.FIXTURE_COLUMNS.items():
                raw = (directory / name).read_bytes()
                self.assertNotIn(b"\r", raw, f"{name}: CRLF в фикстуре")
                rows = fixture_rows(name, directory)
                if name == "ofz_2021_2022.csv":
                    self.assertEqual(rows, [])
                    continue
                self.assertTrue(rows, f"{name}: пустая фикстура")
                self.assertEqual(tuple(rows[0].keys()), columns)

    def test_export_is_deterministic(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            with fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                cm.export_fixtures(Path(first))
                cm.export_fixtures(Path(second))
            for name in FIXTURE_NAMES:
                self.assertEqual(
                    (Path(first) / name).read_bytes(),
                    (Path(second) / name).read_bytes(),
                    f"{name}: повторный экспорт дал другой файл",
                )

    def test_fixture_sql_matches_live_columns(self):
        # Срез — тот же запрос, только с окном: колонки и FINAL обязаны совпасть.
        for sql in (cm.TARGETS_SQL, cm.WEEKLY_SQL, cm.USD_SQL, cm.KEY_RATE_SQL, cm.OFZ_SQL, cm.FAO_SQL):
            self.assertIn("FINAL", sql)
        windowed = cm._window_sql(cm.WEEKLY_SQL, date(2021, 12, 1), date(2022, 5, 31))
        self.assertIn("FINAL", windowed)
        self.assertIn("date >= '2021-12-01'", windowed)
        self.assertIn("date <= '2022-05-31'", windowed)

    def test_main_export_fixtures(self):
        with tempfile.TemporaryDirectory() as tmp:
            with fixture_transport(), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(cm.main(["--export-fixtures", tmp]), 0)
            self.assertTrue((Path(tmp) / "weekly_2022.csv").exists())


class TestRunSummaryOffline(unittest.TestCase):
    """Офлайн-прогон сводки на фикстурах — тот же путь, что живой `make`."""

    def run_summary(self, argv=None) -> tuple:
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            with (
                fixture_transport(),
                unittest.mock.patch.object(cm, "ARTIFACTS_DIR", Path(tmp)),
                contextlib.redirect_stdout(output),
            ):
                code = cm.main(argv or [])
        return code, output.getvalue()

    def test_summary_reports_fixture_sample(self):
        code, text = self.run_summary()
        self.assertEqual(code, 0)
        self.assertEqual(
            summary_value(text, "выборка:"), "5 месяцев (2022-01 … 2022-05), пропущено 0"
        )
        self.assertEqual(
            summary_value(text, "таргет rosstat_ipc_mes:"),
            "4 ряда, 2021-01 … 2022-12 (24 месяца)",
        )
        self.assertEqual(
            summary_value(text, "недельные rosstat_ipc_weeks:"),
            "2022-01-10 … 2022-05-27, 108 позиций",
        )
        self.assertEqual(summary_value(text, "ОФЗ 1y ofz_curve:"), "нет точек")
        self.assertEqual(
            summary_value(text, "ofz_1y недоступна на cutoff:"),
            "5 из 5 месяцев → признак NaN (G-curve без истории: zcyc — снимок дня)",
        )
        self.assertEqual(summary_value(text, "purge-проверка (структурная):"), "OK")

    def test_start_and_embargo_drive_the_main_window(self):
        code, text = self.run_summary(["--start", "3", "--embargo-months", "1"])
        self.assertEqual(code, 0)
        self.assertIn("start=3, embargo=1 (основное)", text)
        self.assertEqual(
            summary_value(text, "--start:"), "3 (основное окно walk-forward)"
        )
        self.assertEqual(
            summary_value(text, "--embargo-months:"), "1 (эмбарго основного окна)"
        )
        # Выборка флагами не меняется.
        self.assertEqual(
            summary_value(text, "выборка:"), "5 месяцев (2022-01 … 2022-05), пропущено 0"
        )

    def test_loaders_go_through_transport(self):
        with fixture_transport():
            self.assertEqual(len(cm.load_targets()), 4)
            self.assertEqual(cm.load_ofz_1y(), [])
            self.assertTrue(cm.load_targets()[cm.TARGET_ALL])

    def test_skipped_months_print_is_capped(self):
        # Сломанный ряд (нет FAO) у длинного мира: причины печатаются
        # построчно не больше MAX_SKIPPED_PRINTED месяцев, дальше — счётчик.
        months = [date(2022, m, 1) for m in range(1, 13)]
        months += [date(2023, m, 1) for m in range(1, 4)]
        world = synthetic_world(months=months, fao=[])
        output = io.StringIO()
        with tempfile.TemporaryDirectory() as tmp:
            with contextlib.ExitStack() as stack:
                for loader, key in (
                    ("load_targets", "targets"), ("load_weekly", "weekly"), ("load_usd", "usd"),
                    ("load_key_rate", "key_rate"), ("load_ofz_1y", "ofz"), ("load_fao", "fao"),
                ):
                    stack.enter_context(
                        unittest.mock.patch.object(cm, loader, lambda key=key: world[key])
                    )
                stack.enter_context(contextlib.redirect_stdout(output))
                code = cm.run_summary(out_dir=Path(tmp), today=date(2026, 10, 16))
        text = output.getvalue()
        self.assertEqual(code, 0)
        self.assertEqual(summary_value(text, "выборка:"), "пусто, пропущено 15")
        self.assertIn("… и ещё 3 месяца", text)
        self.assertEqual(text.count(" — нет пары доступных точек FAO"), cm.MAX_SKIPPED_PRINTED)


class TestSummaryFormat(unittest.TestCase):
    """Формы слов в сводке: 131 позиция, 108 позиций, 24 месяца."""

    def test_plural_forms(self):
        self.assertEqual(cm._plural(131, "позиция", "позиции", "позиций"), "позиция")
        self.assertEqual(cm._plural(108, "позиция", "позиции", "позиций"), "позиций")
        self.assertEqual(cm._plural(24, "месяц", "месяца", "месяцев"), "месяца")
        self.assertEqual(cm._plural(429, "месяц", "месяца", "месяцев"), "месяцев")
        self.assertEqual(cm._plural(11, "точка", "точки", "точек"), "точек")
        self.assertEqual(cm._plural(1, "точка", "точки", "точек"), "точка")


# --- Task 3: синтетика харнесса и бенчмарков ----------------------------------


def harness_rows(count: int = 61, effects: dict = None) -> list:
    """Строки харнесса: `y_t = 1 + 0.6·y_{t−1} + effect(месяц)`, признаки варьируют.

    Формула известна точно, поэтому OLS-бенчмарк B2 обязан её восстановить
    (c = 1, φ = 0.6, коэффициенты дамми — эффекты месяца), а `y_lag1` каждой
    строки — метка предыдущей строки: та же связка «признак ↔ прошлая метка»,
    что в живой выборке. 61 месяц — горизонт длиннее стартов walk-forward 24/48.
    """
    effects = effects or {}
    rows = []
    lag = 1.0
    for index in range(count):
        month = cm._month_shift(date(2020, 1, 1), index)
        y = 1.0 + 0.6 * lag + effects.get(month.month, 0.0)
        features = {
            "share_rising_wk": 0.5 + 0.01 * (index % 7),
            "median_wk": 0.2 + 0.001 * index,
            "median_wk4": 0.25 + 0.001 * index,
            "gasoline_4w": 0.4 + 0.002 * index,
            "usd_20d": 1.0 + 0.01 * (index % 11),
            "key_rate": 7.5 + 0.01 * (index % 5),
            "ofz_1y": 8.0 + 0.01 * (index % 3),
            "fao_mm": 0.1 + 0.001 * index,
            "y_lag1": lag,
            "month": month.month,
        }
        rows.append(
            cm.Row(month=month, y={code: y for code in cm.TARGET_SERIES}, features=features)
        )
        lag = y
    return rows


def enet_rows(count: int = 40) -> list:
    """Синтетика B3: таргет — линейная функция признаков (ElasticNet их находит).

    `ofz_1y` входит в таргет с большим весом: тест импутации обязан видеть, что
    подмена признака (а не только NaN) меняет прогноз.
    """
    generator = random.Random(cm.SEED)
    rows = []
    lag = 0.5
    for index in range(count):
        month = cm._month_shift(date(2020, 1, 1), index)
        values = {name: generator.uniform(-1.0, 1.0) for name in cm.ENET_FEATURES}
        values["y_lag1"] = lag
        y = 1.0 + 0.5 * lag + 0.3 * values["key_rate"] + values["ofz_1y"]
        rows.append(
            cm.Row(
                month=month,
                y={code: y for code in cm.TARGET_SERIES},
                features={**values, "month": month.month},
            )
        )
        lag = y
    return rows


def row_like(row: cm.Row, **features) -> cm.Row:
    """Копия строки с заменёнными признаками (`Row` неизменяем, признаки — dict)."""
    return cm.Row(month=row.month, y=dict(row.y), features={**row.features, **features})


class TestWalkForwardHarness(unittest.TestCase):
    """Walk-forward: только прошлое, эмбарго, счётчик вырожденных окон (спека §4)."""

    def setUp(self):
        self.rows = harness_rows()
        self.months = [row.month for row in self.rows]

    def test_first_fold_is_the_month_after_start_and_trains_on_past_only(self):
        seen = []

        def recording(train, row):
            seen.append([item.month for item in train])
            return float(len(train))

        predictions = cm.walk_forward(self.rows, {"stub": recording}, start=24, embargo=0)
        self.assertEqual(len(predictions), len(self.rows) - 24)
        self.assertEqual(predictions[0].month, self.months[24])  # 25-й месяц
        self.assertEqual(predictions[0].model, "stub")
        self.assertEqual(predictions[0].y_true, self.rows[24].y[cm.TARGET_ALL])
        self.assertEqual(predictions[0].y_pred, 24.0)
        # Обучающее окно каждого фолда — ровно предыдущие месяцы, ничего вперёд.
        for index, train_months in enumerate(seen, start=24):
            self.assertEqual(train_months, self.months[:index])

    def test_embargo_drops_the_month_before_the_test(self):
        seen = []

        def recording(train, row):
            seen.append(len(train))
            return 0.0

        cm.walk_forward(self.rows, {"stub": recording}, start=24, embargo=1)
        self.assertEqual(seen[0], 23)  # месяц перед тестом выброшен
        self.assertEqual(seen[1], 24)
        self.assertEqual(seen[-1], len(self.rows) - 2)

    def test_short_sample_gives_no_folds(self):
        predictions = cm.walk_forward(
            harness_rows(count=10), {"stub": lambda train, row: 0.0}, start=24, embargo=0
        )
        self.assertEqual(predictions, [])

    def test_walk_forward_is_deterministic(self):
        first = cm.walk_forward(self.rows, {cm.MODEL_ENET: cm.fit_enet}, start=24, embargo=0)
        second = cm.walk_forward(self.rows, {cm.MODEL_ENET: cm.fit_enet}, start=24, embargo=0)
        self.assertEqual(first, second)

    def test_degenerate_windows_are_counted_not_raised(self):
        skipped = {}
        predictions = cm.walk_forward(
            self.rows, {"enet": cm.fit_enet}, start=8, embargo=0, skipped=skipped
        )
        # Окно короче ENET_MIN_TRAIN=12 — четыре первых фолда пропущены со счётчиком.
        self.assertEqual(skipped, {"enet": 4})
        self.assertEqual(len(predictions), len(self.rows) - 12)
        self.assertEqual(predictions[0].month, self.months[12])

    def test_model_that_always_degrades_is_skipped_with_counter(self):
        def degenerate(train, row):
            raise cm.DegenerateWindowError("окно вырождено")

        skipped = {}
        predictions = cm.walk_forward(
            self.rows, {"bad": degenerate}, start=24, embargo=0, skipped=skipped
        )
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, {"bad": len(self.rows) - 24})

    def test_other_model_errors_are_not_swallowed(self):
        def broken(train, row):
            raise RuntimeError("баг модели")

        with self.assertRaises(RuntimeError):
            cm.walk_forward(self.rows, {"broken": broken}, start=24, embargo=0)

    def test_invalid_window_parameters_fail(self):
        stub = {"stub": lambda train, row: 0.0}
        with self.assertRaises(ValueError):
            cm.walk_forward(self.rows, stub, start=0, embargo=0)
        with self.assertRaises(ValueError):
            # embargo съедает весь обучающий набор — окна нет по построению
            cm.walk_forward(self.rows, stub, start=24, embargo=24)

    def test_window_specs_default_to_main_plus_robustness(self):
        self.assertEqual(cm.window_specs(), [(24, 0), (48, 0), (24, 1)])

    def test_fold_notes_are_aggregated_per_model(self):
        def noisy(train, row):
            noisy.fold_notes = {cm.NON_CONVERGED: 1}
            return 0.0

        rows = harness_rows()
        notes = {"noisy": {}, "quiet": {}}
        cm.walk_forward(rows, {"noisy": noisy, "quiet": lambda train, row: 0.0},
                        start=24, embargo=0, notes=notes)
        # каждый фолд noisy сообщил недосход — счётчик фолдов, а не предупреждений
        self.assertEqual(notes["noisy"], {cm.NON_CONVERGED: len(rows) - 24})
        # у модели без протокола диагностики нет
        self.assertEqual(notes["quiet"], {})

    def test_evaluate_windows_marks_main_and_collects_counters(self):
        runs = cm.evaluate_windows(self.rows, {"stub": lambda train, row: 0.0}, [(24, 0), (48, 0)])
        self.assertEqual([run.main for run in runs], [True, False])
        self.assertEqual([(run.start, run.embargo) for run in runs], [(24, 0), (48, 0)])
        self.assertEqual(runs[0].skipped, {"stub": 0})
        self.assertEqual(runs[0].notes, {"stub": {}})
        self.assertEqual(runs[0].folds, len(self.rows) - 24)
        self.assertEqual(len(runs[0].predictions), len(self.rows) - 24)
        self.assertEqual(len(runs[1].predictions), len(self.rows) - 48)

    def test_evaluate_windows_counts_non_converged_folds_per_window(self):
        models = {cm.MODEL_ENET: cm.fit_enet}
        runs = cm.evaluate_windows(self.rows, models, [(24, 0), (48, 0)])
        self.assertEqual(len(runs), 2)
        for run in runs:
            # счётчик по окну не меньше нуля и не больше числа его фолдов
            count = run.notes[cm.MODEL_ENET][cm.NON_CONVERGED]
            self.assertLessEqual(count, len(run.predictions))
            self.assertGreaterEqual(count, 0)


class TestBenchmarkLastValue(unittest.TestCase):
    """B1 «прошлый месяц»: прогноз — `y_{M−1}` из признаков строки (спека §3.3)."""

    def test_prediction_is_the_lag_feature(self):
        rows = harness_rows()
        row = rows[30]
        self.assertEqual(cm.predict_last([], row), row.features["y_lag1"])
        self.assertEqual(cm.predict_last(rows[:3], row), rows[29].y[cm.TARGET_ALL])
        # Источник лага не зависит от обучающего окна (и совпадает с моделями).
        self.assertEqual(cm.predict_last(rows[:3], row), cm.predict_last(rows[:29], row))

    def test_every_fold_forecasts_the_previous_month_even_with_embargo(self):
        rows = harness_rows()
        by_month = {row.month: row for row in rows}
        predictions = cm.walk_forward(rows, {cm.MODEL_LAST: cm.predict_last}, start=24, embargo=1)
        self.assertTrue(predictions)
        for prediction in predictions:
            previous = cm.previous_month(prediction.month)
            self.assertEqual(prediction.y_pred, by_month[previous].y[cm.TARGET_ALL])


class TestBenchmarkAr1(unittest.TestCase):
    """B2: OLS `y_M` на `y_{M−1}` + 11 дамми месяца, HAC (спека §3.3)."""

    def test_recovers_phi_and_intercept_on_known_formula(self):
        rows = harness_rows()
        prediction = cm.fit_ar1(rows[:50], rows[50])
        result = cm.fit_ar1.last_result
        self.assertAlmostEqual(float(result.params[0]), 1.0, places=6)
        self.assertAlmostEqual(float(result.params[1]), 0.6, places=6)
        self.assertAlmostEqual(prediction, rows[50].y[cm.TARGET_ALL], places=6)

    def test_hac_configuration_is_pinned_by_result_attributes(self):
        rows = harness_rows()
        cm.fit_ar1(rows[:40], rows[40])
        result = cm.fit_ar1.last_result
        self.assertEqual(result.cov_type, "HAC")
        self.assertEqual(result.cov_kwds["maxlags"], cm.HAC_MAXLAGS)
        self.assertTrue(result.cov_kwds["use_correction"])
        # const + y_lag1 + 11 дамми месяца
        self.assertEqual(len(result.params), 2 + len(cm.AR1_DUMMY_MONTHS))

    def test_month_dummies_map_to_calendar_months(self):
        rows = harness_rows(effects={3: 5.0})
        for month_number, expected in ((3, 5.0), (1, 0.0)):
            target = [row for row in rows if row.month.month == month_number][-1]
            train = [row for row in rows if row.month < target.month]
            seasonality = cm.fit_ar1(train, target) - (1.0 + 0.6 * target.features["y_lag1"])
            self.assertAlmostEqual(seasonality, expected, places=6)

    def test_degenerate_on_short_window(self):
        rows = harness_rows()
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_ar1(rows[:13], rows[13])

    def test_degenerate_on_constant_lag(self):
        rows = harness_rows()
        for row in rows:
            row.features["y_lag1"] = 1.0
            row.y.update({code: 1.0 for code in cm.TARGET_SERIES})
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_ar1(rows[:30], rows[30])


class TestBenchmarkEnet(unittest.TestCase):
    """B3: ElasticNetCV на стандартизованных признаках §2.3 без `month` (спека §3.3)."""

    def test_feature_vector_excludes_month(self):
        self.assertEqual(
            cm.ENET_FEATURES, tuple(name for name in cm.feature_names() if name != "month")
        )
        self.assertEqual(len(cm.ENET_FEATURES), 9)
        self.assertNotIn("month", cm.ENET_FEATURES)

    def test_configuration_is_pinned_from_the_fitted_model(self):
        rows = enet_rows()
        cm.fit_enet(rows[:30], rows[30])
        enet = cm.fit_enet.last_fit.model.named_steps["enet"]
        params = enet.get_params()
        np.testing.assert_allclose(params["alphas"], np.logspace(-4, 1, 20))
        self.assertEqual(list(params["l1_ratio"]), [0.1, 0.5, 0.9])
        self.assertIsInstance(params["cv"], TimeSeriesSplit)
        self.assertEqual(params["cv"].n_splits, 3)
        self.assertEqual(params["random_state"], cm.SEED)

    def test_scaler_is_fitted_on_the_train_window_only(self):
        rows = enet_rows()
        cm.fit_enet(rows[:30], rows[30])
        fit = cm.fit_enet.last_fit
        scaler = fit.model.named_steps["scaler"]
        np.testing.assert_allclose(scaler.mean_, fit.imputed.mean(axis=0))
        np.testing.assert_allclose(scaler.scale_, fit.imputed.std(axis=0))

    def test_prediction_is_deterministic(self):
        rows = enet_rows()
        self.assertEqual(cm.fit_enet(rows[:30], rows[30]), cm.fit_enet(rows[:30], rows[30]))

    def test_fit_emits_no_warnings(self):
        # Вывод тестов и прогона — только числа: недосчёт координатного спуска на
        # малых alpha не печатается (конфигурация закреплена спекой §3.3), а
        # фиксируется диагностикой `fold_notes` последнего фолда.
        rows = enet_rows()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            cm.fit_enet(rows[:30], rows[30])
        self.assertEqual([str(warning.message) for warning in caught], [])
        # Чистое окно: недосход не зафиксирован, но ключ диагностики есть.
        self.assertEqual(cm.fit_enet.fold_notes, {cm.NON_CONVERGED: 0})

    def test_non_converged_fold_is_counted_and_does_not_leak(self):
        # Подмена подгонки ElasticNetCV на предупреждающую: недосход обязан
        # попасть в диагностику фолда, а предупреждение — не утечь в вывод.
        rows = enet_rows()
        real_fit = ElasticNetCV.fit
        calls = []

        def warning_fit(model, *args, **kwargs):
            calls.append(1)
            warnings.warn("тест: координатный спуск не сошёлся", ConvergenceWarning)
            return real_fit(model, *args, **kwargs)

        with unittest.mock.patch.object(ElasticNetCV, "fit", warning_fit):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cm.fit_enet(rows[:30], rows[30])
        self.assertEqual(len(calls), 1)  # одна подгонка на фолд
        self.assertEqual(cm.fit_enet.fold_notes, {cm.NON_CONVERGED: 1})
        self.assertEqual([str(warning.message) for warning in caught], [])

    def test_other_fit_warnings_are_not_swallowed(self):
        # Диагностика считается только по ConvergenceWarning; чужие
        # предупреждения подгонки не прячутся, а переиздаются.
        rows = enet_rows()
        real_fit = ElasticNetCV.fit

        def noisy_fit(model, *args, **kwargs):
            warnings.warn("тест: чужое предупреждение подгонки", UserWarning)
            return real_fit(model, *args, **kwargs)

        with unittest.mock.patch.object(ElasticNetCV, "fit", noisy_fit):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                cm.fit_enet(rows[:30], rows[30])
        self.assertEqual(
            [str(warning.message) for warning in caught], ["тест: чужое предупреждение подгонки"]
        )
        self.assertEqual(cm.fit_enet.fold_notes, {cm.NON_CONVERGED: 0})

    def test_nan_in_train_is_imputed_with_column_median(self):
        rows = enet_rows()
        column = cm.ENET_FEATURES.index("ofz_1y")
        nan_positions = [3, 7, 9]
        for position in nan_positions:
            rows[position].features["ofz_1y"] = float("nan")
        present = [
            rows[position].features["ofz_1y"]
            for position in range(30)
            if position not in nan_positions
        ]
        median = statistics.median(present)
        cm.fit_enet(rows[:30], rows[30])
        fit = cm.fit_enet.last_fit
        self.assertAlmostEqual(fit.medians[column], median, places=12)
        for position in nan_positions:
            self.assertAlmostEqual(fit.imputed[position][column], median, places=12)
        self.assertFalse(np.isnan(fit.imputed).any())

    def test_nan_in_test_is_imputed_with_train_median(self):
        rows = enet_rows()
        column = cm.ENET_FEATURES.index("ofz_1y")
        train, test = rows[:30], rows[30]
        with_nan = cm.fit_enet(train, row_like(test, ofz_1y=float("nan")))
        median = float(cm.fit_enet.last_fit.medians[column])
        with_median = cm.fit_enet(train, row_like(test, ofz_1y=median))
        self.assertAlmostEqual(with_nan, with_median, places=12)
        # Импутация не «съедает» признак: другое значение прогноз всё же меняет.
        self.assertNotAlmostEqual(
            cm.fit_enet(train, row_like(test, ofz_1y=median + 10.0)), with_nan, places=6
        )

    def test_all_nan_column_is_a_neutral_constant(self):
        # Живой бэктест: ofz_1y — NaN во всех месяцах (у G-curve нет истории).
        rows = enet_rows()
        for row in rows:
            row.features["ofz_1y"] = float("nan")
        value = cm.fit_enet(rows[:30], rows[30])
        fit = cm.fit_enet.last_fit
        column = cm.ENET_FEATURES.index("ofz_1y")
        self.assertEqual(fit.medians[column], 0.0)
        self.assertFalse(np.isnan(fit.imputed).any())
        self.assertTrue(math.isfinite(value))

    def test_degenerate_when_most_columns_are_nan_columns(self):
        rows = enet_rows()
        names = ("median_wk", "median_wk4", "gasoline_4w", "usd_20d", "fao_mm")  # 5 из 9
        for position in range(30):
            for name in names:
                if position % 10 != 0:
                    rows[position].features[name] = float("nan")
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_enet(rows[:30], rows[30])

    def test_minority_nan_columns_are_imputed_not_degenerate(self):
        rows = enet_rows()
        names = ("median_wk", "median_wk4", "gasoline_4w", "usd_20d")  # 4 из 9 — меньше половины
        for position in range(30):
            for name in names:
                if position % 10 != 0:
                    rows[position].features[name] = float("nan")
        self.assertTrue(math.isfinite(cm.fit_enet(rows[:30], rows[30])))

    def test_degenerate_below_min_train(self):
        rows = enet_rows()
        with self.assertRaises(cm.DegenerateWindowError):
            cm.fit_enet(rows[:11], rows[11])
        self.assertTrue(math.isfinite(cm.fit_enet(rows[:12], rows[12])))


class TestMetricsAndSignificance(unittest.TestCase):
    """Метрики в п.п. через `eval_utils` и парные тесты значимости (спека §4)."""

    @staticmethod
    def predictions(errors, model="m", first_month=date(2022, 1, 1)):
        return [
            cm.Prediction(
                month=cm._month_shift(first_month, index),
                model=model,
                y_true=1.0,
                y_pred=1.0 + error,
            )
            for index, error in enumerate(errors)
        ]

    def test_errors_are_prediction_minus_fact(self):
        self.assertEqual(cm.errors(self.predictions([0.5, -1.5])), [0.5, -1.5])

    def test_rmse_and_mae_match_eval_utils_on_hand_numbers(self):
        predictions = self.predictions([0.5, -1.5])
        self.assertAlmostEqual(cm.rmse_pp(predictions), eu.rmse([0.5, -1.5]), places=12)
        self.assertAlmostEqual(cm.rmse_pp(predictions), math.sqrt((0.25 + 2.25) / 2), places=12)
        self.assertAlmostEqual(cm.mae_pp(predictions), 1.0, places=12)

    def test_diebold_mariano_wrapper_uses_h1(self):
        err_a = [0.1, -0.2, 0.3, 0.05, -0.4, 0.2, 0.1, -0.1]
        err_b = [0.2, -0.1, 0.25, 0.1, -0.3, 0.15, 0.2, -0.05]
        self.assertEqual(cm.diebold_mariano(err_a, err_b), eu.diebold_mariano(err_a, err_b, 1))

    def test_bootstrap_wrapper_pins_block_n_and_seed(self):
        err_a = [0.1, -0.2, 0.3, 0.05, -0.4, 0.2]
        err_b = [0.2, -0.1, 0.25, 0.1, -0.3, 0.15]
        self.assertEqual(
            cm.block_bootstrap_rmse_diff(err_a, err_b),
            eu.block_bootstrap_rmse_diff(err_a, err_b, block=3, n=2000, seed=cm.SEED),
        )

    def test_pairwise_tests_flag_a_biased_model(self):
        months = [cm._month_shift(date(2020, 1, 1), index) for index in range(24)]
        good = [cm.Prediction(month=m, model="good", y_true=1.0, y_pred=1.0) for m in months]
        bad = [cm.Prediction(month=m, model="bad", y_true=1.0, y_pred=1.4) for m in months]
        tests = cm.pairwise_tests(good + bad)
        self.assertEqual(len(tests), 1)
        pair = tests[0]
        self.assertEqual((pair.model_a, pair.model_b), ("good", "bad"))
        self.assertEqual(pair.n, 24)
        self.assertAlmostEqual(pair.rmse_diff, -0.4, places=12)  # RMSE(good) − RMSE(bad)
        self.assertTrue(math.isinf(pair.dm_stat) and pair.dm_stat < 0)
        self.assertEqual(pair.dm_p, 0.0)
        self.assertAlmostEqual(pair.ci_low, -0.4, places=12)
        self.assertAlmostEqual(pair.ci_high, -0.4, places=12)
        self.assertEqual(pair.bootstrap_p, 0.0)

    def test_pairwise_tests_skip_models_without_common_months(self):
        first = self.predictions([0.1, 0.2, 0.3], model="a", first_month=date(2022, 1, 1))
        second = self.predictions([0.1, 0.2, 0.3], model="b", first_month=date(2024, 1, 1))
        self.assertEqual(cm.pairwise_tests(first + second), [])


class TestReportSkeleton(unittest.TestCase):
    """Скелет отчёта: «модель × окно», последние 12 фолдов, значимость, границы."""

    @staticmethod
    def biased(offset, notes=None):
        """Заглушка FitPredict; `notes` (если задан) — диагностика каждого фолда."""

        def fit_predict(train, row):
            if notes is not None:
                fit_predict.fold_notes = dict(notes)
            return row.y[cm.TARGET_ALL] + offset

        return fit_predict

    @classmethod
    def build_runs(cls):
        rows = harness_rows()
        models = {
            cm.MODEL_LAST: cls.biased(0.0),
            cm.MODEL_AR1: cls.biased(0.2),
            # B3 «не сходится» на каждом фолде — строка диагностики обязана быть
            cm.MODEL_ENET: cls.biased(0.5, notes={cm.NON_CONVERGED: 1}),
        }
        return rows, cm.evaluate_windows(rows, models, cm.window_specs())

    def render(self, rows=None, runs=None, nowcast=None):
        rows = harness_rows() if rows is None else rows
        runs = self.build_runs()[1] if runs is None else runs
        return cm.render_report(rows, [], runs, nowcast=nowcast, today=date(2026, 10, 16))

    def test_sections_and_model_by_window_table(self):
        report = self.render()
        for section in (
            "# CPI nowcast",
            "## Выборка",
            "## Walk-forward",
            "## Метрики (RMSE / MAE, п.п.)",
            "## Последние 12 фолдов (основное окно start=24)",
            "## Тесты значимости между моделями (основное окно start=24)",
            "## Что не проверено",
            "## Текущий nowcast",
        ):
            self.assertIn(section, report)
        self.assertIn("Дата прогона: 2026-10-16", report)
        # «модель × окно»: фолды, пропуски и недосход по каждому окну
        self.assertIn("| B1 last value | start=24 (основное) | 37 | 0 | — |", report)
        self.assertIn("| B3 ElasticNetCV | start=48 (робастность) | 13 | 0 | 13 из 13 |", report)
        self.assertIn("| B1 last value | start=24 (основное) | 37 | 0.000 | 0.000 |", report)
        self.assertIn("| B2 AR(1)+сезонные дамми | start=24 (основное) | 37 | 0.200 | 0.200 |", report)
        # блок «последние 12 фолдов» — тот же отрезок для всех моделей
        self.assertIn("| B1 last value | 12 | 0.000 | 0.000 |", report)
        # значимость между бенчмарками: B1 точнее B2 на 0.2 п.п. (ошибки константны)
        self.assertIn("-0.200", report)
        self.assertIn("<0.0001", report)

    def test_nan_policy_is_documented(self):
        report = self.render()
        self.assertIn("импутирует NaN медианой обучающего окна фолда", report)
        self.assertIn("StandardScaler", report)
        self.assertIn("ofz_1y", report)

    def test_nowcast_block_is_a_placeholder_until_task5(self):
        self.assertIn("Task 5", self.render())
        block = "**Октябрь-2026**: 0.55 п.п. (предварительно)"
        self.assertIn(block, self.render(nowcast=block))

    def test_report_renders_without_sample_or_runs(self):
        report = cm.render_report([], [], [], nowcast=None, today=date(2026, 10, 16))
        self.assertIn("# CPI nowcast", report)
        self.assertIn("## Что не проверено", report)

    def test_report_is_deterministic_for_a_fixed_date(self):
        self.assertEqual(self.render(), self.render())


class TestArtifacts(unittest.TestCase):
    """Артефакты прогона: имена, LF, колонки CSV, порядок строк."""

    @staticmethod
    def runs():
        rows = harness_rows()
        models = {cm.MODEL_LAST: lambda train, row: row.y[cm.TARGET_ALL]}
        return cm.evaluate_windows(rows, models, [(24, 0), (24, 1)])

    def test_write_artifacts_names_columns_and_lf(self):
        runs = self.runs()
        with tempfile.TemporaryDirectory() as tmp:
            report_path, predictions_path = cm.write_artifacts(Path(tmp), "# отчёт\n", runs)
            self.assertEqual(report_path.name, cm.REPORT_NAME)
            self.assertEqual(predictions_path.name, cm.PREDICTIONS_NAME)
            self.assertEqual(report_path.read_text(encoding="utf-8"), "# отчёт\n")
            raw = predictions_path.read_bytes()
            self.assertNotIn(b"\r", raw)
            rows = list(csv.DictReader(io.StringIO(raw.decode("utf-8"))))
            self.assertEqual(len(rows), sum(len(run.predictions) for run in runs))
            self.assertEqual(tuple(rows[0]), cm.PREDICTIONS_COLUMNS)
            self.assertEqual(rows[0]["window"], "start=24")
            self.assertEqual(rows[0]["model"], cm.MODEL_LAST)
            self.assertAlmostEqual(
                float(rows[0]["error"]),
                float(rows[0]["y_pred"]) - float(rows[0]["y_true"]),
                places=12,
            )
            self.assertEqual(
                {row["window"] for row in rows}, {"start=24", "start=24, embargo=1"}
            )
            # Порядок — окна прогона: строки второго окна идут после первого.
            windows = [row["window"] for row in rows]
            self.assertEqual(
                windows.index("start=24, embargo=1"),
                sum(1 for window in windows if window == "start=24"),
            )


class TestRunOffline(unittest.TestCase):
    """Живой путь прогона офлайн: фикстуры, walk-forward, артефакты."""

    def test_run_summary_writes_artifacts_on_fixtures(self):
        with tempfile.TemporaryDirectory() as tmp:
            output = io.StringIO()
            with fixture_transport(), contextlib.redirect_stdout(output):
                code = cm.run_summary(
                    start=3, embargo_months=0, out_dir=Path(tmp), today=date(2026, 10, 16)
                )
            self.assertEqual(code, 0)
            self.assertIn("walk-forward", output.getvalue())
            report = (Path(tmp) / cm.REPORT_NAME).read_text(encoding="utf-8")
            self.assertIn("2022-01 … 2022-05", report)
            self.assertIn("Дата прогона: 2026-10-16", report)
            with (Path(tmp) / cm.PREDICTIONS_NAME).open(encoding="utf-8", newline="") as handle:
                predictions = list(csv.DictReader(handle))
            self.assertTrue(predictions)
            # На пяти месяцах фолд B2/B3 вырожден (окно короче минимума) — пропуск
            # со счётчиком, а не падение: в CSV остаются только прогнозы B1.
            self.assertEqual({row["model"] for row in predictions}, {cm.MODEL_LAST})
            self.assertIn("| B3 ElasticNetCV | start=3 (основное) | 0 | 2 |", report)


if __name__ == "__main__":
    unittest.main()
