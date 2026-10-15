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
(2021-12 — y_lag1 первого месяца выборки). `ofz_2021_2022.csv` — ПУСТОЙ (только
заголовок): у G-curve МосБиржи (`zcyc`) нет истории (снимок текущего дня),
окно 2021–2022 пусто, и экспорт пишет это предупреждением, а не ошибкой. Живой
прогон видит ровно то же самое (единственная точка ряда — 2026-10-09), поэтому
тесты пинуют отсутствие точки как NaN, а формулу признака — на точках в памяти.
Файлы коммитятся: тесты обязаны воспроизводиться без сети и БД.
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

import cpi_nowcast as cm

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
        # Ограничение dev-БД: G-curve без истории. Месяц НЕ выбрасывается,
        # признак честно помечен NaN (см. docstring модуля).
        rows, skipped = build_world(synthetic_world(ofz=[]))
        self.assertEqual(skipped, [])
        for row in rows:
            self.assertTrue(math.isnan(row.features["ofz_1y"]))

    def test_ofz_available_point_is_used(self):
        world = synthetic_world(
            ofz=[(date(2021, 12, 15), 8.0), (date(2022, 1, 20), 9.0), (date(2022, 2, 20), 10.0)]
        )
        rows, _ = build_world(world)
        self.assertAlmostEqual(rows[0].features["ofz_1y"], 9.0, places=9)
        self.assertAlmostEqual(rows[1].features["ofz_1y"], 10.0, places=9)

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
        with fixture_transport(), contextlib.redirect_stdout(output):
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

    def test_start_and_embargo_are_accepted_as_noops(self):
        code, text = self.run_summary(["--start", "24", "--embargo-months", "1"])
        self.assertEqual(code, 0)
        self.assertIn("принят", summary_value(text, "--start:"))
        self.assertIn("принят", summary_value(text, "--embargo-months:"))
        # На саму сводку они пока не влияют (walk-forward — Task 3).
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
        with contextlib.ExitStack() as stack:
            for loader, key in (
                ("load_targets", "targets"), ("load_weekly", "weekly"), ("load_usd", "usd"),
                ("load_key_rate", "key_rate"), ("load_ofz_1y", "ofz"), ("load_fao", "fao"),
            ):
                stack.enter_context(
                    unittest.mock.patch.object(cm, loader, lambda key=key: world[key])
                )
            stack.enter_context(contextlib.redirect_stdout(output))
            code = cm.run_summary()
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


if __name__ == "__main__":
    unittest.main()
