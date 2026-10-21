#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/cpi/cpi_pipeline_sa.py`.

Запуск из корня репозитория:

    make cpi-pipeline-sa-test

ClickHouse и сеть не нужны: STL оценивается на синтетических месячных рядах.
Проверяются as-of (факторы не видят будущее), удаление известной сезонности,
детерминизм, контракт `apply_sa`/`sa_series` и поведение при недостаточной
истории (меньше двух полных периодов).
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_pipeline_sa as sa  # noqa: E402


def shift_month(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_points(values, first: date = date(2020, 1, 1)) -> list:
    """Месячный ряд: значения по порядку с первого числа `first`."""
    return [
        (shift_month(first, index), float(value))
        for index, value in enumerate(values)
    ]


def seasonal_values(first: date = date(2020, 1, 1)) -> list:
    """24 месяца с месячной сезонностью `10 + (номер месяца % 12)`."""
    points = month_points([0.0] * 24, first)
    return [(day, 10.0 + (day.month % 12)) for day, _ in points]


class TestSeasonalFactorsAsOf(unittest.TestCase):
    def test_factors_use_only_past(self):
        # 24 месяца (два полных периода) плюс будущая точка-выброс: факторы
        # на 2021-12-31 не должны зависеть от 2022-01.
        base = [
            (day, 1.0 + 0.1 * day.month)
            for day in (shift_month(date(2020, 1, 1), i) for i in range(24))
        ]
        future = base + [(date(2022, 1, 1), 999.0)]
        as_of = date(2021, 12, 31)
        self.assertEqual(
            sa.seasonal_factors(base, as_of),
            sa.seasonal_factors(future, as_of),
        )

    def test_factors_use_only_past_with_one_period(self):
        # Ровно один период (12 месяцев): сезонность не идентифицируется,
        # факторов нет; будущая точка на ответ тоже не влияет.
        base = [(date(2020, m, 1), 1.0 + 0.1 * m) for m in range(1, 13)]
        future = base + [(date(2021, 1, 1), 999.0)]
        as_of = date(2020, 12, 31)
        self.assertEqual(sa.seasonal_factors(base, as_of), {})
        self.assertEqual(
            sa.seasonal_factors(base, as_of),
            sa.seasonal_factors(future, as_of),
        )

    def test_as_of_is_inclusive(self):
        series = month_points([1.0 + 0.1 * (i % 12) for i in range(25)])
        # 2021-12-01 — 24-я точка (два полных периода), она включается.
        self.assertTrue(sa.seasonal_factors(series, date(2021, 12, 1)))
        # 2021-11-30 — только 23 точки, сезонность не оценивается.
        self.assertEqual(
            sa.seasonal_factors(series, date(2021, 11, 30)), {}
        )

    def test_deterministic(self):
        series = month_points([1.0] * 24)
        as_of = date(2021, 12, 31)
        self.assertEqual(
            sa.seasonal_factors(series, as_of),
            sa.seasonal_factors(series, as_of),
        )

    def test_factors_cover_every_month(self):
        factors = sa.seasonal_factors(seasonal_values(), date(2021, 12, 31))
        self.assertEqual(sorted(factors), list(range(1, 13)))
        for value in factors.values():
            self.assertTrue(math.isfinite(value))

    def test_unsorted_series_is_sorted(self):
        series = month_points([1.0 + 0.1 * (i % 12) for i in range(24)])
        shuffled = list(reversed(series))
        as_of = date(2021, 12, 31)
        self.assertEqual(
            sa.seasonal_factors(shuffled, as_of),
            sa.seasonal_factors(series, as_of),
        )

    def test_insufficient_history_returns_no_factors(self):
        # Меньше двух полных периодов: аддитивная сезонность не
        # идентифицируется, возвращается пустой словарь.
        series = month_points([1.0 + 0.1 * (i % 12) for i in range(23)])
        self.assertEqual(
            sa.seasonal_factors(series, date(2021, 11, 1)), {}
        )

    def test_insufficient_history_leaves_series_unchanged(self):
        series = month_points([1.0 + 0.1 * (i % 12) for i in range(23)])
        as_of = date(2021, 11, 1)
        factors = sa.seasonal_factors(series, as_of)
        self.assertEqual(sa.apply_sa([v for _, v in series], factors),
                         [v for _, v in series])
        self.assertEqual(sa.sa_series(series, as_of), series)

    def test_period_must_be_at_least_two(self):
        with self.assertRaises(ValueError):
            sa.seasonal_factors(
                month_points([1.0] * 24), date(2021, 12, 31), period=1
            )

    def test_nan_rejected(self):
        series = month_points([1.0] * 24)
        series[5] = (series[5][0], float("nan"))
        with self.assertRaises(ValueError):
            sa.seasonal_factors(series, date(2021, 12, 31))

    def test_duplicate_month_rejected(self):
        series = month_points([1.0] * 24)
        series[5] = (series[4][0], 1.0)
        with self.assertRaises(ValueError):
            sa.seasonal_factors(series, date(2021, 12, 31))

    def test_month_gap_rejected(self):
        series = month_points([1.0] * 24)
        del series[5]
        with self.assertRaises(ValueError):
            sa.seasonal_factors(series, date(2021, 12, 31))


class TestApplySa(unittest.TestCase):
    def test_additive_sa_removes_known_seasonality(self):
        series = seasonal_values()
        factors = sa.seasonal_factors(series, date(2021, 12, 31))
        adjusted = sa.apply_sa([value for _, value in series], factors)
        self.assertLess(max(adjusted) - min(adjusted), 2.0)

    def test_cycles_by_month_position(self):
        # apply_sa не получает дат: позиция 0 — январь, 12 — снова январь.
        factors = {month: float(month) for month in range(1, 13)}
        values = [100.0 + index for index in range(14)]
        adjusted = sa.apply_sa(values, factors)
        self.assertEqual(len(adjusted), len(values))
        self.assertAlmostEqual(adjusted[0], 99.0)
        self.assertAlmostEqual(adjusted[12], 111.0)

    def test_without_factors_is_identity(self):
        values = [1.0, 2.0, 3.0]
        self.assertEqual(sa.apply_sa(values, {}), values)

    def test_requires_complete_factor_dict(self):
        with self.assertRaises(ValueError):
            sa.apply_sa([1.0, 2.0], {1: 0.1, 3: 0.3})


class TestSaSeries(unittest.TestCase):
    def test_subtracts_factors_by_calendar_month(self):
        series = seasonal_values()
        as_of = date(2021, 12, 31)
        factors = sa.seasonal_factors(series, as_of)
        adjusted = sa.sa_series(series, as_of)
        self.assertEqual(
            [day for day, _ in adjusted],
            [day for day, _ in series],
        )
        for (day, value), (_, clean) in zip(series, adjusted):
            self.assertAlmostEqual(
                clean, value - factors[day.month], places=12
            )

    def test_truncates_future(self):
        series = seasonal_values() + [(date(2022, 1, 1), 9999.0)]
        adjusted = sa.sa_series(series, date(2021, 12, 31))
        self.assertEqual(len(adjusted), 24)
        self.assertEqual(adjusted[-1][0], date(2021, 12, 1))
        self.assertNotIn(9999.0, [value for _, value in adjusted])

    def test_uses_calendar_month_not_list_position(self):
        # Ряд начинается в марте: позиция 0 — март, а не январь, поэтому
        # фактор берётся по календарному месяцу даты.
        series = seasonal_values(first=date(2020, 3, 1))
        as_of = date(2022, 2, 28)
        factors = sa.seasonal_factors(series, as_of)
        adjusted = sa.sa_series(series, as_of)
        self.assertEqual(len(adjusted), 24)
        self.assertAlmostEqual(adjusted[0][1], series[0][1] - factors[3])


if __name__ == "__main__":
    unittest.main(verbosity=2)
