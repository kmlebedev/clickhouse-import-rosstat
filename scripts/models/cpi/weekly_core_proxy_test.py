#!/usr/bin/env python3
"""Офлайн-тесты research-скрипта недельного прокси базовой инфляции (#348).

Проверяются только чистые функции: 4-нед. средняя, 7-дневная сетка,
календарные регрессоры, тарифные недели, выравнивание на месяц и DM.
ClickHouse, сеть и R не используются.

Запуск — `make weekly-core-proxy-test`.
"""

import math
import unittest
from datetime import date

from weekly_core_proxy import (
    TARIFF_DATES,
    WeeklyRow,
    align_first_next,
    align_last,
    align_mean,
    four_week_average,
    growth_rows,
    month_end,
    month_key,
    months_between,
    regular_grid,
)

from lib import eval_utils as eu


class FourWeekAverageTest(unittest.TestCase):
    def test_matches_manual_geometric_mean(self):
        rows = [
            WeeklyRow(date(2022, 1, 10), 101.0, 1),
            WeeklyRow(date(2022, 1, 17), 99.0, 1),
            WeeklyRow(date(2022, 1, 24), 100.0, 1),
            WeeklyRow(date(2022, 1, 31), 100.0, 1),
        ]
        result = four_week_average(rows)
        expected = ((1.01 * 0.99 * 1.00 * 1.00) ** (52 / 4) - 1) * 100
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0][0], date(2022, 1, 31))
        self.assertAlmostEqual(result[0][1], expected, places=10)

    def test_requires_four_weeks(self):
        rows = [WeeklyRow(date(2022, 1, day), 100.0, 1) for day in (10, 17, 24)]
        self.assertEqual(four_week_average(rows), [])


class RegularGridTest(unittest.TestCase):
    def test_gap_is_interpolated_by_days(self):
        rows = [
            WeeklyRow(date(2022, 1, 3), 100.0, 1),
            WeeklyRow(date(2022, 1, 17), 102.01, 1),
        ]
        grid = regular_grid(rows)
        self.assertEqual([day for day, _ in grid], [date(2022, 1, 3), date(2022, 1, 10), date(2022, 1, 17)])
        self.assertAlmostEqual(grid[0][1], 0.0, places=12)
        self.assertAlmostEqual(grid[1][1], 0.5 * math.log(1.0201), places=12)
        self.assertAlmostEqual(grid[2][1], math.log(1.0201), places=12)

    def test_single_row_gives_empty_grid(self):
        self.assertEqual(regular_grid([WeeklyRow(date(2022, 1, 3), 100.0, 1)]), [])


class MonthAlignTest(unittest.TestCase):
    def test_last_and_mean(self):
        series = [
            (date(2022, 1, 10), 1.0),
            (date(2022, 1, 31), 3.0),
            (date(2022, 2, 7), 5.0),
        ]
        self.assertEqual(align_last(series), {"2022-01": 3.0, "2022-02": 5.0})
        self.assertEqual(align_mean(series), {"2022-01": 2.0, "2022-02": 5.0})

    def test_first_next_month(self):
        series = [
            (date(2022, 1, 31), 1.0),
            (date(2022, 2, 7), 2.0),
        ]
        self.assertEqual(align_first_next(series, ["2022-01"]), {"2022-01": 2.0})
        self.assertEqual(align_first_next(series, ["2022-02"]), {})

    def test_month_helpers(self):
        self.assertEqual(month_key(date(2024, 2, 29)), "2024-02")
        self.assertEqual(month_end("2024-02"), date(2024, 2, 29))
        self.assertEqual(month_end("2024-12"), date(2024, 12, 31))
        self.assertEqual(months_between("2022-11", "2023-02"), ["2022-11", "2022-12", "2023-01", "2023-02"])


class CalendarRegressorsTest(unittest.TestCase):
    def test_new_year_gap_counts_holidays_and_first_january(self):
        rows = [
            WeeklyRow(date(2022, 12, 26), 100.0, 1),
            WeeklyRow(date(2023, 1, 9), 100.5, 1),
        ]
        new_year = {date(2023, 1, day) for day in range(1, 9)}
        growth = growth_rows(rows, new_year)
        self.assertEqual(len(growth), 1)
        row = growth[0]
        self.assertEqual(row.days, 14)
        self.assertEqual(row.holidays, 8)
        self.assertEqual(row.jan_first, 1)
        self.assertEqual(row.may, 0)
        self.assertEqual(row.tariff, 0)
        self.assertAlmostEqual(row.g, 0.004987541511, places=9)

    def test_may_interval_is_flagged(self):
        rows = [
            WeeklyRow(date(2023, 4, 24), 100.0, 1),
            WeeklyRow(date(2023, 5, 2), 100.1, 1),
        ]
        growth = growth_rows(rows, set())
        self.assertEqual(growth[0].may, 1)


class TariffTest(unittest.TestCase):
    def test_july_and_october_intervals(self):
        dates = [
            date(2024, 6, 24),
            date(2024, 7, 1),
            date(2024, 7, 8),
            date(2026, 9, 28),
            date(2026, 10, 5),
        ]
        rows = [WeeklyRow(day, 100.0, 1) for day in dates]
        growth = growth_rows(rows, set(), tariff_dates=TARIFF_DATES)
        flags = {row.date: row.tariff for row in growth}
        self.assertEqual(flags[date(2024, 7, 1)], 1)
        self.assertEqual(flags[date(2024, 7, 8)], 0)
        self.assertEqual(flags[date(2026, 10, 5)], 1)


class MetricsTest(unittest.TestCase):
    def test_dm_identical_errors(self):
        self.assertEqual(eu.diebold_mariano([0.1, 0.2, 0.3], [0.1, 0.2, 0.3], h=1), (0.0, 1.0))

    def test_block_uses_common_months(self):
        from weekly_core_proxy import metrics

        actual = {f"2022-{month:02d}": float(month) for month in range(1, 7)}
        prediction = {key: value + 0.5 for key, value in actual.items()}
        baseline = {key: value + 0.2 for key, value in actual.items()}
        result = metrics(actual, prediction, baseline)
        self.assertEqual(result["n"], 6)
        self.assertAlmostEqual(result["mae"], 0.5, places=12)
        self.assertAlmostEqual(result["bias"], 0.5, places=12)
        self.assertEqual(result["window"], ["2022-01", "2022-06"])


if __name__ == "__main__":
    unittest.main()
