#!/usr/bin/env python3
"""Офлайн-тесты аудита недельного агрегата ИПЦ 2009+ (#396, Task 1).

Проверяются только чистые функции: раскладка недели по месяцам, дубли дат,
гистограмма интервалов, покрытие по годам и сверка стыков источников.
ClickHouse и сеть не используются.

Запуск — `make cpi-weekly-aggregate-test`.
"""

import math
import unittest
from datetime import date

from cpi_weekly_aggregate import (
    coverage_by_year,
    find_duplicate_dates,
    interval_histogram,
    long_gaps,
    seam_report,
    split_week_by_month,
    weekday_histogram,
)


class SplitWeekByMonthTest(unittest.TestCase):
    def test_week_inside_one_month_is_returned_unchanged(self):
        parts = split_week_by_month(date(2022, 1, 17), 7, 100.30)
        self.assertEqual(parts, [(date(2022, 1, 1), 100.30)])

    def test_week_across_month_boundary_splits_by_days_in_log_scale(self):
        # неделя 2022-01-31 .. 2022-02-06 (конец 6 февраля): 1 день января, 6 февраля
        parts = split_week_by_month(date(2022, 2, 6), 7, 101.0)
        self.assertEqual([month for month, _ in parts], [date(2022, 1, 1), date(2022, 2, 1)])
        self.assertAlmostEqual(parts[0][1], 101.0 ** (1 / 7) * 100.0 ** (6 / 7), places=9)
        self.assertAlmostEqual(parts[1][1], 101.0 ** (6 / 7) * 100.0 ** (1 / 7), places=9)

    def test_product_of_parts_equals_original_index(self):
        for week_end, length, index in (
            (date(2022, 2, 6), 7, 101.0),
            (date(2021, 3, 2), 8, 99.7),
            (date(2023, 1, 10), 14, 100.45),  # интервал через новогодний разрыв
        ):
            parts = split_week_by_month(week_end, length, index)
            product = 1.0
            for _, part in parts:
                product *= part / 100.0
            self.assertAlmostEqual(product * 100.0, index, places=9)

    def test_gap_longer_than_a_week_covers_all_touched_months(self):
        parts = split_week_by_month(date(2022, 1, 10), 21, 100.8)
        months = [month for month, _ in parts]
        self.assertEqual(months, [date(2021, 12, 1), date(2022, 1, 1)])

    def test_rejects_non_positive_arguments(self):
        with self.assertRaises(ValueError):
            split_week_by_month(date(2022, 1, 10), 0, 100.0)
        with self.assertRaises(ValueError):
            split_week_by_month(date(2022, 1, 10), 7, 0.0)


class DuplicatesTest(unittest.TestCase):
    def test_finds_dates_with_several_rows(self):
        rows = [
            (date(2022, 1, 10), 100.1),
            (date(2022, 1, 10), 100.1),
            (date(2022, 1, 17), 100.2),
        ]
        self.assertEqual(find_duplicate_dates(rows), {date(2022, 1, 10): [100.1, 100.1]})

    def test_empty_when_unique(self):
        self.assertEqual(find_duplicate_dates([(date(2022, 1, 10), 100.1)]), {})


class IntervalHistogramTest(unittest.TestCase):
    def test_counts_gaps_between_sorted_unique_dates(self):
        dates = [date(2022, 1, 10), date(2022, 1, 17), date(2022, 1, 24), date(2022, 2, 7)]
        self.assertEqual(interval_histogram(dates), {7: 2, 14: 1})

    def test_unsorted_and_repeated_input_is_normalised(self):
        dates = [date(2022, 1, 17), date(2022, 1, 10), date(2022, 1, 10)]
        self.assertEqual(interval_histogram(dates), {7: 1})

    def test_fewer_than_two_dates(self):
        self.assertEqual(interval_histogram([date(2022, 1, 10)]), {})


class LongGapsTest(unittest.TestCase):
    def test_lists_gaps_not_shorter_than_threshold(self):
        dates = [date(2022, 1, 10), date(2022, 1, 17), date(2022, 2, 21), date(2022, 2, 28)]
        self.assertEqual(long_gaps(dates, min_days=15), [(date(2022, 1, 17), date(2022, 2, 21), 35)])

    def test_none_when_all_short(self):
        self.assertEqual(long_gaps([date(2022, 1, 10), date(2022, 1, 17)], min_days=15), [])


class WeekdayHistogramTest(unittest.TestCase):
    def test_counts_by_weekday_monday_is_zero(self):
        dates = [date(2022, 1, 10), date(2022, 1, 17), date(2022, 1, 12)]
        self.assertEqual(weekday_histogram(dates), {0: 2, 2: 1})


class CoverageByYearTest(unittest.TestCase):
    def test_counts_unique_dates_per_year(self):
        dates = [date(2021, 12, 27), date(2022, 1, 10), date(2022, 1, 10), date(2022, 1, 17)]
        self.assertEqual(coverage_by_year(dates), {2021: 1, 2022: 2})


class SeamReportTest(unittest.TestCase):
    def test_matched_dates_and_max_difference(self):
        a = {date(2020, 1, 6): 100.10, date(2020, 1, 13): 100.20}
        b = {date(2020, 1, 6): 100.12, date(2020, 1, 13): 100.20}
        report = seam_report(a, b, tol_pp=0.05)
        self.assertEqual(report["n_common"], 2)
        self.assertAlmostEqual(report["max_abs_diff_pp"], 0.02, places=9)
        self.assertEqual(report["over_tol"], [])

    def test_flags_differences_over_tolerance(self):
        a = {date(2020, 1, 6): 100.10}
        b = {date(2020, 1, 6): 100.30}
        report = seam_report(a, b, tol_pp=0.05)
        self.assertEqual(report["over_tol"], [(date(2020, 1, 6), 100.10, 100.30)])

    def test_offset_pairs_reveal_double_counting_risk(self):
        # те же недели с датами, сдвинутыми на 1–3 дня: точного совпадения нет,
        # поэтому дедуп витрины по (series, date) их не склеит
        a = {date(2020, 1, 6): 100.10}
        b = {date(2020, 1, 8): 100.11}
        report = seam_report(a, b, tol_pp=0.05, max_offset=3)
        self.assertEqual(report["n_common"], 0)
        self.assertEqual(report["offset_pairs"], [(date(2020, 1, 6), date(2020, 1, 8))])

    def test_far_dates_are_not_offset_pairs(self):
        a = {date(2020, 1, 6): 100.10}
        b = {date(2020, 1, 13): 100.11}
        self.assertEqual(seam_report(a, b)["offset_pairs"], [])

    def test_no_overlap_gives_nan_max(self):
        report = seam_report({date(2020, 1, 6): 100.0}, {date(2021, 1, 4): 100.0})
        self.assertEqual(report["n_common"], 0)
        self.assertTrue(math.isnan(report["max_abs_diff_pp"]))


if __name__ == "__main__":
    unittest.main()
