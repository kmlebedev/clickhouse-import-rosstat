#!/usr/bin/env python3
"""Офлайн-тесты аудита недельного агрегата ИПЦ 2009+ (#396, Task 1).

Проверяются только чистые функции: раскладка недели по месяцам, дубли дат,
гистограмма интервалов, покрытие по годам и сверка стыков источников.
ClickHouse и сеть не используются.

Запуск — `make cpi-weekly-aggregate-test`.
"""

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_monthly as cm
from cpi_weekly_aggregate import (
    SAMPLE_BOUNDS_V4,
    WKAGG_FEATURES,
    attach_wkagg,
    feature_names_v4,
    period_length,
    wkagg_summary,
    wkagg_features,
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


def mondays(first: date, count: int, value: float) -> list:
    """Недельные точки по понедельникам: `[(дата, индекс)]`."""
    return [(first + timedelta(days=7 * i), value) for i in range(count)]


# Март 2022: конец периодов 03-07, 03-14, 03-21, 03-28 (по 7 дней, 1–28 марта)
# и 04-04 (29 марта – 4 апреля: 3 дня марта). 02-28 — предыдущая дата для 03-07.
MARCH = mondays(date(2022, 2, 28), 6, 100.2)
MARCH_MONTH = date(2022, 3, 1)
CUTOFF_FULL = date(2022, 4, 8)  # cutoff_for(2022-03); 04-04 доступна с 04-06
CUTOFF_NO_LAST = date(2022, 4, 5)  # точка 04-04 ещё недоступна


def is_nan(value) -> bool:
    return isinstance(value, float) and math.isnan(value)


class PeriodLengthTest(unittest.TestCase):
    def test_regular_and_slightly_longer_gaps_use_the_gap(self):
        self.assertEqual(period_length(date(2022, 3, 7), date(2022, 2, 28)), 7)
        self.assertEqual(period_length(date(2022, 3, 8), date(2022, 2, 28)), 8)
        self.assertEqual(period_length(date(2022, 3, 10), date(2022, 2, 28)), 10)

    def test_long_gap_inside_year_means_missing_weeks_so_period_is_one_week(self):
        self.assertEqual(period_length(date(2013, 7, 15), date(2013, 4, 29)), 7)
        self.assertEqual(period_length(date(2016, 5, 4), date(2016, 4, 18)), 7)

    def test_new_year_gap_across_year_boundary_is_the_whole_gap(self):
        self.assertEqual(period_length(date(2025, 1, 9), date(2024, 12, 23)), 17)
        self.assertEqual(period_length(date(2026, 1, 12), date(2025, 12, 22)), 21)

    def test_first_point_without_predecessor_is_one_week(self):
        self.assertEqual(period_length(date(2009, 4, 6), None), 7)


class WkaggFeaturesTest(unittest.TestCase):
    def test_names_are_pinned(self):
        self.assertEqual(
            WKAGG_FEATURES,
            ("wkagg_mtd", "wkagg_cov", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w", "tariff_week"),
        )

    def test_full_month_coverage(self):
        feats = wkagg_features(MARCH, MARCH_MONTH, CUTOFF_FULL)
        self.assertAlmostEqual(feats["wkagg_cov"], 1.0, places=12)
        expected = (1.002 ** (31 / 7) - 1.0) * 100.0
        self.assertAlmostEqual(feats["wkagg_mtd"], expected, places=9)
        self.assertAlmostEqual(feats["wkagg_mtd_scaled"], expected, places=9)
        self.assertAlmostEqual(feats["wkagg_last"], 0.2, places=9)

    def test_partial_coverage_scales_by_covered_share(self):
        feats = wkagg_features(MARCH, MARCH_MONTH, CUTOFF_NO_LAST)
        self.assertAlmostEqual(feats["wkagg_cov"], 28 / 31, places=12)
        expected = (1.002 ** 4 - 1.0) * 100.0
        self.assertAlmostEqual(feats["wkagg_mtd"], expected, places=9)
        self.assertAlmostEqual(feats["wkagg_mtd_scaled"], expected / (28 / 31), places=9)

    def test_scaled_is_nan_below_half_coverage_but_mtd_is_kept(self):
        points = [(date(2022, 3, 7), 100.2)]  # предыдущей нет → период 7 дней
        feats = wkagg_features(points, MARCH_MONTH, CUTOFF_FULL)
        self.assertAlmostEqual(feats["wkagg_cov"], 7 / 31, places=12)
        self.assertTrue(is_nan(feats["wkagg_mtd_scaled"]))
        self.assertAlmostEqual(feats["wkagg_mtd"], 0.2, places=9)

    def test_no_available_points_gives_nan_not_zero(self):
        feats = wkagg_features(MARCH, MARCH_MONTH, date(2022, 2, 1))
        for name in ("wkagg_mtd", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w"):
            self.assertTrue(is_nan(feats[name]), name)
        self.assertEqual(feats["wkagg_cov"], 0.0)

    def test_last_is_the_last_available_point(self):
        points = [(d, 100.0 + i * 0.1) for i, (d, _) in enumerate(MARCH)]
        feats = wkagg_features(points, MARCH_MONTH, CUTOFF_FULL)
        self.assertAlmostEqual(feats["wkagg_last"], 0.5, places=9)
        feats = wkagg_features(points, MARCH_MONTH, CUTOFF_NO_LAST)
        self.assertAlmostEqual(feats["wkagg_last"], 0.4, places=9)

    def test_four_week_cumulative_over_last_28_days(self):
        points = [
            (date(2022, 2, 28), 100.0),
            (date(2022, 3, 7), 100.1),
            (date(2022, 3, 14), 100.2),
            (date(2022, 3, 21), 100.3),
            (date(2022, 3, 28), 100.4),
        ]
        feats = wkagg_features(points, MARCH_MONTH, CUTOFF_FULL)
        expected = (1.001 * 1.002 * 1.003 * 1.004 - 1.0) * 100.0
        self.assertAlmostEqual(feats["wkagg_4w"], expected, places=9)

    def test_four_week_is_nan_when_window_is_mostly_empty(self):
        points = [(date(2022, 3, 28), 100.4)]
        feats = wkagg_features(points, MARCH_MONTH, CUTOFF_FULL)
        self.assertTrue(is_nan(feats["wkagg_4w"]))


class WkaggLeakageTest(unittest.TestCase):
    """Признаки не видят точек, опубликованных после cutoff (спека §3, §8)."""

    def _features(self, points, cutoff):
        return wkagg_features(points, MARCH_MONTH, cutoff)

    def test_unavailable_point_value_does_not_change_features(self):
        changed = [(d, 150.0 if d == date(2022, 4, 4) else v) for d, v in MARCH]
        self.assertEqual(self._features(MARCH, CUTOFF_NO_LAST), self._features(changed, CUTOFF_NO_LAST))

    def test_available_point_value_changes_features(self):
        changed = [(d, 150.0 if d == date(2022, 4, 4) else v) for d, v in MARCH]
        self.assertNotEqual(self._features(MARCH, CUTOFF_FULL), self._features(changed, CUTOFF_FULL))

    def test_boundary_is_date_plus_two_days(self):
        before = self._features(MARCH, date(2022, 4, 5))
        edge = self._features(MARCH, date(2022, 4, 6))
        self.assertLess(before["wkagg_cov"], edge["wkagg_cov"])
        later = self._features(MARCH, date(2022, 4, 7))
        self.assertEqual(edge, later)  # новых доступных недель нет — признаки не меняются

    def test_dropping_every_point_after_cutoff_is_a_noop(self):
        truncated = [(d, v) for d, v in MARCH if d + timedelta(days=2) <= CUTOFF_NO_LAST]
        self.assertEqual(self._features(MARCH, CUTOFF_NO_LAST), self._features(truncated, CUTOFF_NO_LAST))

    def test_points_entirely_after_the_month_do_not_enter_mtd_or_cov(self):
        extra = MARCH + [(date(2022, 4, 11), 130.0)]  # период 5–11 апреля
        base = self._features(MARCH, CUTOFF_FULL)
        with_extra = self._features(extra, date(2022, 4, 20))
        self.assertEqual(base["wkagg_mtd"], with_extra["wkagg_mtd"])
        self.assertEqual(base["wkagg_cov"], with_extra["wkagg_cov"])

    def test_future_months_data_do_not_change_earlier_month_features(self):
        late = MARCH + mondays(date(2022, 4, 11), 20, 105.0)
        self.assertEqual(self._features(MARCH, CUTOFF_FULL), self._features(late, CUTOFF_FULL))


class TariffWeekTest(unittest.TestCase):
    def test_calendar_flag_by_month(self):
        for month in (date(2022, 7, 1), date(2023, 7, 1), date(2024, 7, 1), date(2025, 7, 1), date(2026, 10, 1)):
            self.assertEqual(wkagg_features([], month, month + timedelta(days=40))["tariff_week"], 1, month)
        for month in (date(2022, 3, 1), date(2022, 8, 1), date(2024, 10, 1), date(2020, 7, 1)):
            self.assertEqual(wkagg_features([], month, month + timedelta(days=40))["tariff_week"], 0, month)

    def test_january_first_week_counts(self):
        for year in (2010, 2022, 2026):
            month = date(year, 1, 1)
            self.assertEqual(wkagg_features([], month, month + timedelta(days=40))["tariff_week"], 1)

    def test_flag_does_not_depend_on_data_availability(self):
        month = date(2023, 7, 1)
        points = mondays(date(2023, 6, 26), 6, 100.1)
        self.assertEqual(
            wkagg_features(points, month, date(2023, 7, 5))["tariff_week"],
            wkagg_features(points, month, date(2023, 8, 8))["tariff_week"],
        )


def _row(month: date, **extra) -> "cm.Row":
    features = {"y_lag1": 0.3, "y_lag2": 0.2, "usd_mm": 1.0, "key_rate": 9.5, "fao_mm": 0.1, "month": month.month}
    features.update(extra)
    return cm.Row(month=month, y={"cpi_all": 0.4}, features=features)


class AttachAndNamesTest(unittest.TestCase):
    def test_sample_bounds_are_pinned(self):
        self.assertEqual(SAMPLE_BOUNDS_V4["S_W22"], (date(2022, 1, 1), date(2026, 8, 1)))
        self.assertEqual(SAMPLE_BOUNDS_V4["S_L13"], (date(2013, 10, 1), date(2026, 8, 1)))
        self.assertEqual(SAMPLE_BOUNDS_V4["S_L09"], (date(2009, 6, 1), date(2026, 8, 1)))

    def test_s_l09_drops_usd_and_key_rate_everywhere(self):
        names = feature_names_v4("S_L09")
        self.assertNotIn("usd_mm", names)
        self.assertNotIn("key_rate", names)
        for name in WKAGG_FEATURES:
            self.assertIn(name, names)
        self.assertEqual(names[-1], "month")

    def test_other_samples_keep_all_v2_features(self):
        for sample in ("S_W22", "S_L13"):
            names = feature_names_v4(sample)
            for name in ("usd_mm", "key_rate", "fao_mm", "y_lag1"):
                self.assertIn(name, names)
            self.assertEqual(names[-1], "month")

    def test_unknown_sample_raises(self):
        with self.assertRaises(ValueError):
            feature_names_v4("nope")

    def test_attach_adds_wkagg_and_drops_excluded_without_mutating_input(self):
        row = _row(MARCH_MONTH)
        out = attach_wkagg([row], MARCH, "S_L09")
        self.assertEqual(len(out), 1)
        self.assertNotIn("usd_mm", out[0].features)
        self.assertNotIn("key_rate", out[0].features)
        self.assertIn("usd_mm", row.features)
        for name in WKAGG_FEATURES:
            self.assertIn(name, out[0].features)
        self.assertEqual(out[0].y, row.y)
        self.assertEqual(out[0].month, row.month)

    def test_attach_uses_the_month_cutoff_of_cpi_monthly(self):
        row = _row(MARCH_MONTH)
        out = attach_wkagg([row], MARCH, "S_L13")[0].features
        expected = wkagg_features(MARCH, MARCH_MONTH, cm.cutoff_for(MARCH_MONTH))
        for name in WKAGG_FEATURES:
            self.assertEqual(out[name], expected[name])

    def test_attach_row_never_sees_points_after_its_cutoff(self):
        late = MARCH + mondays(date(2022, 4, 11), 30, 140.0)
        a = attach_wkagg([_row(MARCH_MONTH)], MARCH, "S_L13")[0].features
        b = attach_wkagg([_row(MARCH_MONTH)], late, "S_L13")[0].features
        for name in WKAGG_FEATURES:
            self.assertEqual(a[name], b[name], name)


class WkaggSummaryTest(unittest.TestCase):
    def test_counts_rows_nan_and_months_passing_cov_by_year(self):
        full = _row(date(2022, 3, 1), **wkagg_features(MARCH, MARCH_MONTH, CUTOFF_FULL))
        empty = _row(date(2021, 3, 1), **wkagg_features([], date(2021, 3, 1), date(2021, 4, 8)))
        summary = wkagg_summary([empty, full])
        self.assertEqual(summary["n_rows"], 2)
        self.assertEqual(summary["nan_counts"]["wkagg_mtd"], 1)
        self.assertEqual(summary["nan_counts"]["wkagg_mtd_scaled"], 1)
        self.assertEqual(summary["cov_ge_half_by_year"], {2021: 0, 2022: 1})
        self.assertEqual(summary["first"], "2021-03-01")
        self.assertEqual(summary["last"], "2022-03-01")


if __name__ == "__main__":
    unittest.main()
