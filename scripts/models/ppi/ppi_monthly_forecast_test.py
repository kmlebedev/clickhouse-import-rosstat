#!/usr/bin/env python3
"""Офлайн-тесты research-прогноза CPI/PPI по ценовым ожиданиям."""

import math
import sys
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.ppi import ppi_monthly_forecast as pm


class CalendarTest(unittest.TestCase):
    def test_shift_month_handles_year_and_leap_february(self):
        self.assertEqual(pm.shift_month(date(2025, 12, 1), 1), date(2026, 1, 1))
        self.assertEqual(pm.shift_month(date(2024, 1, 31), 1), date(2024, 2, 29))
        self.assertEqual(pm.shift_month(date(2024, 2, 29), -1), date(2024, 1, 29))

    def test_cutoff_is_eighth_of_next_month(self):
        self.assertEqual(
            pm.cutoff_for(date(2026, 8, 1)),
            datetime(2026, 9, 8, 23, 59, 59),
        )

    def test_publication_at_cutoff_is_not_available(self):
        origin = date(2026, 8, 1)
        cutoff = pm.cutoff_for(origin)
        values = {("price_expectations", date(2026, 7, 1)): 12.0}
        publications = [
            {"series": "price_expectations", "period_date": "2026-07-01",
             "published_at": cutoff, "quality": "exact_time"},
        ]
        self.assertEqual(
            pm.latest_survey_features(origin, values, publications, ("price_expectations",)),
            {},
        )
        publications[0]["published_at"] = cutoff - timedelta(seconds=1)
        features = pm.latest_survey_features(
            origin, values, publications, ("price_expectations",)
        )
        self.assertEqual(features["price_expectations_lag0"], 12.0)
        for lag in (1, 2, 3):
            self.assertTrue(math.isnan(features[f"price_expectations_lag{lag}"]))

    def test_publication_quality_is_preserved_in_audit(self):
        origin = date(2026, 8, 1)
        publications = [
            {"series": "price_expectations", "period_date": "2026-07-01",
             "published_at": "2026-08-15", "quality": "date"},
            {"series": "price_expectations", "period_date": "2026-06-01",
             "published_at": "2026-07-15", "quality": "fallback"},
        ]
        audit = pm.survey_publication_audit(origin, publications, ("price_expectations",))
        self.assertEqual([row["quality"] for row in audit], ["date", "fallback"])
        self.assertEqual(audit[0]["published_at"], datetime(2026, 8, 15, 23, 59, 59))
        self.assertEqual(audit[1]["published_at"], datetime(2026, 7, 31, 23, 59, 59))

    def test_missing_publication_row_fails_closed(self):
        values = {("price_expectations", date(2026, 7, 1)): 12.0}
        self.assertEqual(
            pm.latest_survey_features(
                date(2026, 8, 1), values, [], ("price_expectations",)
            ),
            {},
        )

    def test_missing_industry_calendar_fails_closed(self):
        values = {("industry_food_price_expectations", date(2026, 7, 1)): 8.0}
        features = pm.latest_survey_features(
            date(2026, 8, 1), values, [], ("industry_food_price_expectations",)
        )
        self.assertEqual(features, {})


class InputParsingTest(unittest.TestCase):
    @staticmethod
    def ppi_rows():
        return [
            {"series": series, "date": "2026-08-31", "mm_sa": 0.4}
            for series in ("total", "food", "nonfood", "core")
        ]

    def test_targets_normalize_month_and_convert_cpi_to_pp(self):
        cpi = [{"name": pm.CPI_ALL_NAME, "date": "2026-08-31", "percent": 100.7}]
        targets = pm.parse_targets(cpi, self.ppi_rows())
        self.assertEqual(set(targets), {"cpi_all", "ppi_total", "ppi_food", "ppi_nonfood", "ppi_core"})
        self.assertEqual(list(targets["cpi_all"]), [date(2026, 8, 1)])
        self.assertAlmostEqual(targets["cpi_all"][date(2026, 8, 1)], 0.7)
        self.assertEqual(targets["ppi_total"], {date(2026, 8, 1): 0.4})

    def test_duplicate_survey_point_is_rejected(self):
        rows = [
            {"series": "price_expectations", "date": "2026-08-01", "value": 1.0},
            {"series": "price_expectations", "date": "2026-08-31", "value": 2.0},
        ]
        with self.assertRaisesRegex(ValueError, "дубль"):
            pm.parse_survey_values(rows)

    def test_nonfinite_target_is_rejected(self):
        rows = self.ppi_rows()
        rows[0]["mm_sa"] = float("inf")
        with self.assertRaisesRegex(ValueError, "finite"):
            pm.parse_targets([], rows)

    def test_missing_required_target_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "cpi_all"):
            pm.parse_targets([], self.ppi_rows())


class DirectFoldTest(unittest.TestCase):
    @staticmethod
    def rows(count=48):
        return [
            pm.Row(
                target="cpi_all",
                origin=pm.shift_month(date(2020, 1, 1), index),
                actual=float(index),
                base_features={"y_lag1": float(index - 1)},
                expectation_features={"price_expectations_lag0": float(index)},
            )
            for index in range(count)
        ]

    def test_direct_fold_respects_horizon_and_embargo(self):
        rows = self.rows()
        for horizon in (1, 3, 6):
            with self.subTest(horizon=horizon):
                folds = pm.direct_folds(rows, horizon)
                first = folds[0]
                self.assertEqual(len(first.train), 36)
                self.assertEqual(first.test.origin, pm.shift_month(date(2020, 1, 1), 36 + horizon))
                self.assertEqual(
                    max(row.origin for row in first.train),
                    pm.shift_month(first.test.origin, -horizon - 1),
                )
                self.assertEqual(first.target_date, pm.shift_month(first.test.origin, horizon))
                self.assertGreater(len(folds[1].train), len(first.train))
                self.assertNotIn(first.test.origin, {row.origin for row in first.train})

    def test_pair_sample_starts_on_common_available_month(self):
        rows = self.rows()
        without_early_expectations = [
            pm.Row(row.target, row.origin, row.actual, row.base_features, {})
            for row in rows
        ]
        base_folds = pm.direct_folds(rows, 3)
        expectation_folds = pm.direct_folds(without_early_expectations, 3)
        self.assertEqual(
            [fold.test.origin for fold in base_folds],
            [fold.test.origin for fold in expectation_folds],
        )

    def test_direct_folds_reject_noncontiguous_months(self):
        rows = self.rows()
        del rows[20]
        with self.assertRaisesRegex(ValueError, "разрыв"):
            pm.direct_folds(rows, 1)


if __name__ == "__main__":
    unittest.main()
