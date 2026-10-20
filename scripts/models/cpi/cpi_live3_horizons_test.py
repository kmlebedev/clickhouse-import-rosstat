#!/usr/bin/env python3
"""Офлайн-тесты `scripts/cpi_live3_horizons.py` (issue #296)."""

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_live3_horizons as cph
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as cmv3


def make_row(month: date, value: float = 1.0, with_target: bool = True):
    features = {name: 0.0 for name in cmv3.feature_names_v3()}
    features["month"] = month.month
    y = (
        {code: value for code in (*cm.BOTTOMUP_COMPONENTS, cm.TARGET_ALL)}
        if with_target
        else {}
    )
    return cm.Row(month=month, y=y, features=features)


class TestTargets(unittest.TestCase):
    def test_cumulative_target_window(self):
        rows = {
            date(2024, 1, 1): make_row(date(2024, 1, 1), 1.0),
            date(2024, 2, 1): make_row(date(2024, 2, 1), 2.0),
            date(2024, 3, 1): make_row(date(2024, 3, 1), 3.0),
        }
        targets = cph.cumulative_targets(rows)
        self.assertAlmostEqual(
            targets[3][date(2024, 3, 1)][cm.TARGET_ALL],
            (1.01 * 1.02 * 1.03 - 1.0) * 100.0,
        )

    def test_cumulative_skips_incomplete_rows(self):
        rows = {
            date(2024, 1, 1): make_row(date(2024, 1, 1)),
            date(2024, 2, 1): make_row(date(2024, 2, 1), with_target=False),
            date(2024, 3, 1): make_row(date(2024, 3, 1)),
        }
        targets = cph.cumulative_targets(rows)
        self.assertNotIn(date(2024, 3, 1), targets[3])

    def test_training_origins_exclude_unobserved_target_end(self):
        months = [date(2024, month, 1) for month in range(1, 7)]
        origin = date(2024, 6, 1)
        # h=1: target end O'+1 <= O-1=May -> O' <= Apr.
        self.assertEqual(
            cph.training_origins(months, origin, 1)[-1], date(2024, 4, 1)
        )
        # h=3: target end O'+3 <= May -> O' <= Feb.
        self.assertEqual(
            cph.training_origins(months, origin, 3)[-1], date(2024, 2, 1)
        )

    def test_target_month_is_origin_plus_horizon(self):
        origin = date(2024, 6, 1)
        self.assertEqual(cph.shift_month(origin, 1), date(2024, 7, 1))
        self.assertEqual(cph.shift_month(origin, 3), date(2024, 9, 1))
        self.assertEqual(cph.shift_month(origin, 6), date(2024, 12, 1))


class TestAsOf(unittest.TestCase):
    def test_before_and_after_cutoff(self):
        origins = [date(2024, 4, 1), date(2024, 5, 1)]
        self.assertEqual(
            cph.asof_origin(date(2024, 6, 7), origins), date(2024, 4, 1)
        )
        self.assertEqual(
            cph.asof_origin(date(2024, 6, 8), origins), date(2024, 5, 1)
        )

    def test_none_early(self):
        self.assertIsNone(cph.asof_origin(date(2014, 1, 1), [date(2024, 1, 1)]))

    def test_latest_origin(self):
        self.assertEqual(cph.latest_origin(date(2026, 10, 7)), date(2026, 8, 1))
        self.assertEqual(cph.latest_origin(date(2026, 10, 8)), date(2026, 9, 1))
        self.assertEqual(cph.latest_origin(date(2026, 10, 20)), date(2026, 9, 1))


class TestForecast(unittest.TestCase):
    def test_live3_forecast_deterministic(self):
        months = [cph.shift_month(date(2020, 1, 1), index) for index in range(30)]
        rows = {
            month: make_row(month, 0.2 + 0.01 * index)
            for index, month in enumerate(months)
        }
        targets = cph.cumulative_targets(rows)
        first = cph.forecast_origin(rows, targets, months, months[-1], 1)
        second = cph.forecast_origin(rows, targets, months, months[-1], 1)
        self.assertIsNotNone(first)
        self.assertAlmostEqual(first, second)

    def test_min_train(self):
        months = [cph.shift_month(date(2020, 1, 1), index) for index in range(10)]
        rows = {month: make_row(month) for month in months}
        targets = cph.cumulative_targets(rows)
        self.assertIsNone(cph.forecast_origin(rows, targets, months, months[-1], 1))

    def test_feature_only_origin_is_predicted_not_trained(self):
        months = [cph.shift_month(date(2020, 1, 1), index) for index in range(30)]
        rows = {
            month: make_row(month, 0.2 + 0.01 * index)
            for index, month in enumerate(months)
        }
        rows[months[-1]] = make_row(months[-1], with_target=False)
        targets = cph.cumulative_targets(rows)
        seen = []

        def capture(train):
            seen.extend(item.month for item in train)
            return (1.0 / 3.0, 1.0 / 3.0, 1.0 / 3.0)

        with mock.patch.object(cph.cm, "nnls_weights", side_effect=capture), mock.patch.object(
            cph.cbc, "fit_component"
        ) as fit, mock.patch.object(cph.cbc, "predict", return_value=1.0):
            fit.return_value = (object(), ["month"])
            value = cph.forecast_origin(rows, targets, months, months[-1], 1)
        self.assertIsNotNone(value)
        self.assertGreaterEqual(fit.call_count, 1)
        self.assertNotIn(months[-1], seen)

    def test_no_lookahead_guard(self):
        months = [cph.shift_month(date(2020, 1, 1), index) for index in range(30)]
        rows = {month: make_row(month) for month in months}
        origin = months[-1]
        cph.assert_no_lookahead(rows, origin, 1)
        with mock.patch.object(cph, "training_origins", return_value=[origin]):
            with self.assertRaises(AssertionError):
                cph.assert_no_lookahead(rows, origin, 1)

    def test_build_features_uses_asof_origin(self):
        months = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        rows = {month: make_row(month) for month in months}
        forecasts = {
            1: {month: 0.1 for month in months},
            3: {month: 0.3 for month in months},
            6: {month: 0.6 for month in months},
        }
        with mock.patch.object(cph, "forecast_all", return_value=forecasts):
            features = cph.build_cpi_features(
                [date(2024, 3, 7), date(2024, 3, 8)], rows
            )
        self.assertEqual(features[date(2024, 3, 7)]["cpi_v3_origin"], date(2024, 1, 1))
        self.assertEqual(features[date(2024, 3, 8)]["cpi_v3_origin"], date(2024, 2, 1))
        self.assertEqual(features[date(2024, 3, 8)]["cpi_v3_h3"], 0.3)
        self.assertEqual(features[date(2024, 3, 8)]["cpi_v3_available"], 1)


if __name__ == "__main__":
    unittest.main()
