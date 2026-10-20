#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/key_rate/key_rate_cpi_v3_experiment.py` (issue #296)."""

import sys
import unittest
from datetime import date
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_monthly as cm
from models.cpi import cpi_monthly_v3 as cmv3
from models.key_rate import key_rate_cpi_v3_experiment as exp


def cpi_item(h1=0.2, h3=0.6, h6=1.1):
    return {
        "cpi_v3_h1": h1,
        "cpi_v3_h3": h3,
        "cpi_v3_h6": h6,
        "cpi_v3_origin": date(2026, 8, 1),
    }


def summary_item(crps=48.0, coverage=0.8, rmse=130.0):
    return {
        "n": 72,
        "crps": crps,
        "coverage": {
            0.50: {"value": 0.68, "interval": (0.56, 0.78)},
            0.80: {"value": coverage, "interval": (0.70, 0.88)},
            0.90: {"value": 0.85, "interval": (0.75, 0.92)},
        },
        "width": {0.80: 150.0},
        "rmse": rmse,
        "mae": 55.0,
    }


class TestBuildExtra(unittest.TestCase):
    def setUp(self):
        self.day = date(2026, 9, 11)
        self.cpi = {self.day: cpi_item()}

    def test_baseline_is_none(self):
        self.assertIsNone(exp.build_extra([self.day], self.cpi, "baseline"))

    def test_h1_only(self):
        extra = exp.build_extra([self.day], self.cpi, "h1")[self.day]
        self.assertEqual(extra["cpi_v3_h1"], 0.2)
        self.assertEqual(extra["cpi_v3_available"], 1)
        self.assertNotIn("cpi_v3_h3", extra)

    def test_all_horizons(self):
        extra = exp.build_extra([self.day], self.cpi, "h1h3h6")[self.day]
        self.assertEqual(extra["cpi_v3_h3"], 0.6)
        self.assertEqual(extra["cpi_v3_h6"], 1.1)
        self.assertEqual(extra["cpi_v3_available"], 1)

    def test_h1_available_when_h3_missing(self):
        cpi = {self.day: cpi_item(h3=None, h6=None)}
        extra = exp.build_extra([self.day], cpi, "h1")[self.day]
        self.assertEqual(extra["cpi_v3_available"], 1)
        all_horizons = exp.build_extra([self.day], cpi, "h1h3h6")[self.day]
        self.assertEqual(all_horizons["cpi_v3_available"], 0)

    def test_replace_survey_removes_expectations(self):
        extra = exp.build_extra([self.day], self.cpi, "replace_survey")[self.day]
        for name in exp.SURVEY_FEATURES:
            self.assertIsNone(extra[name])
        self.assertEqual(extra["has_expectations"], 0)
        self.assertEqual(extra["cpi_v3_h3"], 0.6)


class TestDecide(unittest.TestCase):
    def _results(self, crps=48.0, coverage=0.8, rmse=130.0):
        return {
            "baseline": {"summary": {(exp.PRIMARY_FAN, exp.PRIMARY_WINDOW): summary_item(48.2, 0.778, 130.0)}},
            "h1h3h6": {"summary": {(exp.PRIMARY_FAN, exp.PRIMARY_WINDOW): summary_item(crps, coverage, rmse)}},
        }

    def _comparisons(self, low=-10.0, high=-1.0, diff=-2.0):
        return {
            "h1h3h6": {
                "n": 72,
                "crps_diff": diff,
                "crps_low": low,
                "crps_high": high,
                "crps_p": 0.04,
                "dm_p": 0.2,
            }
        }

    def test_promote(self):
        decision, _ = exp.decide(self._results(), self._comparisons())
        self.assertEqual(decision, "promote")

    def test_iterate_when_ci_includes_zero(self):
        decision, _ = exp.decide(self._results(), self._comparisons(high=1.0))
        self.assertEqual(decision, "iterate")

    def test_reject_when_worse(self):
        decision, _ = exp.decide(
            self._results(crps=50.0, coverage=0.6, rmse=150.0),
            self._comparisons(low=1.0, high=5.0, diff=2.0),
        )
        self.assertEqual(decision, "reject")


class TestCpiRanking(unittest.TestCase):
    def _rows(self):
        rows = {}
        for index in range(6):
            month = date(2020, index + 1, 1)
            features = {name: 0.0 for name in cmv3.feature_names_v3()}
            features["month"] = month.month
            rows[month] = cm.Row(
                month=month,
                y={
                    code: 0.2
                    for code in (*cm.BOTTOMUP_COMPONENTS, cm.TARGET_ALL)
                },
                features=features,
            )
        return rows

    def test_ranking_structure(self):
        rows = self._rows()
        with mock.patch.object(
            exp.cph, "forecast_origin", return_value=1.0
        ), mock.patch.object(
            exp, "forecast_direct", return_value=1.2
        ), mock.patch.object(
            exp, "forecast_b3", return_value=0.9
        ):
            ranking = exp.cpi_ranking(rows, [date(2020, 5, 1)])
        self.assertIn("h1", ranking["horizons"])
        self.assertEqual(
            ranking["horizons"]["h1"]["models"]["live3_bottomup"]["n"], 1
        )
        self.assertEqual(len(ranking["predictions"]), 4)


class TestReport(unittest.TestCase):
    def test_report_mentions_all_variants(self):
        results = {}
        for variant in exp.VARIANTS:
            results[variant] = {
                "summary": {
                    (exp.PRIMARY_FAN, exp.PRIMARY_WINDOW): summary_item()
                },
                "episodes": {},
            }
        ranking = {
            "horizons": {
                "h1": {
                    "n_origins": 1,
                    "models": {
                        "live3_bottomup": {"n": 1, "rmse": 0.2, "mae": 0.1}
                    },
                    "pairs": {},
                }
            },
            "predictions": [],
        }
        text = exp.render_report(
            {"as_of": date(2026, 10, 20), "rows": [object()]},
            {
                "meetings": 108,
                "h1": 100,
                "h3": 95,
                "h6": 90,
                "available": 90,
                "first_origin": date(2016, 4, 1),
                "last_origin": date(2026, 8, 1),
                "feature_last_origin": date(2026, 9, 1),
                "cpi_months": 155,
                "cpi_skipped": 0,
            },
            results,
            {"h1h3h6": {"n": 72, "crps_diff": -2.0, "crps_low": -10.0, "crps_high": -1.0, "crps_p": 0.04, "dm_p": 0.2}},
            ranking,
            "iterate",
            "тест",
        )
        for variant in exp.VARIANTS:
            self.assertIn(variant, text)
        self.assertIn("Task 4.1", text)
        self.assertIn("CPI-ranking", text)
        self.assertIn("live3_bottomup", text)


if __name__ == "__main__":
    unittest.main()
