#!/usr/bin/env python3
"""Офлайн-тесты `scripts/key_rate_scenario_fan_v3.py`.

Запуск:

    make key-rate-scenario-fan-v3-test

или:

    uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn \
        --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_v3_test.py -v

Набор офлайн: ClickHouse и сеть не нужны. Живой прогон —
`ML_LOG=1 make key-rate-scenario-fan-v3`.
"""

import math
import sys
import tempfile
import unittest
from collections import Counter
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import key_rate_cbr_rule as cr
import key_rate_model as krm
import key_rate_scenario_fan as fan1
import key_rate_scenario_fan_v2 as v2
import key_rate_scenario_fan_v3 as v3


def make_decision(day: date, rate_before: float = 10.0, step_bp: int = 0):
    return krm.Decision(
        meeting_date=day,
        rate_before=rate_before,
        rate_after=rate_before + step_bp / 100.0,
        step_bp=step_bp,
        y3=(step_bp > 0) - (step_bp < 0),
    )


def make_row(day: date, step_bp: int = 0):
    return cr.CbrRow(
        decision=make_decision(day, step_bp=step_bp),
        pi_saar=5.0,
        gap=0.0,
        e3_core=6.0,
        e3_headline_adaptive=8.0,
        e3_headline_consensus=7.0,
        gdp_gap=1.0,
        ibk_gap=0.5,
        r_star_trend=3.0,
    )


def external_fixture() -> dict:
    return {
        "savings": {
            v3.DEPOSITS_SERIES: [
                (date(2023, 1, 1), 100.0),
                (date(2024, 1, 1), 120.0),
            ],
            v3.CONSUMER_SERIES: [
                (date(2023, 1, 1), 50.0),
                (date(2024, 1, 1), 60.0),
            ],
            v3.MORTGAGE_SERIES: [
                (date(2023, 1, 1), 30.0),
                (date(2024, 1, 1), 40.0),
            ],
        },
        "ofz": {
            "ofz10_spread": [(date(2024, 3, 1), 1.1)],
            "ofz2_spread": [(date(2024, 3, 1), 0.4)],
            "slope_10y_1y": [(date(2024, 3, 1), 0.7)],
        },
        "brent": [
            (date(2023, 12, 1), 80.0),
            (date(2024, 3, 2), 88.0),
        ],
        "bop": [(date(2023, 9, 30), 2.5)],
        "ccyb": {
            "credit_gap_broad": [(date(2023, 12, 1), 3.2)],
            "hh_yoy": [(date(2023, 12, 1), 12.0)],
            "nfo_rub_yoy": [(date(2023, 12, 1), 18.0)],
        },
        "text": {date(2024, 2, 16): 0.6},
    }


class TestAsOf(unittest.TestCase):
    def test_monthly_asof_lag(self):
        points = [(date(2024, 1, 1), 100.0), (date(2024, 3, 1), 130.0)]
        self.assertIsNone(v3.monthly_asof(points, date(2024, 2, 15)))
        self.assertEqual(v3.monthly_asof(points, date(2024, 3, 5)), 100.0)
        self.assertEqual(v3.monthly_asof(points, date(2024, 5, 1)), 130.0)

    def test_monthly_yoy(self):
        points = [(date(2023, 1, 1), 100.0), (date(2024, 1, 1), 110.0)]
        self.assertAlmostEqual(v3.monthly_yoy(points, date(2024, 3, 1)), 10.0)

    def test_daily_asof_and_change(self):
        points = [
            (date(2023, 12, 1), 100.0),
            (date(2024, 1, 1), 105.0),
            (date(2024, 4, 1), 110.0),
        ]
        self.assertEqual(v3.daily_asof(points, date(2024, 3, 31)), 105.0)
        self.assertAlmostEqual(
            v3.daily_change_3m(points, date(2024, 4, 1)), 100.0 * math.log(110.0 / 105.0)
        )

    def test_quarterly_asof_gdp_lag(self):
        points = [(date(2024, 3, 31), 1.5)]
        self.assertIsNone(v3.quarterly_asof(points, date(2024, 6, 29)))
        self.assertEqual(v3.quarterly_asof(points, date(2024, 6, 30)), 1.5)

    def test_text_asof_meeting_plus_14(self):
        scores = {date(2024, 1, 1): 0.3, date(2024, 2, 1): 0.7}
        self.assertIsNone(v3.text_asof(scores, date(2024, 1, 14)))
        self.assertEqual(v3.text_asof(scores, date(2024, 1, 15)), 0.3)
        self.assertEqual(v3.text_asof(scores, date(2024, 2, 16)), 0.7)


class TestChannelFeatures(unittest.TestCase):
    def setUp(self):
        self.day = date(2024, 3, 20)
        self.base = {
            "rate_before": 10.0,
            "pi_saar": 5.0,
            "cpi_saar": 6.0,
            "infl_exp_expected": 0.0,
            "infl_exp_observed": 8.0,
            "unemployment": 3.0,
            "wage_yoy": 9.0,
            "credit_yoy": 12.0,
            "m2_yoy": 11.0,
            "budget_balance": -2.0,
            "usdrub_3m": 1.5,
            "ruania_spread": 0.2,
            "price_expectations": None,
            "business_climate": 4.0,
        }

    def test_all_external_channels_present(self):
        features = v3.channel_features(
            make_decision(self.day), dict(self.base), external_fixture()
        )
        for name in v3.EXTERNAL_KEYS:
            self.assertIn(name, features)
        self.assertAlmostEqual(features["deposit_yoy"], 20.0)
        self.assertAlmostEqual(features["deposit_share"], 120.0 / 220.0 * 100.0)
        self.assertEqual(features["ofz10_spread"], 1.1)
        self.assertEqual(features["slope_10y_1y"], 0.7)
        self.assertAlmostEqual(features["brent_3m"], 100.0 * math.log(1.1))
        self.assertEqual(features["bop_ca_q"], 2.5)
        self.assertEqual(features["credit_gap_broad"], 3.2)
        self.assertEqual(features["text_channel_score"], 0.6)
        self.assertEqual(features["text_available"], 1)
        self.assertEqual(features["has_savings"], 1)
        self.assertEqual(features["has_ccyb"], 1)
        self.assertEqual(features["has_text"], 1)
        self.assertEqual(features["has_bop"], 1)

    def test_zero_expectation_is_not_missing(self):
        features = v3.channel_features(
            make_decision(self.day), dict(self.base), external_fixture()
        )
        self.assertEqual(features["has_expectations"], 1)

    def test_missing_external_channels_are_flagged(self):
        empty = {
            "savings": {
                v3.DEPOSITS_SERIES: [],
                v3.CONSUMER_SERIES: [],
                v3.MORTGAGE_SERIES: [],
            },
            "ofz": {"ofz10_spread": [], "ofz2_spread": [], "slope_10y_1y": []},
            "brent": [],
            "bop": [],
            "ccyb": {},
            "text": {},
        }
        base = dict(self.base)
        base["infl_exp_expected"] = None
        features = v3.channel_features(make_decision(self.day), base, empty)
        self.assertIsNone(features["deposit_yoy"])
        self.assertIsNone(features["deposit_share"])
        self.assertIsNone(features["ofz10_spread"])
        self.assertIsNone(features["brent_3m"])
        self.assertIsNone(features["bop_ca_q"])
        self.assertIsNone(features["credit_gap_broad"])
        self.assertIsNone(features["text_channel_score"])
        self.assertEqual(features["has_expectations"], 0)
        self.assertEqual(features["has_savings"], 0)
        self.assertEqual(features["has_ccyb"], 0)
        self.assertEqual(features["has_text"], 0)
        self.assertEqual(features["has_bop"], 0)


class TestDispersion(unittest.TestCase):
    def setUp(self):
        self.residuals = [
            -12.0,
            -9.0,
            -6.0,
            -3.0,
            0.0,
            2.0,
            4.0,
            6.0,
            8.0,
            10.0,
            12.0,
            15.0,
            18.0,
            21.0,
        ]
        self.vols = [0.1 * index for index in range(len(self.residuals))]

    def test_scale_positive_and_monotone(self):
        dispersion = v3.disp_vol(self.residuals, self.vols, 0.8)
        self.assertGreater(dispersion.scale, 0.0)
        self.assertEqual(dispersion.quantiles, tuple(sorted(dispersion.quantiles)))
        self.assertEqual(len(dispersion.coefficients), 2)

    def test_high_vol_widens(self):
        low = v3.disp_vol(self.residuals, self.vols, -1.0)
        high = v3.disp_vol(self.residuals, self.vols, 2.0)
        self.assertGreater(high.scale, low.scale)
        self.assertGreater(
            high.quantiles[-1] - high.quantiles[0],
            low.quantiles[-1] - low.quantiles[0],
        )

    def test_none_vols_are_filtered(self):
        vols = list(self.vols)
        vols[0] = None
        vols[5] = None
        dispersion = v3.disp_vol(self.residuals, vols, 0.8)
        self.assertGreater(dispersion.scale, 0.0)

    def test_too_few_valid_vols_raises(self):
        vols = [None] * (len(self.residuals) - v3.MIN_RESIDUALS + 1) + [0.1] * (
            v3.MIN_RESIDUALS - 1
        )
        with self.assertRaises(krm.DegenerateWindowError):
            v3.disp_vol(self.residuals, vols, 0.8)

    def test_none_current_vol_raises(self):
        with self.assertRaises(krm.DegenerateWindowError):
            v3.disp_vol(self.residuals, self.vols, None)

    def test_length_mismatch_raises(self):
        with self.assertRaises(ValueError):
            v3.disp_vol(self.residuals, self.vols[:-1], 0.8)

    def test_calibrated_factor_bounds(self):
        intervals_hit = [(0.0, -1.0, 1.0)] * 12
        intervals_miss = [(9.0, -1.0, 1.0)] * 12
        intervals_ok = [(0.0, -1.0, 1.0)] * 8 + [(9.0, -1.0, 1.0)] * 2
        hit = v3.disp_calibrated(self.residuals, intervals_hit)
        miss = v3.disp_calibrated(self.residuals, intervals_miss)
        ok = v3.disp_calibrated(self.residuals, intervals_ok)
        self.assertAlmostEqual(hit.scale, 0.8)
        self.assertAlmostEqual(miss.scale, 1.5)
        self.assertAlmostEqual(ok.scale, 1.0)
        self.assertEqual(hit.name, "disp_calibrated")

    def test_calibrated_without_history_is_conformal(self):
        base = v2.disp_conformal(self.residuals)
        dispersion = v3.disp_calibrated(self.residuals, [])
        self.assertEqual(dispersion.quantiles, base.quantiles)
        self.assertEqual(dispersion.scale, 1.0)


class TestCore(unittest.TestCase):
    def _features(self):
        return tuple(
            ["rate_before", "pi_saar", "gap", "e3_core", "deposit_yoy", "has_savings"]
        )

    def _row(self, index: int):
        decision = make_decision(date(2024, 1, 1), step_bp=(index % 5) - 2)
        row = v3.ChannelRow(
            decision=decision,
            features={
                "rate_before": 10.0 - 0.1 * index,
                "pi_saar": 4.0 + 0.2 * (index % 7),
                "gap": -1.0 + 0.3 * (index % 5),
                "e3_core": 5.0 + 0.1 * index,
                "deposit_yoy": 10.0 + (index % 6),
                "has_savings": 1,
            },
        )
        return row

    def test_enet_deterministic_and_finite(self):
        train = [self._row(index) for index in range(24)]
        target = self._row(30)
        fit = v3.fit_enet(self._features())
        first = fit(train, target)
        second = fit(train, target)
        self.assertAlmostEqual(first.step_bp, second.step_bp)
        self.assertTrue(math.isfinite(first.step_bp))
        self.assertIsNone(first.probs)

    def test_enet_handles_missing_values(self):
        train = [self._row(index) for index in range(24)]
        train[3].features["deposit_yoy"] = None
        target = self._row(30)
        target.features["deposit_yoy"] = None
        forecast = v3.fit_enet(self._features())(train, target)
        self.assertTrue(math.isfinite(forecast.step_bp))

    def test_degenerate_window_raises(self):
        train = [self._row(index) for index in range(5)]
        with self.assertRaises(krm.DegenerateWindowError):
            v3.fit_enet(self._features())(train, self._row(6))


class TestFan(unittest.TestCase):
    def test_thirty_five_points_and_weights(self):
        row = make_row(date(2024, 1, 1))
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        dispersion = v2.disp_conformal(
            [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        )
        fan = v2.make_fan(
            "channel_fan_conformal",
            date(2024, 1, 1),
            "expanding",
            10.0,
            steps,
            deltas,
            dispersion,
            12.0,
            False,
            0.1,
            0.9,
        )
        self.assertEqual(len(fan.steps), 35)
        self.assertEqual(len(fan.labels), 35)
        self.assertAlmostEqual(sum(weight for _, weight in fan.steps), 1.0)
        metrics = fan1.fan_metrics(fan)
        self.assertTrue(math.isfinite(metrics["crps"]))


class TestDecide(unittest.TestCase):
    def _summary(self, coverage=0.80, crps=55.0, rmse=120.0):
        return {
            (v3.PRIMARY_FAN, v3.PRIMARY_WINDOW): {
                "coverage": {0.80: {"value": coverage, "interval": (0.70, 0.90)}},
                "crps": crps,
                "rmse": rmse,
            }
        }

    def _comparisons(self, diff=-6.0, high=-1.0):
        return {
            "primary_vs_reference": {
                "crps_diff": diff,
                "crps_low": -20.0,
                "crps_high": high,
            },
            "primary_vs_ordered_logit": {
                "bench_crps": 70.0,
                "bench_rmse": 150.0,
            },
        }

    def test_promote(self):
        decision, _ = v3.decide(self._summary(), self._comparisons())
        self.assertEqual(decision, "promote")

    def test_iterate_when_coverage_bad(self):
        decision, _ = v3.decide(self._summary(coverage=0.50), self._comparisons())
        self.assertEqual(decision, "iterate")

    def test_reject_when_no_improvement(self):
        decision, _ = v3.decide(
            self._summary(coverage=0.50, crps=90.0, rmse=200.0),
            self._comparisons(diff=5.0, high=10.0),
        )
        self.assertEqual(decision, "reject")

    def test_reject_without_primary(self):
        decision, _ = v3.decide({}, {})
        self.assertEqual(decision, "reject")


class TestReport(unittest.TestCase):
    def _render(self, current):
        return v3.render_report(
            date(2026, 10, 20),
            [make_row(date(2024, 1, 1))],
            {},
            {},
            {},
            Counter(),
            "reject",
            "нет данных",
            current,
        )

    def test_plan_link_and_current_section(self):
        text = self._render({"skipped": "нет входов as-of"})
        self.assertIn("2026-10-20-cbr-scenario-fan-v3.md", text)
        self.assertNotIn("v3-plan.md", text)
        self.assertIn("## 7. Текущий прогноз", text)
        self.assertIn("Пропущен: нет входов as-of", text)

    def test_current_forecast_numbers(self):
        current = {
            "as_of": "2026-10-20",
            "meeting_date": "2026-10-23",
            "core": "channel_enet",
            "core_step": 12.5,
            "dispersion": "disp_vol",
            "scale": 1.2,
            "fan_quantiles": {"0.1": -25.0, "0.5": 12.5, "0.9": 50.0},
            "weight": 0.9,
            "p_shock": 0.1,
        }
        text = self._render(current)
        self.assertIn("2026-10-23", text)
        self.assertIn("disp_vol", text)
        self.assertIn("50.00", text)


class TestForecastArtifact(unittest.TestCase):
    def test_write_forecast(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / v3.FORECAST_NAME
            fan1.write_forecast(path, {"as_of": "2026-10-20"})
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()
