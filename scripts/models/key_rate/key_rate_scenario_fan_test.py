#!/usr/bin/env python3
"""Офлайн-тесты `scripts/key_rate_scenario_fan.py`.

Запуск из корня репозитория:

    make key-rate-scenario-fan-test

или:

    uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" \
        --with scipy --python 3.12 python3 scripts/key_rate_scenario_fan_test.py -v

Набор офлайн: ClickHouse и сеть не нужны. Живой прогон —
`ML_LOG=1 make key-rate-scenario-fan`.
"""

import csv
import json
import math
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.key_rate import key_rate_cbr_rule as cr
from models.key_rate import key_rate_model as krm
from models.key_rate import key_rate_scenario_fan as fan
from models.key_rate import shock_classifier as sc


def make_decision(day: date, rate_before: float = 10.0, step_bp: int = 0):
    return krm.Decision(
        meeting_date=day,
        rate_before=rate_before,
        rate_after=rate_before + step_bp / 100.0,
        step_bp=step_bp,
        y3=(step_bp > 0) - (step_bp < 0),
    )


def make_row(
    day: date,
    rate_before: float = 10.0,
    step_bp: int = 0,
    e3_core=6.0,
    e3_headline=8.0,
    e3_consensus=7.0,
    gdp_gap=0.0,
    ibk_gap=0.0,
    r_star_trend=3.0,
):
    return cr.CbrRow(
        decision=make_decision(day, rate_before, step_bp),
        pi_saar=5.0,
        gap=0.0,
        e3_core=e3_core,
        e3_headline_adaptive=e3_headline,
        e3_headline_consensus=e3_consensus,
        gdp_gap=gdp_gap,
        ibk_gap=ibk_gap,
        r_star_trend=r_star_trend,
    )


def make_panel_row(day: date, emergency: bool = False):
    return sc.PanelRow(
        decision=make_decision(day),
        emergency=emergency,
        explicit_shock=False,
        features={},
        missing_count=0,
    )


class TestBudgetImpulse(unittest.TestCase):
    def _budget(self, months: int = 12) -> fan.BudgetPoint:
        income = {}
        deficit = {}
        for offset in range(months):
            key = fan._shift_month_key((2023, 1), offset)
            income[key] = 100.0
            deficit[key] = -10.0
        return fan.BudgetPoint(income=income, deficit=deficit)

    def test_full_year(self):
        budget = self._budget(12)
        value = fan.budget_impulse(budget, date(2024, 2, 15))
        self.assertAlmostEqual(value, 10.0)

    def test_lag_uses_previous_month(self):
        budget = self._budget(24)
        # 15.01.2024: декабрь 2023 ещё не доступен (конец M+1 = 31.01.2024),
        # поэтому 12-месячное окно — декабрь 2022 … ноябрь 2023.
        budget.income[(2022, 12)] = 100.0
        budget.deficit[(2022, 12)] = -10.0
        value = fan.budget_impulse(budget, date(2024, 1, 15))
        self.assertIsNotNone(value)
        self.assertAlmostEqual(value, 10.0)

    def test_missing_month(self):
        budget = self._budget(12)
        del budget.income[(2023, 6)]
        self.assertIsNone(fan.budget_impulse(budget, date(2024, 2, 15)))

    def test_nonpositive_income(self):
        budget = self._budget(12)
        for key in budget.income:
            budget.income[key] = 0.0
        self.assertIsNone(fan.budget_impulse(budget, date(2024, 2, 15)))

    def test_duplicate_month_raises(self):
        rows = [
            {"name": fan.BUDGET_INCOME, "date": date(2024, 1, 1), "value": 100.0},
            {"name": fan.BUDGET_INCOME, "date": date(2024, 1, 1), "value": 101.0},
        ]
        with self.assertRaises(krm.SampleError):
            fan.budget_from_rows(rows)


class TestScenarios(unittest.TestCase):
    def setUp(self):
        self.row = make_row(date(2024, 1, 1))

    def test_five_scenarios_and_names(self):
        steps = fan.scenario_steps(self.row, 2.0, 0.5)
        self.assertEqual(
            [item.name for item in steps],
            ["inertial", "rational", "hard", "budget", "no_market_loans"],
        )
        for item in steps:
            self.assertTrue(math.isfinite(item.components.step_bp))

    def test_hard_differs_from_base(self):
        steps = {item.name: item for item in fan.scenario_steps(self.row, 2.0, 0.0)}
        self.assertNotAlmostEqual(
            steps["hard"].components.step_bp,
            steps["inertial"].components.step_bp,
        )

    def test_rational_falls_back_without_consensus(self):
        row = make_row(date(2024, 1, 1), e3_consensus=None)
        steps = {item.name: item for item in fan.scenario_steps(row, 2.0, 0.0)}
        self.assertEqual(steps["rational"].fallback, "rational_fallback")
        self.assertAlmostEqual(
            steps["rational"].components.step_bp,
            steps["inertial"].components.step_bp,
        )

    def test_budget_falls_back_without_impulse(self):
        steps = {item.name: item for item in fan.scenario_steps(self.row, None, 0.0)}
        self.assertEqual(steps["budget"].fallback, "budget_fallback")
        self.assertAlmostEqual(
            steps["budget"].components.step_bp,
            steps["inertial"].components.step_bp,
        )

    def test_budget_and_nml_change_gap(self):
        row = make_row(date(2024, 1, 1), gdp_gap=1.0)
        base = {item.name: item for item in fan.scenario_steps(row, 2.0, 0.5)}
        self.assertGreater(
            base["budget"].components.gap,
            base["inertial"].components.gap,
        )
        self.assertLess(
            base["no_market_loans"].components.gap,
            base["inertial"].components.gap,
        )

    def test_missing_base_feature_raises(self):
        row = make_row(date(2024, 1, 1), gdp_gap=None)
        with self.assertRaises(krm.DegenerateWindowError):
            fan.scenario_steps(row, 2.0, 0.0)


class TestFans(unittest.TestCase):
    def setUp(self):
        self.row = make_row(date(2024, 1, 1))
        self.steps = fan.scenario_steps(self.row, 2.0, 0.5)

    def test_no_shock_reproduces_raw(self):
        fans = fan.build_fans(self.steps, [100.0], 0.0, 1.0, None, False)
        self.assertEqual(fans["fan_shock"], fans["fan_raw"])
        self.assertEqual(fans["fan_shock_weighted"], fans["fan_raw"])

    def test_zero_p_without_branch_reproduces_raw(self):
        fans = fan.build_fans(self.steps, [], 0.0, 1.0, None, False)
        self.assertEqual(fans["fan_shock"], fans["fan_raw"])
        self.assertEqual(fans["fan_shock_weighted"], fans["fan_raw"])

    def test_shock_weights_normalized(self):
        fans = fan.build_fans(self.steps, [100.0, -50.0], 0.2, 0.8, 0.0, False)
        for name in ("fan_raw", "fan_shock", "fan_shock_weighted"):
            self.assertAlmostEqual(sum(weight for _, weight in fans[name]), 1.0)
        self.assertEqual(len(fans["fan_shock"]), 7)

    def test_full_probability_uses_emergency_only(self):
        fans = fan.build_fans(self.steps, [100.0, -50.0], 1.0, 1.0, 0.0, False)
        self.assertEqual(len(fans["fan_shock"]), 2)
        self.assertAlmostEqual(sum(weight for _, weight in fans["fan_shock"]), 1.0)
        self.assertEqual(
            sorted(step for step, _ in fans["fan_shock"]), [-50.0, 100.0]
        )

    def test_weighted_with_unit_weight_equals_shock(self):
        fans = fan.build_fans(self.steps, [100.0, -50.0], 0.2, 1.0, 0.0, False)
        self.assertEqual(fans["fan_shock_weighted"], fans["fan_shock"])

    def test_ex_post_uses_emergency_branch(self):
        fans = fan.build_fans(self.steps, [100.0, -50.0], 0.0, 1.0, 0.0, True)
        self.assertEqual(
            sorted(step for step, _ in fans["fan_emergency_ex_post"]), [-50.0, 100.0]
        )

    def test_ex_post_without_emergency_uses_raw(self):
        fans = fan.build_fans(self.steps, [100.0], 0.0, 1.0, 0.0, False)
        self.assertEqual(fans["fan_emergency_ex_post"], fans["fan_raw"])


class TestMetrics(unittest.TestCase):
    def test_quantile_two_points_is_midpoint(self):
        self.assertAlmostEqual(fan.weighted_quantile([0.0, 10.0], [0.5, 0.5], 0.5), 5.0)

    def test_quantile_five_points(self):
        points = [0.0, 1.0, 2.0, 3.0, 4.0]
        weights = [0.2] * 5
        self.assertAlmostEqual(fan.weighted_quantile(points, weights, 0.5), 2.0)
        self.assertAlmostEqual(fan.weighted_quantile(points, weights, 0.0), 0.0)
        self.assertAlmostEqual(fan.weighted_quantile(points, weights, 1.0), 4.0)

    def test_crps_point_mass(self):
        self.assertAlmostEqual(fan.crps_weighted([5.0], [1.0], 7.0), 2.0)

    def test_crps_two_points(self):
        # |0−1|/2 + |10−1|/2 = 5; поправка 0.5·E|X−X'| = 0.5·5 = 2.5 → 2.5.
        self.assertAlmostEqual(
            fan.crps_weighted([0.0, 10.0], [0.5, 0.5], 1.0), 2.5
        )

    def test_class_probabilities(self):
        probs = fan.class_probabilities([-20.0, 0.0, 20.0], [1 / 3] * 3)
        for value in probs:
            self.assertAlmostEqual(value, 1 / 3)

    def test_wilson_interval(self):
        low, high = fan.wilson_interval(8, 10)
        self.assertLessEqual(low, 0.8)
        self.assertGreaterEqual(high, 0.8)

    def test_bootstrap_deterministic(self):
        first = fan.block_bootstrap_mean_diff([1.0] * 10, [2.0] * 10)
        second = fan.block_bootstrap_mean_diff([1.0] * 10, [2.0] * 10)
        self.assertEqual(first, second)
        self.assertAlmostEqual(first.mean_diff, -1.0)

    def test_fan_metrics_point(self):
        prediction = fan.FanPrediction(
            meeting_date=date(2024, 1, 1),
            window="expanding",
            fan="fan_raw",
            steps=((5.0, 1.0),),
            labels=("inertial",),
            actual_step_bp=5.0,
            emergency=False,
            p_shock=0.0,
            weight=1.0,
        )
        metrics = fan.fan_metrics(prediction)
        self.assertAlmostEqual(metrics["crps"], 0.0)
        self.assertAlmostEqual(metrics["median"], 5.0)
        self.assertTrue(all(metrics["coverage"][level] for level in fan.FAN_LEVELS))


class TestAlignment(unittest.TestCase):
    def test_matching_dates(self):
        rows = [make_row(date(2024, 1, 1)), make_row(date(2024, 2, 1))]
        panel = [make_panel_row(date(2024, 1, 1)), make_panel_row(date(2024, 2, 1))]
        aligned = fan.align_rows(rows, panel)
        self.assertEqual(set(aligned), {date(2024, 1, 1), date(2024, 2, 1)})

    def test_mismatch_raises(self):
        rows = [make_row(date(2024, 1, 1)), make_row(date(2024, 2, 1))]
        panel = [make_panel_row(date(2024, 1, 1))]
        with self.assertRaises(krm.SampleError):
            fan.align_rows(rows, panel)

    def test_shock_arrays_clip_and_none(self):
        panel = [make_panel_row(date(2024, 1, 1)), make_panel_row(date(2024, 2, 1))]
        with unittest.mock.patch.object(
            sc, "walk_forward_weights", return_value=[0.5, 1.0]
        ), unittest.mock.patch.object(
            sc, "walk_forward_classifier", return_value=[None, 0.9]
        ):
            weights, probabilities = fan.shock_arrays(panel)
        self.assertEqual(weights[date(2024, 1, 1)], 0.5)
        self.assertEqual(probabilities[date(2024, 1, 1)], 0.0)
        self.assertEqual(probabilities[date(2024, 2, 1)], fan.P_SHOCK_MAX)


class TestArtifacts(unittest.TestCase):
    def test_predictions_csv(self):
        prediction = fan.FanPrediction(
            meeting_date=date(2024, 1, 1),
            window="expanding",
            fan="fan_shock",
            steps=((5.0, 0.5), (7.0, 0.5)),
            labels=("inertial", "emergency_0"),
            actual_step_bp=6.0,
            emergency=True,
            p_shock=0.5,
            weight=0.8,
            fallbacks=("budget_fallback", ""),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.csv"
            fan.write_predictions(path, [prediction])
            with path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["scenario"], "inertial")
        self.assertEqual(rows[1]["scenario"], "emergency_0")
        self.assertEqual(rows[0]["fallback"], "budget_fallback")

    def test_diagnostics_csv(self):
        rows = [make_row(date(2024, 1, 1)), make_row(date(2024, 2, 1))]
        impulses = {rows[0].decision.meeting_date: 1.0, rows[1].decision.meeting_date: 2.0}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "diagnostics.csv"
            fan.write_diagnostics(path, rows, impulses)
            with path.open(encoding="utf-8") as handle:
                written = list(csv.DictReader(handle))
        self.assertEqual(len(written), 10)
        self.assertIn("budget_z", written[0])

    def test_forecast_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forecast.json"
            fan.write_forecast(path, {"as_of": "2026-10-20"})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"as_of": "2026-10-20"})
            fan.write_forecast(path, None)
            self.assertFalse(path.exists())


if __name__ == "__main__":
    unittest.main()
