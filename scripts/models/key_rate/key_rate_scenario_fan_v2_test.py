#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/key_rate/key_rate_scenario_fan_v2.py`.

Запуск:

    make key-rate-scenario-fan-v2-test

или:

    uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn \
        --with scipy --python 3.12 python3 scripts/models/key_rate/key_rate_scenario_fan_v2_test.py -v

Набор офлайн: ClickHouse и сеть не нужны. Живой прогон —
`ML_LOG=1 make key-rate-scenario-fan-v2`.
"""

import csv
import json
import math
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.key_rate import key_rate_cbr_rule as cr
from models.key_rate import key_rate_model as krm
from models.key_rate import key_rate_scenario_fan as fan1
from models.key_rate import key_rate_scenario_fan_v2 as v2


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
    gdp_gap=1.0,
    ibk_gap=0.5,
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


def make_prediction(day: date, step_bp: float, actual_step_bp: float):
    return krm.Prediction(
        meeting_date=day,
        actual_step_bp=actual_step_bp,
        actual_y3=(actual_step_bp > 0) - (actual_step_bp < 0),
        method="ordered_logit",
        window="expanding",
        step_bp=step_bp,
        probs=(0.2, 0.6, 0.2),
    )


class TestCenteredDeltas(unittest.TestCase):
    def test_five_scenarios_sum_zero(self):
        row = make_row(date(2024, 1, 1))
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        self.assertEqual(len(steps), 5)
        self.assertEqual(len(deltas), 5)
        self.assertAlmostEqual(sum(deltas), 0.0, places=9)
        self.assertTrue(all(math.isfinite(value) for value in deltas))

    def test_hard_budget_nml_change_delta(self):
        row = make_row(date(2024, 1, 1), gdp_gap=1.0)
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        by_name = {step.name: delta for step, delta in zip(steps, deltas)}
        self.assertNotAlmostEqual(by_name["hard"], 0.0)
        self.assertNotAlmostEqual(by_name["budget"], 0.0)
        self.assertNotAlmostEqual(by_name["no_market_loans"], 0.0)

    def test_rational_fallback(self):
        row = make_row(date(2024, 1, 1), e3_consensus=None)
        steps, _ = v2.centered_deltas(row, 2.0, 0.5)
        rational = next(step for step in steps if step.name == "rational")
        self.assertEqual(rational.fallback, "rational_fallback")

    def test_budget_fallback(self):
        row = make_row(date(2024, 1, 1))
        steps, _ = v2.centered_deltas(row, None, 0.0)
        budget = next(step for step in steps if step.name == "budget")
        self.assertEqual(budget.fallback, "budget_fallback")


class TestDispersion(unittest.TestCase):
    def setUp(self):
        self.residuals = [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]

    def test_global_monotone(self):
        dispersion = v2.disp_global(self.residuals)
        self.assertEqual(len(dispersion.quantiles), len(v2.ALPHAS))
        self.assertEqual(dispersion.quantiles, tuple(sorted(dispersion.quantiles)))

    def test_scale_positive_and_coefficients(self):
        dispersion = v2.disp_scale(
            self.residuals,
            [0.5] * len(self.residuals),
            [0.1] * len(self.residuals),
            0.2,
            0.25,
        )
        self.assertGreater(dispersion.scale, 0.0)
        self.assertEqual(len(dispersion.coefficients), 3)
        self.assertEqual(dispersion.quantiles, tuple(sorted(dispersion.quantiles)))

    def test_scale_zero_residuals_falls_back(self):
        with self.assertRaises(krm.DegenerateWindowError):
            v2.disp_scale(
                [0.0] * 12,
                [1.0] * 12,
                [0.0] * 12,
                1.0,
                0.0,
            )

    def test_conformal_index_and_monotone(self):
        dispersion = v2.disp_conformal(self.residuals)
        self.assertEqual(dispersion.quantiles, tuple(sorted(dispersion.quantiles)))
        self.assertEqual(dispersion.quantiles[0], -10.0)
        self.assertEqual(dispersion.quantiles[-1], 45.0)

    def test_short_train_raises(self):
        with self.assertRaises(krm.DegenerateWindowError):
            v2.disp_global([1.0, 2.0])


class TestRegime(unittest.TestCase):
    def test_shock_regime_threshold(self):
        self.assertTrue(v2.shock_regime(True, 0.9))
        self.assertTrue(v2.shock_regime(False, 0.4))
        self.assertFalse(v2.shock_regime(False, 0.5))

    def test_conformal_regime_uses_shock_subset(self):
        residuals = [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        past = [True] * 6 + [False] * 6
        dispersion = v2.disp_conformal_regime(residuals, past, True)
        self.assertEqual(dispersion.quantiles[0], -10.0)
        self.assertEqual(dispersion.quantiles[-1], 15.0)
        self.assertEqual(dispersion.fallback, "")

    def test_conformal_regime_uses_normal_subset(self):
        residuals = [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        past = [True] * 6 + [False] * 6
        dispersion = v2.disp_conformal_regime(residuals, past, False)
        self.assertEqual(dispersion.quantiles[0], 20.0)
        self.assertEqual(dispersion.quantiles[-1], 45.0)

    def test_regime_small_subset_fallback(self):
        residuals = [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        past = [True] * 5 + [False] * 7
        dispersion = v2.disp_conformal_regime(residuals, past, True)
        self.assertEqual(dispersion.fallback, "shock_subset_small")
        self.assertEqual(dispersion.quantiles[0], -10.0)

    def test_global_regime_uses_shock_subset(self):
        residuals = [-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0]
        past = [True] * 6 + [False] * 6
        dispersion = v2.disp_global_regime(residuals, past, True)
        self.assertLess(dispersion.quantiles[-1], 20.0)
        self.assertGreater(dispersion.quantiles[0], -20.0)


class TestFan(unittest.TestCase):
    def test_thirty_five_points_and_weights(self):
        row = make_row(date(2024, 1, 1))
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        dispersion = v2.disp_global([-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0])
        fan = v2.make_fan(
            "core_fan_global",
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
        self.assertEqual(len(fan.fallbacks), 35)
        self.assertAlmostEqual(sum(weight for _, weight in fan.steps), 1.0)
        metrics = fan1.fan_metrics(fan)
        self.assertTrue(math.isfinite(metrics["crps"]))
        self.assertTrue(math.isfinite(metrics["median"]))

    def test_labels_include_scenario_and_quantile(self):
        row = make_row(date(2024, 1, 1))
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        dispersion = v2.disp_global([-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0])
        fan = v2.make_fan(
            "core_fan_global",
            date(2024, 1, 1),
            "expanding",
            10.0,
            steps,
            deltas,
            dispersion,
            12.0,
            False,
            0.0,
            1.0,
        )
        self.assertEqual(fan.labels[0], "inertial|q0.05")
        self.assertEqual(fan.labels[-1], "no_market_loans|q0.95")


class TestCore(unittest.TestCase):
    def test_core_history_only_past(self):
        day = date(2024, 6, 1)
        core = {
            date(2024, 1, 1): make_prediction(date(2024, 1, 1), 5.0, 7.0),
            date(2024, 2, 1): make_prediction(date(2024, 2, 1), -3.0, -1.0),
            day: make_prediction(day, 100.0, 100.0),
        }
        past, residuals = v2.core_history(core, day)
        self.assertEqual([item.meeting_date for item in past], [date(2024, 1, 1), date(2024, 2, 1)])
        self.assertEqual(residuals, [2.0, 2.0])

    def test_enet_core_deterministic(self):
        rows = [
            make_row(date(2024, 1, 1) + timedelta(days=30 * index), step_bp=(index % 5) - 2)
            for index in range(20)
        ]
        target = make_row(date(2024, 12, 1), step_bp=0)
        first = v2.fit_enet_core(rows, target)
        second = v2.fit_enet_core(rows, target)
        self.assertAlmostEqual(first.step_bp, second.step_bp)
        self.assertTrue(math.isfinite(first.step_bp))


class TestDecide(unittest.TestCase):
    def _summary(self, coverage=0.80, crps=60.0, rmse=150.0):
        return {
            (v2.PRIMARY_FAN, v2.PRIMARY_WINDOW): {
                "coverage": {
                    0.80: {
                        "value": coverage,
                        "interval": (0.70, 0.90),
                    }
                },
                "crps": crps,
                "rmse": rmse,
            }
        }

    def _comparisons(self, task1_diff=-50.0, task1_high=-10.0):
        return {
            f"{v2.PRIMARY_FAN}_vs_task1__{v2.PRIMARY_WINDOW}": {
                "crps_diff": task1_diff,
                "crps_low": -80.0,
                "crps_high": task1_high,
            },
            f"{v2.PRIMARY_FAN}_vs_ordered_logit__{v2.PRIMARY_WINDOW}": {
                "bench_crps": 77.0,
                "bench_rmse": 155.0,
            },
        }

    def test_promote(self):
        decision, _ = v2.decide(self._summary(), self._comparisons())
        self.assertEqual(decision, "promote")

    def test_iterate_when_coverage_bad(self):
        decision, _ = v2.decide(self._summary(coverage=0.50), self._comparisons())
        self.assertEqual(decision, "iterate")

    def test_reject_when_no_improvement(self):
        decision, _ = v2.decide(
            self._summary(coverage=0.50, crps=90.0, rmse=200.0),
            self._comparisons(task1_diff=10.0, task1_high=50.0),
        )
        self.assertEqual(decision, "reject")


class TestArtifacts(unittest.TestCase):
    def test_diagnostics_csv(self):
        diagnostic = v2.DiagnosticRow(
            meeting_date=date(2024, 1, 1),
            window="expanding",
            core="ordered_logit",
            core_step=10.0,
            probs=(0.1, 0.8, 0.1),
            scale=5.0,
            quantiles=(-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0),
            deltas=(-3.0, 1.0, 0.0, 1.0, 1.0),
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "diagnostics.csv"
            v2.write_diagnostics(path, [diagnostic])
            with path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["core"], "ordered_logit")
        self.assertEqual(rows[0]["delta_inertial"], "-3.000000")

    def test_predictions_csv(self):
        row = make_row(date(2024, 1, 1))
        steps, deltas = v2.centered_deltas(row, 2.0, 0.5)
        dispersion = v2.disp_global([-10.0, -5.0, 0.0, 5.0, 10.0, 15.0, 20.0, 25.0, 30.0, 35.0, 40.0, 45.0])
        fan = v2.make_fan(
            "core_fan_global",
            date(2024, 1, 1),
            "expanding",
            10.0,
            steps,
            deltas,
            dispersion,
            12.0,
            False,
            0.0,
            1.0,
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.csv"
            fan1.write_predictions(path, [fan])
            with path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(len(rows), 35)
        self.assertEqual(rows[0]["scenario"], "inertial|q0.05")

    def test_forecast_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "forecast.json"
            fan1.write_forecast(path, {"as_of": "2026-10-20"})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["as_of"], "2026-10-20")


if __name__ == "__main__":
    unittest.main()
