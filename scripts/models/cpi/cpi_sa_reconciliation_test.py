#!/usr/bin/env python3
"""Офлайн-тесты сверки SA (#399): уровень из м/м, ряд без КУ, метрики,
решения по таблице §5 спеки и as-of без утечки через подменяемый мост X-13.

ClickHouse, R и сеть не используются. Запуск — `make cpi-sa-reconciliation-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_sa_reconciliation as rec


def months(start: date, n: int) -> list:
    out, year, month = [], start.year, start.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


class LevelTest(unittest.TestCase):
    def test_level_compounds_percent(self):
        grid = months(date(2020, 1, 1), 3)
        level = rec.level_from_mm(grid, {grid[0]: 1.0, grid[1]: 2.0, grid[2]: -1.0})
        self.assertAlmostEqual(level[0], 1.01)
        self.assertAlmostEqual(level[1], 1.01 * 1.02)
        self.assertAlmostEqual(level[2], 1.01 * 1.02 * 0.99)

    def test_gap_in_months_is_error(self):
        with self.assertRaises(ValueError):
            rec.level_from_mm([date(2020, 1, 1), date(2020, 3, 1)], {date(2020, 1, 1): 1.0, date(2020, 3, 1): 1.0})

    def test_missing_value_is_error(self):
        grid = months(date(2020, 1, 1), 2)
        with self.assertRaises(ValueError):
            rec.level_from_mm(grid, {grid[0]: 1.0})


class ExUtilitiesTest(unittest.TestCase):
    def test_identity(self):
        grid = months(date(2020, 1, 1), 2)
        out = rec.ex_utilities_mm({grid[0]: 1.0, grid[1]: 0.5}, {grid[0]: 0.0, grid[1]: 10.0}, 0.1)
        self.assertAlmostEqual(out[grid[0]], 1.0 / 0.9)
        self.assertAlmostEqual(out[grid[1]], (0.5 - 1.0) / 0.9)

    def test_weight_bounds(self):
        grid = months(date(2020, 1, 1), 1)
        for bad in (0.0, 1.0, -0.1, 1.5):
            with self.assertRaises(ValueError):
                rec.ex_utilities_mm({grid[0]: 1.0}, {grid[0]: 1.0}, bad)

    def test_months_must_match(self):
        with self.assertRaises(ValueError):
            rec.ex_utilities_mm({date(2020, 1, 1): 1.0}, {date(2020, 2, 1): 1.0}, 0.1)


class MetricsTest(unittest.TestCase):
    def setUp(self):
        self.grid = months(date(2016, 1, 1), 24)

    def test_perfect_match(self):
        ours = {m: float(i % 5) for i, m in enumerate(self.grid)}
        out = rec.metrics(ours, dict(ours), self.grid[0])
        self.assertEqual(out["n"], 24)
        self.assertAlmostEqual(out["rmse"], 0.0)
        self.assertAlmostEqual(out["corr"], 1.0)
        self.assertAlmostEqual(out["slope"], 1.0)

    def test_constant_shift_is_bias(self):
        ref = {m: float(i % 7) for i, m in enumerate(self.grid)}
        ours = {m: v + 0.2 for m, v in ref.items()}
        out = rec.metrics(ours, ref, self.grid[0])
        self.assertAlmostEqual(out["bias"], 0.2)
        self.assertAlmostEqual(out["rmse"], 0.2)
        self.assertAlmostEqual(out["rmse_cal"], 0.0, places=6)

    def test_window_and_nan(self):
        ref = {m: float(i % 3) for i, m in enumerate(self.grid)}
        ours = dict(ref)
        ours[self.grid[5]] = float("nan")
        out = rec.metrics(ours, ref, self.grid[3], self.grid[10])
        self.assertEqual(out["n"], 7)

    def test_too_few_points(self):
        with self.assertRaises(ValueError):
            rec.metrics({self.grid[0]: 1.0, self.grid[1]: 2.0}, {self.grid[0]: 1.0, self.grid[1]: 2.0}, self.grid[0])


class DecisionsTest(unittest.TestCase):
    def results(self, **over):
        base = {
            "x13_rt_vs_cbr_2016": {"rmse": 0.12, "corr": 0.98},
            "stl_rt_vs_cbr_2016": {"rmse": 0.22, "corr": 0.95},
            "nsa_vs_cbr_2016": {"rmse": 0.226, "corr": 0.95},
            "best_vs_core_2016": {"rmse": 0.207},
            "exku_vs_proxy_2023": {0.08: {"rmse": 0.164}, 0.10: {"rmse": 0.20}, 0.12: {"rmse": 0.246}},
            "weekly_vs_x13_2016": {"rmse": 0.147, "bias": -0.024, "corr": 0.978},
            "weekly_vs_x13_2010": {"rmse": 0.41, "bias": -0.27, "corr": 0.8},
        }
        base.update(over)
        return base

    def test_baseline_exploration_values(self):
        out = rec.decisions(self.results())
        self.assertTrue(out["x13_ok"])
        self.assertFalse(out["stl_ok"])
        self.assertTrue(out["core_not_from_headline"])
        self.assertTrue(out["replace_proxy"])
        self.assertTrue(out["weekly_ok_2016"])
        self.assertTrue(out["weekly_nan_before_2016"])

    def test_x13_fails_on_rmse(self):
        out = rec.decisions(self.results(x13_rt_vs_cbr_2016={"rmse": 0.16, "corr": 0.98}))
        self.assertFalse(out["x13_ok"])

    def test_x13_fails_on_corr(self):
        out = rec.decisions(self.results(x13_rt_vs_cbr_2016={"rmse": 0.12, "corr": 0.94}))
        self.assertFalse(out["x13_ok"])

    def test_stl_ok_only_if_better_than_nsa_and_below_threshold(self):
        out = rec.decisions(self.results(stl_rt_vs_cbr_2016={"rmse": 0.14, "corr": 0.96}))
        self.assertTrue(out["stl_ok"])
        out = rec.decisions(self.results(stl_rt_vs_cbr_2016={"rmse": 0.14, "corr": 0.96}, nsa_vs_cbr_2016={"rmse": 0.13}))
        self.assertFalse(out["stl_ok"])

    def test_proxy_kept_if_all_weights_close(self):
        small = {0.08: {"rmse": 0.05}, 0.10: {"rmse": 0.06}, 0.12: {"rmse": 0.07}}
        self.assertFalse(rec.decisions(self.results(exku_vs_proxy_2023=small))["replace_proxy"])

    def test_proxy_replaced_if_any_weight_far(self):
        mixed = {0.08: {"rmse": 0.05}, 0.10: {"rmse": 0.06}, 0.12: {"rmse": 0.10}}
        self.assertTrue(rec.decisions(self.results(exku_vs_proxy_2023=mixed))["replace_proxy"])

    def test_weekly_fails_on_bias(self):
        out = rec.decisions(self.results(weekly_vs_x13_2016={"rmse": 0.147, "bias": -0.06, "corr": 0.978}))
        self.assertFalse(out["weekly_ok_2016"])

    def test_weekly_pre2016_ok_means_no_nan_rule(self):
        out = rec.decisions(self.results(weekly_vs_x13_2010={"rmse": 0.15, "bias": -0.02, "corr": 0.97}))
        self.assertFalse(out["weekly_nan_before_2016"])


class AsOfTest(unittest.TestCase):
    """As-of: значение месяца t не зависит от будущих точек."""

    def fake_runner(self, levels, first_rt):
        # Детерминированная имитация: SA м/м месяца = м/м уровня минус среднее по истории до n.
        out = []
        for n in range(len(levels)):
            if n + 1 < first_rt or n == 0:
                out.append((float("nan"), float("nan")))
                continue
            history = [levels[i] / levels[i - 1] - 1 for i in range(1, n + 1)]
            mean = sum(history) / len(history)
            mm = levels[n] / levels[n - 1] - 1
            out.append((float("nan"), (mm - mean) * 100))
        return out

    def test_future_change_does_not_move_past_rt(self):
        grid = months(date(2015, 1, 1), 36)
        mm = {m: 0.5 + 0.1 * (i % 4) for i, m in enumerate(grid)}
        base = rec.x13_series(grid, mm, 24, self.fake_runner)
        mm2 = dict(mm)
        mm2[grid[-1]] = 9.0
        changed = rec.x13_series(grid, mm2, 24, self.fake_runner)
        for m in grid[:-1]:
            a, b = base["rt"][m], changed["rt"][m]
            self.assertTrue((math.isnan(a) and math.isnan(b)) or a == b, m)
        self.assertNotEqual(base["rt"][grid[-1]], changed["rt"][grid[-1]])

    def test_runner_result_length_checked(self):
        grid = months(date(2015, 1, 1), 5)
        mm = {m: 0.5 for m in grid}
        with self.assertRaises(ValueError):
            rec.x13_series(grid, mm, 3, lambda levels, first: [(1.0, 1.0)])


if __name__ == "__main__":
    unittest.main()
