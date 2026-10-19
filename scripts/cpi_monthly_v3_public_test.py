#!/usr/bin/env python3
"""Офлайн-тесты `cpi_monthly_v3_public.py`: без ClickHouse, сети и XLSX."""

import dataclasses
import math
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_monthly as cm
import cpi_monthly_industry as ci
import cpi_monthly_v3 as cmv3
import cpi_monthly_v3_public as m


def make_features(month: date) -> dict:
    features = {name: 0.0 for name in cm.feature_names(with_mp=True)}
    features.update({name: float("nan") for name in cmv3.WEEKLY_FEATURES})
    features.update(
        {name: float("nan") for name in ci.industry_feature_names()}
    )
    features["month"] = month.month
    return features


def make_row(month: date, value: float = 1.0) -> cm.Row:
    return cm.Row(
        month=month,
        y={code: value for code in cm.TARGET_SERIES},
        features=make_features(month),
    )


def make_rows(count: int = 10, start: date = date(2022, 1, 1)) -> list:
    return [
        make_row(cm._month_shift(start, offset), float(offset + 1))
        for offset in range(count)
    ]


def synthetic_panel(points_per_series: int = 2) -> dict:
    """48 рядов × points_per_series; даты — первые/последние из спеки."""
    dates = [date(2002, 1, 1), date(2026, 9, 1)]
    dates = dates[:points_per_series]
    panel = {
        sheet: {key: [] for key in ci.INDICATOR_KEYS}
        for sheet in ci.INDUSTRY_SHEETS
    }
    for sheet in ci.INDUSTRY_SHEETS:
        for key in ci.INDICATOR_KEYS:
            panel[sheet][key] = [
                (day, float(index + 1)) for index, day in enumerate(dates)
            ]
    return panel


class TestIndustrySource(unittest.TestCase):
    def test_mapping_is_complete(self):
        self.assertEqual(len(m.CH_INDUSTRY_SERIES), 48)
        self.assertEqual(
            len(set(m.CH_INDUSTRY_SERIES)),
            len(ci.INDUSTRY_SHEETS) * len(ci.INDICATOR_KEYS),
        )
        for sheet in ci.INDUSTRY_SHEETS:
            for key in ci.INDICATOR_KEYS:
                name = m.ch_series_name(sheet, key)
                self.assertIn(name, m.CH_INDUSTRY_SERIES)
                self.assertEqual(m.CH_INDUSTRY_SERIES[name], (sheet, key))
                self.assertTrue(name.startswith("industry_"))
                self.assertTrue(name.endswith("_sa"))

    def test_panel_from_rows_and_validation(self):
        rows = []
        for name, (sheet, key) in m.CH_INDUSTRY_SERIES.items():
            rows.append(
                {"series": name, "date": "2002-01-01", "value": 1.0}
            )
            rows.append(
                {"series": name, "date": "2026-09-01", "value": 2.0}
            )
        panel = m.panel_from_rows(rows)
        meta = m.validate_panel(panel, expected_points=96)
        self.assertEqual(meta["n_series"], 48)
        self.assertEqual(meta["n_points"], 96)
        self.assertEqual(meta["first"], "2002-01-01")
        self.assertEqual(meta["last"], "2026-09-01")

    def test_missing_series_fails(self):
        panel = synthetic_panel()
        panel[ci.INDUSTRY_SHEETS[0]][ci.INDICATOR_KEYS[0]] = []
        with self.assertRaises(SystemExit):
            m.validate_panel(panel, expected_points=95)

    def test_duplicate_date_fails(self):
        panel = synthetic_panel()
        panel[ci.INDUSTRY_SHEETS[0]][ci.INDICATOR_KEYS[0]].append(
            (date(2002, 1, 1), 1.0)
        )
        with self.assertRaises(SystemExit):
            m.validate_panel(panel, expected_points=97)

    def test_non_finite_fails(self):
        panel = synthetic_panel()
        panel[ci.INDUSTRY_SHEETS[0]][ci.INDICATOR_KEYS[0]][0] = (
            date(2002, 1, 1),
            float("nan"),
        )
        with self.assertRaises(SystemExit):
            m.validate_panel(panel, expected_points=96)

    def test_unknown_series_fails(self):
        with self.assertRaises(SystemExit):
            m.panel_from_rows(
                [{"series": "unknown", "date": "2022-01-01", "value": 1.0}]
            )

    def test_verify_panels(self):
        panel = synthetic_panel()
        result = m.verify_panels(panel, panel)
        self.assertTrue(result["ok"])
        self.assertEqual(result["common_points"], 96)
        self.assertEqual(result["max_abs_diff"], 0.0)

        changed = synthetic_panel()
        changed[ci.INDUSTRY_SHEETS[0]][ci.INDICATOR_KEYS[0]][0] = (
            date(2002, 1, 1),
            99.0,
        )
        result = m.verify_panels(panel, changed)
        self.assertFalse(result["ok"])
        self.assertEqual(len(result["mismatched"]), 1)

        extra = synthetic_panel()
        extra[ci.INDUSTRY_SHEETS[0]][ci.INDICATOR_KEYS[0]].append(
            (date(2026, 10, 1), 3.0)
        )
        result = m.verify_panels(extra, panel)
        self.assertTrue(result["ok"])
        self.assertEqual(len(result["extra_in_ch"]), 1)


class TestHorizon(unittest.TestCase):
    def test_compounding(self):
        self.assertAlmostEqual(m.cumulative_target([1.0], 1), 1.0)
        self.assertAlmostEqual(
            m.cumulative_target([1.0, 2.0], 2),
            100.0 * (1.01 * 1.02 - 1.0),
        )
        self.assertAlmostEqual(
            m.cumulative_target([1.0, 2.0, -1.0], 3),
            100.0 * (1.01 * 1.02 * 0.99 - 1.0),
        )

    def test_nan_propagates(self):
        self.assertTrue(math.isnan(m.cumulative_target([1.0, float("nan")], 2)))

    def test_horizon_rows_truncates(self):
        rows = make_rows(10)
        self.assertEqual(len(m.horizon_rows(rows, 1)), 10)
        self.assertEqual(len(m.horizon_rows(rows, 3)), 8)

    def test_horizon_rows_targets(self):
        rows = make_rows(5)
        result = m.horizon_rows(rows, 2)
        self.assertEqual(result[0].month, rows[0].month)
        self.assertAlmostEqual(
            result[0].y[cm.TARGET_ALL],
            m.cumulative_target([1.0, 2.0], 2),
        )
        self.assertEqual(result[-1].month, rows[-2].month)

    def test_non_contiguous_fails(self):
        rows = [make_row(date(2022, 1, 1)), make_row(date(2022, 3, 1))]
        with self.assertRaises(AssertionError):
            m.horizon_rows(rows, 2)

    def test_window_and_purge(self):
        self.assertEqual(m.horizon_window(24, 0, 1), (24, 0))
        self.assertEqual(m.horizon_window(24, 0, 6), (29, 5))
        rows = make_rows(20)
        m.assert_no_horizon_leakage(rows, 3, *m.horizon_window(5, 0, 3))
        with self.assertRaises(AssertionError):
            m.assert_no_horizon_leakage(rows, 3, 5, 0)


class TestFeatures(unittest.TestCase):
    def test_names(self):
        names = m.feature_names()
        self.assertEqual(len(names), len(set(names)))
        self.assertEqual(names[-1], "month")
        for name in cmv3.WEEKLY_FEATURES + tuple(ci.industry_feature_names()):
            self.assertIn(name, names)

    def test_model_sets_are_known(self):
        for names in (
            m.FULL_MODELS,
            m.SECONDARY_MODELS,
            m.LONG_MODELS,
            m.LONG_WINDOW_MODELS,
        ):
            self.assertFalse(set(names) - set(m.MODELS))
        self.assertEqual(set(m.FULL_MODELS), set(ci.MODELS))


class TestModelsSmoke(unittest.TestCase):
    def test_catboost_industry_smoke(self):
        rows = make_rows(30)
        value = ci.fit_catboost_industry(
            rows[:-1], rows[-1], cm.TARGET_ALL, iterations=5
        )
        self.assertTrue(math.isfinite(value))

    def test_catboost_v3_smoke(self):
        rows = make_rows(30)
        value = cmv3.fit_catboost(
            rows[:-1], rows[-1], cm.TARGET_ALL, True, iterations=5
        )
        self.assertTrue(math.isfinite(value))

    def test_nnls_weights(self):
        rows = make_rows(30)
        weights = cm.nnls_weights(rows)
        self.assertEqual(len(weights), 3)
        self.assertAlmostEqual(sum(weights), 1.0)


class TestDecision(unittest.TestCase):
    @staticmethod
    def pair(delta: float, p: float = 0.01, holm: float = None) -> dict:
        if delta < 0:
            low, high = delta * 1.5, delta * 0.5
        else:
            low, high = delta * 0.5, delta * 1.5
        return {
            "n": 30,
            "rmse_left": 0.3,
            "rmse_right": 0.3 - delta,
            "delta": delta,
            "delta_ci": [low, high],
            "dm_stat": 1.0,
            "dm_p": p,
            "dm_p_holm": holm if holm is not None else p,
            "mde_ratio": 0.1,
        }

    def windows(self, a_ok: bool = True, b_ok: bool = True) -> dict:
        a, b = m.PRIMARY_CANDIDATES
        h1 = {}
        for candidate, ok in ((a, a_ok), (b, b_ok)):
            for baseline in m.PRIMARY_BASELINES:
                h1[f"{candidate} vs {baseline}"] = self.pair(
                    -0.05 if ok else 0.05
                )
        h1[f"{a} vs {b}"] = self.pair(-0.01)
        h2 = {
            f"{cand} vs v2_bottomup": self.pair(-0.02, p=0.5)
            for cand in (a, b)
        }
        h3 = {
            f"{cand} vs v2_bottomup": self.pair(-0.02, p=0.5)
            for cand in (a, b)
        }
        return {
            "weekly__h1__start24__embargo0": {"pairs": h1},
            "weekly__h2__start24__embargo0": {"pairs": h2},
            "weekly__h3__start24__embargo0": {"pairs": h3},
        }

    def test_promote_a(self):
        decision = m.decide(self.windows())
        self.assertEqual(decision["verdict"], "promote")
        self.assertEqual(decision["candidate"], "v3_ind_bottomup")

    def test_promote_b_fallback(self):
        decision = m.decide(self.windows(a_ok=False, b_ok=True))
        self.assertEqual(decision["verdict"], "promote")
        self.assertEqual(decision["candidate"], "v3_bottomup")

    def test_reject(self):
        windows = self.windows(a_ok=False, b_ok=False)
        for pairs in (
            windows["weekly__h1__start24__embargo0"]["pairs"],
        ):
            for name in pairs:
                if name.startswith(("v3_ind_bottomup vs v2_bottomup",
                                    "v3_bottomup vs v2_bottomup")):
                    pairs[name] = self.pair(0.05)
        decision = m.decide(windows)
        self.assertEqual(decision["verdict"], "reject")

    def test_iterate_without_h2(self):
        windows = self.windows()
        del windows["weekly__h2__start24__embargo0"]
        decision = m.decide(windows)
        self.assertEqual(decision["verdict"], "iterate")

    def test_significant_helpers(self):
        self.assertTrue(m._significant_better(self.pair(-0.05)))
        not_significant = self.pair(-0.05, p=0.2)
        not_significant["delta_ci"] = [-0.1, 0.01]
        self.assertFalse(m._significant_better(not_significant))
        self.assertTrue(m._significant_worse(self.pair(0.05)))
        self.assertFalse(m._significant_worse(self.pair(0.05, p=0.2)))


class TestGates(unittest.TestCase):
    def test_stage1_gate(self):
        summary = {
            "v2_bottomup": {"n": 32, "rmse": 0.362300},
            "v3_bottomup": {"n": 32, "rmse": 0.311487},
        }
        gate, failed = m.regression_gate("weekly", 24, 0, summary)
        self.assertEqual(failed, [])
        self.assertEqual(gate["v2_bottomup"]["status"], "ok")

        summary["v3_bottomup"]["rmse"] += 0.01
        gate, failed = m.regression_gate("weekly", 24, 0, summary)
        self.assertEqual(failed, ["v3_bottomup"])

    def test_task2_gate(self):
        summary = {
            "v3_ind_bottomup": {"n": 32, "rmse": 0.354359},
            "ind_only_bottomup": {"n": 32, "rmse": 0.384958},
        }
        gate, failed = m.task2_gate("weekly", 24, 0, summary)
        self.assertEqual(failed, [])
        self.assertEqual(gate["v3_ind_bottomup"]["status"], "ok")

    def test_gate_skips_unknown_window(self):
        gate, failed = m.regression_gate("weekly", 24, 1, {})
        self.assertEqual(gate, {})
        self.assertEqual(failed, [])


if __name__ == "__main__":
    unittest.main()
