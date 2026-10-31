#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_tlive_versions` (Task 14 #454): без ClickHouse, R и архива."""

import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_tlive_versions as tv


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


class ConstantsTest(unittest.TestCase):
    def test_margin_is_not_preregistered_and_primary_horizons(self):
        self.assertEqual(tv.NI_MARGIN, 0.02)
        self.assertEqual(tv.NI_HORIZONS, (1, 3, 6))
        self.assertEqual(tv.GAP_PERIODS["2009"][:2], (date(2009, 1, 1), date(2010, 1, 1)))


class CompositionTest(unittest.TestCase):
    def test_missing_groups_and_segments(self):
        months = grid(date(2009, 11, 1), date(2010, 3, 1))
        late = grid(date(2010, 2, 1), date(2010, 3, 1))
        sa = {"a": {m: 0.1 for m in months}, "b": {m: 0.1 for m in late}}
        weights = {2016: {"a": 3.0, "b": 1.0}}
        comp = tv.composition(sa, ["a", "b"], weights, months)
        self.assertEqual(comp[date(2009, 11, 1)]["missing"], ["b"])
        self.assertAlmostEqual(comp[date(2009, 11, 1)]["missing_weight_share"], 0.25)
        self.assertEqual(comp[date(2010, 2, 1)]["n"], 2)
        segments = tv.composition_segments(comp)
        self.assertEqual([(s["first"], s["last"]) for s in segments],
                         [(date(2009, 11, 1), date(2010, 1, 1)), (date(2010, 2, 1), date(2010, 3, 1))])


class TargetGapTest(unittest.TestCase):
    def test_window_must_fit_period_and_normal_filter(self):
        months = grid(date(2009, 1, 1), date(2010, 1, 1))
        t_live = {h: {m: 1.0 for m in months} for h in tv.GAP_HORIZONS}
        target = pd.DataFrame({h: [0.5] * len(months) for h in tv.GAP_HORIZONS}, index=months)
        out = tv.target_gap(t_live, target, set())
        self.assertEqual(out["2009"]["h1"]["n"], 13)
        self.assertEqual(out["2009"]["h6"]["n"], 8)
        self.assertAlmostEqual(out["2009"]["h1"]["bias"], 0.5)
        self.assertIsNone(out["test_normal54"]["h1"])


class NonInferiorityTest(unittest.TestCase):
    def test_identical_errors_are_noninferior(self):
        err = np.array([0.1, -0.2, 0.3, -0.1, 0.05, 0.2, -0.3, 0.1])
        out = tv.noninferiority(err, err.copy(), np.random.default_rng(tv.SEED))
        self.assertAlmostEqual(out["rel"], 1.0)
        self.assertAlmostEqual(out["rel_upper95"], 1.0)
        self.assertTrue(out["noninferior"])

    def test_much_worse_errors_fail(self):
        ref = np.array([0.1, -0.1] * 10)
        out = tv.noninferiority(ref * 1.5, ref, np.random.default_rng(tv.SEED))
        self.assertGreater(out["rel_upper95"], 1.02)
        self.assertFalse(out["noninferior"])

    def test_bootstrap_deterministic(self):
        values = np.arange(20, dtype=float)
        a = tv.block_bootstrap_means(values, np.random.default_rng(1), b=50)
        b = tv.block_bootstrap_means(values, np.random.default_rng(1), b=50)
        np.testing.assert_array_equal(a, b)


class ShiftTest(unittest.TestCase):
    def test_rms_shift(self):
        months = grid(date(2019, 1, 1), date(2019, 4, 1))
        out = tv.sa_shift({m: 0.3 for m in months}, {m: 0.1 for m in months})
        self.assertAlmostEqual(out["2019-01..2024-07"]["rms_pp"], 0.2)
        self.assertEqual(out["2010-02..2012-12"]["n"], 0)


if __name__ == "__main__":
    unittest.main()
