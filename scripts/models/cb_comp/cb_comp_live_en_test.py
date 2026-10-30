#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_live_en` (Task 3 #423): без ClickHouse, R и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en as en
from models.cpi import cpi_monthly as cm


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(en.CANDIDATES, ("live_cb", "live_en", "live_cb_en"))
        self.assertEqual(en.G3_THRESHOLD, {1: 1.264, 3: 1.098, 6: 1.050})
        self.assertEqual(en.PRIMARY_HORIZONS, (1, 3, 6))
        self.assertEqual(en.G3_THRESHOLD[1], lb.g2_threshold(1))

    def test_enet_is_b3_configuration(self):
        model = en.make_enet()
        enet = model.named_steps["enet"]
        np.testing.assert_array_equal(enet.alphas, cm.ENET_ALPHAS)
        self.assertEqual(list(enet.l1_ratio), [0.1, 0.5, 0.9])
        self.assertEqual(enet.cv.n_splits, 3)
        self.assertEqual(enet.random_state, bu.SEED)


class EnetTest(unittest.TestCase):
    def rows(self, n: int, rng, with_nan: bool = False) -> list:
        out = []
        for i in range(n):
            x1, x2 = float(rng.normal()), float(rng.normal())
            features = {"y_lag1": x1, "y_lag2": x2, "usd_mm": float("nan") if with_nan and i < 10 else 0.5,
                        "key_rate": None, "month": (i % 12) + 1}
            out.append((date(2010, 1, 1), features, {1: 2 * x1 - x2 + float(rng.normal(0, 0.01))}))
        return out

    def test_names_drop_month(self):
        self.assertEqual(en.en_names(["y_lag1", "month", "mp_x"]), ["y_lag1", "mp_x"])

    def test_float_matrix_none_and_missing_to_nan(self):
        matrix = en.float_matrix([(None, {"a": None}, {})], ["a", "b"])
        self.assertTrue(np.isnan(matrix).all())

    def test_fits_linear_signal_with_nan_and_empty_column(self):
        rng = np.random.default_rng(7)
        train, test = self.rows(100, rng, with_nan=True), self.rows(5, rng)
        names = ["y_lag1", "y_lag2", "usd_mm", "key_rate"]
        preds, _ = en.enet_fit_predict(train, test, names, 1)
        truth = [row[2][1] for row in test]
        self.assertEqual(len(preds), 5)
        self.assertTrue(all(math.isfinite(p) for p in preds))
        self.assertLess(max(abs(p - t) for p, t in zip(preds, truth)), 0.2)

    def test_deterministic(self):
        rng = np.random.default_rng(9)
        train, test = self.rows(80, rng), self.rows(4, rng)
        names = ["y_lag1", "y_lag2"]
        self.assertEqual(en.enet_fit_predict(train, test, names, 1), en.enet_fit_predict(train, test, names, 1))


class PipelineTest(unittest.TestCase):
    def test_fit_predict_en_end_to_end(self):
        months = grid(date(2010, 1, 1), date(2019, 12, 1))
        rng = np.random.default_rng(11)
        sa = {code: {m: float(rng.normal(0.4, 0.2)) for m in months} for code in ("a", "b")}
        specs = [bu.GroupSpec(code="a", bsu_codes=(), level=3), bu.GroupSpec(code="b", bsu_codes=(), level=3)]
        diff = lb.log_diff_frame(sa, ["a", "b"], months)
        targets = bu.component_targets(diff)
        macro = {m: {"usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1} for m in months}
        train = grid(date(2010, 3, 1), date(2018, 12, 1))
        test = grid(date(2019, 1, 1), date(2019, 12, 1))
        features = bu.feature_table(specs, train + test, diff, macro, {}, feature_mode="mean")
        preds, _ = en.fit_predict_en(specs, features, targets, train, test)
        self.assertEqual(set(preds), {"a", "b"})
        self.assertEqual(set(preds["a"]), {1, 2, 3, 6})
        self.assertEqual(set(preds["a"][1]), set(test))

    def test_combine_equal_weights_and_pairs_only(self):
        m1, m2 = date(2019, 1, 1), date(2019, 2, 1)
        first = {"a": {1: {m1: 1.0, m2: 2.0}}, "b": {1: {m1: 5.0}}}
        second = {"a": {1: {m1: 3.0}}}
        out = en.combine(first, second)
        self.assertEqual(out, {"a": {1: {m1: 2.0}}})


class ChoiceTest(unittest.TestCase):
    def setUp(self):
        self.ratios = {
            "live_cb": {1: 1.19, 3: 1.08, 6: 1.06},
            "live_en": {1: 1.10, 3: 1.00, 6: 0.98},
            "live_cb_en": {1: 1.12, 3: 1.02, 6: 1.00},
        }
        self.all_in = {h: {"live_cb", "live_en", "live_cb_en", "cb_comp"} for h in (1, 3, 6)}

    def beats(self, en_win: bool, mix_win: bool) -> dict:
        return {
            "live_cb": {h: False for h in (1, 3, 6)},
            "live_en": {1: en_win, 3: False, 6: False},
            "live_cb_en": {1: False, 3: mix_win, 6: False},
        }

    def test_no_significant_win_keeps_cb(self):
        chosen, _ = en.choose_model(self.ratios, self.all_in, self.beats(False, False))
        self.assertEqual(chosen, "live_cb")

    def test_lowest_mean_ratio_among_eligible(self):
        chosen, _ = en.choose_model(self.ratios, self.all_in, self.beats(True, True))
        self.assertEqual(chosen, "live_en")

    def test_mcs_exclusion_on_one_horizon(self):
        mcs = {h: set(v) for h, v in self.all_in.items()}
        mcs[6].discard("live_en")
        chosen, _ = en.choose_model(self.ratios, mcs, self.beats(True, True))
        self.assertEqual(chosen, "live_cb_en")

    def test_better_than_cb_needs_p_and_ci(self):
        self.assertTrue(en.better_than_cb({"dm_p": 0.01, "delta_ci": [-0.05, -0.01]}))
        self.assertFalse(en.better_than_cb({"dm_p": 0.01, "delta_ci": [-0.05, 0.01]}))
        self.assertFalse(en.better_than_cb({"dm_p": 0.06, "delta_ci": [-0.05, -0.01]}))

    def test_g3(self):
        self.assertTrue(en.g3_check({1: 1.264, 3: 1.098, 6: 1.05})["ok"])
        out = en.g3_check({1: 1.19, 3: 1.08, 6: 1.058})
        self.assertFalse(out["ok"])
        self.assertFalse(out["horizons"][6]["ok"])


if __name__ == "__main__":
    unittest.main()
