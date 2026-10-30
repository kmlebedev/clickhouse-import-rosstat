#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_live_en16` (Task 11 #439): без ClickHouse, R и архива."""

import copy
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as hb
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_live_en16 as en16


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


def synthetic(first=date(2012, 1, 1), last=date(2019, 12, 1), seed=7):
    """SA-панель двух групп и признаки в режиме `mean`."""
    months = grid(first, last)
    rng = np.random.default_rng(seed)
    sa = {code: {m: float(rng.normal(0.4, 0.2)) for m in months} for code in ("a", "b")}
    specs = [bu.GroupSpec(code="a", bsu_codes=(), level=3), bu.GroupSpec(code="b", bsu_codes=(), level=3)]
    diff = lb.log_diff_frame(sa, ["a", "b"], months)
    targets = bu.component_targets(diff)
    macro = {m: {"usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1} for m in months}
    features = bu.feature_table(specs, months, diff, macro, {}, feature_mode="mean")
    return months, sa, specs, diff, targets, features


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(en16.EN_START, date(2016, 1, 1))
        self.assertEqual(en16.EN_START, hb.BS_START)
        self.assertEqual(en16.CANDIDATES, ("live_cb", "live_en16", "combo3", "combo4"))
        self.assertEqual(en16.G11_ALPHA, 0.10)
        self.assertEqual(en16.NOHARM_MARGIN, 0.02)
        self.assertEqual(en16.NOHARM_MARGIN, hb.NOHARM_MARGIN)

    def test_outliers_are_article_set(self):
        self.assertEqual(en16.ARTICLE_OUTLIERS, hb.ARTICLE_OUTLIERS)
        self.assertEqual(len(en16.ARTICLE_OUTLIERS), 17)
        self.assertIn(date(2020, 4, 1), en16.ARTICLE_OUTLIERS)
        self.assertIn(date(2022, 7, 1), en16.ARTICLE_OUTLIERS)

    def test_r11a_pins(self):
        self.assertEqual(en16.R11A_COMBO3, {1: 1.034768, 2: 0.957109, 3: 1.010198, 6: 0.894035})
        self.assertEqual(en16.R11A_LIVE_CB, hb.TASK3_LIVE_CB_RATIOS)


class CandidateRowsTest(unittest.TestCase):
    def setUp(self):
        self.months, self.sa, self.specs, self.diff, self.targets, self.features = synthetic()

    def test_window_bounds(self):
        for horizon in (1, 3, 6):
            rows = en16.candidate_rows("a", self.features["a"], self.targets[horizon], horizon, self.months[-1])
            self.assertEqual(rows[0][0], bu.shift_month(date(2016, 1, 1), horizon))  # 2016-01 + h
            self.assertEqual(rows[-1][0], bu.shift_month(self.months[-1], -horizon))  # last − h

    def test_article_outliers_excluded(self):
        rows = en16.candidate_rows("a", self.features["a"], self.targets[1], 1, self.months[-1])
        months = {row[0] for row in rows}
        for month in en16.ARTICLE_OUTLIERS:
            self.assertNotIn(month, months)

    def test_nan_target_skipped(self):
        target_month = date(2018, 1, 1)
        clean = {r[0] for r in en16.candidate_rows("a", self.features["a"], self.targets[1], 1, self.months[-1])}
        self.assertIn(target_month, clean)
        targets = {1: self.targets[1].copy()}
        targets[1].loc[target_month, "a"] = float("nan")
        rows = en16.candidate_rows("a", self.features["a"], targets[1], 1, self.months[-1])
        self.assertNotIn(target_month, {row[0] for row in rows})


class TrainingRowsTest(unittest.TestCase):
    def setUp(self):
        self.months, self.sa, self.specs, self.diff, self.targets, self.features = synthetic()

    def test_purge_and_start(self):
        horizon = 3
        rows = en16.candidate_rows("a", self.features["a"], self.targets[horizon], horizon, self.months[-1])
        month = date(2019, 6, 1)
        train = en16.training_rows_upto(rows, month, horizon)
        self.assertEqual(train[-1][0], date(2019, 3, 1))  # M − h
        self.assertEqual(train[0][0], bu.shift_month(date(2016, 1, 1), horizon))
        self.assertNotIn(month, {row[0] for row in train})

    def test_assert_training_bounds(self):
        horizon = 2
        month = date(2019, 6, 1)
        good = [(date(2019, 4, 1), {}, {horizon: 1.0})]
        en16.assert_training_bounds(good, month, horizon)  # не падает
        future = [(date(2019, 5, 1), {}, {horizon: 1.0})]
        with self.assertRaises(AssertionError):
            en16.assert_training_bounds(future, month, horizon)
        early = [(date(2016, 1, 1), {}, {horizon: 1.0})]
        with self.assertRaises(AssertionError):
            en16.assert_training_bounds(early, month, horizon)


class NoFutureTest(unittest.TestCase):
    def test_spoiling_future_does_not_change_prediction(self):
        months, sa, specs, diff, targets, features = synthetic()
        for horizon, month in ((1, date(2019, 6, 1)), (3, date(2019, 6, 1))):
            clean, _ = en16.fit_predict_en16(specs, features, targets, [month])
            spoiled_sa = copy.deepcopy(sa)
            for code in spoiled_sa:
                for m in spoiled_sa[code]:
                    if m > bu.shift_month(month, -horizon):
                        spoiled_sa[code][m] = 1e6
            spoiled_diff = lb.log_diff_frame(spoiled_sa, ["a", "b"], months)
            spoiled_targets = bu.component_targets(spoiled_diff)
            spoiled_features = bu.feature_table(
                specs, months, spoiled_diff, {m: {"usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1} for m in months},
                {}, feature_mode="mean")
            spoiled, _ = en16.fit_predict_en16(specs, spoiled_features, spoiled_targets, [month])
            for code in ("a", "b"):
                self.assertAlmostEqual(clean[code][horizon][month], spoiled[code][horizon][month], places=10)


class PipelineTest(unittest.TestCase):
    def test_fit_predict_structure(self):
        months, sa, specs, diff, targets, features = synthetic()
        test = [date(2019, 1, 1), date(2019, 6, 1)]
        preds, _ = en16.fit_predict_en16(specs, features, targets, test)
        self.assertEqual(set(preds), {"a", "b"})
        for code in ("a", "b"):
            self.assertEqual(set(preds[code]), {1, 2, 3, 6})
            for horizon in (1, 2, 3, 6):
                self.assertEqual(set(preds[code][horizon]), set(test))

    def test_imputation_uses_train_median(self):
        months, sa, specs, diff, targets, features = synthetic()
        names = ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm"]
        horizon = 1
        rows = en16.candidate_rows("a", features["a"], targets[horizon], horizon, months[-1])
        month = date(2019, 6, 1)
        test_row = (month, {"y_lag1": 0.3, "y_lag2": float("nan"), "usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1}, {})
        from models.cpi import cpi_monthly as cm
        from models.cb_comp import cb_comp_live_en as en
        train_rows = en16.training_rows_upto(rows, month, horizon)
        medians = cm.enet_medians(en.float_matrix(train_rows, names))
        filled = (month, {**test_row[1], "y_lag2": float(medians[names.index("y_lag2")])}, {})
        nan_pred, _ = en16.predict_month(train_rows, names, [test_row], horizon)
        median_pred, _ = en16.predict_month(train_rows, names, [filled], horizon)
        self.assertAlmostEqual(nan_pred, median_pred, places=12)


class CombineTest(unittest.TestCase):
    def test_equal_weights_on_common_months(self):
        m1, m2 = date(2019, 1, 1), date(2019, 2, 1)
        first = {1: {m1: 1.0, m2: 2.0}, 3: {m1: 3.0}, 2: {m1: 0.0}, 6: {m1: 0.0}}
        second = {1: {m1: 3.0}, 3: {m1: 5.0, m2: 7.0}, 2: {m1: 0.0}, 6: {m1: 0.0}}
        out = en16.combine_two(first, second)
        self.assertEqual(out[1], {m1: 2.0})
        self.assertEqual(out[3], {m1: 4.0})


class GatesTest(unittest.TestCase):
    def pair(self, p, delta, ci):
        return {"dm_p": p, "delta": delta, "delta_ci": list(ci)}

    def ratios(self, combo4_h1=1.03, combo4_h3=1.00, combo4_h6=0.89, combo3=None):
        combo3 = combo3 or {1: 1.034768, 2: 0.957109, 3: 1.010198, 6: 0.894035}
        return {
            "combo3": combo3,
            "combo4": {1: combo4_h1, 2: 0.95, 3: combo4_h3, 6: combo4_h6},
        }

    def pairs(self, p=0.09, delta=-0.01, ci=(-0.02, -0.001)):
        return {"combo4": {1: self.pair(0.5, 0.0, (-0.01, 0.01)), 3: self.pair(p, delta, ci),
                           6: self.pair(0.5, 0.0, (-0.01, 0.01))}}

    def test_pass(self):
        mcs = {h: {"combo3", "combo4", "cb_comp"} for h in (1, 3, 6)}
        out = en16.g11_check(self.ratios(), self.pairs(), mcs)
        self.assertTrue(out["ok"])
        self.assertFalse(out["h3_weak_ci"])

    def test_alpha_boundary(self):
        out = en16.g11_check(self.ratios(), self.pairs(p=0.10), {h: {"combo4"} for h in (1, 3, 6)})
        self.assertFalse(out["h3_better"])
        self.assertFalse(out["ok"])

    def test_direction_boundary(self):
        out = en16.g11_check(self.ratios(), self.pairs(delta=0.0), {h: {"combo4"} for h in (1, 3, 6)})
        self.assertFalse(out["h3_better"])

    def test_weak_ci_flagged_but_passes(self):
        out = en16.g11_check(self.ratios(), self.pairs(ci=(-0.02, 0.001)), {h: {"combo4"} for h in (1, 3, 6)})
        self.assertTrue(out["h3_better"])
        self.assertTrue(out["h3_weak_ci"])
        self.assertTrue(out["ok"])

    def test_noharm_boundary(self):
        ratios = self.ratios(combo4_h1=1.054768, combo4_h6=0.914035)  # ровно +0,02
        out = en16.g11_check(ratios, self.pairs(), {h: {"combo4"} for h in (1, 3, 6)})
        self.assertTrue(out["noharm"]["ok"])
        ratios = self.ratios(combo4_h1=1.054769, combo4_h6=0.914035)
        out = en16.g11_check(ratios, self.pairs(), {h: {"combo4"} for h in (1, 3, 6)})
        self.assertFalse(out["noharm"]["ok"])
        self.assertFalse(out["ok"])

    def test_mcs_membership(self):
        mcs = {1: {"combo4"}, 3: {"combo4"}, 6: {"combo3"}}
        out = en16.g11_check(self.ratios(), self.pairs(), mcs)
        self.assertFalse(out["mcs"]["horizons"][6])
        self.assertFalse(out["ok"])

    def test_r11a(self):
        ratios = {"combo3": {1: 1.0348, 2: 0.9572, 3: 1.0102, 6: 0.8940},
                  "live_cb": {1: 1.1929, 2: 1.0127, 3: 1.0758, 6: 1.0617}}
        self.assertTrue(en16.r11a_check(ratios)["ok"])
        ratios["combo3"][3] = 1.02
        self.assertFalse(en16.r11a_check(ratios)["ok"])


if __name__ == "__main__":
    unittest.main()
