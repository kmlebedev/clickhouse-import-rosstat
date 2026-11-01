#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h1_weekly` (Task 4 #424): без ClickHouse и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_weekly as hw


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(hw.HORIZON, 1)
        self.assertEqual(hw.CO_PRIMARY, (hw.MODEL_COMBO_WK, hw.MODEL_WK_MIX))
        self.assertEqual(hw.WEEKLY_NAMES, ("wk_g_mtd", "wk_g_cov"))
        self.assertEqual(hw.HOLM_ALPHA, 0.025)
        self.assertEqual(hw.R11_EXPECTED["n_groups"], 52)
        self.assertEqual(hw.R10_PINNED[hw.MODEL_COMBO], 0.16432658972448905)
        self.assertEqual(hw.VERSIONS, ("2010", "2009"))
        self.assertEqual(hw.R10_PINS["2010"], hw.R10_PINNED)

    def test_r10_accepts_version_pin(self):
        per_model = {name: {"rmse": value} for name, value in hw.R10_PINNED.items()}
        out = hw.r10_check(per_model, hw.R10_PINNED)
        self.assertTrue(out["ok"])


class NamesTest(unittest.TestCase):
    def test_live_cb_names(self):
        names = hw.model_names(bu.GroupSpec(code="10", bsu_codes=(), level=3), True)
        self.assertEqual(names, list(bu.BASE_FEATURES))
        self.assertEqual(names[-1], "month")

    def test_weekly_names_appended_after_month(self):
        spec = bu.GroupSpec(code="1100", bsu_codes=("123",), level=4, sheet_slugs=("trade_retail",))
        base = hw.model_names(spec, True)
        names = hw.model_names(spec, True, hw.WEEKLY_NAMES)
        self.assertEqual(names[: len(base)], base)
        self.assertEqual(names[len(base):], list(hw.WEEKLY_NAMES))
        self.assertEqual(names.index("month"), base.index("month"))


class AttachTest(unittest.TestCase):
    def setUp(self):
        self.month = date(2024, 1, 1)
        self.features = {"10": {self.month: {"y_lag1": 1.0, "month": 1}}}
        self.weekly = {self.month: {"10": {"wk_g_mtd": 0.5, "wk_g_cov": 1.0}}}

    def test_adds_and_preserves(self):
        out = hw.attach_weekly(self.features, self.weekly)
        self.assertEqual(out["10"][self.month]["wk_g_mtd"], 0.5)
        self.assertEqual(out["10"][self.month]["wk_g_cov"], 1.0)
        self.assertEqual(out["10"][self.month]["y_lag1"], 1.0)
        self.assertNotIn("wk_g_mtd", self.features["10"][self.month])

    def test_missing_weekly_is_nan_not_zero(self):
        out = hw.attach_weekly(self.features, {})
        self.assertTrue(math.isnan(out["10"][self.month]["wk_g_mtd"]))
        self.assertEqual(out["10"][self.month]["wk_g_cov"], 0.0)


class CombineTest(unittest.TestCase):
    def test_mean_common_months(self):
        a = {date(2022, 1, 1): 1.0, date(2022, 2, 1): 2.0}
        b = {date(2022, 1, 1): 3.0, date(2022, 2, 1): 4.0}
        c = {date(2022, 1, 1): 5.0}
        self.assertEqual(hw.combine_mean([a, b, c]), {date(2022, 1, 1): 3.0})

    def test_mix(self):
        a = {date(2022, 1, 1): 1.0, date(2022, 2, 1): 2.0}
        b = {date(2022, 1, 1): 3.0}
        self.assertEqual(hw.mix(a, b), {date(2022, 1, 1): 2.0})


class WalkForwardTest(unittest.TestCase):
    def test_train_is_strictly_before_and_without_shocks(self):
        months = grid(date(2019, 1, 1), date(2022, 3, 1))
        train = hw.wf_train_months(months, date(2022, 3, 1))
        self.assertNotIn(date(2022, 3, 1), train)
        for shock in (date(2020, 4, 1), date(2022, 1, 1), date(2022, 2, 1)):
            self.assertNotIn(shock, train)
        self.assertIn(date(2021, 12, 1), train)
        self.assertLess(max(train), date(2022, 3, 1))


class OlsTest(unittest.TestCase):
    def test_recovers_exact_model(self):
        months = grid(date(2010, 3, 1), date(2014, 2, 1))
        rng = np.random.default_rng(5)
        x1 = rng.normal(0, 1, len(months))
        x2 = rng.normal(0, 1, len(months))
        features = {m: {"wkagg_mtd": float(a), "wkagg_4w": float(b)} for m, a, b in zip(months, x1, x2)}
        target = {m: 2.0 + 3.0 * a + 5.0 * b for m, a, b in zip(months, x1, x2)}
        pred = hw.ols_agg_predict(features, target, months[:40], months[40:])
        self.assertEqual(len(pred), len(months) - 40)
        for month in months[40:]:
            self.assertAlmostEqual(pred[month], target[month], places=6)

    def test_nan_median_and_empty(self):
        months = grid(date(2010, 3, 1), date(2012, 2, 1))
        features = {m: {"wkagg_mtd": 1.0, "wkagg_4w": float("nan")} for m in months}
        target = {m: 1.0 for m in months}
        pred = hw.ols_agg_predict(features, target, months[:20], months[20:])
        self.assertTrue(all(abs(value - 1.0) < 1e-9 for value in pred.values()))
        self.assertEqual(hw.ols_agg_predict({}, {}, [], []), {})


class GateTest(unittest.TestCase):
    def pair(self, p, high, n=57):
        return {"dm_p": p, "delta_ci": [high - 0.01, high], "n": n}

    def test_g10_pass(self):
        pairs = {hw.MODEL_COMBO_WK: self.pair(0.01, -0.001), hw.MODEL_WK_MIX: self.pair(0.4, 0.01)}
        self.assertTrue(hw.g10_check(pairs, 57)["ok"])

    def test_holm_threshold_fail(self):
        pairs = {hw.MODEL_COMBO_WK: self.pair(0.03, -0.001), hw.MODEL_WK_MIX: self.pair(0.4, 0.01)}
        self.assertFalse(hw.g10_check(pairs, 57)["ok"])

    def test_ci_with_zero_fail(self):
        pairs = {hw.MODEL_COMBO_WK: self.pair(0.01, 0.0), hw.MODEL_WK_MIX: self.pair(0.4, 0.01)}
        self.assertFalse(hw.g10_check(pairs, 57)["ok"])

    def test_missing_month_fail(self):
        pairs = {hw.MODEL_COMBO_WK: self.pair(0.01, -0.001, 56), hw.MODEL_WK_MIX: self.pair(0.4, 0.01)}
        self.assertFalse(hw.g10_check(pairs, 57)["ok"])

    def test_holm(self):
        self.assertEqual(hw.holm({"a": 0.01, "b": 0.04}), {"a": 0.02, "b": 0.04})

    def test_holm_matches_review(self):
        got = hw.holm({hw.MODEL_COMBO_WK: 0.1410959295, hw.MODEL_WK_MIX: 0.2956956541})
        self.assertAlmostEqual(got[hw.MODEL_COMBO_WK], 0.2821918590)
        self.assertAlmostEqual(got[hw.MODEL_WK_MIX], 0.2956956541)


class SensitivityTest(unittest.TestCase):
    def test_slices(self):
        months = [date(2022, 3, 1), date(2022, 4, 1), date(2022, 12, 1), date(2023, 7, 1), date(2023, 8, 1), date(2024, 7, 1)]
        out = hw.sensitivity_slices(months)
        self.assertNotIn(date(2022, 3, 1), out["no_2022_03"])
        self.assertIn(date(2022, 12, 1), out["util_months"])
        self.assertIn(date(2024, 7, 1), out["util_months"])
        self.assertIn(date(2023, 8, 1), out["aug_sep_2023"])
        self.assertNotIn(date(2023, 7, 1), out["aug_sep_2023"])


class R10Test(unittest.TestCase):
    def test_pinned(self):
        per = {name: {"rmse": value} for name, value in hw.R10_PINNED.items()}
        self.assertTrue(hw.r10_check(per)["ok"])
        per[hw.MODEL_COMBO]["rmse"] += 1e-3
        self.assertFalse(hw.r10_check(per)["ok"])


class PeriodsTest(unittest.TestCase):
    def test_subperiods(self):
        months = grid(date(2022, 1, 1), date(2026, 2, 1))
        models = {"a": {m: 1.0 for m in months}}
        truth = {m: 0.0 for m in months}
        out = hw.subperiods(models, truth, months)
        self.assertEqual(set(out), {"2022", "2023_2024", "2025_2026"})
        self.assertAlmostEqual(out["2022"]["a"], 1.0)
        self.assertEqual(sum(len([m for m in months if m.year == y]) for y in (2022,)), 12)


class R12Test(unittest.TestCase):
    def test_diagnostic(self):
        months = [date(2024, m, 1) for m in (1, 2, 3)]
        late = {m: {"10": {"wk_g_mtd": float(i + 1)}} for i, m in enumerate(months)}
        publication = {m: {"10": {"wk_g_mtd": float(i + 1) - 0.5}} for i, m in enumerate(months)}
        agg_late = {m: {"wkagg_mtd": 1.0} for m in months}
        agg_publication = {m: {"wkagg_mtd": 0.5} for m in months}
        out = hw.r12_check(late, publication, agg_late, agg_publication, ["10"])
        self.assertEqual(out["groups_n"], 3)
        self.assertAlmostEqual(out["groups_mae_pp"], 0.5)
        self.assertAlmostEqual(out["agg_mae_pp"], 0.5)


if __name__ == "__main__":
    unittest.main()
