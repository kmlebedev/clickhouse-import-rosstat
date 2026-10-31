#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h1_weekly_ex` (Task 4b #446): без ClickHouse и архива."""

import hashlib
import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_groups_panel as gp
from models.cb_comp import cb_comp_h1_weekly_ex as ex
from models.cpi import cpi_public_panel_audit as audit


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)
    return out


def dictionary() -> "gp.Dictionary":
    codes = ["9457", "9448", "9458", "9449", "9459", "9471", "9472", "9473", "9474"]
    return gp.Dictionary(names={code: code for code in codes}, levels={code: 7 for code in codes},
                         leaves_by_ancestor={"9470": codes})


def leaf_weights() -> dict:
    return {"9457": 0.00512, "9448": 0.00637, "9458": 0.00459, "9449": 0.02066, "9459": 0.00013,
            "9471": 0.00708, "9472": 0.00117, "9473": 0.00349, "9474": 0.00081, "9416": 0.00060}


def weekly() -> dict:
    data = {series: [] for series in ex.UTIL_SERIES}
    data[ex.EXCLUDED_HOUSING] = []
    return data


class ConstantsTest(unittest.TestCase):
    def test_protocol(self):
        self.assertEqual(len(ex.UTIL_SERIES), 6)
        self.assertEqual(ex.EXCLUDED_HOUSING_CODE, "9416")
        self.assertEqual(ex.UTIL_COV_MIN, 0.5)
        self.assertEqual(ex.R13_PINNED["coverage"], 0.8314)
        self.assertEqual(ex.R12_PREDICTIONS_SHA256[:8], "0a67483f")


class UtilitiesTest(unittest.TestCase):
    def test_summary(self):
        out = ex.utilities_summary(weekly(), dictionary(), leaf_weights(), 0.05944)
        self.assertEqual(out["n_series"], 6)
        self.assertEqual(out["n_codes"], 9)
        self.assertAlmostEqual(out["covered_weight"], 0.04942, places=10)
        self.assertAlmostEqual(out["coverage"], 0.04942 / 0.05944, places=10)
        self.assertFalse(out["excluded"]["included"])
        self.assertEqual(out["excluded"]["weight_codes"], "9416")
        self.assertAlmostEqual(out["excluded"]["weight_2024"], 0.00060, places=10)

    def test_code_not_in_9470_rejected(self):
        bad = dictionary()
        bad.leaves_by_ancestor["9470"] = [code for code in bad.leaves_by_ancestor["9470"] if code != "9474"]
        with self.assertRaises(SystemExit):
            ex.utilities_summary(weekly(), bad, leaf_weights(), 0.05944)

    def test_missing_series_rejected(self):
        data = weekly()
        del data["Отопление, Гкал"]
        with self.assertRaises(SystemExit):
            ex.utilities_summary(data, dictionary(), leaf_weights(), 0.05944)

    def test_coverage_by_year(self):
        rows = ex.coverage_by_year({2024: leaf_weights()}, {2024: {"9470": 0.05944}}, years=[2024])
        self.assertEqual(rows[0]["year"], 2024)
        self.assertAlmostEqual(rows[0]["coverage"], 0.8314, places=3)


class ExValueTest(unittest.TestCase):
    def test_zero_weight_identity(self):
        self.assertAlmostEqual(ex.ex_value(2.0, 5.0, 0.0), 2.0, places=10)

    def test_weight_scales(self):
        expected = (math.exp(math.log1p(0.02) / (1 - 0.06)) - 1) * 100
        self.assertAlmostEqual(ex.ex_value(2.0, 0.0, 0.06), expected, places=10)

    def test_nan_and_bad_weight(self):
        self.assertTrue(math.isnan(ex.ex_value(float("nan"), 1.0, 0.06)))
        self.assertTrue(math.isnan(ex.ex_value(1.0, float("nan"), 0.06)))
        self.assertTrue(math.isnan(ex.ex_value(1.0, 1.0, 1.0)))

    def test_agg_ex_features(self):
        points = [(date(2024, 1, 8), 101.0), (date(2024, 1, 15), 102.0), (date(2024, 1, 22), 101.0), (date(2024, 1, 29), 103.0)]
        data = {audit.WEEKLY_AGGREGATE: points}
        util = {date(2024, 1, 1): {"wk_g_mtd": 1.0, "wk_g_cov": 0.83}}
        got = ex.agg_ex_features(data, [date(2024, 1, 1)], util, {2024: {"9470": 0.06}})
        item = got[date(2024, 1, 1)]
        self.assertFalse(math.isnan(item["wkagg_ex_mtd"]))
        self.assertAlmostEqual(item["wkagg_ex_mtd"], ex.ex_value(item["agg_mtd"], 1.0, 0.06), places=10)
        low = ex.agg_ex_features(data, [date(2024, 1, 1)], {date(2024, 1, 1): {"wk_g_mtd": 1.0, "wk_g_cov": 0.4}}, {2024: {"9470": 0.06}})
        self.assertTrue(math.isnan(low[date(2024, 1, 1)]["wkagg_ex_mtd"]))


class OlsTest(unittest.TestCase):
    def test_design_length(self):
        self.assertEqual(len(ex.design(date(2024, 7, 1), {"wkagg_ex_mtd": 1.0}, True)), 13)
        self.assertEqual(len(ex.design(date(2024, 7, 1), {"wkagg_ex_mtd": 1.0}, False)), 2)

    def test_design_dummies_only(self):
        self.assertEqual(len(ex.design(date(2024, 7, 1), {}, True, ex.FEATURE_EX, False)), 12)

    def test_recovers_seasonal_model(self):
        months = grid(date(2010, 3, 1), date(2013, 12, 1))
        features = {m: {"wkagg_ex_mtd": float(i) / 10.0} for i, m in enumerate(months)}
        target = {m: 1.0 + 2.0 * features[m]["wkagg_ex_mtd"] + (0.5 if m.month == 7 else 0.0) for m in months}
        test = months[-6:]
        pred = ex.ols_ex_predict(features, target, months[:-6], test, dummies=True)
        for month in test:
            self.assertAlmostEqual(pred[month], target[month], places=8)

    def test_without_dummies_differs(self):
        months = grid(date(2010, 3, 1), date(2013, 12, 1))
        features = {m: {"wkagg_ex_mtd": 1.0} for m in months}
        target = {m: 1.0 + (0.5 if m.month == 7 else 0.0) for m in months}
        test = [m for m in months if m.month == 7][-1:]
        with_d = ex.ols_ex_predict(features, target, months, test, dummies=True)
        without = ex.ols_ex_predict(features, target, months, test, dummies=False)
        self.assertNotAlmostEqual(with_d[test[0]], without[test[0]], places=3)


class GateTest(unittest.TestCase):
    def pair(self, p, high, n=45):
        return {"dm_p": p, "delta_ci": [high - 0.01, high], "n": n}

    def test_pass(self):
        pairs = {ex.MODEL_AGG_EX: self.pair(0.01, -0.001), ex.MODEL_MIX_EX: self.pair(0.4, 0.01)}
        self.assertTrue(ex.g11_check(pairs, 45)["ok"])

    def test_holm_fail(self):
        pairs = {ex.MODEL_AGG_EX: self.pair(0.03, -0.001), ex.MODEL_MIX_EX: self.pair(0.4, 0.01)}
        self.assertFalse(ex.g11_check(pairs, 45)["ok"])

    def test_ci_fail(self):
        pairs = {ex.MODEL_AGG_EX: self.pair(0.01, 0.0), ex.MODEL_MIX_EX: self.pair(0.4, 0.01)}
        self.assertFalse(ex.g11_check(pairs, 45)["ok"])

    def test_n_fail(self):
        pairs = {ex.MODEL_AGG_EX: self.pair(0.01, -0.001, 44), ex.MODEL_MIX_EX: self.pair(0.4, 0.01)}
        self.assertFalse(ex.g11_check(pairs, 45)["ok"])


class WindowTest(unittest.TestCase):
    def test_primary_is_45_months(self):
        months = grid(date(2022, 1, 1), date(2026, 9, 1))
        self.assertEqual(len(months), 57)
        self.assertEqual(len([m for m in months if m >= ex.PRIMARY_START]), 45)


class LeakTest(unittest.TestCase):
    def test_train_months_strictly_before_test(self):
        captured = []
        original = ex.ols_ex_predict

        def spy(features, target, train_months, test_months, dummies=True, feature=ex.FEATURE_EX, use_feature=True):
            captured.append((test_months[0], list(train_months)))
            return original(features, target, train_months, test_months, dummies, feature, use_feature)

        months = grid(date(2010, 3, 1), date(2014, 12, 1))
        features = {m: {"wkagg_ex_mtd": float(i) / 100.0, "agg_mtd": 1.0} for i, m in enumerate(months)}
        target = {m: 1.0 + features[m]["wkagg_ex_mtd"] for m in months}
        test = months[-6:]
        ex.ols_ex_predict = spy
        try:
            ex.agg_ex_wf(features, target, months, test)
            ex.feature_wf(features, target, months, test, ex.FEATURE_AGG, True)
        finally:
            ex.ols_ex_predict = original
        self.assertEqual(len(captured), 2 * len(test))
        for test_month, train in captured:
            self.assertTrue(train)
            self.assertTrue(all(t < test_month for t in train))


class ControlsTest(unittest.TestCase):
    def test_slice_bias_and_large_errors(self):
        months = grid(date(2023, 7, 1), date(2023, 9, 1))
        truth = {m: 0.0 for m in months}
        errors = {months[0]: 0.2, months[1]: -0.1, months[2]: 0.05}
        ordered = [errors[m] for m in months]
        tariff = ex.slice_rmse(ordered, months, ex.is_tariff_month)
        self.assertEqual(tariff["n"], 1)
        self.assertAlmostEqual(tariff["rmse"], 0.2, places=10)
        outside = ex.slice_rmse(ordered, months, lambda m: not ex.is_tariff_month(m))
        self.assertEqual(outside["n"], 2)
        bias = ex.bias_by_year({"m": errors}, truth, months)
        self.assertAlmostEqual(bias["m"]["2023"]["bias"], 0.05, places=10)
        big = ex.large_errors(errors, truth, months)
        self.assertEqual([row["month"] for row in big], ["2023-07-01"])

    def test_tariff_months(self):
        self.assertTrue(ex.is_tariff_month(date(2023, 7, 1)))
        self.assertTrue(ex.is_tariff_month(date(2025, 12, 1)))
        self.assertTrue(ex.is_tariff_month(date(2026, 1, 1)))
        self.assertFalse(ex.is_tariff_month(date(2023, 1, 1)))
        self.assertTrue(ex.is_tariff_month(date(2026, 7, 1)))


class BasePredictionsTest(unittest.TestCase):
    def test_parse_and_hash(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            path.write_text(
                "arena,model,month,y_pred,t_live,target_full,normal\n"
                "wf2022,combo3,2022-01-01,1.0,0.5,,\n"
                "wf2022,combo3_wk,2022-01-01,1.1,0.5,,\n"
                "wf2022,live_cb_wk,2022-01-01,1.2,0.5,,\n",
                encoding="utf-8",
            )
            old = ex.R12_PREDICTIONS_SHA256
            ex.R12_PREDICTIONS_SHA256 = hashlib.sha256(path.read_bytes()).hexdigest()
            try:
                out = ex.load_base_predictions(path, [date(2022, 1, 1)])
                self.assertEqual(out[ex.MODEL_COMBO][date(2022, 1, 1)], 1.0)
                self.assertEqual(out[ex.MODEL_COMBO_WK][date(2022, 1, 1)], 1.1)
                with_live = ex.load_base_predictions(
                    path, [date(2022, 1, 1)], (ex.MODEL_COMBO, ex.MODEL_LIVE_WK)
                )
                self.assertEqual(with_live[ex.MODEL_LIVE_WK][date(2022, 1, 1)], 1.2)
                with self.assertRaises(SystemExit):
                    ex.load_base_predictions(path, [date(2022, 2, 1)])
            finally:
                ex.R12_PREDICTIONS_SHA256 = old


if __name__ == "__main__":
    unittest.main()
