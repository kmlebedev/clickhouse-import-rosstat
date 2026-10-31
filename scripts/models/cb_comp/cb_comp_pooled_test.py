#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_pooled` (Task 10 #438): без ClickHouse и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb
from models.cb_comp import cb_comp_pooled as pooled


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


def spec(code: str, sheets=("trade_retail",)) -> bu.GroupSpec:
    return bu.GroupSpec(code=code, bsu_codes=("01.1",), level=7, sheet_slugs=tuple(sheets))


def features_for(specs: list, months: list) -> dict:
    """Базовые признаки и свой листовой блок; чужие листы не заполняются."""
    out = {}
    for index, item in enumerate(specs):
        per_month = {}
        for position, month in enumerate(months):
            values = {
                "y_lag1": 0.1 * index + 0.01 * position,
                "y_lag2": 0.2 * index - 0.01 * position,
                "usd_mm": 0.5,
                "key_rate": 7.5,
                "fao_mm": 0.3,
                "month": month.month,
            }
            for sheet in item.sheet_slugs:
                for indicator in ("ibk", "costs", "price_exp"):
                    base = f"mp_sheet_{bu.feature_code(sheet)}_{indicator}"
                    values[base] = 1.0 + index
                    values[f"{base}_chg"] = 0.1 * index
            per_month[month] = values
        out[item.code] = per_month
    return out


def targets_for(specs: list, months: list) -> dict:
    out = {}
    for horizon in pooled.HORIZONS:
        frame = pd.DataFrame(
            {item.code: [0.05 * (i + 1) + 0.01 * horizon + 0.001 * j for j in range(len(months))]
             for i, item in enumerate(specs)},
            index=pd.Index(months),
        )
        out[horizon] = frame
    return out


def sa_for(specs: list, months: list) -> dict:
    return {item.code: {month: 0.1 for month in months} for item in specs}


class FeatureSchemaTest(unittest.TestCase):
    def test_names_are_75_with_66_sheet_columns(self):
        sheets = ["agriculture", "construction", "manufacturing_consumer", "manufacturing_intermediate",
                  "manufacturing_investment", "mining", "services", "trade_auto", "trade_retail",
                  "trade_wholesale", "transport"]
        names = pooled.pooled_feature_names(sheets)
        self.assertEqual(len(names), 75)  # 5 база + 66 МП + 4 id/лист/вес/month
        self.assertEqual(names[:5], list(pooled.BASE_NUMERIC))
        self.assertEqual(names[-4:], ["group_code", "sheet_profile", "weight_share", "month"])
        self.assertEqual(len([name for name in names if name.startswith("mp_sheet_")]), 66)
        self.assertEqual(pooled.cat_indices(names), [names.index(name) for name in pooled.CAT_FEATURES])

    def test_row_uses_only_own_sheet_block(self):
        item = spec("a", ("trade_retail",))
        values = {
            "y_lag1": 1.0, "y_lag2": 2.0, "usd_mm": 3.0, "key_rate": 4.0, "fao_mm": 5.0,
            "month": 3, "mp_sheet_trade_retail_ibk": 6.0, "mp_sheet_trade_retail_ibk_chg": 7.0,
            "mp_sheet_services_ibk": 99.0,
        }
        row = pooled.PooledRow(date(2019, 3, 1), "a", date(2019, 3, 1), values, 1.0,
                               pooled.own_mp_names(item), "trade_retail", 0.1)
        self.assertEqual(pooled.row_value(row, "mp_sheet_trade_retail_ibk"), 6.0)
        self.assertTrue(math.isnan(pooled.row_value(row, "mp_sheet_services_ibk")))
        self.assertEqual(pooled.row_value(row, "group_code"), "a")
        self.assertEqual(pooled.row_value(row, "sheet_profile"), "trade_retail")
        self.assertEqual(pooled.row_value(row, "weight_share"), 0.1)
        self.assertEqual(pooled.row_value(row, "month"), 3)

    def test_group_without_sheet_has_no_own_block(self):
        item = spec("b", ())
        self.assertEqual(pooled.own_mp_names(item), frozenset())
        self.assertEqual(pooled.sheet_profile(item), "none")


class WeightShareTest(unittest.TestCase):
    def test_share_is_over_groups_with_sa_and_excludes_utilities(self):
        month = date(2017, 1, 1)
        weights = {2016: {"a": 1.0, "b": 3.0, bu.UTILITIES_CODE: 1000.0}}
        sa = {"a": {month: 0.1}, "b": {month: 0.1}, bu.UTILITIES_CODE: {month: 0.1}}
        shares = pooled.weight_shares(weights, sa, [month], 1)
        self.assertAlmostEqual(shares[month]["a"], 0.25)
        self.assertAlmostEqual(shares[month]["b"], 0.75)
        self.assertTrue(math.isnan(shares[month][bu.UTILITIES_CODE]))

    def test_missing_lag_year_is_nan_not_fallback_to_future(self):
        month = date(2016, 1, 1)
        weights = {2016: {"a": 1.0}}
        sa = {"a": {month: 0.1}}
        shares = pooled.weight_shares(weights, sa, [month], 1)
        self.assertEqual(shares[month], {})  # 2015 весов нет — никакого fallback

    def test_group_without_sa_is_not_in_denominator(self):
        month = date(2019, 1, 1)
        weights = {2018: {"a": 1.0, "b": 1.0}}
        sa = {"a": {month: 0.1}, "b": {}}
        shares = pooled.weight_shares(weights, sa, [month], 1)
        self.assertAlmostEqual(shares[month]["a"], 1.0)

    def test_share_uses_origin_not_target_composition(self):
        target = date(2019, 6, 1)
        origin = date(2019, 1, 1)
        weights = {2018: {"a": 1.0, "b": 1.0}}
        at_origin = {"a": {origin: 0.1}, "b": {origin: 0.1}}
        target_only = {"a": {origin: 0.1}, "b": {target: 0.1}}  # b появился только в цели
        first = pooled.weight_shares(weights, at_origin, [target], 6)
        second = pooled.weight_shares(weights, target_only, [target], 6)
        self.assertAlmostEqual(first[target]["a"], 0.5)
        self.assertAlmostEqual(second[target]["a"], 1.0)  # состав на target не влияет

    def test_share_uses_weight_year_of_origin(self):
        target = date(2020, 1, 1)
        origin = date(2019, 8, 1)  # target − (h−1), h=6
        weights = {2018: {"a": 1.0, "b": 3.0}, 2019: {"a": 1.0, "b": 1.0}}
        sa = {"a": {origin: 0.1}, "b": {origin: 0.1}}
        shares = pooled.weight_shares(weights, sa, [target], 6)
        self.assertAlmostEqual(shares[target]["a"], 0.25)  # 2018, не 2019 (0,5)


class WindowsTest(unittest.TestCase):
    def test_cutoff_uses_availability_not_year_end(self):
        self.assertEqual(pooled.training_cutoff(date(2016, 1, 1), date(2019, 1, 1)), date(2018, 12, 1))
        self.assertEqual(pooled.training_cutoff(date(2016, 1, 1), date(2018, 11, 1)), date(2018, 10, 1))
        self.assertEqual(pooled.training_cutoff(date(2016, 1, 1), date(2018, 8, 1)), date(2018, 7, 1))

    def test_annual_window_has_no_test_month_in_train(self):
        for horizon in pooled.HORIZONS:
            windows = pooled.annual_windows(date(2016, 1, 1), horizon, (2019, 2020))
            for year, info in windows.items():
                months = pooled.train_months(date(2016, 1, 1), horizon, info["cutoff"])
                self.assertLessEqual(max(months), info["cutoff"])
                self.assertNotIn(date(year, 1, 1), months)
                self.assertEqual(info["origin_min"], bu.shift_month(date(year, 1, 1), -(horizon - 1)))

    def test_article_outliers_are_excluded(self):
        months = pooled.train_months(date(2016, 1, 1), 1, date(2021, 12, 1))
        for month in (date(2020, 4, 1), date(2022, 1, 1), date(2022, 12, 1)):
            self.assertNotIn(month, months)
        self.assertIn(date(2021, 12, 1), months)

    def test_h6_target_windows_with_shocks_are_excluded(self):
        months = pooled.train_months(date(2016, 1, 1), 6, date(2023, 7, 1))
        for month in (date(2023, 1, 1), date(2023, 5, 1)):
            self.assertNotIn(month, months)  # окно включает 2022-08…12
        self.assertIn(date(2023, 6, 1), months)
        self.assertIn(date(2023, 7, 1), months)

    def test_control_window_starts_2010_03(self):
        windows = pooled.annual_windows(date(2010, 3, 1), 1, (2019,))
        self.assertLess(windows[2019]["cutoff"], date(2019, 1, 1))
        self.assertEqual(pooled.WINDOW_START["control"], date(2010, 3, 1))


class RowsTest(unittest.TestCase):
    def setUp(self):
        self.specs = [spec("a"), spec("b", ("services",))]
        self.months = grid(date(2015, 1, 1), date(2019, 12, 1))
        self.features = features_for(self.specs, self.months)
        self.targets = targets_for(self.specs, self.months)
        self.sa = sa_for(self.specs, self.months)
        self.weights = {2016: {"a": 1.0, "b": 1.0}, 2017: {"a": 1.0, "b": 1.0}, 2018: {"a": 1.0, "b": 1.0}}

    def shares(self, horizon: int) -> dict:
        return pooled.weight_shares(self.weights, self.sa, self.months, horizon)

    def test_training_rows_use_origin_h_minus_one(self):
        rows = pooled.training_rows(self.specs, self.features, self.targets, self.shares(6), 6, [date(2018, 6, 1)])
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].origin, date(2018, 1, 1))

    def test_nan_target_and_missing_origin_are_skipped(self):
        self.targets[1].loc[date(2018, 6, 1), "a"] = np.nan
        rows = pooled.training_rows(self.specs, self.features, self.targets, self.shares(1), 1, [date(2018, 6, 1)])
        self.assertEqual([row.code for row in rows], ["b"])
        rows = pooled.training_rows(self.specs, self.features, self.targets, self.shares(6), 6, [date(2015, 1, 1)])
        self.assertEqual(rows, [])  # origin 2014-08 вне признаков

    def test_prediction_rows_have_nan_target(self):
        rows = pooled.prediction_rows(self.specs, self.features, self.shares(3), 3, [date(2019, 5, 1)])
        self.assertEqual(len(rows), 2)
        self.assertTrue(all(math.isnan(row.target) for row in rows))
        self.assertEqual(rows[0].origin, date(2019, 3, 1))


class FitPredictTest(unittest.TestCase):
    def test_pooled_is_deterministic_and_covers_all_groups(self):
        specs = [spec("a"), spec("b", ("services",)), spec("c", ())]
        months = grid(date(2015, 1, 1), date(2019, 12, 1))
        features = features_for(specs, months)
        targets = targets_for(specs, months)
        sa = sa_for(specs, months)
        weights = {2016: {item.code: 1.0 for item in specs}, 2017: {item.code: 1.0 for item in specs},
                   2018: {item.code: 1.0 for item in specs}}
        test_months = grid(date(2019, 1, 1), date(2019, 12, 1))
        overrides = {"iterations": 5, "thread_count": 1}
        first, report, names, sheets = pooled.fit_predict_pooled(
            specs, features, targets, weights, sa, date(2016, 1, 1), test_months, overrides)
        second, _, _, _ = pooled.fit_predict_pooled(
            specs, features, targets, weights, sa, date(2016, 1, 1), test_months, overrides)
        self.assertEqual(first, second)
        self.assertEqual(len(names), 5 + 12 + 4)  # 2 листа × 3 индикатора × 2
        self.assertEqual(sheets, ["services", "trade_retail"])
        for horizon in pooled.HORIZONS:
            self.assertEqual(set(first[horizon]), {"a", "b", "c"})
            self.assertEqual(len(first[horizon]["a"]), len(test_months))
        self.assertEqual(report["6"]["2019"]["cutoff"], date(2018, 7, 1))


class AggregateTest(unittest.TestCase):
    def test_weights_lag_and_missing_group(self):
        month = date(2019, 1, 1)
        predictions = {
            1: {"a": {month: 2.0}, "b": {month: 4.0}, "c": {}},
        }
        weights = {2018: {"a": 1.0, "b": 1.0, "c": 1.0}}
        aggregated, coverage = pooled.aggregate_pooled(predictions, weights, [month])
        self.assertAlmostEqual(aggregated[1][month], 3.0)
        self.assertEqual(coverage[1][month], {"predicted": 2, "weighted": 2})

    def test_missing_month_is_not_aggregated(self):
        month = date(2019, 1, 1)
        predictions = {1: {"a": {month: 2.0}, "b": {}}}
        weights = {2018: {"a": 1.0, "b": 1.0}}
        aggregated, _coverage = pooled.aggregate_pooled(predictions, weights, [month])
        self.assertAlmostEqual(aggregated[1][month], 2.0)


class ComboPooledTest(unittest.TestCase):
    def test_combo_is_mean_of_aggregated_series(self):
        pooled_series = {horizon: {date(2019, 1, 1): 1.0 * horizon} for horizon in pooled.HORIZONS}
        bs = {
            "bs_econ16": {horizon: {date(2019, 1, 1): 2.0 * horizon} for horizon in pooled.HORIZONS},
            "bs_rozn16": {horizon: {date(2019, 1, 1): 3.0 * horizon} for horizon in pooled.HORIZONS},
        }
        combo = pooled.combo_with_pooled(pooled_series, bs)
        for horizon in pooled.HORIZONS:
            self.assertAlmostEqual(combo[horizon][date(2019, 1, 1)], 2.0 * horizon)

    def test_common_months_only(self):
        month, other = date(2019, 1, 1), date(2019, 2, 1)
        pooled_series = {horizon: {month: 1.0, other: 1.0} for horizon in pooled.HORIZONS}
        bs = {
            "bs_econ16": {horizon: {month: 2.0} for horizon in pooled.HORIZONS},
            "bs_rozn16": {horizon: {month: 3.0} for horizon in pooled.HORIZONS},
        }
        combo = pooled.combo_with_pooled(pooled_series, bs)
        self.assertEqual(sorted(combo[1]), [month])


class GateTest(unittest.TestCase):
    @staticmethod
    def pair(left: float, right: float, p: float) -> dict:
        return {"rmse_left": left, "rmse_right": right, "delta": left - right,
                "delta_ci": [left - right - 0.01, left - right + 0.01], "dm_p": p}

    def pairs(self, h1, h3, h6) -> dict:
        return {"pooled_main_vs_combo3": {1: h1, 3: h3, 6: h6}}

    def test_pass_on_h3_with_h1_not_worse(self):
        gate = pooled.g10_check(self.pairs(self.pair(0.16, 0.17, 0.5), self.pair(0.40, 0.45, 0.05),
                                           self.pair(0.90, 0.87, 0.5)))
        self.assertTrue(gate["ok"])
        self.assertTrue(gate["h3"]["ok"])

    def test_pass_on_h6_when_h3_not_significant(self):
        gate = pooled.g10_check(self.pairs(self.pair(0.16, 0.17, 0.5), self.pair(0.40, 0.45, 0.2),
                                           self.pair(0.80, 0.87, 0.05)))
        self.assertTrue(gate["ok"])

    def test_fail_when_neither_h3_nor_h6_significant(self):
        gate = pooled.g10_check(self.pairs(self.pair(0.16, 0.17, 0.5), self.pair(0.40, 0.45, 0.2),
                                           self.pair(0.88, 0.87, 0.2)))
        self.assertFalse(gate["ok"])

    def test_fail_when_h1_worse(self):
        gate = pooled.g10_check(self.pairs(self.pair(0.18, 0.17, 0.5), self.pair(0.40, 0.45, 0.05),
                                           self.pair(0.90, 0.87, 0.5)))
        self.assertFalse(gate["ok"])

    def test_equal_h3_is_not_lower(self):
        gate = pooled.g10_check(self.pairs(self.pair(0.16, 0.17, 0.5), self.pair(0.45, 0.45, 0.01),
                                           self.pair(0.90, 0.87, 0.5)))
        self.assertFalse(gate["ok"])


class LeakAndR10Test(unittest.TestCase):
    @staticmethod
    def report(train_months: list, test_months: list, cutoff: date, max_target: date, horizon="1") -> dict:
        return {"main": {horizon: {"2019": {"origin_min": date(2019, 1, 1), "cutoff": cutoff,
                                            "train_months": train_months, "test_months": test_months,
                                            "max_target": max_target, "n_rows": 1, "n_groups": 1}}}}

    def test_leak_check_detects_test_month_in_train(self):
        good = self.report([date(2018, 12, 1)], [date(2019, 1, 1)], date(2018, 12, 1), date(2018, 12, 1))
        self.assertTrue(pooled.leak_check(good)["ok"])
        bad = self.report([date(2019, 1, 1)], [date(2019, 1, 1)], date(2018, 12, 1), date(2019, 1, 1))
        self.assertFalse(pooled.leak_check(bad)["ok"])

    def test_leak_check_is_independent_of_cutoff(self):
        # max_target == cutoff, но для h6 цель 2018-08 не опубликована к origin 2018-08.
        report = self.report([date(2018, 8, 1)], [date(2019, 1, 1)], date(2018, 8, 1), date(2018, 8, 1), horizon="6")
        result = pooled.leak_check(report)
        self.assertFalse(result["ok"])
        self.assertGreater(result["windows"]["main:h6:2019"]["n_violations"], 0)

    def test_r10_pins_and_coverage(self):
        ratios = {
            "live_cb": dict(pooled.TASK3_LIVE_CB_RATIOS),
            "combo3": dict(pooled.TASK9_COMBO_RATIOS),
        }
        reports = self.report([date(2018, 12, 1)], [date(2019, 1, 1)], date(2018, 12, 1), date(2018, 12, 1))
        inputs = dict(pooled.EXPECTED_INPUTS)
        coverage = {horizon: {date(2019, 1, 1): {"predicted": 87, "weighted": 85}} for horizon in pooled.HORIZONS}
        expected = {date(2019, 1, 1): 85}
        result = pooled.r10_check(ratios, reports, inputs, ["9218"], coverage, expected)
        self.assertTrue(result["ok"])
        ratios["combo3"][1] += 0.01
        self.assertFalse(pooled.r10_check(ratios, reports, inputs, ["9218"], coverage, expected)["ok"])

    def test_r10d_detects_predicted_and_weighted_gaps(self):
        ratios = {
            "live_cb": dict(pooled.TASK3_LIVE_CB_RATIOS),
            "combo3": dict(pooled.TASK9_COMBO_RATIOS),
        }
        reports = self.report([date(2018, 12, 1)], [date(2019, 1, 1)], date(2018, 12, 1), date(2018, 12, 1))
        inputs = dict(pooled.EXPECTED_INPUTS)
        expected = {date(2019, 1, 1): 85}
        short = {horizon: {date(2019, 1, 1): {"predicted": 86, "weighted": 85}} for horizon in pooled.HORIZONS}
        self.assertFalse(pooled.r10_check(ratios, reports, inputs, ["9218"], short, expected)["ok"])
        mismatch = {horizon: {date(2019, 1, 1): {"predicted": 87, "weighted": 84}} for horizon in pooled.HORIZONS}
        self.assertFalse(pooled.r10_check(ratios, reports, inputs, ["9218"], mismatch, expected)["ok"])


class PassportTest(unittest.TestCase):
    def test_jsonable_converts_dates_and_nan(self):
        value = lb._jsonable({"date": date(2019, 1, 1), "nan": float("nan"), "np": np.float64(1.5),
                              "items": (date(2020, 1, 1), np.int64(2))})
        self.assertEqual(value["date"], "2019-01-01")
        self.assertIsNone(value["nan"])
        self.assertEqual(value["np"], 1.5)
        self.assertEqual(value["items"], ["2020-01-01", 2])

    def test_sample_hash_is_stable_and_order_sorted(self):
        first = {"main": {"1": {"2019": {"train_months": [date(2018, 12, 1)], "test_months": [],
                                          "cutoff": date(2018, 12, 1), "max_target": date(2018, 12, 1),
                                          "origin_min": date(2019, 1, 1), "n_rows": 1, "n_groups": 1}}}}
        second = {"main": {"1": {"2019": {"train_months": [date(2018, 12, 1)], "test_months": [],
                                           "cutoff": date(2018, 12, 1), "max_target": date(2018, 12, 1),
                                           "origin_min": date(2019, 1, 1), "n_rows": 2, "n_groups": 2}}}}
        self.assertEqual(pooled.sample_hash(first), pooled.sample_hash(second))
        changed = {"main": {"1": {"2019": {"train_months": [date(2018, 11, 1)], "test_months": [],
                                            "cutoff": date(2018, 12, 1), "max_target": date(2018, 12, 1),
                                            "origin_min": date(2019, 1, 1), "n_rows": 1, "n_groups": 1}}}}
        self.assertNotEqual(pooled.sample_hash(first), pooled.sample_hash(changed))


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(pooled.WINDOW_START["main"], date(2016, 1, 1))
        self.assertEqual(pooled.WINDOW_START["control"], date(2010, 3, 1))
        self.assertEqual(pooled.G10_ALPHA, 0.10)
        self.assertEqual(pooled.GROUPS_WITHOUT_SA, ("9218",))
        self.assertEqual(pooled.GROUPS_EXPECTED, 87)
        self.assertEqual(len(pooled.TASK3_LIVE_CB_RATIOS), 4)
        self.assertEqual(len(pooled.TASK9_COMBO_RATIOS), 4)
        self.assertEqual(pooled.TEST_YEARS, (2019, 2020, 2021, 2022, 2023, 2024))


if __name__ == "__main__":
    unittest.main(verbosity=2)
