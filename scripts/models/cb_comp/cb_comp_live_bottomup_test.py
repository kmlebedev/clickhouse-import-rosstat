#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_live_bottomup` (Task 2 #422): без ClickHouse, R и архива."""

import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


class ConstantsTest(unittest.TestCase):
    def test_g2_thresholds_match_spec(self):
        self.assertEqual({h: lb.g2_threshold(h) for h in (1, 2, 3, 6)}, {1: 1.264, 2: 1.092, 3: 1.098, 6: 1.05})

    def test_r0_pins(self):
        self.assertEqual(lb.R0_PINNED, {1: 0.191215, 2: 0.308390, 3: 0.463865, 6: 0.971324})
        self.assertEqual(lb.ASOF_START, date(2019, 1, 1))

    def test_default_panel_is_c0(self):
        # Спека §13.1: T_LIVE — C0 везде, поправка §11 (panel_amended.csv) не входит.
        self.assertEqual(lb.PANEL_CSV.name, "panel.csv")


class PanelTest(unittest.TestCase):
    def test_load_group_panel_reads_both_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "p.csv"
            path.write_text(
                "code,month,nsa_mm,sa_full_mm,sa_asof_mm\n"
                "10,2019-01-01,0.5,0.4,0.3\n"
                "10,2019-02-01,0.6,0.45,\n",
                encoding="utf-8",
            )
            full, asof = lb.load_group_panel(path)
        self.assertEqual(full["10"], {date(2019, 1, 1): 0.4, date(2019, 2, 1): 0.45})
        self.assertEqual(asof["10"], {date(2019, 1, 1): 0.3})

    def test_first_sa_month_follows_panel(self):
        self.assertEqual(lb.first_sa_month({"a": {date(2010, 2, 1): 1.0}, "b": {date(2010, 3, 1): 1.0}}),
                         date(2010, 2, 1))
        self.assertEqual(lb.first_sa_month({"a": {date(2009, 1, 1): 1.0}}), date(2009, 1, 1))

    def test_panel_version_must_match(self):
        lb.check_panel_version({"a": {date(2010, 2, 1): 1.0}}, "2010")
        lb.check_panel_version({"a": {date(2009, 1, 1): 1.0}}, "2009")
        with self.assertRaises(SystemExit):
            lb.check_panel_version({"a": {date(2010, 2, 1): 1.0}}, "2009")
        with self.assertRaises(SystemExit):
            lb.check_panel_version({"a": {date(2009, 1, 1): 1.0}}, "2010")

    def test_versioned_defaults_keep_2010_canon(self):
        self.assertEqual(lb.default_panel("2010"), lb.PANEL_CSV)
        self.assertEqual(lb.default_panel("2009"), lb.ROOT / "build" / "cb-comp-groups-2009" / "panel.csv")
        self.assertEqual(lb.versioned_dir(lb.OUT_DIR, "2010"), lb.OUT_DIR)
        self.assertEqual(lb.versioned_dir(lb.OUT_DIR, "2009").name, "cb-comp-live-bottomup-2009")

    def test_asof_series_switches_at_2019_and_keeps_gaps(self):
        full = {"10": {date(2018, 12, 1): 1.0, date(2019, 1, 1): 2.0, date(2019, 2, 1): 3.0}}
        asof = {"10": {date(2018, 12, 1): 9.0, date(2019, 1, 1): 2.5}}
        out = lb.asof_series(full, asof)["10"]
        self.assertEqual(out[date(2018, 12, 1)], 1.0)
        self.assertEqual(out[date(2019, 1, 1)], 2.5)
        self.assertNotIn(date(2019, 2, 1), out)

    def test_log_diff_frame(self):
        frame = lb.log_diff_frame({"a": {date(2020, 1, 1): 1.0}}, ["a", "b"], [date(2020, 1, 1)])
        self.assertAlmostEqual(frame.at[date(2020, 1, 1), "a"], math.log1p(0.01))
        self.assertTrue(np.isnan(frame.at[date(2020, 1, 1), "b"]))


class TargetTest(unittest.TestCase):
    def setUp(self):
        self.weights = {2018: {"a": 0.3, "b": 0.1, bu.UTILITIES_CODE: 0.06}, 2019: {"a": 0.1, "b": 0.3}}

    def test_aggregate_uses_previous_year_weights_and_normalizes(self):
        sa = {"a": {date(2019, 3, 1): 1.0}, "b": {date(2019, 3, 1): 2.0}, bu.UTILITIES_CODE: {date(2019, 3, 1): 50.0}}
        out = lb.aggregate_mm(sa, self.weights, [date(2019, 3, 1)])
        self.assertAlmostEqual(out[date(2019, 3, 1)], (0.3 * 1 + 0.1 * 2) / 0.4)

    def test_aggregate_renormalizes_missing_group(self):
        sa = {"a": {date(2019, 3, 1): 1.0}, "b": {}}
        out = lb.aggregate_mm(sa, self.weights, [date(2019, 3, 1)])
        self.assertAlmostEqual(out[date(2019, 3, 1)], 1.0)

    def test_cumulative_matches_target_full_formula(self):
        mm = {date(2020, m, 1): v for m, v in zip((1, 2, 3), (1.0, 2.0, -0.5))}
        out = lb.cumulative(mm, 3)
        self.assertEqual(list(out), [date(2020, 3, 1)])
        self.assertAlmostEqual(out[date(2020, 3, 1)], (1.01 * 1.02 * 0.995 - 1) * 100)
        self.assertAlmostEqual(lb.cumulative(mm, 1)[date(2020, 2, 1)], 2.0)

    def test_t_live_all_horizons(self):
        months = grid(date(2019, 1, 1), date(2019, 12, 1))
        sa = {"a": {m: 0.5 for m in months}, "b": {m: 0.5 for m in months}}
        out = lb.t_live(sa, self.weights, months)
        self.assertEqual(set(out), {1, 2, 3, 6})
        self.assertAlmostEqual(out[6][date(2019, 12, 1)], (1.005 ** 6 - 1) * 100)

    def test_t_live_starts_at_first_sa_month(self):
        months = grid(date(2009, 1, 1), date(2009, 12, 1))
        sa = {"a": {m: 0.5 for m in months}}
        out = lb.t_live(sa, self.weights, months)
        self.assertEqual(min(out[1]), date(2009, 1, 1))
        self.assertEqual(min(out[2]), date(2009, 2, 1))
        self.assertEqual(min(out[6]), date(2009, 6, 1))


class ModelTest(unittest.TestCase):
    """Сквозной прогон на синтетике: цель, признаки, модели, агрегат."""

    def test_fit_predict_and_aggregate(self):
        months = grid(date(2010, 1, 1), date(2019, 12, 1))
        rng = np.random.default_rng(3)
        sa = {code: {m: float(rng.normal(0.4, 0.2)) for m in months} for code in ("a", "b")}
        specs = [bu.GroupSpec(code="a", bsu_codes=(), level=3), bu.GroupSpec(code="b", bsu_codes=(), level=3)]
        diff = lb.log_diff_frame(sa, ["a", "b"], months)
        targets = bu.component_targets(diff)
        macro = {m: {"usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1} for m in months}
        train = grid(date(2010, 3, 1), date(2018, 12, 1))
        test = grid(date(2019, 1, 1), date(2019, 12, 1))
        features = bu.feature_table(specs, train + test, diff, macro, {}, feature_mode="mean")
        preds = lb.fit_predict(specs, features, features, targets, train, test, with_mp=False)
        self.assertEqual(set(preds), {"a", "b"})
        self.assertEqual(set(preds["a"]), {1, 2, 3, 6})
        self.assertEqual(set(preds["a"][1]), set(test))
        weights = {2018: {"a": 0.5, "b": 0.5}}
        agg = lb.aggregate_predictions(preds, weights, test)
        expected = 0.5 * preds["a"][1][date(2019, 5, 1)] + 0.5 * preds["b"][1][date(2019, 5, 1)]
        self.assertAlmostEqual(agg[1][date(2019, 5, 1)], expected / 1.0)

    def test_test_features_change_predictions_not_models(self):
        months = grid(date(2010, 1, 1), date(2019, 12, 1))
        rng = np.random.default_rng(5)
        sa = {"a": {m: float(rng.normal(0.4, 0.3)) for m in months}}
        shifted = {"a": {m: (v + 5.0 if m >= date(2019, 1, 1) else v) for m, v in sa["a"].items()}}
        specs = [bu.GroupSpec(code="a", bsu_codes=(), level=3)]
        diff = lb.log_diff_frame(sa, ["a"], months)
        diff_shift = lb.log_diff_frame(shifted, ["a"], months)
        targets = bu.component_targets(diff)
        macro = {m: {"usd_mm": 0.0, "key_rate": 7.0, "fao_mm": 0.1} for m in months}
        train = grid(date(2010, 3, 1), date(2018, 12, 1))
        test = grid(date(2019, 3, 1), date(2019, 12, 1))
        f_full = bu.feature_table(specs, train + test, diff, macro, {}, feature_mode="mean")
        f_shift = bu.feature_table(specs, train + test, diff_shift, macro, {}, feature_mode="mean")
        base = lb.fit_predict(specs, f_full, f_full, targets, train, test, with_mp=False)
        again = lb.fit_predict(specs, f_full, f_full, targets, train, test, with_mp=False)
        moved = lb.fit_predict(specs, f_full, f_shift, targets, train, test, with_mp=False)
        self.assertEqual(base, again)
        self.assertNotEqual(base["a"][1], moved["a"][1])


class EvalTest(unittest.TestCase):
    def test_g2_check(self):
        self.assertTrue(lb.g2_check(1.264 * 0.2, 0.2, 1)["ok"])
        self.assertFalse(lb.g2_check(1.30 * 0.2, 0.2, 1)["ok"])
        self.assertFalse(lb.g2_check(1.06, 1.0, 6)["ok"])

    def test_r0_check_tolerance(self):
        ok = lb.r0_check({h: v + 5e-7 for h, v in lb.R0_PINNED.items()})
        self.assertTrue(ok["ok"])
        bad = lb.r0_check({**lb.R0_PINNED, 6: 0.9714})
        self.assertFalse(bad["ok"])

    def test_errors_on_aligns(self):
        months = [date(2020, 1, 1), date(2020, 2, 1), date(2020, 3, 1)]
        common, errs = lb.errors_on({months[0]: 1.0, months[2]: 3.0}, {months[0]: 0.5, months[1]: 1.0, months[2]: 2.0}, months)
        self.assertEqual(common, [months[0], months[2]])
        self.assertEqual(errs, [0.5, 1.0])

    def test_target_gap(self):
        months = [date(2020, 1, 1), date(2020, 2, 1)]
        tf = pd.DataFrame({1: [1.0, 2.0]}, index=months)
        out = lb.target_gap({months[0]: 1.1, months[1]: 1.9}, tf, 1, months)
        self.assertEqual(out["n"], 2)
        self.assertAlmostEqual(out["rmse"], 0.1)
        self.assertAlmostEqual(out["bias"], 0.0)


if __name__ == "__main__":
    unittest.main()
