#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h1_shadow` (P0.6 #453): без ClickHouse и архива."""

import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_h1_shadow as sh
from models.cb_comp import cb_comp_h1_weekly_ex as ex
from models.cpi import cpi_monthly as cm
from models.cpi import cpi_public_panel_audit as audit

SHA = "0" * 64


def payload(**overrides) -> dict:
    base = {
        "version": sh.VERSION, "month": "2026-10", "cutoff": "2026-11-08", "run_date": "2026-10-31",
        "git_sha": "abc", "models": {name: 0.3 for name in sh.SHADOW_MODELS},
        "served": {"model": sh.MODEL_MIX_EX, "value": 0.3, "fallback_level": 0},
        "asof": {}, "inputs_sha256": {"panel_csv": SHA}, "truth": sh.TRUTH,
    }
    base.update(overrides)
    return base


class ConstantsTest(unittest.TestCase):
    def test_protocol_frozen(self):
        self.assertEqual(sh.VERSION, "wk_mix_ex@v1")
        self.assertEqual(sh.V1_CODE_SHA, "264ce73")
        self.assertEqual(sh.SHADOW_MODELS, ("combo3", "wk_mix_ex", "wk_mix_d", "live_cb_wk"))
        self.assertEqual(sh.FALLBACK_CHAIN, ("wk_mix_ex", "wk_mix_d", "combo3"))
        self.assertEqual(sh.VINTAGE_DAYS, (7, 14, 21, 28))
        self.assertEqual(sh.BIAS_FLAG, 0.15)
        self.assertEqual(ex.UTIL_COV_MIN, 0.5)

    def test_r_p0_pins_task4b(self):
        self.assertAlmostEqual(sh.R_P0_PINNED["combo3"], 0.2094, places=4)
        self.assertAlmostEqual(sh.R_P0_PINNED["wk_mix_ex"], 0.1411, places=4)


class TruthTest(unittest.TestCase):
    def test_t_live_rt_uses_asof_and_weights(self):
        weights = {year: {"A": 0.6, "B": 0.4, "9470": 0.5} for year in (2022, 2023, 2024)}
        months = [date(2024, 1, 1), date(2024, 2, 1)]
        sa_asof = {"A": {months[0]: 1.0, months[1]: 0.5}, "B": {months[0]: 0.0, months[1]: 0.5}, "9470": {months[0]: 9.0}}
        got = sh.t_live_rt(sa_asof, ["A", "B", "9470"], weights, months)
        self.assertAlmostEqual(got[months[0]], 0.6, places=12)
        self.assertAlmostEqual(got[months[1]], 0.5, places=12)

    def test_missing_group_renormalises(self):
        weights = {2023: {"A": 0.6, "B": 0.4}}
        month = date(2024, 3, 1)
        got = sh.t_live_rt({"A": {month: 1.0}}, ["A", "B"], weights, [month])
        self.assertAlmostEqual(got[month], 1.0, places=12)

    def test_gap(self):
        months = [date(2024, 1, 1), date(2024, 2, 1)]
        got = sh.gap({months[0]: 0.3, months[1]: 0.1}, {months[0]: 0.2, months[1]: 0.2}, months)
        self.assertAlmostEqual(got["bias"], 0.0, places=12)
        self.assertAlmostEqual(got["rmse"], 0.1, places=12)
        self.assertAlmostEqual(got["max_abs"], 0.1, places=12)


class FallbackTest(unittest.TestCase):
    values = {"combo3": 0.2, "wk_mix_ex": 0.3, "wk_mix_d": 0.4}

    def test_level0_candidate(self):
        self.assertEqual(sh.serve(self.values, {"agg_mtd": 0.5, "util_cov": 0.83}), ("wk_mix_ex", 0.3, 0))

    def test_level1_without_utilities(self):
        self.assertEqual(sh.serve(self.values, {"agg_mtd": 0.5, "util_cov": 0.4}), ("wk_mix_d", 0.4, 1))
        self.assertEqual(sh.serve({**self.values, "wk_mix_ex": float("nan")}, {"agg_mtd": 0.5, "util_cov": 0.9}),
                         ("wk_mix_d", 0.4, 1))

    def test_level2_without_aggregate(self):
        self.assertEqual(sh.serve(self.values, {"agg_mtd": float("nan"), "util_cov": 0.9}), ("combo3", 0.2, 2))
        self.assertEqual(sh.serve(self.values, {}), ("combo3", 0.2, 2))

    def test_no_combo_fails(self):
        with self.assertRaises(SystemExit):
            sh.serve({"combo3": float("nan")}, {})

    def test_served_series(self):
        months = [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)]
        models = {name: {m: value for m in months} for name, value in self.values.items()}
        features = {months[0]: {"agg_mtd": 0.1, "util_cov": 0.9}, months[1]: {"agg_mtd": 0.1, "util_cov": 0.1}}
        values, levels = sh.served_series(models, features, months)
        self.assertEqual([levels[m] for m in months], [0, 1, 2])
        self.assertEqual([values[m] for m in months], [0.3, 0.4, 0.2])


class VintageTest(unittest.TestCase):
    def test_cutoff_override_restores(self):
        month = date(2024, 1, 1)
        late = cm.cutoff_for(month)
        with sh.cutoff_override(sh.vintage_cutoff(14)):
            self.assertEqual(cm.cutoff_for(month), date(2024, 1, 14))
        self.assertEqual(cm.cutoff_for(month), late)
        with self.assertRaises(RuntimeError):
            with sh.cutoff_override(sh.vintage_cutoff(7)):
                raise RuntimeError
        self.assertEqual(cm.cutoff_for(month), late)

    def test_vintage_drops_later_points(self):
        points = [(date(2024, 1, 8), 101.0), (date(2024, 1, 15), 102.0), (date(2024, 1, 22), 101.0), (date(2024, 1, 29), 103.0)]
        data = {audit.WEEKLY_AGGREGATE: points}
        util = {date(2024, 1, 1): {"wk_g_mtd": 1.0, "wk_g_cov": 0.83}}
        weights = {2023: {"9470": 0.06}}
        late = ex.agg_ex_features(data, [date(2024, 1, 1)], util, weights)[date(2024, 1, 1)]
        with sh.cutoff_override(sh.vintage_cutoff(14)):
            early = ex.agg_ex_features(data, [date(2024, 1, 1)], util, weights)[date(2024, 1, 1)]
        self.assertLess(early["agg_cov"], late["agg_cov"])
        self.assertNotAlmostEqual(early["agg_mtd"], late["agg_mtd"])


class FlagsTest(unittest.TestCase):
    def test_holds(self):
        good = {"pairs_vs_combo3": {"wk_mix_ex": {"rmse_left": 0.1, "rmse_right": 0.2, "delta_ci": [-0.1, -0.01]}}}
        wide = {"pairs_vs_combo3": {"wk_mix_ex": {"rmse_left": 0.1, "rmse_right": 0.2, "delta_ci": [-0.1, 0.01]}}}
        self.assertTrue(sh.holds(good))
        self.assertFalse(sh.holds(wide))
        self.assertFalse(sh.holds({"pairs_vs_combo3": {}}))

    def test_bias_flag(self):
        per_year = {"2024": {"bias": 0.05}, "2025": {"bias": 0.16}, "2026": {"bias": -0.2}}
        self.assertEqual(sh.bias_flag(per_year), ["2025", "2026"])


class ShadowTest(unittest.TestCase):
    def test_timing(self):
        month = date(2026, 10, 1)
        self.assertEqual(sh.check_shadow_timing(month, date(2026, 10, 31), date(2026, 9, 1)), date(2026, 11, 8))
        self.assertEqual(sh.check_shadow_timing(month, date(2026, 11, 8), date(2026, 9, 1)), date(2026, 11, 8))
        with self.assertRaises(SystemExit):
            sh.check_shadow_timing(month, date(2026, 11, 9), date(2026, 9, 1))
        with self.assertRaises(SystemExit):
            sh.check_shadow_timing(month, date(2026, 10, 31), date(2026, 10, 1))

    def test_validate_ok(self):
        sh.validate_shadow(payload())

    def test_validate_rejects(self):
        for bad in (
            payload(run_date="2026-11-09"),
            payload(models={"combo3": 0.2}),
            payload(served={"model": "wk_agg_ex", "value": 0.1, "fallback_level": 0}),
            payload(inputs_sha256={"panel_csv": "abc"}),
        ):
            with self.assertRaises(SystemExit):
                sh.validate_shadow(bad)
        broken = payload()
        del broken["asof"]
        with self.assertRaises(SystemExit):
            sh.validate_shadow(broken)

    def test_s0_and_reference(self):
        months = [date(2026, 8, 1), date(2026, 9, 1)]
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            path.write_text(
                "arena,model,month,y_pred,t_live\n"
                "wf2022,combo3,2026-08-01,0.2,0.1\nwf2022,combo3,2026-09-01,0.3,0.1\n"
                "frozen,combo3,2026-09-01,9.0,0.1\n",
                encoding="utf-8",
            )
            ref = sh.read_reference(path, "wf2022", ("combo3",), months)
            self.assertEqual(ref["combo3"], {months[0]: 0.2, months[1]: 0.3})
            self.assertEqual(sh.read_reference(Path(tmp) / "none.csv", "wf2022", ("combo3",), months), {"combo3": {}})
        same = sh.s0_check({"combo3": {months[0]: 0.2, months[1]: 0.3}}, ref, months)
        self.assertTrue(same["identical"])
        moved = sh.s0_check({"combo3": {months[0]: 0.2, months[1]: 0.31}}, ref, months)
        self.assertFalse(moved["identical"])
        self.assertAlmostEqual(moved["combo3"]["max_abs_diff"], 0.01, places=12)
        self.assertFalse(sh.s0_check({"combo3": {}}, {"combo3": {}}, months)["identical"])

    def test_dump_sorted_and_nan(self):
        text = sh.dump({"b": 1, "a": float("nan")})
        self.assertLess(text.index('"a"'), text.index('"b"'))
        self.assertTrue(math.isfinite(len(text)))


class VersionTest(unittest.TestCase):
    """Task 16 #466: версия цели @2009 — свои каталоги, пины и незасчитываемый shadow."""

    def test_paths_by_version(self):
        canon = sh.version_paths("2010")
        alt = sh.version_paths("2009")
        self.assertEqual(canon["shadow_dir"], sh.SHADOW_DIR)
        self.assertEqual(alt["shadow_dir"].name, "cb-comp-h1-shadow-2009")
        self.assertEqual(alt["out"].name, "cb-comp-h1-shadow-2009")
        self.assertEqual(alt["base_predictions"].parent.name, "cb-comp-h1-weekly-2009")
        self.assertEqual(alt["ex_predictions"].parent.name, "cb-comp-h1-weekly-ex-2009")
        self.assertEqual(canon["base_predictions"], ex.BASE_PREDICTIONS)

    def test_r_p0_pins(self):
        months = [date(2023, 1, 1), date(2023, 2, 1)]
        truth = {m: 0.0 for m in months}
        wrong = {sh.MODEL_COMBO: {m: 1.0 for m in months}, sh.MODEL_MIX_EX: {m: 1.0 for m in months}}
        self.assertFalse(sh.r_p0_check(wrong, truth, months, "2010")["ok"])
        old = dict(sh.R_P0_PINS["2009"])
        sh.R_P0_PINS["2009"].clear()
        try:
            unpinned = sh.r_p0_check(wrong, truth, months, "2009")
            self.assertTrue(unpinned["ok"])
            self.assertFalse(unpinned["pinned"])
        finally:
            sh.R_P0_PINS["2009"].update(old)

    def test_train_start_rules(self):
        first = date(2009, 1, 1)
        ex.check_train_start(ex.TRAIN_START, "2010", first)
        with self.assertRaises(SystemExit):
            ex.check_train_start(date(2009, 3, 1), "2010", date(2010, 2, 1))
        ex.check_train_start(date(2009, 3, 1), "2009", first)
        with self.assertRaises(SystemExit):
            ex.check_train_start(date(2009, 2, 1), "2009", first)
        months = [date(2010, 3, 1), date(2010, 4, 1)]
        self.assertEqual(ex.train_months(months, first, ex.TRAIN_START), months)
        extended = ex.train_months(months, first, date(2009, 3, 1))
        self.assertEqual(extended[0], date(2009, 3, 1))
        self.assertEqual(extended[-1], date(2010, 4, 1))
        self.assertEqual(len(extended), 14)

    def test_r12_unpinned_version_records_only(self):
        evaluation = {"per_model": {ex.MODEL_COMBO: {"rmse": 0.5}, ex.MODEL_COMBO_WK: {"rmse": 0.4}}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            path.write_text("arena,model,month,y_pred\n", encoding="utf-8")
            old = dict(ex.R12_PINS["2009"])
            ex.R12_PINS["2009"].update({"rmse": {}, "sha256": None})
            try:
                out = ex.r12_check(evaluation, path, "2009")
                self.assertTrue(out["ok"])
                self.assertFalse(out["pinned"])
            finally:
                ex.R12_PINS["2009"].clear()
                ex.R12_PINS["2009"].update(old)
            self.assertFalse(ex.r12_check(evaluation, path, "2010")["ok"])


if __name__ == "__main__":
    unittest.main()
