#!/usr/bin/env python3
"""Офлайн-тесты Task 5 #425: режимы переобучения и гипотеза W5b (спека §17).

Проверяются окна и cutoff годового перефита, правило исключения шоков,
границы rolling, отсутствие утечки, гейт (P1–P5, tie-break, `keep_frozen`),
классификатор гипотезы и отношение пустых горизонтов у контролей.
Архив, R и ClickHouse не используются. Запуск — `make cb-comp-refit-modes-test`.
"""

import math
import sys
import unittest
from datetime import date
from unittest import mock
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

import cpi_two_stage_cb_comp as w5b
from models.cb_comp import cb_comp_refit_modes as rm


def months_range(first: date, last: date) -> list:
    out, current = [], first
    while current <= last:
        out.append(current)
        current = rm.cp.shift_month(current, 1)
    return out


class WindowTest(unittest.TestCase):
    def test_h1_touches_own_month(self):
        self.assertTrue(rm.window_touches(date(2022, 3, 1), 1))
        self.assertFalse(rm.window_touches(date(2023, 1, 1), 1))

    def test_h6_window_overlap(self):
        # 2023-05 h6 = 2022-12…2023-05 — шок 2022-12 входит.
        self.assertTrue(rm.window_touches(date(2023, 5, 1), 6))
        # 2023-06 h6 = 2023-01…2023-06 — шока нет.
        self.assertFalse(rm.window_touches(date(2023, 6, 1), 6))
        # 2015-04 h6 = 2014-11…2015-04 — шок 2014-12…2015-03 входит.
        self.assertTrue(rm.window_touches(date(2015, 4, 1), 6))

    def test_touches_agrees_with_w5b(self):
        for month in (date(2022, 1, 1), date(2022, 12, 1), date(2023, 5, 1), date(2023, 6, 1)):
            for horizon in (1, 3, 6):
                self.assertEqual(rm.window_touches(month, horizon),
                                 w5b.window_touches(month, horizon, w5b.SHOCK_MONTHS))


class TrainMonthsTest(unittest.TestCase):
    def test_cutoff_and_shocks_excluded(self):
        months = rm.train_months(date(2018, 12, 1), 1)
        self.assertEqual(months[0], date(2010, 3, 1))
        self.assertEqual(months[-1], date(2018, 12, 1))
        for shock in (date(2014, 12, 1), date(2015, 1, 1), date(2020, 4, 1), date(2022, 3, 1)):
            self.assertNotIn(shock, months)

    def test_shocks_kept_for_control(self):
        months = rm.train_months(date(2018, 12, 1), 1, exclude_shocks=False)
        self.assertIn(date(2014, 12, 1), months)
        self.assertIn(date(2015, 3, 1), months)

    def test_h6_drops_windows_after_shock(self):
        months = rm.train_months(date(2023, 12, 1), 6)
        self.assertNotIn(date(2023, 5, 1), months)
        self.assertIn(date(2023, 6, 1), months)

    def test_cutoff_none(self):
        self.assertEqual(rm.train_months(None, 1), [])

    def test_rolling_96_bounds(self):
        months = rm.train_months(date(2018, 12, 1), 1, rolling=96)
        self.assertEqual(months[0], date(2011, 1, 1))
        self.assertLessEqual(len(months), 96)

    def test_rolling_120_never_before_window_start(self):
        months = rm.train_months(date(2018, 12, 1), 1, rolling=120)
        self.assertEqual(months[0], date(2010, 3, 1))


class AnnualWindowsTest(unittest.TestCase):
    def test_cutoff_first_origin_h1(self):
        window = rm.annual_windows(1, [2019], rm.MODE_ANNUAL)[2019]
        self.assertEqual(window["cutoff"], date(2018, 12, 1))
        self.assertIn(date(2018, 12, 1), window["train"])
        self.assertNotIn(date(2014, 12, 1), window["train"])

    def test_cutoff_first_origin_h6(self):
        window = rm.annual_windows(6, [2019], rm.MODE_ANNUAL)[2019]
        self.assertEqual(window["cutoff"], date(2018, 7, 1))

    def test_annual_shock_keeps_shocks(self):
        window = rm.annual_windows(1, [2023], rm.MODE_ANNUAL_SHOCK)[2023]
        self.assertEqual(window["cutoff"], date(2022, 12, 1))
        self.assertIn(date(2022, 3, 1), window["train"])

    def test_annual_drops_all_2022_h1(self):
        window = rm.annual_windows(1, [2023], rm.MODE_ANNUAL)[2023]
        for month in months_range(date(2022, 1, 1), date(2022, 12, 1)):
            self.assertNotIn(month, window["train"])
        self.assertEqual(max(window["train"]), date(2021, 12, 1))

    def test_rolling_window_start(self):
        window = rm.annual_windows(1, [2023], "rolling96")[2023]
        # Окно — 96 месяцев до cutoff 2022-12; шоковые 2015-01…03 вырезаны.
        self.assertEqual(min(rm.train_months(date(2022, 12, 1), 1, rolling=96, exclude_shocks=False)),
                         date(2015, 1, 1))
        self.assertEqual(min(window["train"]), date(2015, 4, 1))
        self.assertEqual(max(window["train"]), date(2021, 12, 1))

    def test_no_train_after_cutoff_every_year(self):
        for mode in rm.CANDIDATES[1:]:
            for horizon in rm.HORIZONS:
                for year, window in rm.annual_windows(horizon, list(range(2019, 2025)), mode).items():
                    rm.assert_no_leakage(window["train"], window["cutoff"], horizon)
                    self.assertLessEqual(max(window["train"]), window["cutoff"])


class AnnualFitLeakTest(unittest.TestCase):
    """R14c: годовой фит видит только train-месяцы до cutoff (подмена будущего не влияет)."""

    def _data(self, future_shift: float = 0.0) -> dict:
        months = months_range(date(2016, 1, 1), date(2021, 12, 1))
        features = {"g": {month: {"y_lag1": 0.1, "y_lag2": 0.1, "usd_mm": 0.0, "key_rate": 0.0,
                                  "fao_mm": 0.0, "month": "01"} for month in months}}
        values = {month: 1.0 for month in months}
        for month in months:
            if month >= date(2021, 1, 1):
                values[month] += future_shift
        targets = {horizon: pd.DataFrame({"g": [values[month] for month in months]}, index=months)
                   for horizon in rm.HORIZONS}
        return {
            "specs": [rm.bu.GroupSpec("g", (), 1)], "features": features, "targets": targets,
            "test_months": months_range(date(2021, 1, 1), date(2021, 12, 1)),
        }

    def _run(self, data: dict) -> list:
        recorded = []

        def fake_fit(rows, names, horizon, **kwargs):
            recorded.append((horizon, [month for month, _, _ in rows], [value[horizon] for _, _, value in rows]))
            return object()

        with mock.patch.object(rm.bu, "fit_component", side_effect=fake_fit), \
                mock.patch.object(rm.bu, "predict_component", side_effect=lambda model, rows, names: [0.0] * len(rows)):
            rm.fit_predict_annual(data, data["test_months"], rm.MODE_ANNUAL, pool=1)
        return recorded

    def test_train_rows_stop_at_cutoff(self):
        data = self._data()
        for horizon, months, _values in self._run(data):
            cutoff = rm.annual_windows(horizon, [2021], rm.MODE_ANNUAL)[2021]["cutoff"]
            self.assertTrue(months)
            self.assertLessEqual(max(months), cutoff)
            self.assertNotIn(date(2021, 1, 1), months)

    def test_future_values_do_not_change_train_rows(self):
        base = self._run(self._data(0.0))
        shifted = self._run(self._data(5.0))
        self.assertEqual([(h, months) for h, months, _ in base], [(h, months) for h, months, _ in shifted])
        self.assertEqual([values for _, _, values in base], [values for _, _, values in shifted])


class LeakageTest(unittest.TestCase):
    def test_assert_raises_on_later_month(self):
        with self.assertRaises(AssertionError):
            rm.assert_no_leakage([date(2018, 12, 1), date(2019, 1, 1)], date(2018, 12, 1), 1)

    def test_assert_accepts_cutoff(self):
        rm.assert_no_leakage([date(2018, 11, 1), date(2018, 12, 1)], date(2018, 12, 1), 1)


class SubperiodTest(unittest.TestCase):
    def test_filters_by_year(self):
        months = [date(2019, 1, 1), date(2019, 2, 1), date(2020, 1, 1)]
        out = rm.subperiod([0.3, -0.1, 0.3], months, (2019,))
        self.assertEqual(out["n"], 2)
        self.assertAlmostEqual(out["rmse"], math.sqrt((0.09 + 0.01) / 2))
        self.assertAlmostEqual(out["bias"], 0.1)


def per_h(ratio_normal=1.0, ratio_2023=1.0, ratio_all=1.0, dm_p=1.0, ci=(-0.1, 0.1)):
    return {
        horizon: {
            "n": 54,
            "rmse": ratio_normal,
            "ratio_frozen": ratio_normal,
            "rmse_all67": ratio_all,
            "ratio_all67_frozen": ratio_all,
            "pair_vs_frozen": {"dm_p": dm_p, "delta_ci": list(ci)},
            "subperiods": {"2023_2024": {"rmse": ratio_2023, "n": 19}},
        }
        for horizon in rm.PRIMARY_HORIZONS
    }


def mcs_all(modes=None):
    modes = list(modes or rm.CANDIDATES)
    return {horizon: {"included": modes + ["cb_comp"], "pvalues": {}}
            for horizon in rm.PRIMARY_HORIZONS}


def per_mode(**overrides):
    body = {mode: per_h() for mode in rm.CANDIDATES}
    body.update(overrides)
    return body


class GateTest(unittest.TestCase):
    def test_all_equal_keeps_frozen_by_rank(self):
        out = rm.gate(per_mode(), mcs_all())
        self.assertEqual(out["decision"], "keep_frozen")
        self.assertEqual(out["selected"], rm.MODE_FROZEN)
        self.assertTrue(all(item["ok"] for item in out["candidates"].values()))

    def test_better_annual_selected(self):
        out = rm.gate(per_mode(**{rm.MODE_ANNUAL: per_h(0.98, 0.98, 0.99)}), mcs_all())
        self.assertEqual(out["decision"], "adopt:annual")

    def test_worse_2023_fails(self):
        out = rm.gate(per_mode(**{rm.MODE_ANNUAL: per_h(0.99, 1.05)}), mcs_all())
        self.assertFalse(out["candidates"][rm.MODE_ANNUAL]["ok"])
        self.assertEqual(out["decision"], "keep_frozen")

    def test_not_in_mcs_fails(self):
        modes = [mode for mode in rm.CANDIDATES if mode != rm.MODE_ANNUAL]
        out = rm.gate(per_mode(**{rm.MODE_ANNUAL: per_h(0.95, 0.95, 0.95)}), mcs_all(modes))
        self.assertFalse(out["candidates"][rm.MODE_ANNUAL]["horizons"][1]["p5_mcs"])

    def test_significantly_worse_fails_dm(self):
        out = rm.gate(per_mode(**{rm.MODE_ANNUAL: per_h(0.99, 0.99, 0.99, dm_p=0.01, ci=(0.001, 0.2))}),
                      mcs_all())
        self.assertFalse(out["candidates"][rm.MODE_ANNUAL]["horizons"][1]["p4_dm_ci"])

    def test_nothing_passes_keeps_frozen(self):
        out = rm.gate(per_mode(), mcs_all(["cb_comp"]))
        self.assertEqual(out["passed"], [])
        self.assertEqual(out["decision"], "keep_frozen")


class HypothesisTest(unittest.TestCase):
    def test_support(self):
        out = w5b.hypothesis_verdict(2.5, 1.2, 1.2)
        self.assertTrue(out["reproduce"])
        self.assertTrue(out["mechanism"])
        self.assertEqual(out["verdict"], "support")

    def test_partial_when_mechanism_weak(self):
        out = w5b.hypothesis_verdict(2.5, 2.4, 1.2)
        self.assertTrue(out["reproduce"])
        self.assertFalse(out["mechanism"])
        self.assertEqual(out["verdict"], "partial")

    def test_reject_when_not_reproduced(self):
        out = w5b.hypothesis_verdict(1.1, 1.0, 1.2)
        self.assertFalse(out["reproduce"])
        self.assertIsNone(out["mechanism"])
        self.assertEqual(out["verdict"], "reject")

    def test_boundaries_inclusive(self):
        out = w5b.hypothesis_verdict(1.2 * 1.2, 1.2 * 0.6 + 1.2 * 0.5 * 1.0 - 1.2 * 0.0, 1.2)
        self.assertTrue(out["reproduce"])
        # reduction ровно 0,5 при gap_shock = 1,2·0,2 и gap_noshock = 0,12.
        exact = w5b.hypothesis_verdict(1.44, 1.32, 1.2)
        self.assertAlmostEqual(exact["reduction"], 0.5)
        self.assertTrue(exact["mechanism"])

    def test_live_cb_verdict_uses_h6(self):
        modes = per_mode()
        modes[rm.MODE_WF_SHOCK] = {1: {"rmse": 1.0}, 6: {"rmse": 2.0}}
        modes[rm.MODE_WF_NOSHOCK] = {1: {"rmse": 1.0}, 6: {"rmse": 1.1}}
        modes[rm.MODE_FROZEN] = {**modes[rm.MODE_FROZEN], 6: {"rmse": 1.0}}
        out = rm.hypothesis_live_cb(modes)
        self.assertEqual(out["verdict"], "support")
        self.assertEqual(out["horizons"]["6"]["verdict"], "support")

    def test_live_cb_missing_verdict(self):
        out = rm.hypothesis_live_cb({rm.MODE_FROZEN: per_h()})
        self.assertEqual(out["verdict"], "not_tested")

    def test_overall_matrix(self):
        self.assertEqual(rm.hypothesis_overall({"verdict": "support"}, {"verdict": "support"})["verdict"], "support")
        self.assertEqual(rm.hypothesis_overall({"verdict": "support"}, {"verdict": "reject"})["verdict"], "partial")
        self.assertEqual(rm.hypothesis_overall({"verdict": "reject"}, {"verdict": "reject"})["verdict"], "reject")
        skip = rm.hypothesis_overall(None, {"verdict": "support"})
        self.assertEqual(skip["verdict"], "support")
        self.assertIn("--skip-w5b", skip["note"])


class PinTest(unittest.TestCase):
    def test_exact_pins(self):
        out = rm.pin_check({1: 1.2, 3: 1.0}, {1: 1.2, 3: 1.0})
        self.assertTrue(out["ok"])

    def test_tolerance(self):
        self.assertTrue(rm.pin_check({1: 1.2 + 5e-7}, {1: 1.2})["ok"])
        self.assertFalse(rm.pin_check({1: 1.2 + 2e-6}, {1: 1.2})["ok"])


class EvaluateModesTest(unittest.TestCase):
    def test_empty_horizon_skipped(self):
        normal = months_range(date(2019, 1, 1), date(2019, 6, 1))
        target = {horizon: pd.Series({month: 1.0 for month in normal}) for horizon in rm.HORIZONS}
        data = {"normal": normal, "target_full": target}
        cb_errors = {horizon: [0.1] * len(normal) for horizon in rm.HORIZONS}
        frozen = {horizon: {month: 0.1 for month in normal} for horizon in rm.HORIZONS}
        control = {1: {month: 0.1 for month in normal}}
        raw = rm.error_sets({rm.MODE_FROZEN: frozen, rm.MODE_WF_SHOCK: control}, data)
        out = rm.evaluate_modes(raw, data, cb_errors)
        self.assertIn(1, out[rm.MODE_WF_SHOCK])
        self.assertNotIn(3, out[rm.MODE_WF_SHOCK])
        self.assertEqual(out[rm.MODE_FROZEN][1]["pair_vs_frozen"]["dm_p"], 1.0)


if __name__ == "__main__":
    unittest.main()
