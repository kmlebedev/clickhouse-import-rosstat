#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h3_h6_var.py` — Task 12 #440; без ClickHouse и архива."""

import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_h3_h6_var as v


def month(year: int, number: int) -> date:
    return date(year, number, 1)


def synthetic_endo(months: int = 60, seed: int = 7) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    values = np.zeros((months, 4))
    for index in range(1, months):
        values[index] = 0.5 * values[index - 1] + rng.normal(0.0, 0.05, size=4)
    index = pd.date_range("2016-01-01", periods=months, freq="MS")
    return pd.DataFrame(values, index=index, columns=list(v.ENDOG_COLUMNS))


class PanelTest(unittest.TestCase):
    def test_monthly_rate_takes_last_point_of_month(self):
        points = [(date(2016, 1, 1), 11.0), (date(2016, 1, 20), 10.5), (date(2016, 2, 1), 10.0)]
        self.assertEqual(v.monthly_rate(points, [date(2016, 1, 1)])[date(2016, 1, 1)], 10.5)
        self.assertEqual(v.monthly_rate(points, [date(2016, 2, 1)])[date(2016, 2, 1)], 10.0)
        with self.assertRaises(SystemExit):
            v.monthly_rate(points, [date(2015, 1, 1)])

    def test_monthly_usd_averages_calendar_month_and_logs(self):
        points = [(date(2016, 1, 4), 70.0), (date(2016, 1, 5), 80.0), (date(2016, 2, 1), 90.0)]
        values = v.monthly_usd_log(points, [date(2016, 1, 1), date(2016, 2, 1)])
        self.assertAlmostEqual(values[date(2016, 1, 1)], math.log(75.0))
        self.assertAlmostEqual(values[date(2016, 2, 1)], math.log(90.0))
        with self.assertRaises(SystemExit):
            v.monthly_usd_log(points, [date(2016, 3, 1)])

    def test_monthly_pe_is_level(self):
        survey = {"price_expectations_sa": [(date(2016, 1, 1), 12.0), (date(2016, 2, 1), 12.5)]}
        self.assertEqual(v.monthly_pe(survey, [date(2016, 1, 1)])[date(2016, 1, 1)], 12.0)
        self.assertEqual(v.monthly_pe(survey, [date(2016, 2, 1)])[date(2016, 2, 1)], 12.5)
        with self.assertRaises(SystemExit):
            v.monthly_pe(survey, [date(2016, 3, 1)])

    def test_build_panel_diffs_and_endo_start(self):
        months = [date(2015, 12, 1), date(2016, 1, 1), date(2016, 2, 1)]
        mm = {month: 0.5 for month in months}
        rate = {months[0]: 11.0, months[1]: 10.0, months[2]: 10.0}
        usd = {months[0]: 4.0, months[1]: 4.1, months[2]: 4.2}
        pe = {month: 10.0 for month in months}
        panel, endo, dummies = v.build_panel(mm, rate, usd, pe, months)
        self.assertEqual(len(panel), 3)
        self.assertEqual(len(endo), 2)
        self.assertAlmostEqual(panel.loc[pd.Timestamp(months[1]), "d_rate"], -1.0)
        self.assertAlmostEqual(endo.iloc[0]["cpi"], math.log1p(0.5 / 100))
        self.assertTrue(dummies.empty)
        self.assertFalse(endo.isna().any().any())

    def test_build_panel_rejects_gap(self):
        with self.assertRaises(SystemExit):
            v.build_panel({}, {}, {}, {}, [date(2015, 12, 1), date(2016, 2, 1)])

    def test_build_panel_rejects_nan(self):
        months = [date(2015, 12, 1), date(2016, 1, 1), date(2016, 2, 1)]
        mm = {months[0]: 0.1, months[2]: 0.2}
        with self.assertRaises(SystemExit):
            v.build_panel(mm, {m: 10.0 for m in months}, {m: 4.0 for m in months}, {m: 10.0 for m in months}, months)


class LagTest(unittest.TestCase):
    def test_eligible_lag(self):
        self.assertTrue(v.eligible_lag(36, 6, 0))
        self.assertFalse(v.eligible_lag(30, 6, 0))
        self.assertTrue(v.eligible_lag(30, 5, 0))
        self.assertFalse(v.eligible_lag(24, 1, 0))
        self.assertFalse(v.eligible_lag(30, 2, 20))

    def test_bic_selects_one_lag_for_var1(self):
        train = synthetic_endo(120)
        lag, bics, errors = v.select_lag(train, None)
        self.assertEqual(lag, 1)
        self.assertIsNotNone(bics[1])
        self.assertEqual(errors[1], "")

    def test_select_lag_records_ineligible_reason(self):
        train = synthetic_endo(30)
        _, _, errors = v.select_lag(train, None)
        self.assertIn("not identified", errors[6])

    def test_select_lag_stops_when_nothing_identified(self):
        with self.assertRaises(SystemExit):
            v.select_lag(synthetic_endo(20), None)


class ForecastTest(unittest.TestCase):
    def test_var_forecast_shape_and_finiteness(self):
        train = synthetic_endo(60)
        path = v.var_forecast(train, 2, 6, None)
        self.assertEqual(path.shape, (6, 4))
        self.assertTrue(np.isfinite(path).all())

    def test_cumulative_sums_first_steps(self):
        path = np.zeros((3, 4))
        path[0, v.CPI_COLUMN] = math.log(1.01)
        path[1, v.CPI_COLUMN] = math.log(1.02)
        path[2, v.CPI_COLUMN] = math.log(1.03)
        self.assertAlmostEqual(v.cumulative(path, 1), 1.0)
        self.assertAlmostEqual(v.cumulative(path, 2), (1.01 * 1.02 - 1) * 100)

    def test_cumulative_rejects_nan(self):
        path = np.zeros((2, 4))
        path[0, v.CPI_COLUMN] = float("nan")
        with self.assertRaises(SystemExit):
            v.cumulative(path, 1)

    def test_future_values_do_not_change_forecast(self):
        endo = synthetic_endo(60)
        dummies = pd.DataFrame(index=endo.index)
        test_months = [month(2019, 1)]
        var_first, bvar_first, _ = v.var_forecasts(endo, dummies, test_months)
        changed = endo.copy()
        changed.loc[pd.Timestamp("2019-01-01"):] += 100.0
        var_second, bvar_second, _ = v.var_forecasts(changed, dummies, test_months)
        self.assertEqual(var_first[1][month(2019, 1)], var_second[1][month(2019, 1)])
        self.assertEqual(bvar_first[1][month(2019, 1)], bvar_second[1][month(2019, 1)])

    def test_dummies_only_from_training_sample(self):
        index = pd.date_range("2016-01-01", periods=48, freq="MS")
        dummies = pd.DataFrame({
            "outlier_2018-01-01": [1.0 if day == pd.Timestamp("2018-01-01") else 0.0 for day in index],
            "outlier_2020-04-01": [1.0 if day == pd.Timestamp("2020-04-01") else 0.0 for day in index],
        }, index=index)
        train_index = index[:index.get_loc(pd.Timestamp("2019-01-01"))]
        exog = v.training_exog(dummies, train_index)
        self.assertIsNotNone(exog)
        self.assertEqual(list(exog.columns), ["outlier_2018-01-01"])

    def test_design_matrix_accepts_dataframe_exog(self):
        train = synthetic_endo(12)
        exog = pd.DataFrame({"dummy": [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}, index=train.index)
        x_matrix, y_matrix = v.design_matrix(train, 2, exog)
        self.assertEqual(x_matrix.shape, (10, 1 + 8 + 1))
        self.assertEqual(y_matrix.shape, (10, 4))
        self.assertEqual(x_matrix[0, -1], 1.0)

    def test_bvar_prior_own_first_lag_is_random_walk(self):
        train = synthetic_endo(60)
        b0, omega = v.minnesota_prior(train, 2, 0)
        self.assertEqual(b0.shape, (1 + 8, 4))
        for equation in range(4):
            own_first = 1 + equation
            self.assertEqual(b0[own_first, equation], 1.0)
            self.assertGreater(omega[own_first, equation], 0)
        self.assertTrue(np.isfinite(omega).all())

    def test_bvar_forecast_is_deterministic(self):
        train = synthetic_endo(60)
        first = v.bvar_forecast(train, 2, 6, None)
        second = v.bvar_forecast(train, 2, 6, None)
        np.testing.assert_allclose(first, second)
        self.assertEqual(first.shape, (6, 4))
        self.assertTrue(np.isfinite(first).all())

    def test_bvar_with_diffuse_prior_matches_ols(self):
        train = synthetic_endo(80)
        lag = 2
        x_matrix, y_matrix = v.design_matrix(train, lag, None)
        ols = np.linalg.lstsq(x_matrix, y_matrix, rcond=None)[0]
        bvar = v.bvar_coefficients(train, lag, None, prior_scale=1e12)
        np.testing.assert_allclose(bvar, ols, rtol=1e-5, atol=1e-7)

    def test_bvar_own_lag_is_not_prior_dominated(self):
        rng = np.random.default_rng(11)
        values = np.zeros((160, 4))
        for index in range(1, len(values)):
            values[index] = 0.3 * values[index - 1] + rng.normal(0.0, 0.005, size=4)
        train = pd.DataFrame(
            values,
            index=pd.date_range("2016-01-01", periods=len(values), freq="MS"),
            columns=list(v.ENDOG_COLUMNS),
        )
        coefficients = v.bvar_coefficients(train, 1, None)
        own = float(coefficients[1, 0])
        self.assertGreater(own, 0.0)
        self.assertLess(own, 0.8)


class CombinationTest(unittest.TestCase):
    def test_combo_is_equal_weight_mean(self):
        combo3 = {horizon: {month(2019, 1): 1.0} for horizon in v.HORIZONS}
        var = {horizon: {month(2019, 1): 3.0} for horizon in v.HORIZONS}
        combined = v.combine_combo3_var16(combo3, var)
        for horizon in v.HORIZONS:
            self.assertEqual(combined[horizon][month(2019, 1)], 2.0)

    def test_combo_excludes_month_without_pair(self):
        combo3 = {horizon: {month(2019, 1): 1.0, month(2019, 2): 2.0} for horizon in v.HORIZONS}
        var = {horizon: {month(2019, 1): 3.0} for horizon in v.HORIZONS}
        combined = v.combine_combo3_var16(combo3, var)
        for horizon in v.HORIZONS:
            self.assertEqual(sorted(combined[horizon]), [month(2019, 1)])


def gate_metrics(ratios_combo4_h1=1.03, delta3=-0.1, p3=0.05, delta6=-0.1, p6=0.05, mcs=("combo3_var16",)):
    ratios = {model: {horizon: 1.0 for horizon in v.HORIZONS} for model in v.CANDIDATES}
    ratios[v.MODEL_COMBO3][1] = 1.03
    ratios[v.MODEL_COMBO4][1] = ratios_combo4_h1
    pair = lambda delta, p: {"delta": delta, "dm_p": p, "delta_ci": [delta - 0.1, delta + 0.1]}
    pairs = {model: {horizon: pair(0.0, 0.5) for horizon in v.HORIZONS} for model in v.CANDIDATES if model != v.MODEL_COMBO3}
    pairs[v.MODEL_COMBO4][3] = pair(delta3, p3)
    pairs[v.MODEL_COMBO4][6] = pair(delta6, p6)
    mcs_map = {horizon: {"included": list(mcs), "pvalues": {}} for horizon in v.PRIMARY_HORIZONS}
    return {"ratios": ratios, "pairs_vs_combo3": pairs, "mcs": mcs_map}


class GateTest(unittest.TestCase):
    def test_better_than_uses_alpha_and_direction(self):
        self.assertTrue(v.better_than({"delta": -0.01, "dm_p": 0.099}))
        self.assertFalse(v.better_than({"delta": -0.01, "dm_p": 0.10}))
        self.assertFalse(v.better_than({"delta": 0.01, "dm_p": 0.01}))

    def test_g12_passes_on_h3(self):
        gate = v.g12_check(gate_metrics(delta3=-0.1, p3=0.05, delta6=0.1, p6=0.5))
        self.assertTrue(gate["better_h3_or_h6"])
        self.assertTrue(gate["ok"])

    def test_g12_passes_on_h6(self):
        gate = v.g12_check(gate_metrics(delta3=0.1, p3=0.5, delta6=-0.1, p6=0.05))
        self.assertTrue(gate["ok"])

    def test_g12_fails_without_improvement(self):
        gate = v.g12_check(gate_metrics(delta3=0.1, p3=0.5, delta6=0.1, p6=0.5))
        self.assertFalse(gate["ok"])

    def test_g12_h1_margin(self):
        self.assertTrue(v.g12_check(gate_metrics(ratios_combo4_h1=1.05))["h1"]["ok"])
        self.assertFalse(v.g12_check(gate_metrics(ratios_combo4_h1=1.0501))["h1"]["ok"])

    def test_g12_requires_mcs_inclusion(self):
        gate = v.g12_check(gate_metrics(mcs=("combo3",)))
        self.assertFalse(gate["mcs_ok"])
        self.assertFalse(gate["ok"])


class R12Test(unittest.TestCase):
    def test_r12a_pins(self):
        ratios = {model: dict(pinned) for model, pinned in v.R12A_PINNED.items()}
        self.assertTrue(v.r12a_check(ratios)["ok"])
        ratios["combo3"][1] += 1e-5
        self.assertFalse(v.r12a_check(ratios)["ok"])

    def test_r12b_valid_panel(self):
        months = [date(2015, 12, 1), date(2016, 1, 1), date(2016, 2, 1)]
        panel = pd.DataFrame(
            {"cpi": [0.0, 0.1, 0.2], "rate": [10.0, 10.0, 10.0], "fx": [4.0, 4.0, 4.0], "pe": [10.0, 10.0, 10.0],
             "d_rate": [np.nan, 0.0, 0.0], "d_fx": [np.nan, 0.0, 0.0]},
            index=pd.date_range("2015-12-01", periods=3, freq="MS"),
        )
        endo = panel.loc[pd.Timestamp("2016-01-01"):, list(v.ENDOG_COLUMNS)]
        self.assertTrue(v.r12b_check(panel, endo)["ok"])
        broken = panel.copy()
        broken.loc[pd.Timestamp("2016-01-01"), "pe"] = np.nan
        self.assertFalse(v.r12b_check(broken, endo)["ok"])

    def test_r12c_requires_all_months(self):
        months = [month(2019, 1)]
        data = {"normal": months, "test_months": months}
        forecasts = {model: {horizon: {months[0]: 1.0} for horizon in v.HORIZONS} for model in v.CANDIDATES}
        lag_info = {month(2019, 1): {"lag": 1, "n_train": 36, "bics": {1: 1.0}}}
        self.assertTrue(v.r12c_check(forecasts, data, lag_info)["ok"])
        del forecasts[v.MODEL_VAR][3][months[0]]
        self.assertFalse(v.r12c_check(forecasts, data, lag_info)["ok"])

    def test_write_predictions(self):
        forecasts = {model: {horizon: {month(2019, 1): 1.5} for horizon in v.HORIZONS} for model in v.CANDIDATES}
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "predictions.csv"
            v.write_predictions(path, forecasts)
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(lines[0], "model,horizon,month,y_pred")
            self.assertEqual(len(lines), 1 + len(v.CANDIDATES) * len(v.HORIZONS))


if __name__ == "__main__":
    unittest.main()
