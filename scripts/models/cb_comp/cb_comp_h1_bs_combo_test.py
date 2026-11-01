#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_h1_bs_combo` (Task 9 #436): без ClickHouse и архива."""

import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_h1_bs_combo as bs
from models.cb_comp import cb_comp_mlog as mlog


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


def synthetic(first=date(2012, 1, 1), last=date(2024, 7, 1), a=0.1, b=0.02, c=0.5, horizon=1, seed=7):
    """Ряд, порождённый ровно моделью BS: T(t) = a + b·X(t−h) + c·T(t−h)."""
    rng = np.random.default_rng(seed)
    months = grid(first, last)
    x = {m: float(v) for m, v in zip(months, rng.normal(10, 5, len(months)))}
    target = {}
    for i, m in enumerate(months):
        if i < horizon:
            target[m] = float(rng.normal(0.5, 0.1))
        else:
            lag = months[i - horizon]
            target[m] = a + b * x[lag] + c * target[lag]
    return months, x, target


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(bs.BS_START, date(2016, 1, 1))
        self.assertEqual(bs.CANDIDATES, ("live_cb", "bs_econ16", "bs_rozn16", "combo3"))
        self.assertEqual(bs.G9_RATIO_H1, 1.10)
        self.assertEqual(bs.NOHARM_MARGIN, 0.02)
        self.assertEqual(bs.R9A_EXPECTED, {"bs_econ16": 0.167225, "bs_rozn16": 0.169438})
        self.assertEqual(bs.TASK3_LIVE_CB_RATIOS, {1: 1.193, 2: 1.013, 3: 1.076, 6: 1.062})
        self.assertEqual(bs.VERSIONS, ("2010", "2009"))
        self.assertEqual(bs.TASK3_PINS["2010"], bs.TASK3_LIVE_CB_RATIOS)

    def test_r9c_accepts_version_pin(self):
        ratios = {1: 1.25, 2: 1.05, 3: 1.10, 6: 1.09}
        out = bs.r9c_check(ratios, {h: ratios[h] for h in ratios})
        self.assertTrue(out["ok"])

    def test_outliers_are_article_set(self):
        self.assertIn(date(2015, 1, 1), bs.ARTICLE_OUTLIERS)
        self.assertIn(date(2020, 4, 1), bs.ARTICLE_OUTLIERS)
        self.assertIn(date(2022, 7, 1), bs.ARTICLE_OUTLIERS)
        self.assertEqual(len(bs.ARTICLE_OUTLIERS), 17)


class TrainingRowsTest(unittest.TestCase):
    def test_bounds_h1(self):
        months, x, target = synthetic()
        origin = date(2019, 5, 1)
        rows = bs.training_months(target, x, 1, origin)
        self.assertEqual(rows[0], date(2016, 2, 1))  # 2016-01 + h
        self.assertEqual(rows[-1], date(2019, 4, 1))  # M − h
        self.assertNotIn(origin, rows)

    def test_bounds_h3_purge(self):
        months, x, target = synthetic(horizon=3)
        rows = bs.training_months(target, x, 3, date(2019, 5, 1))
        self.assertEqual(rows[0], date(2016, 4, 1))
        self.assertEqual(rows[-1], date(2019, 2, 1))  # таргет до M−h включительно

    def test_outliers_excluded(self):
        months, x, target = synthetic()
        rows = bs.training_months(target, x, 1, date(2023, 6, 1))
        for m in bs.ARTICLE_OUTLIERS:
            self.assertNotIn(m, rows)
        self.assertIn(date(2021, 12, 1), rows)
        self.assertIn(date(2023, 1, 1), rows)

    def test_missing_points_skip_row(self):
        months, x, target = synthetic()
        del x[date(2017, 5, 1)]  # нужен строке 2017-06 при h1
        rows = bs.training_months(target, x, 1, date(2019, 1, 1))
        self.assertNotIn(date(2017, 6, 1), rows)
        self.assertIn(date(2017, 5, 1), rows)

    def test_features_use_exact_lag(self):
        months, x, target = synthetic(horizon=2)
        t = date(2018, 5, 1)
        self.assertEqual(bs.row_features(target, x, t, 2), [x[date(2018, 3, 1)], target[date(2018, 3, 1)]])
        self.assertIsNone(bs.row_features(target, {}, t, 2))


class PredictTest(unittest.TestCase):
    def test_recovers_exact_model(self):
        for h in (1, 3):
            months, x, target = synthetic(horizon=h)
            tests = grid(date(2019, 1, 1), date(2019, 12, 1))
            pred = bs.bs_predict(target, x, h, tests)
            for m in tests:
                self.assertAlmostEqual(pred[m], target[m], places=8)

    def test_no_future_information(self):
        months, x, target = synthetic()
        tests = [date(2019, 3, 1)]
        base = bs.bs_predict(target, x, 1, tests)
        spoiled_target = dict(target)
        spoiled_x = dict(x)
        for m in months:
            if m >= date(2019, 3, 1):
                spoiled_target[m] = 1e6
            if m >= date(2019, 3, 1):  # X(M) не участвует: признак строки M — X(M−1)
                spoiled_x[m] = 1e6
        spoiled_target[date(2019, 3, 1)] = target[date(2019, 3, 1)]
        self.assertAlmostEqual(bs.bs_predict(spoiled_target, spoiled_x, 1, tests)[tests[0]], base[tests[0]], places=10)

    def test_missing_features_no_prediction(self):
        months, x, target = synthetic()
        del x[date(2019, 2, 1)]
        pred = bs.bs_predict(target, x, 1, [date(2019, 3, 1)])
        self.assertNotIn(date(2019, 3, 1), pred)


class CombineTest(unittest.TestCase):
    def test_mean_on_common_months(self):
        a = {date(2019, 1, 1): 1.0, date(2019, 2, 1): 2.0}
        b = {date(2019, 1, 1): 3.0, date(2019, 2, 1): 4.0}
        c = {date(2019, 1, 1): 5.0}
        self.assertEqual(bs.combine_mean([a, b, c]), {date(2019, 1, 1): 3.0})


class AuthorsProcedureTest(unittest.TestCase):
    def test_matches_notebook(self):
        idx = pd.date_range("2016-01-01", "2020-12-01", freq="MS")
        rng = np.random.default_rng(3)
        series = pd.DataFrame({"YH1": rng.normal(0.4, 0.2, len(idx)), "X1": rng.normal(12, 3, len(idx))}, index=idx)
        out = [pd.Timestamp(m) for m in bs.ARTICLE_OUTLIERS]
        # Ноутбук авторов дословно (data_lag_long, regime lag).
        d = series[["YH1", "X1"]].copy()
        d["X1"] = d["X1"].shift(1)
        d["ipc_lag"] = d["YH1"].shift(1)
        d = d.dropna()
        test = d[d.index.year >= 2019]
        train = d[~d.index.isin(test.index)]
        train = train[~train.index.isin(out)]
        expected = test["YH1"].copy()
        for i in range(test.shape[0]):
            tr = pd.concat([train, test.iloc[:i]])
            tr = tr[~tr.index.isin(out)]
            lm = LinearRegression().fit(tr.loc[:, tr.columns != "YH1"], tr["YH1"])
            expected.iloc[i] = lm.predict(test.loc[:, test.columns != "YH1"].iloc[i:i + 1])[0]
        got = bs.authors_bs_predict(series, "X1", 1)
        pd.testing.assert_series_equal(got, expected, check_names=False)


class GateTest(unittest.TestCase):
    def pair(self, p, high):
        return {"dm_p": p, "delta_ci": [high - 0.01, high]}

    def test_pass(self):
        self.assertTrue(bs.g9_check(1.10, self.pair(0.049, -0.0001))["ok"])

    def test_ratio_fail(self):
        self.assertFalse(bs.g9_check(1.1001, self.pair(0.01, -0.01))["ok"])

    def test_dm_fail(self):
        self.assertFalse(bs.g9_check(1.0, self.pair(0.05, -0.01))["ok"])

    def test_ci_with_zero_fail(self):
        self.assertFalse(bs.g9_check(1.0, self.pair(0.01, 0.0))["ok"])

    def test_noharm(self):
        ok = bs.noharm_check({3: 1.096, 6: 1.082}, {3: 1.076, 6: 1.062})
        self.assertTrue(ok["ok"])
        bad = bs.noharm_check({3: 1.0961, 6: 1.0}, {3: 1.076, 6: 1.062})
        self.assertFalse(bad["ok"])

    def test_r9c(self):
        self.assertTrue(bs.r9c_check({1: 1.1934, 2: 1.013, 3: 1.0755, 6: 1.062})["ok"])
        self.assertFalse(bs.r9c_check({1: 1.195, 2: 1.013, 3: 1.076, 6: 1.062})["ok"])


class MlogKwargsTest(unittest.TestCase):
    """P0.5: контракт паспорта — experiment, folds, sample и имена окон."""

    months = [date(2024, 1, 1), date(2024, 2, 1)]

    def kwargs(self, version="2010"):
        forecasts = {
            model: {h: {month: 1.0 for month in self.months} for h in bs.HORIZONS}
            for model in bs.CANDIDATES
        }
        target = {h: {month: 2.0 for month in self.months} for h in bs.HORIZONS}
        data = {"normal": self.months,
                "train_months": [date(2010, 3, 1), date(2018, 12, 1)],
                "t_live": {h: {month: 1.5 for month in self.months} for h in bs.HORIZONS}}
        result = {"spec": "docs/spec.md", "ratios": {"combo3": {1: 1.0}},
                  "g9": {"ok": True}, "noharm": {"ok": True}, "mcs": {},
                  "inputs": {"panel_csv": "abc"}}
        checks = {"r9a": {"ok": True}, "r9b": {"ok": True}}
        return bs.mlog_kwargs(forecasts, target, data, self.months, result, checks,
                              version, Path("build/cb-comp-h1-bs-combo"))

    def test_contract(self):
        kwargs = self.kwargs()
        self.assertEqual(kwargs["run_name"], "combo3__h1-h6__normal54__target_full__2010")
        self.assertEqual(kwargs["config"]["normal"], len(self.months))
        self.assertEqual(kwargs["config"]["truth"], mlog.TRUTH)
        self.assertEqual(kwargs["config"]["models"], list(bs.CANDIDATES))
        self.assertEqual(kwargs["data_asof"], bs.cp.TEST_END)
        self.assertEqual(len(kwargs["sample"]), len(bs.HORIZONS) * len(self.months))
        self.assertEqual([path.name for path in kwargs["artifacts"]],
                         ["predictions.csv", "result.json"])

    def test_fold_rows_cover_candidates(self):
        kwargs = self.kwargs(version="2009")
        rows = mlog.build_fold_rows(kwargs["aggregated"], kwargs["truth"], kwargs["months"],
                                    kwargs["models"], kwargs["horizons"],
                                    t_live=kwargs["t_live"], version=kwargs["version"])
        self.assertEqual(len(rows), len(bs.CANDIDATES) * len(bs.HORIZONS) * len(self.months))
        self.assertEqual({row.window for row in rows}, {"h1", "h2", "h3", "h6"})
        self.assertEqual({row.meta["version"] for row in rows}, {"2009"})
        self.assertEqual({row.meta["truth"] for row in rows}, {"target_full"})


if __name__ == "__main__":
    unittest.main()
