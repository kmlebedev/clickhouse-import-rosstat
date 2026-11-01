#!/usr/bin/env python3
"""Офлайн-тесты W5b: сравнение с CB_COMP на их тесте (протокол спеки §4 W5b).

Соответствие `M` (конец окна) ↔ строка `O = M−h+1`; обучающие строки `*_frozen` (конец окна 2010-03…2018-12
без 2014-12…2015-03; цели из винтажа на 2019-01); `*_wf` (фолды W3, цели из винтажа тестового origin'а,
purge, `min_train`); признаки `v5_direct` (NaN до 2016-01); классификация «лучше/паритет/хуже/не определено».
Подменяемая модель вместо CatBoost; архив, R, ClickHouse не используются. Запуск —
`make cpi-two-stage-cb-comp-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_cb_comp as cc
import cpi_two_stage_folds as tf
from models.cpi import cpi_monthly as cm


def months_range(first: date, n: int) -> list:
    out, year, month = [], first.year, first.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def shift(m: date, k: int) -> date:
    i = m.year * 12 + (m.month - 1) + k
    return date(i // 12, i % 12 + 1, 1)


class MappingTest(unittest.TestCase):
    def test_window_end_to_origin(self):
        self.assertEqual(cc.origin_for(date(2019, 1, 1), 1), date(2019, 1, 1))
        self.assertEqual(cc.origin_for(date(2019, 1, 1), 3), date(2018, 11, 1))
        self.assertEqual(cc.origin_for(date(2019, 1, 1), 6), date(2018, 8, 1))

    def test_origin_window_end_roundtrip(self):
        for h in cc.HORIZONS:
            for m in months_range(date(2019, 1, 1), 12):
                o = cc.origin_for(m, h)
                self.assertEqual(shift(o, h - 1), m)

    def test_test_months(self):
        self.assertEqual(len(cc.TEST_MONTHS), 67)
        self.assertEqual(len(cc.NORMAL_MONTHS), 54)
        self.assertEqual(cc.HORIZONS, (1, 2, 3, 6))
        self.assertEqual(cc.PRIMARY_HORIZONS, (1, 3, 6))


class FrozenTrainTest(unittest.TestCase):
    def candidates(self):
        return months_range(date(2002, 5, 1), 300)

    def test_h1_has_102_months_without_outliers(self):
        train = cc.frozen_train_origins(1, self.candidates())
        self.assertEqual(len(train), 102)
        for outlier in cc.TRAIN_OUTLIER_MONTHS:
            self.assertNotIn(outlier, train)
        self.assertEqual((train[0], train[-1]), (date(2010, 3, 1), date(2018, 12, 1)))

    def test_origins_shift_with_horizon_and_window_ends_before_2019(self):
        train = cc.frozen_train_origins(6, self.candidates())
        self.assertEqual(len(train), 102)
        self.assertEqual(train[0], date(2009, 10, 1))  # 2010-03 − 5 мес.
        for o in train:
            self.assertLessEqual(shift(o, 5), date(2018, 12, 1))

    def test_missing_candidates_are_skipped(self):
        cands = [m for m in self.candidates() if m != date(2012, 6, 1)]
        self.assertEqual(len(cc.frozen_train_origins(1, cands)), 101)


class V5RowsTest(unittest.TestCase):
    def row(self, month):
        return cm.Row(month=month, y={}, features={"month": month.month, "y_lag1": 0.1})

    def test_names_insert_weekly_before_month(self):
        names = cc.v5_names()
        self.assertEqual(names[-1], "month")
        self.assertEqual(names[-3:-1], ["sa4w_last", "sa4w_partial"])
        self.assertEqual(names[:-3], cc.v3_names()[:-1])

    def test_nan_before_2016(self):
        row = cc.v5_row(self.row(date(2015, 12, 1)), {"sa4w_last": 0.4, "sa4w_partial": 0.5})
        self.assertTrue(math.isnan(row.features["sa4w_last"]))
        self.assertTrue(math.isnan(row.features["sa4w_partial"]))

    def test_values_from_2016(self):
        row = cc.v5_row(self.row(date(2016, 1, 1)), {"sa4w_last": 0.4, "sa4w_partial": 0.5})
        self.assertEqual(row.features["sa4w_last"], 0.4)
        self.assertEqual(row.features["sa4w_partial"], 0.5)

    def test_missing_snapshot_is_nan_not_error(self):
        row = cc.v5_row(self.row(date(2018, 1, 1)), None)
        self.assertTrue(math.isnan(row.features["sa4w_last"]))

    def test_original_row_not_mutated(self):
        base = self.row(date(2018, 1, 1))
        cc.v5_row(base, {"sa4w_last": 0.4, "sa4w_partial": 0.5})
        self.assertNotIn("sa4w_last", base.features)


def make_world(n=250):
    months = months_range(date(2002, 5, 1), n)
    rows = {m: cm.Row(month=m, y={}, features={"month": m.month, "y_lag1": 0.1}) for m in months}
    final = {shift(months[0], -3 + i): 0.3 + 0.01 * (i % 7) for i in range(n + 6)}
    vint = {}
    for o in months + [shift(months[-1], k) for k in range(1, 4)]:
        vint[o] = {m: v + 0.001 * (o.year % 5) for m, v in final.items() if m < o}
    data = tf.TargetData(nsa={}, core={}, sax_final=final, sax_vintages=vint)
    return months, rows, data


class WalkForwardPredictTest(unittest.TestCase):
    def recorder(self):
        calls = []

        def fit(train, rows, target, names):
            calls.append({"train": [r.month for r in train], "y": [r.y[target] for r in train], "rows": [r.month for r in rows], "names": names})
            return [sum(r.y[target] for r in train) / len(train)] * len(rows)

        return fit, calls

    def test_one_fit_per_test_month_with_purge_and_vintage_targets(self):
        months, rows, data = make_world()
        fit, calls = self.recorder()
        test_months = [date(2019, 1, 1), date(2019, 2, 1)]
        out = cc.wf_predict(rows, data, 3, test_months, fit, ["month"], None)
        self.assertEqual(sorted(out), test_months)
        self.assertEqual(len(calls), 2)
        first = calls[0]
        origin = cc.origin_for(test_months[0], 3)
        self.assertEqual(first["rows"], [origin])
        self.assertTrue(all(o <= shift(origin, -3) for o in first["train"]))  # purge: окно закончено к cutoff
        expected = [data.train_value("T_SAX", origin, o, 3) for o in first["train"]]
        for a, b in zip(first["y"], expected):
            self.assertAlmostEqual(a, b)

    def test_min_train_skips_test_month(self):
        months, rows, data = make_world(n=40)
        fit, calls = self.recorder()
        out = cc.wf_predict(rows, data, 1, [shift(months[0], 30)], fit, ["month"], None, min_train=60)
        self.assertEqual(out, {})
        self.assertEqual(calls, [])

    def test_missing_test_row_or_vintage_skips(self):
        months, rows, data = make_world()
        fit, calls = self.recorder()
        origin = cc.origin_for(date(2019, 1, 1), 1)
        del rows[origin]
        self.assertEqual(cc.wf_predict(rows, data, 1, [date(2019, 1, 1)], fit, ["month"], None), {})


class FrozenPredictTest(unittest.TestCase):
    def test_single_fit_with_vintage_2019_01_targets(self):
        months, rows, data = make_world()
        calls = []

        def fit(train, test_rows, target, names):
            calls.append((len(train), [r.month for r in test_rows], [r.y[target] for r in train]))
            return [0.5] * len(test_rows)

        test_months = cc.TEST_MONTHS[:5]
        out = cc.frozen_predict(rows, data, 1, test_months, fit, ["month"])
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0][0], 102)
        self.assertEqual(calls[0][1], [cc.origin_for(m, 1) for m in test_months])
        self.assertEqual(sorted(out), test_months)
        vint = data.sax_vintages[date(2019, 1, 1)]
        train = cc.frozen_train_origins(1, sorted(rows))
        for y, o in zip(calls[0][2], train):
            self.assertAlmostEqual(y, tf.cumulative(vint, o, 1))

    def test_missing_vintage_is_error(self):
        months, rows, data = make_world()
        del data.sax_vintages[date(2019, 1, 1)]
        with self.assertRaises(ValueError):
            cc.frozen_predict(rows, data, 1, cc.TEST_MONTHS[:2], lambda *a: [0.0, 0.0], ["month"])

    def test_months_without_test_row_are_skipped(self):
        months, rows, data = make_world()
        del rows[cc.origin_for(cc.TEST_MONTHS[1], 1)]
        out = cc.frozen_predict(rows, data, 1, cc.TEST_MONTHS[:3], lambda tr, te, t, n: [0.1] * len(te), ["month"])
        self.assertNotIn(cc.TEST_MONTHS[1], out)
        self.assertEqual(len(out), 2)


class WeeklyNaiveTest(unittest.TestCase):
    def test_partial_then_last(self):
        snaps = {date(2019, 1, 1): {"sa4w_last": 0.4, "sa4w_partial": 0.5}, date(2019, 2, 1): {"sa4w_last": 0.3, "sa4w_partial": float("nan")}}
        out = cc.weekly_naive(snaps, [date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1)])
        self.assertEqual(out, {date(2019, 1, 1): 0.5, date(2019, 2, 1): 0.3})


class ErrorsTest(unittest.TestCase):
    def test_errors_on_common_months_in_order(self):
        preds = {date(2019, 1, 1): 1.0, date(2019, 2, 1): 2.0, date(2019, 3, 1): 3.0}
        truth = {date(2019, 2, 1): 1.5, date(2019, 3, 1): 3.5, date(2019, 4, 1): 9.0}
        months, errs = cc.aligned_errors(preds, truth, [date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1), date(2019, 4, 1)])
        self.assertEqual(months, [date(2019, 2, 1), date(2019, 3, 1)])
        self.assertEqual(errs, [0.5, -0.5])

    def test_cb_errors_pair_with_ours_by_month(self):
        cb = {date(2019, 1, 1): 0.9, date(2019, 2, 1): 2.1}
        truth = {date(2019, 1, 1): 1.0, date(2019, 2, 1): 2.0}
        ours = {date(2019, 2, 1): 2.2}
        months, e_ours, e_cb = cc.paired_errors(ours, cb, truth, [date(2019, 1, 1), date(2019, 2, 1)])
        self.assertEqual(months, [date(2019, 2, 1)])
        self.assertAlmostEqual(e_ours[0], 0.2)
        self.assertAlmostEqual(e_cb[0], 0.1)


class PairedMissingCbTest(unittest.TestCase):
    def test_month_missing_in_cb_is_dropped_for_both(self):
        ours = {date(2019, 1, 1): 1.0, date(2019, 2, 1): 2.0}
        cb = {date(2019, 2, 1): 2.1}
        truth = {date(2019, 1, 1): 1.0, date(2019, 2, 1): 2.0}
        months, e_ours, e_cb = cc.paired_errors(ours, cb, truth, [date(2019, 1, 1), date(2019, 2, 1)])
        self.assertEqual(months, [date(2019, 2, 1)])
        self.assertEqual(len(e_ours), len(e_cb))


class ClassifyTest(unittest.TestCase):
    ALPHA = 0.05 / 3

    def test_better(self):
        out = cc.classify(rmse_ours=0.8, rmse_cb=1.0, dm_p=0.001, delta_ci95=(-0.3, -0.1), ratio_ci90=(0.7, 0.9))
        self.assertEqual(out, "better")

    def test_worse(self):
        out = cc.classify(rmse_ours=1.3, rmse_cb=1.0, dm_p=0.001, delta_ci95=(0.1, 0.5), ratio_ci90=(1.1, 1.5))
        self.assertEqual(out, "worse")

    def test_parity_needs_ci_inside_and_p_not_significant(self):
        self.assertEqual(cc.classify(1.02, 1.0, 0.4, (-0.1, 0.15), (0.95, 1.08)), "parity")
        self.assertEqual(cc.classify(1.02, 1.0, 0.04, (-0.1, 0.15), (0.95, 1.08)), "undetermined")  # p < 0,05
        self.assertEqual(cc.classify(1.02, 1.0, 0.4, (-0.1, 0.15), (0.85, 1.08)), "undetermined")  # CI выходит

    def test_better_requires_bonferroni_alpha_and_ci_without_zero(self):
        self.assertEqual(cc.classify(0.8, 1.0, 0.03, (-0.3, -0.1), (0.7, 0.9)), "undetermined")  # p между 0,0167 и 0,05
        self.assertEqual(cc.classify(0.8, 1.0, 0.001, (-0.3, 0.05), (0.7, 0.9)), "undetermined")  # CI содержит 0

    def test_worse_requires_ci_above_zero(self):
        self.assertEqual(cc.classify(1.3, 1.0, 0.001, (-0.05, 0.5), (1.1, 1.5)), "undetermined")

    def test_nan_p_is_undetermined(self):
        self.assertEqual(cc.classify(0.8, 1.0, float("nan"), (-0.3, -0.1), (0.7, 0.9)), "undetermined")


class CompareToCbTest(unittest.TestCase):
    def test_statistics_and_class_on_synthetic_errors(self):
        n = 54
        cb = [(0.4 if i % 2 == 0 else -0.4) * (1 + 0.1 * (i % 5)) for i in range(n)]
        ours = [(0.2 if i % 2 == 0 else -0.2) * (1 + 0.1 * (i % 5)) for i in range(n)]
        out = cc.compare_to_cb(ours, cb, 1)
        self.assertEqual(out["n"], n)
        self.assertLess(out["ratio"], 1.0)
        self.assertEqual(out["class"], "better")
        for key in ("rmse_ours", "rmse_cb", "mae_ours", "mae_cb", "dm_p", "delta_ci95", "ratio_ci90"):
            self.assertIn(key, out)

    def test_noisy_ratio_near_one_is_not_significant(self):
        n = 56
        e = [(0.3 if i % 2 == 0 else -0.3) * (1 + 0.1 * (i % 5)) for i in range(n)]
        scale = [1.12 if i % 7 < 3 else 0.92 for i in range(n)]
        out = cc.compare_to_cb([x * s for x, s in zip(e, scale)], e, 3)
        self.assertIn(out["class"], ("parity", "undetermined"))
        self.assertGreater(out["ratio"], 0.90)
        self.assertLess(out["ratio"], 1.10)
        self.assertGreater(out["dm_p"], 0.05)

    def test_consistently_two_percent_worse_is_worse(self):
        n = 54
        e = [(0.3 if i % 2 == 0 else -0.3) * (1 + 0.1 * (i % 5)) for i in range(n)]
        out = cc.compare_to_cb([x * 1.02 for x in e], e, 3)
        self.assertEqual(out["class"], "worse")  # устойчивая разность значима — паритет не фиксируется



class HypothesisW5bTest(unittest.TestCase):
    """#425: окно шоков и правило H-W5b (до кода и прогона, спека §17.5)."""

    def test_window_touches_h1(self):
        self.assertTrue(cc.window_touches(date(2022, 3, 1), 1, cc.SHOCK_MONTHS))
        self.assertFalse(cc.window_touches(date(2023, 3, 1), 1, cc.SHOCK_MONTHS))

    def test_window_touches_h6(self):
        self.assertTrue(cc.window_touches(date(2023, 5, 1), 6, cc.SHOCK_MONTHS))
        self.assertFalse(cc.window_touches(date(2023, 6, 1), 6, cc.SHOCK_MONTHS))

    def test_window_touches_default_empty(self):
        self.assertFalse(cc.window_touches(date(2022, 3, 1), 1, frozenset()))

    def test_verdict_support(self):
        out = cc.hypothesis_verdict(2.5, 1.2, 1.2)
        self.assertTrue(out["reproduce"])
        self.assertTrue(out["mechanism"])
        self.assertEqual(out["verdict"], "support")

    def test_verdict_partial(self):
        out = cc.hypothesis_verdict(2.5, 2.4, 1.2)
        self.assertTrue(out["reproduce"])
        self.assertFalse(out["mechanism"])
        self.assertEqual(out["verdict"], "partial")

    def test_verdict_reject(self):
        out = cc.hypothesis_verdict(1.1, 1.0, 1.2)
        self.assertFalse(out["reproduce"])
        self.assertIsNone(out["mechanism"])
        self.assertEqual(out["verdict"], "reject")


if __name__ == "__main__":
    unittest.main()
