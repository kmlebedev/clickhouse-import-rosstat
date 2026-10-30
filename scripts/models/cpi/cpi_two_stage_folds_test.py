#!/usr/bin/env python3
"""Офлайн-тесты таргетов, доступности и фолдов two-stage CPI (#401 W3).

Доступность (Росстат — день релиза или 9-е `M+1`; ЦБ SA — 18-е `M+1`), purge по спеке §3
(`M'+h−1 ≤ M−1` для `T_SAX`/`T_NSA`, `≤ M−2` для `T_CORE`), фолды, `common_folds`, цели
обучения и истина (`T_SAX` — винтаж тестового origin'а и окончательный ряд), отсутствие утечки,
отпечатки. R, ClickHouse и сеть не используются. Запуск — `make cpi-two-stage-folds-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_folds as tf


def months_range(first: date, n: int) -> list:
    out, year, month = [], first.year, first.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def shift(m: date, k: int) -> date:
    i = m.year * 12 + (m.month - 1) + k
    return date(i // 12, i % 12 + 1, 1)


class AvailabilityTest(unittest.TestCase):
    def test_rosstat_rule_is_ninth_of_next_month(self):
        self.assertEqual(tf.rosstat_available(date(2014, 3, 1), None), date(2014, 4, 9))
        self.assertEqual(tf.rosstat_available(date(2014, 12, 31), None), date(2015, 1, 9))

    def test_rosstat_exact_date_overrides_rule(self):
        table = {date(2020, 5, 1): date(2020, 6, 5)}
        self.assertEqual(tf.rosstat_available(date(2020, 5, 1), table), date(2020, 6, 5))
        self.assertEqual(tf.rosstat_available(date(2020, 6, 1), table), date(2020, 7, 9))

    def test_cbr_sa_is_eighteenth_of_next_month(self):
        self.assertEqual(tf.cbr_available(date(2014, 3, 1)), date(2014, 4, 18))
        self.assertEqual(tf.cbr_available(date(2014, 12, 1)), date(2015, 1, 18))

    def test_target_availability_dispatch(self):
        table = {}
        m = date(2020, 5, 1)
        self.assertEqual(tf.target_available("T_SAX", m, table), tf.rosstat_available(m, table))
        self.assertEqual(tf.target_available("T_NSA", m, table), tf.rosstat_available(m, table))
        self.assertEqual(tf.target_available("T_CORE", m, table), tf.cbr_available(m))
        with self.assertRaises(ValueError):
            tf.target_available("T_X", m, table)

    def test_cbr_month_is_always_later_than_rosstat_cutoff_of_same_period(self):
        # SA ЦБ за `M−1` (18-е `M`) позже любого cutoff месяца `M` (≤ 17-го): поэтому core отстаёт на месяц.
        table = {date(2024, 4, 1): date(2024, 5, 17)}
        for month in months_range(date(2014, 1, 1), 150):
            cutoff = tf.cutoff(month, table)
            self.assertGreater(tf.cbr_available(shift(month, -1)), cutoff)
            self.assertLessEqual(tf.cbr_available(shift(month, -2)), cutoff)


class WindowAndPurgeTest(unittest.TestCase):
    def test_window_months(self):
        self.assertEqual(tf.window(date(2020, 11, 1), 3), [date(2020, 11, 1), date(2020, 12, 1), date(2021, 1, 1)])
        with self.assertRaises(ValueError):
            tf.window(date(2020, 1, 1), 0)

    def test_purge_matches_spec_formula_under_rule_dates(self):
        # Спека §3: `M'+h−1 ≤ M−1` для T_SAX/T_NSA, `≤ M−2` для T_CORE.
        candidates = months_range(date(2010, 1, 1), 150)
        for test in (date(2018, 1, 1), date(2019, 7, 1), date(2021, 12, 1)):
            for h in (1, 3, 6):
                for target, back in (("T_SAX", 1), ("T_NSA", 1), ("T_CORE", 2)):
                    got = tf.training_origins(target, h, test, candidates, None)
                    want = [m for m in candidates if shift(m, h - 1) <= shift(test, -back)]
                    self.assertEqual(got, want, (target, h, test))

    def test_test_origin_and_future_never_in_training(self):
        candidates = months_range(date(2010, 1, 1), 150)
        test = date(2019, 7, 1)
        for target in tf.TARGETS:
            for h in tf.HORIZONS:
                train = tf.training_origins(target, h, test, candidates, None)
                self.assertTrue(all(m < test for m in train))
                self.assertEqual(train, sorted(train))

    def test_exact_release_date_can_shift_the_boundary(self):
        candidates = months_range(date(2019, 1, 1), 24)
        test = date(2020, 6, 1)
        # Месяц `M−1` = 2020-05 опубликован в cutoff (включительно): строка M' = M−1 (h=1) допустима.
        table = {date(2020, 5, 1): date(2020, 6, 5)}
        train = tf.training_origins("T_SAX", 1, test, candidates, table)
        self.assertIn(date(2020, 5, 1), train)

    def test_core_excludes_previous_month_row_for_h1(self):
        candidates = months_range(date(2019, 1, 1), 24)
        train = tf.training_origins("T_CORE", 1, date(2020, 6, 1), candidates, None)
        self.assertNotIn(date(2020, 5, 1), train)
        self.assertIn(date(2020, 4, 1), train)

    def test_assert_no_lookahead_catches_bad_train(self):
        fold = tf.Fold(origin=date(2020, 6, 1), cutoff=tf.cutoff(date(2020, 6, 1), None), train=[date(2020, 5, 1)])
        tf.assert_no_lookahead("T_SAX", 1, fold, None)
        with self.assertRaises(AssertionError):
            tf.assert_no_lookahead("T_CORE", 1, fold, None)
        bad = tf.Fold(origin=date(2020, 6, 1), cutoff=fold.cutoff, train=[date(2020, 6, 1)])
        with self.assertRaises(AssertionError):
            tf.assert_no_lookahead("T_SAX", 1, bad, None)


class FoldsTest(unittest.TestCase):
    def test_min_train_skips_early_origins(self):
        candidates = months_range(date(2016, 1, 1), 60)
        folds = tf.make_folds("T_SAX", 1, candidates, candidates, first_train=date(2016, 1, 1), min_train=24, release_dates=None)
        self.assertEqual(folds[0].origin, date(2018, 1, 1))  # 2016-01 … 2017-12 = 24 строки
        self.assertEqual(len(folds[0].train), 24)
        self.assertTrue(all(len(f.train) >= 24 for f in folds))

    def test_first_train_respected(self):
        candidates = months_range(date(2010, 1, 1), 100)
        folds = tf.make_folds("T_SAX", 1, candidates, candidates, first_train=date(2012, 6, 1), min_train=12, release_dates=None)
        self.assertTrue(all(min(f.train) >= date(2012, 6, 1) for f in folds))

    def test_core_has_fewer_train_rows_than_sax_by_one_month(self):
        candidates = months_range(date(2016, 1, 1), 60)
        test = [date(2019, 1, 1)]
        sax = tf.make_folds("T_SAX", 3, test, candidates, date(2016, 1, 1), 24, None)[0]
        core = tf.make_folds("T_CORE", 3, test, candidates, date(2016, 1, 1), 24, None)[0]
        self.assertEqual(len(sax.train) - len(core.train), 1)

    def test_longer_horizon_shrinks_train(self):
        candidates = months_range(date(2016, 1, 1), 60)
        test = [date(2019, 1, 1)]
        sizes = [len(tf.make_folds("T_SAX", h, test, candidates, date(2016, 1, 1), 12, None)[0].train) for h in (1, 3, 6)]
        self.assertEqual(sizes, sorted(sizes, reverse=True))
        self.assertEqual(sizes[0] - sizes[2], 5)

    def test_folds_pass_lookahead_check(self):
        candidates = months_range(date(2016, 1, 1), 60)
        for target in tf.TARGETS:
            for h in tf.HORIZONS:
                for fold in tf.make_folds(target, h, candidates, candidates, date(2016, 1, 1), 12, None):
                    tf.assert_no_lookahead(target, h, fold, None)

    def test_common_folds_intersection(self):
        out = tf.common_folds({"a": {date(2020, 1, 1): 1, date(2020, 2, 1): 1}, "b": {date(2020, 2, 1): 2, date(2020, 3, 1): 2}})
        self.assertEqual(out, [date(2020, 2, 1)])
        self.assertEqual(tf.common_folds({}), [])
        self.assertEqual(tf.common_folds({"a": {date(2020, 1, 1): 1}, "b": {date(2020, 1, 1): 1}}, models=["a"]), [date(2020, 1, 1)])


class CumulativeTest(unittest.TestCase):
    def test_compound_growth_over_window(self):
        series = {date(2020, 1, 1): 1.0, date(2020, 2, 1): 2.0, date(2020, 3, 1): -1.0}
        self.assertAlmostEqual(tf.cumulative(series, date(2020, 1, 1), 1), 1.0)
        self.assertAlmostEqual(tf.cumulative(series, date(2020, 1, 1), 3), (1.01 * 1.02 * 0.99 - 1) * 100)

    def test_hole_gives_none(self):
        self.assertIsNone(tf.cumulative({date(2020, 1, 1): 1.0}, date(2020, 1, 1), 2))


def target_data(n=80):
    months = months_range(date(2012, 1, 1), n)
    nsa = {m: 0.4 + 0.1 * math.sin(i) for i, m in enumerate(months)}
    core = {m: 0.35 + 0.05 * math.cos(i) for i, m in enumerate(months)}
    final = {m: 0.3 + 0.1 * math.sin(i + 1) for i, m in enumerate(months)}
    vint = {}
    for k in range(40, n):
        origin = months[k]
        vint[origin] = {m: final[m] + 0.01 * (k % 3) for m in months[:k]}  # ряд до O−1, свой в каждом origin'е
    return tf.TargetData(nsa=nsa, core=core, sax_final=final, sax_vintages=vint), months


class TargetDataTest(unittest.TestCase):
    def test_sax_training_value_comes_from_test_origin_vintage_not_final(self):
        data, months = target_data()
        test, train = months[60], months[50]
        got = data.train_value("T_SAX", test, train, 1)
        self.assertAlmostEqual(got, data.sax_vintages[test][train], places=9)
        self.assertNotEqual(got, data.sax_final[train])

    def test_sax_truth_is_final(self):
        data, months = target_data()
        self.assertAlmostEqual(data.truth("T_SAX", months[60], 1), data.sax_final[months[60]], places=9)
        self.assertAlmostEqual(data.truth("T_SAX", months[60], 3), tf.cumulative(data.sax_final, months[60], 3))

    def test_core_and_nsa_use_latest_series(self):
        data, months = target_data()
        self.assertAlmostEqual(data.train_value("T_CORE", months[60], months[50], 1), data.core[months[50]], places=9)
        self.assertAlmostEqual(data.truth("T_NSA", months[60], 1), data.nsa[months[60]], places=9)

    def test_missing_vintage_or_hole_gives_none(self):
        data, months = target_data()
        self.assertIsNone(data.train_value("T_SAX", months[10], months[5], 1))  # нет винтажа origin'а
        self.assertIsNone(data.train_value("T_SAX", months[60], months[59], 3))  # окно выходит за винтаж

    def test_no_leak_core_months_not_yet_published_do_not_enter_training(self):
        data, months = target_data()
        test = months[60]
        candidates = months[:70]
        train = tf.training_origins("T_CORE", 3, test, candidates, None)
        clean = [data.train_value("T_CORE", test, o, 3) for o in train]
        poisoned_core = {m: (v * 9 + 4 if m >= shift(test, -1) else v) for m, v in data.core.items()}
        dirty = tf.TargetData(nsa=data.nsa, core=poisoned_core, sax_final=data.sax_final, sax_vintages=data.sax_vintages)
        self.assertEqual(clean, [dirty.train_value("T_CORE", test, o, 3) for o in train])

    def test_no_leak_sax_vintage_of_test_origin_covers_only_published_months(self):
        data, months = target_data()
        test = months[60]
        train = tf.training_origins("T_SAX", 3, test, months[:70], None)
        for origin in train:
            self.assertIsNotNone(data.train_value("T_SAX", test, origin, 3), origin)

    def test_unknown_target(self):
        data, months = target_data()
        with self.assertRaises(ValueError):
            data.truth("T_X", months[60], 1)


class FoldSummaryTest(unittest.TestCase):
    def test_summary_and_digest(self):
        candidates = months_range(date(2016, 1, 1), 60)
        folds = tf.make_folds("T_SAX", 3, candidates, candidates, date(2016, 1, 1), 24, None)
        summary = tf.fold_summary(folds)
        self.assertEqual(summary["n"], len(folds))
        self.assertEqual(summary["first_origin"], folds[0].origin.isoformat())
        self.assertEqual(summary["last_origin"], folds[-1].origin.isoformat())
        self.assertEqual(summary["train_min"], min(len(f.train) for f in folds))
        self.assertEqual(summary["train_max"], max(len(f.train) for f in folds))
        again = tf.make_folds("T_SAX", 3, candidates, candidates, date(2016, 1, 1), 24, None)
        self.assertEqual(summary["digest"], tf.fold_summary(again)["digest"])
        other = tf.make_folds("T_CORE", 3, candidates, candidates, date(2016, 1, 1), 24, None)
        self.assertNotEqual(summary["digest"], tf.fold_summary(other)["digest"])

    def test_empty(self):
        self.assertEqual(tf.fold_summary([])["n"], 0)


class BuildTargetDataTest(unittest.TestCase):
    def test_composes_series_and_vintages_from_inputs(self):
        import cpi_two_stage_targets_test as tt_test

        months, nsa, util = tt_test.world(70)
        core = {m: 0.3 for m in months}
        origins = [months[50], months[55], months[69]]
        data = tf.build_target_data(nsa, util, core, 0.10, origins, tt_test.fake_x13)
        self.assertEqual(sorted(data.sax_vintages), origins)
        self.assertEqual(max(data.sax_final), months[-1])
        self.assertEqual(data.core, core)
        self.assertEqual(data.skipped_vintages, [])

    def test_unavailable_vintage_is_reported_not_filled(self):
        import cpi_two_stage_targets_test as tt_test

        months, nsa, util = tt_test.world(72)
        del util[months[71]]  # КУ последнего месяца ещё нет (допущение A1)
        origin_next = date(2016, 1, 1)  # origin после последнего месяца выборки: нужен u(2015-12)
        data = tf.build_target_data(nsa, util, {}, 0.10, [months[55], origin_next], tt_test.fake_x13)
        self.assertIn(months[55], data.sax_vintages)
        self.assertNotIn(origin_next, data.sax_vintages)
        self.assertEqual([o for o, _ in data.skipped_vintages], [origin_next])
        self.assertEqual(max(data.sax_final), months[70])


class DigestTest(unittest.TestCase):
    def test_predictions_digest_order_independent_and_sensitive(self):
        a = [{"model": "m", "origin": "2020-01-01", "pred": 0.1}, {"model": "m", "origin": "2020-02-01", "pred": 0.2}]
        self.assertEqual(tf.predictions_digest(a), tf.predictions_digest(list(reversed(a))))
        b = [dict(a[0]), {**a[1], "pred": 0.2000001}]
        self.assertNotEqual(tf.predictions_digest(a), tf.predictions_digest(b))

    def test_feature_digests_per_feature(self):
        rows_a = {date(2020, 1, 1): {"x": 1.0, "y": 2.0}, date(2020, 2, 1): {"x": 1.5, "y": float("nan")}}
        rows_b = {date(2020, 1, 1): {"x": 1.0, "y": 2.0}, date(2020, 2, 1): {"x": 1.6, "y": float("nan")}}
        da, db = tf.feature_digests(rows_a), tf.feature_digests(rows_b)
        self.assertEqual(da["y"], db["y"])
        self.assertNotEqual(da["x"], db["x"])
        self.assertEqual(da, tf.feature_digests(dict(reversed(list(rows_a.items())))))


if __name__ == "__main__":
    unittest.main()
