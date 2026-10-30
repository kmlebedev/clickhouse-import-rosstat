#!/usr/bin/env python3
"""Офлайн-тесты Stage 1 (#401 W4): признаки as-of, строки фолда, baselines, walk-forward, gate S1.

Признаки соответствуют протоколу W4 (спека §4 W4): набор имён зафиксирован; лаги `T_SAX` — из винтажа
тестового origin'а, лаги `T_CORE` — только опубликованные ЦБ к cutoff; отсутствие утечки; baselines
(`B1`, `B2`, `B3`, `weekly_naive`); выбор кандидата на dev; Holm по двум целям; условия 1–4 gate.
R, ClickHouse и сеть не используются. Запуск — `make cpi-two-stage-s1-test`.
"""

import math
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_two_stage_folds as tf
import cpi_two_stage_s1 as s1
import weekly_sa_asof as wa
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


class FeatureNamesTest(unittest.TestCase):
    def test_protocol_names_fixed(self):
        self.assertEqual(
            s1.FEATURES,
            (
                "sa4w_last", "saar_last", "sa4w_partial", "saar_partial",
                "wkagg_mtd", "wkagg_cov", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w",
                "tariff_expected",
                "sax_l1", "sax_l2", "core_l2", "core_l3", "nsa_l1", "nsa_l2",
                "wk_share_rising", "wk_median", "wk_median4", "wk_gasoline4",
                "month",
            ),
        )

    def test_forbidden_inputs_absent(self):
        # Запрещены МП (все ряды `cm.MP_SERIES`), ожидания/SA ЦБ всего ИПЦ и `tariff_week` (календарь расходится с данными).
        for name in cm.MP_SERIES:
            self.assertNotIn(name, s1.FEATURES)
        for name in s1.FEATURES:
            self.assertFalse(name.startswith(("infl_exp", "cpi_sa", "cbr_", "survey")), name)
        self.assertNotIn("tariff_week", s1.FEATURES)

    def test_enet_excludes_month_and_b3_uses_only_monthly_lags(self):
        self.assertNotIn("month", s1.ENET_FEATURES)
        self.assertEqual(set(s1.B3_FEATURES), {"sax_l1", "sax_l2", "core_l2", "core_l3", "nsa_l1", "nsa_l2"})
        self.assertEqual(s1.ENET_FEATURES, tuple(n for n in s1.FEATURES if n != "month"))


class SnapshotFeaturesTest(unittest.TestCase):
    def snap(self, partial=True):
        months = [wa.MonthPoint(date(2020, 1, 31), 100.5, date(2020, 1, 27), False), wa.MonthPoint(date(2020, 2, 29), 100.6, date(2020, 2, 24), False)]
        part = wa.MonthPoint(date(2020, 3, 31), 100.7, date(2020, 3, 9), True) if partial else None
        return wa.Snapshot(date(2020, 3, 9), date(2020, 3, 9), months, [100.2, 100.4], part, 100.9 if partial else None)

    def test_saar_transform(self):
        self.assertAlmostEqual(s1.saar(1.0), (1.01**13 - 1) * 100)
        self.assertTrue(math.isnan(s1.saar(float("nan"))))

    def test_last_complete_and_partial(self):
        out = s1.snapshot_features(self.snap())
        self.assertAlmostEqual(out["sa4w_last"], 0.4)
        self.assertAlmostEqual(out["sa4w_partial"], 0.9)
        self.assertAlmostEqual(out["saar_last"], s1.saar(0.4))
        self.assertAlmostEqual(out["saar_partial"], s1.saar(0.9))

    def test_no_partial_gives_nan_not_zero(self):
        out = s1.snapshot_features(self.snap(partial=False))
        self.assertTrue(math.isnan(out["sa4w_partial"]))
        self.assertTrue(math.isnan(out["saar_partial"]))


class LagFeaturesTest(unittest.TestCase):
    def data(self):
        months = months_range(date(2015, 1, 1), 80)
        nsa = {m: 0.4 + 0.01 * i for i, m in enumerate(months)}
        core = {m: 0.3 + 0.02 * i for i, m in enumerate(months)}
        vintage = {m: 0.2 + 0.03 * i for i, m in enumerate(months)}
        return months, nsa, core, vintage

    def test_lags_use_expected_months(self):
        months, nsa, core, vintage = self.data()
        origin = months[40]
        out = s1.lag_features(origin, nsa, core, vintage, None)
        self.assertEqual(out["nsa_l1"], nsa[months[39]])
        self.assertEqual(out["nsa_l2"], nsa[months[38]])
        self.assertEqual(out["core_l2"], core[months[38]])
        self.assertEqual(out["core_l3"], core[months[37]])
        self.assertEqual(out["sax_l1"], vintage[months[39]])
        self.assertEqual(out["sax_l2"], vintage[months[38]])

    def test_unpublished_months_are_not_used(self):
        months, nsa, core, vintage = self.data()
        origin = months[40]
        clean = s1.lag_features(origin, nsa, core, vintage, None)
        dirty_core = {m: (v * 9 + 5 if m >= months[39] else v) for m, v in core.items()}  # `O−1` у ЦБ ещё не вышел
        dirty_nsa = {m: (v * 9 + 5 if m >= months[40] else v) for m, v in nsa.items()}
        dirty_vint = {m: (v * 9 + 5 if m >= months[40] else v) for m, v in vintage.items()}
        self.assertEqual(clean, s1.lag_features(origin, dirty_nsa, dirty_core, dirty_vint, None))

    def test_missing_month_is_nan(self):
        months, nsa, core, vintage = self.data()
        del core[months[38]]
        out = s1.lag_features(months[40], nsa, core, vintage, None)
        self.assertTrue(math.isnan(out["core_l2"]))

    def test_core_lag_guard_rejects_unavailable_month(self):
        months, nsa, core, vintage = self.data()
        # Если бы окно сдвинули на `O−1`, оно было бы недоступно к cutoff: проверка доступности срабатывает.
        with self.assertRaises(AssertionError):
            s1.assert_core_available(months[39], months[40], None)
        s1.assert_core_available(months[38], months[40], None)


class StaticFeaturesTest(unittest.TestCase):
    def test_tariff_expected_months(self):
        self.assertEqual(s1.tariff_expected(date(2020, 1, 1)), 1)
        self.assertEqual(s1.tariff_expected(date(2020, 7, 1)), 1)
        self.assertEqual(s1.tariff_expected(date(2020, 3, 1)), 0)

    def test_wkagg_part_excludes_tariff_week(self):
        points = [(date(2020, 3, 2) + timedelta(days=7 * i), 100.1) for i in range(30)]
        out = s1.wkagg_part(points, date(2020, 6, 1), date(2020, 6, 9))
        self.assertEqual(set(out), {"wkagg_mtd", "wkagg_cov", "wkagg_mtd_scaled", "wkagg_last", "wkagg_4w"})

    def test_wkagg_part_no_leak(self):
        points = [(date(2020, 3, 2) + timedelta(days=7 * i), 100.1) for i in range(30)]
        cutoff = date(2020, 6, 9)
        clean = s1.wkagg_part(points, date(2020, 6, 1), cutoff)
        dirty = [(d, v * 5 if d + timedelta(days=2) > cutoff else v) for d, v in points]
        out = s1.wkagg_part(dirty, date(2020, 6, 1), cutoff)
        for name in clean:
            self.assertTrue(clean[name] == out[name] or (math.isnan(clean[name]) and math.isnan(out[name])), name)


def synthetic_world(n=80):
    months = months_range(date(2015, 1, 1), n)
    static = {}
    for i, m in enumerate(months):
        static[m] = {name: float(math.sin(i + k)) for k, name in enumerate(s1.FEATURES) if name not in ("sax_l1", "sax_l2", "month")}
        static[m]["month"] = float(m.month)
    final = {m: 0.3 + 0.1 * math.sin(i) for i, m in enumerate(months)}
    vint = {m: {k: final[k] + 0.01 for k in months if k < m} for m in months}
    nsa = {m: 0.4 for m in months}
    core = {m: 0.35 for m in months}
    data = tf.TargetData(nsa=nsa, core=core, sax_final=final, sax_vintages=vint)
    return months, static, data


class FoldRowsTest(unittest.TestCase):
    def test_rows_carry_vintage_lags_and_targets(self):
        months, static, data = synthetic_world()
        test = months[60]
        folds = tf.make_folds("T_SAX", 1, [test], months, months[12], 24, None)
        train, row, truth = s1.fold_rows(static, data, "T_SAX", folds[0])
        self.assertEqual(len(train), len(folds[0].train))
        first = train[0]
        j = folds[0].train[0]
        self.assertAlmostEqual(first.y["T_SAX"], data.train_value("T_SAX", test, j, 1))
        self.assertEqual(first.features["sax_l1"], data.sax_vintages[test][shift(j, -1)])
        self.assertEqual(row.features["sax_l1"], data.sax_vintages[test][shift(test, -1)])
        self.assertAlmostEqual(truth, data.truth("T_SAX", test, 1))
        for r in train:
            self.assertEqual(set(r.features), set(s1.FEATURES))

    def test_train_origins_all_before_test(self):
        months, static, data = synthetic_world()
        test = months[60]
        folds = tf.make_folds("T_CORE", 1, [test], months, months[12], 24, None)
        train, row, truth = s1.fold_rows(static, data, "T_CORE", folds[0])
        self.assertTrue(all(r.month < test for r in train))
        self.assertEqual(row.month, test)

    def test_no_leak_poisoned_future_static_rows_do_not_change_train_rows(self):
        months, static, data = synthetic_world()
        test = months[60]
        folds = tf.make_folds("T_SAX", 1, [test], months, months[12], 24, None)
        clean_train, clean_row, _ = s1.fold_rows(static, data, "T_SAX", folds[0])
        dirty = {m: ({k: v * 9 + 3 for k, v in f.items()} if m > test else f) for m, f in static.items()}
        d_train, d_row, _ = s1.fold_rows(dirty, data, "T_SAX", folds[0])
        self.assertEqual([r.features for r in clean_train], [r.features for r in d_train])
        self.assertEqual(clean_row.features, d_row.features)

    def test_missing_static_row_skips_training_origin(self):
        months, static, data = synthetic_world()
        test = months[60]
        folds = tf.make_folds("T_SAX", 1, [test], months, months[12], 24, None)
        del static[folds[0].train[3]]
        train, _, _ = s1.fold_rows(static, data, "T_SAX", folds[0])
        self.assertEqual(len(train), len(folds[0].train) - 1)

    def test_missing_test_row_returns_none(self):
        months, static, data = synthetic_world()
        test = months[60]
        folds = tf.make_folds("T_SAX", 1, [test], months, months[12], 24, None)
        del static[test]
        self.assertIsNone(s1.fold_rows(static, data, "T_SAX", folds[0]))


def row(month, lags, y=0.0, **extra):
    features = {name: float("nan") for name in s1.FEATURES}
    features.update(lags)
    features.update(extra)
    features["month"] = float(month.month)
    return cm.Row(month=month, y={"T_SAX": y, "T_CORE": y}, features=features)


class BaselinesTest(unittest.TestCase):
    def test_b1_last_known_value_per_target(self):
        r = row(date(2020, 6, 1), {"sax_l1": 0.31, "core_l2": 0.27})
        self.assertEqual(s1.predict_b1([], r, "T_SAX"), 0.31)
        self.assertEqual(s1.predict_b1([], r, "T_CORE"), 0.27)

    def test_weekly_naive_uses_partial_then_last(self):
        r = row(date(2020, 6, 1), {}, sa4w_partial=0.5, sa4w_last=0.4)
        self.assertEqual(s1.predict_weekly_naive([], r, "T_SAX"), 0.5)
        r = row(date(2020, 6, 1), {}, sa4w_last=0.4)
        self.assertEqual(s1.predict_weekly_naive([], r, "T_CORE"), 0.4)

    def test_b2_recovers_ar1_with_month_dummies(self):
        months = months_range(date(2015, 1, 1), 60)
        train = []
        for i, m in enumerate(months[:-1]):
            last = 0.3 + 0.2 * math.sin(i)
            y = 0.1 + 0.5 * last + (0.2 if m.month == 7 else 0.0)
            train.append(row(m, {"sax_l1": last}, y=y))
        m = date(2020, 7, 1)
        test = row(m, {"sax_l1": 0.4})
        self.assertAlmostEqual(s1.predict_b2(train, test, "T_SAX"), 0.1 + 0.5 * 0.4 + 0.2, places=6)

    def test_b2_needs_enough_rows(self):
        months = months_range(date(2015, 1, 1), 10)
        train = [row(m, {"sax_l1": 0.3}, y=0.3) for m in months]
        with self.assertRaises(s1.DegenerateWindow):
            s1.predict_b2(train, row(date(2016, 1, 1), {"sax_l1": 0.3}), "T_SAX")

    def test_enet_learns_linear_signal(self):
        months = months_range(date(2015, 1, 1), 60)
        train = []
        for i, m in enumerate(months):
            lag = math.sin(i / 3.0)
            train.append(row(m, {"sax_l1": lag, "sax_l2": 0.0, "core_l2": 0.0, "core_l3": 0.0, "nsa_l1": 0.0, "nsa_l2": 0.0}, y=0.5 * lag))
        test = row(date(2020, 1, 1), {"sax_l1": 0.8, "sax_l2": 0.0, "core_l2": 0.0, "core_l3": 0.0, "nsa_l1": 0.0, "nsa_l2": 0.0})
        self.assertAlmostEqual(s1.predict_b3(train, test, "T_SAX"), 0.4, delta=0.1)

    def test_enet_imputes_nan_by_train_median(self):
        months = months_range(date(2015, 1, 1), 40)
        train = [row(m, {"sax_l1": 0.1 * i}, y=0.1 * i) for i, m in enumerate(months)]
        for r in train:
            r.features["sa4w_partial"] = float("nan")
        out = s1.predict_enet(train, row(date(2019, 6, 1), {"sax_l1": 1.0}), "T_SAX")
        self.assertTrue(math.isfinite(out))

    def test_models_registry(self):
        self.assertEqual(set(s1.BASELINES), {"B1", "B2", "B3", "weekly_naive"})
        self.assertEqual(set(s1.STAGE1_MODELS), {"s1_catboost", "s1_enet"})


class WalkForwardTest(unittest.TestCase):
    def test_predictions_per_fold_and_model(self):
        months, static, data = synthetic_world()
        tests = months[36:40]
        folds = tf.make_folds("T_SAX", 1, tests, months, months[0], 24, None)
        models = {"a": lambda train, r, t: 0.3, "b": lambda train, r, t: 0.4}
        out = s1.walk_forward("T_SAX", static, data, folds, models)
        self.assertEqual(len(out), len(folds) * 2)
        for p in out:
            self.assertEqual(set(p), {"model", "origin", "pred", "truth"})
            self.assertTrue(math.isfinite(p["truth"]))

    def test_degenerate_model_is_skipped_for_that_fold(self):
        months, static, data = synthetic_world()
        folds = tf.make_folds("T_SAX", 1, months[36:39], months, months[0], 24, None)

        def flaky(train, r, t):
            if r.month == months[37]:
                raise s1.DegenerateWindow("мало строк")
            return 0.3

        out = s1.walk_forward("T_SAX", static, data, folds, {"a": flaky})
        self.assertEqual(sorted(p["origin"] for p in out), [months[36], months[38]])

    def test_non_finite_prediction_is_error(self):
        months, static, data = synthetic_world()
        folds = tf.make_folds("T_SAX", 1, months[36:37], months, months[0], 24, None)
        with self.assertRaises(ValueError):
            s1.walk_forward("T_SAX", static, data, folds, {"a": lambda train, r, t: float("nan")})

    def test_fold_without_truth_is_dropped(self):
        months, static, data = synthetic_world()
        data.sax_final.pop(months[37])
        folds = tf.make_folds("T_SAX", 1, months[36:39], months, months[0], 24, None)
        out = s1.walk_forward("T_SAX", static, data, folds, {"a": lambda train, r, t: 0.3})
        self.assertNotIn(months[37], [p["origin"] for p in out])


def preds(model_errors, origins):
    """`{model: [{'origin', 'pred', 'truth'}]}` из ошибок `pred − truth` при truth = 0."""
    return {name: [{"model": name, "origin": o, "pred": e[i], "truth": 0.0} for i, o in enumerate(origins)] for name, e in model_errors.items()}


class SelectionAndHolmTest(unittest.TestCase):
    def test_candidate_by_dev_rmse(self):
        origins = months_range(date(2018, 1, 1), 48)
        pr = preds({"s1_catboost": [0.3] * 48, "s1_enet": [0.2] * 48}, origins)
        self.assertEqual(s1.select_candidate(pr, date(2018, 1, 1), date(2021, 12, 1)), "s1_enet")

    def test_candidate_uses_common_dev_folds_only(self):
        origins = months_range(date(2018, 1, 1), 48)
        pr = preds({"s1_catboost": [0.3] * 48, "s1_enet": [0.2] * 48}, origins)
        pr["s1_enet"] = pr["s1_enet"][:40]  # enet не считался на последних 8 фолдах
        pr["s1_catboost"][3]["pred"] = 5.0  # выброс catboost на общем фолде
        self.assertEqual(s1.select_candidate(pr, date(2018, 1, 1), date(2021, 12, 1)), "s1_enet")

    def test_candidate_selection_ignores_origins_outside_dev(self):
        dev = months_range(date(2018, 1, 1), 48)
        later = months_range(date(2022, 1, 1), 24)
        pr = {
            "s1_catboost": [{"model": "s1_catboost", "origin": o, "pred": 0.3, "truth": 0.0} for o in dev]
            + [{"model": "s1_catboost", "origin": o, "pred": 0.0, "truth": 0.0} for o in later],
            "s1_enet": [{"model": "s1_enet", "origin": o, "pred": 0.2, "truth": 0.0} for o in dev]
            + [{"model": "s1_enet", "origin": o, "pred": 1.0, "truth": 0.0} for o in later],
        }
        self.assertEqual(s1.select_candidate(pr, date(2018, 1, 1), date(2021, 12, 1)), "s1_enet")

    def test_holm_thresholds(self):
        # Меньшее p сравнивается с 0,025, большее — с 0,05 (только если меньшее отвергнуто).
        self.assertEqual(s1.holm({"T_SAX": 0.02, "T_CORE": 0.04}), {"T_SAX": True, "T_CORE": True})
        self.assertEqual(s1.holm({"T_SAX": 0.03, "T_CORE": 0.04}), {"T_SAX": False, "T_CORE": False})
        self.assertEqual(s1.holm({"T_SAX": 0.01, "T_CORE": 0.06}), {"T_SAX": True, "T_CORE": False})
        self.assertEqual(s1.holm({"T_SAX": float("nan"), "T_CORE": 0.001}), {"T_SAX": False, "T_CORE": True})


def target_predictions(cand_err, base_errs, dev_n=48, hold_n=57):
    """Предсказания кандидата и пяти baselines на dev (2018-01…) и holdout (2022-01…); truth = 0."""
    dev = months_range(date(2018, 1, 1), dev_n)
    hold = months_range(date(2022, 1, 1), hold_n)
    origins = dev + hold
    names = {"cand": cand_err, **base_errs}
    out = {}
    for name, fn in names.items():
        out[name] = [{"model": name, "origin": o, "pred": fn(i, o), "truth": 0.0} for i, o in enumerate(origins)]
    return out


def noise(i, scale):
    return scale * (1 if i % 2 == 0 else -1) * (1 + 0.1 * (i % 5))


class GateTargetTest(unittest.TestCase):
    def build(self, cand_scale, base_scale, naive_scale=None, dev_cand=None, dev_base=None):
        naive_scale = base_scale if naive_scale is None else naive_scale
        dev_end = date(2021, 12, 1)

        def cand(i, o):
            return noise(i, dev_cand if (dev_cand is not None and o <= dev_end) else cand_scale)

        def base(scale, dev_scale=None):
            return lambda i, o: noise(i + 1, dev_scale if (dev_scale is not None and o <= dev_end) else scale)

        pr = target_predictions(
            cand,
            {"B1": base(base_scale, dev_base), "B2": base(base_scale * 1.1, dev_base), "B3": base(base_scale * 1.2, dev_base),
             "weekly_naive": base(naive_scale, dev_base), "v3_direct": base(base_scale * 1.3, dev_base)},
        )
        return pr

    def test_candidate_clearly_better_passes_conditions(self):
        pr = self.build(0.1, 0.3)
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertTrue(out["cond1"])
        self.assertTrue(out["cond3"])
        self.assertTrue(out["cond4"])
        self.assertEqual(out["best_baseline"], "B1")
        self.assertLess(out["dm_p"], 0.05)
        self.assertLess(out["delta_ci"][1], 0)

    def test_candidate_worse_than_best_baseline_fails_cond1(self):
        pr = self.build(0.4, 0.3)
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertFalse(out["cond1"])

    def test_candidate_worse_than_weekly_naive_fails_cond3(self):
        pr = self.build(0.2, 0.3, naive_scale=0.1)
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertFalse(out["cond3"])
        self.assertEqual(out["best_baseline"], "weekly_naive")

    def test_dev_sign_condition(self):
        pr = self.build(0.1, 0.3, dev_cand=0.5, dev_base=0.2)  # на dev кандидат хуже baselines
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertTrue(out["cond1"])
        self.assertFalse(out["cond4"])

    def test_dev_sign_requires_both_best_baseline_and_weekly_naive(self):
        dev = months_range(date(2018, 1, 1), 48)
        hold = months_range(date(2022, 1, 1), 57)
        scales = {  # (dev, holdout)
            "cand": (0.2, 0.1),
            "B1": (0.3, 0.3),  # лучший baseline на holdout; кандидат лучше его и на dev
            "B2": (0.33, 0.33), "B3": (0.36, 0.36), "v3_direct": (0.39, 0.39),
            "weekly_naive": (0.1, 0.35),  # на dev weekly_naive лучше кандидата
        }
        pr = {}
        for name, (d_scale, h_scale) in scales.items():
            origins = dev + hold
            pr[name] = [{"model": name, "origin": o, "pred": noise(i + (name != "cand"), d_scale if o <= date(2021, 12, 1) else h_scale), "truth": 0.0} for i, o in enumerate(origins)]
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertEqual(out["best_baseline"], "B1")
        self.assertTrue(out["cond1"])
        self.assertTrue(out["cond3"])
        self.assertLess(out["dev_delta"]["B1"], 0)
        self.assertGreater(out["dev_delta"]["weekly_naive"], 0)
        self.assertFalse(out["cond4"])

    def test_holdout_common_folds_only(self):
        pr = self.build(0.1, 0.3)
        pr["B3"] = [p for p in pr["B3"] if p["origin"] < date(2025, 1, 1)]
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertEqual(out["n_holdout"], 36)  # 2022-01 … 2024-12

    def test_holdout_excludes_dev_origins(self):
        pr = self.build(0.1, 0.3)
        out = s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))
        self.assertEqual(out["n_holdout"], 57)
        self.assertEqual(out["n_dev"], 48)

    def test_missing_model_raises(self):
        pr = self.build(0.1, 0.3)
        del pr["B2"]
        with self.assertRaises(ValueError):
            s1.gate_target(pr, "cand", date(2022, 1, 1), date(2018, 1, 1), date(2021, 12, 1))


class GateS1Test(unittest.TestCase):
    def target_result(self, **over):
        base = {"cond1": True, "cond3": True, "cond4": True, "dm_p": 0.001, "delta_ci": [-0.2, -0.1]}
        base.update(over)
        return base

    def test_pass_both(self):
        out = s1.decide_s1({"T_SAX": self.target_result(), "T_CORE": self.target_result()})
        self.assertEqual(out["decision"], "pass")

    def test_partial_one_target(self):
        out = s1.decide_s1({"T_SAX": self.target_result(), "T_CORE": self.target_result(cond1=False)})
        self.assertEqual(out["decision"], "partial")
        self.assertEqual(out["passed"], ["T_SAX"])

    def test_fail_none(self):
        out = s1.decide_s1({"T_SAX": self.target_result(cond3=False), "T_CORE": self.target_result(cond4=False)})
        self.assertEqual(out["decision"], "fail")
        self.assertEqual(out["passed"], [])

    def test_holm_blocks_marginal_p(self):
        out = s1.decide_s1({"T_SAX": self.target_result(dm_p=0.03), "T_CORE": self.target_result(dm_p=0.04)})
        self.assertEqual(out["decision"], "fail")

    def test_ci_containing_zero_fails_condition_2(self):
        out = s1.decide_s1({"T_SAX": self.target_result(delta_ci=[-0.1, 0.05]), "T_CORE": self.target_result()})
        self.assertEqual(out["decision"], "partial")
        self.assertEqual(out["passed"], ["T_CORE"])


class V3RowsTest(unittest.TestCase):
    def test_replace_target_values(self):
        months = months_range(date(2015, 1, 1), 5)
        rows = {m: cm.Row(month=m, y={"cpi_all": 9.9}, features={"month": float(m.month), "y_lag1": 0.1}) for m in months}
        out = s1.v3_fold_rows(rows, {months[0]: 0.5, months[1]: None, months[2]: 0.7}, months[3], "T_SAX")
        self.assertEqual([r.month for r in out], [months[0], months[2]])
        self.assertEqual([r.y["T_SAX"] for r in out], [0.5, 0.7])


if __name__ == "__main__":
    unittest.main()
