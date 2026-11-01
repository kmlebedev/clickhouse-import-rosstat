#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_backend_compare` (Task 17 #501): без ClickHouse, R и архива."""

import sys
import unittest
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "cpi"))

from lib import ml_experiment as mlx
from models.cb_comp import cb_comp_backend_compare as bc
from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_live_bottomup as lb


def synthetic_rows(count: int = 60, start: date = date(2019, 1, 1)) -> list:
    rows = []
    for index in range(count):
        month = bu.shift_month(start, index)
        features = {
            "y_lag1": 0.01 * index,
            "y_lag2": 0.005 * index,
            "usd_mm": 60.0 + index % 5,
            "key_rate": 6.0 + (index % 3) * 0.25,
            "fao_mm": 100.0 + index % 4,
            "month": month.month,
        }
        rows.append((month, features, {1: 0.02 * index - 0.4}))
    return rows


def pair(delta: float, p: float, ci: tuple, rmse: float = 0.2) -> dict:
    return {"delta": delta, "dm_p": p, "delta_ci": list(ci), "rmse_right": rmse,
            "rmse_left": rmse + delta, "mde_ratio": 0.1}


def neutral_pairs() -> dict:
    return {1: pair(0.0, 0.9, (-0.01, 0.01)),
            3: pair(0.0, 0.9, (-0.01, 0.01)),
            6: pair(0.0, 0.9, (-0.01, 0.01))}


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(bc.HORIZONS, bu.HORIZONS)
        self.assertEqual(bc.PRIMARY_HORIZONS, (1, 3, 6))
        self.assertEqual(bc.RUNS[0], ("catboost", "defaults"))
        self.assertEqual(len(bc.RUNS), 5)
        self.assertNotIn(("catboost", "matched"), bc.RUNS)
        self.assertEqual(bc.MATCHED, {"n_estimators": 1000, "learning_rate": 0.03})
        self.assertEqual(bc.NO_HARM_MARGIN, 0.02)


class FeatureFrameTest(unittest.TestCase):
    def test_dtypes_category_and_nan(self):
        rows = synthetic_rows(3)
        rows[1][1]["usd_mm"] = float("nan")
        names = list(rows[0][1])
        frame = bc.feature_frame(rows, names)
        self.assertEqual(len(frame), 3)
        for name in ("y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm"):
            self.assertEqual(str(frame[name].dtype), "float64", name)
        self.assertEqual(str(frame["month"].dtype), "category")
        self.assertEqual(list(frame["month"].cat.categories), bc.MONTH_CATEGORIES)
        self.assertTrue(np.isnan(frame["usd_mm"].iloc[1]))

    def test_empty_rows_keep_columns(self):
        frame = bc.feature_frame([], ["y_lag1", "month"])
        self.assertEqual(list(frame.columns), ["y_lag1", "month"])
        self.assertEqual(len(frame), 0)


class BackendTest(unittest.TestCase):
    def test_each_backend_is_deterministic(self):
        rows = synthetic_rows(60)
        names = list(rows[0][1])
        for name, arm in (("catboost", "defaults"), ("lightgbm", "defaults"),
                          ("lightgbm", "matched"), ("xgboost", "defaults"), ("xgboost", "matched")):
            with self.subTest(backend=name, arm=arm):
                backend = bc.make_backend(name, arm)
                first = backend.fit(rows, names, 1)
                second = backend.fit(rows, names, 1)
                auto = backend.predict(first, rows, names)
                again = backend.predict(second, rows, names)
                self.assertEqual(len(auto), len(rows))
                self.assertTrue(all(np.isfinite(auto)))
                np.testing.assert_allclose(auto, again, rtol=0, atol=1e-10)
                self.assertIsNotNone(backend.trees(first))

    def test_matched_arm_uses_equal_budget(self):
        for name in ("lightgbm", "xgboost"):
            backend = bc.make_backend(name, "matched")
            params = backend.hyperparameters()
            self.assertEqual(params["n_estimators"], 1000)
            self.assertEqual(params["learning_rate"], 0.03)
            self.assertNotIn("n_estimators", bc.make_backend(name, "defaults").hyperparameters())

    def test_unknown_backend_or_arm_rejected(self):
        with self.assertRaises(ValueError):
            bc.make_backend("xgboost", "turbo")
        with self.assertRaises(KeyError):
            bc.make_backend("sklearn", "defaults")


class TimingTest(unittest.TestCase):
    def test_summary_aggregates(self):
        records = [{"fit_seconds": value, "predict_seconds": value / 10, "trees": 10}
                   for value in (0.1, 0.2, 0.3, 0.4)]
        summary = bc.timing_summary(records, 2, 0.05)
        self.assertEqual(summary["n_fits"], 4)
        self.assertEqual(summary["n_fits_skipped"], 2)
        self.assertAlmostEqual(summary["fit_seconds_total"], 1.0)
        self.assertAlmostEqual(summary["fit_seconds_mean"], 0.25)
        self.assertAlmostEqual(summary["fit_seconds_median"], 0.25)
        self.assertAlmostEqual(summary["fit_seconds_p95"], 0.385)
        self.assertAlmostEqual(summary["trees_mean"], 10.0)


class DecideTest(unittest.TestCase):
    def test_swap_candidate(self):
        pairs = neutral_pairs()
        pairs[1] = pair(-0.03, 0.01, (-0.05, -0.01))
        outcome = bc.decide(pairs, {1: 1.0, 3: 1.0, 6: 1.0},
                            {"fit_seconds_median": 1.0}, {"fit_seconds_median": 1.0})
        self.assertEqual(outcome["decision"], "swap_candidate")
        self.assertTrue(outcome["checks"]["swap"])

    def test_significant_loss_on_h3_blocks_swap(self):
        pairs = neutral_pairs()
        pairs[1] = pair(-0.03, 0.01, (-0.05, -0.01))
        pairs[3] = pair(0.04, 0.01, (0.01, 0.08))
        outcome = bc.decide(pairs, {1: 1.0, 3: 1.0, 6: 1.0},
                            {"fit_seconds_median": 0.1}, {"fit_seconds_median": 1.0})
        self.assertFalse(outcome["checks"]["swap"])
        self.assertTrue(any("h3" in reason for reason in outcome["reasons"]))

    def test_speed_candidate_needs_non_inferiority_and_speed(self):
        slow = bc.decide(neutral_pairs(), {1: 1.0, 3: 1.0, 6: 1.0},
                         {"fit_seconds_median": 0.6}, {"fit_seconds_median": 1.0})
        self.assertEqual(slow["decision"], "keep_catboost")
        fast = bc.decide(neutral_pairs(), {1: 1.0, 3: 1.0, 6: 1.0},
                         {"fit_seconds_median": 0.4}, {"fit_seconds_median": 1.0})
        self.assertEqual(fast["decision"], "speed_candidate")

    def test_wide_ci_blocks_speed_candidate(self):
        pairs = neutral_pairs()
        pairs[1] = pair(0.0, 0.9, (-0.04, 0.04))
        outcome = bc.decide(pairs, {1: 1.0, 3: 1.0, 6: 1.0},
                            {"fit_seconds_median": 0.4}, {"fit_seconds_median": 1.0})
        self.assertEqual(outcome["decision"], "keep_catboost")


class MetricsTest(unittest.TestCase):
    def test_metrics_are_finite_and_named(self):
        record = {
            "backend": "lightgbm",
            "model_live": "live_cb_lightgbm",
            "model_combo": "combo3_lightgbm",
            "per_model": {
                "live_cb_lightgbm": {h: {"rmse": 0.2, "rmse_t_live": 0.3, "rmse_all67": 0.4,
                                         "vs_cb_target_full": {"ratio": 1.0}} for h in bc.HORIZONS},
                "combo3_lightgbm": {h: {"rmse": 0.18, "rmse_t_live": 0.3, "rmse_all67": 0.4,
                                        "vs_cb_target_full": {"ratio": 0.95}} for h in bc.HORIZONS},
            },
            "pairs_vs_catboost": {h: pair(0.0, 0.9, (-0.01, 0.01)) for h in bc.HORIZONS},
            "combo_ratio": {h: 1.0 for h in bc.HORIZONS},
            "timing": {"n_fits": 352, "n_fits_skipped": 0, "fit_seconds_total": 10.0,
                       "fit_seconds_mean": 0.03, "fit_seconds_median": 0.02,
                       "fit_seconds_p95": 0.05, "fit_seconds_max": 0.1,
                       "predict_seconds_total": 1.0, "predict_seconds_median": 0.001,
                       "trees_mean": 100.0},
            "decision": {"checks": {"swap": False, "speed": True}},
        }
        reference = {"timing": {"fit_seconds_median": 0.04}}
        mcs = {1: {"included": ["live_cb_lightgbm", "cb_comp"]},
               3: {"included": ["live_cb_lightgbm"]},
               6: {"included": ["live_cb_lightgbm"]}}
        metrics = bc.build_metrics(record, mcs, {"ok": True}, {"max_abs_diff": 0.001, "ok": True}, reference)
        self.assertEqual(metrics["primary_metric"], "rmse_pp")
        self.assertAlmostEqual(metrics["ratio_cb_comp_h1"], 1.0)
        self.assertAlmostEqual(metrics["combo3_ratio_cb_comp_h1"], 0.95)
        self.assertAlmostEqual(metrics["dm_p_vs_catboost_h1"], 0.9)
        self.assertAlmostEqual(metrics["speed_ratio_median"], 0.5)
        self.assertEqual(metrics["mcs_included_h1"], 1.0)
        self.assertEqual(metrics["r_b1_ok"], 1.0)
        for value in metrics.values():
            if isinstance(value, float):
                self.assertTrue(np.isfinite(value))


class HookTest(unittest.TestCase):
    def test_fit_predict_uses_injected_regressor(self):
        months = [bu.shift_month(date(2020, 1, 1), index) for index in range(32)]
        train_months, test_months = months[:30], months[30:]
        spec = bu.GroupSpec(code="01", bsu_codes=(), level=1)
        features = {
            "01": {month: {"y_lag1": 0.1, "y_lag2": 0.2, "usd_mm": 60.0, "key_rate": 6.0,
                           "fao_mm": 100.0, "month": month.month} for month in months}
        }
        targets = {horizon: pd.DataFrame({"01": [0.5] * len(months)}, index=months) for horizon in bc.HORIZONS}

        class Fake:
            def __init__(self):
                self.fits = 0
                self.predicts = 0

            def fit(self, rows, names, horizon):
                self.fits += 1
                return {"rows": len(rows), "horizon": horizon}

            def predict(self, model, rows, names):
                self.predicts += 1
                return [0.123] * len(rows)

        fake = Fake()
        out = lb.fit_predict([spec], features, features, targets, train_months, test_months,
                             with_mp=False, regressor=fake)
        self.assertEqual(fake.fits, len(bc.HORIZONS))
        self.assertEqual(fake.predicts, len(bc.HORIZONS))
        for horizon in bc.HORIZONS:
            self.assertEqual(set(out["01"][horizon].values()), {0.123})


class MlogTagsTest(unittest.TestCase):
    def test_extra_tags_do_not_override_passport(self):
        run = mlx.start_run("exp", "run", {"backend": "lightgbm"}, date(2024, 7, 1), {}, [],
                            enabled=False, tags={"backend": "lightgbm", "experiment": "evil"})
        tags = run._mlflow_tags()
        self.assertEqual(tags["backend"], "lightgbm")
        self.assertEqual(tags["experiment"], "exp")
        run.set_tags({"decision": "keep_catboost"})
        self.assertEqual(run.extra_tags["decision"], "keep_catboost")

    def test_same_sample_hash_across_backends(self):
        sample = [{"horizon": 1, "month": "2019-01-01", "target_full": 0.1, "t_live": 0.1}]
        first = mlx.start_run("exp", "a", {"backend": "catboost"}, date(2024, 7, 1), {}, sample, enabled=False)
        second = mlx.start_run("exp", "b", {"backend": "lightgbm"}, date(2024, 7, 1), {}, sample, enabled=False)
        self.assertEqual(first._mlflow_tags()["sample_hash"], second._mlflow_tags()["sample_hash"])
        self.assertNotEqual(first._mlflow_tags()["config_hash"], second._mlflow_tags()["config_hash"])


if __name__ == "__main__":
    unittest.main()
