#!/usr/bin/env python3
"""Офлайн-тесты движка сетки CPI v4 (эксперимент 1, #396 Task 5).

Движок проверяется на фиктивных моделях (быстро, без CatBoost): накопленные
цели, purge, лаги таргетов, B1 по h-окну, общие фолды, MCS и паспорт. Два
смоук-теста гоняют реальные модели на крошечной синтетике. ClickHouse и сеть
не используются.

Запуск — `make cpi-v4-grid-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import ml_experiment as mlx
from models.cpi import cpi_monthly as cm
from cpi_v4_grid import (
    MODEL_SETS,
    SAMPLE_CONFIG,
    assert_no_lookahead_grid,
    build_origin_rows,
    evaluate_cell,
    feature_override,
    make_b1,
    model_names_for,
    passport_config,
    passport_prediction_rows,
    run_walk_forward,
)
from cpi_v4_leaderboard import cumulative_pp, shift_month


def month_list(first: date, count: int) -> list:
    return [shift_month(first, i) for i in range(count)]


def series_of(first: date, values: list) -> dict:
    return {shift_month(first, i): value for i, value in enumerate(values)}


def make_rows(months: list, y_all: dict, comps: dict = None) -> list:
    rows = []
    for month in months:
        y = {"cpi_all": y_all.get(month, 0.0)}
        for code, values in (comps or {}).items():
            y[code] = values.get(month, 0.0)
        rows.append(
            cm.Row(
                month=month,
                y=y,
                features={"y_lag1": 9.0, "y_lag2": 9.5, "usd_mm": 1.0, "key_rate": 8.0, "fao_mm": 0.1, "month": month.month},
            )
        )
    return rows


class ModelSetsTest(unittest.TestCase):
    def test_sets_are_pinned_by_spec_5_1(self):
        self.assertEqual(
            MODEL_SETS["nsa"],
            ("B1", "B2", "B3", "v2_bottomup", "v3_bottomup", "v4a_bottomup", "v4b_bottomup"),
        )
        self.assertEqual(
            MODEL_SETS["direct"],
            ("B1", "B2", "B3", "v2_baseline", "v3_direct", "v4a_direct", "v4b_direct"),
        )

    def test_target_selects_set(self):
        self.assertEqual(model_names_for("T_NSA"), MODEL_SETS["nsa"])
        self.assertEqual(model_names_for("T_SA"), MODEL_SETS["direct"])
        self.assertEqual(model_names_for("T_CORE"), MODEL_SETS["direct"])
        with self.assertRaises(ValueError):
            model_names_for("T_X")

    def test_sample_config_matches_spec(self):
        self.assertEqual(SAMPLE_CONFIG["S_L09"], {"min_train": 60, "primary_from": date(2016, 1, 1)})
        self.assertEqual(SAMPLE_CONFIG["S_L13"], {"min_train": 60, "primary_from": date(2018, 10, 1)})
        self.assertEqual(SAMPLE_CONFIG["S_W22"], {"min_train": 24, "primary_from": None})


class BuildOriginRowsTest(unittest.TestCase):
    def setUp(self):
        self.months = month_list(date(2020, 1, 1), 12)
        self.all_series = series_of(date(2020, 1, 1), [float(i + 1) for i in range(12)])
        self.food = series_of(date(2020, 1, 1), [0.5 * (i + 1) for i in range(12)])
        self.rows = make_rows(self.months, self.all_series, {"cpi_food": self.food})

    def test_nsa_cumulative_target_is_the_window_after_the_origin(self):
        out = build_origin_rows(self.rows, "T_NSA", 3, self.all_series, {"cpi_food": self.food})
        expected = cumulative_pp(self.all_series, date(2020, 2, 1), 3)
        self.assertAlmostEqual(out[date(2020, 1, 1)].y["cpi_all"], expected, places=12)
        expected_food = cumulative_pp(self.food, date(2020, 2, 1), 3)
        self.assertAlmostEqual(out[date(2020, 1, 1)].y["cpi_food"], expected_food, places=12)

    def test_origins_without_a_full_window_are_dropped(self):
        out = build_origin_rows(self.rows, "T_NSA", 3, self.all_series, {})
        self.assertIn(date(2020, 9, 1), out)  # окно октябрь–декабрь 2020 — последнее полное
        self.assertNotIn(date(2020, 10, 1), out)  # окно ноябрь–январь: января 2021 нет

    def test_nsa_keeps_original_lags(self):
        out = build_origin_rows(self.rows, "T_NSA", 1, self.all_series, {})
        self.assertEqual(out[date(2020, 3, 1)].features["y_lag1"], 9.0)

    def test_non_nsa_replaces_lags_by_target_values_and_keeps_only_cpi_all(self):
        core = series_of(date(2020, 1, 1), [0.1 * (i + 1) for i in range(12)])
        out = build_origin_rows(self.rows, "T_CORE", 1, core, None)
        row = out[date(2020, 4, 1)]
        self.assertAlmostEqual(row.features["y_lag1"], core[date(2020, 3, 1)], places=12)
        self.assertAlmostEqual(row.features["y_lag2"], core[date(2020, 2, 1)], places=12)
        self.assertEqual(set(row.y), {"cpi_all"})

    def test_non_nsa_origin_without_lags_is_dropped(self):
        core = {m: 0.1 for m in self.months[2:]}  # ряд начинается с марта
        out = build_origin_rows(self.rows, "T_CORE", 1, core, None)
        for month in (date(2020, 1, 1), date(2020, 2, 1), date(2020, 3, 1), date(2020, 4, 1)):
            self.assertNotIn(month, out)  # апрелю нужен февраль для y_lag2
        self.assertIn(date(2020, 5, 1), out)


class B1Test(unittest.TestCase):
    def test_last_observed_h_window_before_the_origin(self):
        series = series_of(date(2020, 1, 1), [1.0, 2.0, 3.0, 4.0, 5.0, 6.0])
        b1 = make_b1(series, 3)
        row = cm.Row(month=date(2020, 5, 1), y={"cpi_all": 0.0}, features={})
        # origin май, h=3 → окно февраль–апрель (O−3…O−1)
        self.assertAlmostEqual(b1([], row), cumulative_pp(series, date(2020, 2, 1), 3), places=12)

    def test_missing_window_raises_degenerate(self):
        b1 = make_b1({date(2020, 4, 1): 1.0}, 3)
        row = cm.Row(month=date(2020, 5, 1), y={"cpi_all": 0.0}, features={})
        with self.assertRaises(cm.DegenerateWindowError):
            b1([], row)


class WalkForwardTest(unittest.TestCase):
    def setUp(self):
        self.months = month_list(date(2018, 1, 1), 40)
        self.series = series_of(date(2018, 1, 1), [0.3] * 40)
        self.rows = make_rows(self.months, self.series)
        self.h = 3
        self.origin_rows = build_origin_rows(self.rows, "T_SA", self.h, self.series, None)

    def test_train_respects_purge_and_min_train(self):
        seen = []

        def spy(train, row):
            seen.append((row.month, [item.month for item in train]))
            return 0.9

        preds = run_walk_forward(self.origin_rows, {"spy": spy}, self.h, min_train=12)
        self.assertTrue(preds)
        for origin, train_months in seen:
            self.assertLessEqual(len(train_months), len(self.origin_rows))
            self.assertGreaterEqual(len(train_months), 12)
            self.assertLessEqual(max(train_months), shift_month(origin, -self.h - 1))
        self.assertEqual(min(p["origin"] for p in preds), seen[0][0].isoformat())

    def test_prediction_rows_have_actual_and_fixed_keys(self):
        preds = run_walk_forward(self.origin_rows, {"m": lambda t, r: 1.0}, self.h, min_train=12)
        item = preds[0]
        self.assertEqual(set(item), {"model", "origin", "horizon", "pred", "actual"})
        self.assertEqual(item["horizon"], self.h)
        self.assertAlmostEqual(item["actual"], cumulative_pp(self.series, shift_month(date.fromisoformat(item["origin"]), 1), self.h), places=12)

    def test_degenerate_model_is_skipped_for_that_fold_only(self):
        def flaky(train, row):
            if row.month == date(2019, 6, 1):
                raise cm.DegenerateWindowError("вырожденное окно")
            return 0.5

        preds = run_walk_forward(self.origin_rows, {"flaky": flaky, "ok": lambda t, r: 0.5}, self.h, min_train=12)
        flaky_origins = {p["origin"] for p in preds if p["model"] == "flaky"}
        ok_origins = {p["origin"] for p in preds if p["model"] == "ok"}
        self.assertNotIn("2019-06-01", flaky_origins)
        self.assertIn("2019-06-01", ok_origins)

    def test_other_exceptions_propagate(self):
        def broken(train, row):
            raise RuntimeError("баг")

        with self.assertRaises(RuntimeError):
            run_walk_forward(self.origin_rows, {"broken": broken}, self.h, min_train=12)

    def test_lookahead_guard_rejects_a_train_origin_too_close(self):
        origin = date(2019, 6, 1)
        assert_no_lookahead_grid([date(2019, 2, 1)], origin, 3)  # цель до 2019-05
        with self.assertRaises(AssertionError):
            assert_no_lookahead_grid([date(2019, 3, 1)], origin, 3)  # цель до 2019-06 = O


class EvaluateCellTest(unittest.TestCase):
    def _preds(self):
        rows = []
        origins = month_list(date(2020, 1, 1), 40)
        noise = [(-1) ** i * 0.1 * ((i % 5) + 1) for i in range(40)]
        for i, origin in enumerate(origins):
            actual = 1.0
            for model, scale in (("ref", 1.0), ("good", 0.3), ("good2", 0.35), ("bad", 3.0), ("B3", 1.2)):
                if model == "bad" and i < 4:
                    continue  # у bad нет первых четырёх фолдов → общие фолды сокращаются
                rows.append({"model": model, "origin": origin.isoformat(), "horizon": 1, "pred": actual + scale * noise[i], "actual": actual})
        return rows

    def test_common_folds_primary_window_and_pairs(self):
        result = evaluate_cell(self._preds(), horizon=1, reference="ref", primary_from=date(2020, 3, 1))
        # bad начинается с 2020-05 → общие фолды с 2020-05 по 2023-04 = 36
        self.assertEqual(result["n_common"], 36)
        self.assertEqual(result["first_origin"], "2020-05-01")
        self.assertEqual(set(result["models"]), {"ref", "good", "good2", "bad", "B3"})
        for model in result["models"]:
            self.assertEqual(result["models"][model]["n"], 36)
        self.assertIn("good vs ref", result["pairs"])
        self.assertIn("good vs B3", result["pairs"])
        self.assertLess(result["pairs"]["good vs ref"]["delta"], 0.0)

    def test_mcs_excludes_the_bad_model(self):
        result = evaluate_cell(self._preds(), horizon=1, reference="ref", primary_from=None)
        self.assertIn("good", result["mcs"]["included"])
        self.assertNotIn("bad", result["mcs"]["included"])

    def test_digest_is_stable(self):
        a = evaluate_cell(self._preds(), horizon=1, reference="ref", primary_from=None)
        b = evaluate_cell(list(reversed(self._preds())), horizon=1, reference="ref", primary_from=None)
        self.assertEqual(a["predictions_sha256"], b["predictions_sha256"])
        self.assertEqual(a["models"], b["models"])

    def test_too_few_common_folds_is_reported_not_computed(self):
        preds = [p for p in self._preds() if p["origin"] <= "2020-03-01"]
        result = evaluate_cell(preds, horizon=1, reference="ref", primary_from=None)
        self.assertEqual(result["status"], "insufficient_folds")


class PassportTest(unittest.TestCase):
    def test_config_is_deterministic_and_complete(self):
        a = passport_config("S_L09", ["T_NSA"], [1, 3], quick=False, min_train=60, primary_from=date(2016, 1, 1))
        b = passport_config("S_L09", ["T_NSA"], [1, 3], quick=False, min_train=60, primary_from=date(2016, 1, 1))
        self.assertEqual(mlx.canonical_json(a), mlx.canonical_json(b))
        for key in ("sample", "targets", "horizons", "quick", "min_train", "primary_from", "seed", "models", "spec"):
            self.assertIn(key, a)
        self.assertIs(a["quick"], False)
        self.assertEqual(a["models"]["T_NSA"], list(MODEL_SETS["nsa"]))

    def test_quick_flag_changes_config_hash(self):
        full = passport_config("S_L09", ["T_NSA"], [1], quick=False, min_train=60, primary_from=None)
        quick = passport_config("S_L09", ["T_NSA"], [1], quick=True, min_train=60, primary_from=None)
        self.assertNotEqual(mlx.canonical_json(full), mlx.canonical_json(quick))

    def test_prediction_rows_map_to_ml_log_fields(self):
        preds = [{"model": "B1", "origin": "2020-03-01", "horizon": 3, "pred": 1.5, "actual": 1.0}]
        rows = passport_prediction_rows(preds, target="T_CORE", sample="S_L09")
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.model, "B1")
        self.assertEqual(row.window, "T_CORE:h3")
        self.assertEqual(row.target_date, date(2020, 3, 1))
        self.assertEqual((row.predicted, row.actual, row.unit), (1.5, 1.0, "pp"))
        self.assertEqual(row.meta["sample"], "S_L09")
        self.assertEqual(row.meta["horizon"], 3)


class FeatureOverrideTest(unittest.TestCase):
    def test_excluded_features_vanish_inside_and_return_after(self):
        before = cm.feature_names(with_mp=True)
        self.assertIn("usd_mm", before)
        with feature_override(("usd_mm", "key_rate")):
            inside = cm.feature_names(with_mp=True)
            self.assertNotIn("usd_mm", inside)
            self.assertNotIn("key_rate", inside)
            self.assertEqual(inside[-1], "month")
        self.assertEqual(cm.feature_names(with_mp=True), before)

    def test_restored_even_on_exception(self):
        before = cm.feature_names(with_mp=True)
        with self.assertRaises(RuntimeError):
            with feature_override(("usd_mm",)):
                raise RuntimeError("x")
        self.assertEqual(cm.feature_names(with_mp=True), before)

    def test_empty_exclusion_is_a_noop(self):
        before = cm.feature_names(with_mp=True)
        with feature_override(()):
            self.assertEqual(cm.feature_names(with_mp=True), before)


class RealModelSmokeTest(unittest.TestCase):
    """Реальные модели на крошечной синтетике: фит проходит и даёт конечное число."""

    def _train(self, with_fx: bool):
        import random

        rng = random.Random(3)
        months = month_list(date(2016, 1, 1), 40)
        rows = []
        for month in months:
            features = {
                "y_lag1": rng.random(), "y_lag2": rng.random(), "fao_mm": rng.random(), "month": month.month,
                "price_expectations_sa": rng.random(), "price_expectations_sa_chg": rng.random(),
                "business_climate_sa": rng.random(), "business_climate_sa_chg": rng.random(),
                "capacity_utilization_sa": rng.random(), "headcount_expectations_sa": rng.random(),
                "credit_conditions": rng.random(),
            }
            if with_fx:
                features["usd_mm"] = rng.random()
                features["key_rate"] = 8.0 + rng.random()
            y = {code: rng.random() for code in ("cpi_all", "cpi_food", "cpi_nonfood", "cpi_services")}
            rows.append(cm.Row(month=month, y=y, features=features))
        return rows[:-1], rows[-1]

    def test_v2_baseline_and_enet_run_without_fx_features_under_override(self):
        from models.cpi import cpi_monthly_v3 as v3

        train, row = self._train(with_fx=False)
        with feature_override(("usd_mm", "key_rate")):
            for fit in (cm.fit_catboost_direct_mp, cm.fit_enet_mp, cm.fit_ar1):
                self.assertTrue(math.isfinite(fit(train, row)), fit.__name__)

    def test_v4_catboost_with_explicit_names_runs(self):
        from cpi_v4_grid import fit_catboost_names

        train, row = self._train(with_fx=True)
        names = ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm", "month"]
        self.assertTrue(math.isfinite(fit_catboost_names(train, row, "cpi_all", names)))


if __name__ == "__main__":
    unittest.main()
