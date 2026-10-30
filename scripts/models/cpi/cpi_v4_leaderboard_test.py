#!/usr/bin/env python3
"""Офлайн-тесты каркаса лидерборда CPI v4 и регрессионного гейта (#396, Task 4).

Проверяются чистые функции: сетка 3 таргета × h1/h3/h6, накопленные цели,
purge, общие фолды, парное сравнение, MCS, отпечаток предсказаний и сверка с
Stage 1. ClickHouse и сеть не используются.

Запуск — `make cpi-v4-leaderboard-test`.
"""

import math
import random
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from models.cpi import cpi_live3_horizons as l3
from cpi_v4_leaderboard import (
    HORIZONS,
    REGRESSION_TOLERANCE,
    STAGE1_WEEKLY_RMSE,
    TASK4_FEATURE_SHA256,
    TASK4_REFERENCE_RMSE,
    check_features,
    TARGETS,
    check_regression,
    common_folds,
    compare_pair,
    core_from_rows,
    cumulative_by_origin,
    cumulative_pp,
    feature_digests,
    grid_cells,
    mcs_included,
    predictions_digest,
    purged_train_origins,
    select_reference,
    shift_month,
)


def months(first: date, count: int) -> list:
    return [shift_month(first, i) for i in range(count)]


class GridTest(unittest.TestCase):
    def test_nine_cells_in_fixed_order(self):
        self.assertEqual(TARGETS, ("T_NSA", "T_SA", "T_CORE"))
        self.assertEqual(HORIZONS, (1, 3, 6))
        cells = grid_cells()
        self.assertEqual(len(cells), 9)
        self.assertEqual(cells[0], ("T_NSA", 1))
        self.assertEqual(cells[-1], ("T_CORE", 6))
        self.assertEqual(len(set(cells)), 9)


class CumulativeTest(unittest.TestCase):
    def test_compounds_monthly_percent_changes(self):
        series = {date(2022, 2, 1): 1.0, date(2022, 3, 1): 2.0}
        value = cumulative_pp(series, date(2022, 2, 1), 2)
        self.assertAlmostEqual(value, (1.01 * 1.02 - 1.0) * 100.0, places=12)

    def test_missing_month_gives_none(self):
        series = {date(2022, 2, 1): 1.0, date(2022, 4, 1): 2.0}
        self.assertIsNone(cumulative_pp(series, date(2022, 2, 1), 3))

    def test_rejects_non_positive_horizon(self):
        with self.assertRaises(ValueError):
            cumulative_pp({date(2022, 2, 1): 1.0}, date(2022, 2, 1), 0)

    def test_by_origin_covers_the_months_after_the_origin(self):
        series = {m: float(i) for i, m in enumerate(months(date(2022, 1, 1), 8), start=1)}
        by_origin = cumulative_by_origin(series, 3)
        # origin 2022-01 → окно февраль–апрель = значения 2, 3, 4
        self.assertAlmostEqual(by_origin[date(2022, 1, 1)], (1.02 * 1.03 * 1.04 - 1.0) * 100.0, places=12)
        # origin 2022-06 → июль–сентябрь; сентября нет → origin отсутствует
        self.assertNotIn(date(2022, 6, 1), by_origin)
        self.assertIn(date(2022, 5, 1), by_origin)


class PurgeTest(unittest.TestCase):
    def test_boundary_is_origin_minus_horizon_minus_one(self):
        origins = months(date(2022, 1, 1), 24)
        train = purged_train_origins(origins, date(2022, 12, 1), 3)
        self.assertEqual(max(train), date(2022, 8, 1))
        self.assertEqual(train[0], date(2022, 1, 1))

    def test_equals_live3_rule_for_every_horizon(self):
        origins = months(date(2020, 1, 1), 60)
        for horizon in HORIZONS:
            for origin in origins[30:]:
                self.assertEqual(
                    purged_train_origins(origins, origin, horizon),
                    l3.training_origins(origins, origin, horizon),
                )

    def test_train_targets_are_observed_before_the_origin(self):
        origins = months(date(2020, 1, 1), 60)
        for horizon in HORIZONS:
            for origin in origins[30:]:
                for train_origin in purged_train_origins(origins, origin, horizon):
                    self.assertLessEqual(shift_month(train_origin, horizon), shift_month(origin, -1))


class CommonFoldsTest(unittest.TestCase):
    def test_intersection_sorted(self):
        errors = {
            "a": {date(2022, 3, 1): 0.1, date(2022, 1, 1): 0.1, date(2022, 2, 1): 0.1},
            "b": {date(2022, 2, 1): 0.2, date(2022, 3, 1): 0.2},
            "c": {date(2022, 3, 1): 0.3, date(2022, 2, 1): 0.3, date(2022, 4, 1): 0.3},
        }
        self.assertEqual(common_folds(errors), [date(2022, 2, 1), date(2022, 3, 1)])

    def test_subset_of_models(self):
        errors = {"a": {date(2022, 1, 1): 0.1}, "b": {date(2022, 2, 1): 0.1}}
        self.assertEqual(common_folds(errors, ["a"]), [date(2022, 1, 1)])

    def test_empty_when_no_models(self):
        self.assertEqual(common_folds({}), [])


class ComparePairTest(unittest.TestCase):
    def setUp(self):
        rng = random.Random(7)
        self.bad = [rng.gauss(0, 1.0) for _ in range(60)]
        self.good = [rng.gauss(0, 0.4) for _ in range(60)]

    def test_better_model_has_negative_delta_and_small_p(self):
        result = compare_pair(self.good, self.bad, horizon=1)
        self.assertEqual(result["n"], 60)
        self.assertLess(result["delta"], 0.0)
        self.assertLess(result["dm_p"], 0.05)
        self.assertLess(result["delta_ci"][1], 0.0)
        self.assertGreater(result["mde_ratio"], 0.0)

    def test_identical_errors_are_not_different(self):
        result = compare_pair(self.bad, self.bad, horizon=1)
        self.assertEqual(result["delta"], 0.0)
        self.assertEqual(result["dm_p"], 1.0)

    def test_deterministic_with_fixed_seed(self):
        self.assertEqual(
            compare_pair(self.good, self.bad, horizon=3),
            compare_pair(self.good, self.bad, horizon=3),
        )

    def test_length_mismatch_raises(self):
        with self.assertRaises(ValueError):
            compare_pair([0.1, 0.2], [0.1], horizon=1)


class McsTest(unittest.TestCase):
    def _losses(self):
        rng = random.Random(11)
        base = lambda scale: [rng.gauss(0, scale) ** 2 for _ in range(80)]
        return {"good": base(0.3), "also_good": base(0.3), "bad": base(2.0)}

    def test_clearly_bad_model_is_excluded(self):
        included, pvalues = mcs_included(self._losses())
        self.assertIn("good", included)
        self.assertIn("also_good", included)
        self.assertNotIn("bad", included)
        self.assertEqual(set(pvalues), {"good", "also_good", "bad"})

    def test_deterministic(self):
        self.assertEqual(mcs_included(self._losses()), mcs_included(self._losses()))

    def test_needs_at_least_three_models(self):
        with self.assertRaises(ValueError):
            mcs_included({"a": [1.0, 2.0, 3.0], "b": [1.0, 2.0, 3.0]})


class DigestTest(unittest.TestCase):
    ROWS = [
        {"model": "B1", "origin": "2022-03-01", "horizon": 1, "pred": 0.31, "actual": 0.4},
        {"model": "B1", "origin": "2022-02-01", "horizon": 1, "pred": 0.21, "actual": 0.3},
    ]

    def test_order_independent(self):
        self.assertEqual(predictions_digest(self.ROWS), predictions_digest(list(reversed(self.ROWS))))

    def test_value_change_changes_digest(self):
        changed = [dict(self.ROWS[0], pred=0.32), self.ROWS[1]]
        self.assertNotEqual(predictions_digest(self.ROWS), predictions_digest(changed))

    def test_digest_is_sha256_hex(self):
        self.assertRegex(predictions_digest(self.ROWS), r"^[0-9a-f]{64}$")


class RegressionGateTest(unittest.TestCase):
    def test_stage1_table_is_kept_as_history(self):
        self.assertEqual(
            STAGE1_WEEKLY_RMSE,
            {
                "v3_bottomup": 0.311487,
                "v2_bottomup": 0.362300,
                "v3_direct": 0.384900,
                "B3_weekly": 0.390178,
                "v2_baseline": 0.437790,
                "B3": 0.438010,
                "B1": 0.489295,
                "B2": 0.873540,
            },
        )

    def test_task4_reference_is_pinned_current_data_result(self):
        self.assertEqual(
            TASK4_REFERENCE_RMSE,
            {
            "v3_bottomup": 0.312836438,
            "v2_bottomup": 0.367059285,
            "v3_direct": 0.369967068,
            "B3_weekly": 0.391455174,
            "v2_baseline": 0.419888975,
            "B3": 0.432999805,
            "B1": 0.489294773,
            "B2": 0.873540285
        },
        )
        self.assertEqual(REGRESSION_TOLERANCE, 1e-6)

    def test_task4_feature_digests_cover_the_v3_feature_set(self):
        from models.cpi import cpi_monthly_v3 as v3

        self.assertEqual(sorted(TASK4_FEATURE_SHA256), sorted(v3.feature_names_v3()))
        for digest in TASK4_FEATURE_SHA256.values():
            self.assertRegex(digest, r"^[0-9a-f]{64}$")

    def test_default_reference_is_task4_not_stage1(self):
        report = check_regression(dict(TASK4_REFERENCE_RMSE))
        self.assertTrue(report["passed"])
        self.assertEqual([row["expected"] for row in report["rows"]], [TASK4_REFERENCE_RMSE[r["model"]] for r in report["rows"]])
        self.assertFalse(check_regression(dict(STAGE1_WEEKLY_RMSE))["passed"])

    def test_passes_within_tolerance_of_the_reference(self):
        got = {model: value + 4.4e-7 for model, value in TASK4_REFERENCE_RMSE.items()}
        report = check_regression(got)
        self.assertTrue(report["passed"])
        self.assertEqual(len(report["rows"]), 8)

    def test_fails_on_a_difference_above_tolerance(self):
        got = dict(TASK4_REFERENCE_RMSE)
        got["v3_bottomup"] += 2e-6
        report = check_regression(got)
        self.assertFalse(report["passed"])
        failed = [row["model"] for row in report["rows"] if not row["ok"]]
        self.assertEqual(failed, ["v3_bottomup"])

    def test_fails_when_a_model_is_missing_or_nan(self):
        got = dict(TASK4_REFERENCE_RMSE)
        del got["B1"]
        got["B2"] = float("nan")
        report = check_regression(got)
        self.assertFalse(report["passed"])
        failed = sorted(row["model"] for row in report["rows"] if not row["ok"])
        self.assertEqual(failed, ["B1", "B2"])


class CheckFeaturesTest(unittest.TestCase):
    def test_identical_digests_pass(self):
        report = check_features(dict(TASK4_FEATURE_SHA256))
        self.assertTrue(report["passed"])
        self.assertEqual(report["changed"], [])

    def test_changed_and_missing_features_are_named(self):
        got = dict(TASK4_FEATURE_SHA256)
        got["usd_mm"] = "0" * 64
        del got["fao_mm"]
        report = check_features(got)
        self.assertFalse(report["passed"])
        self.assertEqual(report["changed"], ["fao_mm", "usd_mm"])

    def test_extra_features_are_ignored(self):
        got = dict(TASK4_FEATURE_SHA256)
        got["wkagg_mtd"] = "1" * 64
        self.assertTrue(check_features(got)["passed"])


class SelectReferenceTest(unittest.TestCase):
    def test_none_returns_full_task4_table(self):
        self.assertEqual(select_reference(None), TASK4_REFERENCE_RMSE)

    def test_subset_keeps_requested_models_only(self):
        self.assertEqual(
            select_reference(["B1", "B3"]),
            {"B1": TASK4_REFERENCE_RMSE["B1"], "B3": TASK4_REFERENCE_RMSE["B3"]},
        )

    def test_unknown_model_raises(self):
        with self.assertRaises(ValueError):
            select_reference(["B1", "nope"])


class FeatureDigestsTest(unittest.TestCase):
    class _Row:
        def __init__(self, month, features):
            self.month = month
            self.features = features

    def _rows(self, usd=1.0, rate=9.5):
        return [
            self._Row(date(2022, 1, 1), {"usd_mm": usd, "key_rate": rate, "x": float("nan")}),
            self._Row(date(2022, 2, 1), {"usd_mm": 2.0, "key_rate": 9.5, "x": float("nan")}),
        ]

    def test_one_digest_per_feature_and_nan_is_stable(self):
        digests = feature_digests(self._rows())
        self.assertEqual(set(digests), {"usd_mm", "key_rate", "x"})
        self.assertEqual(digests, feature_digests(self._rows()))

    def test_change_in_one_feature_changes_only_its_digest(self):
        base = feature_digests(self._rows())
        changed = feature_digests(self._rows(usd=1.0001))
        self.assertNotEqual(base["usd_mm"], changed["usd_mm"])
        self.assertEqual(base["key_rate"], changed["key_rate"])
        self.assertEqual(base["x"], changed["x"])

    def test_row_order_does_not_matter(self):
        rows = self._rows()
        self.assertEqual(feature_digests(rows), feature_digests(list(reversed(rows))))


class CoreLoaderTest(unittest.TestCase):
    def test_parses_month_value_rows(self):
        rows = [{"month": "2022-02-01", "value": "0.5"}, {"month": "2022-03-01", "value": 0.7}]
        self.assertEqual(
            core_from_rows(rows),
            {date(2022, 2, 1): 0.5, date(2022, 3, 1): 0.7},
        )

    def test_rejects_non_finite_values(self):
        with self.assertRaises(ValueError):
            core_from_rows([{"month": "2022-02-01", "value": "nan"}])

    def test_rejects_duplicate_months(self):
        with self.assertRaises(ValueError):
            core_from_rows([{"month": "2022-02-01", "value": 0.1}, {"month": "2022-02-01", "value": 0.2}])


if __name__ == "__main__":
    unittest.main()
