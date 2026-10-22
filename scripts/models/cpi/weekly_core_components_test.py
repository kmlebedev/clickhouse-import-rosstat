#!/usr/bin/env python3
"""Офлайн-тесты research-скрипта core-прокси из недельных компонент (#364).

Проверяются только чистые функции: парсинг CSV-фикстуры, взвешенный
core-субиндекс, пропуск месяца при <30 рядах, годовое выражение,
Fourier-дизайн, expanding-разбиение, ridge-бридж, вызов DM и фиксация сбоя
UCM. ClickHouse, сеть и запись в БД не используются.

Запуск — `make weekly-core-components-test`.
"""

import math
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

import weekly_core_components as wcc

from lib import eval_utils as eu

FIXTURE = Path(__file__).resolve().parent / "testdata" / "weekly_core_components_panel_mini.csv"


class PanelParsingTest(unittest.TestCase):
    def test_fixture_parsing_filters_exclusions_weights_and_blank_mm(self):
        raw = wcc.read_panel(FIXTURE)
        self.assertEqual(len(raw), 38)
        rows = wcc.usable_rows(raw)
        # 30 валидных января + 5 февраля; исключённая группа, нулевой вес и
        # пустой mm_pp отброшены.
        self.assertEqual(len(rows), 35)
        self.assertNotIn("Яйца", {row.group_name for row in rows})
        self.assertEqual({row.month for row in rows}, {"2024-01", "2024-02"})
        self.assertTrue(all(row.weight > 0 for row in rows))

    def test_panel_month_validation(self):
        self.assertEqual(wcc.panel_month("2024-01-01"), "2024-01")
        with self.assertRaises(ValueError):
            wcc.panel_month("2024")


class CoreSubindexTest(unittest.TestCase):
    def test_weighted_mean_excludes_groups_and_zero_weight(self):
        index, skipped = wcc.core_subindex(wcc.usable_rows(wcc.read_panel(FIXTURE)), 30)
        self.assertIn("2024-01", index)
        self.assertAlmostEqual(index["2024-01"]["mm"], 1.0, places=12)
        self.assertEqual(index["2024-01"]["n_items"], 30)
        self.assertAlmostEqual(index["2024-01"]["weight_sum"], 0.3, places=12)

    def test_month_below_min_items_is_skipped(self):
        index, skipped = wcc.core_subindex(wcc.usable_rows(wcc.read_panel(FIXTURE)), 30)
        self.assertNotIn("2024-02", index)
        self.assertIn("2024-02", skipped)
        self.assertEqual(skipped["2024-02"]["n_items"], 5)
        self.assertIn("< 30", skipped["2024-02"]["reason"])

    def test_weighted_mean_uses_weights(self):
        from weekly_core_components import PanelRow

        rows = [
            PanelRow("2024-01", "a", "food", "Мясопродукты", 0.25, 1.0),
            PanelRow("2024-01", "b", "food", "Мясопродукты", 0.75, 3.0),
        ]
        index, _ = wcc.core_subindex(rows, min_items=2)
        self.assertAlmostEqual(index["2024-01"]["mm"], 2.5, places=12)
        self.assertAlmostEqual(index["2024-01"]["weight_sum"], 1.0, places=12)


class AnnualizationTest(unittest.TestCase):
    def test_annualize_mm_uses_twelve_months(self):
        expected = (1.01**12 - 1.0) * 100.0
        self.assertAlmostEqual(wcc.annualize_mm(1.0), expected, places=10)

    def test_annualize_zero_is_zero(self):
        self.assertEqual(wcc.annualize_mm(0.0), 0.0)


class FourierDesignTest(unittest.TestCase):
    def test_tariff_and_january_dummies(self):
        july = wcc.fourier_design("2024-07")
        january = wcc.fourier_design("2024-01")
        self.assertEqual(july[0], 1.0)
        self.assertEqual(july[-2], 1.0)  # tariff
        self.assertEqual(july[-1], 0.0)  # January
        self.assertEqual(january[-2], 0.0)
        self.assertEqual(january[-1], 1.0)

    def test_three_harmonics_values(self):
        design = wcc.fourier_design("2024-07")
        self.assertEqual(len(design), 1 + 2 * wcc.FOURIER_HARMONICS + 2)
        angle = 2.0 * math.pi * 7.0 / 12.0
        self.assertAlmostEqual(design[1], math.sin(angle), places=12)
        self.assertAlmostEqual(design[2], math.cos(angle), places=12)
        self.assertAlmostEqual(design[5], math.sin(3.0 * angle), places=12)
        self.assertAlmostEqual(design[6], math.cos(3.0 * angle), places=12)


class WindowTest(unittest.TestCase):
    def test_test_months_require_min_train(self):
        keys = wcc.months_between("2021-01", "2026-09")
        self.assertEqual(len(keys), 69)
        test = wcc.test_months(keys, 36)
        self.assertEqual(test[0], "2024-01")
        self.assertEqual(test[-1], "2026-09")
        self.assertEqual(len(test), 33)

    def test_2022_window_starts_test_in_2025(self):
        keys = wcc.months_between("2022-01", "2026-09")
        test = wcc.test_months(keys, 36)
        self.assertEqual(test[0], "2025-01")
        self.assertEqual(len(test), 21)

    def test_early_slice_has_twelve_test_months(self):
        keys = wcc.months_between("2021-01", "2024-12")
        self.assertEqual(len(keys), 48)
        test = wcc.test_months(keys, 36)
        self.assertEqual((test[0], test[-1], len(test)), ("2024-01", "2024-12", 12))

    def test_build_windows_default_order(self):
        windows = wcc._build_windows(None, "2026-09", early_slice=True)
        self.assertEqual(
            [name for name, _, _ in windows],
            ["2021-01..2026-09", "2022-01..2026-09", "2021-01..2024-12"],
        )


class ExpandingSplitTest(unittest.TestCase):
    def test_time_series_split_boundaries(self):
        self.assertEqual(
            wcc.expanding_splits(36, 3),
            [(0, 9, 18), (0, 18, 27), (0, 27, 36)],
        )

    def test_short_series_has_no_splits(self):
        self.assertEqual(wcc.expanding_splits(1, 3), [])


class RidgeTest(unittest.TestCase):
    def test_ridge_cv_is_deterministic_and_recovers_signal(self):
        generator = np.random.default_rng(42)
        x = generator.normal(size=(80, 3))
        y = 2.0 * x[:, 0] - 1.0 * x[:, 1] + 0.5 * x[:, 2]
        train_x, test_x = x[:70], x[70:]
        train_y, test_y = y[:70], y[70:]
        first = wcc.ridge_cv_predict(train_x, train_y, test_x[0])
        second = wcc.ridge_cv_predict(train_x, train_y, test_x[0])
        self.assertEqual(first.prediction, second.prediction)
        self.assertEqual(first.alpha, second.alpha)
        self.assertIn(first.alpha, wcc.RIDGE_ALPHAS)
        errors = [
            wcc.ridge_cv_predict(train_x, train_y, row).prediction - target
            for row, target in zip(test_x, test_y)
        ]
        self.assertLess(eu.mae(errors), 0.1)

    def test_m4_features_need_three_lags_and_b0(self):
        core = {
            "2024-01": 1.0,
            "2024-02": 1.0,
            "2024-03": 1.0,
            "2024-04": 1.0,
        }
        baseline = {"2024-04": 2.0}
        self.assertIsNone(wcc.m4_features("2024-04", core, {}))
        features = wcc.m4_features("2024-04", core, baseline)
        self.assertEqual(
            features[:5], [core["2024-04"], 2.0, core["2024-03"], core["2024-02"], core["2024-01"]]
        )
        self.assertEqual(len(features), 11)
        self.assertIsNone(
            wcc.m4_features("2024-04", core, baseline, {"2024-04", "2024-03", "2024-02"})
        )


class MetricsTest(unittest.TestCase):
    def test_metrics_calls_dm_and_flags_constant_advantage(self):
        actual = {f"2024-{month:02d}": 0.0 for month in range(1, 7)}
        prediction = {key: 1.0 for key in actual}
        baseline = {key: 0.5 for key in actual}
        result = wcc.metrics(actual, prediction, baseline)
        self.assertEqual(result["n"], 6)
        self.assertAlmostEqual(result["mae"], 1.0, places=12)
        self.assertAlmostEqual(result["baseline"]["mae"], 0.5, places=12)
        self.assertEqual(result["dm_vs_b0"]["p"], 0.0)

    def test_identical_errors_give_dm_p_one(self):
        actual = {f"2024-{month:02d}": 0.0 for month in range(1, 7)}
        prediction = {key: 0.5 for key in actual}
        result = wcc.metrics(actual, prediction, prediction)
        self.assertEqual(result["dm_vs_b0"], {"stat": 0.0, "p": 1.0})

    def test_dm_matches_shared_helper(self):
        left = [0.1, -0.2, 0.3, 0.05]
        right = [-0.1, 0.1, 0.2, -0.05]
        self.assertEqual(
            eu.diebold_mariano(left, right, h=1),
            eu.diebold_mariano(left, right, h=1),
        )


class UcmFailureTest(unittest.TestCase):
    def test_ucm_failure_is_recorded_not_raised(self):
        keys = wcc.months_between("2024-01", "2026-06")
        core_mm = {key: 1.0 for key in keys}
        with patch.object(wcc, "UnobservedComponents", side_effect=RuntimeError("boom")):
            value, failure = wcc.ucm_forecast(core_mm, "2026-07", keys)
        self.assertIsNone(value)
        self.assertEqual(failure["kind"], "failed")
        self.assertIn("boom", failure["error"])

    def test_ucm_skips_short_train(self):
        keys = wcc.months_between("2024-01", "2024-06")
        core_mm = {key: 1.0 for key in keys}
        value, failure = wcc.ucm_forecast(core_mm, "2024-07", keys)
        self.assertIsNone(value)
        self.assertEqual(failure["kind"], "skipped")


if __name__ == "__main__":
    unittest.main()
