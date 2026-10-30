#!/usr/bin/env python3
"""Офлайн-тесты сверки данных CPI с датасетом CB_COMP (#401 W3b, протокол спеки).

Чистые функции: уровни → м/м, выравнивание накопленного таргета (`M` = конец окна ↔ строка `O = M−h+1`),
метрики на окнах, подразумеваемый вес КУ, калибровка веса и правило его смены, решение
`aligned`/`misaligned`, сверка рядов КУ и `1 - 9470` с `TARGET_FULL`. Архив, R, ClickHouse и сеть не
используются. Запуск — `make cb-comp-data-reconcile-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cb_comp_data_reconcile as rc


def months_range(first: date, n: int) -> list:
    out, year, month = [], first.year, first.month
    for _ in range(n):
        out.append(date(year, month, 1))
        year, month = (year + 1, 1) if month == 12 else (year, month + 1)
    return out


def shift(m: date, k: int) -> date:
    i = m.year * 12 + (m.month - 1) + k
    return date(i // 12, i % 12 + 1, 1)


class WindowsTest(unittest.TestCase):
    def test_test_window_has_67_months_and_normal_54(self):
        self.assertEqual(len(rc.window_months("test")), 67)
        self.assertEqual(len(rc.window_months("normal")), 54)
        self.assertNotIn(date(2020, 4, 1), rc.window_months("normal"))
        self.assertTrue(all(m.year != 2022 for m in rc.window_months("normal")))

    def test_train_window_matches_cb_comp_train(self):
        months = rc.window_months("train")
        self.assertEqual((months[0], months[-1]), (date(2010, 3, 1), date(2018, 12, 1)))

    def test_unknown_window(self):
        with self.assertRaises(ValueError):
            rc.window_months("x")


class LevelsTest(unittest.TestCase):
    def test_levels_to_mm(self):
        out = rc.levels_to_mm({date(2020, 1, 1): 100.5, date(2020, 2, 1): 99.8})
        self.assertAlmostEqual(out[date(2020, 1, 1)], 0.5)
        self.assertAlmostEqual(out[date(2020, 2, 1)], -0.2)


class AlignCumulativeTest(unittest.TestCase):
    def test_target_full_month_is_window_end(self):
        mm = {m: 0.5 for m in months_range(date(2020, 1, 1), 12)}
        out = rc.align_cumulative(mm, 3)
        # `TARGET_FULL[3][M]` = рост за M−2 … M: первые два месяца без окна.
        self.assertNotIn(date(2020, 1, 1), out)
        self.assertNotIn(date(2020, 2, 1), out)
        self.assertAlmostEqual(out[date(2020, 3, 1)], (1.005**3 - 1) * 100)

    def test_matches_cumulative_of_origin_row(self):
        import cpi_two_stage_folds as tf

        mm = {m: 0.1 * i for i, m in enumerate(months_range(date(2020, 1, 1), 12))}
        out = rc.align_cumulative(mm, 3)
        for m, value in out.items():
            self.assertAlmostEqual(value, tf.cumulative(mm, shift(m, -2), 3))

    def test_hole_gives_no_value(self):
        mm = {m: 0.5 for m in months_range(date(2020, 1, 1), 6)}
        del mm[date(2020, 3, 1)]
        out = rc.align_cumulative(mm, 2)
        self.assertNotIn(date(2020, 3, 1), out)
        self.assertNotIn(date(2020, 4, 1), out)
        self.assertIn(date(2020, 5, 1), out)


class MetricsOnWindowsTest(unittest.TestCase):
    def test_compare_on_windows_reports_each_window(self):
        months = months_range(date(2009, 1, 1), 200)
        theirs = {m: 0.5 + 0.1 * math.sin(i) for i, m in enumerate(months)}
        ours = {m: v + 0.05 for m, v in theirs.items()}
        out = rc.compare_on_windows(ours, theirs, ("test", "normal", "train"))
        self.assertEqual(set(out), {"test", "normal", "train"})
        self.assertEqual(out["test"]["n"], 67)
        self.assertEqual(out["normal"]["n"], 54)
        self.assertAlmostEqual(out["test"]["bias"], 0.05)
        self.assertAlmostEqual(out["test"]["rmse"], 0.05)

    def test_window_without_data_is_skipped_not_error(self):
        months = months_range(date(2019, 1, 1), 40)
        series = {m: 0.5 + 0.01 * i for i, m in enumerate(months)}
        out = rc.compare_on_windows(series, dict(series), ("test", "train"))
        self.assertIn("test", out)
        self.assertNotIn("train", out)


class ImpliedWeightTest(unittest.TestCase):
    def test_recovers_weight_from_identity(self):
        months = months_range(date(2020, 1, 1), 6)
        x = {m: 0.4 for m in months}
        u = {m: (6.0 if i % 2 == 0 else 0.3) for i, m in enumerate(months)}
        pi = {m: 0.92 * x[m] + 0.08 * u[m] for m in months}
        out = rc.implied_weights(pi, x, u, min_gap=1.0)
        self.assertEqual(sorted(out), [m for i, m in enumerate(months) if i % 2 == 0])
        for w in out.values():
            self.assertAlmostEqual(w, 0.08)

    def test_small_denominator_months_are_dropped(self):
        months = months_range(date(2020, 1, 1), 3)
        x = {m: 0.4 for m in months}
        u = {m: 0.5 for m in months}
        pi = {m: 0.41 for m in months}
        self.assertEqual(rc.implied_weights(pi, x, u, min_gap=1.0), {})

    def test_summary(self):
        out = rc.summarize_weights({date(2020, 1, 1): 0.06, date(2020, 7, 1): 0.08, date(2021, 7, 1): 0.10})
        self.assertAlmostEqual(out["median"], 0.08)
        self.assertEqual(out["n"], 3)
        self.assertAlmostEqual(out["min"], 0.06)
        self.assertEqual(rc.summarize_weights({})["n"], 0)


class UtilitiesComparisonTest(unittest.TestCase):
    def test_diff_and_flagged_months(self):
        months = months_range(date(2020, 1, 1), 6)
        theirs = {m: 0.1 for m in months}
        ours = dict(theirs)
        ours[months[2]] = 0.5
        out = rc.compare_utilities(ours, theirs, threshold=0.05)
        self.assertEqual(out["n"], 6)
        self.assertEqual(out["flagged"], [months[2].isoformat()])
        self.assertAlmostEqual(out["max_abs"], 0.4)

    def test_only_common_months(self):
        out = rc.compare_utilities({date(2020, 1, 1): 1.0}, {date(2020, 2, 1): 1.0}, threshold=0.05)
        self.assertEqual(out["n"], 0)


class TargetHistoryConsistencyTest(unittest.TestCase):
    def test_levels_match_target_full_h1(self):
        months = months_range(date(2016, 1, 1), 6)
        levels = {m: 100.0 + 0.3 * i for i, m in enumerate(months)}
        target = pd.DataFrame({1: [0.3 * i for i in range(6)]}, index=pd.Index(months, name="month"))
        self.assertAlmostEqual(rc.history_vs_target(levels, target)["max_abs"], 0.0)

    def test_detects_difference(self):
        months = months_range(date(2016, 1, 1), 6)
        levels = {m: 100.0 + 0.3 * i for i, m in enumerate(months)}
        levels[months[3]] += 0.02
        target = pd.DataFrame({1: [0.3 * i for i in range(6)]}, index=pd.Index(months, name="month"))
        out = rc.history_vs_target(levels, target)
        self.assertAlmostEqual(out["max_abs"], 0.02)
        self.assertEqual(out["n"], 6)


class FrameReadersTest(unittest.TestCase):
    def test_history_from_frame_reads_codes_and_levels(self):
        frame = pd.DataFrame(
            {
                "Код": ["1 - 9470", "1"],
                "Наименование": ["без КУ", "все"],
                pd.Timestamp("2016-01-01"): [100.66, 100.64],
                pd.Timestamp("2016-02-01"): [100.50, 100.49],
            }
        )
        out = rc.history_from_frame(frame)
        self.assertAlmostEqual(out["1 - 9470"][date(2016, 1, 1)], 100.66)
        self.assertAlmostEqual(out["1"][date(2016, 2, 1)], 100.49)

    def test_utilities_from_frame_keeps_facts_only(self):
        frame = pd.DataFrame(
            {
                "дата": [pd.Timestamp("2024-06-01"), pd.Timestamp("2024-07-01"), pd.Timestamp("2024-08-01")],
                "Россия": [0.327374, 6.038317, 0.327374],
                "факт_прогноз": ["факт", "факт", "прогноз"],
                "Россия не SA": [0.0, 9.92, 0.0],
            }
        )
        sa, nsa = rc.utilities_from_frame(frame)
        self.assertEqual(sorted(sa), [date(2024, 6, 1), date(2024, 7, 1)])
        self.assertAlmostEqual(nsa[date(2024, 7, 1)], 9.92)
        self.assertAlmostEqual(sa[date(2024, 7, 1)], 6.038317)


class CalibrationTest(unittest.TestCase):
    def table(self, base_rmse, cal_rmse, cal_w, fit_best_w):
        """Таблица `{w: {'fit': RMSE на train, 'eval': RMSE на test}}` с заданными минимумами."""
        weights = [round(0.05 + 0.01 * k, 2) for k in range(11)]
        table = {}
        for w in weights:
            table[w] = {"fit": 0.2 + abs(w - fit_best_w), "eval": base_rmse}
        table[cal_w]["eval"] = cal_rmse
        return table

    def test_keeps_base_weight_when_improvement_small(self):
        table = self.table(base_rmse=0.10, cal_rmse=0.095, cal_w=0.08, fit_best_w=0.08)
        out = rc.choose_weight(table, base=0.10)
        self.assertEqual(out["chosen"], 0.10)
        self.assertEqual(out["calibrated"], 0.08)
        self.assertFalse(out["changed"])

    def test_changes_weight_when_improvement_ten_percent_and_far(self):
        table = self.table(base_rmse=0.10, cal_rmse=0.085, cal_w=0.08, fit_best_w=0.08)
        out = rc.choose_weight(table, base=0.10)
        self.assertEqual(out["chosen"], 0.08)
        self.assertTrue(out["changed"])

    def test_close_weight_is_not_changed_even_if_better(self):
        table = self.table(base_rmse=0.10, cal_rmse=0.05, cal_w=0.10, fit_best_w=0.10)
        self.assertFalse(rc.choose_weight(table, base=0.10)["changed"])

    def test_calibration_uses_fit_window_not_eval(self):
        table = self.table(base_rmse=0.10, cal_rmse=0.01, cal_w=0.06, fit_best_w=0.12)
        out = rc.choose_weight(table, base=0.10)
        self.assertEqual(out["calibrated"], 0.12)  # по fit-окну, а не по минимальному eval
        self.assertEqual(out["chosen"], 0.10)


class AlignmentDecisionTest(unittest.TestCase):
    def test_aligned(self):
        self.assertTrue(rc.decide_alignment({"rmse": 0.10, "corr": 0.95})["aligned"])
        self.assertTrue(rc.decide_alignment({"rmse": 0.05, "corr": 0.99})["aligned"])

    def test_misaligned_on_rmse_or_corr(self):
        self.assertFalse(rc.decide_alignment({"rmse": 0.11, "corr": 0.99})["aligned"])
        self.assertFalse(rc.decide_alignment({"rmse": 0.05, "corr": 0.94})["aligned"])


if __name__ == "__main__":
    unittest.main()
