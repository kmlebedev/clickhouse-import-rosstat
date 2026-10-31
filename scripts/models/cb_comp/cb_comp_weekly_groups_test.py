#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_weekly_groups` (Task 4 #424): без ClickHouse и архива."""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_groups_panel as gp
from models.cb_comp import cb_comp_weekly_groups as wg
from models.cpi import cpi_weekly_aggregate as agg


def dictionary() -> "gp.Dictionary":
    return gp.Dictionary(
        names={"10": "Мясопродукты", "1100": "Молоко", "200": "Яйца", "300": "Услуги"},
        levels={"10": 3, "1100": 4, "200": 7, "300": 5, "100": 7, "1101": 7, "999": 7},
        leaves_by_ancestor={"10": ["100", "101"], "1100": ["1101", "1102"], "300": ["301"]},
    )


def weights() -> list:
    return [
        {"name": "Мясо", "code": "100", "weight": 0.10},
        {"name": "Молоко", "code": "1101", "weight": 0.05},
        {"name": "Яйца", "code": "200", "weight": 0.02},
        {"name": "Вне групп", "code": "999", "weight": 0.03},
    ]


class ConstantsTest(unittest.TestCase):
    def test_protocol_constants(self):
        self.assertEqual(wg.WEEKLY_LAG_DAYS, 2)
        self.assertEqual(wg.WEIGHT_YEARS, tuple(range(2016, 2025)))
        self.assertEqual(wg.DICTIONARY_YEAR, 2024)


class YearTest(unittest.TestCase):
    def test_weight_year(self):
        self.assertEqual(wg.weight_year_for(date(2014, 5, 1)), 2016)
        self.assertEqual(wg.weight_year_for(date(2020, 5, 1)), 2020)
        self.assertEqual(wg.weight_year_for(date(2026, 9, 1)), 2024)

    def test_year_table(self):
        table = {2016: {"a": 1}, 2020: {"a": 2}}
        self.assertEqual(wg.year_table(table, 2019), table[2016])
        self.assertEqual(wg.year_table(table, 2021), table[2020])
        self.assertEqual(wg.year_table(table, 2014), table[2016])

    def test_month_window(self):
        self.assertEqual(wg.month_window(date(2024, 2, 1)), (date(2024, 2, 1), date(2024, 2, 29)))


class GroupMapTest(unittest.TestCase):
    def test_deepest_group_for_mixed_levels(self):
        leaf_group = wg.group_leaf_map(dictionary(), ["10", "1100", "200", "300"])
        self.assertEqual(leaf_group["100"], "10")
        self.assertEqual(leaf_group["1101"], "1100")
        self.assertEqual(leaf_group["200"], "200")  # группа уровня 7 — сама лист
        self.assertEqual(leaf_group["301"], "300")
        self.assertNotIn("999", leaf_group)

    def test_nested_groups_rejected(self):
        nested = dictionary()
        nested.leaves_by_ancestor["10"] = ["100", "1101"]
        with self.assertRaises(ValueError):
            wg.group_leaf_map(nested, ["10", "1100"])

    def test_unknown_group(self):
        with self.assertRaises(ValueError):
            wg.group_leaf_map(dictionary(), ["42"])


class MappingTest(unittest.TestCase):
    def setUp(self):
        self.rows, self.unmatched = wg.map_weekly_series(
            ["Мясо", "Молоко", "Яйца", "Вне групп", "Нет совпадения"],
            weights(),
            dictionary(),
            ["10", "1100", "200", "300"],
        )

    def test_assignment(self):
        by_series = {row["series"]: row for row in self.rows}
        self.assertEqual(by_series["Мясо"]["group_code"], "10")
        self.assertEqual(by_series["Молоко"]["group_code"], "1100")
        self.assertEqual(by_series["Яйца"]["group_code"], "200")
        self.assertEqual(by_series["Вне групп"]["group_code"], "")
        self.assertTrue(by_series["Вне групп"]["matched"])
        self.assertFalse(by_series["Нет совпадения"]["matched"])
        self.assertEqual(self.unmatched, ["Нет совпадения"])

    def test_summary(self):
        summary = wg.mapping_summary(self.rows, ["10", "1100", "200", "300"])
        self.assertEqual(summary["n_series"], 5)
        self.assertEqual(summary["n_matched"], 4)
        self.assertEqual(summary["n_in_group"], 3)
        self.assertEqual(summary["n_groups"], 3)
        self.assertEqual(summary["groups_without_weekly"], ["300"])
        self.assertEqual(summary["outside_groups"], ["Вне групп"])
        self.assertEqual(summary["unmatched"], ["Нет совпадения"])

    def test_group_coverage(self):
        rows = wg.group_coverage(
            self.rows,
            ["10", "1100", "200", "300"],
            {"10": 0.2, "1100": 0.1, "200": 0.02, "300": 0.5},
            {"100": 0.10, "1101": 0.05, "200": 0.02},
        )
        by_code = {row["group_code"]: row for row in rows}
        self.assertAlmostEqual(by_code["10"]["coverage"], 0.5)
        self.assertAlmostEqual(by_code["1100"]["coverage"], 0.5)
        self.assertAlmostEqual(by_code["200"]["coverage"], 1.0)
        self.assertEqual(by_code["300"]["coverage"], 0.0)


class FeatureTest(unittest.TestCase):
    def setUp(self):
        # Два ряда группы «10»: A (вес 3) и B (вес 1).
        self.points = {
            "A": [(date(2024, 1, 8), 101.0), (date(2024, 1, 15), 102.0), (date(2024, 1, 22), 101.0), (date(2024, 1, 29), 103.0)],
            "B": [(date(2024, 1, 15), 100.5), (date(2024, 1, 29), 101.0)],
        }
        self.periods = wg.periods_by_series(self.points)
        self.by_group = {"10": [{"series": "A", "weight_code": "100"}, {"series": "B", "weight_code": "101"}]}
        self.leaf_weights = {"100": 3.0, "101": 1.0}
        self.group_weights = {"10": 4.0}
        self.month = date(2024, 1, 1)
        self.cutoff = date(2024, 2, 8)

    def expected_log(self, series: str) -> tuple:
        return agg._log_and_days(self.periods[series], date(2024, 1, 1), date(2024, 1, 31))

    def test_weighted_geometric(self):
        out = wg.weekly_group_features(
            self.periods, self.by_group, self.leaf_weights, self.group_weights, self.month, self.cutoff, ["10"]
        )["10"]
        log_a, _ = self.expected_log("A")
        log_b, _ = self.expected_log("B")
        expected = (math.exp((3.0 * log_a + 1.0 * log_b) / 4.0) - 1.0) * 100.0
        self.assertAlmostEqual(out["wk_g_mtd"], expected, places=10)
        self.assertAlmostEqual(out["wk_g_cov"], 1.0)
        self.assertEqual(out["n_series"], 2)

    def test_renormalization_when_series_missing(self):
        del self.periods["B"]
        out = wg.weekly_group_features(
            self.periods, self.by_group, self.leaf_weights, self.group_weights, self.month, self.cutoff, ["10"]
        )["10"]
        log_a, _ = self.expected_log("A")
        self.assertAlmostEqual(out["wk_g_mtd"], (math.exp(log_a) - 1.0) * 100.0, places=10)
        self.assertAlmostEqual(out["wk_g_cov"], 0.75)
        self.assertEqual(out["n_series"], 1)

    def test_no_points_nan_and_zero_coverage(self):
        out = wg.weekly_group_features(
            {}, self.by_group, self.leaf_weights, self.group_weights, self.month, self.cutoff, ["10"]
        )["10"]
        self.assertTrue(math.isnan(out["wk_g_mtd"]))
        self.assertEqual(out["wk_g_cov"], 0.0)
        self.assertEqual(out["n_series"], 0)

    def test_future_point_excluded(self):
        base = wg.weekly_group_features(
            self.periods, self.by_group, self.leaf_weights, self.group_weights, self.month, self.cutoff, ["10"]
        )["10"]
        spoiled_points = dict(self.points)
        spoiled_points["A"] = self.points["A"] + [(date(2024, 2, 7), 200.0)]  # date+2 > cutoff
        spoiled = wg.periods_by_series(spoiled_points)
        got = wg.weekly_group_features(
            spoiled, self.by_group, self.leaf_weights, self.group_weights, self.month, self.cutoff, ["10"]
        )["10"]
        self.assertAlmostEqual(got["wk_g_mtd"], base["wk_g_mtd"], places=10)

    def test_month_end_point_used(self):
        points = {"A": [(date(2024, 1, 24), 100.0), (date(2024, 1, 31), 101.0)]}
        periods = wg.periods_by_series(points)
        out = wg.weekly_group_features(
            periods, {"10": [{"series": "A", "weight_code": "100"}]}, {"100": 1.0}, {"10": 1.0},
            self.month, self.cutoff, ["10"],
        )["10"]
        self.assertAlmostEqual(out["wk_g_mtd"], 1.0, places=10)
        self.assertEqual(wg.last_used_point(periods["A"], self.month, self.cutoff), date(2024, 1, 31))

    def test_aggregate_wrapper(self):
        points = [(date(2024, 1, 8), 101.0), (date(2024, 1, 15), 102.0), (date(2024, 1, 22), 101.0), (date(2024, 1, 29), 103.0)]
        got = wg.aggregate_features(points, self.month, self.cutoff)
        expected = agg.wkagg_features(points, self.month, self.cutoff)
        self.assertEqual(got, expected)
        self.assertAlmostEqual(got["wkagg_cov"], 28 / 31)  # 1-е января вне первого периода
        self.assertGreater(got["wkagg_mtd"], 0.0)


if __name__ == "__main__":
    unittest.main()
