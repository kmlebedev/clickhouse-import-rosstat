#!/usr/bin/env python3
"""Офлайн-тесты P0.4: live-веса, сверка с архивом и покрытие.

Без ClickHouse и сети: SQL-транспорт подменяется функцией-фикстурой.
Запуск — `make cb-comp-weights-check-test`.
"""

import sys
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_weights_check as wc


class Spec:
    def __init__(self, code):
        self.code = code


class LiveWeightsTest(unittest.TestCase):
    def test_live_items_converts_percent(self):
        rows = [
            {"year": 2024, "code": "10", "weight": 9.072},
            {"year": 2024, "code": "803", "weight": 0.335},
            {"year": 2026, "code": "10", "weight": 8.821},
        ]
        items = bu._live_items(rows)
        self.assertEqual(items[2024], {"10": 0.09072, "803": 0.00335})
        self.assertEqual(items[2026], {"10": 0.08821})

    def test_live_query_reads_item_file_only(self):
        sql = bu.live_weights_query([2024, 2026])
        self.assertIn("v_rosstat_cpi_weights", sql)
        self.assertIn("source_file = 'CPR_tov_RF'", sql)
        self.assertIn("IN (2024, 2026)", sql)
        self.assertIn("NOT startsWith(code, 'name:')", sql)
        self.assertIn("FORMAT JSON", sql)

    def test_missing_code_is_absent_not_zero(self):
        items = bu._live_items([{"year": 2024, "code": "10", "weight": 9.072}])
        self.assertNotIn("9260", items[2024])

    def test_aggregate_year_weights_matches_archive_formula(self):
        dictionary = pd.DataFrame(
            [
                {"code": "1", "уровень": 1, "уровень_1": "1", "уровень_2": "", "уровень_3": "", "уровень_4": "", "уровень_5": "", "уровень_6": ""},
                {"code": "10", "уровень": 3, "уровень_1": "1", "уровень_2": "1", "уровень_3": "10", "уровень_4": "", "уровень_5": "", "уровень_6": ""},
                {"code": "100", "уровень": 7, "уровень_1": "1", "уровень_2": "1", "уровень_3": "10", "уровень_4": "100", "уровень_5": "", "уровень_6": ""},
                {"code": "110", "уровень": 7, "уровень_1": "1", "уровень_2": "1", "уровень_3": "10", "уровень_4": "100", "уровень_5": "", "уровень_6": ""},
            ]
        )
        weights = pd.DataFrame({"code": ["100", "110"], "weight": [0.03, 0.02]})
        result = bu._aggregate_year_weights(weights, dictionary, [Spec("10")])
        self.assertAlmostEqual(result["10"], 0.05)

    def test_load_weights_dispatch(self):
        calls = {}

        def fake_archive(archive, years, specs):
            calls["archive"] = (archive, tuple(years), len(specs))
            return {2024: {"10": 0.1}}

        def fake_live(archive, years, specs, query=None):
            calls["live"] = (archive, tuple(years), len(specs))
            return {2024: {"10": 0.1}}

        original_archive, original_live = bu.load_archive_weights, bu.load_live_weights
        bu.load_archive_weights, bu.load_live_weights = fake_archive, fake_live
        try:
            self.assertEqual(bu.load_weights(Path("/tmp/a"), [2024], [Spec("10")], source="archive"),
                             {2024: {"10": 0.1}})
            self.assertEqual(bu.load_weights(Path("/tmp/a"), [2024], [Spec("10")], source="live"),
                             {2024: {"10": 0.1}})
        finally:
            bu.load_archive_weights, bu.load_live_weights = original_archive, original_live
        self.assertIn("archive", calls)
        self.assertIn("live", calls)


class CompareTest(unittest.TestCase):
    def test_equal_weights(self):
        live = {2024: {"10": 0.09072, "9470": 0.05944}}
        archive = {2024: {"10": 0.09072, "9470": 0.05944}}
        comparison = wc.compare_weights(live, archive)
        self.assertEqual(comparison["years"]["2024"]["max_abs_diff"], 0.0)
        ok, problems = wc.compare_gate(comparison, years=(2024,))
        self.assertTrue(ok, problems)

    def test_missing_live_code_fails_gate(self):
        live = {2024: {"10": 0.09072}}
        archive = {2024: {"10": 0.09072, "9260": 0.00565}}
        comparison = wc.compare_weights(live, archive)
        ok, problems = wc.compare_gate(comparison, years=(2024,))
        self.assertFalse(ok)
        self.assertTrue(any("9260" in problem for problem in problems))

    def test_extra_live_code_is_reported_but_not_failure(self):
        live = {2024: {"10": 0.09072, "803": 0.00335}}
        archive = {2024: {"10": 0.09072}}
        comparison = wc.compare_weights(live, archive)
        self.assertEqual(comparison["years"]["2024"]["extra_live"], ["803"])
        ok, problems = wc.compare_gate(comparison, years=(2024,))
        self.assertTrue(ok, problems)

    def test_missing_year_fails(self):
        comparison = wc.compare_weights({2024: {"10": 0.1}}, {2024: {"10": 0.1}})
        ok, problems = wc.compare_gate(comparison, years=(2023, 2024))
        self.assertFalse(ok)
        self.assertTrue(any("2023" in problem for problem in problems))


class CoverageTest(unittest.TestCase):
    def test_coverage_counts_weight_and_groups(self):
        weights = {2024: {"10": 0.09, "20": 0.05, "9470": 0.06}, 2025: {"10": 0.09, "20": 0.05, "9470": 0.06}}
        sa = {"10": {date(2025, 1, 1): 0.1}, "9470": {date(2025, 1, 1): 0.2}}
        report = wc.coverage_report(weights, sa, ["10", "20", "9470"], [date(2025, 1, 1)])
        self.assertEqual(report[0]["groups"], 2)
        self.assertAlmostEqual(report[0]["weight"], 0.15)
        self.assertAlmostEqual(report[0]["basket"], 0.20)
        self.assertAlmostEqual(report[0]["share"], 0.75)

    def test_special_codes_report(self):
        weights = {2024: {"803": 0.00335, "930.АГ": 0.02648, "9260": 0.00565}}
        sa = {"803": {date(2025, 1, 1): 0.1}, "930.АГ": {date(2025, 1, 1): 0.1}}
        report = wc.special_codes_report(weights, sa, [date(2025, 1, 1)])
        self.assertEqual(report["803"]["months_with_sa"], 1)
        self.assertEqual(report["9260"]["months_with_sa"], 0)
        self.assertEqual(report["9260"]["weight_by_year"]["2024"], 0.00565)


class SumsTest(unittest.TestCase):
    def test_live_sums_reads_view(self):
        def query(sql):
            self.assertIn("CPR_tov_RF", sql)
            return [
                {"year": 2025, "variant": "", "total": 100.0, "n": 550},
                {"year": 2026, "variant": "", "total": 100.0, "n": 552},
            ]

        sums = wc.live_sums(query=query)
        self.assertEqual([item["n"] for item in sums], [550, 552])

    def test_pair_sums_adds_combined_group(self):
        items = [
            {"year": 2024, "variant": "", "total": 99.141, "n": 559},
            {"year": 2025, "variant": "", "total": 99.157, "n": 549},
        ]
        combined = {(2024, ""): 0.859, (2025, ""): 0.843}
        pairs = wc.pair_sums(items, combined)
        self.assertAlmostEqual(pairs[0]["pair_total"], 100.0)
        self.assertAlmostEqual(pairs[1]["pair_total"], 100.0)
        self.assertTrue(all(abs(item["pair_total"] - 100.0) <= 0.5 for item in pairs))

    def test_pair_sums_fails_without_combined(self):
        items = [{"year": 2025, "variant": "", "total": 99.157, "n": 549}]
        pairs = wc.pair_sums(items, {})
        self.assertFalse(all(abs(item["pair_total"] - 100.0) <= 0.5 for item in pairs))

    def test_combined_group_sums_reads_group_file(self):
        def query(sql):
            self.assertIn("CPR_Gr-Tov-RF", sql)
            self.assertIn("positionCaseInsensitive", sql)
            return [{"year": 2025, "variant": "", "total": 0.843}]

        combined = wc.combined_group_sums(query=query)
        self.assertEqual(combined[(2025, "")], 0.843)


if __name__ == "__main__":
    unittest.main(verbosity=2)
