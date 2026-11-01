#!/usr/bin/env python3
"""Офлайн-тесты P0.4: live-веса, сверка с архивом и покрытие.

Без ClickHouse и сети: SQL-транспорт подменяется функцией-фикстурой.
Запуск — `make cb-comp-weights-check-test`.
"""

import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "scripts" / "models" / "cpi"))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_weights_check as wc


class Spec:
    def __init__(self, code):
        self.code = code


class LiveWeightsTest(unittest.TestCase):
    def test_reads_view_and_converts_percent(self):
        sql_seen = []

        def query(sql):
            sql_seen.append(sql)
            return [
                {"year": 2024, "code": "10", "source_file": "CPR_Gr-Tov-RF", "weight": 9.072},
                {"year": 2024, "code": "803", "source_file": "CPR_tov_RF", "weight": 0.335},
                {"year": 2026, "code": "10", "source_file": "CPR_Gr-Tov-RF", "weight": 8.821},
                {"year": 2026, "code": "10", "source_file": "CPR_tov_RF", "weight": 8.821},
            ]

        weights = bu.load_live_weights([2024, 2026], [Spec("10"), Spec("803")], query=query)
        self.assertEqual(weights[2024], {"10": 0.09072, "803": 0.00335})
        self.assertEqual(weights[2026], {"10": 0.08821})
        self.assertEqual(len(sql_seen), 1)
        self.assertIn("v_rosstat_cpi_weights", sql_seen[0])
        self.assertIn("IN (2024, 2026)", sql_seen[0])
        self.assertIn("FORMAT JSON", sql_seen[0])

    def test_group_and_item_conflict_is_error(self):
        def query(_sql):
            return [
                {"year": 2024, "code": "803", "source_file": "CPR_Gr-Tov-RF", "weight": 0.5},
                {"year": 2024, "code": "803", "source_file": "CPR_tov_RF", "weight": 0.335},
            ]

        with self.assertRaises(ValueError):
            bu.load_live_weights([2024], [Spec("803")], query=query)

    def test_missing_code_is_absent_not_zero(self):
        def query(_sql):
            return [{"year": 2024, "code": "10", "source_file": "CPR_Gr-Tov-RF", "weight": 9.072}]

        weights = bu.load_live_weights([2024], [Spec("10"), Spec("9260")], query=query)
        self.assertNotIn("9260", weights[2024])

    def test_empty_years_or_codes(self):
        self.assertEqual(bu.load_live_weights([], [Spec("10")], query=lambda _s: []), {})
        self.assertEqual(bu.load_live_weights([2024], [], query=lambda _s: []), {})

    def test_load_weights_dispatch(self):
        calls = {}

        def fake_archive(archive, years, specs):
            calls["archive"] = (archive, tuple(years), len(specs))
            return {2024: {"10": 0.1}}

        def fake_live(years, specs, query=None):
            calls["live"] = (tuple(years), len(specs))
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

    def test_sum_tolerance(self):
        sums = [{"year": 2025, "variant": "", "total": 100.4, "n": 550}]
        self.assertTrue(all(abs(item["total"] - 100.0) <= 0.5 for item in sums))
        sums[0]["total"] = 100.6
        self.assertFalse(all(abs(item["total"] - 100.0) <= 0.5 for item in sums))


if __name__ == "__main__":
    unittest.main(verbosity=2)
