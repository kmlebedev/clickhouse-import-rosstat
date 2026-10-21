#!/usr/bin/env python3
"""Offline tests for scripts/ppi_sa_mapping.py.

Run:
    python3 scripts/ppi_sa_mapping_test.py -v
"""

import csv
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ppi_sa_mapping as mapping


class NormalizeTest(unittest.TestCase):
    def test_parentheses_and_punctuation(self):
        self.assertEqual(mapping.normalize_name("Молоко «НД» (2,5%)"), "молоко 2 5")

    def test_yo_and_spaces(self):
        self.assertEqual(mapping.normalize_name("  Ёлка   пушистая "), "елка пушистая")


class RulePriorityTest(unittest.TestCase):
    def setUp(self):
        self.rules = [
            mapping.Rule(1000, "code_exact", "10.11.1", "01.1.2", "mapped", "exact"),
            mapping.Rule(600, "name_regex", "мясо", "01.1.2", "mapped", "regex"),
            mapping.Rule(100, "code_prefix", "10.11", "01.1.9", "mapped", "prefix"),
            mapping.Rule(100, "code_prefix", "10", "01.1.1", "mapped", "prefix"),
        ]

    def test_exact_wins(self):
        rule = mapping.classify("10.11.1", "Рыба", self.rules)
        self.assertEqual(rule.kipc_group, "01.1.2")
        self.assertEqual(rule.match_type, "code_exact")

    def test_regex_wins_over_prefix(self):
        rule = mapping.classify("10.11.2", "Мясо охлажденное", self.rules)
        self.assertEqual(rule.match_type, "name_regex")

    def test_longest_prefix(self):
        rule = mapping.classify("10.11.2", "Рыба", self.rules)
        self.assertEqual(rule.kipc_group, "01.1.9")


class HierarchyTest(unittest.TestCase):
    def test_leaf_codes(self):
        codes = {"10", "10.1", "10.11", "10.11.1", "25.6", "25.61", "25.62"}
        self.assertEqual(mapping._leaf_codes(codes), {"10.11.1", "25.61", "25.62"})

    def test_parent_digit_boundary(self):
        self.assertTrue(mapping._is_child("25.61", "25.6"))
        self.assertTrue(mapping._is_child("10.11", "10.1"))
        self.assertTrue(mapping._is_child("25.6.1", "25.6"))
        self.assertFalse(mapping._is_child("26.6", "25.6"))

    def test_kipc_dot_boundary(self):
        self.assertTrue(mapping._is_kipc_child("04.3.1.1.10.1", "04.3.1.1.10"))
        self.assertFalse(mapping._is_kipc_child("04.3.1.1.10", "04.3.1.1.1"))
        self.assertFalse(mapping._is_kipc_child("03.1.2.2.11", "03.1.2.2.1"))
        self.assertFalse(mapping._is_kipc_child("06.1.1.1.0.11", "06.1.1.1.0.1"))

    def test_narrow_only_mapped(self):
        rows = [
            {"is_leaf": "1", "status": "mapped", "okpd2": "13.10.1"},
            {"is_leaf": "1", "status": "excluded", "okpd2": "13.10.2"},
            {"is_leaf": "1", "status": "out_of_scope", "okpd2": "17.24.11.110"},
            {"is_leaf": "1", "status": "mapped", "okpd2": "16.10.1"},
            {"is_leaf": "0", "status": "mapped", "okpd2": "13.10"},
        ]
        self.assertEqual(mapping._narrow_leaves(rows), ["13.10.1"])

    def test_nonfood_scope_includes_construction(self):
        self.assertIn("04.3.1.1", mapping.SCOPE_GROUPS["nonfood"])

    def test_construction_rules(self):
        rules = mapping.load_rules(mapping.RULES_FILE)
        cases = {
            "17.24.11.110": "04.3.1.1.2",
            "23.51.12.111": "04.3.1.1.7",
            "23.31.10.121": "04.3.1.1.10.1",
            "22.23.15": "04.3.1.1.9",
            "16.21.14": "04.3.1.1.5",
            "23.62.1": "04.3.1.1.10.2",
            "23.61.11.141": "04.3.1.1.10.3",
            "25.99.11.110": "04.3.1.1.4",
        }
        for code, group in cases.items():
            rule = mapping.classify(code, "", rules)
            self.assertIsNotNone(rule, code)
            self.assertEqual(rule.kipc_group, group, code)
            self.assertEqual(rule.status, "mapped", code)

    def test_construction_coverage(self):
        weights = {"04.3.1.1.10.1": {"name": "Плитка", "weight": 0.133}}
        rows = [
            {
                "is_leaf": "1",
                "status": "mapped",
                "okpd2": "23.31.10.121",
                "kipc_group": "04.3.1.1.10.1",
                "name": "Плитка",
            }
        ]
        coverage = mapping.build_coverage(rows, weights)
        self.assertEqual(coverage["nonfood"]["weight_in_scope"], 0.133)
        self.assertEqual(coverage["nonfood"]["weight_mapped"], 0.133)
        self.assertEqual(coverage["nonfood"]["coverage"], 1.0)

    def test_taxonomy_version_v2(self):
        self.assertEqual(mapping.TAXONOMY_VERSION, "okpd2-kipc-2026-v2")

    def test_group_weight_fallback(self):
        weights = {"01.1.1.1": {"weight": 1.0}, "01.1.1.2": {"weight": 2.0}}
        self.assertEqual(mapping._group_weight(weights, "01.1.1"), 3.0)

    def test_group_weight_subtract(self):
        weights = {
            "03.1": {"weight": 5.0},
            "03.1.2": {"weight": 4.0},
            "03.1.4": {"weight": 1.0},
        }
        self.assertEqual(mapping._net_group_weight(weights, "03.1"), 4.0)


class KipcParserTest(unittest.TestCase):
    def _workbook(self) -> Path:
        wb = Workbook()
        ws = wb.active
        ws.title = "2026"
        ws.append(["К содержанию"])
        ws.append(["СТРУКТУРА"])
        ws.append(["в процентах к итогу"])
        ws.append(["Код товара в группировке КИПЦ", "Наименование", "СПР"])
        ws.append(["01", "ПРОДУКТЫ ПИТАНИЯ", 31.0])
        ws.append(["01.1", "Продукты питания", 30.0])
        ws.append(["01.1.1", "Зерновые", 5.0])
        ws.append([None, "ВСЕ ТОВАРЫ", 100.0])
        path = Path(tempfile.mkdtemp()) / "kipc.xlsx"
        wb.save(path)
        return path

    def test_single_year(self):
        weights = mapping.parse_kipc_single(self._workbook(), "2026")
        self.assertEqual(weights["01"]["weight"], 31.0)
        self.assertEqual(weights["01.1.1"]["name"], "Зерновые")
        self.assertNotIn("", weights)


class MappingAndCoverageTest(unittest.TestCase):
    def setUp(self):
        self.weights = {
            "01.1.1": {"name": "Зерновые", "weight": 5.0},
            "01.1.2": {"name": "Мясо", "weight": 8.0},
            "03.1": {"name": "Одежда", "weight": 5.0},
            "03.1.4": {"name": "Химчистка", "weight": 1.0},
        }
        self.rules = [
            mapping.Rule(100, "code_prefix", "10.61", "01.1.1", "mapped", ""),
            mapping.Rule(100, "code_prefix", "10.11", "01.1.2", "mapped", ""),
            mapping.Rule(100, "code_prefix", "13", "03.1", "mapped", ""),
            mapping.Rule(100, "code_prefix", "24", "", "excluded", ""),
        ]

    def test_build_mapping_statuses(self):
        products = [
            ("10.61.1", "Рис"),
            ("10.61", "Крупы"),
            ("13.10", "Ткани"),
            ("24.10", "Чугун"),
        ]
        rows = mapping.build_mapping(products, self.weights, self.rules)
        by_code = {row["okpd2"]: row for row in rows}
        self.assertEqual(by_code["10.61.1"]["status"], "mapped")
        self.assertEqual(by_code["10.61"]["status"], "aggregate")
        self.assertEqual(by_code["24.10"]["status"], "excluded")
        coverage = mapping.build_coverage(rows, self.weights)
        self.assertEqual(coverage["leaves_total"], 3)
        self.assertEqual(coverage["by_status"]["mapped"], 2)
        self.assertEqual(coverage["by_status"]["excluded"], 1)

    def test_mapped_weight_dedupe(self):
        weights = {
            "01.1.2": {"weight": 8.0},
            "01.1.2.1": {"weight": 5.0},
        }
        self.assertEqual(
            mapping._mapped_weight_in_group(weights, {"01.1.2", "01.1.2.1"}, "01.1.2"),
            8.0,
        )


class SeriesTest(unittest.TestCase):
    def test_mean_by_date(self):
        points = {
            "a": {date(2020, 1, 1): 100.0, date(2020, 2, 1): 101.0},
            "b": {date(2020, 1, 1): 102.0, date(2020, 2, 1): 103.0},
        }
        result = mapping._mean_by_date(["a", "b"], points)
        self.assertEqual(result[0], (date(2020, 1, 1), 101.0))
        self.assertEqual(result[1], (date(2020, 2, 1), 102.0))

    def test_group_series_reweights_available_groups(self):
        weights = {
            "01.1.1": {"weight": 5.0},
            "01.1.2": {"weight": 3.0},
        }
        points = {
            "a": {date(2020, 1, 1): 100.0},
            "b": {date(2020, 1, 1): 200.0},
        }
        members = {"01.1.1": ["a"], "01.1.2": ["b"], "01.1.3": ["missing"]}
        result = mapping._group_series(members, weights, points)
        self.assertEqual(result[0][1], (5 * 100.0 + 3 * 200.0) / 8)

    def test_members_for_groups_includes_descendants(self):
        members = {"01.2.1": ["a"], "01.2.2": ["b"], "01.1.2": ["c"]}
        self.assertEqual(
            mapping._members_for_groups(members, ["01.2"]),
            {"01.2.1": ["a"], "01.2.2": ["b"]},
        )

    def test_mapped_leaves_include_descendants(self):
        rows = [
            {"is_leaf": "1", "status": "mapped", "kipc_group": "01.2.1", "okpd2": "a"},
            {"is_leaf": "1", "status": "mapped", "kipc_group": "01.1.2", "okpd2": "b"},
        ]
        self.assertEqual(mapping._mapped_leaves(rows, {"01.2"}), ["a"])


class ParityTest(unittest.TestCase):
    def test_mean3_and_level3(self):
        temp = Path(tempfile.mkdtemp())
        sa_path = temp / "sa.csv"
        anchors_path = temp / "anchors.csv"
        with sa_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=["series", "variant", "date", "sa", "mm_sa", "saar", "saar3"]
            )
            writer.writeheader()
            writer.writerow(
                {"series": "equal/food", "variant": "seats_log_cal", "date": "2026-01-01", "sa": "100", "saar3": "1.0"}
            )
            writer.writerow(
                {"series": "equal/food", "variant": "seats_log_cal", "date": "2026-04-01", "sa": "101", "saar3": "2.0"}
            )
        with anchors_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=["issue", "target", "series", "value"])
            writer.writeheader()
            writer.writerow({"issue": "2026-5", "target": "2026-04-01", "series": "food", "value": "1.0"})
        results, summary = mapping.compute_parity(sa_path, anchors_path)
        rows = [row for row in results if row["scheme"] == "equal" and row["formula"] == "mean3_saar"]
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["ours"], 2.0)
        self.assertEqual(rows[0]["diff"], 1.0)
        self.assertEqual(summary["equal/mean3_saar"]["mae"], 1.0)
        level = [
            row
            for row in results
            if row["scheme"] == "equal" and row["formula"] == "level3_annualized"
        ]
        self.assertEqual(len(level), 1)
        self.assertAlmostEqual(level[0]["ours"], ((101 / 100) ** 4 - 1) * 100)


class SeedTest(unittest.TestCase):
    def test_rules_file(self):
        weights = mapping.load_weights()
        rules = mapping.load_rules(mapping.RULES_FILE)
        self.assertGreater(len(rules), 50)
        exact = [rule.pattern for rule in rules if rule.match_type == "code_exact"]
        self.assertEqual(len(exact), len(set(exact)))
        allowed = {"mapped", "excluded", "out_of_scope"}
        for rule in rules:
            self.assertIn(rule.status, allowed)
            if rule.status == "mapped":
                self.assertIn(rule.kipc_group, weights)
            if rule.match_type == "code_prefix":
                self.assertTrue(rule.pattern)
            if rule.match_type == "name_regex":
                self.assertTrue(rule.pattern)


if __name__ == "__main__":
    unittest.main()
