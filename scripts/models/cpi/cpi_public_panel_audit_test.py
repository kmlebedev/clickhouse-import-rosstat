#!/usr/bin/env python3
"""Офлайн-тесты `scripts/cpi_public_panel_audit.py` (issue #234).

Запуск:

    make cpi-public-audit-test

Тесты не требуют ClickHouse, сети и реального архива CB_COMP: веса/словарь и
`mp_survey_data.xlsx` собираются во временном каталоге openpyxl.
"""

import json
import sys
import tempfile
import unittest
from datetime import date, datetime
from pathlib import Path

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_public_panel_audit as audit


def _month_start(year: int, month: int) -> date:
    return date(year, month, 1)


def _shift_month(value: date, months: int) -> date:
    index = value.year * 12 + (value.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def _make_mp_workbook(path: Path, months: int = 297) -> None:
    workbook = Workbook()
    workbook.remove(workbook.active)
    for sheet_name in audit.INDUSTRY_SHEETS:
        sheet = workbook.create_sheet(sheet_name)
        sheet.cell(row=1, column=1, value="Данные мониторинга предприятий")
        sheet.cell(row=3, column=3, value="Исходные данные")
        sheet.cell(row=3, column=16, value="Сезонно скорректированные данные")
        headers = {
            1: "Отчетный период",
            2: "Месяц заполнения и сбора анкет",
            3: "Индикатор бизнес-климата Банка России",
            4: "Индикатор бизнес-климата Банка России (факт)",
            5: "Индикатор бизнес-климата Банка России (ожид)",
            10: "Ценовые ожидания предприятий на следующие 3 месяца",
            12: "Как изменились издержки производства, обращения? баланс ответов",
            16: "Индикатор бизнес-климата Банка России",
            23: "Ценовые ожидания предприятий на следующие 3 месяца",
            25: "Как изменились издержки производства, обращения? баланс ответов",
        }
        for column, value in headers.items():
            sheet.cell(row=4, column=column, value=value)
        cursor = _month_start(2002, 1)
        for index in range(months):
            row = index + 5
            sheet.cell(row=row, column=1, value=datetime(cursor.year, cursor.month, 1))
            for column in (3, 10, 12, 16, 23, 25):
                sheet.cell(row=row, column=column, value=50.0 + index * 0.1)
            cursor = _shift_month(cursor, 1)
    workbook.create_sheet("Экономика всего")
    workbook.create_sheet("Спецификации")
    workbook.save(path)


class TestNormalization(unittest.TestCase):
    def test_extended_removes_units_and_packages(self):
        self.assertEqual(
            audit.normalize_name("Масло подсолнечное, л"),
            audit.normalize_name("Масло подсолнечное, кг"),
        )
        self.assertEqual(
            audit.normalize_name("Сыры твердые, полутвердые и мягкие, кг"),
            "сыры твердые полутвердые и мягкие",
        )

    def test_basic_keeps_units_but_normalizes_punctuation(self):
        self.assertEqual(
            audit.normalize_name("Водка крепостью 40% об.спирта и выше, л", "basic"),
            audit.normalize_name("Водка крепостью 40% об. спирта и выше, л", "basic"),
        )
        self.assertEqual(
            audit.normalize_name("Свёкла столовая, кг", "basic"),
            audit.normalize_name("Свекла столовая, кг", "basic"),
        )

    def test_raw_is_exact(self):
        self.assertNotEqual(
            audit.normalize_name("Масло подсолнечное, л", "raw"),
            audit.normalize_name("Масло подсолнечное, кг", "raw"),
        )

    def test_code_normalization(self):
        self.assertEqual(audit.normalize_code("803.0"), "803")
        self.assertEqual(audit.normalize_code("930.АГ"), "930.АГ")


class TestMappingAndCoverage(unittest.TestCase):
    def _inputs(self):
        weights = [
            {"code": "101", "name": "Масло подсолнечное, кг", "weight": 0.4},
            {"code": "102", "name": "Хлеб, кг", "weight": 0.3},
            {"code": "1", "name": "Все товары и услуги", "weight": 1.0},
        ]
        dictionary = {
            "101": {
                "code": "101",
                "name": "Масло подсолнечное, кг",
                "level": 7,
                "l2": "6",
                "l3": "22",
                "group_name": "Масло и жиры",
                "component_key": "food",
            },
            "102": {
                "code": "102",
                "name": "Хлеб, кг",
                "level": 7,
                "l2": "6",
                "l3": "2200",
                "group_name": "Хлеб и хлебобулочные изделия",
                "component_key": "food",
            },
            "1": {
                "code": "1",
                "name": "Все товары и услуги",
                "level": 1,
                "l2": "",
                "l3": "",
                "group_name": "",
                "component_key": "other",
            },
        }
        products = ["Масло подсолнечное, л", "Хлеб, кг", "Нет в весах"]
        return products, weights, dictionary

    def test_match_and_coverage(self):
        products, weights, dictionary = self._inputs()
        mapping = audit.build_mapping(Path("."), products, weights, dictionary)
        self.assertEqual(mapping["raw_exact_series"], 1)
        self.assertEqual(mapping["basic_normalized_series"], 1)
        self.assertEqual(mapping["extended_normalized_series"], 2)
        self.assertEqual(mapping["matched_weight_codes"], 2)
        self.assertEqual(len(mapping["unmatched"]), 1)
        self.assertAlmostEqual(mapping["coverage_2024"]["coverage"], 1.0)
        self.assertAlmostEqual(
            mapping["coverage_by_component_2024"]["food"]["coverage"], 1.0
        )
        self.assertEqual(
            mapping["coverage_by_component_2024"]["services"]["denominator"], 0.0
        )

    def test_decision_gate(self):
        hybrid = audit.decision(0.499, {"services": {"coverage": 0.29}})
        self.assertFalse(hybrid["bottomup_full"])
        self.assertEqual(hybrid["mode"], "hybrid_4_components_plus_features")
        full = audit.decision(0.5, {"services": {"coverage": 0.29}})
        self.assertTrue(full["bottomup_full"])
        self.assertEqual(full["mode"], "bottomup_full")


class TestPit(unittest.TestCase):
    def test_week_after_cutoff_is_excluded(self):
        weekly = {
            "a": [
                (date(2024, 4, 1), 100.5),
                (date(2024, 4, 8), 100.2),
            ]
        }
        monthly = {"cpi_all": [(date(2024, 3, 31), 100.4)]}
        result = audit.audit_pit(weekly, monthly, [date(2024, 3, 1)])
        self.assertEqual(result["n_months"], 1)
        self.assertEqual(result["n_excluded_weekly_points"], 1)
        self.assertEqual(result["n_violations"], 0)
        self.assertEqual(result["n_missing_targets"], 0)

    def test_monthly_label_after_cutoff_not_used(self):
        weekly = {"a": [(date(2024, 4, 1), 100.5)]}
        monthly = {"cpi_all": []}
        result = audit.audit_pit(weekly, monthly, [date(2024, 3, 1)])
        self.assertEqual(result["n_violations"], 0)
        self.assertEqual(result["n_missing_targets"], 1)


class TestCompounding(unittest.TestCase):
    def test_compound_month(self):
        points = [
            (date(2024, 1, 1), 100.0),
            (date(2024, 1, 8), 101.0),
            (date(2024, 1, 15), 99.0),
        ]
        self.assertAlmostEqual(audit.compound_month(points), 1.01 * 0.99)

    def test_aggregate_stats(self):
        weekly = {
            audit.WEEKLY_AGGREGATE: [
                (date(2024, 1, 10), 100.0),
                (date(2024, 1, 20), 101.0),
            ]
        }
        monthly = {"cpi_all": [(date(2024, 1, 31), 101.0)]}
        result = audit.audit_compounding(weekly, monthly, {"matched_rows": []})
        self.assertEqual(result["aggregate"]["n_months"], 1)
        self.assertAlmostEqual(result["aggregate"]["mae_pp"], 0.0)
        self.assertEqual(result["product_panel_2022_plus"]["n_months"], 0)

    def test_stats(self):
        stats = audit._stats([1.0, -1.0])
        self.assertEqual(stats["n"], 2)
        self.assertAlmostEqual(stats["mae_pp"], 1.0)
        self.assertAlmostEqual(stats["rmse_pp"], 1.0)
        self.assertAlmostEqual(stats["max_abs_pp"], 1.0)


class TestMpSurvey(unittest.TestCase):
    def test_sixteen_industry_sheets_full_months(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "mp_survey_data.xlsx"
            _make_mp_workbook(path)
            result = audit.audit_mp_survey(path)
        self.assertEqual(result["n_industry_sheets"], 16)
        self.assertEqual(result["n_sheets"], 18)
        self.assertEqual(result["n_months"], 297)
        first = result["industry_sheets"][0]
        self.assertEqual(first["n_months"], 297)
        self.assertEqual(first["min_date"], "2002-01-01")
        self.assertEqual(first["max_date"], "2026-09-01")
        self.assertEqual(first["n_missing_months"], 0)
        self.assertEqual(first["weight"], "—")
        self.assertEqual(first["basis"], "feature, not weighted")
        for key in (
            "ibk_raw",
            "ibk_sa",
            "price_expectations_raw",
            "price_expectations_sa",
            "costs_raw",
            "costs_sa",
        ):
            self.assertEqual(first["indicators"][key]["n"], 297)


class TestSummariesAndJson(unittest.TestCase):
    def test_weekly_summary(self):
        weekly = {
            audit.WEEKLY_AGGREGATE: [
                (date(2024, 1, 1), 100.1),
                (date(2024, 1, 8), 100.2),
            ],
            "A": [(date(2024, 1, 1), 100.3)],
        }
        result = audit.weekly_summary(weekly)
        self.assertEqual(result["n_catalog_series"], 2)
        self.assertEqual(result["n_product_series"], 1)
        self.assertEqual(result["n_points"], 3)
        self.assertEqual(result["by_year"][0]["year"], 2024)
        self.assertEqual(result["by_year"][0]["n_series"], 2)

    def test_monthly_summary(self):
        monthly = {"cpi_all": [(date(2024, 1, 31), 100.5)]}
        result = audit.monthly_summary(monthly)
        by_key = {item["key"]: item for item in result["series"]}
        self.assertEqual(by_key["cpi_all"]["n"], 1)
        self.assertEqual(by_key["cpi_all"]["n_missing_months"], 0)

    def test_write_json(self):
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "audit.json"
            audit.write_json(path, {"b": 2, "a": 1})
            self.assertEqual(json.loads(path.read_text(encoding="utf-8")), {"a": 1, "b": 2})

    def test_no_internal_panel_inputs_in_source(self):
        source = Path(audit.__file__).read_text(encoding="utf-8")
        for forbidden in ("ОКВЭД", "BSU", "model.xlsx", "panel.csv"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
