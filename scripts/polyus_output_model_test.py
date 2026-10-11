#!/usr/bin/env python3
"""Тесты scripts/polyus_output_model.py.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/polyus_output_model_test.py -v

Набор ОФЛАЙН: обращения к ClickHouse подменяются `unittest.mock.patch.object`,
поэтому ни БД, ни сети для прогона не нужно. Живая проверка — отдельный ручной
шаг `make polyus-output` + SQL-проверки (план, Task 5).
"""

import sys
import unittest
import unittest.mock
from pathlib import Path

# Скрипт и тест лежат рядом, запуск прямой (`python3 scripts/...`),
# поэтому его каталог добавляется в sys.path до импорта модуля.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import polyus_output_model as om


class TestReadRefined(unittest.TestCase):
    def test_opr_years_are_exactly_the_first_three(self):
        # Окно ОПР в этом срезе — 2025–2027 (спека §3): 2028+ принадлежит Go-сиду.
        self.assertEqual(om.OPR_YEARS, (2025, 2026, 2027))

    def test_metric_is_refined_not_dore(self):
        # 2025: refined 242.2 против dore 230.8. Метрика обязана быть refined.
        with unittest.mock.patch.object(om, "ch_query", return_value=[{"v": 242.2}]):
            self.assertEqual(om.read_refined_output("VERNINSKOYE2", 2025), 242.2)
        self.assertIn(om.REFINED_METRIC, om.output_sql("VERNINSKOYE2", 2025))

    def test_absent_year_raises_instead_of_zero(self):
        # Пустой ответ: ноль на месте факта занизил бы NAV.
        with unittest.mock.patch.object(om, "ch_query", return_value=[]):
            with self.assertRaises(om.OutputDataError):
                om.read_refined_output("VERNINSKOYE2", 2100)


class TestBuildRows(unittest.TestCase):
    def test_2027_is_mean_of_2025_and_2026(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        by_year = {r["year"]: r for r in rows}
        self.assertAlmostEqual(by_year[2027]["production_koz"], 257.0, places=1)

    def test_measured_years_are_not_averaged(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        by_year = {r["year"]: r for r in rows}
        self.assertAlmostEqual(by_year[2025]["production_koz"], 242.2, places=1)
        self.assertAlmostEqual(by_year[2026]["production_koz"], 271.7, places=1)

    def test_grade_is_none_and_capex_zero(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        for row in rows:
            self.assertIsNone(row["grade_gpt"])
            self.assertEqual(row["capex_project"], 0)
            self.assertEqual(row["capex_sustaining"], 0.0)
            self.assertEqual(row["closure_costs"], 0)

    def test_aisc_never_below_tcc(self):
        for row in om.build_rows({2025: 242.2, 2026: 271.7}):
            self.assertGreaterEqual(row["aisc"], row["tcc"])

    def test_missing_measured_year_raises(self):
        with self.assertRaises(om.OutputDataError):
            om.build_rows({2025: 242.2})  # 2026 не прочитан

    def test_zero_output_raises(self):
        # Измеренный ноль неотличим от отсутствия факта, и строка с ним
        # занизила бы NPV. Отказ, а не строка.
        with self.assertRaises(om.OutputDataError):
            om.build_rows({2025: 0.0, 2026: 271.7})

    def test_rows_are_three_and_in_year_order(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        self.assertEqual([r["year"] for r in rows], [2025, 2026, 2027])
        self.assertTrue(all(r["asset"] == om.ASSET for r in rows))


if __name__ == "__main__":
    unittest.main(verbosity=2)
