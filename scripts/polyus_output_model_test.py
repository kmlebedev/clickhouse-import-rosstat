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


if __name__ == "__main__":
    unittest.main(verbosity=2)
