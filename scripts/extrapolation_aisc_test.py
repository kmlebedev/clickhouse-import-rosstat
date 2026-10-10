#!/usr/bin/env python3
"""Тесты констант и конфигурации scripts/extrapolation_aisc.py.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/extrapolation_aisc_test.py -v
"""

import os
import sys
import unittest
from pathlib import Path

# Скрипт и тест лежат рядом, запуск прямой (`python3 scripts/...`),
# поэтому его каталог добавляется в sys.path до импорта модуля.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import extrapolation_aisc as ex


class TestConfig(unittest.TestCase):
    def test_assets_match_mine_plans_names(self):
        self.assertEqual(
            ex.ASSETS,
            ["OLIMPIADA", "BLAGODATNOYE", "NATALKA", "VERNINSKOYE2", "KURANAKH"],
        )

    def test_mine_life_years_match_go_seed(self):
        self.assertEqual(ex.MINE_LIFE_YEARS, {
            "OLIMPIADA": 10, "BLAGODATNOYE": 13, "NATALKA": 24,
            "VERNINSKOYE2": 15, "KURANAKH": 15,
        })

    def test_plan_row_count_is_77(self):
        self.assertEqual(sum(ex.MINE_LIFE_YEARS.values()), 77)

    def test_fixtures_exist(self):
        for year in (2018, 2019, 2020, 2021):
            path = ex.FIXTURE_DIR / f"polyus_pr_4q_fy{year}.pdf"
            self.assertTrue(path.exists(), path)

    def test_ch_config_reads_env(self):
        os.environ["CLICKHOUSE_HOST"] = "example.invalid"
        os.environ["CLICKHOUSE_USER"] = "u"
        os.environ["CLICKHOUSE_PASSWORD"] = "secret"
        self.assertEqual(ex.ch_config()["host"], "example.invalid")

    def test_mask_hides_secret(self):
        self.assertEqual(ex.mask("secret", "pw=secret"), "pw=***")


class TestNormalize(unittest.TestCase):
    def test_joins_wrapped_sentence(self):
        raw = "AISC at Natalka increased\n to $626 per ounce"
        self.assertIn("increased to $626 per ounce", ex.normalize(raw))

    def test_strips_page_number_inside_sentence(self):
        raw = "AISC at\n\n6\n\nBlagodatnoye increased to $658 per ounce"
        self.assertIn("AISC at Blagodatnoye increased", ex.normalize(raw))

    def test_collapses_runs_of_spaces(self):
        self.assertEqual(ex.normalize("a   b \n c"), "a b c")

    def test_amount_handles_comma_and_space(self):
        self.assertEqual(ex.parse_amount("1,277"), 1277.0)
        self.assertEqual(ex.parse_amount("1 277"), 1277.0)
        self.assertEqual(ex.parse_amount("618"), 618.0)

    def test_fy2021_fixture_yields_known_sentence(self):
        text = ex.normalize(ex.pdf_to_text(ex.FIXTURE_DIR / "polyus_pr_4q_fy2021.pdf"))
        self.assertIn("AISC at Natalka increased to $626 per ounce", text)


if __name__ == "__main__":
    unittest.main()
