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


# Нормализованные тексты четырёх фикстур — общие для всех тестов разбора.
# `pdftotext` на четырёх PDF небесплатен, поэтому тексты читаются один раз.
_NORMALIZED_CACHE: dict = {}


def normalized_fixture(year: int) -> str:
    if year not in _NORMALIZED_CACHE:
        _NORMALIZED_CACHE[year] = ex.normalize(
            ex.pdf_to_text(ex.FIXTURE_DIR / f"polyus_pr_4q_fy{year}.pdf")
        )
    return _NORMALIZED_CACHE[year]


class TestExtract(unittest.TestCase):
    """Годовая проза AISC/TCC — сверка с проверенными значениями фикстур."""

    def setUp(self):
        self.texts = {y: normalized_fixture(y) for y in (2018, 2019, 2020, 2021)}

    def test_2021_aisc(self):
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 618.0, "BLAGODATNOYE": 658.0, "NATALKA": 626.0,
                          "VERNINSKOYE2": 644.0, "KURANAKH": 865.0})

    def test_2021_tcc(self):
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual({a: got[a]["tcc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 369.0, "BLAGODATNOYE": 367.0, "NATALKA": 368.0,
                          "VERNINSKOYE2": 358.0, "KURANAKH": 569.0})

    def test_2020_aisc(self):
        got = ex.extract_year(self.texts[2020], 2020)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 515.0, "BLAGODATNOYE": 478.0, "NATALKA": 529.0,
                          "VERNINSKOYE2": 560.0, "KURANAKH": 756.0})

    def test_2019_aisc(self):
        got = ex.extract_year(self.texts[2019], 2019)
        self.assertEqual({a: got[a]["aisc"] for a in ex.ASSETS},
                         {"OLIMPIADA": 455.0, "BLAGODATNOYE": 566.0, "NATALKA": 605.0,
                          "VERNINSKOYE2": 636.0, "KURANAKH": 777.0})

    def test_2018_is_incomplete_for_verninskoye(self):
        got = ex.extract_year(self.texts[2018], 2018)
        self.assertNotIn("VERNINSKOYE2", got)
        self.assertEqual(got["OLIMPIADA"]["aisc"], 468.0)

    def test_shoulder_variant_at_kuranakh_2021(self):
        # "At Kuranakh, AISC increased to $865 per ounce"
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual(got["KURANAKH"]["aisc"], 865.0)

    # --- Полные векторы по всем годам (годовая проза, не квартал) ------------

    ANNUAL_AISC = {
        2019: {"OLIMPIADA": 455.0, "BLAGODATNOYE": 566.0, "NATALKA": 605.0,
               "VERNINSKOYE2": 636.0, "KURANAKH": 777.0},
        2020: {"OLIMPIADA": 515.0, "BLAGODATNOYE": 478.0, "NATALKA": 529.0,
               "VERNINSKOYE2": 560.0, "KURANAKH": 756.0},
        2021: {"OLIMPIADA": 618.0, "BLAGODATNOYE": 658.0, "NATALKA": 626.0,
               "VERNINSKOYE2": 644.0, "KURANAKH": 865.0},
    }
    ANNUAL_TCC = {
        2019: {"OLIMPIADA": 293.0, "BLAGODATNOYE": 398.0, "NATALKA": 396.0,
               "VERNINSKOYE2": 363.0, "KURANAKH": 523.0},
        2020: {"OLIMPIADA": 312.0, "BLAGODATNOYE": 347.0, "NATALKA": 355.0,
               "VERNINSKOYE2": 327.0, "KURANAKH": 518.0},
        2021: {"OLIMPIADA": 369.0, "BLAGODATNOYE": 367.0, "NATALKA": 368.0,
               "VERNINSKOYE2": 358.0, "KURANAKH": 569.0},
    }
    # Годовой AISC 2018 (Вернинское отсутствует — отдельный тест выше).
    ANNUAL_AISC_2018 = {"OLIMPIADA": 468.0, "BLAGODATNOYE": 547.0,
                        "NATALKA": 1253.0, "KURANAKH": 771.0}

    def test_full_aisc_vectors(self):
        for year, expected in self.ANNUAL_AISC.items():
            got = ex.extract_year(self.texts[year], year)
            self.assertEqual({a: got[a]["aisc"] for a in expected}, expected, year)

    def test_full_tcc_vectors(self):
        for year, expected in self.ANNUAL_TCC.items():
            got = ex.extract_year(self.texts[year], year)
            self.assertEqual({a: got[a]["tcc"] for a in expected}, expected, year)

    def test_2018_aisc_vector(self):
        got = ex.extract_year(self.texts[2018], 2018)
        self.assertEqual({a: got[a]["aisc"] for a in self.ANNUAL_AISC_2018},
                         self.ANNUAL_AISC_2018)

    def test_2018_tcc_vector_is_complete(self):
        # В FY2018 годовой TCC раскрыт по четырём рудникам, которые целиком
        # представлены и по AISC. Вернинского здесь нет: его годовой AISC в
        # FY2018 не раскрыт, и неполная пара (AISC, TCC) в результат не попадает.
        got = ex.extract_year(self.texts[2018], 2018)
        self.assertEqual({a: got[a]["tcc"] for a in self.ANNUAL_AISC_2018},
                         {"OLIMPIADA": 267.0, "BLAGODATNOYE": 360.0, "NATALKA": 747.0,
                          "KURANAKH": 511.0})

    def test_multi_asset_sentence_2021(self):
        # Одно предложение несёт сразу два актива:
        # «AISC at Natalka increased to $626 per ounce, while AISC at Verninskoye
        #  increased to $644 per ounce».
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual(got["NATALKA"]["aisc"], 626.0)
        self.assertEqual(got["VERNINSKOYE2"]["aisc"], 644.0)

    # --- Ловушки разбора -----------------------------------------------------

    def test_first_amount_wins_over_prior_year_comparison(self):
        # FY2019 TCC Natalka: «…declined to $396 per ounce, compared to $747 per
        # ounce in 2018…» — 396 это наблюдение, 747 — сравнение.
        got = ex.extract_year(self.texts[2019], 2019)
        self.assertEqual(got["NATALKA"]["tcc"], 396.0)

    def test_annual_values_differ_from_q4_values(self):
        # Квартальные значения того же года должны отсутствовать в результате.
        got = ex.extract_year(self.texts[2019], 2019)
        q4 = {"OLIMPIADA": 352.0, "BLAGODATNOYE": 496.0, "NATALKA": 565.0,
              "VERNINSKOYE2": 696.0, "KURANAKH": 940.0}
        for asset, q4_value in q4.items():
            self.assertNotEqual(got[asset]["aisc"], q4_value, asset)

    def test_alluvial_is_skipped_not_an_error(self):
        # «AISC at Alluvial increased to $1,277 per ounce» — не рудник пятёрки.
        got = ex.extract_year(self.texts[2021], 2021)
        self.assertEqual(set(got), set(ex.ASSETS))
        self.assertNotIn("ALLUVIAL", got)

    def test_unknown_asset_name_raises(self):
        text = "In 2030, AISC at Olympiada decreased to $100 per ounce. Capex"
        with self.assertRaises(ex.AssetNameError) as ctx:
            ex.extract_year(text, 2030)
        self.assertIn("Olympiada", str(ctx.exception))
        self.assertIn("2030", str(ctx.exception))

    def test_no_annual_anchor_returns_empty(self):
        self.assertEqual(ex.extract_year("Capex something else entirely", 1999), {})

    def test_quarterly_only_document_returns_empty(self):
        # Документ с одним квартальным абзацем и без годового якоря.
        text = "In the fourth quarter, AISC at Olimpiada decreased to $352 per ounce."
        self.assertEqual(ex.extract_year(text, 2019), {})


class TestVerniskoyeSpelling(unittest.TestCase):
    def test_both_spellings_map_to_verninskoye2(self):
        for spelling in ("Verninskoye", "Verniskoye"):
            text = (
                f"In 2022, AISC at {spelling} decreased to $100 per ounce "
                f"as TCC fell. In 2022, TCC at {spelling} were $50 per ounce. Capex"
            )
            got = ex.extract_year(text, 2022)
            self.assertEqual(got["VERNINSKOYE2"], {"aisc": 100.0, "tcc": 50.0})


if __name__ == "__main__":
    unittest.main()
