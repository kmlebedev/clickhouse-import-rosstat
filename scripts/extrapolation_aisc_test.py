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

    def test_2018_tcc_vector_without_verninskoye(self):
        # В FY2018 годовой TCC раскрыт по четырём рудникам, которые целиком
        # представлены и по AISC. Вернинского здесь нет: его годовой AISC в
        # FY2018 не раскрыт, и неполная пара (AISC, TCC) в результат не попадает.
        # (Тест НЕ про TCC Вернинского — ради явности имя не «is_complete».)
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

    def test_annual_tcc_values_differ_from_q4_values(self):
        # В блоке TCC годовые и квартальные предложения ИДУТ ВПЕРЕМЕЖКУ
        # (FY2019: «At Blagodatnoye, TCC amounted to $373» рядом с «In 2019, TCC
        # at Blagodatnoye were $398»). Проверяем именно TCC-блок, где живёт
        # перемежение.
        got = ex.extract_year(self.texts[2019], 2019)
        q4_tcc = {"OLIMPIADA": 221.0, "BLAGODATNOYE": 373.0, "NATALKA": 376.0,
                  "VERNINSKOYE2": 372.0, "KURANAKH": 535.0}
        for asset, q4_value in q4_tcc.items():
            self.assertNotEqual(got[asset]["tcc"], q4_value, asset)
        self.assertEqual(got["BLAGODATNOYE"]["tcc"], 398.0)
        self.assertEqual(got["NATALKA"]["tcc"], 396.0)

    def test_block_parsing_is_independent_of_sentence_order(self):
        # КРИТИЧНО: результат не должен зависеть от порядка предложений в блоке.
        # Берём настоящий годовой TCC-блок FY2019 и переносим все предложения без
        # собственного префикса «In 2019,» (это квартальные) в КОНЕЦ — то же
        # содержание, другой порядок. Годовые величины обязаны сохраниться:
        # 396/398, а не квартальные 376/373.
        import re as _re
        block = ex._annual_block(self.texts[2019], 2019, "TCC")
        sentences = [s for s in _re.split(r"(?<=\.)\s+", block) if s.strip()]
        prefixed = [s for s in sentences if s.strip().startswith("In 2019,")]
        bare = [s for s in sentences if not s.strip().startswith("In 2019,")]
        self.assertGreater(len(prefixed), 0)
        self.assertGreater(len(bare), 0)
        reordered = " ".join(prefixed + bare)

        values = ex._block_values(reordered, "TCC", 2019)
        self.assertEqual(values["NATALKA"], 396.0)
        self.assertEqual(values["BLAGODATNOYE"], 398.0)
        self.assertEqual(values["KURANAKH"], 523.0)
        self.assertEqual(values["VERNINSKOYE2"], 363.0)

    def test_foreign_year_sentence_is_ignored(self):
        # Предложение с ЧУЖИМ годовым префиксом внутри блока не относится к
        # разбираемому году и должно быть пропущено.
        block = ex._annual_block(self.texts[2019], 2019, "TCC")
        injected = block + " In 2020, TCC at Kuranakh were $999 per ounce."
        values = ex._block_values(injected, "TCC", 2019)
        self.assertEqual(values["KURANAKH"], 523.0)

    def test_year_prefixed_beats_bare_regardless_of_order(self):
        # Годовая величина приоритетнее унаследованной (квартальной) независимо
        # от того, какое предложение идёт первым.
        bare_first = ("At Natalka, TCC amounted to $376 per ounce. "
                      "In 2019, TCC at Natalka declined to $396 per ounce.")
        prefixed_first = ("In 2019, TCC at Natalka declined to $396 per ounce. "
                          "At Natalka, TCC amounted to $376 per ounce.")
        self.assertEqual(ex._block_values(bare_first, "TCC", 2019)["NATALKA"], 396.0)
        self.assertEqual(ex._block_values(prefixed_first, "TCC", 2019)["NATALKA"], 396.0)

    def test_comparison_before_observation_is_known_limitation(self):
        # ЗАДОКУМЕНТИРОВАННОЕ ОГРАНИЧЕНИЕ (I1): если сравнение стоит ПЕРЕД
        # наблюдением, берётся первая сумма — сравнение. В реальных фикстурах
        # такой конструкции нет, поэтому поведение не менялось; тест фиксирует
        # текущую границу, чтобы регресс был заметен.
        text = ("In 2019, AISC at Olimpiada, compared to $111 per ounce in 2018, "
                "was $222 per ounce. Capex")
        self.assertEqual(ex._block_values(text, "AISC", 2019)["OLIMPIADA"], 111.0)

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
        # Проза затрат ЕСТЬ (есть AISC/TCC при активе), но годового якоря
        # «In 1999, AISC at» нет — разбирать нечего, результат пуст.
        text = ("In 1998, AISC at Olimpiada decreased to $400 per ounce. "
                "In 1998, TCC at Olimpiada were $200 per ounce. Capex")
        self.assertEqual(ex.extract_year(text, 1999), {})

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


def fixture_path(name: str) -> Path:
    return ex.FIXTURE_DIR / name


class TestPriorYearCitations(unittest.TestCase):
    """Цитата прошлогоднего годового TCC внутри годового абзаца.

    Полюс не пересчитывает в отчёте полную таблицу прошлого года — цитируется
    ровно один прошлогодний TCC (Наталки). И именно эта цитата даёт единственную
    проверяемую перекрёстную сверку между двумя документами.
    """

    def setUp(self):
        self.texts = {y: normalized_fixture(y) for y in (2018, 2019, 2020, 2021)}

    def test_all_three_citations_match_previous_annual_value(self):
        # 747 — годовой TCC Наталки за 2018 в FY2018;
        # 396 — годовой TCC Наталки за 2019 в FY2019;
        # 355 — годовой TCC Наталки за 2020 в FY2020.
        expected = {
            2019: {"NATALKA": {2018: 747.0}},
            2020: {"NATALKA": {2019: 396.0}},
            2021: {"NATALKA": {2020: 355.0}},
        }
        for year, want in expected.items():
            got = ex.extract_prior_year_citations(self.texts[year], year)
            self.assertEqual(got, want, year)

    def test_cited_value_equals_previous_report_annual_value(self):
        # Цитата в отчёте года Y+1 совпадает с собственным годовым значением
        # отчёта года Y — это и есть проверяемое пересечение.
        for citing_year in (2019, 2020, 2021):
            cited = ex.extract_prior_year_citations(
                self.texts[citing_year], citing_year
            )["NATALKA"][citing_year - 1]
            annual = ex.extract_year(
                self.texts[citing_year - 1], citing_year - 1
            )["NATALKA"]["tcc"]
            self.assertEqual(cited, annual, citing_year)

    def test_fy2018_has_no_citation(self):
        # У FY2018 нет предшественника среди фикстур — цитат нет вовсе.
        self.assertEqual(ex.extract_prior_year_citations(self.texts[2018], 2018), {})


class TestBuildSeries(unittest.TestCase):
    """Серия AISC/TCC по трём калибровочным релизам."""

    FIXTURES = {
        2019: "polyus_pr_4q_fy2019.pdf",
        2020: "polyus_pr_4q_fy2020.pdf",
        2021: "polyus_pr_4q_fy2021.pdf",
    }

    def series(self):
        return ex.build_series({
            year: fixture_path(name) for year, name in self.FIXTURES.items()
        })

    def test_known_annual_values(self):
        series = self.series()
        for year, aisc, tcc in (
            (2019, 455.0, 293.0), (2020, 515.0, 312.0), (2021, 618.0, 369.0),
        ):
            self.assertEqual(series["OLIMPIADA"][year]["aisc"], aisc, year)
            self.assertEqual(series["OLIMPIADA"][year]["tcc"], tcc, year)

    def test_full_series_matches_annual_vectors(self):
        series = self.series()
        for asset in ex.ASSETS:
            for year in (2019, 2020, 2021):
                text = normalized_fixture(year)
                annual = ex.extract_year(text, year)[asset]
                self.assertEqual(series[asset][year]["aisc"], annual["aisc"],
                                 (asset, year))
                self.assertEqual(series[asset][year]["tcc"], annual["tcc"],
                                 (asset, year))

    def test_missing_asset_year_is_none_not_error(self):
        # FY2018 раскрывает только четыре актива (без Вернинского). Серия из
        # одного FY2018 не должна ни падать, ни выдумывать строку Вернинского.
        series = ex.build_series({2018: fixture_path("polyus_pr_4q_fy2018.pdf")})
        self.assertEqual(
            sorted(series),
            ["BLAGODATNOYE", "KURANAKH", "NATALKA", "OLIMPIADA"],
        )
        self.assertNotIn("VERNINSKOYE2", series)

    def test_fy2018_yields_four_assets(self):
        got = ex.extract_year(normalized_fixture(2018), 2018)
        self.assertEqual(len(got), 4)
        self.assertNotIn("VERNINSKOYE2", got)

    def test_citation_adds_prior_year_without_aisc(self):
        # Цитата FY2019 добавляет 2018-й год Наталки в серию: TCC есть, AISC
        # отсутствует (FY2019 не раскрывает прошлогодний AISC Наталки).
        series = ex.build_series({
            2019: fixture_path("polyus_pr_4q_fy2019.pdf"),
        })
        self.assertEqual(series["NATALKA"][2018], {"aisc": None, "tcc": 747.0})


class TestCrossCheck(unittest.TestCase):
    """Перекрёстная сверка: цитата против собственного годового значения."""

    CLEAN_PAIR = {
        2020: "polyus_pr_4q_fy2020.pdf",
        2021: "polyus_pr_4q_fy2021.pdf",
    }
    CORRUPT_PAIR = {
        2020: "polyus_pr_4q_fy2020.pdf",
        2021: "polyus_pr_4q_fy2021_corrupt.pdf",
    }

    def test_clean_pair_does_not_raise(self):
        # Норма: цитата FY2021 ($355 за 2020) совпадает с годовым FY2020 (355).
        ex.build_series({y: fixture_path(n) for y, n in self.CLEAN_PAIR.items()})

    def test_all_four_clean_fixtures_do_not_raise(self):
        ex.build_series({
            y: fixture_path(f"polyus_pr_4q_fy{y}.pdf")
            for y in (2018, 2019, 2020, 2021)
        })

    def test_corrupt_fixture_is_detected(self):
        # Испорченная фикстура правит цитату TCC Наталки за 2020: $365 вместо
        # $355. Она расходится с собственным годовым значением FY2020 (355).
        with self.assertRaises(ex.CrossCheckError) as ctx:
            ex.build_series({
                y: fixture_path(n) for y, n in self.CORRUPT_PAIR.items()
            })
        message = str(ctx.exception)
        self.assertIn("NATALKA", message)
        self.assertIn("2020", message)
        self.assertIn("355", message)
        self.assertIn("365", message)
        self.assertIn("polyus_pr_4q_fy2020.pdf", message)
        self.assertIn("polyus_pr_4q_fy2021_corrupt.pdf", message)

    def test_corrupt_fixture_is_ignored_on_discovery(self):
        # Автообнаружение (fixtures=None) не подхватывает испорченную фикстуру.
        series = ex.build_series()
        self.assertEqual(series["NATALKA"][2020], {"aisc": 529.0, "tcc": 355.0})
        self.assertEqual(series["NATALKA"][2021], {"aisc": 626.0, "tcc": 368.0})

    def test_missing_year_is_not_a_conflict(self):
        # FY2018 не цитирует ничего, и в наборе из одного FY2018 сверять нечего:
        # отсутствие цитируемого года — норма, а не конфликт.
        ex.build_series({2018: fixture_path("polyus_pr_4q_fy2018.pdf")})

    def test_tcc_tolerance_is_exact(self):
        # TCC сверяется точно: расхождение на 1 USD/oz — уже ошибка.
        with self.assertRaises(ex.CrossCheckError):
            ex.cross_check_overlap(
                2020,
                {"NATALKA": {"tcc": 355.0}},
                {"NATALKA": {"tcc": 356.0}},
                Path("a.pdf"), Path("b.pdf"),
            )

    def test_aisc_within_tolerance_passes(self):
        ex.cross_check_overlap(
            2020,
            {"NATALKA": {"aisc": 529.0}},
            {"NATALKA": {"aisc": 529.4}},
            Path("a.pdf"), Path("b.pdf"),
        )

    def test_aisc_beyond_tolerance_raises(self):
        with self.assertRaises(ex.CrossCheckError) as ctx:
            ex.cross_check_overlap(
                2020,
                {"NATALKA": {"aisc": 529.0}},
                {"NATALKA": {"aisc": 530.0}},
                Path("a.pdf"), Path("b.pdf"),
            )
        self.assertIn("AISC", str(ctx.exception))

    def test_2018_is_excluded_from_calibration(self):
        self.assertNotIn(2018, ex.CALIBRATION_YEARS)


if __name__ == "__main__":
    unittest.main()
