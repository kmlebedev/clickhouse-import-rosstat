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


# Проверенные поактивные пары (AISC, TCC) за 2019–2021, USD/oz, живут в модуле
# (`ex.real_series`), чтобы калибровочные тесты не зависели от разбора PDF и не
# дублировали числа. Имя намеренно отличается от модульного `ex.build_series`
# (тот строит серию из фикстур и участвует в перекрёстной сверке).
real_series = ex.real_series


class TestCalibrate(unittest.TestCase):
    """Клин «затраты поддержания» как доля TCC, с отбраковкой выбросов по MAD."""

    def test_wedge_share(self):
        self.assertAlmostEqual(ex.wedge_share(618.0, 369.0), 0.6748, places=3)
        self.assertAlmostEqual(ex.wedge_share(865.0, 569.0), 0.5202, places=3)

    def test_wedge_share_rejects_nonpositive_tcc(self):
        with self.assertRaises(ValueError):
            ex.wedge_share(500.0, 0.0)
        with self.assertRaises(ValueError):
            ex.wedge_share(500.0, -1.0)

    def test_calibration_uses_2019_to_2021(self):
        cal = ex.calibrate(ex.real_series())
        self.assertEqual(sorted(cal["OLIMPIADA"]["years"]), [2019, 2020, 2021])

    def test_known_r_bar_values(self):
        cal = ex.calibrate(ex.real_series())
        self.assertAlmostEqual(cal["OLIMPIADA"]["r_bar"], 0.626, places=3)
        self.assertAlmostEqual(cal["NATALKA"]["r_bar"], 0.573, places=3)
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], 0.488, places=3)

    def test_no_outliers_in_clean_data(self):
        cal = ex.calibrate(ex.real_series())
        for asset in ex.ASSETS:
            self.assertEqual(cal[asset]["discarded"], [])

    def test_outlier_is_discarded_and_reported(self):
        series = ex.real_series()
        series["KURANAKH"][2020] = {"aisc": 5000.0, "tcc": 518.0}
        cal = ex.calibrate(series)
        self.assertIn(2020, [y for y, _ in cal["KURANAKH"]["discarded"]])
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], 0.503, places=2)

    def test_discarded_records_year_and_value(self):
        # `discarded` — список пар (год, значение клина), и значение относится
        # именно к отброшенному году.
        series = ex.real_series()
        series["KURANAKH"][2020] = {"aisc": 5000.0, "tcc": 518.0}
        cal = ex.calibrate(series)
        discarded = cal["KURANAKH"]["discarded"]
        self.assertEqual([y for y, _ in discarded], [2020])
        self.assertAlmostEqual(discarded[0][1], (5000.0 - 518.0) / 518.0, places=6)
        # Убранный год не попадает в годы, по которым усреднено.
        self.assertEqual(sorted(cal["KURANAKH"]["years"]), [2019, 2021])

    def test_mad_rule_beats_mean_sigma_on_gross_outlier(self):
        """Документирует ПРИЧИНУ выбора MAD: при n=3 mean/σ не ловит выброс.

        На испорченной серии Куранах (клинья 0.486, 8.653, 0.520) правило
        «mean ± 2σ» НЕ отбрасывает 8.653 — выброс сам раздувает σ и проходит
        собственный порог, давая мусорное среднее. Правило MAD отбрасывает его.
        Тест — регресс-страж: возврат к mean/σ его сломает.
        """
        import statistics as _st

        series = ex.real_series()
        series["KURANAKH"][2020] = {"aisc": 5000.0, "tcc": 518.0}
        values = [
            (series["KURANAKH"][y]["aisc"] - series["KURANAKH"][y]["tcc"]) / series["KURANAKH"][y]["tcc"]
            for y in ex.CALIBRATION_YEARS
        ]
        self.assertAlmostEqual(values[1], 8.653, places=3)

        # Наивное правило mean ± 2σ оставляет выброс: он выживает свою же полосу.
        mean = _st.mean(values)
        sigma = _st.pstdev(values)
        kept_by_mean_sigma = [v for v in values if abs(v - mean) <= 2 * sigma]
        self.assertEqual(len(kept_by_mean_sigma), 3)
        self.assertGreater(_st.mean(kept_by_mean_sigma), 3.0)

        # MAD-правило выброс отбрасывает.
        cal = ex.calibrate(series)
        self.assertIn(2020, [y for y, _ in cal["KURANAKH"]["discarded"]])
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], 0.503, places=3)

    def test_zero_mad_discards_nothing(self):
        # Медиана абсолютных отклонений равна нулю (два одинаковых клина),
        # делить на неё нельзя: все точки сохраняются.
        series = ex.real_series()
        series["KURANAKH"] = {
            2019: {"aisc": 300.0, "tcc": 200.0},   # r = 0.5
            2020: {"aisc": 200.0, "tcc": 100.0},   # r = 1.0
            2021: {"aisc": 400.0, "tcc": 200.0},   # r = 1.0
        }
        cal = ex.calibrate(series)
        self.assertEqual(sorted(cal["KURANAKH"]["years"]), [2019, 2020, 2021])
        self.assertEqual(cal["KURANAKH"]["discarded"], [])
        self.assertAlmostEqual(cal["KURANAKH"]["r_bar"], (0.5 + 1.0 + 1.0) / 3, places=6)

    def test_fewer_than_two_observations_raises(self):
        # Защита срабатывает на вырожденном входе: у актива меньше двух полных
        # пар (AISC, TCC). Одна точка не поддерживает ни проверку на выброс, ни
        # оценку разброса — молча вернуть её среднее значило бы выдать шум за
        # калибровку.
        #
        # Почему не «MAD отбросил всё, кроме одной точки»: при n = 3 медиана
        # абсолютных отклонений — это СРЕДНЕЕ из трёх отклонений, поэтому две
        # наименьшие точки всегда лежат внутри своей же полосы, и MAD-правило
        # физически не может оставить меньше двух точек на полном векторе. Тест
        # добирается до ветки через неполный вектор — единственный достижимый
        # для неё вход.
        series = ex.real_series()
        series["KURANAKH"] = {
            2020: {"aisc": 756.0, "tcc": 518.0},   # полная пара
            2019: {"aisc": 777.0, "tcc": None},    # неполная — не клин
            2021: {"aisc": None, "tcc": 569.0},    # неполная — не клин
        }
        with self.assertRaises(ex.CalibrationError) as ctx:
            ex.calibrate(series)
        self.assertIn("KURANAKH", str(ctx.exception))

    def test_asset_absent_from_series_raises(self):
        # Актив из ASSETS, по которому в серии нет ни одного калибровочного
        # года, — тоже вырожденный вход, а не «актив без клина».
        series = ex.real_series()
        del series["NATALKA"]
        with self.assertRaises(ex.CalibrationError) as ctx:
            ex.calibrate(series)
        self.assertIn("NATALKA", str(ctx.exception))

    def test_outlier_band_admits_clean_data_but_rejects_gross_outlier(self):
        # Защита от регресса в ЛЮБУЮ сторону: полоса должна быть достаточно
        # широкой, чтобы чистые данные проходили целиком, и достаточно узкой,
        # чтобы грубый выброс отбрасывался. Обе половины проверяются вместе,
        # потому что порог — один параметр и расширять его «на глаз» нельзя.
        clean = ex.calibrate(ex.real_series())
        for asset in ex.ASSETS:
            self.assertEqual(clean[asset]["discarded"], [], asset)

        series = ex.real_series()
        series["KURANAKH"][2020] = {"aisc": 5000.0, "tcc": 518.0}
        self.assertEqual(
            [y for y, _ in ex.calibrate(series)["KURANAKH"]["discarded"]], [2020]
        )

    def test_years_carry_only_surviving_years(self):
        cal = ex.calibrate(ex.real_series())
        for asset in ex.ASSETS:
            self.assertEqual(sorted(cal[asset]["years"]), [2019, 2020, 2021])

    def test_r_bar_is_mean_of_all_three_verified_years(self):
        # Прибивает ожидаемые значения к таблице брифа: r_bar — В ТОЧНОСТИ
        # среднее трёх клиньев, то есть ни один год не отброшен.
        cal = ex.calibrate(ex.real_series())
        for asset, pairs in ex._REAL_AISC_TCC.items():
            r = [(a - t) / t for a, t in pairs.values()]
            self.assertAlmostEqual(cal[asset]["r_bar"], sum(r) / 3, places=9, msg=asset)
            self.assertEqual(cal[asset]["discarded"], [], asset)


if __name__ == "__main__":
    unittest.main()
