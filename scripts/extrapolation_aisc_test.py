#!/usr/bin/env python3
"""Тесты констант и конфигурации scripts/extrapolation_aisc.py.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/extrapolation_aisc_test.py -v
"""

import csv
import http.client
import io
import os
import sys
import tempfile
import unittest
import unittest.mock
import urllib.error
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


class TestReconcile(unittest.TestCase):
    def test_reconciles_exactly_to_group(self):
        tcc = {"A": 400.0, "B": 700.0}
        rb = {"A": 0.60, "B": 0.50}
        prod = {"A": 1000.0, "B": 500.0}
        aisc, m, resid = ex.reconcile(tcc, rb, prod, group_aisc=1200.0)
        w = {"A": 1000 / 1500, "B": 500 / 1500}
        got = aisc["A"] * w["A"] + aisc["B"] * w["B"]
        self.assertAlmostEqual(got, 1200.0, places=6)
        self.assertAlmostEqual(resid, 0.0, places=6)

    def test_m_applies_to_wedge_not_to_tcc(self):
        tcc = {"A": 400.0}
        rb = {"A": 0.50}
        aisc, m, _ = ex.reconcile(tcc, rb, {"A": 1.0}, group_aisc=800.0)
        # r_bar*m = 1.0 -> AISC = 400*(1+1.0) = 800
        self.assertAlmostEqual(aisc["A"], 800.0, places=6)
        # Масштабируется ТОЛЬКО клин: tcc остаётся наблюдённой величиной.
        self.assertAlmostEqual(m, 2.0, places=6)

    def test_zero_wedge_falls_back_to_m_one(self):
        aisc, m, resid = ex.reconcile({"A": 400.0}, {"A": 0.0}, {"A": 1.0}, 500.0)
        self.assertEqual(m, 1.0)
        self.assertAlmostEqual(aisc["A"], 400.0, places=6)

    def test_all_zero_wedges_take_degenerate_branch(self):
        # Вырожденная ветвь целиком: den = 0, поэтому формула множителя
        # неприменима и берётся нейтральное m = 1.0 (AISC == TCC + клин).
        # Порог назван явно, чтобы связь «ветвь ↔ RECONCILE_WEDGE_EPSILON»
        # была видна в тесте, а не только в коде.
        tcc = {"A": 400.0, "B": 700.0}
        rb = {"A": 0.0, "B": 0.0}
        prod = {"A": 1.0, "B": 3.0}
        aisc, m, resid = ex.reconcile(tcc, rb, prod, 500.0)
        self.assertEqual(ex.RECONCILE_WEDGE_EPSILON, 1e-12)
        # den == 0 <= RECONCILE_WEDGE_EPSILON — ветвь обязана сработать.
        den = sum(tcc[a] * rb[a] * prod[a] / sum(prod.values()) for a in tcc)
        self.assertLessEqual(den, ex.RECONCILE_WEDGE_EPSILON)
        self.assertEqual(m, 1.0)
        self.assertAlmostEqual(aisc["A"], 400.0, places=6)
        self.assertAlmostEqual(aisc["B"], 700.0, places=6)
        # Остаток и здесь POST-HOC: веса (0.25, 0.75) дают 0.25·400 + 0.75·700 =
        # 625, то есть +125 к цели 500. Клиньев нет — увязать нечем.
        self.assertAlmostEqual(resid, 625.0 - 500.0, places=6)

    def test_negative_weighted_wedge_is_treated_as_degenerate(self):
        # ЛАТЕНТНЫЙ СЛУЧАЙ, ЗАКРЕПЛЁН ЯВНО. Отрицательный клин по A перевешивает
        # положительный по B: den < 0, то есть условие `den <= eps` истинно, и
        # срабатывает ВЫРОЖДЕННАЯ ветвь (m = 1.0), а не путь прижима. При m = 0
        # увязка 500 достигалась бы точно, но код намеренно оставлен как есть —
        # такова предписанная спецификация (`den <= eps`); докстрока теперь
        # описывает это буквально, без обещания «только нулевые клинья».
        # Смысл теста — сделать разницу ГРОМКОЙ, а не молчаливой.
        tcc = {"A": 500.0, "B": 500.0}
        rb = {"A": -0.8, "B": 0.5}
        prod = {"A": 900.0, "B": 100.0}
        aisc, m, resid = ex.reconcile(tcc, rb, prod, 500.0)
        den = sum(tcc[a] * rb[a] * prod[a] / sum(prod.values()) for a in tcc)
        self.assertLess(den, 0.0)
        self.assertEqual(m, 1.0)
        self.assertAlmostEqual(aisc["A"], 100.0, places=6)   # 500·(1 − 0.8)
        self.assertAlmostEqual(aisc["B"], 750.0, places=6)   # 500·(1 + 0.5)
        self.assertAlmostEqual(resid, 0.9 * 100.0 + 0.1 * 750.0 - 500.0, places=6)
        self.assertAlmostEqual(resid, -335.0, places=6)

    def test_zero_total_production_raises_value_error(self):
        # Нулевая Σ production — не «неопределённый остаток», а отсутствие
        # весов: должна подниматься внятная ValueError, а не голый
        # ZeroDivisionError из деления веса.
        with self.assertRaises(ValueError) as ctx:
            ex.reconcile({"A": 400.0, "B": 700.0}, {"A": 0.5, "B": 0.5},
                         {"A": 0.0, "B": 0.0}, 100.0)
        self.assertIn("добыча", str(ctx.exception))

    def test_residual_is_zero_when_group_is_reachable(self):
        # A positive m makes any achievable group AISC reachable, so the
        # post-hoc residual must be ~0 -- not merely "some sign".
        _, _, resid = ex.reconcile({"A": 400.0}, {"A": 0.5}, {"A": 1.0}, 900.0)
        self.assertAlmostEqual(resid, 0.0, places=6)

    def test_residual_is_nonzero_when_group_is_unreachable(self):
        # m cannot go below 0: with r_bar = 0.5 and tcc = 400 the floor is
        # AISC = 400, so a group AISC of 100 is unreachable and the residual
        # must report the shortfall rather than a pretend fit.
        #
        # ЗНАК ИСПРАВЛЕН ОТНОСИТЕЛЬНО БРИФА (`assertLess(resid, 0.0)`),
        # РАСХОЖДЕНИЕ ЗАФИКСИРОВАНО СОЗНАТЕЛЬНО. Бриф противоречив сам себе:
        # он требует и прижим m ≥ 0 (биндящий тест), и остаток, посчитанный
        # POST-HOC по ВЫДАННЫМ строкам. При прижиме строка содержит AISC = 400,
        # поэтому `Σ aisc·w − group_aisc = 400 − 100 = +300` — знак
        # ПОЛОЖИТЕЛЬНЫЙ. Отрицательное (−300) получилось бы только из
        # НЕприжатого обещания формулы (400·(1+0.5·(−1.5)) = 100), а это уже не
        # «то, что фактически содержат строки». Два требования брифа
        # несовместимы; приоритет отдан явно и дважды повторённому определению
        # остатка как post-hoc и прижиму (его пинует тот же тест).
        # Существенное в тесте сохранено: остаток НЕНУЛЕВОЙ, то есть
        # недостижимая цель не выдаётся за подогнанную.
        aisc, m, resid = ex.reconcile({"A": 400.0}, {"A": 0.5}, {"A": 1.0}, 100.0)
        self.assertGreater(resid, 0.0)
        # Прижим к нижней границе: m = 0, AISC стоит на полу (TCC), а без
        # прижима m был бы −1.5 и AISC ушёл бы на 100 — ниже TCC.
        self.assertEqual(m, 0.0)
        self.assertAlmostEqual(aisc["A"], 400.0, places=6)

    def test_deterministic_on_repeated_calls(self):
        # Ни скрытого состояния, ни ГПСЧ: тот же вход — тот же выход.
        args = (
            {"A": 400.0, "B": 700.0},
            {"A": 0.6, "B": 0.5},
            {"A": 1000.0, "B": 500.0},
            1200.0,
        )
        self.assertEqual(ex.reconcile(*args), ex.reconcile(*args))

    def test_multi_asset_weighted_mean_is_exact(self):
        tcc = {"A": 300.0, "B": 450.0, "C": 610.0, "D": 780.0, "E": 950.0}
        rb = {"A": 0.55, "B": 0.62, "C": 0.48, "D": 0.71, "E": 0.40}
        prod = {"A": 1200.0, "B": 830.0, "C": 640.0, "D": 310.0, "E": 150.0}
        target = 1500.0
        aisc, m, resid = ex.reconcile(tcc, rb, prod, target)
        total = sum(prod.values())
        got = sum(aisc[a] * prod[a] / total for a in tcc)
        self.assertAlmostEqual(got, target, places=6)
        self.assertAlmostEqual(resid, 0.0, places=6)
        self.assertGreater(m, 0.0)

    def test_real_calibration_reconciles_to_group_aisc_2025(self):
        # Стык с Task 5: клинья из `calibrate` действительно увязываются с
        # опубликованным групповым AISC 2025 теми же формулами.
        cal = ex.calibrate(ex.real_series())
        r_bar = {asset: cal[asset]["r_bar"] for asset in ex.ASSETS}
        tcc = {asset: ex.GROUP_TCC_2025_USD_PER_OZ for asset in ex.ASSETS}
        production = {asset: float(years) for asset, years in ex.MINE_LIFE_YEARS.items()}
        aisc, m, resid = ex.reconcile(
            tcc, r_bar, production, ex.GROUP_AISC_2025_USD_PER_OZ
        )
        self.assertEqual(set(aisc), set(ex.ASSETS))
        self.assertAlmostEqual(resid, 0.0, places=6)
        self.assertGreater(m, 0.0)


class TestProductionQuery(unittest.TestCase):
    """Проверка чтения факта добычи из `databook_polyus`.

    НИ ОДИН тест здесь не ходит в сеть: проверяются чистые функции —
    построитель SQL, разбор ответа и маскирование секрета. Сетевой вызов
    `ch_query` подменяется на уровне `urllib.request.urlopen`, поэтому тесты
    проходят и при остановленном ClickHouse.
    """

    def test_sql_uses_datapack_metric(self):
        sql = ex.production_sql("OLIMPIADA", 2021)
        self.assertIn("databook_polyus", sql)
        self.assertIn("Total Dore gold output", sql)
        self.assertIn("ANNUAL", sql)
        # FINAL обязателен: без него ReplacingMergeTree отдал бы год несколько
        # раз с разными версиями значения.
        self.assertIn("FINAL", sql)

    def test_sql_scopes_asset_and_year_from_date_column(self):
        sql = ex.production_sql("NATALKA", 2020)
        # Имя актива — в кавычках, год — через toYear(date), а НЕ из подписи
        # периода метрики.
        self.assertIn("table = 'NATALKA'", sql)
        self.assertIn("toYear(date) = 2020", sql)
        self.assertNotIn("OLIMPIADA", sql)

    def test_sql_requests_json_format_and_names_the_column(self):
        # HTTP-интерфейс по умолчанию отвечает TabSeparated, а разбор идёт по
        # ключу "v": оба требования к тексту запроса пинуются явно.
        sql = ex.production_sql("OLIMPIADA", 2021)
        self.assertIn("FORMAT JSON", sql)
        self.assertIn("AS v", sql)
        self.assertEqual(ex.ACTUAL_PRODUCTION_COLUMN, "v")

    def test_sql_rejects_unknown_asset(self):
        # Произвольная строка в литерал SQL попасть не должна — проверяется
        # ФОРМА имени, а не только вхождение в ASSETS (датапак шире пятёрки).
        for bad in ("OLIMPIADA'; DROP TABLE databook_polyus; --", "olympiada",
                    "OLIMPIADA\nOR 1=1", "", "OLIMPIADA 2"):
            with self.assertRaises(ValueError, msg=bad) as ctx:
                ex.production_sql(bad, 2021)
            self.assertIn("актив", str(ctx.exception))

    def test_sql_allows_datapack_only_asset(self):
        # ZAPADNOYE есть в датапаке, но не в пятёрке LOM; форма имени верна,
        # поэтому запрос строится (и вернёт 0.0, если строк нет).
        self.assertIn("table = 'ZAPADNOYE'", ex.production_sql("ZAPADNOYE", 2021))

    def test_sql_rejects_absurd_year(self):
        for bad in (-1, 0, 1800, 10**9, "2021", 2021.0):
            with self.assertRaises(ValueError, msg=repr(bad)):
                ex.production_sql("OLIMPIADA", bad)

    def test_parses_clickhouse_json_rows(self):
        # Реальный ответ ClickHouse всегда содержит секцию "data".
        payload = '{"data":[{"asset":"OLIMPIADA","y":2021,"v":1112.8}]}'
        rows = ex.parse_ch_json(payload)
        self.assertEqual(rows[0]["v"], 1112.8)

    def test_zero_production_is_not_an_error(self):
        # TITIMUKHTA/ZAPADNOYE законно нулевые в датапаке: ноль — факт, а не сбой.
        self.assertEqual(ex.parse_ch_json('{"data":[{"v":0}]}')[0]["v"], 0.0)

    def test_empty_data_array_gives_no_rows(self):
        self.assertEqual(ex.parse_ch_json('{"data":[]}'), [])

    def test_missing_data_key_gives_no_rows(self):
        # Пустой ответ сервера без секции "data" равнозначен отсутствию строк.
        self.assertEqual(ex.parse_ch_json("{}"), [])

    def test_malformed_json_raises_contextful_error(self):
        # Голый JSONDecodeError не отличим от сбоя в другом месте; ошибка
        # должна называть источник и не тащить в текст сырой payload (в нём
        # может оказаться пароль, если сервер вернул HTML-страницу логина).
        with self.assertRaises(ex.ClickHouseError) as ctx:
            ex.parse_ch_json("not json at all")
        message = str(ctx.exception)
        self.assertIn("JSON", message)
        self.assertIn("ClickHouse", message)
        self.assertNotIn("not json at all", message)

    def test_password_never_leaks_into_error(self):
        # Секрет не должен попасть ни в текст ошибки, ни в её args. Сетевой
        # вызов подменяется, поэтому тест офлайновый.
        secret = "sup3r-s3cret-pw"
        os.environ["CLICKHOUSE_PASSWORD"] = secret
        self.addCleanup(os.environ.pop, "CLICKHOUSE_PASSWORD", None)
        # Тело ответа сервера намеренно содержит пароль (так бывает, когда
        # ClickHouse отражает запрос в сообщении об ошибке). Тело передаётся
        # файлоподобным объектом, как это делает настоящий urllib.
        body = f"Code: 62. Syntax error near '{secret}'"
        request = ex.urllib.request.Request("http://localhost:8123/")

        with unittest.mock.patch(
            "extrapolation_aisc.urllib.request.urlopen",
            lambda req, timeout=None: (_ for _ in ()).throw(
                ex.HTTPError(
                    "http://localhost:8123/", 500, "Server Error",
                    {"Content-Type": "text/plain"}, io.StringIO(body),
                )
            ),
        ):
            with self.assertRaises(ex.ClickHouseError) as ctx:
                ex.ch_query("SELECT 1")
        self.assertIsInstance(request, ex.urllib.request.Request)
        message = str(ctx.exception)
        self.assertNotIn(secret, message)
        # Маскирование не просто «убрало» тело: диагностика на месте, а секрет
        # заменён на ***.
        self.assertIn("***", message)
        self.assertIn("Syntax error", message)
        self.assertNotIn(secret, repr(ctx.exception.args))

    def test_read_actual_production_maps_asset_year_to_float(self):
        # Сквозная проверка формы результата без сети: ответ подменяется.
        def fake_query(sql):
            if "OLIMPIADA" in sql:
                return [{"v": 1112.8}]
            return []

        with unittest.mock.patch.object(ex, "ch_query", fake_query):
            got = ex.read_actual_production(["OLIMPIADA", "NATALKA"], (2021,))
        self.assertEqual(got, {"OLIMPIADA": {2021: 1112.8}, "NATALKA": {2021: 0.0}})

    def test_missing_row_is_zero_not_error(self):
        with unittest.mock.patch.object(ex, "ch_query", lambda sql: []):
            got = ex.read_actual_production(["ZAPADNOYE"], (2021,))
        self.assertEqual(got["ZAPADNOYE"][2021], 0.0)

    def test_null_value_from_argmax_is_zero(self):
        # argMax по пустому множеству даёт NULL (а не отсутствие строк) —
        # это тот же «факта нет», а не сбой разбора.
        with unittest.mock.patch.object(ex, "ch_query", lambda sql: [{"v": None}]):
            got = ex.read_actual_production(["OLIMPIADA"], (2021,))
        self.assertEqual(got["OLIMPIADA"][2021], 0.0)


class _FakeResponse:
    """Минимальный ответ HTTP: контекстный менеджер и `read()`.

    Настоящий `urlopen` возвращает объект с обоими методами, а `ch_query`
    пользуется им как `with ... as response: response.read()`. Заглушка
    повторяет ровно этот контракт и ничего больше.
    """

    def __init__(self, body: str):
        self._body = body.encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def read(self):
        return self._body


class TestQueryTransport(unittest.TestCase):
    """Инварианты HTTP-вызова: порт, обёртка сбоев, маскирование.

    Все тесты офлайновые: `urlopen` подменяется, поэтому ни сети, ни
    запущенного ClickHouse не требуется.
    """

    def _capture_request(self, body='{"data":[]}'):
        """Прогоняет `ch_query` и возвращает перехваченный `Request`."""
        captured = {}

        def fake_urlopen(request, timeout=None):
            captured["request"] = request
            captured["timeout"] = timeout
            return _FakeResponse(body)

        with unittest.mock.patch(
            "extrapolation_aisc.urllib.request.urlopen", fake_urlopen
        ):
            ex.ch_query("SELECT 1")
        return captured["request"]

    def test_query_targets_http_port_not_native_port(self):
        # ГЛАВНЫЙ инвариант задачи: HTTP-интерфейс живёт на `http_port` (8123),
        # а `port` (9000) — нативный протокол. Запрос обязан идти на 8123:
        # на 9000 `urllib.request` либо повис, либо упал бы, поэтому подмена
        # `http_port` на `port` НЕ должна проходить незамеченной.
        # Порт из окружения не подставляется: проверяется именно default.
        self.addCleanup(os.environ.pop, "CH_HTTP_PORT", None)
        os.environ.pop("CH_HTTP_PORT", None)

        request = self._capture_request()

        self.assertIn(":8123/", request.full_url)
        self.assertNotIn(":9000/", request.full_url)
        self.assertEqual(ex.ch_config()["http_port"], "8123")
        self.assertNotEqual(ex.ch_config()["port"], "8123")

    def test_query_consults_ch_http_port_env(self):
        # Порт НЕ захардкожен: значение из `CH_HTTP_PORT` обязано попадать в
        # URL. Отдельное «непохожее» значение (18999) отличает чтение конфига
        # от случайного совпадения с 8123.
        self.addCleanup(os.environ.pop, "CH_HTTP_PORT", None)
        os.environ["CH_HTTP_PORT"] = "18999"

        request = self._capture_request()

        self.assertIn(":18999/", request.full_url)
        self.assertNotIn(":8123/", request.full_url)
        self.assertNotIn(":9000/", request.full_url)

    def test_query_uses_host_from_config(self):
        self.addCleanup(os.environ.pop, "CLICKHOUSE_HOST", None)
        os.environ["CLICKHOUSE_HOST"] = "ch.example.invalid"

        request = self._capture_request()

        self.assertIn("ch.example.invalid", request.full_url)

    def test_successful_response_returns_parsed_rows(self):
        # Успешный путь не сломан широкой веткой `OSError`: строки доходят.
        request = self._capture_request('{"data":[{"v":1112.8}]}')
        self.assertEqual(request.method, "POST")

        def fake_urlopen(req, timeout=None):
            return _FakeResponse('{"data":[{"v":1112.8}]}')

        with unittest.mock.patch(
            "extrapolation_aisc.urllib.request.urlopen", fake_urlopen
        ):
            self.assertEqual(ex.ch_query("SELECT 1"), [{"v": 1112.8}])

    def test_remote_disconnect_raises_clickhouse_error(self):
        # `http.client.RemoteDisconnected` НЕ является `URLError` (его MRO идёт
        # через ConnectionResetError → ConnectionError → OSError), поэтому
        # обработчика `URLError` ему мало: он обязан быть пойман широкой
        # веткой `OSError` и обёрнут в ClickHouseError, а не вылететь сырым.
        self.assertTrue(issubclass(http.client.RemoteDisconnected, OSError))
        self.assertFalse(issubclass(http.client.RemoteDisconnected, urllib.error.URLError))

        def fake_urlopen(request, timeout=None):
            raise http.client.RemoteDisconnected("Remote end closed connection")

        with unittest.mock.patch(
            "extrapolation_aisc.urllib.request.urlopen", fake_urlopen
        ):
            with self.assertRaises(ex.ClickHouseError) as ctx:
                ex.ch_query("SELECT 1")
        self.assertIn("ClickHouse недоступен", str(ctx.exception))

    def test_bare_oserror_raises_clickhouse_error(self):
        # Любой другой транспортный сбой (таймаут, DNS, сброс) — тоже
        # ClickHouseError, а не сырое исключение.
        for boom in (TimeoutError("timed out"), ConnectionResetError("reset"),
                     OSError("boom")):
            with self.subTest(boom=type(boom).__name__):
                def fake_urlopen(request, timeout=None, _boom=boom):
                    raise _boom

                with unittest.mock.patch(
                    "extrapolation_aisc.urllib.request.urlopen", fake_urlopen
                ):
                    with self.assertRaises(ex.ClickHouseError):
                        ex.ch_query("SELECT 1")

    def test_url_error_reason_is_preserved_and_masked(self):
        # `URLError` — подкласс `OSError`, поэтому его ловит широкая ветка;
        # диагностика (`reason`) при этом не теряется, а пароль маскируется.
        secret = "url-error-secret"
        self.addCleanup(os.environ.pop, "CLICKHOUSE_PASSWORD", None)
        os.environ["CLICKHOUSE_PASSWORD"] = secret

        def fake_urlopen(request, timeout=None):
            raise urllib.error.URLError(f"connection refused for {secret}")

        with unittest.mock.patch(
            "extrapolation_aisc.urllib.request.urlopen", fake_urlopen
        ):
            with self.assertRaises(ex.ClickHouseError) as ctx:
                ex.ch_query("SELECT 1")
        message = str(ctx.exception)
        self.assertIn("connection refused", message)
        self.assertNotIn(secret, message)
        self.assertIn("***", message)


class TestPlanRows(unittest.TestCase):
    """Плановые строки 2025–LOM: эскалация, увязка по годам, аллокация capex.

    Тесты брифа — авторитетны и приведены дословно. Добавлены проверки на
    единицы, полноту горизонта, неотрицательность клина, пропуск актива и
    наблюдаемость причины пропуска.
    """

    # Все пять активов с положительной добычей на весь горизонт Наталки
    # (2025–2048), чтобы у каждого актива были все его годы.
    def _full_production(self, koz: float = 100.0) -> dict:
        return {a: {y: koz for y in range(2025, 2049)} for a in ex.ASSETS}

    def _rows_with_full_production(self, group_aisc: float = 1400.0):
        return ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=group_aisc,
        )

    def test_capex_units_are_million_usd(self):
        # (aisc - tcc) * koz / 1000 = USD million
        got = ex.sustaining_capex_mn(aisc=950.0, tcc=450.0, production_koz=1400.0)
        self.assertAlmostEqual(got, 700.0, places=6)

    def test_row_count_is_77(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production={a: {y: 100.0 for y in range(2021, 2031)} for a in ex.ASSETS},
            group_aisc=1400.0,
        )
        self.assertEqual(len(rows), 77)

    def test_years_start_at_plan_base(self):
        rows = ex.build_plan_rows(
            tcc_2025={"OLIMPIADA": 773.0}, r_bar={"OLIMPIADA": 0.6},
            production={"OLIMPIADA": {2025: 900.0}}, group_aisc=1437.0,
        )
        self.assertEqual(rows[0]["year"], 2025)
        self.assertEqual(rows[-1]["year"], 2034)

    def test_escalation_compounds(self):
        self.assertAlmostEqual(ex.escalation_factor(2025, 2025), 1.0, places=9)
        self.assertGreater(ex.escalation_factor(2025, 2030), 1.0)

    def test_row_count_per_asset(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        self.assertEqual(len(rows), 77)
        counts = {}
        for row in rows:
            counts[row["asset"]] = counts.get(row["asset"], 0) + 1
        self.assertEqual(counts, {
            "OLIMPIADA": 10,
            "BLAGODATNOYE": 13,
            "NATALKA": 24,
            "VERNINSKOYE2": 15,
            "KURANAKH": 15,
        })

    def test_every_row_has_non_negative_wedge(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        self.assertTrue(rows)
        for row in rows:
            self.assertGreaterEqual(row["aisc"], row["tcc"], row)

    def test_every_row_has_zero_sustaining_capex(self):
        # ИНВАРИАНТ ДВОЙНОГО СЧЁТА. Полюс определяет AISC как «TCC plus … sustaining
        # capital expenditures …» — клин УЖЕ ВНУТРИ `aisc`. Движок
        # (dcf/model.go) вычитает и `aisc` (в выручке), и `capex_sustaining`
        # (в opCash), поэтому ненулевое значение здесь списало бы поддержание
        # дважды. Проверка идёт по ВСЕМ строкам и по всем трём наборам входа,
        # которые накрывают разные ветви увязки (штатная цель, недостижимо
        # низкая с прижатым m, недостижимо высокая).
        for group_aisc in (1400.0, 100.0, 5000.0):
            with self.subTest(group_aisc=group_aisc):
                rows = ex.build_plan_rows(
                    tcc_2025={a: 700.0 for a in ex.ASSETS},
                    r_bar={a: 0.6 for a in ex.ASSETS},
                    production=self._full_production(),
                    group_aisc=group_aisc,
                )
                self.assertTrue(rows)
                for row in rows:
                    self.assertEqual(row["capex_sustaining"], 0.0, row)

    def test_wedge_is_exposed_for_inspection_but_not_charged(self):
        # Клин поддержания доступен для глазной сверки в ключе `wedge_mn_usd`
        # (CSV выводит его как `wedge_mn_usd`), но в отток не идёт: он равен
        # `sustaining_capex_mn`, а `capex_sustaining` при этом ноль. Обе
        # величины проверяются вместе — так правка, вернувшая клин в отток,
        # ломает тест, а не уезжает молча.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        for row in rows:
            self.assertAlmostEqual(
                row["wedge_mn_usd"],
                ex.sustaining_capex_mn(row["aisc"], row["tcc"], row["production_koz"]),
                places=9,
            )
            # Клин в МЛН USD — это (aisc − tcc)·koz/1000, поэтому в год
            # КОНСЕРВАЦИИ (production_koz = 0, см. HALT_WINDOWS/`halted_years`)
            # он ЗАКОННО равен нулю: клин на унцию есть, умножать его не на что.
            # Зубы против двойного счёта сохранены отдельным требованием: в
            # любой ПРОИЗВОДЯЩИЙ год клин строго положителен, то есть величина
            # не «обнулилась вообще».
            self.assertGreaterEqual(row["wedge_mn_usd"], 0.0, row)
            if row["production_koz"] > 0.0:
                self.assertGreater(row["wedge_mn_usd"], 0.0, row)
            self.assertEqual(row["capex_sustaining"], 0.0, row)

    def test_year_never_exceeds_asset_mine_life(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        for row in rows:
            last = ex.PLAN_BASE_YEAR + ex.MINE_LIFE_YEARS[row["asset"]] - 1
            self.assertLessEqual(row["year"], last, row)
            self.assertGreaterEqual(row["year"], ex.PLAN_BASE_YEAR, row)

    def test_escalation_pins_compound_product(self):
        # 2025–2027 по 6 %, 2028–2033 по 4 %: 1.06³ · 1.04⁶ (год 2034 —
        # множитель за 2033-й, поэтому шесть четвёрок, а не семь).
        self.assertAlmostEqual(
            ex.escalation_factor(2025, 2034),
            ex.ESCALATION_2025_TO_2034,
            places=9,
        )
        self.assertAlmostEqual(
            ex.ESCALATION_2025_TO_2034, 1.06**3 * 1.04**6, places=12
        )

    def test_escalation_rejects_unknown_year_and_reverse_order(self):
        with self.assertRaises(ValueError):
            ex.escalation_factor(2025, 2050)
        with self.assertRaises(ValueError):
            ex.escalation_factor(2030, 2025)

    def test_tcc_and_group_target_escalate_together(self):
        # Решение контроллера: цель увязки года растёт тем же множителем,
        # что и TCC, поэтому ОТНОСИТЕЛЬНЫЙ клин не меняется, хотя абсолютные
        # USD/oz растут.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        by_asset_year = {(r["asset"], r["year"]): r for r in rows}
        row_2025 = by_asset_year[("OLIMPIADA", 2025)]
        row_2034 = by_asset_year[("OLIMPIADA", 2034)]
        factor_2034 = ex.escalation_factor(2025, 2034)
        self.assertAlmostEqual(row_2025["tcc"] * factor_2034, row_2034["tcc"], places=6)
        self.assertAlmostEqual(
            (row_2025["aisc"] - row_2025["tcc"]) / row_2025["tcc"],
            (row_2034["aisc"] - row_2034["tcc"]) / row_2034["tcc"],
            places=9,
        )

    def test_row_tcc_is_the_2025_anchor_escalated(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        by_asset_year = {(r["asset"], r["year"]): r for r in rows}
        row_2034 = by_asset_year[("NATALKA", 2034)]
        # Сравнение ОТНОСИТЕЛЬНОЕ: `places=6` на величине ~1055 читалось бы как
        # послабление, хотя фактически это 1e-9. Явный допуск 1e-6 от значения.
        expected = 700.0 * ex.escalation_factor(2025, 2034)
        self.assertAlmostEqual(row_2034["tcc"], expected, delta=1e-6 * expected)

    def test_group_aisc_for_year_uses_published_anchor(self):
        self.assertAlmostEqual(
            ex.group_aisc_for_year(2025), ex.GROUP_AISC_2025_USD_PER_OZ, places=9
        )
        self.assertAlmostEqual(
            ex.group_aisc_for_year(2034),
            ex.GROUP_AISC_2025_USD_PER_OZ * ex.escalation_factor(2025, 2034),
            places=9,
        )
        self.assertAlmostEqual(
            ex.group_aisc_for_year(2034), 1437.0 * 1.06**3 * 1.04**6, places=9
        )
        with self.assertRaises(ValueError):
            ex.group_aisc_for_year(2024)

    def test_absent_production_asset_is_skipped_and_reported(self):
        # Вернинского во входе нет вовсе, у Куранача — ключ есть, но добычи
        # нет ни в одном году горизонта. Оба не должны дать ни строки, и оба
        # должны быть названы в LAST_SKIPPED_ASSETS с непустой причиной.
        production = {
            "OLIMPIADA": {y: 100.0 for y in range(2025, 2035)},
            "BLAGODATNOYE": {y: 100.0 for y in range(2025, 2038)},
            "NATALKA": {y: 100.0 for y in range(2025, 2049)},
            "KURANAKH": {y: 0.0 for y in range(2025, 2040)},
        }
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS if a != "KURANAKH"},
            r_bar={a: 0.6 for a in ex.ASSETS if a != "KURANAKH"},
            production=production,
            group_aisc=1400.0,
        )
        emitted = {r["asset"] for r in rows}
        self.assertNotIn("VERNINSKOYE2", emitted)
        self.assertNotIn("KURANAKH", emitted)
        self.assertEqual(len(rows), 10 + 13 + 24)  # 47
        skipped = dict(ex.LAST_SKIPPED_ASSETS)
        self.assertIn("VERNINSKOYE2", skipped)
        self.assertIn("KURANAKH", skipped)
        for asset, reason in skipped.items():
            self.assertTrue(reason, asset)
        # Причина называет именно актив, а не год: у Вернинского пропала
        # добыча целиком, а не один год.
        self.assertIn("добыча", skipped["VERNINSKOYE2"])
        self.assertIn("2025", skipped["KURANAKH"])

    def test_skip_list_is_reset_between_calls(self):
        # Наблюдаемость не должна «залипать»: после полноценного вызова
        # список пропущенных пуст.
        ex.build_plan_rows(
            tcc_2025={"OLIMPIADA": 773.0}, r_bar={"OLIMPIADA": 0.6},
            production={"OLIMPIADA": {2025: 900.0}}, group_aisc=1437.0,
        )
        self.assertTrue(ex.LAST_SKIPPED_ASSETS)
        ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        self.assertEqual(ex.LAST_SKIPPED_ASSETS, [])

    def test_reconciliation_weights_are_within_year(self):
        # Каждый год увязывается отдельно: взвешенный по добыче AISC года
        # лежит ровно на цели этого года (невязка ≈ 0), а не на цели 2025-го.
        # Веса — внутри года, поэтому активы, у которых этот год уже вне
        # срока службы, в среднее не входят.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
        )
        by_year = {}
        for row in rows:
            by_year.setdefault(row["year"], []).append(row)
        self.assertEqual(len(by_year), 24)  # 2025–2048
        for year, year_rows in by_year.items():
            total = sum(r["production_koz"] for r in year_rows)
            weighted = sum(r["aisc"] * r["production_koz"] for r in year_rows) / total
            self.assertAlmostEqual(
                weighted, ex.group_aisc_for_year(year), places=6, msg=f"год {year}"
            )
        # В 2048-м остаётся только Наталка — год не «унаследовал» активы
        # с закончившимся сроком службы.
        self.assertEqual({r["asset"] for r in by_year[2048]}, {"NATALKA"})

    def test_reconciliation_target_tracks_the_passed_anchor(self):
        # Параметр group_aisc — точка подмены якоря, а не жёстко зашитая
        # константа: с якорем 1000 и AISC ровно на нём невязка нулевая.
        rows = ex.build_plan_rows(
            tcc_2025={a: 400.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1000.0,
        )
        for row in rows:
            self.assertAlmostEqual(
                row["aisc"], 1000.0 * ex.escalation_factor(2025, row["year"]), places=6
            )

    def test_plateau_holds_last_known_production(self):
        # У Олимпиады известен только 2025-й: остальные годы горизонта
        # держат его уровень, а не ноль.
        rows = ex.build_plan_rows(
            tcc_2025={"OLIMPIADA": 773.0}, r_bar={"OLIMPIADA": 0.6},
            production={"OLIMPIADA": {2025: 900.0}}, group_aisc=1437.0,
        )
        self.assertEqual([r["production_koz"] for r in rows], [900.0] * 10)

    def test_halt_window_zeroes_production_but_keeps_rows(self):
        # КОНСЕРВАЦИЯ — это НОЛЬ ДОБЫЧИ, А НЕ ОТСУТСТВИЕ СТРОКИ. Пропуск годов
        # сдвинул бы нумерацию лет плана и дисконтирование (см. комментарий к
        # HaltFrom/HaltTo в dcf/mine_plans_seed.go). Поэтому окно Вернинского
        # 2025–2028 (2024–2028 из Go-сида, пересечённое с планом) даёт четыре
        # строки с production_koz == 0, а всего строк по-прежнему 77.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
        )
        self.assertEqual(len(rows), 77)
        by_asset_year = {(r["asset"], r["year"]): r for r in rows}
        for year in (2025, 2026, 2027, 2028):
            row = by_asset_year[("VERNINSKOYE2", year)]
            self.assertEqual(row["production_koz"], 0.0, row)
        # Ровно четыре нулевых года Вернинского — окно ПРИЦЕЛЬНОЕ, а не «весь
        # горизонт»: строки существуют (проверено выше), но годы 2029+ дают
        # добычу.
        v_twelve = [
            r["year"] for r in rows
            if r["asset"] == "VERNINSKOYE2" and r["production_koz"] == 0.0
        ]
        self.assertEqual(sorted(v_twelve), [2025, 2026, 2027, 2028])

    def test_halt_window_is_precise_not_whole_horizon(self):
        # Год ПОСЛЕ окна у того же актива обязан нести РОВНО плато последнего
        # факта, а не просто «что-то ненулевое»: проверка `> 0` пропустила бы и
        # расширенное окно (обрезало бы часть производящих лет), и изменившееся
        # плато. Здесь у Вернинского известен единственный факт 2025-го = 230.8
        # (как в databook_polyus), поэтому 2029–2039 обязаны быть ровно 230.8:
        # 2025-й консервируется (ноль), а дальше плато тянет факт 2025-го.
        fact_koz = 230.8
        production = {a: {2025: 100.0} for a in ex.ASSETS}
        production["VERNINSKOYE2"] = {2025: fact_koz}
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=production,
            group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
        )
        by_asset_year = {(r["asset"], r["year"]): r for r in rows}
        # Первый год плана (2025) в окне — ноль; последний год окна (2028) — ноль.
        for year in (2025, 2028):
            self.assertEqual(
                by_asset_year[("VERNINSKOYE2", year)]["production_koz"], 0.0, year
            )
        # Год сразу ЗА окном и последний год горизонта — РОВНО плато, побитово.
        for year in (2029, 2030, 2039):
            self.assertEqual(
                by_asset_year[("VERNINSKOYE2", year)]["production_koz"], fact_koz, year
            )
        # И граница ПЕРЕД окном проверяется на другом активе: окно не должно
        # задевать чужие годы.
        self.assertEqual(by_asset_year[("KURANAKH", 2025)]["production_koz"], 100.0)
        self.assertEqual(by_asset_year[("KURANAKH", 2028)]["production_koz"], 100.0)

    def test_halt_window_intersects_the_horizon(self):
        """Окно обязано РЕАЛЬНО резать горизонт, а не быть молча инертным.

        Если окно уедет за пределы плана (например, сдвинется на 2040-е),
        `halted_years` вернёт пустое множество, и все прочие проверки консервации
        станут вакуумными: ноль нулевых строк — и тесты «нулевой добычи» пройдут
        на пустом множестве. Здесь проверяется САМ ФАКТ непустого пересечения и
        что объявленное окно содержит хотя бы один год плана.
        """
        window = ex.HALT_WINDOWS["VERNINSKOYE2"]
        first, last = window
        horizon_first = ex.PLAN_BASE_YEAR
        horizon_last = ex.PLAN_BASE_YEAR + ex.MINE_LIFE_YEARS["VERNINSKOYE2"] - 1
        self.assertLessEqual(first, horizon_last, ex.HALT_WINDOWS)
        self.assertGreaterEqual(last, horizon_first, ex.HALT_WINDOWS)
        intersection = ex.halted_years("VERNINSKOYE2")
        self.assertTrue(
            intersection,
            "окно консервации Вернинского не пересекает горизонт — сторож "
            "нулевой добычи стал бы вакуумным",
        )
        self.assertEqual(intersection, frozenset({2025, 2026, 2027, 2028}))

    def test_halt_year_rows_are_real_rows(self):
        # Строка консервации — настоящая строка, а не заглушка: tcc
        # эскалирован, aisc увязан (aisc >= tcc), capex_project/closure нулевые
        # по контракту. Нулевая ТОЛЬКО добыча.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
        )
        by_asset_year = {(r["asset"], r["year"]): r for r in rows}
        for year in (2025, 2026, 2027, 2028):
            row = by_asset_year[("VERNINSKOYE2", year)]
            self.assertGreater(row["tcc"], 0.0, row)
            self.assertGreater(row["aisc"], 0.0, row)
            self.assertGreaterEqual(row["aisc"], row["tcc"], row)
            self.assertEqual(row["capex_sustaining"], 0.0, row)
            # tcc эскалирован как в обычный год: 700 × множитель 2025→year.
            self.assertAlmostEqual(
                row["tcc"],
                700.0 * ex.escalation_factor(2025, year),
                places=6,
            )

    def test_halt_removes_only_that_asset_from_the_year_weights(self):
        # Взвешивание года: у консервируемого актива вес 0, поэтому он не
        # участвует в среднем, а остальные активы продолжают давать веса. Это
        # прямое следствие нулевой добычи — отдельная ветка весов не нужна.
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
        )
        year_2026 = [r for r in rows if r["year"] == 2026]
        self.assertEqual(
            [r["asset"] for r in year_2026], sorted(
                [r["asset"] for r in year_2026], key=ex.ASSETS.index
            ),
        )
        producers = [r for r in year_2026 if r["production_koz"] > 0.0]
        halted = [r for r in year_2026 if r["production_koz"] == 0.0]
        self.assertEqual([r["asset"] for r in halted], ["VERNINSKOYE2"])
        self.assertEqual(len(producers), 4)
        # Увязка 2026 сходится к цели года ровно по производящим активам.
        total = sum(r["production_koz"] for r in producers)
        weighted = sum(r["aisc"] * r["production_koz"] for r in producers) / total
        self.assertAlmostEqual(
            weighted, ex.group_aisc_for_year(2026), places=6
        )

    def test_all_assets_halted_in_a_year_is_a_named_error_not_a_crash(self):
        # Случай «в году консервируют ВСЕ активы» не должен делить на ноль:
        # вырожденные веса ловятся ДО деления и поднимают ValueError с номером
        # года. Здесь окно временно расширено на все пять активов через
        # подмену HALT_WINDOWS — штатные данные этого не содержат, но защита
        # проверяема.
        single_year = {2025: 100.0}
        production = {a: dict(single_year) for a in ex.ASSETS}
        original = ex.HALT_WINDOWS
        try:
            ex.HALT_WINDOWS = {a: (2025, 2025) for a in ex.ASSETS}
            with self.assertRaises(ValueError) as ctx:
                ex.build_plan_rows(
                    tcc_2025={a: 700.0 for a in ex.ASSETS},
                    r_bar={a: 0.6 for a in ex.ASSETS},
                    production=production,
                    group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ,
                )
            self.assertIn("2025", str(ctx.exception))
        finally:
            ex.HALT_WINDOWS = original

    def test_halted_years_helper_clips_to_horizon(self):
        # Окно Go-сида у Вернинского — 2024–2028, но план начинается в 2025-м,
        # поэтому helper отдаёт РОВНО пересечение с горизонтом: 2025–2028 без
        # 2024. У остальных активов окон нет.
        self.assertEqual(ex.halted_years("VERNINSKOYE2"), frozenset({2025, 2026, 2027, 2028}))
        for asset in ("OLIMPIADA", "BLAGODATNOYE", "NATALKA", "KURANAKH"):
            self.assertEqual(ex.halted_years(asset), frozenset(), asset)

    def test_base_year_without_production_is_an_error(self):
        # Молчаливая обрезка базового года сдвинула бы весь горизонт.
        with self.assertRaises(ValueError):
            ex.build_plan_rows(
                tcc_2025={a: 700.0 for a in ex.ASSETS},
                r_bar={a: 0.6 for a in ex.ASSETS},
                production={a: {2026: 100.0} for a in ex.ASSETS},
                group_aisc=1400.0,
            )

    def test_missing_tcc_anchor_is_an_error(self):
        with self.assertRaises(ValueError):
            ex.build_plan_rows(
                tcc_2025={"OLIMPIADA": 773.0},
                r_bar={a: 0.6 for a in ex.ASSETS},
                production=self._full_production(),
                group_aisc=1400.0,
            )

    def test_raising_call_does_not_leak_skip_list(self):
        # F1. Успешный вызов наполняет LAST_SKIPPED_ASSETS, а следующий за ним
        # ПАДАЮЩИЙ обязан оставить его пустым, а не унаследованным: иначе
        # вызывающий в `try/except` отнёс бы чужой пропуск к упавшему вызову.
        ex.build_plan_rows(
            tcc_2025={"OLIMPIADA": 773.0}, r_bar={"OLIMPIADA": 0.6},
            production={"OLIMPIADA": {2025: 900.0}}, group_aisc=1437.0,
        )
        self.assertTrue(ex.LAST_SKIPPED_ASSETS)  # состояние для проверки утечки

        # Наталка есть в добыче, но якоря TCC у неё нет — валидация падает
        # ПОСЛЕ отбора активов, то есть на том самом месте, где раньше
        # стояла запись глобала.
        production = self._full_production()
        with self.assertRaises(ValueError):
            ex.build_plan_rows(
                tcc_2025={a: 700.0 for a in ex.ASSETS if a != "NATALKA"},
                r_bar={a: 0.6 for a in ex.ASSETS},
                production=production,
                group_aisc=1400.0,
            )
        self.assertEqual(ex.LAST_SKIPPED_ASSETS, [])
        self.assertEqual(ex.LAST_YEAR_RECONCILIATION, {})

    def test_missing_r_bar_for_an_active_asset_is_an_error(self):
        # Тот же класс падения, что и выше, но по второй ветке валидации.
        with self.assertRaises(ValueError):
            ex.build_plan_rows(
                tcc_2025={a: 700.0 for a in ex.ASSETS},
                r_bar={a: 0.6 for a in ex.ASSETS if a != "NATALKA"},
                production=self._full_production(),
                group_aisc=1400.0,
            )

    def test_reconciliation_is_reported_per_year(self):
        # F2. `build_plan_rows` возвращает только строки, поэтому невязка и m
        # наблюдаемы через LAST_YEAR_RECONCILIATION — иначе прижатый к нулю m
        # (недостижимая цель) прошёл бы молча.
        self._rows_with_full_production(group_aisc=ex.GROUP_AISC_2025_USD_PER_OZ)
        reported = ex.LAST_YEAR_RECONCILIATION
        self.assertEqual(sorted(reported), list(range(2025, 2049)))
        for year, (target, residual, m) in reported.items():
            self.assertAlmostEqual(target, ex.group_aisc_for_year(year), places=9)
            self.assertAlmostEqual(residual, 0.0, places=6)  # цель достижима
            self.assertGreater(m, 0.0)  # прижим не сработал

    def test_unreachable_group_target_surfaces_a_clamped_m(self):
        # F2. Цель вчетверо ниже пола взвешенного TCC: `reconcile` прижимает m
        # к нулю, и строки при этом структурно безупречны (aisc == tcc).
        # Единственный сигнал — ненулевой остаток и m == 0.
        rows = self._rows_with_full_production(group_aisc=100.0)
        reported = ex.LAST_YEAR_RECONCILIATION
        self.assertEqual(sorted(reported), list(range(2025, 2049)))
        for year, (target, residual, m) in reported.items():
            self.assertEqual(m, 0.0, f"год {year}: m не прижат")
            self.assertGreater(residual, 0.0, f"год {year}")
        # Строки при этом проходят структурную проверку — вот почему нужен
        # отдельный сигнал.
        for row in rows:
            self.assertAlmostEqual(row["aisc"], row["tcc"], places=9)
            self.assertGreaterEqual(row["aisc"], row["tcc"], row)
        # Невязка положительна и равна отрыву пола TCC от цели: пол взвешенного
        # TCC в 2025-м — это 700 USD/oz, цель — 100.
        target_2025, residual_2025, _ = reported[2025]
        self.assertAlmostEqual(target_2025, 100.0, places=9)
        self.assertAlmostEqual(residual_2025, 600.0, places=6)

    def test_residual_exposes_the_overshoot_side_too(self):
        # Та же наблюдаемость с другой стороны: недостижимо ВЫСОКАЯ цель даёт
        # положительный m (он не ограничен сверху) и отрицательную невязку
        # только если увязка не сошлась; здесь цель достижима ровно, поэтому
        # остаток ~0 — тест фиксирует, что ветка не «залипает» на прижим.
        self._rows_with_full_production(group_aisc=5000.0)
        for year, (_target, residual, m) in ex.LAST_YEAR_RECONCILIATION.items():
            self.assertAlmostEqual(residual, 0.0, places=6, msg=f"год {year}")
            self.assertGreater(m, 1.0, f"год {year}")

    def test_reconciliation_report_is_reset_between_calls(self):
        # Наблюдаемость не должна «залипать», как и список пропусков: у вызова,
        # в котором нет ни одного активного года, словарь невязок пуст, а
        # список пропусков перезаписан целиком (а не дополнен).
        self._rows_with_full_production()
        self.assertTrue(ex.LAST_YEAR_RECONCILIATION)
        ex.build_plan_rows(
            tcc_2025={}, r_bar={}, production={},
            group_aisc=1400.0,
        )
        self.assertEqual(ex.LAST_YEAR_RECONCILIATION, {})
        self.assertEqual(len(ex.LAST_SKIPPED_ASSETS), len(ex.ASSETS))

    def test_rows_are_sorted_by_asset_then_year(self):
        rows = ex.build_plan_rows(
            tcc_2025={a: 700.0 for a in ex.ASSETS},
            r_bar={a: 0.6 for a in ex.ASSETS},
            production=self._full_production(),
            group_aisc=1400.0,
        )
        keys = [(ex.ASSETS.index(r["asset"]), r["year"]) for r in rows]
        self.assertEqual(keys, sorted(keys))
        self.assertEqual(rows[0]["asset"], "OLIMPIADA")
        self.assertEqual(rows[0]["year"], 2025)


def _sample_rows(count: int = 1) -> list:
    """Минимальные плановые строки для тестов эмиттеров.

    Значения намеренно «кривые» (не целые доли), чтобы `repr`-формат чисел
    был заметен: округление до копеек прошло бы незамеченным на ровных 900.0.

    Год циклится по ПЛАНОВОМУ горизонту `PLAN_BASE_YEAR … 2048`, а не растёт
    безгранично: `write_csv` считает множитель эскалации, и год вне словаря
    `ESCALATION` законно поднимает `ValueError` (см. `escalation_factor`) —
    тестовые данные не должны упираться в эту защиту, когда проверяется
    формат записи, а не она сама.
    """
    first_year = ex.PLAN_BASE_YEAR
    last_year = max(ex.ESCALATION)
    span = last_year - first_year + 1
    return [
        {
            "asset": "OLIMPIADA",
            "year": first_year + index % span,
            "production_koz": 1441.3,
            "tcc": 773.0,
            "aisc": 1300.5,
            "capex_sustaining": 474.25,
        }
        for index in range(count)
    ]


class TestEmit(unittest.TestCase):
    """Сериализация плановых строк: SQL-сид и CSV.

    Тесты брифа — авторитетны и приведены дословно. Добавлены краевые проверки:
    отклонение не-конечных чисел, размер батча по умолчанию, побайтовый
    круг-трип строки VALUES (ловит перестановку колонок), шапка с датой и
    множителями, отсутствие локализованных разделителей в CSV.
    """

    # Имя таблицы и ПОЛНЫЙ список колонок — из DDL `dcf/schema.go`. Порядок
    # здесь не косметика: ClickHouse примет перестановку однотипных Float64 и
    # молча запишет tcc в aisc, поэтому колонки и значения пинуются вместе.
    COLUMNS = (
        "company", "asset", "year", "production_koz", "grade_gpt",
        "tcc", "aisc", "capex_sustaining", "capex_project", "closure_costs",
    )

    def test_null_grade_and_zero_project_capex(self):
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 474.0}]
        sql = "\n".join(ex.sql_batches(rows))
        self.assertIn("NULL", sql)
        self.assertIn("'OLIMPIADA'", sql)
        self.assertIn(",0,0)", sql)  # capex_project, closure_costs

    def test_all_ten_columns_present(self):
        sql = "\n".join(ex.sql_batches([{
            "asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
            "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 474.0}]))
        for col in self.COLUMNS:
            self.assertIn(col, sql)

    def test_column_list_order_is_exact(self):
        # ПОЛНЫЙ список колонок в INSERT, дословно и в порядке DDL. Именно
        # подстрока с пробелами и скобками: подстрочного вхождения имени мало,
        # перестановку `tcc`/`aisc` оно не поймает.
        self.assertEqual(
            ex.MINE_PLANS_COLUMNS,
            self.COLUMNS,
        )
        self.assertIn("INSERT INTO mine_plans (" + ", ".join(self.COLUMNS) + ")", ex.sql_batches([{
            "asset": "A", "year": 2025, "production_koz": 1.0,
            "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}])[0])

    def test_batching_respects_size(self):
        rows = [{"asset": "A", "year": 2025, "production_koz": 1.0,
                 "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}] * 1200
        self.assertEqual(len(ex.sql_batches(rows, batch_size=500)), 3)

    def test_default_batch_size_yields_one_statement_for_77_rows(self):
        # 77 — реальный размер плана (10+13+24+15+15), он же модульный инвариант
        # `sum(MINE_LIFE_YEARS.values())`. Батч по умолчанию (500) обязан
        # уложить его в ОДИН INSERT: иначе `make ch-sql` писал бы лишние
        # запросы, а поведение по умолчанию не было бы проверено вовсе.
        self.assertEqual(sum(ex.MINE_LIFE_YEARS.values()), 77)
        self.assertEqual(len(ex.sql_batches(_sample_rows(77))), 1)

    def test_every_batch_is_a_complete_insert_statement(self):
        # Каждый батч самодостаточен: заголовок INSERT, хотя бы одна строка,
        # завершающая точка с запятой. Склейка батчей без разделителя — самая
        # частая поломка при правке эмиттера.
        for batch in ex.sql_batches(_sample_rows(7), batch_size=3):
            self.assertTrue(batch.startswith(
                "INSERT INTO mine_plans (" + ", ".join(self.COLUMNS) + ") VALUES"))
            self.assertTrue(batch.endswith(";"), batch[-40:])
            self.assertTrue(batch.rstrip().endswith(");"))

    def test_nan_is_rejected(self):
        with self.assertRaises(ValueError):
            ex.sql_literal(float("nan"))

    def test_inf_is_rejected(self):
        # Бесконечность ClickHouse принял бы как Float64, и NPV уехал бы в inf
        # без единой ошибки. Отклоняются ОБА знака.
        for bad in (float("inf"), float("-inf")):
            with self.subTest(value=bad):
                with self.assertRaises(ValueError):
                    ex.sql_literal(bad)

    def test_nan_in_a_row_is_rejected(self):
        # Отклонение должно срабатывать и на пути через `sql_batches`, а не
        # только на прямом вызове: строка с NaN не должна попасть в сид.
        rows = _sample_rows(1)
        rows[0]["aisc"] = float("nan")
        with self.assertRaises(ValueError):
            ex.sql_batches(rows)

    def test_value_tuple_is_exact(self):
        # КРУГ-ТРИП: точный текст кортежа VALUES. Тест существует ради
        # перестановки колонок в будущем — порядок ЗНАЧЕНИЙ закреплён здесь
        # побайтово, а не только список имён.
        #
        # company='PLZL', asset='OLIMPIADA', year=2025, production_koz=1441.3,
        # grade_gpt=NULL, tcc=773.0, aisc=1300.5, capex_sustaining=474.25,
        # capex_project=0, closure_costs=0.
        expected = "('PLZL','OLIMPIADA',2025,1441.3,NULL,773.0,1300.5,474.25,0,0)"
        self.assertIn(expected, ex.sql_batches(_sample_rows(1))[0])

    def test_sql_literal_forms(self):
        self.assertEqual(ex.sql_literal(1), "1")
        self.assertEqual(ex.sql_literal(1.5), "1.5")
        self.assertEqual(ex.sql_literal("TI"), "'TI'")
        self.assertEqual(ex.sql_literal(None), "NULL")
        # Тик в имени экранируется удвоением: литерал не должен разрывать
        # строку запроса.
        self.assertEqual(ex.sql_literal("O'HARA"), "'O''HARA'")
        self.assertEqual(ex.sql_literal(True), "1")
        self.assertEqual(ex.sql_literal(False), "0")

    def test_sql_literal_keeps_full_float_precision(self):
        # Полная точность `repr`, а не усечение форматом: 0.1 + 0.2 — это
        # 0.30000000000000004, и округление «до красивых» внесло бы ошибку,
        # которую потом не отличить от ошибки модели.
        self.assertEqual(ex.sql_literal(0.1 + 0.2), repr(0.1 + 0.2))
        self.assertEqual(ex.sql_literal(1.0 / 3.0), repr(1.0 / 3.0))

    def test_sql_batches_rejects_nonpositive_batch_size(self):
        # batch_size = 0 — это бесконечный цикл (range(0, n, 0) бросает
        # ValueError, но тогда сообщение указывало бы внутрь реализации);
        # отрицательный — тоже ошибка вызывающей стороны.
        for bad in (0, -1):
            with self.subTest(batch_size=bad):
                with self.assertRaises(ValueError):
                    ex.sql_batches(_sample_rows(3), batch_size=bad)

    def test_empty_rows_give_no_statements(self):
        # Пустой план — это НЕ пустой INSERT со синтаксически неверным
        # «VALUES;». Ни одного оператора: файл останется с одной шапкой.
        self.assertEqual(ex.sql_batches([]), [])
        self.assertEqual(ex.sql_batches([], batch_size=1), [])

    # --- Запись файлов -------------------------------------------------------

    META = {
        "command": "make dcf-aisc",
        "calibration_years": (2019, 2020, 2021),
        "m": 1.5099204817590248,
        "residual": -2.2737367544323206e-13,
    }

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.dir = Path(self._tmp.name)

    def test_write_sql_prepends_a_header_then_all_batches(self):
        path = self.dir / "seed.sql"
        rows = _sample_rows(1200)
        ex.write_sql(path, rows, self.META)
        text = path.read_text(encoding="utf-8")

        # Шапка — КОММЕНТАРИЙ и стоит ПЕРВОЙ: ClickHouse не должен выполнить
        # прозу как SQL.
        self.assertTrue(text.startswith("--"), text[:40])
        self.assertIn("make dcf-aisc", text)
        for year in (2019, 2020, 2021):
            self.assertIn(str(year), text)
        self.assertIn(repr(self.META["m"]), text)
        self.assertIn(repr(self.META["residual"]), text)
        # Дата генерации — ISO-форма (YYYY-MM-DD), а не локальная проза.
        from datetime import date as _date
        self.assertIn(_date.today().isoformat(), text)

        # Данные — ровно те же операторы, что у `sql_batches`, и все строки на
        # месте: 1200 / 500 = 3 оператора.
        data = text[text.index("INSERT INTO"):]
        self.assertEqual(data, "\n".join(ex.sql_batches(rows)))
        self.assertEqual(len(ex.sql_batches(rows)), 3)

    def test_write_sql_names_all_five_assets(self):
        rows = [
            {"asset": asset, "year": 2025, "production_koz": 1.0,
             "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}
            for asset in ex.ASSETS
        ]
        path = self.dir / "seed.sql"
        ex.write_sql(path, rows, self.META)
        text = path.read_text(encoding="utf-8")
        for asset in ex.ASSETS:
            self.assertIn(f"'{asset}'", text)

    def test_write_csv_has_header_then_one_row_per_plan_row(self):
        path = self.dir / "reconciled.csv"
        rows = _sample_rows(77)
        ex.write_csv(path, rows)
        with path.open(newline="", encoding="utf-8") as handle:
            parsed = list(csv.DictReader(handle))
        self.assertEqual(len(parsed), 77)
        with path.open(newline="", encoding="utf-8") as handle:
            header = next(csv.reader(handle))
        # Те же десять колонок, что и в SQL.
        self.assertEqual(header[:len(self.COLUMNS)], list(self.COLUMNS))
        # Плюс справочные колонки для глазной сверки: клин r̄, множитель
        # эскалации и рублёвая справка (в модель не идут).
        for extra in ("r_bar", "escalation_factor", "aisc_per_oz_rub"):
            self.assertIn(extra, header)

    def test_write_csv_numbers_are_plain_not_locale_formatted(self):
        # Ни запятых-разделителей, ни пробелов внутри чисел: файл обязан
        # открываться `csv.DictReader` и парситься в float без правок.
        path = self.dir / "reconciled.csv"
        ex.write_csv(path, _sample_rows(1))
        with path.open(newline="", encoding="utf-8") as handle:
            parsed = list(csv.DictReader(handle))
        self.assertEqual(float(parsed[0]["production_koz"]), 1441.3)
        self.assertEqual(float(parsed[0]["tcc"]), 773.0)
        self.assertEqual(float(parsed[0]["aisc"]), 1300.5)
        self.assertEqual(float(parsed[0]["capex_sustaining"]), 474.25)
        self.assertEqual(float(parsed[0]["year"]), 2025)
        # grade — пустая ячейка: NULL выражается отсутствием значения, а не
        # строкой "nan" и не измеренным нулём.
        self.assertEqual(parsed[0]["grade_gpt"], "")
        self.assertEqual(parsed[0]["company"], "PLZL")

    def test_write_csv_keeps_r_bar_and_escalation_values(self):
        # Справочные колонки несут РЕАЛЬНЫЕ значения, а не константы: r_bar
        # приходит из калибровки, множитель — из `escalation_factor`.
        rows = _sample_rows(2)
        for row in rows:
            row["r_bar"] = 0.626
        path = self.dir / "reconciled.csv"
        ex.write_csv(path, rows)
        with path.open(newline="", encoding="utf-8") as handle:
            parsed = list(csv.DictReader(handle))
        self.assertAlmostEqual(float(parsed[0]["r_bar"]), 0.626, places=9)
        self.assertAlmostEqual(
            float(parsed[1]["escalation_factor"]),
            ex.escalation_factor(2025, 2026),
            places=9,
        )
        self.assertGreater(float(parsed[1]["aisc_per_oz_rub"]), 0.0)

    def test_writers_create_missing_parent_directories(self):
        # Каталог `sql/` может отсутствовать в свежем клоне; эмиттер обязан его
        # создать, а не падать с FileNotFoundError на середине записи.
        path = self.dir / "nested" / "deeper" / "seed.sql"
        ex.write_sql(path, _sample_rows(1), self.META)
        self.assertTrue(path.exists())
        csv_path = self.dir / "nested" / "deep2" / "reconciled.csv"
        ex.write_csv(csv_path, _sample_rows(1))
        self.assertTrue(csv_path.exists())

    def test_no_secrets_in_generated_artifacts(self):
        # Артефакты — публикуемые файлы: ни пароля, ни строки подключения с
        # basic auth в них попасть не должно, даже если окружение задано.
        self.addCleanup(os.environ.pop, "CLICKHOUSE_PASSWORD", None)
        os.environ["CLICKHOUSE_PASSWORD"] = "sup3r-s3cret-pw"
        sql_path = self.dir / "seed.sql"
        csv_path = self.dir / "reconciled.csv"
        ex.write_sql(sql_path, _sample_rows(3), self.META)
        ex.write_csv(csv_path, _sample_rows(3))
        for path in (sql_path, csv_path):
            text = path.read_text(encoding="utf-8")
            self.assertNotIn("sup3r-s3cret-pw", text)
            self.assertNotIn("Basic ", text)


# Полная калибровка (все пять активов) — база для тестов валидации: `validate`
# обязан ругаться на ОТСУТСТВУЮЩИЙ актив, поэтому неполный `cal` в тестах про
# строки дал бы шум из пяти проблем вместо одной проверяемой.
_FULL_CAL = {asset: {"r_bar": 0.6} for asset in ex.ASSETS}


def _valid_row(asset: str = "OLIMPIADA", year: int = 2025, **overrides) -> dict:
    """Строка плана, которая ЗАВЕДОМО проходит `validate`.

    Значения совпадают с брифа: `tcc=773`, `aisc=1300` (клин 0.68 при r̄ 0.68 —
    база 2025 не эскалирована). Тесты портят по одному полю через `overrides`,
    поэтому причина проблемы всегда одна и её видно в тексте.

    `capex_sustaining` = 0 — НЕ «нулевое значение для примера», а КОНТРАКТ
    валидной строки: клин поддержания уже внутри `aisc`, и ненулевой отток
    списал бы его дважды (`validate` это и проверяет). Пример валидной строки,
    несущий ненулевой capex, документировал бы будущему читателю ровно тот
    дефект, который здесь исправлен.
    """
    row = {
        "asset": asset,
        "year": year,
        "production_koz": 900.0,
        "tcc": 773.0,
        "aisc": 1300.0,
        "capex_sustaining": 0.0,
    }
    row.update(overrides)
    return row


def _valid_rows(asset: str = "OLIMPIADA", **overrides) -> list:
    """ПОЛНЫЙ набор строк (по одной на каждый из пяти `ASSETS`).

    База для проверок, которые НЕ про пропущенный актив: `validate` обязан
    называть отсутствующие рудники, поэтому набор из одной строки давал бы в
    каждой такой проверке ещё четыре проблемы и прятал бы проверяемую. Порча
    (`overrides`) применяется только к строке названного актива — у остальных
    строк значения заведомо валидны.
    """
    return [
        _valid_row(a, **overrides) if a == asset else _valid_row(a)
        for a in ex.ASSETS
    ]


class TestValidate(unittest.TestCase):
    """Guard'ы итоговой таблицы плана.

    Тесты брифа (`test_accepts_consistent_rows`, `test_flags_aisc_below_tcc`,
    `test_flags_year_outside_window`, `test_flags_missing_asset`) приведены
    дословно. Добавлены проверки остатка ПО ВЕЛИЧИНЕ (правило контроллера:
    `residual = факт − цель`, поэтому недостижимо НИЗКАЯ цель даёт
    положительный остаток и проверка по знаку промахнулась бы), отрицательной
    добычи, диапазона `r_bar` и границы `2100` включительно.
    """

    def test_accepts_consistent_rows(self):
        # Значение `capex_sustaining` приведено к 0.0 против брифа: строка с
        # 474.0 «согласованной» больше не является — клин поддержания уже внутри
        # `aisc`, и ненулевой отток списал бы его дважды (это и проверяет
        # `validate`). НАМЕРЕНИЕ теста сохранено: согласованный набор строк
        # по-прежнему даёт пустой список проблем.
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 1300.0, "capex_sustaining": 0.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertEqual(problems, [])

    def test_flags_aisc_below_tcc(self):
        rows = [{"asset": "OLIMPIADA", "year": 2025, "production_koz": 900.0,
                 "tcc": 773.0, "aisc": 700.0, "capex_sustaining": 0.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertTrue(any("aisc" in p for p in problems))

    def test_flags_year_outside_window(self):
        rows = [{"asset": "OLIMPIADA", "year": 2100, "production_koz": 1.0,
                 "tcc": 1.0, "aisc": 2.0, "capex_sustaining": 0.0}]
        problems = ex.validate(rows, {"OLIMPIADA": {"r_bar": 0.68}}, {"residual": 0.0})
        self.assertTrue(any("2100" in p for p in problems))

    def test_flags_missing_asset(self):
        problems = ex.validate([], {a: {"r_bar": 0.6} for a in ex.ASSETS}, {"residual": 0.0})
        self.assertTrue(any("OLIMPIADA" in p for p in problems))

    def test_full_row_set_from_all_assets_passes(self):
        # Контроль обратного: полный набор строк по всем пяти активам при
        # согласованных клиньях даёт ПУСТОЙ список — иначе тесты выше проходили
        # бы на функции, которая ругается всегда.
        self.assertEqual(ex.validate(_valid_rows(), _FULL_CAL, {"residual": 0.0}), [])

    def test_flags_nonzero_sustaining_capex(self):
        # РАНТАЙМ-ЗАЩИТА ОТ ДВОЙНОГО СЧЁТА. Ненулевой `capex_sustaining` — это
        # ровно тот дефект, из-за которого был занижен каждый NAV (клин уже
        # внутри `aisc`, а dcf/model.go вычитает и его, и capex_sustaining).
        # Юнит-тесты эмиттера ловят регрессию в CI, но `main()` обязан поймать
        # её и на прогоне — поэтому проверка живёт в `validate`, а этот тест
        # падал на коде ДО правки (тогда 999.0 проходило молча).
        problems = ex.validate(
            _valid_rows(capex_sustaining=999.0), _FULL_CAL, {"residual": 0.0}
        )
        self.assertTrue(problems, "ненулевой capex_sustaining не помечен")
        self.assertTrue(any("capex_sustaining" in p for p in problems), problems)
        self.assertTrue(any("999" in p for p in problems), problems)

    def test_flags_nonzero_project_capex(self):
        # Проектный capex у пяти рудников LOM тоже контрактный ноль: проекта
        # развития нет, и отдельный отток по нему так же занизил бы NPV.
        problems = ex.validate(
            _valid_rows(capex_project=500.0), _FULL_CAL, {"residual": 0.0}
        )
        self.assertTrue(any("capex_project" in p for p in problems), problems)

    def test_accepts_zero_capex_and_zero_project_capex_explicitly(self):
        # Граница обратной стороны: ЯВНЫЕ нули (как их пишет build_plan_rows)
        # проходят. Тест нужен, чтобы проверка `!= 0` не превратилась в
        # «ругаться на отсутствие ключа»: строки без ключей тоже валидны.
        rows = _valid_rows(capex_sustaining=0.0, capex_project=0)
        self.assertEqual(ex.validate(rows, _FULL_CAL, {"residual": 0.0}), [])
        stripped = _valid_rows()
        for row in stripped:
            row.pop("capex_sustaining", None)
        self.assertEqual(ex.validate(stripped, _FULL_CAL, {"residual": 0.0}), [])

    def test_flags_negative_production(self):
        rows = _valid_rows(production_koz=-1.0)
        problems = ex.validate(rows, _FULL_CAL, {"residual": 0.0})
        self.assertTrue(any("добыч" in p for p in problems), problems)

    def test_flags_missing_asset_named_in_problems(self):
        # Отсутствие КАЖДОГО канонического актива называется поимённо: пропуск
        # Веринского не должен выглядеть как пропуск Олимпиады.
        problems = ex.validate([_valid_row("OLIMPIADA")], _FULL_CAL, {"residual": 0.0})
        for asset in ex.ASSETS:
            if asset == "OLIMPIADA":
                continue
            self.assertTrue(any(asset in p for p in problems), (asset, problems))

    def test_flags_r_bar_outside_range(self):
        for bad in (0.29, 1.01):
            with self.subTest(r_bar=bad):
                cal = dict(_FULL_CAL)
                cal["OLIMPIADA"] = {"r_bar": bad}
                problems = ex.validate(_valid_rows(), cal, {"residual": 0.0})
                self.assertTrue(any("OLIMPIADA" in p for p in problems), problems)

    def test_accepts_r_bar_at_range_bounds(self):
        # Границы 0.3 и 1.0 ВКЛЮЧИТЕЛЬНЫ: диапазон назван закрытым «0.3–1.0», и
        # расширить отказ на границу значило бы ругаться на законное значение.
        for bound in (0.3, 1.0):
            with self.subTest(r_bar=bound):
                cal = dict(_FULL_CAL)
                cal["OLIMPIADA"] = {"r_bar": bound}
                self.assertEqual(
                    ex.validate(_valid_rows(), cal, {"residual": 0.0}), []
                )

    def test_residual_below_one_percent_of_group_aisc_passes(self):
        # 1 % от 1437 = 14.37 USD/oz. Остаток заведомо ниже порога.
        rows = _valid_rows()
        self.assertEqual(
            ex.validate(
                rows, _FULL_CAL,
                {"residual": 1.0, "group_aisc": ex.GROUP_AISC_2025_USD_PER_OZ},
            ),
            [],
        )

    def test_residual_at_or_above_one_percent_is_flagged(self):
        rows = _valid_rows()
        problems = ex.validate(
            rows, _FULL_CAL,
            {"residual": 0.01 * ex.GROUP_AISC_2025_USD_PER_OZ},
        )
        self.assertTrue(any("остаток" in p.lower() or "невяз" in p.lower()
                            for p in problems), problems)

    def test_positive_residual_is_flagged_by_magnitude(self):
        # ПРАВИЛО КОНТРОЛЛЕРА 1. `residual = взвешенный(факт) − цель` (Task 6),
        # поэтому ЦЕЛЬ СЛИШКОМ НИЗКАЯ (недостижимая, `m` прижат к нулю) даёт
        # ПОЛОЖИТЕЛЬНЫЙ остаток — это ровно ветвь `test_unreachable_group_target_…`
        # из Task 8. Проверка по направлению («отрицательный остаток — плохо»)
        # пропустила бы её целиком; проверка по |·| обязана поймать.
        self.assertGreater(600.0, 0.01 * ex.GROUP_AISC_2025_USD_PER_OZ)
        problems = ex.validate(_valid_rows(), _FULL_CAL, {"residual": 600.0})
        self.assertTrue(problems, "недостижимо низкая цель не помечена")
        self.assertTrue(any("600" in p for p in problems), problems)

    def test_negative_residual_is_flagged_too(self):
        # Симметрия: недостижимо ВЫСОКАЯ цель даёт отрицательный остаток, и он
        # тоже обязан быть помечен — иначе правило оказалось бы односторонним.
        problems = ex.validate(_valid_rows(), _FULL_CAL, {"residual": -600.0})
        self.assertTrue(any("600" in p for p in problems), problems)

    def test_missing_residual_key_is_not_a_crash(self):
        # `meta` может не нести остатка вовсе (например, план не строился).
        # Отсутствие ключа — не ошибка валидатора: он проверяет то, что ему дали.
        self.assertEqual(ex.validate(_valid_rows(), _FULL_CAL, {}), [])

    def test_problems_are_russian_and_many_rows_are_all_reported(self):
        # Проблема не останавливает обход: каждая битая строка обязана попасть в
        # список, иначе таблицу пришлось бы чинить итеративно по одной строке.
        rows = (
            _valid_rows(year=2100)
            + _valid_rows(aisc=1.0)
            + _valid_rows(production_koz=-5.0)
        )
        problems = ex.validate(rows, _FULL_CAL, {"residual": 0.0})
        self.assertGreaterEqual(len(problems), 3)
        for problem in problems:
            self.assertRegex(problem, r"[А-Яа-яЁё]")


class TestMainGuards(unittest.TestCase):
    """Границы `main()` и запрет испорченной фикстуры в конвейере.

    Живой ClickHouse тестам не нужен: `main()` здесь не вызывается целиком,
    проверяется только то, что можно проверить офлайн — состав набора фикстур и
    явный отказ от `*_corrupt.pdf`.
    """

    def test_corrupt_fixture_exists(self):
        # Предпосылка проверки ниже: испорченная фикстура действительно лежит в
        # каталоге, и «её нет в наборе» означает фильтр, а не отсутствие файла.
        corrupt = ex.FIXTURE_DIR / "polyus_pr_4q_fy2021_corrupt.pdf"
        self.assertTrue(corrupt.exists(), corrupt)

    def test_main_discovery_excludes_corrupt_fixture(self):
        # ПРАВИЛО КОНТРОЛЛЕРА 2. `main()` собирает набор через
        # `discover_fixtures()` — тот же путь, что `build_series()` по умолчанию,
        # и испорченный файл в него попасть не может. Проверяется НАБОР, а не
        # намерение: правка фильтра обязана уронить этот тест.
        fixtures = ex.discover_fixtures()
        names = {path.name for path in fixtures.values()}
        self.assertNotIn("polyus_pr_4q_fy2021_corrupt.pdf", names)
        self.assertEqual(sorted(fixtures), [2018, 2019, 2020, 2021])

    def test_main_refuses_explicitly_passed_corrupt_fixture(self):
        # Второй слой защиты: даже если испорченный путь подсунут явно,
        # `main()` обязан отказаться, а не тихо скормить его калибровке.
        corrupt = {
            2020: ex.FIXTURE_DIR / "polyus_pr_4q_fy2020.pdf",
            2021: ex.FIXTURE_DIR / "polyus_pr_4q_fy2021_corrupt.pdf",
        }
        with self.assertRaises(ex.CorruptFixtureError) as ctx:
            ex.assert_no_corrupt_fixtures(corrupt)
        self.assertIn("corrupt", str(ctx.exception))

    def test_assert_no_corrupt_fixtures_accepts_clean_set(self):
        ex.assert_no_corrupt_fixtures(ex.discover_fixtures())

    def test_calibration_years_are_the_discovered_reporting_years(self):
        # Годы калибровки не шире набора релизов: `build_series` читает именно
        # эти четыре файла, и 2019–2021 — подмножество их отчётных лет.
        available = set(ex.discover_fixtures())
        for year in ex.CALIBRATION_YEARS:
            self.assertIn(year, available)

    def test_tcc_2025_anchors_match_go_seed(self):
        # ДОСЛОВНОЕ СООТВЕТСТВИЕ `dcf/mine_plans_seed.go` (polyusAssetPlans.TCC).
        # SQL-сид ложится ПОВЕРХ Go-сида тем же ключом (company, asset, year), и
        # расхождение в TCC переписало бы затраты плана при неизменном AISC-клоне
        # — относительный клин разъехался бы с откалиброванным МОЛЧА.
        self.assertEqual(ex.TCC_2025_USD_PER_OZ, {
            "OLIMPIADA": 773.0,
            "BLAGODATNOYE": 691.0,
            "NATALKA": 671.0,
            "VERNINSKOYE2": 735.0,
            "KURANAKH": 925.0,
        })
        self.assertEqual(sorted(ex.TCC_2025_USD_PER_OZ), sorted(ex.ASSETS))

    def test_residual_tolerance_is_one_percent_share(self):
        self.assertEqual(ex.RESIDUAL_TOLERANCE_SHARE, 0.01)

    def test_plan_year_window_is_half_open(self):
        self.assertEqual((ex.PLAN_YEAR_MIN, ex.PLAN_YEAR_MAX_EXCLUSIVE), (2000, 2100))


class TestMainRun(unittest.TestCase):
    """`main()` целиком, но БЕЗ сети: транспорт ClickHouse подменён.

    Фикстуры PDF читаются по-настоящему (это offline и дёшево), а единственный
    внешний вызов — `read_actual_production` — заменяется словарём. Артефакты
    идут во временный каталог, чтобы прогон теста не перезаписывал коммитируемые
    `sql/dcf_mine_plans_seed.sql` и `polyus_lom_reconciled_long.csv`.
    """

    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.dir = Path(temp.name)
        for name, value in (
            ("OUTPUT_SQL", self.dir / "seed.sql"),
            ("OUTPUT_CSV", self.dir / "reconciled.csv"),
        ):
            patcher = unittest.mock.patch.object(ex, name, value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def _production(self, koz: float = 1000.0) -> dict:
        return {
            asset: {year: koz for year in range(2021, 2026)}
            for asset in ex.ASSETS
        }

    def _run(self, production=None, group_aisc=None):
        with unittest.mock.patch.object(
            ex, "read_actual_production",
            lambda assets, years: production or self._production(),
        ):
            if group_aisc is None:
                return ex.main()
            with unittest.mock.patch.object(ex, "GROUP_AISC_2025_USD_PER_OZ", group_aisc):
                return ex.main()

    def test_shipped_anchor_returns_zero_and_writes_artifacts(self):
        # Пометка «валидация пройдена» на штатных якорях — предмет приёмки
        # Task 11; здесь проверяется КОД ВОЗВРАТА, потому что именно он решает,
        # строит ли `make ch-sql` следующий шаг цепочки.
        with unittest.mock.patch("sys.stdout", new=io.StringIO()) as out:
            code = self._run()
        self.assertEqual(code, 0)
        self.assertIn("валидация пройдена", out.getvalue())
        self.assertTrue((self.dir / "seed.sql").exists())
        self.assertTrue((self.dir / "reconciled.csv").exists())
        # 77 строк — сумма сроков службы пяти активов.
        with (self.dir / "reconciled.csv").open(newline="", encoding="utf-8") as handle:
            self.assertEqual(len(list(csv.DictReader(handle))), 77)
        # Справочная колонка r̄ заполнена ИЗ КАЛИБРОВКИ, а не константой: пустая
        # ячейка здесь означала бы, что `main()` не связал калибровку со строками.
        with (self.dir / "reconciled.csv").open(newline="", encoding="utf-8") as handle:
            first = next(csv.DictReader(handle))
        self.assertAlmostEqual(float(first["r_bar"]), 0.6261129324997634, places=9)

    def test_unreachable_low_target_returns_nonzero(self):
        # ПРАВИЛО КОНТРОЛЛЕРА 1, сквозная проверка: недостижимо низкая цель
        # прижимает m к нулю и даёт ПОЛОЖИТЕЛЬНЫЙ остаток, и `main()` обязан
        # выйти НЕНУЛЕВЫМ кодом. Проверка по знаку остатка здесь пропустила бы
        # ветвь целиком, и дефектный сид уехал бы как успешный.
        with unittest.mock.patch("sys.stdout", new=io.StringIO()) as out:
            code = self._run(group_aisc=100.0)
        self.assertEqual(code, 1)
        text = out.getvalue()
        self.assertIn("ВАЛИДАЦИЯ НЕ ПРОЙДЕНА", text)
        self.assertIn("остаток", text.lower())

    def test_missing_production_for_an_asset_is_flagged(self):
        # Актив без добычи не даёт строк → в плане его нет, и `validate` обязан
        # это назвать (проверка сквозная, через реальный `build_plan_rows`).
        production = self._production()
        production["KURANAKH"] = {year: 0.0 for year in range(2021, 2026)}
        with unittest.mock.patch("sys.stdout", new=io.StringIO()) as out:
            code = self._run(production=production)
        self.assertEqual(code, 1)
        self.assertIn("KURANAKH", out.getvalue())

    def test_run_reads_no_network(self):
        # Если бы `main()` ходил в ClickHouse мимо `read_actual_production`,
        # транспорт поднял бы исключение — тест обязан остаться offline.
        with unittest.mock.patch.object(
            ex, "ch_query",
            side_effect=AssertionError("main() не должен обращаться к сети"),
        ):
            with unittest.mock.patch("sys.stdout", new=io.StringIO()):
                code = self._run()
        self.assertEqual(code, 0)

    def test_generated_seed_charges_zero_sustaining_capex_in_every_row(self):
        # СКВОЗНОЙ ИНВАРИАНТ ДВОЙНОГО СЧЁТА, по ОБОИМ артефактам. Клин
        # поддержания уже внутри `aisc`, поэтому модель не должна списывать его
        # ещё раз через `capex_sustaining`; SQL и CSV обязаны говорить об этом
        # одно и то же. Проверяются: (а) каждый VALUES-кортеж SQL несёт 0 в
        # позиции capex_sustaining; (б) колонка CSV `capex_sustaining` нулевая во
        # всех 77 строках; (в) клин всё же виден человеку в `wedge_*` и равен
        # `(aisc − tcc)`.
        with unittest.mock.patch("sys.stdout", new=io.StringIO()):
            self.assertEqual(self._run(), 0)
        sql_text = (self.dir / "seed.sql").read_text(encoding="utf-8")
        # Тело — строки, начинающиеся с '('; каждая и есть VALUES-кортеж.
        tuple_lines = [line for line in sql_text.splitlines() if line.startswith("(")]
        self.assertEqual(len(tuple_lines), 77)
        for line in tuple_lines:
            row = line.rstrip(",").rstrip(";").strip("()")
            fields = row.split(",")
            self.assertEqual(len(fields), 10, row)
            self.assertEqual(fields[7], "0.0", row)  # capex_sustaining
            self.assertEqual(fields[8], "0", row)  # capex_project
            self.assertEqual(fields[9], "0", row)  # closure_costs
        with (self.dir / "reconciled.csv").open(newline="", encoding="utf-8") as handle:
            parsed = list(csv.DictReader(handle))
        self.assertEqual(len(parsed), 77)
        for entry in parsed:
            self.assertEqual(float(entry["capex_sustaining"]), 0.0, entry)
            self.assertAlmostEqual(
                float(entry["wedge_usd_per_oz"]),
                float(entry["aisc"]) - float(entry["tcc"]),
                places=9,
            )
            # В год консервации (production_koz = 0) клин в МЛН USD законно
            # ноль; зубы против двойного счёта — на производящих годах.
            self.assertGreaterEqual(float(entry["wedge_mn_usd"]), 0.0)
            if float(entry["production_koz"]) > 0.0:
                self.assertGreater(float(entry["wedge_mn_usd"]), 0.0, entry)
            self.assertAlmostEqual(
                float(entry["wedge_mn_usd"]),
                (float(entry["aisc"]) - float(entry["tcc"]))
                * float(entry["production_koz"]) / 1000.0,
                places=6,
            )


class TestCommittedSeedIsFresh(unittest.TestCase):
    """СТОРОЖ УСТАРЕВАНИЯ коммитируемого сида `sql/dcf_mine_plans_seed.sql`.

    Коммитируемый артефакт ценен ровно настолько, насколько он совпадает с тем,
    что порождает генератор. Без этой проверки ручная правка файла или сид,
    отставший от правки скрипта, уехали бы в репозиторий незамеченными: `make
    dcf-aisc` перезаписал бы файл только при СЛЕДУЮЩЕМ запуске, а между ними
    `make ch-sql` применял бы уже неверный сид.

    ТЕЛО ФАЙЛА — ПРЕДМЕТ ПРОВЕРКИ, А НЕ ШАПКА. Шапка несёт `-- Дата генерации:
    {date.today()}` (см. `write_sql`), поэтому побайтовое сравнение ВСЕГО файла
    падало бы на любой день, кроме дня генерации, — сторож, краснеющий по
    календарю, бесполезен. Тело (операторы `INSERT … VALUES`) от даты не
    зависит и является ровно тем, что применяется к ClickHouse, поэтому
    сравнивается ОНО. Устойчивые поля шапки (команда, годы калибровки, m,
    остаток, число строк) проверяются отдельно — дата из сравнения исключена
    явно, чтобы это исключение было видимым, а не подразумеваемым.

    ЖИВОЙ CLICKHOUSE НУЖЕН, НО НЕ ОБЯЗАТЕЛЕН. Факт добычи приходит из
    `databook_polyus`, поэтому точное воспроизведение тела требует живой базы.
    Если база недоступна (`ClickHouseError`/`OSError`/сбой конфигурации), тест
    ПРОПУСКАЕТСЯ с явной причиной: офлайновый набор обязан оставаться зелёным,
    а сторож — не превращаться в источник ложных падений. Пропуск печатается
    как `skipped`, и это ВИДНО в выводе, а не тонет.
    """

    def _generate_into(self, directory: Path) -> Path:
        """Прогоняет конвейер в temp-каталог, возвращает путь к SQL-сиду."""
        sql_path = directory / "seed.sql"
        csv_path = directory / "reconciled.csv"
        with unittest.mock.patch.object(ex, "OUTPUT_SQL", sql_path), \
                unittest.mock.patch.object(ex, "OUTPUT_CSV", csv_path), \
                unittest.mock.patch("sys.stdout", new=io.StringIO()):
            code = ex.main()
        self.assertEqual(code, 0, "генератор вернул ненулевой код на штатных якорях")
        return sql_path

    @staticmethod
    def _body(text: str) -> str:
        """Тело сида: от первого `INSERT` до конца файла."""
        marker = "INSERT INTO "
        index = text.find(marker)
        if index < 0:
            return ""
        return text[index:]

    def test_committed_seed_body_matches_a_fresh_generation(self):
        committed = ex.ROOT / "sql" / "dcf_mine_plans_seed.sql"
        self.assertTrue(
            committed.exists(),
            f"коммитируемый сид отсутствует: {committed}",
        )
        with tempfile.TemporaryDirectory() as temp:
            try:
                generated_path = self._generate_into(Path(temp))
            except (ex.ClickHouseError, OSError) as exc:
                self.skipTest(
                    "ClickHouse недоступен — воспроизведение тела сида "
                    f"проверить нельзя: {exc}"
                )
            generated = generated_path.read_text(encoding="utf-8")
        committed_text = committed.read_text(encoding="utf-8")
        self.assertEqual(
            self._body(committed_text),
            self._body(generated),
            "тело sql/dcf_mine_plans_seed.sql не совпадает со свежей генерацией: "
            "файл правили руками или он устарел относительно скрипта — "
            "пересоберите `make dcf-aisc`",
        )

    def test_committed_seed_header_fields_match_a_fresh_generation(self):
        committed = ex.ROOT / "sql" / "dcf_mine_plans_seed.sql"
        committed_text = committed.read_text(encoding="utf-8")
        with tempfile.TemporaryDirectory() as temp:
            try:
                generated_path = self._generate_into(Path(temp))
            except (ex.ClickHouseError, OSError) as exc:
                self.skipTest(
                    f"ClickHouse недоступен — поля шапки сверить нельзя: {exc}"
                )
            generated = generated_path.read_text(encoding="utf-8")

        def stable_header(text: str) -> list:
            # Дата генерации исключена ЯВНО и названа здесь, чтобы исключение
            # было видимым: строка «Дата генерации» меняется каждые сутки и
            # сторожем не является.
            return [
                line for line in text.splitlines()
                if line.startswith("--") and "Дата генерации" not in line
            ]

        self.assertEqual(
            stable_header(committed_text),
            stable_header(generated),
            "устойчивые поля шапки сида (команда, годы калибровки, m, остаток, "
            "число строк) разошлись со свежей генерацией",
        )

    def test_clickhouse_unreachable_skips_instead_of_failing(self):
        # Пропуск — именно ПРОПУСК, а не падение: подменяем транспорт так, будто
        # ClickHouse лёг. Сторож обязан назвать причину, а набор — остаться
        # зелёным (проверка офлайновости контракта I2).
        with unittest.mock.patch.object(
            ex, "read_actual_production",
            side_effect=ex.ClickHouseError("нет соединения (подмена тестом)"),
        ):
            with tempfile.TemporaryDirectory() as temp:
                sql_path = Path(temp) / "seed.sql"
                csv_path = Path(temp) / "reconciled.csv"
                with unittest.mock.patch.object(ex, "OUTPUT_SQL", sql_path), \
                        unittest.mock.patch.object(ex, "OUTPUT_CSV", csv_path), \
                        unittest.mock.patch("sys.stdout", new=io.StringIO()):
                    with self.assertRaises(ex.ClickHouseError):
                        ex.main()


if __name__ == "__main__":
    unittest.main()
