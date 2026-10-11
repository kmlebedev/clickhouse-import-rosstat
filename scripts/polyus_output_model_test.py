#!/usr/bin/env python3
"""Тесты scripts/polyus_output_model.py.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/polyus_output_model_test.py -v

Набор ОФЛАЙН: обращения к ClickHouse подменяются `unittest.mock.patch.object`,
поэтому ни БД, ни сети для прогона не нужно. Живая проверка — отдельный ручной
шаг `make polyus-output` + SQL-проверки (план, Task 5).
"""

import csv
import sys
import tempfile
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

    def test_asset_name_keeps_its_space(self):
        # Каноническое имя Полюса — «Sukhoi Log», с пробелом. Проверка формы
        # имени вида ^[A-Z][A-Z0-9_]*$ отвергла бы законные данные.
        self.assertEqual(om.ASSET, "Sukhoi Log")
        self.assertIn("'Sukhoi Log'", om.output_sql(om.ASSET, 2025))

    def test_single_quote_in_asset_is_escaped(self):
        # Литерал не должен разрываться кавычкой из значения.
        self.assertIn("'O''HARA'", om.output_sql("O'HARA", 2025))

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

    def test_implausible_year_raises(self):
        with self.assertRaises(om.OutputDataError):
            om.output_sql("OLIMPIADA", 1800)


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


class TestOverlap(unittest.TestCase):
    def test_rejects_year_owned_by_go_seed(self):
        # Строка 2028 принадлежит Go-сиду: отказ обязан сработать.
        colliding = [{"asset": om.ASSET, "year": 2028}]
        with self.assertRaises(om.OverlapError):
            om.assert_no_overlap(colliding, go_seed_years={2028, 2029, 2030})

    def test_rejects_even_when_only_one_of_many_collides(self):
        # Одна пересекающаяся строка из трёх — уже перезапись чужого года.
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        rows.append({"asset": om.ASSET, "year": 2030})
        with self.assertRaises(om.OverlapError):
            om.assert_no_overlap(rows, go_seed_years={2030})

    def test_accepts_years_before_go_seed_horizon(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        om.assert_no_overlap(rows, go_seed_years={2028, 2029})
        self.assertEqual([r["year"] for r in rows], [2025, 2026, 2027])

    def test_ignores_other_assets(self):
        # Проверка про Сухой Лог: чужой актив в списке не должен её задевать.
        other = [{"asset": "OLIMPIADA", "year": 2029}]
        om.assert_no_overlap(other, go_seed_years={2028, 2029, 2030})

    def test_default_horizon_matches_go_seed(self):
        # MineLifeYears: 10, ProfileStartYear: 2028 -> 2028..2037 включительно.
        self.assertEqual(om.GO_SEED_SUKHOI_LOG_YEARS, frozenset(range(2028, 2038)))

    def test_opr_years_do_not_touch_default_horizon(self):
        rows = om.build_rows({2025: 242.2, 2026: 271.7})
        # Вызов без аргумента обязан пройти: окно ОПР по построению до 2028.
        om.assert_no_overlap(rows)


class TestEmit(unittest.TestCase):
    def setUp(self):
        self.rows = om.build_rows({2025: 242.2, 2026: 271.7})

    def test_sql_has_single_insert_with_three_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seed.sql"
            om.write_sql(path, self.rows)
            text = path.read_text(encoding="utf-8")
        self.assertEqual(text.count("INSERT INTO mine_plans"), 1)
        self.assertEqual(text.count("('PLZL','Sukhoi Log',"), 3)

    def test_sql_header_names_manual_apply(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seed.sql"
            om.write_sql(path, self.rows)
            text = path.read_text(encoding="utf-8")
        self.assertIn("make ch-sql FILE=sql/polyus_output_seed.sql", text)
        self.assertIn("ПРАВИТЬ РУКАМИ НЕЛЬЗЯ", text)

    def test_sql_rows_carry_expected_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "seed.sql"
            om.write_sql(path, self.rows)
            text = path.read_text(encoding="utf-8")
        # 2025: 242.2, 2026: 271.7, 2027: 257.0; tcc=aisc=735; capex 0; closure 0.
        self.assertIn("('PLZL','Sukhoi Log',2025,242.2,NULL,735.0,735.0,0.0,0,0)", text)
        self.assertIn("('PLZL','Sukhoi Log',2027,257.0,NULL,735.0,735.0,0.0,0,0)", text)

    def test_csv_column_count_matches_header(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            om.write_csv(path, self.rows)
            with path.open(encoding="utf-8") as handle:
                table = list(csv.reader(handle))
        self.assertEqual(len(table), 4)
        self.assertTrue(all(len(row) == len(table[0]) for row in table))

    def test_csv_grade_cell_is_empty_not_null_literal(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            om.write_csv(path, self.rows)
            with path.open(encoding="utf-8") as handle:
                rows = list(csv.DictReader(handle))
        self.assertEqual(rows[0]["grade_gpt"], "")

    def test_csv_marks_estimated_year_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "out.csv"
            om.write_csv(path, self.rows)
            with path.open(encoding="utf-8") as handle:
                rows = {r["year"]: r for r in csv.DictReader(handle)}
        self.assertEqual(rows["2025"]["source_metric"], "refined_measured")
        self.assertEqual(rows["2027"]["source_metric"], "mean_2025_2026")


class TestMain(unittest.TestCase):
    def _run(self, side_effect):
        """Прогон `main()` во временном каталоге.

        Проверки расположены ВНУТРИ контекста `TemporaryDirectory`, а не после
        него: каталог удаляется на выходе из `with`, и вынесенный наружу
        `exists()` был бы ложен ВСЕГДА — то есть проверял бы удаление каталога,
        а не запись файлов. Проверяющая функция получает пути и код возврата и
        вызывается до удаления.
        """
        with tempfile.TemporaryDirectory() as tmp:
            sql_path, csv_path = Path(tmp) / "s.sql", Path(tmp) / "s.csv"
            with unittest.mock.patch.object(om, "read_refined_output", side_effect=side_effect), \
                 unittest.mock.patch.object(om, "OUTPUT_SQL", sql_path), \
                 unittest.mock.patch.object(om, "OUTPUT_CSV", csv_path):
                code = om.main()
            return self.check(code, sql_path, csv_path)

    def check(self, code, sql_path, csv_path):
        self.sql_exists = sql_path.exists()
        self.csv_exists = csv_path.exists()
        self.sql_text = sql_path.read_text(encoding="utf-8") if self.sql_exists else ""
        return code

    def test_writes_both_artifacts_and_returns_zero(self):
        measured = {2025: 242.2, 2026: 271.7}
        code = self._run(lambda asset, year: measured[year])
        self.assertEqual(code, 0)
        self.assertTrue(self.sql_exists, "сид не записан")
        self.assertTrue(self.csv_exists, "CSV не записан")

    def test_returns_nonzero_when_fact_missing(self):
        code = self._run(om.OutputDataError("нет факта за 2026"))
        self.assertNotEqual(code, 0)
        self.assertFalse(self.sql_exists, "сид записан, хотя факта нет")
        self.assertEqual(self.sql_text, "")

    def test_reads_source_asset_but_writes_target_asset(self):
        # Суть атрибуции: факт читается у Вернинского (его ЗИФ перерабатывает
        # руду), а строка плана пишется Сухому Логу (руда его).
        seen = []
        measured = {2025: 242.2, 2026: 271.7}

        def record(asset, year):
            seen.append((asset, year))
            return measured[year]

        self._run(record)
        self.assertEqual(seen, [(om.SOURCE_ASSET, 2025), (om.SOURCE_ASSET, 2026)])
        self.assertIn("'Sukhoi Log',2025", self.sql_text)
        self.assertNotIn("'VERNINSKOYE2',2025", self.sql_text)

    def test_stdout_names_the_estimated_year(self):
        # Оценка обязана быть видна в сводке прогона, а не только в CSV:
        # иначе глазная сверка лога приняла бы её за факт.
        import io
        import contextlib
        measured = {2025: 242.2, 2026: 271.7}
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self._run(lambda asset, year: measured[year])
        text = buf.getvalue()
        self.assertIn("257.0", text)
        self.assertIn("оценка", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
