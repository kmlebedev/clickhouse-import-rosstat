#!/usr/bin/env python3
"""Офлайн-тесты `scripts/shock_classifier.py` (Task 2).

Запуск из корня репозитория:

    uv run --with scikit-learn --with pandas --with "statsmodels>=0.15" \
        --with scipy --python 3.12 python3 scripts/shock_classifier_test.py -v

Набор НЕ обращается к ClickHouse и сети: данные — синтетические фикстуры
`scripts/testdata/shock_classifier/`, которые пишет и читает сам модуль
(`export_fixtures`/`read_fixtures`). Проверяются:

* правило метки: явный шок = внеочередное И изменение ставки;
* as-of: точка, опубликованная после заседания, не участвует (месячная и
  дневная конвенции);
* пропуск канала — `None`, а не 0; `missing_count` его считает;
* дельта — к предыдущему ЗАСЕДАНИЮ панели, а не к предыдущему месяцу;
* в панель попадают только решения `key_rate_model`;
* экспорт фикстур детерминирован и читается обратно теми же строками.
"""

import contextlib
import csv
import io
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import shock_classifier as sc


def fixture_panel() -> list:
    """Панель на синтетических точках в памяти (без CSV и ClickHouse)."""
    return sc.build_panel(
        sc.FIXTURE_CANONICAL_MEETINGS,
        sc.FIXTURE_KEY_RATE_POINTS,
        sc.fixture_channel_points(),
        [],
    )


def fixture_rows(name: str, directory: Path) -> list:
    """Строки CSV-фикстуры словарями — в форме ответа `ch_query`."""
    with (directory / name).open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    return rows


class LabelTest(unittest.TestCase):
    def test_label_shock_requires_emergency_and_change(self):
        self.assertTrue(sc.label_shock(True, -300))
        self.assertFalse(sc.label_shock(True, 0))
        self.assertFalse(sc.label_shock(False, 100))

    def test_emergency_without_change_is_not_shock(self):
        # Реальная панель: внеочередное заседание с неизменной ставкой. Метка
        # emergency сохраняется, explicit_shock — нет.
        meetings = [(date(2022, 2, 28), True)]
        key_rate = [
            (date(2022, 2, 27), 7.5),
            (date(2022, 3, 7), 7.5),
            (date(2022, 6, 1), 7.5),
        ]
        rows = sc.build_panel(meetings, key_rate, {}, [])
        self.assertEqual(len(rows), 1)
        self.assertTrue(rows[0].emergency)
        self.assertFalse(rows[0].explicit_shock)
        self.assertEqual(rows[0].decision.step_bp, 0)


class MeetingsTest(unittest.TestCase):
    def test_meetings_filter_title_and_dedup(self):
        rows = [
            # Каноническая строка + её дубль с другим флагом: OR по дате.
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": sc.SAMPLE_TITLE,
                "emergency": 0,
            },
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": sc.SAMPLE_TITLE,
                "emergency": 1,
            },
            # Ручной триггер на ту же дату: отсекается фильтром по title.
            {
                "event_date": "2022-02-28",
                "category": "cbr",
                "title": "СД ЦБ РФ: ручной триггер",
                "emergency": 0,
            },
            # Уникальная дата ручного триггера: без фильтра дала бы заседание.
            {
                "event_date": "2022-03-01",
                "category": "cbr",
                "title": "СД ЦБ РФ: ручной триггер",
                "emergency": 1,
            },
            # Чужая категория: не заседание ЦБ.
            {
                "event_date": "2022-03-02",
                "category": "fomc",
                "title": sc.SAMPLE_TITLE,
                "emergency": 1,
            },
        ]
        self.assertEqual(
            sc.meetings_with_emergency_from_rows(rows),
            [(date(2022, 2, 28), True)],
        )

    def test_fixture_has_eight_meetings_and_two_emergency(self):
        meetings = sc.meetings_with_emergency_from_rows(
            sc._fixture_meeting_rows()
        )
        self.assertEqual(len(meetings), 8)
        self.assertEqual(
            [day for day, emergency in meetings if emergency],
            [date(2022, 2, 28), date(2022, 4, 8)],
        )
        # Ручная строка с уникальной датой не попала в список.
        self.assertNotIn(date(2021, 5, 14), [day for day, _ in meetings])


class AsOfTest(unittest.TestCase):
    def test_point_published_after_meeting_is_not_used(self):
        # Точка за март 2022 доступна только в конце апреля; заседание
        # 2022-04-08 обязано видеть февральскую.
        points = [
            (date(2022, 1, 31), 1.0),
            (date(2022, 2, 28), 2.0),
            (date(2022, 3, 31), 3.0),
        ]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=True), 2.0
        )

    def test_point_available_at_end_of_next_month_is_used(self):
        # Граница включительна: февральская точка доступна 2022-03-31.
        points = [(date(2022, 2, 28), 2.0)]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 3, 31), monthly=True), 2.0
        )
        self.assertIsNone(
            sc.feature_asof(points, date(2022, 3, 30), monthly=True)
        )

    def test_daily_point_after_meeting_is_not_used(self):
        points = [
            (date(2022, 4, 7), 10.0),
            (date(2022, 4, 8), 11.0),
            (date(2022, 4, 9), 12.0),
        ]
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=False), 11.0
        )

    def test_march_fixture_point_is_not_used_on_april_meeting(self):
        points = sc.fixture_cpd_points()["pi"]
        february = dict(points)[date(2022, 2, 28)]
        march = dict(points)[date(2022, 3, 31)]
        self.assertNotEqual(february, march)
        self.assertEqual(
            sc.feature_asof(points, date(2022, 4, 8), monthly=True),
            february,
        )


class PanelTest(unittest.TestCase):
    def test_panel_has_expected_decisions_and_labels(self):
        rows = fixture_panel()
        self.assertEqual(
            [row.decision.meeting_date for row in rows],
            [day for day, _ in sc.FIXTURE_CANONICAL_MEETINGS],
        )
        self.assertEqual(
            [row.emergency for row in rows],
            [emergency for _, emergency in sc.FIXTURE_CANONICAL_MEETINGS],
        )
        self.assertEqual(
            [row.explicit_shock for row in rows],
            [False, False, False, False, False, True, False, True],
        )
        self.assertEqual([row.decision.step_bp for row in rows],
                         [25, 25, 25, 50, 100, 1350, 0, -300])

    def test_missing_channel_is_none_not_zero(self):
        row = fixture_panel()[0]
        self.assertIsNone(row.features["infl_exp_expected"])
        self.assertIsNone(row.features["infl_exp_observed"])
        self.assertGreaterEqual(row.missing_count, 1)

    def test_missing_count_counts_base_features_only(self):
        row = fixture_panel()[0]
        expected = sum(
            1 for name in sc.BASE_FEATURES if row.features[name] is None
        )
        self.assertEqual(row.missing_count, expected)

    def test_delta_is_to_previous_meeting(self):
        rows = fixture_panel()
        self.assertIsNotNone(rows[0].features["pi_saar"])
        self.assertIsNotNone(rows[1].features["pi_saar"])
        self.assertAlmostEqual(
            rows[1].features["pi_saar_chg"],
            rows[1].features["pi_saar"] - rows[0].features["pi_saar"],
            places=9,
        )
        # И это именно сдвиг между заседаниями: дельта второго заседания не
        # равна разнице текущего и прошлого МЕСЯЧНЫХ значений.
        self.assertNotEqual(rows[1].features["pi_saar_chg"], 0.0)

    def test_first_row_deltas_are_none(self):
        row = fixture_panel()[0]
        for name in sc.DELTA_FEATURES:
            self.assertIsNone(row.features[f"{name}_chg"])

    def test_inflation_expectation_delta_appears_after_first_meeting(self):
        rows = fixture_panel()
        self.assertIsNone(rows[0].features["infl_exp_expected"])
        self.assertIsNotNone(rows[1].features["infl_exp_expected"])
        # У второй строки предыдущее значение отсутствовало — дельта None.
        self.assertIsNone(rows[1].features["infl_exp_expected_chg"])
        # А у третьей уже есть оба значения.
        self.assertIsNotNone(rows[2].features["infl_exp_expected_chg"])
        self.assertAlmostEqual(
            rows[2].features["infl_exp_expected_chg"],
            rows[2].features["infl_exp_expected"]
            - rows[1].features["infl_exp_expected"],
            places=9,
        )

    def test_yoy_uses_same_month_last_year(self):
        channels = sc.fixture_channel_points()
        row = fixture_panel()[0]
        wage = dict(channels["wage"])
        expected = (wage[date(2020, 12, 1)] / wage[date(2019, 12, 1)] - 1.0) * 100.0
        self.assertAlmostEqual(row.features["wage_yoy"], expected, places=9)

    def test_budget_balance_is_trailing_12m_ratio(self):
        channels = sc.fixture_channel_points()
        row = fixture_panel()[0]
        income = sum(
            value
            for day, value in channels["budget_income"]
            if date(2020, 1, 1) <= day <= date(2020, 12, 1)
        )
        expense = sum(
            value
            for day, value in channels["budget_expense"]
            if date(2020, 1, 1) <= day <= date(2020, 12, 1)
        )
        expected = (income - expense) / income * 100.0
        self.assertAlmostEqual(row.features["budget_balance"], expected, places=9)

    def test_build_panel_uses_decisions_only(self):
        # 2022-06-30 — будущее заседание: окно исхода (d+7) выходит за ряд
        # ставки, поэтому в панели его быть не должно.
        meetings = list(sc.FIXTURE_CANONICAL_MEETINGS) + [
            (date(2022, 6, 30), False)
        ]
        rows = sc.build_panel(
            meetings,
            sc.FIXTURE_KEY_RATE_POINTS,
            sc.fixture_channel_points(),
            [],
        )
        self.assertEqual(
            [row.decision.meeting_date for row in rows],
            [day for day, _ in sc.FIXTURE_CANONICAL_MEETINGS],
        )

    def test_excluded_future_meeting_does_not_change_previous_decision(self):
        # Предыдущее решение не зависит от того, передали ли вызывающий
        # будущее заседание: окно исхода ограничено d+7.
        without = fixture_panel()
        with_future = sc.build_panel(
            list(sc.FIXTURE_CANONICAL_MEETINGS) + [(date(2022, 6, 30), False)],
            sc.FIXTURE_KEY_RATE_POINTS,
            sc.fixture_channel_points(),
            [],
        )
        self.assertEqual(without, with_future)


class FixturesTest(unittest.TestCase):
    def test_export_fixtures_is_deterministic(self):
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(sc.export_fixtures(Path(first)), 0)
                self.assertEqual(sc.export_fixtures(Path(second)), 0)
            for name in sc.FIXTURE_COLUMNS:
                self.assertEqual(
                    (Path(first) / name).read_bytes(),
                    (Path(second) / name).read_bytes(),
                    f"{name}: повторный экспорт обязан давать те же байты",
                )

    def test_export_fixtures_roundtrip(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stdout(io.StringIO()):
                sc.export_fixtures(Path(directory))
            meetings, key_rate, channels = sc.read_fixtures(Path(directory))
            exported = sc.build_panel(meetings, key_rate, channels, [])
            expected = fixture_panel()
            self.assertEqual(exported, expected)
            self.assertEqual(len(exported), 8)
            self.assertEqual(
                [row.explicit_shock for row in exported],
                [False, False, False, False, False, True, False, True],
            )

    def test_fixture_csv_shapes(self):
        with tempfile.TemporaryDirectory() as directory:
            with contextlib.redirect_stdout(io.StringIO()):
                sc.export_fixtures(Path(directory))
            meetings = fixture_rows(sc.FIXTURE_MEETINGS, Path(directory))
            self.assertEqual(
                set(meetings[0]), set(sc.FIXTURE_COLUMNS[sc.FIXTURE_MEETINGS])
            )
            # Ручная строка с уникальной датой присутствует в файле, но
            # отфильтровывается загрузчиком.
            self.assertIn(
                "2021-05-14", {row["event_date"] for row in meetings}
            )
            channels = fixture_rows(sc.FIXTURE_CHANNELS, Path(directory))
            self.assertEqual(
                set(channels[0]), set(sc.FIXTURE_COLUMNS[sc.FIXTURE_CHANNELS])
            )
            self.assertIn(
                "infl_exp_expected", {row["channel"] for row in channels}
            )


class LoaderTest(unittest.TestCase):
    def test_channel_queries_use_final(self):
        queries = []

        def fake_query(statement):
            queries.append(statement)
            return []

        with unittest.mock.patch.object(sc.krm, "ch_query", fake_query):
            channels = sc.load_channels()
        self.assertEqual(set(channels), set(sc.CHANNEL_KEYS))
        self.assertEqual(len(queries), len(sc.CHANNEL_SQL))
        for sql in queries:
            self.assertIn("FINAL", sql)
        self.assertIn("emergency", sc.MEETINGS_SQL)
        self.assertIn("FINAL", sc.MEETINGS_SQL)

    def test_salary_points_are_normalized_to_reporting_month(self):
        self.assertEqual(
            sc.normalize_salary_points([(date(2021, 3, 1), 100.0)]),
            [(date(2021, 2, 1), 100.0)],
        )
        # Декабрь отчётного года не должен «переехать» на январь.
        self.assertEqual(
            sc.normalize_salary_points([(date(2022, 1, 1), 200.0)]),
            [(date(2021, 12, 1), 200.0)],
        )

    def test_points_from_rows_rejects_duplicate_dates(self):
        rows = [
            {"date": "2022-01-31", "value": 1.0},
            {"date": "2022-01-31", "value": 2.0},
        ]
        with self.assertRaises(ValueError):
            sc.points_from_rows(rows, "тест")

    def test_points_from_rows_rejects_null_value(self):
        with self.assertRaises(ValueError):
            sc.points_from_rows([{"date": "2022-01-31", "value": ""}], "тест")


if __name__ == "__main__":
    unittest.main()
