#!/usr/bin/env python3
"""Тесты scripts/key_rate_model.py.

Запуск из корня репозитория (pytest не нужен):

    uv run --with "statsmodels>=0.15" --with pandas --with scikit-learn --with scipy \
        --python 3.12 python3 scripts/key_rate_model_test.py -v

(`--python 3.12` — требование statsmodels>=0.15 (Python>=3.10) и конвенция
репозитория для uv-прогонов; Makefile пинует тот же интерпретатор.)

Набор ОФЛАЙН: обращения к ClickHouse подменяются `unittest.mock`, данные берутся
из фикстур `scripts/testdata/key_rate/*.csv`. Живая проверка — отдельный ручной
шаг `make key-rate-model`.

ФИКСТУРЫ ПЕРЕСОБИРАЮТСЯ ИЗ ЖИВОГО DEV-CLICKHOUSE (пользователь default):

    make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"

Окно срезов — 2013-01-01…2016-12-31. Верхняя граница выбрана так, что окно
исхода последнего заседания среза (2016-12-16, W = 2016-12-23) наблюдаемо
целиком, а заседания 2017+ лежат за рядом ставки и проверяют правило «окно
исхода вне данных ставки → заседание вне выборки» (на живом прогоне так
отсеиваются будущие заседания). Файлы коммитятся: тесты обязаны
воспроизводиться без сети и без БД.
"""

import csv
import sys
import unittest
import unittest.mock
from datetime import date, timedelta
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import key_rate_model as km

FIXTURE_DIR = Path(__file__).resolve().parent / "testdata" / "key_rate"


def fixture_rows(name: str) -> list:
    """Строки CSV-фикстуры словарями — в той же форме, что отдаёт ch_query."""
    with (FIXTURE_DIR / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fixture_meetings() -> list:
    return km.meetings_from_rows(fixture_rows("meetings.csv"))


def fixture_key_rate() -> list:
    return km.key_rate_from_rows(fixture_rows("key_rate_2013_2017.csv"))


class TestSampleFilter(unittest.TestCase):
    """Список заседаний и фильтр по title (спека §3)."""

    def test_sample_title_is_pinned(self):
        # Дословный title канонических строк events_calendar: расхождение с ним
        # (переименование карточки в сиде) молча обнулило бы выборку.
        self.assertEqual(km.SAMPLE_TITLE, "СД ЦБ РФ: заседание по ключевой ставке")

    def test_fixture_contains_manual_trigger_rows(self):
        # Фикстура — ВСЯ категория 'cbr': 118 канонических строк плюс две ручные
        # строки-триггера сида. Без них фильтр по title нечем проверять.
        rows = fixture_rows("meetings.csv")
        self.assertEqual(len(rows), 120)
        canonical = sum(1 for row in rows if row["title"] == km.SAMPLE_TITLE)
        self.assertEqual(canonical, 118)
        manual = sorted(row["event_date"] for row in rows if row["title"] != km.SAMPLE_TITLE)
        self.assertEqual(manual, ["2026-10-23", "2026-12-18"])

    def test_title_filter_keeps_only_canonical_meetings(self):
        # 120 строк дают 118 заседаний: ручные строки делят даты с каноническими
        # (2026-10-23 и 2026-12-18 помечены и сидом, и календарём ЦБ), поэтому
        # по датам дедуп делает их невидимыми — эффект фильтра проверяется на
        # ручной дате, которой нет среди канонических строк.
        meetings = fixture_meetings()
        self.assertEqual(len(meetings), 118)
        self.assertEqual(meetings[0], date(2014, 2, 14))
        self.assertEqual(meetings[-1], date(2027, 12, 17))
        manual_only = [{"event_date": "2026-10-20", "category": "cbr", "title": "ручной триггер"}]
        self.assertEqual(km.meetings_from_rows(manual_only), [])


class TestLoaders(unittest.TestCase):
    """Загрузчики: транспорт мокается, правила — в чистом коде."""

    def test_load_meetings_converts_rows_and_filters_title(self):
        rows = [
            {"event_date": "2014-02-14", "category": "cbr", "title": km.SAMPLE_TITLE},
            {"event_date": "2014-03-03", "category": "cbr", "title": km.SAMPLE_TITLE},
            {"event_date": "2026-10-23", "category": "cbr", "title": "ручная строка-триггер"},
        ]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows) as query:
            self.assertEqual(km.load_meetings(), [date(2014, 2, 14), date(2014, 3, 3)])
        self.assertIn("events_calendar", query.call_args.args[0])

    def test_load_key_rate_converts_rows(self):
        rows = [{"date": "2014-12-11", "rate": 9.5}, {"date": "2014-12-12", "rate": 10.5}]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows):
            self.assertEqual(
                km.load_key_rate(), [(date(2014, 12, 11), 9.5), (date(2014, 12, 12), 10.5)]
            )

    def test_load_cpd_core_converts_rows(self):
        rows = [{"date": "2013-12-31", "value": 0.30000001192092896}]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows) as query:
            self.assertEqual(
                km.load_cpd_core(), [(date(2013, 12, 31), 0.30000001192092896)]
            )
        self.assertIn("cbr_indicators_cpd", query.call_args.args[0])

    def test_load_ibk_converts_rows(self):
        rows = [{"date": "2013-12-01", "value": -3.5}]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows) as query:
            self.assertEqual(km.load_ibk(), [(date(2013, 12, 1), -3.5)])
        self.assertIn("macro_series", query.call_args.args[0])

    def test_load_mp_summary_deltas_keeps_null(self):
        rows = [
            {"meeting_date": "2024-02-16", "decision_rate": 16.0, "decision_delta_bp": None},
            {"meeting_date": "2024-03-22", "decision_rate": 16.0, "decision_delta_bp": 0},
            {"meeting_date": "2024-07-26", "decision_rate": 18.0, "decision_delta_bp": 200},
        ]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows):
            self.assertEqual(
                km.load_mp_summary_deltas(),
                [(date(2024, 2, 16), None), (date(2024, 3, 22), 0), (date(2024, 7, 26), 200)],
            )

    def test_replacing_merge_tree_queries_use_final(self):
        # Все пять таблиц — ReplacingMergeTree: без FINAL вытесненные копии
        # дали бы дубли точек и поехавший as-of.
        for sql in (
            km.MEETINGS_SQL,
            km.KEY_RATE_SQL,
            km.CPD_CORE_SQL,
            km.IBK_SQL,
            km.MP_SUMMARY_SQL,
        ):
            self.assertIn("FINAL", sql)
            self.assertIn("FORMAT JSON", sql)

    def test_series_filters_are_named(self):
        # Имена рядов — контракт с импортёрами: переименование в источнике
        # обязано ломать тест, а не обнулять признак молча.
        self.assertIn("Базовый ИПЦ", km.CPD_CORE_SQL)
        self.assertIn("source = 'cbr_survey'", km.IBK_SQL)
        self.assertIn("series = 'business_climate_sa'", km.IBK_SQL)


class TestDecisions(unittest.TestCase):
    """Выборка решений на фикстурах (спека §3)."""

    @classmethod
    def setUpClass(cls):
        cls.meetings = fixture_meetings()
        cls.key_rate = fixture_key_rate()
        cls.decisions, cls.excluded = km.build_decisions(cls.meetings, cls.key_rate, [])
        cls.by_date = {d.meeting_date: d for d in cls.decisions}

    def test_composition_is_the_resolved_part_of_the_fixture(self):
        # Ряд ставки фикстуры кончается в декабре 2016 (последняя точка —
        # 2016-12-30): окно исхода заседания 2016-12-16 (W = 2016-12-23) ещё
        # наблюдаемо, у заседаний 2017+ — уже нет.
        self.assertEqual([d.meeting_date for d in self.decisions][0], date(2014, 2, 14))
        self.assertEqual([d.meeting_date for d in self.decisions][-1], date(2016, 12, 16))
        self.assertEqual(len(self.decisions), 26)
        self.assertEqual(len(self.excluded), len(self.meetings) - len(self.decisions))
        self.assertIn(date(2017, 2, 3), self.excluded)
        self.assertNotIn(date(2016, 12, 16), self.excluded)

    def test_decisions_are_chronological(self):
        dates = [d.meeting_date for d in self.decisions]
        self.assertEqual(dates, sorted(dates))

    def test_extraordinary_pair_is_not_glued(self):
        # 2014-12-11 (+100) и 2014-12-15 (+650): окно первого кончается
        # НАКАНУНЕ второго (2014-12-14), иначе оба решения слились бы в одно.
        dec11 = self.by_date[date(2014, 12, 11)]
        self.assertEqual(dec11.rate_before, 9.5)
        self.assertEqual(dec11.rate_after, 10.5)
        self.assertEqual(dec11.step_bp, 100)
        dec15 = self.by_date[date(2014, 12, 15)]
        self.assertEqual(dec15.rate_before, 10.5)
        self.assertEqual(dec15.rate_after, 17.0)
        self.assertEqual(dec15.step_bp, 650)

    def test_rate_before_is_taken_on_the_previous_day(self):
        # Внеочередное 2014-03-03: ставка 7.0 действует с самого дня решения,
        # и только d−1 = 2014-03-02 (последняя точка 2014-02-28) даёт прежние 5.5.
        decision = self.by_date[date(2014, 3, 3)]
        self.assertEqual(decision.rate_before, 5.5)
        self.assertEqual(decision.rate_after, 7.0)
        self.assertEqual(decision.step_bp, 150)

    def test_window_ends_seven_days_later_when_meetings_are_far_apart(self):
        # 2015-01-30: следующее заседание 2015-03-13, окно 2015-02-06 = d+7
        # (ставка 15.0 действует с 2015-02-02, то есть решение уже видно).
        decision = self.by_date[date(2015, 1, 30)]
        self.assertEqual(decision.rate_before, 17.0)
        self.assertEqual(decision.rate_after, 15.0)
        self.assertEqual(decision.step_bp, -200)

    def test_y3_sign_matches_step(self):
        # Классы {−1 снижение, 0 сохранение, +1 повышение}.
        self.assertEqual(self.by_date[date(2014, 12, 15)].y3, 1)
        self.assertEqual(self.by_date[date(2016, 6, 10)].y3, -1)
        self.assertEqual(self.by_date[date(2016, 12, 16)].y3, 0)
        for decision in self.decisions:
            self.assertEqual(
                decision.y3, (decision.step_bp > 0) - (decision.step_bp < 0)
            )

    def test_meeting_without_rate_history_is_excluded(self):
        # Заседание раньше начала ряда ставки: не «шаг 0», а вне выборки.
        decisions, excluded = km.build_decisions(
            [date(2013, 9, 16)], [(date(2013, 9, 17), 5.5), (date(2013, 9, 24), 5.5)], []
        )
        self.assertEqual(decisions, [])
        self.assertEqual(excluded, [date(2013, 9, 16)])


class TestMpSummaryReconciliation(unittest.TestCase):
    """Сверка ярлыков с cbr_mp_summary (спека §3)."""

    @classmethod
    def setUpClass(cls):
        cls.summary_rows = fixture_rows("mp_summary.csv")
        cls.deltas = km.mp_deltas_from_rows(cls.summary_rows)

    def _series(self, deltas=None):
        """Ряд ставки для сводок: уровень r_i действует с d_i+1.

        Ряд ставки фикстуры — 2013–2016, а резюме начинаются 2024-02; оба
        уровня (до и после решения) берутся из САМИХ сводок: между заседаниями
        ставка не меняется, поэтому r_{i−1} — это ставка «на d_i − 1».
        """
        points = [
            (km.row_date(row["meeting_date"]) + timedelta(days=1), float(row["decision_rate"]))
            for row in self.summary_rows
        ]
        meetings = [d for d, _ in self.deltas]
        return meetings, points, (self.deltas if deltas is None else deltas)

    def test_fixture_rates_and_deltas_are_consistent(self):
        # Каждая строка: уровень после решения минус уровень предыдущего
        # заседания = дельта в б.п. (кроме первого резюме — у него дельты нет).
        rates = [float(row["decision_rate"]) for row in self.summary_rows]
        for index, (_, delta) in enumerate(self.deltas):
            if index == 0 or delta is None:
                continue
            self.assertAlmostEqual(rates[index] - rates[index - 1], delta / 100, places=9)

    def test_steps_match_summary_deltas(self):
        meetings, points, deltas = self._series()
        decisions, excluded = km.build_decisions(meetings, points, deltas)
        # Первое резюме (2024-02-16) — без предыдущей ставки, последнее
        # (2026-09-11) — окно исхода ещё не наблюдаемо.
        self.assertEqual(len(decisions), len(deltas) - 2)
        for decision in decisions:
            self.assertEqual(decision.step_bp, dict(deltas)[decision.meeting_date])
        self.assertIn(date(2024, 2, 16), excluded)
        self.assertIn(date(2026, 9, 11), excluded)

    def test_perturbed_delta_raises(self):
        # Подмена дельты в фикстуре: расхождение ярлыка со ставкой — ошибка
        # прогона (fail-fast), а не строка отчёта.
        meetings, points, deltas = self._series()
        perturbed = [
            (day, 100 if day == date(2024, 7, 26) else value) for day, value in deltas
        ]  # в фикстуре у 2024-07-26 +200
        with self.assertRaises(km.SampleError) as caught:
            km.build_decisions(meetings, points, perturbed)
        self.assertIn("2024-07-26", str(caught.exception))

    def test_delta_outside_the_sample_is_not_checked(self):
        # Дельта для заседания вне выборки сверять не с чем: оно не построено,
        # и падать на нём нельзя (иначе фикстура 2013–2016 и сводки 2024+
        # были бы несовместимы).
        decisions, _ = km.build_decisions(
            [date(2014, 2, 14)], fixture_key_rate(), [(date(2024, 7, 26), 200)]
        )
        self.assertEqual(len(decisions), 1)


class TestAsof(unittest.TestCase):
    """As-of слой: правила доступности (спека §4)."""

    def test_available_from_is_the_end_of_the_next_month(self):
        self.assertEqual(km.available_from_cpd(date(2013, 12, 31)), date(2014, 1, 31))
        self.assertEqual(km.available_from_cpd(date(2016, 1, 31)), date(2016, 2, 29))
        self.assertEqual(km.available_from_survey(date(2013, 1, 1)), date(2013, 2, 28))
        self.assertEqual(km.available_from_survey(date(2016, 12, 1)), date(2017, 1, 31))

    def test_point_available_later_is_not_used(self):
        # Точка 2016-11-30 доступна только с 2016-12-31, поэтому на заседание
        # 2016-12-16 идёт октябрьская.
        points = [(date(2016, 10, 31), 0.35), (date(2016, 11, 30), 0.42)]
        self.assertEqual(km.asof(points, km.available_from_cpd, date(2016, 12, 16)), 0.35)

    def test_boundary_on_the_availability_date_is_inclusive(self):
        points = [(date(2016, 10, 31), 0.35), (date(2016, 11, 30), 0.42)]
        self.assertEqual(km.available_from_cpd(date(2016, 11, 30)), date(2016, 12, 31))
        self.assertEqual(km.asof(points, km.available_from_cpd, date(2016, 12, 31)), 0.42)

    def test_boundary_one_day_before_excludes(self):
        points = [(date(2016, 10, 31), 0.35)]
        self.assertIsNone(km.asof(points, km.available_from_cpd, date(2016, 11, 29)))

    def test_point_available_exactly_on_the_meeting_date_is_used(self):
        points = [(date(2016, 10, 31), 0.35)]
        self.assertEqual(km.asof(points, km.available_from_cpd, date(2016, 11, 30)), 0.35)

    def test_missing_series_returns_none(self):
        self.assertIsNone(km.asof([], km.available_from_cpd, date(2016, 12, 16)))
        # Ряд есть, но весь опубликован позже даты заседания.
        self.assertIsNone(
            km.asof([(date(2017, 1, 31), 0.5)], km.available_from_cpd, date(2016, 12, 16))
        )


class TestRows(unittest.TestCase):
    """Признаки pi_saar и gap (спека §4)."""

    @classmethod
    def setUpClass(cls):
        cls.decisions, _ = km.build_decisions(fixture_meetings(), fixture_key_rate(), [])
        cls.cpd = km.cpd_from_rows(fixture_rows("cpd_core_2013_2017.csv"))
        cls.ibk = km.ibk_from_rows(fixture_rows("ibk_2013_2017.csv"))
        cls.rows, cls.missing = km.build_rows(cls.decisions, cls.cpd, cls.ibk)

    def test_no_meeting_is_lost(self):
        # Живая сводка ожидает «0 пропущенных» — на фикстуре ряд покрывает всю
        # выборку, и пропусков быть не должно.
        self.assertEqual(len(self.rows), len(self.decisions))
        self.assertEqual(self.missing, [])

    def test_pi_saar_is_annualized_by_hand(self):
        # Заседание 2016-12-16 берёт точку cpd 2016-10-31 (она доступна с
        # 2016-11-30; ноябрьская — только с 2016-12-31), значение 0.35 % м/м
        # (колонка Float32; JSON отдаёт кратчайшую запись).
        # ((1 + 0.0035)^12 − 1) × 100 = 4.281800719861506
        row = self.rows[-1]
        self.assertEqual(row.decision.meeting_date, date(2016, 12, 16))
        self.assertAlmostEqual(row.pi_saar, 4.281800719861506, places=12)

    def test_gap_comes_from_business_climate_sa(self):
        # ИБК SA за отчётный 2016-10 (доступна с 2016-11-30).
        row = self.rows[-1]
        self.assertAlmostEqual(row.gap, 1.4315041754127, places=12)

    def test_feature_vector_layout(self):
        # x = [rate_before, pi_saar, gap] — порядок регрессоров методов §6.
        row = self.rows[-1]
        self.assertEqual(row.x, [row.decision.rate_before, row.pi_saar, row.gap])
        self.assertEqual(row.x[0], 10.0)

    def test_missing_series_moves_the_meeting_to_missing(self):
        # Ряда нет вовсе — заседание исключается и перечисляется, а не
        # получает выдуманный ноль.
        decision = self.decisions[0]
        rows, missing = km.build_rows([decision], [], self.ibk)
        self.assertEqual(rows, [])
        self.assertEqual(missing, [decision.meeting_date])
        rows, missing = km.build_rows([decision], self.cpd, [])
        self.assertEqual(rows, [])
        self.assertEqual(missing, [decision.meeting_date])


if __name__ == "__main__":
    unittest.main()
