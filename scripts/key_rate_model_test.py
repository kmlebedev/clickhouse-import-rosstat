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

Окно срезов — 2013-01-01…2016-12-31 (имя файлов = фактическое окно). Верхняя
граница выбрана так, что окно исхода последнего заседания среза (2016-12-16,
W = 2016-12-23) наблюдаемо целиком, а заседания 2017+ лежат за рядом ставки и
проверяют правило «окно исхода вне данных ставки → заседание вне выборки»
(на живом прогоне так отсеиваются будущие заседания). В `meetings.csv` экспорт
добавляет тестовую ручную строку-триггер с УНИКАЛЬНОЙ датой (2016-12-20): на
живых данных ручные строки делят даты с каноническими, и фильтр по `title` был
бы ненаблюдаем. Файлы коммитятся: тесты обязаны воспроизводиться без сети и БД.
"""

import contextlib
import csv
import io
import sys
import tempfile
import unittest
import unittest.mock
from datetime import date, timedelta
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import key_rate_model as km

FIXTURE_DIR = Path(__file__).resolve().parent / "testdata" / "key_rate"

FIXTURE_NAMES = (
    "meetings.csv",
    "key_rate_2013_2016.csv",
    "cpd_core_2013_2016.csv",
    "ibk_2013_2016.csv",
    "mp_summary.csv",
)


def fixture_rows(name: str, directory: Path = FIXTURE_DIR) -> list:
    """Строки CSV-фикстуры словарями — в той же форме, что отдаёт ch_query."""
    with (directory / name).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def ch_query_from_fixtures(sql: str) -> list:
    """Транспорт-заглушка: живой SQL → строки соответствующей фикстуры.

    Прогоняет загрузчики и `run_summary` целиком офлайн — тем же путём, что и
    живой прогон, но на закоммиченных срезах.
    """
    if "FROM events_calendar" in sql:
        return fixture_rows("meetings.csv")
    if "FROM cbr_key_rate" in sql:
        return fixture_rows("key_rate_2013_2016.csv")
    if "FROM cbr_indicators_cpd" in sql:
        return fixture_rows("cpd_core_2013_2016.csv")
    if "FROM macro_series" in sql:
        return fixture_rows("ibk_2013_2016.csv")
    if "FROM cbr_mp_summary" in sql:
        return fixture_rows("mp_summary.csv")
    raise AssertionError(f"неизвестный SQL: {sql}")


def fixture_meetings() -> list:
    return km.meetings_from_rows(fixture_rows("meetings.csv"))


def fixture_key_rate() -> list:
    return km.key_rate_from_rows(fixture_rows("key_rate_2013_2016.csv"))


def summary_value(text: str, label: str) -> str:
    """Значение строки сводки по её подписи (без привязки к отступам)."""
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith(label):
            return stripped[len(label):].strip()
    raise AssertionError(f"нет строки {label!r} в сводке:\n{text}")


class TestSampleFilter(unittest.TestCase):
    """Список заседаний и фильтр по title (спека §3)."""

    def test_sample_title_is_pinned(self):
        # Дословный title канонических строк events_calendar: расхождение с ним
        # (переименование карточки в сиде) молча обнулило бы выборку.
        self.assertEqual(km.SAMPLE_TITLE, "СД ЦБ РФ: заседание по ключевой ставке")

    def test_fixture_contains_manual_trigger_rows(self):
        # Фикстура — ВСЯ категория 'cbr': 118 канонических строк плюс ручные
        # строки-триггеры сида. Без них фильтр по title нечем проверять.
        rows = fixture_rows("meetings.csv")
        self.assertEqual(len(rows), 121)
        canonical = sum(1 for row in rows if row["title"] == km.SAMPLE_TITLE)
        self.assertEqual(canonical, 118)
        manual = sorted(row["event_date"] for row in rows if row["title"] != km.SAMPLE_TITLE)
        self.assertEqual(manual, ["2016-12-20", "2026-10-23", "2026-12-18"])

    def test_title_filter_has_observable_effect_on_the_fixture(self):
        # Дата 2016-12-20 есть ТОЛЬКО у ручной строки-триггера (обе живые ручные
        # строки делят даты с каноническими, и там фильтр по датам не виден).
        # Без фильтра эта дата попала бы в список заседаний и дала бы лишнее
        # «решение» — то есть тест падает не на отсутствии строки, а на её эффекте.
        unique_day = [
            row for row in fixture_rows("meetings.csv") if row["event_date"] == "2016-12-20"
        ]
        self.assertEqual(len(unique_day), 1)
        self.assertNotEqual(unique_day[0]["title"], km.SAMPLE_TITLE)
        self.assertNotIn(date(2016, 12, 20), fixture_meetings())

    def test_title_filter_keeps_only_canonical_meetings(self):
        # 121 строка → 118 заседаний: живые ручные строки делят даты с
        # каноническими (2026-10-23 и 2026-12-18 помечены и сидом, и календарём
        # ЦБ), поэтому по датам дедуп делает их невидимыми, а эффект фильтра
        # даёт тестовая строка от экспорта (см. предыдущий тест).
        meetings = fixture_meetings()
        self.assertEqual(len(meetings), 118)
        self.assertEqual(meetings[0], date(2014, 2, 14))
        self.assertEqual(meetings[-1], date(2027, 12, 17))


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
        rows = [{"date": "2013-12-31", "value": 0.3}]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows) as query:
            self.assertEqual(km.load_cpd_core(), [(date(2013, 12, 31), 0.3)])
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

    def test_duplicate_dates_are_rejected(self):
        # Дубль даты в ряду — потерянный FINAL или неверный ключ: as-of выбрал бы
        # одну из двух точек по порядку строк, и решение молча зависело бы от
        # порядка ответа. Поэтому отказ с датой-дубликатом, а не «последний победил».
        cases = (
            (
                km.key_rate_from_rows,
                [{"date": "2014-12-11", "rate": 9.5}, {"date": "2014-12-11", "rate": 10.5}],
            ),
            (
                km.cpd_from_rows,
                [{"date": "2016-10-31", "value": 0.35}, {"date": "2016-10-31", "value": 0.4}],
            ),
            (
                km.ibk_from_rows,
                [{"date": "2016-10-01", "value": 1.4}, {"date": "2016-10-01", "value": 2.0}],
            ),
            (
                km.mp_deltas_from_rows,
                [
                    {"meeting_date": "2024-07-26", "decision_delta_bp": 200},
                    {"meeting_date": "2024-07-26", "decision_delta_bp": 100},
                ],
            ),
        )
        for parser, rows in cases:
            with self.assertRaises(ValueError) as caught:
                parser(rows)
            self.assertIn("встречается дважды", str(caught.exception))
            self.assertIn(rows[0]["date"] if "date" in rows[0] else rows[0]["meeting_date"],
                          str(caught.exception))

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

    def test_fixture_queries_are_windowed(self):
        # Срезы фикстур — окно [2013-01-01, 2016-12-31] (имя файлов = окну).
        # Проверяются границы в тексте WHERE, а не только FINAL/FORMAT JSON:
        # снятый диапазон раздул бы фикстуры до всей истории, а суженный —
        # сломал бы пин состава решений.
        for sql in (km.FIXTURE_KEY_RATE_SQL, km.FIXTURE_CPD_CORE_SQL, km.FIXTURE_IBK_SQL):
            self.assertIn("date >= '2013-01-01'", sql)
            self.assertIn("date <= '2016-12-31'", sql)
            self.assertIn("FINAL", sql)
            self.assertIn("FORMAT JSON", sql)
            self.assertLess(sql.index("date >= '2013-01-01'"), sql.index("ORDER BY "))
        # Список заседаний и сводки выгружаются целиком — окна у них нет.
        self.assertNotIn("date >=", km.MEETINGS_SQL)
        self.assertNotIn("date >=", km.MP_SUMMARY_SQL)
        # Фильтры рядов в фикстурных запросах сохраняются.
        self.assertIn("Базовый ИПЦ", km.FIXTURE_CPD_CORE_SQL)
        self.assertIn("series = 'business_climate_sa'", km.FIXTURE_IBK_SQL)


class TestDecisions(unittest.TestCase):
    """Выборка решений на фикстурах (спека §3)."""

    @classmethod
    def setUpClass(cls):
        cls.meetings = fixture_meetings()
        cls.key_rate = fixture_key_rate()
        cls.decisions, cls.excluded, cls.missing = km.build_decisions(
            cls.meetings, cls.key_rate, []
        )
        cls.by_date = {d.meeting_date: d for d in cls.decisions}

    def test_composition_is_the_resolved_part_of_the_fixture(self):
        # Ряд ставки фикстуры кончается в декабре 2016 (последняя точка —
        # 2016-12-30): окно исхода заседания 2016-12-16 (W = 2016-12-23) ещё
        # наблюдаемо, у заседаний 2017+ — уже нет.
        self.assertEqual([d.meeting_date for d in self.decisions][0], date(2014, 2, 14))
        self.assertEqual([d.meeting_date for d in self.decisions][-1], date(2016, 12, 16))
        self.assertEqual(len(self.decisions), 26)
        self.assertEqual(len(self.excluded), len(self.meetings) - len(self.decisions))
        self.assertEqual(self.missing, [])
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

    def test_meeting_without_rate_history_is_missing(self):
        # Заседание раньше начала ряда ставки: «пропущено» (нет ставки на d−1),
        # а НЕ «вне выборки»: причина другая, и отчёт обязан их различать.
        decisions, excluded, missing = km.build_decisions(
            [date(2013, 9, 16)], [(date(2013, 9, 17), 5.5), (date(2013, 9, 24), 5.5)], []
        )
        self.assertEqual(decisions, [])
        self.assertEqual(excluded, [])
        self.assertEqual(missing, [date(2013, 9, 16)])

    def test_meeting_at_the_series_edge_is_excluded(self):
        # Заседание внутри ряда ставки, но окно исхода (d+7) выходит за его
        # максимум: «вне выборки», а не «пропущено» — ставка на d−1 известна.
        key_rate = [(date(2016, 12, 1), 10.0), (date(2016, 12, 30), 10.0)]
        decisions, excluded, missing = km.build_decisions([date(2016, 12, 28)], key_rate, [])
        self.assertEqual(decisions, [])
        self.assertEqual(excluded, [date(2016, 12, 28)])
        self.assertEqual(missing, [])

    def test_only_the_meeting_beyond_the_edge_is_excluded(self):
        # Соседние заседания: у 2015-12-11 окно (2015-12-18) внутри ряда, у
        # 2016-12-16 — тоже (2016-12-23); краевое 2016-12-28 — вне.
        key_rate = [(date(2015, 12, 10), 11.0), (date(2016, 12, 22), 10.0)]
        decisions, excluded, missing = km.build_decisions(
            [date(2015, 12, 11), date(2016, 12, 28)], key_rate, []
        )
        self.assertEqual([d.meeting_date for d in decisions], [date(2015, 12, 11)])
        self.assertEqual(excluded, [date(2016, 12, 28)])
        self.assertEqual(missing, [])


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
        decisions, excluded, missing = km.build_decisions(meetings, points, deltas)
        # Первое резюме (2024-02-16) — раньше начала синтетического ряда:
        # «пропущено» (нет ставки на d−1), а не «вне выборки». Последнее
        # (2026-09-11) — окно исхода ещё не наблюдаемо: «вне выборки».
        self.assertEqual(len(decisions), len(deltas) - 2)
        for decision in decisions:
            self.assertEqual(decision.step_bp, dict(deltas)[decision.meeting_date])
        self.assertEqual(missing, [date(2024, 2, 16)])
        self.assertEqual(excluded, [date(2026, 9, 11)])

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
        decisions, excluded, missing = km.build_decisions(
            [date(2014, 2, 14)], fixture_key_rate(), [(date(2024, 7, 26), 200)]
        )
        self.assertEqual(len(decisions), 1)
        self.assertEqual(excluded, [])
        self.assertEqual(missing, [])


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
        cls.decisions, _, _ = km.build_decisions(fixture_meetings(), fixture_key_rate(), [])
        cls.cpd = km.cpd_from_rows(fixture_rows("cpd_core_2013_2016.csv"))
        cls.ibk = km.ibk_from_rows(fixture_rows("ibk_2013_2016.csv"))
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


class TestExportFixtures(unittest.TestCase):
    """Экспорт фикстур: окно, LF, round-trip и отказ на пустом срезе."""

    def export(self, directory: Path) -> int:
        """Экспорт с замоканным транспортом и подавленной печатью."""
        with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
            with contextlib.redirect_stdout(io.StringIO()):
                return km.export_fixtures(directory)

    def test_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            self.assertEqual(self.export(directory), 0)
            self.assertEqual(
                sorted(path.name for path in directory.iterdir()),
                [
                    "cpd_core_2013_2016.csv",
                    "ibk_2013_2016.csv",
                    "key_rate_2013_2016.csv",
                    "meetings.csv",
                    "mp_summary.csv",
                ],
            )
            for name in FIXTURE_NAMES:
                raw = (directory / name).read_bytes()
                self.assertNotIn(b"\r\n", raw, f"{name}: ожидались LF-переводы строк")
                self.assertTrue(raw.endswith(b"\n"))
                self.assertEqual(raw.split(b"\n", 1)[0].decode("utf-8"),
                                 ",".join(km.FIXTURE_COLUMNS[name]))
            # Содержимое читается теми же разборщиками, что и живые ответы.
            self.assertEqual(
                km.meetings_from_rows(fixture_rows("meetings.csv", directory)),
                fixture_meetings(),
            )
            self.assertEqual(
                km.key_rate_from_rows(fixture_rows("key_rate_2013_2016.csv", directory)),
                fixture_key_rate(),
            )
            self.assertEqual(
                km.cpd_from_rows(fixture_rows("cpd_core_2013_2016.csv", directory)),
                km.cpd_from_rows(fixture_rows("cpd_core_2013_2016.csv")),
            )
            self.assertEqual(
                km.mp_deltas_from_rows(fixture_rows("mp_summary.csv", directory)),
                km.mp_deltas_from_rows(fixture_rows("mp_summary.csv")),
            )

    def test_exported_meetings_carry_the_unique_manual_row(self):
        # Регенерация обязана давать тот же файл, включая тестовую ручную строку:
        # она добавляется ЭКСПОРТОМ, а не правкой CSV руками.
        with tempfile.TemporaryDirectory() as tmp:
            self.export(Path(tmp))
            rows = fixture_rows("meetings.csv", Path(tmp))
            self.assertIn(
                "2016-12-20", [row["event_date"] for row in rows if row["title"] != km.SAMPLE_TITLE]
            )

    def test_empty_slice_is_an_export_error(self):
        # Пустая фикстура ослабила бы тесты молча — это ошибка экспорта
        # (RuntimeError), а не расхождение выборки (SampleError).
        def empty_key_rate(sql):
            if "FROM cbr_key_rate" in sql:
                return []
            return ch_query_from_fixtures(sql)

        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(km, "ch_query", side_effect=empty_key_rate):
                with contextlib.redirect_stdout(io.StringIO()):
                    with self.assertRaises(RuntimeError) as caught:
                        km.export_fixtures(Path(tmp))
            self.assertIn("key_rate_2013_2016.csv", str(caught.exception))
            # Отказ — до записи: ни одного файла каталог не получил.
            self.assertEqual(list(Path(tmp).iterdir()), [])


class TestCli(unittest.TestCase):
    """CLI: сводка и перехват ошибки выборки."""

    def test_summary_counts_decisions_excluded_and_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
                with contextlib.redirect_stdout(io.StringIO()) as out:
                    code = km.run_summary(
                        rolling=False, today=date(2016, 1, 1), out_dir=Path(tmp)
                    )
        text = out.getvalue()
        self.assertEqual(code, 0)
        # Прошедшие заседания фикстуры по 2016-01-01 — 18 (2014: 10, 2015: 8);
        # все с наблюдаемым окном и признаками.
        self.assertTrue(summary_value(text, "в выборке решений:").startswith("18 ("))
        self.assertTrue(summary_value(text, "вне выборки (окно исхода не наблюдаемо):").startswith("0"))
        self.assertTrue(summary_value(text, "пропущено (нет ставки на d−1):").startswith("0"))
        self.assertTrue(summary_value(text, "пропущено (нет данных по признакам):").startswith("0"))
        self.assertTrue(summary_value(text, "предстоящих (позже даты прогона):").startswith("100"))
        # 18 решений короче обучения (24) — фолдов нет ни в одном окне, и
        # сводка печатает это числом, а не молчит (отчёт рендерится и пустым).
        self.assertTrue(
            summary_value(text, f"фолдов (расширяющееся окно, min_train={km.MIN_TRAIN}):")
            .startswith("0")
        )
        self.assertTrue(
            summary_value(text, f"фолдов (скользящее окно, {km.ROLLING_WINDOW}):").startswith("0")
        )
        self.assertIn("(фолдов нет", text)

    def test_main_reports_sample_error_without_traceback(self):
        # Расхождение с cbr_mp_summary: main обязан вернуть ненулевой код и
        # напечатать причину в stderr, а не упасть трейсбеком.
        with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
            with unittest.mock.patch.object(
                km, "load_mp_summary_deltas", return_value=[(date(2014, 2, 14), 50)]
            ):
                error = io.StringIO()
                with contextlib.redirect_stderr(error), contextlib.redirect_stdout(io.StringIO()):
                    code = km.main([])
        self.assertNotEqual(code, 0)
        message = error.getvalue()
        self.assertIn("2014-02-14", message)
        self.assertIn("не совпал", message)
        self.assertNotIn("Traceback", message)


# --- Синтетика для harness'а (Task 3) -----------------------------------------
#
# Walk-forward, метрики и тесты значимости проверяются на СИНТЕТИЧЕСКИХ
# строках, а не на фикстурах: в фикстуре 26 решений, и при `min_train=24`
# расширяющееся окно даёт 2 фолда, а скользящее 40 — ни одного. Поведение
# окон, метрик и вырожденных случаев на таком материале не проверяется
# (решение контроллера, план Task 3).


def synthetic_rows(count, first=date(2014, 1, 1), rate=10.0, steps=(0,), pi=4.0, gap=0.0):
    """Синтетическая выборка: заседание раз в 30 дней, шаг из `steps` по кругу.

    Уровень ставки накапливается, поэтому `rate_before`/`rate_after`/
    `step_bp`/`y3` каждой строки известны точно, а прогнозы бенчмарков
    сверяются руками (шаг B2 — шаг предыдущей строки и т.п.).
    """
    rows = []
    level = rate
    for index in range(count):
        step = steps[index % len(steps)]
        after = level + step / 100.0
        decision = km.Decision(
            first + timedelta(days=30 * index), level, after, step, (step > 0) - (step < 0)
        )
        rows.append(km.Row(decision, pi, gap))
        level = after
    return rows


def ar1_rows(count, intercept=10.0, phi=0.5):
    """Строки с ТОЧНОЙ связью `rate_after = intercept + phi·rate_before`.

    OLS на такой синтетике обязан восстановить коэффициенты, и прогноз B3
    `pred_level − rate_before` считается руками.
    """
    rows = []
    for index in range(count):
        before = 10.0 + index
        after = intercept + phi * before
        step = round((after - before) * 100.0)
        decision = km.Decision(
            date(2014, 1, 1) + timedelta(days=30 * index),
            before,
            after,
            step,
            (step > 0) - (step < 0),
        )
        rows.append(km.Row(decision, 4.0, 0.0))
    return rows


def benchmark_predictions(rows):
    """Бенчмарки B1–B3 по двум окнам — ровно так, как их строит прогон."""
    predictions = []
    for window in (None, km.ROLLING_WINDOW):
        for method, fit_predict in km.BENCHMARKS:
            predictions.extend(km.walk_forward(rows, method, fit_predict, rolling=window))
    return predictions


class TestWalkForward(unittest.TestCase):
    """Walk-forward: обучение только на прошлом, два окна (спека §7)."""

    def probe(self, seen):
        """Фиктивная модель, отвечающая длиной обучающего окна."""

        def fit_predict(train, row):
            seen.append(len(train))
            return km.Forecast(float(len(train)), None)

        return fit_predict

    def test_expanding_window_trains_only_on_the_past(self):
        rows = synthetic_rows(60)
        seen = []
        predictions = km.walk_forward(rows, "probe", self.probe(seen), min_train=24)
        # Длины строго растут: каждый фолд видит ВСЁ прошлое и только его.
        self.assertEqual(seen, list(range(24, 60)))
        # Первый прогноз — 25-е заседание (24 в обучении), последнее — 60-е.
        self.assertEqual(len(predictions), 36)
        self.assertEqual(predictions[0].meeting_date, rows[24].decision.meeting_date)
        self.assertEqual(predictions[-1].meeting_date, rows[-1].decision.meeting_date)
        dates = [prediction.meeting_date for prediction in predictions]
        self.assertEqual(dates, sorted(dates))
        self.assertEqual(len(set(dates)), len(dates))
        self.assertEqual({prediction.method for prediction in predictions}, {"probe"})
        self.assertEqual({prediction.window for prediction in predictions}, {"expanding"})
        self.assertEqual(predictions[0].step_bp, 24.0)
        self.assertEqual(predictions[0].actual_step_bp, float(rows[24].decision.step_bp))
        self.assertEqual(predictions[0].actual_y3, rows[24].decision.y3)

    def test_rolling_window_enters_the_run_full(self):
        rows = synthetic_rows(60)
        seen = []
        predictions = km.walk_forward(
            rows, "probe", self.probe(seen), min_train=24, rolling=40
        )
        # Окно скользящее 40 входит ПОЛНЫМ: первый фолд — 41-е заседание,
        # то есть 60 − 40 = 20 фолдов (иначе «окно 40» начиналось бы с 24).
        self.assertEqual(seen, [40] * 20)
        self.assertEqual(len(predictions), 20)
        self.assertEqual(predictions[0].meeting_date, rows[40].decision.meeting_date)
        self.assertEqual({prediction.window for prediction in predictions}, {"rolling40"})

    def test_sample_shorter_than_min_train_gives_no_folds(self):
        # Выборка короче обучения — пустой список, а не исключение: отчёт
        # этого шага (например, на срезе фикстур) обязан рендериться.
        self.assertEqual(km.walk_forward(synthetic_rows(20), "hold", km.predict_hold), [])

    def test_probs_and_step_are_passed_through(self):
        def fit_predict(train, row):
            return km.Forecast(1.5, (0.1, 0.8, 0.1))

        predictions = km.walk_forward(synthetic_rows(26), "probe", fit_predict, min_train=24)
        self.assertEqual(predictions[0].step_bp, 1.5)
        self.assertEqual(predictions[0].probs, (0.1, 0.8, 0.1))

    def test_hold_predicts_zero_without_probs(self):
        predictions = km.walk_forward(
            synthetic_rows(30, steps=(50,)), "hold", km.predict_hold, min_train=24
        )
        self.assertEqual(len(predictions), 6)
        for prediction in predictions:
            self.assertEqual(prediction.step_bp, 0.0)
            self.assertIsNone(prediction.probs)

    def test_repeat_repeats_the_previous_decision(self):
        rows = synthetic_rows(60, steps=(0, 50, -25))
        predictions = km.walk_forward(rows, "repeat", km.predict_repeat, min_train=24)
        self.assertEqual(len(predictions), 36)
        for prediction, index in zip(predictions, range(24, 60)):
            self.assertEqual(prediction.step_bp, float(rows[index - 1].decision.step_bp))

    def test_repeat_needs_a_previous_decision(self):
        with self.assertRaises(ValueError) as caught:
            km.predict_repeat([], synthetic_rows(1)[0])
        self.assertIn("предыдущего решения", str(caught.exception))

    def test_ar1_recovers_the_synthetic_level_relation(self):
        rows = ar1_rows(40)
        train = rows[:24]
        row = rows[24]
        forecast = km.fit_ar1(train, row)
        # pred_level = 10 + 0.5·rate_before; прогноз шага — разность уровней.
        expected = 10.0 + 0.5 * row.decision.rate_before - row.decision.rate_before
        self.assertAlmostEqual(forecast.step_bp, expected, places=9)
        self.assertIsNone(forecast.probs)

    def test_ar1_uses_hac_with_the_pinned_configuration(self):
        # Конфигурация HAC — часть спецификации (спека §6): Ньюи–Уэст, 4 лага
        # (≈ полугодие между заседаниями), малая выборка с поправкой. Тест
        # подменяет OLS и проверяет сам вызов: подмена ковариации невидима
        # в числах прогноза, а сравнение p-value с методами №2–3 — нет.
        rows = ar1_rows(40)
        with unittest.mock.patch.object(km.sm, "OLS") as ols:
            ols.return_value.fit.return_value.params = [10.0, 0.5]
            forecast = km.fit_ar1(rows[:24], rows[24])
        kwargs = ols.return_value.fit.call_args.kwargs
        self.assertEqual(kwargs["cov_type"], "HAC")
        self.assertEqual(kwargs["cov_kwds"], {"maxlags": 4, "use_correction": True})
        row = rows[24]
        self.assertAlmostEqual(
            forecast.step_bp,
            10.0 + 0.5 * row.decision.rate_before - row.decision.rate_before,
            places=12,
        )

    def test_ar1_rejects_a_window_too_short_for_ols(self):
        rows = ar1_rows(3)
        with self.assertRaises(ValueError) as caught:
            km.fit_ar1(rows[:2], rows[2])
        self.assertIn("AR(1)", str(caught.exception))
        self.assertIn("OLS", str(caught.exception))

    def test_ar1_rejects_a_window_without_level_variation(self):
        # Уровень в окне неизменен: наклон φ коллинеарен свободному члену,
        # регрессия не определена — понятная ошибка, а не падение statsmodels.
        rows = synthetic_rows(30, steps=(0,))
        with self.assertRaises(ValueError) as caught:
            km.fit_ar1(rows[:24], rows[24])
        self.assertIn("не меняется", str(caught.exception))


class TestMetrics(unittest.TestCase):
    """Метрики §7: числа проверяются руками на фиксированных векторах."""

    def test_rmse_and_mae_on_numbers(self):
        self.assertAlmostEqual(km.rmse_bp([10.0, -10.0]), 10.0, places=12)
        self.assertAlmostEqual(km.mae_bp([10.0, -10.0]), 10.0, places=12)
        # RMSE([3, 4]) = sqrt((9 + 16)/2) = sqrt(12.5); MAE = 3.5.
        self.assertAlmostEqual(km.rmse_bp([3.0, 4.0]), 3.5355339059327378, places=12)
        self.assertAlmostEqual(km.mae_bp([3.0, 4.0]), 3.5, places=12)

    def test_metrics_refuse_an_empty_list(self):
        # Пустой список — это «фолдов нет»: ноль на его месте выглядел бы
        # измеренной ошибкой, и отчёт молча показал бы идеальную модель.
        for metric in (km.rmse_bp, km.mae_bp):
            with self.assertRaises(ValueError):
                metric([])

    def test_hit_rate_counts_matching_directions(self):
        # Совпали 3 из 4: 10↔20 (вверх), −10↔−5 (вниз), 0↔0 (сохранение);
        # 5↔−3 — промах.
        self.assertAlmostEqual(
            km.hit_rate([10.0, -10.0, 0.0, 5.0], [20.0, -5.0, 0.0, -3.0]), 0.75, places=12
        )

    def test_confusion3_rows_are_fact_and_columns_are_forecast(self):
        # Тот же вектор: факт (1, −1, 0, −1) против прогноза (1, −1, 0, 1)
        # в порядке классов (−1, 0, +1).
        matrix = km.confusion3([10.0, -10.0, 0.0, 5.0], [20.0, -5.0, 0.0, -3.0])
        self.assertEqual(
            matrix,
            [
                [1, 0, 1],  # факт −1: прогнозы −1 (1 шт.) и +1 (1 шт.)
                [0, 1, 0],  # факт 0: прогноз 0
                [0, 0, 1],  # факт +1: прогноз +1
            ],
        )
        self.assertEqual(sum(sum(row) for row in matrix), 4)

    def test_qwk_on_a_hand_counted_example(self):
        # Факт (−1, 0, 0, +1), прогноз (−1, 0, +1, +1): единственное
        # расхождение — факт 0 при прогнозе +1 (вес (1/4)).
        #   O = [[1,0,0],[0,1,1],[0,0,1]], маргиналы факта (1,2,1),
        #   прогноза (1,1,2); Σw·E = 1.25, Σw·O = 0.25 → QWK = 1 − 0.2 = 0.8.
        self.assertAlmostEqual(
            km.qwk([-1.0, 0.0, 1.0, 1.0], [-1.0, 0.0, 0.0, 1.0]), 0.8, places=12
        )

    def test_qwk_is_one_on_perfect_agreement(self):
        self.assertAlmostEqual(km.qwk([0.0, 50.0, -25.0], [0.0, 50.0, -25.0]), 1.0, places=12)

    def test_qwk_degenerate_case_is_one(self):
        # Все прогнозы и факты — «сохранение»: ожидаемое расхождение нулевое,
        # kappa не определён; соглашение — 1.0 (полное совпадение).
        self.assertAlmostEqual(km.qwk([0.0, 0.0, 0.0], [0.0, 0.0, 0.0]), 1.0, places=12)

    def test_qwk_does_not_reward_a_constant_forecast(self):
        # B1 («ставка не изменится») при 3 из 4 сохранений получает QWK = 0:
        # постоянный прогноз не несёт информации, и метрика это показывает.
        self.assertAlmostEqual(
            km.qwk([0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 50.0, 0.0]), 0.0, places=12
        )

    def test_brier_multi_on_known_probabilities(self):
        # Идеальный прогноз — 0; (0.5, 0.3, 0.2) при факте «сохранение» —
        # (0.5−0)² + (0.3−1)² + (0.2−0)² = 0.78.
        self.assertAlmostEqual(km.brier_multi([(1.0, 0.0, 0.0)], [-1]), 0.0, places=12)
        self.assertAlmostEqual(km.brier_multi([(0.5, 0.3, 0.2)], [0]), 0.78, places=12)
        # Два фолда: 0.06 и 0.14 → 0.10.
        self.assertAlmostEqual(
            km.brier_multi([(0.8, 0.1, 0.1), (0.1, 0.2, 0.7)], [-1, 1]), 0.10, places=12
        )

    def test_log_loss_multi_on_known_probabilities(self):
        # −ln 0.8 = 0.22314355131420976.
        self.assertAlmostEqual(
            km.log_loss_multi([(0.8, 0.1, 0.1)], [-1]), 0.22314355131420976, places=12
        )
        # Нулевая вероятность факта не даёт бесконечности: вероятность
        # снизу ограничена 1e-15 → −ln(1e-15) = 34.538776394910684.
        self.assertAlmostEqual(
            km.log_loss_multi([(0.0, 1.0, 0.0)], [-1]), 34.538776394910684, places=9
        )

    def test_probabilistic_metrics_refuse_a_broken_distribution(self):
        for metric in (km.brier_multi, km.log_loss_multi, km.reliability_bins):
            with self.assertRaises(ValueError):
                metric([(0.5, 0.3, 0.3)], [0])  # сумма 1.1, не распределение
            with self.assertRaises(ValueError):
                metric([(0.5, 0.5, 0.5)], [0])

    def test_reliability_bins_refuse_a_non_positive_bin_count(self):
        with self.assertRaises(ValueError):
            km.reliability_bins([(1.0, 0.0, 0.0)], [-1], n_bins=0)

    def test_metrics_report_length_mismatch(self):
        for call in (
            lambda: km.hit_rate([1.0], [1.0, 2.0]),
            lambda: km.confusion3([1.0], [1.0, 2.0]),
            lambda: km.qwk([1.0], [1.0, 2.0]),
            lambda: km.brier_multi([(1.0, 0.0, 0.0)], [1, 0]),
            lambda: km.log_loss_multi([(1.0, 0.0, 0.0)], [1, 0]),
            lambda: km.reliability_bins([(1.0, 0.0, 0.0)], [1, 0]),
        ):
            with self.assertRaises(ValueError):
                call()

    def test_reliability_bins_counts_and_shares(self):
        probs = [
            (0.15, 0.55, 0.30),  # уверенность 0.55, класс 0 — попадание
            (0.10, 0.62, 0.28),  # уверенность 0.62, класс 0 — промах (факт +1)
            (0.02, 0.03, 0.95),  # уверенность 0.95, класс +1 — попадание
            (0.85, 0.10, 0.05),  # уверенность 0.85, класс −1 — попадание
        ]
        y_true = [0, 1, 1, -1]
        bins = km.reliability_bins(probs, y_true, n_bins=5)
        # Пустые бины в таблицу не попадают: у них нет ни счётчика, ни долей.
        self.assertEqual(
            [(b.lo, b.hi, b.count) for b in bins],
            [(0.4, 0.6, 1), (0.6, 0.8, 1), (0.8, 1.0, 2)],
        )
        self.assertAlmostEqual(bins[0].mean_prob, 0.55, places=12)
        self.assertAlmostEqual(bins[0].accuracy, 1.0, places=12)
        self.assertAlmostEqual(bins[1].mean_prob, 0.62, places=12)
        self.assertAlmostEqual(bins[1].accuracy, 0.0, places=12)
        self.assertAlmostEqual(bins[2].mean_prob, 0.90, places=12)
        self.assertAlmostEqual(bins[2].accuracy, 1.0, places=12)


class TestDieboldMariano(unittest.TestCase):
    """DM с поправкой HLN (спека §7); соглашение о вырождении — план Task 3."""

    def test_identical_errors_give_zero_and_one(self):
        errors = [10.0, -10.0, 0.0]
        self.assertEqual(km.diebold_mariano(errors, list(errors)), (0.0, 1.0))

    def test_zero_variance_gives_infinite_statistic(self):
        # Разница потерь постоянна и не нулевая: преимущество точное и
        # бесспорное → ±inf при p = 0 (соглашение).
        statistic, pvalue = km.diebold_mariano([5.0] * 10, [0.0] * 10)
        self.assertEqual(statistic, float("inf"))
        self.assertEqual(pvalue, 0.0)
        statistic, pvalue = km.diebold_mariano([0.0] * 10, [5.0] * 10)
        self.assertEqual(statistic, float("-inf"))
        self.assertEqual(pvalue, 0.0)

    def test_known_pair_matches_the_manual_hln_calculation(self):
        # err_a = [1..5], err_b = [0..4] → d = e_a² − e_b² = [1, 3, 5, 7, 9],
        # d̄ = 5; HAC-Бартлетт с 2 лагами (⌈T^{1/3}⌉ — правило библиотеки):
        # γ̂0 = 8, γ̂1 = 3.2, γ̂2 = −0.8 → s = 11.7333;
        # DM = 5/√(s/T) = 3.2639560…; HLN: ×√((T−1)/T) = 2.9193710…;
        # p — из t(T−1 = 4) = 0.043270657626682994.
        statistic, pvalue = km.diebold_mariano([1.0, 2.0, 3.0, 4.0, 5.0], [0.0, 1.0, 2.0, 3.0, 4.0])
        self.assertAlmostEqual(statistic, 2.9193710406057116, places=9)
        self.assertAlmostEqual(pvalue, 0.043270657626682994, places=9)

    def test_swapping_the_series_flips_the_statistic(self):
        err_a = [1.0, 2.0, 3.0, 4.0, 5.0]
        err_b = [0.0, 1.0, 2.0, 3.0, 4.0]
        forward, p_forward = km.diebold_mariano(err_a, err_b)
        backward, p_backward = km.diebold_mariano(err_b, err_a)
        self.assertGreater(forward, 0.0)  # err_a хуже: потеря больше
        self.assertAlmostEqual(forward, -backward, places=12)
        self.assertAlmostEqual(p_forward, p_backward, places=12)

    def test_single_fold_does_not_crash(self):
        statistic, pvalue = km.diebold_mariano([3.0], [1.0])
        self.assertEqual(statistic, float("inf"))
        self.assertEqual(pvalue, 0.0)

    def test_empty_and_mismatched_series_are_refused(self):
        with self.assertRaises(ValueError):
            km.diebold_mariano([], [])
        with self.assertRaises(ValueError):
            km.diebold_mariano([1.0], [1.0, 2.0])


class TestPesaranTimmermann(unittest.TestCase):
    """PT по знакам (спека §7); путь — statsmodels, вырождение — соглашение."""

    def test_known_hit_table_matches_the_manual_formula(self):
        # Таблица попаданий по знаку: факт (+, +, +, 0, 0) против прогноза
        # (+, −, −, 0, 0) → p̂ = 3/5, p̂y = 3/5, p̂z = 1/5, p̂* = 11/25,
        # v̂ = 0.04928, ŵ = 0.01856 → S = 0.16/√0.03072 = 0.912870929…,
        # двустороннее p = 0.361310429….
        statistic, pvalue = km.pesaran_timmermann(
            [1, -1, -1, 0, 0], [1, 1, 1, 0, 0]
        )
        self.assertAlmostEqual(statistic, 0.9128709291752763, places=12)
        self.assertAlmostEqual(pvalue, 0.36131042852617923, places=12)

    def test_perfect_directional_predictor(self):
        # p̂ = 1, p̂y = p̂z = 0.5 → S = 0.5/√(0.00625) = 6.324555…, p ≈ 0.
        statistic, pvalue = km.pesaran_timmermann([1, -1] * 20, [1, -1] * 20)
        self.assertAlmostEqual(statistic, 6.324555320336759, places=9)
        self.assertLess(pvalue, 1e-9)

    def test_no_sign_variation_is_undefined_by_convention(self):
        # Бенчмарк B1 не меняет знак вовсе — тест не определён; соглашение
        # (как у DM на одинаковых ошибках): (0.0, 1.0).
        self.assertEqual(km.pesaran_timmermann([0, 0, 0], [0, 1, -1]), (0.0, 1.0))
        self.assertEqual(km.pesaran_timmermann([0, 0, -1], [0, 1, -1]), (0.0, 1.0))

    def test_empty_and_mismatched_series_are_refused(self):
        with self.assertRaises(ValueError):
            km.pesaran_timmermann([], [])
        with self.assertRaises(ValueError):
            km.pesaran_timmermann([1, 0], [1])


class TestMincerZarnowitz(unittest.TestCase):
    """MZ: регрессия «факт на прогноз» с HAC, гипотеза a = 0 ∧ b = 1."""

    def test_slope_recovers_a_doubled_forecast(self):
        # Точная синтетика `факт = 2×прогноз`: b = 2, a = 0.
        predicted = [10.0 + index for index in range(40)]
        actual = [2.0 * value for value in predicted]
        result = km.mincer_zarnowitz(actual, predicted)
        self.assertAlmostEqual(result.slope, 2.0, places=9)
        self.assertLess(abs(result.intercept), 1e-9)
        # H0 (a = 0 ∧ b = 1) нарушена, и остатков нет: соглашение о точной
        # подгонке даёт p = 0 (дисперсия оценки нулевая — статистика ±inf).
        self.assertEqual(result.p_value, 0.0)

    def test_exact_fit_under_the_null_does_not_reject(self):
        # Точная подгонка при ВЕРНОЙ H0: остатков нет, F = 0/0; без соглашения
        # шум округления дал бы ложное p ≈ 0.0001.
        predicted = [10.0 + index for index in range(40)]
        result = km.mincer_zarnowitz(list(predicted), predicted)
        self.assertAlmostEqual(result.slope, 1.0, places=9)
        self.assertEqual(result.p_value, 1.0)

    def test_noisy_data_with_a_wrong_slope_is_rejected(self):
        predicted = [10.0 + index for index in range(40)]
        actual = [2.0 * value + 0.01 * (-1) ** index for index, value in enumerate(predicted)]
        result = km.mincer_zarnowitz(actual, predicted)
        self.assertAlmostEqual(result.slope, 2.0, places=3)
        self.assertLess(result.p_value, 0.001)

    def test_noisy_data_under_the_null_is_not_rejected(self):
        predicted = [10.0 + index for index in range(40)]
        actual = [value + 0.01 * (-1) ** index for index, value in enumerate(predicted)]
        result = km.mincer_zarnowitz(actual, predicted)
        self.assertAlmostEqual(result.slope, 1.0, places=3)
        self.assertGreater(result.p_value, 0.05)

    def test_constant_forecast_and_tiny_samples_return_none(self):
        # Постоянный прогноз коллинеарен свободному члену (так выглядит B1),
        # двух точек мало для HAC-регрессии: регрессия не определена — None,
        # в отчёте «—», прогон не падает.
        self.assertIsNone(km.mincer_zarnowitz([1.0] * 10, [1.0] * 10))
        self.assertIsNone(km.mincer_zarnowitz([1.0, 2.0], [1.0, 2.0]))


class TestBootstrap(unittest.TestCase):
    """Блочный бутстрап разницы RMSE (спека §7, план Task 3)."""

    def test_repeats_with_the_fixed_seed(self):
        err_a = [3.0, -2.0, 5.0, 0.0, -1.0, 4.0, 2.0, -3.0]
        err_b = [1.0, 1.0, -2.0, 2.0, 0.0, -1.0, 3.0, 1.0]
        first = km.block_bootstrap_rmse_diff(err_a, err_b)
        second = km.block_bootstrap_rmse_diff(err_a, err_b)
        self.assertEqual(first, second)

    def test_defaults_are_the_pinned_specification(self):
        # Значения по умолчанию — часть спецификации, зафиксированной ДО
        # результатов: блок 4 заседания, 2000 ресэмплов, сид SEED.
        err_a = [3.0, -2.0, 5.0, 0.0, -1.0, 4.0, 2.0, -3.0]
        err_b = [1.0, 1.0, -2.0, 2.0, 0.0, -1.0, 3.0, 1.0]
        self.assertEqual(
            km.block_bootstrap_rmse_diff(err_a, err_b),
            km.block_bootstrap_rmse_diff(err_a, err_b, block=4, n=2000, seed=km.SEED),
        )

    def test_identical_errors_include_zero_and_give_p_one(self):
        errors = [4.0, -4.0, 9.0, -9.0, 1.0, -1.0, 6.0, -6.0]
        result = km.block_bootstrap_rmse_diff(errors, list(errors))
        self.assertEqual(result.rmse_diff, 0.0)
        self.assertEqual((result.low, result.high), (0.0, 0.0))
        self.assertEqual(result.p_value, 1.0)

    def test_constant_errors_without_variance_do_not_crash(self):
        result = km.block_bootstrap_rmse_diff([5.0] * 20, [1.0] * 20)
        self.assertAlmostEqual(result.rmse_diff, 4.0, places=12)
        self.assertEqual((result.low, result.high), (4.0, 4.0))
        self.assertEqual(result.p_value, 0.0)

    def test_interval_brackets_the_point_estimate(self):
        err_a = [3.0, -2.0, 5.0, 0.0, -1.0, 4.0, 2.0, -3.0, 6.0, -5.0, 1.0, 0.5]
        err_b = [1.0, 1.0, -2.0, 2.0, 0.0, -1.0, 3.0, 1.0, 2.0, -2.0, 0.0, 1.5]
        result = km.block_bootstrap_rmse_diff(err_a, err_b)
        self.assertLessEqual(result.low, result.rmse_diff)
        self.assertLessEqual(result.rmse_diff, result.high)
        self.assertGreater(result.rmse_diff, 0.0)

    def test_invalid_inputs_are_refused(self):
        with self.assertRaises(ValueError):
            km.block_bootstrap_rmse_diff([], [])
        with self.assertRaises(ValueError):
            km.block_bootstrap_rmse_diff([1.0], [1.0, 2.0])
        with self.assertRaises(ValueError):
            km.block_bootstrap_rmse_diff([1.0, 2.0], [1.0, 2.0], block=0)
        with self.assertRaises(ValueError):
            km.block_bootstrap_rmse_diff([1.0, 2.0], [1.0, 2.0], n=0)


class TestReport(unittest.TestCase):
    """Отчёт и артефакты §8: разделы, строки «метод × окно», CSV."""

    @classmethod
    def setUpClass(cls):
        # Шаги чередуются: с постоянным шагом уровень ставки в окне не менялся
        # бы, и AR(1) справедливо отказывался бы оцениваться.
        cls.rows = synthetic_rows(60, first=date(2024, 1, 1), steps=(0, 50, -25))
        cls.predictions = benchmark_predictions(cls.rows)
        cls.skipped = km.Skipped((), (), ())

    def render(self, predictions=None, forecast=None):
        return km.render_report(
            self.rows,
            self.skipped,
            self.predictions if predictions is None else predictions,
            forecast,
            today=date(2026, 10, 16),
        )

    def test_report_has_every_section_of_the_step(self):
        text = self.render()
        for section in (
            "## Выборка",
            "## Walk-forward",
            "## Метрики: метод × окно",
            "## Тесты значимости",
            "## Mincer–Zarnowitz и Pesaran–Timmermann",
            "## Режимный блок 2024–2026",
            "## Что не проверено",
        ):
            self.assertIn(section, text)
        self.assertIn("2026-10-16", text)

    def test_sample_table_counts_and_enumerates_the_skipped_lists(self):
        skipped = km.Skipped((date(2026, 10, 23),), (), (date(2014, 2, 14),))
        text = km.render_report(
            self.rows, skipped, self.predictions, None, today=date(2026, 10, 16)
        )
        # Пропуски перечисляются ДАТАМИ, а не только счётчиком (спека §4).
        self.assertIn("| вне выборки (окно исхода не наблюдаемо) | 1 — 2026-10-23 |", text)
        self.assertIn("| пропущено (нет данных по признакам) | 1 — 2014-02-14 |", text)
        # Пустой список — без тире и дат.
        self.assertIn("| пропущено (нет ставки на d−1) | 0 |", text)

    def test_walk_forward_section_reports_fold_counts(self):
        text = self.render()
        expanding = sum(1 for p in self.predictions if p.window == "expanding")
        rolling = sum(1 for p in self.predictions if p.window == "rolling40")
        self.assertEqual(expanding, 3 * (60 - 24))
        self.assertEqual(rolling, 3 * (60 - 40))
        self.assertIn(f"расширяющееся окно (минимум {km.MIN_TRAIN} заседаний в обучении)", text)
        self.assertIn(f"скользящее окно {km.ROLLING_WINDOW} заседаний", text)

    def test_metric_table_has_a_row_per_method_and_window(self):
        text = self.render()
        for method_label in ("B1 hold", "B2 repeat", "B3 AR(1)"):
            for window in ("expanding", "rolling40"):
                self.assertIn(f"| {method_label} | {window} |", text)
        # Малая отрицательная разница печатается как «0.00», а не «-0.00»:
        # минус в разрядах таблицы читался бы как ошибка знака.
        self.assertNotIn("-0.00", text)

    def test_benchmark_rows_have_no_probabilistic_metrics(self):
        # Спека §5: вырожденные бенчмарки распределений не дают, Brier и
        # log loss к ним не применяются (в таблице — «—»), и надёжностной
        # таблицы в отчёте из одних бенчмарков нет.
        text = self.render()
        self.assertNotIn("## Надёжностная таблица", text)

    def test_significance_section_covers_every_pair_against_benchmarks(self):
        text = self.render()
        # DM каждой строки против каждого бенчмарка, кроме себя самого.
        for method_label in ("B1 hold", "B2 repeat", "B3 AR(1)"):
            for opponent in ("B1 hold", "B2 repeat", "B3 AR(1)"):
                if method_label != opponent:
                    self.assertIn(f"| {method_label} | expanding | {opponent} |", text)
        # Путь назван явно (требование плана: библиотека или формула).
        self.assertIn("statsmodels", text)

    def test_regime_block_counts_the_meetings_it_covers(self):
        text = self.render()
        regime_rows = [
            row
            for row in self.rows
            if km.REGIME_FROM <= row.decision.meeting_date <= km.REGIME_TO
        ]
        self.assertEqual(len(regime_rows), 37)
        self.assertIn(f"заседаний 2024–2026 в выборке: {len(regime_rows)}", text)

    def test_report_is_deterministic(self):
        self.assertEqual(self.render(), self.render())

    def test_report_without_folds_does_not_crash(self):
        text = km.render_report(self.rows, self.skipped, [], None, today=date(2026, 10, 16))
        self.assertIn("фолдов нет", text)

    def test_probabilistic_method_gets_brier_log_loss_and_reliability(self):
        # Расширяемость шага: строка метода с вероятностями получает
        # вероятностные метрики и надёжностную таблицу (Tasks 4–5 используют
        # этот же путь). Фолды — те же, что у бенчмарков (парность DM).
        predictions = list(self.predictions)
        predictions.extend(
            km.Prediction(
                row.decision.meeting_date,
                float(row.decision.step_bp),
                row.decision.y3,
                "ordered_logit",
                "expanding",
                0.0,
                (0.2, 0.7, 0.1),
            )
            for row in self.rows[24:]
        )
        text = self.render(predictions)
        self.assertIn("| ordered_logit | expanding |", text)
        self.assertIn("## Надёжностная таблица", text)
        self.assertIn("| ordered_logit | expanding | 0.60–0.80 |", text)
        # Все 36 прогнозов — в одном бине уверенности 0.70.
        expected_hits = sum(1 for row in self.rows[24:] if row.decision.y3 == 0)
        self.assertIn(f"| 36 | 0.700 | {expected_hits / 36:.3f} |", text)

    def test_forecast_section_appears_only_with_a_forecast(self):
        # `forecast` — готовый markdown-блок текущего прогноза (Task 6) или
        # None: без него раздела нет, и отчёт не обещает несделанного.
        self.assertNotIn("## Текущий прогноз", self.render())
        text = self.render(forecast="Заседание 2026-10-23: шаг −50 б.п. (заглушка).")
        self.assertIn("## Текущий прогноз", text)
        self.assertIn("Заседание 2026-10-23", text)

    def test_predictions_csv_is_written_row_by_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "predictions.csv"
            km.write_predictions(path, self.predictions)
            raw = path.read_bytes()
            self.assertNotIn(b"\r\n", raw)
            lines = raw.decode("utf-8").splitlines()
            self.assertEqual(lines[0], ",".join(km.PREDICTION_COLUMNS))
            self.assertEqual(len(lines) - 1, len(self.predictions))
            first = lines[1].split(",")
            self.assertEqual(first[0], self.predictions[0].meeting_date.isoformat())
            self.assertEqual(first[1], "hold")
            self.assertEqual(first[2], "expanding")


class TestRunSummaryArtifacts(unittest.TestCase):
    """Живой контур прогона: отчёт и CSV пишутся, окна считаются (§8)."""

    def run_into(self, directory, today, rolling=False):
        with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
            with contextlib.redirect_stdout(io.StringIO()) as out:
                code = km.run_summary(rolling=rolling, today=today, out_dir=Path(directory))
        return code, out.getvalue()

    def test_writes_both_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, text = self.run_into(tmp, date(2017, 1, 1))
            self.assertEqual(code, 0)
            report = Path(tmp) / km.REPORT_NAME
            predictions = Path(tmp) / km.PREDICTIONS_NAME
            self.assertTrue(report.is_file() and predictions.is_file())
            self.assertIn("# Модель ключевой ставки v0", report.read_text(encoding="utf-8"))
            # 26 решений среза: расширяющееся окно — 2 фолда на бенчмарк
            # (3 × 2 = 6 строк CSV), скользящее 40 — ноль.
            self.assertEqual(len(predictions.read_text(encoding="utf-8").splitlines()), 7)
            self.assertIn("фолдов (расширяющееся окно", text)
            self.assertIn("фолдов (скользящее окно", text)

    def test_console_metrics_follow_the_rolling_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, expanding = self.run_into(tmp, date(2017, 1, 1), rolling=False)
            _, rolling = self.run_into(tmp, date(2017, 1, 1), rolling=True)
        self.assertIn("метрики окна «expanding»", expanding)
        self.assertIn("метрики окна «rolling40»", rolling)


if __name__ == "__main__":
    unittest.main()
