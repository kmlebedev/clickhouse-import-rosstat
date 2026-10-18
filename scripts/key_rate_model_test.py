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
import dataclasses
import io
import json
import math
import os
import sys
import tempfile
import unittest
import unittest.mock
import warnings
from datetime import date, timedelta
from pathlib import Path

import numpy as np

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

# Переменные, которые читает журнал эксперимента. Набор ОФЛАЙН: `ML_LOG=1`
# из окружения разработчика включал бы запись из `run_summary(ml_log=None)`,
# а `ML_RUN_NAME`/`ML_LOCAL_DIR` меняли бы имя прогона и путь fallback.
# Модульные хуки убирают переменные на время набора и возвращают их после;
# тесты, которые сами выставляют `ML_LOG` (через `patch.dict`), работают как
# раньше.
_ML_ENV_VARS = ("ML_LOG", "ML_RUN_NAME", "ML_LOCAL_DIR", "MLFLOW_TRACKING_URI")
_SAVED_ML_ENV = {}


def setUpModule():
    for name in _ML_ENV_VARS:
        _SAVED_ML_ENV[name] = os.environ.get(name)
        os.environ.pop(name, None)


def tearDownModule():
    for name in _ML_ENV_VARS:
        previous = _SAVED_ML_ENV.pop(name, None)
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


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
    # Объём Task 2: парсер и загрузчик есть, консенсус в отчёт пойдёт Task 5.
    # Пустая витрина — штатный случай («данных нет»), а не ошибка.
    if "FROM v_cbr_mo_br" in sql:
        return []
    raise AssertionError(f"неизвестный SQL: {sql}")


def ch_query_without_meetings(sql: str) -> list:
    """Транспорт-заглушка с ПУСТЫМ календарём: ранний выход `run_summary`.

    Так выглядит сломанный импорт `events_calendar` (канонических заседаний
    нет): до записи артефактов дело не доходит, и файлы прошлого прогона
    обязаны исчезнуть, а не выжить «последним результатом».
    """
    if "FROM events_calendar" in sql:
        return []
    return ch_query_from_fixtures(sql)


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


class FakeLogger:
    """Duck-typed двойник `ml_experiment.ExperimentRun` для офлайн-тестов.

    Контекстным менеджером быть не обязан (рулинг Task 3): `run_summary` вызывает
    `finish` явно на каждом выходе. Методы повторяют контракт
    `log_metrics`/`log_predictions`/`log_artifacts`/`finish` без похода в
    ClickHouse.
    """

    def __init__(self):
        self.metrics = {}
        self.predictions = []
        self.artifacts = []
        self.finished = None
        self.notes = ""

    def log_metrics(self, metrics):
        self.metrics.update(metrics)

    def log_predictions(self, rows):
        self.predictions.extend(rows)

    def log_artifacts(self, paths):
        self.artifacts.extend(path for path in paths if path is not None)

    def finish(self, status="finished", notes=""):
        self.finished = status
        self.notes = notes
        return "fake"


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

    def test_load_consensus_parses_stats_and_lag(self):
        # Ответ нарочно не отсортирован и содержит чужой ряд витрины: парсер
        # обязан сгруппировать статистики по (волна, год), пропустить не-ставку
        # и вернуть волны по возрастанию (волна, год).
        rows = [
            {"date": "2026-09-01", "series": "key_rate_avg_2027_median", "value": 12.43},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.5},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_p10", "value": 14.3},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_p90", "value": 14.6},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_answers", "value": "32"},
            {"date": "2026-06-01", "series": "key_rate_avg_2027_median", "value": 13.0},
            {"date": "2026-09-01", "series": "cpi_dec_2026_median", "value": 5.0},
        ]
        with unittest.mock.patch.object(km, "ch_query", return_value=rows) as query:
            waves = km.load_consensus()
        self.assertIn("v_cbr_mo_br", query.call_args.args[0])
        self.assertEqual(
            waves,
            [
                km.ConsensusForecast(
                    wave_date=date(2026, 6, 1),
                    available_from=date(2026, 6, 7),
                    year=2027,
                    median=13.0,
                    p10=None,
                    p90=None,
                    answers=None,
                ),
                km.ConsensusForecast(
                    wave_date=date(2026, 9, 1),
                    available_from=date(2026, 9, 7),
                    year=2026,
                    median=14.5,
                    p10=14.3,
                    p90=14.6,
                    answers=32,
                ),
                km.ConsensusForecast(
                    wave_date=date(2026, 9, 1),
                    available_from=date(2026, 9, 7),
                    year=2027,
                    median=12.43,
                    p10=None,
                    p90=None,
                    answers=None,
                ),
            ],
        )

    def test_consensus_without_median_is_rejected(self):
        # Волна без медианы — не консенсус: p10/p90/answers сами по себе
        # не дают точечного прогноза, и тихо выбросить группу нельзя.
        rows = [
            {"date": "2026-09-01", "series": "key_rate_avg_2026_p10", "value": 14.3},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_answers", "value": 32},
        ]
        with self.assertRaises(ValueError) as caught:
            km.consensus_from_rows(rows)
        self.assertIn("нет медианы", str(caught.exception))
        self.assertIn("2026-09-01", str(caught.exception))

    def test_consensus_duplicate_stat_is_rejected(self):
        # Дубль (волна, год, статистика) — потерянный FINAL или неверный ключ
        # витрины: «последний победил» сделал бы прогноз зависимым от порядка.
        rows = [
            {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.5},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.6},
        ]
        with self.assertRaises(ValueError) as caught:
            km.consensus_from_rows(rows)
        self.assertIn("дубль median", str(caught.exception))
        self.assertIn("2026-09-01", str(caught.exception))

    def test_consensus_non_integer_answers_is_rejected(self):
        # Число ответов — счётчик участников; 32.5 — дефект источника, а не
        # повод молча округлить.
        rows = [
            {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.5},
            {"date": "2026-09-01", "series": "key_rate_avg_2026_answers", "value": 32.5},
        ]
        with self.assertRaises(ValueError) as caught:
            km.consensus_from_rows(rows)
        self.assertIn("не целое", str(caught.exception))

    def test_consensus_non_finite_stats_are_rejected(self):
        # NaN/inf прошли бы float() и попали в прогноз как «измеренные»:
        # NaN заразил бы медиану и метрики, inf — деление на остаток года.
        cases = (
            ("median", float("nan")),
            ("median", float("inf")),
            ("p10", float("nan")),
            ("p90", float("-inf")),
        )
        for stat, value in cases:
            rows = [
                {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.5},
            ]
            if stat == "median":
                rows[0]["value"] = value
            else:
                rows.append(
                    {
                        "date": "2026-09-01",
                        "series": f"key_rate_avg_2026_{stat}",
                        "value": value,
                    }
                )
            with self.subTest(stat=stat, value=value):
                with self.assertRaises(ValueError) as caught:
                    km.consensus_from_rows(rows)
                message = str(caught.exception)
                self.assertIn(stat, message)
                self.assertIn("не конечное", message)

    def test_consensus_non_finite_answers_is_rejected(self):
        # int(inf) дал бы OverflowError, int(nan) — ValueError с неверным
        # текстом «не целое»: бесконечность и NaN отсекаются до int().
        for value in (float("inf"), float("nan")):
            rows = [
                {"date": "2026-09-01", "series": "key_rate_avg_2026_median", "value": 14.5},
                {"date": "2026-09-01", "series": "key_rate_avg_2026_answers", "value": value},
            ]
            with self.subTest(value=value):
                with self.assertRaises(ValueError) as caught:
                    km.consensus_from_rows(rows)
                message = str(caught.exception)
                self.assertIn("answers", message)
                self.assertIn("не конечное", message)
                self.assertNotIn("не целое", message)

    def test_consensus_sql_reads_the_view(self):
        # Источник — витрина, а не сырая macro_series: грант у kimi_reader
        # есть только на v_cbr_mo_br, и парсер обязан читать тот же срез.
        self.assertIn("v_cbr_mo_br", km.CONSENSUS_SQL)
        self.assertIn("key_rate_avg_%", km.CONSENSUS_SQL)
        self.assertIn("FORMAT JSON", km.CONSENSUS_SQL)

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


class TestConsensusBenchmark(unittest.TestCase):
    """Годовой бенчмарк: средняя, наивный hold, выборка и DM (спека §3.1)."""

    @staticmethod
    def wave(wave_date, year, median=10.0, p10=None, p90=None, answers=None):
        """Волна с консервативной доступностью `wave_date + 6` (как парсер)."""
        return km.ConsensusForecast(
            wave_date=wave_date,
            available_from=wave_date
            + timedelta(days=km.CONSENSUS_PUBLICATION_LAG_DAYS),
            year=year,
            median=median,
            p10=p10,
            p90=p90,
            answers=answers,
        )

    @staticmethod
    def observation(bucket, consensus_error_bp, hold_error_bp):
        """Синтетическая пара для метрик: поля волны aggregate не читает."""
        return km.ConsensusObservation(
            year=2024,
            wave_date=date(2024, 1, 1),
            available_from=date(2024, 1, 7),
            bucket=bucket,
            median=10.0,
            p10=None,
            p90=None,
            answers=None,
            actual=10.0,
            consensus_error_bp=consensus_error_bp,
            hold_error_bp=hold_error_bp,
        )

    def test_bucket_order_matches_report(self):
        self.assertEqual(
            km.CONSENSUS_BUCKETS, ("ex-ante", "10-12", "7-9", "4-6", "1-3")
        )

    def test_rate_asof_fills_forward_and_switches_on_change_day(self):
        points = [(date(2024, 1, 1), 10.0), (date(2024, 3, 1), 8.0)]
        self.assertIsNone(km._rate_asof(points, date(2023, 12, 31)))
        self.assertEqual(km._rate_asof(points, date(2024, 1, 1)), 10.0)
        # Високосный день не создаёт новой точки: действует ставка 1 января.
        self.assertEqual(km._rate_asof(points, date(2024, 2, 29)), 10.0)
        self.assertEqual(km._rate_asof(points, date(2024, 3, 1)), 8.0)
        self.assertEqual(km._rate_asof(points, date(2024, 12, 31)), 8.0)
        self.assertIsNone(km._rate_asof([], date(2024, 1, 1)))

    def test_sum_rates_between_is_inclusive_and_fills_forward(self):
        points = [(date(2024, 1, 1), 10.0), (date(2024, 1, 3), 20.0)]
        # 1 и 2 января — ставка 10 (fill-forward), 3 января — новая точка 20.
        self.assertEqual(
            km.sum_rates_between(points, date(2024, 1, 1), date(2024, 1, 3)), 40.0
        )
        self.assertEqual(
            km.sum_rates_between(points, date(2024, 1, 2), date(2024, 1, 3)), 30.0
        )
        self.assertEqual(
            km.sum_rates_between(points, date(2024, 1, 3), date(2024, 1, 3)), 20.0
        )
        self.assertEqual(
            km.sum_rates_between(points, date(2024, 12, 31), date(2024, 12, 31)),
            20.0,
        )

    def test_sum_rates_between_without_rate_on_start_is_rejected(self):
        # Молчаливый ноль занизил бы R_known/среднюю; отсутствие точки на
        # старте — разрыв ряда, а не «нулевая ставка».
        with self.assertRaises(ValueError) as caught:
            km.sum_rates_between(
                [(date(2024, 1, 2), 10.0)], date(2024, 1, 1), date(2024, 1, 2)
            )
        self.assertIn("нет точки", str(caught.exception))
        with self.assertRaises(ValueError):
            km.sum_rates_between([], date(2024, 1, 1), date(2024, 1, 2))
        with self.assertRaises(ValueError):
            km.sum_rates_between(
                [(date(2024, 1, 1), 10.0)], date(2024, 1, 2), date(2024, 1, 1)
            )

    def test_annual_average_counts_the_leap_day(self):
        # 2024: 1 января–30 июня = 182 дня, 1 июля–31 декабря = 184 дня; всего
        # 366. Средняя = (182·10 + 184·20) / 366, а не / 365.
        points = [
            (date(2024, 1, 1), 10.0),
            (date(2024, 7, 1), 20.0),
            (date(2024, 12, 31), 20.0),
        ]
        expected = (182 * 10.0 + 184 * 20.0) / 366
        self.assertAlmostEqual(km.annual_average_rate(points, 2024), expected, places=12)

    def test_annual_average_in_a_non_leap_year_uses_365_days(self):
        # 2023: 1 января–30 июня = 181 день, 1 июля–31 декабря = 184; всего 365.
        points = [
            (date(2023, 1, 1), 10.0),
            (date(2023, 7, 1), 20.0),
            (date(2023, 12, 31), 20.0),
        ]
        expected = (181 * 10.0 + 184 * 20.0) / 365
        self.assertAlmostEqual(km.annual_average_rate(points, 2023), expected, places=12)

    def test_annual_average_incomplete_year_is_none(self):
        full_2024 = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        self.assertIsNotNone(km.annual_average_rate(full_2024, 2024))
        # Ряд кончается 30 декабря — 31-е не покрыто.
        self.assertIsNone(
            km.annual_average_rate(
                [(date(2024, 1, 1), 10.0), (date(2024, 12, 30), 10.0)], 2024
            )
        )
        # Ряд начинается 2 января — 1-е не покрыто.
        self.assertIsNone(
            km.annual_average_rate(
                [(date(2024, 1, 2), 10.0), (date(2024, 12, 31), 10.0)], 2024
            )
        )
        self.assertIsNone(km.annual_average_rate([], 2024))
        # Неполный текущий 2026: ряд не дотягивает до 31 декабря.
        self.assertIsNone(
            km.annual_average_rate(
                [(date(2026, 1, 1), 10.0), (date(2026, 10, 19), 10.0)], 2026
            )
        )

    def test_hold_before_the_year_is_the_rate_on_availability_date(self):
        points = [
            (date(2023, 6, 1), 7.5),
            (date(2024, 1, 1), 10.0),
            (date(2024, 12, 31), 10.0),
        ]
        self.assertEqual(km.hold_forecast(points, 2024, date(2023, 12, 7)), 7.5)

    def test_hold_within_the_year_uses_known_path_and_current_rate(self):
        # 2024: 1 января ставка 10, с 1 февраля — 20, A = 1 марта (ставка 20).
        # known = 31·10 + 30·20 = 910 (29 февраля + 1 марта); N_rem = 305.
        points = [
            (date(2024, 1, 1), 10.0),
            (date(2024, 2, 1), 20.0),
            (date(2024, 12, 31), 20.0),
        ]
        expected = (31 * 10.0 + 30 * 20.0 + 20.0 * 305) / 366
        self.assertAlmostEqual(
            km.hold_forecast(points, 2024, date(2024, 3, 1)), expected, places=12
        )
        # На 1 января известен один день, остаток года — та же ставка: hold = 10.
        self.assertAlmostEqual(
            km.hold_forecast(points, 2024, date(2024, 1, 1)), 10.0, places=12
        )
        # На 31 декабря известен весь год: hold совпадает с фактом.
        self.assertAlmostEqual(
            km.hold_forecast(points, 2024, date(2024, 12, 31)),
            km.annual_average_rate(points, 2024),
            places=12,
        )

    def test_hold_without_rate_on_availability_date_is_none(self):
        self.assertIsNone(km.hold_forecast([], 2024, date(2024, 1, 7)))
        self.assertIsNone(
            km.hold_forecast([(date(2024, 2, 1), 10.0)], 2024, date(2024, 1, 7))
        )

    def test_buckets_and_month_boundaries(self):
        # Ряд покрывает 2024 целиком; ставка постоянна — факт 10.
        key_rate = [(date(2023, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        wave_dates = [
            date(2023, 10, 1),  # более ранняя экс-анте — не последняя
            date(2023, 12, 1),  # экс-анте: публикация до 1 января 2024
            date(2024, 1, 1),
            date(2024, 3, 1),  # граница 10-12/7-9 — март ещё 10-12
            date(2024, 4, 1),  # апрель уже 7-9
            date(2024, 6, 1),  # июнь ещё 7-9
            date(2024, 7, 1),  # июль уже 4-6
            date(2024, 9, 1),  # сентябрь ещё 4-6
            date(2024, 10, 1),  # октябрь уже 1-3
            date(2024, 12, 1),
        ]
        consensus = [
            self.wave(wave_date, 2024, median=10.0, p10=9.5, p90=10.5, answers=20)
            for wave_date in wave_dates
        ]
        observations = km.consensus_observations(consensus, key_rate)
        self.assertEqual(
            [observation.wave_date for observation in observations], wave_dates[1:]
        )
        self.assertEqual(
            [observation.bucket for observation in observations],
            ["ex-ante", "10-12", "10-12", "7-9", "7-9", "4-6", "4-6", "1-3", "1-3"],
        )
        for observation in observations:
            self.assertEqual(
                observation.available_from,
                observation.wave_date
                + timedelta(days=km.CONSENSUS_PUBLICATION_LAG_DAYS),
            )
            self.assertEqual(observation.actual, 10.0)
            self.assertEqual(observation.median, 10.0)
            self.assertEqual(observation.p10, 9.5)
            self.assertEqual(observation.p90, 10.5)
            self.assertEqual(observation.answers, 20)
            self.assertEqual(observation.consensus_error_bp, 0.0)
            self.assertEqual(observation.hold_error_bp, 0.0)

    def test_observation_errors_are_signed_and_sorted_by_year(self):
        key_rate = [(date(2023, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [
            self.wave(date(2023, 6, 1), 2023, median=11.0),
            self.wave(date(2023, 12, 1), 2024, median=9.0),
        ]
        observations = km.consensus_observations(consensus, key_rate)
        self.assertEqual(
            [
                (observation.year, observation.available_from)
                for observation in observations
            ],
            [(2023, date(2023, 6, 7)), (2024, date(2023, 12, 7))],
        )
        # Положительная ошибка — консенсус завысил (11 против факта 10),
        # отрицательная — занизил (9 против 10). Hold ошибки не даёт: 10 = 10.
        self.assertEqual(observations[0].consensus_error_bp, 100.0)
        self.assertEqual(observations[1].consensus_error_bp, -100.0)
        self.assertEqual(observations[0].hold_error_bp, 0.0)
        self.assertEqual(observations[1].hold_error_bp, 0.0)

    def test_incomplete_year_is_skipped(self):
        # 2025 в consensus есть, но ряд ставки кончается 31 декабря 2024 —
        # год не завершён, и пара не должна выдавать недосчитанную среднюю.
        key_rate = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [
            self.wave(date(2024, 6, 1), 2024),
            self.wave(date(2024, 9, 1), 2025),
        ]
        observations = km.consensus_observations(consensus, key_rate)
        self.assertEqual([observation.year for observation in observations], [2024])

    def test_completed_year_with_wave_before_rate_start_is_rejected(self):
        # Ряд покрывает 2024, но начинается позже даты доступности экс-анте
        # волны: hold не из чего считать. Это разрыв ряда, а не повод молча
        # выбросить пару из выборки.
        key_rate = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [self.wave(date(2023, 12, 1), 2024)]
        with self.assertRaises(ValueError) as caught:
            km.consensus_observations(consensus, key_rate)
        self.assertIn("нет ставки", str(caught.exception))

    def test_aggregate_consensus_metrics_and_dm(self):
        consensus_errors = [10.0, -10.0, 20.0, -20.0, 5.0, -5.0, 15.0, -15.0, 25.0, -25.0]
        hold_errors = [4.0 * error for error in consensus_errors]
        observations = [
            self.observation("4-6", consensus_error, hold_error)
            for consensus_error, hold_error in zip(consensus_errors, hold_errors)
        ]
        aggregate = km.aggregate_consensus(observations, "4-6")
        self.assertIsNotNone(aggregate)
        self.assertEqual(aggregate.bucket, "4-6")
        self.assertEqual(aggregate.n, 10)
        self.assertAlmostEqual(aggregate.consensus_rmse_bp, math.sqrt(275.0), places=12)
        self.assertAlmostEqual(aggregate.consensus_mae_bp, 15.0, places=12)
        self.assertAlmostEqual(aggregate.consensus_bias_bp, 0.0, places=12)
        self.assertAlmostEqual(aggregate.hold_rmse_bp, math.sqrt(4400.0), places=12)
        self.assertAlmostEqual(aggregate.hold_mae_bp, 60.0, places=12)
        self.assertAlmostEqual(aggregate.hold_bias_bp, 0.0, places=12)
        # Ошибки hold вчетверо больше: положительная статистика значила бы
        # «консенсус хуже», поэтому она отрицательна и значима.
        self.assertLess(aggregate.dm_stat, 0.0)
        self.assertLess(aggregate.dm_p, 0.05)

    def test_aggregate_consensus_empty_bucket_is_none(self):
        observations = [self.observation("4-6", 10.0, 20.0)]
        self.assertIsNone(km.aggregate_consensus(observations, "1-3"))
        self.assertIsNone(km.aggregate_consensus([], "4-6"))

    def test_aggregate_consensus_single_observation_has_no_dm(self):
        aggregate = km.aggregate_consensus(
            [self.observation("1-3", -30.0, 20.0)], "1-3"
        )
        self.assertEqual(aggregate.n, 1)
        self.assertEqual(aggregate.consensus_rmse_bp, 30.0)
        self.assertEqual(aggregate.consensus_mae_bp, 30.0)
        self.assertEqual(aggregate.consensus_bias_bp, -30.0)
        self.assertEqual(aggregate.hold_rmse_bp, 20.0)
        self.assertEqual(aggregate.hold_bias_bp, 20.0)
        self.assertIsNone(aggregate.dm_stat)
        self.assertIsNone(aggregate.dm_p)

    def test_aggregate_consensus_zero_variance_has_no_dm(self):
        # Одинаковые ошибки: различию потерь нечем делиться — DM не определён.
        same = [
            self.observation("7-9", 10.0, 10.0),
            self.observation("7-9", -20.0, -20.0),
        ]
        aggregate = km.aggregate_consensus(same, "7-9")
        self.assertIsNone(aggregate.dm_stat)
        self.assertIsNone(aggregate.dm_p)
        # Постоянная ненулевая разница потерь (hold-ошибка 12 против
        # консенсусной 10) — тоже `None`: p=0.0 при ±inf заявлял бы
        # доказанное превосходство, которого малая выборка не подтверждает.
        shifted = [
            self.observation("7-9", 10.0, 12.0),
            self.observation("7-9", 10.0, 12.0),
        ]
        aggregate = km.aggregate_consensus(shifted, "7-9")
        self.assertIsNone(aggregate.dm_stat)
        self.assertIsNone(aggregate.dm_p)


class TestConsensusPath(unittest.TestCase):
    """B4: проекция годовой медианы на шаг заседания (спека §3.2)."""

    @staticmethod
    def wave(wave_date, year, median=10.0):
        """Волна с консервативной доступностью `wave_date + 6` (как парсер)."""
        return km.ConsensusForecast(
            wave_date=wave_date,
            available_from=wave_date
            + timedelta(days=km.CONSENSUS_PUBLICATION_LAG_DAYS),
            year=year,
            median=median,
            p10=None,
            p90=None,
            answers=None,
        )

    @staticmethod
    def prediction(meeting_date, method, window, step_bp, actual_step_bp=0.0):
        """Фолд с заданным шагом; факт по умолчанию нулевой (ошибка = шаг)."""
        return km.Prediction(
            meeting_date=meeting_date,
            actual_step_bp=actual_step_bp,
            actual_y3=(actual_step_bp > 0) - (actual_step_bp < 0),
            method=method,
            window=window,
            step_bp=step_bp,
            probs=None,
        )

    def test_projects_annual_median_onto_meeting_step(self):
        # 2023 (365 дней): ставка 10 % до 31 марта, 12 % с 1 апреля; последняя
        # волна на 2023 доступна 1 июля (A). R_known = 90·10 + 92·12 = 2004,
        # N_rem = 183, r_implied = (9·365 − 2004)/183 = 7 %.
        key_rate = [
            (date(2023, 1, 1), 10.0),
            (date(2023, 4, 1), 12.0),
            (date(2023, 12, 31), 12.0),
        ]
        consensus = [
            self.wave(date(2023, 5, 1), 2023, median=99.0),  # раньше — не последняя
            self.wave(date(2023, 6, 25), 2023, median=9.0),  # A = 1 июля
            self.wave(date(2023, 6, 1), 2022, median=99.0),  # другой год
        ]
        path = km.consensus_path(consensus, key_rate, date(2023, 7, 21), 10.0)
        self.assertIsNotNone(path)
        self.assertEqual(path.meeting_date, date(2023, 7, 21))
        self.assertEqual(path.wave_date, date(2023, 6, 25))
        self.assertEqual(path.available_from, date(2023, 7, 1))
        self.assertEqual(path.year, 2023)
        self.assertEqual(path.annual_median, 9.0)
        self.assertEqual(path.rate_before, 10.0)
        self.assertAlmostEqual(path.implied_level, 7.0, places=12)
        self.assertAlmostEqual(path.step_bp, -300.0, places=12)
        self.assertEqual(path.days_remaining, 183)

    def test_january_meeting_projects_the_median_itself(self):
        # A = 7 декабря 2023 < 1 января 2024: известного пути в году нет,
        # R_known = 0, N_rem = 366, проекция равна медиане.
        key_rate = [(date(2023, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [self.wave(date(2023, 12, 1), 2024, median=12.5)]
        path = km.consensus_path(consensus, key_rate, date(2024, 1, 26), 10.0)
        self.assertIsNotNone(path)
        self.assertEqual(path.available_from, date(2023, 12, 7))
        self.assertEqual(path.year, 2024)
        self.assertEqual(path.annual_median, 12.5)
        self.assertAlmostEqual(path.implied_level, 12.5, places=12)
        self.assertAlmostEqual(path.step_bp, 250.0, places=12)
        self.assertEqual(path.days_remaining, 366)

    def test_short_remaining_year_is_skipped(self):
        # Декабрьская волна (A = 7 декабря) оставляет 24 дня — меньше guard 30.
        key_rate = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [self.wave(date(2024, 12, 1), 2024, median=10.0)]
        self.assertIsNone(
            km.consensus_path(consensus, key_rate, date(2024, 12, 20), 10.0)
        )
        # Граница проходит включительно: A = 1 декабря оставляет 30 дней.
        boundary = [self.wave(date(2024, 11, 25), 2024, median=10.0)]
        path = km.consensus_path(boundary, key_rate, date(2024, 12, 20), 10.0)
        self.assertIsNotNone(path)
        self.assertEqual(path.days_remaining, 30)

    def test_missing_wave_is_none(self):
        key_rate = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        self.assertIsNone(km.consensus_path([], key_rate, date(2024, 7, 26), 10.0))
        # Волна на другой год не подходит: медиана должна быть на год заседания.
        other_year = [self.wave(date(2024, 6, 1), 2023, median=10.0)]
        self.assertIsNone(
            km.consensus_path(other_year, key_rate, date(2024, 7, 26), 10.0)
        )
        # Доступность ровно в день заседания — уже не строго до события.
        same_day = [self.wave(date(2024, 7, 20), 2024, median=10.0)]
        self.assertIsNone(
            km.consensus_path(same_day, key_rate, date(2024, 7, 26), 10.0)
        )

    def test_missing_rate_coverage_is_none(self):
        consensus = [self.wave(date(2024, 6, 1), 2024, median=10.0)]  # A = 7 июня
        self.assertIsNone(km.consensus_path(consensus, [], date(2024, 7, 26), 10.0))
        # Ряд начинается после A — ставки на дату волны нет.
        late = [(date(2024, 6, 8), 10.0), (date(2024, 12, 31), 10.0)]
        self.assertIsNone(km.consensus_path(consensus, late, date(2024, 7, 26), 10.0))
        # Ряд покрывает A, но не 1 января: R_known не из чего собрать.
        midyear = [(date(2024, 2, 1), 10.0), (date(2024, 12, 31), 10.0)]
        self.assertIsNone(
            km.consensus_path(consensus, midyear, date(2024, 7, 26), 10.0)
        )
        # Январское заседание тоже требует покрытия A — guard общий.
        january = [self.wave(date(2023, 12, 1), 2024, median=12.5)]
        self.assertIsNone(
            km.consensus_path(
                january, [(date(2023, 12, 8), 10.0)], date(2024, 1, 26), 10.0
            )
        )

    def test_out_of_range_projection_is_none(self):
        # Guard `-100 < r_implied < 100`: медиана 100.5 % — уже не ставка.
        key_rate = [(date(2023, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [self.wave(date(2023, 12, 1), 2024, median=100.5)]
        self.assertIsNone(
            km.consensus_path(consensus, key_rate, date(2024, 1, 26), 10.0)
        )

    def test_rate_before_comes_from_the_row(self):
        # Ряд ставки на M−1 даёт 10 %, но B4 обязан взять rate_before из Row
        # (12.5 %): иначе шаг был бы 0 вместо −250 б.п.
        key_rate = [(date(2024, 1, 1), 10.0), (date(2024, 12, 31), 10.0)]
        consensus = [self.wave(date(2024, 6, 1), 2024, median=10.0)]
        row = km.Row(
            decision=km.Decision(
                meeting_date=date(2024, 7, 26),
                rate_before=12.5,
                rate_after=12.5,
                step_bp=0,
                y3=0,
            ),
            pi_saar=4.0,
            gap=0.0,
        )
        # Заседание 2025 не имеет волны на свой год — фолда не будет.
        later = km.Row(
            decision=km.Decision(
                meeting_date=date(2025, 2, 14),
                rate_before=10.0,
                rate_after=10.0,
                step_bp=0,
                y3=0,
            ),
            pi_saar=4.0,
            gap=0.0,
        )
        predictions = km.consensus_path_predictions([row, later], consensus, key_rate)
        self.assertEqual(len(predictions), 1)
        prediction = predictions[0]
        self.assertEqual(prediction.meeting_date, date(2024, 7, 26))
        self.assertEqual(prediction.actual_step_bp, 0.0)
        self.assertEqual(prediction.actual_y3, 0)
        self.assertEqual(prediction.method, "consensus")
        self.assertEqual(prediction.window, km.CONSENSUS_WINDOW)
        self.assertIsNone(prediction.probs)
        self.assertAlmostEqual(prediction.step_bp, -250.0, places=12)

    def test_b4_metrics_on_synthetic_predictions(self):
        predictions = [
            self.prediction(
                date(2024, 1, 1), "consensus", km.CONSENSUS_WINDOW, 10.0, 10.0
            ),
            self.prediction(
                date(2024, 2, 1), "consensus", km.CONSENSUS_WINDOW, -10.0, -10.0
            ),
            self.prediction(
                date(2024, 3, 1), "consensus", km.CONSENSUS_WINDOW, 20.0, -20.0
            ),
            self.prediction(
                date(2024, 4, 1), "consensus", km.CONSENSUS_WINDOW, -20.0, 20.0
            ),
        ]
        metrics = km.b4_metrics(predictions)
        self.assertIsNotNone(metrics)
        self.assertEqual(metrics.n, 4)
        # Ошибки [0, 0, 40, −40]: RMSE = √800, MAE = 20.
        self.assertAlmostEqual(metrics.rmse_bp, math.sqrt(800.0), places=12)
        self.assertAlmostEqual(metrics.mae_bp, 20.0, places=12)
        # Совпали два направления из четырёх; QWK — 0 (маргиналы совпадают,
        # но прогноз и факт согласованы лишь наполовину).
        self.assertAlmostEqual(metrics.hit_rate, 0.5, places=12)
        self.assertAlmostEqual(metrics.qwk, 0.0, places=12)
        self.assertIsNone(km.b4_metrics([]))

    def test_b4_significance_uses_shared_dates_and_method_order(self):
        d1, d2 = date(2024, 1, 1), date(2024, 2, 1)
        d3, d4 = date(2024, 3, 1), date(2024, 4, 1)
        b4 = [
            self.prediction(d1, "consensus", km.CONSENSUS_WINDOW, 10.0),
            self.prediction(d2, "consensus", km.CONSENSUS_WINDOW, -10.0),
            self.prediction(d3, "consensus", km.CONSENSUS_WINDOW, 20.0),
            self.prediction(d4, "consensus", km.CONSENSUS_WINDOW, -20.0),
        ]
        expanding = km.WINDOW_EXPANDING
        rolling = km.window_label(km.ROLLING_WINDOW)
        predictions = [
            # hold: ошибки вчетверо меньше B4 на всех четырёх датах.
            self.prediction(d1, "hold", expanding, 4.0),
            self.prediction(d2, "hold", expanding, -4.0),
            self.prediction(d3, "hold", expanding, 8.0),
            self.prediction(d4, "hold", expanding, -8.0),
            # ar1: только две общие даты — DM обязан взять пересечение.
            self.prediction(d2, "ar1", expanding, 5.0),
            self.prediction(d3, "ar1", expanding, -8.0),
            # repeat: другое окно.
            self.prediction(d1, "repeat", rolling, 1.0),
            self.prediction(d2, "repeat", rolling, 1.0),
            self.prediction(d3, "repeat", rolling, 1.0),
            self.prediction(d4, "repeat", rolling, 1.0),
            # taylor: общих дат нет — строка не выводится.
            self.prediction(date(2024, 5, 1), "taylor", expanding, 1.0),
            # Группа самого B4 в перебор не входит (окна нет в METHODS).
            self.prediction(d1, "consensus", km.CONSENSUS_WINDOW, 1.0),
        ]
        comparisons = km.b4_significance(b4, predictions)
        self.assertEqual(
            [(comparison.against, comparison.window) for comparison in comparisons],
            [("hold", expanding), ("ar1", expanding), ("repeat", rolling)],
        )
        hold, ar1, repeat = comparisons
        self.assertEqual(hold.n, 4)
        self.assertAlmostEqual(
            hold.delta_rmse_bp, math.sqrt(250.0) - math.sqrt(40.0), places=12
        )
        self.assertIsNotNone(hold.dm_stat)
        # err_a = B4: положительная статистика значит «B4 хуже».
        self.assertGreater(hold.dm_stat, 0.0)
        self.assertGreaterEqual(hold.dm_p, 0.0)
        self.assertLessEqual(hold.dm_p, 1.0)
        # ar1 сравнивается только на d2/d3: RMSE(B4) = √250, RMSE(ar1) = √44.5.
        self.assertEqual(ar1.n, 2)
        self.assertAlmostEqual(
            ar1.delta_rmse_bp, math.sqrt(250.0) - math.sqrt(44.5), places=12
        )
        self.assertIsNotNone(ar1.dm_stat)
        self.assertGreater(ar1.dm_stat, 0.0)
        self.assertEqual(repeat.n, 4)
        self.assertAlmostEqual(repeat.delta_rmse_bp, math.sqrt(250.0) - 1.0, places=12)
        self.assertIsNotNone(repeat.dm_stat)

    def test_b4_significance_zero_variance_has_no_dm(self):
        d1, d2 = date(2024, 1, 1), date(2024, 2, 1)
        b4 = [
            self.prediction(d1, "consensus", km.CONSENSUS_WINDOW, 10.0),
            self.prediction(d2, "consensus", km.CONSENSUS_WINDOW, -10.0),
        ]
        # Разница квадратов потерь постоянна (100 − 25 = 75): DM не определён.
        predictions = [
            self.prediction(d1, "hold", km.WINDOW_EXPANDING, 5.0),
            self.prediction(d2, "hold", km.WINDOW_EXPANDING, 5.0),
        ]
        comparisons = km.b4_significance(b4, predictions)
        self.assertEqual(len(comparisons), 1)
        self.assertEqual(comparisons[0].n, 2)
        self.assertAlmostEqual(comparisons[0].delta_rmse_bp, 5.0, places=12)
        self.assertIsNone(comparisons[0].dm_stat)
        self.assertIsNone(comparisons[0].dm_p)
        # Нет общих дат — нет и строки сравнения.
        elsewhere = [
            self.prediction(date(2024, 5, 1), "hold", km.WINDOW_EXPANDING, 1.0)
        ]
        self.assertEqual(km.b4_significance(b4, elsewhere), [])

    def test_b4_is_not_a_forecast_method(self):
        # B4 — механическая проекция, а не метод прогноза (спека §3.2): он не
        # входит в METHODS/FORECAST_METHODS и живёт в своём окне.
        self.assertEqual(km.CONSENSUS_WINDOW, "consensus")
        self.assertNotIn("consensus", [method for method, _ in km.METHODS])
        self.assertNotIn("consensus", [method for method, _ in km.FORECAST_METHODS])
        self.assertNotIn("consensus", km.BENCHMARK_METHODS)
        self.assertNotEqual(km.CONSENSUS_WINDOW, km.WINDOW_EXPANDING)
        self.assertNotEqual(
            km.CONSENSUS_WINDOW, km.window_label(km.ROLLING_WINDOW)
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
    `(pred_level − rate_before) × 100` (б.п., как и остальные прогнозы и факты
    harness'а) считается руками.
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


def taylor_rows(
    count, rho=0.9, c=1.0, a_pi=0.2, b_gap=-0.5, constant=None, first=date(2014, 1, 1)
):
    """Строки с ТОЧНОЙ связью `i_t = c + ρ·i_{t−1} + a·π + b·gap` (метод №2).

    Безрелейная синтетика: OLS на такой выборке обязан восстановить
    коэффициенты до машинной погрешности, а прогноз шага в б.п.
    (`(pred_level − rate_before) × 100`) считается руками.

    Признаки меняются и НЕ коллинеарны друг другу и уровню: `π` чередуется
    5/8, `gap` растёт на 0.1 за заседание. Это важно: у монотонных признаков
    уровень сходится к их линейной комбинации и колонка `rate_before` становится
    коллинеарной свободному члену — окно вырождается по построению, а не по
    делу. Долгосрочные импликации этой синтетики: `r* = 10`, `α = 2`, `β = −5`.

    `constant` — имя регрессора (`"pi"` или `"gap"`), который делается
    ПОСТОЯННЫМ во всей выборке: окно с таким дизайном вырождено ровно из-за
    одного признака (а не потому, что постоянны все столбцы сразу).
    """
    rows = []
    level = 10.0
    for index in range(count):
        pi = 5.0 if constant == "pi" else 5.0 + 3.0 * (index % 2)
        gap = -2.0 if constant == "gap" else -2.0 + 0.1 * index
        after = c + rho * level + a_pi * pi + b_gap * gap
        step = round((after - level) * 100.0)
        decision = km.Decision(
            first + timedelta(days=30 * index),
            level,
            after,
            step,
            (step > 0) - (step < 0),
        )
        rows.append(km.Row(decision, pi, gap))
        level = after
    return rows


def ordinal_rows(count, first=date(2014, 1, 1), spread=4.0, noise=2.4, band=2.0):
    """Синтетика метода №3: «gap выше порога ⇒ повышение» с перекрытием классов.

    Класс — дискретизация `latent = gap + шум` (шум детерминированный: ±2.4 по
    сетке из пяти уровней): выше `band` — повышение (+100 б.п.), ниже `−band` —
    снижение (−50 б.п.), между — сохранение (0). Перекрытие классов здесь не для
    красоты: при ТОЧНОМ пороге классы линейно разделимы, и MLE ordered-модели
    уходит в бесконечность (вырожденный фолд), а не оценивается. Шум и порог
    подобраны так, что КАЖДОЕ окно прогона (24 заседания и больше) содержит не
    меньше двух классов: у окна из одного класса метода тоже нет.

    `gap` растёт по выборке (сетка от `−spread` до `+spread`), `pi_saar`
    чередуется, `rate_before` накапливает шаги — регрессоры метода №3 не
    коллинеарны друг другу и не постоянны (дизайн полного ранга). Шаги классов
    РАЗЛИЧНЫ и в б.п.: ловят перевод точки в п.п. (`E[step]` был бы в 100 раз
    меньше). Зависимость вероятностей от gap — сигмоида линейного индекса:
    `P(повышение) = 1 − F(τ₂ − xβ)`, поэтому при β_gap > 0 вероятность
    монотонно растёт по gap (это пин `TestOrderedLogit`).
    """
    rows = []
    level = 10.0
    for index in range(count):
        gap = -spread + 2.0 * spread * index / (count - 1)
        pi = 4.0 + 2.0 * ((index * 7) % 3)
        latent = gap + noise * (((index * 37) % 5) - 2) / 2.0
        if latent > band:
            step = 100
        elif latent < -band:
            step = -50
        else:
            step = 0
        after = level + step / 100.0
        decision = km.Decision(
            first + timedelta(days=30 * index), level, after, step, (step > 0) - (step < 0)
        )
        rows.append(km.Row(decision, pi, gap))
        level = after
    return rows


def ordinal_test_row(gap, rate=10.0, pi=4.0, day=date(2014, 1, 1)):
    """Тестовая строка метода №3 с заданным `gap` (остальные признаки фиксированы).

    Так проверяется монотонность P(повышение) по gap «при прочих равных»: у
    ordered-модели вероятность зависит от линейного индекса `xβ`, и меняется
    здесь ровно один регрессор.
    """
    return km.Row(km.Decision(day, rate, rate, 0, 0), pi, gap)


def markdown_rows(text: str, columns: int) -> list:
    """Ячейки строк markdown-таблиц отчёта с заданным числом колонок.

    Число колонок различает таблицы отчёта (метрики — 9, надёжностная — 6), а
    строка-разделитель (`|---|---|`) отбрасывается: её ячейки — из дефисов.
    """
    rows = []
    for line in text.splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) == columns and not all(set(cell) <= {"-"} for cell in cells):
            rows.append(cells)
    return rows


def metric_cells(text: str, method: str, window: str) -> list:
    """Ячейки строки таблицы метрик по метке метода и окну отчёта."""
    for cells in markdown_rows(text, 9):
        if cells[0] == method and cells[1] == window:
            return cells
    raise AssertionError(f"нет строки метрик {method!r} / {window!r} в отчёте")


def folds(rows, method, fit_predict, **kwargs):
    """Фолды одного метода — контракт `walk_forward` без счётчика пропусков.

    Счётчик проверяется отдельными тестами (`TestWalkForward`); остальным
    тестам нужны только фолды, и здесь они не отвлекаются на распаковку.
    """
    predictions, _ = km.walk_forward(rows, method, fit_predict, **kwargs)
    return predictions


def method_predictions(rows):
    """Бенчмарки B1–B3 и методы §6 по двум окнам — ровно как их строит прогон."""
    predictions, _ = km.method_predictions(rows)
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
        predictions = folds(rows, "probe", self.probe(seen), min_train=24)
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
        predictions = folds(rows, "probe", self.probe(seen), min_train=24, rolling=40)
        # Окно скользящее 40 входит ПОЛНЫМ: первый фолд — 41-е заседание,
        # то есть 60 − 40 = 20 фолдов (иначе «окно 40» начиналось бы с 24).
        self.assertEqual(seen, [40] * 20)
        self.assertEqual(len(predictions), 20)
        self.assertEqual(predictions[0].meeting_date, rows[40].decision.meeting_date)
        self.assertEqual({prediction.window for prediction in predictions}, {"rolling40"})

    def test_sample_shorter_than_min_train_gives_no_folds(self):
        # Выборка короче обучения — пустой список и нулевой счётчик пропусков,
        # а не исключение: отчёт этого шага (например, на срезе фикстур)
        # обязан рендериться.
        self.assertEqual(km.walk_forward(synthetic_rows(20), "hold", km.predict_hold), ([], 0))

    def test_probs_and_step_are_passed_through(self):
        def fit_predict(train, row):
            return km.Forecast(1.5, (0.1, 0.8, 0.1))

        predictions = folds(synthetic_rows(26), "probe", fit_predict, min_train=24)
        self.assertEqual(predictions[0].step_bp, 1.5)
        self.assertEqual(predictions[0].probs, (0.1, 0.8, 0.1))

    def test_hold_predicts_zero_without_probs(self):
        predictions = folds(
            synthetic_rows(30, steps=(50,)), "hold", km.predict_hold, min_train=24
        )
        self.assertEqual(len(predictions), 6)
        for prediction in predictions:
            self.assertEqual(prediction.step_bp, 0.0)
            self.assertIsNone(prediction.probs)

    def test_repeat_repeats_the_previous_decision(self):
        rows = synthetic_rows(60, steps=(0, 50, -25))
        predictions = folds(rows, "repeat", km.predict_repeat, min_train=24)
        self.assertEqual(len(predictions), 36)
        for prediction, index in zip(predictions, range(24, 60)):
            self.assertEqual(prediction.step_bp, float(rows[index - 1].decision.step_bp))

    def test_repeat_needs_a_previous_decision(self):
        with self.assertRaises(ValueError) as caught:
            km.predict_repeat([], synthetic_rows(1)[0])
        self.assertIn("предыдущего решения", str(caught.exception))

    def test_degenerate_window_is_skipped_and_counted(self):
        # Вырожденное обучающее окно (в Tasks 4–5 — один класс у ordered-модели,
        # здесь — фолды с 30 точками в обучении) пропускается, а не роняет
        # прогон: пропуск — свойство окна, а не ошибка данных.
        rows = synthetic_rows(60)

        def fit_predict(train, row):
            if len(train) == 30 or len(train) == 31:
                raise km.DegenerateWindowError("вырожденное окно (тест)")
            return km.Forecast(float(len(train)), None)

        predictions, skipped = km.walk_forward(rows, "probe", fit_predict, min_train=24)
        self.assertEqual(skipped, 2)
        self.assertEqual(len(predictions), 34)
        # Пропущены ИМЕННО вырожденные фолды (их прогнозы = длина окна).
        realized = {prediction.step_bp for prediction in predictions}
        self.assertNotIn(30.0, realized)
        self.assertNotIn(31.0, realized)
        dates = [prediction.meeting_date for prediction in predictions]
        self.assertEqual(dates, sorted(dates))

    def test_other_exceptions_are_not_swallowed(self):
        # Пропускается ИМЕННО DegenerateWindowError: любая другая ошибка метода
        # (баг в коде, порча данных) обязана остановить прогон.
        def fit_predict(train, row):
            raise ValueError("это не вырожденное окно")

        with self.assertRaises(ValueError) as caught:
            km.walk_forward(synthetic_rows(30), "probe", fit_predict, min_train=24)
        self.assertIn("не вырожденное окно", str(caught.exception))

    def test_ar1_degenerate_folds_are_skipped_by_walk_forward(self):
        # Сквозная проверка механизма на ЖИВОМ бенчмарке: у выборки с
        # неизменным уровнем ставки AR(1) вырожден на каждом фолде — прогон
        # не падает, фолдов нет, счётчик равен числу фолдов.
        rows = synthetic_rows(30, steps=(0,))
        predictions, skipped = km.walk_forward(rows, "ar1", km.fit_ar1, min_train=24)
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, 6)

    def test_ar1_recovers_the_synthetic_level_relation_in_basis_points(self):
        rows = ar1_rows(40)
        train = rows[:24]
        row = rows[24]
        forecast = km.fit_ar1(train, row)
        # pred_level = 10 + 0.5·rate_before (уровни в %); прогноз шага — та же
        # разность в БАЗИСНЫХ пунктах, как и факт `step_bp`: × 100.
        expected = (10.0 + 0.5 * row.decision.rate_before - row.decision.rate_before) * 100.0
        self.assertAlmostEqual(forecast.step_bp, expected, places=9)
        # Масштаб пина отдельно: без ×100 прогноз был бы ~−7 вместо ~−700.
        self.assertAlmostEqual(forecast.step_bp, -700.0, places=9)
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
            (10.0 + 0.5 * row.decision.rate_before - row.decision.rate_before) * 100.0,
            places=9,
        )

    def test_ar1_rejects_a_window_too_short_for_ols(self):
        rows = ar1_rows(3)
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_ar1(rows[:2], rows[2])
        self.assertIn("AR(1)", str(caught.exception))
        self.assertIn("OLS", str(caught.exception))

    def test_ar1_rejects_a_window_without_level_variation(self):
        # Уровень в окне неизменен: наклон φ коллинеарен свободному члену,
        # регрессия не определена — вырожденное окно, а не падение statsmodels.
        rows = synthetic_rows(30, steps=(0,))
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_ar1(rows[:24], rows[24])
        self.assertIn("не меняется", str(caught.exception))


class TestTaylorRule(unittest.TestCase):
    """Метод №2: инерционное правило Тейлора (спека §6)."""

    def test_ols_recovers_the_synthetic_coefficients(self):
        # Безрелейная синтетика: OLS на 40 точках восстанавливает c, ρ, a, b
        # с точностью 1e-6. Порядок параметров — дизайн `[1, rate_before,
        # pi_saar, gap]` (свободный член первым: его добавляет add_constant).
        result = km._taylor_fit(taylor_rows(40))
        for parameter, expected in zip(result.params, (1.0, 0.9, 0.2, -0.5)):
            self.assertAlmostEqual(float(parameter), expected, places=6)

    def test_forecast_step_is_in_basis_points(self):
        # Единицы: прогноз шага — в БАЗИСНЫХ пунктах, как факт и остальные
        # прогнозы harness'а: `(pred_level − rate_before) × 100`. Урок B3
        # (Task 3): смешение п.п./б.п. делает сравнения недействительными.
        # Ожидание считается ПО ФОРМУЛЕ, а не пинуется магическим числом:
        # синтетика известна точно, поэтому прогноз обязан совпасть с фактом.
        rows = taylor_rows(40)
        row = rows[30]
        forecast = km.fit_taylor(rows[:30], row)
        expected = (row.decision.rate_after - row.decision.rate_before) * 100.0
        self.assertAlmostEqual(forecast.step_bp, expected, places=4)
        # Масштаб — отдельный пин тех же единиц: без ×100 (в п.п.) шаг этого
        # фолда был бы ≈ 0.7, и условие ниже не выполнилось бы.
        self.assertGreater(abs(forecast.step_bp), 10.0)
        self.assertIsNone(forecast.probs)

    def test_uses_hac_with_the_pinned_configuration(self):
        # Конфигурация HAC — часть спецификации (спека §6): Ньюи–Уэст,
        # maxlags=4 (≈ полугодие между заседаниями), small-sample correction
        # включена. Пин — по атрибутам ПОДОБРАННОЙ модели (решение
        # контроллера), а не подменой реализации: сравнение p-value с B3,
        # MZ/PT и методом №3 осмысленно только на одной ковариации.
        result = km._taylor_fit(taylor_rows(24))
        self.assertEqual(result.cov_type, "HAC")
        self.assertEqual(result.cov_kwds["maxlags"], 4)
        self.assertTrue(result.cov_kwds["use_correction"])

    def test_rejects_a_window_too_short_for_ols(self):
        # Вырожденное окно (2 точки) — DegenerateWindowError с понятным
        # текстом, а не LinAlgError/ZeroDivisionError из statsmodels: у
        # четырёхпараметрической регрессии не остаётся остаточных степеней
        # свободы, и HAC-поправка делит на ноль.
        rows = taylor_rows(6)
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_taylor(rows[:2], rows[2])
        self.assertIn("Тейлор", str(caught.exception))
        self.assertIn("окно короче", str(caught.exception))

    def test_window_boundary_keeps_one_residual_degree_of_freedom(self):
        # 4 заседания — 4 параметра: остаточных степеней свободы нет (HAC
        # делит на ноль), 5 — минимум. Граница пинуется, а не «примерно».
        rows = taylor_rows(6)
        with self.assertRaises(km.DegenerateWindowError):
            km.fit_taylor(rows[:4], rows[4])
        forecast = km.fit_taylor(rows[:5], rows[5])
        self.assertIsInstance(forecast, km.Forecast)

    def test_rejects_a_degenerate_design(self):
        # Регрессор, не меняющийся в окне, коллинеарен свободному члену:
        # коэффициенты не идентифицированы (пикнуть мусорные числа нельзя).
        rows = synthetic_rows(30)
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_taylor(rows[:24], rows[24])
        self.assertIn("дизайн", str(caught.exception))

    def test_rejects_a_window_with_exactly_one_constant_regressor(self):
        # Причина однозначна: постоянен РОВНО ОДИН регрессор, второй варьирует
        # (и уровень ставки тоже). При `add_constant` с умолчанием statsmodels
        # (`has_constant="skip"`) свободный член молча НЕ добавился бы:
        # дизайн — три столбца, подгонка проходит, а прогноз распаковывает
        # четыре параметра и падает `ValueError: shapes (3,) and (4,)` —
        # walk_forward такое не пропускает, и прогон рушится вместо пропуска
        # фолда со счётчиком.
        for constant, regressor in (("pi", "pi_saar"), ("gap", "gap")):
            with self.subTest(constant=regressor):
                rows = taylor_rows(30, constant=constant)
                with self.assertRaises(km.DegenerateWindowError) as caught:
                    km.fit_taylor(rows[:24], rows[24])
                self.assertIn("дизайн", str(caught.exception))
                # Тот же дизайн, что строит `_taylor_fit` (`[1, X]`), — четыре
                # столбца ранга 3: свободный член добавлен ЯВНО, поэтому
                # постоянный регрессор виден проверке ранга (иначе регрессия
                # шла бы на трёх столбцах и прогноз падал бы ValueError).
                design = np.column_stack(
                    [np.ones(24), np.asarray([row.x for row in rows[:24]], dtype=float)]
                )
                self.assertEqual(design.shape, (24, 4))
                self.assertEqual(np.linalg.matrix_rank(design), 3)

    def test_walk_forward_skips_a_fold_with_a_degenerate_design(self):
        # Сквозная проверка того же контракта: у ЧАСТИ фолдов признак постоянен
        # во всём обучающем окне (вырожденный дизайн), у остальных — варьирует.
        # Вырожденные фолды обязаны пропускаться со счётчиком, а не ронять
        # прогон (walk_forward ловит ИМЕННО DegenerateWindowError).
        rows = []
        level = 10.0
        for index in range(34):
            pi = 5.0 + 3.0 * (index % 2)
            gap = -2.0 if index < 28 else -1.9 + 0.1 * (index - 28)
            after = 1.0 + 0.9 * level + 0.2 * pi - 0.5 * gap
            step = round((after - level) * 100.0)
            decision = km.Decision(
                date(2014, 1, 1) + timedelta(days=30 * index),
                level,
                after,
                step,
                (step > 0) - (step < 0),
            )
            rows.append(km.Row(decision, pi, gap))
            level = after
        predictions, skipped = km.walk_forward(rows, "taylor", km.fit_taylor, min_train=24)
        # Обучающее окно фолда i — rows[:i]: `gap` постоянен в первых 28
        # заседаниях, поэтому вырождены фолды 24…28 (5 штук), а с 29-го окно
        # полноранговое: 10 фолдов всего, 5 — с прогнозом.
        self.assertEqual(skipped, 5)
        self.assertEqual(len(predictions), 5)
        self.assertEqual(predictions[0].meeting_date, rows[29].decision.meeting_date)
        self.assertEqual({prediction.method for prediction in predictions}, {"taylor"})

    def test_degenerate_folds_are_skipped_by_walk_forward(self):
        # Сквозная проверка механизма (как у B3): на выборке с неизменными
        # признаками Тейлор вырожден на каждом фолде — прогон не падает,
        # фолдов нет, а счётчик пропусков равен числу фолдов.
        rows = synthetic_rows(30, steps=(0,))
        predictions, skipped = km.walk_forward(rows, "taylor", km.fit_taylor, min_train=24)
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, 6)

    def test_long_run_implications_follow_the_steady_state_formula(self):
        # Из `i_t = c + ρ·i_{t−1} + a·π + b·gap` в устойчивом состоянии
        # `i* = r* + α·π + β·gap`, где r* = c/(1−ρ), α = a/(1−ρ), β = b/(1−ρ).
        # У синтетики c=1, ρ=0.9, a=0.2, b=−0.5 → r* = 10, α = 2, β = −5.
        long_run = km.taylor_long_run(taylor_rows(40))
        self.assertEqual(long_run.n, 40)
        self.assertAlmostEqual(long_run.rho, 0.9, places=6)
        self.assertAlmostEqual(long_run.c, 1.0, places=6)
        self.assertAlmostEqual(long_run.a_pi, 0.2, places=6)
        self.assertAlmostEqual(long_run.b_gap, -0.5, places=6)
        self.assertAlmostEqual(long_run.r_star, 10.0, places=5)
        self.assertAlmostEqual(long_run.alpha, 2.0, places=5)
        self.assertAlmostEqual(long_run.beta, -5.0, places=5)

    def test_long_run_refuses_a_unit_root(self):
        # ρ = 1: устойчивого состояния нет, 1 − ρ = 0 — долгосрочные
        # импликации не определены (отчёт печатает это строкой, а не падает).
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.taylor_long_run(taylor_rows(40, rho=1.0))
        self.assertIn("устойчивого состояния", str(caught.exception))

    def test_report_prints_the_taylor_row_for_both_windows(self):
        rows = taylor_rows(60)
        # Строки прогона — ровно то, что гоняет `run_summary`: бенчмарки B1–B3
        # и метод №2 (он входит в METHODS), оба окна.
        predictions = method_predictions(rows)
        text = km.render_report(
            rows, km.Skipped((), (), ()), predictions, None, today=date(2026, 10, 16)
        )
        for window in ("expanding", "rolling40"):
            self.assertIn(f"| taylor | {window} |", text)
        # Число фолдов видно в строке: 36 у расширяющегося (60 − 24) и 20 у
        # скользящего 40 — то есть метод посчитан в ОБОИХ окнах, а не пропущен.
        self.assertIn("| taylor | expanding | 36 |", text)
        self.assertIn("| taylor | rolling40 | 20 |", text)
        # DM строки метода — против каждого бенчмарка (спека §7), и долгосрочные
        # импликации — отдельным блоком.
        self.assertIn("| taylor | expanding | B1 hold |", text)
        self.assertIn("| taylor | rolling40 | B3 AR(1) |", text)
        self.assertIn("## Долгосрочные импликации правила Тейлора", text)
        self.assertIn("| r* = c/(1−ρ) | 10.00 |", text)
        self.assertIn("| α = a/(1−ρ) | 2.00 |", text)
        self.assertIn("| β = b/(1−ρ) | -5.00 |", text)

    def test_report_says_not_estimated_when_the_sample_is_degenerate(self):
        # Вырожденная выборка (признаки не меняются) — блок остаётся и
        # называет причину: отчёт не падает и не прячет блок молча.
        text = km.render_report(
            synthetic_rows(30), km.Skipped((), (), ()), [], None, today=date(2026, 10, 16)
        )
        self.assertIn("## Долгосрочные импликации правила Тейлора", text)
        self.assertIn("| долгосрочные импликации | не оценены:", text)


class TestOrderedLogit(unittest.TestCase):
    """Метод №3: вероятности классов, точечный шаг E[step] и вырожденные окна."""

    @classmethod
    def setUpClass(cls):
        cls.rows = ordinal_rows(120)
        cls.train = cls.rows[:80]

    def fit(self, gap):
        return km.fit_ordered_logit(self.train, ordinal_test_row(gap))

    def test_probabilities_are_a_distribution_in_the_fixed_class_order(self):
        forecast = self.fit(1.0)
        self.assertIsInstance(forecast.probs, tuple)
        self.assertEqual(len(forecast.probs), 3)
        self.assertAlmostEqual(sum(forecast.probs), 1.0, delta=1e-9)
        # 3-кортеж — в фиксированном порядке (−1, 0, +1), а не в порядке уровней
        # окна: у «низкого gap» максимум обязан быть в ячейке снижения (0), у
        # «высокого» — в ячейке повышения (2). Это и пинует отображение столбцов
        # statsmodels (они идут по возрастанию уровней обучающего окна).
        cold = self.fit(-3.0).probs
        hot = self.fit(3.0).probs
        self.assertEqual(cold.index(max(cold)), 0)
        self.assertEqual(hot.index(max(hot)), 2)

    def test_point_step_is_the_expected_value_in_basis_points(self):
        forecast = self.fit(1.0)
        # Средние шаги классов известны по конструкции синтетики (а не взяты из
        # `_class_means`): снижение — −50 б.п., сохранение — 0, повышение — +100.
        # E[step] = Σ_c P(c)·μ_c и обязан быть в БАЗИСНЫХ пунктах, как факт.
        means = {-1: -50.0, 0: 0.0, 1: 100.0}
        expected = sum(
            probability * means[label] for probability, label in zip(forecast.probs, (-1, 0, 1))
        )
        self.assertAlmostEqual(forecast.step_bp, expected, places=9)
        # Единицы пинуются отдельно: у уверенного повышения шаг — десятки б.п., а
        # не доли п.п. (перевод уровня в п.п. дал бы ≈ 1.0).
        confident = self.fit(3.5)
        self.assertGreater(confident.probs[2], 0.9)
        self.assertGreater(confident.step_bp, 50.0)

    def test_hike_probability_is_monotone_in_gap(self):
        # Форма зависимости — сигмоида линейного индекса: у ordered-модели
        # `P(повышение) = 1 − F(τ₂ − xβ)`, поэтому при β_gap > 0 вероятность
        # неубывающая по gap (и строго растущая там, где не упирается в 0/1).
        grid = [-4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0]
        hike = [self.fit(gap).probs[2] for gap in grid]
        cut = [self.fit(gap).probs[0] for gap in grid]
        self.assertEqual(hike, sorted(hike))
        self.assertEqual(cut, sorted(cut, reverse=True))
        middle = hike[3:8]
        self.assertTrue(all(before < after for before, after in zip(middle, middle[1:])))
        self.assertLess(hike[0], 0.1)
        self.assertGreater(hike[-1], 0.9)

    def test_a_missing_class_gets_a_zero_probability(self):
        # Окно без снижений — НЕ вырожденное (вырождено окно из одного класса):
        # у ordered-модели два уровня, и третий получает ровно 0, а сумма
        # остаётся единицей.
        without_cuts = [row for row in self.rows if row.decision.y3 >= 0]
        forecast = km.fit_ordered_logit(without_cuts[:60], ordinal_test_row(3.0))
        self.assertEqual(forecast.probs[0], 0.0)
        self.assertAlmostEqual(sum(forecast.probs), 1.0, delta=1e-9)

    def test_brier_and_log_loss_accept_the_three_tuple(self):
        probs = self.fit(2.0).probs
        # Ручной пример: факт — повышение, вероятности — из метода.
        # Brier = Σ(p − 1[y])², log loss = −ln p(факт).
        expected_brier = probs[0] ** 2 + probs[1] ** 2 + (probs[2] - 1.0) ** 2
        self.assertAlmostEqual(km.brier_multi([probs], [1]), expected_brier, places=12)
        self.assertAlmostEqual(km.log_loss_multi([probs], [1]), -math.log(probs[2]), places=12)

    def test_fit_emits_no_warnings(self):
        # Вывод тестов и прогона — только числа: предупреждения оптимизатора не
        # печатаются, а превращаются в вырожденный фолд (и видны счётчиком).
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.fit(1.0)
        self.assertEqual([str(warning.message) for warning in caught], [])

    def test_rejects_a_single_class_window(self):
        rows = synthetic_rows(40, steps=(0,))
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_ordered_logit(rows[:30], rows[30])
        self.assertIn("ordered logit", str(caught.exception))
        self.assertIn("класс", str(caught.exception))

    def test_rejects_unidentifiable_regressors(self):
        # pi_saar и gap в окне постоянны: линейный индекс не идентифицирован
        # (направление `Xv = c·1` сдвигает все пороги и не меняет
        # правдоподобие), MLE вернул бы произвольное значение.
        rows = synthetic_rows(40, steps=(0, 50, -25))
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_ordered_logit(rows[:30], rows[30])
        self.assertIn("дизайн", str(caught.exception))

    def test_perfectly_separated_window_is_degenerate(self):
        # Точный порог «gap > 0 ⇒ повышение, иначе снижение» делает классы
        # линейно разделимыми: конечного максимума правдоподобия нет
        # (коэффициенты уходят в бесконечность), и оптимизатор не сходится.
        # Такой фолд — вырожденный, а не молчаливый мусор в отчёте: именно
        # сходимость, а не форма распределения, отличает оценённое окно.
        rows = []
        level = 10.0
        for index in range(60):
            gap = -4.0 + 8.0 * index / 59
            pi = 4.0 + 2.0 * ((index * 7) % 3)
            step = 100 if gap > 0.0 else -50
            after = level + step / 100.0
            rows.append(
                km.Row(
                    km.Decision(
                        date(2014, 1, 1) + timedelta(days=30 * index),
                        level,
                        after,
                        step,
                        (step > 0) - (step < 0),
                    ),
                    pi,
                    gap,
                )
            )
            level = after
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with self.assertRaises(km.DegenerateWindowError) as raised:
                km.fit_ordered_logit(rows[:60], rows[59])
        self.assertIn("ordered logit", str(raised.exception))
        self.assertIn("не сошёлся", str(raised.exception))
        # Предупреждение не утекло наружу (вывод чистый) — оно стало причиной
        # пропуска фолда.
        self.assertEqual([str(warning.message) for warning in caught], [])

    def test_singular_information_matrix_is_degenerate(self):
        # Ветка сингулярности: у разделимых классов `np.linalg.inv(Hessian)`
        # внутри `fit` поднимает `LinAlgError`, и БЕЗ обёртки он пробил бы
        # `walk_forward` (тот пропускает только `DegenerateWindowError`) —
        # прогон упал бы вместо пропуска фолда. Какой именно ветке отказаться
        # от фолда, зависит от окна и версии statsmodels, поэтому обработка
        # пинуется подменой подгонки, а не поиском входа, который её вызовет.
        rows = ordinal_rows(60)
        with unittest.mock.patch.object(
            km.OrderedModel, "fit", side_effect=np.linalg.LinAlgError("Singular matrix")
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                with self.assertRaises(km.DegenerateWindowError) as raised:
                    km.fit_ordered_logit(rows[:40], ordinal_test_row(1.0))
        self.assertIn("ordered logit", str(raised.exception))
        self.assertIn("сингулярная", str(raised.exception))
        self.assertEqual([str(warning.message) for warning in caught], [])

    def test_a_non_finite_probability_never_reaches_the_forecast(self):
        # Тройка проверяется до `E[step]`: иначе nan молча уехал бы и в точку, и
        # в Brier/log loss отчёта. Это `ValueError` (дефект кода), а НЕ
        # `DegenerateWindowError`: walk-forward обязан остановить прогон, а не
        # пропустить фолд.
        rows = ordinal_rows(40)
        result = unittest.mock.Mock()
        result.mle_retvals = {"converged": True}
        result.predict.return_value = [np.array([0.4, float("nan"), 0.6])]
        with unittest.mock.patch.object(km.OrderedModel, "fit", return_value=result):
            with self.assertRaises(ValueError) as raised:
                km.fit_ordered_logit(rows[:30], ordinal_test_row(1.0))
        self.assertEqual(type(raised.exception), ValueError)
        self.assertIn("nan", str(raised.exception))
        self.assertIn("не вероятности классов", str(raised.exception))

    def test_walk_forward_skips_degenerate_folds_with_a_counter(self):
        # Один класс в окне (ставка не менялась): вырожденный фолд пропускается
        # со счётчиком, прогон не падает.
        rows = synthetic_rows(30, steps=(0,))
        predictions, skipped = km.walk_forward(
            rows, "ordered_logit", km.fit_ordered_logit, min_train=24
        )
        self.assertEqual(predictions, [])
        self.assertEqual(skipped, 6)


class TestClimatology(unittest.TestCase):
    """Базлайн `climatology`: частоты классов окна и точка Σ p_c·μ_c (спека §7)."""

    def setUp(self):
        # Окно из 8 заседаний: 4 сохранения, 2 повышения (+50, +100) и 2 снижения
        # (−50, −25). Частоты 0.5/0.25/0.25 и средние шаги 0/+75/−37.5 берутся
        # руками из конструкции, а не из реализации.
        self.train = synthetic_rows(8, steps=(0, 0, 50, 0, -50, 100, 0, -25))
        self.row = ordinal_test_row(0.0)

    def test_probabilities_are_the_window_frequencies(self):
        forecast = km.fit_climatology(self.train, self.row)
        self.assertEqual(forecast.probs, (0.25, 0.5, 0.25))
        self.assertAlmostEqual(sum(forecast.probs), 1.0, delta=1e-9)

    def test_point_step_is_the_frequency_weighted_mean_in_basis_points(self):
        forecast = km.fit_climatology(self.train, self.row)
        # E[step] = 0.25·(−37.5) + 0.5·0 + 0.25·(+75) = 9.375 б.п. В п.п. вышло
        # бы 0.09375 — масштаб пинуется отдельной проверкой ниже.
        self.assertAlmostEqual(forecast.step_bp, 9.375, places=9)
        self.assertGreater(abs(forecast.step_bp), 1.0)

    def test_brier_and_log_loss_on_a_hand_example(self):
        # Факт — снижение: Brier = (0.25−1)² + 0.5² + 0.25² = 0.875,
        # log loss = −ln 0.25.
        probs = km.fit_climatology(self.train, self.row).probs
        self.assertAlmostEqual(km.brier_multi([probs], [-1]), 0.875, places=12)
        self.assertAlmostEqual(km.log_loss_multi([probs], [-1]), -math.log(0.25), places=12)

    def test_single_class_window_is_degenerate(self):
        rows = synthetic_rows(30, steps=(0,))
        with self.assertRaises(km.DegenerateWindowError) as caught:
            km.fit_climatology(rows[:24], rows[24])
        self.assertIn("climatology", str(caught.exception))


class TestProbabilisticReportRows(unittest.TestCase):
    """Строки отчёта методов с распределением: `climatology` и `ordered_logit`."""

    @classmethod
    def setUpClass(cls):
        cls.rows = ordinal_rows(60)
        cls.predictions, cls.fold_skips = km.method_predictions(cls.rows)

    def render(self):
        return km.render_report(
            self.rows,
            km.Skipped((), (), ()),
            self.predictions,
            None,
            today=date(2026, 10, 16),
            fold_skips=self.fold_skips,
        )

    def test_both_methods_have_a_row_for_both_windows(self):
        text = self.render()
        for method in ("climatology", "ordered_logit"):
            for window, folds in (("expanding", 36), ("rolling40", 20)):
                self.assertEqual(metric_cells(text, method, window)[2], str(folds))
                # DM — против каждого бенчмарка (спека §7), как у остальных строк.
                self.assertIn(f"| {method} | {window} | B1 hold |", text)

    def test_brier_log_loss_and_reliability_bins_are_filled(self):
        text = self.render()
        for method in ("climatology", "ordered_logit"):
            for window in ("expanding", "rolling40"):
                cells = metric_cells(text, method, window)
                self.assertNotEqual(cells[7], "—")  # Brier
                self.assertNotEqual(cells[8], "—")  # log loss
            self.assertTrue(any(cells[0] == method for cells in markdown_rows(text, 6)))

    def test_point_step_is_scored_like_the_other_methods(self):
        text = self.render()
        for method in ("climatology", "ordered_logit"):
            cells = metric_cells(text, method, "expanding")
            for column in (3, 4, 5, 6):  # RMSE, MAE, hit rate, QWK
                self.assertNotEqual(cells[column], "—")
        self.assertIn("## Надёжностная таблица", text)

    def test_no_zero_fold_rows_when_every_method_folds(self):
        # Основная таблица метрик: окна этой синтетики не вырождаются ни у одного
        # метода, поэтому строк с пометкой «0 фолдов» в ней быть не должно (это
        # не «шум формата», а признак метода без единого фолда). В режимном блоке
        # этого отчёта такие строки законны: заседаний 2024–2026 в выборке нет.
        section = self.render().split("## Метрики: метод × окно")[1].split("## Долгосрочные")[0]
        self.assertFalse(
            any(cells[2] == "0 фолдов" for cells in markdown_rows(section, 9))
        )
        for method, _ in km.METHODS:
            for window, folds in (("expanding", 36), ("rolling40", 20)):
                self.assertEqual(
                    metric_cells(section, km.method_label(method), window)[2], str(folds)
                )


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

    def test_probabilistic_metrics_refuse_non_finite_probabilities(self):
        # NaN и ±inf отвергаются ЯВНО (`math.isfinite`): сравнения `p < 0` и
        # `p > 1` для NaN ложны, и без этой проверки тройка с NaN проходила бы
        # диапазон молча, а log loss вернул бы nan-среднее вместо исключения.
        for metric in (km.brier_multi, km.log_loss_multi, km.reliability_bins):
            for broken in (
                (float("nan"), 0.5, 0.5),
                (0.5, float("nan"), 0.5),
                (0.5, 0.5, float("nan")),
                (float("inf"), 0.0, 0.0),
                (0.5, 0.5, float("-inf")),
            ):
                with self.assertRaises(ValueError) as caught:
                    metric([broken], [0])
                self.assertIn("не вероятности классов", str(caught.exception))

    def test_log_loss_does_not_silently_average_a_nan(self):
        # Ядро дефекта: тройка с NaN проходила проверку диапазона (сравнения с NaN
        # ложны), и log loss отдавал nan-среднее вместо исключения — число отчёта
        # «уезжало» в nan молча. Теперь такой фолд отвергается.
        with self.assertRaises(ValueError):
            km.log_loss_multi(
                [(0.2, 0.7, 0.1), (float("nan"), 0.5, 0.5), (0.1, 0.2, 0.7)], [0, 0, 1]
            )
        with self.assertRaises(ValueError):
            km.brier_multi([(float("nan"), float("nan"), float("nan"))], [0])

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

    def test_another_seed_gives_another_resample(self):
        # Сид влияет на ресэмпл (иначе «фиксированный сид» был бы декорацией):
        # ряды ошибок подобраны так, что p не насыщается в 0/1, и интервалы
        # разных сидов расходятся.
        err_a = [3.0, -2.0, 5.0, 0.0, -1.0, 4.0, 2.0, -3.0, 1.5, -0.5, 2.5, -1.5]
        err_b = [2.0, 0.0, 3.0, 1.0, 0.5, 2.5, 1.0, -1.0, 1.0, 0.5, 3.5, -0.5]
        default = km.block_bootstrap_rmse_diff(err_a, err_b)
        other = km.block_bootstrap_rmse_diff(err_a, err_b, seed=km.SEED + 1)
        # Точечная оценка от сида не зависит — различаться обязаны ресэмплы.
        self.assertEqual(default.rmse_diff, other.rmse_diff)
        self.assertNotEqual((default.low, default.high, default.p_value),
                            (other.low, other.high, other.p_value))

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
        cls.predictions = method_predictions(cls.rows)
        cls.skipped = km.Skipped((), (), ())

    def render(self, predictions=None, forecast=None, fold_skips=None):
        return km.render_report(
            self.rows,
            self.skipped,
            self.predictions if predictions is None else predictions,
            forecast,
            today=date(2026, 10, 16),
            fold_skips=fold_skips,
        )

    def test_report_has_every_section_of_the_step(self):
        text = self.render()
        for section in (
            "## Выборка",
            "## Walk-forward",
            "## Метрики: метод × окно",
            "## Долгосрочные импликации правила Тейлора",
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
        # В этой синтетике признаки `pi_saar` и `gap` постоянны: метод №2 и
        # ordered_logit вырождены на КАЖДОМ окне (фолды пропущены со счётчиком),
        # а бенчмарки B1–B3 и `climatology` дают полный набор фолдов. Ожидание
        # считается от выборки и констант окна, а не литералами.
        expanding = [p for p in self.predictions if p.window == "expanding"]
        rolling = [p for p in self.predictions if p.window == "rolling40"]
        folding = sorted({p.method for p in expanding})
        self.assertEqual(folding, ["ar1", "climatology", "hold", "repeat"])
        self.assertEqual({p.method for p in rolling}, set(folding))
        self.assertEqual(len(expanding), len(folding) * (len(self.rows) - km.MIN_TRAIN))
        self.assertEqual(len(rolling), len(folding) * (len(self.rows) - km.ROLLING_WINDOW))
        self.assertIn(f"расширяющееся окно (минимум {km.MIN_TRAIN} заседаний в обучении)", text)
        self.assertIn(f"скользящее окно {km.ROLLING_WINDOW} заседаний", text)

    def test_fold_summary_shows_realized_and_skipped_folds(self):
        # Счётчик пропущенных вырожденных окон — в сводке «метод × окно»
        # (Tasks 4–5 пропускают такие фолды, и отчёт обязан это показать).
        text = self.render(fold_skips={("hold", "expanding"): 2})
        self.assertIn("| B1 hold | expanding | 36 | 2 |", text)
        self.assertIn("| B2 repeat | expanding | 36 | 0 |", text)

    def test_fold_summary_lists_methods_that_skipped_every_window(self):
        # Сводка — перепись «метод × окно», а не только строки с прогнозами:
        # метод, вырожденный на ВСЕХ окнах (здесь ordered_logit при постоянных
        # `pi_saar` и `gap`), остаётся в таблице с нулём фолдов и счётчиком
        # пропущенных окон — спека §6 требует записи о пропуске в отчёт, и без
        # строки пропуск метода был бы виден только консольным итогом.
        text = km.render_report(
            self.rows,
            self.skipped,
            self.predictions,
            None,
            today=date(2026, 10, 16),
            fold_skips=km.method_predictions(self.rows)[1],
        )
        self.assertIn("| ordered_logit | expanding | 0 | 36 |", text)
        self.assertIn("| ordered_logit | rolling40 | 0 | 20 |", text)
        self.assertIn("| taylor | expanding | 0 | 36 |", text)

    def test_metric_table_has_a_row_per_method_and_window(self):
        text = self.render()
        for method_label in ("B1 hold", "B2 repeat", "B3 AR(1)"):
            for window in ("expanding", "rolling40"):
                self.assertIn(f"| {method_label} | {window} |", text)
        # Малая отрицательная разница печатается как «0.00», а не «-0.00»:
        # минус в разрядах таблицы читался бы как ошибка знака.
        self.assertNotIn("-0.00", text)

    def test_metric_table_lists_methods_without_folds_as_zero_folds(self):
        # Таблица метрик — тот же перечень `METHODS` × окна, что и сводка: строку
        # получает каждый метод, и вырожденный на всех окнах виден пометкой
        # «0 фолдов» с прочерками, а не исчезает из отчёта (в этой синтетике
        # `pi_saar` и `gap` постоянны, поэтому вырождены метод №2 и метод №3).
        text = self.render()
        for window in ("expanding", "rolling40"):
            for method in ("taylor", "ordered_logit"):
                self.assertIn(
                    f"| {method} | {window} | 0 фолдов | — | — | — | — | — | — |", text
                )
            for method in ("hold", "repeat", "ar1", "climatology"):
                cells = metric_cells(text, km.method_label(method), window)
                self.assertNotIn("—", cells[3])  # RMSE посчитан — фолды были
        # Пометка появляется только там, где фолдов нет: у строк с фолдами —
        # их число (та же колонка `n`).
        self.assertNotIn("| B1 hold | expanding | 0 фолдов |", text)

    def test_benchmark_rows_have_no_probabilistic_metrics(self):
        # Спека §5: вырожденные бенчмарки распределений не дают — Brier и log
        # loss в их строках «—». Вероятностные строки есть только у методов с
        # распределением (`climatology`, `ordered_logit`), вместе с ними в
        # отчёте появляется и надёжностная таблица.
        text = self.render()
        for method_label in ("B1 hold", "B2 repeat", "B3 AR(1)"):
            for window in ("expanding", "rolling40"):
                cells = metric_cells(text, method_label, window)
                self.assertEqual((cells[7], cells[8]), ("—", "—"))
        self.assertIn("## Надёжностная таблица", text)
        self.assertTrue(any(cells[0] == "climatology" for cells in markdown_rows(text, 6)))
        self.assertFalse(any(cells[0] == "B1 hold" for cells in markdown_rows(text, 6)))

    def test_significance_section_covers_every_pair_against_benchmarks(self):
        text = self.render()
        # DM каждой строки против каждого бенчмарка, кроме себя самого.
        for method_label in ("B1 hold", "B2 repeat", "B3 AR(1)"):
            for opponent in ("B1 hold", "B2 repeat", "B3 AR(1)"):
                if method_label != opponent:
                    self.assertIn(f"| {method_label} | expanding | {opponent} |", text)
        # Путь назван явно (требование плана: библиотека или формула).
        self.assertIn("statsmodels", text)

    def test_significance_pairs_on_shared_folds_only(self):
        # Метод, пропустивший фолд (вырожденное окно), сравнивается с
        # бенчмарками на ОБЩИХ заседаниях: пара строится, а не падает, и
        # заседание выпадает у обеих сторон, а не у одной.
        # Заседание 50-е — внутри обоих окон (расширяющееся начинается с 25-го,
        # скользящее 40 — с 41-го), поэтому выпадают оба фолда.
        dropped = self.rows[50].decision.meeting_date
        trimmed = [
            prediction
            for prediction in self.predictions
            if not (prediction.method == "repeat" and prediction.meeting_date == dropped)
        ]
        self.assertEqual(len(trimmed), len(self.predictions) - 2)  # оба окна
        text = self.render(trimmed)
        for pair in (
            "| B2 repeat | expanding | B1 hold |",
            "| B1 hold | expanding | B2 repeat |",
            "| B2 repeat | expanding | B3 AR(1) |",
        ):
            self.assertIn(pair, text)

    def test_significance_refuses_groups_without_shared_folds(self):
        # Ни одного общего заседания — сравнивать нечего: ошибка прогона, а не
        # строка с пустыми числами.
        alien = [
            km.Prediction(date(2030, 1, 1), 0.0, 0, "alien", "expanding", 0.0, None)
        ]
        with self.assertRaises(ValueError) as caught:
            km.render_report(
                self.rows,
                self.skipped,
                list(self.predictions) + alien,
                None,
                today=date(2026, 10, 16),
            )
        self.assertIn("общих заседаний", str(caught.exception))

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


class TestCurrentForecast(unittest.TestCase):
    """Текущий прогноз на ближайшее заседание: выбор заседания, as-of, JSON (§8).

    Обучение идёт на полной выборке (а не на фолде walk-forward), поэтому
    синтетика здесь — `ordinal_rows(60)`: три класса решения и регрессоры
    полного ранга, на которых оцениваются оба метода §6.
    """

    ROWS = ordinal_rows(60)
    FORECAST_METHODS = tuple(method for method, _ in km.FORECAST_METHODS)

    def series(self, today=date(2016, 3, 15)):
        """Малые ряды входов с границами доступности ровно вокруг `today`.

        Точки, доступные ПОЗЖЕ даты среза, — ловушки на утечку as-of: они
        заведомо исказили бы признаки, если бы `forecast_row` брал последнюю
        точку ряда, а не последнюю доступную.
        """
        key_rate = [
            (date(2016, 1, 1), 11.0),
            (today, 10.5),  # точка ровно на дату среза — участвует
            (date(2016, 4, 1), 99.0),  # позже даты среза — не участвует
            (date(2016, 6, 1), 9.0),  # позже даты среза — не участвует
        ]
        cpd = [
            (date(2016, 1, 31), 0.5),  # доступна 2016-02-29
            (date(2016, 2, 29), 0.3),  # доступна 2016-03-31 — позже среза
        ]
        ibk = [
            (date(2016, 1, 1), 1.0),  # доступна 2016-02-29
            (date(2016, 2, 1), 2.0),  # доступна 2016-03-31 — позже среза
        ]
        return key_rate, cpd, ibk

    def build(self, today, meetings, key_rate=None, cpd=None, ibk=None):
        """`current_forecast` на рядах `series(today)` (можно подменить входы)."""
        default_key_rate, default_cpd, default_ibk = self.series(today)
        return km.current_forecast(
            self.ROWS,
            meetings,
            default_key_rate if key_rate is None else key_rate,
            default_cpd if cpd is None else cpd,
            default_ibk if ibk is None else ibk,
            today,
        )

    def row(self, today, meeting):
        """Прогнозная строка из тех же входов, что считает `current_forecast`.

        Строка собирается ИЗ ГОТОВЫХ входов (`forecast_inputs` → `forecast_row`):
        тройка считается один раз, как в прогоне, — в тесте нет второй формулы.
        """
        return km.forecast_row(meeting, km.forecast_inputs(today, *self.series(today)))

    def test_takes_the_first_meeting_strictly_after_the_run_date(self):
        today = date(2016, 3, 15)
        forecast = self.build(
            today,
            # Порядок списка не важен: ближайшее заседание — min из будущих.
            [date(2016, 6, 10), date(2016, 1, 1), today, date(2016, 3, 20)],
        )
        # Заседание РОВНО на дату среза не выбирается: строго позже.
        self.assertEqual(forecast.meeting_date, date(2016, 3, 20))
        self.assertEqual(forecast.data_asof, today)

    def test_the_acceptance_run_targets_the_october_meeting(self):
        # Спека §1: на дату прогона 2026-10-16 прогноз идёт на 23.10.2026.
        # Календарь — фикстура (полный экспорт events_calendar), ряды входов —
        # `series` (as-of на 2026-10-16 берёт последние доступные точки).
        forecast = self.build(date(2026, 10, 16), fixture_meetings())
        self.assertEqual(forecast.meeting_date, date(2026, 10, 23))

    def test_without_future_meetings_there_is_no_forecast(self):
        today = date(2016, 3, 15)
        forecast = self.build(
            today, [date(2014, 2, 14), date(2016, 3, 1), today]
        )
        self.assertIsNone(forecast)

    def test_forecast_row_takes_inputs_as_of_the_run_date(self):
        today = date(2016, 3, 15)
        key_rate, cpd, ibk = self.series(today)
        row = self.row(today, date(2016, 3, 25))
        self.assertEqual(row.decision.meeting_date, date(2016, 3, 25))
        # Ставка — точка ровно на дату среза; точки позже (99.0) не читаются.
        self.assertEqual(row.decision.rate_before, 10.5)
        # Базовый ИПЦ: точка 2016-02-29 доступна только 2016-03-31 — берётся
        # январьская; приведение к годовому — `annualize_saar`.
        self.assertEqual(row.pi_saar, km.annualize_saar(0.5))
        self.assertEqual(row.gap, 1.0)
        # Те же правила ещё раз, но с более близкими точками, которые НЕ
        # доступны на дату среза: строка обязана остаться той же.
        leaked = km.forecast_row(
            date(2016, 3, 25),
            km.forecast_inputs(
                today,
                key_rate + [(date(2016, 3, 20), 8.0)],
                cpd + [(date(2016, 3, 31), 9.9)],
                ibk + [(date(2016, 3, 1), 9.9)],
            ),
        )
        self.assertEqual(leaked, row)

    def test_forecast_row_refuses_unavailable_inputs(self):
        # `None` на месте входа (любого из `FORECAST_INPUTS`) — строку не
        # собрать: прогнозная строка не выдумывает отсутствующий вход.
        today = date(2016, 3, 15)
        inputs = list(km.forecast_inputs(today, *self.series(today)))
        self.assertEqual(len(inputs), len(km.FORECAST_INPUTS))
        for index, name in enumerate(km.FORECAST_INPUTS):
            with self.subTest(input=name):
                broken = list(inputs)
                broken[index] = None
                self.assertIsNone(km.forecast_row(date(2016, 3, 25), tuple(broken)))

    def test_forecast_row_marks_the_meeting_facts_as_unknown(self):
        row = self.row(date(2016, 3, 15), date(2016, 3, 25))
        # Фактов будущего заседания не существует: фактические поля `Decision`
        # несут ОДИН маркер «нет данных» — NaN, а не ноль («нулевой шаг» /
        # «сохранение» выглядели бы правдоподобным значением). Методы их не
        # читают — следующий тест подменяет факты мусором.
        for value in (row.decision.rate_after, row.decision.step_bp, row.decision.y3):
            with self.subTest(value=value):
                self.assertTrue(math.isnan(value))
        # Маркер — один и тот же объект у всех трёх полей.
        self.assertIs(row.decision.rate_after, km.FORECAST_UNKNOWN_FACT)
        self.assertIs(row.decision.y3, km.FORECAST_UNKNOWN_FACT)

    def test_methods_never_read_the_unknown_facts_of_the_forecast_row(self):
        row = self.row(date(2016, 3, 15), date(2016, 3, 25))
        garbage = dataclasses.replace(
            row,
            decision=dataclasses.replace(
                row.decision, rate_after=123.45, step_bp=999, y3=-1
            ),
        )
        for method, fit_predict in km.FORECAST_METHODS:
            with self.subTest(method=method):
                self.assertEqual(
                    fit_predict(self.ROWS, row), fit_predict(self.ROWS, garbage)
                )

    def test_forecast_json_matches_the_artifact_format(self):
        today = date(2016, 3, 15)
        forecast = self.build(today, [date(2016, 3, 20), date(2016, 6, 10)])
        payload = json.loads(km.forecast_json(forecast))
        self.assertEqual(
            sorted(payload),
            ["data_asof", "forecast_date", "meeting_date", "methods", "note", "rate_before", "sample_size"],
        )
        self.assertEqual(payload["forecast_date"], today.isoformat())
        self.assertEqual(payload["data_asof"], today.isoformat())
        self.assertEqual(payload["meeting_date"], "2016-03-20")
        self.assertEqual(payload["sample_size"], len(self.ROWS))
        # rate_before — уровень ставки на дату среза: writer журнала переводит
        # шаг метода в ожидаемую ставку (rate_before + step_bp/100) и не читает
        # ClickHouse повторно.
        self.assertEqual(
            payload["rate_before"],
            self.row(today, date(2016, 3, 20)).decision.rate_before,
        )
        # Методы §6 в фиксированном порядке: №2 (точечный шаг) и №3
        # (вероятности классов и E[step]).
        self.assertEqual(list(payload["methods"]), list(self.FORECAST_METHODS))
        self.assertIn("forecast_log", payload["note"])
        row = self.row(today, date(2016, 3, 20))
        taylor = km.fit_taylor(self.ROWS, row)
        self.assertEqual(payload["methods"]["taylor"], {"step_bp": taylor.step_bp})
        ordered = km.fit_ordered_logit(self.ROWS, row)
        methods = payload["methods"]["ordered_logit"]
        self.assertEqual(sorted(methods), ["expected_step_bp", "probs"])
        # Ключи классов — строки «-1»/«0»/«+1»: те же классы, что в отчёте и
        # CSV, взятые в порядке `_CLASS_ORDER`; вероятности суммируются в 1.
        self.assertEqual(list(methods["probs"]), ["-1", "0", "+1"])
        self.assertEqual(list(methods["probs"].values()), list(ordered.probs))
        self.assertAlmostEqual(sum(methods["probs"].values()), 1.0)
        self.assertEqual(methods["expected_step_bp"], ordered.step_bp)

    def test_forecast_json_serialization_is_stable(self):
        # Сериализация не добавляет ничего от себя: равный прогноз — равный
        # текст (порядок ключей, форматирование). Детерминизм ПРОГОНА (отметок
        # времени сверх даты нет) пинован на уровне артефакта —
        # `test_forecast_json_is_identical_across_runs`.
        today = date(2016, 3, 15)
        first = km.forecast_json(self.build(today, [date(2016, 3, 20)]))
        second = km.forecast_json(self.build(today, [date(2016, 3, 20)]))
        self.assertEqual(first, second)

    def test_forecast_artifact_is_written_or_cleared(self):
        # Артефакт — снимок прогноза: ему нельзя ни остаться от прошлого
        # прогона, ни появиться без единого метода.
        forecast = self.build(date(2016, 3, 15), [date(2016, 3, 20)])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / km.FORECAST_NAME
            self.assertTrue(km.write_forecast_artifact(path, forecast))
            self.assertEqual(
                path.read_text(encoding="utf-8"), km.forecast_json(forecast)
            )
            self.assertFalse(
                km.write_forecast_artifact(
                    path, dataclasses.replace(forecast, methods={})
                )
            )
            self.assertFalse(path.exists())
            path.write_text("устаревший прогноз", encoding="utf-8")
            self.assertFalse(km.write_forecast_artifact(path, None))
            self.assertFalse(path.exists())

    def test_unavailable_input_keeps_the_methods_out_without_failing(self):
        # Признак недоступен на дату среза: методы не оцениваются, причина — в
        # `skipped`/`note`, и прогон не падает. Прогноз при этом остаётся в
        # отчёте, но артефакт по нему не пишется (`write_forecast_artifact`:
        # пустой `methods` — не прогноз).
        today = date(2016, 3, 15)
        forecast = self.build(today, [date(2016, 3, 20)], key_rate=[])
        self.assertEqual(forecast.methods, {})
        self.assertEqual(
            [method for method, _ in forecast.skipped], list(self.FORECAST_METHODS)
        )
        self.assertIn("rate_before", forecast.note)
        self.assertIn("недоступны входы", forecast.note)
        payload = json.loads(km.forecast_json(forecast))
        self.assertEqual(payload["methods"], {})
        self.assertIn("forecast_log", payload["note"])

    def test_current_forecast_refuses_an_empty_sample(self):
        # Пустая выборка — не «прогноз без данных»: срок наблюдения и методы на
        # ней не определены, и это ошибка вызывающего, а не свойство данных.
        with self.assertRaises(ValueError) as caught:
            km.current_forecast(
                [], [date(2016, 3, 20)], *self.series(date(2016, 3, 15)),
                date(2016, 3, 15),
            )
        self.assertIn("выборка", str(caught.exception))

    def test_missing_inputs_are_named_one_by_one(self):
        today = date(2016, 3, 15)
        for name, overrides in (
            ("rate_before", {"key_rate": []}),
            ("pi_saar", {"cpd": []}),
            ("gap", {"ibk": []}),
        ):
            with self.subTest(input=name):
                forecast = self.build(today, [date(2016, 3, 20)], **overrides)
                self.assertIn(name, forecast.note)

    def test_degenerate_training_sample_keeps_the_methods_out(self):
        # Полная выборка вырождена для обоих методов (постоянные признаки,
        # один класс): причина — в `skipped`/`note`, а не исключение.
        today = date(2016, 3, 15)
        forecast = km.current_forecast(
            synthetic_rows(30), [date(2016, 3, 20)], *self.series(today), today
        )
        self.assertEqual(forecast.methods, {})
        self.assertEqual(
            [method for method, _ in forecast.skipped], list(self.FORECAST_METHODS)
        )
        for method, reason in forecast.skipped:
            with self.subTest(method=method):
                self.assertTrue(reason)

    def test_forecast_block_lists_the_numbers_and_the_log_caveat(self):
        today = date(2016, 3, 15)
        forecast = self.build(today, [date(2016, 3, 20)])
        block = km.render_forecast(forecast)
        for text in (
            "2016-03-20",  # заседание
            "2016-03-15",  # дата среза данных
            str(len(self.ROWS)),  # размер выборки
            # срок наблюдения
            f"{self.ROWS[0].decision.meeting_date} … {self.ROWS[-1].decision.meeting_date}",
            "P(-1)", "P(0)", "P(+1)",  # вероятности классов…
            "%",  # …в процентах
            "E[step]", "б.п.",  # E[step]/шаг в базисных пунктах
            "сам скрипт в журнал",  # оговорка про forecast_log
            "не пишет",
            "scripts/key_rate_forecast_log.py",
        ):
            self.assertIn(text, block)
        # Раздел отчёта собирается из этого блока тем же путём, что и прогон.
        report = km.render_report(
            self.ROWS, km.Skipped((), (), ()), [], block, today=today
        )
        self.assertIn("## Текущий прогноз", report)
        self.assertIn("2016-03-20", report)

    def test_forecast_block_names_a_skipped_method(self):
        today = date(2016, 3, 15)
        forecast = self.build(today, [date(2016, 3, 20)], cpd=[])
        block = km.render_forecast(forecast)
        self.assertIn("не оценён", block)
        self.assertIn("pi_saar", block)


class TestRunSummaryArtifacts(unittest.TestCase):
    """Живой контур прогона: отчёт, CSV и прогноз пишутся, окна считаются (§8)."""

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
            report_text = report.read_text(encoding="utf-8")
            self.assertIn("# Модель ключевой ставки v0", report_text)
            # 26 решений среза: расширяющееся окно — 2 фолда у каждого метода
            # (6 строк METHODS × 2 = 12 строк CSV + заголовок), скользящее 40 —
            # ноль (выборка короче окна).
            self.assertEqual(len(predictions.read_text(encoding="utf-8").splitlines()), 13)
            # Строки метода №2, метода №3 и базлайна climatology, а также блок
            # долгосрочных импликаций — в отчёте.
            for method in ("taylor", "climatology", "ordered_logit"):
                self.assertIn(f"| {method} | expanding |", report_text)
            self.assertIn("## Долгосрочные импликации правила Тейлора", report_text)
            self.assertIn("taylor", text)
            self.assertIn("фолдов (расширяющееся окно", text)
            self.assertIn("фолдов (скользящее окно", text)

    def test_console_metrics_follow_the_rolling_flag(self):
        with tempfile.TemporaryDirectory() as tmp:
            _, expanding = self.run_into(tmp, date(2017, 1, 1), rolling=False)
            _, rolling = self.run_into(tmp, date(2017, 1, 1), rolling=True)
        self.assertIn("метрики окна «expanding»", expanding)
        self.assertIn("метрики окна «rolling40»", rolling)

    def stale_artifacts(self, directory):
        """Три артефакта прошлого прогона (§8) — по одному на каждый путь."""
        paths = {}
        for name in km.ARTIFACT_NAMES:
            path = Path(directory) / name
            path.write_text("снимок прошлого прогона", encoding="utf-8")
            paths[name] = path
        return paths

    def test_early_exit_without_meetings_clears_stale_artifacts(self):
        # Пустой календарь — ранний выход ДО записи артефактов: без подчистки
        # отчёт, CSV и JSON прошлого прогона читались бы как результат этого.
        with tempfile.TemporaryDirectory() as tmp:
            stale = self.stale_artifacts(tmp)
            with unittest.mock.patch.object(
                km, "ch_query", side_effect=ch_query_without_meetings
            ):
                with contextlib.redirect_stdout(io.StringIO()) as out:
                    code = km.run_summary(
                        rolling=False, today=date(2017, 1, 1), out_dir=Path(tmp)
                    )
            self.assertEqual(code, 1)
            for name, path in stale.items():
                with self.subTest(artifact=name):
                    self.assertFalse(path.exists())
            self.assertIn("канонических заседаний нет", out.getvalue())
            self.assertIn("устаревшие артефакты прошлого прогона удалены", out.getvalue())

    def test_early_exit_without_rows_clears_stale_artifacts(self):
        # Пустая выборка (по решениям признаков нет вовсе) — второй ранний
        # выход: артефакты прошлого прогона обязаны исчезнуть так же.
        with tempfile.TemporaryDirectory() as tmp:
            stale = self.stale_artifacts(tmp)
            with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
                with unittest.mock.patch.object(km, "load_cpd_core", return_value=[]):
                    with contextlib.redirect_stdout(io.StringIO()) as out:
                        code = km.run_summary(
                            rolling=False, today=date(2017, 1, 1), out_dir=Path(tmp)
                        )
            self.assertEqual(code, 1)
            for name, path in stale.items():
                with self.subTest(artifact=name):
                    self.assertFalse(path.exists())
            self.assertIn("выборка пуста", out.getvalue())
            self.assertIn("устаревшие артефакты прошлого прогона удалены", out.getvalue())

    def test_early_exit_without_stale_artifacts_is_silent_about_clearing(self):
        # Чистить было нечего — строка об удалении не печатается (сообщение не
        # должно обещать подчистку, которой не было).
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(
                km, "ch_query", side_effect=ch_query_without_meetings
            ):
                with contextlib.redirect_stdout(io.StringIO()) as out:
                    code = km.run_summary(
                        rolling=False, today=date(2017, 1, 1), out_dir=Path(tmp)
                    )
            self.assertEqual(code, 1)
            self.assertNotIn("устаревшие артефакты", out.getvalue())

    def test_writes_the_forecast_artifact_and_section(self):
        with tempfile.TemporaryDirectory() as tmp:
            code, text = self.run_into(tmp, date(2017, 1, 1))
            self.assertEqual(code, 0)
            forecast_path = Path(tmp) / km.FORECAST_NAME
            self.assertTrue(forecast_path.is_file())
            payload = json.loads(forecast_path.read_text(encoding="utf-8"))
            # Ближайшее заседание — первая дата календаря строго позже среза.
            expected = min(day for day in fixture_meetings() if day > date(2017, 1, 1))
            self.assertEqual(payload["meeting_date"], expected.isoformat())
            self.assertEqual(payload["data_asof"], "2017-01-01")
            self.assertEqual(payload["sample_size"], 26)
            self.assertEqual(sorted(payload["methods"]), ["ordered_logit", "taylor"])
            report = (Path(tmp) / km.REPORT_NAME).read_text(encoding="utf-8")
            self.assertIn("## Текущий прогноз", report)
            self.assertIn(expected.isoformat(), report)
            self.assertIn("прогноз на ближайшее заседание", text)

    def test_reports_without_writing_a_forecast_when_no_meeting_is_ahead(self):
        with tempfile.TemporaryDirectory() as tmp:
            # Снимок прошлого прогона НА МЕСТЕ: оставленный, он был бы прочитан
            # как текущий прогноз (в имени файла даты не видно).
            stale = Path(tmp) / km.FORECAST_NAME
            stale.write_text("устаревший прогноз", encoding="utf-8")
            code, text = self.run_into(tmp, date(2028, 1, 1))
            self.assertEqual(code, 0)
            self.assertFalse(stale.exists())
            self.assertIn("не сделан", text)
            self.assertIn("не пишется", text)
            self.assertIn("устаревший файл", text)
            report = (Path(tmp) / km.REPORT_NAME).read_text(encoding="utf-8")
            self.assertNotIn("## Текущий прогноз", report)

    def test_reports_without_writing_a_forecast_when_no_method_is_evaluable(self):
        # Выборка непуста, но для ОБОИХ методов §6 вырождена (признаки
        # постоянны): прогноз есть, методов в нём нет — и артефакт не пишется,
        # а прежний файл удаляется. Причина — в консоли, прогон не падает.
        with tempfile.TemporaryDirectory() as tmp:
            stale = Path(tmp) / km.FORECAST_NAME
            stale.write_text("устаревший прогноз", encoding="utf-8")
            with unittest.mock.patch.object(km, "ch_query", side_effect=ch_query_from_fixtures):
                with unittest.mock.patch.object(
                    km, "load_cpd_core", return_value=[(date(2013, 1, 31), 0.5)]
                ), unittest.mock.patch.object(
                    km, "load_ibk", return_value=[(date(2013, 1, 1), 1.0)]
                ):
                    with contextlib.redirect_stdout(io.StringIO()) as out:
                        code = km.run_summary(
                            rolling=False, today=date(2017, 1, 1), out_dir=Path(tmp)
                        )
            self.assertEqual(code, 0)
            self.assertFalse(stale.exists())
            text = out.getvalue()
            self.assertIn("ни один метод не посчитан", text)
            self.assertIn("не оценён", text)
            self.assertIn("устаревший файл", text)

    def test_forecast_json_is_identical_across_runs(self):
        # Детерминизм артефакта: два прогона на одной дате и данных дают один
        # и тот же JSON — отметок времени сверх даты в нём нет.
        with tempfile.TemporaryDirectory() as first, tempfile.TemporaryDirectory() as second:
            self.run_into(first, date(2017, 1, 1))
            self.run_into(second, date(2017, 1, 1))
            one = (Path(first) / km.FORECAST_NAME).read_bytes()
            two = (Path(second) / km.FORECAST_NAME).read_bytes()
        self.assertEqual(one, two)


def experiment_world(count: int = 48) -> dict:
    """Синтетический мир `run_summary`, дающий фолды ОБОИХ окон walk-forward.

    Закоммиченная фикстура (26 решений) даёт фолды только расширяющегося окна:
    скользящему 40 нужно минимум 41 заседание. Для журнала нужны оба окна
    (бриф Task 4), поэтому мир собирается из `ordinal_rows` — у её окон от 24
    заседаний не меньше двух классов, и ordered logit даёт распределения.

    `load_meetings`/`load_key_rate`/`load_cpd_core`/`load_ibk` подменяются
    целиком (это уже разобранные точки и даты, как их отдают загрузчики):
    ставка «на d−1» каждого заседания — уровень после предыдущего, а точка на
    `d+7` закрывает окно исхода последнего заседания; признаки сняты с месячных
    рядов, доступных по правилам §4.
    """
    rows = ordinal_rows(count)
    meetings = [row.decision.meeting_date for row in rows]
    key_rate = [(row.decision.meeting_date, row.decision.rate_after) for row in rows]
    # Ставка «до» первого заседания и наблюдаемое окно исхода последнего.
    key_rate.insert(
        0, (meetings[0] - timedelta(days=30), rows[0].decision.rate_before)
    )
    key_rate.append((meetings[-1] + timedelta(days=7), rows[-1].decision.rate_after))
    months = []
    cursor = date(2013, 1, 1)
    while cursor <= meetings[-1] + timedelta(days=62):
        months.append(cursor)
        year, month = cursor.year, cursor.month + 1
        if month > 12:
            year, month = year + 1, 1
        cursor = date(year, month, 1)
    cpd = [
        (date(month.year, month.month, 28), 0.4 + 0.1 * (index % 4))
        for index, month in enumerate(months)
    ]
    ibk = [(month, -2.0 + 0.5 * (index % 7)) for index, month in enumerate(months)]
    return {"meetings": meetings, "key_rate": key_rate, "cpd": cpd, "ibk": ibk}


class TestRunSummaryExperimentLog(unittest.TestCase):
    """Task 4: паспорт run'а, fold-предсказания, метрики и артефакты."""

    @contextlib.contextmanager
    def patched_world(self, world):
        """Подменяет загрузчики `run_summary` готовыми разобранными рядами."""
        loaders = {
            "load_meetings": world["meetings"],
            "load_mp_summary_deltas": [],
            "load_key_rate": world["key_rate"],
            "load_cpd_core": world["cpd"],
            "load_ibk": world["ibk"],
        }
        with contextlib.ExitStack() as stack:
            for name, value in loaders.items():
                stack.enter_context(
                    unittest.mock.patch.object(km, name, return_value=value)
                )
            yield stack

    def test_run_summary_logs_key_rate_run(self):
        world = experiment_world()
        fake = FakeLogger()
        with tempfile.TemporaryDirectory() as tmp:
            with self.patched_world(world), contextlib.redirect_stdout(io.StringIO()):
                code = km.run_summary(
                    rolling=False,
                    today=world["meetings"][-1] + timedelta(days=1),
                    out_dir=Path(tmp),
                    ml_log=True,
                    logger=fake,
                )
        self.assertEqual(code, 0)
        self.assertEqual(fake.finished, "finished")
        self.assertEqual({row.unit for row in fake.predictions}, {"bp"})
        # Оба окна walk-forward: expanding и rolling40 (у синтетики 48
        # заседаний скользящему окну 40 хватает длины).
        self.assertEqual(
            {row.window for row in fake.predictions}, {"expanding", "rolling40"}
        )
        self.assertEqual(fake.metrics["primary_metric"], "rmse_bp")
        # Спека §3.1/§7.2: top-level `models`, `windows` — СПИСОК окон с
        # числом фолдов и вложенными метриками методов; пропуски фолдов —
        # отдельным словарём «окно → метод → N».
        self.assertEqual(
            fake.metrics["models"], [method for method, _ in km.METHODS]
        )
        self.assertIsInstance(fake.metrics["windows"], list)
        windows = {item["window"] for item in fake.metrics["windows"]}
        self.assertEqual(windows, {"expanding", "rolling40"})
        for item in fake.metrics["windows"]:
            self.assertEqual(set(item), {"window", "folds", "models"})
            self.assertEqual(set(item["models"]), set(fake.metrics["models"]))
            self.assertEqual(
                item["folds"], max(entry["n"] for entry in item["models"].values())
            )
            for method, entry in item["models"].items():
                self.assertEqual(set(entry), {"rmse", "mae", "hit", "n"})
                self.assertEqual(
                    entry["n"],
                    sum(
                        1
                        for prediction in fake.predictions
                        if prediction.model == method
                        and prediction.window == item["window"]
                    ),
                )
        self.assertEqual(set(fake.metrics["fold_skips"]), windows)
        for window, per_method in fake.metrics["fold_skips"].items():
            self.assertEqual(set(per_method), set(fake.metrics["models"]))
            for skipped in per_method.values():
                self.assertIsInstance(skipped, int)
                self.assertGreaterEqual(skipped, 0)
        self.assertIn("rows", fake.metrics["sample"])
        self.assertEqual(
            {path.name for path in fake.artifacts},
            {km.REPORT_NAME, km.PREDICTIONS_NAME},
        )
        # У ordered_logit вероятности — тройка floats в порядке (−1, 0, +1).
        probabilities = [
            row.meta["probs"]
            for row in fake.predictions
            if isinstance(row.meta.get("probs"), list)
        ]
        self.assertTrue(
            any(
                len(triple) == 3 and all(isinstance(value, float) for value in triple)
                for triple in probabilities
            )
        )

    def test_logging_failure_does_not_change_the_exit_code(self):
        # Журнал — побочный слой: ValueError сборки PredictionRow не должен
        # превращать успешный прогон в non-zero exit. Отчёт и CSV уже
        # записаны; fake logger получает finish("failed").
        fake = FakeLogger()

        def broken_prediction_row(*args, **kwargs):
            raise ValueError("predicted должно быть конечным")

        with tempfile.TemporaryDirectory() as tmp:
            with (
                unittest.mock.patch.object(
                    km, "ch_query", side_effect=ch_query_from_fixtures
                ),
                unittest.mock.patch.object(
                    km.mlx, "PredictionRow", broken_prediction_row
                ),
                contextlib.redirect_stdout(io.StringIO()),
                self.assertLogs(km.log, level="WARNING") as captured,
            ):
                code = km.run_summary(
                    rolling=False,
                    today=date(2017, 1, 1),
                    out_dir=Path(tmp),
                    ml_log=True,
                    logger=fake,
                )
            self.assertEqual(code, 0)
            self.assertEqual(fake.finished, "failed")
            self.assertIn("logging: ValueError", fake.notes)
            self.assertEqual(fake.predictions, [])
            self.assertEqual(fake.artifacts, [])
            self.assertTrue(fake.metrics)  # метрики успели залогироваться до сбоя
            self.assertTrue((Path(tmp) / km.REPORT_NAME).is_file())
            self.assertTrue((Path(tmp) / km.PREDICTIONS_NAME).is_file())
        self.assertIn("журнал эксперимента не записан", "\n".join(captured.output))

    def test_run_summary_without_ml_log_does_not_call_ch_exec(self):
        # Явный ml_log=False важнее ML_LOG=1: журнал выключен, ch_exec не зовётся.
        with tempfile.TemporaryDirectory() as tmp:
            with (
                unittest.mock.patch.object(
                    km, "ch_query", side_effect=ch_query_from_fixtures
                ),
                unittest.mock.patch.dict(os.environ, {"ML_LOG": "1"}),
                unittest.mock.patch.object(km.mlx, "ch_exec") as ch_exec,
                contextlib.redirect_stdout(io.StringIO()),
            ):
                code = km.run_summary(
                    rolling=False,
                    today=date(2017, 1, 1),
                    out_dir=Path(tmp),
                    ml_log=False,
                )
        self.assertEqual(code, 0)
        ch_exec.assert_not_called()

    def test_run_summary_empty_logs_empty_status(self):
        fake = FakeLogger()
        with tempfile.TemporaryDirectory() as tmp:
            with (
                unittest.mock.patch.object(
                    km, "ch_query", side_effect=ch_query_without_meetings
                ),
                contextlib.redirect_stdout(io.StringIO()),
            ):
                code = km.run_summary(
                    rolling=False,
                    today=date(2017, 1, 1),
                    out_dir=Path(tmp),
                    ml_log=True,
                    logger=fake,
                )
        self.assertEqual(code, 1)
        self.assertEqual(fake.finished, "empty")
        self.assertEqual(fake.predictions, [])
        self.assertEqual(fake.metrics, {})
        self.assertEqual(fake.artifacts, [])


if __name__ == "__main__":
    unittest.main()
