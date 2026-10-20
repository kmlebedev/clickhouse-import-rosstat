#!/usr/bin/env python3
"""Тесты scripts/models/key_rate/key_rate_llm_forecast.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/models/key_rate/key_rate_llm_forecast_test.py -v

(`--python 3.12` — конвенция репозитория для uv-прогонов; здесь зависимостей
вне stdlib нет, поэтому тест идёт системным python3, как `harvest_test.py`.)

Набор ОФЛАЙН: `ch_query` и HTTP-вызов провайдера подменяются `unittest.mock`,
LLM/ML env-переменные удаляются на время набора. Живой прогон — отдельный
ручной шаг `make key-rate-llm-forecast`.
"""

import contextlib
import io
import json
import math
import os
import sys
import tempfile
import unittest
import unittest.mock
import urllib.error
from datetime import date
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.key_rate import key_rate_llm_forecast as klf

AS_OF = date(2026, 10, 18)
MEETING = date(2026, 10, 23)

# Переменные, которые читает скрипт/журнал эксперимента. Набор ОФЛАЙН:
# значения из окружения разработчика не должны включать ни реальный LLM-вызов,
# ни запись ML-журнала.
_ENV_VARS = (
    "LLM_BASE_URL",
    "LLM_API_KEY",
    "LLM_MODEL",
    "ML_LOG",
    "ML_RUN_NAME",
    "ML_LOCAL_DIR",
)
_SAVED_ENV = {}


def setUpModule():
    for name in _ENV_VARS:
        _SAVED_ENV[name] = os.environ.get(name)
        os.environ.pop(name, None)


def tearDownModule():
    for name, previous in _SAVED_ENV.items():
        if previous is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = previous


def point(series: str, day: str, value: float) -> dict:
    return {"series": series, "date": day, "value": value}


MEETINGS = [
    {"event_date": "2026-09-11", "category": "cbr", "title": klf.SAMPLE_TITLE},
    {"event_date": "2026-10-23", "category": "cbr", "title": klf.SAMPLE_TITLE},
    {"event_date": "2026-12-18", "category": "cbr", "title": klf.SAMPLE_TITLE},
    # Ручная строка-триггер сида: в выборку заседаний не входит.
    {"event_date": "2026-10-20", "category": "cbr", "title": "Проверить прогноз"},
]

CBR_ROWS = [
    point("cbr_key_rate", "2026-09-11", 14.0),
    point("cbr_key_rate", "2026-10-13", 14.0),
    point("cbr_ruania_ruo", "2026-10-14", 13.91),
    point("cbr_ruania_p25", "2026-10-14", 13.75),
    point("cbr_ruania_p75", "2026-10-14", 13.95),
    point("cbr_ruania_vol", "2026-10-14", 618.8),
    point("cbr_ruania_trans", "2026-10-14", 57.0),
    point("cbr_currency_usd", "2026-10-13", 83.915),
    # Точка августа 2026 доступна с 30.09.2026.
    point("Базовый ИПЦ", "2026-08-31", 0.49),
    point("наблюдаемая инфляция", "2026-08-14", 14.3),
    point("ожидаемая инфляция", "2026-08-14", 13.7),
    # Будущие/ещё не опубликованные точки: обязаны быть отброшены as-of.
    point("наблюдаемая инфляция", "2026-10-14", 99.0),
    point("ожидаемая инфляция", "2026-10-14", 98.0),
    point("cbr_key_rate", "2026-10-20", 99.0),
]

ROSSTAT_ROWS = [
    point(klf.ROSSTAT_CPI_SERIES["all"], "2026-08-31", 100.2),
    point(klf.ROSSTAT_CPI_SERIES["food"], "2026-08-31", 100.1),
    point(klf.ROSSTAT_CPI_SERIES["nonfood"], "2026-08-31", 100.3),
    point(klf.ROSSTAT_CPI_SERIES["services"], "2026-08-31", 100.4),
    # Сентябрьская точка опубликована в конце октября — на 18.10 недоступна.
    point(klf.ROSSTAT_CPI_SERIES["all"], "2026-09-30", 100.35),
    point(klf.ROSSTAT_CPI_SERIES["all"], "2026-10-31", 100.9),
]

SURVEY_ROWS = [
    point("price_expectations_sa", "2026-08-01", 22.65),
    point("price_growth_expected_sa", "2026-08-01", 6.98),
    point("business_climate_sa", "2026-08-01", -2.26),
    point("capacity_utilization_sa", "2026-07-01", 76.92),
    point("staff_availability_sa", "2026-07-01", -19.71),
    point("credit_conditions", "2026-08-01", -10.98),
    point("headcount_expectations_sa", "2026-07-01", 1.06),
]

CONSENSUS_ROWS = [
    point("key_rate_avg_2026_median", "2026-08-01", 15.0),
    point("key_rate_avg_2026_median", "2026-09-01", 14.5),
    point("key_rate_avg_2027_median", "2026-09-01", 12.43),
    point("cpi_dec_2026_median", "2026-09-01", 6.6),
    point("cpi_dec_2027_median", "2026-09-01", 4.56),
    # Волна после даты среза не должна попасть в снимок (публикация + 7 дней).
    point("key_rate_avg_2026_median", "2026-10-15", 13.0),
]

KEYRATE_ROWS = [
    {
        "date": "2026-10-12",
        "secid": "KKV6",
        "series_name": "KEYRATE-10.26",
        "decision_date": "2026-10-23",
        "settle": 13.95,
        "volume": 3,
        "openposition": 124,
        "expected_change_bp": -5.0,
    },
    {
        "date": "2026-10-12",
        "secid": "KKZ6",
        "series_name": "KEYRATE-12.26",
        "decision_date": "2026-12-18",
        "settle": 13.86,
        "volume": 0,
        "openposition": 30,
        "expected_change_bp": -14.0,
    },
    {
        "date": "2026-10-20",
        "secid": "KKV6",
        "series_name": "KEYRATE-10.26",
        "decision_date": "2026-10-23",
        "settle": 99.0,
        "volume": 1,
        "openposition": 1,
        "expected_change_bp": 8500.0,
    },
]

OFZ_ROWS = [
    {"date": "2026-10-09", "tenor": "1y", "yield": 13.7592},
    {"date": "2026-10-09", "tenor": "3y", "yield": 15.9688},
    {"date": "2026-10-09", "tenor": "5y", "yield": 16.568},
    {"date": "2026-10-09", "tenor": "10y", "yield": 16.8486},
    {"date": "2026-10-09", "tenor": "RGBI", "yield": 110.55},
    {"date": "2026-10-20", "tenor": "1y", "yield": 99.0},
]

SUMMARY_ROWS = [
    {
        "meeting_date": "2026-09-11",
        "published": "2026-09-23",
        "decision_action": "hold",
        "decision_rate": 14.0,
        "decision_delta_bp": 0,
        "signal": "Сигнал не сформулирован.",
    },
    {
        "meeting_date": "2026-10-23",
        "published": "2026-11-05",
        "decision_action": "cut",
        "decision_rate": 13.5,
        "decision_delta_bp": -50,
        "signal": "Будущее резюме не должно попасть в снимок.",
    },
]

CHANNEL_ROWS = [
    {
        "meeting_date": "2026-09-11",
        "channel": "current_inflation",
        "kind": "risk_pro",
        "seq": 1,
        "text": "Текущая инфляция остаётся повышенной.",
    },
    {
        "meeting_date": "2026-09-11",
        "channel": "budget_impulse",
        "kind": "prose",
        "seq": 2,
        "text": "Бюджетный импульс остаётся значимым.",
    },
    {
        "meeting_date": "2026-10-23",
        "channel": "current_inflation",
        "kind": "risk_pro",
        "seq": 3,
        "text": "Будущий канал.",
    },
]


def fake_ch_query(sql: str) -> list:
    """Транспорт-заглушка: SQL → строки соответствующей фикстуры."""
    if "v_events_calendar" in sql:
        return MEETINGS
    if "v_cbr_macro" in sql:
        return CBR_ROWS
    if "v_rosstat_macro" in sql:
        return ROSSTAT_ROWS
    if "v_cbr_survey" in sql:
        return SURVEY_ROWS
    if "v_cbr_mo_br" in sql:
        return CONSENSUS_ROWS
    if "v_keyrate_expectations" in sql:
        return KEYRATE_ROWS
    if "v_ofz_curve" in sql:
        return OFZ_ROWS
    if "v_cbr_mp_summary_channels" in sql:
        return CHANNEL_ROWS
    if "v_cbr_mp_summary" in sql:
        return SUMMARY_ROWS
    raise AssertionError(f"неожиданный SQL: {sql}")


@contextlib.contextmanager
def patched_snapshot(world=None):
    """Подменяет ch_query фикстурным транспортом (или словарём world)."""
    if world is None:
        world = fake_ch_query
    with unittest.mock.patch.object(klf, "ch_query", world):
        yield


def make_snapshot() -> klf.Snapshot:
    with patched_snapshot():
        return klf.load_snapshot(AS_OF)


def make_config() -> klf.LLMConfig:
    return klf.LLMConfig(
        base_url="https://llm.example/v1",
        api_key="test-secret-key",
        model="test-model",
    )


def answer(
    cut: float,
    hold: float,
    hike: float,
    step: float,
    rationale: str = "Обоснование.",
    risks=None,
) -> str:
    payload = {
        "probabilities": {"cut": cut, "hold": hold, "hike": hike},
        "expected_step_bp": step,
        "rationale": rationale,
        "risks": ["Риск."] if risks is None else risks,
    }
    return json.dumps(payload, ensure_ascii=False)


def fake_ask(responses):
    """ask-заглушка: выдаёт ответы по порядку и считает вызовы."""
    calls = []

    def ask(config, system, user):
        index = min(len(calls), len(responses) - 1)
        content = responses[index]
        calls.append(
            {"config": config, "system": system, "user": user, "content": content}
        )
        if isinstance(content, Exception):
            raise content
        return klf.LLMCall(content=content, json_mode=True)

    return ask, calls


class TestAsOfRules(unittest.TestCase):
    def test_monthly_available_at_end_of_next_month(self):
        self.assertEqual(
            klf.available_from_monthly(date(2026, 8, 31)), date(2026, 9, 30)
        )
        self.assertEqual(
            klf.available_from_monthly(date(2026, 12, 31)), date(2027, 1, 31)
        )

    def test_market_and_consensus_rules(self):
        self.assertEqual(
            klf.available_from_next_day(date(2026, 10, 9)), date(2026, 10, 10)
        )
        self.assertEqual(
            klf.available_from_consensus(date(2026, 9, 1)), date(2026, 9, 8)
        )


class TestNormalizeProbabilities(unittest.TestCase):
    def test_percent_input_kept(self):
        result = klf.normalize_probabilities({"cut": 60, "hold": 30, "hike": 10})
        self.assertEqual(result, {"cut": 60.0, "hold": 30.0, "hike": 10.0})
        self.assertAlmostEqual(sum(result.values()), 100.0)

    def test_fraction_input_scaled(self):
        result = klf.normalize_probabilities({"cut": 0.6, "hold": 0.3, "hike": 0.1})
        self.assertEqual(result, {"cut": 60.0, "hold": 30.0, "hike": 10.0})

    def test_arbitrary_scale_normalized(self):
        result = klf.normalize_probabilities({"cut": 3, "hold": 5, "hike": 2})
        self.assertEqual(result, {"cut": 30.0, "hold": 50.0, "hike": 20.0})

    def test_rounding_preserves_exactly_100(self):
        result = klf.normalize_probabilities(
            {"cut": 33.333, "hold": 33.333, "hike": 33.334}
        )
        self.assertEqual(round(sum(result.values()), 2), 100.0)
        self.assertEqual(result["cut"] + result["hold"] + result["hike"], 100.0)

    def test_negative_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.normalize_probabilities({"cut": -1, "hold": 50, "hike": 51})

    def test_nan_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.normalize_probabilities(
                {"cut": float("nan"), "hold": 50, "hike": 50}
            )

    def test_zero_sum_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.normalize_probabilities({"cut": 0, "hold": 0, "hike": 0})

    def test_missing_key_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.normalize_probabilities({"cut": 50, "hold": 50})

    def test_extra_key_ignored(self):
        result = klf.normalize_probabilities(
            {"cut": 50, "hold": 50, "hike": 0, "unknown": 999}
        )
        self.assertEqual(result, {"cut": 50.0, "hold": 50.0, "hike": 0.0})


class TestParseAnswer(unittest.TestCase):
    def test_valid_with_markdown_fence(self):
        content = "```json\n" + answer(60, 30, 10, -12.5) + "\n```"
        parsed = klf.parse_answer(content)
        self.assertEqual(parsed.probabilities, {"cut": 60.0, "hold": 30.0, "hike": 10.0})
        self.assertEqual(parsed.expected_step_bp, -12.5)
        self.assertEqual(parsed.rationale, "Обоснование.")
        self.assertEqual(parsed.risks, ["Риск."])

    def test_missing_probabilities_rejected(self):
        content = json.dumps(
            {
                "expected_step_bp": 0,
                "rationale": "x",
                "risks": ["y"],
            }
        )
        with self.assertRaises(klf.LLMFormatError):
            klf.parse_answer(content)

    def test_step_out_of_range_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.parse_answer(answer(50, 50, 0, 10000))

    def test_empty_rationale_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.parse_answer(answer(50, 50, 0, 0, rationale="  "))

    def test_empty_risks_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.parse_answer(answer(50, 50, 0, 0, risks=[]))

    def test_risks_string_becomes_list(self):
        parsed = klf.parse_answer(answer(50, 50, 0, 0, risks="Один риск"))
        self.assertEqual(parsed.risks, ["Один риск"])

    def test_non_json_rejected(self):
        with self.assertRaises(klf.LLMFormatError):
            klf.parse_answer("модель вернула текст без JSON")


class TestSnapshot(unittest.TestCase):
    def test_snapshot_uses_only_available_data(self):
        snapshot = make_snapshot()
        payload = klf.snapshot_payload(snapshot)
        self.assertEqual(snapshot.meeting_date, MEETING)
        self.assertEqual(snapshot.current_rate, 14.0)
        self.assertEqual(payload["current_rate"]["date"], "2026-10-13")
        self.assertEqual(payload["inflation"]["cbr"]["observed_yoy"]["value"], 14.3)
        self.assertEqual(payload["inflation"]["cbr"]["expected_yoy"]["value"], 13.7)
        # Сентябрьский ИПЦ Росстата ещё не опубликован, точка августа — 0.2 %.
        self.assertEqual(payload["inflation"]["rosstat"]["all"]["mom_pct"], 0.2)
        self.assertEqual(
            payload["inflation"]["rosstat"]["all"]["date"], "2026-08-31"
        )
        self.assertEqual(payload["market"]["keyrate"]["settle"], 13.95)
        self.assertEqual(payload["market"]["keyrate"]["decision_date"], "2026-10-23")
        self.assertTrue(payload["market"]["keyrate"]["exact_match"])
        self.assertEqual(payload["market"]["ofz"]["1y"]["date"], "2026-10-09")
        self.assertEqual(payload["consensus"]["key_rate_avg_2026_median"]["value"], 14.5)
        self.assertEqual(payload["mp_summary"]["meeting_date"], "2026-09-11")
        self.assertEqual(payload["mp_summary"]["published"], "2026-09-23")
        self.assertEqual(len(payload["mp_summary"]["channels"]), 2)

    def test_no_future_meeting_raises(self):
        world_rows = [row for row in MEETINGS if row["event_date"] <= "2026-10-18"]

        def world(sql):
            if "v_events_calendar" in sql:
                return world_rows
            return fake_ch_query(sql)

        with patched_snapshot(world):
            with self.assertRaises(klf.SnapshotError):
                klf.load_snapshot(AS_OF)

    def test_no_rate_raises(self):
        rows = [row for row in CBR_ROWS if row["series"] != "cbr_key_rate"]

        def world(sql):
            if "v_cbr_macro" in sql:
                return rows
            return fake_ch_query(sql)

        with patched_snapshot(world):
            with self.assertRaises(klf.SnapshotError):
                klf.load_snapshot(AS_OF)

    def test_data_version_stable_and_sensitive(self):
        snapshot = make_snapshot()
        payload = klf.snapshot_payload(snapshot)
        first = klf.data_version(payload)
        second = klf.data_version(json.loads(json.dumps(payload)))
        self.assertEqual(first, second)
        self.assertRegex(first, r"^[0-9a-f]{12}$")
        changed = json.loads(json.dumps(payload))
        changed["current_rate"]["value"] = 14.25
        self.assertNotEqual(first, klf.data_version(changed))

    def test_summary_only_published_before_as_of(self):
        snapshot = make_snapshot()
        summary = klf.snapshot_payload(snapshot)["mp_summary"]
        self.assertEqual(summary["decision_action"], "hold")
        self.assertNotIn("Будущее", summary["signal"])

    def test_keyrate_falls_back_to_next_series(self):
        rows = [row for row in KEYRATE_ROWS if row["decision_date"] != "2026-10-23"]

        def world(sql):
            if "v_keyrate_expectations" in sql:
                return rows
            return fake_ch_query(sql)

        with patched_snapshot(world):
            snapshot = klf.load_snapshot(AS_OF)
        keyrate = klf.snapshot_payload(snapshot)["market"]["keyrate"]
        self.assertFalse(keyrate["exact_match"])
        self.assertEqual(keyrate["decision_date"], "2026-12-18")
        self.assertTrue(
            any("точной серии KEYRATE" in warning for warning in snapshot.warnings)
        )

    def test_ofz_future_point_dropped(self):
        snapshot = make_snapshot()
        ofz = klf.snapshot_payload(snapshot)["market"]["ofz"]
        self.assertEqual(ofz["1y"]["value"], 13.7592)
        self.assertEqual(ofz["1y"]["date"], "2026-10-09")


class TestLLMConfig(unittest.TestCase):
    def test_missing_env_lists_names(self):
        with unittest.mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(klf.MissingEnvError) as raised:
                klf.llm_config_from_env()
        message = str(raised.exception)
        for name in ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL"):
            self.assertIn(name, message)
        self.assertNotIn("secret", message)

    def test_config_from_env(self):
        env = {
            "LLM_BASE_URL": "https://llm.example",
            "LLM_API_KEY": "secret-key",
            "LLM_MODEL": "model-x",
        }
        with unittest.mock.patch.dict(os.environ, env, clear=True):
            config = klf.llm_config_from_env()
        self.assertEqual(config.base_url, "https://llm.example")
        self.assertEqual(config.api_key, "secret-key")
        self.assertEqual(config.model, "model-x")

    def test_repr_hides_api_key(self):
        config = make_config()
        self.assertNotIn("test-secret-key", repr(config))
        self.assertIn("test-model", repr(config))

    def test_llm_url_variants(self):
        self.assertEqual(
            klf.llm_url("https://llm.example"),
            "https://llm.example/v1/chat/completions",
        )
        self.assertEqual(
            klf.llm_url("https://llm.example/v1"),
            "https://llm.example/v1/chat/completions",
        )
        self.assertEqual(
            klf.llm_url("https://llm.example/anthropic"),
            "https://llm.example/v1/chat/completions",
        )


class TestForecast(unittest.TestCase):
    def test_three_repeats_median_and_spread(self):
        snapshot = make_snapshot()
        ask, calls = fake_ask(
            [
                answer(60, 30, 10, -10, rationale="Первый."),
                answer(50, 40, 10, -20, rationale="Второй."),
                answer(70, 20, 10, 0, rationale="Третий."),
            ]
        )
        result = klf.forecast(snapshot, make_config(), ask=ask)
        self.assertEqual(len(calls), 3)
        self.assertEqual(len(result.samples), 3)
        self.assertEqual(result.probabilities, {"cut": 60.0, "hold": 30.0, "hike": 10.0})
        self.assertEqual(result.expected_step_bp, -10.0)
        self.assertEqual(result.expected_rate, 13.9)
        self.assertEqual(result.spread["probability_spread_pp"]["cut"], 20.0)
        self.assertEqual(result.spread["probability_spread_pp"]["hold"], 20.0)
        self.assertEqual(result.spread["probability_spread_pp"]["hike"], 0.0)
        self.assertEqual(result.spread["expected_step_spread_bp"], 20.0)
        # Первый повтор ровно совпал с медианой — его rationale представителен.
        self.assertEqual(result.rationale, "Первый.")
        self.assertEqual(result.model, "test-model")
        self.assertEqual(result.prompt_version, 1)
        self.assertEqual(result.data_version, klf.data_version(snapshot.payload))

    def test_format_error_retried_once(self):
        snapshot = make_snapshot()
        ask, calls = fake_ask(
            [
                "не JSON",
                answer(60, 30, 10, -10, rationale="Первый."),
                answer(60, 30, 10, -10, rationale="Второй."),
                answer(60, 30, 10, -10, rationale="Третий."),
            ]
        )
        result = klf.forecast(snapshot, make_config(), ask=ask)
        self.assertEqual(len(calls), 4)
        self.assertEqual(result.samples[0].attempts, 2)
        self.assertEqual(len(result.samples), 3)

    def test_two_invalid_samples_fail(self):
        snapshot = make_snapshot()
        ask, calls = fake_ask(
            [
                "не JSON",
                "не JSON",
                "не JSON",
                "не JSON",
                answer(60, 30, 10, -10),
            ]
        )
        with self.assertRaises(klf.ForecastError):
            klf.forecast(snapshot, make_config(), ask=ask)
        self.assertEqual(len(calls), 5)

    def test_provider_error_aborts_without_retry(self):
        snapshot = make_snapshot()
        ask, calls = fake_ask([klf.LLMError("http status 500: upstream error")])
        with self.assertRaises(klf.LLMError):
            klf.forecast(snapshot, make_config(), ask=ask)
        self.assertEqual(len(calls), 1)

    def test_two_valid_samples_median(self):
        snapshot = make_snapshot()
        ask, calls = fake_ask(
            [
                answer(40, 40, 20, -10),
                "не JSON",
                "не JSON",
                answer(60, 20, 20, -20),
            ]
        )
        result = klf.forecast(snapshot, make_config(), ask=ask)
        self.assertEqual(len(calls), 4)
        self.assertEqual(len(result.samples), 2)
        self.assertEqual(result.probabilities, {"cut": 50.0, "hold": 30.0, "hike": 20.0})
        self.assertEqual(result.expected_step_bp, -15.0)

    def test_prompt_contains_required_blocks(self):
        snapshot = make_snapshot()
        system = klf.system_prompt()
        user = klf.user_prompt(snapshot)
        self.assertIn("probabilities", system)
        self.assertIn("expected_step_bp", system)
        self.assertIn('"data_asof"', user)
        self.assertIn('"meeting_date"', user)
        self.assertIn('"keyrate"', user)
        self.assertIn('"ofz"', user)
        self.assertIn('"ruonia"', user)
        self.assertIn('"consensus"', user)
        self.assertIn('"mp_summary"', user)
        self.assertIn("2026-10-23", user)


class TestArtifacts(unittest.TestCase):
    def setUp(self):
        self.snapshot = make_snapshot()
        ask, _ = fake_ask(
            [
                answer(60, 30, 10, -10, rationale="Первый."),
                answer(50, 40, 10, -20, rationale="Второй."),
                answer(70, 20, 10, 0, rationale="Третий."),
            ]
        )
        self.config = make_config()
        self.forecast = klf.forecast(self.snapshot, self.config, ask=ask)

    def test_forecast_json_deterministic_and_sum_100(self):
        first = klf.forecast_json(self.forecast)
        second = klf.forecast_json(self.forecast)
        self.assertEqual(first, second)
        payload = json.loads(first)
        self.assertEqual(round(sum(payload["probabilities"].values()), 2), 100.0)
        self.assertEqual(payload["model"], "test-model")
        self.assertEqual(payload["prompt_version"], 1)
        self.assertRegex(payload["data_version"], r"^[0-9a-f]{12}$")
        self.assertEqual(payload["spread"]["expected_step_spread_bp"], 20.0)
        self.assertNotIn("generated_at", payload)

    def test_report_contains_forecast_and_inputs(self):
        report = klf.render_report(self.forecast, self.snapshot)
        for marker in (
            "LLM-прогноз ключевой ставки",
            "cut",
            "hold",
            "hike",
            "Ожидаемый шаг",
            "KEYRATE",
            "ОФЗ",
            "RUONIA",
            "Консенсус",
            "не является инвестиционной рекомендацией",
        ):
            self.assertIn(marker, report)
        self.assertIn("2026-10-23", report)

    def test_log_payload_has_todo_and_metric(self):
        payload = klf.log_payload(self.forecast)
        self.assertEqual(payload["status"], "pending_ingest_127")
        self.assertIn("#127", payload["todo"])
        row = payload["row"]
        self.assertEqual(row["metric"], "cbr_rate")
        self.assertEqual(row["target_date"], "2026-10-23")
        self.assertEqual(row["forecast_date"], "2026-10-18")
        self.assertEqual(row["predicted"], 13.9)
        self.assertEqual(row["error_bp"], None)
        self.assertEqual(round(sum(row["probabilities"].values()), 2), 100.0)
        self.assertEqual(row["model"], "test-model")
        self.assertEqual(row["prompt_version"], 1)

    def test_run_writes_artifacts_and_log_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.object(
                klf, "load_snapshot", return_value=self.snapshot
            ), unittest.mock.patch.object(
                klf, "llm_config_from_env", return_value=self.config
            ), unittest.mock.patch.object(
                klf, "forecast", return_value=self.forecast
            ), contextlib.redirect_stdout(io.StringIO()) as out:
                code = klf.run(as_of=AS_OF, out_dir=Path(tmp), log_mode=True)
            self.assertEqual(code, 0)
            for name in klf.ARTIFACT_NAMES:
                self.assertTrue((Path(tmp) / name).exists(), name)
            payload = json.loads(
                (Path(tmp) / klf.LOG_PAYLOAD_NAME).read_text(encoding="utf-8")
            )
            self.assertEqual(payload["row"]["metric"], "cbr_rate")
            self.assertIn("payload для #127", out.getvalue())
            self.assertIn("TODO(#127)", out.getvalue())

    def test_run_without_log_removes_stale_payload(self):
        with tempfile.TemporaryDirectory() as tmp:
            stale = Path(tmp) / klf.LOG_PAYLOAD_NAME
            stale.write_text("{}", encoding="utf-8")
            with unittest.mock.patch.object(
                klf, "load_snapshot", return_value=self.snapshot
            ), unittest.mock.patch.object(
                klf, "llm_config_from_env", return_value=self.config
            ), unittest.mock.patch.object(
                klf, "forecast", return_value=self.forecast
            ), contextlib.redirect_stdout(io.StringIO()):
                code = klf.run(as_of=AS_OF, out_dir=Path(tmp), log_mode=False)
            self.assertEqual(code, 0)
            self.assertFalse(stale.exists())
            self.assertTrue((Path(tmp) / klf.FORECAST_JSON_NAME).exists())

    def test_run_failure_clears_stale_artifacts(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in klf.ARTIFACT_NAMES:
                (Path(tmp) / name).write_text("stale", encoding="utf-8")
            with unittest.mock.patch.object(
                klf, "load_snapshot", side_effect=klf.SnapshotError("нет данных")
            ), unittest.mock.patch.object(
                klf, "llm_config_from_env", return_value=self.config
            ), contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(
                io.StringIO()
            ):
                code = klf.run(as_of=AS_OF, out_dir=Path(tmp), log_mode=True)
            self.assertEqual(code, 1)
            for name in klf.ARTIFACT_NAMES:
                self.assertFalse((Path(tmp) / name).exists(), name)

    def test_missing_env_checked_before_clickhouse(self):
        with tempfile.TemporaryDirectory() as tmp:
            with unittest.mock.patch.dict(os.environ, {}, clear=True), \
                    unittest.mock.patch.object(
                        klf, "ch_query", side_effect=AssertionError("ClickHouse вызван")
                    ), contextlib.redirect_stderr(io.StringIO()):
                code = klf.run(as_of=AS_OF, out_dir=Path(tmp))
            self.assertEqual(code, 1)


class FakeResponse:
    def __init__(self, body: bytes):
        self._body = body

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return self._body


class TestHTTPTransport(unittest.TestCase):
    def test_json_mode_fallback_on_unsupported(self):
        error = urllib.error.HTTPError(
            "https://llm.example/v1/chat/completions",
            400,
            "Bad Request",
            {},
            io.BytesIO(b'{"error":"response_format unsupported"}'),
        )
        envelope = json.dumps(
            {"choices": [{"message": {"content": answer(50, 50, 0, 0)}}]}
        ).encode("utf-8")
        with unittest.mock.patch(
            "urllib.request.urlopen", side_effect=[error, FakeResponse(envelope)]
        ) as urlopen:
            call = klf.ask_once(make_config(), "system", "user")
        self.assertFalse(call.json_mode)
        self.assertEqual(urlopen.call_count, 2)
        first_body = json.loads(urlopen.call_args_list[0][0][0].data.decode("utf-8"))
        second_body = json.loads(urlopen.call_args_list[1][0][0].data.decode("utf-8"))
        self.assertEqual(first_body["response_format"], {"type": "json_object"})
        self.assertNotIn("response_format", second_body)
        self.assertEqual(first_body["temperature"], 0)
        self.assertEqual(first_body["messages"][0]["role"], "system")

    def test_http_error_without_fallback_raises_llm_error(self):
        error = urllib.error.HTTPError(
            "https://llm.example/v1/chat/completions",
            500,
            "Internal Server Error",
            {},
            io.BytesIO(b"upstream failed"),
        )
        with unittest.mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(klf.LLMError) as raised:
                klf.ask_once(make_config(), "system", "user")
        self.assertIn("http status 500", str(raised.exception))

    def test_empty_choices_is_format_error(self):
        envelope = json.dumps({"choices": []}).encode("utf-8")
        with unittest.mock.patch(
            "urllib.request.urlopen", return_value=FakeResponse(envelope)
        ):
            with self.assertRaises(klf.LLMFormatError):
                klf.ask_once(make_config(), "system", "user")

    def test_reasoning_content_fallback(self):
        envelope = json.dumps(
            {
                "choices": [
                    {
                        "message": {
                            "content": "",
                            "reasoning_content": answer(50, 50, 0, 0),
                        },
                        "finish_reason": "stop",
                    }
                ]
            }
        ).encode("utf-8")
        with unittest.mock.patch(
            "urllib.request.urlopen", return_value=FakeResponse(envelope)
        ):
            call = klf.ask_once(make_config(), "system", "user")
        self.assertEqual(call.content, answer(50, 50, 0, 0))


if __name__ == "__main__":
    unittest.main(verbosity=2)
