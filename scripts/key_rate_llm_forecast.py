#!/usr/bin/env python3
"""Экспериментальный LLM-прогноз ключевой ставки (пункт `key-rate/phase-4-llm-track`).

Скрипт — третий, независимый трек контура ставки. Он **не заменяет**
статистический (`key_rate_model.py`) и ML-контур (`cpi_nowcast.py`), а даёт
cross-check: LLM получает срез данных на дату и разметку каналов резюме ЦБ и
возвращает распределение `{cut, hold, hike}` и ожидаемый шаг в базисных пунктах.

Что делает прогон:

* **снимок as-of** — данные читаются через тот же транспорт `ch_query`, что у
  `key_rate_model.py` (HTTP-порт 8123, маскирование секретов), и фильтруются
  правилами доступности: точка участвует, только если её дата публикации не
  позже даты среза. Будущие данные в промпт не попадают;
* **промпт редакции 1** — ставка, ИПЦ (месячный м/м Росстата, базовый ИПЦ ЦБ,
  наблюдаемая и ожидаемая инфляция), ценовые ожидания и мониторинг предприятий,
  рыночный контур (KEYRATE, ОФЗ, RUONIA, USD), консенсус макроопроса, последнее
  опубликованное резюме и его regex-каналы;
* **OpenAI-совместимый вызов** — `LLM_BASE_URL`/`LLM_API_KEY`/`LLM_MODEL`,
  `temperature=0`, `response_format=json_object` с фолбэком на провайдера без
  JSON-режима; три независимых повтора, медиана и разброс;
* **валидация** — вероятности неотрицательны и нормируются к 100 %, шаг в
  допустимом диапазоне, `rationale`/`risks` непустые;
* **артефакты** — `key_rate_llm_forecast.json` и
  `key_rate_llm_forecast_report.md` в корне репозитория (gitignored);
* **`--log`** — только payload для ingest #127:
  `key_rate_llm_forecast_log_payload.json`. Прямой записи в `forecast_log` нет:
  endpoint #127 ещё не реализован, и второй путь записи не изобретается. Явный
  TODO живёт в payload и в `log_payload`.

**Ограничение:** as-of — консервативное приближение, а не point-in-time слой;
ревизии рядов и точные даты публикаций не учтены (как и у `key_rate_model.py`).

Запуск — только через `make` (переменные окружения подключает Makefile):

* `make key-rate-llm-forecast` — полный прогон и артефакты;
* `make key-rate-llm-forecast ARGS="--as-of 2026-10-18"` — срез на дату;
* `make key-rate-llm-forecast ARGS="--log"` — дополнительно payload для #127.

Секреты приходят только из окружения; в логах и ошибках — только имена
переменных.
"""

import argparse
import hashlib
import json
import logging
import math
import os
import statistics
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent

log = logging.getLogger(__name__)

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его
# (прецедент `key_rate_model.py`).
sys.path.insert(0, str(ROOT / "scripts"))

import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# --- Версии, имена артефактов и константы -------------------------------------

# Редакция промпта. Изменение промпта — новая редакция (и новая строка журнала
# #127), а не правка старой: иначе прогнозы разных редакций станут
# несопоставимы.
PROMPT_VERSION = 1

# Три независимых повтора при temperature=0: медиана и разброс — требование
# issue #189. Меньше двух валидных повторов — прогноз не строится.
REPEATS = 3
MIN_VALID_SAMPLES = 2

# Две попытки на один повтор: ошибка формата лечится вторым запросом, отказ
# провайдера — нет (см. `forecast`).
MAX_FORMAT_ATTEMPTS = 2

# Предел ожидаемого шага. Не «прогнозный коридор», а защита от выброса модели:
# ставку на 1000 б.п. за одно заседание ЦБ не менял даже в 2022 году.
MIN_STEP_BP = -1000.0
MAX_STEP_BP = 1000.0

# Предел числа рисков в ответе: длинный список не нужен, а промпт и отчёт
# должны оставаться читаемыми.
MAX_RISKS = 10

# Предел фрагментов каналов резюме в снимке: regex-слой может дать десятки
# строк, но промпту нужна карта, а не полный текст резюме.
MAX_CHANNEL_FRAGMENTS = 30

# Дедлайн одного запроса к провайдеру. Переменная, а не константа: тест
# подставляет короткий дедлайн вместо минутного.
LLM_TIMEOUT_SECONDS = 180

# Предел вывода модели: JSON с распределением и коротким обоснованием.
# 8192, а не 2048: reasoning-модели (например, deepseek-flash) тратят часть
# бюджета на reasoning_content, и при малом пределе content приходит пустым.
LLM_MAX_TOKENS = 8192

# Константа названия артефактов. Имена точные: фикстуры тестов лежат рядом и
# обязаны коммититься, поэтому широкие маски в .gitignore недопустимы.
FORECAST_JSON_NAME = "key_rate_llm_forecast.json"
REPORT_NAME = "key_rate_llm_forecast_report.md"
LOG_PAYLOAD_NAME = "key_rate_llm_forecast_log_payload.json"
ARTIFACT_NAMES = (FORECAST_JSON_NAME, REPORT_NAME, LOG_PAYLOAD_NAME)

# Канонический title заседания в `events_calendar`: ручные строки-триггеры сида
# лежат в той же категории `cbr` с другими title и отсекаются этим фильтром
# (правило то же, что у `key_rate_model.py`).
SAMPLE_TITLE = "СД ЦБ РФ: заседание по ключевой ставке"

# Классы решения и их порядок во всех артефактах.
CLASSES = ("cut", "hold", "hike")

# Названия рядов. Литералы вынесены сюда: один источник правды для SQL,
# разбора и тестов.
KEY_RATE_SERIES = "cbr_key_rate"
RUONIA_SERIES = "cbr_ruania_ruo"
RUONIA_P25_SERIES = "cbr_ruania_p25"
RUONIA_P75_SERIES = "cbr_ruania_p75"
RUONIA_VOL_SERIES = "cbr_ruania_vol"
RUONIA_TRANS_SERIES = "cbr_ruania_trans"
USD_SERIES = "cbr_currency_usd"
CORE_CPI_SERIES = "Базовый ИПЦ"
OBSERVED_INFLATION_SERIES = "наблюдаемая инфляция"
EXPECTED_INFLATION_SERIES = "ожидаемая инфляция"

ROSSTAT_CPI_SERIES = {
    "all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
    "food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
    "nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
    "services": "Индексы потребительских цен на услуги по Российской Федерации",
}

SURVEY_SERIES = (
    "price_expectations_sa",
    "price_growth_expected_sa",
    "business_climate_sa",
    "capacity_utilization_sa",
    "staff_availability_sa",
    "credit_conditions",
    "headcount_expectations_sa",
)

CONSENSUS_SERIES = (
    "key_rate_avg_2026_median",
    "key_rate_avg_2027_median",
    "cpi_dec_2026_median",
    "cpi_dec_2027_median",
)

OFZ_TENORS = ("1y", "3y", "5y", "10y", "RGBI")

NOTE = (
    "Экспериментальный LLM-трек: не заменяет статистический и рыночный "
    "контуры, не является инвестиционной рекомендацией. Запись в forecast_log "
    "ждёт issue #127; прогноз условен на данные даты среза."
)

LOG_TODO = (
    "TODO(#127): ingest endpoint для forecast_log ещё не реализован; этот "
    "payload — единственный подготовленный выход. Прямой записи в ClickHouse "
    "нет, второй путь записи не создаётся."
)


# --- Ошибки -------------------------------------------------------------------


class ForecastError(Exception):
    """Ожидаемая ошибка прогона: нет данных, нет env, невалидный ответ."""


class MissingEnvError(ForecastError):
    """Не заданы переменные окружения LLM; сообщение содержит только имена."""


class SnapshotError(ForecastError):
    """Снимок не собран: нет заседания, ставки или обязательного ряда."""


class LLMError(ForecastError):
    """Отказ провайдера или сбой транспорта; формат здесь не повторяется."""


class LLMFormatError(ForecastError):
    """Ответ не разобран или не прошёл схему; повторяется один раз."""


# --- Общие хелперы разбора ----------------------------------------------------


def row_date(value) -> date:
    """Дата из JSON-ответа `ch_query` (строка `YYYY-MM-DD`) или `date`."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def row_float(value, what: str = "значение") -> float:
    """Число из JSON-ответа; `None`, пустая строка и NaN — ошибка снимка."""
    if value is None or value == "":
        raise SnapshotError(f"пустое значение: {what}")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SnapshotError(f"не число: {what}: {value!r}") from exc
    if not math.isfinite(number):
        raise SnapshotError(f"не конечное число: {what}: {value!r}")
    return number


def row_optional_float(value) -> Optional[float]:
    """Необязательное число: пусто/None → None, нечисло — ошибка."""
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise SnapshotError(f"не число: {value!r}") from exc
    if not math.isfinite(number):
        return None
    return number


def _round(value: Optional[float], ndigits: int = 4) -> Optional[float]:
    """Округление для промпта и артефактов; `None` остаётся `None`."""
    if value is None:
        return None
    return round(float(value), ndigits)


def _sql_in(values) -> str:
    """Список SQL-литералов для `IN (...)`; значения — константы кода."""
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)


# --- As-of правила ------------------------------------------------------------


ONE_DAY = timedelta(days=1)


def _month_shift(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def available_from_daily(day: date) -> date:
    """Дневной ряд доступен в день точки (ставка, курс)."""
    return day


def available_from_next_day(day: date) -> date:
    """Рыночный ряд доступен на следующий день (RUONIA, ОФЗ, KEYRATE)."""
    return day + ONE_DAY


def available_from_monthly(period_end: date) -> date:
    """Месячная точка отчётного месяца M доступна в конце M+1.

    Консервативное правило `key_rate_model.py`: фактическая публикация —
    вторая декада M+1, но для честного as-of точка M не должна попадать в
    срез, если она ещё не опубликована.
    """
    return _month_shift(period_end, 2) - ONE_DAY


def available_from_consensus(wave: date) -> date:
    """Волна макроопроса публикуется через 2–6 дней; берём запас 7 дней."""
    return wave + 7 * ONE_DAY


def latest_available(
    rows: list,
    as_of: date,
    available: Callable[[date], date],
    date_field: str = "date",
) -> Optional[dict]:
    """Строка с наибольшей датой среди доступных на `as_of` (граница включительна)."""
    candidates = [
        row
        for row in rows
        if available(row_date(row[date_field])) <= as_of
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda row: row_date(row[date_field]))


def latest_series_point(
    rows: list,
    series: str,
    as_of: date,
    available: Callable[[date], date],
) -> Optional[dict]:
    """Последняя доступная точка ряда `series` или None."""
    return latest_available(
        [row for row in rows if row.get("series") == series],
        as_of,
        available,
    )


def point_payload(row: Optional[dict]) -> Optional[dict]:
    """Точка ряда в промпте: дата и округлённое значение."""
    if row is None:
        return None
    return {
        "date": row_date(row["date"]).isoformat(),
        "value": _round(row_float(row["value"])),
    }


# --- SQL и загрузчики ---------------------------------------------------------


MEETINGS_SQL = f"""
SELECT event_date, category, title
FROM v_events_calendar
WHERE category = 'cbr'
ORDER BY event_date
FORMAT JSON
"""

CBR_MACRO_SQL = f"""
SELECT series, date, value
FROM v_cbr_macro
WHERE series IN ({_sql_in((
    KEY_RATE_SERIES,
    RUONIA_SERIES,
    RUONIA_P25_SERIES,
    RUONIA_P75_SERIES,
    RUONIA_VOL_SERIES,
    RUONIA_TRANS_SERIES,
    USD_SERIES,
    CORE_CPI_SERIES,
    OBSERVED_INFLATION_SERIES,
    EXPECTED_INFLATION_SERIES,
))})
ORDER BY series, date
FORMAT JSON
"""

ROSSTAT_CPI_SQL = f"""
SELECT series, date, value
FROM v_rosstat_macro
WHERE series IN ({_sql_in(ROSSTAT_CPI_SERIES.values())})
ORDER BY series, date
FORMAT JSON
"""

SURVEY_SQL = f"""
SELECT series, date, value
FROM v_cbr_survey
WHERE series IN ({_sql_in(SURVEY_SERIES)})
ORDER BY series, date
FORMAT JSON
"""

CONSENSUS_SQL = f"""
SELECT series, date, value
FROM v_cbr_mo_br
WHERE series IN ({_sql_in(CONSENSUS_SERIES)})
ORDER BY series, date
FORMAT JSON
"""

KEYRATE_SQL = """
SELECT date, secid, series_name, decision_date, settle, volume,
       openposition, expected_change_bp
FROM v_keyrate_expectations
ORDER BY date, decision_date
FORMAT JSON
"""

OFZ_SQL = """
SELECT date, tenor, yield
FROM v_ofz_curve
ORDER BY date, tenor
FORMAT JSON
"""

MP_SUMMARY_SQL = """
SELECT meeting_date, published, decision_action, decision_rate,
       decision_delta_bp, signal
FROM v_cbr_mp_summary
ORDER BY meeting_date
FORMAT JSON
"""

MP_CHANNELS_SQL = """
SELECT meeting_date, channel, kind, seq, text
FROM v_cbr_mp_summary_channels
WHERE method = 'regex'
ORDER BY meeting_date, channel, kind, seq
FORMAT JSON
"""


def load_meetings() -> list:
    return ch_query(MEETINGS_SQL)


def load_cbr_macro() -> list:
    return ch_query(CBR_MACRO_SQL)


def load_rosstat_cpi() -> list:
    return ch_query(ROSSTAT_CPI_SQL)


def load_survey() -> list:
    return ch_query(SURVEY_SQL)


def load_consensus() -> list:
    return ch_query(CONSENSUS_SQL)


def load_keyrate() -> list:
    return ch_query(KEYRATE_SQL)


def load_ofz() -> list:
    return ch_query(OFZ_SQL)


def load_mp_summary() -> list:
    return ch_query(MP_SUMMARY_SQL)


def load_mp_channels() -> list:
    return ch_query(MP_CHANNELS_SQL)


# --- Снимок -------------------------------------------------------------------


@dataclass(frozen=True)
class Snapshot:
    """Срез данных на дату; `payload` — канонический словарь промпта."""

    data_asof: date
    meeting_date: date
    current_rate: float
    current_rate_date: date
    payload: dict
    warnings: tuple = ()


def canonical_meetings(rows: list) -> list:
    """Канонические заседания из `events_calendar` (фильтр по title)."""
    dates = [
        row_date(row["event_date"])
        for row in rows
        if row.get("title") == SAMPLE_TITLE
    ]
    return sorted(set(dates))


def _point(rows: list, series: str, as_of: date, available) -> Optional[dict]:
    return point_payload(latest_series_point(rows, series, as_of, available))


def _series_map(rows: list, series_names, as_of: date, available) -> dict:
    return {
        series: _point(rows, series, as_of, available)
        for series in series_names
    }


def _rosstat_inflation(rows: list, as_of: date) -> tuple:
    """ИПЦ Росстата: последняя точка каждого ряда и три последних общих м/м."""
    points = {}
    warnings = []
    for label, series in ROSSTAT_CPI_SERIES.items():
        row = latest_series_point(rows, series, as_of, available_from_monthly)
        if row is None:
            points[label] = None
            warnings.append(f"нет доступной точки ИПЦ Росстата: {label}")
            continue
        value = row_float(row["value"], f"ИПЦ Росстата: {label}")
        points[label] = {
            "date": row_date(row["date"]).isoformat(),
            "index": _round(value),
            "mom_pct": _round(value - 100.0, 2),
        }

    history_rows = [
        row
        for row in rows
        if row.get("series") == ROSSTAT_CPI_SERIES["all"]
        and available_from_monthly(row_date(row["date"])) <= as_of
    ]
    history_rows.sort(key=lambda row: row_date(row["date"]), reverse=True)
    history = [
        {
            "date": row_date(row["date"]).isoformat(),
            "mom_pct": _round(row_float(row["value"]) - 100.0, 2),
        }
        for row in history_rows[:3]
    ]
    points["recent_3m"] = list(reversed(history))
    return points, warnings


def _keyrate_target(rows: list, meeting: date, as_of: date) -> tuple:
    """Серия KEYRATE к целевому заседанию: точная или ближайшая будущая."""
    available_rows = [
        row
        for row in rows
        if available_from_next_day(row_date(row["date"])) <= as_of
        and row_date(row["decision_date"]) >= meeting
    ]
    if not available_rows:
        return None, "нет доступной серии KEYRATE к заседанию"
    decision_dates = sorted({row_date(row["decision_date"]) for row in available_rows})
    exact = meeting in decision_dates
    target = meeting if exact else decision_dates[0]
    candidates = [
        row for row in available_rows if row_date(row["decision_date"]) == target
    ]
    row = max(candidates, key=lambda item: row_date(item["date"]))
    payload = {
        "date": row_date(row["date"]).isoformat(),
        "secid": str(row.get("secid", "")),
        "series_name": str(row.get("series_name", "")),
        "decision_date": target.isoformat(),
        "exact_match": exact,
        "settle": _round(row_float(row["settle"], "KEYRATE settle")),
        "expected_change_bp": _round(row_optional_float(row.get("expected_change_bp")), 2),
        "volume": int(row_float(row["volume"], "KEYRATE volume")),
        "openposition": int(row_float(row["openposition"], "KEYRATE openposition")),
    }
    warning = None
    if not exact:
        warning = (
            f"точной серии KEYRATE на {meeting.isoformat()} нет; "
            f"использована {target.isoformat()}"
        )
    return payload, warning


def _ofz_points(rows: list, as_of: date) -> dict:
    points = {}
    for tenor in OFZ_TENORS:
        row = latest_available(
            [item for item in rows if item.get("tenor") == tenor],
            as_of,
            available_from_next_day,
        )
        if row is None:
            points[tenor] = None
            continue
        points[tenor] = {
            "date": row_date(row["date"]).isoformat(),
            "value": _round(row_float(row["yield"], f"ОФЗ {tenor}")),
        }
    return points


def _summary_payload(summary_rows: list, channel_rows: list, as_of: date) -> tuple:
    published_rows = [
        row
        for row in summary_rows
        if row_date(row["published"]) <= as_of
    ]
    if not published_rows:
        return None, "нет опубликованного резюме ЦБ к дате среза"
    row = max(
        published_rows,
        key=lambda item: (row_date(item["published"]), row_date(item["meeting_date"])),
    )
    meeting = row_date(row["meeting_date"])
    channels = [
        item
        for item in channel_rows
        if row_date(item["meeting_date"]) == meeting
    ]
    channels.sort(key=lambda item: (item.get("channel", ""), item.get("kind", ""), int(item.get("seq", 0))))
    channels = channels[:MAX_CHANNEL_FRAGMENTS]
    payload = {
        "meeting_date": meeting.isoformat(),
        "published": row_date(row["published"]).isoformat(),
        "decision_action": str(row.get("decision_action", "")),
        "decision_rate": _round(row_float(row["decision_rate"], "ставка решения")),
        "decision_delta_bp": _round(row_optional_float(row.get("decision_delta_bp")), 2),
        "signal": str(row.get("signal", "")).strip(),
        "channels": [
            {
                "channel": str(item.get("channel", "")),
                "kind": str(item.get("kind", "")),
                "text": str(item.get("text", "")).strip(),
            }
            for item in channels
        ],
    }
    return payload, None


def _consensus_payload(rows: list, as_of: date) -> tuple:
    points = {}
    warnings = []
    for series in CONSENSUS_SERIES:
        row = latest_series_point(rows, series, as_of, available_from_consensus)
        if row is None:
            points[series] = None
            warnings.append(f"нет доступной волны консенсуса: {series}")
            continue
        points[series] = {
            "date": row_date(row["date"]).isoformat(),
            "value": _round(row_float(row["value"]), 2),
        }
    wave = max(
        (point["date"] for point in points.values() if point is not None),
        default=None,
    )
    return {"wave": wave, **points}, warnings


def _ruonia_payload(rows: list, as_of: date) -> dict:
    return _series_map(
        rows,
        (
            RUONIA_SERIES,
            RUONIA_P25_SERIES,
            RUONIA_P75_SERIES,
            RUONIA_VOL_SERIES,
            RUONIA_TRANS_SERIES,
        ),
        as_of,
        available_from_next_day,
    )


def build_snapshot_payload(
    *,
    as_of: date,
    meeting: date,
    cbr_rows: list,
    rosstat_rows: list,
    survey_rows: list,
    consensus_rows: list,
    keyrate_rows: list,
    ofz_rows: list,
    summary_rows: list,
    channel_rows: list,
) -> tuple:
    """Собирает канонический снимок и список предупреждений о пропусках."""
    warnings = []

    rate_row = latest_series_point(cbr_rows, KEY_RATE_SERIES, as_of, available_from_daily)
    if rate_row is None:
        raise SnapshotError("нет доступной ключевой ставки на дату среза")
    current_rate = row_float(rate_row["value"], "ключевая ставка")
    current_rate_date = row_date(rate_row["date"])

    inflation, inflation_warnings = _rosstat_inflation(rosstat_rows, as_of)
    warnings.extend(inflation_warnings)
    cbr_inflation = {
        "core_mom": _point(cbr_rows, CORE_CPI_SERIES, as_of, available_from_monthly),
        "observed_yoy": _point(
            cbr_rows, OBSERVED_INFLATION_SERIES, as_of, available_from_monthly
        ),
        "expected_yoy": _point(
            cbr_rows, EXPECTED_INFLATION_SERIES, as_of, available_from_monthly
        ),
    }
    for label, point in cbr_inflation.items():
        if point is None:
            warnings.append(f"нет доступной точки ИПЦ ЦБ: {label}")

    survey = _series_map(survey_rows, SURVEY_SERIES, as_of, available_from_monthly)
    warnings.extend(
        f"нет доступной точки мониторинга предприятий: {series}"
        for series, point in survey.items()
        if point is None
    )

    keyrate, keyrate_warning = _keyrate_target(keyrate_rows, meeting, as_of)
    if keyrate_warning:
        warnings.append(keyrate_warning)
    ofz = _ofz_points(ofz_rows, as_of)
    warnings.extend(
        f"нет доступной точки ОФЗ: {tenor}" for tenor, point in ofz.items() if point is None
    )

    consensus, consensus_warnings = _consensus_payload(consensus_rows, as_of)
    warnings.extend(consensus_warnings)

    summary, summary_warning = _summary_payload(summary_rows, channel_rows, as_of)
    if summary_warning:
        warnings.append(summary_warning)

    payload = {
        "data_asof": as_of.isoformat(),
        "meeting_date": meeting.isoformat(),
        "current_rate": {
            "date": current_rate_date.isoformat(),
            "value": _round(current_rate),
        },
        "inflation": {
            "cbr": cbr_inflation,
            "rosstat": inflation,
        },
        "expectations": {
            "price_expectations_sa": survey.get("price_expectations_sa"),
            "price_growth_expected_sa": survey.get("price_growth_expected_sa"),
        },
        "business_survey": {
            "business_climate_sa": survey.get("business_climate_sa"),
            "capacity_utilization_sa": survey.get("capacity_utilization_sa"),
            "staff_availability_sa": survey.get("staff_availability_sa"),
            "credit_conditions": survey.get("credit_conditions"),
            "headcount_expectations_sa": survey.get("headcount_expectations_sa"),
        },
        "market": {
            "keyrate": keyrate,
            "ofz": ofz,
            "ruonia": _ruonia_payload(cbr_rows, as_of),
            "usd": _point(cbr_rows, USD_SERIES, as_of, available_from_next_day),
        },
        "consensus": consensus,
        "mp_summary": summary,
    }
    return payload, current_rate, current_rate_date, tuple(warnings)


def load_snapshot(as_of: date) -> Snapshot:
    """Читает витрины и собирает снимок as-of; без будущих данных."""
    meetings = canonical_meetings(load_meetings())
    upcoming = [day for day in meetings if day > as_of]
    if not upcoming:
        raise SnapshotError(
            f"в events_calendar нет заседания позже даты среза {as_of.isoformat()}"
        )
    meeting = min(upcoming)

    payload, current_rate, current_rate_date, warnings = build_snapshot_payload(
        as_of=as_of,
        meeting=meeting,
        cbr_rows=load_cbr_macro(),
        rosstat_rows=load_rosstat_cpi(),
        survey_rows=load_survey(),
        consensus_rows=load_consensus(),
        keyrate_rows=load_keyrate(),
        ofz_rows=load_ofz(),
        summary_rows=load_mp_summary(),
        channel_rows=load_mp_channels(),
    )
    return Snapshot(
        data_asof=as_of,
        meeting_date=meeting,
        current_rate=current_rate,
        current_rate_date=current_rate_date,
        payload=payload,
        warnings=warnings,
    )


def snapshot_payload(snapshot: Snapshot) -> dict:
    return snapshot.payload


def data_version(payload: dict) -> str:
    """Версия набора данных: 12 hex SHA-256 канонической JSON-сериализации."""
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:12]


# --- Промпт -------------------------------------------------------------------


def system_prompt() -> str:
    """System-сообщение фиксированной редакции `PROMPT_VERSION`."""
    return (
        f"Редакция промпта: {PROMPT_VERSION}.\n"
        "Ты — аналитик денежно-кредитной политики Банка России. По снимку данных "
        "на дату среза спрогнозируй решение Совета директоров по ключевой ставке "
        "на ближайшем заседании.\n\n"
        "Правила:\n"
        "- используй только данные снимка; не добавляй знания о событиях после "
        "даты среза и не домысливай отсутствующие ряды;\n"
        "- пустое значение (`null`) означает «нет обновления», а не ноль;\n"
        "- вероятности классов cut (снижение), hold (сохранение) и hike "
        "(повышение) — неотрицательные числа, сумма 100;\n"
        "- expected_step_bp — ожидаемый шаг ставки в базисных пунктах "
        "относительно текущей ставки: cut < 0, hold = 0, hike > 0;\n"
        "- вероятности должны быть согласованы с expected_step_bp;\n"
        "- rationale — 2–5 предложений по-русски; risks — список конкретных "
        "рисков по-русски;\n"
        "- не давай инвестиционных рекомендаций и не ссылайся на будущие "
        "публикации.\n\n"
        "Верни строго один JSON-объект без текста и пояснений вокруг него:\n"
        '{"probabilities":{"cut":<число>,"hold":<число>,"hike":<число>},'
        '"expected_step_bp":<число>,"rationale":"<текст>","risks":["<риск>"]}'
    )


def user_prompt(snapshot: Snapshot) -> str:
    """User-сообщение: канонический JSON снимка, детерминированный по данным."""
    return (
        "Снимок данных на дату среза (JSON; единицы указаны в названиях рядов):\n"
        + json.dumps(
            snapshot_payload(snapshot),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )


def prompt_sha256(snapshot: Snapshot) -> str:
    """Отпечаток system+user промпта: фиксирует редакцию и снимок."""
    raw = system_prompt() + "\n" + user_prompt(snapshot)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# --- LLM-транспорт ------------------------------------------------------------


@dataclass(frozen=True)
class LLMConfig:
    base_url: str
    api_key: str
    model: str

    def __repr__(self) -> str:
        # Ключ не должен попадать в repr при отладке/логировании.
        return f"LLMConfig(base_url={self.base_url!r}, model={self.model!r})"


@dataclass(frozen=True)
class LLMCall:
    content: str
    json_mode: bool


def llm_config_from_env() -> LLMConfig:
    """Читает LLM_BASE_URL/LLM_API_KEY/LLM_MODEL; сообщение — только имена."""
    names = ("LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL")
    values = {name: os.environ.get(name, "").strip() for name in names}
    missing = [name for name in names if not values[name]]
    if missing:
        raise MissingEnvError(
            "LLM-прогноз пропущен: не заданы " + ", ".join(missing)
        )
    return LLMConfig(
        base_url=values["LLM_BASE_URL"],
        api_key=values["LLM_API_KEY"],
        model=values["LLM_MODEL"],
    )


def llm_url(base_url: str) -> str:
    """OpenAI-совместимый `/v1/chat/completions` из base провайдера.

    Хвостовой `/v1` не удваиваем; у Anthropic-совместимого base убираем
    `/anthropic` — тот же приём, что у профиля `glm` разметки новостей.
    """
    base = base_url.strip().rstrip("/")
    if base.endswith("/v1"):
        return base + "/chat/completions"
    if base.endswith("/anthropic"):
        base = base[: -len("/anthropic")]
    return base + "/v1/chat/completions"


def _snippet(raw: bytes, limit: int = 512) -> str:
    """Фрагмент тела ошибки: без ключа, без переводов строк, ≤ limit байт."""
    text = raw[:limit].decode("utf-8", errors="replace")
    return " ".join(text.split())


def _mentions_json_mode(detail: str) -> bool:
    lowered = detail.lower()
    return any(
        marker in lowered
        for marker in ("response_format", "json_object", "json mode", "json-mode")
    )


def _openai_request(
    config: LLMConfig,
    system: str,
    user: str,
    json_mode: bool,
) -> tuple:
    body = {
        "model": config.model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": 0,
        "max_tokens": LLM_MAX_TOKENS,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    request = urllib.request.Request(
        llm_url(config.base_url),
        data=data,
        method="POST",
    )
    request.add_header("Content-Type", "application/json")
    # Ключ — заголовком Authorization, не в URL/query: в логи и ошибки не попадает.
    request.add_header("Authorization", "Bearer " + config.api_key)
    try:
        with urllib.request.urlopen(request, timeout=LLM_TIMEOUT_SECONDS) as response:
            return response.read(), json_mode
    except urllib.error.HTTPError as exc:
        detail = _snippet(exc.read() if hasattr(exc, "read") else b"")
        if json_mode and exc.code in (400, 415, 422) and _mentions_json_mode(detail):
            # Провайдер не поддерживает response_format=json_object: повторяем
            # тот же запрос без JSON-режима. Это не ошибка формата и не второй
            # повтор: ответ всё равно пройдёт parse_answer.
            log.warning(
                "LLM: провайдер не поддержал response_format=json_object, "
                "повтор без JSON-режима"
            )
            return _openai_request(config, system, user, json_mode=False)
        raise LLMError(f"http status {exc.code}: {detail}") from exc
    except OSError as exc:
        raise LLMError(f"LLM недоступен: {type(exc).__name__}") from exc


def ask_once(config: LLMConfig, system: str, user: str) -> LLMCall:
    """Один запрос OpenAI-совместимого провайдера и конверт ответа."""
    raw, json_mode = _openai_request(config, system, user, json_mode=True)
    try:
        envelope = json.loads(raw.decode("utf-8", errors="replace"))
    except json.JSONDecodeError as exc:
        raise LLMFormatError(f"OpenAI-конверт не разобран: {exc}") from exc
    choices = envelope.get("choices") if isinstance(envelope, dict) else None
    if not choices:
        raise LLMFormatError("в OpenAI-ответе нет choices")
    message = choices[0].get("message") if isinstance(choices[0], dict) else None
    content = message.get("content") if isinstance(message, dict) else None
    if not isinstance(content, str) or not content.strip():
        # Reasoning-модель может положить весь ответ в reasoning_content, а
        # content оставить пустым (наблюдалось на deepseek-flash при малом
        # max_tokens). JSON всё равно валидируется parse_answer; если и там
        # пусто — это ошибка формата с finish_reason для диагностики.
        reasoning = message.get("reasoning_content") if isinstance(message, dict) else None
        if isinstance(reasoning, str) and reasoning.strip():
            content = reasoning
        else:
            finish_reason = choices[0].get("finish_reason") if isinstance(choices[0], dict) else None
            keys = ",".join(sorted(message.keys())) if isinstance(message, dict) else "нет message"
            raise LLMFormatError(
                f"в choices[0].message.content пусто (finish_reason={finish_reason}; "
                f"поля message: {keys})"
            )
    usage = envelope.get("usage") or {}
    if isinstance(usage, dict):
        log.info(
            "LLM %s: prompt %s, completion %s",
            config.model,
            usage.get("prompt_tokens", "н/д"),
            usage.get("completion_tokens", "н/д"),
        )
    return LLMCall(content=content, json_mode=json_mode)


# --- Валидация ответа ---------------------------------------------------------


@dataclass(frozen=True)
class Answer:
    probabilities: dict
    expected_step_bp: float
    rationale: str
    risks: list


def normalize_probabilities(probabilities: dict) -> dict:
    """Нормирует вероятности к 100 % с точностью 0,01 п.п.

    Принимает и проценты (60/30/10), и доли (0.6/0.3/0.1). Отрицательные
    значения, NaN/бесконечности и нулевая сумма — ошибка формата. Остаток
    округления раздаётся компонентам с наибольшей дробной частью, поэтому
    сумма сотых долей ровно 10000 (100,00 %).
    """
    if not isinstance(probabilities, dict):
        raise LLMFormatError("probabilities: ожидается объект")
    missing = [name for name in CLASSES if name not in probabilities]
    if missing:
        raise LLMFormatError(
            "probabilities: нет ключей " + ", ".join(missing)
        )
    values = {}
    for name in CLASSES:
        value = probabilities[name]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise LLMFormatError(f"probabilities.{name}: ожидается число")
        number = float(value)
        if not math.isfinite(number) or number < 0:
            raise LLMFormatError(
                f"probabilities.{name}: ожидается конечное неотрицательное число"
            )
        values[name] = number
    total = sum(values.values())
    if total <= 0:
        raise LLMFormatError("probabilities: сумма должна быть больше нуля")
    if not math.isfinite(total):
        raise LLMFormatError("probabilities: сумма должна быть конечной")

    total_units = 10000
    exact = {
        name: Decimal(str(values[name])) * total_units / Decimal(str(total))
        for name in CLASSES
    }
    units = {
        name: int(exact[name].to_integral_value(rounding=ROUND_FLOOR))
        for name in CLASSES
    }
    remainder = total_units - sum(units.values())
    order = sorted(
        CLASSES,
        key=lambda name: (exact[name] - units[name], CLASSES.index(name)),
        reverse=True,
    )
    for index in range(remainder):
        units[order[index % len(order)]] += 1
    return {name: units[name] / 100.0 for name in CLASSES}


def _extract_json_object(content: str) -> str:
    text = content.strip()
    start = text.find("{")
    end = text.rfind("}")
    if start < 0 or end < start:
        raise LLMFormatError(f"в ответе нет JSON-объекта ({len(text)} байт)")
    return text[start : end + 1]


def parse_answer(content: str) -> Answer:
    """Разбирает и валидирует один ответ модели."""
    try:
        data = json.loads(_extract_json_object(content))
    except json.JSONDecodeError as exc:
        raise LLMFormatError(f"ответ не разобран: {exc}") from exc
    if not isinstance(data, dict):
        raise LLMFormatError("ответ: ожидается JSON-объект")

    probabilities = normalize_probabilities(data.get("probabilities"))

    step_value = data.get("expected_step_bp")
    if isinstance(step_value, bool) or not isinstance(step_value, (int, float)):
        raise LLMFormatError("expected_step_bp: ожидается число")
    step = float(step_value)
    if not math.isfinite(step) or not (MIN_STEP_BP <= step <= MAX_STEP_BP):
        raise LLMFormatError(
            f"expected_step_bp: вне диапазона [{MIN_STEP_BP}; {MAX_STEP_BP}]"
        )

    rationale = data.get("rationale")
    if not isinstance(rationale, str) or not rationale.strip():
        raise LLMFormatError("rationale: ожидается непустая строка")
    rationale = rationale.strip()

    risks = data.get("risks")
    if isinstance(risks, str):
        risks = [risks]
    if not isinstance(risks, list) or not risks:
        raise LLMFormatError("risks: ожидается непустой список")
    clean_risks = []
    for risk in risks[:MAX_RISKS]:
        if not isinstance(risk, str) or not risk.strip():
            raise LLMFormatError("risks: элементы должны быть непустыми строками")
        clean_risks.append(risk.strip())

    return Answer(
        probabilities=probabilities,
        expected_step_bp=round(step, 4),
        rationale=rationale,
        risks=clean_risks,
    )


# --- Три повтора и агрегация --------------------------------------------------


@dataclass(frozen=True)
class Sample:
    index: int
    probabilities: dict
    expected_step_bp: float
    rationale: str
    risks: list
    attempts: int
    json_mode: bool


@dataclass(frozen=True)
class Forecast:
    data_asof: date
    meeting_date: date
    current_rate: float
    probabilities: dict
    expected_step_bp: float
    expected_rate: float
    rationale: str
    risks: list
    model: str
    prompt_version: int
    prompt_sha256: str
    data_version: str
    spread: dict
    samples: tuple
    note: str = NOTE


Ask = Callable[[LLMConfig, str, str], LLMCall]


def _representative(samples: list, probabilities: dict, step: float) -> Sample:
    """Повтор с минимальным L1 до медианы; тай-брейк — шаг и порядок."""
    def key(sample: Sample):
        distance = sum(
            abs(sample.probabilities[name] - probabilities[name])
            for name in CLASSES
        )
        return (round(distance, 9), abs(sample.expected_step_bp - step), sample.index)

    return min(samples, key=key)


def forecast(snapshot: Snapshot, config: LLMConfig, ask: Ask = ask_once) -> Forecast:
    """Три повтора, медиана, разброс и представительный rationale."""
    system = system_prompt()
    user = user_prompt(snapshot)
    samples = []
    for repeat in range(REPEATS):
        attempts = 0
        while attempts < MAX_FORMAT_ATTEMPTS:
            attempts += 1
            try:
                call = ask(config, system, user)
                answer = parse_answer(call.content)
            except LLMFormatError as exc:
                if attempts >= MAX_FORMAT_ATTEMPTS:
                    log.warning(
                        "LLM: повтор %d/%d пропущен после %d попыток: %s",
                        repeat + 1,
                        REPEATS,
                        attempts,
                        exc,
                    )
                continue
            samples.append(
                Sample(
                    index=len(samples),
                    probabilities=answer.probabilities,
                    expected_step_bp=answer.expected_step_bp,
                    rationale=answer.rationale,
                    risks=answer.risks,
                    attempts=attempts,
                    json_mode=call.json_mode,
                )
            )
            break

    if len(samples) < MIN_VALID_SAMPLES:
        raise ForecastError(
            f"валидных повторов {len(samples)} из {REPEATS}: нужно минимум "
            f"{MIN_VALID_SAMPLES}; прогноз не строится"
        )

    median_probabilities = normalize_probabilities(
        {
            name: statistics.median(sample.probabilities[name] for sample in samples)
            for name in CLASSES
        }
    )
    median_step = round(
        statistics.median(sample.expected_step_bp for sample in samples), 4
    )
    representative = _representative(samples, median_probabilities, median_step)
    spread = {
        "probability_spread_pp": {
            name: round(
                max(sample.probabilities[name] for sample in samples)
                - min(sample.probabilities[name] for sample in samples),
                2,
            )
            for name in CLASSES
        },
        "expected_step_spread_bp": round(
            max(sample.expected_step_bp for sample in samples)
            - min(sample.expected_step_bp for sample in samples),
            2,
        ),
    }
    return Forecast(
        data_asof=snapshot.data_asof,
        meeting_date=snapshot.meeting_date,
        current_rate=round(snapshot.current_rate, 4),
        probabilities=median_probabilities,
        expected_step_bp=median_step,
        expected_rate=round(snapshot.current_rate + median_step / 100.0, 4),
        rationale=representative.rationale,
        risks=list(representative.risks),
        model=config.model,
        prompt_version=PROMPT_VERSION,
        prompt_sha256=prompt_sha256(snapshot),
        data_version=data_version(snapshot_payload(snapshot)),
        spread=spread,
        samples=tuple(samples),
    )


# --- Артефакты ----------------------------------------------------------------


def forecast_payload(forecast_result: Forecast) -> dict:
    """Тело JSON-артефакта; порядок ключей фиксирован, время запуска не входит."""
    return {
        "forecast_date": forecast_result.data_asof.isoformat(),
        "data_asof": forecast_result.data_asof.isoformat(),
        "meeting_date": forecast_result.meeting_date.isoformat(),
        "current_rate": forecast_result.current_rate,
        "probabilities": dict(forecast_result.probabilities),
        "expected_step_bp": forecast_result.expected_step_bp,
        "expected_rate": forecast_result.expected_rate,
        "rationale": forecast_result.rationale,
        "risks": list(forecast_result.risks),
        "model": forecast_result.model,
        "prompt_version": forecast_result.prompt_version,
        "prompt_sha256": forecast_result.prompt_sha256,
        "data_version": forecast_result.data_version,
        "spread": forecast_result.spread,
        "samples": [
            {
                "index": sample.index,
                "probabilities": dict(sample.probabilities),
                "expected_step_bp": sample.expected_step_bp,
                "attempts": sample.attempts,
                "json_mode": sample.json_mode,
            }
            for sample in forecast_result.samples
        ],
        "note": forecast_result.note,
    }


def forecast_json(forecast_result: Forecast) -> str:
    """Текст JSON-артефакта: UTF-8 как есть, отступ 2, LF в конце."""
    return json.dumps(
        forecast_payload(forecast_result), ensure_ascii=False, indent=2
    ) + "\n"


def _fmt(value, suffix: str = "") -> str:
    if value is None:
        return "нет обновления"
    if isinstance(value, float):
        text = f"{value:.2f}".rstrip("0").rstrip(".")
    else:
        text = str(value)
    return text + suffix


def _fmt_point(point: Optional[dict], suffix: str = "") -> str:
    if point is None:
        return "нет обновления"
    value = point.get("value", point.get("mom_pct"))
    return f"{_fmt(value, suffix)} ({point.get('date', '')})"


def render_report(forecast_result: Forecast, snapshot: Snapshot) -> str:
    """Markdown-отчёт прогона: прогноз, разброс, входы и оговорки."""
    payload = snapshot_payload(snapshot)
    inflation = payload.get("inflation", {})
    cbr_inflation = inflation.get("cbr", {})
    rosstat = inflation.get("rosstat", {})
    market = payload.get("market", {})
    keyrate = market.get("keyrate") or {}
    ofz = market.get("ofz", {})
    ruonia = market.get("ruonia", {})
    consensus = payload.get("consensus", {})
    survey = payload.get("business_survey", {})
    expectations = payload.get("expectations", {})
    summary = payload.get("mp_summary") or {}

    probabilities = forecast_result.probabilities
    spread = forecast_result.spread["probability_spread_pp"]
    lines = [
        "# LLM-прогноз ключевой ставки — экспериментальный трек",
        "",
        f"**Дата среза:** {forecast_result.data_asof.isoformat()}  ",
        f"**Заседание:** {forecast_result.meeting_date.isoformat()}  ",
        f"**Модель:** `{forecast_result.model}`  ",
        f"**Промпт:** v{forecast_result.prompt_version} "
        f"(`sha256 {forecast_result.prompt_sha256[:16]}…`)  ",
        f"**Версия данных:** `{forecast_result.data_version}`",
        "",
        "## Прогноз",
        "",
        "| Класс | Вероятность, % | Разброс, п.п. |",
        "|---|---:|---:|",
        f"| cut (снижение) | {probabilities['cut']:.2f} | {spread['cut']:.2f} |",
        f"| hold (сохранение) | {probabilities['hold']:.2f} | {spread['hold']:.2f} |",
        f"| hike (повышение) | {probabilities['hike']:.2f} | {spread['hike']:.2f} |",
        "",
        f"**Ожидаемый шаг:** {forecast_result.expected_step_bp:+.2f} б.п.  ",
        f"**Ожидаемая ставка:** {forecast_result.expected_rate:.2f} % "
        f"(текущая {forecast_result.current_rate:.2f} %)  ",
        f"**Разброс шага между повторами:** "
        f"{forecast_result.spread['expected_step_spread_bp']:.2f} б.п.",
        "",
        f"**Обоснование:** {forecast_result.rationale}",
        "",
        "**Риски:**",
        "",
    ]
    lines += [f"- {risk}" for risk in forecast_result.risks] or ["- нет обновления"]
    lines += [
        "",
        "## Входы снимка",
        "",
        f"- Ключевая ставка: {_fmt_point(payload.get('current_rate'))}",
        f"- Базовый ИПЦ ЦБ, м/м: {_fmt_point(cbr_inflation.get('core_mom'), ' %')}",
        f"- Наблюдаемая инфляция, г/г: "
        f"{_fmt_point(cbr_inflation.get('observed_yoy'), ' %')}",
        f"- Ожидаемая инфляция, г/г: "
        f"{_fmt_point(cbr_inflation.get('expected_yoy'), ' %')}",
        f"- ИПЦ Росстата, м/м (все товары и услуги): "
        f"{_fmt(rosstat.get('all', {}).get('mom_pct') if rosstat.get('all') else None, ' %')}",
        f"- ИПЦ Росстата, м/м (продовольствие): "
        f"{_fmt(rosstat.get('food', {}).get('mom_pct') if rosstat.get('food') else None, ' %')}",
        f"- ИПЦ Росстата, м/м (непродовольственные товары): "
        f"{_fmt(rosstat.get('nonfood', {}).get('mom_pct') if rosstat.get('nonfood') else None, ' %')}",
        f"- ИПЦ Росстата, м/м (услуги): "
        f"{_fmt(rosstat.get('services', {}).get('mom_pct') if rosstat.get('services') else None, ' %')}",
        f"- Ценовые ожидания бизнеса (SA): "
        f"{_fmt_point(expectations.get('price_expectations_sa'))}",
        f"- Ожидаемый прирост отпускных цен (SA): "
        f"{_fmt_point(expectations.get('price_growth_expected_sa'), ' %')}",
        f"- Бизнес-климат (SA): {_fmt_point(survey.get('business_climate_sa'))}",
        f"- Загрузка мощностей (SA): "
        f"{_fmt_point(survey.get('capacity_utilization_sa'), ' %')}",
        f"- Условия кредитования: {_fmt_point(survey.get('credit_conditions'))}",
        f"- KEYRATE: серия {keyrate.get('secid', 'н/д')} "
        f"({keyrate.get('decision_date', 'н/д')}), settle "
        f"{_fmt(keyrate.get('settle'), ' п.п.')}, вменённое изменение "
        f"{_fmt(keyrate.get('expected_change_bp'), ' б.п.')}, объём "
        f"{keyrate.get('volume', 'н/д')}, открытые позиции "
        f"{keyrate.get('openposition', 'н/д')}",
        f"- ОФЗ: 1y {_fmt_point(ofz.get('1y'), ' %')}; 3y "
        f"{_fmt_point(ofz.get('3y'), ' %')}; 5y {_fmt_point(ofz.get('5y'), ' %')}; "
        f"10y {_fmt_point(ofz.get('10y'), ' %')}; RGBI "
        f"{_fmt_point(ofz.get('RGBI'))}",
        f"- RUONIA: {_fmt_point(ruonia.get('cbr_ruania_ruo'), ' %')}; "
        f"p25 {_fmt_point(ruonia.get('cbr_ruania_p25'), ' %')}; "
        f"p75 {_fmt_point(ruonia.get('cbr_ruania_p75'), ' %')}; "
        f"объём {_fmt_point(ruonia.get('cbr_ruania_vol'))}",
        f"- Консенсус (волна {consensus.get('wave') or 'н/д'}): ставка 2026 "
        f"{_fmt(consensus.get('key_rate_avg_2026_median', {}).get('value') if consensus.get('key_rate_avg_2026_median') else None, ' %')}, "
        f"ставка 2027 "
        f"{_fmt(consensus.get('key_rate_avg_2027_median', {}).get('value') if consensus.get('key_rate_avg_2027_median') else None, ' %')}, "
        f"ИПЦ дек. 2026 "
        f"{_fmt(consensus.get('cpi_dec_2026_median', {}).get('value') if consensus.get('cpi_dec_2026_median') else None, ' %')}",
        f"- Резюме ЦБ: {summary.get('meeting_date', 'нет обновления')} "
        f"(опубликовано {summary.get('published', 'н/д')}), решение "
        f"`{summary.get('decision_action', 'н/д')}` "
        f"{_fmt(summary.get('decision_rate'), ' %')}, шаг "
        f"{_fmt(summary.get('decision_delta_bp'), ' б.п.')}; "
        f"каналов в снимке: {len(summary.get('channels', []))}",
        "",
        "**Сигнал резюме:** " + (summary.get("signal") or "нет обновления"),
        "",
        "## Оговорки",
        "",
        f"- {forecast_result.note}",
        "- As-of — консервативное приближение, а не point-in-time слой: "
        "ревизии рядов и точные даты публикаций не учтены.",
        "- Расхождение с статистическим и ML-треком публикуется как есть; "
        "превосходство без DM/encompassing не утверждается.",
        "",
    ]
    if snapshot.warnings:
        lines += ["## Предупреждения снимка", ""]
        lines += [f"- {warning}" for warning in snapshot.warnings]
        lines.append("")
    return "\n".join(lines)


def clear_stale_artifacts(out_dir: Path) -> list:
    """Удаляет артефакты прошлого прогона; возвращает имена удалённых."""
    removed = []
    for name in ARTIFACT_NAMES:
        path = out_dir / name
        if path.exists():
            path.unlink()
            removed.append(name)
    return removed


def write_forecast_artifact(path: Path, forecast_result: Forecast) -> None:
    path.write_text(forecast_json(forecast_result), encoding="utf-8")


def log_payload(forecast_result: Forecast) -> dict:
    """Payload для ingest #127; прямой записи в ClickHouse нет."""
    return {
        "status": "pending_ingest_127",
        "todo": LOG_TODO,
        "row": {
            "metric": "cbr_rate",
            "forecast_date": forecast_result.data_asof.isoformat(),
            "target_date": forecast_result.meeting_date.isoformat(),
            "predicted": forecast_result.expected_rate,
            "actual": None,
            "error_pct": None,
            # Колонка резолвера #127: ошибка ставки — в базисных пунктах, не в
            # процентах (KEY_RATE_PUBLICATIONS §8).
            "error_bp": None,
            "probabilities": dict(forecast_result.probabilities),
            "expected_step_bp": forecast_result.expected_step_bp,
            "model": forecast_result.model,
            "prompt_version": forecast_result.prompt_version,
            "data_version": forecast_result.data_version,
        },
    }


def write_log_payload(path: Path, forecast_result: Forecast) -> None:
    path.write_text(
        json.dumps(log_payload(forecast_result), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


# --- Прогон и CLI -------------------------------------------------------------


def run(
    as_of: Optional[date] = None,
    out_dir: Path = ROOT,
    log_mode: bool = False,
    *,
    config: Optional[LLMConfig] = None,
    ask: Ask = ask_once,
) -> int:
    """Полный прогон: снимок → три повтора → артефакты → payload (#127)."""
    as_of = as_of or date.today()
    out_dir = Path(out_dir)
    try:
        # Env проверяется до чтения ClickHouse: отсутствие ключей — это
        # ошибка конфигурации, а не повод ходить в БД.
        if config is None:
            config = llm_config_from_env()
        snapshot = load_snapshot(as_of)
        forecast_result = forecast(snapshot, config, ask=ask)
    except (ForecastError, aisc.ClickHouseError) as exc:
        removed = clear_stale_artifacts(out_dir)
        print(f"LLM-прогноз не построен: {exc}", file=sys.stderr)
        if removed:
            print(
                "удалены устаревшие артефакты: " + ", ".join(removed),
                file=sys.stderr,
            )
        return 1

    json_path = out_dir / FORECAST_JSON_NAME
    report_path = out_dir / REPORT_NAME
    log_path = out_dir / LOG_PAYLOAD_NAME
    out_dir.mkdir(parents=True, exist_ok=True)
    write_forecast_artifact(json_path, forecast_result)
    report_path.write_text(
        render_report(forecast_result, snapshot), encoding="utf-8"
    )
    if log_mode:
        write_log_payload(log_path, forecast_result)
    else:
        # Stale-payload прошлого прогона не должен выглядеть результатом этого.
        log_path.unlink(missing_ok=True)

    print(
        f"LLM-прогноз ключевой ставки (экспериментальный трек) — "
        f"заседание {forecast_result.meeting_date.isoformat()}, "
        f"срез {forecast_result.data_asof.isoformat()}"
    )
    print(
        f"  cut/hold/hike: {forecast_result.probabilities['cut']:.2f} / "
        f"{forecast_result.probabilities['hold']:.2f} / "
        f"{forecast_result.probabilities['hike']:.2f} %"
    )
    print(
        f"  ожидаемый шаг: {forecast_result.expected_step_bp:+.2f} б.п.; "
        f"ожидаемая ставка {forecast_result.expected_rate:.2f} %"
    )
    print(
        f"  разброс: cut/hold/hike "
        f"{forecast_result.spread['probability_spread_pp']['cut']:.2f} / "
        f"{forecast_result.spread['probability_spread_pp']['hold']:.2f} / "
        f"{forecast_result.spread['probability_spread_pp']['hike']:.2f} п.п.; "
        f"шаг {forecast_result.spread['expected_step_spread_bp']:.2f} б.п."
    )
    print(f"  модель: {forecast_result.model}, prompt v{forecast_result.prompt_version}, "
          f"данные {forecast_result.data_version}")
    print(f"  JSON: {json_path}")
    print(f"  отчёт: {report_path}")
    if log_mode:
        print(f"  payload для #127: {log_path}")
        print(
            "TODO(#127): ingest endpoint не реализован — payload подготовлен, "
            "в ClickHouse ничего не записано"
        )
        print(json.dumps(log_payload(forecast_result), ensure_ascii=False, indent=2))
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Экспериментальный LLM-прогноз ключевой ставки: as-of снимок, "
                    "три повтора, медиана и разброс, артефакты и payload #127."
    )
    parser.add_argument(
        "--as-of",
        type=date.fromisoformat,
        metavar="YYYY-MM-DD",
        default=None,
        help="дата среза данных (по умолчанию — системная дата)",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        metavar="DIR",
        default=ROOT,
        help="каталог артефактов (по умолчанию — корень репозитория)",
    )
    parser.add_argument(
        "--log",
        action="store_true",
        help="дополнительно записать payload для ingest #127; "
             "в ClickHouse не пишет",
    )
    args = parser.parse_args(argv)
    try:
        return run(as_of=args.as_of, out_dir=args.out_dir, log_mode=args.log)
    except (ForecastError, aisc.ClickHouseError) as exc:
        print(f"LLM-прогноз не построен: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    sys.exit(main())
