#!/usr/bin/env python3
"""Первый срез модели ключевой ставки: выборка решений, as-of слой и признаки.

Измерительная основа контура ключевой ставки (спека
`docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md` §3–4): единица
наблюдения — заседание Совета директоров ЦБ по ключевой ставке.

Что делает этот срез:

* **выборка решений** — канонические строки `events_calendar`
  (`category='cbr'∧title=SAMPLE_TITLE`; ручные строки-триггеры сида отсекаются
  фильтром по `title`) плюс дневной ряд `cbr_key_rate`: `rate_before` на `d−1`,
  `rate_after` на `min(d+7, next−1)`, шаг в б.п. и класс `y3`. Заседание даёт
  решение, только если его исход наблюдаем (окно уложилось в ряд ставки); иначе
  оно «вне выборки», а при отсутствии ставки на `d−1` — «пропущено» (§3);
* **as-of слой** — консервативные правила доступности объясняющих рядов (§4):
  точка участвует, только если её `available_from(period) ≤ даты заседания`;
* **признаки** — `pi_saar` (базовый ИПЦ SA, приведённый к годовому) и `gap`
  (ИБК ЦБ SA); рядом — `rate_before`. Ряд без точки раньше заседания
  исключается из выборки и перечисляется в «пропущенных».

Что добавляет этот срез (Task 3):

* **walk-forward** — расширяющееся окно (минимум 24 заседания в обучении) и
  скользящее 40 заседаний; каждый фолд переобучается только на прошлом,
  горизонт прогноза — одно заседание (спека §7); вырожденное обучающее окно
  (`DegenerateWindowError`) пропускается со счётчиком, а не роняет прогон;
* **бенчмарки B1–B3** (спека §5): «ставка не изменится», «повтор последнего
  решения», AR(1) на уровне ставки (OLS с HAC); все прогнозы и факты — в
  БАЗИСНЫХ пунктах (одна шкала у бенчмарков и метрик);
* **метрики §7** — RMSE и MAE по шагу в б.п., hit rate, матрица 3×3, QWK,
  Brier, log loss, надёжностная таблица; тесты DM (поправка HLN через
  `statsmodels.tsa.stattools.diebold_mariano_test`), Mincer–Zarnowitz с HAC,
  Pesaran–Timmermann (через `statsmodels.stats.diagnostic`) и блочный
  бутстрап разницы RMSE;
* **артефакты §8** — `key_rate_model_report.md` и
  `key_rate_model_predictions.csv` в каталоге прогона (корень репозитория; в
  git не коммитятся).

Чего здесь ЕЩЁ НЕТ (следующие шаги плана): методы №2 (инерционное правило
Тейлора) и №3 (упорядоченный логит) и текущий прогноз на ближайшее заседание —
их добавляют Tasks 4–6 в этот же файл. Отчёт и метрики считаются по списку
«метод × окно», поэтому строка метода появляется в них без правки отчёта;
вероятностные метрики (Brier, log loss, надёжностная таблица) считаются
только для методов, которые отдают распределение классов.

**Скрипт ничего не пишет в ClickHouse** — только читает (прецедент
`polyus_output_model.py`); транспорт (`ch_query`) импортируется из
`scripts/extrapolation_aisc.py`, а не копируется: там уже есть маскирование
секретов в текстах ошибок, HTTP-порт 8123 и обработка обрывов соединения.
Секреты приходят только из окружения (их подставляет Makefile).

Запуск — только через `make` (переменные окружения подключает Makefile):

* `make key-rate-model` — полный прогон: сводка выборки, walk-forward
  бенчмарков по двум окнам, отчёт и CSV;
* `make key-rate-model ARGS="--rolling"` — то же, но консольная таблица
  метрик печатается по скользящему окну 40 (в отчёт и CSV оба окна идут
  всегда);
* `make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"` —
  пересборка CSV-фикстур офлайн-тестов `scripts/key_rate_model_test.py`.

**As-of — приближение, а не point-in-time слой** (спека §4): ревизии
объясняющих рядов и точные даты публикаций не учтены, полноценный PIT-слой —
следующий пункт фазы 4.
"""

import argparse
import csv
import math
import random
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import statsmodels.api as sm

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его
# (прецедент `polyus_output_model.py`).
sys.path.insert(0, str(ROOT / "scripts"))

import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# Суффикс `FINAL` обязателен во всех запросах ниже: таблицы — ReplacingMergeTree,
# и без него вытесненные копии строк дали бы дубли точек (и дубли заседаний).
# `FORMAT JSON` — требование транспорта: `ch_query` разбирает JSON-ответ, а
# HTTP-интерфейс по умолчанию отвечает TabSeparated.

# Канонический title заседания в `events_calendar`. Ручные строки-триггеры
# (сид `events_calendar_q4_2026.sql`) лежат в той же категории `cbr` с другими
# title и отсекаются ЭТИМ фильтром. Фильтр живёт в Python (запрос берёт всю
# категорию): так правило — одно, и оно проверяется на фикстуре, в которой
# ручные строки присутствуют.
SAMPLE_TITLE = "СД ЦБ РФ: заседание по ключевой ставке"

# Ряд базового ИПЦ ЦБ: сезонно очищенный месячный темп, % к предыдущему месяцу
# (`cbr_indicators_cpd`, дата точки — последний день месяца).
CPD_CORE_SERIES = "Базовый ИПЦ"

# Ряд ИБК ЦБ SA (пункты) — прокси разрыва выпуска (`macro_series`,
# `source='cbr_survey'`; дата точки — 1-е число отчётного месяца).
SURVEY_SOURCE = "cbr_survey"
IBK_SERIES = "business_climate_sa"

# Сид для бутстрапа (Task 3+) и «версии набора данных»: воспроизводимость
# прогона считается от одного числа, названного здесь и больше нигде.
SEED = 20261015

# Горизонт окна исхода `d+7` (спека §3): верхняя граница в календарных днях.
OUTCOME_WINDOW_DAYS = 7

# Окно срезов-фикстур офлайн-тестов. Верхняя граница — 2016-12-31: у последнего
# заседания среза (2016-12-16) окно исхода (2016-12-23) ещё наблюдаемо целиком,
# а у заседаний 2017+ оно уже выходит за ряд ставки — так на фикстуре
# проверяется правило «окно исхода вне данных ставки → заседание вне выборки»
# (на живом прогоне оно отсеивает будущие заседания). Нижняя граница 2013-01-01
# взята с запасом, чтобы ставка «на d−1» первого заседания (2014-02-14) была
# покрыта. Имена файлов фикстур (`*_2013_2016`) равны фактическому окну содержимого.
FIXTURE_FROM = date(2013, 1, 1)
FIXTURE_TO = date(2016, 12, 31)

# --- Константы бэктеста (спека §5/§7) -----------------------------------------

# Walk-forward: минимум заседаний в обучении (расширяющееся окно) и размер
# скользящего окна. Обе схемы докладываются как проверка робастности; выбор
# окна по ошибке не делается (спека §7) — окна считаются всегда оба.
MIN_TRAIN = 24
ROLLING_WINDOW = 40

# Метка расширяющегося окна в отчёте и CSV; скользящее — `rolling<N>`.
WINDOW_EXPANDING = "expanding"

# Лаги HAC (Ньюи–Уэст) — 4 заседания ≈ полугодие между решениями (спека §6).
# Одна конфигурация на AR(1), Mincer–Zarnowitz и методы №2–3: p-value разных
# строк отчёта обязаны считаться на одинаковой ковариации.
HAC_MAXLAGS = 4

# Режимный блок отчёта — 2024–2026 (спека §7). В тексте спеки заседаний 24,
# но октябрь и декабрь 2026 на дату прогона ещё будущие: в блок входят
# прошедшие заседания, и отчёт печатает их число сам.
REGIME_FROM = date(2024, 1, 1)
REGIME_TO = date(2026, 12, 31)

# OLS с HAC не оценивается на слишком коротком окне: две точки — нулевые
# остаточные степени свободы, и поправка ковариации делит на ноль.
MIN_OLS_OBS = 3

# Точная подгонка MZ (остатков нет) в пределах машинной погрешности: F-тест
# тогда 0/0, и шум округления дал бы ложное p. Допуск по остаточной сумме
# квадратов относительный (масштаб — сумма квадратов факта), по коэффициентам —
# абсолютный (уровни в % и б.п.).
MZ_EXACT_RTOL = 1e-12
MZ_EXACT_ATOL = 1e-9

# Нижний порог вероятности в log loss: нулевая вероятность факта иначе дала бы
# бесконечность, и среднее по фолдам «уехало» бы в inf.
LOG_LOSS_EPS = 1e-15

# Артефакты прогона в каталоге прогона (в git не коммитятся, см. .gitignore).
REPORT_NAME = "key_rate_model_report.md"
PREDICTIONS_NAME = "key_rate_model_predictions.csv"

# Колонки CSV прогнозов. `probs` — вероятности классов в порядке (−1, 0, +1)
# через «|»; пустая ячейка — метод распределения не даёт (бенчмарки B1–B3).
PREDICTION_COLUMNS = (
    "meeting_date",
    "method",
    "window",
    "actual_step_bp",
    "actual_y3",
    "step_bp",
    "probs",
)

# --- Константы запросов -------------------------------------------------------

MEETINGS_SQL = """
SELECT event_date, category, title
FROM events_calendar FINAL
WHERE category = 'cbr'
ORDER BY event_date
FORMAT JSON
"""

KEY_RATE_SQL = """
SELECT date, rate
FROM cbr_key_rate FINAL
ORDER BY date
FORMAT JSON
"""

CPD_CORE_SQL = f"""
SELECT date, value
FROM cbr_indicators_cpd FINAL
WHERE name = '{CPD_CORE_SERIES}'
ORDER BY date
FORMAT JSON
"""

IBK_SQL = f"""
SELECT date, value
FROM macro_series FINAL
WHERE source = '{SURVEY_SOURCE}' AND series = '{IBK_SERIES}'
ORDER BY date
FORMAT JSON
"""

MP_SUMMARY_SQL = """
SELECT meeting_date, decision_rate, decision_delta_bp
FROM cbr_mp_summary FINAL
ORDER BY meeting_date
FORMAT JSON
"""


def _fixture_sql(sql: str) -> str:
    """Тот же запрос, но в окне фикстур `[FIXTURE_FROM, FIXTURE_TO]`.

    Диапазон вставляется в `WHERE` готового запроса, а не дублирует его текст:
    колонки, `FINAL` и имена рядов обязаны совпадать у живого и фикстурного
    запроса, иначе фикстура проверяла бы не то, что читает прогон.
    """
    where = f"date >= '{FIXTURE_FROM.isoformat()}' AND date <= '{FIXTURE_TO.isoformat()}'"
    if "\nWHERE " in sql:
        return sql.replace("\nWHERE ", f"\nWHERE {where} AND ", 1)
    return sql.replace("\nORDER BY ", f"\nWHERE {where}\nORDER BY ", 1)


FIXTURE_KEY_RATE_SQL = _fixture_sql(KEY_RATE_SQL)
FIXTURE_CPD_CORE_SQL = _fixture_sql(CPD_CORE_SQL)
FIXTURE_IBK_SQL = _fixture_sql(IBK_SQL)

# Колонки CSV-фикстур — контракт с тестами: те же имена, что у колонок SELECT
# (строки фикстур читаются теми же `*_from_rows`, что и ответы ch_query).
FIXTURE_COLUMNS = {
    "meetings.csv": ("event_date", "category", "title"),
    "key_rate_2013_2016.csv": ("date", "rate"),
    "cpd_core_2013_2016.csv": ("date", "value"),
    "ibk_2013_2016.csv": ("date", "value"),
    "mp_summary.csv": ("meeting_date", "decision_rate", "decision_delta_bp"),
}

# Ручная строка-триггер с УНИКАЛЬНОЙ датой — только для фикстуры, добавляется
# ЭКСПОРТОМ (регенерация обязана давать тот же файл). Зачем: обе живые ручные
# строки сида делят дату с каноническими (2026-10-23, 2026-12-18 помечены и
# сидом, и календарём ЦБ), поэтому по датам фильтр по `title` на фикстуре
# ненаблюдаем — дата осталась бы в списке и без него. У даты 2016-12-20
# канонической строки нет: без фильтра она попала бы в выборку и дала бы лишнее
# «решение», а тест это ловит.
FIXTURE_MEETINGS_EXTRA = (
    (date(2016, 12, 20), "cbr", "СД ЦБ РФ: тест-фикстура — ручной триггер с уникальной датой"),
)


class SampleError(Exception):
    """Выборка решений внутренне противоречива (fail-fast, спека §3).

    Поднимается, когда шаг, посчитанный по `cbr_key_rate`, расходится с
    `decision_delta_bp` резюме ЦБ: один из двух источников (или сборка окна
    исхода) неверен, и продолжать прогон нельзя — ярлык модели оказался бы
    не тем, что решил ЦБ. Это ошибка прогона, а не строка отчёта.
    """


# --- Разбор строк: чистые функции, тестируемые на фикстурах -------------------

ONE_DAY = timedelta(days=1)


def row_date(value) -> date:
    """Дата из значения строки: `date`/`datetime` или ISO-строка ClickHouse.

    JSON-ответ SQL-транспорта отдаёт `Date`/`Date32` строками `YYYY-MM-DD`,
    CSV-фикстуры — тем же форматом; уже разобранное значение возвращается как
    есть, чтобы функцию можно было звать повторно.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def row_float(value) -> float:
    """Число из значения строки. NULL/пустая ячейка — ошибка, а не ноль.

    Молчаливый ноль на месте значения исказил бы и ставку, и признак: ноль
    ставки в 2014-м выглядел бы измеренным.
    """
    if value is None or value == "":
        raise ValueError("пустое числовое значение в ряду: NULL или пустая ячейка")
    return float(value)


def row_optional_int(value) -> Optional[int]:
    """Целое или None: NULL (`None`) и пустая ячейка CSV (`''`) — одно и то же."""
    if value is None or value == "":
        return None
    return int(value)


def meetings_from_rows(rows: list) -> list:
    """Даты канонических заседаний из строк `events_calendar`.

    Отбор: `category='cbr'` ∧ `title=SAMPLE_TITLE` (фильтр по title отсекает
    ручные строки-триггеры); дедуп и сортировка — свойство выборки, а не
    порядка строк ответа. Дедуп здесь — НОРМА, а не защита от дублей: у одного
    заседания законно лежат несколько строк (каноническая + ручной триггер), и
    без сведения по дате одна дата дала бы два «заседания».
    """
    meetings = []
    for row in rows:
        if str(row.get("category", "")) != "cbr":
            continue
        if str(row.get("title", "")) != SAMPLE_TITLE:
            continue
        meetings.append(row_date(row["event_date"]))
    return sorted(set(meetings))


def _unique_dates(points: list, series: str) -> list:
    """Отказывает на повторяющейся дате точки ряда (возвращает точки как есть).

    Дубль даты в таблице без FINAL (или с неверным ключом) означал бы ДВЕ точки
    одного дня: as-of выбрал бы одну из них по порядку сортировки, и решение
    молча зависело бы от порядка строк в ответе ClickHouse. Поэтому отказ с
    датой, а не «последний победил»: этот класс дефектов иначе не виден.
    """
    seen = set()
    for point_date, _ in points:
        if point_date in seen:
            raise ValueError(
                f"{series}: дата {point_date.isoformat()} встречается дважды — "
                f"повторная точка исказила бы as-of"
            )
        seen.add(point_date)
    return points


def key_rate_from_rows(rows: list) -> list:
    """Точки `cbr_key_rate`: `(дата действия, ставка %)`, по возрастанию даты."""
    points = [(row_date(row["date"]), row_float(row["rate"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_key_rate"))


def cpd_from_rows(rows: list) -> list:
    """Точки базового ИПЦ: `(конец месяца M, SA м/м %)`, по возрастанию даты."""
    points = [(row_date(row["date"]), row_float(row["value"])) for row in rows]
    return sorted(_unique_dates(points, CPD_CORE_SERIES))


def ibk_from_rows(rows: list) -> list:
    """Точки ИБК SA: `(1-е число отчётного месяца, пункты)`, по возрастанию."""
    points = [(row_date(row["date"]), row_float(row["value"])) for row in rows]
    return sorted(_unique_dates(points, IBK_SERIES))


def mp_deltas_from_rows(rows: list) -> list:
    """Шаги решений из `cbr_mp_summary`: `(дата заседания, б.п. или None)`.

    `None` у самого раннего резюме — «предыдущего решения в ряду нет», а не
    «шаг равен нулю»: с NULL сверка не выполняется (сверять не с чем).
    """
    points = [
        (row_date(row["meeting_date"]), row_optional_int(row["decision_delta_bp"]))
        for row in rows
    ]
    return sorted(_unique_dates(points, "cbr_mp_summary"))


# --- Загрузчики: SQL + транспорт + разбор ------------------------------------


def load_meetings() -> list:
    """Даты канонических заседаний за весь доступный ряд (с 2014-02-14)."""
    return meetings_from_rows(ch_query(MEETINGS_SQL))


def load_key_rate() -> list:
    """Дневной ряд действующих ставок `cbr_key_rate` (2013-09-17+)."""
    return key_rate_from_rows(ch_query(KEY_RATE_SQL))


def load_cpd_core() -> list:
    """Базовый ИПЦ SA, м/м, % (серия `CPD_CORE_SERIES`)."""
    return cpd_from_rows(ch_query(CPD_CORE_SQL))


def load_ibk() -> list:
    """ИБК ЦБ SA, пункты — прокси разрыва выпуска (`macro_series`)."""
    return ibk_from_rows(ch_query(IBK_SQL))


def load_mp_summary_deltas() -> list:
    """Шаги решений из резюме ЦБ (`cbr_mp_summary`, 22 резюме с 2024-02)."""
    return mp_deltas_from_rows(ch_query(MP_SUMMARY_SQL))


# --- As-of слой (спека §4) ----------------------------------------------------


def _end_of_next_month(period: date) -> date:
    """Конец месяца, следующего за месяцем даты `period`.

    Общее правило доступности обоих месячных рядов v0: точка отчётного месяца M
    становится доступной в конце M+1 (фактическая публикация — во второй декаде
    M+1; правило консервативно и намеренно НЕ point-in-time, спека §4).
    """
    year, month = period.year, period.month + 1
    if month > 12:
        year, month = year + 1, 1
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - ONE_DAY


def available_from_cpd(period_end: date) -> date:
    """Доступность точки базового ИПЦ: конец месяца M+1 (M — отчётный)."""
    return _end_of_next_month(period_end)


def available_from_survey(period_first: date) -> date:
    """Доступность точки ИБК: конец месяца M+1 (M — отчётный)."""
    return _end_of_next_month(period_first)


def _available_on_the_day(period: date) -> date:
    """Доступность ставки: в день действия лага нет."""
    return period


def asof(points: list, available: Callable[[date], date], asof_date: date) -> Optional[float]:
    """Последняя доступная на `asof_date` точка ряда, или None.

    Выбирается точка с НАИБОЛЬШЕЙ датой периода среди тех, у которых
    `available(period) ≤ asof_date` (граница включительна: точка, ставшая
    доступной ровно в дату заседания, участвует). Порядок входа не важен.
    Пустой результат — это `None`, а вызывающий решает, что это значит
    (для признаков — заседание в «пропущенных», спека §4).
    """
    candidates = [point for point in points if available(point[0]) <= asof_date]
    if not candidates:
        return None
    return max(candidates, key=lambda point: point[0])[1]


def annualize_saar(monthly_percent: float) -> float:
    """Годовой темп из месячного SA-темпа: `((1 + x/100)^12 − 1) × 100`.

    Сложное начисление, а не `×12`: месяц к месяцу — темп прироста, и годовой
    из него получается произведением двенадцати множителей (спека §4).
    """
    return ((1.0 + monthly_percent / 100.0) ** 12 - 1.0) * 100.0


# --- Выборка решений и строки признаков ---------------------------------------


@dataclass(frozen=True)
class Decision:
    """Решение заседания: уровни до/после, шаг в б.п. и класс `y3`.

    `y3` ∈ {−1 снижение, 0 сохранение, +1 повышение} — целевая переменная
    классификационных методов (спека §3/§6).
    """

    meeting_date: date
    rate_before: float
    rate_after: float
    step_bp: int
    y3: int


@dataclass(frozen=True)
class Row:
    """Строка выборки: решение и два признака, снятые as-of (спека §4)."""

    decision: Decision
    pi_saar: float
    gap: float

    @property
    def x(self) -> list:
        """Вектор регрессоров `x = [rate_before, pi_saar, gap]` (спека §4)."""
        return [self.decision.rate_before, self.pi_saar, self.gap]


def build_decisions(meetings: list, key_rate: list, mp_deltas: list) -> tuple:
    """Собирает решения по заседаниям из ряда действующих ставок (спека §3).

    Правила исхода для даты `d`:

    * `rate_before` — ставка, действующая на `d − 1` (решение публикуется в
      13:30 и вступает в силу со следующего дня; у внеочередного 2014-03-03
      ставка действует с того же дня, у внеочередного 2014-12-15 пресс-релиз
      опубликован в ночь на 16-е — `d − 1` защищает оба случая);
    * `rate_after` — ставка на `min(d + 7, next − 1)`, где `next` — следующее
      заседание СПИСКА (защита от двух решений в одном окне: 2014-12-11/15,
      2014-12-15/2015-01-30 и т.п.);
    * сверка ярлыков: для заседаний, присутствующих в `cbr_mp_summary` с
      непустым `decision_delta_bp`, шаг обязан совпасть — расхождение
      поднимает `SampleError` (fail-fast).

    Ряд ставки здесь — ЧАСЫ ВЫБОРКИ. У заседания ТРИ исхода, и они различимы:

    * решение — `rate_before` известен и окно исхода наблюдаемо целиком
      (`window_end ≤ max(date)` ряда);
    * «вне выборки» (`excluded`) — окно исхода ещё не наблюдаемо: заседание
      прошло (или назначено), но `min(d+7, next−1)` выходит за данные ряда, и
      «ставка ещё не изменилась» — не наблюдённый исход. Так же выглядит
      будущее заседание, если вызывающий передал его в списке;
    * «пропущено» (`missing`) — на `d − 1` нет точки ряда вовсе (ряд короче
      выборки или начинается позже заседания): ставку «до» взять неоткуда, и
      это дефект данных, а не незрелое окно.

    Обе причины названы разными списками намеренно: спека §3/§4 требует
    видимого счётчика «пропущено», а слитый список из двух разных причин его
    прятал. Кто именно попадает в `meetings` — решение вызывающего: живой
    прогон подаёт прошедшие заседания (решение ЦБ уже объявлено) и оставляет
    будущие в календаре.

    Возврат: `(decisions, excluded, missing)`.

    Формат входов: `meetings` — даты, `key_rate` — пары `(дата, ставка)`,
    `mp_deltas` — пары `(дата заседания, шаг в б.п. или None)`, как их отдают
    `load_key_rate` и `load_mp_summary_deltas`.
    """
    ordered = sorted(set(meetings))
    deltas = {day: delta for day, delta in mp_deltas}
    rate_points = sorted(key_rate)
    if not rate_points:
        # Пустой ряд ставки: решений нет, а каждое заседание — «пропущено»
        # (ставки «на d−1» нет ни у кого).
        return [], [], ordered
    series_max = rate_points[-1][0]

    decisions = []
    excluded = []
    missing = []
    for index, meeting in enumerate(ordered):
        rate_before = asof(rate_points, _available_on_the_day, meeting - ONE_DAY)
        if rate_before is None:
            missing.append(meeting)
            continue
        window_end = meeting + timedelta(days=OUTCOME_WINDOW_DAYS)
        if index + 1 < len(ordered):
            window_end = min(window_end, ordered[index + 1] - ONE_DAY)
        if window_end > series_max:
            excluded.append(meeting)
            continue
        rate_after = asof(rate_points, _available_on_the_day, window_end)
        step_bp = round((rate_after - rate_before) * 100.0)
        y3 = (step_bp > 0) - (step_bp < 0)

        delta = deltas.get(meeting)
        if delta is not None and int(delta) != step_bp:
            raise SampleError(
                f"заседание {meeting.isoformat()}: шаг по cbr_key_rate "
                f"{step_bp:+d} б.п. не совпал с decision_delta_bp "
                f"{int(delta):+d} б.п. из cbr_mp_summary — один из источников "
                f"неверен, прогон остановлен"
            )
        decisions.append(Decision(meeting, rate_before, rate_after, step_bp, y3))
    return decisions, excluded, missing


def build_rows(decisions: list, cpd: list, ibk: list) -> tuple:
    """Строки выборки: к каждому решению — признаки as-of (спека §4).

    `pi_saar` — годовой базовый ИПЦ из месячного SA-темпа; `gap` — ИБК SA.
    Оба снимаются на дату заседания (`asof`). Если хоть одного ряда нет —
    заседание исключается из выборки и попадает во второй список
    («пропущено»): подставлять ноль вместо неизвестного признака нельзя,
    такая строка выглядела бы измеренной.

    Возврат: `(rows, missing)`.
    """
    rows = []
    missing = []
    for decision in decisions:
        monthly = asof(cpd, available_from_cpd, decision.meeting_date)
        gap = asof(ibk, available_from_survey, decision.meeting_date)
        if monthly is None or gap is None:
            missing.append(decision.meeting_date)
            continue
        rows.append(Row(decision, annualize_saar(monthly), gap))
    return rows, missing


# --- Walk-forward и бенчмарки B1–B3 (спека §5, §7) ----------------------------


class DegenerateWindowError(ValueError):
    """Обучающее окно непригодно для оценки метода (спека §6).

    Поднимается методом walk-forward-шага, когда на прошлом нельзя оценить
    ничего осмысленного: точек меньше, чем нужно регрессии, у ordered-модели в
    окне один класс и т.п. `walk_forward` пропускает такой фолд и считает его,
    а прогон не падает: вырожденность — свойство окна, а не ошибка данных.

    Подкласс `ValueError` — чтобы вызывающий, ждущий `ValueError`, не ломался;
    но walk-forward пропускает ИМЕННО этот тип: прочие `ValueError` — ошибка
    прогона (баг кода или порча данных) и останавливают его.
    """


@dataclass(frozen=True)
class Forecast:
    """Прогноз одного фолда: шаг в БАЗИСНЫХ пунктах и вероятности классов.

    Единицы — б.п., как у факта (`Decision.step_bp`): у бенчмарка и метода
    одна шкала, иначе метрики, DM и бутстрап сравнивали бы разные величины.

    `probs` — вероятности классов в порядке (−1, 0, +1); `None` — метод
    распределения не даёт (вырожденные бенчмарки B1–B3, спека §5). Форма одна
    для бенчмарков и методов №2–3 (Tasks 4–5): walk-forward и отчёт не знают,
    вероятностный метод или нет, — это свойство самого прогноза.
    """

    step_bp: float
    probs: Optional[tuple]


# Контракт метода walk-forward: обучение на окне строк, прогноз следующей.
FitPredict = Callable[[list, Row], Forecast]


@dataclass(frozen=True)
class Prediction:
    """Фолд walk-forward: факт заседания, метод, окно и прогноз.

    `actual_step_bp`/`actual_y3` — факт заседания; он хранится рядом с
    прогнозом, чтобы метрики, тесты значимости и CSV не восстанавливали его
    по выборке повторно (и не разошлись в трактовке, если что-то сдвинется).
    """

    meeting_date: date
    actual_step_bp: float
    actual_y3: int
    method: str
    window: str
    step_bp: float
    probs: Optional[tuple]


def window_label(rolling: Optional[int]) -> str:
    """Метка окна walk-forward для отчёта и CSV: `expanding` или `rolling<N>`."""
    return WINDOW_EXPANDING if rolling is None else f"rolling{rolling}"


def predict_hold(train: list, row: Row) -> Forecast:
    """B1: «ставка не изменится» — нулевой шаг (спека §5)."""
    return Forecast(0.0, None)


def predict_repeat(train: list, row: Row) -> Forecast:
    """B2: «повтор последнего решения» — шаг предыдущего заседания окна (§5)."""
    if not train:
        raise ValueError("B2 («повтор решения»): в обучающем окне нет предыдущего решения")
    return Forecast(float(train[-1].decision.step_bp), None)


def _ols_hac(y: list, x: list):
    """OLS `y = a + b·x` с ковариацией HAC (Ньюи–Уэст, спека §6).

    Конфигурация одна на срез (`HAC_MAXLAGS`, `use_correction=True`) и
    пинована тестом: AR(1), Mincer–Zarnowitz и методы №2–3 обязаны считать
    p-value на одной ковариации, иначе строки отчёта несопоставимы.

    Возврат — результаты statsmodels (`params`, `ssr`, `f_test`): вызывающий
    берёт из них коэффициенты и совместные гипотезы.
    """
    design = sm.add_constant(np.asarray(x, dtype=float))
    model = sm.OLS(np.asarray(y, dtype=float), design)
    return model.fit(cov_type="HAC", cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True})


def fit_ar1(train: list, row: Row) -> Forecast:
    """B3: AR(1) на уровне ставки `i_t = c + φ·i_{t−1}`, OLS с HAC (спека §5).

    В обучении берутся пары «(уровень до, уровень после)» = `(i_{t−1}, i_t)`:
    между соседними заседаниями ставка не меняется, поэтому `rate_before`
    тестового заседания — это `i_{t−1}`. Уровни — в процентных пунктах, а
    прогноз шага обязан быть в БАЗИСНЫХ, как у факта и остальных прогнозов
    harness'а: `(pred_level − rate_before) × 100`.
    """
    if len(train) < MIN_OLS_OBS:
        raise DegenerateWindowError(
            f"B3 (AR(1)): обучающее окно короче {MIN_OLS_OBS} заседаний — "
            f"OLS с HAC не оценивается"
        )
    levels_prev = [sample.decision.rate_before for sample in train]
    levels_next = [sample.decision.rate_after for sample in train]
    if len(set(levels_prev)) < 2:
        # Уровень в окне не менялся: наклон φ не идентифицирован (регрессор
        # коллинеарен свободному члену), и MLE вернул бы мусор или упал.
        raise DegenerateWindowError(
            "B3 (AR(1)): уровень ставки в обучающем окне не меняется — φ не идентифицирован"
        )
    result = _ols_hac(levels_next, levels_prev)
    pred_level = float(result.params[0]) + float(result.params[1]) * row.decision.rate_before
    step_percent = pred_level - row.decision.rate_before
    return Forecast(step_percent * 100.0, None)


# Бенчмарки B1–B3 в порядке отчёта (спека §5). Строки методов №2–3 добавят
# Tasks 4–5 в этот же список: walk-forward, таблицы отчёта и CSV их не
# различают — см. `benchmark_predictions`.
BENCHMARKS = (
    ("hold", predict_hold),
    ("repeat", predict_repeat),
    ("ar1", fit_ar1),
)

# Имена бенчмарков без меток — для сравнений DM («против каждого бенчмарка»).
BENCHMARK_METHODS = tuple(method for method, _ in BENCHMARKS)

# Метки методов в отчёте: бенчмарки названы номерами спеки (§5), имена
# методов №2–3 (Tasks 4–5) идут в отчёт как есть.
METHOD_LABELS = {"hold": "B1 hold", "repeat": "B2 repeat", "ar1": "B3 AR(1)"}


def method_label(method: str) -> str:
    """Метка метода для отчёта: у бенчмарков — номер спеки, у прочих — имя."""
    return METHOD_LABELS.get(method, method)


def walk_forward(
    rows: list,
    method: str,
    fit_predict: FitPredict,
    min_train: int = MIN_TRAIN,
    rolling: Optional[int] = None,
) -> tuple:
    """Walk-forward по одному методу и одному окну (спека §7).

    Фолд `i` обучается ТОЛЬКО на прошлых заседаниях (`rows[:i]`) и прогнозирует
    `rows[i]`; первый фолд — `min_train`-е по счёту заседание. Единый
    train/test split спекой запрещён, и здесь он не выражается вовсе, а не
    отключается флагом.

    Скользящее окно входит в прогон ПОЛНЫМ: первый фолд — когда набрано
    `rolling` заседаний (`max(min_train, rolling)`), иначе «окно 40»
    начиналось бы с окна в `min_train` и окном 40 не было бы. Выборка короче
    обучения даёт `([], 0)`: отчёт этого шага обязан рендериться и без фолдов.

    Вырожденное обучающее окно (`DegenerateWindowError` от метода) — ПРОПУСК
    фолда со счётчиком, а не падение прогона: у ordered-модели в окне может
    оказаться один класс, у AR(1) — неизменный уровень, и это свойство окна, а
    не ошибка данных. Любое другое исключение метода пробрасывается: это баг
    кода или порча данных, и молча пропускать его нельзя.

    Возврат — `(фолды, пропущено)`: фолды по возрастанию даты
    (`Prediction.window` несёт метку окна, по которой отчёт группирует строки
    «метод × окно»), и число пропущенных фолдов — оно попадает в сводку отчёта
    и не даёт прочитать «метод не пропустил ничего» там, где он пропустил.
    """
    start = min_train if rolling is None else max(min_train, rolling)
    window = window_label(rolling)
    predictions = []
    skipped = 0
    for index in range(start, len(rows)):
        train = rows[:index] if rolling is None else rows[index - rolling:index]
        try:
            forecast = fit_predict(train, rows[index])
        except DegenerateWindowError:
            skipped += 1
            continue
        decision = rows[index].decision
        predictions.append(
            Prediction(
                meeting_date=decision.meeting_date,
                actual_step_bp=float(decision.step_bp),
                actual_y3=decision.y3,
                method=method,
                window=window,
                step_bp=float(forecast.step_bp),
                probs=forecast.probs,
            )
        )
    return predictions, skipped


def benchmark_predictions(rows: list, min_train: int = MIN_TRAIN) -> tuple:
    """Бенчмарки B1–B3 по обоим окнам — строки «метод × окно» отчёта.

    Оба окна считаются всегда (спека §7): отчёт показывает их рядом, а выбор
    окна по ошибке не делается. Порядок обхода — окно, затем метод; от него
    зависит порядок строк в отчёте и в CSV, и он же задаёт группировку.

    Возврат — `(фолды, пропущено)`: счётчик вырожденных окон по ключу
    «(метод, окно)» для сводки отчёта. У B1 и B2 он нулевой по построению, у
    B3 ненулевым бывает на выборке с неизменным уровнем ставки (и тогда это
    факт отчёта, а не повод уронить прогон).
    """
    predictions = []
    fold_skips = {}
    for window in (None, ROLLING_WINDOW):
        for method, fit_predict in BENCHMARKS:
            folds, skipped = walk_forward(
                rows, method, fit_predict, min_train=min_train, rolling=window
            )
            predictions.extend(folds)
            fold_skips[(method, window_label(window))] = skipped
    return predictions, fold_skips


# --- Метрики §7 ----------------------------------------------------------------


# Порядок классов решения: {−1 снижение, 0 сохранение, +1 повышение}. Он же
# порядок строк и столбцов матрицы 3×3 и вероятностей в `Forecast.probs`.
_CLASS_ORDER = (-1, 0, 1)


def _sign(step) -> int:
    """Класс направления шага: {−1, 0, +1} — та же конвенция, что у `y3`."""
    return (step > 0) - (step < 0)


def _pairs(predicted: list, actual: list, what: str) -> list:
    """Пары «прогноз, факт» с проверкой длин.

    Молчаливый `zip` по короткому списку исказил бы метрику: лишние фолды
    выпали бы из среднего, и число в отчёте было бы не про те данные. Поэтому
    расхождение длин и пустой список — ошибка прогона.
    """
    if len(predicted) != len(actual):
        raise ValueError(f"{what}: {len(predicted)} прогнозов против {len(actual)} фактов")
    if not predicted:
        raise ValueError(f"{what}: пустой список прогнозов")
    return list(zip(predicted, actual))


def _class_index(outcome, what: str) -> int:
    """Индекс класса в `_CLASS_ORDER`; класс вне {−1, 0, +1} — ошибка."""
    if outcome not in _CLASS_ORDER:
        raise ValueError(f"{what}: класс {outcome!r} вне {{−1, 0, +1}}")
    return _CLASS_ORDER.index(outcome)


def _require_distribution(triple, what: str) -> None:
    """Проверяет, что тройка — распределение вероятностей классов.

    Сумма, отличная от единицы, означает, что модель отдала не распределение;
    усреднять такое в Brier и log loss нельзя — числа отчёта были бы не про
    ту модель, что стоит рядом в таблице.
    """
    if len(triple) != len(_CLASS_ORDER) or any(
        probability < 0.0 or probability > 1.0 for probability in triple
    ):
        raise ValueError(f"{what}: {triple!r} — не вероятности классов {{−1, 0, +1}}")
    if abs(sum(triple) - 1.0) > 1e-9:
        raise ValueError(f"{what}: сумма вероятностей {sum(triple)!r} ≠ 1")


def rmse_bp(errors: list) -> float:
    """RMSE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт)."""
    if not errors:
        raise ValueError("RMSE: пустой список ошибок")
    return math.sqrt(sum(error * error for error in errors) / len(errors))


def mae_bp(errors: list) -> float:
    """MAE прогноза шага в б.п.; `errors` — ошибки (прогноз − факт)."""
    if not errors:
        raise ValueError("MAE: пустой список ошибок")
    return sum(abs(error) for error in errors) / len(errors)


def hit_rate(predicted: list, actual: list) -> float:
    """Доля совпадений направления прогноза и факта (шаги в б.п., спека §7)."""
    pairs = _pairs(predicted, actual, "hit rate")
    return sum(1 for pred, act in pairs if _sign(pred) == _sign(act)) / len(pairs)


def confusion3(predicted: list, actual: list) -> list:
    """Матрица 3×3 «факт × прогноз» по направлениям (строки — факт).

    Порядок строк и столбцов — `_CLASS_ORDER` (−1, 0, +1), то есть «снижение,
    сохранение, повышение».
    """
    matrix = [[0 for _ in _CLASS_ORDER] for _ in _CLASS_ORDER]
    for pred, act in _pairs(predicted, actual, "матрица 3×3"):
        matrix[_class_index(_sign(act), "матрица 3×3")][
            _class_index(_sign(pred), "матрица 3×3")
        ] += 1
    return matrix


def qwk(predicted: list, actual: list) -> float:
    """Quadratic weighted kappa по направлениям (шаги в б.п., спека §7).

    Классы — `_CLASS_ORDER`, квадратичные веса `w_ij = (i − j)²/(k − 1)²`;
    ожидаемая матрица — произведение маргиналов, делённое на n. Полное
    совпадение даёт 1, а постоянный прогноз при непостоянном факте — 0:
    метрика не вознаграждает отсутствие информации (так выглядит B1 на
    режимном блоке).

    Вырождение: все прогнозы и факты в одном классе — ожидаемое расхождение
    нулевое, kappa не определён. В этом случае наблюдённое расхождение тоже
    нулевое (прогноз не ошибся ни разу), и соглашение — 1.0.
    """
    pairs = _pairs(predicted, actual, "QWK")
    size = len(pairs)
    classes = len(_CLASS_ORDER)
    observed = [[0] * classes for _ in range(classes)]
    actual_counts = [0] * classes
    predicted_counts = [0] * classes
    for pred, act in pairs:
        row = _class_index(_sign(act), "QWK")
        column = _class_index(_sign(pred), "QWK")
        observed[row][column] += 1
        actual_counts[row] += 1
        predicted_counts[column] += 1
    numerator = 0.0
    denominator = 0.0
    for row in range(classes):
        for column in range(classes):
            weight = (row - column) ** 2 / (classes - 1) ** 2
            numerator += weight * observed[row][column]
            denominator += weight * actual_counts[row] * predicted_counts[column] / size
    if denominator == 0.0:
        return 1.0
    return 1.0 - numerator / denominator


def brier_multi(probs: list, y_true: list) -> float:
    """Brier (multiclass): среднее Σ_k (p_k − 1[y = k])² по фолдам.

    `probs` — вероятности классов в порядке `_CLASS_ORDER` (−1, 0, +1),
    `y_true` — классы решений.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(f"Brier: {len(probs)} прогнозов против {len(y_true)} фактов")
    total = 0.0
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "Brier")
        target = _class_index(outcome, "Brier")
        total += sum(
            (probability - (1.0 if index == target else 0.0)) ** 2
            for index, probability in enumerate(triple)
        )
    return total / len(probs)


def log_loss_multi(probs: list, y_true: list) -> float:
    """Log loss (multiclass): среднее −ln p(факт) по фолдам.

    Вероятность факта снизу ограничена `LOG_LOSS_EPS`: без порога нулевая
    вероятность дала бы бесконечность, и всё среднее «уехало» бы в inf.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(f"log loss: {len(probs)} прогнозов против {len(y_true)} фактов")
    total = 0.0
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "log loss")
        probability = triple[_class_index(outcome, "log loss")]
        total -= math.log(max(probability, LOG_LOSS_EPS))
    return total / len(probs)


@dataclass(frozen=True)
class ReliabilityBin:
    """Бин надёжностной таблицы: уверенность прогноза против точности.

    `lo`/`hi` — границы бина по вероятности argmax-класса (последний бин
    замкнут справа); `mean_prob` — средняя уверенность бина, `accuracy` —
    доля попаданий argmax-класса в факт.
    """

    lo: float
    hi: float
    count: int
    mean_prob: float
    accuracy: float


def reliability_bins(probs: list, y_true: list, n_bins: int = 5) -> list:
    """Надёжностная таблица по уверенности прогноза (спека §7).

    Фолды раскладываются по `n_bins` равным бинам вероятности argmax-класса
    («уверенности»); в каждом бине — средняя уверенность и фактическая доля
    попаданий. При уверенности 1.0 модель обязана угадывать всегда, при 0.5 —
    в половине случаев; расхождение и есть мера пере- или недоуверенности.

    Ничья argmax разрешается в пользу младшего класса (порядок
    `_CLASS_ORDER`) — у упорядоченных вероятностей она мера нулевая. Пустые
    бины в результат не попадают: у строки без наблюдений нет ни счёта, ни
    долей, и в таблице отчёта она была бы шумом.
    """
    if len(probs) != len(y_true) or not probs:
        raise ValueError(
            f"надёжностная таблица: {len(probs)} прогнозов против {len(y_true)} фактов"
        )
    if n_bins < 1:
        raise ValueError(f"надёжностная таблица: бинов {n_bins} — меньше одного")
    buckets = [[] for _ in range(n_bins)]
    for triple, outcome in zip(probs, y_true):
        _require_distribution(triple, "надёжностная таблица")
        best = max(range(len(_CLASS_ORDER)), key=lambda index: triple[index])
        confidence = triple[best]
        hit = _CLASS_ORDER[best] == outcome
        position = min(int(confidence * n_bins), n_bins - 1)
        buckets[position].append((confidence, hit))
    bins = []
    for position, bucket in enumerate(buckets):
        if not bucket:
            continue
        bins.append(
            ReliabilityBin(
                lo=position / n_bins,
                hi=(position + 1) / n_bins,
                count=len(bucket),
                mean_prob=sum(confidence for confidence, _ in bucket) / len(bucket),
                accuracy=sum(1.0 for _, hit in bucket if hit) / len(bucket),
            )
        )
    return bins


# --- Тесты значимости и бутстрап §7 -------------------------------------------


def diebold_mariano(err_a: list, err_b: list, h: int = 1) -> tuple:
    """DM-тест (поправка HLN) на равенство точности двух прогнозов (спека §7).

    Путь — библиотечный: `statsmodels.tsa.stattools.diebold_mariano_test` с
    `harvey_adj=True` (пин `statsmodels>=0.15`). Поправка HLN масштабирует
    статистику и берёт p-value из распределения Стьюдента с `T − 1` степенями
    свободы; лаги HAC — правило библиотеки `max(h − 1, ⌈T^{1/3}⌉)` (для окон
    прогона это 5 лагов при T = 68…84). Путь назван и в отчёте.

    Потери — квадраты ошибок (как у RMSE, головной метрики §7). В API
    библиотеки подаются «факт» и два прогноза, а у нас на руках ошибки,
    поэтому «фактом» идёт нулевой ряд: `(0 − err)² = err²`, и разница потерь
    `d_t = err_a² − err_b²`. Положительная статистика значит, что ошибки
    `err_a` БОЛЬШЕ, то есть метод хуже.

    Соглашение о вырождении (нулевая дисперсия разницы потерь: t-статистике
    нечем делиться): среднее 0 — в частности, одинаковые ошибки —
    `(0.0, 1.0)`; среднее ≠ 0 — `(±inf, 0.0)`: преимущество бесспорно.
    """
    if len(err_a) != len(err_b):
        raise ValueError(f"DM: {len(err_a)} ошибок против {len(err_b)}")
    if not err_a:
        raise ValueError("DM: пустой список ошибок")
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    if all(difference == differences[0] for difference in differences):
        mean = differences[0]
        if mean == 0.0:
            return (0.0, 1.0)
        return (math.copysign(math.inf, mean), 0.0)
    result = sm.tsa.stattools.diebold_mariano_test(
        [0.0] * len(err_a),
        list(err_a),
        list(err_b),
        criterion="mse",
        harvey_adj=True,
        horizon=h,
    )
    return (float(result.statistic), float(result.pvalue))


def pesaran_timmermann(pred_sign: list, actual_sign: list) -> tuple:
    """Pesaran–Timmermann по знаку прогноза и факта (спека §7).

    Путь — библиотечный: `statsmodels.stats.diagnostic.pesaran_timmermann`
    (нормальная статистика; порядок аргументов библиотеки — «факт, прогноз»,
    направление классифицируется по `> 0`, как и класс `y3`). Тест отвечает,
    зависит ли попадание по знаку от самих знаков, и вырождается, когда
    прогнозы или факты знака не меняют (так выглядит B1 «ставка не
    изменится»): библиотека поднимает `ValueError`. Соглашение, как у DM на
    одинаковых ошибках, — `(0.0, 1.0)`: направленных различий не заявлено.

    Знаки подаются уже сведёнными к {−1, 0, +1}: у нуля здесь то же значение
    «ставка не изменилась», что и у класса `y3`.
    """
    if len(pred_sign) != len(actual_sign):
        raise ValueError(f"PT: {len(pred_sign)} прогнозов против {len(actual_sign)} фактов")
    if not pred_sign:
        raise ValueError("PT: пустой список знаков")
    try:
        result = sm.stats.diagnostic.pesaran_timmermann(actual_sign, pred_sign)
    except ValueError:
        # Единственный ValueError, достижимый здесь, — «оценка дисперсии
        # неположительна»: длины и непустота проверены выше, значения — знаки,
        # а ряд короче двух точек на живом прогоне невозможен.
        return (0.0, 1.0)
    return (float(result.statistic), float(result.pvalue))


@dataclass(frozen=True)
class MincerZarnowitz:
    """Результат MZ-регрессии «факт на прогноз» (спека §7).

    `intercept`/`slope` — коэффициенты (`fact_bp = a + b·pred_bp`);
    `p_value` — совместная гипотеза `a = 0 ∧ b = 1` на HAC-ковариации.
    """

    intercept: float
    slope: float
    p_value: float


def mincer_zarnowitz(actual: list, predicted: list) -> Optional[MincerZarnowitz]:
    """Mincer–Zarnowitz: регрессия «факт на прогноз» с ковариацией HAC (§7).

    Несмещённость и точность прогноза проверяются совместно (`a = 0 ∧ b = 1`)
    F-тестом на той же HAC-конфигурации, что у AR(1) и методов №2–3.

    Вырожденный вход возвращает `None` (в отчёте — «—»), а не исключение:
    регрессия не определена, если точек меньше `MIN_OLS_OBS` или прогноз не
    варьирует (константа коллинеарна свободному члену — так выглядит B1).
    Точная подгонка (остатков нет в пределах машинной погрешности) — тоже
    вырождение: дисперсия оценки нулевая, и F-статистика не определена.
    Соглашение: гипотеза не отклоняется (p = 1.0), если выполнена точно, и
    отклоняется (p = 0.0), если нет. Без него точная подгонка при верной H0
    получала бы ложное p ≈ 0.0001 из одной лишь погрешности округления.
    """
    if len(actual) != len(predicted):
        raise ValueError(f"MZ: {len(actual)} фактов против {len(predicted)} прогнозов")
    if len(actual) < MIN_OLS_OBS or len(set(predicted)) < 2:
        return None
    result = _ols_hac(actual, predicted)
    intercept = float(result.params[0])
    slope = float(result.params[1])
    scale = max(1.0, sum(value * value for value in actual))
    if float(result.ssr) <= MZ_EXACT_RTOL * scale:
        exact = abs(intercept) <= MZ_EXACT_ATOL and abs(slope - 1.0) <= MZ_EXACT_ATOL
        return MincerZarnowitz(intercept, slope, 1.0 if exact else 0.0)
    joint = result.f_test((np.array([[1.0, 0.0], [0.0, 1.0]]), np.array([0.0, 1.0])))
    return MincerZarnowitz(intercept, slope, float(np.squeeze(joint.pvalue)))


@dataclass(frozen=True)
class BootstrapDiff:
    """Блочный бутстрап разницы RMSE: точка, 95%-интервал и p-value."""

    rmse_diff: float
    low: float
    high: float
    p_value: float


def block_bootstrap_rmse_diff(
    err_a: list, err_b: list, block: int = 4, n: int = 2000, seed: int = SEED
) -> BootstrapDiff:
    """95%-интервал и p-value разницы RMSE двух рядов ошибок (спека §7).

    Ресэмплируются пары ошибок ЦЕЛИКОМ: блок из `block` заседаний берётся
    одними и теми же индексами из обоих рядов — иначе разница RMSE размылась
    бы несвязанными точками, и интервал потерял бы парность сравнения. Блоки
    движутся по кругу, поэтому и последний блок полон.

    Квантили — по порядковым статистикам, без интерполяции: нижняя —
    `⌊0.025·n⌋`, верхняя — `⌈0.975·n⌉ − 1`.

    Конвенция p (зафиксирована до результатов): двусторонняя, по доле
    ресэмплов по разные стороны нуля,
    `p = min(1, 2·min(P(Δ* ≤ 0), P(Δ* ≥ 0)))`; нулевая разница во всех
    ресэмплах даёт p = 1.0, точное преимущество — p = 0.0.

    Генератор — собственный `random.Random(seed)`, а не глобальный `random`:
    прогон детерминирован при одном сиде и не зависит (и не влияет) на
    глобальное состояние ГПСЧ.
    """
    if len(err_a) != len(err_b):
        raise ValueError(f"бутстрап: {len(err_a)} ошибок против {len(err_b)}")
    if not err_a:
        raise ValueError("бутстрап: пустой список ошибок")
    if block < 1:
        raise ValueError(f"бутстрап: блок {block} — меньше одного заседания")
    if n < 1:
        raise ValueError(f"бутстрап: ресэмплов {n} — меньше одного")

    size = len(err_a)
    blocks_per_sample = -(-size // block)  # ⌈size/block⌉: блоков хватает на всю выборку
    generator = random.Random(seed)
    deltas = []
    for _ in range(n):
        indices = []
        for _ in range(blocks_per_sample):
            start = generator.randrange(size)
            indices.extend((start + offset) % size for offset in range(block))
        del indices[size:]
        squared_a = sum(err_a[index] * err_a[index] for index in indices)
        squared_b = sum(err_b[index] * err_b[index] for index in indices)
        deltas.append(math.sqrt(squared_a / size) - math.sqrt(squared_b / size))
    deltas.sort()
    low = deltas[int(0.025 * n)]
    high = deltas[min(n - 1, math.ceil(0.975 * n) - 1)]
    below = sum(1 for delta in deltas if delta <= 0.0) / n
    above = sum(1 for delta in deltas if delta >= 0.0) / n
    return BootstrapDiff(
        rmse_diff=rmse_bp(err_a) - rmse_bp(err_b),
        low=low,
        high=high,
        p_value=min(1.0, 2.0 * min(below, above)),
    )


# --- Отчёт и артефакты §8 -----------------------------------------------------


@dataclass(frozen=True)
class Skipped:
    """Заседания вне выборки, по причинам (спека §3/§4).

    Три списка — три разные причины, и отчёт печатает их раздельно: «вне
    выборки» (окно исхода ещё не наблюдаемо), «пропущено» без ставки на d−1 и
    «пропущено» без данных по признакам. Слитый счётчик скрывал бы, какая
    именно проверка не пройдена.
    """

    excluded: tuple
    missing_rate: tuple
    missing_features: tuple


def _predictions_by_group(predictions: list) -> dict:
    """«метод × окно» → фолды, в порядке первого появления.

    Порядок задаёт прогон (`benchmark_predictions` и строки методов №2–3), и
    от него зависят строки таблиц отчёта: сортировка по имени сделала бы
    отчёт зависимым от алфавита, а не от спецификации.
    """
    groups = {}
    for prediction in predictions:
        groups.setdefault((prediction.method, prediction.window), []).append(prediction)
    return groups


def _fold_counts(predictions: list) -> dict:
    """Число фолдов по окнам (полное — по бенчмаркам, максимум по методам)."""
    counts = {}
    for (_, window), group in _predictions_by_group(predictions).items():
        counts[window] = max(counts.get(window, 0), len(group))
    return counts


def _group_errors(group: list) -> list:
    """Ошибки фолдов (прогноз − факт) в б.п."""
    return [prediction.step_bp - prediction.actual_step_bp for prediction in group]


def _group_steps(group: list) -> tuple:
    """(прогнозы, факты) шага в б.п. для метрик и тестов значимости."""
    return (
        [prediction.step_bp for prediction in group],
        [prediction.actual_step_bp for prediction in group],
    )


def _probabilistic(group: list) -> Optional[dict]:
    """Вероятностные метрики группы или `None`, если распределений нет.

    Распределение даёт не каждый метод (у вырожденных бенчмарков его нет,
    спека §5), поэтому метрики считаются только там, где вероятности есть у
    КАЖДОГО фолда группы: усечённая группа дала бы смесь двух определений.
    Пустая группа — тоже `None`: у неё нет ни прогнозов, ни распределения.
    """
    if not group or any(prediction.probs is None for prediction in group):
        return None
    probs = [prediction.probs for prediction in group]
    outcomes = [prediction.actual_y3 for prediction in group]
    return {
        "brier": brier_multi(probs, outcomes),
        "log_loss": log_loss_multi(probs, outcomes),
        "bins": reliability_bins(probs, outcomes),
    }


def _fmt(value: Optional[float], places: int = 2) -> str:
    """Число для таблицы отчёта: `None` — «—», бесконечность — «±inf»."""
    if value is None:
        return "—"
    if math.isinf(value):
        return "+inf" if value > 0 else "−inf"
    formatted = f"{value:.{places}f}"
    # «-0.00» — артефакт округления малой отрицательной величины: знака в
    # разрядах таблицы нет, и минус читался бы как ошибка знака.
    if float(formatted) == 0.0:
        return formatted.lstrip("-")
    return formatted


def _fmt_p(pvalue: Optional[float]) -> str:
    """p-value для таблицы: менее 1e-4 — «<0.0001» (разряды округления не нужны)."""
    if pvalue is None:
        return "—"
    if pvalue < 0.0001:
        return "<0.0001"
    return f"{pvalue:.4f}"


_METRIC_HEADER = (
    "| метод | окно | n | RMSE, б.п. | MAE, б.п. | hit rate | QWK | Brier | log loss |",
    "|---|---|---|---|---|---|---|---|---|",
)
_FOLD_SUMMARY_HEADER = (
    "| метод | окно | фолдов | пропущено (вырожденное окно) |",
    "|---|---|---|---|",
)
_SIGNIFICANCE_HEADER = (
    "| метод | окно | против | ΔRMSE, б.п. | DM | DM p | 95% ДИ ΔRMSE | бутстр. p |",
    "|---|---|---|---|---|---|---|---|",
)
_MZ_PT_HEADER = (
    "| метод | окно | n | MZ a | MZ b | MZ p (a=0 ∧ b=1) | PT | PT p |",
    "|---|---|---|---|---|---|---|---|",
)
_RELIABILITY_HEADER = (
    "| метод | окно | бин уверенности | n | средняя уверенность | точность |",
    "|---|---|---|---|---|---|",
)


def _table(header: tuple, rows: list) -> list:
    """Строки таблицы markdown; без фолдов таблица не рисуется, а помечается."""
    if not rows:
        return ["_(фолдов нет.)_"]
    return list(header) + list(rows)


def _fold_summary_row(method: str, window: str, group: list, fold_skips: dict) -> str:
    """Строка сводки «метод × окно»: сделанные прогнозы и пропущенные окна.

    «фолдов» — реально сделанные прогнозы (они же `n` в таблице метрик),
    «пропущено» — вырожденные обучающие окна, на которых метод не оценился.
    """
    return (
        f"| {method_label(method)} | {window} | {len(group)} "
        f"| {fold_skips.get((method, window), 0)} |"
    )


def _metric_row(method: str, window: str, group: list) -> str:
    """Строка таблицы метрик «метод × окно» (спека §7).

    Пустая группа (в режимном блоке у метода может не быть ни одного фолда
    2024–2026) даёт строку из прочерков: метрики не определены, но строка —
    часть состава блока, и пропускать её значило бы прятать метод.
    """
    if not group:
        return f"| {method_label(method)} | {window} | 0 | — | — | — | — | — | — |"
    predicted, actual = _group_steps(group)
    errors = _group_errors(group)
    probabilistic = _probabilistic(group)
    brier = _fmt(probabilistic["brier"], 4) if probabilistic else "—"
    log_loss = _fmt(probabilistic["log_loss"], 4) if probabilistic else "—"
    return (
        f"| {method_label(method)} | {window} | {len(group)} "
        f"| {_fmt(rmse_bp(errors))} | {_fmt(mae_bp(errors))} "
        f"| {_fmt(hit_rate(predicted, actual), 3)} | {_fmt(qwk(predicted, actual), 3)} "
        f"| {brier} | {log_loss} |"
    )


def _significance_rows(groups: dict) -> list:
    """Строки DM и бутстрапа: каждая строка против каждого бенчмарка (§7).

    Сравнение метода с самим собой не выводится (это тождество «0, 1.0»), а
    бенчмарки между собой выводятся: это часть картины «кто из обязательных
    бенчмарков точнее», и спека §5 фиксирует их порядок.

    Парность: ошибки сравниваются на заседаниях МЕТОДА (у методов №2–3 фолд
    с вырожденным окном пропускается — Tasks 4–5), и у бенчмарка прогноз на
    каждом из этих заседаний обязан быть: пропуск заседания бенчмарком —
    дефект прогона, а не повод молча сравнить разные наборы.
    """
    rows = []
    for (method, window), group in groups.items():
        for benchmark in BENCHMARK_METHODS:
            if benchmark == method:
                continue
            reference = groups.get((benchmark, window))
            if reference is None:
                continue
            reference_errors = {
                prediction.meeting_date: prediction.step_bp - prediction.actual_step_bp
                for prediction in reference
            }
            dates = [prediction.meeting_date for prediction in group]
            missing = [day for day in dates if day not in reference_errors]
            if missing:
                raise ValueError(
                    f"DM: у бенчмарка «{benchmark}» ({window}) нет прогноза на "
                    f"{missing[0]} — фолды бенчмарков полны, сравнивать не с чем"
                )
            errors_a = _group_errors(group)
            errors_b = [reference_errors[day] for day in dates]
            statistic, pvalue = diebold_mariano(errors_a, errors_b)
            bootstrap = block_bootstrap_rmse_diff(errors_a, errors_b)
            rows.append(
                f"| {method_label(method)} | {window} | {method_label(benchmark)} "
                f"| {_fmt(bootstrap.rmse_diff)} | {_fmt(statistic)} | {_fmt_p(pvalue)} "
                f"| [{_fmt(bootstrap.low)}, {_fmt(bootstrap.high)}] "
                f"| {_fmt_p(bootstrap.p_value)} |"
            )
    return rows


def _mz_pt_rows(groups: dict) -> list:
    """Строки MZ и PT по методам и окнам (спека §7)."""
    rows = []
    for (method, window), group in groups.items():
        predicted, actual = _group_steps(group)
        mz = mincer_zarnowitz(actual, predicted)
        statistic, pvalue = pesaran_timmermann(
            [_sign(step) for step in predicted], [_sign(step) for step in actual]
        )
        rows.append(
            f"| {method_label(method)} | {window} | {len(group)} "
            f"| {_fmt(mz.intercept if mz else None, 3)} "
            f"| {_fmt(mz.slope if mz else None, 3)} "
            f"| {_fmt_p(mz.p_value if mz else None)} "
            f"| {_fmt(statistic, 3)} | {_fmt_p(pvalue)} |"
        )
    return rows


def _reliability_rows(groups: dict) -> list:
    """Строки надёжностной таблицы по методам, дающим вероятности (§7)."""
    rows = []
    for (method, window), group in groups.items():
        probabilistic = _probabilistic(group)
        if probabilistic is None:
            continue
        for bucket in probabilistic["bins"]:
            rows.append(
                f"| {method_label(method)} | {window} "
                f"| {_fmt(bucket.lo, 2)}–{_fmt(bucket.hi, 2)} | {bucket.count} "
                f"| {_fmt(bucket.mean_prob, 3)} | {_fmt(bucket.accuracy, 3)} |"
            )
    return rows


def render_report(
    sample: list,
    skipped: Skipped,
    predictions: list,
    forecast: Optional[str] = None,
    today: Optional[date] = None,
    fold_skips: Optional[dict] = None,
) -> str:
    """Markdown-отчёт прогона (спека §8).

    Состав: выборка и пропуски, walk-forward по двум окнам со сводкой «метод ×
    окно» (сделанные и пропущенные фолды), таблица «метод × окно» с метриками
    §7, DM (поправка HLN) против каждого бенчмарка вместе с бутстрап-интервалом
    разницы RMSE, Mincer–Zarnowitz и Pesaran–Timmermann, режимный блок
    2024–2026, надёжностная таблица (если у методов есть распределения) и
    «что не проверено».

    Строки методов №2–3 приходят сюда теми же `Prediction`, что и бенчмарки
    (Tasks 4–5): таблицы и разделы считаются по «метод × окно» и не знают имён
    методов, кроме меток в `METHOD_LABELS`. `forecast` — готовый markdown-блок
    текущего прогноза (Task 6) или `None`, если прогноза в прогоне ещё нет;
    `today` — дата прогона (по умолчанию системная), от неё зависит только
    строка версии набора данных; `fold_skips` — счётчики вырожденных окон по
    ключу «(метод, окно)» (`benchmark_predictions`), без них сводка показывает
    нули — так отчёт не приписывает методу пропусков, которых не считали.
    """
    today = today or date.today()
    fold_skips = fold_skips or {}
    groups = _predictions_by_group(predictions)
    counts = _fold_counts(predictions)
    regime_sample = [
        row for row in sample if REGIME_FROM <= row.decision.meeting_date <= REGIME_TO
    ]

    if sample:
        span = f" ({sample[0].decision.meeting_date} … {sample[-1].decision.meeting_date})"
    else:
        span = ""
    lines = [
        "# Модель ключевой ставки v0 — бэктест",
        "",
        f"Дата прогона: {today.isoformat()}. Версия набора данных v0 = дата прогона "
        f"+ размер выборки: {len(sample)} заседаний. Спека: "
        "`docs/superpowers/specs/2026-10-15-key-rate-model-v0-design.md` "
        "(§5 — бенчмарки, §7 — walk-forward/метрики/тесты, §8 — артефакты).",
        "",
        "## Выборка",
        "",
        "| Состав | Значение |",
        "|---|---|",
        f"| заседаний в выборке | {len(sample)}{span} |",
        # Пропуски ПЕРЕЧИСЛЯЮТСЯ, а не только считаются (спека §4): дата и
        # причина — то, по чему дефект данных ищется в источниках.
        f"| вне выборки (окно исхода не наблюдаемо) | {len(skipped.excluded)}"
        f"{_dates_suffix(list(skipped.excluded))} |",
        f"| пропущено (нет ставки на d−1) | {len(skipped.missing_rate)}"
        f"{_dates_suffix(list(skipped.missing_rate))} |",
        f"| пропущено (нет данных по признакам) | {len(skipped.missing_features)}"
        f"{_dates_suffix(list(skipped.missing_features))} |",
        "",
        "## Walk-forward",
        "",
        "Оба окна идут в отчёт целиком: выбор окна по ошибке не делается "
        "(спека §7). Каждый фолд переобучается только на прошлом, горизонт "
        "прогноза — 1 заседание.",
        "",
    ]
    if counts:
        lines.append(
            f"* расширяющееся окно (минимум {MIN_TRAIN} заседаний в обучении): "
            f"{counts.get(WINDOW_EXPANDING, 0)} фолдов на метод;"
        )
        lines.append(
            f"* скользящее окно {ROLLING_WINDOW} заседаний: "
            f"{counts.get(window_label(ROLLING_WINDOW), 0)} фолдов на метод."
        )
        lines += [
            "",
            "Вырожденное обучающее окно (мало точек, один класс у ordered-модели "
            "и т.п.) — фолд пропускается и считается, а не роняет прогон:",
            "",
        ]
        lines += _table(
            _FOLD_SUMMARY_HEADER,
            [
                _fold_summary_row(method, window, group, fold_skips)
                for (method, window), group in groups.items()
            ],
        )
    else:
        lines.append(
            f"* фолдов нет: выборка короче обучения (минимум {MIN_TRAIN} заседаний)."
        )

    lines += ["", "## Метрики: метод × окно", ""]
    lines += _table(
        _METRIC_HEADER,
        [_metric_row(method, window, group) for (method, window), group in groups.items()],
    )

    lines += [
        "",
        "## Тесты значимости: DM (поправка HLN) и блочный бутстрап",
        "",
        "DM: `err_a` — строка таблицы, `err_b` — бенчмарк; положительные "
        "статистика и ΔRMSE значат, что метод ХУЖЕ бенчмарка. Путь — "
        "библиотечный (`statsmodels.tsa.stattools.diebold_mariano_test`, "
        "`harvey_adj=True`); лаги HAC — правило библиотеки "
        "`max(h − 1, ⌈T^{1/3}⌉)`. Соглашение при нулевой дисперсии разницы "
        "потерь: `(0.0, 1.0)` — различий нет, `(±inf, 0.0)` — преимущество "
        f"бесспорно. Бутстрап — блочный (блок 4 заседания, 2000 ресэмплов, "
        f"сид {SEED}), p — двусторонняя доля ресэмплов по разные стороны нуля.",
        "",
    ]
    lines += _table(_SIGNIFICANCE_HEADER, _significance_rows(groups))

    lines += [
        "",
        "## Mincer–Zarnowitz и Pesaran–Timmermann",
        "",
        "MZ — регрессия «факт на прогноз» с ковариацией HAC "
        f"(maxlags={HAC_MAXLAGS}); p — совместная гипотеза `a = 0 ∧ b = 1`. "
        "PT — зависят ли попадания по знаку от самих знаков "
        "(`statsmodels.stats.diagnostic.pesaran_timmermann`); значение "
        "«0.000 / 1.0000» — тест не определён (знаки не варьируют), а «—» — "
        "регрессия MZ не определена (константный прогноз).",
        "",
    ]
    lines += _table(_MZ_PT_HEADER, _mz_pt_rows(groups))

    lines += [
        "",
        "## Режимный блок 2024–2026",
        "",
        f"Прошедших заседаний 2024–2026 в выборке: {len(regime_sample)}. "
        "Гиперпараметры на блоке не подбираются (спека §7); в тексте спеки "
        "заседаний 24 — октябрь и декабрь 2026 на дату прогона ещё будущие.",
        "",
    ]
    lines += _table(
        _METRIC_HEADER,
        [
            _metric_row(
                method,
                window,
                [  # блок — тот же расчёт на фолдах 2024–2026
                    prediction
                    for prediction in group
                    if REGIME_FROM <= prediction.meeting_date <= REGIME_TO
                ],
            )
            for (method, window), group in groups.items()
        ],
    )

    reliability_rows = _reliability_rows(groups)
    if reliability_rows:
        lines += [
            "",
            "## Надёжностная таблица",
            "",
            "Бины — по уверенности прогноза (вероятность argmax-класса); "
            "«средняя уверенность» против фактической точности бина (спека §7).",
            "",
        ]
        lines += _table(_RELIABILITY_HEADER, reliability_rows)

    if forecast is not None:
        lines += ["", "## Текущий прогноз", "", str(forecast).strip()]

    lines += [
        "",
        "## Что не проверено",
        "",
        "- **As-of — приближение, а не point-in-time слой**: ревизии "
        "объясняющих рядов и точные даты публикаций не учтены, полноценный "
        "PIT-слой — следующий пункт фазы 4 (спека §4).",
        "- **2013 год вне выборки**: страница `cal_mp` начинается с 2014 "
        "(спека §11).",
        "- **MAPE не применяется**: шаг в б.п. пересекает нуль (спека §7).",
        "- **Бенчмарки B1–B3 распределений не дают** (вырожденные): Brier, "
        "log loss и надёжностная таблица к ним не применяются (спека §5).",
        "- **Методы №2–3 и текущий прогноз на ближайшее заседание** — Tasks "
        "4–6 этого же плана; этот прогон содержит только бенчмарки.",
        "- **Гиперпараметры не подбираются**: ни на полной выборке, ни на "
        "режимном блоке 2024–2026; обе схемы окна — проверка робастности, а "
        "не выбор лучшей (спека §7).",
        "- **Консенсус-бенчмарк (медиана макроопроса)** вне v0 — из-за "
        "горизонта и первого публикационного цикла (спека §11).",
    ]
    return "\n".join(lines) + "\n"


def _probs_cell(probs) -> str:
    """Вероятности классов для CSV: «0.1|0.8|0.1»; `None` — пустая ячейка."""
    if probs is None:
        return ""
    return "|".join(_csv_cell(probability) for probability in probs)


def write_predictions(path: Path, predictions: list) -> None:
    """Пишет построчный CSV прогнозов (артефакт §8).

    Одна строка — один фолд (метод × окно × заседание); вероятности — одной
    ячейкой в порядке классов (−1, 0, +1), чтобы CSV читался и глазами, и
    pandas без разбора вложенных структур.
    """
    rows = [
        {
            "meeting_date": prediction.meeting_date.isoformat(),
            "method": prediction.method,
            "window": prediction.window,
            "actual_step_bp": prediction.actual_step_bp,
            "actual_y3": prediction.actual_y3,
            "step_bp": prediction.step_bp,
            "probs": _probs_cell(prediction.probs),
        }
        for prediction in predictions
    ]
    _write_csv(path, PREDICTION_COLUMNS, rows)


# --- Экспорт фикстур и прогон по умолчанию -------------------------------------


def _csv_cell(value) -> str:
    """Ячейка CSV для значения из JSON-ответа.

    `None` → пустая ячейка (отличимо от измеренного нуля); float — `repr`
    (полная точность, значения читаются обратно бит-в-бит); всё остальное —
    `str` (даты и целые).
    """
    if value is None:
        return ""
    if isinstance(value, float):
        return repr(value)
    return str(value)


def _write_csv(path: Path, columns: tuple, rows: list) -> None:
    """Пишет CSV с LF-переводами строк (фикстуры и артефакты прогона).

    `csv.writer` по умолчанию завершает строки CRLF, а в репозитории принят LF:
    с CRLF файл на диске расходится с тем, что лежит в git (нормализация
    `core.autocrlf=input`), и на машине без нормализации повторный экспорт
    показывал бы фикстуры изменёнными. `lineterminator` пинует LF явно.
    """
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([_csv_cell(row.get(column)) for column in columns])


def _with_fixture_extra_meetings(rows: list) -> list:
    """Добавляет к срезу `events_calendar` тестовую ручную строку-триггер.

    Строка объявлена в `FIXTURE_MEETINGS_EXTRA` и добавляется ИМЕННО экспортом:
    правка CSV руками была бы затёрта следующей регенерацией, а требование
    «регенерация даёт тот же файл» — часть контракта фикстур. Строки
    сортируются по дате, чтобы файл читался как обычный срез календаря.
    """
    extra = [
        {"event_date": day.isoformat(), "category": category, "title": title}
        for day, category, title in FIXTURE_MEETINGS_EXTRA
    ]
    return sorted(list(rows) + extra, key=lambda row: str(row.get("event_date", "")))


def export_fixtures(directory: Path) -> int:
    """Пишет пять CSV-фикстур офлайн-тестов в `directory`.

    Ряды серий — срез `[FIXTURE_FROM, FIXTURE_TO]`; список заседаний и резюме
    выгружаются целиком: список нужен вместе с ручными строками-триггерами
    (фильтр по title), а резюме малы и целиком образуют пару «ставка+дельта»
    для сверки ярлыков.

    Ноль строк в ответе — ОШИБКА ЭКСПОРТА (`RuntimeError`), а не расхождение
    выборки: пустая фикстура молча ослабила бы тесты. Все пять запросов
    выполняются и проверяются ДО первой записи, поэтому неудачный экспорт не
    оставляет каталога с половиной файлов.
    """
    directory.mkdir(parents=True, exist_ok=True)
    exports = (
        ("meetings.csv", MEETINGS_SQL),
        ("key_rate_2013_2016.csv", FIXTURE_KEY_RATE_SQL),
        ("cpd_core_2013_2016.csv", FIXTURE_CPD_CORE_SQL),
        ("ibk_2013_2016.csv", FIXTURE_IBK_SQL),
        ("mp_summary.csv", MP_SUMMARY_SQL),
    )
    planned = []
    for name, sql in exports:
        rows = ch_query(sql)
        if not rows:
            raise RuntimeError(f"фикстура {name}: запрос вернул 0 строк")
        if name == "meetings.csv":
            rows = _with_fixture_extra_meetings(rows)
        planned.append((name, FIXTURE_COLUMNS[name], rows))

    print(f"Фикстуры записаны в {directory}:")
    for name, columns, rows in planned:
        _write_csv(directory / name, columns, rows)
        first = _csv_cell(rows[0].get(columns[0]))
        last = _csv_cell(rows[-1].get(columns[0]))
        print(f"  {name:<24} {len(rows):>5} строк, {first} … {last}")
    return 0


def _dates_suffix(days: list) -> str:
    """Перечисление дат для строки сводки и таблицы отчёта.

    Пустой список — пустая строка: у пустого перечня нет ни тире, ни запятых,
    и строка «0» не превращается в «0 — ».
    """
    if not days:
        return ""
    return " — " + ", ".join(day.isoformat() for day in days)


def run_summary(rolling: bool, today: Optional[date] = None, out_dir: Path = ROOT) -> int:
    """Полный прогон: выборка, walk-forward бенчмарков, отчёт и CSV (§7/§8).

    `today` — дата, относительно которой календарь делится на прошедшие и
    предстоящие заседания (по умолчанию системная). Прошедшее заседание —
    то, решение по которому ЦБ уже объявил; наблюдаемо ли уже ЕГО ИСХОД
    (окно `min(d+7, next−1)`), решает `build_decisions` и называет в
    «вне выборки». Предстоящие в выборку решений не подаются вовсе: у них нет
    ни решения, ни исхода, и печатаются они отдельной строкой.

    `rolling` переключает ОКНО консольной таблицы метрик (`--rolling` —
    скользящее 40, по умолчанию — расширяющееся); в отчёт и CSV всегда идут
    оба окна (спека §7: выбор окна по ошибке не делается). `out_dir` — каталог
    артефактов: корень репозитория в живом прогоне, временный каталог в тестах.

    Состав выборки обязан сходиться с ожидаемыми числами приёмки (108
    заседаний, 0 пропущенных, 0 «вне выборки», диапазон 2014-02-14 … 2026-09-11).
    Методы №2–3 (Tasks 4–5) добавляются к `benchmark_predictions` одной
    строкой: отчёт, DM/MZ/PT, CSV и режимный блок считаются по «метод × окно».
    """
    today = today or date.today()
    meetings = load_meetings()
    if not meetings:
        print("events_calendar: канонических заседаний нет — проверить импорт календаря")
        return 1
    upcoming = [day for day in meetings if day > today]
    past = [day for day in meetings if day <= today]

    deltas = load_mp_summary_deltas()
    decisions, excluded, missing_rate = build_decisions(past, load_key_rate(), deltas)
    rows, missing_features = build_rows(decisions, load_cpd_core(), load_ibk())
    if not rows:
        print("выборка пуста: ни одного решения с наблюдаемым исходом и признаками")
        return 1

    delta_map = dict(deltas)
    checked = sum(
        1 for decision in decisions if delta_map.get(decision.meeting_date) is not None
    )

    predictions, fold_skips = benchmark_predictions(rows)
    report_path = out_dir / REPORT_NAME
    predictions_path = out_dir / PREDICTIONS_NAME
    report_path.write_text(
        render_report(
            rows,
            Skipped(tuple(excluded), tuple(missing_rate), tuple(missing_features)),
            predictions,
            None,
            today=today,
            fold_skips=fold_skips,
        ),
        encoding="utf-8",
    )
    write_predictions(predictions_path, predictions)

    print(f"Модель ключевой ставки v0 — выборка решений (дата прогона {today.isoformat()})")
    print(f"  канонических заседаний:                      {len(meetings)}"
          f" ({meetings[0]} … {meetings[-1]})")
    print(f"  предстоящих (позже даты прогона):            {len(upcoming)}"
          + _dates_suffix(upcoming))
    print(f"  в выборке решений:                           {len(rows)}"
          f" ({rows[0].decision.meeting_date} … {rows[-1].decision.meeting_date})")
    print(f"  вне выборки (окно исхода не наблюдаемо):     {len(excluded)}"
          + _dates_suffix(excluded))
    print(f"  пропущено (нет ставки на d−1):               {len(missing_rate)}"
          + _dates_suffix(missing_rate))
    print(f"  пропущено (нет данных по признакам):         {len(missing_features)}"
          + _dates_suffix(missing_features))
    print(f"  сверено шагов с cbr_mp_summary:              {checked} из {len(deltas)}"
          f" (у самого раннего резюме дельты нет)")

    counts = _fold_counts(predictions)
    print(f"  фолдов (расширяющееся окно, min_train={MIN_TRAIN}):   "
          f"{counts.get(WINDOW_EXPANDING, 0)}")
    print(f"  фолдов (скользящее окно, {ROLLING_WINDOW}):               "
          f"{counts.get(window_label(ROLLING_WINDOW), 0)}")
    print(f"  пропущено фолдов (вырожденное окно):         {sum(fold_skips.values())}")

    selected = window_label(ROLLING_WINDOW if rolling else None)
    print(f"  метрики окна «{selected}» (в отчёте и CSV — оба окна):")
    printed = False
    for (method, window), group in _predictions_by_group(predictions).items():
        if window != selected:
            continue
        predicted, actual = _group_steps(group)
        errors = _group_errors(group)
        print(f"    {method_label(method):<16} RMSE {rmse_bp(errors):>8.2f}  "
              f"MAE {mae_bp(errors):>8.2f}  hit {hit_rate(predicted, actual):.3f}")
        printed = True
    if not printed:
        print("    (фолдов нет — выборка короче обучения)")
    print(f"  отчёт:                                        {report_path}")
    print(f"  прогнозы:                                     {predictions_path}")
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Модель ключевой ставки: выборка решений, as-of, walk-forward, отчёт."
    )
    parser.add_argument(
        "--export-fixtures",
        metavar="DIR",
        default=None,
        help="записать CSV-фикстуры офлайн-тестов в каталог DIR и выйти",
    )
    parser.add_argument(
        "--rolling",
        action="store_true",
        help="метрики консольной сводки по скользящему окну 40 (по умолчанию — "
             "по расширяющемуся); в отчёт и CSV идут оба окна",
    )
    args = parser.parse_args(argv)
    try:
        if args.export_fixtures:
            return export_fixtures(Path(args.export_fixtures))
        return run_summary(rolling=args.rolling)
    except SampleError as exc:
        # Расхождение ярлыка со сводкой ЦБ — ожидаемая диагностируемая ошибка
        # прогона: у неё уже есть готовый текст (заседание и оба числа), и ей
        # место в stderr с ненулевым кодом, а не трейсбеком на весь экран.
        print(f"ошибка выборки решений: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
