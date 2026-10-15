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

Чего здесь ЕЩЁ НЕТ (следующие шаги плана): бенчмарки B1–B3, методы №2–3,
walk-forward, метрики, отчёт и текущий прогноз — их добавляют Tasks 3–6 в этот
же файл. Поэтому прогон по умолчанию печатает сводку выборки и «что» пропущено.

**Скрипт ничего не пишет в ClickHouse** — только читает (прецедент
`polyus_output_model.py`); транспорт (`ch_query`) импортируется из
`scripts/extrapolation_aisc.py`, а не копируется: там уже есть маскирование
секретов в текстах ошибок, HTTP-порт 8123 и обработка обрывов соединения.
Секреты приходят только из окружения (их подставляет Makefile).

Запуск — только через `make` (переменные окружения подключает Makefile):

* `make key-rate-model` — сводка выборки на живых данных;
* `make key-rate-model ARGS="--export-fixtures scripts/testdata/key_rate"` —
  пересборка CSV-фикстур офлайн-тестов `scripts/key_rate_model_test.py`.

**As-of — приближение, а не point-in-time слой** (спека §4): ревизии
объясняющих рядов и точные даты публикаций не учтены, полноценный PIT-слой —
следующий пункт фазы 4.
"""

import argparse
import csv
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

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


def _write_fixture(path: Path, columns: tuple, rows: list) -> None:
    """Пишет CSV-фикстуру с LF-переводами строк.

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
        _write_fixture(directory / name, columns, rows)
        first = _csv_cell(rows[0].get(columns[0]))
        last = _csv_cell(rows[-1].get(columns[0]))
        print(f"  {name:<24} {len(rows):>5} строк, {first} … {last}")
    return 0


def _dates_suffix(days: list) -> str:
    """Перечисление дат для строки сводки: пустой список — пустая строка."""
    if not days:
        return ""
    return " — " + ", ".join(day.isoformat() for day in days)


def run_summary(rolling: bool, today: Optional[date] = None) -> int:
    """Прогон по умолчанию: сводка выборки решений и «что» пропущено.

    `today` — дата, относительно которой календарь делится на прошедшие и
    предстоящие заседания (по умолчанию системная). Прошедшее заседание —
    то, решение по которому ЦБ уже объявил; наблюдаемо ли уже ЕГО ИСХОД
    (окно `min(d+7, next−1)`), решает `build_decisions` и называет в
    «вне выборки». Предстоящие в выборку решений не подаются вовсе: у них нет
    ни решения, ни исхода, и печатаются они отдельной строкой.

    На этом шаге бэктеста ещё нет (его добавляют Tasks 3–6): печатается состав
    выборки — то, что обязано сойтись с ожидаемыми числами приёмки (108
    заседаний, 0 пропущенных, 0 «вне выборки», диапазон 2014-02-14 … 2026-09-11).
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
    window = "скользящее 40 (--rolling)" if rolling else "расширяющееся (по умолчанию)"

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
    print(f"  окно walk-forward:                           {window}")
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Первый срез модели ключевой ставки: выборка решений, as-of, признаки."
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
        help="walk-forward по скользящему окну 40 (по умолчанию — расширяющееся)",
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
