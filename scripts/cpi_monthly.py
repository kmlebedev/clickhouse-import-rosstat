#!/usr/bin/env python3
"""CPI monthly (ML-трек v2): данные, as-of, выборка и признаки с МП.

Первый срез месячного nowcast ИПЦ РФ (спека
`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md`, §2): выборка
**155 месяцев 2013-10…2026-08** и два информационных множества на ОДНИХ И ТЕХ
ЖЕ строках — «без МП» (5 числовых + `month`) и «с МП» (плюс 7 признаков
мониторинга предприятий ЦБ из `cbr_survey`). Модели, walk-forward и отчёт —
следующие задачи; здесь только данные, as-of-правила, признаки, экспорт фикстур
и сводка выборки.

Что делает срез:

* **таргет** (§2.1) — 4 ряда `rosstat_ipc_mes` (словарь `TARGET_SERIES`),
  значение — индекс «к концу предыдущего месяца», таргет `y = percent − 100`
  (п.п., м/м, несглаженный ряд); дата точки — последний день месяца;
* **cutoff** (§2.2) — прогноз месяца `M` делается на 8-е число `M+1`, до
  публикации Росстата (выпуски месячного ИПЦ — 10–16-го); граница
  включительна: точка, ставшая доступной ровно в cutoff, участвует;
* **as-of** (§2.6) — консервативные правила доступности: `rosstat_ipc_mes` —
  конец `M+1` (дата точки — конец отчётного месяца), `cbr_currency_usd` —
  `date + 1`, `cbr_key_rate` — `date`, FAO — `date + 6`, `cbr_survey` — конец
  месяца `P+1` для месячных и квартальных точек одинаково. Следствие: на
  cutoff 8-го `M+1` МП за `M−1` уже опубликован, МП за `M` — ещё нет;
* **признаки без МП** (§2.4) — `y_lag1`/`y_lag2` (метки общего ИПЦ M−1/M−2 с
  проверкой `available_from_mes`), `usd_mm = 100·ln(mean USD за M−1 / mean USD
  за M−2)` (обе месячные корзины обязательны), `key_rate` (последняя точка
  ≤ cutoff), `fao_mm` (последняя пара доступных точек), `month` 1–12;
* **признаки с МП** (§2.5) — уровни `price_expectations_sa`,
  `business_climate_sa`, `credit_conditions` за `M−1`;
  `price_expectations_sa_chg`/`business_climate_sa_chg` = `M−1 − M−2`;
  квартальные `capacity_utilization_sa`/`headcount_expectations_sa` — последняя
  опубликованная точка ≤ cutoff, перенос вперёд;
* **выборка** (`build_sample`) — кандидаты только внутри `SAMPLE_FIRST`…
  `SAMPLE_LAST` (2013-10…2026-08; старт задаёт ключевая ставка — 2013-09-17,
  конец — последний месяц с МП). Строки ОБЩИЕ для обоих наборов: месяц без
  любого обязательного признака (включая МП) попадает в «пропущенные» с
  причиной и отсутствует в списке целиком — иначе «с МП» и «без МП»
  сравнивались бы на разных фолдах. `feature_names(with_mp=False)` выбирает
  базовые признаки, `feature_names(with_mp=True)` — базовые + 7 МП.

**NaN-политика:** в этом срезе NaN не используется — отсутствие любой
обязательной точки выбрасывает месяц с причиной. Решение о NaN для моделей
(если понадобится) принимается в следующих задачах.

**As-of — приближение, а не point-in-time слой** (спека §2.6): точные даты
публикаций и ревизии рядов не учтены; PIT-слой — отдельный пункт фазы 4.

Фикстуры офлайн-тестов (`scripts/testdata/cpi_monthly/*.csv`) пересобираются из
живого dev-ClickHouse (пользователь default) той же целью, что и прогон:

    make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"

**Скрипт ничего не пишет в ClickHouse** — только читает; транспорт (`ch_query`)
переиспользуется из `scripts/extrapolation_aisc.py`, а не копируется. Секреты
приходят только из окружения (их подставляет Makefile). Запуск — через `make`:

* `make cpi-monthly` — сводка выборки: 155 месяцев 2013-10…2026-08, пропусков 0,
  диапазоны рядов (модели — Task 2+);
* `make cpi-monthly ARGS="--start N --embargo-months K"` — параметры окна
  walk-forward принимаются и печатаются; пока не используются (моделей нет);
* `make cpi-monthly ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
"""

import argparse
import csv
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
sys.path.insert(0, str(ROOT / "scripts"))

import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# ---------------------------------------------------------------------------
# Константы (контракт с Tasks 2–4; значения пинуются тестами)
# ---------------------------------------------------------------------------

# Сид walk-forward/бутстрапа/DM и «версия набора данных» (спека §3.1):
# воспроизводимость прогона считается от одного числа, названного здесь.
SEED = 20261018

# Ряды таргета (§2.1): код → дословное имя в `rosstat_ipc_mes`. Переименование
# карточки в БД молча обнулило бы выборку, поэтому имена пинуются тестом.
TARGET_SERIES = {
    "cpi_all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
    "cpi_food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
    "cpi_nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
    "cpi_services": "Индексы потребительских цен на услуги по Российской Федерации",
}

# Ряд — «голова» y_lag1/y_lag2 и direct-модели: лаги берутся по общему ИПЦ.
TARGET_ALL = "cpi_all"

# Компоненты bottom-up (§3.2) — коды таргета §2.1; порядок задаёт веса NNLS.
BOTTOMUP_COMPONENTS = ("cpi_food", "cpi_nonfood", "cpi_services")

# Признаки МП (§2.5) — 7 имён в порядке `feature_names(with_mp=True)`: сначала
# уровни и их изменения месячных рядов, затем квартальные, затем условия
# кредитования. `_chg` — производные (M−1 − M−2), в SQL их нет.
MP_SERIES = (
    "price_expectations_sa",
    "price_expectations_sa_chg",
    "business_climate_sa",
    "business_climate_sa_chg",
    "capacity_utilization_sa",
    "headcount_expectations_sa",
    "credit_conditions",
)

# Исходные ряды `macro_series` source='cbr_survey', которые реально читает SQL:
# 5 рядов. Состав фиксирован до результатов (спека §2.5); `price_growth_expected`
# и `staff_availability` не входят — они сдвинули бы старт выборки на 2020.
MP_SOURCE_SERIES = (
    "price_expectations_sa",
    "business_climate_sa",
    "capacity_utilization_sa",
    "headcount_expectations_sa",
    "credit_conditions",
)

# Месячные ряды МП: уровень берётся за M−1, изменение — M−1 − M−2.
MP_MONTHLY = ("price_expectations_sa", "business_climate_sa", "credit_conditions")
# Квартальные ряды МП: последняя опубликованная точка ≤ cutoff, перенос вперёд.
MP_QUARTERLY = ("capacity_utilization_sa", "headcount_expectations_sa")
# Признак изменения → исходный месячный ряд (названия фиксированы спекой §2.5).
MP_CHANGES = {
    "price_expectations_sa": "price_expectations_sa_chg",
    "business_climate_sa": "business_climate_sa_chg",
}

# Ряд FAO (§2.4 п. 5) — дословно: дата точки — 1-е число месяца публикации,
# значение — индекс за предыдущий месяц.
FAO_SERIES = "Food Price Index"

# Границы выборки (спека §2.3): старт — max(старты рядов)+1 месяц (позже всех
# начинается ключевая ставка, 2013-09-17), конец — последний месяц с МП
# (2026-08). Граница ОБЩАЯ для наборов «с МП» и «без МП»: иначе они
# сравнивались бы на разных фолдах.
SAMPLE_FIRST = date(2013, 10, 1)
SAMPLE_LAST = date(2026, 8, 1)

# Окна срезов-фикстур офлайн-тестов: 2013-01…2014-12 — горизонт начала выборки
# (2013-10 требует 2013-08/2013-09), 2026-07…2026-09 — свежая граница.
FIXTURE_MAIN = (date(2013, 1, 1), date(2014, 12, 31))
FIXTURE_RECENT = (date(2026, 7, 1), date(2026, 9, 30))

# Окно walk-forward по умолчанию (спека §4.1: старт 24); Task 1 флаги только
# принимает и печатает — модели появятся в Task 2.
DEFAULT_START = 24
DEFAULT_EMBARGO = 0

ONE_DAY = timedelta(days=1)

# ---------------------------------------------------------------------------
# SQL: колонки те же, что у фикстурных запросов ниже; суффикс FINAL обязателен
# (таблицы — ReplacingMergeTree: без него вытесненные копии строк дали бы
# дубли точек). `FORMAT JSON` — требование транспорта `ch_query`.
# ---------------------------------------------------------------------------

TARGETS_SQL = """
SELECT name, date, percent
FROM rosstat_ipc_mes FINAL
WHERE name IN (%s)
ORDER BY name, date
FORMAT JSON
""" % ", ".join(f"'{name}'" for name in TARGET_SERIES.values())

SURVEY_SQL = """
SELECT series, date, value
FROM macro_series FINAL
WHERE source = 'cbr_survey' AND series IN (%s)
ORDER BY series, date
FORMAT JSON
""" % ", ".join(f"'{name}'" for name in MP_SOURCE_SERIES)

USD_SQL = """
SELECT date, price
FROM cbr_currency_usd FINAL
ORDER BY date
FORMAT JSON
"""

KEY_RATE_SQL = """
SELECT date, rate
FROM cbr_key_rate FINAL
ORDER BY date
FORMAT JSON
"""

FAO_SQL = f"""
SELECT name, date, price_index
FROM fao_food_price_Indices_monthly FINAL
WHERE name = '{FAO_SERIES}'
ORDER BY date
FORMAT JSON
"""


def _window_sql(sql: str, d_from: date, d_to: date) -> str:
    """Тот же запрос, но в окне `[d_from, d_to]` по колонке `date`.

    Окно вставляется в `WHERE` готового запроса, а не дублирует его текст:
    колонки, `FINAL` и фильтры по рядам обязаны совпадать у живого и фикстурного
    запроса, иначе фикстура проверяла бы не то, что читает прогон.
    """
    window = f"date >= '{d_from.isoformat()}' AND date <= '{d_to.isoformat()}'"
    if "\nWHERE " in sql:
        return sql.replace("\nWHERE ", f"\nWHERE {window} AND ", 1)
    return sql.replace("\nORDER BY ", f"\nWHERE {window}\nORDER BY ", 1)


# Колонки CSV-фикстур — контракт с тестами: те же имена, что у колонок SELECT
# (строки фикстур читаются теми же `*_from_rows`, что и ответы ch_query).
FIXTURE_COLUMNS = {
    "ipc_mes_2013_2014.csv": ("name", "date", "percent"),
    "survey_2013_2014.csv": ("series", "date", "value"),
    "usd_2013_2014.csv": ("date", "price"),
    "key_rate_2013_2014.csv": ("date", "rate"),
    "fao_2013_2014.csv": ("name", "date", "price_index"),
    "ipc_mes_recent.csv": ("name", "date", "percent"),
    "survey_recent.csv": ("series", "date", "value"),
    "usd_recent.csv": ("date", "price"),
    "key_rate_recent.csv": ("date", "rate"),
    "fao_recent.csv": ("name", "date", "price_index"),
}

# ---------------------------------------------------------------------------
# Разбор строк: чистые функции, тестируемые на фикстурах
# ---------------------------------------------------------------------------


def row_date(value) -> date:
    """Дата из значения строки: `date`/`datetime` или ISO-строка ClickHouse.

    JSON-ответ транспорта отдаёт `Date`/`Date32` строками `YYYY-MM-DD`,
    CSV-фикстуры — тем же форматом; уже разобранное значение возвращается как
    есть, чтобы функцию можно было звать повторно. `datetime` приводится к дате
    ЯВНО и первым: он — подкласс `date`, и без этой ветки суточное время
    протащилось бы в ключ месяца незамеченным.
    """
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def row_float(value) -> float:
    """Число из значения строки. NULL/пустая ячейка — ошибка, а не ноль.

    Молчаливый ноль на месте значения исказил бы и метку, и признак: ноль
    месячного ИПЦ выглядел бы измеренным.
    """
    if value is None or value == "":
        raise ValueError("пустое числовое значение в ряду: NULL или пустая ячейка")
    return float(value)


def _unique_dates(points: list, series: str) -> list:
    """Отказывает на повторяющейся дате точки ряда (возвращает точки как есть).

    Дубль даты в таблице без FINAL (или с неверным ключом) означал бы ДВЕ точки
    одного дня: as-of выбрал бы одну по порядку сортировки, и признак молча
    зависел бы от порядка строк в ответе ClickHouse. Поэтому отказ с датой, а
    не «последний победил».
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


def targets_from_rows(rows: list) -> dict[str, list[tuple[date, float]]]:
    """Таргеты из строк `rosstat_ipc_mes`: код → `[(конец месяца, y)]`.

    `y = percent − 100` (п.п., §2.1) — здесь же, чтобы ни один потребитель не
    мог случайно взять индекс вместо темпа. Строки чужих рядов игнорируются
    (SQL уже фильтрует `IN`, фильтр в Python — страховка от чужой фикстуры).
    Пустой ряд хотя бы одного из четырёх кодов — ошибка: молча укороченный
    таргет дал бы выборку не той длины.
    """
    parsed = {code: [] for code in TARGET_SERIES}
    by_name = {name: code for code, name in TARGET_SERIES.items()}
    for row in rows:
        code = by_name.get(str(row.get("name", "")))
        if code is None:
            continue
        parsed[code].append((row_date(row["date"]), row_float(row["percent"]) - 100.0))
    for code, points in parsed.items():
        if not points:
            raise ValueError(
                f"{code}: пустой ряд таргета ({TARGET_SERIES[code]!r}) — проверить импорт"
            )
    return {
        code: sorted(_unique_dates(points, TARGET_SERIES[code]))
        for code, points in parsed.items()
    }


def survey_from_rows(rows: list) -> dict[str, list[tuple[date, float]]]:
    """Ряды МП из `macro_series`: series → `[(отчётный период, значение)]`.

    Фильтр по `source='cbr_survey'` уже в SQL; здесь — страховка по списку
    `MP_SOURCE_SERIES` (чужие ряды источника игнорируются) и отказ на дубле
    даты. Пустой ряд любого из пяти обязательных рядов — ошибка: признак,
    собранный на неполном наборе, молча сместил бы выборку.
    """
    parsed = {series: [] for series in MP_SOURCE_SERIES}
    for row in rows:
        series = str(row.get("series", ""))
        if series not in parsed:
            continue
        parsed[series].append((row_date(row["date"]), row_float(row["value"])))
    for series, points in parsed.items():
        if not points:
            raise ValueError(
                f"cbr_survey/{series}: пустой ряд МП — проверить импорт"
            )
    return {
        series: sorted(_unique_dates(points, f"cbr_survey/{series}"))
        for series, points in parsed.items()
    }


def usd_from_rows(rows: list) -> list[tuple[date, float]]:
    """Курс USD из `cbr_currency_usd`: `[(день, руб./USD)]`, по возрастанию."""
    points = [(row_date(row["date"]), row_float(row["price"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_currency_usd"))


def key_rate_from_rows(rows: list) -> list[tuple[date, float]]:
    """Ключевая ставка из `cbr_key_rate`: `[(день вступления, %)]`."""
    points = [(row_date(row["date"]), row_float(row["rate"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_key_rate"))


def fao_from_rows(rows: list) -> list[tuple[date, float]]:
    """Индексы FAO ряда `FAO_SERIES`: `[(дата публикации, индекс)]`.

    Дата точки — 1-е число месяца ПУБЛИКАЦИИ, значение — индекс за предыдущий
    месяц (так хранит импортёр); это же правило закреплено тестом. Чужие ряды
    таблицы (Cereals, Dairy, ...) игнорируются.
    """
    points = [
        (row_date(row["date"]), row_float(row["price_index"]))
        for row in rows
        if str(row.get("name", FAO_SERIES)) == FAO_SERIES
    ]
    return sorted(_unique_dates(points, FAO_SERIES))


# --- Загрузчики: SQL + транспорт + разбор ------------------------------------


def load_targets() -> dict[str, list[tuple[date, float]]]:
    """Таргеты `rosstat_ipc_mes` (4 ряда, y = percent − 100)."""
    return targets_from_rows(ch_query(TARGETS_SQL))


def load_survey() -> dict[str, list[tuple[date, float]]]:
    """Ряды МП `macro_series` source='cbr_survey' (5 исходных рядов)."""
    return survey_from_rows(ch_query(SURVEY_SQL))


def load_usd() -> list[tuple[date, float]]:
    """Дневной курс USD/руб. (`cbr_currency_usd`)."""
    return usd_from_rows(ch_query(USD_SQL))


def load_key_rate() -> list[tuple[date, float]]:
    """Дневной ряд действующих ставок `cbr_key_rate` (2013-09-17+)."""
    return key_rate_from_rows(ch_query(KEY_RATE_SQL))


def load_fao() -> list[tuple[date, float]]:
    """Индексы FAO ряда `FAO_SERIES` (месячно; дата — 1-е число публикации)."""
    return fao_from_rows(ch_query(FAO_SQL))


# ---------------------------------------------------------------------------
# Календарная арифметика месяцев
# ---------------------------------------------------------------------------


def _month_shift(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`.

    Одна точка арифметики для всех окон: конец месяца, предыдущий месяц,
    доступность метки. Дублировать `replace(day=1)+timedelta` по коду опасно —
    именно на границах декабря такие формулы и ломаются.
    """
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_of(day: date) -> date:
    """Первое число месяца дня `day` — канонический ключ месяца выборки."""
    return day.replace(day=1)


def month_end(day: date) -> date:
    """Последний день месяца дня `day` (дата точки месячных рядов)."""
    return _month_shift(day, 1) - ONE_DAY


def previous_month(day: date) -> date:
    """Первое число предыдущего месяца."""
    return _month_shift(day, -1)


def cutoff_for(month: date) -> date:
    """Cutoff месяца `M` (§2.2): 8-е число `M+1` (запас до публикации 10–16-го).

    `month` — первое число месяца (каноническая форма `Row.month`); день
    внутри месяца значения не имеет.
    """
    return _month_shift(month, 1).replace(day=8)


# ---------------------------------------------------------------------------
# As-of слой (спека §2.6)
# ---------------------------------------------------------------------------


def available_from_mes(period_end: date) -> date:
    """Доступность месячного ИПЦ: конец месяца `M+1` (M — отчётный).

    `period_end` — конец отчётного месяца (дата точки `rosstat_ipc_mes`).
    Правило консервативно: фактическая публикация — 10–16-е числа `M+1`, но
    для cutoff(M) = 8-е `M+1` точка месяца `M` недоступна — и это ровно то, что
    нужно: собственная метка тестового месяца в признаки не попадает.
    """
    return _month_shift(period_end, 2) - ONE_DAY


def available_from_usd(day: date) -> date:
    """Доступность курса: `date + 1`."""
    return day + ONE_DAY


def available_from_rate(day: date) -> date:
    """Доступность ставки: в день вступления в силу лага нет."""
    return day


def available_from_fao(day: date) -> date:
    """Доступность точки FAO: `date + 6` (публикация в первые дни месяца)."""
    return day + 6 * ONE_DAY


def available_from_survey(day: date) -> date:
    """Доступность точки МП: конец месяца `P+1` для точки `P`.

    Консервативно и одинаково для месячных и квартальных рядов (§2.6): точная
    дата публикации внутри следующего месяца не хранится; такая граница может
    задержать признак на дни, но не пропускает будущее. Точка `P` — 1-е число
    месяца, поэтому `month_of(P) + 1 месяц` и его конец и есть конец `P+1`.
    """
    return month_end(_month_shift(day, 1))


def asof(points: list, available, on: date) -> Optional[float]:
    """Последняя доступная на `on` точка ряда `[(дата, значение)]`, или None.

    Выбирается точка с НАИБОЛЬШЕЙ датой среди тех, у которых
    `available(дата) ≤ on` (граница включительна: точка, ставшая доступной
    ровно в `on`, участвует). Порядок входа не важен; пустой результат — None,
    и вызывающий решает, что это значит (месяц в «пропущенных»).
    """
    candidates = [point for point in points if available(point[0]) <= on]
    if not candidates:
        return None
    return max(candidates, key=lambda point: point[0])[1]


# ---------------------------------------------------------------------------
# Выборка: строки признаков
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Row:
    """Строка выборки: месяц, метки четырёх рядов и признаки обоих наборов.

    `month` — первое число месяца M; `y` — таргеты §2.1 (уже `percent − 100`,
    п.п.) по кодам `TARGET_SERIES`; `features` — ОБЪЕДИНЕНИЕ базовых и МП
    признаков (§2.4–2.5), порядок задают `feature_names(False/True)`. Строка
    одна на оба информационных множества — это и держит фолды одинаковыми.
    """

    month: date
    y: dict[str, float]
    features: dict[str, float | int]


def target_index(targets: dict) -> dict[str, dict[date, float]]:
    """Индекс таргета: код ряда → `{конец месяца: y}` (быстрый доступ по месяцу)."""
    return {code: {day: value for day, value in points} for code, points in targets.items()}


def sample_months(targets: dict) -> list[date]:
    """Месяцы, где наблюдаются ВСЕ ряды таргета, по возрастанию (первое число).

    Пересечение дат четырёх рядов, а не даты одного: месяц без полной метки в
    выборку не попадает (ни `y`, ни `y_lag1` для следующего месяца из него не
    собрать). Месяц точки — месяц её конца (`2013-10-31` → `2013-10`).
    """
    months = None
    for points in targets.values():
        dates = {month_of(day) for day, _ in points}
        months = dates if months is None else months & dates
    if not months:
        return []
    return sorted(months)


def usd_mm(month: date, usd: list) -> Optional[float]:
    """`usd_mm` §2.4 п. 3: `100·ln(mean USD за M−1 / mean USD за M−2)`.

    Среднее — дневных значений `cbr_currency_usd` за КАЛЕНДАРНЫЙ месяц; обе
    корзины обязательны (иначе None — месяц пропускается). Точки вне M−1/M−2
    (включая сам M) в средние не попадают.
    """
    current = previous_month(month)
    base = previous_month(current)
    baskets = {current: [], base: []}
    for day, value in usd:
        key = month_of(day)
        if key in baskets:
            baskets[key].append(value)
    if not baskets[current] or not baskets[base]:
        return None
    return 100.0 * math.log(
        statistics.mean(baskets[current]) / statistics.mean(baskets[base])
    )


def fao_mm(fao: list, cutoff: date) -> Optional[float]:
    """`fao_mm` §2.4 п. 5: `%` м/м последней пары доступных точек, или None.

    Значение точки — индекс за месяц ПЕРЕД месяцем её даты (дата — 1-е число
    месяца публикации); поэтому м/м последней доступной точки — прирост между
    индексами двух соседних месяцев, а обе точки лежат под датами их
    публикаций. Доступность — `date + 6`. Меньше двух доступных точек — не м/м:
    месяц пропускается.
    """
    eligible = [point for point in fao if available_from_fao(point[0]) <= cutoff]
    if len(eligible) < 2:
        return None
    previous, latest = eligible[-2][1], eligible[-1][1]
    if previous == 0:
        return None
    return (latest / previous - 1.0) * 100.0


def base_features(
    month: date,
    targets_index: dict,
    usd: list,
    key_rate: list,
    fao: list,
    cutoff: date,
) -> tuple[Optional[dict[str, float | int]], str]:
    """Базовые признаки месяца `M` (§2.4) или `(None, причина пропуска)`.

    `cutoff` передаётся явно (для месяца — `cutoff_for(month)`, для живого
    режима — дата прогона), но правила арифметики окон от него не зависят.
    `y_lag1`/`y_lag2` — метки общего ИПЦ за `M−1`/`M−2` с проверкой
    `available_from_mes ≤ cutoff`; `usd_mm` — средние за `M−1`/`M−2`;
    `key_rate` — последняя точка ≤ cutoff (`asof`); `fao_mm` — последняя пара
    доступных точек. Отсутствие любой обязательной точки — причина пропуска.
    """
    lag1_month = previous_month(month)
    lag2_month = previous_month(lag1_month)
    values = targets_index.get(TARGET_ALL, {})
    lag1 = values.get(month_end(lag1_month))
    if lag1 is None:
        return None, "нет метки общего ИПЦ за M−1 (y_lag1)"
    if available_from_mes(month_end(lag1_month)) > cutoff:
        return None, "метка M−1 не опубликована к cutoff (y_lag1)"
    lag2 = values.get(month_end(lag2_month))
    if lag2 is None:
        return None, "нет метки общего ИПЦ за M−2 (y_lag2)"
    if available_from_mes(month_end(lag2_month)) > cutoff:
        return None, "метка M−2 не опубликована к cutoff (y_lag2)"
    usd_value = usd_mm(month, usd)
    if usd_value is None:
        return None, "нет месячных средних USD за M−1 и M−2 (usd_mm)"
    rate = asof(key_rate, available_from_rate, cutoff)
    if rate is None:
        return None, "нет доступной ключевой ставки на cutoff (key_rate)"
    fao_value = fao_mm(fao, cutoff)
    if fao_value is None:
        return None, "нет пары доступных точек FAO (fao_mm)"
    return {
        "y_lag1": lag1,
        "y_lag2": lag2,
        "usd_mm": usd_value,
        "key_rate": rate,
        "fao_mm": fao_value,
        "month": month.month,
    }, ""


def mp_features(
    month: date, survey: dict, cutoff: date
) -> tuple[Optional[dict[str, float]], str]:
    """Признаки МП месяца `M` (§2.5) или `(None, причина пропуска)`.

    Месячные ряды: уровень — ровно точка `M−1`, изменение — `M−1 − M−2`; обе
    точки обязательны и проверяются на `available_from_survey ≤ cutoff`.
    Квартальные: последняя опубликованная точка ≤ cutoff, перенос вперёд до
    следующей опубликованной (точка месяца `M` на cutoff `M+1` не участвует:
    её доступность — конец `M+1`, позже cutoff). Отсутствие любой обязательной
    точки — причина пропуска (не NaN: молчаливое заполнение сместило бы
    информационное множество).
    """
    levels: dict[str, float] = {}
    for series in MP_MONTHLY:
        points = dict(survey.get(series, []))
        level_date = previous_month(month)
        level = points.get(level_date)
        if level is None:
            return None, f"нет МП {series} за M−1"
        if available_from_survey(level_date) > cutoff:
            return None, f"МП {series} за M−1 не опубликован к cutoff"
        levels[series] = level
        change = MP_CHANGES.get(series)
        if change is not None:
            base_date = previous_month(level_date)
            base = points.get(base_date)
            if base is None:
                return None, f"нет МП {series} за M−2 (для {change})"
            if available_from_survey(base_date) > cutoff:
                return None, f"МП {series} за M−2 не опубликован к cutoff (для {change})"
            levels[change] = level - base
    for series in MP_QUARTERLY:
        eligible = [
            point
            for point in survey.get(series, [])
            if available_from_survey(point[0]) <= cutoff
        ]
        if not eligible:
            return None, f"нет опубликованной квартальной точки {series}"
        levels[series] = max(eligible, key=lambda point: point[0])[1]
    return {name: levels[name] for name in MP_SERIES}, ""


def feature_names(with_mp: bool) -> list[str]:
    """Порядок признаков: базовые §2.4 (5 числовых), затем МП §2.5, `month`.

    Порядок — часть контракта с Tasks 2–4 (матрица признаков, monotone-
    ограничения), поэтому пинуется тестом. `month` — последним в обоих наборах.
    """
    names = ["y_lag1", "y_lag2", "usd_mm", "key_rate", "fao_mm"]
    if with_mp:
        names.extend(MP_SERIES)
    names.append("month")
    return names


def build_sample(
    targets: dict, survey: dict, usd: list, key_rate: list, fao: list
) -> tuple[list[Row], list[tuple[date, str]]]:
    """Собирает выборку `([Row], [(месяц, причина)])` (спека §2.3–2.6).

    Кандидаты — месяцы с полной меткой (`sample_months`), только внутри
    `SAMPLE_FIRST`…`SAMPLE_LAST`. Месяц, у которого не собирается ЛЮБОЙ
    обязательный признак (базовый или МП), попадает в «пропущенные» с причиной,
    а прогон не падает.

    Строки ОБЩИЕ для наборов «с МП» и «без МП»: у месяца без МП нет строки
    вовсе, а не «строка только в базовом наборе» — иначе фолды разъехались бы.
    `y` — все четыре метки §2.1; собственный таргет месяца в признаки не
    попадает (cutoff(M) строго раньше публикации метки M).
    """
    index = target_index(targets)
    rows: list[Row] = []
    skipped: list[tuple[date, str]] = []
    for month in sample_months(targets):
        if month < SAMPLE_FIRST or month > SAMPLE_LAST:
            continue
        cutoff = cutoff_for(month)
        features, reason = base_features(month, index, usd, key_rate, fao, cutoff)
        if features is None:
            skipped.append((month, reason))
            continue
        mp_part, reason = mp_features(month, survey, cutoff)
        if mp_part is None:
            skipped.append((month, reason))
            continue
        y = {code: values[month_end(month)] for code, values in index.items()}
        rows.append(Row(month=month, y=y, features={**features, **mp_part}))
    return rows, skipped


def assert_months_purged(rows: list[Row]) -> None:
    """Структурная purge-проверка выборки (спека §2.6–2.7).

    Для каждого тестового месяца `M`:

    1. cutoff(M) = 8-е число `M+1` строго раньше публикации его собственной
       метки (`available_from_mes(конец M)` = конец `M+1`) — модель не видит
       собственный таргет;
    2. точка МП самого месяца `M` на cutoff(M) недоступна (её `available_from`
       — конец `M+1`), а метки `M−1`/`M−2` к cutoff уже опубликованы;
    3. окна измерения меток разных месяцев не пересекаются: метка месяца `M'`
       измеряется в `[начало M', конец M']`, а признаки тестового месяца
       начинаются не раньше начала `M`.

    Проверка структурная (арифметика окон по `month`, данные не читаются):
    она падает при мутации `cutoff_for`/`available_from_mes` и на выборке с
    повторяющимся месяцем, а не «проверяет себя» на заведомо верных данных.
    """
    months = {row.month for row in rows}
    if len(months) != len(rows):
        raise AssertionError("purge: в выборке есть повторяющийся месяц")
    for row in rows:
        month = month_of(row.month)
        label_available = available_from_mes(month_end(month))
        if cutoff_for(month) >= label_available:
            raise AssertionError(
                f"purge: cutoff {cutoff_for(month).isoformat()} месяца "
                f"{month.isoformat()} не раньше публикации его метки "
                f"({label_available.isoformat()}) — утечка собственного таргета"
            )
        if available_from_survey(month) <= cutoff_for(month):
            raise AssertionError(
                f"purge: точка МП месяца {month.isoformat()} доступна на его "
                f"cutoff {cutoff_for(month).isoformat()} — утечка МП месяца M"
            )
        lag1 = previous_month(month)
        if lag1 in months and available_from_mes(month_end(lag1)) > cutoff_for(month):
            raise AssertionError(
                f"purge: метка {lag1.isoformat()} (y_lag1 месяца "
                f"{month.isoformat()}) не опубликована к cutoff"
            )
        lag2 = previous_month(lag1)
        if lag2 in months and available_from_mes(month_end(lag2)) > cutoff_for(month):
            raise AssertionError(
                f"purge: метка {lag2.isoformat()} (y_lag2 месяца "
                f"{month.isoformat()}) не опубликована к cutoff"
            )
    for row in rows:
        for other in rows:
            if other.month < row.month and month_end(other.month) >= row.month:
                raise AssertionError(
                    f"purge: окно метки {other.month.isoformat()} пересекается с "
                    f"началом тестового месяца {row.month.isoformat()}"
                )


# ---------------------------------------------------------------------------
# Экспорт фикстур
# ---------------------------------------------------------------------------


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
    с CRLF файл на диске расходился бы с тем, что лежит в git (нормализация
    `core.autocrlf=input`), и повторный экспорт показывал бы фикстуры
    изменёнными. `lineterminator` пинует LF явно.
    """
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(columns)
        for row in rows:
            writer.writerow([_csv_cell(row.get(column)) for column in columns])


def export_fixtures(directory: Path) -> int:
    """Пишет десять CSV-фикстур офлайн-тестов в `directory`.

    Срезы рядов — окна `FIXTURE_MAIN` (2013-01…2014-12) и `FIXTURE_RECENT`
    (2026-07…2026-09): те же запросы, что читает прогон, только с `WHERE date`;
    полный список файлов и колонок — `FIXTURE_COLUMNS`. Названия файлов —
    фактическое окно содержимого.

    Пустое окно — ошибка экспорта (пустая фикстура молча ослабила бы тесты).
    Все запросы выполняются и проверяются ДО первой записи, поэтому неудачный
    экспорт не оставляет каталога с половиной файлов.
    """
    directory.mkdir(parents=True, exist_ok=True)
    exports = (
        ("ipc_mes_2013_2014.csv", _window_sql(TARGETS_SQL, *FIXTURE_MAIN)),
        ("survey_2013_2014.csv", _window_sql(SURVEY_SQL, *FIXTURE_MAIN)),
        ("usd_2013_2014.csv", _window_sql(USD_SQL, *FIXTURE_MAIN)),
        ("key_rate_2013_2014.csv", _window_sql(KEY_RATE_SQL, *FIXTURE_MAIN)),
        ("fao_2013_2014.csv", _window_sql(FAO_SQL, *FIXTURE_MAIN)),
        ("ipc_mes_recent.csv", _window_sql(TARGETS_SQL, *FIXTURE_RECENT)),
        ("survey_recent.csv", _window_sql(SURVEY_SQL, *FIXTURE_RECENT)),
        ("usd_recent.csv", _window_sql(USD_SQL, *FIXTURE_RECENT)),
        ("key_rate_recent.csv", _window_sql(KEY_RATE_SQL, *FIXTURE_RECENT)),
        ("fao_recent.csv", _window_sql(FAO_SQL, *FIXTURE_RECENT)),
    )
    planned = []
    for name, sql in exports:
        rows = ch_query(sql)
        if not rows:
            raise RuntimeError(f"фикстура {name}: запрос вернул 0 строк")
        planned.append((name, FIXTURE_COLUMNS[name], rows))

    print(f"Фикстуры записаны в {directory}:")
    for name, columns, rows in planned:
        _write_csv(directory / name, columns, rows)
        first = _csv_cell(rows[0].get("date"))
        last = _csv_cell(rows[-1].get("date"))
        print(f"  {name:<26} {len(rows):>5} строк, {first} … {last}")
    return 0


# ---------------------------------------------------------------------------
# Сводка прогона (Task 1: данные, as-of, выборка и признаки)
# ---------------------------------------------------------------------------


def _field(label: str, value: str) -> None:
    """Строка сводки: подпись и значение (подписи пинуются тестами)."""
    print(f"  {label:<34}{value}")


def _plural(count: int, one: str, few: str, many: str) -> str:
    """Форма существительного по числу (155 месяцев, 24 месяца, 1 точка)."""
    if count % 100 in (11, 12, 13, 14):
        return many
    tail = count % 10
    if tail == 1:
        return one
    if 2 <= tail <= 4:
        return few
    return many


def _month_range(months: list) -> str:
    """Диапазон месяцев `[YYYY-MM … YYYY-MM]` (месяцы — первое число)."""
    if not months:
        return "нет месяцев"
    return f"{months[0].isoformat()[:7]} … {months[-1].isoformat()[:7]}"


def _range(points: list) -> str:
    """Диапазон дат набора точек `[(дата, ...)]` — `д0 … д1`."""
    if not points:
        return "нет точек"
    return f"{points[0][0].isoformat()} … {points[-1][0].isoformat()}"


# Сколько пропущенных месяцев печатать построчно: у сломанного импорта их может
# быть сотни, и простыня на экран не нужна — число и одна строка «и ещё N».
MAX_SKIPPED_PRINTED = 12


def run_summary(start: int = DEFAULT_START, embargo_months: int = DEFAULT_EMBARGO) -> int:
    """Сводка Task 1: диапазоны рядов, выборка, пропуски, признаки, purge.

    `start`/`embargo_months` — параметры окна walk-forward (`--start`,
    `--embargo-months`): в Task 1 они принимаются и печатаются, но модели
    появятся в Task 2. Скрипт ничего не пишет в ClickHouse и не создаёт
    артефактов прогона.
    """
    targets = load_targets()
    survey = load_survey()
    usd = load_usd()
    key_rate = load_key_rate()
    fao = load_fao()
    rows, skipped = build_sample(targets, survey, usd, key_rate, fao)
    assert_months_purged(rows)

    months = sample_months(targets)
    survey_points = sorted(
        point for points in survey.values() for point in points
    )
    print("CPI monthly (ML-трек v2) — данные, as-of, выборка и признаки")
    _field(
        "таргет rosstat_ipc_mes:",
        f"{len(TARGET_SERIES)} {_plural(len(TARGET_SERIES), 'ряд', 'ряда', 'рядов')}, "
        f"{_month_range(months)} "
        f"({len(months)} {_plural(len(months), 'месяц', 'месяца', 'месяцев')})",
    )
    _field(
        "МП cbr_survey:",
        f"{len(MP_SOURCE_SERIES)} "
        f"{_plural(len(MP_SOURCE_SERIES), 'ряд', 'ряда', 'рядов')}, "
        f"{_range(survey_points)} ({len(survey_points)} "
        f"{_plural(len(survey_points), 'точка', 'точки', 'точек')})",
    )
    _field("курс cbr_currency_usd:", _range(usd))
    _field("ключевая ставка cbr_key_rate:", _range(key_rate))
    _field("FAO Food Price Index:", _range(fao))
    if rows:
        _field(
            "выборка:",
            f"{len(rows)} {_plural(len(rows), 'месяц', 'месяца', 'месяцев')} "
            f"({rows[0].month.isoformat()[:7]} … {rows[-1].month.isoformat()[:7]}), "
            f"пропущено {len(skipped)}",
        )
    else:
        _field("выборка:", f"пусто, пропущено {len(skipped)}")
    _field("признаки (без МП):", ", ".join(feature_names(with_mp=False)))
    _field("признаки (с МП):", ", ".join(feature_names(with_mp=True)))
    _field("purge-проверка (структурная):", "OK")
    if skipped:
        print("  пропущенные месяцы и причины:")
        for month, reason in skipped[:MAX_SKIPPED_PRINTED]:
            print(f"    {month.isoformat()[:7]} — {reason}")
        hidden = len(skipped) - MAX_SKIPPED_PRINTED
        if hidden > 0:
            print(f"    … и ещё {hidden} {_plural(hidden, 'месяц', 'месяца', 'месяцев')}")
    _field("--start:", f"{start} (принят; walk-forward — Task 2)")
    _field("--embargo-months:", f"{embargo_months} (принят; walk-forward — Task 2)")
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI monthly (ML-трек v2): выборка, as-of, базовые и МП "
                    "признаки месячного ИПЦ; модели — Task 2+."
    )
    parser.add_argument(
        "--export-fixtures",
        metavar="DIR",
        default=None,
        help="записать CSV-фикстуры офлайн-тестов в каталог DIR и выйти",
    )
    parser.add_argument(
        "--start",
        type=int,
        metavar="N",
        default=DEFAULT_START,
        help=f"минимальное обучающее окно walk-forward в месяцах (по умолчанию "
             f"{DEFAULT_START}; в Task 1 принимается, но не используется)",
    )
    parser.add_argument(
        "--embargo-months",
        type=int,
        metavar="K",
        default=DEFAULT_EMBARGO,
        help=f"эмбарго walk-forward в месяцах (по умолчанию {DEFAULT_EMBARGO}; в "
             f"Task 1 принимается, но не используется)",
    )
    args = parser.parse_args(argv)
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    return run_summary(start=args.start, embargo_months=args.embargo_months)


if __name__ == "__main__":
    sys.exit(main())
