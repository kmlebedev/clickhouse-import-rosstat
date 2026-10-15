#!/usr/bin/env python3
"""CPI-nowcast (ML-трек контура ставки): выборка, as-of-правила и признаки.

Первый срез ML-трека (спека `docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md`
§2): nowcast месячного ИПЦ РФ до публикации Росстата. Модели, walk-forward,
бенчмарки и артефакты — Tasks 3–5 этой ветки; здесь — только данные.

Что делает срез:

* **таргет** (§2.1) — 4 ряда `rosstat_ipc_mes` (словарь `TARGET_SERIES`),
  значение — индекс «к концу предыдущего месяца», таргет `y = percent − 100`
  (п.п., м/м, несглаженный ряд); дата точки — последний день месяца. Выборка
  ML-трека — с 2022-01: глубже нет недельных признаков;
* **cutoff** (§2.2) — прогноз месяца `M` делается на 8-е число `M+1`, до
  публикации Росстата (выпуски месячного ИПЦ — 10–16-го);
* **as-of** (§2.4) — консервативные правила доступности: точка участвует в
  признаках месяца `M`, только если её `available_from(дата) ≤ cutoff(M)`.
  Граница включительна: точка, ставшая доступной ровно в cutoff, участвует;
* **признаки** (§2.3) — фиксированный набор из 9 числовых + `month`
  (`feature_names`): `share_rising_wk`, `median_wk`, `median_wk4`,
  `gasoline_4w` (недельные данные считаются по позициям — агрегата «все
  товары» в источнике нет), `usd_20d`, `key_rate`, `ofz_1y`, `fao_mm`,
  `y_lag1`, `month`. Направления/состав не тюнингуются;
* **выборка** (`build_sample`) — месяцы таргета от первого месяца недельных
  данных до последнего месяца со свежим недельным окном; месяц, у которого не
  собирается признак, попадает в «пропущенные» (второй элемент результата) с
  причиной, а прогон не падает.

**Ограничение dev-БД по ОФЗ (важно).** У `ofz_curve` тенор `1y` содержит одну
дату (2026-10-09): G-curve МосБиржи (`zcyc`) — снимок текущего дня, история
накапливается с первого запуска импортёра (ARCHITECTURE.md, `moex/`). Поэтому
ни у одного месяца бэктеста нет доступной к cutoff точки `1y`. Чтобы отсутствие
одного «медленного» ряда не обнуляло выборку целиком, `ofz_1y` при отсутствии
точки записывается как `NaN` (`float("nan")`), а месяц НЕ выбрасывается; число
таких месяцев сводка печатает отдельной строкой. As-of при этом не нарушается:
точка, опубликованная позже cutoff, в признаки не попадает никогда. С появлением
истории признак наполняется сам, без правок кода.

**As-of — приближение, а не point-in-time слой** (спека §2.4): точные даты
публикаций и ревизии рядов не учтены; PIT-слой — отдельный пункт фазы 4.

Фикстуры офлайн-тестов (`scripts/testdata/cpi_nowcast/*.csv`) пересобираются из
живого dev-ClickHouse (пользователь default) той же целью, что и прогон:

    make cpi-nowcast ARGS="--export-fixtures scripts/testdata/cpi_nowcast"

`ofz_2021_2022.csv` при этом получается пустым (только заголовок): окно
2021–2022 у G-curve пусто (см. ограничение выше). Это осознанное предупреждение,
а не ошибка экспорта — файл пишется, регенерация детерминирована.

**Скрипт ничего не пишет в ClickHouse** — только читает; транспорт (`ch_query`)
переиспользуется из `scripts/extrapolation_aisc.py`, а не копируется. Секреты
приходят только из окружения (их подставляет Makefile). Запуск — через `make`:

* `make cpi-nowcast` — сводка выборки (диапазоны рядов, месяцы, пропуски,
  purge-проверка); модели и артефакты — Tasks 3–5;
* `make cpi-nowcast ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
"""

import argparse
import csv
import math
import statistics
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
sys.path.insert(0, str(ROOT / "scripts"))

import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# ---------------------------------------------------------------------------
# Константы (контракт с Tasks 3–6; значения пинуются тестами)
# ---------------------------------------------------------------------------

# Сид бутстрапа/DM и «версия набора данных» (спека §4): воспроизводимость
# прогона считается от одного числа, названного здесь и больше нигде.
SEED = 20261016

# Ряды таргета (§2.1): код → дословное имя в `rosstat_ipc_mes`. Переименование
# карточки в БД молча обнулило бы выборку, поэтому имена пинуются тестом.
TARGET_SERIES = {
    "cpi_all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
    "cpi_food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
    "cpi_nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
    "cpi_services": "Индексы потребительских цен на услуги по Российской Федерации",
}

# Ряд — «голова» y_lag1: лаг берётся по общему ИПЦ (§2.3 п. 9), компонентные
# ряды для моделей bottom-up живут в `Row.y` и лаг им не подменяет признак.
TARGET_ALL = "cpi_all"

# Позиция недельного ИПЦ для признака `gasoline_4w` (§2.3 п. 4) — дословно.
WEEKLY_GASOLINE = "Бензин автомобильный"

# Ряд FAO и тенор ОФЗ (§2.3 пп. 7–8) — дословно/точно.
FAO_SERIES = "Food Price Index"
OFZ_TENOR = "1y"

# Длины окон признаков: число последних недельных публикаций (§2.3 пп. 1–4) и
# число наблюдений курса в `usd_20d` (§2.3 п. 5: `USD_t / USD_{t−20}` — t и
# точка двадцать наблюдений назад, то есть 21 точка включительно).
WEEKLY_WINDOW_WEEKS = 4
USD_WINDOW_OBS = 20

ONE_DAY = timedelta(days=1)

# Окна срезов-фикстур офлайн-тестов: имя файла = фактическое окно содержимого.
# Недельные срезы — 2022 (месяцы после 2022-05 попадают в горизонт выборки и
# проверяют правило «нет свежих недельных публикаций → месяц не строится») и
# свежий (2026-09-14…2026-10-05 — четыре публикации к cutoff сентября-2026).
FIXTURE_WEEKLY_2022 = (date(2021, 12, 1), date(2022, 5, 31))
FIXTURE_WEEKLY_RECENT = (date(2026, 9, 14), date(2026, 10, 5))
# Остальные ряды — 2021–2022: 2021-12 нужен как y_lag1 первого месяца выборки
# (2022-01), 2022 — как её же месяцы.
FIXTURE_MAIN = (date(2021, 1, 1), date(2022, 12, 31))

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

WEEKLY_SQL = """
SELECT name, date, percent
FROM rosstat_ipc_weeks FINAL
ORDER BY date, name
FORMAT JSON
"""

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

OFZ_SQL = f"""
SELECT date, tenor, yield
FROM ofz_curve FINAL
WHERE tenor = '{OFZ_TENOR}'
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
    "weekly_2022.csv": ("name", "date", "percent"),
    "weekly_recent.csv": ("name", "date", "percent"),
    "ipc_mes_2021_2022.csv": ("name", "date", "percent"),
    "usd_2021_2022.csv": ("date", "price"),
    "ofz_2021_2022.csv": ("date", "tenor", "yield"),
    "fao_2021_2022.csv": ("name", "date", "price_index"),
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
    недельного ИПЦ выглядел бы измеренным.
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


def weekly_from_rows(rows: list) -> list[tuple[str, date, float]]:
    """Недельный ИПЦ из строк `rosstat_ipc_weeks`: `[(позиция, дата, percent)]`.

    Значение — индекс к предыдущей дате регистрации (100 = без изменения);
    к п.п. его приводят признаки (`percent − 100`), а не загрузчик: срез
    фикстуры остаётся срезом таблицы. Порядок — по (дата, позиция): группировка
    по дате в признаках не зависит от порядка ответа.
    """
    points = [
        (str(row["name"]), row_date(row["date"]), row_float(row["percent"]))
        for row in rows
    ]
    seen = set()
    for name, day, _ in points:
        if (name, day) in seen:
            raise ValueError(
                f"rosstat_ipc_weeks: {name!r} на {day.isoformat()} встречается дважды"
            )
        seen.add((name, day))
    return sorted(points, key=lambda point: (point[1], point[0]))


def usd_from_rows(rows: list) -> list[tuple[date, float]]:
    """Курс USD из `cbr_currency_usd`: `[(день, руб./USD)]`, по возрастанию."""
    points = [(row_date(row["date"]), row_float(row["price"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_currency_usd"))


def key_rate_from_rows(rows: list) -> list[tuple[date, float]]:
    """Ключевая ставка из `cbr_key_rate`: `[(день вступления, %)]`."""
    points = [(row_date(row["date"]), row_float(row["rate"])) for row in rows]
    return sorted(_unique_dates(points, "cbr_key_rate"))


def ofz_from_rows(rows: list) -> list[tuple[date, float]]:
    """Доходности ОФЗ тензора `OFZ_TENOR`: `[(биржевой день, % годовых)]`.

    Фильтр по тенору в SQL уже есть; здесь он повторён страховкой: чужая
    строка кривой (3y/5y/10y/RGBI) в выборке исказила бы признак незаметно.
    """
    points = [
        (row_date(row["date"]), row_float(row["yield"]))
        for row in rows
        if str(row.get("tenor", OFZ_TENOR)) == OFZ_TENOR
    ]
    return sorted(_unique_dates(points, f"ofz_curve/{OFZ_TENOR}"))


def fao_from_rows(rows: list) -> list[tuple[date, float]]:
    """Индексы FAO ряда `FAO_SERIES`: `[(дата публикации, индекс)]`.

    Дата точки — 1-е число месяца ПУБЛИКАЦИИ, значение — индекс за предыдущий
    месяц (так хранит импортёр: `date = mes.AddDate(0, 1, 0)`); это же правило
    закреплено тестом. Чужие ряды таблицы (Cereals, Dairy, ...) игнорируются.
    """
    points = [
        (row_date(row["date"]), row_float(row["price_index"]))
        for row in rows
        if str(row.get("name", FAO_SERIES)) == FAO_SERIES
    ]
    return sorted(_unique_dates(points, FAO_SERIES))


# --- Загрузчики: SQL + транспорт + разбор ------------------------------------


def load_targets() -> dict[str, list[tuple[date, float]]]:
    """Таргеты `rosstat_ipc_mes` (4 ряда, y = percent − 100, 1991-01+)."""
    return targets_from_rows(ch_query(TARGETS_SQL))


def load_weekly() -> list[tuple[str, date, float]]:
    """Недельный ИПЦ `rosstat_ipc_weeks` (131 позиция, 2022-01-10+)."""
    return weekly_from_rows(ch_query(WEEKLY_SQL))


def load_usd() -> list[tuple[date, float]]:
    """Дневной курс USD/руб. (`cbr_currency_usd`, 2013+)."""
    return usd_from_rows(ch_query(USD_SQL))


def load_key_rate() -> list[tuple[date, float]]:
    """Дневной ряд действующих ставок `cbr_key_rate` (2013-09-17+)."""
    return key_rate_from_rows(ch_query(KEY_RATE_SQL))


def load_ofz_1y() -> list[tuple[date, float]]:
    """Доходности ОФЗ 1 года (`ofz_curve`, тенор `1y`; история — с 2026-10-09)."""
    return ofz_from_rows(ch_query(OFZ_SQL))


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
# As-of слой (спека §2.4)
# ---------------------------------------------------------------------------


def available_from_weekly(day: date) -> date:
    """Доступность недельной точки: `date + 2` (публикация Росстата по средам)."""
    return day + 2 * ONE_DAY


def available_from_usd(day: date) -> date:
    """Доступность курса: `date + 1`."""
    return day + ONE_DAY


def available_from_rate(day: date) -> date:
    """Доступность ставки: в день вступления в силу лага нет."""
    return day


def available_from_ofz(day: date) -> date:
    """Доступность точки ОФЗ: `date + 1`."""
    return day + ONE_DAY


def available_from_fao(day: date) -> date:
    """Доступность точки FAO: `date + 6` (публикация в первые дни месяца)."""
    return day + 6 * ONE_DAY


def available_from_mes(period_end: date) -> date:
    """Доступность месячного ИПЦ: конец месяца `M+1` (M — отчётный).

    `period_end` — конец отчётного месяца (дата точки `rosstat_ipc_mes`).
    Правило консервативно: фактическая публикация — 10–16-е числа `M+1`, но
    для cutoff(M) = 8-е `M+1` точка месяца `M` недоступна — и это ровно то, что
    нужно: собственная метка тестового месяца в признаки не попадает.
    """
    return _month_shift(period_end, 2) - ONE_DAY


def asof(points: list, available: Callable[[date], date], on: date) -> Optional[float]:
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


class DegenerateWindowError(ValueError):
    """Обучающее окно фолда вырождено (Tasks 3–4: walk-forward, веса, модели).

    Поднимается там, где окно обучения не позволяет оценить модель (пустое
    окно, нет вариации нужного ряда, неидентифицируемый дизайн), и
    walk-forward пропускает фолд со счётчиком, а не роняет прогон.
    """


@dataclass(frozen=True)
class Row:
    """Строка выборки: месяц, метки четырёх рядов и признаки.

    `month` — первое число месяца M; `y` — таргеты §2.1 (уже `percent − 100`,
    п.п.) по кодам `TARGET_SERIES`; `features` — признаки §2.3 (порядок
    задаёт `feature_names()`), у `month` — число 1–12. Значение `NaN` у
    `ofz_1y` — «точки нет» (см. ограничение dev-БД в docstring модуля).
    """

    month: date
    y: dict[str, float]
    features: dict[str, float | int]


def eligible_weeks(weekly: list, cutoff: date) -> list[tuple[date, dict[str, float]]]:
    """Недели, опубликованные к `cutoff`, сгруппированные по дате регистрации.

    Возврат — `[(дата, {позиция: percent})]` по возрастанию даты. Фильтр — по
    правилу §2.4 (`date + 2 ≤ cutoff`), граница включительна: публикация ровно
    в cutoff участвует.
    """
    by_date = {}
    for name, day, percent in weekly:
        if available_from_weekly(day) > cutoff:
            continue
        by_date.setdefault(day, {})[name] = percent
    return sorted(by_date.items())


def weekly_features(weekly: list, cutoff: date) -> Optional[dict[str, float]]:
    """Недельные признаки §2.3 пп. 1–4 или None, если окно неполно.

    Берутся `WEEKLY_WINDOW_WEEKS` последних опубликованных недель:

    * `share_rising_wk` — доля позиций с `percent > 100` на последней неделе;
    * `median_wk` — медиана `(percent − 100)` на той же неделе;
    * `median_wk4` — среднее недельных медиан за окно;
    * `gasoline_4w` — сумма `(percent − 100)` позиции `WEEKLY_GASOLINE` за окно.

    None — окно неполно (меньше четырёх публикаций или у бензина нет точки в
    какой-то из недель окна): месяц уходит в «пропущенные», а не считается по
    неполному окну. Порядок недель — календарный; пропуск регистрации (праздник)
    не добирается более старой точкой сверх окна.
    """
    weeks = eligible_weeks(weekly, cutoff)
    if len(weeks) < WEEKLY_WINDOW_WEEKS:
        return None
    window = weeks[-WEEKLY_WINDOW_WEEKS:]
    last_changes = [percent - 100.0 for percent in window[-1][1].values()]
    medians = [
        statistics.median([percent - 100.0 for percent in positions.values()])
        for _, positions in window
    ]
    gasoline = []
    for _, positions in window:
        if WEEKLY_GASOLINE not in positions:
            return None
        gasoline.append(positions[WEEKLY_GASOLINE] - 100.0)
    return {
        "share_rising_wk": sum(1 for change in last_changes if change > 0) / len(last_changes),
        "median_wk": statistics.median(last_changes),
        "median_wk4": sum(medians) / len(medians),
        "gasoline_4w": sum(gasoline),
    }


def usd_features(usd: list, cutoff: date) -> Optional[float]:
    """`usd_20d` §2.3 п. 5 или None, если глубины не хватает.

    `100 × ln(USD_t / USD_{t−20})`, `t` — последняя доступная к cutoff точка
    (`date + 1 ≤ cutoff`), `USD_{t−20}` — точка на `USD_WINDOW_OBS` наблюдений
    раньше `t` (21-я с конца включительно). Именно наблюдение (шаг РЯДА), а не
    календарные 20 дней: у курса выходные и праздники, и «дата t−20 дней» часто
    не существует вовсе. Точка, опубликованная позже cutoff, не участвует —
    ни как `t`, ни как база.
    """
    eligible = [point for point in usd if available_from_usd(point[0]) <= cutoff]
    if len(eligible) < USD_WINDOW_OBS + 1:
        return None
    latest = eligible[-1][1]
    base = eligible[-1 - USD_WINDOW_OBS][1]
    return 100.0 * math.log(latest / base)


def rate_feature(key_rate: list, cutoff: date) -> Optional[float]:
    """`key_rate` §2.3 п. 6: ставка, действующая на cutoff (без лага)."""
    return asof(key_rate, available_from_rate, cutoff)


def ofz_feature(ofz: list, cutoff: date) -> Optional[float]:
    """`ofz_1y` §2.3 п. 7: последняя точка ≤ cutoff (`date + 1 ≤ cutoff`).

    None означает «доступной точки нет» — вызывающий решает, что это значит
    (для dev-БД это норма: G-curve без истории, см. docstring модуля).
    """
    return asof(ofz, available_from_ofz, cutoff)


def fao_features(fao: list, cutoff: date) -> Optional[float]:
    """`fao_mm` §2.3 п. 8: % м/м последней доступной точки, или None.

    Значение точки — индекс за месяц ПЕРЕД месяцем её даты (дата — 1-е число
    месяца публикации); поэтому м/м последней доступной точки — это прирост
    между индексами двух соседних месяцев, а обе точки лежат под датами их
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


def target_index(targets: dict) -> dict[str, dict[date, float]]:
    """Индекс таргета: код ряда → `{конец месяца: y}` (быстрый доступ по месяцу)."""
    return {code: {day: value for day, value in points} for code, points in targets.items()}


def sample_months(targets: dict) -> list[date]:
    """Месяцы, где наблюдаются ВСЕ ряды таргета, по возрастанию (первое число).

    Пересечение дат четырёх рядов, а не даты одного: месяц без полной метки в
    выборку не попадает (ни `y`, ни `y_lag1` для следующего месяца из него не
    собрать). Месяц точки — месяц её конца (`1991-01-31` → `1991-01`).
    """
    months = None
    for points in targets.values():
        dates = {month_of(day) for day, _ in points}
        months = dates if months is None else months & dates
    if not months:
        return []
    return sorted(months)


def month_features(
    month: date, index: dict, weekly: list, usd: list, key_rate: list, ofz: list, fao: list
) -> tuple[Optional[dict[str, float | int]], str]:
    """Признаки месяца `M` (первое число) или `(None, причина пропуска)`.

    Порядок проверок — от «ряда целиком нет» к деталям. Свежесть недельного
    окна проверяется отдельно от полноты: все `WEEKLY_WINDOW_WEEKS` недель окна
    обязаны быть опубликованы ПОСЛЕ конца месяца `M−1`. Иначе окно либо не
    описывает месяц `M` вовсе (недельные данные кончились), либо задевает
    период метки предыдущего месяца (данные стояли) — такой месяц не строится,
    и причина называет это прямо. Сдвиг старой недели в окно вместо свежей
    поймал бы не cutoff, а именно эта проверка (тест на фикстуре).

    `ofz_1y` — единственный признак, отсутствие которого НЕ выбрасывает месяц:
    в dev-БД у G-curve нет истории, и NaN здесь честнее пустой выборки (см.
    ограничение в docstring модуля). Остальные отсутствия — причина пропуска.
    """
    cutoff = cutoff_for(month)
    fresh_from = month_end(previous_month(month))
    weeks = eligible_weeks(weekly, cutoff)
    if len(weeks) < WEEKLY_WINDOW_WEEKS:
        return None, "неполное недельное окно (меньше 4 доступных публикаций)"
    if weeks[-WEEKLY_WINDOW_WEEKS][0] <= fresh_from:
        return None, f"устаревшее недельное окно (публикации не позже {fresh_from.isoformat()})"
    weekly_part = weekly_features(weekly, cutoff)
    if weekly_part is None:
        return None, "неполное недельное окно (нет бензина в одной из недель)"
    usd_value = usd_features(usd, cutoff)
    if usd_value is None:
        return None, "нет глубины USD (нужна 21 доступная точка)"
    rate = rate_feature(key_rate, cutoff)
    if rate is None:
        return None, "нет доступной ставки на cutoff"
    fao_value = fao_features(fao, cutoff)
    if fao_value is None:
        return None, "нет пары доступных точек FAO"
    lag_month = previous_month(month)
    lag = index.get(TARGET_ALL, {}).get(month_end(lag_month))
    if lag is None or available_from_mes(month_end(lag_month)) > cutoff:
        return None, "нет опубликованной к cutoff метки месяца M−1 (y_lag1)"
    ofz_value = ofz_feature(ofz, cutoff)
    features = {
        **weekly_part,
        "usd_20d": usd_value,
        "key_rate": rate,
        "ofz_1y": ofz_value if ofz_value is not None else float("nan"),
        "fao_mm": fao_value,
        "y_lag1": lag,
        "month": month.month,
    }
    return features, ""


def build_sample(
    targets: dict, weekly: list, usd: list, key_rate: list, ofz: list, fao: list
) -> tuple[list[Row], list[date]]:
    """Собирает выборку `([Row], [пропущенные месяцы])` (спека §2.2–2.4).

    Кандидаты — месяцы с полной меткой (`sample_months`), от первого месяца
    недельных данных (глубже признаков нет — так отсекается вся история до
    2022-01) до последнего месяца, покрытого недельными публикациями. Месяц
    внутри этого диапазона, у которого признак не собирается, попадает в
    «пропущенные» — с причиной у вызывающего (`month_features`), прогон при
    этом не падает: пустое окно у одного ряда — не повод терять остальные.

    Порядок строк — календарный, метки — все четыре ряда (§2.1), признаки —
    §2.3. Собственный таргет месяца в признаки не попадает: cutoff(M) строго
    раньше публикации метки M (`available_from_mes`), а метка M−1 для `y_lag1`
    к cutoff уже опубликована.
    """
    index = target_index(targets)
    if not weekly:
        return [], sample_months(targets)
    first_week_month = month_of(min(day for _, day, _ in weekly))
    last_covered_month = month_of(max(day for _, day, _ in weekly))
    rows, skipped = [], []
    for month in sample_months(targets):
        if month < first_week_month or month > last_covered_month:
            continue
        features, reason = month_features(month, index, weekly, usd, key_rate, ofz, fao)
        if features is None:
            skipped.append(month)
            continue
        y = {code: values[month_end(month)] for code, values in index.items()}
        rows.append(Row(month=month, y=y, features=features))
    return rows, skipped


def feature_names() -> list[str]:
    """Порядок признаков §2.3 (9 числовых + категориальный `month`).

    Порядок — часть контракта с Tasks 3–4 (матрица признаков и monotone-
    ограничения собираются по нему), поэтому пинуется тестом.
    """
    return [
        "share_rising_wk",
        "median_wk",
        "median_wk4",
        "gasoline_4w",
        "usd_20d",
        "key_rate",
        "ofz_1y",
        "fao_mm",
        "y_lag1",
        "month",
    ]


def assert_months_purged(rows: list) -> None:
    """Структурная purge-проверка выборки (спека §2.4; план — Review Focus 2).

    Для каждого тестового месяца `M`:

    1. cutoff(M) = 8-е число `M+1` строго раньше публикации его собственной
       метки (`available_from_mes(конец M)` = конец `M+1`) — модель не видит
       собственный таргет;
    2. метки обучающих месяцев `M' < M` измерены внутри своих месяцев и
       опубликованы не позже конца `M'+1 ≤` конца `M`, то есть до cutoff(M) —
       к моменту теста они законно доступны;
    3. окна измерения меток разных месяцев не пересекаются: метка месяца `M'`
       измеряется в `[начало M', конец M']`, а признаки тестового месяца
       начинают читаться не раньше начала `M` (недельные окна, курс и остальные
       ряды §2.4 берут точки после конца `M−1`).

    Проверка структурная (арифметика окон по `month`, данные не читаются):
    она падает при мутации `cutoff_for`/`available_from_mes` и на выборке с
    повторяющимся месяцем, а не «проверяет себя» на заведомо верных данных.
    """
    months = {row.month for row in rows}
    if len(months) != len(rows):
        raise AssertionError("purge: в выборке есть повторяющийся месяц")
    for row in rows:
        label_available = available_from_mes(month_end(row.month))
        if cutoff_for(row.month) >= label_available:
            raise AssertionError(
                f"purge: cutoff {cutoff_for(row.month).isoformat()} месяца "
                f"{row.month.isoformat()} не раньше публикации его метки "
                f"({label_available.isoformat()}) — утечка собственного таргета"
            )
        lag_month = previous_month(row.month)
        if lag_month in months and available_from_mes(month_end(lag_month)) > cutoff_for(row.month):
            raise AssertionError(
                f"purge: метка {lag_month.isoformat()} (y_lag1 месяца "
                f"{row.month.isoformat()}) не опубликована к cutoff"
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
    """Пишет шесть CSV-фикстур офлайн-тестов в `directory`.

    Срезы рядов — окна константы `FIXTURE_*` (те же запросы, что читает прогон,
    только с `WHERE date`); полный список фикстур и колонок — `FIXTURE_COLUMNS`.
    Названия файлов — фактическое окно содержимого.

    Пустое окно — ошибка экспорта (пустая фикстура молча ослабила бы тесты),
    кроме ОФЗ: у G-curve МосБиржи (`zcyc` — снимок текущего дня) окно 2021–2022
    пусто по устройству источника, и это печатается предупреждением. Файл
    пишется (заголовок без строк), тесты выборки видят ровно то, что видит
    живой прогон. Все запросы выполняются и проверяются ДО первой записи,
    поэтому неудачный экспорт не оставляет каталога с половиной файлов.
    """
    directory.mkdir(parents=True, exist_ok=True)
    exports = (
        ("weekly_2022.csv", _window_sql(WEEKLY_SQL, *FIXTURE_WEEKLY_2022), False),
        ("weekly_recent.csv", _window_sql(WEEKLY_SQL, *FIXTURE_WEEKLY_RECENT), False),
        ("ipc_mes_2021_2022.csv", _window_sql(TARGETS_SQL, *FIXTURE_MAIN), False),
        ("usd_2021_2022.csv", _window_sql(USD_SQL, *FIXTURE_MAIN), False),
        ("ofz_2021_2022.csv", _window_sql(OFZ_SQL, *FIXTURE_MAIN), True),
        ("fao_2021_2022.csv", _window_sql(FAO_SQL, *FIXTURE_MAIN), False),
    )
    planned = []
    for name, sql, allow_empty in exports:
        rows = ch_query(sql)
        if not rows and not allow_empty:
            raise RuntimeError(f"фикстура {name}: запрос вернул 0 строк")
        planned.append((name, FIXTURE_COLUMNS[name], rows))

    print(f"Фикстуры записаны в {directory}:")
    for name, columns, rows in planned:
        _write_csv(directory / name, columns, rows)
        if rows:
            first = _csv_cell(rows[0].get("date"))
            last = _csv_cell(rows[-1].get("date"))
            print(f"  {name:<24} {len(rows):>5} строк, {first} … {last}")
        else:
            print(f"  {name:<24} {len(rows):>5} строк (только заголовок: "
                  f"окно пусто — G-curve МосБиржи без истории)")
    return 0


# ---------------------------------------------------------------------------
# Сводка выборки (модели — Tasks 3–5)
# ---------------------------------------------------------------------------


def _field(label: str, value: str) -> None:
    """Строка сводки: подпись и значение (подписи пинуются тестами)."""
    print(f"  {label:<34}{value}")


def _plural(count: int, one: str, few: str, many: str) -> str:
    """Форма существительного по числу (131 позиция, 107 позиций, 1 точка)."""
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
# быть сотни (недельные данные не загрузились), и простыня на экран не нужна —
# число и одна строка «и ещё N» несут то же знание.
MAX_SKIPPED_PRINTED = 12


def run_summary(start: Optional[int] = None, embargo_months: Optional[int] = None) -> int:
    """Сводка выборки: диапазоны рядов, месяцы, пропуски, purge-проверка.

    Модели, walk-forward, метрики и артефакты — Tasks 3–5 (спека §3–§5); этот
    прогон ничего не пишет в ClickHouse и не создаёт файлов, только читает и
    печатает. `start`/`embargo_months` принимаются CLI уже сейчас, но на сводку
    не влияют: walk-forward ещё не реализован, и сводка об этом говорит прямо.
    """
    targets = load_targets()
    weekly = load_weekly()
    usd = load_usd()
    key_rate = load_key_rate()
    ofz = load_ofz_1y()
    fao = load_fao()
    rows, skipped = build_sample(targets, weekly, usd, key_rate, ofz, fao)
    assert_months_purged(rows)

    months = sample_months(targets)
    positions = len({name for name, _, _ in weekly})
    print("CPI nowcast — сводка выборки (каркас; модели — Tasks 3–5)")
    _field("таргет rosstat_ipc_mes:",
           f"{len(TARGET_SERIES)} {_plural(len(TARGET_SERIES), 'ряд', 'ряда', 'рядов')}, "
           f"{_month_range(months)} "
           f"({len(months)} {_plural(len(months), 'месяц', 'месяца', 'месяцев')})")
    _field("недельные rosstat_ipc_weeks:",
           f"{_range([(point[1],) for point in weekly])}, {positions} "
           f"{_plural(positions, 'позиция', 'позиции', 'позиций')}")
    _field("курс cbr_currency_usd:", _range(usd))
    _field("ключевая ставка cbr_key_rate:", _range(key_rate))
    ofz_points = f" ({len(ofz)} {_plural(len(ofz), 'точка', 'точки', 'точек')})" if ofz else ""
    _field(f"ОФЗ {OFZ_TENOR} ofz_curve:", _range(ofz) + ofz_points)
    _field("FAO Food Price Index:", _range(fao))
    if rows:
        _field("выборка:", f"{len(rows)} {_plural(len(rows), 'месяц', 'месяца', 'месяцев')} "
               f"({rows[0].month.isoformat()[:7]} … {rows[-1].month.isoformat()[:7]}), "
               f"пропущено {len(skipped)}")
    else:
        _field("выборка:", f"пусто, пропущено {len(skipped)}")
    ofz_missing = sum(1 for row in rows if math.isnan(row.features["ofz_1y"]))
    if ofz_missing:
        _field("ofz_1y недоступна на cutoff:", f"{ofz_missing} из {len(rows)} месяцев → "
               f"признак NaN (G-curve без истории: zcyc — снимок дня)")
    _field("признаки:", ", ".join(feature_names()))
    _field("purge-проверка (структурная):", "OK")
    if skipped:
        print("  пропущенные месяцы и причины:")
        index = target_index(targets)
        for month in skipped[:MAX_SKIPPED_PRINTED]:
            _, reason = month_features(month, index, weekly, usd, key_rate, ofz, fao)
            print(f"    {month.isoformat()[:7]} — {reason}")
        hidden = len(skipped) - MAX_SKIPPED_PRINTED
        if hidden > 0:
            print(f"    … и ещё {hidden} {_plural(hidden, 'месяц', 'месяца', 'месяцев')}")
    if start is not None:
        _field("--start:", f"{start} (принят; walk-forward — Task 3, на сводку не влияет)")
    if embargo_months is not None:
        _field("--embargo-months:", f"{embargo_months} (принят; walk-forward — Task 3, "
               f"на сводку не влияет)")
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI nowcast: выборка, as-of-правила и признаки (каркас ML-трека)."
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
        default=None,
        help="минимальный размер обучающего окна walk-forward в месяцах; "
             "принимается уже сейчас, но walk-forward появится в Task 3 — "
             "на сводку месяца пока не влияет",
    )
    parser.add_argument(
        "--embargo-months",
        type=int,
        metavar="K",
        default=None,
        help="эмбарго walk-forward в месяцах (спека §2.4); принимается уже "
             "сейчас, но walk-forward появится в Task 3 — на сводку не влияет",
    )
    args = parser.parse_args(argv)
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    return run_summary(start=args.start, embargo_months=args.embargo_months)


if __name__ == "__main__":
    sys.exit(main())
