#!/usr/bin/env python3
"""CPI-nowcast (ML-трек контура ставки): выборка, as-of, walk-forward, бенчмарки.

Срез ML-трека (спека `docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md`
§2–§4): nowcast месячного ИПЦ РФ до публикации Росстата. Здесь — данные (§2),
харнесс walk-forward (§4) и обязательные бенчмарки B1–B3 (§3.3); модели
CatBoost — Task 4, текущий прогноз и JSON — Task 5.

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
  причиной, а прогон не падает;
* **walk-forward** (`walk_forward`, §4) — расширяющееся окно: тест — строка
  `i`, обучение — `[0 … i−1−embargo]` (только прошлое; `embargo` выбрасывает
  месяцы перед тестом). `DegenerateWindowError` модели — пропуск фолда ЭТОЙ
  модели со счётчиком (`skipped`), а не падение прогона; модель может сообщать
  и диагностику фолда (`fold_notes`), которая агрегируется по окну и печатается
  в отчёте — «недосход N из M» (у B3 `ConvergenceWarning` координатного спуска
  считался по фолдам, а не терялся). Окна прогона
  (`window_specs`): основное (`--start`, `--embargo-months`) плюс робастность —
  второй старт 48 и эмбарго 1; консоль печатает основное окно подробно,
  робастные — компактно, полные таблицы идут в отчёт;
* **бенчмарки** (§3.3, фиксируются до результатов) — B1 `predict_last`
  (`y_{M−1}` из `features['y_lag1']` — тот же источник лага, что у моделей),
  B2 `fit_ar1` (OLS `y_M` на `y_{M−1}` + 11 дамми месяца, HAC `maxlags=3`,
  `use_correction=True`) и B3 `fit_enet` (`ElasticNetCV` с конфигурацией спеки
  на 9 числовых признаках §2.3 без `month`, стандартизация по train-окну);
* **метрики** (§4) — RMSE/MAE в п.п. через общие функции
  `scripts/eval_utils.py`, DM (поправка HLN, `h=1`) и блочный бутстрап разницы
  RMSE (блок 3 месяца, n=2000, seed=SEED) между моделями основного окна;
* **артефакты** (§5) — `cpi_nowcast_report.md` (таблицы «модель × окно»,
  «последние 12 фолдов», тесты значимости, «что не проверено») и
  `cpi_nowcast_predictions.csv` в корне worktree; оба gitignored — числа
  прогона фиксируются протоколом приёмки, а не репозиторием.

**NaN-политика (решение контроллера; см. `fit_enet`).** В признаках допустим
NaN: сейчас это `ofz_1y` во всём бэктесте (у G-curve МосБиржи нет истории — см.
ниже). CatBoost (Task 4) получит NaN как есть; `ElasticNetCV` NaN не принимает,
поэтому B3 импутирует NaN медианой ОБУЧАЮЩЕГО окна фолда (колонка без
наблюдений — 0.0: после стандартизации она нулевая и вклада не даёт), и только
затем `StandardScaler` подгоняется на train-окне (`fit` только на train —
статистика теста не участвует). «NaN-колонки» (NaN больше половины значений
окна), которых больше половины, — вырожденное окно B3.

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

* `make cpi-nowcast` — сводка выборки, walk-forward бенчмарков по окнам
  (основное — старт 24, робастность — старт 48 и эмбарго 1), отчёт и CSV
  прогнозов в корне worktree;
* `make cpi-nowcast ARGS="--start N --embargo-months K"` — основное окно
  задаётся явно (робастность остаётся от спеки);
* `make cpi-nowcast ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
"""

import argparse
import csv
import math
import statistics
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import statsmodels.api as sm
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parent.parent

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
sys.path.insert(0, str(ROOT / "scripts"))

import eval_utils as eu
import extrapolation_aisc as aisc

ch_query = aisc.ch_query

# Каталог артефактов живого прогона — корень worktree (файлы gitignored: точные
# имена в `.gitignore`). Тесты подменяют его временным каталогом, поэтому
# `run_summary` берёт значение в момент вызова, а не связывает по умолчанию.
ARTIFACTS_DIR = ROOT

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
# Walk-forward harness (спека §4)
# ---------------------------------------------------------------------------

# Окна прогона: старт 24 — основной (спека §4), 48 и embargo=1 — робастность.
MAIN_START = 24
MAIN_EMBARGO = 0
ROBUST_START = 48
ROBUST_EMBARGO = 1

# Контракт модели walk-forward: обучение на окне строк, прогноз тестовой строки.
FitPredict = Callable[[list[Row], Row], float]

# Ключ диагностики фолда в необязательном протоколе `fold_notes`: спуск модели
# не сошёлся (у B3 — `ConvergenceWarning` координатного спуска). Модель,
# выставляющая атрибут `fold_notes` (dict[str, int] о ПОСЛЕДНЕЙ подгонке),
# отчитывается по фолдам; harness (`walk_forward`) накапливает значения по
# модели, отчёт печатает «N из M» фолдов. У моделей без атрибута (B1, B2)
# диагностики нет — в отчёте «—».
NON_CONVERGED = "non_converged"


@dataclass(frozen=True)
class Prediction:
    """Фолд walk-forward: месяц, модель, факт и прогноз (п.п., м/м).

    Единицы — пункты процента, как у таргета §2.1: у бенчмарков, моделей Task 4
    и метрик одна шкала. `model` — имя из словаря `models` (у бенчмарков —
    константы `MODEL_*`); по нему отчёт собирает таблицы «модель × окно».
    """

    month: date
    model: str
    y_true: float
    y_pred: float


@dataclass(frozen=True)
class WindowRun:
    """Прогон одного окна walk-forward: фолды, счётчики пропусков, метки.

    `folds` — число тестовых месяцев окна (может быть больше числа прогнозов
    модели, если её окна вырождались); `skipped` — «модель → пропущено фолдов»
    по ВСЕМ моделям словаря (нулевые счётчики тоже: модель, вырожденная на всех
    окнах, видна в отчёте строкой «0 фолдов», а не исчезает); `notes` —
    «модель → диагностика» (сумма `fold_notes` фолдов; у моделей без протокола
    словарь пуст); `main` — основное окно прогона (для блока «последние 12
    фолдов» и тестов значимости).
    """

    start: int
    embargo: int
    main: bool
    folds: int
    predictions: list[Prediction]
    skipped: dict[str, int]
    notes: dict[str, dict[str, int]]


def window_key(start: int, embargo: int) -> str:
    """Метка окна для CSV и таблиц: `start=N` и `, embargo=K` при K > 0."""
    return f"start={start}" + (f", embargo={embargo}" if embargo else "")


def window_title(run: WindowRun) -> str:
    """Метка окна в отчёте: ключ плюс роль (основное/робастность)."""
    return f"{window_key(run.start, run.embargo)} ({'основное' if run.main else 'робастность'})"


def window_specs(start: int = MAIN_START, embargo: int = MAIN_EMBARGO) -> list[tuple[int, int]]:
    """Окна прогона: основное (`start`/`embargo`) и робастность спеки §4.

    Робастность — второй старт (`ROBUST_START`, как в докладе) и эмбарго 1;
    дубликаты и несовместимые окна (`embargo ≥ start` — обучать не на чем)
    отбрасываются, поэтому `--embargo-months 1` не даёт основное окно дважды.
    """
    specs = [(start, embargo)]
    for candidate in ((ROBUST_START, embargo), (start, ROBUST_EMBARGO)):
        if candidate not in specs and 0 <= candidate[1] < candidate[0]:
            specs.append(candidate)
    return specs


def walk_forward(
    rows: list[Row],
    models: dict[str, FitPredict],
    start: int,
    embargo: int,
    skipped: Optional[dict[str, int]] = None,
    notes: Optional[dict[str, dict[str, int]]] = None,
) -> list[Prediction]:
    """Walk-forward по словарю моделей и одному окну (спека §4).

    Фолд `i` (нумерация строк `rows`) обучается на `[0 … i−1−embargo]` и
    прогнозирует `rows[i]`; первый фолд — строка `start` (минимальное обучающее
    окно, поэтому при `start=24` первый тест — 25-й месяц). Единый train/test
    split спекой запрещён и здесь не выражается вовсе; `embargo > 0` выбрасывает
    месяцы ПЕРЕД тестом — метки обучения не перекрывают тестовый месяц.

    `DegenerateWindowError` модели — пропуск фолда ЭТОЙ модели со счётчиком, а
    не падение прогона: вырожденное окно — свойство окна, а не порча данных.
    Любое другое исключение пробрасывается: это баг или порча данных. Счётчики
    собираются в необязательные словари, которые заполняет вызывающий
    (`evaluate_windows`): `skipped` («модель → пропущено фолдов») и `notes`
    («модель → сумма `fold_notes` фолдов»; модель без атрибута `fold_notes` —
    без диагностики). Сигнатура возврата — список прогнозов (контракт Tasks 4–5),
    а счётчики нужны отчёту.
    """
    if start < 1:
        raise ValueError(f"walk-forward: start={start} — меньше одного месяца")
    if embargo < 0 or embargo >= start:
        raise ValueError(
            f"walk-forward: embargo={embargo} при start={start} — обучающего окна нет"
        )
    counts = skipped if skipped is not None else {}
    predictions = []
    for index in range(start, len(rows)):
        train = rows[: index - embargo]
        row = rows[index]
        for name, fit_predict in models.items():
            try:
                value = fit_predict(train, row)
            except DegenerateWindowError:
                counts[name] = counts.get(name, 0) + 1
                continue
            if notes is not None:
                fold_notes = getattr(fit_predict, "fold_notes", None)
                if fold_notes:
                    totals = notes.setdefault(name, {})
                    for key, count in fold_notes.items():
                        totals[key] = totals.get(key, 0) + int(count)
            predictions.append(
                Prediction(
                    month=row.month,
                    model=name,
                    y_true=float(row.y[TARGET_ALL]),
                    y_pred=float(value),
                )
            )
    return predictions


def evaluate_windows(
    rows: list[Row],
    models: dict[str, FitPredict],
    specs: Optional[list[tuple[int, int]]] = None,
) -> list[WindowRun]:
    """Walk-forward по всем окнам прогона; основное — первое окно списка.

    Счётчики пропусков создаются по всем моделям словаря (нули тоже), поэтому
    `WindowRun.skipped` хранит порядок моделей `models` — от него зависят строки
    таблиц отчёта, а не алфавит. Диагностика `fold_notes` собирается по окну
    заново (`WindowRun.notes`): число недосошедших фолдов относится к КОНКРЕТНОМУ
    окну, а не к прогону целиком.
    """
    runs = []
    for position, (start, embargo) in enumerate(window_specs() if specs is None else specs):
        skipped = {name: 0 for name in models}
        notes = {name: {} for name in models}
        predictions = walk_forward(rows, models, start, embargo, skipped=skipped, notes=notes)
        runs.append(
            WindowRun(
                start=start,
                embargo=embargo,
                main=position == 0,
                folds=max(0, len(rows) - start),
                predictions=predictions,
                skipped=skipped,
                notes=notes,
            )
        )
    return runs


# ---------------------------------------------------------------------------
# Бенчмарки B1–B3 (спека §3.3; фиксируются до результатов)
# ---------------------------------------------------------------------------

MODEL_LAST = "b1_last_value"
MODEL_AR1 = "b2_ar1_seasonal"
MODEL_ENET = "b3_elastic_net"

# Метки для отчёта: бенчмарки названы номерами спеки §3.3; модели Task 4
# (`catboost_direct`/`catboost_bottomup`) придут сюда своими именами.
MODEL_LABELS = {
    MODEL_LAST: "B1 last value",
    MODEL_AR1: "B2 AR(1)+сезонные дамми",
    MODEL_ENET: "B3 ElasticNetCV",
}


def model_label(model: str) -> str:
    """Метка модели для отчёта: у бенчмарков — номер спеки, у прочих — имя."""
    return MODEL_LABELS.get(model, model)


def predict_last(train: list[Row], row: Row) -> float:
    """B1 «прошлый месяц» (спека §3.3): прогноз — `y_{M−1}`.

    Источник лага — тот же, что у моделей: `row.features['y_lag1']` — значение
    метки `M−1`, проверенное as-of-правилами §2.4. Брать `train[-1].y` нельзя:
    при `embargo > 0` последняя строка обучения — не месяц `M−1`, и бенчмарк
    перестал бы быть «прошлым месяцем». Обучающее окно не используется вовсе
    (параметр — часть контракта `FitPredict`).
    """
    return float(row.features["y_lag1"])


# --- B2: AR(1) с сезонными дамми ---------------------------------------------

HAC_MAXLAGS = 3  # спека §3.3: HAC (Ньюи–Уэст) с maxlags=3
AR1_DUMMY_MONTHS = tuple(range(2, 13))  # 11 дамми; январь — база
AR1_PARAMS = 2 + len(AR1_DUMMY_MONTHS)  # const + y_lag1 + дамми
AR1_MIN_TRAIN = AR1_PARAMS + 1  # 13 параметров + хотя бы одна остаточная степень свободы


def ar1_design(rows: list[Row]) -> tuple:
    """Дизайн B2: `[1, y_lag1, d_2 … d_12]` → `(X, y)`; `y` — таргет `cpi_all`.

    Дамми — ровно 11 (`AR1_DUMMY_MONTHS`), январь — база: спека §3.3 называет
    «11 дамми месяца». Значения берутся из признаков строки (`y_lag1`, `month`) —
    тот же источник, что у моделей; одна и та же функция строит дизайн обучения
    и тестовой строки, поэтому сдвиг месяцев в дамми не может возникнуть «только
    на предсказании».
    """
    design, targets = [], []
    for row in rows:
        month = int(row.features["month"])
        design.append(
            [1.0, float(row.features["y_lag1"])]
            + [1.0 if month == dummy else 0.0 for dummy in AR1_DUMMY_MONTHS]
        )
        targets.append(float(row.y[TARGET_ALL]))
    return np.asarray(design, dtype=float), np.asarray(targets, dtype=float)


def fit_ar1(train: list[Row], row: Row) -> float:
    """B2: OLS `y_M` на `y_{M−1}` + 11 дамми месяца, ковариация HAC (спека §3.3).

    Конфигурация HAC (`maxlags=3`, `use_correction=True`) — часть спеки; она
    пинована тестом по АТРИБУТАМ результата (`fit_ar1.last_result`), а не по
    подменённому statsmodels: от ковариации зависят p-value строк отчёта.

    Вырожденное окно — `DegenerateWindowError` (walk-forward пропускает фолд со
    счётчиком): строк меньше `AR1_MIN_TRAIN` (у дизайна из 13 параметров не
    остаётся остаточной степени свободы) или `y_lag1` без вариации (наклон φ не
    идентифицирован), как у AR(1)-бенчмарка модели ставки.
    """
    if len(train) < AR1_MIN_TRAIN:
        raise DegenerateWindowError(
            f"B2 (AR(1)): обучающее окно {len(train)} месяцев короче {AR1_MIN_TRAIN} — "
            f"у регрессии нет остаточных степеней свободы"
        )
    if len({float(item.features["y_lag1"]) for item in train}) < 2:
        raise DegenerateWindowError(
            "B2 (AR(1)): y_lag1 в обучающем окне не меняется — φ не идентифицирован"
        )
    design, targets = ar1_design(train)
    result = sm.OLS(targets, design).fit(
        cov_type="HAC", cov_kwds={"maxlags": HAC_MAXLAGS, "use_correction": True}
    )
    fit_ar1.last_result = result
    row_design, _ = ar1_design([row])
    return float(np.dot(row_design[0], np.asarray(result.params)))


# Последний подогнанный результат statsmodels: читают тесты (пины HAC), отчёт —
# нет. Атрибут заполняется при каждом вызове `fit_ar1`.
fit_ar1.last_result = None


# --- B3: ElasticNetCV на стандартизованных признаках --------------------------

ENET_ALPHAS = np.logspace(-4, 1, 20)  # спека §3.3, дословно
ENET_L1_RATIOS = (0.1, 0.5, 0.9)  # спека §3.3, дословно
ENET_CV_SPLITS = 3  # TimeSeriesSplit(3) внутри train-окна фолда
ENET_MIN_TRAIN = 12  # окно короче года — не фолд B3
ENET_NAN_COLUMN_SHARE = 0.5  # «NaN-колонка»: NaN строго больше половины значений
ENET_MEDIAN_FALLBACK = 0.0  # колонка без наблюдений: нейтральная константа

# Признаковый вектор B3 — §2.3 без `month` (числовые 9 + `y_lag1`, спека §3.3).
ENET_FEATURES = tuple(name for name in feature_names() if name != "month")


def enet_matrix(rows: list[Row]) -> np.ndarray:
    """Матрица признаков B3 (`ENET_FEATURES`); NaN сохраняется — импутация позже."""
    return np.asarray(
        [[float(row.features[name]) for name in ENET_FEATURES] for row in rows],
        dtype=float,
    )


def enet_medians(matrix: np.ndarray) -> np.ndarray:
    """Медианы колонок train-окна; колонка без наблюдений — `ENET_MEDIAN_FALLBACK`.

    `np.nanmedian` на полностью NaN-колонке возвращает NaN и пишет
    `RuntimeWarning`; медиана здесь не определена, и колонка получает
    нейтральный ноль: после `StandardScaler` она нулевая и вклада в прогноз не
    даёт. Так выглядит живой бэктест — `ofz_1y` = NaN во всех месяцах.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        medians = np.nanmedian(matrix, axis=0)
    return np.where(np.isnan(medians), ENET_MEDIAN_FALLBACK, medians)


def enet_impute(matrix: np.ndarray, medians: np.ndarray) -> np.ndarray:
    """NaN → медиана колонки train-окна (одна импутация для train и теста).

    Тестовая строка импутируется медианами ОБУЧАЮЩЕГО окна: статистика теста в
    импутацию не попадает — иначе это была бы утечка через предобработку.
    """
    return np.where(np.isnan(matrix), medians, matrix)


def _assert_enet_window(matrix: np.ndarray) -> None:
    """Проверки вырожденного окна B3 (спека §3.3; решение контроллера).

    * окно короче `ENET_MIN_TRAIN` месяцев — фолд B3 не оценивается;
    * «NaN-колонок» (NaN строго больше половины значений окна) больше половины
      всех колонок: признаковое пространство вырождено (импутация медианой
      восстановила бы в основном шум). Единичные NaN-колонки, включая колонку
      без наблюдений вовсе, окно не вырождают — они импутируются.
    """
    if matrix.shape[0] < ENET_MIN_TRAIN:
        raise DegenerateWindowError(
            f"B3 (ElasticNetCV): обучающее окно {matrix.shape[0]} месяцев короче "
            f"{ENET_MIN_TRAIN} — признаковое пространство не оценивается"
        )
    nan_shares = np.mean(np.isnan(matrix), axis=0)
    nan_columns = [
        name
        for name, share in zip(ENET_FEATURES, nan_shares)
        if share > ENET_NAN_COLUMN_SHARE
    ]
    if 2 * len(nan_columns) > len(ENET_FEATURES):
        raise DegenerateWindowError(
            f"B3 (ElasticNetCV): {len(nan_columns)} из {len(ENET_FEATURES)} колонок — "
            f"NaN-колонки (NaN больше половины значений окна): {', '.join(nan_columns)}"
        )


@dataclass(frozen=True)
class EnetFit:
    """Подобранный фолд B3: пайплайн и матрицы окна (для тестов-пинов).

    `model` — `Pipeline` (StandardScaler → ElasticNetCV) на импутированной
    train-матрице; `medians` — медианы импутации окна; `imputed` — train-матрица
    после импутации (до стандартизации). Отчёт ничего из этого не читает.
    """

    model: Pipeline
    medians: np.ndarray
    imputed: np.ndarray


def fit_enet(train: list[Row], row: Row) -> float:
    """B3: `ElasticNetCV` на стандартизованных признаках §2.3 без `month` (§3.3).

    Конфигурация — дословно из спеки: `alphas=logspace(−4, 1, 20)`,
    `l1_ratio=[0.1, 0.5, 0.9]`, `cv=TimeSeriesSplit(3)`, `random_state=SEED`;
    сплит — ВНУТРИ train-окна фолда, тест в подбор гиперпараметров не попадает.

    NaN-политика (решение контроллера; в бэктесте NaN — это `ofz_1y`):
    `ElasticNetCV` NaN не принимает, поэтому NaN импутируется медианой
    ОБУЧАЮЩЕГО окна фолда (колонка без наблюдений — `ENET_MEDIAN_FALLBACK`), и
    только затем `StandardScaler` подгоняется на train (`fit` только на train,
    `transform` — train и тест). Модели CatBoost (Task 4) получают NaN как есть.

    Вырожденное окно — `DegenerateWindowError`; walk-forward пропускает фолд со
    счётчиком. `ConvergenceWarning` координатного спуска (малые `alpha` спеки на
    коротких окнах) не печатается — конфигурация `ElasticNetCV` закреплена
    спекой §3.3, а прогноз конечен и взят у выбранной CV модели, — но и не
    теряется: фолд помечается диагностикой `fold_notes[NON_CONVERGED]`, отчёт
    печатает «недосход: N из M» по каждому окну. Подобранный пайплайн и матрицы
    остаются в `fit_enet.last_fit`, диагностика последнего фолда — в
    `fit_enet.fold_notes` (протокол `walk_forward`).
    """
    matrix = enet_matrix(train)
    _assert_enet_window(matrix)
    medians = enet_medians(matrix)
    imputed = enet_impute(matrix, medians)
    targets = np.asarray([float(item.y[TARGET_ALL]) for item in train])
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "enet",
                ElasticNetCV(
                    alphas=ENET_ALPHAS,
                    l1_ratio=list(ENET_L1_RATIOS),
                    cv=TimeSeriesSplit(ENET_CV_SPLITS),
                    random_state=SEED,
                ),
            ),
        ]
    )
    with warnings.catch_warnings(record=True) as caught:
        # Все предупреждения подгонки перехватываются, чтобы считать недосход по
        # фолдам: координатный спуск внутри CV на малых alpha и коротких окнах
        # может не досчитать до допуска (Duality gap чуть выше tolerance) и
        # напечатать ConvergenceWarning. Конфигурация (`alphas` до 1e-4,
        # `max_iter` по умолчанию) закреплена спекой §3.3 — менять её здесь
        # нельзя, поэтому предупреждение не печатается, а фиксируется в
        # диагностике `fold_notes` (walk-forward агрегирует её по окну, отчёт
        # печатает «недосход: N из M»). Прочие предупреждения не прячутся:
        # они переиздаются после фита.
        warnings.simplefilter("always")
        model.fit(imputed, targets)
    non_converged = any(
        issubclass(warning.category, ConvergenceWarning) for warning in caught
    )
    for warning in caught:
        if not issubclass(warning.category, ConvergenceWarning):
            warnings.warn(str(warning.message), warning.category, stacklevel=2)
    fit_enet.fold_notes = {NON_CONVERGED: int(non_converged)}
    fit_enet.last_fit = EnetFit(model=model, medians=medians, imputed=imputed)
    return float(model.predict(enet_impute(enet_matrix([row]), medians))[0])


# Последний подобранный фолд B3 и диагностика последнего фолда: читают тесты
# (пины конфигурации и импутации) и harness (`fold_notes`); отчёт — нет.
# Атрибуты заполняются при каждом вызове `fit_enet`.
fit_enet.last_fit = None
fit_enet.fold_notes = {}

# Бенчмарки спеки §3.3 — порядок B1, B2, B3 задаёт строки таблиц отчёта.
BENCHMARKS = {MODEL_LAST: predict_last, MODEL_AR1: fit_ar1, MODEL_ENET: fit_enet}


# ---------------------------------------------------------------------------
# Метрики и тесты значимости (спека §4)
# ---------------------------------------------------------------------------

BOOTSTRAP_BLOCK = 3  # блочный бутстрап: блок 3 месяца (спека §4)
BOOTSTRAP_N = 2000  # число ресэмплов
LAST_FOLDS = 12  # блок «последние 12 фолдов»

# Тип результата бутстрапа — из общего модуля: `eval_utils.BootstrapDiff`.
BootstrapDiff = eu.BootstrapDiff


def errors(predictions: list[Prediction]) -> list[float]:
    """Ошибки фолдов (прогноз − факт, п.п.) — вход общих функций `eval_utils`."""
    return [prediction.y_pred - prediction.y_true for prediction in predictions]


def rmse_pp(predictions: list[Prediction]) -> float:
    """RMSE фолдов в п.п. — тонкая обёртка над `eval_utils.rmse`."""
    return eu.rmse(errors(predictions))


def mae_pp(predictions: list[Prediction]) -> float:
    """MAE фолдов в п.п. — тонкая обёртка над `eval_utils.mae`."""
    return eu.mae(errors(predictions))


def diebold_mariano(err_a: list, err_b: list) -> tuple:
    """DM (поправка HLN) между моделями: `h=1` — горизонт nowcast месяц (§4)."""
    return eu.diebold_mariano(err_a, err_b, h=1)


def block_bootstrap_rmse_diff(err_a: list, err_b: list) -> BootstrapDiff:
    """Бутстрап разницы RMSE с конфигурацией спеки §4: блок 3, n=2000, seed=SEED."""
    return eu.block_bootstrap_rmse_diff(
        err_a, err_b, block=BOOTSTRAP_BLOCK, n=BOOTSTRAP_N, seed=SEED
    )


@dataclass(frozen=True)
class PairTest:
    """Сравнение двух моделей одного окна: DM и бутстрап разницы RMSE (спека §4).

    `model_a`/`model_b` — порядок строки отчёта; `rmse_diff = RMSE(A) − RMSE(B)`;
    отрицательные `dm_stat`/интервал значат, что модель A точнее. `n` — число
    ОБЩИХ месяцев пары: модель, пропустившая фолд, не сдвигает ошибки другой.
    """

    model_a: str
    model_b: str
    n: int
    dm_stat: float
    dm_p: float
    rmse_diff: float
    ci_low: float
    ci_high: float
    bootstrap_p: float


def pairwise_tests(predictions: list[Prediction]) -> list[PairTest]:
    """DM и бутстрап по всем парам моделей одного окна (спека §4).

    Пары — в порядке первого появления моделей (порядок прогона, не алфавит);
    сравнение — на общих месяцах. Пара с менее чем двумя общими месяцами
    пропускается: тест значимости на одной точке не определён. Модели Task 4
    добавятся сюда тем же списком прогнозов, без правок функции.
    """
    groups: dict[str, list[Prediction]] = {}
    for prediction in predictions:
        groups.setdefault(prediction.model, []).append(prediction)
    tests = []
    names = list(groups)
    for position, name_a in enumerate(names):
        for name_b in names[position + 1 :]:
            by_month_a = {prediction.month: prediction for prediction in groups[name_a]}
            by_month_b = {prediction.month: prediction for prediction in groups[name_b]}
            months = sorted(set(by_month_a) & set(by_month_b))
            if len(months) < 2:
                continue
            err_a = errors([by_month_a[month] for month in months])
            err_b = errors([by_month_b[month] for month in months])
            dm_stat, dm_p = diebold_mariano(err_a, err_b)
            bootstrap = block_bootstrap_rmse_diff(err_a, err_b)
            tests.append(
                PairTest(
                    model_a=name_a,
                    model_b=name_b,
                    n=len(months),
                    dm_stat=dm_stat,
                    dm_p=dm_p,
                    rmse_diff=bootstrap.rmse_diff,
                    ci_low=bootstrap.low,
                    ci_high=bootstrap.high,
                    bootstrap_p=bootstrap.p_value,
                )
            )
    return tests


# ---------------------------------------------------------------------------
# Отчёт и артефакты (Tasks 3–5)
# ---------------------------------------------------------------------------

REPORT_NAME = "cpi_nowcast_report.md"
PREDICTIONS_NAME = "cpi_nowcast_predictions.csv"
PREDICTIONS_COLUMNS = ("window", "model", "month", "y_true", "y_pred", "error")


def _fmt(value: Optional[float], places: int = 3) -> str:
    """Число метрики для таблицы: `None` — «—», бесконечность — «±inf»."""
    if value is None:
        return "—"
    if math.isinf(value):
        return "+inf" if value > 0 else "−inf"
    formatted = f"{value:.{places}f}"
    # «-0.000» — артефакт округления малой отрицательной величины: знака в
    # разрядах таблицы нет, и минус читался бы как ошибка знака.
    if float(formatted) == 0.0:
        return formatted.lstrip("-")
    return formatted


def _fmt_p(pvalue: float) -> str:
    """p-value для таблицы: менее 1e-4 — «<0.0001» (разряды не нужны)."""
    if pvalue < 0.0001:
        return "<0.0001"
    return f"{pvalue:.4f}"


def _months_suffix(months: list, limit: int = 6) -> str:
    """Список месяцев `(2022-01, …)` — для строки о пропущенных месяцах."""
    if not months:
        return ""
    shown = ", ".join(month.isoformat()[:7] for month in months[:limit])
    if len(months) > limit:
        shown += f", … (+{len(months) - limit})"
    return f" ({shown})"


def _table(header: tuple, rows: list, empty: str = "_(нет строк.)_") -> list:
    """Строки markdown-таблицы; без строк — явная пометка, а не пустое место."""
    if not rows:
        return [empty]
    return list(header) + list(rows)


def _run_groups(run: WindowRun) -> dict[str, list[Prediction]]:
    """«Модель → фолды» окна в порядке словаря прогона (включая вырожденные)."""
    groups: dict[str, list[Prediction]] = {name: [] for name in run.skipped}
    for prediction in run.predictions:
        groups.setdefault(prediction.model, []).append(prediction)
    return groups


def last_fold_months(predictions: list[Prediction], count: int = LAST_FOLDS) -> set:
    """Месяцы последних `count` фолдов списка — общий отрезок для всех моделей.

    Блок «последние 12 фолдов» описывает один и тот же отрезок выборки: если
    модель пропустила фолд, у неё в таблице меньше строк, а не другой отрезок.
    """
    return set(sorted({prediction.month for prediction in predictions})[-count:])


def _metrics_cells(group: list[Prediction]) -> str:
    """«n | RMSE | MAE» строки метрик; у пустой группы — «0 фолдов» и прочерки."""
    if not group:
        return "0 фолдов | — | —"
    return f"{len(group)} | {_fmt(rmse_pp(group))} | {_fmt(mae_pp(group))}"


def _fold_note_cell(notes: dict, folds: int) -> str:
    """Ячейка «недосход»: `N из M` у моделей с диагностикой, «—» у остальных.

    `notes` — накопленная диагностика модели за окно (`WindowRun.notes`), `folds`
    — число ЕЁ фолдов, по которым считался недосход (0 фолдов — «—»).
    """
    if NON_CONVERGED not in notes or folds == 0:
        return "—"
    return f"{notes[NON_CONVERGED]} из {folds}"


_METRIC_HEADER = (
    "| модель | окно | n | RMSE, п.п. | MAE, п.п. |",
    "|---|---|---|---|---|",
)
_FOLD_SUMMARY_HEADER = (
    "| модель | окно | фолдов | пропущено (вырожденное окно) | недосход (ConvergenceWarning) |",
    "|---|---|---|---|---|",
)
_LAST_FOLDS_HEADER = (
    "| модель | n | RMSE, п.п. | MAE, п.п. |",
    "|---|---|---|---|",
)
_SIGNIFICANCE_HEADER = (
    "| модель A | окно | против B | n | ΔRMSE (A−B), п.п. | DM | DM p | 95% ДИ ΔRMSE | бутстр. p |",
    "|---|---|---|---|---|---|---|---|---|",
)


def render_report(
    sample: list[Row],
    skipped: list,
    window_runs: list[WindowRun],
    nowcast: Optional[str] = None,
    today: Optional[date] = None,
) -> str:
    """Markdown-отчёт прогона: выборка, walk-forward, метрики, значимость (§4).

    `sample`/`skipped` — выборка `build_sample` (строки и месяцы без собранных
    признаков); `window_runs` — по одному `WindowRun` на окно walk-forward
    (порядок окон задаёт строки таблиц, основное помечено `WindowRun.main`; имя
    отражает и прогнозы, и счётчики окна); `nowcast` — готовый markdown-блок
    текущего прогноза (Task 5) или `None`; `today` — дата прогона (по умолчанию
    системная).

    «Модель × окно» — строки таблиц: перечень моделей берётся из счётчиков окон
    (модель, вырожденная на всех окнах, видна строкой «0 фолдов», а не
    пропадает), метрики — из фолдов, «недосход» — из `WindowRun.notes`. Модели
    Task 4 (CatBoost) попадают в те же таблицы теми же `Prediction` — правок
    отчёта не требуется.
    """
    today = today or date.today()
    runs = list(window_runs)
    main_run = next((run for run in runs if run.main), runs[0] if runs else None)
    lines = [
        "# CPI nowcast — walk-forward бэктест бенчмарков",
        "",
        f"Дата прогона: {today.isoformat()}; сид прогона `SEED={SEED}`. Спека: "
        "`docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md` (§2 — данные и "
        "признаки, §3.3 — бенчмарки, §4 — walk-forward/метрики/тесты); план: "
        "`docs/superpowers/plans/2026-10-16-cpi-nowcast.md`.",
        "",
        "## Выборка",
        "",
        "| состав | значение |",
        "|---|---|",
    ]
    span = (
        f"{sample[0].month.isoformat()[:7]} … {sample[-1].month.isoformat()[:7]}"
        if sample
        else "—"
    )
    ofz_nan = sum(1 for row in sample if math.isnan(float(row.features["ofz_1y"])))
    lines += [
        f"| месяцев в выборке | {len(sample)} ({span}) |",
        f"| пропущено (признак не собран) | {len(skipped)}{_months_suffix(list(skipped))} |",
        f"| `ofz_1y` = NaN (нет точки ≤ cutoff) | {ofz_nan} из {len(sample)} |",
        "| purge-проверка (структурная) | OK |",
        "",
        "As-of — консервативные правила §2.4 (собственный таргет месяца в признаки "
        "не попадает, точка позже cutoff — тоже). Политика NaN: `ofz_1y` — "
        "единственный признак, отсутствие которого не выбрасывает месяц; при "
        f"отсутствии точки ≤ cutoff он равен NaN. `{model_label(MODEL_ENET)}` "
        "импутирует NaN медианой обучающего окна фолда (колонка без наблюдений — "
        "0.0: после стандартизации вклада не даёт) и стандартизует признаки "
        "`StandardScaler`, подобранным ТОЛЬКО на train-окне фолда — статистика "
        "теста в предобработку не попадает. CatBoost (Task 4) получает NaN как есть.",
        "",
        "## Walk-forward",
        "",
        "Расширяющееся обучающее окно, переобучение на каждом фолде: тест — месяц "
        "`i`, обучение — строки `[0 … i−1−embargo]` (только прошлое; embargo > 0 "
        "выбрасывает месяц перед тестом). Единый train/test split не используется. "
        "Вырожденное обучающее окно (`DegenerateWindowError` любой модели) — "
        "пропуск фолда ЭТОЙ модели со счётчиком, а не падение прогона. "
        "«Недосход» — фолды, где численный решатель модели не сошёлся "
        "(`ConvergenceWarning`; для B3 это координатный спуск на малых `alpha` "
        "спеки, `max_iter` не повышался), подсчёт — по предупреждениям вокруг "
        "подгонки; у моделей без такой диагностики (B1, B2) — «—».",
        "",
    ]
    fold_rows, metric_rows = [], []
    for run in runs:
        for name, group in _run_groups(run).items():
            fold_rows.append(
                f"| {model_label(name)} | {window_title(run)} | {len(group)} "
                f"| {run.skipped.get(name, 0)} "
                f"| {_fold_note_cell(run.notes.get(name, {}), len(group))} |"
            )
            metric_rows.append(
                f"| {model_label(name)} | {window_title(run)} | {_metrics_cells(group)} |"
            )
    lines += _table(_FOLD_SUMMARY_HEADER, fold_rows)
    lines += [
        "",
        "## Метрики (RMSE / MAE, п.п.)",
        "",
        "Ошибка фолда — `прогноз − факт` (п.п., м/м); `n` — число фолдов под "
        "метрикой. RMSE/MAE — общие функции `scripts/eval_utils.py`.",
        "",
    ]
    lines += _table(_METRIC_HEADER, metric_rows)
    lines += [""]
    if main_run is not None:
        tail_months = last_fold_months(main_run.predictions)
        last_rows = [
            f"| {model_label(name)} | "
            f"{_metrics_cells([item for item in group if item.month in tail_months])} |"
            for name, group in _run_groups(main_run).items()
        ]
        lines += [
            f"## Последние {LAST_FOLDS} фолдов (основное окно "
            f"{window_key(main_run.start, main_run.embargo)})",
            "",
            f"Последние {LAST_FOLDS} тестовых месяцев основного окна: блок "
            "показывает, что метрики не держатся одним спокойным периодом.",
            "",
        ]
        lines += _table(_LAST_FOLDS_HEADER, last_rows)
        lines += [""]
        pair_rows = [
            f"| {model_label(test.model_a)} | {window_title(main_run)} | "
            f"{model_label(test.model_b)} | {test.n} | {_fmt(test.rmse_diff)} | "
            f"{_fmt(test.dm_stat)} | {_fmt_p(test.dm_p)} | "
            f"[{_fmt(test.ci_low)}; {_fmt(test.ci_high)}] | {_fmt_p(test.bootstrap_p)} |"
            for test in pairwise_tests(main_run.predictions)
        ]
        lines += [
            f"## Тесты значимости между моделями (основное окно "
            f"{window_key(main_run.start, main_run.embargo)})",
            "",
            "DM — Diebold–Mariano с поправкой HLN (спека §4; `statsmodels`, "
            "головная метрика — MSE), `h=1` (горизонт nowcast — месяц), p "
            "двусторонний; положительная статистика значит, что ошибки модели A "
            "БОЛЬШЕ. ΔRMSE = RMSE(A) − RMSE(B); интервал и p — блочный бутстрап "
            f"разницы RMSE (блок {BOOTSTRAP_BLOCK} месяца, n={BOOTSTRAP_N}, "
            "seed=SEED). Пары — на общих месяцах; модели CatBoost (Task 4) "
            "добавятся в ту же таблицу.",
            "",
        ]
        lines += _table(
            _SIGNIFICANCE_HEADER,
            pair_rows,
            empty="_(нет пар: меньше двух моделей с общими фолдами.)_",
        )
        lines += [""]
    lines += [
        "## Что не проверено",
        "",
        "- as-of — приближение, а не point-in-time слой: точные даты публикаций и "
        "ревизии рядов не учтены; PIT — отдельный пункт фазы 4.",
        "- недельный ИПЦ считается по позициям (агрегата «все товары» в источнике "
        "нет), состав позиций между неделями не проверяется.",
        "- `ofz_1y` без истории (NaN во всём бэктесте): вклад признака у B3 на "
        "бэктесте нулевой (импутация константой); CatBoost (Task 4) получит NaN "
        "как есть — сравнение политик NaN моделей не проверено.",
        "- модели CatBoost (direct/bottom-up), веса NNLS и DM «модели против "
        "бенчмарков» — Task 4; здесь таблица значимости только между бенчмарками.",
        "- множественные сравнения (DM/бутстрап по парам) — без поправки на "
        "множественность.",
        "- режимы отдельно не тестируются: 2022 год (шок) входит в общую выборку, "
        "блок «последние 12 фолдов» — описательный, не тест.",
        "- embargo 1 — только робастность: перекрытие окон признаков тестового и "
        "обучающих месяцев структурно исключено (purge-проверка), но границы "
        "доступности — консервативное правило §2.4, а не фактические даты.",
        "- недосход B3 (`ConvergenceWarning` координатного спуска) считается по "
        "фолдам и выводится в таблице Walk-forward, но влияние недосчёта на "
        "прогноз не измерено: прогнозы с увеличенным `max_iter` не сравнивались "
        "(конфигурация `ElasticNetCV` зафиксирована спекой §3.3).",
        "- бенчмарк «медиана макроопроса ЦБ» не берётся (годовой горизонт "
        "выпусков, месячного сопоставления нет — спека §7).",
        "",
        "## Текущий nowcast",
        "",
    ]
    if nowcast:
        lines += [nowcast, ""]
    else:
        lines += [
            "Не рассчитывается в этом срезе (Task 5): здесь только walk-forward "
            "бенчмарков.",
            "",
        ]
    return "\n".join(lines).rstrip("\n") + "\n"


def write_artifacts(out_dir: Path, report: str, window_runs: list[WindowRun]) -> tuple:
    """Пишет markdown-отчёт и CSV фолдов в `out_dir` (LF; имена — константы).

    `window_runs` — окна прогона (`evaluate_windows`); CSV — «длинный» список
    фолдов всех окон: окно, модель, месяц, факт, прогноз, ошибка
    (`прогноз − факт`, п.п.); порядок — окна прогона, модели как зарегистрированы
    (у вырожденных строк нет), месяцы по возрастанию. Файлы gitignored: числа
    прогона зависят от БД и фиксируются протоколом приёмки, а не репозиторием.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / REPORT_NAME
    report_path.write_text(report.rstrip("\n") + "\n", encoding="utf-8")
    rows = []
    for run in window_runs:
        for name, group in _run_groups(run).items():
            for prediction in sorted(group, key=lambda item: item.month):
                rows.append(
                    {
                        "window": window_key(run.start, run.embargo),
                        "model": name,
                        "month": prediction.month.isoformat(),
                        "y_true": prediction.y_true,
                        "y_pred": prediction.y_pred,
                        "error": prediction.y_pred - prediction.y_true,
                    }
                )
    predictions_path = out_dir / PREDICTIONS_NAME
    _write_csv(predictions_path, PREDICTIONS_COLUMNS, rows)
    return report_path, predictions_path


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
# Прогон: сводка выборки, walk-forward бенчмарков, артефакты
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


def _print_walk_forward(runs: list[WindowRun]) -> None:
    """Консольный блок walk-forward: основное окно подробно, робастность — строкой.

    Полные таблицы «модель × окно» — в `cpi_nowcast_report.md`; консоль не
    повторяет отчёт и печатает робастные окна компактно (RMSE по моделям).
    Недосход (`fold_notes`, у B3 — `ConvergenceWarning`) печатается у тех
    моделей, которые его сообщают, — «недосход N из M» в конце строки метрик.
    """
    print("walk-forward (обучающее окно expanding; тест — следующий месяц):")
    if not runs:
        print("  окон нет")
        return
    for run in runs:
        title = (
            f"{window_title(run)}, {run.folds} "
            f"{_plural(run.folds, 'фолд', 'фолда', 'фолдов')}"
        )
        groups = _run_groups(run)
        if run.main:
            print(f"  {title}:")
            for name, group in groups.items():
                skipped = run.skipped.get(name, 0)
                note = _fold_note_cell(run.notes.get(name, {}), len(group))
                note_suffix = f", недосход {note}" if note != "—" else ""
                if group:
                    print(
                        f"    {model_label(name):<34} RMSE {rmse_pp(group):.3f}, "
                        f"MAE {mae_pp(group):.3f} (n={len(group)}, пропущено {skipped}"
                        f"{note_suffix})"
                    )
                else:
                    print(f"    {model_label(name):<34} фолдов нет (пропущено {skipped})")
        else:
            cells = []
            for name, group in groups.items():
                note = _fold_note_cell(run.notes.get(name, {}), len(group))
                cell = f"{model_label(name)} {_fmt(rmse_pp(group)) if group else '—'}"
                if note != "—":
                    cell += f" (недосход {note})"
                cells.append(cell)
            print(f"  {title}: RMSE {', '.join(cells)}")


def run_summary(
    start: int = MAIN_START,
    embargo_months: int = MAIN_EMBARGO,
    out_dir: Optional[Path] = None,
    today: Optional[date] = None,
) -> int:
    """Полный прогон: сводка выборки, walk-forward бенчмарков, отчёт и артефакты.

    `start`/`embargo_months` — основное окно walk-forward (`--start`,
    `--embargo-months`); робастные окна (второй старт и эмбарго 1) добавляет
    `window_specs`. Загрузка рядов, `build_sample` и `assert_months_purged` —
    контракт Task 2; walk-forward считает бенчмарки B1–B3 на одних фолдах
    (модели Task 4 подключаются к `BENCHMARKS`-словарю, отчёт не меняется).

    `out_dir` — каталог артефактов: корень worktree в живом прогоне (значение
    `ARTIFACTS_DIR` берётся в момент вызова, чтобы тесты могли его подменить),
    временный каталог — в тестах. `today` — дата прогона для отчёта (по
    умолчанию системная). Скрипт ничего не пишет в ClickHouse: только читает и
    кладёт `cpi_nowcast_report.md` и `cpi_nowcast_predictions.csv`.
    """
    out_dir = Path(out_dir) if out_dir is not None else ARTIFACTS_DIR
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
    print("CPI nowcast — walk-forward бенчмарков B1–B3 (модели CatBoost — Task 4)")
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
    _field("--start:", f"{start} (основное окно walk-forward)")
    _field("--embargo-months:", f"{embargo_months} (эмбарго основного окна)")

    runs = evaluate_windows(rows, BENCHMARKS, window_specs(start, embargo_months))
    _print_walk_forward(runs)
    report = render_report(rows, skipped, runs, nowcast=None, today=today)
    report_path, predictions_path = write_artifacts(out_dir, report, runs)
    _field("отчёт:", str(report_path))
    _field("прогнозы:", str(predictions_path))
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI nowcast: выборка, as-of, walk-forward, бенчмарки B1–B3, "
                    "отчёт (модели CatBoost — Task 4)."
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
        default=MAIN_START,
        help=f"минимальное обучающее окно walk-forward в месяцах (по умолчанию "
             f"{MAIN_START}); робастное окно старта {ROBUST_START} считается в том "
             f"же прогоне (спека §4)",
    )
    parser.add_argument(
        "--embargo-months",
        type=int,
        metavar="K",
        default=MAIN_EMBARGO,
        help=f"эмбарго основного окна walk-forward в месяцах (по умолчанию "
             f"{MAIN_EMBARGO}; спека §2.4); прогон с embargo {ROBUST_EMBARGO} "
             f"считается робастностью",
    )
    args = parser.parse_args(argv)
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    return run_summary(start=args.start, embargo_months=args.embargo_months)


if __name__ == "__main__":
    sys.exit(main())
