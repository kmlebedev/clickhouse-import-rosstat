#!/usr/bin/env python3
"""CPI monthly (ML-трек v2): данные, as-of, выборка, walk-forward и бенчмарки.

Срез месячного nowcast ИПЦ РФ (спека
`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md`, §2–§5.1): выборка
**155 месяцев 2013-10…2026-08** и два информационных множества на ОДНИХ И ТЕХ
ЖЕ строках — «без МП» (5 числовых + `month`) и «с МП» (плюс 7 признаков
мониторинга предприятий ЦБ из `cbr_survey`). Task 2 добавляет harness
walk-forward, обязательные бенчмарки B1–B3 (B3 — в двух пространствах),
метрики RMSE/MAE и отчёт-скелет; CatBoost — Task 3, DM-пары/сплит статьи/
живой nowcast — Task 4.

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
  базовые признаки, `feature_names(with_mp=True)` — базовые + 7 МП;
* **walk-forward** (`walk_forward`, §4.1) — расширяющееся окно: тест — строка
  `i`, обучение — `[0 … i−1−embargo]` (только прошлое). Первый тест — строка
  `start` (при 24 — 25-й месяц). `DegenerateWindowError` модели — пропуск фолда
  ЭТОЙ модели со счётчиком, а не падение прогона; `fold_notes` модели
  агрегируется по окну («недосход N из M»). Окна прогона (`window_specs`):
  основное (`--start`, `--embargo-months`) плюс робастность — старт 48 и
  эмбарго 1;
* **бенчмарки** (§3.4, фиксируются до результатов) — B1 `predict_last`
  (`y_lag1`), B2 `fit_ar1` (OLS `y_M` на `y_{M−1}` + 11 дамми месяца, HAC
  `maxlags=3`, минимум 14 строк) и B3 `fit_enet_base`/`fit_enet_mp`
  (`ElasticNetCV` на стандартизованных признаках без `month`, импутация NaN
  медианой train, минимум 12 строк; `ConvergenceWarning` — в `fold_notes`, не
  вырождение). Два пространства B3 отделяют эффект МП от эффекта алгоритма;
* **метрики и отчёт** (§4.2, §5.1) — RMSE/MAE в п.п. через общие функции
  `scripts/eval_utils.py`, блок «последние 12 фолдов»; отчёт-скелет
  `cpi_monthly_report.md` (выборка, «модель × окно», последние 12 фолдов, «что
  не проверено») и CSV `cpi_monthly_predictions.csv`
  (`month, window, model, y_true, y_pred`).

**NaN-политика:** в признаках этого среза NaN не возникает — отсутствие любой
обязательной точки выбрасывает месяц с причиной. B3 на всякий случай импутирует
NaN медианой ОБУЧАЮЩЕГО окна (колонка без наблюдений — 0.0: после
стандартизации вклада не даёт), `StandardScaler` подгоняется только на train.
CatBoost (Task 3) получит NaN как есть.

**As-of — приближение, а не point-in-time слой** (спека §2.6): точные даты
публикаций и ревизии рядов не учтены; PIT-слой — отдельный пункт фазы 4.

Фикстуры офлайн-тестов (`scripts/testdata/cpi_monthly/*.csv`) пересобираются из
живого dev-ClickHouse (пользователь default) той же целью, что и прогон:

    make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"

**Скрипт ничего не пишет в ClickHouse** — только читает; транспорт (`ch_query`)
переиспользуется из `scripts/extrapolation_aisc.py`, а не копируется. Секреты
приходят только из окружения (их подставляет Makefile). Запуск — через `make`:

* `make cpi-monthly` — сводка выборки (155 месяцев 2013-10…2026-08, пропусков
  0), walk-forward B1–B3 по окнам 24/48/embargo 1, RMSE/MAE и артефакты
  `cpi_monthly_report.md` и `cpi_monthly_predictions.csv` в корне worktree;
* `make cpi-monthly ARGS="--start N --embargo-months K"` — основное окно
  walk-forward задаётся явно (робастность остаётся от спеки §4.1);
* `make cpi-monthly ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
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

# Основное окно walk-forward (спека §4.1: старт 24) и робастность: второй старт
# 48 (как в докладе) и эмбарго 1. Флаги `--start`/`--embargo-months` управляют
# ОСНОВНЫМ окном, робастные добавляет `window_specs`.
DEFAULT_START = 24
DEFAULT_EMBARGO = 0
ROBUST_START = 48
ROBUST_EMBARGO = 1

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
# Walk-forward harness (спека §4.1)
# ---------------------------------------------------------------------------

# Контракт модели walk-forward: обучение на окне строк, прогноз тестовой строки.
FitPredict = Callable[[list[Row], Row], float]

# Ключ диагностики фолда в необязательном протоколе `fold_notes`: численный
# решатель модели не сошёлся (у B3 — `ConvergenceWarning` координатного спуска).
# Модель, выставляющая атрибут `fold_notes` (dict[str, int] о ПОСЛЕДНЕЙ
# подгонке), отчитывается по фолдам; harness накапливает значения по модели, а
# отчёт печатает «N из M» фолдов. У моделей без атрибута (B1, B2) диагностики
# нет — в отчёте «—».
NON_CONVERGED = "non_converged"


class DegenerateWindowError(ValueError):
    """Обучающее окно фолда вырождено: модель не оценивается, фолд пропускается.

    Поднимается моделями (B2/B3, позже CatBoost) там, где окно не позволяет
    оценить параметры: пустое или слишком короткое окно, нет вариации нужного
    ряда, неидентифицируемый дизайн. `walk_forward` пропускает фолд ЭТОЙ модели
    со счётчиком, а не роняет прогон: вырожденное окно — свойство окна, а не
    порча данных. Любое другое исключение пробрасывается — это баг.
    """


@dataclass(frozen=True)
class Prediction:
    """Фолд walk-forward: месяц, модель, факт и прогноз (п.п., м/м).

    Единицы — пункты процента, как у таргета §2.1: у бенчмарков, моделей Tasks
    3–4 и метрик одна шкала. `model` — имя из словаря `models` (у бенчмарков —
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
    фолдов»).
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


def window_specs(
    start: int = DEFAULT_START, embargo: int = DEFAULT_EMBARGO
) -> list[tuple[int, int]]:
    """Окна прогона: основное (`start`/`embargo`) и робастность спеки §4.1.

    Робастность — второй старт (`ROBUST_START`) и эмбарго 1; дубликаты и
    несовместимые окна (`embargo ≥ start` — обучать не на чем) отбрасываются,
    поэтому `--embargo-months 1` не даёт основное окно дважды.
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
    """Walk-forward по словарю моделей и одному окну (спека §4.1).

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
    без диагностики). Возврат — список прогнозов; счётчики нужны отчёту.
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
    заново (`WindowRun.notes`): число недосошедших фолдов относится к
    КОНКРЕТНОМУ окну, а не к прогону целиком.
    """
    runs = []
    for position, (start, embargo) in enumerate(
        window_specs() if specs is None else specs
    ):
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
# Бенчмарки B1–B3 (спека §3.4; фиксируются до результатов)
# ---------------------------------------------------------------------------

MODEL_LAST = "b1_last_value"
MODEL_AR1 = "b2_ar1_seasonal"
MODEL_ENET_BASE = "b3_elastic_net"
MODEL_ENET_MP = "b3_elastic_net_mp"

# Метки моделей для отчёта: бенчмарки названы номерами спеки §3.4. B3 идёт
# двумя пространствами — эффект МП отделяется от эффекта алгоритма.
MODEL_LABELS = {
    MODEL_LAST: "B1 last value",
    MODEL_AR1: "B2 AR(1)+сезонные дамми",
    MODEL_ENET_BASE: "B3 ElasticNetCV (без МП)",
    MODEL_ENET_MP: "B3 ElasticNetCV (с МП)",
}


def model_label(model: str) -> str:
    """Метка модели для отчёта: у бенчмарков — номер спеки, у прочих — имя."""
    return MODEL_LABELS.get(model, model)


def predict_last(train: list[Row], row: Row) -> float:
    """B1 «прошлый месяц» (спека §3.4): прогноз — `y_{M−1}`.

    Источник лага — тот же, что у моделей: `row.features['y_lag1']` — значение
    метки `M−1`, проверенное as-of-правилами §2.6. Брать `train[-1].y` нельзя:
    при `embargo > 0` последняя строка обучения — не месяц `M−1`, и бенчмарк
    перестал бы быть «прошлым месяцем». Обучающее окно не используется вовсе
    (параметр — часть контракта `FitPredict`).
    """
    return float(row.features["y_lag1"])


# --- B2: AR(1) с сезонными дамми ---------------------------------------------

HAC_MAXLAGS = 3  # спека §3.4: HAC (Ньюи–Уэст) с maxlags=3
AR1_DUMMY_MONTHS = tuple(range(2, 13))  # 11 дамми; январь — база
AR1_PARAMS = 2 + len(AR1_DUMMY_MONTHS)  # const + y_lag1 + дамми
AR1_MIN_TRAIN = 14  # спека §3.4: минимум 14 строк (не меньше, чем у v1)


def ar1_design(rows: list[Row]) -> tuple:
    """Дизайн B2: `[1, y_lag1, d_2 … d_12]` → `(X, y)`; `y` — таргет `cpi_all`.

    Дамми — ровно 11 (`AR1_DUMMY_MONTHS`), январь — база: спека §3.4 называет
    «11 дамми месяца». Значения берутся из признаков строки (`y_lag1`, `month`)
    — тот же источник, что у моделей; одна и та же функция строит дизайн
    обучения и тестовой строки, поэтому сдвиг месяцев в дамми не может
    возникнуть «только на предсказании».
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
    """B2: OLS `y_M` на `y_{M−1}` + 11 дамми месяца, ковариация HAC (спека §3.4).

    Конфигурация HAC (`maxlags=3`, `use_correction=True`) — часть спеки; она
    пинована тестом по АТРИБУТАМ результата (`fit_ar1.last_result`), а не по
    подменённому statsmodels: от ковариации зависят p-value будущих таблиц.

    Вырожденное окно — `DegenerateWindowError` (walk-forward пропускает фолд со
    счётчиком): строк меньше `AR1_MIN_TRAIN` (спека §3.4: ≥ 14) или `y_lag1`
    без вариации (наклон φ не идентифицирован).
    """
    if len(train) < AR1_MIN_TRAIN:
        raise DegenerateWindowError(
            f"B2 (AR(1)): обучающее окно {len(train)} месяцев короче "
            f"{AR1_MIN_TRAIN} — спека §3.4 требует минимум {AR1_MIN_TRAIN}"
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


# --- B3: ElasticNetCV в двух пространствах -----------------------------------

ENET_ALPHAS = np.logspace(-4, 1, 20)  # спека §3.4, дословно
ENET_L1_RATIOS = (0.1, 0.5, 0.9)  # спека §3.4, дословно
ENET_CV_SPLITS = 3  # TimeSeriesSplit(3) внутри train-окна фолда
ENET_MIN_TRAIN = 12  # спека §3.4: минимум 12 строк
ENET_MEDIAN_FALLBACK = 0.0  # колонка без наблюдений: нейтральная константа


def enet_features(with_mp: bool) -> list[str]:
    """Признаки B3: `feature_names(with_mp)` без `month` (как в v1 B3).

    `month` у B3 не участвует — сезонность там не моделируется (в отличие от
    CatBoost с `cat_features`), поэтому два пространства различаются только
    7 признаками МП.
    """
    return [name for name in feature_names(with_mp) if name != "month"]


def enet_matrix(rows: list[Row], with_mp: bool) -> np.ndarray:
    """Матрица признаков B3; NaN сохраняется — импутация позже (по train)."""
    names = enet_features(with_mp)
    return np.asarray(
        [[float(row.features[name]) for name in names] for row in rows],
        dtype=float,
    )


def enet_medians(matrix: np.ndarray) -> np.ndarray:
    """Медианы колонок train-окна; колонка без наблюдений — `ENET_MEDIAN_FALLBACK`.

    `np.nanmedian` на полностью NaN-колонке возвращает NaN и пишет
    `RuntimeWarning`; медиана здесь не определена, и колонка получает
    нейтральный ноль: после `StandardScaler` она нулевая и вклада в прогноз не
    даёт.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)
        medians = np.nanmedian(matrix, axis=0)
    return np.where(np.isnan(medians), ENET_MEDIAN_FALLBACK, medians)


@dataclass(frozen=True)
class EnetFit:
    """Подобранный фолд B3: пайплайн, медианы и матрица train (для тестов-пинов).

    `model` — `Pipeline` (StandardScaler → ElasticNetCV) на импутированной
    train-матрице; `medians` — медианы импутации окна; `imputed` — train-матрица
    после импутации (до стандартизации); `features` — имена колонок (base или
    base+МП). Отчёт ничего из этого не читает.
    """

    model: Pipeline
    medians: np.ndarray
    imputed: np.ndarray
    features: tuple


def _fit_enet(train: list[Row], row: Row, with_mp: bool) -> float:
    """Общий фит B3 для двух пространств (base и с МП).

    Конфигурация — дословно из спеки §3.4: `alphas=logspace(−4, 1, 20)`,
    `l1_ratio=[0.1, 0.5, 0.9]`, `cv=TimeSeriesSplit(3)`, `random_state=SEED`;
    сплит — ВНУТРИ train-окна фолда, тест в подбор гиперпараметров не попадает.

    NaN (в месячной выборке их нет, но политика зафиксирована) импутируется
    медианой ОБУЧАЮЩЕГО окна; `StandardScaler` подгоняется ТОЛЬКО на train
    (`fit` только на train, `transform` — train и тест): статистика теста в
    предобработку не попадает.

    Вырожденное окно (короче `ENET_MIN_TRAIN`) — `DegenerateWindowError`;
    `ConvergenceWarning` координатного спуска не вырождение: прогноз конечен и
    взят у выбранной CV-модели, а факт недосхода фиксируется в
    `fold_notes[NON_CONVERGED]` (harness агрегирует по окну, отчёт печатает
    «N из M»).
    """
    matrix = enet_matrix(train, with_mp)
    if matrix.shape[0] < ENET_MIN_TRAIN:
        raise DegenerateWindowError(
            f"B3 (ElasticNetCV): обучающее окно {matrix.shape[0]} месяцев короче "
            f"{ENET_MIN_TRAIN} — признаковое пространство не оценивается"
        )
    medians = enet_medians(matrix)
    imputed = np.where(np.isnan(matrix), medians, matrix)
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
        # Конфигурация (alphas до 1e-4, max_iter по умолчанию) закреплена спекой
        # §3.4 — менять её нельзя, поэтому недосход не печатается, а считается.
        # Прочие предупреждения не прячутся: они переиздаются после фита.
        warnings.simplefilter("always")
        model.fit(imputed, targets)
    non_converged = any(
        issubclass(warning.category, ConvergenceWarning) for warning in caught
    )
    for warning in caught:
        if not issubclass(warning.category, ConvergenceWarning):
            warnings.warn(str(warning.message), warning.category, stacklevel=2)
    _fit_enet.fold_notes = {NON_CONVERGED: int(non_converged)}
    _fit_enet.last_fit = EnetFit(
        model=model,
        medians=medians,
        imputed=imputed,
        features=tuple(enet_features(with_mp)),
    )
    test_matrix = enet_matrix([row], with_mp)
    imputed_test = np.where(np.isnan(test_matrix), medians, test_matrix)
    return float(model.predict(imputed_test)[0])


def fit_enet_base(train: list[Row], row: Row) -> float:
    """B3 без МП: ElasticNetCV на базовых признаках §2.4 (5 числовых)."""
    value = _fit_enet(train, row, with_mp=False)
    fit_enet_base.last_fit = _fit_enet.last_fit
    fit_enet_base.fold_notes = dict(_fit_enet.fold_notes)
    return value


def fit_enet_mp(train: list[Row], row: Row) -> float:
    """B3 с МП: ElasticNetCV на базовых признаках §2.4 + 7 признаках §2.5."""
    value = _fit_enet(train, row, with_mp=True)
    fit_enet_mp.last_fit = _fit_enet.last_fit
    fit_enet_mp.fold_notes = dict(_fit_enet.fold_notes)
    return value


# Диагностика последнего фолда B3 (и общий фит): читают тесты и harness; отчёт —
# нет. Атрибуты заполняются при каждом вызове.
for _function in (_fit_enet, fit_enet_base, fit_enet_mp):
    _function.last_fit = None
    _function.fold_notes = {}


# Бенчмарки спеки §3.4 — порядок B1, B2, B3-base, B3-mp задаёт строки таблиц
# отчёта и порядок пар будущих DM-тестов (Task 4).
BENCHMARKS = {
    MODEL_LAST: predict_last,
    MODEL_AR1: fit_ar1,
    MODEL_ENET_BASE: fit_enet_base,
    MODEL_ENET_MP: fit_enet_mp,
}

# Модели Task 2 — бенчмарки; CatBoost добавится в Task 3 тем же словарём.
MODELS = dict(BENCHMARKS)


# ---------------------------------------------------------------------------
# Метрики (спека §4.2)
# ---------------------------------------------------------------------------

LAST_FOLDS = 12  # блок «последние 12 фолдов»


def errors(predictions: list[Prediction]) -> list[float]:
    """Ошибки фолдов (прогноз − факт, п.п.) — вход общих функций `eval_utils`."""
    return [prediction.y_pred - prediction.y_true for prediction in predictions]


def rmse_pp(predictions: list[Prediction]) -> float:
    """RMSE фолдов в п.п. — тонкая обёртка над `eval_utils.rmse`."""
    return eu.rmse(errors(predictions))


def mae_pp(predictions: list[Prediction]) -> float:
    """MAE фолдов в п.п. — тонкая обёртка над `eval_utils.mae`."""
    return eu.mae(errors(predictions))


def last_fold_months(predictions: list[Prediction], count: int = LAST_FOLDS) -> set:
    """Месяцы последних `count` фолдов списка — общий отрезок для всех моделей.

    Блок «последние 12 фолдов» описывает один и тот же отрезок выборки: если
    модель пропустила фолд, у неё в таблице меньше строк, а не другой отрезок.
    """
    return set(sorted({prediction.month for prediction in predictions})[-count:])


# ---------------------------------------------------------------------------
# Отчёт-скелет и артефакты (спека §5.1)
# ---------------------------------------------------------------------------

REPORT_NAME = "cpi_monthly_report.md"
PREDICTIONS_NAME = "cpi_monthly_predictions.csv"

# Колонки CSV прогнозов — контракт Tasks 3–4 и приёмки (порядок из спеки §5.1).
PREDICTIONS_COLUMNS = ("month", "window", "model", "y_true", "y_pred")


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


def _months_suffix(months: list, limit: int = 6) -> str:
    """Список месяцев `(2013-10, …)` — для строки о пропущенных месяцах."""
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


def _fold_note_cell(notes: dict, folds: int) -> str:
    """Ячейка «недосход»: `N из M` у моделей с диагностикой, «—» у остальных.

    `notes` — накопленная диагностика модели за окно (`WindowRun.notes`),
    `folds` — число ЕЁ фолдов, по которым считался недосход (0 фолдов — «—»).
    """
    if NON_CONVERGED not in notes or folds == 0:
        return "—"
    return f"{notes[NON_CONVERGED]} из {folds}"


_METRIC_HEADER = (
    "| модель | окно | n | RMSE, п.п. | MAE, п.п. | пропущено (вырожденное окно) | примечания |",
    "|---|---|---|---|---|---|---|",
)
_LAST_FOLDS_HEADER = (
    "| модель | n | RMSE, п.п. | MAE, п.п. |",
    "|---|---|---|---|",
)


def render_report(
    sample: list[Row],
    skipped: list,
    window_runs: list[WindowRun],
    today: Optional[date] = None,
) -> str:
    """Markdown-отчёт-скелет Task 2: выборка, «модель × окно», последние 12 фолдов.

    `sample`/`skipped` — выборка `build_sample` (строки и месяцы без собранных
    признаков); `window_runs` — по одному `WindowRun` на окно walk-forward
    (порядок окон задаёт строки таблиц, основное помечено `WindowRun.main`);
    `today` — дата прогона (по умолчанию системная).

    Таблица «модель × окно» перечисляет ВСЕ модели из счётчиков окон (модель,
    вырожденная на всех окнах, видна строкой «0 фолдов», а не пропадает);
    метрики — из фолдов, «недосход» — из `WindowRun.notes`. CatBoost, DM-пары,
    сплит статьи и живой nowcast появятся в Tasks 3–4; раздел «Что не проверено»
    называет это прямо, чтобы отчёт не читался как полный результат.
    """
    today = today or date.today()
    runs = list(window_runs)
    main_run = next((run for run in runs if run.main), runs[0] if runs else None)
    first_run = runs[0] if runs else None
    main_window = (
        f"`{window_key(first_run.start, first_run.embargo)}`"
        if first_run is not None
        else "—"
    )
    lines = [
        "# CPI monthly (ML-трек v2) — walk-forward, бенчмарки B1–B3 и метрики",
        "",
        f"Дата прогона: {today.isoformat()}; сид прогона `SEED={SEED}`. Спека: "
        "`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md` (§2 — данные и "
        "признаки, §3.4 — бенчмарки, §4.1–4.2 — walk-forward и метрики); план: "
        "`docs/superpowers/plans/2026-10-18-cpi-monthly-mp.md`.",
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
    skipped_months = [month for month, _ in skipped]
    lines += [
        f"| месяцев в выборке | {len(sample)} ({span}) |",
        f"| пропущено (признак не собран) | {len(skipped)}{_months_suffix(skipped_months)} |",
        "| purge-проверка (структурная) | OK |",
        f"| признаки «без МП» | {', '.join(feature_names(with_mp=False))} |",
        f"| признаки «с МП» | {', '.join(feature_names(with_mp=True))} |",
        "",
        "Строки выборки ОБЩИЕ для наборов «без МП» и «с МП»: месяц без любого "
        "обязательного признака отсутствует в обоих, поэтому фолды наборов "
        "совпадают. As-of — консервативные правила §2.6: собственный таргет "
        "месяца и точка МП месяца `M` в признаки не попадают; точка после "
        "cutoff — тоже.",
        "",
        "## Walk-forward",
        "",
        "Расширяющееся обучающее окно, переобучение на каждом фолде: тест — месяц "
        "`i`, обучение — строки `[0 … i−1−embargo]` (только прошлое; embargo > 0 "
        "выбрасывает месяц перед тестом). Единый train/test split не используется "
        f"(спека §4.1). Основное окно — {main_window}; робастность — второй старт "
        f"{ROBUST_START} и embargo {ROBUST_EMBARGO}. Вырожденное обучающее окно "
        "(`DegenerateWindowError` модели) — пропуск фолда ЭТОЙ модели со счётчиком, "
        "а не падение прогона. «Недосход» — фолды, где координатный спуск B3 не "
        "сошёлся (`ConvergenceWarning`), вырождением не считается и публикуется "
        "числом.",
        "",
        "## Модель × окно",
        "",
        "Ошибка фолда — `прогноз − факт` (п.п., м/м); `n` — число фолдов под "
        "метрикой. RMSE/MAE — общие функции `scripts/eval_utils.py`. B1 и B2 от "
        "набора признаков не зависят; B3 считается в двух пространствах "
        "(без МП и с МП), чтобы отделить эффект МП от эффекта алгоритма.",
        "",
    ]
    metric_rows = []
    for run in runs:
        for name, group in _run_groups(run).items():
            if group:
                n_cell = str(len(group))
                rmse_cell = _fmt(rmse_pp(group))
                mae_cell = _fmt(mae_pp(group))
            else:
                n_cell = "0 фолдов"
                rmse_cell = mae_cell = "—"
            metric_rows.append(
                f"| {model_label(name)} | {window_title(run)} | {n_cell} | "
                f"{rmse_cell} | {mae_cell} | {run.skipped.get(name, 0)} | "
                f"{_fold_note_cell(run.notes.get(name, {}), len(group))} |"
            )
    lines += _table(_METRIC_HEADER, metric_rows)
    lines += [""]
    if main_run is not None:
        tail_months = last_fold_months(main_run.predictions)
        last_rows = []
        for name, group in _run_groups(main_run).items():
            tail = [prediction for prediction in group if prediction.month in tail_months]
            last_rows.append(
                f"| {model_label(name)} | {len(tail)} | "
                f"{_fmt(rmse_pp(tail)) if tail else '—'} | "
                f"{_fmt(mae_pp(tail)) if tail else '—'} |"
            )
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
    lines += [
        "## Что не проверено",
        "",
        "- CatBoost: четыре варианта (`catboost_direct`, `catboost_bottomup`, "
        "`catboost_direct_mp`, `catboost_bottomup_mp`) и веса NNLS — Task 3; здесь "
        "только бенчмарки B1–B3.",
        "- DM-HLN «с МП vs без МП» и secondary-пары, фиксированный сплит статьи "
        "2019-01…2024-07, строка «2022 исключён» и живой nowcast — Task 4; "
        "таблицы значимости в этом скелете нет.",
        "- as-of — приближение, а не point-in-time слой: точные даты публикаций и "
        "ревизии рядов не учтены; PIT — отдельный пункт фазы 4.",
        "- гиперпараметры не тюнинговались: B2 (HAC, 11 дамми, минимум 14 строк) "
        "и B3 (`ElasticNetCV(alphas=logspace(−4,1,20), l1_ratio=[0.1,0.5,0.9], "
        "cv=TimeSeriesSplit(3), random_state=SEED)`, минимум 12 строк) "
        "зафиксированы спекой §3.4 до результатов.",
        "- недосход B3 (`ConvergenceWarning` координатного спуска) считается по "
        "фолдам, но влияние недосчёта на прогноз не измерено: конфигурация "
        "`ElasticNetCV` зафиксирована спекой.",
        "- embargo 1 — только робастность: перекрытие окон признаков тестового и "
        "обучающих месяцев структурно исключено purge-проверкой, но границы "
        "доступности — консервативное правило §2.6, а не фактические даты.",
        "- режимы отдельно не тестируются: 2022 год (шок) входит в общую выборку, "
        "блок «последние 12 фолдов» — описательный, не тест.",
        "- числа статьи напрямую не сравнимы: другой таргет, длина train и "
        "обработка выбросов.",
        "",
    ]
    return "\n".join(lines).rstrip("\n") + "\n"


def write_artifacts(
    out_dir: Path, report: str, window_runs: list[WindowRun]
) -> tuple:
    """Пишет markdown-отчёт и CSV фолдов в `out_dir` (LF; имена — константы).

    CSV — «длинный» список фолдов всех окон: месяц, окно, модель, факт,
    прогноз (порядок колонок — `PREDICTIONS_COLUMNS`); порядок строк — окна
    прогона, модели как зарегистрированы (у вырожденных строк нет), месяцы по
    возрастанию. Файлы gitignored: числа прогона зависят от БД и фиксируются
    протоколом приёмки, а не репозиторием.
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
                        "month": prediction.month.isoformat(),
                        "window": window_key(run.start, run.embargo),
                        "model": name,
                        "y_true": prediction.y_true,
                        "y_pred": prediction.y_pred,
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


def _print_walk_forward(runs: list[WindowRun]) -> None:
    """Консольный блок walk-forward: основное окно подробно, робастность — строкой.

    Полные таблицы «модель × окно» — в `cpi_monthly_report.md`; консоль не
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
    start: int = DEFAULT_START,
    embargo_months: int = DEFAULT_EMBARGO,
    out_dir: Optional[Path] = None,
    today: Optional[date] = None,
) -> int:
    """Полный прогон Task 2: выборка, walk-forward B1–B3, метрики и артефакты.

    `start`/`embargo_months` — основное окно walk-forward (`--start`,
    `--embargo-months`); робастные окна (второй старт 48 и эмбарго 1) добавляет
    `window_specs`. Загрузка рядов, `build_sample` и `assert_months_purged` —
    контракт Task 1; walk-forward считает бенчмарки B1–B3 (`MODELS`) на одних
    фолдах, отчёт-скелет (`cpi_monthly_report.md`) и CSV прогнозов
    (`cpi_monthly_predictions.csv`) пишутся в `out_dir` (по умолчанию корень
    worktree; файлы gitignored).

    Ветка «walk-forward не дал ни одного прогноза» (пустая/короткая выборка)
    ничего не пишет и называет это в консоли. Скрипт ничего не пишет в
    ClickHouse: только читает и кладёт локальные артефакты.
    """
    out_dir = Path(out_dir) if out_dir is not None else ARTIFACTS_DIR
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
    print("CPI monthly (ML-трек v2) — выборка, walk-forward и бенчмарки B1–B3")
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
    _field("модели:", ", ".join(model_label(name) for name in MODELS))
    _field("purge-проверка (структурная):", "OK")
    if skipped:
        print("  пропущенные месяцы и причины:")
        for month, reason in skipped[:MAX_SKIPPED_PRINTED]:
            print(f"    {month.isoformat()[:7]} — {reason}")
        hidden = len(skipped) - MAX_SKIPPED_PRINTED
        if hidden > 0:
            print(f"    … и ещё {hidden} {_plural(hidden, 'месяц', 'месяца', 'месяцев')}")
    _field("--start:", f"{start} (основное окно walk-forward)")
    _field("--embargo-months:", f"{embargo_months} (эмбарго основного окна)")

    runs = evaluate_windows(rows, MODELS, window_specs(start, embargo_months))
    _print_walk_forward(runs)
    if not any(run.predictions for run in runs):
        _field("отчёт/CSV:", "не пишутся (walk-forward не дал ни одного прогноза)")
        return 0
    report = render_report(rows, skipped, runs, today=today)
    report_path, predictions_path = write_artifacts(out_dir, report, runs)
    _field("отчёт:", str(report_path))
    _field("прогнозы (CSV):", str(predictions_path))
    return 0


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI monthly (ML-трек v2): выборка, as-of, признаки, "
                    "walk-forward, бенчмарки B1–B3, метрики и отчёт."
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
             f"{DEFAULT_START}); робастное окно старта {ROBUST_START} считается в "
             f"том же прогоне (спека §4.1)",
    )
    parser.add_argument(
        "--embargo-months",
        type=int,
        metavar="K",
        default=DEFAULT_EMBARGO,
        help=f"эмбарго основного окна walk-forward в месяцах (по умолчанию "
             f"{DEFAULT_EMBARGO}); прогон с embargo {ROBUST_EMBARGO} считается "
             f"робастностью",
    )
    args = parser.parse_args(argv)
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    return run_summary(start=args.start, embargo_months=args.embargo_months)


if __name__ == "__main__":
    sys.exit(main())
