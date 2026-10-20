#!/usr/bin/env python3
"""CPI-nowcast (ML-трек контура ставки): выборка, as-of, walk-forward, модели.

Срез ML-трека (спека `docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md`
§2–§5): nowcast месячного ИПЦ РФ до публикации Росстата. Здесь — данные (§2),
харнесс walk-forward (§4), обязательные бенчмарки B1–B3 (§3.3), модели CatBoost
§3.1–3.2 (`catboost_direct`, покомпонентный `catboost_bottomup` с весами NNLS)
и текущий nowcast с артефактами (§5: `cpi_nowcast_forecast.json`).

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
* **модели** (§3.1–3.2) — `catboost_direct` (`fit_catboost_direct`: CatBoost на
  общий ИПЦ) и `catboost_bottomup` (`fit_catboost_bottomup`: `Σ w_j · ŷ_j` — три
  компонентных CatBoost и веса NNLS общего ИПЦ на компоненты ПО ОБУЧАЮЩЕМУ окну
  фолда, нормированные к 1; вырожденность любой компоненты пропускает фолд
  целиком). Конфигурация CatBoost — `loss_function='RMSE'`, `random_seed=SEED`,
  `verbose=False`, `cat_features=['month']`, `monotone_constraints` `+1` у
  `share_rising_wk`, `median_wk4`, `gasoline_4w`, `usd_20d` (порядок —
  `feature_names()`, у категориального 0); остальные параметры — дефолты
  библиотеки, зафиксированные в `CATBOOST_DEFAULTS`/`catboost_metadata` и
  пинуемые тестом через `get_params()`. Вход — numpy-матрица `dtype=object`
  (числовые float + int-коды месяца: иначе CatBoost не принимает `cat_features`),
  NaN — как есть (`nan_mode` по умолчанию);
* **метрики** (§4) — RMSE/MAE в п.п. через общие функции
  `scripts/lib/eval_utils.py`, DM (поправка HLN, `h=1`) и блочный бутстрап разницы
  RMSE (блок 3 месяца, n=2000, seed=SEED) между моделями основного окна;
* **артефакты** (§5) — `cpi_nowcast_report.md` (таблицы «модель × окно»,
  «последние 12 фолдов», веса NNLS по последнему фолду каждого окна, тесты
  значимости, «Текущий nowcast», «что не проверено»), `cpi_nowcast_predictions.csv`
  (фолды walk-forward) и `cpi_nowcast_forecast.json` (текущий nowcast) в корне
  worktree; все gitignored — числа прогона фиксируются протоколом приёмки, а не
  репозиторием;
* **текущий nowcast** (§5, Task 5) — `current_nowcast`: цель — месяц за последним
  ОПУБЛИКОВАННЫМ (метка общего ИПЦ уже есть в `rosstat_ipc_mes`; на 2026-10-16 —
  октябрь-2026), признаки — as-of даты прогона, недельное окно — 1…4 недели,
  опубликованные после конца `M−1` (частичное окно — пометка «частичное окно: N
  из 4 недель» в `note` JSON и в разделе отчёта); модели и бенчмарки обучаются на
  ПОЛНОЙ выборке, веса bottom-up — NNLS по ней же. Ветка «нет цели живого
  nowcast'а» бэктест НЕ отменяет: отчёт и CSV пишутся как обычно (walk-forward
  валиден и без живого прогноза), stale-`cpi_nowcast_forecast.json` удаляется
  (`write_nowcast_artifact`), причина — в разделе отчёта
  (`render_nowcast_absent`) и консоли. Отдельная ветка «ни одного прогноза
  вообще» (пустая выборка или выборка короче старта walk-forward) не пишет
  ничего, а все три артефакта прошлого прогона подчищает
  (`clear_stale_artifacts`) — образец Task 6 ветки v0; прогон не падает ни в
  одной из веток.

**NaN-политика (решение контроллера; см. `fit_enet`).** В признаках допустим
NaN: сейчас это `ofz_1y` во всём бэктесте (у G-curve МосБиржи нет истории — см.
ниже). CatBoost получит NaN как есть (`nan_mode` по умолчанию обрабатывает
пропуск нативно); `ElasticNetCV` NaN не принимает,
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

**В ClickHouse скрипт по умолчанию не пишет** — только читает; с `ML_LOG=1`
паспорт прогона и fold-предсказания уходят в `ml_runs`/`ml_predictions`
(`scripts/lib/ml_experiment.py`), а без переменной поведение прежнее. Транспорт
(`ch_query`) переиспользуется из `scripts/models/polyus/aisc.py`, а не
копируется. Секреты приходят только из окружения (их подставляет Makefile).
Запуск — через `make`:

* `make cpi-nowcast` — сводка выборки, walk-forward моделей и бенчмарков по окнам
  (основное — старт 24, робастность — старт 48 и эмбарго 1), веса NNLS, текущий
  nowcast, отчёт и CSV прогнозов в корне worktree;
* `make cpi-nowcast ARGS="--start N --embargo-months K"` — основное окно
  задаётся явно (робастность остаётся от спеки);
* `make cpi-nowcast ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
"""

import argparse
import csv
import json
import logging
import math
import os
import statistics
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

import numpy as np
import statsmodels.api as sm
from catboost import CatBoostRegressor
from scipy.optimize import nnls
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]

log = logging.getLogger(__name__)

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu
from lib import clickhouse as ch
from lib import ml_experiment as mlx

ch_query = ch.ch_query

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


def run_date(today: Optional[date] = None) -> date:
    """Дата прогона: явная (тесты) или системная — ОДНА точка на модуль.

    `date.today()` вызывается ровно здесь: и `run_summary`, и `render_report`
    резолвят дату через этот хелпер, а не дублируют `today or date.today()`
    (дубль мог бы разъехаться в полночь между двумя вызовами).
    """
    return today or date.today()


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


def weekly_window_features(window: list) -> Optional[dict[str, float]]:
    """Признаки §2.3 пп. 1–4 по готовому окну недель (порядок — календарный).

    Одна формула и для бэктеста (`weekly_features` — ровно `WEEKLY_WINDOW_WEEKS`
    последних опубликованных недель), и для живого nowcast (`nowcast_window` —
    1…4 недели после конца `M−1`): иначе сравнение живого прогноза с бэктестом
    теряло бы смысл. `share_rising_wk` и `median_wk` — по ПОСЛЕДНЕЙ неделе окна
    (в неполном окне она свежайшая, а не четвёртая по счёту), `median_wk4` —
    среднее недельных медиан, `gasoline_4w` — сумма изменений бензина.

    `None` — у бензина нет точки в какой-то из недель окна: агрегат по неполной
    корзине сместил бы признак. Пустое окно — тоже `None`.
    """
    if not window:
        return None
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
    не добирается более старой точкой сверх окна. Сами признаки считает общая
    `weekly_window_features` — та же формула у живого nowcast (Task 5).
    """
    weeks = eligible_weeks(weekly, cutoff)
    if len(weeks) < WEEKLY_WINDOW_WEEKS:
        return None
    return weekly_window_features(weeks[-WEEKLY_WINDOW_WEEKS:])


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
    теряется: фолд помечается диагностикой `fold_notes[NON_CONVERGED]`.
    В БЭКТЕСТЕ harness агрегирует её по окну, и отчёт печатает «недосход: N из
    M»; в ЖИВОМ пути (`current_nowcast`, Task 5) последний фит читается сразу
    после вызова, и недосход называется в `note` артефакта («прогноз
    индикативен») — здесь, в отличие от бэктеста, терять его нечем. Подобранный
    пайплайн и матрицы остаются в `fit_enet.last_fit`, диагностика последнего
    фолда — в `fit_enet.fold_notes` (протокол `walk_forward`).
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
# CatBoost: direct и покомпонентный bottom-up (спека §3.1–3.2)
# ---------------------------------------------------------------------------

MODEL_CATBOOST_DIRECT = "catboost_direct"
MODEL_CATBOOST_BOTTOMUP = "catboost_bottomup"

# Компоненты bottom-up (§3.2) — коды таргета §2.1. Порядок задаёт и порядок весов
# в `nnls_weights`, и колонки блока весов в отчёте; перестановка молча приписала
# бы веса другим компонентам, поэтому имена пинуются тестом.
BOTTOMUP_COMPONENTS = ("cpi_food", "cpi_nonfood", "cpi_services")

# Конфигурация CatBoost (§3.1): заданы `loss_function`, `random_seed`, `verbose`
# (дословно из спеки), monotone-ограничения и категориальный месяц. Остальные
# гиперпараметры НЕ задаются — дефолты библиотеки; `CATBOOST_DEFAULTS` фиксирует
# их значения для метаданных прогона («дефолты фиксируются в метаданных модели»),
# тест сверяет их с `get_all_params()` обученной модели.
CATBOOST_LOSS = "RMSE"
CATBOOST_CAT_FEATURES = ("month",)
CATBOOST_MONOTONE = {"share_rising_wk": 1, "median_wk4": 1, "gasoline_4w": 1, "usd_20d": 1}
CATBOOST_DEFAULTS = {
    "iterations": 1000,
    "depth": 6,
    "l2_leaf_reg": 3,
    "random_strength": 1,
    "bootstrap_type": "MVS",
    "nan_mode": "Min",
    "leaf_estimation_method": "Newton",
    "one_hot_max_size": 2,
}

# Матрица признаков: 9 числовых (`ENET_FEATURES` без `month`) + категориальный
# месяц последним столбцом — индекс для `cat_features` берётся отсюда, а не из
# литерала, поэтому сдвиг `feature_names()` не разъедет столбцы и ограничения.
CATBOOST_NUMERIC_FEATURES = tuple(
    name for name in feature_names() if name not in CATBOOST_CAT_FEATURES
)
CATBOOST_CAT_INDICES = tuple(feature_names().index(name) for name in CATBOOST_CAT_FEATURES)


def monotone_constraints() -> list[int]:
    """`monotone_constraints` §3.1: `+1` четырём признакам, остальным 0.

    Ограничения заданы ИМЕНАМИ признаков, а возвращаются позиционно (индекс
    столбца матрицы — так их читает CatBoost): сдвиг порядка `feature_names()`
    не может молча перенести «+1» на другой признак. У категориального `month` —
    0: CatBoost допускает ограничения только на float-признаках (проверка
    библиотеки: «Monotone constraints may be imposed only on float features»).
    """
    return [CATBOOST_MONOTONE.get(name, 0) for name in feature_names()]


def catboost_params() -> dict:
    """Параметры конструктора CatBoost (§3.1) — единственные заданные, дословно.

    Словарь — ровно то, что уходит в `CatBoostRegressor`; он же сверяется с
    `get_params()` собранной модели, поэтому любое добавленное поле означало бы
    подменённый дефолт библиотеки. `allow_writing_files=False` — флаг побочного
    эффекта, а не гиперпараметр: без него CatBoost пишет каталог `catboost_info/`
    в рабочий каталог прогона (артефакты прогона должны лежать только в своём
    каталоге).
    """
    return {
        "loss_function": CATBOOST_LOSS,
        "random_seed": SEED,
        "verbose": False,
        "monotone_constraints": monotone_constraints(),
        "cat_features": list(CATBOOST_CAT_INDICES),
        "allow_writing_files": False,
    }


def catboost_model() -> CatBoostRegressor:
    """Новый (необученный) `CatBoostRegressor` с конфигурацией §3.1."""
    return CatBoostRegressor(**catboost_params())


def catboost_metadata() -> dict:
    """Метаданные модели CatBoost для артефактов прогона (§3.1; читают отчёт и JSON).

    Заданная конфигурация — по ИМЕНАМ признаков (индекс сдвинется при правке
    `feature_names()`, имя нет) плюс дефолты библиотеки, которые код не
    подменяет. Словарь сериализуется в JSON как есть (`forecast_payload` кладёт
    его в `cpi_nowcast_forecast.json` рядом с прогнозом каждой модели).
    """
    constraints = monotone_constraints()
    return {
        "loss_function": CATBOOST_LOSS,
        "random_seed": SEED,
        "cat_features": list(CATBOOST_CAT_FEATURES),
        "num_features": list(CATBOOST_NUMERIC_FEATURES),
        "monotone_constraints": {
            name: value for name, value in zip(feature_names(), constraints)
        },
        "defaults": dict(CATBOOST_DEFAULTS),
    }


def catboost_matrix(rows: list[Row]) -> np.ndarray:
    """Матрица признаков CatBoost: числовые float + целые коды месяца (§2.3).

    Тип — `object`, и это не украшение: `cat_features` требует не-float матрицу
    (CatBoost отказывает: «'data' is numpy array of floating point numerical
    type, it means no categorical features»), а код месяца обязан дойти целым —
    «10» и «10.0» для категориального признака разные значения. NaN числовых
    колонок передаётся как есть: библиотека обрабатывает пропуски нативно
    (`nan_mode` по умолчанию `Min`), в отличие от B3, которая импутирует.
    """
    matrix = np.empty((len(rows), len(feature_names())), dtype=object)
    for position, name in enumerate(CATBOOST_NUMERIC_FEATURES):
        matrix[:, position] = [float(row.features[name]) for row in rows]
    for offset, name in enumerate(CATBOOST_CAT_FEATURES):
        matrix[:, len(CATBOOST_NUMERIC_FEATURES) + offset] = [
            int(row.features[name]) for row in rows
        ]
    return matrix


@dataclass(frozen=True)
class CatBoostFit:
    """Подобранный фолд CatBoost: модель, матрица и таргет окна (для тестов-пинов).

    `target` — код таргета §2.1, на который обучалась модель (`TARGET_ALL` у
    direct, код компоненты у bottom-up), `targets` — значения этого таргета на
    обучающем окне. Отчёт ничего из этого не читает.
    """

    model: CatBoostRegressor
    matrix: np.ndarray
    target: str
    targets: np.ndarray


def _catboost_targets(train: list[Row], target: str) -> np.ndarray:
    """Значения таргета обучающего окна (float) — общий ИПЦ или компонента §2.1."""
    return np.asarray([float(row.y[target]) for row in train], dtype=float)


def _assert_catboost_window(matrix: np.ndarray, targets: np.ndarray, target: str) -> None:
    """Вырожденное окно CatBoost: пустое или с неизменным таргетом.

    Неизменный таргет — это отказ библиотеки («All train targets are equal»), а
    не порча данных: окно вырождено, и walk-forward пропускает фолд этой модели
    со счётчиком (`DegenerateWindowError`), как B2/B3 на коротком окне.
    """
    if matrix.shape[0] == 0:
        raise DegenerateWindowError(
            f"CatBoost ({target}): пустое обучающее окно — модель не оценивается"
        )
    if len(set(targets.tolist())) < 2:
        raise DegenerateWindowError(
            f"CatBoost ({target}): таргет обучающего окна не меняется — модель "
            f"не оценивается (CatBoost: «All train targets are equal»)"
        )


def _fit_catboost(train: list[Row], target: str) -> CatBoostFit:
    """Обучает CatBoost §3.1 на окне `train` для таргета `target`.

    Обучение только на переданном окне (никаких обращений к выборке целиком):
    фолд walk-forward передаёт сюда исключительно прошлые месяцы.
    """
    matrix = catboost_matrix(train)
    targets = _catboost_targets(train, target)
    _assert_catboost_window(matrix, targets, target)
    model = catboost_model()
    model.fit(matrix, targets)
    return CatBoostFit(model=model, matrix=matrix, target=target, targets=targets)


def catboost_forecast(fit: CatBoostFit, row: Row) -> float:
    """Прогноз обученной модели для тестовой строки (та же матрица признаков)."""
    return float(fit.model.predict(catboost_matrix([row]))[0])


def fit_catboost_direct(train: list[Row], row: Row) -> float:
    """`catboost_direct` (§3.2): CatBoost на таргет `cpi_all`, прогноз в п.п.

    Агрегация не применяется: модель прогнозирует общий ИПЦ напрямую. Подгонка —
    на каждом фолде заново, только по обучающему окну; подобранная модель
    последнего фолда остаётся в `fit_catboost_direct.last_fit` (пины конфигурации
    и метаданные прогона).
    """
    fit = _fit_catboost(train, TARGET_ALL)
    fit_catboost_direct.last_fit = fit
    return catboost_forecast(fit, row)


# Последний подобранный фолд direct: читают тесты (пины параметров и дефолтов) и
# метаданные прогона; отчёт — нет. Атрибут заполняется при каждом вызове.
fit_catboost_direct.last_fit = None


def nnls_weights(train: list[Row]) -> tuple[float, float, float]:
    """NNLS-веса компонент bottom-up по обучающему окну фолда (спека §3.2).

    Неотрицательная регрессия (`scipy.optimize.nnls`) общего ИПЦ на метки трёх
    компонент ТОЛЬКО обучающего окна: тестовый месяц и будущие месяцы в подгонку
    не попадают (утечки нет), веса нормируются к сумме 1 (`w / Σw`), порядок —
    `BOTTOMUP_COMPONENTS`. Вырожденное окно — `DegenerateWindowError`: пустое
    окно или нулевое решение (Σw = 0 — веса не определены, например все метки
    компонент окна нулевые); walk-forward пропускает такой фолд со счётчиком.
    """
    if not train:
        raise DegenerateWindowError("NNLS: пустое обучающее окно — веса не определены")
    design = np.asarray(
        [[float(row.y[code]) for code in BOTTOMUP_COMPONENTS] for row in train], dtype=float
    )
    target = np.asarray([float(row.y[TARGET_ALL]) for row in train], dtype=float)
    weights, _ = nnls(design, target)
    total = float(weights.sum())
    if not total > 0.0:
        raise DegenerateWindowError(
            "NNLS: нулевое решение — сумма весов 0 (регрессия на компоненты "
            "обучающего окна не определена)"
        )
    return tuple(float(weight / total) for weight in weights)


def fit_catboost_component(train: list[Row], row: Row, code: str) -> float:
    """Один компонентный CatBoost (§3.2): таргет — метка компоненты `code`.

    Отдельная функция, а не цикл внутри bottom-up, — точка подмены для тестов:
    сверка агрегата `Σ w·ŷ` на заглушечных компонентных прогнозах.
    """
    return catboost_forecast(_fit_catboost(train, code), row)


def fit_catboost_bottomup(train: list[Row], row: Row) -> float:
    """`catboost_bottomup` (§3.2): `Σ w_j · ŷ_j` — компоненты и веса NNLS.

    Веса считаются по ТОМУ ЖЕ обучающему окну фолда, что и компонентные модели
    (утечки нет), и нормированы к 1. Вырожденность ЛЮБОГО компонентного фита или
    весов (`DegenerateWindowError`) пропускает фолд целиком: частичный агрегат
    («два компонента из трёх») прогнозом общего ИПЦ не является. Веса и
    компонентные прогнозы последнего фолда остаются в `last_weights` и
    `last_components` (читает отчёт — блок весов NNLS).
    """
    weights = nnls_weights(train)
    components = [fit_catboost_component(train, row, code) for code in BOTTOMUP_COMPONENTS]
    fit_catboost_bottomup.last_weights = weights
    fit_catboost_bottomup.last_components = tuple(components)
    return float(sum(weight * value for weight, value in zip(weights, components)))


# Веса и компонентные прогнозы последнего фолда bottom-up: читают тесты и отчёт;
# заполняются при каждом вызове. `last_weights`/`last_components` — не часть
# контракта `FitPredict`, а диагностика фолда.
fit_catboost_bottomup.last_weights = ()
fit_catboost_bottomup.last_components = ()


# Модели прогона (§3.1–3.2): бенчмарки §3.3, затем CatBoost — порядок словаря
# задаёт строки таблиц отчёта и порядок пар DM (§4). Пары «CatBoost против
# каждого бенчмарка» и «bottom-up против direct» собирает существующий механизм
# `pairwise_tests` по списку прогнозов окна — правок функции не требуется.
CATBOOST_MODELS = {
    MODEL_CATBOOST_DIRECT: fit_catboost_direct,
    MODEL_CATBOOST_BOTTOMUP: fit_catboost_bottomup,
}

MODELS = {**BENCHMARKS, **CATBOOST_MODELS}


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
# Текущий nowcast (спека §5; Task 5)
# ---------------------------------------------------------------------------
#
# Живой прогноз, а не фолд walk-forward: модели обучаются на ПОЛНОЙ выборке, а
# признаки снимаются с данных, доступных на дату прогона. Два отличия от
# бэктеста диктует сам живой режим (бэктест не трогается):
#
# * cutoff — дата прогона, а не 8-е число M+1 (признаки as-of даты прогона);
# * недельное окно — 1…4 недели, опубликованные после конца M−1 (в бэктесте
#   строго 4: неполное окно месяц выбрасывает). Неполное окно — пометка
#   «частичное окно: N из 4 недель» в note артефакта и в разделе отчёта.


def latest_published_month(targets: dict) -> Optional[date]:
    """Последний месяц, чья метка ОБЩЕГО ИПЦ уже опубликована (есть в данных).

    Живой режим: месячный ИПЦ импортируется из релиза Росстата (10–16-е числа
    M+1), поэтому «в rosstat_ipc_mes есть точка месяца» и есть «опубликовано к
    дате прогона». Бэктест так делать не может — там cutoff внутри M+1 и
    собственная метка месяца не должна попадать в признаки, зато правило
    §2.4 (`available_from_mes`, конец M+1) отсекает свежие точки.

    Ряд — `TARGET_ALL`: цель — nowcast общего ИПЦ, и его же метка M−1 служит
    `y_lag1`; последний месяц компонентных рядов цель не двигает.
    """
    points = targets.get(TARGET_ALL, [])
    if not points:
        return None
    return max(month_of(day) for day, _ in points)


def nowcast_target_month(targets: dict, today: date) -> tuple[Optional[date], str]:
    """Целевой месяц живого nowcast (спека §5) или причина, почему его нет.

    Цель — месяц СЛЕДОМ за последним опубликованным: он и есть последний месяц,
    чей месячный ИПЦ не вышел к дате прогона (на 2026-10-16 при метках до
    2026-09 — октябрь-2026, включая незакрытый текущий).

    «Публикация = наличие метки в `rosstat_ipc_mes`» (решение контроллера):
    данные — источник истины, календарной проверки отбора цели нет. Возможное
    отставание импорта (метка следующего месяца давно должна была выйти, а её
    нет) отбор не отменяет, а смягчает — `import_lag_note` называет его в `note`.

    `today` в отборе не участвует (признаки as-of считает `nowcast_features`):
    параметр оставлен для сигнатуры контракта и будущего календарного слоя.
    """
    latest = latest_published_month(targets)
    if latest is None:
        return None, "в rosstat_ipc_mes нет ни одной метки общего ИПЦ — проверить импорт"
    return _month_shift(latest, 1), ""


# Мягкое предупреждение о возможном отставании импорта: у выбранной цели истёк
# календарный срок публикации (конец месяца цели + 30 дней ≈ конец M+1, когда
# выпуск Росстата за M гарантированно вышел), а метки следующего месяца в данных
# нет. Отбор цели не отменяется — наличие метки в rosstat_ipc_mes и есть
# «опубликовано» (решение контроллера), — предупреждение идёт в `note`.
IMPORT_LAG_DAYS = 30


def import_lag_note(month: date, today: date) -> str:
    """Пометка «возможное отставание импорта» или пустая строка (спека §5).

    Граница — строгая: `month_end(month) + IMPORT_LAG_DAYS < today`. Ровно на
    сроке (равенство) предупреждения нет: выпуск мог выйти в последний день.
    """
    if month_end(month) + IMPORT_LAG_DAYS * ONE_DAY < today:
        return "возможное отставание импорта: календарный срок публикации прошёл"
    return ""


def nowcast_window(weekly: list, month: date, today: date) -> list:
    """Недельное окно живого nowcast (спека §5): последние ≤4 недели после конца M−1.

    Берутся недели, ОПУБЛИКОВАННЫЕ к `today` (правило §2.4: регистрация + 2 дня)
    и зарегистрированные ПОСЛЕ конца `M−1` — та же граница свежести, что в
    бэктесте (`fresh_from` в `month_features`): недели до неё описывают
    предыдущий месяц. Минимум — одна неделя (пустой список означает «окна нет»),
    максимум — `WEEKLY_WINDOW_WEEKS`: признаки §2.3 пп. 3–4 определены на
    четырёхнедельном окне, и растягивать сумму/среднее на большее число недель
    нельзя. Неполное окно — свойство даты прогона, а не ошибка: `note` артефакта
    называет его «частичное окно: N из 4 недель».
    """
    boundary = month_end(previous_month(month))
    weeks = [week for week in eligible_weeks(weekly, today) if week[0] > boundary]
    return weeks[-WEEKLY_WINDOW_WEEKS:]


def nowcast_features(
    month: date,
    index: dict,
    usd: list,
    key_rate: list,
    ofz: list,
    fao: list,
    window: list,
    today: date,
) -> tuple[Optional[dict[str, float | int]], str]:
    """Признаки §2.3 живого nowcast для месяца `month` (спека §5) или причина.

    Отличия от `month_features` (бэктеста) — только те, что диктует живой режим:

    * cutoff — дата прогона `today` (а не 8-е число M+1): признаки снимаются
      as-of даты прогона;
    * недельное окно передано готовым (`nowcast_window`): 1…4 недели вместо
      строгих четырёх; при неполном окне сумма бензина приводится к 4-недельному
      масштабу (см. ниже);
    * `y_lag1` — метка M−1, ЕСТЬ в данных: месячный ИПЦ приходит в базу из
      релиза Росстата, поэтому наличие точки и есть публикация. Консервативное
      `available_from_mes` живому режиму не подходит: оно исключило бы уже
      вышедший лаг до конца M+1 (для цели-октября 2026-10-16 — до 31.10).

    **Масштаб `gasoline_4w` при неполном окне** (решение контроллера): модели
    обучены на сумме ЗА ЧЕТЫРЕ недели, поэтому сырая сумма за N недель
    (~N/4 масштаба) умножается на `4/N`. Средние признаки (`median_wk4`,
    `share_rising_wk`, `median_wk`) масштаба не требуют — они уже приведены к
    одной неделе. Бэктест не трогается: там окно всегда четыре недели.

    Остальное совпадает с бэктестом: USD/ставка/FAO/ОФЗ — as-of правила §2.4 на
    `today`, отсутствие точки `ofz_1y` даёт NaN и месяц не выбрасывает.
    """
    weekly_part = weekly_window_features(window)
    if weekly_part is None:
        return None, "неполное недельное окно (нет бензина в одной из недель)"
    if len(window) < WEEKLY_WINDOW_WEEKS:
        weekly_part["gasoline_4w"] *= WEEKLY_WINDOW_WEEKS / len(window)
    usd_value = usd_features(usd, today)
    if usd_value is None:
        return None, "нет глубины USD (нужна 21 доступная точка)"
    rate = rate_feature(key_rate, today)
    if rate is None:
        return None, "нет доступной ставки на дату прогона"
    fao_value = fao_features(fao, today)
    if fao_value is None:
        return None, "нет пары доступных точек FAO"
    lag = index.get(TARGET_ALL, {}).get(month_end(previous_month(month)))
    if lag is None:
        return None, "нет опубликованной метки месяца M−1 (y_lag1)"
    ofz_value = ofz_feature(ofz, today)
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


def nowcast_row(month: date, features: dict) -> Row:
    """Строка живого nowcast: признаки as-of, метки месяца не существует.

    `y` — плейсхолдер NaN, а не выдуманное значение: таргета месяца `month` к
    дате прогноза нет (в этом и смысл nowcast). Ни одна модель не читает `y`
    прогнозной строки — fit-функции берут таргет только обучающих строк, — и
    плейсхолдер это фиксирует, а не маскирует «нулём, похожим на значение».
    """
    return Row(
        month=month,
        y={code: float("nan") for code in TARGET_SERIES},
        features=features,
    )


def partial_window_note(weeks: int) -> str:
    """Пометка частичного недельного окна живого nowcast (спека §5, рулинг).

    Текст общий для `note` JSON и раздела отчёта: «частичное окно: N из 4
    недель», оговорка, что режим генерации признаков отличается от бэктеста
    (там строго четыре недели), и явное имя масштабирования `gasoline_4w`
    (`nowcast_features` умножает сырую сумму на 4/N — модели обучены на
    четырёхнедельной сумме).
    """
    return (
        f"частичное окно: {weeks} из {WEEKLY_WINDOW_WEEKS} недель — режим генерации "
        f"признаков отличается от бэктеста (там строго {WEEKLY_WINDOW_WEEKS} недели); "
        f"gasoline_4w приведён к {WEEKLY_WINDOW_WEEKS}-недельному масштабу "
        f"(множитель {WEEKLY_WINDOW_WEEKS}/{weeks})"
    )


# Базовая оговорка артефакта: чем живой прогноз отличается от бэктеста, даже
# когда окно полное (частичное окно добавляет `partial_window_note`).
NOWCAST_NOTE = (
    "живой nowcast v0 (спека §5): признаки — as-of даты прогона, а не канонического "
    "cutoff; as-of — приближение, не point-in-time слой (§2.4); модели обучены на "
    "полной выборке месяцев (не фолд walk-forward)"
)


@dataclass(frozen=True)
class Nowcast:
    """Текущий nowcast: цель, окно, прогнозы моделей и бенчмарков.

    `direct`/`bottomup` — `catboost_direct` и `catboost_bottomup` на полной
    выборке, `None` — метод не посчитан (вырожденное окно; причина — в `note`);
    `benchmarks` — прогнозы B1–B3 (`None` там же); `weights` — NNLS-веса
    компонент по полной выборке (`None` — веса не определены), порядок —
    `BOTTOMUP_COMPONENTS`; `window_last` — последняя неделя окна (регистрация),
    `note` — оговорка артефакта (может включать частичное окно и пропуски).
    """

    target_month: date
    data_asof: date
    window_weeks: int
    window_last: date
    direct: Optional[float]
    bottomup: Optional[float]
    weights: Optional[tuple]
    benchmarks: dict
    sample_size: int
    note: str


def current_nowcast(
    rows: list[Row],
    targets: dict,
    weekly: list,
    usd: list,
    key_rate: list,
    ofz: list,
    fao: list,
    today: date,
) -> tuple[Optional[Nowcast], str]:
    """Текущий nowcast (спека §5, Task 5) или `(None, причина)`.

    Цель — `nowcast_target_month`; признаки — `nowcast_features` as-of `today`;
    модели и бенчмарки (`MODELS`) обучаются на ПОЛНОЙ выборке `rows` — это выход
    среза, а не фолд walk-forward. `DegenerateWindowError` метода — пропуск
    ЭТОГО метода (причина в `note`), а не падение: на короткой выборке B2/B3
    вырождены законно. Если не посчитан НИ ОДИН метод, прогноза нет —
    `(None, причина)`: JSON без чисел был бы пустышкой. Пустая выборка и
    отсутствие цели/окна — тоже `(None, причина)`; исключений здесь нет
    намеренно (прогон не падает, сообщение печатает вызывающий).

    `note` собирается из пометок, которые иначе терялись бы на живом пути:
    частичное окно (`partial_window_note`, включая масштаб `gasoline_4w`),
    возможное отставание импорта (`import_lag_note`) и **недосход фита**
    (`fold_notes` модели — у B3 `ConvergenceWarning` координатного спуска;
    «прогноз индикативен»). Диагностика читается ПОСЛЕ вызова fit-функции:
    `fit_enet` заполняет `fold_notes` на каждом фите.

    `rows` — выборка `build_sample` (`month`, метки, признаки); `targets` и ряды
    признаков — то, что читают загрузчики. Структуры, в которых нет ни цели, ни
    признаков, не додумываются: каждое отсутствие названо причиной.
    """
    if not rows:
        return None, "выборка пуста — модели nowcast обучить не на чем"
    month, reason = nowcast_target_month(targets, today)
    if month is None:
        return None, reason
    window = nowcast_window(weekly, month, today)
    if not window:
        boundary = month_end(previous_month(month))
        return None, (
            f"нет недель, опубликованных к {today.isoformat()} после конца M−1 "
            f"({boundary.isoformat()}) — цель {month.isoformat()[:7]}: частичного "
            f"недельного окна нет"
        )
    features, reason = nowcast_features(
        month, target_index(targets), usd, key_rate, ofz, fao, window, today
    )
    if features is None:
        return None, reason
    row = nowcast_row(month, features)
    values: dict[str, float] = {}
    skipped: list[tuple[str, str]] = []
    non_converged: list[str] = []
    for name in (MODEL_CATBOOST_DIRECT, MODEL_CATBOOST_BOTTOMUP, *BENCHMARKS):
        try:
            values[name] = float(MODELS[name](rows, row))
        except DegenerateWindowError as error:
            skipped.append((name, str(error)))
            continue
        fold_notes = getattr(MODELS[name], "fold_notes", None)
        if fold_notes and int(fold_notes.get(NON_CONVERGED, 0)):
            non_converged.append(name)
    if not values:
        return None, "ни один метод не посчитан: " + "; ".join(
            f"{model_label(name)} — {error}" for name, error in skipped
        )
    try:
        weights = nnls_weights(rows)
    except DegenerateWindowError:
        weights = None
    note = NOWCAST_NOTE
    if len(window) < WEEKLY_WINDOW_WEEKS:
        note += "; " + partial_window_note(len(window))
    lag_note = import_lag_note(month, today)
    if lag_note:
        note += "; " + lag_note
    if non_converged:
        note += "; " + "; ".join(
            f"{model_label(name)}: координатный спуск не сошёлся на живой выборке — "
            f"прогноз индикативен"
            for name in non_converged
        )
    if skipped:
        note += "; не посчитаны: " + "; ".join(
            f"{model_label(name)} ({error})" for name, error in skipped
        )
    return (
        Nowcast(
            target_month=month,
            data_asof=today,
            window_weeks=len(window),
            window_last=window[-1][0],
            direct=values.get(MODEL_CATBOOST_DIRECT),
            bottomup=values.get(MODEL_CATBOOST_BOTTOMUP),
            weights=weights,
            benchmarks={name: values.get(name) for name in BENCHMARKS},
            sample_size=len(rows),
            note=note,
        ),
        "",
    )


# ---------------------------------------------------------------------------
# Отчёт и артефакты (Tasks 3–5)
# ---------------------------------------------------------------------------

REPORT_NAME = "cpi_nowcast_report.md"
PREDICTIONS_NAME = "cpi_nowcast_predictions.csv"
FORECAST_NAME = "cpi_nowcast_forecast.json"
PREDICTIONS_COLUMNS = ("window", "model", "month", "y_true", "y_pred", "error")

# Артефакты прогона: отчёт, CSV фолдов и JSON текущего nowcast. Подчистка ветки
# «ни одного прогноза вообще» идёт по этому списку — прошлый прогон мог оставить
# любой из них, а дат в именах нет: файл читался бы как результат ЭТОГО прогона.
ARTIFACT_NAMES = (REPORT_NAME, PREDICTIONS_NAME, FORECAST_NAME)


@dataclass(frozen=True)
class WeightRun:
    """Веса NNLS последнего фолда окна (спека §3.2) — строка блока весов отчёта.

    `month` — тестовый месяц последнего фолда bottom-up этого окна, `train` —
    число месяцев его обучающего окна, `weights` — нормированные веса в порядке
    `BOTTOMUP_COMPONENTS`.
    """

    start: int
    embargo: int
    month: date
    train: int
    weights: tuple


def bottomup_weight_reports(
    sample: list[Row], window_runs: list[WindowRun]
) -> list[WeightRun]:
    """Веса NNLS по окнам прогона — по последнему фолду каждого окна (§3.2).

    Обучающее окно берётся РОВНО то же, что у harness (`walk_forward`): фолд
    месяца `M` обучается на строках выборки до `M` минус `embargo`, поэтому веса
    отчёта — те, что подставлялись в последний прогноз bottom-up окна, а
    тестовый месяц в подгонку не попадает (утечки нет). Окно без прогнозов
    bottom-up (все фолды вырождены) или с вырожденным окном NNLS строку не
    получает: печатать «веса» фолда, которого нет, нельзя.
    """
    by_month = {row.month: index for index, row in enumerate(sample)}
    reports = []
    for run in window_runs:
        months = [
            prediction.month
            for prediction in run.predictions
            if prediction.model == MODEL_CATBOOST_BOTTOMUP
        ]
        if not months:
            continue
        month = max(months)
        index = by_month.get(month)
        if index is None:
            continue
        train = sample[: index - run.embargo]
        try:
            weights = nnls_weights(train)
        except DegenerateWindowError:
            continue
        reports.append(
            WeightRun(
                start=run.start,
                embargo=run.embargo,
                month=month,
                train=len(train),
                weights=weights,
            )
        )
    return reports


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
_WEIGHTS_HEADER = (
    "| окно | последний фолд | обучающих месяцев | w(cpi_food) | w(cpi_nonfood) "
    "| w(cpi_services) | Σw |",
    "|---|---|---|---|---|---|---|",
)


def _catboost_config_line() -> str:
    """Строка отчёта о конфигурации CatBoost (§3.1) — из `catboost_metadata`.

    Отчёт обязан показывать, на чём считались модели (спека §3.1: «дефолты
    фиксируются в метаданных модели»): строка собирается из тех же метаданных,
    что уходят в JSON прогона (Task 5), поэтому не может разъехаться с кодом.
    """
    metadata = catboost_metadata()
    constrained = ", ".join(
        f"`{name}` {value:+d}"
        for name, value in metadata["monotone_constraints"].items()
        if value
    )
    defaults = ", ".join(
        f"`{name}={value}`" for name, value in metadata["defaults"].items()
    )
    return (
        f"Модели `catboost_direct` и `catboost_bottomup` (§3.2). "
        f"`CatBoostRegressor(loss_function={metadata['loss_function']!r}, "
        f"random_seed={metadata['random_seed']}, verbose=False)`; "
        f"`cat_features={metadata['cat_features']}` (вход — numpy-матрица: числовые "
        f"float + int-коды месяца, NaN как есть); "
        f"`monotone_constraints`: {constrained}, остальные 0 (порядок — "
        f"`{', '.join(metadata['num_features'])}, month`); остальные параметры — "
        f"дефолты библиотеки: {defaults} (кодом не переопределяются; полные "
        f"метаданные — `catboost_metadata()`, `{FORECAST_NAME}`)."
    )


def render_report(
    sample: list[Row],
    skipped: list,
    window_runs: list[WindowRun],
    nowcast: Optional[str] = None,
    today: Optional[date] = None,
    weights: Optional[list[WeightRun]] = None,
) -> str:
    """Markdown-отчёт прогона: выборка, walk-forward, метрики, значимость (§4).

    `sample`/`skipped` — выборка `build_sample` (строки и месяцы без собранных
    признаков); `window_runs` — по одному `WindowRun` на окно walk-forward
    (порядок окон задаёт строки таблиц, основное помечено `WindowRun.main`; имя
    отражает и прогнозы, и счётчики окна); `nowcast` — готовый markdown-блок
    «Текущего nowcast» (`render_nowcast`) или `None` — тогда раздел называет,
    что прогноза нет; `today` — дата прогона (по умолчанию системная);
    `weights` — веса NNLS по окнам (`bottomup_weight_reports`) или `None`, если
    они не считались.

    «Модель × окно» — строки таблиц: перечень моделей берётся из счётчиков окон
    (модель, вырожденная на всех окнах, видна строкой «0 фолдов», а не
    пропадает), метрики — из фолдов, «недосход» — из `WindowRun.notes`. Модели
    CatBoost дают те же `Prediction`, поэтому попадают в таблицы и в пары DM
    (`pairwise_tests`) без правок отчёта.
    """
    today = run_date(today)
    runs = list(window_runs)
    main_run = next((run for run in runs if run.main), runs[0] if runs else None)
    lines = [
        "# CPI nowcast — walk-forward бэктест (бенчмарки B1–B3 и CatBoost)",
        "",
        f"Дата прогона: {today.isoformat()}; сид прогона `SEED={SEED}`. Спека: "
        "`docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md` (§2 — данные и "
        "признаки, §3.1–3.2 — модели, §3.3 — бенчмарки, §4 — "
        "walk-forward/метрики/тесты); план: "
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
        "теста в предобработку не попадает. Модели CatBoost получают NaN как есть "
        "(`nan_mode` по умолчанию — `Min`): пропуск не импутируется.",
        "",
        "## Конфигурация CatBoost (§3.1)",
        "",
        _catboost_config_line(),
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
        "подгонки; у моделей без такой диагностики (B1, B2, CatBoost) — «—».",
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
        "метрикой. RMSE/MAE — общие функции `scripts/lib/eval_utils.py`.",
        "",
    ]
    lines += _table(_METRIC_HEADER, metric_rows)
    lines += [""]
    weight_rows = [
        f"| {window_key(item.start, item.embargo)} | {item.month.isoformat()[:7]} "
        f"| {item.train} | "
        + " | ".join(_fmt(value, 4) for value in item.weights)
        + f" | {_fmt(sum(item.weights), 4)} |"
        for item in (weights or [])
    ]
    lines += [
        "## Веса bottom-up (NNLS)",
        "",
        "Веса `w` — неотрицательная регрессия (`scipy.optimize.nnls`) общего ИПЦ на "
        "три компоненты ПО ОБУЧАЮЩЕМУ ОКНУ фолда (спека §3.2): тестовый месяц и "
        "будущие месяцы в подгонку не попадают, решение нормировано к сумме 1. "
        "Таблица — по последнему фолду каждого окна: месяц, размер обучающего окна "
        "и веса компонент (`cpi_food`, `cpi_nonfood`, `cpi_services`); ожидание "
        "спеки §3.2 — ≈ [0.36–0.40, 0.35–0.40, 0.25–0.30]. Окна с одинаковым "
        "последним фолдом (один и тот же обучающий отрезок) дают одинаковые веса — "
        "они считаются по обучающему окну, а не по старту окна прогона.",
        "",
    ]
    lines += _table(
        _WEIGHTS_HEADER, weight_rows, empty="_(веса NNLS не считались.)_"
    )
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
            "seed=SEED). Пары — на общих месяцах: `catboost_direct` и "
            "`catboost_bottomup` против каждого бенчмарка и `catboost_bottomup` "
            "против `catboost_direct`.",
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
        "бэктесте нулевой (импутация константой); CatBoost получает NaN как есть — "
        "сравнение политик NaN моделей не проверено. В живом nowcast точка ОФЗ уже "
        "есть, а у B3 z-оценка признака не ограничена (колонка обучения нулевой "
        "дисперсии масштаб не задаёт): на 2026-10-16 коэффициент при `ofz_1y` "
        "выбран нулевым (L1) и вклада нет, но гарантии спека не даёт.",
        "- CatBoost: гиперпараметры не тюнинговались (дефолты §3.1 — в метаданных "
        "модели `catboost_metadata`, они же в `" + FORECAST_NAME + "`); веса NNLS "
        "показаны по последнему фолду каждого окна (динамика весов по фолдам не "
        "анализируется); ошибки компонентных моделей bottom-up отдельно не "
        "измеряются.",
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
        # `nowcast=None` — вызывающий не передал блок: тот же единственный текст
        # отсутствия прогноза с дефолтной причиной (`render_nowcast_absent`).
        lines += [render_nowcast_absent(), ""]
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


# --- JSON текущего nowcast (спека §5) -----------------------------------------


def render_nowcast(nowcast: Nowcast) -> str:
    """Markdown-блок «Текущий nowcast» (спека §5) — раздел отчёта.

    Числа округлены до разрядов таблицы (`_fmt`): полная точность — в JSON
    (`nowcast_json`). `note` печатается тем же текстом, что уходит в JSON, —
    пометка частичного окна не может разъехаться между артефактами.
    """
    lines = [
        f"**Целевой месяц: {nowcast.target_month.isoformat()[:7]}** — месячный ИПЦ "
        f"не опубликован на {nowcast.data_asof.isoformat()}; признаки собраны "
        f"as-of даты прогона (не канонического cutoff, спека §5).",
        "",
        "| метод | прогноз, п.п. |",
        "|---|---|",
        f"| `{MODEL_CATBOOST_DIRECT}` | {_fmt(nowcast.direct)} |",
        f"| `{MODEL_CATBOOST_BOTTOMUP}` | {_fmt(nowcast.bottomup)} |",
    ]
    lines += [
        f"| {model_label(name)} | {_fmt(value)} |"
        for name, value in nowcast.benchmarks.items()
    ]
    lines += [
        "",
        f"Недельное окно: {nowcast.window_weeks} из {WEEKLY_WINDOW_WEEKS} недель "
        f"(последняя публикация — {nowcast.window_last.isoformat()}).",
        "",
    ]
    if nowcast.weights is None:
        lines.append("Веса bottom-up (NNLS) не посчитаны.")
    else:
        cells = ", ".join(
            f"`{code}` {_fmt(weight, 4)}"
            for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        )
        lines.append(
            f"Веса bottom-up (NNLS, полная выборка): {cells} "
            f"(Σ {_fmt(sum(nowcast.weights), 4)})."
        )
    lines += ["", f"Примечание: {nowcast.note}"]
    return "\n".join(lines)


# Причина раздела «Текущий nowcast», когда вызывающий не передал её
# (`render_report(nowcast=None)`): живой прогон (`run_summary`) всегда передаёт
# блок — `render_nowcast` на успехе или `render_nowcast_absent(reason)` с
# причиной `current_nowcast`.
NOWCAST_REASON_UNKNOWN = "причина не передана вызывающим (см. консоль прогона)"


def render_nowcast_absent(reason: str = NOWCAST_REASON_UNKNOWN) -> str:
    """Раздел отчёта, когда живого прогноза нет (спека §5) — ОДИН текст на оба входа.

    Ветка «нет цели» бэктест не отменяет: отчёт и CSV пишутся как обычно
    (walk-forward валиден и без живого прогноза), поэтому причину отсутствия
    прогноза называет сам раздел — тем же `reason`, что печатает консоль
    (`current_nowcast`), — а stale-прогноз прошлого прогона удаляется
    (`write_nowcast_artifact`), чтобы не читался как актуальный.

    Вызывается и из `run_summary` (с реальной причиной), и из `render_report`
    при `nowcast=None` (дефолтная причина): формулировка не может разъехаться.
    """
    return (
        f"Текущий nowcast не рассчитан: {reason}. `{FORECAST_NAME}` не пишется; "
        f"устаревший файл прогноза, если был, удалён. Отчёт и CSV посчитаны "
        f"текущим прогоном: walk-forward валиден и без живого прогноза."
    )


def forecast_payload(nowcast: Nowcast) -> dict:
    """Тело JSON текущего nowcast (спека §5) — ключи в порядке спеки.

    `forecast_date` и `data_asof` в v0 совпадают (признаки снимаются as-of даты
    прогона); поля разделены под будущий PIT-слой. Метаданные CatBoost (§3.1:
    «дефолты фиксируются в метаданных модели») едут с каждым прогнозом модели —
    тем же словарём, что печатает отчёт (`catboost_metadata`). Wall-clock-полей
    сверх даты нет: JSON детерминирован при (данные, дата прогона).
    """
    metadata = catboost_metadata()
    weights = None
    if nowcast.weights is not None:
        weights = {
            code: weight for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        }
    return {
        "forecast_date": nowcast.data_asof.isoformat(),
        "target_month": nowcast.target_month.isoformat(),
        "data_asof": nowcast.data_asof.isoformat(),
        "catboost_direct": {"forecast": nowcast.direct, "metadata": metadata},
        "catboost_bottomup": {"forecast": nowcast.bottomup, "metadata": metadata},
        "bottomup_weights": weights,
        "benchmarks": dict(nowcast.benchmarks),
        "sample_size": nowcast.sample_size,
        "note": nowcast.note,
    }


def nowcast_json(nowcast: Nowcast) -> str:
    """JSON текущего nowcast: UTF-8 как есть, отступ 2, LF в конце (спека §5)."""
    return json.dumps(forecast_payload(nowcast), ensure_ascii=False, indent=2) + "\n"


def write_nowcast_artifact(path: Path, nowcast: Optional[Nowcast]) -> bool:
    """Пишет JSON nowcast или УБИРАЕТ устаревший артефакт; `False` — писать нечего.

    Артефакт — снимок текущего nowcast на дату прогона. Ветка «nowcast не
    посчитан» (нет неопубликованного месяца, нет недельного окна, ни один метод
    не посчитан) обязана не молчать, а удалить файл прошлого прогона:
    оставленный, он был бы прочитан как текущий прогноз — тот же класс дефекта,
    что закрыт в `key_rate_model.py` (`write_forecast_artifact`). Возврат: `True`
    — файл записан, `False` — файла нет (в том числе удалён только что).
    """
    if nowcast is None:
        path.unlink(missing_ok=True)
        return False
    path.write_text(nowcast_json(nowcast), encoding="utf-8")
    return True


def clear_stale_artifacts(out_dir: Path) -> list:
    """Убирает ВСЕ артефакты прошлого прогона; возвращает имена удалённых файлов.

    Только для ветки «ни одного прогноза вообще» (пустая выборка, выборка короче
    старта walk-forward или все модели вырождены — считать нечего): она не пишет
    НИЧЕГО, но файлы предыдущего прогона лежат в каталоге и читались бы как
    результат ЭТОГО (дат в именах нет, а `cpi_nowcast_forecast.json` ещё и
    выглядит как «текущий прогноз»). Ветка «нет цели живого nowcast'а» сюда НЕ
    ходит: там бэктест валиден, отчёт и CSV перезаписываются, а подчищается
    только stale-JSON (`write_nowcast_artifact`). Отсутствие файлов — не ошибка.
    Возврат — имена для строки консоли (молчание о подчистке неотличимо от её
    отсутствия).
    """
    removed = []
    for name in ARTIFACT_NAMES:
        path = out_dir / name
        if path.exists():
            path.unlink()
            removed.append(name)
    return removed


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
# Прогон: сводка выборки, walk-forward моделей и бенчмарков, артефакты
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


def _print_weights(weights: list[WeightRun]) -> None:
    """Консольный блок весов NNLS: последний фолд каждого окна (спека §3.2)."""
    print("bottom-up NNLS-веса (последний фолд окна, нормированы к 1):")
    if not weights:
        print("  нет строк (bottom-up не прогнозировал или окно вырождено)")
        return
    for item in weights:
        cells = ", ".join(
            f"{name} {_fmt(value, 3)}"
            for name, value in zip(BOTTOMUP_COMPONENTS, item.weights)
        )
        print(
            f"  {window_key(item.start, item.embargo)}: "
            f"{item.month.isoformat()[:7]}, обучающих {item.train} — {cells}, "
            f"Σ {_fmt(sum(item.weights), 3)}"
        )


def _print_nowcast(nowcast: Nowcast) -> None:
    """Консольный блок текущего nowcast: модели, бенчмарки, веса (спека §5).

    Числа — из тех же полей, что уходят в JSON (полная точность — в файле,
    здесь три разряда для чтения глазами).
    """
    for name, value in (
        (MODEL_CATBOOST_DIRECT, nowcast.direct),
        (MODEL_CATBOOST_BOTTOMUP, nowcast.bottomup),
        *nowcast.benchmarks.items(),
    ):
        print(f"    {model_label(name):<34} {_fmt(value, 4)} п.п.")
    if nowcast.weights is not None:
        cells = ", ".join(
            f"{code} {_fmt(weight, 4)}"
            for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        )
        print(
            f"    {'веса NNLS (полная выборка)':<34} {cells}, "
            f"Σ {_fmt(sum(nowcast.weights), 3)}"
        )


def _print_removed_artifacts(removed: list) -> None:
    """Строка консоли об удалённых артефактах прошлого прогона (пусто — нет строки)."""
    if removed:
        print(f"  устаревшие артефакты прошлого прогона удалены: {', '.join(removed)}")


def run_summary(
    start: int = MAIN_START,
    embargo_months: int = MAIN_EMBARGO,
    out_dir: Optional[Path] = None,
    today: Optional[date] = None,
    ml_log: Optional[bool] = None,
    logger: Optional[object] = None,
    run_name: Optional[str] = None,
) -> int:
    """Полный прогон: выборка, walk-forward, текущий nowcast и артефакты (§4–§5).

    `start`/`embargo_months` — основное окно walk-forward (`--start`,
    `--embargo-months`); робастные окна (второй старт и эмбарго 1) добавляет
    `window_specs`. Загрузка рядов, `build_sample` и `assert_months_purged` —
    контракт Task 2; walk-forward считает бенчмарки B1–B3 и модели CatBoost
    (`MODELS`) на одних фолдах, веса NNLS последнего фолда каждого окна идут в
    отчёт (`bottomup_weight_reports`) и в консоль.

    Текущий nowcast (`current_nowcast`, §5) считается на ПОЛНОЙ выборке:
    признаки — as-of `today`, цель — последний неопубликованный месяц. Отсутствие
    живого прогноза (нет цели, нет недельного окна, ни один метод не посчитан)
    бэктест НЕ отменяет: отчёт и CSV пишутся как обычно (walk-forward валиден и
    без живого прогноза), раздел «Текущий nowcast» называет причину
    (`render_nowcast_absent`), а stale-`cpi_nowcast_forecast.json` удаляется
    (`write_nowcast_artifact`) — прогноз прошлого прогона не должен читаться как
    текущий. Отдельная ветка «ни одного прогноза вообще» (пустая выборка или
    выборка короче старта walk-forward — считать нечего) не пишет НИЧЕГО и
    подчищает все три артефакта прошлого прогона (`clear_stale_artifacts`,
    образец Task 6 ветки v0). Прогон не падает ни в одной из веток.

    `out_dir` — каталог артефактов: корень worktree в живом прогоне (значение
    `ARTIFACTS_DIR` берётся в момент вызова, чтобы тесты могли его подменить),
    временный каталог — в тестах. `today` — дата прогона (по умолчанию
    системная): она же дата среза признаков и `forecast_date` JSON.

    `ml_log`/`logger`/`run_name` — журнал эксперимента (`scripts/lib/ml_experiment.py`):
    при `ml_log=True` (или `ML_LOG=1`, если параметр не задан) создаётся прогон
    `cpi_nowcast` с паспортом, fold-предсказаниями в п.п. и артефактами; явный
    `logger` используется как есть (duck typing). Без журнала скрипт в ClickHouse
    не пишет — только читает и кладёт `cpi_nowcast_report.md`,
    `cpi_nowcast_predictions.csv` и `cpi_nowcast_forecast.json`.
    """
    out_dir = Path(out_dir) if out_dir is not None else ARTIFACTS_DIR
    today = run_date(today)
    targets = load_targets()
    weekly = load_weekly()
    usd = load_usd()
    key_rate = load_key_rate()
    ofz = load_ofz_1y()
    fao = load_fao()
    rows, skipped = build_sample(targets, weekly, usd, key_rate, ofz, fao)
    assert_months_purged(rows)

    if ml_log is None:
        ml_log = os.environ.get("ML_LOG") == "1"
    months = sample_months(targets)
    sample_meta = {
        "rows": len(rows),
        "first": rows[0].month.isoformat() if rows else None,
        "last": rows[-1].month.isoformat() if rows else None,
        "skipped": len(skipped),
        "months": len(months),
    }
    if logger is None and ml_log:
        logger = mlx.start_run(
            experiment="cpi_nowcast",
            run_name=(
                os.environ.get("ML_RUN_NAME")
                or run_name
                or f"cpi_nowcast__start{start}__embargo{embargo_months}"
            ),
            config={
                "start": start,
                "embargo_months": embargo_months,
                "seed": SEED,
                "models": list(MODELS),
                "features": feature_names(),
                "weekly_window_weeks": WEEKLY_WINDOW_WEEKS,
                "target_series": TARGET_SERIES,
            },
            data_asof=today,
            sample_meta=sample_meta,
            sample=[
                {
                    "month": row.month.isoformat(),
                    "y": mlx.json_safe(row.y),
                    "features": mlx.json_safe(row.features),
                }
                for row in rows
            ],
            spec_path="docs/superpowers/specs/2026-10-16-cpi-nowcast-design.md",
            enabled=True,
        )

    try:
        positions = len({name for name, _, _ in weekly})
        print("CPI nowcast — walk-forward: бенчмарки B1–B3 и CatBoost (direct, bottom-up)")
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
        _field("модели:", ", ".join(model_label(name) for name in MODELS))
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

        nowcast, reason = current_nowcast(
            rows, targets, weekly, usd, key_rate, ofz, fao, today
        )
        if nowcast is None:
            _field("текущий nowcast:", f"не рассчитан — {reason}")
        else:
            _field("текущий nowcast:", (
                f"{nowcast.target_month.isoformat()[:7]} — as-of "
                f"{nowcast.data_asof.isoformat()}, недель {nowcast.window_weeks} "
                f"из {WEEKLY_WINDOW_WEEKS}"
            ))
            _print_nowcast(nowcast)

        # Модель полной выборки — тот фит, что сделал `current_nowcast`; читаем
        # его сразу, до walk-forward: `evaluate_windows` перезапишет `last_fit`
        # моделью последнего фолда, а это уже не full-sample fit. `nowcast.direct`
        # — провенанс: если direct-модель в этом прогоне вырождена и пропущена,
        # `last_fit` может хранить фит прошлого вызова, логировать его нельзя.
        if (
            logger is not None
            and getattr(logger, "log_model", None)
            and nowcast is not None
            and nowcast.direct is not None
        ):
            fit = fit_catboost_direct.last_fit
            if fit is not None:
                try:
                    logger.log_model("model", fit.model)
                except Exception as exc:
                    log.warning("модель не залогирована: %s: %s", type(exc).__name__, exc)

        runs = evaluate_windows(rows, MODELS, window_specs(start, embargo_months))
        _print_walk_forward(runs)
        weights = bottomup_weight_reports(rows, runs)
        _print_weights(weights)
        if not any(run.predictions for run in runs):
            # Ветка «ничего не посчитано вообще»: пустая выборка, выборка короче
            # старта walk-forward или все модели вырождены — у ЭТОГО прогона нет ни
            # одного прогноза, поэтому не пишется ничего, а все три артефакта
            # прошлого прогона удаляются, чтобы не читались как его результат
            # (образец — Task 6 ветки v0).
            _field("отчёт/CSV/JSON:", "не пишутся (walk-forward не дал ни одного прогноза)")
            _print_removed_artifacts(clear_stale_artifacts(out_dir))
            if logger is not None:
                # Журнал не меняет код возврата: сбой `finish` здесь — только
                # предупреждение (сам прогон уже отработал ветку «empty»).
                try:
                    logger.finish("empty", "walk-forward не дал ни одного прогноза")
                except Exception as finish_exc:
                    log.warning(
                        "не удалось пометить прогон empty: %s: %s",
                        type(finish_exc).__name__, finish_exc,
                    )
            return 0
        # Ветка «нет цели живого nowcast'а» бэктест НЕ отменяет: отчёт и CSV пишутся
        # как обычно, а подчищается только stale-прогноз (`write_nowcast_artifact`).
        report = render_report(
            rows,
            skipped,
            runs,
            nowcast=(
                render_nowcast(nowcast) if nowcast is not None
                else render_nowcast_absent(reason)
            ),
            today=today,
            weights=weights,
        )
        report_path, predictions_path = write_artifacts(out_dir, report, runs)
        forecast_path = out_dir / FORECAST_NAME
        forecast_written = write_nowcast_artifact(forecast_path, nowcast)
        _field("отчёт:", str(report_path))
        _field("прогнозы:", str(predictions_path))
        if forecast_written:
            _field("прогноз (JSON):", str(forecast_path))
        else:
            _field(
                "прогноз (JSON):",
                f"не пишется ({reason}); устаревший файл, если был, удалён",
            )
        if logger is not None:
            # Журнал эксперимента — побочный слой: его сбой НЕ меняет код
            # возврата. Отчёт и CSV к этому моменту уже записаны, поэтому
            # ValueError `PredictionRow` (например, на NaN-прогнозе) не должен
            # превращать успешный прогон в non-zero exit: помечаем run failed
            # и идём дальше.
            try:
                logger.log_metrics(
                    {
                        "primary_metric": "rmse_pp",
                        "sample": sample_meta,
                        # Спека §3.1: общие ключи паспорта — `models` и
                        # `windows`; список моделей — те же имена, что у
                        # fold-предсказаний и в CSV.
                        "models": list(MODELS),
                        "windows": [
                            {
                                "window": window_key(run.start, run.embargo),
                                "folds": run.folds,
                                "models": {
                                    name: {
                                        "rmse": rmse_pp(group) if group else None,
                                        "mae": mae_pp(group) if group else None,
                                        "n": len(group),
                                        "skipped": int(run.skipped.get(name, 0)),
                                        "notes": dict(run.notes.get(name, {})),
                                    }
                                    for name, group in _run_groups(run).items()
                                },
                            }
                            for run in runs
                        ],
                        # Спека §7.1: у каждой строки весов есть метка окна —
                        # без неё строки разных окон с тем же месяцем
                        # неразличимы.
                        "weights": [
                            {
                                "window": window_key(item.start, item.embargo),
                                "month": item.month.isoformat(),
                                "train": item.train,
                                "weights": dict(zip(BOTTOMUP_COMPONENTS, item.weights)),
                            }
                            for item in weights
                        ],
                    }
                )
                logger.log_predictions(
                    [
                        mlx.PredictionRow(
                            model=prediction.model,
                            window=window_key(run.start, run.embargo),
                            target_date=prediction.month,
                            predicted=prediction.y_pred,
                            actual=prediction.y_true,
                            unit="pp",
                            meta={
                                "start": run.start,
                                "embargo": run.embargo,
                                "main": run.main,
                            },
                        )
                        for run in runs
                        for prediction in run.predictions
                    ]
                )
                logger.log_artifacts(
                    [report_path, predictions_path, forecast_path if forecast_written else None]
                )
                logger.finish(
                    "finished",
                    f"walk-forward: {len(runs)} окон, "
                    f"{sum(len(run.predictions) for run in runs)} фолдов; "
                    f"текущий nowcast: {'рассчитан' if nowcast is not None else 'не рассчитан'}",
                )
            except Exception as exc:
                log.warning(
                    "журнал эксперимента не записан: %s: %s",
                    type(exc).__name__, exc,
                )
                try:
                    logger.finish(
                        "failed", f"logging: {type(exc).__name__}: {exc}"
                    )
                except Exception as finish_exc:
                    log.warning(
                        "не удалось пометить прогон failed: %s: %s",
                        type(finish_exc).__name__, finish_exc,
                    )
        return 0
    except Exception as exc:
        if logger is not None:
            # Журнал — побочный слой и здесь: сбой `finish` не должен заменять
            # исходное исключение эксперимента собственным (вызывающий увидел
            # бы ошибку логирования вместо настоящей причины падения).
            try:
                logger.finish("failed", f"{type(exc).__name__}: {exc}")
            except Exception as finish_exc:
                log.warning(
                    "не удалось пометить прогон failed: %s: %s",
                    type(finish_exc).__name__, finish_exc,
                )
        raise


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(
        description="CPI nowcast: выборка, as-of, walk-forward моделей и "
                    "бенчмарков (B1–B3, CatBoost direct/bottom-up), веса NNLS, отчёт."
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
    # ML_LOG/ML_RUN_NAME читаются внутри run_summary (`ml_log=None`): явный
    # параметр здесь не нужен, и без переменной поведение прогона прежнее.
    return run_summary(start=args.start, embargo_months=args.embargo_months)


if __name__ == "__main__":
    sys.exit(main())
