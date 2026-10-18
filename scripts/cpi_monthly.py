#!/usr/bin/env python3
"""CPI monthly (ML-трек v2): данные, as-of, выборка, walk-forward и бенчмарки.

Срез месячного nowcast ИПЦ РФ (спека
`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md`, §2–§5.1): выборка
**155 месяцев 2013-10…2026-08** и два информационных множества на ОДНИХ И ТЕХ
ЖЕ строках — «без МП» (5 числовых + `month`) и «с МП» (плюс 7 признаков
мониторинга предприятий ЦБ из `cbr_survey`). Task 2 добавляет harness
walk-forward, обязательные бенчмарки B1–B3 (B3 — в двух пространствах),
метрики RMSE/MAE и отчёт-скелет; Task 3 — четыре варианта CatBoost
(direct/bottom-up × без МП/с МП) и NNLS-веса bottom-up; Task 4 — DM-HLN
«с МП vs без МП», фиксированный сплит статьи, строку «2022 исключён»,
живой nowcast и JSON-артефакт.

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
* **выборка** (`build_sample`) — кандидаты только внутри `SAMPLE_BOUNDS[sample]`
  (`--sample short|long`, default short). `short` — 155 месяцев 2013-10…2026-08:
  старт задаёт ключевая ставка — 2013-09-17, конец — последний месяц с МП.
  `long` (§8.2) — 292 месяца 2002-05…2026-08, пропусков 0: старт 2002-05
  задан глубиной живого МП (месячные с 2002-01, но `_chg` месяца 2002-02
  требует 2001-12; квартальные Q1-2002 доступны только 2002-05-31), а
  `usd_mm`/`key_rate` до глубины своих рядов получают NaN, а не пропуск.
  Строки ОБЩИЕ для обоих наборов: месяц без любого обязательного признака
  (включая МП) попадает в «пропущенные» с причиной и отсутствует в списке
  целиком — иначе «с МП» и «без МП» сравнивались бы на разных фолдах.
  `feature_names(with_mp=False)` выбирает базовые признаки,
  `feature_names(with_mp=True)` — базовые + 7 МП;
* **walk-forward** (`walk_forward`, §4.1) — расширяющееся окно: тест — строка
  `i`, обучение — `[0 … i−1−embargo]` (только прошлое). Первый тест — строка
  `start` (при 24 — 25-й месяц; long по умолчанию стартует с 48, §8.2).
  `DegenerateWindowError` модели — пропуск фолда ЭТОЙ модели со счётчиком, а не
  падение прогона; `fold_notes` модели агрегируется по окну («недосход N из M»).
  Окна прогона (`window_specs`): основное (`--start`, `--embargo-months`) плюс
  робастность — старт 48 и эмбарго 1;
* **бенчмарки** (§3.4, фиксируются до результатов) — B1 `predict_last`
  (`y_lag1`), B2 `fit_ar1` (OLS `y_M` на `y_{M−1}` + 11 дамми месяца, HAC
  `maxlags=3`, минимум 14 строк) и B3 `fit_enet_base`/`fit_enet_mp`
  (`ElasticNetCV` на стандартизованных признаках без `month`, импутация NaN
  медианой train, минимум 12 строк; `ConvergenceWarning` — в `fold_notes`, не
  вырождение). Два пространства B3 отделяют эффект МП от эффекта алгоритма;
* **CatBoost, четыре варианта** (§3.1–3.3) — `catboost_direct` (CatBoost на
  таргет `cpi_all`), `catboost_bottomup` (три компонентных CatBoost и агрегат
  `Σ w_j·ŷ_j` с весами NNLS по обучающему окну фолда, нормированными к 1),
  плюс `_mp`-версии на расширенном наборе признаков. Конфигурация §3.1 —
  `loss_function='RMSE'`, `random_seed=SEED`, `verbose=False`,
  `cat_features=['month']`, monotone-ограничения (`+1` только `usd_mm`,
  `price_expectations_sa`, `price_expectations_sa_chg`),
  `allow_writing_files=False`; остальное — дефолты библиотеки
  (`catboost_metadata`). Пустое окно
  или неизменный таргет ЛЮБОЙ компоненты/весов — `DegenerateWindowError`
  (фолд пропускается), как у B2/B3. Веса и компонентные прогнозы последнего
  успешного фолда — в `last_weights`/`last_components`; отчёт печатает веса
  каждого окна и блок конфигурации;
* **DM-HLN и сплит статьи** (§4.3–4.5) — `pairwise_tests` на ОБЩИХ тестовых
  месяцах (DM с поправкой HLN, `h=1`; `diff = RMSE(A) − RMSE(B)`, отрицательное
  — A точнее): primary-пары — каждый `_mp`-вариант против своей базовой модели
  (direct, bottom-up, B3), secondary — `_mp`-модели против B1 и `b3_elastic_net_mp`
  плюс direct против bottom-up. Блочный бутстрап разницы RMSE — `block=3`,
  `n=2000`, `seed=SEED` — для primary-пар. `fixed_split` — вспомогательная
  таблица сопоставимости со статьёй (train ≤ 2018-12, test 2019-01…2024-07,
  без walk-forward), `metrics_without_2022` — отдельная строка «2022 исключён»;
* **живой nowcast** (§5.3) — `current_nowcast`: цель — месяц за последним
  опубликованным общим ИПЦ, признаки as-of даты прогона (а не `cutoff_for`),
  МП — последняя доступная по правилу §2.6 (может быть глубже `M−1`, пометка в
  `note`), модели и NNLS — на полной выборке. Артефакт
  `cpi_monthly_forecast.json` (§5.1): `forecast_date`, `target_month`,
  `data_asof`, 4 CatBoost-модели с `metadata`, `benchmarks`, `bottomup_weights`,
  `sample_size`, `note`; stale-cleanup как в v1;
* **метрики и отчёт** (§4.2, §5.1) — RMSE/MAE в п.п. через общие функции
  `scripts/eval_utils.py`, блок «последние 12 фолдов»; отчёт
  `cpi_monthly_report.md` (выборка, «модель × окно», последние 12 фолдов,
  DM-HLN primary/secondary, блочный бутстрап, веса NNLS, конфигурация
  CatBoost, сплит статьи, «2022 исключён», живой nowcast, «что не проверено»)
  и CSV `cpi_monthly_predictions.csv` (`month, window, model, y_true, y_pred`).

**NaN-политика:** обязательны `y_lag1`, `y_lag2`, `fao_mm` и признаки МП —
отсутствие любой такой точки выбрасывает месяц с причиной. Контрольные
`usd_mm`/`key_rate` в long-sample до глубины рядов — NaN (§8.2), а не причина
пропуска: B3 импутирует NaN медианой ОБУЧАЮЩЕГО окна (колонка без наблюдений —
0.0: после стандартизации вклада не даёт), `StandardScaler` подгоняется только
на train; CatBoost получает NaN как есть. В short-выборке NaN в этих двух
колонках не возникает — число NaN печатается в консоли и отчёте (0/0).

**As-of — приближение, а не point-in-time слой** (спека §2.6): точные даты
публикаций и ревизии рядов не учтены; PIT-слой — отдельный пункт фазы 4.

Фикстуры офлайн-тестов (`scripts/testdata/cpi_monthly/*.csv`) пересобираются из
живого dev-ClickHouse (пользователь default) той же целью, что и прогон:

    make cpi-monthly ARGS="--export-fixtures scripts/testdata/cpi_monthly"

**По умолчанию скрипт только читает ClickHouse**; с `ML_LOG=1` он пишет паспорт
прогона и fold-предсказания в `ml_runs`/`ml_predictions` через
`scripts/ml_experiment.py` (спека §8.3), а при недоступном ClickHouse хелпер
падает обратно в локальный `build/ml-runs` (`ML_LOCAL_DIR`). Без переменной
артефакты побайтно прежние. Транспорт (`ch_query`) переиспользуется из
`scripts/extrapolation_aisc.py`, а не копируется. Секреты приходят только из
окружения (их подставляет Makefile). Запуск — через `make`:

* `make cpi-monthly` — сводка выборки (155 месяцев 2013-10…2026-08, пропусков
  0), walk-forward B1–B3 и четырёх CatBoost-вариантов по окнам
  24/48/embargo 1, RMSE/MAE, веса NNLS и артефакты `cpi_monthly_report.md`
  и `cpi_monthly_predictions.csv` в корне worktree;
* `make cpi-monthly ARGS="--sample long"` — long-sample §8.2 (292 месяца
  2002-05…2026-08, 0 пропусков, старт 48, NaN-контроли до 2013) — отдельный
  эксперимент на репрезентативность; short-числа этим прогоном не меняются;
* `make cpi-monthly ARGS="--start N --embargo-months K"` — основное окно
  walk-forward задаётся явно (робастность остаётся от спеки §4.1; `--start`
  переопределяет sample-дефолт 24/48);
* `make cpi-monthly ARGS="--quick"` — быстрый экспериментальный режим: только
  основное окно walk-forward, без robustness-окон и фиксированного сплита
  статьи; отчёт и `ML_LOG` помечены `quick`, числа не официальные (спека
  `2026-10-18-ml-quick-runs`);
* `make cpi-monthly ARGS="--quick --jobs 8"` — тот же quick с пулом процессов
  по фолдам (`ProcessPoolExecutor`, CatBoost `thread_count=1`); `--jobs 0/1` —
  последовательно, `N>1` без `--quick` отвергается, полный прогон не
  параллелится;
* `make cpi-monthly ARGS="--export-fixtures DIR"` — пересборка CSV-фикстур.
"""

import argparse
import contextlib
import csv
import json
import logging
import math
import multiprocessing
import os
import statistics
import sys
import warnings
from concurrent.futures import ProcessPoolExecutor
from contextvars import ContextVar
from dataclasses import dataclass, field
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

ROOT = Path(__file__).resolve().parent.parent

log = logging.getLogger(__name__)

# Скрипт-сосед: переиспользуем транспорт к ClickHouse, а не копируем его.
sys.path.insert(0, str(ROOT / "scripts"))

import eval_utils as eu
import extrapolation_aisc as aisc
import ml_experiment as mlx

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

# Границы выборок (спека §2.3 и §8.2). `short` — принятая выборка 155 месяцев:
# старт — max(старты рядов)+1 месяц (позже всех начинается ключевая ставка,
# 2013-09-17), конец — последний месяц с МП (2026-08). `long` — эксперимент на
# репрезентативность, 292 месяца 2002-05…2026-08, пропусков 0: старт 2002-05
# задан глубиной живого МП — месячные ряды начинаются 2002-01 (значит, `_chg`
# месяца 2002-02 требует 2001-12), а квартальные Q1-2002 доступны только
# 2002-05-31 (cutoff 2002-06-08); после старта обязательны `y_lag1`/`y_lag2`,
# FAO и МП, а `usd_mm`/`key_rate` до глубины своих рядов — NaN, а не причина
# пропуска месяца (§8.2). Граница ОБЩАЯ для наборов «с МП» и «без МП»: иначе
# они сравнивались бы на разных фолдах.
SAMPLE_BOUNDS = {
    "short": (date(2013, 10, 1), date(2026, 8, 1)),
    "long": (date(2002, 5, 1), date(2026, 8, 1)),
}
# Имена выборок — порядок для справки CLI; значения границ пинуются тестами.
SAMPLE_NAMES = tuple(SAMPLE_BOUNDS)
DEFAULT_SAMPLE = "short"
# Совместимость: прежние константы — границы short-выборки (их пинуют тесты
# Tasks 1–4 и на них опираются офлайн-фикстуры).
SAMPLE_FIRST, SAMPLE_LAST = SAMPLE_BOUNDS["short"]

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

# Старт основного окна long-sample (спека §8.2): 48 месяцев — первый тест
# 2006-05 (long начинается 2002-05), а окно статьи 2009–2018 целиком лежит в
# walk-forward-оценке (тестовые месяцы после старта), не в одном train-окне.
# Для short дефолт остаётся 24 (§4.1); явный `--start` переопределяет оба.
DEFAULT_LONG_START = 48


def validate_sample(sample: str) -> str:
    """Имя выборки `short`/`long` или ValueError (границы — `SAMPLE_BOUNDS`)."""
    if sample not in SAMPLE_BOUNDS:
        raise ValueError(
            f"неизвестная выборка {sample!r}: ожидается одна из "
            f"{', '.join(SAMPLE_NAMES)}"
        )
    return sample


def resolve_start(sample: str = DEFAULT_SAMPLE, start: Optional[int] = None) -> int:
    """Старт основного walk-forward: явный `start` или дефолт выборки.

    `None` — не «ноль», а «не задан»: short стартует с 24 (§4.1), long — с 48
    (§8.2). Явное число (в том числе меньше дефолта) уважается как есть —
    `--start` переопределяет sample-дефолт.
    """
    validate_sample(sample)
    if start is not None:
        return start
    return DEFAULT_LONG_START if sample == "long" else DEFAULT_START


ONE_DAY = timedelta(days=1)

# Сплит статьи (§4.4): train 2013-10…2018-12 (63 месяца), test 2019-01…2024-07
# (67 месяцев). Вспомогательная таблица сопоставимости, не выбор модели.
# ARTICLE_TRAIN_MONTHS — длина train статьи (2009-01…2018-12), с которой
# сравнивается train-часть фиксированного сплита в отчёте и примечаниях.
ARTICLE_TRAIN_END = date(2018, 12, 1)
ARTICLE_TEST_END = date(2024, 7, 1)
ARTICLE_TRAIN_MONTHS = 120

# DM-HLN (§4.3): двусторонний уровень 5% фиксирован до результатов; блочный
# бутстрап разницы RMSE — блок 3 месяца, 2000 ресэмплов, сид прогона.
ALPHA = 0.05
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000

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
    live: bool = False,
) -> tuple[Optional[dict[str, float | int]], str]:
    """Базовые признаки месяца `M` (§2.4) или `(None, причина пропуска)`.

    `cutoff` передаётся явно (для месяца — `cutoff_for(month)`, для живого
    режима — дата прогона), но правила арифметики окон от него не зависят.
    `y_lag1`/`y_lag2` — метки общего ИПЦ за `M−1`/`M−2` с проверкой
    `available_from_mes ≤ cutoff`; `usd_mm` — средние за `M−1`/`M−2`;
    `key_rate` — последняя точка ≤ cutoff (`asof`); `fao_mm` — последняя пара
    доступных точек.

    **NaN-политика long-sample (§8.2):** отсутствие `usd_mm`/`key_rate`
    (ряды начинаются позже старта long — USD с 2013-01, ставка с 2013-09-17)
    даёт `float("nan")`, а НЕ причину пропуска: месяц остаётся в выборке,
    CatBoost обрабатывает NaN нативно, B3 импутирует медианой train-окна.
    Обязательными остаются `y_lag1`, `y_lag2` и `fao_mm` — без них строка не
    собирается (и в short, и в long).

    `live=True` — живой режим (§5.3): наличие метки `M−1` в `rosstat_ipc_mes`
    и есть её публикация (месячный ИПЦ приходит в базу из релиза Росстата),
    поэтому консервативная проверка `available_from_mes ≤ cutoff` для лагов НЕ
    применяется — иначе уже вышедшая метка была бы исключена до конца `M+1`.
    Прочие признаки (USD/ставка/FAO) по-прежнему снимаются as-of `cutoff`.
    """
    lag1_month = previous_month(month)
    lag2_month = previous_month(lag1_month)
    values = targets_index.get(TARGET_ALL, {})
    lag1 = values.get(month_end(lag1_month))
    if lag1 is None:
        return None, "нет метки общего ИПЦ за M−1 (y_lag1)"
    if not live and available_from_mes(month_end(lag1_month)) > cutoff:
        return None, "метка M−1 не опубликована к cutoff (y_lag1)"
    lag2 = values.get(month_end(lag2_month))
    if lag2 is None:
        return None, "нет метки общего ИПЦ за M−2 (y_lag2)"
    if not live and available_from_mes(month_end(lag2_month)) > cutoff:
        return None, "метка M−2 не опубликована к cutoff (y_lag2)"
    usd_value = usd_mm(month, usd)
    if usd_value is None:
        usd_value = float("nan")
    rate = asof(key_rate, available_from_rate, cutoff)
    if rate is None:
        rate = float("nan")
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
    month: date, survey: dict, cutoff: date, live: bool = False
) -> tuple[Optional[dict[str, float]], str]:
    """Признаки МП месяца `M` (§2.5) или `(None, причина пропуска)`.

    Бэктест (`live=False`): месячные ряды — уровень ровно за `M−1`, изменение —
    `M−1 − M−2`; обе точки обязательны и проверяются на
    `available_from_survey ≤ cutoff`. Квартальные — последняя опубликованная
    точка ≤ cutoff, перенос вперёд до следующей опубликованной (точка месяца `M`
    на cutoff `M+1` не участвует: её доступность — конец `M+1`, позже cutoff).
    Отсутствие любой обязательной точки — причина пропуска (не NaN: молчаливое
    заполнение сместило бы информационное множество).

    `live=True` — живой режим (§5.3): «МП — последняя доступная по правилу
    §2.6», а не строго `M−1`. Если свежая точка ещё не опубликована (на
    2026-10-18 сентябрьский опрос не вышел), берётся последняя доступная, а
    глубина отставания называется в `note` артефакта. Изменение — разность двух
    последних ДОСТУПНЫХ точек ряда; квартальные — как в бэктесте. Точка `M`
    живого режима тоже не участвует (конец `M+1` > даты прогона).
    """
    levels: dict[str, float] = {}
    if live:
        for series in MP_MONTHLY:
            eligible = sorted(
                (
                    point
                    for point in survey.get(series, [])
                    if available_from_survey(point[0]) <= cutoff
                ),
                key=lambda point: point[0],
            )
            if not eligible:
                return None, f"нет опубликованной точки МП {series} к дате прогона"
            levels[series] = eligible[-1][1]
            change = MP_CHANGES.get(series)
            if change is not None:
                if len(eligible) < 2:
                    return None, (
                        f"нет пары опубликованных точек МП {series} для {change} "
                        f"к дате прогона"
                    )
                levels[change] = eligible[-1][1] - eligible[-2][1]
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
    targets: dict,
    survey: dict,
    usd: list,
    key_rate: list,
    fao: list,
    sample: str = DEFAULT_SAMPLE,
) -> tuple[list[Row], list[tuple[date, str]]]:
    """Собирает выборку `([Row], [(месяц, причина)])` (спека §2.3–2.6, §8.2).

    Кандидаты — месяцы с полной меткой (`sample_months`), только внутри
    `SAMPLE_BOUNDS[sample]`: `short` — 155 месяцев 2013-10…2026-08 (дефолт,
    прежняя выборка), `long` — 292 месяца 2002-05…2026-08 (эксперимент на
    репрезентативность; старт 2002-05 задан глубиной живого МП, §8.2).
    Неизвестное имя выборки — `ValueError`.

    Месяц, у которого не собирается ЛЮБОЙ обязательный признак (базовый или
    МП), попадает в «пропущенные» с причиной, а прогон не падает. В long
    отсутствие `usd_mm`/`key_rate` пропуском НЕ является: признак получает NaN
    (`base_features`, §8.2), строка остаётся.

    Строки ОБЩИЕ для наборов «с МП» и «без МП»: у месяца без МП нет строки
    вовсе, а не «строка только в базовом наборе» — иначе фолды разъехались бы.
    `y` — все четыре метки §2.1; собственный таргет месяца в признаки не
    попадает (cutoff(M) строго раньше публикации метки M).
    """
    first, last = SAMPLE_BOUNDS[validate_sample(sample)]
    index = target_index(targets)
    rows: list[Row] = []
    skipped: list[tuple[date, str]] = []
    for month in sample_months(targets):
        if month < first or month > last:
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


def nan_control_counts(sample: list[Row]) -> tuple[int, int]:
    """Сколько строк с NaN в `usd_mm` и `key_rate` (контроли long-sample, §8.2).

    В short-выборке оба контроля заполнены всегда (0/0). В long NaN стоит до
    глубины ряда (USD — 2013-01, ставка — 2013-09-17): число печатается в
    консоли и отчёте, чтобы отсутствие контролей было видно, а не пряталось за
    «0 пропусков».
    """
    def count(name: str) -> int:
        return sum(1 for row in sample if math.isnan(float(row.features[name])))

    return count("usd_mm"), count("key_rate")


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
    словарь пуст); `weights` — «модель → веса NNLS» последнего успешного фолда
    ЭТОГО окна (bottom-up; у моделей без весов ключа нет — иначе веса одного
    окна приписались бы другому); `main` — основное окно прогона (для блока
    «последние 12 фолдов»).
    """

    start: int
    embargo: int
    main: bool
    folds: int
    predictions: list[Prediction]
    skipped: dict[str, int]
    notes: dict[str, dict[str, int]]
    weights: dict[str, tuple] = field(default_factory=dict)


def window_key(start: int, embargo: int) -> str:
    """Метка окна для CSV и таблиц: `start=N` и `, embargo=K` при K > 0."""
    return f"start={start}" + (f", embargo={embargo}" if embargo else "")


def window_title(run: WindowRun) -> str:
    """Метка окна в отчёте: ключ плюс роль (основное/робастность)."""
    return f"{window_key(run.start, run.embargo)} ({'основное' if run.main else 'робастность'})"


def window_specs(
    start: int = DEFAULT_START,
    embargo: int = DEFAULT_EMBARGO,
    quick: bool = False,
) -> list[tuple[int, int]]:
    """Окна прогона: основное (`start`/`embargo`) и робастность спеки §4.1.

    Робастность — второй старт (`ROBUST_START`) и эмбарго 1; дубликаты и
    несовместимые окна (`embargo ≥ start` — обучать не на чем) отбрасываются,
    поэтому `--embargo-months 1` не даёт основное окно дважды.

    `quick=True` — быстрый экспериментальный режим (спека `2026-10-18-ml-quick-runs`):
    считается ТОЛЬКО основное окно, robustness-окна не добавляются. Полный
    прогон (`quick=False`) не меняется.
    """
    specs = [(start, embargo)]
    if quick:
        return specs
    for candidate in ((ROBUST_START, embargo), (start, ROBUST_EMBARGO)):
        if candidate not in specs and 0 <= candidate[1] < candidate[0]:
            specs.append(candidate)
    return specs


def _check_walk_forward_window(start: int, embargo: int) -> None:
    """Окно walk-forward обязано иметь обучающий префикс (start ≥ 1, embargo < start)."""
    if start < 1:
        raise ValueError(f"walk-forward: start={start} — меньше одного месяца")
    if embargo < 0 or embargo >= start:
        raise ValueError(
            f"walk-forward: embargo={embargo} при start={start} — обучающего окна нет"
        )


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
    _check_walk_forward_window(start, embargo)
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


def _run_fold_worker(task: tuple) -> tuple:
    """Воркер пула: все модели ОДНОГО фолда (top-level, picklable, spawn-safe).

    Задача — `(index, train, row, model_names)`; модели берутся из глобального
    `MODELS` (имена сериализуемы, функции — нет). Возврат — `(index, {model:
    {"pred", "skipped", "notes", "weights"}})`: главный процесс собирает
    `Prediction` в порядке фолдов и моделей, суммирует `skipped`/`notes` и берёт
    веса последнего успешного bottom-up-фолда. `DegenerateWindowError` — пропуск
    фолда этой модели; прочие исключения пробрасываются, как в `walk_forward`.

    `thread_count=1`: параллельный путь существует только в quick (спека
    `2026-10-18-ml-quick-runs` §2.2), поэтому каждый воркер фиксирует один поток
    на fit — иначе N процессов × дефолтные потоки oversubscribe'или бы CPU.
    Функция обязана оставаться top-level: `spawn` на macOS pickle'ит ссылку, а
    не замыкание.
    """
    index, train, row, model_names = task
    result = {}
    with catboost_thread_count(QUICK_CATBOOST_THREAD_COUNT):
        for name in model_names:
            fit_predict = MODELS[name]
            try:
                value = float(fit_predict(train, row))
            except DegenerateWindowError:
                result[name] = {
                    "pred": None,
                    "skipped": True,
                    "notes": {},
                    "weights": None,
                }
                continue
            fold_notes = getattr(fit_predict, "fold_notes", None) or {}
            last_weights = getattr(fit_predict, "last_weights", None) or ()
            result[name] = {
                "pred": value,
                "skipped": False,
                "notes": {key: int(count) for key, count in fold_notes.items()},
                "weights": tuple(float(weight) for weight in last_weights) or None,
            }
    return index, result


def walk_forward_parallel(
    rows: list[Row],
    models: dict[str, FitPredict],
    start: int,
    embargo: int,
    jobs: int,
    skipped: Optional[dict[str, int]] = None,
    notes: Optional[dict[str, dict[str, int]]] = None,
    weights: Optional[dict[str, tuple]] = None,
) -> list[Prediction]:
    """Параллельный walk-forward одного окна: один пул процессов на фолды.

    Единица параллелизма — фолд, а не отдельный fit: воркер получает префикс
    `rows[:i−embargo]`, тестовую строку и имена моделей, а считает все модели
    фолда (спека quick §2.2). `jobs > 1` — `ProcessPoolExecutor` со `spawn`
    (macOS без глобального состояния), `max_workers=min(jobs, cpu_count())`;
    `jobs == 1` — последовательный `walk_forward` (эквивалент для тестов).
    Результаты собираются в порядке фолдов и `models`, поэтому порядок
    `Prediction` детерминирован. `skipped`/`notes`/`weights` — необязательные
    словари, которые заполняет вызывающий (как у `walk_forward`).
    """
    _check_walk_forward_window(start, embargo)
    if jobs < 1:
        raise ValueError(f"walk-forward parallel: jobs={jobs} — меньше одного процесса")
    if jobs == 1:
        with catboost_thread_count(QUICK_CATBOOST_THREAD_COUNT):
            predictions = walk_forward(
                rows, models, start, embargo, skipped=skipped, notes=notes
            )
        if weights is not None:
            weights.update(_window_weights(models))
        return predictions

    model_names = list(models)
    counts = skipped if skipped is not None else {}
    note_totals = notes if notes is not None else {}
    last_weights = weights if weights is not None else {}
    tasks = (
        (index, rows[: index - embargo], rows[index], model_names)
        for index in range(start, len(rows))
    )
    worker_count = min(int(jobs), os.cpu_count() or 1)
    predictions = []
    context = multiprocessing.get_context("spawn")
    with ProcessPoolExecutor(
        max_workers=worker_count, mp_context=context
    ) as pool:
        for index, fold in pool.map(_run_fold_worker, tasks):
            row = rows[index]
            for name in model_names:
                result = fold[name]
                if result["skipped"]:
                    counts[name] = counts.get(name, 0) + 1
                    continue
                for key, count in result["notes"].items():
                    totals = note_totals.setdefault(name, {})
                    totals[key] = totals.get(key, 0) + int(count)
                if result["weights"]:
                    last_weights[name] = tuple(result["weights"])
                predictions.append(
                    Prediction(
                        month=row.month,
                        model=name,
                        y_true=float(row.y[TARGET_ALL]),
                        y_pred=float(result["pred"]),
                    )
                )
    return predictions


def _reset_fold_weights(models: dict[str, FitPredict]) -> None:
    """Сбрасывает диагностику весов моделей перед окном walk-forward.

    `last_weights`/`last_components` — атрибуты функции-модели, а не результата
    фолда: без сброса окно, все фолды которого вырождены, отчиталось бы весами
    ПРЕДЫДУЩЕГО окна. Сброс — только у моделей, которые такой протокол
    объявляют; у остальных атрибутов нет, и трогать их нечего.
    """
    for fit_predict in models.values():
        if hasattr(fit_predict, "last_weights"):
            fit_predict.last_weights = ()
        if hasattr(fit_predict, "last_components"):
            fit_predict.last_components = ()


def _window_weights(models: dict[str, FitPredict]) -> dict[str, tuple]:
    """«Модель → веса» последнего успешного фолда окна (диагностика bottom-up).

    Читается ПОСЛЕ `walk_forward`: функция модели хранит веса последнего
    успешного вызова. Модели без весов (direct, бенчмарки) в словарь не
    попадают — отчёт печатает только реально посчитанные веса.
    """
    weights = {}
    for name, fit_predict in models.items():
        last = getattr(fit_predict, "last_weights", ())
        if last:
            weights[name] = tuple(float(value) for value in last)
    return weights


def evaluate_windows(
    rows: list[Row],
    models: dict[str, FitPredict],
    specs: Optional[list[tuple[int, int]]] = None,
    quick: bool = False,
    jobs: int = 0,
) -> list[WindowRun]:
    """Walk-forward по всем окнам прогона; основное — первое окно списка.

    Счётчики пропусков создаются по всем моделям словаря (нули тоже), поэтому
    `WindowRun.skipped` хранит порядок моделей `models` — от него зависят строки
    таблиц отчёта, а не алфавит. Диагностика `fold_notes` собирается по окну
    заново (`WindowRun.notes`): число недосошедших фолдов относится к
    КОНКРЕТНОМУ окну, а не к прогону целиком. Веса NNLS (`WindowRun.weights`)
    снимаются с модели после каждого окна — тоже по окну, а не по всему
    прогону.

    `quick=True, jobs>1` — пул процессов по фолдам (`walk_forward_parallel`,
    спека `2026-10-18-ml-quick-runs` §2.2); иначе — прежний последовательный
    `walk_forward`. Quick-путь фиксирует `thread_count=1` у CatBoost (один fit —
    один поток), полный путь без флагов `thread_count` не задаёт. Полный режим
    пул не использует: `jobs>1` без `quick` — ошибка.
    """
    if jobs < 0:
        raise ValueError(f"evaluate_windows: jobs={jobs} — отрицательное число процессов")
    if jobs > 1 and not quick:
        raise ValueError(
            f"evaluate_windows: jobs={jobs} без quick — полный прогон не параллелится"
        )
    runs = []
    for position, (start, embargo) in enumerate(
        window_specs() if specs is None else specs
    ):
        skipped = {name: 0 for name in models}
        notes = {name: {} for name in models}
        _reset_fold_weights(models)
        if quick and jobs > 1:
            weights: dict[str, tuple] = {}
            predictions = walk_forward_parallel(
                rows,
                models,
                start,
                embargo,
                jobs,
                skipped=skipped,
                notes=notes,
                weights=weights,
            )
        else:
            with catboost_thread_count(QUICK_CATBOOST_THREAD_COUNT if quick else None):
                predictions = walk_forward(
                    rows, models, start, embargo, skipped=skipped, notes=notes
                )
            weights = _window_weights(models)
        runs.append(
            WindowRun(
                start=start,
                embargo=embargo,
                main=position == 0,
                folds=max(0, len(rows) - start),
                predictions=predictions,
                skipped=skipped,
                notes=notes,
                weights=weights,
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
MODEL_CATBOOST_DIRECT = "catboost_direct"
MODEL_CATBOOST_BOTTOMUP = "catboost_bottomup"
MODEL_CATBOOST_DIRECT_MP = "catboost_direct_mp"
MODEL_CATBOOST_BOTTOMUP_MP = "catboost_bottomup_mp"

# Bottom-up модели: только у них есть веса NNLS, которые отчёт печатает по
# окнам (порядок — как в `CATBOOST_MODELS`).
BOTTOMUP_MODELS = (MODEL_CATBOOST_BOTTOMUP, MODEL_CATBOOST_BOTTOMUP_MP)

# Метки моделей для отчёта: бенчмарки названы номерами спеки §3.4, CatBoost —
# схемой агрегации §3.3. B3 и CatBoost идут двумя пространствами признаков —
# эффект МП отделяется от эффекта алгоритма.
MODEL_LABELS = {
    MODEL_LAST: "B1 last value",
    MODEL_AR1: "B2 AR(1)+сезонные дамми",
    MODEL_ENET_BASE: "B3 ElasticNetCV (без МП)",
    MODEL_ENET_MP: "B3 ElasticNetCV (с МП)",
    MODEL_CATBOOST_DIRECT: "CatBoost direct (без МП)",
    MODEL_CATBOOST_BOTTOMUP: "CatBoost bottom-up (без МП)",
    MODEL_CATBOOST_DIRECT_MP: "CatBoost direct (с МП)",
    MODEL_CATBOOST_BOTTOMUP_MP: "CatBoost bottom-up (с МП)",
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
    возникнуть «только на предсказании». У прогнозной строки живого nowcast
    (`Row.y` пуст — таргета месяца ещё нет) `y` не заполняется: дизайн от неё
    не зависит, и подстановка выдуманного таргета была бы ложью.
    """
    design, targets = [], []
    for row in rows:
        month = int(row.features["month"])
        design.append(
            [1.0, float(row.features["y_lag1"])]
            + [1.0 if month == dummy else 0.0 for dummy in AR1_DUMMY_MONTHS]
        )
        if TARGET_ALL in row.y:
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


# ---------------------------------------------------------------------------
# CatBoost: direct и покомпонентный bottom-up (спека §3.1–3.3)
# ---------------------------------------------------------------------------

# Конфигурация CatBoost (§3.1): заданы `loss_function`, `random_seed`, `verbose`
# (дословно из спеки), monotone-ограничения и категориальный месяц. Остальные
# гиперпараметры НЕ задаются — дефолты библиотеки; `CATBOOST_DEFAULTS` фиксирует
# их значения для метаданных прогона («конфигурация пишется в метаданные
# модели»), тест сверяет их с `get_all_params()` обученной модели.
CATBOOST_LOSS = "RMSE"
CATBOOST_CAT_FEATURES = ("month",)
CATBOOST_MONOTONE = {
    "usd_mm": 1,
    "price_expectations_sa": 1,
    "price_expectations_sa_chg": 1,
}
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


def catboost_numeric_features(with_mp: bool) -> list[str]:
    """Числовые признаки матрицы CatBoost: `feature_names` без `month`.

    Порядок колонок — контракт с `monotone_constraints` и `catboost_matrix`;
    `month` идёт последней колонкой и передаётся в `cat_features`.
    """
    return [name for name in feature_names(with_mp) if name not in CATBOOST_CAT_FEATURES]


def catboost_cat_indices(with_mp: bool) -> list[int]:
    """Позиции категориальных признаков в матрице (`cat_features` конструктора).

    CatBoost на numpy-матрице ждёт ИНДЕКСЫ, а не имена (имена — для DataFrame);
    индекс берётся из `feature_names`, а не из литерала, поэтому сдвиг порядка
    признаков не разъедет колонки и ограничения.
    """
    names = feature_names(with_mp)
    return [names.index(name) for name in CATBOOST_CAT_FEATURES]


def monotone_constraints(with_mp: bool) -> list[int]:
    """`monotone_constraints` §3.1: `+1` трём признакам, остальным 0.

    Ограничения заданы ИМЕНАМИ признаков, а возвращаются позиционно (индекс
    столбца матрицы — так их читает CatBoost): сдвиг порядка
    `feature_names(with_mp)` не может молча перенести «+1» на другой признак.
    У категориального `month` — 0: CatBoost допускает ограничения только на
    float-признаках. В базовом наборе из трёх имён присутствует только
    `usd_mm`; `_mp`-набор добавляет ценовые ожидания.
    """
    return [CATBOOST_MONOTONE.get(name, 0) for name in feature_names(with_mp)]


# Quick-режим: один fit — один поток (спека `2026-10-18-ml-quick-runs` §2.2).
# Полный путь контекст не включает и `thread_count` не задаёт.
QUICK_CATBOOST_THREAD_COUNT = 1

# Контекст `thread_count` CatBoost: выставляется на quick-прогон (последовательно
# или в воркере пула) и читается конструктором/метаданными. ContextVar, а не
# глобал, — чтобы значение гарантированно снималось после блока и не протекало
# в полный путь (в том числе в тестах, идущих следом).
_CATBOOST_THREAD_COUNT: ContextVar[Optional[int]] = ContextVar(
    "cpi_monthly_catboost_thread_count", default=None
)


@contextlib.contextmanager
def catboost_thread_count(count: Optional[int]):
    """Временно фиксирует `thread_count` CatBoost (`None` — ничего не меняет)."""
    if count is None:
        yield
        return
    token = _CATBOOST_THREAD_COUNT.set(int(count))
    try:
        yield
    finally:
        _CATBOOST_THREAD_COUNT.reset(token)


def catboost_params(with_mp: bool, thread_count: Optional[int] = None) -> dict:
    """Параметры конструктора CatBoost (§3.1) — единственные заданные, дословно.

    Словарь — ровно то, что уходит в `CatBoostRegressor`; он же сверяется с
    `get_params()` собранной модели, поэтому любое добавленное поле означало бы
    подменённый дефолт библиотеки. `allow_writing_files=False` — флаг побочного
    эффекта, а не гиперпараметр: без него CatBoost пишет каталог
    `catboost_info/` в рабочий каталог прогона.

    `thread_count` добавляется ТОЛЬКО когда он задан (quick-режим: 1 поток на
    fit, чтобы процессный параллелизм не oversubscribe'ил CPU; спека
    `2026-10-18-ml-quick-runs` §2.2). Полный путь не задаёт `thread_count` —
    словарь и дефолт библиотеки прежние. Явный аргумент важнее контекста
    `catboost_thread_count`.
    """
    params = {
        "loss_function": CATBOOST_LOSS,
        "random_seed": SEED,
        "verbose": False,
        "monotone_constraints": monotone_constraints(with_mp),
        "cat_features": catboost_cat_indices(with_mp),
        "allow_writing_files": False,
    }
    count = _CATBOOST_THREAD_COUNT.get() if thread_count is None else thread_count
    if count is not None:
        params["thread_count"] = int(count)
    return params


def catboost_model(
    with_mp: bool, thread_count: Optional[int] = None
) -> CatBoostRegressor:
    """Новый (необученный) `CatBoostRegressor` с конфигурацией §3.1."""
    return CatBoostRegressor(**catboost_params(with_mp, thread_count=thread_count))


def catboost_metadata(with_mp: bool, thread_count: Optional[int] = None) -> dict:
    """Метаданные модели CatBoost для отчёта/артефактов (§3.1).

    Заданная конфигурация — по ИМЕНАМ признаков (индекс сдвинется при правке
    `feature_names`, имя нет) плюс дефолты библиотеки, которые код не
    подменяет. `cat_features` — имена (человекочитаемые, JSON), а
    `cat_feature_indices` — позиции тех же признаков в numpy-матрице:
    CatBoost на numpy принимает ТОЛЬКО индексы, поэтому в `catboost_params`
    уходят позиции, а имена остаются в метаданных. Словарь сериализуется в
    JSON как есть (Task 4 кладёт его рядом с прогнозом модели).

    `thread_count` попадает в метаданные только когда он задан (quick: 1) —
    конфигурация паспорта обязана соответствовать конструктору.
    """
    names = feature_names(with_mp)
    constraints = monotone_constraints(with_mp)
    metadata = {
        "loss_function": CATBOOST_LOSS,
        "random_seed": SEED,
        "cat_features": list(CATBOOST_CAT_FEATURES),
        "cat_feature_indices": catboost_cat_indices(with_mp),
        "num_features": catboost_numeric_features(with_mp),
        "monotone_constraints": {
            name: value for name, value in zip(names, constraints)
        },
        "allow_writing_files": False,
        "defaults": dict(CATBOOST_DEFAULTS),
    }
    count = _CATBOOST_THREAD_COUNT.get() if thread_count is None else thread_count
    if count is not None:
        metadata["thread_count"] = int(count)
    return metadata


def catboost_matrix(rows: list[Row], with_mp: bool) -> np.ndarray:
    """Матрица признаков CatBoost: числовые float + целые коды месяца (§3.1).

    Тип — `object`, и это не украшение: `cat_features` требует не-float матрицу
    (CatBoost отказывает: «'data' is numpy array of floating point numerical
    type, it means no categorical features»), а код месяца обязан дойти целым —
    «10» и «10.0» для категориального признака разные значения. Порядок
    колонок: `catboost_numeric_features(with_mp)`, затем `month`. NaN числовых
    колонок передаётся как есть: библиотека обрабатывает пропуски нативно
    (`nan_mode` по умолчанию `Min`), в отличие от B3, которая импутирует.
    """
    names = catboost_numeric_features(with_mp)
    matrix = np.empty(
        (len(rows), len(names) + len(CATBOOST_CAT_FEATURES)), dtype=object
    )
    for position, name in enumerate(names):
        matrix[:, position] = [float(row.features[name]) for row in rows]
    for offset, name in enumerate(CATBOOST_CAT_FEATURES):
        matrix[:, len(names) + offset] = [int(row.features[name]) for row in rows]
    return matrix


@dataclass(frozen=True)
class CatBoostFit:
    """Подобранный фолд CatBoost: модель, матрица, таргет и набор признаков.

    `target` — код таргета §2.1, на который обучалась модель (`TARGET_ALL` у
    direct, код компоненты у bottom-up); `targets` — значения этого таргета на
    обучающем окне; `with_mp` — пространство признаков (матрица тестовой строки
    строится в том же пространстве). Отчёт читает только метаданные модели;
    остальное — пины тестов.
    """

    model: CatBoostRegressor
    matrix: np.ndarray
    target: str
    targets: np.ndarray
    with_mp: bool


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


def _fit_catboost(
    train: list[Row],
    target: str,
    with_mp: bool,
    thread_count: Optional[int] = None,
) -> CatBoostFit:
    """Обучает CatBoost §3.1 на окне `train` для таргета `target`.

    Обучение только на переданном окне (никаких обращений к выборке целиком):
    фолд walk-forward передаёт сюда исключительно прошлые месяцы. `thread_count`
    (quick: 1) пробрасывается в конструктор; по умолчанию берётся из контекста
    `catboost_thread_count`, а вне quick-контекста не задаётся.
    """
    matrix = catboost_matrix(train, with_mp)
    targets = _catboost_targets(train, target)
    _assert_catboost_window(matrix, targets, target)
    model = catboost_model(with_mp, thread_count=thread_count)
    model.fit(matrix, targets)
    return CatBoostFit(
        model=model, matrix=matrix, target=target, targets=targets, with_mp=with_mp
    )


def catboost_forecast(fit: CatBoostFit, row: Row) -> float:
    """Прогноз обученной модели для тестовой строки (та же матрица признаков)."""
    return float(fit.model.predict(catboost_matrix([row], fit.with_mp))[0])


def fit_catboost_direct(train: list[Row], row: Row, with_mp: bool = False) -> float:
    """`catboost_direct` (§3.2): CatBoost на таргет `cpi_all`, прогноз в п.п.

    Агрегация не применяется: модель прогнозирует общий ИПЦ напрямую. Подгонка —
    на каждом фолде заново, только по обучающему окну; подобранная модель
    последнего фолда остаётся в `fit_catboost_direct.last_fit` (пины конфигурации
    и метаданные отчёта). `with_mp` — пространство признаков (по умолчанию
    базовое, §2.4).
    """
    fit = _fit_catboost(train, TARGET_ALL, with_mp)
    fit_catboost_direct.last_fit = fit
    return catboost_forecast(fit, row)


def fit_catboost_direct_mp(train: list[Row], row: Row) -> float:
    """`catboost_direct_mp` (§3.3): direct на базовых признаках + 7 признаках МП."""
    value = fit_catboost_direct(train, row, with_mp=True)
    fit_catboost_direct_mp.last_fit = fit_catboost_direct.last_fit
    return value


# Диагностика последнего фолда direct: читают тесты (пины параметров и дефолтов)
# и метаданные отчёта; атрибуты заполняются при каждом вызове.
for _function in (fit_catboost_direct, fit_catboost_direct_mp):
    _function.last_fit = None


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
        [[float(row.y[code]) for code in BOTTOMUP_COMPONENTS] for row in train],
        dtype=float,
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


def fit_catboost_component(
    train: list[Row], row: Row, code: str, with_mp: bool = False
) -> float:
    """Один компонентный CatBoost (§3.2): таргет — метка компоненты `code`.

    Отдельная функция, а не цикл внутри bottom-up, — точка подмены для тестов:
    сверка агрегата `Σ w·ŷ` на заглушечных компонентных прогнозах. Вырожденное
    окно компоненты (пустое или неизменный таргет) — `DegenerateWindowError`
    всего фолда bottom-up.
    """
    return catboost_forecast(_fit_catboost(train, code, with_mp), row)


def fit_catboost_bottomup(
    train: list[Row], row: Row, with_mp: bool = False
) -> float:
    """`catboost_bottomup` (§3.2): `Σ w_j · ŷ_j` — компоненты и веса NNLS.

    Веса считаются по ТОМУ ЖЕ обучающему окну фолда, что и компонентные модели
    (утечки нет), и нормированы к 1. Вырожденность ЛЮБОГО компонентного фита или
    весов (`DegenerateWindowError`) пропускает фолд целиком: частичный агрегат
    («два компонента из трёх») прогнозом общего ИПЦ не является. Веса и
    компонентные прогнозы последнего успешного фолда остаются в `last_weights` и
    `last_components` (читают тесты и отчёт — блок весов NNLS).
    """
    weights = nnls_weights(train)
    components = tuple(
        fit_catboost_component(train, row, code, with_mp)
        for code in BOTTOMUP_COMPONENTS
    )
    fit_catboost_bottomup.last_weights = tuple(weights)
    fit_catboost_bottomup.last_components = components
    return float(
        sum(weight * value for weight, value in zip(weights, components))
    )


def fit_catboost_bottomup_mp(train: list[Row], row: Row) -> float:
    """`catboost_bottomup_mp` (§3.3): bottom-up на базовых признаках + МП."""
    value = fit_catboost_bottomup(train, row, with_mp=True)
    fit_catboost_bottomup_mp.last_weights = fit_catboost_bottomup.last_weights
    fit_catboost_bottomup_mp.last_components = fit_catboost_bottomup.last_components
    return value


# Веса и компонентные прогнозы последнего успешного фолда bottom-up: читают
# тесты и `evaluate_windows` (веса каждого окна — в `WindowRun.weights`); не
# часть контракта `FitPredict`, а диагностика фолда.
for _function in (fit_catboost_bottomup, fit_catboost_bottomup_mp):
    _function.last_weights = ()
    _function.last_components = ()


# CatBoost-варианты §3.3: direct/bottom-up × без МП/с МП. Порядок — как в
# спеке; он же задаёт строки таблиц отчёта и порядок пар DM (Task 4).
CATBOOST_MODELS = {
    MODEL_CATBOOST_DIRECT: fit_catboost_direct,
    MODEL_CATBOOST_BOTTOMUP: fit_catboost_bottomup,
    MODEL_CATBOOST_DIRECT_MP: fit_catboost_direct_mp,
    MODEL_CATBOOST_BOTTOMUP_MP: fit_catboost_bottomup_mp,
}


# Бенчмарки спеки §3.4 — порядок B1, B2, B3-base, B3-mp задаёт строки таблиц
# отчёта и порядок пар будущих DM-тестов (Task 4).
BENCHMARKS = {
    MODEL_LAST: predict_last,
    MODEL_AR1: fit_ar1,
    MODEL_ENET_BASE: fit_enet_base,
    MODEL_ENET_MP: fit_enet_mp,
}

# Модели прогона: бенчмарки, затем четыре CatBoost-варианта (Task 3). Порядок
# словаря — порядок строк таблиц отчёта и пары DM (Task 4).
MODELS = {**BENCHMARKS, **CATBOOST_MODELS}


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
# DM-HLN, блочный бутстрап и сплит статьи (спека §4.3–4.5)
# ---------------------------------------------------------------------------

# Primary-пары — эффект МП: каждый `_mp`-вариант против своей базовой модели на
# тех же фолдах. Порядок — direct, bottom-up, B3 (как в спеке §4.3).
PRIMARY_PAIRS = (
    (MODEL_CATBOOST_DIRECT_MP, MODEL_CATBOOST_DIRECT),
    (MODEL_CATBOOST_BOTTOMUP_MP, MODEL_CATBOOST_BOTTOMUP),
    (MODEL_ENET_MP, MODEL_ENET_BASE),
)

# Secondary-пары — контекст: `_mp`-модели против B1 и B3-mp (у B3-mp сама с
# собой пара не строится), direct против bottom-up.
SECONDARY_PAIRS = (
    (MODEL_CATBOOST_DIRECT_MP, MODEL_LAST),
    (MODEL_CATBOOST_BOTTOMUP_MP, MODEL_LAST),
    (MODEL_ENET_MP, MODEL_LAST),
    (MODEL_CATBOOST_DIRECT_MP, MODEL_ENET_MP),
    (MODEL_CATBOOST_BOTTOMUP_MP, MODEL_ENET_MP),
    (MODEL_CATBOOST_DIRECT, MODEL_CATBOOST_BOTTOMUP),
)


@dataclass(frozen=True)
class PairTest:
    """DM-HLN двух моделей на ОБЩИХ тестовых месяцах (спека §4.3).

    `diff = RMSE(A) − RMSE(B)` в п.п.: отрицательное значение значит, что модель
    A точнее. `p_value` — двусторонний p-value DM с поправкой HLN (`h=1`);
    `note` — кто точнее и значима ли разница на 5% (без подгонки формулировок).
    """

    model_a: str
    model_b: str
    p_value: float
    diff: float
    note: str


@dataclass(frozen=True)
class PairBootstrap:
    """95%-интервал и p-value блочного бутстрапа разницы RMSE пары (§4.2–4.3)."""

    model_a: str
    model_b: str
    rmse_diff: float
    low: float
    high: float
    p_value: float


def _pair_common_errors(
    predictions: list[Prediction], model_a: str, model_b: str
) -> tuple[list, list, list]:
    """Общие месяцы пары и ошибки обеих моделей на них.

    Пара строится ТОЛЬКО по месяцам, где обе модели дали прогноз: пропущенный
    фолд одной модели не сдвигает ошибки другой и не подменяется соседним
    месяцем. Порядок месяцев — возрастающий (нужен блочному бутстрапу).
    """
    by_model: dict[str, dict] = {}
    for item in predictions:
        by_model.setdefault(item.model, {})[item.month] = item
    months_a = by_model.get(model_a, {})
    months_b = by_model.get(model_b, {})
    months = sorted(set(months_a) & set(months_b))
    err_a = [months_a[month].y_pred - months_a[month].y_true for month in months]
    err_b = [months_b[month].y_pred - months_b[month].y_true for month in months]
    return months, err_a, err_b


def pair_note(model_a: str, model_b: str, diff: float, p_value: float) -> str:
    """Вывод пары: кто точнее и значима ли разница (DM, 5%) — без подгонки."""
    if diff == 0.0:
        verdict = "разницы RMSE нет"
    elif diff < 0.0:
        verdict = f"точнее {model_label(model_a)}"
    else:
        verdict = f"точнее {model_label(model_b)}"
    significance = "значимо" if p_value < ALPHA else "не значимо"
    return f"{verdict}; {significance} на 5% (p={_fmt_p(p_value)})"


def pairwise_tests(
    predictions: list[Prediction], pairs: tuple
) -> list[PairTest]:
    """DM-HLN по заданным парам моделей на общих месяцах (спека §4.3).

    Пары — в порядке `pairs` (primary, затем secondary); пара с менее чем двумя
    общими месяцами пропускается: тест значимости на одной точке не определён.
    `diff = RMSE(A) − RMSE(B)`, p-value — `eval_utils.diebold_mariano(h=1)`.
    """
    tests = []
    for model_a, model_b in pairs:
        months, err_a, err_b = _pair_common_errors(
            predictions, model_a, model_b
        )
        if len(months) < 2:
            continue
        _, p_value = eu.diebold_mariano(err_a, err_b, h=1)
        diff = eu.rmse(err_a) - eu.rmse(err_b)
        tests.append(
            PairTest(
                model_a=model_a,
                model_b=model_b,
                p_value=float(p_value),
                diff=float(diff),
                note=pair_note(model_a, model_b, float(diff), float(p_value)),
            )
        )
    return tests


def bootstrap_pairs(
    predictions: list[Prediction],
    pairs: tuple,
    block: int = BOOTSTRAP_BLOCK,
    n: int = BOOTSTRAP_N,
    seed: int = SEED,
) -> list[PairBootstrap]:
    """Блочный бутстрап разницы RMSE для пар (primary) на общих месяцах (§4.2).

    Конфигурация зафиксирована спекой: блок 3 месяца, 2000 ресэмплов,
    `seed=SEED`; ресэмплируются пары ошибок целиком (см.
    `eval_utils.block_bootstrap_rmse_diff`). Пара без двух общих месяцев
    пропускается, как и в `pairwise_tests`.
    """
    results = []
    for model_a, model_b in pairs:
        months, err_a, err_b = _pair_common_errors(
            predictions, model_a, model_b
        )
        if len(months) < 2:
            continue
        bootstrap = eu.block_bootstrap_rmse_diff(
            err_a, err_b, block=block, n=n, seed=seed
        )
        results.append(
            PairBootstrap(
                model_a=model_a,
                model_b=model_b,
                rmse_diff=float(bootstrap.rmse_diff),
                low=float(bootstrap.low),
                high=float(bootstrap.high),
                p_value=float(bootstrap.p_value),
            )
        )
    return results


def fixed_split(
    rows: list[Row],
    models: dict[str, FitPredict],
    train_end: date = ARTICLE_TRAIN_END,
    test_end: date = ARTICLE_TEST_END,
) -> list[Prediction]:
    """Фиксированный сплит статьи (§4.4): train ≤ 2018-12, test 2019-01…2024-07.

    Вспомогательная таблица сопоставимости с первоисточником, НЕ выбор модели:
    walk-forward остаётся основной оценкой. Train — месяцы `rows` не позже
    `train_end` (на живой выборке 63), test — строго после `train_end` и не
    позже `test_end` (67). Каждая модель фитится на всём train и прогнозирует
    каждый test-месяц (без переобучения по ходу теста); `DegenerateWindowError`
    модели пропускает её фолд, как в walk-forward. Пустой train или test — пустой
    список: на короткой выборке сплит не определён.
    """
    train = [row for row in rows if month_of(row.month) <= train_end]
    test = [
        row
        for row in rows
        if train_end < month_of(row.month) <= test_end
    ]
    if not train or not test:
        return []
    predictions = []
    for row in test:
        for name, fit_predict in models.items():
            try:
                value = fit_predict(train, row)
            except DegenerateWindowError:
                continue
            predictions.append(
                Prediction(
                    month=row.month,
                    model=name,
                    y_true=float(row.y[TARGET_ALL]),
                    y_pred=float(value),
                )
            )
    return predictions


def metrics_without_2022(
    predictions: list[Prediction],
) -> tuple[float, float]:
    """RMSE/MAE без тестовых месяцев 2022 года — строка «2022 исключён» (§4.5).

    Отдельная метрика, НЕ замена основной: показывает вклад шока читателю.
    Пустой остаток (все фолды — 2022) — `ValueError`: метрика не определена.
    """
    rest = [
        prediction for prediction in predictions
        if prediction.month.year != 2022
    ]
    if not rest:
        raise ValueError("2022 исключён: не осталось ни одного тестового месяца")
    return rmse_pp(rest), mae_pp(rest)


# ---------------------------------------------------------------------------
# Живой nowcast (спека §5.3)
# ---------------------------------------------------------------------------

# Мягкое предупреждение о возможном отставании импорта: календарный срок
# публикации месячного ИПЦ за M (конец M + 30 дней) прошёл, а цель всё ещё за
# M. Отбор цели не отменяется (наличие метки в данных и есть публикация), но
# пометка идёт в `note`.
IMPORT_LAG_DAYS = 30

# Базовая оговорка артефакта: чем живой прогноз отличается от бэктеста.
NOWCAST_NOTE = (
    "живой nowcast v2 (спека §5.3): признаки — as-of даты прогона, а не "
    "канонического cutoff; as-of — приближение, не point-in-time слой (§2.6); "
    "модели и веса NNLS обучены на полной выборке месяцев (не фолд walk-forward)"
)


def latest_published_month(targets: dict) -> Optional[date]:
    """Последний месяц, чья метка ОБЩЕГО ИПЦ уже опубликована (есть в данных).

    «Публикация = наличие метки в `rosstat_ipc_mes`»: месячный ИПЦ приходит в
    базу из релиза Росстата, поэтому наличие точки месяца и есть публикация.
    Бэктест так делать не может — там собственная метка месяца не должна
    попадать в признаки, и `available_from_mes` отсекает свежие точки.
    """
    points = targets.get(TARGET_ALL, [])
    if not points:
        return None
    return max(month_of(day) for day, _ in points)


def nowcast_target_month(
    targets: dict, today: date
) -> tuple[Optional[date], str]:
    """Целевой месяц живого nowcast (спека §5.3) или причина, почему его нет.

    Цель — месяц СЛЕДОМ за последним опубликованным: он и есть последний месяц,
    чей месячный ИПЦ не вышел к дате прогона. `today` в отборе не участвует
    (признаки as-of считает `current_nowcast`); параметр оставлен для сигнатуры
    контракта и будущего календарного слоя. Возможное отставание импорта
    отбор не отменяет, а смягчает — пометка в `note`.
    """
    latest = latest_published_month(targets)
    if latest is None:
        return None, (
            "в rosstat_ipc_mes нет ни одной метки общего ИПЦ — проверить импорт"
        )
    return _month_shift(latest, 1), ""


def latest_mp_month(survey: dict, cutoff: date) -> Optional[date]:
    """Месяц самой старой из последних доступных месячных точек МП (или None).

    Для пометки о глубине: если ряды разъехались, называется ХУДШИЙ из них —
    читатель видит консервативную границу информационного множества. Ряды без
    единой доступной точки игнорируются: `mp_features(live=True)` тогда сам
    откажет с причиной.
    """
    months = []
    for series in MP_MONTHLY:
        eligible = [
            day
            for day, _ in survey.get(series, [])
            if available_from_survey(day) <= cutoff
        ]
        if eligible:
            months.append(max(eligible))
    if not months:
        return None
    return min(months)


def mp_lag_note(survey: dict, month: date, cutoff: date) -> str:
    """Пометка «МП за <месяц>, глубже бэктеста на N мес.» или пустая строка.

    В бэктесте признаки МП берутся строго за `M−1`; живой режим — последнюю
    доступную точку. Если она старше `M−1`, глубина называется числом месяцев.
    """
    mp_month = latest_mp_month(survey, cutoff)
    expected = previous_month(month)
    if mp_month is None or mp_month >= expected:
        return ""
    deeper = (
        (expected.year * 12 + expected.month)
        - (mp_month.year * 12 + mp_month.month)
    )
    return (
        f"МП за {mp_month.isoformat()[:7]}, глубже бэктеста на {deeper} мес."
    )


def import_lag_note(month: date, today: date) -> str:
    """Пометка «возможное отставание импорта» или пустая строка (спека §5.3).

    Граница строгая: `month_end(month) + 30 дней < today`; ровно на сроке
    предупреждения нет — выпуск мог выйти в последний день.
    """
    if month_end(month) + IMPORT_LAG_DAYS * ONE_DAY < today:
        return (
            "возможное отставание импорта: календарный срок публикации прошёл"
        )
    return ""


@dataclass(frozen=True)
class Nowcast:
    """Текущий nowcast (спека §5.3): цель, прогнозы моделей и бенчмарков.

    `forecast_date`/`data_asof` в этой версии совпадают (признаки снимаются
    as-of даты прогона; поля разделены под будущий PIT-слой); `models` — четыре
    CatBoost-варианта (значение или `None`, если метод вырожден), `benchmarks` —
    B1–B3 в двух пространствах B3; `weights` — NNLS-веса компонент по полной
    выборке (пустой кортеж, если веса не определены), порядок —
    `BOTTOMUP_COMPONENTS`; `note` — оговорка артефакта (включая глубину МП и
    возможное отставание импорта).
    """

    forecast_date: date
    target_month: date
    data_asof: date
    models: dict
    benchmarks: dict
    weights: tuple
    sample_size: int
    note: str


def current_nowcast(
    rows: list[Row],
    targets: dict,
    survey: dict,
    usd: list,
    key_rate: list,
    fao: list,
    today: date,
    thread_count: Optional[int] = None,
) -> tuple[Optional[Nowcast], str]:
    """Текущий nowcast (спека §5.3) или `(None, причина)`.

    Цель — `nowcast_target_month`; признаки — `base_features`/`mp_features` с
    cutoff = `today` (НЕ `cutoff_for(цели)`), МП — последняя доступная по
    правилу §2.6. `Row(month=target, y={}, features=...)`: модели читают только
    `features`, таргета месяца к дате прогноза не существует. Модели и веса
    NNLS обучаются на ПОЛНОЙ выборке `rows` — это не фолд walk-forward.

    `DegenerateWindowError` метода — пропуск ЭТОГО метода (причина в `note`), а
    не падение: на короткой выборке B2/B3 вырождены законно. Если не посчитан
    НИ ОДИН метод, прогноза нет — `(None, причина)`. Исключений здесь нет
    намеренно: прогон не падает.
    """
    if not rows:
        return None, "выборка пуста — модели nowcast обучить не на чем"
    month, reason = nowcast_target_month(targets, today)
    if month is None:
        return None, reason
    features, reason = base_features(
        month, target_index(targets), usd, key_rate, fao, today, live=True
    )
    if features is None:
        return None, reason
    mp_part, reason = mp_features(month, survey, today, live=True)
    if mp_part is None:
        return None, reason
    row = Row(month=month, y={}, features={**features, **mp_part})
    values: dict[str, float] = {}
    skipped: list[tuple[str, str]] = []
    non_converged: list[str] = []
    with catboost_thread_count(thread_count):
        for name in MODELS:
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
        weights = tuple(float(weight) for weight in nnls_weights(rows))
    except DegenerateWindowError:
        weights = ()
    note = NOWCAST_NOTE
    lag_note = mp_lag_note(survey, month, today)
    if lag_note:
        note += "; " + lag_note
    import_note = import_lag_note(month, today)
    if import_note:
        note += "; " + import_note
    if non_converged:
        note += "; " + "; ".join(
            f"{model_label(name)}: координатный спуск не сошёлся на живой "
            f"выборке — прогноз индикативен"
            for name in non_converged
        )
    if skipped:
        note += "; не посчитаны: " + "; ".join(
            f"{model_label(name)} ({error})" for name, error in skipped
        )
    return (
        Nowcast(
            forecast_date=today,
            target_month=month,
            data_asof=today,
            models={name: values.get(name) for name in CATBOOST_MODELS},
            benchmarks={name: values.get(name) for name in BENCHMARKS},
            weights=weights,
            sample_size=len(rows),
            note=note,
        ),
        "",
    )


def forecast_payload(nowcast: Nowcast, thread_count: Optional[int] = None) -> dict:
    """Тело JSON текущего nowcast (спека §5.1) — ключи в порядке спеки.

    Четыре CatBoost-варианта уходят как `{"forecast", "metadata"}` (метаданные
    §3.1 «дефолты фиксируются в метаданных модели» — тем же словарём
    `catboost_metadata`); `benchmarks` — B1–B3 (B3 в двух пространствах);
    `bottomup_weights` — веса NNLS ПО МОДЕЛЯМ (`catboost_bottomup` и
    `catboost_bottomup_mp` используют один и тот же NNLS-фит полной выборки).
    Wall-clock-полей сверх дат нет: JSON детерминирован при (данные, дата).
    """
    metadata = {
        False: catboost_metadata(with_mp=False, thread_count=thread_count),
        True: catboost_metadata(with_mp=True, thread_count=thread_count),
    }
    payload: dict = {
        "forecast_date": nowcast.forecast_date.isoformat(),
        "target_month": nowcast.target_month.isoformat(),
        "data_asof": nowcast.data_asof.isoformat(),
    }
    for name, with_mp in (
        (MODEL_CATBOOST_DIRECT, False),
        (MODEL_CATBOOST_BOTTOMUP, False),
        (MODEL_CATBOOST_DIRECT_MP, True),
        (MODEL_CATBOOST_BOTTOMUP_MP, True),
    ):
        payload[name] = {
            "forecast": nowcast.models.get(name),
            "metadata": metadata[with_mp],
        }
    payload["benchmarks"] = {
        name: nowcast.benchmarks.get(name) for name in BENCHMARKS
    }
    if nowcast.weights:
        weights = {
            code: float(weight)
            for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        }
        payload["bottomup_weights"] = {
            name: dict(weights) for name in BOTTOMUP_MODELS
        }
    else:
        payload["bottomup_weights"] = {
            name: None for name in BOTTOMUP_MODELS
        }
    payload["sample_size"] = nowcast.sample_size
    payload["note"] = nowcast.note
    return payload


def nowcast_json(nowcast: Nowcast, thread_count: Optional[int] = None) -> str:
    """JSON текущего nowcast: UTF-8 как есть, отступ 2, LF в конце (спека §5.1)."""
    return json.dumps(
        forecast_payload(nowcast, thread_count=thread_count),
        ensure_ascii=False,
        indent=2,
    ) + "\n"


def write_nowcast_artifact(
    path: Path, nowcast: Optional[Nowcast], thread_count: Optional[int] = None
) -> bool:
    """Пишет JSON nowcast или УБИРАЕТ устаревший артефакт; `False` — писать нечего.

    Ветка «nowcast не посчитан» (нет цели, признаки не собраны, ни один метод
    не посчитан) обязана удалить файл прошлого прогона: оставленный, он был бы
    прочитан как текущий прогноз. Возврат: `True` — файл записан, `False` —
    файла нет (в том числе удалён только что).
    """
    if nowcast is None:
        path.unlink(missing_ok=True)
        return False
    path.write_text(nowcast_json(nowcast, thread_count=thread_count), encoding="utf-8")
    return True


def clear_stale_artifacts(out_dir: Path) -> list:
    """Убирает ВСЕ артефакты прошлого прогона; возвращает имена удалённых файлов.

    Только для ветки «ни одного прогноза вообще» (пустая выборка или выборка
    короче старта walk-forward): она не пишет НИЧЕГО, но файлы предыдущего
    прогона лежали бы в каталоге и читались бы как результат ЭТОГО. Ветка «нет
    цели живого nowcast'а» сюда НЕ ходит: там бэктест валиден, отчёт и CSV
    перезаписываются, а подчищается только stale-JSON
    (`write_nowcast_artifact`). Отсутствие файлов — не ошибка.
    """
    removed = []
    for name in ARTIFACT_NAMES:
        path = out_dir / name
        if path.exists():
            path.unlink()
            removed.append(name)
    return removed


# Причина раздела «Текущий nowcast», когда вызывающий не передал её
# (`render_report(nowcast=None)`): живой прогон всегда передаёт реальную.
NOWCAST_REASON_UNKNOWN = "причина не передана вызывающим (см. консоль прогона)"


def render_nowcast(nowcast: Nowcast) -> str:
    """Markdown-блок «Текущий nowcast» (спека §5.3) — раздел отчёта.

    Числа округлены до разрядов таблицы (`_fmt`): полная точность — в JSON.
    `note` печатается тем же текстом, что уходит в JSON, — пометки не могут
    разъехаться между артефактами.
    """
    lines = [
        f"**Целевой месяц: {nowcast.target_month.isoformat()[:7]}** — месячный "
        f"ИПЦ не опубликован на {nowcast.data_asof.isoformat()}; признаки "
        f"собраны as-of даты прогона (не канонического cutoff, спека §5.3).",
        "",
        "| метод | прогноз, п.п. |",
        "|---|---|",
    ]
    lines += [
        f"| {model_label(name)} | {_fmt(value)} |"
        for name, value in nowcast.models.items()
    ]
    lines += [
        f"| {model_label(name)} | {_fmt(value)} |"
        for name, value in nowcast.benchmarks.items()
    ]
    lines += [""]
    if nowcast.weights:
        cells = ", ".join(
            f"`{code}` {_fmt(weight, 4)}"
            for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        )
        lines.append(
            f"Веса bottom-up (NNLS, полная выборка): {cells} "
            f"(Σ {_fmt(sum(nowcast.weights), 4)})."
        )
    else:
        lines.append("Веса bottom-up (NNLS) не посчитаны.")
    lines += ["", f"Примечание: {nowcast.note}"]
    return "\n".join(lines)


def render_nowcast_absent(reason: str = NOWCAST_REASON_UNKNOWN) -> str:
    """Раздел отчёта, когда живого прогноза нет (спека §5.3) — один текст.

    Ветка «нет цели» бэктест не отменяет: отчёт и CSV пишутся как обычно,
    поэтому причину отсутствия прогноза называет сам раздел — тем же `reason`,
    что печатает консоль, — а stale-прогноз прошлого прогона удаляется
    (`write_nowcast_artifact`), чтобы не читался как актуальный.
    """
    return (
        f"Текущий nowcast не рассчитан: {reason}. `{FORECAST_NAME}` не пишется; "
        f"устаревший файл прогноза, если был, удалён. Отчёт и CSV посчитаны "
        f"текущим прогоном: walk-forward валиден и без живого прогноза."
    )


# ---------------------------------------------------------------------------
# Отчёт-скелет и артефакты (спека §5.1)
# ---------------------------------------------------------------------------

REPORT_NAME = "cpi_monthly_report.md"
PREDICTIONS_NAME = "cpi_monthly_predictions.csv"
FORECAST_NAME = "cpi_monthly_forecast.json"

# Артефакты прогона: отчёт, CSV фолдов и JSON текущего nowcast. Подчистка ветки
# «ни одного прогноза вообще» идёт по этому списку — прошлый прогон мог оставить
# любой из них, а дат в именах нет: файл читался бы как результат ЭТОГО прогона.
ARTIFACT_NAMES = (REPORT_NAME, PREDICTIONS_NAME, FORECAST_NAME)

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


def _fmt_p(pvalue: float) -> str:
    """p-value для таблицы: менее 1e-4 — «<0.0001» (разряды не нужны)."""
    if math.isinf(pvalue):
        return "0.0000"
    if pvalue < 0.0001:
        return "<0.0001"
    return f"{pvalue:.4f}"


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
_WEIGHTS_HEADER = (
    "| окно | модель | w(cpi_food) | w(cpi_nonfood) | w(cpi_services) | Σ |",
    "|---|---|---|---|---|---|",
)
_DM_HEADER = (
    "| окно | тип | пара | ΔRMSE, п.п. | p-value (DM-HLN) | вывод |",
    "|---|---|---|---|---|---|",
)
_BOOTSTRAP_HEADER = (
    "| окно | пара | ΔRMSE, п.п. | 95% CI | p (бутстрап) |",
    "|---|---|---|---|---|",
)
_SPLIT_HEADER = (
    "| модель | n | RMSE, п.п. | MAE, п.п. |",
    "|---|---|---|---|",
)


def _weights_rows(runs: list[WindowRun]) -> list:
    """Строки весов NNLS: последний успешный фолд каждого окна (§3.2).

    Печатаются только окна, где bottom-up дал хотя бы один фолд: веса
    вырожденного окна не определены, а переносить сюда веса соседнего окна
    нельзя (`evaluate_windows` сбрасывает диагностику перед каждым окном).
    """
    rows = []
    for run in runs:
        for name in BOTTOMUP_MODELS:
            weights = run.weights.get(name)
            if not weights:
                continue
            cells = " | ".join(_fmt(float(weight)) for weight in weights[:3])
            rows.append(
                f"| {window_title(run)} | {model_label(name)} | {cells} | "
                f"{_fmt(float(sum(weights)))} |"
            )
    return rows


def _monotone_names(metadata: dict) -> str:
    """Имена признаков с `+1` из метаданных CatBoost (для блока конфигурации)."""
    names = [
        name for name, value in metadata["monotone_constraints"].items() if value
    ]
    return ", ".join(names) if names else "—"


def _defaults_cell(metadata: dict) -> str:
    """Дефолты библиотеки строкой `имя=значение` (метаданные `defaults`)."""
    return ", ".join(
        f"`{name}={value}`" for name, value in metadata["defaults"].items()
    )


def _pair_test_rows(predictions: list[Prediction], window_cell: str) -> list:
    """Строки DM-таблицы: primary, затем secondary (§4.3), на общих месяцах."""
    rows = []
    for kind, pairs in (
        ("primary", PRIMARY_PAIRS),
        ("secondary", SECONDARY_PAIRS),
    ):
        for test in pairwise_tests(predictions, pairs):
            rows.append(
                f"| {window_cell} | {kind} | "
                f"{model_label(test.model_a)} vs {model_label(test.model_b)} | "
                f"{_fmt(test.diff)} | {_fmt_p(test.p_value)} | {test.note} |"
            )
    return rows


def _bootstrap_rows(runs: list[WindowRun]) -> list:
    """Строки блочного бутстрапа primary-пар по окнам walk-forward."""
    rows = []
    for run in runs:
        for result in bootstrap_pairs(run.predictions, PRIMARY_PAIRS):
            rows.append(
                f"| {window_title(run)} | "
                f"{model_label(result.model_a)} vs {model_label(result.model_b)} | "
                f"{_fmt(result.rmse_diff)} | "
                f"[{_fmt(result.low)}; {_fmt(result.high)}] | "
                f"{_fmt_p(result.p_value)} |"
            )
    return rows


def _split_metric_rows(predictions: list[Prediction]) -> list:
    """RMSE/MAE моделей на фиксированном test-окне статьи (§4.4)."""
    groups: dict[str, list[Prediction]] = {}
    for prediction in predictions:
        groups.setdefault(prediction.model, []).append(prediction)
    rows = []
    for name in MODELS:
        group = groups.get(name, [])
        if group:
            rows.append(
                f"| {model_label(name)} | {len(group)} | {_fmt(rmse_pp(group))} | "
                f"{_fmt(mae_pp(group))} |"
            )
        else:
            rows.append(f"| {model_label(name)} | 0 фолдов | — | — |")
    return rows


def render_report(
    rows: list[Row],
    skipped: list,
    window_runs: list[WindowRun],
    today: Optional[date] = None,
    split_predictions: Optional[list[Prediction]] = None,
    nowcast: Optional[Nowcast] = None,
    nowcast_reason: str = NOWCAST_REASON_UNKNOWN,
    sample: str = DEFAULT_SAMPLE,
    quick: bool = False,
) -> str:
    """Полный markdown-отчёт Task 4 (спека §4.3–4.5, §5.1, §5.3, §8.2).

    `rows`/`skipped` — выборка `build_sample` (строки и месяцы без собранных
    признаков); `window_runs` — по одному `WindowRun` на окно walk-forward
    (порядок окон задаёт строки таблиц, основное помечено `WindowRun.main`);
    `today` — дата прогона (по умолчанию системная); `split_predictions` —
    прогнозы `fixed_split` (вспомогательный сплит статьи); `nowcast` — живой
    прогноз или `None` (тогда раздел называет `nowcast_reason`); `sample` —
    `short`/`long`: печатается в шапке и строке выборки, а для long добавляет
    оговорку о NaN-контролях до 2013 и длинном сплите статьи. Short-таблицы и
    числа от этого не меняются. `quick=True` — экспериментальный прогон только
    основного окна (спека `2026-10-18-ml-quick-runs`): заголовок и строка
    помечают quick, robustness и фиксированный сплит статьи не считались
    (раздел сплита заменён пометкой), DM primary и живой nowcast остаются.

    Состав: выборка; «модель × окно» с отдельными строками «2022 исключён»;
    последние 12 фолдов; главный блок DM-HLN primary/secondary (p-value,
    ΔRMSE, вывод) с оговоркой о недосходе B3-mp; блочный бутстрап primary-пар;
    веса NNLS; конфигурация CatBoost; сплит статьи 2019-01…2024-07 с DM;
    живой nowcast; «что не проверено». Числа без p-value не подаются как
    доказательство: DM/бутстрап — единственные тесты значимости здесь.
    """
    sample = validate_sample(sample)
    bounds_first, bounds_last = SAMPLE_BOUNDS[sample]
    today = today or date.today()
    runs = list(window_runs)
    main_run = next((run for run in runs if run.main), runs[0] if runs else None)
    first_run = runs[0] if runs else None
    main_window = (
        f"`{window_key(first_run.start, first_run.embargo)}`"
        if first_run is not None
        else "—"
    )
    split = list(split_predictions or [])
    title = "# CPI monthly (ML-трек v2) — walk-forward, DM-HLN, сплит статьи и живой nowcast"
    if sample == "long":
        title += " (long-sample)"
    if quick:
        title += " (quick)"
    lines = [
        title,
        "",
        f"Дата прогона: {today.isoformat()}; выборка `{sample}` "
        f"({bounds_first.isoformat()[:7]} … {bounds_last.isoformat()[:7]}); "
        f"сид прогона `SEED={SEED}`. Спека: "
        "`docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md` (§2 — данные и "
        "признаки, §3.1–3.4 — CatBoost и бенчмарки, §4.1–4.5 — walk-forward, DM, "
        "сплит статьи и «2022 исключён», §5.3 — живой nowcast, §8.2 — "
        "long-sample); план: "
        "`docs/superpowers/plans/2026-10-18-cpi-monthly-mp.md`.",
        "",
        "## Выборка",
        "",
        "| состав | значение |",
        "|---|---|",
    ]
    span = (
        f"{rows[0].month.isoformat()[:7]} … {rows[-1].month.isoformat()[:7]}"
        if rows
        else "—"
    )
    skipped_months = [month for month, _ in skipped]
    nan_usd, nan_rate = nan_control_counts(rows)
    lines += [
        f"| sample | {sample} "
        f"({bounds_first.isoformat()[:7]} … {bounds_last.isoformat()[:7]}) |",
        f"| месяцев в выборке | {len(rows)} ({span}) |",
        f"| пропущено (признак не собран) | {len(skipped)}{_months_suffix(skipped_months)} |",
        f"| NaN-контроли (`usd_mm` / `key_rate`) | {nan_usd} / {nan_rate} |",
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
    ]
    if quick:
        lines += [
            "quick: только основное окно; robustness/сплит не считались. "
            "Числа quick — экспериментальные, не замена полного прогона "
            "без флагов.",
            "",
        ]
    if sample == "long":
        lines += [
            "**Long-sample (§8.2) — эксперимент на репрезентативность.** "
            f"{len(rows)} {_plural(len(rows), 'месяц', 'месяца', 'месяцев')} "
            f"{bounds_first.isoformat()[:7]} … "
            f"{bounds_last.isoformat()[:7]}: МП-месячные начинаются 2002-01, но "
            "`_chg` месяца 2002-02 требует 2001-12, а квартальные Q1-2002 "
            "доступны только 2002-05-31 — поэтому long стартует с 2002-05; "
            "`cbr_currency_usd` (2013-01) и `cbr_key_rate` (2013-09-17) "
            "начинаются позже, поэтому до глубины этих рядов `usd_mm`/`key_rate` "
            f"— NaN, а не пропуск месяца (NaN здесь: `usd_mm` — {nan_usd}, "
            f"`key_rate` — {nan_rate}). CatBoost обрабатывает NaN нативно, B3 "
            "импутирует медианой train-окна (пустая колонка → 0.0). Long-sample "
            "отвечает на вопрос «работает ли МП на длинной истории», а не "
            "«работает ли МП при полном наборе контролей все 24 года»; "
            "сравнение DM primary с short — по знаку, величине и p-value обеих "
            "выборок, без подгонки.",
            "",
        ]
    robustness_note = (
        "robustness-окна не считаются (quick)."
        if quick
        else (
            f"робастность — второй старт {ROBUST_START} и embargo "
            f"{ROBUST_EMBARGO}."
        )
    )
    lines += [
        "## Walk-forward",
        "",
        "Расширяющееся обучающее окно, переобучение на каждом фолде: тест — месяц "
        "`i`, обучение — строки `[0 … i−1−embargo]` (только прошлое; embargo > 0 "
        "выбрасывает месяц перед тестом). Единый train/test split не используется "
        f"(спека §4.1). Основное окно — {main_window}; {robustness_note} "
        "Вырожденное обучающее окно "
        "(`DegenerateWindowError` модели: короткое окно B2/B3, пустое окно или "
        "неизменный таргет компоненты CatBoost) — пропуск фолда ЭТОЙ модели со "
        "счётчиком, а не падение прогона. «Недосход» — фолды, где координатный "
        "спуск B3 не сошёлся (`ConvergenceWarning`), вырождением не считается и "
        "публикуется числом.",
        "",
        "## Модель × окно",
        "",
        "Ошибка фолда — `прогноз − факт` (п.п., м/м); `n` — число фолдов под "
        "метрикой. RMSE/MAE — общие функции `scripts/eval_utils.py`. B1 и B2 от "
        "набора признаков не зависят; B3 и CatBoost считаются в двух "
        "пространствах (без МП и с МП), чтобы отделить эффект МП от эффекта "
        "алгоритма; CatBoost дополнительно в двух схемах агрегации — direct и "
        "bottom-up (§3.2–3.3). Строки «2022 исключён» — отдельная метрика без "
        "тестовых месяцев 2022 года (§4.5), не замена основной.",
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
            if group and any(item.month.year == 2022 for item in group):
                rest = [
                    item for item in group if item.month.year != 2022
                ]
                if rest:
                    rest_rmse, rest_mae = metrics_without_2022(group)
                    metric_rows.append(
                        f"| {model_label(name)} (2022 исключён) | "
                        f"{window_title(run)} | {len(rest)} | {_fmt(rest_rmse)} | "
                        f"{_fmt(rest_mae)} | — | справочно, не замена метрики |"
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
    dm_rows = []
    for run in runs:
        dm_rows.extend(_pair_test_rows(run.predictions, window_title(run)))
    lines += [
        "## DM-HLN: с МП vs без МП",
        "",
        "Diebold–Mariano с поправкой HLN (`statsmodels`, критерий MSE), `h=1`, "
        "на ОБЩИХ тестовых месяцах пары; p двусторонний, уровень 5% "
        f"(`ALPHA={ALPHA}`) зафиксирован до результатов. "
        "ΔRMSE = RMSE(A) − RMSE(B): отрицательное значит, что A точнее. "
        "**Primary** — эффект МП (каждый `_mp`-вариант против своей базовой "
        "модели на тех же фолдах), **secondary** — контекст (`_mp` против B1 и "
        "`b3_elastic_net_mp`, direct против bottom-up). Формулировки не "
        "подгоняются: `note` — кто точнее и значима ли разница. Пара без двух "
        "общих месяцев не строится.",
        "",
    ]
    lines += _table(
        _DM_HEADER,
        dm_rows,
        empty="_(нет пар: меньше двух общих месяцев у моделей.)_",
    )
    non_converged = []
    for run in runs:
        group = _run_groups(run).get(MODEL_ENET_MP, [])
        count = run.notes.get(MODEL_ENET_MP, {}).get(NON_CONVERGED, 0)
        if group:
            non_converged.append(
                f"{window_key(run.start, run.embargo)} — {count} из {len(group)}"
            )
    lines += [""]
    if non_converged:
        lines.append(
            "Оговорка: недосход B3-mp (`ConvergenceWarning` координатного спуска) — "
            + "; ".join(non_converged)
            + ". Числа B3-mp читать с этой оговоркой; без p-value сравнение RMSE "
            "— не доказательство."
        )
    else:
        lines.append(
            "Оговорка: без p-value сравнение RMSE — не доказательство; в этом "
            "прогоне недосхода B3-mp нет."
        )
    lines += [""]
    lines += [
        "## Блочный бутстрап разницы RMSE (primary)",
        "",
        f"Ресэмплирование пар ошибок целиком: блок {BOOTSTRAP_BLOCK} месяца, "
        f"n={BOOTSTRAP_N} ресэмплов, `seed=SEED`. Интервал — 95% по порядковым "
        "статистикам; p — двусторонний по доле ресэмплов по разные стороны нуля "
        "(`eval_utils.block_bootstrap_rmse_diff`). ΔRMSE — та же разница "
        "RMSE(A) − RMSE(B), что в DM-таблице.",
        "",
    ]
    lines += _table(
        _BOOTSTRAP_HEADER,
        _bootstrap_rows(runs),
        empty="_(primary-пар с двумя общими месяцами нет.)_",
    )
    lines += [""]
    lines += [
        "## Веса NNLS (bottom-up)",
        "",
        "Веса `w` — неотрицательная регрессия (`scipy.optimize.nnls`) общего ИПЦ "
        "на метки трёх компонент ТОЛЬКО обучающего окна последнего успешного "
        "фолда окна; нормированы к 1 (§3.2). Агрегат bottom-up — `Σ w_j·ŷ_j`; "
        "вырожденные фолды весов не дают. Совпадение весов на трёх знаках у "
        "разных окон не означает переиспользование: каждое окно снимает веса "
        "со своего последнего успешного фолда, а под embargo=1 обучающий хвост "
        "на месяц короче (полные значения различаются).",
        "",
    ]
    lines += _table(
        _WEIGHTS_HEADER,
        _weights_rows(runs),
        empty="_(весов нет: bottom-up не дал ни одного фолда.)_",
    )
    lines += [""]
    base_metadata = catboost_metadata(
        with_mp=False,
        thread_count=QUICK_CATBOOST_THREAD_COUNT if quick else None,
    )
    mp_metadata = catboost_metadata(
        with_mp=True,
        thread_count=QUICK_CATBOOST_THREAD_COUNT if quick else None,
    )
    lines += [
        "## Конфигурация CatBoost",
        "",
        "Четыре варианта §3.3 (direct/bottom-up × без МП/с МП) обучаются на одних "
        "и тех же фолдах; `_mp`-варианты отличаются ТОЛЬКО набором признаков. "
        "Гиперпараметры — дефолты библиотеки, кроме перечисленных; "
        "`allow_writing_files=False` — запрет побочного каталога `catboost_info/`.",
        "",
        "| параметр | без МП | с МП |",
        "|---|---|---|",
        f"| `loss_function` | `{base_metadata['loss_function']}` | "
        f"`{mp_metadata['loss_function']}` |",
        f"| `random_seed` | `{base_metadata['random_seed']}` | "
        f"`{mp_metadata['random_seed']}` |",
        f"| `cat_features` | `{', '.join(base_metadata['cat_features'])}` | "
        f"`{', '.join(mp_metadata['cat_features'])}` |",
        f"| `allow_writing_files` | `{base_metadata['allow_writing_files']}` | "
        f"`{mp_metadata['allow_writing_files']}` |",
        f"| `monotone_constraints` `+1` | {_monotone_names(base_metadata)} | "
        f"{_monotone_names(mp_metadata)} |",
        f"| `num_features` | {', '.join(base_metadata['num_features'])} | "
        f"{', '.join(mp_metadata['num_features'])} |",
        "",
        "Дефолты библиотеки (`defaults` в `catboost_metadata`): "
        f"{_defaults_cell(base_metadata)}.",
        "",
    ]
    train_months = [
        row.month for row in rows if month_of(row.month) <= ARTICLE_TRAIN_END
    ]
    test_months = [
        row.month
        for row in rows
        if ARTICLE_TRAIN_END < month_of(row.month) <= ARTICLE_TEST_END
    ]
    if len(train_months) < ARTICLE_TRAIN_MONTHS:
        train_note = (
            f"train короче статьи ({len(train_months)} против "
            f"{ARTICLE_TRAIN_MONTHS} месяцев)"
        )
    else:
        train_note = (
            f"train длиннее статьи ({len(train_months)} против "
            f"{ARTICLE_TRAIN_MONTHS} месяцев)"
        )
    if quick:
        lines += [
            "## Сплит статьи (quick: не рассчитывался)",
            "",
            "quick: фиксированный сплит не считался — сопоставимость со статьёй "
            "даёт только полный прогон без флагов.",
            "",
        ]
    else:
        lines += [
            "## Сплит статьи (сопоставимость, не выбор модели)",
            "",
            f"Фиксированный сплит §4.4: train ≤ {ARTICLE_TRAIN_END.isoformat()[:7]} "
            f"({len(train_months)} мес.), test "
            f"{test_months[0].isoformat()[:7] if test_months else '—'} … "
            f"{test_months[-1].isoformat()[:7] if test_months else '—'} "
            f"({len(test_months)} мес.). На train обучаются все модели, на test "
            "считаются RMSE/MAE и DM для тех же primary/secondary-пар. Это "
            f"**вспомогательная таблица сопоставимости**, а не выбор модели: "
            f"{train_note}, таргет другой (несглаженный м/м "
            "с КУ, а не SA ИПЦ без КУ), выбросы не исключены — числа статьи напрямую "
            "не сравнимы.",
            "",
        ]
        lines += _table(
            _SPLIT_HEADER,
            _split_metric_rows(split) if split else [],
            empty="_(нет тестовых месяцев 2019-01…2024-07 в выборке.)_",
        )
        lines += [""]
        split_dm = _pair_test_rows(split, "split статьи")
        lines += [
            "DM на фиксированном test-окне статьи (те же пары, общие месяцы):",
            "",
        ]
        lines += _table(
            _DM_HEADER,
            split_dm,
            empty="_(нет пар на test-окне статьи.)_",
        )
        lines += [""]
    lines += [
        "## Текущий nowcast (live)",
        "",
    ]
    if nowcast is not None:
        lines += [render_nowcast(nowcast), ""]
    else:
        lines += [render_nowcast_absent(nowcast_reason), ""]
    split_bullet = (
        "- quick: robustness-окна и фиксированный сплит статьи не считались; "
        "числа quick экспериментальные, официальные — только полный прогон без "
        "--quick."
        if quick
        else (
            f"- сплит статьи — вспомогательная таблица сопоставимости: "
            f"{'train короче' if len(train_months) < ARTICLE_TRAIN_MONTHS else 'train длиннее'} "
            "статьи, таргет другой, выбросы не исключены; модель по нему не "
            "выбирается."
        )
    )
    lines += [
        "## Что не проверено",
        "",
        "- as-of — приближение, а не point-in-time слой: точные даты публикаций и "
        "ревизии рядов не учтены; PIT — отдельный пункт фазы 4.",
        "- гиперпараметры не тюнинговались: CatBoost — дефолты библиотеки (§3.1), "
        "B2 (HAC, 11 дамми, минимум 14 строк) и B3 "
        "(`ElasticNetCV(alphas=logspace(−4,1,20), l1_ratio=[0.1,0.5,0.9], "
        "cv=TimeSeriesSplit(3), random_state=SEED)`, минимум 12 строк) "
        "зафиксированы спекой §3.4 до результатов.",
        "- недосход B3 (`ConvergenceWarning` координатного спуска) считается по "
        "фолдам; его влияние на прогноз не измерено — конфигурация "
        "`ElasticNetCV` зафиксирована спекой, B3-mp недосходил чаще базового.",
        "- множественные сравнения (DM/бутстрап по парам) — без поправки на "
        "множественность; p-value — описательные, не подтверждающая статистика "
        "в смысле заранее объявленной единственной гипотезы.",
        "- embargo 1 — только робастность: перекрытие окон признаков тестового и "
        "обучающих месяцев структурно исключено purge-проверкой, но границы "
        "доступности — консервативное правило §2.6, а не фактические даты.",
        split_bullet,
        "- «2022 исключён» — описательная строка: вклад шока назван, но режимы "
        "отдельно не тестируются, блок «последние 12 фолдов» — не тест.",
        "- живой nowcast: МП может быть глубже `M−1` (пометка в `note`), признаки "
        "сняты as-of даты прогона, а не PIT; недельный трек v1 не менялся и "
        "здесь не сравнивается.",
        "- числа статьи напрямую не сравнимы: другой таргет, длина train и "
        "обработка выбросов.",
    ]
    if sample == "long":
        lines.append(
            "- long-sample: `usd_mm`/`key_rate` до глубины рядов (2013) — NaN; "
            "ранние фолды проверяют МП без этих контролей, поэтому вывод о "
            "репрезентативности не равен выводу при полном наборе контролей все "
            "24 года."
        )
    lines += [
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


def _print_pair_tests(runs: list[WindowRun]) -> None:
    """Консольный блок DM-HLN primary-пар: ΔRMSE и p-value по окнам (§4.3)."""
    print("DM-HLN (h=1; primary: с МП vs без МП):")
    for run in runs:
        tests = pairwise_tests(run.predictions, PRIMARY_PAIRS)
        if not tests:
            print(f"  {window_title(run)}: primary-пар нет")
            continue
        cells = "; ".join(
            f"{model_label(test.model_a)} vs {model_label(test.model_b)}: "
            f"ΔRMSE {test.diff:+.3f}, p={_fmt_p(test.p_value)}"
            for test in tests
        )
        print(f"  {window_title(run)}: {cells}")


def _print_split(
    split_predictions: list[Prediction], sample: list[Row]
) -> None:
    """Консольный блок сплита статьи: границы, RMSE моделей и DM primary (§4.4)."""
    train = [
        row for row in sample if month_of(row.month) <= ARTICLE_TRAIN_END
    ]
    test = [
        row
        for row in sample
        if ARTICLE_TRAIN_END < month_of(row.month) <= ARTICLE_TEST_END
    ]
    print("сплит статьи (сопоставимость, не выбор модели):")
    if not train or not test:
        print(
            f"  train {len(train)} мес., test {len(test)} мес. — сплит не определён"
        )
        return
    groups: dict[str, list[Prediction]] = {}
    for prediction in split_predictions:
        groups.setdefault(prediction.model, []).append(prediction)
    cells = ", ".join(
        f"{model_label(name)} {_fmt(rmse_pp(groups[name]))}"
        for name in MODELS
        if name in groups
    )
    print(
        f"  train ≤ {ARTICLE_TRAIN_END.isoformat()[:7]} ({len(train)} мес.), "
        f"test {test[0].month.isoformat()[:7]} … {test[-1].month.isoformat()[:7]} "
        f"({len(test)} мес.); RMSE: {cells}"
    )
    tests = pairwise_tests(split_predictions, PRIMARY_PAIRS)
    if tests:
        print(
            "  DM primary на test: "
            + "; ".join(
                f"{model_label(test.model_a)} vs {model_label(test.model_b)}: "
                f"ΔRMSE {test.diff:+.3f}, p={_fmt_p(test.p_value)}"
                for test in tests
            )
        )


def _print_nowcast(nowcast: Nowcast) -> None:
    """Консольный блок живого nowcast: модели, бенчмарки и веса (§5.3)."""
    for name, value in (*nowcast.models.items(), *nowcast.benchmarks.items()):
        print(f"    {model_label(name):<34} {_fmt(value, 4)} п.п.")
    if nowcast.weights:
        cells = ", ".join(
            f"{code} {_fmt(weight, 4)}"
            for code, weight in zip(BOTTOMUP_COMPONENTS, nowcast.weights)
        )
        print(
            f"    {'веса NNLS (полная выборка)':<34} {cells}, "
            f"Σ {_fmt(sum(nowcast.weights), 3)}"
        )
    print(f"    {'note':<34} {nowcast.note}")


def _print_removed_artifacts(removed: list) -> None:
    """Строка консоли об удалённых артефактах прошлого прогона (пусто — нет строки)."""
    if removed:
        print(f"  устаревшие артефакты прошлого прогона удалены: {', '.join(removed)}")


def run_summary(
    start: Optional[int] = None,
    embargo_months: int = DEFAULT_EMBARGO,
    out_dir: Optional[Path] = None,
    today: Optional[date] = None,
    sample: str = DEFAULT_SAMPLE,
    quick: bool = False,
    jobs: int = 0,
    ml_log: Optional[bool] = None,
    logger: Optional[object] = None,
    run_name: Optional[str] = None,
) -> int:
    """Полный прогон Task 4: выборка, walk-forward, DM, сплит, nowcast, артефакты.

    `sample` — `short` (155 месяцев 2013-10…2026-08, дефолт) или `long`
    (292 месяца 2002-05…2026-08, §8.2); границы берёт `build_sample`, а
    NaN-контроли `usd_mm`/`key_rate` печатаются числом. `start=None` — «не
    задан»: short стартует с 24, long — с 48 (`resolve_start`); явное число
    переопределяет sample-дефолт. `embargo_months` — эмбарго основного окна;
    робастные окна (второй старт 48 и эмбарго 1) добавляет `window_specs`
    (у long основное окно 48/0 и робастность 48/1 — дубликат отбрасывается).

    `quick=True` — быстрый экспериментальный режим (спека
    `2026-10-18-ml-quick-runs`): `window_specs` сужается до основного окна,
    `fixed_split` не вызывается (`split_predictions=None`), отчёт и `ML_LOG`
    помечают quick. Модели, признаки, seed и полный путь без флага не меняются.

    `jobs` — число процессов пула фолдов (только с `quick`): `0/1` —
    последовательно, `N>1` — `ProcessPoolExecutor(max_workers=min(N,
    cpu_count()))` со `spawn`; в quick CatBoost строится с `thread_count=1`
    (и последовательно, и в воркерах). `jobs>1` без quick — ошибка: полный
    прогон не параллелится и остаётся байт-в-байт прежним. `ML_LOG` пишет
    `config.jobs`, а `run_name` при `jobs>1` получает сегмент `jobsN`.

    Загрузка рядов, `build_sample` и `assert_months_purged` — контракт Task 1;
    walk-forward считает бенчмарки B1–B3 и четыре CatBoost-варианта (`MODELS`)
    на одних фолдах; отчёт (`cpi_monthly_report.md`: «модель × окно», DM-HLN
    primary/secondary, блочный бутстрап, веса NNLS, конфигурация CatBoost,
    сплит статьи, «2022 исключён», живой nowcast) и CSV прогнозов
    (`cpi_monthly_predictions.csv`) пишутся в `out_dir` (по умолчанию корень
    worktree; файлы gitignored). JSON текущего nowcast
    (`cpi_monthly_forecast.json`, §5.1) пишется на полной выборке; при
    отсутствии цели stale-JSON удаляется, а отчёт/CSV остаются.

    Ветка «walk-forward не дал ни одного прогноза» (пустая/короткая выборка)
    ничего не пишет и подчищает ВСЕ три артефакта прошлого прогона. Прогон не
    падает ни в одной из веток. По умолчанию скрипт только читает ClickHouse и
    кладёт локальные артефакты; журнал эксперимента — опт-ин.

    `ml_log`/`logger`/`run_name` — журнал эксперимента
    (`scripts/ml_experiment.py`): при `ml_log=True` (или `ML_LOG=1`, если
    параметр не задан) после `build_sample` создаётся прогон `cpi_monthly` с
    паспортом, fold-предсказаниями в п.п. и артефактами; явный `logger`
    используется как есть (duck typing), и `ml_log` тогда игнорируется. Сбой
    журнала не меняет код возврата и не портит артефакты: отчёт и CSV остаются
    основным результатом.

    `today` — дата прогона (по умолчанию системная): она же дата среза живого
    nowcast и `forecast_date` JSON; резолвится один раз на прогон.
    """
    sample = validate_sample(sample)
    start = resolve_start(sample, start)
    if jobs < 0:
        raise ValueError(f"--jobs {jobs}: отрицательное число процессов")
    if jobs > 1 and not quick:
        raise ValueError("--jobs N>1 требует --quick: полный прогон не параллелится")
    out_dir = Path(out_dir) if out_dir is not None else ARTIFACTS_DIR
    today = today or date.today()
    targets = load_targets()
    survey = load_survey()
    usd = load_usd()
    key_rate = load_key_rate()
    fao = load_fao()
    rows, skipped = build_sample(
        targets, survey, usd, key_rate, fao, sample=sample
    )
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
    # Явный `logger` — контракт duck typing: используется как есть, даже если
    # `ML_LOG` выключен (иначе тестам и вызывающему коду пришлось бы трогать
    # env). `ML_LOG` решает только, создавать ли прогон самостоятельно.
    if logger is None and ml_log:
        # Журнал создаётся ПОСЛЕ выборки: пустая/короткая выборка — тоже прогон,
        # и её паспорт должен нести фактические rows/first/last/skipped.
        logger = mlx.start_run(
            experiment="cpi_monthly",
            run_name=(
                os.environ.get("ML_RUN_NAME")
                or run_name
                or f"cpi_monthly__{sample}__"
                f"{'quick__' if quick else ''}"
                f"{f'jobs{jobs}__' if jobs > 1 else ''}"
                f"start{start}__embargo{embargo_months}"
            ),
            config={
                "sample": sample,
                "sample_first": SAMPLE_BOUNDS[sample][0].isoformat(),
                "sample_last": SAMPLE_BOUNDS[sample][1].isoformat(),
                "start": start,
                "embargo_months": embargo_months,
                "quick": quick,
                # Число процессов пула фолдов (0/1 — последовательно). В полном
                # режиме всегда 0: `--jobs N>1` без quick отвергается выше.
                "jobs": jobs,
                # Окна прогона: основное и робастность, ровно те, что считает
                # `evaluate_windows` (спека §4.1/§8.2), — по ним паспорт
                # воспроизводим без чтения кода. В quick — только основное.
                "windows": [
                    list(spec)
                    for spec in window_specs(start, embargo_months, quick)
                ],
                "seed": SEED,
                "models": list(MODELS),
                "features_base": feature_names(with_mp=False),
                "features_mp": feature_names(with_mp=True),
                "target_series": TARGET_SERIES,
                "article_split": {
                    "train_end": ARTICLE_TRAIN_END.isoformat(),
                    "test_end": ARTICLE_TEST_END.isoformat(),
                },
                "nan_policy": (
                    "usd_mm/key_rate NaN before availability; "
                    "CatBoost native; B3 train-median"
                ),
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
            spec_path="docs/superpowers/specs/2026-10-18-cpi-monthly-mp-design.md",
            enabled=True,
        )

    survey_points = sorted(
        point for points in survey.values() for point in points
    )
    print(
        "CPI monthly (ML-трек v2) — walk-forward, DM-HLN, сплит статьи и nowcast"
        + (" (quick)" if quick else "")
    )
    bounds_first, bounds_last = SAMPLE_BOUNDS[sample]
    _field(
        "sample:",
        f"{sample} ({bounds_first.isoformat()[:7]} … {bounds_last.isoformat()[:7]})",
    )
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
    nan_usd, nan_rate = nan_control_counts(rows)
    _field("NaN usd_mm / key_rate:", f"{nan_usd} / {nan_rate}")
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
    if quick:
        _field(
            "--quick:",
            "только основное окно; robustness и фиксированный сплит статьи "
            "не считаются",
        )
        if jobs:
            _field(
                "--jobs:",
                f"{jobs} (пул фолдов, CatBoost thread_count="
                f"{QUICK_CATBOOST_THREAD_COUNT})",
            )

    try:
        runs = evaluate_windows(
            rows,
            MODELS,
            window_specs(start, embargo_months, quick),
            quick=quick,
            jobs=jobs,
        )
        _print_walk_forward(runs)
        if not any(run.predictions for run in runs):
            # Ветка «ничего не посчитано вообще»: пустая/короткая выборка или все
            # модели вырождены. У этого прогона нет ни одного прогноза, поэтому
            # не пишется ничего, а три артефакта прошлого прогона удаляются.
            _field(
                "отчёт/CSV/JSON:",
                "не пишутся (walk-forward не дал ни одного прогноза)",
            )
            _print_removed_artifacts(clear_stale_artifacts(out_dir))
            if logger is not None:
                # Журнал не меняет код возврата: сбой `finish` — предупреждение.
                try:
                    logger.finish("empty", "walk-forward не дал ни одного прогноза")
                except Exception as finish_exc:
                    log.warning(
                        "не удалось пометить прогон empty: %s: %s",
                        type(finish_exc).__name__, finish_exc,
                    )
            return 0
        _print_pair_tests(runs)
        if quick:
            # Спека quick §2.1: фиксированный сплит статьи не считается —
            # сопоставимость со статьёй даёт только полный прогон.
            split_predictions = None
            _field("сплит статьи:", "quick — не рассчитывался")
        else:
            split_predictions = fixed_split(rows, MODELS)
            _print_split(split_predictions, rows)
        nowcast, nowcast_reason = current_nowcast(
            rows,
            targets,
            survey,
            usd,
            key_rate,
            fao,
            today,
            thread_count=QUICK_CATBOOST_THREAD_COUNT if quick else None,
        )
        if nowcast is None:
            _field("текущий nowcast:", f"не рассчитан — {nowcast_reason}")
        else:
            _field(
                "текущий nowcast:",
                f"{nowcast.target_month.isoformat()[:7]} — as-of "
                f"{nowcast.data_asof.isoformat()}",
            )
            _print_nowcast(nowcast)
        report = render_report(
            rows,
            skipped,
            runs,
            today=today,
            split_predictions=split_predictions,
            nowcast=nowcast,
            nowcast_reason=nowcast_reason,
            sample=sample,
            quick=quick,
        )
        report_path, predictions_path = write_artifacts(out_dir, report, runs)
        forecast_path = out_dir / FORECAST_NAME
        forecast_written = write_nowcast_artifact(
            forecast_path,
            nowcast,
            thread_count=QUICK_CATBOOST_THREAD_COUNT if quick else None,
        )
        _field("отчёт:", str(report_path))
        _field("прогнозы (CSV):", str(predictions_path))
        if forecast_written:
            _field("прогноз (JSON):", str(forecast_path))
        else:
            _field(
                "прогноз (JSON):",
                f"не пишется ({nowcast_reason}); устаревший файл, если был, удалён",
            )
        if logger is not None:
            # Журнал эксперимента — побочный слой: его сбой НЕ меняет код
            # возврата. Артефакты к этому моменту уже записаны, поэтому
            # ValueError `PredictionRow` (например, на NaN-прогнозе) не должен
            # превращать успешный прогон в non-zero exit: помечаем run failed
            # и идём дальше.
            try:
                # Веса NNLS — по окнам и моделям; окна без весов не попадают,
                # поэтому у прогона без bottom-up моделей словарь пуст (`{}`).
                weights_by_window = {}
                for run in runs:
                    if run.weights:
                        weights_by_window[window_key(run.start, run.embargo)] = {
                            name: dict(zip(BOTTOMUP_COMPONENTS, values))
                            for name, values in run.weights.items()
                        }
                # Сплит статьи — вспомогательная метрика сопоставимости, не
                # выбор модели; модели без фолдов сплита в словарь не входят.
                # В quick `fixed_split` не вызывался: article_split — null
                # (спека quick §2.1), а не пустая таблица.
                if split_predictions is None:
                    article_split = None
                else:
                    split_groups: dict[str, list[Prediction]] = {}
                    for prediction in split_predictions:
                        split_groups.setdefault(prediction.model, []).append(prediction)
                    article_split = {
                        name: {
                            "rmse": rmse_pp(group),
                            "mae": mae_pp(group),
                            "n": len(group),
                        }
                        for name, group in split_groups.items()
                    }
                logger.log_metrics(
                    {
                        "primary_metric": "rmse_pp",
                        "sample": sample,
                        # Спека §8.3: общие ключи паспорта — `models` и
                        # `windows`; форма окон та же, что у v1.
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
                        "weights": weights_by_window,
                        "article_split": article_split,
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
                                "sample": sample,
                            },
                        )
                        for run in runs
                        for prediction in run.predictions
                    ]
                )
                logger.log_artifacts(
                    [
                        report_path,
                        predictions_path,
                        forecast_path if forecast_written else None,
                    ]
                )
                logger.finish(
                    "finished",
                    f"walk-forward: {len(runs)} окон, "
                    f"{sum(len(run.predictions) for run in runs)} фолдов; "
                    f"nowcast: {'рассчитан' if nowcast is not None else 'не рассчитан'}",
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
        description="CPI monthly (ML-трек v2): выборка, as-of, признаки, "
                    "walk-forward, бенчмарки B1–B3 и CatBoost, DM-HLN, сплит "
                    "статьи, живой nowcast, метрики и отчёт."
    )
    parser.add_argument(
        "--export-fixtures",
        metavar="DIR",
        default=None,
        help="записать CSV-фикстуры офлайн-тестов в каталог DIR и выйти",
    )
    parser.add_argument(
        "--sample",
        choices=SAMPLE_NAMES,
        default=DEFAULT_SAMPLE,
        help="выборка: short — 155 месяцев 2013-10…2026-08 (по умолчанию), "
             "long — 292 месяца 2002-05…2026-08 (эксперимент §8.2; "
             "NaN-контроли до 2013)",
    )
    parser.add_argument(
        "--start",
        type=int,
        metavar="N",
        default=None,
        help=f"минимальное обучающее окно walk-forward в месяцах (по умолчанию "
             f"{DEFAULT_START} для short и {DEFAULT_LONG_START} для long; явное "
             f"значение переопределяет sample-дефолт); робастное окно старта "
             f"{ROBUST_START} считается в том же прогоне (спека §4.1)",
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
    parser.add_argument(
        "--quick",
        action="store_true",
        help="быстрый экспериментальный режим: только основное окно walk-forward, "
             "без robustness-окон и фиксированного сплита статьи; числа quick "
             "не официальные и не заменяют полный прогон без флага (спека "
             "2026-10-18-ml-quick-runs)",
    )
    parser.add_argument(
        "--jobs",
        type=int,
        metavar="N",
        default=0,
        help="число процессов пула фолдов: 0/1 — последовательно, N>1 — только "
             "с --quick (ProcessPoolExecutor по фолдам, CatBoost "
             "thread_count=1); без --quick N>1 отвергается, полный прогон не "
             "параллелится (спека 2026-10-18-ml-quick-runs §2.2)",
    )
    args = parser.parse_args(argv)
    if args.jobs < 0:
        parser.error(f"--jobs {args.jobs}: число процессов не может быть отрицательным")
    if args.jobs > 1 and not args.quick:
        parser.error("--jobs N>1 требует --quick: полный прогон не параллелится")
    if args.export_fixtures:
        return export_fixtures(Path(args.export_fixtures))
    return run_summary(
        start=args.start,
        embargo_months=args.embargo_months,
        sample=args.sample,
        quick=args.quick,
        jobs=args.jobs,
    )


if __name__ == "__main__":
    sys.exit(main())
