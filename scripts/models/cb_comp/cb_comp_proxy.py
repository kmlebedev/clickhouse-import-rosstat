#!/usr/bin/env python3
"""Прокси-таргет CB_COMP (SA ИПЦ без КУ) из публичных рядов и оценка nowcast.

Пункт `key-rate/phase-4-ml-cb-comp-parity` (issue #190), **прокси-путь** — не
строгое воспроизведение статьи Кошкина–Крылова (2025), а робастность: собрать
приближение таргета «SA ИПЦ без КУ, накопленный прирост h = 1/2/3/6» из
публичных рядов, посчитать на нём RMSE/MAE нашего месячного nowcast на окне
статьи 2019-01…2024-07 и сравнить агрегатные числа с опубликованными RMSE
CB_COMP. Спека — `docs/superpowers/specs/2026-10-18-cb-comp-proxy-design.md`.

Что читает:

* `cbr_indicators_cpd` — SA ИПЦ м/м, ряд «Все товары и услуги» (агрегат с КУ);
* `cbr_dip_bulletin` — SAAR-компоненты ЦБ (`saar_all_goods`, `saar_food`,
  `saar_nonfood`, `saar_services`), последняя редакция каждого месяца;
* `rosstat_ipc_mes` — 4 NSA-ряда ИПЦ: веса (NNLS) и контроль компонент.

Прокси-таргет — тождество `π_ex = (π − w_u·u)/(1 − w_u)`: если `π` — агрегат
SA ИПЦ, `u` — SA ЖКУ, `w_u` — постоянная доля ЖКУ, то исключение ЖКУ из
агрегата не требует компонент. Компоненты food/nonfood/services после
подстановки сокращаются; они используются для проверки агрегата и оценки
весов. Публичного месячного ряда ЖКУ за 2019–2021 в БД нет (недельные средние
цены начинаются 2022-01-10), поэтому `u` — постоянный рост (4 %/год). После
калибровки постоянный `u` неидентифицируем: он входит в таргет константой и
поглощается свободным членом. Следствие названо в отчёте: прокси — это
масштабированный SA-агрегат, а не строгий таргет статьи.

Наш nowcast — `cpi_monthly_predictions.csv` месячного трека v2 (long-sample,
`--sample long`, окно `start=48`, PR #177): `cpi_nowcast.py` покрывает только
2022+, а окно статьи — 2019–2024. `y_pred` — nowcast NSA общего ИПЦ, поэтому
для каждого h на train 2009-01…2018-12 оценивается прямой перенос

    cum_h(t) = a_h + b_h·nowcast_t + c_h·π_ex_{t−1} + ε_t,

где `cum_h(t) = Σ_{j=0}^{h−1} π_ex_{t+j}`. Кумулятивная цель train-строки не
выходит за train (иначе test затекал бы в обучение). Гиперпараметров нет.

Оценка: test-origin 2019-01…2024-07, кумулятивное окно целиком внутри test;
исключения статьи — 2014-12…2015-03, 2020-04, весь 2022 (наблюдение выпадает,
если исключён любой месяц окна). RMSE/MAE в п.п., блочный бутстрап 95 % CI
нашего RMSE (блок 3, n = 2000, seed = 20261018). DM/TOST против CB_COMP **не
считаются**: у нас нет помесячных ошибок авторов, опубликованы только
агрегатные RMSE; сравнение — отношением и абсолютной разницей.

**Скрипт ничего не пишет в ClickHouse** — только читает. Транспорт — `ch_query`
из `scripts/models/polyus/aisc.py`; секреты приходят из окружения (Makefile).
Запуск:

    make cb-comp-proxy ARGS="--nowcast-csv /path/to/cpi_monthly_predictions.csv"

Артефакты (`--out-dir`, по умолчанию корень worktree, gitignored):
`cb_comp_proxy_results.json`, `cb_comp_proxy_metrics.csv`,
`cb_comp_proxy_report.md`.
"""

import argparse
import csv
import json
import logging
import math
import random
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Iterable, Optional

import numpy as np
from scipy.optimize import nnls

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu
from lib import clickhouse as ch

ch_query = ch.ch_query

log = logging.getLogger(__name__)

# Сид бутстрапа — тот же, что у месячного трека v2 (дата ветки).
SEED = 20261018

# Имена рядов пинуются тестами: переименование карточки в БД обязано ломать
# набор, а не молча обнулять выборку.
SA_CPI_SERIES = "Все товары и услуги"
SAAR_SERIES = ("saar_all_goods", "saar_food", "saar_nonfood", "saar_services")
ROSSTAT_SERIES = {
    "cpi_all": "Индексы потребительских цен на товары и услуги по Российской Федерации",
    "cpi_food": "Индексы потребительских цен на продовольственные товары по Российской Федерации",
    "cpi_nonfood": "Индексы потребительских цен на непродовольственные товары по Российской Федерации",
    "cpi_services": "Индексы потребительских цен на услуги по Российской Федерации",
}
ROSSTAT_COMPONENTS = ("cpi_food", "cpi_nonfood", "cpi_services")

# Четыре CatBoost-варианта месячного трека v2: direct/bottom-up × без МП/с МП.
CATBOOST_MODELS = (
    "catboost_direct",
    "catboost_bottomup",
    "catboost_direct_mp",
    "catboost_bottomup_mp",
)
CONTEXT_MODELS = ("b3_elastic_net", "b3_elastic_net_mp")
MODEL_LABELS = {
    "catboost_direct": "CatBoost direct (без МП)",
    "catboost_bottomup": "CatBoost bottom-up (без МП)",
    "catboost_direct_mp": "CatBoost direct (с МП)",
    "catboost_bottomup_mp": "CatBoost bottom-up (с МП)",
    "b3_elastic_net": "B3 ElasticNetCV (без МП)",
    "b3_elastic_net_mp": "B3 ElasticNetCV (с МП)",
    "proxy_b1_last_value": "B1 last value (на прокси)",
    "proxy_ar1": "AR(1) (на прокси)",
    "proxy_seasonal_mean": "Сезонное среднее (на прокси)",
}

# Опубликованные RMSE CB_COMP, %, SA ИПЦ без КУ (таблица статьи; см.
# docs/research/2026-10-16-cb-comp-vs-cpi-nowcast.md).
ARTICLE_RMSE = {
    "with_mp": {1: 0.16, 2: 0.30, 3: 0.44, 6: 0.97},
    "without_mp": {1: 0.20, 2: 0.39, 3: 0.59, 6: 1.05},
}
ARTICLE_RW_D = {1: 0.22, 2: 0.39, 3: 0.60, 6: 2.11}
# Margin паритета из issue #190 — фиксирован ДО прогона. В прокси-пути он
# применяется только описательно: DM/TOST без помесячных ошибок CB_COMP
# невозможны.
PARITY_MARGIN = 0.10

# Исключения статьи (включительно по месяцам). 2014-12…2015-03 в тест не
# попадает, но границы объявлены, чтобы тест их пинул.
ARTICLE_EXCLUSIONS = (
    (date(2014, 12, 1), date(2015, 3, 1)),
    (date(2020, 4, 1), date(2020, 4, 1)),
    (date(2022, 1, 1), date(2022, 12, 1)),
)

DEFAULT_TRAIN_START = date(2009, 1, 1)
DEFAULT_TRAIN_END = date(2018, 12, 1)
DEFAULT_TEST_START = date(2019, 1, 1)
DEFAULT_TEST_END = date(2024, 7, 1)
DEFAULT_WINDOW = "start=48"
MIN_WEIGHT_MONTHS = 12
MIN_TRAIN_ROWS = 24
MIN_HISTORY_MONTHS = 24
DEFAULT_UTILITIES_WEIGHT = 0.10
DEFAULT_UTILITIES_GROWTH = 4.0


class CbCompProxyError(ValueError):
    """Ошибка данных/конфигурации прокси-пути (не транспортная)."""


class DegenerateFitError(CbCompProxyError):
    """Недостаточно строк или вырожденная матрица прямого переноса."""


# ---------------------------------------------------------------------------
# Календарь и разбор
# ---------------------------------------------------------------------------


def month_of(day: date) -> date:
    """Первый день месяца точки."""
    return day.replace(day=1)


def month_shift(day: date, months: int) -> date:
    """Сдвиг на целое число месяцев (первое число результата)."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def month_end(day: date) -> date:
    """Последний день месяца точки."""
    return month_shift(month_of(day), 1) - timedelta(days=1)


def month_range(start: date, end: date) -> list:
    """Список первых чисел месяцев `start..end` включительно."""
    first, last = month_of(start), month_of(end)
    if first > last:
        raise CbCompProxyError(f"пустой диапазон месяцев: {first} > {last}")
    months = []
    current = first
    while current <= last:
        months.append(current)
        current = month_shift(current, 1)
    return months


def _as_date(value, what: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    try:
        return date.fromisoformat(text[:10])
    except ValueError as exc:
        raise CbCompProxyError(f"{what}: не дата {value!r}") from exc


def _as_float(value, what: str) -> float:
    if isinstance(value, bool):
        raise CbCompProxyError(f"{what}: bool — не число")
    if isinstance(value, (int, float)):
        result = float(value)
    else:
        try:
            result = float(str(value).strip())
        except ValueError as exc:
            raise CbCompProxyError(f"{what}: не число {value!r}") from exc
    if not math.isfinite(result):
        raise CbCompProxyError(f"{what}: не конечное число {value!r}")
    return result


def _sql_literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _ch(sql: str) -> list:
    """ClickHouse-запрос с JSON-форматом; тесты подменяют `ch_query`."""
    return ch_query(sql.rstrip().rstrip(";") + "\nFORMAT JSON")


def _points(rows: list, value_key: str = "value", what: str = "ряд") -> dict:
    """Строки `{month, value}` → отсортированный словарь месяц → значение."""
    points = {}
    for row in rows:
        month = month_of(_as_date(row["month"], f"{what}: month"))
        value = _as_float(row[value_key], f"{what}: value")
        if month in points:
            raise CbCompProxyError(f"{what}: две точки на {month}")
        points[month] = value
    if not points:
        raise CbCompProxyError(f"{what}: пустой ряд")
    return dict(sorted(points.items()))


def saar_to_mm(value: float) -> float:
    """SAAR (% в годовом выражении) → м/м, %: `(1+saar/100)^(1/12) − 1`."""
    value = _as_float(value, "SAAR")
    if value <= -100.0:
        raise CbCompProxyError(f"SAAR {value}: годовой темп ≤ −100 %")
    return ((1.0 + value / 100.0) ** (1.0 / 12.0) - 1.0) * 100.0


def utilities_mm(annual_growth: float) -> float:
    """Постоянный годовой рост тарифа → м/м, % (компаундирование)."""
    growth = _as_float(annual_growth, "utilities_annual_growth")
    if growth <= -100.0:
        raise CbCompProxyError(f"рост ЖКУ {growth}: ≤ −100 %/год")
    return ((1.0 + growth / 100.0) ** (1.0 / 12.0) - 1.0) * 100.0


# ---------------------------------------------------------------------------
# Загрузка рядов
# ---------------------------------------------------------------------------


def load_sa_cpi() -> dict:
    """SA ИПЦ м/м, %: `cbr_indicators_cpd`, ряд «Все товары и услуги»."""
    sql = f"""
        SELECT toStartOfMonth(date) AS month,
               argMax(toFloat64(value), date) AS value
        FROM cbr_indicators_cpd FINAL
        WHERE name = {_sql_literal(SA_CPI_SERIES)}
        GROUP BY month
        ORDER BY month
    """
    return _points(_ch(sql), what=f"cbr_indicators_cpd[{SA_CPI_SERIES}]")


def load_saar() -> dict:
    """SAAR-компоненты ЦБ (последняя редакция месяца), % годовых."""
    names = ", ".join(_sql_literal(name) for name in SAAR_SERIES)
    sql = f"""
        SELECT series,
               toStartOfMonth(date) AS month,
               toFloat64(argMax(value, (publication_date, release))) AS value
        FROM cbr_dip_bulletin FINAL
        WHERE series IN ({names})
        GROUP BY series, month
        ORDER BY series, month
    """
    grouped = {}
    for row in _ch(sql):
        series = str(row["series"])
        grouped.setdefault(series, []).append(row)
    missing = [name for name in SAAR_SERIES if name not in grouped]
    if missing:
        raise CbCompProxyError(f"cbr_dip_bulletin: нет рядов {missing}")
    result = {
        name: _points(grouped[name], what=f"cbr_dip_bulletin[{name}]")
        for name in SAAR_SERIES
    }
    return result


def load_rosstat() -> dict:
    """4 NSA-ряда ИПЦ Росстата, п.п. м/м (`percent − 100`)."""
    names = ", ".join(_sql_literal(name) for name in ROSSTAT_SERIES.values())
    sql = f"""
        SELECT name AS series,
               toStartOfMonth(date) AS month,
               argMax(toFloat64(percent) - 100, date) AS value
        FROM rosstat_ipc_mes FINAL
        WHERE name IN ({names})
        GROUP BY series, month
        ORDER BY series, month
    """
    grouped = {}
    for row in _ch(sql):
        series = str(row["series"])
        grouped.setdefault(series, []).append(row)
    result = {}
    reverse = {name: code for code, name in ROSSTAT_SERIES.items()}
    for series, rows in grouped.items():
        code = reverse.get(series)
        if code is None:
            continue
        result[code] = _points(rows, what=f"rosstat_ipc_mes[{series}]")
    missing = [code for code in ROSSTAT_SERIES if code not in result]
    if missing:
        raise CbCompProxyError(f"rosstat_ipc_mes: нет рядов {missing}")
    return result


# ---------------------------------------------------------------------------
# Веса и прокси-таргет
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class WeightFit:
    """NNLS-веса компонент ИПЦ и качество восстановления агрегата."""

    weights: dict
    raw_sum: float
    rmse: float
    months: int


def nnls_weights(rosstat: dict, start: date, end: date) -> WeightFit:
    """Неотрицательные веса компонент, нормированные к 1, только на train."""
    months = []
    for month in month_range(start, end):
        values = [rosstat.get(code, {}).get(month) for code in ("cpi_all",) + ROSSTAT_COMPONENTS]
        if all(value is not None for value in values):
            months.append((month, values))
    if len(months) < MIN_WEIGHT_MONTHS:
        raise CbCompProxyError(
            f"NNLS: {len(months)} общих месяцев < {MIN_WEIGHT_MONTHS} на {start}…{end}"
        )
    matrix = np.asarray([values[1:] for _, values in months], dtype=float)
    target = np.asarray([values[0] for _, values in months], dtype=float)
    solution, _ = nnls(matrix, target)
    raw_sum = float(solution.sum())
    if raw_sum <= 0.0:
        raise CbCompProxyError("NNLS: сумма весов ≤ 0")
    normalized = solution / raw_sum
    residual = matrix @ normalized - target
    rmse = float(math.sqrt(float((residual * residual).mean())))
    weights = {
        code: float(weight)
        for code, weight in zip(ROSSTAT_COMPONENTS, normalized)
    }
    return WeightFit(weights=weights, raw_sum=raw_sum, rmse=rmse, months=len(months))


def proxy_target(sa_cpi: dict, utilities_weight: float, utilities_growth: float) -> dict:
    """`π_ex = (π − w_u·u)/(1 − w_u)` для каждого месяца."""
    weight = _as_float(utilities_weight, "utilities_weight")
    if not 0.0 <= weight < 1.0:
        raise CbCompProxyError(f"вес ЖКУ {weight} вне [0, 1)")
    u_mm = utilities_mm(utilities_growth)
    scale = 1.0 / (1.0 - weight)
    return {month: (value - weight * u_mm) * scale for month, value in sa_cpi.items()}


def scale_factor(utilities_weight: float) -> float:
    return 1.0 / (1.0 - _as_float(utilities_weight, "utilities_weight"))


def cumulative(points: dict, month: date, h: int) -> Optional[float]:
    """Сумма `month..month+h−1`; None, если хотя бы одного месяца нет."""
    if h < 1:
        raise CbCompProxyError(f"горизонт h={h} < 1")
    total = 0.0
    for offset in range(h):
        value = points.get(month_shift(month, offset))
        if value is None:
            return None
        total += value
    return total


def article_excluded(month: date) -> bool:
    """Месяц входит в выбросы статьи (2014-15, 2020-04, 2022)."""
    month = month_of(month)
    return any(start <= month <= end for start, end in ARTICLE_EXCLUSIONS)


def window_excluded(months: Iterable[date]) -> bool:
    """Хотя бы один месяц кумулятивного окна — выброс статьи."""
    return any(article_excluded(month) for month in months)


def weighted_components(saar_mm: dict, weights: dict) -> dict:
    """Взвешенная сумма SAAR-компонент (м/м) по месяцам, где есть все три."""
    result = {}
    for month in saar_mm["saar_food"]:
        values = []
        ok = True
        for code in ("saar_food", "saar_nonfood", "saar_services"):
            value = saar_mm.get(code, {}).get(month)
            if value is None:
                ok = False
                break
            values.append(value)
        if not ok:
            continue
        result[month] = sum(
            weights[code] * value
            for code, value in zip(ROSSTAT_COMPONENTS, values)
        )
    return result


def saar_weights(saar_mm: dict) -> WeightFit:
    """Веса из SAAR-компонент (контроль согласованности, не таргет)."""
    months = []
    for month, all_value in saar_mm["saar_all_goods"].items():
        components = [saar_mm.get(code, {}).get(month) for code in ("saar_food", "saar_nonfood", "saar_services")]
        if all(value is not None for value in components):
            months.append((month, [all_value] + components))
    if len(months) < MIN_WEIGHT_MONTHS:
        raise CbCompProxyError("SAAR-веса: слишком мало общих месяцев")
    matrix = np.asarray([values[1:] for _, values in months], dtype=float)
    target = np.asarray([values[0] for _, values in months], dtype=float)
    solution, _ = nnls(matrix, target)
    raw_sum = float(solution.sum())
    if raw_sum <= 0:
        raise CbCompProxyError("SAAR-веса: сумма ≤ 0")
    normalized = solution / raw_sum
    residual = matrix @ normalized - target
    return WeightFit(
        weights={code: float(weight) for code, weight in zip(ROSSTAT_COMPONENTS, normalized)},
        raw_sum=raw_sum,
        rmse=float(math.sqrt(float((residual * residual).mean()))),
        months=len(months),
    )


def aligned_rmse(left: dict, right: dict) -> tuple:
    """RMSE/макс/n по общим месяцам двух рядов."""
    common = sorted(set(left) & set(right))
    if not common:
        raise CbCompProxyError("нет общих месяцев для сверки рядов")
    diff = np.asarray([left[month] - right[month] for month in common], dtype=float)
    return (
        float(math.sqrt(float((diff * diff).mean()))),
        float(np.abs(diff).max()),
        len(common),
    )


# ---------------------------------------------------------------------------
# Nowcast из cpi_monthly_predictions.csv
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Nowcast:
    """Прогнозы месячного трека: model → month → y_pred."""

    path: Path
    window: str
    values: dict
    months: dict
    source_rows: int


def load_nowcast(path: Path, window: str = DEFAULT_WINDOW) -> Nowcast:
    """Читает CSV `month,window,model,y_true,y_pred`; дубли — ошибка."""
    path = Path(path)
    if not path.exists():
        raise CbCompProxyError(
            f"нет CSV nowcast: {path}. Он создаётся `make cpi-monthly "
            f'ARGS="--sample long"` в ветке feat/key-rate-ml-v2 (PR #177); '
            f"путь можно задать `--nowcast-csv`."
        )
    rows = []
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"month", "window", "model", "y_true", "y_pred"}
        missing = required - set(reader.fieldnames or ())
        if missing:
            raise CbCompProxyError(f"{path}: нет колонок {sorted(missing)}")
        for line, row in enumerate(reader, start=2):
            if str(row["window"]).strip() != window:
                continue
            rows.append((line, row))
    if not rows:
        with path.open("r", encoding="utf-8", newline="") as handle:
            windows = sorted({str(row["window"]).strip() for row in csv.DictReader(handle)})
        raise CbCompProxyError(
            f"{path}: окно {window!r} не найдено; доступные: {windows}"
        )
    values = {}
    months = {}
    for line, row in rows:
        model = str(row["model"]).strip()
        if not model:
            raise CbCompProxyError(f"{path}:{line}: пустое имя модели")
        month = month_of(_as_date(row["month"], f"{path}:{line}: month"))
        value = _as_float(row["y_pred"], f"{path}:{line}: y_pred")
        per_model = values.setdefault(model, {})
        if month in per_model:
            raise CbCompProxyError(f"{path}:{line}: дубль {model} / {month}")
        per_model[month] = value
    for model in values:
        months[model] = sorted(values[model])
    return Nowcast(path=path, window=window, values=values, months=months, source_rows=len(rows))


# ---------------------------------------------------------------------------
# Прямой h-step перенос и метрики
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class DirectFit:
    """OLS `cum_h ~ 1 + nowcast_t + π_ex_{t−1}`."""

    h: int
    intercept: float
    nowcast_coef: float
    lag_coef: float
    rows: int


def fit_direct(nowcast: dict, proxy: dict, h: int, train_start: date, train_end: date) -> DirectFit:
    """Оценивает прямой перенос ТОЛЬКО на train; цель не выходит за train."""
    if h < 1:
        raise CbCompProxyError(f"горизонт h={h} < 1")
    design = []
    target = []
    for month in month_range(train_start, train_end):
        last = month_shift(month, h - 1)
        if last > month_of(train_end):
            continue
        nowcast_value = nowcast.get(month)
        lag_value = proxy.get(month_shift(month, -1))
        if nowcast_value is None or lag_value is None:
            continue
        total = cumulative(proxy, month, h)
        if total is None:
            continue
        design.append((1.0, nowcast_value, lag_value))
        target.append(total)
    if len(design) < MIN_TRAIN_ROWS:
        raise DegenerateFitError(
            f"h={h}: {len(design)} train-строк < {MIN_TRAIN_ROWS}"
        )
    matrix = np.asarray(design, dtype=float)
    vector = np.asarray(target, dtype=float)
    coefficients, _, rank, _ = np.linalg.lstsq(matrix, vector, rcond=None)
    if rank < matrix.shape[1]:
        raise DegenerateFitError(f"h={h}: вырожденная матрица train (rank={rank})")
    return DirectFit(
        h=h,
        intercept=float(coefficients[0]),
        nowcast_coef=float(coefficients[1]),
        lag_coef=float(coefficients[2]),
        rows=len(design),
    )


def predict_direct(fit: DirectFit, nowcast_value: float, lag_value: float) -> float:
    return fit.intercept + fit.nowcast_coef * nowcast_value + fit.lag_coef * lag_value


def bootstrap_rmse_ci(errors: list, block: int = 3, n: int = 2000, seed: int = SEED) -> tuple:
    """95 % CI RMSE одной серии: блочный бутстрап с переносом блоков по кругу."""
    if not errors:
        raise CbCompProxyError("бутстрап: пустой список ошибок")
    if block < 1:
        raise CbCompProxyError(f"бутстрап: блок {block} < 1")
    if n < 1:
        raise CbCompProxyError(f"бутстрап: ресэмплов {n} < 1")
    size = len(errors)
    blocks = -(-size // block)
    generator = random.Random(seed)
    values = []
    for _ in range(n):
        indices = []
        for _ in range(blocks):
            start = generator.randrange(size)
            indices.extend((start + offset) % size for offset in range(block))
        del indices[size:]
        squared = sum(errors[index] * errors[index] for index in indices)
        values.append(math.sqrt(squared / size))
    values.sort()
    return (
        values[int(0.025 * n)],
        values[min(n - 1, math.ceil(0.975 * n) - 1)],
    )


@dataclass(frozen=True)
class Metrics:
    """RMSE/MAE модели на горизонте h; errors — для диагностики, не в JSON."""

    model: str
    h: int
    n: int
    rmse: float
    mae: float
    n_all: int
    rmse_all: float
    mae_all: float
    ci_low: float
    ci_high: float
    errors: tuple = field(repr=False, compare=False, default=())


def _errors_metrics(model: str, h: int, errors: list, all_errors: list) -> Metrics:
    if not errors:
        raise CbCompProxyError(f"{model} h={h}: нет наблюдений после исключений")
    low, high = bootstrap_rmse_ci(errors)
    return Metrics(
        model=model,
        h=h,
        n=len(errors),
        rmse=eu.rmse(errors),
        mae=eu.mae(errors),
        n_all=len(all_errors),
        rmse_all=eu.rmse(all_errors),
        mae_all=eu.mae(all_errors),
        ci_low=low,
        ci_high=high,
        errors=tuple(errors),
    )


def evaluate_model(
    model: str,
    nowcast: dict,
    proxy: dict,
    h: int,
    fit: DirectFit,
    test_start: date,
    test_end: date,
) -> Metrics:
    """Ошибки прямого переноса на test-месяцах; окно целиком внутри test."""
    errors = []
    all_errors = []
    for month in month_range(test_start, test_end):
        last = month_shift(month, h - 1)
        if last > month_of(test_end):
            continue
        nowcast_value = nowcast.get(month)
        lag_value = proxy.get(month_shift(month, -1))
        if nowcast_value is None or lag_value is None:
            continue
        months = [month_shift(month, offset) for offset in range(h)]
        if any(value not in proxy for value in months):
            continue
        truth = cumulative(proxy, month, h)
        prediction = predict_direct(fit, nowcast_value, lag_value)
        error = prediction - truth
        all_errors.append(error)
        if not window_excluded(months):
            errors.append(error)
    return _errors_metrics(model, h, errors, all_errors)


def evaluate_benchmark(name: str, proxy: dict, h: int, test_start: date, test_end: date) -> Metrics:
    """Бенчмарки на самом прокси-таргете: B1, AR(1), сезонное среднее."""
    errors = []
    all_errors = []
    for month in month_range(test_start, test_end):
        last = month_shift(month, h - 1)
        if last > month_of(test_end):
            continue
        months = [month_shift(month, offset) for offset in range(h)]
        if any(value not in proxy for value in months):
            continue
        history = {key: value for key, value in proxy.items() if key < month}
        if len(history) < MIN_HISTORY_MONTHS:
            continue
        ordered = sorted(history.items())
        last_value = ordered[-1][1]
        if name == "proxy_b1_last_value":
            prediction = h * last_value
        elif name == "proxy_ar1":
            previous = np.asarray([value for _, value in ordered[:-1]], dtype=float)
            current = np.asarray([value for _, value in ordered[1:]], dtype=float)
            matrix = np.column_stack([np.ones(len(previous)), previous])
            coefficients, _, rank, _ = np.linalg.lstsq(matrix, current, rcond=None)
            if rank < 2:
                continue
            intercept, slope = coefficients
            prediction = 0.0
            value = last_value
            for _ in range(h):
                value = intercept + slope * value
                prediction += value
        elif name == "proxy_seasonal_mean":
            means = {}
            for key, value in history.items():
                means.setdefault(key.month, []).append(value)
            prediction = 0.0
            for offset in range(h):
                month_values = means.get(month_shift(month, offset).month, [])
                if not month_values:
                    prediction = float("nan")
                    break
                prediction += sum(month_values) / len(month_values)
            if not math.isfinite(prediction):
                continue
        else:
            raise CbCompProxyError(f"неизвестный бенчмарк {name!r}")
        truth = cumulative(proxy, month, h)
        error = prediction - truth
        all_errors.append(error)
        if not window_excluded(months):
            errors.append(error)
    return _errors_metrics(name, h, errors, all_errors)


# ---------------------------------------------------------------------------
# Сравнение со статьёй
# ---------------------------------------------------------------------------


def best_catboost(metrics: list, h: int, with_mp: bool) -> Optional[Metrics]:
    candidates = [
        metric
        for metric in metrics
        if metric.h == h
        and metric.model in CATBOOST_MODELS
        and metric.model.endswith("_mp") == with_mp
    ]
    if not candidates:
        return None
    return min(candidates, key=lambda metric: metric.rmse)


def comparison_rows(metrics: list) -> list:
    rows = []
    for h in (1, 2, 3, 6):
        base = best_catboost(metrics, h, with_mp=False)
        mp = best_catboost(metrics, h, with_mp=True)
        row = {"h": h}
        for key, metric in (("without_mp", base), ("with_mp", mp)):
            published = ARTICLE_RMSE[key][h]
            if metric is None:
                row[key] = None
                continue
            ratio = metric.rmse / published
            row[key] = {
                "model": metric.model,
                "rmse": metric.rmse,
                "published": published,
                "ratio": ratio,
                "diff": metric.rmse - published,
                "within_margin": abs(ratio - 1.0) <= PARITY_MARGIN,
            }
        rows.append(row)
    return rows


def verdict(rows: list) -> str:
    if not rows:
        return "Нет метрик: все окна вырождены; вывод не делается."
    outside = []
    for row in rows:
        for key in ("without_mp", "with_mp"):
            if row[key] is not None and not row[key]["within_margin"]:
                outside.append(f"h={row['h']} {key} (×{row[key]['ratio']:.2f})")
    if outside:
        return (
            "Не паритет: точка RMSE вне margin ±10 % от опубликованной CB_COMP — "
            + ", ".join(outside)
            + ". DM/TOST не считались: помесячных ошибок CB_COMP нет."
        )
    return (
        "Точки RMSE внутри margin ±10 %, но это НЕ паритет: DM/TOST без помесячных "
        "ошибок CB_COMP не считались."
    )


# ---------------------------------------------------------------------------
# Чувствительность
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SensitivityRow:
    kind: str
    model: str
    h: int
    value: float
    rmse: float
    n: int


def sensitivity(
    model: str,
    nowcast: dict,
    sa_cpi: dict,
    train_start: date,
    train_end: date,
    test_start: date,
    test_end: date,
) -> list:
    """Чувствительность к весу ЖКУ, росту тарифа и окну калибровки."""
    rows = []
    # Вес ЖКУ — единственное допущение, которое меняет масштаб таргета;
    # постоянный рост тарифа после калибровки неидентифицируем, поэтому
    # сетка не декартова: рост проверяется только при центральном весе.
    variants = [(weight, DEFAULT_UTILITIES_GROWTH) for weight in (0.05, 0.07, 0.10, 0.13, 0.15)]
    variants += [(DEFAULT_UTILITIES_WEIGHT, growth) for growth in (0.0, 4.0, 8.0)]
    seen = set()
    for weight, growth in variants:
        if (weight, growth) in seen:
            continue
        seen.add((weight, growth))
        proxy = proxy_target(sa_cpi, weight, growth)
        for h in (1, 2, 3, 6):
            try:
                fit = fit_direct(nowcast, proxy, h, train_start, train_end)
                metric = evaluate_model(
                    model, nowcast, proxy, h, fit, test_start, test_end
                )
            except CbCompProxyError:
                continue
            rows.append(
                SensitivityRow(
                    kind=f"weight={weight:.2f},growth={growth:.0f}",
                    model=model,
                    h=h,
                    value=weight,
                    rmse=metric.rmse,
                    n=metric.n,
                )
            )
    for alt_start in (date(2014, 1, 1),):
        proxy = proxy_target(sa_cpi, DEFAULT_UTILITIES_WEIGHT, DEFAULT_UTILITIES_GROWTH)
        for h in (1, 2, 3, 6):
            try:
                fit = fit_direct(nowcast, proxy, h, alt_start, train_end)
                metric = evaluate_model(model, nowcast, proxy, h, fit, test_start, test_end)
            except CbCompProxyError:
                continue
            rows.append(
                SensitivityRow(
                    kind=f"train_start={alt_start.isoformat()}",
                    model=model,
                    h=h,
                    value=float(alt_start.year),
                    rmse=metric.rmse,
                    n=metric.n,
                )
            )
    return rows


# ---------------------------------------------------------------------------
# Отчёт и артефакты
# ---------------------------------------------------------------------------


def _fmt(value: Optional[float], places: int = 3) -> str:
    if value is None:
        return "—"
    return f"{value:.{places}f}"


def _ranges(points: dict) -> str:
    months = sorted(points)
    if not months:
        return "_(пусто)_"
    return f"{months[0].isoformat()} … {months[-1].isoformat()} ({len(months)})"


def _table(header: tuple, rows: list) -> list:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    for row in rows:
        lines.append("| " + " | ".join(str(cell) for cell in row) + " |")
    return lines


def render_report(result: dict) -> str:
    """Markdown-отчёт прогона (его же можно положить в docs/research)."""
    lines = [
        "# CB_COMP proxy — прогон",
        "",
        f"Дата прогона: {result['run_date']}; сид `{result['seed']}`; "
        f"пункт `key-rate/phase-4-ml-cb-comp-parity` (issue #190), прокси-путь.",
        "",
        "## Данные",
        "",
        f"- `cbr_indicators_cpd` «{SA_CPI_SERIES}»: {result['data']['sa_cpi']}",
        "- `cbr_dip_bulletin` SAAR (последняя редакция месяца): "
        + "; ".join(
            f"`{name}` {result['data']['saar'][name]}" for name in SAAR_SERIES
        ),
        f"- `rosstat_ipc_mes`: {result['data']['rosstat']}",
        f"- nowcast: `{result['nowcast']['path']}`, окно `{result['nowcast']['window']}`, "
        f"строк {result['nowcast']['rows']}; месяцы "
        f"{result['nowcast']['months']}",
        "",
        "## Прокси-таргет",
        "",
        "`π_ex = (π − w_u·u)/(1 − w_u)`, где `π` — SA ИПЦ м/м с КУ, "
        "`u` — SA ЖКУ м/м.",
        "",
        f"- вес ЖКУ `w_u = {result['target']['utilities_weight']:.2f}`; "
        f"постоянный рост тарифа `{result['target']['utilities_growth']:.1f} %/год` "
        f"(`u = {result['target']['utilities_mm']:.4f} %/мес`);",
        f"- масштаб `1/(1−w_u) = {result['target']['scale']:.3f}`;",
        f"- веса компонент (NNLS, train {result['target']['weights_train_period']}): "
        + ", ".join(
            f"`{code}` {result['target']['weights'][code]:.4f}"
            for code in ROSSTAT_COMPONENTS
        )
        + f"; RMSE восстановления агрегата {result['target']['weights_rmse']:.4f} п.п.;",
        f"- контроль SAAR-весов: "
        + ", ".join(
            f"`{code}` {result['target']['saar_weights'][code]:.4f}"
            for code in ROSSTAT_COMPONENTS
        )
        + f" (RMSE {result['target']['saar_weights_rmse']:.4f});",
        f"- сверка агрегата: SA ИПЦ ЦБ vs `saar_all_goods`→м/м — RMSE "
        f"{result['diagnostics']['sa_cpi_vs_saar_rmse']:.4f} п.п., max "
        f"{result['diagnostics']['sa_cpi_vs_saar_max']:.4f}, n = "
        f"{result['diagnostics']['sa_cpi_vs_saar_n']};",
        f"- проверка тождества компонент: RMSE "
        f"{result['diagnostics']['component_identity_rmse']:.4f} п.п., max "
        f"{result['diagnostics']['component_identity_max']:.4f}, n = "
        f"{result['diagnostics']['component_identity_n']}.",
        "",
        "## Метод",
        "",
        f"Прямой перенос на train {result['method']['train_start']}…"
        f"{result['method']['train_end']}: "
        "`cum_h(t) = a_h + b_h·nowcast_t + c_h·π_ex_{t−1}`; "
        "цель train-строки не выходит за train. Test-origin "
        f"{result['method']['test_start']}…{result['method']['test_end']}, "
        "кумулятивное окно целиком внутри test; исключения статьи — "
        "2014-12…2015-03, 2020-04, весь 2022.",
        "",
        "## RMSE/MAE по горизонтам",
        "",
    ]
    header = ("Модель", "h", "n", "RMSE", "MAE", "95 % CI RMSE", "RMSE без исключений", "n без искл.")
    rows = []
    for metric in result["metrics"]:
        rows.append(
            (
                MODEL_LABELS.get(metric["model"], metric["model"]),
                metric["h"],
                metric["n"],
                _fmt(metric["rmse"]),
                _fmt(metric["mae"]),
                f"[{_fmt(metric['ci_low'])}; {_fmt(metric['ci_high'])}]",
                _fmt(metric["rmse_all"]),
                metric["n_all"],
            )
        )
    lines.extend(_table(header, rows))
    lines.extend(["", "## Сравнение с опубликованными CB_COMP", ""])
    header = (
        "h",
        "наш без МП (модель, RMSE)",
        "CB_COMP без МП",
        "отношение",
        "наш с МП (модель, RMSE)",
        "CB_COMP с МП",
        "отношение",
        "margin ±10 %",
    )
    rows = []
    for row in result["comparison"]:
        base = row["without_mp"]
        mp = row["with_mp"]
        rows.append(
            (
                row["h"],
                f"{MODEL_LABELS.get(base['model'], base['model'])}: {_fmt(base['rmse'])}"
                if base
                else "—",
                _fmt(ARTICLE_RMSE["without_mp"][row["h"]]),
                f"×{base['ratio']:.2f}" if base else "—",
                f"{MODEL_LABELS.get(mp['model'], mp['model'])}: {_fmt(mp['rmse'])}"
                if mp
                else "—",
                _fmt(ARTICLE_RMSE["with_mp"][row["h"]]),
                f"×{mp['ratio']:.2f}" if mp else "—",
                ("да" if base and base["within_margin"] else "нет")
                + " / "
                + ("да" if mp and mp["within_margin"] else "нет"),
            )
        )
    lines.extend(_table(header, rows))
    lines.extend(
        [
            "",
            f"**Вывод:** {result['verdict']}",
            "",
            "## Бенчмарки на прокси-таргете",
            "",
        ]
    )
    header = ("Бенчмарк", "h", "n", "RMSE", "MAE")
    rows = [
        (
            MODEL_LABELS.get(metric["model"], metric["model"]),
            metric["h"],
            metric["n"],
            _fmt(metric["rmse"]),
            _fmt(metric["mae"]),
        )
        for metric in result["benchmarks"]
    ]
    lines.extend(_table(header, rows))
    lines.extend(["", "## Чувствительность", ""])
    header = ("Вариант", "Модель", "h", "RMSE", "n")
    rows = [
        (
            row["kind"],
            MODEL_LABELS.get(row["model"], row["model"]),
            row["h"],
            _fmt(row["rmse"]),
            row["n"],
        )
        for row in result["sensitivity"]
    ]
    lines.extend(_table(header, rows))
    lines.extend(
        [
            "",
            "## Почему прокси не равен строгому таргету CB_COMP",
            "",
            "1. Публичного месячного ряда ЖКУ за 2019–2021 в БД нет; постоянные вес и "
            "рост тарифа не воспроизводят административные шоки. После калибровки "
            "постоянный рост неидентифицируем, а прокси — масштабированный SA-агрегат.",
            "2. Нет X-13 SA, весов СПР с лагом год и 88 отраслевых групп ЦБ.",
            "3. Наш nowcast обучен на NSA общем ИПЦ, а не на SA ИПЦ без КУ; перенос — "
            "линейная калибровка, а не обучение на таргете.",
            "4. `cbr_dip_bulletin` читается последней редакцией, а не винтажом на дату; "
            "PIT-слоя нет.",
            "5. DM/TOST против CB_COMP невозможны: опубликованы только агрегатные RMSE, "
            "помесячных ошибок авторов нет. Сравнение отношений — описательное.",
            "",
            "## Что не проверено",
            "",
            "- Строгий таргет и прогнозы CB_COMP (архив figshare) — не скачивались.",
            "- Влияние ревизий `cbr_dip_bulletin` на прокси не измерялось.",
            "- Прокси-путь не является основанием для включения ML-трека в ансамбль.",
            "",
        ]
    )
    return "\n".join(lines)


def _json_safe(value):
    if isinstance(value, dict):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(item) for item in value]
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return value
    return value


def write_artifacts(out_dir: Path, result: dict, report: str) -> dict:
    """Пишет JSON, CSV метрик и markdown-отчёт; возвращает пути."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "cb_comp_proxy_results.json"
    csv_path = out_dir / "cb_comp_proxy_metrics.csv"
    report_path = out_dir / "cb_comp_proxy_report.md"
    json_path.write_text(
        json.dumps(_json_safe(result), ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            ("model", "h", "n", "rmse", "mae", "n_all", "rmse_all", "mae_all", "ci_low", "ci_high")
        )
        for metric in result["metrics"]:
            writer.writerow(
                (
                    metric["model"],
                    metric["h"],
                    metric["n"],
                    f"{metric['rmse']:.6f}",
                    f"{metric['mae']:.6f}",
                    metric["n_all"],
                    f"{metric['rmse_all']:.6f}",
                    f"{metric['mae_all']:.6f}",
                    f"{metric['ci_low']:.6f}",
                    f"{metric['ci_high']:.6f}",
                )
            )
    report_path.write_text(report + "\n", encoding="utf-8")
    return {"json": str(json_path), "csv": str(csv_path), "report": str(report_path)}


# ---------------------------------------------------------------------------
# Сборка прогона
# ---------------------------------------------------------------------------


def build_result(
    sa_cpi: dict,
    saar: dict,
    rosstat: dict,
    nowcast: Nowcast,
    *,
    utilities_weight: float = DEFAULT_UTILITIES_WEIGHT,
    utilities_growth: float = DEFAULT_UTILITIES_GROWTH,
    train_start: date = DEFAULT_TRAIN_START,
    train_end: date = DEFAULT_TRAIN_END,
    test_start: date = DEFAULT_TEST_START,
    test_end: date = DEFAULT_TEST_END,
    today: Optional[date] = None,
) -> dict:
    """Считает прокси-таргет, калибровку, метрики, сравнение и чувствительность."""
    if not train_start < test_start:
        raise CbCompProxyError("train_start обязан быть раньше test_start")
    if train_end >= test_start:
        raise CbCompProxyError("train_end обязан быть раньше test_start")

    saar_mm = {
        code: {month: saar_to_mm(value) for month, value in points.items()}
        for code, points in saar.items()
    }
    proxy = proxy_target(sa_cpi, utilities_weight, utilities_growth)
    weights_fit = nnls_weights(rosstat, train_start, train_end)
    saar_weights_fit = saar_weights(saar_mm)

    sa_cpi_vs_saar = aligned_rmse(
        sa_cpi,
        {month: value for month, value in saar_mm["saar_all_goods"].items()},
    )
    component_identity = aligned_rmse(
        {month: value for month, value in saar_mm["saar_all_goods"].items()},
        weighted_components(saar_mm, weights_fit.weights),
    )

    metrics = []
    fits = {}
    for model in CATBOOST_MODELS + CONTEXT_MODELS:
        values = nowcast.values.get(model)
        if not values:
            log.warning("nowcast: модель %s отсутствует в CSV — пропуск", model)
            continue
        for h in (1, 2, 3, 6):
            try:
                fit = fit_direct(values, proxy, h, train_start, train_end)
                metric = evaluate_model(model, values, proxy, h, fit, test_start, test_end)
            except DegenerateFitError as exc:
                log.warning("nowcast: %s — %s", model, exc)
                continue
            metrics.append(metric)
            fits[(model, h)] = fit

    benchmarks = []
    for name in ("proxy_b1_last_value", "proxy_ar1", "proxy_seasonal_mean"):
        for h in (1, 2, 3, 6):
            try:
                benchmarks.append(evaluate_benchmark(name, proxy, h, test_start, test_end))
            except DegenerateFitError as exc:
                log.warning("бенчмарк %s: %s", name, exc)

    comparison = comparison_rows(metrics)
    sens = []
    for model in ("catboost_bottomup", "catboost_bottomup_mp"):
        if model not in nowcast.values:
            continue
        sens.extend(
            sensitivity(
                model,
                nowcast.values[model],
                sa_cpi,
                train_start,
                train_end,
                test_start,
                test_end,
            )
        )

    result = {
        "run_date": (today or date.today()).isoformat(),
        "seed": SEED,
        "data": {
            "sa_cpi": _ranges(sa_cpi),
            "saar": {name: _ranges(points) for name, points in saar.items()},
            "rosstat": "; ".join(
                f"`{code}` {_ranges(points)}" for code, points in rosstat.items()
            ),
        },
        "nowcast": {
            "path": str(nowcast.path),
            "window": nowcast.window,
            "rows": nowcast.source_rows,
            "months": (
                f"{min(min(points) for points in nowcast.months.values()).isoformat()} … "
                f"{max(max(points) for points in nowcast.months.values()).isoformat()}"
            ),
            "models": sorted(nowcast.values),
        },
        "target": {
            "utilities_weight": float(utilities_weight),
            "utilities_growth": float(utilities_growth),
            "utilities_mm": utilities_mm(utilities_growth),
            "scale": scale_factor(utilities_weight),
            "weights_train_period": f"{train_start}…{train_end}",
            "weights": weights_fit.weights,
            "weights_raw_sum": weights_fit.raw_sum,
            "weights_rmse": weights_fit.rmse,
            "weights_months": weights_fit.months,
            "saar_weights": saar_weights_fit.weights,
            "saar_weights_raw_sum": saar_weights_fit.raw_sum,
            "saar_weights_rmse": saar_weights_fit.rmse,
            "saar_weights_months": saar_weights_fit.months,
        },
        "diagnostics": {
            "sa_cpi_vs_saar_rmse": sa_cpi_vs_saar[0],
            "sa_cpi_vs_saar_max": sa_cpi_vs_saar[1],
            "sa_cpi_vs_saar_n": sa_cpi_vs_saar[2],
            "component_identity_rmse": component_identity[0],
            "component_identity_max": component_identity[1],
            "component_identity_n": component_identity[2],
        },
        "method": {
            "train_start": train_start.isoformat(),
            "train_end": train_end.isoformat(),
            "test_start": test_start.isoformat(),
            "test_end": test_end.isoformat(),
            "fit": "cum_h = a + b*nowcast_t + c*proxy_{t-1}; OLS на train",
            "exclusions": [
                f"{start.isoformat()}…{end.isoformat()}" for start, end in ARTICLE_EXCLUSIONS
            ],
        },
        "fits": [
            {
                "model": model,
                "h": h,
                "intercept": fit.intercept,
                "nowcast_coef": fit.nowcast_coef,
                "lag_coef": fit.lag_coef,
                "rows": fit.rows,
            }
            for (model, h), fit in sorted(fits.items())
        ],
        "metrics": [
            {
                "model": metric.model,
                "h": metric.h,
                "n": metric.n,
                "rmse": metric.rmse,
                "mae": metric.mae,
                "n_all": metric.n_all,
                "rmse_all": metric.rmse_all,
                "mae_all": metric.mae_all,
                "ci_low": metric.ci_low,
                "ci_high": metric.ci_high,
            }
            for metric in metrics
        ],
        "benchmarks": [
            {
                "model": metric.model,
                "h": metric.h,
                "n": metric.n,
                "rmse": metric.rmse,
                "mae": metric.mae,
                "rmse_all": metric.rmse_all,
            }
            for metric in benchmarks
        ],
        "comparison": comparison,
        "verdict": verdict(comparison),
        "sensitivity": [
            {
                "kind": row.kind,
                "model": row.model,
                "h": row.h,
                "value": row.value,
                "rmse": row.rmse,
                "n": row.n,
            }
            for row in sens
        ],
        "article": {
            "rmse_with_mp": ARTICLE_RMSE["with_mp"],
            "rmse_without_mp": ARTICLE_RMSE["without_mp"],
            "rw_d": ARTICLE_RW_D,
            "margin": PARITY_MARGIN,
        },
    }
    return result


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_date(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"не дата: {text!r}") from exc


def parse_args(argv: Optional[list] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Прокси-таргет CB_COMP (SA ИПЦ без КУ) и RMSE нашего nowcast."
    )
    parser.add_argument(
        "--nowcast-csv",
        type=Path,
        default=ROOT / "cpi_monthly_predictions.csv",
        help="CSV месячного трека v2 (month,window,model,y_true,y_pred)",
    )
    parser.add_argument("--window", default=DEFAULT_WINDOW)
    parser.add_argument("--utilities-weight", type=float, default=DEFAULT_UTILITIES_WEIGHT)
    parser.add_argument(
        "--utilities-annual-growth", type=float, default=DEFAULT_UTILITIES_GROWTH
    )
    parser.add_argument("--train-start", type=_parse_date, default=DEFAULT_TRAIN_START)
    parser.add_argument("--train-end", type=_parse_date, default=DEFAULT_TRAIN_END)
    parser.add_argument("--test-start", type=_parse_date, default=DEFAULT_TEST_START)
    parser.add_argument("--test-end", type=_parse_date, default=DEFAULT_TEST_END)
    parser.add_argument("--out-dir", type=Path, default=ROOT)
    parser.add_argument(
        "--no-write",
        action="store_true",
        help="не писать JSON/CSV/MD (только печать отчёта)",
    )
    return parser.parse_args(argv)


def main(argv: Optional[list] = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        log.info("читаю ClickHouse: cbr_indicators_cpd, cbr_dip_bulletin, rosstat_ipc_mes")
        sa_cpi = load_sa_cpi()
        saar = load_saar()
        rosstat = load_rosstat()
        log.info(
            "ряды: SA ИПЦ %s…%s; SAAR %s…%s; Rosstat %s…%s",
            min(sa_cpi),
            max(sa_cpi),
            min(saar["saar_all_goods"]),
            max(saar["saar_all_goods"]),
            min(rosstat["cpi_all"]),
            max(rosstat["cpi_all"]),
        )
        nowcast = load_nowcast(args.nowcast_csv, args.window)
        log.info(
            "nowcast: %s, окно %s, модели %s",
            args.nowcast_csv,
            args.window,
            ", ".join(sorted(nowcast.values)),
        )
        result = build_result(
            sa_cpi,
            saar,
            rosstat,
            nowcast,
            utilities_weight=args.utilities_weight,
            utilities_growth=args.utilities_annual_growth,
            train_start=args.train_start,
            train_end=args.train_end,
            test_start=args.test_start,
            test_end=args.test_end,
        )
        report = render_report(result)
        print(report)
        if not args.no_write:
            paths = write_artifacts(args.out_dir, result, report)
            log.info("записано: %s", ", ".join(paths.values()))
        return 0
    except (CbCompProxyError, FileNotFoundError, ch.ClickHouseError) as exc:
        log.error("cb_comp_proxy: %s", exc)
        return 2


if __name__ == "__main__":
    sys.exit(main())
