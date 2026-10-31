#!/usr/bin/env python3
"""Core-прокси ИПЦ из недельных компонент 2021+ и короткие модели.

Пункт `key-rate/phase-4-weekly-core-components` (issue #364), роль research.
Вопрос: можно ли из взвешенного core-субиндекса недельной компонентной панели
(веса CB_COMP, предрегистрированные исключения) собрать core-подобный месячный
ряд на 2021+ и пройти DM-тест против `Базовый ИПЦ` (`v_cbr_macro`) моделями для
коротких рядов.

Метод предрегистрирован в постановке issue #364 и не меняется по результатам:

* core-субиндекс месяца — взвешенное среднее `mm_pp` по строкам вне исключений
  с непустым `mm_pp` и `weight_used > 0`; месяц пропускается при < 30 рядах;
  годовое выражение `(1 + mm/100)^12 − 1`;
* baseline B0 — официальный агрегат «Индекс потребительских цен (недельная
  оценка)», 4-нед. средняя и `align_last` (формула research #348);
* M1 `core_raw` — сам core-субиндекс (совмещённая оценка, без SA);
* M2 `core_fourier` — OLS месячного `mm` на 3 Fourier-гармоники, dummy
  тарифных месяцев и January dummy, прогноз 1 шаг;
* M3 `core_ucm` — `UnobservedComponents(level='local level', seasonal=12,
  stochastic_level=True, stochastic_seasonal=True)`, прогноз 1 шаг;
* M4 `core_ridge` — ridge-бридж на годовую цель: core-субиндекс, агрегат B0,
  лаги 1–3 core и Fourier-гармоники; expanding StandardScaler + RidgeCV по
  сетке alpha [0.1, 1, 10] с expanding-валидацией внутри train;
* M5 `core_seasonal_naive` — robustness: `mm` того же месяца год назад.

Окна и min train 36 месяцев: тест 2024-01…2026-09 для старта 2021-01,
2025-01…2026-09 для старта 2022-01; для 2021+ дополнительно ранний срез
2021-01…2024-12 (тест 2024-01…2024-12). DM — HLN, `h=1`, квадратичные потери
(`lib.eval_utils.diebold_mariano`). Все p-values выводятся с поправкой
Bonferroni-оговоркой: 0,05 / (число моделей × число окон); при p выше порога
значимость не заявляется.

Скрипт только читает ClickHouse (`v_rosstat_macro`, `v_cbr_macro`) и CSV-панель
из `docs/research/`; в БД не пишет. Артефакт по умолчанию —
`docs/research/2026-10-22-weekly-core-components.json`. Запуск —
`make weekly-core-components`, тесты — `make weekly-core-components-test`.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import subprocess
import sys
import warnings
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import statsmodels
from statsmodels.tsa.statespace.structural import UnobservedComponents

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu
from weekly_core_proxy import (
    HEADLINE_SQL,
    CORE_SQL,
    align_last,
    evaluate_basis,
    four_week_average,
    load_aggregate_history,
    load_monthly,
    load_weekly,
    metrics,
    months_between,
    subset,
)

PANEL_PATH = ROOT / "docs" / "research" / "2026-10-19-cpi-weekly-component-panel-products.csv"
DEFAULT_OUT = ROOT / "docs" / "research" / "2026-10-22-weekly-core-components.json"

SPEC_VERSION = "1.0"

WEEKLY_AGGREGATE = "Индекс потребительских цен (недельная оценка)"
CORE_SERIES = "Базовый ИПЦ"
HEADLINE_SERIES = "Индексы потребительских цен на товары и услуги по Российской Федерации"

# Предрегистрированные исключения core-субиндекса (issue #364). Список
# зафиксирован до результатов и в JSON не меняется.
EXCLUSIONS = (
    "Плодоовощная продукция, включая картофель",
    "Яйца",
    "Сахар",
    "Алкогольные напитки",
    "Топливо моторное",
    "Табачные изделия",
    "Легковые автомобили",
    "Жилищные и коммунальные услуги (включая аренду квартир)",
    "Услуги пассажирского транспорта",
    "Услуги в сфере зарубежного туризма",
    "Экскурсионные услуги",
    "Санаторно-оздоровительные услуги",
    "Услуги гостиниц и прочих мест проживания",
    "Средства связи",
)

# Сверка 2026-10-31: декабрь 2022 — перенос индексации 2023 года; в июле 2023
# индексации не было.
TARIFF_MONTHS = ("2022-07", "2022-12", "2024-07", "2025-07", "2026-10")
FOURIER_HARMONICS = 3
MIN_ITEMS = 30
DEFAULT_MIN_TRAIN = 36
DEFAULT_START = "2021-01"
DEFAULT_END = "2026-09"
EARLY_SLICE_END = "2024-12"
RIDGE_ALPHAS = (0.1, 1.0, 10.0)
RIDGE_SPLITS = 3
MIN_MODEL_TRAIN = 24

MODEL_NAMES = (
    "M1_core_raw",
    "M2_core_fourier",
    "M3_core_ucm",
    "M4_core_ridge",
    "M5_core_seasonal_naive",
)

MODEL_INFO = {
    "M1_core_raw": {
        "kind": "contemporaneous",
        "information_set": "месяц t: недельная панель (cutoff M+1, 10-е число)",
        "note": "Совмещённая оценка, не 1-step прогноз: core-субиндекс месяца t без SA.",
    },
    "M2_core_fourier": {
        "kind": "1-step_forecast",
        "information_set": "месяцы ≤ t−1",
        "note": "OLS месячного m/m: 3 Fourier-гармоники, tariff dummy, January dummy.",
    },
    "M3_core_ucm": {
        "kind": "1-step_forecast",
        "information_set": "месяцы ≤ t−1",
        "note": "UnobservedComponents: local level + stochastic seasonal(12).",
    },
    "M4_core_ridge": {
        "kind": "bridge_nowcast",
        "information_set": "месяц t (панель и B0, cutoff M+1, 10-е число) + лаги 1–3",
        "note": "Ridge-бридж core annualized на core, B0, лаги и Fourier.",
    },
    "M5_core_seasonal_naive": {
        "kind": "1-step_forecast",
        "information_set": "месяцы ≤ t−12",
        "note": "Robustness: m/m того же месяца год назад.",
    },
}


@dataclass(frozen=True)
class PanelRow:
    """Одна пригодная строка панели: месяц, группа, вес и месячный m/m."""

    month: str
    series: str
    component: str
    group_name: str
    weight: float
    mm_pp: float


@dataclass(frozen=True)
class RidgeFit:
    """Результат ridge-бриджа: прогноз, выбранный alpha и диагностика."""

    prediction: float
    alpha: float
    train_rows: int
    folds: int


def _float_or_none(value) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return float(text)


def read_panel(path: Path) -> list:
    """Читает CSV-панель как список словарей, не фильтруя строки."""
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def panel_month(value: str) -> str:
    """`2021-01-01` → `2021-01`; проверяет формат даты панели."""
    text = (value or "").strip()
    if len(text) < 7:
        raise ValueError(f"месяц панели {value!r}: ожидается YYYY-MM-DD")
    return text[:7]


def usable_rows(raw_rows: list, exclusions=EXCLUSIONS) -> list:
    """Строки вне исключений с непустым `mm_pp` и `weight_used > 0`."""
    excluded = set(exclusions)
    out = []
    for row in raw_rows:
        group = (row.get("group_name") or "").strip()
        if group in excluded:
            continue
        mm_pp = _float_or_none(row.get("mm_pp"))
        weight = _float_or_none(row.get("weight_used"))
        if mm_pp is None or weight is None or weight <= 0:
            continue
        out.append(
            PanelRow(
                month=panel_month(row.get("month")),
                series=(row.get("series") or "").strip(),
                component=(row.get("component") or "").strip(),
                group_name=group,
                weight=weight,
                mm_pp=mm_pp,
            )
        )
    return out


def core_subindex(rows: list, min_items: int = MIN_ITEMS) -> tuple:
    """Взвешенный core-субиндекс по месяцам и список пропущенных месяцев.

    Возвращает `({month: {mm, n_items, weight_sum}}, {month: {n_items,
    weight_sum, reason}})`. Требование — не менее `min_items` рядов с весом.
    """
    by_month = defaultdict(list)
    for row in rows:
        by_month[row.month].append(row)

    index = {}
    skipped = {}
    for month in sorted(by_month):
        items = by_month[month]
        weight_sum = math.fsum(item.weight for item in items)
        n_items = len(items)
        if n_items < min_items:
            skipped[month] = {
                "n_items": n_items,
                "weight_sum": weight_sum,
                "reason": f"n_items {n_items} < {min_items}",
            }
            continue
        if weight_sum <= 0:
            skipped[month] = {
                "n_items": n_items,
                "weight_sum": weight_sum,
                "reason": "weight_sum <= 0",
            }
            continue
        mean = math.fsum(item.weight * item.mm_pp for item in items) / weight_sum
        index[month] = {
            "mm": mean,
            "n_items": n_items,
            "weight_sum": weight_sum,
        }
    return index, skipped


def annualize_mm(mm_pp: float) -> float:
    """Месячный m/m в п.п. → % годовых: (1 + mm/100)^12 − 1."""
    return (math.pow(1.0 + mm_pp / 100.0, 12) - 1.0) * 100.0


def shift_month(key: str, delta: int) -> str:
    year, month = (int(part) for part in key.split("-"))
    index = year * 12 + (month - 1) + delta
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def fourier_terms(month: str, harmonics: int = FOURIER_HARMONICS) -> list:
    """3 Fourier-гармоники годового цикла: sin,cos для k = 1..harmonics."""
    number = int(month[5:7])
    terms = []
    for harmonic in range(1, harmonics + 1):
        angle = 2.0 * math.pi * harmonic * number / 12.0
        terms.extend((math.sin(angle), math.cos(angle)))
    return terms


def fourier_design(month: str, tariff_months=TARIFF_MONTHS) -> list:
    """Дизайн OLS: intercept, 3 Fourier-гармоники, tariff dummy, January dummy."""
    return [
        1.0,
        *fourier_terms(month),
        1.0 if month in tariff_months else 0.0,
        1.0 if month[5:7] == "01" else 0.0,
    ]


def test_months(window_keys: list, min_train: int) -> list:
    """Тестовые месяцы окна: все, перед которыми не менее `min_train` месяцев."""
    return list(window_keys[min_train:])


def expanding_splits(size: int, n_splits: int = RIDGE_SPLITS) -> list:
    """Expanding-разбиение (аналог TimeSeriesSplit): train — префикс, val — блок."""
    if size < 2:
        return []
    test_size = size // (n_splits + 1)
    if test_size < 1:
        return []
    splits = []
    for fold in range(n_splits):
        test_end = size - (n_splits - 1 - fold) * test_size
        test_start = test_end - test_size
        if test_start <= 0:
            continue
        splits.append((0, test_start, test_end))
    return splits


def _ridge_fit(x: np.ndarray, y: np.ndarray, alpha: float) -> tuple:
    """Замкнутая ridge-регрессия с нештрафуемым intercept (x уже стандартизован)."""
    mean_x = x.mean(axis=0)
    mean_y = float(y.mean())
    centered_x = x - mean_x
    centered_y = y - mean_y
    gram = centered_x.T @ centered_x + alpha * np.eye(centered_x.shape[1])
    beta = np.linalg.solve(gram, centered_x.T @ centered_y)
    return beta, mean_x, mean_y


def _ridge_predict(x: np.ndarray, beta: np.ndarray, mean_x: np.ndarray, mean_y: float) -> np.ndarray:
    return (x - mean_x) @ beta + mean_y


def _scaler(train_x: np.ndarray, other_x: np.ndarray) -> tuple:
    mean = train_x.mean(axis=0)
    std = train_x.std(axis=0)
    std = np.where(std > 0, std, 1.0)
    return (train_x - mean) / std, (other_x - mean) / std


def ridge_cv_predict(
    train_x: np.ndarray,
    train_y: np.ndarray,
    test_x: np.ndarray,
    alphas=RIDGE_ALPHAS,
    n_splits: int = RIDGE_SPLITS,
) -> RidgeFit:
    """RidgeCV: alpha по expanding-валидации внутри train, затем refit на всём train.

    StandardScaler обучается на train каждого фолда (никакой утечки из
    валидации), финальный scaler — на полном train. При нехватке фолдов
    берётся alpha по умолчанию (1.0). Ничьи разрешаются в пользу большего
    alpha — правило фиксировано до прогона и делает выбор детерминированным.
    """
    train_x = np.asarray(train_x, dtype=float)
    train_y = np.asarray(train_y, dtype=float)
    test_x = np.asarray(test_x, dtype=float).reshape(1, -1)
    if train_x.ndim != 2 or train_x.shape[0] < 2:
        raise ValueError(f"ridge: {train_x.shape[0]} train-строк — мало")

    scores = {alpha: [] for alpha in alphas}
    folds = 0
    for train_end, valid_start, valid_end in expanding_splits(len(train_y), n_splits):
        if train_end < max(3, train_x.shape[1] // 2):
            continue
        x_train, x_valid = _scaler(train_x[:train_end], train_x[valid_start:valid_end])
        y_train = train_y[:train_end]
        y_valid = train_y[valid_start:valid_end]
        for alpha in alphas:
            beta, mean_x, mean_y = _ridge_fit(x_train, y_train, alpha)
            errors = _ridge_predict(x_valid, beta, mean_x, mean_y) - y_valid
            scores[alpha].append(float(np.mean(errors * errors)))
        folds += 1

    if folds:
        mean_scores = {
            alpha: (sum(values) / len(values) if values else float("inf"))
            for alpha, values in scores.items()
        }
        best_alpha = min(alphas, key=lambda value: (mean_scores[value], -value))
    else:
        best_alpha = 1.0

    x_train, x_test = _scaler(train_x, test_x)
    beta, mean_x, mean_y = _ridge_fit(x_train, train_y, best_alpha)
    prediction = float(_ridge_predict(x_test, beta, mean_x, mean_y)[0])
    return RidgeFit(prediction, float(best_alpha), int(train_x.shape[0]), folds)


def m4_features(
    month: str,
    subindex_annual: dict,
    baseline: dict,
    window: set | None = None,
) -> list | None:
    """Признаки M4 для месяца t; None, если нет субиндекса/B0 в t или лагов 1–3.

    `window` ограничивает признаки месяцами окна: лаги до старта окна не
    используются, иначе окно 2022+ подмешивало бы 2021 год.
    """
    lag1 = shift_month(month, -1)
    lag2 = shift_month(month, -2)
    lag3 = shift_month(month, -3)
    required = (month, lag1, lag2, lag3)
    if window is not None and not set(required) <= window:
        return None
    if any(key not in subindex_annual for key in required):
        return None
    if month not in baseline:
        return None
    return [
        subindex_annual[month],
        baseline[month],
        subindex_annual[lag1],
        subindex_annual[lag2],
        subindex_annual[lag3],
        *fourier_terms(month),
    ]


def _failure(kind: str, stage: str, error: str, month: str | None = None) -> dict:
    return {
        "kind": kind,
        "stage": stage,
        "month": month,
        "error": error,
    }


def fourier_forecast(core_mm: dict, target: str) -> tuple:
    """M2: OLS месячного m/m на Fourier + tariff + January, прогноз target."""
    train = sorted(key for key in core_mm if key < target)
    if len(train) < MIN_MODEL_TRAIN:
        return None, _failure(
            "skipped", "fit", f"train {len(train)} < {MIN_MODEL_TRAIN}", target
        )
    design = np.array([fourier_design(key) for key in train], dtype=float)
    values = np.array([core_mm[key] for key in train], dtype=float)
    beta, *_ = np.linalg.lstsq(design, values, rcond=None)
    predicted_mm = float(np.array(fourier_design(target), dtype=float) @ beta)
    return annualize_mm(predicted_mm), None


def ucm_forecast(core_mm: dict, target: str, window_keys: list) -> tuple:
    """M3: state-space local level + stochastic seasonal(12), прогноз на target.

    Ряд строится по непрерывному календарю окна; пропущенный месяц панели
    (например, 2026-01) остаётся NaN. Если пропуск в конце train, последние
    наблюдения срезаются, а горизонт прогноза увеличивается до target.
    """
    train_keys = [key for key in window_keys if key < target]
    if not train_keys:
        return None, _failure("skipped", "fit", "пустой train", target)
    values = np.array([core_mm.get(key, math.nan) for key in train_keys], dtype=float)
    observed = int(np.isfinite(values).sum())
    if observed < MIN_MODEL_TRAIN:
        return None, _failure(
            "skipped", "fit", f"observed {observed} < {MIN_MODEL_TRAIN}", target
        )

    last_observed = None
    for index, value in enumerate(values):
        if math.isfinite(value):
            last_observed = index
    steps = len(train_keys) - last_observed
    trimmed = values[: last_observed + 1]
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model = UnobservedComponents(
                trimmed,
                level="local level",
                seasonal=12,
                stochastic_level=True,
                stochastic_seasonal=True,
            )
            result = model.fit(disp=False)
            forecast = np.asarray(result.forecast(steps), dtype=float)
    except Exception as exc:  # noqa: BLE001 — сбой модели фиксируется и не рвёт прогон
        return None, _failure("failed", "fit", f"{type(exc).__name__}: {exc}", target)
    if forecast.size < steps or not math.isfinite(float(forecast[-1])):
        return None, _failure("failed", "forecast", "нефинитный прогноз", target)
    return annualize_mm(float(forecast[-1])), None


def seasonal_naive(core_mm: dict, target: str) -> tuple:
    """M5: `mm` того же месяца год назад."""
    previous = shift_month(target, -12)
    if previous not in core_mm:
        return None, _failure(
            "skipped", "predict", f"нет {previous}", target
        )
    return annualize_mm(core_mm[previous]), None


def run_models(
    window_keys: list,
    test_keys: list,
    core_index: dict,
    subindex_annual: dict,
    target_core: dict,
    baseline: dict,
) -> tuple:
    """Прогоняет M1–M5 по тестовым месяцам окна.

    `subindex_annual` — core-субиндекс из компонент (предиктор M1 и признак
    M4), `target_core` — официальный «Базовый ИПЦ» (цель M4). Возвращает
    `(predictions, details, failures)`: словари прогнозов по моделям,
    диагностику и список пропусков/сбоев.
    """
    window_set = set(window_keys)
    core_mm = {
        key: value["mm"]
        for key, value in core_index.items()
        if key in window_set
    }
    predictions = {name: {} for name in MODEL_NAMES}
    details = {}
    failures = []

    for target in test_keys:
        if target in subindex_annual:
            predictions["M1_core_raw"][target] = subindex_annual[target]
        else:
            failures.append(
                {
                    "model": "M1_core_raw",
                    "kind": "skipped",
                    "stage": "predict",
                    "month": target,
                    "error": "месяц пропущен: < 30 пригодных рядов панели",
                }
            )

        value, failure = fourier_forecast(core_mm, target)
        if failure:
            failures.append({"model": "M2_core_fourier", **failure})
        else:
            predictions["M2_core_fourier"][target] = value

        value, failure = ucm_forecast(core_mm, target, window_keys)
        if failure:
            failures.append({"model": "M3_core_ucm", **failure})
        else:
            predictions["M3_core_ucm"][target] = value

        value, failure = seasonal_naive(core_mm, target)
        if failure:
            failures.append({"model": "M5_core_seasonal_naive", **failure})
        else:
            predictions["M5_core_seasonal_naive"][target] = value

    feature_rows = {}
    for month in window_keys:
        features = m4_features(month, subindex_annual, baseline, window_set)
        if features is not None:
            feature_rows[month] = features
    details["M4_core_ridge"] = {
        "feature_names": [
            "subindex_annual_t",
            "b0_annual_t",
            "subindex_annual_lag1",
            "subindex_annual_lag2",
            "subindex_annual_lag3",
            *[
                f"{kind}{harmonic}"
                for harmonic in range(1, FOURIER_HARMONICS + 1)
                for kind in ("sin", "cos")
            ],
        ],
        "alphas": list(RIDGE_ALPHAS),
        "splits": RIDGE_SPLITS,
        "feature_rows": len(feature_rows),
        "alpha_counts": {},
    }
    for target in test_keys:
        target_features = feature_rows.get(target)
        if target_features is None:
            failures.append(
                {
                    "model": "M4_core_ridge",
                    "kind": "skipped",
                    "stage": "features",
                    "month": target,
                    "error": "нет core/B0 в t или лагов 1–3",
                }
            )
            continue
        train_months = [
            key
            for key in feature_rows
            if key < target and key in target_core
        ]
        if len(train_months) < MIN_MODEL_TRAIN:
            failures.append(
                {
                    "model": "M4_core_ridge",
                    "kind": "skipped",
                    "stage": "fit",
                    "month": target,
                    "error": f"train {len(train_months)} < {MIN_MODEL_TRAIN}",
                }
            )
            continue
        train_x = np.array([feature_rows[key] for key in train_months], dtype=float)
        train_y = np.array([target_core[key] for key in train_months], dtype=float)
        try:
            fit = ridge_cv_predict(train_x, train_y, np.array(target_features, dtype=float))
        except Exception as exc:  # noqa: BLE001 — сбой модели фиксируется
            failures.append(
                {
                    "model": "M4_core_ridge",
                    "kind": "failed",
                    "stage": "fit",
                    "month": target,
                    "error": f"{type(exc).__name__}: {exc}",
                }
            )
            continue
        predictions["M4_core_ridge"][target] = fit.prediction
        counts = details["M4_core_ridge"]["alpha_counts"]
        key = f"{fit.alpha:g}"
        counts[key] = counts.get(key, 0) + 1

    details["M1_core_raw"] = {"kind": "contemporaneous", "train_rows": 0}
    details["M2_core_fourier"] = {
        "kind": "1-step_forecast",
        "design": "intercept + 3 Fourier harmonics + tariff dummy + January dummy",
    }
    details["M3_core_ucm"] = {
        "kind": "1-step_forecast",
        "model": "UnobservedComponents(local level, seasonal=12, stochastic level & seasonal)",
    }
    details["M5_core_seasonal_naive"] = {"kind": "1-step_forecast", "lag": 12}

    return predictions, details, failures


def _prediction_list(predictions: dict, keys: list) -> list:
    return [
        {"month": key, "value": predictions[key]}
        for key in keys
        if key in predictions
    ]


def _status(predictions: dict, test_keys: list) -> str:
    if not predictions:
        return "failed"
    if set(test_keys) <= set(predictions):
        return "ok"
    return "partial"


def coverage_by_month(
    window_keys: list,
    core_index: dict,
    skipped: dict,
    min_items: int,
) -> list:
    """Покрытие core-субиндекса по месяцам окна: n_items, Σw, m/m, пропуски."""
    out = []
    for month in window_keys:
        record = core_index.get(month)
        if record is not None:
            out.append(
                {
                    "month": month,
                    "n_items": record["n_items"],
                    "weight_sum": record["weight_sum"],
                    "weight_share_pct": record["weight_sum"] * 100.0,
                    "mm_pp": record["mm"],
                    "status": "ok",
                }
            )
            continue
        reason = skipped.get(month)
        out.append(
            {
                "month": month,
                "n_items": reason["n_items"] if reason else 0,
                "weight_sum": reason["weight_sum"] if reason else 0.0,
                "weight_share_pct": (reason["weight_sum"] if reason else 0.0) * 100.0,
                "mm_pp": None,
                "status": "skipped",
                "reason": reason["reason"] if reason else f"нет строк панели (< {min_items})",
            }
        )
    return out


def coverage_by_year(month_coverage: list) -> list:
    by_year = defaultdict(list)
    for record in month_coverage:
        by_year[record["month"][:4]].append(record)
    out = []
    for year in sorted(by_year):
        records = by_year[year]
        valid = [record for record in records if record["status"] == "ok"]
        out.append(
            {
                "year": int(year),
                "months": len(records),
                "months_with_subindex": len(valid),
                "mean_items": sum(record["n_items"] for record in valid) / len(valid)
                if valid
                else 0.0,
                "min_items": min((record["n_items"] for record in valid), default=0),
                "max_items": max((record["n_items"] for record in valid), default=0),
                "mean_weight_share": sum(record["weight_sum"] for record in valid)
                / len(valid)
                if valid
                else 0.0,
                "mean_weight_share_pct": (
                    sum(record["weight_sum"] for record in valid) / len(valid) * 100.0
                    if valid
                    else 0.0
                ),
            }
        )
    return out


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git_sha() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _p_value_entries(window_results: dict, family_size: int, alpha: float) -> list:
    threshold = alpha / family_size if family_size else alpha
    out = []
    for window, block in window_results.items():
        for model in MODEL_NAMES:
            entry = block["models"].get(model) or {}
            result = entry.get("metrics")
            if not result:
                continue
            p_value = result["dm_vs_b0"]["p"]
            out.append(
                {
                    "window": window,
                    "model": model,
                    "n": result["n"],
                    "mae": result["mae"],
                    "baseline_mae": result["baseline"]["mae"],
                    "mae_below_b0": result["mae"] < result["baseline"]["mae"],
                    "dm_p": p_value,
                    "bonferroni_threshold": threshold,
                    "bonferroni_significant": p_value < threshold,
                }
            )
    out.sort(key=lambda row: (row["dm_p"], row["window"], row["model"]))
    return out


def _window_result(
    window: str,
    start: str,
    end: str,
    min_train: int,
    core_index: dict,
    core_skipped: dict,
    subindex_annual: dict,
    target_core: dict,
    baseline: dict,
) -> dict:
    window_keys = months_between(start, end)
    test_keys = test_months(window_keys, min_train)
    predictions, details, failures = run_models(
        window_keys,
        test_keys,
        core_index,
        subindex_annual,
        target_core,
        baseline,
    )
    actual = subset(target_core, window_keys)
    baseline_window = subset(baseline, window_keys)
    evaluated = evaluate_basis(actual, baseline_window, predictions, test_keys)

    models = {}
    for model in MODEL_NAMES:
        result = evaluated.get(model)
        models[model] = {
            "status": _status(predictions[model], test_keys),
            "info": MODEL_INFO[model],
            "details": details.get(model, {}),
            "n_predictions": len(predictions[model]),
            "train_range": None,
            "metrics": result,
            "predictions": _prediction_list(predictions[model], test_keys),
        }
        failures_for_model = [item for item in failures if item.get("model") == model]
        models[model]["failures"] = failures_for_model

    # train_range — фактическое число наблюдений в train по моделям, где оно
    # определено; для остальных оно следует из окна и min_train.
    # M2/M3/M5 используют месяцы окна до target; M2 — только месяцы с m/m.
    for model in ("M2_core_fourier", "M3_core_ucm", "M5_core_seasonal_naive"):
        first_test = test_keys[0] if test_keys else None
        if first_test is None:
            continue
        train_obs = len(
            [key for key in core_index if start <= key < first_test]
        )
        models[model]["train_range"] = {
            "first_test": first_test,
            "observed_train_rows": train_obs,
        }
    # M4: первый тестовый месяц и число train-строк с признаками.
    if test_keys:
        window_set = set(window_keys)
        feature_months = [
            key
            for key in window_keys
            if m4_features(key, subindex_annual, baseline, window_set) is not None
        ]
        first = test_keys[0]
        train_rows = len(
            [key for key in feature_months if key < first and key in target_core]
        )
        models["M4_core_ridge"]["train_range"] = {
            "first_test": first,
            "observed_train_rows": train_rows,
        }

    core_coverage = coverage_by_month(window_keys, core_index, core_skipped, MIN_ITEMS)

    return {
        "window": window,
        "start": start,
        "end": end,
        "min_train": min_train,
        "months": len(window_keys),
        "test_months": test_keys,
        "core_coverage": core_coverage,
        "baseline_b0": evaluated.get("b0"),
        "models": models,
        "failures": failures,
    }


def _build_windows(start_arg: str | None, end: str, early_slice: bool) -> list:
    """Окна прогона: 2021+ и/или 2022+ плюс ранний срез 2021-01…2024-12."""
    windows = []
    if start_arg in (None, "2021-01"):
        windows.append((f"2021-01..{end}", "2021-01", end))
    if start_arg in (None, "2022-01"):
        windows.append((f"2022-01..{end}", "2022-01", end))
    if start_arg in (None, "2021-01") and early_slice and end > EARLY_SLICE_END:
        windows.append((f"2021-01..{EARLY_SLICE_END}", "2021-01", EARLY_SLICE_END))
    return windows


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Core-прокси ИПЦ из недельных компонент и короткие модели (#364)"
    )
    parser.add_argument(
        "--start",
        choices=("2021-01", "2022-01"),
        default=None,
        help="старт окна; по умолчанию оба окна (2021-01 и 2022-01)",
    )
    parser.add_argument("--end", default=DEFAULT_END, help="конец окна (YYYY-MM)")
    parser.add_argument(
        "--min-train",
        type=int,
        default=DEFAULT_MIN_TRAIN,
        help="минимум месяцев истории до первого тестового месяца",
    )
    parser.add_argument("--panel", default=str(PANEL_PATH), help="CSV-панель компонент")
    parser.add_argument("--out", default=str(DEFAULT_OUT), help="путь JSON-артефакта")
    parser.add_argument(
        "--no-early-slice",
        action="store_true",
        help="не добавлять ранний срез 2021-01…2024-12",
    )
    args = parser.parse_args(argv)

    if args.min_train < 12:
        parser.error("--min-train должен быть не меньше 12")

    panel_path = Path(args.panel)
    raw_rows = read_panel(panel_path)
    rows = usable_rows(raw_rows)
    core_index, core_skipped = core_subindex(rows, MIN_ITEMS)
    subindex_annual = {
        month: annualize_mm(value["mm"]) for month, value in core_index.items()
    }

    core_monthly = load_monthly(CORE_SQL, as_index=False)
    headline_monthly = load_monthly(HEADLINE_SQL, as_index=True)

    aggregate_history = load_aggregate_history()
    baseline_all = align_last(four_week_average(aggregate_history))

    # Сверка baseline с путём #348 через load_weekly: на пересечении рядов
    # 4-нед. средняя из load_aggregate_history и из load_weekly должна совпадать.
    _, aggregate_weekly = load_weekly()
    baseline_weekly = align_last(four_week_average(aggregate_weekly))
    overlap = sorted(set(baseline_all) & set(baseline_weekly))
    baseline_validation = None
    if overlap:
        differences = [baseline_all[key] - baseline_weekly[key] for key in overlap]
        baseline_validation = {
            "n": len(overlap),
            "window": [overlap[0], overlap[-1]],
            "mae": eu.mae(differences),
            "max_abs": max(abs(value) for value in differences),
        }

    windows = _build_windows(args.start, args.end, not args.no_early_slice)

    # Покрытие строится на общем месячном диапазоне 2021-01…end, чтобы JSON
    # содержал единую таблицу by_year/by_month независимо от числа окон.
    coverage_start = min(item[1] for item in windows)
    coverage_keys = months_between(coverage_start, args.end)
    month_coverage = coverage_by_month(coverage_keys, core_index, core_skipped, MIN_ITEMS)

    window_results = {}
    for window, start, end in windows:
        window_results[window] = _window_result(
            window,
            start,
            end,
            args.min_train,
            core_index,
            core_skipped,
            subindex_annual,
            core_monthly,
            baseline_all,
        )

    in_sample = {
        "in_sample": True,
        "note": (
            "Диагностика на полном окне, НЕ as-of и НЕ смешивается с walk-forward. "
            "M1 — совмещённая оценка core-субиндекса; B0 — baseline на тех же месяцах."
        ),
        "windows": {},
    }
    for window, start, end in windows:
        keys = months_between(start, end)
        actual = subset(core_monthly, keys)
        baseline_window = subset(baseline_all, keys)
        m1 = subset(subindex_annual, keys)
        block = evaluate_basis(actual, baseline_window, {"M1_core_raw": m1}, keys)
        headline_keys = subset(headline_monthly, keys)
        headline_block = (
            metrics(headline_keys, subset(m1, keys), subset(baseline_window, keys))
            if headline_keys
            else None
        )
        in_sample["windows"][window] = {
            "in_sample": True,
            "M1_core_raw": block.get("M1_core_raw"),
            "B0": block.get("b0"),
            "headline_M1": headline_block,
        }

    headline_as_of = {}
    for window, result in window_results.items():
        test_keys = result["test_months"]
        keys = [key for key in test_keys if key in headline_monthly]
        if not keys:
            headline_as_of[window] = None
            continue
        actual = subset(headline_monthly, keys)
        baseline = subset(baseline_all, keys)
        m1 = subset(subindex_annual, keys)
        headline_as_of[window] = {
            "M1_core_raw": metrics(actual, m1, baseline),
            "B0": metrics(actual, baseline, baseline),
        }

    family_size = len(MODEL_NAMES) * len(windows)
    p_values = _p_value_entries(window_results, family_size, 0.05)
    per_window_threshold = 0.05 / len(MODEL_NAMES)

    main_windows = [name for name, _, end in windows if end == args.end]
    criterion = {}
    for window in main_windows:
        for model in MODEL_NAMES:
            result = window_results[window]["models"][model]["metrics"]
            criterion[(window, model)] = bool(
                result
                and result["mae"] < result["baseline"]["mae"]
                and result["dm_vs_b0"]["p"] < 0.05
            )
    accepted_specs = sorted(
        f"{window}:{model}"
        for (window, model), passed in criterion.items()
        if passed
    )
    # Устойчивость «2021+ и 2022+» проверяема, только когда прогнаны оба окна;
    # при --start с одним окном её не заявляем.
    starts = {start for _, start, end in windows if end == args.end}
    stability_evaluated = {"2021-01", "2022-01"} <= starts
    stable_models = [
        model
        for model in MODEL_NAMES
        if stability_evaluated
        and all(criterion.get((window, model), False) for window in main_windows)
    ]
    any_below = sorted(
        {
            f"{window}:{model}"
            for window, block in window_results.items()
            for model in MODEL_NAMES
            if block["models"][model]["metrics"]
            and block["models"][model]["metrics"]["mae"]
            < block["models"][model]["metrics"]["baseline"]["mae"]
        }
    )
    bonferroni_specs = sorted(
        f"{row['window']}:{row['model']}"
        for row in p_values
        if row["bonferroni_significant"]
    )
    unadjusted_only_specs = sorted(
        f"{row['window']}:{row['model']}"
        for row in p_values
        if 0.05 > row["dm_p"] >= (0.05 / family_size if family_size else 0.05)
    )

    payload = {
        "spec_version": SPEC_VERSION,
        "decision": {
            "criterion": (
                "MAE ниже B0 и DM p<0,05 в предрегистрированной спецификации; "
                "устойчивость — критерий выполнен и на 2021+, и на 2022+."
            ),
            "accepted_specifications": accepted_specs,
            "stable_models_2021_and_2022": stable_models,
            "stable_on_2021_and_2022": bool(stable_models),
            "stability_evaluated": stability_evaluated,
            "any_mae_below_b0": any_below,
            "bonferroni_significant_specs": bonferroni_specs,
            "unadjusted_only_specs": unadjusted_only_specs,
            "bonferroni_threshold": 0.05 / family_size if family_size else None,
            "families_evaluated": len(main_windows),
            "note": (
                "M1 — совмещённая оценка (core-субиндекс месяца t), не 1-step прогноз. "
                "M4 — бридж на данных t и использует B0 как признак, поэтому его "
                "превосходство над B0 — коррекция baseline, а не независимый прогноз. "
                "M2/M3/M5 — прогнозы из месяцев ≤ t−1. Bonferroni-значимость не "
                "заявляется при p ≥ 0,05/(5 моделей × число окон)."
            ),
        },
        "git_sha": git_sha(),
        "package_versions": {
            "statsmodels": statsmodels.__version__,
            "numpy": np.__version__,
        },
        "inputs": {
            "panel": {
                "path": str(panel_path.relative_to(ROOT))
                if panel_path.is_relative_to(ROOT)
                else str(panel_path),
                "sha256": _sha256(panel_path),
                "raw_rows": len(raw_rows),
                "usable_rows": len(rows),
                "months": len({row.month for row in rows}),
                "range": [min(row.month for row in rows), max(row.month for row in rows)],
            },
            "target_core": {
                "source": "v_cbr_macro",
                "series": CORE_SERIES,
                "unit": "% годовых из м/м: pow(1+v/100,12)-1",
                "months": len(core_monthly),
                "range": [min(core_monthly), max(core_monthly)],
            },
            "target_headline": {
                "source": "v_rosstat_macro",
                "series": HEADLINE_SERIES,
                "unit": "% годовых из индекса: pow(v/100,12)-1",
                "role": "диагностика, не цель",
                "months": len(headline_monthly),
                "range": [min(headline_monthly), max(headline_monthly)],
            },
            "baseline": {
                "source": "v_rosstat_macro",
                "series": WEEKLY_AGGREGATE,
                "formula": "pow(exp(avg(ln(idx/100)) по 4 неделям), 52)-1, align_last",
                "points": len(aggregate_history),
                "months": len(baseline_all),
                "range": [min(baseline_all), max(baseline_all)],
                "validation_vs_load_weekly": baseline_validation,
            },
        },
        "exclusions": list(EXCLUSIONS),
        "spec": {
            "core_subindex": {
                "weight": "weight_used (доля ИПЦ, CB_COMP)",
                "mean": "Σ w·mm_pp / Σ w по строкам вне исключений",
                "min_items": MIN_ITEMS,
                "annualization": "(1 + mm/100)^12 - 1",
                "month_skip": "месяц пропускается при n_items < min_items",
            },
            "windows": [
                {
                    "name": name,
                    "start": start,
                    "end": end,
                    "min_train": args.min_train,
                    "test_start": months_between(start, end)[args.min_train]
                    if len(months_between(start, end)) > args.min_train
                    else None,
                }
                for name, start, end in windows
            ],
            "models": {model: MODEL_INFO[model] for model in MODEL_NAMES},
            "m2": {
                "harmonics": FOURIER_HARMONICS,
                "tariff_months": list(TARIFF_MONTHS),
                "january_dummy": True,
                "min_train_observations": MIN_MODEL_TRAIN,
            },
            "m3": {
                "level": "local level",
                "seasonal": 12,
                "stochastic_level": True,
                "stochastic_seasonal": True,
                "missing_months": "NaN в непрерывном календаре окна",
                "min_observed_train": MIN_MODEL_TRAIN,
            },
            "m4": {
                "features": [
                    "subindex_annual_t (core-субиндекс из компонент)",
                    "b0_annual_t (агрегат, 4-нед. средняя)",
                    "subindex_annual_lag1",
                    "subindex_annual_lag2",
                    "subindex_annual_lag3",
                    "3 Fourier harmonics (sin/cos)",
                ],
                "target": "официальный Базовый ИПЦ, годовое выражение",
                "alphas": list(RIDGE_ALPHAS),
                "inner_splits": RIDGE_SPLITS,
                "scaler": "StandardScaler внутри каждого train-фолда; финальный — на полном train",
                "selection": "min mean CV MSE; ничья — больший alpha",
            },
            "m5": {"lag_months": 12},
            "dm": {
                "test": "Diebold-Mariano (HLN, harvey_adj=True)",
                "h": 1,
                "loss": "squared errors",
                "against": "B0 (официальный агрегат, 4-нед. средняя)",
            },
            "multiple_testing": {
                "family_size": family_size,
                "alpha": 0.05,
                "bonferroni_threshold": 0.05 / family_size if family_size else None,
                "per_window_models": len(MODEL_NAMES),
                "per_window_threshold": per_window_threshold,
                "rule": (
                    "Bonferroni-значимость не заявляется при p ≥ 0,05/(число моделей×окон); "
                    "все p-values выводятся в p_values."
                ),
            },
            "min_model_train_observations": MIN_MODEL_TRAIN,
            "note_min_train": (
                "min_train=36 относится к календарю окна: первый тестовый месяц — "
                "37-й месяц окна. M4 из-за лагов и B0 имеет меньше фактических "
                "train-строк на первом тесте; это фиксируется в train_range и не "
                "является look-ahead."
            ),
        },
        "coverage": {
            "by_year": coverage_by_year(month_coverage),
            "by_month": month_coverage,
            "skipped_months": [
                {"month": record["month"], "reason": record.get("reason", "")}
                for record in month_coverage
                if record["status"] == "skipped"
            ],
        },
        "windows": window_results,
        "in_sample": in_sample,
        "headline_diagnostics": {
            "role": "только диагностика, цель — Базовый ИПЦ",
            "as_of": headline_as_of,
        },
        "p_values": p_values,
        "failures": {
            "count": sum(len(block["failures"]) for block in window_results.values()),
            "items": [
                {"window": window, **item}
                for window, block in window_results.items()
                for item in block["failures"]
            ],
        },
        "notes": [
            "Прод-код, DDL, витрины и гранты не менялись; ClickHouse только читается.",
            "Список исключений и модели зафиксированы до прогона и по результатам не менялись.",
            "B0 — официальный агрегат «Индекс потребительских цен (недельная оценка)»: "
            "4-нед. геометрическая средняя, align_last (формула research #348).",
            "M1 — совмещённая оценка, поэтому её DM против B0 не является тестом прогноза.",
            "M4 использует B0 как признак: его выигрыш у B0 — коррекция baseline, не независимый прогноз.",
            "As-of относится к недельной панели (cutoff M+1, 10-е число); цель и B0 — текущий винтаж витрин, PIT-винтажей core нет.",
            "Все p-values — DM-HLN h=1; multiple-testing оговорка в spec.multiple_testing.",
            "JSON детерминирован: нет wall-clock timestamp, порядок ключей и списков фиксирован.",
        ],
    }

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )

    print(f"windows={list(window_results)}")
    for window, block in window_results.items():
        b0 = block["baseline_b0"]
        if b0:
            print(
                f"{window} B0: n={b0['n']:2d} MAE={b0['mae']:.2f} "
                f"RMSE={b0['rmse']:.2f} corr={b0['corr']:.3f}"
            )
        for model in MODEL_NAMES:
            entry = block["models"][model]
            result = entry["metrics"]
            if not result:
                print(f"{window} {model}: no metrics ({entry['status']})")
                continue
            print(
                f"{window} {model}: n={result['n']:2d} MAE={result['mae']:.2f} "
                f"(B0 {result['baseline']['mae']:.2f}) RMSE={result['rmse']:.2f} "
                f"bias={result['bias']:+.2f} corr={result['corr']:.3f} "
                f"DM p={result['dm_vs_b0']['p']:.4f}"
            )
    threshold = 0.05 / family_size if family_size else float("nan")
    print(
        f"stable_models_2021_and_2022={stable_models} "
        f"bonferroni_threshold={threshold:.4f}"
    )
    print(f"written {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
