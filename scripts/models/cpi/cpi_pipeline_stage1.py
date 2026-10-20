#!/usr/bin/env python3
"""Stage 1 конвейера CPI → SA/ожидания → ставка: nowcast `M0`/`M0+1`.

Пункт `key-rate/phase-4-cpi-key-rate-pipeline`, спека
`docs/superpowers/specs/2026-10-20-cpi-key-rate-pipeline-design.md` §4, §6.1.

Идея: на дату заседания ЦБ `d` по календарю публикаций найти последний
вышедший месячный ИПЦ, следующий неопубликованный месяц `M0` и предсказать
NSA m/m для `M0` и `M0+1`. Информационное множество stage 1 — только месячные
лаги, недельная diffusion-статистика, USD, FAO, ключевая ставка и номер
месяца; ожидания, SA ЦБ и мониторинг предприятий принадлежат stage 2 и здесь
запрещены.

As-of: каждое заседание считается моментом 13:30 МСК (решение ЦБ), поэтому
релиз Росстата в 19:00 того же дня в признаки не попадает. Недельные признаки
считаются по cutoff `meeting_date − 1 день`: так же исключается недельный
релиз среды в 19:00. Месячные лаги проверяются через
`PublicationCalendar.is_available`, а не по фиксированному «8-е M+1».

Скрипт только читает ClickHouse; в БД не пишет. Запуск — `make
cpi-pipeline-stage1` (env подключает Makefile), тесты —
`scripts/models/cpi/cpi_pipeline_stage1_test.py`.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import warnings
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Optional

import numpy as np
from catboost import CatBoostRegressor
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import ElasticNetCV
from sklearn.model_selection import TimeSeriesSplit
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import eval_utils as eu  # noqa: E402
from lib import ml_experiment as mlx  # noqa: E402
from lib.publication_calendar import PublicationCalendar  # noqa: E402
from models.cpi import cpi_monthly as cm  # noqa: E402
from models.cpi import cpi_monthly_v3 as cmv3  # noqa: E402

# Ряд-«голова» stage 1: общий ИПЦ Росстата, NSA m/m (percent − 100).
SOURCE = "rosstat"
SERIES = "cpi_all"

# Заседание ЦБ: решение оглашается около 13:30 МСК; релизы Росстата в 19:00
# того же дня уже после решения.
MEETING_TIME = time(13, 30)

# Календарь публикаций: экспериментальная фикстура Task 0. Production-путь —
# витрина `publication_calendar` (отдельный Go/DDL-пункт).
CALENDAR_PATH = ROOT / "scripts" / "testdata" / "publication_calendar.csv"

# Порядок признаков — контракт с матрицами моделей и отчётом. Ни одного
# признака ожиданий/SA/МП здесь нет: они reserved for stage 2.
FEATURE_NAMES = [
    "lag1",
    "lag2",
    "lag3",
    "wk_share_rising",
    "wk_median",
    "wk_median4",
    "wk_gasoline4",
    "usd_mm",
    "key_rate",
    "fao_mm",
    "month",
]

# Недельные признаки берутся из v3 как есть; cutoff передаётся отдельно.
WEEKLY_FEATURES = tuple(cmv3.WEEKLY_FEATURES)

SEED = cm.SEED
DEFAULT_START = 24
MIN_TRAIN = 12
BOOTSTRAP_BLOCK = 3
BOOTSTRAP_N = 2000

# Диагностические пары: CatBoost против B1, B3 против B1 и CatBoost против B3.
PAIR_SPECS = (
    ("catboost_direct", "b1_last_value"),
    ("b3_elastic_net", "b1_last_value"),
    ("catboost_direct", "b3_elastic_net"),
)


@dataclass(frozen=True)
class Stage1Row:
    """Строка stage 1: заседание, целевые месяцы, признаки и метки.

    `m0` — следующий неопубликованный на момент заседания месяц (первое
    число), `m1 = m0 + 1`. `y0`/`y1` — NSA m/m (`percent − 100`, п.п.) этих
    месяцев или `None`, если метка ещё не опубликована к `as_of` прогона.
    """

    meeting_date: date
    m0: date
    m1: date
    features: dict
    y0: Optional[float]
    y1: Optional[float]


@dataclass(frozen=True)
class Stage1Prediction:
    """Фолд stage 1: заседание, модель, цель и прогноз (п.п., м/м).

    `target` — `"y0"` или `"y1"`; `target_month` — месяц цели (`m0`/`m1`).
    `window` для паспорта run — сам target, чтобы fold-предсказания M0 и M0+1
    не смешивались.
    """

    meeting_date: date
    target_month: date
    model: str
    target: str
    y_true: float
    y_pred: float


# ---------------------------------------------------------------------------
# Загрузчики: тонкие обёртки, чтобы тесты подменяли данные, а не транспорт
# ---------------------------------------------------------------------------


def load_targets() -> dict:
    """Таргеты `rosstat_ipc_mes` (4 ряда, NSA m/m, `percent − 100`)."""
    return cm.load_targets()


def load_usd() -> list:
    """Дневной курс USD/руб."""
    return cm.load_usd()


def load_key_rate() -> list:
    """Дневной ряд действующих ставок ЦБ."""
    return cm.load_key_rate()


def load_fao() -> list:
    """Индексы FAO `Food Price Index` (месячно)."""
    return cm.load_fao()


def load_weekly_panel() -> tuple:
    """Недельная панель `(sorted_dates, by_date)` из `cpi_monthly_v3`.

    Обёртка нужна тесту as-of: он подменяет панель, не трогая ClickHouse.
    """
    return cmv3.weekly_panel(cmv3.load_weekly())


def load_meetings() -> list:
    """Даты канонических заседаний ЦБ (тот же отбор, что у key-rate модели)."""
    from models.key_rate import key_rate_model as krm

    return krm.load_meetings()


def load_calendar() -> PublicationCalendar:
    """Календарь публикаций из экспериментальной CSV-фикстуры."""
    return PublicationCalendar.from_csv(CALENDAR_PATH)


# ---------------------------------------------------------------------------
# Календарная арифметика
# ---------------------------------------------------------------------------


def shift_month(day: date, months: int) -> date:
    """Первое число месяца, сдвинутого на `months` от месяца дня `day`."""
    index = day.year * 12 + (day.month - 1) + months
    return date(index // 12, index % 12 + 1, 1)


def meeting_datetime(day: date) -> datetime:
    """Момент заседания: 13:30 МСК, наивное время."""
    return datetime.combine(day, MEETING_TIME)


def _float_or_nan(value: Optional[float]) -> float:
    """`None` из загрузчика — NaN, а не ноль (NaN ≠ 0)."""
    return float("nan") if value is None else float(value)


# ---------------------------------------------------------------------------
# Сборка строк
# ---------------------------------------------------------------------------


def _lag_features(
    m0: date,
    calendar: PublicationCalendar,
    meeting_at: datetime,
    targets_index: dict,
) -> dict:
    """`lag1..lag3` — опубликованные к заседанию NSA m/m до `M0`.

    `lag1` — последний опубликованный месяц (`M0−1`), `lag2`/`lag3` — два
    предыдущих. Доступность каждого месяца проверяется календарём, поэтому
    фиксированное «8-е M+1» в признаки не просачивается.
    """
    features = {}
    values = targets_index.get(cm.TARGET_ALL, {})
    for offset, name in ((1, "lag1"), (2, "lag2"), (3, "lag3")):
        month = shift_month(m0, -offset)
        value = None
        if calendar.is_available(SOURCE, SERIES, month, meeting_at):
            value = values.get(cm.month_end(month))
        features[name] = _float_or_nan(value)
    return features


def _weekly_features(panel: tuple, meeting_day: date) -> dict:
    """Недельные diffusion-признаки на cutoff `meeting_day − 1`.

    Заседание — 13:30, недельный релиз — 19:00, поэтому same-day неделя
    недоступна; cutoff на день раньше отсекает её (и любую другую точку,
    опубликованную в день заседания). Правило `date + 2` и окно 4 недель
    остаются внутри `cpi_monthly_v3.weekly_features`.
    """
    cutoff = meeting_day - timedelta(days=1)
    return cmv3.weekly_features(panel, cutoff)


def _market_features(
    m0: date,
    meeting_day: date,
    usd: list,
    key_rate: list,
    fao: list,
) -> dict:
    """`usd_mm`, `key_rate`, `fao_mm` на момент заседания.

    `usd_mm` — прирост среднего курса за `M0−1` к `M0−2` (обе корзины уже
    закрыты к заседанию); `key_rate` — последняя вступившая в силу ставка;
    `fao_mm` — м/м последней пары точек FAO, опубликованных к заседанию.
    """
    return {
        "usd_mm": _float_or_nan(cm.usd_mm(m0, usd)),
        "key_rate": _float_or_nan(
            cm.asof(key_rate, cm.available_from_rate, meeting_day)
        ),
        "fao_mm": _float_or_nan(cm.fao_mm(fao, meeting_day)),
    }


def _observed_target(
    targets_index: dict,
    month: date,
    calendar: PublicationCalendar,
    as_of_at: datetime,
) -> Optional[float]:
    """NSA m/m месяца, если он опубликован к `as_of`; иначе `None`.

    `as_of` — дата среза прогона; она тоже берётся моментом 13:30, поэтому
    релиз в 19:00 дня среза ещё не наблюдён.
    """
    if not calendar.is_available(SOURCE, SERIES, month, as_of_at):
        return None
    value = targets_index.get(cm.TARGET_ALL, {}).get(cm.month_end(month))
    return None if value is None else float(value)


def build_rows(
    meetings: list,
    calendar: PublicationCalendar,
    as_of: date,
) -> list:
    """Строки stage 1 для заседаний `meetings` на срезе `as_of`.

    Заседания дедуплицируются и сортируются; `as_of` — дата среза для меток
    `y0`/`y1` (тоже 13:30), поэтому исторические заседания имеют наблюдённые
    цели, а свежее — `None`. Признаки каждого заседания строятся на его
    собственный момент 13:30, а не на `as_of`.
    """
    as_of_at = meeting_datetime(as_of)
    targets = load_targets()
    targets_index = cm.target_index(targets)
    usd = load_usd()
    key_rate = load_key_rate()
    fao = load_fao()
    panel = load_weekly_panel()

    rows = []
    for meeting in sorted(set(meetings)):
        meeting_at = meeting_datetime(meeting)
        m0 = calendar.next_unpublished_month(SOURCE, SERIES, meeting_at)
        m1 = shift_month(m0, 1)
        features = {
            **_lag_features(m0, calendar, meeting_at, targets_index),
            **_weekly_features(panel, meeting),
            **_market_features(m0, meeting, usd, key_rate, fao),
            "month": m0.month,
        }
        rows.append(
            Stage1Row(
                meeting_date=meeting,
                m0=m0,
                m1=m1,
                features=features,
                y0=_observed_target(targets_index, m0, calendar, as_of_at),
                y1=_observed_target(targets_index, m1, calendar, as_of_at),
            )
        )
    return rows


def assert_no_lookahead(rows: list, calendar: PublicationCalendar) -> None:
    """Контроль часов: `m0` — действительно следующий неопубликованный месяц.

    Повторная проверка после сборки ловит правку `build_rows`, при которой
    строка получила бы месяц, уже опубликованный к заседанию (look-ahead).
    """
    for row in rows:
        expected = calendar.next_unpublished_month(
            SOURCE, SERIES, meeting_datetime(row.meeting_date)
        )
        if row.m0 != expected:
            raise AssertionError(
                f"утечка часов: заседание {row.meeting_date}, m0={row.m0}, "
                f"календарь даёт {expected}"
            )
        if row.m1 != shift_month(row.m0, 1):
            raise AssertionError(
                f"утечка часов: m1={row.m1} не равен m0+1 ({row.m0})"
            )


# ---------------------------------------------------------------------------
# Модели: CatBoost direct, B1 last value, B3 ElasticNetCV
# ---------------------------------------------------------------------------


def _target_rows(train: list, target: str) -> list:
    """Строки train с наблюдённой меткой `target`."""
    return [row for row in train if getattr(row, target) is not None]


def _catboost_matrix(rows: list) -> np.ndarray:
    """Матрица CatBoost: числовые float + целый код месяца (object-массив).

    `cat_features` требует не-float матрицу; код месяца обязан дойти целым.
    NaN числовых колонок передаётся нативно (`nan_mode` библиотеки).
    """
    matrix = np.empty((len(rows), len(FEATURE_NAMES)), dtype=object)
    for position, name in enumerate(FEATURE_NAMES):
        if name == "month":
            matrix[:, position] = [int(row.features[name]) for row in rows]
        else:
            matrix[:, position] = [float(row.features[name]) for row in rows]
    return matrix


def fit_catboost_target(train: list, row: Stage1Row, target: str) -> float:
    """CatBoost direct на одну цель; дефолты, seed, `month` категориальным."""
    subset = _target_rows(train, target)
    if len(subset) < MIN_TRAIN:
        raise cm.DegenerateWindowError(
            f"CatBoost {target}: обучающее окно {len(subset)} строк короче "
            f"{MIN_TRAIN}"
        )
    model = CatBoostRegressor(
        loss_function="RMSE",
        random_seed=SEED,
        verbose=False,
        allow_writing_files=False,
        cat_features=[FEATURE_NAMES.index("month")],
    )
    model.fit(
        _catboost_matrix(subset),
        np.asarray(
            [float(getattr(item, target)) for item in subset], dtype=float
        ),
    )
    return float(model.predict(_catboost_matrix([row]))[0])


def _enet_features() -> list:
    """Признаки B3: все, кроме `month` (сезонность в ElasticNet не моделится)."""
    return [name for name in FEATURE_NAMES if name != "month"]


def fit_enet_target(train: list, row: Stage1Row, target: str) -> float:
    """ElasticNetCV на одну цель; медиана и scaler — только внутри train."""
    subset = _target_rows(train, target)
    if len(subset) < cm.ENET_MIN_TRAIN:
        raise cm.DegenerateWindowError(
            f"B3 {target}: обучающее окно {len(subset)} строк короче "
            f"{cm.ENET_MIN_TRAIN}"
        )
    names = _enet_features()
    train_matrix = np.asarray(
        [[float(item.features[name]) for name in names] for item in subset],
        dtype=float,
    )
    row_matrix = np.asarray(
        [[float(row.features[name]) for name in names]], dtype=float
    )
    with warnings.catch_warnings():
        # Полностью NaN-колонка (например, `lag3` на старте истории) — не
        # ошибка: медиана не определена, колонка получает нейтральный ноль.
        warnings.simplefilter("ignore", RuntimeWarning)
        medians = np.nanmedian(train_matrix, axis=0)
    medians = np.where(np.isnan(medians), cm.ENET_MEDIAN_FALLBACK, medians)
    train_matrix = np.where(np.isnan(train_matrix), medians, train_matrix)
    row_matrix = np.where(np.isnan(row_matrix), medians, row_matrix)
    model = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "enet",
                ElasticNetCV(
                    alphas=cm.ENET_ALPHAS,
                    l1_ratio=list(cm.ENET_L1_RATIOS),
                    cv=TimeSeriesSplit(cm.ENET_CV_SPLITS),
                    random_state=SEED,
                    max_iter=10000,
                ),
            ),
        ]
    )
    with warnings.catch_warnings():
        # Недосход координатного спуска на крошечных alpha — диагностика, а не
        # порча прогноза: выбранная CV-модель даёт конечное значение (та же
        # политика, что у B3 в `cpi_monthly`). Шум не печатается.
        warnings.simplefilter("ignore", ConvergenceWarning)
        model.fit(
            train_matrix,
            np.asarray(
                [float(getattr(item, target)) for item in subset], dtype=float
            ),
        )
    return float(model.predict(row_matrix)[0])


def fit_catboost(train: list, row: Stage1Row) -> dict:
    """CatBoost direct по двум целям stage 1."""
    return {
        "y0": fit_catboost_target(train, row, "y0"),
        "y1": fit_catboost_target(train, row, "y1"),
    }


def fit_enet(train: list, row: Stage1Row) -> dict:
    """B3 ElasticNetCV по двум целям stage 1."""
    return {
        "y0": fit_enet_target(train, row, "y0"),
        "y1": fit_enet_target(train, row, "y1"),
    }


def fit_b1(train: list, row: Stage1Row) -> dict:
    """B1 last value: последний опубликованный m/m на обе цели."""
    value = float(row.features["lag1"])
    if not math.isfinite(value):
        raise cm.DegenerateWindowError("B1: lag1 отсутствует")
    return {"y0": value, "y1": value}


MODELS = {
    "catboost_direct": fit_catboost,
    "b1_last_value": fit_b1,
    "b3_elastic_net": fit_enet,
}


def fit_predict(train: list, row: Stage1Row, model: str) -> dict:
    """Прогноз модели `model` по двум целям: `{"y0": float, "y1": float}`."""
    if model not in MODELS:
        raise ValueError(
            f"неизвестная модель {model!r}: доступны {sorted(MODELS)}"
        )
    return MODELS[model](train, row)


# ---------------------------------------------------------------------------
# Walk-forward и оценка
# ---------------------------------------------------------------------------


def walk_forward(
    rows: list,
    start: int = DEFAULT_START,
    models: Optional[dict] = None,
) -> list:
    """Walk-forward по строкам stage 1: train `rows[:i]`, прогноз `rows[i]`.

    Строка с обеими ненаблюдёнными целями пропускается целиком (это свежее
    заседание без меток). Если наблюдена только одна цель, оценивается только
    она. `DegenerateWindowError` модели — пропуск фолда этой модели, как в
    `cpi_monthly.walk_forward`; прочие исключения пробрасываются.
    """
    if start < 1:
        raise ValueError(f"walk-forward: start={start} — меньше одной строки")
    for previous, current in zip(rows, rows[1:]):
        if previous.meeting_date >= current.meeting_date:
            raise ValueError(
                "walk-forward: строки не упорядочены по meeting_date "
                f"({previous.meeting_date} >= {current.meeting_date})"
            )
    selected = dict(MODELS) if models is None else dict(models)
    predictions = []
    for index in range(start, len(rows)):
        train = rows[:index]
        row = rows[index]
        if row.y0 is None and row.y1 is None:
            continue
        for name, fit in selected.items():
            try:
                values = fit(train, row)
            except cm.DegenerateWindowError:
                continue
            for target in ("y0", "y1"):
                actual = getattr(row, target)
                if actual is None:
                    continue
                predicted = values.get(target)
                if predicted is None or not math.isfinite(float(predicted)):
                    continue
                predictions.append(
                    Stage1Prediction(
                        meeting_date=row.meeting_date,
                        target_month=row.m0 if target == "y0" else row.m1,
                        model=name,
                        target=target,
                        y_true=float(actual),
                        y_pred=float(predicted),
                    )
                )
    return predictions


def _grouped(predictions: list, model: str, target: str) -> list:
    group = [
        item
        for item in predictions
        if item.model == model and item.target == target
    ]
    group.sort(key=lambda item: item.meeting_date)
    return group


def _mde_ratio(err_a: list, err_b: list) -> float:
    """MDE отношения RMSE (как в `cpi_monthly_v3`), при n < 2 — NaN."""
    if len(err_a) < 2:
        return float("nan")
    differences = [a * a - b * b for a, b in zip(err_a, err_b)]
    sd = float(np.std(differences, ddof=1))
    rmse_a = eu.rmse(err_a)
    rmse_b = eu.rmse(err_b)
    mde_mse = 2.802 * sd / math.sqrt(len(differences))
    return mde_mse / (rmse_a + rmse_b) if (rmse_a + rmse_b) else float("nan")


def evaluate(predictions: list) -> dict:
    """RMSE/MAE по модель × цель и DM/CI по зафиксированным парам.

    Возврат: `{"summary": {model: {target: {"n", "rmse", "mae", "meetings"}}},
    "pairs": {"left vs right:target": {"n", "rmse_left", "rmse_right",
    "delta", "delta_ci", "dm_stat", "dm_p", "mde_ratio"}}}`. Пары — только те,
    что объявлены в `PAIR_SPECS`; сравнение идёт на общих фолдах заседаний.
    """
    summary = {}
    for model in sorted({item.model for item in predictions}):
        summary[model] = {}
        for target in ("y0", "y1"):
            group = _grouped(predictions, model, target)
            if not group:
                summary[model][target] = {"n": 0}
                continue
            errors = [item.y_pred - item.y_true for item in group]
            summary[model][target] = {
                "n": len(group),
                "rmse": eu.rmse(errors),
                "mae": eu.mae(errors),
                "meetings": [
                    item.meeting_date.isoformat() for item in group
                ],
            }

    pairs = {}
    for left, right in PAIR_SPECS:
        for target in ("y0", "y1"):
            key = f"{left} vs {right}:{target}"
            group_left = _grouped(predictions, left, target)
            group_right = _grouped(predictions, right, target)
            by_left = {item.meeting_date: item for item in group_left}
            by_right = {item.meeting_date: item for item in group_right}
            common = sorted(set(by_left) & set(by_right))
            if len(common) < 2:
                # DM/бутстрап на одном фолде не определены: честно n, без
                # статистик (в прогонах фолдов десятки, это защита тестов).
                pairs[key] = {"n": len(common)}
                continue
            err_left = [
                by_left[day].y_pred - by_left[day].y_true for day in common
            ]
            err_right = [
                by_right[day].y_pred - by_right[day].y_true for day in common
            ]
            dm_stat, dm_p = eu.diebold_mariano(err_left, err_right, 1)
            boot = eu.block_bootstrap_rmse_diff(
                err_left, err_right, BOOTSTRAP_BLOCK, BOOTSTRAP_N, SEED
            )
            pairs[key] = {
                "n": len(common),
                "rmse_left": eu.rmse(err_left),
                "rmse_right": eu.rmse(err_right),
                "delta": boot.rmse_diff,
                "delta_ci": [boot.low, boot.high],
                "dm_stat": dm_stat,
                "dm_p": dm_p,
                "mde_ratio": _mde_ratio(err_left, err_right),
            }
    return {"summary": summary, "pairs": pairs}


# ---------------------------------------------------------------------------
# Артефакты
# ---------------------------------------------------------------------------

PREDICTIONS_NAME = "stage1_predictions.csv"
RESULT_NAME = "stage1_result.json"
REPORT_NAME = "stage1_report.md"
ARTIFACT_NAMES = (PREDICTIONS_NAME, RESULT_NAME, REPORT_NAME)
PREDICTIONS_COLUMNS = (
    "meeting_date",
    "target_month",
    "model",
    "target",
    "y_true",
    "y_pred",
)


def write_predictions(path: Path, predictions: list) -> None:
    """Fold-предсказания в CSV: заседание, цель, модель, факт и прогноз."""
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(PREDICTIONS_COLUMNS)
        for item in predictions:
            writer.writerow(
                [
                    item.meeting_date.isoformat(),
                    item.target_month.isoformat(),
                    item.model,
                    item.target,
                    f"{item.y_true:.6f}",
                    f"{item.y_pred:.6f}",
                ]
            )


def write_json(path: Path, payload: dict) -> None:
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def write_report(path: Path, payload: dict) -> None:
    """Markdown-отчёт прогона; числа не редактируются руками."""
    lines = ["# Stage 1 CPI nowcast `M0`/`M0+1` — отчёт прогона", ""]
    lines.append(
        "> `scripts/models/cpi/cpi_pipeline_stage1.py`; пункт "
        "`key-rate/phase-4-cpi-key-rate-pipeline`. Только чтение ClickHouse."
    )
    lines.append("")
    lines.append(f"- As-of: {payload['as_of']}")
    lines.append(
        f"- Заседаний: {payload['n_meetings']}; прогнозов: "
        f"{payload['n_predictions']}; start={payload['start']}"
    )
    if payload.get("first_meeting"):
        lines.append(
            f"- Окно: {payload['first_meeting']} … {payload['last_meeting']}"
        )
    lines.append(f"- Seed: {payload['seed']}")
    lines.append("")
    lines.append("## Модели")
    lines.append("")
    lines.append("| Модель | Цель | n | RMSE | MAE |")
    lines.append("|---|---|---:|---:|---:|")
    for model, by_target in payload["summary"].items():
        for target, item in by_target.items():
            if not item.get("n"):
                lines.append(f"| {model} | {target} | 0 | — | — |")
                continue
            lines.append(
                f"| {model} | {target} | {item['n']} | "
                f"{item['rmse']:.6f} | {item['mae']:.6f} |"
            )
    lines.append("")
    lines.append("## Парные сравнения")
    lines.append("")
    lines.append(
        "| Пара | n | RMSE left | RMSE right | Δ | 95 % CI Δ | DM p | MDE (ratio) |"
    )
    lines.append("|---|---:|---:|---:|---:|---|---:|---:|")
    for name, item in payload["pairs"].items():
        if not item.get("n"):
            lines.append(f"| {name} | 0 | — | — | — | — | — | — |")
            continue
        lines.append(
            f"| {name} | {item['n']} | {item['rmse_left']:.6f} | "
            f"{item['rmse_right']:.6f} | {item['delta']:+.6f} | "
            f"[{item['delta_ci'][0]:+.6f}; {item['delta_ci'][1]:+.6f}] | "
            f"{item['dm_p']:.4f} | {item['mde_ratio']:.4f} |"
        )
    lines.append("")
    lines.append("## Признаки")
    lines.append("")
    lines.append(
        "`" + "`, `".join(payload["features"]) + "`"
    )
    lines.append("")
    lines.append("## Что не проверено")
    lines.append("")
    lines.append(
        "- Gate 1 (RMSE ≤ текущий `cpi_live3_horizons`/`cpi_monthly_v3` + 5 %) "
        "требует отдельного прогона baseline на тех же фолдах."
    )
    lines.append(
        "- Исторические даты публикации — fallback-правила календаря; доля "
        "`precision=fallback` в паспорт не вынесена."
    )
    lines.append(
        "- Недельная точка считается доступной `date + 2`; фактическая "
        "задержка публикации может отличаться."
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _log_experiment(
    payload: dict,
    rows: list,
    predictions: list,
    paths: list,
    as_of: date,
) -> None:
    """Паспорт run (`ML_LOG=1`): ошибки журнала не роняют прогон."""
    try:
        logger = mlx.start_run(
            experiment="cpi_pipeline_stage1",
            run_name=(
                f"cpi_pipeline_stage1__asof{as_of.isoformat()}"
                f"__start{payload['start']}"
            ),
            config={
                "features": list(FEATURE_NAMES),
                "models": list(MODELS),
                "start": payload["start"],
                "seed": SEED,
            },
            data_asof=as_of,
            sample_meta={
                "rows": len(rows),
                "predictions": len(predictions),
                "first_meeting": payload.get("first_meeting"),
                "last_meeting": payload.get("last_meeting"),
            },
            sample=[
                {
                    "meeting_date": row.meeting_date.isoformat(),
                    "m0": row.m0.isoformat(),
                    "m1": row.m1.isoformat(),
                    "y0": row.y0,
                    "y1": row.y1,
                    "features": mlx.json_safe(row.features),
                }
                for row in rows
            ],
            spec_path=(
                "docs/superpowers/specs/"
                "2026-10-20-cpi-key-rate-pipeline-design.md"
            ),
            enabled=True,
        )
        logger.log_metrics(
            {"summary": payload["summary"], "pairs": payload["pairs"]}
        )
        logger.log_predictions(
            [
                mlx.PredictionRow(
                    model=item.model,
                    window=item.target,
                    target_date=item.target_month,
                    predicted=item.y_pred,
                    actual=item.y_true,
                    unit="pp",
                    meta={"meeting_date": item.meeting_date.isoformat()},
                )
                for item in predictions
            ]
        )
        logger.log_artifacts(paths)
        logger.finish(
            "finished",
            f"as_of={as_of}; rows={len(rows)}; predictions={len(predictions)}",
        )
    except Exception as exc:  # журнал — best-effort, прогон не роняем
        print(
            f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}",
            flush=True,
        )


# ---------------------------------------------------------------------------
# Прогон
# ---------------------------------------------------------------------------


def run(
    as_of: date,
    out_dir: Path,
    start: int = DEFAULT_START,
    meetings: Optional[list] = None,
    calendar: Optional[PublicationCalendar] = None,
) -> dict:
    """Собрать строки, прогнать walk-forward и записать артефакты."""
    calendar = calendar if calendar is not None else load_calendar()
    all_meetings = load_meetings() if meetings is None else meetings
    selected = [day for day in sorted(set(all_meetings)) if day <= as_of]
    if not selected:
        raise SystemExit(f"нет заседаний ЦБ по {as_of.isoformat()}")
    rows = build_rows(selected, calendar, as_of)
    assert_no_lookahead(rows, calendar)
    predictions = walk_forward(rows, start)
    result = evaluate(predictions)
    summary = result["summary"]
    pairs = result["pairs"]
    payload = {
        "as_of": as_of.isoformat(),
        "start": start,
        "seed": SEED,
        "n_meetings": len(rows),
        "n_predictions": len(predictions),
        "first_meeting": rows[0].meeting_date.isoformat(),
        "last_meeting": rows[-1].meeting_date.isoformat(),
        "features": list(FEATURE_NAMES),
        "models": list(MODELS),
        "summary": summary,
        "pairs": pairs,
    }

    out_dir.mkdir(parents=True, exist_ok=True)
    predictions_path = out_dir / PREDICTIONS_NAME
    result_path = out_dir / RESULT_NAME
    report_path = out_dir / REPORT_NAME
    write_predictions(predictions_path, predictions)
    write_json(result_path, payload)
    write_report(report_path, payload)

    if os.environ.get("ML_LOG") == "1":
        _log_experiment(
            payload,
            rows,
            predictions,
            [predictions_path, result_path, report_path],
            as_of,
        )

    print(
        f"Заседаний {len(rows)} ({rows[0].meeting_date}…"
        f"{rows[-1].meeting_date}), прогнозов {len(predictions)}, "
        f"start={start}",
        flush=True,
    )
    for model, by_target in summary.items():
        for target, item in by_target.items():
            if item.get("n"):
                print(
                    f"  {model}/{target}: n={item['n']}, "
                    f"RMSE {item['rmse']:.6f}, MAE {item['mae']:.6f}",
                    flush=True,
                )
    for name, item in pairs.items():
        if item.get("n"):
            print(
                f"  {name}: Δ {item['delta']:+.6f} "
                f"[{item['delta_ci'][0]:+.6f}; {item['delta_ci'][1]:+.6f}], "
                f"DM p={item['dm_p']:.4f}, MDE {item['mde_ratio']:.4f}",
                flush=True,
            )
    print(
        f"Артефакты: {predictions_path}, {result_path}, {report_path}",
        flush=True,
    )
    return payload


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Stage 1 CPI nowcast M0/M0+1 по календарю публикаций "
            "(пункт key-rate/phase-4-cpi-key-rate-pipeline)"
        )
    )
    parser.add_argument(
        "--as-of", default=None, help="дата среза, YYYY-MM-DD (по умолчанию — сегодня)"
    )
    parser.add_argument(
        "--out-dir",
        default=str(ROOT / "build" / "cpi-pipeline-stage1"),
        help="каталог артефактов",
    )
    parser.add_argument("--start", type=int, default=DEFAULT_START)
    args = parser.parse_args(argv)
    try:
        as_of = (
            date.fromisoformat(args.as_of) if args.as_of else date.today()
        )
    except ValueError:
        print(f"ошибка --as-of: {args.as_of!r}", file=sys.stderr)
        return 2
    run(as_of=as_of, out_dir=Path(args.out_dir), start=args.start)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
