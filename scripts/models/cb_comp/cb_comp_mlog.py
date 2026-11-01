#!/usr/bin/env python3
"""Паспорт прогона линии 88 групп: `ml_runs`/`ml_predictions` и MLflow — P0.5.

Общий слой `cb_comp_h1_bs_combo` и `cb_comp_live_bottomup`: один прогон =
один конфиг, fold-предсказания по `(model, hN, month)`, `config_hash` /
`sample_hash`, sha256 артефактов. `ML_LOG=1` включает приёмник; без него
`log_run` — no-op. Сбой приёмника не роняет эксперимент: числа и файлы
прогона остаются прежними, а ошибка печатается одной строкой.

`actual` — `TARGET_FULL` (предзарегистрированная истина оценки, normal-54);
`T_LIVE` кладётся в `meta_json`, потому что ключ `ml_predictions`
`(experiment, run_id, model, window, target_date)` допускает одну истину.
"""

import math
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))

from lib import ml_experiment as mlx

EXPERIMENT = "cb_comp_88_groups"
UNIT = "pp"
TRUTH = "target_full"


def _number(value):
    """Конечное число или `None`: numpy-скаляры и `NaN` не должны ломать fold."""
    if isinstance(value, bool) or value is None:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def build_fold_rows(aggregated: dict, truth: dict, months: list, models, horizons,
                    t_live: dict = None, version: str = "", unit: str = UNIT) -> list:
    """Fold-предсказания `PredictionRow` по `(model, horizon, month)`.

    `aggregated[model][h][month]` — прогноз, `truth[h][month]` — факт
    `TARGET_FULL`; `t_live[h][month]` — справочная истина для `meta_json`.
    Пары с отсутствующим или неконечным значением пропускаются: `ml_predictions`
    не допускает `NaN`, а причина пропуска видна по числу строк в отчёте.
    """
    t_live = t_live or {}
    rows = []
    for model in models:
        by_horizon = aggregated.get(model, {})
        for horizon in horizons:
            predictions = by_horizon.get(horizon, {})
            actuals = truth.get(horizon, {})
            live = t_live.get(horizon, {})
            for month in months:
                predicted = _number(predictions.get(month))
                actual = _number(actuals.get(month))
                if predicted is None or actual is None:
                    continue
                rows.append(
                    mlx.PredictionRow(
                        model=model,
                        window=f"h{horizon}",
                        target_date=month,
                        predicted=predicted,
                        actual=actual,
                        unit=unit,
                        meta={
                            "horizon": horizon,
                            "version": version,
                            "normal": 1,
                            "truth": TRUTH,
                            "t_live": _number(live.get(month)),
                        },
                    )
                )
    return rows


def start(run_name: str, config, data_asof, sample_meta, sample, spec_path: str,
          tags: dict = None):
    """`ExperimentRun` или `None`; `ML_LOG` читается здесь же, как в `ml_experiment`."""
    if os.environ.get("ML_LOG") != "1":
        return None
    try:
        return mlx.start_run(
            experiment=EXPERIMENT,
            run_name=run_name,
            config=config,
            data_asof=data_asof,
            sample_meta=sample_meta,
            sample=sample,
            spec_path=spec_path,
            enabled=True,
            tags=tags,
        )
    except Exception as exc:  # приёмник не должен ронять эксперимент
        print(f"Журнал эксперимента не создан: {type(exc).__name__}: {exc}", flush=True)
        return None


def record(logger, folds: list, metrics: dict, artifacts, notes: str,
           tags: dict = None) -> str:
    """Best-effort запись прогона; возвращает `clickhouse`/`local`/`failed`."""
    if logger is None:
        return "disabled"
    try:
        if tags:
            logger.set_tags(tags)
        if folds:
            logger.log_predictions(folds)
        logger.log_metrics(metrics)
        logger.log_artifacts(artifacts)
        return logger.finish("finished", notes)
    except Exception as exc:
        print(f"Журнал эксперимента не записан: {type(exc).__name__}: {exc}", flush=True)
        try:
            logger.finish("failed", f"logging: {type(exc).__name__}: {exc}")
        except Exception:
            pass
        return "failed"


def log_run(run_name: str, config, data_asof, sample_meta, sample, spec_path: str,
            aggregated: dict, truth: dict, months: list, models, horizons,
            t_live: dict = None, version: str = "", metrics: dict = None,
            artifacts=(), notes: str = "", tags: dict = None) -> str:
    """Собирает и пишет паспорт прогона; без `ML_LOG=1` ничего не делает."""
    if os.environ.get("ML_LOG") != "1":
        return "disabled"
    try:
        folds = build_fold_rows(aggregated, truth, months, models, horizons,
                                t_live=t_live, version=version)
    except Exception as exc:
        print(f"Fold-предсказания не собраны: {type(exc).__name__}: {exc}", flush=True)
        return "failed"
    logger = start(run_name, config, data_asof, sample_meta, sample, spec_path, tags=tags)
    if logger is None:
        return "failed"
    return record(logger, folds, metrics or {}, artifacts, notes, tags=tags)
