#!/usr/bin/env python3
"""Gate промоушена ML-моделей и live-оценка shadow/canary.

Пункт роадмапа `infra/4.3` (issue #184), спека
`docs/superpowers/specs/2026-10-20-ml-promotion-live-monitoring-design.md`.

Два режима:

* **backtest** (по умолчанию) — сравнивает champion и challenger на общих фолдах
  из `v_ml_error_by_fold`: RMSE, DM-HLN, 95% блочный бутстрап разницы RMSE и
  robustness-окна. Решение — `promote|reject|iterate`.
* **live** (`--live`) — читает `v_ml_live_accuracy` для shadow/canary-трека и
  решает `promote|hold|rollback|wait` по разрешённым заседаниям.

Скрипт только читает ClickHouse (`ch_query`, тот же транспорт, что у остальных
ML-скриптов). Запись live-прогноза — отдельный writer через ingest; gate ничего
не пишет в ClickHouse и не двигает alias MLflow. Артефакт решения —
`build/ml-gate/<challenger-run>.json`; при заданном `MLFLOW_TRACKING_URI` теги
решения ставятся на run challenger'а.

Запуск — только через `make ml-gate` (env подключает Makefile).
"""

import argparse
import json
import math
import os
import re
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from lib.eval_utils import block_bootstrap_rmse_diff, diebold_mariano, rmse
from lib.clickhouse import ch_query

ROOT = Path(__file__).resolve().parent.parent

# Предрегистрированные константы gate (спека §3.1).
MIN_PRIMARY_FOLDS = 12
MIN_ROBUSTNESS_FOLDS = 6
DM_ALPHA = 0.05
BOOTSTRAP_BLOCK = 4
BOOTSTRAP_REPS = 2000
BOOTSTRAP_SEED = 20261020
MCS_ALPHA = 0.10
MCS_REPS = 2000

# Live-пороги (спека §3.2).
LIVE_SHADOW_MEETINGS = 2
LIVE_CANARY_MEETINGS = 1
LIVE_PROMOTE_ABS_BP = 100.0
LIVE_ROLLBACK_ABS_BP = 200.0

# Основные и robustness-окна по эксперименту. Значения — точные метки
# `v_ml_leaderboard.window`, а не пересчёт: чужой формат должен падать явно.
DEFAULT_WINDOWS = {
    "key_rate_model": ("expanding", ("rolling40",)),
    "cpi_nowcast": ("start=24", ("start=48", "start=24, embargo=1")),
    "cpi_monthly": ("start=24", ("start=48", "start=24, embargo=1")),
}

EXPERIMENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


class GateError(Exception):
    """Ошибка входа/данных gate; текст — без секретов."""


@dataclass(frozen=True)
class Fold:
    """Одна строка `v_ml_error_by_fold`."""

    run_id: str
    experiment: str
    model: str
    window: str
    target_date: str
    error: float
    unit: str


@dataclass(frozen=True)
class WindowResult:
    """Результат сравнения одного окна."""

    window: str
    folds: int
    rmse_champion: float
    rmse_challenger: float
    dm_stat: float
    dm_p: float
    bootstrap_low: float
    bootstrap_high: float
    bootstrap_p: float
    challenger_better: bool
    significant_worse: bool
    promote: bool
    note: str = ""


def _number(value, field: str) -> float:
    """Конечное число из ответа ClickHouse."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GateError(f"{field}: ожидается число, получено {value!r}")
    number = float(value)
    if not math.isfinite(number):
        raise GateError(f"{field}: не конечное число {value!r}")
    return number


def _validate_uuid(value, field: str) -> str:
    """Канонический UUID; защищает SQL от подстановки произвольной строки."""
    try:
        return str(uuid.UUID(str(value)))
    except (ValueError, AttributeError) as error:
        raise GateError(f"{field}: ожидается UUID, получено {value!r}") from error


def _validate_experiment(value: str) -> str:
    """Имя эксперимента — только буквы/цифры/подчёркивание (SQL-литерал)."""
    if not EXPERIMENT_RE.match(value or ""):
        raise GateError(f"experiment: недопустимое имя {value!r}")
    return value


def _sql_literal(value: str) -> str:
    """Строковый SQL-литерал для уже проверенного значения."""
    return "'" + value.replace("'", "''") + "'"


def _date_text(value) -> str:
    """Дата из ответа ClickHouse (`YYYY-MM-DD`) — строка."""
    if isinstance(value, str):
        try:
            return date.fromisoformat(value[:10]).isoformat()
        except ValueError as error:
            raise GateError(f"target_date: не дата {value!r}") from error
    raise GateError(f"target_date: ожидается строка, получено {value!r}")


def load_folds(run_id: str, experiment: str) -> list:
    """Читает fold-предсказания одного run'а из `v_ml_error_by_fold`."""
    sql = (
        "SELECT toString(run_id) AS run_id, experiment, model, window, "
        "toString(target_date) AS target_date, error, unit "
        "FROM v_ml_error_by_fold "
        f"WHERE experiment = {_sql_literal(experiment)} "
        f"AND run_id = {_sql_literal(run_id)} "
        "ORDER BY model, window, target_date\nFORMAT JSON"
    )
    rows = ch_query(sql)
    folds = []
    for row in rows:
        folds.append(
            Fold(
                run_id=str(row.get("run_id", "")),
                experiment=str(row.get("experiment", "")),
                model=str(row.get("model", "")),
                window=str(row.get("window", "")),
                target_date=_date_text(row.get("target_date")),
                error=_number(row.get("error"), "error"),
                unit=str(row.get("unit", "")),
            )
        )
    if not folds:
        raise GateError(
            f"run {run_id}: в v_ml_error_by_fold нет строк — прогон без ML_LOG?"
        )
    return folds


def align(folds_a: list, folds_b: list, model: str, window: str) -> tuple:
    """Общие фолды двух run'ов по `(model, window, target_date)`.

    Возврат `(a, b, note)`: строки в порядке дат. Пустой общий набор — ошибка
    входа, а не «нет данных»: сравнивать нечего, gate не должен молча вернуть
    решение.
    """
    def pick(folds):
        return {
            row.target_date: row
            for row in folds
            if row.model == model and row.window == window
        }

    left, right = pick(folds_a), pick(folds_b)
    common = sorted(set(left) & set(right))
    note = ""
    if not common:
        raise GateError(
            f"нет общих фолдов model={model!r} window={window!r}: "
            f"champion {len(left)}, challenger {len(right)}"
        )
    units = {left[day].unit for day in common} | {right[day].unit for day in common}
    if len(units) != 1:
        raise GateError(f"unit не совпадает на общих фолдах: {sorted(units)}")
    return [left[day] for day in common], [right[day] for day in common], note


def window_result(window: str, champion: list, challenger: list, minimum: int) -> WindowResult:
    """Считает RMSE/DM/бутстрап одного окна и фиксирует решение по нему."""
    if len(champion) < minimum or len(challenger) < minimum:
        return WindowResult(
            window=window,
            folds=min(len(champion), len(challenger)),
            rmse_champion=0.0,
            rmse_challenger=0.0,
            dm_stat=0.0,
            dm_p=1.0,
            bootstrap_low=0.0,
            bootstrap_high=0.0,
            bootstrap_p=1.0,
            challenger_better=False,
            significant_worse=False,
            promote=False,
            note=f"insufficient_folds: {min(len(champion), len(challenger))} < {minimum}",
        )

    errors_champion = [row.error for row in champion]
    errors_challenger = [row.error for row in challenger]
    rmse_champion = rmse(errors_champion)
    rmse_challenger = rmse(errors_challenger)

    # DM: положительная статистика — challenger хуже (потери — квадраты ошибок).
    dm_stat, dm_p = diebold_mariano(errors_challenger, errors_champion, h=1)
    bootstrap = block_bootstrap_rmse_diff(
        errors_challenger,
        errors_champion,
        block=BOOTSTRAP_BLOCK,
        n=BOOTSTRAP_REPS,
        seed=BOOTSTRAP_SEED,
    )

    challenger_better = rmse_challenger < rmse_champion
    significant_worse = (
        not challenger_better
        and dm_stat > 0
        and dm_p < DM_ALPHA
    )
    promote = (
        challenger_better
        and dm_stat < 0
        and dm_p < DM_ALPHA
        and bootstrap.high < 0
    )
    return WindowResult(
        window=window,
        folds=len(champion),
        rmse_champion=rmse_champion,
        rmse_challenger=rmse_challenger,
        dm_stat=dm_stat,
        dm_p=dm_p,
        bootstrap_low=bootstrap.low,
        bootstrap_high=bootstrap.high,
        bootstrap_p=bootstrap.p_value,
        challenger_better=challenger_better,
        significant_worse=significant_worse,
        promote=promote,
    )


def _mcs_included(candidates: list, model: str, window: str) -> tuple:
    """90% MCS по общим фолдам; возвращает `(included, pvalues)`.

    `candidates` — список `(run_id, folds)`, минимум три. Потери — квадраты
    ошибок; выравнивание по общим датам всех кандидатов. `arch` импортируется
    лениво: при двух кандидатах MCS не нужен и пакет не требуется.
    """
    if len(candidates) < 3:
        raise GateError("MCS: нужен минимум третий кандидат (--candidate-run)")

    by_run = {}
    for run_id, folds in candidates:
        by_run[run_id] = {
            row.target_date: row
            for row in folds
            if row.model == model and row.window == window
        }

    common = set.intersection(*(set(rows) for rows in by_run.values()))
    common = sorted(common)
    if len(common) < MIN_PRIMARY_FOLDS:
        raise GateError(
            f"MCS: общих фолдов {len(common)} < {MIN_PRIMARY_FOLDS}"
        )
    order = [run_id for run_id, _ in candidates]
    losses = [
        [by_run[run_id][day].error ** 2 for day in common]
        for run_id in order
    ]

    try:
        import numpy as np
        from arch.bootstrap import MCS
    except Exception as error:  # pragma: no cover - зависит от окружения
        raise GateError(
            "MCS: установите arch (`make ml-gate` добавляет --with arch)"
        ) from error

    mcs = MCS(
        np.asarray(losses, dtype=float),
        size=MCS_ALPHA,
        reps=MCS_REPS,
        block_size=BOOTSTRAP_BLOCK,
        method="R",
        bootstrap="stationary",
        seed=BOOTSTRAP_SEED,
    )
    mcs.compute()
    included = {order[index] for index in mcs.included}
    pvalues = {
        order[index]: float(value)
        for index, value in enumerate(mcs.pvalues)
    }
    return included, pvalues


def decide(primary: WindowResult, robustness: list, mcs: dict = None) -> tuple:
    """Решение по предрегистрированному правилу; возврат `(decision, reason)`."""
    if primary.note.startswith("insufficient_folds"):
        return "iterate", primary.note

    if primary.significant_worse:
        return "reject", "primary_significantly_worse"

    for result in robustness:
        if result.note.startswith("insufficient_folds"):
            continue
        if result.significant_worse:
            return "reject", f"robustness_significantly_worse:{result.window}"

    if primary.promote:
        if mcs is not None and not mcs.get("included", False):
            return "iterate", "not_in_mcs"
        return "promote", "dm_bootstrap_robustness"

    if primary.challenger_better:
        return "iterate", "improvement_not_significant"
    return "iterate", "no_improvement"


def _git_sha() -> str:
    """Текущий git SHA без падения вне репозитория."""
    try:
        import subprocess

        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=ROOT, text=True, stderr=subprocess.DEVNULL
        ).strip()
    except Exception:
        return ""


def _artifact_path(out: Path, name: str) -> Path:
    """Путь артефакта: каталог создаётся, имя — безопасный идентификатор."""
    out.mkdir(parents=True, exist_ok=True)
    return out / f"{name}.json"


def _write_artifact(path: Path, payload: dict) -> None:
    """Атомарная запись JSON: повтор не оставляет половину файла."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)


def _mlflow_tag(run_id: str, tags: dict) -> None:
    """Теги решения на run challenger'а; любая ошибка — warning, не падение."""
    uri = os.environ.get("MLFLOW_TRACKING_URI")
    if not uri:
        return
    try:
        import mlflow
        from mlflow.tracking import MlflowClient
    except Exception as error:
        print(f"warn: mlflow недоступен, теги не записаны: {error}", file=sys.stderr)
        return
    try:
        mlflow.set_tracking_uri(uri)
        client = MlflowClient()
        # Канонический run_id в ClickHouse — UUID с дефисами; MLflow API ждёт
        # исходные 32 hex.
        remote_id = uuid.UUID(run_id).hex
        for key, value in tags.items():
            client.set_tag(remote_id, key, str(value))
    except Exception as error:
        print(f"warn: mlflow-теги не записаны: {error}", file=sys.stderr)


def _backtest(args) -> int:
    experiment = _validate_experiment(args.experiment)
    champion_run = _validate_uuid(args.champion_run, "champion-run")
    challenger_run = _validate_uuid(args.challenger_run, "challenger-run")

    default_window, default_robustness = DEFAULT_WINDOWS.get(experiment, ("", ()))
    primary_window = args.window or default_window
    if not primary_window:
        raise GateError(
            f"experiment {experiment!r}: задайте --window (нет дефолта для этого эксперимента)"
        )
    robustness_windows = list(args.robustness_window or default_robustness)

    champion_folds = load_folds(champion_run, experiment)
    challenger_folds = load_folds(challenger_run, experiment)

    champion, challenger, _ = align(champion_folds, challenger_folds, args.model, primary_window)
    primary = window_result(primary_window, champion, challenger, MIN_PRIMARY_FOLDS)

    robustness = []
    for window in robustness_windows:
        try:
            left, right, _ = align(champion_folds, challenger_folds, args.model, window)
        except GateError as error:
            robustness.append(
                WindowResult(
                    window=window,
                    folds=0,
                    rmse_champion=0.0,
                    rmse_challenger=0.0,
                    dm_stat=0.0,
                    dm_p=1.0,
                    bootstrap_low=0.0,
                    bootstrap_high=0.0,
                    bootstrap_p=1.0,
                    challenger_better=False,
                    significant_worse=False,
                    promote=False,
                    note=str(error),
                )
            )
            continue
        robustness.append(
            window_result(window, left, right, MIN_ROBUSTNESS_FOLDS)
        )

    mcs = None
    if args.mcs:
        candidate_runs = [champion_run, challenger_run] + list(args.candidate_run or [])
        candidate_runs = list(dict.fromkeys(candidate_runs))
        candidates = [
            (run_id, champion_folds if run_id == champion_run else
             challenger_folds if run_id == challenger_run else load_folds(run_id, experiment))
            for run_id in candidate_runs
        ]
        included, pvalues = _mcs_included(candidates, args.model, primary_window)
        mcs = {
            "alpha": MCS_ALPHA,
            "reps": MCS_REPS,
            "candidates": candidate_runs,
            "included": challenger_run in included,
            "included_runs": sorted(included),
            "pvalues": pvalues,
        }

    decision, reason = decide(primary, robustness, mcs)
    payload = {
        "schema_version": 1,
        "mode": "backtest",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "experiment": experiment,
        "champion_run": champion_run,
        "challenger_run": challenger_run,
        "model": args.model,
        "unit": champion[0].unit if champion else "",
        "primary_window": primary_window,
        "robustness_windows": robustness_windows,
        "gate": {
            "dm": True,
            "bootstrap": True,
            "robustness": bool(robustness_windows),
            "mcs": bool(args.mcs),
        },
        "primary": asdict(primary),
        "robustness": [asdict(result) for result in robustness],
        "mcs": mcs,
        "decision": decision,
        "reason": reason,
    }
    path = _artifact_path(Path(args.out), challenger_run)
    _write_artifact(path, payload)
    _mlflow_tag(
        challenger_run,
        {
            "decision": decision,
            "gate": "dm",
            "gate_bootstrap": 1,
            "gate_robustness": int(bool(robustness_windows)),
            "primary_metric": f"rmse_{payload['unit'] or 'unit'}",
            "gate_artifact": str(path),
        },
    )
    print(
        f"{decision}: {reason}; primary {primary_window}: "
        f"RMSE {primary.rmse_champion:.4f} -> {primary.rmse_challenger:.4f}, "
        f"DM p={primary.dm_p:.4g}, CI [{primary.bootstrap_low:.4f}; {primary.bootstrap_high:.4f}]"
    )
    print(f"артефакт: {path}")
    return 0


def _live_rows(metric: str) -> list:
    """Строки live-витрины по одной метрике."""
    sql = (
        "SELECT metric, toString(forecast_date) AS forecast_date, "
        "toString(target_date) AS target_date, abs_error_bp, is_resolved "
        "FROM v_ml_live_accuracy "
        f"WHERE metric = {_sql_literal(metric)} "
        "ORDER BY target_date\nFORMAT JSON"
    )
    return ch_query(sql)


def _latest_by_date(rows: list) -> list:
    """Одна строка на target_date: последний forecast_date.

    `forecast_log` хранит несколько прогнозов одного трека на одно заседание
    (разные `forecast_date`), и live-оценка не должна считать их разными
    заседаниями: rolling MAE идёт по заседаниям, а не по перезапускам writer'а.
    """
    latest = {}
    for row in rows:
        day = _date_text(row.get("target_date"))
        current = latest.get(day)
        if current is None or str(row.get("forecast_date", "")) > str(
            current.get("forecast_date", "")
        ):
            latest[day] = row
    return list(latest.values())


def _live(args) -> int:
    track = args.live_track
    if not track.startswith("cbr_rate:"):
        raise GateError("--live-track: ожидается cbr_rate:<track>")

    shadow_metric = track + ":shadow"
    canary_metric = track + ":canary"
    shadow = sorted(
        _latest_by_date(
            [row for row in _live_rows(shadow_metric) if int(row.get("is_resolved", 0)) == 1]
        ),
        key=lambda row: _date_text(row["target_date"]),
    )
    canary = sorted(
        _latest_by_date(
            [row for row in _live_rows(canary_metric) if int(row.get("is_resolved", 0)) == 1]
        ),
        key=lambda row: _date_text(row["target_date"]),
    )

    shadow_dates = sorted({_date_text(row["target_date"]) for row in shadow})
    canary_dates = sorted({_date_text(row["target_date"]) for row in canary})
    if len(shadow_dates) < LIVE_SHADOW_MEETINGS or len(canary_dates) < LIVE_CANARY_MEETINGS:
        decision, reason = "wait", (
            f"shadow {len(shadow_dates)}/{LIVE_SHADOW_MEETINGS}, "
            f"canary {len(canary_dates)}/{LIVE_CANARY_MEETINGS}"
        )
        canary_abs = None
        rolling_mae = None
    else:
        canary_abs = abs(_number(canary[-1].get("abs_error_bp"), "canary.abs_error_bp"))
        window = sorted(shadow + canary, key=lambda row: _date_text(row["target_date"]))[-3:]
        rolling_mae = sum(
            abs(_number(row.get("abs_error_bp"), "live.abs_error_bp")) for row in window
        ) / len(window)
        if canary_abs > LIVE_ROLLBACK_ABS_BP:
            decision, reason = "rollback", f"canary_abs_bp={canary_abs:.1f} > {LIVE_ROLLBACK_ABS_BP:.0f}"
        elif canary_abs <= LIVE_PROMOTE_ABS_BP and rolling_mae <= LIVE_PROMOTE_ABS_BP:
            decision, reason = "promote", (
                f"canary_abs_bp={canary_abs:.1f}, rolling_mae_bp={rolling_mae:.1f}"
            )
        else:
            decision, reason = "hold", (
                f"canary_abs_bp={canary_abs:.1f}, rolling_mae_bp={rolling_mae:.1f}"
            )

    payload = {
        "schema_version": 1,
        "mode": "live",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "git_sha": _git_sha(),
        "track": track,
        "shadow_metric": shadow_metric,
        "canary_metric": canary_metric,
        "shadow_meetings": len(shadow_dates),
        "canary_meetings": len(canary_dates),
        "canary_abs_bp": canary_abs,
        "rolling_mae_bp": rolling_mae,
        "decision": decision,
        "reason": reason,
    }
    path = _artifact_path(Path(args.out), "live-" + track.replace(":", "-"))
    _write_artifact(path, payload)
    print(f"{decision}: {reason}; артефакт: {path}")
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Gate промоушена ML-моделей и live-оценка shadow/canary "
                    "(infra/4.3)."
    )
    parser.add_argument("--experiment", default="key_rate_model")
    parser.add_argument("--champion-run")
    parser.add_argument("--challenger-run")
    parser.add_argument("--model", default="ordered_logit")
    parser.add_argument("--window", default="")
    parser.add_argument("--robustness-window", action="append", default=[])
    parser.add_argument("--candidate-run", action="append", default=[],
                        help="дополнительный run для MCS (повторяемый)")
    parser.add_argument("--mcs", action="store_true",
                        help="включить 90%% MCS (нужно >=3 кандидатов и arch)")
    parser.add_argument("--live", action="store_true",
                        help="live-режим: shadow/canary по v_ml_live_accuracy")
    parser.add_argument("--live-track", default="cbr_rate:ordered_logit",
                        help="production-метрика трека для live-режима")
    parser.add_argument("--out", default=str(ROOT / "build" / "ml-gate"))
    args = parser.parse_args(argv)

    try:
        if args.live:
            return _live(args)
        if not args.champion_run or not args.challenger_run:
            raise GateError("нужны --champion-run и --challenger-run (или --live)")
        return _backtest(args)
    except GateError as error:
        print(f"ошибка: {error}", file=sys.stderr)
        return 1
    except Exception as error:  # ClickHouse/MLflow/транспорт — без traceback в CLI
        print(f"ошибка: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
