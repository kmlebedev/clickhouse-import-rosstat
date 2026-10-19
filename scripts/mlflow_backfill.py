#!/usr/bin/env python3
"""Backfill существующих прогонов ClickHouse `ml_runs` в MLflow по run_id.

`ml_experiment.py` пишет прогон одновременно в ClickHouse и MLflow, но если
MLflow был недоступен, в ClickHouse паспорт есть, а в MLflow — нет. Скрипт
читает `ml_runs`/`ml_predictions` через `ch_query` (env-пользователь) и
создаёт MLflow-run с тегом `clickhouse_run_id`; повторный запуск идемпотентен
(существующий тег — skip, `--force` создаёт дубль).

Запуск:

    make mlflow-backfill ARGS="--run-id <uuid> --run-id <uuid>"

Скрипт не меняет ClickHouse. `mlflow` импортируется лениво; зависимость
ставит Makefile-цель (`--with mlflow==...`).
"""

import argparse
import json
import math
import os
import re
import sys
from datetime import date, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import ml_experiment as mlx
from extrapolation_aisc import ch_query

DEFAULT_URI = "http://127.0.0.1:5000"
RUN_ID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
PARAM_MAX = 5000


def validate_run_ids(values: list) -> list:
    result = []
    for value in values:
        for item in value.split(","):
            item = item.strip()
            if not item:
                continue
            if not RUN_ID_RE.match(item):
                raise SystemExit(f"некорректный run_id: {item!r}")
            result.append(item.lower())
    if not result:
        raise SystemExit("нужен хотя бы один --run-id")
    return sorted(set(result))


def fetch_runs(run_ids: list) -> dict:
    quoted = ", ".join(f"'{item}'" for item in run_ids)
    rows = ch_query(
        f"SELECT * FROM ml_runs FINAL WHERE run_id IN ({quoted}) FORMAT JSON"
    )
    return {str(row["run_id"]).lower(): row for row in rows}


def fetch_predictions(run_ids: list) -> dict:
    quoted = ", ".join(f"'{item}'" for item in run_ids)
    rows = ch_query(
        "SELECT * FROM ml_predictions FINAL "
        f"WHERE run_id IN ({quoted}) "
        "ORDER BY run_id, model, window, target_date FORMAT JSON"
    )
    grouped = {item: [] for item in run_ids}
    for row in rows:
        grouped.setdefault(str(row["run_id"]).lower(), []).append(row)
    return grouped


def scalar_params(config: dict) -> dict:
    params = {}
    for key, value in (config or {}).items():
        if isinstance(value, bool):
            value = str(value)
        if not isinstance(value, (str, int, float)):
            continue
        if isinstance(value, float) and not math.isfinite(value):
            continue
        name = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(key)).strip("_")
        if not name or len(str(value)) > PARAM_MAX:
            continue
        params[name] = value
    return params


def backfill_one(run_id: str, row: dict, predictions: list, tracking_uri: str,
                 force: bool) -> str:
    import mlflow
    from mlflow.tracking import MlflowClient

    experiment = str(row.get("experiment") or "backfill")
    run_name = str(row.get("run_name") or run_id)
    status = str(row.get("status") or "finished")
    config = {}
    if row.get("config_json"):
        try:
            config = json.loads(row["config_json"])
        except (TypeError, ValueError):
            config = {}
    metrics = {}
    if row.get("metrics_json"):
        try:
            metrics = json.loads(row["metrics_json"])
        except (TypeError, ValueError):
            metrics = {}

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_experiment(experiment)
    client = MlflowClient(tracking_uri=tracking_uri)
    experiment_obj = client.get_experiment_by_name(experiment)
    if experiment_obj is None:
        raise RuntimeError(f"MLflow experiment {experiment!r} не создан")
    existing = client.search_runs(
        [experiment_obj.experiment_id],
        filter_string=f"tags.clickhouse_run_id = '{run_id}'",
        max_results=1,
    )
    if existing and not force:
        return "skip"

    tags = {
        "clickhouse_run_id": run_id,
        "backfill": "force" if force else "1",
        "experiment": experiment,
        "status": status,
        "git_sha": str(row.get("git_sha") or ""),
        "git_branch": str(row.get("git_branch") or ""),
        "git_dirty": str(row.get("git_dirty") or 0),
        "config_hash": str(row.get("config_hash") or ""),
        "sample_hash": str(row.get("sample_hash") or ""),
        "data_asof": str(row.get("data_asof") or ""),
        "spec_path": str(row.get("spec_path") or ""),
    }
    primary = metrics.get("primary_metric") if isinstance(metrics, dict) else None
    if primary is not None:
        tags["primary_metric"] = str(primary)

    mlflow.start_run(run_name=run_name, tags=tags)
    try:
        params = scalar_params(config)
        if params:
            mlflow.log_params(params)
        mlflow.log_text(mlx.canonical_json(row), "run.json")
        if predictions:
            mlflow.log_text(mlx.json_each_row(predictions), "predictions.jsonl")
        flat = mlx.flatten_metrics(metrics)
        if flat:
            mlflow.log_metrics(flat)
        mlflow.end_run(status="FINISHED" if status == "finished" else "FAILED")
    except Exception:
        mlflow.end_run(status="FAILED")
        raise
    return "created"


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Backfill ClickHouse ml_runs into MLflow")
    parser.add_argument("--run-id", action="append", required=True,
                        help="UUID прогона; можно несколько раз или через запятую")
    parser.add_argument("--tracking-uri", default=os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_URI)
    parser.add_argument("--force", action="store_true",
                        help="создать дубль, даже если тег clickhouse_run_id уже есть")
    arguments = parser.parse_args(argv)
    run_ids = validate_run_ids(arguments.run_id)
    runs = fetch_runs(run_ids)
    missing = [item for item in run_ids if item not in runs]
    if missing:
        raise SystemExit(f"в ml_runs не найдены: {', '.join(missing)}")
    predictions = fetch_predictions(run_ids)
    for run_id in run_ids:
        result = backfill_one(
            run_id,
            runs[run_id],
            predictions.get(run_id, []),
            arguments.tracking_uri,
            arguments.force,
        )
        print(f"{run_id}: {result}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
