#!/usr/bin/env python3
"""Экспорт прогонов MLflow в ClickHouse `ml_runs`/`ml_predictions`.

`ml_experiment.py` пишет прогон в MLflow и в ClickHouse одновременно, но если
ClickHouse был недоступен (или прогон создан только через MLflow), паспорт и
fold-предсказания остаются артефактами `run.json`/`predictions.jsonl`. Этот
скрипт проигрывает артефакты обратно в ClickHouse.

Идемпотентность: `run_id` в паспорте и во всех предсказаниях перезаписывается
каноническим UUID, выведенным из MLflow-run id (`canonical_run_id` из
`ml_experiment`), поэтому повторный экспорт даёт те же строки и
`ReplacingMergeTree` их схлопывает.

Запуск (mlflow импортируется лениво, только в `main`):

    python3 scripts/mlflow_export.py \
        --tracking-uri http://127.0.0.1:5000 \
        --experiment cpi_nowcast --experiment key_rate_model \
        --since 2026-10-18 --limit 100 --dry-run

Тесты — stdlib-only, без пакета mlflow и без сети:

    python3 scripts/mlflow_export_test.py -v
"""

import argparse
import json
import logging
import os
import re
import sys
import tempfile
from datetime import date, datetime
from pathlib import Path

try:  # запуск из каталога scripts (`python3 scripts/...`)
    from lib.clickhouse import ch_exec
    from lib.ml_experiment import canonical_run_id, insert_sql
except ImportError:  # запуск как пакет из корня репозитория
    from scripts.lib.clickhouse import ch_exec
    from scripts.lib.ml_experiment import canonical_run_id, insert_sql

log = logging.getLogger(__name__)

DEFAULT_TRACKING_URI = "http://127.0.0.1:5000"
DEFAULT_EXPERIMENTS = ("cpi_nowcast", "key_rate_model")

# RUNNING (и прочие незавершённые) пропускаются: артефактов у них ещё нет.
EXPORTED_STATUSES = ("FINISHED", "FAILED")

# Поля-таймстемпы схем `ml_runs`/`ml_predictions`: ClickHouse `DateTime`
# канонически хранится как `YYYY-MM-DD HH:MM:SS`, без `T` и таймзоны.
TIMESTAMP_FIELDS = ("started_at", "finished_at", "created_at")

# Обязательные текстовые поля паспорта: частичный run.json (например, от
# стороннего логгера) не должен ронять экспорт из-за отсутствующего ключа.
RUN_ROW_DEFAULTS = {
    "experiment": "",
    "run_name": "",
    "status": "",
    "notes": "",
}

# ISO-8601 timezone designator в конце строки: `Z`, `+03:00`, `-0500`.
_TZ_SUFFIX_RE = re.compile(r"(?:Z|[+-]\d{2}:?\d{2})$")

# Код MLflow для отсутствующего артефакта. Только он (или консервативный
# текстовый маркер «No such artifact») означает «артефакта нет», а не сбой
# сервера/сети, который обязан провалить экспорт, а не притвориться пустым.
MISSING_ARTIFACT_ERROR_CODE = "RESOURCE_DOES_NOT_EXIST"


def _is_missing_artifact(exc: Exception) -> bool:
    """`True` только для «артефакт не найден», не для прочих ошибок MLflow.

    `download_artifacts` поднимает `MlflowException` и при отсутствии
    артефакта, и при HTTP-ошибке/таймауте/сбое авторизации. Различаем их по
    `error_code`; запасная проверка текста узкая («no such artifact»), чтобы
    не спутать сетевую ошибку с отсутствующим файлом.
    """
    if getattr(exc, "error_code", None) == MISSING_ARTIFACT_ERROR_CODE:
        return True
    return "no such artifact" in str(exc).lower()


def ch_datetime(value) -> str:
    """Приводит таймстемп к виду ClickHouse `YYYY-MM-DD HH:MM:SS`.

    `T` заменяется пробелом, суффикс таймзоны (`Z`, `+03:00`, `-0500`)
    отбрасывается, дробная часть секунд отсекается первыми 19 символами.
    Таймзона **не конвертируется**: `10:00:00Z` и `10:00:00+03:00` дают одно
    и то же настенное время `10:00:00` — так требует схема `DateTime`
    ClickHouse (спека §6.4); третьи стороны с `Z`/`+03:00` теряют смещение.
    `None` и пустая строка возвращаются как есть: `ml_experiment` может не
    заполнить необязательное поле, и подставлять выдуманную дату нельзя.
    """
    if value is None:
        return value
    text = str(value).strip()
    if not text:
        return text
    text = text.replace("T", " ")
    match = _TZ_SUFFIX_RE.search(text)
    if match:
        text = text[:match.start()].rstrip()
    return text[:19]


def parse_jsonl(text: str) -> list:
    """Разбирает `predictions.jsonl`: один JSON-объект на строку.

    Пустые и пробельные строки пропускаются. Невалидный JSON или строка не
    объектом — `ValueError` с номером строки: это порча артефакта, и
    экспорт такого прогона должен закончиться кодом 1, а не молчаливой
    потерей предсказаний.
    """
    rows = []
    for lineno, line in enumerate(text.splitlines(), 1):
        line = line.strip()
        if not line:
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"predictions.jsonl: строка {lineno}: невалидный JSON: {exc.msg}"
            ) from exc
        if not isinstance(row, dict):
            raise ValueError(
                f"predictions.jsonl: строка {lineno}: ожидался JSON-объект, "
                f"получено {type(row).__name__}"
            )
        rows.append(row)
    return rows


def build_run_row(run_json: dict, mlflow_run_id: str) -> dict:
    """Собирает строку `ml_runs` из артефакта `run.json`.

    Копия (входной dict не мутируется), канонический `run_id`, нормализация
    таймстемпов и пустые значения для обязательных ключей. Благодаря
    детерминированному порядку ключей повторная сборка даёт тот же JSON и
    тот же SQL.
    """
    if not isinstance(run_json, dict):
        raise TypeError(
            f"run.json должен быть JSON-объектом, получено "
            f"{type(run_json).__name__}"
        )
    row = dict(run_json)
    row["run_id"] = canonical_run_id(mlflow_run_id)
    for key in TIMESTAMP_FIELDS:
        if key in row:
            row[key] = ch_datetime(row[key])
    for key, default in RUN_ROW_DEFAULTS.items():
        row.setdefault(key, default)
    return row


def build_prediction_rows(predictions: list, mlflow_run_id: str) -> list:
    """Собирает строки `ml_predictions` из `predictions.jsonl`.

    В каждой строке перезаписывается `run_id` (канонический UUID) и
    нормализуется `created_at`; остальные поля сохраняются как есть.
    """
    if not isinstance(predictions, list):
        raise TypeError(
            f"predictions.jsonl должен быть списком строк, получено "
            f"{type(predictions).__name__}"
        )
    run_id = canonical_run_id(mlflow_run_id)
    rows = []
    for index, prediction in enumerate(predictions, 1):
        if not isinstance(prediction, dict):
            raise TypeError(
                f"predictions.jsonl: строка {index}: ожидался JSON-объект, "
                f"получено {type(prediction).__name__}"
            )
        row = dict(prediction)
        row["run_id"] = run_id
        if "created_at" in row:
            row["created_at"] = ch_datetime(row["created_at"])
        rows.append(row)
    return rows


def _read_json_object(path) -> dict:
    """Читает локальный JSON-файл и требует на выходе объект."""
    value = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(
            f"ожидался JSON-объект, получено {type(value).__name__}"
        )
    return value


def _parse_args(argv=None) -> argparse.Namespace:
    """Разбирает CLI; `--since` проверяется до похода в MLflow.

    `--limit` меньше 1 отвергается argparse-ошибкой: `max_results=0` у MLflow
    вернул бы пустой список молча, что выглядело бы как «прогонов нет».
    """
    parser = argparse.ArgumentParser(
        description=(
            "Экспортирует завершённые прогоны MLflow в ClickHouse "
            "ml_runs/ml_predictions."
        ),
    )
    parser.add_argument(
        "--tracking-uri",
        default=(
            os.environ.get("MLFLOW_TRACKING_URI") or DEFAULT_TRACKING_URI
        ),
        help=(
            "MLflow Tracking URI (по умолчанию MLFLOW_TRACKING_URI "
            "или %(default)s)"
        ),
    )
    parser.add_argument(
        "--experiment",
        dest="experiments",
        action="append",
        default=None,
        metavar="NAME",
        help=(
            "имя эксперимента MLflow; можно повторять "
            "(по умолчанию: %s)" % ", ".join(DEFAULT_EXPERIMENTS)
        ),
    )
    parser.add_argument(
        "--since",
        metavar="YYYY-MM-DD",
        help="экспортировать только прогоны, начатые не раньше этой даты",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        metavar="N",
        help=(
            "максимум прогонов на эксперимент, положительное число "
            "(по умолчанию 100)"
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="проверить артефакты и напечатать строки, не трогая ClickHouse",
    )
    args = parser.parse_args(argv)

    if args.experiments is None:
        args.experiments = list(DEFAULT_EXPERIMENTS)
    if args.since is not None:
        try:
            args.since_date = date.fromisoformat(args.since)
        except ValueError:
            parser.error(
                f"--since: ожидается YYYY-MM-DD, получено {args.since!r}"
            )
    else:
        args.since_date = None
    if args.limit < 1:
        parser.error(
            f"--limit: ожидается положительное число, получено {args.limit}"
        )
    return args


def _since_ms(day: date) -> int:
    """Полночь указанной даты в epoch-миллисекундах (локальная зона)."""
    return int(datetime.combine(day, datetime.min.time()).timestamp() * 1000)


def _export_run(client, mlflow_exception, experiment: str,
                mlflow_run_id: str, dry_run: bool):
    """Экспортирует один run; возвращает `(outcome, folds, detail)`.

    `outcome`: `exported`, `would-export` (dry-run), `skipped`
    (нет `run.json` — артефакт создан не нашим хелпером) или `failed`
    (порча артефакта, ошибка MLflow/ClickHouse). Решение — по ruling SDD:
    отсутствующий `run.json` не фатален, битый JSON и ошибка вставки — да.
    """
    with tempfile.TemporaryDirectory(prefix="mlflow-export-") as tmpdir:
        try:
            run_json_path = client.download_artifacts(
                mlflow_run_id, "run.json", tmpdir
            )
        except mlflow_exception as exc:
            # Только «артефакта нет» — это skip. Прочие MlflowException
            # (HTTP 500, таймаут, авторизация) — сбой прогона, exit 1.
            if _is_missing_artifact(exc):
                return "skipped", 0, f"run.json отсутствует: {exc}"
            return "failed", 0, f"run.json не скачан: {exc}"
        except Exception as exc:
            # Осознанная граница изоляции на один прогон: любой неожиданный
            # сбой скачивания помечает failed, но не обрывает экспорт
            # остальных прогонов (итоговый код возврата всё равно 1).
            return "failed", 0, f"run.json не скачан: {exc}"

        try:
            run_json = _read_json_object(run_json_path)
        except (OSError, ValueError) as exc:
            return "failed", 0, f"run.json не разобран: {exc}"

        predictions = []
        try:
            predictions_path = client.download_artifacts(
                mlflow_run_id, "predictions.jsonl", tmpdir
            )
        except mlflow_exception as exc:
            if _is_missing_artifact(exc):
                # Предсказания необязательны: паспорт без фолдов — валидный
                # run. Но это именно отсутствие артефакта, а не сбой сервера.
                log.warning(
                    "%s/%s: predictions.jsonl отсутствует — 0 фолдов",
                    experiment, mlflow_run_id,
                )
                predictions_path = None
            else:
                return "failed", 0, f"predictions.jsonl не скачан: {exc}"
        except Exception as exc:
            # Та же граница изоляции, что и для run.json: сбой одного
            # прогона не должен останавливать остальные.
            return "failed", 0, f"predictions.jsonl не скачан: {exc}"
        if predictions_path is not None:
            try:
                predictions = parse_jsonl(
                    Path(predictions_path).read_text(encoding="utf-8")
                )
            except (OSError, ValueError) as exc:
                return "failed", 0, f"predictions.jsonl не разобран: {exc}"

        try:
            run_row = build_run_row(run_json, mlflow_run_id)
            prediction_rows = build_prediction_rows(
                predictions, mlflow_run_id
            )
            predictions_sql = (
                insert_sql("ml_predictions", prediction_rows)
                if prediction_rows else ""
            )
            run_sql = insert_sql("ml_runs", [run_row])
        except (TypeError, ValueError) as exc:
            return "failed", 0, f"строки не собраны: {exc}"

        if dry_run:
            print(
                f"{experiment} {mlflow_run_id} {len(prediction_rows)}"
            )
            return "would-export", len(prediction_rows), ""

        try:
            # Порядок как в `ml_experiment.finish`: сначала предсказания,
            # затем паспорт — паспорт не должен появиться без фолдов.
            if predictions_sql:
                ch_exec(predictions_sql)
            ch_exec(run_sql)
        except Exception as exc:
            # Осознанная граница: ch_exec контрактно поднимает ClickHouseError,
            # транспорт может прийти как OSError/HTTPException, а тестовый
            # двойник — как угодно. Любой сбой = failed для этого прогона,
            # остальные прогоны продолжают экспортироваться.
            return "failed", 0, f"вставка в ClickHouse не удалась: {exc}"
        return "exported", len(prediction_rows), ""


def main(argv=None) -> int:
    """CLI экспорта; 0 — все прогоны экспортированы/пропущены, 1 — ошибка."""
    args = _parse_args(argv)

    # Ленивый импорт: модуль и тесты обязаны работать без установленного mlflow.
    try:
        from mlflow import MlflowClient
        from mlflow.exceptions import MlflowException
    except Exception as exc:
        print(f"mlflow недоступен: {exc}", file=sys.stderr)
        return 1

    try:
        client = MlflowClient(tracking_uri=args.tracking_uri)
    except Exception as exc:
        print(f"MLflow-клиент не создан: {exc}", file=sys.stderr)
        return 1

    since_ms = _since_ms(args.since_date) if args.since_date else None
    exported = 0
    exported_predictions = 0
    skipped = 0
    failed = 0

    for experiment in args.experiments:
        try:
            registered = client.get_experiment_by_name(experiment)
        except Exception as exc:
            log.error(
                "эксперимент %s: get_experiment_by_name: %s", experiment, exc
            )
            failed += 1
            continue
        if registered is None:
            log.warning("эксперимент %s не найден — пропущен", experiment)
            skipped += 1
            continue

        # `--since` уходит и на сервер: без filter_string `--limit` отрезал бы
        # старые прогоны ДО клиентского фильтра, и выборка оказалась бы пустой.
        filter_string = (
            f"attributes.start_time >= {since_ms}"
            if since_ms is not None else ""
        )
        try:
            # DESC — чтобы при упоре в --limit повторные экспорты сходились на
            # самых новых прогонах, а не вечно отдавали один и тот же старый
            # батч. `limit` с сервера остаётся ограничением выборки.
            runs = client.search_runs(
                [registered.experiment_id],
                filter_string=filter_string,
                order_by=["attributes.start_time DESC"],
                max_results=args.limit,
            )
        except Exception as exc:
            log.error("эксперимент %s: search_runs: %s", experiment, exc)
            failed += 1
            continue

        if len(runs) == args.limit:
            # Сервер не отдаёт признак «есть ещё»: ровно limit строк — это
            # либо точное попадание, либо усечение. Предупреждаем всегда,
            # чтобы «exported N runs» не выглядел полным экспортом.
            log.warning(
                "эксперимент %s: выбрано %d прогонов = --limit, выборка может "
                "быть обрезана (увеличьте --limit или используйте --since)",
                experiment, len(runs),
            )

        for run in runs:
            info = run.info
            mlflow_run_id = info.run_id
            if info.status not in EXPORTED_STATUSES:
                log.warning(
                    "%s/%s: статус %s — пропущен",
                    experiment, mlflow_run_id, info.status,
                )
                skipped += 1
                continue
            # Страховка поверх серверного filter_string: клиент мог его
            # проигнорировать (фейк/старая версия) — тогда skip считаем явно.
            if since_ms is not None and int(info.start_time or 0) < since_ms:
                log.info(
                    "%s/%s: start_time %s раньше --since — пропущен",
                    experiment, mlflow_run_id, info.start_time,
                )
                skipped += 1
                continue

            outcome, folds, detail = _export_run(
                client, MlflowException, experiment, mlflow_run_id,
                args.dry_run,
            )
            if outcome in ("exported", "would-export"):
                exported += 1
                exported_predictions += folds
            elif outcome == "skipped":
                log.warning(
                    "%s/%s: %s — пропущен", experiment, mlflow_run_id, detail
                )
                skipped += 1
            else:
                log.error(
                    "%s/%s: %s", experiment, mlflow_run_id, detail
                )
                failed += 1

    if args.dry_run:
        # В dry-run ничего не записано: отдельная формулировка, чтобы строка
        # «exported N runs» не выдавала предпросмотр за реальный экспорт.
        print(
            f"would export {exported} runs, {exported_predictions} "
            f"predictions; skipped {skipped}"
        )
    else:
        print(
            f"exported {exported} runs, {exported_predictions} predictions; "
            f"skipped {skipped}"
        )
    if failed:
        print(f"не экспортировано прогонов: {failed}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raise SystemExit(main())
