#!/usr/bin/env python3
"""Тесты scripts/mlflow_export.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/mlflow_export_test.py -v

Набор ОФЛАЙН: настоящий пакет `mlflow` не нужен, клиент подменяется фейком
через `patch.dict(sys.modules, ...)`, `ch_exec` — `unittest.mock`. Артефакты
(`run.json`, `predictions.jsonl`) пишутся в `tempfile`, сеть не используется.
"""

import io
import json
import os
import shutil
import sys
import tempfile
import types
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import mlflow_export as mx
from lib.ml_experiment import canonical_run_id as experiment_canonical_run_id
from lib.ml_experiment import insert_sql

# 32 hex-символа — ровно то, что MLflow кладёт в `run.info.run_id`.
MLFLOW_RUN_ID = "0123456789abcdef0123456789abcdef"
CANONICAL_RUN_ID = "01234567-89ab-cdef-0123-456789abcdef"
OTHER_MLFLOW_RUN_ID = "fedcba9876543210fedcba9876543210"
OTHER_CANONICAL_RUN_ID = "fedcba98-7654-3210-fedc-ba9876543210"

# Фикстура повторяет форму `ml_experiment._run_row`/`_prediction_rows`.
RUN_JSON = {
    "run_id": "8f14e45f-ea2d-41f4-9b3b-3b3b3b3b3b3b",
    "experiment": "cpi_nowcast",
    "run_name": "test-run",
    "status": "finished",
    "author": "tester",
    "git_sha": "abc123",
    "git_branch": "main",
    "git_dirty": 0,
    "config_hash": "hash",
    "config_json": "{}",
    "env_json": '{"python":"3.12"}',
    "data_asof": "2026-09-30",
    "sample_hash": "sample",
    "sample_meta_json": "{}",
    "spec_path": "docs/spec.md",
    "metrics_json": "{}",
    "artifacts_json": "[]",
    "started_at": "2026-10-18T10:00:00Z",
    "finished_at": "2026-10-18T10:05:00+00:00",
    "duration_sec": 300.0,
    "notes": "",
}

PREDICTIONS = [
    {
        "run_id": "local-only-id",
        "experiment": "cpi_nowcast",
        "model": "catboost_direct",
        "window": "start=24",
        "target_date": "2026-09-01",
        "predicted": 8.1,
        "actual": 8.0,
        "error": 0.1,
        "unit": "pp",
        "meta_json": "{}",
        "created_at": "2026-10-18T10:05:00Z",
    },
    {
        "run_id": "local-only-id",
        "experiment": "cpi_nowcast",
        "model": "catboost_direct",
        "window": "start=24",
        "target_date": "2026-10-01",
        "predicted": 7.9,
        "actual": 7.8,
        "error": 0.1,
        "unit": "pp",
        "meta_json": "{}",
        "created_at": "2026-10-18T10:05:00+03:00",
    },
]

# Граница `--since` — полночь 2026-10-18 в локальной зоне, как в `_since_ms`.
BEFORE_SINCE_MS = int(datetime(2026, 10, 17, 23, 59).timestamp() * 1000)
AFTER_SINCE_MS = int(datetime(2026, 10, 18, 0, 1).timestamp() * 1000)


class FakeMlflowException(Exception):
    """Двойник `mlflow.exceptions.MlflowException` с `error_code`."""

    def __init__(self, message, error_code=None):
        super().__init__(message)
        self.error_code = error_code


class FakeInfo:
    def __init__(self, run_id, status="FINISHED", start_time=AFTER_SINCE_MS,
                 end_time=None, run_name="run"):
        self.run_id = run_id
        self.status = status
        self.start_time = start_time
        self.end_time = end_time
        self.run_name = run_name


class FakeRun:
    def __init__(self, info):
        self.info = info


class FakeExperiment:
    def __init__(self, experiment_id):
        self.experiment_id = experiment_id


class FakeClient:
    """Офлайн-двойник `MlflowClient`: артефакты копирует из `artifacts`."""

    def __init__(self, experiments=None, runs=None, artifacts=None,
                 missing=(), errors=None):
        self.experiments = experiments or {}
        self.runs = runs or {}
        self.artifacts = artifacts or {}
        # `(run_id, artifact_path)` → `MlflowException` «артефакта нет».
        self.missing = set(missing)
        # `(run_id, artifact_path)` → произвольное исключение скачивания.
        self.errors = errors or {}
        self.tracking_uri = None
        self.names = []
        self.search_calls = []
        self.download_calls = []

    def get_experiment_by_name(self, name):
        self.names.append(name)
        return self.experiments.get(name)

    def search_runs(self, experiment_ids, filter_string="", order_by=None,
                    max_results=None):
        self.search_calls.append((
            list(experiment_ids), filter_string, order_by, max_results,
        ))
        runs = []
        for experiment_id in experiment_ids:
            runs.extend(self.runs.get(experiment_id, []))
        # Сервер MLflow применяет order_by/max_results; двойник повторяет это,
        # иначе тест на --limit видел бы больше строк, чем реальный клиент.
        if order_by:
            descending = any("DESC" in str(item) for item in order_by)
            runs.sort(key=lambda run: run.info.start_time or 0,
                      reverse=descending)
        if max_results is not None:
            runs = runs[:max_results]
        return runs

    def download_artifacts(self, run_id, artifact_path, dst_path):
        self.download_calls.append((run_id, artifact_path, dst_path))
        key = (run_id, artifact_path)
        if key in self.errors:
            raise self.errors[key]
        if key in self.missing:
            raise FakeMlflowException(
                f"No such artifact: {artifact_path!r} for run {run_id}",
                error_code="RESOURCE_DOES_NOT_EXIST",
            )
        source = Path(self.artifacts[run_id]) / artifact_path
        target = Path(dst_path) / artifact_path
        shutil.copyfile(source, target)
        return str(target)


def _fake_mlflow(client):
    """`sys.modules`-двойник пакета mlflow с работоспособным `MlflowClient`."""
    module = types.ModuleType("mlflow")
    exceptions = types.ModuleType("mlflow.exceptions")
    exceptions.MlflowException = FakeMlflowException

    def make_client(tracking_uri=None):
        client.tracking_uri = tracking_uri
        return client

    module.MlflowClient = make_client
    module.exceptions = exceptions
    return patch.dict(
        sys.modules, {"mlflow": module, "mlflow.exceptions": exceptions}
    )


class ExportTestBase(unittest.TestCase):
    """Общая песочница: каталог артефактов, фейковый клиент и запуск main."""

    def setUp(self):
        self._tmp = tempfile.TemporaryDirectory(prefix="mlflow-export-test-")
        self.addCleanup(self._tmp.cleanup)
        self.root = Path(self._tmp.name)
        self.artifacts = self.root / "artifacts"
        self.run_dir = self.artifacts / MLFLOW_RUN_ID
        self.run_dir.mkdir(parents=True)
        self._write_artifact("run.json", json.dumps(RUN_JSON))
        # Пустая строка между объектами — легальный JSONL, её надо пропустить.
        self._write_artifact(
            "predictions.jsonl",
            "\n".join(
                [json.dumps(PREDICTIONS[0]), "", json.dumps(PREDICTIONS[1])]
            ) + "\n",
        )

    def _write_artifact(self, name, text, run_id=MLFLOW_RUN_ID):
        directory = self.artifacts / run_id
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_text(text, encoding="utf-8")

    def _client(self, status="FINISHED", start_time=AFTER_SINCE_MS,
                experiments=None, runs=None, **kwargs):
        info = FakeInfo(
            MLFLOW_RUN_ID, status=status, start_time=start_time
        )
        client = FakeClient(
            experiments=(
                {"cpi_nowcast": FakeExperiment("exp-1")}
                if experiments is None else experiments
            ),
            runs=(
                {"exp-1": [FakeRun(info)]} if runs is None else runs
            ),
            artifacts={MLFLOW_RUN_ID: str(self.run_dir)},
            **kwargs,
        )
        return client

    def _main(self, client, argv, ch_exec=None):
        """Запуск `main` без сети: фейковый mlflow и перехваченный вывод."""
        stdout = io.StringIO()
        stderr = io.StringIO()
        patcher = (
            patch.object(mx, "ch_exec") if ch_exec is None
            else patch.object(mx, "ch_exec", side_effect=ch_exec)
        )
        with _fake_mlflow(client), patcher, \
                redirect_stdout(stdout), redirect_stderr(stderr):
            code = mx.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()


class CanonicalRunIdTest(unittest.TestCase):
    def test_hex_id_becomes_dashed_uuid(self):
        self.assertEqual(mx.canonical_run_id(MLFLOW_RUN_ID), CANONICAL_RUN_ID)

    def test_single_implementation_with_ml_experiment(self):
        self.assertIs(mx.canonical_run_id, experiment_canonical_run_id)

    def test_non_hex_falls_back_deterministically(self):
        first = mx.canonical_run_id("not-a-run-id")
        second = mx.canonical_run_id("not-a-run-id")
        self.assertEqual(first, second)
        self.assertNotEqual(first, mx.canonical_run_id("another-id"))
        self.assertEqual(str(uuid.UUID(first)), first)


class ChDatetimeTest(unittest.TestCase):
    def test_t_and_z_are_removed(self):
        self.assertEqual(
            mx.ch_datetime("2026-10-18T10:00:00Z"), "2026-10-18 10:00:00"
        )

    def test_positive_offset_is_stripped(self):
        self.assertEqual(
            mx.ch_datetime("2026-10-18T10:00:00+03:00"),
            "2026-10-18 10:00:00",
        )

    def test_negative_offset_is_stripped(self):
        self.assertEqual(
            mx.ch_datetime("2026-10-18T10:00:00-05:00"),
            "2026-10-18 10:00:00",
        )

    def test_fractional_seconds_are_truncated(self):
        self.assertEqual(
            mx.ch_datetime("2026-10-18T10:00:00.123456+00:00"),
            "2026-10-18 10:00:00",
        )

    def test_date_only_passes_through(self):
        self.assertEqual(mx.ch_datetime("2026-10-18"), "2026-10-18")

    def test_none_and_empty_pass_through(self):
        self.assertIsNone(mx.ch_datetime(None))
        self.assertEqual(mx.ch_datetime(""), "")


class ParseJsonlTest(unittest.TestCase):
    def test_skips_empty_and_whitespace_lines(self):
        text = '{"a": 1}\n\n   \n{"b": 2}\n'
        self.assertEqual(mx.parse_jsonl(text), [{"a": 1}, {"b": 2}])

    def test_empty_text_is_empty_list(self):
        self.assertEqual(mx.parse_jsonl(""), [])

    def test_malformed_json_raises_with_line_number(self):
        with self.assertRaises(ValueError) as ctx:
            mx.parse_jsonl('{"a": 1}\n{oops\n')
        self.assertIn("строка 2", str(ctx.exception))

    def test_non_object_line_raises(self):
        with self.assertRaises(ValueError) as ctx:
            mx.parse_jsonl("[1, 2]\n")
        self.assertIn("строка 1", str(ctx.exception))


class BuildRunRowTest(unittest.TestCase):
    def test_overwrites_run_id_and_normalizes_timestamps(self):
        row = mx.build_run_row(dict(RUN_JSON), MLFLOW_RUN_ID)
        self.assertEqual(row["run_id"], CANONICAL_RUN_ID)
        self.assertEqual(row["started_at"], "2026-10-18 10:00:00")
        self.assertEqual(row["finished_at"], "2026-10-18 10:05:00")

    def test_preserves_other_fields_and_does_not_mutate_input(self):
        source = dict(RUN_JSON)
        row = mx.build_run_row(source, MLFLOW_RUN_ID)
        self.assertEqual(source, RUN_JSON)
        for key, value in RUN_JSON.items():
            if key not in ("run_id", "started_at", "finished_at"):
                self.assertEqual(row[key], value)

    def test_missing_required_keys_get_empty_defaults(self):
        row = mx.build_run_row({}, MLFLOW_RUN_ID)
        self.assertEqual(row["run_id"], CANONICAL_RUN_ID)
        for key in ("experiment", "run_name", "status", "notes"):
            self.assertEqual(row[key], "")

    def test_repeat_build_is_identical(self):
        first = mx.build_run_row(dict(RUN_JSON), MLFLOW_RUN_ID)
        second = mx.build_run_row(dict(RUN_JSON), MLFLOW_RUN_ID)
        self.assertEqual(first, second)
        self.assertEqual(
            insert_sql("ml_runs", [first]), insert_sql("ml_runs", [second])
        )

    def test_non_object_run_json_is_rejected(self):
        with self.assertRaises(TypeError):
            mx.build_run_row(["not", "a", "row"], MLFLOW_RUN_ID)


class BuildPredictionRowsTest(unittest.TestCase):
    def test_overwrites_run_id_and_normalizes_created_at(self):
        rows = mx.build_prediction_rows(PREDICTIONS, MLFLOW_RUN_ID)
        self.assertEqual(len(rows), 2)
        for row in rows:
            self.assertEqual(row["run_id"], CANONICAL_RUN_ID)
        self.assertEqual(rows[0]["created_at"], "2026-10-18 10:05:00")
        self.assertEqual(rows[1]["created_at"], "2026-10-18 10:05:00")

    def test_preserves_other_fields_and_does_not_mutate_input(self):
        source = [dict(PREDICTIONS[0])]
        rows = mx.build_prediction_rows(source, MLFLOW_RUN_ID)
        self.assertEqual(source, [PREDICTIONS[0]])
        for key, value in PREDICTIONS[0].items():
            if key not in ("run_id", "created_at"):
                self.assertEqual(rows[0][key], value)

    def test_empty_predictions_stay_empty(self):
        self.assertEqual(mx.build_prediction_rows([], MLFLOW_RUN_ID), [])

    def test_repeat_build_is_identical(self):
        first = mx.build_prediction_rows(PREDICTIONS, MLFLOW_RUN_ID)
        second = mx.build_prediction_rows(PREDICTIONS, MLFLOW_RUN_ID)
        self.assertEqual(first, second)
        self.assertEqual(
            insert_sql("ml_predictions", first),
            insert_sql("ml_predictions", second),
        )

    def test_non_dict_rows_are_rejected(self):
        with self.assertRaises(TypeError):
            mx.build_prediction_rows([{"a": 1}, 42], MLFLOW_RUN_ID)


class DryRunTest(ExportTestBase):
    def test_dry_run_prints_line_and_does_not_touch_clickhouse(self):
        client = self._client()
        ch_exec = unittest.mock.Mock()
        code, out, _err = self._main(
            client, ["--experiment", "cpi_nowcast", "--dry-run"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 0)
        self.assertFalse(ch_exec.called)
        self.assertIn(
            f"cpi_nowcast {MLFLOW_RUN_ID} 2", out.splitlines()
        )
        self.assertIn(
            "would export 1 runs, 2 predictions; skipped 0", out
        )

    def test_dry_run_still_downloads_and_validates_artifacts(self):
        client = self._client()
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast", "--dry-run"]
        )
        self.assertEqual(code, 0)
        downloaded = {path for _run_id, path, _dst in client.download_calls}
        self.assertEqual(downloaded, {"run.json", "predictions.jsonl"})


class MainExportTest(ExportTestBase):
    def test_inserts_predictions_then_run_with_canonical_ids(self):
        client = self._client()
        calls = []
        code, out, _err = self._main(
            client,
            ["--experiment", "cpi_nowcast"],
            ch_exec=calls.append,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 2)
        self.assertTrue(
            calls[0].startswith("INSERT INTO ml_predictions FORMAT JSONEachRow\n")
        )
        self.assertTrue(
            calls[1].startswith("INSERT INTO ml_runs FORMAT JSONEachRow\n")
        )

        prediction_rows = [
            json.loads(line)
            for line in calls[0].partition("\n")[2].splitlines()
        ]
        self.assertEqual(len(prediction_rows), 2)
        self.assertEqual(
            {row["run_id"] for row in prediction_rows}, {CANONICAL_RUN_ID}
        )
        self.assertEqual(
            prediction_rows[0]["created_at"], "2026-10-18 10:05:00"
        )

        run_row = json.loads(calls[1].partition("\n")[2])
        self.assertEqual(run_row["run_id"], CANONICAL_RUN_ID)
        self.assertEqual(run_row["experiment"], "cpi_nowcast")
        self.assertEqual(run_row["started_at"], "2026-10-18 10:00:00")
        self.assertEqual(run_row["finished_at"], "2026-10-18 10:05:00")
        self.assertIn("exported 1 runs, 2 predictions; skipped 0", out)

    def test_repeat_export_builds_identical_sql(self):
        client = self._client()
        first_calls = []
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=first_calls.append,
        )
        self.assertEqual(code, 0)
        second_calls = []
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=second_calls.append,
        )
        self.assertEqual(code, 0)
        self.assertEqual(first_calls, second_calls)

    def test_missing_predictions_still_exports_run(self):
        client = self._client(missing={(MLFLOW_RUN_ID, "predictions.jsonl")})
        calls = []
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=calls.append,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 1)
        self.assertTrue(calls[0].startswith("INSERT INTO ml_runs FORMAT JSONEachRow"))


class FilterTest(ExportTestBase):
    def test_running_run_is_skipped_with_warning_and_not_downloaded(self):
        client = self._client(status="RUNNING")
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 0)
        self.assertFalse(ch_exec.called)
        self.assertEqual(client.download_calls, [])
        self.assertIn("RUNNING", err)

    def test_killed_run_is_skipped(self):
        client = self._client(status="KILLED")
        ch_exec = unittest.mock.Mock()
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 0)
        self.assertFalse(ch_exec.called)

    def test_failed_run_is_exported(self):
        client = self._client(status="FAILED")
        calls = []
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=calls.append,
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(calls), 2)

    def test_since_filters_client_side(self):
        info_old = FakeInfo(
            MLFLOW_RUN_ID, start_time=BEFORE_SINCE_MS
        )
        info_new = FakeInfo(
            OTHER_MLFLOW_RUN_ID, start_time=AFTER_SINCE_MS
        )
        self._write_artifact("run.json", json.dumps(RUN_JSON),
                             run_id=OTHER_MLFLOW_RUN_ID)
        self._write_artifact("predictions.jsonl", "",
                             run_id=OTHER_MLFLOW_RUN_ID)
        client = FakeClient(
            experiments={"cpi_nowcast": FakeExperiment("exp-1")},
            runs={"exp-1": [FakeRun(info_old), FakeRun(info_new)]},
            artifacts={
                MLFLOW_RUN_ID: str(self.run_dir),
                OTHER_MLFLOW_RUN_ID: str(self.artifacts / OTHER_MLFLOW_RUN_ID),
            },
        )
        calls = []
        code, out, _err = self._main(
            client,
            ["--experiment", "cpi_nowcast", "--since", "2026-10-18"],
            ch_exec=calls.append,
        )
        self.assertEqual(code, 0)
        downloaded_run_ids = {run_id for run_id, _p, _d in client.download_calls}
        self.assertEqual(downloaded_run_ids, {OTHER_MLFLOW_RUN_ID})
        self.assertEqual(len(calls), 1)
        run_row = json.loads(calls[0].partition("\n")[2])
        self.assertEqual(run_row["run_id"], OTHER_CANONICAL_RUN_ID)
        # Клиентский skip старого прогона обязан попасть в summary (M1).
        self.assertIn("skipped 1", out)

    def test_since_passes_server_side_filter_string(self):
        # I3: --since обязан уйти в search_runs, иначе --limit отрежет
        # старые прогоны до клиентского фильтра и выборка будет пустой.
        client = self._client()
        code, _out, _err = self._main(
            client,
            ["--experiment", "cpi_nowcast", "--since", "2026-10-18",
             "--dry-run"],
        )
        self.assertEqual(code, 0)
        self.assertEqual(len(client.search_calls), 1)
        _ids, filter_string, order_by, limit = client.search_calls[0]
        expected_ms = int(datetime(2026, 10, 18).timestamp() * 1000)
        self.assertEqual(
            filter_string, f"attributes.start_time >= {expected_ms}"
        )
        self.assertEqual(order_by, ["attributes.start_time DESC"])
        self.assertEqual(limit, 100)

    def test_search_uses_order_by_and_limit(self):
        client = self._client()
        code, _out, _err = self._main(
            client, ["--experiment", "cpi_nowcast", "--limit", "7", "--dry-run"]
        )
        self.assertEqual(code, 0)
        self.assertEqual(
            client.search_calls,
            [(["exp-1"], "", ["attributes.start_time DESC"], 7)],
        )

    def test_limit_truncation_warns_and_keeps_newest_run(self):
        # I2: DESC + предупреждение при len(runs) == --limit. Два прогона,
        # --limit 1: экспортируется самый новый, а stderr сообщает, что
        # выборка могла быть обрезана (иначе повторный экспорт навсегда
        # застревал бы на старом батче).
        self._write_artifact("run.json", json.dumps(RUN_JSON),
                             run_id=OTHER_MLFLOW_RUN_ID)
        self._write_artifact("predictions.jsonl", "",
                             run_id=OTHER_MLFLOW_RUN_ID)
        info_old = FakeInfo(
            MLFLOW_RUN_ID, start_time=BEFORE_SINCE_MS
        )
        info_new = FakeInfo(
            OTHER_MLFLOW_RUN_ID, start_time=AFTER_SINCE_MS
        )
        client = FakeClient(
            experiments={"cpi_nowcast": FakeExperiment("exp-1")},
            runs={"exp-1": [FakeRun(info_old), FakeRun(info_new)]},
            artifacts={
                MLFLOW_RUN_ID: str(self.run_dir),
                OTHER_MLFLOW_RUN_ID: str(self.artifacts / OTHER_MLFLOW_RUN_ID),
            },
        )
        code, out, err = self._main(
            client,
            ["--experiment", "cpi_nowcast", "--limit", "1", "--dry-run"],
        )
        self.assertEqual(code, 0)
        self.assertIn("--limit", err)
        self.assertIn("может быть обрезана", err)
        self.assertIn("would export 1 runs", out)
        self.assertIn(OTHER_MLFLOW_RUN_ID, out)
        self.assertNotIn(MLFLOW_RUN_ID, out)

    def test_unknown_experiment_is_skipped_with_message(self):
        client = self._client(experiments={})
        code, out, err = self._main(
            client, ["--experiment", "missing_experiment", "--dry-run"]
        )
        self.assertEqual(code, 0)
        self.assertEqual(client.search_calls, [])
        self.assertIn("missing_experiment", err)
        # M2: неизвестный эксперимент учитывается в skipped.
        self.assertIn("skipped 1", out)


class DefaultsTest(ExportTestBase):
    def test_default_experiments_and_tracking_uri(self):
        client = FakeClient(
            experiments={
                "cpi_nowcast": FakeExperiment("exp-1"),
                "key_rate_model": FakeExperiment("exp-2"),
            },
            runs={"exp-1": [], "exp-2": []},
        )
        with patch.dict(os.environ, {"MLFLOW_TRACKING_URI": ""}):
            code, _out, _err = self._main(client, ["--dry-run"])
        self.assertEqual(code, 0)
        self.assertEqual(client.names, ["cpi_nowcast", "key_rate_model"])
        self.assertEqual(client.tracking_uri, "http://127.0.0.1:5000")
        # --limit по умолчанию — 100 прогонов на эксперимент.
        self.assertEqual(
            [call[3] for call in client.search_calls], [100, 100]
        )

    def test_tracking_uri_env_default_and_flag_override(self):
        client = FakeClient()
        with patch.dict(
            os.environ, {"MLFLOW_TRACKING_URI": "http://env:5000"}
        ):
            code, _out, _err = self._main(client, ["--dry-run"])
        self.assertEqual(code, 0)
        self.assertEqual(client.tracking_uri, "http://env:5000")

        code, _out, _err = self._main(
            client, ["--dry-run", "--tracking-uri", "http://flag:5000"]
        )
        self.assertEqual(code, 0)
        self.assertEqual(client.tracking_uri, "http://flag:5000")


class ErrorHandlingTest(ExportTestBase):
    def test_missing_run_json_skips_run_and_exits_zero(self):
        client = self._client(missing={(MLFLOW_RUN_ID, "run.json")})
        ch_exec = unittest.mock.Mock()
        code, out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 0)
        self.assertFalse(ch_exec.called)
        self.assertIn("run.json", err)
        self.assertIn("exported 0 runs, 0 predictions; skipped 1", out)

    def test_missing_run_json_by_message_only_is_also_skipped(self):
        # error_code нет, но текст «No such artifact» — консервативный
        # запасной признак отсутствующего артефакта (I2).
        client = self._client(errors={
            (MLFLOW_RUN_ID, "run.json"): FakeMlflowException(
                "No such artifact: run.json"
            ),
        })
        ch_exec = unittest.mock.Mock()
        code, out, _err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 0)
        self.assertFalse(ch_exec.called)
        self.assertIn("skipped 1", out)

    def test_mlflow_server_error_on_run_json_is_fatal(self):
        # I2: HTTP/connection-ошибка MLflow не должна маскироваться под
        # «артефакта нет» и давать exit 0.
        client = self._client(errors={
            (MLFLOW_RUN_ID, "run.json"): FakeMlflowException(
                "HTTP 500: internal error", error_code="INTERNAL_ERROR"
            ),
        })
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("run.json", err)

    def test_generic_exception_on_run_json_is_fatal(self):
        client = self._client(errors={
            (MLFLOW_RUN_ID, "run.json"): RuntimeError("connection reset"),
        })
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("run.json", err)

    def test_mlflow_server_error_on_predictions_is_fatal(self):
        # I1: сбой сервера при скачивании predictions.jsonl — не «0 фолдов».
        client = self._client(errors={
            (MLFLOW_RUN_ID, "predictions.jsonl"): FakeMlflowException(
                "HTTP 502: bad gateway", error_code="INTERNAL_ERROR"
            ),
        })
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("predictions.jsonl", err)

    def test_generic_exception_on_predictions_is_fatal(self):
        client = self._client(errors={
            (MLFLOW_RUN_ID, "predictions.jsonl"): RuntimeError("timed out"),
        })
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("predictions.jsonl", err)

    def test_malformed_run_json_exits_one(self):
        self._write_artifact("run.json", "{not json")
        client = self._client()
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast", "--dry-run"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("run.json", err)

    def test_malformed_predictions_exit_one(self):
        self._write_artifact("predictions.jsonl", '{"a": 1}\n{oops\n')
        client = self._client()
        ch_exec = unittest.mock.Mock()
        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"],
            ch_exec=ch_exec,
        )
        self.assertEqual(code, 1)
        self.assertFalse(ch_exec.called)
        self.assertIn("predictions.jsonl", err)

    def test_clickhouse_insert_error_exits_one(self):
        client = self._client()
        calls = []

        def boom(sql):
            calls.append(sql)
            if len(calls) == 2:
                raise RuntimeError("clickhouse is down")

        code, _out, err = self._main(
            client, ["--experiment", "cpi_nowcast"], ch_exec=boom
        )
        self.assertEqual(code, 1)
        # Порядок как в ml_experiment.finish: предсказания, затем паспорт.
        self.assertEqual(len(calls), 2)
        self.assertTrue(
            calls[0].startswith("INSERT INTO ml_predictions FORMAT JSONEachRow\n")
        )
        self.assertTrue(
            calls[1].startswith("INSERT INTO ml_runs FORMAT JSONEachRow\n")
        )
        self.assertIn("ClickHouse", err)

    def test_invalid_since_is_rejected(self):
        stderr = io.StringIO()
        with redirect_stderr(stderr), self.assertRaises(SystemExit) as ctx:
            mx.main(["--since", "18.10.2026"])
        self.assertEqual(ctx.exception.code, 2)
        self.assertIn("--since", stderr.getvalue())

    def test_non_positive_limit_is_rejected(self):
        # max_results=0 у MLflow вернул бы пустой список молча — CLI
        # отвергает 0 и отрицательные значения до похода в MLflow.
        for value in ("0", "-1"):
            stderr = io.StringIO()
            with redirect_stderr(stderr), \
                    self.assertRaises(SystemExit) as ctx:
                mx.main(["--limit", value])
            self.assertEqual(ctx.exception.code, 2)
            self.assertIn("--limit", stderr.getvalue())

    def test_missing_mlflow_package_exits_one(self):
        with patch.dict(sys.modules, {"mlflow": None}), \
                redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            code = mx.main([])
        self.assertEqual(code, 1)


if __name__ == "__main__":
    unittest.main()
