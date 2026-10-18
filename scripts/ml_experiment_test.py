#!/usr/bin/env python3
"""Тесты scripts/ml_experiment.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/ml_experiment_test.py -v

Набор ОФЛАЙН: `ch_exec` подменяется `unittest.mock`, git-команды выполняются
в текущем рабочем дереве и не требуют сети. ClickHouse для тестов не нужен.
"""

import hashlib
import http.client
import importlib.machinery
import json
import os
import sys
import tempfile
import unittest
import uuid
from datetime import date, datetime
from pathlib import Path
from unittest.mock import patch

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import ml_experiment as me
from extrapolation_aisc import ClickHouseError
from ml_experiment import (
    PredictionRow,
    canonical_json,
    insert_sql,
    json_each_row,
    json_safe,
    sha256_text,
    start_run,
)

# Переменные, которые читает хелпер. Тесты, трогающие поведение по умолчанию,
# обязаны изолировать все: иначе `ML_LOG=1` из окружения разработчика
# включал бы запись там, где тест ждёт «выключено», а `MLFLOW_TRACKING_URI`
# уводил бы прогон в живой MLflow вместо фейка.
_ML_ENV_VARS = ("ML_LOG", "ML_RUN_NAME", "ML_LOCAL_DIR", "MLFLOW_TRACKING_URI")


class _env:
    """Контекст: выставляет ML_* и возвращает прежние значения.

    Отсутствие переменной восстанавливается как отсутствие, а не пустой
    строкой: `start_run` различает «не задано» и «задано пусто».
    """

    def __init__(self, **values):
        self._values = values
        self._saved = {}

    def __enter__(self):
        for name in _ML_ENV_VARS:
            self._saved[name] = os.environ.get(name)
            os.environ.pop(name, None)
        for name, value in self._values.items():
            os.environ[name] = value
        return self

    def __exit__(self, *_exc):
        for name, previous in self._saved.items():
            if previous is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = previous
        return False


def _sql_payload(sql: str) -> tuple:
    """Заголовок INSERT и разобранное тело JSONEachRow."""
    header, _, body = sql.partition("\n")
    return header, [json.loads(line) for line in body.splitlines() if line]


class _ItemScalar:
    """Numpy-подобный скаляр: `json.dumps` его не знает, `.item()` — знает.

    Настоящий numpy в тесты не тянем: хелпер обязан оставаться stdlib-only,
    а контракт здесь — именно протокол `.item()`, а не конкретный тип.
    """

    def __init__(self, value):
        self._value = value

    def item(self):
        return self._value


# Идентификатор, который вернёт фейковый MLflow: 32 hex-символа, как у
# настоящего `run.info.run_id`.
MLFLOW_RUN_ID = "0123456789abcdef0123456789abcdef"
CANONICAL_MLFLOW_RUN_ID = "01234567-89ab-cdef-0123-456789abcdef"


class _FakeInfo:
    def __init__(self, run_id):
        self.run_id = run_id


class _FakeStartedRun:
    def __init__(self, run_id):
        self.info = _FakeInfo(run_id)


class _FakeCatboost:
    """Двойник `mlflow.catboost`: `log_model(model, name=...)`."""

    def __init__(self, owner):
        self._owner = owner

    def log_model(self, model, name=None):
        self._owner.calls.append(("catboost.log_model", (model,), {"name": name}))
        if self._owner.model_error is not None:
            raise self._owner.model_error


class FakeMlflow:
    """Офлайн-двойник модуля `mlflow`: записывает вызовы, никуда не ходит.

    Устанавливается через `patch.dict(sys.modules, {"mlflow": fake})`:
    `_start_mlflow` делает ленивый `import mlflow` и получает этот объект.
    Ошибки (`start_error`, `log_params_error`, `log_text_error`,
    `model_error`) позволяют проверить, что сбой приёмника не меняет
    результат прогона.
    """

    def __init__(self, run_id=MLFLOW_RUN_ID, start_error=None,
                 log_params_error=None, log_text_error=None, model_error=None):
        self.run_id = run_id
        self.start_error = start_error
        self.log_params_error = log_params_error
        self.log_text_error = log_text_error
        self.model_error = model_error
        self.tracking_uri = None
        self.experiment = None
        self.calls = []
        self.catboost = _FakeCatboost(self)

    def _record(self, method, *args, **kwargs):
        self.calls.append((method, args, kwargs))

    def set_tracking_uri(self, uri):
        self.tracking_uri = uri
        self._record("set_tracking_uri", uri)

    def set_experiment(self, name):
        self.experiment = name
        self._record("set_experiment", name)

    def start_run(self, run_name=None, tags=None):
        self._record("start_run", run_name=run_name, tags=tags)
        if self.start_error is not None:
            raise self.start_error
        return _FakeStartedRun(self.run_id)

    def log_text(self, text, artifact_file):
        self._record("log_text", text, artifact_file)
        if self.log_text_error is not None:
            raise self.log_text_error

    def log_artifact(self, path):
        self._record("log_artifact", path)

    def log_metrics(self, metrics):
        self._record("log_metrics", metrics)

    def log_params(self, params):
        self._record("log_params", params)
        if self.log_params_error is not None:
            raise self.log_params_error

    def log_tags(self, tags):
        self._record("log_tags", tags)

    def end_run(self, status="FINISHED"):
        self._record("end_run", status=status)

    def names(self):
        return [name for name, _args, _kwargs in self.calls]

    def calls_of(self, name):
        return [call for call in self.calls if call[0] == name]


class _BrokenMlflowLoader:
    """Loader, который при импорте падает не-ImportError'ом.

    Конфликт protobuf/pydantic в транзитивных зависимостях MLflow всплывает
    уже на `import mlflow`, а не только как ImportError.
    """

    def create_module(self, spec):
        return None

    def exec_module(self, module):
        raise TypeError("protobuf version conflict")


class _BrokenMlflowFinder:
    """Ставится первым в `sys.meta_path` и ломает именно `import mlflow`."""

    def find_spec(self, fullname, path=None, target=None):
        if fullname != "mlflow":
            return None
        return importlib.machinery.ModuleSpec(fullname, _BrokenMlflowLoader())


def _mlflow_env(**extra):
    """`ML_LOG=1` и Tracking URI по умолчанию — окружение MLflow-тестов."""
    values = {"ML_LOG": "1", "MLFLOW_TRACKING_URI": "http://mlflow:5000"}
    values.update(extra)
    return _env(**values)


# Колонки таблиц дословно из `mlregistry/schema.go`. JSONEachRow разбирает
# объекты по именам ключей, поэтому набор ключей — межъязыковой контракт:
# лишний/пропавший ключ ClickHouse либо отвергнет, либо молча заполнит
# default'ом. Тест ниже пинует его независимо от Go-тестов Task 1.
ML_RUNS_COLUMNS = {
    "run_id", "experiment", "run_name", "status", "author", "git_sha",
    "git_branch", "git_dirty", "config_hash", "config_json", "env_json",
    "data_asof", "sample_hash", "sample_meta_json", "spec_path",
    "metrics_json", "artifacts_json", "started_at", "finished_at",
    "duration_sec", "notes",
}
ML_PREDICTIONS_COLUMNS = {
    "run_id", "experiment", "model", "window", "target_date", "predicted",
    "actual", "error", "unit", "meta_json", "created_at",
}


class CanonicalJsonTest(unittest.TestCase):
    def test_order_does_not_change_hash(self):
        a = {"b": 1, "a": [2, 3]}
        b = {"a": [2, 3], "b": 1}
        self.assertEqual(canonical_json(a), canonical_json(b))
        self.assertEqual(sha256_text(canonical_json(a)), sha256_text(canonical_json(b)))

    def test_compact_separators_and_utf8(self):
        # Канонический JSON — не «красивый»: пробелы меняли бы hash, а
        # escaped-unicode сделал бы hash зависимым от формы записи строки.
        self.assertEqual(
            canonical_json({"b": "Привет", "a": [2, 3]}),
            '{"a":[2,3],"b":"Привет"}',
        )

    def test_non_finite_becomes_null(self):
        self.assertEqual(
            canonical_json({"x": float("nan"), "y": [float("inf"), float("-inf")]}),
            '{"x":null,"y":[null,null]}',
        )


class JsonSafeTest(unittest.TestCase):
    def test_converts_dates_paths_uuids_and_tuples(self):
        safe = json_safe({
            "d": date(2026, 10, 18),
            "t": datetime(2026, 10, 18, 12, 0),
            "p": Path("build") / "report.csv",
            "u": "00000000-0000-0000-0000-000000000000",
            "x": (1, 2),
            "n": float("-inf"),
        })
        self.assertEqual(safe, {
            "d": "2026-10-18",
            "t": "2026-10-18T12:00:00",
            "p": "build/report.csv",
            "u": "00000000-0000-0000-0000-000000000000",
            "x": [1, 2],
            "n": None,
        })

    def test_recurses_into_nested_containers(self):
        safe = json_safe({"rows": [{"when": date(2026, 1, 2)}], "nan": float("nan")})
        self.assertEqual(safe, {"rows": [{"when": "2026-01-02"}], "nan": None})

    def test_item_scalars_are_unwrapped(self):
        # numpy-скаляр без импорта numpy: проверяется протокол `.item()`.
        safe = json_safe({
            "n": _ItemScalar(3),
            "f": _ItemScalar(0.5),
            "nested": [_ItemScalar(True)],
        })
        self.assertEqual(safe, {"n": 3, "f": 0.5, "nested": [True]})

    def test_item_returning_self_does_not_recurse_forever(self):
        class SelfItem:
            def item(self):
                return self

        marker = SelfItem()
        self.assertIs(json_safe(marker), marker)


class PredictionRowTest(unittest.TestCase):
    def test_error_is_predicted_minus_actual(self):
        row = PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp")
        self.assertAlmostEqual(row.error, 0.2)

    def test_nan_rejected(self):
        with self.assertRaises(ValueError):
            PredictionRow("m", "w", date(2026, 9, 30), float("nan"), 0.5, "pp")

    def test_infinity_rejected_for_both_values(self):
        for predicted, actual in ((float("inf"), 0.5), (0.5, float("-inf"))):
            with self.subTest(predicted=predicted, actual=actual):
                with self.assertRaises(ValueError):
                    PredictionRow("m", "w", date(2026, 9, 30), predicted, actual, "pp")

    def test_error_property_revalidates_after_mutation(self):
        # `error` — не просто вычитание: значение, испорченное после
        # конструирования, обязано упереться в проверку, а не уехать NaN'ом.
        row = PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp")
        row.predicted = float("nan")
        with self.assertRaises(ValueError):
            _ = row.error

    def test_meta_defaults_are_independent(self):
        first = PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp")
        second = PredictionRow("m2", "w", date(2026, 9, 30), 0.6, 0.5, "pp")
        first.meta["x"] = 1
        self.assertEqual(second.meta, {})


class JsonEachRowTest(unittest.TestCase):
    def test_dates_and_datetimes(self):
        text = json_each_row([{
            "run_id": "00000000-0000-0000-0000-000000000000",
            "d": date(2026, 10, 18),
            "t": datetime(2026, 10, 18, 12, 0),
        }])
        self.assertIn('"2026-10-18"', text)
        self.assertIn('"2026-10-18 12:00:00"', text)

    def test_one_compact_object_per_line(self):
        text = json_each_row([{"a": 1}, {"b": [2, 3]}])
        self.assertEqual(text.splitlines(), ['{"a":1}', '{"b":[2,3]}'])

    def test_non_finite_float_rejected(self):
        # Float64-колонки не Nullable: NaN/Inf — ValueError, а не `null`.
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    json_each_row([{"x": value}])


class InsertSqlTest(unittest.TestCase):
    def test_header_and_json_each_row_body(self):
        sql = insert_sql("ml_predictions", [{"a": 1}, {"a": 2}])
        self.assertTrue(sql.startswith("INSERT INTO ml_predictions FORMAT JSONEachRow\n"))
        self.assertEqual(_sql_payload(sql)[1], [{"a": 1}, {"a": 2}])


class SchemaKeysTest(unittest.TestCase):
    def test_batch_keys_match_ml_schema(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            run.log_predictions([
                PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp"),
            ])
            with patch.object(me, "ch_exec") as ch_exec:
                run.finish()

        preds_sql, runs_sql = (call.args[0] for call in ch_exec.call_args_list)
        self.assertEqual(set(_sql_payload(runs_sql)[1][0]), ML_RUNS_COLUMNS)
        self.assertEqual(set(_sql_payload(preds_sql)[1][0]), ML_PREDICTIONS_COLUMNS)


class FinishTest(unittest.TestCase):
    def test_disabled_writes_nothing(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=False)
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish("finished"), "disabled")
            ch_exec.assert_not_called()

    def test_clickhouse_failure_falls_back_to_local(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"ML_LOCAL_DIR": tmp}):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
                with patch("ml_experiment.ch_exec", side_effect=ClickHouseError("down")):
                    self.assertEqual(run.finish("finished"), "local")
                self.assertTrue((Path(tmp) / run.run_id / "run.json").is_file())

    def test_http_exception_from_ch_exec_falls_back_to_local(self):
        # `IncompleteRead` — `http.client.HTTPException`, а НЕ `OSError`: без
        # отдельной ветки исключение уносило бы `finish`, `run.json` не
        # писался бы, а паспорт прогона терялся.
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict(os.environ, {"ML_LOCAL_DIR": tmp}):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
                with patch(
                    "ml_experiment.ch_exec",
                    side_effect=http.client.IncompleteRead(b"partial"),
                ):
                    self.assertEqual(run.finish("finished"), "local")
                self.assertTrue((Path(tmp) / run.run_id / "run.json").is_file())

    def test_clickhouse_success_writes_predictions_then_run(self):
        with _env():
            run = start_run(
                "cpi_nowcast", "run", {"alpha": 0.5}, date(2026, 10, 18),
                {"rows": 2}, [{"y": 1}], spec_path="spec §3", enabled=True,
            )
            run.log_metrics({"rmse": 0.5, "n": 2})
            run.log_predictions([
                PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp",
                              {"probs": [0.1, 0.9]}),
                PredictionRow("m", "w", date(2026, 10, 31), 0.4, 0.6, "pp"),
            ])
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish("finished", "ok"), "clickhouse")

            self.assertEqual(ch_exec.call_count, 2)
            preds_sql, runs_sql = (call.args[0] for call in ch_exec.call_args_list)
            # Паспорт пишется последним: порядок вызовов — часть контракта.
            self.assertTrue(preds_sql.startswith("INSERT INTO ml_predictions"))
            self.assertTrue(runs_sql.startswith("INSERT INTO ml_runs"))

            run_header, run_rows = _sql_payload(runs_sql)
            self.assertEqual(run_header, "INSERT INTO ml_runs FORMAT JSONEachRow")
            self.assertEqual(len(run_rows), 1)
            run_row = run_rows[0]
            self.assertEqual(run_row["run_id"], run.run_id)
            self.assertEqual(run_row["experiment"], "cpi_nowcast")
            self.assertEqual(run_row["run_name"], "run")
            self.assertEqual(run_row["status"], "finished")
            self.assertEqual(run_row["data_asof"], "2026-10-18")
            self.assertEqual(run_row["spec_path"], "spec §3")
            self.assertEqual(
                run_row["config_hash"], sha256_text(canonical_json({"alpha": 0.5}))
            )
            self.assertEqual(run_row["sample_hash"], sha256_text(canonical_json([{"y": 1}])))
            self.assertEqual(json.loads(run_row["sample_meta_json"]), {"rows": 2})
            self.assertEqual(json.loads(run_row["metrics_json"]), {"rmse": 0.5, "n": 2})
            self.assertEqual(run_row["notes"], "ok")
            self.assertIn("started_at", run_row)
            self.assertIn("finished_at", run_row)
            self.assertGreaterEqual(run_row["duration_sec"], 0.0)

            env = json.loads(run_row["env_json"])
            self.assertEqual(set(env), {"python", "platform", "packages"})
            self.assertEqual(env["python"], sys.version)
            self.assertTrue(env["platform"])
            self.assertEqual(set(env["packages"]), set(me.PACKAGES))
            for version in env["packages"].values():
                self.assertTrue(version is None or isinstance(version, str))

            _, pred_rows = _sql_payload(preds_sql)
            self.assertEqual(len(pred_rows), 2)
            self.assertEqual(pred_rows[0]["run_id"], run.run_id)
            self.assertEqual(pred_rows[0]["experiment"], "cpi_nowcast")
            self.assertEqual(pred_rows[0]["model"], "m")
            self.assertEqual(pred_rows[0]["window"], "w")
            self.assertEqual(pred_rows[0]["target_date"], "2026-09-30")
            self.assertAlmostEqual(pred_rows[0]["predicted"], 0.7)
            self.assertAlmostEqual(pred_rows[0]["actual"], 0.5)
            self.assertAlmostEqual(pred_rows[0]["error"], 0.2)
            self.assertEqual(pred_rows[0]["unit"], "pp")
            self.assertEqual(json.loads(pred_rows[0]["meta_json"]), {"probs": [0.1, 0.9]})
            self.assertEqual(pred_rows[1]["meta_json"], "{}")
            self.assertIn("created_at", pred_rows[0])

    def test_predictions_are_written_before_run_row(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            run.log_predictions([
                PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp"),
            ])
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish(), "clickhouse")

            headers = [
                call.args[0].partition("\n")[0] for call in ch_exec.call_args_list
            ]
            self.assertEqual(headers, [
                "INSERT INTO ml_predictions FORMAT JSONEachRow",
                "INSERT INTO ml_runs FORMAT JSONEachRow",
            ])

    def test_predictions_failure_skips_run_row(self):
        # Паспорт последним: если batch предсказаний не записался, `finished`
        # паспорт не должен появиться в ClickHouse вообще.
        with tempfile.TemporaryDirectory() as tmp:
            with _env(ML_LOCAL_DIR=tmp):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
                run.log_predictions([
                    PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp"),
                ])
                with patch.object(
                    me, "ch_exec", side_effect=ClickHouseError("down")
                ) as ch_exec:
                    self.assertEqual(run.finish(), "local")

                self.assertEqual(ch_exec.call_count, 1)
                self.assertTrue(
                    ch_exec.call_args.args[0].startswith("INSERT INTO ml_predictions")
                )
                self.assertFalse(
                    any(
                        call.args[0].startswith("INSERT INTO ml_runs")
                        for call in ch_exec.call_args_list
                    )
                )
                self.assertTrue((Path(tmp) / run.run_id / "run.json").is_file())

    def test_run_row_env_json_has_python_platform_and_packages(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            with patch.object(me, "ch_exec") as ch_exec:
                run.finish()

            _, rows = _sql_payload(ch_exec.call_args.args[0])
            env = json.loads(rows[0]["env_json"])
            self.assertEqual(env["python"], sys.version)
            self.assertTrue(env["platform"])
            self.assertEqual(set(env["packages"]), set(me.PACKAGES))

    def test_item_scalars_in_config_and_metrics_do_not_break_finish(self):
        # np.int64 в конфиге/метриках: json_safe обязан развернуть `.item()`,
        # а не уронить сериализацию паспорта.
        with _env():
            run = start_run(
                "exp", "run", {"folds": _ItemScalar(2)}, date(2026, 10, 18),
                {}, [], enabled=True,
            )
            run.log_metrics({"n": _ItemScalar(3)})
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish(), "clickhouse")

            _, rows = _sql_payload(ch_exec.call_args.args[0])
            self.assertEqual(json.loads(rows[0]["config_json"]), {"folds": 2})
            self.assertEqual(json.loads(rows[0]["metrics_json"]), {"n": 3})

    def test_serialization_failure_falls_back_to_minimal_local_passport(self):
        # Произвольный объект в конфиге: полный run_row построить нельзя, но
        # finish не имеет права бросить исключение или потерять паспорт.
        with tempfile.TemporaryDirectory() as tmp:
            with _env(ML_LOCAL_DIR=tmp):
                run = start_run(
                    "exp", "run", {"bad": object()}, date(2026, 10, 18),
                    {}, [], enabled=True,
                )
                with patch.object(me, "ch_exec") as ch_exec:
                    self.assertEqual(run.finish("finished", "context"), "local")
                ch_exec.assert_not_called()
                # Повторный finish — no-op с тем же результатом.
                self.assertEqual(run.finish(), "local")

                payload = json.loads(
                    (Path(tmp) / run.run_id / "run.json").read_text(encoding="utf-8")
                )
                self.assertEqual(payload["status"], "failed")
                self.assertEqual(payload["run_id"], run.run_id)
                self.assertEqual(payload["experiment"], "exp")
                self.assertEqual(payload["run_name"], "run")
                self.assertIn("TypeError", payload["notes"])
                self.assertIn("started_at", payload)
                self.assertIn("finished_at", payload)

    def test_success_without_predictions_writes_only_run(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish(), "clickhouse")
            self.assertEqual(ch_exec.call_count, 1)
            self.assertTrue(ch_exec.call_args.args[0].startswith("INSERT INTO ml_runs"))

    def test_finish_is_idempotent(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            with patch.object(me, "ch_exec") as ch_exec:
                self.assertEqual(run.finish(), "clickhouse")
                self.assertEqual(run.finish(), "clickhouse")
            # Один run и (здесь) ноль предсказаний: один INSERT, не два.
            self.assertEqual(ch_exec.call_count, 1)

    def test_local_fallback_writes_predictions_jsonl(self):
        with tempfile.TemporaryDirectory() as tmp:
            with _env(ML_LOCAL_DIR=tmp):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
                run.log_predictions([
                    PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp"),
                ])
                with patch.object(me, "ch_exec", side_effect=ClickHouseError("down")):
                    self.assertEqual(run.finish(), "local")
                predictions = Path(tmp) / run.run_id / "predictions.jsonl"
                self.assertTrue(predictions.is_file())
                rows = [json.loads(line) for line in predictions.read_text().splitlines()]
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]["model"], "m")
                self.assertAlmostEqual(rows[0]["error"], 0.2)

    def test_exit_marks_run_failed_and_does_not_swallow(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            with patch.object(me, "ch_exec") as ch_exec:
                with self.assertRaises(RuntimeError):
                    with run:
                        raise RuntimeError("boom")

            self.assertEqual(ch_exec.call_count, 1)
            _, rows = _sql_payload(ch_exec.call_args.args[0])
            self.assertEqual(rows[0]["status"], "failed")
            self.assertIn("RuntimeError: boom", rows[0]["notes"])

    def test_exit_without_finish_marks_failed(self):
        # `with run:` без явного finish — не «ничего не произошло»: паспорт
        # помечается failed с диагностикой, иначе прогон потерялся бы молча.
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=True)
            with patch.object(me, "ch_exec") as ch_exec:
                with run:
                    pass

            self.assertEqual(ch_exec.call_count, 1)
            _, rows = _sql_payload(ch_exec.call_args.args[0])
            self.assertEqual(rows[0]["status"], "failed")
            self.assertEqual(rows[0]["notes"], "finish не вызван")

    def test_log_artifacts_hashes_files_and_skips_none(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "report.csv"
            payload = b"a,b\n1,2\n"
            path.write_bytes(payload)

            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [], enabled=False)
            run.log_artifacts([path, None])

        self.assertEqual(run.artifacts, [{
            "name": "report.csv",
            "path": str(path),
            "sha256": hashlib.sha256(payload).hexdigest(),
            "bytes": len(payload),
        }])


class CanonicalRunIdTest(unittest.TestCase):
    def test_hex_becomes_dashed_uuid(self):
        self.assertEqual(
            me.canonical_run_id(MLFLOW_RUN_ID), CANONICAL_MLFLOW_RUN_ID
        )

    def test_dashed_uuid_is_kept(self):
        self.assertEqual(
            me.canonical_run_id(CANONICAL_MLFLOW_RUN_ID),
            CANONICAL_MLFLOW_RUN_ID,
        )

    def test_invalid_id_falls_back_to_deterministic_uuid5(self):
        expected = str(uuid.uuid5(uuid.NAMESPACE_URL, "mlflow:not-a-run"))
        self.assertEqual(me.canonical_run_id("not-a-run"), expected)
        self.assertEqual(me.canonical_run_id("not-a-run"), expected)
        self.assertNotEqual(
            me.canonical_run_id("not-a-run"), me.canonical_run_id("other")
        )

    def test_none_falls_back_instead_of_raising(self):
        self.assertEqual(
            me.canonical_run_id(None),
            str(uuid.uuid5(uuid.NAMESPACE_URL, "mlflow:None")),
        )


class FlattenMetricsTest(unittest.TestCase):
    def test_skips_none_non_numeric_and_non_finite(self):
        flat = me.flatten_metrics({
            "rmse": 0.5,
            "n": 2,
            "missing": None,
            "nan": float("nan"),
            "inf": float("inf"),
            "text": "not-a-metric",
        })
        self.assertEqual(flat, {"rmse": 0.5, "n": 2.0})
        self.assertIsInstance(flat["n"], float)

    def test_primary_metric_is_not_emitted_as_metric(self):
        flat = me.flatten_metrics({"primary_metric": "rmse", "rmse": 0.5})
        self.assertEqual(flat, {"rmse": 0.5})

    def test_keys_are_sanitized(self):
        flat = me.flatten_metrics({
            "cv rmse": 1.0,
            "model/a:b": 2.0,
            "ok-key.name": 3.0,
        })
        self.assertEqual(flat, {
            "cv_rmse": 1.0,
            "model_a_b": 2.0,
            "ok-key.name": 3.0,
        })

    def test_sanitized_key_collision_gets_suffix(self):
        # `cv rmse` и `cv_rmse` после санитизации неразличимы: вторая
        # метрика обязана получить суффикс, а не молча перезаписать первую.
        with self.assertLogs("ml_experiment", level="WARNING") as logs:
            flat = me.flatten_metrics({"cv rmse": 1.0, "cv_rmse": 2.0})
        self.assertEqual(flat, {"cv_rmse": 1.0, "cv_rmse_2": 2.0})
        self.assertIn("cv_rmse_2", "\n".join(logs.output))

    def test_item_scalars_are_unwrapped(self):
        self.assertEqual(me.flatten_metrics({"n": _ItemScalar(2)}), {"n": 2.0})

    def test_empty_metrics(self):
        self.assertEqual(me.flatten_metrics({}), {})


class MlflowSinkTest(unittest.TestCase):
    def test_start_logs_tags_params_artifacts_and_finishes(self):
        fake = FakeMlflow()
        model = object()
        with tempfile.TemporaryDirectory() as tmp:
            artifact = Path(tmp) / "report.csv"
            artifact.write_text("a,b\n", encoding="utf-8")
            with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                    patch.object(me, "ch_exec") as ch_exec:
                run = start_run(
                    "cpi_nowcast", "run", {"alpha": 0.5, "models": ["m"]},
                    date(2026, 10, 18), {"rows": 2}, [{"y": 1}],
                    spec_path="spec §3",
                )
                # Канонический UUID появляется сразу после старта run'а и
                # попадает и в MLflow-артефакты, и в строки ClickHouse.
                self.assertEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)
                run.log_metrics({"rmse": 0.5, "primary_metric": "rmse"})
                run.log_predictions([
                    PredictionRow("m", "w", date(2026, 9, 30), 0.7, 0.5, "pp"),
                ])
                run.log_artifacts([artifact])
                run.log_model("model", model)
                self.assertEqual(run.finish("finished", "ok"), "clickhouse")

            self.assertEqual(fake.tracking_uri, "http://mlflow:5000")
            self.assertEqual(fake.experiment, "cpi_nowcast")

            start = fake.calls_of("start_run")[0][2]
            self.assertEqual(start["run_name"], "run")
            tags = start["tags"]
            self.assertEqual(tags["experiment"], "cpi_nowcast")
            self.assertEqual(tags["author"], run.git["author"])
            self.assertEqual(tags["git_sha"], run.git["git_sha"])
            self.assertEqual(tags["git_branch"], run.git["git_branch"])
            self.assertEqual(tags["git_dirty"], str(run.git["git_dirty"]))
            self.assertEqual(
                tags["config_hash"],
                sha256_text(canonical_json({"alpha": 0.5, "models": ["m"]})),
            )
            self.assertEqual(
                tags["sample_hash"], sha256_text(canonical_json([{"y": 1}]))
            )
            self.assertEqual(tags["data_asof"], "2026-10-18")
            self.assertEqual(tags["spec_path"], "spec §3")
            for value in tags.values():
                self.assertIsInstance(value, str)

            # В params попадают только скаляры: nested-конфиг не влезает
            # в лимит длины param и остаётся в run.json.
            self.assertEqual(fake.calls_of("log_params")[0][1][0], {"alpha": 0.5})

            texts = {call[1][1]: call[1][0] for call in fake.calls_of("log_text")}
            self.assertEqual(set(texts), {"run.json", "predictions.jsonl"})
            run_row = json.loads(texts["run.json"])
            self.assertEqual(run_row["run_id"], CANONICAL_MLFLOW_RUN_ID)
            self.assertEqual(run_row["status"], "finished")
            predictions = [
                json.loads(line)
                for line in texts["predictions.jsonl"].splitlines()
            ]
            self.assertEqual(len(predictions), 1)
            self.assertEqual(predictions[0]["run_id"], CANONICAL_MLFLOW_RUN_ID)

            self.assertEqual(
                [call[1][0] for call in fake.calls_of("log_artifact")],
                [str(artifact)],
            )

            model_calls = fake.calls_of("catboost.log_model")
            self.assertEqual(len(model_calls), 1)
            self.assertIs(model_calls[0][1][0], model)
            self.assertEqual(model_calls[0][2], {"name": "model"})

            # primary_metric — tag, а не числовая метрика.
            self.assertEqual(
                fake.calls_of("log_metrics")[0][1][0], {"rmse": 0.5}
            )
            self.assertEqual(
                [call[2]["status"] for call in fake.calls_of("end_run")],
                ["FINISHED"],
            )

            runs_sql = ch_exec.call_args_list[-1].args[0]
            _, rows = _sql_payload(runs_sql)
            self.assertEqual(rows[0]["run_id"], CANONICAL_MLFLOW_RUN_ID)

    def test_failed_status_ends_run_failed(self):
        fake = FakeMlflow()
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec"):
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertEqual(run.finish("failed", "boom"), "clickhouse")

        self.assertEqual(
            [call[2]["status"] for call in fake.calls_of("end_run")],
            ["FAILED"],
        )

    def test_empty_tracking_uri_makes_no_mlflow_calls(self):
        fake = FakeMlflow()
        with _env(ML_LOG="1", MLFLOW_TRACKING_URI=""), \
                patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec"):
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertEqual(run.finish(), "clickhouse")

        self.assertEqual(fake.calls, [])
        # Без MLflow остаётся локальный UUID версии 4.1.
        self.assertEqual(str(uuid.UUID(run.run_id)), run.run_id)
        self.assertNotEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)

    def test_ml_log_unset_makes_no_mlflow_calls(self):
        fake = FakeMlflow()
        with _env(MLFLOW_TRACKING_URI="http://mlflow:5000"), \
                patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec"):
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertFalse(run.enabled)
            self.assertEqual(run.finish(), "disabled")

        self.assertEqual(fake.calls, [])

    def test_start_failure_warns_and_keeps_local_run_id(self):
        fake = FakeMlflow(start_error=RuntimeError("tracking down"))
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec") as ch_exec:
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertEqual(run.finish(), "clickhouse")

        ch_exec.assert_called_once()
        self.assertIn("tracking down", "\n".join(logs.output))
        self.assertEqual(str(uuid.UUID(run.run_id)), run.run_id)
        self.assertNotEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)
        self.assertEqual(
            fake.names(), ["set_tracking_uri", "set_experiment", "start_run"]
        )

    def test_log_text_failure_warns_ends_failed_and_keeps_result(self):
        fake = FakeMlflow(log_text_error=RuntimeError("artifact store down"))
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec") as ch_exec:
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                self.assertEqual(run.finish("finished"), "clickhouse")

        ch_exec.assert_called_once()
        self.assertIn("artifact store down", "\n".join(logs.output))
        self.assertEqual(
            [call[2]["status"] for call in fake.calls_of("end_run")],
            ["FAILED"],
        )

    def test_mlflow_failure_does_not_break_local_fallback(self):
        fake = FakeMlflow(log_text_error=RuntimeError("artifact store down"))
        with tempfile.TemporaryDirectory() as tmp:
            with _mlflow_env(ML_LOCAL_DIR=tmp), \
                    patch.dict(sys.modules, {"mlflow": fake}):
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
                with patch.object(
                    me, "ch_exec", side_effect=ClickHouseError("ch down")
                ):
                    with self.assertLogs("ml_experiment", level="WARNING"):
                        self.assertEqual(run.finish(), "local")

                self.assertTrue(
                    (Path(tmp) / run.run_id / "run.json").is_file()
                )

    def test_model_logging_failure_warns_but_run_finishes(self):
        fake = FakeMlflow(model_error=RuntimeError("catboost missing"))
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec"):
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            run.log_model("model", object())
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                self.assertEqual(run.finish(), "clickhouse")

        self.assertIn("catboost missing", "\n".join(logs.output))
        self.assertEqual(
            [call[2]["status"] for call in fake.calls_of("end_run")],
            ["FINISHED"],
        )

    def test_import_type_error_does_not_break_start_run(self):
        # Конфликт protobuf/pydantic всплывает уже на `import mlflow`:
        # не-ImportError не имеет права уронить `start_run`.
        with _mlflow_env(), patch.dict(sys.modules) as modules, \
                patch.object(
                    sys, "meta_path",
                    [_BrokenMlflowFinder(), *sys.meta_path],
                ), patch.object(me, "ch_exec") as ch_exec:
            modules.pop("mlflow", None)
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertEqual(run.finish(), "clickhouse")

        ch_exec.assert_called_once()
        self.assertIn("protobuf version conflict", "\n".join(logs.output))
        # Локальный UUID 4.1: MLflow-приёмник так и не включился.
        self.assertEqual(str(uuid.UUID(run.run_id)), run.run_id)
        self.assertNotEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)

    def test_missing_mlflow_package_warns_and_keeps_local_run(self):
        # `None` в sys.modules — детерминированный ImportError, даже если
        # mlflow установлен в окружении разработчика.
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": None}), \
                patch.object(me, "ch_exec") as ch_exec:
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertEqual(run.finish(), "clickhouse")

        ch_exec.assert_called_once()
        self.assertIn("mlflow", "\n".join(logs.output))
        self.assertEqual(str(uuid.UUID(run.run_id)), run.run_id)
        self.assertNotEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)

    def test_long_string_param_is_skipped_with_warning(self):
        fake = FakeMlflow()
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec"):
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                run = start_run(
                    "exp", "run", {"huge": "x" * 6001, "alpha": 0.5},
                    date(2026, 10, 18), {}, [],
                )
            self.assertEqual(run.finish(), "clickhouse")

        self.assertEqual(fake.calls_of("log_params")[0][1][0], {"alpha": 0.5})
        self.assertIn("huge", "\n".join(logs.output))

    def test_log_params_failure_after_start_ends_failed(self):
        # Run уже создан, но log_params упал: его нужно закрыть FAILED,
        # а прогон продолжается по ClickHouse/local-пути без log_text.
        fake = FakeMlflow(log_params_error=RuntimeError("bad param"))
        with _mlflow_env(), patch.dict(sys.modules, {"mlflow": fake}), \
                patch.object(me, "ch_exec") as ch_exec:
            with self.assertLogs("ml_experiment", level="WARNING") as logs:
                run = start_run(
                    "exp", "run", {"alpha": 0.5}, date(2026, 10, 18), {}, []
                )
            self.assertEqual(run.finish(), "clickhouse")

        ch_exec.assert_called_once()
        self.assertIn("bad param", "\n".join(logs.output))
        self.assertEqual(run.run_id, CANONICAL_MLFLOW_RUN_ID)
        self.assertEqual(
            [call[2]["status"] for call in fake.calls_of("end_run")],
            ["FAILED"],
        )
        self.assertEqual(fake.calls_of("log_text"), [])

    def test_serialization_failure_ends_mlflow_failed(self):
        fake = FakeMlflow()
        with tempfile.TemporaryDirectory() as tmp:
            with _mlflow_env(ML_LOCAL_DIR=tmp), \
                    patch.dict(sys.modules, {"mlflow": fake}):
                # `sample_meta` не участвует в тегах/params, поэтому старт
                # проходит, а сериализация паспорта в finish — нет.
                run = start_run(
                    "exp", "run", {}, date(2026, 10, 18),
                    {"bad": object()}, [], enabled=True,
                )
                with patch.object(me, "ch_exec") as ch_exec:
                    with self.assertLogs("ml_experiment", level="WARNING"):
                        self.assertEqual(run.finish(), "local")
                ch_exec.assert_not_called()

            self.assertEqual(
                [call[2]["status"] for call in fake.calls_of("end_run")],
                ["FAILED"],
            )
            self.assertEqual(fake.calls_of("log_text"), [])
            payload = json.loads(
                (Path(tmp) / run.run_id / "run.json").read_text(encoding="utf-8")
            )
            self.assertEqual(payload["status"], "failed")


class StartRunEnvTest(unittest.TestCase):
    def test_ml_log_env_enables_logging(self):
        with _env(ML_LOG="1"):
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertTrue(run.enabled)
            with patch.object(me, "ch_exec"):
                self.assertEqual(run.finish(), "clickhouse")

    def test_default_is_disabled(self):
        with _env():
            run = start_run("exp", "run", {}, date(2026, 10, 18), {}, [])
            self.assertFalse(run.enabled)
            self.assertEqual(run.finish(), "disabled")

    def test_ml_run_name_overrides_argument(self):
        with _env(ML_RUN_NAME="env-run"):
            run = start_run("exp", "cli-run", {}, date(2026, 10, 18), {}, [], enabled=False)
            self.assertEqual(run.run_name, "env-run")


if __name__ == "__main__":
    unittest.main(verbosity=2)
