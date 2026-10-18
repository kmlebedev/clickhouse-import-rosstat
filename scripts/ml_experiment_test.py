#!/usr/bin/env python3
"""Тесты scripts/ml_experiment.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/ml_experiment_test.py -v

Набор ОФЛАЙН: `ch_exec` подменяется `unittest.mock`, git-команды выполняются
в текущем рабочем дереве и не требуют сети. ClickHouse для тестов не нужен.
"""

import hashlib
import json
import os
import sys
import tempfile
import unittest
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
# обязаны изолировать все три: иначе `ML_LOG=1` из окружения разработчика
# включал бы запись там, где тест ждёт «выключено».
_ML_ENV_VARS = ("ML_LOG", "ML_RUN_NAME", "ML_LOCAL_DIR")


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
