#!/usr/bin/env python3
"""Тесты scripts/key_rate_forecast_log.py.

Запуск из корня репозитория (pytest не нужен):

    python3 scripts/key_rate_forecast_log_test.py -v

Набор ОФЛАЙН: HTTP-транспорт замокан (`urllib.request.urlopen`), файлы артефактов
создаются во временном каталоге. Живая проверка — отдельный шаг: запущенный
ingest (`make run-ingest`) и `make key-rate-forecast-log`.
"""

import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
import unittest.mock
import urllib.error
from pathlib import Path

# Модуль и тест лежат рядом, запуск прямой (`python3 scripts/...`), поэтому его
# каталог добавляется в sys.path до импорта.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import key_rate_forecast_log as kw

# Артефакт модели в формате после этого пункта: rate_before — ставка на дату
# среза, методы — шаги в б.п.; вероятности ordered_logit — доли.
MODEL_ARTIFACT = {
    "forecast_date": "2026-10-16",
    "meeting_date": "2026-10-23",
    "data_asof": "2026-10-16",
    "rate_before": 14.0,
    "methods": {
        "taylor": {"step_bp": -65.15213277015147},
        "ordered_logit": {
            "probs": {"-1": 0.8164500768004022, "0": 0.16238743839378045, "+1": 0.021162484805817394},
            "expected_step_bp": -63.35504004507305,
        },
    },
    "sample_size": 108,
    "note": "unit test",
}

LLM_ARTIFACT = {
    "forecast_date": "2026-10-18",
    "data_asof": "2026-10-18",
    "meeting_date": "2026-10-23",
    "current_rate": 14.0,
    "probabilities": {"cut": 62.0, "hold": 33.0, "hike": 5.0},
    "expected_step_bp": -12.5,
    "expected_rate": 13.875,
    "rationale": "unit test",
    "risks": ["unit test"],
    "model": "zai-org/GLM-5.3-Flash",
    "prompt_version": 1,
    "prompt_sha256": "0" * 64,
    "data_version": "0123456789ab",
}

TOKEN = "unit-test-secret-token"


class FakeResponse:
    """Минимальный ответ urlopen: контекстный менеджер и read()."""

    def __init__(self, payload: bytes = b'{"inserted": 1}'):
        self.payload = payload

    def read(self, amount=None):
        return self.payload if amount is None else self.payload[:amount]

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False


def write_json(directory: Path, name: str, payload: dict) -> Path:
    path = directory / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


class TestModelArtifact(unittest.TestCase):
    def test_rows_convert_step_bp_to_expected_rate(self):
        rows, missing = kw.model_rows(MODEL_ARTIFACT)

        self.assertEqual(missing, [])
        self.assertEqual([row["metric"] for row in rows], ["cbr_rate:taylor", "cbr_rate:ordered_logit"])
        self.assertEqual([row["target_date"] for row in rows], ["2026-10-23", "2026-10-23"])
        self.assertEqual(rows[0]["model"], "taylor")
        self.assertEqual(rows[1]["model"], "ordered_logit")
        self.assertEqual(rows[0]["prompt_version"], 0)
        self.assertAlmostEqual(rows[0]["predicted"], 14.0 - 0.6515213277015147, places=9)
        self.assertAlmostEqual(rows[1]["predicted"], 14.0 - 0.6335504004507305, places=9)
        self.assertEqual(
            rows[0]["data_version"], "key-rate-model-v0:2026-10-16:n=108"
        )
        self.assertEqual(rows[1]["data_version"], rows[0]["data_version"])
        self.assertNotIn("probabilities", rows[0])
        self.assertEqual(sorted(rows[1]["probabilities"]), ["cut", "hike", "hold"])
        self.assertAlmostEqual(sum(rows[1]["probabilities"].values()), 100.0, places=6)
        self.assertAlmostEqual(rows[1]["probabilities"]["cut"], 81.64500768004022, places=9)

    def test_missing_rate_before_is_a_clear_error(self):
        artifact = dict(MODEL_ARTIFACT)
        del artifact["rate_before"]

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.model_rows(artifact)

        self.assertIn("rate_before", str(caught.exception))
        self.assertIn("make key-rate-model", str(caught.exception))

    def test_missing_method_is_reported_not_silent(self):
        artifact = dict(MODEL_ARTIFACT)
        artifact["methods"] = {"taylor": MODEL_ARTIFACT["methods"]["taylor"]}

        rows, missing = kw.model_rows(artifact)

        self.assertEqual([row["metric"] for row in rows], ["cbr_rate:taylor"])
        self.assertEqual(missing, ["ordered_logit"])

    def test_no_methods_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            artifact = dict(MODEL_ARTIFACT)
            artifact["methods"] = {}
            model_path = write_json(Path(tmp), "model.json", artifact)

            with self.assertRaises(kw.ForecastLogError) as caught:
                kw.rows_from_artifacts(model_path, Path(tmp) / "missing.json", log=lambda _: None)

        self.assertIn("ни одного метода", str(caught.exception))

    def test_probabilities_outside_unit_interval_are_rejected(self):
        artifact = dict(MODEL_ARTIFACT)
        artifact["methods"] = {
            "ordered_logit": {
                "probs": {"-1": 1.2, "0": -0.1, "+1": -0.1},
                "expected_step_bp": -63.0,
            }
        }

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.model_rows(artifact)

        self.assertIn("долями 0..1", str(caught.exception))

    def test_probabilities_wrong_keys_are_rejected(self):
        artifact = dict(MODEL_ARTIFACT)
        artifact["methods"] = {
            "ordered_logit": {
                "probs": {"cut": 0.8, "hold": 0.15, "hike": 0.05},
                "expected_step_bp": -63.0,
            }
        }

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.model_rows(artifact)

        self.assertIn("ключи", str(caught.exception))

    def test_probabilities_sum_not_one_is_rejected(self):
        artifact = dict(MODEL_ARTIFACT)
        artifact["methods"] = {
            "ordered_logit": {
                "probs": {"-1": 0.5, "0": 0.4, "+1": 0.05},
                "expected_step_bp": -63.0,
            }
        }

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.model_rows(artifact)

        self.assertIn("сумма", str(caught.exception))

    def test_sample_size_must_be_a_positive_integer(self):
        for sample_size in (0, -1, 1.5):
            with self.subTest(sample_size=sample_size):
                artifact = dict(MODEL_ARTIFACT)
                artifact["sample_size"] = sample_size
                with self.assertRaises(kw.ForecastLogError):
                    kw.model_rows(artifact)

    def test_artifact_dates_are_validated(self):
        artifact = dict(MODEL_ARTIFACT)
        artifact["meeting_date"] = "2026-10-01"  # раньше даты прогноза

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.model_rows(artifact)

        self.assertIn("раньше forecast_date", str(caught.exception))


class TestLLMArtifact(unittest.TestCase):
    def test_expected_rate_wins(self):
        row = kw.llm_row(LLM_ARTIFACT)

        self.assertEqual(row["metric"], "cbr_rate:llm")
        self.assertEqual(row["target_date"], "2026-10-23")
        self.assertEqual(row["predicted"], 13.875)
        self.assertEqual(row["model"], "zai-org/GLM-5.3-Flash")
        self.assertEqual(row["prompt_version"], 1)
        self.assertEqual(row["data_version"], "0123456789ab")
        self.assertEqual(row["probabilities"], {"cut": 62.0, "hold": 33.0, "hike": 5.0})

    def test_expected_rate_falls_back_to_current_rate_plus_step(self):
        artifact = dict(LLM_ARTIFACT)
        del artifact["expected_rate"]
        artifact["current_rate"] = 14.0
        artifact["expected_step_bp"] = -12.5

        row = kw.llm_row(artifact)

        self.assertAlmostEqual(row["predicted"], 13.875, places=9)

    def test_prompt_version_is_required(self):
        artifact = dict(LLM_ARTIFACT)
        del artifact["prompt_version"]

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.llm_row(artifact)

        self.assertIn("prompt_version", str(caught.exception))

    def test_model_and_data_version_are_required(self):
        for field in ("model", "data_version"):
            with self.subTest(field=field):
                artifact = dict(LLM_ARTIFACT)
                artifact[field] = "  "
                with self.assertRaises(kw.ForecastLogError) as caught:
                    kw.llm_row(artifact)
                self.assertIn(field, str(caught.exception))

    def test_probabilities_must_sum_to_100(self):
        artifact = dict(LLM_ARTIFACT)
        artifact["probabilities"] = {"cut": 60.0, "hold": 30.0, "hike": 5.0}

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.llm_row(artifact)

        self.assertIn("сумма", str(caught.exception))

    def test_probabilities_are_percent_not_fractions(self):
        artifact = dict(LLM_ARTIFACT)
        artifact["probabilities"] = {"cut": 0.62, "hold": 0.33, "hike": 0.05}

        with self.assertRaises(kw.ForecastLogError) as caught:
            kw.llm_row(artifact)

        self.assertIn("сумма", str(caught.exception))


class TestArtifactFiles(unittest.TestCase):
    def test_llm_artifact_is_optional_and_reported(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_path = write_json(Path(tmp), "model.json", MODEL_ARTIFACT)
            messages = []

            rows = kw.rows_from_artifacts(model_path, Path(tmp) / "llm.json", log=messages.append)

        self.assertEqual(len(rows), 2)
        self.assertTrue(any("LLM-артефакт не найден" in message for message in messages))

    def test_llm_artifact_is_added_when_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_path = write_json(Path(tmp), "model.json", MODEL_ARTIFACT)
            llm_path = write_json(Path(tmp), "llm.json", LLM_ARTIFACT)

            rows = kw.rows_from_artifacts(model_path, llm_path, log=lambda _: None)

        self.assertEqual([row["metric"] for row in rows],
                         ["cbr_rate:taylor", "cbr_rate:ordered_logit", "cbr_rate:llm"])

    def test_missing_model_artifact_is_an_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(kw.ForecastLogError) as caught:
                kw.rows_from_artifacts(Path(tmp) / "model.json", Path(tmp) / "llm.json",
                                       log=lambda _: None)

        self.assertIn("артефакт модели", str(caught.exception))
        self.assertIn("не найден", str(caught.exception))


class TestHTTP(unittest.TestCase):
    def test_ingest_url_appends_endpoint_once(self):
        self.assertEqual(
            kw.ingest_url("http://127.0.0.1:8081/"),
            "http://127.0.0.1:8081/v1/key_rate_forecast",
        )
        self.assertEqual(
            kw.ingest_url("http://127.0.0.1:8081/v1/key_rate_forecast"),
            "http://127.0.0.1:8081/v1/key_rate_forecast",
        )
        with self.assertRaises(kw.ForecastLogError):
            kw.ingest_url("  ")

    def test_post_row_sends_bearer_and_json(self):
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return FakeResponse()

        with unittest.mock.patch.object(kw.urllib.request, "urlopen", fake_urlopen):
            response = kw.post_row("http://ingest/v1/key_rate_forecast", TOKEN,
                                   {"metric": "cbr_rate:taylor"})

        self.assertEqual(response, {"inserted": 1})
        self.assertEqual(len(requests), 1)
        request, timeout = requests[0]
        self.assertEqual(request.method, "POST")
        self.assertEqual(request.get_header("Authorization"), f"Bearer {TOKEN}")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(json.loads(request.data), {"metric": "cbr_rate:taylor"})
        self.assertEqual(timeout, kw.HTTP_TIMEOUT)

    def test_post_row_http_error_reports_status_without_token(self):
        def fake_urlopen(request, timeout):
            raise urllib.error.HTTPError(
                request.full_url, 400, "Bad Request", {},
                io.BytesIO(b'{"error":"bad metric"}'),
            )

        with unittest.mock.patch.object(kw.urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(kw.ForecastLogError) as caught:
                kw.post_row("http://ingest/v1/key_rate_forecast", TOKEN, {"metric": "x"})

        text = str(caught.exception)
        self.assertIn("HTTP 400", text)
        self.assertIn("bad metric", text)
        self.assertNotIn(TOKEN, text)

    def test_post_row_network_error_is_a_clear_error(self):
        def fake_urlopen(request, timeout):
            raise urllib.error.URLError("connection refused")

        with unittest.mock.patch.object(kw.urllib.request, "urlopen", fake_urlopen):
            with self.assertRaises(kw.ForecastLogError) as caught:
                kw.post_row("http://ingest/v1/key_rate_forecast", TOKEN, {"metric": "x"})

        self.assertIn("запрос не выполнен", str(caught.exception))


class TestMain(unittest.TestCase):
    def test_missing_env_names_variables_without_values(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_path = write_json(Path(tmp), "model.json", MODEL_ARTIFACT)
            stderr = io.StringIO()
            stdout = io.StringIO()
            with unittest.mock.patch.dict(os.environ, {}, clear=True):
                with contextlib.redirect_stderr(stderr), contextlib.redirect_stdout(stdout):
                    code = kw.main([
                        "--model-artifact", str(model_path),
                        "--llm-artifact", str(Path(tmp) / "llm.json"),
                    ])

        self.assertEqual(code, 1)
        text = stderr.getvalue()
        self.assertIn(kw.INGEST_URL_ENV, text)
        self.assertIn(kw.INGEST_TOKEN_ENV, text)

    def test_dry_run_does_not_use_network(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_path = write_json(Path(tmp), "model.json", MODEL_ARTIFACT)
            llm_path = write_json(Path(tmp), "llm.json", LLM_ARTIFACT)
            stdout = io.StringIO()
            with unittest.mock.patch.object(kw.urllib.request, "urlopen",
                                            side_effect=AssertionError("network call")):
                with contextlib.redirect_stdout(stdout):
                    code = kw.main([
                        "--model-artifact", str(model_path),
                        "--llm-artifact", str(llm_path),
                        "--dry-run",
                    ])

        self.assertEqual(code, 0)
        lines = [line for line in stdout.getvalue().splitlines() if line.startswith("{")]
        self.assertEqual(len(lines), 3)
        metrics = [json.loads(line)["metric"] for line in lines]
        self.assertEqual(metrics, ["cbr_rate:taylor", "cbr_rate:ordered_logit", "cbr_rate:llm"])

    def test_main_posts_every_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_path = write_json(Path(tmp), "model.json", MODEL_ARTIFACT)
            requests = []

            def fake_urlopen(request, timeout):
                requests.append(request)
                return FakeResponse()

            stdout = io.StringIO()
            with unittest.mock.patch.dict(
                os.environ,
                {kw.INGEST_URL_ENV: "http://127.0.0.1:18081", kw.INGEST_TOKEN_ENV: TOKEN},
            ), unittest.mock.patch.object(kw.urllib.request, "urlopen", fake_urlopen):
                with contextlib.redirect_stdout(stdout):
                    code = kw.main([
                        "--model-artifact", str(model_path),
                        "--llm-artifact", str(Path(tmp) / "missing.json"),
                    ])

        self.assertEqual(code, 0)
        self.assertEqual(len(requests), 2)
        self.assertEqual(requests[0].full_url, "http://127.0.0.1:18081/v1/key_rate_forecast")
        self.assertIn("записано строк: 2", stdout.getvalue())
        self.assertNotIn(TOKEN, stdout.getvalue())

    def test_old_artifact_without_rate_before_fails_without_http(self):
        with tempfile.TemporaryDirectory() as tmp:
            artifact = dict(MODEL_ARTIFACT)
            del artifact["rate_before"]
            model_path = write_json(Path(tmp), "model.json", artifact)
            stderr = io.StringIO()

            with unittest.mock.patch.dict(
                os.environ,
                {kw.INGEST_URL_ENV: "http://ingest", kw.INGEST_TOKEN_ENV: TOKEN},
            ), unittest.mock.patch.object(kw.urllib.request, "urlopen",
                                          side_effect=AssertionError("network call")):
                with contextlib.redirect_stderr(stderr):
                    code = kw.main([
                        "--model-artifact", str(model_path),
                        "--llm-artifact", str(Path(tmp) / "llm.json"),
                    ])

        self.assertEqual(code, 1)
        self.assertIn("rate_before", stderr.getvalue())
        self.assertIn("make key-rate-model", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
