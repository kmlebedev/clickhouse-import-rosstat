#!/usr/bin/env python3
"""Офлайн-тесты smoke-проверки Grafana-алертов (infra/4.3)."""

import io
import json
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import grafana_alert_check as gac


def rules_body():
    return json.dumps([
        {
            "uid": "ml-live-degradation",
            "title": "ML live accuracy degraded",
            "for": "5m",
            "isPaused": False,
            "labels": {"severity": "warning", "contour": "ml-live", "rule": "degradation"},
        },
        {
            "uid": "ml-live-freshness",
            "title": "ML live accuracy freshness",
            "for": "5m",
            "isPaused": False,
            "labels": {"severity": "warning", "contour": "ml-live", "rule": "freshness"},
        },
    ])


def run(status=200, body=None):
    output, errors = io.StringIO(), io.StringIO()
    with mock.patch.dict(
        "os.environ", {"GRAFANA_URL": "http://grafana", "GRAFANA_TOKEN": "token"}
    ), mock.patch.object(gac, "request", return_value=(status, body if body is not None else rules_body())):
        with redirect_stdout(output), redirect_stderr(errors):
            code = gac.main()
    return code, output.getvalue() + errors.getvalue()


class AlertCheckTest(unittest.TestCase):
    def test_both_rules_present(self):
        code, text = run()
        self.assertEqual(code, 0)
        self.assertIn("ml-live-degradation", text)
        self.assertIn("ПРОВЕРКА ПРОЙДЕНА", text)

    def test_missing_rule(self):
        code, text = run(body=json.dumps([{
            "uid": "ml-live-degradation",
            "labels": {"severity": "warning", "contour": "ml-live"},
        }]))
        self.assertEqual(code, 1)
        self.assertIn("ml-live-freshness", text)

    def test_paused_rule_fails(self):
        body = json.loads(rules_body())
        body[0]["isPaused"] = True
        code, text = run(body=json.dumps(body))
        self.assertEqual(code, 1)
        self.assertIn("paused=True", text)

    def test_http_error(self):
        code, text = run(status=401, body="unauthorized")
        self.assertEqual(code, 1)
        self.assertIn("HTTP 401", text)

    def test_missing_env(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                code = gac.main()
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
