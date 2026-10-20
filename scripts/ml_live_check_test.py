#!/usr/bin/env python3
"""Офлайн-тесты проверки live-прогноза перед публикацией (infra/4.3)."""

import io
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest import mock

import ml_live_check as mlc

MEETING = "2026-10-23"


def row(metric, forecast_date="2026-10-18", target_date=MEETING,
        stage="production", data_version="key-rate-model-v0:2026-10-18:n=108"):
    return {
        "metric": metric,
        "forecast_date": forecast_date,
        "target_date": target_date,
        "stage": stage,
        "data_version": data_version,
        "is_resolved": 0,
    }


def fake_query(rows):
    def query(sql):
        if "v_events_calendar" in sql:
            return [{"next_meeting": MEETING}]
        if "v_ml_live_accuracy" in sql:
            return rows
        raise AssertionError(f"неожиданный SQL: {sql}")

    return query


def run(rows, args=None):
    buffer = io.StringIO()
    output = io.StringIO()
    with mock.patch.object(mlc, "ch_query", side_effect=fake_query(rows)):
        with redirect_stdout(output), redirect_stderr(buffer):
            code = mlc.main(args or [])
    return code, output.getvalue() + buffer.getvalue()


class CheckTest(unittest.TestCase):
    def test_all_tracks_present(self):
        rows = [row("cbr_rate:taylor"), row("cbr_rate:ordered_logit")]
        code, stdout = run(rows)
        self.assertEqual(code, 0)
        self.assertIn("2 трек", stdout)

    def test_missing_track(self):
        rows = [row("cbr_rate:taylor")]
        code, stderr = run(rows)
        self.assertEqual(code, 1)
        self.assertIn("cbr_rate:ordered_logit", stderr)

    def test_shadow_row_is_not_production(self):
        rows = [row("cbr_rate:taylor"), row("cbr_rate:ordered_logit", stage="shadow")]
        code, stderr = run(rows)
        self.assertEqual(code, 1)
        self.assertIn("cbr_rate:ordered_logit", stderr)

    def test_stale_forecast_date(self):
        rows = [row("cbr_rate:taylor", forecast_date=MEETING), row("cbr_rate:ordered_logit")]
        code, stderr = run(rows)
        self.assertEqual(code, 1)
        self.assertIn("не раньше", stderr)

    def test_empty_data_version(self):
        rows = [row("cbr_rate:taylor"), row("cbr_rate:ordered_logit", data_version="  ")]
        code, stderr = run(rows)
        self.assertEqual(code, 1)
        self.assertIn("data_version", stderr)

    def test_warn_only_returns_zero(self):
        rows = [row("cbr_rate:taylor")]
        code, stderr = run(rows, ["--warn-only"])
        self.assertEqual(code, 0)
        self.assertIn("WARN", stderr)

    def test_require_llm(self):
        rows = [row("cbr_rate:taylor"), row("cbr_rate:ordered_logit")]
        code, _ = run(rows)
        self.assertEqual(code, 0)
        code, stderr = run(rows, ["--require-llm"])
        self.assertEqual(code, 1)
        self.assertIn("cbr_rate:llm", stderr)

    def test_explicit_track_overrides_default(self):
        rows = [row("cbr_rate:taylor")]
        code, stdout = run(rows, ["--track", "cbr_rate:taylor"])
        self.assertEqual(code, 0)
        self.assertIn("1 трек", stdout)


class NextMeetingTest(unittest.TestCase):
    def test_no_future_meeting(self):
        with mock.patch.object(mlc, "ch_query", return_value=[{"next_meeting": ""}]):
            with self.assertRaises(mlc.LiveCheckError):
                mlc.next_meeting()


if __name__ == "__main__":
    unittest.main()
