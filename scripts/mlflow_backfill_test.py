#!/usr/bin/env python3
"""Офлайн-тесты backfill/preflight MLflow (без MLflow-сервера и ClickHouse)."""

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import mlflow_backfill as backfill
import mlflow_preflight as preflight


class TestRunIds(unittest.TestCase):
    def test_accepts_single_and_comma_separated(self):
        first = "a70314ac-e7bb-4031-8e7f-a9f40268b6df"
        second = "d1d63610-51ce-44d8-81ff-21929194d958"
        self.assertEqual(
            backfill.validate_run_ids([f"{first},{second}"]),
            sorted([first, second]),
        )

    def test_rejects_bad_id(self):
        with self.assertRaises(SystemExit):
            backfill.validate_run_ids(["not-a-uuid"])


class TestScalarParams(unittest.TestCase):
    def test_filters_nested_nan_and_long(self):
        params = backfill.scalar_params(
            {
                "sample": "weekly",
                "start": 24,
                "flag": True,
                "nested": {"x": 1},
                "bad": math.nan,
                "long": "x" * 6000,
                "dirty key": 1,
            }
        )
        self.assertEqual(params["sample"], "weekly")
        self.assertEqual(params["start"], 24)
        self.assertEqual(params["flag"], "True")
        self.assertEqual(params["dirty_key"], 1)
        self.assertNotIn("nested", params)
        self.assertNotIn("bad", params)
        self.assertNotIn("long", params)


class TestPreflightHealth(unittest.TestCase):
    def test_unreachable_is_false(self):
        self.assertFalse(preflight.health("http://127.0.0.1:5999"))


if __name__ == "__main__":
    unittest.main()
