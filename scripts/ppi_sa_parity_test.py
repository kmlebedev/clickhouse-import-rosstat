#!/usr/bin/env python3
"""Офлайн-тесты паритета ppi_sa: якоря, статистика и сопоставление."""

from __future__ import annotations

import math
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ppi_sa_parity as parity


class LoadAnchorsTest(unittest.TestCase):
    def test_real_anchors(self) -> None:
        anchors = parity.load_anchors(parity.ANCHORS)
        self.assertEqual(len(anchors), 10)
        self.assertEqual({item["series"] for item in anchors}, {"food", "nonfood", "core"})
        self.assertEqual({item["issue"] for item in anchors}, {"2026-5", "2026-6", "2026-7", "2026-8", "2026-9"})
        self.assertTrue(all(item["source"].startswith("https://www.cbr.ru/") for item in anchors))


class StatsTest(unittest.TestCase):
    def test_known_values(self) -> None:
        values = [
            {"anchor": 1.0, "ours": 3.0, "diff": 2.0},
            {"anchor": 2.0, "ours": 2.0, "diff": 0.0},
            {"anchor": 3.0, "ours": 1.0, "diff": -2.0},
        ]
        stats = parity._stats(values)
        self.assertEqual(stats["n"], 3)
        self.assertAlmostEqual(stats["mae"], 4.0 / 3.0)
        self.assertAlmostEqual(stats["max_abs"], 2.0)
        self.assertAlmostEqual(stats["bias"], 0.0)
        self.assertAlmostEqual(stats["corr"], -1.0)

    def test_empty(self) -> None:
        self.assertEqual(parity._stats([]), {"n": 0})

    def test_zero_variance(self) -> None:
        stats = parity._stats(
            [
                {"anchor": 1.0, "ours": 2.0, "diff": 1.0},
                {"anchor": 1.0, "ours": 3.0, "diff": 2.0},
            ]
        )
        self.assertIsNone(stats["corr"])


class ComputeTest(unittest.TestCase):
    def test_compute_and_diff(self) -> None:
        anchors = [
            {"issue": "2026-5", "target": "2026-04-01", "anchor_series": "food",
             "series": "food", "anchor": -4.1, "source": "x"},
            {"issue": "2026-5", "target": "2026-04-01", "anchor_series": "nonfood",
             "series": "nonfood", "anchor": 2.9, "source": "x"},
        ]
        rows = [
            {"series": "food", "target": "2026-04-01", "saar3": -2.0},
            {"series": "nonfood", "target": "2026-04-01", "saar3": 2.0},
        ]
        result, by_series, overall = parity.compute(anchors, rows)
        self.assertEqual(len(result), 2)
        self.assertAlmostEqual(result[0]["diff"], 2.1)
        self.assertAlmostEqual(result[1]["diff"], -0.9)
        self.assertEqual(by_series["food"]["n"], 1)
        self.assertEqual(overall["n"], 2)
        self.assertAlmostEqual(overall["mae"], (2.1 + 0.9) / 2)

    def test_missing_row_raises(self) -> None:
        anchors = [
            {"issue": "2026-5", "target": "2026-04-01", "anchor_series": "food",
             "series": "food", "anchor": -4.1, "source": "x"},
        ]
        with self.assertRaises(ValueError):
            parity.compute(anchors, [])

    def test_tolerance_flag_math(self) -> None:
        self.assertFalse(math.isclose(1.728, 0.3))
        self.assertTrue(0.2 <= parity.TOLERANCE_PP)
        self.assertFalse(1.728 <= parity.TOLERANCE_PP)


if __name__ == "__main__":
    unittest.main()
