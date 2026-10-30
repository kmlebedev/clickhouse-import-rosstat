#!/usr/bin/env python3
"""Тесты чистых функций `ppi_sa_text_anchors.py` (без ClickHouse)."""

from __future__ import annotations

import unittest

import ppi_sa_text_anchors as m


class EvaluateTest(unittest.TestCase):
    def test_near_zero(self) -> None:
        self.assertEqual(m.evaluate("near_zero", 0.5)[0], "pass")
        self.assertEqual(m.evaluate("near_zero", 3.03)[0], "fail")

    def test_near_4(self) -> None:
        self.assertEqual(m.evaluate("near_4", 4.0)[0], "pass")
        self.assertEqual(m.evaluate("near_4", 3.4)[0], "fail")
        self.assertEqual(m.evaluate("near_4", 4.6)[0], "fail")

    def test_below_4(self) -> None:
        self.assertEqual(m.evaluate("below_4", 3.99)[0], "pass")
        self.assertEqual(m.evaluate("below_4", 4.0)[0], "fail")

    def test_declining_and_flat(self) -> None:
        self.assertEqual(m.evaluate("declining", -0.1)[0], "pass")
        self.assertEqual(m.evaluate("declining", 0.0)[0], "fail")
        self.assertEqual(m.evaluate("almost_flat", 0.5)[0], "pass")
        self.assertEqual(m.evaluate("almost_flat", 1.1)[0], "fail")

    def test_direction_up(self) -> None:
        self.assertEqual(m.evaluate("direction_up", 4.9, 3.0)[0], "pass")
        self.assertEqual(m.evaluate("direction_up", 2.0, 3.0)[0], "fail")
        self.assertEqual(m.evaluate("direction_up", 2.0, None)[0], "n/a")

    def test_not_stated_and_exact(self) -> None:
        self.assertEqual(m.evaluate("not_stated", 0.0)[0], "n/a")
        self.assertEqual(m.evaluate("exact", 0.0)[0], "")


class StatsTest(unittest.TestCase):
    def test_stats(self) -> None:
        rows = [
            {"anchor_value": 2.0, "ours": 1.0, "diff": -1.0},
            {"anchor_value": 4.0, "ours": 5.0, "diff": 1.0},
        ]
        s = m.stats(rows)
        self.assertEqual(s["n"], 2)
        self.assertAlmostEqual(s["mae"], 1.0)
        self.assertAlmostEqual(s["max_abs"], 1.0)
        self.assertAlmostEqual(s["bias"], 0.0)
        self.assertAlmostEqual(s["corr"], 1.0)


class BuildRowsTest(unittest.TestCase):
    def test_all_issues_and_kinds(self) -> None:
        targets = {
            "2025-09-01", "2025-10-01", "2025-11-01", "2025-12-01",
            "2026-02-01", "2026-03-01", "2026-04-01", "2026-05-01",
            "2026-06-01", "2026-07-01", "2026-08-01",
        }
        fake = {(s, t): 0.0 for s in ("food", "nonfood", "core", "all") for t in targets}
        rows = m.build_rows(fake)
        self.assertEqual(len(rows), 37)
        self.assertEqual(sum(r["kind"] == "exact" for r in rows), 11)
        self.assertEqual(
            sum(r["kind"] not in ("exact", "not_stated") for r in rows), 14
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
