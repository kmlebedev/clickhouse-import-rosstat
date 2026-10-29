#!/usr/bin/env python3
"""Офлайн-тесты таблицы фактических дат публикации месячного ИПЦ Росстата (#401 W0).

Данные — график срочных информаций на https://rosstat.gov.ru/compendium/document/50798
(«Об индексе потребительских цен в <месяц> <год> года»). Запуск —
`make cpi-asof-test`.
"""

import statistics
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import cpi_release_dates as rd


class RuleTest(unittest.TestCase):
    def test_rule_is_ninth_of_next_month(self):
        self.assertEqual(rd.rule_date(date(2026, 9, 1)), date(2026, 10, 9))
        self.assertEqual(rd.rule_date(date(2025, 12, 1)), date(2026, 1, 9))
        self.assertEqual(rd.rule_date(date(2026, 9, 30)), date(2026, 10, 9))


class LoadTest(unittest.TestCase):
    def write(self, text: str) -> Path:
        handle = tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False, encoding="utf-8")
        handle.write(text)
        handle.close()
        return Path(handle.name)

    def test_bad_header(self):
        with self.assertRaises(ValueError):
            rd.load_release_dates(self.write("a,b\n1,2\n"))

    def test_duplicate_period(self):
        path = self.write(
            "period,published_on,source_file\n2026-01,2026-02-13,1_13-02-2026.html\n"
            "2026-01,2026-02-14,1_14-02-2026.html\n"
        )
        with self.assertRaises(ValueError):
            rd.load_release_dates(path)

    def test_file_date_must_match_published_on(self):
        path = self.write("period,published_on,source_file\n2026-01,2026-02-13,1_14-02-2026.html\n")
        with self.assertRaises(ValueError):
            rd.load_release_dates(path)

    def test_release_before_period_end_is_error(self):
        path = self.write("period,published_on,source_file\n2026-01,2026-01-20,1_20-01-2026.html\n")
        with self.assertRaises(ValueError):
            rd.load_release_dates(path)


class VerifiedTableTest(unittest.TestCase):
    def setUp(self):
        self.dates = rd.load_release_dates(rd.DEFAULT_PATH)

    def test_ten_months_dec_2025_to_sep_2026(self):
        self.assertEqual(len(self.dates), 10)
        self.assertEqual(min(self.dates), date(2025, 12, 1))
        self.assertEqual(max(self.dates), date(2026, 9, 1))

    def test_known_points(self):
        self.assertEqual(self.dates[date(2025, 12, 1)], date(2026, 1, 16))
        self.assertEqual(self.dates[date(2026, 8, 1)], date(2026, 9, 11))
        self.assertEqual(self.dates[date(2026, 9, 1)], date(2026, 10, 9))

    def test_difference_to_rule(self):
        rows = rd.compare_with_rule(self.dates)
        self.assertEqual([row["diff_days"] for row in rows], [7, 4, 4, 1, 6, 1, 1, 3, 2, 0])
        summary = rd.summarize(rows)
        self.assertEqual(summary["n"], 10)
        self.assertEqual(summary["later_than_rule"], 9)
        self.assertEqual(summary["earlier_than_rule"], 0)
        self.assertEqual(summary["max_diff"], 7)
        self.assertAlmostEqual(summary["mean_diff"], 2.9)
        self.assertEqual(summary["median_diff"], statistics.median([7, 4, 4, 1, 6, 1, 1, 3, 2, 0]))


if __name__ == "__main__":
    unittest.main()
