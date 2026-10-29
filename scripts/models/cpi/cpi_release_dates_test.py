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

    def test_source_must_be_release_file_or_wayback(self):
        path = self.write("period,published_on,source_file\n2026-01,2026-02-13,somewhere\n")
        with self.assertRaises(ValueError):
            rd.load_release_dates(path)
        ok = self.write("period,published_on,source_file\n2026-01,2026-02-13,wayback:compendium/document/50798\n")
        self.assertEqual(rd.load_release_dates(ok), {date(2026, 1, 1): date(2026, 2, 13)})

    def test_release_before_period_end_is_error(self):
        path = self.write("period,published_on,source_file\n2026-01,2026-01-20,1_20-01-2026.html\n")
        with self.assertRaises(ValueError):
            rd.load_release_dates(path)


class VerifiedTableTest(unittest.TestCase):
    def setUp(self):
        self.dates = rd.load_release_dates(rd.DEFAULT_PATH)

    def test_continuous_82_months_dec_2019_to_sep_2026(self):
        self.assertEqual(len(self.dates), 82)
        months = sorted(self.dates)
        self.assertEqual(months[0], date(2019, 12, 1))
        self.assertEqual(months[-1], date(2026, 9, 1))
        for left, right in zip(months, months[1:]):
            self.assertEqual((right.year * 12 + right.month) - (left.year * 12 + left.month), 1)

    def test_release_is_in_next_month_and_ordered(self):
        months = sorted(self.dates)
        for period in months:
            published = self.dates[period]
            index = period.year * 12 + period.month  # следующий месяц
            self.assertEqual(published.year * 12 + published.month, index + 1, period)
        released = [self.dates[period] for period in months]
        self.assertEqual(released, sorted(released))

    def test_known_points_from_two_independent_sources(self):
        # Файлы релизов rosstat.gov.ru (дата в имени) и таблицы графика (wayback).
        self.assertEqual(self.dates[date(2019, 12, 1)], date(2020, 1, 10))
        self.assertEqual(self.dates[date(2020, 5, 1)], date(2020, 6, 5))
        self.assertEqual(self.dates[date(2021, 7, 1)], date(2021, 8, 6))
        self.assertEqual(self.dates[date(2022, 4, 1)], date(2022, 5, 13))
        self.assertEqual(self.dates[date(2023, 8, 1)], date(2023, 9, 8))
        self.assertEqual(self.dates[date(2024, 4, 1)], date(2024, 5, 17))
        self.assertEqual(self.dates[date(2024, 12, 1)], date(2025, 1, 15))
        self.assertEqual(self.dates[date(2025, 4, 1)], date(2025, 5, 16))
        self.assertEqual(self.dates[date(2025, 12, 1)], date(2026, 1, 16))
        self.assertEqual(self.dates[date(2026, 8, 1)], date(2026, 9, 11))
        self.assertEqual(self.dates[date(2026, 9, 1)], date(2026, 10, 9))

    def test_two_sources_agree_on_overlap(self):
        # Июль–ноябрь 2021 в таблице графика и в именах файлов — одни и те же даты.
        self.assertEqual(self.dates[date(2021, 7, 1)], date(2021, 8, 6))
        self.assertEqual(self.dates[date(2021, 8, 1)], date(2021, 9, 8))
        self.assertEqual(self.dates[date(2021, 11, 1)], date(2021, 12, 8))

    def test_rule_is_wrong_in_both_directions(self):
        rows = rd.compare_with_rule(self.dates)
        summary = rd.summarize(rows)
        self.assertEqual(summary["n"], 82)
        self.assertGreater(summary["later_than_rule"], 0)
        self.assertGreater(summary["earlier_than_rule"], 0)
        self.assertEqual(summary["min_diff"], -6)
        self.assertEqual(summary["max_diff"], 8)
        self.assertEqual(summary["later_than_rule"], 45)
        self.assertEqual(summary["earlier_than_rule"], 28)

    def test_difference_by_year_changes_sign(self):
        rows = rd.compare_with_rule(self.dates)
        by_year = {}
        for row in rows:
            by_year.setdefault(row["actual"].year, []).append(row["diff_days"])
        self.assertLess(statistics.mean(by_year[2020]), 0)
        self.assertLess(statistics.mean(by_year[2021]), 0)
        self.assertGreater(statistics.mean(by_year[2025]), 0)
        self.assertGreater(statistics.mean(by_year[2026]), 0)


if __name__ == "__main__":
    unittest.main()
