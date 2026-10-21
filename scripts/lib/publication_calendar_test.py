#!/usr/bin/env python3
"""Офлайн-тесты календаря публикаций `scripts/lib/publication_calendar.py`.

Запуск из корня репозитория (pytest не нужен):

    make publication-calendar-test

ClickHouse и сеть не нужны: точные даты берутся из фикстуры
`scripts/testdata/publication_calendar.csv`, остальное проверяется правилами
fallback. Все времена — наивные, московское время.
"""

import sys
import tempfile
import unittest
from datetime import date, datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from lib.publication_calendar import CalendarPoint, PublicationCalendar


CAL = PublicationCalendar.from_csv(
    ROOT / "scripts" / "testdata" / "publication_calendar.csv"
)


class TestExactPoints(unittest.TestCase):
    def test_lookup_returns_exact_point(self):
        point = CAL.lookup("rosstat", "cpi_all", date(2026, 9, 30))
        self.assertEqual(
            point,
            CalendarPoint(
                "rosstat",
                "cpi_all",
                date(2026, 9, 30),
                datetime(2026, 10, 9, 19, 0),
                "exact",
            ),
        )

    def test_published_at_returns_release_datetime(self):
        self.assertEqual(
            CAL.published_at("rosstat", "cpi_all", date(2026, 9, 30)),
            datetime(2026, 10, 9, 19, 0),
        )

    def test_monthly_period_is_normalised_to_month(self):
        # В CSV месячная точка лежит на конце месяца, но вызывающий код может
        # передать первое число того же месяца — это одна и та же точка.
        point = CAL.lookup("rosstat", "cpi_all", date(2026, 9, 1))
        self.assertEqual(point.period, date(2026, 9, 30))
        self.assertEqual(point.published_at, datetime(2026, 10, 9, 19, 0))
        self.assertEqual(point.precision, "exact")

    def test_exact_release_beats_fallback(self):
        # Точная дата ЦБ по сентябрю — 16 октября, а правило fallback дало бы
        # 18 октября. Для as-of между ними доступна только точная точка.
        point = CAL.lookup("cbr", "cpi_sa", date(2026, 9, 30))
        self.assertEqual(point.published_at, datetime(2026, 10, 16, 15, 0))
        self.assertEqual(point.precision, "exact")
        self.assertFalse(
            CAL.is_available(
                "cbr", "cpi_sa", date(2026, 9, 30), datetime(2026, 10, 16, 13, 30)
            )
        )
        self.assertTrue(
            CAL.is_available(
                "cbr", "cpi_sa", date(2026, 9, 30), datetime(2026, 10, 17, 12, 0)
            )
        )


class TestReleaseTimeBoundary(unittest.TestCase):
    def test_monthly_release_time_boundary(self):
        period = date(2026, 9, 30)
        self.assertFalse(
            CAL.is_available(
                "rosstat", "cpi_all", period, datetime(2026, 10, 9, 13, 30)
            )
        )
        self.assertTrue(
            CAL.is_available(
                "rosstat", "cpi_all", period, datetime(2026, 10, 9, 19, 1)
            )
        )

    def test_release_at_exact_time_is_available(self):
        # Граница включительная: `published_at <= as_of`.
        self.assertTrue(
            CAL.is_available(
                "rosstat",
                "cpi_all",
                date(2026, 9, 30),
                datetime(2026, 10, 9, 19, 0),
            )
        )

    def test_weekly_wednesday_boundary(self):
        period = date(2026, 10, 5)
        self.assertFalse(
            CAL.is_available(
                "rosstat", "cpi_weekly", period, datetime(2026, 10, 7, 13, 30)
            )
        )
        self.assertTrue(
            CAL.is_available(
                "rosstat", "cpi_weekly", period, datetime(2026, 10, 7, 19, 1)
            )
        )

    def test_expectations_same_month(self):
        period = date(2026, 10, 1)
        self.assertFalse(
            CAL.is_available(
                "cbr", "infl_exp_expected", period, datetime(2026, 10, 20, 13, 30)
            )
        )
        self.assertTrue(
            CAL.is_available(
                "cbr", "infl_exp_expected", period, datetime(2026, 10, 23, 13, 30)
            )
        )


class TestLatestAndNext(unittest.TestCase):
    def test_latest_and_next_sep11(self):
        as_of = datetime(2026, 9, 11, 13, 30)
        self.assertEqual(
            CAL.latest_published_month("rosstat", "cpi_all", as_of), date(2026, 8, 1)
        )
        self.assertEqual(
            CAL.next_unpublished_month("rosstat", "cpi_all", as_of), date(2026, 9, 1)
        )

    def test_latest_and_next_oct23(self):
        as_of = datetime(2026, 10, 23, 13, 30)
        self.assertEqual(
            CAL.latest_published_month("rosstat", "cpi_all", as_of), date(2026, 9, 1)
        )
        self.assertEqual(
            CAL.next_unpublished_month("rosstat", "cpi_all", as_of), date(2026, 10, 1)
        )

    def test_before_september_release_latest_is_july(self):
        # Пример спеки §4.1: заседание 8 сентября — август ещё не опубликован
        # (релиз 9 сентября 19:00), поэтому M0 = август.
        as_of = datetime(2026, 9, 8, 13, 30)
        self.assertEqual(
            CAL.latest_published_month("rosstat", "cpi_all", as_of), date(2026, 7, 1)
        )
        self.assertEqual(
            CAL.next_unpublished_month("rosstat", "cpi_all", as_of), date(2026, 8, 1)
        )

    def test_monthly_scan_rejects_weekly_series(self):
        with self.assertRaises(ValueError):
            CAL.latest_published_month("rosstat", "cpi_weekly", datetime(2026, 10, 23))

    def test_no_published_month_returns_none(self):
        # Вырожденный as_of: ни одна месячная точка ещё не опубликована,
        # поэтому latest = None, а next — месяц as_of.
        as_of = datetime(1, 1, 1, 0, 0)
        self.assertIsNone(CAL.latest_published_month("rosstat", "cpi_all", as_of))
        self.assertEqual(
            CAL.next_unpublished_month("rosstat", "cpi_all", as_of), date(1, 1, 1)
        )

    def test_business_survey_exact_row_returns_latest_and_next(self):
        # У ряда без fallback-правила latest/next идут по точным точкам,
        # а не падают с ValueError (контракт класса).
        calendar = PublicationCalendar(
            [
                CalendarPoint(
                    "cbr",
                    "price_expectations",
                    date(2026, 8, 31),
                    datetime(2026, 9, 10, 15, 0),
                    "exact",
                ),
                CalendarPoint(
                    "cbr",
                    "price_expectations",
                    date(2026, 9, 30),
                    datetime(2026, 10, 15, 15, 0),
                    "exact",
                ),
            ]
        )
        as_of = datetime(2026, 10, 20, 13, 30)
        self.assertEqual(
            calendar.latest_published_month("cbr", "price_expectations", as_of),
            date(2026, 9, 1),
        )
        self.assertEqual(
            calendar.next_unpublished_month("cbr", "price_expectations", as_of),
            date(2026, 10, 1),
        )

    def test_series_without_rule_or_points_returns_none(self):
        # Нет ни точных точек, ни fallback-правила: latest = None,
        # next — месяц as_of, без ValueError.
        as_of = datetime(2026, 10, 20, 13, 30)
        self.assertIsNone(CAL.latest_published_month("fred", "cpi", as_of))
        self.assertEqual(
            CAL.next_unpublished_month("fred", "cpi", as_of), date(2026, 10, 1)
        )


class TestFallbackRules(unittest.TestCase):
    def test_fallback_precision(self):
        point = CAL.lookup("rosstat", "cpi_all", date(2025, 1, 31))
        self.assertEqual(point.precision, "fallback")

    def test_monthly_cpi_rule(self):
        # 9-е число M+1, 19:00 МСК.
        self.assertEqual(
            CAL.published_at("rosstat", "cpi_all", date(2025, 1, 31)),
            datetime(2025, 2, 9, 19, 0),
        )
        self.assertEqual(
            CAL.published_at("rosstat", "cpi_food", date(2026, 2, 28)),
            datetime(2026, 3, 9, 19, 0),
        )

    def test_weekly_cpi_rule(self):
        # period + 2 дня (понедельник → среда), 19:00 МСК.
        self.assertEqual(
            CAL.published_at("rosstat", "cpi_weekly", date(2026, 10, 12)),
            datetime(2026, 10, 14, 19, 0),
        )

    def test_cbr_sa_rule(self):
        # SA/тренд ЦБ — 18-е число M+1, 15:00 МСК.
        self.assertEqual(
            CAL.published_at("cbr", "cpi_sa", date(2026, 8, 31)),
            datetime(2026, 9, 18, 15, 0),
        )
        self.assertEqual(
            CAL.published_at("cbr", "cpi_trend", date(2026, 8, 31)),
            datetime(2026, 9, 18, 15, 0),
        )

    def test_cbr_expectations_rule(self):
        # Ожидания населения — 21-е число того же месяца, 15:00 МСК.
        self.assertEqual(
            CAL.published_at("cbr", "infl_exp_expected", date(2026, 11, 1)),
            datetime(2026, 11, 21, 15, 0),
        )

    def test_business_survey_requires_exact_row(self):
        # Спека §3.2 запрещает фиксированный лаг для опросов ЦБ: без точной
        # даты в календаре точка недоступна, а не «доступна по умолчанию».
        with self.assertRaises(ValueError):
            CAL.published_at("cbr", "price_expectations", date(2026, 9, 30))
        with self.assertRaises(ValueError):
            CAL.published_at("cbr", "industry_mining_ibk", date(2026, 9, 30))

    def test_unknown_series_raises(self):
        with self.assertRaises(ValueError):
            CAL.lookup("cbr", "cbr_mo_br", date(2026, 9, 30))
        with self.assertRaises(ValueError):
            CAL.lookup("fred", "cpi", date(2026, 9, 30))


class TestCsvValidation(unittest.TestCase):
    def _write(self, text: str) -> Path:
        handle = tempfile.NamedTemporaryFile(
            "w", suffix=".csv", delete=False, encoding="utf-8"
        )
        with handle:
            handle.write(text)
        self.addCleanup(Path(handle.name).unlink)
        return Path(handle.name)

    def test_missing_column_raises(self):
        path = self._write("source,series,period\nrosstat,cpi_all,2026-09-30\n")
        with self.assertRaises(ValueError):
            PublicationCalendar.from_csv(path)

    def test_duplicate_month_raises(self):
        path = self._write(
            "source,series,period,published_at,precision\n"
            "rosstat,cpi_all,2026-09-30,2026-10-09T19:00,exact\n"
            "rosstat,cpi_all,2026-09-01,2026-10-09T19:00,exact\n"
        )
        with self.assertRaises(ValueError):
            PublicationCalendar.from_csv(path)

    def test_bad_precision_raises(self):
        path = self._write(
            "source,series,period,published_at,precision\n"
            "rosstat,cpi_all,2026-09-30,2026-10-09T19:00,guessed\n"
        )
        with self.assertRaises(ValueError):
            PublicationCalendar.from_csv(path)

    def test_business_survey_exact_row_is_used(self):
        path = self._write(
            "source,series,period,published_at,precision\n"
            "cbr,price_expectations,2026-09-30,2026-10-15T15:00,exact\n"
        )
        calendar = PublicationCalendar.from_csv(path)
        point = calendar.lookup("cbr", "price_expectations", date(2026, 9, 30))
        self.assertEqual(point.published_at, datetime(2026, 10, 15, 15, 0))
        self.assertEqual(point.precision, "exact")

    def test_tz_aware_published_at_raises(self):
        path = self._write(
            "source,series,period,published_at,precision\n"
            "rosstat,cpi_all,2026-09-30,2026-10-09T19:00+03:00,exact\n"
        )
        with self.assertRaises(ValueError):
            PublicationCalendar.from_csv(path)


class TestNaiveDatetimes(unittest.TestCase):
    def test_tz_aware_as_of_raises(self):
        as_of = datetime(2026, 10, 9, 19, 0, tzinfo=timezone.utc)
        with self.assertRaises(ValueError):
            CAL.is_available("rosstat", "cpi_all", date(2026, 9, 30), as_of)
        with self.assertRaises(ValueError):
            CAL.latest_published_month("rosstat", "cpi_all", as_of)
        with self.assertRaises(ValueError):
            CAL.next_unpublished_month("rosstat", "cpi_all", as_of)

    def test_calendar_point_rejects_tz_aware_published_at(self):
        with self.assertRaises(ValueError):
            CalendarPoint(
                "rosstat",
                "cpi_all",
                date(2026, 9, 30),
                datetime(2026, 10, 9, 19, 0, tzinfo=timezone.utc),
                "exact",
            )


if __name__ == "__main__":
    unittest.main()
