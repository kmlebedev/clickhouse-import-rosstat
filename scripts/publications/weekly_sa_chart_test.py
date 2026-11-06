#!/usr/bin/env python3
"""Тесты генератора графика недельного SA м/м ИПЦ.

Запуск из корня репозитория (pytest не нужен):
    python3 scripts/publications/weekly_sa_chart_test.py -v

Все тесты офлайновые: ни сети, ни живого ClickHouse, ни чтения витрин.
"""

import json
import sys
import unittest
import xml.etree.ElementTree as ET
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from publications import weekly_sa_chart as chart


def weekly_row(day, value, *, partial=0, as_of=""):
    return {"date": day, "value": value, "partial": partial, "as_of": as_of}


def cbr_row(day, value):
    return {"date": day, "value": value}


WEEKLY_FIXTURE = [
    weekly_row("2021-01-31", 0.5325088656169985, as_of="2021-01-25"),
    weekly_row("2021-02-28", 0.474, as_of="2021-02-22"),
    weekly_row("2022-03-31", 7.035206000541994, as_of="2022-03-28"),
    weekly_row("2026-09-30", 0.43832164128501105, as_of="2026-09-28"),
    weekly_row("2026-10-31", 0.976490622184012, partial=1, as_of="2026-10-12"),
]

CBR_FIXTURE = [
    cbr_row("2021-01-31", 0.38999998569488525),
    cbr_row("2021-02-28", 0.56),
    cbr_row("2022-03-31", 7.5),
    cbr_row("2026-09-30", 0.5299999713897705),
]


class BuildChartDataTest(unittest.TestCase):
    def setUp(self):
        self.data = chart.build_chart_data(WEEKLY_FIXTURE, CBR_FIXTURE)

    def test_rounds_weekly_to_four_digits(self):
        self.assertEqual(self.data["weekly"][0], ["2021-01-31", 0.5325])
        september = [point for point in self.data["weekly"] if point[0] == "2026-09-30"]
        self.assertEqual(september, [["2026-09-30", 0.4383]])

    def test_rounds_float32_cbr_to_two_digits(self):
        # ЦБ хранится Float32: без округления в JSON уходило бы 0.38999998569488525.
        self.assertEqual(self.data["cbr"][0], ["2021-01-31", 0.39])
        self.assertEqual(self.data["cbr"][-1], ["2026-09-30", 0.53])

    def test_nowcast_appears_in_series_and_separately(self):
        # Ряд непрерывен до последнего месяца, а маркер nowcast берётся из
        # отдельного блока: так устроен опубликованный chart-data.json.
        self.assertEqual(len(self.data["weekly"]), 5)
        self.assertEqual(self.data["weekly"][-1], ["2026-10-31", 0.9765])
        self.assertEqual(
            self.data["nowcast"],
            {"date": "2026-10-31", "value": 0.9765, "partial": True},
        )
        self.assertEqual(self.data["as_of"], "2026-10-12")

    def test_subtitle_names_nowcast_month(self):
        self.assertEqual(
            self.data["subtitle"],
            "2021-01 … 2026-09; октябрь 2026 — nowcast; % м/м",
        )

    def test_key_order_matches_published_file(self):
        self.assertEqual(
            list(self.data),
            [
                "title",
                "subtitle",
                "unit",
                "as_of",
                "weekly",
                "cbr",
                "nowcast",
                "source",
            ],
        )

    def test_without_nowcast_row(self):
        data = chart.build_chart_data(WEEKLY_FIXTURE[:-1], CBR_FIXTURE)
        self.assertIsNone(data["nowcast"])
        self.assertEqual(data["as_of"], "")
        self.assertEqual(data["subtitle"], "2021-01 … 2026-09; % м/м")

    def test_null_value_is_skipped(self):
        rows = WEEKLY_FIXTURE[:-1] + [weekly_row("2026-10-31", None, partial=1)]
        data = chart.build_chart_data(rows, CBR_FIXTURE)
        self.assertIsNone(data["nowcast"])

    def test_short_weekly_series_is_error(self):
        with self.assertRaises(ValueError):
            chart.build_chart_data(WEEKLY_FIXTURE[:1], CBR_FIXTURE)

    def test_empty_cbr_is_error(self):
        with self.assertRaises(ValueError):
            chart.build_chart_data(WEEKLY_FIXTURE, [])

    def test_only_nowcast_is_error(self):
        rows = [weekly_row("2026-10-31", 0.9765, partial=1, as_of="2026-10-12")]
        with self.assertRaises(ValueError):
            chart.build_chart_data(rows, CBR_FIXTURE)


class RenderTest(unittest.TestCase):
    def setUp(self):
        self.data = chart.build_chart_data(WEEKLY_FIXTURE, CBR_FIXTURE)
        self.svg = chart.render_svg(self.data)

    def test_svg_is_well_formed_xml(self):
        root = ET.fromstring(self.svg)
        self.assertTrue(root.tag.endswith("svg"))

    def test_labels_are_computed_from_data(self):
        self.assertIn("окт. — nowcast, as_of 12.10", self.svg)
        self.assertIn("март 2022: 7,04 / 7,50", self.svg)
        self.assertIn("Октябрь 2026 — неполный месяц", self.svg)

    def test_titles_are_escaped(self):
        data = dict(self.data)
        data["title"] = "A & B"
        self.assertIn("A &amp; B", chart.render_svg(data))

    def test_without_nowcast_has_no_nowcast_label(self):
        data = chart.build_chart_data(WEEKLY_FIXTURE[:3], CBR_FIXTURE)
        svg = chart.render_svg(data)
        ET.fromstring(svg)
        self.assertNotIn("nowcast", svg)


class HelpersTest(unittest.TestCase):
    def test_month_index_counts_from_base(self):
        base = date(2021, 1, 1)
        self.assertEqual(chart.month_index("2021-01-31", base), 0)
        self.assertEqual(chart.month_index("2026-09-30", base), 68)
        self.assertEqual(chart.month_index("2026-10-31", base), 69)

    def test_month_label(self):
        self.assertEqual(chart.month_label("2026-10-31"), "октябрь 2026")
        self.assertEqual(chart.month_label("2026-10-31", short=True), "окт.")

    def test_decimal_uses_comma(self):
        self.assertEqual(chart.decimal(7.0352, 2), "7,04")
        self.assertEqual(chart.decimal(7.5, 2), "7,50")

    def test_json_matches_published_serialization(self):
        data = chart.build_chart_data(WEEKLY_FIXTURE, CBR_FIXTURE)
        payload = json.dumps(data, ensure_ascii=False, indent=2) + "\n"
        self.assertTrue(payload.startswith('{\n  "title": "Недельный'))
        self.assertIn('"weekly": [\n    [\n      "2021-01-31",\n      0.5325', payload)


if __name__ == "__main__":
    unittest.main()
