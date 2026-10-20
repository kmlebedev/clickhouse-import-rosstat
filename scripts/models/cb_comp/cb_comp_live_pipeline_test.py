#!/usr/bin/env python3
"""Офлайн-тесты live-пайплайна CB_COMP: корректировки, дашборд, ЖКУ."""

import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd
from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_live_pipeline as pipe


def _rows(month=date(2026, 10, 1)):
    return [
        {
            "month": month,
            "component": "cpi_food",
            "label": "food",
            "actual": None,
            "forecast": 0.2,
            "error": None,
            "weight": 0.4,
            "source": "nowcast",
            "adjustment": 0.0,
        },
        {
            "month": month,
            "component": "cpi_nonfood",
            "label": "nonfood",
            "actual": None,
            "forecast": 0.1,
            "error": None,
            "weight": 0.3,
            "source": "nowcast",
            "adjustment": 0.0,
        },
        {
            "month": month,
            "component": "cpi_services",
            "label": "services",
            "actual": None,
            "forecast": 0.3,
            "error": None,
            "weight": 0.3,
            "source": "nowcast",
            "adjustment": 0.0,
        },
        {
            "month": month,
            "component": "cpi_all",
            "label": "all",
            "actual": None,
            "forecast": 0.20,
            "error": None,
            "weight": 1.0,
            "source": "nowcast",
            "adjustment": 0.0,
        },
    ]


class TestAdjustments(unittest.TestCase):
    def test_load_adjustments_aliases(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "adjustments.csv"
            path.write_text(
                "month,component,delta_pp\n2026-10,food,0.1\n2026-10,cpi_all,-0.05\n",
                encoding="utf-8",
            )
            items = pipe.load_adjustments(path)
        self.assertEqual(len(items), 2)
        self.assertEqual(items[0]["component"], "cpi_food")
        self.assertEqual(items[0]["delta"], 0.1)
        self.assertEqual(items[1]["component"], "cpi_all")

    def test_load_adjustments_unknown_component(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "adjustments.csv"
            path.write_text(
                "month,component,delta_pp\n2026-10,bogus,0.1\n", encoding="utf-8"
            )
            with self.assertRaises(ValueError):
                pipe.load_adjustments(path)

    def test_component_adjustment_recomputes_aggregate(self):
        rows = _rows()
        adjusted = pipe.apply_adjustments(
            rows,
            [
                {
                    "month": date(2026, 10, 1),
                    "component": "cpi_food",
                    "delta": 0.1,
                }
            ],
        )
        by_component = {row["component"]: row for row in adjusted}
        self.assertAlmostEqual(by_component["cpi_food"]["forecast"], 0.3)
        self.assertAlmostEqual(by_component["cpi_all"]["forecast"], 0.24)
        self.assertIn("expert", by_component["cpi_food"]["source"])
        self.assertIn("expert", by_component["cpi_all"]["source"])

    def test_aggregate_adjustment_is_applied_after_recompute(self):
        adjusted = pipe.apply_adjustments(
            _rows(),
            [
                {
                    "month": date(2026, 10, 1),
                    "component": "cpi_all",
                    "delta": 0.05,
                }
            ],
        )
        aggregate = next(row for row in adjusted if row["component"] == "cpi_all")
        self.assertAlmostEqual(aggregate["forecast"], 0.25)
        self.assertIn("expert", aggregate["source"])


class TestDashboard(unittest.TestCase):
    def test_dashboard_frame_sorts_and_wide_columns(self):
        rows = _rows(date(2026, 9, 1)) + _rows(date(2026, 10, 1))
        frame = pipe.dashboard_frame(rows)
        self.assertEqual(
            list(frame["component"].iloc[:4]),
            ["cpi_food", "cpi_nonfood", "cpi_services", "cpi_all"],
        )
        wide = pipe.wide_dashboard(frame)
        self.assertIn("cpi_food_forecast", wide.columns)
        self.assertIn("cpi_all_actual", wide.columns)
        self.assertEqual(len(wide), 2)


class TestUtilities(unittest.TestCase):
    def test_load_utilities_last_forecast_and_override(self):
        with tempfile.TemporaryDirectory() as tmp:
            archive = Path(tmp)
            path = archive / pipe.UTILITIES_RELATIVE
            path.parent.mkdir(parents=True)
            frame = pd.DataFrame(
                [
                    {"дата": "2025-04-01", "Россия": 0.2, "Дальневосточное ГУ": 0.3, "факт_прогноз": "факт"},
                    {"дата": "2025-05-01", "Россия": 0.4, "Дальневосточное ГУ": 0.5, "факт_прогноз": "прогноз"},
                ]
            )
            frame.to_excel(path, sheet_name="Прогноз", index=False)
            result = pipe.load_utilities(archive)
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["last_forecast_date"], "2025-05-01")
            self.assertAlmostEqual(result["value_used"], 0.4)
            self.assertAlmostEqual(result["last_forecast_far_eastern"], 0.5)
            overridden = pipe.load_utilities(archive, override=0.7)
            self.assertAlmostEqual(overridden["value_used"], 0.7)
            self.assertEqual(overridden["source"], "override")


class TestCalendar(unittest.TestCase):
    def test_parse_month(self):
        self.assertEqual(pipe.parse_month("2026-10"), date(2026, 10, 1))
        self.assertEqual(pipe.parse_month("2026-10-20"), date(2026, 10, 1))


if __name__ == "__main__":
    unittest.main(verbosity=2)
