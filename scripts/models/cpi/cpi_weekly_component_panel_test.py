#!/usr/bin/env python3
"""Офлайн-тесты `scripts/models/cpi/cpi_weekly_component_panel.py` (issue #236).

Запуск:

    make cpi-weekly-panel-test

Тесты не требуют ClickHouse, сети и реального архива CB_COMP.
"""

import csv
import json
import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cpi import cpi_public_panel_audit as audit
from models.cpi import cpi_weekly_component_panel as panel


def _series(points):
    return sorted(points)


def _month(value):
    return audit.month_start(value)


def _cell(
    month=date(2024, 1, 1),
    status="ok",
    mm_pp=1.0,
    n_weeks=4,
    first_week=date(2024, 1, 8),
    last_week=date(2024, 1, 29),
    prev_week=date(2024, 1, 1),
    max_gap_days=7,
):
    return panel.MonthlyCell(
        month,
        status,
        mm_pp,
        n_weeks,
        first_week,
        last_week,
        prev_week,
        max_gap_days,
    )


def _meta(component="food", code="A", weight=0.6):
    return {
        "component": component,
        "group_code": "10",
        "group_name": "Группа",
        "weight_code": code,
        "weight_2024": weight,
    }


def _weights():
    by_code = {"A": 0.6, "B": 0.4, "C": 0.5, "D": 0.5}
    return {
        year: {
            "by_code": dict(by_code),
            "denominators": {"food": 1.0, "nonfood": 1.0, "services": 1.0},
            "total": 3.0,
        }
        for year in range(2016, 2025)
    }


def _product_row(
    series,
    component,
    code,
    weight,
    mm_pp,
    month=date(2024, 1, 1),
    status="ok",
):
    return panel.product_row(
        month,
        series,
        _meta(component, code, weight),
        _cell(month=month, status=status, mm_pp=mm_pp),
        _weights(),
    )


class TestCompounding(unittest.TestCase):
    def test_product_of_weekly_indices(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 7), 101.0),
                (date(2026, 9, 14), 99.5),
                (date(2026, 9, 21), 100.0),
                (date(2026, 9, 28), 100.5),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        expected = (1.01 * 0.995 * 1.0 * 1.005 - 1.0) * 100.0
        self.assertAlmostEqual(mm_pp, expected, places=12)
        self.assertEqual(cell.status, "ok")
        self.assertEqual(cell.n_weeks, 4)
        self.assertEqual(cell.first_week, date(2026, 9, 7))
        self.assertEqual(cell.last_week, date(2026, 9, 28))
        self.assertEqual(cell.prev_week, date(2026, 8, 31))
        self.assertEqual(cell.max_gap_days, 7)

    def test_100_is_zero_change(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 7), 100.0),
                (date(2026, 9, 14), 100.0),
                (date(2026, 9, 21), 100.0),
            ]
        )
        mm_pp, _ = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertEqual(mm_pp, 0.0)


class TestCompleteness(unittest.TestCase):
    def test_two_weeks_is_nan(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 7), 100.0),
                (date(2026, 9, 14), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "lt_min_weeks")

    def test_gap_inside_month_is_nan(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 1), 100.0),
                (date(2026, 9, 23), 100.0),
                (date(2026, 9, 29), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "gap_too_large")
        self.assertEqual(cell.max_gap_days, 22)

    def test_first_week_too_late_is_nan(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 12), 100.0),
                (date(2026, 9, 19), 100.0),
                (date(2026, 9, 26), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "incomplete_month")

    def test_last_week_too_early_is_nan(self):
        points = _series(
            [
                (date(2026, 8, 31), 100.0),
                (date(2026, 9, 1), 100.0),
                (date(2026, 9, 8), 100.0),
                (date(2026, 9, 15), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "incomplete_month")

    def test_no_previous_point_is_nan(self):
        points = _series(
            [
                (date(2026, 9, 7), 100.0),
                (date(2026, 9, 14), 100.0),
                (date(2026, 9, 21), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "no_previous")
        self.assertIsNone(cell.prev_week)

    def test_gap_after_long_break_is_nan(self):
        points = _series(
            [
                (date(2026, 1, 5), 100.0),
                (date(2026, 9, 7), 100.0),
                (date(2026, 9, 14), 100.0),
                (date(2026, 9, 21), 100.0),
                (date(2026, 9, 28), 100.0),
            ]
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "gap_too_large")

    def test_no_points(self):
        points = _series([(date(2026, 9, 7), 100.0)])
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 10, 1), date(2026, 11, 8)
        )
        self.assertIsNone(mm_pp)
        self.assertEqual(cell.status, "no_points")
        self.assertEqual(cell.n_weeks, 0)

    def test_duplicate_date_is_error(self):
        points = [
            (date(2026, 9, 7), 100.0),
            (date(2026, 9, 7), 101.0),
        ]
        with self.assertRaises(ValueError):
            panel.month_points(points, date(2026, 9, 1), date(2026, 10, 8))


class TestMonthAndAsOf(unittest.TestCase):
    def test_month_attribution_by_registration_date(self):
        points = _series(
            [
                (date(2026, 9, 1), 100.0),
                (date(2026, 9, 30), 100.0),
                (date(2026, 10, 1), 100.0),
                (date(2026, 10, 8), 100.0),
                (date(2026, 10, 15), 100.0),
                (date(2026, 10, 22), 100.0),
                (date(2026, 10, 29), 100.0),
            ]
        )
        september = panel.month_points(
            points, date(2026, 9, 1), date(2026, 10, 8)
        )
        october = panel.month_points(
            points, date(2026, 10, 1), date(2026, 11, 8)
        )
        self.assertEqual([day for day, _ in september], [date(2026, 9, 1), date(2026, 9, 30)])
        self.assertEqual(
            [day for day, _ in october],
            [
                date(2026, 10, 1),
                date(2026, 10, 8),
                date(2026, 10, 15),
                date(2026, 10, 22),
                date(2026, 10, 29),
            ],
        )
        mm_pp, cell = panel.monthly_mm(
            points, date(2026, 10, 1), date(2026, 11, 8)
        )
        self.assertEqual(cell.status, "ok")
        self.assertEqual(cell.prev_week, date(2026, 9, 30))
        self.assertEqual(mm_pp, 0.0)

    def test_cutoff_rules(self):
        self.assertEqual(
            audit.cutoff_for(date(2026, 9, 1)), date(2026, 10, 8)
        )
        self.assertEqual(
            audit.available_from_weekly(date(2026, 9, 7)),
            date(2026, 9, 9),
        )
        self.assertEqual(
            audit.available_from_mes(date(2026, 9, 30)),
            date(2026, 10, 31),
        )
        self.assertGreater(
            audit.available_from_mes(date(2026, 9, 30)),
            audit.cutoff_for(date(2026, 9, 1)),
        )

    def test_point_after_cutoff_is_excluded(self):
        points = _series(
            [
                (date(2026, 9, 5), 100.0),
                (date(2026, 9, 7), 101.0),
            ]
        )
        included = panel.month_points(
            points, date(2026, 9, 1), date(2026, 9, 8)
        )
        self.assertEqual([day for day, _ in included], [date(2026, 9, 5)])

    def test_as_of_check_has_no_violations(self):
        products = {
            "A": _series(
                [
                    (date(2026, 8, 31), 100.0),
                    (date(2026, 9, 7), 100.0),
                    (date(2026, 9, 14), 100.0),
                    (date(2026, 9, 21), 100.0),
                    (date(2026, 9, 28), 100.0),
                ]
            )
        }
        monthly_pp = {
            "cpi_all": {date(2026, 9, 1): 0.35},
            "cpi_food": {date(2026, 9, 1): 0.4},
            "cpi_nonfood": {date(2026, 9, 1): 0.2},
            "cpi_services": {date(2026, 9, 1): 0.3},
        }
        result = panel.as_of_check(
            products, monthly_pp, [date(2026, 9, 1)]
        )
        self.assertEqual(result["n_violations"], 0)
        self.assertEqual(result["n_used_weekly_points"], 4)


class TestWeightsAndReconstruction(unittest.TestCase):
    def test_weight_year(self):
        self.assertEqual(panel.weight_year_for(date(2015, 1, 1)), 2016)
        self.assertEqual(panel.weight_year_for(date(2020, 6, 1)), 2020)
        self.assertEqual(panel.weight_year_for(date(2026, 1, 1)), 2024)

    def _products(self):
        return [
            _product_row("A", "food", "A", 0.6, 1.0),
            _product_row("B", "food", "B", 0.4, 2.0),
            _product_row("C", "nonfood", "C", 0.5, 0.5),
            _product_row("D", "services", "D", 0.5, 0.0),
        ]

    def _official(self, month=date(2024, 1, 1)):
        return {
            "cpi_food": {month: 1.0},
            "cpi_nonfood": {month: 0.5},
            "cpi_services": {month: 0.0},
            "cpi_all": {month: 0.6},
        }

    def test_component_weighted_average(self):
        rows = panel.build_component_panel(
            self._products(),
            self._official(),
            _weights(),
            [date(2024, 1, 1)],
        )
        by_component = {row["component"]: row for row in rows}
        self.assertAlmostEqual(by_component["food"]["recon_mm_pp"], 1.4)
        self.assertAlmostEqual(by_component["food"]["error_pp"], 0.4)
        self.assertAlmostEqual(by_component["nonfood"]["recon_mm_pp"], 0.5)
        self.assertAlmostEqual(by_component["services"]["recon_mm_pp"], 0.0)
        self.assertAlmostEqual(
            by_component["all_components"]["recon_mm_pp"], (1.4 + 0.5 + 0.0) / 3
        )
        self.assertEqual(by_component["food"]["n_products"], 2)

    def test_nan_is_not_imputed_and_weight_is_renormalized(self):
        rows = panel.build_component_panel(
            [
                _product_row("A", "food", "A", 0.6, 1.0),
                _product_row("B", "food", "B", 0.4, None, status="no_points"),
            ],
            self._official(),
            _weights(),
            [date(2024, 1, 1)],
        )
        food = next(row for row in rows if row["component"] == "food")
        self.assertAlmostEqual(food["recon_mm_pp"], 1.0)
        self.assertAlmostEqual(food["covered_weight"], 0.6)
        self.assertAlmostEqual(food["covered_share"], 0.6)
        self.assertEqual(food["n_products"], 1)

    def test_all_components_is_nan_if_component_missing(self):
        rows = panel.build_component_panel(
            [
                _product_row("A", "food", "A", 0.6, 1.0),
                _product_row("C", "nonfood", "C", 0.5, 0.5),
            ],
            self._official(),
            _weights(),
            [date(2024, 1, 1)],
        )
        all_components = next(
            row for row in rows if row["component"] == "all_components"
        )
        self.assertIsNone(all_components["recon_mm_pp"])

    def test_fixed_2024_mode_uses_2024_weight(self):
        matrix = _weights()
        matrix[2024] = {
            "by_code": {"A": 0.9, "B": 0.1, "C": 0.5, "D": 0.5},
            "denominators": {"food": 1.0, "nonfood": 1.0, "services": 1.0},
            "total": 3.0,
        }
        row_a = _product_row("A", "food", "A", 0.9, 1.0)
        row_a["weight_used"] = 0.6
        row_b = _product_row("B", "food", "B", 0.1, 3.0)
        row_b["weight_used"] = 0.4
        rows = panel.build_component_panel(
            [row_a, row_b],
            self._official(),
            matrix,
            [date(2024, 1, 1)],
            "fixed_2024",
        )
        food = next(row for row in rows if row["component"] == "food")
        self.assertAlmostEqual(food["recon_mm_pp"], 1.2)
        self.assertEqual(food["weight_year"], 2024)

    def test_metrics(self):
        rows = panel.build_component_panel(
            self._products(),
            self._official(),
            _weights(),
            [date(2024, 1, 1)],
        )
        metrics = panel.series_metrics(
            rows, "all_components", date(2024, 1, 1), date(2024, 1, 1)
        )
        expected_error = (1.4 + 0.5 + 0.0) / 3 - 0.6
        self.assertEqual(metrics["n"], 1)
        self.assertAlmostEqual(metrics["mae_pp"], abs(expected_error))
        self.assertAlmostEqual(metrics["rmse_pp"], abs(expected_error))
        self.assertAlmostEqual(metrics["max_abs_pp"], abs(expected_error))
        self.assertAlmostEqual(metrics["bias_pp"], expected_error)


class TestDecision(unittest.TestCase):
    def test_hybrid_below_gate(self):
        result = panel.decision(
            0.499,
            {"food": 0.6, "nonfood": 0.6, "services": 0.6},
        )
        self.assertFalse(result["bottomup_full"])
        self.assertEqual(result["mode"], "hybrid_4_components_plus_features")
        self.assertEqual(result["partial_bottomup"], "robustness_only")

    def test_full_at_gate(self):
        result = panel.decision(
            0.5,
            {"food": 0.5, "nonfood": 0.5, "services": 0.5},
        )
        self.assertTrue(result["bottomup_full"])
        self.assertEqual(result["mode"], "bottomup_full")
        self.assertEqual(result["partial_bottomup"], "allowed")

    def test_component_gate_blocks_full(self):
        result = panel.decision(
            0.6,
            {"food": 0.6, "nonfood": 0.6, "services": 0.4},
        )
        self.assertFalse(result["bottomup_full"])
        self.assertFalse(result["gate_all_components"])


class TestOutputs(unittest.TestCase):
    def test_json_safe_replaces_nan(self):
        value = panel.json_safe(
            {"nan": float("nan"), "inf": float("inf"), "date": date(2026, 9, 1)}
        )
        self.assertIsNone(value["nan"])
        self.assertIsNone(value["inf"])
        self.assertEqual(value["date"], "2026-09-01")
        text = json.dumps(value)
        self.assertNotIn("NaN", text)
        self.assertNotIn("Infinity", text)

    def test_products_csv_columns_and_nan(self):
        rows = [
            _product_row("A", "food", "A", 0.6, 1.0),
            _product_row("B", "food", "B", 0.4, None, status="no_points"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "products.csv"
            panel.write_products_csv(path, rows)
            with path.open(encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                self.assertEqual(tuple(reader.fieldnames), panel.PRODUCT_COLUMNS)
                values = list(reader)
            self.assertEqual(values[0]["mm_pp"], "1.000000")
            self.assertEqual(values[1]["mm_pp"], "")
            self.assertEqual(values[1]["status"], "no_points")

    def test_components_csv_sorted(self):
        rows = panel.build_component_panel(
            [
                _product_row("A", "food", "A", 0.6, 1.0),
                _product_row("C", "nonfood", "C", 0.5, 0.5),
                _product_row("D", "services", "D", 0.5, 0.0),
            ],
            {
                "cpi_food": {date(2024, 1, 1): 1.0},
                "cpi_nonfood": {date(2024, 1, 1): 0.5},
                "cpi_services": {date(2024, 1, 1): 0.0},
                "cpi_all": {date(2024, 1, 1): 0.6},
            },
            _weights(),
            [date(2024, 1, 1)],
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "components.csv"
            panel.write_components_csv(path, rows)
            with path.open(encoding="utf-8") as handle:
                values = list(csv.DictReader(handle))
        self.assertEqual(
            [row["component"] for row in values],
            list(panel.SERIES_ORDER),
        )

    def test_passport_run_id_is_stable(self):
        config = {"min_weeks": 3, "max_gap_days": 14}
        rows = [_product_row("A", "food", "A", 0.6, 1.0)]
        first = panel.build_passport(
            config, rows, "sha256:a", "sha256:b", date(2026, 9, 28),
            date(2026, 9, 1), [], "DONE_WITH_CONCERNS"
        )
        second = panel.build_passport(
            config, rows, "sha256:a", "sha256:b", date(2026, 9, 28),
            date(2026, 9, 1), [], "DONE_WITH_CONCERNS"
        )
        self.assertEqual(first["run_id"], second["run_id"])
        third = panel.build_passport(
            config, rows, "sha256:a", "sha256:c", date(2026, 9, 28),
            date(2026, 9, 1), [], "DONE_WITH_CONCERNS"
        )
        self.assertNotEqual(first["run_id"], third["run_id"])

    def test_panel_summary(self):
        rows = [
            _product_row("A", "food", "A", 0.6, 1.0),
            _product_row("B", "food", "B", 0.4, None, status="no_points"),
        ]
        summary = panel.panel_summary(
            rows,
            [date(2024, 1, 1)],
            [{"series": "A", "weight_code": "A"}],
        )
        self.assertEqual(summary["cells"]["total"], 2)
        self.assertEqual(summary["cells"]["ok"], 1)
        self.assertEqual(summary["cells"]["status_counts"]["no_points"], 1)


class TestAggregateValidation(unittest.TestCase):
    def test_compound_against_official(self):
        aggregate = _series(
            [
                (date(2021, 1, 4), 100.0),
                (date(2021, 1, 11), 101.0),
            ]
        )
        official = {"cpi_all": {date(2021, 1, 1): 0.5}}
        result = panel.aggregate_validation(aggregate, official["cpi_all"])
        self.assertEqual(result["n_months"], 1)
        self.assertAlmostEqual(result["mae_pp"], 0.5)
        self.assertFalse(result["matches_task1"])


class TestStaticBoundaries(unittest.TestCase):
    def test_source_has_no_internal_panel_or_writes(self):
        source = (Path(__file__).parent / "cpi_weekly_component_panel.py").read_text(
            encoding="utf-8"
        )
        for forbidden in ("ОКВЭД", "BSU", "model.xlsx", "panel.csv"):
            self.assertNotIn(forbidden, source)
        for forbidden in ("CREATE ", "INSERT ", "ALTER ", "GRANT ", "DROP "):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()
