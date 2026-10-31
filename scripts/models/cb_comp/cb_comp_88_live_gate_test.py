#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_88_live_gate` (Task 7 #427): без ClickHouse, архива и сети."""

import sys
import unittest
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_88_live_gate as gate


class ShadowWindowTest(unittest.TestCase):
    def test_window_starts_at_2024_08(self):
        months = gate.shadow_months(date(2024, 10, 1))
        self.assertEqual([m.isoformat() for m in months], ["2024-08-01", "2024-09-01", "2024-10-01"])

    def test_window_is_26_months_to_2026_09(self):
        months = gate.shadow_months(date(2026, 9, 1))
        self.assertEqual(len(months), 26)
        self.assertEqual(months[0], date(2024, 8, 1))
        self.assertEqual(months[-1], date(2026, 9, 1))

    def test_panel_before_start_gives_empty_window(self):
        self.assertEqual(gate.shadow_months(date(2024, 7, 1)), [])


class DescribeTest(unittest.TestCase):
    def test_metrics_and_coverage(self):
        models = {"m": {date(2024, 8, 1): 1.0, date(2024, 9, 1): 3.0}}
        truth = {date(2024, 8, 1): 0.0, date(2024, 9, 1): 2.0}
        out = gate.describe(models, truth, [date(2024, 8, 1), date(2024, 9, 1)])["m"]
        self.assertEqual(out["n"], 2)
        self.assertAlmostEqual(out["rmse"], 1.0)
        self.assertAlmostEqual(out["mae"], 1.0)
        self.assertAlmostEqual(out["bias"], 1.0)
        self.assertAlmostEqual(out["coverage"], 1.0)

    def test_missing_month_lowers_coverage_and_is_not_zero(self):
        models = {"m": {date(2024, 8, 1): 1.0}}
        truth = {date(2024, 8, 1): 0.0, date(2024, 9, 1): 2.0}
        out = gate.describe(models, truth, [date(2024, 8, 1), date(2024, 9, 1)])["m"]
        self.assertEqual(out["n"], 1)
        self.assertAlmostEqual(out["coverage"], 0.5)
        self.assertAlmostEqual(out["rmse"], 1.0)


class ByYearTest(unittest.TestCase):
    def test_split_by_year(self):
        months = [date(2024, 12, 1), date(2025, 1, 1)]
        models = {"m": {months[0]: 1.0, months[1]: 3.0}}
        truth = {months[0]: 0.0, months[1]: 1.0}
        out = gate.by_year(models, truth, months)["m"]
        self.assertEqual(out["2024"]["n"], 1)
        self.assertAlmostEqual(out["2024"]["rmse"], 1.0)
        self.assertAlmostEqual(out["2025"]["rmse"], 2.0)


class PersistenceTest(unittest.TestCase):
    def test_b1_h1_is_previous_month(self):
        truth = {date(2024, 8, 1): 1.0, date(2024, 9, 1): 2.0, date(2024, 10, 1): 3.0}
        b1 = gate.persistence(truth, 1)
        self.assertEqual(b1, {date(2024, 9, 1): 1.0, date(2024, 10, 1): 2.0})

    def test_b1_h6_is_origin_month_not_future(self):
        truth = {date(2024, 3, 1): 0.5, date(2024, 9, 1): 2.0}
        b1 = gate.persistence(truth, 6)
        self.assertEqual(b1, {date(2024, 9, 1): 0.5})


class GapStatsTest(unittest.TestCase):
    def test_constant_gap(self):
        months = [date(2024, 8, 1), date(2024, 9, 1)]
        left = {months[0]: 1.0, months[1]: 3.0}
        right = {months[0]: 0.5, months[1]: 2.5}
        out = gate.gap_stats(left, right, months)
        self.assertEqual(out["n"], 2)
        self.assertAlmostEqual(out["rmse"], 0.5)
        self.assertAlmostEqual(out["bias"], 0.5)
        self.assertAlmostEqual(out["corr"], 1.0)

    def test_too_few_points_are_nan(self):
        out = gate.gap_stats({date(2024, 8, 1): 1.0}, {date(2024, 8, 1): 0.0}, [date(2024, 8, 1)])
        self.assertEqual(out["n"], 1)
        self.assertNotEqual(out["rmse"], out["rmse"])  # NaN


class CbrUnitsTest(unittest.TestCase):
    def test_levels_become_mm(self):
        values, unit = gate.cbr_to_mm({date(2024, 8, 1): 100.4, date(2024, 9, 1): 100.6})
        self.assertEqual(unit, "levels")
        self.assertAlmostEqual(values[date(2024, 8, 1)], 0.4)

    def test_mm_percent_is_kept_as_is(self):
        values, unit = gate.cbr_to_mm({date(2024, 8, 1): 0.4, date(2024, 9, 1): -0.2})
        self.assertEqual(unit, "mm_pct")
        self.assertAlmostEqual(values[date(2024, 9, 1)], -0.2)


class PinsTest(unittest.TestCase):
    def result(self, delta=0.0):
        ratios = {name: {str(h): pins[h] + delta for h in gate.HORIZONS}
                  for name, pins in ((gate.MODEL_COMBO, gate.COMBO_RATIO_PINS),
                                     (gate.MODEL_CB, gate.LIVE_CB_RATIO_PINS))}
        per_model = {gate.MODEL_COMBO: {
            str(h): {"vs_cb_target_full": {"rmse_cb": gate.CB_RMSE_PINS[h]}} for h in gate.HORIZONS}}
        return {"ratios": ratios, "per_model": per_model}

    def test_matching_pins_pass(self):
        self.assertTrue(gate.pins_check(self.result())["ok"])

    def test_deviation_fails(self):
        checks = gate.pins_check(self.result(delta=1e-3))
        self.assertFalse(checks["ok"])
        self.assertFalse(checks["ratio"]["combo3:h1"]["ok"])

    def test_int_keys_from_raw_run_are_supported(self):
        result = self.result()
        result["ratios"] = {name: {h: value for h, value in by_h.items()}
                            for name, by_h in result["ratios"].items()}
        result["per_model"] = {gate.MODEL_COMBO: {
            h: value for h, value in result["per_model"][gate.MODEL_COMBO].items()}}
        self.assertTrue(gate.pins_check(result)["ok"])


class DecisionTest(unittest.TestCase):
    def test_undetermined_gives_iterate_without_promote(self):
        out = gate.decision_of({1: "undetermined", 3: "undetermined", 6: "undetermined"})
        self.assertEqual(out["decision"], "iterate")
        self.assertFalse(out["promote"])
        self.assertFalse(out["production"])
        self.assertTrue(any("h1" in reason for reason in out["reasons"]))

    def test_all_better_still_iterate(self):
        out = gate.decision_of({1: "better", 3: "better", 6: "better"})
        self.assertEqual(out["decision"], "iterate")
        self.assertIn("проспективный", out["reasons"][0])

    def test_worse_is_not_promote(self):
        out = gate.decision_of({1: "better", 3: "worse", 6: "better"})
        self.assertEqual(out["decision"], "iterate")
        self.assertFalse(out["promote"])


class VerdictTest(unittest.TestCase):
    def test_verdict_mentions_class_and_ratios(self):
        text = gate.verdict_76({1: "undetermined", 3: "undetermined", 6: "undetermined"},
                               {1: 1.0347, 3: 1.0102, 6: 0.894})
        self.assertIn("не определено", text)
        self.assertIn("1.035", text)
        self.assertIn("39,3", text)


class RefitRowsTest(unittest.TestCase):
    def rows(self):
        return [
            {"kind": "combo3", "model": "annual", "horizon": "1", "month": "2019-01-01", "y_pred": "0.5"},
            {"kind": "combo3", "model": "rolling120", "horizon": "3", "month": "2019-01-01", "y_pred": "0.7"},
            {"kind": "combo3", "model": "annual", "horizon": "1", "month": "2019-02-01", "y_pred": "nan"},
        ]

    def test_filters_by_horizon_and_model(self):
        out = gate.parse_refit_rows(self.rows(), ("annual",), 1)
        self.assertIn(date(2019, 1, 1), out["annual"])
        self.assertAlmostEqual(out["annual"][date(2019, 1, 1)], 0.5)

    def test_other_horizon_not_taken(self):
        out = gate.parse_refit_rows(self.rows(), ("rolling120",), 1)
        self.assertEqual(out, {})

    def test_live_cb_kind_is_not_mixed_with_combo3(self):
        rows = self.rows() + [{"kind": "live_cb", "model": "annual", "horizon": "1",
                               "month": "2019-01-01", "y_pred": "9.9"}]
        out = gate.parse_refit_rows(rows, ("annual",), 1)
        self.assertAlmostEqual(out["annual"][date(2019, 1, 1)], 0.5)


class AlignedErrorsTest(unittest.TestCase):
    def test_alignment_is_by_month_not_position(self):
        months = [date(2019, 1, 1), date(2019, 2, 1), date(2019, 3, 1)]
        predictions = {months[0]: 1.0, months[2]: 3.0}
        truth = {m: 0.0 for m in months}
        cb = {months[0]: 0.1, months[1]: 0.2, months[2]: 0.3}
        ours, reference = gate.aligned_errors(predictions, truth, months, cb)
        self.assertEqual(ours, [1.0, 3.0])
        self.assertEqual(reference, [0.1, 0.3])


class ConfigHashTest(unittest.TestCase):
    def test_deterministic_and_order_independent(self):
        first = gate.config_hash({"a": 1, "b": [1, 2]})
        second = gate.config_hash({"b": [1, 2], "a": 1})
        self.assertEqual(first, second)
        self.assertNotEqual(first, gate.config_hash({"a": 1, "b": [1, 3]}))


def sample_artifact():
    cell = {"class": "undetermined", "rmse_ours": 0.1643, "rmse_cb": 0.1588, "ratio": 1.0348,
            "delta_ci95": [-0.015, 0.029], "dm_p": 0.626}
    classes = {model: {str(h): {"normal54": dict(cell), "months67": dict(cell)} for h in gate.HORIZONS}
               for model in gate.REFERENCE_MODELS}
    metric = {"n": 26, "coverage": 1.0, "rmse": 0.2, "mae": 0.15, "bias": 0.01}
    horizons = {str(h): {"rt": {gate.MODEL_COMBO: dict(metric), gate.MODEL_CB: dict(metric),
                                "b1": dict(metric)},
                         "final": {gate.MODEL_COMBO: dict(metric), gate.MODEL_CB: dict(metric)},
                         "by_year_rt": {}, "coverage": {}} for h in gate.HORIZONS}
    return {
        "spec": gate.SPEC, "git_sha": "deadbeef",
        "configuration": {"primary": "combo3@frozen", "refit_mode": "not_adopted"},
        "gate": {"classes": classes,
                 "mcs": {str(h): {"included": ["cb_comp", "combo3"]} for h in gate.PRIMARY_HORIZONS},
                 "regime": {"provided": False, "note": "нет файла"}},
        "shadow": {"window": {"start": "2024-08-01", "end": "2026-09-01", "n": 26}, "cutoff": "late",
                   "horizons": horizons,
                   "cbr": {"series": "Все товары и услуги", "table": "cbr_indicators_cpd",
                           "last_available": "2026-08-01", "window": ["2024-08-01", "2026-08-01"], "n": 25,
                           "target_gap": {"rmse": 0.1, "bias": 0.0, "corr": 0.9},
                           "model_h1": {"rmse": 0.2, "bias": 0.0}, "note": "разрыв целей"}},
        "decision": gate.decision_of({1: "undetermined", 3: "undetermined", 6: "undetermined"}),
        "verdict_76": "вердикт", "not_proven": ["пункт"],
    }


class RenderTest(unittest.TestCase):
    def test_markdown_has_decision_and_tables(self):
        text = gate.render_md(sample_artifact())
        self.assertIn("решение: `iterate`", text)
        self.assertIn("Классы против CB_COMP", text)
        self.assertIn("Paper shadow", text)
        self.assertIn("| `combo3`", text)
        self.assertIn("| 1 | `combo3` |", text)


if __name__ == "__main__":
    unittest.main()
