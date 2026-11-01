#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_tlive_target` (P0.3 #480): без ClickHouse, R и архива."""

import json
import math
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_bottomup as bu
from models.cb_comp import cb_comp_tlive_target as tt


def grid(first: date, last: date) -> list:
    out, month = [], first
    while month <= last:
        out.append(month)
        month = bu.shift_month(month, 1)
    return out


MONTHS = grid(date(2020, 1, 1), date(2020, 6, 1))
WEIGHTS = {
    2019: {"10": 0.5, "20": 0.5, "9470": 0.1},
    2024: {"10": 0.4, "20": 0.6, "9470": 0.2},
}
TOTALS = {2019: 1.0, 2024: 1.0}


class WeightYearTest(unittest.TestCase):
    def test_exact_year(self):
        self.assertEqual(tt.weight_year(WEIGHTS, date(2020, 5, 1)), (2019, False))

    def test_missing_year_falls_back(self):
        self.assertEqual(tt.weight_year(WEIGHTS, date(2021, 5, 1)), (2019, True))

    def test_fallback_uses_nearest_earlier(self):
        self.assertEqual(tt.weight_year(WEIGHTS, date(2026, 3, 1)), (2024, True))

    def test_fallback_before_first_year_uses_first(self):
        self.assertEqual(tt.weight_year(WEIGHTS, date(2009, 3, 1)), (2019, True))


class MissingRuleTest(unittest.TestCase):
    def test_rt_series_drops_groups_without_full_series(self):
        # 9218: as-of значения есть, полной серии нет — в цель не входит.
        sa_full = {"10": {MONTHS[0]: 1.0}}
        sa_asof = {"10": {MONTHS[0]: 1.0}, "9218": {MONTHS[0]: 5.0}}
        self.assertEqual(sorted(tt.rt_series(sa_full, sa_asof)), ["10"])

    def test_renormalizes_over_groups_with_sa(self):
        sa = {"10": {month: 1.0 for month in MONTHS},
              "20": {month: 2.0 for month in MONTHS}}
        mm, stats = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        for month in MONTHS:
            self.assertAlmostEqual(mm[month], 1.5)
            self.assertEqual(stats[month]["n_groups"], 2)
            self.assertAlmostEqual(stats[month]["cov_group"], 1.0)
            self.assertAlmostEqual(stats[month]["cov_total"], 1.0)

    def test_group_without_sa_leaves_numerator_and_denominator(self):
        sa = {"10": {month: 1.0 for month in MONTHS},
              "20": {month: 3.0 for month in MONTHS[:3]}}
        mm, stats = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        self.assertAlmostEqual(mm[MONTHS[0]], 2.0)
        self.assertAlmostEqual(mm[MONTHS[-1]], 1.0)
        self.assertEqual(stats[MONTHS[0]]["n_groups"], 2)
        self.assertEqual(stats[MONTHS[-1]]["n_groups"], 1)
        self.assertAlmostEqual(stats[MONTHS[-1]]["cov_group"], 0.5)

    def test_no_imputation_of_missing_group(self):
        # Группа без SA не подменяется нулём: месяц считается по оставшимся.
        sa = {"10": {month: 2.0 for month in MONTHS},
              "20": {MONTHS[0]: 100.0}}
        mm, _ = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        self.assertAlmostEqual(mm[MONTHS[-1]], 2.0)

    def test_group_without_weight_is_excluded(self):
        weights = {2019: {"10": 0.5, "9470": 0.1}, 2024: {"10": 0.5}}
        sa = {"10": {month: 1.0 for month in MONTHS}, "803": {month: 9.0 for month in MONTHS}}
        mm, stats = tt.aggregate_with_coverage(sa, weights, {2019: 1.0, 2024: 1.0}, MONTHS, ["10", "803"])
        self.assertAlmostEqual(mm[MONTHS[0]], 1.0)
        self.assertEqual(stats[MONTHS[0]]["n_groups"], 1)

    def test_zero_denominator_leaves_month_empty(self):
        sa = {"20": {month: 1.0 for month in MONTHS}}
        weights = {2019: {"10": 0.5}, 2024: {"10": 0.5}}
        mm, stats = tt.aggregate_with_coverage(sa, weights, {2019: 1.0, 2024: 1.0}, MONTHS, ["10"])
        self.assertEqual(mm, {})
        self.assertAlmostEqual(stats[MONTHS[0]]["cov_group"], 0.0)

    def test_weight_fallback_flag_in_stats(self):
        sa = {"10": {month: 1.0 for month in MONTHS}}
        _, stats = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10"])
        self.assertEqual(stats[date(2020, 1, 1)]["weight_year"], 2019)
        self.assertFalse(stats[date(2020, 1, 1)]["weight_fallback"])

    def test_unknown_rule_raises(self):
        with self.assertRaises(ValueError):
            tt.aggregate_with_coverage({}, WEIGHTS, TOTALS, MONTHS, ["10"], rule="renorm_v2")


class CoverageTest(unittest.TestCase):
    def test_cov_total_uses_all_cpi_weight(self):
        sa = {"10": {month: 1.0 for month in MONTHS},
              "20": {month: 1.0 for month in MONTHS}}
        _, stats = tt.aggregate_with_coverage(sa, WEIGHTS, {2019: 2.0, 2024: 2.0}, MONTHS, ["10", "20"])
        self.assertAlmostEqual(stats[MONTHS[0]]["cov_group"], 1.0)
        self.assertAlmostEqual(stats[MONTHS[0]]["cov_total"], 0.5)

    def test_coverage_yearly_and_window(self):
        sa = {"10": {month: 1.0 for month in MONTHS}, "20": {MONTHS[0]: 1.0}}
        _, stats = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        yearly = tt.coverage_yearly(stats)
        self.assertEqual(yearly["2020"]["months"], 6)
        self.assertAlmostEqual(yearly["2020"]["cov_group_min"], 0.5)
        window = tt.coverage_window(stats, date(2020, 5, 1))
        self.assertEqual(window["months"], 2)
        self.assertAlmostEqual(window["cov_group_max"], 0.5)


class TargetsTest(unittest.TestCase):
    def test_cumulative_matches_compounding(self):
        mm = {month: 1.0 for month in MONTHS}
        series = tt.targets(mm)
        expected = (math.exp(3 * math.log(1.01)) - 1) * 100
        self.assertAlmostEqual(series[3][MONTHS[2]], expected, places=12)
        self.assertEqual(min(series[1]), MONTHS[0])
        self.assertEqual(min(series[6]), MONTHS[5])

    def test_targets_match_live_bottomup(self):
        sa = {"10": {month: 0.3 + 0.01 * index for index, month in enumerate(MONTHS)},
              "20": {month: 0.2 for month in MONTHS}}
        mm, _ = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        ours = tt.targets(mm)
        reference = bu_targets(sa)
        for horizon in reference:
            self.assertEqual(sorted(ours[horizon]), sorted(reference[horizon]))
            for month in ours[horizon]:
                self.assertAlmostEqual(ours[horizon][month], reference[horizon][month], places=12)

    def test_gaps_detects_interior_break(self):
        series = {MONTHS[0]: 1.0, MONTHS[2]: 1.0}
        self.assertTrue(tt.gaps(series))
        self.assertFalse(tt.gaps({month: 1.0 for month in MONTHS}))


def bu_targets(sa: dict) -> dict:
    from models.cb_comp import cb_comp_live_bottomup as lb
    return lb.t_live(sa, WEIGHTS, MONTHS)


class DirectTest(unittest.TestCase):
    def test_direct_uses_injected_runner(self):
        nsa = {"10": {month: 1.0 for month in MONTHS}, "20": {month: 2.0 for month in MONTHS}}
        seen = {}

        def factory(start):
            seen["start"] = start

            def runner(levels, first_rt):
                seen["levels"] = levels
                return [(1.0, 1.0) for _ in levels]

            return runner

        series, block = tt.direct_targets(nsa, WEIGHTS, TOTALS, MONTHS, ["10", "20"], factory)
        self.assertEqual(seen["start"], MONTHS[0])
        self.assertEqual(block["months"], 6)
        self.assertAlmostEqual(series[1][MONTHS[-1]], 1.0)

    def test_direct_without_months_returns_empty(self):
        series, block = tt.direct_targets({}, WEIGHTS, TOTALS, MONTHS, ["10"])
        self.assertEqual(block["months"], 0)
        self.assertTrue(all(not values for values in series.values()))


class EvaluationTest(unittest.TestCase):
    def setUp(self):
        self.target = pd.DataFrame({horizon: [1.0, 2.0, 3.0] for horizon in (1, 2, 3, 6)},
                                   index=pd.Index(MONTHS[:3], name="month"))
        self.months = MONTHS[:3]

    def test_evaluate_rmse_and_bias(self):
        values = {MONTHS[0]: 1.0, MONTHS[1]: 4.0, MONTHS[2]: 5.0}
        result = tt.evaluate(values, self.target, 1, self.months)
        self.assertEqual(result["n"], 3)
        self.assertAlmostEqual(result["bias"], 4 / 3)
        self.assertAlmostEqual(result["rmse"], math.sqrt((0 + 4 + 4) / 3))

    def test_evaluate_ignores_months_without_target(self):
        values = {MONTHS[0]: 1.0, date(2021, 1, 1): 1.0}
        self.assertEqual(tt.evaluate(values, self.target, 1, self.months + [date(2021, 1, 1)])["n"], 1)

    def test_compare_blocks(self):
        truths = {"T_LIVE": {1: {month: 1.0 for month in self.months}}}
        normal = [MONTHS[0], MONTHS[2]]
        result = tt.compare_to_target_full(truths, self.target, normal, self.months)
        self.assertEqual(result["T_LIVE"]["h1"]["test_67"]["n"], 3)
        self.assertEqual(result["T_LIVE"]["h1"]["normal_54"]["n"], 2)
        self.assertNotIn("h2", result["T_LIVE"])

    def test_discrepancy_counts_live_window_only(self):
        alternative = {1: {month: 1.5 for month in MONTHS}}
        base = {1: {month: 1.0 for month in MONTHS}}
        result = tt.discrepancy(alternative, base, MONTHS + [date(2024, 8, 1)])
        self.assertEqual(result["h1"]["n"], 0)
        self.assertTrue(math.isnan(result["h1"]["rmse"]))


class DecisionTest(unittest.TestCase):
    def _comparison(self, base_rmse: float, direct_rmse: float, stable_left: float, stable_right: float):
        def block(rmse: float, left: float, right: float):
            return {"normal_54": {"rmse": rmse}, "2019_2021": {"rmse": left}, "2023_2024": {"rmse": right}}

        return {"T_LIVE": {"h1": block(base_rmse, 0.2, 0.2)},
                "T_LIVE_direct": {"h1": block(direct_rmse, stable_left, stable_right)}}

    def test_no_escalation_below_threshold(self):
        result = tt.decision(self._comparison(0.160, 0.155, 0.19, 0.19))
        self.assertFalse(result["escalate_to_owner"])
        self.assertEqual(result["live_truth"], "T_LIVE")

    def test_escalation_above_threshold_and_stable(self):
        result = tt.decision(self._comparison(0.160, 0.140, 0.10, 0.11))
        self.assertGreaterEqual(result["margin_normal_54_h1"], tt.DECISION_MARGIN)
        self.assertTrue(result["stable_subperiods"])
        self.assertTrue(result["escalate_to_owner"])

    def test_no_escalation_when_unstable_across_subperiods(self):
        result = tt.decision(self._comparison(0.160, 0.140, 0.30, 0.11))
        self.assertFalse(result["stable_subperiods"])
        self.assertFalse(result["escalate_to_owner"])


class GatesTest(unittest.TestCase):
    def _stats(self, coverage: float) -> dict:
        return {month: {"cov_total": coverage, "cov_group": coverage} for month in MONTHS}

    def _series(self, months: list) -> dict:
        """Ряд каждого h начинается со своего месяца: first + h − 1."""
        return {horizon: {month: 1.0 for month in months if month >= bu.shift_month(months[0], horizon - 1)}
                for horizon in tt.cp.ALL_HORIZONS}

    def test_passes_on_clean_input(self):
        series = self._series(MONTHS)
        result = tt.gates(series, self._stats(0.93), {"h1": {"n": 1}}, {"max_abs_diff": 0.0, "ok": True}, MONTHS[0])
        self.assertTrue(result["ok"])
        self.assertTrue(result["starts_ok"])

    def test_fails_on_gap(self):
        series = self._series(MONTHS)
        del series[1][MONTHS[2]]
        result = tt.gates(series, self._stats(0.93), {"h1": {"n": 1}}, {"max_abs_diff": 0.0, "ok": True}, MONTHS[0])
        self.assertFalse(result["no_gaps"])
        self.assertFalse(result["ok"])

    def test_fails_on_low_coverage(self):
        result = tt.gates(self._series(MONTHS), self._stats(0.80), {"h1": {"n": 1}},
                          {"max_abs_diff": 0.0, "ok": True}, MONTHS[0])
        self.assertFalse(result["coverage_ok"])
        self.assertFalse(result["ok"])

    def test_fails_on_wrong_start(self):
        result = tt.gates(self._series(MONTHS), self._stats(0.93), {"h1": {"n": 1}},
                          {"max_abs_diff": 0.0, "ok": True}, MONTHS[1])
        self.assertFalse(result["starts_ok"])

    def test_fails_on_regression(self):
        result = tt.gates(self._series(MONTHS), self._stats(0.93), {"h1": {"n": 1}},
                          {"max_abs_diff": 1e-9, "ok": False}, MONTHS[0])
        self.assertFalse(result["regression_ok"])

    def test_fails_on_undescribed_discrepancy(self):
        result = tt.gates(self._series(MONTHS), self._stats(0.93), {"h1": {"n": 0}},
                          {"max_abs_diff": 0.0, "ok": True}, MONTHS[0])
        self.assertFalse(result["discrepancy_documented"])


class SnapshotTest(unittest.TestCase):
    def _payload(self, value=0.4):
        return {"version": "2010", "month": "2026-09", "computed_at": "2026-11-01T00:00:00Z",
                "panel_sha256": "abc", "rule": tt.RULE,
                "t_live": {"h1": value}, "t_live_rt": {"h1": value},
                "coverage": {"cov_total": 0.93},
                "note": "наша пересборка цели на дату cutoff, не первая публикация Росстата"}

    def test_writes_then_skips(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "2026-09.json"
            self.assertEqual(tt.write_snapshot(path, self._payload())["status"], "written")
            self.assertEqual(tt.write_snapshot(path, {**self._payload(), "computed_at": "later"})["status"], "skip")

    def test_conflict_does_not_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "2026-09.json"
            tt.write_snapshot(path, self._payload())
            before = path.read_text(encoding="utf-8")
            result = tt.write_snapshot(path, self._payload(0.9))
            self.assertEqual(result["status"], "conflict")
            self.assertEqual(path.read_text(encoding="utf-8"), before)

    def test_payload_fields(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "2026-09.json"
            tt.write_snapshot(path, self._payload())
            stored = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(stored["rule"], tt.RULE)
            self.assertIn("не первая публикация", stored["note"])


class ArtifactsTest(unittest.TestCase):
    def test_writes_are_deterministic(self):
        sa = {"10": {month: 1.0 + 0.01 * index for index, month in enumerate(MONTHS)},
              "20": {month: 0.5 for month in MONTHS}}
        mm, stats = tt.aggregate_with_coverage(sa, WEIGHTS, TOTALS, MONTHS, ["10", "20"])
        series = tt.targets(mm)
        rows = tt.group_report(sa, {}, WEIGHTS, ["10", "20"], MONTHS[-1])
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp) / "a", Path(tmp) / "b"
            for folder in (first, second):
                tt.write_target(folder.with_suffix(".target.csv"), series, stats)
                tt.write_coverage(folder.with_suffix(".coverage.csv"), stats)
                tt.write_groups(folder.with_suffix(".groups.csv"), rows)
            for suffix in (".target.csv", ".coverage.csv", ".groups.csv"):
                self.assertEqual(first.with_suffix(suffix).read_bytes(), second.with_suffix(suffix).read_bytes())

    def test_target_csv_has_all_horizons(self):
        mm = {month: 0.5 for month in MONTHS}
        series = tt.targets(mm)
        stats = {month: {"n_groups": 2, "w_included": 1.0, "cov_group": 1.0, "cov_total": 0.9,
                         "weight_year": 2019, "weight_fallback": False} for month in MONTHS}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "target.csv"
            tt.write_target(path, series, stats)
            lines = path.read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(lines[0], "month,h1,h2,h3,h4,h5,h6")
            self.assertEqual(len(lines), len(MONTHS) + 1)

    def test_group_report_marks_groups_of_interest(self):
        rows = tt.group_report({"9218": {}, "10": {MONTHS[0]: 1.0}}, {}, WEIGHTS, ["9218", "10"], MONTHS[-1])
        marks = {row["code"]: row["of_interest"] for row in rows}
        self.assertTrue(marks["9218"])
        self.assertFalse(marks["10"])
        self.assertEqual(rows[0]["n_sa"], 0)
        self.assertEqual(rows[1]["last_sa"], MONTHS[0].isoformat())


class NsaCheckTest(unittest.TestCase):
    MONTHS_2022 = grid(date(2022, 1, 1), date(2022, 6, 1))
    MONTHS_2025 = grid(date(2025, 1, 1), date(2025, 3, 1))

    def test_reports_bias_and_verdict(self):
        nsa = {"10": {month: 1.0 for month in self.MONTHS_2022},
               "20": {month: 1.0 for month in self.MONTHS_2022}}
        published = {tt.HEADLINE_SERIES: {month: 1.0 for month in self.MONTHS_2022},
                     tt.UTILITIES_SERIES: {month: 1.0 for month in self.MONTHS_2022}}
        result = tt.nsa_weights_check(nsa, WEIGHTS, self.MONTHS_2022, ["10", "20"], published)
        self.assertEqual(result["all"]["months"], 6)
        self.assertAlmostEqual(result["all"]["bias"], 0.0)
        self.assertEqual(result["late_2025_2026"]["months"], 0)

    def test_detects_weights_drift(self):
        published = {tt.HEADLINE_SERIES: {month: 1.5 for month in self.MONTHS_2022},
                     tt.UTILITIES_SERIES: {month: 1.0 for month in self.MONTHS_2022}}
        nsa = {"10": {month: 1.0 for month in self.MONTHS_2022},
               "20": {month: 1.0 for month in self.MONTHS_2022}}
        result = tt.nsa_weights_check(nsa, WEIGHTS, self.MONTHS_2022, ["10", "20"], published)
        self.assertAlmostEqual(result["all"]["bias"], -0.5)

    def test_marks_degraded_weights_in_2025(self):
        nsa = {"10": {month: 1.0 for month in self.MONTHS_2025},
               "20": {month: 1.0 for month in self.MONTHS_2025}}
        published = {tt.HEADLINE_SERIES: {month: 1.4 for month in self.MONTHS_2025},
                     tt.UTILITIES_SERIES: {month: 1.0 for month in self.MONTHS_2025}}
        result = tt.nsa_weights_check(nsa, WEIGHTS, self.MONTHS_2025, ["10", "20"], published)
        self.assertEqual(result["late_2025_2026"]["months"], 3)
        self.assertEqual(result["late_2025_2026"]["verdict"], "weight_vintage_degraded")


class WeightTableTest(unittest.TestCase):
    def test_group_weights_match_bottomup_loader(self):
        try:
            archive = tt.cp.resolve_archive(None)
        except SystemExit:
            self.skipTest("архив CB_COMP недоступен")
        specs = [type("Spec", (), {"code": code})() for code in ("10", "20", "803")]
        expected = bu._year_weights(archive, 2020, specs)
        table, total = tt.year_weights_with_total(archive, 2020, [spec.code for spec in specs])
        for code, value in expected.items():
            self.assertAlmostEqual(table[code], value, places=12)
        self.assertAlmostEqual(total, 1.0, places=6)
        self.assertGreater(table[bu.UTILITIES_CODE], 0.0)


if __name__ == "__main__":
    unittest.main()
