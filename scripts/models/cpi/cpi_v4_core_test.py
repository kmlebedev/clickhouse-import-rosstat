#!/usr/bin/env python3
"""Офлайн-тесты эксперимента 3 (H3, #396 Task 7, спека §7.3).

Проверяются: перевод B0 в месячные п.п., наивные прогнозы B0f/M1f, состав
моделей по ячейкам, парные сравнения на общих фолдах, RMSE совмещённых оценок
и решение gate (promote / iterate / reject / incomplete). ClickHouse и сеть не
используются.

Запуск — `make cpi-v4-core-test`.
"""

import math
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from models.cpi import cpi_monthly as cm
from cpi_v4_core import (
    CANDIDATES,
    b0_monthly_pp,
    core_model_names,
    decide_h3,
    nowcast_rmse,
    pairs_from_predictions,
    proxy_model,
)

X = "v4c_core_nnls"
Y = "v4d_core_direct"


def pair(delta, p=0.01, ci=None, rmse_right=1.0):
    ci = ci if ci is not None else ([delta - 0.05, delta - 0.01] if delta < 0 else [delta + 0.01, delta + 0.05])
    return {"delta": delta, "dm_p": p, "delta_ci": ci, "rmse_left": rmse_right + delta, "rmse_right": rmse_right, "n": 40}


def cell(spec, status="ok"):
    """`spec` — {"X vs Y": pair kwargs tuple|float}."""
    if status != "ok":
        return {"status": status}
    return {
        "status": "ok",
        "core_pairs": {k: pair(*v) if isinstance(v, tuple) else pair(v) for k, v in spec.items()},
    }


def good_cells():
    both = lambda other, d=-0.2: {f"{c} vs {other}": d for c in CANDIDATES}  # noqa: E731
    return {
        ("S_L09", 1): cell(both("B0f")),
        ("S_L09", 3): cell({f"{c} vs B3": (-0.05, 0.4) for c in CANDIDATES}),
        ("S_L09", 6): cell({f"{c} vs B3": (-0.05, 0.4) for c in CANDIDATES}),
        ("S_W22", 1): cell({**both("B0f"), **both("M1f")}),
    }


class B0Test(unittest.TestCase):
    def test_annual_to_monthly_pp(self):
        annual = (1.01**12 - 1) * 100
        self.assertAlmostEqual(b0_monthly_pp(annual), 1.0, places=9)

    def test_zero_is_zero(self):
        self.assertEqual(b0_monthly_pp(0.0), 0.0)


class ProxyModelTest(unittest.TestCase):
    def row(self, month):
        return cm.Row(month=month, y={"cpi_all": 0.0}, features={})

    def test_returns_value_of_origin_month(self):
        model = proxy_model({date(2024, 3, 1): 0.7})
        self.assertEqual(model([], self.row(date(2024, 3, 1))), 0.7)

    def test_undefined_month_skips_fold(self):
        with self.assertRaises(cm.DegenerateWindowError):
            proxy_model({}) ([], self.row(date(2024, 3, 1)))

    def test_nan_value_skips_fold(self):
        with self.assertRaises(cm.DegenerateWindowError):
            proxy_model({date(2024, 3, 1): float("nan")})([], self.row(date(2024, 3, 1)))


class ModelNamesTest(unittest.TestCase):
    def test_s_l09_h1_has_b0f_not_m1f(self):
        names = core_model_names("S_L09", 1)
        self.assertIn("B0f", names)
        self.assertNotIn("M1f", names)

    def test_s_l09_h3_has_no_proxies(self):
        names = core_model_names("S_L09", 3)
        self.assertNotIn("B0f", names)
        self.assertNotIn("M1f", names)

    def test_s_w22_h1_has_both(self):
        names = core_model_names("S_W22", 1)
        self.assertIn("B0f", names)
        self.assertIn("M1f", names)

    def test_candidates_and_benchmarks_present_and_v4b_absent(self):
        names = core_model_names("S_L09", 1)
        for name in ("B1", "B2", "B3", "v2_baseline", "v3_direct", "v4a_direct", X, Y):
            self.assertIn(name, names)
        self.assertNotIn("v4b_direct", names)


class PairsTest(unittest.TestCase):
    def preds(self, models, n=30, start_year=2015):
        out = []
        for model, errs in models.items():
            for i in range(n):
                year, month = start_year + i // 12, i % 12 + 1
                out.append(
                    {"model": model, "origin": f"{year}-{month:02d}-01", "horizon": 1, "pred": errs[i], "actual": 0.0}
                )
        return out

    def test_candidate_better_gives_negative_delta(self):
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(30)]
        right = [x + 0.3 + 0.02 * (i % 3) for i, x in enumerate(left)]
        out = pairs_from_predictions(self.preds({X: left, "B0f": right}), 1, [X], ["B0f"], None)
        self.assertLess(out[f"{X} vs B0f"]["delta"], 0)
        self.assertEqual(out[f"{X} vs B0f"]["n"], 30)

    def test_floor_drops_earlier_origins(self):
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(30)]
        right = [x + 0.3 + 0.02 * (i % 3) for i, x in enumerate(left)]
        out = pairs_from_predictions(self.preds({X: left, "B0f": right}), 1, [X], ["B0f"], date(2016, 1, 1))
        self.assertEqual(out[f"{X} vs B0f"]["n"], 30 - 12)

    def test_missing_counterpart_is_skipped(self):
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(30)]
        out = pairs_from_predictions(self.preds({X: left}), 1, [X], ["B0f"], None)
        self.assertEqual(out, {})

    def test_common_folds_only(self):
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(30)]
        data = self.preds({X: left, "B0f": [x + 0.3 + 0.02 * (i % 3) for i, x in enumerate(left)]})
        data = [d for d in data if not (d["model"] == "B0f" and d["origin"] < "2016-01-01")]
        out = pairs_from_predictions(data, 1, [X], ["B0f"], None)
        self.assertEqual(out[f"{X} vs B0f"]["n"], 18)


class NowcastTest(unittest.TestCase):
    def test_rmse_on_common_months(self):
        m = [date(2024, k, 1) for k in (1, 2, 3)]
        core = {m[0]: 1.0, m[1]: 1.0, m[2]: 1.0}
        proxies = {"a": {m[0]: 2.0, m[1]: 2.0}, "b": {m[0]: 1.0, m[1]: 1.0, m[2]: 4.0}}
        out = nowcast_rmse(core, proxies)
        self.assertEqual(out["months"], [m[0].isoformat(), m[1].isoformat()])
        self.assertAlmostEqual(out["rmse"]["a"], 1.0)
        self.assertAlmostEqual(out["rmse"]["b"], 0.0)

    def test_nan_months_excluded(self):
        m = [date(2024, k, 1) for k in (1, 2)]
        out = nowcast_rmse({m[0]: 1.0, m[1]: 1.0}, {"a": {m[0]: 2.0, m[1]: float("nan")}})
        self.assertEqual(len(out["months"]), 1)

    def test_empty_overlap_gives_none(self):
        out = nowcast_rmse({date(2024, 1, 1): 1.0}, {"a": {date(2025, 1, 1): 2.0}})
        self.assertEqual(out["months"], [])
        self.assertIsNone(out["rmse"]["a"])


class DecideTest(unittest.TestCase):
    def test_all_conditions_promote(self):
        out = decide_h3(good_cells())
        for c in CANDIDATES:
            self.assertEqual(out[c]["decision"], "promote", out[c])

    def test_not_lower_than_b0f_on_s_l09_rejects(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({f"{c} vs B0f": (0.05, 0.4) for c in CANDIDATES})
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "reject")
        self.assertIn(1, out[X]["failed"])

    def test_not_lower_than_m1f_on_s_w22_rejects(self):
        cells = good_cells()
        cells[("S_W22", 1)] = cell(
            {**{f"{c} vs B0f": -0.2 for c in CANDIDATES}, **{f"{c} vs M1f": (0.01, 0.4) for c in CANDIDATES}}
        )
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "reject")
        self.assertIn(2, out[X]["failed"])

    def test_lower_but_not_significant_iterates(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({f"{c} vs B0f": (-0.2, 0.2) for c in CANDIDATES})
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "iterate")
        self.assertEqual(out[X]["failed"], [1])

    def test_ci_containing_zero_iterates(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({f"{c} vs B0f": (-0.2, 0.01, [-0.4, 0.1]) for c in CANDIDATES})
        self.assertEqual(decide_h3(cells)[X]["failed"], [1])

    def test_s_w22_needs_both_proxies_significant(self):
        cells = good_cells()
        cells[("S_W22", 1)] = cell(
            {**{f"{c} vs B0f": -0.2 for c in CANDIDATES}, **{f"{c} vs M1f": (-0.2, 0.3) for c in CANDIDATES}}
        )
        self.assertEqual(decide_h3(cells)[X]["failed"], [2])

    def test_h3_worse_than_b3_beyond_tolerance_iterates(self):
        cells = good_cells()
        cells[("S_L09", 3)] = cell({f"{c} vs B3": (0.06, 0.4) for c in CANDIDATES})  # 1.06 > 1.05
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "iterate")
        self.assertEqual(out[X]["failed"], [3])

    def test_h3_within_tolerance_is_fine(self):
        cells = good_cells()
        cells[("S_L09", 3)] = cell({f"{c} vs B3": (0.04, 0.4) for c in CANDIDATES})
        self.assertEqual(decide_h3(cells)[X]["decision"], "promote")

    def test_h6_significantly_worse_than_b3_iterates_even_within_tolerance(self):
        cells = good_cells()
        cells[("S_L09", 6)] = cell({f"{c} vs B3": (0.03, 0.01) for c in CANDIDATES})
        self.assertEqual(decide_h3(cells)[X]["failed"], [3])

    def test_missing_cell_is_incomplete(self):
        cells = good_cells()
        del cells[("S_W22", 1)]
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "incomplete")

    def test_missing_pair_is_incomplete(self):
        cells = good_cells()
        cells[("S_W22", 1)] = cell({f"{c} vs B0f": -0.2 for c in CANDIDATES})  # нет M1f
        self.assertEqual(decide_h3(cells)[X]["decision"], "incomplete")

    def test_candidates_independent(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({f"{X} vs B0f": -0.2, f"{Y} vs B0f": (-0.2, 0.3)})
        out = decide_h3(cells)
        self.assertEqual(out[X]["decision"], "promote")
        self.assertEqual(out[Y]["decision"], "iterate")

    def test_wording_is_hybrid_proxy(self):
        out = decide_h3(good_cells())
        self.assertIn("hybrid/proxy", out[X]["wording"])

    def test_reasons_carry_numbers(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({f"{c} vs B0f": (-0.2, 0.123) for c in CANDIDATES})
        self.assertIn("0.123", " ".join(decide_h3(cells)[X]["reasons"]))

    def test_candidates_constant(self):
        self.assertEqual(CANDIDATES, (X, Y))
        self.assertTrue(math.isfinite(b0_monthly_pp(5.0)))


if __name__ == "__main__":
    unittest.main()
