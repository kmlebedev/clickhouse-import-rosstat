#!/usr/bin/env python3
"""Офлайн-тесты решения gate эксперимента 2 (H2, #396 Task 6, спека §7/§7.2).

Решение проверяется на синтетических результатах ячеек (формат
`cpi_v4_grid.evaluate_cell`): promote / iterate / reject / incomplete, каждое
условие по отдельности, независимость двух кандидатов, сравнение с v3 по
предсказаниям. ClickHouse и сеть не используются.

Запуск — `make cpi-v4-h2-test`.
"""

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from cpi_v4_h2 import CANDIDATES, REFERENCE, decide_h2, relative_effect, vs_v3_from_predictions

CAND = "v4a_bottomup"
OTHER = "v4b_bottomup"


def pair(delta, p=0.01, ci=None):
    ci = ci if ci is not None else ([delta - 0.05, delta - 0.01] if delta < 0 else [delta + 0.01, delta + 0.05])
    return {"delta": delta, "dm_p": p, "delta_ci": ci, "n": 100}


def cell(deltas, included=None, status="ok", rmse_ref=1.0):
    """`deltas` — {модель: (delta, p, ci?)} к `v2_bottomup`."""
    if status != "ok":
        return {"status": status}
    pairs = {f"{m} vs {REFERENCE}": pair(*d) if isinstance(d, tuple) else pair(d) for m, d in deltas.items()}
    models = {REFERENCE: {"rmse": rmse_ref}}
    for m, d in deltas.items():
        delta = d[0] if isinstance(d, tuple) else d
        models[m] = {"rmse": rmse_ref + delta}
    everyone = included if included is not None else [REFERENCE, *deltas]
    return {"status": "ok", "pairs": pairs, "models": models, "mcs": {"included": everyone}}


def good_cells(delta=-0.1):
    deltas = {c: delta for c in CANDIDATES}
    return {
        ("S_L09", 1): cell(deltas),
        ("S_L09", 3): cell(deltas),
        ("S_L09", 6): cell({c: (0.0, 0.8) for c in CANDIDATES}),
        ("S_L13", 1): cell(deltas),
        ("S_L13", 3): cell(deltas),
        ("S_W22", 1): cell(deltas),
        ("S_W22", 3): cell(deltas),
    }


class DecideTest(unittest.TestCase):
    def test_all_conditions_met_promotes(self):
        out = decide_h2(good_cells())
        for c in CANDIDATES:
            self.assertEqual(out[c]["decision"], "promote", out[c])
            self.assertEqual(out[c]["failed"], [])

    def test_h1_rmse_not_lower_rejects(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({c: (0.02, 0.4) for c in CANDIDATES})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["decision"], "reject")
        self.assertIn(1, out[CAND]["failed"])

    def test_h3_rmse_not_lower_iterates_not_rejects(self):
        cells = good_cells()
        cells[("S_L09", 3)] = cell({c: (0.01, 0.4) for c in CANDIDATES})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["decision"], "iterate")
        self.assertEqual(out[CAND]["failed"], [1])

    def test_dm_not_significant_iterates(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({c: (-0.1, 0.07) for c in CANDIDATES})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["decision"], "iterate")
        self.assertEqual(out[CAND]["failed"], [2])

    def test_ci_containing_zero_iterates(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({c: (-0.1, 0.01, [-0.2, 0.05]) for c in CANDIDATES})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["failed"], [2])

    def test_sign_flip_on_robustness_sample_iterates(self):
        for sample, h in (("S_L13", 1), ("S_L13", 3), ("S_W22", 1), ("S_W22", 3)):
            cells = good_cells()
            cells[(sample, h)] = cell({c: (0.05, 0.5) for c in CANDIDATES})
            out = decide_h2(cells)
            self.assertEqual(out[CAND]["decision"], "iterate", (sample, h))
            self.assertEqual(out[CAND]["failed"], [3], (sample, h))

    def test_outside_mcs_in_either_primary_cell_iterates(self):
        for h in (1, 3):
            cells = good_cells()
            cells[("S_L09", h)] = cell({c: -0.1 for c in CANDIDATES}, included=[REFERENCE, OTHER])
            out = decide_h2(cells)
            self.assertEqual(out[CAND]["failed"], [4], h)
            self.assertEqual(out[OTHER]["decision"], "promote", h)

    def test_significant_h6_degradation_iterates(self):
        cells = good_cells()
        cells[("S_L09", 6)] = cell({c: (0.3, 0.01) for c in CANDIDATES})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["failed"], [5])

    def test_insignificant_h6_degradation_is_fine(self):
        cells = good_cells()
        cells[("S_L09", 6)] = cell({c: (0.3, 0.2) for c in CANDIDATES})
        self.assertEqual(decide_h2(cells)[CAND]["decision"], "promote")

    def test_h6_improvement_significant_is_fine(self):
        cells = good_cells()
        cells[("S_L09", 6)] = cell({c: (-0.3, 0.01) for c in CANDIDATES})
        self.assertEqual(decide_h2(cells)[CAND]["decision"], "promote")

    def test_missing_robustness_sample_is_incomplete(self):
        cells = good_cells()
        del cells[("S_L13", 3)]
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["decision"], "incomplete")
        self.assertIn(("S_L13", 3), [tuple(x) for x in out[CAND]["missing"]])

    def test_insufficient_folds_cell_is_incomplete(self):
        cells = good_cells()
        cells[("S_W22", 1)] = cell({}, status="insufficient_folds")
        self.assertEqual(decide_h2(cells)[CAND]["decision"], "incomplete")

    def test_candidates_decided_independently(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({CAND: -0.1, OTHER: (-0.1, 0.2)})
        out = decide_h2(cells)
        self.assertEqual(out[CAND]["decision"], "promote")
        self.assertEqual(out[OTHER]["decision"], "iterate")

    def test_reasons_carry_numbers(self):
        cells = good_cells()
        cells[("S_L09", 1)] = cell({c: (-0.1, 0.07) for c in CANDIDATES})
        text = " ".join(decide_h2(cells)[CAND]["reasons"])
        self.assertIn("0.07", text)

    def test_reference_is_v2_bottomup(self):
        self.assertEqual(REFERENCE, "v2_bottomup")
        self.assertEqual(CANDIDATES, ("v4a_bottomup", "v4b_bottomup"))


class RelativeEffectTest(unittest.TestCase):
    def test_relative_effect_is_delta_over_reference_rmse(self):
        c = cell({CAND: -0.2}, rmse_ref=1.0)
        self.assertAlmostEqual(relative_effect(c, CAND), -0.2)

    def test_none_for_incomplete_cell(self):
        self.assertIsNone(relative_effect({"status": "insufficient_folds"}, CAND))


class VsV3Test(unittest.TestCase):
    def preds(self, errors_by_model, horizon=1):
        out = []
        for model, errs in errors_by_model.items():
            for index, err in enumerate(errs):
                origin = f"2020-{index % 12 + 1:02d}-01" if index < 12 else f"{2021 + index // 12}-{index % 12 + 1:02d}-01"
                out.append({"model": model, "origin": origin, "horizon": horizon, "pred": err, "actual": 0.0})
        return out

    def test_candidate_better_gives_negative_delta(self):
        n = 40
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(n)]
        data = self.preds({CAND: left, "v3_bottomup": [x + 0.3 + 0.02 * (i % 3) for i, x in enumerate(left)]})
        out = vs_v3_from_predictions(data, 1, CAND)
        self.assertLess(out["delta"], 0)
        self.assertEqual(out["n"], n)

    def test_only_common_folds_used(self):
        left = [0.1 + 0.05 * ((i * 7) % 5) for i in range(40)]
        data = self.preds({CAND: left, "v3_bottomup": [x + 0.3 + 0.02 * (i % 3) for i, x in enumerate(left[:30])]})
        self.assertEqual(vs_v3_from_predictions(data, 1, CAND)["n"], 30)


if __name__ == "__main__":
    unittest.main()
