#!/usr/bin/env python3
"""Офлайн-тесты `cb_comp_per_code_reconstruction` (Task 8 #434): без ClickHouse и архива."""

import csv
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from models.cb_comp import cb_comp_per_code_reconstruction as rec


def months(n: int, start: str = "2010-01-01") -> pd.DatetimeIndex:
    return pd.date_range(start, periods=n, freq="MS")


def synthetic_panel(n: int = 177, offsets=(-1.0, 0.0, 1.0, -1.0, 0.0, 1.0)) -> tuple:
    idx = months(n)
    slugs = ["sheet_a", "sheet_b"]
    codes = ["01.1", "01.2", "01.3", "02.1", "02.2", "02.3"]
    code_sheet = {code: slugs[i // 3] for i, code in enumerate(codes)}
    sheets = {slug: pd.Series(np.sin(np.arange(n) / 6.0) * 10 + (i + 1) * 3, index=idx)
              for i, slug in enumerate(slugs)}
    codes_frame = {}
    for i, code in enumerate(codes):
        codes_frame[code] = sheets[code_sheet[code]] + offsets[i]
    return idx, slugs, codes, code_sheet, sheets, codes_frame


class ReconstructionTest(unittest.TestCase):
    def test_sha256_stable(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "x.txt"
            path.write_text("abc")
            self.assertEqual(rec.sha256(path), rec.sha256(path))
            self.assertEqual(len(rec.sha256(path)), 64)

    def test_estimate_weights_block_diagonal(self):
        idx, slugs, codes, code_sheet, sheets, codes_frame = synthetic_panel()
        codes_df = pd.DataFrame(codes_frame, index=idx)[codes]
        sheets_df = pd.DataFrame(sheets)[slugs]
        train = pd.Series(True, index=idx)
        weights, got_slugs, got_codes = rec.estimate_weights(codes_df, sheets_df, code_sheet, train)
        self.assertEqual(got_slugs, slugs)
        self.assertEqual(got_codes, codes)
        self.assertTrue(np.allclose(weights.sum(axis=1), 1.0))
        for row, slug in enumerate(slugs):
            for col, code in enumerate(codes):
                if code_sheet[code] != slug:
                    self.assertEqual(weights[row, col], 0.0)

    def test_forward_oos_exact(self):
        idx, slugs, codes, code_sheet, sheets, codes_frame = synthetic_panel()
        codes_df = pd.DataFrame(codes_frame, index=idx)[codes]
        sheets_df = pd.DataFrame(sheets)[slugs]
        train = pd.Series(True, index=idx)
        test = pd.Series(idx >= "2023-01-01", index=idx)
        weights, got_slugs, _ = rec.estimate_weights(codes_df, sheets_df, code_sheet, train, ridge=1e-6)
        forward = rec.forward_oos(weights, got_slugs, codes_df, sheets_df, train, test)
        for slug in slugs:
            self.assertGreater(forward[slug]["test"], 0.99)

    def test_identity_and_offset(self):
        weights = np.array([[0.5, 0.5, 0.0]])
        code_sheet = {"a": "s", "b": "s", "c": "s"}
        s_train = np.array([[4.0], [5.0]])
        x_train = np.array([[5.0, 6.0, 7.0], [6.0, 7.0, 8.0]])
        s_test = np.array([[6.0]])
        ident = rec.reconstruct(weights, ["s"], code_sheet, ["a", "b", "c"], s_train, x_train, s_test, np.zeros_like(x_train), np.zeros_like(s_test), "identity", None)
        np.testing.assert_allclose(ident, [[6.0, 6.0, 6.0]])
        offset = rec.reconstruct(weights, ["s"], code_sheet, ["a", "b", "c"], s_train, x_train, s_test, np.zeros_like(x_train), np.zeros_like(s_test), "offset", None)
        np.testing.assert_allclose(offset, [[7.0, 8.0, 9.0]])

    def test_ridge_prior_shrinks_to_prior(self):
        weights = np.array([[1.0, 0.0]])
        solution = rec._solve(weights, np.array([10.0]), 1e9, np.array([3.0, 4.0]))
        np.testing.assert_allclose(solution, [3.0, 4.0], atol=1e-6)

    def test_omp_selects_deviating_code(self):
        weights = np.array([[0.5, 0.5, 0.0]])
        code_sheet = {"a": "s", "b": "s", "c": "s"}
        s_train = np.zeros((4, 1))
        x_train = np.zeros((4, 3))
        s_test = np.array([[0.6]])
        m_test = np.array([[0.5, 0.5, 0.5]])
        out = rec.reconstruct(weights, ["s"], code_sheet, ["a", "b", "c"], s_train, x_train, s_test, np.zeros_like(x_train), m_test, "omp", 1)
        np.testing.assert_allclose(out, [[0.7, 0.5, 0.5]], atol=1e-9)

    def test_coord_respects_bounds(self):
        weights = np.array([[1.0, 0.0]])
        code_sheet = {"a": "s", "b": "s"}
        s_train = np.zeros((4, 1))
        x_train = np.zeros((4, 2))
        out = rec.reconstruct(weights, ["s"], code_sheet, ["a", "b"], s_train, x_train,
                              np.array([[1e6]]), np.zeros_like(x_train), np.zeros((1, 2)), "coord", 0.1)
        self.assertLessEqual(np.abs(out).max(), rec.BOUND + 1e-9)

    def test_elim_drops_zero_weight(self):
        weights = np.array([[0.5, 0.5, 0.0]])
        code_sheet = {"a": "s", "b": "s", "c": "s"}
        s_train = np.zeros((4, 1))
        x_train = np.zeros((4, 3))
        out = rec.reconstruct(weights, ["s"], code_sheet, ["a", "b", "c"], s_train, x_train,
                              np.array([[3.0]]), np.zeros_like(x_train), np.array([[2.0, 2.0, 2.0]]),
                              "elim", 0.01)
        self.assertAlmostEqual(out[0, 2], 2.0)
        np.testing.assert_allclose(out[0, :2], [3.0, 3.0], atol=1e-4)

    def test_null_space_static_share(self):
        idx = months(24)
        weights = np.array([[0.5, 0.5]])
        code_sheet = {"a": "s", "b": "s"}
        codes = pd.DataFrame({"a": np.arange(24.0) + 1.0, "b": np.arange(24.0) + 3.0}, index=idx)
        sheets = pd.DataFrame({"s": np.arange(24.0) + 1.0}, index=idx)
        info = rec.null_space(codes, sheets, weights, ["s"], code_sheet, ["a", "b"])
        self.assertEqual(info["rank_weights"], 1)
        self.assertEqual(info["null_dim"], 1)
        self.assertAlmostEqual(info["static_share"], 1.0)

    def test_evaluate_perfect(self):
        truth = np.array([[1.0, 2.0], [2.0, 3.0], [3.0, 4.0]])
        metrics = rec.evaluate(truth, truth.copy())
        self.assertAlmostEqual(metrics["r2_median"], 1.0)
        self.assertEqual(metrics["share_r2_positive"], 1.0)

    def test_run_end_to_end(self):
        idx, slugs, codes, code_sheet, sheets, codes_frame = synthetic_panel()
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            with (tmp / "panel.csv").open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["date", "group_stat", "code", "kind", "value", "lag"])
                for ind_name, ind in rec.INDICATORS.items():
                    for code in codes:
                        for date, value in codes_frame[code].items():
                            writer.writerow([date.date(), "Мониторинг предприятий", code, ind_name, value, 1])
            with (tmp / "public_panel.csv").open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["code", "indicator", "date", "value"])
                for ind in rec.IND_ORDER:
                    for code in codes:
                        for date, value in sheets[code_sheet[code]].items():
                            writer.writerow([code, ind, date.date(), value])
            with (tmp / "sheet_map.csv").open("w", newline="") as handle:
                writer = csv.writer(handle)
                writer.writerow(["code", "industry_slug"])
                for code in codes:
                    writer.writerow([code, code_sheet[code]])
            result = rec.run(tmp / "panel.csv", tmp, tmp / "out")
            self.assertIn(result["decision"], {"go", "no-go"})
            self.assertIn("G-R1", result["gates"])
            self.assertIn("G-R2", result["gates"])
            self.assertTrue((tmp / "out" / "result.json").is_file())
            self.assertEqual(result["gates"]["G-R1"]["pass"], True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
